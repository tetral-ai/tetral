package runtimecontrol

import (
	"context"
	"encoding/json"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/sessioneventwrite"
)

// RefreshSessionStatusTx is the one writer of a Session's change between
// running and idle and of the Session's session.status_running and
// session.status_idle events. Every transaction that changes a thread's
// status calls it after that change, holding the Session's runtime mutation
// lock (LockRuntimeMutationSessionTx). The Session runs while at least one
// thread, of any role, is running or rescheduling; it writes the event and
// the row only when that fact differs from the stored status. A terminated
// Session is left alone.
func RefreshSessionStatusTx(ctx context.Context, tx *dbconnect.Tx, workspaceID, sessionID string, now time.Time) error {
	var stored, sessionStatus string
	if err := tx.QueryRow(ctx, `SELECT srs.status,s.status FROM session_runtime_status srs
		JOIN sessions s ON s.workspace_id=srs.workspace_id AND s.id=srs.session_id
		WHERE srs.workspace_id=$1 AND srs.session_id=$2 FOR UPDATE OF srs`, workspaceID, sessionID).Scan(&stored, &sessionStatus); err != nil {
		return err
	}
	if sessionStatus == "terminated" {
		return nil
	}
	var active bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM session_threads
		WHERE workspace_id=$1 AND session_id=$2 AND status IN ('running','rescheduling'))`, workspaceID, sessionID).Scan(&active); err != nil {
		return err
	}
	switch {
	case stored == "idle" && active:
		eventID, err := insertSessionStatusEventTx(ctx, tx, workspaceID, sessionID, "session.status_running", `{"type":"session.status_running"}`, now)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE session_runtime_status SET status='running',status_event_id=$3,running_since=$4,idle_since=NULL,
			cleanup_after=NULL,cleanup_enqueued_at=NULL,cleanup_claimed_at=NULL,cleanup_job_id=NULL,updated_at=$4
			WHERE workspace_id=$1 AND session_id=$2`, workspaceID, sessionID, eventID, now)
		return err
	case stored == "running" && !active:
		stopReason, err := sessionIdleStopReasonTx(ctx, tx, workspaceID, sessionID)
		if err != nil {
			return err
		}
		payload, err := IdleStatusPayloadJSON(stopReason)
		if err != nil {
			return err
		}
		eventID, err := insertSessionStatusEventTx(ctx, tx, workspaceID, sessionID, "session.status_idle", payload, now)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE session_runtime_status SET status='idle',status_event_id=$3,idle_since=$4,
			active_seconds_total=active_seconds_total+CASE WHEN running_since IS NULL THEN 0 ELSE GREATEST(0,EXTRACT(EPOCH FROM ($4-running_since))) END,
			running_since=NULL,cleanup_after=$5,cleanup_enqueued_at=NULL,cleanup_claimed_at=NULL,cleanup_job_id=NULL,updated_at=$4
			WHERE workspace_id=$1 AND session_id=$2`, workspaceID, sessionID, eventID, now, now.Add(IdleCleanupDelay))
		return err
	}
	return nil
}

// sessionIdleStopReasonTx is the stop reason of the Session's idle: when the
// latest idle of an idle main or sub-agent thread stopped with
// requires_action, requires_action with the event_ids of every such idle,
// the main thread's first and then the sub-agents' in creation order;
// otherwise the main thread's latest idle stop reason.
func sessionIdleStopReasonTx(ctx context.Context, tx *dbconnect.Tx, workspaceID, sessionID string) (string, error) {
	rows, err := tx.Query(ctx, `SELECT t.role,latest.payload_json FROM session_threads t
		CROSS JOIN LATERAL (SELECT e.payload_json FROM session_events e
			WHERE e.workspace_id=t.workspace_id AND e.session_id=t.session_id AND e.session_thread_id=t.id
			AND e.type IN ('session.status_idle','session.thread_status_idle') ORDER BY e.sequence DESC LIMIT 1) latest
		WHERE t.workspace_id=$1 AND t.session_id=$2 AND t.role IN ('main','subagent') AND t.status IN ('idle','requires_action')
		ORDER BY t.role='main' DESC,t.storage_sequence`, workspaceID, sessionID)
	if err != nil {
		return "", err
	}
	defer func() { _ = rows.Close() }()
	mainStopReason := `{"type":"end_turn"}`
	var waiting []string
	requiresAction := false
	for rows.Next() {
		var role, payloadJSON string
		if err := rows.Scan(&role, &payloadJSON); err != nil {
			return "", err
		}
		var payload struct {
			StopReason json.RawMessage `json:"stop_reason"`
		}
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil || len(payload.StopReason) == 0 {
			continue
		}
		var stop struct {
			Type     string   `json:"type"`
			EventIDs []string `json:"event_ids"`
		}
		if err := json.Unmarshal(payload.StopReason, &stop); err != nil {
			continue
		}
		if role == "main" {
			mainStopReason = string(payload.StopReason)
		}
		if stop.Type == "requires_action" {
			requiresAction = true
			waiting = append(waiting, stop.EventIDs...)
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if !requiresAction {
		return mainStopReason, nil
	}
	return MarshalJSON(map[string]any{"type": "requires_action", "event_ids": append([]string{}, waiting...)})
}

// insertSessionStatusEventTx writes a Session event, which belongs to no
// thread, on the primary stream.
func insertSessionStatusEventTx(ctx context.Context, tx *dbconnect.Tx, workspaceID, sessionID, eventType, payloadJSON string, now time.Time) (string, error) {
	var sequence int64
	if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(sequence),0)+1 FROM session_events
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id IS NULL`, workspaceID, sessionID).Scan(&sequence); err != nil {
		return "", err
	}
	eventID := id.New("evt_")
	_, err := sessioneventwrite.InsertInitialTx(ctx, tx, sessioneventwrite.InitialEvent{
		WorkspaceID: workspaceID, SessionID: sessionID, EventID: eventID, Sequence: sequence, Type: eventType,
		PayloadJSON: payloadJSON, ProjectionJSON: payloadJSON, Visibility: "public", SessionVisible: true,
		CreatedAt: now, ProcessedAt: &now,
	})
	return eventID, err
}
