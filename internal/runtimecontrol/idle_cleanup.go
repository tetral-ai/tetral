package runtimecontrol

import (
	"context"
	"database/sql"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
)

// EnsureIdleCleanupScheduledTx restores the idle timer when input admission
// cleared it but terminal settlement started no run. The caller holds the
// Session mutation lock. Existing deadlines and cleanup claims retain ownership;
// the current binding and idle event remain the cleanup fence. If the Session
// has never published a status event, first publish its idle under the same guards.
// Delivered agent mail may remain accepted after its committed_at records consumption.
func EnsureIdleCleanupScheduledTx(ctx context.Context, tx *dbconnect.Tx, workspaceID, sessionID string, now time.Time) error {
	var idleEventID sql.NullString
	var bindingID string
	var bindingGeneration int64
	err := tx.QueryRow(ctx, `SELECT srs.status_event_id, binding.binding_id, binding.binding_generation
		FROM session_runtime_status srs
		JOIN sessions s ON s.workspace_id=srs.workspace_id AND s.id=srs.session_id
		JOIN session_runtime_bindings binding ON binding.workspace_id=srs.workspace_id AND binding.session_id=srs.session_id
		LEFT JOIN session_events idle ON idle.workspace_id=srs.workspace_id AND idle.session_id=srs.session_id AND idle.event_id=srs.status_event_id
		WHERE srs.workspace_id=$1 AND srs.session_id=$2 AND srs.status='idle'
		AND srs.cleanup_after IS NULL AND srs.cleanup_job_id IS NULL
		AND srs.cleanup_enqueued_at IS NULL AND srs.cleanup_claimed_at IS NULL
		AND s.lifecycle_state='active' AND s.status<>'terminated'
		AND ((binding.binding_id=srs.binding_id AND binding.binding_generation=srs.binding_generation)
		  OR (srs.status_event_id IS NULL AND srs.binding_id IS NULL AND srs.binding_generation IS NULL))
		AND (srs.status_event_id IS NULL OR (idle.type='session.status_idle' AND idle.latest_stream_position>0))
		AND NOT EXISTS (SELECT 1 FROM session_threads thread
		  WHERE thread.workspace_id=$1 AND thread.session_id=$2 AND thread.status IN ('running','rescheduling'))
		AND NOT EXISTS (SELECT 1 FROM session_runtime_inbox inbox
		  WHERE inbox.workspace_id=$1 AND inbox.session_id=$2
		  AND inbox.status IN ('queued','delivering','accepted','parked') AND inbox.input_kind<>ALL($3)
		  AND NOT (inbox.input_kind='agent_mail' AND inbox.status='accepted' AND inbox.committed_at IS NOT NULL))`,
		workspaceID, sessionID, queue.NonStartingRuntimeInputKinds()).Scan(&idleEventID, &bindingID, &bindingGeneration)
	if dbconnect.IsNoRows(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !idleEventID.Valid {
		eventID, err := insertSessionIdleEventTx(ctx, tx, workspaceID, sessionID, now)
		if err != nil {
			return err
		}
		idleEventID = sql.NullString{String: eventID, Valid: true}
	}
	_, err = tx.Exec(ctx, `UPDATE session_runtime_status SET status_event_id=$3,cleanup_after=$4,updated_at=$5,binding_id=$6,binding_generation=$7
		WHERE workspace_id=$1 AND session_id=$2`, workspaceID, sessionID, idleEventID.String, now.Add(IdleCleanupDelay), now, bindingID, bindingGeneration)
	return err
}
