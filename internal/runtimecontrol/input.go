package runtimecontrol

import (
	"context"
	"database/sql"
	"encoding/json"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
)

type InputFinalization struct {
	ThreadID        string
	InputKind       string
	RejectionReason sql.NullString
	EventIDs        []string
	SequenceFrom    sql.NullInt64
	SequenceTo      sql.NullInt64
	Status          string
}

// Exhaustion is authorized only by the producer-owned Inbox row. This lock and
// identity check runs before any source event, task, mail, or exhaustion fact
// can be mutated; Queue payloads never reconstruct missing custody.
func LockRuntimeInboxFinalizationTx(ctx context.Context, tx *dbconnect.Tx, job InputIdentity) (InputFinalization, error) {
	var row InputFinalization
	var eventIDsJSON string
	err := tx.QueryRow(ctx, `SELECT session_thread_id, input_kind, rejection_reason_code,
		event_ids_json, sequence_from, sequence_to, status
		FROM session_runtime_inbox
		WHERE workspace_id=$1 AND session_id=$2 AND runtime_input_id=$3
		FOR UPDATE`, job.WorkspaceID, job.SessionID, job.RuntimeInputID).Scan(
		&row.ThreadID, &row.InputKind, &row.RejectionReason, &eventIDsJSON,
		&row.SequenceFrom, &row.SequenceTo, &row.Status,
	)
	if dbconnect.IsNoRows(err) {
		return InputFinalization{}, InvalidRuntimeFinalizationIdentity("runtime Inbox custody is missing")
	}
	if err != nil {
		return InputFinalization{}, err
	}
	if err := json.Unmarshal([]byte(eventIDsJSON), &row.EventIDs); err != nil || row.EventIDs == nil {
		return InputFinalization{}, InvalidRuntimeFinalizationIdentity("runtime Inbox event identity is invalid")
	}
	if row.ThreadID != job.SessionThreadID {
		return InputFinalization{}, InvalidRuntimeFinalizationIdentity("runtime Inbox thread conflicts with Queue custody")
	}

	switch job.InputKind {
	case "agent_mail":
		if row.InputKind != "agent_mail" || len(job.EventIDs) != 0 || job.SequenceFrom != 0 || job.SequenceTo != 0 {
			return InputFinalization{}, InvalidRuntimeFinalizationIdentity("agent mail Queue identity conflicts with Inbox custody")
		}
		if row.Status == "dead_lettered" {
			return row, nil
		}
		if err := validateAgentMailFinalizationIdentityTx(ctx, tx, job, row); err != nil {
			return InputFinalization{}, err
		}
	case "task_notification":
		if row.InputKind != "task_notification" || len(job.EventIDs) != 0 || len(row.EventIDs) != 0 ||
			job.SequenceFrom != 0 || job.SequenceTo != 0 || row.SequenceFrom.Valid || row.SequenceTo.Valid {
			return InputFinalization{}, InvalidRuntimeFinalizationIdentity("task notification Queue identity conflicts with Inbox custody")
		}
		if row.Status == "dead_lettered" {
			return row, nil
		}
		if err := validateTaskNotificationFinalizationIdentityTx(ctx, tx, job); err != nil {
			return InputFinalization{}, err
		}
	default:
		kindMatches := row.InputKind == job.InputKind
		if job.InputKind == "messages" && row.InputKind == "rejection" {
			kindMatches = row.RejectionReason.Valid && (row.RejectionReason.String == "runtime_command_payload_too_large" || row.RejectionReason.String == "runtime_command_rejected")
		}
		if !kindMatches || len(job.EventIDs) == 0 || !slices.Equal(row.EventIDs, job.EventIDs) ||
			!row.SequenceFrom.Valid || !row.SequenceTo.Valid || row.SequenceFrom.Int64 != job.SequenceFrom || row.SequenceTo.Int64 != job.SequenceTo {
			return InputFinalization{}, InvalidRuntimeFinalizationIdentity("runtime Queue identity conflicts with Inbox custody")
		}
		if err := validateRuntimeFinalizationEventsTx(ctx, tx, job); err != nil {
			return InputFinalization{}, err
		}
	}
	return row, nil
}

func validateRuntimeFinalizationEventsTx(ctx context.Context, tx *dbconnect.Tx, job InputIdentity) error {
	if job.SequenceTo-job.SequenceFrom+1 != int64(len(job.EventIDs)) {
		return InvalidRuntimeFinalizationIdentity("runtime Queue event range is invalid")
	}
	for index, eventID := range job.EventIDs {
		var threadID string
		var sequence int64
		err := tx.QueryRow(ctx, `SELECT session_thread_id, sequence FROM session_events
			WHERE workspace_id=$1 AND session_id=$2 AND event_id=$3 FOR UPDATE`,
			job.WorkspaceID, job.SessionID, eventID,
		).Scan(&threadID, &sequence)
		if dbconnect.IsNoRows(err) || (err == nil && (threadID != job.SessionThreadID || sequence != job.SequenceFrom+int64(index))) {
			return InvalidRuntimeFinalizationIdentity("runtime Queue event identity conflicts with durable events")
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func validateAgentMailFinalizationIdentityTx(ctx context.Context, tx *dbconnect.Tx, job InputIdentity, inbox InputFinalization) error {
	deliveryID := strings.TrimPrefix(job.RuntimeInputID, "agent_mail:")
	if deliveryID == "" || deliveryID == job.RuntimeInputID {
		return InvalidRuntimeFinalizationIdentity("agent mail runtime input id is invalid")
	}
	if inbox.Status == "committed" {
		_, err := committedAgentMailSourceEventTx(ctx, tx, job, inbox)
		return err
	}
	envelope, err := LoadStoredAgentMailEnvelopeByDeliveryTx(ctx, tx, job.WorkspaceID, job.SessionID, deliveryID)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return InvalidRuntimeFinalizationIdentity("agent mail envelope is missing")
		}
		return err
	}
	if envelope.TargetThreadID != job.SessionThreadID || envelope.SourceThreadID == "" || envelope.SourceToolUseEventID == "" {
		return InvalidRuntimeFinalizationIdentity("agent mail envelope conflicts with Queue custody")
	}
	if inbox.Status == "queued" {
		if len(inbox.EventIDs) == 0 && !inbox.SequenceFrom.Valid && !inbox.SequenceTo.Valid {
			return nil
		}
	}
	receivedEventID := StableRuntimeID("agent_mail_received_event", job.WorkspaceID, job.SessionID, job.SessionThreadID, deliveryID)
	if len(inbox.EventIDs) != 1 || inbox.EventIDs[0] != receivedEventID || !inbox.SequenceFrom.Valid || !inbox.SequenceTo.Valid || inbox.SequenceFrom.Int64 != inbox.SequenceTo.Int64 {
		return InvalidRuntimeFinalizationIdentity("agent mail received identity conflicts with Inbox custody")
	}
	var sequence int64
	var sourceThreadID, sourceToolUseEventID string
	err = tx.QueryRow(ctx, `SELECT sequence, payload_json::jsonb ->> 'source_thread_id',
		payload_json::jsonb ->> 'source_tool_use_event_id'
		FROM session_events WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
		AND event_id=$4 AND type='agent.thread_message_received'
		AND payload_json::jsonb ->> 'delivery_id'=$5 FOR UPDATE`,
		job.WorkspaceID, job.SessionID, job.SessionThreadID, receivedEventID, deliveryID,
	).Scan(&sequence, &sourceThreadID, &sourceToolUseEventID)
	if dbconnect.IsNoRows(err) || (err == nil && (sequence != inbox.SequenceFrom.Int64 || sourceThreadID != envelope.SourceThreadID || sourceToolUseEventID != envelope.SourceToolUseEventID)) {
		return InvalidRuntimeFinalizationIdentity("agent mail received event conflicts with durable envelope")
	}
	return err
}

func committedAgentMailSourceEventTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	job InputIdentity,
	inbox InputFinalization,
) (string, error) {
	deliveryID := strings.TrimPrefix(job.RuntimeInputID, "agent_mail:")
	if deliveryID == "" || deliveryID == job.RuntimeInputID || len(inbox.EventIDs) != 1 ||
		!inbox.SequenceFrom.Valid || !inbox.SequenceTo.Valid || inbox.SequenceFrom.Int64 != inbox.SequenceTo.Int64 {
		return "", InvalidRuntimeFinalizationIdentity("committed agent mail identity is invalid")
	}
	sourceEventID := inbox.EventIDs[0]
	var eventSequence, messageSequence int64
	err := tx.QueryRow(ctx, `SELECT event.sequence, message.sequence
		FROM session_events event
		JOIN session_messages message
		  ON message.workspace_id=event.workspace_id
		 AND message.session_id=event.session_id
		 AND message.session_thread_id=event.session_thread_id
		 AND message.source_event_id=event.event_id
		WHERE event.workspace_id=$1 AND event.session_id=$2 AND event.session_thread_id=$3
		  AND event.event_id=$4 AND event.type='agent.thread_message_received'
		  AND event.payload_json::jsonb ->> 'delivery_id'=$5
		FOR UPDATE OF event, message`,
		job.WorkspaceID, job.SessionID, job.SessionThreadID, sourceEventID, deliveryID,
	).Scan(&eventSequence, &messageSequence)
	if dbconnect.IsNoRows(err) {
		return "", InvalidRuntimeFinalizationIdentity("committed agent mail Message relation is missing")
	}
	if err != nil {
		return "", err
	}
	if eventSequence != inbox.SequenceFrom.Int64 || messageSequence <= 0 {
		return "", InvalidRuntimeFinalizationIdentity("committed agent mail relation conflicts with Inbox custody")
	}
	return sourceEventID, nil
}

func validateTaskNotificationFinalizationIdentityTx(ctx context.Context, tx *dbconnect.Tx, job InputIdentity) error {
	taskID := TaskNotificationTaskID(job.RuntimeInputID)
	if taskID == "" {
		return InvalidRuntimeFinalizationIdentity("task notification runtime input id is invalid")
	}
	var threadID, sourceToolUseEventID, taskStatus string
	var terminalResultJSON sql.NullString
	var sourceExists bool
	err := tx.QueryRow(ctx, `SELECT task.session_thread_id, task.source_tool_use_event_id, task.status,
		task.terminal_result_json, EXISTS (
			SELECT 1 FROM session_events source WHERE source.workspace_id=task.workspace_id
			AND source.session_id=task.session_id AND source.session_thread_id=task.session_thread_id
			AND source.event_id=task.source_tool_use_event_id
		)
		FROM session_background_tasks task
		WHERE task.workspace_id=$1 AND task.session_id=$2 AND task.task_id=$3 FOR UPDATE`,
		job.WorkspaceID, job.SessionID, taskID,
	).Scan(&threadID, &sourceToolUseEventID, &taskStatus, &terminalResultJSON, &sourceExists)
	if dbconnect.IsNoRows(err) {
		return InvalidRuntimeFinalizationIdentity("task notification source is missing")
	}
	if err != nil {
		return err
	}
	if threadID != job.SessionThreadID || sourceToolUseEventID == "" || !sourceExists || !ValidBackgroundTaskTerminalStatus(taskStatus) ||
		!terminalResultJSON.Valid || !json.Valid([]byte(terminalResultJSON.String)) {
		return InvalidRuntimeFinalizationIdentity("task notification source conflicts with Queue custody")
	}
	return nil
}

func InvalidRuntimeFinalizationIdentity(message string) error {
	return PreparationError{Kind: "invalid_runtime_job_payload", Message: message, Retryable: false}
}

func RuntimeTaskNotificationStatus(terminalStatus string) string {
	switch terminalStatus {
	case "completed":
		return "completed"
	case "failed", "unknown_outcome":
		return "failed"
	case "cancelled":
		return "cancelled"
	case "expired":
		return "expired"
	default:
		return ""
	}
}

// parkTaskNotificationInboxTx is the sole queued/delivering/accepted-to-parked
// transition. Queue ownership remains with the caller: a leased delivery ACKs
// its exact token, while close admission targeted-cancels pending custody.
func ParkTaskNotificationInboxTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	threadID string,
	runtimeInputID string,
	binding Binding,
	now time.Time,
) (bool, error) {
	result, err := tx.Exec(ctx, `UPDATE session_runtime_inbox
		SET status='parked',
		    binding_id=COALESCE(binding_id,$6),
		    binding_generation=COALESCE(binding_generation,$7),
		    target_pod_uid=COALESCE(target_pod_uid,$8),
		    updated_at=$5
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND runtime_input_id=$4
		  AND input_kind='task_notification' AND status IN ('queued','delivering','accepted','parked')`,
		workspaceID, sessionID, threadID, runtimeInputID, now,
		binding.BindingID, binding.BindingGeneration, binding.PodUID,
	)
	if err != nil {
		return false, err
	}
	return RowsAffected(result), nil
}

func TaskNotificationTaskID(runtimeInputID string) string {
	value := strings.TrimSpace(runtimeInputID)
	for _, prefix := range []string{"task_notification:"} {
		if strings.HasPrefix(value, prefix) {
			return strings.TrimSpace(strings.TrimPrefix(value, prefix))
		}
	}
	return value
}
