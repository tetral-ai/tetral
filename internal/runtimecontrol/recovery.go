package runtimecontrol

import (
	"context"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func RecoveryDedupeKey(workspaceID string, payload queue.RuntimeRecoveryPayload) string {
	if payload.HandoffID != "" {
		return queue.FormatRuntimeHandoffDedupeKey(workspace.ID(workspaceID), payload.SessionID, payload.SessionThreadID, payload.HandoffID)
	}
	return queue.FormatRuntimeRecoveryDedupeKey(workspace.ID(workspaceID), payload.SessionID, payload.SourceEventID)
}

// VerifyRecoverySourceTx validates the committed source under Session
// arbitration. A handoff Thread's stored Queue ID is exact authority; neither
// a caller-named receipt nor another Thread's wake can activate recovery.
func VerifyRecoverySourceTx(ctx context.Context, tx *dbconnect.Tx, workspaceID, queueJobID string, payload queue.RuntimeRecoveryPayload) (bool, error) {
	if (payload.SourceEventID == "") == (payload.HandoffID == "") {
		return false, PreparationError{Kind: "invalid_runtime_job_payload", Message: "runtime recovery requires exactly one source", Retryable: false}
	}
	if payload.HandoffID != "" {
		var exists bool
		err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM session_runtime_handoff_threads thread JOIN session_runtime_handoffs handoff USING(workspace_id,session_id,handoff_id)
   WHERE thread.workspace_id=$1 AND thread.session_id=$2 AND thread.session_thread_id=$3 AND thread.handoff_id=$4 AND thread.disposition='recover' AND thread.queue_job_id=$5 AND NOT EXISTS(SELECT 1 FROM session_runtime_handoffs later WHERE later.workspace_id=handoff.workspace_id AND later.session_id=handoff.session_id AND later.binding_generation>handoff.binding_generation))`, workspaceID, payload.SessionID, payload.SessionThreadID, payload.HandoffID, queueJobID).Scan(&exists)
		return exists, err
	}
	var threadID, sourceType string
	err := tx.QueryRow(ctx, `SELECT session_thread_id,type FROM session_events WHERE workspace_id=$1 AND session_id=$2 AND event_id=$3 FOR UPDATE`, workspaceID, payload.SessionID, payload.SourceEventID).Scan(&threadID, &sourceType)
	if dbconnect.IsNoRows(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if threadID != payload.SessionThreadID || (sourceType != "agent.tool_use" && sourceType != "agent.mcp_tool_use" && sourceType != "session.status_rescheduled" && sourceType != "session.thread_status_rescheduled" && sourceType != "span.model_request_end") {
		return false, PreparationError{Kind: "invalid_runtime_job_payload", Message: "runtime recovery source is invalid", Retryable: false}
	}
	return true, nil
}
