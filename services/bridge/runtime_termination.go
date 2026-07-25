package agentruntimebridge

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const runtimeTerminationFailureMaxBytes = 64 * 1024

type runtimeTerminationFailure struct {
	Type        string `json:"type"`
	Code        string `json:"code"`
	Message     string `json:"message"`
	Reason      string `json:"reason,omitempty"`
	Retryable   bool   `json:"retryable"`
	RetryStatus struct {
		Type string `json:"type"`
	} `json:"retryStatus"`
}

func parseRuntimeTerminationFailure(raw string) (runtimeTerminationFailure, string, error) {
	if raw == "" || len(raw) > runtimeTerminationFailureMaxBytes {
		return runtimeTerminationFailure{}, "", status.Error(codes.InvalidArgument, "runtime termination failure is invalid")
	}
	var object map[string]any
	if err := json.Unmarshal([]byte(raw), &object); err != nil || object == nil {
		return runtimeTerminationFailure{}, "", status.Error(codes.InvalidArgument, "runtime termination failure must be a JSON object")
	}
	var failure runtimeTerminationFailure
	if err := json.Unmarshal([]byte(raw), &failure); err != nil {
		return runtimeTerminationFailure{}, "", status.Error(codes.InvalidArgument, "runtime termination failure is invalid")
	}
	if failure.Retryable || failure.RetryStatus.Type != "terminal" || !isWhitelistedRuntimeTerminationFailure(failure) {
		return runtimeTerminationFailure{}, "", status.Error(codes.InvalidArgument, "runtime failure is not terminal")
	}
	canonical, err := marshalBridgeJSON(object)
	if err != nil {
		return runtimeTerminationFailure{}, "", status.Error(codes.InvalidArgument, "runtime termination failure is invalid")
	}
	return failure, canonical, nil
}

func isWhitelistedRuntimeTerminationFailure(failure runtimeTerminationFailure) bool {
	if failure.Type == "runtime" {
		return failure.Code == "runtime_invalid_sequence" && failure.Reason == "runtime_contract_validation"
	}
	if failure.Type != "provider" {
		return false
	}
	switch failure.Code {
	case "credential_required", "platform_keys_exhausted", "provider_key_unavailable",
		"provider_rate_limited", "provider_timeout", "provider_stream_error", "provider_unavailable",
		"provider_cancelled", "attachment_unavailable":
		return false
	default:
		return strings.HasPrefix(failure.Code, "provider_") || failure.Code == "context_overflow"
	}
}

func runtimeTerminationErrorKind(failure runtimeTerminationFailure) string {
	if failure.Type == "provider" {
		return "provider_error"
	}
	return "runtime_semantic_error"
}

func closeRuntimeTerminationSpansTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, sessionWide bool, failure runtimeTerminationFailure, now time.Time) error {
	starts, err := runtimeTerminationOpenRequestStartsTx(ctx, tx, scope, sessionWide)
	if err != nil {
		return err
	}
	binding := runtimeTerminationBinding(scope)
	for _, start := range starts {
		if _, err := insertRuntimeTerminalRequestEndTx(
			ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId(), binding, start,
			runtimeTerminationErrorKind(failure), "rwrite_runtime_termination_", now,
		); err != nil {
			return err
		}
	}
	return nil
}

func runtimeTerminationOpenRequestStartsTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, sessionWide bool) ([]runtimeOpenRequestStart, error) {
	threadID := scope.GetSessionThreadId()
	if sessionWide {
		threadID = ""
	}
	rows, err := tx.Query(ctx,
		`SELECT e.session_thread_id, e.event_id, e.model_request_id, e.payload_json
		   FROM session_events e
		  WHERE e.workspace_id = $1
		    AND e.session_id = $2
		    AND ($3 = '' OR e.session_thread_id = $3)
		    AND e.type = 'span.model_request_start'
		    AND COALESCE(e.model_request_id, '') <> ''
		    AND NOT EXISTS (
		        SELECT 1 FROM session_events ended
		         WHERE ended.workspace_id = e.workspace_id
		           AND ended.session_id = e.session_id
		           AND ended.model_request_id = e.model_request_id
		           AND ended.type = 'span.model_request_end'
		    )
		  ORDER BY e.sequence ASC, e.event_id ASC
		  FOR UPDATE OF e`,
		scope.GetWorkspaceId(), scope.GetSessionId(), threadID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	starts := make([]runtimeOpenRequestStart, 0)
	for rows.Next() {
		var start runtimeOpenRequestStart
		var payloadJSON string
		if err := rows.Scan(&start.SessionThreadID, &start.EventID, &start.ModelRequestID, &payloadJSON); err != nil {
			return nil, err
		}
		start.RequestKind = requestKindFromModelRequestStartPayload(payloadJSON)
		starts = append(starts, start)
	}
	return starts, rows.Err()
}

func settleRuntimeTerminationToolUsesTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, sessionWide bool, now time.Time) error {
	toolUses, err := runtimeTerminationOrphanToolUsesTx(ctx, tx, scope, sessionWide)
	if err != nil {
		return err
	}
	binding := runtimeTerminationBinding(scope)
	for _, toolUse := range toolUses {
		if _, err := insertRuntimeTerminalToolResultTx(ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId(), binding, toolUse, runtimeTerminalToolResult{
			WriteIDPrefix: "rwrite_runtime_termination_tool_",
			Reason:        "runtime_terminated",
			ErrorType:     "runtime_terminated",
			Message:       "Tool execution terminated with the runtime session.",
			Retryable:     false,
		}, now); err != nil {
			return err
		}
	}
	return nil
}

func runtimeTerminationOrphanToolUsesTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, sessionWide bool) ([]runtimeOrphanToolUse, error) {
	threadID := scope.GetSessionThreadId()
	if sessionWide {
		threadID = ""
	}
	rows, err := tx.Query(ctx,
		`SELECT e.session_thread_id, e.event_id, COALESCE(e.model_request_id, ''), e.payload_json, COALESCE(e.projection_json, '{}')
		   FROM session_events e
		  WHERE e.workspace_id = $1
		    AND e.session_id = $2
		    AND ($3 = '' OR e.session_thread_id = $3)
		    AND e.type = 'agent.tool_use'
		    AND e.visibility = 'public'
		    AND NOT EXISTS (
		        SELECT 1 FROM session_events result
		         WHERE result.workspace_id = e.workspace_id
		           AND result.session_id = e.session_id
		           AND result.type = 'agent.tool_result'
		           AND (result.payload_json::jsonb ->> 'tool_use_event_id' = e.event_id
		             OR result.payload_json::jsonb ->> 'tool_use_id' = e.event_id)
		    )
		  ORDER BY e.sequence ASC, e.event_id ASC
		  FOR UPDATE OF e`,
		scope.GetWorkspaceId(), scope.GetSessionId(), threadID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	toolUses := make([]runtimeOrphanToolUse, 0)
	for rows.Next() {
		var toolUse runtimeOrphanToolUse
		if err := rows.Scan(&toolUse.SessionThreadID, &toolUse.EventID, &toolUse.ModelRequestID, &toolUse.PayloadJSON, &toolUse.ProjectionJSON); err != nil {
			return nil, err
		}
		toolUses = append(toolUses, toolUse)
	}
	return toolUses, rows.Err()
}

func settleRuntimeTerminationPendingWaitsTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, sessionWide bool, now time.Time) error {
	threadID := scope.GetSessionThreadId()
	if sessionWide {
		threadID = ""
	}
	rows, err := tx.Query(ctx,
		`SELECT p.session_thread_id, p.tool_use_event_id, p.model_tool_call_id, p.tool_name, p.input_json,
		        e.type, COALESCE(e.payload_json::jsonb ->> 'mcp_server_name', '')
		   FROM session_pending_tool_uses p
		   JOIN session_events e
		     ON e.workspace_id = p.workspace_id AND e.session_id = p.session_id
		    AND e.session_thread_id = p.session_thread_id AND e.event_id = p.tool_use_event_id
		  WHERE p.workspace_id = $1 AND p.session_id = $2
		    AND ($3 = '' OR p.session_thread_id = $3)
		    AND p.status IN ('pending', 'resolving')
		  ORDER BY e.sequence ASC, p.tool_use_event_id ASC
		  FOR UPDATE OF p`,
		scope.GetWorkspaceId(), scope.GetSessionId(), threadID)
	if err != nil {
		return err
	}
	waits := make([]cleanupPendingWait, 0)
	for rows.Next() {
		var wait cleanupPendingWait
		if err := rows.Scan(&wait.ThreadID, &wait.ToolUseEventID, &wait.ModelToolCallID, &wait.ToolName, &wait.InputJSON, &wait.EventType, &wait.MCPServerName); err != nil {
			_ = rows.Close()
			return err
		}
		waits = append(waits, wait)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	_ = rows.Close()
	for _, wait := range waits {
		waitScope := bridgeSessionScope(scope.GetWorkspaceId(), scope.GetSessionId(), wait.ThreadID)
		waitScope.Binding = scope.GetBinding()
		eventID, exists, err := toolResultForToolUseExistsTx(ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId(), wait.ToolUseEventID)
		if err != nil {
			return err
		}
		if !exists {
			eventID, err = insertPendingToolTerminalResultTx(ctx, tx, waitScope, wait, pendingToolTerminal{
				PartStatus: "error",
				ErrorType:  "runtime_terminated",
				Message:    "Tool execution terminated with the runtime session.",
			}, now)
			if err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx,
			`UPDATE session_pending_tool_uses
			    SET status = 'cancelled', result_event_id = COALESCE(result_event_id, $5),
			        resolved_at = COALESCE(resolved_at, $6), updated_at = $6
			  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3
			    AND tool_use_event_id = $4 AND status IN ('pending', 'resolving')`,
			scope.GetWorkspaceId(), scope.GetSessionId(), wait.ThreadID, wait.ToolUseEventID,
			eventID, now); err != nil {
			return err
		}
	}
	return nil
}

func appendRuntimeTerminationErrorTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, threadScope threadMutationScope, runtimeWriteID string, failureJSON string, now time.Time) error {
	var failure runtimeTerminationFailure
	if err := json.Unmarshal([]byte(failureJSON), &failure); err != nil {
		return err
	}
	payloadJSON, err := marshalBridgeJSON(map[string]any{
		"type":  "session.error",
		"error": publicRuntimeTerminationError(failure),
	})
	if err != nil {
		return err
	}
	return insertRuntimeTerminationEventTx(ctx, tx, scope, threadScope, runtimeWriteID+":error", "session.error", payloadJSON, now)
}

func publicRuntimeTerminationError(failure runtimeTerminationFailure) map[string]any {
	errorType := "unknown_error"
	if failure.Type == "provider" {
		errorType = "model_request_failed_error"
	}
	message := strings.TrimSpace(failure.Message)
	if message == "" {
		message = "The runtime terminated because the request could not be completed."
	}
	return map[string]any{
		"type":         errorType,
		"message":      message,
		"retry_status": map[string]any{"type": "terminal"},
	}
}

func appendRuntimeTerminatedStatusTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, threadScope threadMutationScope, runtimeWriteID string, now time.Time) error {
	eventType := "session.thread_status_terminated"
	payloadJSON, err := threadStatusPayloadJSON(eventType, scope, threadScope, "")
	if err != nil {
		return err
	}
	if threadScope.role == "main" {
		eventType = "session.status_terminated"
		payloadJSON, err = marshalBridgeJSON(map[string]any{"type": eventType})
		if err != nil {
			return err
		}
		result, err := tx.Exec(ctx,
			`UPDATE sessions SET status = 'terminated', updated_at = $3 WHERE workspace_id = $1 AND id = $2`,
			scope.GetWorkspaceId(), scope.GetSessionId(), now)
		if err != nil {
			return err
		}
		if !rowsAffected(result) {
			return status.Error(codes.FailedPrecondition, "runtime session is stale")
		}
		if _, err := tx.Exec(ctx,
			`UPDATE session_runtime_status
			    SET active_seconds_total = active_seconds_total + CASE
			          WHEN running_since IS NULL THEN 0
			          ELSE GREATEST(0, EXTRACT(EPOCH FROM ($3 - running_since)))
			        END,
			        running_since = NULL,
			        updated_at = $3
			  WHERE workspace_id = $1 AND session_id = $2`,
			scope.GetWorkspaceId(), scope.GetSessionId(), now); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE session_threads
			    SET status = CASE WHEN id = $3 THEN 'failed' ELSE 'terminated' END,
			        closed_at = COALESCE(closed_at, $4), last_active_at = $4, updated_at = $4
			  WHERE workspace_id = $1 AND session_id = $2
			    AND status NOT IN ('terminated', 'failed')`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), now); err != nil {
			return err
		}
	} else {
		result, err := tx.Exec(ctx,
			`UPDATE session_threads
			    SET status = 'failed', closed_at = COALESCE(closed_at, $4), last_active_at = $4, updated_at = $4
			  WHERE workspace_id = $1 AND session_id = $2 AND id = $3 AND role <> 'main'`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), now)
		if err != nil {
			return err
		}
		if !rowsAffected(result) {
			return status.Error(codes.FailedPrecondition, "child thread status update failed")
		}
	}
	return insertRuntimeTerminationEventTx(ctx, tx, scope, threadScope, runtimeWriteID, eventType, payloadJSON, now)
}

func insertRuntimeTerminationEventTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, threadScope threadMutationScope, runtimeWriteID string, eventType string, payloadJSON string, now time.Time) error {
	visibility, sessionVisible := threadScope.publicProjection(eventType)
	eventID := id.New("evt_")
	sequence, err := nextSessionEventSequenceTx(ctx, tx, scope)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, runtime_write_id, projection_json, created_at, updated_at, processed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $7, $11, $11, $11)`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), eventID, sequence,
		eventType, payloadJSON, visibility, sessionVisible, runtimeWriteID, now); err != nil {
		return err
	}
	_, err = appendSessionEventStreamChangeTx(ctx, tx, scope, eventID, visibility, sessionVisible, now)
	return err
}

func runtimeTerminationBinding(scope *bridgev1.RuntimeScope) runtimeBindingForDelivery {
	return runtimeBindingForDelivery{
		BindingID:         scope.GetBinding().GetBindingId(),
		BindingGeneration: scope.GetBinding().GetBindingGeneration(),
		PodUID:            scope.GetBinding().GetTargetPodUid(),
	}
}
