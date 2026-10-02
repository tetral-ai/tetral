package runtimecontrol

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

type ThreadMutationScope struct {
	Visibility string
	Role       string
	Status     string
	TaskName   sql.NullString
}

func (s ThreadMutationScope) PublicProjection(eventType string) (string, bool) {
	if s.Visibility != "public" || s.Role == "approval_reviewer" {
		return "internal", false
	}
	if s.Role == "main" {
		return "public", true
	}
	switch eventType {
	case "agent.thread_message_sent",
		"agent.thread_message_received",
		"session.thread_created",
		"session.thread_status_running",
		"session.thread_status_idle",
		"session.thread_status_rescheduled",
		"session.thread_status_terminated":
		return "public", true
	default:
		return "public", false
	}
}

func LockThreadMutationTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) (ThreadMutationScope, error) {
	if err := RequireThreadMutationAllowedTx(ctx, tx, scope); err != nil {
		return ThreadMutationScope{}, err
	}
	return LockThreadMutationRowTx(ctx, tx, scope)
}

func LockThreadMutationRowTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) (ThreadMutationScope, error) {
	row := tx.QueryRow(ctx,
		`SELECT visibility, role, status, task_name
		   FROM session_threads
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND id = $3
		  FOR UPDATE`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	)
	var result ThreadMutationScope
	if err := row.Scan(&result.Visibility, &result.Role, &result.Status, &result.TaskName); dbconnect.IsNoRows(err) {
		return ThreadMutationScope{}, CloseoutUnrepairableError(status.Error(codes.FailedPrecondition, "runtime thread is stale"))
	} else if err != nil {
		return ThreadMutationScope{}, err
	}
	return result, nil
}

func SessionThreadCallableTaskNameTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	threadID string,
) (sql.NullString, error) {
	var role string
	var taskName sql.NullString
	err := tx.QueryRow(ctx,
		`SELECT role, task_name
		   FROM session_threads
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND id = $3`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		threadID,
	).Scan(&role, &taskName)
	if dbconnect.IsNoRows(err) {
		return sql.NullString{}, status.Error(codes.FailedPrecondition, "inter-agent message endpoint thread is stale")
	}
	if err != nil {
		return sql.NullString{}, err
	}
	if role == "main" {
		return sql.NullString{}, nil
	}
	if !taskName.Valid || taskName.String == "" {
		return sql.NullString{}, status.Error(codes.FailedPrecondition, "inter-agent message endpoint has no callable task name")
	}
	return taskName, nil
}

func NextSessionEventSequenceTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) (int64, error) {
	var sequence int64
	err := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(sequence), 0) + 1
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id IS NOT DISTINCT FROM $3`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	).Scan(&sequence)
	return sequence, err
}

func AppendSessionEventStreamChangeTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, eventID string, visibility string, sessionVisible bool, now time.Time) (int64, error) {
	return AppendSessionEventStreamChangeForRevisionTx(ctx, tx, scope, eventID, 1, visibility, sessionVisible, now)
}

func AppendSessionEventStreamChangeForRevisionTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, eventID string, revision int64, visibility string, sessionVisible bool, now time.Time) (int64, error) {
	var streamPosition int64
	if err := tx.QueryRow(ctx,
		`INSERT INTO session_event_stream_changes (
			workspace_id, session_id, event_id, session_thread_id, revision, visibility, session_visible, changed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING stream_position`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		eventID,
		scope.GetSessionThreadId(),
		revision,
		visibility,
		sessionVisible,
		now,
	).Scan(&streamPosition); err != nil {
		return 0, err
	}
	_, err := tx.Exec(ctx,
		`UPDATE session_events
		    SET latest_stream_position = $4,
		        insert_stream_position = CASE
		            WHEN $5 = 1 AND insert_stream_position = 0 THEN $4
		            ELSE insert_stream_position
		        END
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND event_id = $3`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		eventID,
		streamPosition,
		revision,
	)
	return streamPosition, err
}

type ToolProjection struct {
	EventType               string                              `json:"event_type"`
	EvaluatedPermission     string                              `json:"evaluated_permission"`
	MCPServerName           string                              `json:"mcp_server_name,omitempty"`
	ModelToolCallID         string                              `json:"model_tool_call_id"`
	ToolName                string                              `json:"tool_name"`
	ProviderInput           json.RawMessage                     `json:"provider_input"`
	RouteCapability         string                              `json:"route_capability"`
	CanonicalExecutionInput json.RawMessage                     `json:"canonical_execution_input"`
	State                   string                              `json:"state"`
	LeadingReasoning        []*bridgev1.RuntimeContextReasoning `json:"-"`
	Output                  *struct {
		Text      string `json:"text"`
		Truncated bool   `json:"truncated"`
	} `json:"output"`
	Error *struct {
		Type      string `json:"type"`
		Message   string `json:"message"`
		Retryable bool   `json:"retryable"`
	} `json:"error"`
}

func RuntimeToolRouteCapabilityAllowed(value string) bool {
	switch value {
	case "sandbox_execute", "background_command", "web_execute", "mcp_execute", "memory_execute",
		"child_create", "child_message", "child_wait", "child_interrupt", "child_close", "child_resume", "child_list":
		return true
	default:
		return false
	}
}

func ConsumeSandboxExecutionForTerminalWriterTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	toolUseEventID string,
	terminalEventID string,
	reason string,
	now time.Time,
) error {
	if reason != "pod_lost" && reason != "runtime_terminated" && reason != "cleanup_wait_expired" && reason != "conversation_tool_result" {
		return status.Error(codes.Internal, "sandbox execution terminal consumption reason is invalid")
	}
	var terminalPayloadJSON string
	if err := tx.QueryRow(ctx,
		`SELECT payload_json
		   FROM session_events
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3
		    AND event_id = $4`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), terminalEventID,
	).Scan(&terminalPayloadJSON); err != nil {
		return err
	}
	// A staged provider result is not the conversation's terminal Tool Result.
	// The first terminal session event owns settlement; alternate terminal
	// writers clear staged output and retain only its digest and execution record so a
	// late provider result cannot rewrite terminal conversation history.
	fallbackDigest := Sha256Hex(terminalPayloadJSON)
	result, err := tx.Exec(ctx,
		`UPDATE session_runtime_tool_results
		    SET execution_state = CASE WHEN tool_kind='sandbox_tool' THEN 'consumed' ELSE execution_state END,
		        background_operation_state = CASE WHEN tool_kind='sandbox_background' THEN 'terminal' ELSE background_operation_state END,
		        result_json = NULL,
		        result_digest = COALESCE(NULLIF(result_digest, ''), $8),
		        consumed_by_terminal_event_id = $5,
		        consumption_reason = $6, updated_at = $7
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3
		    AND tool_use_event_id = $4 AND tool_kind IN ('sandbox_tool','sandbox_background')
		    AND (tool_kind='sandbox_tool' AND execution_state <> 'consumed'
		      OR tool_kind='sandbox_background' AND consumed_by_terminal_event_id IS NULL)`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(),
		toolUseEventID, terminalEventID, reason, now, fallbackDigest,
	)
	if err != nil {
		return err
	}
	_ = result
	return nil
}

func ResolveSettledToolRouteTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, toolUseEventID string, resultEventID string, now time.Time) error {
	result, err := tx.Exec(ctx,
		`UPDATE session_pending_tool_uses
		    SET status = 'resolved',
		        result_event_id = $5,
		        resolved_at = $6,
		        updated_at = $6
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND tool_use_event_id = $4
		    AND (status = 'cancelled' OR (status = 'resolving' AND decision IN ('allow','deny')))
		    AND result_event_id IS NULL`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		toolUseEventID,
		resultEventID,
		now,
	)
	if err != nil {
		return err
	}
	if !RowsAffected(result) {
		return status.Error(codes.FailedPrecondition, "durable Tool route settlement did not close its exact route")
	}
	return nil
}

func UserMessageContextDraftJSON(payloadJSON string) (string, error) {
	var payload struct {
		Content []struct {
			Type   string `json:"type"`
			Text   string `json:"text"`
			Source *struct {
				Type   string `json:"type"`
				FileID string `json:"file_id"`
			} `json:"source"`
		} `json:"content"`
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return "", status.Error(codes.FailedPrecondition, "user message event payload is not projectable")
	}
	if len(payload.Content) == 0 {
		return "", status.Error(codes.FailedPrecondition, "user message event payload is not projectable")
	}
	parts := make([]map[string]any, 0, len(payload.Content))
	for _, item := range payload.Content {
		switch item.Type {
		case "text":
			if item.Text == "" || item.Source != nil {
				return "", status.Error(codes.FailedPrecondition, "user message event payload is not projectable")
			}
		case "image", "document":
			if item.Text != "" || item.Source == nil || item.Source.Type != "file" || item.Source.FileID == "" {
				return "", status.Error(codes.FailedPrecondition, "user message event payload is not projectable")
			}
			continue
		default:
			return "", status.Error(codes.FailedPrecondition, "user message event payload is not projectable")
		}
		parts = append(parts, map[string]any{
			"type":      "text",
			"text":      item.Text,
			"truncated": false,
		})
	}
	return MarshalDataJSON(map[string]any{"parts": parts})
}
