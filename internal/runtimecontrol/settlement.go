package runtimecontrol

import (
	"context"
	"encoding/json"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

type OpenRequestStart struct {
	SessionThreadID string
	EventID         string
	ModelRequestID  string
	RequestKind     string
}

type OrphanToolUse struct {
	SessionThreadID string
	EventID         string
	EventType       string
	ModelRequestID  string
	ModelToolCallID string
	PayloadJSON     string
}

func RuntimePodLostOpenRequestStartsTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, affectedThreadIDs []string) ([]OpenRequestStart, error) {
	threadIDsJSON, err := json.Marshal(affectedThreadIDs)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx,
		`SELECT e.session_thread_id, e.event_id, e.model_request_id, e.projection_json
		   FROM session_events e
		  WHERE e.workspace_id = $1
		    AND e.session_id = $2
		    AND e.session_thread_id IN (SELECT jsonb_array_elements_text($3::jsonb))
		    AND e.type = 'span.model_request_start'
		    AND COALESCE(e.model_request_id, '') <> ''
			    AND NOT EXISTS (
		        SELECT 1
		          FROM session_events ended
		         WHERE ended.workspace_id = e.workspace_id
		           AND ended.session_id = e.session_id
		           AND ended.session_thread_id = e.session_thread_id
		           AND ended.model_request_id = e.model_request_id
		           AND ended.type = 'span.model_request_end'
		    )
		  ORDER BY e.sequence ASC, e.event_id ASC
		  FOR UPDATE OF e`,
		workspaceID,
		sessionID,
		string(threadIDsJSON),
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	starts := make([]OpenRequestStart, 0)
	for rows.Next() {
		var start OpenRequestStart
		var projectionJSON string
		if err := rows.Scan(&start.SessionThreadID, &start.EventID, &start.ModelRequestID, &projectionJSON); err != nil {
			return nil, err
		}
		requestKind, err := RequestKindFromModelRequestStartProjection(projectionJSON)
		if err != nil {
			return nil, err
		}
		start.RequestKind = requestKind
		starts = append(starts, start)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return starts, nil
}

type RuntimeOrphanToolSelection struct {
	IncludeSubAgentDeliveries bool
	PreserveNonterminalRoutes bool
}

func selectRuntimeTerminationOrphanToolUsesTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, affectedThreadIDs []string) ([]OrphanToolUse, error) {
	return RuntimeOrphanToolUsesTx(ctx, tx, workspaceID, sessionID, affectedThreadIDs, RuntimeOrphanToolSelection{
		IncludeSubAgentDeliveries: true,
	})
}

func RuntimeOrphanToolUsesTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, affectedThreadIDs []string, selection RuntimeOrphanToolSelection) ([]OrphanToolUse, error) {
	threadIDsJSON, err := json.Marshal(affectedThreadIDs)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx,
		`SELECT e.session_thread_id, e.event_id, e.type, e.model_request_id,
		        e.model_tool_call_id, e.payload_json
		   FROM session_events e
		   JOIN session_threads thread_scope
		     ON thread_scope.workspace_id = e.workspace_id
		    AND thread_scope.session_id = e.session_id
		    AND thread_scope.id = e.session_thread_id
		  WHERE e.workspace_id = $1
		    AND e.session_id = $2
		    AND e.session_thread_id IN (SELECT jsonb_array_elements_text($3::jsonb))
		    AND e.type IN ('agent.tool_use', 'agent.mcp_tool_use')
		    AND (
		      e.visibility = 'public'
		      OR (e.visibility = 'internal' AND thread_scope.role = 'approval_reviewer')
		    )
			    AND ($4 OR (
			      e.type <> 'agent.tool_use'
			      OR COALESCE(e.payload_json::jsonb ->> 'name', '') NOT IN ('spawn_agent', 'send_message')
			    ))
			    AND (NOT $5 OR NOT EXISTS (
			      SELECT 1
			        FROM session_pending_tool_uses route
			       WHERE route.workspace_id = e.workspace_id
			         AND route.session_id = e.session_id
			         AND route.session_thread_id = e.session_thread_id
			         AND route.tool_use_event_id = e.event_id
			         AND route.status IN ('pending','resolving')
			    ))
			    AND NOT EXISTS (
		        SELECT 1
		          FROM session_events result
			         WHERE result.workspace_id = e.workspace_id
			           AND result.session_id = e.session_id
			           AND result.session_thread_id = e.session_thread_id
			           AND (
			                (e.type = 'agent.tool_use' AND result.type = 'agent.tool_result')
			             OR (e.type = 'agent.mcp_tool_use' AND result.type = 'agent.mcp_tool_result')
			           )
			           AND result.tool_use_event_id = e.event_id
		    )
		  ORDER BY e.sequence ASC, e.event_id ASC
		  FOR UPDATE OF e`,
		workspaceID,
		sessionID,
		string(threadIDsJSON),
		selection.IncludeSubAgentDeliveries,
		selection.PreserveNonterminalRoutes,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	toolUses := make([]OrphanToolUse, 0)
	for rows.Next() {
		var toolUse OrphanToolUse
		if err := rows.Scan(
			&toolUse.SessionThreadID,
			&toolUse.EventID,
			&toolUse.EventType,
			&toolUse.ModelRequestID,
			&toolUse.ModelToolCallID,
			&toolUse.PayloadJSON,
		); err != nil {
			return nil, err
		}
		if toolUse.ModelToolCallID == "" {
			return nil, status.Error(codes.FailedPrecondition, "durable Tool Use identity is missing")
		}
		toolUses = append(toolUses, toolUse)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return toolUses, nil
}

func InsertRuntimeTerminalRequestEndTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, start OpenRequestStart, errorKind string, writeIDPrefix string, now time.Time) (bool, error) {
	if _, ok, err := ModelRequestEndExistsTx(ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId(), start.SessionThreadID, start.ModelRequestID); err != nil || ok {
		return false, err
	}
	threadScope, err := LockThreadMutationTx(ctx, tx, scope)
	if err != nil {
		return false, err
	}
	visibility, sessionVisible := threadScope.PublicProjection("span.model_request_end")
	retention, err := RuntimeTerminalRequestRetentionTx(ctx, tx, scope, start.ModelRequestID)
	if err != nil {
		return false, err
	}
	request := &bridgev1.WriteRequestEndRequest{
		Scope:                    scope,
		RuntimeWriteId:           writeIDPrefix + start.ModelRequestID,
		ModelRequestId:           start.ModelRequestID,
		IsError:                  true,
		ErrorKind:                errorKind,
		FinishReason:             "error",
		UsageJson:                "{}",
		ProviderContextRetention: retention,
	}
	payloadJSON, err := ModelRequestEndPayloadJSON(request, start.EventID, start.RequestKind, "error", Usage{})
	if err != nil {
		return false, err
	}
	eventID := id.New("evt_")
	sequence, err := NextSessionEventSequenceTx(ctx, tx, scope)
	if err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, runtime_write_id, model_request_id, projection_json, created_at, updated_at, processed_at
		) VALUES ($1, $2, $3, $4, $5, 'span.model_request_end', $6, $7, $8, $9, $10, $6, $11, $11, $11)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		start.SessionThreadID,
		eventID,
		sequence,
		payloadJSON,
		visibility,
		sessionVisible,
		request.GetRuntimeWriteId(),
		start.ModelRequestID,
		now,
	); err != nil {
		return false, err
	}
	if _, err := AppendSessionEventStreamChangeTx(ctx, tx, scope, eventID, visibility, sessionVisible, now); err != nil {
		return false, err
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO request_usage_details (
			workspace_id, session_id, session_thread_id, model_request_id, runtime_write_id,
			request_kind, input_total_tokens, input_uncached_tokens, output_total_tokens,
			total_tokens, provider_usage_json, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, 0, 0, 0, 0, '{}', $7)
		ON CONFLICT (workspace_id, session_id, model_request_id, runtime_write_id) DO NOTHING`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		start.SessionThreadID,
		start.ModelRequestID,
		request.GetRuntimeWriteId(),
		start.RequestKind,
		now,
	)
	if err != nil {
		return false, err
	}
	return true, nil
}

// A synthetic terminal Request boundary preserves every member carried by the
// Request's direct durable ownership relations. The exact Assistant owner,
// Tool Use events, and internal repair events survive cold reconstruction;
// route state, provider-visible content, and Tool payloads are never consulted.
func RuntimeTerminalRequestRetentionTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	modelRequestID string,
) (*bridgev1.ProviderContextRetention, error) {
	rows, err := tx.Query(ctx,
		`SELECT event.event_id,
		        CASE WHEN repair.idempotency_key IS NULL THEN 'tool_use' ELSE 'repair' END
		   FROM session_events event
		   LEFT JOIN session_bridge_operations repair
		     ON repair.workspace_id = event.workspace_id
		    AND repair.session_id = event.session_id
		    AND repair.session_thread_id = event.session_thread_id
		    AND repair.operation = 'commit_internal_tool_repair'
		    AND repair.source_kind = 'internal_tool_repair'
		    AND repair.idempotency_key = event.runtime_write_id
		    AND repair.ack_status = 'committed'
		  WHERE event.workspace_id = $1
		    AND event.session_id = $2
		    AND event.session_thread_id = $3
		    AND event.model_request_id = $4
		    AND (event.type IN ('agent.tool_use','agent.mcp_tool_use') OR
		         (event.type = 'agent.tool_result' AND repair.idempotency_key IS NOT NULL))
		  ORDER BY event.sequence ASC, event.event_id ASC`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), modelRequestID,
	)
	if err != nil {
		return nil, err
	}
	toolUseEventIDs := make([]string, 0)
	repairEventIDs := make([]string, 0)
	for rows.Next() {
		var eventID, memberKind string
		if err := rows.Scan(&eventID, &memberKind); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if memberKind == "tool_use" {
			toolUseEventIDs = append(toolUseEventIDs, eventID)
		} else {
			repairEventIDs = append(repairEventIDs, eventID)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	retention := &bridgev1.ProviderContextRetention{
		Disposition:     "failed",
		ToolUseEventIds: toolUseEventIDs,
		RepairEventIds:  repairEventIDs,
	}
	if len(toolUseEventIDs) == 0 && len(repairEventIDs) == 0 {
		return retention, nil
	}
	var assistantCount int
	var assistantSequence int64
	if err := tx.QueryRow(ctx,
		`SELECT count(*), COALESCE(MAX(sequence), 0)
		   FROM session_messages
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND model_request_id = $4
		    AND kind = 'assistant'`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), modelRequestID,
	).Scan(&assistantCount, &assistantSequence); err != nil {
		return nil, err
	}
	if assistantCount != 1 || assistantSequence <= 0 {
		return nil, status.Error(codes.FailedPrecondition, "live Tool routes have no exact Assistant owner")
	}
	retention.AssistantMessageSequence = &assistantSequence
	return retention, nil
}

type TerminalToolResult struct {
	WriteIDPrefix     string
	Reason            string
	ErrorType         string
	Message           string
	Retryable         bool
	Success           bool
	ConsumptionReason string
}

func InsertRuntimeTerminalToolResultForScopeTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, toolUse OrphanToolUse, terminal TerminalToolResult, now time.Time) (bool, error) {
	if _, ok, err := ToolResultForToolUseExistsTx(ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId(), toolUse.SessionThreadID, toolUse.EventType, toolUse.EventID); err != nil || ok {
		return false, err
	}
	threadScope, err := LockThreadMutationTx(ctx, tx, scope)
	if err != nil {
		return false, err
	}
	resultEventType := "agent.tool_result"
	toolUseField := "tool_use_id"
	if toolUse.EventType == "agent.mcp_tool_use" {
		resultEventType = "agent.mcp_tool_result"
		toolUseField = "mcp_tool_use_id"
	}
	visibility, sessionVisible := threadScope.PublicProjection(resultEventType)
	eventPayload := map[string]any{
		"type":       resultEventType,
		toolUseField: toolUse.EventID,
		"content": []map[string]string{{
			"type": "text",
			"text": terminal.Message,
		}},
		"is_error": !terminal.Success,
	}
	if terminal.Reason != "" {
		eventPayload["reason"] = terminal.Reason
	}
	payloadJSON, err := MarshalJSON(eventPayload)
	if err != nil {
		return false, err
	}
	eventID := id.New("evt_")
	sequence, err := NextSessionEventSequenceTx(ctx, tx, scope)
	if err != nil {
		return false, err
	}
	projection, err := SettleRuntimeTerminalToolPartTx(ctx, tx, scope, toolUse, terminal, now)
	if err != nil {
		return false, err
	}
	projectionJSON, err := MarshalJSON(projection)
	if err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, runtime_write_id, model_request_id, projection_json,
			tool_use_event_id, created_at, updated_at, processed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $14, $14)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		toolUse.SessionThreadID,
		eventID,
		sequence,
		resultEventType,
		payloadJSON,
		visibility,
		sessionVisible,
		terminal.WriteIDPrefix+toolUse.EventID,
		toolUse.ModelRequestID,
		projectionJSON,
		toolUse.EventID,
		now,
	); err != nil {
		return false, ToolRelationInsertError(err)
	}
	if _, err := AppendSessionEventStreamChangeTx(ctx, tx, scope, eventID, visibility, sessionVisible, now); err != nil {
		return false, err
	}
	consumptionReason := terminal.ConsumptionReason
	if consumptionReason == "" {
		consumptionReason = "pod_lost"
	}
	if err := ConsumeSandboxExecutionForTerminalWriterTx(ctx, tx, scope, toolUse.EventID, eventID, consumptionReason, now); err != nil {
		return false, err
	}
	if terminal.Success {
		if err := ResolveSettledToolRouteTx(ctx, tx, scope, toolUse.EventID, eventID, now); err != nil {
			return false, err
		}
	} else {
		if err := cancelPendingToolUseForTerminalResultTx(ctx, tx, scope, toolUse.EventID, eventID, now); err != nil {
			return false, err
		}
	}
	return true, nil
}

func SettleRuntimeTerminalToolPartTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	toolUse OrphanToolUse,
	terminal TerminalToolResult,
	now time.Time,
) (ToolProjection, error) {
	tool, err := LoadDurableToolExecutionTx(ctx, tx, scope, toolUse.EventID, toolUse.EventType, false)
	if err != nil {
		return ToolProjection{}, err
	}
	if tool.ModelRequestID != toolUse.ModelRequestID || tool.ModelToolCallID != toolUse.ModelToolCallID {
		return ToolProjection{}, status.Error(codes.FailedPrecondition, "durable Tool repair identity is inconsistent")
	}
	resultValue := map[string]any{"type": "completed", "output": map[string]any{"text": terminal.Message}}
	if !terminal.Success {
		resultValue = map[string]any{"type": "error", "error": map[string]any{
			"type": terminal.ErrorType, "message": terminal.Message, "retryable": terminal.Retryable,
		}}
	}
	resultPartsJSON, err := json.Marshal([]map[string]any{{
		"type": "tool_result", "modelToolCallId": toolUse.ModelToolCallID, "result": resultValue,
	}})
	if err != nil {
		return ToolProjection{}, err
	}
	result, err := tx.Exec(ctx,
		`UPDATE session_messages
		    SET data_json = jsonb_set(
		          data_json::jsonb,
		          '{parts}',
		          (data_json::jsonb -> 'parts') || $5::jsonb
		        )::text,
		        updated_at = $6
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND model_request_id = $4
		    AND kind = 'assistant'
		    AND jsonb_typeof(data_json::jsonb -> 'parts') = 'array'`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		toolUse.ModelRequestID,
		string(resultPartsJSON),
		now,
	)
	if err != nil {
		return ToolProjection{}, err
	}
	if !RowsAffected(result) {
		return ToolProjection{}, status.Error(codes.FailedPrecondition, "durable tool message lost its fence")
	}
	return RuntimeToolProjectionFromDurableTool(tool, resultValue), nil
}

func ModelRequestEndExistsTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, sessionThreadID string, modelRequestID string) (string, bool, error) {
	var eventID string
	err := tx.QueryRow(ctx,
		`SELECT event_id
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND model_request_id = $4
		    AND type = 'span.model_request_end'
		  ORDER BY sequence ASC
		  LIMIT 1`,
		workspaceID,
		sessionID,
		sessionThreadID,
		modelRequestID,
	).Scan(&eventID)
	if dbconnect.IsNoRows(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return eventID, true, nil
}

func ToolResultForToolUseExistsTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, sessionThreadID string, toolUseEventType string, toolUseEventID string) (string, bool, error) {
	resultEventType := ""
	switch toolUseEventType {
	case "agent.tool_use":
		resultEventType = "agent.tool_result"
	case "agent.mcp_tool_use":
		resultEventType = "agent.mcp_tool_result"
	default:
		return "", false, status.Error(codes.FailedPrecondition, "tool use family is invalid")
	}
	var eventID string
	err := tx.QueryRow(ctx,
		`SELECT event_id
		   FROM session_events
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND session_thread_id = $3
			    AND type = $4
			    AND tool_use_event_id = $5
		  ORDER BY sequence ASC
		  LIMIT 1`,
		workspaceID,
		sessionID,
		sessionThreadID,
		resultEventType,
		toolUseEventID,
	).Scan(&eventID)
	if dbconnect.IsNoRows(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return eventID, true, nil
}

func cancelPendingToolUseForTerminalResultTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, toolUseEventID string, resultEventID string, now time.Time) error {
	_, err := tx.Exec(ctx,
		`UPDATE session_pending_tool_uses
		    SET status = 'cancelled',
		        result_event_id = COALESCE(result_event_id, $5),
		        resolved_at = COALESCE(resolved_at, $6),
		        updated_at = $6
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND tool_use_event_id = $4
		    AND status IN ('pending', 'resolving')`,
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
	return nil
}

func RequestKindFromModelRequestStartProjection(projectionJSON string) (string, error) {
	var projection struct {
		RequestKind                   string `json:"request_kind"`
		ContextThroughMessageSequence *int64 `json:"context_through_message_sequence"`
	}
	if err := json.Unmarshal([]byte(projectionJSON), &projection); err != nil ||
		projection.RequestKind == "" || projection.ContextThroughMessageSequence == nil ||
		*projection.ContextThroughMessageSequence < 0 {
		return "", status.Error(codes.FailedPrecondition, "request start projection is malformed")
	}
	requestKind, err := NormalizeRequestKind(projection.RequestKind)
	if err != nil {
		return "", status.Error(codes.FailedPrecondition, "request start projection is malformed")
	}
	return requestKind, nil
}
