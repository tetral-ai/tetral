package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

type bridgeLoadContextTurnFacts struct {
	Events          []bridgeLoadContextTurnEvent  `json:"events"`
	InternalRepairs []bridgeLoadContextRepairFact `json:"internalRepairs"`
}

type bridgeLoadContextTurnEvent struct {
	EventID        string                         `json:"eventId"`
	EventSequence  int64                          `json:"eventSequence"`
	Type           string                         `json:"type"`
	ModelRequestID *string                        `json:"modelRequestId,omitempty"`
	RequestStart   *bridgeLoadContextRequestStart `json:"requestStart,omitempty"`
	RequestEnd     *bridgeLoadContextRequestEnd   `json:"requestEnd,omitempty"`
	ToolUse        *bridgeLoadContextToolUse      `json:"toolUse,omitempty"`
	ToolResult     *bridgeLoadContextToolResult   `json:"toolResult,omitempty"`
	Idle           *bridgeLoadContextIdle         `json:"idle,omitempty"`
	Failure        *bridgeLoadContextFailure      `json:"failure,omitempty"`
}

type bridgeLoadContextRequestStart struct {
	RequestKind                   string `json:"requestKind"`
	ContextThroughMessageSequence int64  `json:"contextThroughMessageSequence"`
}

type bridgeLoadContextRequestEnd struct {
	RequestStartEventID string  `json:"requestStartEventId"`
	IsError             bool    `json:"isError"`
	ErrorKind           *string `json:"errorKind,omitempty"`
	Rescheduled         bool    `json:"rescheduled"`
}

type bridgeLoadContextToolUse struct {
	ModelToolCallID string `json:"modelToolCallId"`
	ToolName        string `json:"toolName"`
}

type bridgeLoadContextToolResult struct {
	ModelToolCallID string `json:"modelToolCallId,omitempty"`
	ToolName        string `json:"toolName,omitempty"`
	Outcome         string `json:"outcome,omitempty"`
	RepairKey       string `json:"repairKey,omitempty"`
}

type bridgeLoadContextRepairFact struct {
	RepairKey       string `json:"repairKey"`
	RepairEventID   string `json:"repairEventId"`
	EventSequence   int64  `json:"eventSequence"`
	ModelRequestID  string `json:"modelRequestId"`
	ModelToolCallID string `json:"modelToolCallId"`
	ToolName        string `json:"toolName"`
}

type bridgeLoadContextIdle struct {
	StopReason string `json:"stopReason"`
}

type bridgeLoadContextFailure struct {
	ErrorType   string `json:"errorType"`
	RetryStatus string `json:"retryStatus"`
}

var bridgeLoadContextTurnEventTypes = []string{
	"session.status_running",
	"session.thread_status_running",
	"span.model_request_start",
	"span.model_request_end",
	"agent.tool_use",
	"agent.mcp_tool_use",
	"agent.tool_result",
	"agent.mcp_tool_result",
	"user.interrupt",
	childInterruptRequestedEventType,
	"session.error",
	"session.status_rescheduled",
	"session.thread_status_rescheduled",
	"session.status_idle",
	"session.thread_status_idle",
	"session.status_terminated",
	"session.thread_status_terminated",
}

// loadThreadTurnFactsTx projects only facts with independent durable identity.
// Runtime relates public Tools through their Tool Use references and internal
// repairs through their own stable event identity; Message mutation history is
// not a checkpoint input.
func loadThreadTurnFactsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	messages []bridgeLoadContextMessageDescriptor,
	durableTurnID *string,
) (bridgeLoadContextTurnFacts, error) {
	facts := bridgeLoadContextTurnFacts{
		Events:          make([]bridgeLoadContextTurnEvent, 0),
		InternalRepairs: make([]bridgeLoadContextRepairFact, 0),
	}
	eventFloor, err := loadContextTurnEventFloorTx(ctx, tx, scope, messages, durableTurnID)
	if err != nil {
		return facts, err
	}
	durableTurnEventID := ""
	if durableTurnID != nil {
		durableTurnEventID = *durableTurnID
	}
	rows, err := tx.Query(ctx,
		`SELECT event_id, sequence, type, model_request_id, payload_json, projection_json, runtime_write_id
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND (sequence >= $4 OR event_id = $5)
		    AND type IN (
		      'session.status_running',
		      'session.thread_status_running',
		      'span.model_request_start',
		      'span.model_request_end',
		      'agent.tool_use',
		      'agent.mcp_tool_use',
		      'agent.tool_result',
		      'agent.mcp_tool_result',
		      'user.interrupt',
		      'agent.thread_interrupt_requested',
		      'session.error',
		      'session.status_rescheduled',
		      'session.thread_status_rescheduled',
		      'session.status_idle',
		      'session.thread_status_idle',
		      'session.status_terminated',
		      'session.thread_status_terminated'
		    )
		  ORDER BY sequence ASC`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		eventFloor,
		durableTurnEventID,
	)
	if err != nil {
		return facts, err
	}
	type rawTurnEvent struct {
		eventID        string
		eventSequence  int64
		eventType      string
		modelRequestID sql.NullString
		payloadJSON    string
		projectionJSON string
		runtimeWriteID sql.NullString
	}
	rawEvents := make([]rawTurnEvent, 0)
	for rows.Next() {
		var raw rawTurnEvent
		if err := rows.Scan(&raw.eventID, &raw.eventSequence, &raw.eventType, &raw.modelRequestID, &raw.payloadJSON, &raw.projectionJSON, &raw.runtimeWriteID); err != nil {
			return facts, err
		}
		rawEvents = append(rawEvents, raw)
	}
	if err := rows.Err(); err != nil {
		return facts, err
	}
	if err := rows.Close(); err != nil {
		return facts, err
	}
	for _, raw := range rawEvents {
		if raw.eventType == childInterruptRequestedEventType {
			include, err := includeChildInterruptTurnFact(raw.payloadJSON)
			if err != nil {
				return facts, err
			}
			if !include {
				continue
			}
		}
		event, err := bridgeTurnEventFact(
			ctx, tx, scope, raw.eventID, raw.eventSequence, raw.eventType, raw.modelRequestID,
			raw.payloadJSON, raw.projectionJSON, raw.runtimeWriteID,
		)
		if err != nil {
			return facts, err
		}
		facts.Events = append(facts.Events, event)
	}
	repairs, err := loadContextRepairFactsTx(ctx, tx, scope, eventFloor)
	if err != nil {
		return facts, err
	}
	facts.InternalRepairs = repairs
	return facts, nil
}

// Child-control admission stores its complete durable census as events, but
// only pending_control is Runtime work. Cold projection validates the closed
// disposition set before omitting terminal census evidence from the hot turn.
func includeChildInterruptTurnFact(payloadJSON string) (bool, error) {
	var payload struct {
		Disposition string `json:"disposition"`
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return false, status.Error(codes.FailedPrecondition, "child interrupt disposition is malformed")
	}
	switch payload.Disposition {
	case "pending_control":
		return true, nil
	case "already_closed", "preserved_failed", "preserved_terminated":
		return false, nil
	default:
		return false, status.Error(codes.FailedPrecondition, "child interrupt disposition is malformed")
	}
}

func loadContextTurnEventFloorTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	messages []bridgeLoadContextMessageDescriptor,
	durableTurnID *string,
) (int64, error) {
	var compactionFloor int64
	for _, message := range messages {
		if message.Kind != "compaction" {
			continue
		}
		if message.SourceEventID == nil || *message.SourceEventID == "" {
			return 0, status.Error(codes.FailedPrecondition, "compaction message has no source event identity")
		}
		var sequence int64
		err := tx.QueryRow(ctx,
			`SELECT request_start.sequence
			   FROM session_events AS compacted
			   JOIN session_events AS request_start
			     ON request_start.workspace_id = compacted.workspace_id
			    AND request_start.session_id = compacted.session_id
			    AND request_start.session_thread_id = compacted.session_thread_id
			    AND request_start.model_request_id = compacted.model_request_id
			    AND request_start.type = 'span.model_request_start'
			  WHERE compacted.workspace_id = $1
			    AND compacted.session_id = $2
			    AND compacted.session_thread_id = $3
			    AND compacted.event_id = $4
			    AND compacted.type = 'agent.thread_context_compacted'`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), *message.SourceEventID,
		).Scan(&sequence)
		if dbconnect.IsNoRows(err) {
			return 0, status.Error(codes.FailedPrecondition, "compaction message has no Request Start")
		}
		if err != nil {
			return 0, err
		}
		compactionFloor = sequence
		break
	}
	var durableTurnFloor int64
	if durableTurnID != nil {
		err := tx.QueryRow(ctx,
			`SELECT sequence
			   FROM session_events
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND session_thread_id = $3
			    AND event_id = $4
			    AND type IN ('session.status_running', 'session.thread_status_running')`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), *durableTurnID,
		).Scan(&durableTurnFloor)
		if dbconnect.IsNoRows(err) {
			return 0, status.Error(codes.FailedPrecondition, "durable turn has no running event")
		}
		if err != nil {
			return 0, err
		}
	} else {
		err := tx.QueryRow(ctx,
			`SELECT sequence
			   FROM session_events
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND session_thread_id = $3
			    AND type IN ('session.status_running', 'session.thread_status_running')
			  ORDER BY sequence DESC
			  LIMIT 1`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(),
		).Scan(&durableTurnFloor)
		if err != nil && !dbconnect.IsNoRows(err) {
			return 0, err
		}
	}
	eventFloor := durableTurnFloor
	var unresolvedRequestFloor sql.NullInt64
	err := tx.QueryRow(ctx,
		`SELECT MIN(request_start.sequence)
		   FROM session_events tool_use
		   JOIN session_events request_start
		     ON request_start.workspace_id=tool_use.workspace_id
		    AND request_start.session_id=tool_use.session_id
		    AND request_start.session_thread_id=tool_use.session_thread_id
		    AND request_start.model_request_id=tool_use.model_request_id
		    AND request_start.type='span.model_request_start'
		  WHERE tool_use.workspace_id=$1 AND tool_use.session_id=$2 AND tool_use.session_thread_id=$3
		    AND tool_use.type IN ('agent.tool_use','agent.mcp_tool_use')
		    AND request_start.sequence >= $4
		    AND NOT EXISTS (
		      SELECT 1 FROM session_events tool_result
		       WHERE tool_result.workspace_id=tool_use.workspace_id
		         AND tool_result.session_id=tool_use.session_id
		         AND tool_result.session_thread_id=tool_use.session_thread_id
		         AND tool_result.type IN ('agent.tool_result','agent.mcp_tool_result')
		         AND COALESCE(
		               tool_result.payload_json::jsonb ->> 'tool_use_event_id',
		               tool_result.payload_json::jsonb ->> 'tool_use_id',
		               tool_result.payload_json::jsonb ->> 'mcp_tool_use_id'
		             )=tool_use.event_id
		    )`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), compactionFloor,
	).Scan(&unresolvedRequestFloor)
	if err != nil {
		return 0, err
	}
	if unresolvedRequestFloor.Valid && unresolvedRequestFloor.Int64 < eventFloor {
		eventFloor = unresolvedRequestFloor.Int64
	}
	if eventFloor < compactionFloor {
		eventFloor = compactionFloor
	}
	return eventFloor, nil
}

func bridgeTurnEventFact(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	eventID string,
	eventSequence int64,
	eventType string,
	modelRequestID sql.NullString,
	payloadJSON string,
	projectionJSON string,
	runtimeWriteID sql.NullString,
) (bridgeLoadContextTurnEvent, error) {
	event := bridgeLoadContextTurnEvent{EventID: eventID, EventSequence: eventSequence, Type: eventType}
	if modelRequestID.Valid {
		event.ModelRequestID = &modelRequestID.String
	}
	switch eventType {
	case "span.model_request_start":
		if !modelRequestID.Valid {
			return event, status.Error(codes.FailedPrecondition, "request start fact has no model request identity")
		}
		var projection struct {
			ContextThroughMessageSequence *int64 `json:"context_through_message_sequence"`
			RequestKind                   string `json:"request_kind"`
		}
		if err := json.Unmarshal([]byte(projectionJSON), &projection); err != nil ||
			projection.ContextThroughMessageSequence == nil || projection.RequestKind == "" {
			return event, status.Error(codes.FailedPrecondition, "request start projection is malformed")
		}
		if _, err := normalizeRequestKind(projection.RequestKind); err != nil {
			return event, status.Error(codes.FailedPrecondition, "request start projection is malformed")
		}
		event.RequestStart = &bridgeLoadContextRequestStart{
			RequestKind:                   projection.RequestKind,
			ContextThroughMessageSequence: *projection.ContextThroughMessageSequence,
		}
	case "span.model_request_end":
		if !modelRequestID.Valid {
			return event, status.Error(codes.FailedPrecondition, "request end fact has no model request identity")
		}
		var payload struct {
			RequestStartEventID string  `json:"model_request_start_id"`
			IsError             *bool   `json:"is_error"`
			ErrorKind           *string `json:"error_kind"`
		}
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil || payload.RequestStartEventID == "" || payload.IsError == nil {
			return event, status.Error(codes.FailedPrecondition, "request end projection is malformed")
		}
		rescheduled, err := loadContextRequestEndRescheduledTx(ctx, tx, scope, modelRequestID.String)
		if err != nil {
			return event, err
		}
		event.RequestEnd = &bridgeLoadContextRequestEnd{
			RequestStartEventID: payload.RequestStartEventID,
			IsError:             *payload.IsError,
			ErrorKind:           payload.ErrorKind,
			Rescheduled:         rescheduled,
		}
	case "agent.tool_use", "agent.mcp_tool_use":
		if !modelRequestID.Valid {
			return event, status.Error(codes.FailedPrecondition, "tool use fact has no model request identity")
		}
		toolUse, err := bridgeTurnToolUseFact(payloadJSON, projectionJSON)
		if err != nil {
			return event, err
		}
		event.ToolUse = toolUse
	case "agent.tool_result", "agent.mcp_tool_result":
		result, resultModelRequestID, err := bridgeTurnToolResultFact(ctx, tx, scope, eventType, payloadJSON, projectionJSON, runtimeWriteID)
		if err != nil {
			return event, err
		}
		if !modelRequestID.Valid && resultModelRequestID != "" {
			modelRequestID = sql.NullString{String: resultModelRequestID, Valid: true}
			event.ModelRequestID = &resultModelRequestID
		}
		if !modelRequestID.Valid {
			return event, status.Error(codes.FailedPrecondition, "tool result fact has no model request identity")
		}
		event.ToolResult = result
	case "session.status_idle", "session.thread_status_idle":
		var payload struct {
			StopReason struct {
				Type string `json:"type"`
			} `json:"stop_reason"`
		}
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil || payload.StopReason.Type == "" {
			return event, status.Error(codes.FailedPrecondition, "idle projection is malformed")
		}
		event.Idle = &bridgeLoadContextIdle{StopReason: payload.StopReason.Type}
	case "session.error":
		var payload struct {
			Error struct {
				Type        string `json:"type"`
				RetryStatus struct {
					Type string `json:"type"`
				} `json:"retry_status"`
			} `json:"error"`
		}
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil || payload.Error.Type == "" ||
			(payload.Error.RetryStatus.Type != "retrying" && payload.Error.RetryStatus.Type != "exhausted" && payload.Error.RetryStatus.Type != "terminal") {
			return event, status.Error(codes.FailedPrecondition, "failure projection is malformed")
		}
		event.Failure = &bridgeLoadContextFailure{ErrorType: payload.Error.Type, RetryStatus: payload.Error.RetryStatus.Type}
	case "session.status_rescheduled", "session.thread_status_rescheduled":
		event.ModelRequestID = nil
	default:
		if !bridgeTurnEventTypeAllowed(eventType) {
			return event, status.Error(codes.FailedPrecondition, "turn fact type is not recognized")
		}
		if modelRequestID.Valid {
			return event, status.Error(codes.FailedPrecondition, "turn fact carries an unsupported model request identity")
		}
	}
	return event, nil
}

func bridgeTurnEventTypeAllowed(eventType string) bool {
	for _, candidate := range bridgeLoadContextTurnEventTypes {
		if candidate == eventType {
			return true
		}
	}
	return false
}

func loadContextRequestEndRescheduledTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, modelRequestID string) (bool, error) {
	var rescheduled bool
	err := tx.QueryRow(ctx,
		`SELECT EXISTS (
		   SELECT 1 FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND model_request_id = $4
		    AND type IN ('session.status_rescheduled', 'session.thread_status_rescheduled')
		)`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), modelRequestID,
	).Scan(&rescheduled)
	if err != nil {
		return false, err
	}
	return rescheduled, nil
}

func bridgeTurnToolUseFact(payloadJSON string, projectionJSON string) (*bridgeLoadContextToolUse, error) {
	var payload runtimeToolUseEventPayload
	var projection struct {
		ModelToolCallID string `json:"model_tool_call_id"`
	}
	if json.Unmarshal([]byte(payloadJSON), &payload) != nil || json.Unmarshal([]byte(projectionJSON), &projection) != nil ||
		payload.Name == "" || projection.ModelToolCallID == "" {
		return nil, status.Error(codes.FailedPrecondition, "tool use direct facts are malformed")
	}
	return &bridgeLoadContextToolUse{ModelToolCallID: projection.ModelToolCallID, ToolName: payload.Name}, nil
}

func bridgeTurnToolResultFact(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	eventType string,
	payloadJSON string,
	projectionJSON string,
	runtimeWriteID sql.NullString,
) (*bridgeLoadContextToolResult, string, error) {
	var payload struct {
		ToolUseEventID  string `json:"tool_use_event_id"`
		ToolUseID       string `json:"tool_use_id"`
		MCPToolUseID    string `json:"mcp_tool_use_id"`
		RepairKind      string `json:"repair_kind"`
		ModelToolCallID string `json:"model_tool_call_id"`
		ToolName        string `json:"tool_name"`
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return nil, "", status.Error(codes.FailedPrecondition, "tool result projection is malformed")
	}
	if payload.RepairKind != "" {
		if eventType != "agent.tool_result" || payload.RepairKind != "invalid_tool" || !runtimeWriteID.Valid || runtimeWriteID.String == "" ||
			payload.ModelToolCallID == "" || payload.ToolName == "" {
			return nil, "", status.Error(codes.FailedPrecondition, "internal tool repair projection is malformed")
		}
		return &bridgeLoadContextToolResult{RepairKey: runtimeWriteID.String}, "", nil
	}
	toolUseEventID, err := durableToolResultUseEventID(eventType, durableToolResultEventPayload{
		ToolUseEventID: payload.ToolUseEventID,
		ToolUseID:      payload.ToolUseID,
		MCPToolUseID:   payload.MCPToolUseID,
	})
	if err != nil {
		return nil, "", err
	}
	var modelRequestID, toolUsePayloadJSON, toolUseProjectionJSON string
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(model_request_id, ''), payload_json, projection_json
		   FROM session_events
		  WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
		    AND event_id=$4 AND type IN ('agent.tool_use','agent.mcp_tool_use')`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), toolUseEventID,
	).Scan(&modelRequestID, &toolUsePayloadJSON, &toolUseProjectionJSON); err != nil {
		if dbconnect.IsNoRows(err) {
			return nil, "", status.Error(codes.FailedPrecondition, "tool result has no durable Tool Use")
		}
		return nil, "", err
	}
	toolUse, err := bridgeTurnToolUseFact(toolUsePayloadJSON, toolUseProjectionJSON)
	if err != nil {
		return nil, "", err
	}
	var projection struct {
		State string `json:"state"`
	}
	if err := json.Unmarshal([]byte(projectionJSON), &projection); err != nil ||
		(projection.State != "completed" && projection.State != "error" && projection.State != "cancelled") {
		return nil, "", status.Error(codes.FailedPrecondition, "tool result direct facts are malformed")
	}
	return &bridgeLoadContextToolResult{ModelToolCallID: toolUse.ModelToolCallID, ToolName: toolUse.ToolName, Outcome: projection.State}, modelRequestID, nil
}

func loadContextRepairFactsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	eventFloor int64,
) ([]bridgeLoadContextRepairFact, error) {
	rows, err := tx.Query(ctx,
		`SELECT runtime_write_id, payload_json, event_id, sequence, model_request_id
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND type = 'agent.tool_result'
		    AND payload_json::jsonb ->> 'repair_kind' = 'invalid_tool'
		    AND sequence >= $4
		  ORDER BY sequence ASC`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		eventFloor,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	facts := make([]bridgeLoadContextRepairFact, 0)
	seen := make(map[string]struct{})
	for rows.Next() {
		var fact bridgeLoadContextRepairFact
		var payloadJSON string
		var modelRequestID sql.NullString
		if err := rows.Scan(
			&fact.RepairKey,
			&payloadJSON,
			&fact.RepairEventID,
			&fact.EventSequence,
			&modelRequestID,
		); err != nil {
			return nil, err
		}
		if fact.RepairKey == "" || fact.RepairEventID == "" ||
			fact.EventSequence <= 0 || !modelRequestID.Valid || modelRequestID.String == "" {
			return nil, status.Error(codes.FailedPrecondition, "internal repair direct reference is malformed")
		}
		if _, duplicate := seen[fact.RepairKey]; duplicate {
			return nil, status.Error(codes.FailedPrecondition, "internal repair direct reference is ambiguous")
		}
		seen[fact.RepairKey] = struct{}{}
		fact.ModelRequestID = modelRequestID.String
		payload, err := decodeRuntimeDeclarationObject(payloadJSON)
		if err != nil || requireRuntimeObjectFields(
			payload,
			[]string{"type", "model_tool_call_id", "tool_name", "repair_kind"},
			[]string{"type", "model_tool_call_id", "tool_name", "repair_kind"},
		) != nil || payload["type"] != "agent.tool_result" || payload["repair_kind"] != "invalid_tool" {
			return nil, status.Error(codes.FailedPrecondition, "internal repair direct event is malformed")
		}
		fact.ModelToolCallID, _ = payload["model_tool_call_id"].(string)
		fact.ToolName, _ = payload["tool_name"].(string)
		if fact.ModelToolCallID == "" || fact.ToolName == "" {
			return nil, status.Error(codes.FailedPrecondition, "internal repair direct event is incomplete")
		}
		facts = append(facts, fact)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return facts, nil
}
