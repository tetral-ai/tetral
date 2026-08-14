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

type bridgeLoadContextToolUse struct{}

type bridgeLoadContextToolResult struct {
	ToolUseEventID string `json:"toolUseEventId,omitempty"`
	RepairKey      string `json:"repairKey,omitempty"`
}

type bridgeLoadContextRepairFact struct {
	RepairKey      string `json:"repairKey"`
	MessageID      string `json:"messageId"`
	RepairEventID  string `json:"repairEventId"`
	EventSequence  int64  `json:"eventSequence"`
	ModelRequestID string `json:"modelRequestId"`
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
// repairs through repair_key/runtime_write_id; Message mutation history is not
// a checkpoint input.
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
	if durableTurnID == nil {
		return compactionFloor, nil
	}
	var durableTurnFloor int64
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
	return compactionFloor, nil
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
		event.ToolUse = &bridgeLoadContextToolUse{}
	case "agent.tool_result", "agent.mcp_tool_result":
		result, err := bridgeTurnToolResultFact(eventType, payloadJSON, runtimeWriteID)
		if err != nil {
			return event, err
		}
		if !modelRequestID.Valid && result.ToolUseEventID != "" {
			if err := tx.QueryRow(ctx,
				`SELECT model_request_id
				   FROM session_events
				  WHERE workspace_id = $1
				    AND session_id = $2
				    AND session_thread_id = $3
				    AND event_id = $4
				    AND type IN ('agent.tool_use', 'agent.mcp_tool_use')`,
				scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), result.ToolUseEventID,
			).Scan(&modelRequestID); err != nil {
				if dbconnect.IsNoRows(err) {
					return event, status.Error(codes.FailedPrecondition, "tool result fact has no model request identity")
				}
				return event, err
			}
			if !modelRequestID.Valid || modelRequestID.String == "" {
				return event, status.Error(codes.FailedPrecondition, "tool result fact has no model request identity")
			}
			event.ModelRequestID = &modelRequestID.String
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
	var receiptJSON string
	err := tx.QueryRow(ctx,
		`SELECT COALESCE(receipt_json, '')
		   FROM session_bridge_operations
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND operation = 'write_request_end'
		    AND source_kind = 'model_request'
		    AND idempotency_key = $4`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), modelRequestID,
	).Scan(&receiptJSON)
	if dbconnect.IsNoRows(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	receipt, err := unmarshalDeclarationReceipt(receiptJSON)
	if err != nil {
		return false, status.Error(codes.FailedPrecondition, "request end receipt is malformed")
	}
	return receipt.GetRequestReschedule().GetDisposition() == bridgev1.RequestRescheduleDisposition_REQUEST_RESCHEDULE_DISPOSITION_ACCEPTED, nil
}

func bridgeTurnToolResultFact(
	eventType string,
	payloadJSON string,
	runtimeWriteID sql.NullString,
) (*bridgeLoadContextToolResult, error) {
	var payload struct {
		ToolUseEventID string `json:"tool_use_event_id"`
		ToolUseID      string `json:"tool_use_id"`
		MCPToolUseID   string `json:"mcp_tool_use_id"`
		RepairKind     string `json:"repair_kind"`
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return nil, status.Error(codes.FailedPrecondition, "tool result projection is malformed")
	}
	if payload.RepairKind != "" {
		if eventType != "agent.tool_result" || payload.RepairKind != "invalid_tool" || !runtimeWriteID.Valid || runtimeWriteID.String == "" {
			return nil, status.Error(codes.FailedPrecondition, "internal tool repair projection is malformed")
		}
		return &bridgeLoadContextToolResult{
			RepairKey: runtimeWriteID.String,
		}, nil
	}
	toolUseEventID, err := durableToolResultUseEventID(eventType, durableToolResultEventPayload{
		ToolUseEventID: payload.ToolUseEventID,
		ToolUseID:      payload.ToolUseID,
		MCPToolUseID:   payload.MCPToolUseID,
	})
	if err != nil {
		return nil, err
	}
	return &bridgeLoadContextToolResult{ToolUseEventID: toolUseEventID}, nil
}

func loadContextRepairFactsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	eventFloor int64,
) ([]bridgeLoadContextRepairFact, error) {
	rows, err := tx.Query(ctx,
		`SELECT m.repair_key, m.message_id, e.event_id, e.sequence, e.model_request_id
		   FROM session_messages AS m
		   JOIN session_events AS e
		     ON e.workspace_id = m.workspace_id
		    AND e.session_id = m.session_id
		    AND e.session_thread_id = m.session_thread_id
		    AND e.runtime_write_id = m.repair_key
		    AND e.type = 'agent.tool_result'
		  WHERE m.workspace_id = $1
		    AND m.session_id = $2
		    AND m.session_thread_id = $3
		    AND m.repair_key IS NOT NULL
		    AND e.sequence >= $4
		  ORDER BY e.sequence ASC`,
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
		var modelRequestID sql.NullString
		if err := rows.Scan(
			&fact.RepairKey,
			&fact.MessageID,
			&fact.RepairEventID,
			&fact.EventSequence,
			&modelRequestID,
		); err != nil {
			return nil, err
		}
		if fact.RepairKey == "" || fact.MessageID == "" || fact.RepairEventID == "" ||
			fact.EventSequence <= 0 || !modelRequestID.Valid || modelRequestID.String == "" {
			return nil, status.Error(codes.FailedPrecondition, "internal repair direct reference is malformed")
		}
		if _, duplicate := seen[fact.RepairKey]; duplicate {
			return nil, status.Error(codes.FailedPrecondition, "internal repair direct reference is ambiguous")
		}
		seen[fact.RepairKey] = struct{}{}
		fact.ModelRequestID = modelRequestID.String
		facts = append(facts, fact)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return facts, nil
}
