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
	RequestStartEventID      string                              `json:"requestStartEventId"`
	IsError                  bool                                `json:"isError"`
	ErrorKind                *string                             `json:"errorKind,omitempty"`
	ProviderContextRetention bridgeLoadContextProviderRetention  `json:"providerContextRetention"`
	Reschedule               *bridgeLoadContextRequestReschedule `json:"reschedule,omitempty"`
}

type bridgeLoadContextProviderRetention struct {
	Disposition              string   `json:"disposition"`
	AssistantMessageSequence *int64   `json:"assistantMessageSequence,omitempty"`
	ToolUseEventIDs          []string `json:"toolUseEventIds"`
	RepairEventIDs           []string `json:"repairEventIds"`
}

type bridgeLoadContextRequestReschedule struct {
	Attempt            int64  `json:"attempt"`
	EffectiveDeadline  string `json:"effectiveDeadline"`
	ProviderAttempts   int64  `json:"providerAttempts"`
	CompactionAttempts int64  `json:"compactionAttempts"`
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

// Provider Messages select their own compacted/retained window. This query is
// the independent Active Turn read: every row is reached through a current
// lifecycle boundary or a direct Request/Tool identity already selected by an
// owning durable relation.
const loadContextTurnEventsSQL = `WITH turn_root AS MATERIALIZED (
		SELECT event_id, sequence
		  FROM session_events
		 WHERE workspace_id = $1
		   AND session_id = $2
		   AND session_thread_id = $3
		   AND type IN ('session.status_running', 'session.thread_status_running')
		   AND $4 <> ''
		   AND event_id = $4
		 ORDER BY sequence DESC
		 LIMIT 1
	), current_running AS MATERIALIZED (
		SELECT event_id, sequence
		  FROM session_events
		 WHERE workspace_id = $1
		   AND session_id = $2
		   AND session_thread_id = $3
		   AND type IN ('session.status_running', 'session.thread_status_running')
		   AND sequence >= COALESCE((SELECT sequence FROM turn_root), 9223372036854775807)
		 ORDER BY sequence DESC
		 LIMIT 1
	), previous_lifecycle AS MATERIALIZED (
		SELECT event_id, sequence
		  FROM session_events
		 WHERE workspace_id = $1
		   AND session_id = $2
		   AND session_thread_id = $3
		   AND type IN (
		     'session.status_running', 'session.thread_status_running',
		     'session.status_rescheduled', 'session.thread_status_rescheduled',
		     'session.status_idle', 'session.thread_status_idle',
		     'session.status_terminated', 'session.thread_status_terminated'
		   )
		   AND sequence < COALESCE((SELECT sequence FROM current_running), 0)
		 ORDER BY sequence DESC
		 LIMIT 1
	), current_reschedule AS MATERIALIZED (
		SELECT event_id, model_request_id
		  FROM session_events
		 WHERE workspace_id = $1
		   AND session_id = $2
		   AND session_thread_id = $3
		   AND type IN ('session.status_rescheduled', 'session.thread_status_rescheduled')
		   AND model_request_id IS NOT NULL
		   AND sequence >= COALESCE((SELECT sequence FROM turn_root), 9223372036854775807)
		 ORDER BY sequence DESC
		 LIMIT 1
	),
	open_request AS MATERIALIZED (
		SELECT request_start.sequence, request_start.model_request_id
		  FROM session_events request_start
		 WHERE request_start.workspace_id = $1
		   AND request_start.session_id = $2
		   AND request_start.session_thread_id = $3
		   AND request_start.type = 'span.model_request_start'
		   AND request_start.model_request_id IS NOT NULL
		   AND NOT EXISTS (
		     SELECT 1 FROM session_events request_end
		      WHERE request_end.workspace_id = request_start.workspace_id
		        AND request_end.session_id = request_start.session_id
		        AND request_end.session_thread_id = request_start.session_thread_id
		        AND request_end.model_request_id = request_start.model_request_id
		        AND request_end.type = 'span.model_request_end'
		   )
		 ORDER BY request_start.sequence DESC
		 LIMIT 1
	),
	selected_request_input AS MATERIALIZED (
		SELECT jsonb_array_elements_text($5::jsonb) AS model_request_id
	),
	pending_tools AS MATERIALIZED (
		SELECT jsonb_array_elements_text($6::jsonb) AS event_id
	),
	pending_interrupts AS MATERIALIZED (
		SELECT jsonb_array_elements_text(inbox.event_ids_json::jsonb) AS event_id
		  FROM session_runtime_inbox inbox
		 WHERE inbox.workspace_id = $1
		   AND inbox.session_id = $2
		   AND inbox.session_thread_id = $3
		   AND inbox.input_kind = 'interrupt_control'
		   AND inbox.status IN ('queued', 'delivering', 'accepted')
	),
	retained_ends AS MATERIALIZED (
		SELECT event_id, model_request_id, payload_json::jsonb AS payload
		  FROM session_events
		 WHERE workspace_id = $1
		   AND session_id = $2
		   AND session_thread_id = $3
		   AND type = 'span.model_request_end'
		   AND model_request_id IS NOT NULL
		   AND model_request_id IN (
		     SELECT model_request_id FROM selected_request_input
		     UNION SELECT model_request_id FROM open_request
		     UNION SELECT model_request_id FROM current_reschedule
		   )
	),
	retained_requests AS MATERIALIZED (
		SELECT model_request_id FROM retained_ends
		UNION
		SELECT model_request_id FROM selected_request_input
		UNION
		SELECT model_request_id FROM open_request
		UNION
		SELECT model_request_id FROM current_reschedule
	),
	retained_tools AS MATERIALIZED (
		SELECT jsonb_array_elements_text(CASE
		         WHEN jsonb_typeof(payload #> '{provider_context_retention,tool_use_event_ids}') = 'array'
		         THEN payload #> '{provider_context_retention,tool_use_event_ids}'
		         ELSE '[]'::jsonb
		       END) AS event_id
		  FROM retained_ends
		UNION
		SELECT event_id FROM pending_tools
	),
	retained_repairs AS MATERIALIZED (
		SELECT jsonb_array_elements_text(CASE
		         WHEN jsonb_typeof(payload #> '{provider_context_retention,repair_event_ids}') = 'array'
		         THEN payload #> '{provider_context_retention,repair_event_ids}'
		         ELSE '[]'::jsonb
		       END) AS event_id
		  FROM retained_ends
	)
	SELECT event_id, sequence, type, model_request_id, payload_json, projection_json, runtime_write_id
	  FROM session_events event
	 WHERE workspace_id = $1
	   AND session_id = $2
	   AND session_thread_id = $3
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
	   AND (
	     event_id IN (SELECT event_id FROM turn_root)
	     OR event_id IN (SELECT event_id FROM current_running)
	     OR event_id IN (SELECT event_id FROM previous_lifecycle)
	     OR event_id IN (SELECT event_id FROM current_reschedule)
	     OR event_id IN (SELECT event_id FROM retained_ends)
	     OR (type = 'span.model_request_start' AND model_request_id IN (SELECT model_request_id FROM retained_requests))
	     OR (type IN ('agent.tool_use', 'agent.mcp_tool_use') AND event_id IN (SELECT event_id FROM retained_tools))
	     OR (type IN ('agent.tool_result', 'agent.mcp_tool_result') AND COALESCE(
	          payload_json::jsonb ->> 'tool_use_event_id',
	          payload_json::jsonb ->> 'tool_use_id',
	          payload_json::jsonb ->> 'mcp_tool_use_id'
	        ) IN (SELECT event_id FROM retained_tools))
	     OR (type = 'agent.tool_result' AND event_id IN (SELECT event_id FROM retained_repairs))
	     OR (type IN ('user.interrupt', 'agent.thread_interrupt_requested') AND event_id IN (SELECT event_id FROM pending_interrupts))
	   )
	 ORDER BY sequence ASC`

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
	pendingModelRequestIDs []string,
	pendingToolUseEventIDs []string,
) (bridgeLoadContextTurnFacts, error) {
	facts := bridgeLoadContextTurnFacts{
		Events:          make([]bridgeLoadContextTurnEvent, 0),
		InternalRepairs: make([]bridgeLoadContextRepairFact, 0),
	}
	durableTurnEventID := ""
	if durableTurnID != nil {
		durableTurnEventID = *durableTurnID
	}
	selectedModelRequestIDs := append(make([]string, 0, len(pendingModelRequestIDs)+len(messages)), pendingModelRequestIDs...)
	seenModelRequests := make(map[string]struct{}, len(selectedModelRequestIDs))
	for _, modelRequestID := range selectedModelRequestIDs {
		seenModelRequests[modelRequestID] = struct{}{}
	}
	for _, message := range messages {
		if message.ModelRequestID == nil || *message.ModelRequestID == "" {
			continue
		}
		if _, exists := seenModelRequests[*message.ModelRequestID]; exists {
			continue
		}
		seenModelRequests[*message.ModelRequestID] = struct{}{}
		selectedModelRequestIDs = append(selectedModelRequestIDs, *message.ModelRequestID)
	}
	selectedModelRequestIDsJSON, _ := json.Marshal(selectedModelRequestIDs)
	pendingToolUseEventIDsJSON, _ := json.Marshal(append([]string{}, pendingToolUseEventIDs...))
	rows, err := tx.Query(ctx,
		loadContextTurnEventsSQL,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		durableTurnEventID,
		string(selectedModelRequestIDsJSON),
		string(pendingToolUseEventIDsJSON),
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
	seenRepairKeys := make(map[string]struct{})
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
		if event.ToolResult != nil && event.ToolResult.RepairKey != "" {
			repair, err := bridgeRepairFactFromTurnEvent(
				raw.eventID,
				raw.eventSequence,
				raw.modelRequestID,
				raw.payloadJSON,
				raw.runtimeWriteID,
			)
			if err != nil {
				return facts, err
			}
			if _, duplicate := seenRepairKeys[repair.RepairKey]; duplicate {
				return facts, status.Error(codes.FailedPrecondition, "internal repair direct reference is ambiguous")
			}
			seenRepairKeys[repair.RepairKey] = struct{}{}
			facts.InternalRepairs = append(facts.InternalRepairs, repair)
		}
	}
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

func bridgeRepairFactFromTurnEvent(
	eventID string,
	eventSequence int64,
	modelRequestID sql.NullString,
	payloadJSON string,
	runtimeWriteID sql.NullString,
) (bridgeLoadContextRepairFact, error) {
	if eventID == "" || eventSequence <= 0 || !modelRequestID.Valid || modelRequestID.String == "" ||
		!runtimeWriteID.Valid || runtimeWriteID.String == "" {
		return bridgeLoadContextRepairFact{}, status.Error(codes.FailedPrecondition, "internal repair direct reference is malformed")
	}
	payload, err := decodeRuntimeDeclarationObject(payloadJSON)
	if err != nil || requireRuntimeObjectFields(
		payload,
		[]string{"type", "model_tool_call_id", "tool_name", "repair_kind"},
		[]string{"type", "model_tool_call_id", "tool_name", "repair_kind"},
	) != nil || payload["type"] != "agent.tool_result" || payload["repair_kind"] != "invalid_tool" {
		return bridgeLoadContextRepairFact{}, status.Error(codes.FailedPrecondition, "internal repair direct event is malformed")
	}
	modelToolCallID, _ := payload["model_tool_call_id"].(string)
	toolName, _ := payload["tool_name"].(string)
	if modelToolCallID == "" || toolName == "" {
		return bridgeLoadContextRepairFact{}, status.Error(codes.FailedPrecondition, "internal repair direct event is incomplete")
	}
	return bridgeLoadContextRepairFact{
		RepairKey:       runtimeWriteID.String,
		RepairEventID:   eventID,
		EventSequence:   eventSequence,
		ModelRequestID:  modelRequestID.String,
		ModelToolCallID: modelToolCallID,
		ToolName:        toolName,
	}, nil
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
			RequestStartEventID      string  `json:"model_request_start_id"`
			IsError                  *bool   `json:"is_error"`
			ErrorKind                *string `json:"error_kind"`
			ProviderContextRetention struct {
				Disposition              string   `json:"disposition"`
				AssistantMessageSequence *int64   `json:"assistant_message_sequence"`
				ToolUseEventIDs          []string `json:"tool_use_event_ids"`
				RepairEventIDs           []string `json:"repair_event_ids"`
			} `json:"provider_context_retention"`
		}
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil || payload.RequestStartEventID == "" || payload.IsError == nil || payload.ProviderContextRetention.Disposition == "" {
			return event, status.Error(codes.FailedPrecondition, "request end projection is malformed")
		}
		reschedule, err := loadContextRequestEndRescheduleTx(ctx, tx, scope, modelRequestID.String)
		if err != nil {
			return event, err
		}
		if payload.ProviderContextRetention.ToolUseEventIDs == nil {
			payload.ProviderContextRetention.ToolUseEventIDs = make([]string, 0)
		}
		if payload.ProviderContextRetention.RepairEventIDs == nil {
			payload.ProviderContextRetention.RepairEventIDs = make([]string, 0)
		}
		event.RequestEnd = &bridgeLoadContextRequestEnd{
			RequestStartEventID: payload.RequestStartEventID,
			IsError:             *payload.IsError,
			ErrorKind:           payload.ErrorKind,
			ProviderContextRetention: bridgeLoadContextProviderRetention{
				Disposition:              payload.ProviderContextRetention.Disposition,
				AssistantMessageSequence: payload.ProviderContextRetention.AssistantMessageSequence,
				ToolUseEventIDs:          payload.ProviderContextRetention.ToolUseEventIDs,
				RepairEventIDs:           payload.ProviderContextRetention.RepairEventIDs,
			},
			Reschedule: reschedule,
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

func loadContextRequestEndRescheduleTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, modelRequestID string) (*bridgeLoadContextRequestReschedule, error) {
	var rescheduleProjectionJSON string
	err := tx.QueryRow(ctx,
		`SELECT projection_json
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND model_request_id = $4
		    AND type IN ('session.status_rescheduled', 'session.thread_status_rescheduled')
		  ORDER BY sequence DESC
		  LIMIT 1`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), modelRequestID,
	).Scan(&rescheduleProjectionJSON)
	if dbconnect.IsNoRows(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var projection struct {
		Attempt            int64  `json:"attempt"`
		EffectiveDeadline  string `json:"effective_deadline"`
		ProviderAttempts   int64  `json:"provider_attempts"`
		CompactionAttempts int64  `json:"compaction_attempts"`
	}
	if json.Unmarshal([]byte(rescheduleProjectionJSON), &projection) != nil ||
		projection.Attempt <= 0 || projection.EffectiveDeadline == "" ||
		projection.ProviderAttempts < 0 || projection.CompactionAttempts < 0 {
		return nil, status.Error(codes.FailedPrecondition, "rescheduled request end projection is malformed")
	}
	var receiptJSON string
	err = tx.QueryRow(ctx,
		`SELECT receipt_json
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
		return nil, status.Error(codes.FailedPrecondition, "rescheduled request end has no durable receipt")
	}
	if err != nil {
		return nil, err
	}
	facts, err := unmarshalRequestEndReplay(receiptJSON)
	if err != nil || facts.Disposition != "rescheduled" || facts.EffectiveDeadline == "" {
		return nil, status.Error(codes.FailedPrecondition, "rescheduled request end receipt is malformed")
	}
	var requestKind string
	err = tx.QueryRow(ctx,
		`SELECT projection_json::jsonb ->> 'request_kind'
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND model_request_id = $4
		    AND type = 'span.model_request_start'`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), modelRequestID,
	).Scan(&requestKind)
	if err != nil {
		return nil, err
	}
	attempt := projection.ProviderAttempts
	if requestKind == requestKindCompactionSummary {
		attempt = projection.CompactionAttempts
	}
	if attempt != projection.Attempt || facts.EffectiveDeadline != projection.EffectiveDeadline {
		return nil, status.Error(codes.FailedPrecondition, "rescheduled request end facts conflict")
	}
	return &bridgeLoadContextRequestReschedule{
		Attempt:            attempt,
		EffectiveDeadline:  facts.EffectiveDeadline,
		ProviderAttempts:   projection.ProviderAttempts,
		CompactionAttempts: projection.CompactionAttempts,
	}, nil
}

func bridgeTurnToolUseFact(_ string, projectionJSON string) (*bridgeLoadContextToolUse, error) {
	var projection struct {
		ModelToolCallID string `json:"model_tool_call_id"`
		ToolName        string `json:"tool_name"`
	}
	if json.Unmarshal([]byte(projectionJSON), &projection) != nil ||
		projection.ToolName == "" || projection.ModelToolCallID == "" {
		return nil, status.Error(codes.FailedPrecondition, "tool use direct facts are malformed")
	}
	return &bridgeLoadContextToolUse{ModelToolCallID: projection.ModelToolCallID, ToolName: projection.ToolName}, nil
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
