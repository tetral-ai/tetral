package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

type bridgeLoadContextTurnFacts struct {
	Events         []bridgeLoadContextTurnEvent      `json:"events"`
	MessageLineage []bridgeLoadContextMessageLineage `json:"messageLineage"`
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
	ToolUseEventID  string `json:"toolUseEventId,omitempty"`
	ModelToolCallID string `json:"modelToolCallId,omitempty"`
	ToolName        string `json:"toolName,omitempty"`
	RepairKind      string `json:"repairKind,omitempty"`
	Outcome         string `json:"outcome"`
}

type bridgeLoadContextIdle struct {
	StopReason string `json:"stopReason"`
}

type bridgeLoadContextFailure struct {
	ErrorType   string `json:"errorType"`
	RetryStatus string `json:"retryStatus"`
}

type bridgeLoadContextMessageLineage struct {
	MessageID       string                                 `json:"messageId"`
	MessageSequence int64                                  `json:"messageSequence"`
	OwningEventID   string                                 `json:"owningEventId"`
	ModelRequestID  *string                                `json:"modelRequestId,omitempty"`
	Entries         []bridgeLoadContextMessageLineageEntry `json:"entries"`
}

type bridgeLoadContextMessageLineageEntry struct {
	LineageKind    string `json:"lineageKind"`
	OperationKind  string `json:"operationKind,omitempty"`
	SourceKind     string `json:"sourceKind,omitempty"`
	SourceID       string `json:"sourceId,omitempty"`
	EventID        string `json:"eventId,omitempty"`
	RepairEventID  string `json:"repairEventId,omitempty"`
	EventSequence  int64  `json:"eventSequence"`
	Disposition    string `json:"disposition,omitempty"`
	ToolUseEventID string `json:"toolUseEventId,omitempty"`
	Outcome        string `json:"outcome,omitempty"`
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
	"session.error",
	"session.status_rescheduled",
	"session.thread_status_rescheduled",
	"session.status_idle",
	"session.thread_status_idle",
	"session.status_terminated",
	"session.thread_status_terminated",
}

func loadThreadTurnFactsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	messages []bridgeLoadContextMessageDescriptor,
	durableTurnID *string,
) (bridgeLoadContextTurnFacts, error) {
	facts := bridgeLoadContextTurnFacts{
		Events:         make([]bridgeLoadContextTurnEvent, 0),
		MessageLineage: make([]bridgeLoadContextMessageLineage, 0, len(messages)),
	}
	toolParts, err := loadContextToolParts(messages)
	if err != nil {
		return facts, err
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
		event, err := bridgeTurnEventFact(
			ctx, tx, scope, raw.eventID, raw.eventSequence, raw.eventType, raw.modelRequestID,
			raw.payloadJSON, raw.projectionJSON, raw.runtimeWriteID, toolParts,
		)
		if err != nil {
			return facts, err
		}
		facts.Events = append(facts.Events, event)
	}
	lineage, err := loadContextMessageLineageTx(ctx, tx, scope, messages, eventFloor)
	if err != nil {
		return facts, err
	}
	facts.MessageLineage = lineage
	return facts, nil
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
		if message.ModelRequestID == nil || *message.ModelRequestID == "" {
			return 0, status.Error(codes.FailedPrecondition, "compaction message has no model request identity")
		}
		var sequence int64
		err := tx.QueryRow(ctx,
			`SELECT sequence
			   FROM session_events
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND session_thread_id = $3
			    AND model_request_id = $4
			    AND type = 'span.model_request_start'`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), *message.ModelRequestID,
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

type bridgeContextToolPart struct {
	ToolCallID     string
	ToolName       string
	ToolUseEventID string
	Status         string
	ErrorType      string
}

func loadContextToolParts(messages []bridgeLoadContextMessageDescriptor) (map[string][]bridgeContextToolPart, error) {
	partsByEventID := make(map[string][]bridgeContextToolPart)
	for _, message := range messages {
		var decoded struct {
			Parts []struct {
				Type           string `json:"type"`
				ToolCallID     string `json:"toolCallId"`
				ToolName       string `json:"toolName"`
				ToolUseEventID string `json:"toolUseEventId"`
				State          struct {
					Status string `json:"status"`
					Error  struct {
						Type string `json:"type"`
					} `json:"error"`
				} `json:"state"`
			} `json:"parts"`
		}
		if err := json.Unmarshal(message.DataJSON, &decoded); err != nil {
			return nil, status.Error(codes.FailedPrecondition, "session message tool projection is malformed")
		}
		for _, part := range decoded.Parts {
			if part.Type != "tool" {
				continue
			}
			if part.ToolCallID == "" || part.ToolName == "" {
				return nil, status.Error(codes.FailedPrecondition, "session message tool projection is malformed")
			}
			projected := bridgeContextToolPart{
				ToolCallID:     part.ToolCallID,
				ToolName:       part.ToolName,
				ToolUseEventID: part.ToolUseEventID,
				Status:         part.State.Status,
				ErrorType:      part.State.Error.Type,
			}
			if part.ToolUseEventID != "" {
				partsByEventID[part.ToolUseEventID] = append(partsByEventID[part.ToolUseEventID], projected)
			} else {
				partsByEventID[message.OwningEventID] = append(partsByEventID[message.OwningEventID], projected)
			}
		}
	}
	return partsByEventID, nil
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
	toolParts map[string][]bridgeContextToolPart,
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
		parts := toolParts[eventID]
		if len(parts) == 0 || parts[0].ToolUseEventID != eventID {
			return event, status.Error(codes.FailedPrecondition, "tool use projection is malformed")
		}
		for _, part := range parts[1:] {
			if part.ToolUseEventID != eventID || part.ToolCallID != parts[0].ToolCallID || part.ToolName != parts[0].ToolName {
				return event, status.Error(codes.FailedPrecondition, "tool use projection is malformed")
			}
		}
		event.ToolUse = &bridgeLoadContextToolUse{ModelToolCallID: parts[0].ToolCallID, ToolName: parts[0].ToolName}
	case "agent.tool_result", "agent.mcp_tool_result":
		result, err := bridgeTurnToolResultFact(eventType, payloadJSON, toolParts)
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
	_ = runtimeWriteID
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

func bridgeTurnToolResultFact(eventType string, payloadJSON string, toolParts map[string][]bridgeContextToolPart) (*bridgeLoadContextToolResult, error) {
	var payload struct {
		ToolUseEventID  string `json:"tool_use_event_id"`
		ToolUseID       string `json:"tool_use_id"`
		MCPToolUseID    string `json:"mcp_tool_use_id"`
		ModelToolCallID string `json:"model_tool_call_id"`
		ToolName        string `json:"tool_name"`
		RepairKind      string `json:"repair_kind"`
		IsError         *bool  `json:"is_error"`
		Reason          string `json:"reason"`
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return nil, status.Error(codes.FailedPrecondition, "tool result projection is malformed")
	}
	if payload.RepairKind != "" {
		if eventType != "agent.tool_result" || payload.RepairKind != "invalid_tool" || payload.ModelToolCallID == "" || payload.ToolName == "" {
			return nil, status.Error(codes.FailedPrecondition, "internal tool repair projection is malformed")
		}
		return &bridgeLoadContextToolResult{
			ModelToolCallID: payload.ModelToolCallID,
			ToolName:        payload.ToolName,
			RepairKind:      payload.RepairKind,
			Outcome:         "error",
		}, nil
	}
	toolUseEventID, err := runtimeToolResultUseEventID(eventType, runtimeToolResultEventPayload{
		ToolUseEventID: payload.ToolUseEventID,
		ToolUseID:      payload.ToolUseID,
		MCPToolUseID:   payload.MCPToolUseID,
	})
	if err != nil {
		return nil, err
	}
	parts := toolParts[toolUseEventID]
	outcome := "unknown"
	if len(parts) == 1 {
		switch parts[0].Status {
		case "completed":
			outcome = "success"
		case "error":
			outcome = "error"
		case "cancelled":
			outcome = "cancelled"
		}
	} else if payload.IsError != nil {
		if *payload.IsError {
			outcome = "error"
		} else {
			outcome = "success"
		}
	}
	if strings.Contains(payload.Reason, "interrupt") || strings.Contains(payload.Reason, "cancel") {
		outcome = "cancelled"
	}
	return &bridgeLoadContextToolResult{ToolUseEventID: toolUseEventID, Outcome: outcome}, nil
}

func loadContextMessageLineageTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	messages []bridgeLoadContextMessageDescriptor,
	eventFloor int64,
) ([]bridgeLoadContextMessageLineage, error) {
	lineageByID := make(map[string]*bridgeLoadContextMessageLineage, len(messages))
	for _, message := range messages {
		lineageByID[message.MessageID] = &bridgeLoadContextMessageLineage{
			MessageID:       message.MessageID,
			MessageSequence: message.MessageSequence,
			OwningEventID:   message.OwningEventID,
			ModelRequestID:  message.ModelRequestID,
			Entries:         make([]bridgeLoadContextMessageLineageEntry, 0),
		}
	}
	rows, err := tx.Query(ctx,
		`SELECT receipt_json
		   FROM session_bridge_operations
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND receipt_json IS NOT NULL
		    AND receipt_json <> ''`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(),
	)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var receiptJSON string
		if err := rows.Scan(&receiptJSON); err != nil {
			_ = rows.Close()
			return nil, err
		}
		receipt, err := unmarshalDeclarationReceipt(receiptJSON)
		if err != nil {
			_ = rows.Close()
			return nil, status.Error(codes.FailedPrecondition, "message declaration receipt is malformed")
		}
		eventSequenceByID := make(map[string]int64, len(receipt.GetEvents()))
		for _, event := range receipt.GetEvents() {
			eventSequenceByID[event.GetEventId()] = event.GetEventSequence()
		}
		for _, stamp := range receipt.GetMessages() {
			lineage := lineageByID[stamp.GetMessageId()]
			if lineage == nil {
				continue
			}
			if stamp.GetMessageSequence() != lineage.MessageSequence || stamp.GetOwningEventId() == "" {
				_ = rows.Close()
				return nil, status.Error(codes.FailedPrecondition, "message declaration receipt does not match its durable message")
			}
			eventID := stamp.GetOwningEventId()
			eventSequence, ok := eventSequenceByID[eventID]
			if !ok && stamp.GetDisposition() == bridgev1.DurableProjectionDisposition_DURABLE_PROJECTION_DISPOSITION_UPDATED && len(receipt.GetEvents()) > 0 {
				eventID = receipt.GetEvents()[0].GetEventId()
				eventSequence = receipt.GetEvents()[0].GetEventSequence()
				ok = eventID != "" && eventSequence > 0
			}
			if !ok || eventSequence <= 0 {
				_ = rows.Close()
				return nil, status.Error(codes.FailedPrecondition, "message declaration receipt has no event stamp")
			}
			disposition := ""
			switch stamp.GetDisposition() {
			case bridgev1.DurableProjectionDisposition_DURABLE_PROJECTION_DISPOSITION_CREATED:
				disposition = "created"
			case bridgev1.DurableProjectionDisposition_DURABLE_PROJECTION_DISPOSITION_UPDATED:
				disposition = "updated"
			default:
				_ = rows.Close()
				return nil, status.Error(codes.FailedPrecondition, "message declaration receipt disposition is malformed")
			}
			lineage.Entries = append(lineage.Entries, bridgeLoadContextMessageLineageEntry{
				LineageKind:   "declaration_receipt",
				OperationKind: receipt.GetOperationKind(),
				SourceKind:    receipt.GetSourceKind(),
				SourceID:      receipt.GetSourceId(),
				EventID:       eventID,
				EventSequence: eventSequence,
				Disposition:   disposition,
			})
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	_ = rows.Close()
	if err := appendContextRepairLineageTx(ctx, tx, scope, lineageByID, eventFloor); err != nil {
		return nil, err
	}
	result := make([]bridgeLoadContextMessageLineage, 0, len(messages))
	for _, message := range messages {
		lineage := lineageByID[message.MessageID]
		sort.Slice(lineage.Entries, func(i, j int) bool {
			return lineage.Entries[i].EventSequence < lineage.Entries[j].EventSequence
		})
		if len(lineage.Entries) == 0 {
			return nil, status.Error(codes.FailedPrecondition, "loaded message has no durable lineage")
		}
		result = append(result, *lineage)
	}
	return result, nil
}

func appendContextRepairLineageTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	lineageByID map[string]*bridgeLoadContextMessageLineage,
	eventFloor int64,
) error {
	type repairProjection struct {
		lineage   *bridgeLoadContextMessageLineage
		errorType string
	}
	projectionsByToolUse := make(map[string][]repairProjection)
	for _, lineage := range lineageByID {
		var raw string
		if err := tx.QueryRow(ctx,
			`SELECT data_json FROM session_messages
			  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3 AND message_id = $4`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), lineage.MessageID,
		).Scan(&raw); err != nil {
			return err
		}
		var message struct {
			Parts []struct {
				Type           string `json:"type"`
				ToolUseEventID string `json:"toolUseEventId"`
				State          struct {
					Error struct {
						Type string `json:"type"`
					} `json:"error"`
				} `json:"state"`
			} `json:"parts"`
		}
		if err := json.Unmarshal([]byte(raw), &message); err != nil {
			return status.Error(codes.FailedPrecondition, "repair message projection is malformed")
		}
		for _, part := range message.Parts {
			if part.Type == "tool" && part.ToolUseEventID != "" {
				projectionsByToolUse[part.ToolUseEventID] = append(
					projectionsByToolUse[part.ToolUseEventID],
					repairProjection{lineage: lineage, errorType: part.State.Error.Type},
				)
			}
		}
	}

	rows, err := tx.Query(ctx,
		`SELECT event_id, sequence, type, payload_json, runtime_write_id
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND sequence >= $4
		    AND type IN ('agent.tool_result', 'agent.mcp_tool_result')
		  ORDER BY sequence ASC`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), eventFloor,
	)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var eventID, eventType, payloadJSON string
		var eventSequence int64
		var runtimeWriteID sql.NullString
		if err := rows.Scan(&eventID, &eventSequence, &eventType, &payloadJSON, &runtimeWriteID); err != nil {
			return err
		}
		result, err := bridgeTurnToolResultFact(eventType, payloadJSON, nil)
		if err != nil || result.ToolUseEventID == "" {
			continue
		}
		lineageKind := ""
		if runtimeWriteID.Valid &&
			(strings.HasPrefix(runtimeWriteID.String, "rwrite_runtime_pod_lost_tool_") ||
				strings.HasPrefix(runtimeWriteID.String, "rwrite_runtime_pod_lost_delivery_")) {
			lineageKind = "bridge_pod_loss_repair"
		}
		candidates := projectionsByToolUse[result.ToolUseEventID]
		if lineageKind == "" {
			for _, candidate := range candidates {
				if candidate.errorType == "cleanup_expired" {
					lineageKind = "bridge_idle_cleanup_repair"
					break
				}
			}
		}
		if lineageKind == "" {
			continue
		}
		matching := make([]repairProjection, 0, len(candidates))
		for _, candidate := range candidates {
			if lineageKind == "bridge_idle_cleanup_repair" && candidate.errorType != "cleanup_expired" {
				continue
			}
			matching = append(matching, candidate)
		}
		if len(matching) > 1 {
			owned := matching[:0]
			for _, candidate := range matching {
				if candidate.lineage.OwningEventID == eventID {
					owned = append(owned, candidate)
				}
			}
			matching = owned
		}
		if len(matching) != 1 {
			return status.Error(codes.FailedPrecondition, "repair event has no unique durable message projection")
		}
		matching[0].lineage.Entries = append(matching[0].lineage.Entries, bridgeLoadContextMessageLineageEntry{
			LineageKind:    lineageKind,
			RepairEventID:  eventID,
			EventSequence:  eventSequence,
			ToolUseEventID: result.ToolUseEventID,
			Outcome:        result.Outcome,
		})
	}
	return rows.Err()
}
