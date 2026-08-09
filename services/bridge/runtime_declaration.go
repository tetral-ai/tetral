package agentruntimebridge

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// stableRuntimeID derives deterministic identities for durable replay keys.
func stableRuntimeID(parts ...string) string {
	hasher := sha256.New()
	var length [4]byte
	for _, part := range parts {
		binary.BigEndian.PutUint32(length[:], uint32(len([]byte(part)))) // #nosec G115 -- identifiers are bounded below uint32 at protocol validation.
		_, _ = hasher.Write(length[:])
		_, _ = hasher.Write([]byte(part))
	}
	return "stid_" + hex.EncodeToString(hasher.Sum(nil))
}

func marshalRuntimeDeclarationObject(value map[string]any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buffer.Bytes(), []byte{'\n'}), nil
}

func marshalRuntimeDeclarationObjectWithRawField(value map[string]any, fieldName string, rawJSON string) ([]byte, error) {
	if !json.Valid([]byte(rawJSON)) {
		return nil, fmt.Errorf("invalid raw declaration JSON")
	}
	encoded, err := marshalRuntimeDeclarationObject(value)
	if err != nil {
		return nil, err
	}
	encodedFieldName, err := json.Marshal(fieldName)
	if err != nil {
		return nil, err
	}
	result := make([]byte, 0, len(encoded)+len(encodedFieldName)+len(rawJSON)+2)
	result = append(result, encoded[:len(encoded)-1]...)
	if len(encoded) > 2 {
		result = append(result, ',')
	}
	result = append(result, encodedFieldName...)
	result = append(result, ':')
	result = append(result, rawJSON...)
	result = append(result, '}')
	return result, nil
}

func commitInputsDeclarationDigest(request *bridgev1.CommitInputsRequest, inputKind string) (string, error) {
	creates, err := canonicalRuntimeMessageCreates(request.GetMessageCreates())
	if err != nil {
		return "", err
	}
	raw, err := marshalRuntimeDeclarationObject(map[string]any{
		"event_ids":         request.GetEventIds(),
		"input_kind":        inputKind,
		"message_creates":   creates,
		"operation_kind":    bridgeOpCommitInputs,
		"runtime_input_id":  request.GetRuntimeInputId(),
		"sequence_from":     request.GetSequenceFrom(),
		"sequence_to":       request.GetSequenceTo(),
		"session_thread_id": request.GetScope().GetSessionThreadId(),
	})
	if err != nil {
		return "", err
	}
	canonical, err := canonicalRunToolJSON(string(raw))
	if err != nil {
		return "", err
	}
	return sha256Hex(canonical), nil
}

func writeEventDeclarationDigest(
	request *bridgev1.WriteEventRequest,
	payloadJSON string,
	serverToolUseJSON string,
) (string, error) {
	payloadJSON = stripInternalProviderFields(payloadJSON)
	assistantAppend, err := canonicalRuntimeAssistantPartAppend(request.GetAssistantPartAppend())
	if err != nil {
		return "", err
	}
	toolSettlement, err := canonicalRuntimeToolSettlement(request.GetToolSettlement())
	if err != nil {
		return "", err
	}
	declaration := map[string]any{
		"assistant_part_append": assistantAppend,
		"event_type":            request.GetEventType(),
		"mcp_materialization_handle": nullableDeclarationString(
			request.GetMcpMaterializationHandle(),
		),
		"model_request_id": nullableDeclarationString(request.GetModelRequestId()),
		"operation_kind":   bridgeOpWriteEvent,
		"runtime_write_id": request.GetRuntimeWriteId(),
		"sandbox_result_digest": nullableDeclarationString(
			request.GetSandboxResultDigest(),
		),
		"server_tool_use":   json.RawMessage(serverToolUseJSON),
		"session_thread_id": request.GetScope().GetSessionThreadId(),
		"tool_settlement":   toolSettlement,
	}
	if request.GetEventType() == "span.model_request_start" {
		declaration["context_through_message_sequence"] = nullableDeclarationInt64(request.ContextThroughMessageSequence)
		declaration["request_kind"] = nullableDeclarationString(request.GetRequestKind())
	}
	raw, err := marshalRuntimeDeclarationObjectWithRawField(declaration, "payload", payloadJSON)
	if err != nil {
		return "", err
	}
	canonical, err := canonicalRunToolJSON(string(raw))
	if err != nil {
		return "", err
	}
	return sha256Hex(canonical), nil
}

func writeRequestEndDeclarationDigest(
	request *bridgev1.WriteRequestEndRequest,
	requestKind string,
	finishReason string,
	usageJSON string,
	consumedTransientJSON string,
	consumedFileJSON string,
) (string, error) {
	trailingAppend, err := canonicalRuntimeAssistantPartAppend(request.GetTrailingPartAppend())
	if err != nil {
		return "", err
	}
	checkpointCreate, err := canonicalRuntimeMessageCreate(request.GetCompactionCheckpointCreate())
	if err != nil {
		return "", err
	}
	var compactionEventPayload any
	if request.GetCompactionEventPayloadJson() != "" {
		canonicalPayload, err := canonicalRuntimeDeclarationJSON(request.GetCompactionEventPayloadJson())
		if err != nil {
			return "", status.Error(codes.InvalidArgument, "compaction event payload is invalid")
		}
		compactionEventPayload = json.RawMessage(canonicalPayload)
	}
	var prefixConsumption any
	if prefix := request.GetPrefixConsumption(); prefix != nil {
		prefixConsumption = map[string]any{
			"child_thread_id":          prefix.GetChildThreadId(),
			"parent_boundary_event_id": prefix.GetParentBoundaryEventId(),
		}
	}
	var compactedThrough any
	if request.CompactedThroughMessageSequence != nil {
		compactedThrough = request.GetCompactedThroughMessageSequence()
	}
	var reschedule any
	if value := request.GetReschedule(); value != nil {
		reschedule = map[string]any{
			"attempt":    value.GetAttempt(),
			"backoff_ms": value.GetBackoffMs(),
			"deadline":   value.GetDeadline(),
		}
	}
	var interruptSettlement any
	if value := request.GetInterruptSettlement(); value != nil {
		interruptSettlement = map[string]any{
			"event_ids":        value.GetEventIds(),
			"runtime_input_id": value.GetRuntimeInputId(),
			"sequence_from":    value.GetSequenceFrom(),
			"sequence_to":      value.GetSequenceTo(),
		}
	}
	raw, err := marshalRuntimeDeclarationObject(map[string]any{
		"compacted_through_message_sequence": compactedThrough,
		"compaction_checkpoint_create":       checkpointCreate,
		"compaction_event_payload":           compactionEventPayload,
		"consumed_attachment_refs":           json.RawMessage(consumedTransientJSON),
		"consumed_file_attachments":          json.RawMessage(consumedFileJSON),
		"error_kind":                         nullableDeclarationString(request.GetErrorKind()),
		"finish_reason":                      finishReason,
		"interrupt_settlement":               interruptSettlement,
		"is_error":                           request.GetIsError(),
		"model_request_id":                   request.GetModelRequestId(),
		"model_request_start_event_id":       request.GetModelRequestStartEventId(),
		"operation_kind":                     bridgeOpWriteRequestEnd,
		"prefix_consumption":                 prefixConsumption,
		"request_kind":                       requestKind,
		"reschedule":                         reschedule,
		"session_thread_id":                  request.GetScope().GetSessionThreadId(),
		"trailing_part_append":               trailingAppend,
		"usage":                              json.RawMessage(usageJSON),
	})
	if err != nil {
		return "", err
	}
	canonical, err := canonicalRunToolJSON(string(raw))
	if err != nil {
		return "", err
	}
	return sha256Hex(canonical), nil
}

func finishIdleDeclarationDigest(request *bridgev1.FinishIdleRequest, stopReasonJSON string) (string, error) {
	completionMail, err := canonicalRuntimeMessageCreate(request.GetCompletionMailCreate())
	if err != nil {
		return "", err
	}
	raw, err := marshalRuntimeDeclarationObject(map[string]any{
		"completion_mail_create": completionMail,
		"durable_turn_id":        request.GetDurableTurnId(),
		"operation_kind":         bridgeOpFinishIdle,
		"session_thread_id":      request.GetScope().GetSessionThreadId(),
		"stop_reason":            json.RawMessage(stopReasonJSON),
	})
	if err != nil {
		return "", err
	}
	canonical, err := canonicalRunToolJSON(string(raw))
	if err != nil {
		return "", err
	}
	return sha256Hex(canonical), nil
}

func runtimeTerminationDeclarationDigest(
	request *bridgev1.CommitRuntimeTerminationRequest,
	failureJSON string,
) (string, error) {
	settlements := make([]any, 0, len(request.GetToolSettlements()))
	for _, settlement := range request.GetToolSettlements() {
		canonical, err := canonicalRuntimeToolSettlement(settlement)
		if err != nil {
			return "", err
		}
		settlements = append(settlements, canonical)
	}
	completionMail, err := canonicalRuntimeMessageCreate(request.GetCompletionMailCreate())
	if err != nil {
		return "", err
	}
	raw, err := marshalRuntimeDeclarationObject(map[string]any{
		"completion_mail_create": completionMail,
		"failure":                json.RawMessage(failureJSON),
		"operation_kind":         bridgeOpCommitRuntimeTermination,
		"runtime_write_id":       request.GetRuntimeWriteId(),
		"session_thread_id":      request.GetScope().GetSessionThreadId(),
		"tool_settlements":       settlements,
	})
	if err != nil {
		return "", err
	}
	canonical, err := canonicalRunToolJSON(string(raw))
	if err != nil {
		return "", err
	}
	return sha256Hex(canonical), nil
}

func childLifecycleDeclarationDigest(
	operationKind string,
	action string,
	sessionThreadID string,
	childThreadID string,
	sourceKind string,
	sourceCommandID string,
) (string, error) {
	raw, err := marshalRuntimeDeclarationObject(map[string]any{
		"action":            action,
		"child_thread_id":   childThreadID,
		"operation_kind":    operationKind,
		"session_thread_id": sessionThreadID,
		"source_command_id": sourceCommandID,
		"source_kind":       sourceKind,
	})
	if err != nil {
		return "", err
	}
	canonical, err := canonicalRunToolJSON(string(raw))
	if err != nil {
		return "", err
	}
	return sha256Hex(canonical), nil
}

func internalToolRepairDeclarationDigest(
	request *bridgev1.CommitInternalToolRepairRequest,
	repairKey string,
) (string, error) {
	create, err := canonicalRuntimeMessageCreate(request.GetMessageCreate())
	if err != nil {
		return "", err
	}
	raw, err := marshalRuntimeDeclarationObject(map[string]any{
		"message_create":     create,
		"model_request_id":   request.GetModelRequestId(),
		"model_tool_call_id": request.GetModelToolCallId(),
		"operation_kind":     bridgeOpCommitInternalToolRepair,
		"repair_key":         repairKey,
		"session_thread_id":  request.GetScope().GetSessionThreadId(),
		"tool_name":          request.GetToolName(),
	})
	if err != nil {
		return "", err
	}
	canonical, err := canonicalRunToolJSON(string(raw))
	if err != nil {
		return "", err
	}
	return sha256Hex(canonical), nil
}

func taskNotificationDeclarationDigest(
	request *bridgev1.CommitTaskNotificationResultRequest,
	resultJSON string,
) (string, error) {
	resultJSON = stripInternalProviderFields(resultJSON)
	create, err := canonicalRuntimeMessageCreate(request.GetMessageCreate())
	if err != nil {
		return "", err
	}
	raw, err := marshalRuntimeDeclarationObjectWithRawField(map[string]any{
		"message_create":    create,
		"operation_kind":    bridgeOpCommitTaskNotificationResult,
		"runtime_input_id":  request.GetRuntimeInputId(),
		"session_thread_id": request.GetScope().GetSessionThreadId(),
		"task_id":           request.GetTaskId(),
	}, "result", resultJSON)
	if err != nil {
		return "", err
	}
	canonical, err := canonicalRunToolJSON(string(raw))
	if err != nil {
		return "", err
	}
	return sha256Hex(canonical), nil
}

func mcpMaterializationSourceID(request *bridgev1.CommitMcpToolResultRequest) string {
	return stableRuntimeID(
		"mcp_tool_execution",
		request.GetToolUseEventId(),
		request.GetNormalizedInputHash(),
	)
}

func mcpMaterializationDeclarationDigest(request *bridgev1.CommitMcpToolResultRequest) (string, error) {
	inputJSON, err := canonicalRuntimeDeclarationJSON(request.GetInputJson())
	if err != nil {
		return "", status.Error(codes.InvalidArgument, "mcp tool input is invalid")
	}
	resultJSON, err := canonicalRuntimeDeclarationJSON(request.GetResultJson())
	if err != nil {
		return "", status.Error(codes.InvalidArgument, "mcp tool result is invalid")
	}
	inlineMedia := make([]any, 0, len(request.GetInlineMedia()))
	for _, media := range request.GetInlineMedia() {
		if media == nil {
			return "", status.Error(codes.InvalidArgument, "mcp inline media is invalid")
		}
		contentHash := sha256.Sum256(media.GetData())
		inlineMedia = append(inlineMedia, map[string]any{
			"content_sha256":     hex.EncodeToString(contentHash[:]),
			"mime":               media.GetMime(),
			"suggested_filename": media.GetSuggestedFilename(),
		})
	}
	raw, err := marshalRuntimeDeclarationObject(map[string]any{
		"inline_media":          inlineMedia,
		"input":                 json.RawMessage(inputJSON),
		"mcp_server_name":       request.GetMcpServerName(),
		"normalized_input_hash": request.GetNormalizedInputHash(),
		"operation_kind":        bridgeOpCommitMcpToolResult,
		"result":                json.RawMessage(resultJSON),
		"session_thread_id":     request.GetScope().GetSessionThreadId(),
		"tool_name":             request.GetToolName(),
		"tool_use_event_id":     request.GetToolUseEventId(),
	})
	if err != nil {
		return "", err
	}
	canonical, err := canonicalRunToolJSON(string(raw))
	if err != nil {
		return "", err
	}
	return sha256Hex(canonical), nil
}

func canonicalRuntimeMessageCreates(creates []*bridgev1.RuntimeMessageCreate) ([]any, error) {
	result := make([]any, 0, len(creates))
	for _, create := range creates {
		canonical, err := canonicalRuntimeMessageCreate(create)
		if err != nil {
			return nil, err
		}
		result = append(result, canonical)
	}
	return result, nil
}

func canonicalRuntimeMessageCreate(create *bridgev1.RuntimeMessageCreate) (any, error) {
	if create == nil {
		return nil, nil
	}
	messageInfo, err := canonicalRuntimeDeclarationJSON(create.GetMessageInfoJson())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "runtime message create info is invalid")
	}
	parts, err := canonicalRuntimeParts(create.GetParts())
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"message_info":    json.RawMessage(messageInfo),
		"message_kind":    create.GetMessageKind().String(),
		"parts":           parts,
		"source_event_id": nullableDeclarationString(create.GetSourceEventId()),
	}, nil
}

func canonicalRuntimeAssistantPartAppend(appendValue *bridgev1.RuntimeAssistantPartAppend) (any, error) {
	if appendValue == nil {
		return nil, nil
	}
	parts, err := canonicalRuntimeParts(appendValue.GetParts())
	if err != nil {
		return nil, err
	}
	return map[string]any{"parts": parts}, nil
}

func canonicalRuntimeParts(runtimeParts []*bridgev1.RuntimePartCreate) ([]any, error) {
	parts := make([]any, 0, len(runtimeParts))
	for _, part := range runtimeParts {
		if part == nil {
			return nil, status.Error(codes.InvalidArgument, "runtime part is invalid")
		}
		partJSON, err := canonicalRuntimeDeclarationJSON(part.GetPartJson())
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "runtime part is invalid")
		}
		parts = append(parts, map[string]any{
			"part_json": json.RawMessage(partJSON),
			"part_kind": part.GetPartKind(),
		})
	}
	return parts, nil
}

func canonicalRuntimeToolSettlement(settlement *bridgev1.RuntimeToolSettlement) (any, error) {
	if settlement == nil {
		return nil, nil
	}
	value := map[string]any{"tool_use_event_id": settlement.GetToolUseEventId()}
	switch outcome := settlement.GetOutcome().(type) {
	case *bridgev1.RuntimeToolSettlement_Completed:
		canonical, err := canonicalRuntimeDeclarationJSON(outcome.Completed.GetOutputJson())
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "runtime tool completion is invalid")
		}
		value["completed"] = json.RawMessage(canonical)
	case *bridgev1.RuntimeToolSettlement_Error:
		canonical, err := canonicalRuntimeDeclarationJSON(outcome.Error.GetErrorJson())
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "runtime tool error is invalid")
		}
		value["error"] = json.RawMessage(canonical)
	case *bridgev1.RuntimeToolSettlement_Cancelled:
		var errorValue any
		if outcome.Cancelled.ErrorJson != nil {
			canonical, err := canonicalRuntimeDeclarationJSON(outcome.Cancelled.GetErrorJson())
			if err != nil {
				return nil, status.Error(codes.InvalidArgument, "runtime tool cancellation is invalid")
			}
			errorValue = json.RawMessage(canonical)
		}
		value["cancelled"] = errorValue
	default:
		return nil, status.Error(codes.InvalidArgument, "runtime tool settlement outcome is missing")
	}
	return value, nil
}

func canonicalRuntimeDeclarationJSON(raw string) (string, error) {
	if raw == "" {
		return "", fmt.Errorf("missing declaration JSON")
	}
	return canonicalRunToolJSON(raw)
}

func nullableDeclarationString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullableDeclarationInt64(value *int64) any {
	if value == nil {
		return nil
	}
	return *value
}

func commitInputCreatesTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	inputKind string,
	runtimeInputID string,
	eventIDs []string,
	creates []*bridgev1.RuntimeMessageCreate,
	now time.Time,
) (*bridgev1.DeclarationReceipt, error) {
	events := make(map[string]*bridgev1.DurableEventStamp, len(eventIDs))
	for _, eventID := range eventIDs {
		var eventType string
		var eventSequence int64
		if err := tx.QueryRow(ctx,
			`SELECT type, sequence
			   FROM session_events
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND session_thread_id = $3
			    AND event_id = $4
			  FOR UPDATE`,
			scope.GetWorkspaceId(),
			scope.GetSessionId(),
			scope.GetSessionThreadId(),
			eventID,
		).Scan(&eventType, &eventSequence); err != nil {
			return nil, err
		}
		events[eventID] = &bridgev1.DurableEventStamp{
			SessionThreadId: scope.GetSessionThreadId(),
			EventId:         eventID,
			EventSequence:   eventSequence,
			Disposition:     bridgev1.DurableEventDisposition_DURABLE_EVENT_DISPOSITION_EXISTING,
		}
	}

	messageStamps := make([]*bridgev1.DurableMessageStamp, 0, len(creates))
	for index, create := range creates {
		if index >= len(eventIDs) {
			return nil, status.Error(codes.InvalidArgument, "runtime message create has no source event")
		}
		if create == nil || create.SourceEventId == nil || create.GetSourceEventId() != eventIDs[index] {
			return nil, status.Error(codes.InvalidArgument, "runtime input message lineage is invalid")
		}
		stamp, err := insertRuntimeMessageCreateTx(ctx, tx, scope, inputKind, eventIDs[index], create, now)
		if err != nil {
			return nil, err
		}
		messageStamps = append(messageStamps, stamp)
	}
	eventStamps := make([]*bridgev1.DurableEventStamp, 0, len(eventIDs))
	for _, eventID := range eventIDs {
		eventStamps = append(eventStamps, events[eventID])
	}
	return &bridgev1.DeclarationReceipt{
		SessionThreadId: scope.GetSessionThreadId(),
		OperationKind:   bridgeOpCommitInputs,
		SourceKind:      inputKind,
		OperationId:     runtimeInputID,
		Events:          eventStamps,
		Messages:        messageStamps,
	}, nil
}

func commitWriteEventDeclarationTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	runtimeWriteID string,
	eventType string,
	eventID string,
	eventSequence int64,
	modelRequestID string,
	appendValue *bridgev1.RuntimeAssistantPartAppend,
	settlement *bridgev1.RuntimeToolSettlement,
	now time.Time,
) (*bridgev1.DeclarationReceipt, error) {
	var messageStamps []*bridgev1.DurableMessageStamp
	if appendValue != nil {
		stamp, err := appendRuntimeAssistantMembersTx(
			ctx,
			tx,
			scope,
			eventType,
			eventID,
			modelRequestID,
			appendValue,
			now,
		)
		if err != nil {
			return nil, err
		}
		messageStamps = []*bridgev1.DurableMessageStamp{stamp}
	}
	if settlement != nil {
		if _, err := settleRuntimeToolPartTx(ctx, tx, scope, modelRequestID, eventID, settlement, now); err != nil {
			return nil, err
		}
	}
	return &bridgev1.DeclarationReceipt{
		SessionThreadId: scope.GetSessionThreadId(),
		OperationKind:   bridgeOpWriteEvent,
		SourceKind:      eventType,
		OperationId:     runtimeWriteID,
		Events: []*bridgev1.DurableEventStamp{{
			SessionThreadId: scope.GetSessionThreadId(),
			EventId:         eventID,
			EventSequence:   eventSequence,
			Disposition:     bridgev1.DurableEventDisposition_DURABLE_EVENT_DISPOSITION_CREATED,
		}},
		Messages: messageStamps,
	}, nil
}

func commitWriteRequestEndDeclarationTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	request *bridgev1.WriteRequestEndRequest,
	threadScope threadMutationScope,
	requestEndEventID string,
	requestEndSequence int64,
	now time.Time,
) (*bridgev1.DeclarationReceipt, error) {
	receipt := &bridgev1.DeclarationReceipt{
		SessionThreadId: request.GetScope().GetSessionThreadId(),
		OperationKind:   bridgeOpWriteRequestEnd,
		SourceKind:      "model_request",
		OperationId:     request.GetModelRequestId(),
		Events: []*bridgev1.DurableEventStamp{{
			SessionThreadId: request.GetScope().GetSessionThreadId(),
			EventId:         requestEndEventID,
			EventSequence:   requestEndSequence,
			Disposition:     bridgev1.DurableEventDisposition_DURABLE_EVENT_DISPOSITION_CREATED,
		}},
	}
	if request.GetRequestKind() != requestKindCompactionSummary {
		if request.GetCompactionCheckpointCreate() != nil || request.GetPrefixConsumption() != nil ||
			request.CompactedThroughMessageSequence != nil ||
			request.GetCompactionEventPayloadJson() != "" {
			return nil, status.Error(codes.InvalidArgument, "ordinary request end declaration is invalid")
		}
		if request.GetTrailingPartAppend() != nil {
			if request.GetIsError() || request.GetReschedule() != nil {
				return nil, status.Error(codes.InvalidArgument, "unsuccessful request end cannot append assistant parts")
			}
			messageStamp, err := appendRuntimeAssistantMembersTx(
				ctx,
				tx,
				request.GetScope(),
				"model_request",
				requestEndEventID,
				request.GetModelRequestId(),
				request.GetTrailingPartAppend(),
				now,
			)
			if err != nil {
				return nil, err
			}
			receipt.Messages = []*bridgev1.DurableMessageStamp{messageStamp}
		}
		if err := sealRuntimeAssistantMessageTx(ctx, tx, request, requestEndEventID, now); err != nil {
			return nil, err
		}
		return receipt, nil
	}
	if request.GetIsError() || request.GetReschedule() != nil {
		if request.GetTrailingPartAppend() != nil || request.GetCompactionCheckpointCreate() != nil ||
			request.GetPrefixConsumption() != nil ||
			request.CompactedThroughMessageSequence != nil ||
			request.GetCompactionEventPayloadJson() != "" {
			return nil, status.Error(codes.InvalidArgument, "non-successful compaction request end carries checkpoint fields")
		}
		return receipt, nil
	}
	if request.GetTrailingPartAppend() != nil || request.GetCompactionCheckpointCreate() == nil ||
		request.GetCompactionCheckpointCreate().GetMessageKind() != bridgev1.RuntimeMessageCreateKind_RUNTIME_MESSAGE_CREATE_KIND_COMPACTION_CHECKPOINT ||
		request.CompactedThroughMessageSequence == nil ||
		request.GetCompactionEventPayloadJson() == "" {
		return nil, status.Error(codes.InvalidArgument, "successful compaction request end is incomplete")
	}
	var compactionPayload struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal([]byte(request.GetCompactionEventPayloadJson()), &compactionPayload); err != nil ||
		compactionPayload.Type != "agent.thread_context_compacted" {
		return nil, status.Error(codes.InvalidArgument, "compaction event payload is invalid")
	}
	compactionPayloadJSON, err := canonicalRuntimeDeclarationJSON(request.GetCompactionEventPayloadJson())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "compaction event payload is invalid")
	}
	var durableBoundary int64
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(sequence), 0)
		   FROM session_messages
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3`,
		request.GetScope().GetWorkspaceId(),
		request.GetScope().GetSessionId(),
		request.GetScope().GetSessionThreadId(),
	).Scan(&durableBoundary); err != nil {
		return nil, err
	}
	if request.GetCompactedThroughMessageSequence() != durableBoundary {
		return nil, status.Error(codes.FailedPrecondition, "compaction message boundary is stale")
	}
	visibility, sessionVisible := threadScope.publicProjection("agent.thread_context_compacted")
	compactionEventID := id.New("evt_")
	compactionEventSequence, err := nextSessionEventSequenceTx(ctx, tx, request.GetScope())
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, runtime_write_id, model_request_id, projection_json,
			created_at, updated_at, processed_at
		) VALUES ($1, $2, $3, $4, $5, 'agent.thread_context_compacted', $6, $7, $8, $9, $10, '{}', $11, $11, $11)`,
		request.GetScope().GetWorkspaceId(),
		request.GetScope().GetSessionId(),
		request.GetScope().GetSessionThreadId(),
		compactionEventID,
		compactionEventSequence,
		compactionPayloadJSON,
		visibility,
		sessionVisible,
		request.GetModelRequestId(),
		request.GetModelRequestId(),
		now,
	); err != nil {
		return nil, err
	}
	if _, err := appendSessionEventStreamChangeTx(
		ctx,
		tx,
		request.GetScope(),
		compactionEventID,
		visibility,
		sessionVisible,
		now,
	); err != nil {
		return nil, err
	}
	checkpointStamp, err := insertRuntimeMessageCreateTx(
		ctx,
		tx,
		request.GetScope(),
		"agent.thread_context_compacted",
		compactionEventID,
		request.GetCompactionCheckpointCreate(),
		now,
	)
	if err != nil {
		return nil, err
	}
	if checkpointStamp.GetMessageSequence() != durableBoundary+1 {
		return nil, status.Error(codes.FailedPrecondition, "compaction checkpoint sequence is invalid")
	}
	prefixStamp, err := consumeThreadContextPrefixTx(
		ctx,
		tx,
		request.GetScope(),
		request.GetPrefixConsumption(),
		checkpointStamp,
	)
	if err != nil {
		return nil, err
	}
	receipt.Events = append(receipt.Events, &bridgev1.DurableEventStamp{
		SessionThreadId: request.GetScope().GetSessionThreadId(),
		EventId:         compactionEventID,
		EventSequence:   compactionEventSequence,
		Disposition:     bridgev1.DurableEventDisposition_DURABLE_EVENT_DISPOSITION_CREATED,
	})
	receipt.Messages = []*bridgev1.DurableMessageStamp{checkpointStamp}
	if prefixStamp != nil {
		receipt.PrefixConsumptions = []*bridgev1.PrefixConsumptionStamp{prefixStamp}
	}
	receipt.CompactedThroughMessageSequence = &durableBoundary
	return receipt, nil
}

// sealRuntimeAssistantMessageTx owns the request-level terminal fields. It
// loads the locked durable projection and changes no member identity or Tool
// state; member append, target settlement, and request seal therefore remain
// disjoint writers even though they share one Assistant row.
func sealRuntimeAssistantMessageTx(ctx context.Context, tx *dbconnect.Tx, request *bridgev1.WriteRequestEndRequest, requestEndEventID string, now time.Time) error {
	var messageID, dataJSON string
	err := tx.QueryRow(ctx, `SELECT message_id,data_json FROM session_messages
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
		 AND model_request_id=$4 AND kind='assistant' FOR UPDATE`,
		request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId(), request.GetScope().GetSessionThreadId(), request.GetModelRequestId()).Scan(&messageID, &dataJSON)
	if dbconnect.IsNoRows(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var message map[string]any
	if err := json.Unmarshal([]byte(dataJSON), &message); err != nil || message == nil || message["role"] != "assistant" {
		return status.Error(codes.FailedPrecondition, "durable Assistant message is invalid")
	}
	message["status"] = "completed"
	if request.GetIsError() || request.GetReschedule() != nil {
		message["status"] = "failed"
	}
	message["finishReason"] = request.GetFinishReason()
	if request.GetUsageJson() != "" {
		var usage any
		if err := json.Unmarshal([]byte(request.GetUsageJson()), &usage); err != nil {
			return status.Error(codes.InvalidArgument, "request usage is invalid")
		}
		message["usage"] = usage
	}
	message["updatedAt"] = now.UTC().Format(time.RFC3339Nano)
	updatedJSON, err := json.Marshal(message)
	if err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `UPDATE session_messages SET data_json=$5,last_event_id=$6,updated_at=$7
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND message_id=$4 AND model_request_id=$8`,
		request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId(), request.GetScope().GetSessionThreadId(),
		messageID, string(updatedJSON), requestEndEventID, now, request.GetModelRequestId())
	if err != nil {
		return err
	}
	if !rowsAffected(result) {
		return status.Error(codes.FailedPrecondition, "Assistant request seal lost its durable message")
	}
	return nil
}

func consumeThreadContextPrefixTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	requested *bridgev1.PrefixConsumptionDraft,
	checkpoint *bridgev1.DurableMessageStamp,
) (*bridgev1.PrefixConsumptionStamp, error) {
	var parentBoundaryEventID string
	var consumedBy sql.NullString
	err := tx.QueryRow(ctx,
		`SELECT parent_boundary_event_id, consumed_by_checkpoint_message_id
		   FROM session_thread_context_prefixes
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND child_thread_id = $3
		  FOR UPDATE`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	).Scan(&parentBoundaryEventID, &consumedBy)
	if dbconnect.IsNoRows(err) {
		if requested != nil {
			return nil, status.Error(codes.FailedPrecondition, "thread context prefix does not exist")
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if consumedBy.Valid {
		return nil, status.Error(codes.FailedPrecondition, "thread context prefix is already consumed")
	}
	if requested == nil {
		return nil, status.Error(codes.FailedPrecondition, "unconsumed thread context prefix is omitted")
	}
	if requested.GetChildThreadId() != scope.GetSessionThreadId() ||
		requested.GetParentBoundaryEventId() != parentBoundaryEventID {
		return nil, status.Error(codes.InvalidArgument, "thread context prefix consumption identity is invalid")
	}
	result, err := tx.Exec(ctx,
		`UPDATE session_thread_context_prefixes
		    SET consumed_by_checkpoint_message_id = $4
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND child_thread_id = $3
		    AND consumed_by_checkpoint_message_id IS NULL`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		checkpoint.GetMessageId(),
	)
	if err != nil {
		return nil, err
	}
	if !rowsAffected(result) {
		return nil, status.Error(codes.FailedPrecondition, "thread context prefix consumption lost its fence")
	}
	return &bridgev1.PrefixConsumptionStamp{
		ChildThreadId:         scope.GetSessionThreadId(),
		ParentBoundaryEventId: parentBoundaryEventID,
		CheckpointMessageId:   checkpoint.GetMessageId(),
		Disposition:           bridgev1.PrefixConsumptionDisposition_PREFIX_CONSUMPTION_DISPOSITION_CONSUMED,
	}, nil
}

func appendRuntimeAssistantMembersTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	eventType string,
	eventID string,
	modelRequestID string,
	appendValue *bridgev1.RuntimeAssistantPartAppend,
	now time.Time,
) (*bridgev1.DurableMessageStamp, error) {
	if modelRequestID == "" || appendValue == nil || len(appendValue.GetParts()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "assistant part append is incomplete")
	}
	switch eventType {
	case "agent.message", "agent.tool_use", "agent.mcp_tool_use", "model_request":
	default:
		return nil, status.Error(codes.InvalidArgument, "event cannot append assistant parts")
	}

	var (
		messageID       string
		messageSequence int64
		owningEventID   string
		messageCreated  time.Time
		existingJSON    string
	)
	err := tx.QueryRow(ctx,
		`SELECT message_id, sequence, source_event_id, created_at, data_json
		   FROM session_messages
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND model_request_id = $4
		  FOR UPDATE`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), modelRequestID,
	).Scan(&messageID, &messageSequence, &owningEventID, &messageCreated, &existingJSON)
	insertMessage := false
	disposition := bridgev1.DurableProjectionDisposition_DURABLE_PROJECTION_DISPOSITION_UPDATED
	if dbconnect.IsNoRows(err) {
		if err := tx.QueryRow(ctx,
			`SELECT COALESCE(MAX(sequence), 0) + 1
			   FROM session_messages
			  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(),
		).Scan(&messageSequence); err != nil {
			return nil, err
		}
		messageID = id.New("msg_")
		owningEventID = eventID
		messageCreated = now
		existingJSON = `{"role":"assistant","origin":"agent","status":"streaming","parts":[]}`
		insertMessage = true
		disposition = bridgev1.DurableProjectionDisposition_DURABLE_PROJECTION_DISPOSITION_CREATED
	} else if err != nil {
		return nil, err
	}

	var message map[string]any
	if err := json.Unmarshal([]byte(existingJSON), &message); err != nil || message == nil {
		return nil, status.Error(codes.FailedPrecondition, "durable assistant message is invalid")
	}
	if message["role"] != "assistant" || message["origin"] != "agent" {
		return nil, status.Error(codes.FailedPrecondition, "durable assistant message ownership is invalid")
	}
	if statusValue, _ := message["status"].(string); statusValue != "" && statusValue != "streaming" {
		return nil, status.Error(codes.AlreadyExists, "assistant message is already sealed")
	}
	existingParts, ok := message["parts"].([]any)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "durable assistant parts are invalid")
	}

	toolCount := 0
	textCount := 0
	partStamps := make([]*bridgev1.DurablePartStamp, 0, len(appendValue.GetParts()))
	for _, partCreate := range appendValue.GetParts() {
		if partCreate == nil || !validRuntimePartKind(partCreate.GetPartKind()) {
			return nil, status.Error(codes.InvalidArgument, "assistant part append contains an invalid part")
		}
		var part map[string]any
		if err := json.Unmarshal([]byte(partCreate.GetPartJson()), &part); err != nil || part == nil ||
			part["type"] != partCreate.GetPartKind() {
			return nil, status.Error(codes.InvalidArgument, "assistant part append payload is invalid")
		}
		for _, field := range []string{"id", "sessionId", "messageId", "sequence", "createdAt", "updatedAt"} {
			if _, present := part[field]; present {
				return nil, status.Error(codes.InvalidArgument, "assistant part append contains a durable field")
			}
		}
		if partCreate.GetPartKind() == "tool" {
			toolCount++
			if part["toolUseEventId"] != nil && part["toolUseEventId"] != "" {
				return nil, status.Error(codes.InvalidArgument, "new Tool Use already has a durable event identity")
			}
			part["toolUseEventId"] = eventID
			if eventType == "agent.tool_use" {
				part["toolEvent"] = map[string]any{"kind": "tool"}
			}
		}
		if partCreate.GetPartKind() == "text" {
			textCount++
		}
		partID := id.New("part_")
		partSequence := int64(len(existingParts))
		timestamp := now.UTC().Format(time.RFC3339Nano)
		part["id"] = partID
		part["sessionId"] = scope.GetSessionId()
		part["messageId"] = messageID
		part["sequence"] = partSequence
		part["createdAt"] = timestamp
		part["updatedAt"] = timestamp
		existingParts = append(existingParts, part)
		partStamps = append(partStamps, &bridgev1.DurablePartStamp{
			PartId: partID, MessageId: messageID, PartSequence: partSequence,
			CreatedAt: timestamp, UpdatedAt: timestamp,
			Disposition: bridgev1.DurableProjectionDisposition_DURABLE_PROJECTION_DISPOSITION_CREATED,
		})
	}
	switch eventType {
	case "agent.tool_use", "agent.mcp_tool_use":
		if toolCount != 1 || textCount != 0 {
			return nil, status.Error(codes.InvalidArgument, "Tool Use append must contain exactly one Tool member")
		}
	case "agent.message":
		if textCount != 1 || toolCount != 0 {
			return nil, status.Error(codes.InvalidArgument, "message append must contain exactly one text member")
		}
	case "model_request":
		if toolCount != 0 || textCount != 0 {
			return nil, status.Error(codes.InvalidArgument, "request-end append may contain only reasoning boundaries")
		}
		message["status"] = "completed"
	}
	if err := validateStableReasoningBudget(existingParts); err != nil {
		return nil, err
	}
	timestamp := now.UTC().Format(time.RFC3339Nano)
	createdAt := messageCreated.UTC().Format(time.RFC3339Nano)
	message["id"] = messageID
	message["sessionId"] = scope.GetSessionId()
	message["sequence"] = messageSequence
	message["createdAt"] = createdAt
	message["updatedAt"] = timestamp
	message["parts"] = existingParts
	dataJSON, err := json.Marshal(message)
	if err != nil {
		return nil, err
	}
	if insertMessage {
		if _, err := tx.Exec(ctx,
			`INSERT INTO session_messages (
				workspace_id, session_id, session_thread_id, message_id, sequence, kind,
				data_json, source_event_id, last_event_id, model_request_id, created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, 'assistant', $6, $7, $7, $8, $9, $9)`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(),
			messageID, messageSequence, string(dataJSON), eventID, modelRequestID, now,
		); err != nil {
			return nil, err
		}
	} else {
		result, err := tx.Exec(ctx,
			`UPDATE session_messages
			    SET data_json = $5, last_event_id = $6, updated_at = $7
			  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3
			    AND message_id = $4 AND model_request_id = $8`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(),
			messageID, string(dataJSON), eventID, now, modelRequestID,
		)
		if err != nil {
			return nil, err
		}
		if !rowsAffected(result) {
			return nil, status.Error(codes.FailedPrecondition, "assistant append lost its durable message")
		}
	}
	return &bridgev1.DurableMessageStamp{
		SessionThreadId: scope.GetSessionThreadId(), OwningEventId: owningEventID,
		MessageId: messageID, MessageSequence: messageSequence,
		CreatedAt: createdAt, UpdatedAt: timestamp, Disposition: disposition, Parts: partStamps,
	}, nil
}

func settleRuntimeToolPartTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	modelRequestID string,
	resultEventID string,
	settlement *bridgev1.RuntimeToolSettlement,
	now time.Time,
) (runtimeToolProjectionPayload, error) {
	if modelRequestID == "" || settlement == nil || settlement.GetToolUseEventId() == "" {
		return runtimeToolProjectionPayload{}, status.Error(codes.InvalidArgument, "runtime Tool settlement is incomplete")
	}
	var messageID, dataJSON string
	if err := tx.QueryRow(ctx,
		`SELECT message_id, data_json
		   FROM session_messages
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3
		    AND model_request_id = $4 AND kind = 'assistant'
		  FOR UPDATE`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), modelRequestID,
	).Scan(&messageID, &dataJSON); dbconnect.IsNoRows(err) {
		return runtimeToolProjectionPayload{}, status.Error(codes.FailedPrecondition, "Tool settlement has no Assistant message")
	} else if err != nil {
		return runtimeToolProjectionPayload{}, err
	}
	var message map[string]any
	if err := json.Unmarshal([]byte(dataJSON), &message); err != nil || message == nil {
		return runtimeToolProjectionPayload{}, status.Error(codes.FailedPrecondition, "durable Assistant message is invalid")
	}
	parts, ok := message["parts"].([]any)
	if !ok {
		return runtimeToolProjectionPayload{}, status.Error(codes.FailedPrecondition, "durable Assistant parts are invalid")
	}
	var selected map[string]any
	selectedIndex := -1
	for index, rawPart := range parts {
		part, ok := rawPart.(map[string]any)
		if !ok || part["type"] != "tool" || part["toolUseEventId"] != settlement.GetToolUseEventId() {
			continue
		}
		if selected != nil {
			return runtimeToolProjectionPayload{}, status.Error(codes.FailedPrecondition, "Tool settlement target is ambiguous")
		}
		selected = part
		selectedIndex = index
	}
	if selected == nil {
		return runtimeToolProjectionPayload{}, status.Error(codes.FailedPrecondition, "Tool settlement target is missing")
	}
	state, ok := selected["state"].(map[string]any)
	if !ok {
		return runtimeToolProjectionPayload{}, status.Error(codes.FailedPrecondition, "durable Tool state is invalid")
	}
	switch state["status"] {
	case "completed", "error", "cancelled":
		return runtimeToolProjectionPayload{}, status.Error(codes.AlreadyExists, "Tool Use already has a terminal settlement")
	}
	nextState := map[string]any{}
	if input, present := state["input"]; present {
		nextState["input"] = input
	}
	switch outcome := settlement.GetOutcome().(type) {
	case *bridgev1.RuntimeToolSettlement_Completed:
		var output any
		if err := json.Unmarshal([]byte(outcome.Completed.GetOutputJson()), &output); err != nil || output == nil {
			return runtimeToolProjectionPayload{}, status.Error(codes.InvalidArgument, "Tool completion output is invalid")
		}
		nextState["status"] = "completed"
		nextState["output"] = output
	case *bridgev1.RuntimeToolSettlement_Error:
		var normalizedError any
		if err := json.Unmarshal([]byte(outcome.Error.GetErrorJson()), &normalizedError); err != nil || normalizedError == nil {
			return runtimeToolProjectionPayload{}, status.Error(codes.InvalidArgument, "Tool error is invalid")
		}
		nextState["status"] = "error"
		nextState["error"] = normalizedError
	case *bridgev1.RuntimeToolSettlement_Cancelled:
		nextState["status"] = "cancelled"
		if outcome.Cancelled.ErrorJson != nil {
			var normalizedError any
			if err := json.Unmarshal([]byte(outcome.Cancelled.GetErrorJson()), &normalizedError); err != nil || normalizedError == nil {
				return runtimeToolProjectionPayload{}, status.Error(codes.InvalidArgument, "Tool cancellation error is invalid")
			}
			nextState["error"] = normalizedError
		}
	default:
		return runtimeToolProjectionPayload{}, status.Error(codes.InvalidArgument, "Tool settlement outcome is missing")
	}
	selected["state"] = nextState
	selected["updatedAt"] = now.UTC().Format(time.RFC3339Nano)
	parts[selectedIndex] = selected
	message["parts"] = parts
	message["updatedAt"] = now.UTC().Format(time.RFC3339Nano)
	updatedJSON, err := json.Marshal(message)
	if err != nil {
		return runtimeToolProjectionPayload{}, err
	}
	result, err := tx.Exec(ctx,
		`UPDATE session_messages
		    SET data_json = $5, last_event_id = $6, updated_at = $7
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3
		    AND message_id = $4 AND model_request_id = $8`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(),
		messageID, string(updatedJSON), resultEventID, now, modelRequestID,
	)
	if err != nil {
		return runtimeToolProjectionPayload{}, err
	}
	if !rowsAffected(result) {
		return runtimeToolProjectionPayload{}, status.Error(codes.FailedPrecondition, "Tool settlement lost its durable message")
	}
	return runtimeToolProjectionFromDurablePart(messageID, selected), nil
}

func runtimeToolProjectionFromDurablePart(messageID string, part map[string]any) runtimeToolProjectionPayload {
	projection := runtimeToolProjectionPayload{MessageID: messageID}
	projection.PartID, _ = part["id"].(string)
	if sequence, ok := part["sequence"].(float64); ok {
		projection.PartSequence = int(sequence)
	} else if sequence, ok := part["sequence"].(int64); ok {
		projection.PartSequence = int(sequence)
	}
	projection.ModelToolCallID, _ = part["toolCallId"].(string)
	projection.ToolName, _ = part["toolName"].(string)
	if toolEvent, ok := part["toolEvent"].(map[string]any); ok && toolEvent["kind"] == "mcp" {
		projection.MCPServerName, _ = toolEvent["mcpServerName"].(string)
	}
	if state, ok := part["state"].(map[string]any); ok {
		projection.State, _ = state["status"].(string)
		if input, present := state["input"]; present {
			if bounded, ok := input.(map[string]any); ok {
				if value, present := bounded["value"]; present {
					projection.Input, _ = json.Marshal(value)
				}
			}
		}
		if output, ok := state["output"].(map[string]any); ok {
			encoded, _ := json.Marshal(output)
			var bounded struct {
				Text      string `json:"text"`
				Truncated bool   `json:"truncated"`
			}
			if json.Unmarshal(encoded, &bounded) == nil {
				projection.Output = &bounded
			}
		}
		if normalizedError, ok := state["error"].(map[string]any); ok {
			encoded, _ := json.Marshal(normalizedError)
			var failure struct {
				Type      string `json:"type"`
				Message   string `json:"message"`
				Retryable bool   `json:"retryable"`
			}
			if json.Unmarshal(encoded, &failure) == nil {
				projection.Error = &failure
			}
		}
	}
	if len(projection.Input) == 0 {
		projection.Input = json.RawMessage(`{}`)
	}
	return projection
}

func stableReasoningLedgerTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	modelRequestID string,
) (normalizedStableReasoningSet, error) {
	var dataJSON string
	if err := tx.QueryRow(ctx,
		`SELECT data_json
		   FROM session_messages
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3
		    AND model_request_id = $4 AND kind = 'assistant'
		  FOR UPDATE`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), modelRequestID,
	).Scan(&dataJSON); dbconnect.IsNoRows(err) {
		return normalizedStableReasoningSet{CanonicalJSON: "[]", StrictlyOrdered: true}, nil
	} else if err != nil {
		return normalizedStableReasoningSet{}, err
	}
	var message struct {
		Parts []map[string]any `json:"parts"`
	}
	if err := json.Unmarshal([]byte(dataJSON), &message); err != nil {
		return normalizedStableReasoningSet{}, status.Error(codes.FailedPrecondition, "durable Assistant message is invalid")
	}
	result := normalizedStableReasoningSet{StrictlyOrdered: true}
	aggregateBytes := 0
	for _, part := range message.Parts {
		if part["type"] != "reasoning" {
			continue
		}
		sequenceValue, ok := part["sequence"].(float64)
		if !ok || sequenceValue < 0 || sequenceValue != float64(int64(sequenceValue)) {
			return normalizedStableReasoningSet{}, status.Error(codes.FailedPrecondition, "durable reasoning sequence is invalid")
		}
		sequence := int64(sequenceValue)
		textValue, _ := part["text"].(string)
		providerPartID, _ := part["providerPartId"].(string)
		metadata, _ := part["providerMetadata"].(map[string]any)
		if metadata == nil {
			metadata = map[string]any{}
		}
		metadataJSON, err := marshalBridgeDataJSON(metadata)
		if err != nil {
			return normalizedStableReasoningSet{}, status.Error(codes.FailedPrecondition, "durable reasoning metadata is invalid")
		}
		aggregateBytes += len(textValue) + len(metadataJSON)
		result.Parts = append(result.Parts, normalizedStableReasoningPart{
			ReasoningPartID: stableRuntimeID(
				"reasoning_ledger_part", scope.GetWorkspaceId(), scope.GetSessionId(),
				scope.GetSessionThreadId(), modelRequestID, strconv.FormatInt(sequence, 10),
			),
			ProviderPartID: providerPartID,
			PartSequence:   int32(sequence), // #nosec G115 -- bounded by the per-request part count below.
			Text:           textValue,
			Metadata:       metadata,
			Truncated:      part["truncated"] == true,
		})
	}
	if len(result.Parts) > MaxStableReasoningPartsPerRequest || aggregateBytes > MaxStableReasoningBytesPerRequest {
		return normalizedStableReasoningSet{}, status.Error(codes.InvalidArgument, "stable reasoning exceeds per-request budget")
	}
	canonical, err := marshalBridgeDataJSON(result.Parts)
	if err != nil {
		return normalizedStableReasoningSet{}, err
	}
	result.CanonicalJSON = canonical
	return result, nil
}

func lockThreadMutationOnlyTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) error {
	_, err := lockThreadMutationTx(ctx, tx, scope)
	return err
}

type runtimeMessageCreateClass struct {
	Kind        bridgev1.RuntimeMessageCreateKind
	Role        string
	Origin      string
	MessageKind string
}

func runtimeMessageCreateClassForSource(sourceKind string) (runtimeMessageCreateClass, bool) {
	switch sourceKind {
	case "messages":
		return runtimeMessageCreateClass{bridgev1.RuntimeMessageCreateKind_RUNTIME_MESSAGE_CREATE_KIND_USER_INPUT, "user", "user", "user"}, true
	case "tool_confirmation":
		return runtimeMessageCreateClass{bridgev1.RuntimeMessageCreateKind_RUNTIME_MESSAGE_CREATE_KIND_APPROVAL_INPUT, "user", "user", "user"}, true
	case "approval_review":
		return runtimeMessageCreateClass{bridgev1.RuntimeMessageCreateKind_RUNTIME_MESSAGE_CREATE_KIND_REVIEWER_INPUT, "user", "user", "user"}, true
	case "agent_mail":
		return runtimeMessageCreateClass{bridgev1.RuntimeMessageCreateKind_RUNTIME_MESSAGE_CREATE_KIND_AGENT_MAIL_INPUT, "user", "agent", "user"}, true
	case "rejection":
		return runtimeMessageCreateClass{bridgev1.RuntimeMessageCreateKind_RUNTIME_MESSAGE_CREATE_KIND_REJECTION, "assistant", "agent", "assistant"}, true
	case "task_notification":
		return runtimeMessageCreateClass{bridgev1.RuntimeMessageCreateKind_RUNTIME_MESSAGE_CREATE_KIND_TASK_NOTIFICATION, "user", "runtime", "runtime_notification"}, true
	case "completion_mail":
		return runtimeMessageCreateClass{bridgev1.RuntimeMessageCreateKind_RUNTIME_MESSAGE_CREATE_KIND_COMPLETION_MAIL, "user", "runtime", "runtime_notification"}, true
	case "agent.thread_context_compacted":
		return runtimeMessageCreateClass{bridgev1.RuntimeMessageCreateKind_RUNTIME_MESSAGE_CREATE_KIND_COMPACTION_CHECKPOINT, "user", "runtime", "compaction"}, true
	case "internal_tool_repair":
		return runtimeMessageCreateClass{bridgev1.RuntimeMessageCreateKind_RUNTIME_MESSAGE_CREATE_KIND_INTERNAL_TOOL_REPAIR, "assistant", "agent", "assistant"}, true
	case "termination":
		return runtimeMessageCreateClass{bridgev1.RuntimeMessageCreateKind_RUNTIME_MESSAGE_CREATE_KIND_TERMINATION, "assistant", "agent", "assistant"}, true
	default:
		return runtimeMessageCreateClass{}, false
	}
}

func insertRuntimeMessageCreateTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	sourceKind string,
	owningEventID string,
	create *bridgev1.RuntimeMessageCreate,
	now time.Time,
) (*bridgev1.DurableMessageStamp, error) {
	class, ok := runtimeMessageCreateClassForSource(sourceKind)
	if !ok || create == nil || create.GetMessageKind() != class.Kind ||
		(create.SourceEventId != nil && create.GetSourceEventId() != owningEventID) {
		return nil, status.Error(codes.InvalidArgument, "runtime message create identity is invalid")
	}
	var message map[string]any
	if err := json.Unmarshal([]byte(create.GetMessageInfoJson()), &message); err != nil || message == nil {
		return nil, status.Error(codes.InvalidArgument, "runtime message create info is invalid")
	}
	for _, field := range []string{"id", "sessionId", "sequence", "createdAt", "updatedAt", "providerId", "modelId", "parts"} {
		if _, present := message[field]; present {
			return nil, status.Error(codes.InvalidArgument, "runtime message create contains a durable or routing field")
		}
	}
	if message["role"] != class.Role || message["origin"] != class.Origin {
		return nil, status.Error(codes.InvalidArgument, "runtime message create ownership is invalid")
	}
	var messageSequence int64
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(sequence), 0) + 1
		   FROM session_messages
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(),
	).Scan(&messageSequence); err != nil {
		return nil, err
	}
	messageID := id.New("msg_")
	timestamp := now.UTC().Format(time.RFC3339Nano)
	parts := make([]any, 0, len(create.GetParts()))
	partStamps := make([]*bridgev1.DurablePartStamp, 0, len(create.GetParts()))
	for index, partCreate := range create.GetParts() {
		if partCreate == nil || !validRuntimePartKind(partCreate.GetPartKind()) {
			return nil, status.Error(codes.InvalidArgument, "runtime message create part is invalid")
		}
		var part map[string]any
		if err := json.Unmarshal([]byte(partCreate.GetPartJson()), &part); err != nil || part == nil ||
			part["type"] != partCreate.GetPartKind() {
			return nil, status.Error(codes.InvalidArgument, "runtime message create part payload is invalid")
		}
		for _, field := range []string{"id", "sessionId", "messageId", "sequence", "createdAt", "updatedAt"} {
			if _, present := part[field]; present {
				return nil, status.Error(codes.InvalidArgument, "runtime message create part contains a durable field")
			}
		}
		partID := id.New("part_")
		part["id"] = partID
		part["sessionId"] = scope.GetSessionId()
		part["messageId"] = messageID
		part["sequence"] = index
		part["createdAt"] = timestamp
		part["updatedAt"] = timestamp
		parts = append(parts, part)
		partStamps = append(partStamps, &bridgev1.DurablePartStamp{
			PartId: partID, MessageId: messageID, PartSequence: int64(index),
			CreatedAt: timestamp, UpdatedAt: timestamp,
			Disposition: bridgev1.DurableProjectionDisposition_DURABLE_PROJECTION_DISPOSITION_CREATED,
		})
	}
	message["id"] = messageID
	message["sessionId"] = scope.GetSessionId()
	message["sequence"] = messageSequence
	message["createdAt"] = timestamp
	message["updatedAt"] = timestamp
	message["parts"] = parts
	dataJSON, err := json.Marshal(message)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_messages (
			workspace_id, session_id, session_thread_id, message_id, sequence, kind,
			data_json, source_event_id, last_event_id, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, $9, $9)`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(),
		messageID, messageSequence, class.MessageKind, string(dataJSON), owningEventID, now,
	); err != nil {
		return nil, err
	}
	return &bridgev1.DurableMessageStamp{
		SessionThreadId: scope.GetSessionThreadId(), OwningEventId: owningEventID,
		MessageId: messageID, MessageSequence: messageSequence,
		CreatedAt: timestamp, UpdatedAt: timestamp,
		Disposition: bridgev1.DurableProjectionDisposition_DURABLE_PROJECTION_DISPOSITION_CREATED,
		Parts:       partStamps,
	}, nil
}

func commitInternalToolRepairCreateTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	repairKey string,
	eventID string,
	eventSequence int64,
	modelToolCallID string,
	toolName string,
	create *bridgev1.RuntimeMessageCreate,
	now time.Time,
) (*bridgev1.DeclarationReceipt, error) {
	if create == nil || len(create.GetParts()) != 1 {
		return nil, status.Error(codes.InvalidArgument, "internal Tool repair create is invalid")
	}
	var part map[string]any
	if err := json.Unmarshal([]byte(create.GetParts()[0].GetPartJson()), &part); err != nil {
		return nil, status.Error(codes.InvalidArgument, "internal Tool repair part is invalid")
	}
	state, _ := part["state"].(map[string]any)
	if create.GetParts()[0].GetPartKind() != "tool" || part["toolCallId"] != modelToolCallID ||
		part["toolName"] != toolName || state["status"] != "error" {
		return nil, status.Error(codes.InvalidArgument, "internal Tool repair payload does not match request")
	}
	stamp, err := insertRuntimeMessageCreateTx(ctx, tx, scope, "internal_tool_repair", eventID, create, now)
	if err != nil {
		return nil, err
	}
	result, err := tx.Exec(ctx,
		`UPDATE session_messages SET repair_key = $5
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3 AND message_id = $4`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), stamp.GetMessageId(), repairKey,
	)
	if err != nil {
		return nil, err
	}
	if !rowsAffected(result) {
		return nil, status.Error(codes.FailedPrecondition, "internal Tool repair message is missing")
	}
	return &bridgev1.DeclarationReceipt{
		SessionThreadId: scope.GetSessionThreadId(), OperationKind: bridgeOpCommitInternalToolRepair,
		SourceKind: "internal_tool_repair", OperationId: repairKey,
		Events: []*bridgev1.DurableEventStamp{{
			SessionThreadId: scope.GetSessionThreadId(), EventId: eventID, EventSequence: eventSequence,
			Disposition: bridgev1.DurableEventDisposition_DURABLE_EVENT_DISPOSITION_CREATED,
		}},
		Messages: []*bridgev1.DurableMessageStamp{stamp},
	}, nil
}

func validRuntimePartKind(kind string) bool {
	switch kind {
	case "text", "reasoning", "tool", "step-start", "step-finish":
		return true
	default:
		return false
	}
}

func marshalDeclarationReceipt(receipt *bridgev1.DeclarationReceipt) (string, error) {
	raw, err := protojson.Marshal(receipt)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func unmarshalDeclarationReceipt(raw string) (*bridgev1.DeclarationReceipt, error) {
	receipt := new(bridgev1.DeclarationReceipt)
	if err := protojson.Unmarshal([]byte(raw), receipt); err != nil {
		return nil, err
	}
	return receipt, nil
}

type declarationApplicationObservation struct {
	BindingID         string
	BindingGeneration int64
	Disposition       bridgev1.ReceiptApplicationDisposition
}

func logRuntimeDeclaration(
	logger *slog.Logger,
	scope *bridgev1.RuntimeScope,
	operation string,
	sourceKind string,
	operationID string,
	declarationDigest string,
	ack *bridgev1.BridgeWriteAck,
	observation declarationApplicationObservation,
) {
	if logger == nil || scope == nil || ack == nil {
		return
	}
	eventKind := "runtime_declaration_committed"
	outcome := "committed"
	if ack.GetStatus() == bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE {
		eventKind = "runtime_declaration_replayed"
		outcome = "replayed"
	}
	logger.Info(
		"runtime declaration settled",
		slog.String("operation", operation),
		slog.String("event.kind", eventKind),
		slog.String("component", ServiceNameBridgeAPI),
		slog.String("workspace.id", scope.GetWorkspaceId()),
		slog.String("session.id", scope.GetSessionId()),
		slog.String("thread.id", scope.GetSessionThreadId()),
		slog.String("declaration.source.kind", sourceKind),
		slog.String("operation.id", operationID),
		slog.String("declaration.digest", declarationDigest),
		slog.String("receipt.application_disposition", strings.ToLower(strings.TrimPrefix(observation.Disposition.String(), "RECEIPT_APPLICATION_DISPOSITION_"))),
		slog.String("binding.id", observation.BindingID),
		slog.Int64("binding.generation", observation.BindingGeneration),
		slog.String("outcome", outcome),
	)
}

func (s *PostgreSQLBridgeAPIStore) declarationApplicationObservation(
	ctx context.Context,
	scope *bridgev1.RuntimeScope,
) (declarationApplicationObservation, error) {
	var observation declarationApplicationObservation
	err := s.withScopeReadOnlyTx(ctx, scope, "agentruntimebridge.commit_inputs", func(tx *dbconnect.Tx) error {
		var err error
		observation, err = declarationApplicationObservationTx(ctx, tx, scope)
		return err
	})
	return observation, err
}

func declarationApplicationObservationTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
) (declarationApplicationObservation, error) {
	var observation declarationApplicationObservation
	var podUID string
	err := tx.QueryRow(ctx,
		`SELECT binding_id, binding_generation, agent_runtime_pod_uid
		   FROM session_runtime_bindings
		  WHERE workspace_id = $1
		    AND session_id = $2`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
	).Scan(&observation.BindingID, &observation.BindingGeneration, &podUID)
	if dbconnect.IsNoRows(err) {
		observation.Disposition = bridgev1.ReceiptApplicationDisposition_RECEIPT_APPLICATION_DISPOSITION_STALE_CUSTODY
		return observation, nil
	}
	if err != nil {
		return declarationApplicationObservation{}, err
	}
	if observation.BindingID == scope.GetBinding().GetBindingId() &&
		observation.BindingGeneration == scope.GetBinding().GetBindingGeneration() &&
		podUID == scope.GetBinding().GetTargetPodUid() {
		observation.Disposition = bridgev1.ReceiptApplicationDisposition_RECEIPT_APPLICATION_DISPOSITION_CURRENT_CUSTODY
	} else {
		observation.Disposition = bridgev1.ReceiptApplicationDisposition_RECEIPT_APPLICATION_DISPOSITION_STALE_CUSTODY
	}
	return observation, nil
}
