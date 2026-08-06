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
	"sort"
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

// stableRuntimeID derives the cross-runtime local identity used to correlate
// an unstamped declaration with its database-assigned receipt.
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
	drafts, err := canonicalRuntimeDrafts(request.GetDrafts())
	if err != nil {
		return "", err
	}
	pendingToolCancellations := make([]any, 0, len(request.GetPendingToolCancellations()))
	for _, cancellation := range request.GetPendingToolCancellations() {
		if cancellation == nil {
			return "", status.Error(codes.InvalidArgument, "pending tool cancellation is invalid")
		}
		pendingToolCancellations = append(pendingToolCancellations, map[string]any{
			"runtime_local_id":  cancellation.GetRuntimeLocalId(),
			"tool_use_event_id": cancellation.GetToolUseEventId(),
		})
	}
	raw, err := marshalRuntimeDeclarationObject(map[string]any{
		"drafts":                               drafts,
		"event_ids":                            request.GetEventIds(),
		"input_kind":                           inputKind,
		"operation_kind":                       bridgeOpCommitInputs,
		"pending_tool_cancellations":           pendingToolCancellations,
		"runtime_input_id":                     request.GetRuntimeInputId(),
		"sandbox_execution_tool_use_event_ids": append([]string{}, request.GetSandboxExecutionToolUseEventIds()...),
		"sequence_from":                        request.GetSequenceFrom(),
		"sequence_to":                          request.GetSequenceTo(),
		"session_thread_id":                    request.GetScope().GetSessionThreadId(),
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
	stableReasoningJSON string,
	serverToolUseJSON string,
) (string, error) {
	payloadJSON = stripInternalProviderFields(payloadJSON)
	drafts, err := canonicalRuntimeDrafts(request.GetDrafts())
	if err != nil {
		return "", err
	}
	declaration := map[string]any{
		"drafts":     drafts,
		"event_type": request.GetEventType(),
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
		"stable_reasoning":  json.RawMessage(stableReasoningJSON),
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
	stableReasoningJSON string,
	consumedTransientJSON string,
	consumedFileJSON string,
) (string, error) {
	drafts, err := canonicalRuntimeDrafts(request.GetDrafts())
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
			"checkpoint_runtime_local_id": prefix.GetCheckpointRuntimeLocalId(),
			"child_thread_id":             prefix.GetChildThreadId(),
			"parent_boundary_event_id":    prefix.GetParentBoundaryEventId(),
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
		pendingToolCancellations := make([]any, 0, len(value.GetPendingToolCancellations()))
		for _, cancellation := range value.GetPendingToolCancellations() {
			if cancellation == nil {
				return "", status.Error(codes.InvalidArgument, "pending tool cancellation is invalid")
			}
			pendingToolCancellations = append(pendingToolCancellations, map[string]any{
				"runtime_local_id":  cancellation.GetRuntimeLocalId(),
				"tool_use_event_id": cancellation.GetToolUseEventId(),
			})
		}
		interruptSettlement = map[string]any{
			"event_ids":                            value.GetEventIds(),
			"pending_tool_cancellations":           pendingToolCancellations,
			"runtime_input_id":                     value.GetRuntimeInputId(),
			"sandbox_execution_tool_use_event_ids": append([]string{}, value.GetSandboxExecutionToolUseEventIds()...),
			"sequence_from":                        value.GetSequenceFrom(),
			"sequence_to":                          value.GetSequenceTo(),
		}
	}
	raw, err := marshalRuntimeDeclarationObject(map[string]any{
		"compacted_through_message_sequence": compactedThrough,
		"compaction_event_payload":           compactionEventPayload,
		"consumed_attachment_refs":           json.RawMessage(consumedTransientJSON),
		"consumed_file_attachments":          json.RawMessage(consumedFileJSON),
		"drafts":                             drafts,
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
		"stable_reasoning":                   json.RawMessage(stableReasoningJSON),
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
	drafts, err := canonicalRuntimeDrafts(request.GetDrafts())
	if err != nil {
		return "", err
	}
	raw, err := marshalRuntimeDeclarationObject(map[string]any{
		"drafts":            drafts,
		"durable_turn_id":   request.GetDurableTurnId(),
		"operation_kind":    bridgeOpFinishIdle,
		"session_thread_id": request.GetScope().GetSessionThreadId(),
		"stop_reason":       json.RawMessage(stopReasonJSON),
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
	drafts, err := canonicalRuntimeDrafts(request.GetDrafts())
	if err != nil {
		return "", err
	}
	pendingToolCancellations := make([]any, 0, len(request.GetPendingToolCancellations()))
	for _, cancellation := range request.GetPendingToolCancellations() {
		if cancellation == nil {
			return "", status.Error(codes.InvalidArgument, "pending tool cancellation is invalid")
		}
		pendingToolCancellations = append(pendingToolCancellations, map[string]any{
			"runtime_local_id":  cancellation.GetRuntimeLocalId(),
			"tool_use_event_id": cancellation.GetToolUseEventId(),
		})
	}
	raw, err := marshalRuntimeDeclarationObject(map[string]any{
		"drafts":                               drafts,
		"failure":                              json.RawMessage(failureJSON),
		"operation_kind":                       bridgeOpCommitRuntimeTermination,
		"pending_tool_cancellations":           pendingToolCancellations,
		"runtime_write_id":                     request.GetRuntimeWriteId(),
		"sandbox_execution_tool_use_event_ids": append([]string{}, request.GetSandboxExecutionToolUseEventIds()...),
		"session_thread_id":                    request.GetScope().GetSessionThreadId(),
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
	requestedAt string,
) (string, error) {
	raw, err := marshalRuntimeDeclarationObject(map[string]any{
		"action":            action,
		"child_thread_id":   childThreadID,
		"operation_kind":    operationKind,
		"requested_at":      requestedAt,
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
	drafts, err := canonicalRuntimeDrafts(request.GetDrafts())
	if err != nil {
		return "", err
	}
	raw, err := marshalRuntimeDeclarationObject(map[string]any{
		"drafts":             drafts,
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
	var draft any
	if request.GetDraft() != nil {
		drafts, err := canonicalRuntimeDrafts([]*bridgev1.RuntimeMessageDraft{request.GetDraft()})
		if err != nil {
			return "", err
		}
		draft = drafts[0]
	}
	raw, err := marshalRuntimeDeclarationObjectWithRawField(map[string]any{
		"draft":             draft,
		"operation_kind":    bridgeOpCommitTaskNotificationResult,
		"runtime_input_id":  request.GetRuntimeInputId(),
		"session_thread_id": request.GetScope().GetSessionThreadId(),
		"source_id":         request.GetDraft().GetSourceId(),
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

func canonicalRuntimeDrafts(runtimeDrafts []*bridgev1.RuntimeMessageDraft) ([]any, error) {
	drafts := make([]any, 0, len(runtimeDrafts))
	for _, draft := range runtimeDrafts {
		if draft == nil {
			return nil, status.Error(codes.InvalidArgument, "runtime message draft is invalid")
		}
		messageInfo, err := canonicalRuntimeDeclarationJSON(draft.GetMessageInfoJson())
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "runtime message draft info is invalid")
		}
		parts := make([]any, 0, len(draft.GetParts()))
		for _, part := range draft.GetParts() {
			if part == nil {
				return nil, status.Error(codes.InvalidArgument, "runtime part draft is invalid")
			}
			partJSON, err := canonicalRuntimeDeclarationJSON(part.GetPartJson())
			if err != nil {
				return nil, status.Error(codes.InvalidArgument, "runtime part draft is invalid")
			}
			parts = append(parts, map[string]any{
				"ordinal":               part.GetOrdinal(),
				"part_json":             json.RawMessage(partJSON),
				"part_kind":             part.GetPartKind(),
				"runtime_local_part_id": part.GetRuntimeLocalPartId(),
			})
		}
		drafts = append(drafts, map[string]any{
			"draft_kind":       draft.GetDraftKind().String(),
			"message_info":     json.RawMessage(messageInfo),
			"ordinal":          draft.GetOrdinal(),
			"parts":            parts,
			"runtime_local_id": draft.GetRuntimeLocalId(),
			"source_event_id":  nullableDeclarationString(draft.GetSourceEventId()),
			"source_id":        draft.GetSourceId(),
			"source_kind":      draft.GetSourceKind(),
		})
	}
	return drafts, nil
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

func commitInputDraftsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	inputKind string,
	runtimeInputID string,
	eventIDs []string,
	drafts []*bridgev1.RuntimeMessageDraft,
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
			SourceEventId:   eventID,
			EventId:         eventID,
			EventSequence:   eventSequence,
			Disposition:     bridgev1.DurableEventDisposition_DURABLE_EVENT_DISPOSITION_EXISTING,
		}
	}

	messageStamps := make([]*bridgev1.DurableMessageStamp, 0, len(drafts))
	for index, draft := range drafts {
		stamp, err := insertRuntimeMessageDraftTx(ctx, tx, scope, inputKind, runtimeInputID, eventIDs, index, draft, now)
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
		SourceId:        runtimeInputID,
		Events:          eventStamps,
		Messages:        messageStamps,
	}, nil
}

func commitWriteEventDraftsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	runtimeWriteID string,
	eventType string,
	eventID string,
	eventSequence int64,
	modelRequestID string,
	drafts []*bridgev1.RuntimeMessageDraft,
	stableReasoning normalizedStableReasoningSet,
	now time.Time,
) (*bridgev1.DeclarationReceipt, error) {
	messageStamps := make([]*bridgev1.DurableMessageStamp, 0, len(drafts))
	for index, draft := range drafts {
		stamp, err := upsertRuntimeOutputDraftTx(
			ctx,
			tx,
			scope,
			runtimeWriteID,
			eventType,
			eventID,
			modelRequestID,
			index,
			draft,
			runtimeOutputWritePolicy{
				AllowNewParts:   true,
				StableReasoning: stableReasoning,
			},
			now,
		)
		if err != nil {
			return nil, err
		}
		messageStamps = append(messageStamps, stamp)
	}
	return &bridgev1.DeclarationReceipt{
		SessionThreadId: scope.GetSessionThreadId(),
		OperationKind:   bridgeOpWriteEvent,
		SourceKind:      eventType,
		SourceId:        runtimeWriteID,
		Events: []*bridgev1.DurableEventStamp{{
			SessionThreadId: scope.GetSessionThreadId(),
			SourceEventId:   runtimeWriteID,
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
	stableReasoning normalizedStableReasoningSet,
	now time.Time,
) (*bridgev1.DeclarationReceipt, error) {
	primaryDrafts := requestEndPrimaryDrafts(request.GetDrafts())
	receipt := &bridgev1.DeclarationReceipt{
		SessionThreadId: request.GetScope().GetSessionThreadId(),
		OperationKind:   bridgeOpWriteRequestEnd,
		SourceKind:      "model_request",
		SourceId:        request.GetModelRequestId(),
		Events: []*bridgev1.DurableEventStamp{{
			SessionThreadId: request.GetScope().GetSessionThreadId(),
			SourceEventId:   request.GetModelRequestId(),
			EventId:         requestEndEventID,
			EventSequence:   requestEndSequence,
			Disposition:     bridgev1.DurableEventDisposition_DURABLE_EVENT_DISPOSITION_CREATED,
		}},
	}
	if request.GetRequestKind() != requestKindCompactionSummary {
		if len(primaryDrafts) > 1 ||
			request.GetPrefixConsumption() != nil ||
			request.CompactedThroughMessageSequence != nil ||
			request.GetCompactionEventPayloadJson() != "" {
			return nil, status.Error(codes.InvalidArgument, "ordinary request end declaration is invalid")
		}
		if !request.GetIsError() && request.GetReschedule() == nil && len(stableReasoning.Parts) > 0 && len(primaryDrafts) == 0 {
			return nil, status.Error(codes.InvalidArgument, "successful stable reasoning settlement requires an assistant seal")
		}
		if len(primaryDrafts) == 1 {
			messageStamp, err := upsertRuntimeOutputDraftTx(
				ctx,
				tx,
				request.GetScope(),
				request.GetModelRequestId(),
				"model_request",
				requestEndEventID,
				request.GetModelRequestId(),
				0,
				primaryDrafts[0],
				runtimeOutputWritePolicy{
					StableReasoning:          stableReasoning,
					RequireExactReasoning:    !request.GetIsError() && request.GetReschedule() == nil,
					AllowReasoningOnlyCreate: !request.GetIsError() && request.GetReschedule() == nil && len(stableReasoning.Parts) > 0,
				},
				now,
			)
			if err != nil {
				return nil, err
			}
			receipt.Messages = []*bridgev1.DurableMessageStamp{messageStamp}
		}
		return receipt, nil
	}
	if request.GetIsError() || request.GetReschedule() != nil {
		if len(primaryDrafts) != 0 ||
			request.GetPrefixConsumption() != nil ||
			request.CompactedThroughMessageSequence != nil ||
			request.GetCompactionEventPayloadJson() != "" {
			return nil, status.Error(codes.InvalidArgument, "non-successful compaction request end carries checkpoint fields")
		}
		return receipt, nil
	}
	if len(primaryDrafts) != 1 ||
		primaryDrafts[0].GetDraftKind() != bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_COMPACTION_CHECKPOINT ||
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
	checkpointStamp, err := upsertRuntimeOutputDraftTx(
		ctx,
		tx,
		request.GetScope(),
		request.GetModelRequestId(),
		"agent.thread_context_compacted",
		compactionEventID,
		request.GetModelRequestId(),
		0,
		primaryDrafts[0],
		runtimeOutputWritePolicy{AllowNewParts: true},
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
		SourceEventId:   request.GetModelRequestId(),
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
		requested.GetParentBoundaryEventId() != parentBoundaryEventID ||
		requested.GetCheckpointRuntimeLocalId() != checkpoint.GetRuntimeLocalId() {
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

func upsertRuntimeOutputDraftTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	runtimeWriteID string,
	eventType string,
	eventID string,
	modelRequestID string,
	index int,
	draft *bridgev1.RuntimeMessageDraft,
	policy runtimeOutputWritePolicy,
	now time.Time,
) (*bridgev1.DurableMessageStamp, error) {
	draftClass, ok := runtimeOutputDraftClassForEvent(eventType)
	if !ok || draft == nil || draft.GetDraftKind() != draftClass.DraftKind ||
		draft.GetOrdinal() < 0 || int(draft.GetOrdinal()) != index ||
		draft.GetSourceKind() != eventType || draft.GetSourceId() != runtimeWriteID ||
		draft.GetSourceEventId() != "" {
		return nil, status.Error(codes.InvalidArgument, "runtime output draft identity is invalid")
	}
	if draftClass.MessageKind == "assistant" && modelRequestID == "" {
		return nil, status.Error(codes.InvalidArgument, "runtime output draft requires a model request id")
	}
	expectedMessageID := stableRuntimeID(
		"runtime_message_draft",
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		eventType,
		runtimeWriteID,
		runtimeDraftKindToken(draft.GetDraftKind()),
		strconv.FormatInt(int64(draft.GetOrdinal()), 10),
	)
	if draft.GetRuntimeLocalId() != expectedMessageID {
		return nil, status.Error(codes.InvalidArgument, "runtime output draft id is invalid")
	}

	var messageInfo map[string]any
	if err := json.Unmarshal([]byte(draft.GetMessageInfoJson()), &messageInfo); err != nil || messageInfo == nil {
		return nil, status.Error(codes.InvalidArgument, "runtime output draft info is invalid")
	}
	if messageInfo["role"] != draftClass.Role || messageInfo["origin"] != draftClass.Origin {
		return nil, status.Error(codes.InvalidArgument, "runtime output draft role is invalid")
	}
	sealsModelRequest := eventType == "model_request"
	if sealsModelRequest {
		switch messageInfo["status"] {
		case "completed", "failed", "cancelled":
		default:
			return nil, status.Error(codes.InvalidArgument, "request end assistant seal must be terminal")
		}
	}
	for _, field := range []string{"id", "sessionId", "sequence", "createdAt", "updatedAt", "providerId", "modelId", "parts"} {
		if _, present := messageInfo[field]; present {
			return nil, status.Error(codes.InvalidArgument, "runtime output draft contains a durable or routing field")
		}
	}

	var (
		messageID                string
		messageSequence          int64
		owningEventID            string
		messageCreated           time.Time
		existingJSON             string
		associatedModelRequestID any
		insertMessage            bool
	)
	err := sql.ErrNoRows
	if draftClass.MessageKind == "assistant" {
		associatedModelRequestID = modelRequestID
		err = tx.QueryRow(ctx,
			`SELECT message_id, sequence, source_event_id, created_at, data_json
			   FROM session_messages
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND session_thread_id = $3
			    AND model_request_id = $4
			  FOR UPDATE`,
			scope.GetWorkspaceId(),
			scope.GetSessionId(),
			scope.GetSessionThreadId(),
			modelRequestID,
		).Scan(&messageID, &messageSequence, &owningEventID, &messageCreated, &existingJSON)
	}
	disposition := bridgev1.DurableProjectionDisposition_DURABLE_PROJECTION_DISPOSITION_UPDATED
	if dbconnect.IsNoRows(err) {
		if sealsModelRequest && !policy.AllowReasoningOnlyCreate {
			return nil, status.Error(codes.FailedPrecondition, "request end assistant seal requires a durable message")
		}
		if err := tx.QueryRow(ctx,
			`SELECT COALESCE(MAX(sequence), 0) + 1
			   FROM session_messages
			  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3`,
			scope.GetWorkspaceId(),
			scope.GetSessionId(),
			scope.GetSessionThreadId(),
		).Scan(&messageSequence); err != nil {
			return nil, err
		}
		messageID = id.New("msg_")
		owningEventID = eventID
		messageCreated = now
		existingJSON = `{"parts":[]}`
		disposition = bridgev1.DurableProjectionDisposition_DURABLE_PROJECTION_DISPOSITION_CREATED
		insertMessage = true
	} else if err != nil {
		return nil, err
	}
	if sealsModelRequest && owningEventID == eventID {
		disposition = bridgev1.DurableProjectionDisposition_DURABLE_PROJECTION_DISPOSITION_CREATED
	}

	var existingMessage map[string]any
	if err := json.Unmarshal([]byte(existingJSON), &existingMessage); err != nil || existingMessage == nil {
		return nil, status.Error(codes.FailedPrecondition, "durable runtime message is invalid")
	}
	existingParts, _ := existingMessage["parts"].([]any)
	parts, partStamps, err := stampRuntimeOutputParts(
		scope,
		messageID,
		eventType,
		eventID,
		draft,
		existingParts,
		policy,
		now,
	)
	if err != nil {
		return nil, err
	}
	timestamp := now.UTC().Format(time.RFC3339Nano)
	createdAt := messageCreated.UTC().Format(time.RFC3339Nano)
	messageInfo["id"] = messageID
	messageInfo["sessionId"] = scope.GetSessionId()
	messageInfo["sequence"] = messageSequence
	messageInfo["createdAt"] = createdAt
	messageInfo["updatedAt"] = timestamp
	messageInfo["parts"] = parts
	dataJSON, err := json.Marshal(messageInfo)
	if err != nil {
		return nil, err
	}
	if insertMessage {
		if _, err := tx.Exec(ctx,
			`INSERT INTO session_messages (
				workspace_id, session_id, session_thread_id, message_id, sequence, kind,
				data_json, source_event_id, last_event_id, model_request_id, created_at, updated_at
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, $9, $10, $10)`,
			scope.GetWorkspaceId(),
			scope.GetSessionId(),
			scope.GetSessionThreadId(),
			messageID,
			messageSequence,
			draftClass.MessageKind,
			string(dataJSON),
			eventID,
			associatedModelRequestID,
			now,
		); err != nil {
			return nil, err
		}
	} else {
		tag, err := tx.Exec(ctx,
			`UPDATE session_messages
			    SET data_json = $5,
			        last_event_id = $6,
			        updated_at = $7
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND session_thread_id = $3
			    AND message_id = $4
			    AND model_request_id = $8`,
			scope.GetWorkspaceId(),
			scope.GetSessionId(),
			scope.GetSessionThreadId(),
			messageID,
			string(dataJSON),
			eventID,
			now,
			modelRequestID,
		)
		if err != nil {
			return nil, err
		}
		affected, err := tag.RowsAffected()
		if err != nil {
			return nil, err
		}
		if affected != 1 {
			return nil, status.Error(codes.FailedPrecondition, "runtime output draft lost its durable message")
		}
	}
	return &bridgev1.DurableMessageStamp{
		RuntimeLocalId:  draft.GetRuntimeLocalId(),
		SessionThreadId: scope.GetSessionThreadId(),
		OwningEventId:   owningEventID,
		MessageId:       messageID,
		MessageSequence: messageSequence,
		CreatedAt:       createdAt,
		UpdatedAt:       timestamp,
		Disposition:     disposition,
		Parts:           partStamps,
	}, nil
}

type runtimeOutputWritePolicy struct {
	AllowNewParts            bool
	AllowReasoningOnlyCreate bool
	RequireExactReasoning    bool
	StableReasoning          normalizedStableReasoningSet
}

func stampRuntimeOutputParts(
	scope *bridgev1.RuntimeScope,
	messageID string,
	eventType string,
	eventID string,
	draft *bridgev1.RuntimeMessageDraft,
	existingParts []any,
	policy runtimeOutputWritePolicy,
	now time.Time,
) ([]any, []*bridgev1.DurablePartStamp, error) {
	existingByKey := make(map[string]map[string]any, len(existingParts))
	existingKindOrdinals := make(map[string]int)
	for _, raw := range existingParts {
		part, ok := raw.(map[string]any)
		if !ok {
			return nil, nil, status.Error(codes.FailedPrecondition, "durable runtime part is invalid")
		}
		kind, _ := part["type"].(string)
		ordinal := existingKindOrdinals[kind]
		existingKindOrdinals[kind] = ordinal + 1
		key, err := runtimePartAssociationKey(part, ordinal)
		if err != nil {
			return nil, nil, err
		}
		if _, duplicate := existingByKey[key]; duplicate {
			return nil, nil, status.Error(codes.FailedPrecondition, "durable runtime part association is ambiguous")
		}
		existingByKey[key] = part
	}

	partKindOrdinal := make(map[string]int32)
	parts := make([]any, 0, len(draft.GetParts()))
	stamps := make([]*bridgev1.DurablePartStamp, 0, len(draft.GetParts()))
	seenExisting := make(map[string]struct{}, len(existingParts))
	for index, partDraft := range draft.GetParts() {
		if partDraft == nil || !validRuntimePartKind(partDraft.GetPartKind()) ||
			partDraft.GetOrdinal() != partKindOrdinal[partDraft.GetPartKind()] {
			return nil, nil, status.Error(codes.InvalidArgument, "runtime output part order is invalid")
		}
		partKindOrdinal[partDraft.GetPartKind()]++
		expectedPartID := stableRuntimeID(
			"runtime_message_part_draft",
			draft.GetRuntimeLocalId(),
			partDraft.GetPartKind(),
			strconv.FormatInt(int64(partDraft.GetOrdinal()), 10),
		)
		if partDraft.GetRuntimeLocalPartId() != expectedPartID {
			return nil, nil, status.Error(codes.InvalidArgument, "runtime output part id is invalid")
		}
		var partInfo map[string]any
		if err := json.Unmarshal([]byte(partDraft.GetPartJson()), &partInfo); err != nil || partInfo == nil ||
			partInfo["type"] != partDraft.GetPartKind() {
			return nil, nil, status.Error(codes.InvalidArgument, "runtime output part payload is invalid")
		}
		for _, field := range []string{"id", "sessionId", "messageId", "sequence", "createdAt", "updatedAt"} {
			if _, present := partInfo[field]; present {
				return nil, nil, status.Error(codes.InvalidArgument, "runtime output part contains a durable field")
			}
		}
		key, err := runtimePartAssociationKey(partInfo, int(partDraft.GetOrdinal()))
		if err != nil {
			return nil, nil, err
		}
		existing := existingByKey[key]
		toolUseEvent := partInfo["type"] == "tool" && (eventType == "agent.tool_use" || eventType == "agent.mcp_tool_use")
		priorToolUseEventID, _ := partInfo["toolUseEventId"].(string)
		if toolUseEvent && priorToolUseEventID == "" && existing != nil {
			return nil, nil, status.Error(codes.AlreadyExists, "runtime tool declaration cannot remove a stamped tool association")
		}
		if toolUseEvent && priorToolUseEventID == "" {
			partInfo["toolUseEventId"] = eventID
			if eventType == "agent.tool_use" {
				partInfo["toolEvent"] = map[string]any{"kind": "tool"}
			}
		}
		if toolUseEvent && priorToolUseEventID != "" &&
			(existing == nil || !sameRuntimeSealPartContent(existing, partInfo)) {
			return nil, nil, status.Error(codes.AlreadyExists, "runtime tool declaration cannot change a stamped tool part")
		}
		partID := id.New("part_")
		partSequence := int64(index)
		partCreatedAt := now.UTC().Format(time.RFC3339Nano)
		partDisposition := bridgev1.DurableProjectionDisposition_DURABLE_PROJECTION_DISPOSITION_CREATED
		if existing != nil {
			if !policy.AllowNewParts && !sameRuntimeSealPartContent(existing, partInfo) {
				return nil, nil, status.Error(codes.AlreadyExists, "request end assistant seal cannot change durable part content")
			}
			var ok bool
			partID, ok = existing["id"].(string)
			if !ok || partID == "" {
				return nil, nil, status.Error(codes.FailedPrecondition, "durable runtime part identity is invalid")
			}
			if created, ok := existing["createdAt"].(string); ok && created != "" {
				partCreatedAt = created
			}
			partDisposition = bridgev1.DurableProjectionDisposition_DURABLE_PROJECTION_DISPOSITION_UPDATED
			seenExisting[key] = struct{}{}
		} else if !policy.AllowNewParts {
			if partInfo["type"] != "reasoning" || !stableReasoningContainsDraftPart(policy.StableReasoning, partInfo, int(partDraft.GetOrdinal())) {
				return nil, nil, status.Error(codes.FailedPrecondition, "request end assistant seal cannot create a durable part")
			}
		}
		partUpdatedAt := now.UTC().Format(time.RFC3339Nano)
		partInfo["id"] = partID
		partInfo["sessionId"] = scope.GetSessionId()
		partInfo["messageId"] = messageID
		partInfo["sequence"] = partSequence
		partInfo["createdAt"] = partCreatedAt
		partInfo["updatedAt"] = partUpdatedAt
		parts = append(parts, partInfo)
		stamps = append(stamps, &bridgev1.DurablePartStamp{
			RuntimeLocalPartId: partDraft.GetRuntimeLocalPartId(),
			PartId:             partID,
			MessageId:          messageID,
			PartSequence:       partSequence,
			CreatedAt:          partCreatedAt,
			UpdatedAt:          partUpdatedAt,
			Disposition:        partDisposition,
		})
	}
	if len(seenExisting) != len(existingByKey) {
		return nil, nil, status.Error(codes.AlreadyExists, "runtime output draft cannot remove durable parts")
	}
	if err := validateStableReasoningBudget(parts); err != nil {
		return nil, nil, err
	}
	if err := validateStableReasoningProjection(parts, policy.StableReasoning, policy.RequireExactReasoning); err != nil {
		return nil, nil, err
	}
	sort.SliceStable(parts, func(left int, right int) bool {
		leftPart, _ := parts[left].(map[string]any)
		rightPart, _ := parts[right].(map[string]any)
		leftSequence, _ := leftPart["sequence"].(int64)
		rightSequence, _ := rightPart["sequence"].(int64)
		return leftSequence < rightSequence
	})
	return parts, stamps, nil
}

func sameRuntimeSealPartContent(left, right map[string]any) bool {
	semantic := func(part map[string]any) []byte {
		identity := make(map[string]any, len(part))
		for key, value := range part {
			switch key {
			case "id", "sessionId", "messageId", "sequence", "createdAt", "updatedAt", "startedAt", "completedAt":
				continue
			default:
				identity[key] = value
			}
		}
		encoded, _ := json.Marshal(identity)
		return encoded
	}
	return bytes.Equal(semantic(left), semantic(right))
}

func stableReasoningContainsDraftPart(
	set normalizedStableReasoningSet,
	part map[string]any,
	reasoningOrdinal int,
) bool {
	key, err := runtimePartAssociationKey(part, reasoningOrdinal)
	if err != nil {
		return false
	}
	for _, candidate := range set.Parts {
		if stableReasoningAssociationKey(candidate) == key && sameStableReasoningDraftContent(candidate, part) {
			return true
		}
	}
	return false
}

func validateStableReasoningProjection(
	parts []any,
	set normalizedStableReasoningSet,
	requireExact bool,
) error {
	projected := make(map[string]map[string]any)
	reasoningOrdinal := 0
	for _, rawPart := range parts {
		part, ok := rawPart.(map[string]any)
		if !ok || part["type"] != "reasoning" {
			continue
		}
		key, err := runtimePartAssociationKey(part, reasoningOrdinal)
		if err != nil {
			return err
		}
		if _, duplicate := projected[key]; duplicate {
			return status.Error(codes.FailedPrecondition, "stable reasoning projection is ambiguous")
		}
		projected[key] = part
		reasoningOrdinal++
	}
	for _, candidate := range set.Parts {
		part, ok := projected[stableReasoningAssociationKey(candidate)]
		if !ok || !sameStableReasoningDraftContent(candidate, part) {
			return status.Error(codes.AlreadyExists, "stable reasoning declaration diverges from its assistant draft")
		}
	}
	if requireExact && len(projected) != len(set.Parts) {
		return status.Error(codes.AlreadyExists, "successful assistant seal diverges from its stable reasoning set")
	}
	return nil
}

func stableReasoningAssociationKey(part normalizedStableReasoningPart) string {
	if part.ProviderPartID != "" {
		return "reasoning:" + part.ProviderPartID
	}
	return "reasoning:" + strconv.FormatInt(int64(part.PartSequence), 10)
}

func sameStableReasoningDraftContent(part normalizedStableReasoningPart, draft map[string]any) bool {
	text, _ := draft["text"].(string)
	truncated, _ := draft["truncated"].(bool)
	statusValue, _ := draft["status"].(string)
	providerPartID, _ := draft["providerPartId"].(string)
	metadata, _ := draft["providerMetadata"].(map[string]any)
	if metadata == nil {
		metadata = map[string]any{}
	}
	leftMetadata, _ := json.Marshal(part.Metadata)
	rightMetadata, _ := json.Marshal(metadata)
	return providerPartID == part.ProviderPartID &&
		text == part.Text &&
		truncated == part.Truncated &&
		statusValue == "completed" &&
		bytes.Equal(leftMetadata, rightMetadata)
}

func runtimePartAssociationKey(part map[string]any, fallbackOrdinal int) (string, error) {
	kind, _ := part["type"].(string)
	switch kind {
	case "tool":
		toolCallID, _ := part["toolCallId"].(string)
		if toolCallID == "" {
			return "", status.Error(codes.InvalidArgument, "runtime tool part association is missing")
		}
		return "tool:" + toolCallID, nil
	case "reasoning":
		if providerPartID, _ := part["providerPartId"].(string); providerPartID != "" {
			return "reasoning:" + providerPartID, nil
		}
	case "text", "step-start", "step-finish":
	default:
		return "", status.Error(codes.InvalidArgument, "runtime part kind is invalid")
	}
	return kind + ":" + strconv.Itoa(fallbackOrdinal), nil
}

type runtimeOutputDraftClass struct {
	DraftKind   bridgev1.RuntimeDraftKind
	Role        string
	Origin      string
	MessageKind string
}

func runtimeOutputDraftClassForEvent(eventType string) (runtimeOutputDraftClass, bool) {
	switch eventType {
	case "agent.message":
		return runtimeOutputDraftClass{
			DraftKind: bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_ASSISTANT_TEXT,
			Role:      "assistant", Origin: "agent", MessageKind: "assistant",
		}, true
	case "agent.tool_use", "agent.mcp_tool_use":
		return runtimeOutputDraftClass{
			DraftKind: bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_TOOL_USE,
			Role:      "assistant", Origin: "agent", MessageKind: "assistant",
		}, true
	case "agent.tool_result", "agent.mcp_tool_result":
		return runtimeOutputDraftClass{
			DraftKind: bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_TOOL_RESULT,
			Role:      "assistant", Origin: "agent", MessageKind: "assistant",
		}, true
	case "agent.thread_context_compacted":
		return runtimeOutputDraftClass{
			DraftKind: bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_COMPACTION_CHECKPOINT,
			Role:      "user", Origin: "runtime", MessageKind: "compaction",
		}, true
	case "task_notification":
		return runtimeOutputDraftClass{
			DraftKind: bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_TASK_NOTIFICATION,
			Role:      "user", Origin: "runtime", MessageKind: "runtime_notification",
		}, true
	case "model_request":
		return runtimeOutputDraftClass{
			DraftKind: bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_ASSISTANT_TEXT,
			Role:      "assistant", Origin: "agent", MessageKind: "assistant",
		}, true
	default:
		return runtimeOutputDraftClass{}, false
	}
}

func writeEventDraftClass(eventType string) (bridgev1.RuntimeDraftKind, string, bool) {
	class, ok := runtimeOutputDraftClassForEvent(eventType)
	if !ok || eventType == "agent.thread_context_compacted" || eventType == "model_request" || eventType == "task_notification" {
		return bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_UNSPECIFIED, "", false
	}
	return class.DraftKind, class.Role, true
}

func lockThreadMutationOnlyTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) error {
	var locked string
	return tx.QueryRow(ctx,
		`SELECT id
		   FROM session_threads
		  WHERE workspace_id = $1 AND session_id = $2 AND id = $3
		  FOR UPDATE`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	).Scan(&locked)
}

func insertRuntimeMessageDraftTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	inputKind string,
	runtimeInputID string,
	eventIDs []string,
	index int,
	draft *bridgev1.RuntimeMessageDraft,
	now time.Time,
) (*bridgev1.DurableMessageStamp, error) {
	expectedDraftKind, expectedRole, ok := commitInputDraftClass(inputKind)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "runtime input draft class is invalid")
	}
	if draft == nil || draft.GetDraftKind() == bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_UNSPECIFIED ||
		draft.GetOrdinal() < 0 || int(draft.GetOrdinal()) != index ||
		draft.GetSourceKind() != inputKind || draft.GetSourceId() != runtimeInputID ||
		!containsDeclarationString(eventIDs, draft.GetSourceEventId()) {
		return nil, status.Error(codes.InvalidArgument, "runtime message draft identity is invalid")
	}
	if draft.GetDraftKind() != expectedDraftKind {
		return nil, status.Error(codes.InvalidArgument, "runtime message draft class is invalid")
	}
	expectedMessageID := stableRuntimeID(
		"runtime_message_draft",
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		draft.GetSourceKind(),
		draft.GetSourceId(),
		runtimeDraftKindToken(draft.GetDraftKind()),
		strconv.FormatInt(int64(draft.GetOrdinal()), 10),
	)
	if draft.GetRuntimeLocalId() != expectedMessageID {
		return nil, status.Error(codes.InvalidArgument, "runtime message draft id is invalid")
	}
	var messageInfo map[string]any
	if err := json.Unmarshal([]byte(draft.GetMessageInfoJson()), &messageInfo); err != nil || messageInfo == nil {
		return nil, status.Error(codes.InvalidArgument, "runtime message draft info is invalid")
	}
	if _, ok := messageInfo["providerId"]; ok {
		return nil, status.Error(codes.InvalidArgument, "runtime message draft contains routing metadata")
	}
	if _, ok := messageInfo["modelId"]; ok {
		return nil, status.Error(codes.InvalidArgument, "runtime message draft contains routing metadata")
	}
	if messageInfo["role"] != expectedRole {
		return nil, status.Error(codes.InvalidArgument, "runtime input draft role is invalid")
	}
	var messageSequence int64
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(sequence), 0) + 1
		   FROM session_messages
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	).Scan(&messageSequence); err != nil {
		return nil, err
	}
	messageID := id.New("msg_")
	timestamp := now.UTC().Format(time.RFC3339Nano)
	messageInfo["id"] = messageID
	messageInfo["sessionId"] = scope.GetSessionId()
	messageInfo["sequence"] = messageSequence
	messageInfo["createdAt"] = timestamp
	messageInfo["updatedAt"] = timestamp

	partStamps := make([]*bridgev1.DurablePartStamp, 0, len(draft.GetParts()))
	parts := make([]any, 0, len(draft.GetParts()))
	partKindOrdinal := make(map[string]int32)
	for partSequence, part := range draft.GetParts() {
		if part == nil || !validRuntimePartKind(part.GetPartKind()) || part.GetOrdinal() != partKindOrdinal[part.GetPartKind()] {
			return nil, status.Error(codes.InvalidArgument, "runtime part draft order is invalid")
		}
		partKindOrdinal[part.GetPartKind()]++
		expectedPartID := stableRuntimeID(
			"runtime_message_part_draft",
			draft.GetRuntimeLocalId(),
			part.GetPartKind(),
			strconv.FormatInt(int64(part.GetOrdinal()), 10),
		)
		if part.GetRuntimeLocalPartId() != expectedPartID {
			return nil, status.Error(codes.InvalidArgument, "runtime part draft id is invalid")
		}
		var partInfo map[string]any
		if err := json.Unmarshal([]byte(part.GetPartJson()), &partInfo); err != nil || partInfo == nil || partInfo["type"] != part.GetPartKind() {
			return nil, status.Error(codes.InvalidArgument, "runtime part draft payload is invalid")
		}
		durablePartID := id.New("part_")
		partInfo["id"] = durablePartID
		partInfo["sessionId"] = scope.GetSessionId()
		partInfo["messageId"] = messageID
		partInfo["sequence"] = partSequence
		partInfo["createdAt"] = timestamp
		partInfo["updatedAt"] = timestamp
		parts = append(parts, partInfo)
		partStamps = append(partStamps, &bridgev1.DurablePartStamp{
			RuntimeLocalPartId: part.GetRuntimeLocalPartId(),
			PartId:             durablePartID,
			MessageId:          messageID,
			PartSequence:       int64(partSequence),
			CreatedAt:          timestamp,
			UpdatedAt:          timestamp,
			Disposition:        bridgev1.DurableProjectionDisposition_DURABLE_PROJECTION_DISPOSITION_CREATED,
		})
	}
	messageInfo["parts"] = parts
	dataJSON, err := json.Marshal(messageInfo)
	if err != nil {
		return nil, err
	}
	kind := expectedRole
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_messages (
			workspace_id, session_id, session_thread_id, message_id, sequence, kind,
			data_json, source_event_id, last_event_id, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, $9, $9)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		messageID,
		messageSequence,
		kind,
		string(dataJSON),
		draft.GetSourceEventId(),
		now,
	); err != nil {
		return nil, err
	}
	return &bridgev1.DurableMessageStamp{
		RuntimeLocalId:  draft.GetRuntimeLocalId(),
		SessionThreadId: scope.GetSessionThreadId(),
		OwningEventId:   draft.GetSourceEventId(),
		MessageId:       messageID,
		MessageSequence: messageSequence,
		CreatedAt:       timestamp,
		UpdatedAt:       timestamp,
		Disposition:     bridgev1.DurableProjectionDisposition_DURABLE_PROJECTION_DISPOSITION_CREATED,
		Parts:           partStamps,
	}, nil
}

func insertGeneratedRuntimeMessageDraftTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	owningEventID string,
	expectedRole string,
	expectedOrigin string,
	draft *bridgev1.RuntimeMessageDraft,
	now time.Time,
) (*bridgev1.DurableMessageStamp, error) {
	var messageInfo map[string]any
	if err := json.Unmarshal([]byte(draft.GetMessageInfoJson()), &messageInfo); err != nil || messageInfo == nil {
		return nil, status.Error(codes.InvalidArgument, "runtime message draft info is invalid")
	}
	for _, field := range []string{"id", "sessionId", "sequence", "createdAt", "updatedAt", "providerId", "modelId", "parts"} {
		if _, present := messageInfo[field]; present {
			return nil, status.Error(codes.InvalidArgument, "runtime message draft contains a durable or routing field")
		}
	}
	if messageInfo["role"] != expectedRole || messageInfo["origin"] != expectedOrigin {
		return nil, status.Error(codes.InvalidArgument, "runtime message draft role is invalid")
	}
	var messageSequence int64
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(sequence), 0) + 1
		   FROM session_messages
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	).Scan(&messageSequence); err != nil {
		return nil, err
	}
	messageID := id.New("msg_")
	timestamp := now.UTC().Format(time.RFC3339Nano)
	messageInfo["id"] = messageID
	messageInfo["sessionId"] = scope.GetSessionId()
	messageInfo["sequence"] = messageSequence
	messageInfo["createdAt"] = timestamp
	messageInfo["updatedAt"] = timestamp

	partStamps := make([]*bridgev1.DurablePartStamp, 0, len(draft.GetParts()))
	parts := make([]any, 0, len(draft.GetParts()))
	partKindOrdinal := make(map[string]int32)
	for partSequence, part := range draft.GetParts() {
		if part == nil || !validRuntimePartKind(part.GetPartKind()) || part.GetOrdinal() != partKindOrdinal[part.GetPartKind()] {
			return nil, status.Error(codes.InvalidArgument, "runtime part draft order is invalid")
		}
		partKindOrdinal[part.GetPartKind()]++
		expectedPartID := stableRuntimeID(
			"runtime_message_part_draft",
			draft.GetRuntimeLocalId(),
			part.GetPartKind(),
			strconv.FormatInt(int64(part.GetOrdinal()), 10),
		)
		if part.GetRuntimeLocalPartId() != expectedPartID {
			return nil, status.Error(codes.InvalidArgument, "runtime part draft id is invalid")
		}
		var partInfo map[string]any
		if err := json.Unmarshal([]byte(part.GetPartJson()), &partInfo); err != nil || partInfo == nil ||
			partInfo["type"] != part.GetPartKind() {
			return nil, status.Error(codes.InvalidArgument, "runtime part draft payload is invalid")
		}
		for _, field := range []string{"id", "sessionId", "messageId", "sequence", "createdAt", "updatedAt"} {
			if _, present := partInfo[field]; present {
				return nil, status.Error(codes.InvalidArgument, "runtime part draft contains a durable field")
			}
		}
		durablePartID := id.New("part_")
		partInfo["id"] = durablePartID
		partInfo["sessionId"] = scope.GetSessionId()
		partInfo["messageId"] = messageID
		partInfo["sequence"] = partSequence
		partInfo["createdAt"] = timestamp
		partInfo["updatedAt"] = timestamp
		parts = append(parts, partInfo)
		partStamps = append(partStamps, &bridgev1.DurablePartStamp{
			RuntimeLocalPartId: part.GetRuntimeLocalPartId(),
			PartId:             durablePartID,
			MessageId:          messageID,
			PartSequence:       int64(partSequence),
			CreatedAt:          timestamp,
			UpdatedAt:          timestamp,
			Disposition:        bridgev1.DurableProjectionDisposition_DURABLE_PROJECTION_DISPOSITION_CREATED,
		})
	}
	messageInfo["parts"] = parts
	dataJSON, err := json.Marshal(messageInfo)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_messages (
			workspace_id, session_id, session_thread_id, message_id, sequence, kind,
			data_json, source_event_id, last_event_id, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, $9, $9)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		messageID,
		messageSequence,
		expectedRole,
		string(dataJSON),
		owningEventID,
		now,
	); err != nil {
		return nil, err
	}
	return &bridgev1.DurableMessageStamp{
		RuntimeLocalId:  draft.GetRuntimeLocalId(),
		SessionThreadId: scope.GetSessionThreadId(),
		OwningEventId:   owningEventID,
		MessageId:       messageID,
		MessageSequence: messageSequence,
		CreatedAt:       timestamp,
		UpdatedAt:       timestamp,
		Disposition:     bridgev1.DurableProjectionDisposition_DURABLE_PROJECTION_DISPOSITION_CREATED,
		Parts:           partStamps,
	}, nil
}

func commitInternalToolRepairDraftTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	repairKey string,
	eventID string,
	eventSequence int64,
	modelToolCallID string,
	toolName string,
	draft *bridgev1.RuntimeMessageDraft,
	now time.Time,
) (*bridgev1.DeclarationReceipt, error) {
	const sourceKind = "internal_tool_repair"
	if draft == nil ||
		draft.GetDraftKind() != bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_INTERNAL_TOOL_REPAIR ||
		draft.GetOrdinal() != 0 ||
		draft.GetSourceKind() != sourceKind ||
		draft.GetSourceId() != repairKey ||
		draft.GetSourceEventId() != "" ||
		len(draft.GetParts()) != 1 {
		return nil, status.Error(codes.InvalidArgument, "internal tool repair draft identity is invalid")
	}
	expectedMessageLocalID := stableRuntimeID(
		"runtime_message_draft",
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		sourceKind,
		repairKey,
		runtimeDraftKindToken(draft.GetDraftKind()),
		"0",
	)
	if draft.GetRuntimeLocalId() != expectedMessageLocalID {
		return nil, status.Error(codes.InvalidArgument, "internal tool repair draft id is invalid")
	}

	var messageInfo map[string]any
	if err := json.Unmarshal([]byte(draft.GetMessageInfoJson()), &messageInfo); err != nil || messageInfo == nil {
		return nil, status.Error(codes.InvalidArgument, "internal tool repair draft info is invalid")
	}
	for _, field := range []string{"id", "sessionId", "sequence", "createdAt", "updatedAt", "providerId", "modelId", "parts"} {
		if _, present := messageInfo[field]; present {
			return nil, status.Error(codes.InvalidArgument, "internal tool repair draft contains a durable or routing field")
		}
	}
	if messageInfo["role"] != "assistant" || messageInfo["origin"] != "agent" || messageInfo["status"] != "completed" {
		return nil, status.Error(codes.InvalidArgument, "internal tool repair draft message is invalid")
	}

	partDraft := draft.GetParts()[0]
	if partDraft == nil ||
		partDraft.GetPartKind() != "tool" ||
		partDraft.GetOrdinal() != 0 ||
		partDraft.GetRuntimeLocalPartId() != stableRuntimeID("runtime_message_part_draft", expectedMessageLocalID, "tool", "0") {
		return nil, status.Error(codes.InvalidArgument, "internal tool repair part identity is invalid")
	}
	var partInfo map[string]any
	if err := json.Unmarshal([]byte(partDraft.GetPartJson()), &partInfo); err != nil || partInfo == nil {
		return nil, status.Error(codes.InvalidArgument, "internal tool repair part is invalid")
	}
	for _, field := range []string{"id", "sessionId", "messageId", "sequence", "createdAt", "updatedAt", "toolUseEventId"} {
		if _, present := partInfo[field]; present {
			return nil, status.Error(codes.InvalidArgument, "internal tool repair part contains a durable or public-association field")
		}
	}
	state, _ := partInfo["state"].(map[string]any)
	if partInfo["type"] != "tool" ||
		partInfo["toolCallId"] != modelToolCallID ||
		partInfo["toolName"] != toolName ||
		state["status"] != "error" {
		return nil, status.Error(codes.InvalidArgument, "internal tool repair payload does not match request")
	}

	var messageSequence int64
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(sequence), 0) + 1
		   FROM session_messages
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	).Scan(&messageSequence); err != nil {
		return nil, err
	}
	messageID := id.New("msg_")
	partID := id.New("part_")
	timestamp := now.UTC().Format(time.RFC3339Nano)
	messageInfo["id"] = messageID
	messageInfo["sessionId"] = scope.GetSessionId()
	messageInfo["sequence"] = messageSequence
	messageInfo["createdAt"] = timestamp
	messageInfo["updatedAt"] = timestamp
	partInfo["id"] = partID
	partInfo["sessionId"] = scope.GetSessionId()
	partInfo["messageId"] = messageID
	partInfo["sequence"] = 0
	partInfo["createdAt"] = timestamp
	partInfo["updatedAt"] = timestamp
	messageInfo["parts"] = []any{partInfo}
	dataJSON, err := json.Marshal(messageInfo)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_messages (
			workspace_id, session_id, session_thread_id, message_id, sequence, kind,
			data_json, source_event_id, last_event_id, repair_key, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, 'assistant', $6, $7, $7, $8, $9, $9)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		messageID,
		messageSequence,
		string(dataJSON),
		eventID,
		repairKey,
		now,
	); err != nil {
		return nil, err
	}
	return &bridgev1.DeclarationReceipt{
		SessionThreadId: scope.GetSessionThreadId(),
		OperationKind:   bridgeOpCommitInternalToolRepair,
		SourceKind:      sourceKind,
		SourceId:        repairKey,
		Events: []*bridgev1.DurableEventStamp{{
			SessionThreadId: scope.GetSessionThreadId(),
			SourceEventId:   repairKey,
			EventId:         eventID,
			EventSequence:   eventSequence,
			Disposition:     bridgev1.DurableEventDisposition_DURABLE_EVENT_DISPOSITION_CREATED,
		}},
		Messages: []*bridgev1.DurableMessageStamp{{
			RuntimeLocalId:  draft.GetRuntimeLocalId(),
			SessionThreadId: scope.GetSessionThreadId(),
			OwningEventId:   eventID,
			MessageId:       messageID,
			MessageSequence: messageSequence,
			CreatedAt:       timestamp,
			UpdatedAt:       timestamp,
			Disposition:     bridgev1.DurableProjectionDisposition_DURABLE_PROJECTION_DISPOSITION_CREATED,
			Parts: []*bridgev1.DurablePartStamp{{
				RuntimeLocalPartId: partDraft.GetRuntimeLocalPartId(),
				PartId:             partID,
				MessageId:          messageID,
				PartSequence:       0,
				CreatedAt:          timestamp,
				UpdatedAt:          timestamp,
				Disposition:        bridgev1.DurableProjectionDisposition_DURABLE_PROJECTION_DISPOSITION_CREATED,
			}},
		}},
	}, nil
}

func commitInputDraftClass(inputKind string) (bridgev1.RuntimeDraftKind, string, bool) {
	switch inputKind {
	case "messages":
		return bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_USER_INPUT, "user", true
	case "tool_confirmation":
		return bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_APPROVAL_INPUT, "user", true
	case "approval_review":
		return bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_REVIEWER_INPUT, "user", true
	case "agent_mail":
		return bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_AGENT_MAIL_INPUT, "user", true
	case "rejection":
		return bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_REJECTION, "assistant", true
	case "interrupt_control":
		return bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_CANCELLATION, "assistant", true
	default:
		return bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_UNSPECIFIED, "", false
	}
}

func runtimeDraftKindToken(kind bridgev1.RuntimeDraftKind) string {
	return strings.ToLower(strings.TrimPrefix(kind.String(), "RUNTIME_DRAFT_KIND_"))
}

func containsDeclarationString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
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
	sourceID string,
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
		slog.String("session.thread.id", scope.GetSessionThreadId()),
		slog.String("declaration.source.kind", sourceKind),
		slog.String("declaration.source.id", sourceID),
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
