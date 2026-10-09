package agentruntimebridge

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/tetral-ai/tetral/internal/runtimecontrol"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/sessioneventwrite"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func marshalRuntimeDeclarationObject(value map[string]any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return runtimecontrol.RestoreJSONStringifySeparatorEscapes(bytes.TrimSuffix(buffer.Bytes(), []byte{'\n'})), nil
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
	raw, err := marshalRuntimeDeclarationObject(map[string]any{
		"approval_review_text": request.GetApprovalReviewText(),
		"input_kind":           inputKind,
		"operation_kind":       runtimecontrol.OperationCommitInputs,
		"runtime_input_id":     request.GetRuntimeInputId(),
		"session_thread_id":    request.GetScope().GetSessionThreadId(),
	})
	if err != nil {
		return "", err
	}
	canonical, err := runtimecontrol.CanonicalRunToolJSON(string(raw))
	if err != nil {
		return "", err
	}
	return runtimecontrol.Sha256Hex(canonical), nil
}

func writeEventDeclarationDigest(
	request *bridgev1.WriteEventRequest,
	payloadJSON string,
	consumedFileJSON string,
) (string, error) {
	assistantDelta, err := canonicalRuntimeContextDelta(request.GetAssistantContextDelta())
	if err != nil {
		return "", err
	}
	declaration := map[string]any{
		"assistant_context_delta": assistantDelta,
		"event_type":              request.GetEventType(),
		"model_request_id":        nullableDeclarationString(request.GetModelRequestId()),
		"operation_kind":          bridgeOpWriteEvent,
		"runtime_write_id":        request.GetRuntimeWriteId(),
		"session_thread_id":       request.GetScope().GetSessionThreadId(),
	}
	if request.GetEventType() == "agent.message" || request.GetEventType() == "agent.thinking" {
		declaration["preallocated_event_id"] = request.GetPreallocatedEventId()
	}
	if request.GetEventType() == "span.model_request_start" {
		declaration["context_through_message_sequence"] = nullableDeclarationInt64(request.ContextThroughMessageSequence)
		declaration["request_kind"] = nullableDeclarationString(request.GetRequestKind())
		declaration["consumed_file_attachments"] = json.RawMessage(consumedFileJSON)
	}
	raw, err := marshalRuntimeDeclarationObjectWithRawField(declaration, "payload", payloadJSON)
	if err != nil {
		return "", err
	}
	canonical, err := runtimecontrol.CanonicalRunToolJSON(string(raw))
	if err != nil {
		return "", err
	}
	return runtimecontrol.Sha256Hex(canonical), nil
}

func writeToolDeclarationDigest(request *bridgev1.WriteEventRequest, prepared preparedRuntimeToolDeclaration) (string, error) {
	declaration := prepared.projection
	contextDelta := map[string]any{"parts": prepared.contextParts}
	raw, err := marshalPreparedRuntimeToolDeclarationObject(map[string]any{
		"assistant_context_delta": contextDelta,
		"evaluated_permission":    declaration.EvaluatedPermission,
		"event_type":              declaration.EventType,
		"mcp_server_name":         nullableDeclarationString(declaration.MCPServerName),
		"model_request_id":        request.GetModelRequestId(),
		"model_tool_call_id":      declaration.ModelToolCallID,
		"operation_kind":          bridgeOpWriteEvent,
		"provider_input":          declaration.ProviderInput,
		"public_execution_input":  declaration.CanonicalExecutionInput,
		"route_capability":        declaration.RouteCapability,
		"runtime_write_id":        request.GetRuntimeWriteId(),
		"session_thread_id":       request.GetScope().GetSessionThreadId(),
		"tool_name":               declaration.ToolName,
	}, prepared)
	if err != nil {
		return "", err
	}
	canonical, err := runtimecontrol.CanonicalRunToolJSON(string(raw))
	if err != nil {
		return "", err
	}
	return runtimecontrol.Sha256Hex(canonical), nil
}

func writeRequestEndDeclarationDigest(
	request *bridgev1.WriteRequestEndRequest,
	requestKind string,
	finishReason string,
	usageJSON string,
	consumedTransientJSON string,
) (string, error) {
	trailingDelta, err := canonicalRuntimeContextDelta(request.GetTrailingContextDelta())
	if err != nil {
		return "", err
	}
	compactionContext, err := canonicalRuntimeContextDelta(request.GetCompactionContext())
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
			"runtime_input_id": value.GetRuntimeInputId(),
		}
	}
	var providerContextRetention any
	if value := request.GetProviderContextRetention(); value != nil {
		providerContextRetention = map[string]any{
			"disposition":                value.GetDisposition(),
			"assistant_message_sequence": value.AssistantMessageSequence,
			"tool_use_event_ids":         value.GetToolUseEventIds(),
			"repair_event_ids":           value.GetRepairEventIds(),
		}
	}
	raw, err := marshalRuntimeDeclarationObject(map[string]any{
		"compacted_through_message_sequence": compactedThrough,
		"compaction_context":                 compactionContext,
		"compaction_event_payload":           compactionEventPayload,
		"consumed_attachment_refs":           json.RawMessage(consumedTransientJSON),
		"error_kind":                         nullableDeclarationString(request.GetErrorKind()),
		"finish_reason":                      finishReason,
		"interrupt_settlement":               interruptSettlement,
		"is_error":                           request.GetIsError(),
		"model_request_id":                   request.GetModelRequestId(),
		"operation_kind":                     bridgeOpWriteRequestEnd,
		"prefix_consumption":                 prefixConsumption,
		"provider_context_retention":         providerContextRetention,
		"request_kind":                       requestKind,
		"reschedule":                         reschedule,
		"session_thread_id":                  request.GetScope().GetSessionThreadId(),
		"trailing_context_delta":             trailingDelta,
		"usage":                              json.RawMessage(usageJSON),
	})
	if err != nil {
		return "", err
	}
	canonical, err := runtimecontrol.CanonicalRunToolJSON(string(raw))
	if err != nil {
		return "", err
	}
	return runtimecontrol.Sha256Hex(canonical), nil
}

func finishIdleDeclarationDigest(request *bridgev1.FinishIdleRequest, stopReasonJSON string) (string, error) {
	raw, err := marshalRuntimeDeclarationObject(map[string]any{
		"completion_mail_text": nullableDeclarationString(request.GetCompletionMailText()),
		"durable_turn_id":      request.GetDurableTurnId(),
		"operation_kind":       bridgeOpFinishIdle,
		"session_thread_id":    request.GetScope().GetSessionThreadId(),
		"stop_reason":          json.RawMessage(stopReasonJSON),
	})
	if err != nil {
		return "", err
	}
	canonical, err := runtimecontrol.CanonicalRunToolJSON(string(raw))
	if err != nil {
		return "", err
	}
	return runtimecontrol.Sha256Hex(canonical), nil
}

func runtimeTerminationRequestHash(
	request *bridgev1.CommitRuntimeTerminationRequest,
	failureJSON string,
) (string, error) {
	raw, err := marshalRuntimeDeclarationObject(map[string]any{
		"failure":            json.RawMessage(failureJSON),
		"binding_id":         request.GetScope().GetBinding().GetBindingId(),
		"binding_generation": request.GetScope().GetBinding().GetBindingGeneration(),
		"pod_uid":            request.GetScope().GetBinding().GetTargetPodUid(),
		"runtime_process_id": request.GetScope().GetBinding().GetRuntimeProcessId(),
		"operation_kind":     bridgeOpCommitRuntimeTermination,
		"runtime_write_id":   request.GetRuntimeWriteId(),
		"session_thread_id":  request.GetScope().GetSessionThreadId(),
	})
	if err != nil {
		return "", err
	}
	canonical, err := runtimecontrol.CanonicalRunToolJSON(string(raw))
	if err != nil {
		return "", err
	}
	return runtimecontrol.Sha256Hex(canonical), nil
}

func childLifecycleDeclarationDigest(
	operationKind string,
	action string,
	sessionThreadID string,
	childThreadID string,
	sourceKind string,
	sourceCommandID string,
	settlementKind string,
	settlementEventID string,
) (string, error) {
	declaration := map[string]any{
		"action":            action,
		"child_thread_id":   childThreadID,
		"operation_kind":    operationKind,
		"session_thread_id": sessionThreadID,
		"source_command_id": sourceCommandID,
		"source_kind":       sourceKind,
	}
	if settlementKind != "" || settlementEventID != "" {
		declaration["settlement_kind"] = settlementKind
		declaration["settlement_event_id"] = settlementEventID
	}
	raw, err := marshalRuntimeDeclarationObject(declaration)
	if err != nil {
		return "", err
	}
	canonical, err := runtimecontrol.CanonicalRunToolJSON(string(raw))
	if err != nil {
		return "", err
	}
	return runtimecontrol.Sha256Hex(canonical), nil
}

func internalToolRepairDeclarationDigest(
	request *bridgev1.CommitInternalToolRepairRequest,
	repairKey string,
) (string, error) {
	input, err := canonicalRuntimeDeclarationJSON(request.GetCanonicalInputJson())
	if err != nil {
		return "", status.Error(codes.InvalidArgument, "internal Tool repair input is invalid")
	}
	errorValue, err := runtimecontrol.DecodeRuntimeToolErrorJSON(request.GetError().GetErrorJson())
	if err != nil {
		return "", status.Error(codes.InvalidArgument, "internal Tool repair error is invalid")
	}
	declaration := map[string]any{
		"canonical_input":    json.RawMessage(input),
		"error":              errorValue,
		"model_request_id":   request.GetModelRequestId(),
		"model_tool_call_id": request.GetModelToolCallId(),
		"operation_kind":     bridgeOpCommitInternalToolRepair,
		"repair_key":         repairKey,
		"session_thread_id":  request.GetScope().GetSessionThreadId(),
		"tool_name":          request.GetToolName(),
	}
	prefix, err := canonicalInternalToolRepairPrefix(request.GetReasoningPrefixContextDelta())
	if err != nil {
		return "", err
	}
	if prefix != nil {
		declaration["reasoning_prefix_context_delta"] = prefix
	}
	raw, err := marshalRuntimeDeclarationObject(declaration)
	if err != nil {
		return "", err
	}
	canonical, err := runtimecontrol.CanonicalRunToolJSON(string(raw))
	if err != nil {
		return "", err
	}
	return runtimecontrol.Sha256Hex(canonical), nil
}

func taskNotificationDeclarationDigest(
	request *bridgev1.CommitTaskNotificationResultRequest,
) (string, error) {
	raw, err := marshalRuntimeDeclarationObject(map[string]any{
		"operation_kind":    runtimecontrol.OperationCommitTaskNotificationResult,
		"runtime_input_id":  request.GetRuntimeInputId(),
		"session_thread_id": request.GetScope().GetSessionThreadId(),
	})
	if err != nil {
		return "", err
	}
	canonical, err := runtimecontrol.CanonicalRunToolJSON(string(raw))
	if err != nil {
		return "", err
	}
	return runtimecontrol.Sha256Hex(canonical), nil
}

func mcpToolCommitDeclarationDigest(request *bridgev1.CommitMcpToolResultRequest) (string, error) {
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
		"claim_id":          request.GetClaimId(),
		"inline_media":      inlineMedia,
		"operation_kind":    bridgeOpCommitMcpToolResult,
		"result":            json.RawMessage(resultJSON),
		"session_thread_id": request.GetScope().GetSessionThreadId(),
		"tool_use_event_id": request.GetToolUseEventId(),
	})
	if err != nil {
		return "", err
	}
	canonical, err := runtimecontrol.CanonicalRunToolJSON(string(raw))
	if err != nil {
		return "", err
	}
	return runtimecontrol.Sha256Hex(canonical), nil
}

func mcpToolRelinquishDeclarationDigest(request *bridgev1.RelinquishMcpToolResultRequest) (string, error) {
	raw, err := marshalRuntimeDeclarationObject(map[string]any{
		"claim_id":          request.GetClaimId(),
		"operation_kind":    bridgeOpRelinquishMcpToolResult,
		"session_thread_id": request.GetScope().GetSessionThreadId(),
		"tool_use_event_id": request.GetToolUseEventId(),
	})
	if err != nil {
		return "", err
	}
	canonical, err := runtimecontrol.CanonicalRunToolJSON(string(raw))
	if err != nil {
		return "", err
	}
	return runtimecontrol.Sha256Hex(canonical), nil
}

func canonicalRuntimeToolSettlement(settlement *bridgev1.RuntimeToolSettlement) (any, error) {
	if settlement == nil {
		return nil, nil
	}
	value := map[string]any{"tool_use_event_id": settlement.GetToolUseEventId()}
	switch outcome := settlement.GetOutcome().(type) {
	case *bridgev1.RuntimeToolSettlement_Completed:
		output, err := runtimecontrol.DecodeRuntimeDeclarationValue(outcome.Completed.GetOutputJson())
		if err != nil || runtimecontrol.ValidateRuntimeBoundedText(output) != nil {
			return nil, status.Error(codes.InvalidArgument, "runtime tool completion is invalid")
		}
		canonical, err := canonicalRuntimeDeclarationJSON(outcome.Completed.GetOutputJson())
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "runtime tool completion is invalid")
		}
		usage, err := normalizeServerToolUseUsage(outcome.Completed.GetServerToolUse())
		if err != nil {
			return nil, err
		}
		value["completed"] = map[string]any{
			"output":          json.RawMessage(canonical),
			"server_tool_use": json.RawMessage(usage.CanonicalJSON),
		}
	case *bridgev1.RuntimeToolSettlement_Error:
		toolError, err := runtimecontrol.DecodeRuntimeToolErrorJSON(outcome.Error.GetErrorJson())
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "runtime tool error is invalid")
		}
		usage, err := normalizeServerToolUseUsage(outcome.Error.GetServerToolUse())
		if err != nil {
			return nil, err
		}
		value["error"] = map[string]any{
			"error":           toolError,
			"server_tool_use": json.RawMessage(usage.CanonicalJSON),
		}
	case *bridgev1.RuntimeToolSettlement_Cancelled:
		var errorValue any
		if outcome.Cancelled.ErrorJson != nil {
			toolError, err := runtimecontrol.DecodeRuntimeToolErrorJSON(outcome.Cancelled.GetErrorJson())
			if err != nil {
				return nil, status.Error(codes.InvalidArgument, "runtime tool cancellation is invalid")
			}
			errorValue = toolError
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
	return runtimecontrol.CanonicalRunToolJSON(raw)
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

type durableContextWrite struct {
	MessageID              string
	MessageSequence        int64
	CreatedToolUseEventIDs []string
}

type writeEventDurableFacts struct {
	EventID         string `json:"eventId"`
	MessageSequence *int64 `json:"messageSequence,omitempty"`
}

func commitWriteEventContextTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	eventType string,
	eventID string,
	modelRequestID string,
	delta *bridgev1.RuntimeContextDelta,
	preparedTool *preparedRuntimeToolDeclaration,
	now time.Time,
) (writeEventDurableFacts, error) {
	facts := writeEventDurableFacts{EventID: eventID}
	if delta != nil {
		write, err := appendPreparedRuntimeAssistantContextTx(ctx, tx, scope, eventType, eventID, modelRequestID, delta, preparedTool, now)
		if err != nil {
			return writeEventDurableFacts{}, err
		}
		facts.MessageSequence = &write.MessageSequence
	}
	return facts, nil
}

type requestEndDurableFacts struct {
	RequestEndEventID         string
	Disposition               string
	SealedMessageSequence     *int64
	EffectiveDeadline         string
	CompactionEventID         string
	CheckpointMessageSequence int64
	InterruptToolResults      []*bridgev1.RuntimeInterruptToolResult
}

func commitWriteRequestEndContextTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	request *bridgev1.WriteRequestEndRequest,
	requestKind string,
	threadScope runtimecontrol.ThreadMutationScope,
	requestEndEventID string,
	now time.Time,
) (requestEndDurableFacts, error) {
	facts := requestEndDurableFacts{RequestEndEventID: requestEndEventID}
	if requestKind != runtimecontrol.RequestKindCompactionSummary {
		if request.GetCompactionContext() != nil || request.GetPrefixConsumption() != nil ||
			request.CompactedThroughMessageSequence != nil ||
			request.GetCompactionEventPayloadJson() != "" {
			return requestEndDurableFacts{}, status.Error(codes.InvalidArgument, "ordinary request end declaration is invalid")
		}
		if request.GetTrailingContextDelta() != nil {
			if request.GetIsError() || request.GetReschedule() != nil {
				return requestEndDurableFacts{}, status.Error(codes.InvalidArgument, "unsuccessful request end cannot append assistant context")
			}
			write, err := appendRuntimeAssistantContextTx(ctx, tx, request.GetScope(), "model_request", requestEndEventID, request.GetModelRequestId(), request.GetTrailingContextDelta(), now)
			if err != nil {
				return requestEndDurableFacts{}, err
			}
			facts.SealedMessageSequence = &write.MessageSequence
		}
		sequence, err := sealRuntimeAssistantContextTx(ctx, tx, request, now)
		if err != nil {
			return requestEndDurableFacts{}, err
		}
		if sequence != nil {
			facts.SealedMessageSequence = sequence
		}
		return facts, nil
	}
	if request.GetIsError() || request.GetReschedule() != nil {
		if request.GetTrailingContextDelta() != nil || request.GetCompactionContext() != nil ||
			request.GetPrefixConsumption() != nil ||
			request.CompactedThroughMessageSequence != nil ||
			request.GetCompactionEventPayloadJson() != "" {
			return requestEndDurableFacts{}, status.Error(codes.InvalidArgument, "non-successful compaction request end carries checkpoint fields")
		}
		if _, err := sealRuntimeAssistantContextTx(ctx, tx, request, now); err != nil {
			return requestEndDurableFacts{}, err
		}
		return facts, nil
	}
	if request.GetTrailingContextDelta() != nil || request.GetCompactionContext() == nil ||
		request.CompactedThroughMessageSequence == nil ||
		request.GetCompactionEventPayloadJson() == "" {
		return requestEndDurableFacts{}, status.Error(codes.InvalidArgument, "successful compaction request end is incomplete")
	}
	var compactionPayload struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal([]byte(request.GetCompactionEventPayloadJson()), &compactionPayload); err != nil ||
		compactionPayload.Type != "agent.thread_context_compacted" {
		return requestEndDurableFacts{}, status.Error(codes.InvalidArgument, "compaction event payload is invalid")
	}
	compactionPayloadJSON, err := canonicalRuntimeDeclarationJSON(request.GetCompactionEventPayloadJson())
	if err != nil {
		return requestEndDurableFacts{}, status.Error(codes.InvalidArgument, "compaction event payload is invalid")
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
		return requestEndDurableFacts{}, err
	}
	if request.GetCompactedThroughMessageSequence() != durableBoundary {
		return requestEndDurableFacts{}, status.Error(codes.FailedPrecondition, "compaction message boundary is stale")
	}
	visibility, sessionVisible := threadScope.PublicProjection("agent.thread_context_compacted")
	compactionEventID := id.New("evt_")
	compactionEventSequence, err := runtimecontrol.NextSessionEventSequenceTx(ctx, tx, request.GetScope())
	if err != nil {
		return requestEndDurableFacts{}, err
	}
	if _, err := sessioneventwrite.InsertInitialTx(ctx, tx, sessioneventwrite.InitialEvent{
		WorkspaceID: request.GetScope().GetWorkspaceId(), SessionID: request.GetScope().GetSessionId(),
		SessionThreadID: request.GetScope().GetSessionThreadId(), EventID: compactionEventID, Sequence: compactionEventSequence, Type: "agent.thread_context_compacted",
		PayloadJSON: compactionPayloadJSON, Visibility: visibility, SessionVisible: sessionVisible,
		RuntimeWriteID: request.GetModelRequestId(), ModelRequestID: request.GetModelRequestId(), CreatedAt: now, ProcessedAt: &now,
	}); err != nil {
		return requestEndDurableFacts{}, err
	}
	checkpoint, err := insertCompactionContextEntryTx(
		ctx,
		tx,
		request.GetScope(),
		compactionEventID,
		nil,
		request.GetCompactionContext(),
		now,
	)
	if err != nil {
		return requestEndDurableFacts{}, err
	}
	if checkpoint.MessageSequence != durableBoundary+1 {
		return requestEndDurableFacts{}, status.Error(codes.FailedPrecondition, "compaction checkpoint sequence is invalid")
	}
	if err := consumeThreadContextPrefixTx(
		ctx,
		tx,
		request.GetScope(),
		request.GetPrefixConsumption(),
		checkpoint.MessageID,
	); err != nil {
		return requestEndDurableFacts{}, err
	}
	facts.CompactionEventID = compactionEventID
	facts.CheckpointMessageSequence = checkpoint.MessageSequence
	return facts, nil
}

// Request End seals every member that already crossed its durable append
// boundary. Runtime discards incomplete in-memory fragments before declaring
// the End, so deleting this row would make hot and cold history disagree.
// Pod-loss closeout is classified separately by LoadContext because no hot
// owner remains to prove a partial draft complete.
func sealRuntimeAssistantContextTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	request *bridgev1.WriteRequestEndRequest,
	now time.Time,
) (*int64, error) {
	var messageID string
	var sequence int64
	err := tx.QueryRow(ctx, `SELECT message_id,sequence FROM session_messages
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
		 AND model_request_id=$4 AND kind='assistant' FOR UPDATE`,
		request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId(), request.GetScope().GetSessionThreadId(), request.GetModelRequestId()).Scan(&messageID, &sequence)
	if dbconnect.IsNoRows(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &sequence, nil
}

func consumeThreadContextPrefixTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	requested *bridgev1.PrefixConsumptionDraft,
	checkpointMessageID string,
) error {
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
			return status.Error(codes.FailedPrecondition, "thread context prefix does not exist")
		}
		return nil
	}
	if err != nil {
		return err
	}
	if consumedBy.Valid {
		return status.Error(codes.FailedPrecondition, "thread context prefix is already consumed")
	}
	if requested == nil {
		return status.Error(codes.FailedPrecondition, "unconsumed thread context prefix is omitted")
	}
	if requested.GetChildThreadId() != scope.GetSessionThreadId() ||
		requested.GetParentBoundaryEventId() != parentBoundaryEventID {
		return status.Error(codes.InvalidArgument, "thread context prefix consumption identity is invalid")
	}
	updateResult, err := tx.Exec(ctx,
		`UPDATE session_thread_context_prefixes
		    SET consumed_by_checkpoint_message_id = $4
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND child_thread_id = $3
		    AND consumed_by_checkpoint_message_id IS NULL`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		checkpointMessageID,
	)
	if err != nil {
		return err
	}
	if !runtimecontrol.RowsAffected(updateResult) {
		return status.Error(codes.FailedPrecondition, "thread context prefix consumption lost its fence")
	}
	return nil
}

func runtimeAssistantContextParts(delta *bridgev1.RuntimeContextDelta, preparedTool *preparedRuntimeToolDeclaration) ([]map[string]any, error) {
	if preparedTool != nil {
		return preparedTool.contextParts, nil
	}
	return canonicalRuntimeContextParts(delta)
}

func appendRuntimeAssistantContextTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	eventType string,
	eventID string,
	modelRequestID string,
	delta *bridgev1.RuntimeContextDelta,
	now time.Time,
) (durableContextWrite, error) {
	return appendPreparedRuntimeAssistantContextTx(ctx, tx, scope, eventType, eventID, modelRequestID, delta, nil, now)
}

func appendPreparedRuntimeAssistantContextTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	eventType string,
	eventID string,
	modelRequestID string,
	delta *bridgev1.RuntimeContextDelta,
	preparedTool *preparedRuntimeToolDeclaration,
	now time.Time,
) (durableContextWrite, error) {
	if modelRequestID == "" || delta == nil || len(delta.GetParts()) == 0 {
		return durableContextWrite{}, status.Error(codes.InvalidArgument, "assistant context append is incomplete")
	}
	switch eventType {
	case "agent.message", "agent.tool_use", "agent.mcp_tool_use", "model_request", "internal_tool_repair":
	default:
		return durableContextWrite{}, status.Error(codes.InvalidArgument, "event cannot append assistant context")
	}

	header, found, err := runtimecontrol.LockAssistantMessageHeaderTx(ctx, tx, scope, modelRequestID)
	if err != nil {
		return durableContextWrite{}, err
	}

	toolCount := 0
	textCount := 0
	createdToolIDs := make([]string, 0)
	declaredParts, err := runtimeAssistantContextParts(delta, preparedTool)
	if err != nil {
		return durableContextWrite{}, err
	}
	parts := make([]runtimecontrol.AssistantPart, 0, len(declaredParts))
	for _, part := range declaredParts {
		if part["type"] == "tool_call" {
			toolCount++
			createdToolIDs = append(createdToolIDs, eventID)
		}
		if part["type"] == "text" {
			textCount++
		}
		if runtimecontrol.ValidateStoredRuntimeContextPart(part) != nil {
			return durableContextWrite{}, status.Error(codes.InvalidArgument, "runtime context part is invalid")
		}
		appended := runtimecontrol.AssistantPart{Value: part}
		if part["type"] == "reasoning" {
			appended.ReasoningBytes, err = stableReasoningCharge(part)
			if err != nil {
				return durableContextWrite{}, err
			}
		}
		parts = append(parts, appended)
	}
	switch eventType {
	case "agent.tool_use", "agent.mcp_tool_use":
		if toolCount != 1 || textCount != 0 {
			return durableContextWrite{}, status.Error(codes.InvalidArgument, "Tool Use append must contain exactly one Tool Call")
		}
	case "agent.message":
		if textCount != 1 || toolCount != 0 {
			return durableContextWrite{}, status.Error(codes.InvalidArgument, "message append must contain exactly one text member")
		}
	case "model_request":
		if toolCount != 0 || textCount != 0 {
			return durableContextWrite{}, status.Error(codes.InvalidArgument, "request-end append may contain only reasoning context")
		}
	case "internal_tool_repair":
		pairStart := len(declaredParts) - 2
		if toolCount != 1 || pairStart < 0 || declaredParts[pairStart]["type"] != "tool_call" || declaredParts[pairStart+1]["type"] != "tool_result" {
			return durableContextWrite{}, status.Error(codes.InvalidArgument, "internal Tool repair must end with one Tool Call and one Tool result")
		}
		for _, part := range declaredParts[:pairStart] {
			if part["type"] != "reasoning" {
				return durableContextWrite{}, status.Error(codes.InvalidArgument, "internal Tool repair prefix must contain only reasoning")
			}
		}
	}
	// Every declaration charges its new reasoning against the request's
	// stored counters; earlier parts are neither read nor re-serialized.
	request := runtimecontrol.AssistantPartsAppend{
		Scope: scope, ModelRequestID: modelRequestID, SourceEventID: eventID, Parts: parts, Now: now,
		ReasoningBudget: &runtimecontrol.ReasoningBudget{
			Parts: MaxStableReasoningPartsPerRequest,
			Bytes: MaxStableReasoningBytesPerRequest,
		},
	}
	if found {
		request.Header = &header
	}
	written, err := runtimecontrol.AppendAssistantMessagePartsTx(ctx, tx, request)
	if err != nil {
		return durableContextWrite{}, err
	}
	return durableContextWrite{MessageID: written.MessageID, MessageSequence: written.Sequence, CreatedToolUseEventIDs: createdToolIDs}, nil
}

func lockThreadMutationOnlyTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) error {
	_, err := runtimecontrol.LockThreadMutationTx(ctx, tx, scope)
	return err
}

func insertCompactionContextEntryTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	sourceEventID string,
	modelRequestID *string,
	delta *bridgev1.RuntimeContextDelta,
	now time.Time,
) (durableContextWrite, error) {
	if delta == nil {
		return durableContextWrite{}, status.Error(codes.InvalidArgument, "runtime context create identity is invalid")
	}
	const contextKind = "compaction"
	// A compaction summary is text only. Its parts never carry reasoning,
	// provider metadata or Tool identity, so no Tool relation is admitted from
	// compaction and the historical identity events stay authoritative.
	for _, part := range delta.GetParts() {
		if part.GetText() == nil {
			return durableContextWrite{}, status.Error(codes.InvalidArgument, "compaction context must contain only text")
		}
	}
	parts, err := canonicalRuntimeContextParts(delta)
	if err != nil {
		return durableContextWrite{}, err
	}
	var messageSequence int64
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(sequence), 0) + 1
		   FROM session_messages
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(),
	).Scan(&messageSequence); err != nil {
		return durableContextWrite{}, err
	}
	messageID := id.New("msg_")
	dataJSON, err := runtimecontrol.RuntimeContextDataJSON(parts)
	if err != nil {
		return durableContextWrite{}, err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_messages (
			workspace_id, session_id, session_thread_id, message_id, sequence, kind,
			data_json, source_event_id, model_request_id, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10)`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(),
		messageID, messageSequence, contextKind, dataJSON, sourceEventID, modelRequestID, now,
	); err != nil {
		return durableContextWrite{}, err
	}
	return durableContextWrite{MessageID: messageID, MessageSequence: messageSequence}, nil
}

type internalToolRepairDurableFacts struct {
	RepairEventID           string `json:"repairEventId"`
	AssignedMessageSequence int64  `json:"assignedMessageSequence"`
}

func commitInternalToolRepairContextTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	eventID string,
	modelRequestID string,
	modelToolCallID string,
	toolName string,
	canonicalInputJSON string,
	toolError *bridgev1.RuntimeToolError,
	reasoningPrefix *bridgev1.RuntimeContextDelta,
	now time.Time,
) (internalToolRepairDurableFacts, error) {
	if toolError == nil {
		return internalToolRepairDurableFacts{}, status.Error(codes.InvalidArgument, "internal Tool repair error is missing")
	}
	delta := &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{
		{Content: &bridgev1.RuntimeContextPart_ToolCall{ToolCall: &bridgev1.RuntimeContextToolCall{
			ModelToolCallId: modelToolCallID, ToolName: toolName, ProviderInputJson: canonicalInputJSON,
		}}},
		{Content: &bridgev1.RuntimeContextPart_ToolResult{ToolResult: &bridgev1.RuntimeContextToolResult{
			ModelToolCallId: modelToolCallID,
			Outcome: &bridgev1.RuntimeContextToolResult_Error{Error: &bridgev1.RuntimeContextToolError{
				ErrorJson: toolError.GetErrorJson(),
			}},
		}}},
	}}
	parts := make([]*bridgev1.RuntimeContextPart, 0, len(reasoningPrefix.GetParts())+len(delta.Parts))
	parts = append(parts, reasoningPrefix.GetParts()...)
	delta.Parts = append(parts, delta.Parts...)
	write, err := appendRuntimeAssistantContextTx(
		ctx, tx, scope, "internal_tool_repair", eventID, modelRequestID, delta, now,
	)
	if err != nil {
		return internalToolRepairDurableFacts{}, err
	}
	return internalToolRepairDurableFacts{RepairEventID: eventID, AssignedMessageSequence: write.MessageSequence}, nil
}

type declarationApplicationObservation struct {
	BindingID         string
	BindingGeneration int64
	Current           bool
}

func logRuntimeDeclaration(
	logger *slog.Logger,
	scope *bridgev1.RuntimeScope,
	operation string,
	sourceKind string,
	operationID string,
	declarationDigest string,
	duplicate bool,
	observation declarationApplicationObservation,
) {
	if logger == nil || scope == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	eventKind := "runtime_declaration_committed"
	outcome := "committed"
	if duplicate {
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
		slog.Bool("runtime.binding.current", observation.Current),
		slog.String("binding.id", observation.BindingID),
		slog.Int64("binding.generation", observation.BindingGeneration),
		slog.String("outcome", outcome),
	)
}

type runtimeDeclarationRejectionEvidence struct {
	Kind          string
	Operation     string
	OperationID   string
	MessageOrPart string
	ThreadRole    string
}

func logRuntimeDeclarationRejected(logger *slog.Logger, scope *bridgev1.RuntimeScope, evidence runtimeDeclarationRejectionEvidence, rejection error) {
	if logger == nil || rejection == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	kind := evidence.Kind
	if status.Code(rejection) == codes.AlreadyExists {
		kind = "replay_conflict"
	}
	if kind == "transaction" {
		switch status.Code(rejection) {
		case codes.InvalidArgument, codes.FailedPrecondition, codes.NotFound:
			kind = "lineage"
		default:
			kind = "durable_constraint"
		}
	}
	if kind == "" {
		kind = "durable_constraint"
	}
	attributes := []any{
		slog.String("operation", evidence.Operation),
		slog.String("event.kind", "runtime_declaration_rejected"),
		slog.String("component", ServiceNameBridgeAPI),
		slog.String("phase", "declaration_validation"),
		slog.String("reason", kind),
		slog.String("rejection.kind", kind),
		slog.String("rpc.grpc.status_name", status.Code(rejection).String()),
	}
	if scope != nil {
		attributes = append(attributes,
			slog.String("workspace.id", safeActorDiagnosticIdentity(scope.GetWorkspaceId())),
			slog.String("session.id", safeActorDiagnosticIdentity(scope.GetSessionId())),
			slog.String("thread.id", safeActorDiagnosticIdentity(scope.GetSessionThreadId())),
		)
	}
	if evidence.OperationID != "" {
		attributes = append(attributes, slog.String("operation.id", safeActorDiagnosticIdentity(evidence.OperationID)))
	}
	if evidence.MessageOrPart != "" {
		attributes = append(attributes, slog.String("declaration.kind", evidence.MessageOrPart))
	}
	if evidence.ThreadRole != "" {
		attributes = append(attributes, slog.String("thread.role", safeActorDiagnosticIdentity(evidence.ThreadRole)))
	}
	logger.Error("runtime declaration rejected", attributes...)
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
	var podUID, processID string
	err := tx.QueryRow(ctx,
		`SELECT binding_id, binding_generation, agent_runtime_pod_uid, runtime_process_id
		   FROM session_runtime_bindings
		  WHERE workspace_id = $1
		    AND session_id = $2`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
	).Scan(&observation.BindingID, &observation.BindingGeneration, &podUID, &processID)
	if dbconnect.IsNoRows(err) {
		return observation, nil
	}
	if err != nil {
		return declarationApplicationObservation{}, err
	}
	if observation.BindingID == scope.GetBinding().GetBindingId() &&
		observation.BindingGeneration == scope.GetBinding().GetBindingGeneration() &&
		podUID == scope.GetBinding().GetTargetPodUid() && processID == scope.GetBinding().GetRuntimeProcessId() {
		observation.Current = true
	}
	return observation, nil
}

func (s *PostgreSQLBridgeAPIStore) runtimeScopeApplicationCurrent(ctx context.Context, scope *bridgev1.RuntimeScope) (bool, error) {
	observation, err := s.declarationApplicationObservation(ctx, scope)
	return observation.Current, err
}

// marshalPreparedRuntimeToolDeclarationObject reuses the fresh canonical raw
// inputs owned by successful normalization, avoiding their redundant encoder
// validation. Other fields retain the ordinary encoder; changed or unprepared
// envelopes fall back to it. Separator restoration must cover the assembled
// object before the caller performs whole-envelope canonicalization.
func marshalPreparedRuntimeToolDeclarationObject(value map[string]any, prepared preparedRuntimeToolDeclaration) ([]byte, error) {
	if !prepared.rawInputsNormalized {
		return marshalRuntimeDeclarationObject(value)
	}
	keys := []string{"assistant_context_delta", "evaluated_permission", "event_type", "mcp_server_name", "model_request_id", "model_tool_call_id", "operation_kind", "provider_input", "public_execution_input", "route_capability", "runtime_write_id", "session_thread_id", "tool_name"}
	if len(value) != len(keys) {
		return marshalRuntimeDeclarationObject(value)
	}
	for _, key := range keys {
		if _, ok := value[key]; !ok {
			return marshalRuntimeDeclarationObject(value)
		}
	}
	fields := make([][]byte, len(keys))
	size := 2 + len(keys) - 1
	for i, key := range keys {
		if key == "provider_input" || key == "public_execution_input" {
			raw, ok := value[key].(json.RawMessage)
			owned := prepared.projection.ProviderInput
			if key == "public_execution_input" {
				owned = prepared.projection.CanonicalExecutionInput
			}
			if !ok || len(raw) == 0 || !bytes.Equal(raw, owned) {
				return marshalRuntimeDeclarationObject(value)
			}
			fields[i] = raw
			size += len(key) + 3 + len(raw)
			continue
		}
		var field bytes.Buffer
		encoder := json.NewEncoder(&field)
		encoder.SetEscapeHTML(false)
		if err := encoder.Encode(map[string]any{key: value[key]}); err != nil {
			return marshalRuntimeDeclarationObject(value)
		}
		encoded := bytes.TrimSuffix(field.Bytes(), []byte{'\n'})
		fields[i] = encoded[1 : len(encoded)-1]
		size += len(fields[i])
	}
	encoded := make([]byte, 0, size)
	encoded = append(encoded, '{')
	for i, key := range keys {
		if i > 0 {
			encoded = append(encoded, ',')
		}
		if key == "provider_input" || key == "public_execution_input" {
			encoded = append(encoded, '"')
			encoded = append(encoded, key...)
			encoded = append(encoded, '"', ':')
		}
		encoded = append(encoded, fields[i]...)
	}
	encoded = append(encoded, '}')
	return runtimecontrol.RestoreJSONStringifySeparatorEscapes(encoded), nil
}
