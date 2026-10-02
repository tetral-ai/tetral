package runtimecontrol

import (
	"context"
	"encoding/json"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxrelease "github.com/tetral-ai/tetral/internal/sandbox/release"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func ResetTurnRetryCountersTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, now time.Time) error {
	_, err := tx.Exec(ctx,
		`UPDATE session_turn_retries
		    SET provider_attempts = 0,
		        compaction_attempts = 0,
		        updated_at = $4
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		now,
	)
	return err
}

type RuntimeTerminationResult struct {
	FailureEventID  string `json:"failure_event_id"`
	CloseoutEventID string `json:"closeout_event_id"`
}

// SettleRuntimeTerminationTx is the Session termination owner shared by
// Bridge's Runtime-declared terminal failure and Job Runner's exhausted
// interrupt fence and delivery exhaustion.
// Main-session termination also cancels queued and parked input: retaining the
// binding for closeout replay must not leave delivery authority for a terminal
// Session. Child termination remains scoped to active custody for that Thread.
func SettleRuntimeTerminationTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	threadScope ThreadMutationScope,
	runtimeWriteID string,
	failure RuntimeTerminationFailure,
	failureJSON string,
	now time.Time,
) (RuntimeTerminationResult, RuntimeTerminationCustodyTransitions, error) {
	if err := closeRuntimeTerminationSpansTx(ctx, tx, scope, failure, now); err != nil {
		return RuntimeTerminationResult{}, RuntimeTerminationCustodyTransitions{}, err
	}
	if err := settleRuntimeTerminationDurableFactsTx(ctx, tx, scope, threadScope, runtimeWriteID, failure, now); err != nil {
		return RuntimeTerminationResult{}, RuntimeTerminationCustodyTransitions{}, err
	}
	orphanToolUses, err := runtimeTerminationOrphanToolUsesTx(ctx, tx, scope, false)
	if err != nil {
		return RuntimeTerminationResult{}, RuntimeTerminationCustodyTransitions{}, err
	}
	if len(orphanToolUses) != 0 {
		return RuntimeTerminationResult{}, RuntimeTerminationCustodyTransitions{}, status.Error(codes.FailedPrecondition, "runtime termination has a live Tool use after derived settlement")
	}
	if threadScope.Role == "main" {
		rootCtx := withSessionRootTermination(
			ctx, scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), runtimeWriteID,
		)
		if err := closeRuntimeTerminatedSessionSiblingsTx(rootCtx, tx, scope, runtimeWriteID, now); err != nil {
			return RuntimeTerminationResult{}, RuntimeTerminationCustodyTransitions{}, err
		}
	}
	transitions, err := cancelRuntimeTerminationInputsTx(
		ctx, tx, scope, threadScope.Role == "main", true, now,
	)
	if err != nil {
		return RuntimeTerminationResult{}, RuntimeTerminationCustodyTransitions{}, err
	}
	// Runtime termination removes every current-thread and, for the main
	// thread, sibling Sandbox blocker before release readiness is evaluated
	// once for the Session. Queue custody is assigned in this transaction.
	releaseJobs, err := sandboxrelease.ReadyRequestsTx(ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId(), now, nil)
	if err != nil {
		return RuntimeTerminationResult{}, RuntimeTerminationCustodyTransitions{}, err
	}
	if _, err := queue.EnqueueBatchTx(ctx, tx, releaseJobs); err != nil {
		return RuntimeTerminationResult{}, RuntimeTerminationCustodyTransitions{}, err
	}
	errorStamp, err := appendRuntimeTerminationErrorTx(ctx, tx, scope, threadScope, runtimeWriteID, failureJSON, now)
	if err != nil {
		return RuntimeTerminationResult{}, RuntimeTerminationCustodyTransitions{}, err
	}
	statusStamp, err := appendRuntimeTerminatedStatusTx(ctx, tx, scope, threadScope, runtimeWriteID, now)
	if err != nil {
		return RuntimeTerminationResult{}, RuntimeTerminationCustodyTransitions{}, err
	}
	return RuntimeTerminationResult{
		FailureEventID: errorStamp.EventID, CloseoutEventID: statusStamp.EventID,
	}, transitions, nil
}

func ScopeForThread(scope *bridgev1.RuntimeScope, threadID string) *bridgev1.RuntimeScope {
	return &bridgev1.RuntimeScope{
		WorkspaceId:     scope.GetWorkspaceId(),
		SessionId:       scope.GetSessionId(),
		SessionThreadId: threadID,
		Binding: &bridgev1.RuntimeBindingRef{
			BindingId:         scope.GetBinding().GetBindingId(),
			BindingGeneration: scope.GetBinding().GetBindingGeneration(),
			TargetPodUid:      scope.GetBinding().GetTargetPodUid(),
			RuntimeProcessId:  scope.GetBinding().GetRuntimeProcessId(),
		},
	}
}

func ModelRequestEndPayloadJSON(request *bridgev1.WriteRequestEndRequest, requestStartEventID string, requestKind string, finishReason string, usage Usage) (string, error) {
	cacheRead := int64(0)
	if usage.InputCacheRead != nil {
		cacheRead = *usage.InputCacheRead
	}
	cacheWrite := int64(0)
	if usage.InputCacheWrite != nil {
		cacheWrite = *usage.InputCacheWrite
	}
	payload := map[string]any{
		"type":                   "span.model_request_end",
		"model_request_id":       request.GetModelRequestId(),
		"model_request_start_id": requestStartEventID,
		"request_kind":           requestKind,
		"is_error":               request.GetIsError(),
		"finish_reason":          finishReason,
		"model_usage": map[string]any{
			"input_tokens":                usage.InputTotal,
			"output_tokens":               usage.OutputTotal,
			"cache_creation_input_tokens": cacheWrite,
			"cache_read_input_tokens":     cacheRead,
			"speed":                       nil,
		},
		"request_usage": json.RawMessage(DefaultString(request.GetUsageJson(), "{}")),
		"provider_context_retention": map[string]any{
			"disposition":                request.GetProviderContextRetention().GetDisposition(),
			"assistant_message_sequence": request.GetProviderContextRetention().AssistantMessageSequence,
			"tool_use_event_ids":         request.GetProviderContextRetention().GetToolUseEventIds(),
			"repair_event_ids":           request.GetProviderContextRetention().GetRepairEventIds(),
		},
	}
	if request.GetErrorKind() != "" {
		payload["error_kind"] = request.GetErrorKind()
	}
	return MarshalJSON(payload)
}

func NormalizeRequestKind(value string) (string, error) {
	switch DefaultString(value, RequestKindAgentProviderRequest) {
	case RequestKindAgentProviderRequest:
		return RequestKindAgentProviderRequest, nil
	case RequestKindCompactionSummary:
		return RequestKindCompactionSummary, nil
	case RequestKindApprovalReviewer:
		return RequestKindApprovalReviewer, nil
	default:
		return "", status.Error(codes.InvalidArgument, "request_kind is invalid")
	}
}

func IdleStatusPayloadJSON(stopReasonJSON string) (string, error) {
	return MarshalJSON(map[string]any{
		"type":        "session.status_idle",
		"stop_reason": json.RawMessage(stopReasonJSON),
	})
}

func ThreadStatusPayloadJSON(eventType string, scope *bridgev1.RuntimeScope, threadScope ThreadMutationScope, stopReasonJSON string) (string, error) {
	payload := map[string]any{
		"type":              eventType,
		"session_thread_id": scope.GetSessionThreadId(),
		"task_name":         NullableJSONString(threadScope.TaskName),
	}
	if stopReasonJSON != "" {
		payload["stop_reason"] = json.RawMessage(stopReasonJSON)
	}
	return MarshalJSON(payload)
}

type Usage struct {
	InputTotal      int64
	InputUncached   int64
	InputCacheRead  *int64
	InputCacheWrite *int64
	OutputTotal     int64
	OutputReasoning *int64
	Total           *int64
}
