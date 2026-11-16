package agentruntimebridge

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/internalgrpc"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// This file owns the machine-bound tool calls: admission of exec_command,
// edit and view_image calls for a Session computer, write_stdin controls and
// the turn-end cancel. internal/runtimecontrol writes their records.

// AdmitToolCall records one machine-bound call with its tool result row and
// dispatch record in one transaction and routes it to its computer.
func (s *PostgreSQLBridgeAPIStore) AdmitToolCall(ctx context.Context, request *bridgev1.AdmitToolCallRequest) (*bridgev1.AdmitToolCallResponse, error) {
	lifecycle, policyErr := s.lifecyclePolicy()
	if policyErr != nil {
		return nil, policyErr
	}
	ctx, cancelAdmission := context.WithTimeout(ctx, lifecycle.AdmissionTimeout)
	defer cancelAdmission()
	if err := validateDurableToolTarget(request.GetScope(), request.GetToolUseEventId()); err != nil {
		return nil, err
	}
	scope := request.GetScope()
	ctx = workload.WithFields(ctx, "session.id", scope.GetSessionId(), "computer.id", request.GetComputerId())
	ctx, span := workload.StartSpan(ctx, "tool.admit")
	defer span.End()
	var outcome runtimecontrol.AdmissionOutcome
	var routing *computer.AdmissionRouting
	err := s.withScopeTx(ctx, scope, "agentruntimebridge.admit_tool_call", func(tx *dbconnect.Tx) error {
		routing = &computer.AdmissionRouting{}
		tool, err := lockMachineToolRouteTx(ctx, tx, scope, request.GetToolUseEventId(), "machine_tool")
		if err != nil {
			return err
		}
		outcome, err = runtimecontrol.AdmitToolCallTx(ctx, tx, runtimecontrol.ToolCallAdmission{
			Key:             machineToolCallKey(scope, request.GetToolUseEventId()),
			ModelToolCallID: tool.ModelToolCallID,
			ComputerID:      request.GetComputerId(),
			Tool:            request.GetTool(),
			InputJSON:       request.GetInputJson(),
			Limits:          visibleLimitsOf(request.GetLimits()),
			Traceparent:     workload.ContextTraceparent(ctx),
			// The policy is validated by AdmitToolCallTx.
			SandboxPolicyJSON: request.GetSandboxPolicyJson(),
		}, routing)
		return err
	})
	if err != nil {
		if runtimecontrol.IsConversationMutationStaleError(err) {
			span.SetString("outcome", "stale")
			return &bridgev1.AdmitToolCallResponse{Outcome: &bridgev1.AdmitToolCallResponse_Stale{Stale: &bridgev1.ToolCallStale{}}}, nil
		}
		err = machineAdmissionError(ctx, err)
		span.SetFailure("tool_admission_error", internalgrpc.StatusCodeName(status.Code(err)))
		return nil, err
	}
	span.SetString("outcome", outcome.Outcome)
	if outcome.TaskID != "" {
		span.SetString("task.id", outcome.TaskID)
	}
	if routing.Created != nil {
		computer.RecordRequestPhase(ctx, *routing.Created, "request", time.Time{})
	}
	for _, binding := range routing.Issued {
		computer.RecordPreparationStarted(ctx, binding)
	}
	switch outcome.Outcome {
	case runtimecontrol.AdmissionDuplicate:
		return &bridgev1.AdmitToolCallResponse{Outcome: &bridgev1.AdmitToolCallResponse_Duplicate{Duplicate: &bridgev1.ToolCallAdmitted{TaskId: outcome.TaskID}}}, nil
	case runtimecontrol.AdmissionTooLarge:
		return &bridgev1.AdmitToolCallResponse{Outcome: &bridgev1.AdmitToolCallResponse_TooLarge{TooLarge: tooLargeOf(outcome)}}, nil
	case runtimecontrol.AdmissionComputerNotAttached:
		return &bridgev1.AdmitToolCallResponse{Outcome: &bridgev1.AdmitToolCallResponse_ComputerNotAttached{ComputerNotAttached: &bridgev1.ToolCallComputerNotAttached{}}}, nil
	}
	return &bridgev1.AdmitToolCallResponse{Outcome: &bridgev1.AdmitToolCallResponse_Admitted{Admitted: &bridgev1.ToolCallAdmitted{TaskId: outcome.TaskID}}}, nil
}

// AdmitToolControl records one write_stdin and routes it on its task's
// recorded route.
func (s *PostgreSQLBridgeAPIStore) AdmitToolControl(ctx context.Context, request *bridgev1.AdmitToolControlRequest) (*bridgev1.AdmitToolControlResponse, error) {
	lifecycle, policyErr := s.lifecyclePolicy()
	if policyErr != nil {
		return nil, policyErr
	}
	ctx, cancelAdmission := context.WithTimeout(ctx, lifecycle.AdmissionTimeout)
	defer cancelAdmission()
	if err := validateDurableToolTarget(request.GetScope(), request.GetToolUseEventId()); err != nil {
		return nil, err
	}
	if request.GetTaskId() == "" {
		return nil, status.Error(codes.InvalidArgument, "task id is required")
	}
	scope := request.GetScope()
	var outcome runtimecontrol.AdmissionOutcome
	err := s.withScopeTx(ctx, scope, "agentruntimebridge.admit_tool_control", func(tx *dbconnect.Tx) error {
		tool, err := lockMachineToolRouteTx(ctx, tx, scope, request.GetToolUseEventId(), "machine_control")
		if err != nil {
			return err
		}
		waitMS := protocol.Nullable[int64]{}
		if request.WaitMs != nil {
			waitMS = protocol.Value(request.GetWaitMs())
		}
		outcome, err = runtimecontrol.AdmitToolControlTx(ctx, tx, runtimecontrol.ToolControlAdmission{
			Key:             machineToolCallKey(scope, request.GetToolUseEventId()),
			ModelToolCallID: tool.ModelToolCallID,
			TaskID:          request.GetTaskId(),
			Chars:           request.GetChars(),
			WaitMS:          waitMS,
			Limits:          visibleLimitsOf(request.GetLimits()),
		})
		return err
	})
	if err != nil {
		if runtimecontrol.IsConversationMutationStaleError(err) {
			return &bridgev1.AdmitToolControlResponse{Outcome: &bridgev1.AdmitToolControlResponse_Stale{Stale: &bridgev1.ToolCallStale{}}}, nil
		}
		return nil, machineAdmissionError(ctx, err)
	}
	switch outcome.Outcome {
	case runtimecontrol.AdmissionDuplicate:
		return &bridgev1.AdmitToolControlResponse{Outcome: &bridgev1.AdmitToolControlResponse_Duplicate{Duplicate: &bridgev1.ToolCallAdmitted{TaskId: outcome.TaskID}}}, nil
	case runtimecontrol.AdmissionTooLarge:
		return &bridgev1.AdmitToolControlResponse{Outcome: &bridgev1.AdmitToolControlResponse_TooLarge{TooLarge: tooLargeOf(outcome)}}, nil
	}
	return &bridgev1.AdmitToolControlResponse{Outcome: &bridgev1.AdmitToolControlResponse_Admitted{Admitted: &bridgev1.ToolCallAdmitted{TaskId: outcome.TaskID}}}, nil
}

// CancelToolCall cancels one machine-bound call of a turn that ended
// normally, through runtimecontrol.CancelToolCallTx with cause turn_end. It
// does not wait for the cancel's reply.
func (s *PostgreSQLBridgeAPIStore) CancelToolCall(ctx context.Context, request *bridgev1.CancelToolCallRequest) (*bridgev1.CancelToolCallResponse, error) {
	if err := validateDurableToolTarget(request.GetScope(), request.GetToolUseEventId()); err != nil {
		return nil, err
	}
	scope := request.GetScope()
	err := s.withScopeTx(ctx, scope, "agentruntimebridge.cancel_tool_call", func(tx *dbconnect.Tx) error {
		if err := verifyRuntimeScopeTx(ctx, tx, scope); err != nil {
			return err
		}
		if err := lockSandboxExecutionThreadTx(ctx, tx, scope); err != nil {
			return err
		}
		_, err := runtimecontrol.CancelToolCallTx(ctx, tx, machineToolCallKey(scope, request.GetToolUseEventId()), runtimecontrol.CancelTurnEnd)
		return err
	})
	if err != nil {
		if runtimecontrol.IsConversationMutationStaleError(err) {
			return &bridgev1.CancelToolCallResponse{Outcome: &bridgev1.CancelToolCallResponse_Stale{Stale: &bridgev1.ToolCallStale{}}}, nil
		}
		return nil, machineAdmissionError(ctx, err)
	}
	return &bridgev1.CancelToolCallResponse{Outcome: &bridgev1.CancelToolCallResponse_Cancelled{Cancelled: &bridgev1.ToolCallCancelled{}}}, nil
}

// lockMachineToolRouteTx takes the locks and checks every Bridge-owned
// executor takes before its first effect: the Runtime scope and process, the
// thread, the thread mutation fence and the durable route of toolUseEventID
// with routeCapability. A retired process may still replay an admission it
// recorded, which AdmitToolCallTx answers as a duplicate before any effect.
func lockMachineToolRouteTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, toolUseEventID, routeCapability string) (runtimecontrol.DurableToolExecution, error) {
	proof, err := lockRuntimeReceiptScopeTx(ctx, tx, scope)
	if err != nil {
		return runtimecontrol.DurableToolExecution{}, err
	}
	if err := lockSandboxExecutionThreadTx(ctx, tx, scope); err != nil {
		return runtimecontrol.DurableToolExecution{}, err
	}
	tool, err := runtimecontrol.LoadDurableToolExecutionTx(ctx, tx, scope, toolUseEventID, "agent.tool_use", true)
	if err != nil {
		return runtimecontrol.DurableToolExecution{}, err
	}
	if existing, found, err := readRuntimeToolResultTx(ctx, tx, scope, toolUseEventID); err != nil || found && existing.ToolKind == toolKindOfRoute(routeCapability) {
		return tool, err
	}
	if err := proof.requireCurrent(tx); err != nil {
		return runtimecontrol.DurableToolExecution{}, err
	}
	if err := runtimecontrol.RequireThreadMutationAllowedTx(ctx, tx, scope); err != nil {
		return runtimecontrol.DurableToolExecution{}, err
	}
	return tool, lockExecutableToolRouteTx(ctx, tx, scope, toolUseEventID, routeCapability)
}

func toolKindOfRoute(routeCapability string) string {
	if routeCapability == "machine_control" {
		return bridgeToolKindMachineControl
	}
	return bridgeToolKindMachineTool
}

func machineToolCallKey(scope *bridgev1.RuntimeScope, toolUseEventID string) runtimecontrol.ToolCallKey {
	return runtimecontrol.ToolCallKey{WorkspaceID: workspace.ID(scope.GetWorkspaceId()), SessionID: scope.GetSessionId(),
		SessionThreadID: scope.GetSessionThreadId(), ToolUseEventID: toolUseEventID}
}

func visibleLimitsOf(limits *bridgev1.ToolVisibleLimits) protocol.Optional[protocol.VisibleLimits] {
	if limits == nil {
		return protocol.Optional[protocol.VisibleLimits]{}
	}
	var value protocol.VisibleLimits
	if limits.VisibleBytes != nil {
		value.VisibleBytes = protocol.Value(limits.GetVisibleBytes())
	}
	if limits.VisibleLines != nil {
		value.VisibleLines = protocol.Value(limits.GetVisibleLines())
	}
	return protocol.Some(value)
}

func tooLargeOf(outcome runtimecontrol.AdmissionOutcome) *bridgev1.ToolCallTooLarge {
	return &bridgev1.ToolCallTooLarge{Limit: outcome.Limit, LimitBytes: int64(outcome.LimitBytes)}
}

// machineAdmissionError passes a refusal's gRPC status through and answers
// every other failure, a database or routing failure, as Unavailable:
// nothing was admitted or sent, and the Runtime retries.
func machineAdmissionError(ctx context.Context, err error) error {
	err = bridgeContextError(ctx, err)
	if _, ok := status.FromError(err); ok && !runtimecontrol.IsRouteChanged(err) {
		return err
	}
	return status.Error(codes.Unavailable, errors.Join(errors.New("machine tool admission failed"), err).Error())
}

// AwaitToolCall returns the settlement record of one machine-bound call or
// write_stdin once its row is terminal_unconsumed; a consumed row answers
// FailedPrecondition. It sets no deadline of its own: between reads it waits
// for the call's result notice or the listener's reconnect, and when the RPC
// breaks the Runtime calls again, on any Bridge pod, whose first step is a
// read.
func (s *PostgreSQLBridgeAPIStore) AwaitToolCall(ctx context.Context, request *bridgev1.AwaitToolCallRequest) (response *bridgev1.AwaitToolCallResponse, err error) {
	defer func() { err = bridgeContextError(ctx, err) }()
	scope := request.GetScope()
	if err := validateDurableToolTarget(scope, request.GetToolUseEventId()); err != nil {
		return nil, err
	}
	hub := s.executionResultWake()
	key := executionResultKey(scope, request.GetToolUseEventId())
	wake := hub.register(key)
	defer hub.unregister(key)
	for {
		snapshot := wake.Snapshot()
		var call runtimeToolResult
		var found bool
		err := s.withScopeReadOnlyTx(ctx, scope, "agentruntimebridge.await_tool_call", func(tx *dbconnect.Tx) error {
			if err := verifyRuntimeReceiptScopeReadOnlyTx(ctx, tx, scope); err != nil {
				return err
			}
			var err error
			call, found, err = readRuntimeToolReceiptReadOnlyTx(ctx, tx, scope, request.GetToolUseEventId())
			return err
		})
		if runtimecontrol.IsScopeSupersededError(err) {
			return &bridgev1.AwaitToolCallResponse{Outcome: &bridgev1.AwaitToolCallResponse_Stale{Stale: &bridgev1.ToolCallStale{}}}, nil
		}
		if err != nil {
			return nil, err
		}
		if !found || (call.ToolKind != bridgeToolKindMachineTool && call.ToolKind != bridgeToolKindMachineControl) {
			return nil, status.Error(codes.FailedPrecondition, "no machine-bound tool call is admitted for this tool use")
		}
		switch call.ExecutionState.String {
		case "terminal_unconsumed":
			return &bridgev1.AwaitToolCallResponse{Outcome: &bridgev1.AwaitToolCallResponse_Settled{
				Settled: &bridgev1.ToolCallSettled{SettlementJson: call.ResultJSON}}}, nil
		case "consumed":
			return nil, status.Error(codes.FailedPrecondition, "the tool call's result is already consumed")
		}
		if err := wake.WaitForWake(ctx, snapshot); err != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
}
