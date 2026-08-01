package tetralsandbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/tetral-ai/tetral/internal/sandbox"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
)

type ExecutionReadiness string

const (
	ExecutionReady           ExecutionReadiness = "ready"
	ExecutionNeedsActivation ExecutionReadiness = "needs_activation"
	ExecutionNeedsCreation   ExecutionReadiness = "needs_creation"
)

type ProviderEffectBoundary string

const (
	ProviderProvedNotStarted ProviderEffectBoundary = "proved_not_started"
	ProviderSubmitted        ProviderEffectBoundary = "submitted"
	ProviderOutcomeUnknown   ProviderEffectBoundary = "outcome_unknown"
)

type ProviderDisposition string

const (
	ProviderRetryable ProviderDisposition = "retryable"
	ProviderTerminal  ProviderDisposition = "terminal"
)

type ProviderOutcome[T any] struct {
	Value          T
	EffectBoundary ProviderEffectBoundary
	Disposition    ProviderDisposition
	ErrorKind      string
	SafeMessage    string
}

func (o ProviderOutcome[T]) Failed() bool {
	return o.EffectBoundary != "" || o.Disposition != "" || o.ErrorKind != ""
}

type ActivationKind string

const (
	ActivationCreate  ActivationKind = "create"
	ActivationStart   ActivationKind = "start"
	ActivationReplace ActivationKind = "replace"
)

type ActivationRequest struct {
	Kind          ActivationKind
	Setup         sandbox.SandboxSetup
	CurrentHandle sandbox.ProviderHandle
}

type ActivationResolutionRequest struct {
	StableName string
	Labels     map[string]string
}

type ActivationResolution struct {
	Found  bool
	Handle sandbox.ProviderHandle
}

type MaterializationRequest struct {
	Setup                       sandbox.SandboxSetup
	Handle                      sandbox.ProviderHandle
	BindingRevision             int64
	TargetEnvironmentGeneration int64
	TargetResourceRevision      int64
}

type MaterializationResult struct {
	Resources                         sandbox.ResourceSetup
	MaterializedEnvironmentGeneration int64
	MaterializedResourceRevision      int64
}

type ToolExecutionRequest struct {
	Handle     sandbox.ProviderHandle
	Invocation sandboxdriver.ToolInvocation
	Prepared   ProviderPreparedTool
}

type ProviderPreparedTool interface {
	providerPreparedTool()
}

type ToolPreparationResult struct {
	Prepared        ProviderPreparedTool
	ImmediateResult *sandboxdriver.ToolExecution
}

type daytonaPreparedTool struct {
	value sandboxdriver.PreparedToolExecution
}

func (daytonaPreparedTool) providerPreparedTool() {}

type ReleaseRequest struct {
	Handle sandbox.ProviderHandle
	Reason sandbox.ReleaseReason
}

type ReleaseResult struct {
	Released bool
}

type ProviderAdapter interface {
	InspectForExecution(context.Context, string) ProviderOutcome[ExecutionReadiness]
	ResolveActivation(context.Context, ActivationResolutionRequest) ProviderOutcome[ActivationResolution]
	Activate(context.Context, ActivationRequest) ProviderOutcome[sandbox.ProviderHandle]
	MaterializeResources(context.Context, MaterializationRequest) ProviderOutcome[MaterializationResult]
	PrepareTool(context.Context, ToolExecutionRequest) ProviderOutcome[ToolPreparationResult]
	ExecuteTool(context.Context, ToolExecutionRequest) ProviderOutcome[sandboxdriver.ToolExecution]
	ObserveTool(context.Context, sandboxdriver.ForegroundCommandObservation) ProviderOutcome[sandboxdriver.ToolExecution]
	Release(context.Context, ReleaseRequest) ProviderOutcome[ReleaseResult]
}

type ProviderRegistry struct {
	adapters map[string]ProviderAdapter
}

func NewProviderRegistry(adapters map[string]ProviderAdapter) (*ProviderRegistry, error) {
	if len(adapters) != 1 {
		return nil, errors.New("sandbox provider registry must contain exactly one alpha provider")
	}
	adapter, ok := adapters[sandboxdriver.DaytonaProviderName]
	if !ok || adapter == nil {
		return nil, errors.New("sandbox provider registry requires daytona")
	}
	return &ProviderRegistry{adapters: map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter}}, nil
}

func (r *ProviderRegistry) Resolve(provider string) (ProviderAdapter, bool) {
	if r == nil {
		return nil, false
	}
	adapter, ok := r.adapters[provider]
	return adapter, ok
}

type DaytonaAdapter struct {
	Lifecycle sandbox.LifecycleProvider
	Resolver  DaytonaSandboxResolver
	Tools     DaytonaToolExecutor
	Resources DaytonaResourceMaterialization
}

type DaytonaSandboxResolver interface {
	ResolveSandbox(context.Context, string, map[string]string) (sandbox.ProviderHandle, bool, error)
}

type DaytonaToolExecutor interface {
	PrepareTool(context.Context, sandboxdriver.ToolInvocation) (sandboxdriver.PreparedToolExecution, error)
	ExecutePreparedTool(context.Context, sandboxdriver.PreparedToolExecution) (sandboxdriver.ToolExecution, error)
	SubmitPreparedTool(context.Context, sandboxdriver.PreparedToolExecution) (sandboxdriver.ToolExecution, error)
	ObserveForegroundTool(context.Context, sandboxdriver.ForegroundCommandObservation) (sandboxdriver.ToolExecution, error)
	ReadCommandResult(context.Context, sandboxdriver.CommandReference) (sandboxdriver.CommandResult, error)
	SendCommandInput(context.Context, sandboxdriver.CommandInput) (sandboxdriver.CommandResult, error)
	CancelCommand(context.Context, sandboxdriver.CommandCancel) (sandboxdriver.CommandResult, error)
}

func (a *DaytonaAdapter) PollBackground(ctx context.Context, reference sandboxdriver.CommandReference) ProviderOutcome[sandboxdriver.CommandResult] {
	if a == nil || a.Tools == nil {
		return terminalProviderFailure[sandboxdriver.CommandResult]("provider_configuration_invalid", "daytona background command adapter is unavailable")
	}
	result, err := a.Tools.ReadCommandResult(ctx, reference)
	if err != nil {
		return outcomeFromProviderError[sandboxdriver.CommandResult](err, ProviderProvedNotStarted)
	}
	return normalizeBackgroundCommandResult(result)
}

func (a *DaytonaAdapter) SendBackgroundInput(ctx context.Context, input sandboxdriver.CommandInput) ProviderOutcome[sandboxdriver.CommandResult] {
	if a == nil || a.Tools == nil {
		return terminalProviderFailure[sandboxdriver.CommandResult]("provider_configuration_invalid", "daytona background command adapter is unavailable")
	}
	result, err := a.Tools.SendCommandInput(ctx, input)
	if err != nil {
		return outcomeFromProviderError[sandboxdriver.CommandResult](err, ProviderOutcomeUnknown)
	}
	return normalizeBackgroundCommandResult(result)
}

func (a *DaytonaAdapter) CancelBackground(ctx context.Context, cancel sandboxdriver.CommandCancel) ProviderOutcome[sandboxdriver.CommandResult] {
	if a == nil || a.Tools == nil {
		return terminalProviderFailure[sandboxdriver.CommandResult]("provider_configuration_invalid", "daytona background command adapter is unavailable")
	}
	result, err := a.Tools.CancelCommand(ctx, cancel)
	if err != nil {
		return outcomeFromProviderError[sandboxdriver.CommandResult](err, ProviderOutcomeUnknown)
	}
	return normalizeBackgroundCommandResult(result)
}

func normalizeBackgroundCommandResult(result sandboxdriver.CommandResult) ProviderOutcome[sandboxdriver.CommandResult] {
	if strings.TrimSpace(result.ResultJSON) == "" || !json.Valid([]byte(result.ResultJSON)) {
		return terminalProviderFailure[sandboxdriver.CommandResult]("provider_response_malformed", "daytona returned a malformed background command result")
	}
	return ProviderOutcome[sandboxdriver.CommandResult]{Value: result}
}

// DaytonaResourceMaterialization owns Daytona-specific credentials, mounts,
// repository checkout, memory projection, and helper execution behind the
// provider adapter boundary.
type DaytonaResourceMaterialization interface {
	MaterializeResources(context.Context, sandbox.SandboxSetup, sandbox.ProviderHandle) (sandbox.ResourceSetup, error)
}

func (a *DaytonaAdapter) ResolveActivation(ctx context.Context, request ActivationResolutionRequest) ProviderOutcome[ActivationResolution] {
	if a == nil || a.Resolver == nil || request.StableName == "" || len(request.Labels) == 0 {
		return terminalProviderFailure[ActivationResolution]("provider_configuration_invalid", "daytona activation resolution is unavailable")
	}
	handle, found, err := a.Resolver.ResolveSandbox(ctx, request.StableName, request.Labels)
	if err != nil {
		return outcomeFromProviderError[ActivationResolution](err, ProviderProvedNotStarted)
	}
	return ProviderOutcome[ActivationResolution]{Value: ActivationResolution{Found: found, Handle: handle}}
}

func (a *DaytonaAdapter) InspectForExecution(ctx context.Context, providerResourceID string) ProviderOutcome[ExecutionReadiness] {
	if a == nil || a.Lifecycle == nil || providerResourceID == "" {
		return terminalProviderFailure[ExecutionReadiness]("provider_configuration_invalid", "daytona adapter is unavailable")
	}
	status, err := a.Lifecycle.GetStatus(ctx, sandbox.ProviderHandle{Provider: sandboxdriver.DaytonaProviderName, SandboxID: providerResourceID})
	if err != nil {
		var providerErr *sandbox.ProviderError
		if errors.As(err, &providerErr) && providerErr.Kind == sandbox.ProviderErrorNotFound {
			return ProviderOutcome[ExecutionReadiness]{Value: ExecutionNeedsCreation}
		}
		return outcomeFromProviderError[ExecutionReadiness](err, ProviderProvedNotStarted)
	}
	if status.Retryable {
		return ProviderOutcome[ExecutionReadiness]{
			EffectBoundary: ProviderProvedNotStarted,
			Disposition:    ProviderRetryable,
			ErrorKind:      "provider_transition_in_progress",
			SafeMessage:    status.SafeMessage,
		}
	}
	switch {
	case status.Availability == sandbox.ProviderAvailable && status.SandboxStatus == sandbox.StatusActive:
		return ProviderOutcome[ExecutionReadiness]{Value: ExecutionReady}
	case status.Availability == sandbox.ProviderMissing || status.SandboxStatus == sandbox.StatusReleased:
		return ProviderOutcome[ExecutionReadiness]{Value: ExecutionNeedsCreation}
	case status.Availability == sandbox.ProviderUnavailable && (status.SandboxStatus == sandbox.StatusStopped || status.SandboxStatus == sandbox.StatusArchived):
		return ProviderOutcome[ExecutionReadiness]{Value: ExecutionNeedsActivation}
	default:
		return terminalProviderFailure[ExecutionReadiness]("provider_response_malformed", "daytona returned an unusable execution state")
	}
}

func (a *DaytonaAdapter) Activate(ctx context.Context, request ActivationRequest) ProviderOutcome[sandbox.ProviderHandle] {
	if a == nil || a.Lifecycle == nil {
		return terminalProviderFailure[sandbox.ProviderHandle]("provider_configuration_invalid", "daytona adapter is unavailable")
	}
	switch request.Kind {
	case ActivationCreate, ActivationReplace:
		handle, err := a.Lifecycle.CreateSandbox(ctx, sandbox.CreateSandboxRequest{Setup: request.Setup})
		if err != nil {
			return outcomeFromProviderError[sandbox.ProviderHandle](err, ProviderOutcomeUnknown)
		}
		return ProviderOutcome[sandbox.ProviderHandle]{Value: handle}
	case ActivationStart:
		if request.CurrentHandle.SandboxID == "" {
			return terminalProviderFailure[sandbox.ProviderHandle]("provider_request_invalid", "daytona start requires a provider resource")
		}
		if err := a.Lifecycle.StartSandbox(ctx, request.CurrentHandle); err != nil {
			return outcomeFromProviderError[sandbox.ProviderHandle](err, ProviderOutcomeUnknown)
		}
		return ProviderOutcome[sandbox.ProviderHandle]{Value: request.CurrentHandle}
	default:
		return terminalProviderFailure[sandbox.ProviderHandle]("provider_request_invalid", "sandbox activation kind is invalid")
	}
}

func (a *DaytonaAdapter) MaterializeResources(ctx context.Context, request MaterializationRequest) ProviderOutcome[MaterializationResult] {
	if a == nil || a.Lifecycle == nil || a.Resources == nil || request.Handle.SandboxID == "" {
		return terminalProviderFailure[MaterializationResult]("provider_configuration_invalid", "daytona materialization is unavailable")
	}
	if err := a.Lifecycle.CheckBaseTemplateHealth(ctx, request.Handle); err != nil {
		return outcomeFromProviderError[MaterializationResult](err, ProviderProvedNotStarted)
	}
	if err := a.Lifecycle.ApplyNetworkPolicy(ctx, request.Handle, request.Setup.Network); err != nil {
		return materializationOutcomeFromProviderError[MaterializationResult](err)
	}
	if err := a.Lifecycle.PrepareBaseDirectories(ctx, request.Handle); err != nil {
		return materializationOutcomeFromProviderError[MaterializationResult](err)
	}
	resources, err := a.Resources.MaterializeResources(ctx, request.Setup, request.Handle)
	if err != nil {
		return materializationOutcomeFromProviderError[MaterializationResult](err)
	}
	if resources.ResourceCredExpiresAt == nil {
		return ProviderOutcome[MaterializationResult]{
			EffectBoundary: ProviderSubmitted,
			Disposition:    ProviderTerminal,
			ErrorKind:      "provider_response_malformed",
			SafeMessage:    "daytona materialization returned an incomplete receipt",
		}
	}
	if err := a.Lifecycle.CheckBaseTemplateHealth(ctx, request.Handle); err != nil {
		return materializationOutcomeFromProviderError[MaterializationResult](err)
	}
	return ProviderOutcome[MaterializationResult]{Value: MaterializationResult{
		Resources:                         resources,
		MaterializedEnvironmentGeneration: request.TargetEnvironmentGeneration,
		MaterializedResourceRevision:      request.TargetResourceRevision,
	}}
}

func (a *DaytonaAdapter) PrepareTool(ctx context.Context, request ToolExecutionRequest) ProviderOutcome[ToolPreparationResult] {
	if a == nil || a.Tools == nil || request.Handle.SandboxID == "" {
		return terminalProviderFailure[ToolPreparationResult]("provider_configuration_invalid", "daytona tool preparation is unavailable")
	}
	request.Invocation.Target.ProviderSandboxID = request.Handle.SandboxID
	prepared, err := a.Tools.PrepareTool(ctx, request.Invocation)
	if err != nil {
		return outcomeFromProviderError[ToolPreparationResult](err, ProviderProvedNotStarted)
	}
	result := ToolPreparationResult{Prepared: daytonaPreparedTool{value: prepared}, ImmediateResult: prepared.ImmediateResult()}
	if result.ImmediateResult != nil && (strings.TrimSpace(result.ImmediateResult.ResultJSON) == "" || !json.Valid([]byte(result.ImmediateResult.ResultJSON))) {
		return terminalProviderFailure[ToolPreparationResult]("provider_response_malformed", "daytona returned a malformed pre-execution result")
	}
	return ProviderOutcome[ToolPreparationResult]{Value: result}
}

func (a *DaytonaAdapter) ExecuteTool(ctx context.Context, request ToolExecutionRequest) ProviderOutcome[sandboxdriver.ToolExecution] {
	if a == nil || a.Tools == nil || request.Handle.SandboxID == "" {
		return terminalProviderFailure[sandboxdriver.ToolExecution]("provider_configuration_invalid", "daytona tool execution is unavailable")
	}
	prepared, ok := request.Prepared.(daytonaPreparedTool)
	if !ok {
		return terminalProviderFailure[sandboxdriver.ToolExecution]("provider_request_invalid", "daytona tool execution requires a prepared payload")
	}
	result, err := a.Tools.SubmitPreparedTool(ctx, prepared.value)
	if err != nil {
		return ProviderOutcome[sandboxdriver.ToolExecution]{
			EffectBoundary: ProviderOutcomeUnknown,
			Disposition:    ProviderTerminal,
			ErrorKind:      "sandbox_execution_outcome_unknown",
			SafeMessage:    "daytona tool execution outcome is unknown",
		}
	}
	if strings.TrimSpace(result.ResultJSON) == "" || !json.Valid([]byte(result.ResultJSON)) {
		return ProviderOutcome[sandboxdriver.ToolExecution]{
			EffectBoundary: ProviderSubmitted,
			Disposition:    ProviderTerminal,
			ErrorKind:      "provider_response_malformed",
			SafeMessage:    "daytona returned a malformed tool result",
		}
	}
	return ProviderOutcome[sandboxdriver.ToolExecution]{Value: result}
}

func (a *DaytonaAdapter) ObserveTool(ctx context.Context, observation sandboxdriver.ForegroundCommandObservation) ProviderOutcome[sandboxdriver.ToolExecution] {
	if a == nil || a.Tools == nil {
		return terminalProviderFailure[sandboxdriver.ToolExecution]("provider_configuration_invalid", "daytona tool observation is unavailable")
	}
	if observation.Reference.Target.ProviderSandboxID == "" ||
		observation.Reference.Task.TaskID == "" || observation.Reference.Task.ProviderCommandID == "" {
		return terminalProviderFailure[sandboxdriver.ToolExecution]("provider_response_malformed", "daytona command reference is malformed")
	}
	result, err := a.Tools.ObserveForegroundTool(ctx, observation)
	if err != nil {
		return outcomeFromProviderError[sandboxdriver.ToolExecution](err, ProviderSubmitted)
	}
	if strings.TrimSpace(result.ResultJSON) == "" || !json.Valid([]byte(result.ResultJSON)) {
		return ProviderOutcome[sandboxdriver.ToolExecution]{
			EffectBoundary: ProviderSubmitted,
			Disposition:    ProviderTerminal,
			ErrorKind:      "provider_response_malformed",
			SafeMessage:    "daytona returned a malformed observed tool result",
		}
	}
	return ProviderOutcome[sandboxdriver.ToolExecution]{Value: result}
}

func (a *DaytonaAdapter) Release(ctx context.Context, request ReleaseRequest) ProviderOutcome[ReleaseResult] {
	if a == nil || a.Lifecycle == nil || request.Handle.SandboxID == "" {
		return terminalProviderFailure[ReleaseResult]("provider_request_invalid", "daytona release requires a provider resource")
	}
	if err := a.Lifecycle.ReleaseSandbox(ctx, request.Handle, request.Reason); err != nil {
		return outcomeFromProviderError[ReleaseResult](err, ProviderSubmitted)
	}
	return ProviderOutcome[ReleaseResult]{Value: ReleaseResult{Released: true}}
}

func materializationOutcomeFromProviderError[T any](err error) ProviderOutcome[T] {
	outcome := outcomeFromProviderError[T](err, ProviderSubmitted)
	if outcome.Disposition == ProviderRetryable {
		// Materialization is deterministic convergence. Re-running the same
		// immutable target replaces any partial provider-side writes before a
		// user command is authorized.
		outcome.EffectBoundary = ProviderProvedNotStarted
	}
	return outcome
}

func outcomeFromProviderError[T any](err error, boundary ProviderEffectBoundary) ProviderOutcome[T] {
	var providerErr *sandbox.ProviderError
	if !errors.As(err, &providerErr) {
		return terminalProviderFailure[T]("provider_response_malformed", "provider returned an unclassified failure")
	}
	disposition := ProviderTerminal
	if boundary != ProviderOutcomeUnknown && providerErr.Retryable {
		disposition = ProviderRetryable
	}
	return ProviderOutcome[T]{
		EffectBoundary: boundary,
		Disposition:    disposition,
		ErrorKind:      string(providerErr.Kind),
		SafeMessage:    providerErr.SafeMessage,
	}
}

func terminalProviderFailure[T any](kind string, message string) ProviderOutcome[T] {
	if kind == "" {
		kind = "provider_response_malformed"
	}
	return ProviderOutcome[T]{
		EffectBoundary: ProviderProvedNotStarted,
		Disposition:    ProviderTerminal,
		ErrorKind:      kind,
		SafeMessage:    fmt.Sprintf("%s", message),
	}
}
