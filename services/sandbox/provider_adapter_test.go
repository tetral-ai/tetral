package tetralsandbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/sandbox"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
)

func TestDaytonaAdapterNormalizesExecutionReadiness(t *testing.T) {
	tests := []struct {
		name         string
		status       sandbox.ProviderStatus
		wantReady    ExecutionReadiness
		wantBoundary ProviderEffectBoundary
		wantRetry    ProviderDisposition
	}{
		{name: "started", status: sandbox.ProviderStatus{Availability: sandbox.ProviderAvailable, SandboxStatus: sandbox.StatusActive}, wantReady: ExecutionReady},
		{name: "stopped", status: sandbox.ProviderStatus{Availability: sandbox.ProviderUnavailable, SandboxStatus: sandbox.StatusStopped}, wantReady: ExecutionNeedsActivation},
		{name: "archived", status: sandbox.ProviderStatus{Availability: sandbox.ProviderUnavailable, SandboxStatus: sandbox.StatusArchived}, wantReady: ExecutionNeedsActivation},
		{name: "missing", status: sandbox.ProviderStatus{Availability: sandbox.ProviderMissing, SandboxStatus: sandbox.StatusReleased}, wantReady: ExecutionNeedsCreation},
		{name: "transition", status: sandbox.ProviderStatus{Availability: sandbox.ProviderUnavailable, SandboxStatus: sandbox.StatusCreating, Retryable: true}, wantBoundary: ProviderProvedNotStarted, wantRetry: ProviderRetryable},
		{name: "malformed", status: sandbox.ProviderStatus{Availability: sandbox.ProviderUnknown, SandboxStatus: sandbox.StatusFailed}, wantBoundary: ProviderProvedNotStarted, wantRetry: ProviderTerminal},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			adapter := &DaytonaAdapter{Lifecycle: &adapterLifecycleFake{status: test.status}}
			outcome := adapter.InspectForExecution(context.Background(), "provider-sandbox")
			if outcome.Value != test.wantReady || outcome.EffectBoundary != test.wantBoundary || outcome.Disposition != test.wantRetry {
				t.Fatalf("outcome = %+v; want readiness=%q boundary=%q disposition=%q", outcome, test.wantReady, test.wantBoundary, test.wantRetry)
			}
		})
	}
}

func TestDaytonaAdapterMapsProviderErrorsWithoutRawErrorState(t *testing.T) {
	adapter := &DaytonaAdapter{Lifecycle: &adapterLifecycleFake{err: &sandbox.ProviderError{
		Provider:    sandboxdriver.DaytonaProviderName,
		Stage:       sandbox.StageStatus,
		Kind:        sandbox.ProviderErrorUnavailable,
		Retryable:   true,
		SafeMessage: "provider unavailable",
	}}}
	outcome := adapter.InspectForExecution(context.Background(), "provider-sandbox")
	if outcome.EffectBoundary != ProviderProvedNotStarted || outcome.Disposition != ProviderRetryable || outcome.ErrorKind != string(sandbox.ProviderErrorUnavailable) {
		t.Fatalf("provider error outcome = %+v", outcome)
	}
	adapter.Lifecycle = &adapterLifecycleFake{err: errors.New("unclassified")}
	outcome = adapter.InspectForExecution(context.Background(), "provider-sandbox")
	if outcome.EffectBoundary != ProviderProvedNotStarted || outcome.Disposition != ProviderTerminal || outcome.ErrorKind != "provider_response_malformed" {
		t.Fatalf("unclassified outcome = %+v", outcome)
	}
}

func TestDaytonaAdapterTreatsMissingResourceAsCreationReadiness(t *testing.T) {
	adapter := &DaytonaAdapter{Lifecycle: &adapterLifecycleFake{err: &sandbox.ProviderError{
		Provider:    sandboxdriver.DaytonaProviderName,
		Stage:       sandbox.StageStatus,
		Kind:        sandbox.ProviderErrorNotFound,
		SafeMessage: "daytona sandbox not found",
	}}}
	outcome := adapter.InspectForExecution(context.Background(), "provider-sandbox")
	if outcome.Failed() || outcome.Value != ExecutionNeedsCreation {
		t.Fatalf("missing provider outcome = %+v; want needs_creation", outcome)
	}
}

func TestDaytonaAdapterDoesNotReplayAmbiguousActivation(t *testing.T) {
	retryableTimeout := &sandbox.ProviderError{
		Provider:    sandboxdriver.DaytonaProviderName,
		Stage:       sandbox.StageCreateSandbox,
		Kind:        sandbox.ProviderErrorTimeout,
		Retryable:   true,
		SafeMessage: "daytona sandbox request timed out",
	}
	tests := []struct {
		name    string
		request ActivationRequest
		fake    *adapterLifecycleFake
	}{
		{
			name:    "create",
			request: ActivationRequest{Kind: ActivationCreate},
			fake:    &adapterLifecycleFake{createErr: retryableTimeout},
		},
		{
			name:    "start",
			request: ActivationRequest{Kind: ActivationStart, CurrentHandle: sandbox.ProviderHandle{Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider-sandbox"}},
			fake:    &adapterLifecycleFake{startErr: retryableTimeout},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outcome := (&DaytonaAdapter{Lifecycle: test.fake}).Activate(context.Background(), test.request)
			if outcome.EffectBoundary != ProviderOutcomeUnknown || outcome.Disposition != ProviderTerminal {
				t.Fatalf("ambiguous activation outcome = %+v; want outcome_unknown + terminal", outcome)
			}
		})
	}
}

func TestDaytonaAdapterMaterializationRechecksHealthAndRequiresCredentialReceipt(t *testing.T) {
	expiresAt := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	lifecycle := &adapterLifecycleFake{}
	resources := &adapterResourceMaterializerFake{result: sandbox.ResourceSetup{
		ResourceCredExpiresAt: &expiresAt,
		ResourceRootsJSON:     "[]",
	}}
	adapter := &DaytonaAdapter{Lifecycle: lifecycle, Resources: resources}
	request := MaterializationRequest{
		Setup: sandbox.SandboxSetup{},
		Handle: sandbox.ProviderHandle{
			Provider:  sandboxdriver.DaytonaProviderName,
			SandboxID: "provider-sandbox",
		},
		TargetEnvironmentGeneration: 3,
		TargetResourceRevision:      7,
	}

	outcome := adapter.MaterializeResources(context.Background(), request)
	if outcome.Failed() {
		t.Fatalf("MaterializeResources outcome = %+v; want success", outcome)
	}
	if lifecycle.healthChecks != 2 {
		t.Fatalf("health checks = %d; want preflight and final verification", lifecycle.healthChecks)
	}

	resources.result.ResourceCredExpiresAt = nil
	outcome = adapter.MaterializeResources(context.Background(), request)
	if outcome.EffectBoundary != ProviderSubmitted || outcome.Disposition != ProviderTerminal || outcome.ErrorKind != "provider_response_malformed" {
		t.Fatalf("incomplete materialization outcome = %+v; want submitted terminal malformed response", outcome)
	}
}

func TestDaytonaAdapterRetriesDeterministicMaterializationAfterPartialWrite(t *testing.T) {
	retryable := &sandbox.ProviderError{
		Provider: sandboxdriver.DaytonaProviderName, Stage: sandbox.StageMountResources,
		Kind: sandbox.ProviderErrorUnavailable, Retryable: true, SafeMessage: "resource service unavailable",
	}
	adapter := &DaytonaAdapter{
		Lifecycle: &adapterLifecycleFake{},
		Resources: &adapterResourceMaterializerFake{err: retryable},
	}
	outcome := adapter.MaterializeResources(context.Background(), MaterializationRequest{
		Handle:                      sandbox.ProviderHandle{Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider-sandbox"},
		TargetEnvironmentGeneration: 3,
		TargetResourceRevision:      7,
	})
	if outcome.EffectBoundary != ProviderProvedNotStarted || outcome.Disposition != ProviderRetryable {
		t.Fatalf("partial materialization outcome = %+v; want retryable declarative convergence", outcome)
	}
}

func TestDaytonaAdapterRejectsMalformedToolResultAfterSubmission(t *testing.T) {
	tests := []struct {
		name       string
		resultJSON string
	}{
		{name: "empty"},
		{name: "invalid_json", resultJSON: "{"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			adapter := &DaytonaAdapter{Tools: &adapterToolExecutorFake{result: sandboxdriver.ToolExecution{ResultJSON: test.resultJSON}}}
			outcome := adapter.ExecuteTool(context.Background(), ToolExecutionRequest{
				Handle:   sandbox.ProviderHandle{Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider-sandbox"},
				Prepared: daytonaPreparedTool{},
			})
			if outcome.EffectBoundary != ProviderSubmitted || outcome.Disposition != ProviderTerminal || outcome.ErrorKind != "provider_response_malformed" {
				t.Fatalf("malformed tool result outcome = %+v; want submitted terminal malformed response", outcome)
			}
		})
	}
}

func TestProviderRegistryIsClosedToDaytona(t *testing.T) {
	adapter := &DaytonaAdapter{}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	if got, ok := registry.Resolve(sandboxdriver.DaytonaProviderName); !ok || got != adapter {
		t.Fatalf("Resolve(daytona) = (%T,%t); want registered adapter", got, ok)
	}
	if _, ok := registry.Resolve("tetral"); ok {
		t.Fatal("retired provider label tetral resolved")
	}
	if _, err := NewProviderRegistry(map[string]ProviderAdapter{"other": adapter}); err == nil {
		t.Fatal("registry without daytona succeeded")
	}
}

type adapterLifecycleFake struct {
	status       sandbox.ProviderStatus
	err          error
	createErr    error
	startErr     error
	healthChecks int
}

func (f *adapterLifecycleFake) CreateSandbox(context.Context, sandbox.CreateSandboxRequest) (sandbox.ProviderHandle, error) {
	return sandbox.ProviderHandle{}, f.createErr
}
func (f *adapterLifecycleFake) StartSandbox(context.Context, sandbox.ProviderHandle) error {
	return f.startErr
}
func (f *adapterLifecycleFake) CheckBaseTemplateHealth(context.Context, sandbox.ProviderHandle) error {
	f.healthChecks++
	return nil
}
func (f *adapterLifecycleFake) ApplyNetworkPolicy(context.Context, sandbox.ProviderHandle, sandbox.NetworkSetup) error {
	return nil
}
func (f *adapterLifecycleFake) PrepareBaseDirectories(context.Context, sandbox.ProviderHandle) error {
	return nil
}
func (f *adapterLifecycleFake) GetStatus(context.Context, sandbox.ProviderHandle) (sandbox.ProviderStatus, error) {
	return f.status, f.err
}
func (f *adapterLifecycleFake) ReleaseSandbox(context.Context, sandbox.ProviderHandle, sandbox.ReleaseReason) error {
	return errors.New("not implemented")
}

type adapterResourceMaterializerFake struct {
	result sandbox.ResourceSetup
	err    error
}

func (f *adapterResourceMaterializerFake) MaterializeResources(context.Context, sandbox.SandboxSetup, sandbox.ProviderHandle) (sandbox.ResourceSetup, error) {
	return f.result, f.err
}

type adapterToolExecutorFake struct {
	result sandboxdriver.ToolExecution
}

func (f *adapterToolExecutorFake) RunTool(context.Context, sandboxdriver.ToolInvocation) (sandboxdriver.ToolExecution, error) {
	return f.result, nil
}

func (f *adapterToolExecutorFake) PrepareTool(context.Context, sandboxdriver.ToolInvocation) (sandboxdriver.PreparedToolExecution, error) {
	return sandboxdriver.PreparedToolExecution{}, nil
}

func (f *adapterToolExecutorFake) ExecutePreparedTool(context.Context, sandboxdriver.PreparedToolExecution) (sandboxdriver.ToolExecution, error) {
	return f.result, nil
}

func (f *adapterToolExecutorFake) CheckHealth(context.Context, sandboxdriver.ToolTarget) error {
	return nil
}

func (f *adapterToolExecutorFake) ReadCommandResult(context.Context, sandboxdriver.CommandReference) (sandboxdriver.CommandResult, error) {
	return sandboxdriver.CommandResult{}, errors.New("not implemented")
}

func (f *adapterToolExecutorFake) SendCommandInput(context.Context, sandboxdriver.CommandInput) (sandboxdriver.CommandResult, error) {
	return sandboxdriver.CommandResult{}, errors.New("not implemented")
}

func (f *adapterToolExecutorFake) CancelCommand(context.Context, sandboxdriver.CommandCancel) (sandboxdriver.CommandResult, error) {
	return sandboxdriver.CommandResult{}, errors.New("not implemented")
}
