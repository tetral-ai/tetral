package tetralsandbox

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	apiclient "github.com/daytonaio/daytona/libs/api-client-go"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/sandbox"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/services/sandbox/internal/resourceprojection"
)

func TestProviderCompletionLogsDurableIdentityAndRedactsUnsafeFailureDetail(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	adapter := &DaytonaAdapter{Lifecycle: &adapterLifecycleFake{}, Logger: logger}
	adapter.Activate(context.Background(), ActivationRequest{
		Kind: ActivationCreate,
		Setup: sandbox.SandboxSetup{
			WorkspaceID: "ws_log", SessionID: "sesn_log", EnvironmentID: "env_log",
			LifecycleOperationID: "slop_log",
		},
	})
	unsafeDetail := "prompt=do-not-log tool_json={secret:true} command=rm credential=token-secret https://user:password@example.test/path stack=raw-stack"
	logProviderCompletion(context.Background(), logger, "sandbox.provider.inspect_execution", providerOperationIdentity{
		workspaceID: "ws_log", providerResourceID: "provider_sandbox_log",
	}, 1500*time.Millisecond, &sandbox.ProviderError{
		Provider: "daytona", Stage: sandbox.StageStatus, Kind: sandbox.ProviderErrorAuthFailed,
		StatusCode: 401, SafeMessage: unsafeDetail, Cause: errors.New("raw-stack"),
	})
	got := logs.String()
	for _, want := range []string{
		`"operation":"sandbox.provider.create"`, `"workspace.id":"ws_log"`,
		`"session.id":"sesn_log"`, `"environment.id":"env_log"`,
		`"sandbox.lifecycle_operation.id":"slop_log"`, `"outcome":"success"`,
		`"operation":"sandbox.provider.inspect_execution"`, `"duration.ms":1500`,
		`"provider.status_code":401`, `"error.kind":"auth_failed"`,
		`"error.message_safe":"sandbox provider failure detail redacted"`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("provider log missing %s: %s", want, got)
		}
	}
	for _, forbidden := range []string{"do-not-log", "secret:true", "rm", "token-secret", "password", "raw-stack"} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("provider log contains %q: %s", forbidden, got)
		}
	}
}

func TestDaytonaAdapterNormalizesExecutionReadiness(t *testing.T) {
	tests := []struct {
		name         string
		state        string
		wantReady    ExecutionReadiness
		wantBoundary ProviderEffectBoundary
		wantRetry    ProviderDisposition
	}{
		{name: "started", state: string(apiclient.SANDBOXSTATE_STARTED), wantReady: ExecutionReady},
		{name: "stopped", state: string(apiclient.SANDBOXSTATE_STOPPED), wantReady: ExecutionNeedsActivation},
		{name: "archived", state: string(apiclient.SANDBOXSTATE_ARCHIVED), wantReady: ExecutionNeedsActivation},
		{name: "paused", state: "paused", wantReady: ExecutionNeedsActivation},
		{name: "destroyed", state: string(apiclient.SANDBOXSTATE_DESTROYED), wantReady: ExecutionNeedsCreation},
		{name: "deleted", state: "deleted", wantReady: ExecutionNeedsCreation},
		{name: "creating", state: string(apiclient.SANDBOXSTATE_CREATING), wantBoundary: ProviderProvedNotStarted, wantRetry: ProviderRetryable},
		{name: "resuming", state: "resuming", wantBoundary: ProviderProvedNotStarted, wantRetry: ProviderRetryable},
		{name: "destroying", state: string(apiclient.SANDBOXSTATE_DESTROYING), wantBoundary: ProviderProvedNotStarted, wantRetry: ProviderRetryable},
		{name: "provider error", state: string(apiclient.SANDBOXSTATE_ERROR), wantBoundary: ProviderProvedNotStarted, wantRetry: ProviderTerminal},
		{name: "unknown future state", state: "future_state", wantBoundary: ProviderProvedNotStarted, wantRetry: ProviderTerminal},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			adapter := &DaytonaAdapter{Lifecycle: &adapterLifecycleFake{state: test.state}}
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

func TestDaytonaAdapterReleasePresenceUsesProviderExistence(t *testing.T) {
	for _, state := range []string{
		string(apiclient.SANDBOXSTATE_STARTED),
		string(apiclient.SANDBOXSTATE_DESTROYED),
		string(apiclient.SANDBOXSTATE_ERROR),
		"future_state",
	} {
		t.Run(state, func(t *testing.T) {
			outcome := (&DaytonaAdapter{Lifecycle: &adapterLifecycleFake{state: state}}).InspectForRelease(context.Background(), "provider-sandbox")
			if outcome.Failed() || !outcome.Value {
				t.Fatalf("release presence = %+v; want present after successful provider Get", outcome)
			}
		})
	}
	missing := (&DaytonaAdapter{Lifecycle: &adapterLifecycleFake{err: &sandbox.ProviderError{
		Provider: sandboxdriver.DaytonaProviderName, Stage: sandbox.StageStatus, Kind: sandbox.ProviderErrorNotFound,
	}}}).InspectForRelease(context.Background(), "provider-sandbox")
	if missing.Failed() || missing.Value {
		t.Fatalf("missing release presence = %+v; want absent", missing)
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

func TestDaytonaAdapterKeepsPreSubmissionProviderLookupsRetryable(t *testing.T) {
	retryable := &sandbox.ProviderError{
		Provider: sandboxdriver.DaytonaProviderName, Stage: sandbox.StageReleaseSandbox,
		Kind: sandbox.ProviderErrorUnavailable, Retryable: true, SafeMessage: "provider unavailable",
	}
	adapter := &DaytonaAdapter{Lifecycle: &adapterLifecycleFake{releaseErr: sandboxdriver.MarkProviderOperationNotSubmitted(retryable)}}
	release := adapter.Release(context.Background(), ReleaseRequest{
		Handle: sandbox.ProviderHandle{Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider-sandbox"},
	})
	if release.EffectBoundary != ProviderProvedNotStarted || release.Disposition != ProviderRetryable {
		t.Fatalf("release lookup outcome = %+v; want proved-not-started retry", release)
	}

	adapter = &DaytonaAdapter{Lifecycle: &adapterLifecycleFake{createErr: sandboxdriver.MarkProviderOperationNotSubmitted(retryable)}}
	activation := adapter.Activate(context.Background(), ActivationRequest{Kind: ActivationCreate})
	if activation.EffectBoundary != ProviderProvedNotStarted || activation.Disposition != ProviderRetryable {
		t.Fatalf("activation pre-submission outcome = %+v; want proved-not-started retry", activation)
	}

	adapter = &DaytonaAdapter{Tools: &adapterToolExecutorFake{memoryErr: sandboxdriver.MarkProviderOperationNotSubmitted(retryable)}}
	memory := adapter.RefreshMemoryProjection(context.Background(), sandboxdriver.MemoryProjectionRefresh{})
	if memory.EffectBoundary != ProviderProvedNotStarted || memory.Disposition != ProviderRetryable {
		t.Fatalf("memory lookup outcome = %+v; want proved-not-started retry", memory)
	}
}

func TestDaytonaAdapterClassifiesEnvironmentArtifactCreateRejection(t *testing.T) {
	retryable := &sandbox.ProviderError{
		Provider: sandboxdriver.DaytonaProviderName, Stage: sandbox.StageBuildArtifact,
		Kind: sandbox.ProviderErrorUnavailable, Retryable: true, SafeMessage: "provider unavailable",
	}
	adapter := &DaytonaAdapter{Artifacts: &recordingArtifactBuilder{
		err: sandboxdriver.MarkProviderOperationNotSubmitted(retryable),
	}}
	outcome := adapter.BuildEnvironmentArtifact(context.Background(), sandbox.BuildArtifactRequest{})
	if outcome.EffectBoundary != ProviderProvedNotStarted || outcome.Disposition != ProviderRetryable {
		t.Fatalf("artifact rejection outcome = %+v; want proved-not-started retry", outcome)
	}
}

func TestDaytonaAdapterTreatsMissingReleaseTargetAsReleased(t *testing.T) {
	notFound := &sandbox.ProviderError{
		Provider: sandboxdriver.DaytonaProviderName, Stage: sandbox.StageReleaseSandbox,
		Kind: sandbox.ProviderErrorNotFound, SafeMessage: "sandbox not found",
	}
	adapter := &DaytonaAdapter{Lifecycle: &adapterLifecycleFake{releaseErr: sandboxdriver.MarkProviderOperationNotSubmitted(notFound)}}
	outcome := adapter.Release(context.Background(), ReleaseRequest{
		Handle: sandbox.ProviderHandle{Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider-sandbox"},
	})
	if outcome.Failed() || !outcome.Value.Released {
		t.Fatalf("release outcome = %+v; want already-absent success", outcome)
	}
}

func TestDaytonaAdapterRetriesBackgroundObservationWithoutAuthoritativeEnvelope(t *testing.T) {
	adapter := &DaytonaAdapter{Tools: &adapterToolExecutorFake{
		commandErr: &sandboxdriver.HelperFailureError{Message: "helper did not return an envelope"},
	}}
	outcome := adapter.PollBackground(context.Background(), sandboxdriver.CommandReference{})
	if outcome.EffectBoundary != ProviderProvedNotStarted || outcome.Disposition != ProviderRetryable {
		t.Fatalf("background observation outcome = %+v; want retryable observation", outcome)
	}
}

func TestDaytonaAdapterRetriesCredentialMintTransportFailure(t *testing.T) {
	adapter := &DaytonaAdapter{
		Lifecycle: &adapterLifecycleFake{},
		Resources: &adapterResourceMaterializerFake{err: &resourceprojection.CredentialMintError{
			Operation: "mint", Cause: errors.New("credential service unavailable"),
		}},
	}
	outcome := adapter.MaterializeResources(context.Background(), MaterializationRequest{
		Handle: sandbox.ProviderHandle{Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider-sandbox"},
	})
	if outcome.EffectBoundary != ProviderProvedNotStarted || outcome.Disposition != ProviderRetryable {
		t.Fatalf("credential mint outcome = %+v; want retryable pre-submission failure", outcome)
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
			var logs bytes.Buffer
			adapter := &DaytonaAdapter{
				Tools:  &adapterToolExecutorFake{result: sandboxdriver.ToolExecution{ResultJSON: test.resultJSON}},
				Logger: slog.New(slog.NewJSONHandler(&logs, nil)),
			}
			outcome := adapter.ExecuteTool(context.Background(), ToolExecutionRequest{
				Handle:   sandbox.ProviderHandle{Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider-sandbox"},
				Prepared: daytonaPreparedTool{},
			})
			if outcome.EffectBoundary != ProviderSubmitted || outcome.Disposition != ProviderTerminal || outcome.ErrorKind != "provider_response_malformed" {
				t.Fatalf("malformed tool result outcome = %+v; want submitted terminal malformed response", outcome)
			}
			if !strings.Contains(logs.String(), `"outcome":"error"`) ||
				!strings.Contains(logs.String(), `"error.kind":"provider_response_malformed"`) ||
				strings.Contains(logs.String(), `"outcome":"success"`) {
				t.Fatalf("malformed tool result log = %s; want only normalized failure", logs.String())
			}
		})
	}
}

func TestDaytonaAdapterLogsProviderExecutionFailureDetailWithoutChangingDurableResult(t *testing.T) {
	var logs bytes.Buffer
	adapter := &DaytonaAdapter{
		Tools: &adapterToolExecutorFake{submitErr: &sandbox.ProviderError{
			Provider:    sandboxdriver.DaytonaProviderName,
			Stage:       sandbox.StageExecuteTool,
			Kind:        sandbox.ProviderErrorUnavailable,
			StatusCode:  503,
			SafeMessage: "daytona tool service unavailable",
		}},
		Logger: slog.New(slog.NewJSONHandler(&logs, nil)),
	}
	outcome := adapter.ExecuteTool(context.Background(), ToolExecutionRequest{
		Handle:   sandbox.ProviderHandle{Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider-sandbox"},
		Prepared: daytonaPreparedTool{},
	})
	if outcome.EffectBoundary != ProviderOutcomeUnknown || outcome.Disposition != ProviderTerminal ||
		outcome.ErrorKind != "sandbox_execution_outcome_unknown" || outcome.SafeMessage != "daytona tool execution outcome is unknown" {
		t.Fatalf("execution outcome = %+v; want durable unknown-outcome failure", outcome)
	}
	got := logs.String()
	for _, want := range []string{
		`"operation":"sandbox.provider.execute_tool"`,
		`"outcome":"error"`,
		`"provider.status_code":503`,
		`"error.message_safe":"daytona tool service unavailable"`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("provider execution log missing %s: %s", want, got)
		}
	}
	if strings.Contains(got, `"outcome":"success"`) {
		t.Fatalf("provider execution failure also logged success: %s", got)
	}
}

func TestProviderRegistryIsClosedToDaytona(t *testing.T) {
	if _, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: &DaytonaAdapter{}}); err == nil {
		t.Fatal("registry accepted an incomplete Daytona adapter")
	}
	lifecycle := &adapterLifecycleFake{}
	adapter := &DaytonaAdapter{
		Lifecycle: lifecycle, Resolver: lifecycle, Tools: &adapterToolExecutorFake{},
		Resources: &adapterResourceMaterializerFake{}, Artifacts: &recordingArtifactBuilder{},
		BlobStore: blob.NewFakeBlobStore(),
	}
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
	state        string
	err          error
	createErr    error
	startErr     error
	releaseErr   error
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
func (f *adapterLifecycleFake) PrepareBaseDirectories(context.Context, sandbox.ProviderHandle) error {
	return nil
}
func (f *adapterLifecycleFake) InspectState(context.Context, string) (string, error) {
	return f.state, f.err
}
func (f *adapterLifecycleFake) ReleaseSandbox(context.Context, sandbox.ProviderHandle) error {
	return f.releaseErr
}
func (f *adapterLifecycleFake) ResolveSandbox(context.Context, string, map[string]string) (sandbox.ProviderHandle, bool, error) {
	return sandbox.ProviderHandle{}, false, nil
}

type adapterResourceMaterializerFake struct {
	result sandbox.ResourceSetup
	err    error
}

func (f *adapterResourceMaterializerFake) MaterializeResources(context.Context, sandbox.SandboxSetup, sandbox.ProviderHandle) (sandbox.ResourceSetup, error) {
	return f.result, f.err
}

type adapterToolExecutorFake struct {
	result     sandboxdriver.ToolExecution
	submitErr  error
	memoryErr  error
	commandErr error
}

func (f *adapterToolExecutorFake) PrepareTool(context.Context, sandboxdriver.ToolInvocation) (sandboxdriver.PreparedToolExecution, error) {
	return sandboxdriver.PreparedToolExecution{}, nil
}

func (f *adapterToolExecutorFake) SubmitPreparedTool(context.Context, sandboxdriver.PreparedToolExecution) (sandboxdriver.ToolExecution, error) {
	return f.result, f.submitErr
}

func (f *adapterToolExecutorFake) ObserveForegroundTool(context.Context, sandboxdriver.ForegroundCommandObservation) (sandboxdriver.ToolExecution, error) {
	return f.result, nil
}

func (f *adapterToolExecutorFake) CheckHealth(context.Context, sandboxdriver.ToolTarget) error {
	return nil
}

func (f *adapterToolExecutorFake) ReadCommandResult(context.Context, sandboxdriver.CommandReference) (sandboxdriver.CommandResult, error) {
	return sandboxdriver.CommandResult{}, f.commandErr
}

func (f *adapterToolExecutorFake) SendCommandInput(context.Context, sandboxdriver.CommandInput) (sandboxdriver.CommandResult, error) {
	return sandboxdriver.CommandResult{}, errors.New("not implemented")
}

func (f *adapterToolExecutorFake) CancelCommand(context.Context, sandboxdriver.CommandCancel) (sandboxdriver.CommandResult, error) {
	return sandboxdriver.CommandResult{}, errors.New("not implemented")
}

func (f *adapterToolExecutorFake) RefreshMemoryProjection(context.Context, sandboxdriver.MemoryProjectionRefresh) error {
	return f.memoryErr
}

func (f *adapterToolExecutorFake) CaptureOutputs(context.Context, sandboxdriver.OutputCaptureTarget) (sandboxdriver.OutputCaptureScan, error) {
	return sandboxdriver.OutputCaptureScan{}, nil
}
