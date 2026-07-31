package tetralsandbox

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sandbox"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

func TestSandboxToolExecutionRunnerFinalizesOnlyPastAttemptBudget(t *testing.T) {
	job := sandboxExecutionQueueJob()
	job.AttemptCount = job.MaxAttempts + 1
	queueClient := &recordingSessionPrepareQueue{leased: []*queuev1.QueueJob{job}}
	coordinator := &recordingSandboxExecutionCoordinator{}
	adapter := &recordingProviderAdapter{}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxToolExecutionJobRunner{
		Queue: queueClient, Coordinator: coordinator, Providers: registry,
		Config: SandboxToolExecutionRunnerConfig{WorkspaceID: "ws_execution", LeaseDuration: 2 * time.Minute, HeartbeatInterval: 15 * time.Second, PreparationTimeout: 45 * time.Second},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(coordinator.calls, []string{"exhausted"}) {
		t.Fatalf("coordinator calls = %v; want exhausted finalizer only", coordinator.calls)
	}
	if len(adapter.calls) != 0 {
		t.Fatalf("provider calls = %v; want none at attempt budget", adapter.calls)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"dead:qjob_sandbox_execution:sandbox_execution_attempts_exhausted"}) {
		t.Fatalf("queue transitions = %v; want dead letter", queueClient.transitions)
	}
}

func TestSandboxToolExecutionRunnerExecutesTheLastPermittedAttempt(t *testing.T) {
	job := sandboxExecutionQueueJob()
	job.AttemptCount = job.MaxAttempts
	queueClient := &recordingSessionPrepareQueue{leased: []*queuev1.QueueJob{job}}
	coordinator := &recordingSandboxExecutionCoordinator{load: false}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: &recordingProviderAdapter{}})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxToolExecutionJobRunner{
		Queue: queueClient, Coordinator: coordinator, Providers: registry,
		Config: SandboxToolExecutionRunnerConfig{WorkspaceID: "ws_execution", LeaseDuration: 2 * time.Minute, HeartbeatInterval: 15 * time.Second, PreparationTimeout: 45 * time.Second},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(coordinator.calls, []string{"load"}) {
		t.Fatalf("coordinator calls = %v; want handler at exact budget", coordinator.calls)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"ack:qjob_sandbox_execution"}) {
		t.Fatalf("queue transitions = %v; want ack", queueClient.transitions)
	}
}

func TestSandboxToolExecutionRunnerSettlesBusinessStateBeforeDeadLetteringInvalidPayload(t *testing.T) {
	job := sandboxExecutionQueueJob()
	job.PayloadJson = `{not-json`
	queueClient := &recordingSessionPrepareQueue{leased: []*queuev1.QueueJob{job}}
	coordinator := &recordingSandboxExecutionCoordinator{}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: &recordingProviderAdapter{}})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxToolExecutionJobRunner{
		Queue: queueClient, Coordinator: coordinator, Providers: registry,
		Config: SandboxToolExecutionRunnerConfig{WorkspaceID: "ws_execution", LeaseDuration: 2 * time.Minute, HeartbeatInterval: 15 * time.Second, PreparationTimeout: 45 * time.Second},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(coordinator.calls, []string{"invalid"}) {
		t.Fatalf("coordinator calls = %v; want business settlement before transport closure", coordinator.calls)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"dead:qjob_sandbox_execution:invalid_sandbox_tool_execute_payload"}) {
		t.Fatalf("queue transitions = %v; want invalid payload dead letter", queueClient.transitions)
	}
}

func TestSandboxToolExecutionRunnerFinalizesRetryFromLastPermittedAttempt(t *testing.T) {
	job := sandboxExecutionQueueJob()
	job.AttemptCount = job.MaxAttempts
	queueClient := &recordingSessionPrepareQueue{leased: []*queuev1.QueueJob{job}}
	coordinator := &recordingSandboxExecutionCoordinator{work: sandboxExecutionTestWork(true), load: true}
	adapter := &recordingProviderAdapter{inspection: ProviderOutcome[ExecutionReadiness]{
		EffectBoundary: ProviderProvedNotStarted,
		Disposition:    ProviderRetryable,
		ErrorKind:      "provider_transition_in_progress",
	}}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxToolExecutionJobRunner{
		Queue: queueClient, Coordinator: coordinator, Providers: registry,
		Config: SandboxToolExecutionRunnerConfig{WorkspaceID: "ws_execution", LeaseDuration: 2 * time.Minute, HeartbeatInterval: 15 * time.Second, PreparationTimeout: 45 * time.Second},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(coordinator.calls, []string{"load", "exhausted"}) {
		t.Fatalf("coordinator calls = %v; want live-handler exhaustion finalizer", coordinator.calls)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"dead:qjob_sandbox_execution:sandbox_execution_attempts_exhausted"}) {
		t.Fatalf("queue transitions = %v; want dead letter after business finalization", queueClient.transitions)
	}
}

func TestSandboxToolExecutionRunnerConvergesBeforeExecuting(t *testing.T) {
	tests := []struct {
		name             string
		work             SandboxExecutionWork
		inspection       ProviderOutcome[ExecutionReadiness]
		wantCoordinator  []string
		wantAdapterCalls []string
	}{
		{
			name:            "missing binding creates activation",
			work:            SandboxExecutionWork{Ref: sandboxExecutionTestRef(), AttemptGeneration: 1},
			wantCoordinator: []string{"load", "activation:needs_creation"},
		},
		{
			name:             "stopped binding joins activation",
			work:             sandboxExecutionTestWork(false),
			inspection:       ProviderOutcome[ExecutionReadiness]{Value: ExecutionNeedsActivation},
			wantCoordinator:  []string{"load", "activation:needs_activation"},
			wantAdapterCalls: []string{"inspect"},
		},
		{
			name:             "ready binding joins materialization",
			work:             sandboxExecutionTestWork(false),
			inspection:       ProviderOutcome[ExecutionReadiness]{Value: ExecutionReady},
			wantCoordinator:  []string{"load", "materialization"},
			wantAdapterCalls: []string{"inspect"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			queueClient := &recordingSessionPrepareQueue{leased: []*queuev1.QueueJob{sandboxExecutionQueueJob()}}
			coordinator := &recordingSandboxExecutionCoordinator{work: test.work, load: true}
			adapter := &recordingProviderAdapter{inspection: test.inspection}
			registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
			if err != nil {
				t.Fatalf("NewProviderRegistry: %v", err)
			}
			runner := &SandboxToolExecutionJobRunner{
				Queue:       queueClient,
				Coordinator: coordinator,
				Providers:   registry,
				Config: SandboxToolExecutionRunnerConfig{
					WorkspaceID:        "ws_execution",
					LeaseDuration:      2 * time.Minute,
					HeartbeatInterval:  15 * time.Second,
					PreparationTimeout: 45 * time.Second,
				},
			}
			if err := runner.RunOnce(context.Background()); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if !reflect.DeepEqual(coordinator.calls, test.wantCoordinator) {
				t.Fatalf("coordinator calls = %v; want %v", coordinator.calls, test.wantCoordinator)
			}
			if !reflect.DeepEqual(adapter.calls, test.wantAdapterCalls) {
				t.Fatalf("adapter calls = %v; want %v", adapter.calls, test.wantAdapterCalls)
			}
			if !reflect.DeepEqual(queueClient.transitions, []string{"ack:qjob_sandbox_execution"}) {
				t.Fatalf("queue transitions = %v; want ack", queueClient.transitions)
			}
		})
	}
}

func TestSandboxToolExecutionRunnerDoesNotResubmitRunningExecution(t *testing.T) {
	queueClient := &recordingSessionPrepareQueue{leased: []*queuev1.QueueJob{sandboxExecutionQueueJob()}}
	work := sandboxExecutionTestWork(true)
	work.State = "running"
	coordinator := &recordingSandboxExecutionCoordinator{work: work, load: true}
	adapter := &recordingProviderAdapter{}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxToolExecutionJobRunner{
		Queue: queueClient, Coordinator: coordinator, Providers: registry,
		Config: SandboxToolExecutionRunnerConfig{WorkspaceID: "ws_execution", LeaseDuration: 2 * time.Minute, HeartbeatInterval: 15 * time.Second, PreparationTimeout: 45 * time.Second},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(coordinator.calls, []string{"load", "settle:unknown_outcome"}) {
		t.Fatalf("coordinator calls = %v; want running recovery settlement", coordinator.calls)
	}
	if len(adapter.calls) != 0 {
		t.Fatalf("provider calls = %v; want no resubmission", adapter.calls)
	}
}

func TestSandboxToolExecutionRunnerSettlesOnlyDurableProviderOutcomes(t *testing.T) {
	tests := []struct {
		name            string
		inspection      ProviderOutcome[ExecutionReadiness]
		execution       ProviderOutcome[sandboxdriver.ToolExecution]
		wantCoordinator []string
		wantTransitions []string
		wantAdapter     []string
	}{
		{
			name:            "completed",
			inspection:      ProviderOutcome[ExecutionReadiness]{Value: ExecutionReady},
			execution:       ProviderOutcome[sandboxdriver.ToolExecution]{Value: sandboxdriver.ToolExecution{ResultJSON: `{"status":"success"}`}},
			wantCoordinator: []string{"load", "preparing", "running", "settle:completed"},
			wantTransitions: []string{"ack:qjob_sandbox_execution"},
			wantAdapter:     []string{"inspect", "prepare", "execute"},
		},
		{
			name: "retryable inspection",
			inspection: ProviderOutcome[ExecutionReadiness]{
				EffectBoundary: ProviderProvedNotStarted,
				Disposition:    ProviderRetryable,
				ErrorKind:      "provider_transition_in_progress",
			},
			wantCoordinator: []string{"load"},
			wantTransitions: []string{"retry:qjob_sandbox_execution:provider_transition_in_progress"},
			wantAdapter:     []string{"inspect"},
		},
		{
			name:       "unknown after authorization",
			inspection: ProviderOutcome[ExecutionReadiness]{Value: ExecutionReady},
			execution: ProviderOutcome[sandboxdriver.ToolExecution]{
				EffectBoundary: ProviderOutcomeUnknown,
				Disposition:    ProviderTerminal,
				ErrorKind:      "provider_outcome_unknown",
			},
			wantCoordinator: []string{"load", "preparing", "running", "settle:unknown_outcome"},
			wantTransitions: []string{"ack:qjob_sandbox_execution"},
			wantAdapter:     []string{"inspect", "prepare", "execute"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			queueClient := &recordingSessionPrepareQueue{leased: []*queuev1.QueueJob{sandboxExecutionQueueJob()}}
			coordinator := &recordingSandboxExecutionCoordinator{work: sandboxExecutionTestWork(true), load: true, prepare: true, authorize: true}
			adapter := &recordingProviderAdapter{inspection: test.inspection, execution: test.execution}
			registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
			if err != nil {
				t.Fatalf("NewProviderRegistry: %v", err)
			}
			runner := &SandboxToolExecutionJobRunner{
				Queue: queueClient, Coordinator: coordinator, Providers: registry,
				Config: SandboxToolExecutionRunnerConfig{WorkspaceID: "ws_execution", LeaseDuration: 2 * time.Minute, HeartbeatInterval: 15 * time.Second, PreparationTimeout: 45 * time.Second},
			}
			if err := runner.RunOnce(context.Background()); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if !reflect.DeepEqual(coordinator.calls, test.wantCoordinator) {
				t.Fatalf("coordinator calls = %v; want %v", coordinator.calls, test.wantCoordinator)
			}
			if !reflect.DeepEqual(queueClient.transitions, test.wantTransitions) {
				t.Fatalf("queue transitions = %v; want %v", queueClient.transitions, test.wantTransitions)
			}
			if !reflect.DeepEqual(adapter.calls, test.wantAdapter) {
				t.Fatalf("adapter calls = %v; want %v", adapter.calls, test.wantAdapter)
			}
		})
	}
}

func sandboxExecutionTestRef() SandboxExecutionRef {
	return SandboxExecutionRef{WorkspaceID: "ws_execution", SessionID: "sesn_execution", SessionThreadID: "thr_execution", ToolUseEventID: "evt_execution"}
}

func sandboxExecutionTestWork(materialized bool) SandboxExecutionWork {
	return SandboxExecutionWork{
		Ref: sandboxExecutionTestRef(), AttemptGeneration: 1,
		Binding: &SandboxBinding{
			LogicalSandboxID: "sbox_execution", Provider: sandboxdriver.DaytonaProviderName,
			ProviderResourceID: "provider_execution", BindingRevision: 2,
		},
		MaterializationReady: materialized,
		Invocation: sandboxdriver.ToolInvocation{
			ToolUseEventID: "evt_execution", ToolName: "bash", InputJSON: `{"command":"true"}`,
		},
	}
}

func sandboxExecutionQueueJob() *queuev1.QueueJob {
	return &queuev1.QueueJob{
		Id: "qjob_sandbox_execution", WorkspaceId: "ws_execution",
		Kind:         queue.KindSandboxToolExecute,
		PartitionKey: queue.FormatSandboxExecutionPartitionKey("ws_execution", "sesn_execution", "thr_execution", "evt_execution"),
		DedupeKey:    queue.FormatSandboxToolExecuteDedupeKey("ws_execution", "sesn_execution", "thr_execution", "evt_execution", 1),
		PayloadJson:  `{"session_id":"sesn_execution","session_thread_id":"thr_execution","tool_use_event_id":"evt_execution","workspace_id":"ws_execution"}`,
		LeaseToken:   "lease_execution", AttemptCount: 1, MaxAttempts: 5,
	}
}

type recordingSandboxExecutionCoordinator struct {
	work      SandboxExecutionWork
	load      bool
	prepare   bool
	authorize bool
	calls     []string
}

func (c *recordingSandboxExecutionCoordinator) LoadExecution(context.Context, SandboxExecutionJob) (SandboxExecutionWork, bool, error) {
	c.calls = append(c.calls, "load")
	return c.work, c.load, nil
}
func (c *recordingSandboxExecutionCoordinator) WaitForActivation(_ context.Context, _ SandboxExecutionWork, readiness ExecutionReadiness) error {
	c.calls = append(c.calls, "activation:"+string(readiness))
	return nil
}
func (c *recordingSandboxExecutionCoordinator) WaitForMaterialization(context.Context, SandboxExecutionWork) error {
	c.calls = append(c.calls, "materialization")
	return nil
}
func (c *recordingSandboxExecutionCoordinator) BeginPreparing(context.Context, SandboxExecutionWork, time.Time) (bool, error) {
	c.calls = append(c.calls, "preparing")
	return c.prepare, nil
}
func (c *recordingSandboxExecutionCoordinator) AuthorizeRunning(context.Context, SandboxExecutionWork) (bool, error) {
	c.calls = append(c.calls, "running")
	return c.authorize, nil
}
func (c *recordingSandboxExecutionCoordinator) SettleExecution(_ context.Context, _ SandboxExecutionWork, result SandboxExecutionSettlement) error {
	c.calls = append(c.calls, "settle:"+string(result.Kind))
	return nil
}
func (c *recordingSandboxExecutionCoordinator) FinalizeExhaustedExecution(context.Context, *queuev1.QueueJob) error {
	c.calls = append(c.calls, "exhausted")
	return nil
}
func (c *recordingSandboxExecutionCoordinator) FinalizeInvalidExecution(context.Context, *queuev1.QueueJob) error {
	c.calls = append(c.calls, "invalid")
	return nil
}

type recordingProviderAdapter struct {
	inspection ProviderOutcome[ExecutionReadiness]
	execution  ProviderOutcome[sandboxdriver.ToolExecution]
	calls      []string
}

func (a *recordingProviderAdapter) InspectForExecution(context.Context, string) ProviderOutcome[ExecutionReadiness] {
	a.calls = append(a.calls, "inspect")
	return a.inspection
}
func (a *recordingProviderAdapter) ResolveActivation(context.Context, ActivationResolutionRequest) ProviderOutcome[ActivationResolution] {
	return ProviderOutcome[ActivationResolution]{}
}
func (a *recordingProviderAdapter) Activate(context.Context, ActivationRequest) ProviderOutcome[sandbox.ProviderHandle] {
	return ProviderOutcome[sandbox.ProviderHandle]{}
}
func (a *recordingProviderAdapter) MaterializeResources(context.Context, MaterializationRequest) ProviderOutcome[MaterializationResult] {
	return ProviderOutcome[MaterializationResult]{}
}
func (a *recordingProviderAdapter) PrepareTool(context.Context, ToolExecutionRequest) ProviderOutcome[ToolPreparationResult] {
	a.calls = append(a.calls, "prepare")
	return ProviderOutcome[ToolPreparationResult]{Value: ToolPreparationResult{Prepared: recordingPreparedTool{}}}
}
func (a *recordingProviderAdapter) ExecuteTool(context.Context, ToolExecutionRequest) ProviderOutcome[sandboxdriver.ToolExecution] {
	a.calls = append(a.calls, "execute")
	return a.execution
}

type recordingPreparedTool struct{}

func (recordingPreparedTool) providerPreparedTool() {}
func (a *recordingProviderAdapter) Release(context.Context, ReleaseRequest) ProviderOutcome[ReleaseResult] {
	return ProviderOutcome[ReleaseResult]{}
}
