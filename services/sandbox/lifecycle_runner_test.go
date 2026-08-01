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

func TestSandboxActivationRunnerAdoptsBeforeCreateAndDoesNotReplayUnknownCreate(t *testing.T) {
	tests := []struct {
		name            string
		work            SandboxActivationWork
		resolution      ProviderOutcome[ActivationResolution]
		activation      ProviderOutcome[sandbox.ProviderHandle]
		inspection      ProviderOutcome[ExecutionReadiness]
		wantAdapter     []string
		wantStore       []string
		wantTransitions []string
	}{
		{
			name: "adopts stable name",
			work: sandboxActivationTestWork(true),
			resolution: ProviderOutcome[ActivationResolution]{Value: ActivationResolution{
				Found: true, Handle: sandbox.ProviderHandle{Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider_adopted"},
			}},
			inspection:  ProviderOutcome[ExecutionReadiness]{Value: ExecutionReady},
			wantAdapter: []string{"resolve", "inspect"}, wantStore: []string{"claim", "complete:provider_adopted"},
			wantTransitions: []string{"ack:qjob_activation"},
		},
		{
			name:       "creates after proved absence",
			work:       sandboxActivationTestWork(true),
			resolution: ProviderOutcome[ActivationResolution]{Value: ActivationResolution{Found: false}},
			activation: ProviderOutcome[sandbox.ProviderHandle]{Value: sandbox.ProviderHandle{
				Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider_created",
			}},
			inspection:  ProviderOutcome[ExecutionReadiness]{Value: ExecutionReady},
			wantAdapter: []string{"resolve", "activate:create", "inspect"}, wantStore: []string{"claim", "complete:provider_created"},
			wantTransitions: []string{"ack:qjob_activation"},
		},
		{
			name:       "unknown create enters observation only",
			work:       sandboxActivationTestWork(true),
			resolution: ProviderOutcome[ActivationResolution]{Value: ActivationResolution{Found: false}},
			activation: ProviderOutcome[sandbox.ProviderHandle]{
				EffectBoundary: ProviderOutcomeUnknown, Disposition: ProviderTerminal,
				ErrorKind: "provider_timeout",
			},
			wantAdapter: []string{"resolve", "activate:create"}, wantStore: []string{"claim", "observe:provider_timeout"},
			wantTransitions: []string{"retry:qjob_activation:provider_timeout"},
		},
		{
			name:        "observing retry never creates again",
			work:        sandboxActivationTestWork(false),
			resolution:  ProviderOutcome[ActivationResolution]{Value: ActivationResolution{Found: false}},
			wantAdapter: []string{"resolve"}, wantStore: []string{"claim", "fail:outcome_unknown:activation_outcome_unknown"},
			wantTransitions: []string{"dead:qjob_activation:activation_outcome_unknown"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			queueClient := &recordingSessionPrepareQueue{leased: []*queuev1.QueueJob{sandboxActivationQueueJob()}}
			store := &recordingSandboxLifecycleStore{activation: test.work, current: true}
			adapter := &recordingLifecycleAdapter{resolution: test.resolution, activation: test.activation, inspection: test.inspection}
			registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
			if err != nil {
				t.Fatalf("NewProviderRegistry: %v", err)
			}
			runner := &SandboxActivationJobRunner{
				Queue: queueClient, Store: store, Providers: registry,
				Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
			}
			if err := runner.RunOnce(context.Background()); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if !reflect.DeepEqual(adapter.calls, test.wantAdapter) {
				t.Fatalf("adapter calls = %v; want %v", adapter.calls, test.wantAdapter)
			}
			if !reflect.DeepEqual(store.calls, test.wantStore) {
				t.Fatalf("store calls = %v; want %v", store.calls, test.wantStore)
			}
			if !reflect.DeepEqual(queueClient.transitions, test.wantTransitions) {
				t.Fatalf("queue transitions = %v; want %v", queueClient.transitions, test.wantTransitions)
			}
		})
	}
}

func TestSandboxActivationRunnerConvertsMissingStartToReplacement(t *testing.T) {
	queueClient := &recordingSessionPrepareQueue{leased: []*queuev1.QueueJob{sandboxActivationQueueJob()}}
	work := sandboxActivationTestWork(true)
	work.Kind = ActivationStart
	work.CurrentHandle = sandbox.ProviderHandle{Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider_deleted"}
	replacement := sandboxActivationTestWork(true)
	replacement.Kind = ActivationReplace
	store := &recordingSandboxLifecycleStore{activation: work, replacement: replacement, current: true}
	adapter := &recordingLifecycleAdapter{
		inspectionSequence: []ProviderOutcome[ExecutionReadiness]{
			{Value: ExecutionNeedsCreation},
			{Value: ExecutionReady},
		},
		resolution: ProviderOutcome[ActivationResolution]{Value: ActivationResolution{Found: false}},
		activation: ProviderOutcome[sandbox.ProviderHandle]{Value: sandbox.ProviderHandle{
			Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider_replacement",
		}},
	}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxActivationJobRunner{
		Queue: queueClient, Store: store, Providers: registry,
		Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(adapter.calls, []string{"inspect", "resolve", "activate:replace", "inspect"}) {
		t.Fatalf("adapter calls = %v", adapter.calls)
	}
	if !reflect.DeepEqual(store.calls, []string{"claim", "replace-missing", "complete:provider_replacement"}) {
		t.Fatalf("store calls = %v", store.calls)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"ack:qjob_activation"}) {
		t.Fatalf("queue transitions = %v", queueClient.transitions)
	}
}

func TestSandboxActivationRunnerRetainsObservationAfterFinalSubmissionAttempt(t *testing.T) {
	queueJob := sandboxActivationQueueJob()
	queueJob.AttemptCount = sandboxActivationSubmissionMaxAttempts
	queueJob.MaxAttempts = sandboxActivationMaxAttempts
	queueClient := &recordingSessionPrepareQueue{leased: []*queuev1.QueueJob{queueJob}}
	store := &recordingSandboxLifecycleStore{activation: sandboxActivationTestWork(true), current: true}
	adapter := &recordingLifecycleAdapter{
		resolution: ProviderOutcome[ActivationResolution]{Value: ActivationResolution{Found: false}},
		activation: ProviderOutcome[sandbox.ProviderHandle]{
			EffectBoundary: ProviderOutcomeUnknown, Disposition: ProviderTerminal, ErrorKind: "provider_timeout",
		},
	}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxActivationJobRunner{
		Queue: queueClient, Store: store, Providers: registry,
		Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(store.calls, []string{"claim", "observe:provider_timeout"}) ||
		!reflect.DeepEqual(queueClient.transitions, []string{"retry:qjob_activation:provider_timeout"}) {
		t.Fatalf("store=%v queue=%v; want durable observation retry", store.calls, queueClient.transitions)
	}
}

func TestSandboxActivationRunnerReinspectsAfterAmbiguousStart(t *testing.T) {
	queueClient := &recordingSessionPrepareQueue{leased: []*queuev1.QueueJob{sandboxActivationQueueJob()}}
	work := sandboxActivationTestWork(true)
	work.Kind = ActivationStart
	work.CurrentHandle = sandbox.ProviderHandle{Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider_stopped"}
	store := &recordingSandboxLifecycleStore{activation: work, current: true}
	adapter := &recordingLifecycleAdapter{
		inspection: ProviderOutcome[ExecutionReadiness]{Value: ExecutionNeedsActivation},
		activation: ProviderOutcome[sandbox.ProviderHandle]{
			EffectBoundary: ProviderOutcomeUnknown, Disposition: ProviderTerminal,
			ErrorKind: "provider_timeout",
		},
	}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxActivationJobRunner{
		Queue: queueClient, Store: store, Providers: registry,
		Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(store.calls, []string{"claim"}) {
		t.Fatalf("store calls = %v; ambiguous Start must remain observable", store.calls)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"retry:qjob_activation:provider_timeout"}) {
		t.Fatalf("queue transitions = %v; want observation retry", queueClient.transitions)
	}
}

func TestSandboxMaterializationRunnerRechecksReadinessBeforeProjection(t *testing.T) {
	tests := []struct {
		name            string
		inspection      ProviderOutcome[ExecutionReadiness]
		materialization ProviderOutcome[MaterializationResult]
		wantAdapter     []string
		wantStore       []string
	}{
		{
			name:       "ready projects resources",
			inspection: ProviderOutcome[ExecutionReadiness]{Value: ExecutionReady},
			materialization: ProviderOutcome[MaterializationResult]{Value: MaterializationResult{
				MaterializedEnvironmentGeneration: 1,
				MaterializedResourceRevision:      1,
				Resources:                         sandbox.ResourceSetup{ResourceRootsJSON: `[{"path":"/mnt/session/uploads/a","mode":"read"}]`},
			}},
			wantAdapter: []string{"inspect", "materialize"}, wantStore: []string{"claim-materialization", "complete-materialization"},
		},
		{
			name:        "cold resource rejoins activation",
			inspection:  ProviderOutcome[ExecutionReadiness]{Value: ExecutionNeedsActivation},
			wantAdapter: []string{"inspect"}, wantStore: []string{"claim-materialization", "materialization-activation:needs_activation"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			queueClient := &recordingSessionPrepareQueue{leased: []*queuev1.QueueJob{sandboxMaterializationQueueJob()}}
			store := &recordingSandboxLifecycleStore{materialization: sandboxMaterializationTestWork(), current: true}
			adapter := &recordingLifecycleAdapter{inspection: test.inspection, materialization: test.materialization}
			registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
			if err != nil {
				t.Fatalf("NewProviderRegistry: %v", err)
			}
			runner := &SandboxMaterializationJobRunner{
				Queue: queueClient, Store: store, Providers: registry,
				Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
			}
			if err := runner.RunOnce(context.Background()); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if !reflect.DeepEqual(adapter.calls, test.wantAdapter) {
				t.Fatalf("adapter calls = %v; want %v", adapter.calls, test.wantAdapter)
			}
			if !reflect.DeepEqual(store.calls, test.wantStore) {
				t.Fatalf("store calls = %v; want %v", store.calls, test.wantStore)
			}
			if !reflect.DeepEqual(queueClient.transitions, []string{"ack:qjob_materialization"}) {
				t.Fatalf("queue transitions = %v; want ack", queueClient.transitions)
			}
		})
	}
}

func TestSandboxMaterializationRunnerDoesNotReplaySubmittedFailure(t *testing.T) {
	queueClient := &recordingSessionPrepareQueue{leased: []*queuev1.QueueJob{sandboxMaterializationQueueJob()}}
	store := &recordingSandboxLifecycleStore{materialization: sandboxMaterializationTestWork(), current: true}
	adapter := &recordingLifecycleAdapter{
		inspection: ProviderOutcome[ExecutionReadiness]{Value: ExecutionReady},
		materialization: ProviderOutcome[MaterializationResult]{
			EffectBoundary: ProviderSubmitted, Disposition: ProviderRetryable,
			ErrorKind: "provider_transport_lost", SafeMessage: "materialization outcome is not replayable",
		},
	}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxMaterializationJobRunner{
		Queue: queueClient, Store: store, Providers: registry,
		Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(store.calls, []string{"claim-materialization", "fail-materialization:submitted:retryable:provider_transport_lost"}) {
		t.Fatalf("store calls = %v; want submitted failure receipt", store.calls)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"dead:qjob_materialization:provider_transport_lost"}) {
		t.Fatalf("queue transitions = %v; submitted materialization must not be retried", queueClient.transitions)
	}
}

func TestSandboxLifecycleRunnersFinalizeExhaustedAttempts(t *testing.T) {
	tests := []struct {
		name      string
		kind      string
		queueJob  *queuev1.QueueJob
		run       func(*recordingSessionPrepareQueue, *recordingSandboxLifecycleStore, *ProviderRegistry) error
		wantStore string
		wantDead  string
	}{
		{
			name: "activation", kind: queue.KindSandboxActivate, queueJob: sandboxActivationQueueJob(),
			run: func(queueClient *recordingSessionPrepareQueue, store *recordingSandboxLifecycleStore, registry *ProviderRegistry) error {
				return (&SandboxActivationJobRunner{Queue: queueClient, Store: store, Providers: registry,
					Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second}}).RunOnce(context.Background())
			},
			wantStore: "exhausted:sandbox_activate", wantDead: "dead:qjob_activation:sandbox_activation_attempts_exhausted",
		},
		{
			name: "materialization", kind: queue.KindSandboxMaterialize, queueJob: sandboxMaterializationQueueJob(),
			run: func(queueClient *recordingSessionPrepareQueue, store *recordingSandboxLifecycleStore, registry *ProviderRegistry) error {
				return (&SandboxMaterializationJobRunner{Queue: queueClient, Store: store, Providers: registry,
					Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second}}).RunOnce(context.Background())
			},
			wantStore: "exhausted:sandbox_materialize", wantDead: "dead:qjob_materialization:sandbox_materialization_attempts_exhausted",
		},
	}
	adapter := &recordingLifecycleAdapter{}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.queueJob.AttemptCount = test.queueJob.MaxAttempts + 1
			queueClient := &recordingSessionPrepareQueue{leased: []*queuev1.QueueJob{test.queueJob}}
			store := &recordingSandboxLifecycleStore{}
			if err := test.run(queueClient, store, registry); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if !reflect.DeepEqual(store.calls, []string{test.wantStore}) {
				t.Fatalf("store calls = %v; want finalizer", store.calls)
			}
			if !reflect.DeepEqual(queueClient.transitions, []string{test.wantDead}) {
				t.Fatalf("queue transitions = %v; want dead letter", queueClient.transitions)
			}
		})
	}
}

func TestSandboxLifecycleRunnersCheckAttemptBudgetBeforePayloadDecode(t *testing.T) {
	tests := []struct {
		name      string
		kind      string
		queueJob  *queuev1.QueueJob
		run       func(*recordingSessionPrepareQueue, *recordingSandboxLifecycleStore, *ProviderRegistry) error
		wantStore string
		wantDead  string
	}{
		{
			name: "activation", kind: queue.KindSandboxActivate, queueJob: sandboxActivationQueueJob(),
			run: func(queueClient *recordingSessionPrepareQueue, store *recordingSandboxLifecycleStore, registry *ProviderRegistry) error {
				return (&SandboxActivationJobRunner{Queue: queueClient, Store: store, Providers: registry,
					Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second}}).RunOnce(context.Background())
			},
			wantStore: "exhausted:sandbox_activate", wantDead: "dead:qjob_activation:sandbox_activation_attempts_exhausted",
		},
		{
			name: "materialization", kind: queue.KindSandboxMaterialize, queueJob: sandboxMaterializationQueueJob(),
			run: func(queueClient *recordingSessionPrepareQueue, store *recordingSandboxLifecycleStore, registry *ProviderRegistry) error {
				return (&SandboxMaterializationJobRunner{Queue: queueClient, Store: store, Providers: registry,
					Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second}}).RunOnce(context.Background())
			},
			wantStore: "exhausted:sandbox_materialize", wantDead: "dead:qjob_materialization:sandbox_materialization_attempts_exhausted",
		},
	}
	adapter := &recordingLifecycleAdapter{}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.queueJob.AttemptCount = test.queueJob.MaxAttempts + 1
			test.queueJob.PayloadJson = `{not-json`
			queueClient := &recordingSessionPrepareQueue{leased: []*queuev1.QueueJob{test.queueJob}}
			store := &recordingSandboxLifecycleStore{}
			if err := test.run(queueClient, store, registry); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if !reflect.DeepEqual(store.calls, []string{test.wantStore}) {
				t.Fatalf("store calls = %v; want exhaustion before payload decode", store.calls)
			}
			if !reflect.DeepEqual(queueClient.transitions, []string{test.wantDead}) {
				t.Fatalf("queue transitions = %v; want dead letter", queueClient.transitions)
			}
		})
	}
}

func TestSandboxLifecycleRunnersSettleBusinessStateBeforeDeadLetteringInvalidPayload(t *testing.T) {
	tests := []struct {
		name     string
		kind     string
		queueJob *queuev1.QueueJob
		run      func(*recordingSessionPrepareQueue, *recordingSandboxLifecycleStore, *ProviderRegistry) error
		wantDead string
	}{
		{
			name: "activation", kind: queue.KindSandboxActivate, queueJob: sandboxActivationQueueJob(),
			run: func(queueClient *recordingSessionPrepareQueue, store *recordingSandboxLifecycleStore, registry *ProviderRegistry) error {
				return (&SandboxActivationJobRunner{Queue: queueClient, Store: store, Providers: registry,
					Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second}}).RunOnce(context.Background())
			},
			wantDead: "dead:qjob_activation:invalid_sandbox_activate_payload",
		},
		{
			name: "materialization", kind: queue.KindSandboxMaterialize, queueJob: sandboxMaterializationQueueJob(),
			run: func(queueClient *recordingSessionPrepareQueue, store *recordingSandboxLifecycleStore, registry *ProviderRegistry) error {
				return (&SandboxMaterializationJobRunner{Queue: queueClient, Store: store, Providers: registry,
					Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second}}).RunOnce(context.Background())
			},
			wantDead: "dead:qjob_materialization:invalid_sandbox_materialize_payload",
		},
	}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: &recordingLifecycleAdapter{}})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.queueJob.PayloadJson = `{not-json`
			queueClient := &recordingSessionPrepareQueue{leased: []*queuev1.QueueJob{test.queueJob}}
			store := &recordingSandboxLifecycleStore{}
			if err := test.run(queueClient, store, registry); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if !reflect.DeepEqual(store.calls, []string{"invalid:" + test.kind}) {
				t.Fatalf("store calls = %v; want business settlement before transport closure", store.calls)
			}
			if !reflect.DeepEqual(queueClient.transitions, []string{test.wantDead}) {
				t.Fatalf("queue transitions = %v; want invalid payload dead letter", queueClient.transitions)
			}
		})
	}
}

func TestSandboxLifecycleRunnersExecuteTheLastPermittedAttempt(t *testing.T) {
	tests := []struct {
		name     string
		queueJob *queuev1.QueueJob
		run      func(*recordingSessionPrepareQueue, *recordingSandboxLifecycleStore, *ProviderRegistry) error
		wantCall string
		wantAck  string
	}{
		{
			name: "activation", queueJob: sandboxActivationQueueJob(), wantCall: "claim", wantAck: "ack:qjob_activation",
			run: func(queueClient *recordingSessionPrepareQueue, store *recordingSandboxLifecycleStore, registry *ProviderRegistry) error {
				return (&SandboxActivationJobRunner{Queue: queueClient, Store: store, Providers: registry,
					Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second}}).RunOnce(context.Background())
			},
		},
		{
			name: "materialization", queueJob: sandboxMaterializationQueueJob(), wantCall: "claim-materialization", wantAck: "ack:qjob_materialization",
			run: func(queueClient *recordingSessionPrepareQueue, store *recordingSandboxLifecycleStore, registry *ProviderRegistry) error {
				return (&SandboxMaterializationJobRunner{Queue: queueClient, Store: store, Providers: registry,
					Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second}}).RunOnce(context.Background())
			},
		},
	}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: &recordingLifecycleAdapter{}})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.queueJob.AttemptCount = test.queueJob.MaxAttempts
			queueClient := &recordingSessionPrepareQueue{leased: []*queuev1.QueueJob{test.queueJob}}
			store := &recordingSandboxLifecycleStore{current: false}
			if err := test.run(queueClient, store, registry); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if !reflect.DeepEqual(store.calls, []string{test.wantCall}) {
				t.Fatalf("store calls = %v; want handler at exact budget", store.calls)
			}
			if !reflect.DeepEqual(queueClient.transitions, []string{test.wantAck}) {
				t.Fatalf("queue transitions = %v; want ack", queueClient.transitions)
			}
		})
	}
}

func TestSandboxLifecycleRunnersFinalizeRetryFromLastPermittedAttempt(t *testing.T) {
	tests := []struct {
		name     string
		kind     string
		queueJob *queuev1.QueueJob
		run      func(*recordingSessionPrepareQueue, *recordingSandboxLifecycleStore, *ProviderRegistry) error
		wantDead string
	}{
		{
			name: "activation", kind: queue.KindSandboxActivate, queueJob: sandboxActivationQueueJob(),
			run: func(queueClient *recordingSessionPrepareQueue, store *recordingSandboxLifecycleStore, registry *ProviderRegistry) error {
				return (&SandboxActivationJobRunner{Queue: queueClient, Store: store, Providers: registry,
					Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second}}).RunOnce(context.Background())
			},
			wantDead: "dead:qjob_activation:sandbox_activation_attempts_exhausted",
		},
		{
			name: "materialization", kind: queue.KindSandboxMaterialize, queueJob: sandboxMaterializationQueueJob(),
			run: func(queueClient *recordingSessionPrepareQueue, store *recordingSandboxLifecycleStore, registry *ProviderRegistry) error {
				return (&SandboxMaterializationJobRunner{Queue: queueClient, Store: store, Providers: registry,
					Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_lifecycle", LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second}}).RunOnce(context.Background())
			},
			wantDead: "dead:qjob_materialization:sandbox_materialization_attempts_exhausted",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.queueJob.AttemptCount = test.queueJob.MaxAttempts
			queueClient := &recordingSessionPrepareQueue{leased: []*queuev1.QueueJob{test.queueJob}}
			store := &recordingSandboxLifecycleStore{activation: sandboxActivationTestWork(false), materialization: sandboxMaterializationTestWork(), current: true}
			adapter := &recordingLifecycleAdapter{
				resolution: ProviderOutcome[ActivationResolution]{EffectBoundary: ProviderProvedNotStarted, Disposition: ProviderRetryable, ErrorKind: "provider_transition_in_progress"},
				inspection: ProviderOutcome[ExecutionReadiness]{EffectBoundary: ProviderProvedNotStarted, Disposition: ProviderRetryable, ErrorKind: "provider_transition_in_progress"},
			}
			registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
			if err != nil {
				t.Fatalf("NewProviderRegistry: %v", err)
			}
			if err := test.run(queueClient, store, registry); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			wantStore := "exhausted:" + test.kind
			if store.calls[len(store.calls)-1] != wantStore {
				t.Fatalf("store calls = %v; want final call %q", store.calls, wantStore)
			}
			if !reflect.DeepEqual(queueClient.transitions, []string{test.wantDead}) {
				t.Fatalf("queue transitions = %v; want dead letter after business finalization", queueClient.transitions)
			}
		})
	}
}

func sandboxActivationTestWork(mayCreate bool) SandboxActivationWork {
	return SandboxActivationWork{
		Job:  SandboxLifecycleJob{WorkspaceID: "ws_lifecycle", SessionID: "sesn_lifecycle", LogicalSandboxID: "sbox_lifecycle", OperationID: "sop_activation"},
		Kind: ActivationCreate, Provider: sandboxdriver.DaytonaProviderName,
		StableName: "sbox_lifecycle", Labels: map[string]string{"tetral.workspace_id": "ws_lifecycle"},
		MayCreate: mayCreate,
		Setup:     sandbox.SandboxSetup{WorkspaceID: "ws_lifecycle", SessionID: "sesn_lifecycle", SandboxID: "sbox_lifecycle", LifecycleOperationID: "sop_activation", EnvironmentID: "env_lifecycle", ProviderArtifactRef: "artifact_lifecycle"},
	}
}

func sandboxMaterializationTestWork() SandboxMaterializationWork {
	return SandboxMaterializationWork{
		Job:             SandboxLifecycleJob{WorkspaceID: "ws_lifecycle", SessionID: "sesn_lifecycle", LogicalSandboxID: "sbox_lifecycle", OperationID: "sop_materialization"},
		Provider:        sandboxdriver.DaytonaProviderName,
		Handle:          sandbox.ProviderHandle{Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider_lifecycle"},
		BindingRevision: 1, TargetEnvironmentGeneration: 1, TargetResourceRevision: 1,
		Setup: sandbox.SandboxSetup{WorkspaceID: "ws_lifecycle", SessionID: "sesn_lifecycle", SandboxID: "sbox_lifecycle",
			LifecycleOperationID: "sop_materialization", EnvironmentID: "env_lifecycle", EnvironmentGeneration: 1, ResourceRevision: 1},
	}
}

func sandboxActivationQueueJob() *queuev1.QueueJob {
	return &queuev1.QueueJob{
		Id: "qjob_activation", WorkspaceId: "ws_lifecycle", Kind: queue.KindSandboxActivate,
		PartitionKey: queue.FormatSandboxLifecyclePartitionKey("ws_lifecycle", "sbox_lifecycle"),
		DedupeKey:    queue.FormatSandboxLifecycleDedupeKey(queue.KindSandboxActivate, "ws_lifecycle", "sbox_lifecycle", "sop_activation"),
		PayloadJson:  `{"workspace_id":"ws_lifecycle","session_id":"sesn_lifecycle","logical_sandbox_id":"sbox_lifecycle","operation_id":"sop_activation"}`,
		LeaseToken:   "lease_activation", AttemptCount: 1, MaxAttempts: 5,
	}
}

func sandboxMaterializationQueueJob() *queuev1.QueueJob {
	return &queuev1.QueueJob{
		Id: "qjob_materialization", WorkspaceId: "ws_lifecycle", Kind: queue.KindSandboxMaterialize,
		PartitionKey: queue.FormatSandboxLifecyclePartitionKey("ws_lifecycle", "sbox_lifecycle"),
		DedupeKey:    queue.FormatSandboxLifecycleDedupeKey(queue.KindSandboxMaterialize, "ws_lifecycle", "sbox_lifecycle", "sop_materialization"),
		PayloadJson:  `{"workspace_id":"ws_lifecycle","session_id":"sesn_lifecycle","logical_sandbox_id":"sbox_lifecycle","operation_id":"sop_materialization"}`,
		LeaseToken:   "lease_materialization", AttemptCount: 1, MaxAttempts: 5,
	}
}

type recordingSandboxLifecycleStore struct {
	activation      SandboxActivationWork
	replacement     SandboxActivationWork
	materialization SandboxMaterializationWork
	current         bool
	calls           []string
}

func (s *recordingSandboxLifecycleStore) ClaimActivation(context.Context, SandboxLifecycleJob, time.Time) (SandboxActivationWork, bool, error) {
	s.calls = append(s.calls, "claim")
	return s.activation, s.current, nil
}
func (s *recordingSandboxLifecycleStore) CompleteActivation(_ context.Context, _ SandboxActivationWork, handle sandbox.ProviderHandle, _ time.Time) error {
	s.calls = append(s.calls, "complete:"+handle.SandboxID)
	return nil
}
func (s *recordingSandboxLifecycleStore) ReplaceMissingActivation(_ context.Context, _ SandboxActivationWork, _ time.Time) (SandboxActivationWork, bool, error) {
	s.calls = append(s.calls, "replace-missing")
	return s.replacement, s.current, nil
}
func (s *recordingSandboxLifecycleStore) ObserveUnknownActivation(_ context.Context, _ SandboxActivationWork, kind string, _ time.Time) error {
	s.calls = append(s.calls, "observe:"+kind)
	return nil
}
func (s *recordingSandboxLifecycleStore) FailActivation(_ context.Context, _ SandboxActivationWork, boundary ProviderEffectBoundary, _ ProviderDisposition, kind string, _ string, _ time.Time) error {
	s.calls = append(s.calls, "fail:"+string(boundary)+":"+kind)
	return nil
}
func (s *recordingSandboxLifecycleStore) ClaimMaterialization(context.Context, SandboxLifecycleJob, time.Time) (SandboxMaterializationWork, bool, error) {
	s.calls = append(s.calls, "claim-materialization")
	return s.materialization, s.current, nil
}
func (s *recordingSandboxLifecycleStore) WaitMaterializationForActivation(_ context.Context, _ SandboxMaterializationWork, readiness ExecutionReadiness, _ time.Time) error {
	s.calls = append(s.calls, "materialization-activation:"+string(readiness))
	return nil
}
func (s *recordingSandboxLifecycleStore) CompleteMaterialization(_ context.Context, _ SandboxMaterializationWork, _ MaterializationResult, _ time.Time) error {
	s.calls = append(s.calls, "complete-materialization")
	return nil
}
func (s *recordingSandboxLifecycleStore) FailMaterialization(_ context.Context, _ SandboxMaterializationWork, boundary ProviderEffectBoundary, disposition ProviderDisposition, kind string, _ string, _ time.Time) error {
	s.calls = append(s.calls, "fail-materialization:"+string(boundary)+":"+string(disposition)+":"+kind)
	return nil
}
func (s *recordingSandboxLifecycleStore) FinalizeExhaustedLifecycle(_ context.Context, _ *queuev1.QueueJob, kind string, _ time.Time) error {
	s.calls = append(s.calls, "exhausted:"+kind)
	return nil
}
func (s *recordingSandboxLifecycleStore) FinalizeInvalidLifecycle(_ context.Context, _ *queuev1.QueueJob, kind string, _ time.Time) error {
	s.calls = append(s.calls, "invalid:"+kind)
	return nil
}

type recordingLifecycleAdapter struct {
	resolution         ProviderOutcome[ActivationResolution]
	activation         ProviderOutcome[sandbox.ProviderHandle]
	inspection         ProviderOutcome[ExecutionReadiness]
	inspectionSequence []ProviderOutcome[ExecutionReadiness]
	materialization    ProviderOutcome[MaterializationResult]
	calls              []string
}

func (a *recordingLifecycleAdapter) ResolveActivation(context.Context, ActivationResolutionRequest) ProviderOutcome[ActivationResolution] {
	a.calls = append(a.calls, "resolve")
	return a.resolution
}
func (a *recordingLifecycleAdapter) Activate(_ context.Context, request ActivationRequest) ProviderOutcome[sandbox.ProviderHandle] {
	a.calls = append(a.calls, "activate:"+string(request.Kind))
	return a.activation
}
func (a *recordingLifecycleAdapter) InspectForExecution(context.Context, string) ProviderOutcome[ExecutionReadiness] {
	a.calls = append(a.calls, "inspect")
	if len(a.inspectionSequence) > 0 {
		outcome := a.inspectionSequence[0]
		a.inspectionSequence = a.inspectionSequence[1:]
		return outcome
	}
	return a.inspection
}
func (a *recordingLifecycleAdapter) MaterializeResources(context.Context, MaterializationRequest) ProviderOutcome[MaterializationResult] {
	a.calls = append(a.calls, "materialize")
	return a.materialization
}
func (a *recordingLifecycleAdapter) PrepareTool(context.Context, ToolExecutionRequest) ProviderOutcome[ToolPreparationResult] {
	return ProviderOutcome[ToolPreparationResult]{Value: ToolPreparationResult{Prepared: recordingPreparedTool{}}}
}
func (a *recordingLifecycleAdapter) ExecuteTool(context.Context, ToolExecutionRequest) ProviderOutcome[sandboxdriver.ToolExecution] {
	return ProviderOutcome[sandboxdriver.ToolExecution]{}
}
func (a *recordingLifecycleAdapter) ObserveTool(context.Context, sandboxdriver.ForegroundCommandObservation) ProviderOutcome[sandboxdriver.ToolExecution] {
	return ProviderOutcome[sandboxdriver.ToolExecution]{}
}
func (a *recordingLifecycleAdapter) Release(context.Context, ReleaseRequest) ProviderOutcome[ReleaseResult] {
	return ProviderOutcome[ReleaseResult]{}
}
