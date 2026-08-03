package tetralsandbox

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sandbox"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

func TestSandboxToolExecutionRunnerFinalizesOnlyPastAttemptBudget(t *testing.T) {
	job := sandboxExecutionQueueJob()
	job.AttemptCount = job.MaxAttempts + 1
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{job}}
	coordinator := &recordingSandboxExecutionCoordinator{}
	adapter := &recordingProviderAdapter{}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxToolExecutionJobRunner{
		Queue: queueClient, Coordinator: coordinator, Providers: registry, Media: passthroughSandboxMedia{},
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
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{job}}
	coordinator := &recordingSandboxExecutionCoordinator{load: false}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: &recordingProviderAdapter{}})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxToolExecutionJobRunner{
		Queue: queueClient, Coordinator: coordinator, Providers: registry, Media: passthroughSandboxMedia{},
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
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{job}}
	coordinator := &recordingSandboxExecutionCoordinator{}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: &recordingProviderAdapter{}})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxToolExecutionJobRunner{
		Queue: queueClient, Coordinator: coordinator, Providers: registry, Media: passthroughSandboxMedia{},
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
	finalizing := make(chan struct{})
	heartbeatObserved := make(chan struct{}, 1)
	queueClient := &observingSandboxFinalizerQueue{
		recordingSandboxQueue: recordingSandboxQueue{leased: []*queuev1.QueueJob{job}},
		finalizing:            finalizing, heartbeatObserved: heartbeatObserved,
	}
	coordinator := &recordingSandboxExecutionCoordinator{
		work: sandboxExecutionTestWork(true), load: true,
		finalizeStarted: finalizing, heartbeatObserved: heartbeatObserved,
	}
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
		Queue: queueClient, Coordinator: coordinator, Providers: registry, Media: passthroughSandboxMedia{},
		Config: SandboxToolExecutionRunnerConfig{WorkspaceID: "ws_execution", LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Millisecond, PreparationTimeout: 45 * time.Second},
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
			queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{sandboxExecutionQueueJob()}}
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
				Media:       passthroughSandboxMedia{},
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
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{sandboxExecutionQueueJob()}}
	work := sandboxExecutionTestWork(true)
	work.State = "running"
	coordinator := &recordingSandboxExecutionCoordinator{work: work, load: true}
	adapter := &recordingProviderAdapter{}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxToolExecutionJobRunner{
		Queue: queueClient, Coordinator: coordinator, Providers: registry, Media: passthroughSandboxMedia{},
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

func TestSandboxToolExecutionRunnerRecoversDurableMediaCustodyBeforeUnknownOutcome(t *testing.T) {
	tests := []struct {
		name       string
		recovery   SandboxMediaRecovery
		settlement SandboxExecutionSettlementKind
	}{
		{name: "staged", recovery: SandboxMediaRecovery{Found: true, Ready: true, ResultJSON: `{"status":"success","result":{"attachment_ref":"att_recovered"}}`}, settlement: SandboxExecutionCompleted},
		{name: "uploading", recovery: SandboxMediaRecovery{Found: true}, settlement: SandboxExecutionFailed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{sandboxExecutionQueueJob()}}
			work := sandboxExecutionTestWork(true)
			work.State = "running"
			coordinator := &recordingSandboxExecutionCoordinator{work: work, load: true}
			registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: &recordingProviderAdapter{}})
			if err != nil {
				t.Fatalf("NewProviderRegistry: %v", err)
			}
			runner := &SandboxToolExecutionJobRunner{
				Queue: queueClient, Coordinator: coordinator, Providers: registry,
				Media:  recoveringSandboxMedia{recovery: test.recovery},
				Config: SandboxToolExecutionRunnerConfig{WorkspaceID: "ws_execution", LeaseDuration: 2 * time.Minute, HeartbeatInterval: 15 * time.Second, PreparationTimeout: 45 * time.Second},
			}
			if err := runner.RunOnce(context.Background()); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if len(coordinator.settlements) != 1 || coordinator.settlements[0].Kind != test.settlement {
				t.Fatalf("settlements = %#v; want %s", coordinator.settlements, test.settlement)
			}
		})
	}
}

func TestSandboxToolExecutionRunnerOwnsBackgroundTaskSourceIdentity(t *testing.T) {
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{sandboxExecutionQueueJob()}}
	work := sandboxExecutionTestWork(true)
	coordinator := &recordingSandboxExecutionCoordinator{work: work, load: true, prepare: true, authorize: true}
	adapter := &recordingProviderAdapter{inspection: ProviderOutcome[ExecutionReadiness]{Value: ExecutionReady}, execution: ProviderOutcome[sandboxdriver.ToolExecution]{
		Value: sandboxdriver.ToolExecution{
			ResultJSON: `{"status":"running","result":{"task_id":"task_execution"}}`,
			BackgroundTask: &sandboxdriver.BackgroundTask{
				TaskID: "task_execution", ProviderSessionID: "provider_execution",
				ProviderCommandID: "command_execution", ProviderCommandMetadataJSON: `{}`,
			},
		},
	}}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxToolExecutionJobRunner{
		Queue: queueClient, Coordinator: coordinator, Providers: registry, Media: passthroughSandboxMedia{},
		Config: SandboxToolExecutionRunnerConfig{WorkspaceID: "ws_execution", LeaseDuration: 2 * time.Minute, HeartbeatInterval: 15 * time.Second, PreparationTimeout: 45 * time.Second},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(coordinator.settlements) != 1 || coordinator.settlements[0].BackgroundTask == nil ||
		coordinator.settlements[0].BackgroundTask.SourceToolUseEventID != work.Ref.ToolUseEventID {
		t.Fatalf("settlements = %#v; want runner-owned Tool Use source", coordinator.settlements)
	}
}

func TestSandboxToolExecutionRunnerDoesNotCallProviderWhenAuthorizationIsCancelled(t *testing.T) {
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{sandboxExecutionQueueJob()}}
	coordinator := &recordingSandboxExecutionCoordinator{
		work: sandboxExecutionTestWork(true), load: true, prepare: true, authorize: false,
	}
	adapter := &recordingProviderAdapter{inspection: ProviderOutcome[ExecutionReadiness]{Value: ExecutionReady}}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxToolExecutionJobRunner{
		Queue: queueClient, Coordinator: coordinator, Providers: registry, Media: passthroughSandboxMedia{},
		Config: SandboxToolExecutionRunnerConfig{WorkspaceID: "ws_execution", LeaseDuration: 2 * time.Minute, HeartbeatInterval: 15 * time.Second, PreparationTimeout: 45 * time.Second},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(coordinator.calls, []string{"load", "preparing", "running"}) {
		t.Fatalf("coordinator calls = %v; want authorization cancellation before settlement", coordinator.calls)
	}
	if !reflect.DeepEqual(adapter.calls, []string{"inspect", "prepare"}) {
		t.Fatalf("provider calls = %v; want no execution after authorization cancellation", adapter.calls)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"ack:qjob_sandbox_execution"}) {
		t.Fatalf("queue transitions = %v; want acknowledged cancelled execution", queueClient.transitions)
	}
}

func TestSandboxToolExecutionRunnerObservesRunningExecutionByStoredReference(t *testing.T) {
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{sandboxExecutionQueueJob()}}
	work := sandboxExecutionTestWork(true)
	work.State = "running"
	encodedReference, err := encodeSandboxToolObservationReference(sandboxdriver.DaytonaProviderName, sandboxdriver.ForegroundCommandObservation{
		Reference: sandboxdriver.CommandReference{
			Target: sandboxdriver.ToolTarget{ProviderSandboxID: "provider_execution"},
			Task: sandboxdriver.BackgroundTask{
				TaskID: "task_execution", ProviderSessionID: "provider_execution", ProviderCommandID: "task_execution",
			},
		},
	})
	if err != nil {
		t.Fatalf("encode command reference: %v", err)
	}
	work.ProviderCommandReference = encodedReference
	coordinator := &recordingSandboxExecutionCoordinator{work: work, load: true}
	adapter := &recordingProviderAdapter{observation: ProviderOutcome[sandboxdriver.ToolExecution]{
		Value: sandboxdriver.ToolExecution{ResultJSON: `{"status":"success"}`},
	}}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxToolExecutionJobRunner{
		Queue: queueClient, Coordinator: coordinator, Providers: registry, Media: passthroughSandboxMedia{},
		Config: SandboxToolExecutionRunnerConfig{WorkspaceID: "ws_execution", LeaseDuration: 2 * time.Minute, HeartbeatInterval: 15 * time.Second, PreparationTimeout: 45 * time.Second},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(coordinator.calls, []string{"load", "settle:completed"}) {
		t.Fatalf("coordinator calls = %v; want observed terminal settlement", coordinator.calls)
	}
	if !reflect.DeepEqual(adapter.calls, []string{"observe"}) {
		t.Fatalf("provider calls = %v; want observation without submission", adapter.calls)
	}
}

func TestSandboxToolExecutionRunnerSpacesForegroundObservationPolls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		work, firstObservation := foregroundObservationTestWork(t)
		coordinator := &recordingSandboxExecutionCoordinator{work: work, load: true}
		adapter := &recordingProviderAdapter{observations: []ProviderOutcome[sandboxdriver.ToolExecution]{
			{Value: sandboxdriver.ToolExecution{ForegroundObservation: &firstObservation}},
			{Value: sandboxdriver.ToolExecution{ResultJSON: `{"status":"success"}`}},
		}}
		registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
		if err != nil {
			t.Fatalf("NewProviderRegistry: %v", err)
		}
		var observationTimes []time.Time
		adapter.onObserve = func() {
			observationTimes = append(observationTimes, time.Now())
		}
		runner := &SandboxToolExecutionJobRunner{
			Queue:       &recordingSandboxQueue{leased: []*queuev1.QueueJob{sandboxExecutionQueueJob()}},
			Coordinator: coordinator, Providers: registry, Media: passthroughSandboxMedia{},
			Config: SandboxToolExecutionRunnerConfig{WorkspaceID: "ws_execution", LeaseDuration: 2 * time.Minute, HeartbeatInterval: 15 * time.Second, PreparationTimeout: 45 * time.Second},
		}
		if err := runner.RunOnce(context.Background()); err != nil {
			t.Fatalf("RunOnce: %v", err)
		}
		if len(observationTimes) != 2 || observationTimes[1].Sub(observationTimes[0]) != sandboxForegroundObservationPollInterval {
			t.Fatalf("observation times = %v; want two polls separated by %s", observationTimes, sandboxForegroundObservationPollInterval)
		}
		if !reflect.DeepEqual(adapter.calls, []string{"observe", "observe"}) {
			t.Fatalf("provider calls = %v; want two spaced observations", adapter.calls)
		}
	})
}

func TestSandboxToolExecutionRunnerCancelsForegroundObservationWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		work, firstObservation := foregroundObservationTestWork(t)
		coordinator := &recordingSandboxExecutionCoordinator{work: work, load: true}
		adapter := &recordingProviderAdapter{observations: []ProviderOutcome[sandboxdriver.ToolExecution]{
			{Value: sandboxdriver.ToolExecution{ForegroundObservation: &firstObservation}},
		}}
		registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
		if err != nil {
			t.Fatalf("NewProviderRegistry: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		adapter.onObserve = cancel
		runner := &SandboxToolExecutionJobRunner{
			Queue:       &recordingSandboxQueue{leased: []*queuev1.QueueJob{sandboxExecutionQueueJob()}},
			Coordinator: coordinator, Providers: registry, Media: passthroughSandboxMedia{},
			Config: SandboxToolExecutionRunnerConfig{WorkspaceID: "ws_execution", LeaseDuration: 2 * time.Minute, HeartbeatInterval: 15 * time.Second, PreparationTimeout: 45 * time.Second},
		}
		if err := runner.RunOnce(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("RunOnce after cancellation = %v; want context canceled", err)
		}
		if !reflect.DeepEqual(adapter.calls, []string{"observe"}) {
			t.Fatalf("provider calls = %v; want no observation after cancellation", adapter.calls)
		}
	})
}

func foregroundObservationTestWork(t *testing.T) (SandboxExecutionWork, sandboxdriver.ForegroundCommandObservation) {
	t.Helper()
	work := sandboxExecutionTestWork(true)
	work.State = "running"
	observation := sandboxdriver.ForegroundCommandObservation{Reference: sandboxdriver.CommandReference{
		Target: sandboxdriver.ToolTarget{ProviderSandboxID: "provider_execution"},
		Task:   sandboxdriver.BackgroundTask{TaskID: "task_execution", ProviderSessionID: "provider_execution", ProviderCommandID: "command_execution"},
	}}
	encodedReference, err := encodeSandboxToolObservationReference(sandboxdriver.DaytonaProviderName, observation)
	if err != nil {
		t.Fatalf("encode command reference: %v", err)
	}
	work.ProviderCommandReference = encodedReference
	return work, observation
}

func TestSandboxToolExecutionRunnerRejectsForegroundObservationAfterQueueTakeover(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ref := SandboxExecutionRef{
		WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store",
		SessionThreadID: "thr_execution_store", ToolUseEventID: "evt_execution_a",
	}
	encodedReference, err := encodeSandboxToolObservationReference(sandboxdriver.DaytonaProviderName, sandboxdriver.ForegroundCommandObservation{
		Reference: sandboxdriver.CommandReference{
			Target: sandboxdriver.ToolTarget{ProviderSandboxID: "provider_execution_store"},
			Task: sandboxdriver.BackgroundTask{
				TaskID: "task_execution", ProviderSessionID: "provider_execution_store", ProviderCommandID: "command_execution",
			},
		},
	})
	if err != nil {
		t.Fatalf("encode command reference: %v", err)
	}
	if _, err := adminDB.Exec(`UPDATE session_runtime_tool_results
		SET execution_state='running', provider_command_reference_json=$5
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND tool_use_event_id=$4`,
		ref.WorkspaceID, ref.SessionID, ref.SessionThreadID, ref.ToolUseEventID, encodedReference); err != nil {
		t.Fatalf("seed running Sandbox execution: %v", err)
	}
	payload, err := sandboxExecutionQueuePayload(ref)
	if err != nil {
		t.Fatalf("encode Sandbox execution Queue payload: %v", err)
	}
	request := queue.EnqueueRequest{
		ID: queue.NewJobID(), WorkspaceID: "ws_execution_store", Kind: queue.KindSandboxToolExecute,
		PartitionKey:   queue.FormatSandboxExecutionPartitionKey("ws_execution_store", ref.SessionID, ref.SessionThreadID, ref.ToolUseEventID),
		DedupeKey:      queue.FormatSandboxToolExecuteDedupeKey("ws_execution_store", ref.SessionID, ref.SessionThreadID, ref.ToolUseEventID, 1),
		PayloadVersion: 1, PayloadJSON: payload, MaxAttempts: SandboxToolExecuteMaxAttempts,
	}
	observeStarted := make(chan struct{})
	observeRelease := make(chan struct{})
	firstResult := make(chan error, 1)
	firstQueue := &recordingSandboxQueue{}
	firstAdapter := &recordingProviderAdapter{
		observation:    ProviderOutcome[sandboxdriver.ToolExecution]{Value: sandboxdriver.ToolExecution{ResultJSON: `{"status":"success"}`}},
		observeStarted: observeStarted, observeRelease: observeRelease,
	}
	registryFor := func(adapter ProviderAdapter) *ProviderRegistry {
		t.Helper()
		registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
		if err != nil {
			t.Fatalf("NewProviderRegistry: %v", err)
		}
		return registry
	}
	firstRegistry := registryFor(firstAdapter)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	_, _, _, secondJob := supersedeSandboxQueueLeaseAfter(t, runtimeDB, adminDB, request, func(runCtx context.Context, firstJob *queuev1.QueueJob) {
		firstQueue.leased = []*queuev1.QueueJob{firstJob}
		go func() {
			firstResult <- (&SandboxToolExecutionJobRunner{
				Queue: firstQueue, Coordinator: coordinator, Providers: firstRegistry, Media: passthroughSandboxMedia{},
				Config: SandboxToolExecutionRunnerConfig{
					WorkspaceID: ref.WorkspaceID, LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second, PreparationTimeout: 45 * time.Second,
				},
			}).RunOnce(runCtx)
		}()
		select {
		case <-observeStarted:
		case <-time.After(5 * time.Second):
			t.Fatal("foreground observation did not reach provider barrier")
		}
	})
	close(observeRelease)
	if err := <-firstResult; !errors.Is(err, errQueueLeaseLost) {
		t.Fatalf("superseded foreground observation error = %v; want Queue authority loss", err)
	}
	if len(firstQueue.transitions) != 0 {
		t.Fatalf("superseded foreground Queue transitions = %v; want none", firstQueue.transitions)
	}
	secondQueue := &recordingSandboxQueue{leased: []*queuev1.QueueJob{secondJob}}
	secondAdapter := &recordingProviderAdapter{observation: ProviderOutcome[sandboxdriver.ToolExecution]{
		Value: sandboxdriver.ToolExecution{ResultJSON: `{"status":"success"}`},
	}}
	secondRegistry := registryFor(secondAdapter)
	secondRunner := &SandboxToolExecutionJobRunner{
		Queue: secondQueue, Coordinator: coordinator, Providers: secondRegistry, Media: passthroughSandboxMedia{},
		Config: SandboxToolExecutionRunnerConfig{
			WorkspaceID: ref.WorkspaceID, LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second, PreparationTimeout: 45 * time.Second,
		},
	}
	if err := secondRunner.RunOnce(context.Background()); err != nil {
		t.Fatalf("successor RunOnce: %v", err)
	}
	assertSandboxExecutionState(t, adminDB, ref.ToolUseEventID, "terminal_unconsumed", 1)
	if !reflect.DeepEqual(secondQueue.transitions, []string{"ack:" + secondJob.GetId()}) {
		t.Fatalf("successor Queue transitions = %v; want one ack", secondQueue.transitions)
	}
}

func TestSandboxToolExecutionRunnerSettlesOnlyDurableProviderOutcomes(t *testing.T) {
	tests := []struct {
		name            string
		inspection      ProviderOutcome[ExecutionReadiness]
		preparation     ProviderOutcome[ToolPreparationResult]
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
			name:       "sandbox disappears before preparation",
			inspection: ProviderOutcome[ExecutionReadiness]{Value: ExecutionReady},
			preparation: ProviderOutcome[ToolPreparationResult]{
				EffectBoundary: ProviderProvedNotStarted, Disposition: ProviderTerminal,
				ErrorKind: string(sandbox.ProviderErrorNotFound), SafeMessage: "daytona sandbox not found",
			},
			wantCoordinator: []string{"load", "preparing", "activation:needs_creation"},
			wantTransitions: []string{"ack:qjob_sandbox_execution"},
			wantAdapter:     []string{"inspect", "prepare"},
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
			queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{sandboxExecutionQueueJob()}}
			coordinator := &recordingSandboxExecutionCoordinator{work: sandboxExecutionTestWork(true), load: true, prepare: true, authorize: true}
			adapter := &recordingProviderAdapter{inspection: test.inspection, preparation: test.preparation, execution: test.execution}
			registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
			if err != nil {
				t.Fatalf("NewProviderRegistry: %v", err)
			}
			runner := &SandboxToolExecutionJobRunner{
				Queue: queueClient, Coordinator: coordinator, Providers: registry, Media: passthroughSandboxMedia{},
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

func TestSandboxToolExecutionRunnerReturnsMediaCustodyFailureAsToolError(t *testing.T) {
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{sandboxExecutionQueueJob()}}
	coordinator := &recordingSandboxExecutionCoordinator{work: sandboxExecutionTestWork(true), load: true, prepare: true, authorize: true}
	adapter := &recordingProviderAdapter{
		inspection: ProviderOutcome[ExecutionReadiness]{Value: ExecutionReady},
		execution:  ProviderOutcome[sandboxdriver.ToolExecution]{Value: sandboxdriver.ToolExecution{ResultJSON: `{"status":"success"}`}},
	}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxToolExecutionJobRunner{
		Queue: queueClient, Coordinator: coordinator, Providers: registry,
		Media:  failingSandboxMedia{err: errors.New("temporary Blob failure")},
		Config: SandboxToolExecutionRunnerConfig{WorkspaceID: "ws_execution", LeaseDuration: 2 * time.Minute, HeartbeatInterval: 15 * time.Second, PreparationTimeout: 45 * time.Second},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(coordinator.calls, []string{"load", "preparing", "running", "settle:failed"}) {
		t.Fatalf("coordinator calls = %v; want terminal Tool Error settlement", coordinator.calls)
	}
	if len(coordinator.settlements) != 1 || coordinator.settlements[0].ErrorKind != "transient_attachment_unavailable" {
		t.Fatalf("settlements = %#v; want transient attachment Tool Error", coordinator.settlements)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"ack:qjob_sandbox_execution"}) {
		t.Fatalf("queue transitions = %v; want transport closure after business settlement", queueClient.transitions)
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

type passthroughSandboxMedia struct{}

func (passthroughSandboxMedia) MaterializeResult(_ context.Context, _ SandboxExecutionRef, _ string, _ string, raw string, _ time.Time) (string, error) {
	return raw, nil
}

func (passthroughSandboxMedia) RecoverResult(context.Context, SandboxExecutionRef) (SandboxMediaRecovery, error) {
	return SandboxMediaRecovery{}, nil
}

type failingSandboxMedia struct{ err error }

func (m failingSandboxMedia) MaterializeResult(context.Context, SandboxExecutionRef, string, string, string, time.Time) (string, error) {
	return "", m.err
}

func (failingSandboxMedia) RecoverResult(context.Context, SandboxExecutionRef) (SandboxMediaRecovery, error) {
	return SandboxMediaRecovery{}, nil
}

type recoveringSandboxMedia struct{ recovery SandboxMediaRecovery }

func (recoveringSandboxMedia) MaterializeResult(_ context.Context, _ SandboxExecutionRef, _ string, _ string, raw string, _ time.Time) (string, error) {
	return raw, nil
}

func (m recoveringSandboxMedia) RecoverResult(context.Context, SandboxExecutionRef) (SandboxMediaRecovery, error) {
	return m.recovery, nil
}

type recordingSandboxExecutionCoordinator struct {
	work              SandboxExecutionWork
	load              bool
	prepare           bool
	authorize         bool
	calls             []string
	settlements       []SandboxExecutionSettlement
	finalizeStarted   chan struct{}
	heartbeatObserved <-chan struct{}
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
func (c *recordingSandboxExecutionCoordinator) RecordProviderCommandReference(context.Context, SandboxExecutionWork, string) (bool, error) {
	c.calls = append(c.calls, "reference")
	return true, nil
}
func (c *recordingSandboxExecutionCoordinator) SettleExecution(_ context.Context, _ SandboxExecutionWork, result SandboxExecutionSettlement) error {
	c.calls = append(c.calls, "settle:"+string(result.Kind))
	c.settlements = append(c.settlements, result)
	return nil
}
func (c *recordingSandboxExecutionCoordinator) FinalizeExhaustedExecution(context.Context, *queuev1.QueueJob) error {
	c.calls = append(c.calls, "exhausted")
	if c.finalizeStarted != nil {
		return requireHeartbeatDuringFinalizer(c.finalizeStarted, c.heartbeatObserved)
	}
	return nil
}
func (c *recordingSandboxExecutionCoordinator) FinalizeInvalidExecution(context.Context, *queuev1.QueueJob) error {
	c.calls = append(c.calls, "invalid")
	return nil
}

type recordingProviderAdapter struct {
	inspection       ProviderOutcome[ExecutionReadiness]
	preparation      ProviderOutcome[ToolPreparationResult]
	execution        ProviderOutcome[sandboxdriver.ToolExecution]
	observation      ProviderOutcome[sandboxdriver.ToolExecution]
	observations     []ProviderOutcome[sandboxdriver.ToolExecution]
	observationIndex int
	observeStarted   chan struct{}
	observeRelease   <-chan struct{}
	onObserve        func()
	calls            []string
}

func (a *recordingProviderAdapter) InspectForExecution(context.Context, string) ProviderOutcome[ExecutionReadiness] {
	a.calls = append(a.calls, "inspect")
	return a.inspection
}
func (a *recordingProviderAdapter) InspectForRelease(context.Context, string) ProviderOutcome[bool] {
	return ProviderOutcome[bool]{Value: true}
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
	if a.preparation.Failed() {
		return a.preparation
	}
	return ProviderOutcome[ToolPreparationResult]{Value: ToolPreparationResult{Prepared: recordingPreparedTool{}}}
}
func (a *recordingProviderAdapter) ExecuteTool(context.Context, ToolExecutionRequest) ProviderOutcome[sandboxdriver.ToolExecution] {
	a.calls = append(a.calls, "execute")
	return a.execution
}
func (a *recordingProviderAdapter) ObserveTool(ctx context.Context, _ sandboxdriver.ForegroundCommandObservation) ProviderOutcome[sandboxdriver.ToolExecution] {
	a.calls = append(a.calls, "observe")
	if a.onObserve != nil {
		a.onObserve()
	}
	if a.observeStarted != nil {
		close(a.observeStarted)
	}
	if a.observeRelease != nil {
		select {
		case <-a.observeRelease:
		case <-ctx.Done():
			return ProviderOutcome[sandboxdriver.ToolExecution]{
				EffectBoundary: ProviderProvedNotStarted, Disposition: ProviderRetryable, ErrorKind: "provider_observation_cancelled",
			}
		}
	}
	if a.observationIndex < len(a.observations) {
		outcome := a.observations[a.observationIndex]
		a.observationIndex++
		return outcome
	}
	return a.observation
}

type recordingPreparedTool struct{}

func (recordingPreparedTool) providerPreparedTool() {}
func (a *recordingProviderAdapter) Release(context.Context, ReleaseRequest) ProviderOutcome[ReleaseResult] {
	return ProviderOutcome[ReleaseResult]{}
}
