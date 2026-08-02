package tetralsandbox

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sandbox"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

func TestSandboxBackgroundReconcileRunnerSchedulesOneSuccessor(t *testing.T) {
	job := sandboxBackgroundReconcileQueueJob()
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{job}}
	store := &recordingBackgroundCommandStore{reconcileCurrent: true, task: sandboxBackgroundTaskWork()}
	adapter := &recordingBackgroundProviderAdapter{poll: ProviderOutcome[sandboxdriver.CommandResult]{Value: sandboxdriver.CommandResult{ResultJSON: `{"status":"running"}`}}}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxBackgroundReconcileJobRunner{
		Queue: queueClient, Store: store, Providers: registry,
		Config: SandboxBackgroundRunnerConfig{WorkspaceID: "ws_background", LeaseDuration: 2 * time.Minute, HeartbeatInterval: 15 * time.Second},
		Clock:  func() time.Time { return time.Unix(1_000, 0).UTC() },
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(adapter.calls, []string{"poll"}) {
		t.Fatalf("provider calls = %v; want one poll", adapter.calls)
	}
	if !reflect.DeepEqual(store.calls, []string{"load_reconcile", "advance_reconcile"}) {
		t.Fatalf("store calls = %v; want one successor transition", store.calls)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"ack:qjob_background_reconcile"}) {
		t.Fatalf("queue transitions = %v; want ack", queueClient.transitions)
	}
}

func TestSandboxBackgroundCommandRunnerDoesNotReplaySubmittedInput(t *testing.T) {
	job := sandboxBackgroundCommandQueueJob("stdin")
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{job}}
	store := &recordingBackgroundCommandStore{commandCurrent: true, operation: SandboxBackgroundOperationWork{
		Task: sandboxBackgroundTaskWork(), Kind: SandboxBackgroundOperationStdin, State: SandboxBackgroundOperationSubmitted,
	}}
	adapter := &recordingBackgroundProviderAdapter{}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxBackgroundCommandJobRunner{
		Queue: queueClient, Store: store, Providers: registry,
		Config: SandboxBackgroundRunnerConfig{WorkspaceID: "ws_background", LeaseDuration: 2 * time.Minute, HeartbeatInterval: 15 * time.Second},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(adapter.calls) != 0 {
		t.Fatalf("provider calls = %v; want none after submitted handoff", adapter.calls)
	}
	if !reflect.DeepEqual(store.calls, []string{"load_command", "settle_command:unknown_outcome"}) {
		t.Fatalf("store calls = %v; want unknown-outcome settlement", store.calls)
	}
}

func TestSandboxBackgroundRunnersKeepHeartbeatThroughLiveExhaustion(t *testing.T) {
	tests := []struct {
		name           string
		job            *queuev1.QueueJob
		configureStore func(*recordingBackgroundCommandStore)
		run            func(*observingSandboxFinalizerQueue, *recordingBackgroundCommandStore, *ProviderRegistry) error
		wantTransition string
	}{
		{
			name: "reconcile", job: sandboxBackgroundReconcileQueueJob(),
			configureStore: func(store *recordingBackgroundCommandStore) {
				store.reconcileErr = errors.New("background reconcile store unavailable")
			},
			run: func(queueClient *observingSandboxFinalizerQueue, store *recordingBackgroundCommandStore, registry *ProviderRegistry) error {
				return (&SandboxBackgroundReconcileJobRunner{Queue: queueClient, Store: store, Providers: registry,
					Config: SandboxBackgroundRunnerConfig{WorkspaceID: "ws_background", LeaseDuration: 500 * time.Millisecond, HeartbeatInterval: 10 * time.Millisecond}}).RunOnce(context.Background())
			},
			wantTransition: "dead:qjob_background_reconcile:sandbox_background_reconcile_exhausted",
		},
		{
			name: "command", job: sandboxBackgroundCommandQueueJob("stdin"),
			configureStore: func(store *recordingBackgroundCommandStore) {
				store.commandErr = errors.New("background command store unavailable")
			},
			run: func(queueClient *observingSandboxFinalizerQueue, store *recordingBackgroundCommandStore, registry *ProviderRegistry) error {
				return (&SandboxBackgroundCommandJobRunner{Queue: queueClient, Store: store, Providers: registry,
					Config: SandboxBackgroundRunnerConfig{WorkspaceID: "ws_background", LeaseDuration: 500 * time.Millisecond, HeartbeatInterval: 10 * time.Millisecond}}).RunOnce(context.Background())
			},
			wantTransition: "dead:qjob_background_command:sandbox_background_command_exhausted",
		},
	}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: &recordingBackgroundProviderAdapter{}})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.job.AttemptCount = tc.job.MaxAttempts
			finalizing := make(chan struct{})
			heartbeatObserved := make(chan struct{}, 1)
			queueClient := &observingSandboxFinalizerQueue{
				recordingSandboxQueue: recordingSandboxQueue{leased: []*queuev1.QueueJob{tc.job}},
				finalizing:            finalizing, heartbeatObserved: heartbeatObserved,
			}
			store := &recordingBackgroundCommandStore{finalizeStarted: finalizing, heartbeatObserved: heartbeatObserved}
			tc.configureStore(store)
			if err := tc.run(queueClient, store, registry); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if !reflect.DeepEqual(queueClient.transitions, []string{tc.wantTransition}) {
				t.Fatalf("transitions = %v; want %s after live finalizer", queueClient.transitions, tc.wantTransition)
			}
		})
	}
}

func TestSandboxBackgroundRunnersSettleBusinessStateBeforeDeadLetteringInvalidPayload(t *testing.T) {
	tests := []struct {
		name            string
		job             *queuev1.QueueJob
		run             func(*recordingSandboxQueue, *recordingBackgroundCommandStore, *ProviderRegistry) error
		wantStore       []string
		wantTransitions []string
	}{
		{
			name: "reconcile",
			job: func() *queuev1.QueueJob {
				job := sandboxBackgroundReconcileQueueJob()
				job.PayloadJson = `{}`
				return job
			}(),
			run: func(queueClient *recordingSandboxQueue, store *recordingBackgroundCommandStore, registry *ProviderRegistry) error {
				return (&SandboxBackgroundReconcileJobRunner{
					Queue: queueClient, Store: store, Providers: registry,
					Config: SandboxBackgroundRunnerConfig{WorkspaceID: "ws_background", LeaseDuration: 2 * time.Minute, HeartbeatInterval: 15 * time.Second},
				}).RunOnce(context.Background())
			},
			wantStore:       []string{"exhaust_reconcile"},
			wantTransitions: []string{"dead:qjob_background_reconcile:invalid_sandbox_background_reconcile_payload"},
		},
		{
			name: "command",
			job: func() *queuev1.QueueJob {
				job := sandboxBackgroundCommandQueueJob("stdin")
				job.PayloadJson = `{}`
				return job
			}(),
			run: func(queueClient *recordingSandboxQueue, store *recordingBackgroundCommandStore, registry *ProviderRegistry) error {
				return (&SandboxBackgroundCommandJobRunner{
					Queue: queueClient, Store: store, Providers: registry,
					Config: SandboxBackgroundRunnerConfig{WorkspaceID: "ws_background", LeaseDuration: 2 * time.Minute, HeartbeatInterval: 15 * time.Second},
				}).RunOnce(context.Background())
			},
			wantStore:       []string{"exhaust_command"},
			wantTransitions: []string{"dead:qjob_background_command:invalid_sandbox_background_command_payload"},
		},
	}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: &recordingBackgroundProviderAdapter{}})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{tc.job}}
			store := &recordingBackgroundCommandStore{}
			if err := tc.run(queueClient, store, registry); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if !reflect.DeepEqual(store.calls, tc.wantStore) {
				t.Fatalf("store calls = %v; want %v", store.calls, tc.wantStore)
			}
			if !reflect.DeepEqual(queueClient.transitions, tc.wantTransitions) {
				t.Fatalf("queue transitions = %v; want %v", queueClient.transitions, tc.wantTransitions)
			}
		})
	}
}

func sandboxBackgroundTaskWork() SandboxBackgroundTaskWork {
	return SandboxBackgroundTaskWork{
		WorkspaceID: "ws_background", SessionID: "sesn_background", SessionThreadID: "thr_background",
		TaskID: "task_background", Provider: sandboxdriver.DaytonaProviderName, ReconcileGeneration: 1,
		Reference: sandboxdriver.CommandReference{
			Target: sandboxdriver.ToolTarget{WorkspaceID: "ws_background", SessionID: "sesn_background", SessionThreadID: "thr_background", ProviderSandboxID: "provider_background"},
			Task:   sandboxdriver.BackgroundTask{TaskID: "task_background", SourceToolUseEventID: "evt_background", ProviderSessionID: "provider_background", ProviderCommandID: "command_background", ProviderCommandMetadataJSON: `{}`},
		},
	}
}

func sandboxBackgroundReconcileQueueJob() *queuev1.QueueJob {
	return &queuev1.QueueJob{
		Id: "qjob_background_reconcile", WorkspaceId: "ws_background", Kind: queue.KindSandboxBackgroundReconcile,
		PartitionKey: queue.FormatSandboxBackgroundPartitionKey("ws_background", "sesn_background", "task_background"),
		DedupeKey:    queue.FormatSandboxBackgroundReconcileDedupeKey("ws_background", "sesn_background", "task_background", 1),
		PayloadJson:  `{"workspace_id":"ws_background","session_id":"sesn_background","task_id":"task_background","reconcile_generation":1}`,
		LeaseToken:   "lease_background_reconcile", AttemptCount: 1, MaxAttempts: SandboxBackgroundReconcileMaxAttempts,
	}
}

func sandboxBackgroundCommandQueueJob(kind string) *queuev1.QueueJob {
	return &queuev1.QueueJob{
		Id: "qjob_background_command", WorkspaceId: "ws_background", Kind: queue.KindSandboxBackgroundCommand,
		PartitionKey: queue.FormatSandboxBackgroundPartitionKey("ws_background", "sesn_background", "task_background"),
		DedupeKey:    queue.FormatSandboxBackgroundCommandDedupeKey("ws_background", "sesn_background", "task_background", "evt_command"),
		PayloadJson:  `{"workspace_id":"ws_background","session_id":"sesn_background","task_id":"task_background","request_id":"evt_command"}`,
		LeaseToken:   "lease_background_command", AttemptCount: 1, MaxAttempts: SandboxBackgroundCommandMaxAttempts,
	}
}

type recordingBackgroundCommandStore struct {
	reconcileCurrent  bool
	commandCurrent    bool
	task              SandboxBackgroundTaskWork
	operation         SandboxBackgroundOperationWork
	calls             []string
	reconcileErr      error
	commandErr        error
	finalizeStarted   chan struct{}
	heartbeatObserved <-chan struct{}
}

func (s *recordingBackgroundCommandStore) LoadReconcile(context.Context, SandboxBackgroundReconcileJob) (SandboxBackgroundTaskWork, bool, error) {
	s.calls = append(s.calls, "load_reconcile")
	return s.task, s.reconcileCurrent, s.reconcileErr
}
func (s *recordingBackgroundCommandStore) AdvanceReconcile(context.Context, SandboxBackgroundTaskWork, sandboxdriver.CommandResult, time.Time, time.Time) error {
	s.calls = append(s.calls, "advance_reconcile")
	return nil
}
func (s *recordingBackgroundCommandStore) SettleTask(context.Context, SandboxBackgroundTaskWork, sandboxdriver.CommandResult, time.Time) error {
	s.calls = append(s.calls, "settle_task")
	return nil
}
func (s *recordingBackgroundCommandStore) FinalizeReconcileExhaustion(context.Context, *queuev1.QueueJob, time.Time) error {
	s.calls = append(s.calls, "exhaust_reconcile")
	if s.finalizeStarted != nil {
		return requireHeartbeatDuringFinalizer(s.finalizeStarted, s.heartbeatObserved)
	}
	return nil
}
func (s *recordingBackgroundCommandStore) LoadCommand(context.Context, SandboxBackgroundCommandJob, time.Time) (SandboxBackgroundOperationWork, bool, error) {
	s.calls = append(s.calls, "load_command")
	return s.operation, s.commandCurrent, s.commandErr
}
func (s *recordingBackgroundCommandStore) MarkCommandSubmitted(context.Context, SandboxBackgroundOperationWork, time.Time) (bool, error) {
	s.calls = append(s.calls, "submit_command")
	return true, nil
}
func (s *recordingBackgroundCommandStore) SettleCommand(_ context.Context, _ SandboxBackgroundOperationWork, disposition string, _ sandboxdriver.CommandResult, _ time.Time) error {
	s.calls = append(s.calls, "settle_command:"+disposition)
	return nil
}
func (s *recordingBackgroundCommandStore) FinalizeCommandExhaustion(context.Context, *queuev1.QueueJob, time.Time) error {
	s.calls = append(s.calls, "exhaust_command")
	if s.finalizeStarted != nil {
		return requireHeartbeatDuringFinalizer(s.finalizeStarted, s.heartbeatObserved)
	}
	return nil
}

type recordingBackgroundProviderAdapter struct {
	recordingProviderAdapter
	poll ProviderOutcome[sandboxdriver.CommandResult]
}

func (a *recordingBackgroundProviderAdapter) PollBackground(context.Context, sandboxdriver.CommandReference) ProviderOutcome[sandboxdriver.CommandResult] {
	a.calls = append(a.calls, "poll")
	return a.poll
}
func (a *recordingBackgroundProviderAdapter) SendBackgroundInput(context.Context, sandboxdriver.CommandInput) ProviderOutcome[sandboxdriver.CommandResult] {
	a.calls = append(a.calls, "stdin")
	return ProviderOutcome[sandboxdriver.CommandResult]{Value: sandboxdriver.CommandResult{ResultJSON: `{"status":"running"}`}}
}
func (a *recordingBackgroundProviderAdapter) CancelBackground(context.Context, sandboxdriver.CommandCancel) ProviderOutcome[sandboxdriver.CommandResult] {
	a.calls = append(a.calls, "cancel")
	return ProviderOutcome[sandboxdriver.CommandResult]{Value: sandboxdriver.CommandResult{ResultJSON: `{"status":"cancelled"}`, TerminalStatus: "cancelled"}}
}

var _ ProviderAdapter = (*recordingBackgroundProviderAdapter)(nil)
var _ BackgroundCommandAdapter = (*recordingBackgroundProviderAdapter)(nil)
var _ = sandbox.ProviderHandle{}
