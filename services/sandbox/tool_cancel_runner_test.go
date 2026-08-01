package tetralsandbox

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/queue"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

func TestSandboxToolCancelRunnerSubmitsProviderCancelOnceAndSettles(t *testing.T) {
	job := sandboxToolCancelQueueJob()
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{job}}
	store := &recordingToolCancellationStore{current: true, submitted: true, work: SandboxToolCancellationWork{
		Job: SandboxToolCancelJob{
			QueueJob: job, JobID: job.GetId(), LeaseToken: job.GetLeaseToken(), WorkspaceID: "ws_cancel",
			SessionID: "sesn_cancel", SessionThreadID: "sthr_cancel", ToolUseEventID: "evt_cancel",
		},
		AttemptGeneration: 1, Provider: "daytona",
		Reference: sandboxdriver.CommandReference{
			Target: sandboxdriver.ToolTarget{ProviderSandboxID: "sandbox_provider"},
			Task:   sandboxdriver.BackgroundTask{TaskID: "task_cancel", ProviderCommandID: "command_cancel"},
		},
	}}
	adapter := &recordingCancellationAdapter{result: ProviderOutcome[sandboxdriver.CommandResult]{Value: sandboxdriver.CommandResult{
		TerminalStatus: "cancelled", ResultJSON: `{"status":"success","result":{"cancelled":true}}`,
	}}}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{"daytona": adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxToolCancelJobRunner{
		Queue: queueClient, Store: store, Providers: registry,
		Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_cancel", LeaseOwner: "sandbox-test", MaxJobs: 1, LeaseDuration: 10 * time.Second, HeartbeatInterval: time.Second},
	}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(store.calls, []string{"claim", "submit", "settle:cancelled"}) {
		t.Fatalf("store calls = %v", store.calls)
	}
	if store.resultJSON != `{"status":"cancelled"}` {
		t.Fatalf("stored cancellation result = %q; want normalized cancelled status", store.resultJSON)
	}
	if adapter.cancelCalls != 1 {
		t.Fatalf("provider cancel calls = %d; want 1", adapter.cancelCalls)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"ack:qjob_cancel"}) {
		t.Fatalf("queue transitions = %v", queueClient.transitions)
	}
}

func TestSandboxToolCancelRunnerSettlesExecutionBeforeDeadLetteringPoisonedPayload(t *testing.T) {
	job := sandboxToolCancelQueueJob()
	job.PayloadJson = `{"poison":`
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{job}}
	store := &recordingToolCancellationStore{}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{"daytona": &recordingCancellationAdapter{}})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxToolCancelJobRunner{
		Queue: queueClient, Store: store, Providers: registry,
		Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_cancel", LeaseOwner: "sandbox-test", MaxJobs: 1, LeaseDuration: 10 * time.Second, HeartbeatInterval: time.Second},
	}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(store.calls, []string{"finalize"}) {
		t.Fatalf("store calls = %v; want business finalization", store.calls)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"dead:qjob_cancel:invalid_sandbox_tool_cancel_payload"}) {
		t.Fatalf("queue transitions = %v; want dead-letter after finalization", queueClient.transitions)
	}
}

func TestSandboxToolCancelRunnerChecksExhaustionBeforePoisonedPayload(t *testing.T) {
	job := sandboxToolCancelQueueJob()
	job.AttemptCount = job.MaxAttempts + 1
	job.PayloadJson = `{"poison":`
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{job}}
	store := &recordingToolCancellationStore{}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{"daytona": &recordingCancellationAdapter{}})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxToolCancelJobRunner{
		Queue: queueClient, Store: store, Providers: registry,
		Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_cancel", LeaseOwner: "sandbox-test", MaxJobs: 1, LeaseDuration: 10 * time.Second, HeartbeatInterval: time.Second},
	}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(store.calls, []string{"finalize"}) {
		t.Fatalf("store calls = %v; want exhaustion finalization before payload decode", store.calls)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"dead:qjob_cancel:sandbox_tool_cancel_attempts_exhausted"}) {
		t.Fatalf("queue transitions = %v; want exhausted dead letter", queueClient.transitions)
	}
}

func sandboxToolCancelQueueJob() *queuev1.QueueJob {
	return &queuev1.QueueJob{
		Id: "qjob_cancel", WorkspaceId: "ws_cancel", Kind: queue.KindSandboxToolCancel,
		PartitionKey: queue.FormatSandboxCancelPartitionKey("ws_cancel", "sesn_cancel", "sthr_cancel", "evt_cancel"),
		DedupeKey:    queue.FormatSandboxToolCancelDedupeKey("ws_cancel", "sesn_cancel", "sthr_cancel", "evt_cancel"),
		PayloadJson:  `{"workspace_id":"ws_cancel","session_id":"sesn_cancel","session_thread_id":"sthr_cancel","tool_use_event_id":"evt_cancel"}`,
		LeaseToken:   "lease_cancel", AttemptCount: 1, MaxAttempts: 5,
	}
}

type recordingToolCancellationStore struct {
	work       SandboxToolCancellationWork
	current    bool
	submitted  bool
	calls      []string
	resultJSON string
}

func (s *recordingToolCancellationStore) ClaimToolCancellation(context.Context, SandboxToolCancelJob, time.Time) (SandboxToolCancellationWork, bool, error) {
	s.calls = append(s.calls, "claim")
	return s.work, s.current, nil
}

func (s *recordingToolCancellationStore) MarkToolCancellationSubmitted(context.Context, SandboxToolCancellationWork, time.Time) (bool, error) {
	s.calls = append(s.calls, "submit")
	return s.submitted, nil
}

func (s *recordingToolCancellationStore) SettleToolCancellation(_ context.Context, _ SandboxToolCancellationWork, resultJSON string, kind string, _ string, _ time.Time) error {
	s.calls = append(s.calls, "settle:"+kind)
	s.resultJSON = resultJSON
	return nil
}

func (s *recordingToolCancellationStore) FinalizeToolCancellation(context.Context, SandboxToolCancelJob, time.Time) error {
	s.calls = append(s.calls, "finalize")
	return nil
}

type recordingCancellationAdapter struct {
	recordingProviderAdapter
	result      ProviderOutcome[sandboxdriver.CommandResult]
	cancelCalls int
}

func (a *recordingCancellationAdapter) CancelBackground(context.Context, sandboxdriver.CommandCancel) ProviderOutcome[sandboxdriver.CommandResult] {
	a.cancelCalls++
	return a.result
}
