package tetralsandbox

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/workspace"
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
	finalizing := make(chan struct{})
	heartbeatObserved := make(chan struct{}, 1)
	queueClient := &observingSandboxFinalizerQueue{
		recordingSandboxQueue: recordingSandboxQueue{leased: []*queuev1.QueueJob{job}},
		finalizing:            finalizing, heartbeatObserved: heartbeatObserved,
	}
	store := &recordingToolCancellationStore{finalizeStarted: finalizing, heartbeatObserved: heartbeatObserved}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{"daytona": &recordingCancellationAdapter{}})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxToolCancelJobRunner{
		Queue: queueClient, Store: store, Providers: registry,
		Config: SandboxLifecycleRunnerConfig{WorkspaceID: "ws_cancel", LeaseOwner: "sandbox-test", MaxJobs: 1, LeaseDuration: 500 * time.Millisecond, HeartbeatInterval: 10 * time.Millisecond},
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

func TestPostgreSQLSandboxToolCancellationRejectsSupersededResults(t *testing.T) {
	tests := []struct {
		name       string
		resultJSON string
		finalize   bool
		submit     bool
	}{
		{name: "success", resultJSON: `{"status":"cancelled"}`},
		{name: "unknown outcome"},
		{name: "provider submission", resultJSON: `{"status":"cancelled"}`, submit: true},
		{name: "attempt exhaustion", finalize: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtimeDB, adminDB := newSandboxServiceTestDB(t)
			seedSandboxExecutionStoreFixture(t, adminDB)
			now := time.Now().UTC()
			seedReadySandboxBinding(t, adminDB, now)
			encodedReference, err := encodeSandboxToolObservationReference(sandboxdriver.DaytonaProviderName, sandboxdriver.ForegroundCommandObservation{
				Reference: sandboxdriver.CommandReference{
					Target: sandboxdriver.ToolTarget{ProviderSandboxID: "provider_execution_store"},
					Task:   sandboxdriver.BackgroundTask{TaskID: "task_cancel", ProviderSessionID: "provider_execution_store", ProviderCommandID: "command_cancel"},
				},
			})
			if err != nil {
				t.Fatalf("encode command reference: %v", err)
			}
			if _, err := adminDB.Exec(`UPDATE session_runtime_tool_results
				SET execution_state='running', cancel_state='pending', cancel_requested_at=$2,
				    provider_command_reference_json=$1
				WHERE workspace_id='ws_execution_store' AND tool_use_event_id='evt_execution_a'`, encodedReference, now); err != nil {
				t.Fatalf("seed cancellation work: %v", err)
			}
			client := dbconnect.NewClientForTesting(runtimeDB)
			coordinator := NewPostgreSQLSandboxExecutionCoordinator(client, 30*time.Minute)
			var firstWork SandboxToolCancellationWork
			firstCtx, secondCtx, firstTransport, secondTransport := supersedeSandboxQueueLeaseAfter(t, runtimeDB, adminDB, queue.EnqueueRequest{
				ID: "qjob_cancel_fence", WorkspaceID: workspace.ID("ws_execution_store"), Kind: queue.KindSandboxToolCancel,
				PartitionKey:   queue.FormatSandboxCancelPartitionKey("ws_execution_store", "sesn_execution_store", "thr_execution_store", "evt_execution_a"),
				DedupeKey:      queue.FormatSandboxToolCancelDedupeKey("ws_execution_store", "sesn_execution_store", "thr_execution_store", "evt_execution_a"),
				PayloadVersion: 1,
				PayloadJSON:    []byte(`{"workspace_id":"ws_execution_store","session_id":"sesn_execution_store","session_thread_id":"thr_execution_store","tool_use_event_id":"evt_execution_a"}`),
				MaxAttempts:    5,
			}, func(ctx context.Context, transport *queuev1.QueueJob) {
				if test.finalize {
					return
				}
				job, err := DecodeSandboxToolCancelJob(transport)
				if err != nil {
					t.Fatalf("decode first cancellation job: %v", err)
				}
				var current bool
				firstWork, current, err = coordinator.ClaimToolCancellation(ctx, job, now)
				if err != nil || !current {
					t.Fatalf("first ClaimToolCancellation = current %t, %v", current, err)
				}
			})
			firstJob, err := DecodeSandboxToolCancelJob(firstTransport)
			if err != nil {
				t.Fatalf("decode first cancellation job: %v", err)
			}
			secondJob, err := DecodeSandboxToolCancelJob(secondTransport)
			if err != nil {
				t.Fatalf("decode successor cancellation job: %v", err)
			}
			if !test.finalize {
				secondWork, current, claimErr := coordinator.ClaimToolCancellation(secondCtx, secondJob, now)
				if claimErr != nil || !current {
					t.Fatalf("successor ClaimToolCancellation = current %t, %v", current, claimErr)
				}
				if test.submit {
					_, err = coordinator.MarkToolCancellationSubmitted(firstCtx, firstWork, now)
				} else {
					err = coordinator.SettleToolCancellation(firstCtx, firstWork, test.resultJSON, "cancelled", "sandbox execution was cancelled", now)
				}
				firstWork = secondWork
			} else {
				err = coordinator.FinalizeToolCancellation(firstCtx, firstJob, now)
			}
			if !errors.Is(err, errQueueLeaseLost) {
				t.Fatalf("superseded cancellation writer error = %v; want Queue authority loss", err)
			}
			var state string
			if err := adminDB.QueryRow(`SELECT execution_state FROM session_runtime_tool_results
				WHERE workspace_id='ws_execution_store' AND tool_use_event_id='evt_execution_a'`).Scan(&state); err != nil {
				t.Fatalf("read execution after stale cancellation: %v", err)
			}
			if state != "running" {
				t.Fatalf("execution after stale cancellation = %q; want running", state)
			}
			if !test.finalize {
				if test.submit {
					submitted, submitErr := coordinator.MarkToolCancellationSubmitted(secondCtx, firstWork, now)
					if submitErr != nil || !submitted {
						t.Fatalf("successor cancellation submission = %t, %v", submitted, submitErr)
					}
				}
				err = coordinator.SettleToolCancellation(secondCtx, firstWork, test.resultJSON, "cancelled", "sandbox execution was cancelled", now)
			} else {
				err = coordinator.FinalizeToolCancellation(secondCtx, secondJob, now)
			}
			if err != nil {
				t.Fatalf("successor cancellation writer: %v", err)
			}
			if err := adminDB.QueryRow(`SELECT execution_state FROM session_runtime_tool_results
				WHERE workspace_id='ws_execution_store' AND tool_use_event_id='evt_execution_a'`).Scan(&state); err != nil {
				t.Fatalf("read successor cancellation: %v", err)
			}
			if state != "terminal_unconsumed" {
				t.Fatalf("successor cancellation state = %q; want terminal_unconsumed", state)
			}
		})
	}
}

func TestPostgreSQLToolCancellationLocksLifecycleBeforeExecution(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	now := time.Now().UTC()
	seedReadySandboxBinding(t, adminDB, now)
	seedSandboxLifecycleLockRow(t, adminDB, now)
	encodedReference, err := encodeSandboxToolObservationReference(sandboxdriver.DaytonaProviderName, sandboxdriver.ForegroundCommandObservation{
		Reference: sandboxdriver.CommandReference{
			Target: sandboxdriver.ToolTarget{ProviderSandboxID: "provider_execution_store"},
			Task:   sandboxdriver.BackgroundTask{TaskID: "task_cancel", ProviderSessionID: "provider_execution_store", ProviderCommandID: "command_cancel"},
		},
	})
	if err != nil {
		t.Fatalf("encode command reference: %v", err)
	}
	if _, err := adminDB.Exec(`UPDATE session_runtime_tool_results
		SET execution_state='running', cancel_state='pending', cancel_requested_at=$2,
		    provider_command_reference_json=$1
		WHERE workspace_id='ws_execution_store' AND tool_use_event_id='evt_execution_a'`, encodedReference, now); err != nil {
		t.Fatalf("seed cancellation work: %v", err)
	}
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	job := SandboxToolCancelJob{WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store", SessionThreadID: "thr_execution_store", ToolUseEventID: "evt_execution_a"}
	work, current, err := coordinator.ClaimToolCancellation(ctx, job, now)
	if err != nil || !current {
		t.Fatalf("ClaimToolCancellation = current %t, %v", current, err)
	}
	blocker, err := adminDB.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin blocker: %v", err)
	}
	defer func() { _ = blocker.Rollback() }()
	var operationID string
	if err := blocker.QueryRow(`SELECT operation_id FROM sandbox_lifecycle_operations
		WHERE workspace_id='ws_execution_store' AND operation_id='sop_lock_order' FOR UPDATE`).Scan(&operationID); err != nil {
		t.Fatalf("lock lifecycle row: %v", err)
	}
	settled := make(chan error, 1)
	go func() {
		settled <- coordinator.SettleToolCancellation(ctx, work, `{"status":"cancelled"}`, "cancelled", "sandbox execution was cancelled", now)
	}()
	waitForSandboxLifecycleLockWait(t, adminDB)
	lockCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var eventID string
	if err := blocker.QueryRowContext(lockCtx, `SELECT tool_use_event_id FROM session_runtime_tool_results
		WHERE workspace_id='ws_execution_store' AND tool_use_event_id='evt_execution_a' FOR UPDATE`).Scan(&eventID); err != nil {
		t.Fatalf("lock execution while writer waits on lifecycle: %v", err)
	}
	if err := blocker.Rollback(); err != nil {
		t.Fatalf("release blocker: %v", err)
	}
	select {
	case err := <-settled:
		if err != nil {
			t.Fatalf("SettleToolCancellation after lock release: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SettleToolCancellation did not complete after lifecycle lock release")
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
	work              SandboxToolCancellationWork
	current           bool
	submitted         bool
	calls             []string
	resultJSON        string
	finalizeStarted   chan struct{}
	heartbeatObserved <-chan struct{}
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
	if s.finalizeStarted != nil {
		return requireHeartbeatDuringFinalizer(s.finalizeStarted, s.heartbeatObserved)
	}
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
