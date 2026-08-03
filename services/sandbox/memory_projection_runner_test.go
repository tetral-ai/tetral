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
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

func TestSandboxMemoryProjectionRunnerNormalizesProviderState(t *testing.T) {
	tests := []struct {
		name      string
		readiness ProviderOutcome[ExecutionReadiness]
		refresh   ProviderOutcome[struct{}]
		noHandle  bool
		wantCalls []string
		wantState string
	}{
		{name: "ready refreshes", readiness: ProviderOutcome[ExecutionReadiness]{Value: ExecutionReady}, wantCalls: []string{"inspect", "refresh"}, wantState: "refreshed"},
		{name: "stopped skips cold", readiness: ProviderOutcome[ExecutionReadiness]{Value: ExecutionNeedsActivation}, wantCalls: []string{"inspect"}, wantState: "skipped_cold"},
		{name: "missing skips cold", readiness: ProviderOutcome[ExecutionReadiness]{Value: ExecutionNeedsCreation}, wantCalls: []string{"inspect"}, wantState: "skipped_cold"},
		{name: "detached binding skips without provider call", noHandle: true, wantState: "skipped_cold"},
		{name: "unknown refresh outcome settles failed", readiness: ProviderOutcome[ExecutionReadiness]{Value: ExecutionReady}, refresh: ProviderOutcome[struct{}]{EffectBoundary: ProviderOutcomeUnknown, Disposition: ProviderTerminal, ErrorKind: "projection_failed", SafeMessage: "projection failed"}, wantCalls: []string{"inspect", "refresh"}, wantState: "failed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			job := sandboxMemoryProjectionQueueJob()
			queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{job}}
			work := sandboxMemoryProjectionWork()
			if tc.noHandle {
				work.ProviderResourceID = ""
			}
			store := &recordingMemoryProjectionStore{current: true, work: work}
			adapter := &recordingMemoryProjectionAdapter{readiness: tc.readiness, refresh: tc.refresh}
			registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
			if err != nil {
				t.Fatalf("NewProviderRegistry: %v", err)
			}
			runner := &SandboxMemoryProjectionJobRunner{
				Queue: queueClient, Store: store, Providers: registry,
				Config: SandboxMemoryProjectionRunnerConfig{WorkspaceID: "ws_memory", LeaseDuration: 2 * time.Minute, HeartbeatInterval: 15 * time.Second},
			}
			if err := runner.RunOnce(context.Background()); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if !reflect.DeepEqual(adapter.calls, tc.wantCalls) {
				t.Fatalf("adapter calls = %v; want %v", adapter.calls, tc.wantCalls)
			}
			if !reflect.DeepEqual(store.calls, []string{"load", "settle:" + tc.wantState}) {
				t.Fatalf("store calls = %v; want load/settle:%s", store.calls, tc.wantState)
			}
			if !reflect.DeepEqual(queueClient.transitions, []string{"ack:qjob_memory_projection"}) {
				t.Fatalf("queue transitions = %v; want ack", queueClient.transitions)
			}
		})
	}
}

func TestSandboxMemoryProjectionRunnerKeepsHeartbeatThroughLiveExhaustion(t *testing.T) {
	job := sandboxMemoryProjectionQueueJob()
	job.AttemptCount = job.MaxAttempts
	finalizing := make(chan struct{})
	heartbeatObserved := make(chan struct{}, 1)
	queueClient := &observingSandboxFinalizerQueue{
		recordingSandboxQueue: recordingSandboxQueue{leased: []*queuev1.QueueJob{job}},
		finalizing:            finalizing, heartbeatObserved: heartbeatObserved,
	}
	store := &recordingMemoryProjectionStore{
		loadErr:         errors.New("memory projection store unavailable"),
		finalizeStarted: finalizing, heartbeatObserved: heartbeatObserved,
	}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: &recordingMemoryProjectionAdapter{}})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxMemoryProjectionJobRunner{
		Queue: queueClient, Store: store, Providers: registry,
		Config: SandboxMemoryProjectionRunnerConfig{WorkspaceID: "ws_memory", LeaseDuration: 500 * time.Millisecond, HeartbeatInterval: 10 * time.Millisecond},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"dead:qjob_memory_projection:sandbox_memory_projection_exhausted"}) {
		t.Fatalf("transitions = %v; want dead letter after live finalizer", queueClient.transitions)
	}
}

func TestSandboxMemoryProjectionRunnerAcknowledgesDetachedStoreOnFirstDelivery(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	now := time.Date(2026, 8, 2, 11, 0, 0, 0, time.UTC)
	const writeID = "evt_memory_projection_runner_detached"
	if _, err := adminDB.Exec(`INSERT INTO session_runtime_tool_results (
		workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
		normalized_input_hash, tool_name, input_json, ack_status, result_json,
		memory_projection_state, created_at, updated_at
	) VALUES (
		'ws_execution_store', 'sesn_execution_store', 'thr_execution_store', $1,
		'memory', 'hash_projection_runner_detached', 'memory',
		'{"action":"create","path":"note.md","content":"x"}', 'committed',
		'{"status":"completed","action":"create","path":"/note.md"}', 'pending', $2, $2
	)`, writeID, now); err != nil {
		t.Fatalf("seed detached memory projection: %v", err)
	}
	client := dbconnect.NewClientForTesting(runtimeDB)
	queueStore := queue.NewPostgreSQLStore(client)
	jobID := queue.NewJobID()
	if _, err := queueStore.Enqueue(context.Background(), queue.EnqueueRequest{
		ID: jobID, WorkspaceID: "ws_execution_store", Kind: queue.KindSandboxMemoryProjection,
		PartitionKey:   queue.FormatSandboxMemoryPartitionKey("ws_execution_store", "memstore_detached"),
		DedupeKey:      queue.FormatSandboxMemoryProjectionDedupeKey("ws_execution_store", "memstore_detached", writeID),
		PayloadVersion: 1,
		PayloadJSON:    []byte(`{"workspace_id":"ws_execution_store","session_id":"sesn_execution_store","memory_store_id":"memstore_detached","memory_write_id":"` + writeID + `"}`),
		MaxAttempts:    queue.SandboxMemoryProjectionMaxAttempts, Now: now,
	}); err != nil {
		t.Fatalf("enqueue detached projection: %v", err)
	}
	providers, err := NewProviderRegistry(map[string]ProviderAdapter{"daytona": &recordingMemoryProjectionAdapter{}})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxMemoryProjectionJobRunner{
		Queue:     sandboxProductionQueueClient(t, queueStore),
		Store:     NewPostgreSQLSandboxMemoryProjectionStore(client),
		Providers: providers,
		Config: SandboxMemoryProjectionRunnerConfig{
			WorkspaceID: "ws_execution_store", LeaseOwner: "memory-runner-test", MaxJobs: 1,
			LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second,
		},
		Clock: func() time.Time { return now.Add(time.Minute) },
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	var projectionState, queueStatus string
	var attempts int
	if err := adminDB.QueryRow(`SELECT memory_projection_state FROM session_runtime_tool_results
		WHERE workspace_id='ws_execution_store' AND tool_use_event_id=$1`, writeID).Scan(&projectionState); err != nil {
		t.Fatalf("read detached projection: %v", err)
	}
	if err := adminDB.QueryRow(`SELECT status, attempt_count FROM queue_jobs
		WHERE workspace_id='ws_execution_store' AND id=$1`, jobID).Scan(&queueStatus, &attempts); err != nil {
		t.Fatalf("read detached projection Queue row: %v", err)
	}
	if projectionState != "failed" || queueStatus != queue.StatusAcknowledged || attempts != 1 {
		t.Fatalf("detached projection = %q queue %q attempts %d; want failed/acknowledged/1", projectionState, queueStatus, attempts)
	}
}

func sandboxMemoryProjectionQueueJob() *queuev1.QueueJob {
	return &queuev1.QueueJob{
		Id: "qjob_memory_projection", WorkspaceId: "ws_memory", Kind: queue.KindSandboxMemoryProjection,
		PartitionKey: queue.FormatSandboxMemoryPartitionKey("ws_memory", "mem_store"),
		DedupeKey:    queue.FormatSandboxMemoryProjectionDedupeKey("ws_memory", "mem_store", "evt_memory"),
		PayloadJson:  `{"workspace_id":"ws_memory","session_id":"sesn_memory","memory_store_id":"mem_store","memory_write_id":"evt_memory"}`,
		LeaseToken:   "lease_memory", AttemptCount: 1, MaxAttempts: queue.SandboxMemoryProjectionMaxAttempts,
	}
}

func sandboxMemoryProjectionWork() SandboxMemoryProjectionWork {
	return SandboxMemoryProjectionWork{
		WorkspaceID: "ws_memory", SessionID: "sesn_memory", SessionThreadID: "thr_memory",
		MemoryStoreID: "mem_store", MemoryWriteID: "evt_memory", Provider: sandboxdriver.DaytonaProviderName,
		ProviderResourceID: "provider_memory", MountPaths: []string{"/memories"},
		Ops: []sandboxdriver.MemoryProjectionOp{{Kind: "upsert", RelativePath: "/note.md", Content: "hello", ContentSHA256: "digest"}},
	}
}

type recordingMemoryProjectionStore struct {
	current           bool
	work              SandboxMemoryProjectionWork
	calls             []string
	loadErr           error
	finalizeStarted   chan struct{}
	heartbeatObserved <-chan struct{}
}

func (s *recordingMemoryProjectionStore) LoadProjection(context.Context, SandboxMemoryProjectionJob) (SandboxMemoryProjectionWork, bool, error) {
	s.calls = append(s.calls, "load")
	return s.work, s.current, s.loadErr
}

func (s *recordingMemoryProjectionStore) SettleProjection(_ context.Context, _ SandboxMemoryProjectionWork, state string, _ string, _ time.Time) error {
	s.calls = append(s.calls, "settle:"+state)
	return nil
}

func (s *recordingMemoryProjectionStore) FinalizeProjectionExhaustion(context.Context, *queuev1.QueueJob, time.Time) error {
	s.calls = append(s.calls, "exhaust")
	if s.finalizeStarted != nil {
		return requireHeartbeatDuringFinalizer(s.finalizeStarted, s.heartbeatObserved)
	}
	return nil
}

type recordingMemoryProjectionAdapter struct {
	recordingProviderAdapter
	readiness ProviderOutcome[ExecutionReadiness]
	refresh   ProviderOutcome[struct{}]
}

func (a *recordingMemoryProjectionAdapter) InspectForExecution(context.Context, string) ProviderOutcome[ExecutionReadiness] {
	a.calls = append(a.calls, "inspect")
	return a.readiness
}
func (a *recordingMemoryProjectionAdapter) InspectForRelease(context.Context, string) ProviderOutcome[bool] {
	return ProviderOutcome[bool]{Value: true}
}

func (a *recordingMemoryProjectionAdapter) RefreshMemoryProjection(context.Context, sandboxdriver.MemoryProjectionRefresh) ProviderOutcome[struct{}] {
	a.calls = append(a.calls, "refresh")
	return a.refresh
}

var _ MemoryProjectionAdapter = (*recordingMemoryProjectionAdapter)(nil)
