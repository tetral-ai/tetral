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
			queueClient := &recordingSessionPrepareQueue{leased: []*queuev1.QueueJob{job}}
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
	current bool
	work    SandboxMemoryProjectionWork
	calls   []string
}

func (s *recordingMemoryProjectionStore) LoadProjection(context.Context, SandboxMemoryProjectionJob) (SandboxMemoryProjectionWork, bool, error) {
	s.calls = append(s.calls, "load")
	return s.work, s.current, nil
}

func (s *recordingMemoryProjectionStore) SettleProjection(_ context.Context, _ SandboxMemoryProjectionWork, state string, _ string, _ time.Time) error {
	s.calls = append(s.calls, "settle:"+state)
	return nil
}

func (s *recordingMemoryProjectionStore) FinalizeProjectionExhaustion(context.Context, *queuev1.QueueJob, time.Time) error {
	s.calls = append(s.calls, "exhaust")
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

func (a *recordingMemoryProjectionAdapter) RefreshMemoryProjection(context.Context, sandboxdriver.MemoryProjectionRefresh) ProviderOutcome[struct{}] {
	a.calls = append(a.calls, "refresh")
	return a.refresh
}

var _ MemoryProjectionAdapter = (*recordingMemoryProjectionAdapter)(nil)
