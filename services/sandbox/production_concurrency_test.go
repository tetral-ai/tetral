package tetralsandbox

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/workspace"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestSandboxProductionConsumerGroupSettlesIndependentSessionsWithinGlobalBound(t *testing.T) {
	const (
		workspaceID  = "ws_sandbox_concurrency"
		sessionCount = 100
		workerCount  = 4
	)
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxConcurrencyFixtures(t, adminDB, workspaceID, sessionCount)
	client := dbconnect.NewClientForTesting(runtimeDB)
	queueStore := queue.NewPostgreSQLStore(client)
	for index := range sessionCount {
		sessionID, threadID, eventID := sandboxConcurrencyIdentity(index)
		payload, err := sandboxExecutionQueuePayload(SandboxExecutionRef{
			WorkspaceID: workspaceID, SessionID: sessionID,
			SessionThreadID: threadID, ToolUseEventID: eventID,
		})
		if err != nil {
			t.Fatalf("encode execution %d Queue payload: %v", index, err)
		}
		if _, err := queueStore.Enqueue(context.Background(), queue.EnqueueRequest{
			ID: queue.NewJobID(), WorkspaceID: workspace.ID(workspaceID), Kind: queue.KindSandboxToolExecute,
			PartitionKey:   queue.FormatSandboxExecutionPartitionKey(workspace.ID(workspaceID), sessionID, threadID, eventID),
			DedupeKey:      queue.FormatSandboxToolExecuteDedupeKey(workspace.ID(workspaceID), sessionID, threadID, eventID, 1),
			PayloadVersion: 1, PayloadJSON: payload, MaxAttempts: SandboxToolExecuteMaxAttempts,
		}); err != nil {
			t.Fatalf("enqueue execution %d: %v", index, err)
		}
	}
	queueClient := sandboxProductionQueueClient(t, queueStore)
	provider := newConcurrentSandboxProvider("evt_concurrency_000")
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: provider})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(client, 30*time.Minute)
	pool, err := NewWorkspaceConsumerPool(workerCount)
	if err != nil {
		t.Fatalf("NewWorkspaceConsumerPool: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- RunSandboxToolExecutionConsumerGroup(
			ctx, workerCount, pool, sandboxStaticWorkspaceLister{workspace.ID(workspaceID)}, time.Millisecond,
			queueClient, coordinator, registry, passthroughSandboxMedia{}, SandboxToolExecutionRunnerConfig{
				LeaseOwner: ServiceName, MaxJobs: 1,
				LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second,
				PreparationTimeout: 45 * time.Second,
			},
			nil,
			nil,
		)
	}()
	select {
	case <-provider.blockedStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("blocked Session did not reach the provider")
	}
	waitForSandboxTerminalCount(t, adminDB, workspaceID, 10)
	close(provider.releaseBlocked)
	waitForSandboxTerminalCount(t, adminDB, workspaceID, sessionCount)
	waitForSandboxAcknowledgedCount(t, adminDB, workspaceID, sessionCount)
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("RunWorkspaceConsumerGroup error = %v; want context cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("production consumer group did not stop")
	}
	if got := provider.maximum.Load(); got <= 1 || got > workerCount {
		t.Fatalf("provider maximum concurrency = %d; want greater than 1 and at most %d", got, workerCount)
	}
}

func sandboxProductionQueueClient(t *testing.T, store *queue.PostgreSQLQueueStore) SandboxQueueClient {
	t.Helper()
	listener := bufconn.Listen(2 * 1024 * 1024)
	server := grpc.NewServer()
	queuev1.RegisterQueueServiceServer(server, tetralqueue.NewServer(store, nil))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	connection, err := grpc.NewClient("passthrough:///sandbox-production-queue",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
	)
	if err != nil {
		t.Fatalf("dial production Queue: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return SandboxQueueFromGRPC(queuev1.NewQueueServiceClient(connection))
}

func seedSandboxConcurrencyFixtures(t *testing.T, db *sql.DB, workspaceID string, count int) {
	t.Helper()
	const environmentID = "env_sandbox_concurrency"
	seedEnvironmentArtifactStoreEnvironment(t, db, workspaceID, environmentID)
	if _, err := db.Exec(`UPDATE environments SET current_generation=1 WHERE workspace_id=$1 AND id=$2`, workspaceID, environmentID); err != nil {
		t.Fatalf("set concurrency environment generation: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO environment_artifacts (
		workspace_id, environment_id, generation, status, provider, provider_artifact_ref,
		normalized_config_hash, artifact_input_hash, runtime_network_policy_json, packages_json,
		created_at, updated_at
	) VALUES ($1, $2, 1, 'ready', 'daytona', 'artifact_concurrency',
		'config_hash', 'artifact_hash', '{"type":"unrestricted"}', '{}', clock_timestamp(), clock_timestamp())`, workspaceID, environmentID); err != nil {
		t.Fatalf("seed concurrency environment artifact: %v", err)
	}
	for index := range count {
		sessionID, threadID, eventID := sandboxConcurrencyIdentity(index)
		seedEnvironmentArtifactStoreSession(t, db, workspaceID, sessionID, environmentID)
		if _, err := db.Exec(`INSERT INTO session_threads (
			workspace_id, session_id, id, role, status, visibility, created_at, last_active_at, updated_at
		) VALUES ($1, $2, $3, 'main', 'idle', 'internal', clock_timestamp(), clock_timestamp(), clock_timestamp())`, workspaceID, sessionID, threadID); err != nil {
			t.Fatalf("seed concurrency thread %d: %v", index, err)
		}
		if _, err := db.Exec(`INSERT INTO session_sandbox_bindings (
			workspace_id, session_id, logical_sandbox_id, environment_id, environment_generation,
			provider, provider_resource_id, binding_revision, materialized_resource_revision,
			resource_credential_expires_at, resource_roots_json, provider_metadata_json,
			helper_verified_at, created_at, updated_at
		) VALUES ($1, $2, $3, $4, 1, 'daytona', $5, 1, 1,
			clock_timestamp()+interval '2 hours', '[]', '{}', clock_timestamp(), clock_timestamp(), clock_timestamp())`,
			workspaceID, sessionID, fmt.Sprintf("sbox_concurrency_%03d", index), environmentID, fmt.Sprintf("provider_concurrency_%03d", index)); err != nil {
			t.Fatalf("seed concurrency binding %d: %v", index, err)
		}
		if _, err := db.Exec(`INSERT INTO session_runtime_tool_results (
			workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
			normalized_input_hash, tool_name, input_json, ack_status, model_tool_call_id,
			execution_state, execution_attempt_generation, created_at, updated_at
		) VALUES ($1, $2, $3, $4, 'sandbox_tool', $4 || '_hash', 'bash', '{"command":"true"}',
			'committed', $4 || '_call', 'pending', 1, clock_timestamp(), clock_timestamp())`, workspaceID, sessionID, threadID, eventID); err != nil {
			t.Fatalf("seed concurrency execution %d: %v", index, err)
		}
	}
}

func sandboxConcurrencyIdentity(index int) (sessionID string, threadID string, eventID string) {
	return fmt.Sprintf("sesn_concurrency_%03d", index), fmt.Sprintf("thr_concurrency_%03d", index), fmt.Sprintf("evt_concurrency_%03d", index)
}

func waitForSandboxTerminalCount(t *testing.T, db *sql.DB, workspaceID string, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var count int
		if err := db.QueryRow(`SELECT count(*) FROM session_runtime_tool_results
			WHERE workspace_id=$1 AND execution_state='terminal_unconsumed'`, workspaceID).Scan(&count); err != nil {
			t.Fatalf("count terminal Sandbox executions: %v", err)
		}
		if count >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("terminal Sandbox executions = %d; want at least %d", count, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForSandboxAcknowledgedCount(t *testing.T, db *sql.DB, workspaceID string, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var count int
		if err := db.QueryRow(`SELECT count(*) FROM queue_jobs
			WHERE workspace_id=$1 AND kind=$2 AND status='acknowledged'`, workspaceID, queue.KindSandboxToolExecute).Scan(&count); err != nil {
			t.Fatalf("count acknowledged Sandbox executions: %v", err)
		}
		if count >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("acknowledged Sandbox executions = %d; want at least %d", count, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type concurrentSandboxProvider struct {
	*recordingProviderAdapter
	blockedEvent   string
	blockedStarted chan struct{}
	releaseBlocked chan struct{}
	blockedOnce    sync.Once
	active         atomic.Int32
	maximum        atomic.Int32
}

func newConcurrentSandboxProvider(blockedEvent string) *concurrentSandboxProvider {
	return &concurrentSandboxProvider{
		recordingProviderAdapter: &recordingProviderAdapter{},
		blockedEvent:             blockedEvent, blockedStarted: make(chan struct{}), releaseBlocked: make(chan struct{}),
	}
}

func (p *concurrentSandboxProvider) InspectForExecution(context.Context, string) ProviderOutcome[ExecutionReadiness] {
	return ProviderOutcome[ExecutionReadiness]{Value: ExecutionReady}
}

func (p *concurrentSandboxProvider) PrepareTool(context.Context, ToolExecutionRequest) ProviderOutcome[ToolPreparationResult] {
	return ProviderOutcome[ToolPreparationResult]{Value: ToolPreparationResult{}}
}

func (p *concurrentSandboxProvider) ExecuteTool(ctx context.Context, request ToolExecutionRequest) ProviderOutcome[sandboxdriver.ToolExecution] {
	current := p.active.Add(1)
	defer p.active.Add(-1)
	for {
		observed := p.maximum.Load()
		if current <= observed || p.maximum.CompareAndSwap(observed, current) {
			break
		}
	}
	if request.Invocation.ToolUseEventID == p.blockedEvent {
		p.blockedOnce.Do(func() { close(p.blockedStarted) })
		select {
		case <-p.releaseBlocked:
		case <-ctx.Done():
			return ProviderOutcome[sandboxdriver.ToolExecution]{
				EffectBoundary: ProviderOutcomeUnknown, Disposition: ProviderTerminal,
				ErrorKind: "provider_cancelled", SafeMessage: "provider execution was cancelled",
			}
		}
	} else {
		time.Sleep(5 * time.Millisecond)
	}
	return ProviderOutcome[sandboxdriver.ToolExecution]{Value: sandboxdriver.ToolExecution{
		ResultJSON: `{"status":"success","result":{"text":"ok"}}`,
	}}
}

var _ ProviderAdapter = (*concurrentSandboxProvider)(nil)
