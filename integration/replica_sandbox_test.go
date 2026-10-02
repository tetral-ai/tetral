package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	driver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
	sandbox "github.com/tetral-ai/tetral/services/sandbox"
)

type sandboxReplicaLedger struct {
	mu       sync.Mutex
	submits  map[string]int
	observes map[string][]string
}
type sandboxReplicaProvider struct {
	*bridgeMemoryProjectionProvider
	ledger          *sandboxReplicaLedger
	entered         chan string
	release         <-chan struct{}
	beforeReference bool
}

func (p *sandboxReplicaProvider) ExecuteTool(ctx context.Context, r sandbox.ToolExecutionRequest) sandbox.ProviderOutcome[driver.ToolExecution] {
	id := r.Invocation.ToolUseEventID
	p.ledger.mu.Lock()
	p.ledger.submits[id]++
	p.ledger.mu.Unlock()
	observation := &driver.ForegroundCommandObservation{Reference: driver.CommandReference{Target: r.Invocation.Target, ToolUseEventID: id, Task: driver.BackgroundTask{TaskID: "task_" + id, ProviderSessionID: r.Handle.SandboxID, ProviderCommandID: "command_" + id}}}
	if p.beforeReference {
		p.entered <- id
		select {
		case <-ctx.Done():
		case <-p.release:
		}
	}
	return sandbox.ProviderOutcome[driver.ToolExecution]{Value: driver.ToolExecution{ForegroundObservation: observation}}
}
func (p *sandboxReplicaProvider) ObserveTool(ctx context.Context, r driver.ForegroundCommandObservation) sandbox.ProviderOutcome[driver.ToolExecution] {
	p.ledger.mu.Lock()
	p.ledger.observes[r.Reference.ToolUseEventID] = append(p.ledger.observes[r.Reference.ToolUseEventID], r.Reference.Task.ProviderCommandID)
	p.ledger.mu.Unlock()
	p.entered <- r.Reference.ToolUseEventID
	select {
	case <-ctx.Done():
		return sandbox.ProviderOutcome[driver.ToolExecution]{EffectBoundary: sandbox.ProviderOutcomeUnknown, Disposition: sandbox.ProviderRetryable, ErrorKind: "fixture_observation_interrupted"}
	case <-p.release:
	}
	return sandbox.ProviderOutcome[driver.ToolExecution]{Value: driver.ToolExecution{ResultJSON: `{"status":"completed","stdout":{"text":"original command","truncated":false},"stderr":{"text":"","truncated":false}}`}}
}

// Both instances run actual production consumers with independent database pools
// and reusable TCP Queue clients. Only external provider timing is controlled.
func TestPostgreSQLReplicaSandboxTakeover(t *testing.T) {
	for _, beforeReference := range []bool{false, true} {
		name := "stored command reference"
		if beforeReference {
			name = "submission before reference persistence"
		}
		t.Run(name, func(t *testing.T) {
			runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			bridgeStore := bridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
			ids := make([]string, 6)
			sessions := make([]string, 6)
			for i := range ids {
				session := fmt.Sprintf("sesn_sandbox_replica_%d", i)
				thread := fmt.Sprintf("thr_sandbox_replica_%d", i)
				binding := fmt.Sprintf("binding_sandbox_replica_%d", i)
				pod := fmt.Sprintf("pod_sandbox_replica_%d", i)
				sessions[i] = session
				seedBridgeAPISession(t, admin, "default", session, thread)
				seedBridgeAPIRuntimeBinding(t, admin, "default", session, binding, 1, pod)
				seedReadySandboxForSharedToolExecution(t, admin, "default", session)
				scope := bridgeAPIScope(session, thread, binding, 1, pod)
				ids[i] = writeDurableOrdinaryToolUseForTest(t, bridgeStore, scope, "request_"+session, "call_"+session, "Read", `{"file_path":"/workspace/input.txt"}`)
				if ack, err := bridgeStore.AcceptSandboxExecution(context.Background(), &bridgev1.AcceptSandboxExecutionRequest{Scope: scope, ToolUseEventId: ids[i]}); err != nil || ack.GetCommitted() == nil {
					t.Fatalf("durable execution admission=%v/%v", ack, err)
				}
			}
			ctx, stop := context.WithTimeout(context.Background(), 15*time.Second)
			defer stop()
			ledger := &sandboxReplicaLedger{submits: map[string]int{}, observes: map[string][]string{}}
			var pools [2]*dbconnect.Client
			var stores [2]*queue.PostgreSQLQueueStore
			var stops [2]context.CancelFunc
			var done [2]chan struct{}
			var cancelWorkByInstance [2]context.CancelFunc
			var providers [2]*sandboxReplicaProvider
			var releases [2]chan struct{}
			for i := range pools {
				pool := storagetest.OpenRuntimeRoleDBWithTracer(t, runtimeDB, nil)
				pools[i] = dbconnect.NewClientForTesting(pool)
				stores[i] = queue.NewPostgreSQLStore(pools[i])
				q := sandbox.WithQueueAcquisition(sandbox.SandboxQueueFromGRPC(serveQueueReplica(t, stores[i], &queueReplicaResponseFault{})))
				acquire, quiesce := context.WithCancel(ctx)
				defer quiesce()
				stops[i] = quiesce
				work, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
				work = sandbox.WithAcquisitionContext(work, acquire)
				done[i] = make(chan struct{})
				cancelWorkByInstance[i] = cancelWork
				releases[i] = make(chan struct{})
				providers[i] = &sandboxReplicaProvider{bridgeMemoryProjectionProvider: &bridgeMemoryProjectionProvider{}, ledger: ledger, entered: make(chan string, 20), release: releases[i], beforeReference: i == 0 && beforeReference}
				registry, err := sandbox.NewProviderRegistry(map[string]sandbox.ProviderAdapter{driver.DaytonaProviderName: providers[i]})
				if err != nil {
					t.Fatal(err)
				}
				workers, _ := sandbox.NewWorkspaceConsumerPool(1)
				index := i
				t.Cleanup(func() {
					stops[index]()
					cancelWork()
					select {
					case <-done[index]:
					case <-time.After(5 * time.Second):
						t.Error("Sandbox instance did not join")
					}
				})
				go func() {
					defer close(done[index])
					_ = sandbox.RunSandboxToolExecutionConsumerGroup(work, 2, workers, sandboxReplicaWorkspaceLister{}, 10*time.Millisecond, q, sandbox.NewPostgreSQLSandboxExecutionCoordinator(pools[index], 30*time.Minute), registry, backgroundNotificationMedia{}, sandbox.SandboxToolExecutionRunnerConfig{LeaseOwner: fmt.Sprintf("sandbox-replica-%d", index), MaxJobs: 1, LeaseDuration: 3 * time.Second, HeartbeatInterval: 200 * time.Millisecond, PreparationTimeout: time.Second}, nil, nil)
				}()
				if i == 0 {
					select {
					case id := <-providers[0].entered:
						ids[0] = id
					case <-ctx.Done():
						t.Fatal("first worker did not reach provider effect")
					}
				}
			}
			var secondID string
			select {
			case secondID = <-providers[1].entered:
			case <-ctx.Done():
				t.Fatal("second independent worker did not acquire work")
			}
			if secondID == ids[0] {
				t.Fatal("two workers acquired same execution")
			}
			var originalReference string
			waitHandoffCondition(t, "first worker durable running effect", func() bool {
				var state string
				if err := admin.QueryRowContext(ctx, `SELECT execution_state,COALESCE(provider_command_reference_json,'') FROM session_runtime_tool_results WHERE tool_use_event_id=$1`, ids[0]).Scan(&state, &originalReference); err != nil {
					t.Fatal(err)
				}
				return state == "running" && (beforeReference || originalReference != "")
			})
			if beforeReference && originalReference != "" {
				t.Fatal("pre-reference barrier persisted a command reference")
			}
			var sandboxIdentityBefore string
			if err := admin.QueryRowContext(ctx, `SELECT jsonb_agg(jsonb_build_array(session_id,logical_sandbox_id,provider,provider_resource_id,binding_revision) ORDER BY session_id)::text FROM session_sandbox_bindings`).Scan(&sandboxIdentityBefore); err != nil {
				t.Fatal(err)
			}
			var jobID, oldToken string
			var leasedUntilBefore time.Time
			if err := admin.QueryRowContext(ctx, `SELECT id,lease_token,leased_until FROM queue_jobs WHERE kind=$1 AND payload_json::jsonb->>'tool_use_event_id'=$2 AND status='leased'`, queue.KindSandboxToolExecute, ids[0]).Scan(&jobID, &oldToken, &leasedUntilBefore); err != nil {
				t.Fatal(err)
			}
			// Quiesce then cancel the worker at its configured cutoff. Its external
			// command remains durable; process cancellation does not release the Sandbox.
			stops[0]()
			drained := make(chan error, 1)
			go func() {
				drained <- sandbox.JoinSandboxWorkers(done[0], cancelWorkByInstance[0], 600*time.Millisecond, time.Second)
			}()
			waitHandoffCondition(t, "in-flight heartbeat survives Sandbox acquisition stop", func() bool {
				var leasedUntil time.Time
				var token string
				if err := admin.QueryRowContext(ctx, `SELECT leased_until,lease_token FROM queue_jobs WHERE id=$1`, jobID).Scan(&leasedUntil, &token); err != nil {
					t.Fatal(err)
				}
				if token != oldToken {
					t.Fatal("quiesce changed in-flight lease capability")
				}
				return leasedUntil.After(leasedUntilBefore)
			})
			if err := <-drained; err != nil {
				t.Fatal(err)
			}
			awaitReplicaQueueExpiry(ctx, t, admin, jobID)
			close(releases[1])
			if n, err := stores[1].ReclaimExpiredLeases(ctx, queue.ReclaimExpiredLeasesRequest{Limit: 20}); err != nil || n != 1 {
				t.Fatalf("expired worker lease reclamation=%d/%v", n, err)
			}
			waitHandoffCondition(t, "replacement settles every admitted execution", func() bool {
				var count int
				if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM queue_jobs WHERE kind=$1 AND status='acknowledged'`, queue.KindSandboxToolExecute).Scan(&count); err != nil {
					t.Fatal(err)
				}
				return count == 6
			})
			stops[1]()
			select {
			case <-done[1]:
			case <-ctx.Done():
				t.Fatal("second instance did not join")
			}
			ledger.mu.Lock()
			if len(ledger.submits) != 6 {
				t.Errorf("external ledger contains%d identities; want6", len(ledger.submits))
			}
			for id, count := range ledger.submits {
				if count != 1 {
					t.Errorf("external submission %s repeated %d times", id, count)
				}
			}
			for _, command := range ledger.observes[ids[0]] {
				if command != "command_"+ids[0] {
					t.Errorf("replacement observed different command %q", command)
				}
			}
			if !beforeReference && len(ledger.observes[ids[0]]) < 2 {
				t.Error("replacement did not observe stored original command")
			}
			ledger.mu.Unlock()
			var state, result string
			if err := admin.QueryRowContext(ctx, `SELECT execution_state,result_json FROM session_runtime_tool_results WHERE tool_use_event_id=$1`, ids[0]).Scan(&state, &result); err != nil {
				t.Fatal(err)
			}
			if state != "terminal_unconsumed" || !json.Valid([]byte(result)) {
				t.Fatalf("replacement terminal=%s/%s", state, result)
			}
			if beforeReference && !containsSandboxUnknownResult(result) {
				t.Fatalf("ambiguous submission did not preserve unknown outcome: %s", result)
			}
			var sandboxIdentityAfter string
			if err := admin.QueryRowContext(ctx, `SELECT jsonb_agg(jsonb_build_array(session_id,logical_sandbox_id,provider,provider_resource_id,binding_revision) ORDER BY session_id)::text FROM session_sandbox_bindings`).Scan(&sandboxIdentityAfter); err != nil || sandboxIdentityAfter != sandboxIdentityBefore {
				t.Fatalf("takeover replaced Sandbox identities=%v/%v", sandboxIdentityBefore == sandboxIdentityAfter, err)
			}
			var bindings, releaseRequested int
			if err := admin.QueryRowContext(ctx, `SELECT count(*),count(release_requested_at) FROM session_sandbox_bindings`).Scan(&bindings, &releaseRequested); err != nil || bindings != 6 || releaseRequested != 0 {
				t.Fatalf("worker loss changed Sandbox bindings=%d/%d/%v", bindings, releaseRequested, err)
			}
			stale, err := sandbox.SandboxQueueFromGRPC(serveQueueReplica(t, stores[0], &queueReplicaResponseFault{})).Ack(ctx, &queuev1.AckRequest{WorkspaceId: "default", JobId: jobID, LeaseToken: oldToken})
			if err != nil || stale.GetUpdated() {
				t.Fatalf("stale worker ACK=%v/%v", stale, err)
			}
		})
	}
}

type sandboxReplicaWorkspaceLister struct{}

func (sandboxReplicaWorkspaceLister) ListIDs(context.Context) ([]workspace.ID, error) {
	return []workspace.ID{workspace.DefaultID}, nil
}
func containsSandboxUnknownResult(result string) bool {
	var value map[string]any
	_ = json.Unmarshal([]byte(result), &value)
	return value["status"] == "unknown_outcome" || strings.Contains(result, "sandbox_execution_outcome_unknown")
}
