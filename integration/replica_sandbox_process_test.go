package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	driver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
	sandbox "github.com/tetral-ai/tetral/services/sandbox"
)

type sandboxProcessHTTPProvider struct {
	*bridgeMemoryProjectionProvider
	address string
}

func processProviderCall[T any](ctx context.Context, address string, value any) sandbox.ProviderOutcome[T] {
	body, err := json.Marshal(value)
	if err != nil {
		return sandbox.ProviderOutcome[T]{ErrorKind: "fixture_transport_failure", SafeMessage: "fixture transport failed", Disposition: sandbox.ProviderRetryable}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, address, bytes.NewReader(body))
	if err != nil {
		return sandbox.ProviderOutcome[T]{ErrorKind: "fixture_transport_failure", SafeMessage: "fixture transport failed", Disposition: sandbox.ProviderRetryable}
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return sandbox.ProviderOutcome[T]{ErrorKind: "fixture_transport_failure", SafeMessage: "fixture transport failed", Disposition: sandbox.ProviderRetryable, EffectBoundary: sandbox.ProviderOutcomeUnknown}
	}
	defer func() { _ = response.Body.Close() }()
	var result sandbox.ProviderOutcome[T]
	if err = json.NewDecoder(response.Body).Decode(&result); err != nil {
		return sandbox.ProviderOutcome[T]{ErrorKind: "fixture_transport_failure", SafeMessage: "fixture transport failed", Disposition: sandbox.ProviderRetryable}
	}
	return result
}
func (p *sandboxProcessHTTPProvider) ExecuteTool(ctx context.Context, r sandbox.ToolExecutionRequest) sandbox.ProviderOutcome[driver.ToolExecution] {
	return processProviderCall[driver.ToolExecution](ctx, p.address+"/execute", r)
}
func (p *sandboxProcessHTTPProvider) ObserveTool(ctx context.Context, r driver.ForegroundCommandObservation) sandbox.ProviderOutcome[driver.ToolExecution] {
	return processProviderCall[driver.ToolExecution](ctx, p.address+"/observe", r)
}

type sandboxKilledCapabilityQueue struct {
	sandbox.SandboxQueueClient
	job *queuev1.QueueJob
}

func (q sandboxKilledCapabilityQueue) Lease(context.Context, *queuev1.LeaseRequest) (*queuev1.LeaseResponse, error) {
	return &queuev1.LeaseResponse{Jobs: []*queuev1.QueueJob{q.job}}, nil
}

// The selected consumer is killed as an operating-system process, after the
// external provider accepted the command. Only the provider HTTP timing is held.
type sandboxSuccessorLeaseStore struct {
	tetralqueue.Store
	selected string
	observed chan string
}

func (s *sandboxSuccessorLeaseStore) Lease(ctx context.Context, r queue.LeaseRequest) ([]*queue.Job, error) {
	jobs, err := s.Store.Lease(ctx, r)
	for _, job := range jobs {
		if job.ID == s.selected {
			// Preserve the first exact successor capability without making the
			// diagnostic observer a barrier to later lease replies.
			select {
			case s.observed <- job.LeaseToken:
			default:
			}
		}
	}
	return jobs, err
}

// The process fault selects the sixth actual submission, after the first five
// real commands settle. Provider timing controls no transaction or Queue lease.
type sandboxProcessFaultProvider struct {
	*sandboxReplicaProvider
	selected string // guarded by ledger.mu
}

func (p *sandboxProcessFaultProvider) ExecuteTool(ctx context.Context, r sandbox.ToolExecutionRequest) sandbox.ProviderOutcome[driver.ToolExecution] {
	id := r.Invocation.ToolUseEventID
	p.ledger.mu.Lock()
	p.ledger.submits[id]++
	if p.selected == "" && len(p.ledger.submits) == 6 {
		p.selected = id
	}
	hold := id == p.selected
	p.ledger.mu.Unlock()
	if !hold {
		return sandbox.ProviderOutcome[driver.ToolExecution]{Value: driver.ToolExecution{ResultJSON: `{"status":"completed","stdout":{"text":"original command","truncated":false},"stderr":{"text":"","truncated":false}}`}}
	}
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

func TestPostgreSQLReplicaSandboxProcessTakeover(t *testing.T) {
	if os.Getenv("TETRAL_SANDBOX_PROCESS_CHILD") == "true" {
		ctx := context.Background()
		opened, err := dbconnect.OpenPlainDSN(ctx, "fixture_database", os.Getenv("TETRAL_SANDBOX_PROCESS_DATABASE"))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = opened.Client.Close() }()
		queueClient := sandbox.WithQueueAcquisition(sandbox.SandboxQueueFromGRPC(serveQueueReplica(t, queue.NewPostgreSQLStore(opened.Client), nil)))
		provider := &sandboxProcessHTTPProvider{bridgeMemoryProjectionProvider: &bridgeMemoryProjectionProvider{}, address: os.Getenv("TETRAL_SANDBOX_PROCESS_PROVIDER")}
		registry, err := sandbox.NewProviderRegistry(map[string]sandbox.ProviderAdapter{driver.DaytonaProviderName: provider})
		if err != nil {
			t.Fatal(err)
		}
		workers, err := sandbox.NewWorkspaceConsumerPool(1)
		if err != nil {
			t.Fatal(err)
		}
		err = sandbox.RunSandboxToolExecutionConsumerGroup(ctx, 2, workers, sandboxReplicaWorkspaceLister{}, 10*time.Millisecond, queueClient, sandbox.NewPostgreSQLSandboxExecutionCoordinator(opened.Client, 30*time.Minute), registry, backgroundNotificationMedia{}, sandbox.SandboxToolExecutionRunnerConfig{LeaseOwner: "sandbox-killed-process", MaxJobs: 1, LeaseDuration: 3 * time.Second, HeartbeatInterval: 200 * time.Millisecond, PreparationTimeout: time.Second}, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	for _, beforeReference := range []bool{false, true} {
		t.Run(fmt.Sprintf("before_reference_%t", beforeReference), func(t *testing.T) {
			runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			bridgeStore := bridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
			for i := range 6 {
				session := fmt.Sprintf("sesn_process_takeover_%d", i)
				thread := fmt.Sprintf("thr_process_takeover_%d", i)
				binding := fmt.Sprintf("binding_process_takeover_%d", i)
				pod := fmt.Sprintf("pod_process_takeover_%d", i)
				seedBridgeAPISession(t, admin, "default", session, thread)
				seedBridgeAPIRuntimeBinding(t, admin, "default", session, binding, 1, pod)
				seedReadySandboxForSharedToolExecution(t, admin, "default", session)
				scope := bridgeAPIScope(session, thread, binding, 1, pod)
				id := writeDurableOrdinaryToolUseForTest(t, bridgeStore, scope, "request_"+session, "call_"+session, "Read", `{"file_path":"/workspace/input.txt"}`)
				ack, err := bridgeStore.AcceptSandboxExecution(context.Background(), &bridgev1.AcceptSandboxExecutionRequest{Scope: scope, ToolUseEventId: id})
				if err != nil || ack.GetCommitted() == nil {
					t.Fatalf("execution admission=%v/%v", ack, err)
				}
			}
			ctx, stop := context.WithTimeout(context.Background(), 20*time.Second)
			defer stop()
			ledger := &sandboxReplicaLedger{submits: map[string]int{}, observes: map[string][]string{}}
			release := make(chan struct{})
			provider := &sandboxProcessFaultProvider{sandboxReplicaProvider: &sandboxReplicaProvider{bridgeMemoryProjectionProvider: &bridgeMemoryProjectionProvider{}, ledger: ledger, entered: make(chan string, 20), release: release, beforeReference: beforeReference}}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var result sandbox.ProviderOutcome[driver.ToolExecution]
				if r.URL.Path == "/execute" {
					var input sandbox.ToolExecutionRequest
					if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
						http.Error(w, "invalid fixture request", 400)
						return
					}
					result = provider.ExecuteTool(r.Context(), input)
				} else {
					var input driver.ForegroundCommandObservation
					if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
						http.Error(w, "invalid fixture request", 400)
						return
					}
					result = provider.ObserveTool(r.Context(), input)
				}
				_ = json.NewEncoder(w).Encode(result)
			}))
			defer server.Close()
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPostgreSQLReplicaSandboxProcessTakeover$")
			child.Env = append(os.Environ(), "TETRAL_SANDBOX_PROCESS_CHILD=true", "TETRAL_SANDBOX_PROCESS_DATABASE="+storagetest.RuntimeDatabaseURL(t, runtimeDB), "TETRAL_SANDBOX_PROCESS_PROVIDER="+server.URL)
			var output bytes.Buffer
			child.Stdout = &output
			child.Stderr = &output
			if err := child.Start(); err != nil {
				t.Fatal(err)
			}
			childExited := false
			defer func() {
				if !childExited {
					_ = child.Process.Kill()
					_ = child.Wait()
				}
			}()
			var selected string
			select {
			case selected = <-provider.entered:
			case <-ctx.Done():
				t.Fatal("child did not reach external effect")
			}
			// The fault selects the sixth actual external submission, so an
			// unrelated recoverable lease loss cannot be mistaken for a missing
			// provider effect after death. All six commands still execute through
			// the real child runner. ReplicaSandboxTakeover independently covers
			// two live owners holding distinct work; this root owns exact PID
			// death and reference/unknown recovery of the selected command.
			ledger.mu.Lock()
			selectedMatch := selected == provider.selected
			sixEffects := len(ledger.submits) == 6
			for _, n := range ledger.submits {
				sixEffects = sixEffects && n == 1
			}
			ledger.mu.Unlock()
			if !selectedMatch || !sixEffects {
				t.Fatal("six actual single effects were not established before selected process death")
			}
			var completed, acknowledged int
			if err := admin.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM session_runtime_tool_results WHERE tool_use_event_id<>$1 AND execution_state='terminal_unconsumed' AND result_json IS NOT NULL),(SELECT count(*) FROM queue_jobs WHERE kind=$2 AND payload_json::jsonb->>'tool_use_event_id'<>$1 AND status='acknowledged')`, selected, queue.KindSandboxToolExecute).Scan(&completed, &acknowledged); err != nil || completed != 5 || acknowledged != 5 {
				t.Fatalf("first five real commands terminal/ACK=%d/%d/%v", completed, acknowledged, err)
			}
			var reference string
			waitHandoffCondition(t, "actual worker process durable running boundary", func() bool {
				var state string
				if err := admin.QueryRowContext(ctx, `SELECT execution_state,COALESCE(provider_command_reference_json,'') FROM session_runtime_tool_results WHERE tool_use_event_id=$1`, selected).Scan(&state, &reference); err != nil {
					t.Fatal(err)
				}
				return state == "running" && (beforeReference || reference != "")
			})
			if beforeReference && reference != "" {
				t.Fatal("ambiguous submit already persisted reference")
			}
			var identityBefore string
			if err := admin.QueryRowContext(ctx, `SELECT jsonb_agg(jsonb_build_array(session_id,logical_sandbox_id,provider,provider_resource_id,binding_revision) ORDER BY session_id)::text FROM session_sandbox_bindings`).Scan(&identityBefore); err != nil {
				t.Fatal(err)
			}
			var jobID, oldToken string
			if err := admin.QueryRowContext(ctx, `SELECT id,lease_token FROM queue_jobs WHERE kind=$1 AND payload_json::jsonb->>'tool_use_event_id'=$2 AND status='leased'`, queue.KindSandboxToolExecute, selected).Scan(&jobID, &oldToken); err != nil {
				t.Fatal(err)
			}
			oldJob := &queuev1.QueueJob{Id: jobID, WorkspaceId: "default", Kind: queue.KindSandboxToolExecute, LeaseToken: oldToken}
			var oldExpiry time.Time
			if err := admin.QueryRowContext(ctx, `SELECT payload_json,payload_version,attempt_count,max_attempts,leased_until FROM queue_jobs WHERE id=$1`, jobID).Scan(&oldJob.PayloadJson, &oldJob.PayloadVersion, &oldJob.AttemptCount, &oldJob.MaxAttempts, &oldExpiry); err != nil {
				t.Fatal(err)
			}
			oldJob.LeasedUntil = oldExpiry.UTC().Format(time.RFC3339Nano)
			if err := child.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			childErr := child.Wait()
			childExited = true
			if childErr == nil || child.ProcessState.Success() {
				t.Fatalf("killed worker remained successful: %v", childErr)
			}
			t.Logf("killed actual Sandbox worker pid=%d reference_persisted=%t", child.Process.Pid, !beforeReference)
			awaitReplicaQueueExpiry(ctx, t, admin, jobID)
			t.Logf("successor Sandbox worker process pid=%d", os.Getpid())
			replacement := dbconnect.NewClientForTesting(storagetest.OpenRuntimeRoleDBWithTracer(t, runtimeDB, nil))
			replacementQueue := queue.NewPostgreSQLStore(replacement)
			if n, err := replacementQueue.ReclaimExpiredLeases(ctx, queue.ReclaimExpiredLeasesRequest{Limit: 20}); err != nil || n != 1 {
				t.Fatalf("reclaim killed worker=%d/%v", n, err)
			}
			startReplicaQueueMaintenance(ctx, t, replacementQueue, 10*time.Second)
			close(release)
			replacementProvider := &sandboxReplicaProvider{bridgeMemoryProjectionProvider: &bridgeMemoryProjectionProvider{}, ledger: ledger, entered: make(chan string, 20), release: release}
			registry, err := sandbox.NewProviderRegistry(map[string]sandbox.ProviderAdapter{driver.DaytonaProviderName: replacementProvider})
			if err != nil {
				t.Fatal(err)
			}
			workers, err := sandbox.NewWorkspaceConsumerPool(1)
			if err != nil {
				t.Fatal(err)
			}
			work, cancel := context.WithCancel(ctx)
			defer cancel()
			done := make(chan error, 1)
			successorLeases := &sandboxSuccessorLeaseStore{Store: replacementQueue, selected: jobID, observed: make(chan string, 1)}
			successorRPC := serveQueueReplica(t, successorLeases, nil)
			workerJoined := make(chan struct{})
			t.Cleanup(func() {
				cancel()
				select {
				case <-workerJoined:
				case <-time.After(10 * time.Second):
					t.Error("Sandbox successor did not join")
				}
			})
			go func() {
				defer close(workerJoined)
				done <- sandbox.RunSandboxToolExecutionConsumerGroup(work, 2, workers, sandboxReplicaWorkspaceLister{}, 10*time.Millisecond, sandbox.WithQueueAcquisition(sandbox.SandboxQueueFromGRPC(successorRPC)), sandbox.NewPostgreSQLSandboxExecutionCoordinator(replacement, 30*time.Minute), registry, backgroundNotificationMedia{}, sandbox.SandboxToolExecutionRunnerConfig{LeaseOwner: "sandbox-successor", MaxJobs: 1, LeaseDuration: 3 * time.Second, HeartbeatInterval: 200 * time.Millisecond, PreparationTimeout: time.Second}, nil, nil)
			}()
			waitHandoffCondition(t, "successor settles all six executions", func() bool {
				var count int
				if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM queue_jobs WHERE kind=$1 AND status='acknowledged'`, queue.KindSandboxToolExecute).Scan(&count); err != nil {
					t.Fatal(err)
				}
				return count == 6
			})
			cancel()
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("successor did not join")
			}
			var newToken, state, result, newReference string
			select {
			case newToken = <-successorLeases.observed:
			case <-ctx.Done():
				t.Fatal("successor exact lease not observed")
			}
			if newToken == oldToken {
				t.Fatal("successor reused killed worker lease")
			}
			if err := admin.QueryRowContext(ctx, `SELECT execution_state,result_json,COALESCE(provider_command_reference_json,'') FROM session_runtime_tool_results WHERE tool_use_event_id=$1`, selected).Scan(&state, &result, &newReference); err != nil {
				t.Fatal(err)
			}
			if state != "terminal_unconsumed" || !json.Valid([]byte(result)) {
				t.Fatalf("successor terminal=%s/%s", state, result)
			}
			if beforeReference {
				if !containsSandboxUnknownResult(result) || newReference != "" {
					t.Fatalf("ambiguous command blindly resubmitted: %s", result)
				}
			} else if newReference != reference {
				t.Fatal("successor changed original provider command reference")
			}
			ledger.mu.Lock()
			if len(ledger.submits) != 6 {
				t.Errorf("external submit identities=%d", len(ledger.submits))
			}
			for id, n := range ledger.submits {
				if n != 1 {
					t.Errorf("external command %s submitted %d times", id, n)
				}
			}
			if !beforeReference && len(ledger.observes[selected]) < 2 {
				t.Error("successor did not reobserve original command")
			}
			for _, id := range ledger.observes[selected] {
				if id != "command_"+selected {
					t.Error("successor observed different command")
				}
			}
			ledger.mu.Unlock()
			var identityAfter string
			var count, released int
			if err := admin.QueryRowContext(ctx, `SELECT jsonb_agg(jsonb_build_array(session_id,logical_sandbox_id,provider,provider_resource_id,binding_revision) ORDER BY session_id)::text FROM session_sandbox_bindings`).Scan(&identityAfter); err != nil || identityBefore != identityAfter {
				t.Fatalf("Sandbox identities changed=%t/%v", identityBefore != identityAfter, err)
			}
			if err := admin.QueryRowContext(ctx, `SELECT count(*),count(release_requested_at) FROM session_sandbox_bindings`).Scan(&count, &released); err != nil || count != 6 || released != 0 {
				t.Fatalf("Sandbox continuity=%d/%d/%v", count, released, err)
			}
			stale, err := replacementQueue.Ack(ctx, queue.AckRequest{WorkspaceID: "default", JobID: jobID, LeaseToken: oldToken})
			if err != nil || stale {
				t.Fatalf("killed worker stale ACK=%v/%v", stale, err)
			}
			snapshot := func() string {
				var value string
				if err := admin.QueryRowContext(ctx, `SELECT jsonb_build_object('execution',(SELECT to_jsonb(r) FROM session_runtime_tool_results r WHERE tool_use_event_id=$1),'queue',(SELECT to_jsonb(q) FROM queue_jobs q WHERE id=$2))::text`, selected, jobID).Scan(&value); err != nil {
					t.Fatal(err)
				}
				return value
			}
			beforeStale := snapshot()
			oldRunner := &sandbox.SandboxToolExecutionJobRunner{Queue: sandboxKilledCapabilityQueue{SandboxQueueClient: sandbox.SandboxQueueFromGRPC(successorRPC), job: oldJob}, Coordinator: sandbox.NewPostgreSQLSandboxExecutionCoordinator(replacement, 30*time.Minute), Providers: registry, Media: backgroundNotificationMedia{}, Config: sandbox.SandboxToolExecutionRunnerConfig{WorkspaceID: "default", LeaseOwner: "killed-capability-replay", MaxJobs: 1, LeaseDuration: 3 * time.Second, HeartbeatInterval: 200 * time.Millisecond, PreparationTimeout: time.Second}}
			if _, err := oldRunner.RunOnceWithActivity(ctx); err == nil {
				t.Fatal("killed execution capability escaped production runner fence")
			}
			if afterStale := snapshot(); afterStale != beforeStale {
				t.Fatal("stale worker changed successor execution or Queue custody")
			}

		})
	}
}
