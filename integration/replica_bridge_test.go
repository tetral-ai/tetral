package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"
)

func bridgeChildRequest(t *testing.T, message proto.Message) json.RawMessage {
	t.Helper()
	raw, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func startReplicaBridgeChild(t *testing.T, addresses []string, actions []map[string]any) *handoffRuntimeChild {
	t.Helper()
	child := &handoffRuntimeChild{directory: t.TempDir(), done: make(chan struct{})}
	input, err := json.Marshal(map[string]any{"addresses": addresses, "token": "runtime", "directory": child.directory, "env": map[string]string{}, "actions": actions})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(child.directory, "input.json")
	if err = os.WriteFile(path, input, 0600); err != nil {
		t.Fatal(err)
	}
	output, err := os.Create(filepath.Join(child.directory, "output.log"))
	if err != nil {
		t.Fatal(err)
	}
	child.command = exec.Command("bun", "packages/runtime-pod/test/fixtures/replica-bridge-recovery.ts", path) //nolint:gosec // Fixed repository child and test-owned input.
	child.command.Dir = "../services/agent-runtime"
	child.command.Stdout = output
	child.command.Stderr = output
	if err = child.command.Start(); err != nil {
		_ = output.Close()
		t.Fatal(err)
	}
	go func() { child.err = child.command.Wait(); _ = output.Close(); close(child.done) }()
	t.Cleanup(func() {
		select {
		case <-child.done:
		default:
			_ = child.command.Process.Kill()
			select {
			case <-child.done:
			case <-time.After(5 * time.Second):
				t.Error("Bridge child cleanup did not join")
			}
		}
	})
	return child
}
func bridgeChildResult(t *testing.T, child *handoffRuntimeChild, name string) json.RawMessage {
	t.Helper()
	var found json.RawMessage
	waitHandoffCondition(t, "Runtime Bridge action "+name, func() bool {
		raw, err := os.ReadFile(filepath.Join(child.directory, "results.json"))
		var results map[string]json.RawMessage
		if err == nil && json.Unmarshal(raw, &results) == nil && results[name] != nil {
			found = results[name]
			return true
		}
		select {
		case <-child.done:
			if child.err != nil {
				out, _ := os.ReadFile(filepath.Join(child.directory, "output.log"))
				t.Fatalf("Runtime Bridge action exited %v\n%s", child.err, out)
			}
		default:
		}
		return false
	})
	return found
}
func bridgeChildAction(name string, replica int, method string, request json.RawMessage, timeout int, errorCode ...codes.Code) map[string]any {
	action := map[string]any{"name": name, "replica": replica, "method": method, "request": request, "timeoutMs": timeout}
	if len(errorCode) > 0 {
		action["error"] = int(errorCode[0])
	}
	return action
}
func replicaBridgePair(t *testing.T, db *sql.DB, podUID string, after func(context.Context, string, any) error) (*bridge.PostgreSQLBridgeAPIStore, []string) {
	t.Helper()
	first := bridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(db))
	second := bridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(db))
	first.RuntimeBindingTokenHMACKey = []byte("replica-bridge-recovery-shared-key")
	second.RuntimeBindingTokenHMACKey = first.RuntimeBindingTokenHMACKey
	ctx, cancel := context.WithCancel(context.Background())
	joined := make(chan error, 2)
	for _, store := range []*bridge.PostgreSQLBridgeAPIStore{first, second} {
		go func(store *bridge.PostgreSQLBridgeAPIStore) { joined <- store.RunExecutionResultListener(ctx) }(store)
	}
	t.Cleanup(func() {
		cancel()
		for range 2 {
			select {
			case <-joined:
			case <-time.After(5 * time.Second):
				t.Error("Bridge result listener did not join")
			}
		}
	})
	a := serveReplicaBridge(t, first, map[string]string{"runtime": podUID}, after)
	b := serveReplicaBridge(t, second, map[string]string{"runtime": podUID}, nil)
	return first, []string{a.Address, b.Address}
}
func TestPostgreSQLReplicaBridgeRecovery(t *testing.T) {
	t.Run("lost declaration response and exact retired fences", func(t *testing.T) {
		runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		seedBridgeAPISession(t, admin, "default", "sesn_bridge_replica", "thr_bridge_replica")
		seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_replica", "bind_bridge_replica", 1, "pod_replica")
		var lost atomic.Bool
		_, addresses := replicaBridgePair(t, runtimeDB, "pod_replica", func(_ context.Context, method string, _ any) error {
			if method == bridgev1.AgentRuntimeBridgeService_WriteEvent_FullMethodName && lost.CompareAndSwap(false, true) {
				return status.Error(codes.Unavailable, "committed declaration response lost")
			}
			return nil
		})
		request := &bridgev1.WriteEventRequest{Scope: bridgeAPIScope("sesn_bridge_replica", "thr_bridge_replica", "bind_bridge_replica", 1, "pod_replica"), RuntimeWriteId: "receipt-replica-original", EventType: "session.status_running", PayloadJson: `{"type":"session.status_running"}`}
		conflict := proto.Clone(request).(*bridgev1.WriteEventRequest)
		conflict.PayloadJson = `{"type":"session.status_running","conflict":true}`
		stale := proto.Clone(request).(*bridgev1.WriteEventRequest)
		stale.Scope.Binding.RuntimeProcessId = "unregistered"
		child := startReplicaBridgeChild(t, addresses, []map[string]any{bridgeChildAction("cold_read", 0, "loadContext", bridgeChildRequest(t, &bridgev1.LoadContextRequest{Scope: request.Scope}), 7000), bridgeChildAction("lost", 0, "writeEvent", bridgeChildRequest(t, request), 5000, codes.Unavailable), bridgeChildAction("replay", 1, "writeEvent", bridgeChildRequest(t, request), 5000), bridgeChildAction("conflict", 1, "writeEvent", bridgeChildRequest(t, conflict), 5000, codes.AlreadyExists), bridgeChildAction("stale", 1, "writeEvent", bridgeChildRequest(t, stale), 5000)})
		replay := bridgeChildResult(t, child, "replay")
		var result struct {
			Duplicate *struct {
				EventID string `json:"eventId"`
			} `json:"duplicate"`
		}
		if err := json.Unmarshal(replay, &result); err != nil || result.Duplicate == nil || result.Duplicate.EventID == "" {
			t.Fatalf("exact original declaration replay=%s/%v", replay, err)
		}
		staleResult := bridgeChildResult(t, child, "stale")
		var staleOutcome map[string]json.RawMessage
		if err := json.Unmarshal(staleResult, &staleOutcome); err != nil || staleOutcome["stale"] == nil {
			t.Fatalf("typed stale scope result=%s/%v", staleResult, err)
		}
		child.join(t)
		logReplicaBridgeCompletions(t, child)
		var count int
		var eventID string
		if err := admin.QueryRow(`SELECT count(*),min(event_id) FROM session_events WHERE session_id='sesn_bridge_replica' AND runtime_write_id='receipt-replica-original'`).Scan(&count, &eventID); err != nil || count != 1 || eventID != result.Duplicate.EventID {
			t.Fatalf("durable declaration count=%d event=%s error=%v", count, eventID, err)
		}
	})
	t.Run("accepted execution expires one wait then rejoins another Bridge", func(t *testing.T) {
		runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		const session = "sesn_bridge_execution_rejoin"
		const thread = "thr_bridge_execution_rejoin"
		seedBridgeAPISession(t, admin, "default", session, thread)
		seedBridgeAPIRuntimeBinding(t, admin, "default", session, "bind_execution", 1, "pod_execution")
		var lost atomic.Bool
		store, addresses := replicaBridgePair(t, runtimeDB, "pod_execution", func(_ context.Context, method string, _ any) error {
			if method == bridgev1.AgentRuntimeBridgeService_AcceptSandboxExecution_FullMethodName && lost.CompareAndSwap(false, true) {
				return status.Error(codes.Unavailable, "accepted execution response lost")
			}
			return nil
		})
		scope := bridgeAPIScope(session, thread, "bind_execution", 1, "pod_execution")
		toolID := writeDurableOrdinaryToolUseForTest(t, store, scope, "request_execution", "call_execution", "Read", `{"file_path":"/workspace/input.txt"}`)
		accepted := bridgeChildRequest(t, &bridgev1.AcceptSandboxExecutionRequest{Scope: scope, ToolUseEventId: toolID})
		awaited := bridgeChildRequest(t, &bridgev1.AwaitSandboxExecutionRequest{Scope: scope, ToolUseEventId: toolID})
		child := startReplicaBridgeChild(t, addresses, []map[string]any{bridgeChildAction("accept_lost", 0, "acceptSandboxExecution", accepted, 5000, codes.Unavailable), bridgeChildAction("accept_replay", 1, "acceptSandboxExecution", accepted, 5000), bridgeChildAction("expired_wait", 0, "awaitSandboxExecution", awaited, 80, codes.DeadlineExceeded), bridgeChildAction("rejoined_wait", 1, "awaitSandboxExecution", awaited, 5000)})
		bridgeChildResult(t, child, "expired_wait")
		var acceptedRows int
		if err := admin.QueryRow(`SELECT count(*) FROM session_runtime_tool_results WHERE session_id=$1 AND tool_use_event_id=$2`, session, toolID).Scan(&acceptedRows); err != nil || acceptedRows != 1 {
			t.Fatalf("wait expiry lost accepted custody=%d/%v", acceptedRows, err)
		}
		fixture := &separatedOwners{ctx: context.Background(), admin: admin, runner: jobrunner.NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtimeDB), 19090), queue: queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtimeDB)), sessionID: session, threadID: thread}
		calls := separatedExecuteSandbox(t, fixture, scope, toolID, `{"status":"completed","stdout":{"text":"original result","truncated":false},"stderr":{"text":"","truncated":false}}`)
		if calls != 1 {
			t.Fatalf("external invocation ledger=%d", calls)
		}
		completed := bridgeChildResult(t, child, "rejoined_wait")
		var result map[string]json.RawMessage
		if err := json.Unmarshal(completed, &result); err != nil || result["completed"] == nil {
			t.Fatalf("rejoined execution=%s/%v", completed, err)
		}
		child.join(t)
		logReplicaBridgeCompletions(t, child)
	})
	for _, method := range []string{"readCommandResult", "sendCommandInput", "cancelCommand"} {
		t.Run("background receipt "+method+" rejoins exact operation", func(t *testing.T) {
			runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			session, thread, binding, task := "sesn_"+method, "thr_"+method, "bind_"+method, "task_"+method
			seedBridgeAPISession(t, admin, "default", session, thread)
			seedBridgeAPIRuntimeBinding(t, admin, "default", session, binding, 1, "pod_background")
			seedReadySandboxForSharedToolExecution(t, admin, "default", session)
			store, addresses := replicaBridgePair(t, runtimeDB, "pod_background", nil)
			scope := bridgeAPIScope(session, thread, binding, 1, "pod_background")
			chars := ""
			if method == "sendCommandInput" {
				chars = "hello\n"
			}
			input, _ := json.Marshal(map[string]string{"session_id": task, "chars": chars})
			toolID := writeDurableOrdinaryToolUseForTest(t, store, scope, "request_background", "call_background", "write_stdin", string(input))
			seedBridgeAPIBackgroundTask(t, admin, "default", session, thread, binding, task, "evt_source_background")
			var request proto.Message
			switch method {
			case "readCommandResult":
				request = &bridgev1.ReadCommandResultRequest{Scope: scope, TaskId: task, ToolUseEventId: toolID, OperationId: "original_poll"}
			case "sendCommandInput":
				request = &bridgev1.SendCommandInputRequest{Scope: scope, TaskId: task, ToolUseEventId: toolID, OperationId: "original_stdin"}
			case "cancelCommand":
				request = &bridgev1.CancelCommandRequest{Scope: scope, TaskId: task, ToolUseEventId: toolID, OperationId: "original_cancel", Reason: "runtime_interrupted"}
			}
			actions := []map[string]any{bridgeChildAction("expired", 0, method, bridgeChildRequest(t, request), 200, codes.DeadlineExceeded), bridgeChildAction("rejoin", 1, method, bridgeChildRequest(t, request), 5000), bridgeChildAction("replay", 0, method, bridgeChildRequest(t, request), 5000)}
			expectedReceipts, expectedCalls := 1, int32(1)
			if method == "cancelCommand" {
				actions = append([]map[string]any{bridgeChildAction("initial_poll", 0, "readCommandResult", bridgeChildRequest(t, &bridgev1.ReadCommandResultRequest{Scope: scope, TaskId: task, ToolUseEventId: toolID, OperationId: "original_control_poll"}), 200, codes.DeadlineExceeded)}, actions...)
				expectedReceipts = 2
				expectedCalls = 2
			}
			child := startReplicaBridgeChild(t, addresses, actions)
			bridgeChildResult(t, child, "expired")
			var receipts int
			if err := admin.QueryRow(`SELECT count(*) FROM session_runtime_tool_results WHERE session_id=$1 AND background_operation_state='pending'`, session).Scan(&receipts); err != nil || receipts != expectedReceipts {
				t.Fatalf("original pending receipt=%d/%v", receipts, err)
			}
			provider := &replicaBackgroundProvider{terminalBackgroundProvider: terminalBackgroundProvider{result: sandboxdriver.CommandResult{ResultJSON: `{"status":"completed","stdout":{"text":"original result","truncated":false},"stderr":{"text":"","truncated":false}}`, TerminalStatus: "completed"}}}
			providers, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{"daytona": provider})
			if err != nil {
				t.Fatal(err)
			}
			client := dbconnect.NewClientForTesting(runtimeDB)
			runner := &tetralsandbox.SandboxBackgroundCommandJobRunner{Queue: tetralqueue.NewServer(queue.NewPostgreSQLStore(client), nil), Store: tetralsandbox.NewPostgreSQLSandboxBackgroundCommandStore(client), Providers: providers, Config: tetralsandbox.SandboxBackgroundRunnerConfig{WorkspaceID: "default", LeaseOwner: "replica-background", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Second}}
			if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
				t.Fatalf("actual background owner=%t/%v", active, err)
			}
			if method == "cancelCommand" {
				if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
					t.Fatalf("actual cancellation owner=%t/%v", active, err)
				}
			}
			bridgeChildResult(t, child, "rejoin")
			bridgeChildResult(t, child, "replay")
			child.join(t)
			logReplicaBridgeCompletions(t, child)
			if provider.calls.Load() != expectedCalls {
				t.Fatalf("external operation replayed %d times", provider.calls.Load())
			}
			if err := admin.QueryRow(`SELECT count(*) FROM session_runtime_tool_results WHERE session_id=$1 AND background_operation_state='terminal'`, session).Scan(&receipts); err != nil || receipts != expectedReceipts {
				t.Fatalf("terminal original receipt=%d/%v", receipts, err)
			}
			if method == "sendCommandInput" {
				var seq int
				if err := admin.QueryRow(`SELECT stdin_write_sequence FROM session_background_tasks WHERE session_id=$1 AND task_id=$2`, session, task).Scan(&seq); err != nil || seq != 1 {
					t.Fatalf("stdin sequence=%d/%v", seq, err)
				}
			}
		})
	}
	t.Run("memory mutation waits for original projection across replicas", func(t *testing.T) {
		runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		const session = "sesn_replica_memory"
		const thread = "thr_replica_memory"
		const memoryStore = "memstore_replica_memory"
		seedBridgeAPISession(t, admin, "default", session, thread)
		seedBridgeAPIRuntimeBinding(t, admin, "default", session, "bind_memory", 1, "pod_memory")
		seedReadySandboxForSharedToolExecution(t, admin, "default", session)
		for _, statement := range []string{`INSERT INTO memory_stores(workspace_id,memory_store_id,name,created_at,updated_at) VALUES('default','memstore_replica_memory','memory',now(),now())`, `INSERT INTO session_resources(workspace_id,session_id,resource_id,type,created_at,updated_at) VALUES('default','sesn_replica_memory','res_memory','memory_store',now(),now())`, `INSERT INTO session_memory_store_resources(workspace_id,session_id,resource_id,memory_store_id,access,name,mount_path) VALUES('default','sesn_replica_memory','res_memory','memstore_replica_memory','read_write','memory','/mnt/memory/replica')`} {
			if _, err := admin.Exec(statement); err != nil {
				t.Fatal(err)
			}
		}
		store, addresses := replicaBridgePair(t, runtimeDB, "pod_memory", nil)
		scope := bridgeAPIScope(session, thread, "bind_memory", 1, "pod_memory")
		toolID := writeDurableOrdinaryToolUseForTest(t, store, scope, "request_memory", "call_memory", "memory", `{"action":"create","path":"notes/replica.md","content":"original durable mutation"}`)
		request := bridgeChildRequest(t, &bridgev1.RunMemoryRequest{Scope: scope, ToolUseEventId: toolID})
		child := startReplicaBridgeChild(t, addresses, []map[string]any{bridgeChildAction("memory_expired", 0, "runMemory", request, 200, codes.DeadlineExceeded), bridgeChildAction("memory_rejoin", 1, "runMemory", request, 5000), bridgeChildAction("memory_replay", 0, "runMemory", request, 5000)})
		bridgeChildResult(t, child, "memory_expired")
		var versions int
		if err := admin.QueryRow(`SELECT count(*) FROM memory_versions WHERE memory_store_id=$1`, memoryStore).Scan(&versions); err != nil || versions != 1 {
			t.Fatalf("committed memory versions=%d/%v", versions, err)
		}
		provider := &bridgeMemoryProjectionProvider{}
		providers, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{"daytona": provider})
		if err != nil {
			t.Fatal(err)
		}
		client := dbconnect.NewClientForTesting(runtimeDB)
		runner := &tetralsandbox.SandboxMemoryProjectionJobRunner{Queue: tetralqueue.NewServer(queue.NewPostgreSQLStore(client), nil), Store: tetralsandbox.NewPostgreSQLSandboxMemoryProjectionStore(client), Providers: providers, Config: tetralsandbox.SandboxMemoryProjectionRunnerConfig{WorkspaceID: "default", LeaseOwner: "replica-memory", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Second}}
		if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
			t.Fatalf("actual memory projection=%t/%v", active, err)
		}
		bridgeChildResult(t, child, "memory_rejoin")
		bridgeChildResult(t, child, "memory_replay")
		child.join(t)
		logReplicaBridgeCompletions(t, child)
		if len(provider.requests) != 1 || len(provider.requests[0].Ops) != 1 || provider.requests[0].Ops[0].Content != "original durable mutation" {
			t.Fatalf("projection replayed or changed %+v", provider.requests)
		}
		if err := admin.QueryRow(`SELECT count(*) FROM memory_versions WHERE memory_store_id=$1`, memoryStore).Scan(&versions); err != nil || versions != 1 {
			t.Fatalf("memory version replay=%d/%v", versions, err)
		}
	})
	t.Run("output capture expires without terminal failure then rejoins", func(t *testing.T) {
		runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		const session = "sesn_bridge_capture_rejoin"
		const thread = "thr_bridge_capture_rejoin"
		seedBridgeAPISession(t, admin, "default", session, thread)
		seedBridgeAPIRuntimeBinding(t, admin, "default", session, "bind_capture", 1, "pod_capture")
		seedReadySandboxForSharedToolExecution(t, admin, "default", session)
		_, addresses := replicaBridgePair(t, runtimeDB, "pod_capture", nil)
		scope := bridgeAPIScope(session, thread, "bind_capture", 1, "pod_capture")
		request := bridgeAPIFinishIdleRequest(t, admin, scope, "durable_capture_original", `{"reason":"done"}`)
		child := startReplicaBridgeChild(t, addresses, []map[string]any{bridgeChildAction("capture_expired", 0, "finishIdle", bridgeChildRequest(t, request), 80, codes.DeadlineExceeded), bridgeChildAction("capture_rejoin", 1, "finishIdle", bridgeChildRequest(t, request), 5000)})
		bridgeChildResult(t, child, "capture_expired")
		writeID, generation, err := waitForPendingOutputCapture(admin, session, "")
		if err != nil || writeID != request.DurableTurnId {
			t.Fatalf("original capture identity=%s generation=%d error=%v", writeID, generation, err)
		}
		if err = settleOutputCaptureGenerationForTest(admin, session, writeID, generation, "staged"); err != nil {
			t.Fatal(err)
		}
		completed := bridgeChildResult(t, child, "capture_rejoin")
		var result map[string]json.RawMessage
		if err = json.Unmarshal(completed, &result); err != nil || result["committed"] == nil {
			t.Fatalf("capture rejoin=%s/%v", completed, err)
		}
		child.join(t)
		logReplicaBridgeCompletions(t, child)
		var errors, count int
		if err = admin.QueryRow(`SELECT (SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.error'),(SELECT count(*) FROM sandbox_output_capture_operations WHERE session_id=$1)`, session).Scan(&errors, &count); err != nil || errors != 0 || count != 1 {
			t.Fatalf("capture expiry terminal errors=%d captures=%d/%v", errors, count, err)
		}
	})
}

type replicaBackgroundProvider struct {
	terminalBackgroundProvider
	calls atomic.Int32
}

func (p *replicaBackgroundProvider) PollBackground(ctx context.Context, r sandboxdriver.CommandReference) tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult] {
	p.calls.Add(1)
	return tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult]{Value: sandboxdriver.CommandResult{ResultJSON: `{"status":"running","stdout":{"text":"polled once","truncated":false},"stderr":{"text":"","truncated":false}}`}}
}
func (p *replicaBackgroundProvider) SendBackgroundInput(context.Context, sandboxdriver.CommandInput) tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult] {
	p.calls.Add(1)
	return tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult]{Value: p.result}
}
func (p *replicaBackgroundProvider) CancelBackground(context.Context, sandboxdriver.CommandCancel) tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult] {
	p.calls.Add(1)
	return tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult]{Value: p.result}
}

func logReplicaBridgeCompletions(t *testing.T, child *handoffRuntimeChild) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(child.directory, "results.json"))
	if err != nil {
		t.Fatal(err)
	}
	var results struct {
		CompletionSamples []replicaCompletionSample `json:"completionSamples"`
	}
	if err = json.Unmarshal(raw, &results); err != nil {
		t.Fatal(err)
	}
	for _, sample := range results.CompletionSamples {
		replicaLogCompletion(t, sample)
	}
}
