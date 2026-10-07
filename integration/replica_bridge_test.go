package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/internalgrpc/auth"
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
	firstPool := storagetest.OpenRuntimeRoleDBWithTracer(t, db, nil)
	secondPool := storagetest.OpenRuntimeRoleDBWithTracer(t, db, nil)
	first := bridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(firstPool))
	second := bridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(secondPool))
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
	t.Run("Core rejoins pending capture after three actual wait expiries", func(t *testing.T) {
		runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		const session, thread, binding, pod = "sesn_core_capture_rejoin", "thr_core_capture_rejoin", "bind_core_capture", "pod_core_capture"
		seedBridgeAPISession(t, admin, "default", session, thread)
		seedBridgeAPIRuntimeBinding(t, admin, "default", session, binding, 1, pod)
		seedReadySandboxForSharedToolExecution(t, admin, "default", session)
		provider := &heldCoreCaptureProvider{handoffCaptureProvider: handoffCaptureProvider{bridgeMemoryProjectionProvider: &bridgeMemoryProjectionProvider{}}, release: make(chan struct{})}
		t.Cleanup(provider.finish)
		startHandoffOutputCapturesWithProvider(t, runtimeDB, provider)
		store := bridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
		observed := &coreCaptureRejoinStore{BridgeAPIStore: store}
		endpoint := serveReplicaBridge(t, observed, map[string]string{"runtime": pod}, nil)
		request := bridgeAPIFinishIdleRequest(t, admin, bridgeAPIScope(session, thread, binding, 1, pod), "turn_core_capture_original", `{"type":"end_turn"}`)
		child := startReplicaFinishIdleCoreChild(t, endpoint.Address, bridgeChildRequest(t, request))
		// Returned Bridge waits do not establish independent Sandbox worker dispatch.
		waitHandoffCondition(t, "original capture provider entered", func() bool { return provider.calls.Load() >= 1 })
		joinedBaseline := observed.joined.Load()
		waitHandoffCondition(t, "three joined actual pending capture waits while provider held", func() bool {
			return observed.joined.Load() >= joinedBaseline+3
		})
		writeID, generation, err := waitForPendingOutputCapture(admin, session, "")
		if err != nil || writeID != request.DurableTurnId || provider.calls.Load() != 1 {
			t.Fatalf("one original held capture id=%s generation=%d dispatches=%d err=%v", writeID, generation, provider.calls.Load(), err)
		}
		provider.finish()
		var report struct {
			Result struct {
				Type              string `json:"type"`
				ModelMessageCount int    `json:"modelMessageCount"`
			} `json:"result"`
			Backoffs []int `json:"backoffs"`
		}
		waitHandoffCondition(t, "Core late capture completes normally", func() bool {
			raw, readErr := os.ReadFile(filepath.Join(child.directory, "results.json"))
			return readErr == nil && json.Unmarshal(raw, &report) == nil
		})
		child.join(t)
		if report.Result.Type != "completed" || report.Result.ModelMessageCount != 0 || len(report.Backoffs) < 3 || report.Backoffs[0] != 100 || report.Backoffs[1] != 300 || report.Backoffs[2] != 300 {
			t.Fatalf("late-ready Core result=%+v backoffs=%v", report.Result, report.Backoffs)
		}
		observed.mu.Lock()
		requests := append([]*bridgev1.FinishIdleRequest(nil), observed.requests...)
		observed.mu.Unlock()
		if len(requests) < 4 || observed.maximum.Load() != 1 || observed.active.Load() != 0 || observed.joined.Load() != int32(len(requests)) {
			t.Fatalf("serial raw waits=%d maximum=%d active=%d joined=%d", len(requests), observed.maximum.Load(), observed.active.Load(), observed.joined.Load())
		}
		for _, actual := range requests {
			if !proto.Equal(actual, request) {
				t.Fatalf("capture declaration changed: %v versus %v", actual, request)
			}
		}
		var captures, adopted, idle, failures int
		if err := admin.QueryRow(`SELECT (SELECT count(*) FROM sandbox_output_capture_operations WHERE session_id=$1),(SELECT count(*) FROM sandbox_output_capture_operations WHERE session_id=$1 AND state='adopted'),(SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.status_idle'),(SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.error')`, session).Scan(&captures, &adopted, &idle, &failures); err != nil || captures != 1 || adopted != 1 || idle != 1 || failures != 0 || provider.calls.Load() != 1 {
			t.Fatalf("original capture=%d adopted=%d idle=%d failures=%d dispatches=%d err=%v", captures, adopted, idle, failures, provider.calls.Load(), err)
		}
	})
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
		fixture := &separatedOwners{ctx: context.Background(), admin: admin, peerDB: runtimeDB, runner: jobrunner.NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtimeDB), 19090), queue: queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtimeDB)), sessionID: session, threadID: thread}
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
			gates := map[string]*replicaBackgroundAdmissionGate{
				"original_poll":         newReplicaBackgroundAdmissionGate(),
				"original_stdin":        newReplicaBackgroundAdmissionGate(),
				"original_cancel":       newReplicaBackgroundAdmissionGate(),
				"original_control_poll": newReplicaBackgroundAdmissionGate(),
			}
			store, addresses := replicaBackgroundBridgePair(t, runtimeDB, "pod_background", gates)
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
			actions := []map[string]any{bridgeChildAction("expired", 0, method, bridgeChildRequest(t, request), 5000, codes.DeadlineExceeded), bridgeChildAction("rejoin", 1, method, bridgeChildRequest(t, request), 5000), bridgeChildAction("replay", 0, method, bridgeChildRequest(t, request), 5000)}
			expectedReceipts, expectedCalls := 1, int32(1)
			if method == "cancelCommand" {
				actions = append([]map[string]any{bridgeChildAction("initial_poll", 0, "readCommandResult", bridgeChildRequest(t, &bridgev1.ReadCommandResultRequest{Scope: scope, TaskId: task, ToolUseEventId: toolID, OperationId: "original_control_poll"}), 5000, codes.DeadlineExceeded)}, actions...)
				expectedReceipts = 2
				expectedCalls = 2
			}
			// Five seconds is a fixture admission/caller guard, not an admission SLO.
			// Park only the postcommit wait and prove exact custody while the real
			// caller is alive; a failed admission cannot masquerade as preserved work.
			actions[len(actions)-2]["waitFor"] = "continue_after_expired"
			if method == "cancelCommand" {
				actions[1]["waitFor"] = "continue_after_initial_poll"
			}
			child := startReplicaBridgeChild(t, addresses, actions)
			if method == "cancelCommand" {
				assertReplicaBackgroundAdmissionExpiry(t, admin, child, gates["original_control_poll"], scope, toolID, task, "original_control_poll", "poll", "initial_poll", 1)
				if err := os.WriteFile(filepath.Join(child.directory, "continue_after_initial_poll"), nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			operation := request.(interface{ GetOperationId() string }).GetOperationId()
			receiptID, kind := toolID, "poll"
			if method == "sendCommandInput" {
				kind = "stdin"
			}
			if method == "cancelCommand" {
				receiptID, kind = "background_receipt:"+operation, "cancel"
			}
			assertReplicaBackgroundAdmissionExpiry(t, admin, child, gates[operation], scope, receiptID, task, operation, kind, "expired", expectedReceipts)
			var receipts int
			if err := admin.QueryRow(`SELECT count(*) FROM session_runtime_tool_results WHERE session_id=$1 AND background_operation_state='pending'`, session).Scan(&receipts); err != nil || receipts != expectedReceipts {
				t.Fatalf("original pending receipt=%d/%v", receipts, err)
			}
			if err := os.WriteFile(filepath.Join(child.directory, "continue_after_expired"), nil, 0600); err != nil {
				t.Fatal(err)
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

type heldCoreCaptureProvider struct {
	handoffCaptureProvider
	mu      sync.Once
	release chan struct{}
	calls   atomic.Int32
}

func (p *heldCoreCaptureProvider) finish() { p.mu.Do(func() { close(p.release) }) }
func (p *heldCoreCaptureProvider) CaptureOutputs(ctx context.Context, target sandboxdriver.OutputCaptureTarget) tetralsandbox.ProviderOutcome[sandboxdriver.OutputCaptureScan] {
	p.calls.Add(1)
	select {
	case <-p.release:
		return p.handoffCaptureProvider.CaptureOutputs(ctx, target)
	case <-ctx.Done():
		return tetralsandbox.ProviderOutcome[sandboxdriver.OutputCaptureScan]{Disposition: tetralsandbox.ProviderRetryable, ErrorKind: "fixture_cancelled", SafeMessage: "fixture stopped"}
	}
}

type coreCaptureRejoinStore struct {
	bridge.BridgeAPIStore
	active, maximum, joined atomic.Int32
	mu                      sync.Mutex
	requests                []*bridgev1.FinishIdleRequest
}

func (s *coreCaptureRejoinStore) FinishIdle(ctx context.Context, request *bridgev1.FinishIdleRequest) (*bridgev1.FinishIdleResponse, error) {
	s.mu.Lock()
	s.requests = append(s.requests, proto.Clone(request).(*bridgev1.FinishIdleRequest))
	s.mu.Unlock()
	active := s.active.Add(1)
	for maximum := s.maximum.Load(); active > maximum; maximum = s.maximum.Load() {
		if s.maximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	defer func() { s.active.Add(-1); s.joined.Add(1) }()
	return s.BridgeAPIStore.FinishIdle(ctx, request)
}
func startReplicaFinishIdleCoreChild(t *testing.T, address string, request json.RawMessage) *handoffRuntimeChild {
	t.Helper()
	child := &handoffRuntimeChild{directory: t.TempDir(), done: make(chan struct{})}
	input, err := json.Marshal(map[string]any{"address": address, "token": "runtime", "directory": child.directory, "request": request, "env": map[string]string{"TETRAL_BRIDGE_FINISH_IDLE_TIMEOUT_MS": "80"}})
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
	child.command = exec.Command("bun", "packages/runtime-pod/test/fixtures/replica-finish-idle-policy.ts", path) //nolint:gosec // Fixed repository child and test-owned input.
	child.command.Dir = "../services/agent-runtime"
	child.command.Stdout, child.command.Stderr = output, output
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
				t.Error("FinishIdle child cleanup did not join")
			}
		}
	})
	return child
}

// The tracer parks only the first read-only wait for the exact operation. The
// admission transaction has committed before this marker; no fixture write or
// server-generated status substitutes for actual caller expiry.
type replicaBackgroundAdmissionGate struct {
	entered               chan context.Context
	release               chan struct{}
	returned              chan struct{}
	parkOnce, releaseOnce sync.Once
	calls                 atomic.Int32
}

func newReplicaBackgroundAdmissionGate() *replicaBackgroundAdmissionGate {
	return &replicaBackgroundAdmissionGate{entered: make(chan context.Context, 1), release: make(chan struct{}), returned: make(chan struct{})}
}
func (g *replicaBackgroundAdmissionGate) resume() { g.releaseOnce.Do(func() { close(g.release) }) }

type replicaBackgroundGateContextKey struct{}
type replicaBackgroundQueryContextKey struct{}
type replicaBackgroundAdmissionTracer struct{}

func (replicaBackgroundAdmissionTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, replicaBackgroundQueryContextKey{}, strings.Contains(data.SQL, "/* runtime receipt scope validation */"))
}
func (replicaBackgroundAdmissionTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	matched, _ := ctx.Value(replicaBackgroundQueryContextKey{}).(bool)
	gate, _ := ctx.Value(replicaBackgroundGateContextKey{}).(*replicaBackgroundAdmissionGate)
	if matched && gate != nil && data.Err == nil {
		gate.parkOnce.Do(func() { gate.entered <- ctx; <-gate.release })
	}
}

func replicaBackgroundBridgePair(t *testing.T, db *sql.DB, podUID string, gates map[string]*replicaBackgroundAdmissionGate) (*bridge.PostgreSQLBridgeAPIStore, []string) {
	t.Helper()
	first := bridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(storagetest.OpenRuntimeRoleDBWithTracer(t, db, replicaBackgroundAdmissionTracer{})))
	second := bridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(storagetest.OpenRuntimeRoleDBWithTracer(t, db, nil)))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var handlers sync.WaitGroup
	var admission sync.Mutex
	stopping := false
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		admission.Lock()
		if stopping {
			admission.Unlock()
			return nil, status.Error(codes.Unavailable, "background fixture is stopping")
		}
		handlers.Add(1)
		admission.Unlock()
		defer handlers.Done()
		md, _ := metadata.FromIncomingContext(ctx)
		values := md.Get("authorization")
		if len(values) != 1 || values[0] != "Bearer runtime" {
			return nil, status.Error(codes.Unauthenticated, "verified fixture identity required")
		}
		identity := auth.Identity{ServiceAccount: auth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"}, KubernetesPodUID: podUID}
		if err := bridge.BridgeAPIMethodAuthorizer(identity, info.FullMethod); err != nil {
			return nil, err
		}
		if operation, ok := request.(interface{ GetOperationId() string }); ok {
			if gate := gates[operation.GetOperationId()]; gate != nil && gate.calls.Add(1) == 1 {
				ctx = context.WithValue(ctx, replicaBackgroundGateContextKey{}, gate)
				defer close(gate.returned)
			}
		}
		return handler(auth.ContextWithIdentity(ctx, identity), request)
	}))
	bridge.RegisterBridgeAPI(server, first)
	joined := make(chan struct{})
	go func() { defer close(joined); _ = server.Serve(listener) }()
	// The child uses actual generated clients; this first endpoint needs no
	// extra Go channel. Second-replica authentication remains the shared seam.
	t.Cleanup(func() {
		admission.Lock()
		stopping = true
		admission.Unlock()
		for _, gate := range gates {
			gate.resume()
		}
		server.Stop()
		_ = listener.Close()
		handlersJoined := make(chan struct{})
		go func() { handlers.Wait(); close(handlersJoined) }()
		select {
		case <-handlersJoined:
		case <-time.After(5 * time.Second):
			t.Error("background Bridge handlers did not join")
		}
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("background Bridge server did not join")
		}
	})
	b := serveReplicaBridge(t, second, map[string]string{"runtime": podUID}, nil)
	return first, []string{listener.Addr().String(), b.Address}
}

func replicaBackgroundCustody(ctx context.Context, t *testing.T, admin *sql.DB, session string) string {
	t.Helper()
	var snapshot string
	if err := admin.QueryRowContext(ctx, `SELECT jsonb_build_object(
	 'receipts',(SELECT jsonb_agg(to_jsonb(r) ORDER BY tool_use_event_id) FROM session_runtime_tool_results r WHERE session_id=$1),
	 'tasks',(SELECT jsonb_agg(to_jsonb(b) ORDER BY task_id) FROM session_background_tasks b WHERE session_id=$1),
	 'jobs',(SELECT jsonb_agg(to_jsonb(j) ORDER BY id) FROM queue_jobs j WHERE kind=$2 AND payload_json::jsonb->>'session_id'=$1))::text`, session, queue.KindSandboxBackgroundCommand).Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func assertReplicaBackgroundAdmissionExpiry(t *testing.T, admin *sql.DB, child *handoffRuntimeChild, gate *replicaBackgroundAdmissionGate, scope *bridgev1.RuntimeScope, receiptID, taskID, operationID, kind, action string, totalJobs int) {
	t.Helper()
	var caller context.Context
	waitHandoffCondition(t, "postcommit background wait "+operationID, func() bool {
		select {
		case caller = <-gate.entered:
			return true
		default:
		}
		if raw, err := os.ReadFile(filepath.Join(child.directory, "results.json")); err == nil {
			var results map[string]json.RawMessage
			if json.Unmarshal(raw, &results) == nil && results[action] != nil {
				t.Fatalf("actual caller returned before committed admission boundary for %s", operationID)
			}
		}
		select {
		case <-child.done:
			out, _ := os.ReadFile(filepath.Join(child.directory, "output.log"))
			t.Fatalf("background admission child exited %v: %s", child.err, out)
		default:
		}
		return false
	})
	defer gate.resume()
	var state, storedKind, storedOperation, storedTask string
	if err := admin.QueryRowContext(caller, `SELECT background_operation_state,background_operation_kind,background_request_id,background_task_id FROM session_runtime_tool_results
	 WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND tool_use_event_id=$4`, scope.WorkspaceId, scope.SessionId, scope.SessionThreadId, receiptID).Scan(&state, &storedKind, &storedOperation, &storedTask); err != nil || state != "pending" || storedKind != kind || storedOperation != operationID || storedTask != taskID {
		t.Fatalf("exact admitted receipt=%s/%s/%s/%s/%v", state, storedKind, storedOperation, storedTask, err)
	}
	var jobs, matching int
	var jobID string
	if err := admin.QueryRowContext(caller, `SELECT count(*),count(*) FILTER(WHERE workspace_id=$2 AND payload_json::jsonb->>'session_id'=$3 AND payload_json::jsonb->>'task_id'=$4 AND payload_json::jsonb->>'request_id'=$5 AND status='pending'),
	 COALESCE(min(id) FILTER(WHERE workspace_id=$2 AND payload_json::jsonb->>'session_id'=$3 AND payload_json::jsonb->>'task_id'=$4 AND payload_json::jsonb->>'request_id'=$5),'') FROM queue_jobs WHERE kind=$1`, queue.KindSandboxBackgroundCommand, scope.WorkspaceId, scope.SessionId, taskID, operationID).Scan(&jobs, &matching, &jobID); err != nil || jobs != totalJobs || matching != 1 || jobID == "" {
		t.Fatalf("exact admitted Queue=%d/%d/%s/%v", jobs, matching, jobID, err)
	}
	before := replicaBackgroundCustody(caller, t, admin, scope.SessionId)
	if err := caller.Err(); err != nil {
		t.Fatalf("caller expired before independent exact admission proof: %v", err)
	}
	t.Logf("background phase admitted operation=%s receipt=%s queue=%s kind=%s", operationID, receiptID, jobID, kind)
	bridgeChildResult(t, child, action) // Actual generated client must consume DeadlineExceeded.
	gate.resume()
	select {
	case <-gate.returned:
	case <-time.After(5 * time.Second):
		t.Fatal("expired first background handler did not join")
	}
	if after := replicaBackgroundCustody(context.Background(), t, admin, scope.SessionId); after != before {
		t.Fatal("caller expiry changed accepted receipt/task/Queue custody")
	}
	t.Logf("background phase expired-and-joined operation=%s queue=%s", operationID, jobID)
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
