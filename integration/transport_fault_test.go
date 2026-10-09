package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/integration/transporttest"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"

	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
)

type transportMCPExternal struct {
	URL      string
	calls    atomic.Int32
	admitted chan struct{}
}

func serveTransportMCPExternal(t *testing.T) *transportMCPExternal {
	t.Helper()
	fixture := &transportMCPExternal{admitted: make(chan struct{}, 16)}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			http.Error(w, "stateless", http.StatusMethodNotAllowed)
			return
		}
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusOK)
			return
		}
		var message struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
			http.Error(w, "invalid", 400)
			return
		}
		if len(message.ID) == 0 {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		var result any
		switch message.Method {
		case "initialize":
			result = map[string]any{"protocolVersion": "2025-03-26", "capabilities": map[string]any{"tools": map[string]any{}}, "serverInfo": map[string]any{"name": "controlled-business-tool", "version": "1"}}
		case "tools/list":
			result = map[string]any{"tools": []any{map[string]any{"name": "create_issue", "inputSchema": map[string]any{"type": "object"}}}}
		case "tools/call":
			fixture.calls.Add(1)
			fixture.admitted <- struct{}{}
			result = map[string]any{"content": []any{map[string]any{"type": "text", "text": "original external result"}}}
		default:
			http.Error(w, "unknown", 400)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": message.ID, "result": result})
	}))
	fixture.URL = server.URL
	t.Cleanup(server.Close)
	return fixture
}
func seedTransportMCP(t *testing.T, runtimeDB, admin *sql.DB) (*bridge.PostgreSQLBridgeAPIStore, *bridgev1.RuntimeScope, string) {
	t.Helper()
	seedMCPFamilySession(t, admin, "sesn_transport_mcp", "thr_transport_mcp", "claude")
	// Restore the configured catalog alongside the seeded pending declaration.
	// The cold Core must resolve the original MCP tool through normal policy.
	if _, err := admin.Exec(`INSERT INTO session_mcp_manifests
		(workspace_id, session_id, mcp_server_name, tools_json, manifest_etag, manifest_generation, created_at, updated_at)
		VALUES ('default', 'sesn_transport_mcp', 'github', '[{"name":"create_issue","description":"create issue","input_schema":{"type":"object"}}]', 'etag_transport_mcp', 1, now(), now())`); err != nil {
		t.Fatal(err)
	}
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_transport_mcp", "bind_transport_mcp", 1, "pod_transport")
	store := bridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("replica-handoff-shared-token-signing-key")
	scope := sessionfixture.BridgeAPIScope("sesn_transport_mcp", "thr_transport_mcp", "bind_transport_mcp", 1, "pod_transport")
	seedBridgeAPIOpenDurableTurn(t, admin, scope, "turn_transport_mcp")
	seedBridgeAPIRequestStart(t, store, scope, "start_transport_mcp", "request_transport_mcp", "agent_provider_request", 0)
	serverName := "github"
	declaration := sessionfixture.BridgeToolDeclarationForTest("call_transport_mcp", "create_issue", `{"title":"original"}`, "allow", "mcp_execute")
	declaration.EventKind = bridgev1.RuntimeToolEventKind_RUNTIME_TOOL_EVENT_KIND_MCP
	declaration.McpServerName = &serverName
	response, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{Scope: scope, RuntimeWriteId: "write_transport_mcp", ModelRequestId: "request_transport_mcp", ToolDeclaration: declaration})
	if err != nil || response.GetCommitted() == nil {
		t.Fatalf("MCP declaration %v/%v", response, err)
	}
	eventID := response.GetCommitted().EventId
	end, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{Scope: scope, RuntimeWriteId: "end_transport_mcp", ModelRequestId: "request_transport_mcp", FinishReason: "tool-calls", UsageJson: `{}`, ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "completed", AssistantMessageSequence: response.GetCommitted().AssignedMessageSequence, ToolUseEventIds: []string{eventID}}})
	if err != nil || end.GetCommitted() == nil {
		t.Fatalf("MCP request seal %v/%v", end, err)
	}
	return store, scope, eventID
}
func startTransportMCPChild(t *testing.T, addresses []string, external string, scope *bridgev1.RuntimeScope, eventID, bootID string, retry bool) *handoffRuntimeChild {
	t.Helper()
	child := &handoffRuntimeChild{directory: t.TempDir(), done: make(chan struct{})}
	input, err := json.Marshal(map[string]any{"directory": child.directory, "bridges": addresses, "externalURL": external, "claimID": "claim_transport_original", "bootID": bootID, "retryReceiver": retry, "podUID": scope.Binding.TargetPodUid, "request": map[string]any{"workspaceId": scope.WorkspaceId, "sessionId": scope.SessionId, "sessionThreadId": scope.SessionThreadId, "bindingId": scope.Binding.BindingId, "bindingGeneration": scope.Binding.BindingGeneration, "runtimeProcessId": scope.Binding.RuntimeProcessId, "toolUseEventId": eventID}})
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
	child.command = exec.Command("bun", "packages/mcp-connector/test/fixtures/transport-business-fault.ts", path) //nolint:gosec // Fixed source and test-owned arguments.
	child.command.Dir = "../services/gateway"
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
			case <-time.After(3 * time.Second):
				t.Error("MCP child cleanup failed to join")
			}
		}
	})
	rawReady := waitTransportMCPFile(t, child, "ready.json")
	var ready struct {
		Port   int    `json:"port"`
		PID    int    `json:"pid"`
		BootID string `json:"bootID"`
	}
	if err := json.Unmarshal(rawReady, &ready); err != nil || ready.Port == 0 || ready.PID != child.command.Process.Pid || ready.BootID != bootID {
		t.Fatalf("MCP ready %s/%v", rawReady, err)
	}
	child.port = ready.Port
	return child
}
func waitTransportMCPFile(t *testing.T, child *handoffRuntimeChild, name string) []byte {
	t.Helper()
	var found []byte
	waitHandoffCondition(t, "MCP child "+name, func() bool {
		raw, err := os.ReadFile(filepath.Join(child.directory, name))
		if err == nil {
			found = raw
			return true
		}
		select {
		case <-child.done:
			out, _ := os.ReadFile(filepath.Join(child.directory, "output.log"))
			t.Fatalf("MCP child exited before %s: %v\n%s", name, child.err, out)
		default:
		}
		return false
	})
	return found
}
func transportMCPResponse(t *testing.T, child *handoffRuntimeChild) string {
	t.Helper()
	raw := waitTransportMCPFile(t, child, "result.json")
	var result struct {
		Result struct {
			Response *struct {
				Status     int    `json:"status"`
				ResultText string `json:"resultText"`
			} `json:"response"`
			Code int `json:"code"`
		} `json:"result"`
		DurationMS     float64           `json:"durationMs"`
		CommitRequests []json.RawMessage `json:"commitRequests"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	if result.DurationMS > 3000 {
		t.Fatalf("finite caller exceeded deadline + tolerance: %s", raw)
	}
	if result.Result.Response == nil {
		t.Fatalf("MCP caller did not consume durable response: %s", raw)
	}
	if result.Result.Response.Status != 1 || !strings.Contains(result.Result.Response.ResultText, "original external result") {
		t.Fatalf("MCP durable response changed: %s", raw)
	}
	for i, request := range result.CommitRequests {
		if i == 0 {
			continue
		}
		if string(request) != string(result.CommitRequests[0]) {
			t.Fatalf("named commit replay changed bytes: %s", raw)
		}
	}
	return result.Result.Response.ResultText
}
func assertTransportMCPStored(t *testing.T, admin *sql.DB, eventID string) string {
	t.Helper()
	var count int
	var result string
	if err := admin.QueryRow(`SELECT count(*),coalesce(min(result_json),'') FROM session_runtime_tool_results WHERE tool_use_event_id=$1 AND mcp_claim_status IN ('stored','consumed')`, eventID).Scan(&count, &result); err != nil || count != 1 || !strings.Contains(result, "original external result") {
		t.Fatalf("independent committed SQL oracle count=%d result=%s err=%v", count, result, err)
	}
	var receipts int
	if err := admin.QueryRow(`SELECT count(*) FROM session_bridge_operations WHERE operation='commit_mcp_tool_result'`).Scan(&receipts); err != nil || receipts != 1 {
		t.Fatalf("named commit receipt count=%d error=%v", receipts, err)
	}
	return result
}

// transportMCPBeforeStore holds each commit before the production store method,
// which it never calls, until the receiver observes the caller's cancellation.
type transportMCPBeforeStore struct {
	bridge.BridgeAPIStore
	reached           chan struct{}
	entered, returned atomic.Int32
}

func (s *transportMCPBeforeStore) CommitMcpToolResult(ctx context.Context, r *bridgev1.CommitMcpToolResultRequest) (*bridgev1.CommitMcpToolResultResponse, error) {
	s.entered.Add(1)
	defer s.returned.Add(1)
	select {
	case s.reached <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return nil, status.FromContextError(ctx.Err()).Err()
}
func TestPostgreSQLTransportCommittedResponseLoss(t *testing.T) {
	// This reset run is also the reset variant of transport disconnect
	// recovery; TestPostgreSQLTransportDisconnectRecovery adds the blackhole.
	t.Run("commit response reset reconciles through independent receiver", func(t *testing.T) { runTransportMCPCommittedFault(t, transporttest.FaultReset) })
	t.Run("before commit cancellation does not invoke store", func(t *testing.T) {
		runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		store, scope, eventID := seedTransportMCP(t, runtimeDB, admin)
		external := serveTransportMCPExternal(t)
		before := &transportMCPBeforeStore{BridgeAPIStore: store, reached: make(chan struct{}, 1)}
		a := serveReplicaMCPBridge(t, before, "mcp", nil)
		child := startTransportMCPChild(t, []string{a.Address, a.Address}, external.URL, scope, eventID, "before", false)
		child.signal(t, "run")
		select {
		case <-before.reached:
		case <-time.After(3 * time.Second):
			t.Fatal("before-commit barrier not reached")
		}
		raw := waitTransportMCPFile(t, child, "result.json")
		resultAt := time.Now()
		var outcome struct {
			Result struct {
				Response json.RawMessage `json:"response"`
				Code     int             `json:"code"`
			} `json:"result"`
			DurationMS     float64           `json:"durationMs"`
			CommitRequests []json.RawMessage `json:"commitRequests"`
		}
		if err := json.Unmarshal(raw, &outcome); err != nil {
			t.Fatal(err)
		}
		// The caller gets a bounded failure, never a fabricated result.
		if outcome.Result.Response != nil || codes.Code(outcome.Result.Code) == codes.OK || outcome.DurationMS > 3000 {
			t.Fatalf("before-commit caller outcome %s", raw)
		}
		if len(outcome.CommitRequests) == 0 {
			t.Fatalf("no commit attempt reached the receiver: %s", raw)
		}
		for _, request := range outcome.CommitRequests[1:] {
			if string(request) != string(outcome.CommitRequests[0]) {
				t.Fatalf("same-identity commit retry changed bytes: %s", raw)
			}
		}
		// Every receiver entry observed the cancellation and returned within
		// one second of the caller's result.
		joinedCtx, cancelJoined := context.WithDeadline(context.Background(), resultAt.Add(time.Second))
		defer cancelJoined()
		if err := transporttest.Await(joinedCtx, func() bool {
			entered := before.entered.Load()
			return entered >= 1 && before.returned.Load() == entered
		}); err != nil {
			t.Fatalf("receiver commit entries entered=%d returned=%d did not join", before.entered.Load(), before.returned.Load())
		}
		child.signal(t, "shutdown")
		child.join(t)
		var committed, receipts int
		if err := admin.QueryRow(`SELECT (SELECT count(*) FROM session_runtime_tool_results WHERE mcp_claim_status IN ('stored','consumed')),(SELECT count(*) FROM session_bridge_operations WHERE operation='commit_mcp_tool_result')`).Scan(&committed, &receipts); err != nil || committed != 0 || receipts != 0 || external.calls.Load() != 1 {
			t.Fatalf("before-commit control committed=%d receipts=%d external=%d err=%v", committed, receipts, external.calls.Load(), err)
		}
	})
}
func runTransportMCPCommittedFault(t *testing.T, mode transporttest.FaultMode) {
	t.Helper()
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	store, scope, eventID := seedTransportMCP(t, runtimeDB, admin)
	external := serveTransportMCPExternal(t)
	reached, release, joined := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	var releaseOnce sync.Once
	releaseBarrier := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseBarrier)
	a := serveReplicaMCPBridge(t, store, "mcp", func(ctx context.Context, method string, _ any) error {
		if method == bridgev1.AgentRuntimeBridgeService_CommitMcpToolResult_FullMethodName && once.CompareAndSwap(false, true) {
			close(reached)
			defer close(joined)
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
		return nil
	})
	second := bridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	b := serveReplicaMCPBridge(t, second, "mcp", nil)
	forwarder, err := transporttest.NewFaultForwarder(context.Background(), a.Address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		releaseBarrier()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := forwarder.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	child := startTransportMCPChild(t, []string{forwarder.Address, b.Address}, external.URL, scope, eventID, "initial", true)
	child.signal(t, "run")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	connection, err := forwarder.AwaitConnection(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = forwarder.Arm(eventID, connection); err != nil {
		t.Fatal(err)
	}
	select {
	case <-reached:
	case <-ctx.Done():
		t.Fatal("committed response barrier not reached")
	}
	original := assertTransportMCPStored(t, admin, eventID)
	if err = forwarder.Reached(eventID); err != nil {
		t.Fatal(err)
	}
	if err = forwarder.Fault(eventID, mode); err != nil {
		t.Fatal(err)
	}
	releaseBarrier()
	if err = forwarder.Release(eventID); err != nil {
		t.Fatal(err)
	}
	_ = transportMCPResponse(t, child)
	child.signal(t, "shutdown")
	child.join(t)
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("owned server callback did not join")
	}
	snapshot, err := forwarder.Snapshot(eventID)
	if err != nil || snapshot.ConnectionID != connection.ID || snapshot.Mode != mode || len(snapshot.Events) != 4 {
		t.Fatalf("selected actual transport fault %+v/%v", snapshot, err)
	}
	if mode == transporttest.FaultReset && (!snapshot.ClientClosed || !snapshot.ServerClosed) {
		t.Fatal("reset did not close both sockets")
	}
	if mode == transporttest.FaultBlackhole && (snapshot.DiscardedServerBytes == 0 || snapshot.DiscardedClientBytes == 0) {
		t.Fatalf("blackhole did not discard both directions %+v", snapshot)
	}
	replay := startTransportMCPChild(t, []string{b.Address, b.Address}, external.URL, scope, eventID, "fresh", true)
	replay.signal(t, "run")
	_ = transportMCPResponse(t, replay)
	replay.signal(t, "shutdown")
	replay.join(t)
	if external.calls.Load() != 1 || assertTransportMCPStored(t, admin, eventID) != original {
		t.Fatalf("receipt recovery repeated effect %d", external.calls.Load())
	}
	changed := &bridgev1.CommitMcpToolResultRequest{Scope: proto.Clone(scope).(*bridgev1.RuntimeScope), ToolUseEventId: eventID, ClaimId: "claim_transport_original", ResultJson: `{"response":{"status":1,"resultText":"changed","attachments":[]},"contentItems":1,"refreshTriggered":false}`}
	if _, err := b.Client.CommitMcpToolResult(replicaRuntimeContext(context.Background(), "mcp"), changed); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("changed receipt payload accepted %v", err)
	}
	changed.Scope.Binding.RuntimeProcessId = "unregistered"
	if denied, err := b.Client.CommitMcpToolResult(replicaRuntimeContext(context.Background(), "mcp"), changed); err != nil || denied.GetStale() == nil {
		t.Fatalf("changed process identity must produce exact stale union: %v/%v", denied, err)
	}
}

// The reset variant runs once, as the commit-response reset subtest of
// TestPostgreSQLTransportCommittedResponseLoss: the scenario and assertions are
// the same, so it is not repeated here.
func TestPostgreSQLTransportDisconnectRecovery(t *testing.T) {
	t.Run(string(transporttest.FaultBlackhole), func(t *testing.T) { runTransportMCPCommittedFault(t, transporttest.FaultBlackhole) })
}
func TestPostgreSQLTransportProcessRestart(t *testing.T) {
	for _, mode := range []string{"graceful", "forced", "abrupt"} {
		t.Run(mode, func(t *testing.T) {
			runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			store, scope, eventID := seedTransportMCP(t, runtimeDB, admin)
			external := serveTransportMCPExternal(t)
			reached, release := make(chan struct{}), make(chan struct{})
			var once atomic.Bool
			var released sync.Once
			releaseBarrier := func() { released.Do(func() { close(release) }) }
			t.Cleanup(releaseBarrier)
			a := serveReplicaMCPBridge(t, store, "mcp", func(ctx context.Context, method string, _ any) error {
				if method == bridgev1.AgentRuntimeBridgeService_CommitMcpToolResult_FullMethodName && (mode == "forced" || !once.Load()) {
					if once.CompareAndSwap(false, true) {
						close(reached)
					}
					select {
					case <-release:
					case <-ctx.Done():
					}
				}
				return nil
			})
			b := serveReplicaMCPBridge(t, bridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB)), "mcp", nil)
			old := startTransportMCPChild(t, []string{a.Address, a.Address}, external.URL, scope, eventID, "boot_old", false)
			old.signal(t, "run")
			select {
			case <-reached:
			case <-time.After(3 * time.Second):
				t.Fatal("selected child admission absent")
			}
			original := assertTransportMCPStored(t, admin, eventID)
			if mode == "abrupt" {
				_ = old.command.Process.Kill()
				select {
				case <-old.done:
					if old.err == nil {
						t.Fatal("killed child exited successfully")
					}
				case <-time.After(3 * time.Second):
					t.Fatal("killed child not joined")
				}
			} else {
				old.signal(t, "shutdown")
				waitTransportMCPFile(t, old, "draining.json")
				if mode == "graceful" {
					releaseBarrier()
				}
				old.join(t)
			}
			releaseBarrier()
			replacement := startTransportMCPChild(t, []string{b.Address, b.Address}, external.URL, scope, eventID, "boot_replacement", true)
			replacement.signal(t, "run")
			_ = transportMCPResponse(t, replacement)
			proveTransportMCPRuntimeContinuation(t, runtimeDB, admin, scope, replacement)
			replacement.signal(t, "shutdown")
			replacement.join(t)
			if external.calls.Load() != 1 || assertTransportMCPStored(t, admin, eventID) != original {
				t.Fatal("process replacement repeated committed external work")
			}
		})
	}
}

// The real cold Runtime rejoins the original turn and sends its durable result
// through the next provider request before connector resources close.
func proveTransportMCPRuntimeContinuation(t *testing.T, runtimeDB, admin *sql.DB, scope *bridgev1.RuntimeScope, mcp *handoffRuntimeChild) {
	t.Helper()
	store := bridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("replica-handoff-shared-token-signing-key")
	startHandoffResultListener(t, store)
	sessionfixture.SeedRuntimePodLostStatusFence(t, admin, scope.SessionId, scope.Binding.BindingId, scope.Binding.BindingGeneration)
	endpoint := serveReplicaBridge(t, store, map[string]string{"old": scope.Binding.TargetPodUid, "new": "pod_new"}, nil)
	// The selected connector replacement leaves the original Runtime binding
	// intact. A fresh local Core host loads that same current process and admits
	// a real business input, restoring its already committed MCP result first.
	runtime := startHandoffRuntimeChild(t, endpoint.Address, scope.Binding.TargetPodUid, scope.Binding.RuntimeProcessId, "old", true, nil, "", fmt.Sprintf("127.0.0.1:%d", mcp.port))
	t.Cleanup(func() {
		if t.Failed() {
			for _, name := range []string{"output.log", "diagnostics.jsonl", "ledger.json"} {
				data, _ := os.ReadFile(filepath.Join(runtime.directory, name))
				t.Logf("continuation %s: %s", name, data)
			}
			var queueState string
			if err := admin.QueryRow(`SELECT coalesce(jsonb_agg(jsonb_build_object('kind',kind,'status',status,'error_kind',last_error_kind)),'[]'::jsonb)::text FROM queue_jobs`).Scan(&queueState); err == nil {
				t.Logf("continuation queue states: %s", queueState)
			}
		}
	})
	appendHandoffMessage(t, dbconnect.NewClientForTesting(runtimeDB), scope.SessionId, "after_connector_restart")
	deliverMCPContinuationInput(t, runtimeDB, admin, runtime.port, scope)
	waitHandoffCondition(t, "original MCP durable result reaches next actual provider", func() bool {
		select {
		case <-runtime.done:
			out, _ := os.ReadFile(filepath.Join(runtime.directory, "output.log"))
			diag, _ := os.ReadFile(filepath.Join(runtime.directory, "diagnostics.jsonl"))
			t.Fatalf("cold Runtime exited %v %s %s", runtime.err, out, diag)
		default:
		}
		if runtime.calls(scope.SessionId) != 1 {
			diag, _ := os.ReadFile(filepath.Join(runtime.directory, "diagnostics.jsonl"))
			if strings.Contains(string(diag), "session_failed") || strings.Contains(string(diag), "runtime_context_load_parse_failed") {
				t.Fatalf("cold Runtime diagnostics %s", diag)
			}
		}
		return runtime.calls(scope.SessionId) == 1
	})
	rawLedger, err := os.ReadFile(filepath.Join(runtime.directory, "ledger.json"))
	if err != nil {
		t.Fatal(err)
	}
	var ledger []struct {
		MessagesJSON string `json:"messagesJson"`
	}
	if err := json.Unmarshal(rawLedger, &ledger); err != nil {
		t.Fatal(err)
	}
	if len(ledger) != 1 || !strings.Contains(ledger[0].MessagesJSON, "original external result") {
		t.Fatalf("cold provider missed original MCP result %+v", ledger)
	}
	runtime.signal(t, "quiesce")
	runtime.signal(t, scope.SessionId+"-1.release")
	runtime.join(t)
	var results, ends int
	if err := admin.QueryRow(`SELECT (SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.mcp_tool_result'),(SELECT count(*) FROM session_events WHERE session_id=$1 AND type='span.model_request_end' AND payload_json::jsonb->>'is_error'='false')`, scope.SessionId).Scan(&results, &ends); err != nil || results != 1 || ends != 2 {
		t.Fatalf("complete Runtime result/continuation %d/%d/%v", results, ends, err)
	}
}

type continuationDeliverer struct {
	jobrunner.RuntimePodDirectDeliverer
	result jobrunner.RuntimeDeliveryResult
	err    error
}

func (d *continuationDeliverer) DeliverRuntimeJob(ctx context.Context, job jobrunner.RuntimeJob) (jobrunner.RuntimeDeliveryResult, error) {
	d.result, d.err = d.RuntimePodDirectDeliverer.DeliverRuntimeJob(ctx, job)
	return d.result, d.err
}

// A Queue attempt is not proof of Runtime admission: retain the actual
// production deliverer's outcome before waiting for the provider boundary.
func deliverMCPContinuationInput(t *testing.T, runtimeDB, admin *sql.DB, port int, scope *bridgev1.RuntimeScope) {
	t.Helper()
	if _, err := admin.Exec(`UPDATE session_runtime_bindings SET agent_runtime_pod_ip='127.0.0.1' WHERE workspace_id=$1 AND session_id=$2`, scope.WorkspaceId, scope.SessionId); err != nil {
		t.Fatal(err)
	}
	client := dbconnect.NewClientForTesting(runtimeDB)
	store := jobrunner.NewPostgreSQLRuntimeDeliveryStore(client, port, jobrunner.KubernetesRuntimeTargetResolver{LoadClient: fixtureRuntimeLoadClient(t), Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: scope.Binding.TargetPodUid, PodIP: "127.0.0.1"}})
	}})
	deliverer := &continuationDeliverer{RuntimePodDirectDeliverer: jobrunner.RuntimePodDirectDeliverer{Store: store, Sender: fixtureRuntimeCommandClient(t, attachmentRuntimeTokenSource{})}}
	runner := &jobrunner.JobRunner{Queue: tetralqueue.NewServer(queue.NewPostgreSQLStore(client), nil), Deliverer: deliverer, Config: jobrunner.JobRunnerConfig{LeaseOwner: "mcp-continuation", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour}}
	if active, err := acquireAndJoinJobRunnerActive(context.Background(), runner); err != nil || !active {
		t.Fatalf("continuation Runner attempt active=%t: %v", active, err)
	}
	if deliverer.err != nil || (deliverer.result.Status != jobrunner.RuntimeDeliveryAccepted && deliverer.result.Status != jobrunner.RuntimeDeliveryDuplicate) {
		t.Fatalf("continuation actual Runtime admission: result=%+v error=%v", deliverer.result, deliverer.err)
	}
}
