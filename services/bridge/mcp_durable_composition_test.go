package agentruntimebridge

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/encryption"
	"github.com/tetral-ai/tetral/internal/internalgrpc"
	internalgrpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/mcpmanifest"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

const mcpDurableKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

type mcpDurableComposition struct {
	t                 *testing.T
	admin             *sql.DB
	store             *PostgreSQLBridgeAPIStore
	scope             *bridgev1.RuntimeScope
	bridge            *mcpDurableBridgeServer
	bridgeReplicaTwo  *mcpDurableBridgeServer
	controlAddress    string
	ctx               context.Context
	client            *http.Client
	sequence          int
	versions          map[string]string
	executionBudgetMS int
	gatewayUser       string
}

func newMCPDurableComposition(t *testing.T) *mcpDurableComposition {
	return newMCPDurableCompositionWithOptions(t, nil)
}
func newMCPDurableCompositionWithOptions(t *testing.T, options map[string]any) *mcpDurableComposition {
	t.Helper()
	bun, err := exec.LookPath("bun")
	if err != nil {
		t.Fatalf("durable MCP composition requires Bun: %v", err)
	}
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	roles := storagetest.OpenWorkloadDB(t, admin, "bridge")
	gatewayDB := roles.OpenWorkload(t, "mcp_connector", nil)
	var bridgeUser, gatewayUser string
	if err := roles.DB.QueryRow("SELECT current_user").Scan(&bridgeUser); err != nil {
		t.Fatal(err)
	}
	if err := gatewayDB.QueryRow("SELECT current_user").Scan(&gatewayUser); err != nil {
		t.Fatal(err)
	}
	if bridgeUser == gatewayUser {
		t.Fatal("Bridge and MCP Connector serving roles must differ")
	}
	for _, serving := range []*sql.DB{roles.DB, gatewayDB} {
		var unsafe, sessionRead bool
		if err := serving.QueryRow(`SELECT rolsuper OR rolbypassrls, has_table_privilege(current_user,'sessions','SELECT') FROM pg_roles WHERE rolname=current_user`).Scan(&unsafe, &sessionRead); err != nil {
			t.Fatal(err)
		}
		if unsafe || !sessionRead {
			t.Fatal("installed serving role posture is invalid")
		}
	}
	var gatewayEventWrite bool
	if err := gatewayDB.QueryRow(`SELECT has_table_privilege(current_user,'session_events','INSERT')`).Scan(&gatewayEventWrite); err != nil {
		t.Fatal(err)
	}
	if gatewayEventWrite {
		t.Fatal("MCP Connector must not write public events")
	}

	const sessionID, threadID, bindingID, podUID = "sesn_mcp_durable", "thr_mcp_durable", "bind_mcp_durable", "pod_mcp_durable"
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	installed := `{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"work-github"},{"type":"mcp_toolset","mcp_server_name":"work-slack"}],"mcp_servers":[{"type":"url","name":"work-github","url":"https://api.githubcopilot.com/mcp/"},{"type":"url","name":"work-slack","url":"https://mcp.slack.com/mcp"}]}`
	if _, err := admin.Exec(`UPDATE sessions SET installed_tools_json=$1,vault_ids_json='["vlt_mcp_durable"]' WHERE workspace_id='default' AND id=$2`, installed, sessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(`INSERT INTO vaults(workspace_id,id,display_name,metadata_json,created_at,updated_at) VALUES('default','vlt_mcp_durable','MCP fixture','{}',now(),now())`); err != nil {
		t.Fatal(err)
	}
	encryptor, err := encryption.NewAES256GCMEncryptor(mcpDurableKey)
	if err != nil {
		t.Fatal(err)
	}
	for _, adapter := range []string{"github", "slack"} {
		endpoint := "https://api.githubcopilot.com/mcp/"
		if adapter == "slack" {
			endpoint = "https://mcp.slack.com/mcp"
		}
		auth, err := json.Marshal(map[string]string{"type": "static_bearer", "mcp_server_url": endpoint, "token": "fixture-" + adapter + "-token"})
		if err != nil {
			t.Fatal(err)
		}
		encrypted, err := encryptor.Encrypt(auth)
		if err != nil {
			t.Fatal(err)
		}
		public, _ := json.Marshal(map[string]string{"type": "static_bearer", "mcp_server_url": endpoint})
		if _, err := admin.Exec(`INSERT INTO credentials(workspace_id,id,vault_id,display_name,metadata_json,auth_type,auth_public_json,mcp_server_url,encrypted_auth,created_at,updated_at) VALUES('default',$1,'vlt_mcp_durable',$1,'{}','static_bearer',$2,$3,$4,now(),now())`, "cred_mcp_"+adapter, string(public), endpoint, encrypted); err != nil {
			t.Fatal(err)
		}
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(roles.DB))
	store.RuntimeBindingTokenHMACKey = []byte("mcp-durable-binding-key-with-32-bytes")
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_mcp_durable_start", "mreq_mcp_durable", "agent_provider_request", 0)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	h := &mcpDurableComposition{t: t, admin: admin, store: store, scope: scope, ctx: ctx, client: &http.Client{Timeout: 30 * time.Second}}
	h.gatewayUser = gatewayUser
	h.executionBudgetMS = 170000
	if budget, ok := options["executionTimeoutMs"].(int); ok {
		h.executionBudgetMS = budget
	}
	h.bridge = &mcpDurableBridgeServer{store: store}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := internalgrpc.NewServer(internalgrpc.Config{ServiceName: "bridge-api-mcp-durable", Listener: listener, Authenticator: mcpCompositionBridgeAuthenticator{runtimePodUID: podUID}, MethodAuthorizer: BridgeAPIMethodAuthorizer, Register: func(server *grpc.Server) { bridgev1.RegisterAgentRuntimeBridgeServiceServer(server, h.bridge) }})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	recoveryAddress := ""
	if enabled, _ := options["secondBridgeReplica"].(bool); enabled {
		secondStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(roles.DB))
		secondStore.RuntimeBindingTokenHMACKey = append([]byte(nil), store.RuntimeBindingTokenHMACKey...)
		h.bridgeReplicaTwo = &mcpDurableBridgeServer{store: secondStore}
		secondListener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		secondServer, err := internalgrpc.NewServer(internalgrpc.Config{ServiceName: "bridge-api-mcp-recovery", Listener: secondListener, Authenticator: mcpCompositionBridgeAuthenticator{runtimePodUID: podUID}, MethodAuthorizer: BridgeAPIMethodAuthorizer, Register: func(server *grpc.Server) {
			bridgev1.RegisterAgentRuntimeBridgeServiceServer(server, h.bridgeReplicaTwo)
		}})
		if err != nil {
			t.Fatal(err)
		}
		go func() { _ = secondServer.Serve(secondListener) }()
		t.Cleanup(secondServer.Stop)
		recoveryAddress = secondListener.Addr().String()
	}
	dir := t.TempDir()
	gatewayTokenPath, runtimeTokenPath, bridgeTokenPath := filepath.Join(dir, "gateway-token"), filepath.Join(dir, "runtime-token"), filepath.Join(dir, "bridge-token")
	for path, token := range map[string]string{gatewayTokenPath: "mcp-production-gateway-token", runtimeTokenPath: "mcp-production-runtime-token", bridgeTokenPath: "mcp-durable-bridge-token"} {
		if err := os.WriteFile(path, []byte(token+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	inputPath := filepath.Join(dir, "composition.json")
	compositionInput := map[string]any{"bridgeAddress": listener.Addr().String(), "gatewayTokenPath": gatewayTokenPath, "runtimeTokenPath": runtimeTokenPath, "bridgeTokenPath": bridgeTokenPath, "workspaceId": "default", "sessionId": sessionID, "threadId": threadID, "bindingId": bindingID, "podUid": podUID, "masterKeyHex": mcpDurableKey, "bindingKey": string(store.RuntimeBindingTokenHMACKey)}
	for key, value := range options {
		compositionInput[key] = value
	}
	if recoveryAddress != "" {
		compositionInput["bridgeRecoveryAddress"] = recoveryAddress
	}
	input, _ := json.Marshal(compositionInput)
	if err := os.WriteFile(inputPath, input, 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(ctx, bun, "run", "packages/mcp-connector/test/fixtures/mcp-durable-composition.ts", inputPath) //nolint:gosec // repository fixture and owned input.
	command.Dir = filepath.Clean(filepath.Join("..", "gateway"))
	command.Env = append(os.Environ(), "TETRAL_TEST_GATEWAY_DATABASE_URL="+storagetest.RuntimeDatabaseURL(t, gatewayDB))
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if h.controlAddress != "" {
			if _, err := h.tryAction(map[string]any{"kind": "shutdown"}); err != nil {
				t.Errorf("MCP child shutdown request: %v", err)
			}
		}
		done := make(chan error, 1)
		go func() { done <- command.Wait() }()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("MCP child cleanup: %v; stderr=%s", err, stderr.String())
			}
			if err == nil {
				t.Logf("case_cleanup=%s", stderr.String())
			}
		case <-time.After(8 * time.Second):
			cancel()
			<-done
			t.Errorf("MCP child failed bounded cleanup: stderr=%s", stderr.String())
		}
	})
	var startup struct {
		ControlAddress   string            `json:"controlAddress"`
		ConnectorAddress string            `json:"connectorAddress"`
		Versions         map[string]string `json:"versions"`
	}
	if err := json.NewDecoder(stdout).Decode(&startup); err != nil {
		t.Fatalf("start MCP child: %v; stderr=%s", err, stderr.String())
	}
	h.controlAddress = startup.ControlAddress
	h.versions = startup.Versions
	h.versions["go"] = runtime.Version()
	if err := admin.QueryRow(`SELECT current_setting('server_version')`).Scan(&bridgeUser); err != nil {
		t.Fatal(err)
	}
	h.versions["postgresql"] = bridgeUser
	lister := mcpmanifest.NewConnectorLister(startup.ConnectorAddress, internalgrpcauth.FileTokenSource{Path: bridgeTokenPath})
	store.MCPManifestLister = lister
	t.Cleanup(func() { _ = lister.Close() })
	var receipt string
	if err := admin.QueryRow(`SELECT registration_receipt FROM runtime_processes WHERE namespace='tetral-agent-runtime' AND pod_uid=$1 AND runtime_process_id=$2`, podUID, "process_"+podUID).Scan(&receipt); err != nil {
		t.Fatal(err)
	}
	reportCtx, stopReport := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-reportCtx.Done():
				return
			case <-ticker.C:
				_, _, _ = runtimecontrol.ReportProcess(reportCtx, dbconnect.NewClientForTesting(roles.DB), runtimecontrol.ProcessIdentity{Namespace: "tetral-agent-runtime", PodUID: podUID, ID: "process_" + podUID}, receipt, runtimecontrol.ProcessAccepting)
			}
		}
	}()
	t.Cleanup(func() { stopReport(); <-done })
	return h
}

func (h *mcpDurableComposition) declare(server, nonce, permission string) (string, string) {
	return h.declareTool(server, "read_echo", nonce, permission)
}

func (h *mcpDurableComposition) declareTool(server, toolName, nonce, permission string) (string, string) {
	h.t.Helper()
	h.sequence++
	callID := fmt.Sprintf("call_mcp_durable_%d", h.sequence)
	response, err := h.store.WriteEvent(h.ctx, &bridgev1.WriteEventRequest{Scope: h.scope, RuntimeWriteId: fmt.Sprintf("rwrite_mcp_durable_%d", h.sequence), ModelRequestId: "mreq_mcp_durable", ToolDeclaration: bridgeMCPToolDeclarationForTest(callID, toolName, server, fmt.Sprintf(`{"nonce":%q}`, nonce), permission)})
	if err != nil || response.GetCommitted() == nil {
		h.t.Fatalf("declare exact MCP Tool Use: %+v/%v", response, err)
	}
	return response.GetCommitted().EventId, callID
}
func (h *mcpDurableComposition) tryAction(action map[string]any) (json.RawMessage, error) {
	body, err := json.Marshal(action)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(h.ctx, http.MethodPost, h.controlAddress, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	response, err := h.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := response.Body.Close(); err != nil {
			h.t.Errorf("close MCP child response: %v", err)
		}
	}()
	var result json.RawMessage
	err = json.NewDecoder(response.Body).Decode(&result)
	if err == nil && response.StatusCode != http.StatusOK {
		err = fmt.Errorf("fixture action status %d: %s", response.StatusCode, result)
	}
	return result, err
}
func (h *mcpDurableComposition) action(action map[string]any) json.RawMessage {
	h.t.Helper()
	result, err := h.tryAction(action)
	if err != nil {
		h.t.Fatal(err)
	}
	return result
}

func (h *mcpDurableComposition) evidence(caseID, variant string, output json.RawMessage, sqlFacts map[string]any) {
	h.t.Helper()
	var result map[string]json.RawMessage
	if err := json.Unmarshal(output, &result); err != nil {
		h.t.Fatal(err)
	}
	barriers := []string{"durable-declaration-ack-before-route", "actual-rpc-callback-before-sql-observe"}
	if caseID == "credential-scope" {
		barriers = []string{"installed-snapshot-workspace-rls", "actual-discovery-rpc-callback-before-observe", "encrypted-row-observe-before-fixture-restoration"}
	}
	if caseID == "manifest-notification" {
		barriers = []string{"two-actual-sdk-sse-notifications-before-bridge-acceptance", "actual-owner-callbacks-before-sql-observe"}
		if strings.HasSuffix(variant, "/unready-restore") {
			barriers = append(barriers, "retained-etag-tools-restored-under-acceptance-lock")
		} else {
			barriers = append(barriers, "two-actual-verification-rpc-responses-held-before-acceptance")
		}
		if strings.HasSuffix(variant, "/committed-ack-loss") {
			barriers = append(barriers, "actual-sql-commit-before-controlled-ack-loss")
		}
	}
	if caseID == "client-recreation" {
		barriers = append(barriers, "old-endpoint-handlers-disconnected-before-new-sdk-owner", "execution-readiness-published-before-bridge-verification")
	}
	if caseID == "oauth-refresh" {
		barriers = []string{"configured-server-resolver-before-credential-row-selection", "owned-operation-exit-before-sql-observe"}
		var issuerCalls int
		_ = json.Unmarshal(result["issuerCalls"], &issuerCalls)
		if issuerCalls > 0 {
			barriers = append(barriers, "actual-oauth-http-form-observed-before-sql-observe")
		}
		if !strings.Contains(variant, "discovery") {
			barriers = append(barriers, "original-durable-declaration-ack-before-execution", "original-runtime-settlement-receipt-before-observe")
		}
		if strings.HasSuffix(variant, "refresh-response-deadline") {
			barriers = append(barriers, "actual-held-issuer-request-abort-before-release")
		}
	}
	substitutions := []string{"controlled-mcp-http-peer", "workload-token-review-fixture"}
	if h.executionBudgetMS != 170000 {
		substitutions = append(substitutions, "shortened-execution-budget")
	}
	record := map[string]any{"case_id": caseID, "variant": variant, "versions": h.versions, "substitutions": substitutions, "execution_budget_ms": h.executionBudgetMS, "barriers": barriers, "endpoint_counts": result["counts"], "endpoint_trace": result["requests"], "runtime_observation": result["result"], "runtime_settlement": result["settlement"], "sql_assertions": sqlFacts, "cleanup": "case-owner-joins-emitted-separately"}
	encoded, err := json.Marshal(record)
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Log(string(encoded))
}

type mcpDurableBridgeServer struct {
	bridgev1.UnimplementedAgentRuntimeBridgeServiceServer
	store                          *PostgreSQLBridgeAPIStore
	mu                             sync.Mutex
	claims, commits, notifications int
	manifestBarrier                func(context.Context, *bridgev1.McpManifestChangedRequest) error
	dropManifestACK                bool
	manifestOutcomes               []mcpDurableManifestOutcome
	commitACKBarrier               func(context.Context, *bridgev1.CommitMcpToolResultRequest, *bridgev1.CommitMcpToolResultResponse) error
	commitRequests                 []*bridgev1.CommitMcpToolResultRequest
	commitTimes                    []time.Time
}

type mcpDurableManifestOutcome struct {
	ETag    string `json:"etag"`
	Outcome string `json:"outcome"`
	Code    string `json:"grpc_code"`
}

func (s *mcpDurableBridgeServer) ClaimMcpToolResult(ctx context.Context, r *bridgev1.ClaimMcpToolResultRequest) (*bridgev1.ClaimMcpToolResultResponse, error) {
	s.mu.Lock()
	s.claims++
	s.mu.Unlock()
	return s.store.ClaimMcpToolResult(ctx, r)
}
func (s *mcpDurableBridgeServer) CommitMcpToolResult(ctx context.Context, r *bridgev1.CommitMcpToolResultRequest) (*bridgev1.CommitMcpToolResultResponse, error) {
	s.mu.Lock()
	s.commits++
	s.commitRequests = append(s.commitRequests, r)
	s.commitTimes = append(s.commitTimes, time.Now())
	barrier := s.commitACKBarrier
	s.mu.Unlock()
	response, err := s.store.CommitMcpToolResult(ctx, r)
	if err == nil && barrier != nil {
		if err := barrier(ctx, r, response); err != nil {
			return nil, err
		}
	}
	return response, err
}
func (s *mcpDurableBridgeServer) RelinquishMcpToolResult(ctx context.Context, r *bridgev1.RelinquishMcpToolResultRequest) (*bridgev1.RelinquishMcpToolResultResponse, error) {
	return s.store.RelinquishMcpToolResult(ctx, r)
}
func (s *mcpDurableBridgeServer) SettleToolResult(ctx context.Context, r *bridgev1.SettleToolResultRequest) (*bridgev1.SettleToolResultResponse, error) {
	return s.store.SettleToolResult(ctx, r)
}
func (s *mcpDurableBridgeServer) LoadContext(ctx context.Context, r *bridgev1.LoadContextRequest) (*bridgev1.LoadContextResponse, error) {
	return s.store.LoadContext(ctx, r)
}
func (s *mcpDurableBridgeServer) McpManifestChanged(ctx context.Context, r *bridgev1.McpManifestChangedRequest) (*bridgev1.McpManifestChangedResponse, error) {
	s.mu.Lock()
	s.notifications++
	barrier := s.manifestBarrier
	s.mu.Unlock()
	if barrier != nil {
		if err := barrier(ctx, r); err != nil {
			return nil, err
		}
	}
	response, err := s.store.McpManifestChanged(ctx, r)
	outcome := "rejected"
	if response.GetCommitted() != nil {
		outcome = "committed"
	} else if response.GetDuplicate() != nil {
		outcome = "duplicate"
	}
	s.mu.Lock()
	drop := err == nil && response.GetCommitted() != nil && s.dropManifestACK
	if drop {
		s.dropManifestACK = false
	}
	s.manifestOutcomes = append(s.manifestOutcomes, mcpDurableManifestOutcome{ETag: r.GetManifestEtag(), Outcome: outcome, Code: status.Code(err).String()})
	s.mu.Unlock()
	if drop {
		return nil, status.Error(codes.Unavailable, "controlled committed manifest ACK loss")
	}
	return response, err
}

func (s *mcpDurableBridgeServer) manifestResults() []mcpDurableManifestOutcome {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]mcpDurableManifestOutcome(nil), s.manifestOutcomes...)
}
