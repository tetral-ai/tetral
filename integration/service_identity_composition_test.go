package integration

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/internalgrpc"
	grpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/kubernetes/kubernetest"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	gatewayv1 "github.com/tetral-ai/tetral/services/gateway/gen/tetral/provider_gateway/v1"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
	web "github.com/tetral-ai/tetral/services/web-connector"
)

const identityBindingKey = "service-identity-binding-key-at-least-32-bytes"
const identityPodUID = "pod_uid_service_identity"

type identityEnv map[string]string

func (e identityEnv) Getenv(key string) string { return e[key] }

// TestSeparatedServiceWorkloadAuthentication exercises the real Go and Bun receivers.
// Reaching an authorized handler is distinct from its business response. Denied callers
// must leave receiver-entry counts, external invocations and durable receipts unchanged.
func TestSeparatedServiceWorkloadAuthentication(t *testing.T) {
	runtimeDB, adminDB := storagetest.NewPostgreSQLDBWithAdmin(t)
	var reviews atomic.Int64
	decodeReview, err := kubernetest.NewTokenReviewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	review := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/apis/authentication.k8s.io/v1/tokenreviews" || r.Header.Get("Authorization") != "Bearer reviewer-token" && r.Header.Get("Authorization") != "bearer reviewer-token" {
			http.Error(w, "unexpected review request", http.StatusForbidden)
			return
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "invalid review", 400)
			return
		}
		body, err := decodeReview(raw)
		if err != nil {
			http.Error(w, "invalid review", 400)
			return
		}
		if len(body.Audiences) != 1 || body.Audiences[0] != "tetral-internal-grpc" {
			http.Error(w, "unexpected audience", 400)
			return
		}
		reviews.Add(1)
		names := map[string]string{"runtime": "tetral-agent-runtime:agent-runtime", "runner": "tetral-system:job-runner", "bridge": "tetral-system:bridge", "provider": "tetral-system:provider-gateway", "mcp": "tetral-system:mcp-connector", "oldgateway": "tetral-system:gateway", "wrongns": "wrong-system:job-runner", "expired": "tetral-agent-runtime:agent-runtime", "wrongaud": "tetral-agent-runtime:agent-runtime"}
		name, ok := names[body.Token]
		aud := []string{"tetral-internal-grpc"}
		if body.Token == "wrongaud" {
			aud = []string{"kubernetes-api"}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "authentication.k8s.io/v1", "kind": "TokenReview", "status": map[string]any{"authenticated": ok && body.Token != "expired", "audiences": aud, "user": map[string]any{"username": "system:serviceaccount:" + name, "extra": map[string]any{"authentication.kubernetes.io/pod-uid": []string{identityPodUID}}}}})
	}))
	t.Cleanup(review.Close)
	review.StartTLS()
	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "reviewer-token")
	caPath := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(tokenPath, []byte("reviewer-token"), 0600); err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(review.TLS.Certificates[0].Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	env := identityEnv{grpcauth.EnvKubernetesAPIServerURL: review.URL, grpcauth.EnvKubernetesAPICACertPath: caPath, grpcauth.EnvKubernetesTokenReviewReviewerTokenPath: tokenPath}
	reviewClient, err := grpcauth.NewTokenReviewClientFromEnv(env)
	if err != nil {
		t.Fatal(err)
	}
	authenticator := func(accounts ...grpcauth.ServiceAccount) internalgrpc.Authenticator {
		return grpcauth.NewTokenReviewAuthenticator(reviewClient, grpcauth.Config{Audience: grpcauth.Audience, AllowedServiceAccounts: accounts})
	}
	account := func(ns, name string) grpcauth.ServiceAccount {
		return grpcauth.ServiceAccount{Namespace: ns, Name: name}
	}
	var bridgeEntries atomic.Int64
	store := bridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte(identityBindingKey)
	seedIdentitySession(t, adminDB)
	delivery := jobrunner.NewJobRunnerRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtimeDB), nil, jobrunner.JobRunnerConfig{AgentRuntimeGRPCPort: 9090}, func() kubernetes.BindingVisibilitySnapshot {
		return kubernetes.BindingVisibilitySnapshot{Ready: true, Candidates: []kubernetes.BindingCandidate{{Namespace: "tetral-agent-runtime", PodName: "runtime-identity", PodUID: identityPodUID, PodIP: "127.0.0.1"}}}
	})
	declaration, err := delivery.PrepareRuntimeCommand(context.Background(), jobrunner.RuntimeJob{Kind: queue.KindRuntimeConfigUpdate, WorkspaceID: "default", SessionID: "sesn_identity", RuntimeInputID: "runtime_config_update:sesn_identity:1", ConfigGeneration: "1"})
	if err != nil || declaration.AttemptedBinding.BindingID == "" {
		t.Fatalf("production Runner binding declaration: %v %+v", err, declaration)
	}
	bridgeConn := startIdentityReceiver(t, internalgrpc.Config{ServiceName: "bridge", Authenticator: authenticator(account("tetral-agent-runtime", "agent-runtime"), account("tetral-system", "provider-gateway"), account("tetral-system", "mcp-connector")), MethodAuthorizer: bridge.BridgeAPIMethodAuthorizer, Register: func(s *grpc.Server) { bridge.RegisterBridgeAPI(s, store) }, ServerOptions: []grpc.ServerOption{grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		bridgeEntries.Add(1)
		return handler(ctx, req)
	})}})
	client := bridgev1.NewAgentRuntimeBridgeServiceClient(bridgeConn)
	scope := &bridgev1.RuntimeScope{WorkspaceId: "default", SessionId: "sesn_identity", SessionThreadId: "thrd_identity", Binding: &bridgev1.RuntimeBindingRef{BindingId: declaration.AttemptedBinding.BindingID, BindingGeneration: declaration.AttemptedBinding.Generation, TargetPodUid: declaration.AttemptedBinding.TargetPodUID}}
	issued, err := client.RefreshRuntimeBindingToken(identityContext("runtime"), &bridgev1.RefreshRuntimeBindingTokenRequest{Scope: scope})
	if err != nil || issued.GetRuntimeBindingToken() == "" {
		t.Fatalf("production Bridge binding issuance: %v", err)
	}
	receipts := func() int {
		var n int
		if err := adminDB.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	initialReceipts := receipts()
	var goOutcomes []map[string]any
	goOutcomes = append(goOutcomes, map[string]any{"receiver": "bridge", "method": "RefreshRuntimeBindingToken", "caller": "runtime", "business": "issued token for production declared binding"})
	for _, edge := range []struct {
		name, token string
		invoke      func(context.Context) error
	}{
		{"Runtime LoadContext", "runtime", func(ctx context.Context) error {
			_, err := client.LoadContext(ctx, &bridgev1.LoadContextRequest{})
			return err
		}},
		{"Provider attachment", "provider", func(ctx context.Context) error {
			_, err := client.ResolveTransientAttachment(ctx, &bridgev1.ResolveTransientAttachmentRequest{})
			return err
		}},
		{"MCP manifest", "mcp", func(ctx context.Context) error {
			_, err := client.McpManifestChanged(ctx, &bridgev1.McpManifestChangedRequest{})
			return err
		}},
	} {
		t.Run(edge.name, func(t *testing.T) {
			before := bridgeEntries.Load()
			err := edge.invoke(identityContext(edge.token))
			if status.Code(err) == codes.Unauthenticated || status.Code(err) == codes.PermissionDenied || err == nil {
				t.Fatalf("authorized invalid business input=%v; want owning validation failure", err)
			}
			if bridgeEntries.Load() != before+1 {
				t.Fatal("authorized caller never entered receiver")
			}
			goOutcomes = append(goOutcomes, map[string]any{"receiver": "bridge", "method": edge.name, "caller": edge.token, "code": status.Code(err).String(), "receiverEntered": true, "business": "invalid business request rejected"})
			for _, token := range []string{"runner", "bridge", "oldgateway", "wrongns", "expired", "wrongaud"} {
				before := bridgeEntries.Load()
				err := edge.invoke(identityContext(token))
				want := codes.PermissionDenied
				if token == "expired" || token == "wrongaud" {
					want = codes.Unauthenticated
				}
				if status.Code(err) != want {
					t.Fatalf("%s status=%v; want%v", token, err, want)
				}
				if bridgeEntries.Load() != before || receipts() != initialReceipts {
					t.Fatalf("%s denied caller changed receiver entry or durable receipts", token)
				}
				goOutcomes = append(goOutcomes, map[string]any{"receiver": "bridge", "method": edge.name, "caller": token, "code": want.String(), "receiverEntered": false, "receiptsUnchanged": true})
			}
		})
	}
	// The two connector roles cannot use each other's otherwise valid Bridge methods.
	for _, edge := range []struct {
		token  string
		invoke func(context.Context) error
	}{
		{"mcp", func(ctx context.Context) error {
			_, err := client.ResolveTransientAttachment(ctx, &bridgev1.ResolveTransientAttachmentRequest{})
			return err
		}},
		{"provider", func(ctx context.Context) error {
			_, err := client.McpManifestChanged(ctx, &bridgev1.McpManifestChangedRequest{})
			return err
		}},
	} {
		before := bridgeEntries.Load()
		if err := edge.invoke(identityContext(edge.token)); status.Code(err) != codes.PermissionDenied {
			t.Fatal(err)
		}
		if bridgeEntries.Load() != before {
			t.Fatal("cross-role caller entered receiver")
		}
		goOutcomes = append(goOutcomes, map[string]any{"receiver": "bridge", "method": "cross-role method", "caller": edge.token, "code": codes.PermissionDenied.String(), "receiverEntered": false})
	}

	blobs := blob.NewFakeBlobStore()
	backend := &identityWebBackend{}
	service := web.NewService(blobs, backend, web.NewBindingVerifier([]byte(identityBindingKey), time.Now), web.NewMetrics(), time.Now, nil)
	var webEntries atomic.Int64
	webConn := startIdentityReceiver(t, internalgrpc.Config{ServiceName: "web-connector", Authenticator: authenticator(account("tetral-agent-runtime", "agent-runtime")), MethodAuthorizer: web.MethodAuthorizer, Register: func(s *grpc.Server) { web.Register(s, service) }, ServerOptions: []grpc.ServerOption{grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		webEntries.Add(1)
		return handler(ctx, req)
	})}})
	webClient := gatewayv1.NewProviderGatewayServiceClient(webConn)
	request := &gatewayv1.RunWebRequest{WorkspaceId: "default", SessionId: "sesn_identity", SessionThreadId: "thrd_identity", BindingId: scope.Binding.BindingId, BindingGeneration: scope.Binding.BindingGeneration, ToolUseEventId: "sevt_identity", Input: &gatewayv1.WebToolInput{SearchQuery: []*gatewayv1.WebSearchQuery{{Q: "identity fixture"}}}}
	request.RuntimeBindingToken = issued.GetRuntimeBindingToken()
	response, err := webClient.RunWeb(identityContext("runtime"), request)
	if err != nil || response.GetStatus() != gatewayv1.RunWebStatus_RUN_WEB_STATUS_COMPLETED || backend.calls.Load() != 1 || blobs.Len() == 0 {
		t.Fatalf("authorized web outcome=%v error=%v calls=%d objects=%d", response, err, backend.calls.Load(), blobs.Len())
	}
	goOutcomes = append(goOutcomes, map[string]any{"receiver": "web", "method": "RunWeb", "caller": "runtime", "business": "completed", "backendCalls": 1, "blobPersisted": true, "bindingVerified": true})
	for _, token := range []string{"runner", "bridge", "provider", "mcp", "oldgateway", "wrongns", "expired", "wrongaud"} {
		beforeEntry, beforeCalls, beforeObjects := webEntries.Load(), backend.calls.Load(), blobs.Len()
		_, err := webClient.RunWeb(identityContext(token), request)
		want := codes.PermissionDenied
		if token == "expired" || token == "wrongaud" {
			want = codes.Unauthenticated
		}
		if status.Code(err) != want || webEntries.Load() != beforeEntry || backend.calls.Load() != beforeCalls || blobs.Len() != beforeObjects {
			t.Fatalf("denied Web token=%s error=%v changed business effects", token, err)
		}
		goOutcomes = append(goOutcomes, map[string]any{"receiver": "web", "method": "RunWeb", "caller": token, "code": want.String(), "receiverEntered": false, "backendAndBlobsUnchanged": true})
	}
	// Valid workload identity does not replace the signed per-session/pod binding fence.
	request.RuntimeBindingToken = identitySignedBinding(request, "other-pod", time.Now().Add(time.Minute).Unix())
	beforeCalls, beforeObjects := backend.calls.Load(), blobs.Len()
	_, err = webClient.RunWeb(identityContext("runtime"), request)
	if status.Code(err) != codes.PermissionDenied || backend.calls.Load() != beforeCalls || blobs.Len() != beforeObjects {
		t.Fatalf("wrong Web binding bypassed fence: %v", err)
	}
	goOutcomes = append(goOutcomes, map[string]any{"receiver": "web", "method": "RunWeb", "caller": "runtime", "binding": "wrong reviewed pod", "code": codes.PermissionDenied.String(), "backendAndBlobsUnchanged": true})
	if receipts() != initialReceipts {
		t.Fatal("identity composition changed durable Bridge receipts")
	}
	bun, err := exec.LookPath("bun")
	if err != nil {
		t.Fatal("Bun is required for receiving workload identity composition")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, bun, "run", "testdata/service-identity.ts")
	command.Env = append(os.Environ(), "TETRAL_TEST_REVIEW_URL="+review.URL, "TETRAL_TEST_REVIEW_TOKEN_PATH="+tokenPath, "TETRAL_TEST_REVIEW_CA_PATH="+caPath, "TETRAL_TEST_DATABASE_URL=postgres://fixture-local", "TETRAL_TEST_RUNTIME_BINDING_TOKEN="+issued.GetRuntimeBindingToken(), "TETRAL_TEST_RUNTIME_BINDING_ID="+scope.Binding.BindingId, "TETRAL_TEST_RUNTIME_BINDING_GENERATION="+fmt.Sprint(scope.Binding.BindingGeneration))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("Bun receiving identity composition: %v\n%s", err, output)
	}
	var proof struct {
		Rows                                            []json.RawMessage
		RuntimeEffects, DiscoveryEffects, ToolEffects   int
		ProviderCredentialReads, ProviderPoolSelections int
		Cleanup                                         string
	}
	if err := json.Unmarshal(output, &proof); err != nil {
		t.Fatalf("Bun identity proof decode: %v\n%s", err, output)
	}
	if len(proof.Rows) < 30 || proof.RuntimeEffects != 1 || proof.DiscoveryEffects != 2 || proof.ToolEffects != 1 || proof.ProviderCredentialReads != 1 || proof.ProviderPoolSelections != 1 || proof.Cleanup != "all receivers stopped" || reviews.Load() < 40 {
		t.Fatalf("incomplete receiver proof: %s reviews=%d", output, reviews.Load())
	}
	goProof, err := json.Marshal(goOutcomes)
	if err != nil {
		t.Fatal(err)
	}
	// Fixed role labels and outcome counters only: no token, binding ID or credential is logged.
	t.Logf("Go identity outcomes: %s", goProof)
	t.Logf("Bun identity outcomes: %s", output)
	t.Logf("real Go Bridge/Web and Bun Runtime/Provider/MCP receivers accepted exact owners; %d Bun outcomes; TokenReviews=%d", len(proof.Rows), reviews.Load())
}

func identityContext(token string) context.Context {
	return metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "bearer "+token))
}
func startIdentityReceiver(t *testing.T, cfg internalgrpc.Config) *grpc.ClientConn {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Listener = listener
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	ready := make(chan struct{})
	cfg.OnServing = func() { close(ready) }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var runErr error
	var conn *grpc.ClientConn
	// Register ownership before startup: failed bind/readiness/client construction still joins the receiver.
	t.Cleanup(func() {
		defer func() {
			if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				t.Errorf("close receiver listener: %v", err)
			}
		}()
		if conn != nil {
			_ = conn.Close()
		}
		cancel()
		select {
		case <-done:
			if runErr != nil {
				t.Errorf("receiver failed: %v", runErr)
			}
		case <-time.After(5 * time.Second):
			t.Error("receiver did not stop")
		}
	})
	go func() { runErr = internalgrpc.Run(ctx, cfg); close(done) }()
	select {
	case <-ready:
	case <-done:
		t.Fatal(runErr)
	case <-time.After(5 * time.Second):
		t.Fatal("receiver did not start")
	}
	conn, err = grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

type identityWebBackend struct{ calls atomic.Int64 }

func (b *identityWebBackend) MaxAttemptsPerCall() int { return 1 }
func (b *identityWebBackend) Search(context.Context, string, []string) ([]web.SearchHit, web.BackendOutcome) {
	b.calls.Add(1)
	return []web.SearchHit{{URL: "https://example.com/", Title: "identity fixture", Description: "controlled search"}}, web.BackendOutcome{Kind: web.BackendSuccess, Requests: 1}
}
func (b *identityWebBackend) Fetch(context.Context, string) (web.Page, web.BackendOutcome) {
	panic("unexpected identity fixture fetch")
}
func identitySignedBinding(request *gatewayv1.RunWebRequest, pod string, expiry int64) string {
	payload, _ := json.Marshal(map[string]any{"v": 1, "workspace_id": request.GetWorkspaceId(), "session_id": request.GetSessionId(), "session_thread_id": request.GetSessionThreadId(), "binding_id": request.GetBindingId(), "binding_generation": request.GetBindingGeneration(), "runtime_pod_uid": pod, "exp": expiry})
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, []byte(identityBindingKey))
	_, _ = mac.Write([]byte(encoded))
	return fmt.Sprintf("rtbt_v1.%s.%s", encoded, base64.RawURLEncoding.EncodeToString(mac.Sum(nil)))
}

func seedIdentitySession(t *testing.T, db *sql.DB) {
	sessionID, threadID := "sesn_identity", "thrd_identity"
	t.Helper()
	agentID := "agent_" + sessionID
	environmentID := "env_" + sessionID
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO workspaces (id, type, name, created_at) VALUES ('default', 'workspace', 'default', '2026-01-01T00:00:00Z') ON CONFLICT (id) DO NOTHING`, nil},
		{`INSERT INTO agents (workspace_id, id, name, version, created_at, updated_at) VALUES ('default', $1, $1, 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`, []any{agentID}},
		{`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at) VALUES ('default', $1, $2, 1, '{"tools":[{"type":"tetral_agent_toolset","family":"claude"}]}', $3, '2026-01-01T00:00:00Z')`, []any{"agv_" + sessionID, agentID, "hash_" + sessionID}},
		{`INSERT INTO environments (workspace_id, id, name, config_json, current_generation, created_at, updated_at) VALUES ('default', $1, $1, '{}', 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`, []any{environmentID}},
		{`INSERT INTO sessions (workspace_id, id, main_thread_id, type, status, lifecycle_state, agent_id, agent_version, environment_id, installed_tools_json, created_at, updated_at) VALUES ('default', $1, $2, 'session', 'idle', 'active', $3, 1, $4, '{"tools":[{"type":"tetral_agent_toolset","family":"claude"}]}', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`, []any{sessionID, threadID, agentID, environmentID}},
		{`INSERT INTO session_threads (workspace_id, id, session_id, role, visibility, status, created_at, last_active_at, updated_at) VALUES ('default', $1, $2, 'main', 'public', 'idle', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`, []any{threadID, sessionID}},
		{`INSERT INTO session_runtime_status (workspace_id, session_id, status, idle_since, created_at, updated_at) VALUES ('default', $1, 'idle', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`, []any{sessionID}},
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(context.Background(), statement.query, statement.args...); err != nil {
			t.Fatalf("seed transport session: %v", err)
		}
	}
}
