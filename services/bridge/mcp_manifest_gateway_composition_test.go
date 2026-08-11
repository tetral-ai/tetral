package agentruntimebridge

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPostgreSQLMCPManifestCapacityClassificationAcrossBridgeAndGateway(t *testing.T) {
	bunPath, err := exec.LookPath("bun")
	if err != nil {
		t.Fatalf("production Gateway composition requires bun: %v", err)
	}
	gatewayRoot := filepath.Clean(filepath.Join("..", "gateway"))
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_mcp_capacity", "thr_mcp_capacity")
	seedBridgeAPIAgentConfig(t, admin, "default", "sesn_mcp_capacity", `{"name":"agent","model":"anthropic/claude-opus-4-8","tools":[{"type":"mcp_toolset","mcp_server_name":"github"}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}],"skills":[],"metadata":{}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET installed_tools_json =
		'{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"github"}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}]}'
		WHERE workspace_id='default' AND id='sesn_mcp_capacity'`); err != nil {
		t.Fatalf("seed capacity manifest config: %v", err)
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.MCPManifestLister = staticManifestLister{tools: []MCPManifestTool{{
		Name: "github_search", Description: strings.Repeat("x", MaxMcpManifestBytes), InputSchemaJSON: `{"type":"object"}`,
	}}}
	bridge := capacityClassificationBridgeServer{store: store}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for manifest capacity composition: %v", err)
	}
	grpcServer := grpc.NewServer()
	bridgev1.RegisterAgentRuntimeBridgeServiceServer(grpcServer, bridge)
	go func() { _ = grpcServer.Serve(listener) }()
	defer grpcServer.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, bunPath, "run", "packages/mcp-connector/test/fixtures/manifest-capacity-classification-composition.ts", listener.Addr().String()) //nolint:gosec // fixed repository fixture and test-owned address.
	command.Dir = gatewayRoot
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run manifest capacity composition: %v\n%s", err, output)
	}
	var result struct {
		OverCap           map[string]any `json:"overCap"`
		TransportCapacity map[string]any `json:"transportCapacity"`
	}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode manifest capacity composition: %v: %s", err, output)
	}
	if result.OverCap["ok"] != true || result.TransportCapacity["ok"] != false || result.TransportCapacity["code"] != "grpc_8" {
		t.Fatalf("capacity classifications = over-cap %#v transport %#v", result.OverCap, result.TransportCapacity)
	}
	var readiness, diagnostic string
	if err := admin.QueryRowContext(context.Background(), `SELECT readiness, diagnostic FROM session_mcp_manifests
		WHERE workspace_id='default' AND session_id='sesn_mcp_capacity' AND mcp_server_name='github'`).Scan(&readiness, &diagnostic); err != nil {
		t.Fatalf("read durable over-cap settlement: %v", err)
	}
	if readiness != "unready" || diagnostic != "manifest_too_large" {
		t.Fatalf("durable over-cap settlement = %s/%s; want unready/manifest_too_large", readiness, diagnostic)
	}
}

type capacityClassificationBridgeServer struct {
	bridgev1.UnimplementedAgentRuntimeBridgeServiceServer
	store *PostgreSQLBridgeAPIStore
}

func (s capacityClassificationBridgeServer) McpManifestChanged(ctx context.Context, request *bridgev1.McpManifestChangedRequest) (*bridgev1.McpManifestChangedResponse, error) {
	if request.GetSessionId() == "sesn_transport_capacity" {
		return nil, status.Error(codes.ResourceExhausted, "ordinary transport capacity rejection")
	}
	return s.store.McpManifestChanged(ctx, request)
}

type staticManifestLister struct{ tools []MCPManifestTool }

func (l staticManifestLister) ListMCPTools(_ context.Context, request MCPManifestListRequest) (MCPManifestListResult, error) {
	return MCPManifestListResult{ManifestETag: request.ManifestETag, Tools: l.tools}, nil
}

func TestPostgreSQLMCPManifestAckLossReplaysThroughGatewayRetry(t *testing.T) {
	bunPath, err := exec.LookPath("bun")
	if err != nil {
		t.Fatalf("production Gateway composition requires bun: %v", err)
	}
	gatewayRoot := filepath.Clean(filepath.Join("..", "gateway"))
	if _, err := os.Stat(filepath.Join(gatewayRoot, "node_modules", "@grpc", "grpc-js")); err != nil {
		t.Fatalf("production Gateway composition requires installed Gateway dependencies: %v", err)
	}

	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_mcp_ack_loss", "thr_mcp_ack_loss")
	seedBridgeAPIAgentConfig(t, admin, "default", "sesn_mcp_ack_loss", `{"name":"agent","model":"anthropic/claude-opus-4-8","tools":[{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"enabled":false,"permission_policy":{"type":"always_ask"}},"configs":[{"name":"github_search","enabled":true,"permission_policy":{"type":"always_allow"}}]}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}],"skills":[],"metadata":{}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions
		SET installed_tools_json = '{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"enabled":false,"permission_policy":{"type":"always_ask"}},"configs":[{"name":"github_search","enabled":true,"permission_policy":{"type":"always_allow"}}]}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}]}'
		WHERE workspace_id = 'default' AND id = 'sesn_mcp_ack_loss'`); err != nil {
		t.Fatalf("seed durable MCP config: %v", err)
	}

	lister := &echoManifestETagLister{}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.MCPManifestLister = lister
	bridge := &commitThenDropFirstManifestACKServer{store: store}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for Bridge composition: %v", err)
	}
	grpcServer := grpc.NewServer()
	bridgev1.RegisterAgentRuntimeBridgeServiceServer(grpcServer, bridge)
	serveError := make(chan error, 1)
	go func() {
		serveError <- grpcServer.Serve(listener)
	}()
	defer func() {
		grpcServer.Stop()
		_ = listener.Close()
		select {
		case err := <-serveError:
			if err != nil && err != grpc.ErrServerStopped {
				t.Errorf("serve Bridge composition: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("Bridge composition server did not stop")
		}
	}()

	tokenPath := filepath.Join(t.TempDir(), "bridge-token")
	if err := os.WriteFile(tokenPath, []byte("projected-token\n"), 0o600); err != nil {
		t.Fatalf("write Bridge token: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, bunPath, "run", "packages/mcp-connector/test/fixtures/manifest-ack-loss-composition.ts", listener.Addr().String(), tokenPath) //nolint:gosec // fixed repository fixture and test-owned inputs.
	command.Dir = gatewayRoot
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("run Gateway manifest ACK-loss composition: %v\nstdout=%s\nstderr=%s", err, stdout.String(), stderr.String())
	}
	var result struct {
		InitialManifestETag string `json:"initialManifestEtag"`
		Recovered           struct {
			Status       string `json:"status"`
			ManifestETag string `json:"manifestEtag"`
			Duplicate    bool   `json:"duplicate"`
		} `json:"recovered"`
		LaterReplay struct {
			Status       string `json:"status"`
			ManifestETag string `json:"manifestEtag"`
			Duplicate    bool   `json:"duplicate"`
		} `json:"laterReplay"`
		ListCalls int   `json:"listCalls"`
		Sleeps    []int `json:"sleeps"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("decode Gateway manifest ACK-loss result: %v\nstdout=%s", err, stdout.String())
	}
	if result.Recovered.Status != "notified" || !result.Recovered.Duplicate || result.Recovered.ManifestETag != result.InitialManifestETag {
		t.Fatalf("ACK-loss recovery = %#v; want duplicate durable notification for initial etag", result.Recovered)
	}
	if result.LaterReplay.Status != "notified" || !result.LaterReplay.Duplicate || result.LaterReplay.ManifestETag != result.InitialManifestETag {
		t.Fatalf("later same-manifest notification = %#v; want replay/current", result.LaterReplay)
	}
	if result.ListCalls != 3 {
		t.Fatalf("Gateway list calls = %d; want initial capture plus two real notifications", result.ListCalls)
	}
	if len(result.Sleeps) != 1 || result.Sleeps[0] != 1_000 {
		t.Fatalf("Gateway retry sleeps = %v; want first existing retry delay only", result.Sleeps)
	}
	if bridge.callCount() != 3 {
		t.Fatalf("Bridge McpManifestChanged calls = %d; want dropped ACK, retry, and later replay", bridge.callCount())
	}
	if lister.callCount() != 1 {
		t.Fatalf("Bridge manifest list calls = %d; want only first acceptance to list", lister.callCount())
	}

	var manifestRows int
	var generation int64
	var readiness string
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*), COALESCE(max(manifest_generation), 0), COALESCE(max(readiness), '')
		FROM session_mcp_manifests
		WHERE workspace_id = 'default' AND session_id = 'sesn_mcp_ack_loss' AND mcp_server_name = 'github'`).Scan(&manifestRows, &generation, &readiness); err != nil {
		t.Fatalf("read durable manifest after ACK loss: %v", err)
	}
	if manifestRows != 1 || generation != 1 || readiness != "ready" {
		t.Fatalf("durable manifest rows/generation/readiness = %d/%d/%q; want 1/1/ready", manifestRows, generation, readiness)
	}
	var queueJobs int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM queue_jobs
		WHERE workspace_id = 'default' AND kind = $1
		AND payload_json::jsonb ->> 'session_id' = 'sesn_mcp_ack_loss'
		AND payload_json::jsonb ->> 'mcp_server_name' = 'github'`, queue.KindRuntimeConfigUpdate).Scan(&queueJobs); err != nil {
		t.Fatalf("count manifest Queue custody after ACK loss: %v", err)
	}
	if queueJobs != 1 {
		t.Fatalf("manifest Queue jobs after ACK loss = %d; want 1", queueJobs)
	}
}

type commitThenDropFirstManifestACKServer struct {
	bridgev1.UnimplementedAgentRuntimeBridgeServiceServer
	store *PostgreSQLBridgeAPIStore
	mu    sync.Mutex
	calls int
}

func (s *commitThenDropFirstManifestACKServer) McpManifestChanged(ctx context.Context, request *bridgev1.McpManifestChangedRequest) (*bridgev1.McpManifestChangedResponse, error) {
	response, err := s.store.McpManifestChanged(ctx, request)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.calls++
	drop := s.calls == 1
	s.mu.Unlock()
	if drop {
		return nil, status.Error(codes.Unavailable, "manifest acknowledgement unavailable")
	}
	return response, nil
}

func (s *commitThenDropFirstManifestACKServer) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

type echoManifestETagLister struct {
	mu    sync.Mutex
	calls int
}

func (l *echoManifestETagLister) ListMCPTools(_ context.Context, request MCPManifestListRequest) (MCPManifestListResult, error) {
	l.mu.Lock()
	l.calls++
	l.mu.Unlock()
	return MCPManifestListResult{
		ManifestETag: request.ManifestETag,
		Tools: []MCPManifestTool{{
			Name: "github_search", Description: "Search GitHub", InputSchemaJSON: `{"type":"object"}`,
		}},
	}, nil
}

func (l *echoManifestETagLister) callCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls
}
