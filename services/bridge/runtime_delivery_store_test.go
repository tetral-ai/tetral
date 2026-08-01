package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	providergatewayv1 "github.com/tetral-ai/tetral/services/gateway/gen/tetral/provider_gateway/v1"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"

	"google.golang.org/grpc"
)

// This file owns PostgreSQL runtime delivery-store behavior tests.

type recordingMCPConnectorServer struct {
	providergatewayv1.UnimplementedMcpConnectorServiceServer

	mu       sync.Mutex
	requests []*providergatewayv1.ListMcpToolsRequest
}

func (s *recordingMCPConnectorServer) ListMcpTools(_ context.Context, request *providergatewayv1.ListMcpToolsRequest) (*providergatewayv1.ListMcpToolsResponse, error) {
	s.mu.Lock()
	s.requests = append(s.requests, request)
	s.mu.Unlock()
	return &providergatewayv1.ListMcpToolsResponse{
		ManifestEtag: "etag_production_assembly",
		Tools: []*providergatewayv1.McpToolDefinition{{
			Name:            "github_search",
			Description:     "Search GitHub",
			InputSchemaJson: `{"type":"object"}`,
		}},
	}, nil
}

func (s *recordingMCPConnectorServer) recordedRequests() []*providergatewayv1.ListMcpToolsRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*providergatewayv1.ListMcpToolsRequest(nil), s.requests...)
}

type expiringMCPManifestLister struct {
	contexts []context.Context
}

func (l *expiringMCPManifestLister) ListMCPTools(ctx context.Context, request MCPManifestListRequest) (MCPManifestListResult, error) {
	if len(l.contexts) > 0 {
		select {
		case <-l.contexts[len(l.contexts)-1].Done():
		default:
			return MCPManifestListResult{}, errors.New("previous list context was not canceled before the next iteration")
		}
	}
	l.contexts = append(l.contexts, ctx)
	<-ctx.Done()
	return MCPManifestListResult{
		ManifestETag: "etag_" + request.MCPServerName,
		Tools: []MCPManifestTool{{
			Name:            request.MCPServerName + "_search",
			Description:     "Search " + request.MCPServerName,
			InputSchemaJSON: `{"type":"object"}`,
		}},
	}, nil
}

func TestPostgreSQLRuntimeDeliveryStoreDerivesBirthAttemptSealFromDurableHistory(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	for _, sessionID := range []string{"sesn_bridge_seal_order", "sesn_bridge_cold_supersede", "sesn_bridge_fractional_order"} {
		seedBridgeAPISession(t, admin, "default", sessionID, "thr_"+sessionID)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_preparations (
			workspace_id, session_id, preparation_attempt_id, environment_id,
			environment_generation, sandbox_id, status, failure_reason, retryable,
			created_at, updated_at, failed_at, superseded_at
		) VALUES
			('default', 'sesn_bridge_seal_order', 'prep_00_failed_before', 'env_sesn_bridge_seal_order', 1, 'sandbox_seal', 'failed', 'before_birth', false,
			 '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:01Z'),
			('default', 'sesn_bridge_seal_order', 'prep_10_birth', 'env_sesn_bridge_seal_order', 1, 'sandbox_seal', 'ready', NULL, NULL,
			 '2026-01-01T00:01:00Z', '2026-01-01T00:01:00Z', NULL, '2026-01-01T00:01:01Z'),
			('default', 'sesn_bridge_seal_order', 'prep_20_cold_successor', 'env_sesn_bridge_seal_order', 1, 'sandbox_seal', 'pending', NULL, NULL,
			 '2026-01-01T00:02:00Z', '2026-01-01T00:02:00Z', NULL, '2026-01-01T00:02:01Z'),
			('default', 'sesn_bridge_seal_order', 'prep_30_failed_b', 'env_sesn_bridge_seal_order', 1, 'sandbox_seal', 'failed', 'later_failure', false,
			 '2026-01-01T00:03:00Z', '2026-01-01T00:03:00Z', '2026-01-01T00:03:00Z', '2026-01-01T00:03:01Z'),
			('default', 'sesn_bridge_seal_order', 'prep_30_failed_a', 'env_sesn_bridge_seal_order', 1, 'sandbox_seal', 'failed', 'earliest_tie_break', false,
			 '2026-01-01T00:03:00Z', '2026-01-01T00:03:00Z', '2026-01-01T00:03:00Z', '2026-01-01T00:03:01Z'),
			('default', 'sesn_bridge_seal_order', 'prep_40_after_failures', 'env_sesn_bridge_seal_order', 1, 'sandbox_seal', 'ready', NULL, NULL,
			 '2026-01-01T00:04:00Z', '2026-01-01T00:04:00Z', NULL, NULL),
			('default', 'sesn_bridge_cold_supersede', 'prep_cold_birth', 'env_sesn_bridge_cold_supersede', 1, 'sandbox_cold', 'ready', NULL, NULL,
			 '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', NULL, '2026-01-01T00:00:01Z'),
			('default', 'sesn_bridge_cold_supersede', 'prep_cold_successor', 'env_sesn_bridge_cold_supersede', 1, 'sandbox_cold', 'pending', NULL, NULL,
			 '2026-01-01T00:01:00Z', '2026-01-01T00:01:00Z', NULL, NULL),
			('default', 'sesn_bridge_fractional_order', 'prep_fractional_birth', 'env_sesn_bridge_fractional_order', 1, 'sandbox_sesn_bridge_fractional_order', 'ready', NULL, NULL,
			 '2025-12-31T23:59:59Z', '2025-12-31T23:59:59Z', NULL, '2026-01-01T00:00:00Z'),
			('default', 'sesn_bridge_fractional_order', 'prep_fractional_failure', 'env_sesn_bridge_fractional_order', 1, 'sandbox_sesn_bridge_fractional_order', 'failed', 'fractional_failure', false,
			 '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', NULL),
			('default', 'sesn_bridge_fractional_order', 'prep_fractional_successor', 'env_sesn_bridge_fractional_order', 1, 'sandbox_sesn_bridge_fractional_order', 'ready', NULL, NULL,
			 '2026-01-01T00:00:00.1Z', '2026-01-01T00:00:00.1Z', NULL, NULL)`,
	); err != nil {
		t.Fatalf("seed preparation seal history: %v", err)
	}
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	taskNotificationSeal, err := store.ResolveRuntimeInputSeal(context.Background(), RuntimeJob{
		Kind: queue.KindRuntimeInput, WorkspaceID: "default", SessionID: "sesn_bridge_seal_order", InputKind: "task_notification",
	})
	if err != nil || taskNotificationSeal != "" {
		t.Fatalf("preparation-free task notification seal = %q, %v; want empty success", taskNotificationSeal, err)
	}
	for _, test := range []struct {
		name      string
		sessionID string
		birthID   string
		wantSeal  string
	}{
		{name: "preexisting input crossing successors seals on earliest later failure", sessionID: "sesn_bridge_seal_order", birthID: "prep_10_birth", wantSeal: "prep_30_failed_a"},
		{name: "failed-attempt fanout seals on its own attempt", sessionID: "sesn_bridge_seal_order", birthID: "prep_30_failed_a", wantSeal: "prep_30_failed_a"},
		{name: "birth after all failures remains unsealed", sessionID: "sesn_bridge_seal_order", birthID: "prep_40_after_failures"},
		{name: "cold supersession without failure remains unsealed", sessionID: "sesn_bridge_cold_supersede", birthID: "prep_cold_birth"},
		{name: "fractional timestamp ordering finds the first later failure", sessionID: "sesn_bridge_fractional_order", birthID: "prep_fractional_birth", wantSeal: "prep_fractional_failure"},
		{name: "birth under fractional successor is not over-sealed by whole-second failure", sessionID: "sesn_bridge_fractional_order", birthID: "prep_fractional_successor"},
	} {
		t.Run(test.name, func(t *testing.T) {
			sealedAttemptID, err := store.ResolveRuntimeInputSeal(context.Background(), RuntimeJob{
				Kind: queue.KindRuntimeInput, WorkspaceID: "default", SessionID: test.sessionID,
				PreparationAttemptID: test.birthID,
			})
			if err != nil {
				t.Fatalf("ResolveRuntimeInputSeal: %v", err)
			}
			if sealedAttemptID != test.wantSeal {
				t.Fatalf("sealed attempt = %q; want %q", sealedAttemptID, test.wantSeal)
			}
		})
	}

	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_fractional_order", "2026-01-01T00:00:01Z")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_fractional_order", "bind_bridge_fractional_order", 1, "pod_uid_fractional_order")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_fractional_order", "thr_sesn_bridge_fractional_order", "evt_bridge_fractional_order", 1, "user.message", `{"type":"user.message","content":[{"type":"text","text":"continue"}]}`)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC) }
	plan, err := store.PrepareRuntimeCommand(context.Background(), RuntimeJob{
		JobID: "qjob_bridge_fractional_order", LeaseToken: "lease_bridge_fractional_order",
		Kind: queue.KindRuntimeInput, WorkspaceID: "default", SessionID: "sesn_bridge_fractional_order",
		PreparationAttemptID: "prep_fractional_successor",
		SessionThreadID:      "thr_sesn_bridge_fractional_order",
		RuntimeInputID:       "rin_bridge_fractional_order",
		EventIDs:             []string{"evt_bridge_fractional_order"},
		SequenceFrom:         1,
		SequenceTo:           1,
		InputKind:            "messages",
		CommandKind:          agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:          `{"type":"messages"}`,
	})
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand under fractional successor: %v", err)
	}
	if plan.SettledAccepted || plan.StaleAccepted || plan.Request.GetRuntimeInputId() != "rin_bridge_fractional_order" {
		t.Fatalf("fractional successor plan = %#v; want delivery through the chronologically latest ready attempt", plan)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreInitialMCPManifestCrashAfterCaptureRedrivesDurableGeneration(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_initial_mcp", "thr_bridge_initial_mcp")
	seedBridgeAPIAgentConfig(t, admin, "default", "sesn_bridge_initial_mcp", `{"name":"agent","model":"anthropic/claude-opus-4-8","tools":[{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"enabled":false,"permission_policy":{"type":"always_ask"}},"configs":[{"name":"github_search","enabled":true,"permission_policy":{"type":"always_allow"}}]}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}],"skills":[],"metadata":{}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET installed_tools_json = '{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"enabled":false,"permission_policy":{"type":"always_ask"}},"configs":[{"name":"github_search","enabled":true,"permission_policy":{"type":"always_allow"}}]}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}]}' WHERE workspace_id = 'default' AND id = 'sesn_bridge_initial_mcp'`); err != nil {
		t.Fatalf("seed durable initial MCP config: %v", err)
	}
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_initial_mcp", "prep_bridge_initial_mcp")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_initial_mcp", "2026-01-01T00:00:30Z")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_initial_mcp", "bind_bridge_initial_mcp", 1, "pod_uid_initial_mcp")

	lister := &recordingMCPManifestLister{results: []MCPManifestListResult{{
		ManifestETag: "etag_initial",
		Tools:        []MCPManifestTool{{Name: "github_search", Description: "Search GitHub", InputSchemaJSON: `{"type":"object"}`}},
	}}}
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	store.MCPManifestLister = lister
	sender := &recordingRuntimeCommandSender{}
	job := RuntimeJob{
		JobID:                "qjob_bridge_initial_mcp",
		LeaseToken:           "lease_bridge_initial_mcp",
		Kind:                 queue.KindRuntimeInput,
		WorkspaceID:          "default",
		SessionID:            "sesn_bridge_initial_mcp",
		PreparationAttemptID: "prep_bridge_initial_mcp",
		SessionThreadID:      "thr_bridge_initial_mcp",
		RuntimeInputID:       "rin_bridge_initial_mcp",
		EventIDs:             []string{"evt_bridge_initial_mcp"},
		SequenceFrom:         1,
		SequenceTo:           1,
		InputKind:            "messages",
		CommandKind:          agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:          `{"type":"messages"}`,
	}

	result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
	if err != nil {
		t.Fatalf("DeliverRuntimeJob initial MCP manifest: %v", err)
	}
	if result.Status != RuntimeDeliveryRejected || !result.Retryable || result.ErrorKind != "mcp_manifest_discovery_pending" {
		t.Fatalf("initial MCP manifest delivery result = %#v; want retryable mcp_manifest_discovery_pending", result)
	}
	if len(lister.requests) != 1 ||
		lister.requests[0].WorkspaceID != "default" ||
		lister.requests[0].SessionID != "sesn_bridge_initial_mcp" ||
		lister.requests[0].MCPServerName != "github" ||
		lister.requests[0].ManifestETag != "" {
		t.Fatalf("MCP manifest lister requests = %#v; want initial github list", lister.requests)
	}
	if len(sender.requests) != 0 {
		t.Fatalf("runtime commands sent = %d; want 0 while manifest update is queued first", len(sender.requests))
	}
	assertRuntimeMCPManifestQueueJob(t, admin, "default", "sesn_bridge_initial_mcp", "github", 1)
	var operationRuntimeInputID string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT runtime_input_id
		   FROM session_bridge_operations
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_initial_mcp'
		    AND operation = $1
		    AND idempotency_key = 'github:1'`,
		bridgeOpMcpManifestChanged,
	).Scan(&operationRuntimeInputID); err != nil {
		t.Fatalf("read initial MCP manifest bridge operation: %v", err)
	}
	if operationRuntimeInputID != "runtime_config_update:mcp_manifest:sesn_bridge_initial_mcp:github:1" {
		t.Fatalf("bridge operation runtime_input_id = %q; want manifest runtime config update id", operationRuntimeInputID)
	}

	// Reconstruct the delivery from the committed queue payload, as a fresh worker
	// would after the accepting process crashed before sending anything to Runtime.
	var queuedJobID string
	var queuedPayload string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT id, payload_json
		   FROM queue_jobs
		  WHERE workspace_id = 'default'
		    AND payload_json::jsonb ->> 'session_id' = 'sesn_bridge_initial_mcp'
		    AND kind = 'runtime_config_update'
		    AND payload_json::jsonb ->> 'mcp_server_name' = 'github'
		    AND payload_json::jsonb ->> 'manifest_generation' = '1'`,
	).Scan(&queuedJobID, &queuedPayload); err != nil {
		t.Fatalf("read committed MCP manifest delivery intent: %v", err)
	}
	redrivenJob, err := DecodeRuntimeJob(&queuev1.QueueJob{ //nolint:gosec // Test lease token fixture, not a secret.
		Id:          queuedJobID,
		WorkspaceId: "default",
		Kind:        queue.KindRuntimeConfigUpdate,
		LeaseToken:  "lease_redriven_manifest",
		PayloadJson: queuedPayload,
	})
	if err != nil {
		t.Fatalf("decode committed MCP manifest delivery intent: %v", err)
	}
	redriveSender := &recordingRuntimeCommandSender{response: &agentruntimev1.RuntimeInputCommandResponse{
		Status: agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED,
	}}
	redriveStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	redriveResult, err := (RuntimePodDirectDeliverer{Store: redriveStore, Sender: redriveSender}).DeliverRuntimeJob(context.Background(), redrivenJob)
	if err != nil {
		t.Fatalf("redrive committed MCP manifest generation: %v", err)
	}
	if redriveResult.Status != RuntimeDeliveryAccepted || len(redriveSender.requests) != 1 ||
		redriveSender.requests[0].GetRuntimeInputId() != "runtime_config_update:mcp_manifest:sesn_bridge_initial_mcp:github:1" {
		t.Fatalf("redriven manifest delivery = %#v requests %#v; want one accepted generation-1 command", redriveResult, redriveSender.requests)
	}
	var delivered struct {
		MCPManifest map[string]json.RawMessage `json:"mcp_manifest"`
	}
	if err := json.Unmarshal([]byte(redriveSender.requests[0].GetPayloadJson()), &delivered); err != nil {
		t.Fatalf("decode rebuilt MCP command: %v", err)
	}
	var manifestETag string
	var tools []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(delivered.MCPManifest["manifest_etag"], &manifestETag); err != nil {
		t.Fatalf("decode rebuilt manifest etag: %v", err)
	}
	if err := json.Unmarshal(delivered.MCPManifest["tools"], &tools); err != nil {
		t.Fatalf("decode rebuilt manifest tools: %v", err)
	}
	if manifestETag != "etag_initial" || len(tools) != 1 || tools[0].Name != "github_search" {
		t.Fatalf("rebuilt MCP command = %#v; want durable manifest row content", delivered)
	}
	for _, forbidden := range []string{"default_config", "configs"} {
		if _, exists := delivered.MCPManifest[forbidden]; exists {
			t.Fatalf("rebuilt MCP command carries forbidden policy key %q: %#v", forbidden, delivered)
		}
	}
	if len(lister.requests) != 1 {
		t.Fatalf("connector list calls after durable redrive = %d; want original capture call only", len(lister.requests))
	}
}

func TestJobRunnerRuntimeDeliveryStoreDiscoversInitialMCPManifestThroughProductionAssembly(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		workspaceID = "default"
		sessionID   = "sesn_job_runner_initial_mcp"
		threadID    = "thr_job_runner_initial_mcp"
		eventID     = "evt_job_runner_initial_mcp"
	)
	seedBridgeAPISession(t, admin, workspaceID, sessionID, threadID)
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET installed_tools_json = '{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"enabled":false,"permission_policy":{"type":"always_ask"}},"configs":[{"name":"github_search","enabled":true,"permission_policy":{"type":"always_allow"}}]}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}]}' WHERE workspace_id = $1 AND id = $2`, workspaceID, sessionID); err != nil {
		t.Fatalf("seed durable initial MCP config: %v", err)
	}
	seedBridgeAPIPreparationReady(t, admin, workspaceID, sessionID, "prep_job_runner_initial_mcp")
	seedBridgeAPIActiveSandbox(t, admin, workspaceID, sessionID, "2026-01-01T00:00:30Z")
	seedBridgeAPIEvent(t, admin, workspaceID, sessionID, threadID, eventID, 1, "user.message", `{"content":[{"type":"text","text":"hello"}]}`)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for MCP connector: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	connector := &recordingMCPConnectorServer{}
	server := grpc.NewServer()
	providergatewayv1.RegisterMcpConnectorServiceServer(server, connector)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	tokenPath := t.TempDir() + "/gateway-token"
	if err := os.WriteFile(tokenPath, []byte("test-gateway-token"), 0o600); err != nil {
		t.Fatalf("write gateway token: %v", err)
	}
	candidate := enginekubernetes.BindingCandidate{
		Namespace: "tetral-agent-runtime",
		PodName:   "runtime-pod-initial-mcp",
		PodUID:    "pod-uid-initial-mcp",
		PodIP:     "10.0.0.42",
	}
	store := NewJobRunnerRuntimeDeliveryStore(
		dbconnect.NewClientForTesting(runtime),
		nil,
		JobRunnerConfig{
			AgentRuntimeGRPCPort:            9090,
			MCPConnectorGRPCAddress:         listener.Addr().String(),
			GatewayTokenPath:                tokenPath,
			SandboxServiceGRPCAddress:       "sandbox.invalid:9090",
			SandboxServiceTokenPath:         "/unused-sandbox-token",
			SandboxStatusFreshnessWindow:    time.Minute,
			ResourceCredentialRefreshMargin: 30 * time.Minute,
		},
		func() enginekubernetes.BindingVisibilitySnapshot {
			return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{candidate})
		},
	)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	job := RuntimeJob{
		JobID:                "qjob_job_runner_initial_mcp",
		LeaseToken:           "lease_job_runner_initial_mcp",
		Kind:                 queue.KindRuntimeInput,
		WorkspaceID:          workspaceID,
		SessionID:            sessionID,
		PreparationAttemptID: "prep_job_runner_initial_mcp",
		SessionThreadID:      threadID,
		RuntimeInputID:       "rin_job_runner_initial_mcp",
		EventIDs:             []string{eventID},
		SequenceFrom:         1,
		SequenceTo:           1,
		InputKind:            "messages",
		CommandKind:          agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:          `{"type":"messages"}`,
	}

	_, firstErr := store.PrepareRuntimeCommand(context.Background(), job)
	var pendingErr runtimeDeliveryPrepareError
	if !errors.As(firstErr, &pendingErr) ||
		pendingErr.kind != "mcp_manifest_discovery_pending" ||
		!pendingErr.retryable {
		t.Fatalf("first PrepareRuntimeCommand error = %#v; want retryable mcp_manifest_discovery_pending", firstErr)
	}
	requests := connector.recordedRequests()
	if len(requests) != 1 ||
		requests[0].GetWorkspaceId() != workspaceID ||
		requests[0].GetSessionId() != sessionID ||
		requests[0].GetMcpServerName() != "github" {
		t.Errorf("connector requests = %#v; want one request for %s/%s/github", requests, workspaceID, sessionID)
	}
	var readiness string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT readiness FROM session_mcp_manifests WHERE workspace_id = $1 AND session_id = $2 AND mcp_server_name = 'github'`,
		workspaceID, sessionID,
	).Scan(&readiness); err != nil {
		t.Fatalf("read captured MCP manifest: %v", err)
	}
	if readiness != "ready" {
		t.Fatalf("captured MCP manifest readiness = %q; want ready", readiness)
	}
	plan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil {
		t.Fatalf("second PrepareRuntimeCommand: %v", err)
	}
	if plan.Request == nil || plan.Request.GetRuntimeInputId() != job.RuntimeInputID {
		t.Fatalf("second PrepareRuntimeCommand plan = %#v; want runtime input delivery", plan)
	}
}

func TestInitialMCPManifestListUsesFreshRPCOnlyDeadline(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_mcp_list_deadline"
	seedBridgeAPISession(t, admin, "default", sessionID, "thr_mcp_list_deadline")
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET installed_tools_json = '{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"github"}]}' WHERE workspace_id = 'default' AND id = $1`, sessionID); err != nil {
		t.Fatalf("seed deadline MCP toolsets: %v", err)
	}
	lister := &expiringMCPManifestLister{}
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.MCPManifestLister = lister
	err := store.enqueueInitialMCPManifestUpdatesWithListTimeout(
		context.Background(),
		RuntimeJob{WorkspaceID: "default", SessionID: sessionID},
		[]MCPManifestToolsetConfig{
			{MCPServerName: "github", BuiltinFamily: "claude"},
			{MCPServerName: "github", BuiltinFamily: "claude"},
		},
		time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC),
		10*time.Millisecond,
	)
	if err != nil {
		t.Fatalf("enqueueInitialMCPManifestUpdatesWithListTimeout: %v", err)
	}
	if len(lister.contexts) != 2 || lister.contexts[0] == lister.contexts[1] {
		t.Fatalf("MCP list contexts = %#v; want one fresh context per toolset", lister.contexts)
	}
	for index, listCtx := range lister.contexts {
		if listCtx.Err() != context.DeadlineExceeded {
			t.Fatalf("MCP list context %d error = %v; want deadline exceeded before persistence", index, listCtx.Err())
		}
	}
	var readyRows int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_mcp_manifests WHERE workspace_id = 'default' AND session_id = $1 AND readiness = 'ready'`,
		sessionID,
	).Scan(&readyRows); err != nil {
		t.Fatalf("count persisted MCP manifests: %v", err)
	}
	if readyRows != 1 {
		t.Fatalf("persisted ready MCP manifests = %d; want 1 duplicate-safe row after both list contexts expired", readyRows)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreBuildsTaskNotificationFromBackgroundTask(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_task_delivery", "thr_bridge_task_delivery")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_task_delivery", "bind_bridge_task_delivery", 1, "pod_uid_task_delivery")
	seedBridgeAPIBackgroundTask(t, admin, "default", "sesn_bridge_task_delivery", "thr_bridge_task_delivery", "bind_bridge_task_delivery", "task_bridge_delivery", "sevt_tool_delivery")
	storedResult := fmt.Sprintf(
		`{"status":"completed","stdout":{"text":%q,"truncated":false,"total_bytes":51200,"total_lines":5000},"stderr":{"text":%q,"truncated":false,"total_bytes":51200,"total_lines":6000},"provider_command_id":"must_not_escape"}`,
		strings.Repeat("out-", 12800),
		strings.Repeat("err-", 12800),
	)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_background_tasks
		SET status='completed', terminal_result_json=$1, terminal_result_digest='digest', terminal_at='2026-01-01T00:01:00Z'
		WHERE workspace_id='default' AND session_id='sesn_bridge_task_delivery' AND task_id='task_bridge_delivery'`, storedResult); err != nil {
		t.Fatalf("seed terminal background task: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_inbox (
		workspace_id, session_id, session_thread_id, runtime_input_id, input_kind, event_ids_json,
		status, created_at, updated_at
	) VALUES ('default','sesn_bridge_task_delivery','thr_bridge_task_delivery','task_notification:task_bridge_delivery',
		'task_notification','[]','queued','2026-01-01T00:01:00Z','2026-01-01T00:01:00Z')`); err != nil {
		t.Fatalf("seed queued task notification: %v", err)
	}
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC) }
	resolver := &recordingRuntimeTargetResolver{binding: runtimeBindingForDelivery{
		BindingID:         "bind_bridge_task_delivery",
		BindingGeneration: 1,
		Namespace:         "runtime-ns",
		PodName:           "runtime-pod",
		PodUID:            "pod_uid_task_delivery",
		PodIP:             "10.0.0.1",
	}}
	store.TargetResolver = resolver

	job := RuntimeJob{
		JobID:           "qjob_bridge_task_delivery",
		LeaseToken:      "lease_bridge_task_delivery",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       "sesn_bridge_task_delivery",
		SessionThreadID: "thr_bridge_task_delivery",
		RuntimeInputID:  "task_notification:task_bridge_delivery",
		EventIDs:        []string{},
		InputKind:       "task_notification",
		CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_TASK_NOTIFICATION,
		PayloadJSON:     `{"workspace_id":"default","session_id":"sesn_bridge_task_delivery","session_thread_id":"thr_bridge_task_delivery","runtime_input_id":"task_notification:task_bridge_delivery","event_ids":[],"input_kind":"task_notification"}`,
	}
	plan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand task notification: %v", err)
	}
	if plan.TaskNotification == nil || plan.TaskNotification.TaskID != "task_bridge_delivery" {
		t.Fatalf("prepared task notification plan = %#v", plan)
	}
	if len(resolver.jobs) != 1 || resolver.jobs[0].RuntimeInputID != job.RuntimeInputID || resolver.jobs[0].SessionThreadID != "thr_bridge_task_delivery" {
		t.Fatalf("target resolver jobs = %+v; want task notification delivery resolved through runtime target", resolver.jobs)
	}
	if strings.Contains(plan.Request.GetPayloadJson(), "provider_command_id") {
		t.Fatalf("runtime task notification leaked provider metadata: %s", plan.Request.GetPayloadJson())
	}
	var payload struct {
		TaskID               string `json:"task_id"`
		SourceToolUseEventID string `json:"source_tool_use_event_id"`
		Status               string `json:"status"`
		Stdout               struct {
			Truncated     bool  `json:"truncated"`
			OriginalBytes int64 `json:"original_bytes"`
			OriginalLines int64 `json:"original_lines"`
		} `json:"stdout"`
		Stderr struct {
			Truncated     bool  `json:"truncated"`
			OriginalBytes int64 `json:"original_bytes"`
			OriginalLines int64 `json:"original_lines"`
		} `json:"stderr"`
	}
	if err := json.Unmarshal([]byte(plan.Request.GetPayloadJson()), &payload); err != nil {
		t.Fatalf("decode runtime task notification payload: %v", err)
	}
	if payload.TaskID != "task_bridge_delivery" || payload.SourceToolUseEventID != "sevt_tool_delivery" || payload.Status != "completed" {
		t.Fatalf("runtime task notification payload = %#v", payload)
	}
	if len([]byte(plan.Request.GetPayloadJson())) > runtimeTaskNotificationPayloadMaxBytes {
		t.Fatalf("runtime task notification payload bytes = %d; want <= %d", len([]byte(plan.Request.GetPayloadJson())), runtimeTaskNotificationPayloadMaxBytes)
	}
	if !payload.Stdout.Truncated || !payload.Stderr.Truncated ||
		payload.Stdout.OriginalBytes != 51200 || payload.Stdout.OriginalLines != 5000 ||
		payload.Stderr.OriginalBytes != 51200 || payload.Stderr.OriginalLines != 6000 {
		t.Fatalf("runtime task notification stream bounds = stdout:%+v stderr:%+v", payload.Stdout, payload.Stderr)
	}
	assertNoTaskOutputPaths(t, plan.Request.GetPayloadJson())
	apiStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	apiStore.Clock = store.Clock
	committed, err := apiStore.CommitTaskNotificationResult(context.Background(), bridgeTaskNotificationRequestForTest(
		t,
		runtimeScopeFromCommandRequest(plan.Request),
		job.RuntimeInputID,
		plan.TaskNotification.TaskID,
		plan.TaskNotification.ResultJSON,
	))
	if err != nil {
		t.Fatalf("CommitTaskNotificationResult delivery: %v", err)
	}
	if committed.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("CommitTaskNotificationResult ack = %s; want committed", committed.GetAck().GetStatus())
	}
	var taskStatus string
	var inboxStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status FROM session_background_tasks WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_task_delivery' AND task_id = 'task_bridge_delivery'`).Scan(&taskStatus); err != nil {
		t.Fatalf("read task delivery status: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status FROM session_runtime_inbox WHERE workspace_id = 'default' AND runtime_input_id = 'task_notification:task_bridge_delivery'`).Scan(&inboxStatus); err != nil {
		t.Fatalf("read task delivery inbox status: %v", err)
	}
	if taskStatus != "completed" || inboxStatus != "committed" {
		t.Fatalf("task delivery status=%q inbox=%q; want completed/committed", taskStatus, inboxStatus)
	}
}

func TestRuntimeConfigDeliveryRebuildsTheColdBootstrapAgentSettings(t *testing.T) {
	for _, test := range []struct {
		name          string
		sessionID     string
		agentConfig   string
		installedJSON string
		wantSystem    string
	}{
		{
			name:          "configured MCP policy",
			sessionID:     "sesn_runtime_config_policy",
			agentConfig:   `{"name":"agent","model":"anthropic/claude-opus-4-8","system":"Operate as the session specialist.","tools":[{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"enabled":false,"permission_policy":{"type":"always_ask"}},"configs":[{"name":"github_search","enabled":true,"permission_policy":{"type":"always_allow"}}]}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}],"skills":[],"metadata":{}}`,
			installedJSON: `{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"enabled":false,"permission_policy":{"type":"always_ask"}},"configs":[{"name":"github_search","enabled":true,"permission_policy":{"type":"always_allow"}}]}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}]}`,
			wantSystem:    "Operate as the session specialist.",
		},
		{
			name:          "empty MCP policy",
			sessionID:     "sesn_runtime_config_policy_empty",
			agentConfig:   `{"name":"agent","model":"anthropic/claude-opus-4-8","system":null,"tools":[],"mcp_servers":[],"skills":[],"metadata":{}}`,
			installedJSON: `{"tools":[{"type":"tetral_agent_toolset","family":"claude"}],"mcp_servers":[]}`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			seedBridgeAPISession(t, admin, "default", test.sessionID, "thr_"+test.sessionID)
			seedBridgeAPIAgentConfig(t, admin, "default", test.sessionID, test.agentConfig)
			seedBridgeAPIWritableMemoryStore(t, admin, "default", test.sessionID, "memstore_runtime_config")
			if _, err := admin.ExecContext(context.Background(),
				`UPDATE session_memory_store_resources
				    SET name = 'Project notes', instructions = 'Preserve this guidance.'
				  WHERE workspace_id = 'default' AND session_id = $1`, test.sessionID); err != nil {
				t.Fatalf("seed runtime memory guidance: %v", err)
			}
			if _, err := admin.ExecContext(context.Background(),
				`UPDATE sessions SET approval_mode = 'approve_for_me', config_generation = 7, installed_tools_json = $1
				  WHERE workspace_id = 'default' AND id = $2`, test.installedJSON, test.sessionID); err != nil {
				t.Fatalf("seed runtime config: %v", err)
			}

			var payloadJSON string
			var runtimeInputID string
			client := dbconnect.NewClientForTesting(runtime)
			if err := client.WithWorkspaceTx(context.Background(), "default", "test.rebuild_runtime_config", func(tx *dbconnect.Tx) error {
				var err error
				payloadJSON, runtimeInputID, err = runtimeCommandPayloadForJobTx(context.Background(), tx, RuntimeJob{
					Kind: queue.KindRuntimeConfigUpdate, WorkspaceID: "default", SessionID: test.sessionID, ConfigGeneration: "1",
				})
				return err
			}); err != nil {
				t.Fatalf("rebuild runtime config: %v", err)
			}
			if runtimeInputID != runtimeConfigUpdateInputID(test.sessionID, "7") {
				t.Fatalf("rebuilt runtime input id = %q; want current generation 7", runtimeInputID)
			}
			var payload struct {
				ToolPolicy   any                        `json:"tool_policy"`
				System       *string                    `json:"system"`
				MemoryStores []bridgeRuntimeMemoryStore `json:"memory_stores"`
			}
			if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
				t.Fatalf("decode rebuilt runtime config: %v", err)
			}
			var payloadFields map[string]json.RawMessage
			if err := json.Unmarshal([]byte(payloadJSON), &payloadFields); err != nil {
				t.Fatalf("decode rebuilt runtime config fields: %v", err)
			}
			if _, ok := payloadFields["system"]; !ok {
				t.Fatalf("rebuilt runtime config omits nullable system: %s", payloadJSON)
			}
			settings, err := bridgeRuntimeSessionAgentSettings("approve_for_me", test.agentConfig, test.installedJSON, payload.MemoryStores)
			if err != nil {
				t.Fatalf("resolve cold-bootstrap agent settings: %v", err)
			}
			wantJSON, err := json.Marshal(settings.ToolPolicy)
			if err != nil {
				t.Fatalf("marshal cold-bootstrap policy: %v", err)
			}
			var want any
			if err := json.Unmarshal(wantJSON, &want); err != nil {
				t.Fatalf("decode cold-bootstrap policy: %v", err)
			}
			if !reflect.DeepEqual(payload.ToolPolicy, want) {
				t.Fatalf("rebuilt policy = %#v; cold-bootstrap policy = %#v", payload.ToolPolicy, want)
			}
			if !reflect.DeepEqual(payload.MemoryStores, settings.MemoryStores) || len(payload.MemoryStores) != 1 ||
				payload.MemoryStores[0].MemoryStoreID != "memstore_runtime_config" ||
				payload.MemoryStores[0].Name != "Project notes" ||
				payload.MemoryStores[0].Access != "read_write" ||
				payload.MemoryStores[0].Instructions == nil || *payload.MemoryStores[0].Instructions != "Preserve this guidance." {
				t.Fatalf("rebuilt memory stores = %#v; cold-bootstrap memory stores = %#v", payload.MemoryStores, settings.MemoryStores)
			}
			if test.wantSystem == "" {
				if payload.System != nil || settings.System != nil {
					t.Fatalf("rebuilt system = %#v; cold-bootstrap system = %#v; want nil", payload.System, settings.System)
				}
			} else if payload.System == nil || settings.System == nil || *payload.System != test.wantSystem || *settings.System != test.wantSystem {
				t.Fatalf("rebuilt system = %#v; cold-bootstrap system = %#v; want %q", payload.System, settings.System, test.wantSystem)
			}
		})
	}
}

func TestManifestServerSetIsSubsetOfColdToolPolicyMCPToolsets(t *testing.T) {
	const sessionID = "sesn_manifest_policy_subset"
	const agentConfig = `{"name":"agent","model":"anthropic/claude-opus-4-8","tools":[],"mcp_servers":[],"skills":[],"metadata":{}}`
	const installedConfig = `{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"github"}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"},{"type":"url","name":"unused","url":"https://unused.example/mcp/"}]}`
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", sessionID, "thr_manifest_policy_subset")
	seedBridgeAPIAgentConfig(t, admin, "default", sessionID, agentConfig)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE sessions SET installed_tools_json = $1 WHERE workspace_id = 'default' AND id = $2`, installedConfig, sessionID); err != nil {
		t.Fatalf("seed installed tool policy: %v", err)
	}

	var manifestToolsets []MCPManifestToolsetConfig
	client := dbconnect.NewClientForTesting(runtime)
	if err := client.WithWorkspaceTx(context.Background(), "default", "test.manifest_policy_subset", func(tx *dbconnect.Tx) error {
		var err error
		manifestToolsets, err = sessionAgentMCPManifestToolsetsTx(context.Background(), tx, "default", sessionID)
		return err
	}); err != nil {
		t.Fatalf("read manifest toolsets: %v", err)
	}
	config, err := effectiveBridgeRuntimeAgentConfig(agentConfig, installedConfig)
	if err != nil {
		t.Fatalf("resolve cold tool policy: %v", err)
	}
	policyJSON, err := json.Marshal(bridgeRuntimeToolPolicy("approve_for_me", config))
	if err != nil {
		t.Fatalf("marshal cold tool policy: %v", err)
	}
	var policy struct {
		MCPToolsets []struct {
			MCPServerName string `json:"mcpServerName"`
		} `json:"mcpToolsets"`
	}
	if err := json.Unmarshal(policyJSON, &policy); err != nil {
		t.Fatalf("decode cold tool policy: %v", err)
	}
	held := make(map[string]bool, len(policy.MCPToolsets))
	for _, toolset := range policy.MCPToolsets {
		held[toolset.MCPServerName] = true
	}
	for _, toolset := range manifestToolsets {
		if !held[toolset.MCPServerName] {
			t.Fatalf("manifest server %q is absent from cold policy MCP toolsets %v", toolset.MCPServerName, held)
		}
	}
	if len(manifestToolsets) != 1 || manifestToolsets[0].MCPServerName != "github" || held["unused"] {
		t.Fatalf("manifest toolsets=%+v held=%v; want only declared github toolset", manifestToolsets, held)
	}
}

func TestRuntimeConfigDeliveryRebuildsManifestWithoutConsultingToolPolicy(t *testing.T) {
	const sessionID = "sesn_manifest_single_row"
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", sessionID, "thr_manifest_single_row")
	seedBridgeAPIAgentConfig(t, admin, "default", sessionID, `{
		"name":"agent",
		"model":"anthropic/claude-opus-4-8",
		"tools":[],
		"mcp_servers":[],
		"skills":[],
		"metadata":{}
	}`)
	insertAcceptedMCPManifest(t, admin, sessionID, "etag_single_row", 4, "github_single_row")

	var payloadJSON string
	var runtimeInputID string
	client := dbconnect.NewClientForTesting(runtime)
	if err := client.WithWorkspaceTx(context.Background(), "default", "test.manifest_single_row", func(tx *dbconnect.Tx) error {
		var err error
		payloadJSON, runtimeInputID, err = runtimeCommandPayloadForJobTx(context.Background(), tx, RuntimeJob{
			Kind: queue.KindRuntimeConfigUpdate, WorkspaceID: "default", SessionID: sessionID,
			MCPServerName: "github", MCPManifestGeneration: "1",
		})
		return err
	}); err != nil {
		t.Fatalf("rebuild MCP manifest without held policy: %v", err)
	}
	if runtimeInputID != runtimeMCPManifestInputID(sessionID, "github", 4) {
		t.Fatalf("rebuilt runtime input id = %q; want durable generation 4", runtimeInputID)
	}
	var payload struct {
		Manifest struct {
			ServerName string `json:"mcp_server_name"`
			Tools      []struct {
				Name string `json:"name"`
			} `json:"tools"`
		} `json:"mcp_manifest"`
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode rebuilt MCP manifest: %v", err)
	}
	if payload.Manifest.ServerName != "github" || len(payload.Manifest.Tools) != 1 || payload.Manifest.Tools[0].Name != "github_single_row" {
		t.Fatalf("rebuilt MCP manifest = %+v; want durable row content", payload.Manifest)
	}
}

func TestRuntimeConfigDeliveryRebuildsSupersededManifestAtTheCurrentGeneration(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_runtime_manifest_superseded"
	seedMCPFamilySession(t, admin, sessionID, "thr_"+sessionID, "claude")
	seedBridgeAPIPreparationReady(t, admin, "default", sessionID, "prep_runtime_manifest_superseded")
	seedBridgeAPIActiveSandbox(t, admin, "default", sessionID, "2026-01-01T00:00:30Z")
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_runtime_manifest_superseded", 1, "pod_runtime_manifest_superseded")
	insertAcceptedMCPManifest(t, admin, sessionID, "etag_old", 1, "github_old")
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_mcp_manifests
		    SET tools_json = '[{"name":"github_current","description":"current","input_schema":{"type":"object"}}]',
		        manifest_etag = 'etag_current', manifest_generation = 2, updated_at = '2026-01-01T00:00:20Z'
		  WHERE workspace_id = 'default' AND session_id = $1 AND mcp_server_name = 'github'`, sessionID); err != nil {
		t.Fatalf("advance durable manifest: %v", err)
	}

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	sender := &recordingRuntimeCommandSender{response: &agentruntimev1.RuntimeInputCommandResponse{
		Status: agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED,
	}}
	result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), RuntimeJob{
		Kind: queue.KindRuntimeConfigUpdate, WorkspaceID: "default", SessionID: sessionID,
		RuntimeInputID: runtimeMCPManifestInputID(sessionID, "github", 1), MCPServerName: "github", MCPManifestGeneration: "1",
	})
	if err != nil {
		t.Fatalf("deliver superseded manifest intent: %v", err)
	}
	if result.Status != RuntimeDeliveryAccepted || len(sender.requests) != 1 {
		t.Fatalf("superseded manifest result = %#v requests=%d; want one accepted fresh apply", result, len(sender.requests))
	}
	request := sender.requests[0]
	if request.GetRuntimeInputId() != runtimeMCPManifestInputID(sessionID, "github", 2) ||
		!strings.Contains(request.GetPayloadJson(), `"manifest_generation":2`) ||
		!strings.Contains(request.GetPayloadJson(), `"name":"github_current"`) ||
		strings.Contains(request.GetPayloadJson(), `github_old`) {
		t.Fatalf("rebuilt superseded command id/payload = %q/%s; want current generation and content", request.GetRuntimeInputId(), request.GetPayloadJson())
	}
}

func TestPostgreSQLRuntimeDeliveryStoreTaskNotificationTerminalDuplicateIsStale(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_task_delivery_terminal_dup", "thr_bridge_task_delivery_terminal_dup")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_task_delivery_terminal_dup", "bind_bridge_task_delivery_terminal_dup", 1, "pod_uid_task_delivery_terminal_dup")
	seedBridgeAPIBackgroundTask(t, admin, "default", "sesn_bridge_task_delivery_terminal_dup", "thr_bridge_task_delivery_terminal_dup", "bind_bridge_task_delivery_terminal_dup", "task_bridge_delivery_terminal_dup", "sevt_tool_delivery_terminal_dup")
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_background_tasks
		    SET status = 'completed', terminal_event_id = 'sevt_terminal_dup', updated_at = '2026-01-01T00:20:00Z'
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_task_delivery_terminal_dup'
		    AND task_id = 'task_bridge_delivery_terminal_dup'`); err != nil {
		t.Fatalf("mark terminal task: %v", err)
	}

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 40, 1, 0, time.UTC) }
	store.SandboxStatusFreshnessWindow = time.Minute
	resolver := &recordingRuntimeTargetResolver{err: errors.New("resolver must not run for terminal duplicate task notification")}
	store.TargetResolver = resolver
	plan, err := store.PrepareRuntimeCommand(context.Background(), RuntimeJob{
		JobID:           "qjob_bridge_task_delivery_terminal_dup",
		LeaseToken:      "lease_bridge_task_delivery_terminal_dup",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       "sesn_bridge_task_delivery_terminal_dup",
		SessionThreadID: "thr_bridge_task_delivery_terminal_dup",
		RuntimeInputID:  "task_notification:task_bridge_delivery_terminal_dup",
		InputKind:       "task_notification",
		CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_TASK_NOTIFICATION,
		PayloadJSON:     `{"workspace_id":"default","session_id":"sesn_bridge_task_delivery_terminal_dup","session_thread_id":"thr_bridge_task_delivery_terminal_dup","runtime_input_id":"task_notification:task_bridge_delivery_terminal_dup","event_ids":[],"input_kind":"task_notification"}`,
	})
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand terminal duplicate task notification: %v", err)
	}
	if !plan.StaleAccepted || plan.Request != nil || plan.TaskNotification != nil {
		t.Fatalf("plan = %#v; want terminal duplicate stale accepted without runtime command", plan)
	}
	assertNoRuntimeInboxRow(t, admin, "task_notification:task_bridge_delivery_terminal_dup")
	if len(resolver.jobs) != 0 {
		t.Fatalf("target resolver jobs = %+v; want none for terminal duplicate task notification", resolver.jobs)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreRequeuesOlderPendingInboxBeforeLaterInput(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_inbox_repair", "thr_bridge_inbox_repair")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_inbox_repair", "bind_bridge_inbox_repair", 1, "pod_uid_inbox_repair")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_inbox_repair", "prep_bridge_inbox_repair")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_inbox_repair", "2026-01-01T00:04:00Z")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_inbox_repair", "thr_bridge_inbox_repair", "sevt_repair_old", 1, "user.message", `{"content":[{"type":"text","text":"old"}]}`)
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_inbox_repair", "thr_bridge_inbox_repair", "sevt_repair_later", 2, "user.message", `{"content":[{"type":"text","text":"later"}]}`)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_events
		    SET preparation_attempt_id = 'prep_bridge_inbox_repair_birth'
		  WHERE workspace_id = 'default'
		    AND event_id = 'sevt_repair_old'`); err != nil {
		t.Fatalf("stamp earlier inbox event birth: %v", err)
	}
	seedBridgeAPIRuntimeInbox(t, admin, "default", "sesn_bridge_inbox_repair", "thr_bridge_inbox_repair", "rin_repair_old", "messages", `["sevt_repair_old"]`, "accepted", "bind_bridge_inbox_repair", "pod_uid_inbox_repair", 1, 1)

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 4, 1, 0, time.UTC) }
	_, err := store.PrepareRuntimeCommand(context.Background(), RuntimeJob{
		JobID:                "qjob_repair_later",
		LeaseToken:           "lease_repair_later",
		Kind:                 queue.KindRuntimeInput,
		WorkspaceID:          "default",
		SessionID:            "sesn_bridge_inbox_repair",
		PreparationAttemptID: "prep_bridge_inbox_repair",
		SessionThreadID:      "thr_bridge_inbox_repair",
		RuntimeInputID:       "rin_repair_later",
		EventIDs:             []string{"sevt_repair_later"},
		SequenceFrom:         2,
		SequenceTo:           2,
		InputKind:            "messages",
		CommandKind:          agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:          `{"workspace_id":"default","session_id":"sesn_bridge_inbox_repair","session_thread_id":"thr_bridge_inbox_repair","runtime_input_id":"rin_repair_later","event_ids":["sevt_repair_later"],"sequence_from":2,"sequence_to":2,"input_kind":"messages"}`,
	})
	var prepareErr runtimeDeliveryPrepareError
	if !errors.As(err, &prepareErr) || prepareErr.kind != "runtime_inbox_repair_pending" || !prepareErr.retryable {
		t.Fatalf("PrepareRuntimeCommand later err = %v; want retryable runtime_inbox_repair_pending", err)
	}
	assertRuntimeInputRepairQueueJob(t, admin, "default", "sesn_bridge_inbox_repair", "thr_bridge_inbox_repair", "rin_repair_old", []string{"sevt_repair_old"}, 1, 1, "messages", 0)
	var repairBirth string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT payload_json::jsonb ->> 'preparation_attempt_id'
		   FROM queue_jobs
		  WHERE workspace_id = 'default'
		    AND kind = 'runtime_input'
		    AND dedupe_key = $1`,
		queue.FormatRuntimeInputDedupeKey("default", "sesn_bridge_inbox_repair", "rin_repair_old"),
	).Scan(&repairBirth); err != nil {
		t.Fatalf("read runtime input repair birth: %v", err)
	}
	if repairBirth != "prep_bridge_inbox_repair_birth" {
		t.Fatalf("runtime input repair birth = %q; want event-recorded birth", repairBirth)
	}
	var laterInboxRows int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_runtime_inbox
		  WHERE workspace_id = 'default'
		    AND runtime_input_id = 'rin_repair_later'`).Scan(&laterInboxRows); err != nil {
		t.Fatalf("count later inbox rows: %v", err)
	}
	if laterInboxRows != 0 {
		t.Fatalf("later inbox rows = %d; want 0 while earlier repair is pending", laterInboxRows)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreCancelsSupersededEarlierInboxBeforeLaterInput(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_inbox_repair_superseded", "thr_bridge_inbox_repair_superseded")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_inbox_repair_superseded", "bind_bridge_inbox_repair_superseded", 1, "pod_uid_inbox_repair_superseded")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_inbox_repair_superseded", "prep_bridge_inbox_repair_superseded")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_inbox_repair_superseded", "2026-01-01T00:04:00Z")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_inbox_repair_superseded", "thr_bridge_inbox_repair_superseded", "sevt_repair_superseded_old", 1, "user.message", `{"content":[{"type":"text","text":"old"}]}`)
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_inbox_repair_superseded", "thr_bridge_inbox_repair_superseded", "sevt_repair_superseded_interrupt", 2, "user.interrupt", `{}`)
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_inbox_repair_superseded", "thr_bridge_inbox_repair_superseded", "sevt_repair_superseded_later", 3, "user.message", `{"content":[{"type":"text","text":"later"}]}`)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_events
		    SET processed_at = '2026-01-01T00:03:01Z'
		  WHERE workspace_id = 'default'
		    AND event_id = 'sevt_repair_superseded_interrupt'`); err != nil {
		t.Fatalf("mark interrupt processed: %v", err)
	}
	seedBridgeAPIRuntimeInbox(t, admin, "default", "sesn_bridge_inbox_repair_superseded", "thr_bridge_inbox_repair_superseded", "rin_repair_superseded_old", "messages", `["sevt_repair_superseded_old"]`, "accepted", "bind_bridge_inbox_repair_superseded", "pod_uid_inbox_repair_superseded", 1, 1)

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 4, 1, 0, time.UTC) }
	_, err := store.PrepareRuntimeCommand(context.Background(), RuntimeJob{
		JobID:                "qjob_repair_superseded_later",
		LeaseToken:           "lease_repair_superseded_later",
		Kind:                 queue.KindRuntimeInput,
		WorkspaceID:          "default",
		SessionID:            "sesn_bridge_inbox_repair_superseded",
		PreparationAttemptID: "prep_bridge_inbox_repair_superseded",
		SessionThreadID:      "thr_bridge_inbox_repair_superseded",
		RuntimeInputID:       "rin_repair_superseded_later",
		EventIDs:             []string{"sevt_repair_superseded_later"},
		SequenceFrom:         3,
		SequenceTo:           3,
		InputKind:            "messages",
		CommandKind:          agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:          `{"workspace_id":"default","session_id":"sesn_bridge_inbox_repair_superseded","session_thread_id":"thr_bridge_inbox_repair_superseded","runtime_input_id":"rin_repair_superseded_later","event_ids":["sevt_repair_superseded_later"],"sequence_from":3,"sequence_to":3,"input_kind":"messages"}`,
	})
	var prepareErr runtimeDeliveryPrepareError
	if !errors.As(err, &prepareErr) || prepareErr.kind != "runtime_inbox_repair_pending" || !prepareErr.retryable {
		t.Fatalf("PrepareRuntimeCommand later err = %v; want retryable runtime_inbox_repair_pending", err)
	}
	var oldInboxStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status
		   FROM session_runtime_inbox
		  WHERE workspace_id = 'default'
		    AND runtime_input_id = 'rin_repair_superseded_old'`).Scan(&oldInboxStatus); err != nil {
		t.Fatalf("read superseded earlier inbox: %v", err)
	}
	if oldInboxStatus != "cancelled" {
		t.Fatalf("superseded earlier inbox status = %q; want cancelled", oldInboxStatus)
	}
	assertNoRuntimeInputRepairQueueJob(t, admin, "default", "sesn_bridge_inbox_repair_superseded", "rin_repair_superseded_old")
}

func TestPostgreSQLRuntimeDeliveryStoreRepairsAcceptedInboxWithoutLaterInput(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_inbox_standalone_repair", "thr_bridge_inbox_standalone_repair")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_inbox_standalone_repair", "prep_bridge_inbox_standalone_repair")
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_preparations (
			workspace_id, session_id, preparation_attempt_id, environment_id, environment_generation,
			sandbox_id, status, failure_stage, last_error_kind, failure_reason, retryable,
			created_at, updated_at, failed_at, superseded_at
		) VALUES (
			'default', 'sesn_bridge_inbox_standalone_repair', 'prep_bridge_inbox_standalone_birth',
			'env_sesn_bridge_inbox_standalone_repair', 1, 'sandbox_sesn_bridge_inbox_standalone_repair',
			'failed', 'resource_preparation', 'provider_unavailable', 'provider unavailable', FALSE,
			'2025-12-31T23:59:00Z', '2026-01-01T00:00:00Z',
			'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		)`); err != nil {
		t.Fatalf("seed standalone repair birth preparation: %v", err)
	}
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_inbox_standalone_repair", "thr_bridge_inbox_standalone_repair", "sevt_standalone_repair", 1, "user.message", `{"content":[{"type":"text","text":"lost after ack"}]}`)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_events
		    SET preparation_attempt_id = 'prep_bridge_inbox_standalone_birth'
		  WHERE workspace_id = 'default'
		    AND event_id = 'sevt_standalone_repair'`); err != nil {
		t.Fatalf("stamp standalone repair event birth: %v", err)
	}
	seedBridgeAPIRuntimeInbox(t, admin, "default", "sesn_bridge_inbox_standalone_repair", "thr_bridge_inbox_standalone_repair", "rin_standalone_repair", "messages", `["sevt_standalone_repair"]`, "accepted", "bind_bridge_standalone_repair", "pod_uid_standalone_repair", 1, 1)

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC) }
	repaired, err := store.RepairRuntimeInbox(context.Background(), "default", 10)
	if err != nil {
		t.Fatalf("RepairRuntimeInbox: %v", err)
	}
	if repaired != 1 {
		t.Fatalf("repaired = %d; want 1", repaired)
	}
	assertRuntimeInputRepairQueueJob(t, admin, "default", "sesn_bridge_inbox_standalone_repair", "thr_bridge_inbox_standalone_repair", "rin_standalone_repair", []string{"sevt_standalone_repair"}, 1, 1, "messages", 0)
	var repairBirth string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT payload_json::jsonb ->> 'preparation_attempt_id'
		   FROM queue_jobs
		  WHERE workspace_id = 'default'
		    AND kind = 'runtime_input'
		    AND dedupe_key = $1`,
		queue.FormatRuntimeInputDedupeKey("default", "sesn_bridge_inbox_standalone_repair", "rin_standalone_repair"),
	).Scan(&repairBirth); err != nil {
		t.Fatalf("read standalone runtime input repair birth: %v", err)
	}
	if repairBirth != "prep_bridge_inbox_standalone_birth" {
		t.Fatalf("standalone runtime input repair birth = %q; want event-recorded historical birth", repairBirth)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreRepairsTaskNotificationFromDurableTaskAndInbox(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID            = "sesn_task_notification_repair"
		threadID             = "thr_task_notification_repair"
		runtimeInputID       = "task_notification:task_notification_repair"
		taskID               = "task_notification_repair"
		sourceToolUseEventID = "sevt_task_notification_repair_tool"
		terminalResultJSON   = `{"status":"completed","stdout":{"text":"repaired task result","truncated":false},"stderr":{"text":"","truncated":false},"exit_code":0}`
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_task_notification_repair", 1, "pod_task_notification_repair")
	seedBridgeAPIBackgroundTask(t, admin, "default", sessionID, threadID, "bind_task_notification_repair", taskID, sourceToolUseEventID)
	now := time.Date(2026, 1, 1, 0, 4, 0, 0, time.UTC)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_background_tasks
		SET status='completed', terminal_result_json=$4, terminal_result_digest=$5,
		    terminal_at=$6, next_poll_at=NULL, updated_at=$6
		WHERE workspace_id=$1 AND session_id=$2 AND task_id=$3`,
		"default", sessionID, taskID, terminalResultJSON, bridgeRequestHash(terminalResultJSON), now); err != nil {
		t.Fatalf("settle durable background task: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_inbox (
		workspace_id, session_id, session_thread_id, runtime_input_id, input_kind,
		event_ids_json, status, binding_id, binding_generation, target_pod_uid,
		created_at, updated_at
	) VALUES ('default',$1,$2,$3,'task_notification','[]','accepted',
		'bind_task_notification_repair',1,'pod_task_notification_repair',$4,$4)`,
		sessionID, threadID, runtimeInputID, now); err != nil {
		t.Fatalf("seed task notification inbox: %v", err)
	}
	payload := fmt.Sprintf(
		`{"workspace_id":"default","session_id":%q,"session_thread_id":%q,"runtime_input_id":%q,"event_ids":[],"sequence_from":0,"sequence_to":0,"input_kind":"task_notification"}`,
		sessionID, threadID, runtimeInputID,
	)
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return now.Add(3 * time.Second) }
	store.TargetResolver = &recordingRuntimeTargetResolver{binding: runtimeBindingForDelivery{
		BindingID:         "bind_task_notification_repair",
		BindingGeneration: 1,
		Namespace:         "runtime-ns",
		PodName:           "runtime-pod",
		PodUID:            "pod_task_notification_repair",
		PodIP:             "10.0.0.1",
	}}
	if repaired, err := store.RepairRuntimeInbox(context.Background(), "default", 10); err != nil || repaired != 1 {
		t.Fatalf("RepairRuntimeInbox = %d/%v; want one task redelivery", repaired, err)
	}
	var (
		jobCount        int
		pendingCount    int
		repairedPayload string
		priority        int
		maxAttempts     int
	)
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*),
		        count(*) FILTER (WHERE status = 'pending'),
		        COALESCE(MAX(payload_json) FILTER (WHERE status = 'pending'), ''),
		        COALESCE(MAX(priority) FILTER (WHERE status = 'pending'), 0),
		        COALESCE(MAX(max_attempts) FILTER (WHERE status = 'pending'), 0)
		   FROM queue_jobs
		  WHERE workspace_id = 'default'
		    AND kind = 'runtime_input'
		    AND dedupe_key = $1`,
		queue.FormatRuntimeInputDedupeKey("default", sessionID, runtimeInputID),
	).Scan(&jobCount, &pendingCount, &repairedPayload, &priority, &maxAttempts); err != nil {
		t.Fatalf("read repaired task notification queue rows: %v", err)
	}
	if jobCount != 1 || pendingCount != 1 || repairedPayload != payload || priority != 0 || maxAttempts != queue.DefaultMaxAttempts {
		t.Fatalf(
			"repaired task notification rows = count %d pending %d payload %q priority %d max_attempts %d; want task-and-inbox reconstruction policy",
			jobCount,
			pendingCount,
			repairedPayload,
			priority,
			maxAttempts,
		)
	}
	if repaired, err := store.RepairRuntimeInbox(context.Background(), "default", 10); err != nil || repaired != 0 {
		t.Fatalf("RepairRuntimeInbox with pending redelivery = %d/%v; want reuse", repaired, err)
	}
	repairedLease, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID:   "default",
		Kinds:         []string{queue.KindRuntimeInput},
		LeaseOwner:    "bridge-task-repair-test",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now.Add(4 * time.Second),
	})
	if err != nil || len(repairedLease) != 1 {
		t.Fatalf("lease repaired task notification = %d/%v; want one", len(repairedLease), err)
	}
	if repaired, err := store.RepairRuntimeInbox(context.Background(), "default", 10); err != nil || repaired != 0 {
		t.Fatalf("RepairRuntimeInbox with leased redelivery = %d/%v; want reuse", repaired, err)
	}
	repairedJob, err := DecodeRuntimeJob(queueJobProto(repairedLease[0]))
	if err != nil {
		t.Fatalf("decode repaired task notification: %v", err)
	}
	plan, err := store.PrepareRuntimeCommand(context.Background(), repairedJob)
	if err != nil {
		t.Fatalf("prepare repaired task notification: %v", err)
	}
	if plan.TaskNotification == nil ||
		plan.Request.GetRuntimeInputId() != runtimeInputID ||
		plan.TaskNotification.TaskID != taskID {
		t.Fatalf("repaired task notification plan = %#v; want original runtime input and task", plan)
	}
	if err := store.MarkRuntimeInputAccepted(context.Background(), repairedJob, plan.Request); err != nil {
		t.Fatalf("mark repaired task notification accepted: %v", err)
	}
	if ok, err := queueStore.Ack(context.Background(), queue.AckRequest{
		WorkspaceID: "default",
		JobID:       repairedLease[0].ID,
		LeaseToken:  repairedLease[0].LeaseToken,
		Now:         now.Add(5 * time.Second),
	}); err != nil || !ok {
		t.Fatalf("ack repaired task notification = %v/%v; want true/nil", ok, err)
	}
	request := bridgeTaskNotificationRequestForTest(
		t,
		runtimeScopeFromCommandRequest(plan.Request),
		runtimeInputID,
		plan.TaskNotification.TaskID,
		plan.TaskNotification.ResultJSON,
	)
	apiStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	apiStore.Clock = func() time.Time { return now.Add(6 * time.Second) }
	committed, err := apiStore.CommitTaskNotificationResult(context.Background(), request)
	if err != nil {
		t.Fatalf("commit repaired task notification: %v", err)
	}
	if committed.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("repaired task notification ack = %s; want committed", committed.GetAck().GetStatus())
	}
	replayed, err := apiStore.CommitTaskNotificationResult(context.Background(), request)
	if err != nil {
		t.Fatalf("replay repaired task notification: %v", err)
	}
	if replayed.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE {
		t.Fatalf("repaired task notification replay ack = %s; want duplicate", replayed.GetAck().GetStatus())
	}
	var (
		inboxStatus       string
		terminalEventID   sql.NullString
		notificationEvent int
		notificationMsg   int
	)
	if err := admin.QueryRowContext(context.Background(),
		`SELECT i.status, t.terminal_event_id
		   FROM session_runtime_inbox i
		   JOIN session_background_tasks t
		     ON t.workspace_id = i.workspace_id
		    AND t.session_id = i.session_id
		    AND t.task_id = $3
		  WHERE i.workspace_id = $1
		    AND i.runtime_input_id = $2`,
		"default",
		runtimeInputID,
		taskID,
	).Scan(&inboxStatus, &terminalEventID); err != nil {
		t.Fatalf("read repaired task settlement: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events
		  WHERE workspace_id = 'default' AND session_id = $1 AND type = 'runtime_notification'`,
		sessionID,
	).Scan(&notificationEvent); err != nil {
		t.Fatalf("count repaired task events: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_messages
		  WHERE workspace_id = 'default' AND session_id = $1 AND kind = 'runtime_notification'`,
		sessionID,
	).Scan(&notificationMsg); err != nil {
		t.Fatalf("count repaired task messages: %v", err)
	}
	if inboxStatus != "committed" || !terminalEventID.Valid || notificationEvent != 1 || notificationMsg != 1 {
		t.Fatalf(
			"repaired task settlement inbox=%q terminal=%v events=%d messages=%d; want committed/true/1/1",
			inboxStatus,
			terminalEventID.Valid,
			notificationEvent,
			notificationMsg,
		)
	}
	if repaired, err := store.RepairRuntimeInbox(context.Background(), "default", 10); err != nil || repaired != 0 {
		t.Fatalf("RepairRuntimeInbox after semantic commit = %d/%v; want inert", repaired, err)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreCancelsSupersededAcceptedInboxWithoutLaterInput(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_inbox_standalone_superseded", "thr_bridge_inbox_standalone_superseded")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_inbox_standalone_superseded", "prep_bridge_inbox_standalone_superseded")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_inbox_standalone_superseded", "thr_bridge_inbox_standalone_superseded", "sevt_standalone_superseded_old", 1, "user.message", `{"content":[{"type":"text","text":"old"}]}`)
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_inbox_standalone_superseded", "thr_bridge_inbox_standalone_superseded", "sevt_standalone_superseded_interrupt", 2, "user.interrupt", `{}`)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_events
		    SET processed_at = '2026-01-01T00:03:01Z'
		  WHERE workspace_id = 'default'
		    AND event_id = 'sevt_standalone_superseded_interrupt'`); err != nil {
		t.Fatalf("mark interrupt processed: %v", err)
	}
	seedBridgeAPIRuntimeInbox(t, admin, "default", "sesn_bridge_inbox_standalone_superseded", "thr_bridge_inbox_standalone_superseded", "rin_standalone_superseded", "messages", `["sevt_standalone_superseded_old"]`, "accepted", "bind_bridge_standalone_superseded", "pod_uid_standalone_superseded", 1, 1)

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC) }
	repaired, err := store.RepairRuntimeInbox(context.Background(), "default", 10)
	if err != nil {
		t.Fatalf("RepairRuntimeInbox: %v", err)
	}
	if repaired != 1 {
		t.Fatalf("repaired = %d; want 1 terminal repair", repaired)
	}
	var inboxStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status
		   FROM session_runtime_inbox
		  WHERE workspace_id = 'default'
		    AND runtime_input_id = 'rin_standalone_superseded'`).Scan(&inboxStatus); err != nil {
		t.Fatalf("read standalone superseded inbox: %v", err)
	}
	if inboxStatus != "cancelled" {
		t.Fatalf("standalone superseded inbox status = %q; want cancelled", inboxStatus)
	}
	assertNoRuntimeInputRepairQueueJob(t, admin, "default", "sesn_bridge_inbox_standalone_superseded", "rin_standalone_superseded")
}

func TestPostgreSQLRuntimeDeliveryStoreIgnoresProcessedPendingInboxRepair(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_inbox_processed", "thr_bridge_inbox_processed")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_inbox_processed", "bind_bridge_inbox_processed", 1, "pod_uid_inbox_processed")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_inbox_processed", "prep_bridge_inbox_processed")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_inbox_processed", "2026-01-01T00:04:00Z")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_inbox_processed", "thr_bridge_inbox_processed", "sevt_processed_old", 1, "user.message", `{"content":[{"type":"text","text":"old"}]}`)
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_inbox_processed", "thr_bridge_inbox_processed", "sevt_processed_later", 2, "user.message", `{"content":[{"type":"text","text":"later"}]}`)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_events
		    SET processed_at = '2026-01-01T00:04:01Z'
		  WHERE workspace_id = 'default'
		    AND event_id = 'sevt_processed_old'`); err != nil {
		t.Fatalf("mark old event processed: %v", err)
	}
	seedBridgeAPIRuntimeInbox(t, admin, "default", "sesn_bridge_inbox_processed", "thr_bridge_inbox_processed", "rin_processed_old", "messages", `["sevt_processed_old"]`, "accepted", "bind_bridge_inbox_processed", "pod_uid_inbox_processed", 1, 1)

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 4, 2, 0, time.UTC) }
	plan, err := store.PrepareRuntimeCommand(context.Background(), RuntimeJob{
		JobID:                "qjob_processed_later",
		LeaseToken:           "lease_processed_later",
		Kind:                 queue.KindRuntimeInput,
		WorkspaceID:          "default",
		SessionID:            "sesn_bridge_inbox_processed",
		PreparationAttemptID: "prep_bridge_inbox_processed",
		SessionThreadID:      "thr_bridge_inbox_processed",
		RuntimeInputID:       "rin_processed_later",
		EventIDs:             []string{"sevt_processed_later"},
		SequenceFrom:         2,
		SequenceTo:           2,
		InputKind:            "messages",
		CommandKind:          agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:          `{"workspace_id":"default","session_id":"sesn_bridge_inbox_processed","session_thread_id":"thr_bridge_inbox_processed","runtime_input_id":"rin_processed_later","event_ids":["sevt_processed_later"],"sequence_from":2,"sequence_to":2,"input_kind":"messages"}`,
	})
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand later with processed old inbox: %v", err)
	}
	if plan.Request == nil || plan.Request.GetRuntimeInputId() != "rin_processed_later" {
		t.Fatalf("plan request = %#v; want later runtime input command", plan.Request)
	}
	assertNoRuntimeInputRepairQueueJob(t, admin, "default", "sesn_bridge_inbox_processed", "rin_processed_old")
}

func TestPostgreSQLRuntimeDeliveryStorePreparesRequeuedInboxInput(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_inbox_requeued", "thr_bridge_inbox_requeued")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_inbox_requeued", "bind_bridge_inbox_requeued", 3, "pod_uid_inbox_requeued")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_inbox_requeued", "prep_bridge_inbox_requeued")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_inbox_requeued", "2026-01-01T00:04:00Z")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_inbox_requeued", "thr_bridge_inbox_requeued", "sevt_requeued_old", 1, "user.message", `{"content":[{"type":"text","text":"old"}]}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", "sesn_bridge_inbox_requeued", "thr_bridge_inbox_requeued", "rin_requeued_old", "messages", `["sevt_requeued_old"]`, "accepted", "bind_bridge_inbox_requeued", "pod_uid_inbox_requeued", 1, 1)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_runtime_inbox
		    SET binding_generation = 3
		  WHERE workspace_id = 'default'
		    AND runtime_input_id = 'rin_requeued_old'`); err != nil {
		t.Fatalf("align requeued inbox binding generation: %v", err)
	}

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 4, 3, 0, time.UTC) }
	plan, err := store.PrepareRuntimeCommand(context.Background(), RuntimeJob{
		JobID:                "qjob_requeued_old",
		LeaseToken:           "lease_requeued_old",
		Kind:                 queue.KindRuntimeInput,
		WorkspaceID:          "default",
		SessionID:            "sesn_bridge_inbox_requeued",
		PreparationAttemptID: "prep_bridge_inbox_requeued",
		SessionThreadID:      "thr_bridge_inbox_requeued",
		RuntimeInputID:       "rin_requeued_old",
		EventIDs:             []string{"sevt_requeued_old"},
		SequenceFrom:         1,
		SequenceTo:           1,
		InputKind:            "messages",
		CommandKind:          agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:          `{"workspace_id":"default","session_id":"sesn_bridge_inbox_requeued","session_thread_id":"thr_bridge_inbox_requeued","runtime_input_id":"rin_requeued_old","event_ids":["sevt_requeued_old"],"sequence_from":1,"sequence_to":1,"input_kind":"messages"}`,
	})
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand requeued old input: %v", err)
	}
	if plan.Request == nil || plan.Request.GetRuntimeInputId() != "rin_requeued_old" || plan.Request.GetBindingGeneration() != 3 {
		t.Fatalf("requeued plan request = %#v; want old runtime input on existing binding", plan.Request)
	}
	var inboxStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status
		   FROM session_runtime_inbox
		  WHERE workspace_id = 'default'
		    AND runtime_input_id = 'rin_requeued_old'`).Scan(&inboxStatus); err != nil {
		t.Fatalf("read requeued inbox status: %v", err)
	}
	if inboxStatus != "accepted" {
		t.Fatalf("requeued inbox status = %q; want accepted repair state preserved", inboxStatus)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreMarksInboxAcceptedAfterRuntimeAccepts(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_inbox_accept", "thr_bridge_inbox_accept")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_inbox_accept", "bind_bridge_inbox_accept", 1, "pod_uid_inbox_accept")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_inbox_accept", "prep_bridge_inbox_accept")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_inbox_accept", "2026-01-01T00:04:00Z")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_inbox_accept", "thr_bridge_inbox_accept", "sevt_inbox_accept", 1, "user.message", `{"content":[{"type":"text","text":"hello"}]}`)

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 4, 4, 0, time.UTC) }
	sender := &recordingRuntimeCommandSender{response: &agentruntimev1.RuntimeInputCommandResponse{
		Status: agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED,
	}}
	job := RuntimeJob{
		JobID:                "qjob_inbox_accept",
		LeaseToken:           "lease_inbox_accept",
		Kind:                 queue.KindRuntimeInput,
		WorkspaceID:          "default",
		SessionID:            "sesn_bridge_inbox_accept",
		PreparationAttemptID: "prep_bridge_inbox_accept",
		SessionThreadID:      "thr_bridge_inbox_accept",
		RuntimeInputID:       "rin_inbox_accept",
		EventIDs:             []string{"sevt_inbox_accept"},
		SequenceFrom:         1,
		SequenceTo:           1,
		InputKind:            "messages",
		CommandKind:          agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:          `{"workspace_id":"default","session_id":"sesn_bridge_inbox_accept","session_thread_id":"thr_bridge_inbox_accept","runtime_input_id":"rin_inbox_accept","event_ids":["sevt_inbox_accept"],"sequence_from":1,"sequence_to":1,"input_kind":"messages"}`,
	}

	result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
	if err != nil {
		t.Fatalf("DeliverRuntimeJob: %v", err)
	}
	if result.Status != RuntimeDeliveryAccepted {
		t.Fatalf("delivery result = %#v; want accepted", result)
	}
	var inboxStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status
		   FROM session_runtime_inbox
		  WHERE workspace_id = 'default'
		    AND runtime_input_id = 'rin_inbox_accept'`).Scan(&inboxStatus); err != nil {
		t.Fatalf("read accepted inbox status: %v", err)
	}
	if inboxStatus != "accepted" {
		t.Fatalf("inbox status = %q; want accepted after Runtime ACK", inboxStatus)
	}
}

func TestPostgreSQLRuntimeDeliveryStorePersistsDistinctInboxesForChunkedBacklog(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_chunked_backlog"
		threadID  = "thr_bridge_chunked_backlog"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIPreparationReady(t, admin, "default", sessionID, "prep_bridge_chunked_backlog")
	seedBridgeAPIActiveSandbox(t, admin, "default", sessionID, "2026-01-01T00:04:00Z")
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_bridge_chunked_backlog", 1, "pod_uid_chunked_backlog")
	eventIDs := make([]string, queue.MaxRuntimeInputEventRefsPerJob+1)
	for index := range eventIDs {
		eventIDs[index] = fmt.Sprintf("sevt_bridge_chunk_%04d", index+1)
		seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventIDs[index], int64(index+1), "user.message", `{"content":[{"type":"text","text":"queued"}]}`)
	}

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 4, 4, 0, time.UTC) }
	chunks := [][]string{eventIDs[:queue.MaxRuntimeInputEventRefsPerJob], eventIDs[queue.MaxRuntimeInputEventRefsPerJob:]}
	for index, chunk := range chunks {
		runtimeInputID := fmt.Sprintf("rin_bridge_chunk_%d", index+1)
		payload, err := json.Marshal(map[string]any{
			"workspace_id": "default", "session_id": sessionID, "session_thread_id": threadID,
			"runtime_input_id": runtimeInputID, "event_ids": chunk,
			"sequence_from": index*queue.MaxRuntimeInputEventRefsPerJob + 1,
			"sequence_to":   index*queue.MaxRuntimeInputEventRefsPerJob + len(chunk),
			"input_kind":    "messages", "preparation_attempt_id": "prep_bridge_chunked_backlog",
		})
		if err != nil {
			t.Fatalf("marshal chunk %d: %v", index+1, err)
		}
		plan, err := store.PrepareRuntimeCommand(context.Background(), RuntimeJob{
			JobID: fmt.Sprintf("qjob_bridge_chunk_%d", index+1), LeaseToken: fmt.Sprintf("lease_bridge_chunk_%d", index+1),
			Kind: queue.KindRuntimeInput, WorkspaceID: "default", SessionID: sessionID,
			PreparationAttemptID: "prep_bridge_chunked_backlog", SessionThreadID: threadID,
			RuntimeInputID: runtimeInputID, EventIDs: chunk,
			SequenceFrom: int64(index*queue.MaxRuntimeInputEventRefsPerJob + 1),
			SequenceTo:   int64(index*queue.MaxRuntimeInputEventRefsPerJob + len(chunk)),
			InputKind:    "messages", CommandKind: agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
			PayloadJSON: string(payload),
		})
		if err != nil {
			t.Fatalf("PrepareRuntimeCommand chunk %d: %v", index+1, err)
		}
		if plan.Request == nil || plan.Request.GetRuntimeInputId() != runtimeInputID {
			t.Fatalf("chunk %d plan = %#v; want runtime input %s", index+1, plan, runtimeInputID)
		}
		if _, err := admin.ExecContext(context.Background(),
			`UPDATE session_runtime_inbox SET status = 'committed', updated_at = '2026-01-01T00:04:05Z'
			  WHERE workspace_id = 'default' AND runtime_input_id = $1`, runtimeInputID); err != nil {
			t.Fatalf("commit chunk %d inbox: %v", index+1, err)
		}
		if _, err := admin.ExecContext(context.Background(),
			`UPDATE session_events SET processed_at = '2026-01-01T00:04:05Z'
			  WHERE workspace_id = 'default' AND session_id = $1 AND session_thread_id = $2
			    AND sequence BETWEEN $3 AND $4`, sessionID, threadID,
			index*queue.MaxRuntimeInputEventRefsPerJob+1,
			index*queue.MaxRuntimeInputEventRefsPerJob+len(chunk)); err != nil {
			t.Fatalf("settle chunk %d events: %v", index+1, err)
		}
	}

	var inboxCount int
	var distinctInputs int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*), count(DISTINCT runtime_input_id)
		   FROM session_runtime_inbox
		  WHERE workspace_id = 'default' AND session_id = $1`, sessionID).Scan(&inboxCount, &distinctInputs); err != nil {
		t.Fatalf("count chunked inbox rows: %v", err)
	}
	if inboxCount != 2 || distinctInputs != 2 {
		t.Fatalf("chunked inbox rows/distinct inputs = %d/%d; want 2/2", inboxCount, distinctInputs)
	}
}

func TestAcceptedMessageCommandPayloadAvoidsHTMLExpansionAtTheAdmissionLimit(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_escape_fuse"
		threadID  = "thr_bridge_escape_fuse"
		eventID   = "sevt_bridge_escape_fuse"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)

	const bodyCap = 1 << 20
	prefix := `{"content":[{"type":"text","text":"`
	suffix := `"}]}`
	repeated := strings.Repeat(`&<\"`, (bodyCap-len(prefix)-len(suffix))/4)
	payloadJSON := prefix + repeated + suffix
	if len(payloadJSON) > bodyCap {
		t.Fatalf("fixture payload bytes = %d; want at most %d", len(payloadJSON), bodyCap)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 1, "user.message", payloadJSON)

	commandPayload := bridgeAcceptedMessageDeliveryPayload(
		t,
		runtime,
		"default",
		sessionID,
		threadID,
		"rin_bridge_escape_fuse",
		[]string{eventID},
		1,
		1,
	)

	if len(commandPayload) > 2*1024*1024 {
		t.Fatalf("command payload bytes = %d; want within 2 MiB payload fuse", len(commandPayload))
	}
	if len(commandPayload) >= 4*1024*1024 {
		t.Fatalf("command payload bytes = %d; want headroom within 4 MiB channel fuse", len(commandPayload))
	}
	for _, escaped := range []string{`\u0026`, `\u003c`} {
		if strings.Contains(commandPayload, escaped) {
			t.Fatalf("command payload contains HTML escape %q", escaped)
		}
	}
}

func TestPostgreSQLRuntimeDeliveryStoreMarkAcceptedFencesRuntimeInboxBinding(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_inbox_accept_fence", "thr_bridge_inbox_accept_fence")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_inbox_accept_fence", "bind_bridge_inbox_accept_fence", 1, "pod_uid_inbox_accept_fence")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_inbox_accept_fence", "prep_bridge_inbox_accept_fence")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_inbox_accept_fence", "2026-01-01T00:04:00Z")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_inbox_accept_fence", "thr_bridge_inbox_accept_fence", "sevt_inbox_accept_fence", 1, "user.message", `{"content":[{"type":"text","text":"hello"}]}`)

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 4, 4, 0, time.UTC) }
	job := RuntimeJob{
		JobID:                "qjob_inbox_accept_fence",
		LeaseToken:           "lease_inbox_accept_fence",
		Kind:                 queue.KindRuntimeInput,
		WorkspaceID:          "default",
		SessionID:            "sesn_bridge_inbox_accept_fence",
		PreparationAttemptID: "prep_bridge_inbox_accept_fence",
		SessionThreadID:      "thr_bridge_inbox_accept_fence",
		RuntimeInputID:       "rin_inbox_accept_fence",
		EventIDs:             []string{"sevt_inbox_accept_fence"},
		SequenceFrom:         1,
		SequenceTo:           1,
		InputKind:            "messages",
		CommandKind:          agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:          `{"workspace_id":"default","session_id":"sesn_bridge_inbox_accept_fence","session_thread_id":"thr_bridge_inbox_accept_fence","runtime_input_id":"rin_inbox_accept_fence","event_ids":["sevt_inbox_accept_fence"],"sequence_from":1,"sequence_to":1,"input_kind":"messages"}`,
	}
	plan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand: %v", err)
	}
	replayedPlan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand replay: %v", err)
	}
	if replayedPlan.Request.GetPayloadJson() != plan.Request.GetPayloadJson() {
		t.Fatalf("replayed message payload = %q; want byte-identical %q", replayedPlan.Request.GetPayloadJson(), plan.Request.GetPayloadJson())
	}
	var payload struct {
		Messages []struct {
			Origin string `json:"origin"`
			Role   string `json:"role"`
			Parts  []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"messages"`
	}
	if err := json.Unmarshal([]byte(plan.Request.GetPayloadJson()), &payload); err != nil {
		t.Fatalf("decode accepted message payload: %v", err)
	}
	if len(payload.Messages) != 1 || payload.Messages[0].Origin != "user" || payload.Messages[0].Role != "user" ||
		len(payload.Messages[0].Parts) != 1 || payload.Messages[0].Parts[0].Type != "text" || payload.Messages[0].Parts[0].Text != "hello" {
		t.Fatalf("accepted message payload = %#v; want canonical SDK user message", payload.Messages)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_runtime_inbox
		    SET target_pod_uid = 'pod_uid_inbox_accept_fence_other'
		  WHERE workspace_id = 'default'
		    AND runtime_input_id = 'rin_inbox_accept_fence'`); err != nil {
		t.Fatalf("mutate inbox fence: %v", err)
	}
	err = store.MarkRuntimeInputAccepted(context.Background(), job, plan.Request)
	var prepareErr runtimeDeliveryPrepareError
	if !errors.As(err, &prepareErr) || prepareErr.kind != "runtime_inbox_accept_missing" || !prepareErr.retryable {
		t.Fatalf("MarkRuntimeInputAccepted fenced err = %v; want retryable runtime_inbox_accept_missing", err)
	}
	var inboxStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status
		   FROM session_runtime_inbox
		  WHERE workspace_id = 'default'
		    AND runtime_input_id = 'rin_inbox_accept_fence'`).Scan(&inboxStatus); err != nil {
		t.Fatalf("read fenced accepted inbox status: %v", err)
	}
	if inboxStatus != "delivering" {
		t.Fatalf("fenced accepted inbox status = %q; want delivering", inboxStatus)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreRejectsRuntimeInboxPayloadConflict(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_inbox_conflict", "thr_bridge_inbox_conflict")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_inbox_conflict", "bind_bridge_inbox_conflict", 1, "pod_uid_inbox_conflict")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_inbox_conflict", "prep_bridge_inbox_conflict")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_inbox_conflict", "2026-01-01T00:04:00Z")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_inbox_conflict", "thr_bridge_inbox_conflict", "sevt_inbox_conflict_one", 1, "user.message", `{"content":[{"type":"text","text":"one"}]}`)
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_inbox_conflict", "thr_bridge_inbox_conflict", "sevt_inbox_conflict_two", 2, "user.message", `{"content":[{"type":"text","text":"two"}]}`)

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 4, 5, 0, time.UTC) }
	first := RuntimeJob{
		JobID:                "qjob_inbox_conflict_one",
		LeaseToken:           "lease_inbox_conflict_one",
		Kind:                 queue.KindRuntimeInput,
		WorkspaceID:          "default",
		SessionID:            "sesn_bridge_inbox_conflict",
		PreparationAttemptID: "prep_bridge_inbox_conflict",
		SessionThreadID:      "thr_bridge_inbox_conflict",
		RuntimeInputID:       "rin_inbox_conflict",
		EventIDs:             []string{"sevt_inbox_conflict_one"},
		SequenceFrom:         1,
		SequenceTo:           1,
		InputKind:            "messages",
		CommandKind:          agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:          `{"workspace_id":"default","session_id":"sesn_bridge_inbox_conflict","session_thread_id":"thr_bridge_inbox_conflict","runtime_input_id":"rin_inbox_conflict","event_ids":["sevt_inbox_conflict_one"],"sequence_from":1,"sequence_to":1,"input_kind":"messages"}`,
	}
	if _, err := store.PrepareRuntimeCommand(context.Background(), first); err != nil {
		t.Fatalf("PrepareRuntimeCommand first: %v", err)
	}
	second := first
	second.JobID = "qjob_inbox_conflict_two"
	second.LeaseToken = "lease_inbox_conflict_two"
	second.EventIDs = []string{"sevt_inbox_conflict_two"}
	second.SequenceFrom = 2
	second.SequenceTo = 2
	second.PayloadJSON = `{"workspace_id":"default","session_id":"sesn_bridge_inbox_conflict","session_thread_id":"thr_bridge_inbox_conflict","runtime_input_id":"rin_inbox_conflict","event_ids":["sevt_inbox_conflict_two"],"sequence_from":2,"sequence_to":2,"input_kind":"messages"}`

	_, err := store.PrepareRuntimeCommand(context.Background(), second)
	var prepareErr runtimeDeliveryPrepareError
	if !errors.As(err, &prepareErr) || prepareErr.kind != "runtime_inbox_payload_conflict" || prepareErr.retryable {
		t.Fatalf("PrepareRuntimeCommand conflicting replay err = %v; want terminal runtime_inbox_payload_conflict", err)
	}
	var eventIDsJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT event_ids_json
		   FROM session_runtime_inbox
		  WHERE workspace_id = 'default'
		    AND runtime_input_id = 'rin_inbox_conflict'`).Scan(&eventIDsJSON); err != nil {
		t.Fatalf("read conflict inbox event ids: %v", err)
	}
	if eventIDsJSON != `["sevt_inbox_conflict_one"]` {
		t.Fatalf("event_ids_json = %s; want original payload preserved", eventIDsJSON)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreBuildsControlPayloadsFromSourceEvents(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_control_delivery", "thr_bridge_control_delivery")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_control_delivery", "bind_bridge_control_delivery", 1, "pod_uid_control_delivery")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_control_delivery", "prep_bridge_control_delivery")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_control_delivery", "2026-01-01T00:03:00Z")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_control_delivery", "thr_bridge_control_delivery", "sevt_interrupt_control", 1, "user.interrupt", `{}`)
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_control_delivery", "thr_bridge_control_delivery", "sevt_confirmation_control", 2, "user.tool_confirmation", `{"tool_use_id":"sevt_tool_control","result":"deny","deny_message":"not now"}`)

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 3, 0, 0, time.UTC) }

	interrupt, err := store.PrepareRuntimeCommand(context.Background(), RuntimeJob{
		JobID:                "qjob_bridge_interrupt",
		LeaseToken:           "lease_bridge_interrupt",
		Kind:                 queue.KindRuntimeInput,
		WorkspaceID:          "default",
		SessionID:            "sesn_bridge_control_delivery",
		PreparationAttemptID: "prep_bridge_control_delivery",
		SessionThreadID:      "thr_bridge_control_delivery",
		RuntimeInputID:       "rin_bridge_interrupt",
		EventIDs:             []string{"sevt_interrupt_control"},
		SequenceFrom:         1,
		SequenceTo:           1,
		InputKind:            "interrupt_control",
		CommandKind:          agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_INTERRUPT_CONTROL,
		PayloadJSON:          `{"workspace_id":"default","session_id":"sesn_bridge_control_delivery","session_thread_id":"thr_bridge_control_delivery","runtime_input_id":"rin_bridge_interrupt","event_ids":["sevt_interrupt_control"],"sequence_from":1,"sequence_to":1,"input_kind":"interrupt_control"}`,
	})
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand interrupt: %v", err)
	}
	var interruptPayload struct {
		SourceEventID          string `json:"source_event_id"`
		InterruptFenceSequence int64  `json:"interrupt_fence_sequence"`
	}
	if err := json.Unmarshal([]byte(interrupt.Request.GetPayloadJson()), &interruptPayload); err != nil {
		t.Fatalf("decode interrupt payload: %v", err)
	}
	if interruptPayload.SourceEventID != "sevt_interrupt_control" || interruptPayload.InterruptFenceSequence != 1 || strings.Contains(interrupt.Request.GetPayloadJson(), "runtime_input_id") {
		t.Fatalf("interrupt runtime payload = %s; want runtime control payload", interrupt.Request.GetPayloadJson())
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_events
		    SET processed_at = '2026-01-01T00:03:01Z'
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_control_delivery'
		    AND event_id = 'sevt_interrupt_control'`); err != nil {
		t.Fatalf("mark interrupt event processed: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_runtime_inbox
		    SET status = 'committed',
		        committed_at = '2026-01-01T00:03:01Z'
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_control_delivery'
		    AND runtime_input_id = 'rin_bridge_interrupt'`); err != nil {
		t.Fatalf("mark interrupt inbox committed: %v", err)
	}

	confirmation, err := store.PrepareRuntimeCommand(context.Background(), RuntimeJob{
		JobID:                "qjob_bridge_confirmation",
		LeaseToken:           "lease_bridge_confirmation",
		Kind:                 queue.KindRuntimeInput,
		WorkspaceID:          "default",
		SessionID:            "sesn_bridge_control_delivery",
		PreparationAttemptID: "prep_bridge_control_delivery",
		SessionThreadID:      "thr_bridge_control_delivery",
		RuntimeInputID:       "rin_bridge_confirmation",
		EventIDs:             []string{"sevt_confirmation_control"},
		SequenceFrom:         2,
		SequenceTo:           2,
		InputKind:            "tool_confirmation",
		CommandKind:          agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_TOOL_CONFIRMATION,
		PayloadJSON:          `{"workspace_id":"default","session_id":"sesn_bridge_control_delivery","session_thread_id":"thr_bridge_control_delivery","runtime_input_id":"rin_bridge_confirmation","event_ids":["sevt_confirmation_control"],"sequence_from":2,"sequence_to":2,"input_kind":"tool_confirmation"}`,
	})
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand tool confirmation: %v", err)
	}
	var confirmationPayload struct {
		SourceEventID  string `json:"source_event_id"`
		ToolUseEventID string `json:"tool_use_event_id"`
		Decision       string `json:"decision"`
		DenyMessage    string `json:"deny_message"`
	}
	if err := json.Unmarshal([]byte(confirmation.Request.GetPayloadJson()), &confirmationPayload); err != nil {
		t.Fatalf("decode confirmation payload: %v", err)
	}
	if confirmationPayload.SourceEventID != "sevt_confirmation_control" ||
		confirmationPayload.ToolUseEventID != "sevt_tool_control" ||
		confirmationPayload.Decision != "deny" ||
		confirmationPayload.DenyMessage != "not now" ||
		strings.Contains(confirmation.Request.GetPayloadJson(), "runtime_input_id") ||
		strings.Contains(confirmation.Request.GetPayloadJson(), `"result"`) ||
		strings.Contains(confirmation.Request.GetPayloadJson(), `"tool_use_id"`) {
		t.Fatalf("tool confirmation runtime payload = %s; want runtime command payload", confirmation.Request.GetPayloadJson())
	}
}

func TestPostgreSQLRuntimeDeliveryStoreAppliesProcessedInterruptFence(t *testing.T) {
	t.Run("all superseded messages stale ack without inbox", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		seedBridgeAPISession(t, admin, "default", "sesn_bridge_interrupt_fence", "thr_bridge_interrupt_fence")
		seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_interrupt_fence", "prep_bridge_interrupt_fence")
		seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_interrupt_fence", "2026-01-01T00:03:00Z")
		seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_interrupt_fence", "thr_bridge_interrupt_fence", "sevt_old_message_1", 1, "user.message", `{"content":[{"type":"text","text":"old 1"}]}`)
		seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_interrupt_fence", "thr_bridge_interrupt_fence", "sevt_old_message_2", 2, "user.message", `{"content":[{"type":"text","text":"old 2"}]}`)
		seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_interrupt_fence", "thr_bridge_interrupt_fence", "sevt_processed_interrupt", 3, "user.interrupt", `{}`)
		if _, err := admin.ExecContext(context.Background(),
			`UPDATE session_events
			    SET processed_at = '2026-01-01T00:03:01Z'
			  WHERE workspace_id = 'default'
			    AND session_id = 'sesn_bridge_interrupt_fence'
			    AND event_id = 'sevt_processed_interrupt'`); err != nil {
			t.Fatalf("mark interrupt processed: %v", err)
		}
		store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
		store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 3, 2, 0, time.UTC) }

		plan, err := store.PrepareRuntimeCommand(context.Background(), RuntimeJob{
			JobID:                "qjob_superseded_messages",
			LeaseToken:           "lease_superseded_messages",
			Kind:                 queue.KindRuntimeInput,
			WorkspaceID:          "default",
			SessionID:            "sesn_bridge_interrupt_fence",
			PreparationAttemptID: "prep_bridge_interrupt_fence",
			SessionThreadID:      "thr_bridge_interrupt_fence",
			RuntimeInputID:       "rin_superseded_messages",
			EventIDs:             []string{"sevt_old_message_1", "sevt_old_message_2"},
			SequenceFrom:         1,
			SequenceTo:           2,
			InputKind:            "messages",
			CommandKind:          agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
			PayloadJSON:          `{"workspace_id":"default","session_id":"sesn_bridge_interrupt_fence","session_thread_id":"thr_bridge_interrupt_fence","runtime_input_id":"rin_superseded_messages","event_ids":["sevt_old_message_1","sevt_old_message_2"],"sequence_from":1,"sequence_to":2,"input_kind":"messages"}`,
		})
		if err != nil {
			t.Fatalf("PrepareRuntimeCommand superseded messages: %v", err)
		}
		if !plan.StaleAccepted || plan.Request != nil {
			t.Fatalf("superseded plan = %+v; want stale accepted without Runtime request", plan)
		}
		var inboxRows int
		if err := admin.QueryRowContext(context.Background(),
			`SELECT count(*)
			   FROM session_runtime_inbox
			  WHERE workspace_id = 'default'
			    AND runtime_input_id = 'rin_superseded_messages'`).Scan(&inboxRows); err != nil {
			t.Fatalf("read superseded inbox rows: %v", err)
		}
		if inboxRows != 0 {
			t.Fatalf("superseded inbox rows = %d; want 0", inboxRows)
		}
	})

	t.Run("mixed superseded and deliverable messages are fatal", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		seedBridgeAPISession(t, admin, "default", "sesn_bridge_interrupt_mixed", "thr_bridge_interrupt_mixed")
		seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_interrupt_mixed", "prep_bridge_interrupt_mixed")
		seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_interrupt_mixed", "2026-01-01T00:03:00Z")
		seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_interrupt_mixed", "thr_bridge_interrupt_mixed", "sevt_old_message", 1, "user.message", `{"content":[{"type":"text","text":"old"}]}`)
		seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_interrupt_mixed", "thr_bridge_interrupt_mixed", "sevt_processed_interrupt", 3, "user.interrupt", `{}`)
		seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_interrupt_mixed", "thr_bridge_interrupt_mixed", "sevt_new_message", 4, "user.message", `{"content":[{"type":"text","text":"new"}]}`)
		if _, err := admin.ExecContext(context.Background(),
			`UPDATE session_events
			    SET processed_at = '2026-01-01T00:03:01Z'
			  WHERE workspace_id = 'default'
			    AND session_id = 'sesn_bridge_interrupt_mixed'
			    AND event_id = 'sevt_processed_interrupt'`); err != nil {
			t.Fatalf("mark mixed interrupt processed: %v", err)
		}
		store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
		store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 3, 2, 0, time.UTC) }

		_, err := store.PrepareRuntimeCommand(context.Background(), RuntimeJob{
			JobID:                "qjob_mixed_messages",
			LeaseToken:           "lease_mixed_messages",
			Kind:                 queue.KindRuntimeInput,
			WorkspaceID:          "default",
			SessionID:            "sesn_bridge_interrupt_mixed",
			PreparationAttemptID: "prep_bridge_interrupt_mixed",
			SessionThreadID:      "thr_bridge_interrupt_mixed",
			RuntimeInputID:       "rin_mixed_messages",
			EventIDs:             []string{"sevt_old_message", "sevt_new_message"},
			SequenceFrom:         1,
			SequenceTo:           4,
			InputKind:            "messages",
			CommandKind:          agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
			PayloadJSON:          `{"workspace_id":"default","session_id":"sesn_bridge_interrupt_mixed","session_thread_id":"thr_bridge_interrupt_mixed","runtime_input_id":"rin_mixed_messages","event_ids":["sevt_old_message","sevt_new_message"],"sequence_from":1,"sequence_to":4,"input_kind":"messages"}`,
		})
		var prepareErr runtimeDeliveryPrepareError
		if !errors.As(err, &prepareErr) || prepareErr.kind != "runtime_input_superseded_by_interrupt" || prepareErr.retryable {
			t.Fatalf("mixed messages err = %v; want nonretryable runtime_input_superseded_by_interrupt", err)
		}
	})

	t.Run("post fence messages remain deliverable", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		seedBridgeAPISession(t, admin, "default", "sesn_bridge_interrupt_post_fence", "thr_bridge_interrupt_post_fence")
		seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_interrupt_post_fence", "bind_bridge_interrupt_post_fence", 1, "pod_uid_interrupt_post_fence")
		seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_interrupt_post_fence", "prep_bridge_interrupt_post_fence")
		seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_interrupt_post_fence", "2026-01-01T00:03:00Z")
		seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_interrupt_post_fence", "thr_bridge_interrupt_post_fence", "sevt_processed_interrupt", 3, "user.interrupt", `{}`)
		seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_interrupt_post_fence", "thr_bridge_interrupt_post_fence", "sevt_new_message", 4, "user.message", `{"content":[{"type":"text","text":"new"}]}`)
		if _, err := admin.ExecContext(context.Background(),
			`UPDATE session_events
			    SET processed_at = '2026-01-01T00:03:01Z'
			  WHERE workspace_id = 'default'
			    AND session_id = 'sesn_bridge_interrupt_post_fence'
			    AND event_id = 'sevt_processed_interrupt'`); err != nil {
			t.Fatalf("mark post-fence interrupt processed: %v", err)
		}
		store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
		store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 3, 2, 0, time.UTC) }

		plan, err := store.PrepareRuntimeCommand(context.Background(), RuntimeJob{
			JobID:                "qjob_post_fence_message",
			LeaseToken:           "lease_post_fence_message",
			Kind:                 queue.KindRuntimeInput,
			WorkspaceID:          "default",
			SessionID:            "sesn_bridge_interrupt_post_fence",
			PreparationAttemptID: "prep_bridge_interrupt_post_fence",
			SessionThreadID:      "thr_bridge_interrupt_post_fence",
			RuntimeInputID:       "rin_post_fence_message",
			EventIDs:             []string{"sevt_new_message"},
			SequenceFrom:         4,
			SequenceTo:           4,
			InputKind:            "messages",
			CommandKind:          agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
			PayloadJSON:          `{"workspace_id":"default","session_id":"sesn_bridge_interrupt_post_fence","session_thread_id":"thr_bridge_interrupt_post_fence","runtime_input_id":"rin_post_fence_message","event_ids":["sevt_new_message"],"sequence_from":4,"sequence_to":4,"input_kind":"messages"}`,
		})
		if err != nil {
			t.Fatalf("PrepareRuntimeCommand post-fence messages: %v", err)
		}
		if plan.StaleAccepted || plan.Request == nil {
			t.Fatalf("post-fence plan = %+v; want Runtime request", plan)
		}
		var inboxStatus string
		var sequenceFrom int64
		if err := admin.QueryRowContext(context.Background(),
			`SELECT status, sequence_from
			   FROM session_runtime_inbox
			  WHERE workspace_id = 'default'
			    AND runtime_input_id = 'rin_post_fence_message'`).Scan(&inboxStatus, &sequenceFrom); err != nil {
			t.Fatalf("read post-fence inbox: %v", err)
		}
		if inboxStatus != "delivering" || sequenceFrom != 4 {
			t.Fatalf("post-fence inbox status=%q sequence_from=%d; want delivering/4", inboxStatus, sequenceFrom)
		}
	})
}

func TestPostgreSQLRuntimeDeliveryStoreClaimsBindingFromKubernetesVisibility(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_resolve", "thr_bridge_resolve")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_resolve", "prep_bridge_resolve")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_resolve", "2026-01-01T00:00:00Z")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_resolve", "thr_bridge_resolve", "evt_bridge_resolve", 1, "user.message", `{"content":[{"type":"text","text":"hello"}]}`)

	candidate := enginekubernetes.BindingCandidate{
		Namespace: "tetral-agent-runtime",
		PodName:   "runtime-pod-a",
		PodUID:    "pod-uid-a",
		PodIP:     "10.0.0.25",
	}
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	store.TargetResolver = KubernetesRuntimeTargetResolver{
		Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
			return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{candidate})
		},
	}
	plan, err := store.PrepareRuntimeCommand(context.Background(), RuntimeJob{
		JobID:                "qjob_bridge_resolve",
		LeaseToken:           "lease_bridge_resolve",
		Kind:                 queue.KindRuntimeInput,
		WorkspaceID:          "default",
		SessionID:            "sesn_bridge_resolve",
		PreparationAttemptID: "prep_bridge_resolve",
		SessionThreadID:      "thr_bridge_resolve",
		RuntimeInputID:       "rin_bridge_resolve",
		EventIDs:             []string{"evt_bridge_resolve"},
		SequenceFrom:         1,
		SequenceTo:           1,
		InputKind:            "messages",
		CommandKind:          agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:          `{"type":"messages"}`,
	})
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand: %v", err)
	}
	if plan.Target.PodName != candidate.PodName || plan.Target.PodUID != candidate.PodUID || plan.Target.PodIP != candidate.PodIP {
		t.Fatalf("target = %+v; want candidate %+v", plan.Target, candidate)
	}
	if plan.Request.GetBindingId() == "" || plan.Request.GetBindingGeneration() == 0 || plan.Request.GetTargetPodName() != candidate.PodName {
		t.Fatalf("request binding envelope = %+v; want claimed binding and target identity", plan.Request)
	}

	var podName string
	var podUID string
	var podIP string
	var generation int64
	if err := admin.QueryRowContext(context.Background(),
		`SELECT agent_runtime_pod_name, agent_runtime_pod_uid, agent_runtime_pod_ip, binding_generation
		   FROM session_runtime_bindings
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_resolve'`).Scan(&podName, &podUID, &podIP, &generation); err != nil {
		t.Fatalf("read claimed binding: %v", err)
	}
	if podName != candidate.PodName || podUID != candidate.PodUID || podIP != candidate.PodIP || generation != plan.Request.GetBindingGeneration() {
		t.Fatalf("claimed binding = %s/%s/%s gen %d; want %s/%s/%s gen %d", podName, podUID, podIP, generation, candidate.PodName, candidate.PodUID, candidate.PodIP, plan.Request.GetBindingGeneration())
	}
}

func TestTPREP6PostgreSQLRuntimeDeliveryStoreUsesGenericReasonTextWhenResourceIdentityIsNull(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_delivery_failed", "thr_bridge_delivery_failed")
	seedBridgeAPIPreparationFailed(t, admin, "default", "sesn_bridge_delivery_failed", "prep_bridge_delivery_failed", "github_credential_required")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_delivery_failed", "bind_bridge_delivery_failed", 1, "pod_uid_delivery_failed")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_delivery_failed", "thr_bridge_delivery_failed", "evt_bridge_delivery_failed", 1, "user.message", `{"type":"user.message","content":[{"type":"text","text":"clone private repo"}]}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", "sesn_bridge_delivery_failed", "thr_bridge_delivery_failed", "rin_bridge_delivery_failed", "messages", `["evt_bridge_delivery_failed"]`, "accepted", "bind_bridge_delivery_failed", "pod_uid_delivery_failed", 1, 1)
	initialStreamPosition := seedBridgeAPIStreamChange(t, admin, "default", "sesn_bridge_delivery_failed", "thr_bridge_delivery_failed", "evt_bridge_delivery_failed", 1, "public", true)

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 4, 0, 0, time.UTC) }
	job := RuntimeJob{
		JobID:                "qjob_bridge_delivery_failed",
		LeaseToken:           "lease_bridge_delivery_failed",
		Kind:                 queue.KindRuntimeInput,
		WorkspaceID:          "default",
		SessionID:            "sesn_bridge_delivery_failed",
		PreparationAttemptID: "prep_bridge_delivery_failed",
		SessionThreadID:      "thr_bridge_delivery_failed",
		RuntimeInputID:       "rin_bridge_delivery_failed",
		EventIDs:             []string{"evt_bridge_delivery_failed"},
		SequenceFrom:         1,
		SequenceTo:           1,
		InputKind:            "messages",
		CommandKind:          agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:          `{"type":"messages"}`,
	}

	result, err := (RuntimePodDirectDeliverer{Store: store}).DeliverRuntimeJob(context.Background(), job)
	if err != nil {
		t.Fatalf("DeliverRuntimeJob terminal preparation failure: %v", err)
	}
	if result.Status != RuntimeDeliveryAccepted {
		t.Fatalf("terminal preparation delivery result = %#v; want accepted no-command settlement", result)
	}

	var processedAt sql.NullString
	var eventRevision int64
	var latestStreamPosition int64
	if err := admin.QueryRowContext(context.Background(),
		`SELECT processed_at, revision, latest_stream_position
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND event_id = 'evt_bridge_delivery_failed'`).Scan(&processedAt, &eventRevision, &latestStreamPosition); err != nil {
		t.Fatalf("read settled input event: %v", err)
	}
	var streamChangeCount int
	var maxStreamRevision int64
	var maxStreamPosition int64
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*), COALESCE(MAX(revision), 0), COALESCE(MAX(stream_position), 0)
		   FROM session_event_stream_changes
		  WHERE workspace_id = 'default'
		    AND event_id = 'evt_bridge_delivery_failed'`).Scan(&streamChangeCount, &maxStreamRevision, &maxStreamPosition); err != nil {
		t.Fatalf("read settled input stream changes: %v", err)
	}
	if !processedAt.Valid || eventRevision != 2 || streamChangeCount != 2 || maxStreamRevision != 2 || latestStreamPosition != maxStreamPosition || latestStreamPosition <= initialStreamPosition {
		t.Fatalf("input settlement processed=%v eventRev=%d changes=%d maxRev=%d latest=%d initial=%d maxPos=%d; want processed revision stream update",
			processedAt.Valid, eventRevision, streamChangeCount, maxStreamRevision, latestStreamPosition, initialStreamPosition, maxStreamPosition)
	}

	var errorEventID string
	var errorPayload string
	var errorVisibility string
	var errorSessionVisible bool
	var errorProcessedAt sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT event_id, payload_json, visibility, session_visible, processed_at
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_delivery_failed'
		    AND type = 'session.error'`).Scan(&errorEventID, &errorPayload, &errorVisibility, &errorSessionVisible, &errorProcessedAt); err != nil {
		t.Fatalf("read terminal preparation error event: %v", err)
	}
	errorMessage := testJSONPathString(t, errorPayload, "error.message")
	const wantGenericMessage = "A GitHub repository could not be authenticated. Rotate that resource's authorization token, then send a new input to retry preparation."
	if errorVisibility != "public" || !errorSessionVisible || !errorProcessedAt.Valid ||
		testJSONPathString(t, errorPayload, "error.type") != "unknown_error" ||
		testJSONPathString(t, errorPayload, "error.retry_status.type") != "exhausted" ||
		errorMessage != wantGenericMessage ||
		strings.Contains(errorMessage, "github_credential_required") ||
		strings.Contains(strings.ToLower(errorMessage), "vault") {
		t.Fatalf("session.error payload/projection = visibility %s visible %v processed %v payload %s; want actionable token-rotation error without internal reason",
			errorVisibility, errorSessionVisible, errorProcessedAt.Valid, errorPayload)
	}
	var errorStreamChanges int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_event_stream_changes
		  WHERE workspace_id = 'default'
		    AND event_id = $1`,
		errorEventID,
	).Scan(&errorStreamChanges); err != nil {
		t.Fatalf("read error stream changes: %v", err)
	}
	var assistantProjectionCount int
	var assistantProjection string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*), COALESCE(MAX(data_json), '')
		   FROM session_messages
		  WHERE workspace_id = 'default'
		    AND source_event_id = $1
		    AND kind = 'assistant'`,
		errorEventID,
	).Scan(&assistantProjectionCount, &assistantProjection); err != nil {
		t.Fatalf("read error message projection: %v", err)
	}
	if errorStreamChanges != 1 || assistantProjectionCount != 1 ||
		!strings.Contains(assistantProjection, "Rotate that resource's authorization token") ||
		strings.Contains(assistantProjection, "github_credential_required") {
		t.Fatalf("error stream/projection = changes %d messages %d projection %s; want caller and model-visible error",
			errorStreamChanges, assistantProjectionCount, assistantProjection)
	}
	var inboxStatus string
	var inboxCommittedAt sql.NullString
	var prepareRows int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status, committed_at
		   FROM session_runtime_inbox
		  WHERE workspace_id = 'default'
		    AND runtime_input_id = 'rin_bridge_delivery_failed'`).Scan(&inboxStatus, &inboxCommittedAt); err != nil {
		t.Fatalf("read runtime inbox settlement: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM queue_jobs WHERE kind = 'session_prepare' AND partition_key = 'session:default:sesn_bridge_delivery_failed'`).Scan(&prepareRows); err != nil {
		t.Fatalf("read session_prepare jobs: %v", err)
	}
	if inboxStatus != "committed" || !inboxCommittedAt.Valid || prepareRows != 0 {
		t.Fatalf("terminal settlement inbox=%q committed=%v prepareJobs=%d; want pre-existing inbox committed and no requeue", inboxStatus, inboxCommittedAt.Valid, prepareRows)
	}

	replay, err := (RuntimePodDirectDeliverer{Store: store}).DeliverRuntimeJob(context.Background(), job)
	if err != nil {
		t.Fatalf("DeliverRuntimeJob terminal preparation replay: %v", err)
	}
	if replay.Status != RuntimeDeliveryDuplicate {
		t.Fatalf("terminal preparation replay result = %#v; want duplicate stale ack", replay)
	}
	var errorCount int
	var replayRevision int64
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_delivery_failed' AND type = 'session.error'`).Scan(&errorCount); err != nil {
		t.Fatalf("read replay error count: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT revision FROM session_events WHERE workspace_id = 'default' AND event_id = 'evt_bridge_delivery_failed'`).Scan(&replayRevision); err != nil {
		t.Fatalf("read replay input revision: %v", err)
	}
	if errorCount != 1 || replayRevision != 2 {
		t.Fatalf("replay wrote errors=%d input revision=%d; want idempotent stale ack", errorCount, replayRevision)
	}
}

func TestTPREP6PostgreSQLRuntimeDeliveryStorePrefixesPersistedFailingRepositoryIdentity(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_bridge_fenced_prepare_failure"
	const threadID = "thr_bridge_fenced_prepare_failure"
	const failedAttemptID = "prep_bridge_fenced_failed"
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIPreparationFailed(t, admin, "default", sessionID, failedAttemptID, "github_repository_unavailable")
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_preparations
		    SET failure_resource_id = 'sesrsc_missing',
		        failure_resource_url = 'https://github.com/tetral-ai/missing'
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND preparation_attempt_id = $2`,
		sessionID, failedAttemptID,
	); err != nil {
		t.Fatalf("seed failed preparation resource identity: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_preparations (
			workspace_id, session_id, preparation_attempt_id, environment_id, environment_generation,
			sandbox_id, status, created_at, updated_at
		 ) SELECT workspace_id, session_id, 'prep_bridge_fenced_new', environment_id, environment_generation,
		          sandbox_id, 'pending', '2026-01-01T00:01:00Z', '2026-01-01T00:01:00Z'
		     FROM session_preparations
		    WHERE workspace_id = 'default' AND session_id = $1 AND preparation_attempt_id = $2`,
		sessionID, failedAttemptID,
	); err != nil {
		t.Fatalf("seed superseding preparation: %v", err)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "evt_bridge_fenced_failure", 1, "user.message", `{"type":"user.message","content":[{"type":"text","text":"continue"}]}`)
	seedBridgeAPIStreamChange(t, admin, "default", sessionID, threadID, "evt_bridge_fenced_failure", 1, "public", true)

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC) }
	job := RuntimeJob{
		JobID: "qjob_bridge_fenced_failure", LeaseToken: "lease_bridge_fenced_failure",
		Kind: queue.KindRuntimeInput, WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: "rin_bridge_fenced_failure", EventIDs: []string{"evt_bridge_fenced_failure"},
		SequenceFrom: 1, SequenceTo: 1, InputKind: "messages",
		CommandKind:                agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PreparationAttemptID:       failedAttemptID,
		SealedPreparationAttemptID: failedAttemptID,
		PayloadJSON:                `{"type":"messages"}`,
	}
	result, err := (RuntimePodDirectDeliverer{Store: store}).DeliverRuntimeJob(context.Background(), job)
	if err != nil {
		t.Fatalf("DeliverRuntimeJob fenced failure: %v", err)
	}
	if result.Status != RuntimeDeliveryAccepted {
		t.Fatalf("fenced failure result = %#v; want accepted settlement", result)
	}
	var message string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT payload_json
		   FROM session_events
		  WHERE workspace_id = 'default' AND session_id = $1 AND type = 'session.error'`,
		sessionID,
	).Scan(&message); err != nil {
		t.Fatalf("read fenced failure error: %v", err)
	}
	errorMessage := testJSONPathString(t, message, "error.message")
	const wantMessage = "GitHub repository https://github.com/tetral-ai/missing (resource sesrsc_missing) may have the wrong URL or have been deleted. Repository URLs are fixed for the session lifetime, so create a new session to correct it; if this token lacks access, rotate that resource's authorization token, then send a new input."
	if errorMessage != wantMessage {
		t.Fatalf("fenced failure message = %q; want %q", errorMessage, wantMessage)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreFencedPreparationFailureSerializesWithInputCommit(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_fenced_commit_race"
		threadID  = "thr_bridge_fenced_commit_race"
		attemptID = "prep_bridge_fenced_commit_race"
		eventID   = "evt_bridge_fenced_commit_race"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIPreparationFailed(t, admin, "default", sessionID, attemptID, "github_credential_required")
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 1, "user.message", `{"type":"user.message","content":[{"type":"text","text":"continue"}]}`)
	seedBridgeAPIStreamChange(t, admin, "default", sessionID, threadID, eventID, 1, "public", true)

	inputCommit, err := admin.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin input commit: %v", err)
	}
	defer func() { _ = inputCommit.Rollback() }()
	var lockedEventID string
	if err := inputCommit.QueryRowContext(context.Background(),
		`SELECT event_id
		   FROM session_events
		  WHERE workspace_id = 'default' AND session_id = $1 AND event_id = $2
		  FOR UPDATE`,
		sessionID, eventID,
	).Scan(&lockedEventID); err != nil {
		t.Fatalf("lock input event: %v", err)
	}
	if lockedEventID != eventID {
		t.Fatalf("locked event id = %q; want %q", lockedEventID, eventID)
	}

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC) }
	job := RuntimeJob{
		JobID: "qjob_bridge_fenced_commit_race", LeaseToken: "lease_bridge_fenced_commit_race",
		Kind: queue.KindRuntimeInput, WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: "rin_bridge_fenced_commit_race", EventIDs: []string{eventID},
		SequenceFrom: 1, SequenceTo: 1, InputKind: "messages",
		CommandKind:                agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PreparationAttemptID:       attemptID,
		SealedPreparationAttemptID: attemptID,
	}
	type deliveryOutcome struct {
		result RuntimeDeliveryResult
		err    error
	}
	delivered := make(chan deliveryOutcome, 1)
	go func() {
		result, err := (RuntimePodDirectDeliverer{Store: store}).DeliverRuntimeJob(context.Background(), job)
		delivered <- deliveryOutcome{result: result, err: err}
	}()
	select {
	case outcome := <-delivered:
		t.Fatalf("fenced settlement escaped the input event lock: result=%#v err=%v", outcome.result, outcome.err)
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := inputCommit.ExecContext(context.Background(),
		`UPDATE session_events
		    SET processed_at = '2026-01-01T00:01:30Z',
		        updated_at = '2026-01-01T00:01:30Z'
		  WHERE workspace_id = 'default' AND session_id = $1 AND event_id = $2`,
		sessionID, eventID,
	); err != nil {
		t.Fatalf("commit input event state: %v", err)
	}
	if err := inputCommit.Commit(); err != nil {
		t.Fatalf("commit input transaction: %v", err)
	}
	outcome := <-delivered
	if outcome.err != nil || outcome.result.Status != RuntimeDeliveryDuplicate {
		t.Fatalf("fenced stale settlement result=%#v err=%v; want duplicate stale ACK", outcome.result, outcome.err)
	}
	var errorCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_events
		  WHERE workspace_id = 'default' AND session_id = $1 AND type = 'session.error'`,
		sessionID,
	).Scan(&errorCount); err != nil {
		t.Fatalf("count session errors: %v", err)
	}
	if errorCount != 0 {
		t.Fatalf("session.error count = %d; want no failure projection after input won the row lock", errorCount)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreFinalAttemptRechecksPreparationSealBeforeDeadLetter(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_bridge_fenced_finalize"
	const threadID = "thr_bridge_fenced_finalize"
	const attemptID = "prep_bridge_fenced_finalize"
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIPreparationReady(t, admin, "default", sessionID, attemptID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "evt_bridge_fenced_finalize", 1, "user.message", `{"type":"user.message","content":[{"type":"text","text":"continue"}]}`)
	seedBridgeAPIStreamChange(t, admin, "default", sessionID, threadID, "evt_bridge_fenced_finalize", 1, "public", true)

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 3, 0, 0, time.UTC) }
	job := RuntimeJob{
		JobID: "qjob_bridge_fenced_finalize", LeaseToken: "lease_bridge_fenced_finalize",
		Kind: queue.KindRuntimeInput, WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: "rin_bridge_fenced_finalize", EventIDs: []string{"evt_bridge_fenced_finalize"},
		SequenceFrom: 1, SequenceTo: 1, InputKind: "messages",
		CommandKind:          agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PreparationAttemptID: attemptID, AttemptCount: 3, MaxAttempts: 3,
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_preparations
		    SET status = 'failed',
		        failure_stage = 'github_repository_clone',
		        last_error_kind = 'credential_required',
		        failure_reason = 'github_credential_required',
		        retryable = FALSE,
		        failed_at = '2026-01-01T00:02:30Z',
		        updated_at = '2026-01-01T00:02:30Z'
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND preparation_attempt_id = $2`,
		sessionID,
		attemptID,
	); err != nil {
		t.Fatalf("commit preparation failure after runner seal observation: %v", err)
	}
	result, err := store.FinalizeRuntimeDelivery(context.Background(), job, RuntimeDeliveryResult{
		Status: RuntimeDeliveryRejected, Retryable: true, ErrorKind: "runtime_transport_unavailable",
	})
	if err != nil {
		t.Fatalf("FinalizeRuntimeDelivery fenced failure: %v", err)
	}
	if result.Status != RuntimeDeliveryRejected || result.Retryable {
		t.Fatalf("finalized result = %#v; want terminal dead-letter disposition", result)
	}
	var processed bool
	var errorCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT processed_at IS NOT NULL
		   FROM session_events
		  WHERE workspace_id = 'default' AND event_id = 'evt_bridge_fenced_finalize'`,
	).Scan(&processed); err != nil {
		t.Fatalf("read finalized input: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_events
		  WHERE workspace_id = 'default' AND session_id = $1 AND type = 'session.error'`,
		sessionID,
	).Scan(&errorCount); err != nil {
		t.Fatalf("count finalizer errors: %v", err)
	}
	if !processed || errorCount != 1 {
		t.Fatalf("finalizer settlement processed=%v errors=%d; want durable settlement before dead letter", processed, errorCount)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreFinalAttemptSerializesWithConcurrentPreparationFailure(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_bridge_concurrent_seal_finalize"
	const threadID = "thr_bridge_concurrent_seal_finalize"
	const birthAttemptID = "prep_bridge_concurrent_seal_birth"
	const failingAttemptID = "prep_bridge_concurrent_seal_failure"
	const eventID = "evt_bridge_concurrent_seal_finalize"
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIPreparationReady(t, admin, "default", sessionID, birthAttemptID)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_preparations
		    SET superseded_at = '2026-01-01T00:01:00Z',
		        updated_at = '2026-01-01T00:01:00Z'
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND preparation_attempt_id = $2`,
		sessionID,
		birthAttemptID,
	); err != nil {
		t.Fatalf("supersede birth attempt: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_preparations (
			workspace_id, session_id, preparation_attempt_id, environment_id, environment_generation,
			sandbox_id, status, created_at, updated_at
		) VALUES ('default', $1, $2, $3, 1, $4, 'preparing',
		          '2026-01-01T00:01:00Z', '2026-01-01T00:01:00Z')`,
		sessionID,
		failingAttemptID,
		"env_"+sessionID,
		"sandbox_"+sessionID,
	); err != nil {
		t.Fatalf("seed later preparing attempt: %v", err)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 1, "user.message", `{"type":"user.message","content":[{"type":"text","text":"continue"}]}`)
	seedBridgeAPIStreamChange(t, admin, "default", sessionID, threadID, eventID, 1, "public", true)

	failureTx, err := admin.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin preparation failure transaction: %v", err)
	}
	defer func() { _ = failureTx.Rollback() }()
	if _, err := failureTx.ExecContext(context.Background(),
		`UPDATE session_preparations
		    SET status = 'failed',
		        failure_stage = 'github_repository_clone',
		        last_error_kind = 'credential_required',
		        failure_reason = 'github_credential_required',
		        retryable = FALSE,
		        failed_at = '2026-01-01T00:01:30Z',
		        updated_at = '2026-01-01T00:01:30Z'
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND preparation_attempt_id = $2`,
		sessionID,
		failingAttemptID,
	); err != nil {
		t.Fatalf("stage preparation failure: %v", err)
	}
	var failureBackendPID int
	if err := failureTx.QueryRowContext(context.Background(), `SELECT pg_backend_pid()`).Scan(&failureBackendPID); err != nil {
		t.Fatalf("read preparation failure backend: %v", err)
	}

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC) }
	job := RuntimeJob{
		JobID: "qjob_bridge_concurrent_seal_finalize", LeaseToken: "lease_bridge_concurrent_seal_finalize",
		Kind: queue.KindRuntimeInput, WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: "rin_bridge_concurrent_seal_finalize", EventIDs: []string{eventID},
		SequenceFrom: 1, SequenceTo: 1, InputKind: "messages",
		CommandKind:          agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PreparationAttemptID: birthAttemptID, AttemptCount: 3, MaxAttempts: 3,
	}
	type finalizationOutcome struct {
		result RuntimeDeliveryResult
		err    error
	}
	finalized := make(chan finalizationOutcome, 1)
	go func() {
		result, err := store.FinalizeRuntimeDelivery(context.Background(), job, RuntimeDeliveryResult{
			Status: RuntimeDeliveryRejected, Retryable: true, ErrorKind: "runtime_transport_unavailable",
		})
		finalized <- finalizationOutcome{result: result, err: err}
	}()
	waitForPostgreSQLLockWaiters(t, admin, failureBackendPID, 1)
	if err := failureTx.Commit(); err != nil {
		t.Fatalf("commit concurrent preparation failure: %v", err)
	}

	outcome := <-finalized
	if outcome.err != nil {
		t.Fatalf("FinalizeRuntimeDelivery concurrent preparation failure: %v", outcome.err)
	}
	if outcome.result.Status != RuntimeDeliveryRejected || outcome.result.Retryable {
		t.Fatalf("finalized result = %#v; want terminal preparation settlement", outcome.result)
	}
	var processed bool
	var errorCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT processed_at IS NOT NULL
		   FROM session_events
		  WHERE workspace_id = 'default' AND event_id = $1`,
		eventID,
	).Scan(&processed); err != nil {
		t.Fatalf("read concurrently settled input: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_events
		  WHERE workspace_id = 'default' AND session_id = $1 AND type = 'session.error'`,
		sessionID,
	).Scan(&errorCount); err != nil {
		t.Fatalf("count concurrent settlement errors: %v", err)
	}
	if !processed || errorCount != 1 {
		t.Fatalf("concurrent settlement processed=%v errors=%d; want failed-attempt settlement", processed, errorCount)
	}
}

func TestTerminalPreparationFailureMessageHidesInternalFailureDetails(t *testing.T) {
	readiness := sessionPreparationReadiness{
		FailureReason: sql.NullString{String: "session_prepare_error", Valid: true},
		FailureStage:  sql.NullString{String: "provider_create", Valid: true},
	}
	message := terminalPreparationFailureMessage(readiness)
	if strings.Contains(message, readiness.FailureReason.String) ||
		strings.Contains(message, readiness.FailureStage.String) {
		t.Fatalf("caller-visible message exposed internal preparation details: %q", message)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreConvertsOversizedInputToBoundedLoopRejection(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_runtime_rejected", "thr_bridge_runtime_rejected")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_runtime_rejected", "prep_bridge_runtime_rejected")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_runtime_rejected", "2026-01-01T00:02:00Z")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_runtime_rejected", "bind_bridge_runtime_rejected", 1, "pod_uid_runtime_rejected")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_runtime_rejected", "thr_bridge_runtime_rejected", "evt_bridge_runtime_rejected", 1, "user.message", `{"type":"user.message","content":[{"type":"text","text":"hello"}]}`)
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC) }
	job := RuntimeJob{
		JobID:                "qjob_bridge_runtime_rejected",
		LeaseToken:           "lease_bridge_runtime_rejected",
		Kind:                 queue.KindRuntimeInput,
		WorkspaceID:          "default",
		SessionID:            "sesn_bridge_runtime_rejected",
		PreparationAttemptID: "prep_bridge_runtime_rejected",
		SessionThreadID:      "thr_bridge_runtime_rejected",
		RuntimeInputID:       "rin_bridge_runtime_rejected",
		EventIDs:             []string{"evt_bridge_runtime_rejected"},
		SequenceFrom:         1,
		SequenceTo:           1,
		InputKind:            "messages",
		CommandKind:          agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:          `{"workspace_id":"default","session_id":"sesn_bridge_runtime_rejected"}`,
	}

	plan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand runtime rejection: %v", err)
	}
	if got := plan.Request.GetRuntimeInputId(); got != "rin_bridge_runtime_rejected" {
		t.Fatalf("prepared runtime input id = %q; want rin_bridge_runtime_rejected", got)
	}
	result := RuntimeDeliveryResult{
		Status:       RuntimeDeliveryRejected,
		Retryable:    false,
		ErrorKind:    "runtime_command_payload_too_large",
		ErrorMessage: "runtime command exceeds the transport fuse",
	}
	converted, err := store.PrepareRuntimeInputRejection(context.Background(), job, result)
	if err != nil {
		t.Fatalf("PrepareRuntimeInputRejection: %T %v", err, err)
	}
	if !converted {
		t.Fatal("PrepareRuntimeInputRejection converted = false; want true")
	}

	var inboxStatus string
	var inboxKind string
	var rejectionReason string
	var inboxUpdatedAt string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status, input_kind, rejection_reason_code, updated_at
		   FROM session_runtime_inbox
		  WHERE workspace_id = 'default'
		    AND runtime_input_id = 'rin_bridge_runtime_rejected'`).Scan(&inboxStatus, &inboxKind, &rejectionReason, &inboxUpdatedAt); err != nil {
		t.Fatalf("read rejected runtime inbox: %v", err)
	}
	if inboxStatus != "delivering" || inboxKind != "rejection" ||
		rejectionReason != "runtime_command_payload_too_large" ||
		inboxUpdatedAt != "2026-01-01T00:02:00Z" {
		t.Fatalf("rejected inbox status/kind/reason/updatedAt = %q/%q/%q/%q; want delivering bounded rejection",
			inboxStatus, inboxKind, rejectionReason, inboxUpdatedAt)
	}
	var inputProcessedAt sql.NullString
	var inputRevision int64
	if err := admin.QueryRowContext(context.Background(),
		`SELECT processed_at, revision
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND event_id = 'evt_bridge_runtime_rejected'`).Scan(&inputProcessedAt, &inputRevision); err != nil {
		t.Fatalf("read rejected input event: %v", err)
	}
	if inputProcessedAt.Valid || inputRevision != 1 {
		t.Fatalf("rejected input event processed=%v revision=%d; want untouched source until loop commit", inputProcessedAt.Valid, inputRevision)
	}

	var errorEventCount int
	var messageProjectionCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_runtime_rejected'
		    AND type = 'session.error'`).Scan(&errorEventCount); err != nil {
		t.Fatalf("count runtime rejection session.error rows: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_messages
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_runtime_rejected'`).Scan(&messageProjectionCount); err != nil {
		t.Fatalf("count runtime rejection message projections: %v", err)
	}
	if errorEventCount != 0 || messageProjectionCount != 0 {
		t.Fatalf("Bridge-authored rejection content = events %d messages %d; want none", errorEventCount, messageProjectionCount)
	}

	retryPlan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand bounded rejection: %v", err)
	}
	var bounded struct {
		InputKind  string `json:"input_kind"`
		ReasonCode string `json:"reason_code"`
	}
	if err := json.Unmarshal([]byte(retryPlan.Request.GetPayloadJson()), &bounded); err != nil {
		t.Fatalf("decode bounded rejection payload: %v", err)
	}
	if bounded.InputKind != "rejection" || bounded.ReasonCode != "runtime_command_payload_too_large" {
		t.Fatalf("bounded rejection payload = %+v; want closed source-free fact", bounded)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreSettlesTerminalPreparationFailureToolConfirmation(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_delivery_failed_tool", "thr_bridge_delivery_failed_tool")
	seedBridgeAPIPreparationFailed(t, admin, "default", "sesn_bridge_delivery_failed_tool", "prep_bridge_delivery_failed_tool", "github_credential_required")
	seedBridgeAPIPendingApproval(t, admin, "default", "sesn_bridge_delivery_failed_tool", "thr_bridge_delivery_failed_tool", "evt_bridge_failed_tool_use", 1)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_pending_tool_uses
		    SET status = 'resolving'
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_delivery_failed_tool'
		    AND tool_use_event_id = 'evt_bridge_failed_tool_use'`); err != nil {
		t.Fatalf("seed resolving pending approval: %v", err)
	}
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_delivery_failed_tool", "thr_bridge_delivery_failed_tool", "evt_bridge_failed_confirmation", 2, "user.tool_confirmation", `{"type":"user.tool_confirmation","tool_use_id":"evt_bridge_failed_tool_use","result":"allow"}`)
	seedBridgeAPIStreamChange(t, admin, "default", "sesn_bridge_delivery_failed_tool", "thr_bridge_delivery_failed_tool", "evt_bridge_failed_confirmation", 1, "public", true)

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC) }
	result, err := (RuntimePodDirectDeliverer{Store: store}).DeliverRuntimeJob(context.Background(), RuntimeJob{
		JobID:                "qjob_bridge_failed_confirmation",
		LeaseToken:           "lease_bridge_failed_confirmation",
		Kind:                 queue.KindRuntimeInput,
		WorkspaceID:          "default",
		SessionID:            "sesn_bridge_delivery_failed_tool",
		PreparationAttemptID: "prep_bridge_delivery_failed_tool",
		SessionThreadID:      "thr_bridge_delivery_failed_tool",
		RuntimeInputID:       "rin_bridge_failed_confirmation",
		EventIDs:             []string{"evt_bridge_failed_confirmation"},
		SequenceFrom:         2,
		SequenceTo:           2,
		InputKind:            "tool_confirmation",
		CommandKind:          agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_TOOL_CONFIRMATION,
		PayloadJSON:          `{"type":"tool_confirmation"}`,
	})
	if err != nil {
		t.Fatalf("DeliverRuntimeJob terminal preparation tool confirmation: %v", err)
	}
	if result.Status != RuntimeDeliveryAccepted {
		t.Fatalf("terminal preparation tool confirmation result = %#v; want accepted settlement", result)
	}

	var pendingStatus string
	var resultEventID sql.NullString
	var resolvedAt sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status, result_event_id, resolved_at
		   FROM session_pending_tool_uses
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_delivery_failed_tool'
		    AND tool_use_event_id = 'evt_bridge_failed_tool_use'`).Scan(&pendingStatus, &resultEventID, &resolvedAt); err != nil {
		t.Fatalf("read settled pending approval: %v", err)
	}
	var resultEventType string
	if resultEventID.Valid {
		if err := admin.QueryRowContext(context.Background(),
			`SELECT type FROM session_events WHERE workspace_id = 'default' AND event_id = $1`,
			resultEventID.String,
		).Scan(&resultEventType); err != nil {
			t.Fatalf("read pending result event: %v", err)
		}
	}
	var confirmationProcessedAt sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT processed_at
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND event_id = 'evt_bridge_failed_confirmation'`).Scan(&confirmationProcessedAt); err != nil {
		t.Fatalf("read confirmation processed_at: %v", err)
	}
	if pendingStatus != "cancelled" || !resultEventID.Valid || resultEventType != "session.error" || !resolvedAt.Valid || !confirmationProcessedAt.Valid {
		t.Fatalf("pending approval status=%q result=%v resultType=%q resolved=%v confirmationProcessed=%v; want cancelled by session.error",
			pendingStatus, resultEventID, resultEventType, resolvedAt.Valid, confirmationProcessedAt.Valid)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreRequeuesStaleSandboxPreparation(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_delivery_stale", "thr_bridge_delivery_stale")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_delivery_stale", "prep_bridge_delivery_stale")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_delivery_stale", "2026-01-01T00:00:00Z")

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC) }
	store.SandboxStatusFreshnessWindow = time.Minute
	_, err := store.PrepareRuntimeCommand(context.Background(), RuntimeJob{
		JobID:                "qjob_bridge_delivery_stale",
		LeaseToken:           "lease_bridge_delivery_stale",
		Kind:                 queue.KindRuntimeInput,
		WorkspaceID:          "default",
		SessionID:            "sesn_bridge_delivery_stale",
		PreparationAttemptID: "prep_bridge_delivery_stale",
		SessionThreadID:      "thr_bridge_delivery_stale",
		RuntimeInputID:       "rin_bridge_delivery_stale",
		EventIDs:             []string{"evt_bridge_delivery_stale"},
		SequenceFrom:         1,
		SequenceTo:           1,
		InputKind:            "messages",
		CommandKind:          agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:          `{"type":"messages"}`,
	})
	result := runtimeDeliveryResultFromPrepareError(err)
	if result.Status != RuntimeDeliveryRejected || !result.Retryable || result.ErrorKind != "runtime_preparation_not_ready" {
		t.Fatalf("prepare stale sandbox result = %#v err=%v; want retryable runtime_preparation_not_ready", result, err)
	}
	assertSessionPrepareRequeued(t, admin, "default", "sesn_bridge_delivery_stale", "prep_bridge_delivery_stale")
}

func TestPostgreSQLRuntimeDeliveryStoreDoesNotRequeueInFlightSandboxLifecycle(t *testing.T) {
	for _, sandboxStatus := range []string{"creating", "resuming", "releasing"} {
		t.Run(sandboxStatus, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID := "sesn_bridge_delivery_" + sandboxStatus + "_sandbox"
			threadID := "thr_bridge_delivery_" + sandboxStatus + "_sandbox"
			preparationID := "prep_bridge_delivery_" + sandboxStatus + "_sandbox"

			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIPreparationReady(t, admin, "default", sessionID, preparationID)
			seedBridgeAPIActiveSandbox(t, admin, "default", sessionID, "2026-01-01T00:02:00Z")
			setBridgeAPISandboxStatus(t, admin, "default", sessionID, sandboxStatus)

			store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
			store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC) }
			store.SandboxStatusFreshnessWindow = time.Minute
			_, err := store.PrepareRuntimeCommand(context.Background(), RuntimeJob{
				JobID:                "qjob_bridge_delivery_" + sandboxStatus + "_sandbox",
				LeaseToken:           "lease_bridge_delivery_" + sandboxStatus + "_sandbox",
				Kind:                 queue.KindRuntimeInput,
				WorkspaceID:          "default",
				SessionID:            sessionID,
				PreparationAttemptID: preparationID,
				SessionThreadID:      threadID,
				RuntimeInputID:       "rin_bridge_delivery_" + sandboxStatus + "_sandbox",
				EventIDs:             []string{"evt_bridge_delivery_" + sandboxStatus + "_sandbox"},
				SequenceFrom:         1,
				SequenceTo:           1,
				InputKind:            "messages",
				CommandKind:          agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
				PayloadJSON:          `{"type":"messages"}`,
			})
			result := runtimeDeliveryResultFromPrepareError(err)
			if result.Status != RuntimeDeliveryRejected || !result.Retryable || result.ErrorKind != "runtime_preparation_not_ready" {
				t.Fatalf("prepare %s sandbox result = %#v err=%v; want retryable runtime_preparation_not_ready", sandboxStatus, result, err)
			}
			assertSessionPreparationReady(t, admin, "default", sessionID, preparationID)
			assertNoSessionPrepareJobsForSession(t, admin, "default", sessionID)
		})
	}
}

func TestPostgreSQLRuntimeDeliveryStoreDoesNotRequeueFailedSandboxPreparation(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_delivery_failed_sandbox", "thr_bridge_delivery_failed_sandbox")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_delivery_failed_sandbox", "thr_bridge_delivery_failed_sandbox", "evt_bridge_delivery_failed_sandbox", 1, "user.message", `{"content":[{"type":"text","text":"hello"}]}`)
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_delivery_failed_sandbox", "prep_bridge_delivery_failed_sandbox")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_delivery_failed_sandbox", "2026-01-01T00:02:00Z")
	if _, err := admin.ExecContext(context.Background(), `UPDATE sandboxes SET status = 'failed' WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_delivery_failed_sandbox'`); err != nil {
		t.Fatalf("mark sandbox failed: %v", err)
	}

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC) }
	plan, err := store.PrepareRuntimeCommand(context.Background(), RuntimeJob{
		JobID:                "qjob_bridge_delivery_failed_sandbox",
		LeaseToken:           "lease_bridge_delivery_failed_sandbox",
		Kind:                 queue.KindRuntimeInput,
		WorkspaceID:          "default",
		SessionID:            "sesn_bridge_delivery_failed_sandbox",
		PreparationAttemptID: "prep_bridge_delivery_failed_sandbox",
		SessionThreadID:      "thr_bridge_delivery_failed_sandbox",
		RuntimeInputID:       "rin_bridge_delivery_failed_sandbox",
		EventIDs:             []string{"evt_bridge_delivery_failed_sandbox"},
		SequenceFrom:         1,
		SequenceTo:           1,
		InputKind:            "messages",
		CommandKind:          agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:          `{"type":"messages"}`,
	})
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand failed sandbox: %v", err)
	}
	if !plan.SettledAccepted || plan.Request != nil {
		t.Fatalf("plan = %#v; want terminal preparation settlement without Runtime command", plan)
	}
	assertSessionPreparationReady(t, admin, "default", "sesn_bridge_delivery_failed_sandbox", "prep_bridge_delivery_failed_sandbox")
	assertNoSessionPrepareJobsForSession(t, admin, "default", "sesn_bridge_delivery_failed_sandbox")
}

func TestPostgreSQLRuntimeDeliveryStoreStaleAcceptedBeforePreparationReset(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_delivery_stale_accepted", "thr_bridge_delivery_stale_accepted")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_delivery_stale_accepted", "prep_bridge_delivery_stale_accepted")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_delivery_stale_accepted", "2026-01-01T00:00:00Z")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_delivery_stale_accepted", "thr_bridge_delivery_stale_accepted", "evt_bridge_delivery_stale_accepted", 1, "user.message", `{"content":[{"type":"text","text":"already processed"}]}`)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_events
		    SET processed_at = '2026-01-01T00:00:30Z'
		  WHERE workspace_id = 'default'
		    AND event_id = 'evt_bridge_delivery_stale_accepted'`); err != nil {
		t.Fatalf("mark stale replay event processed: %v", err)
	}

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC) }
	store.SandboxStatusFreshnessWindow = time.Minute
	plan, err := store.PrepareRuntimeCommand(context.Background(), RuntimeJob{
		JobID:                "qjob_bridge_delivery_stale_accepted",
		LeaseToken:           "lease_bridge_delivery_stale_accepted",
		Kind:                 queue.KindRuntimeInput,
		WorkspaceID:          "default",
		SessionID:            "sesn_bridge_delivery_stale_accepted",
		PreparationAttemptID: "prep_bridge_delivery_stale_accepted",
		SessionThreadID:      "thr_bridge_delivery_stale_accepted",
		RuntimeInputID:       "rin_bridge_delivery_stale_accepted",
		EventIDs:             []string{"evt_bridge_delivery_stale_accepted"},
		SequenceFrom:         1,
		SequenceTo:           1,
		InputKind:            "messages",
		CommandKind:          agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:          `{"type":"messages"}`,
	})
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand stale accepted replay: %v", err)
	}
	if !plan.StaleAccepted || plan.Request != nil {
		t.Fatalf("plan = %#v; want stale accepted without Runtime command", plan)
	}
	assertSessionPreparationReady(t, admin, "default", "sesn_bridge_delivery_stale_accepted", "prep_bridge_delivery_stale_accepted")
	assertNoSessionPrepareJobsForSession(t, admin, "default", "sesn_bridge_delivery_stale_accepted")
}

func TestPostgreSQLRuntimeDeliveryStoreStaleSandboxPreservesFreshCredentialForActiveRemount(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_delivery_stale_cred", "thr_bridge_delivery_stale_cred")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_delivery_stale_cred", "prep_bridge_delivery_stale_cred")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_delivery_stale_cred", "2026-01-01T00:00:00Z")
	seedBridgeAPIResourceCredentialExpiresAt(t, admin, "default", "sesn_bridge_delivery_stale_cred", "prep_bridge_delivery_stale_cred", "2026-01-02T00:00:00Z")

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC) }
	store.SandboxStatusFreshnessWindow = time.Minute
	store.ResourceCredentialRefreshMargin = 30 * time.Minute
	_, err := store.PrepareRuntimeCommand(context.Background(), RuntimeJob{
		JobID:                "qjob_bridge_delivery_stale_cred",
		LeaseToken:           "lease_bridge_delivery_stale_cred",
		Kind:                 queue.KindRuntimeInput,
		WorkspaceID:          "default",
		SessionID:            "sesn_bridge_delivery_stale_cred",
		PreparationAttemptID: "prep_bridge_delivery_stale_cred",
		SessionThreadID:      "thr_bridge_delivery_stale_cred",
		RuntimeInputID:       "rin_bridge_delivery_stale_cred",
		EventIDs:             []string{"evt_bridge_delivery_stale_cred"},
		SequenceFrom:         1,
		SequenceTo:           1,
		InputKind:            "messages",
		CommandKind:          agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:          `{"type":"messages"}`,
	})
	result := runtimeDeliveryResultFromPrepareError(err)
	if result.Status != RuntimeDeliveryRejected || !result.Retryable || result.ErrorKind != "runtime_preparation_not_ready" {
		t.Fatalf("prepare stale sandbox with credential result = %#v err=%v; want retryable runtime_preparation_not_ready", result, err)
	}
	assertSessionPrepareRequeuedPreservingCredential(t, admin, "default", "sesn_bridge_delivery_stale_cred", "prep_bridge_delivery_stale_cred", "2026-01-02T00:00:00Z")
}

func TestPostgreSQLRuntimeDeliveryStoreRequeuesExpiringResourceCredential(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_delivery_cred_expiring", "thr_bridge_delivery_cred_expiring")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_delivery_cred_expiring", "prep_bridge_delivery_cred_expiring")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_delivery_cred_expiring", "2026-01-01T00:40:00Z")
	seedBridgeAPIResourceCredentialExpiresAt(t, admin, "default", "sesn_bridge_delivery_cred_expiring", "prep_bridge_delivery_cred_expiring", "2026-01-01T01:00:00Z")

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 40, 1, 0, time.UTC) }
	store.SandboxStatusFreshnessWindow = time.Minute
	store.ResourceCredentialRefreshMargin = 30 * time.Minute
	_, err := store.PrepareRuntimeCommand(context.Background(), RuntimeJob{
		JobID:                "qjob_bridge_delivery_cred_expiring",
		LeaseToken:           "lease_bridge_delivery_cred_expiring",
		Kind:                 queue.KindRuntimeInput,
		WorkspaceID:          "default",
		SessionID:            "sesn_bridge_delivery_cred_expiring",
		PreparationAttemptID: "prep_bridge_delivery_cred_expiring",
		SessionThreadID:      "thr_bridge_delivery_cred_expiring",
		RuntimeInputID:       "rin_bridge_delivery_cred_expiring",
		EventIDs:             []string{"evt_bridge_delivery_cred_expiring"},
		SequenceFrom:         1,
		SequenceTo:           1,
		InputKind:            "messages",
		CommandKind:          agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:          `{"type":"messages"}`,
	})
	result := runtimeDeliveryResultFromPrepareError(err)
	if result.Status != RuntimeDeliveryRejected || !result.Retryable || result.ErrorKind != "runtime_preparation_not_ready" {
		t.Fatalf("prepare expiring resource credential result = %#v err=%v; want retryable runtime_preparation_not_ready", result, err)
	}
	assertSessionPrepareRequeuedForCredentialRotation(t, admin, "default", "sesn_bridge_delivery_cred_expiring", "prep_bridge_delivery_cred_expiring")
}
