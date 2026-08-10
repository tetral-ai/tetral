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

func TestPostgreSQLRuntimeDeliveryStoreInitialMCPManifestCrashAfterCaptureRedrivesDurableGeneration(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_initial_mcp", "thr_bridge_initial_mcp")
	seedBridgeAPIAgentConfig(t, admin, "default", "sesn_bridge_initial_mcp", `{"name":"agent","model":"anthropic/claude-opus-4-8","tools":[{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"enabled":false,"permission_policy":{"type":"always_ask"}},"configs":[{"name":"github_search","enabled":true,"permission_policy":{"type":"always_allow"}}]}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}],"skills":[],"metadata":{}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET installed_tools_json = '{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"enabled":false,"permission_policy":{"type":"always_ask"}},"configs":[{"name":"github_search","enabled":true,"permission_policy":{"type":"always_allow"}}]}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}]}' WHERE workspace_id = 'default' AND id = 'sesn_bridge_initial_mcp'`); err != nil {
		t.Fatalf("seed durable initial MCP config: %v", err)
	}
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
		JobID:           "qjob_bridge_initial_mcp",
		LeaseToken:      "lease_bridge_initial_mcp",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       "sesn_bridge_initial_mcp",
		SessionThreadID: "thr_bridge_initial_mcp",
		RuntimeInputID:  "rin_bridge_initial_mcp",
		EventIDs:        []string{"evt_bridge_initial_mcp"},
		SequenceFrom:    1,
		SequenceTo:      1,
		InputKind:       "messages",
		CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:     `{"type":"messages"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, job)

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
			AgentRuntimeGRPCPort:    9090,
			MCPConnectorGRPCAddress: listener.Addr().String(),
			GatewayTokenPath:        tokenPath,
		},
		func() enginekubernetes.BindingVisibilitySnapshot {
			return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{candidate})
		},
	)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	job := RuntimeJob{
		JobID:           "qjob_job_runner_initial_mcp",
		LeaseToken:      "lease_job_runner_initial_mcp",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     workspaceID,
		SessionID:       sessionID,
		SessionThreadID: threadID,
		RuntimeInputID:  "rin_job_runner_initial_mcp",
		EventIDs:        []string{eventID},
		SequenceFrom:    1,
		SequenceTo:      1,
		InputKind:       "messages",
		CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:     `{"type":"messages"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, job)

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

func TestPostgreSQLRuntimeDeliveryStoreMarksInboxAcceptedAfterRuntimeAccepts(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_inbox_accept", "thr_bridge_inbox_accept")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_inbox_accept", "bind_bridge_inbox_accept", 1, "pod_uid_inbox_accept")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_inbox_accept", "thr_bridge_inbox_accept", "sevt_inbox_accept", 1, "user.message", `{"content":[{"type":"text","text":"hello"}]}`)

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 4, 4, 0, time.UTC) }
	sender := &recordingRuntimeCommandSender{response: &agentruntimev1.RuntimeInputCommandResponse{
		Status: agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED,
	}}
	job := RuntimeJob{
		JobID:           "qjob_inbox_accept",
		LeaseToken:      "lease_inbox_accept",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       "sesn_bridge_inbox_accept",
		SessionThreadID: "thr_bridge_inbox_accept",
		RuntimeInputID:  "rin_inbox_accept",
		EventIDs:        []string{"sevt_inbox_accept"},
		SequenceFrom:    1,
		SequenceTo:      1,
		InputKind:       "messages",
		CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:     `{"workspace_id":"default","session_id":"sesn_bridge_inbox_accept","session_thread_id":"thr_bridge_inbox_accept","runtime_input_id":"rin_inbox_accept","event_ids":["sevt_inbox_accept"],"sequence_from":1,"sequence_to":1,"input_kind":"messages"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, job)

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
			"input_kind":    "messages",
		})
		if err != nil {
			t.Fatalf("marshal chunk %d: %v", index+1, err)
		}
		job := RuntimeJob{
			JobID: fmt.Sprintf("qjob_bridge_chunk_%d", index+1), LeaseToken: fmt.Sprintf("lease_bridge_chunk_%d", index+1),
			Kind: queue.KindRuntimeInput, WorkspaceID: "default", SessionID: sessionID,
			SessionThreadID: threadID,
			RuntimeInputID:  runtimeInputID, EventIDs: chunk,
			SequenceFrom: int64(index*queue.MaxRuntimeInputEventRefsPerJob + 1),
			SequenceTo:   int64(index*queue.MaxRuntimeInputEventRefsPerJob + len(chunk)),
			InputKind:    "messages", CommandKind: agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
			PayloadJSON: string(payload),
		}
		seedRuntimeInboxBirthForJob(t, admin, job)
		plan, err := store.PrepareRuntimeCommand(context.Background(), job)
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
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_inbox_accept_fence", "thr_bridge_inbox_accept_fence", "sevt_inbox_accept_fence", 1, "user.message", `{"content":[{"type":"text","text":"hello"}]}`)

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 4, 4, 0, time.UTC) }
	job := RuntimeJob{
		JobID:           "qjob_inbox_accept_fence",
		LeaseToken:      "lease_inbox_accept_fence",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       "sesn_bridge_inbox_accept_fence",
		SessionThreadID: "thr_bridge_inbox_accept_fence",
		RuntimeInputID:  "rin_inbox_accept_fence",
		EventIDs:        []string{"sevt_inbox_accept_fence"},
		SequenceFrom:    1,
		SequenceTo:      1,
		InputKind:       "messages",
		CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:     `{"workspace_id":"default","session_id":"sesn_bridge_inbox_accept_fence","session_thread_id":"thr_bridge_inbox_accept_fence","runtime_input_id":"rin_inbox_accept_fence","event_ids":["sevt_inbox_accept_fence"],"sequence_from":1,"sequence_to":1,"input_kind":"messages"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, job)
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
	_, err = store.MarkRuntimeInputAccepted(context.Background(), job, plan.Request)
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
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_inbox_conflict", "thr_bridge_inbox_conflict", "sevt_inbox_conflict_one", 1, "user.message", `{"content":[{"type":"text","text":"one"}]}`)
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_inbox_conflict", "thr_bridge_inbox_conflict", "sevt_inbox_conflict_two", 2, "user.message", `{"content":[{"type":"text","text":"two"}]}`)

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 4, 5, 0, time.UTC) }
	first := RuntimeJob{
		JobID:           "qjob_inbox_conflict_one",
		LeaseToken:      "lease_inbox_conflict_one",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       "sesn_bridge_inbox_conflict",
		SessionThreadID: "thr_bridge_inbox_conflict",
		RuntimeInputID:  "rin_inbox_conflict",
		EventIDs:        []string{"sevt_inbox_conflict_one"},
		SequenceFrom:    1,
		SequenceTo:      1,
		InputKind:       "messages",
		CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:     `{"workspace_id":"default","session_id":"sesn_bridge_inbox_conflict","session_thread_id":"thr_bridge_inbox_conflict","runtime_input_id":"rin_inbox_conflict","event_ids":["sevt_inbox_conflict_one"],"sequence_from":1,"sequence_to":1,"input_kind":"messages"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, first)
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
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_control_delivery", "thr_bridge_control_delivery", "sevt_interrupt_control", 1, "user.interrupt", `{}`)
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_control_delivery", "thr_bridge_control_delivery", "sevt_confirmation_control", 2, "user.tool_confirmation", `{"tool_use_id":"sevt_tool_control","result":"deny","deny_message":"not now"}`)

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 3, 0, 0, time.UTC) }

	interruptJob := RuntimeJob{
		JobID:           "qjob_bridge_interrupt",
		LeaseToken:      "lease_bridge_interrupt",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       "sesn_bridge_control_delivery",
		SessionThreadID: "thr_bridge_control_delivery",
		RuntimeInputID:  "rin_bridge_interrupt",
		EventIDs:        []string{"sevt_interrupt_control"},
		SequenceFrom:    1,
		SequenceTo:      1,
		InputKind:       "interrupt_control",
		CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_INTERRUPT_CONTROL,
		PayloadJSON:     `{"workspace_id":"default","session_id":"sesn_bridge_control_delivery","session_thread_id":"thr_bridge_control_delivery","runtime_input_id":"rin_bridge_interrupt","event_ids":["sevt_interrupt_control"],"sequence_from":1,"sequence_to":1,"input_kind":"interrupt_control"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, interruptJob)
	interrupt, err := store.PrepareRuntimeCommand(context.Background(), interruptJob)
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

	confirmationJob := RuntimeJob{
		JobID:           "qjob_bridge_confirmation",
		LeaseToken:      "lease_bridge_confirmation",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       "sesn_bridge_control_delivery",
		SessionThreadID: "thr_bridge_control_delivery",
		RuntimeInputID:  "rin_bridge_confirmation",
		EventIDs:        []string{"sevt_confirmation_control"},
		SequenceFrom:    2,
		SequenceTo:      2,
		InputKind:       "tool_confirmation",
		CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_TOOL_CONFIRMATION,
		PayloadJSON:     `{"workspace_id":"default","session_id":"sesn_bridge_control_delivery","session_thread_id":"thr_bridge_control_delivery","runtime_input_id":"rin_bridge_confirmation","event_ids":["sevt_confirmation_control"],"sequence_from":2,"sequence_to":2,"input_kind":"tool_confirmation"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, confirmationJob)
	confirmation, err := store.PrepareRuntimeCommand(context.Background(), confirmationJob)
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

		job := RuntimeJob{
			JobID:           "qjob_superseded_messages",
			LeaseToken:      "lease_superseded_messages",
			Kind:            queue.KindRuntimeInput,
			WorkspaceID:     "default",
			SessionID:       "sesn_bridge_interrupt_fence",
			SessionThreadID: "thr_bridge_interrupt_fence",
			RuntimeInputID:  "rin_superseded_messages",
			EventIDs:        []string{"sevt_old_message_1", "sevt_old_message_2"},
			SequenceFrom:    1,
			SequenceTo:      2,
			InputKind:       "messages",
			CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
			PayloadJSON:     `{"workspace_id":"default","session_id":"sesn_bridge_interrupt_fence","session_thread_id":"thr_bridge_interrupt_fence","runtime_input_id":"rin_superseded_messages","event_ids":["sevt_old_message_1","sevt_old_message_2"],"sequence_from":1,"sequence_to":2,"input_kind":"messages"}`,
		}
		seedRuntimeInboxBirthForJob(t, admin, job)
		plan, err := store.PrepareRuntimeCommand(context.Background(), job)
		if err != nil {
			t.Fatalf("PrepareRuntimeCommand superseded messages: %v", err)
		}
		if !plan.StaleAccepted || plan.Request != nil {
			t.Fatalf("superseded plan = %+v; want stale accepted without Runtime request", plan)
		}
		var inboxStatus string
		if err := admin.QueryRowContext(context.Background(),
			`SELECT status
			   FROM session_runtime_inbox
			  WHERE workspace_id = 'default'
			    AND runtime_input_id = 'rin_superseded_messages'`).Scan(&inboxStatus); err != nil {
			t.Fatalf("read superseded inbox custody: %v", err)
		}
		if inboxStatus != "cancelled" {
			t.Fatalf("superseded inbox status = %q; want cancelled", inboxStatus)
		}
	})

	t.Run("mixed superseded and deliverable messages are fatal", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		seedBridgeAPISession(t, admin, "default", "sesn_bridge_interrupt_mixed", "thr_bridge_interrupt_mixed")
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
			JobID:           "qjob_mixed_messages",
			LeaseToken:      "lease_mixed_messages",
			Kind:            queue.KindRuntimeInput,
			WorkspaceID:     "default",
			SessionID:       "sesn_bridge_interrupt_mixed",
			SessionThreadID: "thr_bridge_interrupt_mixed",
			RuntimeInputID:  "rin_mixed_messages",
			EventIDs:        []string{"sevt_old_message", "sevt_new_message"},
			SequenceFrom:    1,
			SequenceTo:      4,
			InputKind:       "messages",
			CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
			PayloadJSON:     `{"workspace_id":"default","session_id":"sesn_bridge_interrupt_mixed","session_thread_id":"thr_bridge_interrupt_mixed","runtime_input_id":"rin_mixed_messages","event_ids":["sevt_old_message","sevt_new_message"],"sequence_from":1,"sequence_to":4,"input_kind":"messages"}`,
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

		job := RuntimeJob{
			JobID:           "qjob_post_fence_message",
			LeaseToken:      "lease_post_fence_message",
			Kind:            queue.KindRuntimeInput,
			WorkspaceID:     "default",
			SessionID:       "sesn_bridge_interrupt_post_fence",
			SessionThreadID: "thr_bridge_interrupt_post_fence",
			RuntimeInputID:  "rin_post_fence_message",
			EventIDs:        []string{"sevt_new_message"},
			SequenceFrom:    4,
			SequenceTo:      4,
			InputKind:       "messages",
			CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
			PayloadJSON:     `{"workspace_id":"default","session_id":"sesn_bridge_interrupt_post_fence","session_thread_id":"thr_bridge_interrupt_post_fence","runtime_input_id":"rin_post_fence_message","event_ids":["sevt_new_message"],"sequence_from":4,"sequence_to":4,"input_kind":"messages"}`,
		}
		seedRuntimeInboxBirthForJob(t, admin, job)
		plan, err := store.PrepareRuntimeCommand(context.Background(), job)
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
	job := RuntimeJob{
		JobID:           "qjob_bridge_resolve",
		LeaseToken:      "lease_bridge_resolve",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       "sesn_bridge_resolve",
		SessionThreadID: "thr_bridge_resolve",
		RuntimeInputID:  "rin_bridge_resolve",
		EventIDs:        []string{"evt_bridge_resolve"},
		SequenceFrom:    1,
		SequenceTo:      1,
		InputKind:       "messages",
		CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:     `{"type":"messages"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, job)
	plan, err := store.PrepareRuntimeCommand(context.Background(), job)
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

func TestPostgreSQLRuntimeDeliveryStoreConvertsOversizedInputToBoundedLoopRejection(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_runtime_rejected", "thr_bridge_runtime_rejected")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_runtime_rejected", "bind_bridge_runtime_rejected", 1, "pod_uid_runtime_rejected")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_runtime_rejected", "thr_bridge_runtime_rejected", "evt_bridge_runtime_rejected", 1, "user.message", `{"type":"user.message","content":[{"type":"text","text":"hello"}]}`)
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC) }
	job := RuntimeJob{
		JobID:           "qjob_bridge_runtime_rejected",
		LeaseToken:      "lease_bridge_runtime_rejected",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       "sesn_bridge_runtime_rejected",
		SessionThreadID: "thr_bridge_runtime_rejected",
		RuntimeInputID:  "rin_bridge_runtime_rejected",
		EventIDs:        []string{"evt_bridge_runtime_rejected"},
		SequenceFrom:    1,
		SequenceTo:      1,
		InputKind:       "messages",
		CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:     `{"workspace_id":"default","session_id":"sesn_bridge_runtime_rejected"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, job)

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
