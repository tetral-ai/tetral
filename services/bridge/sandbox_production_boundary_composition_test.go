package agentruntimebridge

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/internalgrpc"
	internalgrpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestPostgreSQLSandboxProductionBoundaryLostACKAndLeaseTakeover(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		workspaceID     = "default"
		sessionID       = "sesn_sandbox_production_boundary"
		threadID        = "thr_sandbox_production_boundary"
		bindingID       = "bind_sandbox_production_boundary"
		podUID          = "pod_sandbox_production_boundary"
		modelRequestID  = "mreq_sandbox_production_boundary"
		modelToolCallID = "call_sandbox_production_boundary"
	)
	seedBridgeAPISession(t, admin, workspaceID, sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, workspaceID, sessionID, bindingID, 1, podUID)
	seedReadySandboxForSharedToolExecution(t, admin, workspaceID, sessionID)

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	bridge := &sandboxProductionBoundaryBridgeServer{store: store}
	client, bridgeAddress := startSandboxProductionBoundaryBridgeClient(t, bridge, podUID)
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	if _, err := client.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{Scope: scope}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unauthenticated production Bridge request error = %v; want Unauthenticated", err)
	}
	wrongPodContext := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer sandbox-production-wrong-pod-token")
	if _, err := client.WriteEvent(wrongPodContext, &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_sandbox_wrong_pod", ModelRequestId: "mreq_sandbox_wrong_pod",
		EventType: "span.model_request_start", PayloadJson: `{"type":"span.model_request_start","model_request_id":"mreq_sandbox_wrong_pod"}`,
		ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: "agent_provider_request",
	}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("wrong-pod production Bridge request error = %v; want PermissionDenied", err)
	}
	runtimeContext := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer sandbox-production-runtime-token")

	start, err := client.WriteEvent(runtimeContext, &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_sandbox_production_start", ModelRequestId: modelRequestID,
		EventType: "span.model_request_start", PayloadJson: `{"type":"span.model_request_start","model_request_id":"` + modelRequestID + `"}`,
		ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: "agent_provider_request",
	})
	if err != nil || start.GetCommitted() == nil {
		t.Fatalf("WriteEvent request start = %#v/%v; want committed", start, err)
	}
	const inputJSON = `{"cmd":"printf production-boundary"}`
	toolUse, err := client.WriteEvent(runtimeContext, &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_sandbox_production_tool", ModelRequestId: modelRequestID,
		EventType: "agent.tool_use", SessionVisible: true,
		PayloadJson:           `{"type":"agent.tool_use","name":"exec_command","input":` + inputJSON + `,"evaluated_permission":"allow"}`,
		AssistantContextDelta: bridgeToolCallContextDeltaForTest(modelToolCallID, "exec_command", inputJSON),
	})
	if err != nil || toolUse.GetCommitted() == nil || toolUse.GetCommitted().GetEventId() == "" {
		t.Fatalf("WriteEvent Tool use = %#v/%v; want committed durable target", toolUse, err)
	}
	toolUseEventID := toolUse.GetCommitted().GetEventId()

	bunPath, err := exec.LookPath("bun")
	if err != nil {
		t.Fatalf("Sandbox Runtime composition requires bun: %v", err)
	}
	runtimeRoot := filepath.Clean(filepath.Join("..", "agent-runtime"))
	tokenPath := filepath.Join(t.TempDir(), "bridge-token")
	if err := os.WriteFile(tokenPath, []byte("sandbox-production-runtime-token\n"), 0o600); err != nil {
		t.Fatalf("write Runtime composition token: %v", err)
	}
	fixtureInput, err := json.Marshal(map[string]any{
		"address": bridgeAddress, "tokenPath": tokenPath, "workspaceId": workspaceID,
		"sessionId": sessionID, "sessionThreadId": threadID, "bindingId": bindingID,
		"bindingGeneration": 1, "targetPodUid": podUID, "toolUseEventId": toolUseEventID,
		"modelRequestId": modelRequestID, "modelToolCallId": modelToolCallID,
	})
	if err != nil {
		t.Fatalf("encode Runtime Sandbox composition input: %v", err)
	}
	inputPath := filepath.Join(t.TempDir(), "sandbox-production.json")
	if err := os.WriteFile(inputPath, fixtureInput, 0o600); err != nil {
		t.Fatalf("write Runtime Sandbox composition input: %v", err)
	}
	commandContext, cancelCommand := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelCommand()
	command := exec.CommandContext(commandContext, bunPath, "run", "packages/runtime-pod/test/fixtures/sandbox-production-composition.ts", inputPath) //nolint:gosec // fixed repository fixture and test-owned input.
	command.Dir = runtimeRoot
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatalf("start Runtime Sandbox production composition: %v", err)
	}

	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	var abandoned []*queue.Job
	for deadline := time.Now().Add(5 * time.Second); len(abandoned) == 0 && time.Now().Before(deadline); {
		abandoned, err = queueStore.Lease(context.Background(), queue.LeaseRequest{
			WorkspaceID: workspace.ID(workspaceID), Kinds: []string{queue.KindSandboxToolExecute},
			LeaseOwner: "sandbox-production-abandoned", MaxJobs: 1, LeaseDuration: time.Millisecond,
		})
		if err == nil && len(abandoned) == 0 {
			time.Sleep(20 * time.Millisecond)
		}
	}
	if err != nil || len(abandoned) != 1 {
		t.Fatalf("abandoned Sandbox lease = %#v/%v; want one lease", abandoned, err)
	}
	time.Sleep(10 * time.Millisecond)
	reclaimed, err := queueStore.ReclaimExpiredLeases(context.Background(), queue.ReclaimExpiredLeasesRequest{
		WorkspaceID: workspace.ID(workspaceID), Kind: queue.KindSandboxToolExecute, Limit: 1,
	})
	if err != nil || reclaimed != 1 {
		t.Fatalf("reclaim expired Sandbox lease = %d/%v; want one", reclaimed, err)
	}

	queueConnection := startBackgroundNotificationQueueServer(t, queueStore)
	provider := &sandboxProductionBoundaryProvider{bridgeMemoryProjectionProvider: &bridgeMemoryProjectionProvider{}}
	registry, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{
		"daytona": provider,
	})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &tetralsandbox.SandboxToolExecutionJobRunner{
		Queue:       tetralsandbox.SandboxQueueFromGRPC(queuev1.NewQueueServiceClient(queueConnection)),
		Coordinator: tetralsandbox.NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtime), 30*time.Minute),
		Providers:   registry,
		Media:       backgroundNotificationMedia{},
		Config: tetralsandbox.SandboxToolExecutionRunnerConfig{
			WorkspaceID: workspaceID, LeaseOwner: "sandbox-production-takeover", MaxJobs: 1,
			LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second, PreparationTimeout: 45 * time.Second,
		},
	}
	active, err := runner.RunOnceWithActivity(context.Background())
	if err != nil || !active {
		t.Fatalf("Sandbox takeover runner = active %v, err %v; want true,nil", active, err)
	}

	if err := command.Wait(); err != nil {
		t.Fatalf("run Runtime Sandbox production composition: %v\nstdout=%s\nstderr=%s", err, stdout.String(), stderr.String())
	}
	var runtimeResult struct {
		Result struct {
			Type string `json:"type"`
		} `json:"result"`
		Settlement struct {
			Type string `json:"type"`
		} `json:"settlement"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &runtimeResult); err != nil || runtimeResult.Result.Type != "completed" || runtimeResult.Settlement.Type != "duplicate" {
		t.Fatalf("Runtime Sandbox result = %q/%v; want completed/duplicate", stdout.String(), err)
	}

	var executionState string
	var retainedResult sql.NullString
	var resultEvents int
	var queueStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT execution_state, result_json
		FROM session_runtime_tool_results WHERE workspace_id=$1 AND session_id=$2 AND tool_use_event_id=$3`,
		workspaceID, sessionID, toolUseEventID).Scan(&executionState, &retainedResult); err != nil {
		t.Fatalf("read consumed Sandbox execution: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events
		WHERE workspace_id=$1 AND session_id=$2 AND type='agent.tool_result'
		  AND payload_json::jsonb ->> 'tool_use_id'=$3`, workspaceID, sessionID, toolUseEventID).Scan(&resultEvents); err != nil {
		t.Fatalf("count durable Tool result events: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM queue_jobs
		WHERE workspace_id=$1 AND kind='sandbox_tool_execute' AND partition_key=$2`, workspaceID,
		queue.FormatSandboxExecutionPartitionKey(workspace.ID(workspaceID), sessionID, threadID, toolUseEventID)).Scan(&queueStatus); err != nil {
		t.Fatalf("read Sandbox queue status: %v", err)
	}
	acceptDropped, settlementDropped := bridge.snapshot()
	if executionState != "consumed" || retainedResult.Valid || resultEvents != 1 || queueStatus != "acknowledged" ||
		provider.calls.Load() != 1 || !acceptDropped || !settlementDropped {
		t.Fatalf("production boundary = state %q retained %v events %d queue %q calls %d dropped %v/%v; want consumed/NULL/1/acknowledged/1/true/true",
			executionState, retainedResult, resultEvents, queueStatus, provider.calls.Load(), acceptDropped, settlementDropped)
	}
}

type sandboxProductionBoundaryBridgeServer struct {
	bridgev1.UnimplementedAgentRuntimeBridgeServiceServer
	store *PostgreSQLBridgeAPIStore

	mu                   sync.Mutex
	acceptACKDropped     bool
	settlementACKDropped bool
}

func (s *sandboxProductionBoundaryBridgeServer) WriteEvent(ctx context.Context, request *bridgev1.WriteEventRequest) (*bridgev1.WriteEventResponse, error) {
	return s.store.WriteEvent(ctx, request)
}

func (s *sandboxProductionBoundaryBridgeServer) AcceptSandboxExecution(ctx context.Context, request *bridgev1.AcceptSandboxExecutionRequest) (*bridgev1.AcceptSandboxExecutionResponse, error) {
	response, err := s.store.AcceptSandboxExecution(ctx, request)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.acceptACKDropped {
		s.acceptACKDropped = true
		return nil, status.Error(codes.Unavailable, "Sandbox acceptance acknowledgement unavailable")
	}
	return response, nil
}

func (s *sandboxProductionBoundaryBridgeServer) AwaitSandboxExecution(ctx context.Context, request *bridgev1.AwaitSandboxExecutionRequest) (*bridgev1.AwaitSandboxExecutionResponse, error) {
	return s.store.AwaitSandboxExecution(ctx, request)
}

func (s *sandboxProductionBoundaryBridgeServer) SettleToolResult(ctx context.Context, request *bridgev1.SettleToolResultRequest) (*bridgev1.SettleToolResultResponse, error) {
	response, err := s.store.SettleToolResult(ctx, request)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.settlementACKDropped {
		s.settlementACKDropped = true
		return nil, status.Error(codes.Unavailable, "Tool settlement acknowledgement unavailable")
	}
	return response, nil
}

func (s *sandboxProductionBoundaryBridgeServer) snapshot() (bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.acceptACKDropped, s.settlementACKDropped
}

func startSandboxProductionBoundaryBridgeClient(t *testing.T, server bridgev1.AgentRuntimeBridgeServiceServer, runtimePodUID string) (bridgev1.AgentRuntimeBridgeServiceClient, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for Sandbox production Bridge: %v", err)
	}
	grpcServer, err := internalgrpc.NewServer(internalgrpc.Config{
		ServiceName:      "bridge-api-composition",
		Listener:         listener,
		Authenticator:    sandboxCompositionAuthenticator{runtimePodUID: runtimePodUID},
		MethodAuthorizer: BridgeAPIMethodAuthorizer,
		Register: func(serverInstance *grpc.Server) {
			bridgev1.RegisterAgentRuntimeBridgeServiceServer(serverInstance, server)
		},
	})
	if err != nil {
		t.Fatalf("construct authenticated Sandbox production Bridge: %v", err)
	}
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial Sandbox production Bridge: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return bridgev1.NewAgentRuntimeBridgeServiceClient(connection), listener.Addr().String()
}

type sandboxCompositionAuthenticator struct {
	runtimePodUID string
}

func (a sandboxCompositionAuthenticator) Authenticate(_ context.Context, token string) (internalgrpcauth.Identity, error) {
	identity := internalgrpcauth.Identity{
		ServiceAccount:   internalgrpcauth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"},
		KubernetesPodUID: a.runtimePodUID,
	}
	switch token {
	case "sandbox-production-runtime-token":
		return identity, nil
	case "sandbox-production-wrong-pod-token":
		identity.KubernetesPodUID = "pod_sandbox_production_wrong"
		return identity, nil
	default:
		return internalgrpcauth.Identity{}, status.Error(codes.Unauthenticated, "composition token rejected")
	}
}

type sandboxProductionBoundaryProvider struct {
	*bridgeMemoryProjectionProvider
	calls atomic.Int32
}

func (p *sandboxProductionBoundaryProvider) ExecuteTool(context.Context, tetralsandbox.ToolExecutionRequest) tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution] {
	p.calls.Add(1)
	return tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution]{Value: sandboxdriver.ToolExecution{
		ResultJSON: `{"status":"success","result":{"text":"production-boundary"}}`,
	}}
}

var _ tetralsandbox.ProviderAdapter = (*sandboxProductionBoundaryProvider)(nil)
