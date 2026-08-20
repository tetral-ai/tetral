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
	"reflect"
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
	"google.golang.org/protobuf/proto"
)

func TestPostgreSQLSandboxProductionBoundaryLostACKAndLeaseTakeover(t *testing.T) {
	tests := []struct {
		name           string
		toolName       string
		providerInput  any
		executionInput any
	}{
		{
			name:           "freeform apply_patch",
			toolName:       "apply_patch",
			providerInput:  "*** Begin Patch\n*** Add File: note.txt\n+hello\n*** End Patch\n",
			executionInput: map[string]any{"patch": "*** Begin Patch\n*** Add File: note.txt\n+hello\n*** End Patch\n"},
		},
		{
			name:           "ordinary object exec_command",
			toolName:       "exec_command",
			providerInput:  map[string]any{"cmd": "printf object"},
			executionInput: map[string]any{"cmd": "printf object"},
		},
		{
			name:     "nested provider metadata remains ordinary input",
			toolName: "exec_command",
			providerInput: map[string]any{
				"cmd": "printf nested",
				"env": map[string]any{"provider_metadata": "kept"},
			},
			executionInput: map[string]any{
				"cmd": "printf nested",
				"env": map[string]any{"provider_metadata": "kept"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testPostgreSQLSandboxProductionBoundaryLostACKAndLeaseTakeover(
				t, test.toolName, test.providerInput, test.executionInput,
			)
		})
	}
}

func testPostgreSQLSandboxProductionBoundaryLostACKAndLeaseTakeover(
	t *testing.T,
	toolName string,
	providerInput any,
	executionInput any,
) {
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
	store.RuntimeBindingTokenHMACKey = []byte("sandbox-production-boundary-key")
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
	providerInputJSON, err := json.Marshal(providerInput)
	if err != nil {
		t.Fatalf("marshal provider patch input: %v", err)
	}
	executionInputJSON, err := json.Marshal(executionInput)
	if err != nil {
		t.Fatalf("marshal canonical patch execution input: %v", err)
	}
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
		"bindingGeneration": 1, "targetPodUid": podUID,
		"modelRequestId": modelRequestID, "modelToolCallId": modelToolCallID,
		"toolName": toolName, "providerInput": json.RawMessage(providerInputJSON),
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
	provider := &sandboxProductionBoundaryProvider{
		bridgeMemoryProjectionProvider: &bridgeMemoryProjectionProvider{},
		expectedToolName:               toolName,
		expectedInputJSON:              string(executionInputJSON),
	}
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
		ToolUseEventID          string `json:"toolUseEventId"`
		CanonicalExecutionInput any    `json:"canonicalExecutionInput"`
		Result                  struct {
			Type string `json:"type"`
		} `json:"result"`
		Settlement struct {
			Type string `json:"type"`
		} `json:"settlement"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &runtimeResult); err != nil || runtimeResult.ToolUseEventID == "" ||
		runtimeResult.Result.Type != "completed" || runtimeResult.Settlement.Type != "duplicate" ||
		!reflect.DeepEqual(runtimeResult.CanonicalExecutionInput, executionInput) {
		t.Fatalf("Runtime Sandbox result = %q/%v; want completed/duplicate", stdout.String(), err)
	}
	toolUseEventID := runtimeResult.ToolUseEventID

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
	var payloadJSON string
	var projectionJSON string
	if err := admin.QueryRowContext(context.Background(), `SELECT payload_json, projection_json
		FROM session_events WHERE workspace_id=$1 AND session_id=$2 AND event_id=$3`,
		workspaceID, sessionID, toolUseEventID).Scan(&payloadJSON, &projectionJSON); err != nil {
		t.Fatalf("read durable Tool declaration: %v", err)
	}
	if testJSONPathString(t, payloadJSON, "name") != toolName ||
		!reflect.DeepEqual(testJSONPathValue(t, payloadJSON, "input"), executionInput) ||
		testJSONPathString(t, projectionJSON, "tool_name") != toolName ||
		!reflect.DeepEqual(testJSONPathValue(t, projectionJSON, "provider_input"), providerInput) ||
		!reflect.DeepEqual(testJSONPathValue(t, projectionJSON, "canonical_execution_input"), executionInput) {
		t.Fatalf("durable Tool declaration payload/projection = %s / %s", payloadJSON, projectionJSON)
	}
	loaded, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope})
	if err != nil {
		t.Fatalf("LoadContext after Sandbox composition: %v", err)
	}
	var contextPayload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(loaded.GetContextJson()), &contextPayload); err != nil {
		t.Fatalf("decode Sandbox composition context: %v", err)
	}
	providerInputFound := false
	contextEntries := contextPayload.ContextEntries
	if contextPayload.OpenRequestDraft != nil {
		contextEntries = append(contextEntries, bridgeRuntimeContextEntry{Parts: contextPayload.OpenRequestDraft.Parts})
	}
	for _, entry := range contextEntries {
		for _, rawPart := range entry.Parts {
			var part map[string]any
			if json.Unmarshal(rawPart, &part) == nil && part["type"] == "tool_call" &&
				part["modelToolCallId"] == modelToolCallID && reflect.DeepEqual(part["canonicalInput"], providerInput) {
				providerInputFound = true
			}
		}
	}
	writeDropped, acceptDropped, settlementDropped := bridge.snapshot()
	declarations, receipts := bridge.toolDeclarationTrace()
	if len(declarations) != 2 || len(receipts) != 2 || !proto.Equal(declarations[0], declarations[1]) ||
		declarations[0].GetToolDeclaration() == nil || declarations[0].GetEventType() != "" ||
		declarations[0].GetPayloadJson() != "" || declarations[0].GetAssistantContextDelta() != nil ||
		receipts[0].GetCommitted() == nil || receipts[1].GetDuplicate() == nil ||
		receipts[0].GetCommitted().GetEventId() != receipts[1].GetDuplicate().GetEventId() ||
		receipts[0].GetCommitted().AssignedMessageSequence == nil || receipts[1].GetDuplicate().AssignedMessageSequence == nil {
		t.Fatalf("Tool declaration trace = requests %#v receipts %#v; want exact replay and minimal durable receipt", declarations, receipts)
	}
	if (toolName == "apply_patch") != (declarations[0].GetToolDeclaration().DistinctProviderInputJson != nil) {
		t.Fatalf("distinct Provider input presence for %q = %v", toolName, declarations[0].GetToolDeclaration().DistinctProviderInputJson != nil)
	}
	if executionState != "consumed" || retainedResult.Valid || resultEvents != 1 || queueStatus != "acknowledged" ||
		provider.prepareCalls.Load() != 1 || provider.prepareMismatches.Load() != 0 ||
		provider.calls.Load() != 1 || provider.mismatches.Load() != 0 || !providerInputFound ||
		!writeDropped || !acceptDropped || !settlementDropped {
		t.Fatalf("production boundary = state %q retained %v events %d queue %q prepares %d/%d calls %d/%d provider_input %v dropped %v/%v/%v; want consumed/NULL/1/acknowledged/1/0/1/0/true/true/true/true",
			executionState, retainedResult, resultEvents, queueStatus,
			provider.prepareCalls.Load(), provider.prepareMismatches.Load(), provider.calls.Load(), provider.mismatches.Load(),
			providerInputFound, writeDropped, acceptDropped, settlementDropped)
	}
}

type sandboxProductionBoundaryBridgeServer struct {
	bridgev1.UnimplementedAgentRuntimeBridgeServiceServer
	store *PostgreSQLBridgeAPIStore

	mu                   sync.Mutex
	writeACKDropped      bool
	acceptACKDropped     bool
	settlementACKDropped bool
	toolDeclarations     []*bridgev1.WriteEventRequest
	toolReceipts         []*bridgev1.WriteEventResponse
}

func (s *sandboxProductionBoundaryBridgeServer) WriteEvent(ctx context.Context, request *bridgev1.WriteEventRequest) (*bridgev1.WriteEventResponse, error) {
	response, err := s.store.WriteEvent(ctx, request)
	if err != nil || request.GetToolDeclaration() == nil {
		return response, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.toolDeclarations = append(s.toolDeclarations, proto.Clone(request).(*bridgev1.WriteEventRequest))
	s.toolReceipts = append(s.toolReceipts, proto.Clone(response).(*bridgev1.WriteEventResponse))
	if !s.writeACKDropped {
		s.writeACKDropped = true
		return nil, status.Error(codes.Unavailable, "Tool declaration acknowledgement unavailable")
	}
	return response, nil
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

func (s *sandboxProductionBoundaryBridgeServer) snapshot() (bool, bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeACKDropped, s.acceptACKDropped, s.settlementACKDropped
}

func (s *sandboxProductionBoundaryBridgeServer) toolDeclarationTrace() ([]*bridgev1.WriteEventRequest, []*bridgev1.WriteEventResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*bridgev1.WriteEventRequest(nil), s.toolDeclarations...), append([]*bridgev1.WriteEventResponse(nil), s.toolReceipts...)
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
	prepareCalls      atomic.Int32
	prepareMismatches atomic.Int32
	calls             atomic.Int32
	mismatches        atomic.Int32
	expectedToolName  string
	expectedInputJSON string
}

func (p *sandboxProductionBoundaryProvider) PrepareTool(_ context.Context, request tetralsandbox.ToolExecutionRequest) tetralsandbox.ProviderOutcome[tetralsandbox.ToolPreparationResult] {
	p.prepareCalls.Add(1)
	if request.Invocation.ToolName != p.expectedToolName || request.Invocation.InputJSON != p.expectedInputJSON {
		p.prepareMismatches.Add(1)
	}
	return tetralsandbox.ProviderOutcome[tetralsandbox.ToolPreparationResult]{}
}

func (p *sandboxProductionBoundaryProvider) ExecuteTool(_ context.Context, request tetralsandbox.ToolExecutionRequest) tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution] {
	p.calls.Add(1)
	if request.Invocation.ToolName != p.expectedToolName || request.Invocation.InputJSON != p.expectedInputJSON {
		p.mismatches.Add(1)
	}
	return tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution]{Value: sandboxdriver.ToolExecution{
		ResultJSON: `{"status":"success","result":{"text":"production-boundary"}}`,
	}}
}

var _ tetralsandbox.ProviderAdapter = (*sandboxProductionBoundaryProvider)(nil)
