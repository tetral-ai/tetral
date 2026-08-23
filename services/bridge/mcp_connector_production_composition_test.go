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
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/internalgrpc"
	internalgrpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestPostgreSQLMCPConnectorExecutionLostACKAndLeaseTakeover(t *testing.T) {
	bunPath, err := exec.LookPath("bun")
	if err != nil {
		t.Fatalf("production MCP Connector composition requires bun: %v", err)
	}
	gatewayRoot := filepath.Clean(filepath.Join("..", "gateway"))
	if _, err := os.Stat(filepath.Join(gatewayRoot, "node_modules", "@grpc", "grpc-js")); err != nil {
		t.Fatalf("production MCP Connector composition requires installed Gateway dependencies: %v", err)
	}

	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_mcp_production_composition"
		threadID  = "thr_mcp_production_composition"
		bindingID = "bind_mcp_production_composition"
		podUID    = "pod_mcp_production_composition"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("mcp-production-context-signing-key")
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	toolUseEventID := writeDurableMCPToolUseForTest(t, store, scope)
	cancelledToolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_mcp_production_cancelled_use", ModelRequestId: "mreq_mcp_durable_claim",
		ToolDeclaration: bridgeMCPToolDeclarationForTest(
			"call_mcp_production_cancelled", "create_issue", "github", `{"mode":"cancel-between"}`, "allow",
		),
	})
	if err != nil || cancelledToolUse.GetCommitted() == nil || cancelledToolUse.GetCommitted().GetEventId() == "" {
		t.Fatalf("write cancellation-between-claims MCP Tool use = %#v/%v", cancelledToolUse, err)
	}
	cancelledToolUseEventID := cancelledToolUse.GetCommitted().GetEventId()
	oauthSuccessToolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_mcp_oauth_success_use", ModelRequestId: "mreq_mcp_durable_claim",
		ToolDeclaration: bridgeMCPToolDeclarationForTest(
			"call_mcp_oauth_success", "create_issue", "github", `{"mode":"oauth-success"}`, "allow",
		),
	})
	if err != nil || oauthSuccessToolUse.GetCommitted() == nil {
		t.Fatalf("write OAuth-success MCP Tool use = %#v/%v", oauthSuccessToolUse, err)
	}
	oauthFailureToolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_mcp_oauth_failure_use", ModelRequestId: "mreq_mcp_durable_claim",
		ToolDeclaration: bridgeMCPToolDeclarationForTest(
			"call_mcp_oauth_failure", "create_issue", "github", `{"mode":"oauth-failure"}`, "allow",
		),
	})
	if err != nil || oauthFailureToolUse.GetCommitted() == nil {
		t.Fatalf("write OAuth-failure MCP Tool use = %#v/%v", oauthFailureToolUse, err)
	}
	oauthSuccessToolUseEventID := oauthSuccessToolUse.GetCommitted().GetEventId()
	oauthFailureToolUseEventID := oauthFailureToolUse.GetCommitted().GetEventId()
	cleanupToolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_mcp_production_cleanup_use", ModelRequestId: "mreq_mcp_durable_claim",
		ToolDeclaration: bridgeMCPToolDeclarationForTest(
			"call_mcp_production_cleanup", "create_issue", "github", `{"mode":"cleanup"}`, "allow",
		),
	})
	if err != nil || cleanupToolUse.GetCommitted() == nil || cleanupToolUse.GetCommitted().GetEventId() == "" {
		t.Fatalf("write cleanup MCP Tool use = %#v/%v", cleanupToolUse, err)
	}
	cleanupToolUseEventID := cleanupToolUse.GetCommitted().GetEventId()
	if _, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_mcp_production_end", ModelRequestId: "mreq_mcp_durable_claim",
		FinishReason: "tool-calls", UsageJson: `{}`,
		ProviderContextRetention: &bridgev1.ProviderContextRetention{
			Disposition:              "completed",
			AssistantMessageSequence: cleanupToolUse.GetCommitted().AssignedMessageSequence,
			ToolUseEventIds: []string{
				toolUseEventID, cancelledToolUseEventID, oauthSuccessToolUseEventID,
				oauthFailureToolUseEventID, cleanupToolUseEventID,
			},
		},
	}); err != nil {
		t.Fatalf("seal MCP production request: %v", err)
	}

	bridge := &mcpConnectorProductionBridgeServer{
		store:                   store,
		cancelledToolUseEventID: cancelledToolUseEventID,
		now:                     time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC),
	}
	store.Clock = bridge.clock
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for MCP Connector composition: %v", err)
	}
	grpcServer, err := internalgrpc.NewServer(internalgrpc.Config{
		ServiceName:      "bridge-api-mcp-composition",
		Listener:         listener,
		Authenticator:    mcpCompositionBridgeAuthenticator{runtimePodUID: podUID},
		MethodAuthorizer: BridgeAPIMethodAuthorizer,
		Register: func(serverInstance *grpc.Server) {
			bridgev1.RegisterAgentRuntimeBridgeServiceServer(serverInstance, bridge)
		},
	})
	if err != nil {
		t.Fatalf("construct authenticated MCP production Bridge: %v", err)
	}
	go func() { _ = grpcServer.Serve(listener) }()
	defer grpcServer.Stop()

	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial authenticated MCP production Bridge: %v", err)
	}
	defer func() { _ = connection.Close() }()
	bridgeClient := bridgev1.NewAgentRuntimeBridgeServiceClient(connection)
	if _, err := bridgeClient.ClaimMcpToolResult(context.Background(), &bridgev1.ClaimMcpToolResultRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unauthenticated MCP production Bridge request error = %v; want Unauthenticated", err)
	}
	wrongPodContext := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer mcp-production-wrong-runtime-token")
	wrongPodSettlement, err := bridgeClient.SettleToolResult(wrongPodContext, &bridgev1.SettleToolResultRequest{
		Scope:      scope,
		Settlement: bridgeCompletedToolSettlementForTest(toolUseEventID, "wrong pod must not settle"),
	})
	if err != nil || wrongPodSettlement.GetStale() == nil {
		t.Fatalf("wrong-pod MCP production settlement = %+v, %v; want stale without mutation", wrongPodSettlement, err)
	}

	tokenDir := t.TempDir()
	gatewayTokenPath := filepath.Join(tokenDir, "gateway-token")
	runtimeTokenPath := filepath.Join(tokenDir, "runtime-token")
	if err := os.WriteFile(gatewayTokenPath, []byte("mcp-production-gateway-token\n"), 0o600); err != nil {
		t.Fatalf("write Gateway composition token: %v", err)
	}
	if err := os.WriteFile(runtimeTokenPath, []byte("mcp-production-runtime-token\n"), 0o600); err != nil {
		t.Fatalf("write Runtime composition token: %v", err)
	}
	var databaseSchema string
	if err := admin.QueryRowContext(context.Background(), `SELECT current_schema()`).Scan(&databaseSchema); err != nil {
		t.Fatalf("read MCP composition database schema: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(
		ctx,
		bunPath,
		"run",
		"packages/mcp-connector/test/fixtures/tool-claim-production-composition.ts",
		listener.Addr().String(),
		gatewayTokenPath,
		runtimeTokenPath,
		toolUseEventID,
		cleanupToolUseEventID,
		cancelledToolUseEventID,
		oauthSuccessToolUseEventID,
		oauthFailureToolUseEventID,
	) //nolint:gosec // fixed repository fixture and test-owned arguments.
	command.Dir = gatewayRoot
	command.Env = append(os.Environ(), "TETRAL_TEST_DATABASE_SCHEMA="+databaseSchema)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("run MCP Connector production composition: %v\nstdout=%s\nstderr=%s", err, stdout.String(), stderr.String())
	}
	var result struct {
		ExecutionCalls []string `json:"executionCalls"`
		Takeover       struct {
			Status     int32  `json:"status"`
			ResultText string `json:"resultText"`
			ErrorKind  int32  `json:"errorKind"`
		} `json:"takeover"`
		StaleFirst struct {
			Status     int32  `json:"status"`
			ResultText string `json:"resultText"`
			ErrorKind  int32  `json:"errorKind"`
		} `json:"staleFirst"`
		CleanupFailureCode int32 `json:"cleanupFailureCode"`
		CleanupReacquired  struct {
			Status     int32  `json:"status"`
			ResultText string `json:"resultText"`
		} `json:"cleanupReacquired"`
		CancelledTakeover struct {
			Status    int32 `json:"status"`
			ErrorKind int32 `json:"errorKind"`
		} `json:"cancelledTakeover"`
		CancelledFirst struct {
			Status     int32  `json:"status"`
			ResultText string `json:"resultText"`
			ErrorKind  int32  `json:"errorKind"`
		} `json:"cancelledFirst"`
		RuntimeResult struct {
			Type   string `json:"type"`
			Output struct {
				Text string `json:"text"`
			} `json:"output"`
		} `json:"runtimeResult"`
		Settlement struct {
			Type string `json:"type"`
		} `json:"settlement"`
		OAuthSuccess struct {
			Type   string `json:"type"`
			Output struct {
				Text string `json:"text"`
			} `json:"output"`
		} `json:"oauthSuccess"`
		OAuthFailure struct {
			Type  string `json:"type"`
			Error struct {
				Message   string `json:"message"`
				Retryable bool   `json:"retryable"`
			} `json:"error"`
		} `json:"oauthFailure"`
		OAuthProof struct {
			IssuerRequests                   int      `json:"issuerRequests"`
			SuccessTransportCount            int      `json:"successTransportCount"`
			FailureTransportCount            int      `json:"failureTransportCount"`
			DurableRotation                  bool     `json:"durableRotation"`
			FailedRefreshPreservedCredential bool     `json:"failedRefreshPreservedCredential"`
			RefreshOutcomes                  []string `json:"refreshOutcomes"`
			LeakSurfacesClean                bool     `json:"leakSurfacesClean"`
		} `json:"oauthProof"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("decode MCP Connector production composition: %v\nstdout=%s", err, stdout.String())
	}
	if len(result.ExecutionCalls) != 5 ||
		result.Takeover.Status != 1 || result.Takeover.ResultText != "takeover claimant" ||
		result.StaleFirst.Status != 3 || result.StaleFirst.ErrorKind != 11 ||
		result.StaleFirst.ResultText != "MCP tool execution lost runtime custody." ||
		result.CancelledTakeover.Status != 3 || result.CancelledTakeover.ErrorKind != 11 ||
		result.CancelledFirst.Status != 1 || result.CancelledFirst.ResultText != "cancelled claimant" ||
		result.CleanupFailureCode != int32(codes.Internal) ||
		result.CleanupReacquired.Status != 1 || result.CleanupReacquired.ResultText != "reacquired claimant" ||
		result.RuntimeResult.Type != "completed" || result.RuntimeResult.Output.Text != "takeover claimant" ||
		result.Settlement.Type != "duplicate" ||
		result.OAuthSuccess.Type != "completed" || result.OAuthSuccess.Output.Text != "oauth refreshed" ||
		result.OAuthFailure.Type != "error" || result.OAuthFailure.Error.Retryable ||
		result.OAuthFailure.Error.Message != "MCP authorization is unavailable. Reconnect the integration and try again." ||
		result.OAuthProof.IssuerRequests != 2 || result.OAuthProof.SuccessTransportCount != 1 ||
		result.OAuthProof.FailureTransportCount != 0 || !result.OAuthProof.DurableRotation ||
		!result.OAuthProof.FailedRefreshPreservedCredential || !result.OAuthProof.LeakSurfacesClean ||
		len(result.OAuthProof.RefreshOutcomes) != 2 || result.OAuthProof.RefreshOutcomes[0] != "refreshed" ||
		result.OAuthProof.RefreshOutcomes[1] != "failed" {
		t.Fatalf("MCP Connector composition = %+v; want takeover, exact-claim cleanup/reacquisition, Runtime mapping, and settlement replay", result)
	}

	claims, commits, relinquishes, commitDropped, relinquishDropped, settlementDropped := bridge.snapshot()
	if len(claims) != 9 || claims[0] != "claim_mcp_production_old" ||
		claims[1] != "claim_mcp_production_takeover" ||
		claims[2] != "claim_mcp_production_runtime_replay" ||
		claims[3] != "claim_mcp_production_cancel_old" ||
		claims[4] != "claim_mcp_production_cancel_takeover" ||
		claims[5] != "claim_mcp_production_cleanup_failed" ||
		claims[6] != "claim_mcp_production_cleanup_reacquired" ||
		claims[7] != "claim_mcp_oauth_success" || claims[8] != "claim_mcp_oauth_failure" {
		t.Fatalf("Bridge claim sequence = %v; want receipt convergence and immediate cleanup reacquisition", claims)
	}
	if len(commits) != 7 || commits[0] != "claim_mcp_production_takeover" ||
		commits[1] != "claim_mcp_production_takeover" || commits[2] != "claim_mcp_production_old" ||
		commits[3] != "claim_mcp_production_cancel_old" || commits[4] != "claim_mcp_production_cleanup_reacquired" ||
		commits[5] != "claim_mcp_oauth_success" || commits[6] != "claim_mcp_oauth_failure" ||
		len(relinquishes) != 3 || relinquishes[0] != "claim_mcp_production_old" ||
		relinquishes[1] != "claim_mcp_production_cleanup_failed" ||
		relinquishes[2] != "claim_mcp_production_cleanup_failed" ||
		!commitDropped || !relinquishDropped || !settlementDropped {
		t.Fatalf("Bridge commits/relinquishes/dropped ACKs = %v/%v/%t/%t/%t", commits, relinquishes, commitDropped, relinquishDropped, settlementDropped)
	}

	var claimStatus, storedResult string
	var claimID, claimExpiry sql.NullString
	if err := admin.QueryRowContext(context.Background(), `SELECT mcp_claim_status, result_json, mcp_claim_id, mcp_claim_lease_expires_at
		FROM session_runtime_tool_results
		WHERE workspace_id='default' AND session_id=$1 AND tool_use_event_id=$2`, sessionID, toolUseEventID).
		Scan(&claimStatus, &storedResult, &claimID, &claimExpiry); err != nil {
		t.Fatalf("read production MCP durable result: %v", err)
	}
	if claimStatus != "consumed" || claimID.Valid || claimExpiry.Valid || !bytes.Contains([]byte(storedResult), []byte("takeover claimant")) {
		t.Fatalf("durable MCP result = status %q result %q claim %+v expiry %+v", claimStatus, storedResult, claimID, claimExpiry)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT mcp_claim_status, result_json, mcp_claim_id, mcp_claim_lease_expires_at
		FROM session_runtime_tool_results
		WHERE workspace_id='default' AND session_id=$1 AND tool_use_event_id=$2`, sessionID, cleanupToolUseEventID).
		Scan(&claimStatus, &storedResult, &claimID, &claimExpiry); err != nil {
		t.Fatalf("read reacquired MCP durable result: %v", err)
	}
	if claimStatus != "stored" || claimID.Valid || claimExpiry.Valid || !bytes.Contains([]byte(storedResult), []byte("reacquired claimant")) {
		t.Fatalf("reacquired MCP result = status %q result %q claim %+v expiry %+v", claimStatus, storedResult, claimID, claimExpiry)
	}

	loaded, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope})
	if err != nil {
		t.Fatalf("cold-load MCP production settlement: %v", err)
	}
	var payload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(loaded.GetContextJson()), &payload); err != nil {
		t.Fatalf("decode MCP production cold context: %v", err)
	}
	var mainCalls, mainResults, cleanupCalls, cleanupResults int
	for _, entry := range payload.ContextEntries {
		for _, raw := range entry.Parts {
			var part struct {
				Type            string `json:"type"`
				ModelToolCallID string `json:"modelToolCallId"`
			}
			if err := json.Unmarshal(raw, &part); err != nil {
				t.Fatalf("decode MCP production context part: %v", err)
			}
			switch part.ModelToolCallID {
			case "call_mcp_durable_claim":
				switch part.Type {
				case "tool_call":
					mainCalls++
				case "tool_result":
					mainResults++
				}
			case "call_mcp_production_cleanup":
				switch part.Type {
				case "tool_call":
					cleanupCalls++
				case "tool_result":
					cleanupResults++
				}
			}
		}
	}
	if mainCalls != 1 || mainResults != 1 || cleanupCalls != 1 || cleanupResults != 0 {
		t.Fatalf("MCP cold context calls/results main=%d/%d cleanup=%d/%d; want one settled pair and one unresolved call", mainCalls, mainResults, cleanupCalls, cleanupResults)
	}
	cleanupSettlement, err := store.SettleToolResult(context.Background(), bridgeToolSettlementRequestForTest(
		scope,
		bridgeCompletedToolSettlementForTest(cleanupToolUseEventID, "reacquired claimant"),
	))
	if err != nil {
		t.Fatalf("settle staged cleanup MCP result: %v", err)
	}
	bridgeRequireToolSettlementOutcomeForTest(t, cleanupSettlement, "committed")
	finalLoaded, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope})
	if err != nil {
		t.Fatalf("cold-load fully settled MCP production context: %v", err)
	}
	ready := runRuntimeColdContextComposition(t, finalLoaded.GetContextJson(), true)
	if ready.ReducerAction.Action != "prepare_next_request" {
		t.Fatalf("settled MCP Runtime action = %+v; want exactly one next Provider request authority", ready.ReducerAction)
	}
	assertNoInventedAssistantText(t, ready.ProviderComposition)
	assertProviderCompositionToolOrder(t, ready.ProviderComposition, []string{
		"call_mcp_durable_claim",
		"call_mcp_production_cancelled",
		"call_mcp_oauth_success",
		"call_mcp_oauth_failure",
		"call_mcp_production_cleanup",
	})
	providerJSON, err := json.Marshal(ready.ProviderComposition)
	if err != nil {
		t.Fatalf("encode MCP OAuth provider composition: %v", err)
	}
	for _, required := range []string{"oauth refreshed", "MCP authorization is unavailable. Reconnect the integration and try again."} {
		if !bytes.Contains(providerJSON, []byte(required)) {
			t.Fatalf("MCP OAuth provider composition omitted %q: %s", required, providerJSON)
		}
	}
	var durablePublicSurfaces string
	if err := admin.QueryRowContext(context.Background(), `
		SELECT COALESCE(string_agg(surface, ''), '')
		FROM (
			SELECT payload_json AS surface FROM session_events WHERE workspace_id='default' AND session_id=$1
			UNION ALL
			SELECT data_json AS surface FROM session_messages WHERE workspace_id='default' AND session_id=$1
			UNION ALL
			SELECT result_json AS surface FROM session_runtime_tool_results WHERE workspace_id='default' AND session_id=$1
		) surfaces`, sessionID).Scan(&durablePublicSurfaces); err != nil {
		t.Fatalf("read MCP OAuth durable public surfaces: %v", err)
	}
	publicSurfaces := bytes.Join([][]byte{stdout.Bytes(), []byte(loaded.GetContextJson()), []byte(finalLoaded.GetContextJson()), providerJSON, []byte(durablePublicSurfaces)}, nil)
	for _, forbidden := range []string{"ACCESS_TOKEN_CANARY", "REFRESH_TOKEN_CANARY", "RAW_ISSUER_ERROR_CANARY"} {
		if bytes.Contains(publicSurfaces, []byte(forbidden)) {
			t.Fatalf("MCP OAuth public surfaces exposed %q", forbidden)
		}
	}
}

type mcpConnectorProductionBridgeServer struct {
	bridgev1.UnimplementedAgentRuntimeBridgeServiceServer
	store                   *PostgreSQLBridgeAPIStore
	cancelledToolUseEventID string

	mu                   sync.Mutex
	now                  time.Time
	claims               []string
	commits              []string
	relinquishes         []string
	droppedACK           bool
	relinquishACKDropped bool
	settlementACKDropped bool
}

func (s *mcpConnectorProductionBridgeServer) clock() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.now
}

func (s *mcpConnectorProductionBridgeServer) ClaimMcpToolResult(ctx context.Context, request *bridgev1.ClaimMcpToolResultRequest) (*bridgev1.ClaimMcpToolResultResponse, error) {
	s.mu.Lock()
	s.claims = append(s.claims, request.GetClaimId())
	if request.GetClaimId() == "claim_mcp_production_takeover" {
		s.now = time.Date(2026, 1, 1, 0, 4, 0, 0, time.UTC)
	}
	if request.GetClaimId() == "claim_mcp_production_cancel_takeover" {
		s.now = time.Date(2026, 1, 1, 0, 8, 0, 0, time.UTC)
	}
	s.mu.Unlock()
	if request.GetClaimId() == "claim_mcp_production_cancel_takeover" {
		response, err := s.store.SettleToolResult(ctx, bridgeToolSettlementRequestForTest(request.GetScope(), &bridgev1.RuntimeToolSettlement{
			ToolUseEventId: s.cancelledToolUseEventID,
			Outcome:        &bridgev1.RuntimeToolSettlement_Cancelled{Cancelled: &bridgev1.RuntimeToolCancelled{}},
		}))
		if err != nil || response.GetCommitted() == nil {
			return nil, status.Error(codes.Internal, "could not settle the route before takeover")
		}
	}
	return s.store.ClaimMcpToolResult(ctx, request)
}

func (s *mcpConnectorProductionBridgeServer) CommitMcpToolResult(ctx context.Context, request *bridgev1.CommitMcpToolResultRequest) (*bridgev1.CommitMcpToolResultResponse, error) {
	s.mu.Lock()
	s.commits = append(s.commits, request.GetClaimId())
	s.mu.Unlock()
	response, err := s.store.CommitMcpToolResult(ctx, request)
	if err != nil {
		return nil, err
	}
	if request.GetClaimId() == "claim_mcp_production_takeover" {
		s.mu.Lock()
		if !s.droppedACK {
			s.droppedACK = true
			s.mu.Unlock()
			return nil, status.Error(codes.Unavailable, "MCP commit acknowledgement unavailable")
		}
		s.mu.Unlock()
	}
	return response, nil
}

func (s *mcpConnectorProductionBridgeServer) RelinquishMcpToolResult(ctx context.Context, request *bridgev1.RelinquishMcpToolResultRequest) (*bridgev1.RelinquishMcpToolResultResponse, error) {
	s.mu.Lock()
	s.relinquishes = append(s.relinquishes, request.GetClaimId())
	s.mu.Unlock()
	response, err := s.store.RelinquishMcpToolResult(ctx, request)
	if err != nil {
		return nil, err
	}
	if request.GetClaimId() == "claim_mcp_production_cleanup_failed" {
		s.mu.Lock()
		if !s.relinquishACKDropped {
			s.relinquishACKDropped = true
			s.mu.Unlock()
			return nil, status.Error(codes.Unavailable, "MCP relinquish acknowledgement unavailable")
		}
		s.mu.Unlock()
	}
	return response, nil
}

func (s *mcpConnectorProductionBridgeServer) SettleToolResult(ctx context.Context, request *bridgev1.SettleToolResultRequest) (*bridgev1.SettleToolResultResponse, error) {
	response, err := s.store.SettleToolResult(ctx, request)
	if err != nil {
		return nil, err
	}
	if response.GetCommitted() == nil {
		return response, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.settlementACKDropped {
		s.settlementACKDropped = true
		return nil, status.Error(codes.Unavailable, "MCP Tool settlement acknowledgement unavailable")
	}
	return response, nil
}

func (s *mcpConnectorProductionBridgeServer) snapshot() (claims []string, commits []string, relinquishes []string, commitDropped bool, relinquishDropped bool, settlementDropped bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.claims...), append([]string(nil), s.commits...), append([]string(nil), s.relinquishes...), s.droppedACK, s.relinquishACKDropped, s.settlementACKDropped
}

type mcpCompositionBridgeAuthenticator struct {
	runtimePodUID string
}

func (a mcpCompositionBridgeAuthenticator) Authenticate(_ context.Context, token string) (internalgrpcauth.Identity, error) {
	switch token {
	case "mcp-production-gateway-token":
		return internalgrpcauth.Identity{
			ServiceAccount: internalgrpcauth.ServiceAccount{Namespace: "tetral-system", Name: "gateway"},
		}, nil
	case "mcp-production-runtime-token":
		return internalgrpcauth.Identity{
			ServiceAccount:   internalgrpcauth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"},
			KubernetesPodUID: a.runtimePodUID,
		}, nil
	case "mcp-production-wrong-runtime-token":
		return internalgrpcauth.Identity{
			ServiceAccount:   internalgrpcauth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"},
			KubernetesPodUID: "pod_mcp_production_wrong",
		}, nil
	default:
		return internalgrpcauth.Identity{}, status.Error(codes.Unauthenticated, "composition token rejected")
	}
}
