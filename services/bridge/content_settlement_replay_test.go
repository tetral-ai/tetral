package agentruntimebridge

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/sessionevent"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// This proves the authenticated RPC/SQL settlement boundary. Runtime's local
// work release and actual cancellation ownership have separate compositions;
// an error supplied here is an authoritative stored outcome, not a claim that
// an external command was cancelled by this test.
func TestPostgreSQLToolSettlementReplayTransactions(t *testing.T) {
	for _, variant := range []string{"precommit-rollback", "committed-response-loss", "duplicate-response", "unknown-target", "changed-completed-outcome", "late-success-after-error"} {
		t.Run(variant, func(t *testing.T) {
			f := newContentDeclarationFixture(t)
			// The public input owner requires the same runtime status fence that
			// normal Session creation installs alongside the seeded binding.
			if _, err := f.admin.ExecContext(f.ctx, `INSERT INTO session_runtime_status (workspace_id,session_id,status,binding_id,binding_generation,created_at,updated_at) VALUES ($1,$2,'running',$3,$4,now(),now())`, f.scope.WorkspaceId, f.scope.SessionId, f.scope.Binding.BindingId, f.scope.Binding.BindingGeneration); err != nil {
				t.Fatal(err)
			}
			f.start(t)
			tool, err := f.store.WriteEvent(f.ctx, &bridgev1.WriteEventRequest{Scope: f.scope, RuntimeWriteId: "settlement-tool", ModelRequestId: "request", ToolDeclaration: bridgeSignedReasoningToolDeclarationForTest("call-settle", "Read", `{"file_path":"/workspace/note.txt"}`, "ask")})
			if err != nil || tool.GetCommitted() == nil {
				t.Fatalf("actual Tool declaration=%v/%v", tool, err)
			}
			toolID := tool.GetCommitted().GetEventId()
			api := f.workload.OpenWorkload(t, "api", nil)
			born, err := sessionevent.NewService(sessionevent.NewPostgreSQLStore(dbconnect.NewClientForTesting(api))).AppendClientEvents(f.ctx, "default", f.scope.SessionId, "settlement-confirmation", sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{Type: sessionevent.EventTypeUserToolConfirmation, ToolUseID: toolID, Result: sessionevent.ToolConfirmationResultAllow}}})
			if err != nil || len(born.Data) != 1 {
				t.Fatalf("owning public confirmation service=%v/%v", born, err)
			}
			request := sessionfixture.BridgeToolSettlementRequestForTest(f.scope, sessionfixture.BridgeCompletedToolSettlementForTest(toolID, "fixture-result"))
			if variant == "late-success-after-error" {
				request.Settlement = sessionfixture.BridgeErrorToolSettlementForTest(toolID, "fixed terminal error")
			}
			fault := &contentSettlementResponseFault{BridgeAPIServer: NewBridgeAPIServer(f.store), lose: variant == "committed-response-loss"}
			client, _ := startSandboxProductionBoundaryBridgeClient(t, fault, f.scope.Binding.TargetPodUid)
			ctx := metadata.AppendToOutgoingContext(f.ctx, "authorization", "Bearer sandbox-production-runtime-token")
			other := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(f.workload.OpenWorkload(t, "bridge", nil)))
			second, _ := startSandboxProductionBoundaryBridgeClient(t, NewBridgeAPIServer(other), f.scope.Binding.TargetPodUid)
			before := f.snapshot(t)
			if variant == "unknown-target" {
				request.Settlement.ToolUseEventId = "evt_unknown_fixture_settlement"
				response, err := client.SettleToolResult(ctx, request)
				if status.Code(err) != codes.FailedPrecondition || response != nil {
					t.Fatalf("unknown actual settlement=%v/%v", response, err)
				}
				if f.snapshot(t) != before {
					t.Fatal("unknown settlement changed durable rows")
				}
				assertContentSettlementRoute(t, f, toolID, "resolving", 0)
				return
			}
			if variant == "precommit-rollback" {
				if _, err := f.admin.ExecContext(f.ctx, `CREATE FUNCTION content_fail_settlement() RETURNS trigger AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql;
				CREATE TRIGGER content_fail_settlement BEFORE UPDATE ON session_pending_tool_uses FOR EACH ROW EXECUTE FUNCTION content_fail_settlement()`); err != nil {
					t.Fatal(err)
				}
				if response, err := client.SettleToolResult(ctx, request); err == nil || response != nil {
					t.Fatalf("forced precommit settlement=%v/%v", response, err)
				}
				if f.snapshot(t) != before {
					t.Fatal("precommit failure leaked content/Event/change/receipt")
				}
				assertContentSettlementRoute(t, f, toolID, "resolving", 0)
				if _, err := f.admin.ExecContext(f.ctx, `DROP TRIGGER content_fail_settlement ON session_pending_tool_uses; DROP FUNCTION content_fail_settlement()`); err != nil {
					t.Fatal(err)
				}
			}
			response, err := client.SettleToolResult(ctx, request)
			if variant == "committed-response-loss" {
				if status.Code(err) != codes.Unavailable || response != nil || !fault.committed() {
					t.Fatalf("actual committed ACK loss=%v/%v", response, err)
				}
			} else if err != nil || response.GetCommitted() == nil {
				t.Fatalf("actual settlement commit=%v/%v", response, err)
			}
			assertContentSettlementRoute(t, f, toolID, "resolved", 1)
			committed := f.snapshot(t)
			for range 2 {
				duplicate, err := second.SettleToolResult(ctx, proto.Clone(request).(*bridgev1.SettleToolResultRequest))
				if err != nil || duplicate.GetDuplicate() == nil {
					t.Fatalf("actual second-pool replay=%v/%v", duplicate, err)
				}
			}
			if variant == "changed-completed-outcome" || variant == "late-success-after-error" {
				changed := proto.Clone(request).(*bridgev1.SettleToolResultRequest)
				changed.Settlement = sessionfixture.BridgeCompletedToolSettlementForTest(toolID, "late changed success")
				if response, err := second.SettleToolResult(ctx, changed); status.Code(err) != codes.AlreadyExists || response != nil {
					t.Fatalf("changed terminal settlement=%v/%v", response, err)
				}
			}
			if f.snapshot(t) != committed {
				t.Fatal("replay or conflicting terminal outcome changed exact durable rows")
			}
			var stored string
			if err := f.admin.QueryRowContext(f.ctx, `SELECT data_json FROM session_messages WHERE workspace_id='default' AND session_id=$1 AND kind='assistant'`, f.scope.SessionId).Scan(&stored); err != nil {
				t.Fatal(err)
			}
			var actual, literal any
			if json.Unmarshal([]byte(stored), &actual) != nil {
				t.Fatal("invalid stored Assistant")
			}
			want := `{"parts":[{"type":"reasoning","text":"provider-declared reasoning","providerMetadata":{"anthropic":{"signature":"sig_provider_context"}}},{"type":"tool_call","modelToolCallId":"call-settle","toolName":"Read","canonicalInput":{"file_path":"/workspace/note.txt"}},{"type":"tool_result","modelToolCallId":"call-settle","result":{"type":"completed","output":{"text":"fixture-result"}}}]}`
			if variant == "late-success-after-error" {
				want = `{"parts":[{"type":"reasoning","text":"provider-declared reasoning","providerMetadata":{"anthropic":{"signature":"sig_provider_context"}}},{"type":"tool_call","modelToolCallId":"call-settle","toolName":"Read","canonicalInput":{"file_path":"/workspace/note.txt"}},{"type":"tool_result","modelToolCallId":"call-settle","result":{"type":"error","error":{"type":"tool_error","message":"fixed terminal error"}}}]}`
			}
			if json.Unmarshal([]byte(want), &literal) != nil {
				t.Fatal("invalid independent literal")
			}
			a, _ := json.Marshal(actual)
			b, _ := json.Marshal(literal)
			if string(a) != string(b) {
				t.Fatalf("actual settlement Assistant differs from literal: %s", stored)
			}
		})
	}
}

type contentSettlementResponseFault struct {
	BridgeAPIServer
	mu              sync.Mutex
	lose, didCommit bool
}

func (s *contentSettlementResponseFault) SettleToolResult(ctx context.Context, request *bridgev1.SettleToolResultRequest) (*bridgev1.SettleToolResultResponse, error) {
	response, err := s.BridgeAPIServer.SettleToolResult(ctx, request)
	if err != nil || response.GetCommitted() == nil {
		return response, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.didCommit = true
	if s.lose {
		s.lose = false
		return nil, status.Error(codes.Unavailable, "settlement response unavailable")
	}
	return response, nil
}

func (s *contentSettlementResponseFault) committed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.didCommit
}

func assertContentSettlementRoute(t *testing.T, f contentDeclarationFixture, toolID, wantStatus string, wantResults int) {
	t.Helper()
	var statusValue string
	var results int
	if err := f.admin.QueryRowContext(f.ctx, `SELECT status,(SELECT count(*) FROM session_events e WHERE e.workspace_id=p.workspace_id AND e.session_id=p.session_id AND e.type='agent.tool_result' AND e.payload_json::jsonb->>'tool_use_id'=p.tool_use_event_id) FROM session_pending_tool_uses p WHERE workspace_id='default' AND session_id=$1 AND tool_use_event_id=$2`, f.scope.SessionId, toolID).Scan(&statusValue, &results); err != nil {
		t.Fatal(err)
	}
	if statusValue != wantStatus || results != wantResults {
		t.Fatalf("settlement route/results=%s/%d want%s/%d", statusValue, results, wantStatus, wantResults)
	}
}
