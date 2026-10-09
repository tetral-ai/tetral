package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

type contentDeclarationFixture struct {
	admin    *sql.DB
	workload *storagetest.WorkloadDB
	store    *PostgreSQLBridgeAPIStore
	scope    *bridgev1.RuntimeScope
	ctx      context.Context
}

func newContentDeclarationFixture(t *testing.T) contentDeclarationFixture {
	t.Helper()
	if strings.TrimSpace(os.Getenv(storagetest.EnvTestDatabaseURL)) == "" {
		t.Fatal("content declaration requires its declared PostgreSQL dependency")
	}
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	workload := storagetest.OpenWorkloadDB(t, admin, "bridge")
	fixture := seedContentDeclarationScope(t, admin, workload.DB, "default")
	fixture.workload = workload
	return fixture
}

func seedContentDeclarationScope(t *testing.T, admin, runtimeDB *sql.DB, workspaceID string) contentDeclarationFixture {
	t.Helper()
	sessionID, threadID, bindingID, podUID := id.New("sesn_"), id.New("thr_"), id.New("bind_"), id.New("pod_")
	sessionfixture.SeedBridgeAPISession(t, admin, workspaceID, sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, workspaceID, sessionID, bindingID, 1, podUID)
	scope := sessionfixture.BridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	scope.WorkspaceId = workspaceID
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return contentDeclarationFixture{admin: admin, store: NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB)), scope: scope, ctx: ctx}
}

func (f contentDeclarationFixture) start(t *testing.T) {
	t.Helper()
	seedBridgeAPIRequestStart(t, f.store, f.scope, "start", "request", runtimecontrol.RequestKindAgentProviderRequest, 0)
}

func (f contentDeclarationFixture) request(eventType, writeID, eventID string) *bridgev1.WriteEventRequest {
	request := &bridgev1.WriteEventRequest{
		Scope: f.scope, RuntimeWriteId: writeID, ModelRequestId: "request", EventType: eventType,
		PayloadJson: `{"type":"agent.thinking"}`, PreallocatedEventId: &eventID,
	}
	if eventType == "agent.message" {
		request.PayloadJson = `{"type":"agent.message","content":[{"type":"text","text":"alpha"}]}`
		request.AssistantContextDelta = &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{
			{Content: &bridgev1.RuntimeContextPart_Reasoning{Reasoning: &bridgev1.RuntimeContextReasoning{Text: "reason-before-text", ProviderMetadataJson: bridgeString(`{"anthropic":{"signature":"fixture-signature-text"}}`)}}},
			{Content: &bridgev1.RuntimeContextPart_Text{Text: &bridgev1.RuntimeContextText{Text: "alpha"}}},
		}}
	}
	return request
}

func (f contentDeclarationFixture) snapshot(t *testing.T) string {
	t.Helper()
	var snapshot string
	err := f.admin.QueryRowContext(f.ctx, `SELECT jsonb_build_object(
	  'events', COALESCE((SELECT jsonb_agg(to_jsonb(e) ORDER BY sequence) FROM session_events e WHERE workspace_id=$1 AND session_id=$2), '[]'::jsonb),
	  'messages', COALESCE((SELECT jsonb_agg(to_jsonb(m) ORDER BY sequence) FROM session_messages m WHERE workspace_id=$1 AND session_id=$2), '[]'::jsonb),
	  'parts', COALESCE((SELECT jsonb_agg(to_jsonb(p) ORDER BY message_id, part_index) FROM session_message_parts p WHERE workspace_id=$1 AND session_id=$2), '[]'::jsonb),
	  'changes', COALESCE((SELECT jsonb_agg(to_jsonb(c) ORDER BY stream_position) FROM session_event_stream_changes c WHERE workspace_id=$1 AND session_id=$2), '[]'::jsonb),
	  'operations', COALESCE((SELECT jsonb_agg(to_jsonb(o) ORDER BY operation,source_kind,idempotency_key) FROM session_bridge_operations o WHERE workspace_id=$1 AND session_id=$2), '[]'::jsonb)
	)::text`, f.scope.WorkspaceId, f.scope.SessionId).Scan(&snapshot)
	if err != nil {
		t.Fatalf("read independent durable snapshot: %v", err)
	}
	return snapshot
}

func (f contentDeclarationFixture) end(t *testing.T, messageSequence *int64) {
	t.Helper()
	_, err := f.store.WriteRequestEnd(f.ctx, &bridgev1.WriteRequestEndRequest{
		Scope: f.scope, RuntimeWriteId: "end", ModelRequestId: "request", FinishReason: "stop", UsageJson: `{}`,
		ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "completed", AssistantMessageSequence: messageSequence},
	})
	if err != nil {
		t.Fatalf("commit request End: %v", err)
	}
}

func TestPostgreSQLContentIdentityConflicts(t *testing.T) {
	for _, variant := range []string{"same-key-changed-id", "same-key-changed-text", "same-key-changed-prefix", "new-key-same-id", "exact-replay", "append-validation-rollback"} {
		t.Run(variant, func(t *testing.T) {
			f := newContentDeclarationFixture(t)
			f.start(t)
			request := f.request("agent.message", "write", "evt_00000000000000000000000000000001")
			first, err := f.store.WriteEvent(f.ctx, request)
			if err != nil || first.GetCommitted().GetEventId() != request.GetPreallocatedEventId() {
				t.Fatalf("first commit = %v/%v", first, err)
			}
			before := f.snapshot(t)
			next := proto.Clone(request).(*bridgev1.WriteEventRequest)
			switch variant {
			case "same-key-changed-id":
				next.PreallocatedEventId = bridgeString("evt_00000000000000000000000000000002")
			case "same-key-changed-text":
				next.PayloadJson = `{"type":"agent.message","content":[{"type":"text","text":"beta"}]}`
				next.AssistantContextDelta.Parts[1].GetText().Text = "beta"
			case "same-key-changed-prefix":
				next.AssistantContextDelta.Parts[0].GetReasoning().Text = "changed-prefix"
			case "new-key-same-id":
				next.RuntimeWriteId = "other-write"
			case "append-validation-rollback":
				next.RuntimeWriteId = "over-reasoning-budget"
				next.PreallocatedEventId = bridgeString("evt_00000000000000000000000000000002")
				next.AssistantContextDelta.Parts = nil
				for range 17 {
					next.AssistantContextDelta.Parts = append(next.AssistantContextDelta.Parts, &bridgev1.RuntimeContextPart{Content: &bridgev1.RuntimeContextPart_Reasoning{Reasoning: &bridgev1.RuntimeContextReasoning{Text: "extra"}}})
				}
				next.AssistantContextDelta.Parts = append(next.AssistantContextDelta.Parts, &bridgev1.RuntimeContextPart{Content: &bridgev1.RuntimeContextPart_Text{Text: &bridgev1.RuntimeContextText{Text: "alpha"}}})
			}
			response, err := f.store.WriteEvent(f.ctx, next)
			switch variant {
			case "exact-replay":
				if err != nil || response.GetDuplicate().GetEventId() != first.GetCommitted().GetEventId() || response.GetDuplicate().GetAssignedMessageSequence() != first.GetCommitted().GetAssignedMessageSequence() {
					t.Fatalf("replay = %v/%v", response, err)
				}
			case "append-validation-rollback":
				if status.Code(err) != codes.InvalidArgument {
					t.Fatalf("reasoning overage = %v; want InvalidArgument", err)
				}
			default:
				if status.Code(err) != codes.AlreadyExists {
					t.Fatalf("identity conflict = %v; want AlreadyExists", err)
				}
			}
			if after := f.snapshot(t); after != before {
				t.Fatal("replay/conflict changed durable event/context/change/receipt rows")
			}
		})
	}
}

func TestPostgreSQLContentWriteAuthorization(t *testing.T) {
	for _, eventType := range []string{"agent.message", "agent.thinking"} {
		for _, variant := range []string{"missing-start", "ended-request", "absent-id-with-receipt", "stale-binding", "wrong-authenticated-pod", "cross-workspace-collision"} {
			t.Run(eventType+"/"+variant, func(t *testing.T) {
				f := newContentDeclarationFixture(t)
				if variant != "missing-start" {
					f.start(t)
				}
				request := f.request(eventType, "write", "evt_00000000000000000000000000000001")
				client, _ := startSandboxProductionBoundaryBridgeClient(t, NewBridgeAPIServer(f.store), f.scope.Binding.TargetPodUid)
				ctx := metadata.AppendToOutgoingContext(f.ctx, "authorization", "Bearer sandbox-production-runtime-token")
				want := codes.FailedPrecondition
				wantStale := false
				var foreign *contentDeclarationFixture
				var foreignBefore string
				switch variant {
				case "ended-request":
					f.end(t, nil)
				case "absent-id-with-receipt":
					// A stored predecessor receipt cannot legalize a missing ID.
					_, err := f.admin.ExecContext(f.ctx, `INSERT INTO session_bridge_operations(workspace_id,session_id,session_thread_id,operation,source_kind,idempotency_key,request_hash,declaration_digest,receipt_json,ack_status,created_at,updated_at) VALUES($1,$2,$3,'write_event','write_event','write','old-digest','old-digest','{"eventId":"evt_predecessor"}','committed',now(),now())`, f.scope.WorkspaceId, f.scope.SessionId, f.scope.SessionThreadId)
					if err != nil {
						t.Fatal(err)
					}
					request.PreallocatedEventId = nil
					want = codes.InvalidArgument
				case "stale-binding":
					request.Scope = proto.Clone(f.scope).(*bridgev1.RuntimeScope)
					request.Scope.Binding.BindingGeneration++
					wantStale = true
				case "wrong-authenticated-pod":
					ctx = metadata.AppendToOutgoingContext(f.ctx, "authorization", "Bearer sandbox-production-wrong-pod-token")
					wantStale = true
				case "cross-workspace-collision":
					other := seedContentDeclarationScope(t, f.admin, f.workload.OpenWorkload(t, "bridge", nil), "workspace_content_foreign")
					other.start(t)
					if _, err := other.store.WriteEvent(other.ctx, other.request(eventType, "foreign-write", request.GetPreallocatedEventId())); err != nil {
						t.Fatal(err)
					}
					foreign = &other
					foreignBefore = other.snapshot(t)
					want = codes.AlreadyExists
				}
				before := f.snapshot(t)
				response, err := client.WriteEvent(ctx, request)
				if wantStale {
					if err != nil || response.GetStale() == nil || response.GetCommitted() != nil || response.GetDuplicate() != nil {
						t.Fatalf("custody result = %v/%v; want typed stale", response, err)
					}
				} else if status.Code(err) != want || response != nil {
					t.Fatalf("authorization result = %v/%v; want %s", response, err, want)
				}
				if err != nil && (strings.Contains(err.Error(), "workspace_content_foreign") || strings.Contains(err.Error(), "SQLSTATE") || strings.Contains(err.Error(), "session_events_pkey")) {
					t.Fatal("rejection disclosed foreign scope or SQL details")
				}
				if f.snapshot(t) != before || foreign != nil && foreign.snapshot(t) != foreignBefore {
					t.Fatal("rejected content write mutated durable facts")
				}
			})
		}
	}
}

type contentResponseLossServer struct {
	BridgeAPIServer
	mu        sync.Mutex
	drop      bool
	committed *bridgev1.WriteEventResponse
}

func (s *contentResponseLossServer) WriteEvent(ctx context.Context, request *bridgev1.WriteEventRequest) (*bridgev1.WriteEventResponse, error) {
	response, err := s.BridgeAPIServer.WriteEvent(ctx, request)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err == nil && s.drop && response.GetCommitted() != nil {
		s.drop = false
		s.committed = proto.Clone(response).(*bridgev1.WriteEventResponse)
		return nil, status.Error(codes.Unavailable, "selected committed response lost")
	}
	return response, err
}

func (s *contentResponseLossServer) receipt() *bridgev1.WriteEventResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.committed == nil {
		return nil
	}
	return proto.Clone(s.committed).(*bridgev1.WriteEventResponse)
}

func TestPostgreSQLContentWriteReplay(t *testing.T) {
	for _, eventType := range []string{"agent.message", "agent.thinking"} {
		for _, afterEnd := range []bool{false, true} {
			name := eventType + "/open"
			if afterEnd {
				name = eventType + "/after-end"
			}
			t.Run(name, func(t *testing.T) {
				f := newContentDeclarationFixture(t)
				f.start(t)
				request := f.request(eventType, "write", "evt_00000000000000000000000000000001")
				loss := &contentResponseLossServer{BridgeAPIServer: NewBridgeAPIServer(f.store), drop: true}
				first, _ := startSandboxProductionBoundaryBridgeClient(t, loss, f.scope.Binding.TargetPodUid)
				secondStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(f.workload.OpenWorkload(t, "bridge", nil)))
				second, _ := startSandboxProductionBoundaryBridgeClient(t, NewBridgeAPIServer(secondStore), f.scope.Binding.TargetPodUid)
				ctx := metadata.AppendToOutgoingContext(f.ctx, "authorization", "Bearer sandbox-production-runtime-token")
				if _, err := first.WriteEvent(ctx, request); status.Code(err) != codes.Unavailable {
					t.Fatalf("lost response = %v", err)
				}
				committed := loss.receipt()
				if committed.GetCommitted().GetEventId() != request.GetPreallocatedEventId() {
					t.Fatal("loss wrapper did not observe the original actual committed receipt")
				}
				if afterEnd {
					f.end(t, committed.GetCommitted().AssignedMessageSequence)
				}
				before := f.snapshot(t)
				response, err := second.WriteEvent(ctx, request)
				if err != nil || response.GetDuplicate().GetEventId() != request.GetPreallocatedEventId() || response.GetDuplicate().GetAssignedMessageSequence() != committed.GetCommitted().GetAssignedMessageSequence() {
					t.Fatalf("receipt recovery = %v/%v", response, err)
				}
				if f.snapshot(t) != before {
					t.Fatal("receipt recovery appended or modified durable facts")
				}
				var events, changes, receipts, messages int
				if err := f.admin.QueryRowContext(f.ctx, `SELECT
				 (SELECT count(*) FROM session_events WHERE event_id=$1),
				 (SELECT count(*) FROM session_event_stream_changes WHERE event_id=$1),
				 (SELECT count(*) FROM session_bridge_operations WHERE workspace_id=$2 AND session_id=$3 AND idempotency_key='write'),
				 (SELECT count(*) FROM session_messages WHERE workspace_id=$2 AND session_id=$3)`, request.GetPreallocatedEventId(), f.scope.WorkspaceId, f.scope.SessionId).Scan(&events, &changes, &receipts, &messages); err != nil {
					t.Fatal(err)
				}
				wantMessages := 0
				if eventType == "agent.message" {
					wantMessages = 1
				}
				if events != 1 || changes != 1 || receipts != 1 || messages != wantMessages {
					t.Fatalf("event/change/receipt/message counts = %d/%d/%d/%d", events, changes, receipts, messages)
				}
				if messages == 1 {
					var raw string
					if err := f.admin.QueryRowContext(f.ctx, `SELECT `+sessionfixture.MessageContentSQL+` FROM session_messages m WHERE workspace_id=$1 AND session_id=$2`, f.scope.WorkspaceId, f.scope.SessionId).Scan(&raw); err != nil {
						t.Fatal(err)
					}
					var actual, expected any
					if err := json.Unmarshal([]byte(raw), &actual); err != nil {
						t.Fatal(err)
					}
					if err := json.Unmarshal([]byte(`{"parts":[{"type":"reasoning","text":"reason-before-text","providerMetadata":{"anthropic":{"signature":"fixture-signature-text"}}},{"type":"text","text":"alpha"}]}`), &expected); err != nil {
						t.Fatal(err)
					}
					got, _ := json.Marshal(actual)
					want, _ := json.Marshal(expected)
					if string(got) != string(want) {
						t.Fatalf("stored content = %s; want fixed %s", got, want)
					}
				}
			})
		}
	}
}
