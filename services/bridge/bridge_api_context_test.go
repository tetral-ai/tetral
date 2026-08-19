package agentruntimebridge

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestLoadContextRejectionEmitsBoundedBridgePhaseReason(t *testing.T) {
	runtimeDB, _ := storagetest.NewPostgreSQLDBWithAdmin(t)
	var output bytes.Buffer
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.Logger = slog.New(slog.NewJSONHandler(&output, nil))
	unsafeIdentity := strings.Repeat("x", 129)
	_, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: &bridgev1.RuntimeScope{
		WorkspaceId: unsafeIdentity,
		SessionId:   unsafeIdentity,
	}})
	if err == nil {
		t.Fatal("LoadContext with invalid scope unexpectedly succeeded")
	}
	logText := output.String()
	if strings.Count(logText, `"event.kind":"runtime_context_load_rejected"`) != 1 ||
		!strings.Contains(logText, `"phase":"scope_validation"`) ||
		!strings.Contains(logText, `"reason":"InvalidArgument"`) ||
		!strings.Contains(logText, `"workspace.id":"invalid"`) ||
		strings.Contains(logText, unsafeIdentity) {
		t.Fatalf("LoadContext rejection diagnostic = %q", logText)
	}
}

func TestLoadContextReturnsDirectNarrowContextFacts(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_narrow_context"
		threadID  = "sthr_narrow_context"
		bindingID = "bind_narrow_context"
		podUID    = "pod_narrow_context"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	now := time.Date(2026, 8, 15, 1, 2, 3, 0, time.UTC)
	if _, err := admin.Exec(`INSERT INTO session_messages (
		workspace_id,session_id,session_thread_id,message_id,sequence,kind,data_json,created_at,updated_at
	) VALUES ($1,$2,$3,'msg_bridge_owned',1,'user',$4,$5,$5)`,
		"default", sessionID, threadID, `{"parts":[{"type":"text","text":"hello"}]}`, now); err != nil {
		t.Fatalf("seed narrow context: %v", err)
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("narrow-context-test-signing-key")
	response, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)})
	if err != nil {
		t.Fatalf("LoadContext: %v", err)
	}
	var payload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(response.GetContextJson()), &payload); err != nil {
		t.Fatalf("decode context: %v", err)
	}
	if len(payload.ContextEntries) != 1 || payload.ContextEntries[0].MessageSequence != 1 || payload.ContextEntries[0].ContextKind != "user" || len(payload.ContextEntries[0].Parts) != 1 {
		t.Fatalf("context entries = %#v", payload.ContextEntries)
	}
	var part map[string]any
	if err := json.Unmarshal(payload.ContextEntries[0].Parts[0], &part); err != nil {
		t.Fatalf("decode part: %v", err)
	}
	if part["type"] != "text" || part["text"] != "hello" || len(part) != 2 {
		t.Fatalf("narrow part = %#v", part)
	}
	if payload.OpenRequestDraft != nil {
		t.Fatalf("open request draft = %#v; want nil", payload.OpenRequestDraft)
	}
	if response.GetRuntimeBindingToken() == "" {
		t.Fatal("LoadContext omitted refreshed binding token")
	}
}

func TestLoadContextRejectsMalformedDurableContext(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bad_narrow_context"
		threadID  = "sthr_bad_narrow_context"
		bindingID = "bind_bad_narrow_context"
		podUID    = "pod_bad_narrow_context"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	if _, err := admin.Exec(`INSERT INTO session_messages (
		workspace_id,session_id,session_thread_id,message_id,sequence,kind,data_json,created_at,updated_at
	) VALUES ($1,$2,$3,'msg_bad',1,'user',$4,now(),now())`,
		"default", sessionID, threadID, `{"parts":[{"type":"text","text":"hello","status":"completed"}]}`); err != nil {
		t.Fatalf("seed malformed context: %v", err)
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("bad-context-test-signing-key")
	_, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("LoadContext error = %v; want FailedPrecondition", err)
	}
}

func TestLoadContextBoundsTurnFactsButRetainsLivePriorToolRequest(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bounded_turn_facts"
		threadID  = "sthr_bounded_turn_facts"
		bindingID = "bind_bounded_turn_facts"
		podUID    = "pod_bounded_turn_facts"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_messages (
		workspace_id,session_id,session_thread_id,message_id,sequence,kind,data_json,model_request_id,created_at,updated_at
	) VALUES ('default',$1,$2,'msg_live_prior_tool',1,'assistant',
		'{"parts":[{"type":"tool_call","modelToolCallId":"call_live_prior","toolName":"Read","canonicalInput":{"path":"README.md"}}]}',
		'mreq_live_prior','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`, sessionID, threadID); err != nil {
		t.Fatalf("seed live prior Tool message: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_events (
		workspace_id,session_id,session_thread_id,event_id,sequence,type,payload_json,model_request_id,projection_json,created_at,updated_at
	) VALUES
		('default',$1,$2,'evt_historical_corrupt',1,'session.error','{}',NULL,'{}','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z'),
		('default',$1,$2,'evt_prior_running',2,'session.status_running','{"type":"session.status_running"}',NULL,'{}','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z'),
		('default',$1,$2,'evt_live_prior_start',3,'span.model_request_start','{}','mreq_live_prior','{"context_through_message_sequence":0,"request_kind":"agent_provider_request"}','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z'),
		('default',$1,$2,'evt_live_prior_tool',4,'agent.tool_use','{"type":"agent.tool_use","name":"Read","input":{"path":"README.md"}}','mreq_live_prior','{"model_tool_call_id":"call_live_prior"}','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z'),
		('default',$1,$2,'evt_live_prior_end',5,'span.model_request_end','{"model_request_start_id":"evt_live_prior_start","is_error":false}','mreq_live_prior','{}','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z'),
		('default',$1,$2,'evt_prior_idle',6,'session.status_idle','{"stop_reason":{"type":"requires_action"}}',NULL,'{}','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z'),
		('default',$1,$2,'evt_current_running',7,'session.status_running','{"type":"session.status_running"}',NULL,'{}','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`,
		sessionID, threadID); err != nil {
		t.Fatalf("seed bounded turn facts: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET status='running' WHERE workspace_id='default' AND id=$1`, sessionID); err != nil {
		t.Fatalf("mark bounded Session active: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_threads SET status='running' WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, threadID); err != nil {
		t.Fatalf("mark bounded Thread active: %v", err)
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("bounded-turn-facts-signing-key")
	response, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)})
	if err != nil {
		t.Fatalf("LoadContext bounded turn facts: %v", err)
	}
	var payload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(response.GetContextJson()), &payload); err != nil {
		t.Fatalf("decode bounded turn facts: %v", err)
	}
	ids := make([]string, 0, len(payload.TurnFacts.Events))
	for _, event := range payload.TurnFacts.Events {
		ids = append(ids, event.EventID)
	}
	if len(ids) == 0 || ids[0] != "evt_live_prior_start" {
		t.Fatalf("bounded turn fact IDs = %v; want live prior Request Start first", ids)
	}
	for _, id := range ids {
		if id == "evt_historical_corrupt" || id == "evt_prior_running" {
			t.Fatalf("bounded turn facts retained historical event %q: %v", id, ids)
		}
	}
}

func TestLoadContextRetainsFailedAssistantRequestBeforeNewerRunningBoundary(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_failed_request_floor"
		threadID  = "sthr_failed_request_floor"
		bindingID = "bind_failed_request_floor"
		podUID    = "pod_failed_request_floor"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_messages (
		workspace_id,session_id,session_thread_id,message_id,sequence,kind,data_json,model_request_id,created_at,updated_at
	) VALUES ('default',$1,$2,'msg_failed_partial',1,'assistant',
		'{"parts":[{"type":"text","text":"failed partial"}]}',
		'mreq_failed_floor','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`, sessionID, threadID); err != nil {
		t.Fatalf("seed failed Assistant message: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_events (
		workspace_id,session_id,session_thread_id,event_id,sequence,type,payload_json,model_request_id,projection_json,created_at,updated_at
	) VALUES
		('default',$1,$2,'evt_before_failed_floor',1,'session.error','{}',NULL,'{}','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z'),
		('default',$1,$2,'evt_failed_floor_running',2,'session.status_running','{"type":"session.status_running"}',NULL,'{}','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z'),
		('default',$1,$2,'evt_failed_floor_start',3,'span.model_request_start','{}','mreq_failed_floor','{"context_through_message_sequence":0,"request_kind":"agent_provider_request"}','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z'),
		('default',$1,$2,'evt_failed_floor_end',4,'span.model_request_end','{"model_request_start_id":"evt_failed_floor_start","is_error":true,"error_kind":"gateway_stream_error"}','mreq_failed_floor','{}','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z'),
		('default',$1,$2,'evt_failed_floor_idle',5,'session.status_idle','{"stop_reason":{"type":"error"}}',NULL,'{}','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z'),
		('default',$1,$2,'evt_newer_running',6,'session.status_running','{"type":"session.status_running"}',NULL,'{}','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`,
		sessionID, threadID); err != nil {
		t.Fatalf("seed failed request turn facts: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET status='running' WHERE workspace_id='default' AND id=$1`, sessionID); err != nil {
		t.Fatalf("mark Session active: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_threads SET status='running' WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, threadID); err != nil {
		t.Fatalf("mark Thread active: %v", err)
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("failed-request-floor-signing-key")
	response, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)})
	if err != nil {
		t.Fatalf("LoadContext failed request floor: %v", err)
	}
	var payload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(response.GetContextJson()), &payload); err != nil {
		t.Fatalf("decode failed request floor: %v", err)
	}
	ids := make([]string, 0, len(payload.TurnFacts.Events))
	for _, event := range payload.TurnFacts.Events {
		ids = append(ids, event.EventID)
	}
	if len(ids) == 0 || ids[0] != "evt_failed_floor_start" {
		t.Fatalf("failed request turn fact IDs = %v; want failed Request Start first", ids)
	}
	for _, id := range ids {
		if id == "evt_before_failed_floor" || id == "evt_failed_floor_running" {
			t.Fatalf("failed request turn facts retained bounded event %q: %v", id, ids)
		}
	}
	if !containsString(strings.Join(ids, ","), "evt_failed_floor_end") || !containsString(strings.Join(ids, ","), "evt_newer_running") {
		t.Fatalf("failed request turn facts = %v; want failed end and newer running boundary", ids)
	}
}
