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
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
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

func TestLoadContextConsumesExactLiveRecoveryLeaseBeforeColdFacts(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_context_recovery_lease"
		threadID  = "thr_context_recovery_lease"
		bindingID = "bind_context_recovery_lease"
		podUID    = "pod_context_recovery_lease"
		sourceID  = "evt_context_recovery_lease"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, sourceID, 1, "session.status_rescheduled", `{}`)
	client := dbconnect.NewClientForTesting(runtimeDB)
	queueStore := queue.NewPostgreSQLStore(client)
	enqueue, err := queue.NewRuntimeRecoveryEnqueueRequest(workspace.DefaultID, sessionID, threadID, sourceID, time.Now().UTC())
	if err != nil {
		t.Fatalf("build recovery Queue job: %v", err)
	}
	if _, err := queueStore.Enqueue(context.Background(), enqueue); err != nil {
		t.Fatalf("enqueue recovery Queue job: %v", err)
	}
	leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeRecovery}, LeaseOwner: "context-loader",
		MaxJobs: 1, LeaseDuration: time.Minute,
	})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease recovery Queue job = %#v/%v", leased, err)
	}
	request := &bridgev1.LoadContextRequest{
		Scope: bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID),
		RecoveryLeaseRef: &bridgev1.RecoveryLeaseRef{
			JobId: leased[0].ID, LeaseToken: leased[0].LeaseToken,
			PartitionKey: leased[0].PartitionKey, DedupeKey: leased[0].DedupeKey,
		},
	}
	store := NewPostgreSQLBridgeAPIStore(client)
	store.RuntimeBindingTokenHMACKey = []byte("recovery-load-authority-key")
	var diagnostics bytes.Buffer
	store.Logger = slog.New(slog.NewJSONHandler(&diagnostics, nil))
	if _, err := store.LoadContext(context.Background(), request); err != nil {
		t.Fatalf("LoadContext with live recovery lease: %v diagnostics=%s", err, diagnostics.String())
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE queue_jobs SET leased_until=clock_timestamp()-interval '1 second'
		 WHERE workspace_id='default' AND id=$1`, leased[0].ID); err != nil {
		t.Fatalf("expire recovery lease: %v", err)
	}
	if _, err := store.LoadContext(context.Background(), request); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("LoadContext with stale recovery lease = %v; want FailedPrecondition", err)
	}
}

func TestLoadContextColdParserOmitsTerminalFailureBelowCompactionFloor(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_terminal_context"
		threadID  = "thr_terminal_context"
		bindingID = "bind_terminal_context"
		podUID    = "pod_terminal_context"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_events (
		workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
		visibility, session_visible, runtime_write_id, model_request_id, projection_json,
		created_at, updated_at, processed_at
	) VALUES
	('default',$1,$2,'evt_terminal_old_open',1,'span.model_request_start',
	 '{"type":"span.model_request_start","model_request_id":"mreq_terminal_old_open"}',
	 'internal',false,'rwrite_terminal_old_open','mreq_terminal_old_open',
	 '{"context_through_message_sequence":0,"request_kind":"agent_provider_request"}',now(),now(),now()),
	('default',$1,$2,'evt_terminal_error',2,'session.error',
	 '{"type":"session.error","error":{"type":"unknown_error","message":"The runtime could not complete the request.","retry_status":{"type":"terminal"}}}',
	 'public',true,'rwrite_terminal:error',NULL,'{}',now(),now(),now()),
	('default',$1,$2,'evt_terminal_close',3,'session.status_terminated','{"type":"session.status_terminated"}',
	 'public',true,'rwrite_terminal',NULL,'{}',now(),now(),now()),
	('default',$1,$2,'evt_terminal_compaction_start',4,'span.model_request_start',
	 '{"type":"span.model_request_start","model_request_id":"mreq_terminal_compaction"}',
	 'internal',false,'rwrite_terminal_compaction_start','mreq_terminal_compaction',
	 '{"context_through_message_sequence":1,"request_kind":"compaction_summary"}',now(),now(),now()),
	('default',$1,$2,'evt_terminal_compaction_end',5,'span.model_request_end',
	 '{"model_request_start_id":"evt_terminal_compaction_start","is_error":false,"provider_context_retention":{"disposition":"compacted","tool_use_event_ids":[],"repair_event_ids":[]}}',
	 'internal',false,'rwrite_terminal_compaction_end','mreq_terminal_compaction','{}',now(),now(),now()),
	('default',$1,$2,'evt_terminal_compacted',6,'agent.thread_context_compacted','{}',
	 'internal',false,'rwrite_terminal_compacted','mreq_terminal_compaction','{}',now(),now(),now()),
	('default',$1,$2,'evt_terminal_running',7,'session.status_running','{"type":"session.status_running"}',
	 'internal',false,'rwrite_terminal_running',NULL,'{}',now(),now(),now())`, sessionID, threadID); err != nil {
		t.Fatalf("seed terminal context facts: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_messages (
		workspace_id, session_id, session_thread_id, message_id, sequence, kind, data_json,
		source_event_id, model_request_id, created_at, updated_at
	) VALUES
	('default',$1,$2,'msg_terminal_old_open',1,'assistant','{"parts":[{"type":"text","text":"superseded"}]}',
	 'evt_terminal_old_open','mreq_terminal_old_open',now(),now()),
	('default',$1,$2,'msg_terminal_compaction',2,'compaction','{"parts":[{"type":"text","text":"summary"}]}',
	 'evt_terminal_compacted',NULL,now(),now())`, sessionID, threadID); err != nil {
		t.Fatalf("seed terminal context messages: %v", err)
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("terminal-context-signing-key")
	bridgeAddress, stopBridge := serveAttachmentCompositionBridge(t, store)
	t.Cleanup(stopBridge)
	result := runProviderRescheduleRecoveryComposition(t, map[string]any{
		"bridgeAddress": bridgeAddress, "workspaceId": workspace.DefaultID,
		"sessionId": sessionID, "sessionThreadId": threadID,
		"bindingId": bindingID, "bindingGeneration": 1, "targetPodUid": podUID,
		"now": "2026-08-21T12:00:00Z", "preloadOnly": true,
	})
	turnEventsJSON, err := json.Marshal(result.RecoveredTurnEvents)
	if err != nil {
		t.Fatalf("encode recovered terminal events: %v", err)
	}
	turnEvents := string(turnEventsJSON)
	if !strings.Contains(turnEvents, "evt_terminal_running") ||
		strings.Contains(turnEvents, "evt_terminal_error") ||
		strings.Contains(turnEvents, "evt_terminal_close") ||
		strings.Contains(turnEvents, "evt_terminal_old_open") ||
		!strings.Contains(string(result.PreloadResult), `"ok":true`) {
		t.Fatalf("terminal cold parse = events %s preload %s", turnEvents, result.PreloadResult)
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
