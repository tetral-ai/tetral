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

func TestLoadContextClosedRetryRetainsOwningRunningFactAcrossReschedule(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_closed_retry_context"
		parentThreadID = "thr_closed_retry_parent"
		childThreadID  = "thr_closed_retry_child"
		bindingID      = "bind_closed_retry_context"
		podUID         = "pod_closed_retry_context"
		firstRequestID = "mreq_closed_retry_first"
		retryRequestID = "mreq_closed_retry_success"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentThreadID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, parentThreadID, childThreadID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, childThreadID, "evt_closed_retry_child_created", 1,
		"session.thread_created", `{"type":"session.thread_created","parent_thread_id":"`+parentThreadID+`","source_tool_use_event_id":"evt_closed_retry_spawn"}`)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("closed-retry-context-signing-key")
	acceptedAt := time.Date(2026, 8, 22, 4, 0, 0, 0, time.UTC)
	store.Clock = func() time.Time { return acceptedAt }
	scope := bridgeAPIScope(sessionID, childThreadID, bindingID, 1, podUID)

	running, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_closed_retry_running",
		EventType: "session.status_running", PayloadJson: `{"type":"session.status_running"}`,
	})
	if err != nil || running.GetCommitted() == nil {
		t.Fatalf("open retry-owned durable turn = %#v/%v", running, err)
	}
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_closed_retry_first_start", firstRequestID, requestKindAgentProviderRequest, 0)
	firstEnd, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_closed_retry_first_end", ModelRequestId: firstRequestID,
		FinishReason: "error", UsageJson: `{}`, IsError: true, ErrorKind: "gateway_stream_error",
		ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "rescheduled"},
		Reschedule: &bridgev1.RequestEndReschedule{
			Attempt: 1, Deadline: acceptedAt.Add(time.Second).Format(time.RFC3339Nano), BackoffMs: 1_000,
		},
	})
	if err != nil || firstEnd.GetCommitted().GetRescheduled() == nil {
		t.Fatalf("commit retryable provider end = %#v/%v", firstEnd, err)
	}
	retryStart := seedBridgeAPIRequestStart(t, store, scope, "rwrite_closed_retry_success_start", retryRequestID, requestKindAgentProviderRequest, 0)
	retryEnd, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_closed_retry_success_end", ModelRequestId: retryRequestID,
		FinishReason: "end_turn", UsageJson: `{}`,
		ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "completed"},
	})
	if err != nil || retryEnd.GetCommitted() == nil {
		t.Fatalf("commit successful provider retry = %#v/%v", retryEnd, err)
	}
	finishRequest := &bridgev1.FinishIdleRequest{
		Scope: scope, DurableTurnId: running.GetCommitted().GetEventId(), StopReasonJson: `{"type":"end_turn"}`,
		CompletionMailText: bridgeString(completionMailEnvelope("main", "task_"+childThreadID, "completed")),
	}
	if idle, finishErr := finishIdleWithStagedCaptureForTest(t, admin, store, finishRequest); finishErr != nil || idle.GetCommitted() == nil {
		t.Fatalf("close successful provider retry = %#v/%v", idle, finishErr)
	}
	bridgeAddress, stopBridge := serveAttachmentCompositionBridge(t, store)
	t.Cleanup(stopBridge)
	actorClient := startActorProductionBridge(t, runtimeDB)
	closeChildThroughProductionInterrupt(
		t, runtimeDB, admin, actorClient, bridgeAddress,
		bridgeAPIScope(sessionID, parentThreadID, bindingID, 1, podUID),
		sessionID, parentThreadID, childThreadID, bindingID, podUID, "evt_closed_retry_close",
	)
	const compactionEventID = "evt_closed_retry_compacted"
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_events (
		workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
		visibility, session_visible, runtime_write_id, model_request_id, projection_json,
		created_at, updated_at, processed_at
	)
	SELECT 'default', $1, $2, $3, COALESCE(MAX(sequence), 0) + 1,
	       'agent.thread_context_compacted', '{}', 'internal', false, $4, $5, '{}', now(), now(), now()
	  FROM session_events
	 WHERE workspace_id='default' AND session_id=$1`,
		sessionID, childThreadID, compactionEventID, "rwrite_closed_retry_compacted", retryRequestID); err != nil {
		t.Fatalf("seed closed retry compaction event: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_messages (
		workspace_id, session_id, session_thread_id, message_id, sequence, kind, data_json,
		source_event_id, created_at, updated_at
	) VALUES ('default',$1,$2,'msg_closed_retry_compaction',1,'compaction',
	          '{"parts":[{"type":"text","text":"retained summary"}]}',$3,now(),now())`,
		sessionID, childThreadID, compactionEventID); err != nil {
		t.Fatalf("seed closed retry compaction Message: %v", err)
	}

	loaded, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope})
	if err != nil {
		t.Fatalf("cold LoadContext after provider retry: %v", err)
	}
	var payload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(loaded.GetContextJson()), &payload); err != nil {
		t.Fatalf("decode closed retry context: %v", err)
	}
	wantRunningID := running.GetCommitted().GetEventId()
	wantStartID := retryStart.GetCommitted().GetEventId()
	wantEndID := retryEnd.GetCommitted().GetRequestEndEventId()
	seenRunning, seenStart, seenEnd, seenIdle := false, false, false, false
	for _, event := range payload.TurnFacts.Events {
		switch event.EventID {
		case wantRunningID:
			seenRunning = event.Type == "session.thread_status_running"
		case wantStartID:
			seenStart = event.RequestStart != nil && event.ModelRequestID != nil && *event.ModelRequestID == retryRequestID
		case wantEndID:
			seenEnd = event.RequestEnd != nil && event.RequestEnd.RequestStartEventID == wantStartID && !event.RequestEnd.IsError
		}
		if event.Idle != nil {
			seenIdle = true
		}
		if event.ModelRequestID != nil && *event.ModelRequestID == firstRequestID {
			t.Fatalf("closed retry context retained superseded provider attempt: %#v", payload.TurnFacts.Events)
		}
	}
	if !seenRunning || !seenStart || !seenEnd || !seenIdle {
		t.Fatalf("closed retry context facts running/start/end/idle = %t/%t/%t/%t; events=%#v",
			seenRunning, seenStart, seenEnd, seenIdle, payload.TurnFacts.Events)
	}
	var runningSequence, floorSequence int64
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT sequence FROM session_events WHERE workspace_id='default' AND session_id=$1 AND event_id=$2),
		(SELECT sequence FROM session_events WHERE workspace_id='default' AND session_id=$1 AND event_id=$3)`,
		sessionID, wantRunningID, wantStartID).Scan(&runningSequence, &floorSequence); err != nil {
		t.Fatalf("read closed retry owner/floor sequences: %v", err)
	}
	if runningSequence >= floorSequence {
		t.Fatalf("closed retry owner sequence = %d, compaction floor = %d; want owner below selected Start floor", runningSequence, floorSequence)
	}
}

func TestLoadContextClosedThreadUsesLatestIdleAfterNoWorkResumeCycle(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_closed_no_work_resume"
		parentThreadID = "thr_closed_no_work_parent"
		childThreadID  = "thr_closed_no_work_child"
		bindingID      = "bind_closed_no_work_resume"
		podUID         = "pod_closed_no_work_resume"
		modelRequestID = "mreq_closed_no_work_resume"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentThreadID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, parentThreadID, childThreadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_threads
		SET status='closed_for_runtime', closed_at=now(), updated_at=now()
		WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, childThreadID); err != nil {
		t.Fatalf("close no-work child: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_events (
		workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
		visibility, session_visible, runtime_write_id, model_request_id, projection_json,
		created_at, updated_at, processed_at
	) VALUES
	('default',$1,$2,'evt_no_work_running_owner',1,'session.thread_status_running','{"type":"session.thread_status_running"}','internal',false,'rwrite_no_work_running_owner',NULL,'{}',now(),now(),now()),
	('default',$1,$2,'evt_no_work_start',2,'span.model_request_start','{"type":"span.model_request_start","model_request_id":"`+modelRequestID+`"}','internal',false,'rwrite_no_work_start','`+modelRequestID+`','{"context_through_message_sequence":0,"request_kind":"agent_provider_request"}',now(),now(),now()),
	('default',$1,$2,'evt_no_work_end',3,'span.model_request_end','{"model_request_start_id":"evt_no_work_start","is_error":false,"provider_context_retention":{"disposition":"completed","tool_use_event_ids":[],"repair_event_ids":[]}}','internal',false,'rwrite_no_work_end','`+modelRequestID+`','{}',now(),now(),now()),
	('default',$1,$2,'evt_no_work_first_idle',4,'session.thread_status_idle','{"type":"session.thread_status_idle","stop_reason":{"type":"end_turn"}}','internal',false,'rwrite_no_work_first_idle',NULL,'{}',now(),now(),now()),
	('default',$1,$2,'evt_no_work_resume_running',5,'session.thread_status_running','{"type":"session.thread_status_running"}','internal',false,'rwrite_no_work_resume_running',NULL,'{}',now(),now(),now()),
	('default',$1,$2,'evt_no_work_latest_idle',6,'session.thread_status_idle','{"type":"session.thread_status_idle","stop_reason":{"type":"end_turn"}}','internal',false,'rwrite_no_work_latest_idle',NULL,'{}',now(),now(),now())`,
		sessionID, childThreadID); err != nil {
		t.Fatalf("seed no-work close cycle: %v", err)
	}

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("closed-no-work-context-key")
	loaded, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope: bridgeAPIScope(sessionID, childThreadID, bindingID, 1, podUID),
	})
	if err != nil {
		t.Fatalf("load no-work close cycle: %v", err)
	}
	var payload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(loaded.GetContextJson()), &payload); err != nil {
		t.Fatalf("decode no-work close cycle: %v", err)
	}
	want := map[string]bool{
		"evt_no_work_running_owner": false,
		"evt_no_work_start":         false,
		"evt_no_work_end":           false,
		"evt_no_work_latest_idle":   false,
	}
	for _, event := range payload.TurnFacts.Events {
		if _, expected := want[event.EventID]; expected {
			want[event.EventID] = true
		}
		if event.EventID == "evt_no_work_first_idle" || event.EventID == "evt_no_work_resume_running" {
			t.Fatalf("no-work close cycle retained stale lifecycle fact %s: %#v", event.EventID, payload.TurnFacts.Events)
		}
	}
	for eventID, present := range want {
		if !present {
			t.Fatalf("no-work close cycle omitted %s: %#v", eventID, payload.TurnFacts.Events)
		}
	}

	bridgeAddress, stopBridge := serveAttachmentCompositionBridge(t, store)
	t.Cleanup(stopBridge)
	result := runProviderRescheduleRecoveryComposition(t, map[string]any{
		"bridgeAddress": bridgeAddress, "workspaceId": workspace.DefaultID,
		"sessionId": sessionID, "sessionThreadId": childThreadID,
		"bindingId": bindingID, "bindingGeneration": 1, "targetPodUid": podUID,
		"now": "2026-08-22T18:00:00Z", "preloadOnly": true,
	})
	if !strings.Contains(string(result.PreloadResult), `"ok":true`) || result.ProviderInvocations != 0 {
		t.Fatalf("no-work close cold reconstruction = preload:%s provider:%d", result.PreloadResult, result.ProviderInvocations)
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
