package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

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
	store := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
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
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_closed_retry_first_start", firstRequestID, runtimecontrol.RequestKindAgentProviderRequest, 0)
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
	retryStart := seedBridgeAPIRequestStart(t, store, scope, "rwrite_closed_retry_success_start", retryRequestID, runtimecontrol.RequestKindAgentProviderRequest, 0)
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
	idle, finishErr := finishIdleWithStagedCaptureForTest(t, admin, store, finishRequest)
	if finishErr != nil || idle.GetCommitted() == nil {
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
	var wantIdleID string
	if err := admin.QueryRowContext(context.Background(), `SELECT event_id FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
		  AND type='session.thread_status_idle' ORDER BY sequence DESC LIMIT 1`,
		sessionID, childThreadID).Scan(&wantIdleID); err != nil {
		t.Fatalf("read latest closed retry idle owner: %v", err)
	}
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
		case wantIdleID:
			seenIdle = event.Idle != nil
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
