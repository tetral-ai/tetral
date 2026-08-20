package agentruntimebridge

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestFailedRequestWithoutRetentionDeclarationKeepsAssistantAuditOnly(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_failed_acknowledged_context"
		threadID  = "thr_failed_acknowledged_context"
		bindingID = "bind_failed_acknowledged_context"
		podUID    = "pod_failed_acknowledged_context"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("failed-context-test-signing-key")
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_failed_context_start", "mreq_failed_context", requestKindAgentProviderRequest, 0)
	member, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_failed_context_member", ModelRequestId: "mreq_failed_context",
		EventType: "agent.message", PayloadJson: `{"type":"agent.message","content":[{"type":"text","text":"acknowledged before failure"}]}`,
		AssistantContextDelta: bridgeTextContextDeltaForTest("acknowledged before failure"),
	})
	if err != nil || member.GetCommitted() == nil {
		t.Fatalf("write acknowledged member = %#v/%v", member, err)
	}
	if _, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_failed_context_end", ModelRequestId: "mreq_failed_context",
		FinishReason: "error", UsageJson: `{}`,
		ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "failed"}, IsError: true, ErrorKind: "provider_error",
	}); err != nil {
		t.Fatalf("write failed request end: %v", err)
	}
	loaded, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope})
	if err != nil {
		t.Fatalf("load failed request context: %v", err)
	}
	var payload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(loaded.GetContextJson()), &payload); err != nil {
		t.Fatalf("decode failed request context: %v", err)
	}
	if payload.OpenRequestDraft != nil || len(payload.ContextEntries) != 0 {
		t.Fatalf("audit-only failed Assistant entered cold provider context = %#v", payload)
	}
}

func TestWriteRequestEndReturnsOnlyDirectDurableFacts(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_request_end_facts"
		threadID  = "sthr_request_end_facts"
		bindingID = "bind_request_end_facts"
		podUID    = "pod_request_end_facts"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_start_end", "mreq_end", requestKindAgentProviderRequest, 0)
	message, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_member_end", ModelRequestId: "mreq_end",
		EventType: "agent.message", PayloadJson: `{"type":"agent.message","content":[{"type":"text","text":"done"}]}`,
		AssistantContextDelta: bridgeTextContextDeltaForTest("done"),
	})
	if err != nil || message.GetCommitted() == nil {
		t.Fatalf("seed Assistant context: response=%#v err=%v", message, err)
	}
	request := &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_end", ModelRequestId: "mreq_end",
		FinishReason: "stop", UsageJson: `{}`,
		ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "completed"},
	}
	first, err := store.WriteRequestEnd(context.Background(), request)
	if err != nil {
		t.Fatalf("WriteRequestEnd: %v", err)
	}
	if first.GetCommitted() == nil || first.GetCommitted().GetRequestEndEventId() == "" || first.GetCommitted().GetOrdinary() == nil ||
		first.GetCommitted().GetOrdinary().GetSealedMessageSequence() != message.GetCommitted().GetAssignedMessageSequence() {
		t.Fatalf("committed request end = %#v", first)
	}
	second, err := store.WriteRequestEnd(context.Background(), request)
	if err != nil {
		t.Fatalf("duplicate WriteRequestEnd: %v", err)
	}
	if second.GetDuplicate() == nil || second.GetDuplicate().GetRequestEndEventId() != first.GetCommitted().GetRequestEndEventId() || second.GetDuplicate().GetOrdinary() == nil {
		t.Fatalf("duplicate request end = %#v; first=%#v", second, first)
	}
}

func TestAcceptedRescheduleResultDoesNotEchoAttempt(t *testing.T) {
	facts := requestEndDurableFacts{RequestEndEventID: "evt_end", Disposition: "rescheduled", EffectiveDeadline: "2026-08-15T01:00:00Z"}
	encoded, err := marshalRequestEndReplay(facts)
	if err != nil {
		t.Fatalf("marshal replay: %v", err)
	}
	if containsJSONField(encoded, "attempt") {
		t.Fatalf("request end result echoed caller attempt: %s", encoded)
	}
	replayed, err := unmarshalRequestEndReplay(encoded)
	if err != nil || replayed.EffectiveDeadline != facts.EffectiveDeadline || replayed.Disposition != "rescheduled" {
		t.Fatalf("replayed facts = %#v err=%v", replayed, err)
	}
}

func TestLoadContextCarriesAcceptedRescheduleAttemptAndDeadline(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_reschedule_context_facts"
		threadID  = "sthr_reschedule_context_facts"
		bindingID = "bind_reschedule_context_facts"
		podUID    = "pod_reschedule_context_facts"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("reschedule-context-test-signing-key")
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	store.Clock = func() time.Time { return now }
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_reschedule_compaction_start", "mreq_reschedule_compaction", requestKindCompactionSummary, 0)
	compactionEnd, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_reschedule_compaction_end", ModelRequestId: "mreq_reschedule_compaction",
		FinishReason: "error", UsageJson: `{}`,
		ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "rescheduled"}, IsError: true, ErrorKind: "gateway_stream_error",
		Reschedule: &bridgev1.RequestEndReschedule{
			Attempt: 1, Deadline: now.Add(30 * time.Second).Format(time.RFC3339Nano), BackoffMs: 1_000,
		},
	})
	if err != nil || compactionEnd.GetCommitted().GetRescheduled() == nil {
		t.Fatalf("write prior compaction reschedule: response=%#v err=%v", compactionEnd, err)
	}
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_reschedule_start", "mreq_reschedule", requestKindAgentProviderRequest, 0)
	message, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_reschedule_member", ModelRequestId: "mreq_reschedule",
		EventType: "agent.message", PayloadJson: `{"type":"agent.message","content":[{"type":"text","text":"partial"}]}`,
		AssistantContextDelta: bridgeTextContextDeltaForTest("partial"),
	})
	if err != nil || message.GetCommitted() == nil {
		t.Fatalf("seed rescheduled Assistant context: response=%#v err=%v", message, err)
	}
	ended, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_reschedule_end", ModelRequestId: "mreq_reschedule",
		FinishReason: "error", UsageJson: `{}`,
		ProviderContextRetention: &bridgev1.ProviderContextRetention{
			Disposition: "rescheduled", AssistantMessageSequence: message.GetCommitted().AssignedMessageSequence,
		}, IsError: true, ErrorKind: "gateway_stream_error",
		Reschedule: &bridgev1.RequestEndReschedule{
			Attempt:   1,
			Deadline:  now.Add(time.Minute).Format(time.RFC3339Nano),
			BackoffMs: 1_000,
		},
	})
	if err != nil || ended.GetCommitted().GetRescheduled() == nil {
		t.Fatalf("write rescheduled request end: response=%#v err=%v", ended, err)
	}
	loaded, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope})
	if err != nil {
		t.Fatalf("load rescheduled request context: %v", err)
	}
	var payload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(loaded.GetContextJson()), &payload); err != nil {
		t.Fatalf("decode rescheduled request context: %v", err)
	}
	var requestEnd *bridgeLoadContextRequestEnd
	for _, event := range payload.TurnFacts.Events {
		if event.ModelRequestID != nil && *event.ModelRequestID == "mreq_reschedule" && event.RequestEnd != nil {
			requestEnd = event.RequestEnd
			break
		}
	}
	if requestEnd == nil || requestEnd.Reschedule == nil || requestEnd.Reschedule.Attempt != 1 ||
		requestEnd.Reschedule.ProviderAttempts != 1 || requestEnd.Reschedule.CompactionAttempts != 1 ||
		requestEnd.Reschedule.EffectiveDeadline != ended.GetCommitted().GetRescheduled().GetEffectiveDeadline() ||
		requestEnd.ProviderContextRetention.AssistantMessageSequence == nil ||
		*requestEnd.ProviderContextRetention.AssistantMessageSequence != message.GetCommitted().GetAssignedMessageSequence() {
		t.Fatalf("rescheduled Request End direct facts = %#v", requestEnd)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_turn_retries
		SET provider_attempts=0, compaction_attempts=0
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2`, sessionID, threadID); err != nil {
		t.Fatalf("reset mutable retry counters: %v", err)
	}
	reloaded, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope})
	if err != nil {
		t.Fatalf("reload rescheduled request after counter reset: %v", err)
	}
	var reloadedPayload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(reloaded.GetContextJson()), &reloadedPayload); err != nil {
		t.Fatalf("decode reloaded reschedule context: %v", err)
	}
	requestEnd = nil
	for _, event := range reloadedPayload.TurnFacts.Events {
		if event.ModelRequestID != nil && *event.ModelRequestID == "mreq_reschedule" && event.RequestEnd != nil {
			requestEnd = event.RequestEnd
			break
		}
	}
	if requestEnd == nil || requestEnd.Reschedule == nil || requestEnd.Reschedule.Attempt != 1 ||
		requestEnd.Reschedule.ProviderAttempts != 1 || requestEnd.Reschedule.CompactionAttempts != 1 {
		t.Fatalf("immutable rescheduled Request End facts after counter reset = %#v", requestEnd)
	}
}

func containsJSONField(raw, field string) bool {
	return len(raw) > 0 && (raw == `"`+field+`"` || containsString(raw, `"`+field+`"`))
}

func containsString(raw, needle string) bool {
	for index := 0; index+len(needle) <= len(raw); index++ {
		if raw[index:index+len(needle)] == needle {
			return true
		}
	}
	return false
}
