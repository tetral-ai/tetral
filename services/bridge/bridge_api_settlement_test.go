package agentruntimebridge

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestFailedRequestPreservesAcknowledgedAssistantContext(t *testing.T) {
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
		FinishReason: "error", UsageJson: `{}`, IsError: true, ErrorKind: "provider_error",
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
	if payload.OpenRequestDraft != nil || len(payload.ContextEntries) != 1 ||
		payload.ContextEntries[0].MessageSequence != member.GetCommitted().GetAssignedMessageSequence() ||
		len(payload.ContextEntries[0].Parts) != 1 ||
		!containsString(string(payload.ContextEntries[0].Parts[0]), "acknowledged before failure") {
		t.Fatalf("failed request cold context = %#v", payload)
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
	facts := requestEndDurableFacts{RequestEndEventID: "evt_end", Disposition: "rescheduled", EffectiveDeadline: "2026-08-15T01:00:00Z", PendingAttachmentJSON: []string{}}
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
