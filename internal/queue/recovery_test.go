package queue

import (
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestRuntimeRecoveryPayloadSourceUnion(t *testing.T) {
	for _, raw := range []string{
		`{"session_id":"session","session_thread_id":"thread"}`,
		`{"session_id":"session","session_thread_id":"thread","source_event_id":"event","handoff_id":"handoff"}`,
		`{"session_id":"session","session_thread_id":"thread","source_event_id":"","handoff_id":"handoff"}`,
		`{"session_id":"session","session_thread_id":"thread","handoff_id":null}`,
		`{"session_id":"session","session_thread_id":"thread","handoff_id":42}`,
		`{"session_id":"session","session_thread_id":"thread","handoff_id":"handoff","claimed_idle":true}`,
	} {
		t.Run(raw, func(t *testing.T) {
			if _, err := DecodeRuntimeRecoveryPayload([]byte(raw)); err == nil {
				t.Fatal("invalid source union accepted")
			}
		})
	}
	event, err := NewRuntimeRecoveryEnqueueRequest(workspace.DefaultID, "session", "thread", "event", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	handoff, err := NewRuntimeHandoffEnqueueRequest(workspace.DefaultID, "session", "thread", "handoff", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range []EnqueueRequest{event, handoff} {
		if _, err := NormalizeEnqueueRequest(request); err != nil {
			t.Fatalf("canonical source rejected: %v", err)
		}
	}
	if event.DedupeKey != FormatRuntimeRecoveryDedupeKey(workspace.DefaultID, "session", "event") {
		t.Fatal("existing event-origin key changed")
	}
	sibling := handoff
	sibling.DedupeKey = FormatRuntimeHandoffDedupeKey(workspace.DefaultID, "session", "other-thread", "handoff")
	if _, err := NormalizeEnqueueRequest(sibling); err == nil {
		t.Fatal("handoff source accepted another Thread key")
	}
}
