package sandbox

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestExecutionResultHintRoundTripKeepsDurableIdentityOnly(t *testing.T) {
	hint := ExecutionResultHint{
		WorkspaceID:     "ws_1",
		SessionID:       "sesn_1",
		SessionThreadID: "thr_1",
		ToolUseEventID:  "evt_1",
	}
	body, err := json.Marshal(hint)
	if err != nil {
		t.Fatalf("marshal hint: %v", err)
	}
	payload := string(body)
	for _, field := range []string{`"workspace_id":"ws_1"`, `"session_id":"sesn_1"`, `"session_thread_id":"thr_1"`, `"tool_use_event_id":"evt_1"`} {
		if !strings.Contains(payload, field) {
			t.Fatalf("hint payload missing %s: %s", field, payload)
		}
	}
	parsed, ok := ParseExecutionResultHint(payload)
	if !ok || parsed != hint {
		t.Fatalf("parse round trip = %+v,%t; want %+v,true", parsed, ok, hint)
	}
}

func TestParseExecutionResultHintRejectsMalformedAndIncompletePayloads(t *testing.T) {
	payloads := []string{
		"",
		"not json",
		`null`,
		`[]`,
		`{"workspace_id":"ws_1"}`,
		`{"workspace_id":"ws_1","session_id":"sesn_1","session_thread_id":"thr_1"}`,
		`{"workspace_id":"","session_id":"sesn_1","session_thread_id":"thr_1","tool_use_event_id":"evt_1"}`,
		`{"workspace_id":1,"session_id":"sesn_1","session_thread_id":"thr_1","tool_use_event_id":"evt_1"}`,
	}
	for _, payload := range payloads {
		if hint, ok := ParseExecutionResultHint(payload); ok {
			t.Fatalf("ParseExecutionResultHint(%q) = %+v,true; want rejected", payload, hint)
		}
	}
}

func TestNotifyExecutionResultTxRejectsIncompleteIdentity(t *testing.T) {
	if err := NotifyExecutionResultTx(context.Background(), nil, ExecutionResultHint{WorkspaceID: "ws_1"}); err == nil {
		t.Fatal("NotifyExecutionResultTx accepted an incomplete identity")
	}
}
