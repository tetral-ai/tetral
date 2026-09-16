package sandbox

import (
	"context"
	"testing"
)

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
