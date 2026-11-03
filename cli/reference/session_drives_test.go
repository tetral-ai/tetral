package reference

import (
	"net/http"
	"testing"
)

func TestAppliedReportAnswersOnlyTheSessionDrivesSentThroughSend(t *testing.T) {
	ts := startServer(t)
	stream, connectionID := ts.online()
	ts.prepare("sess_1", "bind_1")
	stream.expect("session.prepare")
	ts.call("send", map[string]any{"instance_id": "inst_a", "event": map[string]any{
		"protocol_version": 1, "connection_id": connectionID, "message_id": "drives_1",
		"type": "session.drives", "session_id": "sess_1", "binding_id": "bind_1",
		"payload": map[string]any{"session_id": "sess_1", "binding_id": "bind_1", "drives": []any{}},
	}}, nil)
	stream.expect("session.drives")
	applied := func(messageID, requestMessageID, bindingID string) map[string]any {
		return map[string]any{
			"protocol_version": 1, "connection_id": connectionID, "message_id": messageID,
			"request_message_id": requestMessageID, "session_id": "sess_1", "binding_id": bindingID,
			"status": "applied",
		}
	}
	refused := []struct {
		name   string
		report map[string]any
	}{
		{"another binding", applied("report_1", "drives_1", "bind_other")},
		{"an unsent session.drives", applied("report_2", "drives_unsent", "bind_1")},
	}
	for _, row := range refused {
		if answer := ts.post("session-status", "tetral_ck_a", row.report); answer.status != http.StatusConflict || answer.code() != "unknown_instruction" {
			t.Errorf("%s: %d %s; want 409 unknown_instruction", row.name, answer.status, answer.body)
		}
	}
	if answer := ts.post("session-status", "tetral_ck_a", applied("report_3", "drives_1", "bind_1")); answer.status != http.StatusOK || answer.member("accepted") != true {
		t.Errorf("applied: %d %s; want an ACK", answer.status, answer.body)
	}
}
