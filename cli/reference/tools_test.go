package reference

import (
	"net/http"
	"strings"
	"testing"
)

func executePayload(taskID string) map[string]any {
	return map[string]any{
		"session_id": "sess_1", "binding_id": "bind_1", "tool_use_event_id": "call_" + taskID, "task_id": taskID,
		"tool": "exec_command", "input": map[string]any{"cmd": "true"},
		"sandbox_policy": map[string]any{
			"schema_version": 1, "filesystem": map[string]any{"rules": []any{}}, "network": map[string]any{"mode": "disabled"},
		},
	}
}

// toolReport is a tool-status report on connectionID answering the
// instruction requestMessageID of task_1.
func toolReport(connectionID, messageID, requestMessageID string, members map[string]any) map[string]any {
	report := map[string]any{
		"protocol_version": 1, "connection_id": connectionID, "message_id": messageID,
		"session_id": "sess_1", "binding_id": "bind_1", "tool_use_event_id": "call_task_1", "task_id": "task_1",
		"request_message_id": requestMessageID,
	}
	for name, value := range members {
		report[name] = value
	}
	return report
}

func rejection() map[string]any {
	return map[string]any{
		"kind": "execution", "phase": "rejected",
		"error": map[string]any{"code": "binding_unavailable", "message": "no binding"},
	}
}

// execResult is an exec_command result report of the given phase whose
// stdout text is stdout.
func execResult(phase string, stdout string) map[string]any {
	stream := func(text string) map[string]any {
		return map[string]any{"text": text, "total_bytes": len(text), "total_lines": 0, "returned_bytes": len(text), "truncated": false}
	}
	result := map[string]any{
		"task_id": "task_1", "exit_code": 0, "signal": nil, "timed_out": false,
		"stdout": stream(stdout), "stderr": stream(""), "duration_ms": 1,
	}
	envelope := map[string]any{
		"schema_version": 1, "tool": "exec_command", "status": "success", "truncated": false, "error": nil, "result": result,
	}
	if phase == "terminal" {
		envelope["metrics"] = map[string]any{"wall_time_ms": 1, "queued_ms": 0, "drive_wait_ms": 0, "run_ms": 1, "sync_ms": 0}
	} else {
		envelope["status"] = "running"
		result["exit_code"] = nil
	}
	return map[string]any{"kind": "execution", "phase": phase, "report_seq": 1, "result": envelope}
}

func TestAToolInstructionIsWrittenWithTheBytesOfItsPayload(t *testing.T) {
	ts := startServer(t)
	stream, _ := ts.online()
	// Two commands of equal length, one of characters that HTML escaping
	// would lengthen, each written by hand into the control's body so that
	// no encoder of the test escapes them.
	for messageID, cmd := range map[string]string{"msg_html": "<a&b>", "msg_text": "xaxbx"} {
		body := `{"instance_id":"inst_a","message_id":"` + messageID + `","payload":{"session_id":"sess_1",` +
			`"binding_id":"bind_1","tool_use_event_id":"call_task_1","task_id":"task_1","tool":"exec_command",` +
			`"input":{"cmd":"` + cmd + `"},"sandbox_policy":{"schema_version":1,"filesystem":{"rules":[]},` +
			`"network":{"mode":"disabled"}}}}`
		response, err := http.Post(ts.control.URL+"/control/execute", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if sent := payload(stream.expect("tool.execute")); sent["input"].(map[string]any)["cmd"] != cmd {
			t.Errorf("sent %v", sent)
		}
	}
	sizes := stream.dataBytes()
	if html, text := sizes[len(sizes)-2], sizes[len(sizes)-1]; html != text {
		t.Errorf("the events are %d and %d bytes", html, text)
	}
}

func TestToolControlsIssueToolInstructionsWithTheirPayloadAsGiven(t *testing.T) {
	ts := startServer(t)
	stream, connectionID := ts.online()
	overBudget := strings.Repeat("x", 32_769)
	for _, row := range []struct {
		control, kind string
		payload       map[string]any
	}{
		{"execute", "tool.execute", executePayload("task_1")},
		{"stdin", "tool.stdin", map[string]any{
			"session_id": "sess_1", "binding_id": "bind_1", "tool_use_event_id": "call_2", "task_id": "task_1",
			"chars": overBudget, "write_seq": 1,
		}},
		{"cancel", "tool.cancel", map[string]any{
			"session_id": "sess_1", "binding_id": "bind_1", "tool_use_event_id": "call_3", "task_id": "task_1",
		}},
	} {
		var issued struct {
			MessageID string `json:"message_id"`
		}
		ts.call(row.control, map[string]any{
			"instance_id": "inst_a", "payload": row.payload, "extra": map[string]any{"extension": row.control},
		}, &issued)
		event := stream.expect(row.kind)
		sent := payload(event)
		if event["message_id"] != issued.MessageID || sent["tool_use_event_id"] != row.payload["tool_use_event_id"] ||
			sent["chars"] != row.payload["chars"] || event["extension"] != row.control {
			t.Errorf("%s: sent %v", row.control, event)
		}
	}
	issued := ts.connection(connectionID).Issued
	if len(issued) != 3 || issued[1].TaskID != "task_1" || issued[1].ToolUseEventID != "call_2" {
		t.Errorf("issued %+v", issued)
	}
}

func TestAToolStatusReportIsAcceptedOnlyForItsIssuedInstructionWithinItsLimits(t *testing.T) {
	for _, row := range []struct {
		name     string
		instruct string
		members  map[string]any
		status   int
		code     string
		reported bool
	}{
		{"a rejection of the execute", "execute", rejection(), http.StatusOK, "", true},
		{"a terminal result", "execute", execResult("terminal", "done"), http.StatusOK, "", true},
		{"a running result, which leaves the execute unreported", "execute", execResult("running", "x"), http.StatusOK, "", false},
		{"a control reply to the stdin", "stdin", map[string]any{
			"kind": "control", "error": map[string]any{"code": "task_not_found", "message": "no task"},
		}, http.StatusOK, "", true},
		{"an execution report for the stdin", "stdin", rejection(), http.StatusConflict, "unknown_instruction", false},
		{"another task's identity", "execute", map[string]any{
			"kind": "execution", "phase": "rejected", "task_id": "task_2",
			"error": map[string]any{"code": "invalid_input", "message": "x"},
		}, http.StatusConflict, "unknown_instruction", false},
		{"a result envelope over 262,144 bytes in a report within 327,680", "execute",
			execResult("terminal", strings.Repeat("a", 262_144)), http.StatusBadRequest, "invalid_message", false},
		{"an ordinary report over 327,680 bytes", "execute",
			execResult("terminal", strings.Repeat("a", 327_680)), http.StatusRequestEntityTooLarge, "request_too_large", false},
		{"a body over 14,680,064 bytes", "execute",
			execResult("terminal", strings.Repeat("a", 14_680_064)), http.StatusRequestEntityTooLarge, "request_too_large", false},
	} {
		t.Run(row.name, func(t *testing.T) {
			ts := startServer(t)
			stream, connectionID := ts.online()
			instructionPayload := executePayload("task_1")
			if row.instruct == "stdin" {
				instructionPayload = map[string]any{
					"session_id": "sess_1", "binding_id": "bind_1", "tool_use_event_id": "call_task_1", "task_id": "task_1",
					"chars": "",
				}
			}
			var issued struct {
				MessageID string `json:"message_id"`
			}
			ts.call(row.instruct, map[string]any{"instance_id": "inst_a", "payload": instructionPayload}, &issued)
			stream.expect("tool." + row.instruct)
			answer := ts.post("tool-status", "tetral_ck_a", toolReport(connectionID, "report_1", issued.MessageID, row.members))
			if answer.status != row.status || answer.code() != row.code {
				t.Fatalf("answer %d %.200s; want %d %q", answer.status, answer.body, row.status, row.code)
			}
			if row.status == http.StatusOK && (answer.member("accepted") != true || answer.member("message_id") != "report_1") {
				t.Errorf("ACK %s", answer.body)
			}
			if reported := ts.connection(connectionID).Issued[0].Reported; reported != row.reported {
				t.Errorf("reported %v; want %v", reported, row.reported)
			}
		})
	}
}
