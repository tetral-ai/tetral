package reference

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/cli/reference/wire"
)

// toolListReport is a `cua` tool-list report on connectionID listing the
// tools named.
func toolListReport(connectionID, messageID string, seq int, names ...string) map[string]any {
	tools := []any{}
	for _, name := range names {
		tools = append(tools, map[string]any{"name": name, "inputSchema": map[string]any{"type": "object"}})
	}
	return map[string]any{
		"protocol_version": 1, "connection_id": connectionID, "message_id": messageID, "server": "cua", "seq": seq,
		"server_info": map[string]any{"name": "test", "title": nil, "version": "1"}, "tools": tools, "error": nil,
	}
}

// rowTools is the names of the tools in inst_a's `cua` row, and its
// connection and seq.
func (ts *testServer) rowTools() (string, int64, []string) {
	ts.t.Helper()
	for _, row := range ts.state().ToolLists {
		if row.InstanceID == "inst_a" && row.Server == "cua" {
			var report struct {
				Tools []struct {
					Name string `json:"name"`
				} `json:"tools"`
			}
			if err := json.Unmarshal(row.Report, &report); err != nil {
				ts.t.Fatal(err)
			}
			var names []string
			for _, tool := range report.Tools {
				names = append(names, tool.Name)
			}
			return row.ConnectionID, row.Seq, names
		}
	}
	return "", 0, nil
}

func TestAToolListReportReplacesItsRowByConnectionAndSeq(t *testing.T) {
	ts := startServer(t)
	_, old := ts.online()
	accept := func(report map[string]any) {
		t.Helper()
		if answer := ts.post("mcp-tools", "tetral_ck_a", report); answer.status != http.StatusOK || answer.member("accepted") != true {
			t.Fatalf("got %d %s; want an ACK", answer.status, answer.body)
		}
	}
	expect := func(connectionID string, seq int64, names string) {
		t.Helper()
		gotConnection, gotSeq, gotNames := ts.rowTools()
		if gotConnection != connectionID || gotSeq != seq || strings.Join(gotNames, ",") != names {
			t.Fatalf("row %s %d %v; want %s %d %s", gotConnection, gotSeq, gotNames, connectionID, seq, names)
		}
	}

	accept(toolListReport(old, "tools_1", 1, "a"))
	expect(old, 1, "a")
	accept(toolListReport(old, "tools_2", 1, "b"))
	expect(old, 1, "a")
	accept(toolListReport(old, "tools_3", 2, "b"))
	expect(old, 2, "b")

	refusals := []struct {
		name   string
		report func() any
		status int
		code   string
	}{
		{"a tool named twice", func() any { return toolListReport(old, "tools_4", 3, "c", "c") }, http.StatusBadRequest, "invalid_message"},
		{"a report over 524,288 JSON values", func() any {
			report := toolListReport(old, "tools_7", 3, "c")
			report["x"] = make([]any, wire.MaxReportValues)
			return report
		}, http.StatusBadRequest, "invalid_message"},
		{"a body over 4,194,304 bytes", func() any {
			report := toolListReport(old, "tools_5", 3, "c")
			report["tools"].([]any)[0].(map[string]any)["description"] = strings.Repeat("x", 4_194_304)
			return report
		}, http.StatusRequestEntityTooLarge, "request_too_large"},
	}
	for _, refused := range refusals {
		if answer := ts.post("mcp-tools", "tetral_ck_a", refused.report()); answer.status != refused.status || answer.code() != refused.code {
			t.Errorf("%s: got %d %s; want %d %s", refused.name, answer.status, answer.body, refused.status, refused.code)
		}
	}
	expect(old, 2, "b")
	requests := ts.state().Requests
	if last := requests[len(requests)-1]; last.BodyBytes != 4_194_305 {
		t.Errorf("the oversized body is logged as %d bytes; want the limit and one", last.BodyBytes)
	}

	ts.call("break_stream", map[string]any{"instance_id": "inst_a"}, nil)
	ts.advance(GrantMax)
	fresh := ts.mustOpen("tetral_ck_a", "inst_a", "cred_a", nil)
	current := fresh.expect("connected")["connection_id"].(string)
	if answer := ts.post("ready", "tetral_ck_a", readyBody(current, "ready_2")); answer.status != http.StatusOK {
		t.Fatalf("ready: %d %s", answer.status, answer.body)
	}
	if answer := ts.post("mcp-tools", "tetral_ck_a", toolListReport(old, "tools_6", 9, "z")); answer.status != http.StatusConflict || answer.code() != "stale_connection" {
		t.Fatalf("a report of the old connection: got %d %s; want 409 stale_connection", answer.status, answer.body)
	}
	expect(old, 2, "b")
	accept(toolListReport(current, "tools_1", 1, "d"))
	expect(current, 1, "d")
}
