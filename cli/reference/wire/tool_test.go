package wire

import (
	"encoding/json"
	"testing"
)

// Each row puts a result envelope into a terminal report and names the text
// its refusal holds, or nothing when the report decodes.
func TestAResultEnvelopeIsHeldToItsToolStatusAndErrorKind(t *testing.T) {
	const (
		noError = `null`
		empty   = `{}`
	)
	rows := []struct {
		name, tool, status, toolError, result, refusal string
	}{
		{"an unknown tool", "shell", "success", noError, empty,
			"one of exec_command, edit, view_image, mcp, stdin, cancel"},
		{"an unknown status", "edit", "done", noError, empty, "one of success, error, running"},
		{"status error without an error", "edit", "error", noError, empty,
			"status error without an error, or an error without status error"},
		{"an error without status error", "edit", "success",
			`{"kind": "match_ambiguous", "message": "two spans"}`, `{"replacements": 1, "bytes_written": 4}`,
			"status error without an error, or an error without status error"},
		{"status running for a tool that runs no process", "view_image", "running", noError, empty,
			"status running for a tool that runs no process"},

		{"an edit's counts", "edit", "success", noError, `{"replacements": 1, "bytes_written": 4}`, ""},
		{"an edit's negative count", "edit", "success", noError, `{"replacements": -1, "bytes_written": 4}`,
			"/result/replacements"},
		{"a webp image", "view_image", "success", noError,
			`{"mime": "image/webp", "size_bytes": 3, "data_base64": "AAAA"}`, ""},
		{"an image of a type no model reads", "view_image", "success", noError,
			`{"mime": "image/bmp", "size_bytes": 3, "data_base64": "AAAA"}`,
			"one of image/png, image/jpeg, image/gif, image/webp"},

		{"an mcp_error with its code and message", "mcp", "error",
			`{"kind": "mcp_error", "message": "invalid params", "detail": {"code": -32602, "message": "invalid params"}}`, empty, ""},
		{"an mcp_error without its message", "mcp", "error",
			`{"kind": "mcp_error", "message": "invalid params", "detail": {"code": -32602}}`, empty,
			"an mcp_error detail with code and message"},
		{"an mcp_error whose code is text", "mcp", "error",
			`{"kind": "mcp_error", "message": "invalid params", "detail": {"code": "-32602", "message": "invalid params"}}`, empty,
			"an mcp_error detail with code and message"},
		{"an mcp_error whose code is a fraction", "mcp", "error",
			`{"kind": "mcp_error", "message": "invalid params", "detail": {"code": 1.5, "message": "invalid params"}}`, empty,
			"an mcp_error detail with code and message"},
		{"an mcp_result_too_large with its byte counts", "mcp", "error",
			`{"kind": "mcp_result_too_large", "message": "too large", "detail": {"media_bytes": 0, "envelope_bytes": 10, "report_bytes": 20}}`, empty, ""},
		{"an mcp_result_too_large with a negative count", "mcp", "error",
			`{"kind": "mcp_result_too_large", "message": "too large", "detail": {"media_bytes": -1, "envelope_bytes": 10, "report_bytes": 20}}`, empty,
			"an mcp_result_too_large detail with its byte counts"},
		{"an mcp_result_too_large without a count", "mcp", "error",
			`{"kind": "mcp_result_too_large", "message": "too large", "detail": {"media_bytes": 0, "envelope_bytes": 10}}`, empty,
			"an mcp_result_too_large detail with its byte counts"},
		{"an mcp_timeout without detail", "mcp", "error", `{"kind": "mcp_timeout", "message": "timed out"}`, empty, ""},
		{"an mcp_server_exited without detail", "mcp", "error", `{"kind": "mcp_server_exited", "message": "exited"}`, empty, ""},
		{"an mcp_invalid_result without detail", "mcp", "error", `{"kind": "mcp_invalid_result", "message": "invalid"}`, empty, ""},
		{"a resource_exhausted without detail", "mcp", "error", `{"kind": "resource_exhausted", "message": "full"}`, empty, ""},
		{"an error kind of another tool", "mcp", "error", `{"kind": "match_ambiguous", "message": "two spans"}`, empty,
			"an error kind of an mcp result"},
		{"an mcp error with a result", "mcp", "error", `{"kind": "mcp_timeout", "message": "timed out"}`,
			`{"content": []}`, "an mcp error with a nonempty result"},
	}
	files := fixturesByName(t)
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			envelope := `{"schema_version": 1, "tool": "` + row.tool + `", "status": "` + row.status +
				`", "truncated": false, "error": ` + row.toolError + `, "result": ` + row.result +
				`, "metrics": {"wall_time_ms": 5, "queued_ms": 0, "drive_wait_ms": 0, "run_ms": 5, "sync_ms": 0}}`
			if !json.Valid([]byte(envelope)) {
				t.Fatalf("not JSON: %s", envelope)
			}
			tree := readTree(t, compact(t, files["tool-execution-v1"]["mcp_terminal"].Value))
			put(t, tree, "/result", readTree(t, []byte(envelope)))
			err := decodeEdited(t, files, "tool-execution-v1", "mcp_terminal", tree)
			switch {
			case row.refusal == "" && err != nil:
				t.Fatalf("refused: %v", err)
			case row.refusal != "" && !refusedNaming(err, row.refusal):
				t.Fatalf("decoded with %v, want a refusal holding %q", err, row.refusal)
			}
		})
	}
}
