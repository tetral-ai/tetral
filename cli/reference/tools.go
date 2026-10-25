package reference

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/tetral-ai/tetral/cli/reference/wire"
)

// toolInstructionTypes are the tool instructions a control issues.
var toolInstructionTypes = []string{"tool.execute", "tool.stdin", "tool.cancel"}

// toolStatus accepts a tool-status report that answers a tool instruction
// issued on its connection, within the byte limits of its kind, and marks a
// final one (a terminal or rejected execution report, a control's reply) as
// reported, so that a resume does not write its instruction again.
func (server *Server) toolStatus(w http.ResponseWriter, request *http.Request) {
	server.serve(w, request, endpoint{
		name: "tool-status", maxBytes: wire.MediaToolStatusJSONBytes, replayable: true,
		decode: func(body []byte) (string, string, any, error) {
			report, err := wire.Decode[wire.ToolStatus](body)
			return string(report.ConnectionID), string(report.MessageID), &report, err
		},
		apply: func(conn *connection, value any, body []byte) (answer, *refusal) {
			report := value.(*wire.ToolStatus)
			if refused := reportSizes(report, body); refused != nil {
				return answer{}, refused
			}
			instruction := conn.issued[string(report.RequestMessageID)]
			if instruction == nil || !answersInstruction(report, instruction) {
				return answer{}, refuse("unknown_instruction", "the report answers no tool instruction issued on this connection")
			}
			final := report.Kind == "control" || report.Phase.Value != "running"
			if final {
				instruction.reported = true
			}
			return acknowledge(conn, string(report.MessageID)), nil
		},
	})
}

// answersInstruction is whether report carries the identities of the tool
// instruction it names and has the kind that answers it.
func answersInstruction(report *wire.ToolStatus, instruction *issuedInstruction) bool {
	kind := instruction.kind == "tool.execute" && report.Kind == "execution" ||
		(instruction.kind == "tool.stdin" || instruction.kind == "tool.cancel") && report.Kind == "control"
	return kind && instruction.sessionID == string(report.SessionID) &&
		instruction.bindingID == string(report.BindingID) &&
		instruction.toolUseEventID == string(report.ToolUseEventID) &&
		instruction.taskID == string(report.TaskID)
}

// reportSizes checks the byte limits of a tool-status body within the media
// limit: a complete ordinary report over its limit is `413
// request_too_large`; a result envelope (for `view_image`, with its
// `data_base64` emptied) or metadata (the report with its result replaced by
// `{}`) over its own limit is `400 invalid_message`. Each size is of the
// compact JSON as written.
func reportSizes(report *wire.ToolStatus, body []byte) *refusal {
	complete := compactLen(body)
	var members map[string]json.RawMessage
	_ = json.Unmarshal(body, &members)
	result, hasResult := members["result"]
	resultLen := compactLen(result)
	media := hasResult && report.Result.Set && report.Result.Value.Tool == "view_image"
	if !media && complete > wire.OrdinaryToolStatusJSONBytes {
		return refuse("request_too_large", "the ordinary tool-status report is over its limit")
	}
	envelope := resultLen
	if media {
		envelope -= len(imageData(result)) - len(`""`)
	}
	if hasResult && envelope > wire.HelperResultJSONBytes {
		return refuse("invalid_message", "the result envelope is over its limit")
	}
	metadata := complete
	if hasResult {
		metadata = complete - resultLen + len(`{}`)
	}
	if metadata > wire.MetadataJSONBytes {
		return refuse("invalid_message", "the report's metadata is over its limit")
	}
	return nil
}

// imageData is the compact `data_base64` member of a view_image result
// envelope, quotes included.
func imageData(envelope json.RawMessage) []byte {
	var members struct {
		Result struct {
			DataBase64 json.RawMessage `json:"data_base64"`
		} `json:"result"`
	}
	_ = json.Unmarshal(envelope, &members)
	var compact bytes.Buffer
	_ = json.Compact(&compact, members.Result.DataBase64)
	return compact.Bytes()
}

// compactLen is the length of JSON text without insignificant whitespace,
// every token as written.
func compactLen(text []byte) int {
	var compact bytes.Buffer
	if json.Compact(&compact, text) != nil {
		return len(text)
	}
	return compact.Len()
}
