package reference

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/tetral-ai/tetral/cli/reference/wire"
)

// toolInstructionTypes are the tool instructions a control issues.
var toolInstructionTypes = []string{"tool.execute", "tool.stdin", "tool.cancel"}

// toolStatus accepts a tool-status report that answers a tool instruction
// issued on its connection, within the byte limits that the issued tool
// sets, and marks a final one (a terminal or rejected execution report, a
// control's reply) as reported, so that a resume does not write its
// instruction again.
func (server *Server) toolStatus(w http.ResponseWriter, request *http.Request) {
	server.serve(w, request, endpoint{
		name: "tool-status", maxBytes: wire.MediaToolStatusJSONBytes, replayable: true,
		decode: func(body []byte) (string, string, any, error) {
			report, err := wire.Decode[wire.ToolStatus](body)
			return string(report.ConnectionID), string(report.MessageID), &report, err
		},
		apply: func(conn *connection, value any, body []byte) (answer, *refusal) {
			report := value.(*wire.ToolStatus)
			instruction := conn.issued[string(report.RequestMessageID)]
			if instruction == nil || !answersInstruction(report, instruction) {
				return answer{}, refuse("unknown_instruction", "the report answers no tool instruction issued on this connection")
			}
			if refused := reportSizes(report, instruction.tool, body); refused != nil {
				return answer{}, refused
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
// instruction it names and has the kind that answers it, an execution
// result of the tool that was issued.
func answersInstruction(report *wire.ToolStatus, instruction *issuedInstruction) bool {
	kind := instruction.kind == "tool.execute" && report.Kind == "execution" ||
		(instruction.kind == "tool.stdin" || instruction.kind == "tool.cancel") && report.Kind == "control"
	tool := report.Kind != "execution" || !report.Result.Set || report.Result.Value.Tool == instruction.tool
	return kind && tool && instruction.sessionID == string(report.SessionID) &&
		instruction.bindingID == string(report.BindingID) &&
		instruction.toolUseEventID == string(report.ToolUseEventID) &&
		instruction.taskID == string(report.TaskID)
}

// reportSizes checks the byte limits of a tool-status body within the media
// limit, which only a report of an issued `view_image` may use: a complete
// ordinary report over its limit is `413 request_too_large`; a result
// envelope (for `view_image`, with its `data_base64` emptied), metadata (the
// report with its result replaced by `{}`) or an image ([imageRefusal]) over
// its own limit is `400 invalid_message`. Each size is of the compact JSON as
// written.
func reportSizes(report *wire.ToolStatus, tool string, body []byte) *refusal {
	complete := compactLen(body)
	var members map[string]json.RawMessage
	_ = json.Unmarshal(body, &members)
	result, hasResult := members["result"]
	resultLen := compactLen(result)
	media := hasResult && tool == "view_image"
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
	if image, ok := report.Result.Value.Result.(*wire.MediaResult); media && ok {
		return imageRefusal(image)
	}
	return nil
}

// imageRefusal checks a view_image result's image, whose declared size is
// never trusted alone: `data_base64` in the standard alphabet without line
// breaks, at most `ViewImageDecodedBytes` decoded, and as many bytes as
// `size_bytes`.
func imageRefusal(image *wire.MediaResult) *refusal {
	decoded, err := base64.StdEncoding.Strict().DecodeString(image.DataBase64)
	switch {
	case err != nil || strings.ContainsAny(image.DataBase64, "\r\n"):
		return refuse("invalid_message", "the image's data_base64 is not standard Base64")
	case len(decoded) > wire.ViewImageDecodedBytes:
		return refuse("invalid_message", "the decoded image is over its limit")
	case int64(len(decoded)) != image.SizeBytes:
		return refuse("invalid_message", "size_bytes differs from the decoded image")
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
