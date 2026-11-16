package reference

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"slices"
	"strings"

	"github.com/tetral-ai/tetral/cli/reference/wire"
)

// toolListKey names one row of tool lists: an instance's local server.
type toolListKey struct {
	instanceID string
	server     string
}

// toolList is the row of one instance's local server: the report that
// last replaced it.
type toolList struct {
	connectionID string
	seq          int64
	body         json.RawMessage
}

// ToolListState is a row of tool lists in the state.
type ToolListState struct {
	InstanceID   string          `json:"instance_id"`
	Server       string          `json:"server"`
	ConnectionID string          `json:"connection_id"`
	Seq          int64           `json:"seq"`
	Report       json.RawMessage `json:"report"`
}

// mcpTools accepts a local server's tool-list report from the instance's
// live connection: a body over 4,194,304 bytes is `413 request_too_large`,
// one that fails the schema or names a tool twice `400 invalid_message`.
// The report replaces its row when the row is absent or came from another
// connection, or when its `seq` is higher than the row's; otherwise it is
// acknowledged and changes nothing.
func (server *Server) mcpTools(w http.ResponseWriter, request *http.Request) {
	server.serve(w, request, endpoint{
		name: "mcp-tools", maxBytes: wire.ToolListMaxBytes, replayable: true,
		decode: func(body []byte) (string, string, any, error) {
			report, err := wire.Decode[wire.MCPTools](body)
			return string(report.ConnectionID), string(report.MessageID), &report, err
		},
		apply: func(conn *connection, value any, body []byte) (answer, *refusal) {
			report := value.(*wire.MCPTools)
			key := toolListKey{instanceID: conn.instance.id, server: string(report.Server)}
			row := server.toolLists[key]
			if row == nil || row.connectionID != conn.id || report.Seq > row.seq {
				server.toolLists[key] = &toolList{connectionID: conn.id, seq: report.Seq, body: canonical(body)}
			}
			return server.acknowledge(conn, string(report.MessageID)), nil
		},
	})
}

// toolListStates is every row of tool lists, by instance and server.
func (server *Server) toolListStates() []ToolListState {
	var rows []ToolListState
	for key, row := range server.toolLists {
		rows = append(rows, ToolListState{
			InstanceID: key.instanceID, Server: key.server, ConnectionID: row.connectionID,
			Seq: row.seq, Report: row.body,
		})
	}
	slices.SortFunc(rows, func(a, b ToolListState) int {
		return strings.Compare(a.InstanceID+"/"+a.Server, b.InstanceID+"/"+b.Server)
	})
	return rows
}

// mcpMedia is the media strings of an `mcp` result: every `data` of an
// `image` or `audio` block and every `blob` of a `resource` block in its
// `content`.
func mcpMedia(result *wire.MCPResult) []string {
	var media []string
	for _, block := range result.Content {
		var text any
		switch block["type"] {
		case "image", "audio":
			text = block["data"]
		case "resource":
			resource, _ := block["resource"].(map[string]any)
			text = resource["blob"]
		}
		if text, ok := text.(string); ok {
			media = append(media, text)
		}
	}
	return media
}

// mcpMediaRefusal decodes an `mcp` result's media strings strictly, as
// Engine does when it stores them: each in the standard alphabet with
// padding and no line break, together at most `ViewImageDecodedBytes`.
func mcpMediaRefusal(result *wire.MCPResult) *refusal {
	decoded := 0
	for _, text := range mcpMedia(result) {
		data, err := base64.StdEncoding.Strict().DecodeString(text)
		if err != nil || strings.ContainsAny(text, "\r\n") {
			return refuse("invalid_message", "an MCP media string is not standard Base64")
		}
		decoded += len(data)
	}
	if decoded > wire.ViewImageDecodedBytes {
		return refuse("invalid_message", "the decoded MCP media are over their limit")
	}
	return nil
}
