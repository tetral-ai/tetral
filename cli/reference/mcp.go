package reference

import (
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
			return acknowledge(conn, string(report.MessageID)), nil
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
