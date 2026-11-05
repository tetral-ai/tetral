package integration

import (
	"bytes"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/integration/enginetest"
	"github.com/tetral-ai/tetral/internal/protocol"
)

// Tests of POST /v1/client/mcp-tools: tool list reports of a local MCP
// server from a test CLI over real HTTP to computer-hub, stored per instance
// and server in PostgreSQL.

// mcpToolsReport is the JSON of a tool list report of server on
// connectionID with seq, listing a tool of each name, with members extra
// after error.
func mcpToolsReport(connectionID, messageID, server string, seq int, names []string, extra string) []byte {
	tools := make([]string, 0, len(names))
	for _, name := range names {
		tools = append(tools, `{"name":`+quoteJSON(name)+`,"inputSchema":{"type":"object"}}`)
	}
	return []byte(`{"protocol_version":1,"connection_id":` + quoteJSON(connectionID) + `,"message_id":` + quoteJSON(messageID) +
		`,"server":` + quoteJSON(server) + `,"seq":` + strconv.Itoa(seq) + `,"server_info":{"name":"cua","title":null,"version":"1.0.0"},"tools":[` +
		strings.Join(tools, ",") + `],"error":null` + extra + `}`)
}

// padded is report with a padding member that makes it exactly size bytes.
func padded(t *testing.T, connectionID, messageID, server string, seq, size int) []byte {
	t.Helper()
	bare := mcpToolsReport(connectionID, messageID, server, seq, []string{"click"}, `,"padding":""`)
	if len(bare) > size {
		t.Fatalf("report of %d bytes is over %d already", len(bare), size)
	}
	return mcpToolsReport(connectionID, messageID, server, seq, []string{"click"}, `,"padding":"`+strings.Repeat("x", size-len(bare))+`"`)
}

// mcpTools posts body as cli through hub server hub.
func (f *machineTools) mcpTools(hub int, cli *enginetest.CLI, body []byte) enginetest.Response {
	f.t.Helper()
	at := *cli
	at.BaseURL = f.hubs[hub]
	response, err := at.Post(f.ctx, "/v1/client/mcp-tools", body)
	if err != nil {
		f.t.Fatal(err)
	}
	return response
}

// mcpToolsAcked posts body through hub and requires its ACK for messageID
// on connectionID.
func (f *machineTools) mcpToolsAcked(hub int, cli *enginetest.CLI, body []byte, connectionID, messageID string) enginetest.Response {
	f.t.Helper()
	response := f.mcpTools(hub, cli, body)
	if ack, err := response.Acknowledgement(); err != nil || ack.MessageID != messageID || ack.ConnectionID != connectionID {
		f.t.Fatalf("tool list report %s = %d %s, %v; want its ACK", messageID, response.Status, response.Body, err)
	}
	return response
}

// mcpToolsRefused posts body through hub and requires status and code.
func (f *machineTools) mcpToolsRefused(hub int, cli *enginetest.CLI, body []byte, status int, code protocol.ErrorCode) {
	f.t.Helper()
	response := f.mcpTools(hub, cli, body)
	if failure, err := response.Error(); response.Status != status || err != nil || failure.Code != code {
		f.t.Fatalf("tool list report = %d %s; want %d %s", response.Status, response.Body, status, code)
	}
}

// toolList is the stored row of instanceID's server, every column as text,
// or nil when there is none.
func (f *machineTools) toolList(instanceID, server string) map[string]any {
	f.t.Helper()
	if f.count(`SELECT count(*) FROM instance_mcp_tool_lists WHERE instance_id=$1 AND server=$2`, instanceID, server) == 0 {
		return nil
	}
	return f.row(`SELECT connection_id, seq::text, server_info_json, tools_json, error_json, reported_at::text
		FROM instance_mcp_tool_lists WHERE instance_id=$1 AND server=$2`, instanceID, server)
}

// The instance's stream lives on the first hub server and every report goes
// to the second, which stores it. A report replaces the server's row only
// when it is newer on the same connection or comes from another one; a
// lower seq, a stale connection, a body over 4,194,304 bytes (whitespace
// included) and a tool named twice leave the row byte-identical, and only
// the lower seq gets an ACK. A report sent again gets its first ACK; its
// message_id with other content gets 409 message_conflict.
func TestAToolListReplacesItsRowOnlyWhenNewerOrFromAnotherConnection(t *testing.T) {
	f := newMachineTools(t)
	cli := f.userInstance("laptop")
	first := f.connect(cli, everyTool)

	report := mcpToolsReport(first.connectionID, "mcp_57", "cua", 57, []string{"click", "type"}, "")
	ack := f.mcpToolsAcked(1, cli, report, first.connectionID, "mcp_57")
	stored := f.toolList(cli.InstanceID, "cua")
	if stored["connection_id"] != first.connectionID || stored["seq"] != "57" ||
		stored["tools_json"] != `[{"name":"click","inputSchema":{"type":"object"}},{"name":"type","inputSchema":{"type":"object"}}]` ||
		stored["server_info_json"] != `{"name":"cua","title":null,"version":"1.0.0"}` || stored["error_json"] != "null" {
		t.Fatalf("stored list = %v; want the report's", stored)
	}
	if again := f.mcpToolsAcked(1, cli, report, first.connectionID, "mcp_57"); !bytes.Equal(again.Body, ack.Body) {
		t.Fatalf("second ACK %s; want the first %s", again.Body, ack.Body)
	}
	f.mcpToolsRefused(1, cli, mcpToolsReport(first.connectionID, "mcp_57", "cua", 58, []string{"click"}, ""), http.StatusConflict, protocol.ErrorMessageConflict)

	unchanged := func(step string) {
		t.Helper()
		if got := f.toolList(cli.InstanceID, "cua"); !reflect.DeepEqual(got, stored) {
			t.Fatalf("list after %s = %v; want %v", step, got, stored)
		}
	}
	unchanged("the conflict")
	f.mcpToolsAcked(1, cli, mcpToolsReport(first.connectionID, "mcp_12", "cua", 12, nil, ""), first.connectionID, "mcp_12")
	unchanged("a lower seq")
	f.mcpToolsRefused(1, cli, append(padded(t, first.connectionID, "mcp_big", "cua", 90, protocol.McpToolsJSONBytes), ' '), http.StatusRequestEntityTooLarge, protocol.ErrorRequestTooLarge)
	unchanged("a body over the limit")
	f.mcpToolsRefused(1, cli, mcpToolsReport(first.connectionID, "mcp_twice", "cua", 91, []string{"click", "click"}, ""), http.StatusBadRequest, protocol.ErrorInvalidMessage)
	unchanged("a tool named twice")

	f.exec(`UPDATE instance_connections SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, first.connectionID)
	second := f.connect(cli, everyTool)
	unchanged("the connection's end")
	f.mcpToolsRefused(1, cli, mcpToolsReport(first.connectionID, "mcp_old", "cua", 92, []string{"click"}, ""), http.StatusConflict, protocol.ErrorStaleConnection)
	unchanged("a report on the old connection")

	f.mcpToolsAcked(1, cli, mcpToolsReport(second.connectionID, "mcp_1", "cua", 1, []string{"scroll"}, ""), second.connectionID, "mcp_1")
	if got := f.toolList(cli.InstanceID, "cua"); got["connection_id"] != second.connectionID || got["seq"] != "1" ||
		got["tools_json"] != `[{"name":"scroll","inputSchema":{"type":"object"}}]` {
		t.Fatalf("list after the new connection's report = %v; want its connection, seq 1 and its list", got)
	}
	f.mcpToolsAcked(1, cli, padded(t, second.connectionID, "mcp_limit", "cua", 2, protocol.McpToolsJSONBytes), second.connectionID, "mcp_limit")
	if got := f.toolList(cli.InstanceID, "cua"); got["seq"] != "2" {
		t.Fatalf("list after a report of exactly the limit = %v; want seq 2", got)
	}
}

// DELETE /v1/instances/{instance_id} deletes the instance's tool lists in
// the transaction that revokes its credentials: when the revocation fails,
// the rows and the registration stay; when the deletion fails, the
// credentials and the registration stay; when it commits, the rows are gone.
func TestDeregistrationDeletesTheInstancesToolLists(t *testing.T) {
	f := newMachineTools(t)
	cli := f.userInstance("laptop")
	stream := f.connect(cli, everyTool)
	for i, server := range []string{"cua", "files"} {
		messageID := "mcp_" + server
		f.mcpToolsAcked(0, cli, mcpToolsReport(stream.connectionID, messageID, server, i+1, []string{"click"}, ""), stream.connectionID, messageID)
	}

	f.exec(`CREATE FUNCTION fail_credential_revocation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected credential revocation failure'; END $$;
		CREATE TRIGGER fail_credential_revocation BEFORE UPDATE ON instance_credentials FOR EACH ROW EXECUTE FUNCTION fail_credential_revocation()`)
	if status, body := f.controller(http.MethodDelete, "/v1/instances/"+cli.InstanceID, ""); status != http.StatusServiceUnavailable {
		t.Fatalf("DELETE with a failing revocation = %d %s; want 503", status, body)
	}
	if lists, state := f.count(`SELECT count(*) FROM instance_mcp_tool_lists WHERE instance_id=$1`, cli.InstanceID),
		f.row(`SELECT registration_state FROM instances WHERE id=$1`, cli.InstanceID)["registration_state"]; lists != 2 || state != "active" {
		t.Fatalf("after the failed deregistration: %d lists, instance %v; want 2 and active", lists, state)
	}

	f.exec(`DROP TRIGGER fail_credential_revocation ON instance_credentials`)
	f.exec(`CREATE FUNCTION fail_tool_list_deletion() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected tool list deletion failure'; END $$;
		CREATE TRIGGER fail_tool_list_deletion BEFORE DELETE ON instance_mcp_tool_lists FOR EACH ROW EXECUTE FUNCTION fail_tool_list_deletion()`)
	if status, body := f.controller(http.MethodDelete, "/v1/instances/"+cli.InstanceID, ""); status != http.StatusServiceUnavailable {
		t.Fatalf("DELETE with a failing tool list deletion = %d %s; want 503", status, body)
	}
	if revoked, state := f.count(`SELECT count(*) FROM instance_credentials WHERE instance_id=$1 AND revoked_at IS NOT NULL`, cli.InstanceID),
		f.row(`SELECT registration_state FROM instances WHERE id=$1`, cli.InstanceID)["registration_state"]; revoked != 0 || state != "active" {
		t.Fatalf("after the failed deletion: %d credentials revoked, instance %v; want none and active", revoked, state)
	}

	f.exec(`DROP TRIGGER fail_tool_list_deletion ON instance_mcp_tool_lists`)
	f.deregister(cli.InstanceID)
	if lists := f.count(`SELECT count(*) FROM instance_mcp_tool_lists WHERE instance_id=$1`, cli.InstanceID); lists != 0 {
		t.Fatalf("%d lists after deregistration; want none", lists)
	}
}
