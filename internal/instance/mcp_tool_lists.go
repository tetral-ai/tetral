package instance

import (
	"context"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// MCPToolList is the stored tool list of one local MCP server of an
// instance, from the report that last replaced it; the JSON members are
// compact, "null" for a null server_info or error.
type MCPToolList struct {
	Server         string
	ConnectionID   string
	Seq            int64
	ServerInfoJSON string
	ToolsJSON      string
	ErrorJSON      string
	ReportedAt     time.Time
}

// ReportMCPToolList stores report, received on its connection_id at now, as
// the tool list of instanceID's server when no row holds that server, the
// row came from another connection, or the report's seq is higher than the
// row's, and reports whether it did. The statement locks the row it compares.
func ReportMCPToolList(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID string, report protocol.McpTools, now time.Time) (bool, error) {
	serverInfo, listError := "null", "null"
	if report.ServerInfo.Present {
		serverInfo = string(protocol.Encode(report.ServerInfo.Value))
	}
	if report.Error.Present {
		listError = string(protocol.Encode(report.Error.Value))
	}
	tools := make([]string, 0, len(report.Tools))
	for _, tool := range report.Tools {
		tools = append(tools, string(protocol.Encode(tool)))
	}
	result, err := tx.Exec(ctx, `INSERT INTO instance_mcp_tool_lists (workspace_id,instance_id,server,connection_id,seq,server_info_json,tools_json,error_json,reported_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (workspace_id,instance_id,server) DO UPDATE SET connection_id=EXCLUDED.connection_id,seq=EXCLUDED.seq,
		  server_info_json=EXCLUDED.server_info_json,tools_json=EXCLUDED.tools_json,error_json=EXCLUDED.error_json,reported_at=EXCLUDED.reported_at
		WHERE instance_mcp_tool_lists.connection_id<>EXCLUDED.connection_id OR instance_mcp_tool_lists.seq<EXCLUDED.seq`,
		string(ws), instanceID, report.Server, report.ConnectionID, report.Seq, serverInfo, "["+strings.Join(tools, ",")+"]", listError, now)
	if err != nil {
		return false, err
	}
	stored, err := result.RowsAffected()
	return stored == 1, err
}

// MCPToolLists reads the stored tool lists of instanceID, in byte order of
// server.
func MCPToolLists(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID string) ([]MCPToolList, error) {
	rows, err := tx.Query(ctx, `SELECT server,connection_id,seq,server_info_json,tools_json,error_json,reported_at FROM instance_mcp_tool_lists
		WHERE workspace_id=$1 AND instance_id=$2 ORDER BY server COLLATE "C"`, string(ws), instanceID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var lists []MCPToolList
	for rows.Next() {
		var list MCPToolList
		if err := rows.Scan(&list.Server, &list.ConnectionID, &list.Seq, &list.ServerInfoJSON, &list.ToolsJSON, &list.ErrorJSON, &list.ReportedAt); err != nil {
			return nil, err
		}
		lists = append(lists, list)
	}
	return lists, rows.Err()
}
