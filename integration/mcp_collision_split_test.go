package integration

import (
	"context"
	"database/sql"
	"testing"
)

func seedMCPFamilySession(t *testing.T, admin *sql.DB, sessionID string, threadID string, family string) {
	t.Helper()
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIAgentConfig(t, admin, "default", sessionID, `{"name":"agent","model":"anthropic/claude-opus-4-8","tools":[{"type":"mcp_toolset","mcp_server_name":"github"}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}],"skills":[],"metadata":{}}`)
	installed := `{"tools":[{"type":"tetral_agent_toolset","family":"` + family + `"},{"type":"mcp_toolset","mcp_server_name":"github"}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}]}`
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET installed_tools_json = $1 WHERE workspace_id = 'default' AND id = $2`, installed, sessionID); err != nil {
		t.Fatalf("seed installed MCP tools: %v", err)
	}
}
