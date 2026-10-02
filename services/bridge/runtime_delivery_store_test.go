package agentruntimebridge

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/mcpmanifest"
	"github.com/tetral-ai/tetral/internal/runtimeconfig"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

func TestManifestServerSetIsSubsetOfColdToolPolicyMCPToolsets(t *testing.T) {
	const sessionID = "sesn_manifest_policy_subset"
	const agentConfig = `{"name":"agent","model":"anthropic/claude-opus-4-8","tools":[],"mcp_servers":[],"skills":[],"metadata":{}}`
	const installedConfig = `{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"github"}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"},{"type":"url","name":"unused","url":"https://unused.example/mcp/"}]}`
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", sessionID, "thr_manifest_policy_subset")
	seedBridgeAPIAgentConfig(t, admin, "default", sessionID, agentConfig)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE sessions SET installed_tools_json = $1 WHERE workspace_id = 'default' AND id = $2`, installedConfig, sessionID); err != nil {
		t.Fatalf("seed installed tool policy: %v", err)
	}

	var manifestToolsets []mcpmanifest.ToolsetConfig
	client := dbconnect.NewClientForTesting(runtime)
	if err := client.WithWorkspaceTx(context.Background(), "default", "test.manifest_policy_subset", func(tx *dbconnect.Tx) error {
		var err error
		manifestToolsets, err = mcpmanifest.SessionToolsetsTx(context.Background(), tx, "default", sessionID)
		return err
	}); err != nil {
		t.Fatalf("read manifest toolsets: %v", err)
	}
	config, err := runtimeconfig.InstalledConfig(installedConfig)
	if err != nil {
		t.Fatalf("resolve cold tool policy: %v", err)
	}
	policyJSON, err := json.Marshal(runtimeconfig.ToolPolicy("approve_for_me", config))
	if err != nil {
		t.Fatalf("marshal cold tool policy: %v", err)
	}
	var policy struct {
		MCPToolsets []struct {
			MCPServerName string `json:"mcpServerName"`
		} `json:"mcpToolsets"`
	}
	if err := json.Unmarshal(policyJSON, &policy); err != nil {
		t.Fatalf("decode cold tool policy: %v", err)
	}
	held := make(map[string]bool, len(policy.MCPToolsets))
	for _, toolset := range policy.MCPToolsets {
		held[toolset.MCPServerName] = true
	}
	for _, toolset := range manifestToolsets {
		if !held[toolset.MCPServerName] {
			t.Fatalf("manifest server %q is absent from cold policy MCP toolsets %v", toolset.MCPServerName, held)
		}
	}
	if len(manifestToolsets) != 1 || manifestToolsets[0].MCPServerName != "github" || held["unused"] {
		t.Fatalf("manifest toolsets=%+v held=%v; want only declared github toolset", manifestToolsets, held)
	}
}
