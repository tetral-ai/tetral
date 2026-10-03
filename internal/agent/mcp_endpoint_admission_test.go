package agent

import (
	"encoding/json"
	"testing"
)

func TestMCPEndpointAdmissionMatchesRegisteredAdapters(t *testing.T) {
	for _, test := range []struct {
		name, endpoint, canonical string
	}{
		{"work-github", "https://api.githubcopilot.com/mcp/", "https://api.githubcopilot.com/mcp/"},
		{"work-github", "https://api.githubcopilot.com/mcp", "https://api.githubcopilot.com/mcp/"},
		{"work-slack", "https://mcp.slack.com/mcp", "https://mcp.slack.com/mcp"},
		{"work-slack", "https://mcp.slack.com/mcp/", "https://mcp.slack.com/mcp"},
	} {
		t.Run(test.name+"/"+test.endpoint, func(t *testing.T) {
			body := mcpAdmissionBody(t, test.name, test.endpoint)
			created, err := DecodeCreateAgentRequest(body)
			if err != nil {
				t.Fatalf("create supported endpoint: %v", err)
			}
			var server mcpServerURLEntry
			if err := json.Unmarshal(created.MCPServers[0], &server); err != nil {
				t.Fatal(err)
			}
			if server.Name != test.name || server.URL != test.canonical {
				t.Fatalf("admitted server = %+v", server)
			}
			var patch AgentPatch
			patchBody, _ := json.Marshal(map[string]any{"version": 1, "mcp_servers": []mcpServerURLEntry{{Type: "url", Name: test.name, URL: test.endpoint}}})
			if err := json.Unmarshal(patchBody, &patch); err != nil {
				t.Fatal(err)
			}
			updated, err := patch.Materialize(created.AgentConfig)
			if err != nil {
				t.Fatalf("update supported endpoint: %v", err)
			}
			if string(updated.MCPServers[0]) != string(created.MCPServers[0]) {
				t.Fatalf("create/update canonical endpoint differs")
			}
		})
	}
	for _, endpoint := range []string{
		"https://example.com/mcp", "https://mcp.slack.com/other", "http://mcp.slack.com/mcp",
		"https://mcp.slack.com/mcp?token=selector", "https://mcp.slack.com/mcp#fragment",
		"https://user:password@mcp.slack.com/mcp", "https://mcp.slack.com.evil.example/mcp",
		"https://api.githubcopilot.com/mcp/extra", "https://127.0.0.1/mcp",
	} {
		t.Run("rejected/"+endpoint, func(t *testing.T) {
			if _, err := DecodeCreateAgentRequest(mcpAdmissionBody(t, "work-slack", endpoint)); err == nil {
				t.Fatal("unsupported endpoint admitted")
			}
		})
	}
}

func mcpAdmissionBody(t *testing.T, name, endpoint string) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"name": "adapter admission", "model": "openai/gpt-5.5",
		"tools":       []map[string]string{{"type": "tetral_agent_toolset", "family": "gpt"}, {"type": "mcp_toolset", "mcp_server_name": name}},
		"mcp_servers": []mcpServerURLEntry{{Type: "url", Name: name, URL: endpoint}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return body
}
