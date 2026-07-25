package agent

import (
	"encoding/json"
	"strings"
	"testing"
)

func crossArrayConfig(toolsRaw []string, serversRaw []string) AgentConfig {
	cfg := AgentConfig{Name: "x", Model: "anthropic/claude-opus-4-8"}.Normalize()
	for _, tool := range toolsRaw {
		cfg.Tools = append(cfg.Tools, json.RawMessage(tool))
	}
	for _, server := range serversRaw {
		cfg.MCPServers = append(cfg.MCPServers, json.RawMessage(server))
	}
	return cfg
}

func TestValidateCrossArrayHappyPath(t *testing.T) {
	cfg := crossArrayConfig(
		[]string{
			`{"type":"tetral_agent_toolset","family":"claude"}`,
			`{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"enabled":true,"permission_policy":{"type":"always_ask"}}}`,
		},
		[]string{`{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}`},
	)
	if err := ValidateCrossArray(cfg); err != nil {
		t.Errorf("ValidateCrossArray(happy) = %v; want nil", err)
	}
}

func TestValidateCrossArrayRejectsMissingMCPReference(t *testing.T) {
	cfg := crossArrayConfig(
		[]string{`{"type":"mcp_toolset","mcp_server_name":"github"}`},
		nil,
	)
	err := ValidateCrossArray(cfg)
	if err == nil {
		t.Fatal("must reject missing reference")
	}
	if !strings.Contains(err.Error(), "github") {
		t.Errorf("error %q must name the missing reference", err.Error())
	}
}

func TestValidateCrossArrayRejectsDuplicateMCPServerName(t *testing.T) {
	cfg := crossArrayConfig(
		nil,
		[]string{
			`{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}`,
			`{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}`,
		},
	)
	err := ValidateCrossArray(cfg)
	if err == nil {
		t.Fatal("must reject duplicate mcp_servers name")
	}
	if !strings.Contains(err.Error(), "github") {
		t.Errorf("error %q must name the duplicate", err.Error())
	}
}

func TestValidateCrossArrayAllowsUnreferencedMCPServer(t *testing.T) {
	cfg := crossArrayConfig(nil, []string{`{"type":"url","name":"orphan","url":"https://api.githubcopilot.com/mcp/"}`})
	if err := ValidateCrossArray(cfg); err != nil {
		t.Errorf("ValidateCrossArray must allow unreferenced server; got %v", err)
	}
}

func TestValidateCrossArrayRejectsDuplicateSkillIDs(t *testing.T) {
	cfg := crossArrayConfig(nil, nil)
	cfg.Skills = RawArray{
		json.RawMessage(`{"type":"custom","skill_id":"skill_dup","version":"latest"}`),
		json.RawMessage(`{"type":"custom","skill_id":"skill_dup","version":"1759178010641129"}`),
	}
	err := ValidateCrossArray(cfg)
	if err == nil {
		t.Fatal("duplicate skill_id must reject")
	}
	if !strings.Contains(err.Error(), "skill_dup") {
		t.Errorf("error %q must name the duplicated skill_id", err.Error())
	}
}
