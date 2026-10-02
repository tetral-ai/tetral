package runtimeconfig

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// Runtime agent config application is ACK-gated across the Bridge/Runtime Pod
// boundary. Job Runner applies a config update by sending the Runtime Pod
// control-path ApplyRuntimeConfig command; the installed snapshot decoded here
// is what a new config_generation carries. Application state table:
//
//	state        meaning                              writer / gate            legal next
//	delivered    ApplyRuntimeConfig command sent      Job Runner        installed
//	installed    new config_generation in idle hot    Runtime Pod (only then   effective
//	             state; Runtime ACKs                   does it ACK)
//	effective    policy governs FUTURE stable tool    Runtime Pod              (terminal for
//	             calls after the ACK (or a later                               this generation)
//	             cold LoadContext)
//
// INVARIANT: the new policy never reinterprets the in-flight request turn,
// already-emitted agent.tool_use events, pending approvals, pending waits, or
// running tool fibers — it binds only stable tool calls that start after the
// ACK. Runtime does not ACK until the new config_generation is installed into
// idle hot state, so a losing/racing update can never take effect mid-turn.
type AgentConfig struct {
	Tools      []json.RawMessage `json:"tools"`
	MCPServers []json.RawMessage `json:"mcp_servers"`
}

type SessionSettings struct {
	System       *string
	MemoryStores []MemoryStore
	ToolPolicy   map[string]any
	Config       AgentConfig
}

// InterpretSessionSettings is the sole serializer input for cold LoadContext
// and hot runtime_config_update delivery. The caller reads the Session-pinned
// agent_version.config_json for system instructions, installed_tools_json for
// the current family and MCP policy, and attached memory resources in its own
// transaction. Neither delivery path substitutes the original agent's tools
// for the installed policy.
func InterpretSessionSettings(
	approvalMode string,
	agentConfigJSON string,
	installedToolsJSON string,
	memoryStores []MemoryStore,
) (SessionSettings, error) {
	config, err := InstalledConfig(installedToolsJSON)
	if err != nil {
		return SessionSettings{}, err
	}
	system, err := agentSystem(agentConfigJSON)
	if err != nil {
		return SessionSettings{}, err
	}
	return SessionSettings{
		System:       system,
		MemoryStores: memoryStores,
		ToolPolicy:   ToolPolicy(approvalMode, config),
		Config:       config,
	}, nil
}

func agentSystem(agentConfigJSON string) (*string, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(agentConfigJSON), &fields); err != nil || fields == nil {
		return nil, fmt.Errorf("runtime agent config must be an object")
	}
	raw, ok := fields["system"]
	if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}
	var system string
	if err := json.Unmarshal(raw, &system); err != nil || system == "" {
		return nil, fmt.Errorf("runtime agent config system must be a non-empty string or null")
	}
	return &system, nil
}

func InstalledBuiltinFamily(config AgentConfig) (string, error) {
	family := ""
	for index, raw := range config.Tools {
		var entry struct {
			Type   string `json:"type"`
			Family string `json:"family"`
		}
		if err := json.Unmarshal(raw, &entry); err != nil {
			return "", err
		}
		if entry.Type != "tetral_agent_toolset" {
			continue
		}
		if family != "" {
			return "", fmt.Errorf("tools[%d] duplicates tetral_agent_toolset", index)
		}
		if entry.Family != "claude" && entry.Family != "gpt" {
			return "", fmt.Errorf("tools[%d].family is invalid", index)
		}
		family = entry.Family
	}
	if family == "" {
		return "", fmt.Errorf("tools lacks tetral_agent_toolset")
	}
	return family, nil
}

// InstalledConfig validates the installed tool authority without falling back
// to the original agent's tool declarations.
func InstalledConfig(raw string) (AgentConfig, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || !strings.HasPrefix(trimmed, "{") {
		return AgentConfig{}, fmt.Errorf("installed tools snapshot must be an object")
	}
	config, err := decodeAgentConfigObject(trimmed)
	if err != nil {
		return AgentConfig{}, err
	}
	if _, err := InstalledBuiltinFamily(config); err != nil {
		return AgentConfig{}, err
	}
	return normalizeAgentConfig(config), nil
}

func decodeAgentConfigObject(raw string) (AgentConfig, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &fields); err != nil {
		return AgentConfig{}, err
	}
	if fields == nil {
		return AgentConfig{}, fmt.Errorf("runtime agent config must be an object")
	}
	var config AgentConfig
	rawTools, ok := fields["tools"]
	if !ok {
		return AgentConfig{}, fmt.Errorf("runtime agent config lacks tools")
	}
	if err := json.Unmarshal(rawTools, &config.Tools); err != nil || config.Tools == nil {
		return AgentConfig{}, fmt.Errorf("runtime agent config tools must be an array")
	}
	if rawServers, ok := fields["mcp_servers"]; ok {
		if err := json.Unmarshal(rawServers, &config.MCPServers); err != nil || config.MCPServers == nil {
			return AgentConfig{}, fmt.Errorf("runtime agent config mcp_servers must be an array")
		}
	}
	return normalizeAgentConfig(config), nil
}

func normalizeAgentConfig(config AgentConfig) AgentConfig {
	if config.Tools == nil {
		config.Tools = []json.RawMessage{}
	}
	if config.MCPServers == nil {
		config.MCPServers = []json.RawMessage{}
	}
	return config
}

func ToolPolicy(approvalMode string, config AgentConfig) map[string]any {
	config = normalizeAgentConfig(config)
	return map[string]any{
		"approvalMode": approvalMode,
		"mcpServers":   config.MCPServers,
		"mcpToolsets":  mcpToolsetsPayload(config.Tools),
	}
}

func mcpToolsetsPayload(tools []json.RawMessage) []map[string]any {
	toolsets := []map[string]any{}
	for _, raw := range tools {
		var entry struct {
			Type          string           `json:"type"`
			MCPServerName string           `json:"mcp_server_name"`
			DefaultConfig map[string]any   `json:"default_config"`
			Configs       []map[string]any `json:"configs"`
		}
		if err := json.Unmarshal(raw, &entry); err != nil {
			continue
		}
		if entry.Type != "mcp_toolset" || entry.MCPServerName == "" {
			continue
		}
		toolset := map[string]any{"mcpServerName": entry.MCPServerName}
		if entry.DefaultConfig != nil {
			toolset["defaultConfig"] = entry.DefaultConfig
		}
		if len(entry.Configs) > 0 {
			toolset["configs"] = entry.Configs
		}
		toolsets = append(toolsets, toolset)
	}
	return toolsets
}
