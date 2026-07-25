package agentruntimebridge

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// Runtime agent config application is ACK-gated across the Bridge/Runtime Pod
// boundary. Bridge applies a config update by sending the Runtime Pod
// control-path ApplyRuntimeConfig command; the installed snapshot decoded here
// is what a new config_generation carries. Application state table:
//
//	state        meaning                              writer / gate            legal next
//	delivered    ApplyRuntimeConfig command sent      Bridge Job Runner        installed
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
type bridgeRuntimeAgentConfig struct {
	Tools      []json.RawMessage `json:"tools"`
	MCPServers []json.RawMessage `json:"mcp_servers"`
}

type bridgeRuntimeSessionAgentSettingsPayload struct {
	System       *string
	MemoryStores []bridgeRuntimeMemoryStore
	ToolPolicy   map[string]any
	Config       bridgeRuntimeAgentConfig
}

// bridgeRuntimeSessionAgentSettings is the sole serializer input for both
// cold LoadContext and hot runtime_config_update delivery. Keeping the two
// carriers on one decoded value prevents config generations from installing
// a tool policy and agent instruction from different durable snapshots.
func bridgeRuntimeSessionAgentSettings(
	approvalMode string,
	agentConfigJSON string,
	installedToolsJSON string,
	memoryStores []bridgeRuntimeMemoryStore,
) (bridgeRuntimeSessionAgentSettingsPayload, error) {
	config, err := effectiveBridgeRuntimeAgentConfig(agentConfigJSON, installedToolsJSON)
	if err != nil {
		return bridgeRuntimeSessionAgentSettingsPayload{}, err
	}
	system, err := bridgeRuntimeAgentSystem(agentConfigJSON)
	if err != nil {
		return bridgeRuntimeSessionAgentSettingsPayload{}, err
	}
	return bridgeRuntimeSessionAgentSettingsPayload{
		System:       system,
		MemoryStores: memoryStores,
		ToolPolicy:   bridgeRuntimeToolPolicy(approvalMode, config),
		Config:       config,
	}, nil
}

func bridgeRuntimeAgentSystem(agentConfigJSON string) (*string, error) {
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

func bridgeInstalledBuiltinFamily(config bridgeRuntimeAgentConfig) (string, error) {
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

func effectiveBridgeRuntimeAgentConfig(_ string, installedToolsJSON string) (bridgeRuntimeAgentConfig, error) {
	installedConfig, _, err := decodeInstalledRuntimeAgentConfig(installedToolsJSON)
	if err != nil {
		return bridgeRuntimeAgentConfig{}, err
	}
	return normalizeBridgeRuntimeAgentConfig(installedConfig), nil
}

func decodeInstalledRuntimeAgentConfig(raw string) (bridgeRuntimeAgentConfig, bool, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || !strings.HasPrefix(trimmed, "{") {
		return bridgeRuntimeAgentConfig{}, false, fmt.Errorf("installed tools snapshot must be an object")
	}
	config, err := decodeBridgeRuntimeAgentConfigObject(trimmed)
	if err != nil {
		return bridgeRuntimeAgentConfig{}, false, err
	}
	if _, err := bridgeInstalledBuiltinFamily(config); err != nil {
		return bridgeRuntimeAgentConfig{}, false, err
	}
	return normalizeBridgeRuntimeAgentConfig(config), true, nil
}

func decodeBridgeRuntimeAgentConfigObject(raw string) (bridgeRuntimeAgentConfig, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &fields); err != nil {
		return bridgeRuntimeAgentConfig{}, err
	}
	if fields == nil {
		return bridgeRuntimeAgentConfig{}, fmt.Errorf("runtime agent config must be an object")
	}
	var config bridgeRuntimeAgentConfig
	rawTools, ok := fields["tools"]
	if !ok {
		return bridgeRuntimeAgentConfig{}, fmt.Errorf("runtime agent config lacks tools")
	}
	if err := json.Unmarshal(rawTools, &config.Tools); err != nil || config.Tools == nil {
		return bridgeRuntimeAgentConfig{}, fmt.Errorf("runtime agent config tools must be an array")
	}
	if rawServers, ok := fields["mcp_servers"]; ok {
		if err := json.Unmarshal(rawServers, &config.MCPServers); err != nil || config.MCPServers == nil {
			return bridgeRuntimeAgentConfig{}, fmt.Errorf("runtime agent config mcp_servers must be an array")
		}
	}
	return normalizeBridgeRuntimeAgentConfig(config), nil
}

func normalizeBridgeRuntimeAgentConfig(config bridgeRuntimeAgentConfig) bridgeRuntimeAgentConfig {
	if config.Tools == nil {
		config.Tools = []json.RawMessage{}
	}
	if config.MCPServers == nil {
		config.MCPServers = []json.RawMessage{}
	}
	return config
}

func bridgeRuntimeToolPolicy(approvalMode string, config bridgeRuntimeAgentConfig) map[string]any {
	config = normalizeBridgeRuntimeAgentConfig(config)
	return map[string]any{
		"approvalMode": approvalMode,
		"mcpServers":   config.MCPServers,
		"mcpToolsets":  bridgeRuntimeMCPToolsetsPayload(config.Tools),
	}
}

func bridgeRuntimeMCPToolsetsPayload(tools []json.RawMessage) []map[string]any {
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
