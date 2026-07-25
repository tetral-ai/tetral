// Package agent provides Agent control-plane domain types.
package agent

import (
	"encoding/json"
	"strconv"
	"time"
)

// RawArray is a JSON array slice that marshals nil as `[]`.
type RawArray []json.RawMessage

func (array RawArray) MarshalJSON() ([]byte, error) {
	if array == nil {
		return []byte("[]"), nil
	}
	return json.Marshal([]json.RawMessage(array))
}

// StringMap is a string-to-string JSON object that marshals nil as `{}`.
type StringMap map[string]string

func (metadata StringMap) MarshalJSON() ([]byte, error) {
	if metadata == nil {
		return []byte("{}"), nil
	}
	return json.Marshal(map[string]string(metadata))
}

type ApprovalMode string

const (
	ApprovalModeFullAccess     ApprovalMode = "full_access"
	ApprovalModeAskForApproval ApprovalMode = "ask_for_approval"
	ApprovalModeApproveForMe   ApprovalMode = "approve_for_me"
)

// AgentConfig is the immutable Agent snapshot stored in
// agent_versions.config_json. It contains only Agent-owned fields.
type AgentConfig struct {
	Name         string       `json:"name"`
	Model        string       `json:"model"`
	ModelVariant string       `json:"-"`
	ApprovalMode ApprovalMode `json:"approval_mode"`
	System       *string      `json:"system"`
	Description  *string      `json:"description"`
	Tools        RawArray     `json:"tools"`
	MCPServers   RawArray     `json:"mcp_servers"`
	Skills       RawArray     `json:"skills"`
	Metadata     StringMap    `json:"metadata"`
}

func (config AgentConfig) Normalize() AgentConfig {
	if config.ApprovalMode == "" {
		config.ApprovalMode = ApprovalModeAskForApproval
	}
	if config.Tools == nil {
		config.Tools = RawArray{}
	}
	if config.MCPServers == nil {
		config.MCPServers = RawArray{}
	}
	if config.Skills == nil {
		config.Skills = RawArray{}
	}
	if config.Metadata == nil {
		config.Metadata = StringMap{}
	}
	if config.System != nil && *config.System == "" {
		config.System = nil
	}
	if config.Description != nil && *config.Description == "" {
		config.Description = nil
	}
	return config
}

func (config *AgentConfig) UnmarshalJSON(data []byte) error {
	var decoded storedAgentConfig
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*config = AgentConfig{
		Name:         decoded.Name,
		Model:        decoded.Model,
		ApprovalMode: decoded.ApprovalMode,
		System:       decoded.System,
		Description:  decoded.Description,
		Tools:        decoded.Tools,
		MCPServers:   decoded.MCPServers,
		Skills:       decoded.Skills,
		Metadata:     decoded.Metadata,
	}.Normalize()
	return nil
}

// Agent is the flat public Agent resource.
type Agent struct {
	ID          string           `json:"id"`
	Type        string           `json:"type"`
	VersionID   string           `json:"-"`
	Version     int              `json:"version"`
	AgentConfig                  // embedded for flat JSON responses
	Multiagent  *json.RawMessage `json:"multiagent"`
	CreatedAt   time.Time        `json:"created_at"`
	UpdatedAt   time.Time        `json:"updated_at"`
	ArchivedAt  *time.Time       `json:"archived_at"`
}

type ModelConfig struct {
	ID    string `json:"id"`
	Speed string `json:"speed,omitempty"`
}

func (value Agent) MarshalJSON() ([]byte, error) {
	config := value.Normalize()
	tools, err := ProjectToolsetsForResponse(config.Tools)
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		ID           string           `json:"id"`
		Type         string           `json:"type"`
		Version      int              `json:"version"`
		Name         string           `json:"name"`
		Model        ModelConfig      `json:"model"`
		ApprovalMode ApprovalMode     `json:"approval_mode"`
		System       *string          `json:"system"`
		Description  *string          `json:"description"`
		Tools        RawArray         `json:"tools"`
		MCPServers   RawArray         `json:"mcp_servers"`
		Skills       RawArray         `json:"skills"`
		Metadata     StringMap        `json:"metadata"`
		Multiagent   *json.RawMessage `json:"multiagent"`
		CreatedAt    time.Time        `json:"created_at"`
		UpdatedAt    time.Time        `json:"updated_at"`
		ArchivedAt   *time.Time       `json:"archived_at"`
	}{
		ID:           value.ID,
		Type:         value.Type,
		Version:      value.Version,
		Name:         config.Name,
		Model:        ModelConfig{ID: config.Model},
		ApprovalMode: config.ApprovalMode,
		System:       config.System,
		Description:  config.Description,
		Tools:        tools,
		MCPServers:   config.MCPServers,
		Skills:       config.Skills,
		Metadata:     config.Metadata,
		Multiagent:   value.Multiagent,
		CreatedAt:    value.CreatedAt,
		UpdatedAt:    value.UpdatedAt,
		ArchivedAt:   value.ArchivedAt,
	})
}

// ProjectToolsetsForResponse resolves SDK-visible tool defaults without
// changing the canonical raw toolset bytes used by storage and runtime code.
func ProjectToolsetsForResponse(input RawArray) (RawArray, error) {
	if len(input) == 0 {
		return RawArray{}, nil
	}
	projected := make(RawArray, 0, len(input))
	for index, raw := range input {
		var discriminator struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(raw, &discriminator); err != nil {
			return nil, err
		}
		switch discriminator.Type {
		case "tetral_agent_toolset":
			var canonical tetralToolsetEntry
			if err := json.Unmarshal(raw, &canonical); err != nil {
				return nil, err
			}
			configNames := []string{"read", "grep", "glob"}
			if canonical.Family == "gpt" {
				configNames = []string{"view_image"}
			}
			configs := make([]toolConfig, 0, len(configNames))
			for _, name := range configNames {
				configs = append(configs, resolvedToolConfig(name, true, "always_allow"))
			}
			encoded, err := json.Marshal(struct {
				Type          string            `json:"type"`
				Family        string            `json:"family"`
				DefaultConfig toolDefaultConfig `json:"default_config"`
				Configs       []toolConfig      `json:"configs"`
			}{
				Type:          canonical.Type,
				Family:        canonical.Family,
				DefaultConfig: resolvedToolDefault(true, "always_ask"),
				Configs:       configs,
			})
			if err != nil {
				return nil, err
			}
			projected = append(projected, encoded)
		case "mcp_toolset":
			var canonical mcpToolsetEntry
			if err := json.Unmarshal(raw, &canonical); err != nil {
				return nil, err
			}
			defaults := resolvedToolDefault(true, "always_ask")
			if canonical.DefaultConfig != nil {
				if canonical.DefaultConfig.Enabled != nil {
					defaults.Enabled = boolPointer(*canonical.DefaultConfig.Enabled)
				}
				if canonical.DefaultConfig.PermissionPolicy != nil {
					defaults.PermissionPolicy = &permissionPolicy{Type: canonical.DefaultConfig.PermissionPolicy.Type}
				}
			}
			configs := make([]toolConfig, 0, len(canonical.Configs))
			for _, configured := range canonical.Configs {
				resolved := resolvedToolConfig(configured.Name, true, "always_ask")
				if configured.Enabled != nil {
					resolved.Enabled = boolPointer(*configured.Enabled)
				}
				if configured.PermissionPolicy != nil {
					resolved.PermissionPolicy = &permissionPolicy{Type: configured.PermissionPolicy.Type}
				}
				configs = append(configs, resolved)
			}
			encoded, err := json.Marshal(struct {
				Type          string            `json:"type"`
				MCPServerName string            `json:"mcp_server_name"`
				DefaultConfig toolDefaultConfig `json:"default_config"`
				Configs       []toolConfig      `json:"configs"`
			}{
				Type:          canonical.Type,
				MCPServerName: canonical.MCPServerName,
				DefaultConfig: defaults,
				Configs:       configs,
			})
			if err != nil {
				return nil, err
			}
			projected = append(projected, encoded)
		default:
			return nil, &ValidationError{Message: "tools[" + strconv.Itoa(index) + "].type is unsupported"}
		}
	}
	return projected, nil
}

func resolvedToolDefault(enabled bool, policyType string) toolDefaultConfig {
	return toolDefaultConfig{
		Enabled:          boolPointer(enabled),
		PermissionPolicy: &permissionPolicy{Type: policyType},
	}
}

func resolvedToolConfig(name string, enabled bool, policyType string) toolConfig {
	return toolConfig{
		Name:             name,
		Enabled:          boolPointer(enabled),
		PermissionPolicy: &permissionPolicy{Type: policyType},
	}
}

func boolPointer(value bool) *bool { return &value }

type CreateAgentRequest struct {
	AgentConfig
}

type UpdateAgentRequest struct {
	Version int `json:"version"`
	AgentConfig
}

type NotFoundError struct {
	Message string
}

func (errorValue *NotFoundError) Error() string { return errorValue.Message }

type ValidationError struct {
	Message string
}

func (errorValue *ValidationError) Error() string { return errorValue.Message }

type ConflictError struct {
	Message string
}

func (errorValue *ConflictError) Error() string { return errorValue.Message }
