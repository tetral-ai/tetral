package mcpmanifest

import (
	"context"
	"encoding/json"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/runtimeconfig"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
)

type ToolsetConfig struct {
	MCPServerName string
	BuiltinFamily string
}

// SessionToolsetsTx returns the Session's enabled MCP toolsets from its
// installed_tools_json snapshot, the authoritative installed configuration,
// inside the caller's transaction.
func SessionToolsetsTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string) ([]ToolsetConfig, error) {
	var installedToolsJSON string
	if err := tx.QueryRow(ctx,
		`SELECT installed_tools_json
		   FROM sessions
		  WHERE workspace_id = $1
		    AND id = $2`,
		workspaceID,
		sessionID,
	).Scan(&installedToolsJSON); dbconnect.IsNoRows(err) {
		return nil, runtimecontrol.PreparationError{Kind: "runtime_session_unavailable", Message: "session agent config is unavailable", Retryable: true}
	} else if err != nil {
		return nil, err
	}
	config, err := runtimeconfig.InstalledConfig(installedToolsJSON)
	if err != nil {
		return nil, runtimecontrol.PreparationError{Kind: "invalid_session_agent_config", Message: "session agent config is invalid", Retryable: false}
	}
	builtinFamily, err := runtimeconfig.InstalledBuiltinFamily(config)
	if err != nil {
		return nil, runtimecontrol.PreparationError{Kind: "invalid_session_agent_config", Message: "session agent config is invalid", Retryable: false}
	}
	seen := map[string]struct{}{}
	var toolsets []ToolsetConfig
	for _, raw := range config.Tools {
		var tool struct {
			Type          string `json:"type"`
			MCPServerName string `json:"mcp_server_name"`
		}
		if err := json.Unmarshal(raw, &tool); err != nil {
			return nil, runtimecontrol.PreparationError{Kind: "invalid_session_agent_config", Message: "session agent tool config is invalid", Retryable: false}
		}
		if tool.Type != "mcp_toolset" || tool.MCPServerName != "github" {
			continue
		}
		if _, ok := seen[tool.MCPServerName]; ok {
			continue
		}
		seen[tool.MCPServerName] = struct{}{}
		toolsets = append(toolsets, ToolsetConfig{
			MCPServerName: tool.MCPServerName,
			BuiltinFamily: builtinFamily,
		})
	}
	return toolsets, nil
}

func InitialToolsetsTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string) ([]ToolsetConfig, error) {
	toolsets, err := SessionToolsetsTx(ctx, tx, workspaceID, sessionID)
	if err != nil {
		return nil, err
	}
	var pending []ToolsetConfig
	for _, toolset := range toolsets {
		delivered, err := DeliveryExistsTx(ctx, tx, workspaceID, sessionID, toolset.MCPServerName)
		if err != nil {
			return nil, err
		}
		if delivered {
			continue
		}
		pending = append(pending, toolset)
	}
	return pending, nil
}

func ToolsetConfigTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, mcpServerName string) (ToolsetConfig, error) {
	toolsets, err := SessionToolsetsTx(ctx, tx, workspaceID, sessionID)
	if err != nil {
		return ToolsetConfig{}, err
	}
	for _, toolset := range toolsets {
		if toolset.MCPServerName == mcpServerName {
			return toolset, nil
		}
	}
	return ToolsetConfig{}, status.Error(codes.FailedPrecondition, "mcp toolset config is unavailable")
}

func DeliveryExistsTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, mcpServerName string) (bool, error) {
	var exists bool
	err := tx.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1 FROM session_mcp_manifests
			 WHERE workspace_id = $1 AND session_id = $2 AND mcp_server_name = $3
		)`,
		workspaceID,
		sessionID,
		mcpServerName,
	).Scan(&exists)
	return exists, err
}
