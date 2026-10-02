package jobrunner

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/tetral-ai/tetral/internal/mcpmanifest"
)

func mcpManifestResult(etag string, toolName string) mcpmanifest.ListResult {
	return mcpmanifest.ListResult{ManifestETag: etag, Tools: []mcpmanifest.Tool{{
		Name: toolName, Description: toolName, InputSchemaJSON: `{"type":"object"}`,
	}}}
}

func insertAcceptedMCPManifest(t *testing.T, db *sql.DB, sessionID string, etag string, generation int64, toolName string) {
	t.Helper()
	tools, err := json.Marshal([]map[string]any{{
		"name": toolName, "description": toolName, "input_schema": map[string]any{"type": "object"},
	}})
	if err != nil {
		t.Fatalf("marshal accepted manifest: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_mcp_manifests (
			workspace_id, session_id, mcp_server_name, tools_json, manifest_etag, manifest_generation, created_at, updated_at
		 ) VALUES ('default', $1, 'github', $2, $3, $4, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		sessionID, string(tools), etag, generation); err != nil {
		t.Fatalf("insert accepted manifest: %v", err)
	}
}
