package jobrunner

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/mcpmanifest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

func TestPostgreSQLRuntimeDeliveryStoreInitialMCPManifestFiltersPinnedFamilyAndLogsAfterAcceptance(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedMCPFamilySession(t, admin, "sesn_mcp_collision_initial", "thr_mcp_collision_initial", "gpt")
	var logs bytes.Buffer
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	store.MCPManifestLister = &recordingMCPManifestLister{results: []mcpmanifest.ListResult{{
		ManifestETag: "connector_etag_initial",
		Tools: []mcpmanifest.Tool{
			{Name: "apply_patch", Description: "Pinned family", InputSchemaJSON: `{"type":"object"}`},
			{Name: "Read", Description: "Other family", InputSchemaJSON: `{"type":"object"}`},
			{Name: "web", Description: "Platform", InputSchemaJSON: `{"type":"object"}`},
			{Name: "github_search", Description: "Ordinary", InputSchemaJSON: `{"type":"object"}`},
		},
	}}}

	err := store.captureInitialMCPManifests(context.Background(), RuntimeJob{
		WorkspaceID: "default", SessionID: "sesn_mcp_collision_initial",
	}, []mcpmanifest.ToolsetConfig{{MCPServerName: "github", BuiltinFamily: "gpt"}}, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("captureInitialMCPManifests: %v", err)
	}
	assertStoredMCPManifest(t, admin, "sesn_mcp_collision_initial", "connector_etag_initial", []string{"Read", "web", "github_search"})
	assertMCPFamilyOmissionWarnings(t, logs.Bytes(), ServiceNameJobRunner, "sesn_mcp_collision_initial", "gpt", []string{"apply_patch"})
}

func TestPostgreSQLRuntimeDeliveryStoreInitialMCPManifestFailureBeforeAcceptanceLogsNoOmission(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedMCPFamilySession(t, admin, "sesn_mcp_collision_initial_fail", "thr_mcp_collision_initial_fail", "gpt")
	var logs bytes.Buffer
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	store.MCPManifestLister = &recordingMCPManifestLister{results: []mcpmanifest.ListResult{{
		ManifestETag: "connector_etag_initial_fail",
		Tools: []mcpmanifest.Tool{
			{Name: "apply_patch", InputSchemaJSON: `{"type":"object"}`},
			{Name: "github_search", InputSchemaJSON: `not-json`},
		},
	}}}

	err := store.captureInitialMCPManifests(context.Background(), RuntimeJob{
		WorkspaceID: "default", SessionID: "sesn_mcp_collision_initial_fail",
	}, []mcpmanifest.ToolsetConfig{{MCPServerName: "github", BuiltinFamily: "gpt"}}, time.Now())
	if err != nil {
		t.Fatalf("captureInitialMCPManifests: %v", err)
	}
	assertMCPFamilyOmissionWarnings(t, logs.Bytes(), ServiceNameJobRunner, "sesn_mcp_collision_initial_fail", "gpt", nil)
}

func seedMCPFamilySession(t *testing.T, admin *sql.DB, sessionID string, threadID string, family string) {
	t.Helper()
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIAgentConfig(t, admin, "default", sessionID, `{"name":"agent","model":"anthropic/claude-opus-4-8","tools":[{"type":"mcp_toolset","mcp_server_name":"github"}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}],"skills":[],"metadata":{}}`)
	installed := `{"tools":[{"type":"tetral_agent_toolset","family":"` + family + `"},{"type":"mcp_toolset","mcp_server_name":"github"}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}]}`
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET installed_tools_json = $1 WHERE workspace_id = 'default' AND id = $2`, installed, sessionID); err != nil {
		t.Fatalf("seed installed MCP tools: %v", err)
	}
}

func assertStoredMCPManifest(t *testing.T, db *sql.DB, sessionID string, manifestETag string, wantNames []string) {
	t.Helper()
	var toolsJSON string
	if err := db.QueryRowContext(context.Background(),
		`SELECT tools_json
		   FROM session_mcp_manifests
		  WHERE workspace_id = 'default' AND session_id = $1 AND manifest_etag = $2`,
		sessionID,
		manifestETag,
	).Scan(&toolsJSON); err != nil {
		t.Fatalf("read stored MCP manifest: %v", err)
	}
	var tools []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal([]byte(toolsJSON), &tools); err != nil {
		t.Fatalf("parse stored MCP manifest: %v", err)
	}
	gotNames := make([]string, 0, len(tools))
	for _, tool := range tools {
		gotNames = append(gotNames, tool.Name)
	}
	if !reflect.DeepEqual(gotNames, wantNames) {
		t.Fatalf("stored MCP manifest names = %v; want %v", gotNames, wantNames)
	}
}

func assertMCPFamilyOmissionWarnings(t *testing.T, raw []byte, component string, sessionID string, family string, wantTools []string) {
	t.Helper()
	var warnings []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("parse slog record: %v", err)
		}
		if record["event.kind"] == "mcp_manifest.tool_omitted" {
			warnings = append(warnings, record)
		}
	}
	if len(warnings) != len(wantTools) {
		t.Fatalf("family omission warning count = %d; want %d; records=%v", len(warnings), len(wantTools), warnings)
	}
	for index, record := range warnings {
		want := map[string]any{
			"level": "WARN", "msg": "bridge.mcp_manifest.tool_omitted",
			"operation": "mcp_manifest.filter", "event.kind": "mcp_manifest.tool_omitted", "component": component,
			"workspace.id": "default", "session.id": sessionID, "mcp.server.name": "github",
			"mcp.tool.name": wantTools[index], "mcp.tool.family": family, "mcp.omission.reason": "builtin_name_collision",
		}
		for key, value := range want {
			if record[key] != value {
				t.Fatalf("warning[%d][%q] = %#v; want %#v; record=%v", index, key, record[key], value, record)
			}
		}
		for _, forbidden := range []string{"description", "input_schema", "input_schema_json", "credentials", "payload_json"} {
			if _, exists := record[forbidden]; exists {
				t.Fatalf("warning[%d] contains forbidden field %q: %v", index, forbidden, record)
			}
		}
	}
}
