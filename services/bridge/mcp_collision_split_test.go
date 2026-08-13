package agentruntimebridge

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestFilterMCPManifestCollisionsOwnsOnlyPinnedFamily(t *testing.T) {
	claudeNames := []string{"Bash", "Read", "Write", "Edit", "Glob", "Grep"}
	gptNames := []string{"exec_command", "write_stdin", "view_image", "apply_patch"}
	platformNames := []string{"web", "memory", "spawn_agent", "send_message", "wait_agent", "interrupt_agent", "close_agent", "resume_agent", "list_agents"}
	allNames := append(append(append([]string{}, claudeNames...), gptNames...), platformNames...)
	allNames = append(allNames, "github_search")
	tools := make([]MCPManifestTool, 0, len(allNames))
	for _, name := range allNames {
		tools = append(tools, MCPManifestTool{Name: name})
	}

	for _, test := range []struct {
		family  string
		blocked []string
		passed  []string
	}{
		{family: "claude", blocked: claudeNames, passed: append(append(append([]string{}, gptNames...), platformNames...), "github_search")},
		{family: "gpt", blocked: gptNames, passed: append(append(append([]string{}, claudeNames...), platformNames...), "github_search")},
	} {
		t.Run(test.family, func(t *testing.T) {
			filtered, omissions := filterMCPManifestCollisions(test.family, tools)
			got := make([]string, 0, len(filtered))
			for _, tool := range filtered {
				got = append(got, tool.Name)
			}
			if !reflect.DeepEqual(got, test.passed) {
				t.Fatalf("filtered names = %v; want pinned-family-only pass set %v", got, test.passed)
			}
			gotOmissions := make([]string, 0, len(omissions))
			for _, omission := range omissions {
				gotOmissions = append(gotOmissions, omission.ToolName)
			}
			if !reflect.DeepEqual(gotOmissions, test.blocked) {
				t.Fatalf("omitted names = %v; want pinned family %v", gotOmissions, test.blocked)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreMcpManifestChangedFiltersPinnedFamilyAndLogsAfterAcceptance(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedMCPFamilySession(t, admin, "sesn_mcp_collision_changed", "thr_mcp_collision_changed", "claude")
	lister := &recordingMCPManifestLister{results: []MCPManifestListResult{{
		ManifestETag: "connector_etag_changed",
		Tools: []MCPManifestTool{
			{Name: "Read", Description: "Pinned family", InputSchemaJSON: `{"type":"object"}`},
			{Name: "exec_command", Description: "Other family", InputSchemaJSON: `{"type":"object"}`},
			{Name: "memory", Description: "Platform", InputSchemaJSON: `{"type":"object"}`},
			{Name: "github_search", Description: "Ordinary", InputSchemaJSON: `{"type":"object"}`},
		},
	}}}
	var logs bytes.Buffer
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.MCPManifestLister = lister
	store.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	request := &bridgev1.McpManifestChangedRequest{
		WorkspaceId: "default", SessionId: "sesn_mcp_collision_changed", McpServerName: "github", ManifestEtag: "connector_etag_changed",
	}

	response, err := store.McpManifestChanged(context.Background(), request)
	if err != nil {
		t.Fatalf("McpManifestChanged: %v", err)
	}
	if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("McpManifestChanged status = %s; want committed", response.GetAck().GetStatus())
	}
	assertStoredMCPManifest(t, admin, "sesn_mcp_collision_changed", "connector_etag_changed", []string{"exec_command", "memory", "github_search"})
	assertMCPFamilyOmissionWarnings(t, logs.Bytes(), ServiceNameBridgeAPI, "sesn_mcp_collision_changed", "claude", []string{"Read"})

	if _, err := store.McpManifestChanged(context.Background(), request); err != nil {
		t.Fatalf("McpManifestChanged replay: %v", err)
	}
	if len(lister.requests) != 1 {
		t.Fatalf("McpManifestChanged replay lister calls = %d; want 1", len(lister.requests))
	}
	assertMCPFamilyOmissionWarnings(t, logs.Bytes(), ServiceNameBridgeAPI, "sesn_mcp_collision_changed", "claude", []string{"Read"})
}

func TestPostgreSQLBridgeAPIStoreMcpManifestChangedFailureBeforeAcceptanceLogsNoOmission(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedMCPFamilySession(t, admin, "sesn_mcp_collision_changed_fail", "thr_mcp_collision_changed_fail", "claude")
	var logs bytes.Buffer
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	store.MCPManifestLister = &recordingMCPManifestLister{results: []MCPManifestListResult{{
		ManifestETag: "connector_etag_changed_fail",
		Tools: []MCPManifestTool{
			{Name: "Read", InputSchemaJSON: `{"type":"object"}`},
			{Name: "github_search", InputSchemaJSON: `not-json`},
		},
	}}}

	_, err := store.McpManifestChanged(context.Background(), &bridgev1.McpManifestChangedRequest{
		WorkspaceId: "default", SessionId: "sesn_mcp_collision_changed_fail", McpServerName: "github", ManifestEtag: "connector_etag_changed_fail",
	})
	if err == nil {
		t.Fatal("McpManifestChanged error = nil; want invalid manifest failure")
	}
	assertMCPFamilyOmissionWarnings(t, logs.Bytes(), ServiceNameBridgeAPI, "sesn_mcp_collision_changed_fail", "claude", nil)
}

func TestPostgreSQLRuntimeDeliveryStoreInitialMCPManifestFiltersPinnedFamilyAndLogsAfterAcceptance(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedMCPFamilySession(t, admin, "sesn_mcp_collision_initial", "thr_mcp_collision_initial", "gpt")
	var logs bytes.Buffer
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	store.MCPManifestLister = &recordingMCPManifestLister{results: []MCPManifestListResult{{
		ManifestETag: "connector_etag_initial",
		Tools: []MCPManifestTool{
			{Name: "apply_patch", Description: "Pinned family", InputSchemaJSON: `{"type":"object"}`},
			{Name: "Read", Description: "Other family", InputSchemaJSON: `{"type":"object"}`},
			{Name: "web", Description: "Platform", InputSchemaJSON: `{"type":"object"}`},
			{Name: "github_search", Description: "Ordinary", InputSchemaJSON: `{"type":"object"}`},
		},
	}}}

	err := store.captureInitialMCPManifests(context.Background(), RuntimeJob{
		WorkspaceID: "default", SessionID: "sesn_mcp_collision_initial",
	}, []MCPManifestToolsetConfig{{MCPServerName: "github", BuiltinFamily: "gpt"}}, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
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
	store.MCPManifestLister = &recordingMCPManifestLister{results: []MCPManifestListResult{{
		ManifestETag: "connector_etag_initial_fail",
		Tools: []MCPManifestTool{
			{Name: "apply_patch", InputSchemaJSON: `{"type":"object"}`},
			{Name: "github_search", InputSchemaJSON: `not-json`},
		},
	}}}

	err := store.captureInitialMCPManifests(context.Background(), RuntimeJob{
		WorkspaceID: "default", SessionID: "sesn_mcp_collision_initial_fail",
	}, []MCPManifestToolsetConfig{{MCPServerName: "github", BuiltinFamily: "gpt"}}, time.Now())
	if err != nil {
		t.Fatalf("captureInitialMCPManifests: %v", err)
	}
	assertMCPFamilyOmissionWarnings(t, logs.Bytes(), ServiceNameJobRunner, "sesn_mcp_collision_initial_fail", "gpt", nil)
}

func TestProductionMCPManifestStoresReceiveWorkloadLoggers(t *testing.T) {
	bridgeMain, err := os.ReadFile("cmd/bridge-api/main.go")
	if err != nil {
		t.Fatalf("read bridge-api main: %v", err)
	}
	jobRunnerMain, err := os.ReadFile("cmd/job-runner/main.go")
	if err != nil {
		t.Fatalf("read job-runner main: %v", err)
	}
	if !strings.Contains(string(bridgeMain), "store.Logger = logger") {
		t.Fatal("bridge-api main must inject its production logger into PostgreSQLBridgeAPIStore")
	}
	if !strings.Contains(string(jobRunnerMain), "NewJobRunnerRuntimeDeliveryStore(\n\t\tdatabase.Client,\n\t\tlogger,") {
		t.Fatal("job-runner main must pass its production logger to NewJobRunnerRuntimeDeliveryStore")
	}
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
