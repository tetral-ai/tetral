package session

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/agent"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// T-COMPAT-SESS-3
func TestSDKCompatibilityTCOMPATSession3ToolsFullReplaceKeepsPinnedFamily(t *testing.T) {
	store := newRecordingSessionStore()
	service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, time.Now().UTC())
	store.sessions["sesn_test"] = testStoredSession(time.Now().UTC())
	tools := agent.RawArray{
		json.RawMessage(`{"type":"tetral_agent_toolset","family":"claude"}`),
		json.RawMessage(`{"type":"mcp_toolset","mcp_server_name":"github"}`),
	}
	servers := agent.RawArray{json.RawMessage(`{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}`)}
	response, err := service.Update(context.Background(), workspace.DefaultID, "sesn_test", UpdateRequest{ToolsPatch: &tools, MCPServersPatch: &servers})
	if err != nil {
		t.Fatalf("Update tools: %v", err)
	}
	if len(response.Agent.Tools) != 2 {
		t.Fatalf("response tools = %s; want family plus MCP", mustMarshalSessionSDKJSON(t, response.Agent.Tools))
	}
}

// T-COMPAT-SESS-15
func TestSDKCompatibilityTCOMPATSession15ToolsReplacementRejectsMissingOrChangedFamily(t *testing.T) {
	for _, tc := range []struct {
		name  string
		tools agent.RawArray
		want  string
	}{
		{name: "missing", tools: agent.RawArray{}, want: "tools must contain exactly one tetral_agent_toolset entry"},
		{name: "changed", tools: agent.RawArray{json.RawMessage(`{"type":"tetral_agent_toolset","family":"gpt"}`)}, want: "tools[0].family must match the session's pinned family"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newRecordingSessionStore()
			service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, time.Now().UTC())
			store.sessions["sesn_test"] = testStoredSession(time.Now().UTC())
			_, err := service.Update(context.Background(), workspace.DefaultID, "sesn_test", UpdateRequest{ToolsPatch: &tc.tools})
			assertSessionValidationMessage(t, err, tc.want)
		})
	}
}

// T-COMPAT-SESS-16
func TestSDKCompatibilityTCOMPATSession16CreateRejectsPreLawAgent(t *testing.T) {
	preLaw := testAgent(1)
	preLaw.Tools = agent.RawArray{}
	service := NewService(staticAgentReader{agent: preLaw}, testEnvironments{}, &recordingFileIdentities{}, testMemories{}, &recordingVaultValidator{}, newRecordingSessionStore(), testSessionEncryptor{})
	_, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent: AgentReference{ID: preLaw.ID}, EnvironmentID: "env_test",
	})
	assertSessionValidationMessage(t, err, "agent must declare exactly one tetral_agent_toolset entry; update the agent")
}

// T-COMPAT-SESS-17
func TestSDKCompatibilityTCOMPATSession17RotatesOnlyGitHubResourceTokenWithoutEcho(t *testing.T) {
	fixed := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, fixed)
	stored := testStoredSession(fixed)
	stored.Resources = []*Resource{
		{
			ID:          "sesrsc_github",
			SessionID:   stored.ID,
			WorkspaceID: workspace.DefaultID,
			Type:        ResourceTypeGitHubRepository,
			GitHubRepository: &GitHubRepositoryResource{
				URL:                         "https://github.com/tetral-ai/tetral",
				MountPath:                   "/workspace/tetral",
				AuthorizationTokenEncrypted: []byte("encrypted:old"),
			},
		},
		{
			ID:          "sesrsc_file",
			SessionID:   stored.ID,
			WorkspaceID: workspace.DefaultID,
			Type:        ResourceTypeFile,
			File:        &FileResource{FileID: "file_test", MountPath: "/workspace/file.txt"},
		},
	}
	store.sessions[stored.ID] = stored

	response, err := service.UpdateResource(
		context.Background(),
		workspace.DefaultID,
		stored.ID,
		"sesrsc_github",
		"github_resource_token_rotated",
	)
	if err != nil {
		t.Fatalf("UpdateResource GitHub: %v", err)
	}
	body, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("Marshal ResourceUpdateResponse: %v", err)
	}
	if strings.Contains(string(body), "github_resource_token_rotated") ||
		strings.Contains(string(body), "AuthorizationTokenEncrypted") ||
		strings.Contains(string(body), "authorization_token_encrypted") {
		t.Fatalf("ResourceUpdateResponse echoed credential state: %s", body)
	}
	if got := string(store.sessions[stored.ID].Resources[0].GitHubRepository.AuthorizationTokenEncrypted); got != "encrypted:github_resource_token_rotated" {
		t.Fatalf("stored token = %q; want encrypted rotated token", got)
	}

	_, err = service.UpdateResource(
		context.Background(),
		workspace.DefaultID,
		stored.ID,
		"sesrsc_file",
		"must-not-encrypt",
	)
	assertSessionValidationMessage(t, err, "only github_repository authorization_token can be updated")
}

// T-COMPAT-SESS-18
func TestSDKCompatibilityTCOMPATSession18RejectsDuplicateGitHubRepositoryComparisonKeys(t *testing.T) {
	fixed := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, fixed)
	firstMount := "/workspace/first"
	secondMount := "/workspace/second"

	_, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent:         AgentReference{ID: "agent_test", Version: intPtr(2)},
		EnvironmentID: "env_test",
		Resources: []ResourceRequest{
			{
				Type:               string(ResourceTypeGitHubRepository),
				GitHubURL:          "https://github.com/tetral-ai/tetral.git",
				AuthorizationToken: "github_resource_token_first",
				MountPath:          &firstMount,
			},
			{
				Type:               string(ResourceTypeGitHubRepository),
				GitHubURL:          "https://github.com/tetral-ai/tetral",
				AuthorizationToken: "github_resource_token_second",
				MountPath:          &secondMount,
			},
		},
	})
	assertSessionValidationMessage(t, err, "duplicate GitHub repository URL")
	if len(store.sessions) != 0 {
		t.Fatalf("sessions persisted after duplicate repository rejection: %d", len(store.sessions))
	}

	_, err = service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent:         AgentReference{ID: "agent_test", Version: intPtr(2)},
		EnvironmentID: "env_test",
		Resources: []ResourceRequest{
			{
				Type:               string(ResourceTypeGitHubRepository),
				GitHubURL:          "https://github.com/Tetral-AI/owner-case",
				AuthorizationToken: "github_resource_token_mixed_owner_case",
				MountPath:          &firstMount,
			},
			{
				Type:               string(ResourceTypeGitHubRepository),
				GitHubURL:          "https://github.com/tetral-ai/owner-case",
				AuthorizationToken: "github_resource_token_lower_owner_case",
				MountPath:          &secondMount,
			},
			{
				Type:               string(ResourceTypeGitHubRepository),
				GitHubURL:          "https://github.com/tetral-ai/Repo-Case",
				AuthorizationToken: "github_resource_token_mixed_repo_case",
				MountPath:          stringPtr("/workspace/third"),
			},
			{
				Type:               string(ResourceTypeGitHubRepository),
				GitHubURL:          "https://github.com/tetral-ai/repo-case",
				AuthorizationToken: "github_resource_token_lower_repo_case",
				MountPath:          stringPtr("/workspace/fourth"),
			},
		},
	})
	assertSessionValidationMessage(t, err, "duplicate GitHub repository URL")
	if len(store.sessions) != 0 {
		t.Fatalf("sessions persisted after case-variant duplicate rejection: %d", len(store.sessions))
	}

	_, err = service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent:         AgentReference{ID: "agent_test", Version: intPtr(2)},
		EnvironmentID: "env_test",
		Resources: []ResourceRequest{
			{
				Type:               string(ResourceTypeGitHubRepository),
				GitHubURL:          "https://github.com/tetral-ai/Repo-Case",
				AuthorizationToken: "github_resource_token_mixed_repo_case",
				MountPath:          &firstMount,
			},
			{
				Type:               string(ResourceTypeGitHubRepository),
				GitHubURL:          "https://github.com/tetral-ai/repo-case",
				AuthorizationToken: "github_resource_token_lower_repo_case",
				MountPath:          &secondMount,
			},
		},
	})
	assertSessionValidationMessage(t, err, "duplicate GitHub repository URL")
	if len(store.sessions) != 0 {
		t.Fatalf("sessions persisted after repository-case duplicate rejection: %d", len(store.sessions))
	}
}

func assertSessionValidationMessage(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil || err.Error() != want {
		t.Fatalf("error = %#v; want %q", err, want)
	}
}

func TestSDKCompatibilitySessionAgentToolsetsProjectWithoutCanonicalMutation(t *testing.T) {
	canonical := agent.RawArray{
		json.RawMessage(`{"type":"tetral_agent_toolset","family":"claude"}`),
		json.RawMessage(`{"type":"mcp_toolset","mcp_server_name":"github","configs":[{"name":"github_search"}]}`),
	}
	original := make(agent.RawArray, len(canonical))
	for index := range canonical {
		original[index] = append(json.RawMessage(nil), canonical[index]...)
	}
	body, err := json.Marshal(SessionAgentResponse{Tools: canonical})
	if err != nil {
		t.Fatalf("Marshal SessionAgentResponse: %v", err)
	}
	var response struct {
		Tools []struct {
			DefaultConfig map[string]any   `json:"default_config"`
			Configs       []map[string]any `json:"configs"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("Unmarshal SessionAgentResponse: %v", err)
	}
	if len(response.Tools) != 2 {
		t.Fatalf("tools = %#v; want two", response.Tools)
	}
	for index, tool := range response.Tools {
		if tool.DefaultConfig["enabled"] != true {
			t.Fatalf("tools[%d].default_config = %#v; want enabled", index, tool.DefaultConfig)
		}
		permission, ok := tool.DefaultConfig["permission_policy"].(map[string]any)
		if !ok || permission["type"] != "always_ask" {
			t.Fatalf("tools[%d].default permission = %#v; want always_ask", index, tool.DefaultConfig["permission_policy"])
		}
	}
	if got := response.Tools[0].Configs; len(got) != 3 || got[0]["name"] != "read" || got[1]["name"] != "grep" || got[2]["name"] != "glob" {
		t.Fatalf("Claude configs = %#v; want lowercase SDK read/grep/glob", got)
	}
	if got := response.Tools[1].Configs; len(got) != 1 || got[0]["name"] != "github_search" || got[0]["enabled"] != true {
		t.Fatalf("MCP configs = %#v; want fully populated configured override only", got)
	}
	mcpPolicy, ok := response.Tools[1].Configs[0]["permission_policy"].(map[string]any)
	if !ok || mcpPolicy["type"] != "always_ask" {
		t.Fatalf("MCP configured override policy = %#v; want inherited always_ask", response.Tools[1].Configs[0]["permission_policy"])
	}
	if !reflect.DeepEqual(canonical, original) {
		t.Fatalf("Session response projection mutated canonical tools: got %s want %s", mustMarshalSessionSDKJSON(t, canonical), mustMarshalSessionSDKJSON(t, original))
	}
}

func mustMarshalSessionSDKJSON(t *testing.T, value any) []byte {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("Marshal JSON: %v", err)
	}
	return body
}
