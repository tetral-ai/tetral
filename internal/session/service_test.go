package session

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/agent"
	"github.com/tetral-ai/tetral/internal/environment"
	"github.com/tetral-ai/tetral/internal/files"
	"github.com/tetral-ai/tetral/internal/memory"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestCreatePersistsDurableResourcesAndNeverReturnsGitHubToken(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	const authorizationToken = "github_resource_token_create"
	store := newRecordingSessionStore()
	fileIdentities := &recordingFileIdentities{}
	vaults := &recordingVaultValidator{}
	service := newTestService(store, fileIdentities, vaults, fixed)

	title := "control plane"
	fileMount := "/workspace/input.txt"
	checkout := &GitHubCheckout{Type: "branch", Name: "main"}
	response, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent:         AgentReference{ID: "agent_test", Version: intPtr(2)},
		EnvironmentID: "env_test",
		Title:         &title,
		Metadata:      map[string]string{"team": "infra"},
		VaultIDs:      []string{"vlt_test"},
		Resources: []ResourceRequest{
			{Type: string(ResourceTypeFile), FileID: "file_source", MountPath: &fileMount},
			{Type: string(ResourceTypeMemoryStore), MemoryStoreID: "memstore_test", Access: "read_only", Instructions: "use carefully"},
			{
				Type:               string(ResourceTypeGitHubRepository),
				GitHubURL:          "https://github.com/tetral-ai/tetral.git",
				AuthorizationToken: authorizationToken,
				Checkout:           checkout,
			},
		},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if response.ID != "sesn_test" || response.Status != StatusIdle {
		t.Fatalf("response id/status = %s/%s; want sesn_test/idle", response.ID, response.Status)
	}
	if len(response.Resources) != 3 {
		t.Fatalf("resources = %d; want 3", len(response.Resources))
	}
	if response.Resources[0].ID != "sesrsc_1" || response.Resources[0].FileID != "file_session_1" || response.Resources[0].MountPath != fileMount {
		t.Fatalf("file resource = %+v", response.Resources[0])
	}
	if response.Resources[1].MemoryStoreID != "memstore_test" || response.Resources[1].Access != "read_only" || response.Resources[1].MountPath != "/mnt/memory/project-memory" {
		t.Fatalf("memory resource = %+v", response.Resources[1])
	}
	if response.Resources[2].URL != "https://github.com/tetral-ai/tetral" || response.Resources[2].MountPath != "/workspace/tetral" {
		t.Fatalf("github resource = %+v", response.Resources[2])
	}
	if response.Resources[2].CheckoutType != "branch" || response.Resources[2].CheckoutRef != "main" {
		t.Fatalf("checkout = %s/%s; want branch/main", response.Resources[2].CheckoutType, response.Resources[2].CheckoutRef)
	}

	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("Marshal response: %v", err)
	}
	if strings.Contains(string(encoded), "AuthorizationTokenEncrypted") ||
		strings.Contains(string(encoded), "authorization_token_encrypted") ||
		strings.Contains(string(encoded), "encrypted:") ||
		strings.Contains(string(encoded), authorizationToken) {
		t.Fatalf("response serialized internal credential state: %s", encoded)
	}

	stored := store.sessions["sesn_test"]
	if stored.AgentID != "agent_test" || stored.AgentVersion != 2 || stored.EnvironmentID != "env_test" {
		t.Fatalf("stored session = %+v", stored)
	}
	if len(fileIdentities.created) != 1 {
		t.Fatalf("file identities created = %d; want 1", len(fileIdentities.created))
	}
	createdFile := fileIdentities.created[0]
	if createdFile.SourceFileID != "file_source" || createdFile.SessionID != "sesn_test" || createdFile.SessionFileID != "file_session_1" {
		t.Fatalf("file identity request = %+v", createdFile)
	}
	if len(vaults.validated) != 1 || vaults.validated[0] != "vlt_test" {
		t.Fatalf("validated vault ids = %v", vaults.validated)
	}
	if stored.Resources[2].GitHubRepository == nil {
		t.Fatalf("stored github resource = %+v", stored.Resources[2])
	}
	if got := string(stored.Resources[2].GitHubRepository.AuthorizationTokenEncrypted); got != "encrypted:"+authorizationToken {
		t.Fatalf("stored encrypted github token = %q; want encrypted token", got)
	}
}

func TestCreateRejectsGitHubResourceWithoutAuthorizationToken(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, fixed)

	_, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent:         AgentReference{ID: "agent_test"},
		EnvironmentID: "env_test",
		Resources: []ResourceRequest{{
			Type:      string(ResourceTypeGitHubRepository),
			GitHubURL: "https://github.com/tetral-ai/tetral",
		}},
	})
	var validation *ValidationError
	if !errors.As(err, &validation) || validation.Message != "authorization_token is required" {
		t.Fatalf("Create err = %T %v; want required authorization token", err, err)
	}
	if len(store.sessions) != 0 {
		t.Fatalf("sessions persisted after missing GitHub token: %v", store.sessions)
	}
}

func TestUpdateGitHubResourceTokenRequiresIdleSessionAndNeverReturnsToken(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, fixed)
	session := testStoredSession(fixed)
	session.Status = StatusIdle
	session.Resources = []*Resource{{
		ID:          "sesrsc_github",
		SessionID:   session.ID,
		WorkspaceID: workspace.DefaultID,
		Type:        ResourceTypeGitHubRepository,
		GitHubRepository: &GitHubRepositoryResource{
			URL:                         "https://github.com/tetral-ai/tetral",
			MountPath:                   "/workspace/tetral",
			AuthorizationTokenEncrypted: []byte("encrypted:old"),
		},
	}}
	store.sessions[session.ID] = session

	response, err := service.UpdateResource(
		context.Background(),
		workspace.DefaultID,
		session.ID,
		"sesrsc_github",
		"github_resource_token_rotated",
	)
	if err != nil {
		t.Fatalf("UpdateResource: %v", err)
	}
	if got := string(store.sessions[session.ID].Resources[0].GitHubRepository.AuthorizationTokenEncrypted); got != "encrypted:github_resource_token_rotated" {
		t.Fatalf("stored encrypted github token = %q; want rotated token", got)
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("Marshal response: %v", err)
	}
	if strings.Contains(string(encoded), "github_resource_token_rotated") || strings.Contains(string(encoded), "encrypted:") {
		t.Fatalf("resource response leaked credential material: %s", encoded)
	}
}

func TestUpdateGitHubResourceTokenRejectsRunningSession(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, fixed)
	session := testStoredSession(fixed)
	session.Status = StatusRunning
	session.Resources = []*Resource{{
		ID: "sesrsc_github", SessionID: session.ID, WorkspaceID: workspace.DefaultID,
		Type:             ResourceTypeGitHubRepository,
		GitHubRepository: &GitHubRepositoryResource{URL: "https://github.com/tetral-ai/tetral", MountPath: "/workspace/tetral", AuthorizationTokenEncrypted: []byte("encrypted:old")},
	}}
	store.sessions[session.ID] = session
	_, err := service.UpdateResource(context.Background(), workspace.DefaultID, session.ID, "sesrsc_github", "github_resource_token_rotated")
	var conflict *ConflictError
	if !errors.As(err, &conflict) || conflict.Message != "session must be idle for mutation" {
		t.Fatalf("UpdateResource err = %T %v; want idle conflict", err, err)
	}
}

func TestUpdateGitHubResourceTokenRejectsArchivingAndArchivedSessions(t *testing.T) {
	for _, lifecycleState := range []LifecycleState{LifecycleStateArchiving, LifecycleStateArchived} {
		t.Run(string(lifecycleState), func(t *testing.T) {
			fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
			store := newRecordingSessionStore()
			service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, fixed)
			session := testStoredSession(fixed)
			session.Resources = []*Resource{{
				ID:          "sesrsc_github",
				SessionID:   session.ID,
				WorkspaceID: workspace.DefaultID,
				Type:        ResourceTypeGitHubRepository,
				GitHubRepository: &GitHubRepositoryResource{
					URL:                         "https://github.com/tetral-ai/tetral",
					MountPath:                   "/workspace/tetral",
					AuthorizationTokenEncrypted: []byte("encrypted:old"),
				},
			}}
			session.LifecycleState = lifecycleState
			if lifecycleState == LifecycleStateArchived {
				archivedAt := fixed
				session.ArchivedAt = &archivedAt
			}
			store.sessions[session.ID] = session

			_, err := service.UpdateResource(
				context.Background(),
				workspace.DefaultID,
				session.ID,
				"sesrsc_github",
				"github_resource_token_rotated",
			)
			var conflict *ConflictError
			if !errors.As(err, &conflict) || conflict.Message != "session is archived" {
				t.Fatalf("UpdateResource err = %T %v; want archived conflict", err, err)
			}
			if got := string(store.sessions[session.ID].Resources[0].GitHubRepository.AuthorizationTokenEncrypted); got != "encrypted:old" {
				t.Fatalf("archived session token changed to %q", got)
			}
		})
	}
}

func TestUpdateGitHubResourceTokenHidesDeletedArchivedSession(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, fixed)
	session := testStoredSession(fixed)
	archivedAt := fixed
	session.ArchivedAt = &archivedAt
	session.LifecycleState = LifecycleStateDeleted
	session.Resources = []*Resource{{
		ID:          "sesrsc_github",
		SessionID:   session.ID,
		WorkspaceID: workspace.DefaultID,
		Type:        ResourceTypeGitHubRepository,
		GitHubRepository: &GitHubRepositoryResource{
			URL:                         "https://github.com/tetral-ai/tetral",
			MountPath:                   "/workspace/tetral",
			AuthorizationTokenEncrypted: []byte("encrypted:old"),
		},
	}}
	store.sessions[session.ID] = session

	_, err := service.UpdateResource(
		context.Background(),
		workspace.DefaultID,
		session.ID,
		"sesrsc_github",
		"github_resource_token_rotated",
	)
	var notFound *NotFoundError
	if !errors.As(err, &notFound) || notFound.Message != "session not found" {
		t.Fatalf("UpdateResource err = %T %v; want deleted session not found", err, err)
	}
	if got := string(store.sessions[session.ID].Resources[0].GitHubRepository.AuthorizationTokenEncrypted); got != "encrypted:old" {
		t.Fatalf("deleted session token changed to %q", got)
	}
}

func TestCreateAdmitsProviderCredentialSelector(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	store.providerCredentials["cred_anthropic"] = providerCredentialForSessionTest("cred_anthropic", "vlt_test", "provider_api_key", "anthropic", "user_api_key")
	service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, fixed)

	response, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent:         AgentReference{ID: "agent_test"},
		EnvironmentID: "env_test",
		VaultIDs:      []string{"vlt_test"},
		Providers: ProviderSelectors{
			"anthropic": {CredentialID: "cred_anthropic"}, //nolint:gosec // Test credential id, not a secret.
		},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if response.Providers["anthropic"].CredentialID != "cred_anthropic" {
		t.Fatalf("response providers = %#v; want anthropic credential id", response.Providers)
	}

	selector, ok := store.providerAuth["sesn_test"]
	if !ok {
		t.Fatal("session provider auth was not written")
	}
	if selector.ProviderID != "anthropic" ||
		selector.CredentialID != "cred_anthropic" ||
		selector.VaultID != "vlt_test" ||
		selector.AccessMode != "user_api_key" ||
		!selector.UpdatedAt.Equal(fixed) {
		t.Fatalf("provider selector = %+v", selector)
	}
	if stored := store.sessions["sesn_test"]; stored == nil || len(stored.VaultIDs) != 1 || stored.VaultIDs[0] != "vlt_test" {
		t.Fatalf("stored session = %+v; want immutable vault_ids preserved", stored)
	}
}

func TestCreateTreatsOmittedAndEmptyProvidersAsPlatformAccess(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		providers ProviderSelectors
	}{
		{name: "omitted", providers: nil},
		{name: "empty", providers: ProviderSelectors{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := newRecordingSessionStore()
			service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, fixed)

			if _, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
				Agent:         AgentReference{ID: "agent_test"},
				EnvironmentID: "env_test",
				VaultIDs:      []string{"vlt_test"},
				Providers:     tc.providers,
			}); err != nil {
				t.Fatalf("Create: %v", err)
			}
			if len(store.providerAuth) != 0 {
				t.Fatalf("provider auth rows = %+v; want none", store.providerAuth)
			}
		})
	}
}

func TestCreateRejectsInvalidProviderSelectorsBeforeCommit(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		vaultIDs    []string
		providers   ProviderSelectors
		credentials map[string]*ProviderCredentialForAdmission
		wantMessage string
	}{
		{
			name:        "no bound vaults",
			providers:   ProviderSelectors{"anthropic": {CredentialID: "cred_anthropic"}}, //nolint:gosec // Test credential id, not a secret.
			credentials: map[string]*ProviderCredentialForAdmission{"cred_anthropic": providerCredentialForSessionTest("cred_anthropic", "vlt_test", "provider_api_key", "anthropic", "user_api_key")},
			wantMessage: "providers requires at least one vault_id",
		},
		{
			name:        "multiple providers",
			vaultIDs:    []string{"vlt_test"},
			providers:   ProviderSelectors{"anthropic": {CredentialID: "cred_anthropic"}, "openai": {CredentialID: "cred_openai"}}, //nolint:gosec // Test credential ids, not secrets.
			credentials: map[string]*ProviderCredentialForAdmission{},
			wantMessage: "providers must contain exactly one provider",
		},
		{
			name:        "wrong provider key",
			vaultIDs:    []string{"vlt_test"},
			providers:   ProviderSelectors{"openai": {CredentialID: "cred_openai"}},
			credentials: map[string]*ProviderCredentialForAdmission{"cred_openai": providerCredentialForSessionTest("cred_openai", "vlt_test", "provider_api_key", "openai", "user_api_key")},
			wantMessage: "providers provider_id must match the agent model provider",
		},
		{
			name:        "missing credential",
			vaultIDs:    []string{"vlt_test"},
			providers:   ProviderSelectors{"anthropic": {CredentialID: "cred_missing"}},
			credentials: map[string]*ProviderCredentialForAdmission{},
			wantMessage: "provider credential not found",
		},
		{
			name:      "archived credential",
			vaultIDs:  []string{"vlt_test"},
			providers: ProviderSelectors{"anthropic": {CredentialID: "cred_archived"}}, //nolint:gosec // Test credential id, not a secret.
			credentials: map[string]*ProviderCredentialForAdmission{
				"cred_archived": providerCredentialForSessionTestWithArchive("cred_archived", "vlt_test", "provider_api_key", "anthropic", "user_api_key", true),
			},
			wantMessage: "provider credential is inaccessible",
		},
		{
			name:      "revoked credential",
			vaultIDs:  []string{"vlt_test"},
			providers: ProviderSelectors{"anthropic": {CredentialID: "cred_revoked"}}, //nolint:gosec // Test credential id, not a secret.
			credentials: map[string]*ProviderCredentialForAdmission{
				"cred_revoked": providerCredentialForSessionTestWithLifecycle("cred_revoked", "vlt_test", "provider_api_key", "anthropic", "user_api_key", false, true),
			},
			wantMessage: "provider credential is inaccessible",
		},
		{
			name:      "mcp credential",
			vaultIDs:  []string{"vlt_test"},
			providers: ProviderSelectors{"anthropic": {CredentialID: "cred_mcp"}},
			credentials: map[string]*ProviderCredentialForAdmission{
				"cred_mcp": providerCredentialForSessionTest("cred_mcp", "vlt_test", "static_bearer", "", ""),
			},
			wantMessage: "provider credential is inaccessible",
		},
		{
			name:      "credential provider mismatch",
			vaultIDs:  []string{"vlt_test"},
			providers: ProviderSelectors{"anthropic": {CredentialID: "cred_wrong_provider"}}, //nolint:gosec // Test credential id, not a secret.
			credentials: map[string]*ProviderCredentialForAdmission{
				"cred_wrong_provider": providerCredentialForSessionTest("cred_wrong_provider", "vlt_test", "provider_api_key", "openai", "user_api_key"),
			},
			wantMessage: "provider credential is inaccessible",
		},
		{
			name:      "credential vault not bound",
			vaultIDs:  []string{"vlt_other"},
			providers: ProviderSelectors{"anthropic": {CredentialID: "cred_anthropic"}}, //nolint:gosec // Test credential id, not a secret.
			credentials: map[string]*ProviderCredentialForAdmission{
				"cred_anthropic": providerCredentialForSessionTest("cred_anthropic", "vlt_test", "provider_oauth", "anthropic", "oauth"),
			},
			wantMessage: "provider credential is inaccessible",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := newRecordingSessionStore()
			for id, credential := range tc.credentials {
				store.providerCredentials[id] = credential
			}
			service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, fixed)

			_, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
				Agent:         AgentReference{ID: "agent_test"},
				EnvironmentID: "env_test",
				VaultIDs:      tc.vaultIDs,
				Providers:     tc.providers,
			})
			if err == nil {
				t.Fatal("Create succeeded; want provider selector rejection")
			}
			if !strings.Contains(err.Error(), tc.wantMessage) {
				t.Fatalf("Create error = %q; want %q", err.Error(), tc.wantMessage)
			}
			if len(store.sessions) != 0 || len(store.providerAuth) != 0 {
				t.Fatalf("state persisted after provider selector rejection: sessions=%+v provider_auth=%+v", store.sessions, store.providerAuth)
			}
		})
	}
}

func TestGetProjectsPublicSessionUsageDTO(t *testing.T) {
	createdAt := time.Date(2026, 5, 11, 12, 30, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, createdAt)
	ephemeral1h := int64(2)
	ephemeral5m := int64(0)
	stored := testStoredSession(createdAt)
	stored.ApprovalMode = ApprovalModeFullAccess
	stored.Usage = Usage{
		InputTokens:          11,
		OutputTokens:         7,
		CacheReadInputTokens: 3,
		CacheCreation: UsageCacheCreation{
			Ephemeral1hInputTokens: &ephemeral1h,
			Ephemeral5mInputTokens: &ephemeral5m,
		},
		ServerToolUse: UsageServerToolUse{
			WebSearchRequests: 2,
			WebFetchRequests:  1,
		},
	}
	store.sessions[stored.ID] = stored

	response, err := service.Get(context.Background(), workspace.DefaultID, stored.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if response.Usage.InputTokens != 11 || response.Usage.OutputTokens != 7 || response.Usage.CacheReadInputTokens != 3 {
		t.Fatalf("usage = %+v; want public token counts", response.Usage)
	}
	if response.Usage.CacheCreation.Ephemeral1hInputTokens == nil || *response.Usage.CacheCreation.Ephemeral1hInputTokens != 2 {
		t.Fatalf("usage cache_creation 1h = %v; want 2", response.Usage.CacheCreation.Ephemeral1hInputTokens)
	}
	if response.Usage.ServerToolUse.WebSearchRequests != 2 || response.Usage.ServerToolUse.WebFetchRequests != 1 {
		t.Fatalf("usage server_tool_use = %+v; want web search/fetch counts", response.Usage.ServerToolUse)
	}
	if response.Agent.ApprovalMode != agent.ApprovalModeFullAccess {
		t.Fatalf("session approval mode = %q; want stored full_access", response.Agent.ApprovalMode)
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("Marshal response: %v", err)
	}
	body := string(encoded)
	for _, want := range []string{
		`"outcome_evaluations":[]`,
		`"usage":{"input_tokens":11`,
		`"output_tokens":7`,
		`"cache_creation":{"ephemeral_1h_input_tokens":2,"ephemeral_5m_input_tokens":0}`,
		`"cache_read_input_tokens":3`,
		`"server_tool_use":{"web_search_requests":2,"web_fetch_requests":1}`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("response JSON missing %s: %s", want, body)
		}
	}
	var projected map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &projected); err != nil {
		t.Fatalf("decode projected session: %v", err)
	}
	var projectedAgent map[string]json.RawMessage
	if err := json.Unmarshal(projected["agent"], &projectedAgent); err != nil {
		t.Fatalf("decode projected session agent: %v", err)
	}
	for _, forbidden := range []string{"metadata", "created_at", "updated_at", "archived_at"} {
		if _, ok := projectedAgent[forbidden]; ok {
			t.Fatalf("session agent leaked Agent resource field %q: %s", forbidden, projected["agent"])
		}
	}
	if string(projectedAgent["model"]) != `{"id":"anthropic/claude-opus-4-8"}` || string(projectedAgent["multiagent"]) != "null" {
		t.Fatalf("session agent model/multiagent shape = %s", projected["agent"])
	}
	for _, forbidden := range []string{"input_total_tokens", "input_uncached_tokens", "reasoning_output_tokens", "provider_usage_json", "cache_creation_input_tokens", "request_count"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("response JSON leaked internal usage field %q: %s", forbidden, body)
		}
	}
}

func TestThreadResponseProjectsThreadUsageDTO(t *testing.T) {
	createdAt := time.Date(2026, 5, 11, 12, 31, 0, 0, time.UTC)
	ephemeral1h := int64(4)
	thread := &Thread{
		ID:              "thrd_usage",
		SessionID:       "sesn_usage",
		WorkspaceID:     workspace.DefaultID,
		Role:            ThreadRoleMain,
		Visibility:      ThreadVisibilityPublic,
		Status:          ThreadStatusIdle,
		CreatedAt:       createdAt,
		LastActiveAt:    createdAt,
		UpdatedAt:       createdAt,
		StorageSequence: 1,
		Usage: Usage{
			InputTokens:          13,
			OutputTokens:         8,
			CacheReadInputTokens: 5,
			CacheCreation: UsageCacheCreation{
				Ephemeral1hInputTokens: &ephemeral1h,
			},
			ServerToolUse: UsageServerToolUse{
				WebSearchRequests: 3,
				WebFetchRequests:  2,
			},
		},
	}
	response, err := assembleThreadResponse(thread, &agent.Agent{
		ID:      "agent_test",
		Type:    "agent",
		Version: 1,
		AgentConfig: agent.AgentConfig{
			Name:  "Agent",
			Model: "anthropic/claude-opus-4-8",
		},
		CreatedAt: createdAt,
		UpdatedAt: createdAt,
	}, nil, ApprovalModeApproveForMe)
	if err != nil {
		t.Fatalf("assembleThreadResponse: %v", err)
	}
	if response.Usage.InputTokens != 13 || response.Usage.OutputTokens != 8 || response.Usage.CacheReadInputTokens != 5 {
		t.Fatalf("thread usage = %+v; want projected session usage", response.Usage)
	}
	if response.Agent.ApprovalMode != agent.ApprovalModeApproveForMe {
		t.Fatalf("thread approval mode = %q; want session override approve_for_me", response.Agent.ApprovalMode)
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("Marshal response: %v", err)
	}
	body := string(encoded)
	if !strings.Contains(body, `"usage":{"input_tokens":13,"output_tokens":8,"cache_creation":{"ephemeral_1h_input_tokens":4,"ephemeral_5m_input_tokens":null},"cache_read_input_tokens":5}`) {
		t.Fatalf("thread response JSON missing public projected usage shape: %s", body)
	}
	if strings.Contains(body, `"server_tool_use"`) {
		t.Fatalf("thread response JSON contains session-only server tool usage: %s", body)
	}
	for _, forbidden := range []string{"input_total_tokens", "input_uncached_tokens", "reasoning_output_tokens", "provider_usage_json", "cache_creation_input_tokens", "request_count"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("thread response JSON leaked internal usage field %q: %s", forbidden, body)
		}
	}
}

func TestSessionStatsAccumulateRunningTimeAndFreezeTerminalDuration(t *testing.T) {
	created := time.Date(2026, 7, 13, 10, 0, 0, 0, time.UTC)
	runningSince := created.Add(20 * time.Second)
	running := &Session{
		Status: StatusRunning, CreatedAt: created, RunningSince: &runningSince, ActiveSecondsTotal: 7,
	}
	if got := sessionStats(running, created.Add(50*time.Second)); got.ActiveSeconds != 37 || got.DurationSeconds != 50 {
		t.Fatalf("running stats = %+v; want active 37 duration 50", got)
	}
	terminatedAt := created.Add(80 * time.Second)
	terminated := &Session{
		Status: StatusTerminated, CreatedAt: created, TerminatedAt: &terminatedAt, ActiveSecondsTotal: 42,
	}
	if got := sessionStats(terminated, created.Add(5*time.Minute)); got.ActiveSeconds != 42 || got.DurationSeconds != 80 {
		t.Fatalf("terminated stats = %+v; want active 42 duration 80", got)
	}
}

func TestThreadResponseStatsAreNullThisStage(t *testing.T) {
	response, err := assembleThreadResponse(&Thread{
		ID: "thread_stats_null", SessionID: "sesn_stats_null", Status: ThreadStatusIdle,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}, &agent.Agent{
		ID: "agent_stats_null", Type: "agent", Version: 1,
		AgentConfig: agent.AgentConfig{Model: "openai/gpt-5.5"},
	}, nil, ApprovalModeAskForApproval)
	if err != nil {
		t.Fatalf("assembleThreadResponse: %v", err)
	}
	body, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("marshal thread response: %v", err)
	}
	if !bytes.Contains(body, []byte(`"stats":null`)) {
		t.Fatalf("thread response = %s; want stats:null", body)
	}
}

func TestCreatePersistsSessionPrimaryThreadAndResources(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	fileIdentities := &recordingFileIdentities{}
	envReader := &recordingEnvironmentReader{env: &environment.Environment{
		ID:   "env_test",
		Type: "environment",
		Config: environment.EnvironmentConfig{
			Type: "cloud",
			Networking: &environment.NetworkingConfig{
				Type:             "cidr_allow_list",
				NetworkAllowList: "10.0.0.0/8",
			},
			Packages: environment.PackageMap{
				"apt": []string{"git"},
				"go":  []string{"golang.org/x/tools/cmd/stringer"},
			},
		},
	}}
	service := NewService(
		testAgents{},
		envReader,
		fileIdentities,
		testMemories{},
		&recordingVaultValidator{},
		store,
		testSessionEncryptor{},
		WithClock(func() time.Time { return fixed }),
	)
	service.sessionIDStrategy = func() string { return "sesn_test" }
	service.threadIDStrategy = func() string { return "thread_test" }
	resourceCount := 0
	service.resourceIDStrategy = func() string {
		resourceCount++
		return "sesrsc_" + strconv.Itoa(resourceCount)
	}
	fileCount := 0
	service.fileIDStrategy = func() string {
		fileCount++
		return "file_session_" + strconv.Itoa(fileCount)
	}

	fileMount := "/workspace/input.txt"
	checkout := &GitHubCheckout{Type: "branch", Name: "main"}
	response, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent:         AgentReference{ID: "agent_test"},
		EnvironmentID: "env_test",
		Resources: []ResourceRequest{
			{Type: string(ResourceTypeFile), FileID: "file_source", MountPath: &fileMount},
			{Type: string(ResourceTypeMemoryStore), MemoryStoreID: "memstore_test", Access: "read_only", Instructions: "use carefully"},
			{Type: string(ResourceTypeGitHubRepository), GitHubURL: "https://github.com/tetral-ai/tetral", AuthorizationToken: "github_resource_token", Checkout: checkout},
		},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if response.Status != StatusIdle {
		t.Fatalf("response status = %s; want idle", response.Status)
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("Marshal response: %v", err)
	}
	for _, forbidden := range []string{"sandbox", "thread_", "primary_thread", `"provider":`} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("public response leaked internal startup field %q: %s", forbidden, encoded)
		}
	}
	thread := store.threads["thread_test"]
	if thread == nil {
		t.Fatal("primary thread was not persisted")
		return
	}
	if thread.SessionID != "sesn_test" || thread.ParentThreadID != nil || thread.Status != ThreadStatusIdle {
		t.Fatalf("primary thread = %+v; want session primary idle thread", thread)
	}
	if !strings.HasPrefix(thread.ID, "thread_") && thread.ID != "thread_test" {
		t.Fatalf("thread id = %q; want thread_ prefix", thread.ID)
	}
	encodedResources, err := json.Marshal(store.sessions["sesn_test"].Resources)
	if err != nil {
		t.Fatalf("Marshal session resources: %v", err)
	}
	if strings.Contains(string(encodedResources), "encrypted:") || strings.Contains(string(encodedResources), "skill") {
		t.Fatalf("session resource admission leaked credential material or skills: %s", encodedResources)
	}
}

func TestCreateDefaultsOmittedFileMountPathBeforePersistence(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, fixed)

	response, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent:         AgentReference{ID: "agent_test"},
		EnvironmentID: "env_test",
		Resources: []ResourceRequest{{
			Type:   string(ResourceTypeFile),
			FileID: "file_source",
		}},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	wantMountPath := "/mnt/session/uploads/file_session_1"
	if len(response.Resources) != 1 || response.Resources[0].MountPath != wantMountPath || response.Resources[0].FileID != "file_session_1" {
		t.Fatalf("response resources = %+v; want default session upload mount path", response.Resources)
	}
	if got := store.sessions["sesn_test"].Resources[0].File.MountPath; got != wantMountPath {
		t.Fatalf("stored mount_path = %q; want %q", got, wantMountPath)
	}
}

func TestCreateCommitsSessionAndPrimaryThreadTogether(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, fixed)
	service.sessionIDStrategy = func() string { return "sesn_boundary" }
	service.threadIDStrategy = func() string { return "thread_boundary" }

	response, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent:         AgentReference{ID: "agent_test"},
		EnvironmentID: "env_test",
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if response.ID != "sesn_boundary" || response.Status != StatusIdle {
		t.Fatalf("response = %+v; want committed idle session", response)
	}
	if store.committedTxCount != 1 || store.sessions["sesn_boundary"] == nil || store.threads["thread_boundary"] == nil {
		t.Fatalf("committed baseline rows: commits=%d session=%+v thread=%+v", store.committedTxCount, store.sessions["sesn_boundary"], store.threads["thread_boundary"])
	}
}

func TestCreateWithMCPConfigurationCommitsIdleSession(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	agents := staticAgentReader{agent: &agent.Agent{
		ID:      "agent_test",
		Type:    "agent",
		Version: 1,
		AgentConfig: agent.AgentConfig{
			Name:  "MCP agent",
			Model: "anthropic/claude-opus-4-8",
			Tools: agent.RawArray{
				json.RawMessage(`{"type":"tetral_agent_toolset","family":"claude"}`),
				json.RawMessage(`{"type":"mcp_toolset","mcp_server_name":"github"}`),
			},
			MCPServers: agent.RawArray{json.RawMessage(`{"type":"url","name":"github","url":"https://example.com/mcp"}`)},
		},
		CreatedAt: fixed,
		UpdatedAt: fixed,
	}}
	environments := &recordingEnvironmentReader{env: &environment.Environment{
		ID:   "env_test",
		Type: "environment",
		Config: environment.EnvironmentConfig{
			Type:       "cloud",
			Networking: &environment.NetworkingConfig{Type: "cidr_allow_list", NetworkAllowList: "10.0.0.0/8"},
		},
	}}
	service := NewService(
		agents,
		environments,
		&recordingFileIdentities{},
		testMemories{},
		&recordingVaultValidator{},
		store,
		testSessionEncryptor{},
		WithClock(func() time.Time { return fixed }),
	)
	service.sessionIDStrategy = func() string { return "sesn_test" }
	service.threadIDStrategy = func() string { return "thread_test" }

	response, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent:         AgentReference{ID: "agent_test"},
		EnvironmentID: "env_test",
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if response.Status != StatusIdle {
		t.Fatalf("status = %s; want idle", response.Status)
	}
	if store.sessions["sesn_test"] == nil || store.threads["thread_test"] == nil {
		t.Fatalf("session=%+v thread=%+v; want committed session and primary thread", store.sessions["sesn_test"], store.threads["thread_test"])
	}
}

func TestCreateRollsBackSessionAndThreadWhenBaselineCommitFails(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	durableErr := errors.New("synthetic durable commit failure")
	store := newRecordingSessionStore()
	store.commitErr = durableErr
	service := NewService(
		testAgents{},
		testEnvironments{},
		&recordingFileIdentities{},
		testMemories{},
		&recordingVaultValidator{},
		store,
		testSessionEncryptor{},
		WithClock(func() time.Time { return fixed }),
	)
	service.sessionIDStrategy = func() string { return "sesn_commit_failure" }
	service.threadIDStrategy = func() string { return "thread_commit_failure" }

	response, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent:         AgentReference{ID: "agent_test"},
		EnvironmentID: "env_test",
	})
	if !errors.Is(err, durableErr) {
		t.Fatalf("Create err = %T %v; want durableErr", err, err)
	}
	if response != nil {
		t.Fatalf("response = %+v; want nil after durable failure", response)
	}
	if len(store.sessions) != 0 || len(store.threads) != 0 {
		t.Fatalf("durable state persisted after commit failure: sessions=%d threads=%d", len(store.sessions), len(store.threads))
	}
}

func TestSessionStatusSurfaceRejectsInternalSandboxStatuses(t *testing.T) {
	accepted := []Status{StatusIdle, StatusRunning, StatusRescheduling, StatusTerminated}
	for _, status := range accepted {
		if err := validateStatus(status); err != nil {
			t.Fatalf("validateStatus(%q): %v", status, err)
		}
	}
	rejected := []Status{"preparing", "ready", "sandbox_running", "creating", "active", "releasing", "released", "failed", "cloudflare_running", "e2b_ready", "modal_started"}
	for _, status := range rejected {
		if err := validateStatus(status); err == nil {
			t.Fatalf("validateStatus(%q) succeeded; want rejection", status)
		}
	}
}

func TestCreatePersistsGitHubCommitCheckoutAsCanonicalSHA(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, fixed)
	rawSHA := "ABCDEF0123456789ABCDEF0123456789ABCDEF01"

	response, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent:         AgentReference{ID: "agent_test", Version: intPtr(2)},
		EnvironmentID: "env_test",
		Resources: []ResourceRequest{
			{
				Type:               string(ResourceTypeGitHubRepository),
				GitHubURL:          "https://github.com/tetral-ai/tetral",
				AuthorizationToken: "github_resource_token",
				Checkout:           &GitHubCheckout{Type: "commit", SHA: rawSHA},
			},
		},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	wantSHA := strings.ToLower(rawSHA)
	if response.Resources[0].CheckoutType != "commit" || response.Resources[0].CheckoutRef != wantSHA {
		t.Fatalf("response checkout = %s/%s; want commit/%s", response.Resources[0].CheckoutType, response.Resources[0].CheckoutRef, wantSHA)
	}
	stored := store.sessions["sesn_test"].Resources[0].GitHubRepository
	if stored.CheckoutType != "commit" || stored.CheckoutRef != wantSHA {
		t.Fatalf("stored checkout = %s/%s; want commit/%s", stored.CheckoutType, stored.CheckoutRef, wantSHA)
	}
}

func TestCreatePreservesExplicitGitHubMountPathUnderWorkspace(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, fixed)
	mountPath := "/workspace/repos/tetral"

	response, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent:         AgentReference{ID: "agent_test", Version: intPtr(2)},
		EnvironmentID: "env_test",
		Resources: []ResourceRequest{
			{
				Type:               string(ResourceTypeGitHubRepository),
				GitHubURL:          "https://github.com/tetral-ai/tetral",
				AuthorizationToken: "github_resource_token",
				MountPath:          &mountPath,
			},
		},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if response.Resources[0].MountPath != mountPath {
		t.Fatalf("response github mount_path = %q; want %q", response.Resources[0].MountPath, mountPath)
	}
	stored := store.sessions["sesn_test"].Resources[0].GitHubRepository
	if stored.MountPath != mountPath {
		t.Fatalf("stored github mount_path = %q; want %q", stored.MountPath, mountPath)
	}
}

func TestCreateRejectsGitHubMountPathOutsideWorkspace(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, fixed)
	mountPath := "/tmp/repos/tetral"

	_, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent:         AgentReference{ID: "agent_test", Version: intPtr(2)},
		EnvironmentID: "env_test",
		Resources: []ResourceRequest{
			{
				Type:               string(ResourceTypeGitHubRepository),
				GitHubURL:          "https://github.com/tetral-ai/tetral",
				AuthorizationToken: "github_resource_token",
				MountPath:          &mountPath,
			},
		},
	})
	var validation *ValidationError
	if !errors.As(err, &validation) || validation.Message != "mount_path root is invalid" {
		t.Fatalf("Create err = %T %v; want mount_path root validation", err, err)
	}
	if len(store.sessions) != 0 {
		t.Fatalf("sessions persisted despite outside-workspace github mount_path: %v", store.sessions)
	}
}

func TestCreateRejectsGitHubMountPathOverReservedSubtree(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, fixed)
	mountPath := "/mnt/tetral/r2/repo"

	_, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent:         AgentReference{ID: "agent_test", Version: intPtr(2)},
		EnvironmentID: "env_test",
		Resources: []ResourceRequest{
			{
				Type:               string(ResourceTypeGitHubRepository),
				GitHubURL:          "https://github.com/tetral-ai/tetral",
				AuthorizationToken: "github_resource_token",
				MountPath:          &mountPath,
			},
		},
	})
	var validation *ValidationError
	if !errors.As(err, &validation) || validation.Message != "mount_path overlaps a reserved path" {
		t.Fatalf("Create err = %T %v; want reserved mount_path validation", err, err)
	}
	if len(store.sessions) != 0 {
		t.Fatalf("sessions persisted despite reserved github mount_path: %v", store.sessions)
	}
}

func TestCreateEnforcesDocumentedResourceCountBoundaries(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)

	t.Run("accepts exactly one hundred file resources", func(t *testing.T) {
		store := newRecordingSessionStore()
		fileIdentities := &recordingFileIdentities{}
		service := newTestService(store, fileIdentities, &recordingVaultValidator{}, fixed)
		resources := make([]ResourceRequest, 0, maxFileResources)
		for index := 0; index < maxFileResources; index++ {
			resources = append(resources, ResourceRequest{
				Type:   string(ResourceTypeFile),
				FileID: "file_source_" + strconv.Itoa(index),
			})
		}

		response, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
			Agent:         AgentReference{ID: "agent_test"},
			EnvironmentID: "env_test",
			Resources:     resources,
		})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		if len(response.Resources) != maxFileResources {
			t.Fatalf("resources = %d; want %d", len(response.Resources), maxFileResources)
		}
		if len(fileIdentities.created) != maxFileResources {
			t.Fatalf("created file identities = %d; want %d", len(fileIdentities.created), maxFileResources)
		}
	})

	t.Run("rejects the 101st file resource", func(t *testing.T) {
		store := newRecordingSessionStore()
		fileIdentities := &recordingFileIdentities{}
		service := newTestService(store, fileIdentities, &recordingVaultValidator{}, fixed)
		resources := make([]ResourceRequest, 0, maxFileResources+1)
		for index := 0; index <= maxFileResources; index++ {
			resources = append(resources, ResourceRequest{
				Type:   string(ResourceTypeFile),
				FileID: "file_source_" + strconv.Itoa(index),
			})
		}

		_, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
			Agent:         AgentReference{ID: "agent_test"},
			EnvironmentID: "env_test",
			Resources:     resources,
		})
		if err == nil {
			t.Fatal("Create succeeded; want too many file resources")
		}
		var validation *ValidationError
		if !errors.As(err, &validation) {
			t.Fatalf("err = %T %v; want ValidationError", err, err)
		}
		if validation.Message != "too many file resources" {
			t.Fatalf("validation message = %q; want file count rejection", validation.Message)
		}
		if len(store.sessions) != 0 || len(fileIdentities.created) != 0 {
			t.Fatalf("state persisted after rejected file count: sessions=%d created=%d", len(store.sessions), len(fileIdentities.created))
		}
	})

	t.Run("accepts exactly eight memory store resources", func(t *testing.T) {
		store := newRecordingSessionStore()
		service := NewService(
			testAgents{},
			testEnvironments{},
			&recordingFileIdentities{},
			memoryReaderByID{},
			&recordingVaultValidator{},
			store,
			testSessionEncryptor{},
			WithClock(func() time.Time { return fixed }),
		)
		service.sessionIDStrategy = func() string { return "sesn_test" }
		resourceCount := 0
		service.resourceIDStrategy = func() string {
			resourceCount++
			return "sesrsc_" + strconv.Itoa(resourceCount)
		}
		resources := make([]ResourceRequest, 0, maxMemoryResources)
		for index := 0; index < maxMemoryResources; index++ {
			resources = append(resources, ResourceRequest{
				Type:          string(ResourceTypeMemoryStore),
				MemoryStoreID: "memstore_" + strconv.Itoa(index),
			})
		}

		response, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
			Agent:         AgentReference{ID: "agent_test"},
			EnvironmentID: "env_test",
			Resources:     resources,
		})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		if len(response.Resources) != maxMemoryResources {
			t.Fatalf("resources = %d; want %d", len(response.Resources), maxMemoryResources)
		}
		for _, resource := range response.Resources {
			if resource.Access != "read_only" {
				t.Fatalf("omitted memory_store access = %q; want read_only", resource.Access)
			}
		}
	})

	t.Run("accepts explicit read_write memory store access", func(t *testing.T) {
		store := newRecordingSessionStore()
		service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, fixed)
		response, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
			Agent:         AgentReference{ID: "agent_test"},
			EnvironmentID: "env_test",
			Resources: []ResourceRequest{{
				Type:          string(ResourceTypeMemoryStore),
				MemoryStoreID: "memstore_test",
				Access:        "read_write",
			}},
		})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		if len(response.Resources) != 1 || response.Resources[0].Access != "read_write" {
			t.Fatalf("memory_store access = %+v; want explicit read_write", response.Resources)
		}
		if stored := store.sessions["sesn_test"].Resources[0]; stored.MemoryStore == nil || stored.MemoryStore.Access != "read_write" {
			t.Fatalf("stored memory_store access = %+v; want explicit read_write", stored)
		}
	})

	t.Run("rejects the ninth memory store resource", func(t *testing.T) {
		store := newRecordingSessionStore()
		service := NewService(
			testAgents{},
			testEnvironments{},
			&recordingFileIdentities{},
			memoryReaderByID{},
			&recordingVaultValidator{},
			store,
			testSessionEncryptor{},
			WithClock(func() time.Time { return fixed }),
		)
		resources := make([]ResourceRequest, 0, maxMemoryResources+1)
		for index := 0; index <= maxMemoryResources; index++ {
			resources = append(resources, ResourceRequest{
				Type:          string(ResourceTypeMemoryStore),
				MemoryStoreID: "memstore_" + strconv.Itoa(index),
			})
		}

		_, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
			Agent:         AgentReference{ID: "agent_test"},
			EnvironmentID: "env_test",
			Resources:     resources,
		})
		if err == nil {
			t.Fatal("Create succeeded; want too many memory_store resources")
		}
		var validation *ValidationError
		if !errors.As(err, &validation) {
			t.Fatalf("err = %T %v; want ValidationError", err, err)
		}
		if validation.Message != "too many memory_store resources" {
			t.Fatalf("validation message = %q; want memory count rejection", validation.Message)
		}
		if len(store.sessions) != 0 {
			t.Fatalf("sessions persisted after rejected memory count: %v", store.sessions)
		}
	})
}

func TestCreateEnforcesUnicodeCodePointBoundaries(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	validKey := strings.Repeat("\u754c", maxMetadataKeyRunes)
	validValue := strings.Repeat("\u754c", maxMetadataValueRunes)

	store := newRecordingSessionStore()
	service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, fixed)
	if _, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent:         AgentReference{ID: "agent_test"},
		EnvironmentID: "env_test",
		Metadata:      map[string]string{validKey: validValue},
	}); err != nil {
		t.Fatalf("Create with multibyte metadata at limit: %v", err)
	}

	tests := []struct {
		name     string
		metadata map[string]string
		message  string
	}{
		{
			name:     "metadata key one code point over",
			metadata: map[string]string{strings.Repeat("\u754c", maxMetadataKeyRunes+1): "value"},
			message:  "metadata key is invalid",
		},
		{
			name:     "metadata value one code point over",
			metadata: map[string]string{"key": strings.Repeat("\u754c", maxMetadataValueRunes+1)},
			message:  "metadata value is invalid",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			store := newRecordingSessionStore()
			service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, fixed)

			_, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
				Agent:         AgentReference{ID: "agent_test"},
				EnvironmentID: "env_test",
				Metadata:      testCase.metadata,
			})
			if err == nil {
				t.Fatal("Create succeeded; want metadata validation error")
			}
			var validation *ValidationError
			if !errors.As(err, &validation) {
				t.Fatalf("err = %T %v; want ValidationError", err, err)
			}
			if validation.Message != testCase.message {
				t.Fatalf("validation message = %q; want %q", validation.Message, testCase.message)
			}
			if len(store.sessions) != 0 {
				t.Fatalf("sessions persisted after rejected metadata: %v", store.sessions)
			}
		})
	}

	validInstructions := strings.Repeat("\u754c", maxMemoryInstructions)
	store = newRecordingSessionStore()
	service = newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, fixed)
	if _, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent:         AgentReference{ID: "agent_test"},
		EnvironmentID: "env_test",
		Resources: []ResourceRequest{{
			Type:          string(ResourceTypeMemoryStore),
			MemoryStoreID: "memstore_test",
			Instructions:  validInstructions,
		}},
	}); err != nil {
		t.Fatalf("Create with multibyte memory instructions at limit: %v", err)
	}

	store = newRecordingSessionStore()
	service = newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, fixed)
	_, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent:         AgentReference{ID: "agent_test"},
		EnvironmentID: "env_test",
		Resources: []ResourceRequest{{
			Type:          string(ResourceTypeMemoryStore),
			MemoryStoreID: "memstore_test",
			Instructions:  strings.Repeat("\u754c", maxMemoryInstructions+1),
		}},
	})
	if err == nil {
		t.Fatal("Create succeeded; want memory instructions length validation")
	}
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("err = %T %v; want ValidationError", err, err)
	}
	if validation.Message != "memory_store instructions are too long" {
		t.Fatalf("validation message = %q; want memory instructions rejection", validation.Message)
	}
	if len(store.sessions) != 0 {
		t.Fatalf("sessions persisted after rejected memory instructions: %v", store.sessions)
	}
}

func TestCreateRejectsDependencyFailuresBeforePersistence(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	dependencyErr := errors.New("dependency failed")
	archivedAt := fixed.Add(-time.Minute)

	tests := []struct {
		name              string
		agents            AgentReader
		environments      EnvironmentReader
		memories          MemoryStoreReader
		vaults            VaultReferenceValidator
		fileIdentities    *recordingFileIdentities
		request           CreateRequest
		wantErr           error
		wantValidationMsg string
	}{
		{
			name:    "agent lookup",
			agents:  failingAgentReader{err: dependencyErr},
			request: CreateRequest{Agent: AgentReference{ID: "agent_missing"}, EnvironmentID: "env_test"},
			wantErr: dependencyErr,
		},
		{
			name:         "environment lookup",
			environments: failingEnvironmentReader{err: dependencyErr},
			request:      CreateRequest{Agent: AgentReference{ID: "agent_test"}, EnvironmentID: "env_missing"},
			wantErr:      dependencyErr,
		},
		{
			name:              "archived environment",
			environments:      failingEnvironmentReader{env: &environment.Environment{ID: "env_archived", Type: "environment", ArchivedAt: &archivedAt}},
			request:           CreateRequest{Agent: AgentReference{ID: "agent_test"}, EnvironmentID: "env_archived"},
			wantValidationMsg: "environment is archived",
		},
		{
			name:    "vault lookup",
			vaults:  &recordingVaultValidator{err: dependencyErr},
			request: CreateRequest{Agent: AgentReference{ID: "agent_test"}, EnvironmentID: "env_test", VaultIDs: []string{"vlt_missing"}},
			wantErr: dependencyErr,
		},
		{
			name:     "memory lookup",
			memories: &recordingMemoryReader{err: dependencyErr},
			request: CreateRequest{
				Agent:         AgentReference{ID: "agent_test"},
				EnvironmentID: "env_test",
				Resources:     []ResourceRequest{{Type: string(ResourceTypeMemoryStore), MemoryStoreID: "memstore_missing"}},
			},
			wantErr: dependencyErr,
		},
		{
			name:           "file lookup",
			fileIdentities: &recordingFileIdentities{validateErr: dependencyErr},
			request: CreateRequest{
				Agent:         AgentReference{ID: "agent_test"},
				EnvironmentID: "env_test",
				Resources:     []ResourceRequest{{Type: string(ResourceTypeFile), FileID: "file_missing"}},
			},
			wantErr: dependencyErr,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			store := newRecordingSessionStore()
			agents := testCase.agents
			if agents == nil {
				agents = testAgents{}
			}
			environments := testCase.environments
			if environments == nil {
				environments = testEnvironments{}
			}
			memories := testCase.memories
			if memories == nil {
				memories = testMemories{}
			}
			vaults := testCase.vaults
			if vaults == nil {
				vaults = &recordingVaultValidator{}
			}
			fileIdentities := testCase.fileIdentities
			if fileIdentities == nil {
				fileIdentities = &recordingFileIdentities{}
			}
			service := NewService(
				agents,
				environments,
				fileIdentities,
				memories,
				vaults,
				store,
				testSessionEncryptor{},
				WithClock(func() time.Time { return fixed }),
			)

			_, err := service.Create(context.Background(), workspace.DefaultID, testCase.request)
			if err == nil {
				t.Fatal("Create succeeded; want dependency validation failure")
			}
			if testCase.wantErr != nil && !errors.Is(err, testCase.wantErr) {
				t.Fatalf("Create error = %T %v; want %v", err, err, testCase.wantErr)
			}
			if testCase.wantValidationMsg != "" {
				var validation *ValidationError
				if !errors.As(err, &validation) {
					t.Fatalf("err = %T %v; want ValidationError", err, err)
				}
				if validation.Message != testCase.wantValidationMsg {
					t.Fatalf("validation message = %q; want %q", validation.Message, testCase.wantValidationMsg)
				}
			}
			if len(store.sessions) != 0 || len(fileIdentities.created) != 0 {
				t.Fatalf("state persisted after rejected dependency: sessions=%d file identities=%d", len(store.sessions), len(fileIdentities.created))
			}
		})
	}
}

func TestCreatePropagatesWorkspaceToDependencyBoundaries(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	workspaceID := workspace.ID("workspace_dependency_proof")
	store := newRecordingSessionStore()
	agents := &recordingAgentReader{}
	environments := &recordingEnvironmentReader{}
	memories := &recordingMemoryReader{}
	vaults := &recordingVaultValidator{}
	fileIdentities := &recordingFileIdentities{}
	service := NewService(
		agents,
		environments,
		fileIdentities,
		memories,
		vaults,
		store,
		testSessionEncryptor{},
		WithClock(func() time.Time { return fixed }),
	)
	service.sessionIDStrategy = func() string { return "sesn_workspace_dependency" }
	service.resourceIDStrategy = func() string { return "sesrsc_workspace_dependency" }
	service.fileIDStrategy = func() string { return "file_workspace_dependency" }

	fileMount := "/workspace/input.txt"
	_, err := service.Create(context.Background(), workspaceID, CreateRequest{
		Agent:         AgentReference{ID: "agent_workspace", Version: intPtr(2)},
		EnvironmentID: "env_workspace",
		VaultIDs:      []string{"vlt_workspace"},
		Resources: []ResourceRequest{
			{Type: string(ResourceTypeMemoryStore), MemoryStoreID: "memstore_workspace"},
			{Type: string(ResourceTypeFile), FileID: "file_source_workspace", MountPath: &fileMount},
		},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if agents.versionWorkspaces[0] != workspaceID {
		t.Fatalf("agent workspace = %s; want %s", agents.versionWorkspaces[0], workspaceID)
	}
	if environments.workspaces[0] != workspaceID {
		t.Fatalf("environment workspace = %s; want %s", environments.workspaces[0], workspaceID)
	}
	if memories.workspaces[0] != workspaceID {
		t.Fatalf("memory workspace = %s; want %s", memories.workspaces[0], workspaceID)
	}
	if vaults.workspaces[0] != workspaceID {
		t.Fatalf("vault workspace = %s; want %s", vaults.workspaces[0], workspaceID)
	}
	if fileIdentities.validateWorkspaces[0] != workspaceID {
		t.Fatalf("file validate workspace = %s; want %s", fileIdentities.validateWorkspaces[0], workspaceID)
	}
	if fileIdentities.createWorkspaces[0] != workspaceID {
		t.Fatalf("file create workspace = %s; want %s", fileIdentities.createWorkspaces[0], workspaceID)
	}
}

func TestCreateRejectsOverlappingResourceMountsBeforePersistence(t *testing.T) {
	store := newRecordingSessionStore()
	service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, time.Now().UTC())

	fileMount := "/workspace/tetral"
	_, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent:         AgentReference{ID: "agent_test"},
		EnvironmentID: "env_test",
		Resources: []ResourceRequest{
			{Type: string(ResourceTypeFile), FileID: "file_source", MountPath: &fileMount},
			{Type: string(ResourceTypeGitHubRepository), GitHubURL: "https://github.com/tetral-ai/tetral", AuthorizationToken: "github_resource_token"},
		},
	})
	if err == nil {
		t.Fatal("Create must reject overlapping mounts")
	}
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("err = %T %v; want ValidationError", err, err)
	}
	if len(store.sessions) != 0 {
		t.Fatalf("sessions persisted despite validation failure: %v", store.sessions)
	}
}

func TestCreateRejectsMemoryStoreResourceWithoutIDBeforeLookupOrPersistence(t *testing.T) {
	store := newRecordingSessionStore()
	memories := &recordingMemoryReader{}
	service := NewService(
		testAgents{},
		testEnvironments{},
		&recordingFileIdentities{},
		memories,
		&recordingVaultValidator{},
		store,
		testSessionEncryptor{},
	)

	_, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent:         AgentReference{ID: "agent_test"},
		EnvironmentID: "env_test",
		Resources: []ResourceRequest{{
			Type: string(ResourceTypeMemoryStore),
		}},
	})
	if err == nil {
		t.Fatal("Create must reject memory_store resources without memory_store_id")
	}
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("err = %T %v; want ValidationError", err, err)
	}
	if validation.Message != "memory_store_id is required" {
		t.Fatalf("validation message = %q; want missing memory_store_id", validation.Message)
	}
	if memories.getStoreCalls != 0 {
		t.Fatalf("memory store lookups = %d; want 0", memories.getStoreCalls)
	}
	if len(store.sessions) != 0 {
		t.Fatalf("sessions persisted despite validation failure: %v", store.sessions)
	}
}

func TestCreateValidatesFileResourceInRequestOrderBeforeLaterMemoryLookup(t *testing.T) {
	store := newRecordingSessionStore()
	fileErr := &files.NotFoundError{Message: "file not found"}
	fileIdentities := &recordingFileIdentities{validateErr: fileErr}
	memories := &recordingMemoryReader{err: &memory.NotFoundError{Message: "memory store not found"}}
	service := NewService(
		testAgents{},
		testEnvironments{},
		fileIdentities,
		memories,
		&recordingVaultValidator{},
		store,
		testSessionEncryptor{},
	)

	_, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent:         AgentReference{ID: "agent_test"},
		EnvironmentID: "env_test",
		Resources: []ResourceRequest{
			{Type: string(ResourceTypeFile), FileID: "file_missing"},
			{Type: string(ResourceTypeMemoryStore), MemoryStoreID: "memstore_missing"},
		},
	})
	if !errors.Is(err, fileErr) {
		t.Fatalf("Create error = %T %v; want file validation error", err, err)
	}
	if fileIdentities.validateCalls != 1 || fileIdentities.validated[0] != "file_missing" {
		t.Fatalf("validated files = %v (%d calls); want file_missing once", fileIdentities.validated, fileIdentities.validateCalls)
	}
	if memories.getStoreCalls != 0 {
		t.Fatalf("memory store lookups = %d; want 0", memories.getStoreCalls)
	}
	if len(store.sessions) != 0 {
		t.Fatalf("sessions persisted despite validation failure: %v", store.sessions)
	}
}

func TestCreateValidatesFileResourceInRequestOrderBeforeLaterGitHubValidation(t *testing.T) {
	store := newRecordingSessionStore()
	fileErr := &files.NotFoundError{Message: "file not found"}
	fileIdentities := &recordingFileIdentities{validateErr: fileErr}
	service := NewService(
		testAgents{},
		testEnvironments{},
		fileIdentities,
		testMemories{},
		&recordingVaultValidator{},
		store,
		testSessionEncryptor{},
	)

	_, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent:         AgentReference{ID: "agent_test"},
		EnvironmentID: "env_test",
		Resources: []ResourceRequest{
			{Type: string(ResourceTypeFile), FileID: "file_missing"},
			{Type: string(ResourceTypeGitHubRepository), GitHubURL: "not a github url"},
		},
	})
	if !errors.Is(err, fileErr) {
		t.Fatalf("Create error = %T %v; want file validation error", err, err)
	}
	if fileIdentities.validateCalls != 1 || fileIdentities.validated[0] != "file_missing" {
		t.Fatalf("validated files = %v (%d calls); want file_missing once", fileIdentities.validated, fileIdentities.validateCalls)
	}
	if len(store.sessions) != 0 {
		t.Fatalf("sessions persisted despite validation failure: %v", store.sessions)
	}
}

func TestCreateRejectsResourceFieldsOutsideSelectedTypeBeforePersistence(t *testing.T) {
	fileMount := "/workspace/input.txt"
	tests := []struct {
		name      string
		resource  ResourceRequest
		forbidden string
	}{
		{
			name: "file rejects github url field",
			resource: ResourceRequest{
				Type:      string(ResourceTypeFile),
				FileID:    "file_source",
				GitHubURL: "https://github.com/tetral-ai/forbidden-file-field",
			},
			forbidden: "forbidden-file-field",
		},
		{
			name: "memory rejects file fields",
			resource: ResourceRequest{
				Type:          string(ResourceTypeMemoryStore),
				MemoryStoreID: "memstore_test",
				FileID:        "file_source",
				MountPath:     &fileMount,
			},
			forbidden: "file_source",
		},
		{
			name: "github rejects memory fields",
			resource: ResourceRequest{
				Type:          string(ResourceTypeGitHubRepository),
				GitHubURL:     "https://github.com/tetral-ai/tetral",
				MemoryStoreID: "memstore_test",
				Access:        "read_only",
			},
			forbidden: "memstore_test",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := newRecordingSessionStore()
			fileIdentities := &recordingFileIdentities{}
			memories := &recordingMemoryReader{}
			service := NewService(
				testAgents{},
				testEnvironments{},
				fileIdentities,
				memories,
				&recordingVaultValidator{},
				store,
				testSessionEncryptor{},
			)

			_, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
				Agent:         AgentReference{ID: "agent_test"},
				EnvironmentID: "env_test",
				Resources:     []ResourceRequest{tc.resource},
			})
			if err == nil {
				t.Fatal("Create must reject resource fields outside the selected type")
			}
			var validation *ValidationError
			if !errors.As(err, &validation) {
				t.Fatalf("err = %T %v; want ValidationError", err, err)
			}
			if strings.Contains(err.Error(), tc.forbidden) {
				t.Fatalf("validation error echoed rejected field value: %q", err.Error())
			}
			if fileIdentities.validateCalls != 0 || len(fileIdentities.created) != 0 {
				t.Fatalf("file validation/create ran before type closure: validate=%d created=%+v", fileIdentities.validateCalls, fileIdentities.created)
			}
			if memories.getStoreCalls != 0 {
				t.Fatalf("memory lookups = %d; want 0", memories.getStoreCalls)
			}
			if len(store.sessions) != 0 {
				t.Fatalf("sessions persisted despite validation failure: %v", store.sessions)
			}
		})
	}
}

func TestAddResourceRejectsResourceFieldsOutsideFileTypeBeforeMutation(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	fileIdentities := &recordingFileIdentities{}
	service := newTestService(store, fileIdentities, &recordingVaultValidator{}, fixed)
	store.sessions["sesn_test"] = testStoredSession(fixed)

	request := ResourceRequest{
		Type:      string(ResourceTypeFile),
		FileID:    "file_source",
		GitHubURL: "https://github.com/tetral-ai/forbidden-add-field",
	}
	_, err := service.AddResource(context.Background(), workspace.DefaultID, "sesn_test", request)
	if err == nil {
		t.Fatal("AddResource must reject fields outside the selected file type")
	}
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("err = %T %v; want ValidationError", err, err)
	}
	if strings.Contains(err.Error(), "forbidden-add-field") {
		t.Fatalf("validation error echoed rejected field value: %q", err.Error())
	}
	if len(fileIdentities.created) != 0 {
		t.Fatalf("session file identity created despite type-closure failure: %+v", fileIdentities.created)
	}
	if got := len(store.sessions["sesn_test"].Resources); got != 0 {
		t.Fatalf("resources created after type-closure failure = %d; want 0", got)
	}
}

func TestAddResourceRejectsGitHubRepositoryWithoutMutation(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, fixed)
	store.sessions["sesn_test"] = testStoredSession(fixed)

	_, err := service.AddResource(context.Background(), workspace.DefaultID, "sesn_test", ResourceRequest{
		Type:               string(ResourceTypeGitHubRepository),
		GitHubURL:          "https://github.com/tetral-ai/tetral",
		AuthorizationToken: "github-token",
	})
	var validation *ValidationError
	if !errors.As(err, &validation) || validation.Message != "only file resources can be added after session creation" {
		t.Fatalf("AddResource err = %T %v; want file-only admission rejection", err, err)
	}
	if got := len(store.sessions["sesn_test"].Resources); got != 0 {
		t.Fatalf("resources created after GitHub add rejection = %d; want 0", got)
	}
}

func TestResolveAgentForCreateRequiresPositiveExplicitVersion(t *testing.T) {
	tests := []struct {
		name             string
		version          *int
		wantGetCalls     int
		wantVersionCalls []int
		wantValidation   bool
	}{
		{
			name:         "omitted version resolves latest",
			version:      nil,
			wantGetCalls: 1,
		},
		{
			name:             "positive version resolves pinned version",
			version:          intPtr(2),
			wantVersionCalls: []int{2},
		},
		{
			name:           "explicit zero version is invalid",
			version:        intPtr(0),
			wantValidation: true,
		},
		{
			name:           "negative version is invalid",
			version:        intPtr(-1),
			wantValidation: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			agents := &recordingAgentReader{}
			service := &Service{agents: agents}

			_, err := service.resolveAgentForCreate(context.Background(), workspace.DefaultID, AgentReference{
				ID:      "agent_test",
				Version: tc.version,
			})

			if tc.wantValidation {
				if err == nil {
					t.Fatal("resolveAgentForCreate succeeded; want validation error")
				}
				var validation *ValidationError
				if !errors.As(err, &validation) {
					t.Fatalf("err = %T %v; want ValidationError", err, err)
				}
			} else if err != nil {
				t.Fatalf("resolveAgentForCreate: %v", err)
			}
			if agents.getCalls != tc.wantGetCalls {
				t.Fatalf("Get calls = %d; want %d", agents.getCalls, tc.wantGetCalls)
			}
			if strings.Join(agents.versionCallStrings(), ",") != strings.Join(intStrings(tc.wantVersionCalls), ",") {
				t.Fatalf("GetVersion calls = %v; want %v", agents.versionCalls, tc.wantVersionCalls)
			}
		})
	}
}

func TestUpdateValidatesMetadataAfterLockingSession(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, fixed)
	stored := testStoredSession(fixed)
	stored.Metadata = metadataWithPairs(15)
	store.sessions["sesn_test"] = stored
	store.beforeTx = func() {
		store.sessions["sesn_test"].Metadata["concurrent"] = "value"
	}
	value := "value"

	_, err := service.Update(context.Background(), workspace.DefaultID, "sesn_test", UpdateRequest{
		MetadataPatch: map[string]*string{"requested": &value},
	})
	if err == nil {
		t.Fatal("Update must reject metadata that exceeds the limit after the session row is locked")
	}
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("err = %T %v; want ValidationError", err, err)
	}
	if _, ok := store.sessions["sesn_test"].Metadata["requested"]; ok {
		t.Fatalf("over-limit metadata was committed: %#v", store.sessions["sesn_test"].Metadata)
	}
	if got := len(store.sessions["sesn_test"].Metadata); got != maxMetadataPairs {
		t.Fatalf("metadata entries after rejected update = %d; want %d", got, maxMetadataPairs)
	}
}

func TestMutableSessionMutationsDoNotRequireSandboxReadiness(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name   string
		mutate func(*testing.T, *Service) error
		assert func(*testing.T, *recordingSessionStore, *recordingFileIdentities)
	}{
		{
			name: "update session",
			mutate: func(t *testing.T, service *Service) error {
				t.Helper()
				title := "Updated"
				_, err := service.Update(context.Background(), workspace.DefaultID, "sesn_test", UpdateRequest{Title: &title})
				return err
			},
			assert: func(t *testing.T, store *recordingSessionStore, _ *recordingFileIdentities) {
				t.Helper()
				if store.sessions["sesn_test"].Title == nil || *store.sessions["sesn_test"].Title != "Updated" {
					t.Fatalf("title = %v; want durable update", store.sessions["sesn_test"].Title)
				}
			},
		},
		{
			name: "add resource",
			mutate: func(t *testing.T, service *Service) error {
				t.Helper()
				mount := "/workspace/new.csv"
				_, err := service.AddResource(context.Background(), workspace.DefaultID, "sesn_test", ResourceRequest{
					Type:      string(ResourceTypeFile),
					FileID:    "file_source_new",
					MountPath: &mount,
				})
				return err
			},
			assert: func(t *testing.T, store *recordingSessionStore, fileIdentities *recordingFileIdentities) {
				t.Helper()
				if len(fileIdentities.created) != 1 || fileIdentities.created[0].SessionFileID == "" {
					t.Fatalf("file identities = %+v; want one durable session file identity", fileIdentities.created)
				}
				if got := len(store.sessions["sesn_test"].Resources); got != 3 {
					t.Fatalf("resource count = %d; want added file resource", got)
				}
			},
		},
		{
			name: "delete resource",
			mutate: func(t *testing.T, service *Service) error {
				t.Helper()
				_, err := service.DeleteResource(context.Background(), workspace.DefaultID, "sesn_test", "sesrsc_file")
				return err
			},
			assert: func(t *testing.T, store *recordingSessionStore, fileIdentities *recordingFileIdentities) {
				t.Helper()
				if len(fileIdentities.tombstoned) != 1 || fileIdentities.tombstoned[0] != "sesn_test:file_session" {
					t.Fatalf("file identities tombstoned = %+v; want immediate session file tombstone", fileIdentities.tombstoned)
				}
				if store.sessions["sesn_test"].Resources[0].DeleteRequestedAt != nil {
					t.Fatal("file resource delete requested for unmaterialized resource")
				}
				if store.sessions["sesn_test"].Resources[0].DetachedAt == nil {
					t.Fatal("file resource was not detached immediately")
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newRecordingSessionStore()
			fileIdentities := &recordingFileIdentities{}
			service := newTestService(store, fileIdentities, &recordingVaultValidator{}, fixed)
			store.sessions["sesn_test"] = sessionWithFileAndGitHubResources(fixed)

			if err := tc.mutate(t, service); err != nil {
				t.Fatalf("mutation: %v", err)
			}
			tc.assert(t, store, fileIdentities)
		})
	}
}

func TestAddResourceInvalidMountPathDoesNotEnterMutationBoundary(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	fileIdentities := &recordingFileIdentities{}
	service := newTestService(store, fileIdentities, &recordingVaultValidator{}, fixed)
	store.sessions["sesn_test"] = testStoredSession(fixed)
	invalidMountPath := "relative/path.txt"

	_, err := service.AddResource(context.Background(), workspace.DefaultID, "sesn_test", ResourceRequest{
		Type:      string(ResourceTypeFile),
		FileID:    "file_source_new",
		MountPath: &invalidMountPath,
	})

	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("AddResource err = %T %v; want ValidationError", err, err)
	}
	if store.runtimeLockCalls != 0 {
		t.Fatalf("runtime mutation lock calls = %d; want none for invalid mount_path", store.runtimeLockCalls)
	}
	if store.txCalls != 0 {
		t.Fatalf("transaction calls = %d; want none for invalid mount_path", store.txCalls)
	}
	if len(fileIdentities.created) != 0 {
		t.Fatalf("file identities created for invalid mount_path: %+v", fileIdentities.created)
	}
}

func TestMutableSessionMutationsUseDurableBoundary(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name   string
		mutate func(*testing.T, *Service) error
	}{
		{
			name: "update session",
			mutate: func(t *testing.T, service *Service) error {
				t.Helper()
				title := "Updated"
				_, err := service.Update(context.Background(), workspace.DefaultID, "sesn_test", UpdateRequest{Title: &title})
				return err
			},
		},
		{
			name: "add resource",
			mutate: func(t *testing.T, service *Service) error {
				t.Helper()
				mount := "/workspace/new.csv"
				_, err := service.AddResource(context.Background(), workspace.DefaultID, "sesn_test", ResourceRequest{
					Type:      string(ResourceTypeFile),
					FileID:    "file_source_new",
					MountPath: &mount,
				})
				return err
			},
		},
		{
			name: "delete resource",
			mutate: func(t *testing.T, service *Service) error {
				t.Helper()
				_, err := service.DeleteResource(context.Background(), workspace.DefaultID, "sesn_test", "sesrsc_file")
				return err
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newRecordingSessionStore()
			fileIdentities := &recordingFileIdentities{}
			service := newTestService(store, fileIdentities, &recordingVaultValidator{}, fixed)
			store.sessions["sesn_test"] = sessionWithFileAndGitHubResources(fixed)

			if err := tc.mutate(t, service); err != nil {
				t.Fatalf("mutation: %v", err)
			}
			if store.txCalls == 0 {
				t.Fatal("mutation did not enter durable transaction boundary")
			}
		})
	}
}

func TestUpdateReturnsTitleAndMetadataAfterTransactionValidation(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, fixed)
	stored := testStoredSession(fixed)
	stored.Metadata = map[string]string{"remove": "old", "team": "infra"}
	store.sessions["sesn_test"] = stored
	title := "renamed"
	tier := "prod"

	response, err := service.Update(context.Background(), workspace.DefaultID, "sesn_test", UpdateRequest{
		Title: &title,
		MetadataPatch: map[string]*string{
			"remove": nil,
			"tier":   &tier,
		},
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if response.Title == nil || *response.Title != title {
		t.Fatalf("response title = %v; want %q", response.Title, title)
	}
	if response.Metadata["team"] != "infra" || response.Metadata["tier"] != tier {
		t.Fatalf("response metadata = %#v; want team and tier", response.Metadata)
	}
}

func TestUpdateSkipsDurableMutationForNoEffectiveChange(t *testing.T) {
	createdAt := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	updateTime := createdAt.Add(time.Hour)
	tests := []struct {
		name    string
		request UpdateRequest
	}{
		{
			name:    "empty patch",
			request: UpdateRequest{},
		},
		{
			name:    "empty metadata patch",
			request: UpdateRequest{MetadataPatch: map[string]*string{}},
		},
		{
			name: "same title",
			request: UpdateRequest{
				Title: stringPtr("existing title"),
			},
		},
		{
			name: "same metadata value",
			request: UpdateRequest{
				MetadataPatch: map[string]*string{"team": stringPtr("infra")},
			},
		},
		{
			name: "delete missing metadata key",
			request: UpdateRequest{
				MetadataPatch: map[string]*string{"missing": nil},
			},
		},
		{
			name: "same approval mode",
			request: UpdateRequest{
				ApprovalMode: approvalModePtr(ApprovalModeAskForApproval),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newRecordingSessionStore()
			service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, updateTime)
			stored := testStoredSession(createdAt)
			stored.Title = stringPtr("existing title")
			stored.Metadata = map[string]string{"team": "infra"}
			store.sessions["sesn_test"] = stored

			response, err := service.Update(context.Background(), workspace.DefaultID, "sesn_test", test.request)
			if err != nil {
				t.Fatalf("Update: %v", err)
			}

			if store.updateSessionCalls != 0 {
				t.Fatalf("UpdateSession calls = %d; want 0", store.updateSessionCalls)
			}
			if !response.UpdatedAt.Equal(createdAt) {
				t.Fatalf("response updated_at = %s; want %s", response.UpdatedAt, createdAt)
			}
			if !store.sessions["sesn_test"].UpdatedAt.Equal(createdAt) {
				t.Fatalf("stored updated_at = %s; want %s", store.sessions["sesn_test"].UpdatedAt, createdAt)
			}
			if response.Title == nil || *response.Title != "existing title" {
				t.Fatalf("response title = %v; want existing title", response.Title)
			}
			if response.Metadata["team"] != "infra" {
				t.Fatalf("response metadata = %#v; want team=infra", response.Metadata)
			}
		})
	}
}

func TestUpdateOmittedProviderSelectorLeavesCreateTimeBindingUnchanged(t *testing.T) {
	createdAt := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	updateTime := createdAt.Add(time.Hour)
	store := newRecordingSessionStore()
	service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, updateTime)
	stored := testStoredSession(createdAt)
	stored.VaultIDs = []string{"vlt_test"}
	store.sessions["sesn_test"] = stored
	store.providerAuth["sesn_test"] = SessionProviderAuthAdmission{
		SessionID:    "sesn_test",
		ProviderID:   "anthropic",
		VaultID:      "vlt_test",
		CredentialID: "cred_old",
		AccessMode:   "user_api_key",
		UpdatedAt:    createdAt,
	}

	title := "renamed"
	if _, err := service.Update(context.Background(), workspace.DefaultID, "sesn_test", UpdateRequest{Title: &title}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	selector := store.providerAuth["sesn_test"]
	if selector.CredentialID != "cred_old" || !selector.UpdatedAt.Equal(createdAt) {
		t.Fatalf("provider selector changed on omitted update: %+v", selector)
	}
}

func TestUpdateApprovalModeQueuesRuntimeConfigPatch(t *testing.T) {
	createdAt := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	updateTime := createdAt.Add(time.Hour)
	store := newRecordingSessionStore()
	service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, updateTime)
	stored := testStoredSession(createdAt)
	stored.ConfigGeneration = 4
	store.sessions["sesn_test"] = stored

	response, err := service.Update(context.Background(), workspace.DefaultID, "sesn_test", UpdateRequest{
		ApprovalMode: approvalModePtr(ApprovalModeFullAccess),
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}

	if store.updateSessionCalls != 1 {
		t.Fatalf("UpdateSession calls = %d; want 1", store.updateSessionCalls)
	}
	updated := store.sessions["sesn_test"]
	if updated.ApprovalMode != ApprovalModeFullAccess || updated.ConfigGeneration != 5 {
		t.Fatalf("stored runtime config = mode %q generation %d; want full_access generation 5", updated.ApprovalMode, updated.ConfigGeneration)
	}
	if len(store.runtimeConfigUpdates) != 1 || store.runtimeConfigUpdates[0] != `{"config_generation":5,"session_id":"sesn_test","workspace_id":"default"}` {
		t.Fatalf("runtime config updates = %#v; want refs-only generation 5", store.runtimeConfigUpdates)
	}
	if !response.UpdatedAt.Equal(updateTime) {
		t.Fatalf("response updated_at = %s; want %s", response.UpdatedAt, updateTime)
	}
	if response.Agent.ApprovalMode != agent.ApprovalModeFullAccess {
		t.Fatalf("response agent approval mode = %q; want updated full_access", response.Agent.ApprovalMode)
	}
}

func TestUpdateAgentToolsAndMCPServersQueuesRuntimeConfigPatch(t *testing.T) {
	createdAt := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	updateTime := createdAt.Add(time.Hour)
	store := newRecordingSessionStore()
	service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, updateTime)
	stored := testStoredSession(createdAt)
	stored.ConfigGeneration = 2
	store.sessions["sesn_test"] = stored
	tools := agent.RawArray{
		json.RawMessage(`{"type":"tetral_agent_toolset","family":"claude"}`),
		json.RawMessage(`{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"enabled":false,"permission_policy":{"type":"always_ask"}},"configs":[{"name":"github_search","enabled":true,"permission_policy":{"type":"always_allow"}}]}`),
	}
	mcpServers := agent.RawArray{json.RawMessage(`{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp"}`)}

	response, err := service.Update(context.Background(), workspace.DefaultID, "sesn_test", UpdateRequest{
		ToolsPatch:      &tools,
		MCPServersPatch: &mcpServers,
	})
	if err != nil {
		t.Fatalf("Update tools/mcp_servers: %v", err)
	}

	if store.updateSessionCalls != 1 {
		t.Fatalf("UpdateSession calls = %d; want 1", store.updateSessionCalls)
	}
	updated := store.sessions["sesn_test"]
	if updated.ConfigGeneration != 3 || updated.RuntimeAgentConfig == nil {
		t.Fatalf("stored runtime config = generation %d config %#v; want generation 3 runtime agent config", updated.ConfigGeneration, updated.RuntimeAgentConfig)
	}
	if len(response.Agent.Tools) != 2 || !strings.Contains(string(response.Agent.Tools[1]), `"mcp_toolset"`) {
		t.Fatalf("response agent tools = %s; want updated mcp_toolset", string(mustMarshalJSONForSessionTest(t, response.Agent.Tools)))
	}
	if len(response.Agent.MCPServers) != 1 || !strings.Contains(string(response.Agent.MCPServers[0]), `"https://api.githubcopilot.com/mcp/"`) {
		t.Fatalf("response agent mcp_servers = %s; want canonical catalog URL", string(mustMarshalJSONForSessionTest(t, response.Agent.MCPServers)))
	}
	if len(store.runtimeConfigUpdates) != 1 || store.runtimeConfigUpdates[0] != `{"config_generation":3,"session_id":"sesn_test","workspace_id":"default"}` {
		t.Fatalf("runtime config updates = %#v; want refs-only generation 3", store.runtimeConfigUpdates)
	}
}

func TestUpdateApprovalModeRejectsConflictingRuntimeWork(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name      string
		configure func(*recordingSessionStore)
	}{
		{
			name: "running session",
			configure: func(store *recordingSessionStore) {
				store.sessions["sesn_test"].Status = StatusRunning
			},
		},
		{
			name: "pending runtime input",
			configure: func(store *recordingSessionStore) {
				store.pendingRuntimeInput = true
			},
		},
		{
			name: "pending runtime config",
			configure: func(store *recordingSessionStore) {
				store.pendingRuntimeConfigUpdate = true
			},
		},
		{
			name: "cleanup",
			configure: func(store *recordingSessionStore) {
				store.pendingCleanup = true
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newRecordingSessionStore()
			service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, fixed)
			store.sessions["sesn_test"] = testStoredSession(fixed)
			tc.configure(store)

			_, err := service.Update(context.Background(), workspace.DefaultID, "sesn_test", UpdateRequest{
				ApprovalMode: approvalModePtr(ApprovalModeFullAccess),
			})
			if err == nil {
				t.Fatal("Update succeeded; want conflict")
			}
			var conflict *ConflictError
			if !errors.As(err, &conflict) {
				t.Fatalf("err = %T %v; want ConflictError", err, err)
			}
			if len(store.runtimeConfigUpdates) != 0 {
				t.Fatalf("runtime config updates = %#v; want none", store.runtimeConfigUpdates)
			}
		})
	}
}

func TestUpdateMetadataNullDeletionMutatesDurableState(t *testing.T) {
	createdAt := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	updateTime := createdAt.Add(time.Hour)
	store := newRecordingSessionStore()
	service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, updateTime)
	stored := testStoredSession(createdAt)
	stored.Metadata = map[string]string{"drop": "old", "team": "infra"}
	store.sessions["sesn_test"] = stored

	response, err := service.Update(context.Background(), workspace.DefaultID, "sesn_test", UpdateRequest{
		MetadataPatch: map[string]*string{"drop": nil},
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}

	if store.updateSessionCalls != 1 {
		t.Fatalf("UpdateSession calls = %d; want 1", store.updateSessionCalls)
	}
	if !response.UpdatedAt.Equal(updateTime) {
		t.Fatalf("response updated_at = %s; want %s", response.UpdatedAt, updateTime)
	}
	if _, ok := response.Metadata["drop"]; ok {
		t.Fatalf("response metadata retained deleted key: %#v", response.Metadata)
	}
	if _, ok := store.sessions["sesn_test"].Metadata["drop"]; ok {
		t.Fatalf("stored metadata retained deleted key: %#v", store.sessions["sesn_test"].Metadata)
	}
}

func TestUpdateRejectsSessionArchivedInsideWriteTransaction(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, fixed)
	stored := testStoredSession(fixed)
	store.sessions["sesn_test"] = stored
	archivedAt := fixed.Add(time.Minute)
	store.beforeTx = func() {
		store.sessions["sesn_test"].ArchivedAt = &archivedAt
	}
	title := "should not persist"

	_, err := service.Update(context.Background(), workspace.DefaultID, "sesn_test", UpdateRequest{Title: &title})
	if err == nil {
		t.Fatal("Update must reject a session archived before the write transaction mutates it")
	}
	var conflict *ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("err = %T %v; want ConflictError", err, err)
	}
	if store.sessions["sesn_test"].Title != nil {
		t.Fatalf("title changed after archived update: %q", *store.sessions["sesn_test"].Title)
	}
}

func TestAddResourceRejectsSessionArchivedInsideWriteTransaction(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	fileIdentities := &recordingFileIdentities{}
	service := newTestService(store, fileIdentities, &recordingVaultValidator{}, fixed)
	store.sessions["sesn_test"] = testStoredSession(fixed)
	archivedAt := fixed.Add(time.Minute)
	store.beforeTx = func() {
		store.sessions["sesn_test"].ArchivedAt = &archivedAt
	}

	_, err := service.AddResource(context.Background(), workspace.DefaultID, "sesn_test", ResourceRequest{
		Type:   string(ResourceTypeFile),
		FileID: "file_source",
	})
	if err == nil {
		t.Fatal("AddResource must reject a session archived before the write transaction mutates resources")
	}
	var conflict *ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("err = %T %v; want ConflictError", err, err)
	}
	if len(fileIdentities.created) != 0 {
		t.Fatalf("session file identities created after archive = %+v", fileIdentities.created)
	}
	if got := len(store.sessions["sesn_test"].Resources); got != 0 {
		t.Fatalf("resources created after archive = %d; want 0", got)
	}
}

func TestAddResourceDefaultsOmittedFileMountPath(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	fileIdentities := &recordingFileIdentities{}
	service := newTestService(store, fileIdentities, &recordingVaultValidator{}, fixed)
	store.sessions["sesn_test"] = testStoredSession(fixed)

	response, err := service.AddResource(context.Background(), workspace.DefaultID, "sesn_test", ResourceRequest{
		Type:   string(ResourceTypeFile),
		FileID: "file_source",
	})
	if err != nil {
		t.Fatalf("AddResource: %v", err)
	}
	wantMountPath := "/mnt/session/uploads/file_session_1"
	if response.FileID != "file_session_1" || response.MountPath != wantMountPath {
		t.Fatalf("response = %+v; want default upload mount path", response)
	}
	if got := store.sessions["sesn_test"].Resources[0].File.MountPath; got != wantMountPath {
		t.Fatalf("stored mount_path = %q; want %q", got, wantMountPath)
	}
	if len(fileIdentities.created) != 1 || fileIdentities.created[0].SessionFileID != "file_session_1" {
		t.Fatalf("session file identities = %+v; want file_session_1 identity", fileIdentities.created)
	}
}

func TestAddResourcePreservesExplicitNonWorkspaceMountPath(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	fileIdentities := &recordingFileIdentities{}
	service := newTestService(store, fileIdentities, &recordingVaultValidator{}, fixed)
	store.sessions["sesn_test"] = testStoredSession(fixed)
	mountPath := "/project/input.csv"

	response, err := service.AddResource(context.Background(), workspace.DefaultID, "sesn_test", ResourceRequest{
		Type:      string(ResourceTypeFile),
		FileID:    "file_source",
		MountPath: &mountPath,
	})
	if err != nil {
		t.Fatalf("AddResource: %v", err)
	}
	if response.MountPath != mountPath {
		t.Fatalf("response mount_path = %q; want %q", response.MountPath, mountPath)
	}
	if got := store.sessions["sesn_test"].Resources[0].File.MountPath; got != mountPath {
		t.Fatalf("stored mount_path = %q; want %q", got, mountPath)
	}
}

func TestDeleteResourceRequestsCleanupForMaterializedFile(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	fileIdentities := &recordingFileIdentities{}
	service := newTestService(store, fileIdentities, &recordingVaultValidator{}, fixed)
	stored := testStoredSession(fixed)
	stored.Resources = []*Resource{{
		ID:              "sesrsc_delete",
		SessionID:       "sesn_test",
		WorkspaceID:     workspace.DefaultID,
		StorageSequence: 1,
		Type:            ResourceTypeFile,
		CreatedAt:       fixed,
		UpdatedAt:       fixed,
		File: &FileResource{
			SourceFileID: "file_source_delete",
			FileID:       "file_session_delete",
			MountPath:    "/workspace/delete.csv",
		},
	}}
	store.sessions["sesn_test"] = stored
	store.materializedSessions["sesn_test"] = true

	_, err := service.DeleteResource(context.Background(), workspace.DefaultID, "sesn_test", "sesrsc_delete")
	if err != nil {
		t.Fatalf("DeleteResource: %v", err)
	}
	if store.sessions["sesn_test"].Resources[0].DetachedAt != nil {
		t.Fatalf("resource detached before sandbox cleanup finalized: %+v", store.sessions["sesn_test"].Resources[0].DetachedAt)
	}
	if store.sessions["sesn_test"].Resources[0].DeleteRequestedAt == nil {
		t.Fatal("resource delete was not requested")
	}
	if len(fileIdentities.tombstoned) != 0 {
		t.Fatalf("file identities tombstoned during API delete: %+v", fileIdentities.tombstoned)
	}
}

func TestDeleteResourceRequestsCleanupForMaterializedMemoryAndGitHubResources(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	for _, resource := range []*Resource{
		{
			ID: "sesrsc_memory_delete", SessionID: "sesn_test", WorkspaceID: workspace.DefaultID,
			StorageSequence: 1, Type: ResourceTypeMemoryStore, CreatedAt: fixed, UpdatedAt: fixed,
			MemoryStore: &MemoryStoreResource{MemoryStoreID: "memstore_delete", Access: "read_write", MountPath: "/mnt/memory/delete"},
		},
		{
			ID: "sesrsc_github_delete", SessionID: "sesn_test", WorkspaceID: workspace.DefaultID,
			StorageSequence: 1, Type: ResourceTypeGitHubRepository, CreatedAt: fixed, UpdatedAt: fixed,
			GitHubRepository: &GitHubRepositoryResource{URL: "https://github.com/tetral-ai/tetral", MountPath: "/workspace/tetral"},
		},
	} {
		store := newRecordingSessionStore()
		fileIdentities := &recordingFileIdentities{}
		service := newTestService(store, fileIdentities, &recordingVaultValidator{}, fixed)
		stored := testStoredSession(fixed)
		stored.Resources = []*Resource{resource}
		store.sessions["sesn_test"] = stored
		store.materializedSessions["sesn_test"] = true

		if _, err := service.DeleteResource(context.Background(), workspace.DefaultID, "sesn_test", resource.ID); err != nil {
			t.Fatalf("DeleteResource(%s): %v", resource.Type, err)
		}
		if resource.DetachedAt != nil || resource.DeleteRequestedAt == nil {
			t.Fatalf("resource %s lifecycle = detached:%v delete_requested:%v", resource.Type, resource.DetachedAt, resource.DeleteRequestedAt)
		}
		if len(fileIdentities.tombstoned) != 0 {
			t.Fatalf("resource %s tombstoned file identity: %+v", resource.Type, fileIdentities.tombstoned)
		}
	}
}

func TestDeleteResourceRejectsRepeatedDeleteWhileCleanupPending(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, fixed)
	stored := testStoredSession(fixed)
	stored.Resources = []*Resource{{
		ID:              "sesrsc_delete",
		SessionID:       "sesn_test",
		WorkspaceID:     workspace.DefaultID,
		StorageSequence: 1,
		Type:            ResourceTypeFile,
		CreatedAt:       fixed,
		UpdatedAt:       fixed,
		File: &FileResource{
			SourceFileID: "file_source_delete",
			FileID:       "file_session_delete",
			MountPath:    "/workspace/delete.csv",
		},
	}}
	store.sessions["sesn_test"] = stored
	store.materializedSessions["sesn_test"] = true

	if _, err := service.DeleteResource(context.Background(), workspace.DefaultID, "sesn_test", "sesrsc_delete"); err != nil {
		t.Fatalf("first DeleteResource: %v", err)
	}
	_, err := service.DeleteResource(context.Background(), workspace.DefaultID, "sesn_test", "sesrsc_delete")
	var conflict *ConflictError
	if !errors.As(err, &conflict) || !conflict.InvalidRequest {
		t.Fatalf("second DeleteResource err = %T %v; want invalid_request conflict", err, err)
	}
}

func TestDeleteResourceTombstonesUnmaterializedFileImmediately(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	fileIdentities := &recordingFileIdentities{}
	service := newTestService(store, fileIdentities, &recordingVaultValidator{}, fixed)
	stored := testStoredSession(fixed)
	stored.Resources = []*Resource{{
		ID:              "sesrsc_delete",
		SessionID:       "sesn_test",
		WorkspaceID:     workspace.DefaultID,
		StorageSequence: 1,
		Type:            ResourceTypeFile,
		CreatedAt:       fixed,
		UpdatedAt:       fixed,
		File: &FileResource{
			SourceFileID: "file_source_delete",
			FileID:       "file_session_delete",
			MountPath:    "/workspace/delete.csv",
		},
	}}
	store.sessions["sesn_test"] = stored

	_, err := service.DeleteResource(context.Background(), workspace.DefaultID, "sesn_test", "sesrsc_delete")
	if err != nil {
		t.Fatalf("DeleteResource: %v", err)
	}
	if store.sessions["sesn_test"].Resources[0].DetachedAt == nil {
		t.Fatal("resource was not detached immediately")
	}
	if store.sessions["sesn_test"].Resources[0].DeleteRequestedAt != nil {
		t.Fatalf("resource delete requested for unmaterialized file: %+v", store.sessions["sesn_test"].Resources[0].DeleteRequestedAt)
	}
	if len(fileIdentities.tombstoned) != 1 || fileIdentities.tombstoned[0] != "sesn_test:file_session_delete" {
		t.Fatalf("file identities tombstoned = %+v; want file_session_delete", fileIdentities.tombstoned)
	}
	if store.resourceMutationCalls != 1 {
		t.Fatalf("resourceMutationCalls = %d; want the detached declaration to advance the resource revision", store.resourceMutationCalls)
	}
}

func TestAddResourceRecomputesMountsInsideWriteTransaction(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	fileIdentities := &recordingFileIdentities{}
	service := newTestService(store, fileIdentities, &recordingVaultValidator{}, fixed)
	store.sessions["sesn_test"] = testStoredSession(fixed)
	mountPath := "/workspace/data.csv"
	addedConcurrentResource := false
	store.beforeTx = func() {
		if addedConcurrentResource {
			return
		}
		addedConcurrentResource = true
		store.sessions["sesn_test"].Resources = append(store.sessions["sesn_test"].Resources, &Resource{
			ID:          "sesrsc_existing",
			SessionID:   "sesn_test",
			WorkspaceID: workspace.DefaultID,
			Type:        ResourceTypeFile,
			CreatedAt:   fixed,
			UpdatedAt:   fixed,
			File: &FileResource{
				SourceFileID: "file_source_existing",
				FileID:       "file_session_existing",
				MountPath:    mountPath,
			},
		})
	}

	_, err := service.AddResource(context.Background(), workspace.DefaultID, "sesn_test", ResourceRequest{
		Type:      string(ResourceTypeFile),
		FileID:    "file_source",
		MountPath: &mountPath,
	})
	if err == nil {
		t.Fatal("AddResource must reject a mount path that appears before the write transaction mutates resources")
	}
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("err = %T %v; want ValidationError", err, err)
	}
	if len(fileIdentities.created) != 0 {
		t.Fatalf("session file identities created after mount validation failure = %+v", fileIdentities.created)
	}
	if got := len(store.sessions["sesn_test"].Resources); got != 1 {
		t.Fatalf("resources after rejected add = %d; want only the concurrent resource", got)
	}
}

func TestAddResourceRecomputesFileCountInsideWriteTransaction(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	fileIdentities := &recordingFileIdentities{}
	service := newTestService(store, fileIdentities, &recordingVaultValidator{}, fixed)
	store.sessions["sesn_test"] = testStoredSession(fixed)
	populatedConcurrentResources := false
	store.beforeTx = func() {
		if populatedConcurrentResources {
			return
		}
		populatedConcurrentResources = true
		for index := 0; index < maxFileResources; index++ {
			suffix := strconv.Itoa(index)
			store.sessions["sesn_test"].Resources = append(store.sessions["sesn_test"].Resources, &Resource{
				ID:          "sesrsc_existing_count_" + suffix,
				SessionID:   "sesn_test",
				WorkspaceID: workspace.DefaultID,
				Type:        ResourceTypeFile,
				CreatedAt:   fixed,
				UpdatedAt:   fixed,
				File: &FileResource{
					SourceFileID: "file_source_existing",
					FileID:       "file_session_existing",
					MountPath:    "/workspace/existing-" + suffix,
				},
			})
		}
	}

	_, err := service.AddResource(context.Background(), workspace.DefaultID, "sesn_test", ResourceRequest{
		Type:   string(ResourceTypeFile),
		FileID: "file_source",
	})
	if err == nil {
		t.Fatal("AddResource must reject when the transaction-local file resource count is already at the limit")
	}
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("err = %T %v; want ValidationError", err, err)
	}
	if validation.Message != "too many file resources" {
		t.Fatalf("validation message = %q; want file count rejection", validation.Message)
	}
	if len(fileIdentities.created) != 0 {
		t.Fatalf("session file identities created after file count validation failure = %+v", fileIdentities.created)
	}
	if got := len(store.sessions["sesn_test"].Resources); got != maxFileResources {
		t.Fatalf("resources after rejected add = %d; want existing limit %d", got, maxFileResources)
	}
}

func TestResourceMutationsRejectArchivedParentInsideWriteTransaction(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	archivedAt := fixed.Add(time.Minute)

	t.Run("delete file resource", func(t *testing.T) {
		store := newRecordingSessionStore()
		fileIdentities := &recordingFileIdentities{}
		service := newTestService(store, fileIdentities, &recordingVaultValidator{}, fixed)
		stored := testStoredSession(fixed)
		stored.ArchivedAt = &archivedAt
		stored.Resources = []*Resource{{
			ID:              "sesrsc_file",
			SessionID:       "sesn_test",
			WorkspaceID:     workspace.DefaultID,
			StorageSequence: 1,
			Type:            ResourceTypeFile,
			CreatedAt:       fixed,
			UpdatedAt:       fixed,
			File: &FileResource{
				SourceFileID: "file_source",
				FileID:       "file_session",
				MountPath:    "/workspace/data.csv",
			},
		}}
		store.sessions["sesn_test"] = stored

		_, err := service.DeleteResource(context.Background(), workspace.DefaultID, "sesn_test", "sesrsc_file")
		if err == nil {
			t.Fatal("DeleteResource must reject archived parent session")
		}
		var conflict *ConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("err = %T %v; want ConflictError", err, err)
		}
		if len(fileIdentities.tombstoned) != 0 {
			t.Fatalf("file identities tombstoned for archived session = %+v", fileIdentities.tombstoned)
		}
		if store.sessions["sesn_test"].Resources[0].DetachedAt != nil {
			t.Fatalf("resource detached despite archived parent: %+v", store.sessions["sesn_test"].Resources[0].DetachedAt)
		}
	})
}

func TestDeleteTombstonesFileResourceAddedBeforeDeleteTransaction(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	fileIdentities := &recordingFileIdentities{}
	service := newTestService(store, fileIdentities, &recordingVaultValidator{}, fixed)
	store.sessions["sesn_test"] = testStoredSession(fixed)
	addedConcurrentResource := false
	store.beforeTx = func() {
		if addedConcurrentResource {
			return
		}
		addedConcurrentResource = true
		store.sessions["sesn_test"].Resources = append(store.sessions["sesn_test"].Resources, &Resource{
			ID:          "sesrsc_concurrent",
			SessionID:   "sesn_test",
			WorkspaceID: workspace.DefaultID,
			Type:        ResourceTypeFile,
			CreatedAt:   fixed,
			UpdatedAt:   fixed,
			File: &FileResource{
				SourceFileID: "file_source_concurrent",
				FileID:       "file_session_concurrent",
				MountPath:    "/workspace/concurrent.csv",
			},
		})
	}

	_, err := service.Delete(context.Background(), workspace.DefaultID, "sesn_test")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if len(fileIdentities.tombstoned) != 1 || fileIdentities.tombstoned[0] != "sesn_test:file_session_concurrent" {
		t.Fatalf("tombstoned identities = %+v; want concurrent session file identity", fileIdentities.tombstoned)
	}
}

func TestArchiveSoftArchivesDurableSession(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, fixed)
	store.sessions["sesn_test"] = testStoredSession(fixed)

	response, err := service.Archive(context.Background(), workspace.DefaultID, "sesn_test")
	if err != nil {
		t.Fatalf("Archive: %v", err)
	}
	if response.ArchivedAt == nil {
		t.Fatalf("response archived_at = nil; want archived session")
	}
	stored := store.sessions["sesn_test"]
	if stored.ArchivedAt == nil || stored.LifecycleState != LifecycleStateArchived {
		t.Fatalf("stored session archived_at/lifecycle = %v/%s; want archived tombstone state", stored.ArchivedAt, stored.LifecycleState)
	}
	if store.txCalls != 1 || store.committedTxCount != 1 {
		t.Fatalf("archive transactions = %d committed=%d; want one durable transaction", store.txCalls, store.committedTxCount)
	}
}

func TestArchiveRejectsWithoutDurableMutation(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		prepare func(*recordingSessionStore)
	}{
		{name: "not found"},
		{
			name: "lock conflict",
			prepare: func(store *recordingSessionStore) {
				store.sessions["sesn_test"] = testStoredSession(fixed)
				store.lockErr = &ConflictError{Message: "session has non-terminal runs"}
			},
		},
		{
			name: "running",
			prepare: func(store *recordingSessionStore) {
				store.sessions["sesn_test"] = testStoredSession(fixed)
				store.sessions["sesn_test"].Status = StatusRunning
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newRecordingSessionStore()
			service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, fixed)
			if test.prepare != nil {
				test.prepare(store)
			}

			if _, err := service.Archive(context.Background(), workspace.DefaultID, "sesn_test"); err == nil {
				t.Fatal("Archive succeeded; want durable admission rejection")
			}
			if sess := store.sessions["sesn_test"]; sess != nil && sess.ArchivedAt != nil {
				t.Fatalf("session archived despite rejected archive: %+v", sess.ArchivedAt)
			}
		})
	}
}

func TestArchiveDurableFailureRollsBackSession(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	archiveErr := errors.New("archive write failed")
	store := newRecordingSessionStore()
	store.archiveErr = archiveErr
	service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, fixed)
	store.sessions["sesn_test"] = testStoredSession(fixed)

	response, err := service.Archive(context.Background(), workspace.DefaultID, "sesn_test")
	if !errors.Is(err, archiveErr) {
		t.Fatalf("Archive err = %T %v; want archiveErr", err, err)
	}
	if response != nil {
		t.Fatalf("response = %+v; want nil after archive failure", response)
	}
	if store.sessions["sesn_test"].ArchivedAt != nil || store.sessions["sesn_test"].LifecycleState == LifecycleStateArchived {
		t.Fatalf("session archived despite durable failure: %+v", store.sessions["sesn_test"])
	}
}

func TestDeleteTombstonesFileIdentitiesAndMarksSessionDeleted(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	fileIdentities := &recordingFileIdentities{}
	service := newTestService(store, fileIdentities, &recordingVaultValidator{}, fixed)
	stored := testStoredSession(fixed)
	stored.Resources = []*Resource{{
		ID:          "sesrsc_file",
		SessionID:   "sesn_test",
		WorkspaceID: workspace.DefaultID,
		Type:        ResourceTypeFile,
		File:        &FileResource{SourceFileID: "file_source", FileID: "file_session", MountPath: "/workspace/data.csv"},
	}}
	store.sessions["sesn_test"] = stored

	response, err := service.Delete(context.Background(), workspace.DefaultID, "sesn_test")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if response.ID != "sesn_test" || response.Type != "session_deleted" {
		t.Fatalf("response = %+v; want session_deleted", response)
	}
	if len(fileIdentities.tombstoned) != 1 || fileIdentities.tombstoned[0] != "sesn_test:file_session" {
		t.Fatalf("tombstones = %+v; want session file tombstone", fileIdentities.tombstoned)
	}
	if got := store.sessions["sesn_test"].LifecycleState; got != LifecycleStateDeleted {
		t.Fatalf("session lifecycle = %s; want deleted", got)
	}
	if detachedAt := store.sessions["sesn_test"].Resources[0].DetachedAt; detachedAt == nil {
		t.Fatal("deleted session resource detached_at = nil; want durable resource-row release")
	}
}

func TestDeleteRejectsBeforeFileTombstone(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		prepare func(*recordingSessionStore)
	}{
		{name: "not found"},
		{
			name: "lock conflict",
			prepare: func(store *recordingSessionStore) {
				store.sessions["sesn_test"] = testStoredSession(fixed)
				store.lockErr = &ConflictError{Message: "session has non-terminal runs"}
			},
		},
		{
			name: "running",
			prepare: func(store *recordingSessionStore) {
				store.sessions["sesn_test"] = testStoredSession(fixed)
				store.sessions["sesn_test"].Status = StatusRunning
				store.sessions["sesn_test"].Resources = []*Resource{{
					ID:          "sesrsc_file",
					SessionID:   "sesn_test",
					WorkspaceID: workspace.DefaultID,
					Type:        ResourceTypeFile,
					File:        &FileResource{SourceFileID: "file_source", FileID: "file_session", MountPath: "/workspace/data.csv"},
				}}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newRecordingSessionStore()
			fileIdentities := &recordingFileIdentities{}
			service := newTestService(store, fileIdentities, &recordingVaultValidator{}, fixed)
			if test.prepare != nil {
				test.prepare(store)
			}

			if _, err := service.Delete(context.Background(), workspace.DefaultID, "sesn_test"); err == nil {
				t.Fatal("Delete succeeded; want durable admission rejection")
			}
			if len(fileIdentities.tombstoned) != 0 {
				t.Fatalf("tombstones = %+v; want none before delete admission", fileIdentities.tombstoned)
			}
		})
	}
}

func TestDeleteTombstoneFailureRollsBackSessionLifecycle(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	tombstoneErr := errors.New("tombstone failed")
	store := newRecordingSessionStore()
	fileIdentities := &recordingFileIdentities{tombstoneErr: tombstoneErr}
	service := newTestService(store, fileIdentities, &recordingVaultValidator{}, fixed)
	stored := testStoredSession(fixed)
	stored.Resources = []*Resource{{
		ID:          "sesrsc_file",
		SessionID:   "sesn_test",
		WorkspaceID: workspace.DefaultID,
		Type:        ResourceTypeFile,
		File:        &FileResource{SourceFileID: "file_source", FileID: "file_session", MountPath: "/workspace/data.csv"},
	}}
	store.sessions["sesn_test"] = stored

	response, err := service.Delete(context.Background(), workspace.DefaultID, "sesn_test")
	if !errors.Is(err, tombstoneErr) {
		t.Fatalf("Delete err = %T %v; want tombstoneErr", err, err)
	}
	if response != nil {
		t.Fatalf("response = %+v; want nil after tombstone failure", response)
	}
	if got := store.sessions["sesn_test"].LifecycleState; got == LifecycleStateDeleted {
		t.Fatalf("session lifecycle = %s; want rollback before deleted", got)
	}
	if len(fileIdentities.tombstoned) != 0 {
		t.Fatalf("tombstones = %+v; want none when tombstone fails before durable mutation", fileIdentities.tombstoned)
	}
}

func TestDeleteDurableFailureRollsBackSessionLifecycle(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	deleteErr := errors.New("delete write failed")
	store := newRecordingSessionStore()
	store.deleteErr = deleteErr
	service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, fixed)
	store.sessions["sesn_test"] = testStoredSession(fixed)

	response, err := service.Delete(context.Background(), workspace.DefaultID, "sesn_test")
	if !errors.Is(err, deleteErr) {
		t.Fatalf("Delete err = %T %v; want deleteErr", err, err)
	}
	if response != nil {
		t.Fatalf("response = %+v; want nil after delete failure", response)
	}
	if got := store.sessions["sesn_test"].LifecycleState; got == LifecycleStateDeleted {
		t.Fatalf("session lifecycle = %s; want rollback before deleted", got)
	}
}

func TestListResourcesForwardsSignedNextPageFromStore(t *testing.T) {
	store := newRecordingSessionStore()
	nextPage := "signed-resource-page"
	store.nextResourcePage = &nextPage
	service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, time.Now().UTC())
	store.sessions["sesn_test"] = &Session{
		ID:            "sesn_test",
		Type:          "session",
		WorkspaceID:   workspace.DefaultID,
		Status:        StatusIdle,
		AgentID:       "agent_test",
		AgentVersion:  1,
		EnvironmentID: "env_test",
		Resources: []*Resource{{
			ID:              "sesrsc_1",
			SessionID:       "sesn_test",
			WorkspaceID:     workspace.DefaultID,
			StorageSequence: 1,
			Type:            ResourceTypeFile,
			File:            &FileResource{SourceFileID: "file_source", FileID: "file_session", MountPath: "/workspace/a.txt"},
		}},
	}

	result, err := service.ListResources(context.Background(), workspace.DefaultID, "sesn_test", ResourceListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("ListResources: %v", err)
	}
	if result.NextPage == nil || *result.NextPage != nextPage {
		t.Fatalf("next_page = %v; want %q", result.NextPage, nextPage)
	}
}

func TestPublicThreadAPIsProjectSDKShapeAndArchiveLifecycle(t *testing.T) {
	fixed := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	store := newRecordingSessionStore()
	store.sessions["sesn_test"] = testStoredSession(fixed)
	store.sessions["sesn_test"].ApprovalMode = ApprovalModeApproveForMe
	store.sessions["sesn_test"].Usage.InputTokens = 999
	parent := "thread_main"
	store.threads["thread_main"] = &Thread{
		ID:              "thread_main",
		WorkspaceID:     workspace.DefaultID,
		SessionID:       "sesn_test",
		Role:            ThreadRoleMain,
		Visibility:      ThreadVisibilityPublic,
		Status:          ThreadStatusIdle,
		AgentType:       "default",
		StorageSequence: 1,
		CreatedAt:       fixed,
		LastActiveAt:    fixed,
		UpdatedAt:       fixed,
		Usage:           Usage{InputTokens: 11},
	}
	store.threads["thread_child"] = &Thread{
		ID:              "thread_child",
		WorkspaceID:     workspace.DefaultID,
		SessionID:       "sesn_test",
		ParentThreadID:  &parent,
		Role:            ThreadRoleSubagent,
		Visibility:      ThreadVisibilityPublic,
		Status:          ThreadStatusClosedForRuntime,
		AgentType:       "default",
		StorageSequence: 2,
		CreatedAt:       fixed.Add(time.Minute),
		LastActiveAt:    fixed.Add(time.Minute),
		UpdatedAt:       fixed.Add(time.Minute),
		Usage:           Usage{InputTokens: 22},
	}
	store.threads["thread_reviewer"] = &Thread{
		ID:              "thread_reviewer",
		WorkspaceID:     workspace.DefaultID,
		SessionID:       "sesn_test",
		ParentThreadID:  &parent,
		Role:            ThreadRoleApprovalReviewer,
		Visibility:      ThreadVisibilityInternal,
		Status:          ThreadStatusIdle,
		AgentType:       "default",
		StorageSequence: 3,
		CreatedAt:       fixed,
		LastActiveAt:    fixed,
		UpdatedAt:       fixed,
	}
	service := newTestService(store, &recordingFileIdentities{}, &recordingVaultValidator{}, fixed.Add(2*time.Hour))

	listed, err := service.ListThreads(context.Background(), workspace.DefaultID, "sesn_test", ThreadListOptions{})
	if err != nil {
		t.Fatalf("ListThreads: %v", err)
	}
	if len(listed.Data) != 2 {
		t.Fatalf("listed threads = %d; want public main and subagent only: %+v", len(listed.Data), listed.Data)
	}
	if listed.Data[0].ID != "thread_main" || listed.Data[1].ID != "thread_child" {
		t.Fatalf("thread order = %s, %s; want storage order", listed.Data[0].ID, listed.Data[1].ID)
	}
	if listed.Data[1].Status != StatusIdle {
		t.Fatalf("closed_for_runtime public status = %s; want idle", listed.Data[1].Status)
	}
	if listed.Data[1].ParentThreadID == nil || *listed.Data[1].ParentThreadID != "thread_main" {
		t.Fatalf("parent_thread_id = %v; want thread_main", listed.Data[1].ParentThreadID)
	}
	if listed.Data[1].Type != "session_thread" || listed.Data[1].Agent.Type != "agent" || listed.Data[1].Agent.Model.ID != "anthropic/claude-opus-4-8" || listed.Data[1].Agent.Model.Speed != "" {
		t.Fatalf("thread DTO/agent projection = %+v", listed.Data[1])
	}
	if listed.Data[0].Usage.InputTokens != 11 || listed.Data[1].Usage.InputTokens != 22 {
		t.Fatalf("thread usages = %d, %d; want per-thread 11, 22", listed.Data[0].Usage.InputTokens, listed.Data[1].Usage.InputTokens)
	}
	if listed.Data[0].Agent.ApprovalMode != agent.ApprovalModeApproveForMe || listed.Data[1].Agent.ApprovalMode != agent.ApprovalModeApproveForMe {
		t.Fatalf("thread approval modes = %q, %q; want session approve_for_me", listed.Data[0].Agent.ApprovalMode, listed.Data[1].Agent.ApprovalMode)
	}
	if _, err := service.GetThread(context.Background(), workspace.DefaultID, "sesn_test", "thread_reviewer"); err == nil {
		t.Fatal("GetThread returned internal reviewer thread; want not found")
	}
	store.threads["thread_child"].Status = ThreadStatusRunning
	if _, err := service.ArchiveThread(context.Background(), workspace.DefaultID, "sesn_test", "thread_child"); err == nil {
		t.Fatal("ArchiveThread running public thread succeeded; want conflict")
	}
	store.threads["thread_child"].Status = ThreadStatusIdle
	archived, err := service.ArchiveThread(context.Background(), workspace.DefaultID, "sesn_test", "thread_child")
	if err != nil {
		t.Fatalf("ArchiveThread: %v", err)
	}
	if archived.ArchivedAt == nil || !archived.ArchivedAt.Equal(fixed.Add(2*time.Hour)) {
		t.Fatalf("archived_at = %v; want service clock", archived.ArchivedAt)
	}
	if archived.Usage.InputTokens != 22 {
		t.Fatalf("archived thread usage = %d; want thread-scoped 22", archived.Usage.InputTokens)
	}
	again, err := service.ArchiveThread(context.Background(), workspace.DefaultID, "sesn_test", "thread_child")
	if err != nil {
		t.Fatalf("ArchiveThread replay: %v", err)
	}
	if again.ArchivedAt == nil || !again.ArchivedAt.Equal(*archived.ArchivedAt) {
		t.Fatalf("re-archive changed archived_at: first=%v second=%v", archived.ArchivedAt, again.ArchivedAt)
	}
}

func newTestService(store *recordingSessionStore, fileIdentities *recordingFileIdentities, vaults *recordingVaultValidator, now time.Time) *Service {
	service := NewService(
		testAgents{},
		testEnvironments{},
		fileIdentities,
		testMemories{},
		vaults,
		store,
		testSessionEncryptor{},
		WithClock(func() time.Time { return now }),
	)
	service.sessionIDStrategy = func() string { return "sesn_test" }
	resourceCount := 0
	service.resourceIDStrategy = func() string {
		resourceCount++
		return "sesrsc_" + strconv.Itoa(resourceCount)
	}
	fileCount := 0
	service.fileIDStrategy = func() string {
		fileCount++
		return "file_session_" + strconv.Itoa(fileCount)
	}
	return service
}

type testSessionEncryptor struct{}

func (testSessionEncryptor) Encrypt(value []byte) ([]byte, error) {
	return append([]byte("encrypted:"), value...), nil
}

func testStoredSession(createdAt time.Time) *Session {
	return &Session{
		ID:               "sesn_test",
		Type:             "session",
		WorkspaceID:      workspace.DefaultID,
		Status:           StatusIdle,
		LifecycleState:   LifecycleStateActive,
		ConfigGeneration: 1,
		ApprovalMode:     ApprovalModeAskForApproval,
		AgentID:          "agent_test",
		AgentVersion:     1,
		EnvironmentID:    "env_test",
		CreatedAt:        createdAt,
		UpdatedAt:        createdAt,
		Metadata:         map[string]string{},
		VaultIDs:         []string{},
		Resources:        []*Resource{},
	}
}

func providerCredentialForSessionTest(id string, vaultID string, authType string, providerID string, accessMode string) *ProviderCredentialForAdmission {
	return providerCredentialForSessionTestWithArchive(id, vaultID, authType, providerID, accessMode, false)
}

func providerCredentialForSessionTestWithArchive(id string, vaultID string, authType string, providerID string, accessMode string, archived bool) *ProviderCredentialForAdmission {
	return providerCredentialForSessionTestWithLifecycle(id, vaultID, authType, providerID, accessMode, archived, false)
}

func providerCredentialForSessionTestWithLifecycle(id string, vaultID string, authType string, providerID string, accessMode string, archived bool, revoked bool) *ProviderCredentialForAdmission {
	return &ProviderCredentialForAdmission{
		ID:         id,
		VaultID:    vaultID,
		AuthType:   authType,
		ProviderID: providerID,
		AccessMode: accessMode,
		Archived:   archived,
		Revoked:    revoked,
	}
}

func sessionWithFileAndGitHubResources(createdAt time.Time) *Session {
	stored := testStoredSession(createdAt)
	stored.Resources = []*Resource{
		{
			ID:              "sesrsc_file",
			SessionID:       "sesn_test",
			WorkspaceID:     workspace.DefaultID,
			StorageSequence: 1,
			Type:            ResourceTypeFile,
			CreatedAt:       createdAt,
			UpdatedAt:       createdAt,
			File: &FileResource{
				SourceFileID: "file_source",
				FileID:       "file_session",
				MountPath:    "/workspace/data.csv",
			},
		},
		{
			ID:              "sesrsc_repo",
			SessionID:       "sesn_test",
			WorkspaceID:     workspace.DefaultID,
			StorageSequence: 2,
			Type:            ResourceTypeGitHubRepository,
			CreatedAt:       createdAt,
			UpdatedAt:       createdAt,
			GitHubRepository: &GitHubRepositoryResource{
				URL:       "https://github.com/tetral-ai/tetral",
				MountPath: "/workspace/tetral",
			},
		},
	}
	return stored
}

func intPtr(value int) *int { return &value }

func stringPtr(value string) *string { return &value }

func approvalModePtr(value ApprovalMode) *ApprovalMode { return &value }

func mustMarshalJSONForSessionTest(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal JSON: %v", err)
	}
	return encoded
}

func metadataWithPairs(count int) map[string]string {
	metadata := make(map[string]string, count)
	for index := 0; index < count; index++ {
		key := "key_" + strconv.Itoa(index)
		metadata[key] = "value"
	}
	return metadata
}

type testAgents struct{}

func (testAgents) Get(context.Context, workspace.ID, string) (*agent.Agent, error) {
	return testAgent(1), nil
}

func (testAgents) GetVersion(_ context.Context, _ workspace.ID, _ string, version int) (*agent.Agent, error) {
	return testAgent(version), nil
}

type staticAgentReader struct {
	agent *agent.Agent
}

func (r staticAgentReader) Get(context.Context, workspace.ID, string) (*agent.Agent, error) {
	return r.agent, nil
}

func (r staticAgentReader) GetVersion(context.Context, workspace.ID, string, int) (*agent.Agent, error) {
	return r.agent, nil
}

type failingAgentReader struct {
	err error
}

func (r failingAgentReader) Get(context.Context, workspace.ID, string) (*agent.Agent, error) {
	return nil, r.err
}

func (r failingAgentReader) GetVersion(context.Context, workspace.ID, string, int) (*agent.Agent, error) {
	return nil, r.err
}

type recordingAgentReader struct {
	getCalls          int
	getWorkspaces     []workspace.ID
	versionCalls      []int
	versionWorkspaces []workspace.ID
}

func (r *recordingAgentReader) Get(_ context.Context, ws workspace.ID, _ string) (*agent.Agent, error) {
	r.getCalls++
	r.getWorkspaces = append(r.getWorkspaces, ws)
	return testAgent(1), nil
}

func (r *recordingAgentReader) GetVersion(_ context.Context, ws workspace.ID, _ string, version int) (*agent.Agent, error) {
	r.versionCalls = append(r.versionCalls, version)
	r.versionWorkspaces = append(r.versionWorkspaces, ws)
	return testAgent(version), nil
}

func (r *recordingAgentReader) versionCallStrings() []string {
	return intStrings(r.versionCalls)
}

func intStrings(values []int) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, strconv.Itoa(value))
	}
	return out
}

func testAgent(version int) *agent.Agent {
	return &agent.Agent{
		ID:      "agent_test",
		Type:    "agent",
		Version: version,
		AgentConfig: agent.AgentConfig{
			Name:         "test agent",
			Model:        "anthropic/claude-opus-4-8",
			ApprovalMode: agent.ApprovalModeAskForApproval,
			Tools:        agent.RawArray{json.RawMessage(`{"type":"tetral_agent_toolset","family":"claude"}`)},
		},
		CreatedAt: time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC),
	}
}

type testEnvironments struct{}

func (testEnvironments) Get(context.Context, workspace.ID, string) (*environment.Environment, error) {
	return &environment.Environment{ID: "env_test", Type: "environment"}, nil
}

type failingEnvironmentReader struct {
	env *environment.Environment
	err error
}

func (r failingEnvironmentReader) Get(context.Context, workspace.ID, string) (*environment.Environment, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.env, nil
}

type recordingEnvironmentReader struct {
	workspaces []workspace.ID
	env        *environment.Environment
}

func (r *recordingEnvironmentReader) Get(_ context.Context, ws workspace.ID, _ string) (*environment.Environment, error) {
	r.workspaces = append(r.workspaces, ws)
	if r.env != nil {
		return r.env, nil
	}
	return &environment.Environment{ID: "env_test", Type: "environment"}, nil
}

type testMemories struct{}

func (testMemories) GetStore(context.Context, workspace.ID, string) (*memory.Store, error) {
	return &memory.Store{ID: "memstore_test", Type: "memory_store", Name: "Project Memory", Description: "shared"}, nil
}

type memoryReaderByID struct{}

func (memoryReaderByID) GetStore(_ context.Context, _ workspace.ID, memoryStoreID string) (*memory.Store, error) {
	return &memory.Store{ID: memoryStoreID, Type: "memory_store", Name: memoryStoreID, Description: "shared"}, nil
}

type recordingMemoryReader struct {
	getStoreCalls int
	workspaces    []workspace.ID
	err           error
}

func (r *recordingMemoryReader) GetStore(_ context.Context, ws workspace.ID, _ string) (*memory.Store, error) {
	r.getStoreCalls++
	r.workspaces = append(r.workspaces, ws)
	if r.err != nil {
		return nil, r.err
	}
	return &memory.Store{ID: "memstore_test", Type: "memory_store", Name: "Project Memory", Description: "shared"}, nil
}

type recordingVaultValidator struct {
	validated  []string
	workspaces []workspace.ID
	err        error
}

func (v *recordingVaultValidator) ValidateVaultReferences(_ context.Context, ws workspace.ID, vaultIDs []string) error {
	v.workspaces = append(v.workspaces, ws)
	v.validated = append(v.validated, vaultIDs...)
	return v.err
}

type recordingFileIdentities struct {
	validateCalls       int
	validated           []string
	validateWorkspaces  []workspace.ID
	validateErr         error
	created             []files.SessionFileIdentityRequest
	createWorkspaces    []workspace.ID
	tombstoned          []string
	tombstoneWorkspaces []workspace.ID
	tombstoneErr        error
}

func (r *recordingFileIdentities) ValidateSessionFileSource(_ context.Context, ws workspace.ID, fileID string) error {
	r.validateCalls++
	r.validateWorkspaces = append(r.validateWorkspaces, ws)
	r.validated = append(r.validated, fileID)
	return r.validateErr
}

func (r *recordingFileIdentities) CreateSessionFileIdentity(_ context.Context, _ files.SessionTransaction, ws workspace.ID, request files.SessionFileIdentityRequest) (*files.FileMetadata, error) {
	r.createWorkspaces = append(r.createWorkspaces, ws)
	r.created = append(r.created, request)
	return &files.FileMetadata{ID: request.SessionFileID, Type: "file"}, nil
}

func (r *recordingFileIdentities) TombstoneSessionFileIdentity(_ context.Context, _ files.SessionTransaction, ws workspace.ID, sessionID string, fileID string) error {
	if r.tombstoneErr != nil {
		return r.tombstoneErr
	}
	r.tombstoneWorkspaces = append(r.tombstoneWorkspaces, ws)
	r.tombstoned = append(r.tombstoned, sessionID+":"+fileID)
	return nil
}

type recordingSessionStore struct {
	sessions                   map[string]*Session
	threads                    map[string]*Thread
	materializedSessions       map[string]bool
	providerAuth               map[string]SessionProviderAuthAdmission
	providerCredentials        map[string]*ProviderCredentialForAdmission
	runtimeConfigUpdates       []string
	pendingRuntimeInput        bool
	pendingRuntimeConfigUpdate bool
	pendingCleanup             bool
	runtimeMu                  sync.Mutex
	runtimeLocks               map[string]*sync.Mutex
	runtimeLockCalls           int
	nextResourcePage           *string
	beforeTx                   func()
	inTx                       bool
	txCalls                    int
	committedTxCount           int
	commitErr                  error
	lockErr                    error
	archiveErr                 error
	deleteErr                  error
	updateSessionCalls         int
	resourceMutationCalls      int
}

func newRecordingSessionStore() *recordingSessionStore {
	return &recordingSessionStore{
		sessions:             map[string]*Session{},
		threads:              map[string]*Thread{},
		materializedSessions: map[string]bool{},
		providerAuth:         map[string]SessionProviderAuthAdmission{},
		providerCredentials:  map[string]*ProviderCredentialForAdmission{},
	}
}

func (s *recordingSessionStore) WithRuntimeMutationLock(_ context.Context, ws workspace.ID, sessionID string, fn func() error) error {
	lockKey := string(ws) + "\x00" + sessionID
	s.runtimeMu.Lock()
	if s.runtimeLocks == nil {
		s.runtimeLocks = map[string]*sync.Mutex{}
	}
	lock := s.runtimeLocks[lockKey]
	if lock == nil {
		lock = &sync.Mutex{}
		s.runtimeLocks[lockKey] = lock
	}
	s.runtimeMu.Unlock()

	lock.Lock()
	defer lock.Unlock()
	s.runtimeMu.Lock()
	s.runtimeLockCalls++
	s.runtimeMu.Unlock()
	return fn()
}

func (s *recordingSessionStore) WithWorkspaceTx(_ context.Context, ws workspace.ID, fn func(Transaction) error) error {
	return s.WithWorkspaceTxAndCleanup(context.Background(), ws, fn, nil)
}

func (s *recordingSessionStore) WithWorkspaceTxAndCleanup(_ context.Context, ws workspace.ID, fn func(Transaction) error, onCommitFailure func()) error {
	s.txCalls++
	if s.beforeTx != nil {
		s.beforeTx()
	}
	sessionSnapshot := map[string]*Session{}
	for id, sess := range s.sessions {
		sessionSnapshot[id] = cloneSession(sess)
	}
	threadSnapshot := map[string]*Thread{}
	for id, thread := range s.threads {
		threadSnapshot[id] = cloneThread(thread)
	}
	materializedSnapshot := map[string]bool{}
	for id, materialized := range s.materializedSessions {
		materializedSnapshot[id] = materialized
	}
	runtimeConfigUpdatesSnapshot := append([]string(nil), s.runtimeConfigUpdates...)
	providerAuthSnapshot := map[string]SessionProviderAuthAdmission{}
	for id, selector := range s.providerAuth {
		providerAuthSnapshot[id] = selector
	}
	providerCredentialSnapshot := map[string]*ProviderCredentialForAdmission{}
	for id, credential := range s.providerCredentials {
		providerCredentialSnapshot[id] = cloneProviderCredentialForAdmission(credential)
	}
	s.inTx = true
	if err := fn(&recordingSessionTx{store: s, workspaceID: ws}); err != nil {
		s.inTx = false
		s.sessions = sessionSnapshot
		s.threads = threadSnapshot
		s.materializedSessions = materializedSnapshot
		s.runtimeConfigUpdates = runtimeConfigUpdatesSnapshot
		s.providerAuth = providerAuthSnapshot
		s.providerCredentials = providerCredentialSnapshot
		return err
	}
	s.inTx = false
	if s.commitErr != nil {
		if onCommitFailure != nil {
			onCommitFailure()
		}
		s.sessions = sessionSnapshot
		s.threads = threadSnapshot
		s.materializedSessions = materializedSnapshot
		s.runtimeConfigUpdates = runtimeConfigUpdatesSnapshot
		s.providerAuth = providerAuthSnapshot
		s.providerCredentials = providerCredentialSnapshot
		return s.commitErr
	}
	s.committedTxCount++
	return nil
}

func (s *recordingSessionStore) Get(_ context.Context, _ workspace.ID, sessionID string) (*Session, error) {
	sess, ok := s.sessions[sessionID]
	if !ok {
		return nil, &NotFoundError{Message: "session not found"}
	}
	clone := cloneSession(sess)
	clone.Resources = activeResourceClones(sess.Resources)
	return clone, nil
}

func (s *recordingSessionStore) List(context.Context, workspace.ID, ListOptions) (*StoreListResult, error) {
	data := make([]*Session, 0, len(s.sessions))
	for _, sess := range s.sessions {
		data = append(data, cloneSession(sess))
	}
	return &StoreListResult{Data: data}, nil
}

func (s *recordingSessionStore) ListSessionProviderAuth(_ context.Context, _ workspace.ID, sessionID string) (ProviderSelectors, error) {
	selectors := ProviderSelectors{}
	if selector, ok := s.providerAuth[sessionID]; ok {
		selectors[selector.ProviderID] = ProviderCredentialSelector{CredentialID: selector.CredentialID}
	}
	return selectors, nil
}

func (s *recordingSessionStore) ListThreads(_ context.Context, _ workspace.ID, sessionID string, options ThreadListOptions) (*StoreThreadListResult, error) {
	if _, ok := s.sessions[sessionID]; !ok {
		return nil, &NotFoundError{Message: "session not found"}
	}
	data := make([]*Thread, 0, len(s.threads))
	for _, thread := range s.threads {
		if thread.SessionID != sessionID || thread.Visibility != ThreadVisibilityPublic || thread.Role == ThreadRoleApprovalReviewer {
			continue
		}
		data = append(data, cloneThread(thread))
	}
	sort.Slice(data, func(i, j int) bool {
		if data[i].StorageSequence == data[j].StorageSequence {
			return data[i].ID < data[j].ID
		}
		return data[i].StorageSequence < data[j].StorageSequence
	})
	limit := options.Limit
	if limit <= 0 {
		limit = 20
	}
	if len(data) > limit {
		data = data[:limit]
	}
	return &StoreThreadListResult{Data: data}, nil
}

func (s *recordingSessionStore) GetThread(_ context.Context, _ workspace.ID, sessionID string, threadID string) (*Thread, error) {
	if _, ok := s.sessions[sessionID]; !ok {
		return nil, &NotFoundError{Message: "session not found"}
	}
	thread, ok := s.threads[threadID]
	if !ok || thread.SessionID != sessionID || thread.Visibility != ThreadVisibilityPublic || thread.Role == ThreadRoleApprovalReviewer {
		return nil, &NotFoundError{Message: "session thread not found"}
	}
	return cloneThread(thread), nil
}

func (s *recordingSessionStore) ArchiveThread(_ context.Context, _ workspace.ID, sessionID string, threadID string, archivedAt time.Time) (*Thread, error) {
	if s.archiveErr != nil {
		return nil, s.archiveErr
	}
	if _, ok := s.sessions[sessionID]; !ok {
		return nil, &NotFoundError{Message: "session not found"}
	}
	thread, ok := s.threads[threadID]
	if !ok || thread.SessionID != sessionID || thread.Visibility != ThreadVisibilityPublic || thread.Role == ThreadRoleApprovalReviewer {
		return nil, &NotFoundError{Message: "session thread not found"}
	}
	if thread.ArchivedAt != nil {
		return cloneThread(thread), nil
	}
	if thread.Status == ThreadStatusRunning || thread.Status == ThreadStatusRescheduling {
		return nil, &ConflictError{Message: "running or rescheduling session threads cannot be archived", InvalidRequest: true}
	}
	thread.ArchivedAt = &archivedAt
	thread.UpdatedAt = archivedAt
	return cloneThread(thread), nil
}

func (s *recordingSessionStore) ListResources(_ context.Context, _ workspace.ID, sessionID string, _ ResourceListOptions) (*StoreResourceListResult, error) {
	sess, ok := s.sessions[sessionID]
	if !ok {
		return nil, &NotFoundError{Message: "session not found"}
	}
	return &StoreResourceListResult{Data: activeResourceClones(sess.Resources), NextPage: s.nextResourcePage}, nil
}

type recordingSessionTx struct {
	store       *recordingSessionStore
	workspaceID workspace.ID
}

func (tx *recordingSessionTx) Exec(context.Context, string, ...any) (ExecResult, error) {
	return fakeExecResult(1), nil
}

func (tx *recordingSessionTx) QueryRows(context.Context, string, ...any) (QueryRows, error) {
	return emptyRows{}, nil
}

func (tx *recordingSessionTx) QueryRowScanner(context.Context, string, ...any) RowScanner {
	return errorRow{err: &NotFoundError{Message: "not found"}}
}

func (tx *recordingSessionTx) CreateSession(_ context.Context, sess *Session) error {
	if sess.LifecycleState == "" {
		sess.LifecycleState = LifecycleStateActive
	}
	if sess.ConfigGeneration <= 0 {
		sess.ConfigGeneration = 1
	}
	approvalMode, err := normalizeApprovalMode(sess.ApprovalMode)
	if err != nil {
		return err
	}
	sess.ApprovalMode = approvalMode
	tx.store.sessions[sess.ID] = cloneSession(sess)
	return nil
}

func (tx *recordingSessionTx) CreatePrimaryThread(_ context.Context, thread *Thread) error {
	if thread.Role == "" {
		thread.Role = ThreadRoleMain
	}
	if thread.Visibility == "" {
		thread.Visibility = ThreadVisibilityPublic
	}
	if thread.Status == "" {
		thread.Status = ThreadStatusIdle
	}
	if thread.AgentType == "" {
		thread.AgentType = "default"
	}
	if thread.LastActiveAt.IsZero() {
		thread.LastActiveAt = thread.CreatedAt
	}
	tx.store.threads[thread.ID] = cloneThread(thread)
	return nil
}

func (tx *recordingSessionTx) GetSession(_ context.Context, sessionID string) (*Session, error) {
	return tx.store.Get(context.Background(), tx.workspaceID, sessionID)
}

func (tx *recordingSessionTx) LockSession(_ context.Context, sessionID string) (*Session, error) {
	if tx.store.lockErr != nil {
		return nil, tx.store.lockErr
	}
	return tx.store.Get(context.Background(), tx.workspaceID, sessionID)
}

func (tx *recordingSessionTx) LockSessionForDelete(_ context.Context, sessionID string) (*Session, error) {
	if tx.store.lockErr != nil {
		return nil, tx.store.lockErr
	}
	sess, ok := tx.store.sessions[sessionID]
	if !ok {
		return nil, &NotFoundError{Message: "session not found"}
	}
	if sess.LifecycleState == "" {
		sess.LifecycleState = LifecycleStateActive
	}
	if sess.LifecycleState == LifecycleStateArchiving {
		return nil, &ConflictError{Message: "session lifecycle transition is already in progress", InvalidRequest: true}
	}
	if sess.Status == StatusRunning {
		return nil, &ConflictError{Message: "running sessions cannot be deleted", InvalidRequest: true}
	}
	return cloneSession(sess), nil
}

func (tx *recordingSessionTx) ListSessions(context.Context, ListOptions) ([]*Session, bool, error) {
	result, err := tx.store.List(context.Background(), tx.workspaceID, ListOptions{})
	return result.Data, false, err
}

func (tx *recordingSessionTx) RequireSessionUsableForMutation(_ context.Context, sessionID string) error {
	sess, ok := tx.store.sessions[sessionID]
	if !ok {
		return &NotFoundError{Message: "session not found"}
	}
	state := sess.LifecycleState
	if state == "" {
		state = LifecycleStateActive
	}
	if err := rejectUnusableSession(sessionUsability{
		archivedAt:     nullTimeFromTime(sess.ArchivedAt),
		status:         sess.Status,
		lifecycleState: state,
	}); err != nil {
		return err
	}
	switch {
	case tx.store.pendingRuntimeInput:
		return &ConflictError{Message: "session has pending runtime input", InvalidRequest: true}
	case tx.store.pendingRuntimeConfigUpdate:
		return &ConflictError{Message: "session has pending runtime config update", InvalidRequest: true}
	case tx.store.pendingCleanup:
		return &ConflictError{Message: "session cleanup is in progress", InvalidRequest: true}
	default:
		return nil
	}
}

func (tx *recordingSessionTx) RecordSessionResourceMutation(_ context.Context, sessionID string, _ time.Time) error {
	tx.store.resourceMutationCalls++
	sess, ok := tx.store.sessions[sessionID]
	if !ok {
		return &NotFoundError{Message: "session not found"}
	}
	if sess.Status == StatusRunning {
		return &ConflictError{Message: "session must be idle for resource mutation", InvalidRequest: true}
	}
	return nil
}

func nullTimeFromTime(value *time.Time) sql.NullTime {
	if value == nil {
		return sql.NullTime{}
	}
	return sql.NullTime{Time: value.UTC(), Valid: true}
}

func (tx *recordingSessionTx) UpdateSession(_ context.Context, sessionID string, update UpdateSession) (*Session, error) {
	tx.store.updateSessionCalls++
	sess, ok := tx.store.sessions[sessionID]
	if !ok {
		return nil, &NotFoundError{Message: "session not found"}
	}
	state := sess.LifecycleState
	if state == "" {
		state = LifecycleStateActive
	}
	if err := rejectUnusableSession(sessionUsability{
		archivedAt:     nullTimeFromTime(sess.ArchivedAt),
		status:         sess.Status,
		lifecycleState: state,
	}); err != nil {
		return nil, err
	}
	switch {
	case tx.store.pendingRuntimeInput:
		return nil, &ConflictError{Message: "session has pending runtime input", InvalidRequest: true}
	case tx.store.pendingRuntimeConfigUpdate:
		return nil, &ConflictError{Message: "session has pending runtime config update", InvalidRequest: true}
	case tx.store.pendingCleanup:
		return nil, &ConflictError{Message: "session cleanup is in progress", InvalidRequest: true}
	}
	runtimeConfigChanged := false
	nextApprovalMode := sess.ApprovalMode
	nextRuntimeAgentConfig := cloneRuntimeAgentConfigPointer(sess.RuntimeAgentConfig)
	if update.ApprovalMode != nil && sess.ApprovalMode != *update.ApprovalMode {
		if err := validateSessionApprovalMode(*update.ApprovalMode); err != nil {
			return nil, err
		}
		nextApprovalMode = *update.ApprovalMode
		runtimeConfigChanged = true
	}
	if update.RuntimeAgentConfig != nil {
		candidate := normalizeRuntimeAgentConfig(*update.RuntimeAgentConfig)
		if nextRuntimeAgentConfig == nil || !runtimeAgentConfigEqual(*nextRuntimeAgentConfig, candidate) {
			nextRuntimeAgentConfig = &candidate
			runtimeConfigChanged = true
		}
	}
	if runtimeConfigChanged {
		if sess.ConfigGeneration <= 0 {
			sess.ConfigGeneration = 1
		}
		sess.ConfigGeneration++
		sess.ApprovalMode = nextApprovalMode
		sess.RuntimeAgentConfig = cloneRuntimeAgentConfigPointer(nextRuntimeAgentConfig)
		payload := map[string]any{
			"workspace_id":      "default",
			"session_id":        sessionID,
			"config_generation": sess.ConfigGeneration,
		}
		encoded, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		tx.store.runtimeConfigUpdates = append(tx.store.runtimeConfigUpdates, string(encoded))
	}
	if update.TitlePresent || update.Title != nil {
		sess.Title = update.Title
	}
	if update.MetadataPresent && update.MetadataPatch == nil {
		sess.Metadata = map[string]string{}
	}
	for key, value := range update.MetadataPatch {
		if sess.Metadata == nil {
			sess.Metadata = map[string]string{}
		}
		if value == nil {
			delete(sess.Metadata, key)
			continue
		}
		sess.Metadata[key] = *value
	}
	sess.UpdatedAt = update.UpdatedAt
	return cloneSession(sess), nil
}

func (tx *recordingSessionTx) GetProviderCredentialForAdmission(_ context.Context, credentialID string, boundVaultIDs []string) (*ProviderCredentialForAdmission, error) {
	credential, ok := tx.store.providerCredentials[credentialID]
	if !ok {
		return nil, &NotFoundError{Message: "provider credential not found"}
	}
	if !stringSliceContains(boundVaultIDs, credential.VaultID) {
		return nil, &PermissionError{Message: "provider credential is inaccessible"}
	}
	return cloneProviderCredentialForAdmission(credential), nil
}

func (tx *recordingSessionTx) UpsertSessionProviderAuth(_ context.Context, selector SessionProviderAuthAdmission) error {
	tx.store.providerAuth[selector.SessionID] = selector
	return nil
}

func (tx *recordingSessionTx) ArchiveSession(_ context.Context, sessionID string, archivedAt time.Time) (*Session, error) {
	if tx.store.archiveErr != nil {
		return nil, tx.store.archiveErr
	}
	if tx.store.lockErr != nil {
		return nil, tx.store.lockErr
	}
	sess, ok := tx.store.sessions[sessionID]
	if !ok {
		return nil, &NotFoundError{Message: "session not found"}
	}
	if sess.LifecycleState == LifecycleStateDeleted {
		return nil, &ConflictError{Message: "session lifecycle transition is already in progress", InvalidRequest: true}
	}
	if sess.Status == StatusRunning {
		return nil, &ConflictError{Message: "running sessions cannot be archived", InvalidRequest: true}
	}
	sess.ArchivedAt = &archivedAt
	sess.LifecycleState = LifecycleStateArchived
	sess.UpdatedAt = archivedAt
	return cloneSession(sess), nil
}

func (tx *recordingSessionTx) DeleteSession(_ context.Context, sessionID string) error {
	if tx.store.deleteErr != nil {
		return tx.store.deleteErr
	}
	sess, ok := tx.store.sessions[sessionID]
	if !ok {
		return &NotFoundError{Message: "session not found"}
	}
	if sess.LifecycleState == LifecycleStateArchiving {
		return &ConflictError{Message: "session lifecycle transition is already in progress", InvalidRequest: true}
	}
	if sess.Status == StatusRunning {
		return &ConflictError{Message: "running sessions cannot be deleted", InvalidRequest: true}
	}
	now := time.Now().UTC()
	for _, resource := range sess.Resources {
		if resource.DetachedAt == nil {
			detachedAt := now
			resource.DetachedAt = &detachedAt
			resource.UpdatedAt = now
		}
	}
	sess.LifecycleState = LifecycleStateDeleted
	sess.UpdatedAt = now
	return nil
}

func (tx *recordingSessionTx) CreateResource(_ context.Context, resource *Resource) error {
	sess, ok := tx.store.sessions[resource.SessionID]
	if !ok {
		return &NotFoundError{Message: "session not found"}
	}
	stored := cloneResource(resource)
	stored.StorageSequence = int64(len(sess.Resources) + 1)
	sess.Resources = append(sess.Resources, stored)
	return nil
}

func (tx *recordingSessionTx) ListResources(_ context.Context, sessionID string, _ ResourceListOptions) ([]*Resource, bool, error) {
	sess, ok := tx.store.sessions[sessionID]
	if !ok {
		return nil, false, &NotFoundError{Message: "session not found"}
	}
	return activeResourceClones(sess.Resources), tx.store.nextResourcePage != nil, nil
}

func (tx *recordingSessionTx) GetResource(_ context.Context, sessionID string, resourceID string) (*Resource, error) {
	sess, ok := tx.store.sessions[sessionID]
	if !ok {
		return nil, &NotFoundError{Message: "session not found"}
	}
	for _, resource := range sess.Resources {
		if resource.ID == resourceID && resource.DetachedAt == nil && resource.DeleteRequestedAt == nil {
			return cloneResource(resource), nil
		}
	}
	return nil, &NotFoundError{Message: "session resource not found"}
}

func (tx *recordingSessionTx) UpdateGitHubRepositoryToken(_ context.Context, sessionID string, resourceID string, encryptedToken []byte, updatedAt time.Time) (*Resource, error) {
	sess, ok := tx.store.sessions[sessionID]
	if !ok {
		return nil, &NotFoundError{Message: "session not found"}
	}
	for _, resource := range sess.Resources {
		if resource.ID == resourceID && resource.DetachedAt == nil && resource.DeleteRequestedAt == nil &&
			resource.Type == ResourceTypeGitHubRepository && resource.GitHubRepository != nil {
			resource.GitHubRepository.AuthorizationTokenEncrypted = append([]byte(nil), encryptedToken...)
			resource.UpdatedAt = updatedAt
			return cloneResource(resource), nil
		}
	}
	return nil, &NotFoundError{Message: "session resource not found"}
}

func (tx *recordingSessionTx) RequestResourceDelete(_ context.Context, sessionID string, resourceID string, requestedAt time.Time) (*Resource, error) {
	sess, ok := tx.store.sessions[sessionID]
	if !ok {
		return nil, &NotFoundError{Message: "session not found"}
	}
	for _, resource := range sess.Resources {
		if resource.ID == resourceID && resource.DetachedAt == nil {
			if resource.DeleteRequestedAt != nil {
				return nil, &ConflictError{Message: "session resource deletion is already in progress", InvalidRequest: true}
			}
			if tx.store.materializedSessions[sessionID] {
				resource.DeleteRequestedAt = &requestedAt
			} else {
				resource.DetachedAt = &requestedAt
				resource.DeleteRequestedAt = nil
			}
			resource.UpdatedAt = requestedAt
			return cloneResource(resource), nil
		}
	}
	return nil, &NotFoundError{Message: "session resource not found"}
}

func (tx *recordingSessionTx) DetachResource(_ context.Context, sessionID string, resourceID string, detachedAt time.Time) (*Resource, error) {
	sess, ok := tx.store.sessions[sessionID]
	if !ok {
		return nil, &NotFoundError{Message: "session not found"}
	}
	for _, resource := range sess.Resources {
		if resource.ID == resourceID {
			resource.DeleteRequestedAt = nil
			resource.DetachedAt = &detachedAt
			resource.UpdatedAt = detachedAt
			return cloneResource(resource), nil
		}
	}
	return nil, &NotFoundError{Message: "session resource not found"}
}

func (tx *recordingSessionTx) ReattachResource(_ context.Context, sessionID string, resourceID string, updatedAt time.Time) (*Resource, error) {
	sess, ok := tx.store.sessions[sessionID]
	if !ok {
		return nil, &NotFoundError{Message: "session not found"}
	}
	for _, resource := range sess.Resources {
		if resource.ID == resourceID {
			resource.DetachedAt = nil
			resource.UpdatedAt = updatedAt
			return cloneResource(resource), nil
		}
	}
	return nil, &NotFoundError{Message: "session resource not found"}
}

type fakeExecResult int64

func (r fakeExecResult) RowsAffected() (int64, error) { return int64(r), nil }

type emptyRows struct{}

func (emptyRows) Next() bool         { return false }
func (emptyRows) Scan(...any) error  { return nil }
func (emptyRows) Err() error         { return nil }
func (emptyRows) Close() error       { return nil }
func (r errorRow) Scan(...any) error { return r.err }

type errorRow struct{ err error }

func cloneSession(sess *Session) *Session {
	if sess == nil {
		return nil
	}
	clone := *sess
	if sess.Metadata != nil {
		clone.Metadata = map[string]string{}
		for key, value := range sess.Metadata {
			clone.Metadata[key] = value
		}
	}
	clone.VaultIDs = append([]string(nil), sess.VaultIDs...)
	clone.Resources = cloneResources(sess.Resources)
	clone.RuntimeAgentConfig = cloneRuntimeAgentConfigPointer(sess.RuntimeAgentConfig)
	return &clone
}

func cloneResources(resources []*Resource) []*Resource {
	out := make([]*Resource, 0, len(resources))
	for _, resource := range resources {
		out = append(out, cloneResource(resource))
	}
	return out
}

func activeResourceClones(resources []*Resource) []*Resource {
	out := make([]*Resource, 0, len(resources))
	for _, resource := range resources {
		if resource == nil || resource.DetachedAt != nil || resource.DeleteRequestedAt != nil {
			continue
		}
		out = append(out, cloneResource(resource))
	}
	return out
}

func cloneResource(resource *Resource) *Resource {
	if resource == nil {
		return nil
	}
	clone := *resource
	if resource.File != nil {
		file := *resource.File
		clone.File = &file
	}
	if resource.MemoryStore != nil {
		store := *resource.MemoryStore
		clone.MemoryStore = &store
	}
	if resource.GitHubRepository != nil {
		repo := *resource.GitHubRepository
		clone.GitHubRepository = &repo
	}
	return &clone
}

func cloneThread(thread *Thread) *Thread {
	if thread == nil {
		return nil
	}
	clone := *thread
	if thread.ParentThreadID != nil {
		parent := *thread.ParentThreadID
		clone.ParentThreadID = &parent
	}
	if thread.Title != nil {
		title := *thread.Title
		clone.Title = &title
	}
	if thread.TaskName != nil {
		taskName := *thread.TaskName
		clone.TaskName = &taskName
	}
	if thread.ClosedAt != nil {
		closedAt := *thread.ClosedAt
		clone.ClosedAt = &closedAt
	}
	if thread.ArchivedAt != nil {
		archivedAt := *thread.ArchivedAt
		clone.ArchivedAt = &archivedAt
	}
	return &clone
}

func cloneProviderCredentialForAdmission(credential *ProviderCredentialForAdmission) *ProviderCredentialForAdmission {
	if credential == nil {
		return nil
	}
	clone := *credential
	return &clone
}
