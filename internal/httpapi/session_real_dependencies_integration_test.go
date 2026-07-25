package httpapi_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/agent"
	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/environment"
	"github.com/tetral-ai/tetral/internal/files"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/memory"
	"github.com/tetral-ai/tetral/internal/session"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/vault"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type sessionRealDependenciesIntegrationEnv struct {
	router http.Handler
	admin  *sql.DB
	vaults *vault.Service

	agentID               string
	archivedAgentID       string
	otherWorkspaceAgentID string

	environmentID               string
	archivedEnvironmentID       string
	otherWorkspaceEnvironmentID string

	memoryStoreID               string
	archivedMemoryStoreID       string
	otherWorkspaceMemoryStoreID string

	vaultID               string
	archivedVaultID       string
	otherWorkspaceVaultID string

	fileID               string
	deletedFileID        string
	otherWorkspaceFileID string
}

func newSessionRealDependenciesIntegrationEnv(t *testing.T) *sessionRealDependenciesIntegrationEnv {
	t.Helper()
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Skip(storagetest.EnvTestDatabaseURL + " is not set")
	}

	runtimeDB, adminDB := storagetest.NewPostgreSQLDBWithAdmin(t)
	runtimeClient := dbconnect.NewClientForTesting(runtimeDB)
	seedHTTPWorkspace(t, adminDB, workspace.DefaultID)
	otherWorkspaceID := workspace.ID("workspace_session_real_dependencies_other")
	seedHTTPWorkspace(t, adminDB, otherWorkspaceID)

	agentService := agent.NewService(agent.NewPostgreSQLAgentStore(runtimeClient), nil)
	environmentService := environment.NewService(environment.NewPostgreSQLEnvironmentStore(
		runtimeClient,
		environment.WithDefaultArtifactRef("artifact_default_test"),
	))
	memoryService := memory.NewService(memory.NewPostgreSQLStore(runtimeClient))
	vaultService := vault.NewService(
		vault.NewPostgreSQLVaultStore(runtimeClient),
		vault.NewPostgreSQLCredentialStore(runtimeClient, sessionRealDependenciesVaultEncryptor{}),
	)
	fileService := files.NewService(files.NewPostgreSQLStore(runtimeClient, blob.NewFakeBlobStore()))
	sessionStore := session.NewPostgreSQLSessionStore(
		runtimeClient,
		session.WithPageTokenSecret([]byte("real-dependencies-session-secret")),
	)
	sessionService := session.NewService(
		agentService,
		environmentService,
		fileService,
		memoryService,
		vaultService,
		sessionStore,
		sessionRealDependenciesVaultEncryptor{},
		session.WithClock(func() time.Time { return time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC) }),
	)

	env := &sessionRealDependenciesIntegrationEnv{admin: adminDB, vaults: vaultService}
	env.agentID = createSessionRealDependenciesAgent(t, agentService, workspace.DefaultID, "Real Session Agent")
	env.archivedAgentID = createSessionRealDependenciesAgent(t, agentService, workspace.DefaultID, "Archived Real Session Agent")
	archiveSessionRealDependenciesAgent(t, agentService, workspace.DefaultID, env.archivedAgentID)
	env.otherWorkspaceAgentID = createSessionRealDependenciesAgent(t, agentService, otherWorkspaceID, "Other Workspace Agent")

	env.environmentID = createSessionRealDependenciesEnvironment(t, environmentService, workspace.DefaultID, "Real Session Environment")
	env.archivedEnvironmentID = createSessionRealDependenciesEnvironment(t, environmentService, workspace.DefaultID, "Archived Real Session Environment")
	archiveSessionRealDependenciesEnvironment(t, environmentService, workspace.DefaultID, env.archivedEnvironmentID)
	env.otherWorkspaceEnvironmentID = createSessionRealDependenciesEnvironment(t, environmentService, otherWorkspaceID, "Other Workspace Environment")

	env.memoryStoreID = createSessionRealDependenciesMemoryStore(t, memoryService, workspace.DefaultID, "Real Session Memory")
	env.archivedMemoryStoreID = createSessionRealDependenciesMemoryStore(t, memoryService, workspace.DefaultID, "Archived Real Session Memory")
	archiveSessionRealDependenciesMemoryStore(t, memoryService, workspace.DefaultID, env.archivedMemoryStoreID)
	env.otherWorkspaceMemoryStoreID = createSessionRealDependenciesMemoryStore(t, memoryService, otherWorkspaceID, "Other Workspace Memory")

	env.vaultID = createSessionRealDependenciesVault(t, vaultService, workspace.DefaultID, "Real Session Vault")
	env.archivedVaultID = createSessionRealDependenciesVault(t, vaultService, workspace.DefaultID, "Archived Real Session Vault")
	archiveSessionRealDependenciesVault(t, vaultService, workspace.DefaultID, env.archivedVaultID)
	env.otherWorkspaceVaultID = createSessionRealDependenciesVault(t, vaultService, otherWorkspaceID, "Other Workspace Vault")

	env.fileID = createSessionRealDependenciesFile(t, fileService, workspace.DefaultID, "real-session-file.txt", "file contents")
	env.deletedFileID = createSessionRealDependenciesFile(t, fileService, workspace.DefaultID, "deleted-session-file.txt", "deleted contents")
	deleteSessionRealDependenciesFile(t, fileService, workspace.DefaultID, env.deletedFileID)
	env.otherWorkspaceFileID = createSessionRealDependenciesFile(t, fileService, otherWorkspaceID, "other-session-file.txt", "other contents")

	authenticator := auth.AuthenticatorFunc(func(context.Context, string) (auth.Principal, error) {
		return auth.Principal{Workspace: workspace.Workspace{ID: workspace.DefaultID}, APIKeyID: "ak_session_real_dependencies"}, nil //nolint:gosec // G101: synthetic test API key id
	})
	env.router = httpapi.NewRouter(httpapi.NewSessionHandler(sessionService), "", httpapi.WithAuthenticator(authenticator))
	return env
}

func TestSessionRealDependenciesIntegrationCreatesThroughPublicRoute(t *testing.T) {
	env := newSessionRealDependenciesIntegrationEnv(t)

	recorder := env.request(http.MethodPost, "/v1/sessions?beta=true", env.createBody(
		env.agentID,
		env.environmentID,
		env.vaultID,
		env.memoryStoreID,
		env.fileID,
	))

	assertHTTPStatus(t, recorder, http.StatusOK)
	var created sessionIntegrationSessionResponse
	decodeSessionIntegrationJSON(t, recorder, &created)
	if created.Agent.ID != env.agentID || created.EnvironmentID != env.environmentID {
		t.Fatalf("created session agent/environment = %s/%s; want %s/%s", created.Agent.ID, created.EnvironmentID, env.agentID, env.environmentID)
	}
	if len(created.VaultIDs) != 1 || created.VaultIDs[0] != env.vaultID {
		t.Fatalf("created session vault_ids = %#v; want %s", created.VaultIDs, env.vaultID)
	}
	if findSessionIntegrationResource(t, created.Resources, string(session.ResourceTypeFile)).FileID == env.fileID {
		t.Fatalf("created file resource reused source file id %s instead of session-scoped identity", env.fileID)
	}
	memoryResource := findSessionIntegrationResource(t, created.Resources, string(session.ResourceTypeMemoryStore))
	if memoryResource.MemoryStoreID != env.memoryStoreID {
		t.Fatalf("created memory resource = %#v; want memory store %s", memoryResource, env.memoryStoreID)
	}
}

func TestSessionHTTPCompatibilityPinsToolFamilyAndRejectsPreLawAgent(t *testing.T) {
	env := newSessionRealDependenciesIntegrationEnv(t)
	createdRecorder := env.request(http.MethodPost, "/v1/sessions?beta=true", `{
		"agent":{"type":"agent","id":"`+env.agentID+`","version":1},
		"environment_id":"`+env.environmentID+`",
		"vault_ids":[]
	}`)
	assertHTTPStatus(t, createdRecorder, http.StatusOK)
	var created sessionIntegrationSessionResponse
	decodeSessionIntegrationJSON(t, createdRecorder, &created)

	for _, tc := range []struct {
		name  string
		tools string
		want  string
	}{
		{name: "missing", tools: `[]`, want: "tools must contain exactly one tetral_agent_toolset entry"},
		{name: "changed", tools: `[{"type":"tetral_agent_toolset","family":"gpt"}]`, want: "tools[0].family must match the session's pinned family"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder := env.request(http.MethodPost, "/v1/sessions/"+created.ID+"?beta=true", `{"agent":{"tools":`+tc.tools+`}}`)
			assertHTTPStatus(t, recorder, http.StatusBadRequest)
			if !strings.Contains(recorder.Body.String(), tc.want) {
				t.Fatalf("body = %s; want %q", recorder.Body.String(), tc.want)
			}
		})
	}

	if _, err := env.admin.ExecContext(context.Background(),
		`UPDATE agent_versions SET config_json = '{"name":"pre-law","model":"anthropic/claude-opus-4-8","tools":[]}' WHERE agent_id = $1 AND version = 1`,
		env.agentID); err != nil {
		t.Fatalf("seed pre-law agent version: %v", err)
	}
	preLaw := env.request(http.MethodPost, "/v1/sessions?beta=true", `{
		"agent":{"type":"agent","id":"`+env.agentID+`","version":1},
		"environment_id":"`+env.environmentID+`",
		"vault_ids":[]
	}`)
	assertHTTPStatus(t, preLaw, http.StatusBadRequest)
	if !strings.Contains(preLaw.Body.String(), "agent must declare exactly one tetral_agent_toolset entry; update the agent") {
		t.Fatalf("pre-law body = %s; want update-agent message", preLaw.Body.String())
	}
}

func TestSessionRealDependenciesProviderSelectorAdmissionAndCreateTimeBinding(t *testing.T) {
	env := newSessionRealDependenciesIntegrationEnv(t)
	credentialID := createSessionRealDependenciesProviderCredential(t, env.vaults, workspace.DefaultID, env.vaultID, "provider_api_key", "anthropic", "user_api_key")

	createBody := `{
		"agent":{"type":"agent","id":"` + env.agentID + `","version":1},
		"environment_id":"` + env.environmentID + `",
		"vault_ids":["` + env.vaultID + `"],
		"providers":{"anthropic":{"credential_id":"` + credentialID + `"}}
	}`
	createRecorder := env.request(http.MethodPost, "/v1/sessions?beta=true", createBody)
	assertHTTPStatus(t, createRecorder, http.StatusOK)
	var created sessionIntegrationSessionResponse
	decodeSessionIntegrationJSON(t, createRecorder, &created)
	if created.Providers["anthropic"].CredentialID != credentialID {
		t.Fatalf("created providers = %#v; want anthropic credential", created.Providers)
	}
	assertActiveSessionProviderAuth(t, env.admin, created.ID, "anthropic", env.vaultID, credentialID, "user_api_key")

	titleRecorder := env.request(http.MethodPost, "/v1/sessions/"+created.ID+"?beta=true", `{"title":"selector unchanged"}`)
	assertHTTPStatus(t, titleRecorder, http.StatusOK)
	var titleUpdated sessionIntegrationSessionResponse
	decodeSessionIntegrationJSON(t, titleRecorder, &titleUpdated)
	if titleUpdated.Providers["anthropic"].CredentialID != credentialID {
		t.Fatalf("title update providers = %#v; want unchanged anthropic credential", titleUpdated.Providers)
	}
	assertActiveSessionProviderAuth(t, env.admin, created.ID, "anthropic", env.vaultID, credentialID, "user_api_key")
}

func TestSessionRealDependenciesProviderSelectorRejectsInvalidCredentialsFailClosed(t *testing.T) {
	tests := []struct {
		name           string
		credentialSeed func(t *testing.T, env *sessionRealDependenciesIntegrationEnv) string
		body           func(env *sessionRealDependenciesIntegrationEnv, credentialID string) string
		wantStatus     int
		wantError      string
	}{
		{
			name: "no bound vaults",
			credentialSeed: func(t *testing.T, env *sessionRealDependenciesIntegrationEnv) string {
				return createSessionRealDependenciesProviderCredential(t, env.vaults, workspace.DefaultID, env.vaultID, "provider_api_key", "anthropic", "user_api_key")
			},
			body: func(env *sessionRealDependenciesIntegrationEnv, credentialID string) string {
				return `{"agent":{"type":"agent","id":"` + env.agentID + `","version":1},"environment_id":"` + env.environmentID + `","vault_ids":[],"providers":{"anthropic":{"credential_id":"` + credentialID + `"}}}`
			},
			wantStatus: http.StatusBadRequest,
			wantError:  "invalid_request_error",
		},
		{
			name: "missing credential",
			credentialSeed: func(t *testing.T, env *sessionRealDependenciesIntegrationEnv) string {
				return "cred_missing_real_dependency"
			},
			body:       standardProviderSelectorCreateBody,
			wantStatus: http.StatusNotFound,
			wantError:  "not_found_error",
		},
		{
			name: "cross-workspace credential",
			credentialSeed: func(t *testing.T, env *sessionRealDependenciesIntegrationEnv) string {
				return createSessionRealDependenciesProviderCredential(t, env.vaults, workspace.ID("workspace_session_real_dependencies_other"), env.otherWorkspaceVaultID, "provider_api_key", "anthropic", "user_api_key")
			},
			body:       standardProviderSelectorCreateBody,
			wantStatus: http.StatusNotFound,
			wantError:  "not_found_error",
		},
		{
			name: "mcp credential",
			credentialSeed: func(t *testing.T, env *sessionRealDependenciesIntegrationEnv) string {
				return createSessionRealDependenciesMCPCredential(t, env.vaults, workspace.DefaultID, env.vaultID)
			},
			body:       standardProviderSelectorCreateBody,
			wantStatus: http.StatusForbidden,
			wantError:  "permission_error",
		},
		{
			name: "archived credential",
			credentialSeed: func(t *testing.T, env *sessionRealDependenciesIntegrationEnv) string {
				credentialID := createSessionRealDependenciesProviderCredential(t, env.vaults, workspace.DefaultID, env.vaultID, "provider_api_key", "anthropic", "user_api_key")
				archiveSessionRealDependenciesCredential(t, env.vaults, workspace.DefaultID, env.vaultID, credentialID)
				return credentialID
			},
			body:       standardProviderSelectorCreateBody,
			wantStatus: http.StatusForbidden,
			wantError:  "permission_error",
		},
		{
			name: "revoked credential",
			credentialSeed: func(t *testing.T, env *sessionRealDependenciesIntegrationEnv) string {
				credentialID := createSessionRealDependenciesProviderCredential(t, env.vaults, workspace.DefaultID, env.vaultID, "provider_api_key", "anthropic", "user_api_key")
				revokeSessionRealDependenciesCredential(t, env.admin, workspace.DefaultID, credentialID)
				return credentialID
			},
			body:       standardProviderSelectorCreateBody,
			wantStatus: http.StatusForbidden,
			wantError:  "permission_error",
		},
		{
			name: "wrong provider",
			credentialSeed: func(t *testing.T, env *sessionRealDependenciesIntegrationEnv) string {
				return createSessionRealDependenciesProviderCredential(t, env.vaults, workspace.DefaultID, env.vaultID, "provider_api_key", "openai", "user_api_key")
			},
			body:       standardProviderSelectorCreateBody,
			wantStatus: http.StatusForbidden,
			wantError:  "permission_error",
		},
		{
			name: "unbound vault",
			credentialSeed: func(t *testing.T, env *sessionRealDependenciesIntegrationEnv) string {
				unboundVaultID := createSessionRealDependenciesVault(t, env.vaults, workspace.DefaultID, "Unbound Provider Vault")
				return createSessionRealDependenciesProviderCredential(t, env.vaults, workspace.DefaultID, unboundVaultID, "provider_api_key", "anthropic", "user_api_key")
			},
			body:       standardProviderSelectorCreateBody,
			wantStatus: http.StatusForbidden,
			wantError:  "permission_error",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := newSessionRealDependenciesIntegrationEnv(t)
			credentialID := tc.credentialSeed(t, env)

			recorder := env.request(http.MethodPost, "/v1/sessions?beta=true", tc.body(env, credentialID))

			if recorder.Code != tc.wantStatus {
				t.Fatalf("status = %d; want %d body=%s", recorder.Code, tc.wantStatus, recorder.Body.String())
			}
			assertErrorType(t, recorder, tc.wantError)
			assertSessionIntegrationOwnedRowCounts(t, env.admin, workspace.DefaultID, sessionIntegrationRowCounts{})
			assertNoSessionProviderAuthRows(t, env.admin)
		})
	}
}

func TestSessionRealDependenciesIntegrationRejectedCreateLeavesNoSessionOwnedRows(t *testing.T) {
	tests := []struct {
		name       string
		body       func(*sessionRealDependenciesIntegrationEnv) string
		wantStatus int
		wantError  string
	}{
		{
			name: "missing agent",
			body: func(env *sessionRealDependenciesIntegrationEnv) string {
				return env.createBody("agent_missing_real_dependency", env.environmentID, env.vaultID, env.memoryStoreID, env.fileID)
			},
			wantStatus: http.StatusNotFound,
			wantError:  "not_found_error",
		},
		{
			name: "archived agent",
			body: func(env *sessionRealDependenciesIntegrationEnv) string {
				return env.createBody(env.archivedAgentID, env.environmentID, env.vaultID, env.memoryStoreID, env.fileID)
			},
			wantStatus: http.StatusBadRequest,
			wantError:  "invalid_request_error",
		},
		{
			name: "cross-workspace agent",
			body: func(env *sessionRealDependenciesIntegrationEnv) string {
				return env.createBody(env.otherWorkspaceAgentID, env.environmentID, env.vaultID, env.memoryStoreID, env.fileID)
			},
			wantStatus: http.StatusNotFound,
			wantError:  "not_found_error",
		},
		{
			name: "missing environment",
			body: func(env *sessionRealDependenciesIntegrationEnv) string {
				return env.createBody(env.agentID, "env_missing_real_dependency", env.vaultID, env.memoryStoreID, env.fileID)
			},
			wantStatus: http.StatusNotFound,
			wantError:  "not_found_error",
		},
		{
			name: "archived environment",
			body: func(env *sessionRealDependenciesIntegrationEnv) string {
				return env.createBody(env.agentID, env.archivedEnvironmentID, env.vaultID, env.memoryStoreID, env.fileID)
			},
			wantStatus: http.StatusBadRequest,
			wantError:  "invalid_request_error",
		},
		{
			name: "cross-workspace environment",
			body: func(env *sessionRealDependenciesIntegrationEnv) string {
				return env.createBody(env.agentID, env.otherWorkspaceEnvironmentID, env.vaultID, env.memoryStoreID, env.fileID)
			},
			wantStatus: http.StatusNotFound,
			wantError:  "not_found_error",
		},
		{
			name: "missing memory store",
			body: func(env *sessionRealDependenciesIntegrationEnv) string {
				return env.createBody(env.agentID, env.environmentID, env.vaultID, "memstore_missing_real_dependency", env.fileID)
			},
			wantStatus: http.StatusNotFound,
			wantError:  "not_found_error",
		},
		{
			name: "archived memory store",
			body: func(env *sessionRealDependenciesIntegrationEnv) string {
				return env.createBody(env.agentID, env.environmentID, env.vaultID, env.archivedMemoryStoreID, env.fileID)
			},
			wantStatus: http.StatusBadRequest,
			wantError:  "invalid_request_error",
		},
		{
			name: "cross-workspace memory store",
			body: func(env *sessionRealDependenciesIntegrationEnv) string {
				return env.createBody(env.agentID, env.environmentID, env.vaultID, env.otherWorkspaceMemoryStoreID, env.fileID)
			},
			wantStatus: http.StatusNotFound,
			wantError:  "not_found_error",
		},
		{
			name: "missing vault",
			body: func(env *sessionRealDependenciesIntegrationEnv) string {
				return env.createBody(env.agentID, env.environmentID, "vlt_missing_real_dependency", env.memoryStoreID, env.fileID)
			},
			wantStatus: http.StatusNotFound,
			wantError:  "not_found_error",
		},
		{
			name: "archived vault",
			body: func(env *sessionRealDependenciesIntegrationEnv) string {
				return env.createBody(env.agentID, env.environmentID, env.archivedVaultID, env.memoryStoreID, env.fileID)
			},
			wantStatus: http.StatusBadRequest,
			wantError:  "invalid_request_error",
		},
		{
			name: "cross-workspace vault",
			body: func(env *sessionRealDependenciesIntegrationEnv) string {
				return env.createBody(env.agentID, env.environmentID, env.otherWorkspaceVaultID, env.memoryStoreID, env.fileID)
			},
			wantStatus: http.StatusNotFound,
			wantError:  "not_found_error",
		},
		{
			name: "missing file",
			body: func(env *sessionRealDependenciesIntegrationEnv) string {
				return env.createBody(env.agentID, env.environmentID, env.vaultID, env.memoryStoreID, "file_missing_real_dependency")
			},
			wantStatus: http.StatusNotFound,
			wantError:  "not_found_error",
		},
		{
			name: "deleted file",
			body: func(env *sessionRealDependenciesIntegrationEnv) string {
				return env.createBody(env.agentID, env.environmentID, env.vaultID, env.memoryStoreID, env.deletedFileID)
			},
			wantStatus: http.StatusNotFound,
			wantError:  "not_found_error",
		},
		{
			name: "cross-workspace file",
			body: func(env *sessionRealDependenciesIntegrationEnv) string {
				return env.createBody(env.agentID, env.environmentID, env.vaultID, env.memoryStoreID, env.otherWorkspaceFileID)
			},
			wantStatus: http.StatusNotFound,
			wantError:  "not_found_error",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			env := newSessionRealDependenciesIntegrationEnv(t)

			recorder := env.request(http.MethodPost, "/v1/sessions?beta=true", test.body(env))

			if recorder.Code != test.wantStatus {
				t.Fatalf("status = %d; want %d body=%s", recorder.Code, test.wantStatus, recorder.Body.String())
			}
			assertErrorType(t, recorder, test.wantError)
			assertSessionIntegrationOwnedRowCounts(t, env.admin, workspace.DefaultID, sessionIntegrationRowCounts{})
		})
	}
}

func (e *sessionRealDependenciesIntegrationEnv) request(method string, path string, body string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	setAuthHeader(request)
	e.router.ServeHTTP(recorder, request)
	return recorder
}

func (e *sessionRealDependenciesIntegrationEnv) createBody(agentID string, environmentID string, vaultID string, memoryStoreID string, fileID string) string {
	return `{
		"agent":{"type":"agent","id":"` + agentID + `","version":1},
		"environment_id":"` + environmentID + `",
		"vault_ids":["` + vaultID + `"],
		"resources":[
			{"type":"memory_store","memory_store_id":"` + memoryStoreID + `","access":"read_only"},
			{"type":"file","file_id":"` + fileID + `","mount_path":"/workspace/input.txt"}
		]
	}`
}

func createSessionRealDependenciesAgent(t *testing.T, service *agent.Service, workspaceID workspace.ID, name string) string {
	t.Helper()
	created, err := service.Create(context.Background(), workspaceID, agent.CreateAgentRequest{
		AgentConfig: agent.AgentConfig{
			Name:  name,
			Model: "anthropic/claude-opus-4-8",
			Tools: agent.RawArray{json.RawMessage(`{"type":"tetral_agent_toolset","family":"claude"}`)},
		},
	})
	if err != nil {
		t.Fatalf("create agent %s: %v", name, err)
	}
	return created.ID
}

func archiveSessionRealDependenciesAgent(t *testing.T, service *agent.Service, workspaceID workspace.ID, agentID string) {
	t.Helper()
	if _, err := service.Archive(context.Background(), workspaceID, agentID); err != nil {
		t.Fatalf("archive agent %s: %v", agentID, err)
	}
}

func createSessionRealDependenciesEnvironment(t *testing.T, service *environment.Service, workspaceID workspace.ID, name string) string {
	t.Helper()
	created, err := service.Create(context.Background(), workspaceID, environment.CreateEnvironmentRequest{Name: name})
	if err != nil {
		t.Fatalf("create environment %s: %v", name, err)
	}
	return created.ID
}

func archiveSessionRealDependenciesEnvironment(t *testing.T, service *environment.Service, workspaceID workspace.ID, environmentID string) {
	t.Helper()
	if _, err := service.Archive(context.Background(), workspaceID, environmentID); err != nil {
		t.Fatalf("archive environment %s: %v", environmentID, err)
	}
}

func createSessionRealDependenciesMemoryStore(t *testing.T, service *memory.Service, workspaceID workspace.ID, name string) string {
	t.Helper()
	created, err := service.CreateStore(context.Background(), workspaceID, memory.CreateStoreRequest{Name: name, Description: "session dependency proof"})
	if err != nil {
		t.Fatalf("create memory store %s: %v", name, err)
	}
	return created.ID
}

func archiveSessionRealDependenciesMemoryStore(t *testing.T, service *memory.Service, workspaceID workspace.ID, memoryStoreID string) {
	t.Helper()
	if _, err := service.ArchiveStore(context.Background(), workspaceID, memoryStoreID); err != nil {
		t.Fatalf("archive memory store %s: %v", memoryStoreID, err)
	}
}

func createSessionRealDependenciesVault(t *testing.T, service *vault.Service, workspaceID workspace.ID, displayName string) string {
	t.Helper()
	created, err := service.CreateVault(context.Background(), workspaceID, vault.CreateVaultRequest{DisplayName: displayName})
	if err != nil {
		t.Fatalf("create vault %s: %v", displayName, err)
	}
	return created.ID
}

func archiveSessionRealDependenciesVault(t *testing.T, service *vault.Service, workspaceID workspace.ID, vaultID string) {
	t.Helper()
	if _, err := service.ArchiveVault(context.Background(), workspaceID, vaultID); err != nil {
		t.Fatalf("archive vault %s: %v", vaultID, err)
	}
}

func createSessionRealDependenciesFile(t *testing.T, service *files.Service, workspaceID workspace.ID, filename string, body string) string {
	t.Helper()
	staged, err := files.StageUpload(strings.NewReader(body), t.TempDir(), filename, "text/plain", files.UploadLimits{MaxFileBytes: 1024})
	if err != nil {
		t.Fatalf("stage file %s: %v", filename, err)
	}
	t.Cleanup(func() { _ = staged.Cleanup() })
	created, err := service.CreateUploadedFile(context.Background(), workspaceID, staged)
	if err != nil {
		t.Fatalf("create file %s: %v", filename, err)
	}
	return created.ID
}

func createSessionRealDependenciesProviderCredential(t *testing.T, service *vault.Service, workspaceID workspace.ID, vaultID string, authType string, providerID string, accessMode string) string {
	t.Helper()
	auth := vault.CredentialAuth{
		Type:       authType,
		ProviderID: providerID,
		AccessMode: accessMode,
	}
	switch authType {
	case "provider_api_key":
		auth.Token = "provider-key-" + providerID
	case "provider_oauth":
		auth.AccessToken = "provider-access-" + providerID
		auth.RefreshToken = "provider-refresh-" + providerID
		auth.ExpiresAt = "2026-05-03T00:00:00Z"
		auth.AccountID = "acct_" + providerID
	default:
		t.Fatalf("unsupported provider credential auth type %s", authType)
	}
	created, err := service.CreateCredential(context.Background(), workspaceID, vaultID, vault.CreateCredentialRequest{
		DisplayName: "Provider " + providerID,
		Auth:        auth,
	})
	if err != nil {
		t.Fatalf("create provider credential %s: %v", providerID, err)
	}
	return created.ID
}

func createSessionRealDependenciesMCPCredential(t *testing.T, service *vault.Service, workspaceID workspace.ID, vaultID string) string {
	t.Helper()
	created, err := service.CreateCredential(context.Background(), workspaceID, vaultID, vault.CreateCredentialRequest{
		DisplayName: "MCP Static Bearer",
		Auth: vault.CredentialAuth{
			Type:         "static_bearer",
			MCPServerURL: "https://mcp.example.com/service",
			Token:        "mcp-token",
		},
	})
	if err != nil {
		t.Fatalf("create mcp credential: %v", err)
	}
	return created.ID
}

func archiveSessionRealDependenciesCredential(t *testing.T, service *vault.Service, workspaceID workspace.ID, vaultID string, credentialID string) {
	t.Helper()
	if _, err := service.ArchiveCredential(context.Background(), workspaceID, vaultID, credentialID); err != nil {
		t.Fatalf("archive credential %s: %v", credentialID, err)
	}
}

func revokeSessionRealDependenciesCredential(t *testing.T, db *sql.DB, workspaceID workspace.ID, credentialID string) {
	t.Helper()
	revokedAt := time.Date(2026, 5, 11, 12, 1, 0, 0, time.UTC).Format(time.RFC3339Nano)
	result, err := db.ExecContext(context.Background(),
		`UPDATE credentials
		    SET revoked_at = $1,
		        updated_at = $1
		  WHERE workspace_id = $2 AND id = $3`,
		revokedAt,
		string(workspaceID),
		credentialID,
	)
	if err != nil {
		t.Fatalf("revoke credential %s: %v", credentialID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("revoke credential rows %s: %v", credentialID, err)
	}
	if rows != 1 {
		t.Fatalf("revoke credential rows %s = %d; want 1", credentialID, rows)
	}
}

func standardProviderSelectorCreateBody(env *sessionRealDependenciesIntegrationEnv, credentialID string) string {
	return `{"agent":{"type":"agent","id":"` + env.agentID + `","version":1},"environment_id":"` + env.environmentID + `","vault_ids":["` + env.vaultID + `"],"providers":{"anthropic":{"credential_id":"` + credentialID + `"}}}`
}

func deleteSessionRealDependenciesFile(t *testing.T, service *files.Service, workspaceID workspace.ID, fileID string) {
	t.Helper()
	if _, err := service.DeleteFile(context.Background(), workspaceID, fileID); err != nil {
		t.Fatalf("delete file %s: %v", fileID, err)
	}
}

func assertActiveSessionProviderAuth(t *testing.T, db *sql.DB, sessionID string, providerID string, vaultID string, credentialID string, accessMode string) {
	t.Helper()
	var gotProviderID string
	var gotVaultID string
	var gotCredentialID string
	var gotAccessMode string
	if err := db.QueryRowContext(context.Background(),
		`SELECT provider_id, vault_id, credential_id, access_mode
		   FROM session_provider_auth
		  WHERE workspace_id = $1 AND session_id = $2 AND deleted_at IS NULL`,
		string(workspace.DefaultID), sessionID,
	).Scan(&gotProviderID, &gotVaultID, &gotCredentialID, &gotAccessMode); err != nil {
		t.Fatalf("load active session provider auth %s: %v", sessionID, err)
	}
	if gotProviderID != providerID || gotVaultID != vaultID || gotCredentialID != credentialID || gotAccessMode != accessMode {
		t.Fatalf("active provider auth = %s/%s/%s/%s; want %s/%s/%s/%s", gotProviderID, gotVaultID, gotCredentialID, gotAccessMode, providerID, vaultID, credentialID, accessMode)
	}
	var count int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_provider_auth
		  WHERE workspace_id = $1 AND session_id = $2 AND deleted_at IS NULL`,
		string(workspace.DefaultID), sessionID,
	).Scan(&count); err != nil {
		t.Fatalf("count active session provider auth %s: %v", sessionID, err)
	}
	if count != 1 {
		t.Fatalf("active provider auth row count = %d; want 1", count)
	}
}

func assertNoSessionProviderAuthRows(t *testing.T, db *sql.DB) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM session_provider_auth`).Scan(&count); err != nil {
		t.Fatalf("count session provider auth rows: %v", err)
	}
	if count != 0 {
		t.Fatalf("session_provider_auth row count = %d; want 0", count)
	}
}

type sessionRealDependenciesVaultEncryptor struct{}

func (sessionRealDependenciesVaultEncryptor) Encrypt(value []byte) ([]byte, error) {
	return append([]byte("vault-encrypted:"), value...), nil
}

func (sessionRealDependenciesVaultEncryptor) Decrypt(value []byte) ([]byte, error) {
	return []byte(strings.TrimPrefix(string(value), "vault-encrypted:")), nil
}
