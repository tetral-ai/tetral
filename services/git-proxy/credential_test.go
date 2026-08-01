package gitproxy

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/vault"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const gitProxyTestVaultKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestPostgreSQLRepositoryTokenResolverExcludesDetachedAndDeletingRows(t *testing.T) {
	env := newRepositoryTokenTestEnv(t)
	env.seedSession(t, "sesn_visibility")
	env.seedRepository(t, "sesn_visibility", "rsrc_detached", "https://github.com/tetral-ai/detached", tokenPointer("detached-token"))
	env.seedRepository(t, "sesn_visibility", "rsrc_deleting", "https://github.com/tetral-ai/deleting", tokenPointer("deleting-token"))
	now := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
	if _, err := env.admin.ExecContext(context.Background(),
		`UPDATE session_resources
		    SET detached_at = $1
		  WHERE workspace_id = $2 AND session_id = $3 AND resource_id = 'rsrc_detached'`,
		now, string(workspace.DefaultID), "sesn_visibility",
	); err != nil {
		t.Fatalf("detach resource: %v", err)
	}
	if _, err := env.admin.ExecContext(context.Background(),
		`UPDATE session_resources
		    SET delete_requested_at = $1
		  WHERE workspace_id = $2 AND session_id = $3 AND resource_id = 'rsrc_deleting'`,
		now, string(workspace.DefaultID), "sesn_visibility",
	); err != nil {
		t.Fatalf("mark resource deleting: %v", err)
	}

	for _, repo := range []string{"detached", "deleting"} {
		resolution, err := env.resolver.ResolveRepositoryToken(context.Background(), RepositoryAuthRequest{
			WorkspaceID: workspace.DefaultID,
			SessionID:   "sesn_visibility",
			Owner:       "tetral-ai",
			Repo:        repo,
		})
		if err != nil {
			t.Fatalf("ResolveRepositoryToken(%s): %v", repo, err)
		}
		if resolution.Mounted || resolution.Token != "" {
			t.Fatalf("resolution(%s) = %+v; want not mounted", repo, resolution)
		}
	}
}

func TestPostgreSQLRepositoryTokenResolverObservesRotationWithoutCache(t *testing.T) {
	env := newRepositoryTokenTestEnv(t)
	env.seedSession(t, "sesn_rotate")
	env.seedRepository(t, "sesn_rotate", "rsrc_rotate", "https://github.com/tetral-ai/rotate", tokenPointer("token-before"))

	resolve := func() RepositoryTokenResolution {
		t.Helper()
		resolution, err := env.resolver.ResolveRepositoryToken(context.Background(), RepositoryAuthRequest{
			WorkspaceID: workspace.DefaultID,
			SessionID:   "sesn_rotate",
			Owner:       "tetral-ai",
			Repo:        "rotate",
		})
		if err != nil {
			t.Fatalf("ResolveRepositoryToken: %v", err)
		}
		return resolution
	}
	if got := resolve().Token; got != "token-before" {
		t.Fatalf("initial token = %q; want token-before", got)
	}
	encrypted, err := env.encryptor.Encrypt([]byte("token-after"))
	if err != nil {
		t.Fatalf("encrypt rotated token: %v", err)
	}
	if _, err := env.admin.ExecContext(context.Background(),
		`UPDATE session_github_repository_resources
		    SET authorization_token_encrypted = $1
		  WHERE workspace_id = $2 AND session_id = $3 AND resource_id = $4`,
		encrypted, string(workspace.DefaultID), "sesn_rotate", "rsrc_rotate",
	); err != nil {
		t.Fatalf("rotate token: %v", err)
	}
	if got := resolve().Token; got != "token-after" {
		t.Fatalf("rotated token = %q; want token-after", got)
	}
}

func TestProxyMatchesMixedCaseRepositoryAndRelaysClientCasingVerbatim(t *testing.T) {
	env := newRepositoryTokenTestEnv(t)
	const sessionID = "sesn_mixed_case_repository"
	env.seedSession(t, sessionID)
	env.seedRepository(
		t,
		sessionID,
		"rsrc_mixed_case_repository",
		"https://github.com/Tetral-AI/Repo-Case",
		tokenPointer("mixed-case-token"),
	)

	ticket, ticketHash := deterministicTicket(t, 39)
	tickets := liveTickets(ticketHash)
	tickets[string(ticketHash)].SessionID = sessionID
	var (
		upstreamPath          string
		upstreamAuthorization string
	)
	proxy := testProxyWithTickets(t, tickets, func(w http.ResponseWriter, request *http.Request) {
		upstreamPath = request.URL.Path
		upstreamAuthorization = request.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}, NewRepositoryPolicyAuthorizer(env.resolver))

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodGet,
		"/github.com/tEtRaL-aI/rEpO-cAsE.git/info/refs?service=git-upload-pack",
		nil,
	)
	request.Header.Set("X-Tetral-Git-Ticket", ticket)
	proxy.ServeHTTP(recorder, request)

	wantAuthorization := "Basic " + base64.StdEncoding.EncodeToString([]byte("x-access-token:mixed-case-token"))
	if recorder.Code != http.StatusOK ||
		upstreamPath != "/tEtRaL-aI/rEpO-cAsE.git/info/refs" ||
		upstreamAuthorization != wantAuthorization {
		t.Fatalf(
			"mixed-case relay status/path/auth = %d/%q/%q; want 200/client casing/injected token",
			recorder.Code,
			upstreamPath,
			upstreamAuthorization,
		)
	}
}

func TestProxyFailsClosedOnCaseVariantLegacyRepositoryCollision(t *testing.T) {
	env := newRepositoryTokenTestEnv(t)
	const sessionID = "sesn_case_variant_collision"
	env.seedSession(t, sessionID)
	env.seedRepository(
		t,
		sessionID,
		"rsrc_case_variant_collision_one",
		"https://github.com/Tetral-AI/Repo-Case",
		tokenPointer("collision-token-one"),
	)
	env.seedRepository(
		t,
		sessionID,
		"rsrc_case_variant_collision_two",
		"https://github.com/tetral-ai/repo-case.git",
		tokenPointer("collision-token-two"),
	)

	ticket, ticketHash := deterministicTicket(t, 40)
	tickets := liveTickets(ticketHash)
	tickets[string(ticketHash)].SessionID = sessionID
	upstreamCalls := 0
	proxy := testProxyWithTickets(t, tickets, func(w http.ResponseWriter, _ *http.Request) {
		upstreamCalls++
		w.WriteHeader(http.StatusOK)
	}, NewRepositoryPolicyAuthorizer(env.resolver))

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodGet,
		"/github.com/tEtRaL-aI/rEpO-cAsE.git/info/refs?service=git-upload-pack",
		nil,
	)
	request.Header.Set("X-Tetral-Git-Ticket", ticket)
	proxy.ServeHTTP(recorder, request)

	if recorder.Code != statusBadUpstream || upstreamCalls != 0 {
		t.Fatalf(
			"case-variant collision status/upstream = %d/%d; want %d/0",
			recorder.Code,
			upstreamCalls,
			statusBadUpstream,
		)
	}
}

func TestProxyDoesNotTrimUppercaseGITSuffixFromRepositoryIdentity(t *testing.T) {
	env := newRepositoryTokenTestEnv(t)
	const sessionID = "sesn_uppercase_git_suffix"
	env.seedSession(t, sessionID)
	env.seedRepository(
		t,
		sessionID,
		"rsrc_without_git_suffix",
		"https://github.com/Tetral-AI/Repo-Case",
		tokenPointer("without-suffix-token"),
	)
	env.seedRepository(
		t,
		sessionID,
		"rsrc_uppercase_git_suffix",
		"https://github.com/Tetral-AI/Repo-Case.GIT",
		tokenPointer("uppercase-suffix-token"),
	)

	ticket, ticketHash := deterministicTicket(t, 41)
	tickets := liveTickets(ticketHash)
	tickets[string(ticketHash)].SessionID = sessionID
	var (
		upstreamPath          string
		upstreamAuthorization string
	)
	proxy := testProxyWithTickets(t, tickets, func(w http.ResponseWriter, request *http.Request) {
		upstreamPath = request.URL.Path
		upstreamAuthorization = request.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}, NewRepositoryPolicyAuthorizer(env.resolver))

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodGet,
		"/github.com/tetral-ai/repo-case.GIT/info/refs?service=git-upload-pack",
		nil,
	)
	request.Header.Set("X-Tetral-Git-Ticket", ticket)
	proxy.ServeHTTP(recorder, request)

	wantAuthorization := "Basic " + base64.StdEncoding.EncodeToString([]byte("x-access-token:uppercase-suffix-token"))
	if recorder.Code != http.StatusOK ||
		upstreamPath != "/tetral-ai/repo-case.GIT/info/refs" ||
		upstreamAuthorization != wantAuthorization {
		t.Fatalf(
			"uppercase .GIT relay status/path/auth = %d/%q/%q; want 200/verbatim path/uppercase-suffix token",
			recorder.Code,
			upstreamPath,
			upstreamAuthorization,
		)
	}
}

func TestGitCredentialVectorFileIsExercisedCompletely(t *testing.T) {
	body, err := os.ReadFile(filepath.Join("testdata", "git-credential-vectors.json"))
	if err != nil {
		t.Fatalf("read git credential vectors: %v", err)
	}
	var fixture struct {
		Cases []gitCredentialVectorCase `json:"cases"`
	}
	if err := json.Unmarshal(body, &fixture); err != nil {
		t.Fatalf("decode git credential vectors: %v", err)
	}
	wantNames := map[string]struct{}{
		"mounted token":          {},
		"two repositories":       {},
		"mounted undecryptable":  {},
		"repository not mounted": {},
	}
	if len(fixture.Cases) != len(wantNames) {
		t.Fatalf("git credential vector cases = %d; want %d", len(fixture.Cases), len(wantNames))
	}
	seenNames := make(map[string]struct{}, len(fixture.Cases))
	executed := 0
	for caseIndex, vector := range fixture.Cases {
		t.Run(vector.Name, func(t *testing.T) {
			if vector.Name == "" {
				t.Fatal("vector name is required")
			}
			if _, required := wantNames[vector.Name]; !required {
				t.Fatalf("unexpected vector name %q", vector.Name)
			}
			if _, exists := seenNames[vector.Name]; exists {
				t.Fatalf("duplicate vector name %q", vector.Name)
			}
			seenNames[vector.Name] = struct{}{}
			env := newRepositoryTokenTestEnv(t)
			sessionID := fmt.Sprintf("sesn_vector_%d", caseIndex)
			env.seedSession(t, sessionID)
			for _, repository := range vector.Repositories {
				switch repository.TokenState {
				case "":
					if repository.Token == "" {
						t.Fatalf("repository %s token is required", repository.ResourceID)
					}
					env.seedRepository(t, sessionID, repository.ResourceID, repository.URL, tokenPointer(repository.Token))
				case "undecryptable":
					env.seedRepositoryCiphertext(t, sessionID, repository.ResourceID, repository.URL, []byte("not-valid-ciphertext"))
				default:
					t.Fatalf("repository %s token_state = %q", repository.ResourceID, repository.TokenState)
				}
			}
			ticket, ticketHash := deterministicTicket(t, byte(caseIndex+40))
			tickets := liveTickets(ticketHash)
			tickets[string(ticketHash)].SessionID = sessionID
			upstreamCalls := 0
			upstreamAuthorizations := make([]string, 0, len(vector.Requests))
			proxy := testProxyWithTickets(t, tickets, func(w http.ResponseWriter, request *http.Request) {
				upstreamCalls++
				upstreamAuthorizations = append(upstreamAuthorizations, request.Header.Get("Authorization"))
				w.WriteHeader(http.StatusOK)
			}, NewRepositoryPolicyAuthorizer(env.resolver))
			for _, request := range vector.Requests {
				callsBefore := upstreamCalls
				recorder := httptest.NewRecorder()
				proxyRequest := httptest.NewRequest(
					http.MethodGet,
					"/github.com/"+request.Owner+"/"+request.Repo+"/info/refs?service=git-upload-pack",
					nil,
				)
				proxyRequest.Header.Set("X-Tetral-Git-Ticket", ticket)
				proxy.ServeHTTP(recorder, proxyRequest)
				switch request.Outcome {
				case "token":
					if recorder.Code != http.StatusOK || upstreamCalls != callsBefore+1 {
						t.Fatalf("proxy(%s/%s) status/upstream = %d/%d; want 200/%d", request.Owner, request.Repo, recorder.Code, upstreamCalls, callsBefore+1)
					}
					wantAuthorization := "Basic " + base64.StdEncoding.EncodeToString([]byte("x-access-token:"+request.Token))
					if got := upstreamAuthorizations[len(upstreamAuthorizations)-1]; got != wantAuthorization {
						t.Fatalf("proxy(%s/%s) authorization = %q; want %q", request.Owner, request.Repo, got, wantAuthorization)
					}
				case "credential_required":
					if recorder.Code != http.StatusFailedDependency || upstreamCalls != callsBefore {
						t.Fatalf("proxy(%s/%s) status/upstream = %d/%d; want 424/%d", request.Owner, request.Repo, recorder.Code, upstreamCalls, callsBefore)
					}
				case "anonymous":
					if recorder.Code != http.StatusOK || upstreamCalls != callsBefore+1 {
						t.Fatalf("proxy(%s/%s) status/upstream = %d/%d; want 200/%d", request.Owner, request.Repo, recorder.Code, upstreamCalls, callsBefore+1)
					}
					if got := upstreamAuthorizations[len(upstreamAuthorizations)-1]; got != "" {
						t.Fatalf("proxy(%s/%s) anonymous authorization = %q; want empty", request.Owner, request.Repo, got)
					}
				default:
					t.Fatalf("request outcome = %q", request.Outcome)
				}
			}
			executed++
		})
	}
	if executed != len(fixture.Cases) {
		t.Fatalf("executed vector cases = %d; file cases = %d", executed, len(fixture.Cases))
	}
}

type gitCredentialVectorCase struct {
	Name         string                          `json:"name"`
	Repositories []gitCredentialVectorRepository `json:"repositories"`
	Requests     []gitCredentialVectorRequest    `json:"requests"`
}

type gitCredentialVectorRepository struct {
	ResourceID string `json:"resource_id"`
	URL        string `json:"url"`
	Token      string `json:"token"`
	TokenState string `json:"token_state"`
}

type gitCredentialVectorRequest struct {
	Owner   string `json:"owner"`
	Repo    string `json:"repo"`
	Outcome string `json:"outcome"`
	Token   string `json:"token"`
}

type repositoryTokenTestEnv struct {
	admin     *sql.DB
	resolver  *PostgreSQLRepositoryTokenResolver
	encryptor *vault.Encryptor
}

func newRepositoryTokenTestEnv(t *testing.T) *repositoryTokenTestEnv {
	t.Helper()
	runtimeDB, adminDB := storagetest.NewPostgreSQLDBWithAdmin(t)
	encryptor, err := vault.NewEncryptor(gitProxyTestVaultKey)
	if err != nil {
		t.Fatalf("NewEncryptor: %v", err)
	}
	return &repositoryTokenTestEnv{
		admin:     adminDB,
		resolver:  NewPostgreSQLRepositoryTokenResolver(dbconnect.NewClientForTesting(runtimeDB), encryptor),
		encryptor: encryptor,
	}
}

func (e *repositoryTokenTestEnv) seedSession(t *testing.T, sessionID string) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 7, 17, 9, 0, 0, 0, time.UTC)
	if _, err := e.admin.ExecContext(ctx,
		`INSERT INTO agents (workspace_id, id, name, created_at, updated_at)
		 VALUES ($1, 'agent_git_proxy', 'Git Proxy Agent', $2, $2)
		 ON CONFLICT (workspace_id, id) DO NOTHING`,
		string(workspace.DefaultID), now,
	); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := e.admin.ExecContext(ctx,
		`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at)
		 VALUES ($1, 'agv_git_proxy_1', 'agent_git_proxy', 1, '{}', 'hash_git_proxy', $2)
		 ON CONFLICT (workspace_id, agent_id, version) DO NOTHING`,
		string(workspace.DefaultID), now,
	); err != nil {
		t.Fatalf("seed agent version: %v", err)
	}
	if _, err := e.admin.ExecContext(ctx,
		`INSERT INTO environments (workspace_id, id, name, config_json, created_at, updated_at)
		 VALUES ($1, 'env_git_proxy', 'Git Proxy Env', '{}', $2, $2)
		 ON CONFLICT (workspace_id, id) DO NOTHING`,
		string(workspace.DefaultID), now,
	); err != nil {
		t.Fatalf("seed environment: %v", err)
	}
	if _, err := e.admin.ExecContext(ctx,
		`INSERT INTO sessions (
			workspace_id, id, type, metadata_json, status, lifecycle_state,
			agent_id, agent_version, environment_id, vault_ids_json,
			created_at, updated_at
		) VALUES (
			$1, $2, 'session', '{}', 'idle', 'admitted',
			'agent_git_proxy', 1, 'env_git_proxy', '[]',
			$3, $3
		)`,
		string(workspace.DefaultID), sessionID, now,
	); err != nil {
		t.Fatalf("seed session: %v", err)
	}
}

func (e *repositoryTokenTestEnv) seedRepository(t *testing.T, sessionID string, resourceID string, repoURL string, token *string) {
	t.Helper()
	var encrypted []byte
	if token != nil {
		var err error
		encrypted, err = e.encryptor.Encrypt([]byte(*token))
		if err != nil {
			t.Fatalf("encrypt token: %v", err)
		}
	}
	e.seedRepositoryCiphertext(t, sessionID, resourceID, repoURL, encrypted)
}

func (e *repositoryTokenTestEnv) seedRepositoryCiphertext(t *testing.T, sessionID string, resourceID string, repoURL string, encrypted []byte) {
	t.Helper()
	now := time.Date(2026, 7, 17, 9, 0, 0, 0, time.UTC)
	if _, err := e.admin.ExecContext(context.Background(),
		`INSERT INTO session_resources (
			workspace_id, session_id, resource_id, type, created_at, updated_at
		) VALUES ($1, $2, $3, 'github_repository', $4, $4)`,
		string(workspace.DefaultID), sessionID, resourceID, now,
	); err != nil {
		t.Fatalf("seed session resource: %v", err)
	}
	if _, err := e.admin.ExecContext(context.Background(),
		`INSERT INTO session_github_repository_resources (
			workspace_id, session_id, resource_id, url, mount_path, checkout_type, checkout_ref,
			authorization_token_encrypted
		) VALUES ($1, $2, $3, $4, $5, NULL, NULL, $6)`,
		string(workspace.DefaultID), sessionID, resourceID, repoURL, "/workspace/"+resourceID, encrypted,
	); err != nil {
		t.Fatalf("seed github repository resource: %v", err)
	}
}

func tokenPointer(value string) *string {
	return &value
}
