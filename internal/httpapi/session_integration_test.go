package httpapi_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
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
	"github.com/tetral-ai/tetral/internal/workspace"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"
)

const (
	sessionIntegrationAgentID     = "agent_http_session"
	sessionIntegrationEnvironment = "env_http_session"
	sessionIntegrationMemoryStore = "memstore_http_session"
	sessionIntegrationSourceFileA = "file_http_source_a"
	sessionIntegrationSourceFileB = "file_http_source_b"
)

type sessionIntegrationEnv struct {
	router http.Handler
	admin  *sql.DB
	clock  time.Time
}

func newSessionIntegrationEnv(t *testing.T) *sessionIntegrationEnv {
	t.Helper()
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Skip(storagetest.EnvTestDatabaseURL + " is not set")
	}
	runtimeDB, adminDB := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedSessionIntegrationReferences(t, adminDB, workspace.DefaultID)

	testEnv := &sessionIntegrationEnv{
		admin: adminDB,
		clock: time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC),
	}
	runtimeClient := dbconnect.NewClientForTesting(runtimeDB)
	sessionStore := session.NewPostgreSQLSessionStore(
		runtimeClient,
		session.WithPageTokenSecret([]byte("session-integration-secret-12345")),
		session.WithSessionDeleteSandboxRelease(func(ctx context.Context, tx *dbconnect.Tx, workspaceID workspace.ID, sessionID string, now time.Time) error {
			_, _, err := tetralsandbox.EnsureSandboxReleaseTx(ctx, tx, string(workspaceID), sessionID, tetralsandbox.SandboxReleaseSessionDelete, "", now)
			return err
		}),
	)
	fileStore := files.NewPostgreSQLStore(runtimeClient, blob.NewFakeBlobStore())
	service := session.NewService(
		sessionIntegrationAgents{},
		sessionIntegrationEnvironments{},
		files.NewService(fileStore),
		sessionIntegrationMemories{},
		sessionIntegrationVaults{},
		sessionStore,
		sessionIntegrationEncryptor{},
		session.WithClock(func() time.Time { return testEnv.clock }),
	)
	authenticator := auth.AuthenticatorFunc(func(context.Context, string) (auth.Principal, error) {
		return auth.Principal{Workspace: workspace.Workspace{ID: workspace.DefaultID}, APIKeyID: "ak_session_integration"}, nil //nolint:gosec // G101: synthetic test API key id
	})
	testEnv.router = httpapi.NewRouter(httpapi.NewSessionHandler(service), "", httpapi.WithAuthenticator(authenticator))
	return testEnv
}

func TestSessionIntegrationCreateListResourcePathUsesServiceStoreAndFiles(t *testing.T) {
	env := newSessionIntegrationEnv(t)

	const authorizationToken = "github_token_create_a"
	createRecorder := env.request(http.MethodPost, "/v1/sessions?beta=true", `{
			"agent":{"type":"agent","id":"agent_http_session","version":2},
			"environment_id":"env_http_session",
			"title":"integration session",
			"metadata":{"team":"runtime"},
			"vault_ids":["vlt_b","vlt_a"],
			"resources":[
				{"type":"file","file_id":"file_http_source_a","mount_path":"/workspace/input.txt"},
				{"type":"memory_store","memory_store_id":"memstore_http_session","instructions":"use the stable snapshot"},
				{"type":"github_repository","url":"https://github.com/tetral-ai/tetral.git","authorization_token":"`+authorizationToken+`","checkout":{"type":"branch","name":"main"}}
			]
		}`)
	assertHTTPStatus(t, createRecorder, http.StatusOK)
	if strings.Contains(createRecorder.Body.String(), authorizationToken) {
		t.Fatalf("session create response leaked submitted authorization token: %s", createRecorder.Body.String())
	}
	var created sessionIntegrationSessionResponse
	decodeSessionIntegrationJSON(t, createRecorder, &created)

	if created.Status != string(session.StatusIdle) || created.Agent.Version != 2 {
		t.Fatalf("created session status/version = %s/%d", created.Status, created.Agent.Version)
	}
	if len(created.Agent.Skills) != 1 || created.Agent.Skills[0]["id"] != "skill_alpha" {
		t.Fatalf("agent skills = %#v; want skill_alpha from service assembly", created.Agent.Skills)
	}
	if strings.Join(created.VaultIDs, ",") != "vlt_b,vlt_a" {
		t.Fatalf("vault_ids = %#v; want caller order", created.VaultIDs)
	}
	if len(created.Resources) != 3 {
		t.Fatalf("resources = %#v; want file, memory_store, github_repository", created.Resources)
	}
	fileResource := created.Resources[0]
	if fileResource.Type != string(session.ResourceTypeFile) || fileResource.FileID == sessionIntegrationSourceFileA || !strings.HasPrefix(fileResource.FileID, files.IDPrefix) {
		t.Fatalf("file resource = %#v; want session-scoped file identity", fileResource)
	}
	memoryResource := created.Resources[1]
	if memoryResource.Type != string(session.ResourceTypeMemoryStore) || memoryResource.Access != "read_only" {
		t.Fatalf("memory resource = %#v; want omitted access to default read_only", memoryResource)
	}
	sessionFile := loadSessionIntegrationFileRow(t, env.admin, workspace.DefaultID, fileResource.FileID)
	sourceFile := loadSessionIntegrationFileRow(t, env.admin, workspace.DefaultID, sessionIntegrationSourceFileA)
	if sessionFile.ObjectID != sourceFile.ObjectID || sessionFile.ScopeType.String != "session" || sessionFile.ScopeID.String != created.ID {
		t.Fatalf("session file row = %#v source = %#v; want shared object scoped to session", sessionFile, sourceFile)
	}

	listRecorder := env.request(http.MethodGet, "/v1/sessions?beta=true&agent_id=agent_http_session&agent_version=2&memory_store_id=memstore_http_session&order=asc", "")
	assertHTTPStatus(t, listRecorder, http.StatusOK)
	var sessionList sessionIntegrationListResponse
	decodeSessionIntegrationJSON(t, listRecorder, &sessionList)
	if len(sessionList.Data) != 1 || sessionList.Data[0].ID != created.ID {
		t.Fatalf("session list = %#v; want created session through store filters", sessionList.Data)
	}

	resourcesRecorder := env.request(http.MethodGet, "/v1/sessions/"+created.ID+"/resources?limit=1", "")
	assertHTTPStatus(t, resourcesRecorder, http.StatusOK)
	var firstResources sessionIntegrationResourceListResponse
	decodeSessionIntegrationJSON(t, resourcesRecorder, &firstResources)
	if len(firstResources.Data) != 1 || firstResources.Data[0].ID != fileResource.ID || firstResources.NextPage == nil {
		t.Fatalf("first resource page = %#v next=%v; want file resource and next page", firstResources.Data, firstResources.NextPage)
	}
	secondResourcesRecorder := env.request(http.MethodGet, "/v1/sessions/"+created.ID+"/resources?limit=1&page="+url.QueryEscape(*firstResources.NextPage), "")
	assertHTTPStatus(t, secondResourcesRecorder, http.StatusOK)
	var secondResources sessionIntegrationResourceListResponse
	decodeSessionIntegrationJSON(t, secondResourcesRecorder, &secondResources)
	if len(secondResources.Data) != 1 || secondResources.Data[0].Type != string(session.ResourceTypeMemoryStore) {
		t.Fatalf("second resource page = %#v; want memory_store in storage order", secondResources.Data)
	}

	addRecorder := env.request(http.MethodPost, "/v1/sessions/"+created.ID+"/resources", `{"type":"file","file_id":"file_http_source_b","mount_path":"/workspace/add.txt"}`)
	assertHTTPStatus(t, addRecorder, http.StatusOK)
	var added sessionIntegrationResourceResponse
	decodeSessionIntegrationJSON(t, addRecorder, &added)
	addedFile := loadSessionIntegrationFileRow(t, env.admin, workspace.DefaultID, added.FileID)
	if addedFile.ObjectID == "" || addedFile.ScopeType.String != "session" || addedFile.ScopeID.String != created.ID {
		t.Fatalf("added file row = %#v; want session-scoped identity", addedFile)
	}

	deleteRecorder := env.request(http.MethodDelete, "/v1/sessions/"+created.ID+"/resources/"+added.ID, "")
	assertHTTPStatus(t, deleteRecorder, http.StatusOK)
	deletedFile := loadSessionIntegrationFileRow(t, env.admin, workspace.DefaultID, added.FileID)
	if !deletedFile.DeletedAt.Valid {
		t.Fatalf("deleted file row = %#v; want tombstoned session file identity", deletedFile)
	}
}

func TestSessionIntegrationReadUpdateArchiveDeleteAndResourcePolicies(t *testing.T) {
	env := newSessionIntegrationEnv(t)
	created := env.createSession(t, `{
		"agent":{"type":"agent","id":"agent_http_session","version":2},
		"environment_id":"env_http_session",
		"title":"before update",
		"metadata":{"team":"control-plane","drop":"old"},
		"vault_ids":[],
		"resources":[
			{"type":"file","file_id":"file_http_source_a","mount_path":"/workspace/input.txt"},
			{"type":"memory_store","memory_store_id":"memstore_http_session","access":"read_only","instructions":"use the stable snapshot"},
			{"type":"github_repository","url":"https://github.com/tetral-ai/tetral.git","authorization_token":"github_token_create_b","checkout":{"type":"branch","name":"main"}}
		]
	}`)
	fileResource := findSessionIntegrationResource(t, created.Resources, string(session.ResourceTypeFile))
	memoryResource := findSessionIntegrationResource(t, created.Resources, string(session.ResourceTypeMemoryStore))
	githubResource := findSessionIntegrationResource(t, created.Resources, string(session.ResourceTypeGitHubRepository))

	getRecorder := env.request(http.MethodGet, "/v1/sessions/"+created.ID+"?beta=true", "")
	assertHTTPStatus(t, getRecorder, http.StatusOK)
	var got sessionIntegrationSessionResponse
	decodeSessionIntegrationJSON(t, getRecorder, &got)
	if got.ID != created.ID || got.Agent.Version != 2 || got.Metadata["drop"] != "old" {
		t.Fatalf("GET session = %#v; want created state", got)
	}

	updateRecorder := env.request(http.MethodPost, "/v1/sessions/"+created.ID+"?beta=true", `{"title":"after update","metadata":{"team":"control","drop":null}}`)
	assertHTTPStatus(t, updateRecorder, http.StatusOK)
	var updated sessionIntegrationSessionResponse
	decodeSessionIntegrationJSON(t, updateRecorder, &updated)
	if updated.Title == nil || *updated.Title != "after update" || updated.Metadata["team"] != "control" {
		t.Fatalf("updated session = %#v; want patched title and metadata", updated)
	}
	if _, ok := updated.Metadata["drop"]; ok {
		t.Fatalf("updated metadata retained deleted key: %#v", updated.Metadata)
	}

	resourceRecorder := env.request(http.MethodGet, "/v1/sessions/"+created.ID+"/resources/"+fileResource.ID, "")
	assertHTTPStatus(t, resourceRecorder, http.StatusOK)
	var gotFileResource sessionIntegrationResourceResponse
	decodeSessionIntegrationJSON(t, resourceRecorder, &gotFileResource)
	if gotFileResource.FileID != fileResource.FileID || gotFileResource.FileID == sessionIntegrationSourceFileA {
		t.Fatalf("GET file resource = %#v; want session-scoped B identity", gotFileResource)
	}

	for _, resource := range []sessionIntegrationResourceResponse{fileResource, memoryResource} {
		recorder := env.request(http.MethodPost, "/v1/sessions/"+created.ID+"/resources/"+resource.ID, `{"field":"removed_route_value"}`)
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("update %s resource status = %d; want 400 body=%s", resource.Type, recorder.Code, recorder.Body.String())
		}
		assertErrorType(t, recorder, "invalid_request_error")
		if strings.Contains(recorder.Body.String(), "removed_route_value") {
			t.Fatalf("removed resource update echoed request body value: %s", recorder.Body.String())
		}
	}

	rotateRecorder := env.request(http.MethodPost, "/v1/sessions/"+created.ID+"/resources/"+githubResource.ID+"?beta=true", `{"authorization_token":"github_token_rotated"}`)
	assertHTTPStatus(t, rotateRecorder, http.StatusOK)
	if strings.Contains(rotateRecorder.Body.String(), "github_token_rotated") {
		t.Fatalf("GitHub resource update echoed write-only token: %s", rotateRecorder.Body.String())
	}
	var encryptedToken []byte
	if err := env.admin.QueryRowContext(context.Background(),
		`SELECT authorization_token_encrypted
		   FROM session_github_repository_resources
		  WHERE workspace_id = $1 AND session_id = $2 AND resource_id = $3`,
		string(workspace.DefaultID), created.ID, githubResource.ID,
	).Scan(&encryptedToken); err != nil {
		t.Fatalf("read rotated GitHub resource token: %v", err)
	}
	if string(encryptedToken) != "encrypted:github_token_rotated" {
		t.Fatalf("encrypted token = %q; want rotated encrypted value", encryptedToken)
	}

	for _, resource := range []sessionIntegrationResourceResponse{memoryResource, githubResource} {
		recorder := env.request(http.MethodDelete, "/v1/sessions/"+created.ID+"/resources/"+resource.ID, "")
		if recorder.Code != http.StatusOK {
			t.Fatalf("delete %s resource status = %d; want 200 body=%s", resource.Type, recorder.Code, recorder.Body.String())
		}
		var deletedResource sessionIntegrationDeleteResponse
		decodeSessionIntegrationJSON(t, recorder, &deletedResource)
		if deletedResource.ID != resource.ID || deletedResource.Type != "session_resource_deleted" {
			t.Fatalf("delete %s resource response = %#v", resource.Type, deletedResource)
		}
	}

	addRecorder := env.request(http.MethodPost, "/v1/sessions/"+created.ID+"/resources", `{"type":"file","file_id":"file_http_source_b","mount_path":"/workspace/delete-me.txt"}`)
	assertHTTPStatus(t, addRecorder, http.StatusOK)
	var added sessionIntegrationResourceResponse
	decodeSessionIntegrationJSON(t, addRecorder, &added)
	assertHTTPStatus(t, env.request(http.MethodDelete, "/v1/sessions/"+created.ID+"/resources/"+added.ID, ""), http.StatusOK)
	if got := env.request(http.MethodGet, "/v1/sessions/"+created.ID+"/resources/"+added.ID, ""); got.Code != http.StatusNotFound {
		t.Fatalf("GET detached resource status = %d; want 404 body=%s", got.Code, got.Body.String())
	}
	listAfterDetach := env.request(http.MethodGet, "/v1/sessions/"+created.ID+"/resources", "")
	assertHTTPStatus(t, listAfterDetach, http.StatusOK)
	var listedAfterDetach sessionIntegrationResourceListResponse
	decodeSessionIntegrationJSON(t, listAfterDetach, &listedAfterDetach)
	for _, resource := range listedAfterDetach.Data {
		if resource.ID == added.ID {
			t.Fatalf("detached resource remained listed: %#v", listedAfterDetach.Data)
		}
	}

	archiveRecorder := env.request(http.MethodPost, "/v1/sessions/"+created.ID+"/archive?beta=true", "")
	assertHTTPStatus(t, archiveRecorder, http.StatusOK)
	var archived sessionIntegrationSessionResponse
	decodeSessionIntegrationJSON(t, archiveRecorder, &archived)
	if archived.ArchivedAt == nil {
		t.Fatalf("archived_at = nil; want archive timestamp")
	}
	assertHTTPStatus(t, env.request(http.MethodGet, "/v1/sessions/"+created.ID+"?beta=true", ""), http.StatusOK)
	assertHTTPStatus(t, env.request(http.MethodGet, "/v1/sessions/"+created.ID+"/resources/"+memoryResource.ID, ""), http.StatusNotFound)

	deleteRecorder := env.request(http.MethodDelete, "/v1/sessions/"+created.ID+"?beta=true", "")
	assertHTTPStatus(t, deleteRecorder, http.StatusOK)
	var deleted sessionIntegrationDeleteResponse
	decodeSessionIntegrationJSON(t, deleteRecorder, &deleted)
	if deleted.ID != created.ID || deleted.Type != "session_deleted" {
		t.Fatalf("delete response = %#v; want deleted session", deleted)
	}
	if got := env.request(http.MethodGet, "/v1/sessions/"+created.ID+"?beta=true", ""); got.Code != http.StatusNotFound {
		t.Fatalf("GET deleted session status = %d; want 404 body=%s", got.Code, got.Body.String())
	}
	sourceFile := loadSessionIntegrationFileRow(t, env.admin, workspace.DefaultID, sessionIntegrationSourceFileA)
	deletedSessionFile := loadSessionIntegrationFileRow(t, env.admin, workspace.DefaultID, fileResource.FileID)
	if sourceFile.DeletedAt.Valid {
		t.Fatalf("source file A was tombstoned by session delete: %#v", sourceFile)
	}
	if !deletedSessionFile.DeletedAt.Valid {
		t.Fatalf("session file B was not tombstoned by session delete: %#v", deletedSessionFile)
	}
}

func TestSessionIntegrationAdmittedSessionRemainsPublicBeforeSandboxUse(t *testing.T) {
	env := newSessionIntegrationEnv(t)
	if _, err := env.admin.ExecContext(context.Background(),
		`UPDATE environment_artifacts
		    SET status = 'pending', provider_artifact_ref = NULL
		  WHERE workspace_id = $1 AND environment_id = $2 AND generation = 1`,
		string(workspace.DefaultID), sessionIntegrationEnvironment); err != nil {
		t.Fatalf("make environment artifact unready: %v", err)
	}
	created := env.createSession(t, `{
		"agent":{"type":"agent","id":"agent_http_session","version":2},
		"environment_id":"env_http_session",
		"title":"admitted preparation",
		"vault_ids":[],
		"resources":[{"type":"file","file_id":"file_http_source_a","mount_path":"/workspace/input.txt"}]
	}`)
	fileResource := findSessionIntegrationResource(t, created.Resources, string(session.ResourceTypeFile))

	if got := countSessionIntegrationRows(t, env.admin, `SELECT count(*) FROM session_sandbox_bindings WHERE workspace_id = $1`, workspace.DefaultID); got != 0 {
		t.Fatalf("sandbox bindings after public admission = %d; want lazy binding on first Sandbox tool", got)
	}

	assertHTTPStatus(t, env.request(http.MethodGet, "/v1/sessions/"+created.ID+"?beta=true", ""), http.StatusOK)
	assertHTTPStatus(t, env.request(http.MethodGet, "/v1/sessions/"+created.ID+"/resources", ""), http.StatusOK)
	assertHTTPStatus(t, env.request(http.MethodGet, "/v1/sessions/"+created.ID+"/resources/"+fileResource.ID, ""), http.StatusOK)

	listRecorder := env.request(http.MethodGet, "/v1/sessions?beta=true&agent_id=agent_http_session&agent_version=2", "")
	assertHTTPStatus(t, listRecorder, http.StatusOK)
	var sessionList sessionIntegrationListResponse
	decodeSessionIntegrationJSON(t, listRecorder, &sessionList)
	for _, item := range sessionList.Data {
		if item.ID == created.ID {
			return
		}
	}
	t.Fatalf("session list omitted admitted session %s: %+v", created.ID, sessionList.Data)
}

func TestSessionIntegrationArchiveDeleteRunningSessionUsesPublicConflictEnvelope(t *testing.T) {
	for _, test := range []struct {
		name    string
		method  string
		pathFor func(string) string
		message string
	}{
		{
			name:    "archive",
			method:  http.MethodPost,
			pathFor: func(sessionID string) string { return "/v1/sessions/" + sessionID + "/archive?beta=true" },
			message: "running sessions cannot be archived",
		},
		{
			name:    "delete",
			method:  http.MethodDelete,
			pathFor: func(sessionID string) string { return "/v1/sessions/" + sessionID + "?beta=true" },
			message: "running sessions cannot be deleted",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			env := newSessionIntegrationEnv(t)
			created := env.createSession(t, `{"agent":"agent_http_session","environment_id":"env_http_session","vault_ids":[]}`)
			setHTTPSessionRuntimeStatus(t, env.admin, workspace.DefaultID, created.ID, session.StatusRunning)

			recorder := env.request(test.method, test.pathFor(created.ID), "")

			if recorder.Code != http.StatusConflict {
				t.Fatalf("status = %d; want 409 body=%s", recorder.Code, recorder.Body.String())
			}
			assertErrorType(t, recorder, "invalid_request_error")
			if !strings.Contains(recorder.Body.String(), test.message) {
				t.Fatalf("body missing %q: %s", test.message, recorder.Body.String())
			}
			status, archivedAt := loadHTTPSessionLifecycle(t, env.admin, workspace.DefaultID, created.ID)
			if status != string(session.StatusIdle) || archivedAt.Valid {
				t.Fatalf("session lifecycle after rejected %s = status %s archived_at %#v; want durable idle and unarchived", test.name, status, archivedAt)
			}
			if runtimeStatus := loadHTTPSessionRuntimeStatus(t, env.admin, workspace.DefaultID, created.ID); runtimeStatus != string(session.StatusRunning) {
				t.Fatalf("runtime status after rejected %s = %s; want running", test.name, runtimeStatus)
			}
		})
	}
}

func TestSessionIntegrationRejectsNullUpdateWithoutChangingUpdatedAt(t *testing.T) {
	env := newSessionIntegrationEnv(t)
	created := env.createSession(t, `{
		"agent":"agent_http_session",
		"environment_id":"env_http_session",
		"title":"stable update target",
		"vault_ids":[]
	}`)
	before := loadSessionIntegrationUpdatedAt(t, env.admin, workspace.DefaultID, created.ID)
	env.clock = before.Add(time.Hour)

	for _, test := range []struct {
		name string
		body string
	}{
		{name: "null body", body: `null`},
	} {
		t.Run(test.name, func(t *testing.T) {
			recorder := env.request(http.MethodPost, "/v1/sessions/"+created.ID+"?beta=true", test.body)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d; want 400 body=%s", recorder.Code, recorder.Body.String())
			}
			assertErrorType(t, recorder, "invalid_request_error")
			after := loadSessionIntegrationUpdatedAt(t, env.admin, workspace.DefaultID, created.ID)
			if !after.Equal(before) {
				t.Fatalf("updated_at = %s; want unchanged %s", after, before)
			}
		})
	}
}

func TestSessionIntegrationOwnershipFailuresLeaveNoSessionOwnedRows(t *testing.T) {
	env := newSessionIntegrationEnv(t)
	otherWorkspace := workspace.ID("workspace_b")
	seedHTTPWorkspace(t, env.admin, otherWorkspace)
	seedSessionIntegrationSourceFile(t, env.admin, otherWorkspace, "file_http_other_workspace")

	createRecorder := env.request(http.MethodPost, "/v1/sessions?beta=true", `{
		"agent":"agent_http_session",
		"environment_id":"env_http_session",
		"vault_ids":[],
		"resources":[{"type":"file","file_id":"file_http_other_workspace","mount_path":"/workspace/other.txt"}]
	}`)
	if createRecorder.Code != http.StatusNotFound {
		t.Fatalf("cross-workspace create status = %d; want 404 body=%s", createRecorder.Code, createRecorder.Body.String())
	}
	assertErrorType(t, createRecorder, "not_found_error")
	assertSessionIntegrationOwnedRowCounts(t, env.admin, workspace.DefaultID, sessionIntegrationRowCounts{})

	created := env.createSession(t, `{"agent":"agent_http_session","environment_id":"env_http_session","vault_ids":[]}`)
	before := loadSessionIntegrationOwnedRowCounts(t, env.admin, workspace.DefaultID)
	addRecorder := env.request(http.MethodPost, "/v1/sessions/"+created.ID+"/resources", `{"type":"file","file_id":"file_http_other_workspace","mount_path":"/workspace/other.txt"}`)
	if addRecorder.Code != http.StatusNotFound {
		t.Fatalf("cross-workspace add status = %d; want 404 body=%s", addRecorder.Code, addRecorder.Body.String())
	}
	assertErrorType(t, addRecorder, "not_found_error")
	after := loadSessionIntegrationOwnedRowCounts(t, env.admin, workspace.DefaultID)
	if after != before {
		t.Fatalf("row counts after rejected add = %+v; want unchanged %+v", after, before)
	}
}

func TestSessionIntegrationResourceListRequiresExistingParent(t *testing.T) {
	env := newSessionIntegrationEnv(t)

	recorder := env.request(http.MethodGet, "/v1/sessions/sesn_missing/resources", "")

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d; want 404 for missing parent session body=%s", recorder.Code, recorder.Body.String())
	}
	assertErrorType(t, recorder, "not_found_error")
}

func TestSessionIntegrationArchivedSessionRejectsResourceMutations(t *testing.T) {
	env := newSessionIntegrationEnv(t)
	created := env.createSession(t, `{
		"agent":{"type":"agent","id":"agent_http_session","version":2},
		"environment_id":"env_http_session",
		"vault_ids":[],
		"resources":[
			{"type":"file","file_id":"file_http_source_a","mount_path":"/workspace/input.txt"},
			{"type":"github_repository","url":"https://github.com/tetral-ai/tetral","authorization_token":"github_token_archived","checkout":{"type":"branch","name":"main"}}
		]
	}`)
	assertHTTPStatus(t, env.request(http.MethodPost, "/v1/sessions/"+created.ID+"/archive?beta=true", ""), http.StatusOK)

	var fileResourceID, githubResourceID string
	for _, resource := range created.Resources {
		switch resource.Type {
		case string(session.ResourceTypeFile):
			fileResourceID = resource.ID
		case string(session.ResourceTypeGitHubRepository):
			githubResourceID = resource.ID
		}
	}
	mutations := []struct {
		name          string
		method        string
		path          string
		body          string
		wantStatus    int
		wantErrorType string
	}{
		{
			name:          "add file",
			method:        http.MethodPost,
			path:          "/v1/sessions/" + created.ID + "/resources",
			body:          `{"type":"file","file_id":"file_http_source_b","mount_path":"/workspace/add.txt"}`,
			wantStatus:    http.StatusConflict,
			wantErrorType: "invalid_request_error",
		},
		{
			name:          "rotate github resource token",
			method:        http.MethodPost,
			path:          "/v1/sessions/" + created.ID + "/resources/" + githubResourceID + "?beta=true",
			body:          `{"authorization_token":"github_token_after_archive"}`,
			wantStatus:    http.StatusConflict,
			wantErrorType: "invalid_request_error",
		},
		{
			name:          "delete file",
			method:        http.MethodDelete,
			path:          "/v1/sessions/" + created.ID + "/resources/" + fileResourceID,
			wantStatus:    http.StatusConflict,
			wantErrorType: "invalid_request_error",
		},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			recorder := env.request(mutation.method, mutation.path, mutation.body)
			if recorder.Code != mutation.wantStatus {
				t.Fatalf("status = %d; want %d for archived session mutation body=%s", recorder.Code, mutation.wantStatus, recorder.Body.String())
			}
			assertErrorType(t, recorder, mutation.wantErrorType)
		})
	}
}

func TestSessionIntegrationListDefaultsToNewestFirst(t *testing.T) {
	env := newSessionIntegrationEnv(t)
	env.clock = time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	older := env.createSession(t, `{"agent":"agent_http_session","environment_id":"env_http_session","vault_ids":[],"title":"older"}`)
	env.clock = env.clock.Add(time.Minute)
	newer := env.createSession(t, `{"agent":"agent_http_session","environment_id":"env_http_session","vault_ids":[],"title":"newer"}`)

	recorder := env.request(http.MethodGet, "/v1/sessions?beta=true", "")
	assertHTTPStatus(t, recorder, http.StatusOK)
	var response sessionIntegrationListResponse
	decodeSessionIntegrationJSON(t, recorder, &response)
	if len(response.Data) < 2 {
		t.Fatalf("session list = %#v; want two sessions", response.Data)
	}
	if response.Data[0].ID != newer.ID || response.Data[1].ID != older.ID {
		t.Fatalf("default order ids = %s,%s; want newest-first %s,%s", response.Data[0].ID, response.Data[1].ID, newer.ID, older.ID)
	}
}

func TestSessionIntegrationRejectsExplicitAgentVersionZero(t *testing.T) {
	env := newSessionIntegrationEnv(t)

	recorder := env.request(http.MethodPost, "/v1/sessions?beta=true", `{"agent":{"type":"agent","id":"agent_http_session","version":0},"environment_id":"env_http_session","vault_ids":[]}`)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d; want 400 for explicit version zero body=%s", recorder.Code, recorder.Body.String())
	}
	assertErrorType(t, recorder, "invalid_request_error")
}

func TestSessionIntegrationAcceptsDocumentedCommitCheckoutSHA(t *testing.T) {
	env := newSessionIntegrationEnv(t)
	sha := "0123456789abcdef0123456789abcdef01234567"

	recorder := env.request(http.MethodPost, "/v1/sessions?beta=true", `{
		"agent":{"type":"agent","id":"agent_http_session","version":2},
		"environment_id":"env_http_session",
		"vault_ids":[],
		"resources":[
			{"type":"github_repository","url":"https://github.com/tetral-ai/tetral","authorization_token":"github_token_commit","checkout":{"type":"commit","sha":"`+sha+`"}}
		]
	}`)

	assertHTTPStatus(t, recorder, http.StatusOK)
	var created sessionIntegrationSessionResponse
	decodeSessionIntegrationJSON(t, recorder, &created)
	if len(created.Resources) != 1 {
		t.Fatalf("resources = %#v; want one github_repository", created.Resources)
	}
	if created.Resources[0].Checkout["type"] != "commit" || created.Resources[0].Checkout["sha"] != sha || len(created.Resources[0].Checkout) != 2 {
		t.Fatalf("checkout = %#v; want nested commit/%s", created.Resources[0].Checkout, sha)
	}
	if created.Resources[0].CheckoutType != "" || created.Resources[0].CheckoutRef != "" {
		t.Fatalf("response leaked legacy flat checkout: %s/%s", created.Resources[0].CheckoutType, created.Resources[0].CheckoutRef)
	}
}

func (e *sessionIntegrationEnv) createSession(t *testing.T, body string) sessionIntegrationSessionResponse {
	t.Helper()
	recorder := e.request(http.MethodPost, "/v1/sessions?beta=true", body)
	assertHTTPStatus(t, recorder, http.StatusOK)
	if strings.Contains(recorder.Body.String(), "encrypted:") {
		t.Fatalf("session response leaked credential material: %s", recorder.Body.String())
	}
	var response sessionIntegrationSessionResponse
	decodeSessionIntegrationJSON(t, recorder, &response)
	return response
}

func (e *sessionIntegrationEnv) request(method string, path string, body string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	setAuthHeader(request)
	e.router.ServeHTTP(recorder, request)
	return recorder
}

func assertHTTPStatus(t *testing.T, recorder *httptest.ResponseRecorder, want int) {
	t.Helper()
	if recorder.Code != want {
		t.Fatalf("status = %d; want %d body=%s", recorder.Code, want, recorder.Body.String())
	}
}

func decodeSessionIntegrationJSON(t *testing.T, recorder *httptest.ResponseRecorder, target any) {
	t.Helper()
	if err := json.Unmarshal(recorder.Body.Bytes(), target); err != nil {
		t.Fatalf("decode JSON response: %v body=%s", err, recorder.Body.String())
	}
}

type sessionIntegrationSessionResponse struct {
	ID            string                               `json:"id"`
	Status        string                               `json:"status"`
	Title         *string                              `json:"title"`
	ArchivedAt    *string                              `json:"archived_at"`
	Agent         sessionIntegrationAgentResponse      `json:"agent"`
	EnvironmentID string                               `json:"environment_id"`
	VaultIDs      []string                             `json:"vault_ids"`
	Providers     session.ProviderSelectors            `json:"providers"`
	Metadata      map[string]string                    `json:"metadata"`
	Resources     []sessionIntegrationResourceResponse `json:"resources"`
}

type sessionIntegrationAgentResponse struct {
	ID      string              `json:"id"`
	Version int                 `json:"version"`
	Skills  []map[string]string `json:"skills"`
}

type sessionIntegrationResourceResponse struct {
	ID            string            `json:"id"`
	Type          string            `json:"type"`
	FileID        string            `json:"file_id"`
	MemoryStoreID string            `json:"memory_store_id"`
	Access        string            `json:"access"`
	URL           string            `json:"url"`
	MountPath     string            `json:"mount_path"`
	CheckoutType  string            `json:"checkout_type"`
	CheckoutRef   string            `json:"checkout_ref"`
	Checkout      map[string]string `json:"checkout"`
}

type sessionIntegrationListResponse struct {
	Data     []sessionIntegrationSessionResponse `json:"data"`
	NextPage *string                             `json:"next_page"`
}

type sessionIntegrationResourceListResponse struct {
	Data     []sessionIntegrationResourceResponse `json:"data"`
	NextPage *string                              `json:"next_page"`
}

type sessionIntegrationDeleteResponse struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

type sessionIntegrationFileRow struct {
	ObjectID  string
	ScopeType sql.NullString
	ScopeID   sql.NullString
	DeletedAt sql.NullString
}

type sessionIntegrationRowCounts struct {
	Sessions                  int
	Resources                 int
	FileResources             int
	MemoryStoreResources      int
	GitHubRepositoryResources int
	SessionScopedFiles        int
}

func findSessionIntegrationResource(t *testing.T, resources []sessionIntegrationResourceResponse, resourceType string) sessionIntegrationResourceResponse {
	t.Helper()
	for _, resource := range resources {
		if resource.Type == resourceType {
			return resource
		}
	}
	t.Fatalf("resource type %s not found in %#v", resourceType, resources)
	return sessionIntegrationResourceResponse{}
}

func loadSessionIntegrationFileRow(t *testing.T, db *sql.DB, workspaceID workspace.ID, fileID string) sessionIntegrationFileRow {
	t.Helper()
	var row sessionIntegrationFileRow
	if err := db.QueryRowContext(context.Background(),
		`SELECT object_id, scope_type, scope_id, deleted_at
		   FROM files
		  WHERE workspace_id = $1 AND file_id = $2`,
		string(workspaceID), fileID,
	).Scan(&row.ObjectID, &row.ScopeType, &row.ScopeID, &row.DeletedAt); err != nil {
		t.Fatalf("load file row %s: %v", fileID, err)
	}
	return row
}

func loadSessionIntegrationUpdatedAt(t *testing.T, db *sql.DB, workspaceID workspace.ID, sessionID string) time.Time {
	t.Helper()
	var raw string
	if err := db.QueryRowContext(context.Background(),
		`SELECT updated_at
		   FROM sessions
		  WHERE workspace_id = $1 AND id = $2`,
		string(workspaceID), sessionID,
	).Scan(&raw); err != nil {
		t.Fatalf("load session updated_at %s: %v", sessionID, err)
	}
	updatedAt, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		t.Fatalf("parse session updated_at %q: %v", raw, err)
	}
	return updatedAt
}

func setHTTPSessionRuntimeStatus(t *testing.T, db *sql.DB, workspaceID workspace.ID, sessionID string, status session.Status) {
	t.Helper()
	var idleSince any = "2026-05-11T12:00:00Z"
	if status == session.StatusRunning {
		idleSince = nil
	}
	result, err := db.ExecContext(context.Background(),
		`UPDATE session_runtime_status
		    SET status = $1,
		        idle_since = $2,
		        updated_at = '2026-05-11T12:00:00Z'
		  WHERE workspace_id = $3 AND session_id = $4`,
		string(status), idleSince, string(workspaceID), sessionID,
	)
	if err != nil {
		t.Fatalf("set session runtime status %s: %v", sessionID, err)
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		t.Fatalf("set session runtime status affected %d rows; want 1", affected)
	}
}

func loadHTTPSessionRuntimeStatus(t *testing.T, db *sql.DB, workspaceID workspace.ID, sessionID string) string {
	t.Helper()
	var status string
	if err := db.QueryRowContext(context.Background(),
		`SELECT status
		   FROM session_runtime_status
		  WHERE workspace_id = $1 AND session_id = $2`,
		string(workspaceID), sessionID,
	).Scan(&status); err != nil {
		t.Fatalf("load session runtime status %s: %v", sessionID, err)
	}
	return status
}

func loadHTTPSessionLifecycle(t *testing.T, db *sql.DB, workspaceID workspace.ID, sessionID string) (string, sql.NullString) {
	t.Helper()
	var status string
	var archivedAt sql.NullString
	if err := db.QueryRowContext(context.Background(),
		`SELECT status, archived_at
		   FROM sessions
		  WHERE workspace_id = $1 AND id = $2`,
		string(workspaceID), sessionID,
	).Scan(&status, &archivedAt); err != nil {
		t.Fatalf("load session lifecycle %s: %v", sessionID, err)
	}
	return status, archivedAt
}

func loadSessionIntegrationOwnedRowCounts(t *testing.T, db *sql.DB, workspaceID workspace.ID) sessionIntegrationRowCounts {
	t.Helper()
	return sessionIntegrationRowCounts{
		Sessions:                  countSessionIntegrationRows(t, db, `SELECT count(*) FROM sessions WHERE workspace_id = $1`, workspaceID),
		Resources:                 countSessionIntegrationRows(t, db, `SELECT count(*) FROM session_resources WHERE workspace_id = $1`, workspaceID),
		FileResources:             countSessionIntegrationRows(t, db, `SELECT count(*) FROM session_file_resources WHERE workspace_id = $1`, workspaceID),
		MemoryStoreResources:      countSessionIntegrationRows(t, db, `SELECT count(*) FROM session_memory_store_resources WHERE workspace_id = $1`, workspaceID),
		GitHubRepositoryResources: countSessionIntegrationRows(t, db, `SELECT count(*) FROM session_github_repository_resources WHERE workspace_id = $1`, workspaceID),
		SessionScopedFiles:        countSessionIntegrationRows(t, db, `SELECT count(*) FROM files WHERE workspace_id = $1 AND scope_type = 'session'`, workspaceID),
	}
}

func assertSessionIntegrationOwnedRowCounts(t *testing.T, db *sql.DB, workspaceID workspace.ID, want sessionIntegrationRowCounts) {
	t.Helper()
	if got := loadSessionIntegrationOwnedRowCounts(t, db, workspaceID); got != want {
		t.Fatalf("row counts = %+v; want %+v", got, want)
	}
}

func countSessionIntegrationRows(t *testing.T, db *sql.DB, query string, workspaceID workspace.ID) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(), query, string(workspaceID)).Scan(&count); err != nil {
		t.Fatalf("count query %q: %v", query, err)
	}
	return count
}

func seedSessionIntegrationReferences(t *testing.T, db *sql.DB, workspaceID workspace.ID) {
	t.Helper()
	seedHTTPWorkspace(t, db, workspaceID)
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO agents (workspace_id, id, name, version, created_at, updated_at)
		 VALUES ($1, $2, 'HTTP Session Agent', 3, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
		 ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version`,
		string(workspaceID), sessionIntegrationAgentID,
	); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	for _, version := range []int{1, 2, 3} {
		if _, err := db.ExecContext(context.Background(),
			`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at)
			 VALUES ($1, $2, $3, $4, $5, $6, '2026-01-01T00:00:00Z')
			 ON CONFLICT (agent_id, version) DO NOTHING`,
			string(workspaceID),
			"agv_http_session_"+string(rune('0'+version)),
			sessionIntegrationAgentID,
			version,
			`{"name":"HTTP Session Agent","model":"anthropic/claude-opus-4-8","skills":[{"type":"custom","id":"skill_alpha"}]}`,
			"hash-http-session-"+string(rune('0'+version)),
		); err != nil {
			t.Fatalf("seed agent version %d: %v", version, err)
		}
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO environments (workspace_id, id, name, config_json, created_at, updated_at)
		 VALUES ($1, $2, 'HTTP Session Environment', '{}', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
		 ON CONFLICT (id) DO NOTHING`,
		string(workspaceID), sessionIntegrationEnvironment,
	); err != nil {
		t.Fatalf("seed environment: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO environment_artifacts (
			workspace_id, environment_id, generation, status, provider, provider_artifact_ref,
			normalized_config_hash, artifact_input_hash, runtime_network_policy_json, packages_json,
			created_at, updated_at
		) VALUES (
			$1, $2, 1, 'ready', 'daytona', 'artifact_http_session',
			'hash-http-session-config', 'hash-http-session-artifact', '{"type":"unrestricted"}', '{}',
			'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		)
		ON CONFLICT (workspace_id, environment_id, generation) DO UPDATE SET
			status = EXCLUDED.status,
			provider_artifact_ref = EXCLUDED.provider_artifact_ref,
			updated_at = EXCLUDED.updated_at`,
		string(workspaceID), sessionIntegrationEnvironment,
	); err != nil {
		t.Fatalf("seed ready environment artifact: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO memory_stores (workspace_id, memory_store_id, name, description, metadata_json, created_at, updated_at)
		 VALUES ($1, $2, 'Shared Memory', 'stable memory snapshot', '{}', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
		 ON CONFLICT (memory_store_id) DO NOTHING`,
		string(workspaceID), sessionIntegrationMemoryStore,
	); err != nil {
		t.Fatalf("seed memory store: %v", err)
	}
	seedSessionIntegrationSourceFile(t, db, workspaceID, sessionIntegrationSourceFileA)
	seedSessionIntegrationSourceFile(t, db, workspaceID, sessionIntegrationSourceFileB)
}

func seedSessionIntegrationSourceFile(t *testing.T, db *sql.DB, workspaceID workspace.ID, fileID string) {
	t.Helper()
	objectID := "fobj_" + fileID
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO file_objects (workspace_id, object_id, blob_key, size_bytes, sha256, created_at)
		 VALUES ($1, $2, $3, 5, 'sha', '2026-01-01T00:00:00Z')
		 ON CONFLICT (object_id) DO NOTHING`,
		string(workspaceID), objectID, "files/"+string(workspaceID)+"/"+objectID,
	); err != nil {
		t.Fatalf("seed file object %s: %v", objectID, err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO files (workspace_id, file_id, object_id, filename, mime_type, downloadable, created_at)
		 VALUES ($1, $2, $3, $4, 'text/plain', true, '2026-01-01T00:00:00Z')
		 ON CONFLICT (file_id) DO NOTHING`,
		string(workspaceID), fileID, objectID, fileID+".txt",
	); err != nil {
		t.Fatalf("seed source file %s: %v", fileID, err)
	}
}

type sessionIntegrationAgents struct{}

func (sessionIntegrationAgents) Get(context.Context, workspace.ID, string) (*agent.Agent, error) {
	return newSessionIntegrationAgent(3), nil
}

func (sessionIntegrationAgents) GetVersion(_ context.Context, _ workspace.ID, _ string, version int) (*agent.Agent, error) {
	return newSessionIntegrationAgent(version), nil
}

func newSessionIntegrationAgent(version int) *agent.Agent {
	now := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	return &agent.Agent{
		ID:      sessionIntegrationAgentID,
		Type:    "agent",
		Version: version,
		AgentConfig: agent.AgentConfig{
			Name:   "HTTP Session Agent",
			Model:  "anthropic/claude-opus-4-8",
			Tools:  agent.RawArray{json.RawMessage(`{"type":"tetral_agent_toolset","family":"claude"}`)},
			Skills: agent.RawArray{json.RawMessage(`{"type":"custom","id":"skill_alpha"}`)},
		},
		CreatedAt: now,
		UpdatedAt: now,
	}
}

type sessionIntegrationEnvironments struct{}

func (sessionIntegrationEnvironments) Get(context.Context, workspace.ID, string) (*environment.Environment, error) {
	return &environment.Environment{ID: sessionIntegrationEnvironment, Type: "environment", Name: "HTTP Session Environment"}, nil
}

type sessionIntegrationMemories struct{}

func (sessionIntegrationMemories) GetStore(context.Context, workspace.ID, string) (*memory.Store, error) {
	return &memory.Store{
		ID:          sessionIntegrationMemoryStore,
		Type:        "memory_store",
		Name:        "Shared Memory",
		Description: "stable memory snapshot",
	}, nil
}

type sessionIntegrationVaults struct{}

func (sessionIntegrationVaults) ValidateVaultReferences(context.Context, workspace.ID, []string) error {
	return nil
}

type sessionIntegrationEncryptor struct{}

func (sessionIntegrationEncryptor) Encrypt(value []byte) ([]byte, error) {
	return append([]byte("encrypted:"), value...), nil
}
