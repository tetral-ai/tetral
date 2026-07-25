package httpapi_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/memory"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestMemoryRoutesAreRegisteredAndAuthenticated(t *testing.T) {
	env := newAuthTestEnv(t)
	service := memory.NewService(memory.NewPostgreSQLStore(dbconnect.NewClientForTesting(env.runtime)))
	router := env.router(httpapi.WithMemoryHandler(httpapi.NewMemoryHandler(service)))

	routes := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/v1/memory_stores"},
		{http.MethodGet, "/v1/memory_stores"},
		{http.MethodGet, "/v1/memory_stores/memstore_test"},
		{http.MethodPost, "/v1/memory_stores/memstore_test"},
		{http.MethodDelete, "/v1/memory_stores/memstore_test"},
		{http.MethodPost, "/v1/memory_stores/memstore_test/archive"},
		{http.MethodPost, "/v1/memory_stores/memstore_test/memories"},
		{http.MethodGet, "/v1/memory_stores/memstore_test/memories"},
		{http.MethodGet, "/v1/memory_stores/memstore_test/memories/mem_test"},
		{http.MethodPost, "/v1/memory_stores/memstore_test/memories/mem_test"},
		{http.MethodDelete, "/v1/memory_stores/memstore_test/memories/mem_test"},
		{http.MethodGet, "/v1/memory_stores/memstore_test/memory_versions"},
		{http.MethodGet, "/v1/memory_stores/memstore_test/memory_versions/memver_test"},
		{http.MethodPost, "/v1/memory_stores/memstore_test/memory_versions/memver_test/redact"},
	}

	for _, route := range routes {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			code, body := performJSONRequest(t, router, route.method, route.path, "", "")
			if code != http.StatusUnauthorized || !strings.Contains(body, `"type":"authentication_error"`) {
				t.Fatalf("status/body = %d %s; want 401 authentication_error", code, body)
			}
		})
	}
}

func TestMemoryHTTPStoreMemoryAndVersionHappyPath(t *testing.T) {
	env := newAuthTestEnv(t)
	service := memory.NewService(memory.NewPostgreSQLStore(dbconnect.NewClientForTesting(env.runtime)))
	router := env.router(httpapi.WithMemoryHandler(httpapi.NewMemoryHandler(service)))
	principal, err := env.store.AuthenticateRawKey(defaultWorkspaceContext(), env.envKey)
	if err != nil {
		t.Fatalf("AuthenticateRawKey: %v", err)
	}

	code, body := performJSONRequest(t, router, http.MethodPost, "/v1/memory_stores", env.envKey, `{"name":"http-store","metadata":{"team":"runtime"}}`)
	if code != http.StatusOK {
		t.Fatalf("create store status/body = %d %s; want 200", code, body)
	}
	var store memory.Store
	decodeMemoryJSON(t, body, &store)

	code, body = performJSONRequest(t, router, http.MethodGet, "/v1/memory_stores/"+store.ID, env.envKey, "")
	if code != http.StatusOK || !strings.Contains(body, `"name":"http-store"`) {
		t.Fatalf("get store status/body = %d %s; want created store", code, body)
	}
	code, body = performJSONRequest(t, router, http.MethodPost, "/v1/memory_stores/"+store.ID, env.envKey, `{"description":null,"metadata":{"team":null,"region":"iad"}}`)
	if code != http.StatusOK {
		t.Fatalf("update store status/body = %d %s; want 200", code, body)
	}
	var updatedStore memory.Store
	decodeMemoryJSON(t, body, &updatedStore)
	if updatedStore.Description != "" || updatedStore.Metadata["region"] != "iad" {
		t.Fatalf("updated store = %+v; want cleared description and patched metadata", updatedStore)
	}
	if _, ok := updatedStore.Metadata["team"]; ok {
		t.Fatalf("metadata null value must delete team: %+v", updatedStore.Metadata)
	}
	code, body = performJSONRequest(t, router, http.MethodGet, "/v1/memory_stores", env.envKey, "")
	if code != http.StatusOK || !strings.Contains(body, `"data":[`) || !strings.Contains(body, store.ID) {
		t.Fatalf("list stores status/body = %d %s; want listed store", code, body)
	}

	code, body = performJSONRequest(t, router, http.MethodPost, "/v1/memory_stores/"+store.ID+"/memories?view=full", env.envKey, `{"path":"/notes/a.md","content":"hello"}`)
	if code != http.StatusOK {
		t.Fatalf("create memory status/body = %d %s; want 200", code, body)
	}
	var created memory.Memory
	decodeMemoryJSON(t, body, &created)
	if created.Content == nil || *created.Content != "hello" {
		t.Fatalf("created content = %v; want hello", created.Content)
	}

	code, body = performJSONRequest(t, router, http.MethodGet, "/v1/memory_stores/"+store.ID+"/memories/"+created.ID+"?view=full", env.envKey, "")
	if code != http.StatusOK || !strings.Contains(body, `"content":"hello"`) {
		t.Fatalf("get memory status/body = %d %s; want full memory", code, body)
	}
	code, body = performJSONRequest(t, router, http.MethodPost, "/v1/memory_stores/"+store.ID+"/memories/"+created.ID+"?view=full", env.envKey, `{"content":"updated","precondition":{"type":"content_sha256","content_sha256":"`+created.ContentSHA256+`"}}`)
	if code != http.StatusOK {
		t.Fatalf("update memory status/body = %d %s; want 200", code, body)
	}
	var updated memory.Memory
	decodeMemoryJSON(t, body, &updated)
	if updated.Content == nil || *updated.Content != "updated" {
		t.Fatalf("updated content = %v; want updated", updated.Content)
	}
	code, body = performJSONRequest(t, router, http.MethodGet, "/v1/memory_stores/"+store.ID+"/memories?view=full&order_by=path&order=asc", env.envKey, "")
	if code != http.StatusOK || !strings.Contains(body, `"path":"/notes/a.md"`) {
		t.Fatalf("list memories status/body = %d %s; want memory", code, body)
	}
	code, body = performJSONRequest(t, router, http.MethodGet, "/v1/memory_stores/"+store.ID+"/memory_versions?view=full&memory_id="+created.ID, env.envKey, "")
	if code != http.StatusOK {
		t.Fatalf("list versions status/body = %d %s; want 200", code, body)
	}
	var versionList memory.MemoryVersionListResult
	decodeMemoryJSON(t, body, &versionList)
	if len(versionList.Data) != 2 {
		t.Fatalf("version count = %d; want 2", len(versionList.Data))
	}
	if versionList.Data[1].CreatedBy.APIKeyID != principal.APIKeyID {
		t.Fatalf("created_by api_key_id = %q; want authenticated key id %q", versionList.Data[1].CreatedBy.APIKeyID, principal.APIKeyID)
	}
	versionID := versionList.Data[1].ID
	code, body = performJSONRequest(t, router, http.MethodGet, "/v1/memory_stores/"+store.ID+"/memory_versions?session_id=sesn_missing", env.envKey, "")
	if code != http.StatusOK {
		t.Fatalf("list versions by session_id status/body = %d %s; want 200", code, body)
	}
	decodeMemoryJSON(t, body, &versionList)
	if len(versionList.Data) != 0 {
		t.Fatalf("session_id filter returned %d versions; want 0", len(versionList.Data))
	}
	code, body = performJSONRequest(t, router, http.MethodGet, "/v1/memory_stores/"+store.ID+"/memory_versions/"+versionID+"?view=full", env.envKey, "")
	if code != http.StatusOK || !strings.Contains(body, `"created_by":{"type":"api_actor","api_key_id":"`+principal.APIKeyID+`"}`) {
		t.Fatalf("get version status/body = %d %s; want api actor from auth", code, body)
	}
	code, body = performJSONRequest(t, router, http.MethodDelete, "/v1/memory_stores/"+store.ID+"/memories/"+created.ID+"?expected_content_sha256="+updated.ContentSHA256, env.envKey, "")
	if code != http.StatusOK || !strings.Contains(body, `"type":"memory_deleted"`) {
		t.Fatalf("delete memory status/body = %d %s; want memory_deleted", code, body)
	}
	code, body = performJSONRequest(t, router, http.MethodPost, "/v1/memory_stores/"+store.ID+"/archive", env.envKey, "")
	if code != http.StatusOK || !strings.Contains(body, `"archived_at":"`) {
		t.Fatalf("archive store status/body = %d %s; want archived store", code, body)
	}
	code, body = performJSONRequest(t, router, http.MethodDelete, "/v1/memory_stores/"+store.ID, env.envKey, "")
	if code != http.StatusOK || !strings.Contains(body, `"type":"memory_store_deleted"`) {
		t.Fatalf("delete store status/body = %d %s; want memory_store_deleted", code, body)
	}
}

func TestMemoryHTTPDeleteStoreRejectsDurableSessionReferences(t *testing.T) {
	env := newAuthTestEnv(t)
	service := memory.NewService(memory.NewPostgreSQLStore(dbconnect.NewClientForTesting(env.runtime)))
	router := env.router(httpapi.WithMemoryHandler(httpapi.NewMemoryHandler(service)))
	store, err := service.CreateStore(defaultWorkspaceContext(), workspace.DefaultID, memory.CreateStoreRequest{Name: "referenced"})
	if err != nil {
		t.Fatalf("CreateStore: %v", err)
	}
	seedHTTPMemoryStoreReference(t, env.admin, workspace.DefaultID, "sesn_http_memory_reference", store.ID)

	code, body := performJSONRequest(t, router, http.MethodDelete, "/v1/memory_stores/"+store.ID, env.envKey, "")

	if code != http.StatusBadRequest || !strings.Contains(body, `"type":"invalid_request_error"`) {
		t.Fatalf("delete store status/body = %d %s; want 400 invalid_request_error", code, body)
	}
	if _, err := service.GetStore(defaultWorkspaceContext(), workspace.DefaultID, store.ID); err != nil {
		t.Fatalf("referenced store should remain after rejected delete: %v", err)
	}
}

func TestMemoryHTTPStrictValidation(t *testing.T) {
	env := newAuthTestEnv(t)
	service := memory.NewService(memory.NewPostgreSQLStore(dbconnect.NewClientForTesting(env.runtime)))
	router := env.router(httpapi.WithMemoryHandler(httpapi.NewMemoryHandler(service)))
	store, err := service.CreateStore(defaultWorkspaceContext(), workspace.DefaultID, memory.CreateStoreRequest{Name: "strict"})
	if err != nil {
		t.Fatalf("CreateStore: %v", err)
	}
	created, err := service.CreateMemory(defaultWorkspaceContext(), workspace.DefaultID, store.ID, memory.CreateMemoryRequest{Path: "/strict.md", Content: "one", ContentSet: true}, memory.Actor{Type: memory.ActorAPI, APIKeyID: authenticatedAPIKeyID(t, env)})
	if err != nil {
		t.Fatalf("CreateMemory: %v", err)
	}

	cases := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"duplicate query", http.MethodGet, "/v1/memory_stores/" + store.ID + "/memories?view=full&view=basic", ""},
		{"unknown query", http.MethodGet, "/v1/memory_stores/" + store.ID + "/memories?unsupported=1", ""},
		{"unsupported view", http.MethodGet, "/v1/memory_stores/" + store.ID + "/memories/" + created.ID + "?view=wide", ""},
		{"unsupported order_by", http.MethodGet, "/v1/memory_stores/" + store.ID + "/memories?order_by=bad", ""},
		{"unsupported order", http.MethodGet, "/v1/memory_stores/" + store.ID + "/memories?order=bad", ""},
		{"unsupported operation", http.MethodGet, "/v1/memory_stores/" + store.ID + "/memory_versions?operation=bad", ""},
		{"body actor", http.MethodPost, "/v1/memory_stores/" + store.ID + "/memories", `{"path":"/actor.md","content":"x","created_by":{"type":"api_actor","api_key_id":"ak_fake"}}`},
		{"body hash", http.MethodPost, "/v1/memory_stores/" + store.ID + "/memories/" + created.ID, `{"content":"two","content_sha256":"fake"}`},
		{"oversized body", http.MethodPost, "/v1/memory_stores/" + store.ID + "/memories", `{"path":"/too-large.md","content":"` + strings.Repeat("x", 102401) + `"}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, body := performJSONRequest(t, router, tc.method, tc.path, env.envKey, tc.body)
			if code != http.StatusBadRequest && code != http.StatusRequestEntityTooLarge {
				t.Fatalf("status/body = %d %s; want 400 or 413", code, body)
			}
			if !strings.Contains(body, `"type":"invalid_request_error"`) && !strings.Contains(body, `"type":"request_too_large"`) {
				t.Fatalf("body = %s; want centralized validation envelope", body)
			}
		})
	}
}

func TestMemoryHTTPBodyCapUsesCentralizedRequestTooLargeEnvelope(t *testing.T) {
	router := newMemoryValidationRouter()

	code, body := performJSONRequest(t, router, http.MethodPost, "/v1/memory_stores", "test-key", `{"name":"`+strings.Repeat("x", 1<<20)+`"}`)

	if code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status/body = %d %s; want 413", code, body)
	}
	if !strings.Contains(body, `"type":"request_too_large"`) {
		t.Fatalf("body = %s; want request_too_large envelope", body)
	}
}

func TestMemoryHTTPOversizedMemoryContentIsInvalidRequest(t *testing.T) {
	router := newMemoryValidationRouter()

	oversizedContent := strings.Repeat("x", 102401)
	code, body := performJSONRequest(t, router, http.MethodPost, "/v1/memory_stores/memstore_test/memories", "test-key", `{"path":"/too-large.md","content":"`+oversizedContent+`"}`)
	if code != http.StatusBadRequest || !strings.Contains(body, `"type":"invalid_request_error"`) {
		t.Fatalf("oversized create status/body = %d %s; want 400 invalid_request_error", code, body)
	}

	code, body = performJSONRequest(t, router, http.MethodPost, "/v1/memory_stores/memstore_test/memories/mem_test", "test-key", `{"content":"`+oversizedContent+`"}`)
	if code != http.StatusBadRequest || !strings.Contains(body, `"type":"invalid_request_error"`) {
		t.Fatalf("oversized update status/body = %d %s; want 400 invalid_request_error", code, body)
	}
}

func TestMemoryHTTPCreateMemoryRejectsNullContentAndAcceptsEmptyContent(t *testing.T) {
	service := &createMemoryCaptureService{}
	authenticator := auth.AuthenticatorFunc(func(context.Context, string) (auth.Principal, error) {
		return auth.Principal{Workspace: workspace.Workspace{ID: workspace.DefaultID}, APIKeyID: "ak_memory_empty_content"}, nil
	})
	router := httpapi.NewRouter(nil, "", httpapi.WithAuthenticator(authenticator), httpapi.WithMemoryHandler(httpapi.NewMemoryHandler(service)))

	code, body := performJSONRequest(t, router, http.MethodPost, "/v1/memory_stores/memstore_test/memories", "test-key", `{"path":"/null.md","content":null}`)
	if code != http.StatusBadRequest || !strings.Contains(body, `"type":"invalid_request_error"`) {
		t.Fatalf("null content status/body = %d %s; want 400 invalid_request_error", code, body)
	}
	if service.called {
		t.Fatal("null content reached memory service")
	}

	code, body = performJSONRequest(t, router, http.MethodPost, "/v1/memory_stores/memstore_test/memories", "test-key", `{"path":"/empty.md","content":""}`)
	if code != http.StatusOK {
		t.Fatalf("empty content status/body = %d %s; want 200", code, body)
	}
	if !service.called || service.request == nil {
		t.Fatal("empty content did not reach memory service")
	}
	if service.storeID != "memstore_test" || service.request.Path != "/empty.md" || service.request.Content != "" || !service.request.ContentSet || service.request.View != memory.ViewBasic {
		t.Fatalf("captured request = store %q request %+v", service.storeID, service.request)
	}
	var created memory.Memory
	decodeMemoryJSON(t, body, &created)
	if created.Content == nil || *created.Content != "" {
		t.Fatalf("created content = %v; want empty string", created.Content)
	}
	if created.ContentSHA256 != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" || created.ContentSizeBytes != 0 {
		t.Fatalf("hash/size = %s/%d; want empty content hash and zero size", created.ContentSHA256, created.ContentSizeBytes)
	}
}

func TestMemoryHTTPUpdateMemoryDefaultsToBasicView(t *testing.T) {
	service := &updateMemoryCaptureService{}
	authenticator := auth.AuthenticatorFunc(func(context.Context, string) (auth.Principal, error) {
		return auth.Principal{Workspace: workspace.Workspace{ID: workspace.DefaultID}, APIKeyID: "ak_memory_basic_update"}, nil
	})
	router := httpapi.NewRouter(nil, "", httpapi.WithAuthenticator(authenticator), httpapi.WithMemoryHandler(httpapi.NewMemoryHandler(service)))

	code, body := performJSONRequest(t, router, http.MethodPost, "/v1/memory_stores/memstore_test/memories/mem_test", "test-key", `{"content":"updated"}`)
	if code != http.StatusOK {
		t.Fatalf("status/body = %d %s; want 200", code, body)
	}
	if service.request == nil || service.request.View != memory.ViewBasic {
		t.Fatalf("captured request = %+v; want basic view", service.request)
	}
}

type createMemoryCaptureService struct {
	panicMemoryService
	called  bool
	storeID string
	request *memory.CreateMemoryRequest
}

type updateMemoryCaptureService struct {
	panicMemoryService
	request *memory.UpdateMemoryRequest
}

func (s *updateMemoryCaptureService) UpdateMemory(_ context.Context, _ workspace.ID, storeID string, memoryID string, request memory.UpdateMemoryRequest, _ memory.Actor) (*memory.Memory, error) {
	s.request = &request
	return &memory.Memory{ID: memoryID, Type: "memory", MemoryStoreID: storeID, Path: "/updated.md"}, nil
}

func (s *createMemoryCaptureService) CreateMemory(_ context.Context, _ workspace.ID, storeID string, request memory.CreateMemoryRequest, _ memory.Actor) (*memory.Memory, error) {
	s.called = true
	s.storeID = storeID
	s.request = &request
	content := request.Content
	return &memory.Memory{
		ID:               "mem_empty",
		Type:             "memory",
		MemoryStoreID:    storeID,
		MemoryVersionID:  "memver_empty",
		Path:             request.Path,
		Content:          &content,
		ContentSHA256:    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		ContentSizeBytes: int64(len([]byte(content))),
	}, nil
}

func newMemoryValidationRouter() http.Handler {
	authenticator := auth.AuthenticatorFunc(func(context.Context, string) (auth.Principal, error) {
		return auth.Principal{Workspace: workspace.Workspace{ID: workspace.DefaultID}, APIKeyID: "ak_memory_content_validation"}, nil
	})
	return httpapi.NewRouter(nil, "", httpapi.WithAuthenticator(authenticator), httpapi.WithMemoryHandler(httpapi.NewMemoryHandler(panicMemoryService{})))
}

func seedHTTPMemoryStoreReference(t *testing.T, db *sql.DB, workspaceID workspace.ID, sessionID string, memoryStoreID string) {
	t.Helper()
	agentID := "agent_" + sessionID
	environmentID := "env_" + sessionID
	resourceID := "sesrsc_" + sessionID
	ctx := context.Background()
	if _, err := db.ExecContext(ctx,
		`INSERT INTO agents (workspace_id, id, name, version, created_at, updated_at)
		 VALUES ($1, $2, $2, 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
		 ON CONFLICT (id) DO NOTHING`,
		string(workspaceID), agentID); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at)
		 VALUES ($1, $2, $3, 1, '{}', $4, '2026-01-01T00:00:00Z')
		 ON CONFLICT (agent_id, version) DO NOTHING`,
		string(workspaceID), "agv_"+sessionID, agentID, "hash_"+sessionID); err != nil {
		t.Fatalf("seed agent version: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO environments (workspace_id, id, name, config_json, created_at, updated_at)
		 VALUES ($1, $2, $2, '{}', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
		 ON CONFLICT (id) DO NOTHING`,
		string(workspaceID), environmentID); err != nil {
		t.Fatalf("seed environment: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO sessions (workspace_id, id, type, metadata_json, status, lifecycle_state, agent_id, agent_version, environment_id, vault_ids_json, created_at, updated_at)
		 VALUES ($1, $2, 'session', '{}', 'idle', 'active', $3, 1, $4, '[]', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		string(workspaceID), sessionID, agentID, environmentID); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO session_resources (workspace_id, session_id, resource_id, type, created_at, updated_at)
		 VALUES ($1, $2, $3, 'memory_store', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		string(workspaceID), sessionID, resourceID); err != nil {
		t.Fatalf("seed session resource: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO session_memory_store_resources (workspace_id, session_id, resource_id, memory_store_id, access, instructions, name, description, mount_path)
		 VALUES ($1, $2, $3, $4, 'read_write', '', 'Referenced', '', '/mnt/memory/referenced')`,
		string(workspaceID), sessionID, resourceID, memoryStoreID); err != nil {
		t.Fatalf("seed memory resource: %v", err)
	}
}

func TestMemoryHTTPPathConflictIncludesFields(t *testing.T) {
	env := newAuthTestEnv(t)
	service := memory.NewService(memory.NewPostgreSQLStore(dbconnect.NewClientForTesting(env.runtime)))
	router := env.router(httpapi.WithMemoryHandler(httpapi.NewMemoryHandler(service)))

	code, body := performJSONRequest(t, router, http.MethodPost, "/v1/memory_stores", env.envKey, `{"name":"conflict"}`)
	if code != http.StatusOK {
		t.Fatalf("create store status/body = %d %s", code, body)
	}
	var store memory.Store
	decodeMemoryJSON(t, body, &store)
	code, body = performJSONRequest(t, router, http.MethodPost, "/v1/memory_stores/"+store.ID+"/memories", env.envKey, `{"path":"/same.md","content":"one"}`)
	if code != http.StatusOK {
		t.Fatalf("create first memory status/body = %d %s", code, body)
	}
	var first memory.Memory
	decodeMemoryJSON(t, body, &first)

	code, body = performJSONRequest(t, router, http.MethodPost, "/v1/memory_stores/"+store.ID+"/memories", env.envKey, `{"path":"/same.md","content":"two"}`)
	if code != http.StatusConflict {
		t.Fatalf("duplicate path status/body = %d %s; want 409", code, body)
	}
	for _, want := range []string{
		`"type":"invalid_request_error"`,
		`"message":"memory path already exists"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("conflict body = %s; missing %s", body, want)
		}
	}
	if strings.Contains(body, first.ID) || strings.Contains(body, "conflicting_memory_id") || strings.Contains(body, "conflicting_path") {
		t.Fatalf("conflict body leaked non-SDK memory conflict fields: %s", body)
	}
}

func TestMemoryHTTPUnsupportedVersionMutationRoutesDoNotMutate(t *testing.T) {
	env := newAuthTestEnv(t)
	service := memory.NewService(memory.NewPostgreSQLStore(dbconnect.NewClientForTesting(env.runtime)))
	router := env.router(httpapi.WithMemoryHandler(httpapi.NewMemoryHandler(service)))
	store, err := service.CreateStore(defaultWorkspaceContext(), workspace.DefaultID, memory.CreateStoreRequest{Name: "unsupported-version-mutation"})
	if err != nil {
		t.Fatalf("CreateStore: %v", err)
	}
	created, err := service.CreateMemory(defaultWorkspaceContext(), workspace.DefaultID, store.ID, memory.CreateMemoryRequest{Path: "/version.md", Content: "one", ContentSet: true}, memory.Actor{Type: memory.ActorAPI, APIKeyID: authenticatedAPIKeyID(t, env)})
	if err != nil {
		t.Fatalf("CreateMemory: %v", err)
	}
	routes := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/v1/memory_stores/" + store.ID + "/memory_versions"},
		{http.MethodPost, "/v1/memory_stores/" + store.ID + "/memory_versions/" + created.MemoryVersionID},
		{http.MethodDelete, "/v1/memory_stores/" + store.ID + "/memory_versions/" + created.MemoryVersionID},
		{http.MethodPost, "/v1/memory_stores/" + store.ID + "/memory_versions/" + created.MemoryVersionID + "/archive"},
	}
	for _, route := range routes {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			code, body := performJSONRequest(t, router, route.method, route.path, env.envKey, `{"content":"mutate"}`)
			if code != http.StatusNotFound && code != http.StatusMethodNotAllowed && code != http.StatusNotImplemented {
				t.Fatalf("status/body = %d %s; want absent, 405, or existing stub response", code, body)
			}
		})
	}
	versions, err := service.ListMemoryVersions(defaultWorkspaceContext(), workspace.DefaultID, store.ID, memory.ListMemoryVersionsOptions{
		MemoryID: created.ID,
		View:     memory.ViewFull,
	})
	if err != nil {
		t.Fatalf("ListMemoryVersions: %v", err)
	}
	if len(versions.Data) != 1 || versions.Data[0].Content == nil || *versions.Data[0].Content != "one" {
		t.Fatalf("unsupported routes mutated versions: %+v", versions.Data)
	}
}

func TestMemoryVersionRedactRejectsBodyWithoutSideEffect(t *testing.T) {
	env := newAuthTestEnv(t)
	service := memory.NewService(memory.NewPostgreSQLStore(dbconnect.NewClientForTesting(env.runtime)))
	router := env.router(httpapi.WithMemoryHandler(httpapi.NewMemoryHandler(service)))
	store, oldVersionID := createRedactableVersionViaHTTPTestService(t, service, "redact-body")

	code, body := performJSONRequest(t, router, http.MethodPost,
		"/v1/memory_stores/"+store.ID+"/memory_versions/"+oldVersionID+"/redact",
		env.envKey,
		`{}`,
	)
	if code != http.StatusBadRequest || !strings.Contains(body, `"type":"invalid_request_error"`) {
		t.Fatalf("redact with body status/body = %d %s; want 400 invalid_request_error", code, body)
	}
	version, err := service.GetMemoryVersion(defaultWorkspaceContext(), workspace.DefaultID, store.ID, oldVersionID, memory.ViewFull)
	if err != nil {
		t.Fatalf("GetMemoryVersion after rejected body: %v", err)
	}
	if version.RedactedAt != nil || version.Content == nil {
		t.Fatalf("redact body rejection had side effect: %+v", version)
	}
}

func TestMemoryVersionRedactArchivedStoreRejectedOverHTTP(t *testing.T) {
	env := newAuthTestEnv(t)
	service := memory.NewService(memory.NewPostgreSQLStore(dbconnect.NewClientForTesting(env.runtime)))
	router := env.router(httpapi.WithMemoryHandler(httpapi.NewMemoryHandler(service)))
	store, oldVersionID := createRedactableVersionViaHTTPTestService(t, service, "redact-archived")
	if _, err := service.ArchiveStore(defaultWorkspaceContext(), workspace.DefaultID, store.ID); err != nil {
		t.Fatalf("ArchiveStore: %v", err)
	}

	code, body := performJSONRequest(t, router, http.MethodPost,
		"/v1/memory_stores/"+store.ID+"/memory_versions/"+oldVersionID+"/redact",
		env.envKey,
		"",
	)
	if code != http.StatusBadRequest || !strings.Contains(body, `"type":"invalid_request_error"`) {
		t.Fatalf("archived redact status/body = %d %s; want 400 invalid_request_error", code, body)
	}
	version, err := service.GetMemoryVersion(defaultWorkspaceContext(), workspace.DefaultID, store.ID, oldVersionID, memory.ViewFull)
	if err != nil {
		t.Fatalf("GetMemoryVersion after archived reject: %v", err)
	}
	if version.RedactedAt != nil || version.Content == nil {
		t.Fatalf("archived redact rejection had side effect: %+v", version)
	}
}

func createRedactableVersionViaHTTPTestService(t *testing.T, service *memory.Service, name string) (*memory.Store, string) {
	t.Helper()
	ctx := defaultWorkspaceContext()
	store, err := service.CreateStore(ctx, workspace.DefaultID, memory.CreateStoreRequest{Name: name})
	if err != nil {
		t.Fatalf("CreateStore: %v", err)
	}
	created, err := service.CreateMemory(ctx, workspace.DefaultID, store.ID, memory.CreateMemoryRequest{Path: "/" + name + ".md", Content: "old", ContentSet: true}, memory.Actor{Type: memory.ActorSession, SessionID: "sesn_http_redact"})
	if err != nil {
		t.Fatalf("CreateMemory: %v", err)
	}
	if _, err := service.UpdateMemory(ctx, workspace.DefaultID, store.ID, created.ID, memory.UpdateMemoryRequest{Content: "head", ContentSet: true}, memory.Actor{Type: memory.ActorSession, SessionID: "sesn_http_redact"}); err != nil {
		t.Fatalf("UpdateMemory: %v", err)
	}
	return store, created.MemoryVersionID
}

type panicMemoryService struct{}

func (panicMemoryService) CreateStore(context.Context, workspace.ID, memory.CreateStoreRequest) (*memory.Store, error) {
	panic("memory service should not be called")
}

func (panicMemoryService) GetStore(context.Context, workspace.ID, string) (*memory.Store, error) {
	panic("memory service should not be called")
}

func (panicMemoryService) UpdateStore(context.Context, workspace.ID, string, memory.StorePatch) (*memory.Store, error) {
	panic("memory service should not be called")
}

func (panicMemoryService) ListStores(context.Context, workspace.ID, memory.ListStoresOptions) (memory.StoreListResult, error) {
	panic("memory service should not be called")
}

func (panicMemoryService) ArchiveStore(context.Context, workspace.ID, string) (*memory.Store, error) {
	panic("memory service should not be called")
}

func (panicMemoryService) DeleteStore(context.Context, workspace.ID, string) error {
	panic("memory service should not be called")
}

func (panicMemoryService) CreateMemory(context.Context, workspace.ID, string, memory.CreateMemoryRequest, memory.Actor) (*memory.Memory, error) {
	panic("memory service should not be called")
}

func (panicMemoryService) GetMemory(context.Context, workspace.ID, string, string, string) (*memory.Memory, error) {
	panic("memory service should not be called")
}

func (panicMemoryService) UpdateMemory(context.Context, workspace.ID, string, string, memory.UpdateMemoryRequest, memory.Actor) (*memory.Memory, error) {
	panic("memory service should not be called")
}

func (panicMemoryService) DeleteMemory(context.Context, workspace.ID, string, string, *string, memory.Actor) (*memory.DeleteMemoryResult, error) {
	panic("memory service should not be called")
}

func (panicMemoryService) ListMemories(context.Context, workspace.ID, string, memory.ListMemoriesOptions) (memory.MemoryListResult, error) {
	panic("memory service should not be called")
}

func (panicMemoryService) ListMemoryVersions(context.Context, workspace.ID, string, memory.ListMemoryVersionsOptions) (memory.MemoryVersionListResult, error) {
	panic("memory service should not be called")
}

func (panicMemoryService) GetMemoryVersion(context.Context, workspace.ID, string, string, string) (*memory.MemoryVersion, error) {
	panic("memory service should not be called")
}

func (panicMemoryService) RedactMemoryVersion(context.Context, workspace.ID, string, string, memory.Actor) (*memory.MemoryVersion, error) {
	panic("memory service should not be called")
}

func decodeMemoryJSON(t *testing.T, body string, target any) {
	t.Helper()
	if err := json.Unmarshal([]byte(body), target); err != nil {
		t.Fatalf("decode %T from %s: %v", target, body, err)
	}
}

func authenticatedAPIKeyID(t *testing.T, env *authTestEnv) string {
	t.Helper()
	result, err := env.store.AuthenticateRawKey(defaultWorkspaceContext(), env.envKey)
	if err != nil {
		t.Fatalf("AuthenticateRawKey: %v", err)
	}
	if result.APIKeyID == "" {
		t.Fatal("authenticated API key id is empty")
	}
	return result.APIKeyID
}

func TestMemoryHTTPRedactActorComesFromAuthContext(t *testing.T) {
	env := newAuthTestEnv(t)
	service := memory.NewService(memory.NewPostgreSQLStore(dbconnect.NewClientForTesting(env.runtime)))
	router := env.router(httpapi.WithMemoryHandler(httpapi.NewMemoryHandler(service)))
	apiKeyID := authenticatedAPIKeyID(t, env)
	store, oldVersionID := createRedactableVersionViaHTTPTestService(t, service, "redact-actor")

	code, body := performJSONRequest(t, router, http.MethodPost,
		fmt.Sprintf("/v1/memory_stores/%s/memory_versions/%s/redact", store.ID, oldVersionID),
		env.envKey,
		"",
	)
	if code != http.StatusOK {
		t.Fatalf("redact status/body = %d %s; want 200", code, body)
	}
	var version memory.MemoryVersion
	decodeMemoryJSON(t, body, &version)
	if version.RedactedBy == nil || version.RedactedBy.APIKeyID != apiKeyID {
		t.Fatalf("redacted_by = %+v; want api key id %q from auth context", version.RedactedBy, apiKeyID)
	}
}
