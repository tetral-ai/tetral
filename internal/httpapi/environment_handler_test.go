package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/environment"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/session"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type environmentListQueryTestService struct{}

var _ httpapi.EnvironmentService = environmentListQueryTestService{}

func (s environmentListQueryTestService) Create(context.Context, workspace.ID, environment.CreateEnvironmentRequest) (*environment.Environment, error) {
	panic("Create should not be called")
}

func (s environmentListQueryTestService) Get(context.Context, workspace.ID, string) (*environment.Environment, error) {
	panic("Get should not be called")
}

func (s environmentListQueryTestService) List(context.Context, workspace.ID, environment.ListOptions) (environment.ListResult, error) {
	panic("List should not be called")
}

func (s environmentListQueryTestService) Update(context.Context, workspace.ID, string, environment.EnvironmentPatch) (*environment.Environment, error) {
	panic("Update should not be called")
}

func (s environmentListQueryTestService) Archive(context.Context, workspace.ID, string) (*environment.Environment, error) {
	panic("Archive should not be called")
}

func (s environmentListQueryTestService) Delete(context.Context, workspace.ID, string) (*environment.DeleteResult, error) {
	panic("Delete should not be called")
}

func newEnvironmentTestRouter(t *testing.T) http.Handler {
	t.Helper()
	db := newTestDBFromStorage(t)
	envStore := environment.NewPostgreSQLEnvironmentStore(
		dbconnect.NewClientForTesting(db),
		environment.WithDefaultArtifactRef("artifact_default_test"),
	)
	envHandler := httpapi.NewEnvironmentHandler(environment.NewService(envStore))
	return newAuthenticatedRouter(t, newTestHandler(t), httpapi.WithEnvironmentHandler(envHandler))
}

func newEnvironmentTestRouterAndDB(t *testing.T) (http.Handler, *environment.PostgreSQLEnvironmentStore, *session.PostgreSQLSessionStore) {
	t.Helper()
	db := newTestDBFromStorage(t)
	envStore := environment.NewPostgreSQLEnvironmentStore(
		dbconnect.NewClientForTesting(db),
		environment.WithDefaultArtifactRef("artifact_default_test"),
	)
	sessionStore := session.NewPostgreSQLSessionStore(dbconnect.NewClientForTesting(db))
	envHandler := httpapi.NewEnvironmentHandler(environment.NewService(envStore))
	return newAuthenticatedRouter(t, newTestHandler(t), httpapi.WithEnvironmentHandler(envHandler)), envStore, sessionStore
}

func newEnvironmentListQueryTestRouter() http.Handler {
	envHandler := httpapi.NewEnvironmentHandler(environmentListQueryTestService{})
	authenticator := auth.AuthenticatorFunc(func(context.Context, string) (auth.Principal, error) {
		return auth.Principal{Workspace: workspace.Workspace{ID: workspace.DefaultID}, APIKeyID: "ak_test"}, nil
	})
	return httpapi.NewRouter(nil, "", httpapi.WithAuthenticator(authenticator), httpapi.WithEnvironmentHandler(envHandler))
}

func makeHTTPEnvSession(environmentID string) *session.Session {
	now := time.Now().UTC()
	title := "reference"
	return &session.Session{
		ID:            "sesn_env_reference",
		Type:          "session",
		Title:         &title,
		Status:        session.StatusIdle,
		AgentID:       "agent_env_reference",
		AgentVersion:  1,
		EnvironmentID: environmentID,
		CreatedAt:     now,
		UpdatedAt:     now,
		WorkspaceID:   workspace.DefaultID,
	}
}

func seedHTTPSessionAgentReference(ctx context.Context, sessionStore *session.PostgreSQLSessionStore, environmentID string) error {
	err := sessionStore.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
		if _, err := tx.Exec(ctx,
			`INSERT INTO agents (workspace_id, id, name, version, created_at, updated_at)
			 VALUES ($1, 'agent_env_reference', 'agent env reference', 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
			 ON CONFLICT (id) DO NOTHING`,
			string(workspace.DefaultID),
		); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at)
			 VALUES ($1, 'agv_env_reference', 'agent_env_reference', 1, '{}', 'hash', '2026-01-01T00:00:00Z')
			 ON CONFLICT (agent_id, version) DO NOTHING`,
			string(workspace.DefaultID),
		); err != nil {
			return err
		}
		return tx.CreateSession(ctx, makeHTTPEnvSession(environmentID))
	})
	return err
}

func TestListEnvironmentsRejectsMalformedRawQuerySyntax(t *testing.T) {
	router := newEnvironmentListQueryTestRouter()
	for _, path := range []string{
		"/v1/environments?limit=%zz",
		"/v1/environments?limit=1;page=abc",
	} {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, path, nil)
		setAuthHeader(request)
		router.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("%s: expected 400, got %d: %s", path, recorder.Code, recorder.Body.String())
		}
		assertErrorType(t, recorder, "invalid_request_error")
	}
}

func TestCreateEnvironmentValid(t *testing.T) {
	router := newEnvironmentTestRouter(t)

	body := `{"name":"production","config":{"networking":{"type":"cidr_allow_list","network_allow_list":"10.0.0.0/8, 2001:db8::/32"}},"metadata":{"team":"infra"}}`
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/environments", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	setAuthHeader(request)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", recorder.Code, recorder.Body.String())
	}

	var response map[string]any
	_ = json.Unmarshal(recorder.Body.Bytes(), &response)
	id, _ := response["id"].(string)
	if !strings.HasPrefix(id, "env_") {
		t.Errorf("expected env_ prefix, got %q", id)
	}
	assertEnvironmentResponseShape(t, response)
	config := response["config"].(map[string]any)
	networking := config["networking"].(map[string]any)
	if networking["type"] != "cidr_allow_list" {
		t.Errorf("networking.type = %v; want cidr_allow_list", networking["type"])
	}
	if networking["network_allow_list"] != "10.0.0.0/8,2001:db8::/32" {
		t.Errorf("network_allow_list = %v; want normalized CIDR list", networking["network_allow_list"])
	}
	metadata := response["metadata"].(map[string]any)
	if metadata["team"] != "infra" {
		t.Errorf("metadata = %v; want team=infra", metadata)
	}
}

func TestCreateEnvironmentRejectsUnknownAndOldShapeFields(t *testing.T) {
	router := newEnvironmentTestRouter(t)
	for _, body := range []string{
		`{"name":"production","version":1}`,
		`{"name":"production","config":{"packages":[{"manager":"pip","packages":["pandas"]}]}}`,
		`{"name":"production","config":{"networking":{"type":"limited","allowed_hosts":["api.example.com"]}}}`,
		`{"name":"production","config":{"networking":{"type":"unrestricted","allowed_hosts":["https://api.example.com"]}}}`,
	} {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/v1/environments", strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		setAuthHeader(request)
		router.ServeHTTP(recorder, request)

		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("body %s: expected 400, got %d: %s", body, recorder.Code, recorder.Body.String())
		}
	}
}

func TestEnvironmentRejectsNetworkAllowListOutsideCIDRModeBeforePersistence(t *testing.T) {
	router := newEnvironmentListQueryTestRouter()
	for _, tc := range []struct {
		method string
		path   string
		body   string
	}{
		{
			method: http.MethodPost,
			path:   "/v1/environments",
			body:   `{"name":"production","config":{"networking":{"type":"unrestricted","network_allow_list":"10.0.0.0/8"}}}`,
		},
		{
			method: http.MethodPost,
			path:   "/v1/environments/env_test",
			body:   `{"config":{"networking":{"type":"blocked","network_allow_list":"10.0.0.0/8"}}}`,
		},
	} {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		request.Header.Set("Content-Type", "application/json")
		setAuthHeader(request)
		router.ServeHTTP(recorder, request)

		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("%s %s: expected 400, got %d: %s", tc.method, tc.path, recorder.Code, recorder.Body.String())
		}
		assertErrorType(t, recorder, "invalid_request_error")
	}
}

func TestCreateEnvironmentDuplicateName(t *testing.T) {
	router := newEnvironmentTestRouter(t)

	body := `{"name":"prod","config":{}}`
	for i := 0; i < 2; i++ {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/v1/environments", strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		setAuthHeader(request)
		router.ServeHTTP(recorder, request)

		if i == 1 && recorder.Code != http.StatusConflict {
			t.Fatalf("expected 409 for duplicate name, got %d: %s", recorder.Code, recorder.Body.String())
		}
	}
}

func TestGetEnvironmentExisting(t *testing.T) {
	router := newEnvironmentTestRouter(t)

	createBody := `{"name":"test","config":{"networking":{"type":"unrestricted"}}}`
	createRec := httptest.NewRecorder()
	createReq := httptest.NewRequest(http.MethodPost, "/v1/environments", strings.NewReader(createBody))
	createReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(createReq)
	router.ServeHTTP(createRec, createReq)

	var created map[string]any
	_ = json.Unmarshal(createRec.Body.Bytes(), &created)
	envID := created["id"].(string)

	getRec := httptest.NewRecorder()
	getReq := httptest.NewRequest(http.MethodGet, "/v1/environments/"+envID, nil)
	setAuthHeader(getReq)
	router.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", getRec.Code)
	}
	var response map[string]any
	_ = json.Unmarshal(getRec.Body.Bytes(), &response)
	assertEnvironmentResponseShape(t, response)
}

func TestGetEnvironmentNonexistent(t *testing.T) {
	router := newEnvironmentTestRouter(t)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/environments/env_fake", nil)
	setAuthHeader(request)
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", recorder.Code)
	}
}

func TestListEnvironments(t *testing.T) {
	router := newEnvironmentTestRouter(t)

	for i := 0; i < 2; i++ {
		body := `{"name":"env` + string(rune('a'+i)) + `","config":{}}`
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/environments", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		setAuthHeader(req)
		router.ServeHTTP(rec, req)
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/environments", nil)
	setAuthHeader(request)
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", recorder.Code)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(recorder.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode raw list response: %v", err)
	}
	if _, ok := raw["has_more"]; ok {
		t.Fatalf("Environment list exposed has_more: %s", recorder.Body.String())
	}
	if _, ok := raw["next_page"]; !ok {
		t.Fatalf("Environment list missing next_page: %s", recorder.Body.String())
	}
	if string(raw["next_page"]) != "null" {
		t.Fatalf("next_page = %s; want null", raw["next_page"])
	}
	var response struct {
		Data     []any   `json:"data"`
		NextPage *string `json:"next_page"`
	}
	_ = json.Unmarshal(recorder.Body.Bytes(), &response)
	if len(response.Data) != 2 {
		t.Errorf("expected 2 environments, got %d", len(response.Data))
	}
}

func TestListEnvironmentsLimitDefaultAndCap(t *testing.T) {
	router, envStore, _ := newEnvironmentTestRouterAndDB(t)
	ctx := defaultWorkspaceContext()
	for i := 0; i < 101; i++ {
		if _, err := envStore.Create(ctx, workspace.DefaultID, environment.CreateEnvironmentRequest{Name: "bulk-" + string(rune('a'+i/26)) + string(rune('a'+i%26))}); err != nil {
			t.Fatalf("Create bulk env %d: %v", i, err)
		}
	}

	defaultRec := httptest.NewRecorder()
	defaultReq := httptest.NewRequest(http.MethodGet, "/v1/environments", nil)
	setAuthHeader(defaultReq)
	router.ServeHTTP(defaultRec, defaultReq)
	if defaultRec.Code != http.StatusOK {
		t.Fatalf("default list expected 200, got %d: %s", defaultRec.Code, defaultRec.Body.String())
	}
	var defaultResponse struct {
		Data     []any   `json:"data"`
		NextPage *string `json:"next_page"`
	}
	if err := json.Unmarshal(defaultRec.Body.Bytes(), &defaultResponse); err != nil {
		t.Fatalf("decode default list: %v", err)
	}
	if len(defaultResponse.Data) != 20 {
		t.Fatalf("default limit returned %d rows; want 20", len(defaultResponse.Data))
	}
	if defaultResponse.NextPage == nil {
		t.Fatal("default list next_page is nil; want token")
	}

	cappedRec := httptest.NewRecorder()
	cappedReq := httptest.NewRequest(http.MethodGet, "/v1/environments?limit=101", nil)
	setAuthHeader(cappedReq)
	router.ServeHTTP(cappedRec, cappedReq)
	if cappedRec.Code != http.StatusOK {
		t.Fatalf("capped list expected 200, got %d: %s", cappedRec.Code, cappedRec.Body.String())
	}
	var cappedResponse struct {
		Data     []any   `json:"data"`
		NextPage *string `json:"next_page"`
	}
	if err := json.Unmarshal(cappedRec.Body.Bytes(), &cappedResponse); err != nil {
		t.Fatalf("decode capped list: %v", err)
	}
	if len(cappedResponse.Data) != 100 {
		t.Fatalf("limit cap returned %d rows; want 100", len(cappedResponse.Data))
	}
	if cappedResponse.NextPage == nil {
		t.Fatal("capped list next_page is nil; want token")
	}
}

func TestListEnvironmentsRejectsLegacyAndMalformedQueryParams(t *testing.T) {
	router := newEnvironmentTestRouter(t)
	for _, path := range []string{
		"/v1/environments?after_id=env_old",
		"/v1/environments?before_id=env_old",
		"/v1/environments?cursor=env_old",
		"/v1/environments?limit=",
		"/v1/environments?limit=abc",
		"/v1/environments?limit=0",
		"/v1/environments?limit=%zz",
		"/v1/environments?limit=1;page=abc",
		"/v1/environments?include_archived=",
		"/v1/environments?include_archived=1",
		"/v1/environments?page=",
		"/v1/environments?page=not-a-token",
	} {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, path, nil)
		setAuthHeader(request)
		router.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("%s: expected 400, got %d: %s", path, recorder.Code, recorder.Body.String())
		}
		assertErrorType(t, recorder, "invalid_request_error")
	}
}

func TestListEnvironmentsIncludesArchivedOnlyWhenRequested(t *testing.T) {
	router, envStore, _ := newEnvironmentTestRouterAndDB(t)
	ctx := defaultWorkspaceContext()
	live, err := envStore.Create(ctx, workspace.DefaultID, environment.CreateEnvironmentRequest{Name: "http-live"})
	if err != nil {
		t.Fatalf("Create live: %v", err)
	}
	archived, err := envStore.Create(ctx, workspace.DefaultID, environment.CreateEnvironmentRequest{Name: "http-archived"})
	if err != nil {
		t.Fatalf("Create archived: %v", err)
	}
	if _, err := envStore.Archive(ctx, workspace.DefaultID, archived.ID); err != nil {
		t.Fatalf("Archive: %v", err)
	}

	defaultRec := httptest.NewRecorder()
	defaultReq := httptest.NewRequest(http.MethodGet, "/v1/environments", nil)
	setAuthHeader(defaultReq)
	router.ServeHTTP(defaultRec, defaultReq)
	if defaultRec.Code != http.StatusOK {
		t.Fatalf("default list expected 200, got %d: %s", defaultRec.Code, defaultRec.Body.String())
	}
	defaultIDs := decodeEnvironmentListIDs(t, defaultRec.Body.Bytes())
	if len(defaultIDs) != 1 || defaultIDs[0] != live.ID {
		t.Fatalf("default list IDs = %v; want only %s", defaultIDs, live.ID)
	}

	includeRec := httptest.NewRecorder()
	includeReq := httptest.NewRequest(http.MethodGet, "/v1/environments?include_archived=true", nil)
	setAuthHeader(includeReq)
	router.ServeHTTP(includeRec, includeReq)
	if includeRec.Code != http.StatusOK {
		t.Fatalf("include_archived list expected 200, got %d: %s", includeRec.Code, includeRec.Body.String())
	}
	includeIDs := decodeEnvironmentListIDs(t, includeRec.Body.Bytes())
	if len(includeIDs) != 2 {
		t.Fatalf("include_archived list IDs = %v; want 2 rows", includeIDs)
	}
}

func TestListEnvironmentsPaginatesWithOpaqueNextPage(t *testing.T) {
	router, envStore, _ := newEnvironmentTestRouterAndDB(t)
	ctx := defaultWorkspaceContext()
	for i := 0; i < 3; i++ {
		if _, err := envStore.Create(ctx, workspace.DefaultID, environment.CreateEnvironmentRequest{Name: "http-page-" + string(rune('a'+i))}); err != nil {
			t.Fatalf("Create page env %d: %v", i, err)
		}
	}

	firstRec := httptest.NewRecorder()
	firstReq := httptest.NewRequest(http.MethodGet, "/v1/environments?limit=2", nil)
	setAuthHeader(firstReq)
	router.ServeHTTP(firstRec, firstReq)
	if firstRec.Code != http.StatusOK {
		t.Fatalf("first page expected 200, got %d: %s", firstRec.Code, firstRec.Body.String())
	}
	var first struct {
		Data     []map[string]any `json:"data"`
		NextPage *string          `json:"next_page"`
	}
	if err := json.Unmarshal(firstRec.Body.Bytes(), &first); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if len(first.Data) != 2 || first.NextPage == nil {
		t.Fatalf("first page data=%d next=%v; want 2 rows and token", len(first.Data), first.NextPage)
	}

	secondRec := httptest.NewRecorder()
	secondReq := httptest.NewRequest(http.MethodGet, "/v1/environments?limit=2&page="+*first.NextPage, nil)
	setAuthHeader(secondReq)
	router.ServeHTTP(secondRec, secondReq)
	if secondRec.Code != http.StatusOK {
		t.Fatalf("second page expected 200, got %d: %s", secondRec.Code, secondRec.Body.String())
	}
	var second struct {
		Data     []map[string]any `json:"data"`
		NextPage *string          `json:"next_page"`
	}
	if err := json.Unmarshal(secondRec.Body.Bytes(), &second); err != nil {
		t.Fatalf("decode second page: %v", err)
	}
	if len(second.Data) != 1 {
		t.Fatalf("second page data=%d; want 1", len(second.Data))
	}
	for _, left := range first.Data {
		for _, right := range second.Data {
			if left["id"] == right["id"] {
				t.Fatalf("environment %v appeared on both pages", left["id"])
			}
		}
	}
	if second.NextPage != nil {
		t.Fatalf("second page next_page=%q; want nil", *second.NextPage)
	}
}

func TestUpdateEnvironmentValid(t *testing.T) {
	router := newEnvironmentTestRouter(t)

	createBody := `{"name":"test","config":{}}`
	createRec := httptest.NewRecorder()
	createReq := httptest.NewRequest(http.MethodPost, "/v1/environments", strings.NewReader(createBody))
	createReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(createReq)
	router.ServeHTTP(createRec, createReq)

	var created map[string]any
	_ = json.Unmarshal(createRec.Body.Bytes(), &created)
	envID := created["id"].(string)

	updateBody := `{"name":"test-updated","config":{"networking":{"type":"cidr_allow_list","network_allow_list":"10.0.0.0/8"}},"metadata":{"owner":"runtime"}}`
	updateRec := httptest.NewRecorder()
	updateReq := httptest.NewRequest(http.MethodPost, "/v1/environments/"+envID, strings.NewReader(updateBody))
	updateReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(updateReq)
	router.ServeHTTP(updateRec, updateReq)

	if updateRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", updateRec.Code, updateRec.Body.String())
	}

	var result map[string]any
	_ = json.Unmarshal(updateRec.Body.Bytes(), &result)
	assertEnvironmentResponseShape(t, result)
	if result["name"] != "test-updated" {
		t.Errorf("name = %v; want test-updated", result["name"])
	}
	config := result["config"].(map[string]any)
	networking := config["networking"].(map[string]any)
	if networking["type"] != "cidr_allow_list" {
		t.Errorf("networking.type = %v; want cidr_allow_list", networking["type"])
	}
	if networking["network_allow_list"] != "10.0.0.0/8" {
		t.Errorf("network_allow_list = %v; want 10.0.0.0/8", networking["network_allow_list"])
	}
	metadata := result["metadata"].(map[string]any)
	if metadata["owner"] != "runtime" {
		t.Errorf("metadata = %v; want owner=runtime", metadata)
	}
}

func TestUpdateEnvironmentRejectsVersionAndUnknownFields(t *testing.T) {
	router := newEnvironmentTestRouter(t)

	createBody := `{"name":"test","config":{}}`
	createRec := httptest.NewRecorder()
	createReq := httptest.NewRequest(http.MethodPost, "/v1/environments", strings.NewReader(createBody))
	createReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(createReq)
	router.ServeHTTP(createRec, createReq)

	var created map[string]any
	_ = json.Unmarshal(createRec.Body.Bytes(), &created)
	envID := created["id"].(string)

	for _, body := range []string{
		`{"version":1}`,
		`{"unknown":true}`,
		`{"config":{"networking":{"type":"private"}}}`,
	} {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/v1/environments/"+envID, strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		setAuthHeader(request)
		router.ServeHTTP(recorder, request)

		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("body %s: expected 400, got %d: %s", body, recorder.Code, recorder.Body.String())
		}
	}
}

func TestUpdateEnvironmentRejectsNullBody(t *testing.T) {
	router := newEnvironmentListQueryTestRouter()

	updateRec := httptest.NewRecorder()
	updateReq := httptest.NewRequest(http.MethodPost, "/v1/environments/env_test", strings.NewReader(`null`))
	updateReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(updateReq)
	router.ServeHTTP(updateRec, updateReq)

	if updateRec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", updateRec.Code, updateRec.Body.String())
	}
	assertErrorType(t, updateRec, "invalid_request_error")
}

func TestArchiveEnvironmentReturnsArchivedShapeAndBlocksUpdate(t *testing.T) {
	router := newEnvironmentTestRouter(t)

	createBody := `{"name":"archive","config":{}}`
	createRec := httptest.NewRecorder()
	createReq := httptest.NewRequest(http.MethodPost, "/v1/environments", strings.NewReader(createBody))
	createReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(createReq)
	router.ServeHTTP(createRec, createReq)

	var created map[string]any
	_ = json.Unmarshal(createRec.Body.Bytes(), &created)
	envID := created["id"].(string)

	archiveRec := httptest.NewRecorder()
	archiveReq := httptest.NewRequest(http.MethodPost, "/v1/environments/"+envID+"/archive", nil)
	setAuthHeader(archiveReq)
	router.ServeHTTP(archiveRec, archiveReq)
	if archiveRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", archiveRec.Code, archiveRec.Body.String())
	}
	var archived map[string]any
	_ = json.Unmarshal(archiveRec.Body.Bytes(), &archived)
	if _, ok := archived["version"]; ok {
		t.Fatalf("archive response exposed version: %v", archived)
	}
	if archived["archived_at"] == nil {
		t.Fatalf("archived_at = nil; want timestamp in %v", archived)
	}
	config := archived["config"].(map[string]any)
	if config["type"] != "cloud" {
		t.Errorf("config.type = %v; want cloud", config["type"])
	}
	if _, ok := config["packages"].(map[string]any); !ok {
		t.Fatalf("config.packages = %T; want object", config["packages"])
	}
	if _, ok := archived["metadata"].(map[string]any); !ok {
		t.Fatalf("metadata = %T; want object", archived["metadata"])
	}

	updateRec := httptest.NewRecorder()
	updateReq := httptest.NewRequest(http.MethodPost, "/v1/environments/"+envID, strings.NewReader(`{"name":"blocked"}`))
	updateReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(updateReq)
	router.ServeHTTP(updateRec, updateReq)
	if updateRec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 updating archived environment, got %d: %s", updateRec.Code, updateRec.Body.String())
	}
	assertErrorType(t, updateRec, "invalid_request_error")
}

func TestDeleteEnvironmentReturnsDeletedObject(t *testing.T) {
	router := newEnvironmentTestRouter(t)

	createBody := `{"name":"del","config":{}}`
	createRec := httptest.NewRecorder()
	createReq := httptest.NewRequest(http.MethodPost, "/v1/environments", strings.NewReader(createBody))
	createReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(createReq)
	router.ServeHTTP(createRec, createReq)

	var created map[string]any
	_ = json.Unmarshal(createRec.Body.Bytes(), &created)
	envID := created["id"].(string)

	delRec := httptest.NewRecorder()
	delReq := httptest.NewRequest(http.MethodDelete, "/v1/environments/"+envID, nil)
	setAuthHeader(delReq)
	router.ServeHTTP(delRec, delReq)

	if delRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", delRec.Code, delRec.Body.String())
	}
	var deleted map[string]any
	if err := json.Unmarshal(delRec.Body.Bytes(), &deleted); err != nil {
		t.Fatalf("decode delete response: %v", err)
	}
	if deleted["id"] != envID || deleted["type"] != "environment_deleted" {
		t.Fatalf("delete response = %v; want id/type environment_deleted", deleted)
	}

	getRec := httptest.NewRecorder()
	getReq := httptest.NewRequest(http.MethodGet, "/v1/environments/"+envID, nil)
	setAuthHeader(getReq)
	router.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", getRec.Code)
	}
}

func TestDeleteEnvironmentRejectsReferencedEnvironment(t *testing.T) {
	router, envStore, sessionStore := newEnvironmentTestRouterAndDB(t)
	ctx := defaultWorkspaceContext()
	created, err := envStore.Create(ctx, workspace.DefaultID, environment.CreateEnvironmentRequest{Name: "referenced"})
	if err != nil {
		t.Fatalf("Create environment: %v", err)
	}
	if err := seedHTTPSessionAgentReference(ctx, sessionStore, created.ID); err != nil {
		t.Fatalf("Create session reference: %v", err)
	}

	delRec := httptest.NewRecorder()
	delReq := httptest.NewRequest(http.MethodDelete, "/v1/environments/"+created.ID, nil)
	setAuthHeader(delReq)
	router.ServeHTTP(delRec, delReq)

	if delRec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", delRec.Code, delRec.Body.String())
	}
	assertErrorType(t, delRec, "invalid_request_error")
}

func TestDeleteEnvironmentRejectsDurableSessionReferences(t *testing.T) {
	router, envStore, sessionStore := newEnvironmentTestRouterAndDB(t)
	ctx := defaultWorkspaceContext()
	created, err := envStore.Create(ctx, workspace.DefaultID, environment.CreateEnvironmentRequest{Name: "referenced"})
	if err != nil {
		t.Fatalf("Create environment: %v", err)
	}
	if err := seedHTTPSessionAgentReference(ctx, sessionStore, created.ID); err != nil {
		t.Fatalf("Create session reference: %v", err)
	}

	delRec := httptest.NewRecorder()
	delReq := httptest.NewRequest(http.MethodDelete, "/v1/environments/"+created.ID, nil)
	setAuthHeader(delReq)
	router.ServeHTTP(delRec, delReq)

	if delRec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", delRec.Code, delRec.Body.String())
	}
	assertErrorType(t, delRec, "invalid_request_error")
	if _, err := envStore.Get(ctx, workspace.DefaultID, created.ID); err != nil {
		t.Fatalf("referenced environment should remain after rejected delete: %v", err)
	}
}

func TestEnvironmentArchiveStays501(t *testing.T) {
	router := newAuthenticatedRouter(t, newTestHandler(t))

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/environments/env_test/archive", nil)
	setAuthHeader(request)
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501, got %d", recorder.Code)
	}
}

func assertEnvironmentResponseShape(t *testing.T, response map[string]any) {
	t.Helper()
	if _, ok := response["version"]; ok {
		t.Fatalf("response exposed version: %v", response)
	}
	if _, ok := response["archived_at"]; !ok {
		t.Fatalf("response missing archived_at: %v", response)
	}
	if response["archived_at"] != nil {
		t.Errorf("archived_at = %v; want null", response["archived_at"])
	}
	config, ok := response["config"].(map[string]any)
	if !ok {
		t.Fatalf("config = %T %v; want object", response["config"], response["config"])
	}
	if config["type"] != "cloud" {
		t.Errorf("config.type = %v; want cloud", config["type"])
	}
	if _, ok := config["networking"].(map[string]any); !ok {
		t.Fatalf("config.networking = %T %v; want object", config["networking"], config["networking"])
	}
	packages, ok := config["packages"].(map[string]any)
	if !ok {
		t.Fatalf("config.packages = %T %v; want object", config["packages"], config["packages"])
	}
	if packages == nil {
		t.Fatal("config.packages decoded nil; want object")
	}
	metadata, ok := response["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("metadata = %T %v; want object", response["metadata"], response["metadata"])
	}
	if metadata == nil {
		t.Fatal("metadata decoded nil; want object")
	}
}

func decodeEnvironmentListIDs(t *testing.T, body []byte) []string {
	t.Helper()
	var response struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("decode environment list: %v", err)
	}
	ids := make([]string, 0, len(response.Data))
	for _, item := range response.Data {
		id, _ := item["id"].(string)
		ids = append(ids, id)
	}
	return ids
}
