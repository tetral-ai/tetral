package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/session"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func newSessionHTTPTestRouter() http.Handler {
	authenticator := auth.AuthenticatorFunc(func(context.Context, string) (auth.Principal, error) {
		return auth.Principal{Workspace: workspace.Workspace{ID: workspace.DefaultID}, APIKeyID: "ak_test"}, nil
	})
	return httpapi.NewRouter(httpapi.NewSessionHandler(fakeSessionService{}), "", httpapi.WithAuthenticator(authenticator))
}

func newSessionHTTPTestRouterWithListService(service *recordingSessionListService) http.Handler {
	authenticator := auth.AuthenticatorFunc(func(context.Context, string) (auth.Principal, error) {
		return auth.Principal{Workspace: workspace.Workspace{ID: workspace.DefaultID}, APIKeyID: "ak_test"}, nil
	})
	return httpapi.NewRouter(httpapi.NewSessionHandler(service), "", httpapi.WithAuthenticator(authenticator))
}

func newSessionHTTPTestRouterWithCreateService(service *recordingCreateSessionService) http.Handler {
	authenticator := auth.AuthenticatorFunc(func(context.Context, string) (auth.Principal, error) {
		return auth.Principal{Workspace: workspace.Workspace{ID: workspace.DefaultID}, APIKeyID: "ak_test"}, nil
	})
	return httpapi.NewRouter(httpapi.NewSessionHandler(service), "", httpapi.WithAuthenticator(authenticator))
}

func newSessionHTTPTestRouterWithMutationService(service *recordingSessionMutationService) http.Handler {
	authenticator := auth.AuthenticatorFunc(func(context.Context, string) (auth.Principal, error) {
		return auth.Principal{Workspace: workspace.Workspace{ID: workspace.DefaultID}, APIKeyID: "ak_test"}, nil
	})
	return httpapi.NewRouter(httpapi.NewSessionHandler(service), "", httpapi.WithAuthenticator(authenticator))
}

func newSessionHTTPTestRouterWithThreadService(service *recordingThreadService) http.Handler {
	authenticator := auth.AuthenticatorFunc(func(context.Context, string) (auth.Principal, error) {
		return auth.Principal{Workspace: workspace.Workspace{ID: workspace.DefaultID}, APIKeyID: "ak_test"}, nil
	})
	return httpapi.NewRouter(httpapi.NewSessionHandler(service), "", httpapi.WithAuthenticator(authenticator))
}

func TestCreateSessionRejectsOversizedBody(t *testing.T) {
	router := newSessionHTTPTestRouter()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions?beta=true", strings.NewReader(strings.Repeat("x", (1<<20)+1)))
	request.Header.Set("Content-Type", "application/json")
	setAuthHeader(request)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d; want 413 body=%s", recorder.Code, recorder.Body.String())
	}
	assertErrorType(t, recorder, "request_too_large")
}

func TestCreateSessionSandboxFailureReturnsSafeAPIErrorWithRequestID(t *testing.T) {
	service := &recordingCreateSessionService{
		err: &sandbox.SandboxError{
			Code:    sandbox.SandboxErrorCreateFailed,
			Message: "sandbox startup failed",
			Cause:   errors.New("cloudflare provider sandbox psbox_123 returned raw body with access_token=secret from https://provider.example.invalid/tmp"),
		},
	}
	router := newSessionHTTPTestRouterWithCreateService(service)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions?beta=true", strings.NewReader(`{"agent":"agent_router","environment_id":"env_router","vault_ids":[]}`))
	request.Header.Set("Content-Type", "application/json")
	setAuthHeader(request)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status = %d; want 502 body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Type  string `json:"type"`
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
		RequestID string `json:"request_id"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode error response: %v body=%s", err, recorder.Body.String())
	}
	if response.Type != "error" || response.Error.Type != "api_error" || response.Error.Message != "session sandbox setup failed" || response.RequestID == "" {
		t.Fatalf("response = %+v; want safe api_error with request_id", response)
	}
	for _, forbidden := range []string{
		"cloudflare",
		"psbox_123",
		"raw body",
		"access_token",
		"secret",
		"https://",
		"/tmp",
		"create_failed",
	} {
		if strings.Contains(recorder.Body.String(), forbidden) {
			t.Fatalf("response leaked %q in body %s", forbidden, recorder.Body.String())
		}
	}
}

func TestSessionWriteHandlersRejectOversizedBodiesBeforeService(t *testing.T) {
	tests := []struct {
		name  string
		path  string
		calls func(*recordingSessionMutationService) int
	}{
		{
			name: "update session",
			path: "/v1/sessions/sesn_test?beta=true",
			calls: func(service *recordingSessionMutationService) int {
				return service.updateCalls
			},
		},
		{
			name: "add resource",
			path: "/v1/sessions/sesn_test/resources?beta=true",
			calls: func(service *recordingSessionMutationService) int {
				return service.addResourceCalls
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &recordingSessionMutationService{}
			router := newSessionHTTPTestRouterWithMutationService(service)
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, test.path, strings.NewReader(strings.Repeat("x", (1<<20)+1)))
			request.Header.Set("Content-Type", "application/json")
			setAuthHeader(request)

			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusRequestEntityTooLarge {
				t.Fatalf("status = %d; want 413 body=%s", recorder.Code, recorder.Body.String())
			}
			assertErrorType(t, recorder, "request_too_large")
			if test.calls(service) != 0 {
				t.Fatalf("service calls = %d; want 0", test.calls(service))
			}
		})
	}
}

func TestListSessionsRejectsLegacyIDCursorParameters(t *testing.T) {
	router := newSessionHTTPTestRouter()
	for _, path := range []string{"/v1/sessions?after_id=sesn_old", "/v1/sessions?before_id=sesn_old"} {
		t.Run(path, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, path, nil)
			setAuthHeader(request)

			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d; want 400 body=%s", recorder.Code, recorder.Body.String())
			}
			assertErrorType(t, recorder, "invalid_request_error")
		})
	}
}

func TestGetSessionResponseReturnsNullAgentMultiagent(t *testing.T) {
	router := newSessionHTTPTestRouter()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/sessions/sesn_router?beta=true", nil)
	setAuthHeader(request)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Agent map[string]any `json:"agent"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode session response: %v body=%s", err, recorder.Body.String())
	}
	value, ok := response.Agent["multiagent"]
	if !ok || value != nil {
		t.Fatalf("agent.multiagent = %v present=%v; want null in body %s", value, ok, recorder.Body.String())
	}
}

func TestUpdateResourceRouteRejectsAnythingExceptOneNonEmptyStringToken(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
	}{
		{name: "missing", body: `{}`},
		{name: "null", body: `{"authorization_token":null}`},
		{name: "number", body: `{"authorization_token":7}`},
		{name: "empty", body: `{"authorization_token":""}`},
		{name: "extra", body: `{"authorization_token":"token","other":true}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := &recordingSessionMutationService{}
			router := newSessionHTTPTestRouterWithMutationService(service)
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(
				http.MethodPost,
				"/v1/sessions/sesn_test/resources/sesrsc_test?beta=true",
				strings.NewReader(test.body),
			)
			request.Header.Set("Content-Type", "application/json")
			setAuthHeader(request)

			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d; want 400 body=%s", recorder.Code, recorder.Body.String())
			}
			assertErrorType(t, recorder, "invalid_request_error")
			if service.updateResourceCalls != 0 {
				t.Fatalf("UpdateResource calls = %d; want zero", service.updateResourceCalls)
			}
		})
	}
}

func TestListSessionsDefaultsToDescendingOrderAndAcceptsAscending(t *testing.T) {
	service := &recordingSessionListService{}
	router := newSessionHTTPTestRouterWithListService(service)

	defaultRecorder := httptest.NewRecorder()
	defaultRequest := httptest.NewRequest(http.MethodGet, "/v1/sessions?beta=true", nil)
	setAuthHeader(defaultRequest)
	router.ServeHTTP(defaultRecorder, defaultRequest)
	if defaultRecorder.Code != http.StatusOK {
		t.Fatalf("default order status = %d; want 200 body=%s", defaultRecorder.Code, defaultRecorder.Body.String())
	}
	if len(service.options) != 1 || service.options[0].Order != session.ListOrderDescending {
		t.Fatalf("default order options = %#v; want desc", service.options)
	}

	ascendingRecorder := httptest.NewRecorder()
	ascendingRequest := httptest.NewRequest(http.MethodGet, "/v1/sessions?beta=true&order=asc", nil)
	setAuthHeader(ascendingRequest)
	router.ServeHTTP(ascendingRecorder, ascendingRequest)
	if ascendingRecorder.Code != http.StatusOK {
		t.Fatalf("ascending order status = %d; want 200 body=%s", ascendingRecorder.Code, ascendingRecorder.Body.String())
	}
	if len(service.options) != 2 || service.options[1].Order != session.ListOrderAscending {
		t.Fatalf("ascending order options = %#v; want asc", service.options)
	}
}

func TestListSessionsParsesAllSupportedPublicFilters(t *testing.T) {
	service := &recordingSessionListService{}
	router := newSessionHTTPTestRouterWithListService(service)
	createdAtGT := time.Date(2026, 5, 11, 10, 0, 0, 0, time.UTC)
	createdAtGTE := time.Date(2026, 5, 11, 10, 30, 0, 0, time.UTC)
	createdAtLT := time.Date(2026, 5, 11, 13, 0, 0, 0, time.UTC)
	createdAtLTE := time.Date(2026, 5, 11, 13, 30, 0, 0, time.UTC)
	values := url.Values{}
	values.Set("limit", "7")
	values.Set("page", "signed-session-page-token")
	values.Set("include_archived", "true")
	values.Set("agent_id", "agent_http_session")
	values.Set("agent_version", "2")
	values.Set("memory_store_id", "memstore_http_session")
	values.Set("deployment_id", "deployment_unsupported")
	values.Add("statuses[]", "running")
	values.Add("statuses[]", "idle")
	values.Set("created_at[gt]", createdAtGT.Format(time.RFC3339))
	values.Set("created_at[gte]", createdAtGTE.Format(time.RFC3339))
	values.Set("created_at[lt]", createdAtLT.Format(time.RFC3339))
	values.Set("created_at[lte]", createdAtLTE.Format(time.RFC3339))
	values.Set("order", "asc")
	values.Set("beta", "true")

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/sessions?"+values.Encode(), nil)
	setAuthHeader(request)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 body=%s", recorder.Code, recorder.Body.String())
	}
	if len(service.options) != 1 {
		t.Fatalf("List calls = %d; want 1", len(service.options))
	}
	got := service.options[0]
	if got.Limit != 7 {
		t.Fatalf("limit = %d; want 7", got.Limit)
	}
	if got.Page != "signed-session-page-token" {
		t.Fatalf("page = %q; want signed-session-page-token", got.Page)
	}
	if !got.IncludeArchived {
		t.Fatal("include_archived = false; want true")
	}
	if got.AgentID != "agent_http_session" {
		t.Fatalf("agent_id = %q; want agent_http_session", got.AgentID)
	}
	if got.AgentVersion != 2 {
		t.Fatalf("agent_version = %d; want 2", got.AgentVersion)
	}
	if got.MemoryStoreID != "memstore_http_session" {
		t.Fatalf("memory_store_id = %q; want memstore_http_session", got.MemoryStoreID)
	}
	if got.DeploymentID != "deployment_unsupported" {
		t.Fatalf("deployment_id = %q; want deployment_unsupported", got.DeploymentID)
	}
	if !reflect.DeepEqual(got.Statuses, []session.Status{session.StatusRunning, session.StatusIdle}) {
		t.Fatalf("statuses = %v; want running,idle in canonical order", got.Statuses)
	}
	if got.CreatedAtGT == nil || !got.CreatedAtGT.Equal(createdAtGT) {
		t.Fatalf("created_at[gt] = %v; want %s", got.CreatedAtGT, createdAtGT.Format(time.RFC3339))
	}
	if got.CreatedAtGTE == nil || !got.CreatedAtGTE.Equal(createdAtGTE) {
		t.Fatalf("created_at[gte] = %v; want %s", got.CreatedAtGTE, createdAtGTE.Format(time.RFC3339))
	}
	if got.CreatedAtLT == nil || !got.CreatedAtLT.Equal(createdAtLT) {
		t.Fatalf("created_at[lt] = %v; want %s", got.CreatedAtLT, createdAtLT.Format(time.RFC3339))
	}
	if got.CreatedAtLTE == nil || !got.CreatedAtLTE.Equal(createdAtLTE) {
		t.Fatalf("created_at[lte] = %v; want %s", got.CreatedAtLTE, createdAtLTE.Format(time.RFC3339))
	}
	if got.Order != session.ListOrderAscending {
		t.Fatalf("order = %q; want asc", got.Order)
	}
}

func TestSessionThreadRoutesParseSDKQueriesAndArchiveBody(t *testing.T) {
	service := &recordingThreadService{}
	router := newSessionHTTPTestRouterWithThreadService(service)

	listRecorder := httptest.NewRecorder()
	listRequest := httptest.NewRequest(http.MethodGet, "/v1/sessions/sesn_http_threads/threads?limit=7&page=signed-thread-page&beta=true", nil)
	setAuthHeader(listRequest)
	router.ServeHTTP(listRecorder, listRequest)
	if listRecorder.Code != http.StatusOK {
		t.Fatalf("list status = %d; want 200 body=%s", listRecorder.Code, listRecorder.Body.String())
	}
	if len(service.listOptions) != 1 || service.listSessionID != "sesn_http_threads" || service.listOptions[0].Limit != 7 || service.listOptions[0].Page != "signed-thread-page" {
		t.Fatalf("thread list scope/options = session %q options %+v", service.listSessionID, service.listOptions)
	}

	getRecorder := httptest.NewRecorder()
	getRequest := httptest.NewRequest(http.MethodGet, "/v1/sessions/sesn_http_threads/threads/thread_http_child?beta=true", nil)
	setAuthHeader(getRequest)
	router.ServeHTTP(getRecorder, getRequest)
	if getRecorder.Code != http.StatusOK {
		t.Fatalf("get status = %d; want 200 body=%s", getRecorder.Code, getRecorder.Body.String())
	}
	if service.getSessionID != "sesn_http_threads" || service.getThreadID != "thread_http_child" {
		t.Fatalf("get scope = %q/%q; want session/thread", service.getSessionID, service.getThreadID)
	}

	archiveRecorder := httptest.NewRecorder()
	archiveRequest := httptest.NewRequest(http.MethodPost, "/v1/sessions/sesn_http_threads/threads/thread_http_child/archive?beta=true", nil)
	setAuthHeader(archiveRequest)
	router.ServeHTTP(archiveRecorder, archiveRequest)
	if archiveRecorder.Code != http.StatusOK {
		t.Fatalf("archive status = %d; want 200 body=%s", archiveRecorder.Code, archiveRecorder.Body.String())
	}
	if service.archiveSessionID != "sesn_http_threads" || service.archiveThreadID != "thread_http_child" || service.archiveCalls != 1 {
		t.Fatalf("archive scope/calls = %q/%q calls=%d", service.archiveSessionID, service.archiveThreadID, service.archiveCalls)
	}

	service.archiveErr = &session.ConflictError{Message: "running or rescheduling session threads cannot be archived", InvalidRequest: true}
	conflictRecorder := httptest.NewRecorder()
	conflictRequest := httptest.NewRequest(http.MethodPost, "/v1/sessions/sesn_http_threads/threads/thread_http_child/archive?beta=true", nil)
	setAuthHeader(conflictRequest)
	router.ServeHTTP(conflictRecorder, conflictRequest)
	if conflictRecorder.Code != http.StatusConflict {
		t.Fatalf("archive conflict status = %d; want 409 body=%s", conflictRecorder.Code, conflictRecorder.Body.String())
	}
	assertErrorType(t, conflictRecorder, "invalid_request_error")

	badQueryRecorder := httptest.NewRecorder()
	badQueryRequest := httptest.NewRequest(http.MethodGet, "/v1/sessions/sesn_http_threads/threads?beta=false", nil)
	setAuthHeader(badQueryRequest)
	router.ServeHTTP(badQueryRecorder, badQueryRequest)
	if badQueryRecorder.Code != http.StatusBadRequest {
		t.Fatalf("bad beta status = %d; want 400 body=%s", badQueryRecorder.Code, badQueryRecorder.Body.String())
	}
	if len(service.listOptions) != 1 {
		t.Fatalf("bad beta reached service: calls=%d", len(service.listOptions))
	}

	bodyRecorder := httptest.NewRecorder()
	bodyRequest := httptest.NewRequest(http.MethodPost, "/v1/sessions/sesn_http_threads/threads/thread_http_child/archive", strings.NewReader(`{}`))
	setAuthHeader(bodyRequest)
	router.ServeHTTP(bodyRecorder, bodyRequest)
	if bodyRecorder.Code != http.StatusBadRequest {
		t.Fatalf("archive body status = %d; want 400 body=%s", bodyRecorder.Code, bodyRecorder.Body.String())
	}
	if service.archiveCalls != 2 {
		t.Fatalf("archive body reached service: calls=%d", service.archiveCalls)
	}
}

func TestListSessionsRejectsMalformedQueryParameters(t *testing.T) {
	router := newSessionHTTPTestRouter()
	for _, path := range []string{
		"/v1/sessions?after_id=sesn_old",
		"/v1/sessions?before_id=sesn_old",
		"/v1/sessions?statuses=idle",
		"/v1/sessions?statuses%5B%5D=unknown",
		"/v1/sessions?cursor=sesn_old",
		"/v1/sessions?order=sideways",
		"/v1/sessions?include_archived=1",
		"/v1/sessions?beta=false",
		"/v1/sessions?agent_version=1",
		"/v1/sessions?limit=",
		"/v1/sessions?limit=abc",
		"/v1/sessions?limit=0",
		"/v1/sessions?page=",
		"/v1/sessions?limit=%zz",
		"/v1/sessions?limit=1;page=abc",
	} {
		t.Run(path, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, path, nil)
			setAuthHeader(request)

			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d; want 400 body=%s", recorder.Code, recorder.Body.String())
			}
			assertErrorType(t, recorder, "invalid_request_error")
		})
	}
}

func TestSessionBetaRoutesRejectMalformedQueryBeforeService(t *testing.T) {
	for _, route := range []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{name: "create", method: http.MethodPost, path: "/v1/sessions", body: `{}`},
		{name: "list", method: http.MethodGet, path: "/v1/sessions"},
		{name: "retrieve", method: http.MethodGet, path: "/v1/sessions/sesn_test"},
		{name: "update", method: http.MethodPost, path: "/v1/sessions/sesn_test", body: `{}`},
		{name: "archive", method: http.MethodPost, path: "/v1/sessions/sesn_test/archive"},
		{name: "delete", method: http.MethodDelete, path: "/v1/sessions/sesn_test"},
	} {
		for _, query := range []struct {
			name  string
			value string
		}{
			{name: "missing"},
			{name: "false", value: "?beta=false"},
			{name: "duplicate", value: "?beta=true&beta=true"},
			{name: "unknown", value: "?beta=true&unknown=1"},
		} {
			t.Run(route.name+"/"+query.name, func(t *testing.T) {
				router, calls := sessionBetaRouteTestFixture(route.name)
				recorder := httptest.NewRecorder()
				request := httptest.NewRequest(route.method, route.path+query.value, strings.NewReader(route.body))
				request.Header.Set("x-api-key", testAPIKey)

				router.ServeHTTP(recorder, request)

				if recorder.Code != http.StatusBadRequest {
					t.Fatalf("status = %d; want 400 body=%s", recorder.Code, recorder.Body.String())
				}
				assertErrorType(t, recorder, "invalid_request_error")
				if calls() != 0 {
					t.Fatalf("service calls = %d; want 0", calls())
				}
			})
		}
	}
}

func sessionBetaRouteTestFixture(route string) (http.Handler, func() int) {
	switch route {
	case "create":
		service := &recordingCreateSessionService{}
		return newSessionHTTPTestRouterWithCreateService(service), func() int { return service.calls }
	case "list":
		service := &recordingSessionListService{}
		return newSessionHTTPTestRouterWithListService(service), func() int { return len(service.options) }
	default:
		service := &recordingSessionMutationService{}
		return newSessionHTTPTestRouterWithMutationService(service), func() int {
			switch route {
			case "retrieve":
				return service.getCalls
			case "update":
				return service.updateCalls
			case "archive":
				return service.archiveCalls
			default:
				return service.deleteCalls
			}
		}
	}
}

func TestListSessionResourcesRejectsMalformedQueryParameters(t *testing.T) {
	router := newSessionHTTPTestRouter()
	for _, path := range []string{
		"/v1/sessions/sesn_test/resources?cursor=sesrsc_old",
		"/v1/sessions/sesn_test/resources?limit=",
		"/v1/sessions/sesn_test/resources?limit=abc",
		"/v1/sessions/sesn_test/resources?limit=0",
		"/v1/sessions/sesn_test/resources?page=",
	} {
		t.Run(path, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, path, nil)
			setAuthHeader(request)

			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d; want 400 body=%s", recorder.Code, recorder.Body.String())
			}
			assertErrorType(t, recorder, "invalid_request_error")
		})
	}
}

func TestCreateSessionRejectsNullAndNonStringFieldsBeforeService(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "metadata null",
			body: `{"agent":"agent_test","environment_id":"env_test","vault_ids":[],"metadata":null}`,
		},
		{
			name: "metadata value null",
			body: `{"agent":"agent_test","environment_id":"env_test","vault_ids":[],"metadata":{"team":null}}`,
		},
		{
			name: "metadata value number",
			body: `{"agent":"agent_test","environment_id":"env_test","vault_ids":[],"metadata":{"team":42}}`,
		},
		{
			name: "agent version null",
			body: `{"agent":{"id":"agent_test","version":null},"environment_id":"env_test","vault_ids":[]}`,
		},
		{
			name: "agent version string",
			body: `{"agent":{"id":"agent_test","version":"2"},"environment_id":"env_test","vault_ids":[]}`,
		},
		{
			name: "agent version zero",
			body: `{"agent":{"id":"agent_test","version":0},"environment_id":"env_test","vault_ids":[]}`,
		},
		{
			name: "agent type null",
			body: `{"agent":{"type":null,"id":"agent_test"},"environment_id":"env_test","vault_ids":[]}`,
		},
		{
			name: "environment id null",
			body: `{"agent":"agent_test","environment_id":null,"vault_ids":[]}`,
		},
		{
			name: "github url null",
			body: `{"agent":"agent_test","environment_id":"env_test","vault_ids":[],"resources":[{"type":"github_repository","url":null}]}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			service := &recordingCreateSessionService{}
			router := newSessionHTTPTestRouterWithCreateService(service)
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/v1/sessions?beta=true", strings.NewReader(tc.body))
			request.Header.Set("Content-Type", "application/json")
			setAuthHeader(request)

			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d; want 400 body=%s", recorder.Code, recorder.Body.String())
			}
			assertErrorType(t, recorder, "invalid_request_error")
			if service.calls != 0 {
				t.Fatalf("Create calls = %d; want 0", service.calls)
			}
		})
	}
}

func TestCreateSessionRequiresExplicitVaultIDsBeforeService(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "missing vault_ids",
			body: `{"agent":"agent_test","environment_id":"env_test"}`,
		},
		{
			name: "null vault_ids",
			body: `{"agent":"agent_test","environment_id":"env_test","vault_ids":null}`,
		},
		{
			name: "non array vault_ids",
			body: `{"agent":"agent_test","environment_id":"env_test","vault_ids":"vlt_test"}`,
		},
		{
			name: "non string vault id",
			body: `{"agent":"agent_test","environment_id":"env_test","vault_ids":[42]}`,
		},
		{
			name: "null vault id",
			body: `{"agent":"agent_test","environment_id":"env_test","vault_ids":[null]}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			service := &recordingCreateSessionService{}
			router := newSessionHTTPTestRouterWithCreateService(service)
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/v1/sessions?beta=true", strings.NewReader(tc.body))
			request.Header.Set("Content-Type", "application/json")
			setAuthHeader(request)

			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d; want 400 body=%s", recorder.Code, recorder.Body.String())
			}
			assertErrorType(t, recorder, "invalid_request_error")
			if service.calls != 0 {
				t.Fatalf("Create calls = %d; want 0", service.calls)
			}
		})
	}
}

func TestCreateSessionPreservesOmittedAgentVersionAsLatest(t *testing.T) {
	service := &recordingCreateSessionService{}
	router := newSessionHTTPTestRouterWithCreateService(service)
	body := `{"agent":{"id":"agent_test"},"environment_id":"env_test","vault_ids":[]}`
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions?beta=true", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	setAuthHeader(request)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 body=%s", recorder.Code, recorder.Body.String())
	}
	if service.calls != 1 {
		t.Fatalf("Create calls = %d; want 1", service.calls)
	}
	if service.request.Agent.Version != nil {
		t.Fatalf("agent version = %v; want nil for latest", *service.request.Agent.Version)
	}
}

func TestCreateSessionAcceptsProviderSelector(t *testing.T) {
	service := &recordingCreateSessionService{}
	router := newSessionHTTPTestRouterWithCreateService(service)
	body := `{"agent":"agent_test","environment_id":"env_test","vault_ids":["vlt_test"],"providers":{"anthropic":{"credential_id":"cred_provider"}}}`
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions?beta=true", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	setAuthHeader(request)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 body=%s", recorder.Code, recorder.Body.String())
	}
	if service.calls != 1 {
		t.Fatalf("Create calls = %d; want 1", service.calls)
	}
	selector, ok := service.request.Providers["anthropic"]
	if !ok || selector.CredentialID != "cred_provider" {
		t.Fatalf("providers = %#v; want anthropic credential selector", service.request.Providers)
	}
}

func TestCreateSessionRejectsMalformedProvidersBeforeService(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"null providers", `{"agent":"agent_test","environment_id":"env_test","vault_ids":[],"providers":null}`},
		{"array providers", `{"agent":"agent_test","environment_id":"env_test","vault_ids":[],"providers":[]}`},
		{"null selector", `{"agent":"agent_test","environment_id":"env_test","vault_ids":[],"providers":{"anthropic":null}}`},
		{"selector array", `{"agent":"agent_test","environment_id":"env_test","vault_ids":[],"providers":{"anthropic":[]}}`},
		{"missing credential", `{"agent":"agent_test","environment_id":"env_test","vault_ids":[],"providers":{"anthropic":{}}}`},
		{"empty credential", `{"agent":"agent_test","environment_id":"env_test","vault_ids":[],"providers":{"anthropic":{"credential_id":""}}}`},
		{"extra selector field", `{"agent":"agent_test","environment_id":"env_test","vault_ids":[],"providers":{"anthropic":{"credential_id":"cred_provider","extra":"x"}}}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			service := &recordingCreateSessionService{}
			router := newSessionHTTPTestRouterWithCreateService(service)
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/v1/sessions?beta=true", strings.NewReader(tc.body))
			request.Header.Set("Content-Type", "application/json")
			setAuthHeader(request)

			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d; want 400 body=%s", recorder.Code, recorder.Body.String())
			}
			assertErrorType(t, recorder, "invalid_request_error")
			if service.calls != 0 {
				t.Fatalf("Create calls = %d; want 0", service.calls)
			}
		})
	}
}

func TestCreateSessionResponseIncludesNullDeploymentID(t *testing.T) {
	service := &recordingCreateSessionService{}
	router := newSessionHTTPTestRouterWithCreateService(service)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions?beta=true", strings.NewReader(`{"agent":"agent_test","environment_id":"env_test","vault_ids":[]}`))
	request.Header.Set("Content-Type", "application/json")
	setAuthHeader(request)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 body=%s", recorder.Code, recorder.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v body=%s", err, recorder.Body.String())
	}
	if value, ok := response["deployment_id"]; !ok || value != nil {
		t.Fatalf("deployment_id = %v (present=%v); want explicit null", value, ok)
	}
}

func TestCreateSessionAcceptsDocumentedGitHubCommitCheckoutShape(t *testing.T) {
	service := &recordingCreateSessionService{}
	router := newSessionHTTPTestRouterWithCreateService(service)
	rawSHA := "ABCDEF0123456789ABCDEF0123456789ABCDEF01"
	body := `{"agent":"agent_test","environment_id":"env_test","vault_ids":[],"resources":[{"type":"github_repository","url":"https://github.com/tetral-ai/tetral","checkout":{"type":"commit","sha":"` + rawSHA + `"}}]}`
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions?beta=true", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	setAuthHeader(request)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 body=%s", recorder.Code, recorder.Body.String())
	}
	if len(service.request.Resources) != 1 || service.request.Resources[0].Checkout == nil {
		t.Fatalf("captured resources = %+v", service.request.Resources)
	}
	checkout := service.request.Resources[0].Checkout
	if checkout.Type != "commit" || checkout.SHA != rawSHA {
		t.Fatalf("checkout = %+v; want documented sha shape", checkout)
	}
}

func TestCreateSessionAcceptsRequiredWriteOnlyGitHubResourceAuthorizationToken(t *testing.T) {
	service := &recordingCreateSessionService{}
	router := newSessionHTTPTestRouterWithCreateService(service)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/sessions?beta=true",
		strings.NewReader(`{"agent":"agent_test","environment_id":"env_test","vault_ids":[],"resources":[{"type":"github_repository","url":"https://github.com/tetral-ai/tetral","authorization_token":"redacted_legacy_resource_token"}]}`),
	)
	request.Header.Set("Content-Type", "application/json")
	setAuthHeader(request)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 body=%s", recorder.Code, recorder.Body.String())
	}
	if service.calls != 1 || len(service.request.Resources) != 1 ||
		service.request.Resources[0].AuthorizationToken != "redacted_legacy_resource_token" {
		t.Fatalf("Create request = %#v; want admitted write-only token", service.request)
	}
	if strings.Contains(recorder.Body.String(), "redacted_legacy_resource_token") {
		t.Fatalf("response echoed resource credential value: %s", recorder.Body.String())
	}
}

func TestCreateSessionRejectsResourceFieldsOutsideSelectedTypeBeforeService(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		forbidden string
	}{
		{
			name:      "file rejects github url field",
			body:      `{"agent":"agent_test","environment_id":"env_test","vault_ids":[],"resources":[{"type":"file","file_id":"file_source","url":"https://github.com/tetral-ai/forbidden-file-field"}]}`,
			forbidden: "forbidden-file-field",
		},
		{
			name:      "memory rejects file fields",
			body:      `{"agent":"agent_test","environment_id":"env_test","vault_ids":[],"resources":[{"type":"memory_store","memory_store_id":"memstore_test","file_id":"forbidden_file_source"}]}`,
			forbidden: "forbidden_file_source",
		},
		{
			name:      "github rejects memory fields",
			body:      `{"agent":"agent_test","environment_id":"env_test","vault_ids":[],"resources":[{"type":"github_repository","url":"https://github.com/tetral-ai/tetral","memory_store_id":"forbidden_memstore"}]}`,
			forbidden: "forbidden_memstore",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			service := &recordingCreateSessionService{}
			router := newSessionHTTPTestRouterWithCreateService(service)
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/v1/sessions?beta=true", strings.NewReader(tc.body))
			request.Header.Set("Content-Type", "application/json")
			setAuthHeader(request)

			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d; want 400 body=%s", recorder.Code, recorder.Body.String())
			}
			assertErrorType(t, recorder, "invalid_request_error")
			if service.calls != 0 {
				t.Fatalf("Create calls = %d; want 0", service.calls)
			}
			if strings.Contains(recorder.Body.String(), tc.forbidden) {
				t.Fatalf("error response echoed rejected field value: %s", recorder.Body.String())
			}
		})
	}
}

func TestAddResourceRejectsResourceFieldsOutsideFileTypeBeforeService(t *testing.T) {
	service := &recordingSessionMutationService{}
	router := newSessionHTTPTestRouterWithMutationService(service)
	recorder := httptest.NewRecorder()
	body := `{"type":"file","file_id":"file_source","url":"https://github.com/tetral-ai/forbidden-add-field"}`
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/sesn_test/resources?beta=true", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	setAuthHeader(request)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d; want 400 body=%s", recorder.Code, recorder.Body.String())
	}
	assertErrorType(t, recorder, "invalid_request_error")
	if service.addResourceCalls != 0 {
		t.Fatalf("AddResource calls = %d; want 0", service.addResourceCalls)
	}
	if strings.Contains(recorder.Body.String(), "forbidden-add-field") {
		t.Fatalf("error response echoed rejected field value: %s", recorder.Body.String())
	}
}

func TestUpdateSessionRejectsImmutableFields(t *testing.T) {
	router := newSessionHTTPTestRouter()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/sesn_test?beta=true", strings.NewReader(`{"status":"running"}`))
	request.Header.Set("Content-Type", "application/json")
	setAuthHeader(request)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d; want 400 body=%s", recorder.Code, recorder.Body.String())
	}
	assertErrorType(t, recorder, "invalid_request_error")
}

func TestUpdateSessionRejectsNullBodyBeforeService(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "null body",
			body: `null`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &recordingSessionMutationService{}
			router := newSessionHTTPTestRouterWithMutationService(service)
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/v1/sessions/sesn_test?beta=true", strings.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			setAuthHeader(request)

			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d; want 400 body=%s", recorder.Code, recorder.Body.String())
			}
			assertErrorType(t, recorder, "invalid_request_error")
			if service.updateCalls != 0 {
				t.Fatalf("Update calls = %d; want 0", service.updateCalls)
			}
		})
	}
}

func TestUpdateSessionRejectsProviderSelectorPatch(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
	}{
		{
			name: "selector entry",
			body: `{"providers":{"anthropic":{"credential_id":"cred_provider"}}}`,
		},
		{
			name: "empty object",
			body: `{"providers":{}}`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := &recordingSessionMutationService{}
			router := newSessionHTTPTestRouterWithMutationService(service)
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/v1/sessions/sesn_test?beta=true", strings.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			setAuthHeader(request)

			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d; want 400 body=%s", recorder.Code, recorder.Body.String())
			}
			assertErrorType(t, recorder, "invalid_request_error")
			if !strings.Contains(recorder.Body.String(), `"message":"field is immutable"`) {
				t.Fatalf("body = %s; want immutable-field message", recorder.Body.String())
			}
			if service.updateCalls != 0 {
				t.Fatalf("Update calls = %d; want 0", service.updateCalls)
			}
		})
	}
}

func TestUpdateSessionAcceptsApprovalModePatch(t *testing.T) {
	service := &recordingSessionMutationService{}
	router := newSessionHTTPTestRouterWithMutationService(service)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/sesn_test?beta=true", strings.NewReader(`{"agent":{"approval_mode":"full_access"}}`))
	request.Header.Set("Content-Type", "application/json")
	setAuthHeader(request)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 body=%s", recorder.Code, recorder.Body.String())
	}
	if service.updateCalls != 1 {
		t.Fatalf("Update calls = %d; want 1", service.updateCalls)
	}
	if service.updateRequest.ApprovalMode == nil || *service.updateRequest.ApprovalMode != session.ApprovalModeFullAccess {
		t.Fatalf("ApprovalMode = %#v; want full_access", service.updateRequest.ApprovalMode)
	}
}

func TestUpdateSessionAcceptsRuntimeVisibleAgentPatch(t *testing.T) {
	service := &recordingSessionMutationService{}
	router := newSessionHTTPTestRouterWithMutationService(service)
	recorder := httptest.NewRecorder()
	body := `{"agent":{"tools":[{"type":"mcp_toolset","mcp_server_name":"github","configs":[{"name":"github_search","enabled":true}]}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}]}}`
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/sesn_test?beta=true", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	setAuthHeader(request)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 body=%s", recorder.Code, recorder.Body.String())
	}
	if service.updateCalls != 1 {
		t.Fatalf("Update calls = %d; want 1", service.updateCalls)
	}
	if service.updateRequest.ToolsPatch == nil || len(*service.updateRequest.ToolsPatch) != 1 {
		t.Fatalf("ToolsPatch = %#v; want one replacement entry", service.updateRequest.ToolsPatch)
	}
	if service.updateRequest.MCPServersPatch == nil || len(*service.updateRequest.MCPServersPatch) != 1 {
		t.Fatalf("MCPServersPatch = %#v; want one replacement entry", service.updateRequest.MCPServersPatch)
	}
}

func TestUpdateSessionRejectsMalformedApprovalModeBeforeService(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{name: "null", body: `{"agent":{"approval_mode":null}}`},
		{name: "number", body: `{"agent":{"approval_mode":42}}`},
		{name: "unknown", body: `{"agent":{"approval_mode":"always"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := &recordingSessionMutationService{}
			router := newSessionHTTPTestRouterWithMutationService(service)
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/v1/sessions/sesn_test?beta=true", strings.NewReader(tc.body))
			request.Header.Set("Content-Type", "application/json")
			setAuthHeader(request)

			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d; want 400 body=%s", recorder.Code, recorder.Body.String())
			}
			assertErrorType(t, recorder, "invalid_request_error")
			if service.updateCalls != 0 {
				t.Fatalf("Update calls = %d; want 0", service.updateCalls)
			}
		})
	}
}

func TestUpdateSessionRejectsTopLevelApprovalModeBeforeService(t *testing.T) {
	service := &recordingSessionMutationService{}
	router := newSessionHTTPTestRouterWithMutationService(service)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/sesn_test?beta=true", strings.NewReader(`{"approval_mode":"full_access"}`))
	request.Header.Set("Content-Type", "application/json")
	setAuthHeader(request)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d; want 400 body=%s", recorder.Code, recorder.Body.String())
	}
	if service.updateCalls != 0 {
		t.Fatalf("Update calls = %d; want 0", service.updateCalls)
	}
}

func TestUpdateSessionRejectsMalformedAgentPatchBeforeService(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "null agent", body: `{"agent":null}`},
		{name: "array agent", body: `{"agent":[]}`},
		{name: "null tools", body: `{"agent":{"tools":null}}`},
		{name: "object tools", body: `{"agent":{"tools":{}}}`},
		{name: "immutable model", body: `{"agent":{"model":"anthropic/claude-opus-4-8"}}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			service := &recordingSessionMutationService{}
			router := newSessionHTTPTestRouterWithMutationService(service)
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/v1/sessions/sesn_test?beta=true", strings.NewReader(tc.body))
			request.Header.Set("Content-Type", "application/json")
			setAuthHeader(request)

			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d; want 400 body=%s", recorder.Code, recorder.Body.String())
			}
			assertErrorType(t, recorder, "invalid_request_error")
			if service.updateCalls != 0 {
				t.Fatalf("Update calls = %d; want 0", service.updateCalls)
			}
		})
	}
}

func TestUpdateSessionMetadataRequiresObjectButAllowsPerKeyNull(t *testing.T) {
	router := newSessionHTTPTestRouter()
	for _, body := range []string{`{"metadata":[]}`} {
		t.Run(body, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/v1/sessions/sesn_test?beta=true", strings.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			setAuthHeader(request)

			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d; want 400 body=%s", recorder.Code, recorder.Body.String())
			}
			assertErrorType(t, recorder, "invalid_request_error")
		})
	}

	service := &recordingSessionMutationService{}
	router = newSessionHTTPTestRouterWithMutationService(service)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/sesn_test?beta=true", strings.NewReader(`{"metadata":{"drop":null}}`))
	request.Header.Set("Content-Type", "application/json")
	setAuthHeader(request)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("per-key null status = %d; want 200 body=%s", recorder.Code, recorder.Body.String())
	}
	if service.updateCalls != 1 {
		t.Fatalf("Update calls = %d; want 1", service.updateCalls)
	}
	if value, ok := service.updateRequest.MetadataPatch["drop"]; !ok {
		t.Fatalf("MetadataPatch missing drop key: %#v", service.updateRequest.MetadataPatch)
	} else if value != nil {
		t.Fatalf("MetadataPatch[drop] = %q; want nil", *value)
	}
}

func TestSessionNoBodyMutationRoutesRejectBodiesBeforeService(t *testing.T) {
	tests := []struct {
		name        string
		method      string
		path        string
		calls       func(*recordingSessionMutationService) int
		description string
	}{
		{
			name:        "archive session",
			method:      http.MethodPost,
			path:        "/v1/sessions/sesn_test/archive?beta=true",
			calls:       func(service *recordingSessionMutationService) int { return service.archiveCalls },
			description: "Archive",
		},
		{
			name:        "delete session",
			method:      http.MethodDelete,
			path:        "/v1/sessions/sesn_test?beta=true",
			calls:       func(service *recordingSessionMutationService) int { return service.deleteCalls },
			description: "Delete",
		},
		{
			name:        "delete resource",
			method:      http.MethodDelete,
			path:        "/v1/sessions/sesn_test/resources/sesrsc_test?beta=true",
			calls:       func(service *recordingSessionMutationService) int { return service.deleteResourceCalls },
			description: "DeleteResource",
		},
	}
	for _, test := range tests {
		t.Run(test.name+"/non-empty", func(t *testing.T) {
			service := &recordingSessionMutationService{}
			router := newSessionHTTPTestRouterWithMutationService(service)
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(test.method, test.path, strings.NewReader(`{}`))
			request.Header.Set("Content-Type", "application/json")
			setAuthHeader(request)

			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d; want 400 body=%s", recorder.Code, recorder.Body.String())
			}
			assertErrorType(t, recorder, "invalid_request_error")
			if test.calls(service) != 0 {
				t.Fatalf("%s calls = %d; want 0", test.description, test.calls(service))
			}
		})

		t.Run(test.name+"/oversized", func(t *testing.T) {
			service := &recordingSessionMutationService{}
			router := newSessionHTTPTestRouterWithMutationService(service)
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(test.method, test.path, strings.NewReader(strings.Repeat("x", (1<<20)+1)))
			request.Header.Set("Content-Type", "application/json")
			setAuthHeader(request)

			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusRequestEntityTooLarge {
				t.Fatalf("status = %d; want 413 body=%s", recorder.Code, recorder.Body.String())
			}
			assertErrorType(t, recorder, "request_too_large")
			if test.calls(service) != 0 {
				t.Fatalf("%s calls = %d; want 0", test.description, test.calls(service))
			}
		})

		t.Run(test.name+"/empty", func(t *testing.T) {
			service := &recordingSessionMutationService{}
			router := newSessionHTTPTestRouterWithMutationService(service)
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(test.method, test.path, nil)
			setAuthHeader(request)

			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d; want 200 body=%s", recorder.Code, recorder.Body.String())
			}
			if test.calls(service) != 1 {
				t.Fatalf("%s calls = %d; want 1", test.description, test.calls(service))
			}
		})
	}
}

func TestSessionResourceLifecycleConflictsUseInvalidRequestError(t *testing.T) {
	tests := []struct {
		name      string
		method    string
		path      string
		body      string
		configure func(*recordingSessionMutationService)
		calls     func(*recordingSessionMutationService) int
	}{
		{
			name:   "add resource while session is not idle",
			method: http.MethodPost,
			path:   "/v1/sessions/sesn_test/resources?beta=true",
			body:   `{"type":"file","file_id":"file_source"}`,
			configure: func(service *recordingSessionMutationService) {
				service.addResourceErr = &session.ConflictError{Message: "session must be idle for resource mutation", InvalidRequest: true}
			},
			calls: func(service *recordingSessionMutationService) int { return service.addResourceCalls },
		},
		{
			name:   "delete resource running",
			method: http.MethodDelete,
			path:   "/v1/sessions/sesn_test/resources/sesrsc_test?beta=true",
			configure: func(service *recordingSessionMutationService) {
				service.deleteResourceErr = &session.ConflictError{Message: "session must be idle for resource mutation", InvalidRequest: true}
			},
			calls: func(service *recordingSessionMutationService) int { return service.deleteResourceCalls },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &recordingSessionMutationService{}
			test.configure(service)
			router := newSessionHTTPTestRouterWithMutationService(service)
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
			if test.body != "" {
				request.Header.Set("Content-Type", "application/json")
			}
			setAuthHeader(request)

			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusConflict {
				t.Fatalf("status = %d; want 409 body=%s", recorder.Code, recorder.Body.String())
			}
			assertErrorType(t, recorder, "invalid_request_error")
			if strings.Contains(recorder.Body.String(), "conflict_error") {
				t.Fatalf("resource lifecycle conflict exposed conflict_error: %s", recorder.Body.String())
			}
			if test.calls(service) != 1 {
				t.Fatalf("service calls = %d; want 1", test.calls(service))
			}
		})
	}
}

func TestSessionArchiveDeleteRunningSessionUsesPublicConflictEnvelope(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		path       string
		configure  func(*recordingSessionMutationService)
		assertCall func(*testing.T, *recordingSessionMutationService)
	}{
		{
			name:   "archive",
			method: http.MethodPost,
			path:   "/v1/sessions/sesn_running/archive?beta=true",
			configure: func(service *recordingSessionMutationService) {
				service.archiveErr = &session.ConflictError{Message: "running sessions cannot be archived", InvalidRequest: true}
			},
			assertCall: func(t *testing.T, service *recordingSessionMutationService) {
				t.Helper()
				if service.archiveCalls != 1 || service.deleteCalls != 0 {
					t.Fatalf("archive/delete calls = %d/%d; want 1/0", service.archiveCalls, service.deleteCalls)
				}
			},
		},
		{
			name:   "delete",
			method: http.MethodDelete,
			path:   "/v1/sessions/sesn_running?beta=true",
			configure: func(service *recordingSessionMutationService) {
				service.deleteErr = &session.ConflictError{Message: "running sessions cannot be deleted", InvalidRequest: true}
			},
			assertCall: func(t *testing.T, service *recordingSessionMutationService) {
				t.Helper()
				if service.archiveCalls != 0 || service.deleteCalls != 1 {
					t.Fatalf("archive/delete calls = %d/%d; want 0/1", service.archiveCalls, service.deleteCalls)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &recordingSessionMutationService{}
			test.configure(service)
			router := newSessionHTTPTestRouterWithMutationService(service)
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(test.method, test.path, nil)
			setAuthHeader(request)

			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusConflict {
				t.Fatalf("status = %d; want 409 body=%s", recorder.Code, recorder.Body.String())
			}
			assertErrorType(t, recorder, "invalid_request_error")
			if !strings.Contains(recorder.Body.String(), "running sessions cannot be") {
				t.Fatalf("body missing running-session conflict message: %s", recorder.Body.String())
			}
			test.assertCall(t, service)
		})
	}
}

type recordingSessionListService struct {
	fakeSessionService
	options []session.ListOptions
}

func (s *recordingSessionListService) List(_ context.Context, _ workspace.ID, options session.ListOptions) (*session.ListResult, error) {
	s.options = append(s.options, options)
	return &session.ListResult{Data: []*session.Response{}}, nil
}

type recordingThreadService struct {
	fakeSessionService
	listSessionID    string
	listOptions      []session.ThreadListOptions
	getSessionID     string
	getThreadID      string
	archiveSessionID string
	archiveThreadID  string
	archiveCalls     int
	archiveErr       error
}

func (s *recordingThreadService) ListThreads(_ context.Context, _ workspace.ID, sessionID string, options session.ThreadListOptions) (*session.ThreadListResult, error) {
	s.listSessionID = sessionID
	s.listOptions = append(s.listOptions, options)
	return s.fakeSessionService.ListThreads(context.Background(), workspace.DefaultID, sessionID, options)
}

func (s *recordingThreadService) GetThread(_ context.Context, _ workspace.ID, sessionID string, threadID string) (*session.ThreadResponse, error) {
	s.getSessionID = sessionID
	s.getThreadID = threadID
	return s.fakeSessionService.GetThread(context.Background(), workspace.DefaultID, sessionID, threadID)
}

func (s *recordingThreadService) ArchiveThread(_ context.Context, _ workspace.ID, sessionID string, threadID string) (*session.ThreadResponse, error) {
	s.archiveSessionID = sessionID
	s.archiveThreadID = threadID
	s.archiveCalls++
	if s.archiveErr != nil {
		return nil, s.archiveErr
	}
	return s.fakeSessionService.ArchiveThread(context.Background(), workspace.DefaultID, sessionID, threadID)
}

type recordingCreateSessionService struct {
	fakeSessionService
	calls   int
	request session.CreateRequest
	err     error
}

func (s *recordingCreateSessionService) Create(ctx context.Context, ws workspace.ID, request session.CreateRequest) (*session.Response, error) {
	s.calls++
	s.request = request
	if s.err != nil {
		return nil, s.err
	}
	return s.fakeSessionService.Create(ctx, ws, request)
}

type recordingSessionMutationService struct {
	fakeSessionService
	getCalls            int
	updateCalls         int
	updateRequest       session.UpdateRequest
	archiveCalls        int
	deleteCalls         int
	addResourceCalls    int
	updateResourceCalls int
	deleteResourceCalls int
	archiveErr          error
	deleteErr           error
	addResourceErr      error
	deleteResourceErr   error
}

func (s *recordingSessionMutationService) Get(ctx context.Context, ws workspace.ID, sessionID string) (*session.Response, error) {
	s.getCalls++
	return s.fakeSessionService.Get(ctx, ws, sessionID)
}

func (s *recordingSessionMutationService) Update(ctx context.Context, ws workspace.ID, sessionID string, request session.UpdateRequest) (*session.Response, error) {
	s.updateCalls++
	s.updateRequest = request
	return s.fakeSessionService.Update(ctx, ws, sessionID, request)
}

func (s *recordingSessionMutationService) Archive(ctx context.Context, ws workspace.ID, sessionID string) (*session.Response, error) {
	s.archiveCalls++
	if s.archiveErr != nil {
		return nil, s.archiveErr
	}
	return s.fakeSessionService.Archive(ctx, ws, sessionID)
}

func (s *recordingSessionMutationService) Delete(ctx context.Context, ws workspace.ID, sessionID string) (*session.DeleteResponse, error) {
	s.deleteCalls++
	if s.deleteErr != nil {
		return nil, s.deleteErr
	}
	return s.fakeSessionService.Delete(ctx, ws, sessionID)
}

func (s *recordingSessionMutationService) AddResource(ctx context.Context, ws workspace.ID, sessionID string, request session.ResourceRequest) (*session.ResourceResponse, error) {
	s.addResourceCalls++
	if s.addResourceErr != nil {
		return nil, s.addResourceErr
	}
	return s.fakeSessionService.AddResource(ctx, ws, sessionID, request)
}

func (s *recordingSessionMutationService) UpdateResource(_ context.Context, _ workspace.ID, _ string, _ string, _ string) (*session.ResourceResponse, error) {
	s.updateResourceCalls++
	return &session.ResourceResponse{ID: "sesrsc_test", Type: string(session.ResourceTypeGitHubRepository)}, nil
}

func (s *recordingSessionMutationService) DeleteResource(ctx context.Context, ws workspace.ID, sessionID string, resourceID string) (*session.ResourceDeleteResponse, error) {
	s.deleteResourceCalls++
	if s.deleteResourceErr != nil {
		return nil, s.deleteResourceErr
	}
	return s.fakeSessionService.DeleteResource(ctx, ws, sessionID, resourceID)
}
