package httpapi_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// testAPIKey is the bootstrap API key used across httpapi tests. It
// satisfies the strength floor enforced by auth.ValidateBootstrapKey.
const testAPIKey = "test-api-key-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" //nolint:gosec // G101: synthetic test bootstrap key

func defaultWorkspaceContext() context.Context {
	return workspace.WithContext(context.Background(), workspace.Workspace{ID: workspace.DefaultID})
}

// setAuthHeader adds the test bootstrap API key and the query marker emitted by
// the forked SDK for mapped beta routes.
func setAuthHeader(r *http.Request) {
	r.Header.Set("x-api-key", testAPIKey)
	addSDKBetaQueryMarker(r)
}

func addSDKBetaQueryMarker(r *http.Request) {
	path := r.URL.Path
	mapped := path == "/v1/agents" || strings.HasPrefix(path, "/v1/agents/") ||
		path == "/v1/environments" || strings.HasPrefix(path, "/v1/environments/") ||
		path == "/v1/memory_stores" || strings.HasPrefix(path, "/v1/memory_stores/") ||
		path == "/v1/sessions" || strings.HasPrefix(path, "/v1/sessions/") ||
		path == "/v1/files" || strings.HasPrefix(path, "/v1/files/") ||
		path == "/v1/vaults" || strings.HasPrefix(path, "/v1/vaults/") ||
		path == "/v1/skills" || strings.HasPrefix(path, "/v1/skills/")
	if !mapped {
		return
	}
	if _, ok := r.URL.Query()["beta"]; ok {
		return
	}
	if r.URL.RawQuery == "" {
		r.URL.RawQuery = "beta=true"
		return
	}
	r.URL.RawQuery += "&beta=true"
}

func assertErrorType(t *testing.T, recorder *httptest.ResponseRecorder, want string) {
	t.Helper()
	var response struct {
		Type  string `json:"type"`
		Error struct {
			Type string `json:"type"`
		} `json:"error"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode error response: %v body=%s", err, recorder.Body.String())
	}
	if response.Type != "error" {
		t.Fatalf("response type = %q; want error body=%s", response.Type, recorder.Body.String())
	}
	if response.Error.Type != want {
		t.Fatalf("error type = %q; want %q body=%s", response.Error.Type, want, recorder.Body.String())
	}
}

func assertErrorRequestID(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()
	var response struct {
		RequestID string `json:"request_id"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode error response request_id: %v body=%s", err, recorder.Body.String())
	}
	if response.RequestID == "" {
		t.Fatalf("error response missing request_id body=%s", recorder.Body.String())
	}
}

// newTestDBFromStorage provisions a PostgreSQL runtime database with
// the schema initialized, the bootstrap api_keys row seeded for the
// default workspace, and returns the *sql.DB. The handler/router/error
// tests run against the same PostgreSQL stack production wires.
func newTestDBFromStorage(t *testing.T) *sql.DB {
	t.Helper()
	db := storagetest.NewPostgreSQLDB(t)
	store := auth.NewAPIKeyStore(db)
	if err := auth.RefreshBootstrap(defaultWorkspaceContext(), store, workspace.DefaultID, testAPIKey); err != nil {
		t.Fatalf("seed bootstrap key: %v", err)
	}
	return db
}

func newAuthenticatedRouter(t *testing.T, sessionHandler *httpapi.SessionHandler, options ...httpapi.RouterOption) http.Handler {
	t.Helper()
	authDB := newTestDBFromStorage(t)
	authStore := auth.NewAPIKeyStore(authDB)
	allOptions := []httpapi.RouterOption{
		httpapi.WithAuthenticator(&auth.StoreAuthenticator{Store: authStore}),
	}
	allOptions = append(allOptions, options...)
	return httpapi.NewRouter(sessionHandler, "", allOptions...)
}
