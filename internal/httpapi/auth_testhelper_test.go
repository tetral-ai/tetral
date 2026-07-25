package httpapi_test

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// bootstrapEnvKey is the test ENGINE_API_KEY value for U2 HTTP
// integration tests. It is exactly 32 bytes (the strength floor)
// plus filler so any future floor bump still leaves the helper
// passing.
const bootstrapEnvKey = "tetral_test_bootstrap_key_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

// authTestEnv groups the dependencies the api_key + workspace HTTP
// tests share: a runtime *sql.DB, an admin *sql.DB for fixture
// seeding, the runtime-role api-key store, and the bootstrap raw key.
type authTestEnv struct {
	runtime *sql.DB
	admin   *sql.DB
	store   *auth.APIKeyStore
	envKey  string
}

func newAuthTestEnv(t *testing.T) *authTestEnv {
	t.Helper()
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	store := auth.NewAPIKeyStore(runtime)
	if err := auth.RefreshBootstrap(defaultWorkspaceContext(), store, workspace.DefaultID, bootstrapEnvKey); err != nil {
		t.Fatalf("seed bootstrap: %v", err)
	}
	return &authTestEnv{
		runtime: runtime,
		admin:   admin,
		store:   store,
		envKey:  bootstrapEnvKey,
	}
}

// router returns a chi router wired with the U2 auth surface and the
// supplied options. API-key management lives outside httpapi in the draft
// architecture, so this helper only installs authentication context for
// route families that still belong here.
func (e *authTestEnv) router(opts ...httpapi.RouterOption) http.Handler {
	options := []httpapi.RouterOption{
		httpapi.WithAuthenticator(&auth.StoreAuthenticator{Store: e.store}),
	}
	options = append(options, opts...)
	return httpapi.NewRouter(nil, "", options...)
}

func (e *authTestEnv) seedWorkspace(t *testing.T, id, name string) {
	t.Helper()
	_, err := e.admin.ExecContext(defaultWorkspaceContext(),
		`INSERT INTO workspaces (id, type, name, created_at) VALUES ($1, 'workspace', $2, '2026-01-01T00:00:00Z')
		 ON CONFLICT (id) DO NOTHING`, id, name)
	if err != nil {
		t.Fatalf("seed workspace %s: %v", id, err)
	}
}

// performJSONRequest issues req with the given key, returns the
// recorded response, and decodes the body as JSON.
func performJSONRequest(t *testing.T, h http.Handler, method, path, key, body string) (int, string) {
	t.Helper()
	var rdr *strings.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	} else {
		rdr = strings.NewReader("")
	}
	req := httptest.NewRequest(method, path, rdr)
	if key != "" {
		req.Header.Set("x-api-key", key)
		addSDKBetaQueryMarker(req)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w.Code, w.Body.String()
}
