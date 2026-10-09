package tetralauth

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestAPIKeyManagementUsesSignedPrincipalAndManagedAgentsCursorShape(t *testing.T) {
	router, _, privateKey := newTestAuthRouter(t)
	token := mintTestPrincipal(t, privateKey, http.MethodPost, "/v1/api_keys")

	createRequest := httptest.NewRequest(http.MethodPost, "/v1/api_keys", strings.NewReader(`{"name":"ci-key"}`))
	createRequest.Header.Set("X-Tetral-Internal-Principal", token)
	createRecorder := httptest.NewRecorder()
	router.ServeHTTP(createRecorder, createRequest)
	if createRecorder.Code != http.StatusOK {
		t.Fatalf("create status = %d body=%s; want 200", createRecorder.Code, createRecorder.Body.String())
	}
	var created auth.CreateAPIKeyResult
	if err := json.Unmarshal(createRecorder.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create: %v", err)
	}
	if created.APIKey == "" || created.WorkspaceID != "ws_auth_test" || created.KeyKind != auth.KindStandard {
		t.Fatalf("created key = %#v; want raw key once and workspace-scoped standard metadata", created)
	}

	listToken := mintTestPrincipal(t, privateKey, http.MethodGet, "/v1/api_keys")
	listRequest := httptest.NewRequest(http.MethodGet, "/v1/api_keys?limit=1", nil)
	listRequest.Header.Set("X-Tetral-Internal-Principal", listToken)
	listRecorder := httptest.NewRecorder()
	router.ServeHTTP(listRecorder, listRequest)
	if listRecorder.Code != http.StatusOK {
		t.Fatalf("list status = %d body=%s; want 200", listRecorder.Code, listRecorder.Body.String())
	}
	var listed apiKeyListResponse
	if err := json.Unmarshal(listRecorder.Body.Bytes(), &listed); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(listed.Data) != 1 || listed.NextPage == nil || *listed.NextPage == "" {
		t.Fatalf("list response = %#v; want data plus signed next_page", listed)
	}

	deletePath := "/v1/api_keys/" + created.ID
	deleteRequest := httptest.NewRequest(http.MethodDelete, deletePath, nil)
	deleteRequest.Header.Set("X-Tetral-Internal-Principal", mintTestPrincipal(t, privateKey, http.MethodDelete, deletePath))
	deleteRecorder := httptest.NewRecorder()
	router.ServeHTTP(deleteRecorder, deleteRequest)
	if deleteRecorder.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d body=%s; want 204", deleteRecorder.Code, deleteRecorder.Body.String())
	}
	if deleteRecorder.Body.Len() != 0 {
		t.Fatalf("delete body = %q; want empty", deleteRecorder.Body.String())
	}
	if got := deleteRecorder.Header().Get("Content-Type"); got != "" {
		t.Fatalf("delete Content-Type = %q; want absent for empty 204 response", got)
	}
}

func TestAPIKeyManagementErrorsUseSDKEnvelopeWithRequestID(t *testing.T) {
	privateKey := mustGenerateTestPrivateKey(t)
	signer, err := auth.NewInternalPrincipalSignerFromBase64(privateKey)
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	router := NewRouter(RouterConfig{
		Store:  auth.NewAPIKeyStore(nil),
		Signer: signer,
	})

	request := httptest.NewRequest(http.MethodPost, "/v1/api_keys", strings.NewReader(`{"name":"ci-key","unexpected":true}`))
	request.Header.Set("X-Tetral-Internal-Principal", mintTestPrincipal(t, privateKey, http.MethodPost, "/v1/api_keys"))
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s; want 400", recorder.Code, recorder.Body.String())
	}
	var envelope errorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode error envelope: %v body=%s", err, recorder.Body.String())
	}
	if envelope.Type != "error" || envelope.Error.Type != "invalid_request_error" || envelope.RequestID == "" {
		t.Fatalf("envelope = %+v; want SDK error with request_id", envelope)
	}
}

func TestAPIKeyManagementTooLargeErrorsUseInvalidRequestEnvelope(t *testing.T) {
	signer, err := auth.NewInternalPrincipalSignerFromBase64(mustGenerateTestPrivateKey(t))
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	router := NewRouter(RouterConfig{
		Store:  auth.NewAPIKeyStore(nil),
		Signer: signer,
	})
	token, err := signer.Mint(auth.IndependentKeyPrincipal(workspace.Workspace{ID: workspace.ID("ws_auth_test"), Type: "workspace"}, "ak_test_principal"), http.MethodPost, "/v1/api_keys", "req_large_test", 60_000_000_000)
	if err != nil {
		t.Fatalf("mint principal: %v", err)
	}

	request := httptest.NewRequest(http.MethodPost, "/v1/api_keys", strings.NewReader(`{"name":"`+strings.Repeat("x", apiKeyBodyByteCap)+`"}`))
	request.Header.Set("X-Tetral-Internal-Principal", token)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d body=%s; want 413", recorder.Code, recorder.Body.String())
	}
	var envelope errorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode error envelope: %v body=%s", err, recorder.Body.String())
	}
	if envelope.Type != "error" || envelope.Error.Type != "invalid_request_error" || envelope.RequestID == "" {
		t.Fatalf("envelope = %+v; want 413 invalid_request_error with request_id", envelope)
	}
}

func newTestAuthRouter(t *testing.T) (http.Handler, *sql.DB, string) {
	t.Helper()
	runtimeDB, adminDB := storagetest.NewPostgreSQLDBWithAdmin(t)
	if _, err := adminDB.ExecContext(context.Background(),
		`INSERT INTO workspaces (id, type, name, created_at)
		 VALUES ('ws_auth_test', 'workspace', 'Auth Test', '2026-01-01T00:00:00Z')`,
	); err != nil {
		t.Fatalf("seed workspace: %v", err)
	}
	rawFixtureKey, err := auth.GenerateAPIKey()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adminDB.ExecContext(context.Background(), `INSERT INTO api_keys(id,workspace_id,name,key_prefix,key_digest,key_kind,authority_kind,created_at) VALUES('ak_test_principal','ws_auth_test','signed fixture','fixture',$1,'standard','independent_key',clock_timestamp())`, auth.DigestAPIKey(rawFixtureKey)); err != nil {
		t.Fatal(err)
	}

	privateKey, err := auth.GenerateEd25519PrivateKeyBase64()
	if err != nil {
		t.Fatalf("generate signing key: %v", err)
	}
	router, err := BuildRouter(context.Background(), RouterBuildConfig{
		RawDatabase: runtimeDB,
		Config: Config{
			BootstrapAPIKey:                testBootstrapAPIKey(),
			BootstrapWorkspaceID:           workspace.ID("ws_auth_test"),
			InternalPrincipalPrivateKeyB64: privateKey,
			InternalPrincipalTTL:           60_000_000_000,
		},
	})
	if err != nil {
		t.Fatalf("BuildRouter: %v", err)
	}
	return router, adminDB, privateKey
}

func mintTestPrincipal(t *testing.T, privateKey string, method string, path string) string {
	t.Helper()
	signer, err := auth.NewInternalPrincipalSignerFromBase64(privateKey)
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	token, err := signer.Mint(auth.IndependentKeyPrincipal(workspace.Workspace{ID: workspace.ID("ws_auth_test"), Type: "workspace"}, "ak_test_principal"), method, path, "req_test", 60_000_000_000)
	if err != nil {
		t.Fatalf("mint principal: %v", err)
	}
	return token
}

func mustGenerateTestPrivateKey(t *testing.T) string {
	t.Helper()
	privateKey, err := auth.GenerateEd25519PrivateKeyBase64()
	if err != nil {
		t.Fatalf("generate signing key: %v", err)
	}
	return privateKey
}

func testBootstrapAPIKey() string {
	return "tetral_sk_bootstrap_auth_test_012345678901234567890123456789"
}
