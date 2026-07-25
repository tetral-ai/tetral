package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/vault"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const testVaultKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func newVaultTestRouter(t *testing.T) http.Handler {
	return newVaultTestRouterWithVaultOptions(t)
}

func newVaultTestRouterWithVaultOptions(t *testing.T, options ...vault.ServiceOption) http.Handler {
	t.Helper()
	db := newTestDBFromStorage(t)
	enc, err := vault.NewEncryptor(testVaultKey)
	if err != nil {
		t.Fatal(err)
	}
	vaultStore := vault.NewPostgreSQLVaultStore(dbconnect.NewClientForTesting(db))
	credStore := vault.NewPostgreSQLCredentialStore(dbconnect.NewClientForTesting(db), enc)
	vaultHandler := httpapi.NewVaultHandler(vault.NewService(vaultStore, credStore, options...))
	return newAuthenticatedRouter(t, newTestHandler(t), httpapi.WithVaultHandler(vaultHandler))
}

func newVaultAuthTestRouter(t *testing.T) (http.Handler, *authTestEnv) {
	t.Helper()
	env := newAuthTestEnv(t)
	enc, err := vault.NewEncryptor(testVaultKey)
	if err != nil {
		t.Fatal(err)
	}
	vaultHandler := httpapi.NewVaultHandler(vault.NewService(
		vault.NewPostgreSQLVaultStore(dbconnect.NewClientForTesting(env.runtime)),
		vault.NewPostgreSQLCredentialStore(dbconnect.NewClientForTesting(env.runtime), enc),
	))
	return env.router(httpapi.WithVaultHandler(vaultHandler)), env
}

func vaultRequest(t *testing.T, router http.Handler, method string, path string, key string, body string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	if key == "" {
		setAuthHeader(request)
	} else {
		request.Header.Set("x-api-key", key)
		addSDKBetaQueryMarker(request)
	}
	router.ServeHTTP(recorder, request)
	return recorder
}

func createVaultViaHTTP(t *testing.T, router http.Handler, key string, body string) string {
	t.Helper()
	recorder := vaultRequest(t, router, http.MethodPost, "/v1/vaults", key, body)
	if recorder.Code != http.StatusOK {
		t.Fatalf("create vault status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var created struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode vault: %v", err)
	}
	if created.ID == "" {
		t.Fatal("create vault response missing id")
	}
	return created.ID
}

func createCredentialViaHTTP(t *testing.T, router http.Handler, key string, vaultID string, body string) string {
	t.Helper()
	recorder := vaultRequest(t, router, http.MethodPost, "/v1/vaults/"+vaultID+"/credentials", key, body)
	if recorder.Code != http.StatusOK {
		t.Fatalf("create credential status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var created struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode credential: %v", err)
	}
	if created.ID == "" {
		t.Fatal("create credential response missing id")
	}
	return created.ID
}

func staticBearerCredentialBody(displayName string, token string) string {
	return staticBearerCredentialBodyWithURL(displayName, "https://"+credentialHostToken(token)+".example.com/mcp", token)
}

func staticBearerCredentialBodyWithURL(displayName string, mcpServerURL string, token string) string {
	return fmt.Sprintf(`{"display_name":%q,"auth":{"type":"static_bearer","mcp_server_url":%q,"token":%q}}`, displayName, mcpServerURL, token)
}

func credentialHostToken(token string) string {
	host := strings.ToLower(token)
	host = strings.NewReplacer("_", "-", ".", "-", ":", "-").Replace(host)
	return host
}

func assertVaultHTTPError(t *testing.T, recorder *httptest.ResponseRecorder, status int, errorType string, forbidden ...string) {
	t.Helper()
	if recorder.Code != status {
		t.Fatalf("status = %d body=%s; want %d", recorder.Code, recorder.Body.String(), status)
	}
	var response struct {
		Error struct {
			Type string `json:"type"`
		} `json:"error"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if response.Error.Type != errorType {
		t.Fatalf("error.type = %q body=%s; want %q", response.Error.Type, recorder.Body.String(), errorType)
	}
	for _, value := range forbidden {
		if value != "" && strings.Contains(recorder.Body.String(), value) {
			t.Fatalf("error response leaked %q: %s", value, recorder.Body.String())
		}
	}
}

func assertProviderCredentialHTTPBody(t *testing.T, body string, wantType string, wantProviderID string, wantAccessMode string, wantExpiresAt string, wantAccountID string, forbidden ...string) {
	t.Helper()
	var response struct {
		Auth map[string]any `json:"auth"`
	}
	if err := json.Unmarshal([]byte(body), &response); err != nil {
		t.Fatalf("decode provider credential body: %v", err)
	}
	if response.Auth["type"] != wantType ||
		response.Auth["provider_id"] != wantProviderID ||
		response.Auth["access_mode"] != wantAccessMode {
		t.Fatalf("auth = %+v; want type/provider/access_mode %q/%q/%q", response.Auth, wantType, wantProviderID, wantAccessMode)
	}
	if wantExpiresAt != "" && response.Auth["expires_at"] != wantExpiresAt {
		t.Fatalf("auth.expires_at = %v; want %q", response.Auth["expires_at"], wantExpiresAt)
	}
	if wantAccountID != "" && response.Auth["account_id"] != wantAccountID {
		t.Fatalf("auth.account_id = %v; want %q", response.Auth["account_id"], wantAccountID)
	}
	for _, field := range []string{"token", "access_token", "refresh_token"} {
		if _, ok := response.Auth[field]; ok {
			t.Fatalf("public provider auth must not include secret field %q: %s", field, body)
		}
	}
	for _, value := range forbidden {
		if value != "" && strings.Contains(body, value) {
			t.Fatalf("provider credential response leaked %q: %s", value, body)
		}
	}
}

func listVaultIDsViaHTTP(t *testing.T, router http.Handler, key string, target string) []string {
	t.Helper()
	recorder := vaultRequest(t, router, http.MethodGet, target, key, "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("list vaults status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode vault list: %v", err)
	}
	ids := make([]string, 0, len(response.Data))
	for _, item := range response.Data {
		ids = append(ids, item.ID)
	}
	return ids
}

func listCredentialIDsViaHTTP(t *testing.T, router http.Handler, key string, target string) []string {
	t.Helper()
	recorder := vaultRequest(t, router, http.MethodGet, target, key, "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("list credentials status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode credential list: %v", err)
	}
	ids := make([]string, 0, len(response.Data))
	for _, item := range response.Data {
		ids = append(ids, item.ID)
	}
	return ids
}

type credentialListHTTPPage struct {
	IDs      []string
	NextPage *string
}

func listCredentialPageViaHTTP(t *testing.T, router http.Handler, key string, target string) credentialListHTTPPage {
	t.Helper()
	recorder := vaultRequest(t, router, http.MethodGet, target, key, "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("list credentials status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var topLevel map[string]json.RawMessage
	if err := json.Unmarshal(recorder.Body.Bytes(), &topLevel); err != nil {
		t.Fatalf("decode credential list keys: %v", err)
	}
	if _, ok := topLevel["data"]; !ok {
		t.Fatalf("credential list response missing data: %s", recorder.Body.String())
	}
	if _, ok := topLevel["next_page"]; !ok {
		t.Fatalf("credential list response missing next_page: %s", recorder.Body.String())
	}
	for _, removedKey := range []string{"has_more", "after_id", "before_id"} {
		if _, ok := topLevel[removedKey]; ok {
			t.Fatalf("credential list response included removed %s: %s", removedKey, recorder.Body.String())
		}
	}
	var response struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
		NextPage *string `json:"next_page"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode credential list: %v", err)
	}
	ids := make([]string, 0, len(response.Data))
	for _, item := range response.Data {
		ids = append(ids, item.ID)
	}
	return credentialListHTTPPage{IDs: ids, NextPage: response.NextPage}
}

func requireIDs(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("ids = %v; want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ids = %v; want %v", got, want)
		}
	}
}

type recordingVaultStore struct {
	listOptions vault.ListOptions
	listCalled  bool
	listResult  vault.VaultListResult
	getCalled   bool
	getVaultID  string
	getErr      error
}

func (s *recordingVaultStore) Create(context.Context, workspace.ID, vault.CreateVaultRequest) (*vault.Vault, error) {
	return nil, nil
}

func (s *recordingVaultStore) Get(_ context.Context, _ workspace.ID, vaultID string) (*vault.Vault, error) {
	s.getCalled = true
	s.getVaultID = vaultID
	if s.getErr != nil {
		return nil, s.getErr
	}
	return &vault.Vault{ID: "vlt_test", Type: "vault"}, nil
}

func (s *recordingVaultStore) List(_ context.Context, _ workspace.ID, options vault.ListOptions) (vault.VaultListResult, error) {
	s.listCalled = true
	s.listOptions = options
	return s.listResult, nil
}

func (s *recordingVaultStore) Update(context.Context, workspace.ID, string, vault.VaultPatch) (*vault.Vault, error) {
	return &vault.Vault{ID: "vlt_test", Type: "vault", DisplayName: "updated"}, nil
}

func (s *recordingVaultStore) Archive(context.Context, workspace.ID, string) (*vault.Vault, error) {
	now := time.Now().UTC()
	return &vault.Vault{ID: "vlt_test", Type: "vault", DisplayName: "archived", ArchivedAt: &now}, nil
}

func (s *recordingVaultStore) Delete(_ context.Context, _ workspace.ID, vaultID string) (*vault.DeleteResult, error) {
	return &vault.DeleteResult{ID: vaultID, Type: "vault_deleted"}, nil
}

type recordingCredentialStore struct {
	listOptions  vault.ListOptions
	listCalled   bool
	listVaultID  string
	listResult   vault.CredentialListResult
	createCalled bool
}

func (s *recordingCredentialStore) Create(context.Context, workspace.ID, string, vault.CreateCredentialRequest) (*vault.CredentialMetadata, error) {
	s.createCalled = true
	return nil, nil
}

func (s *recordingCredentialStore) GetMetadata(context.Context, workspace.ID, string, string) (*vault.CredentialMetadata, error) {
	return nil, nil
}

func (s *recordingCredentialStore) GetSecret(context.Context, workspace.ID, string, string) (*vault.CredentialMetadata, *vault.CredentialAuth, error) {
	return &vault.CredentialMetadata{ID: "cred_test", Type: "vault_credential", VaultID: "vlt_test"}, &vault.CredentialAuth{Type: "mcp_oauth", MCPServerURL: "https://example.com/mcp", AccessToken: "oauth-access"}, nil
}

func (s *recordingCredentialStore) List(_ context.Context, _ workspace.ID, vaultID string, options vault.ListOptions) (vault.CredentialListResult, error) {
	s.listCalled = true
	s.listVaultID = vaultID
	s.listOptions = options
	return s.listResult, nil
}

func (s *recordingCredentialStore) Update(context.Context, workspace.ID, string, string, vault.CredentialPatch) (*vault.CredentialMetadata, error) {
	return &vault.CredentialMetadata{ID: "cred_test", Type: "vault_credential", VaultID: "vlt_test", Auth: vault.CredentialAuthPublic{Type: "static_bearer", MCPServerURL: "https://example.com/mcp"}}, nil
}

func (s *recordingCredentialStore) UpdateWithLockedCredential(context.Context, workspace.ID, string, string, vault.LockedCredentialPatchFunc) (*vault.CredentialMetadata, error) {
	return &vault.CredentialMetadata{ID: "cred_test", Type: "vault_credential", VaultID: "vlt_test", Auth: vault.CredentialAuthPublic{Type: "mcp_oauth", MCPServerURL: "https://example.com/mcp"}}, nil
}

func (s *recordingCredentialStore) Archive(context.Context, workspace.ID, string, string) (*vault.CredentialMetadata, error) {
	now := time.Now().UTC()
	return &vault.CredentialMetadata{ID: "cred_test", Type: "vault_credential", VaultID: "vlt_test", ArchivedAt: &now, Auth: vault.CredentialAuthPublic{Type: "static_bearer", MCPServerURL: "https://example.com/mcp"}}, nil
}

func (s *recordingCredentialStore) Delete(_ context.Context, _ workspace.ID, _ string, credentialID string) (*vault.DeleteResult, error) {
	return &vault.DeleteResult{ID: credentialID, Type: "vault_credential_deleted"}, nil
}

type recordingMCPOAuthValidator struct {
	called bool
	input  vault.MCPOAuthValidationInput
	result vault.MCPOAuthValidationResult
	err    error
}

func (v *recordingMCPOAuthValidator) Validate(_ context.Context, input vault.MCPOAuthValidationInput) (vault.MCPOAuthValidationResult, error) {
	v.called = true
	v.input = input
	if v.result.Validation != nil || v.err != nil {
		return v.result, v.err
	}
	return vault.MCPOAuthValidationResult{
		Validation: &vault.CredentialValidation{
			Type:            "vault_credential_validation",
			CredentialID:    input.Credential.ID,
			VaultID:         input.Credential.VaultID,
			Status:          "valid",
			ValidatedAt:     time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
			HasRefreshToken: input.Auth.Refresh != nil && input.Auth.Refresh.RefreshToken != "",
			Refresh:         &vault.CredentialValidationCheck{Status: "succeeded", HTTPResponse: &vault.CredentialValidationHTTPResponse{StatusCode: 200}},
			MCPProbe:        &vault.CredentialValidationCheck{Method: "initialize", HTTPResponse: &vault.CredentialValidationHTTPResponse{StatusCode: 200}},
		},
	}, nil
}

func newVaultHandlerRouterForListTests(t *testing.T, vaults *recordingVaultStore, credentials *recordingCredentialStore) http.Handler {
	t.Helper()
	vaultHandler := httpapi.NewVaultHandler(vault.NewService(vaults, credentials))
	authenticator := auth.AuthenticatorFunc(func(context.Context, string) (auth.Principal, error) {
		return auth.Principal{Workspace: workspace.Workspace{ID: workspace.DefaultID}, APIKeyID: "ak_test"}, nil
	})
	return httpapi.NewRouter(nil, "", httpapi.WithAuthenticator(authenticator), httpapi.WithVaultHandler(vaultHandler))
}

func TestCreateVaultValid(t *testing.T) {
	router := newVaultTestRouter(t)

	body := `{"display_name":"my vault"}`
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/vaults", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	setAuthHeader(request)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", recorder.Code, recorder.Body.String())
	}

	var response map[string]any
	_ = json.Unmarshal(recorder.Body.Bytes(), &response)
	id, _ := response["id"].(string)
	if !strings.HasPrefix(id, "vlt_") {
		t.Errorf("expected vlt_ prefix, got %q", id)
	}
}

func TestListVaultsUsesPageOptionsAndNextPageEnvelope(t *testing.T) {
	nextPage := "next-token"
	vaults := &recordingVaultStore{
		listResult: vault.VaultListResult{
			Data:     []*vault.Vault{{ID: "vlt_one", Type: "vault"}},
			NextPage: &nextPage,
		},
	}
	credentials := &recordingCredentialStore{}
	router := newVaultHandlerRouterForListTests(t, vaults, credentials)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/vaults?limit=7&page=current-token&include_archived=true", nil)
	setAuthHeader(request)
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", recorder.Code, recorder.Body.String())
	}
	if !vaults.listCalled {
		t.Fatal("vault list store was not called")
	}
	if vaults.listOptions.Limit != 7 || vaults.listOptions.Page != "current-token" || !vaults.listOptions.IncludeArchived {
		t.Fatalf("vault list options = %+v", vaults.listOptions)
	}
	var response map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response["next_page"] != nextPage {
		t.Fatalf("next_page = %v; want %q", response["next_page"], nextPage)
	}
	if _, ok := response["has"+"_"+"more"]; ok {
		t.Fatal("list response included removed cursor boolean")
	}
}

func TestListVaultsRejectsMalformedPageQuery(t *testing.T) {
	cases := []string{
		"/v1/vaults?limit=0",
		"/v1/vaults?limit=abc",
		"/v1/vaults?page=",
		"/v1/vaults?include_archived=maybe",
		"/v1/vaults?cursor=vlt_one",
		"/v1/vaults?limit=1&limit=2",
	}
	for _, target := range cases {
		t.Run(target, func(t *testing.T) {
			vaults := &recordingVaultStore{}
			router := newVaultHandlerRouterForListTests(t, vaults, &recordingCredentialStore{})

			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, target, nil)
			setAuthHeader(request)
			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d: %s", recorder.Code, recorder.Body.String())
			}
			if vaults.listCalled {
				t.Fatal("store must not be called for invalid query")
			}
		})
	}
}

func TestVaultCreateUpdateStrictBodyAndRequestTooLarge(t *testing.T) {
	router := newVaultTestRouter(t)

	cases := []struct {
		name string
		body string
	}{
		{name: "unknown", body: `{"display_name":"x","name":"old"}`},
		{name: "server managed", body: `{"display_name":"x","id":"vlt_user"}`},
		{name: "trailing", body: `{"display_name":"x"}{"display_name":"y"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/v1/vaults", strings.NewReader(tc.body))
			request.Header.Set("Content-Type", "application/json")
			setAuthHeader(request)
			router.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d body=%s; want 400", recorder.Code, recorder.Body.String())
			}
		})
	}

	oversized := `{"display_name":"` + strings.Repeat("x", 1<<20) + `"}`
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/vaults", strings.NewReader(oversized))
	request.Header.Set("Content-Type", "application/json")
	setAuthHeader(request)
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d body=%s; want 413", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), `"request_too_large"`) {
		t.Fatalf("request too large response = %s", recorder.Body.String())
	}
}

func TestGetVaultExisting(t *testing.T) {
	router := newVaultTestRouter(t)

	createRec := httptest.NewRecorder()
	createReq := httptest.NewRequest(http.MethodPost, "/v1/vaults", strings.NewReader(`{"display_name":"test"}`))
	createReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(createReq)
	router.ServeHTTP(createRec, createReq)

	var created map[string]any
	_ = json.Unmarshal(createRec.Body.Bytes(), &created)
	vaultID := created["id"].(string)

	getRec := httptest.NewRecorder()
	getReq := httptest.NewRequest(http.MethodGet, "/v1/vaults/"+vaultID, nil)
	setAuthHeader(getReq)
	router.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", getRec.Code)
	}
}

func TestDeleteVault(t *testing.T) {
	router := newVaultTestRouter(t)

	createRec := httptest.NewRecorder()
	createReq := httptest.NewRequest(http.MethodPost, "/v1/vaults", strings.NewReader(`{"display_name":"del"}`))
	createReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(createReq)
	router.ServeHTTP(createRec, createReq)

	var created map[string]any
	_ = json.Unmarshal(createRec.Body.Bytes(), &created)
	vaultID := created["id"].(string)

	delRec := httptest.NewRecorder()
	delReq := httptest.NewRequest(http.MethodDelete, "/v1/vaults/"+vaultID, nil)
	setAuthHeader(delReq)
	router.ServeHTTP(delRec, delReq)

	if delRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", delRec.Code, delRec.Body.String())
	}
	var deleted vault.DeleteResult
	if err := json.Unmarshal(delRec.Body.Bytes(), &deleted); err != nil {
		t.Fatalf("decode delete response: %v", err)
	}
	if deleted.ID != vaultID || deleted.Type != "vault_deleted" {
		t.Fatalf("delete response = %+v", deleted)
	}

	getRec := httptest.NewRecorder()
	getReq := httptest.NewRequest(http.MethodGet, "/v1/vaults/"+vaultID, nil)
	setAuthHeader(getReq)
	router.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", getRec.Code)
	}
}

func TestUpdateAndArchiveVault(t *testing.T) {
	router := newVaultTestRouter(t)

	createRec := httptest.NewRecorder()
	createReq := httptest.NewRequest(http.MethodPost, "/v1/vaults", strings.NewReader(`{"display_name":"before","metadata":{"keep":"old"}}`))
	createReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(createReq)
	router.ServeHTTP(createRec, createReq)
	var created map[string]any
	_ = json.Unmarshal(createRec.Body.Bytes(), &created)
	vaultID := created["id"].(string)

	updateRec := httptest.NewRecorder()
	updateReq := httptest.NewRequest(http.MethodPost, "/v1/vaults/"+vaultID, strings.NewReader(`{"display_name":"after","metadata":{"keep":"new","added":"yes"}}`))
	updateReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(updateReq)
	router.ServeHTTP(updateRec, updateReq)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("update status = %d body=%s", updateRec.Code, updateRec.Body.String())
	}
	var updated vault.Vault
	if err := json.Unmarshal(updateRec.Body.Bytes(), &updated); err != nil {
		t.Fatalf("decode updated vault: %v", err)
	}
	if updated.DisplayName != "after" || updated.Metadata["keep"] != "new" || updated.Metadata["added"] != "yes" {
		t.Fatalf("updated vault = %+v", updated)
	}

	clearNameRec := vaultRequest(t, router, http.MethodPost, "/v1/vaults/"+vaultID, "", `{"display_name":null}`)
	if clearNameRec.Code != http.StatusOK {
		t.Fatalf("clear display_name status = %d body=%s", clearNameRec.Code, clearNameRec.Body.String())
	}
	var cleared vault.Vault
	if err := json.Unmarshal(clearNameRec.Body.Bytes(), &cleared); err != nil {
		t.Fatalf("decode display_name-cleared vault: %v", err)
	}
	if cleared.DisplayName != "" {
		t.Fatalf("null display_name must clear vault display name, got %q", cleared.DisplayName)
	}

	archiveRec := httptest.NewRecorder()
	archiveReq := httptest.NewRequest(http.MethodPost, "/v1/vaults/"+vaultID+"/archive", nil)
	setAuthHeader(archiveReq)
	router.ServeHTTP(archiveRec, archiveReq)
	if archiveRec.Code != http.StatusOK {
		t.Fatalf("archive status = %d body=%s", archiveRec.Code, archiveRec.Body.String())
	}
	var archived vault.Vault
	if err := json.Unmarshal(archiveRec.Body.Bytes(), &archived); err != nil {
		t.Fatalf("decode archived vault: %v", err)
	}
	if archived.ID != vaultID || archived.ArchivedAt == nil {
		t.Fatalf("archived vault = %+v", archived)
	}
}

func TestVaultAndCredentialUpdateStrictBodyAndRequestTooLarge(t *testing.T) {
	router := newVaultTestRouter(t)

	vaultID := createVaultViaHTTP(t, router, "", `{"display_name":"strict-update-vault"}`)
	credentialID := createCredentialViaHTTP(t, router, "", vaultID, staticBearerCredentialBody("strict-update-key", "sk-strict-update"))

	overlongDisplayName := strings.Repeat("x", 256)
	vaultUpdateCases := []struct {
		name string
		body string
	}{
		{name: "unknown", body: `{"display_name":"x","name":"old"}`},
		{name: "server managed", body: `{"display_name":"x","archived_at":"2026-01-01T00:00:00Z"}`},
		{name: "wrong metadata shape", body: `{"metadata":[]}`},
		{name: "trailing", body: `{"display_name":"x"}{"display_name":"y"}`},
		{name: "display name limit", body: `{"display_name":"` + overlongDisplayName + `"}`},
	}
	for _, tc := range vaultUpdateCases {
		t.Run("vault update "+tc.name, func(t *testing.T) {
			recorder := vaultRequest(t, router, http.MethodPost, "/v1/vaults/"+vaultID, "", tc.body)
			assertVaultHTTPError(t, recorder, http.StatusBadRequest, "invalid_request_error")
		})
	}

	oversized := `{"display_name":"` + strings.Repeat("x", 1<<20) + `"}`
	recorder := vaultRequest(t, router, http.MethodPost, "/v1/vaults/"+vaultID, "", oversized)
	assertVaultHTTPError(t, recorder, http.StatusRequestEntityTooLarge, "request_too_large")

	recorder = vaultRequest(t, router, http.MethodPost, "/v1/vaults/"+vaultID+"/credentials", "", oversized)
	assertVaultHTTPError(t, recorder, http.StatusRequestEntityTooLarge, "request_too_large")

	const nestedUpdateSecret = "nested-update-secret" //nolint:gosec // G101: synthetic test sentinel, not a real secret
	credentialUpdateCases := []struct {
		name      string
		body      string
		forbidden []string
	}{
		{name: "unknown", body: `{"display_name":"x","credential_id":"cred_user"}`},
		{name: "server managed", body: `{"display_name":"x","archived_at":"2026-01-01T00:00:00Z"}`},
		{name: "null auth", body: `{"auth":null}`},
		{name: "trailing", body: `{"display_name":"x"} trailing`},
		{name: "nested unknown", body: `{"auth":{"type":"mcp_oauth","refresh":{"token_endpoint_auth":{"type":"client_secret_basic","client_secret":"` + nestedUpdateSecret + `","extra":"nope"}}}}`, forbidden: []string{nestedUpdateSecret}},
		{name: "display name limit", body: `{"display_name":"` + overlongDisplayName + `"}`},
	}
	for _, tc := range credentialUpdateCases {
		t.Run("credential update "+tc.name, func(t *testing.T) {
			recorder := vaultRequest(t, router, http.MethodPost, "/v1/vaults/"+vaultID+"/credentials/"+credentialID, "", tc.body)
			assertVaultHTTPError(t, recorder, http.StatusBadRequest, "invalid_request_error", tc.forbidden...)
		})
	}

	recorder = vaultRequest(t, router, http.MethodPost, "/v1/vaults/"+vaultID+"/credentials/"+credentialID, "", oversized)
	assertVaultHTTPError(t, recorder, http.StatusRequestEntityTooLarge, "request_too_large")
}

func TestCreateCredentialValid(t *testing.T) {
	router := newVaultTestRouter(t)

	// Create vault first.
	createVaultRec := httptest.NewRecorder()
	createVaultReq := httptest.NewRequest(http.MethodPost, "/v1/vaults", strings.NewReader(`{"display_name":"cred-vault"}`))
	createVaultReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(createVaultReq)
	router.ServeHTTP(createVaultRec, createVaultReq)

	var vaultObj map[string]any
	_ = json.Unmarshal(createVaultRec.Body.Bytes(), &vaultObj)
	vaultID := vaultObj["id"].(string)

	body := staticBearerCredentialBody("LLM Key", "sk-real")
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/vaults/"+vaultID+"/credentials", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	setAuthHeader(request)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", recorder.Code, recorder.Body.String())
	}

	var response map[string]any
	_ = json.Unmarshal(recorder.Body.Bytes(), &response)
	credID, _ := response["id"].(string)
	if !strings.HasPrefix(credID, "cred_") {
		t.Errorf("expected cred_ prefix, got %q", credID)
	}
}

func TestCreateCredentialRejectsWrongVariantFieldsWithoutEcho(t *testing.T) {
	router := newVaultTestRouter(t)

	createVaultRec := httptest.NewRecorder()
	createVaultReq := httptest.NewRequest(http.MethodPost, "/v1/vaults", strings.NewReader(`{"display_name":"variant-vault"}`))
	createVaultReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(createVaultReq)
	router.ServeHTTP(createVaultRec, createVaultReq)
	var vaultObj map[string]any
	_ = json.Unmarshal(createVaultRec.Body.Bytes(), &vaultObj)
	vaultID := vaultObj["id"].(string)

	const wrongVariantSecret = "wrong-variant-secret"
	const staticBearerSecret = "sk-wrong-variant-test"
	body := `{"display_name":"LLM Key","auth":{"type":"static_bearer","mcp_server_url":"https://example.com/mcp","token":"` + staticBearerSecret + `","access_token":"` + wrongVariantSecret + `"}}` //nolint:gosec // G101: synthetic test sentinels
	createCredRec := httptest.NewRecorder()
	createCredReq := httptest.NewRequest(http.MethodPost, "/v1/vaults/"+vaultID+"/credentials", strings.NewReader(body))
	createCredReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(createCredReq)
	router.ServeHTTP(createCredRec, createCredReq)
	if createCredRec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", createCredRec.Code, createCredRec.Body.String())
	}
	createBody := createCredRec.Body.String()
	if strings.Contains(createBody, staticBearerSecret) || strings.Contains(createBody, wrongVariantSecret) {
		t.Fatalf("create error leaked wrong-variant or secret field: %s", createBody)
	}
}

func TestCredentialCreateStrictBodyAndConflict(t *testing.T) {
	router := newVaultTestRouter(t)

	createVaultRec := httptest.NewRecorder()
	createVaultReq := httptest.NewRequest(http.MethodPost, "/v1/vaults", strings.NewReader(`{"display_name":"strict-cred-vault"}`))
	createVaultReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(createVaultReq)
	router.ServeHTTP(createVaultRec, createVaultReq)
	var vaultObj map[string]any
	_ = json.Unmarshal(createVaultRec.Body.Bytes(), &vaultObj)
	vaultID := vaultObj["id"].(string)

	strictCases := []string{
		`{"display_name":"x","auth":{"type":"static_bearer","mcp_server_url":"https://strict.example.com/mcp","token":"sk-x","access_token":"old"}}`,
		`{"display_name":"x","type":"credential","auth":{"type":"static_bearer","mcp_server_url":"https://strict.example.com/mcp","token":"sk-x"}}`,
		`{"display_name":"x","auth":{"type":"mcp_oauth","mcp_server_url":"https://example.com/mcp","access_token":"oauth","refresh":{"refresh_token":"r","client_id":"c","token_endpoint":"https://auth.example.com/token","token_endpoint_auth":{"type":"none","extra":"nope"}}}}`,
		`{"display_name":"x","auth":{"type":"static_bearer","mcp_server_url":"https://strict.example.com/mcp","token":"sk-x"}} trailing`,
	}
	for _, body := range strictCases {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/vaults/"+vaultID+"/credentials", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		setAuthHeader(req)
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("body %s status = %d response=%s; want 400", body, rec.Code, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), "sk-x") || strings.Contains(rec.Body.String(), "old") || strings.Contains(rec.Body.String(), "nope") {
			t.Fatalf("strict error leaked secret: %s", rec.Body.String())
		}
	}

	first := `{"display_name":"mcp-one","auth":{"type":"static_bearer","mcp_server_url":"https://dup.example.com/mcp","token":"first-token"}}`
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/vaults/"+vaultID+"/credentials", strings.NewReader(first))
	req.Header.Set("Content-Type", "application/json")
	setAuthHeader(req)
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("first create status = %d body=%s", rec.Code, rec.Body.String())
	}
	duplicate := `{"display_name":"mcp-two","auth":{"type":"static_bearer","mcp_server_url":"https://dup.example.com/mcp","token":"second-token"}}`
	dupRec := httptest.NewRecorder()
	dupReq := httptest.NewRequest(http.MethodPost, "/v1/vaults/"+vaultID+"/credentials", strings.NewReader(duplicate))
	dupReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(dupReq)
	router.ServeHTTP(dupRec, dupReq)
	if dupRec.Code != http.StatusConflict {
		t.Fatalf("duplicate status = %d body=%s; want 409", dupRec.Code, dupRec.Body.String())
	}
	if !strings.Contains(dupRec.Body.String(), `"conflict_error"`) {
		t.Fatalf("duplicate response = %s", dupRec.Body.String())
	}
	if strings.Contains(dupRec.Body.String(), "second-token") {
		t.Fatalf("duplicate response leaked secret: %s", dupRec.Body.String())
	}
}

func TestGetCredentialWriteOnly(t *testing.T) {
	router := newVaultTestRouter(t)

	// Create vault + credential.
	createVaultRec := httptest.NewRecorder()
	createVaultReq := httptest.NewRequest(http.MethodPost, "/v1/vaults", strings.NewReader(`{"display_name":"wo-vault"}`))
	createVaultReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(createVaultReq)
	router.ServeHTTP(createVaultRec, createVaultReq)

	var vaultObj map[string]any
	_ = json.Unmarshal(createVaultRec.Body.Bytes(), &vaultObj)
	vaultID := vaultObj["id"].(string)

	credBody := staticBearerCredentialBody("LLM Key", "sk-secret-123")
	createCredRec := httptest.NewRecorder()
	createCredReq := httptest.NewRequest(http.MethodPost, "/v1/vaults/"+vaultID+"/credentials", strings.NewReader(credBody))
	createCredReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(createCredReq)
	router.ServeHTTP(createCredRec, createCredReq)

	var credObj map[string]any
	_ = json.Unmarshal(createCredRec.Body.Bytes(), &credObj)
	credID := credObj["id"].(string)

	// GET credential — must NOT contain secret fields.
	getRec := httptest.NewRecorder()
	getReq := httptest.NewRequest(http.MethodGet, "/v1/vaults/"+vaultID+"/credentials/"+credID, nil)
	setAuthHeader(getReq)
	router.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", getRec.Code)
	}

	var getResult map[string]any
	_ = json.Unmarshal(getRec.Body.Bytes(), &getResult)
	if getResult["display_name"] != "LLM Key" {
		t.Errorf("expected display_name 'LLM Key', got %v", getResult["display_name"])
	}

	auth, _ := getResult["auth"].(map[string]any)
	if auth["type"] != "static_bearer" {
		t.Errorf("expected auth.type 'static_bearer', got %v", auth["type"])
	}
	if _, hasAccessToken := auth["access_token"]; hasAccessToken {
		t.Error("access_token must NOT appear in GET response")
	}
	if _, hasRefreshToken := auth["refresh_token"]; hasRefreshToken {
		t.Error("refresh_token must NOT appear in GET response")
	}
	if _, hasClientSecret := auth["client_secret"]; hasClientSecret {
		t.Error("client_secret must NOT appear in GET response")
	}
}

func TestListCredentials(t *testing.T) {
	router := newVaultTestRouter(t)

	createVaultRec := httptest.NewRecorder()
	createVaultReq := httptest.NewRequest(http.MethodPost, "/v1/vaults", strings.NewReader(`{"display_name":"list-vault"}`))
	createVaultReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(createVaultReq)
	router.ServeHTTP(createVaultRec, createVaultReq)

	var vaultObj map[string]any
	_ = json.Unmarshal(createVaultRec.Body.Bytes(), &vaultObj)
	vaultID := vaultObj["id"].(string)

	for i := 0; i < 2; i++ {
		body := staticBearerCredentialBody("cred"+string(rune('a'+i)), "sk-list-"+string(rune('a'+i)))
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/vaults/"+vaultID+"/credentials", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		setAuthHeader(req)
		router.ServeHTTP(rec, req)
	}

	listRec := httptest.NewRecorder()
	listReq := httptest.NewRequest(http.MethodGet, "/v1/vaults/"+vaultID+"/credentials", nil)
	setAuthHeader(listReq)
	router.ServeHTTP(listRec, listReq)

	if listRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", listRec.Code)
	}

	var response struct {
		Data []any `json:"data"`
	}
	_ = json.Unmarshal(listRec.Body.Bytes(), &response)
	if len(response.Data) != 2 {
		t.Errorf("expected 2 credentials, got %d", len(response.Data))
	}
}

func TestListCredentialsUsesParentBoundPageOptionsAndNextPageEnvelope(t *testing.T) {
	nextPage := "credential-token"
	vaults := &recordingVaultStore{}
	credentials := &recordingCredentialStore{
		listResult: vault.CredentialListResult{
			Data: []*vault.Credential{{
				ID:      "cred_one",
				Type:    "vault_credential",
				VaultID: "vlt_parent",
				Auth:    vault.CredentialAuthPublic{Type: "static_bearer", MCPServerURL: "https://example.com/mcp"},
			}},
			NextPage: &nextPage,
		},
	}
	router := newVaultHandlerRouterForListTests(t, vaults, credentials)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/vaults/vlt_parent/credentials?limit=3&page=current-token", nil)
	setAuthHeader(request)
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", recorder.Code, recorder.Body.String())
	}
	if !credentials.listCalled {
		t.Fatal("credential list store was not called")
	}
	if credentials.listVaultID != "vlt_parent" {
		t.Fatalf("credential list vault id = %q", credentials.listVaultID)
	}
	if credentials.listOptions.Limit != 3 || credentials.listOptions.Page != "current-token" || credentials.listOptions.IncludeArchived {
		t.Fatalf("credential list options = %+v", credentials.listOptions)
	}
	var response map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response["next_page"] != nextPage {
		t.Fatalf("next_page = %v; want %q", response["next_page"], nextPage)
	}
	if _, ok := response["has"+"_"+"more"]; ok {
		t.Fatal("credential list response included removed cursor boolean")
	}
}

func TestListCredentialsRejectsMalformedQueryBeforeParentLookup(t *testing.T) {
	cases := []struct {
		name   string
		target string
	}{
		{name: "non numeric limit", target: "/v1/vaults/vlt_parent/credentials?limit=abc"},
		{name: "non positive limit", target: "/v1/vaults/vlt_parent/credentials?limit=0"},
		{name: "duplicate limit", target: "/v1/vaults/vlt_parent/credentials?limit=1&limit=2"},
		{name: "unknown parameter", target: "/v1/vaults/vlt_parent/credentials?cursor=cred_one"},
		{name: "empty page", target: "/v1/vaults/vlt_parent/credentials?page="},
		{name: "invalid include archived", target: "/v1/vaults/vlt_parent/credentials?include_archived=maybe"},
	}

	parentStates := []struct {
		name   string
		getErr error
	}{
		{name: "existing parent"},
		{name: "missing parent", getErr: &vault.NotFoundError{Message: "vault vlt_parent not found"}},
	}

	for _, parentState := range parentStates {
		t.Run(parentState.name, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					vaults := &recordingVaultStore{getErr: parentState.getErr}
					credentials := &recordingCredentialStore{}
					router := newVaultHandlerRouterForListTests(t, vaults, credentials)

					recorder := httptest.NewRecorder()
					request := httptest.NewRequest(http.MethodGet, tc.target, nil)
					setAuthHeader(request)
					router.ServeHTTP(recorder, request)

					assertVaultHTTPError(t, recorder, http.StatusBadRequest, "invalid_request_error")
					if vaults.getCalled {
						t.Fatal("parent vault lookup ran before list query validation")
					}
					if credentials.listCalled {
						t.Fatal("credential list store was called after rejected query")
					}
				})
			}
		})
	}
}

func TestListCredentialsPaginationUsesRealAuthenticatedRoute(t *testing.T) {
	router, env := newVaultAuthTestRouter(t)
	vaultID := createVaultViaHTTP(t, router, env.envKey, `{"display_name":"credential-pagination"}`)
	otherVaultID := createVaultViaHTTP(t, router, env.envKey, `{"display_name":"other-credential-pagination"}`)
	created := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		credentialID := createCredentialViaHTTP(t, router, env.envKey, vaultID, fmt.Sprintf(
			`{"display_name":"credential-%d","auth":{"type":"static_bearer","mcp_server_url":"https://credential-page-%d.example.com/mcp","token":"sk-page-%d"}}`,
			i,
			i,
			i,
		))
		created = append(created, credentialID)
	}
	archiveRecorder := vaultRequest(t, router, http.MethodPost, "/v1/vaults/"+vaultID+"/credentials/"+created[1]+"/archive", env.envKey, "")
	if archiveRecorder.Code != http.StatusOK {
		t.Fatalf("archive credential status = %d body=%s", archiveRecorder.Code, archiveRecorder.Body.String())
	}

	first := listCredentialPageViaHTTP(t, router, env.envKey, "/v1/vaults/"+vaultID+"/credentials?limit=1")
	requireIDs(t, first.IDs, created[0])
	if first.NextPage == nil {
		t.Fatal("first credential page must return next_page")
	}
	second := listCredentialPageViaHTTP(t, router, env.envKey, "/v1/vaults/"+vaultID+"/credentials?limit=1&page="+*first.NextPage)
	requireIDs(t, second.IDs, created[2])
	if second.NextPage != nil {
		t.Fatalf("second credential page next_page = %q; want nil", *second.NextPage)
	}
	withArchived := listCredentialPageViaHTTP(t, router, env.envKey, "/v1/vaults/"+vaultID+"/credentials?include_archived=true")
	requireIDs(t, withArchived.IDs, created[0], created[1], created[2])

	changedParentRecorder := vaultRequest(t, router, http.MethodGet, "/v1/vaults/"+otherVaultID+"/credentials?limit=1&page="+*first.NextPage, env.envKey, "")
	assertVaultHTTPError(t, changedParentRecorder, http.StatusBadRequest, "invalid_request_error")
	changedFilterRecorder := vaultRequest(t, router, http.MethodGet, "/v1/vaults/"+vaultID+"/credentials?limit=1&page="+*first.NextPage+"&include_archived=true", env.envKey, "")
	assertVaultHTTPError(t, changedFilterRecorder, http.StatusBadRequest, "invalid_request_error")
}

func TestVaultAndCredentialHTTPArchivedFilteringAndWorkspaceIsolation(t *testing.T) {
	router, env := newVaultAuthTestRouter(t)

	liveVaultID := createVaultViaHTTP(t, router, env.envKey, `{"display_name":"live-vault"}`)
	archivedVaultID := createVaultViaHTTP(t, router, env.envKey, `{"display_name":"archived-vault"}`)
	archiveVaultRec := vaultRequest(t, router, http.MethodPost, "/v1/vaults/"+archivedVaultID+"/archive", env.envKey, "")
	if archiveVaultRec.Code != http.StatusOK {
		t.Fatalf("archive vault status = %d body=%s", archiveVaultRec.Code, archiveVaultRec.Body.String())
	}

	requireIDs(t, listVaultIDsViaHTTP(t, router, env.envKey, "/v1/vaults"), liveVaultID)
	requireIDs(t, listVaultIDsViaHTTP(t, router, env.envKey, "/v1/vaults?include_archived=true"), liveVaultID, archivedVaultID)

	credentialVaultID := createVaultViaHTTP(t, router, env.envKey, `{"display_name":"credential-list-vault"}`)
	activeCredentialID := createCredentialViaHTTP(t, router, env.envKey, credentialVaultID, staticBearerCredentialBody("active-credential", "sk-active-list"))
	archivedCredentialID := createCredentialViaHTTP(t, router, env.envKey, credentialVaultID, staticBearerCredentialBody("archived-credential", "sk-archived-list"))
	archiveCredentialRec := vaultRequest(t, router, http.MethodPost, "/v1/vaults/"+credentialVaultID+"/credentials/"+archivedCredentialID+"/archive", env.envKey, "")
	if archiveCredentialRec.Code != http.StatusOK {
		t.Fatalf("archive credential status = %d body=%s", archiveCredentialRec.Code, archiveCredentialRec.Body.String())
	}

	requireIDs(t, listCredentialIDsViaHTTP(t, router, env.envKey, "/v1/vaults/"+credentialVaultID+"/credentials"), activeCredentialID)
	requireIDs(t, listCredentialIDsViaHTTP(t, router, env.envKey, "/v1/vaults/"+credentialVaultID+"/credentials?include_archived=true"), activeCredentialID, archivedCredentialID)

	env.seedWorkspace(t, "workspace_b", "B")
	workspaceBKey, err := env.store.CreateForWorkspace(defaultWorkspaceContext(), "workspace_b", "b-key")
	if err != nil {
		t.Fatalf("create workspace_b key: %v", err)
	}
	workspaceBVaultID := createVaultViaHTTP(t, router, workspaceBKey.APIKey, `{"display_name":"workspace-b-vault"}`)

	defaultVaultIDs := listVaultIDsViaHTTP(t, router, env.envKey, "/v1/vaults")
	for _, id := range defaultVaultIDs {
		if id == workspaceBVaultID {
			t.Fatalf("default workspace list leaked workspace_b vault %s: %v", workspaceBVaultID, defaultVaultIDs)
		}
	}
	requireIDs(t, listVaultIDsViaHTTP(t, router, workspaceBKey.APIKey, "/v1/vaults"), workspaceBVaultID)

	crossWorkspaceRec := vaultRequest(t, router, http.MethodGet, "/v1/vaults/"+workspaceBVaultID, env.envKey, "")
	assertVaultHTTPError(t, crossWorkspaceRec, http.StatusNotFound, "not_found_error")
	crossWorkspaceRec = vaultRequest(t, router, http.MethodGet, "/v1/vaults/"+liveVaultID, workspaceBKey.APIKey, "")
	assertVaultHTTPError(t, crossWorkspaceRec, http.StatusNotFound, "not_found_error")
}

func TestDeleteCredential(t *testing.T) {
	router := newVaultTestRouter(t)

	createVaultRec := httptest.NewRecorder()
	createVaultReq := httptest.NewRequest(http.MethodPost, "/v1/vaults", strings.NewReader(`{"display_name":"del-vault"}`))
	createVaultReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(createVaultReq)
	router.ServeHTTP(createVaultRec, createVaultReq)

	var vaultObj map[string]any
	_ = json.Unmarshal(createVaultRec.Body.Bytes(), &vaultObj)
	vaultID := vaultObj["id"].(string)

	credBody := staticBearerCredentialBody("del-cred", "sk-delete")
	createCredRec := httptest.NewRecorder()
	createCredReq := httptest.NewRequest(http.MethodPost, "/v1/vaults/"+vaultID+"/credentials", strings.NewReader(credBody))
	createCredReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(createCredReq)
	router.ServeHTTP(createCredRec, createCredReq)

	var credObj map[string]any
	_ = json.Unmarshal(createCredRec.Body.Bytes(), &credObj)
	credID := credObj["id"].(string)

	delRec := httptest.NewRecorder()
	delReq := httptest.NewRequest(http.MethodDelete, "/v1/vaults/"+vaultID+"/credentials/"+credID, nil)
	setAuthHeader(delReq)
	router.ServeHTTP(delRec, delReq)

	if delRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", delRec.Code, delRec.Body.String())
	}
	var deleted vault.DeleteResult
	if err := json.Unmarshal(delRec.Body.Bytes(), &deleted); err != nil {
		t.Fatalf("decode delete response: %v", err)
	}
	if deleted.ID != credID || deleted.Type != "vault_credential_deleted" {
		t.Fatalf("delete response = %+v", deleted)
	}

	getRec := httptest.NewRecorder()
	getReq := httptest.NewRequest(http.MethodGet, "/v1/vaults/"+vaultID+"/credentials/"+credID, nil)
	setAuthHeader(getReq)
	router.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", getRec.Code)
	}
}

func TestUpdateAndArchiveCredential(t *testing.T) {
	router := newVaultTestRouter(t)

	createVaultRec := httptest.NewRecorder()
	createVaultReq := httptest.NewRequest(http.MethodPost, "/v1/vaults", strings.NewReader(`{"display_name":"cred-update-vault"}`))
	createVaultReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(createVaultReq)
	router.ServeHTTP(createVaultRec, createVaultReq)
	var vaultObj map[string]any
	_ = json.Unmarshal(createVaultRec.Body.Bytes(), &vaultObj)
	vaultID := vaultObj["id"].(string)

	credBody := staticBearerCredentialBody("before", "sk-before")
	createCredRec := httptest.NewRecorder()
	createCredReq := httptest.NewRequest(http.MethodPost, "/v1/vaults/"+vaultID+"/credentials", strings.NewReader(credBody))
	createCredReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(createCredReq)
	router.ServeHTTP(createCredRec, createCredReq)
	var credObj map[string]any
	_ = json.Unmarshal(createCredRec.Body.Bytes(), &credObj)
	credID := credObj["id"].(string)

	updateBody := `{"display_name":"after","metadata":{"rotated":"yes"},"auth":{"token":"sk-after"}}`
	updateRec := httptest.NewRecorder()
	updateReq := httptest.NewRequest(http.MethodPost, "/v1/vaults/"+vaultID+"/credentials/"+credID, strings.NewReader(updateBody))
	updateReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(updateReq)
	router.ServeHTTP(updateRec, updateReq)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("update status = %d body=%s", updateRec.Code, updateRec.Body.String())
	}
	var updated vault.CredentialMetadata
	if err := json.Unmarshal(updateRec.Body.Bytes(), &updated); err != nil {
		t.Fatalf("decode updated credential: %v", err)
	}
	if updated.DisplayName != "after" || updated.Metadata["rotated"] != "yes" || updated.Auth.Type != "static_bearer" {
		t.Fatalf("updated credential = %+v", updated)
	}
	if strings.Contains(updateRec.Body.String(), "sk-after") {
		t.Fatalf("update response leaked secret: %s", updateRec.Body.String())
	}

	clearNameRec := vaultRequest(t, router, http.MethodPost, "/v1/vaults/"+vaultID+"/credentials/"+credID, "", `{"display_name":null}`)
	if clearNameRec.Code != http.StatusOK {
		t.Fatalf("clear credential display_name status = %d body=%s", clearNameRec.Code, clearNameRec.Body.String())
	}
	var cleared vault.CredentialMetadata
	if err := json.Unmarshal(clearNameRec.Body.Bytes(), &cleared); err != nil {
		t.Fatalf("decode display_name-cleared credential: %v", err)
	}
	if cleared.DisplayName != "" {
		t.Fatalf("null display_name must clear credential display name, got %q", cleared.DisplayName)
	}

	archiveRec := httptest.NewRecorder()
	archiveReq := httptest.NewRequest(http.MethodPost, "/v1/vaults/"+vaultID+"/credentials/"+credID+"/archive", nil)
	setAuthHeader(archiveReq)
	router.ServeHTTP(archiveRec, archiveReq)
	if archiveRec.Code != http.StatusOK {
		t.Fatalf("archive status = %d body=%s", archiveRec.Code, archiveRec.Body.String())
	}
	var archived vault.CredentialMetadata
	if err := json.Unmarshal(archiveRec.Body.Bytes(), &archived); err != nil {
		t.Fatalf("decode archived credential: %v", err)
	}
	if archived.ID != credID || archived.ArchivedAt == nil {
		t.Fatalf("archived credential = %+v", archived)
	}
	if strings.Contains(archiveRec.Body.String(), "sk-after") {
		t.Fatalf("archive response leaked secret: %s", archiveRec.Body.String())
	}
}

func TestCredentialUpdateRotatesMutableAuthFieldsWithoutTypeThroughHTTP(t *testing.T) {
	router := newVaultTestRouter(t)
	vaultID := createVaultViaHTTP(t, router, "", `{"display_name":"partial-update-vault"}`)

	staticBearerID := createCredentialViaHTTP(t, router, "", vaultID, `{"display_name":"static","auth":{"type":"static_bearer","mcp_server_url":"https://static-partial.example.com/mcp","token":"static-original"}}`)
	staticUpdate := vaultRequest(t, router, http.MethodPost, "/v1/vaults/"+vaultID+"/credentials/"+staticBearerID, "", `{"auth":{"token":"static-rotated"}}`)
	if staticUpdate.Code != http.StatusOK {
		t.Fatalf("static update status = %d body=%s", staticUpdate.Code, staticUpdate.Body.String())
	}
	if strings.Contains(staticUpdate.Body.String(), "static-rotated") {
		t.Fatalf("static update response leaked secret: %s", staticUpdate.Body.String())
	}
	var staticCredential vault.CredentialMetadata
	if err := json.Unmarshal(staticUpdate.Body.Bytes(), &staticCredential); err != nil {
		t.Fatalf("decode static update: %v", err)
	}
	if staticCredential.Auth.Type != "static_bearer" || staticCredential.Auth.MCPServerURL != "https://static-partial.example.com/mcp" {
		t.Fatalf("static public auth = %+v", staticCredential.Auth)
	}

	oauthID := createCredentialViaHTTP(t, router, "", vaultID, `{"display_name":"oauth","auth":{"type":"mcp_oauth","mcp_server_url":"https://oauth-partial.example.com/mcp","access_token":"oauth-original","refresh":{"refresh_token":"refresh-original","client_id":"client-original","token_endpoint":"https://oauth-partial-auth.example.com/token","token_endpoint_auth":{"type":"client_secret_basic","client_secret":"client-original-secret"}}}}`)
	oauthUpdate := vaultRequest(t, router, http.MethodPost, "/v1/vaults/"+vaultID+"/credentials/"+oauthID, "", `{"auth":{"access_token":"oauth-rotated","refresh":{"refresh_token":"refresh-rotated","scope":"read","token_endpoint_auth":{"type":"client_secret_post","client_secret":"client-rotated-secret"}}}}`)
	if oauthUpdate.Code != http.StatusOK {
		t.Fatalf("oauth update status = %d body=%s", oauthUpdate.Code, oauthUpdate.Body.String())
	}
	for _, forbidden := range []string{"oauth-rotated", "refresh-rotated", "client-rotated-secret"} {
		if strings.Contains(oauthUpdate.Body.String(), forbidden) {
			t.Fatalf("oauth update response leaked %q: %s", forbidden, oauthUpdate.Body.String())
		}
	}
	var oauthCredential vault.CredentialMetadata
	if err := json.Unmarshal(oauthUpdate.Body.Bytes(), &oauthCredential); err != nil {
		t.Fatalf("decode oauth update: %v", err)
	}
	if oauthCredential.Auth.Type != "mcp_oauth" ||
		oauthCredential.Auth.MCPServerURL != "https://oauth-partial.example.com/mcp" ||
		oauthCredential.Auth.Refresh == nil ||
		oauthCredential.Auth.Refresh.Scope != "read" ||
		oauthCredential.Auth.Refresh.TokenEndpointAuth.Type != "client_secret_post" {
		t.Fatalf("oauth public auth = %+v", oauthCredential.Auth)
	}
}

func TestCredentialUpdateRejectsEmptyOAuthPublicFieldsThroughHTTP(t *testing.T) {
	router := newVaultTestRouter(t)
	vaultID := createVaultViaHTTP(t, router, "", `{"display_name":"empty-oauth-public-vault"}`)
	oauthID := createCredentialViaHTTP(t, router, "", vaultID, `{"display_name":"oauth","auth":{"type":"mcp_oauth","mcp_server_url":"https://oauth-empty-public.example.com/mcp","access_token":"oauth-original","expires_at":"2026-05-04T06:00:00Z","refresh":{"refresh_token":"refresh-original","client_id":"client-original","token_endpoint":"https://oauth-empty-public-auth.example.com/token","scope":"read","token_endpoint_auth":{"type":"client_secret_basic","client_secret":"client-original-secret"}}}}`)

	expiresAtUpdate := vaultRequest(t, router, http.MethodPost, "/v1/vaults/"+vaultID+"/credentials/"+oauthID, "", `{"auth":{"expires_at":""}}`)
	assertVaultHTTPError(t, expiresAtUpdate, http.StatusBadRequest, "invalid_request_error")

	scopeUpdate := vaultRequest(t, router, http.MethodPost, "/v1/vaults/"+vaultID+"/credentials/"+oauthID, "", `{"auth":{"refresh":{"scope":""}}}`)
	assertVaultHTTPError(t, scopeUpdate, http.StatusBadRequest, "invalid_request_error")

	getRecorder := vaultRequest(t, router, http.MethodGet, "/v1/vaults/"+vaultID+"/credentials/"+oauthID, "", "")
	if getRecorder.Code != http.StatusOK {
		t.Fatalf("get status = %d body=%s", getRecorder.Code, getRecorder.Body.String())
	}
	var credential vault.CredentialMetadata
	if err := json.Unmarshal(getRecorder.Body.Bytes(), &credential); err != nil {
		t.Fatalf("decode credential: %v", err)
	}
	if credential.Auth.ExpiresAt != "2026-05-04T06:00:00Z" ||
		credential.Auth.Refresh == nil ||
		credential.Auth.Refresh.Scope != "read" {
		t.Fatalf("oauth public auth changed after empty field rejects: %+v", credential.Auth)
	}
}

func TestCredentialUpdateRejectsImmutableArchivedAndDisplayNameThroughHTTP(t *testing.T) {
	router := newVaultTestRouter(t)
	vaultID := createVaultViaHTTP(t, router, "", `{"display_name":"immutable-vault"}`)

	primaryStaticID := createCredentialViaHTTP(t, router, "", vaultID, `{"display_name":"primary-static","auth":{"type":"static_bearer","mcp_server_url":"https://primary-static-immutable.example.com/mcp","token":"static-original"}}`)
	staticBearerID := createCredentialViaHTTP(t, router, "", vaultID, `{"display_name":"static","auth":{"type":"static_bearer","mcp_server_url":"https://static-immutable.example.com/mcp","token":"static-original"}}`)
	oauthID := createCredentialViaHTTP(t, router, "", vaultID, `{"display_name":"oauth","auth":{"type":"mcp_oauth","mcp_server_url":"https://oauth-immutable.example.com/mcp","access_token":"oauth-original","refresh":{"refresh_token":"refresh-original","client_id":"client-original","token_endpoint":"https://oauth-auth.example.com/token","resource":"https://oauth-resource.example.com","token_endpoint_auth":{"type":"client_secret_basic","client_secret":"client-original-secret"}}}}`)

	cases := []struct {
		name         string
		credentialID string
		body         string
		forbidden    string
	}{
		{
			name:         "auth type",
			credentialID: primaryStaticID,
			body:         `{"auth":{"type":"mcp_oauth","mcp_server_url":"https://type-change.example.com/mcp","access_token":"type-change-secret"}}`,
			forbidden:    "type-change-secret",
		},
		{
			name:         "static empty token",
			credentialID: primaryStaticID,
			body:         `{"auth":{"token":""}}`,
		},
		{
			name:         "static wrong variant access token",
			credentialID: staticBearerID,
			body:         `{"auth":{"access_token":"wrong-access-secret"}}`,
			forbidden:    "wrong-access-secret",
		},
		{
			name:         "static empty token",
			credentialID: staticBearerID,
			body:         `{"auth":{"token":""}}`,
		},
		{
			name:         "oauth wrong variant token",
			credentialID: oauthID,
			body:         `{"auth":{"token":"wrong-token-secret"}}`,
			forbidden:    "wrong-token-secret",
		},
		{
			name:         "static mcp url",
			credentialID: staticBearerID,
			body:         `{"auth":{"type":"static_bearer","mcp_server_url":"https://other-static.example.com/mcp","token":"static-change-secret"}}`,
			forbidden:    "static-change-secret",
		},
		{
			name:         "oauth mcp url",
			credentialID: oauthID,
			body:         `{"auth":{"type":"mcp_oauth","mcp_server_url":"https://other-oauth.example.com/mcp","access_token":"oauth-url-secret"}}`,
			forbidden:    "oauth-url-secret",
		},
		{
			name:         "oauth empty access token",
			credentialID: oauthID,
			body:         `{"auth":{"access_token":""}}`,
		},
		{
			name:         "oauth empty refresh token",
			credentialID: oauthID,
			body:         `{"auth":{"refresh":{"refresh_token":""}}}`,
		},
		{
			name:         "oauth empty expires at",
			credentialID: oauthID,
			body:         `{"auth":{"expires_at":""}}`,
		},
		{
			name:         "oauth empty refresh scope",
			credentialID: oauthID,
			body:         `{"auth":{"refresh":{"scope":""}}}`,
		},
		{
			name:         "oauth client id",
			credentialID: oauthID,
			body:         `{"auth":{"type":"mcp_oauth","access_token":"oauth-client-secret","refresh":{"client_id":"client-changed"}}}`,
			forbidden:    "oauth-client-secret",
		},
		{
			name:         "oauth token endpoint",
			credentialID: oauthID,
			body:         `{"auth":{"type":"mcp_oauth","access_token":"oauth-endpoint-secret","refresh":{"token_endpoint":"https://other-auth.example.com/token"}}}`,
			forbidden:    "oauth-endpoint-secret",
		},
		{
			name:         "oauth resource",
			credentialID: oauthID,
			body:         `{"auth":{"type":"mcp_oauth","access_token":"oauth-resource-secret","refresh":{"resource":"https://other-resource.example.com"}}}`,
			forbidden:    "oauth-resource-secret",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := vaultRequest(t, router, http.MethodPost, "/v1/vaults/"+vaultID+"/credentials/"+tc.credentialID, "", tc.body)
			assertVaultHTTPError(t, recorder, http.StatusBadRequest, "invalid_request_error", tc.forbidden)
		})
	}

	overlongDisplayName := strings.Repeat("x", 256)
	recorder := vaultRequest(t, router, http.MethodPost, "/v1/vaults/"+vaultID+"/credentials/"+primaryStaticID, "", `{"display_name":"`+overlongDisplayName+`"}`)
	assertVaultHTTPError(t, recorder, http.StatusBadRequest, "invalid_request_error")

	archiveRec := vaultRequest(t, router, http.MethodPost, "/v1/vaults/"+vaultID+"/credentials/"+primaryStaticID+"/archive", "", "")
	if archiveRec.Code != http.StatusOK {
		t.Fatalf("archive status = %d body=%s", archiveRec.Code, archiveRec.Body.String())
	}
	recorder = vaultRequest(t, router, http.MethodPost, "/v1/vaults/"+vaultID+"/credentials/"+primaryStaticID, "", `{"auth":{"token":"archived-update-secret"}}`)
	assertVaultHTTPError(t, recorder, http.StatusBadRequest, "invalid_request_error", "archived-update-secret")
}

func TestCredentialHTTPMCPActiveCap(t *testing.T) {
	router := newVaultTestRouter(t)
	vaultID := createVaultViaHTTP(t, router, "", `{"display_name":"mcp-cap-vault"}`)

	createdMCPIDs := make([]string, 0, 20)
	for i := 0; i < 20; i++ {
		body := fmt.Sprintf(`{"display_name":"mcp-%02d","auth":{"type":"static_bearer","mcp_server_url":"https://mcp-cap-%02d.example.com/service","token":"mcp-token-%02d"}}`, i, i, i)
		createdMCPIDs = append(createdMCPIDs, createCredentialViaHTTP(t, router, "", vaultID, body))
	}

	overCapBody := `{"display_name":"mcp-over","auth":{"type":"static_bearer","mcp_server_url":"https://mcp-cap-over.example.com/service","token":"mcp-over-token"}}`
	overCapRec := vaultRequest(t, router, http.MethodPost, "/v1/vaults/"+vaultID+"/credentials", "", overCapBody)
	assertVaultHTTPError(t, overCapRec, http.StatusBadRequest, "invalid_request_error", "mcp-over-token")

	archiveRec := vaultRequest(t, router, http.MethodPost, "/v1/vaults/"+vaultID+"/credentials/"+createdMCPIDs[0]+"/archive", "", "")
	if archiveRec.Code != http.StatusOK {
		t.Fatalf("archive status = %d body=%s", archiveRec.Code, archiveRec.Body.String())
	}
	createCredentialViaHTTP(t, router, "", vaultID, `{"display_name":"mcp-after-archive","auth":{"type":"static_bearer","mcp_server_url":"https://mcp-after-archive.example.com/service","token":"mcp-after-archive-token"}}`)

}

func TestCredentialSecretSentinelsDoNotAppearInRouterResponsesOrLogs(t *testing.T) {
	var logs bytes.Buffer
	previousLogOutput := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(previousLogOutput) })

	router := newVaultTestRouter(t)
	vaultID := createVaultViaHTTP(t, router, "", `{"display_name":"log-vault"}`)

	var responses []struct {
		name string
		body string
	}
	record := func(name string, method string, path string, body string, wantStatus int) *httptest.ResponseRecorder {
		t.Helper()
		recorder := vaultRequest(t, router, method, path, "", body)
		if recorder.Code != wantStatus {
			t.Fatalf("%s status = %d body=%s; want %d", name, recorder.Code, recorder.Body.String(), wantStatus)
		}
		responses = append(responses, struct {
			name string
			body string
		}{name: name, body: recorder.Body.String()})
		return recorder
	}
	credentialIDFromResponse := func(name string, recorder *httptest.ResponseRecorder) string {
		t.Helper()
		var created struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(recorder.Body.Bytes(), &created); err != nil {
			t.Fatalf("%s decode credential: %v", name, err)
		}
		if created.ID == "" {
			t.Fatalf("%s response missing credential id", name)
		}
		return created.ID
	}

	const primaryStaticCreateSecret = "router-primary-static-create-secret" //nolint:gosec // G101: synthetic test sentinel
	const staticBearerCreateSecret = "router-static-bearer-create-secret"   //nolint:gosec // G101: synthetic test sentinel
	const oauthAccessCreateSecret = "router-oauth-access-create-secret"     //nolint:gosec // G101: synthetic test sentinel
	const oauthRefreshCreateSecret = "router-oauth-refresh-create-secret"   //nolint:gosec // G101: synthetic test sentinel
	const oauthClientCreateSecret = "router-oauth-client-create-secret"     //nolint:gosec // G101: synthetic test sentinel
	const primaryStaticUpdateSecret = "router-primary-static-update-secret" //nolint:gosec // G101: synthetic test sentinel
	const staticBearerUpdateSecret = "router-static-bearer-update-secret"   //nolint:gosec // G101: synthetic test sentinel
	const oauthAccessUpdateSecret = "router-oauth-access-update-secret"     //nolint:gosec // G101: synthetic test sentinel
	const oauthRefreshUpdateSecret = "router-oauth-refresh-update-secret"   //nolint:gosec // G101: synthetic test sentinel
	const oauthClientUpdateSecret = "router-oauth-client-update-secret"     //nolint:gosec // G101: synthetic test sentinel
	const rawURLSecret = "router-raw-url-secret"                            //nolint:gosec // G101: synthetic test sentinel
	const rawURLTokenSecret = "router-raw-url-token-secret"                 //nolint:gosec // G101: synthetic test sentinel
	const duplicateSecretOne = "router-duplicate-create-secret-one"         //nolint:gosec // G101: synthetic test sentinel
	const duplicateSecretTwo = "router-duplicate-create-secret-two"         //nolint:gosec // G101: synthetic test sentinel
	const capRejectSecret = "router-mcp-cap-reject-secret"                  //nolint:gosec // G101: synthetic test sentinel
	const immutableCreateSecret = "router-immutable-create-secret"          //nolint:gosec // G101: synthetic test sentinel
	const immutableRejectSecret = "router-immutable-reject-secret"          //nolint:gosec // G101: synthetic test sentinel
	const archivedUpdateRejectSecret = "router-archived-update-secret"      //nolint:gosec // G101: synthetic test sentinel
	rawSecretURL := "https://user:" + rawURLSecret + "@router-raw.example.com/mcp"

	primaryStaticID := credentialIDFromResponse("primary_static create", record("primary_static create", http.MethodPost, "/v1/vaults/"+vaultID+"/credentials",
		`{"display_name":"primary-static","auth":{"type":"static_bearer","mcp_server_url":"https://router-primary-static.example.com/mcp","token":"`+primaryStaticCreateSecret+`"}}`, http.StatusOK))
	staticBearerID := credentialIDFromResponse("static_bearer create", record("static_bearer create", http.MethodPost, "/v1/vaults/"+vaultID+"/credentials",
		`{"display_name":"static","auth":{"type":"static_bearer","mcp_server_url":"https://router-static.example.com/mcp","token":"`+staticBearerCreateSecret+`"}}`, http.StatusOK))
	oauthID := credentialIDFromResponse("mcp_oauth create", record("mcp_oauth create", http.MethodPost, "/v1/vaults/"+vaultID+"/credentials",
		`{"display_name":"oauth","auth":{"type":"mcp_oauth","mcp_server_url":"https://router-oauth.example.com/mcp","access_token":"`+oauthAccessCreateSecret+`","refresh":{"refresh_token":"`+oauthRefreshCreateSecret+`","client_id":"router-client","token_endpoint":"https://router-oauth-auth.example.com/token","token_endpoint_auth":{"type":"client_secret_basic","client_secret":"`+oauthClientCreateSecret+`"}}}}`, http.StatusOK))

	record("primary_static update", http.MethodPost, "/v1/vaults/"+vaultID+"/credentials/"+primaryStaticID,
		`{"auth":{"token":"`+primaryStaticUpdateSecret+`"}}`, http.StatusOK)
	record("static_bearer update", http.MethodPost, "/v1/vaults/"+vaultID+"/credentials/"+staticBearerID,
		`{"auth":{"token":"`+staticBearerUpdateSecret+`"}}`, http.StatusOK)
	record("mcp_oauth update", http.MethodPost, "/v1/vaults/"+vaultID+"/credentials/"+oauthID,
		`{"auth":{"access_token":"`+oauthAccessUpdateSecret+`","refresh":{"refresh_token":"`+oauthRefreshUpdateSecret+`","token_endpoint_auth":{"type":"client_secret_post","client_secret":"`+oauthClientUpdateSecret+`"}}}}`, http.StatusOK)

	record("primary_static get", http.MethodGet, "/v1/vaults/"+vaultID+"/credentials/"+primaryStaticID, "", http.StatusOK)
	record("static_bearer get", http.MethodGet, "/v1/vaults/"+vaultID+"/credentials/"+staticBearerID, "", http.StatusOK)
	record("mcp_oauth get", http.MethodGet, "/v1/vaults/"+vaultID+"/credentials/"+oauthID, "", http.StatusOK)
	record("credential list", http.MethodGet, "/v1/vaults/"+vaultID+"/credentials", "", http.StatusOK)

	record("primary_static archive", http.MethodPost, "/v1/vaults/"+vaultID+"/credentials/"+primaryStaticID+"/archive", "", http.StatusOK)
	record("archived update", http.MethodPost, "/v1/vaults/"+vaultID+"/credentials/"+primaryStaticID,
		`{"auth":{"token":"`+archivedUpdateRejectSecret+`"}}`, http.StatusBadRequest)
	record("static_bearer archive", http.MethodPost, "/v1/vaults/"+vaultID+"/credentials/"+staticBearerID+"/archive", "", http.StatusOK)
	record("mcp_oauth archive", http.MethodPost, "/v1/vaults/"+vaultID+"/credentials/"+oauthID+"/archive", "", http.StatusOK)
	record("primary_static delete", http.MethodDelete, "/v1/vaults/"+vaultID+"/credentials/"+primaryStaticID, "", http.StatusOK)
	record("static_bearer delete", http.MethodDelete, "/v1/vaults/"+vaultID+"/credentials/"+staticBearerID, "", http.StatusOK)
	record("mcp_oauth delete", http.MethodDelete, "/v1/vaults/"+vaultID+"/credentials/"+oauthID, "", http.StatusOK)

	record("strict raw userinfo URL", http.MethodPost, "/v1/vaults/"+vaultID+"/credentials",
		`{"display_name":"bad-url","auth":{"type":"static_bearer","mcp_server_url":"`+rawSecretURL+`","token":"`+rawURLTokenSecret+`"}}`, http.StatusBadRequest)
	_ = credentialIDFromResponse("duplicate first", record("duplicate first", http.MethodPost, "/v1/vaults/"+vaultID+"/credentials",
		`{"display_name":"dup-one","auth":{"type":"static_bearer","mcp_server_url":"https://Router-Duplicate.example.com/mcp","token":"`+duplicateSecretOne+`"}}`, http.StatusOK))
	record("duplicate canonical MCP URL", http.MethodPost, "/v1/vaults/"+vaultID+"/credentials",
		`{"display_name":"dup-two","auth":{"type":"static_bearer","mcp_server_url":"https://router-duplicate.example.com/mcp","token":"`+duplicateSecretTwo+`"}}`, http.StatusConflict)
	immutableID := credentialIDFromResponse("immutable create", record("immutable create", http.MethodPost, "/v1/vaults/"+vaultID+"/credentials",
		`{"display_name":"immutable","auth":{"type":"static_bearer","mcp_server_url":"https://router-immutable.example.com/mcp","token":"`+immutableCreateSecret+`"}}`, http.StatusOK))
	record("immutable field update", http.MethodPost, "/v1/vaults/"+vaultID+"/credentials/"+immutableID,
		`{"auth":{"mcp_server_url":"https://router-immutable-other.example.com/mcp","token":"`+immutableRejectSecret+`"}}`, http.StatusBadRequest)

	capVaultID := createVaultViaHTTP(t, router, "", `{"display_name":"mcp-cap-log-vault"}`)
	capFillSecrets := make([]string, 0, 20)
	for i := 0; i < 20; i++ {
		fillSecret := fmt.Sprintf("router-mcp-cap-fill-secret-%02d", i)
		capFillSecrets = append(capFillSecrets, fillSecret)
		record(fmt.Sprintf("mcp cap fill %02d", i), http.MethodPost, "/v1/vaults/"+capVaultID+"/credentials",
			fmt.Sprintf(`{"display_name":"cap-%02d","auth":{"type":"static_bearer","mcp_server_url":"https://router-cap-%02d.example.com/mcp","token":"%s"}}`, i, i, fillSecret), http.StatusOK)
	}
	record("mcp cap rejection", http.MethodPost, "/v1/vaults/"+capVaultID+"/credentials",
		`{"display_name":"cap-over","auth":{"type":"static_bearer","mcp_server_url":"https://router-cap-over.example.com/mcp","token":"`+capRejectSecret+`"}}`, http.StatusBadRequest)

	forbidden := []string{
		primaryStaticCreateSecret, staticBearerCreateSecret,
		oauthAccessCreateSecret, oauthRefreshCreateSecret, oauthClientCreateSecret,
		primaryStaticUpdateSecret, staticBearerUpdateSecret,
		oauthAccessUpdateSecret, oauthRefreshUpdateSecret, oauthClientUpdateSecret,
		rawSecretURL, rawURLSecret, rawURLTokenSecret,
		duplicateSecretOne, duplicateSecretTwo,
		capRejectSecret, immutableCreateSecret, immutableRejectSecret, archivedUpdateRejectSecret,
	}
	forbidden = append(forbidden, capFillSecrets...)
	for _, response := range responses {
		for _, sentinel := range forbidden {
			if strings.Contains(response.body, sentinel) {
				t.Fatalf("%s response leaked %q: %s", response.name, sentinel, response.body)
			}
		}
	}
	logOutput := logs.String()
	for _, sentinel := range forbidden {
		if strings.Contains(logOutput, sentinel) {
			t.Fatalf("router logs leaked %q: %s", sentinel, logOutput)
		}
	}
}

func TestCreateCredentialRejectsUnsupportedAuthVariant(t *testing.T) {
	router := newVaultTestRouter(t)

	createVaultRec := httptest.NewRecorder()
	createVaultReq := httptest.NewRequest(http.MethodPost, "/v1/vaults", strings.NewReader(`{"display_name":"unsupported-auth-check"}`))
	createVaultReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(createVaultReq)
	router.ServeHTTP(createVaultRec, createVaultReq)
	var vaultObj map[string]any
	_ = json.Unmarshal(createVaultRec.Body.Bytes(), &vaultObj)
	vaultID := vaultObj["id"].(string)

	body := `{"display_name":"Unsupported","auth":{"type":"password","token":"password-secret"}}`
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/vaults/"+vaultID+"/credentials", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	setAuthHeader(req)
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
	var response map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &response)
	errObj, _ := response["error"].(map[string]any)
	if errObj["type"] != "invalid_request_error" {
		t.Errorf("error.type = %v; want invalid_request_error", errObj["type"])
	}
	msg, _ := errObj["message"].(string)
	if msg != "auth.type must be mcp_oauth, static_bearer, provider_api_key, or provider_oauth" {
		t.Errorf("error.message = %q; want supported auth variants", msg)
	}
}

func TestCreateCredentialAcceptsProviderAuthVariants(t *testing.T) {
	router := newVaultTestRouter(t)
	vaultID := createVaultViaHTTP(t, router, "", `{"display_name":"provider-vault"}`)

	const apiKeySecret = "provider-api-key-http-secret"       //nolint:gosec // G101: synthetic test sentinel
	const apiKeyRotated = "provider-api-key-http-rotated"     //nolint:gosec // G101: synthetic test sentinel
	const oauthAccess = "provider-oauth-http-access"          //nolint:gosec // G101: synthetic test sentinel
	const oauthRefresh = "provider-oauth-http-refresh"        //nolint:gosec // G101: synthetic test sentinel
	const oauthAccessRotated = "provider-oauth-http-access-2" //nolint:gosec // G101: synthetic test sentinel
	const oauthRefreshRotated = "provider-oauth-http-refresh-2"

	apiKeyRec := vaultRequest(t, router, http.MethodPost, "/v1/vaults/"+vaultID+"/credentials", "", `{
		"display_name":"Anthropic key",
		"auth":{"type":"provider_api_key","provider_id":"anthropic","access_mode":"user_api_key","token":"`+apiKeySecret+`"}
	}`)
	if apiKeyRec.Code != http.StatusOK {
		t.Fatalf("create provider_api_key status = %d body=%s", apiKeyRec.Code, apiKeyRec.Body.String())
	}
	assertProviderCredentialHTTPBody(t, apiKeyRec.Body.String(), "provider_api_key", "anthropic", "user_api_key", "", "", apiKeySecret)
	var apiKeyObj struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(apiKeyRec.Body.Bytes(), &apiKeyObj); err != nil {
		t.Fatalf("decode provider_api_key response: %v", err)
	}
	if apiKeyObj.ID == "" {
		t.Fatal("provider_api_key response missing id")
	}
	apiKeyUpdateRec := vaultRequest(t, router, http.MethodPost, "/v1/vaults/"+vaultID+"/credentials/"+apiKeyObj.ID, "", `{
		"auth":{"type":"provider_api_key","provider_id":"anthropic","access_mode":"user_api_key","token":"`+apiKeyRotated+`"}
	}`)
	if apiKeyUpdateRec.Code != http.StatusOK {
		t.Fatalf("update provider_api_key status = %d body=%s", apiKeyUpdateRec.Code, apiKeyUpdateRec.Body.String())
	}
	assertProviderCredentialHTTPBody(t, apiKeyUpdateRec.Body.String(), "provider_api_key", "anthropic", "user_api_key", "", "", apiKeyRotated)
	apiKeyGetRec := vaultRequest(t, router, http.MethodGet, "/v1/vaults/"+vaultID+"/credentials/"+apiKeyObj.ID, "", "")
	if apiKeyGetRec.Code != http.StatusOK {
		t.Fatalf("get provider_api_key status = %d body=%s", apiKeyGetRec.Code, apiKeyGetRec.Body.String())
	}
	assertProviderCredentialHTTPBody(t, apiKeyGetRec.Body.String(), "provider_api_key", "anthropic", "user_api_key", "", "", apiKeySecret, apiKeyRotated)

	oauthRec := vaultRequest(t, router, http.MethodPost, "/v1/vaults/"+vaultID+"/credentials", "", `{
		"display_name":"OpenAI OAuth",
		"auth":{"type":"provider_oauth","provider_id":"openai","access_mode":"oauth","access_token":"`+oauthAccess+`","refresh_token":"`+oauthRefresh+`","expires_at":"2026-05-04T05:00:00Z","account_id":"acct_http_old"}
	}`)
	if oauthRec.Code != http.StatusOK {
		t.Fatalf("create provider_oauth status = %d body=%s", oauthRec.Code, oauthRec.Body.String())
	}
	assertProviderCredentialHTTPBody(t, oauthRec.Body.String(), "provider_oauth", "openai", "oauth", "2026-05-04T05:00:00Z", "acct_http_old", oauthAccess, oauthRefresh)
	var oauthObj struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(oauthRec.Body.Bytes(), &oauthObj); err != nil {
		t.Fatalf("decode provider_oauth response: %v", err)
	}
	if oauthObj.ID == "" {
		t.Fatal("provider_oauth response missing id")
	}
	oauthUpdateRec := vaultRequest(t, router, http.MethodPost, "/v1/vaults/"+vaultID+"/credentials/"+oauthObj.ID, "", `{
		"auth":{"access_token":"`+oauthAccessRotated+`","refresh_token":"`+oauthRefreshRotated+`","expires_at":"2026-05-04T06:00:00Z","account_id":"acct_http_new"}
	}`)
	if oauthUpdateRec.Code != http.StatusOK {
		t.Fatalf("update provider_oauth status = %d body=%s", oauthUpdateRec.Code, oauthUpdateRec.Body.String())
	}
	assertProviderCredentialHTTPBody(t, oauthUpdateRec.Body.String(), "provider_oauth", "openai", "oauth", "2026-05-04T06:00:00Z", "acct_http_new", oauthAccessRotated, oauthRefreshRotated)
	oauthGetRec := vaultRequest(t, router, http.MethodGet, "/v1/vaults/"+vaultID+"/credentials/"+oauthObj.ID, "", "")
	if oauthGetRec.Code != http.StatusOK {
		t.Fatalf("get provider_oauth status = %d body=%s", oauthGetRec.Code, oauthGetRec.Body.String())
	}
	assertProviderCredentialHTTPBody(t, oauthGetRec.Body.String(), "provider_oauth", "openai", "oauth", "2026-05-04T06:00:00Z", "acct_http_new", oauthAccess, oauthRefresh, oauthAccessRotated, oauthRefreshRotated)

	listRec := vaultRequest(t, router, http.MethodGet, "/v1/vaults/"+vaultID+"/credentials", "", "")
	if listRec.Code != http.StatusOK {
		t.Fatalf("list credentials status = %d body=%s", listRec.Code, listRec.Body.String())
	}
	for _, forbidden := range []string{apiKeySecret, apiKeyRotated, oauthAccess, oauthRefresh, oauthAccessRotated, oauthRefreshRotated} {
		if strings.Contains(listRec.Body.String(), forbidden) {
			t.Fatalf("credential list leaked %q: %s", forbidden, listRec.Body.String())
		}
	}
}

func TestCreateCredentialAcceptsMCPOAuthWithoutVendor(t *testing.T) {
	router := newVaultTestRouter(t)

	createVaultRec := httptest.NewRecorder()
	createVaultReq := httptest.NewRequest(http.MethodPost, "/v1/vaults", strings.NewReader(`{"display_name":"mcp-vault"}`))
	createVaultReq.Header.Set("Content-Type", "application/json")
	setAuthHeader(createVaultReq)
	router.ServeHTTP(createVaultRec, createVaultReq)
	var vaultObj map[string]any
	_ = json.Unmarshal(createVaultRec.Body.Bytes(), &vaultObj)
	vaultID := vaultObj["id"].(string)

	body := `{"display_name":"MCP OAuth","auth":{"type":"mcp_oauth","mcp_server_url":"https://example.invalid/mcp","access_token":"oauth-secret"}}`
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/vaults/"+vaultID+"/credentials", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	setAuthHeader(req)
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var created map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &created)
	credID := created["id"].(string)

	getRec := httptest.NewRecorder()
	getReq := httptest.NewRequest(http.MethodGet, "/v1/vaults/"+vaultID+"/credentials/"+credID, nil)
	setAuthHeader(getReq)
	router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("expected 200 on GET, got %d", getRec.Code)
	}
	var got map[string]any
	_ = json.Unmarshal(getRec.Body.Bytes(), &got)
	auth, _ := got["auth"].(map[string]any)
	if _, ok := auth["vendor"]; ok {
		t.Errorf("auth.vendor must not appear for mcp_oauth: %v", auth)
	}
}

func TestValidateMCPOAuthCredentialRouteReturnsScrubbedDiagnostic(t *testing.T) {
	validator := &recordingMCPOAuthValidator{}
	router := newVaultTestRouterWithVaultOptions(t, vault.WithMCPOAuthValidator(validator))
	vaultID := createVaultViaHTTP(t, router, "", `{"display_name":"validation-vault"}`)
	const accessSecret = "validation-route-access-secret"   //nolint:gosec // G101: synthetic test sentinel
	const refreshSecret = "validation-route-refresh-secret" //nolint:gosec // G101: synthetic test sentinel
	const clientSecret = "validation-route-client-secret"   //nolint:gosec // G101: synthetic test sentinel
	credentialID := createCredentialViaHTTP(t, router, "", vaultID,
		`{"display_name":"oauth","auth":{"type":"mcp_oauth","mcp_server_url":"https://validation-route.example.com/mcp","access_token":"`+accessSecret+`","refresh":{"refresh_token":"`+refreshSecret+`","client_id":"client-123","token_endpoint":"https://tokens.validation-route.example.com/oauth","token_endpoint_auth":{"type":"client_secret_post","client_secret":"`+clientSecret+`"}}}}`)

	recorder := vaultRequest(t, router, http.MethodPost, "/v1/vaults/"+vaultID+"/credentials/"+credentialID+"/mcp_oauth_validate", "", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("validate status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	if !validator.called {
		t.Fatal("validator was not called")
	}
	if validator.input.Auth.AccessToken != accessSecret ||
		validator.input.Auth.Refresh == nil ||
		validator.input.Auth.Refresh.RefreshToken != refreshSecret ||
		validator.input.Auth.Refresh.TokenEndpointAuth == nil ||
		validator.input.Auth.Refresh.TokenEndpointAuth.ClientSecret != clientSecret {
		t.Fatalf("validator did not receive stored secret material: %+v", validator.input.Auth)
	}

	var response struct {
		Type            string                           `json:"type"`
		CredentialID    string                           `json:"credential_id"`
		VaultID         string                           `json:"vault_id"`
		Status          string                           `json:"status"`
		ValidatedAt     string                           `json:"validated_at"`
		HasRefreshToken bool                             `json:"has_refresh_token"`
		Refresh         *vault.CredentialValidationCheck `json:"refresh"`
		MCPProbe        *vault.CredentialValidationCheck `json:"mcp_probe"`
		Auth            map[string]any                   `json:"auth"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode validation response: %v", err)
	}
	if response.Type != "vault_credential_validation" ||
		response.CredentialID != credentialID ||
		response.VaultID != vaultID ||
		response.Status != "valid" ||
		response.ValidatedAt == "" ||
		!response.HasRefreshToken ||
		response.Refresh == nil ||
		response.MCPProbe == nil {
		t.Fatalf("validation response = %+v", response)
	}
	if response.Auth != nil {
		t.Fatalf("validation response must not embed credential auth: %+v", response.Auth)
	}
	for _, forbidden := range []string{accessSecret, refreshSecret, clientSecret, "tokens.validation-route.example.com", "validation-route.example.com"} {
		if strings.Contains(recorder.Body.String(), forbidden) {
			t.Fatalf("validation response leaked %q: %s", forbidden, recorder.Body.String())
		}
	}
}

func TestValidateMCPOAuthCredentialRouteRejectsNonOAuthCredential(t *testing.T) {
	validator := &recordingMCPOAuthValidator{}
	router := newVaultTestRouterWithVaultOptions(t, vault.WithMCPOAuthValidator(validator))
	vaultID := createVaultViaHTTP(t, router, "", `{"display_name":"validation-vault"}`)
	credentialID := createCredentialViaHTTP(t, router, "", vaultID, staticBearerCredentialBody("static", "static-validation-route-secret"))

	recorder := vaultRequest(t, router, http.MethodPost, "/v1/vaults/"+vaultID+"/credentials/"+credentialID+"/mcp_oauth_validate", "", "")
	assertVaultHTTPError(t, recorder, http.StatusBadRequest, "invalid_request_error", "static-validation-route-secret")
	if validator.called {
		t.Fatal("validator must not run for static_bearer credentials")
	}
}

func TestVaultArchiveMissingVaultIsNotFound(t *testing.T) {
	router := newVaultTestRouter(t)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/vaults/vlt_test/archive", nil)
	setAuthHeader(request)
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", recorder.Code, recorder.Body.String())
	}
}

func TestCreateCredentialMissingVault(t *testing.T) {
	router := newVaultTestRouter(t)

	body := staticBearerCredentialBody("LLM Key", "sk-test")
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/vaults/vlt_nonexistent/credentials", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	setAuthHeader(request)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", recorder.Code, recorder.Body.String())
	}
}

func TestCreateCredentialMissingVaultStrictDecodePrecedesParentLookup(t *testing.T) {
	vaults := &recordingVaultStore{
		getErr: &vault.NotFoundError{Message: "vault vlt_nonexistent not found"},
	}
	credentials := &recordingCredentialStore{}
	router := newVaultHandlerRouterForListTests(t, vaults, credentials)

	const unknownTopLevelSecret = "unknown-top-level-secret"
	const unknownNestedSecret = "unknown-nested-secret" //nolint:gosec // G101: synthetic test sentinel
	const trailingSecret = "trailing-secret"
	const rawURLSecret = "raw-url-secret" //nolint:gosec // G101: synthetic test sentinel
	rawURL := "https://user:" + rawURLSecret + "@example.com/mcp"

	cases := []struct {
		name      string
		body      string
		forbidden []string
	}{
		{
			name:      "unknown top-level field",
			body:      `{"display_name":"LLM Key","unexpected":"` + unknownTopLevelSecret + `","auth":{"type":"static_bearer","mcp_server_url":"https://strict-missing-vault.example.com/mcp","token":"sk-unknown-top-level"}}`,
			forbidden: []string{unknownTopLevelSecret, "sk-unknown-top-level"},
		},
		{
			name:      "unknown nested auth field",
			body:      `{"display_name":"LLM Key","auth":{"type":"static_bearer","mcp_server_url":"https://strict-missing-vault.example.com/mcp","token":"sk-unknown-nested","unexpected":"` + unknownNestedSecret + `"}}`,
			forbidden: []string{unknownNestedSecret, "sk-unknown-nested"},
		},
		{
			name:      "trailing JSON value",
			body:      `{"display_name":"LLM Key","auth":{"type":"static_bearer","mcp_server_url":"https://strict-missing-vault.example.com/mcp","token":"sk-trailing"}} {"secret":"` + trailingSecret + `"}`,
			forbidden: []string{trailingSecret, "sk-trailing"},
		},
		{
			name:      "raw userinfo URL",
			body:      `{"display_name":"MCP","auth":{"type":"static_bearer","mcp_server_url":"` + rawURL + `","token":"static-userinfo-secret"}}`,
			forbidden: []string{rawURL, rawURLSecret, "static-userinfo-secret"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vaults.getCalled = false
			credentials.createCalled = false
			recorder := vaultRequest(t, router, http.MethodPost, "/v1/vaults/vlt_nonexistent/credentials", "", tc.body)
			assertVaultHTTPError(t, recorder, http.StatusBadRequest, "invalid_request_error", tc.forbidden...)
			if vaults.getCalled {
				t.Fatal("parent vault lookup ran before strict body decoding")
			}
			if credentials.createCalled {
				t.Fatal("credential create ran after rejected body")
			}
		})
	}

	validBody := staticBearerCredentialBody("LLM Key", "sk-valid-missing-vault")
	recorder := vaultRequest(t, router, http.MethodPost, "/v1/vaults/vlt_nonexistent/credentials", "", validBody)
	assertVaultHTTPError(t, recorder, http.StatusNotFound, "not_found_error", "sk-valid-missing-vault")
	if !vaults.getCalled {
		t.Fatal("valid missing-vault request did not check parent vault")
	}
	if credentials.createCalled {
		t.Fatal("credential create ran for missing parent vault")
	}
}

func TestListCredentialsMissingVault(t *testing.T) {
	router := newVaultTestRouter(t)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/vaults/vlt_nonexistent/credentials", nil)
	setAuthHeader(request)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", recorder.Code, recorder.Body.String())
	}
}

func TestCredentialArchiveMissingCredentialIsNotFound(t *testing.T) {
	router := newVaultTestRouter(t)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/vaults/vlt_test/credentials/cred_test/archive", nil)
	setAuthHeader(request)
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", recorder.Code, recorder.Body.String())
	}
}
