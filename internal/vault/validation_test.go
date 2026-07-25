package vault

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHTTPMCPOAuthValidatorRefreshesProbesAndScrubsPublicResult(t *testing.T) {
	const oldAccess = "old-oauth-access-sentinel"
	const oldRefresh = "old-oauth-refresh-sentinel"
	const clientSecret = "oauth-client-secret-sentinel"
	const newAccess = "new-oauth-access-sentinel"
	const newRefresh = "new-oauth-refresh-sentinel"

	var sawRefresh bool
	var sawProbe bool
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/oauth":
			sawRefresh = true
			if request.Method != http.MethodPost {
				t.Fatalf("refresh method = %s", request.Method)
			}
			if got := request.Header.Get("Authorization"); got != "Basic "+base64.StdEncoding.EncodeToString([]byte("client-123:"+clientSecret)) {
				t.Fatalf("refresh Authorization = %q", got)
			}
			body, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatalf("read refresh body: %v", err)
			}
			form := string(body)
			for _, want := range []string{"grant_type=refresh_token", "refresh_token=" + oldRefresh, "scope=repo", "resource=https%3A%2F%2Fapi.example.com%2Fmcp"} {
				if !strings.Contains(form, want) {
					t.Fatalf("refresh form %q missing %q", form, want)
				}
			}
			if strings.Contains(form, "client_secret") {
				t.Fatalf("client_secret_basic must not put client_secret in form: %s", form)
			}
			writer.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(writer, `{"access_token":"`+newAccess+`","refresh_token":"`+newRefresh+`","expires_in":3600}`)
		case "/mcp":
			sawProbe = true
			if request.Method != http.MethodPost {
				t.Fatalf("probe method = %s", request.Method)
			}
			if got := request.Header.Get("Authorization"); got != "Bearer "+newAccess {
				t.Fatalf("probe Authorization = %q", got)
			}
			if got := request.Header.Get("MCP-Protocol-Version"); got != mcpOAuthProtocolVersion {
				t.Fatalf("probe protocol header = %q", got)
			}
			body, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatalf("read probe body: %v", err)
			}
			if !strings.Contains(string(body), `"method":"initialize"`) {
				t.Fatalf("probe body missing initialize: %s", string(body))
			}
			writer.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(writer, `{"jsonrpc":"2.0","id":"tetral-vault-validation","result":{"protocolVersion":"2025-06-18","capabilities":{},"serverInfo":{"name":"unit","version":"1"}}}`)
		default:
			t.Fatalf("unexpected URL path %s", request.URL.Path)
		}
	}))
	defer server.Close()
	trustValidationTestServer(t, server)
	now := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	validator := pinnedValidationTestValidator(t, server)
	validator.Now = func() time.Time { return now }

	result, err := validator.Validate(context.Background(), MCPOAuthValidationInput{
		Credential: CredentialMetadata{ID: "cred_test", VaultID: "vlt_test"},
		Auth: CredentialAuth{
			Type:         credentialAuthTypeMCPOAuth,
			MCPServerURL: "https://example.com/mcp",
			AccessToken:  oldAccess,
			Refresh: &CredentialOAuthRefresh{
				RefreshToken:  oldRefresh,
				ClientID:      "client-123",
				TokenEndpoint: "https://example.com/oauth",
				Scope:         "repo",
				Resource:      "https://api.example.com/mcp",
				TokenEndpointAuth: &CredentialTokenEndpointAuth{
					Type:         "client_secret_basic",
					ClientSecret: clientSecret,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if !sawRefresh || !sawProbe {
		t.Fatalf("refresh/probe not both executed: refresh=%v probe=%v", sawRefresh, sawProbe)
	}
	if result.Validation.Status != "valid" || result.Validation.Type != "vault_credential_validation" || !result.Validation.HasRefreshToken {
		t.Fatalf("validation = %+v", result.Validation)
	}
	if result.Validation.Refresh == nil || result.Validation.Refresh.Status != "succeeded" || result.Validation.Refresh.HTTPResponse == nil || result.Validation.Refresh.HTTPResponse.StatusCode != 200 {
		t.Fatalf("refresh check = %+v", result.Validation.Refresh)
	}
	if result.Validation.MCPProbe == nil || result.Validation.MCPProbe.Method != "initialize" || result.Validation.MCPProbe.HTTPResponse == nil || result.Validation.MCPProbe.HTTPResponse.StatusCode != 200 {
		t.Fatalf("probe check = %+v", result.Validation.MCPProbe)
	}
	if result.RefreshedAuth == nil || result.RefreshedAuth.AccessToken != newAccess ||
		result.RefreshedAuth.Refresh == nil || result.RefreshedAuth.Refresh.RefreshToken != newRefresh ||
		result.RefreshedAuth.ExpiresAt != "2026-05-01T01:00:00Z" {
		t.Fatalf("refreshed auth = %+v", result.RefreshedAuth)
	}
	public, err := json.Marshal(result.Validation)
	if err != nil {
		t.Fatalf("marshal validation: %v", err)
	}
	for _, forbidden := range []string{oldAccess, oldRefresh, clientSecret, newAccess, newRefresh, "tokens.example.com", "mcp.example.com"} {
		if strings.Contains(string(public), forbidden) {
			t.Fatalf("public validation leaked %q in %s", forbidden, string(public))
		}
	}
}

func TestHTTPMCPOAuthValidatorOmitsUnparsedRefreshResponseBody(t *testing.T) {
	const (
		issuedAccessToken  = "form-encoded-issued-access-token"
		issuedRefreshToken = "form-encoded-issued-refresh-token"
	)
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/token":
			writer.Header().Set("Content-Type", "application/x-www-form-urlencoded")
			_, _ = io.WriteString(writer, "access_token="+issuedAccessToken+"&refresh_token="+issuedRefreshToken)
		case "/mcp":
			writer.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(writer, `{"jsonrpc":"2.0","id":"validation","result":{}}`)
		default:
			t.Fatalf("unexpected URL path %s", request.URL.Path)
		}
	}))
	defer server.Close()
	trustValidationTestServer(t, server)
	validator := pinnedValidationTestValidator(t, server)
	validator.Now = func() time.Time { return time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC) }

	result, err := validator.Validate(context.Background(), MCPOAuthValidationInput{
		Credential: CredentialMetadata{ID: "cred_form_refresh", VaultID: "vlt_form_refresh"},
		Auth: CredentialAuth{
			Type:         credentialAuthTypeMCPOAuth,
			MCPServerURL: "https://example.com/mcp",
			AccessToken:  "stored-access",
			ExpiresAt:    "2000-01-01T00:00:00Z",
			Refresh: &CredentialOAuthRefresh{
				ClientID:          "public-client",
				RefreshToken:      "stored-refresh",
				TokenEndpoint:     "https://example.com/token",
				TokenEndpointAuth: &CredentialTokenEndpointAuth{Type: "none"},
			},
		},
	})
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if result.Validation.Refresh == nil || result.Validation.Refresh.HTTPResponse == nil {
		t.Fatalf("refresh check = %+v", result.Validation.Refresh)
	}
	if response := result.Validation.Refresh.HTTPResponse; response.StatusCode != http.StatusOK ||
		response.Body != "" || response.ContentType != "" {
		t.Fatalf("refresh HTTP response = %+v; want status only", response)
	}
	public, err := json.Marshal(result.Validation)
	if err != nil {
		t.Fatalf("marshal validation: %v", err)
	}
	for _, token := range []string{issuedAccessToken, issuedRefreshToken} {
		if strings.Contains(string(public), token) {
			t.Fatalf("public validation leaked issued token: %s", public)
		}
	}
}

func TestHTTPMCPOAuthValidatorOmitsPartialRefreshBodyOnReadFailure(t *testing.T) {
	const issuedToken = "partial-issued-access-token"
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/token":
			writer.Header().Set("Content-Type", "application/x-www-form-urlencoded")
			writer.Header().Set("Content-Length", "4096")
			_, _ = io.WriteString(writer, "access_token="+issuedToken)
		case "/mcp":
			writer.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(writer, `{"jsonrpc":"2.0","id":"validation","result":{}}`)
		default:
			t.Fatalf("unexpected URL path %s", request.URL.Path)
		}
	}))
	defer server.Close()
	trustValidationTestServer(t, server)
	validator := pinnedValidationTestValidator(t, server)
	validator.Now = func() time.Time { return time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC) }

	result, err := validator.Validate(context.Background(), MCPOAuthValidationInput{
		Credential: CredentialMetadata{ID: "cred_partial_refresh", VaultID: "vlt_partial_refresh"},
		Auth: CredentialAuth{
			Type:         credentialAuthTypeMCPOAuth,
			MCPServerURL: "https://example.com/mcp",
			AccessToken:  "stored-access",
			ExpiresAt:    "2000-01-01T00:00:00Z",
			Refresh: &CredentialOAuthRefresh{
				ClientID:          "public-client",
				RefreshToken:      "stored-refresh",
				TokenEndpoint:     "https://example.com/token",
				TokenEndpointAuth: &CredentialTokenEndpointAuth{Type: "none"},
			},
		},
	})
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if result.Validation.Refresh == nil || result.Validation.Refresh.Status != "connect_error" ||
		result.Validation.Refresh.HTTPResponse == nil {
		t.Fatalf("refresh check = %+v", result.Validation.Refresh)
	}
	if response := result.Validation.Refresh.HTTPResponse; response.StatusCode != http.StatusOK ||
		response.Body != "" || response.ContentType != "" {
		t.Fatalf("refresh HTTP response = %+v; want status only", response)
	}
	public, err := json.Marshal(result.Validation)
	if err != nil {
		t.Fatalf("marshal validation: %v", err)
	}
	if strings.Contains(string(public), issuedToken) {
		t.Fatalf("public validation leaked partial issued token: %s", public)
	}
}
