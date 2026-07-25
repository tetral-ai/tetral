package vault_test

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/vault"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestSDKCompatibilityVaultAndCredentialMetadataNullClearsAll(t *testing.T) {
	vaultPatch, err := vault.DecodeUpdateVaultRequest([]byte(`{"metadata":null}`))
	if err != nil {
		t.Fatalf("DecodeUpdateVaultRequest metadata:null: %v", err)
	}
	updatedVault, err := vaultPatch.Materialize(vault.Vault{Metadata: vault.StringMap{"team": "runtime"}})
	if err != nil {
		t.Fatalf("Materialize Vault metadata:null: %v", err)
	}
	if len(updatedVault.Metadata) != 0 {
		t.Fatalf("Vault metadata = %#v; want cleared", updatedVault.Metadata)
	}

	credentialPatch, err := vault.DecodeUpdateCredentialRequest([]byte(`{"metadata":null}`))
	if err != nil {
		t.Fatalf("DecodeUpdateCredentialRequest metadata:null: %v", err)
	}
	updatedCredential, err := credentialPatch.Materialize(vault.Credential{Metadata: vault.StringMap{"team": "runtime"}})
	if err != nil {
		t.Fatalf("Materialize Credential metadata:null: %v", err)
	}
	if len(updatedCredential.Metadata) != 0 {
		t.Fatalf("Credential metadata = %#v; want cleared", updatedCredential.Metadata)
	}
}

func TestSDKCompatibilityProviderOAuthDerivesRefreshPresenceForOldRows(t *testing.T) {
	runtime, admin, encryptor := newPGVaultEnv(t)
	ctx := context.Background()
	vaultStore := vault.NewPostgreSQLVaultStore(dbconnect.NewClientForTesting(runtime))
	credentialStore := vault.NewPostgreSQLCredentialStore(dbconnect.NewClientForTesting(runtime), encryptor)
	container, err := vaultStore.Create(ctx, workspace.DefaultID, vault.CreateVaultRequest{DisplayName: "provider auth"})
	if err != nil {
		t.Fatalf("Create Vault: %v", err)
	}

	withRefresh, err := credentialStore.Create(ctx, workspace.DefaultID, container.ID, vault.CreateCredentialRequest{
		Auth: vault.CredentialAuth{
			Type:         "provider_oauth",
			ProviderID:   "anthropic",
			AccessMode:   "workspace",
			AccessToken:  "provider-access-sentinel",
			RefreshToken: "provider-refresh-sentinel",
		},
	})
	if err != nil {
		t.Fatalf("Create provider_oauth: %v", err)
	}
	if _, err := admin.ExecContext(ctx,
		`UPDATE credentials SET auth_public_json = $1 WHERE id = $2`,
		`{"type":"provider_oauth","provider_id":"anthropic","access_mode":"workspace"}`,
		withRefresh.ID,
	); err != nil {
		t.Fatalf("downgrade auth_public_json to old-row shape: %v", err)
	}
	oldRow, err := credentialStore.GetMetadata(ctx, workspace.DefaultID, container.ID, withRefresh.ID)
	if err != nil {
		t.Fatalf("Get old provider_oauth row: %v", err)
	}
	assertProviderOAuthRefreshField(t, oldRow.Auth, true)

	withoutRefresh, err := credentialStore.Create(ctx, workspace.DefaultID, container.ID, vault.CreateCredentialRequest{
		Auth: vault.CredentialAuth{
			Type:        "provider_oauth",
			ProviderID:  "openai",
			AccessMode:  "workspace",
			AccessToken: "provider-access-without-refresh",
		},
	})
	if err != nil {
		t.Fatalf("Create provider_oauth without refresh: %v", err)
	}
	assertProviderOAuthRefreshField(t, withoutRefresh.Auth, false)

	staticBearer, err := credentialStore.Create(ctx, workspace.DefaultID, container.ID, vault.CreateCredentialRequest{
		Auth: staticBearerAuth("static-provider-refresh-field"),
	})
	if err != nil {
		t.Fatalf("Create static bearer: %v", err)
	}
	body, err := json.Marshal(staticBearer.Auth)
	if err != nil {
		t.Fatalf("Marshal static bearer auth: %v", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(body, &fields); err != nil {
		t.Fatalf("Unmarshal static bearer auth: %v", err)
	}
	if _, ok := fields["has_refresh_token"]; ok {
		t.Fatalf("non-provider auth emitted has_refresh_token: %s", body)
	}
}

func TestSDKCompatibilityCredentialValidationUsesSDKShapeAndScrubbedHTTPResponse(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/mcp" {
			t.Fatalf("unexpected validation path %s", request.URL.Path)
		}
		writer.Header().Set("Content-Type", "application/json; charset=utf-8")
		_, _ = io.WriteString(writer, `{"jsonrpc":"2.0","id":"validation","result":{}}`)
	}))
	defer server.Close()
	trustSDKValidationServer(t, server)
	validator := sdkPinnedValidator(server)
	result, err := validator.Validate(context.Background(), vault.MCPOAuthValidationInput{
		Credential: vault.CredentialMetadata{ID: "cred_test", VaultID: "vlt_test"},
		Auth: vault.CredentialAuth{
			Type:         "mcp_oauth",
			MCPServerURL: "https://example.com/mcp",
			AccessToken:  "probe-access-token-sentinel",
		},
	})
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	body, err := json.Marshal(result.Validation)
	if err != nil {
		t.Fatalf("Marshal validation: %v", err)
	}
	var response map[string]any
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("Unmarshal validation: %v", err)
	}
	if response["type"] != "vault_credential_validation" || response["status"] != "valid" || response["credential_id"] != "cred_test" || response["vault_id"] != "vlt_test" || response["has_refresh_token"] != false {
		t.Fatalf("validation top-level shape = %#v", response)
	}
	refresh, ok := response["refresh"].(map[string]any)
	if !ok || refresh["status"] != "no_refresh_token" || refresh["http_response"] != nil {
		t.Fatalf("refresh = %#v; want no_refresh_token/null response", response["refresh"])
	}
	probe, ok := response["mcp_probe"].(map[string]any)
	if !ok || probe["method"] != "initialize" {
		t.Fatalf("mcp_probe = %#v; want initialize", response["mcp_probe"])
	}
	httpResponse, ok := probe["http_response"].(map[string]any)
	if !ok || httpResponse["status_code"] != float64(http.StatusOK) || httpResponse["content_type"] != "application/json" || httpResponse["body_truncated"] != false {
		t.Fatalf("mcp_probe.http_response = %#v", probe["http_response"])
	}
	if bodyText, ok := httpResponse["body"].(string); !ok || !strings.Contains(bodyText, `"jsonrpc":"2.0"`) {
		t.Fatalf("mcp_probe.http_response.body = %#v; want bounded response body", httpResponse["body"])
	}
	if strings.Contains(string(body), "probe-access-token-sentinel") {
		t.Fatalf("validation leaked access token: %s", body)
	}
}

func TestSDKCompatibilityCredentialValidationTransportFailureIsUnknown(t *testing.T) {
	validator := vault.HTTPMCPOAuthValidator{
		Resolver: sdkValidationResolverFunc(func(context.Context, string) ([]net.IPAddr, error) {
			return []net.IPAddr{{IP: net.ParseIP("93.184.216.34")}}, nil
		}),
		DialContext: func(context.Context, string, string) (net.Conn, error) {
			return nil, errors.New("dial refused at secret-internal-address")
		},
	}
	result, err := validator.Validate(context.Background(), vault.MCPOAuthValidationInput{
		Credential: vault.CredentialMetadata{ID: "cred_test", VaultID: "vlt_test"},
		Auth:       vault.CredentialAuth{Type: "mcp_oauth", MCPServerURL: "https://mcp.example.com/mcp", AccessToken: "secret"},
	})
	if err != nil {
		t.Fatalf("Validate transport failure: %v", err)
	}
	body, _ := json.Marshal(result.Validation)
	var response map[string]any
	_ = json.Unmarshal(body, &response)
	if response["status"] != "unknown" {
		t.Fatalf("status = %#v; want unknown (%s)", response["status"], body)
	}
	probe, ok := response["mcp_probe"].(map[string]any)
	if !ok || probe["method"] != "initialize" || probe["http_response"] != nil {
		t.Fatalf("mcp_probe = %#v; want initialize with null response", response["mcp_probe"])
	}
	if strings.Contains(string(body), "secret-internal-address") {
		t.Fatalf("validation leaked transport detail: %s", body)
	}
}

func assertProviderOAuthRefreshField(t *testing.T, auth vault.CredentialAuthPublic, want bool) {
	t.Helper()
	body, err := json.Marshal(auth)
	if err != nil {
		t.Fatalf("Marshal provider_oauth auth: %v", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(body, &fields); err != nil {
		t.Fatalf("Unmarshal provider_oauth auth: %v", err)
	}
	if fields["has_refresh_token"] != want {
		t.Fatalf("has_refresh_token = %#v; want %v (%s)", fields["has_refresh_token"], want, body)
	}
	if strings.Contains(string(body), "provider-refresh-sentinel") {
		t.Fatalf("public auth leaked refresh token: %s", body)
	}
}

type sdkValidationResolverFunc func(context.Context, string) ([]net.IPAddr, error)

func (f sdkValidationResolverFunc) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	return f(ctx, host)
}

type sdkValidationRemoteConn struct {
	net.Conn
}

func (c *sdkValidationRemoteConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("93.184.216.34"), Port: 443}
}

func sdkPinnedValidator(server *httptest.Server) vault.HTTPMCPOAuthValidator {
	return vault.HTTPMCPOAuthValidator{
		Resolver: sdkValidationResolverFunc(func(context.Context, string) ([]net.IPAddr, error) {
			return []net.IPAddr{{IP: net.ParseIP("93.184.216.34")}}, nil
		}),
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
			if err != nil {
				return nil, err
			}
			return &sdkValidationRemoteConn{Conn: conn}, nil
		},
	}
}

func trustSDKValidationServer(t *testing.T, server *httptest.Server) {
	t.Helper()
	certificate := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.TLS.Certificates[0].Certificate[0]})
	path := filepath.Join(t.TempDir(), "sdk-validation-root.pem")
	if err := os.WriteFile(path, certificate, 0o600); err != nil {
		t.Fatalf("write SDK validation root: %v", err)
	}
	t.Setenv("SSL_CERT_FILE", path)
}
