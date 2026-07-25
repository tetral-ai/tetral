package vault

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestDecodeCreateVaultRequestStrictShapeAndNormalization(t *testing.T) {
	request, err := DecodeCreateVaultRequest([]byte(`{"display_name":"Primary","metadata":{"team":"runtime"}}`))
	if err != nil {
		t.Fatalf("DecodeCreateVaultRequest: %v", err)
	}
	if request.DisplayName != "Primary" {
		t.Fatalf("DisplayName = %q; want Primary", request.DisplayName)
	}
	if request.Metadata["team"] != "runtime" {
		t.Fatalf("Metadata = %v; want team=runtime", request.Metadata)
	}

	encoded, err := json.Marshal(Vault{Type: "vault", Metadata: nil})
	if err != nil {
		t.Fatalf("Marshal Vault: %v", err)
	}
	if !strings.Contains(string(encoded), `"metadata":{}`) {
		t.Fatalf("Vault metadata must encode as object, got %s", encoded)
	}
}

func TestDecodeCreateVaultRequestRejectsUnknownAndServerFields(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{"unknown", `{"unexpected":"value","display_name":"Primary"}`, "unexpected"},
		{"workspace", `{"display_name":"Primary","workspace_id":"ws"}`, "workspace_id"},
		{"id", `{"id":"vlt_x","display_name":"Primary"}`, "id"},
		{"type", `{"type":"vault","display_name":"Primary"}`, "type"},
		{"created_at", `{"created_at":"2026-01-01T00:00:00Z","display_name":"Primary"}`, "created_at"},
		{"updated_at", `{"updated_at":"2026-01-01T00:00:00Z","display_name":"Primary"}`, "updated_at"},
		{"archived_at", `{"archived_at":null,"display_name":"Primary"}`, "archived_at"},
		{"trailing", `{"display_name":"Primary"} {"display_name":"Second"}`, "exactly one"},
		{"null body", `null`, "object"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeCreateVaultRequest([]byte(tc.body))
			if err == nil {
				t.Fatal("must reject")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q must mention %q", err.Error(), tc.want)
			}
		})
	}
}

func TestDecodeUpdateVaultRequestRejectsUnknownAndServerFields(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{"unknown", `{"unexpected":"value","display_name":"Primary"}`, "unexpected"},
		{"workspace", `{"workspace_id":"ws","display_name":"Primary"}`, "workspace_id"},
		{"id", `{"id":"vlt_x","display_name":"Primary"}`, "id"},
		{"type", `{"type":"vault","display_name":"Primary"}`, "type"},
		{"created_at", `{"created_at":"2026-01-01T00:00:00Z","display_name":"Primary"}`, "created_at"},
		{"updated_at", `{"updated_at":"2026-01-01T00:00:00Z","display_name":"Primary"}`, "updated_at"},
		{"archived_at", `{"archived_at":null,"display_name":"Primary"}`, "archived_at"},
		{"trailing", `{"display_name":"Primary"} []`, "exactly one"},
		{"null body", `null`, "object"},
		{"wrong metadata", `{"metadata":[]}`, "metadata"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeUpdateVaultRequest([]byte(tc.body))
			if err == nil {
				t.Fatal("must reject")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q must mention %q", err.Error(), tc.want)
			}
		})
	}
}

func TestDecodeVaultUpdatePatchMergesMetadataAndValidatesDisplayName(t *testing.T) {
	patch, err := DecodeUpdateVaultRequest([]byte(`{"display_name":"Updated","metadata":{"keep":"new","delete":null,"empty":""}}`))
	if err != nil {
		t.Fatalf("DecodeUpdateVaultRequest: %v", err)
	}
	current := Vault{
		DisplayName: "Current",
		Metadata: StringMap{
			"keep":   "old",
			"delete": "value",
			"empty":  "value",
		},
	}
	updated, err := patch.Materialize(current)
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	if updated.DisplayName != "Updated" {
		t.Fatalf("DisplayName = %q; want Updated", updated.DisplayName)
	}
	if updated.Metadata["keep"] != "new" {
		t.Fatalf("keep metadata = %q; want new", updated.Metadata["keep"])
	}
	if _, ok := updated.Metadata["delete"]; ok {
		t.Fatalf("null metadata value must delete key: %v", updated.Metadata)
	}
	if updated.Metadata["empty"] != "" {
		t.Fatalf("empty metadata value = %q; want stored empty string", updated.Metadata["empty"])
	}

	nullNamePatch, err := DecodeUpdateVaultRequest([]byte(`{"display_name":null}`))
	if err != nil {
		t.Fatalf("DecodeUpdateVaultRequest null display_name: %v", err)
	}
	nullNameUpdated, err := nullNamePatch.Materialize(current)
	if err != nil {
		t.Fatalf("Materialize null display_name: %v", err)
	}
	if nullNameUpdated.DisplayName != "" {
		t.Fatalf("null display_name must clear display name, got %q", nullNameUpdated.DisplayName)
	}
}

func TestDecodeVaultRequestsRejectDisplayNameLimits(t *testing.T) {
	overlong := strings.Repeat("界", 256)
	cases := []struct {
		name string
		body string
		fn   func([]byte) error
	}{
		{"create missing", `{}`, func(body []byte) error { _, err := DecodeCreateVaultRequest(body); return err }},
		{"create empty", `{"display_name":""}`, func(body []byte) error { _, err := DecodeCreateVaultRequest(body); return err }},
		{"create overlong", `{"display_name":"` + overlong + `"}`, func(body []byte) error { _, err := DecodeCreateVaultRequest(body); return err }},
		{"update empty", `{"display_name":""}`, func(body []byte) error { _, err := DecodeUpdateVaultRequest(body); return err }},
		{"update overlong", `{"display_name":"` + overlong + `"}`, func(body []byte) error { _, err := DecodeUpdateVaultRequest(body); return err }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.fn([]byte(tc.body))
			if err == nil {
				t.Fatal("must reject")
			}
			if !strings.Contains(err.Error(), "display_name") {
				t.Fatalf("error %q must mention display_name", err.Error())
			}
		})
	}
}

func TestCredentialCreateDecodersCoverAuthVariants(t *testing.T) {
	oauth, err := DecodeCreateCredentialRequest([]byte(`{"display_name":"Notion","metadata":{"kind":"mcp"},"auth":{"type":"mcp_oauth","mcp_server_url":"https://MCP.Example.COM/mcp","access_token":"at-secret","expires_at":"2026-05-03T00:00:00Z","refresh":{"refresh_token":"rt-secret","client_id":"client","token_endpoint":"https://TOKEN.Example.COM/oauth/token","scope":"read","resource":"https://resource.example.com","token_endpoint_auth":{"type":"client_secret_basic","client_secret":"cs-secret"}}}}`))
	if err != nil {
		t.Fatalf("Decode mcp_oauth: %v", err)
	}
	if oauth.Auth.MCPServerURL != "https://mcp.example.com/mcp" {
		t.Fatalf("MCPServerURL = %q; want canonical lowercase host", oauth.Auth.MCPServerURL)
	}
	if oauth.Auth.Refresh == nil || oauth.Auth.Refresh.TokenEndpoint != "https://token.example.com/oauth/token" {
		t.Fatalf("refresh token endpoint not canonicalized: %+v", oauth.Auth.Refresh)
	}
	if oauth.Auth.Refresh.TokenEndpointAuth == nil || oauth.Auth.Refresh.TokenEndpointAuth.ClientSecret != "cs-secret" {
		t.Fatalf("refresh auth secret not decoded: %+v", oauth.Auth.Refresh.TokenEndpointAuth)
	}

	staticBearer, err := DecodeCreateCredentialRequest([]byte(`{"auth":{"type":"static_bearer","mcp_server_url":"https://Bearer.Example.COM/mcp","token":"bearer-secret"}}`))
	if err != nil {
		t.Fatalf("Decode static_bearer: %v", err)
	}
	if staticBearer.Auth.MCPServerURL != "https://bearer.example.com/mcp" || staticBearer.Auth.Token != "bearer-secret" {
		t.Fatalf("static bearer auth = %+v", staticBearer.Auth)
	}

	providerAPIKey, err := DecodeCreateCredentialRequest([]byte(`{"auth":{"type":"provider_api_key","provider_id":"anthropic","access_mode":"user_api_key","token":"provider-key-secret"}}`))
	if err != nil {
		t.Fatalf("Decode provider_api_key: %v", err)
	}
	if providerAPIKey.Auth.ProviderID != "anthropic" || providerAPIKey.Auth.AccessMode != "user_api_key" || providerAPIKey.Auth.Token != "provider-key-secret" {
		t.Fatalf("provider API key auth = %+v", providerAPIKey.Auth)
	}

	providerOAuth, err := DecodeCreateCredentialRequest([]byte(`{"auth":{"type":"provider_oauth","provider_id":"openai","access_mode":"oauth","access_token":"provider-access-secret","refresh_token":"provider-refresh-secret","expires_at":"2026-05-03T00:00:00Z","account_id":"acct_123"}}`))
	if err != nil {
		t.Fatalf("Decode provider_oauth: %v", err)
	}
	if providerOAuth.Auth.ProviderID != "openai" ||
		providerOAuth.Auth.AccessMode != "oauth" ||
		providerOAuth.Auth.AccessToken != "provider-access-secret" ||
		providerOAuth.Auth.RefreshToken != "provider-refresh-secret" ||
		providerOAuth.Auth.ExpiresAt != "2026-05-03T00:00:00Z" ||
		providerOAuth.Auth.AccountID != "acct_123" {
		t.Fatalf("provider oauth auth = %+v", providerOAuth.Auth)
	}
}

func TestMCPOAuthDecoderAcceptsContractNullableFields(t *testing.T) {
	created, err := DecodeCreateCredentialRequest([]byte(`{"auth":{"type":"mcp_oauth","mcp_server_url":"https://mcp.example.com","access_token":"secret","expires_at":null,"refresh":null}}`))
	if err != nil {
		t.Fatalf("Decode create nullable mcp_oauth: %v", err)
	}
	if created.Auth.ExpiresAt != "" || created.Auth.Refresh != nil {
		t.Fatalf("create nullable auth = %+v", created.Auth)
	}

	for _, body := range []string{
		`{"auth":{"expires_at":null,"refresh":null}}`,
		`{"auth":{"refresh":{"scope":null,"resource":null}}}`,
	} {
		if _, err := DecodeUpdateCredentialRequest([]byte(body)); err != nil {
			t.Fatalf("Decode nullable update %s: %v", body, err)
		}
	}
}

func TestCredentialUpdateDecodersCoverAuthVariants(t *testing.T) {
	oauthWithoutType, err := DecodeUpdateCredentialRequest([]byte(`{"auth":{"access_token":"at-rotated","refresh":{"refresh_token":"rt-rotated","scope":"write","token_endpoint_auth":{"type":"client_secret_post","client_secret":"cs-rotated"}}}}`))
	if err != nil {
		t.Fatalf("Decode mcp_oauth update without type: %v", err)
	}
	if oauthWithoutType.Auth == nil || oauthWithoutType.Auth.Type != "" || oauthWithoutType.Auth.AccessToken != "at-rotated" {
		t.Fatalf("mcp_oauth update without type auth = %+v", oauthWithoutType.Auth)
	}
	if oauthWithoutType.Auth.Refresh == nil || oauthWithoutType.Auth.Refresh.RefreshToken != "rt-rotated" || oauthWithoutType.Auth.Refresh.Scope != "write" {
		t.Fatalf("mcp_oauth update without type refresh = %+v", oauthWithoutType.Auth.Refresh)
	}

	oauth, err := DecodeUpdateCredentialRequest([]byte(`{"auth":{"type":"mcp_oauth","mcp_server_url":"https://MCP.Example.COM/mcp","access_token":"at-rotated","expires_at":"2026-05-03T00:00:00Z","refresh":{"refresh_token":"rt-rotated","token_endpoint":"https://TOKEN.Example.COM/oauth/token","scope":"write","token_endpoint_auth":{"type":"client_secret_post","client_secret":"cs-rotated"}}}}`))
	if err != nil {
		t.Fatalf("Decode mcp_oauth update: %v", err)
	}
	if oauth.Auth == nil || oauth.Auth.MCPServerURL != "https://mcp.example.com/mcp" || oauth.Auth.AccessToken != "at-rotated" {
		t.Fatalf("mcp_oauth update auth = %+v", oauth.Auth)
	}
	if oauth.Auth.Refresh == nil || oauth.Auth.Refresh.TokenEndpoint != "https://token.example.com/oauth/token" {
		t.Fatalf("mcp_oauth update refresh = %+v", oauth.Auth.Refresh)
	}
	if oauth.Auth.Refresh.TokenEndpointAuth == nil || oauth.Auth.Refresh.TokenEndpointAuth.Type != "client_secret_post" || oauth.Auth.Refresh.TokenEndpointAuth.ClientSecret != "cs-rotated" {
		t.Fatalf("mcp_oauth update endpoint auth = %+v", oauth.Auth.Refresh.TokenEndpointAuth)
	}

	staticBearerWithoutType, err := DecodeUpdateCredentialRequest([]byte(`{"auth":{"token":"rotated-bearer"}}`))
	if err != nil {
		t.Fatalf("Decode static_bearer update without type: %v", err)
	}
	if staticBearerWithoutType.Auth == nil || staticBearerWithoutType.Auth.Type != "" || staticBearerWithoutType.Auth.Token != "rotated-bearer" {
		t.Fatalf("static bearer update without type auth = %+v", staticBearerWithoutType.Auth)
	}

	staticBearer, err := DecodeUpdateCredentialRequest([]byte(`{"auth":{"type":"static_bearer","mcp_server_url":"https://Bearer.Example.COM/mcp","token":"rotated-bearer"}}`))
	if err != nil {
		t.Fatalf("Decode static_bearer update: %v", err)
	}
	if staticBearer.Auth == nil || staticBearer.Auth.MCPServerURL != "https://bearer.example.com/mcp" || staticBearer.Auth.Token != "rotated-bearer" {
		t.Fatalf("static bearer update auth = %+v", staticBearer.Auth)
	}

	providerAPIKeyWithoutType, err := DecodeUpdateCredentialRequest([]byte(`{"auth":{"token":"rotated-provider-key"}}`))
	if err != nil {
		t.Fatalf("Decode provider_api_key update without type: %v", err)
	}
	if providerAPIKeyWithoutType.Auth == nil || providerAPIKeyWithoutType.Auth.Type != "" || providerAPIKeyWithoutType.Auth.Token != "rotated-provider-key" {
		t.Fatalf("provider API key update without type auth = %+v", providerAPIKeyWithoutType.Auth)
	}

	providerAPIKey, err := DecodeUpdateCredentialRequest([]byte(`{"auth":{"type":"provider_api_key","provider_id":"anthropic","access_mode":"user_api_key","token":"rotated-provider-key"}}`))
	if err != nil {
		t.Fatalf("Decode provider_api_key update: %v", err)
	}
	if providerAPIKey.Auth == nil || providerAPIKey.Auth.ProviderID != "anthropic" || providerAPIKey.Auth.AccessMode != "user_api_key" || providerAPIKey.Auth.Token != "rotated-provider-key" {
		t.Fatalf("provider API key update auth = %+v", providerAPIKey.Auth)
	}

	providerOAuthWithoutType, err := DecodeUpdateCredentialRequest([]byte(`{"auth":{"access_token":"rotated-provider-access","refresh_token":"rotated-provider-refresh","expires_at":"2026-05-03T00:00:00Z","account_id":"acct_456"}}`))
	if err != nil {
		t.Fatalf("Decode provider_oauth update without type: %v", err)
	}
	if providerOAuthWithoutType.Auth == nil ||
		providerOAuthWithoutType.Auth.Type != "" ||
		providerOAuthWithoutType.Auth.AccessToken != "rotated-provider-access" ||
		providerOAuthWithoutType.Auth.RefreshToken != "rotated-provider-refresh" ||
		providerOAuthWithoutType.Auth.AccountID != "acct_456" {
		t.Fatalf("provider oauth update without type auth = %+v", providerOAuthWithoutType.Auth)
	}

	providerOAuth, err := DecodeUpdateCredentialRequest([]byte(`{"auth":{"type":"provider_oauth","provider_id":"openai","access_mode":"oauth","access_token":"rotated-provider-access","refresh_token":"rotated-provider-refresh","expires_at":"2026-05-03T00:00:00Z","account_id":"acct_456"}}`))
	if err != nil {
		t.Fatalf("Decode provider_oauth update: %v", err)
	}
	if providerOAuth.Auth == nil ||
		providerOAuth.Auth.ProviderID != "openai" ||
		providerOAuth.Auth.AccessMode != "oauth" ||
		providerOAuth.Auth.AccessToken != "rotated-provider-access" ||
		providerOAuth.Auth.RefreshToken != "rotated-provider-refresh" ||
		providerOAuth.Auth.ExpiresAt != "2026-05-03T00:00:00Z" ||
		providerOAuth.Auth.AccountID != "acct_456" {
		t.Fatalf("provider oauth update auth = %+v", providerOAuth.Auth)
	}
}

func TestCredentialUpdateDecoderRejectsEmptySecretRotationFields(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{
			name: "static bearer without type",
			body: `{"auth":{"token":""}}`,
			want: "auth.token",
		},
		{
			name: "static bearer with type",
			body: `{"auth":{"type":"static_bearer","token":""}}`,
			want: "auth.token",
		},
		{
			name: "oauth access without type",
			body: `{"auth":{"access_token":""}}`,
			want: "auth.access_token",
		},
		{
			name: "oauth access with type",
			body: `{"auth":{"type":"mcp_oauth","access_token":""}}`,
			want: "auth.access_token",
		},
		{
			name: "oauth refresh without type",
			body: `{"auth":{"refresh":{"refresh_token":""}}}`,
			want: "auth.refresh.refresh_token",
		},
		{
			name: "oauth refresh with type",
			body: `{"auth":{"type":"mcp_oauth","refresh":{"refresh_token":""}}}`,
			want: "auth.refresh.refresh_token",
		},
		{
			name: "provider oauth top-level refresh without type",
			body: `{"auth":{"refresh_token":""}}`,
			want: "auth.refresh_token",
		},
		{
			name: "provider oauth top-level refresh with type",
			body: `{"auth":{"type":"provider_oauth","refresh_token":""}}`,
			want: "auth.refresh_token",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeUpdateCredentialRequest([]byte(tc.body))
			if err == nil {
				t.Fatal("must reject")
			}
			var validationErr *ValidationError
			if !errors.As(err, &validationErr) {
				t.Fatalf("error = %T %v; want ValidationError", err, err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q must mention %q", err.Error(), tc.want)
			}
		})
	}
}

func TestCredentialUpdateDecoderRejectsEmptyOAuthPublicFields(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{
			name: "expires_at without type",
			body: `{"auth":{"expires_at":""}}`,
			want: "auth.expires_at",
		},
		{
			name: "expires_at with type",
			body: `{"auth":{"type":"mcp_oauth","expires_at":""}}`,
			want: "auth.expires_at",
		},
		{
			name: "refresh scope without type",
			body: `{"auth":{"refresh":{"scope":""}}}`,
			want: "auth.refresh.scope",
		},
		{
			name: "refresh scope with type",
			body: `{"auth":{"type":"mcp_oauth","refresh":{"scope":""}}}`,
			want: "auth.refresh.scope",
		},
		{
			name: "provider id without type",
			body: `{"auth":{"provider_id":""}}`,
			want: "auth.provider_id",
		},
		{
			name: "provider id with type",
			body: `{"auth":{"type":"provider_api_key","provider_id":""}}`,
			want: "auth.provider_id",
		},
		{
			name: "access mode without type",
			body: `{"auth":{"access_mode":""}}`,
			want: "auth.access_mode",
		},
		{
			name: "provider account id",
			body: `{"auth":{"type":"provider_oauth","account_id":""}}`,
			want: "auth.account_id",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeUpdateCredentialRequest([]byte(tc.body))
			if err == nil {
				t.Fatal("must reject")
			}
			var validationErr *ValidationError
			if !errors.As(err, &validationErr) {
				t.Fatalf("error = %T %v; want ValidationError", err, err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q must mention %q", err.Error(), tc.want)
			}
		})
	}
}

func TestCredentialCreateDecoderRejectsStrictAndSecretBearingInvalidInputsSafely(t *testing.T) {
	const secret = "sentinel-secret-value"
	cases := []struct {
		name string
		body string
		want string
	}{
		{"server id", `{"id":"cred_x","auth":{"type":"static_bearer","mcp_server_url":"https://example.com/mcp","token":"` + secret + `"}}`, "id"},
		{"workspace", `{"workspace_id":"ws","auth":{"type":"static_bearer","mcp_server_url":"https://example.com/mcp","token":"` + secret + `"}}`, "workspace_id"},
		{"type", `{"type":"vault_credential","auth":{"type":"static_bearer","mcp_server_url":"https://example.com/mcp","token":"` + secret + `"}}`, "type"},
		{"unknown auth", `{"auth":{"type":"static_bearer","mcp_server_url":"https://example.com/mcp","token":"` + secret + `","extra":"` + secret + `"}}`, "auth.extra"},
		{"unknown refresh", `{"auth":{"type":"mcp_oauth","mcp_server_url":"https://example.com/mcp","access_token":"` + secret + `","refresh":{"refresh_token":"` + secret + `","client_id":"client","token_endpoint":"https://example.com/token","token_endpoint_auth":{"type":"none"},"extra":"` + secret + `"}}}`, "auth.refresh.extra"},
		{"unknown token endpoint auth", `{"auth":{"type":"mcp_oauth","mcp_server_url":"https://example.com/mcp","access_token":"` + secret + `","refresh":{"refresh_token":"` + secret + `","client_id":"client","token_endpoint":"https://example.com/token","token_endpoint_auth":{"type":"none","extra":"` + secret + `"}}}}`, "auth.refresh.token_endpoint_auth.extra"},
		{"none with client secret", `{"auth":{"type":"mcp_oauth","mcp_server_url":"https://example.com/mcp","access_token":"` + secret + `","refresh":{"refresh_token":"` + secret + `","client_id":"client","token_endpoint":"https://example.com/token","token_endpoint_auth":{"type":"none","client_secret":"` + secret + `"}}}}`, "auth.refresh.token_endpoint_auth.client_secret"},
		{"unsafe mcp url", `{"auth":{"type":"static_bearer","mcp_server_url":"https://user:` + secret + `@localhost/mcp","token":"` + secret + `"}}`, "auth.mcp_server_url"},
		{"unsafe token endpoint", `{"auth":{"type":"mcp_oauth","mcp_server_url":"https://example.com/mcp","access_token":"` + secret + `","refresh":{"refresh_token":"` + secret + `","client_id":"client","token_endpoint":"https://user:` + secret + `@localhost/token","token_endpoint_auth":{"type":"none"}}}}`, "auth.refresh.token_endpoint"},
		{"malformed mcp dns host", `{"auth":{"type":"static_bearer","mcp_server_url":"https://example..com/mcp","token":"` + secret + `"}}`, "auth.mcp_server_url"},
		{"trailing", `{"auth":{"type":"static_bearer","mcp_server_url":"https://example.com/mcp","token":"` + secret + `"}} []`, "exactly one"},
		{"unsupported auth type", `{"auth":{"type":"password","token":"` + secret + `"}}`, "auth.type"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeCreateCredentialRequest([]byte(tc.body))
			if err == nil {
				t.Fatal("must reject")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q must mention %q", err.Error(), tc.want)
			}
			for _, forbidden := range []string{secret, "user:" + secret, "https://user:" + secret + "@localhost", "example..com", "https://example..com/mcp"} {
				if strings.Contains(err.Error(), forbidden) {
					t.Fatalf("error %q leaked forbidden substring %q", err.Error(), forbidden)
				}
			}
		})
	}
}

func TestCredentialAuthDecoderReportsSupportedAuthVariants(t *testing.T) {
	createErr := func() error {
		_, err := DecodeCreateCredentialRequest([]byte(`{"auth":{"type":"password","token":"secret"}}`))
		return err
	}()
	if createErr == nil {
		t.Fatal("create must reject unsupported auth type")
	}
	if got, want := createErr.Error(), unsupportedCredentialAuthTypeError().Error(); got != want {
		t.Fatalf("create unsupported-auth error = %q; want %q", got, want)
	}

	updateErr := func() error {
		_, err := DecodeUpdateCredentialRequest([]byte(`{"auth":{"type":"unsupported","token":"secret"}}`))
		return err
	}()
	if updateErr == nil {
		t.Fatal("update must reject unsupported auth type")
	}
	if got, want := updateErr.Error(), unsupportedCredentialAuthTypeError().Error(); got != want {
		t.Fatalf("update unsupported-auth error = %q; want %q", got, want)
	}
}

func TestCredentialUpdateDecoderRejectsStrictAndSecretBearingInvalidInputsSafely(t *testing.T) {
	const secret = "sentinel-secret-value"
	cases := []struct {
		name string
		body string
		want string
	}{
		{"server id", `{"id":"cred_x","auth":{"type":"static_bearer","token":"` + secret + `"}}`, "id"},
		{"workspace", `{"workspace_id":"ws","auth":{"type":"static_bearer","token":"` + secret + `"}}`, "workspace_id"},
		{"type", `{"type":"vault_credential","auth":{"type":"static_bearer","token":"` + secret + `"}}`, "type"},
		{"archived_at", `{"archived_at":null,"auth":{"type":"static_bearer","token":"` + secret + `"}}`, "archived_at"},
		{"unknown auth", `{"auth":{"type":"static_bearer","token":"` + secret + `","extra":"` + secret + `"}}`, "auth.extra"},
		{"unknown refresh", `{"auth":{"type":"mcp_oauth","refresh":{"refresh_token":"` + secret + `","token_endpoint_auth":{"type":"client_secret_basic","client_secret":"` + secret + `"},"extra":"` + secret + `"}}}`, "auth.refresh.extra"},
		{"unknown token endpoint auth", `{"auth":{"type":"mcp_oauth","refresh":{"token_endpoint_auth":{"type":"client_secret_basic","client_secret":"` + secret + `","extra":"` + secret + `"}}}}`, "auth.refresh.token_endpoint_auth.extra"},
		{"none token endpoint auth", `{"auth":{"type":"mcp_oauth","refresh":{"token_endpoint_auth":{"type":"none","client_secret":"` + secret + `"}}}}`, "auth.refresh.token_endpoint_auth.type"},
		{"basic missing client secret", `{"auth":{"type":"mcp_oauth","refresh":{"token_endpoint_auth":{"type":"client_secret_basic"}}}}`, "auth.refresh.token_endpoint_auth.client_secret"},
		{"post missing client secret", `{"auth":{"type":"mcp_oauth","refresh":{"token_endpoint_auth":{"type":"client_secret_post"}}}}`, "auth.refresh.token_endpoint_auth.client_secret"},
		{"unsafe mcp url", `{"auth":{"type":"static_bearer","mcp_server_url":"https://user:` + secret + `@localhost/mcp","token":"` + secret + `"}}`, "auth.mcp_server_url"},
		{"unsafe token endpoint", `{"auth":{"type":"mcp_oauth","refresh":{"token_endpoint":"https://user:` + secret + `@localhost/token","token_endpoint_auth":{"type":"client_secret_basic","client_secret":"` + secret + `"}}}}`, "auth.refresh.token_endpoint"},
		{"trailing", `{"auth":{"type":"static_bearer","token":"` + secret + `"}} []`, "exactly one"},
		{"wrong auth", `{"auth":[]}`, "auth"},
		{"null auth", `{"auth":null}`, "auth"},
		{"unsupported auth type", `{"auth":{"type":"unsupported","token":"` + secret + `"}}`, "auth.type"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeUpdateCredentialRequest([]byte(tc.body))
			if err == nil {
				t.Fatal("must reject")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q must mention %q", err.Error(), tc.want)
			}
			for _, forbidden := range []string{secret, "user:" + secret, "https://user:" + secret + "@localhost"} {
				if strings.Contains(err.Error(), forbidden) {
					t.Fatalf("error %q leaked forbidden substring %q", err.Error(), forbidden)
				}
			}
		})
	}
}

func TestCredentialUpdatePatchValidatesMutableShape(t *testing.T) {
	patch, err := DecodeUpdateCredentialRequest([]byte(`{"display_name":"Updated","metadata":{"keep":"new","delete":null},"auth":{"type":"static_bearer","token":"rotated-secret"}}`))
	if err != nil {
		t.Fatalf("DecodeUpdateCredentialRequest: %v", err)
	}
	current := Credential{DisplayName: "Current", Metadata: StringMap{"keep": "old", "delete": "value"}}
	updated, err := patch.Materialize(current)
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	if updated.DisplayName != "Updated" || updated.Metadata["keep"] != "new" {
		t.Fatalf("updated credential = %+v", updated)
	}
	if _, ok := updated.Metadata["delete"]; ok {
		t.Fatalf("metadata null must delete key: %v", updated.Metadata)
	}
	if patch.Auth == nil || patch.Auth.Token != "rotated-secret" {
		t.Fatalf("auth patch = %+v", patch.Auth)
	}

	nullNamePatch, err := DecodeUpdateCredentialRequest([]byte(`{"display_name":null}`))
	if err != nil {
		t.Fatalf("DecodeUpdateCredentialRequest null display_name: %v", err)
	}
	nullNameUpdated, err := nullNamePatch.Materialize(current)
	if err != nil {
		t.Fatalf("Materialize null display_name: %v", err)
	}
	if nullNameUpdated.DisplayName != "" {
		t.Fatalf("null display_name must clear display name, got %q", nullNameUpdated.DisplayName)
	}
}

func TestCredentialDisplayNameAndMetadataLimits(t *testing.T) {
	overlongName := strings.Repeat("界", 256)
	overlongKey := strings.Repeat("k", 65)
	overlongValue := strings.Repeat("v", 513)
	tooManyMetadata := make([]string, 0, 17)
	for i := range 17 {
		tooManyMetadata = append(tooManyMetadata, `"`+string(rune('a'+i))+`":"v"`)
	}
	bodies := []string{
		`{"display_name":"` + overlongName + `","auth":{"type":"static_bearer","mcp_server_url":"https://example.com/mcp","token":"token"}}`,
		`{"metadata":{"` + overlongKey + `":"v"},"auth":{"type":"static_bearer","mcp_server_url":"https://example.com/mcp","token":"token"}}`,
		`{"metadata":{"k":"` + overlongValue + `"},"auth":{"type":"static_bearer","mcp_server_url":"https://example.com/mcp","token":"token"}}`,
		`{"metadata":{` + strings.Join(tooManyMetadata, ",") + `},"auth":{"type":"static_bearer","mcp_server_url":"https://example.com/mcp","token":"token"}}`,
		`{"metadata":null,"auth":{"type":"static_bearer","mcp_server_url":"https://example.com/mcp","token":"token"}}`,
		`{"metadata":{"k":null},"auth":{"type":"static_bearer","mcp_server_url":"https://example.com/mcp","token":"token"}}`,
	}
	for _, body := range bodies {
		t.Run(body[:min(len(body), 40)], func(t *testing.T) {
			if _, err := DecodeCreateCredentialRequest([]byte(body)); err == nil {
				t.Fatal("must reject")
			}
		})
	}

	if _, err := DecodeUpdateCredentialRequest([]byte(`{"display_name":"` + overlongName + `"}`)); err == nil {
		t.Fatal("credential update overlong display_name must reject")
	}

	encoded, err := json.Marshal(Credential{Type: "vault_credential", Metadata: nil})
	if err != nil {
		t.Fatalf("Marshal Credential: %v", err)
	}
	if !strings.Contains(string(encoded), `"metadata":{}`) {
		t.Fatalf("Credential metadata must encode as object, got %s", encoded)
	}
}

func TestCredentialAndVaultUpdateRejectMetadataLimitsAfterMerge(t *testing.T) {
	overlongKey := strings.Repeat("k", 65)
	overlongValue := strings.Repeat("v", 513)

	vaultPatch, err := DecodeUpdateVaultRequest([]byte(`{"metadata":{"` + overlongKey + `":"v"}}`))
	if err != nil {
		t.Fatalf("DecodeUpdateVaultRequest: %v", err)
	}
	if _, err := vaultPatch.Materialize(Vault{Metadata: StringMap{}}); err == nil {
		t.Fatal("vault metadata patch with overlong key must reject")
	}

	credentialPatch, err := DecodeUpdateCredentialRequest([]byte(`{"metadata":{"k":"` + overlongValue + `"}}`))
	if err != nil {
		t.Fatalf("DecodeUpdateCredentialRequest: %v", err)
	}
	if _, err := credentialPatch.Materialize(Credential{Metadata: StringMap{}}); err == nil {
		t.Fatal("credential metadata patch with overlong value must reject")
	}

	fullMetadata := StringMap{}
	for i := range 16 {
		fullMetadata[string(rune('a'+i))] = "v"
	}
	tooManyPatch, err := DecodeUpdateCredentialRequest([]byte(`{"metadata":{"extra":"v"}}`))
	if err != nil {
		t.Fatalf("DecodeUpdateCredentialRequest too many: %v", err)
	}
	if _, err := tooManyPatch.Materialize(Credential{Metadata: fullMetadata}); err == nil {
		t.Fatal("metadata patch that exceeds pair limit must reject")
	}
}

func TestVaultPageTokensBindScopeAndFilters(t *testing.T) {
	token, err := encodePageToken(pageToken{
		Version:         pageTokenVersion,
		Resource:        pageTokenVaults,
		WorkspaceID:     string(workspace.DefaultID),
		IncludeArchived: false,
		LastSequence:    42,
	})
	if err != nil {
		t.Fatalf("encodePageToken: %v", err)
	}
	decoded, err := decodePageToken(token, workspace.DefaultID, pageTokenVaults, "", false)
	if err != nil {
		t.Fatalf("decodePageToken: %v", err)
	}
	if decoded.LastSequence != 42 {
		t.Fatalf("LastSequence = %d; want 42", decoded.LastSequence)
	}
	if _, err := decodePageToken(token, "workspace_b", pageTokenVaults, "", false); err == nil {
		t.Fatal("cross-workspace token must reject")
	}
	if _, err := decodePageToken(token, workspace.DefaultID, pageTokenVaultAuth, "vlt_1", false); err == nil {
		t.Fatal("wrong resource token must reject")
	}
	if _, err := decodePageToken(token, workspace.DefaultID, pageTokenVaults, "", true); err == nil {
		t.Fatal("include_archived mismatch must reject")
	}
}

func TestCredentialPageTokensBindParentVault(t *testing.T) {
	token, err := encodePageToken(pageToken{
		Version:         pageTokenVersion,
		Resource:        pageTokenVaultAuth,
		WorkspaceID:     string(workspace.DefaultID),
		ParentVaultID:   "vlt_a",
		IncludeArchived: true,
		LastSequence:    7,
	})
	if err != nil {
		t.Fatalf("encodePageToken: %v", err)
	}
	if _, err := decodePageToken(token, workspace.DefaultID, pageTokenVaultAuth, "vlt_a", true); err != nil {
		t.Fatalf("decode credential page token: %v", err)
	}
	if _, err := decodePageToken(token, workspace.DefaultID, pageTokenVaultAuth, "vlt_b", true); err == nil {
		t.Fatal("parent vault mismatch must reject")
	}
	if _, err := decodePageToken("not-a-token", workspace.DefaultID, pageTokenVaultAuth, "vlt_a", true); err == nil {
		t.Fatal("malformed token must reject")
	}
}
