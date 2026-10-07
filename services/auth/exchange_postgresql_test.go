package tetralauth

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/auth/authtest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestPostgreSQLOIDCExchange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	var unavailable atomic.Bool
	issuer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/keys" {
			http.NotFound(w, r)
			return
		}
		if unavailable.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]string{"kty": "RSA", "kid": "exchange-key", "alg": "RS256", "use": "sig", "n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()), "e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes())}}})
	}))
	defer issuer.Close()
	admin := storagetest.NewPostgreSQLAdminDB(t)
	if _, err := workspace.NewSeeder(admin).Seed(ctx, "workspace_exchange_b", "second"); err != nil {
		t.Fatal(err)
	}
	rule := auth.FederationRule{ID: "rule_exchange", OrganizationID: "organization_exchange", Issuer: issuer.URL, Audience: "tetral-engine", JWKSURL: issuer.URL + "/keys", Algorithm: "RS256", AllowedCIDRs: []string{"127.0.0.1/32"}, TrustedCAPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: issuer.Certificate().Raw})), Enabled: true}
	document := auth.PolicyDocument{
		FederationRules: []auth.FederationRule{rule},
		Identities: []auth.IdentityBinding{
			{ID: "identity_exchange_human", OrganizationID: rule.OrganizationID, Issuer: rule.Issuer, Subject: "human", Kind: auth.IdentityHuman, Enabled: true},
			{ID: "identity_exchange_service", OrganizationID: rule.OrganizationID, Issuer: rule.Issuer, Subject: "service", Kind: auth.IdentityService, ServiceAccountID: "service_account_exchange", Enabled: true},
			{ID: "identity_exchange_ungranted", OrganizationID: rule.OrganizationID, Issuer: rule.Issuer, Subject: "ungranted", Kind: auth.IdentityHuman, Enabled: true},
		},
		WorkspaceGrants: []auth.WorkspaceGrant{
			{ID: "grant_exchange_human_a", IdentityID: "identity_exchange_human", WorkspaceID: workspace.DefaultID, Role: auth.WorkspaceFullAccess, Enabled: true},
			{ID: "grant_exchange_human_b", IdentityID: "identity_exchange_human", WorkspaceID: "workspace_exchange_b", Role: auth.WorkspaceFullAccess, Enabled: true},
			{ID: "grant_exchange_service", IdentityID: "identity_exchange_service", WorkspaceID: workspace.DefaultID, Role: auth.WorkspaceFullAccess, Enabled: true},
		},
	}
	if _, err := auth.NewPolicyStore(admin).Apply(ctx, document); err != nil {
		t.Fatal(err)
	}
	installed := storagetest.OpenWorkloadDB(t, admin, "auth")
	runtime := installed.DB
	resolver := auth.NewAuthorityResolver(runtime, "workspace_exchange_b")
	store := auth.NewAPIKeyStore(runtime)
	verifier := auth.NewAssertionVerifier(ctx)
	defer verifier.Close()
	signer, err := auth.NewInternalPrincipalSignerFromBase64(mustGenerateTestPrivateKey(t))
	if err != nil {
		t.Fatal(err)
	}
	var diagnosticOutput bytes.Buffer
	processLogger := workload.NewProcessLogger(&diagnosticOutput, "auth", "test", "unit", workload.DefaultDiagnosticConfig())
	defer processLogger.CloseWithBudget()
	router := NewRouter(RouterConfig{Logger: processLogger.Logger, Store: store, Resolver: resolver, AssertionVerifier: verifier, Signer: signer})
	server := httptest.NewServer(router)
	defer server.Close()
	counts := func(t *testing.T) (tokens, identities, grants int) {
		t.Helper()
		if err := admin.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM auth_access_tokens),(SELECT count(*) FROM auth_identities),(SELECT count(*) FROM auth_workspace_grants)`).Scan(&tokens, &identities, &grants); err != nil {
			t.Fatal(err)
		}
		return
	}
	assertion := func(t *testing.T, subject, audience, kid string) string {
		t.Helper()
		header, _ := json.Marshal(map[string]string{"alg": "RS256", "kid": kid})
		claims, _ := json.Marshal(map[string]any{"iss": issuer.URL, "sub": subject, "aud": audience, "exp": time.Now().Add(20 * time.Minute).Unix()})
		content := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(claims)
		digest := sha256.Sum256([]byte(content))
		signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
		if err != nil {
			t.Fatal(err)
		}
		return content + "." + base64.RawURLEncoding.EncodeToString(signature)
	}
	exchange := func(t *testing.T, subject, selected, serviceID, ruleID, organization, audience, kid string, want int) auth.AccessTokenResponse {
		t.Helper()
		requestBody, _ := json.Marshal(exchangeRequest{GrantType: jwtBearerGrant, Assertion: assertion(t, subject, audience, kid), FederationRuleID: ruleID, OrganizationID: organization, WorkspaceID: selected, ServiceAccountID: serviceID})
		before, identitiesBefore, grantsBefore := counts(t)
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/oauth/token", strings.NewReader(string(requestBody)))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != want {
			t.Fatalf("exchange subject=%s selector=%s status=%d want=%d", subject, selected, response.StatusCode, want)
		}
		after, identitiesAfter, grantsAfter := counts(t)
		expectedDelta := 0
		if want == http.StatusOK {
			expectedDelta = 1
		}
		if after-before != expectedDelta {
			t.Fatalf("exchange durable token delta=%d want=%d", after-before, expectedDelta)
		}
		if identitiesAfter != identitiesBefore || grantsAfter != grantsBefore {
			t.Fatal("token exchange provisioned or mutated identity/grant state")
		}
		if response.Header.Get("Cache-Control") != "no-store" || response.Header.Get("Pragma") != "no-cache" {
			t.Fatal("exchange response permits caching")
		}
		var token auth.AccessTokenResponse
		if want == http.StatusOK {
			if err := json.Unmarshal(body, &token); err != nil {
				t.Fatal(err)
			}
			if token.TokenType != "Bearer" || token.ExpiresIn <= 120 || token.ExpiresIn > 600 || !strings.HasPrefix(token.AccessToken, "tetral_at_") {
				t.Fatal("exchange response violated opaque token lifetime contract")
			}
		} else if strings.Contains(string(body), "BEGIN CERTIFICATE") || strings.Contains(string(body), "eyJ") {
			t.Fatal("exchange failure disclosed issuer or credential material")
		}
		return token
	}
	t.Run("EngineWorkspaceSelection", func(t *testing.T) {
		exchange(t, "human", "", "", rule.ID, rule.OrganizationID, rule.Audience, "exchange-key", 401)
		for _, tc := range []struct {
			selector  string
			workspace workspace.ID
		}{{"default", "workspace_exchange_b"}, {"workspace_exchange_b", "workspace_exchange_b"}, {"default-not-an-alias", ""}} {
			want := 200
			if tc.workspace == "" {
				want = 401
			}
			token := exchange(t, "human", tc.selector, "", rule.ID, rule.OrganizationID, rule.Audience, "exchange-key", want)
			if want == 200 {
				principal, err := resolver.AuthenticateBearer(ctx, token.AccessToken)
				if err != nil {
					t.Fatal(err)
				}
				if principal.Workspace.ID != tc.workspace || principal.Identity.ID != "identity_exchange_human" || principal.APIKeyID != "" {
					t.Fatal("Engine selector resolved upstream or ambiguous workspace authority")
				}
			}
		}
		exchange(t, "human", "workspace_absent", "", rule.ID, rule.OrganizationID, rule.Audience, "exchange-key", 401)
	})
	var serviceToken auth.AccessTokenResponse
	t.Run("TypedHumanAndServiceBindings", func(t *testing.T) {
		exchange(t, "human", "workspace_exchange_b", "service_account_exchange", rule.ID, rule.OrganizationID, rule.Audience, "exchange-key", 401)
		exchange(t, "ungranted", "", "", rule.ID, rule.OrganizationID, rule.Audience, "exchange-key", 401)
		exchange(t, "unknown-subject", "", "", rule.ID, rule.OrganizationID, rule.Audience, "exchange-key", 401)
		exchange(t, "service", "", "wrong-service-account", rule.ID, rule.OrganizationID, rule.Audience, "exchange-key", 401)
		serviceToken = exchange(t, "service", "", "service_account_exchange", rule.ID, rule.OrganizationID, rule.Audience, "exchange-key", 200)
		principal, err := resolver.AuthenticateBearer(ctx, serviceToken.AccessToken)
		if err != nil {
			t.Fatal(err)
		}
		if principal.Workspace.ID != workspace.DefaultID || principal.Identity.Kind != auth.IdentityService || principal.Identity.ID != "identity_exchange_service" || principal.APIKeyID != "" {
			t.Fatal("service exchange lost truthful stable actor")
		}
	})
	t.Run("SelectedCredentialPrecedence", func(t *testing.T) {
		key, err := authtest.SeedIndependentKey(ctx, runtime, workspace.DefaultID, "selected independent key")
		if err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			name, key     string
			authorization []string
			want          int
		}{
			{"selected key ignores duplicated bearer", key.APIKey, []string{"Bearer invalid", "Bearer " + serviceToken.AccessToken}, 200},
			{"selected invalid key never falls back", "invalid-key", []string{"Bearer " + serviceToken.AccessToken}, 401},
			{"selected whitespace stays exact", " " + key.APIKey, []string{"Bearer " + serviceToken.AccessToken}, 401},
			{"empty selected key admits bearer", "", []string{"bEaReR " + serviceToken.AccessToken}, 200},
			{"repeated bearer denies", "", []string{"Bearer " + serviceToken.AccessToken, "Bearer " + serviceToken.AccessToken}, 401},
			{"combined bearer denies", "", []string{"Bearer " + serviceToken.AccessToken + ", Bearer invalid"}, 401},
		} {
			t.Run(tc.name, func(t *testing.T) {
				request := httptest.NewRequest(http.MethodPost, "/internal/auth/authorize", nil)
				request.Header.Set("X-Api-Key", tc.key)
				request.Header["Authorization"] = tc.authorization
				request.Header.Set("X-Original-Method", http.MethodGet)
				request.Header.Set("X-Original-Path", "/v1/sessions")
				request.Header.Set("X-Request-Id", "request_exchange_test")
				request.Header.Set("X-Forwarded-For", "127.0.0.1")
				response := httptest.NewRecorder()
				router.ServeHTTP(response, request)
				if response.Code != tc.want {
					t.Fatalf("authorize status=%d want=%d", response.Code, tc.want)
				}
				if tc.want == 200 {
					principal, _, err := signer.Verify(response.Header().Get("X-Tetral-Internal-Principal"), http.MethodGet, "/v1/sessions")
					if err != nil {
						t.Fatal(err)
					}
					if tc.key != "" && (principal.APIKeyID != key.ID || principal.Identity != nil) {
						t.Fatal("selected independent key borrowed bearer identity")
					}
					if tc.key == "" && (principal.Identity == nil || principal.Identity.Kind != auth.IdentityService || principal.APIKeyID != "") {
						t.Fatal("bearer actor was stamped as API key")
					}
				}
			})
		}
	})
	t.Run("RepeatedMixedCaseSelectedKeysOnWire", func(t *testing.T) {
		key, err := authtest.SeedIndependentKey(ctx, runtime, workspace.DefaultID, "wire selected key")
		if err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			keys     []string
			want     int
			apiActor bool
		}{
			{[]string{key.APIKey, "invalid-second"}, 200, true},
			{[]string{"invalid-first", key.APIKey}, 401, false},
			{[]string{"", key.APIKey}, 200, false},
		} {
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/internal/auth/authorize", nil)
			if err != nil {
				t.Fatal(err)
			}
			request.Header["x-aPi-kEy"] = tc.keys
			request.Header.Set("Authorization", "Bearer "+serviceToken.AccessToken)
			request.Header.Set("X-Original-Method", http.MethodGet)
			request.Header.Set("X-Original-Path", "/v1/sessions")
			request.Header.Set("X-Request-Id", "request_exchange_wire")
			request.Header.Set("X-Forwarded-For", "127.0.0.1")
			response, err := server.Client().Do(request)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode != tc.want {
				t.Fatalf("wire selected-key status=%d want=%d", response.StatusCode, tc.want)
			}
			if tc.want == 200 {
				principal, _, err := signer.Verify(response.Header.Get("X-Tetral-Internal-Principal"), http.MethodGet, "/v1/sessions")
				if err != nil {
					t.Fatal(err)
				}
				if tc.apiActor && (principal.APIKeyID != key.ID || principal.Identity != nil) {
					t.Fatal("first selected wire key lost precedence")
				}
				if !tc.apiActor && (principal.APIKeyID != "" || principal.Identity == nil || principal.Identity.Kind != auth.IdentityService) {
					t.Fatal("empty first wire key selected a later key instead of Bearer")
				}
			}
		}
	})
	t.Run("DatabaseUnavailableIsDependencyFailure", func(t *testing.T) {
		closed := installed.OpenWorkload(t, "auth", nil)
		if err := closed.Close(); err != nil {
			t.Fatal(err)
		}
		unavailableRouter := NewRouter(RouterConfig{Logger: processLogger.Logger, Store: auth.NewAPIKeyStore(closed), Resolver: auth.NewAuthorityResolver(closed, "workspace_exchange_b"), AssertionVerifier: verifier, Signer: signer})
		endpoint := httptest.NewServer(unavailableRouter)
		defer endpoint.Close()
		before, _, _ := counts(t)
		for _, tc := range []struct{ path, body, key, bearer string }{
			{"/internal/auth/authorize", "", "selected-key", ""},
			{"/internal/auth/authorize", "", "", "Bearer " + serviceToken.AccessToken},
			{"/v1/oauth/token", "exchange", "", ""},
		} {
			body := tc.body
			if body == "exchange" {
				encoded, _ := json.Marshal(exchangeRequest{GrantType: jwtBearerGrant, Assertion: assertion(t, "service", rule.Audience, "exchange-key"), FederationRuleID: rule.ID, OrganizationID: rule.OrganizationID})
				body = string(encoded)
			}
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.URL+tc.path, strings.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("X-Api-Key", tc.key)
			request.Header.Set("Authorization", tc.bearer)
			request.Header.Set("X-Original-Method", http.MethodGet)
			request.Header.Set("X-Original-Path", "/v1/sessions")
			request.Header.Set("X-Request-Id", "request_dependency_exchange")
			request.Header.Set("X-Forwarded-For", "127.0.0.1")
			response, err := endpoint.Client().Do(request)
			if err != nil {
				t.Fatal(err)
			}
			safe, err := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("DB failure at %s classified status=%d", tc.path, response.StatusCode)
			}
			if !strings.Contains(string(safe), "authentication unavailable") || strings.Contains(string(safe), "database is closed") || strings.Contains(string(safe), "tetral_at_") {
				t.Fatal("DB error envelope leaked dependency or credential details")
			}
		}
		after, _, _ := counts(t)
		if after != before {
			t.Fatal("unavailable DB path inserted token")
		}
	})
	t.Run("InvalidVersusUnavailable", func(t *testing.T) {
		exchange(t, "service", "", "", rule.ID, "wrong-organization", rule.Audience, "exchange-key", 401)
		exchange(t, "service", "", "", rule.ID, rule.OrganizationID, "wrong-audience", "exchange-key", 401)
		unavailableRule := rule
		unavailableRule.ID = "rule_exchange_unavailable"
		if _, err := auth.NewPolicyStore(admin).Apply(ctx, auth.PolicyDocument{FederationRules: []auth.FederationRule{unavailableRule}}); err != nil {
			t.Fatal(err)
		}
		unavailable.Store(true)
		exchange(t, "service", "", "", unavailableRule.ID, rule.OrganizationID, rule.Audience, "new-unknown-key", 503)
	})
	processLogger.CloseWithBudget()
	events := map[string]map[string]any{}
	kinds := map[string]bool{}
	for _, line := range bytes.Split(bytes.TrimSpace(diagnosticOutput.Bytes()), []byte("\n")) {
		var event map[string]any
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatal(err)
		}
		if code, ok := event["error.code"].(string); ok {
			stage, _ := event["auth.stage"].(string)
			events[code+":"+stage] = event
		}
		if event["event"] == "auth.exchange.success" {
			kind := event["auth.identity.kind"]
			if value, ok := kind.(string); ok {
				kinds[value] = true
			}
			if kind != auth.IdentityHuman && kind != auth.IdentityService {
				t.Fatal("exchange lost verified identity kind")
			}
			for _, field := range []string{"auth.rule.revision", "auth.identity.revision", "auth.grant.revision"} {
				if value, ok := event[field].(float64); !ok || value <= 0 {
					t.Fatalf("exchange missing trusted %s", field)
				}
			}
		}
	}
	for code, want := range map[string]struct{ stage, result string }{
		"auth_rule_invalid": {"rule", "invalid"}, "auth_assertion_invalid": {"assertion", "invalid"},
		"auth_grant_denied": {"grant", "denied"}, "auth_admission_invalid": {"admission", "invalid"},
		"auth_store_unavailable": {"rule", "unavailable"}, "auth_jwks_unavailable": {"assertion", "unavailable"},
	} {
		event, ok := events[code+":"+want.stage]
		// Store failure occurs through admission and exchange; process suppression
		// partitions by fixed stage as well as safe code, not by identity/revision.
		if !ok || event["auth.stage"] != want.stage || event["auth.result"] != want.result {
			t.Fatalf("missing semantic diagnostic %s", code)
		}
	}
	if !kinds[auth.IdentityHuman] || !kinds[auth.IdentityService] {
		t.Fatal("successful exchanges lost truthful kind diagnostics")
	}
	if event := events["auth_store_unavailable:admission"]; event == nil || event["auth.result"] != "unavailable" {
		t.Fatal("store admission diagnostic missing")
	}
	for _, forbidden := range []string{issuer.URL, rule.ID, rule.OrganizationID, "identity_exchange_", "grant_exchange_", "tetral_at_", "selected-key", "database is closed"} {
		if strings.Contains(diagnosticOutput.String(), forbidden) {
			t.Fatal("authentication diagnostic leaked private request/store material")
		}
	}

}
