package integration

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/testinfra"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// This composition sends real Keycloak JWTs through the actual Auth command.
// A typed short cache lifetime permits a bounded real-time retirement check;
// deterministic verifier tests separately own exact equality and network counts.
func TestOIDCKeycloakAuthRotation(t *testing.T) {
	oidcIsolatedTLSCaseWithMarker(t, "oidc_rotation_assertion=", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		t.Cleanup(cancel)
		fixture, err := testinfra.LoadKeycloakFixture()
		if err != nil {
			t.Fatal(err)
		}
		realm, err := fixture.ProvisionRealm(ctx)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			if err := realm.Close(cleanup); err != nil {
				t.Errorf("rotation realm cleanup: %v", err)
			}
		})
		client, err := fixture.HTTPClient()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(client.CloseIdleConnections)
		_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		pools := storagetest.OpenWorkloadDB(t, admin, "auth")
		if _, err := workspace.NewSeeder(admin).Seed(ctx, workspace.DefaultID, "OIDC real signing rotation"); err != nil {
			t.Fatal(err)
		}
		ca, err := os.ReadFile(realm.CAPath)
		if err != nil {
			t.Fatal(err)
		}
		issuer, err := url.Parse(realm.Issuer)
		if err != nil {
			t.Fatal(err)
		}
		rule := auth.FederationRule{ID: "fdrl_rotation", OrganizationID: "org_rotation", Issuer: realm.Issuer, Audience: realm.Audience, JWKSURL: realm.JWKSURL, AllowedOrigins: []string{issuer.Scheme + "://" + issuer.Host}, AllowedCIDRs: realm.AllowedCIDRs, TrustedCAPEM: string(ca), Algorithm: "RS256", Enabled: true}
		identity := auth.IdentityBinding{ID: "identity_rotation", OrganizationID: rule.OrganizationID, Issuer: rule.Issuer, Subject: realm.HumanSubject, Kind: auth.IdentityHuman, Enabled: true}
		grant := auth.WorkspaceGrant{ID: "grant_rotation", IdentityID: identity.ID, WorkspaceID: workspace.DefaultID, Role: auth.WorkspaceFullAccess, Enabled: true}
		if _, err := auth.NewPolicyStore(admin).Apply(ctx, auth.PolicyDocument{FederationRules: []auth.FederationRule{rule}, Identities: []auth.IdentityBinding{identity}, WorkspaceGrants: []auth.WorkspaceGrant{grant}}); err != nil {
			t.Fatal(err)
		}
		var ruleRevision int64
		if err := admin.QueryRowContext(ctx, `SELECT revision FROM auth_federation_rules WHERE id=$1`, rule.ID).Scan(&ruleRevision); err != nil {
			t.Fatal(err)
		}
		privateKey, err := auth.GenerateEd25519PrivateKeyBase64()
		if err != nil {
			t.Fatal(err)
		}
		const cacheLifetime = 2 * time.Second
		process := startOIDCAuthProcessFromBinary(ctx, t, buildOIDCAuthCommand(ctx, t), pools.DB, privateKey, map[string]string{"TETRAL_AUTH_JWKS_CACHE_TTL_SECONDS": "2"})
		oldAssertion, err := realm.HumanAssertion(ctx)
		if err != nil {
			t.Fatal(err)
		}
		oldKid, oldExpires := oidcRotationJWTMetadata(t, oldAssertion)
		oldBearer := oidcReplicaExchange(ctx, t, process, rule, oldAssertion)
		oidcRotationAssertBearer(ctx, t, process, oldBearer, identity.ID, "rotation-before")
		metadata, err := realm.SigningKeys(ctx)
		if err != nil {
			t.Fatal(err)
		}
		oldComponent := ""
		for _, key := range metadata {
			if key.Kid == oldKid {
				oldComponent = key.ComponentID
			}
		}
		if oldComponent == "" {
			t.Fatal("real old JWT has no controlled signing provider")
		}
		rotated, err := realm.RotateSigningKey(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if rotated.Kid == oldKid || rotated.Status != "ACTIVE" {
			t.Fatal("real rotation did not select a fresh active signer")
		}
		newAssertion, err := realm.HumanAssertion(ctx)
		if err != nil {
			t.Fatal(err)
		}
		newKid, _ := oidcRotationJWTMetadata(t, newAssertion)
		if newKid != rotated.Kid {
			t.Fatal("real fresh assertion did not use the rotated signer")
		}
		keys := oidcRotationJWKS(ctx, t, client, realm.JWKSURL)
		if !keys[oldKid] || !keys[newKid] {
			t.Fatal("real JWKS lacks old/new signing-key overlap")
		}
		oidcReplicaExchange(ctx, t, process, rule, newAssertion)
		oidcRotationAssertLiveAssertion(t, oldExpires)
		oidcReplicaExchange(ctx, t, process, rule, oldAssertion)
		t.Log("oidc_rotation_assertion=actual_auth_old_and_new_overlap status=200")
		if err := realm.SetSigningKeyState(ctx, oldComponent, false, true); err != nil {
			t.Fatal(err)
		}
		if !oidcRotationJWKS(ctx, t, client, realm.JWKSURL)[oldKid] {
			t.Fatal("passive enabled signer disappeared before retirement")
		}
		oidcRotationAssertLiveAssertion(t, oldExpires)
		oidcReplicaExchange(ctx, t, process, rule, oldAssertion)
		// The cache fill cannot occur after its complete HTTP response. Waiting past
		// this response plus the configured maximum lifetime covers all possible
		// fill instants, without changing the trusted rule or restarting Auth.
		latestSuccessfulResponse := time.Now()
		if err := realm.SetSigningKeyState(ctx, oldComponent, false, false); err != nil {
			t.Fatal(err)
		}
		keys = oidcRotationJWKS(ctx, t, client, realm.JWKSURL)
		if keys[oldKid] || !keys[newKid] {
			t.Fatal("real retirement did not preserve only the current signer")
		}
		timer := time.NewTimer(time.Until(latestSuccessfulResponse.Add(cacheLifetime + 100*time.Millisecond)))
		defer timer.Stop()
		select {
		case <-ctx.Done():
			t.Fatal("rotation cache expiry barrier cancelled")
		case <-timer.C:
		}
		oidcRotationAssertLiveAssertion(t, oldExpires)
		var rowsBefore, rowsAfter int
		if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM auth_access_tokens WHERE federation_rule_id=$1`, rule.ID).Scan(&rowsBefore); err != nil {
			t.Fatal(err)
		}
		oidcRotationRejectRetired(ctx, t, process, rule, oldAssertion)
		if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM auth_access_tokens WHERE federation_rule_id=$1`, rule.ID).Scan(&rowsAfter); err != nil {
			t.Fatal(err)
		}
		if rowsAfter != rowsBefore {
			t.Fatal("rejected retired signer persisted an Engine token")
		}
		currentBearer := oidcReplicaExchange(ctx, t, process, rule, newAssertion)
		oidcRotationAssertBearer(ctx, t, process, currentBearer, identity.ID, "rotation-current")
		// Upstream key retirement ends new exchanges after cache expiry. It does not
		// revoke otherwise-live opaque Engine tokens issued from that signer.
		oidcRotationAssertBearer(ctx, t, process, oldBearer, identity.ID, "rotation-issued-before-retirement")
		var finalRevision int64
		if err := admin.QueryRowContext(ctx, `SELECT revision FROM auth_federation_rules WHERE id=$1`, rule.ID).Scan(&finalRevision); err != nil {
			t.Fatal(err)
		}
		if finalRevision != ruleRevision {
			t.Fatal("policy revision changed during same-revision real key retirement")
		}
		oidcRotationAssertLiveAssertion(t, oldExpires)
		t.Log("oidc_rotation_assertion=actual_auth_same_revision_retirement old_exchange=401 current_exchange=200 old_engine_bearer=200 denied_token_rows=0 cache_seconds=2")
	})
}

// Only public key IDs and expiry are inspected locally. The production command
// owns cryptographic verification; fixture-specific independent RSA controls
// are covered by TestKeycloakHTTPSIdentityAndSigningKeyLifecycle.
func oidcRotationJWTMetadata(t *testing.T, assertion string) (string, time.Time) {
	t.Helper()
	parts := strings.Split(assertion, ".")
	if len(parts) != 3 {
		t.Fatal("real rotation assertion is not a JWT")
	}
	header, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		t.Fatal("real rotation JWT header is invalid")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal("real rotation JWT payload is invalid")
	}
	var head struct {
		Kid       string `json:"kid"`
		Algorithm string `json:"alg"`
	}
	var claims struct {
		Expires int64 `json:"exp"`
	}
	if json.Unmarshal(header, &head) != nil || json.Unmarshal(payload, &claims) != nil || head.Kid == "" || head.Algorithm != "RS256" || claims.Expires == 0 {
		t.Fatal("real rotation JWT lacks configured signer/expiry")
	}
	return head.Kid, time.Unix(claims.Expires, 0)
}
func oidcRotationAssertLiveAssertion(t *testing.T, expires time.Time) {
	t.Helper()
	if time.Until(expires) <= 120*time.Second {
		t.Fatal("expiring assertion cannot distinguish retired signing-key denial from issuance floor")
	}
}
func oidcRotationJWKS(ctx context.Context, t *testing.T, client *http.Client, endpoint string) map[string]bool {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal("real rotation JWKS request failed")
	}
	defer func() { _ = response.Body.Close() }()
	var body struct {
		Keys []struct {
			Kid string `json:"kid"`
		} `json:"keys"`
	}
	if response.StatusCode != http.StatusOK || json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&body) != nil {
		t.Fatal("real rotation JWKS unavailable")
	}
	keys := make(map[string]bool)
	for _, key := range body.Keys {
		keys[key.Kid] = true
	}
	return keys
}
func oidcRotationRejectRetired(ctx context.Context, t *testing.T, process *oidcAuthProcess, rule auth.FederationRule, assertion string) {
	t.Helper()
	body, err := json.Marshal(map[string]string{"grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer", "assertion": assertion, "federation_rule_id": rule.ID, "organization_id": rule.OrganizationID, "workspace_id": "default"})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, process.URL+"/v1/oauth/token", strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
	if err != nil {
		t.Fatal("retired-signer exchange transport failed")
	}
	defer func() { _ = response.Body.Close() }()
	var result struct {
		AccessToken string `json:"access_token"`
		Error       *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.NewDecoder(io.LimitReader(response.Body, 64*1024)).Decode(&result) != nil || response.StatusCode != http.StatusUnauthorized || result.AccessToken != "" || result.Error == nil || result.Error.Message != "invalid assertion" || response.Header.Get("Cache-Control") != "no-store" || response.Header.Get("Pragma") != "no-cache" {
		t.Fatalf("retired signing-key exchange classification status=%d", response.StatusCode)
	}
}
func oidcRotationAssertBearer(ctx context.Context, t *testing.T, process *oidcAuthProcess, token oidcFrozenBearer, identityID, requestID string) {
	t.Helper()
	if !time.Now().Before(token.conservativeExpiry) {
		t.Fatal("rotation Engine bearer expired before control")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, process.URL+"/internal/auth/authorize", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token.raw)
	request.Header.Set("X-Original-Method", http.MethodGet)
	request.Header.Set("X-Original-Path", "/v1/sessions")
	request.Header.Set("X-Request-Id", requestID)
	request.Header.Set("X-Forwarded-For", "127.0.0.1")
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
	if err != nil {
		t.Fatal("rotation Engine bearer transport failed")
	}
	defer func() { _ = response.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64*1024))
	principal, claims, err := process.verifier.Verify(response.Header.Get("X-Tetral-Internal-Principal"), http.MethodGet, "/v1/sessions")
	if response.StatusCode != http.StatusOK || err != nil || claims.RequestID != requestID || principal.Identity == nil || principal.Identity.ID != identityID || principal.Identity.Kind != auth.IdentityHuman || principal.Workspace.ID != workspace.DefaultID || principal.Credential.Kind != auth.CredentialAccessToken || principal.APIKeyID != "" || principal.Authority.Kind != auth.AuthorityIdentityGrant {
		t.Fatal("rotation bearer lacks actual bound Auth identity admission")
	}
}
