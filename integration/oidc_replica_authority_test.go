package integration

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
	"log"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// Unlike the mandatory actual Keycloak SDK root, this controlled real HTTPS
// issuer is an exact request-count oracle. Counters advance before response
// serialization, so successful or rejected Auth responses cannot hide fetches.
type oidcReplicaIssuer struct {
	server      *httptest.Server
	key         *rsa.PrivateKey
	rule        auth.FederationRule
	mu          sync.Mutex
	total, jwks int
}
type oidcIssuerCounts struct{ total, jwks int }

func newOIDCReplicaIssuer(t *testing.T) *oidcReplicaIssuer {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	issuer := &oidcReplicaIssuer{key: key}
	issuer.server = httptest.NewUnstartedServer(http.HandlerFunc(issuer.serve))
	issuer.server.Config.ErrorLog = log.New(io.Discard, "", 0)
	issuer.server.StartTLS()
	t.Cleanup(issuer.server.Close)
	issuer.rule = auth.FederationRule{ID: "fdrl_replica", OrganizationID: "org_replica", Issuer: issuer.server.URL + "/realm", Audience: "tetral-engine", JWKSURL: issuer.server.URL + "/jwks", AllowedOrigins: []string{issuer.server.URL}, AllowedCIDRs: []string{"127.0.0.1/32"}, TrustedCAPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: issuer.server.Certificate().Raw})), Algorithm: "RS256", Enabled: true}
	return issuer
}
func (issuer *oidcReplicaIssuer) serve(w http.ResponseWriter, r *http.Request) {
	issuer.mu.Lock()
	issuer.total++
	if r.URL.Path == "/jwks" {
		issuer.jwks++
	}
	issuer.mu.Unlock()
	if r.Method != http.MethodGet || r.URL.Path != "/jwks" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]string{"kty": "RSA", "kid": "replica-key", "alg": "RS256", "use": "sig", "n": base64.RawURLEncoding.EncodeToString(issuer.key.N.Bytes()), "e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(issuer.key.E)).Bytes())}}})
}
func (issuer *oidcReplicaIssuer) counts() oidcIssuerCounts {
	issuer.mu.Lock()
	defer issuer.mu.Unlock()
	return oidcIssuerCounts{issuer.total, issuer.jwks}
}
func (issuer *oidcReplicaIssuer) assertion(t *testing.T, subject string) string {
	t.Helper()
	header, err := json.Marshal(map[string]string{"alg": "RS256", "kid": "replica-key", "typ": "JWT"})
	if err != nil {
		t.Fatal(err)
	}
	claims, err := json.Marshal(map[string]any{"iss": issuer.rule.Issuer, "sub": subject, "aud": issuer.rule.Audience, "exp": time.Now().Add(15 * time.Minute).Unix()})
	if err != nil {
		t.Fatal(err)
	}
	input := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(claims)
	digest := sha256.Sum256([]byte(input))
	signature, err := rsa.SignPKCS1v15(rand.Reader, issuer.key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return input + "." + base64.RawURLEncoding.EncodeToString(signature)
}

type oidcFrozenBearer struct {
	raw                string
	conservativeExpiry time.Time
}

func oidcReplicaExchange(ctx context.Context, t *testing.T, process *oidcAuthProcess, rule auth.FederationRule, assertion string) oidcFrozenBearer {
	t.Helper()
	body, err := json.Marshal(map[string]string{"grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer", "assertion": assertion, "federation_rule_id": rule.ID, "organization_id": rule.OrganizationID, "workspace_id": "default"})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, process.URL+"/v1/oauth/token", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	requestedAt := time.Now()
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
	if err != nil {
		t.Fatal("actual replica exchange transport failed")
	}
	defer func() { _ = response.Body.Close() }()
	var token struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		Expires     int    `json:"expires_in"`
	}
	if response.StatusCode != http.StatusOK || json.NewDecoder(io.LimitReader(response.Body, 64*1024)).Decode(&token) != nil || token.AccessToken == "" || token.TokenType != "Bearer" || token.Expires <= 120 || response.Header.Get("Cache-Control") != "no-store" || response.Header.Get("Pragma") != "no-cache" {
		t.Fatalf("actual replica exchange rejected its provisioned assertion status=%d", response.StatusCode)
	}
	return oidcFrozenBearer{raw: token.AccessToken, conservativeExpiry: requestedAt.Add(time.Duration(token.Expires) * time.Second)}
}
func oidcReplicaAuthorize(ctx context.Context, t *testing.T, process *oidcAuthProcess, token oidcFrozenBearer, requestID string) int {
	t.Helper()
	response, err := directEdgeCheck(ctx, process.GRPCAddress, http.MethodGet, "/v1/sessions", requestID, http.Header{"Authorization": []string{"Bearer " + token.raw}})
	if err != nil {
		t.Fatal("actual replica bearer Check transport failed")
	}
	status, signed := directEdgeCheckStatus(response)
	if status == http.StatusOK {
		principal, claims, err := process.verifier.Verify(signed, http.MethodGet, "/v1/sessions")
		if err != nil || claims.RequestID != requestID || principal.Workspace.ID != workspace.DefaultID || principal.Identity == nil || principal.Identity.ID != "identity_replica" || principal.Identity.Kind != auth.IdentityHuman || principal.Credential.Kind != auth.CredentialAccessToken || principal.APIKeyID != "" || principal.Authority.Kind != auth.AuthorityIdentityGrant {
			t.Fatal("actual Auth response lacks its valid bound typed identity principal")
		}
	}
	return status
}
func oidcAssertFrozenReplicas(ctx context.Context, t *testing.T, issuer *oidcReplicaIssuer, processes []*oidcAuthProcess, token oidcFrozenBearer, status int, label string) {
	t.Helper()
	if !time.Now().Before(token.conservativeExpiry) {
		t.Fatal("naturally expired bearer cannot establish policy invalidation")
	}
	before := issuer.counts()
	for index, process := range processes {
		if got := oidcReplicaAuthorize(ctx, t, process, token, label+string(rune('a'+index))); got != status {
			t.Fatalf("frozen bearer replica%d status=%d;want%d", index, got, status)
		}
	}
	if !time.Now().Before(token.conservativeExpiry) {
		t.Fatal("bearer expired during the policy invalidation observation")
	}
	if after := issuer.counts(); after != before {
		t.Fatal("frozen bearer admission contacted issuer/JWKS")
	}
	t.Logf("oidc_replica_assertion=%s replicas=2 status=%d issuer_delta=0 jwks_delta=0", label, status)
}

func TestOIDCReplicaAuthority(t *testing.T) {
	isolatedTLSPostgreSQLRoot(t, "oidc_replica_assertion=", isolatedTLSRootBudget, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		t.Cleanup(cancel)
		_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		pools := storagetest.OpenWorkloadDB(t, admin, "auth")
		if _, err := workspace.NewSeeder(admin).Seed(ctx, workspace.DefaultID, "OIDC replicas"); err != nil {
			t.Fatal(err)
		}
		issuer := newOIDCReplicaIssuer(t)
		identity := auth.IdentityBinding{ID: "identity_replica", OrganizationID: issuer.rule.OrganizationID, Issuer: issuer.rule.Issuer, Subject: "immutable-replica-subject", Kind: auth.IdentityHuman, Enabled: true}
		grant := auth.WorkspaceGrant{ID: "grant_replica", IdentityID: identity.ID, WorkspaceID: workspace.DefaultID, Role: auth.WorkspaceFullAccess, Enabled: true}
		document := auth.PolicyDocument{FederationRules: []auth.FederationRule{issuer.rule}, Identities: []auth.IdentityBinding{identity}, WorkspaceGrants: []auth.WorkspaceGrant{grant}}
		policy := auth.NewPolicyStore(admin)
		if _, err := policy.Apply(ctx, document); err != nil {
			t.Fatal(err)
		}
		privateKey, err := auth.GenerateEd25519PrivateKeyBase64()
		if err != nil {
			t.Fatal(err)
		}
		binary := buildOIDCAuthCommand(ctx, t)
		processes := []*oidcAuthProcess{startOIDCAuthProcessFromBinary(ctx, t, binary, pools.DB, privateKey, nil), startOIDCAuthProcessFromBinary(ctx, t, binary, pools.DB, privateKey, nil)}
		if processes[0].command.Process.Pid == processes[1].command.Process.Pid || processes[0].URL == processes[1].URL || processes[0].MetricsURL == processes[1].MetricsURL || processes[0].GRPCAddress == processes[1].GRPCAddress {
			t.Fatal("replica fixture lacks two independently observed actual Auth processes")
		}
		for _, process := range processes {
			oidcAssertActualMaintenanceExport(ctx, t, process)
		}
		t.Log("oidc_replica_assertion=maintenance_export replicas=2 families=9 samples=14 public_metrics=404; export observation only")
		assertion := issuer.assertion(t, identity.Subject)
		frozen := oidcReplicaExchange(ctx, t, processes[0], issuer.rule, assertion)
		initial := issuer.counts()
		if initial.total != 1 || initial.jwks != 1 {
			t.Fatal("initial registered direct-JWKS exchange lacks its exact HTTPS fetch")
		}
		oidcAssertFrozenReplicas(ctx, t, issuer, processes, frozen, http.StatusOK, "initial")
		changes, err := policy.Apply(ctx, document)
		if err != nil || len(changes) != 0 {
			t.Fatal("identical policy import must be a no-op")
		}
		oidcAssertFrozenReplicas(ctx, t, issuer, processes, frozen, http.StatusOK, "no_op_import")

		// Every Apply return is the committed barrier. Each phase starts from an
		// admitted frozen token so a previous invalidation cannot mask another bug.
		identity.Enabled = false
		if _, err := policy.Apply(ctx, auth.PolicyDocument{Identities: []auth.IdentityBinding{identity}}); err != nil {
			t.Fatal(err)
		}
		oidcAssertFrozenReplicas(ctx, t, issuer, processes, frozen, http.StatusUnauthorized, "identity_disabled")
		identity.Enabled = true
		if _, err := policy.Apply(ctx, auth.PolicyDocument{Identities: []auth.IdentityBinding{identity}}); err != nil {
			t.Fatal(err)
		}
		oidcAssertFrozenReplicas(ctx, t, issuer, processes, frozen, http.StatusUnauthorized, "identity_reenabled_old_revision")
		identityCurrent := oidcReplicaExchange(ctx, t, processes[0], issuer.rule, issuer.assertion(t, identity.Subject))
		oidcAssertFrozenReplicas(ctx, t, issuer, processes, identityCurrent, http.StatusOK, "current_identity_revision")
		grant.Enabled = false
		if _, err := policy.Apply(ctx, auth.PolicyDocument{WorkspaceGrants: []auth.WorkspaceGrant{grant}}); err != nil {
			t.Fatal(err)
		}
		oidcAssertFrozenReplicas(ctx, t, issuer, processes, identityCurrent, http.StatusUnauthorized, "grant_disabled")
		grant.Enabled = true
		if _, err := policy.Apply(ctx, auth.PolicyDocument{WorkspaceGrants: []auth.WorkspaceGrant{grant}}); err != nil {
			t.Fatal(err)
		}
		oidcAssertFrozenReplicas(ctx, t, issuer, processes, identityCurrent, http.StatusUnauthorized, "grant_reenabled_old_revision")
		current := oidcReplicaExchange(ctx, t, processes[1], issuer.rule, issuer.assertion(t, identity.Subject))
		oidcAssertFrozenReplicas(ctx, t, issuer, processes, current, http.StatusOK, "current_grant_revision")
		if _, err := policy.Apply(ctx, auth.PolicyDocument{RevokeWorkspaceGrants: []string{grant.ID}}); err != nil {
			t.Fatal(err)
		}
		grant.ID = "grant_replica_regrant"
		if _, err := policy.Apply(ctx, auth.PolicyDocument{WorkspaceGrants: []auth.WorkspaceGrant{grant}}); err != nil {
			t.Fatal(err)
		}
		oidcAssertFrozenReplicas(ctx, t, issuer, processes, identityCurrent, http.StatusUnauthorized, "terminal_grant_old_token")
		oidcAssertFrozenReplicas(ctx, t, issuer, processes, current, http.StatusUnauthorized, "terminal_grant_current_token")
		regrant := oidcReplicaExchange(ctx, t, processes[0], issuer.rule, issuer.assertion(t, identity.Subject))
		oidcAssertFrozenReplicas(ctx, t, issuer, processes, regrant, http.StatusOK, "new_grant_identity")
		peer := oidcReplicaExchange(ctx, t, processes[1], issuer.rule, issuer.assertion(t, identity.Subject))
		if peer.raw == regrant.raw {
			t.Fatal("distinct successful exchanges reused opaque bearer bytes")
		}
		oidcAssertFrozenReplicas(ctx, t, issuer, processes, peer, http.StatusOK, "already_issued_peer_before_revoke")
		digest := sha256.Sum256([]byte(regrant.raw))
		var tokenID string
		if err := admin.QueryRowContext(ctx, `SELECT id FROM auth_access_tokens WHERE token_digest=$1`, digest[:]).Scan(&tokenID); err != nil {
			t.Fatal("actual issued bearer has no digest row")
		}
		if _, err := policy.Apply(ctx, auth.PolicyDocument{RevokeAccessTokens: []auth.TokenReference{{ID: tokenID, WorkspaceID: workspace.DefaultID}}}); err != nil {
			t.Fatal(err)
		}
		oidcAssertFrozenReplicas(ctx, t, issuer, processes, regrant, http.StatusUnauthorized, "token_local_revoke")
		oidcAssertFrozenReplicas(ctx, t, issuer, processes, peer, http.StatusOK, "already_issued_peer_survives_local_revoke")
		issuer.rule.Audience = "tetral-engine-revised"
		if _, err := policy.Apply(ctx, auth.PolicyDocument{FederationRules: []auth.FederationRule{issuer.rule}}); err != nil {
			t.Fatal(err)
		}
		oidcAssertFrozenReplicas(ctx, t, issuer, processes, peer, http.StatusUnauthorized, "rule_security_revision")
		beforeRevisedExchange := issuer.counts()
		revised := oidcReplicaExchange(ctx, t, processes[0], issuer.rule, issuer.assertion(t, identity.Subject))
		if after := issuer.counts(); after.total != beforeRevisedExchange.total+1 || after.jwks != beforeRevisedExchange.jwks+1 {
			t.Fatal("new rule security revision reused old issuer cache partition")
		}
		oidcAssertFrozenReplicas(ctx, t, issuer, processes, revised, http.StatusOK, "revised_assertion")
		issuer.rule.Enabled = false
		if _, err := policy.Apply(ctx, auth.PolicyDocument{FederationRules: []auth.FederationRule{issuer.rule}}); err != nil {
			t.Fatal(err)
		}
		oidcAssertFrozenReplicas(ctx, t, issuer, processes, revised, http.StatusUnauthorized, "rule_disabled")
		issuer.rule.Enabled = true
		if _, err := policy.Apply(ctx, auth.PolicyDocument{FederationRules: []auth.FederationRule{issuer.rule}}); err != nil {
			t.Fatal(err)
		}
		oidcAssertFrozenReplicas(ctx, t, issuer, processes, revised, http.StatusUnauthorized, "rule_reenabled_old_revision")
		latest := oidcReplicaExchange(ctx, t, processes[1], issuer.rule, issuer.assertion(t, identity.Subject))
		oidcAssertFrozenReplicas(ctx, t, issuer, processes, latest, http.StatusOK, "current_rule_revision")
	})
}

// Export wiring is observable immediately; zero initial counters do not prove a
// maintenance pass or a usage write. The pruner and usage owners supply loop
// evidence; this checks that the actual process exports their fixed series.
func oidcAssertActualMaintenanceExport(ctx context.Context, t *testing.T, process *oidcAuthProcess) {
	t.Helper()
	families := map[string][]string{
		"tetral_auth_token_prune_passes_total":                   {`status="success"`, `status="failed"`, `status="cancelled"`},
		"tetral_auth_token_prune_deleted_total":                  nil,
		"tetral_auth_token_prune_expired_backlog":                nil,
		"tetral_auth_token_prune_oldest_expiry_age_seconds":      nil,
		"tetral_auth_token_prune_last_success_timestamp_seconds": nil,
		"tetral_auth_token_prune_healthy":                        nil,
		"tetral_auth_api_key_usage_submissions_dropped_total":    {`reason="capacity"`, `reason="contended"`, `reason="closed"`},
		"tetral_auth_api_key_usage_samples_dropped_total":        {`reason="deadline"`, `reason="database"`},
		"tetral_auth_api_key_usage_rows_updated_total":           nil,
	}
	expected := map[string]bool{}
	for family, labels := range families {
		if len(labels) == 0 {
			expected[family] = false
		}
		for _, label := range labels {
			expected[family+"{"+label+"}"] = false
		}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, process.MetricsURL+"/metrics", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
	if err != nil {
		t.Fatal("actual Auth metrics listener unavailable")
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("actual metrics status=%d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 1024*1024))
	if err != nil {
		t.Fatal("actual metrics body unreadable")
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		if _, exists := families[strings.SplitN(fields[0], "{", 2)[0]]; !exists {
			continue
		}
		seen, fixed := expected[fields[0]]
		if !fixed || seen {
			t.Fatal("Auth maintenance metric exposed an unbounded, unexpected or repeated series")
		}
		expected[fields[0]] = true
	}
	for _, seen := range expected {
		if !seen {
			t.Fatal("actual Auth metrics listener omitted a fixed pruning or usage series")
		}
	}
	public, err := http.NewRequestWithContext(ctx, http.MethodGet, process.URL+"/metrics", nil)
	if err != nil {
		t.Fatal(err)
	}
	publicResponse, err := (&http.Client{Timeout: 5 * time.Second}).Do(public)
	if err != nil {
		t.Fatal("actual Auth public metrics separation probe failed")
	}
	defer func() { _ = publicResponse.Body.Close() }()
	if publicResponse.StatusCode != http.StatusNotFound {
		t.Fatal("Auth public listener exposed metrics")
	}
}
