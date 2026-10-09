package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/memory"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/testinfra"
	"github.com/tetral-ai/tetral/internal/workspace"
	tetralapi "github.com/tetral-ai/tetral/services/api"
)

// One native root composes a real HTTPS IdP, the actual Auth command, production
// role-separated API services, and a persistent process importing the pinned
// SDK source. A successful final response alone cannot prove the refresh path:
// ordered edge observations and durable SQL effects distinguish it below.
func TestOIDCKeycloakSDK(t *testing.T) {
	oidcIsolatedSDKIdentityCases(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		t.Cleanup(cancel)
		oidcVerifySDKPin(t)
		t.Cleanup(func() { oidcVerifySDKPin(t) })
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
				t.Errorf("real Keycloak realm cleanup: %v", err)
			}
		})
		_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		pools := storagetest.OpenWorkloadDB(t, admin, "auth")
		if _, err := workspace.NewSeeder(admin).Seed(ctx, workspace.DefaultID, "OIDC SDK default workspace"); err != nil {
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
		rule := auth.FederationRule{ID: "fdrl_oidc_sdk", OrganizationID: "org_oidc_sdk", Issuer: realm.Issuer, Audience: realm.Audience, JWKSURL: realm.JWKSURL, AllowedOrigins: []string{issuer.Scheme + "://" + issuer.Host}, AllowedCIDRs: realm.AllowedCIDRs, TrustedCAPEM: string(ca), Algorithm: "RS256", Enabled: true}
		actors := []struct {
			name, subject, kind, selector string
			assertion                     func(context.Context) (string, error)
		}{
			{"human", realm.HumanSubject, auth.IdentityHuman, "", realm.HumanAssertion},
			{"service", realm.ServiceSubject, auth.IdentityService, realm.ServiceAccountID, realm.ServiceAssertion},
		}
		document := auth.PolicyDocument{FederationRules: []auth.FederationRule{rule}}
		for _, actor := range actors {
			document.Identities = append(document.Identities, auth.IdentityBinding{ID: "identity_oidc_sdk_" + actor.name, OrganizationID: rule.OrganizationID, Issuer: realm.Issuer, Subject: actor.subject, Kind: actor.kind, ServiceAccountID: actor.selector, Enabled: true})
			document.WorkspaceGrants = append(document.WorkspaceGrants, auth.WorkspaceGrant{ID: "grant_oidc_sdk_" + actor.name, IdentityID: "identity_oidc_sdk_" + actor.name, WorkspaceID: workspace.DefaultID, Role: auth.WorkspaceFullAccess, Enabled: true})
		}
		policy := auth.NewPolicyStore(admin)
		if _, err := policy.Apply(ctx, document); err != nil {
			t.Fatal(err)
		}
		privateKey, err := auth.GenerateEd25519PrivateKeyBase64()
		if err != nil {
			t.Fatal(err)
		}
		signer, err := auth.NewInternalPrincipalSignerFromBase64(privateKey)
		if err != nil {
			t.Fatal(err)
		}
		verifier, err := auth.NewInternalPrincipalVerifierFromBase64(signer.PublicKeyBase64())
		if err != nil {
			t.Fatal(err)
		}
		authProcess := startOIDCAuthProcess(ctx, t, pools.DB, privateKey)
		apiPool := pools.OpenWorkload(t, "api", nil)
		dataDirectory := t.TempDir()
		if err := os.Chmod(dataDirectory, 0700); err != nil {
			t.Fatal(err)
		}
		api, err := tetralapi.BuildRouter(ctx, tetralapi.RouterConfig{RuntimeClient: dbconnect.NewClientForTesting(apiPool), RawDatabase: apiPool, VaultKey: sdkIntegrationVaultKey, DataDir: dataDirectory, Env: sdkIntegrationEnv{"TETRAL_DEFAULT_ENVIRONMENT_ARTIFACT_REF": "artifact_oidc_sdk"}, PrincipalVerifier: verifier})
		if err != nil {
			t.Fatal(err)
		}
		if owner, ok := api.(io.Closer); ok {
			t.Cleanup(func() {
				if err := owner.Close(); err != nil {
					t.Error("OIDC API resource cleanup failed")
				}
			})
		}
		apiServer := httptest.NewServer(api)
		t.Cleanup(apiServer.Close)
		edge := newOIDCEdge(t, authProcess.URL, apiServer.URL, authProcess.GRPCAddress)
		server := httptest.NewServer(edge)
		t.Cleanup(server.Close)

		for index, actor := range actors {
			t.Run(actor.name, func(t *testing.T) {
				assertion, err := actor.assertion(ctx)
				if err != nil {
					t.Fatal(err)
				}
				assertionPath := filepath.Join(t.TempDir(), "identity.jwt")
				if err := os.WriteFile(assertionPath, []byte(assertion), 0600); err != nil {
					t.Fatal(err)
				}
				mark := edge.mark()
				t.Cleanup(func() {
					if t.Failed() {
						t.Logf("OIDC sanitized boundary journal=%s", edge.safeJournal(mark))
					}
				})
				identityID := document.Identities[index].ID
				child := startOIDCSDKChild(ctx, t, map[string]string{"identityID": identityID, "baseURL": server.URL, "assertionPath": assertionPath, "ruleID": rule.ID, "organizationID": rule.OrganizationID, "workspaceID": "default", "serviceAccountID": actor.selector, "fixtureName": "oidc-sdk-" + actor.name})
				session := oidcDecodeSDKResult(t, child.control(t, "session", nil))
				var storedWorkspace string
				if err := admin.QueryRowContext(ctx, `SELECT workspace_id FROM sessions WHERE id=$1`, session.SessionID).Scan(&storedWorkspace); err != nil {
					t.Fatal(err)
				}
				if storedWorkspace != string(workspace.DefaultID) {
					t.Fatal("SDK Session escaped its selected Engine workspace")
				}
				store := oidcDecodeSDKResult(t, child.control(t, "store", nil))
				created := oidcDecodeSDKResult(t, child.control(t, "create", map[string]any{"storeID": store.StoreID, "path": "/before.md", "content": "before"}))
				oidcAssertMemoryActor(ctx, t, admin, created.VersionID, actor.kind, identityID, false)
				oidcAssertWireActor(t, created.Version.CreatedBy, actor.kind, identityID)
				oidcAssertCachedRequests(t, edge, mark)
				oidcAssertTokenPersistence(ctx, t, admin, edge, mark.exchanges, assertion, identityID)
				cached := edge.mark()
				child.control(t, "read", map[string]any{"storeID": store.StoreID, "memoryID": created.MemoryID})
				oidcAssertNoExchange(t, edge, cached)

				// Both committed edits target the existing root. Re-enabling it must not
				// resurrect a token issued at the previous grant revision. Apply's return
				// is the synchronization barrier; no sleep can stand in for revocation.
				grant := document.WorkspaceGrants[index]
				grant.Enabled = false
				if _, err := policy.Apply(ctx, auth.PolicyDocument{WorkspaceGrants: []auth.WorkspaceGrant{grant}}); err != nil {
					t.Fatal(err)
				}
				grant.Enabled = true
				if _, err := policy.Apply(ctx, auth.PolicyDocument{WorkspaceGrants: []auth.WorkspaceGrant{grant}}); err != nil {
					t.Fatal(err)
				}
				freshAssertion, err := actor.assertion(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(assertionPath, []byte(freshAssertion), 0600); err != nil {
					t.Fatal(err)
				}
				var versionsBefore int
				if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM memory_versions WHERE workspace_id=$1 AND memory_store_id=$2`, workspace.DefaultID, store.StoreID).Scan(&versionsBefore); err != nil {
					t.Fatal(err)
				}
				refresh := edge.mark()
				recovered := oidcDecodeSDKResult(t, child.control(t, "create", map[string]any{"storeID": store.StoreID, "path": "/after.md", "content": "after"}))
				oidcAssertOne401Refresh(t, edge, refresh, "/v1/memory_stores/"+store.StoreID+"/memories")
				oidcAssertTokenPersistence(ctx, t, admin, edge, refresh.exchanges, freshAssertion, identityID)
				var writes int
				if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM memory_versions WHERE workspace_id=$1 AND memory_store_id=$2 AND memory_id=$3`, workspace.DefaultID, store.StoreID, recovered.MemoryID).Scan(&writes); err != nil {
					t.Fatal(err)
				}
				var versionsAfter int
				if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM memory_versions WHERE workspace_id=$1 AND memory_store_id=$2`, workspace.DefaultID, store.StoreID).Scan(&versionsAfter); err != nil {
					t.Fatal(err)
				}
				if versionsAfter != versionsBefore+1 {
					t.Fatal("reactive refresh produced unexpected additional durable versions")
				}
				if writes != 1 {
					t.Fatalf("reactive refresh durable versions=%d; want exactly one", writes)
				}
				oidcAssertMemoryActor(ctx, t, admin, recovered.VersionID, actor.kind, identityID, false)
				oidcAssertWireActor(t, recovered.Version.CreatedBy, actor.kind, identityID)
				updated := oidcDecodeSDKResult(t, child.control(t, "update", map[string]any{"storeID": store.StoreID, "memoryID": recovered.MemoryID, "content": "modified"}))
				oidcAssertMemoryActor(ctx, t, admin, updated.VersionID, actor.kind, identityID, false)
				oidcAssertWireActor(t, updated.Version.CreatedBy, actor.kind, identityID)
				child.control(t, "delete", map[string]any{"storeID": store.StoreID, "memoryID": recovered.MemoryID})
				var deletedVersion string
				if err := admin.QueryRowContext(ctx, `SELECT memory_version_id FROM memory_versions WHERE workspace_id=$1 AND memory_id=$2 AND operation='deleted'`, workspace.DefaultID, recovered.MemoryID).Scan(&deletedVersion); err != nil {
					t.Fatal(err)
				}
				oidcAssertMemoryActor(ctx, t, admin, deletedVersion, actor.kind, identityID, false)
				var redacted memory.MemoryVersion
				if err := json.Unmarshal(child.control(t, "redact", map[string]any{"storeID": store.StoreID, "versionID": updated.VersionID}), &redacted); err != nil {
					t.Fatal(err)
				}
				if redacted.RedactedBy == nil || redacted.Content != nil || redacted.Path != nil {
					t.Fatal("SDK redact did not retain truthful redaction actor and clear content")
				}
				oidcAssertWireActor(t, *redacted.RedactedBy, actor.kind, identityID)
				oidcAssertMemoryActor(ctx, t, admin, updated.VersionID, actor.kind, identityID, true)
				oidcAssertWireActor(t, redacted.CreatedBy, actor.kind, identityID)
				t.Logf("oidc_sdk_assertion=%s_typed_created_by_redacted_by_stable_identity passed=true sdk_pin=%s", actor.name, oidcSDKPin)
				t.Logf("oidc_sdk_assertion=%s_session_memory_cached_token_one401_one_exchange_one_effect passed=true sdk_pin=%s", actor.name, oidcSDKPin)
			})
		}
	})
}

type oidcSDKResult struct {
	SessionID string               `json:"sessionID"`
	StoreID   string               `json:"storeID"`
	MemoryID  string               `json:"memoryID"`
	VersionID string               `json:"versionID"`
	Version   memory.MemoryVersion `json:"version"`
}

func oidcDecodeSDKResult(t *testing.T, data json.RawMessage) oidcSDKResult {
	t.Helper()
	var result oidcSDKResult
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal("SDK result is not its owning fixture DTO")
	}
	return result
}
func oidcAssertWireActor(t *testing.T, actor memory.Actor, kind, id string) {
	t.Helper()
	expected := memory.Actor{Type: "user_actor", UserID: id}
	if kind == auth.IdentityService {
		expected = memory.Actor{Type: "service_actor", ServiceID: id}
	}
	if actor != expected {
		t.Fatal("wire actor differs from stable Engine identity or carries a fictional credential")
	}
}
func oidcAssertMemoryActor(ctx context.Context, t *testing.T, admin *sql.DB, versionID, kind, id string, redacted bool) {
	t.Helper()
	var actorType string
	var key, session, human, service sql.NullString
	query := `SELECT created_actor_type,created_api_key_id,created_session_id,created_user_id,created_service_id FROM memory_versions WHERE memory_version_id=$1 AND workspace_id=$2`
	if redacted {
		query = `SELECT redacted_actor_type,redacted_api_key_id,redacted_session_id,redacted_user_id,redacted_service_id FROM memory_versions WHERE memory_version_id=$1 AND workspace_id=$2`
	}
	if err := admin.QueryRowContext(ctx, query, versionID, workspace.DefaultID).Scan(&actorType, &key, &session, &human, &service); err != nil {
		t.Fatal(err)
	}
	if key.Valid || session.Valid {
		t.Fatal("direct OIDC Memory write invented API-key or Session attribution")
	}
	if kind == auth.IdentityHuman {
		if actorType != "user_actor" || !human.Valid || human.String != id || service.Valid {
			t.Fatal("direct human Memory SQL actor is not its Engine identity")
		}
	} else if actorType != "service_actor" || !service.Valid || service.String != id || human.Valid {
		t.Fatal("direct service Memory SQL actor is not its Engine identity")
	}
}
func oidcAssertCachedRequests(t *testing.T, edge *oidcEdge, mark oidcEdgeMark) {
	t.Helper()
	edge.mu.Lock()
	defer edge.mu.Unlock()
	exchanges := edge.exchanges[mark.exchanges:]
	if len(exchanges) != 1 || exchanges[0].status != http.StatusOK || exchanges[0].tokenType != "Bearer" || exchanges[0].expires <= 120 || !exchanges[0].noStore || !exchanges[0].noCache || exchanges[0].token == "" {
		t.Fatal("actual SDK did not exchange once for a cacheable lifetime with protected response headers")
	}
	for _, attempt := range edge.attempts[mark.attempts:] {
		if attempt.status != http.StatusOK || attempt.bearer != "Bearer "+exchanges[0].token {
			t.Fatal("SDK did not reuse the exchanged opaque bearer for its public requests")
		}
	}
}
func oidcAssertNoExchange(t *testing.T, edge *oidcEdge, mark oidcEdgeMark) {
	t.Helper()
	edge.mu.Lock()
	defer edge.mu.Unlock()
	if len(edge.exchanges) != mark.exchanges || len(edge.attempts) <= mark.attempts {
		t.Fatal("cached SDK request unexpectedly exchanged or did not reach Auth")
	}
	for _, attempt := range edge.attempts[mark.attempts:] {
		if attempt.status != http.StatusOK {
			t.Fatal("cached public request failed")
		}
	}
}
func oidcAssertOne401Refresh(t *testing.T, edge *oidcEdge, mark oidcEdgeMark, path string) {
	t.Helper()
	edge.mu.Lock()
	defer edge.mu.Unlock()
	if len(edge.exchanges)-mark.exchanges != 1 {
		t.Fatal("reactive SDK refresh did not perform exactly one exchange")
	}
	var attempts []oidcAttempt
	for _, attempt := range edge.attempts[mark.attempts:] {
		if attempt.method == http.MethodPost && attempt.path == path {
			attempts = append(attempts, attempt)
		}
	}
	if len(attempts) != 2 || attempts[0].status != http.StatusUnauthorized || attempts[1].status != http.StatusOK {
		t.Fatal("SDK did not perform exactly cached bearer401 then successful retry")
	}
	exchange := edge.exchanges[mark.exchanges]
	if mark.exchanges == 0 || exchange.status != http.StatusOK || exchange.tokenType != "Bearer" || exchange.expires <= 120 || !exchange.noStore || !exchange.noCache || attempts[0].bearer != "Bearer "+edge.exchanges[mark.exchanges-1].token || attempts[0].bearer == attempts[1].bearer || attempts[1].bearer != "Bearer "+exchange.token || attempts[0].ordinal >= exchange.ordinal || exchange.ordinal >= attempts[1].ordinal {
		t.Fatal("SDK refresh did not replace the revoked bearer with the actual new exchange token")
	}
}

// Correlate the actual exchanged token with its database row through a private
// bind. Failure text contains neither token, assertion, digest nor row JSON.
func oidcAssertTokenPersistence(ctx context.Context, t *testing.T, admin *sql.DB, edge *oidcEdge, index int, assertion, identityID string) {
	t.Helper()
	edge.mu.Lock()
	if index < 0 || index >= len(edge.exchanges) {
		edge.mu.Unlock()
		t.Fatal("actual exchange record is absent")
	}
	token := edge.exchanges[index].token
	edge.mu.Unlock()
	digest := sha256.Sum256([]byte(token))
	var stored []byte
	var rowJSON, storedIdentity, storedWorkspace string
	if err := admin.QueryRowContext(ctx, `SELECT token_digest,to_jsonb(t)::text,identity_id,workspace_id FROM auth_access_tokens t WHERE token_digest=$1`, digest[:]).Scan(&stored, &rowJSON, &storedIdentity, &storedWorkspace); err != nil {
		t.Fatal("actual exchanged opaque token has no persisted digest row")
	}
	if len(stored) != sha256.Size || !bytes.Equal(stored, digest[:]) || storedIdentity != identityID || storedWorkspace != string(workspace.DefaultID) {
		t.Fatal("exchanged token persistence has invalid digest or authority identity")
	}
	if token == "" || assertion == "" || strings.Contains(rowJSON, token) || strings.Contains(rowJSON, assertion) {
		t.Fatal("exchange persisted raw credential material")
	}
}
