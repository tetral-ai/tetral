package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/integration/transporttest"
	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/auth/authtest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// This separate composition owns authorization denials and their effects through
// real Auth, API and Event Stream. The accepted business root owns SDK/Git/upload
// behavior; its unchanged assertions are not repeated here.
func TestPostgreSQLEnvoyGatewayAuthority(t *testing.T) {
	isolatedTLSPostgreSQLRoot(t, "envoy_gateway_authority_assertion=", envoyGatewayCompositionBudget, func(t *testing.T) {
		for _, profile := range []string{"standard-routed", "hardened"} {
			t.Run(profile, func(t *testing.T) {
				edge := &translatedPublicEdge{}
				f := newPublicProjectionFixture(t, publicProjectionOptions{publicEdge: edge.factory(profile)})
				issuer := newOIDCReplicaIssuer(t)
				identity := auth.IdentityBinding{ID: "identity_edge_authority", OrganizationID: issuer.rule.OrganizationID, Issuer: issuer.rule.Issuer, Subject: "edge-authority", Kind: auth.IdentityHuman, Enabled: true}
				grant := auth.WorkspaceGrant{ID: "grant_edge_authority", IdentityID: identity.ID, WorkspaceID: workspace.DefaultID, Role: auth.WorkspaceFullAccess, Enabled: true}
				if _, err := auth.NewPolicyStore(f.db).Apply(edge.ctx, auth.PolicyDocument{FederationRules: []auth.FederationRule{issuer.rule}, Identities: []auth.IdentityBinding{identity}, WorkspaceGrants: []auth.WorkspaceGrant{grant}}); err != nil {
					t.Fatal("provision actual edge authority fixture")
				}
				exchange := transporttest.Must(json.Marshal(map[string]string{"grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer", "assertion": issuer.assertion(t, identity.Subject), "federation_rule_id": issuer.rule.ID, "organization_id": issuer.rule.OrganizationID, "workspace_id": "default"}))
				status, body := edge.authorityRequest(edge.ctx, t, "POST", "/v1/oauth/token", "", "", exchange)
				var issued auth.AccessTokenResponse
				if status != 200 || json.Unmarshal(body, &issued) != nil || issued.AccessToken == "" {
					t.Fatal("real edge assertion exchange did not issue a bearer")
				}
				if issuer.counts() != (oidcIssuerCounts{total: 1, jwks: 1}) {
					t.Fatal("actual exchange did not fetch exactly the registered real HTTPS JWKS")
				}
				issuerBefore := issuer.counts()
				principal := transporttest.Must(auth.NewAuthorityResolver(f.db, workspace.DefaultID).AuthenticateBearer(edge.ctx, issued.AccessToken))
				restricted := transporttest.Must(auth.GenerateAPIKey())
				// Provision an old, immutable narrower issuance ceiling using the same
				// accepted PR6 fixture boundary. Current product role remains full access;
				// authentication and every public authorization decision use production code.
				if _, err := f.db.ExecContext(edge.ctx, `INSERT INTO api_keys(id,workspace_id,name,key_prefix,key_digest,key_kind,authority_kind,federation_rule_id,identity_id,grant_id,rule_revision,identity_revision,grant_revision,role_version,issuance_operations,parent_credential_id,created_at) VALUES('ak_edge_restricted','default','historical restricted fixture','fixture',$1,'standard','identity_grant',$2,$3,$4,$5,$6,$7,$8,'["sessions.read"]'::jsonb,$9,clock_timestamp())`, auth.DigestAPIKey(restricted), principal.Authority.RuleID, principal.Authority.IdentityID, principal.Authority.GrantID, principal.Authority.RuleRevision, principal.Authority.IdentityRevision, principal.Authority.GrantRevision, principal.Authority.PolicyVersion, principal.Credential.ID); err != nil {
					t.Fatal("provision restricted immutable derived key fixture")
				}
				baseline := edgeAuthorityEffects(edge.ctx, t, f.db, f.session)
				for _, vector := range []struct {
					method, path, body string
					event              bool
				}{
					{"POST", "/v1/sessions", `{"agent_id":"unreached"}`, false},
					{"GET", "/v1/memory_stores?beta=true", "", false},
					{"GET", "/v1/memory_stores/memstore_missing?beta=true", "", false},
					{"GET", "/v1/sessions/" + f.session + "/events", "", false},
					{"GET", "/v1/sessions/" + f.session + "/events/stream", "", true},
					{"GET", "/v1/sessions/" + f.session + "/threads/" + f.thread(t) + "/stream", "", true},
				} {
					beforeAPI, beforeEvent := edge.apiRequests.Load(), edge.eventRequests.Load()
					status, body = edge.authorityRequest(edge.ctx, t, vector.method, vector.path, restricted, "", []byte(vector.body))
					if status != 403 || edgeAuthorityErrorType(t, body) != "permission_error" || bytes.Contains(body, []byte(f.session)) {
						t.Fatal("actual authenticated permission denial changed status/type or disclosed selected resource")
					}
					wantAPI, wantEvent := int64(1), int64(0)
					if vector.event {
						wantAPI, wantEvent = 0, 1
					}
					if edge.apiRequests.Load() != beforeAPI+wantAPI || edge.eventRequests.Load() != beforeEvent+wantEvent {
						t.Fatal("actual permission denial did not reach exactly its owning business receiver")
					}
				}
				if edgeAuthorityEffects(edge.ctx, t, f.db, f.session) != baseline {
					t.Fatal("authenticated denied operations changed durable business state")
				}
				if _, err := f.db.ExecContext(edge.ctx, `INSERT INTO workspaces(id,type,name,created_at) VALUES('edge_other','workspace','Other',clock_timestamp())`); err != nil {
					t.Fatal(err)
				}
				other := transporttest.Must(authtest.SeedIndependentKey(edge.ctx, f.db, "edge_other", "other authority"))
				before := edge.apiRequests.Load()
				beforeChecks := edgeAuthCheckCount(edge.ctx, t, edge.fixture.Ports.Admin)
				status, body = edge.authorityRequest(edge.ctx, t, "GET", "/v1/sessions/"+f.session+"?beta=true", other.APIKey, "", nil)
				errorType := edgeAuthorityErrorType(t, body)
				apiDelta := edge.apiRequests.Load() - before
				checkDelta := edgeAuthCheckCount(edge.ctx, t, edge.fixture.Ports.Admin) - beforeChecks
				disclosedSession := bytes.Contains(body, []byte(f.session))
				t.Logf("envoy_gateway_authority_wrong_workspace profile=%s status=%d error_type=%s api_delta=%d check_delta=%d disclosed_session=%t", profile, status, errorType, apiDelta, checkDelta, disclosedSession)
				if status != 404 || errorType != "not_found_error" || disclosedSession || apiDelta != 1 || checkDelta != 1 {
					t.Fatal("wrong-workspace credential escaped real API isolation")
				}
				status, _ = edge.authorityRequest(edge.ctx, t, "GET", "/v1/api_keys-extra", edge.key, "", nil)
				if status != 404 {
					t.Fatal("ordinary API did not retain its own missing-route 404")
				}
				// A real issued bearer makes precedence and duplicate rejection distinct
				// from an invalid-token control; raw TLS bytes preserve each header entry.
				roots := edge.client.Transport.(*http.Transport).TLSClientConfig.RootCAs
				for _, vector := range []struct {
					headers string
					want    int
				}{
					{"Authorization: Bearer " + issued.AccessToken + "\r\n", 200},
					{"X-Api-Key: invalid\r\nAuthorization: Bearer " + issued.AccessToken + "\r\n", 401},
					{"x-aPi-kEy: " + edge.key + "\r\nAuthorization: Bearer invalid\r\n", 200},
					{"Authorization: Bearer " + issued.AccessToken + "\r\nAuthorization: Bearer invalid\r\n", 401},
				} {
					before := edge.apiRequests.Load()
					beforeChecks := edgeAuthCheckCount(edge.ctx, t, edge.fixture.Ports.Admin)
					response := edgeRawTLSRequest(edge.ctx, t, edge.fixture.Ports.HTTPS, roots, "GET", "api.localhost", "/v1/sessions/"+f.session+"?beta=true", vector.headers)
					body = transporttest.Must(io.ReadAll(response.Body))
					_ = response.Body.Close()
					wantDelta := int64(0)
					if vector.want == 200 {
						wantDelta = 1
					}
					if response.StatusCode != vector.want || edge.apiRequests.Load() != before+wantDelta || edgeAuthCheckCount(edge.ctx, t, edge.fixture.Ports.Admin) != beforeChecks+1 {
						t.Fatal("real bearer/raw credential precedence did not preserve exact actual backend admission")
					}
					if vector.want == 200 && !bytes.Contains(body, []byte(f.session)) {
						t.Fatal("admitted real credential did not retrieve its actual scoped session")
					}
				}
				if issuer.counts() != issuerBefore {
					t.Fatal("frozen bearer/derived key admission unexpectedly contacted issuer")
				}
				key := edge.authorityCreateKey(t, "revoke through actual Auth CRUD")
				status, _ = edge.authorityRequest(edge.ctx, t, "GET", "/v1/sessions/"+f.session+"?beta=true", key.APIKey, "", nil)
				if status != 200 {
					t.Fatal("fresh actual key was not admitted")
				}
				status, _ = edge.authorityRequest(edge.ctx, t, "DELETE", "/v1/api_keys/"+key.ID, edge.key, "", nil)
				if status != 204 {
					t.Fatal("actual Auth CRUD did not revoke selected key")
				}
				before = edge.apiRequests.Load()
				status, _ = edge.authorityRequest(edge.ctx, t, "GET", "/v1/sessions/"+f.session+"?beta=true", key.APIKey, "", nil)
				if status != 401 || edge.apiRequests.Load() != before {
					t.Fatal("new admission after actual revocation reached business receiver")
				}
				edge.assertHeldAuthority(t, f)
				if edgeAuthorityEffects(edge.ctx, t, f.db, f.session) != baseline {
					t.Fatal("authorization isolation, rejection or held Check controls changed durable business state")
				}
				t.Logf("envoy_gateway_authority_assertion=actual_receivers profile=%s permission403_noeffects=true wrongworkspace404=true bearer_precedence=true revocation401=true held_check=true passed=true", profile)
			})
		}
	})
}

func (edge *translatedPublicEdge) authorityRequest(ctx context.Context, t *testing.T, method, path, key, bearer string, body []byte) (int, []byte) {
	t.Helper()
	request := transporttest.Must(http.NewRequestWithContext(ctx, method, edge.baseURL+path, bytes.NewReader(body)))
	request.Header.Set("Content-Type", "application/json")
	if key != "" {
		request.Header.Set("X-Api-Key", key)
	}
	if bearer != "" {
		request.Header.Set("Authorization", "Bearer "+bearer)
	}
	response, err := edge.client.Do(request)
	if err != nil {
		t.Fatal("actual authority public request failed")
	}
	defer func() { _ = response.Body.Close() }()
	data := transporttest.Must(io.ReadAll(io.LimitReader(response.Body, 1<<20)))
	return response.StatusCode, data
}
func (edge *translatedPublicEdge) authorityCreateKey(t *testing.T, name string) auth.CreateAPIKeyResult {
	t.Helper()
	body := transporttest.Must(json.Marshal(map[string]string{"name": name}))
	status, data := edge.authorityRequest(edge.ctx, t, "POST", "/v1/api_keys", edge.key, "", body)
	var created auth.CreateAPIKeyResult
	if status != 200 || json.Unmarshal(data, &created) != nil || created.APIKey == "" || created.ID == "" {
		t.Fatal("actual Auth CRUD did not create declared selected key")
	}
	return created
}
func edgeAuthorityErrorType(t *testing.T, body []byte) string {
	t.Helper()
	var data struct {
		Error struct {
			Type string `json:"type"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &data) != nil {
		t.Fatal("actual denial omitted typed JSON error")
	}
	return data.Error.Type
}

type edgeAuthorityState struct {
	Sessions, Events, Inbox, Jobs int
	Session                       string
}

func edgeAuthorityEffects(ctx context.Context, t *testing.T, db *sql.DB, session string) edgeAuthorityState {
	t.Helper()
	var state edgeAuthorityState
	if err := db.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM sessions),(SELECT count(*) FROM session_events),(SELECT count(*) FROM session_runtime_inbox),(SELECT count(*) FROM queue_jobs),(SELECT row_to_json(s)::text FROM sessions s WHERE id=$1)`, session).Scan(&state.Sessions, &state.Events, &state.Inbox, &state.Jobs, &state.Session); err != nil {
		t.Fatal("durable authority effect snapshot unavailable")
	}
	return state
}
func (edge *translatedPublicEdge) assertHeldAuthority(t *testing.T, f *publicProjectionFixture) {
	t.Helper()
	for _, timeout := range []bool{false, true} {
		key := edge.authorityCreateKey(t, "held Check")
		held := transporttest.Must(f.db.BeginTx(edge.ctx, nil))
		t.Cleanup(func() { _ = held.Rollback() })
		var holder int
		if held.QueryRowContext(edge.ctx, `SELECT pg_backend_pid()`).Scan(&holder) != nil {
			t.Fatal("held authority PG identity unavailable")
		}
		if _, err := held.ExecContext(edge.ctx, `UPDATE api_keys SET revoked_at=clock_timestamp() WHERE id=$1`, key.ID); err != nil {
			t.Fatal(err)
		}
		requestCtx, cancel := context.WithTimeout(edge.ctx, 8*time.Second)
		t.Cleanup(cancel)
		request := transporttest.Must(http.NewRequestWithContext(requestCtx, "GET", edge.baseURL+"/v1/sessions/"+f.session+"?beta=true", nil))
		request.Header.Set("X-Api-Key", key.APIKey)
		type outcome struct {
			status int
			err    error
		}
		done := make(chan outcome, 1)
		joined := false
		t.Cleanup(func() {
			cancel()
			if !joined {
				select {
				case <-done:
					joined = true
				case <-time.After(6 * time.Second):
					t.Error("held public Check caller did not join cleanup")
				}
			}
		})
		before := edge.apiRequests.Load()
		go func() {
			response, err := edge.client.Do(request)
			if err != nil {
				done <- outcome{err: err}
				return
			}
			_ = response.Body.Close()
			done <- outcome{status: response.StatusCode}
		}()
		waitCtx, stop := context.WithTimeout(edge.ctx, 3*time.Second)
		waiter := edgeAwaitAuthorityBlock(waitCtx, t, f.db, holder)
		stop()
		if !timeout {
			cancel()
		}
		select {
		case result := <-done:
			joined = true
			if timeout {
				if result.err != nil || result.status != 503 {
					t.Fatal("actual five-second held Check timeout did not fail closed with 503")
				}
			} else if !errors.Is(result.err, context.Canceled) {
				t.Fatal("shorter downstream cancellation did not join held edge request")
			}
		case <-time.After(9 * time.Second):
			t.Fatal("held edge request did not join before its owning deadline")
		}
		cancel()
		releaseCtx, release := context.WithTimeout(edge.ctx, 3*time.Second)
		ticker := time.NewTicker(10 * time.Millisecond)
		for {
			var blocked bool
			if f.db.QueryRowContext(releaseCtx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND cardinality(pg_blocking_pids(pid))>0)`, waiter).Scan(&blocked) != nil {
				ticker.Stop()
				release()
				t.Fatal("held Check cancellation lock census unavailable")
			}
			if !blocked {
				break
			}
			select {
			case <-releaseCtx.Done():
				ticker.Stop()
				release()
				t.Fatal("held Check retained blocked Auth work after request completion")
			case <-ticker.C:
			}
		}
		ticker.Stop()
		release()
		var lastUsed sql.NullTime
		if f.db.QueryRowContext(edge.ctx, `SELECT last_used_at FROM api_keys WHERE id=$1`, key.ID).Scan(&lastUsed) != nil || lastUsed.Valid || edge.apiRequests.Load() != before {
			t.Fatal("failed held Check changed usage or forwarded to business backend")
		}
		if held.Rollback() != nil {
			t.Fatal("held authority transaction did not release")
		}
		status, _ := edge.authorityRequest(edge.ctx, t, "GET", "/v1/sessions/"+f.session+"?beta=true", key.APIKey, "", nil)
		if status != 200 {
			t.Fatal("held Check rollback did not restore actual authority admission")
		}
	}
}
func edgeAwaitAuthorityBlock(ctx context.Context, t *testing.T, db *sql.DB, holder int) int {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiter int
		err := db.QueryRowContext(ctx, `SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND $1=ANY(pg_blocking_pids(pid)) ORDER BY pid LIMIT 1`, holder).Scan(&waiter)
		if err == nil {
			return waiter
		}
		if err != sql.ErrNoRows {
			t.Fatal("actual Check lock graph unavailable")
		}
		select {
		case <-ctx.Done():
			t.Fatal("actual Check did not reach held PG row lock")
		case <-ticker.C:
		}
	}
}
