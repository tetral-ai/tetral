package integration

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/auth/authtest"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
	computerhub "github.com/tetral-ai/tetral/services/computer-hub"
)

// computerHubManagementRoutes are the developer and Controller routes of
// instances, instance requests and Session computers.
var computerHubManagementRoutes = [][2]string{
	{"GET", "/v1/instance_requests"},
	{"GET", "/v1/instance_requests/stream"},
	{"POST", "/v1/instance_requests/ireq_class/claim"},
	{"POST", "/v1/instance_requests/ireq_class/release"},
	{"POST", "/v1/instance_requests/ireq_class/fail"},
	{"POST", "/v1/instances"},
	{"GET", "/v1/instances"},
	{"GET", "/v1/instances/stream"},
	{"GET", "/v1/instances/inst_class"},
	{"DELETE", "/v1/instances/inst_class"},
	{"POST", "/v1/instances/inst_class/credentials"},
	{"GET", "/v1/instances/inst_class/credentials"},
	{"DELETE", "/v1/instances/inst_class/credentials/ccred_class"},
	{"POST", "/v1/sessions/sesn_class/computers"},
	{"GET", "/v1/sessions/sesn_class/computers/comp_class"},
	{"DELETE", "/v1/sessions/sesn_class/computers/comp_class"},
}

// TestComputerHubSeparatesConnectionKeysFromWorkspaceCredentials sends each
// credential class to the routes of the other class, once through auth's
// Check and a Go edge that routes the hub's prefixes to computer-hub, and
// once straight to computer-hub with a principal Check never mints there.
// No attempt changes a row of any table.
func TestComputerHubSeparatesConnectionKeysFromWorkspaceCredentials(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	ws := workspace.DefaultID
	if _, err := workspace.NewSeeder(admin).Seed(ctx, ws, "class"); err != nil {
		t.Fatal(err)
	}
	workloads := storagetest.OpenWorkloadDB(t, admin, "auth")
	authDB := workloads.DB
	apiKey, err := authtest.SeedIndependentKey(ctx, authDB, ws, "class")
	if err != nil {
		t.Fatal(err)
	}
	accessToken := seedClassAccessToken(ctx, t, admin, ws)
	if _, err := auth.NewAuthorityResolver(authDB, ws, nil).AuthenticateBearer(ctx, accessToken); err != nil {
		t.Fatalf("seeded access token does not authenticate: %v", err)
	}
	connectionKey := instance.GenerateKey()
	name := "class"
	var registered instance.Instance
	var credential instance.Credential
	if err := dbconnect.NewClientForTesting(admin).WithWorkspaceTx(ctx, string(ws), "integration.class_connection_key", func(tx *dbconnect.Tx) error {
		if registered, err = instance.InsertInstance(ctx, tx, ws, instance.NewInstance{Kind: instance.KindUser, Name: &name}); err != nil {
			return err
		}
		credential, err = instance.InsertCredential(ctx, tx, ws, instance.NewCredential{InstanceID: registered.ID, State: instance.CredentialActive, Key: connectionKey, ExpiresAt: time.Now().Add(time.Hour)})
		return err
	}); err != nil {
		t.Fatal(err)
	}

	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewInternalPrincipalSigner(private)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := auth.NewInternalPrincipalVerifier(public)
	if err != nil {
		t.Fatal(err)
	}
	hubDB := dbconnect.NewClientForTesting(workloads.OpenWorkload(t, "computer_hub", nil))
	instances := computerhub.InstanceRoutes{Database: hubDB, MaxCredentialLifetime: time.Hour, PageTokenKey: make([]byte, 32), Events: computerhub.NewInstanceEvents(hubDB, "0.0.0", nil)}
	hub := httptest.NewServer(computerhub.BuildHTTPHandler(workload.NewReadiness(), computerhub.NewRouter(verifier, computerhub.WithInstanceRoutes(instances))))
	t.Cleanup(hub.Close)
	edge := httptest.NewServer(classEdge{checkAddress: startSDKAuthorization(t, authDB, signer, time.Minute), hubURL: hub.URL})
	t.Cleanup(edge.Close)

	type attempt struct {
		name, method, path string
		headers            http.Header
		principal          *auth.Principal
		clientCode         string
		apiErrorType       string
	}
	connection := http.Header{"Authorization": {"Bearer " + connectionKey.Reveal()}}
	var attempts []attempt
	for _, route := range append(computerHubManagementRoutes, [2]string{"POST", "/v1/sessions"}) {
		attempts = append(attempts, attempt{name: "connection key at the edge", method: route[0], path: route[1], headers: connection, apiErrorType: "authentication_error"})
		if strings.HasPrefix(route[1], "/v1/instance") {
			p := auth.ConnectionKeyPrincipal(workspace.Workspace{ID: ws}, registered.ID, credential.ID)
			attempts = append(attempts, attempt{name: "connection key principal at the hub", method: route[0], path: route[1], principal: &p, apiErrorType: "authentication_error"})
		}
	}
	keyPrincipal := auth.IndependentKeyPrincipal(workspace.Workspace{ID: ws}, apiKey.ID)
	tokenPrincipal := auth.Principal{Workspace: workspace.Workspace{ID: ws}, Identity: &auth.Identity{ID: "identity_class", Kind: auth.IdentityService}, Credential: auth.Credential{ID: "at_class", Kind: auth.CredentialAccessToken}, Authority: auth.Authority{Kind: auth.AuthorityIdentityGrant, RuleID: "rule_class", IdentityID: "identity_class", GrantID: "grant_class", RuleRevision: 1, IdentityRevision: 1, GrantRevision: 1, PolicyVersion: auth.PolicyVersion, Operations: auth.RegisteredOperations(), Scope: auth.ResourceReference{WorkspaceID: ws, Type: "workspace"}}}
	attempts = append(attempts,
		attempt{name: "api key at the edge", method: "POST", path: "/v1/client/ready", headers: http.Header{"X-Api-Key": {apiKey.APIKey}}, clientCode: "credential_invalid"},
		attempt{name: "access token at the edge", method: "POST", path: "/v1/client/ready", headers: http.Header{"Authorization": {"Bearer " + accessToken}}, clientCode: "credential_invalid"},
		attempt{name: "api key principal at the hub", method: "POST", path: "/v1/client/ready", principal: &keyPrincipal, clientCode: "credential_invalid"},
		attempt{name: "access token principal at the hub", method: "POST", path: "/v1/client/ready", principal: &tokenPrincipal, clientCode: "credential_invalid"},
	)
	for _, a := range attempts {
		t.Run(a.name+" "+a.method+" "+a.path, func(t *testing.T) {
			before := countEveryTable(ctx, t, admin)
			target := edge.URL + a.path
			headers := a.headers.Clone()
			if a.principal != nil {
				target = hub.URL + a.path
				token, err := signer.Mint(*a.principal, a.method, a.path, "req_class", time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				headers = http.Header{"X-Tetral-Internal-Principal": {token}}
			}
			var body io.Reader
			if a.method == "POST" {
				body = strings.NewReader("{}")
			}
			request, err := http.NewRequestWithContext(ctx, a.method, target, body)
			if err != nil {
				t.Fatal(err)
			}
			request.Header = headers
			request.Header.Set("Content-Type", "application/json")
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if response.StatusCode != http.StatusUnauthorized {
				t.Fatalf("status %d body %s; want 401", response.StatusCode, raw)
			}
			for _, secret := range []string{connectionKey.Reveal(), apiKey.APIKey, accessToken} {
				if bytes.Contains(raw, []byte(secret)) {
					t.Fatal("the answer holds a credential")
				}
			}
			if a.clientCode != "" {
				var denial map[string]any
				if err := json.Unmarshal(raw, &denial); err != nil || len(denial) != 2 || denial["code"] != a.clientCode || denial["message"] == "" {
					t.Fatalf("client body %s; want code %s", raw, a.clientCode)
				}
			} else {
				var envelope struct {
					Type  string `json:"type"`
					Error struct {
						Type string `json:"type"`
					} `json:"error"`
				}
				if err := json.Unmarshal(raw, &envelope); err != nil || envelope.Type != "error" || envelope.Error.Type != a.apiErrorType {
					t.Fatalf("API body %s; want error.type %s", raw, a.apiErrorType)
				}
			}
			if after := countEveryTable(ctx, t, admin); fmt.Sprint(after) != fmt.Sprint(before) {
				t.Fatalf("row counts changed: %v -> %v", before, after)
			}
		})
	}
}

// classEdge stands in for the public edge: Check first, then the route rules
// that send the hub's prefixes to computer-hub. This test reaches no other
// backend.
type classEdge struct {
	checkAddress string
	hubURL       string
}

func (e classEdge) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	headers := r.Header.Clone()
	stripSDKIntegrationUntrustedMetadata(headers)
	response, err := directEdgeCheck(r.Context(), e.checkAddress, r.Method, r.URL.RequestURI(), "req_class_edge", headers)
	if err != nil {
		http.Error(w, "authorization unavailable", http.StatusServiceUnavailable)
		return
	}
	status, principal := directEdgeCheckStatus(response)
	if status != http.StatusOK {
		forwardFixtureCheckDenial(w, response)
		return
	}
	hubRoute := false
	for _, prefix := range []string{"/v1/client", "/v1/instance_requests", "/v1/instances"} {
		hubRoute = hubRoute || r.URL.Path == prefix || strings.HasPrefix(r.URL.Path, prefix+"/")
	}
	if !hubRoute {
		http.Error(w, "no backend in this test", http.StatusBadGateway)
		return
	}
	//nolint:gosec // G704: the URL is this test's own hub server.
	outbound, err := http.NewRequestWithContext(r.Context(), r.Method, e.hubURL+r.URL.RequestURI(), r.Body)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadGateway)
		return
	}
	outbound.Header.Set("X-Tetral-Internal-Principal", principal)
	//nolint:gosec // G704: the URL is this test's own hub server.
	forwarded, err := http.DefaultClient.Do(outbound)
	if err != nil {
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
		return
	}
	defer func() { _ = forwarded.Body.Close() }()
	w.Header().Set("Content-Type", forwarded.Header.Get("Content-Type"))
	w.WriteHeader(forwarded.StatusCode)
	_, _ = io.Copy(w, forwarded.Body)
}

// seedClassAccessToken writes a valid access token for a service identity
// with a full-access grant in ws and returns its raw value.
func seedClassAccessToken(ctx context.Context, t *testing.T, admin *sql.DB, ws workspace.ID) string {
	t.Helper()
	raw := "tetral_at_class_" + strings.Repeat("a", 32)
	digest := sha256.Sum256([]byte(raw))
	for _, statement := range []string{
		`INSERT INTO auth_federation_rules (id,organization_id,issuer,audience,security_config,enabled) VALUES ('rule_class','org_class','https://issuer.class','tetral-engine','{}',$1)`,
		`INSERT INTO auth_identities (id,organization_id,issuer,subject,identity_kind,service_account_id,enabled) VALUES ('identity_class','org_class','https://issuer.class','service','service','service_class',$1)`,
	} {
		if _, err := admin.ExecContext(ctx, statement, true); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := admin.ExecContext(ctx, `INSERT INTO auth_workspace_grants (id,identity_id,workspace_id,role,role_version,enabled) VALUES ('grant_class','identity_class',$1,'workspace_full_access',1,true)`, string(ws)); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, `INSERT INTO auth_access_tokens (id,workspace_id,token_digest,federation_rule_id,identity_id,grant_id,rule_revision,identity_revision,grant_revision,role_version,expires_at) VALUES ('at_class',$1,$2,'rule_class','identity_class','grant_class',1,1,1,$3,clock_timestamp()+interval '1 hour')`, string(ws), digest[:], auth.PolicyVersion); err != nil {
		t.Fatal(err)
	}
	return raw
}

// countEveryTable returns the row count of every table in the test schema.
func countEveryTable(ctx context.Context, t *testing.T, admin *sql.DB) map[string]int64 {
	t.Helper()
	rows, err := admin.QueryContext(ctx, `SELECT tablename FROM pg_tables WHERE schemaname=current_schema() ORDER BY tablename`)
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, table)
	}
	_ = rows.Close()
	counts := make(map[string]int64, len(tables))
	for _, table := range tables {
		var count int64
		//nolint:gosec // G202: table names come from the catalog of the test database.
		if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM "`+table+`"`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		counts[table] = count
	}
	if len(counts) == 0 || counts["instance_credentials"] != 1 {
		t.Fatalf("table counts = %v", counts)
	}
	return counts
}
