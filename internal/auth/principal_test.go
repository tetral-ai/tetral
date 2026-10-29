package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestPrincipalAuthorityUnionAndOperationGate(t *testing.T) {
	independent := IndependentKeyPrincipal(workspace.Workspace{ID: "workspace-a"}, "ak_actual")
	op := OperationSessionsList
	if err := Authorize(independent, op, ResourceReference{WorkspaceID: "workspace-a", Type: "workspace"}); err != nil {
		t.Fatal(err)
	}
	if err := Authorize(independent, op, ResourceReference{WorkspaceID: "workspace-b", Type: "workspace"}); err == nil {
		t.Fatal("foreign workspace admitted")
	}
	if err := Authorize(independent, Operation("GET /v1/future_action"), ResourceReference{WorkspaceID: "workspace-a", Type: "workspace"}); err == nil {
		t.Fatal("unregistered future operation admitted")
	}
	restricted := independent
	restricted.Authority.Operations = []Operation{op}
	if err := Authorize(restricted, OperationSessionsCreate, ResourceReference{WorkspaceID: "workspace-a", Type: "workspace"}); err == nil {
		t.Fatal("restricted ceiling widened")
	}
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := NewInternalPrincipalSigner(private)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{IdentityHuman, IdentityService} {
		federated := Principal{Workspace: independent.Workspace, Identity: &Identity{ID: "identity-stable", Kind: kind}, Credential: Credential{ID: "at_opaque_id", Kind: CredentialAccessToken}, Authority: Authority{Kind: AuthorityIdentityGrant, RuleID: "rule-a", IdentityID: "identity-stable", GrantID: "grant-a", RuleRevision: 1, IdentityRevision: 2, GrantRevision: 3, PolicyVersion: PolicyVersion, Operations: []Operation{op}, Scope: ResourceReference{WorkspaceID: "workspace-a", Type: "workspace"}}}
		if err := federated.Validate(); err != nil {
			t.Fatal(err)
		}
		derived := federated
		derived.Credential = Credential{ID: "ak_derived", Kind: CredentialAPIKey}
		derived.APIKeyID = "ak_derived"
		if err := derived.Validate(); err != nil {
			t.Fatal(err)
		}
		for _, admission := range []Principal{federated, derived} {
			token, err := signer.Mint(admission, "GET", "/v1/sessions", "req-union", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			verified, _, err := signer.Verify(token, "GET", "/v1/sessions")
			if err != nil {
				t.Fatal(err)
			}
			if verified.Identity == nil || verified.Identity.Kind != kind || verified.Identity.ID != "identity-stable" || verified.Credential != admission.Credential || verified.APIKeyID != admission.APIKeyID || verified.Authority.GrantRevision != 3 {
				t.Fatal("signed actor/provenance union lost")
			}
			if err := Authorize(verified, OperationSessionsCreate, ResourceReference{WorkspaceID: "workspace-a", Type: "workspace"}); err == nil {
				t.Fatal("signed root ceiling widened")
			}
		}
		invalid := federated
		invalid.APIKeyID = "ak_fabricated"
		if invalid.Validate() == nil {
			t.Fatal("OIDC identity accepted fabricated key attribution")
		}
		invalid = federated
		invalid.Authority.GrantRevision = 0
		if invalid.Validate() == nil {
			t.Fatal("incomplete root authority admitted")
		}
	}
	if (Principal{Workspace: independent.Workspace, APIKeyID: "ak_partial"}).Validate() == nil {
		t.Fatal("partial principal upgraded")
	}
	if _, err := RoleOperations("restricted_fixture"); err == nil {
		t.Fatal("fixture role publicly assignable")
	}
	operations := RegisteredOperations()
	operations[0] = "mutated"
	if IsRegisteredOperation("mutated") {
		t.Fatal("registry mutable by caller")
	}
}

func TestConnectionKeyPrincipalCarriesOnlyClientConnect(t *testing.T) {
	ws := workspace.Workspace{ID: "workspace-a"}
	connection := ConnectionKeyPrincipal(ws, "inst_a", "ccred_a")
	if err := connection.Validate(); err != nil {
		t.Fatal(err)
	}
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := NewInternalPrincipalSigner(private)
	if err != nil {
		t.Fatal(err)
	}
	token, err := signer.Mint(connection, "POST", "/v1/client/ready", "req-connection", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	verified, _, err := signer.Verify(token, "POST", "/v1/client/ready")
	if err != nil || verified.Credential != connection.Credential || verified.Authority.Scope != connection.Authority.Scope || len(verified.Authority.Operations) != 1 || verified.Authority.Operations[0] != OperationClientConnect {
		t.Fatalf("signed connection principal = %+v, %v", verified, err)
	}
	var credentialError *AuthenticationError
	if err := Authorize(connection, OperationSessionsList, ResourceReference{WorkspaceID: ws.ID, Type: "workspace"}); !errors.As(err, &credentialError) {
		t.Fatalf("connection key on a public operation = %v; want an authentication error", err)
	}
	if IsRegisteredOperation(OperationClientConnect) {
		t.Fatal("client.connect is registered")
	}
	if operations, err := RoleOperations(WorkspaceFullAccess); err != nil || slices.Contains(operations, OperationClientConnect) {
		t.Fatal("workspace_full_access includes client.connect")
	}
	federated := Principal{Workspace: ws, Identity: &Identity{ID: "identity-a", Kind: IdentityService}, Credential: Credential{ID: "at_a", Kind: CredentialAccessToken}, Authority: Authority{Kind: AuthorityIdentityGrant, RuleID: "rule-a", IdentityID: "identity-a", GrantID: "grant-a", RuleRevision: 1, IdentityRevision: 1, GrantRevision: 1, PolicyVersion: PolicyVersion, Operations: []Operation{OperationClientConnect}, Scope: ResourceReference{WorkspaceID: ws.ID, Type: "workspace"}}}
	independent := IndependentKeyPrincipal(ws, "ak_a")
	independent.Authority.Operations = []Operation{OperationClientConnect}
	for name, mutate := range map[string]func(*Principal){
		"workspace scope":        func(p *Principal) { p.Authority.Scope = ResourceReference{WorkspaceID: ws.ID, Type: "workspace"} },
		"scope without instance": func(p *Principal) { p.Authority.Scope.ID = "" },
		"scope in another space": func(p *Principal) { p.Authority.Scope.WorkspaceID = "workspace-b" },
		"a registered operation": func(p *Principal) {
			p.Authority.Operations = []Operation{OperationClientConnect, OperationSessionsList}
		},
		"no operation":                func(p *Principal) { p.Authority.Operations = nil },
		"api key attribution":         func(p *Principal) { p.APIKeyID = "ak_a" },
		"an identity":                 func(p *Principal) { p.Identity = &Identity{ID: "identity-a", Kind: IdentityService} },
		"independent key kind":        func(p *Principal) { p.Authority.Kind = AuthorityIndependentKey },
		"grant lineage":               func(p *Principal) { p.Authority.GrantID = "grant-a" },
		"api key with client.connect": func(p *Principal) { *p = independent },
		"token with client.connect":   func(p *Principal) { *p = federated },
		"instance scope on an api key": func(p *Principal) {
			*p = IndependentKeyPrincipal(ws, "ak_a")
			p.Authority.Scope = connection.Authority.Scope
		},
	} {
		invalid := ConnectionKeyPrincipal(ws, "inst_a", "ccred_a")
		mutate(&invalid)
		if invalid.Validate() == nil {
			t.Fatalf("%s: principal validated", name)
		}
	}
}

func TestSignedPrincipalMandatoryProvenanceAndRequestBinding(t *testing.T) {
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := NewInternalPrincipalSigner(private)
	if err != nil {
		t.Fatal(err)
	}
	p := IndependentKeyPrincipal(workspace.Workspace{ID: "workspace-a"}, "ak_actual")
	p.Authority.Operations = []Operation{OperationSessionsList}
	token, err := signer.Mint(p, "GET", "/v1/sessions", "req-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := signer.Verify(token, "GET", "/v1/sessions")
	if err != nil {
		t.Fatal(err)
	}
	if got.APIKeyID != "ak_actual" || len(got.Authority.Operations) != 1 {
		t.Fatal("provenance or ceiling lost")
	}
	for _, request := range [][2]string{{"POST", "/v1/sessions"}, {"GET", "/v1/sessions/foreign"}} {
		if _, _, err := signer.Verify(token, request[0], request[1]); err == nil {
			t.Fatal("request replay admitted")
		}
	}
	if _, err := signer.Mint(Principal{Workspace: p.Workspace, APIKeyID: p.APIKeyID}, "GET", "/v1/sessions", "req-a", time.Minute); err == nil {
		t.Fatal("mint upgraded partial principal")
	}
	var claims InternalPrincipalClaims
	if err := verifyCompactJSON(signer.publicKey, token, "tetral-internal-principal", &claims); err != nil {
		t.Fatal(err)
	}
	claims.Authority = Authority{}
	partial, err := signCompactJSON(private, "tetral-internal-principal", claims)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := signer.Verify(partial, "GET", "/v1/sessions"); err == nil {
		t.Fatal("signed missing authority admitted")
	}
	parts := strings.Split(token, ".")
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	payload = append([]byte(`{"workspace_id":"workspace-b",`), payload[1:]...)
	parts[1] = base64.RawURLEncoding.EncodeToString(payload)
	parts[2] = base64.RawURLEncoding.EncodeToString(ed25519.Sign(private, []byte(parts[0]+"."+parts[1])))
	if _, _, err := signer.Verify(strings.Join(parts, "."), "GET", "/v1/sessions"); err == nil {
		t.Fatal("signed duplicate security field admitted")
	}
	signer.now = func() time.Time { return time.Now().Add(2 * time.Minute) }
	if _, _, err := signer.Verify(token, "GET", "/v1/sessions"); err == nil {
		t.Fatal("expired admission admitted")
	}
}
