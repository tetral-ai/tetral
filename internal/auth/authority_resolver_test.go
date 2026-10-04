package auth

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func authorityFixture(t *testing.T) (*sql.DB, *AuthorityResolver, *APIKeyStore, VerifiedAssertion) {
	t.Helper()
	db := storagetest.NewPostgreSQLAdminDB(t)
	d := policyFixture()
	if _, err := NewPolicyStore(db).Apply(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	runtime := storagetest.OpenWorkloadDB(t, db, "auth").DB
	return db, NewAuthorityResolver(runtime, workspace.DefaultID), NewAPIKeyStore(runtime), VerifiedAssertion{ruleID: d.FederationRules[0].ID, ruleRevision: 1, issuer: d.FederationRules[0].Issuer, subject: d.Identities[0].Subject, expiresAt: time.Now().Add(30 * time.Minute)}
}
func requireCredentialRejection(t *testing.T, err error) {
	t.Helper()
	var denied *AuthenticationError
	if !errors.As(err, &denied) {
		t.Fatalf("wanted credential rejection, got %T", err)
	}
}

func TestAuthorityResolverDerivedKeysPreserveCeilingAndDurableLineage(t *testing.T) {
	db, resolver, store, proof := authorityFixture(t)
	ctx := context.Background()
	issued, err := resolver.Issue(ctx, proof, ExchangeSelectors{})
	if err != nil {
		t.Fatal(err)
	}
	if issued.ExpiresIn != 600 {
		t.Fatalf("token maxTTL=%d", issued.ExpiresIn)
	}
	principal, err := resolver.AuthenticateBearer(ctx, issued.AccessToken)
	if err != nil {
		t.Fatal(err)
	}
	if principal.Credential.Kind != CredentialAccessToken || principal.APIKeyID != "" || principal.Identity == nil || principal.Identity.Kind != IdentityHuman {
		t.Fatal("direct bearer attribution incorrect")
	}
	first, err := store.CreateForPrincipal(ctx, principal, "first derived")
	if err != nil {
		t.Fatal(err)
	}
	firstPrincipal, err := resolver.AuthenticateKey(ctx, first.APIKey)
	if err != nil {
		t.Fatal(err)
	}
	if firstPrincipal.APIKeyID != first.ID || firstPrincipal.Authority.Kind != AuthorityIdentityGrant || firstPrincipal.Identity.ID != principal.Identity.ID {
		t.Fatal("derived key lost truthful actor or identity provenance")
	}
	second, err := store.CreateForPrincipal(ctx, firstPrincipal, "second derived")
	if err != nil {
		t.Fatal(err)
	}
	secondPrincipal, err := (&StoreAuthenticator{Store: store}).Authenticate(ctx, second.APIKey)
	if err != nil {
		t.Fatal(err)
	}
	if secondPrincipal.APIKeyID != second.ID || secondPrincipal.Authority.Kind != AuthorityIdentityGrant || !sameOperations(secondPrincipal.Authority.Operations, principal.Authority.Operations) {
		t.Fatal("second generation upgraded authority")
	}
	for _, key := range []*CreateAPIKeyResult{first, second} {
		var authority, rule, identity, grant, ws string
		var r, i, g, p int64
		if err := db.QueryRow(`SELECT authority_kind,federation_rule_id,identity_id,grant_id,workspace_id,rule_revision,identity_revision,grant_revision,role_version FROM api_keys WHERE id=$1`, key.ID).Scan(&authority, &rule, &identity, &grant, &ws, &r, &i, &g, &p); err != nil {
			t.Fatal(err)
		}
		if authority != AuthorityIdentityGrant || rule != principal.Authority.RuleID || identity != principal.Identity.ID || grant != principal.Authority.GrantID || ws != string(principal.Workspace.ID) || r != 1 || i != 1 || g != 1 || p != PolicyVersion {
			t.Fatal("durable lineage diverged")
		}
	}
	// Individual issuer deletion does not recursively revoke another issued key.
	if err := store.RevokeForWorkspace(ctx, workspace.DefaultID, first.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.AuthenticateKey(ctx, first.APIKey); err == nil {
		t.Fatal("revoked key admitted")
	}
	if _, err := resolver.AuthenticateKey(ctx, second.APIKey); err != nil {
		t.Fatal("individual revoke recursively revoked child")
	}
	// Physically removing an expired parent token leaves durable key authority.
	if _, err := db.Exec(`UPDATE auth_access_tokens SET created_at=clock_timestamp()-interval '26 hours',expires_at=clock_timestamp()-interval '25 hours' WHERE id=$1`, principal.Credential.ID); err != nil {
		t.Fatal(err)
	}
	result, err := resolver.PruneTokens(ctx, 1000)
	if err != nil || result.DeletedCount != 1 {
		t.Fatalf("parent prune result=%v errtype=%T", result, err)
	}
	if _, err := resolver.AuthenticateKey(ctx, second.APIKey); err != nil {
		t.Fatal("parent physical purge revoked derived key")
	}
	if _, err := NewPolicyStore(db).Apply(ctx, PolicyDocument{RevokeWorkspaceGrants: []string{principal.Authority.GrantID}}); err != nil {
		t.Fatal(err)
	}
	_, err = resolver.AuthenticateKey(ctx, second.APIKey)
	requireCredentialRejection(t, err)
}

func TestAuthorityResolverRestrictedKeyAndImmutableProvenance(t *testing.T) {
	db, resolver, store, proof := authorityFixture(t)
	ctx := context.Background()
	issued, err := resolver.Issue(ctx, proof, ExchangeSelectors{})
	if err != nil {
		t.Fatal(err)
	}
	p, err := resolver.AuthenticateBearer(ctx, issued.AccessToken)
	if err != nil {
		t.Fatal(err)
	}
	rawFixture, err := GenerateAPIKey()
	if err != nil {
		t.Fatal(err)
	}
	key := &CreateAPIKeyResult{APIKey: rawFixture, APIKeyMetadata: APIKeyMetadata{ID: "ak_restricted_fixture"}}
	// Provision only the restricted fixture ceiling. Authentication and every
	// subsequent public gate/key issuance use unchanged production code.
	if _, err := db.Exec(`INSERT INTO api_keys(id,workspace_id,name,key_prefix,key_digest,key_kind,authority_kind,federation_rule_id,identity_id,grant_id,rule_revision,identity_revision,grant_revision,role_version,issuance_operations,parent_credential_id,created_at) VALUES($1,'default','restricted fixture','fixture',$2,'standard','identity_grant',$3,$4,$5,1,1,1,1,'["api_keys.create","sessions.read"]'::jsonb,$6,clock_timestamp())`, key.ID, DigestAPIKey(rawFixture), p.Authority.RuleID, p.Authority.IdentityID, p.Authority.GrantID, p.Credential.ID); err != nil {
		t.Fatal(err)
	}

	restricted, err := resolver.AuthenticateKey(ctx, key.APIKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := Authorize(restricted, OperationSessionsCreate, ResourceReference{WorkspaceID: workspace.DefaultID, Type: "workspace"}); err == nil {
		t.Fatal("restricted fixture granted denied action")
	}
	child, err := store.CreateForPrincipal(ctx, restricted, "restricted child")
	if err != nil {
		t.Fatal(err)
	}
	childP, err := resolver.AuthenticateKey(ctx, child.APIKey)
	if err != nil {
		t.Fatal(err)
	}
	if !sameOperations(childP.Authority.Operations, restricted.Authority.Operations) || childP.Authority.Kind != AuthorityIdentityGrant {
		t.Fatal("restricted child escaped ceiling")
	}
	for _, statement := range []string{
		`UPDATE api_keys SET authority_kind='independent_key',federation_rule_id=NULL,identity_id=NULL,grant_id=NULL,rule_revision=NULL,identity_revision=NULL,grant_revision=NULL,role_version=NULL,issuance_operations=NULL,parent_credential_id=NULL WHERE id=$1`,
		`UPDATE api_keys SET issuance_operations='["sessions.create"]'::jsonb WHERE id=$1`,
		`UPDATE api_keys SET grant_revision=grant_revision+1 WHERE id=$1`,
	} {
		if _, err := db.Exec(statement, child.ID); err == nil {
			t.Fatal("immutable key provenance edited")
		}
	}
	if _, err := db.Exec(`INSERT INTO api_keys(id,workspace_id,name,key_prefix,key_digest,key_kind,created_at) VALUES('key_without_provenance','default','invalid','invalid',decode(repeat('aa',32),'hex'),'standard',clock_timestamp())`); err == nil {
		t.Fatal("missing provenance upgraded to independent")
	}
}

func TestAuthorityResolverExactProofRevisionAndIntegerExpiry(t *testing.T) {
	db, resolver, _, proof := authorityFixture(t)
	ctx := context.Background()
	for _, remaining := range []time.Duration{120 * time.Second, 120*time.Second + 500*time.Millisecond} {
		p := proof
		p.expiresAt = time.Now().Add(remaining)
		_, err := resolver.Issue(ctx, p, ExchangeSelectors{})
		requireCredentialRejection(t, err)
	}
	p := proof
	p.expiresAt = time.Now().Add(180 * time.Second)
	token, err := resolver.Issue(ctx, p, ExchangeSelectors{})
	if err != nil {
		t.Fatal(err)
	}
	if token.ExpiresIn <= 120 || token.ExpiresIn > 180 {
		t.Fatalf("advertised integer TTL=%d", token.ExpiresIn)
	}
	d := policyFixture()
	d.FederationRules[0].Audience = "new audience"
	if _, err := NewPolicyStore(db).Apply(ctx, PolicyDocument{FederationRules: d.FederationRules}); err != nil {
		t.Fatal(err)
	}
	_, err = resolver.Issue(ctx, proof, ExchangeSelectors{})
	requireCredentialRejection(t, err)
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM auth_access_tokens`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("stale or too short proof inserted token count=%d", count)
	}
}
