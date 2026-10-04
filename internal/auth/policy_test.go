package auth

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/database"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestDecodePolicyDocumentRejectsAmbiguousSecurityFields(t *testing.T) {
	for _, input := range []string{
		`{"identities":[{"id":"first","ID":"last"}]}`,
		`{"workspace_grants":[{"workspace_id":"first","WORKSPACE_ID":"last"}]}`,
		`{"federation_rules":[],"FEDERATION_RULES":[]}`,
		`{"unknown":true}`, `null`, `[]`,
	} {
		if _, err := DecodePolicyDocument([]byte(input)); err == nil {
			t.Fatal("ambiguous or unsupported admin document accepted")
		}
	}
	if _, err := DecodePolicyDocument([]byte(`{}`)); err != nil {
		t.Fatal(err)
	}
}

func policyFixture() PolicyDocument {
	return PolicyDocument{
		FederationRules: []FederationRule{{ID: "rule_policy", OrganizationID: "organization_policy", Issuer: "https://issuer.example.test", Audience: "tetral-engine", JWKSURL: "https://issuer.example.test/keys", Algorithm: "RS256", Enabled: true}},
		Identities:      []IdentityBinding{{ID: "identity_policy", OrganizationID: "organization_policy", Issuer: "https://issuer.example.test", Subject: "subject_policy", Kind: IdentityHuman, Enabled: true}},
		WorkspaceGrants: []WorkspaceGrant{{ID: "grant_policy", IdentityID: "identity_policy", WorkspaceID: workspace.DefaultID, Role: WorkspaceFullAccess, Enabled: true}},
	}
}

func TestAuthPolicyImportAtomicNoopAndTerminalGrants(t *testing.T) {
	db := storagetest.NewPostgreSQLAdminDB(t)
	store := NewPolicyStore(db)
	ctx := context.Background()
	d := policyFixture()
	apply := func(d PolicyDocument) []PolicyChange {
		t.Helper()
		changes, err := store.Apply(ctx, d)
		if err != nil {
			t.Fatal(err)
		}
		return changes
	}
	if n := len(apply(d)); n != 3 {
		t.Fatalf("initial changes=%d want3", n)
	}
	if n := len(apply(d)); n != 0 {
		t.Fatalf("identical import changes=%d", n)
	}
	if n := len(apply(PolicyDocument{})); n != 0 {
		t.Fatalf("empty import changes=%d", n)
	}
	g := d.WorkspaceGrants[0]
	g.Enabled = false
	changes := apply(PolicyDocument{WorkspaceGrants: []WorkspaceGrant{g}})
	if len(changes) != 1 || changes[0].Revision != 2 {
		t.Fatalf("grant-only disable revisions=%v", changes)
	}
	if n := len(apply(PolicyDocument{WorkspaceGrants: []WorkspaceGrant{g}})); n != 0 {
		t.Fatalf("grant-only noop changes=%d", n)
	}
	g.Enabled = true
	changes = apply(PolicyDocument{WorkspaceGrants: []WorkspaceGrant{g}})
	if len(changes) != 1 || changes[0].Revision != 3 {
		t.Fatalf("reenable revisions=%v", changes)
	}
	invalid := policyFixture()
	invalid.FederationRules[0].Audience = "changed-audience"
	invalid.WorkspaceGrants[0].WorkspaceID = "missing_workspace"
	if _, err := store.Apply(ctx, invalid); err == nil {
		t.Fatal("invalid multi-row policy accepted")
	}
	var audience string
	var revision int64
	if err := db.QueryRow(`SELECT audience,revision FROM auth_federation_rules WHERE id='rule_policy'`).Scan(&audience, &revision); err != nil {
		t.Fatal(err)
	}
	if audience != "tetral-engine" || revision != 1 {
		t.Fatal("invalid import partially changed rule")
	}
	replacement := g
	replacement.ID = "grant_policy_replacement"
	changes = apply(PolicyDocument{RevokeWorkspaceGrants: []string{g.ID}, WorkspaceGrants: []WorkspaceGrant{replacement}})
	if len(changes) != 2 {
		t.Fatalf("atomic revoke/regrant changes=%v", changes)
	}
	var revoked bool
	if err := db.QueryRow(`SELECT revoked_at IS NOT NULL AND NOT enabled FROM auth_workspace_grants WHERE id=$1`, g.ID).Scan(&revoked); err != nil {
		t.Fatal(err)
	}
	if !revoked {
		t.Fatal("old grant lacks terminal tombstone")
	}
	if _, err := store.Apply(ctx, PolicyDocument{WorkspaceGrants: []WorkspaceGrant{g}}); err == nil {
		t.Fatal("terminal grant resurrected")
	}
	if _, err := db.Exec(`UPDATE auth_workspace_grants SET enabled=true,revoked_at=NULL WHERE id=$1`, g.ID); err == nil {
		t.Fatal("SQL bypass resurrected terminal grant")
	}
	// Disable plus replacement is also atomic when the new ID sorts first.
	replacement.Enabled = false
	next := replacement
	next.ID = "aaa_policy_replacement"
	next.Enabled = true
	changes = apply(PolicyDocument{WorkspaceGrants: []WorkspaceGrant{next, replacement}})
	if len(changes) != 2 {
		t.Fatalf("disable/replacement changes=%v", changes)
	}

	changes = apply(PolicyDocument{RemoveFederationRules: []string{"rule_policy"}, RemoveIdentities: []string{"identity_policy"}})
	if len(changes) != 2 {
		t.Fatalf("explicit removals=%v", changes)
	}
	if n := len(apply(PolicyDocument{RemoveFederationRules: []string{"rule_policy"}, RemoveIdentities: []string{"identity_policy"}})); n != 0 {
		t.Fatalf("repeat removals changed %d roots", n)
	}
	if err := db.QueryRow(`SELECT revision FROM auth_federation_rules WHERE id='rule_policy' AND removed AND NOT enabled`).Scan(&revision); err != nil {
		t.Fatal(err)
	}
	if revision != 2 {
		t.Fatalf("removed root revision=%d", revision)
	}
	// Recreating a retained root can never restore an old security revision.
	changes = apply(PolicyDocument{FederationRules: d.FederationRules, Identities: d.Identities})
	for _, change := range changes {
		if change.Revision != 3 {
			t.Fatalf("root recreation restored revision: %v", change)
		}
	}
}

func TestAuthPolicyServingConnectionCannotAdminister(t *testing.T) {
	runtime, _ := storagetest.NewPostgreSQLDBWithAdmin(t)
	_, err := NewPolicyStore(runtime).Apply(context.Background(), policyFixture())
	var denied *PermissionError
	if !errors.As(err, &denied) {
		t.Fatalf("serving admin result=%T %v", err, err)
	}
}

func TestAuthPolicyLookupCatalogMatchesContract(t *testing.T) {
	db := storagetest.NewPostgreSQLAdminDB(t)
	contract, err := database.LoadPostgreSQL()
	if err != nil {
		t.Fatal(err)
	}
	normalize := func(s string) string { return strings.Join(strings.Fields(s), "") }
	for _, policy := range contract.SpecialPolicies {
		if policy.Name != "auth_lookup" {
			continue
		}
		var command, using, check string
		if err := db.QueryRow(`SELECT cmd,qual,with_check FROM pg_policies WHERE schemaname='public' AND tablename=$1 AND policyname=$2`, policy.Table, policy.Name).Scan(&command, &using, &check); err != nil {
			t.Fatal(err)
		}
		if command != policy.Command || normalize(using) != normalize(policy.Using) || normalize(check) != normalize(policy.Check) {
			t.Errorf("%s Auth lookup canonical mismatch: command=%q using=%q check=%q", policy.Table, command, using, check)
		}
	}
}
