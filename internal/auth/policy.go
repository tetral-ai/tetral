package auth

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"sort"
	"time"

	"github.com/tetral-ai/tetral/internal/workspace"
)

// PolicyDocument is an explicit administrative change set. Omission preserves
// existing rows; removals and revocations retain their durable revision facts.
type PolicyDocument struct {
	FederationRules       []FederationRule  `json:"federation_rules,omitempty"`
	Identities            []IdentityBinding `json:"identities,omitempty"`
	WorkspaceGrants       []WorkspaceGrant  `json:"workspace_grants,omitempty"`
	RemoveFederationRules []string          `json:"remove_federation_rules,omitempty"`
	RemoveIdentities      []string          `json:"remove_identities,omitempty"`
	RevokeWorkspaceGrants []string          `json:"revoke_workspace_grants,omitempty"`
	RevokeAccessTokens    []TokenReference  `json:"revoke_access_tokens,omitempty"`
}
type IdentityBinding struct {
	ID               string `json:"id"`
	OrganizationID   string `json:"organization_id"`
	Issuer           string `json:"issuer"`
	Subject          string `json:"subject"`
	Kind             string `json:"kind"`
	ServiceAccountID string `json:"service_account_id,omitempty"`
	Enabled          bool   `json:"enabled"`
}
type WorkspaceGrant struct {
	ID          string       `json:"id"`
	IdentityID  string       `json:"identity_id"`
	WorkspaceID workspace.ID `json:"workspace_id"`
	Role        string       `json:"role"`
	Enabled     bool         `json:"enabled"`
}
type TokenReference struct {
	ID          string       `json:"id"`
	WorkspaceID workspace.ID `json:"workspace_id"`
}

// PolicyChange contains only sanitized administrative identifiers and revisions.
type PolicyChange struct {
	Kind     string `json:"kind"`
	ID       string `json:"id"`
	Revision int64  `json:"revision,omitempty"`
}
type PolicyStore struct{ db *sql.DB }

func NewPolicyStore(adminDB *sql.DB) *PolicyStore { return &PolicyStore{db: adminDB} }

func validatePolicyDocument(d PolicyDocument) error {
	invalid := func() error { return &ValidationError{Message: "invalid auth policy document"} }
	total := len(d.FederationRules) + len(d.Identities) + len(d.WorkspaceGrants) + len(d.RemoveFederationRules) + len(d.RemoveIdentities) + len(d.RevokeWorkspaceGrants) + len(d.RevokeAccessTokens)
	if total > 1000 {
		return invalid()
	}
	seen := map[string]bool{}
	subjects := map[[3]string]bool{}
	grantScopes := map[[2]string]bool{}
	add := func(kind, id string) bool {
		key := kind + ":" + id
		if !boundedIdentity(id) || seen[key] {
			return false
		}
		seen[key] = true
		return true
	}
	for _, r := range d.FederationRules {
		if !add("rule", r.ID) || r.Revision != 0 || ValidateFederationRule(r) != nil {
			return invalid()
		}
	}
	for _, i := range d.Identities {
		tuple := [3]string{i.OrganizationID, i.Issuer, i.Subject}
		if subjects[tuple] {
			return invalid()
		}
		subjects[tuple] = true
		if !add("identity", i.ID) || !boundedIdentity(i.OrganizationID) || !boundedIdentity(i.Subject) {
			return invalid()
		}
		if _, err := secureURL(i.Issuer); err != nil {
			return invalid()
		}
		if i.Kind == IdentityHuman {
			if i.ServiceAccountID != "" {
				return invalid()
			}
		} else if i.Kind != IdentityService || !boundedIdentity(i.ServiceAccountID) {
			return invalid()
		}
	}
	for _, g := range d.WorkspaceGrants {
		if g.Enabled {
			scope := [2]string{g.IdentityID, string(g.WorkspaceID)}
			if grantScopes[scope] {
				return invalid()
			}
			grantScopes[scope] = true
		}
		if !add("grant", g.ID) || !boundedIdentity(g.IdentityID) || !boundedIdentity(string(g.WorkspaceID)) || g.Role != WorkspaceFullAccess {
			return invalid()
		}
	}
	for _, id := range d.RemoveFederationRules {
		if !add("rule", id) {
			return invalid()
		}
	}
	for _, id := range d.RemoveIdentities {
		if !add("identity", id) {
			return invalid()
		}
	}
	for _, id := range d.RevokeWorkspaceGrants {
		if !add("grant", id) {
			return invalid()
		}
	}
	for _, t := range d.RevokeAccessTokens {
		if !add("token", t.ID) || !boundedIdentity(string(t.WorkspaceID)) {
			return invalid()
		}
	}
	return nil
}

// Apply validates the whole change set before writes, serializes administrative
// imports, and commits every change in one bounded transaction. It requires the
// installation owner connection; serving credentials cannot administer policy.
func (s *PolicyStore) Apply(ctx context.Context, document PolicyDocument) ([]PolicyChange, error) {
	if err := validatePolicyDocument(document); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	var owner bool
	err = tx.QueryRowContext(ctx, `SELECT r.rolsuper OR current_user=pg_get_userbyid(c.relowner) FROM pg_roles r,pg_class c WHERE r.rolname=current_user AND c.oid='public.auth_federation_rules'::regclass`).Scan(&owner)
	if err != nil {
		return nil, err
	}
	if !owner {
		return nil, &PermissionError{}
	}
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('tetral.auth.policy',0)),set_config('tetral.auth_lookup','true',true)`); err != nil {
		return nil, err
	}
	if err = lockPolicyRoots(ctx, tx, document); err != nil {
		return nil, err
	}
	if err = validatePolicyReferences(ctx, tx, document); err != nil {
		return nil, err
	}
	// Stable root ordering matches admission: rules, identities, then grants.
	rules := append([]FederationRule(nil), document.FederationRules...)
	sort.Slice(rules, func(i, j int) bool { return rules[i].ID < rules[j].ID })
	identities := append([]IdentityBinding(nil), document.Identities...)
	sort.Slice(identities, func(i, j int) bool { return identities[i].ID < identities[j].ID })
	grants := append([]WorkspaceGrant(nil), document.WorkspaceGrants...)
	sort.Slice(grants, func(i, j int) bool {
		if grants[i].Enabled != grants[j].Enabled {
			return !grants[i].Enabled
		}
		return grants[i].ID < grants[j].ID
	})
	changes := []PolicyChange{}
	record := func(kind, id string, query string, args ...any) error {
		var revision int64
		err := tx.QueryRowContext(ctx, query, args...).Scan(&revision)
		if err == sql.ErrNoRows {
			return nil
		}
		if err != nil {
			return err
		}
		changes = append(changes, PolicyChange{Kind: kind, ID: id, Revision: revision})
		return nil
	}
	remove := func(kind, table string, ids []string) error {
		sorted := append([]string(nil), ids...)
		sort.Strings(sorted)
		for _, id := range sorted {
			query := `UPDATE ` + table + ` SET enabled=false,removed=true,revision=revision+1,updated_at=clock_timestamp() WHERE id=$1 AND NOT removed RETURNING revision`
			if kind == "workspace_grant" {
				query = `UPDATE auth_workspace_grants SET enabled=false,revoked_at=clock_timestamp(),revision=revision+1,updated_at=clock_timestamp() WHERE id=$1 AND revoked_at IS NULL RETURNING revision`
			}
			if err := record(kind, id, query, id); err != nil {
				return err
			}
		}
		return nil
	}

	for _, r := range rules {
		config := r
		config.Enabled = false
		config.Revision = 0
		raw, err := json.Marshal(config)
		if err != nil {
			return nil, err
		}
		err = record("federation_rule", r.ID, `INSERT INTO auth_federation_rules(id,organization_id,issuer,audience,security_config,enabled) VALUES($1,$2,$3,$4,$5,$6)
   ON CONFLICT(id) DO UPDATE SET organization_id=EXCLUDED.organization_id,issuer=EXCLUDED.issuer,audience=EXCLUDED.audience,security_config=EXCLUDED.security_config,enabled=EXCLUDED.enabled,removed=false,revision=auth_federation_rules.revision+1,updated_at=clock_timestamp()
   WHERE (auth_federation_rules.organization_id,auth_federation_rules.issuer,auth_federation_rules.audience,auth_federation_rules.security_config,auth_federation_rules.enabled,auth_federation_rules.removed) IS DISTINCT FROM (EXCLUDED.organization_id,EXCLUDED.issuer,EXCLUDED.audience,EXCLUDED.security_config,EXCLUDED.enabled,false) RETURNING revision`, r.ID, r.OrganizationID, r.Issuer, r.Audience, raw, r.Enabled)
		if err != nil {
			return nil, err
		}
	}
	if err = remove("federation_rule", "auth_federation_rules", document.RemoveFederationRules); err != nil {
		return nil, err
	}
	for _, i := range identities {
		err = record("identity", i.ID, `INSERT INTO auth_identities(id,organization_id,issuer,subject,identity_kind,service_account_id,enabled) VALUES($1,$2,$3,$4,$5,NULLIF($6,''),$7)
   ON CONFLICT(id) DO UPDATE SET enabled=EXCLUDED.enabled,removed=false,revision=auth_identities.revision+1,updated_at=clock_timestamp()
   WHERE (auth_identities.enabled,auth_identities.removed) IS DISTINCT FROM (EXCLUDED.enabled,false) RETURNING revision`, i.ID, i.OrganizationID, i.Issuer, i.Subject, i.Kind, i.ServiceAccountID, i.Enabled)
		if err != nil {
			return nil, err
		}
	}
	if err = remove("identity", "auth_identities", document.RemoveIdentities); err != nil {
		return nil, err
	}
	if err = remove("workspace_grant", "auth_workspace_grants", document.RevokeWorkspaceGrants); err != nil {
		return nil, err
	}
	for _, g := range grants {
		err = record("workspace_grant", g.ID, `INSERT INTO auth_workspace_grants(id,identity_id,workspace_id,role,role_version,enabled) VALUES($1,$2,$3,$4,$5,$6)
   ON CONFLICT(id) DO UPDATE SET role=EXCLUDED.role,role_version=EXCLUDED.role_version,enabled=EXCLUDED.enabled,revision=auth_workspace_grants.revision+1,updated_at=clock_timestamp()
   WHERE (auth_workspace_grants.role,auth_workspace_grants.role_version,auth_workspace_grants.enabled) IS DISTINCT FROM (EXCLUDED.role,EXCLUDED.role_version,EXCLUDED.enabled) RETURNING revision`, g.ID, g.IdentityID, string(g.WorkspaceID), g.Role, PolicyVersion, g.Enabled)
		if err != nil {
			return nil, err
		}
	}
	for _, t := range document.RevokeAccessTokens {
		res, err := tx.ExecContext(ctx, `UPDATE auth_access_tokens SET revoked_at=clock_timestamp() WHERE id=$1 AND workspace_id=$2 AND revoked_at IS NULL`, t.ID, string(t.WorkspaceID))
		if err != nil {
			return nil, err
		}
		n, _ := res.RowsAffected()
		if n > 0 {
			changes = append(changes, PolicyChange{Kind: "access_token", ID: t.ID})
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return changes, nil
}

// Lock every existing affected root before validating or writing. A mixed
// update/removal import cannot acquire a rule lock after holding a grant lock.
func lockPolicyRoots(ctx context.Context, tx *sql.Tx, d PolicyDocument) error {
	rules := append([]string(nil), d.RemoveFederationRules...)
	for _, r := range d.FederationRules {
		rules = append(rules, r.ID)
	}
	identities := append([]string(nil), d.RemoveIdentities...)
	for _, i := range d.Identities {
		identities = append(identities, i.ID)
	}
	grants := append([]string(nil), d.RevokeWorkspaceGrants...)
	for _, g := range d.WorkspaceGrants {
		grants = append(grants, g.ID)
	}
	for _, group := range []struct {
		table string
		ids   []string
	}{{"auth_federation_rules", rules}, {"auth_identities", identities}, {"auth_workspace_grants", grants}} {
		if len(group.ids) == 0 {
			continue
		}
		encoded, err := json.Marshal(group.ids)
		if err != nil {
			return err
		}
		//nolint:gosec // G202: table is one of the three fixed policy root constants above.
		rows, err := tx.QueryContext(ctx, `SELECT id FROM `+group.table+` WHERE id IN (SELECT jsonb_array_elements_text($1::jsonb)) ORDER BY id FOR UPDATE`, encoded)
		if err != nil {
			return err
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				_ = rows.Close()
				return err
			}
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func validatePolicyReferences(ctx context.Context, tx *sql.Tx, d PolicyDocument) error {
	invalid := func() error { return &ValidationError{Message: "invalid auth policy references"} }
	suppliedIdentities := map[string]bool{}
	retiringGrants := map[string]bool{}
	for _, id := range d.RevokeWorkspaceGrants {
		retiringGrants[id] = true
	}
	for _, g := range d.WorkspaceGrants {
		if !g.Enabled {
			retiringGrants[g.ID] = true
		}
	}
	for _, i := range d.Identities {
		suppliedIdentities[i.ID] = true
		var current IdentityBinding
		err := tx.QueryRowContext(ctx, `SELECT organization_id,issuer,subject,identity_kind,COALESCE(service_account_id,'') FROM auth_identities WHERE id=$1`, i.ID).Scan(&current.OrganizationID, &current.Issuer, &current.Subject, &current.Kind, &current.ServiceAccountID)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		if err == nil && (current.OrganizationID != i.OrganizationID || current.Issuer != i.Issuer || current.Subject != i.Subject || current.Kind != i.Kind || current.ServiceAccountID != i.ServiceAccountID) {
			return invalid()
		}
		var existing string
		err = tx.QueryRowContext(ctx, `SELECT id FROM auth_identities WHERE organization_id=$1 AND issuer=$2 AND subject=$3`, i.OrganizationID, i.Issuer, i.Subject).Scan(&existing)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		if err == nil && existing != i.ID {
			return invalid()
		}
	}
	for _, g := range d.WorkspaceGrants {
		var exists bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM workspaces WHERE id=$1)`, string(g.WorkspaceID)).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return invalid()
		}
		if !suppliedIdentities[g.IdentityID] {
			if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM auth_identities WHERE id=$1 AND NOT removed)`, g.IdentityID).Scan(&exists); err != nil {
				return err
			}
			if !exists {
				return invalid()
			}
		}
		if g.Enabled {
			var active string
			err := tx.QueryRowContext(ctx, `SELECT id FROM auth_workspace_grants WHERE identity_id=$1 AND workspace_id=$2 AND enabled AND revoked_at IS NULL AND id<>$3`, g.IdentityID, string(g.WorkspaceID), g.ID).Scan(&active)
			if err != nil && err != sql.ErrNoRows {
				return err
			}
			if err == nil && !retiringGrants[active] {
				return invalid()
			}
		}

		var identity, ws string
		var revoked sql.NullTime
		err := tx.QueryRowContext(ctx, `SELECT identity_id,workspace_id,revoked_at FROM auth_workspace_grants WHERE id=$1`, g.ID).Scan(&identity, &ws, &revoked)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		if err == nil && (identity != g.IdentityID || ws != string(g.WorkspaceID) || revoked.Valid) {
			return invalid()
		}
	}
	for _, op := range []struct {
		table string
		ids   []string
	}{{"auth_federation_rules", d.RemoveFederationRules}, {"auth_identities", d.RemoveIdentities}, {"auth_workspace_grants", d.RevokeWorkspaceGrants}} {
		for _, id := range op.ids {
			var exists bool
			if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM `+op.table+` WHERE id=$1)`, id).Scan(&exists); err != nil {
				return err
			}
			if !exists {
				return invalid()
			}
		}
	}
	for _, t := range d.RevokeAccessTokens {
		var exists bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM auth_access_tokens WHERE id=$1 AND workspace_id=$2)`, t.ID, string(t.WorkspaceID)).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return invalid()
		}
	}
	return nil
}

// DecodePolicyDocument bounds administrative input independently of the serving
// token endpoint, and rejects unknown, duplicate and case-equivalent fields.
func DecodePolicyDocument(input []byte) (PolicyDocument, error) {
	var d PolicyDocument
	if len(input) > 1024*1024 {
		return d, &ValidationError{Message: "auth policy document exceeds limit"}
	}
	input = bytes.TrimSpace(input)
	if len(input) == 0 || input[0] != '{' {
		return d, &ValidationError{Message: "invalid auth policy document"}
	}
	if err := DecodeStrictJSON(input, &d); err != nil {
		return d, &ValidationError{Message: "invalid auth policy document"}
	}
	if err := validatePolicyDocument(d); err != nil {
		return d, err
	}
	return d, nil
}
