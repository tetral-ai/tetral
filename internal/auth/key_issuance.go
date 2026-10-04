package auth

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/id"
)

func sameOperations(a, b []Operation) bool {
	if len(a) != len(b) {
		return false
	}
	for _, op := range a {
		if !slices.Contains(b, op) {
			return false
		}
	}
	return true
}

// CreateForPrincipal is the public issuance path. It preserves the exact
// admitted root revisions and current ceiling; it never stamps stale authority
// with newer database revisions or derives independent authority from identity.
func (s *APIKeyStore) CreateForPrincipal(ctx context.Context, p Principal, name string) (*CreateAPIKeyResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := Authorize(p, OperationAPIKeysCreate, ResourceReference{WorkspaceID: p.Workspace.ID, Type: "workspace"}); err != nil {
		return nil, err
	}
	trimmed := strings.TrimSpace(name)
	if trimmed == "" || runeCount(trimmed) > keyMetadataNameMaxRunes {
		return nil, &ValidationError{Message: fmt.Sprintf("name must contain 1 to %d Unicode code points", keyMetadataNameMaxRunes)}
	}
	if p.Authority.Kind == AuthorityIndependentKey && !sameOperations(p.Authority.Operations, RegisteredOperations()) {
		return nil, &PermissionError{}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if p.Authority.Kind == AuthorityIdentityGrant {
		kind, err := lockCurrentAuthority(ctx, tx, p.Authority)
		if err != nil {
			return nil, err
		}
		if p.Identity == nil || kind != p.Identity.Kind {
			return nil, authRejected()
		}
	}
	if err = setAuthWorkspace(ctx, tx, p.Workspace.ID); err != nil {
		return nil, err
	}
	var issuer credentialSnapshot
	if p.Credential.Kind == CredentialAccessToken {
		issuer, err = readTokenSnapshot(tx.QueryRowContext(ctx, `SELECT `+tokenAuthorityColumns+` FROM auth_access_tokens WHERE id=$1 AND workspace_id=$2 FOR SHARE`, p.Credential.ID, string(p.Workspace.ID)))
	} else {
		issuer, err = readKeySnapshot(tx.QueryRowContext(ctx, `SELECT `+keyAuthorityColumns+` FROM api_keys WHERE id=$1 AND workspace_id=$2 FOR SHARE`, p.Credential.ID, string(p.Workspace.ID)))
	}
	if err != nil {
		return nil, err
	}
	// Expiry predicates in a locking SELECT may run before a lock wait. The
	// credential is now locked; evaluate its current validity with fresh DB time.
	table := "api_keys"
	if p.Credential.Kind == CredentialAccessToken {
		table = "auth_access_tokens"
	}
	var valid bool
	err = tx.QueryRowContext(ctx, `SELECT revoked_at IS NULL AND (expires_at IS NULL OR expires_at>clock_timestamp()) FROM `+table+` WHERE id=$1 AND workspace_id=$2`, p.Credential.ID, string(p.Workspace.ID)).Scan(&valid)
	if err == sql.ErrNoRows || err == nil && !valid {
		return nil, authRejected()
	}
	if err != nil {
		return nil, err
	}
	if issuer.authority.Kind != p.Authority.Kind || issuer.authority.RuleID != p.Authority.RuleID || issuer.authority.IdentityID != p.Authority.IdentityID || issuer.authority.GrantID != p.Authority.GrantID || issuer.authority.RuleRevision != p.Authority.RuleRevision || issuer.authority.IdentityRevision != p.Authority.IdentityRevision || issuer.authority.GrantRevision != p.Authority.GrantRevision || issuer.authority.PolicyVersion != p.Authority.PolicyVersion || !sameOperations(issuer.authority.Operations, p.Authority.Operations) {
		return nil, authRejected()
	}
	raw, err := GenerateAPIKey()
	if err != nil {
		return nil, err
	}
	keyID := id.New("ak_")
	var now time.Time
	if err = tx.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return nil, err
	}
	if p.Authority.Kind == AuthorityIndependentKey {
		_, err = tx.ExecContext(ctx, `INSERT INTO api_keys(id,workspace_id,name,key_prefix,key_digest,key_kind,authority_kind,created_at) VALUES($1,$2,$3,$4,$5,'standard','independent_key',$6)`, keyID, string(p.Workspace.ID), trimmed, KeyPrefixFor(raw), DigestAPIKey(raw), now)
	} else {
		// Current role and the immutable issuer ceiling intersect. The only current
		// production role contains all operations; retaining this intersection makes
		// restricted fixtures exercise exactly the same issuance boundary.
		effective := []Operation{}
		currentOperations, roleErr := RoleOperations(WorkspaceFullAccess)
		if roleErr != nil {
			return nil, roleErr
		}
		for _, op := range currentOperations {
			if slices.Contains(p.Authority.Operations, op) {
				effective = append(effective, op)
			}
		}
		encoded, encodeErr := json.Marshal(effective)
		if encodeErr != nil {
			return nil, encodeErr
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO api_keys(id,workspace_id,name,key_prefix,key_digest,key_kind,authority_kind,federation_rule_id,identity_id,grant_id,rule_revision,identity_revision,grant_revision,role_version,issuance_operations,parent_credential_id,created_at) VALUES($1,$2,$3,$4,$5,'standard','identity_grant',$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`, keyID, string(p.Workspace.ID), trimmed, KeyPrefixFor(raw), DigestAPIKey(raw), p.Authority.RuleID, p.Authority.IdentityID, p.Authority.GrantID, p.Authority.RuleRevision, p.Authority.IdentityRevision, p.Authority.GrantRevision, p.Authority.PolicyVersion, encoded, p.Credential.ID, now)
	}
	if err != nil {
		return nil, mapPostgreSQLError(err)
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return &CreateAPIKeyResult{APIKey: raw, APIKeyMetadata: APIKeyMetadata{ID: keyID, Type: "api_key", WorkspaceID: string(p.Workspace.ID), Name: trimmed, KeyPrefix: KeyPrefixFor(raw), KeyKind: KindStandard, CreatedAt: now.UTC().Format(time.RFC3339)}}, nil
}
