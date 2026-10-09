package auth

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// CredentialRequest contains normalized request facts from the transport owner.
// Selected X-Api-Key bytes retain Header.Get semantics and take precedence.
type CredentialRequest struct {
	Method, Path, APIKey, Authorization string
	AuthorizationValues                 []string
}
type RequestAuthenticator struct{ Resolver *AuthorityResolver }

func (a *RequestAuthenticator) AuthenticateRequest(ctx context.Context, r CredentialRequest) (Principal, error) {
	if a == nil || a.Resolver == nil {
		err := &UnavailableError{}
		RecordDecision(ctx, "admission", err, AuditEvent{})
		return Principal{}, err
	}
	if r.APIKey != "" {
		p, err := a.Resolver.AuthenticateKey(ctx, r.APIKey)
		return p, authenticationDependencyError(err)
	}
	if len(r.AuthorizationValues) > 1 {
		RecordDecision(ctx, "admission", authRejected(), AuditEvent{})
		return Principal{}, authRejected()
	}
	if len(r.AuthorizationValues) == 1 {
		r.Authorization = r.AuthorizationValues[0]
	}
	scheme, token, ok := strings.Cut(r.Authorization, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" || strings.ContainsAny(token, " \t\r\n") {
		err := &AuthenticationError{Message: "missing or invalid credentials"}
		RecordDecision(ctx, "admission", err, AuditEvent{})
		return Principal{}, err
	}
	p, err := a.Resolver.AuthenticateBearer(ctx, token)
	return p, authenticationDependencyError(err)
}

type AuthorityResolver struct {
	db               *sql.DB
	defaultWorkspace workspace.ID
	usage            *APIKeyUsageRecorder
}

// NewAuthorityResolver borrows Auth's pool. usage receives one observation per
// committed API-key admission; a nil recorder records no usage.
func NewAuthorityResolver(db *sql.DB, defaultWorkspace workspace.ID, usage *APIKeyUsageRecorder) *AuthorityResolver {
	return &AuthorityResolver{db: db, defaultWorkspace: defaultWorkspace, usage: usage}
}

type credentialSnapshot struct {
	id          string
	workspaceID workspace.ID
	authority   Authority
	expires     sql.NullTime
}

func authRejected() error { return &AuthenticationError{Message: "invalid credentials"} }
func setAuthWorkspace(ctx context.Context, tx *sql.Tx, ws workspace.ID) error {
	_, err := tx.ExecContext(ctx, `SELECT set_config('tetral.workspace_id',$1,true)`, string(ws))
	return err
}
func readKeySnapshot(row *sql.Row) (credentialSnapshot, error) {
	var c credentialSnapshot
	var rule, identity, grant sql.NullString
	var r, i, g, p sql.NullInt64
	var ops []byte
	err := row.Scan(&c.id, &c.workspaceID, &c.authority.Kind, &rule, &identity, &grant, &r, &i, &g, &p, &ops, &c.expires)
	if err == sql.ErrNoRows {
		return c, authRejected()
	}
	if err != nil {
		return c, err
	}
	c.authority.RuleID = rule.String
	c.authority.IdentityID = identity.String
	c.authority.GrantID = grant.String
	c.authority.RuleRevision = r.Int64
	c.authority.IdentityRevision = i.Int64
	c.authority.GrantRevision = g.Int64
	c.authority.PolicyVersion = p.Int64
	c.authority.Scope = ResourceReference{WorkspaceID: c.workspaceID, Type: "workspace"}
	if c.authority.Kind == AuthorityIndependentKey {
		// The schema keeps independent rows free of lineage columns.
		c.authority = IndependentKeyPrincipal(workspace.Workspace{ID: c.workspaceID}, c.id).Authority
	} else if json.Unmarshal(ops, &c.authority.Operations) != nil {
		return c, authRejected()
	}
	return c, nil
}

const keyAuthorityColumns = `id,workspace_id,authority_kind,federation_rule_id,identity_id,grant_id,rule_revision,identity_revision,grant_revision,role_version,issuance_operations,expires_at`

func readTokenSnapshot(row *sql.Row) (credentialSnapshot, error) {
	var c credentialSnapshot
	err := row.Scan(&c.id, &c.workspaceID, &c.authority.RuleID, &c.authority.IdentityID, &c.authority.GrantID, &c.authority.RuleRevision, &c.authority.IdentityRevision, &c.authority.GrantRevision, &c.authority.PolicyVersion, &c.expires)
	if err == sql.ErrNoRows {
		return c, authRejected()
	}
	if err != nil {
		return c, err
	}
	c.authority.Kind = AuthorityIdentityGrant
	c.authority.Operations = RegisteredOperations()
	c.authority.Scope = ResourceReference{WorkspaceID: c.workspaceID, Type: "workspace"}
	return c, nil
}

//nolint:gosec // G101: fixed credential metadata column names, not credential material.
const tokenAuthorityColumns = `id,workspace_id,federation_rule_id,identity_id,grant_id,rule_revision,identity_revision,grant_revision,role_version,expires_at`

func lockCurrentAuthority(ctx context.Context, tx *sql.Tx, a Authority) (string, error) {
	var r, i, g, p int64
	var kind string
	err := tx.QueryRowContext(ctx, `SELECT rule_revision,identity_revision,grant_revision,role_version,identity_kind FROM public.tetral_auth_lock_authority($1,$2,$3,$4)`, a.RuleID, a.IdentityID, a.GrantID, string(a.Scope.WorkspaceID)).Scan(&r, &i, &g, &p, &kind)
	if err == sql.ErrNoRows {
		return "", authRejected()
	}
	if err != nil {
		return "", err
	}
	if r != a.RuleRevision || i != a.IdentityRevision || g != a.GrantRevision || p != a.PolicyVersion || p != PolicyVersion {
		return "", authRejected()
	}
	return kind, nil
}
func (s *AuthorityResolver) AuthenticateKey(ctx context.Context, raw string) (Principal, error) {
	return s.authenticate(ctx, raw, false)
}
func (s *AuthorityResolver) AuthenticateBearer(ctx context.Context, raw string) (Principal, error) {
	return s.authenticate(ctx, raw, true)
}
func (s *AuthorityResolver) authenticate(ctx context.Context, raw string, bearer bool) (principal Principal, resultErr error) {
	facts := AuditEvent{}
	defer func() { RecordDecision(ctx, "admission", resultErr, facts) }()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if raw == "" || len(raw) > 4096 {
		return Principal{}, authRejected()
	}
	digest := DigestAPIKey(raw)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Principal{}, err
	}
	defer func() { _ = tx.Rollback() }()
	var c credentialSnapshot
	if bearer {
		c, err = readTokenSnapshot(tx.QueryRowContext(ctx, `SELECT `+tokenAuthorityColumns+` FROM public.tetral_auth_lookup_token($1)`, digest))
	} else {
		c, err = readKeySnapshot(tx.QueryRowContext(ctx, `SELECT `+keyAuthorityColumns+` FROM public.tetral_auth_lookup_key($1)`, digest))
	}
	if err != nil {
		return Principal{}, err
	}
	kind := ""
	if c.authority.Kind == AuthorityIdentityGrant {
		kind, err = lockCurrentAuthority(ctx, tx, c.authority)
		if err != nil {
			return Principal{}, err
		}
	} else if c.authority.Kind != AuthorityIndependentKey || bearer {
		return Principal{}, authRejected()
	}
	facts = AuditEvent{IdentityKind: kind, RuleRevision: c.authority.RuleRevision, IdentityRevision: c.authority.IdentityRevision, GrantRevision: c.authority.GrantRevision}
	if err = setAuthWorkspace(ctx, tx, c.workspaceID); err != nil {
		return Principal{}, err
	}
	// Recheck the credential after root locks, under its workspace. The share
	// lock admits concurrent requests of one credential together, while an
	// individual revoke or rotation either commits first and is observed after
	// the wait, or waits for this admission to end; token pruning skips the
	// locked row until a later pass. Admission writes nothing.
	table := "api_keys"
	digestColumn := "key_digest"
	if bearer {
		table = "auth_access_tokens"
		digestColumn = "token_digest"
	}
	var lockedID string
	err = tx.QueryRowContext(ctx, `SELECT id FROM `+table+` WHERE id=$1 AND workspace_id=$2 AND `+digestColumn+`=$3 FOR SHARE`, c.id, string(c.workspaceID), digest).Scan(&lockedID)
	if err == sql.ErrNoRows {
		return Principal{}, authRejected()
	}
	if err != nil {
		return Principal{}, err
	}
	// A locking SELECT can evaluate its target list before waiting for the row
	// lock. Read database time in a separate statement after acquiring it. For
	// an API key, that one clock reading is also the usage observation, bound
	// to the credential generation it admitted.
	var valid bool
	var usage apiKeyUsageObservation
	if bearer {
		err = tx.QueryRowContext(ctx, `SELECT revoked_at IS NULL AND (expires_at IS NULL OR expires_at>clock_timestamp()) AND token_digest=$3 FROM auth_access_tokens WHERE id=$1 AND workspace_id=$2`, c.id, string(c.workspaceID), digest).Scan(&valid)
	} else {
		usage.identity = apiKeyUsageIdentity{workspaceID: c.workspaceID, keyID: c.id, digest: [sha256.Size]byte(digest)}
		err = tx.QueryRowContext(ctx, `SELECT k.revoked_at IS NULL AND (k.expires_at IS NULL OR k.expires_at>admission.observed_at) AND k.key_digest=$3,k.usage_generation,admission.observed_at FROM api_keys k CROSS JOIN (SELECT clock_timestamp() AS observed_at) admission WHERE k.id=$1 AND k.workspace_id=$2`, c.id, string(c.workspaceID), digest).Scan(&valid, &usage.identity.generation, &usage.observedAt)
	}
	if err == sql.ErrNoRows || err == nil && !valid {
		return Principal{}, authRejected()
	}
	if err != nil {
		return Principal{}, err
	}
	var ws workspace.Workspace
	if err = tx.QueryRowContext(ctx, `SELECT id,type,name,created_at FROM workspaces WHERE id=$1`, string(c.workspaceID)).Scan(&ws.ID, &ws.Type, &ws.Name, &ws.CreatedAt); err != nil {
		return Principal{}, err
	}
	p := Principal{Workspace: ws, Credential: Credential{ID: c.id, Kind: CredentialAPIKey}, Authority: c.authority}
	if c.authority.Kind == AuthorityIndependentKey {
		p = IndependentKeyPrincipal(ws, c.id)
	} else if bearer {
		p.Credential.Kind = CredentialAccessToken
	} else {
		p.APIKeyID = c.id
	}
	if kind != "" {
		p.Identity = &Identity{ID: c.authority.IdentityID, Kind: kind}
	}
	if err = p.Validate(); err != nil {
		return Principal{}, err
	}
	if err = tx.Commit(); err != nil {
		return Principal{}, err
	}
	// Only a committed API-key admission is sampled. Submission never waits,
	// and a dropped sample cannot change this admission.
	if !bearer {
		s.usage.submit(usage)
	}
	return p, nil
}

func (s *AuthorityResolver) LoadFederationRule(ctx context.Context, ruleID, organizationID string) (FederationRule, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var r FederationRule
	var raw []byte
	err := s.db.QueryRowContext(ctx, `SELECT security_config,enabled,revision FROM auth_federation_rules WHERE id=$1 AND organization_id=$2 AND NOT removed`, ruleID, organizationID).Scan(&raw, &r.Enabled, &r.Revision)
	if err == sql.ErrNoRows {
		return r, authRejected()
	}
	if err != nil {
		return r, err
	}
	enabled, revision := r.Enabled, r.Revision
	if json.Unmarshal(raw, &r) != nil {
		return FederationRule{}, authRejected()
	}
	r.Enabled = enabled
	r.Revision = revision
	if !enabled {
		return FederationRule{}, authRejected()
	}
	return r, nil
}

type ExchangeSelectors struct{ WorkspaceID, ServiceAccountID string }
type AccessTokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
}

// Issue consumes an immutable verifier proof, then locks/rechecks the same
// security revision. No issuer I/O occurs while these PostgreSQL locks are held.
func (s *AuthorityResolver) Issue(ctx context.Context, proof VerifiedAssertion, selectors ExchangeSelectors) (token AccessTokenResponse, resultErr error) {
	facts := AuditEvent{}
	defer func() { RecordDecision(ctx, "grant", resultErr, facts) }()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if selectors.WorkspaceID != "" && !boundedIdentity(selectors.WorkspaceID) {
		return AccessTokenResponse{}, authRejected()
	}
	if selectors.ServiceAccountID != "" && !boundedIdentity(selectors.ServiceAccountID) {
		return AccessTokenResponse{}, authRejected()
	}
	if proof.ruleID == "" || proof.ruleRevision <= 0 || proof.subject == "" {
		return AccessTokenResponse{}, authRejected()
	}
	facts.RuleRevision = proof.ruleRevision
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AccessTokenResponse{}, err
	}
	defer func() { _ = tx.Rollback() }()
	var identity IdentityBinding
	var identityRevision int64
	err = tx.QueryRowContext(ctx, `SELECT i.id,i.identity_kind,COALESCE(i.service_account_id,''),i.revision FROM auth_identities i JOIN auth_federation_rules r ON r.organization_id=i.organization_id AND r.issuer=i.issuer WHERE r.id=$1 AND i.issuer=$2 AND i.subject=$3 AND i.enabled AND NOT i.removed`, proof.ruleID, proof.issuer, proof.subject).Scan(&identity.ID, &identity.Kind, &identity.ServiceAccountID, &identityRevision)
	if err == sql.ErrNoRows {
		return AccessTokenResponse{}, authRejected()
	}
	if err != nil {
		return AccessTokenResponse{}, err
	}
	facts.IdentityKind = identity.Kind
	facts.IdentityRevision = identityRevision
	if selectors.ServiceAccountID != "" && (identity.Kind != IdentityService || selectors.ServiceAccountID != identity.ServiceAccountID) {
		return AccessTokenResponse{}, authRejected()
	}
	selected := selectors.WorkspaceID
	if selected == "default" {
		if s.defaultWorkspace == "" {
			return AccessTokenResponse{}, authRejected()
		}
		selected = string(s.defaultWorkspace)
	}
	rows, err := tx.QueryContext(ctx, `SELECT id,workspace_id,revision,role_version FROM public.tetral_auth_lookup_grants($1,NULLIF($2,''))`, identity.ID, selected)
	if err != nil {
		return AccessTokenResponse{}, err
	}
	var authority Authority
	matches := 0
	for rows.Next() {
		var grantID, ws string
		var revision, policy int64
		if err := rows.Scan(&grantID, &ws, &revision, &policy); err != nil {
			_ = rows.Close()
			return AccessTokenResponse{}, err
		}
		if selected == "" || selected == ws {
			matches++
			authority = Authority{Kind: AuthorityIdentityGrant, RuleID: proof.ruleID, IdentityID: identity.ID, GrantID: grantID, RuleRevision: proof.ruleRevision, IdentityRevision: identityRevision, GrantRevision: revision, PolicyVersion: policy, Scope: ResourceReference{WorkspaceID: workspace.ID(ws), Type: "workspace"}}
		}
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		return AccessTokenResponse{}, err
	}
	if matches != 1 {
		return AccessTokenResponse{}, authRejected()
	}
	if _, err = lockCurrentAuthority(ctx, tx, authority); err != nil {
		return AccessTokenResponse{}, err
	}
	facts.RuleRevision = authority.RuleRevision
	facts.GrantRevision = authority.GrantRevision
	if err = setAuthWorkspace(ctx, tx, authority.Scope.WorkspaceID); err != nil {
		return AccessTokenResponse{}, err
	}
	var now time.Time
	if err = tx.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return AccessTokenResponse{}, err
	}
	seconds := int(proof.expiresAt.Sub(now) / time.Second)
	if seconds > 600 {
		seconds = 600
	}
	if seconds <= 120 {
		return AccessTokenResponse{}, authRejected()
	}
	raw, err := GenerateAPIKey()
	if err != nil {
		return AccessTokenResponse{}, err
	}
	raw = "tetral_at_" + strings.TrimPrefix(raw, TokenPrefix)
	_, err = tx.ExecContext(ctx, `INSERT INTO auth_access_tokens(id,workspace_id,token_digest,federation_rule_id,identity_id,grant_id,rule_revision,identity_revision,grant_revision,role_version,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, id.New("at_"), string(authority.Scope.WorkspaceID), DigestAPIKey(raw), authority.RuleID, authority.IdentityID, authority.GrantID, authority.RuleRevision, authority.IdentityRevision, authority.GrantRevision, authority.PolicyVersion, now.Add(time.Duration(seconds)*time.Second))
	if err != nil {
		return AccessTokenResponse{}, err
	}
	if err = tx.Commit(); err != nil {
		return AccessTokenResponse{}, err
	}
	return AccessTokenResponse{AccessToken: raw, TokenType: "Bearer", ExpiresIn: seconds}, nil
}

func authenticationDependencyError(err error) error {
	if err == nil {
		return nil
	}
	var denied *AuthenticationError
	var validation *ValidationError
	var unavailable *UnavailableError
	if errors.As(err, &denied) || errors.As(err, &validation) || errors.As(err, &unavailable) {
		return err
	}
	return &UnavailableError{}
}

// ExchangeError exposes only typed identity failures or safe dependency failure.
func ExchangeError(err error) error { return authenticationDependencyError(err) }
