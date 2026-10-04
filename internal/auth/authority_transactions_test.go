package auth

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/workspace"
)

// awaitAuthorityBlock observes the actual backend lock graph. No production
// pause hook or driver timing estimate stands in for the transaction boundary.
func awaitAuthorityBlock(ctx context.Context, t *testing.T, admin *sql.DB, holder int) int {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiter int
		err := admin.QueryRowContext(ctx, `SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND $1=ANY(pg_blocking_pids(pid)) ORDER BY pid LIMIT 1`, holder).Scan(&waiter)
		if err == nil {
			return waiter
		}
		if err != sql.ErrNoRows {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("credential operation did not reach its observed row-lock boundary")
		case <-ticker.C:
		}
	}
}
func holdAuthorityRow(ctx context.Context, t *testing.T, admin *sql.DB, table, id string) (*sql.Tx, int) {
	t.Helper()
	if table != "api_keys" && table != "auth_access_tokens" && table != "auth_workspace_grants" && table != "auth_federation_rules" {
		t.Fatal("unsupported fixture lock table")
	}
	tx, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	var pid int
	if err := tx.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	var found string
	if err := tx.QueryRowContext(ctx, `SELECT id FROM `+table+` WHERE id=$1 FOR UPDATE`, id).Scan(&found); err != nil {
		t.Fatal(err)
	}
	return tx, pid
}
func awaitDatabaseExpiry(ctx context.Context, t *testing.T, admin *sql.DB, expiry time.Time) {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var expired bool
		if err := admin.QueryRowContext(ctx, `SELECT clock_timestamp()>=$1`, expiry).Scan(&expired); err != nil {
			t.Fatal(err)
		}
		if expired {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("database expiry boundary was not reached")
		case <-ticker.C:
		}
	}
}

// startAuthorityOperation retains cancellation, holder rollback and joining even
// when a subsequent lock-graph assertion aborts the test.
func startAuthorityOperation(t *testing.T, cancel context.CancelFunc, holder *sql.Tx, operation func() error) <-chan error {
	t.Helper()
	result := make(chan error, 1)
	done := make(chan struct{})
	go func() { defer close(done); result <- operation() }()
	t.Cleanup(func() {
		cancel()
		_ = holder.Rollback()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("credential operation did not join after cancellation and holder rollback")
		}
		select {
		case <-result:
		default:
		}
	})
	return result
}

func TestAuthorityResolverCredentialExpiryAfterLockWait(t *testing.T) {
	for _, bearer := range []bool{false, true} {
		for _, issueChild := range []bool{false, true} {
			name := "api_key"
			if bearer {
				name = "access_token"
			}
			if issueChild {
				name += "_child_issuance"
			} else {
				name += "_admission"
			}
			t.Run(name, func(t *testing.T) {
				admin, resolver, store, proof := authorityFixture(t)
				ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
				defer cancel()
				var raw, id, table string
				if bearer {
					result, err := resolver.Issue(ctx, proof, ExchangeSelectors{})
					if err != nil {
						t.Fatal(err)
					}
					raw = result.AccessToken
					p, err := resolver.AuthenticateBearer(ctx, raw)
					if err != nil {
						t.Fatal(err)
					}
					id = p.Credential.ID
					table = "auth_access_tokens"
				} else {
					result, err := store.CreateForWorkspace(ctx, workspace.DefaultID, "expiry barrier")
					if err != nil {
						t.Fatal(err)
					}
					raw, id, table = result.APIKey, result.ID, "api_keys"
				}
				var principal Principal
				var err error
				if issueChild {
					if bearer {
						principal, err = resolver.AuthenticateBearer(ctx, raw)
					} else {
						principal, err = resolver.AuthenticateKey(ctx, raw)
					}
					if err != nil {
						t.Fatal(err)
					}
				}
				var expiry time.Time
				if err := admin.QueryRowContext(ctx, `UPDATE `+table+` SET expires_at=clock_timestamp()+interval '2 seconds' WHERE id=$1 RETURNING expires_at`, id).Scan(&expiry); err != nil {
					t.Fatal(err)
				}
				held, pid := holdAuthorityRow(ctx, t, admin, table, id)
				joined := startAuthorityOperation(t, cancel, held, func() error {
					if issueChild {
						_, err := store.CreateForPrincipal(ctx, principal, "must not issue after expiry")
						return err
					}
					if bearer {
						_, err := resolver.AuthenticateBearer(ctx, raw)
						return err
					}
					_, err := resolver.AuthenticateKey(ctx, raw)
					return err
				})
				awaitAuthorityBlock(ctx, t, admin, pid)
				awaitDatabaseExpiry(ctx, t, admin, expiry)
				if err := held.Commit(); err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-joined:
					requireCredentialRejection(t, err)
				case <-ctx.Done():
					t.Fatal("credential operation failed to join after holder commit")
				}
				if issueChild {
					var count int
					if err := admin.QueryRow(`SELECT count(*) FROM api_keys WHERE name='must not issue after expiry'`).Scan(&count); err != nil {
						t.Fatal(err)
					}
					if count != 0 {
						t.Fatal("expired issuer produced a durable child")
					}
				}
			})
		}
	}
}

func TestAuthorityResolverBootstrapRotationBindsSelectedDigest(t *testing.T) {
	admin, resolver, store, _ := authorityFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	oldRaw, err := GenerateAPIKey()
	if err != nil {
		t.Fatal(err)
	}
	newRaw, err := GenerateAPIKey()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertBootstrap(ctx, workspace.DefaultID, oldRaw); err != nil {
		t.Fatal(err)
	}
	var keyID string
	if err := admin.QueryRow(`SELECT id FROM api_keys WHERE key_kind='bootstrap'`).Scan(&keyID); err != nil {
		t.Fatal(err)
	}
	held, pid := holdAuthorityRow(ctx, t, admin, "api_keys", keyID)
	// MVCC lookup sees the old digest, while the locking recheck waits on this
	// legitimate in-place bootstrap rotation with the same durable key ID.
	if _, err := held.ExecContext(ctx, `UPDATE api_keys SET key_digest=$2,key_prefix=$3 WHERE id=$1`, keyID, DigestAPIKey(newRaw), KeyPrefixFor(newRaw)); err != nil {
		t.Fatal(err)
	}
	joined := startAuthorityOperation(t, cancel, held, func() error { _, err := resolver.AuthenticateKey(ctx, oldRaw); return err })
	awaitAuthorityBlock(ctx, t, admin, pid)
	if err := held.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-joined:
		requireCredentialRejection(t, err)
	case <-ctx.Done():
		t.Fatal("bootstrap admission did not join")
	}
	if _, err := resolver.AuthenticateKey(ctx, newRaw); err != nil {
		t.Fatal(err)
	}
}

func TestAuthorityResolverPolicyMutationBeforeIssuanceRejectsStaleAuthority(t *testing.T) {
	t.Run("VerifierProofBeforeRuleEdit", func(t *testing.T) {
		admin, resolver, _, proof := authorityFixture(t)
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		held, pid := holdAuthorityRow(ctx, t, admin, "auth_federation_rules", proof.ruleID)
		if _, err := held.ExecContext(ctx, `UPDATE auth_federation_rules SET revision=revision+1,audience='revised-audience' WHERE id=$1`, proof.ruleID); err != nil {
			t.Fatal(err)
		}
		joined := startAuthorityOperation(t, cancel, held, func() error { _, err := resolver.Issue(ctx, proof, ExchangeSelectors{}); return err })
		awaitAuthorityBlock(ctx, t, admin, pid)
		if err := held.Commit(); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-joined:
			requireCredentialRejection(t, err)
		case <-ctx.Done():
			t.Fatal("token issuance did not join")
		}
		var n int
		if err := admin.QueryRow(`SELECT count(*) FROM auth_access_tokens`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Fatal("old verifier proof was restamped into current authority")
		}
	})
	t.Run("AdmittedPrincipalBeforeGrantEdit", func(t *testing.T) {
		admin, resolver, store, proof := authorityFixture(t)
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		token, err := resolver.Issue(ctx, proof, ExchangeSelectors{})
		if err != nil {
			t.Fatal(err)
		}
		principal, err := resolver.AuthenticateBearer(ctx, token.AccessToken)
		if err != nil {
			t.Fatal(err)
		}
		held, pid := holdAuthorityRow(ctx, t, admin, "auth_workspace_grants", principal.Authority.GrantID)
		if _, err := held.ExecContext(ctx, `UPDATE auth_workspace_grants SET revision=revision+1 WHERE id=$1`, principal.Authority.GrantID); err != nil {
			t.Fatal(err)
		}
		joined := startAuthorityOperation(t, cancel, held, func() error { _, err := store.CreateForPrincipal(ctx, principal, "must not restamp"); return err })
		awaitAuthorityBlock(ctx, t, admin, pid)
		if err := held.Commit(); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-joined:
			requireCredentialRejection(t, err)
		case <-ctx.Done():
			t.Fatal("key issuance did not join")
		}
		var n int
		if err := admin.QueryRow(`SELECT count(*) FROM api_keys WHERE name='must not restamp'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Fatal("admitted principal was restamped into newer grant revision")
		}
	})
}

func TestAuthorityResolverAdmissionAndRevocationLockOrder(t *testing.T) {
	t.Run("RevocationBeforeAdmission", func(t *testing.T) {
		admin, resolver, store, _ := authorityFixture(t)
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		key, err := store.CreateForWorkspace(ctx, workspace.DefaultID, "revoke first")
		if err != nil {
			t.Fatal(err)
		}
		held, pid := holdAuthorityRow(ctx, t, admin, "api_keys", key.ID)
		if _, err := held.ExecContext(ctx, `UPDATE api_keys SET revoked_at=clock_timestamp() WHERE id=$1`, key.ID); err != nil {
			t.Fatal(err)
		}
		joined := startAuthorityOperation(t, cancel, held, func() error { _, err := resolver.AuthenticateKey(ctx, key.APIKey); return err })
		awaitAuthorityBlock(ctx, t, admin, pid)
		if err := held.Commit(); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-joined:
			requireCredentialRejection(t, err)
		case <-ctx.Done():
			t.Fatal("revocation-first admission failed to join")
		}
	})
	t.Run("AdmissionBeforeRevocation", func(t *testing.T) {
		admin, resolver, store, _ := authorityFixture(t)
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		key, err := store.CreateForWorkspace(ctx, workspace.DefaultID, "admit first")
		if err != nil {
			t.Fatal(err)
		}
		held, err := admin.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = held.Rollback() }()
		var pid int
		if err := held.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
			t.Fatal(err)
		}
		// Workspace read follows the credential locking recheck. This table lock
		// pauses real admission while it owns the selected credential row lock.
		if _, err := held.ExecContext(ctx, `LOCK TABLE workspaces IN ACCESS EXCLUSIVE MODE`); err != nil {
			t.Fatal(err)
		}
		admission := startAuthorityOperation(t, cancel, held, func() error { _, err := resolver.AuthenticateKey(ctx, key.APIKey); return err })
		admissionPID := awaitAuthorityBlock(ctx, t, admin, pid)
		revocation := startAuthorityOperation(t, cancel, held, func() error {
			_, err := admin.ExecContext(ctx, `UPDATE api_keys SET revoked_at=clock_timestamp() WHERE id=$1`, key.ID)
			return err
		})
		awaitAuthorityBlock(ctx, t, admin, admissionPID)
		if err := held.Commit(); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-admission:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("admission-first operation failed to join")
		}
		select {
		case err := <-revocation:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("waiting revocation failed to join")
		}
		_, err = resolver.AuthenticateKey(ctx, key.APIKey)
		requireCredentialRejection(t, err)
	})
}

func TestAuthorityResolverIssuanceBeforePolicyCommitHoldsRoots(t *testing.T) {
	admin, resolver, _, proof := authorityFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	held, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Rollback() }()
	var pid int
	if err := held.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	// INSERT acquires RowExclusive after all policy roots have been locked. The
	// actual lock graph then proves the administrative mixed rule-removal/grant edit waits on Issue.
	if _, err := held.ExecContext(ctx, `LOCK TABLE auth_access_tokens IN SHARE MODE`); err != nil {
		t.Fatal(err)
	}
	var issued AccessTokenResponse
	issuance := startAuthorityOperation(t, cancel, held, func() error { var err error; issued, err = resolver.Issue(ctx, proof, ExchangeSelectors{}); return err })
	issuancePID := awaitAuthorityBlock(ctx, t, admin, pid)
	grant := policyFixture().WorkspaceGrants[0]
	grant.Enabled = false
	policy := startAuthorityOperation(t, cancel, held, func() error {
		_, err := NewPolicyStore(admin).Apply(ctx, PolicyDocument{RemoveFederationRules: []string{proof.ruleID}, WorkspaceGrants: []WorkspaceGrant{grant}})
		return err
	})
	awaitAuthorityBlock(ctx, t, admin, issuancePID)
	if err := held.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-issuance:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("issuance-first operation failed to join")
	}
	select {
	case err := <-policy:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("waiting policy edit failed to join")
	}
	// The token legitimately committed first. The later policy revision now
	// invalidates it, rather than allowing the earlier issuance to escape roots.
	_, err = resolver.AuthenticateBearer(ctx, issued.AccessToken)
	requireCredentialRejection(t, err)
}
