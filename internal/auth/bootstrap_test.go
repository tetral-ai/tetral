package auth_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestValidateBootstrapKeyRejectsEmpty(t *testing.T) {
	if err := auth.ValidateBootstrapKey(""); err == nil {
		t.Fatal("expected error for empty bootstrap key")
	}
}

func TestValidateBootstrapKeyRejectsWhitespace(t *testing.T) {
	if err := auth.ValidateBootstrapKey("   \t\n"); err == nil {
		t.Fatal("expected error for whitespace-only bootstrap key")
	}
}

func TestValidateBootstrapKeyRejectsShortValue(t *testing.T) {
	if err := auth.ValidateBootstrapKey("short"); err == nil {
		t.Fatal("expected error for short bootstrap key")
	}
	var weak *auth.WeakBootstrapKeyError
	if !errors.As(auth.ValidateBootstrapKey("short"), &weak) {
		t.Fatalf("expected *auth.WeakBootstrapKeyError")
	}
}

func TestValidateBootstrapKeyAcceptsThirtyTwoBytes(t *testing.T) {
	// 32-byte key (exactly the floor).
	key := strings.Repeat("a", 32)
	if err := auth.ValidateBootstrapKey(key); err != nil {
		t.Fatalf("32-byte key must satisfy floor: %v", err)
	}
}

func TestValidateBootstrapKeyAcceptsLongerKey(t *testing.T) {
	key := "tetral_sk_" + strings.Repeat("X", 50)
	if err := auth.ValidateBootstrapKey(key); err != nil {
		t.Fatalf("long key must satisfy floor: %v", err)
	}
}

func TestRefreshBootstrapValidatesBeforeUpsert(t *testing.T) {
	db := storagetest.NewPostgreSQLAdminDB(t)
	store := auth.NewAPIKeyStore(db)
	if err := auth.RefreshBootstrap(context.Background(), store, workspace.DefaultID, "weakkey"); err == nil {
		t.Fatal("expected RefreshBootstrap to reject a weak env key")
	}
}

func TestRefreshBootstrapInsertsBootstrapRowOnFreshSchema(t *testing.T) {
	db := storagetest.NewPostgreSQLAdminDB(t)
	store := auth.NewAPIKeyStore(db)
	envKey := strings.Repeat("a", 64)
	if err := auth.RefreshBootstrap(context.Background(), store, workspace.DefaultID, envKey); err != nil {
		t.Fatalf("RefreshBootstrap: %v", err)
	}
	// Auth must succeed for the env key.
	result, err := store.AuthenticateRawKey(context.Background(), envKey)
	if err != nil {
		t.Fatalf("AuthenticateRawKey for bootstrap: %v", err)
	}
	if result.Workspace.ID != workspace.DefaultID {
		t.Errorf("authenticated workspace = %q; want %q", result.Workspace.ID, workspace.DefaultID)
	}
}

func TestRefreshBootstrapRotatesBootstrapKey(t *testing.T) {
	db := storagetest.NewPostgreSQLAdminDB(t)
	store := auth.NewAPIKeyStore(db)
	ctx := context.Background()
	first := strings.Repeat("a", 64)
	second := strings.Repeat("b", 64)

	if err := auth.RefreshBootstrap(ctx, store, workspace.DefaultID, first); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	// The first key authenticates initially.
	if _, err := store.AuthenticateRawKey(ctx, first); err != nil {
		t.Fatalf("first key must authenticate before rotation: %v", err)
	}

	if err := auth.RefreshBootstrap(ctx, store, workspace.DefaultID, second); err != nil {
		t.Fatalf("rotate refresh: %v", err)
	}
	// The first key must no longer authenticate.
	if _, err := store.AuthenticateRawKey(ctx, first); err == nil {
		t.Error("first env key still authenticates after rotation; bootstrap rotate did not invalidate it")
	}
	// The second key must authenticate.
	if _, err := store.AuthenticateRawKey(ctx, second); err != nil {
		t.Fatalf("second env key must authenticate after rotation: %v", err)
	}
}

func TestRefreshBootstrapPreservesStandardKeys(t *testing.T) {
	db := storagetest.NewPostgreSQLAdminDB(t)
	store := auth.NewAPIKeyStore(db)
	ctx := context.Background()

	// Seed bootstrap, then create a standard key, then rotate
	// bootstrap and prove the standard key still authenticates.
	first := strings.Repeat("a", 64)
	if err := auth.RefreshBootstrap(ctx, store, workspace.DefaultID, first); err != nil {
		t.Fatalf("first refresh: %v", err)
	}

	created, err := store.CreateForWorkspace(ctx, workspace.DefaultID, "standard-key")
	if err != nil {
		t.Fatalf("create standard key: %v", err)
	}
	if created.APIKey == "" {
		t.Fatal("create result must include raw api_key")
	}

	if err := auth.RefreshBootstrap(ctx, store, workspace.DefaultID, strings.Repeat("c", 64)); err != nil {
		t.Fatalf("rotate refresh: %v", err)
	}

	// Standard key must still authenticate.
	if _, err := store.AuthenticateRawKey(ctx, created.APIKey); err != nil {
		t.Errorf("standard key invalidated by bootstrap rotation: %v", err)
	}
}

func TestRefreshBootstrapNoOpOnUnchangedDigest(t *testing.T) {
	db := storagetest.NewPostgreSQLAdminDB(t)
	store := auth.NewAPIKeyStore(db)
	ctx := context.Background()
	envKey := strings.Repeat("a", 64)
	if err := auth.RefreshBootstrap(ctx, store, workspace.DefaultID, envKey); err != nil {
		t.Fatalf("first refresh: %v", err)
	}

	// Capture the bootstrap row identity + created_at after the
	// first refresh.
	var firstID, firstCreatedAt string
	if err := db.QueryRowContext(ctx,
		`SELECT id, created_at FROM api_keys WHERE workspace_id = $1 AND key_kind = 'bootstrap'`,
		string(workspace.DefaultID),
	).Scan(&firstID, &firstCreatedAt); err != nil {
		t.Fatalf("read bootstrap row after first refresh: %v", err)
	}

	// Re-run with the same env key. No-op semantics: the bootstrap
	// row must keep the same id and created_at, and the row count
	// must stay at 1.
	if err := auth.RefreshBootstrap(ctx, store, workspace.DefaultID, envKey); err != nil {
		t.Fatalf("idempotent refresh: %v", err)
	}
	var bootstrapCount int
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM api_keys WHERE workspace_id = $1 AND key_kind = 'bootstrap'`,
		string(workspace.DefaultID),
	).Scan(&bootstrapCount); err != nil {
		t.Fatalf("count bootstrap rows: %v", err)
	}
	if bootstrapCount != 1 {
		t.Errorf("bootstrap row count after idempotent refresh = %d; want 1", bootstrapCount)
	}
	var secondID, secondCreatedAt string
	if err := db.QueryRowContext(ctx,
		`SELECT id, created_at FROM api_keys WHERE workspace_id = $1 AND key_kind = 'bootstrap'`,
		string(workspace.DefaultID),
	).Scan(&secondID, &secondCreatedAt); err != nil {
		t.Fatalf("read bootstrap row after no-op refresh: %v", err)
	}
	if secondID != firstID {
		t.Errorf("bootstrap row id changed across no-op refresh: first=%q second=%q (must stay stable)", firstID, secondID)
	}
	if secondCreatedAt != firstCreatedAt {
		t.Errorf("bootstrap created_at changed across no-op refresh: first=%q second=%q (must not be touched)", firstCreatedAt, secondCreatedAt)
	}

	if _, err := store.AuthenticateRawKey(ctx, envKey); err != nil {
		t.Errorf("env key must still authenticate after no-op refresh: %v", err)
	}
}

func TestRefreshBootstrapReactivatesRevokedBootstrapRow(t *testing.T) {
	db := storagetest.NewPostgreSQLAdminDB(t)
	store := auth.NewAPIKeyStore(db)
	ctx := context.Background()
	envKey := strings.Repeat("a", 64)

	if err := auth.RefreshBootstrap(ctx, store, workspace.DefaultID, envKey); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	if _, err := store.AuthenticateRawKey(ctx, envKey); err != nil {
		t.Fatalf("env key must authenticate before revoke: %v", err)
	}

	var firstID string
	if err := db.QueryRowContext(ctx,
		`SELECT id FROM api_keys WHERE workspace_id = $1 AND key_kind = 'bootstrap'`,
		string(workspace.DefaultID),
	).Scan(&firstID); err != nil {
		t.Fatalf("read bootstrap row id: %v", err)
	}

	if err := store.RevokeForWorkspace(ctx, workspace.DefaultID, firstID); err != nil {
		t.Fatalf("revoke bootstrap row: %v", err)
	}
	if _, err := store.AuthenticateRawKey(ctx, envKey); err == nil {
		t.Fatal("revoked bootstrap env key still authenticates before refresh")
	}

	if err := auth.RefreshBootstrap(ctx, store, workspace.DefaultID, envKey); err != nil {
		t.Fatalf("reactivating refresh: %v", err)
	}
	if _, err := store.AuthenticateRawKey(ctx, envKey); err != nil {
		t.Fatalf("env key must authenticate after reactivating refresh: %v", err)
	}

	var (
		bootstrapCount int
		secondID       string
		revokedAt      sql.NullString
	)
	if err := db.QueryRowContext(ctx,
		`SELECT count(*), min(id), min(revoked_at)
		   FROM api_keys
		  WHERE workspace_id = $1 AND key_kind = 'bootstrap'`,
		string(workspace.DefaultID),
	).Scan(&bootstrapCount, &secondID, &revokedAt); err != nil {
		t.Fatalf("read bootstrap row after reactivation: %v", err)
	}
	if bootstrapCount != 1 {
		t.Errorf("bootstrap row count after reactivation = %d; want 1", bootstrapCount)
	}
	if secondID != firstID {
		t.Errorf("bootstrap row id changed during reactivation: first=%q second=%q", firstID, secondID)
	}
	if revokedAt.Valid {
		t.Errorf("revoked_at after reactivation = %q; want NULL", revokedAt.String)
	}
}

func TestRefreshBootstrapDoesNotReactivateRevokedStandardKey(t *testing.T) {
	db := storagetest.NewPostgreSQLAdminDB(t)
	store := auth.NewAPIKeyStore(db)
	ctx := context.Background()
	envKey := strings.Repeat("a", 64)

	if err := auth.RefreshBootstrap(ctx, store, workspace.DefaultID, envKey); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	created, err := store.CreateForWorkspace(ctx, workspace.DefaultID, "revoked-standard")
	if err != nil {
		t.Fatalf("create standard key: %v", err)
	}
	if _, err := store.AuthenticateRawKey(ctx, created.APIKey); err != nil {
		t.Fatalf("standard key must authenticate before revoke: %v", err)
	}
	if err := store.RevokeForWorkspace(ctx, workspace.DefaultID, created.ID); err != nil {
		t.Fatalf("revoke standard key: %v", err)
	}
	if _, err := store.AuthenticateRawKey(ctx, created.APIKey); err == nil {
		t.Fatal("revoked standard key still authenticates before refresh")
	}

	if err := auth.RefreshBootstrap(ctx, store, workspace.DefaultID, strings.Repeat("b", 64)); err != nil {
		t.Fatalf("rotate refresh: %v", err)
	}
	if _, err := store.AuthenticateRawKey(ctx, created.APIKey); err == nil {
		t.Fatal("RefreshBootstrap reactivated a revoked standard key")
	}

	var revokedAt sql.NullString
	if err := db.QueryRowContext(ctx,
		`SELECT revoked_at FROM api_keys WHERE id = $1 AND key_kind = 'standard'`,
		created.ID,
	).Scan(&revokedAt); err != nil {
		t.Fatalf("read standard revoked_at: %v", err)
	}
	if !revokedAt.Valid || revokedAt.String == "" {
		t.Errorf("standard revoked_at after RefreshBootstrap = %v; want non-empty timestamp", revokedAt)
	}
}
