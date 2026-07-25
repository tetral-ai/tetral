package auth_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// seedDefaultBootstrap installs a default-workspace bootstrap key
// and returns the raw bootstrap secret so tests can authenticate
// against it. Used as a one-line setup for store tests that need a
// realistic bootstrap row.
func seedDefaultBootstrap(t *testing.T, store *auth.APIKeyStore) string {
	t.Helper()
	envKey := strings.Repeat("z", 64)
	if err := auth.RefreshBootstrap(context.Background(), store, workspace.DefaultID, envKey); err != nil {
		t.Fatalf("seedDefaultBootstrap: %v", err)
	}
	return envKey
}

func TestCreateForWorkspaceReturnsRawKeyOnceAndStoresMetadataOnly(t *testing.T) {
	db := storagetest.NewPostgreSQLAdminDB(t)
	store := auth.NewAPIKeyStore(db)

	created, err := store.CreateForWorkspace(context.Background(), workspace.DefaultID, "ci-key")
	if err != nil {
		t.Fatalf("CreateForWorkspace: %v", err)
	}
	if created.APIKey == "" {
		t.Fatal("CreateForWorkspace must return raw api_key on creation")
	}
	if !strings.HasPrefix(created.APIKey, auth.TokenPrefix) {
		t.Errorf("api_key %q does not start with %q", created.APIKey, auth.TokenPrefix)
	}
	if created.WorkspaceID != string(workspace.DefaultID) {
		t.Errorf("WorkspaceID = %q; want %q", created.WorkspaceID, workspace.DefaultID)
	}
	if created.Name != "ci-key" {
		t.Errorf("Name = %q; want %q", created.Name, "ci-key")
	}
	if created.KeyKind != auth.KindStandard {
		t.Errorf("KeyKind = %q; want %q", created.KeyKind, auth.KindStandard)
	}
	if !strings.HasPrefix(created.APIKey, created.KeyPrefix) {
		t.Errorf("KeyPrefix %q must be a prefix of api_key %q", created.KeyPrefix, created.APIKey)
	}

	// Direct DB inspection: api_keys must store only the digest, not
	// the raw key. The secret must not appear in name, key_prefix, or
	// in the BYTEA digest as a substring.
	var name, prefix string
	var digest []byte
	if err := db.QueryRow(
		`SELECT name, key_prefix, key_digest FROM api_keys WHERE id = $1`, created.ID,
	).Scan(&name, &prefix, &digest); err != nil {
		t.Fatalf("inspect api_keys row: %v", err)
	}
	if strings.Contains(name, created.APIKey) {
		t.Error("api_keys.name must not contain the raw key")
	}
	if strings.Contains(prefix, strings.TrimPrefix(created.APIKey, created.KeyPrefix)) {
		t.Error("api_keys.key_prefix must not contain the secret tail of the key")
	}
	if bytes.Contains(digest, []byte(created.APIKey)) {
		t.Error("api_keys.key_digest must not contain the raw key bytes")
	}
	if len(digest) != 32 {
		t.Errorf("digest length = %d; want 32 (SHA-256)", len(digest))
	}
}

func TestCreateForWorkspaceRejectsEmptyName(t *testing.T) {
	db := storagetest.NewPostgreSQLAdminDB(t)
	store := auth.NewAPIKeyStore(db)
	for _, name := range []string{"", "   ", "\t\n"} {
		_, err := store.CreateForWorkspace(context.Background(), workspace.DefaultID, name)
		if err == nil {
			t.Errorf("CreateForWorkspace accepted blank name %q", name)
			continue
		}
		var validation *auth.ValidationError
		if !errors.As(err, &validation) {
			t.Errorf("expected *auth.ValidationError for name %q, got %T", name, err)
		}
	}
}

func TestCreateForWorkspaceRejectsOversizedName(t *testing.T) {
	db := storagetest.NewPostgreSQLAdminDB(t)
	store := auth.NewAPIKeyStore(db)
	tooLong := strings.Repeat("x", 257)
	_, err := store.CreateForWorkspace(context.Background(), workspace.DefaultID, tooLong)
	if err == nil {
		t.Fatal("CreateForWorkspace accepted name with 257 code points; want rejection")
	}
	var validation *auth.ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("expected *auth.ValidationError for oversized name, got %T (%v)", err, err)
	}
}

func TestCreateForWorkspaceProducesUniqueRawTokens(t *testing.T) {
	db := storagetest.NewPostgreSQLAdminDB(t)
	store := auth.NewAPIKeyStore(db)
	seen := map[string]bool{}
	for i := 0; i < 5; i++ {
		created, err := store.CreateForWorkspace(context.Background(), workspace.DefaultID, "key-"+string(rune('a'+i)))
		if err != nil {
			t.Fatalf("CreateForWorkspace iter %d: %v", i, err)
		}
		if seen[created.APIKey] {
			t.Fatalf("duplicate raw token across calls: %q", created.APIKey)
		}
		seen[created.APIKey] = true
	}
}

func TestListActiveForWorkspaceReturnsMetadataOnly(t *testing.T) {
	db := storagetest.NewPostgreSQLAdminDB(t)
	store := auth.NewAPIKeyStore(db)
	ctx := context.Background()
	first, err := store.CreateForWorkspace(ctx, workspace.DefaultID, "first")
	if err != nil {
		t.Fatalf("create first: %v", err)
	}
	second, err := store.CreateForWorkspace(ctx, workspace.DefaultID, "second")
	if err != nil {
		t.Fatalf("create second: %v", err)
	}

	results, hasMore, err := store.ListActiveForWorkspace(ctx, workspace.DefaultID, 50, "", "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if hasMore {
		t.Error("hasMore = true for full result set")
	}
	if len(results) != 2 {
		t.Fatalf("len(results) = %d; want 2", len(results))
	}
	for _, meta := range results {
		if meta.ID != first.ID && meta.ID != second.ID {
			t.Errorf("unexpected api_keys row %q in list", meta.ID)
		}
		// Metadata-only response: must not embed digest or raw key.
		if strings.Contains(meta.KeyPrefix, "==") {
			t.Errorf("KeyPrefix %q looks base64-padded; metadata must use raw url encoding", meta.KeyPrefix)
		}
	}
}

func TestListActiveForWorkspaceExcludesRevoked(t *testing.T) {
	db := storagetest.NewPostgreSQLAdminDB(t)
	store := auth.NewAPIKeyStore(db)
	ctx := context.Background()
	created, err := store.CreateForWorkspace(ctx, workspace.DefaultID, "revoke-me")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := store.RevokeForWorkspace(ctx, workspace.DefaultID, created.ID); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	results, _, err := store.ListActiveForWorkspace(ctx, workspace.DefaultID, 50, "", "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, meta := range results {
		if meta.ID == created.ID {
			t.Error("revoked api key surfaced in active list")
		}
	}
}

func TestListActiveForWorkspaceHonorsLimitAndCursor(t *testing.T) {
	db := storagetest.NewPostgreSQLAdminDB(t)
	store := auth.NewAPIKeyStore(db)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if _, err := store.CreateForWorkspace(ctx, workspace.DefaultID, "k"+string(rune('a'+i))); err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
	}
	page1, hasMore, err := store.ListActiveForWorkspace(ctx, workspace.DefaultID, 2, "", "")
	if err != nil {
		t.Fatalf("List page1: %v", err)
	}
	if len(page1) != 2 {
		t.Fatalf("page1 len = %d; want 2", len(page1))
	}
	if !hasMore {
		t.Error("page1 hasMore = false; want true (5 rows total, limit 2)")
	}
	page2, _, err := store.ListActiveForWorkspace(ctx, workspace.DefaultID, 2, page1[len(page1)-1].ID, "")
	if err != nil {
		t.Fatalf("List page2: %v", err)
	}
	if len(page2) != 2 {
		t.Fatalf("page2 len = %d; want 2", len(page2))
	}
	if page2[0].ID == page1[0].ID {
		t.Error("page2 returned same first row as page1")
	}
}

func TestListActiveForWorkspaceRejectsCrossWorkspaceCursor(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	_ = runtime
	store := auth.NewAPIKeyStore(admin)
	ctx := context.Background()
	if _, err := admin.ExecContext(ctx,
		`INSERT INTO workspaces (id, type, name, created_at) VALUES ('workspace_b', 'workspace', 'B', '2026-01-01T00:00:00Z')
		 ON CONFLICT DO NOTHING`); err != nil {
		t.Fatalf("seed workspace_b: %v", err)
	}
	bID, err := store.CreateForWorkspace(ctx, "workspace_b", "b-key")
	if err != nil {
		t.Fatalf("create workspace_b key: %v", err)
	}

	_, _, err = store.ListActiveForWorkspace(ctx, workspace.DefaultID, 50, bID.ID, "")
	if err == nil {
		t.Fatal("expected ValidationError for cross-workspace cursor")
	}
	var vErr *auth.ValidationError
	if !errors.As(err, &vErr) {
		t.Fatalf("expected *auth.ValidationError, got %T (%v)", err, err)
	}
}

func TestRevokeForWorkspaceMarksRowAndBlocksReauth(t *testing.T) {
	db := storagetest.NewPostgreSQLAdminDB(t)
	store := auth.NewAPIKeyStore(db)
	ctx := context.Background()
	created, err := store.CreateForWorkspace(ctx, workspace.DefaultID, "revoke")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := store.AuthenticateRawKey(ctx, created.APIKey); err != nil {
		t.Fatalf("authenticate before revoke: %v", err)
	}
	if err := store.RevokeForWorkspace(ctx, workspace.DefaultID, created.ID); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if _, err := store.AuthenticateRawKey(ctx, created.APIKey); err == nil {
		t.Error("revoked api key still authenticates")
	}

	// revoked_at must be set in the DB and not be NULL.
	var revokedAt sql.NullString
	if err := db.QueryRow(`SELECT revoked_at FROM api_keys WHERE id = $1`, created.ID).Scan(&revokedAt); err != nil {
		t.Fatalf("query revoked_at: %v", err)
	}
	if !revokedAt.Valid || revokedAt.String == "" {
		t.Errorf("revoked_at = %v; want non-empty timestamp", revokedAt)
	}
}

func TestRevokeForWorkspaceReturnsNotFoundForMissingKey(t *testing.T) {
	db := storagetest.NewPostgreSQLAdminDB(t)
	store := auth.NewAPIKeyStore(db)
	err := store.RevokeForWorkspace(context.Background(), workspace.DefaultID, "ak_missing")
	if err == nil {
		t.Fatal("expected error for missing api_key id")
	}
	var notFound *auth.NotFoundError
	if !errors.As(err, &notFound) {
		t.Fatalf("expected *auth.NotFoundError, got %T (%v)", err, err)
	}
}

func TestRevokeForWorkspaceReturnsNotFoundForAlreadyRevoked(t *testing.T) {
	db := storagetest.NewPostgreSQLAdminDB(t)
	store := auth.NewAPIKeyStore(db)
	ctx := context.Background()
	created, err := store.CreateForWorkspace(ctx, workspace.DefaultID, "twice")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := store.RevokeForWorkspace(ctx, workspace.DefaultID, created.ID); err != nil {
		t.Fatalf("first revoke: %v", err)
	}
	err = store.RevokeForWorkspace(ctx, workspace.DefaultID, created.ID)
	if err == nil {
		t.Fatal("expected NotFound for already-revoked key")
	}
	var notFound *auth.NotFoundError
	if !errors.As(err, &notFound) {
		t.Fatalf("expected *auth.NotFoundError, got %T (%v)", err, err)
	}
}

func TestRevokeForWorkspaceCannotCrossWorkspace(t *testing.T) {
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	store := auth.NewAPIKeyStore(admin)
	ctx := context.Background()
	if _, err := admin.ExecContext(ctx,
		`INSERT INTO workspaces (id, type, name, created_at) VALUES ('workspace_b', 'workspace', 'B', '2026-01-01T00:00:00Z')
		 ON CONFLICT DO NOTHING`); err != nil {
		t.Fatalf("seed workspace_b: %v", err)
	}
	bKey, err := store.CreateForWorkspace(ctx, "workspace_b", "b-key")
	if err != nil {
		t.Fatalf("create workspace_b key: %v", err)
	}
	// Default workspace tries to revoke workspace_b's key — must fail.
	err = store.RevokeForWorkspace(ctx, workspace.DefaultID, bKey.ID)
	if err == nil {
		t.Fatal("revoking workspace_b key from default workspace must fail")
	}
	var notFound *auth.NotFoundError
	if !errors.As(err, &notFound) {
		t.Fatalf("expected *auth.NotFoundError, got %T (%v)", err, err)
	}
}

func TestAuthenticateRawKeyReturnsBootstrapWorkspace(t *testing.T) {
	db := storagetest.NewPostgreSQLAdminDB(t)
	store := auth.NewAPIKeyStore(db)
	envKey := seedDefaultBootstrap(t, store)
	result, err := store.AuthenticateRawKey(context.Background(), envKey)
	if err != nil {
		t.Fatalf("AuthenticateRawKey: %v", err)
	}
	if result.Workspace.ID != workspace.DefaultID {
		t.Errorf("Workspace.ID = %q; want %q", result.Workspace.ID, workspace.DefaultID)
	}
	if result.APIKeyID == "" {
		t.Error("APIKeyID must be populated")
	}
	if result.Workspace.Name == "" {
		t.Error("Workspace.Name must be populated by the join with workspaces")
	}
}

func TestStoreAuthenticatorPropagatesAPIKeyIDInPrincipal(t *testing.T) {
	db := storagetest.NewPostgreSQLAdminDB(t)
	store := auth.NewAPIKeyStore(db)
	created, err := store.CreateForWorkspace(context.Background(), workspace.DefaultID, "principal-key")
	if err != nil {
		t.Fatalf("CreateForWorkspace: %v", err)
	}
	principal, err := (&auth.StoreAuthenticator{Store: store}).Authenticate(context.Background(), created.APIKey)
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if principal.APIKeyID != created.ID {
		t.Errorf("Principal.APIKeyID = %q; want %q", principal.APIKeyID, created.ID)
	}
	if principal.Workspace.ID != workspace.DefaultID {
		t.Errorf("Principal.Workspace.ID = %q; want %q", principal.Workspace.ID, workspace.DefaultID)
	}
	if strings.Contains(principal.APIKeyID, created.APIKey) {
		t.Error("Principal.APIKeyID must not contain raw key material")
	}
}

func TestAuthenticateRawKeyRejectsUnknownKey(t *testing.T) {
	db := storagetest.NewPostgreSQLAdminDB(t)
	store := auth.NewAPIKeyStore(db)
	_ = seedDefaultBootstrap(t, store)
	_, err := store.AuthenticateRawKey(context.Background(), "tetral_sk_unknown_"+strings.Repeat("x", 32))
	if err == nil {
		t.Fatal("expected AuthenticationError for unknown key")
	}
	var authErr *auth.AuthenticationError
	if !errors.As(err, &authErr) {
		t.Fatalf("expected *auth.AuthenticationError, got %T (%v)", err, err)
	}
}

func TestAuthenticateRawKeyRejectsEmpty(t *testing.T) {
	db := storagetest.NewPostgreSQLAdminDB(t)
	store := auth.NewAPIKeyStore(db)
	_, err := store.AuthenticateRawKey(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty key")
	}
	var authErr *auth.AuthenticationError
	if !errors.As(err, &authErr) {
		t.Fatalf("expected *auth.AuthenticationError, got %T (%v)", err, err)
	}
}
