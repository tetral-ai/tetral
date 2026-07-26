package tetralauth

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestAuthBuildRequiresSeededWorkspaceBeforeRegisteringBootstrapKey(t *testing.T) {
	ctx := context.Background()
	adminDB := storagetest.NewEmptyPostgreSQLAdminDB(t)
	if err := storage.MigrateSchema(ctx, adminDB); err != nil {
		t.Fatalf("migrate schema: %v", err)
	}
	privateKey, err := auth.GenerateEd25519PrivateKeyBase64()
	if err != nil {
		t.Fatalf("generate signing key: %v", err)
	}
	workspaceID := workspace.ID("bootstrap-order-test")
	config := RouterBuildConfig{
		RawDatabase: adminDB,
		Config: Config{
			BootstrapAPIKey:                testBootstrapAPIKey(),
			BootstrapWorkspaceID:           workspaceID,
			InternalPrincipalPrivateKeyB64: privateKey,
			InternalPrincipalTTL:           time.Minute,
		},
	}

	if _, err := BuildRouter(ctx, config); err == nil {
		t.Fatal("BuildRouter without a workspace seed unexpectedly succeeded")
	} else {
		var notFound *workspace.NotFoundError
		if !errors.As(err, &notFound) {
			t.Fatalf("BuildRouter without a workspace seed error = %T %v; want workspace.NotFoundError", err, err)
		}
	}

	if _, err := workspace.NewSeeder(adminDB).Seed(ctx, workspaceID, "Bootstrap Order Test"); err != nil {
		t.Fatalf("seed workspace: %v", err)
	}
	if _, err := BuildRouter(ctx, config); err != nil {
		t.Fatalf("BuildRouter after workspace seed: %v", err)
	}

	var count int
	if err := adminDB.QueryRowContext(ctx,
		`SELECT count(*) FROM api_keys WHERE workspace_id = $1 AND key_kind = 'bootstrap'`,
		workspaceID,
	).Scan(&count); err != nil {
		t.Fatalf("count bootstrap api keys: %v", err)
	}
	if count != 1 {
		t.Fatalf("bootstrap api key count = %d; want 1", count)
	}
}
