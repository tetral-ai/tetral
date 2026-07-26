package workspace_test

import (
	"context"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestSeederCreatesWorkspaceOnce(t *testing.T) {
	_, adminDB := storagetest.NewPostgreSQLDBWithAdmin(t)
	ctx := context.Background()
	seeder := workspace.NewSeeder(adminDB)
	id := workspace.ID("seeded-workspace")

	created, err := seeder.Seed(ctx, id, "Seeded Workspace")
	if err != nil {
		t.Fatalf("first seed: %v", err)
	}
	if !created {
		t.Fatal("first seed reported an existing workspace")
	}

	created, err = seeder.Seed(ctx, id, "Ignored Replacement Name")
	if err != nil {
		t.Fatalf("second seed: %v", err)
	}
	if created {
		t.Fatal("second seed reported a newly created workspace")
	}

	got, err := workspace.NewStore(adminDB).Get(ctx, id)
	if err != nil {
		t.Fatalf("read seeded workspace: %v", err)
	}
	if got.Type != "workspace" || got.Name != "Seeded Workspace" {
		t.Fatalf("seeded workspace = %#v; want original workspace record", got)
	}
	if _, err := time.Parse(time.RFC3339, got.CreatedAt); err != nil {
		t.Fatalf("seeded created_at = %q; want RFC3339: %v", got.CreatedAt, err)
	}

	var count int
	if err := adminDB.QueryRowContext(ctx, `SELECT count(*) FROM workspaces WHERE id = $1`, id).Scan(&count); err != nil {
		t.Fatalf("count seeded workspace: %v", err)
	}
	if count != 1 {
		t.Fatalf("seeded workspace count = %d; want 1", count)
	}
}
