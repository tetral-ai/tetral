package workspace_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestStoreGetReturnsExplicitWorkspaceFixture(t *testing.T) {
	_, db := storagetest.NewPostgreSQLDBWithAdmin(t)
	workspaceID := workspace.ID("ws_store_fixture")
	seedWorkspace(t, db, workspaceID, "Store Fixture")
	store := workspace.NewStore(db)

	ws, err := store.Get(context.Background(), workspaceID)
	if err != nil {
		t.Fatalf("Get explicit workspace fixture: %v", err)
	}
	if ws.ID != workspaceID {
		t.Errorf("ID = %q; want %q", ws.ID, workspaceID)
	}
	if ws.Type != "workspace" {
		t.Errorf("Type = %q; want %q", ws.Type, "workspace")
	}
	if ws.Name != "Store Fixture" {
		t.Errorf("Name = %q; want %q", ws.Name, "Store Fixture")
	}
	if ws.CreatedAt == "" {
		t.Error("CreatedAt must be a non-empty UTC RFC3339 timestamp")
	}
}

func TestStoreGetReturnsNotFoundForMissingWorkspace(t *testing.T) {
	db := storagetest.NewPostgreSQLDB(t)
	store := workspace.NewStore(db)

	_, err := store.Get(context.Background(), "workspace_does_not_exist")
	if err == nil {
		t.Fatal("expected error for missing workspace, got nil")
	}
	var notFound *workspace.NotFoundError
	if !errors.As(err, &notFound) {
		t.Fatalf("expected *workspace.NotFoundError, got %T (%v)", err, err)
	}
}

func TestStoreListIDsReturnsEveryWorkspaceInStableOrder(t *testing.T) {
	_, db := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedWorkspace(t, db, "ws_list_zeta", "Zeta")
	seedWorkspace(t, db, "ws_list_alpha", "Alpha")
	store := workspace.NewStore(db)

	ids, err := store.ListIDs(context.Background())
	if err != nil {
		t.Fatalf("ListIDs: %v", err)
	}
	alphaIndex := indexWorkspaceID(ids, "ws_list_alpha")
	zetaIndex := indexWorkspaceID(ids, "ws_list_zeta")
	if alphaIndex < 0 || zetaIndex < 0 {
		t.Fatalf("ListIDs = %v; missing seeded workspaces", ids)
	}
	if alphaIndex >= zetaIndex {
		t.Fatalf("ListIDs = %v; want stable ID order", ids)
	}
}

func indexWorkspaceID(ids []workspace.ID, want workspace.ID) int {
	for index, id := range ids {
		if id == want {
			return index
		}
	}
	return -1
}

func seedWorkspace(t *testing.T, db interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, id workspace.ID, name string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO workspaces (id, type, name, created_at)
		 VALUES ($1, 'workspace', $2, '2026-01-01T00:00:00Z')
		 ON CONFLICT (id) DO NOTHING`,
		string(id), name,
	); err != nil {
		t.Fatalf("seed workspace %s: %v", id, err)
	}
}
