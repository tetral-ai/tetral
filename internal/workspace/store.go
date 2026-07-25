package workspace

import (
	"context"
	"database/sql"
	"fmt"
)

// Store reads workspace metadata from the `workspaces` system table.
// This stage has no public workspace lifecycle API, so the store is
// intentionally read-only. The `workspaces` table itself is RLS-free
// because it is the workspace catalog, not a workspace-owned table.
type Store struct {
	db *sql.DB
}

// NewStore constructs a Store backed by the given *sql.DB. The DB must already
// be initialized through storage.InitializePostgreSQLSchema, but workspace rows
// are deployment/bootstrap data and are not created by schema initialization.
func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

// Get returns the workspace with the given ID. Returns
// NotFoundError when the row does not exist.
func (s *Store) Get(ctx context.Context, id ID) (*Workspace, error) {
	var ws Workspace
	err := s.db.QueryRowContext(ctx,
		`SELECT id, type, name, created_at FROM workspaces WHERE id = $1`,
		string(id),
	).Scan(&ws.ID, &ws.Type, &ws.Name, &ws.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, &NotFoundError{Message: fmt.Sprintf("workspace %s not found", id)}
	}
	if err != nil {
		return nil, err
	}
	return &ws, nil
}

// ListIDs returns every configured workspace in deterministic order. Queue
// consumers use the catalog only for discovery; each lease and durable access
// remains scoped to the returned workspace ID.
func (s *Store) ListIDs(ctx context.Context) ([]ID, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM workspaces ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	ids := make([]ID, 0)
	for rows.Next() {
		var id ID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ids, nil
}

// NotFoundError is returned by Store.Get when the requested workspace
// does not exist. Mapped to HTTP 404 by the centralized writeError.
type NotFoundError struct {
	Message string
}

func (e *NotFoundError) Error() string { return e.Message }
