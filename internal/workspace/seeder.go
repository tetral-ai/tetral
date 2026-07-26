package workspace

import (
	"context"
	"database/sql"
)

// Seeder creates deployment-owned workspace catalog rows without widening the
// serving Store's read-only capability.
type Seeder struct {
	db *sql.DB
}

// NewSeeder constructs a workspace bootstrap writer.
func NewSeeder(db *sql.DB) *Seeder {
	return &Seeder{db: db}
}

// Seed inserts a workspace once and reports whether this call created it.
// Existing rows are left unchanged.
func (s *Seeder) Seed(ctx context.Context, id ID, name string) (bool, error) {
	result, err := s.db.ExecContext(ctx,
		`INSERT INTO workspaces (id, type, name, created_at)
		 VALUES ($1, 'workspace', $2, now())
		 ON CONFLICT (id) DO NOTHING`,
		id,
		name,
	)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows == 1, nil
}
