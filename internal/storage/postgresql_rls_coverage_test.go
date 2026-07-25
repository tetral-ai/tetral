package storage_test

import (
	"context"
	"testing"
)

// Workspace isolation has two layers: every store query predicates on
// workspace_id, and the schema forces a row-level policy so a query that
// forgets its predicate still resolves nothing. The second layer only holds if
// EVERY workspace-owned table carries it, so this test enumerates the live
// schema rather than trusting a hand-maintained list: any table that has a
// workspace_id column must have row security enabled AND forced.
func TestPostgreSQLEveryWorkspaceOwnedTableForcesRowLevelSecurity(t *testing.T) {
	db, schema := newIsolatedPostgreSQLSchemaDB(t)

	rows, err := db.QueryContext(context.Background(),
		`SELECT class.relname, class.relrowsecurity, class.relforcerowsecurity
		   FROM pg_class class
		   JOIN pg_namespace namespace ON namespace.oid = class.relnamespace
		   JOIN information_schema.columns columns
		     ON columns.table_schema = namespace.nspname
		    AND columns.table_name = class.relname
		  WHERE namespace.nspname = $1
		    AND class.relkind = 'r'
		    AND columns.column_name = 'workspace_id'
		  ORDER BY class.relname`,
		schema,
	)
	if err != nil {
		t.Fatalf("enumerate workspace-owned tables: %v", err)
	}
	defer func() { _ = rows.Close() }()

	inspected := 0
	for rows.Next() {
		var table string
		var enabled, forced bool
		if err := rows.Scan(&table, &enabled, &forced); err != nil {
			t.Fatalf("scan workspace-owned table: %v", err)
		}
		inspected++
		if !enabled || !forced {
			t.Errorf("%s has a workspace_id column but RLS enabled=%t forced=%t; add it to rlsTargets", table, enabled, forced)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate workspace-owned tables: %v", err)
	}
	if inspected == 0 {
		t.Fatal("no workspace-owned tables discovered; the enumeration query is wrong")
	}
}
