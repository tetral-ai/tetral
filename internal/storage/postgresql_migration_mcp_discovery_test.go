package storage_test

import (
	"context"
	"testing"

	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

func TestMCPDiscoveryMigrationPreservesExistingInput(t *testing.T) {
	db := storagetest.NewPostgreSQLAdminDB(t)
	// Recreate the V2 boundary, then place a real old-format input in it.
	if _, err := db.Exec(`ALTER TABLE session_runtime_inbox DROP COLUMN mcp_discovery_attempts, DROP COLUMN mcp_discovery_deadline_at, DROP COLUMN mcp_discovery_diagnostic;
		DELETE FROM tetral_schema_migrations WHERE version=3`); err != nil {
		t.Fatal(err)
	}
	seedStorageSchemaSession(t, db, "workspace_discovery_upgrade", "sesn_discovery_upgrade")
	if _, err := db.Exec(`INSERT INTO session_threads (workspace_id, id, session_id, role, visibility, status, created_at, last_active_at, updated_at)
		VALUES ('workspace_discovery_upgrade', 'thr_discovery_upgrade', 'sesn_discovery_upgrade', 'main', 'public', 'idle', NOW(), NOW(), NOW());
		INSERT INTO session_runtime_inbox (workspace_id, session_id, session_thread_id, runtime_input_id, input_kind, event_ids_json, sequence_from, sequence_to, status, created_at, updated_at)
		VALUES ('workspace_discovery_upgrade', 'sesn_discovery_upgrade', 'thr_discovery_upgrade', 'rin_upgrade', 'messages', '["evt_preserved"]', 1, 1, 'queued', NOW(), NOW())`); err != nil {
		t.Fatal(err)
	}
	var before, after string
	if err := db.QueryRow(`SELECT to_jsonb(i)::text FROM session_runtime_inbox i WHERE runtime_input_id='rin_upgrade'`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if err := storage.MigrateSchema(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT (to_jsonb(i)-'mcp_discovery_attempts'-'mcp_discovery_deadline_at'-'mcp_discovery_diagnostic')::text FROM session_runtime_inbox i WHERE runtime_input_id='rin_upgrade'`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatal("upgrade changed existing input identity, status, payload, or timestamps")
	}
	var defaults bool
	if err := db.QueryRow(`SELECT mcp_discovery_attempts=0 AND mcp_discovery_deadline_at IS NULL AND mcp_discovery_diagnostic IS NULL FROM session_runtime_inbox WHERE runtime_input_id='rin_upgrade'`).Scan(&defaults); err != nil {
		t.Fatal(err)
	}
	if !defaults {
		t.Fatal("old input did not receive an unused discovery budget")
	}
	for _, update := range []string{
		`UPDATE session_runtime_inbox SET mcp_discovery_attempts=-1 WHERE runtime_input_id='rin_upgrade'`,
		`UPDATE session_runtime_inbox SET mcp_discovery_attempts=1 WHERE runtime_input_id='rin_upgrade'`,
		`UPDATE session_runtime_inbox SET mcp_discovery_diagnostic='raw server response' WHERE runtime_input_id='rin_upgrade'`,
	} {
		if _, err := db.Exec(update); err == nil {
			t.Fatalf("accepted invalid budget: %s", update)
		}
	}
}
