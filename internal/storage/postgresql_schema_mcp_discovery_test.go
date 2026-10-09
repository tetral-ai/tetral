package storage_test

import (
	"testing"

	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

func TestInitialSchemaMCPDiscoveryBudgetDefaultsAndBounds(t *testing.T) {
	db := storagetest.NewPostgreSQLAdminDB(t)
	seedStorageSchemaSession(t, db, "workspace_discovery", "sesn_discovery")
	if _, err := db.Exec(`INSERT INTO session_threads (workspace_id, id, session_id, role, visibility, status, created_at, last_active_at, updated_at)
		VALUES ('workspace_discovery', 'thr_discovery', 'sesn_discovery', 'main', 'public', 'idle', NOW(), NOW(), NOW());
		INSERT INTO session_runtime_inbox (workspace_id, session_id, session_thread_id, runtime_input_id, input_kind, event_ids_json, sequence_from, sequence_to, status, created_at, updated_at)
		VALUES ('workspace_discovery', 'sesn_discovery', 'thr_discovery', 'rin_discovery', 'messages', '["evt_preserved"]', 1, 1, 'queued', NOW(), NOW())`); err != nil {
		t.Fatal(err)
	}
	var defaults bool
	if err := db.QueryRow(`SELECT mcp_discovery_attempts=0 AND mcp_discovery_deadline_at IS NULL AND mcp_discovery_diagnostic IS NULL FROM session_runtime_inbox WHERE runtime_input_id='rin_discovery'`).Scan(&defaults); err != nil {
		t.Fatal(err)
	}
	if !defaults {
		t.Fatal("new input did not receive an unused discovery budget")
	}
	for _, update := range []string{
		`UPDATE session_runtime_inbox SET mcp_discovery_attempts=-1 WHERE runtime_input_id='rin_discovery'`,
		`UPDATE session_runtime_inbox SET mcp_discovery_attempts=1 WHERE runtime_input_id='rin_discovery'`,
		`UPDATE session_runtime_inbox SET mcp_discovery_diagnostic='raw server response' WHERE runtime_input_id='rin_discovery'`,
	} {
		if _, err := db.Exec(update); err == nil {
			t.Fatalf("accepted invalid budget: %s", update)
		}
	}
}
