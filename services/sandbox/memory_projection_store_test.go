package tetralsandbox

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
)

func TestPostgreSQLSandboxMemoryProjectionSettlesDetachedStoreWithoutRetry(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	now := time.Now().UTC()
	const writeID = "evt_memory_projection_detached"
	if _, err := adminDB.Exec(`INSERT INTO session_runtime_tool_results (
		workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
		normalized_input_hash, tool_name, input_json, ack_status, result_json,
		memory_projection_state, created_at, updated_at
	) VALUES (
		'ws_execution_store', 'sesn_execution_store', 'thr_execution_store', $1,
		'memory', 'hash_projection_detached', 'memory',
		'{"action":"create","path":"note.md","content":"x"}', 'committed',
		'{"status":"completed","action":"create","path":"/note.md"}', 'pending', $2, $2
	)`, writeID, now); err != nil {
		t.Fatalf("seed detached memory projection: %v", err)
	}

	store := NewPostgreSQLSandboxMemoryProjectionStore(dbconnect.NewClientForTesting(runtimeDB))
	work, current, err := store.LoadProjection(sandboxTestQueueContext(t, runtimeDB), SandboxMemoryProjectionJob{
		WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store",
		MemoryStoreID: "memstore_detached", MemoryWriteID: writeID,
	})
	if err != nil || current || work.MemoryWriteID != writeID {
		t.Fatalf("LoadProjection = (%+v,%t,%v); want terminal non-current work", work, current, err)
	}
	var state, resultJSON string
	if err := adminDB.QueryRow(`SELECT memory_projection_state, result_json
		FROM session_runtime_tool_results WHERE workspace_id='ws_execution_store' AND tool_use_event_id=$1`, writeID).Scan(&state, &resultJSON); err != nil {
		t.Fatalf("read detached projection: %v", err)
	}
	if state != "failed" || !strings.Contains(resultJSON, "memory store is no longer attached") {
		t.Fatalf("detached projection = %s/%s; want terminal failure", state, resultJSON)
	}
}

func TestPostgreSQLSandboxMemoryProjectionSettlesDetachedStoreAfterReplacement(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	now := time.Now().UTC()
	const writeID = "evt_memory_projection_replaced"
	if _, err := adminDB.Exec(`INSERT INTO session_runtime_tool_results (
		workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
		normalized_input_hash, tool_name, input_json, ack_status, result_json,
		memory_projection_state, created_at, updated_at
	) VALUES (
		'ws_execution_store', 'sesn_execution_store', 'thr_execution_store', $1,
		'memory', 'hash_projection_replaced', 'memory',
		'{"action":"create","path":"note.md","content":"x"}', 'committed',
		'{"status":"completed","action":"create","path":"/note.md"}', 'pending', $2, $2
	)`, writeID, now); err != nil {
		t.Fatalf("seed replaced memory projection: %v", err)
	}
	seedSandboxWritableMemoryStore(t, adminDB, "memstore_replacement", "res_memory_replacement", now)

	store := NewPostgreSQLSandboxMemoryProjectionStore(dbconnect.NewClientForTesting(runtimeDB))
	work, current, err := store.LoadProjection(sandboxTestQueueContext(t, runtimeDB), SandboxMemoryProjectionJob{
		WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store",
		MemoryStoreID: "memstore_detached", MemoryWriteID: writeID,
	})
	if err != nil || current || work.MemoryWriteID != writeID {
		t.Fatalf("LoadProjection = (%+v,%t,%v); want terminal non-current work", work, current, err)
	}
	var state string
	if err := adminDB.QueryRow(`SELECT memory_projection_state FROM session_runtime_tool_results
		WHERE workspace_id='ws_execution_store' AND tool_use_event_id=$1`, writeID).Scan(&state); err != nil {
		t.Fatalf("read replaced projection: %v", err)
	}
	if state != "failed" {
		t.Fatalf("replaced projection state = %q; want failed", state)
	}
}

func seedSandboxWritableMemoryStore(t *testing.T, db *sql.DB, storeID string, resourceID string, now time.Time) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO memory_stores (workspace_id, memory_store_id, name, created_at, updated_at)
		VALUES ('ws_execution_store', $1, $1, $2, $2)`, storeID, now); err != nil {
		t.Fatalf("seed replacement memory store: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO session_resources (
		workspace_id, session_id, resource_id, type, created_at, updated_at
	) VALUES ('ws_execution_store', 'sesn_execution_store', $1, 'memory_store', $2, $2)`, resourceID, now); err != nil {
		t.Fatalf("seed replacement session resource: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO session_memory_store_resources (
		workspace_id, session_id, resource_id, memory_store_id, access, name, mount_path
	) VALUES ('ws_execution_store', 'sesn_execution_store', $1, $2, 'read_write', 'memory', '/mnt/memory')`, resourceID, storeID); err != nil {
		t.Fatalf("seed replacement memory binding: %v", err)
	}
}
