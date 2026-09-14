package storage_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

func rewindEnvironmentBuildMigration(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`ALTER TABLE environment_artifacts
		DROP COLUMN build_started_at, DROP COLUMN build_warn_at, DROP COLUMN build_deadline_at,
		DROP COLUMN build_warned_at, DROP COLUMN provider_build_ref, DROP COLUMN provider_build_state;
		DELETE FROM tetral_schema_migrations WHERE version=4`); err != nil {
		t.Fatal(err)
	}
}

func TestEnvironmentBuildMigrationPreservesFailedHistory(t *testing.T) {
	db := storagetest.NewPostgreSQLAdminDB(t)
	rewindEnvironmentBuildMigration(t, db)
	seedStorageSchemaSession(t, db, "ws_build_upgrade", "sesn_build_upgrade")
	if _, err := db.Exec(`INSERT INTO environment_artifacts
		(workspace_id, environment_id, generation, status, provider, normalized_config_hash, artifact_input_hash, runtime_network_policy_json, packages_json, failure_reason, retryable, created_at, updated_at)
		SELECT workspace_id, environment_id, 1, 'failed', 'daytona', 'config', 'packages', '{}', '{}', 'original failure', FALSE, NOW(), NOW()
		FROM sessions WHERE id='sesn_build_upgrade'`); err != nil {
		t.Fatal(err)
	}
	var before, after string
	if err := db.QueryRow(`SELECT to_jsonb(a)::text FROM environment_artifacts a`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if err := storage.MigrateSchema(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT (to_jsonb(a)-'build_started_at'-'build_warn_at'-'build_deadline_at'-'build_warned_at'-'provider_build_ref'-'provider_build_state')::text FROM environment_artifacts a`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatal("migration altered historical failure or identity")
	}
	if _, err := db.Exec(`UPDATE environment_artifacts SET build_started_at=NOW()`); err == nil {
		t.Fatal("accepted partial deadline identity")
	}
	if _, err := db.Exec(`UPDATE environment_artifacts SET provider_build_state='untrusted provider output'`); err == nil {
		t.Fatal("accepted unbounded provider state")
	}
}
