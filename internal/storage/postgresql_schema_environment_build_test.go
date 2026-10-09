package storage_test

import (
	"testing"

	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

func TestInitialSchemaEnvironmentArtifactConstraints(t *testing.T) {
	db := storagetest.NewPostgreSQLAdminDB(t)
	seedStorageSchemaSession(t, db, "ws_build", "sesn_build")
	if _, err := db.Exec(`INSERT INTO environment_artifacts
		(workspace_id, environment_id, generation, status, provider, normalized_config_hash, artifact_input_hash, runtime_network_policy_json, packages_json, failure_reason, retryable, created_at, updated_at)
		SELECT workspace_id, environment_id, 1, 'failed', 'daytona', 'config', 'packages', '{}', '{}', 'original failure', FALSE, NOW(), NOW()
		FROM sessions WHERE id='sesn_build'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE environment_artifacts SET build_started_at=NOW()`); err == nil {
		t.Fatal("accepted partial deadline identity")
	}
	if _, err := db.Exec(`UPDATE environment_artifacts SET provider_build_state='untrusted provider output'`); err == nil {
		t.Fatal("accepted unbounded provider state")
	}
}
