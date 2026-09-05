package session

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestPostgreSQLSessionResourceMutationAdvancesRevisionAndQueuesOneMaterialization(t *testing.T) {
	env := newSessionPostgreSQLProofEnv(t)
	seedEnvironmentArtifact(t, env.admin, workspace.DefaultID, "env_test", 1, "ready")
	seedSessionSourceFile(t, env.admin, workspace.DefaultID, "file_resource_first_source", 5)
	seedSessionSourceFile(t, env.admin, workspace.DefaultID, "file_resource_second_source", 6)
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	service := env.newService(now)
	service.sessionIDStrategy = fixedSessionIDs("sesn_resource_materialize")
	service.threadIDStrategy = fixedSessionIDs("thread_resource_materialize")
	service.resourceIDStrategy = fixedSessionIDs("sesrsc_resource_first", "sesrsc_resource_second")
	service.fileIDStrategy = fixedSessionIDs("file_resource_first_session", "file_resource_second_session")
	if _, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent: AgentReference{ID: "agent_test"}, EnvironmentID: "env_test",
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := env.admin.Exec(`INSERT INTO session_sandbox_bindings (
		workspace_id, session_id, logical_sandbox_id, environment_id,
		environment_generation, provider, provider_resource_id, binding_revision,
		materialized_resource_revision, resource_roots_json,
		provider_metadata_json, created_at, updated_at
	) VALUES ($1, 'sesn_resource_materialize', 'sbox_resource_materialize', 'env_test',
		1, 'daytona', 'provider_resource_materialize', 1, 0, '[]', '{}', $2, $2)`,
		string(workspace.DefaultID), now); err != nil {
		t.Fatalf("seed sandbox binding: %v", err)
	}

	for _, sourceFileID := range []string{"file_resource_first_source", "file_resource_second_source"} {
		if _, err := service.AddResource(context.Background(), workspace.DefaultID, "sesn_resource_materialize", ResourceRequest{
			Type: string(ResourceTypeFile), FileID: sourceFileID,
		}); err != nil {
			t.Fatalf("AddResource(%s): %v", sourceFileID, err)
		}
	}

	var revision int64
	if err := env.admin.QueryRow(`SELECT sandbox_resource_revision FROM sessions
		WHERE workspace_id = $1 AND id = 'sesn_resource_materialize'`, string(workspace.DefaultID)).Scan(&revision); err != nil {
		t.Fatalf("read resource revision: %v", err)
	}
	if revision != 3 {
		t.Fatalf("sandbox_resource_revision = %d; want 3", revision)
	}
	var operationCount int
	var targetRevision int64
	var resourcesJSON string
	if err := env.admin.QueryRow(`SELECT count(*), min(target_resource_revision), min(materialization_resources_json)
		FROM sandbox_lifecycle_operations
		WHERE workspace_id = $1 AND session_id = 'sesn_resource_materialize' AND kind = 'materialize'`,
		string(workspace.DefaultID)).Scan(&operationCount, &targetRevision, &resourcesJSON); err != nil {
		t.Fatalf("read materialization operation: %v", err)
	}
	if operationCount != 1 || targetRevision != 2 {
		t.Fatalf("materialization count/revision = %d/%d; want one immutable revision-2 operation", operationCount, targetRevision)
	}
	var snapshot map[string]any
	if err := json.Unmarshal([]byte(resourcesJSON), &snapshot); err != nil {
		t.Fatalf("decode materialization snapshot: %v", err)
	}
	files, ok := snapshot["Files"].([]any)
	if !ok || len(files) != 1 {
		t.Fatalf("materialization Files = %#v; want first mutation only", snapshot["Files"])
	}
	if got := sessionRowCount(t, env.admin, `SELECT count(*) FROM queue_jobs
		WHERE workspace_id = $1 AND kind = 'sandbox_materialize' AND status = 'pending'`, string(workspace.DefaultID)); got != 1 {
		t.Fatalf("pending materialization jobs = %d; want 1", got)
	}
}

func TestPostgreSQLGitHubRepositoryGitIdentitySurvivesPersistenceSnapshotAndRotation(t *testing.T) {
	env := newSessionPostgreSQLProofEnv(t)
	seedEnvironmentArtifact(t, env.admin, workspace.DefaultID, "env_test", 1, "ready")
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	service := env.newService(now)
	service.sessionIDStrategy = fixedSessionIDs("sesn_github_identity")
	service.threadIDStrategy = fixedSessionIDs("thread_github_identity")
	service.resourceIDStrategy = fixedSessionIDs("sesrsc_github_identity")
	const authorizationToken = "github_resource_token_identity_proof"
	if _, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent: AgentReference{ID: "agent_test"}, EnvironmentID: "env_test",
		Resources: []ResourceRequest{{
			Type:               string(ResourceTypeGitHubRepository),
			GitHubURL:          "https://github.com/tetral-ai/tetral",
			AuthorizationToken: authorizationToken,
			GitIdentity:        &GitIdentity{Name: "Example Automation", Email: "example-automation@users.noreply.github.com"},
		}},
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	var storedName, storedEmail string
	if err := env.admin.QueryRow(`SELECT git_identity_name, git_identity_email
		FROM session_github_repository_resources
		WHERE workspace_id = $1 AND session_id = 'sesn_github_identity' AND resource_id = 'sesrsc_github_identity'`,
		string(workspace.DefaultID)).Scan(&storedName, &storedEmail); err != nil {
		t.Fatalf("read stored git identity: %v", err)
	}
	if storedName != "Example Automation" || storedEmail != "example-automation@users.noreply.github.com" {
		t.Fatalf("stored git identity = %q/%q; want declared identity", storedName, storedEmail)
	}

	readback, err := service.GetResource(context.Background(), workspace.DefaultID, "sesn_github_identity", "sesrsc_github_identity")
	if err != nil {
		t.Fatalf("GetResource: %v", err)
	}
	if readback.GitIdentity == nil || readback.GitIdentity.Name != "Example Automation" ||
		readback.GitIdentity.Email != "example-automation@users.noreply.github.com" {
		t.Fatalf("readback git_identity = %+v; want declared identity", readback.GitIdentity)
	}

	if _, err := env.admin.Exec(`INSERT INTO session_sandbox_bindings (
		workspace_id, session_id, logical_sandbox_id, environment_id,
		environment_generation, provider, provider_resource_id, binding_revision,
		materialized_resource_revision, resource_roots_json,
		provider_metadata_json, created_at, updated_at
	) VALUES ($1, 'sesn_github_identity', 'sbox_github_identity', 'env_test',
		1, 'daytona', 'provider_github_identity', 1, 0, '[]', '{}', $2, $2)`,
		string(workspace.DefaultID), now); err != nil {
		t.Fatalf("seed sandbox binding: %v", err)
	}

	// Token rotation is a pure credential write: it preserves the declared
	// identity and records a fresh materialization snapshot carrying it.
	if _, err := service.UpdateResource(context.Background(), workspace.DefaultID,
		"sesn_github_identity", "sesrsc_github_identity", "github_resource_token_rotated"); err != nil {
		t.Fatalf("UpdateResource: %v", err)
	}
	if err := env.admin.QueryRow(`SELECT git_identity_name, git_identity_email
		FROM session_github_repository_resources
		WHERE workspace_id = $1 AND session_id = 'sesn_github_identity' AND resource_id = 'sesrsc_github_identity'`,
		string(workspace.DefaultID)).Scan(&storedName, &storedEmail); err != nil {
		t.Fatalf("read git identity after rotation: %v", err)
	}
	if storedName != "Example Automation" || storedEmail != "example-automation@users.noreply.github.com" {
		t.Fatalf("git identity after rotation = %q/%q; want preserved declared identity", storedName, storedEmail)
	}

	var resourcesJSON string
	if err := env.admin.QueryRow(`SELECT materialization_resources_json
		FROM sandbox_lifecycle_operations
		WHERE workspace_id = $1 AND session_id = 'sesn_github_identity' AND kind = 'materialize'`,
		string(workspace.DefaultID)).Scan(&resourcesJSON); err != nil {
		t.Fatalf("read materialization snapshot: %v", err)
	}
	if strings.Contains(resourcesJSON, authorizationToken) || strings.Contains(resourcesJSON, "github_resource_token_rotated") {
		t.Fatalf("materialization snapshot leaked credential material: %s", resourcesJSON)
	}
	var snapshot struct {
		GitHubRepositories []struct {
			ResourceID       string
			GitIdentityName  string
			GitIdentityEmail string
		}
	}
	if err := json.Unmarshal([]byte(resourcesJSON), &snapshot); err != nil {
		t.Fatalf("decode materialization snapshot: %v", err)
	}
	if len(snapshot.GitHubRepositories) != 1 {
		t.Fatalf("snapshot GitHubRepositories = %#v; want the declared mount", snapshot.GitHubRepositories)
	}
	mount := snapshot.GitHubRepositories[0]
	if mount.ResourceID != "sesrsc_github_identity" ||
		mount.GitIdentityName != "Example Automation" ||
		mount.GitIdentityEmail != "example-automation@users.noreply.github.com" {
		t.Fatalf("snapshot mount = %+v; want declared identity carried into materialization", mount)
	}
}

func TestPostgreSQLUnmaterializedResourceDeleteAdvancesRevisionWithoutMaterialization(t *testing.T) {
	env := newSessionPostgreSQLProofEnv(t)
	seedEnvironmentArtifact(t, env.admin, workspace.DefaultID, "env_test", 1, "ready")
	seedSessionSourceFile(t, env.admin, workspace.DefaultID, "file_unmaterialized_source", 5)
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	service := env.newService(now)
	service.sessionIDStrategy = fixedSessionIDs("sesn_unmaterialized_delete")
	service.threadIDStrategy = fixedSessionIDs("thread_unmaterialized_delete")
	service.resourceIDStrategy = fixedSessionIDs("sesrsc_unmaterialized_delete")
	service.fileIDStrategy = fixedSessionIDs("file_unmaterialized_session")
	if _, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent: AgentReference{ID: "agent_test"}, EnvironmentID: "env_test",
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	resource, err := service.AddResource(context.Background(), workspace.DefaultID, "sesn_unmaterialized_delete", ResourceRequest{
		Type: string(ResourceTypeFile), FileID: "file_unmaterialized_source",
	})
	if err != nil {
		t.Fatalf("AddResource: %v", err)
	}
	if _, err := service.DeleteResource(context.Background(), workspace.DefaultID, "sesn_unmaterialized_delete", resource.ID); err != nil {
		t.Fatalf("DeleteResource: %v", err)
	}

	var revision int64
	if err := env.admin.QueryRow(`SELECT sandbox_resource_revision FROM sessions
		WHERE workspace_id = $1 AND id = 'sesn_unmaterialized_delete'`, string(workspace.DefaultID)).Scan(&revision); err != nil {
		t.Fatalf("read resource revision: %v", err)
	}
	if revision != 3 {
		t.Fatalf("sandbox_resource_revision = %d; want 3 after add and delete", revision)
	}
	if got := sessionRowCount(t, env.admin, `SELECT count(*) FROM queue_jobs
		WHERE workspace_id = $1 AND kind = 'sandbox_materialize'`, string(workspace.DefaultID)); got != 0 {
		t.Fatalf("materialization jobs = %d; want none without a binding", got)
	}
}
