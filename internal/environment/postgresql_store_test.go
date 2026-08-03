package environment_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"

	"github.com/tetral-ai/tetral/internal/environment"
	"github.com/tetral-ai/tetral/internal/session"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func newPGEnvEnv(t *testing.T) (runtime *sql.DB, admin *sql.DB) {
	t.Helper()
	return storagetest.NewPostgreSQLDBWithAdmin(t)
}

func newPGEnvStore(runtime *sql.DB) *environment.PostgreSQLEnvironmentStore {
	return environment.NewPostgreSQLEnvironmentStore(
		dbconnect.NewClientForTesting(runtime),
		environment.WithDefaultArtifactRef("artifact_default_test"),
	)
}

func seedEnvWorkspace(t *testing.T, admin *sql.DB, ws workspace.ID, name string) {
	t.Helper()
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO workspaces (id, type, name, created_at) VALUES ($1, 'workspace', $2, '2026-01-01T00:00:00Z')
		 ON CONFLICT DO NOTHING`, string(ws), name); err != nil {
		t.Fatalf("seed workspace %s: %v", ws, err)
	}
}

func makeEnvSession(workspaceID workspace.ID, environmentID string, status session.Status) *session.Session {
	now := time.Now().UTC()
	title := "env reference"
	return &session.Session{
		ID:            "sesn_env_reference",
		Type:          "session",
		Title:         &title,
		Status:        status,
		AgentID:       "agent_env_reference",
		AgentVersion:  1,
		EnvironmentID: environmentID,
		CreatedAt:     now,
		UpdatedAt:     now,
		WorkspaceID:   workspaceID,
	}
}

func seedEnvSessionReference(ctx context.Context, sessionStore *session.PostgreSQLSessionStore, sess *session.Session) error {
	return sessionStore.WithWorkspaceTx(ctx, sess.WorkspaceID, func(tx session.Transaction) error {
		if _, err := tx.Exec(ctx,
			`INSERT INTO agents (workspace_id, id, name, version, created_at, updated_at)
			 VALUES ($1, 'agent_env_reference', 'agent env reference', 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
			 ON CONFLICT (id) DO NOTHING`,
			string(sess.WorkspaceID),
		); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at)
			 VALUES ($1, 'agv_env_reference', 'agent_env_reference', 1, '{}', 'hash', '2026-01-01T00:00:00Z')
			 ON CONFLICT (agent_id, version) DO NOTHING`,
			string(sess.WorkspaceID),
		); err != nil {
			return err
		}
		return tx.CreateSession(ctx, sess)
	})
}

func TestPostgreSQLEnvironmentStoreCreateAndGet(t *testing.T) {
	runtime, _ := newPGEnvEnv(t)
	store := newPGEnvStore(runtime)
	ctx := context.Background()
	created, err := store.Create(ctx, workspace.DefaultID, environment.CreateEnvironmentRequest{
		Name:        "prod",
		Description: "production",
		Config:      environment.EnvironmentConfig{},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	got, err := store.Get(ctx, workspace.DefaultID, created.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Name != "prod" {
		t.Errorf("Name = %q; want prod", got.Name)
	}
	if got.Description != "production" {
		t.Errorf("Description = %q; want production", got.Description)
	}
	if got.ArchivedAt != nil {
		t.Errorf("ArchivedAt = %v; want nil", got.ArchivedAt)
	}
	assertCompleteDefaultConfig(t, got.Config)
	if len(got.Metadata) != 0 {
		t.Errorf("Metadata = %v; want empty", got.Metadata)
	}
}

func TestPostgreSQLEnvironmentStorePersistsCanonicalConfigAndMetadata(t *testing.T) {
	runtime, admin := newPGEnvEnv(t)
	store := newPGEnvStore(runtime)
	ctx := context.Background()

	created, err := store.Create(ctx, workspace.DefaultID, environment.CreateEnvironmentRequest{
		Name: "canonical",
		Config: environment.EnvironmentConfig{
			Type: "cloud",
			Networking: &environment.NetworkingConfig{
				Type:             "cidr_allow_list",
				NetworkAllowList: "10.0.0.0/8",
			},
			Packages: environment.PackageMap{"pip": []string{"pandas==2.2.0"}},
		},
		Metadata: environment.StringMap{"team": "infra"},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	var configJSON, metadataJSON string
	if err := admin.QueryRowContext(ctx,
		`SELECT config_json, metadata_json FROM environments WHERE workspace_id = $1 AND id = $2`,
		string(workspace.DefaultID), created.ID,
	).Scan(&configJSON, &metadataJSON); err != nil {
		t.Fatalf("read raw environment json: %v", err)
	}

	assertJSONEqual(t, configJSON, `{"type":"cloud","networking":{"type":"cidr_allow_list","network_allow_list":"10.0.0.0/8"},"packages":{"pip":["pandas==2.2.0"]}}`)
	assertJSONEqual(t, metadataJSON, `{"team":"infra"}`)
}

func TestPostgreSQLEnvironmentStoreEmptyPackagesUseDefaultReadyArtifact(t *testing.T) {
	runtime, admin := newPGEnvEnv(t)
	store := newPGEnvStore(runtime)
	ctx := context.Background()

	created, err := store.Create(ctx, workspace.DefaultID, environment.CreateEnvironmentRequest{Name: "default-artifact"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	artifact := readEnvironmentArtifact(t, admin, workspace.DefaultID, created.ID, 1)
	if artifact.status != "ready" || artifact.providerArtifactRef != "artifact_default_test" {
		t.Fatalf("artifact = %+v; want ready default artifact", artifact)
	}
	assertJSONEqual(t, artifact.runtimeNetworkPolicyJSON, `{"type":"unrestricted"}`)
	assertJSONEqual(t, artifact.packagesJSON, `{}`)
	if got := countEnvironmentBuildJobs(t, admin, workspace.DefaultID); got != 0 {
		t.Fatalf("environment_build jobs = %d; want none for empty packages", got)
	}
}

func TestPostgreSQLEnvironmentStoreRequiresDefaultArtifactRefForEmptyPackages(t *testing.T) {
	runtime, _ := newPGEnvEnv(t)
	store := environment.NewPostgreSQLEnvironmentStore(dbconnect.NewClientForTesting(runtime))
	_, err := store.Create(context.Background(), workspace.DefaultID, environment.CreateEnvironmentRequest{Name: "missing-default"})
	var validation *environment.ValidationError
	if !errors.As(err, &validation) || validation.Message != "default environment artifact ref is required" {
		t.Fatalf("Create err = %T %v; want missing default artifact validation", err, err)
	}
}

func TestPostgreSQLEnvironmentStoreNonEmptyPackagesEnqueueBuild(t *testing.T) {
	runtime, admin := newPGEnvEnv(t)
	store := newPGEnvStore(runtime)
	ctx := context.Background()

	created, err := store.Create(ctx, workspace.DefaultID, environment.CreateEnvironmentRequest{
		Name: "custom-packages",
		Config: environment.EnvironmentConfig{
			Type:       "cloud",
			Networking: &environment.NetworkingConfig{Type: "unrestricted"},
			Packages:   environment.PackageMap{"pip": []string{"pandas==2.2.0"}},
		},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	artifact := readEnvironmentArtifact(t, admin, workspace.DefaultID, created.ID, 1)
	if artifact.status != "pending" || artifact.providerArtifactRef != "" {
		t.Fatalf("artifact = %+v; want pending without provider artifact ref", artifact)
	}
	assertJSONEqual(t, artifact.packagesJSON, `{"pip":["pandas==2.2.0"]}`)
	if got := countEnvironmentBuildJobs(t, admin, workspace.DefaultID); got != 1 {
		t.Fatalf("environment_build jobs = %d; want one for custom packages", got)
	}
}

func TestPostgreSQLEnvironmentStoreNetworkingOnlyUpdateReusesReadyArtifact(t *testing.T) {
	runtime, admin := newPGEnvEnv(t)
	store := newPGEnvStore(runtime)
	ctx := context.Background()

	created, err := store.Create(ctx, workspace.DefaultID, environment.CreateEnvironmentRequest{
		Name: "network-only",
		Config: environment.EnvironmentConfig{
			Type:       "cloud",
			Networking: &environment.NetworkingConfig{Type: "unrestricted"},
			Packages:   environment.PackageMap{"pip": []string{"pandas==2.2.0"}},
		},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	markEnvironmentArtifactReady(t, admin, workspace.DefaultID, created.ID, 1, "artifact_custom_test")

	patch, err := environment.DecodeUpdateEnvironmentRequest([]byte(`{"config":{"networking":{"type":"cidr_allow_list","network_allow_list":"10.0.0.0/8"}}}`))
	if err != nil {
		t.Fatalf("DecodeUpdateEnvironmentRequest: %v", err)
	}
	updated, err := store.Update(ctx, workspace.DefaultID, created.ID, patch)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updated.CurrentGeneration != 2 {
		t.Fatalf("CurrentGeneration = %d; want new generation for changed runtime policy", updated.CurrentGeneration)
	}

	artifact := readEnvironmentArtifact(t, admin, workspace.DefaultID, created.ID, 2)
	if artifact.status != "ready" || artifact.providerArtifactRef != "artifact_custom_test" {
		t.Fatalf("artifact = %+v; want ready reused provider artifact ref", artifact)
	}
	assertJSONEqual(t, artifact.runtimeNetworkPolicyJSON, `{"type":"cidr_allow_list","network_allow_list":"10.0.0.0/8"}`)
	assertJSONEqual(t, artifact.packagesJSON, `{"pip":["pandas==2.2.0"]}`)
	if got := countEnvironmentBuildJobs(t, admin, workspace.DefaultID); got != 1 {
		t.Fatalf("environment_build jobs = %d; want only original package build", got)
	}
}

func TestPostgreSQLEnvironmentStoreNetworkingOnlyUpdateFollowsInFlightArtifact(t *testing.T) {
	for _, status := range []string{"pending", "building"} {
		t.Run(status, func(t *testing.T) {
			runtime, admin := newPGEnvEnv(t)
			store := newPGEnvStore(runtime)
			ctx := context.Background()
			created, err := store.Create(ctx, workspace.DefaultID, environment.CreateEnvironmentRequest{
				Name: "network-in-flight-" + status,
				Config: environment.EnvironmentConfig{
					Type:       "cloud",
					Networking: &environment.NetworkingConfig{Type: "unrestricted"},
					Packages:   environment.PackageMap{"pip": []string{"pandas==2.2.0"}},
				},
			})
			if err != nil {
				t.Fatalf("Create: %v", err)
			}
			if status == "building" {
				if _, err := admin.ExecContext(ctx,
					`UPDATE environment_artifacts
					    SET status = 'building', lease_job_id = 'qjob_environment_test',
					        lease_token = 'lease_environment_test', lease_attempt_count = 1
					  WHERE workspace_id = $1 AND environment_id = $2 AND generation = 1`,
					string(workspace.DefaultID), created.ID,
				); err != nil {
					t.Fatalf("mark building: %v", err)
				}
			}
			patch, err := environment.DecodeUpdateEnvironmentRequest([]byte(`{"config":{"networking":{"type":"blocked"}}}`))
			if err != nil {
				t.Fatalf("DecodeUpdateEnvironmentRequest: %v", err)
			}
			updated, err := store.Update(ctx, workspace.DefaultID, created.ID, patch)
			if err != nil {
				t.Fatalf("Update: %v", err)
			}
			if updated.CurrentGeneration != 2 {
				t.Fatalf("CurrentGeneration = %d; want 2", updated.CurrentGeneration)
			}
			artifact := readEnvironmentArtifact(t, admin, workspace.DefaultID, created.ID, 2)
			if artifact.status != "pending" || artifact.providerArtifactRef != "" {
				t.Fatalf("follower artifact = %+v; want pending without its own build", artifact)
			}
			if got := countEnvironmentBuildJobs(t, admin, workspace.DefaultID); got != 1 {
				t.Fatalf("environment_build jobs = %d; want original job only", got)
			}
		})
	}
}

func TestPostgreSQLEnvironmentStoreUpdateMaterializesPatch(t *testing.T) {
	runtime, admin := newPGEnvEnv(t)
	store := newPGEnvStore(runtime)
	ctx := context.Background()

	created, err := store.Create(ctx, workspace.DefaultID, environment.CreateEnvironmentRequest{
		Name:        "patch",
		Description: "kept",
		Config: environment.EnvironmentConfig{
			Type:       "cloud",
			Networking: &environment.NetworkingConfig{Type: "unrestricted"},
			Packages:   environment.PackageMap{"pip": []string{"pandas==2.2.0"}},
		},
		Metadata: environment.StringMap{"keep": "yes", "remove": "x"},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	patch, err := environment.DecodeUpdateEnvironmentRequest([]byte(`{"name":"patched","metadata":{"remove":null,"new":"value"},"config":{"packages":{"npm":["typescript@5.5.0"]}}}`))
	if err != nil {
		t.Fatalf("DecodeUpdateEnvironmentRequest: %v", err)
	}
	updated, err := store.Update(ctx, workspace.DefaultID, created.ID, patch)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updated.Name != "patched" {
		t.Errorf("Name = %q; want patched", updated.Name)
	}
	if updated.Description != "kept" {
		t.Errorf("Description = %q; want kept", updated.Description)
	}
	if updated.Config.Networking == nil {
		t.Fatal("Networking is nil; want preserved unrestricted")
	}
	if updated.Config.Networking.Type != "unrestricted" {
		t.Errorf("Networking.Type = %q; want preserved unrestricted", updated.Config.Networking.Type)
	}
	if _, ok := updated.Config.Packages["pip"]; ok {
		t.Errorf("Packages preserved old pip after replacement: %v", updated.Config.Packages)
	}
	if got := updated.Config.Packages["npm"]; len(got) != 1 || got[0] != "typescript@5.5.0" {
		t.Errorf("Packages = %v; want npm replacement", updated.Config.Packages)
	}
	if updated.Metadata["keep"] != "yes" || updated.Metadata["new"] != "value" {
		t.Errorf("Metadata = %v; want keep/new", updated.Metadata)
	}
	if _, ok := updated.Metadata["remove"]; ok {
		t.Errorf("Metadata remove still present: %v", updated.Metadata)
	}

	var configJSON, metadataJSON string
	if err := admin.QueryRowContext(ctx,
		`SELECT config_json, metadata_json FROM environments WHERE workspace_id = $1 AND id = $2`,
		string(workspace.DefaultID), created.ID,
	).Scan(&configJSON, &metadataJSON); err != nil {
		t.Fatalf("read raw json: %v", err)
	}
	assertJSONEqual(t, configJSON, `{"type":"cloud","networking":{"type":"unrestricted"},"packages":{"npm":["typescript@5.5.0"]}}`)
	assertJSONEqual(t, metadataJSON, `{"keep":"yes","new":"value"}`)
}

func TestPostgreSQLEnvironmentStoreUniqueNameConflictMappedTo23505(t *testing.T) {
	runtime, _ := newPGEnvEnv(t)
	store := newPGEnvStore(runtime)
	ctx := context.Background()
	if _, err := store.Create(ctx, workspace.DefaultID, environment.CreateEnvironmentRequest{Name: "dup"}); err != nil {
		t.Fatalf("first Create: %v", err)
	}
	_, err := store.Create(ctx, workspace.DefaultID, environment.CreateEnvironmentRequest{Name: "dup"})
	if err == nil {
		t.Fatal("duplicate name must surface as ConflictError")
	}
	var c *environment.ConflictError
	if !errors.As(err, &c) {
		t.Errorf("expected *environment.ConflictError, got %T (%v)", err, err)
	}
}

func TestPostgreSQLEnvironmentStoreCrossWorkspaceCannotSeeRow(t *testing.T) {
	runtime, admin := newPGEnvEnv(t)
	seedEnvWorkspace(t, admin, "workspace_b", "B")
	store := newPGEnvStore(runtime)
	ctx := context.Background()
	bEnv, err := store.Create(ctx, "workspace_b", environment.CreateEnvironmentRequest{Name: "b"})
	if err != nil {
		t.Fatalf("Create workspace_b: %v", err)
	}
	_, err = store.Get(ctx, workspace.DefaultID, bEnv.ID)
	if err == nil {
		t.Fatal("default must not Get workspace_b env")
	}
	var nf *environment.NotFoundError
	if !errors.As(err, &nf) {
		t.Errorf("expected *environment.NotFoundError, got %T (%v)", err, err)
	}
	// Even the same name across workspaces is allowed (uniqueness is
	// per workspace, not global).
	if _, err := store.Create(ctx, workspace.DefaultID, environment.CreateEnvironmentRequest{Name: "b"}); err != nil {
		t.Errorf("same name across different workspaces must succeed: %v", err)
	}
}

func TestPostgreSQLEnvironmentStoreUpdateDoesNotRequireVersion(t *testing.T) {
	runtime, _ := newPGEnvEnv(t)
	store := newPGEnvStore(runtime)
	ctx := context.Background()
	created, err := store.Create(ctx, workspace.DefaultID, environment.CreateEnvironmentRequest{Name: "v"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, err = environment.DecodeUpdateEnvironmentRequest([]byte(`{"version":2}`))
	if err == nil {
		t.Fatal("version field must be rejected before store update")
	}
	var validation *environment.ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("expected *environment.ValidationError, got %T (%v)", err, err)
	}
	patch, err := environment.DecodeUpdateEnvironmentRequest([]byte(`{"name":"new"}`))
	if err != nil {
		t.Fatalf("DecodeUpdateEnvironmentRequest: %v", err)
	}
	updated, err := store.Update(ctx, workspace.DefaultID, created.ID, patch)
	if err != nil {
		t.Fatalf("Update without version: %v", err)
	}
	if updated.Name != "new" {
		t.Errorf("Name = %q; want new", updated.Name)
	}
}

func TestPostgreSQLEnvironmentStoreArchiveIsIdempotentAndReadOnly(t *testing.T) {
	runtime, _ := newPGEnvEnv(t)
	store := newPGEnvStore(runtime)
	ctx := context.Background()
	created, err := store.Create(ctx, workspace.DefaultID, environment.CreateEnvironmentRequest{Name: "archive"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	archived, err := store.Archive(ctx, workspace.DefaultID, created.ID)
	if err != nil {
		t.Fatalf("Archive: %v", err)
	}
	if archived.ArchivedAt == nil {
		t.Fatal("ArchivedAt is nil; want archive timestamp")
	}
	again, err := store.Archive(ctx, workspace.DefaultID, created.ID)
	if err != nil {
		t.Fatalf("Archive again: %v", err)
	}
	if again.ArchivedAt == nil || !again.ArchivedAt.Equal(*archived.ArchivedAt) {
		t.Fatalf("Archive changed timestamp: first=%v second=%v", archived.ArchivedAt, again.ArchivedAt)
	}
	got, err := store.Get(ctx, workspace.DefaultID, created.ID)
	if err != nil {
		t.Fatalf("Get archived: %v", err)
	}
	if got.ArchivedAt == nil {
		t.Fatal("Get archived returned nil ArchivedAt")
	}
	patch, err := environment.DecodeUpdateEnvironmentRequest([]byte(`{"name":"blocked"}`))
	if err != nil {
		t.Fatalf("DecodeUpdateEnvironmentRequest: %v", err)
	}
	_, err = store.Update(ctx, workspace.DefaultID, created.ID, patch)
	if err == nil {
		t.Fatal("Update archived environment must fail")
	}
	var validation *environment.ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("expected *environment.ValidationError, got %T (%v)", err, err)
	}
}

func TestPostgreSQLEnvironmentStoreUpdateRechecksArchiveAfterStaleRead(t *testing.T) {
	runtime, _ := newPGEnvEnv(t)
	store := newPGEnvStore(runtime)
	ctx := context.Background()

	created, err := store.Create(ctx, workspace.DefaultID, environment.CreateEnvironmentRequest{Name: "stale-read"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	stale, err := store.Get(ctx, workspace.DefaultID, created.ID)
	if err != nil {
		t.Fatalf("precondition Get: %v", err)
	}
	if stale.ArchivedAt != nil {
		t.Fatal("precondition Get returned archived environment")
	}
	if _, err := store.Archive(ctx, workspace.DefaultID, created.ID); err != nil {
		t.Fatalf("Archive: %v", err)
	}
	patch, err := environment.DecodeUpdateEnvironmentRequest([]byte(`{"name":"must-not-commit"}`))
	if err != nil {
		t.Fatalf("DecodeUpdateEnvironmentRequest: %v", err)
	}

	_, err = store.Update(ctx, workspace.DefaultID, created.ID, patch)
	if err == nil {
		t.Fatal("Update after archive must fail even when a caller previously observed the row as live")
	}
	var validation *environment.ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("expected *environment.ValidationError, got %T (%v)", err, err)
	}
	got, err := store.Get(ctx, workspace.DefaultID, created.ID)
	if err != nil {
		t.Fatalf("Get after rejected update: %v", err)
	}
	if got.Name != "stale-read" {
		t.Fatalf("archived environment was mutated to %q", got.Name)
	}
}

func TestPostgreSQLEnvironmentStoreListPaginationScopedByWorkspace(t *testing.T) {
	runtime, admin := newPGEnvEnv(t)
	seedEnvWorkspace(t, admin, "workspace_b", "B")
	store := newPGEnvStore(runtime)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if _, err := store.Create(ctx, workspace.DefaultID, environment.CreateEnvironmentRequest{
			Name: "default-" + string(rune('a'+i)),
		}); err != nil {
			t.Fatalf("Create default %d: %v", i, err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := store.Create(ctx, "workspace_b", environment.CreateEnvironmentRequest{
			Name: "b-" + string(rune('a'+i)),
		}); err != nil {
			t.Fatalf("Create workspace_b %d: %v", i, err)
		}
	}
	dResults, err := store.List(ctx, workspace.DefaultID, environment.ListOptions{Limit: 100})
	if err != nil {
		t.Fatalf("List default: %v", err)
	}
	if len(dResults.Data) != 3 {
		t.Errorf("default workspace List size = %d; want 3", len(dResults.Data))
	}
	bResults, err := store.List(ctx, "workspace_b", environment.ListOptions{Limit: 100})
	if err != nil {
		t.Fatalf("List workspace_b: %v", err)
	}
	if len(bResults.Data) != 2 {
		t.Errorf("workspace_b List size = %d; want 2", len(bResults.Data))
	}
}

func TestPostgreSQLEnvironmentStoreCrossWorkspacePageTokenRejected(t *testing.T) {
	runtime, admin := newPGEnvEnv(t)
	seedEnvWorkspace(t, admin, "workspace_b", "B")
	store := newPGEnvStore(runtime)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := store.Create(ctx, "workspace_b", environment.CreateEnvironmentRequest{Name: "b-" + string(rune('a'+i))}); err != nil {
			t.Fatalf("Create workspace_b %d: %v", i, err)
		}
	}
	page, err := store.List(ctx, "workspace_b", environment.ListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("List workspace_b: %v", err)
	}
	if page.NextPage == nil {
		t.Fatal("workspace_b page returned nil token; want next page token")
	}
	_, err = store.List(ctx, workspace.DefaultID, environment.ListOptions{Limit: 100, Page: *page.NextPage})
	if err == nil {
		t.Fatal("default workspace must not list using workspace_b cursor")
	}
	var validation *environment.ValidationError
	if !errors.As(err, &validation) {
		t.Errorf("expected *environment.ValidationError, got %T (%v)", err, err)
	}
}

func TestPostgreSQLEnvironmentStoreCrossWorkspaceDeleteIsNotFound(t *testing.T) {
	runtime, admin := newPGEnvEnv(t)
	seedEnvWorkspace(t, admin, "workspace_b", "B")
	store := newPGEnvStore(runtime)
	ctx := context.Background()
	bEnv, err := store.Create(ctx, "workspace_b", environment.CreateEnvironmentRequest{Name: "b"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, err = store.Delete(ctx, workspace.DefaultID, bEnv.ID)
	if err == nil {
		t.Fatal("default must not Delete workspace_b env")
	}
	var notFound *environment.NotFoundError
	if !errors.As(err, &notFound) {
		t.Fatalf("Delete cross-workspace error = %T %v; want NotFoundError", err, err)
	}
}

func TestPostgreSQLEnvironmentStoreDeleteReturnsDeletedObjectAndRemovesRow(t *testing.T) {
	runtime, _ := newPGEnvEnv(t)
	store := newPGEnvStore(runtime)
	ctx := context.Background()

	created, err := store.Create(ctx, workspace.DefaultID, environment.CreateEnvironmentRequest{Name: "delete-free"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	deleted, err := store.Delete(ctx, workspace.DefaultID, created.ID)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if deleted.ID != created.ID || deleted.Type != "environment_deleted" {
		t.Fatalf("Delete result = %+v; want id/type environment_deleted", deleted)
	}
	_, err = store.Get(ctx, workspace.DefaultID, created.ID)
	if err == nil {
		t.Fatal("Get after delete must fail")
	}
	var notFound *environment.NotFoundError
	if !errors.As(err, &notFound) {
		t.Fatalf("expected *environment.NotFoundError, got %T (%v)", err, err)
	}
	list, err := store.List(ctx, workspace.DefaultID, environment.ListOptions{Limit: 100})
	if err != nil {
		t.Fatalf("List after delete: %v", err)
	}
	for _, env := range list.Data {
		if env.ID == created.ID {
			t.Fatalf("deleted environment still listed: %+v", env)
		}
	}
}

func TestPostgreSQLEnvironmentStoreListArchiveFiltering(t *testing.T) {
	runtime, _ := newPGEnvEnv(t)
	store := newPGEnvStore(runtime)
	ctx := context.Background()

	live, err := store.Create(ctx, workspace.DefaultID, environment.CreateEnvironmentRequest{Name: "live"})
	if err != nil {
		t.Fatalf("Create live: %v", err)
	}
	archived, err := store.Create(ctx, workspace.DefaultID, environment.CreateEnvironmentRequest{Name: "archived"})
	if err != nil {
		t.Fatalf("Create archived: %v", err)
	}
	if _, err := store.Archive(ctx, workspace.DefaultID, archived.ID); err != nil {
		t.Fatalf("Archive: %v", err)
	}

	defaultList, err := store.List(ctx, workspace.DefaultID, environment.ListOptions{Limit: 100})
	if err != nil {
		t.Fatalf("List default: %v", err)
	}
	if len(defaultList.Data) != 1 || defaultList.Data[0].ID != live.ID {
		t.Fatalf("default list = %v; want only live environment %s", environmentIDs(defaultList.Data), live.ID)
	}

	withArchived, err := store.List(ctx, workspace.DefaultID, environment.ListOptions{Limit: 100, IncludeArchived: true})
	if err != nil {
		t.Fatalf("List include archived: %v", err)
	}
	if len(withArchived.Data) != 2 {
		t.Fatalf("include_archived list size = %d; want 2 (%v)", len(withArchived.Data), environmentIDs(withArchived.Data))
	}
}

func TestPostgreSQLEnvironmentStoreListPageTokensAreStableAndBoundToFilter(t *testing.T) {
	runtime, _ := newPGEnvEnv(t)
	store := newPGEnvStore(runtime)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := store.Create(ctx, workspace.DefaultID, environment.CreateEnvironmentRequest{Name: "page-" + string(rune('a'+i))}); err != nil {
			t.Fatalf("Create page env %d: %v", i, err)
		}
	}

	first, err := store.List(ctx, workspace.DefaultID, environment.ListOptions{Limit: 2})
	if err != nil {
		t.Fatalf("List first page: %v", err)
	}
	if len(first.Data) != 2 {
		t.Fatalf("first page size = %d; want 2", len(first.Data))
	}
	if first.NextPage == nil {
		t.Fatal("first page NextPage is nil; want token")
	}
	second, err := store.List(ctx, workspace.DefaultID, environment.ListOptions{Limit: 2, Page: *first.NextPage})
	if err != nil {
		t.Fatalf("List second page: %v", err)
	}
	if len(second.Data) != 1 {
		t.Fatalf("second page size = %d; want 1", len(second.Data))
	}
	for _, left := range first.Data {
		for _, right := range second.Data {
			if left.ID == right.ID {
				t.Fatalf("environment %s appeared on both pages", left.ID)
			}
		}
	}
	if second.NextPage != nil {
		t.Fatalf("second page NextPage = %q; want nil", *second.NextPage)
	}

	_, err = store.List(ctx, workspace.DefaultID, environment.ListOptions{Limit: 2, IncludeArchived: true, Page: *first.NextPage})
	if err == nil {
		t.Fatal("token from include_archived=false must reject when reused with include_archived=true")
	}
	var validation *environment.ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("expected *environment.ValidationError, got %T (%v)", err, err)
	}
}

func TestPostgreSQLEnvironmentStoreListRejectsMalformedPageToken(t *testing.T) {
	runtime, _ := newPGEnvEnv(t)
	store := newPGEnvStore(runtime)
	ctx := context.Background()

	_, err := store.List(ctx, workspace.DefaultID, environment.ListOptions{Limit: 2, Page: "not-a-page-token"})
	if err == nil {
		t.Fatal("malformed page token must reject")
	}
	var validation *environment.ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("expected *environment.ValidationError, got %T (%v)", err, err)
	}
}

func TestPostgreSQLEnvironmentStoreListUsesSequenceTokenAfterArchivedOrDeletedCursorRow(t *testing.T) {
	for _, mode := range []string{"archive", "delete"} {
		t.Run(mode, func(t *testing.T) {
			runtime, _ := newPGEnvEnv(t)
			store := newPGEnvStore(runtime)
			ctx := context.Background()

			names := []string{mode + "-a", mode + "-b", mode + "-c"}
			var created []*environment.Environment
			for _, name := range names {
				env, err := store.Create(ctx, workspace.DefaultID, environment.CreateEnvironmentRequest{Name: name})
				if err != nil {
					t.Fatalf("Create %s: %v", name, err)
				}
				created = append(created, env)
			}
			first, err := store.List(ctx, workspace.DefaultID, environment.ListOptions{Limit: 1})
			if err != nil {
				t.Fatalf("List first page: %v", err)
			}
			if len(first.Data) != 1 || first.Data[0].ID != created[0].ID || first.NextPage == nil {
				t.Fatalf("first page = %v next=%v; want first env and next token", environmentIDs(first.Data), first.NextPage)
			}
			switch mode {
			case "archive":
				if _, err := store.Archive(ctx, workspace.DefaultID, created[0].ID); err != nil {
					t.Fatalf("Archive first row: %v", err)
				}
			case "delete":
				if _, err := store.Delete(ctx, workspace.DefaultID, created[0].ID); err != nil {
					t.Fatalf("Delete first row: %v", err)
				}
			}
			second, err := store.List(ctx, workspace.DefaultID, environment.ListOptions{Limit: 2, Page: *first.NextPage})
			if err != nil {
				t.Fatalf("List second page after %s: %v", mode, err)
			}
			got := environmentIDs(second.Data)
			want := []string{created[1].ID, created[2].ID}
			if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
				t.Fatalf("second page after %s = %v; want %v", mode, got, want)
			}
		})
	}
}

func TestPostgreSQLEnvironmentStoreDeleteRejectsSameWorkspaceSessionReferences(t *testing.T) {
	for _, status := range []session.Status{session.StatusIdle, session.StatusRunning, session.StatusTerminated} {
		t.Run(string(status), func(t *testing.T) {
			runtime, _ := newPGEnvEnv(t)
			environmentStore := newPGEnvStore(runtime)
			sessionStore := session.NewPostgreSQLSessionStore(dbconnect.NewClientForTesting(runtime))
			ctx := context.Background()

			created, err := environmentStore.Create(ctx, workspace.DefaultID, environment.CreateEnvironmentRequest{Name: "referenced-" + string(status)})
			if err != nil {
				t.Fatalf("Create environment: %v", err)
			}
			if err := seedEnvSessionReference(ctx, sessionStore, makeEnvSession(workspace.DefaultID, created.ID, status)); err != nil {
				t.Fatalf("Create session reference: %v", err)
			}

			_, err = environmentStore.Delete(ctx, workspace.DefaultID, created.ID)
			if err == nil {
				t.Fatal("Delete referenced environment must fail")
			}
			var validation *environment.ValidationError
			if !errors.As(err, &validation) {
				t.Fatalf("expected *environment.ValidationError, got %T (%v)", err, err)
			}
		})
	}
}

func TestPostgreSQLEnvironmentStoreDeleteRejectsDurableSessionReference(t *testing.T) {
	runtime, _ := newPGEnvEnv(t)
	environmentStore := newPGEnvStore(runtime)
	sessionStore := session.NewPostgreSQLSessionStore(dbconnect.NewClientForTesting(runtime))
	ctx := context.Background()

	created, err := environmentStore.Create(ctx, workspace.DefaultID, environment.CreateEnvironmentRequest{Name: "referenced"})
	if err != nil {
		t.Fatalf("Create environment: %v", err)
	}
	sess := makeEnvSession(workspace.DefaultID, created.ID, session.StatusIdle)
	sess.ID = "sesn_env_reference_delete"
	if err := seedEnvSessionReference(ctx, sessionStore, sess); err != nil {
		t.Fatalf("Create session reference: %v", err)
	}

	_, err = environmentStore.Delete(ctx, workspace.DefaultID, created.ID)
	var validation *environment.ValidationError
	if !errors.As(err, &validation) || validation.Message != "environment has session references" {
		t.Fatalf("Delete err = %T %v; want safe session reference validation", err, err)
	}
}

func TestPostgreSQLEnvironmentStoreDeleteIgnoresOtherWorkspaceSessionReferences(t *testing.T) {
	runtime, admin := newPGEnvEnv(t)
	seedEnvWorkspace(t, admin, "workspace_b", "B")
	environmentStore := newPGEnvStore(runtime)
	sessionStore := session.NewPostgreSQLSessionStore(dbconnect.NewClientForTesting(runtime))
	ctx := context.Background()

	defaultEnv, err := environmentStore.Create(ctx, workspace.DefaultID, environment.CreateEnvironmentRequest{Name: "default-free"})
	if err != nil {
		t.Fatalf("Create default environment: %v", err)
	}
	bEnv, err := environmentStore.Create(ctx, "workspace_b", environment.CreateEnvironmentRequest{Name: "b-referenced"})
	if err != nil {
		t.Fatalf("Create workspace_b environment: %v", err)
	}
	if err := seedEnvSessionReference(ctx, sessionStore, makeEnvSession("workspace_b", bEnv.ID, session.StatusIdle)); err != nil {
		t.Fatalf("Create workspace_b session reference: %v", err)
	}

	if _, err := environmentStore.Delete(ctx, workspace.DefaultID, defaultEnv.ID); err != nil {
		t.Fatalf("Delete default environment blocked by another workspace: %v", err)
	}
}

func assertJSONEqual(t *testing.T, got string, want string) {
	t.Helper()
	var gotValue any
	if err := json.Unmarshal([]byte(got), &gotValue); err != nil {
		t.Fatalf("got JSON invalid: %v (%s)", err, got)
	}
	var wantValue any
	if err := json.Unmarshal([]byte(want), &wantValue); err != nil {
		t.Fatalf("want JSON invalid: %v (%s)", err, want)
	}
	gotBytes, _ := json.Marshal(gotValue)
	wantBytes, _ := json.Marshal(wantValue)
	if string(gotBytes) != string(wantBytes) {
		t.Fatalf("JSON = %s; want %s", gotBytes, wantBytes)
	}
}

type environmentArtifactTestRow struct {
	status                   string
	providerArtifactRef      string
	runtimeNetworkPolicyJSON string
	packagesJSON             string
}

func readEnvironmentArtifact(t *testing.T, db *sql.DB, ws workspace.ID, environmentID string, generation int64) environmentArtifactTestRow {
	t.Helper()
	var row environmentArtifactTestRow
	var providerRef sql.NullString
	if err := db.QueryRowContext(context.Background(),
		`SELECT status, provider_artifact_ref, runtime_network_policy_json, packages_json
		   FROM environment_artifacts
		  WHERE workspace_id = $1 AND environment_id = $2 AND generation = $3`,
		string(ws), environmentID, generation,
	).Scan(&row.status, &providerRef, &row.runtimeNetworkPolicyJSON, &row.packagesJSON); err != nil {
		t.Fatalf("read environment artifact generation %d: %v", generation, err)
	}
	if providerRef.Valid {
		row.providerArtifactRef = providerRef.String
	}
	return row
}

func markEnvironmentArtifactReady(t *testing.T, db *sql.DB, ws workspace.ID, environmentID string, generation int64, providerArtifactRef string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`UPDATE environment_artifacts
		    SET status = 'ready',
		        provider_artifact_ref = $4,
		        updated_at = '2026-01-01T00:00:00Z'
		  WHERE workspace_id = $1 AND environment_id = $2 AND generation = $3`,
		string(ws), environmentID, generation, providerArtifactRef,
	); err != nil {
		t.Fatalf("mark environment artifact ready: %v", err)
	}
}

func countEnvironmentBuildJobs(t *testing.T, db *sql.DB, ws workspace.ID) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM queue_jobs WHERE workspace_id = $1 AND kind = 'environment_build'`,
		string(ws),
	).Scan(&count); err != nil {
		t.Fatalf("count environment build jobs: %v", err)
	}
	return count
}

func assertCompleteDefaultConfig(t *testing.T, cfg environment.EnvironmentConfig) {
	t.Helper()
	if cfg.Type != "cloud" {
		t.Errorf("Config.Type = %q; want cloud", cfg.Type)
	}
	if cfg.Networking == nil {
		t.Fatal("Networking is nil; want unrestricted object")
	}
	if cfg.Networking.Type != "unrestricted" {
		t.Errorf("Networking.Type = %q; want unrestricted", cfg.Networking.Type)
	}
	if cfg.Networking.NetworkAllowList != "" {
		t.Errorf("Networking = %+v; want unrestricted with no network allow list", cfg.Networking)
	}
	if len(cfg.Packages) != 0 {
		t.Errorf("Packages = %v; want empty", cfg.Packages)
	}
}

func environmentIDs(items []*environment.Environment) []string {
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.ID)
	}
	return ids
}
