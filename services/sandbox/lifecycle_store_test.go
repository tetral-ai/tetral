package tetralsandbox

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/workspace"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

func TestPostgreSQLSandboxLifecycleConvergesBeforeReenqueueingExecution(t *testing.T) {
	runtimeDB, adminDB := newReleaseHandlerTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := context.Background()
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	coordinator.clock = func() time.Time { return now }
	if _, err := adminDB.Exec(`INSERT INTO session_resources (
		workspace_id, session_id, resource_id, type, created_at, updated_at, delete_requested_at
	) VALUES ('ws_execution_store', 'sesn_execution_store', 'sesrsc_deleted_repo',
		'github_repository', $1, $1, $1)`, now); err != nil {
		t.Fatalf("seed deleted repository resource: %v", err)
	}
	if _, err := adminDB.Exec(`INSERT INTO session_github_repository_resources (
		workspace_id, session_id, resource_id, url, mount_path, authorization_token_encrypted
	) VALUES ('ws_execution_store', 'sesn_execution_store', 'sesrsc_deleted_repo',
		'https://github.com/tetral-ai/deleted', '/workspace/deleted', 'encrypted-token')`); err != nil {
		t.Fatalf("seed deleted repository resource detail: %v", err)
	}
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}

	activationJob := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_activation_operation_id")
	lifecycle := NewPostgreSQLSandboxLifecycleStore(
		dbconnect.NewClientForTesting(runtimeDB),
		&fixedSandboxResourceSource{resources: sandbox.ResourceSetup{
			ResourceCredExpiresAt: ptrTime(now.Add(2 * time.Hour)),
			ResourceRootsJSON:     `[{"path":"/mnt/session/uploads/a","mode":"read"}]`,
			DeletedGitHubRepositories: []sandbox.GitHubRepositoryMount{{
				ResourceID: "sesrsc_deleted_repo", URL: "https://github.com/tetral-ai/deleted",
				MountPath: "/workspace/deleted",
			}},
		}},
		30*time.Minute,
	)
	activation, current, err := lifecycle.ClaimActivation(ctx, activationJob, now)
	if err != nil {
		t.Fatalf("ClaimActivation: %v", err)
	}
	if !current || activation.Kind != ActivationCreate {
		t.Fatalf("activation = %+v current=%t; want current create", activation, current)
	}
	if err := lifecycle.CompleteActivation(ctx, activation, sandbox.ProviderHandle{
		Provider: "daytona", SandboxID: "provider_execution_store",
	}, now.Add(time.Second)); err != nil {
		t.Fatalf("CompleteActivation: %v", err)
	}
	assertSandboxExecutionState(t, adminDB, "evt_execution_a", "waiting_materialization", 1)
	assertQueueJobCount(t, adminDB, "ws_execution_store", "sandbox_tool_execute", 0)

	materializationJob := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_materialization_operation_id")
	materialization, current, err := lifecycle.ClaimMaterialization(ctx, materializationJob, now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("ClaimMaterialization: %v", err)
	}
	if !current || materialization.Handle.SandboxID != "provider_execution_store" {
		t.Fatalf("materialization = %+v current=%t", materialization, current)
	}
	if err := lifecycle.CompleteMaterialization(ctx, materialization, MaterializationResult{
		MaterializedEnvironmentGeneration: materialization.TargetEnvironmentGeneration,
		MaterializedResourceRevision:      materialization.TargetResourceRevision,
		Resources: sandbox.ResourceSetup{
			ResourceCredExpiresAt: ptrTime(now.Add(2 * time.Hour)),
			ResourceRootsJSON:     `[{"path":"/mnt/session/uploads/a","mode":"read"}]`,
		},
	}, now.Add(3*time.Second)); err != nil {
		t.Fatalf("CompleteMaterialization: %v", err)
	}
	assertSandboxExecutionState(t, adminDB, "evt_execution_a", "pending", 2)
	assertQueueJobCount(t, adminDB, "ws_execution_store", "sandbox_tool_execute", 1)

	var materializedRevision int64
	var desiredRevision int64
	var credentialExpiresAt, helperVerifiedAt sql.NullTime
	if err := adminDB.QueryRow(`SELECT materialized_resource_revision,
		resource_credential_expires_at, helper_verified_at
		FROM session_sandbox_bindings
		WHERE workspace_id = 'ws_execution_store' AND session_id = 'sesn_execution_store'`).Scan(
		&materializedRevision, &credentialExpiresAt, &helperVerifiedAt,
	); err != nil {
		t.Fatalf("read binding receipt: %v", err)
	}
	if err := adminDB.QueryRow(`SELECT sandbox_resource_revision FROM sessions
		WHERE workspace_id = 'ws_execution_store' AND id = 'sesn_execution_store'`).Scan(&desiredRevision); err != nil {
		t.Fatalf("read desired resource revision: %v", err)
	}
	if materializedRevision != desiredRevision || !credentialExpiresAt.Valid || !helperVerifiedAt.Valid {
		t.Fatalf("binding materialization receipt = revision %d/%d credential=%t helper=%t", materializedRevision, desiredRevision, credentialExpiresAt.Valid, helperVerifiedAt.Valid)
	}
	var detachedAt sql.NullTime
	if err := adminDB.QueryRow(`SELECT detached_at FROM session_resources
		WHERE workspace_id = 'ws_execution_store' AND session_id = 'sesn_execution_store'
		  AND resource_id = 'sesrsc_deleted_repo'`).Scan(&detachedAt); err != nil {
		t.Fatalf("read deleted resource: %v", err)
	}
	if !detachedAt.Valid {
		t.Fatal("deleted resource remained attached after successful materialization")
	}
}

func TestPostgreSQLSandboxLifecycleReleaseFencePreventsActivationClaim(t *testing.T) {
	runtimeDB, adminDB := newReleaseHandlerTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := context.Background()
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	job := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_activation_operation_id")
	if _, err := adminDB.Exec(`UPDATE session_sandbox_bindings
		SET release_requested_at = $1, release_reason = 'explicit_release'
		WHERE workspace_id = $2 AND session_id = $3`, now, job.WorkspaceID, job.SessionID); err != nil {
		t.Fatalf("set release fence: %v", err)
	}
	lifecycle := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
	_, current, err := lifecycle.ClaimActivation(ctx, job, now)
	if err != nil {
		t.Fatalf("ClaimActivation: %v", err)
	}
	if current {
		t.Fatal("release-fenced activation was claimed")
	}
}

func TestPostgreSQLSandboxLifecycleActivationCompletesAcrossPostCallReleaseFence(t *testing.T) {
	runtimeDB, adminDB := newReleaseHandlerTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := context.Background()
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	lifecycle := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
	job := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_activation_operation_id")
	activation, current, err := lifecycle.ClaimActivation(ctx, job, now)
	if err != nil || !current {
		t.Fatalf("ClaimActivation = current %t err %v", current, err)
	}
	if _, err := adminDB.Exec(`UPDATE session_sandbox_bindings
		SET release_requested_at = $1, release_reason = 'explicit_release'
		WHERE workspace_id = $2 AND session_id = $3`, now, job.WorkspaceID, job.SessionID); err != nil {
		t.Fatalf("set post-call release fence: %v", err)
	}
	if err := lifecycle.CompleteActivation(ctx, activation, sandbox.ProviderHandle{
		Provider: "daytona", SandboxID: "provider_post_call_release",
	}, now.Add(time.Second)); err != nil {
		t.Fatalf("CompleteActivation: %v", err)
	}
	var state, providerResourceID string
	if err := adminDB.QueryRow(`SELECT o.state, b.provider_resource_id
		FROM sandbox_lifecycle_operations o
		JOIN session_sandbox_bindings b ON b.workspace_id = o.workspace_id AND b.session_id = o.session_id
		WHERE o.workspace_id = $1 AND o.operation_id = $2`, job.WorkspaceID, job.OperationID).Scan(&state, &providerResourceID); err != nil {
		t.Fatalf("read activation completion: %v", err)
	}
	if state != "completed" || providerResourceID != "provider_post_call_release" {
		t.Fatalf("activation completion = state %q provider %q; want completed adopted handle", state, providerResourceID)
	}
}

func TestPostgreSQLSandboxLifecycleMaterializationCompletesAcrossPostCallReleaseFence(t *testing.T) {
	runtimeDB, adminDB := newReleaseHandlerTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := context.Background()
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	lifecycle := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
	activationJob := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_activation_operation_id")
	activation, current, err := lifecycle.ClaimActivation(ctx, activationJob, now)
	if err != nil || !current {
		t.Fatalf("ClaimActivation = current %t err %v", current, err)
	}
	if err := lifecycle.CompleteActivation(ctx, activation, sandbox.ProviderHandle{Provider: "daytona", SandboxID: "provider_post_call_materialization"}, now); err != nil {
		t.Fatalf("CompleteActivation: %v", err)
	}
	materializationJob := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_materialization_operation_id")
	materialization, current, err := lifecycle.ClaimMaterialization(ctx, materializationJob, now.Add(time.Second))
	if err != nil || !current {
		t.Fatalf("ClaimMaterialization = current %t err %v", current, err)
	}
	if _, err := adminDB.Exec(`UPDATE session_sandbox_bindings
		SET release_requested_at = $1, release_reason = 'explicit_release'
		WHERE workspace_id = $2 AND session_id = $3`, now, materializationJob.WorkspaceID, materializationJob.SessionID); err != nil {
		t.Fatalf("set post-call release fence: %v", err)
	}
	if err := lifecycle.CompleteMaterialization(ctx, materialization, MaterializationResult{
		MaterializedEnvironmentGeneration: materialization.TargetEnvironmentGeneration,
		MaterializedResourceRevision:      materialization.TargetResourceRevision,
		Resources:                         sandbox.ResourceSetup{ResourceCredExpiresAt: ptrTime(now.Add(2 * time.Hour)), ResourceRootsJSON: "[]"},
	}, now.Add(2*time.Second)); err != nil {
		t.Fatalf("CompleteMaterialization: %v", err)
	}
	var state string
	if err := adminDB.QueryRow(`SELECT state FROM sandbox_lifecycle_operations
		WHERE workspace_id = $1 AND operation_id = $2`, materializationJob.WorkspaceID, materializationJob.OperationID).Scan(&state); err != nil {
		t.Fatalf("read materialization completion: %v", err)
	}
	if state != "completed" {
		t.Fatalf("materialization state = %q; want completed", state)
	}
}

func TestPostgreSQLSandboxLifecycleActivationRecordsPostCallStaleCompletion(t *testing.T) {
	runtimeDB, adminDB := newReleaseHandlerTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := context.Background()
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	lifecycle := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
	job := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_activation_operation_id")
	activation, current, err := lifecycle.ClaimActivation(ctx, job, now)
	if err != nil || !current {
		t.Fatalf("ClaimActivation = current %t err %v", current, err)
	}
	if _, err := adminDB.Exec(`UPDATE session_sandbox_bindings SET binding_revision = binding_revision + 1
		WHERE workspace_id = $1 AND session_id = $2`, job.WorkspaceID, job.SessionID); err != nil {
		t.Fatalf("advance binding revision: %v", err)
	}
	if err := lifecycle.CompleteActivation(ctx, activation, sandbox.ProviderHandle{
		Provider: "daytona", SandboxID: "provider_stale_activation",
	}, now.Add(time.Second)); err != nil {
		t.Fatalf("CompleteActivation: %v", err)
	}
	var state, adopted string
	if err := adminDB.QueryRow(`SELECT state, adopted_provider_resource_id FROM sandbox_lifecycle_operations
		WHERE workspace_id = $1 AND operation_id = $2`, job.WorkspaceID, job.OperationID).Scan(&state, &adopted); err != nil {
		t.Fatalf("read stale activation receipt: %v", err)
	}
	if state != "completed" || adopted != "provider_stale_activation" {
		t.Fatalf("stale activation receipt = state %q adopted %q", state, adopted)
	}
	assertSandboxExecutionState(t, adminDB, "evt_execution_a", "pending", 2)
}

func TestPostgreSQLSandboxLifecycleReclaimsRunningActivationAcrossReleaseFence(t *testing.T) {
	runtimeDB, adminDB := newReleaseHandlerTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := context.Background()
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	lifecycle := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
	job := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_activation_operation_id")
	if _, current, err := lifecycle.ClaimActivation(ctx, job, now); err != nil || !current {
		t.Fatalf("ClaimActivation(first) = current %t err %v", current, err)
	}
	if _, err := adminDB.Exec(`UPDATE session_sandbox_bindings
		SET release_requested_at = $1, release_reason = 'explicit_release'
		WHERE workspace_id = $2 AND session_id = $3`, now, job.WorkspaceID, job.SessionID); err != nil {
		t.Fatalf("set release fence: %v", err)
	}
	if _, current, err := lifecycle.ClaimActivation(ctx, job, now.Add(time.Second)); err != nil || !current {
		t.Fatalf("ClaimActivation(redelivery) = current %t err %v; want running receipt reclaimed", current, err)
	}
}

func TestPostgreSQLSandboxLifecycleMaterializationRecordsPostCallStaleCompletion(t *testing.T) {
	runtimeDB, adminDB := newReleaseHandlerTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := context.Background()
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	lifecycle := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
	activationJob := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_activation_operation_id")
	activation, current, err := lifecycle.ClaimActivation(ctx, activationJob, now)
	if err != nil || !current {
		t.Fatalf("ClaimActivation = current %t err %v", current, err)
	}
	if err := lifecycle.CompleteActivation(ctx, activation, sandbox.ProviderHandle{Provider: "daytona", SandboxID: "provider_stale_materialization"}, now); err != nil {
		t.Fatalf("CompleteActivation: %v", err)
	}
	materializationJob := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_materialization_operation_id")
	materialization, current, err := lifecycle.ClaimMaterialization(ctx, materializationJob, now.Add(time.Second))
	if err != nil || !current {
		t.Fatalf("ClaimMaterialization = current %t err %v", current, err)
	}
	if _, err := adminDB.Exec(`UPDATE session_sandbox_bindings SET binding_revision = binding_revision + 1
		WHERE workspace_id = $1 AND session_id = $2`, materializationJob.WorkspaceID, materializationJob.SessionID); err != nil {
		t.Fatalf("advance binding revision: %v", err)
	}
	if err := lifecycle.CompleteMaterialization(ctx, materialization, MaterializationResult{
		MaterializedEnvironmentGeneration: materialization.TargetEnvironmentGeneration,
		MaterializedResourceRevision:      materialization.TargetResourceRevision,
		Resources:                         sandbox.ResourceSetup{ResourceCredExpiresAt: ptrTime(now.Add(2 * time.Hour)), ResourceRootsJSON: "[]"},
	}, now.Add(2*time.Second)); err != nil {
		t.Fatalf("CompleteMaterialization: %v", err)
	}
	var state string
	if err := adminDB.QueryRow(`SELECT state FROM sandbox_lifecycle_operations
		WHERE workspace_id = $1 AND operation_id = $2`, materializationJob.WorkspaceID, materializationJob.OperationID).Scan(&state); err != nil {
		t.Fatalf("read stale materialization receipt: %v", err)
	}
	if state != "completed" {
		t.Fatalf("stale materialization state = %q; want completed", state)
	}
	assertSandboxExecutionState(t, adminDB, "evt_execution_a", "pending", 2)
}

func TestPostgreSQLSandboxLifecycleReclaimsRunningMaterializationAcrossReleaseFence(t *testing.T) {
	runtimeDB, adminDB := newReleaseHandlerTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := context.Background()
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	lifecycle := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
	activationJob := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_activation_operation_id")
	activation, current, err := lifecycle.ClaimActivation(ctx, activationJob, now)
	if err != nil || !current {
		t.Fatalf("ClaimActivation = current %t err %v", current, err)
	}
	if err := lifecycle.CompleteActivation(ctx, activation, sandbox.ProviderHandle{Provider: "daytona", SandboxID: "provider_redelivered_materialization"}, now); err != nil {
		t.Fatalf("CompleteActivation: %v", err)
	}
	job := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_materialization_operation_id")
	if _, current, err := lifecycle.ClaimMaterialization(ctx, job, now.Add(time.Second)); err != nil || !current {
		t.Fatalf("ClaimMaterialization(first) = current %t err %v", current, err)
	}
	if _, err := adminDB.Exec(`UPDATE session_sandbox_bindings
		SET release_requested_at = $1, release_reason = 'explicit_release'
		WHERE workspace_id = $2 AND session_id = $3`, now, job.WorkspaceID, job.SessionID); err != nil {
		t.Fatalf("set release fence: %v", err)
	}
	if _, current, err := lifecycle.ClaimMaterialization(ctx, job, now.Add(2*time.Second)); err != nil || !current {
		t.Fatalf("ClaimMaterialization(redelivery) = current %t err %v; want running receipt reclaimed", current, err)
	}
}

func TestPostgreSQLSandboxMaterializationFreezesEnvironmentAndResourceTargets(t *testing.T) {
	runtimeDB, adminDB := newReleaseHandlerTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := context.Background()
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	resourceSource := &fixedSandboxResourceSource{resources: sandbox.ResourceSetup{Files: []sandbox.FileMount{{
		ResourceID: "resource_original", MountPath: "/mnt/session/uploads/original.txt", ReadOnly: true,
	}}}}
	lifecycle := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), resourceSource, 30*time.Minute)
	activationJob := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_activation_operation_id")
	activation, current, err := lifecycle.ClaimActivation(ctx, activationJob, now)
	if err != nil || !current {
		t.Fatalf("ClaimActivation = current %t err %v", current, err)
	}
	if err := lifecycle.CompleteActivation(ctx, activation, sandbox.ProviderHandle{Provider: "daytona", SandboxID: "provider_execution_store"}, now); err != nil {
		t.Fatalf("CompleteActivation: %v", err)
	}

	materializationJob := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_materialization_operation_id")
	var targetEnvironmentGeneration, targetResourceRevision int64
	if err := adminDB.QueryRow(`SELECT target_environment_generation, target_resource_revision
		FROM sandbox_lifecycle_operations
		WHERE workspace_id = $1 AND operation_id = $2`, materializationJob.WorkspaceID, materializationJob.OperationID).Scan(
		&targetEnvironmentGeneration, &targetResourceRevision,
	); err != nil {
		t.Fatalf("read materialization targets: %v", err)
	}
	if targetEnvironmentGeneration != 1 || targetResourceRevision != 1 {
		t.Fatalf("materialization targets = environment %d resource %d; want 1/1", targetEnvironmentGeneration, targetResourceRevision)
	}
	resourceSource.resources = sandbox.ResourceSetup{Files: []sandbox.FileMount{{
		ResourceID: "resource_changed", MountPath: "/mnt/session/uploads/changed.txt", ReadOnly: true,
	}}}
	materialization, current, err := lifecycle.ClaimMaterialization(ctx, materializationJob, now)
	if err != nil || !current {
		t.Fatalf("ClaimMaterialization = current %t err %v", current, err)
	}
	if got := materialization.Setup.Resources.Files; len(got) != 1 || got[0].ResourceID != "resource_original" {
		t.Fatalf("materialization resources = %+v; want immutable operation snapshot", got)
	}
}

func TestPostgreSQLSandboxActivationFailurePropagatesThroughMaterialization(t *testing.T) {
	runtimeDB, adminDB := newReleaseHandlerTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := context.Background()
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	lifecycle := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
	firstActivationJob := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_activation_operation_id")
	firstActivation, current, err := lifecycle.ClaimActivation(ctx, firstActivationJob, now)
	if err != nil || !current {
		t.Fatalf("ClaimActivation = current %t err %v", current, err)
	}
	if err := lifecycle.CompleteActivation(ctx, firstActivation, sandbox.ProviderHandle{Provider: "daytona", SandboxID: "provider_execution_store"}, now); err != nil {
		t.Fatalf("CompleteActivation: %v", err)
	}
	materializationJob := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_materialization_operation_id")
	materialization, current, err := lifecycle.ClaimMaterialization(ctx, materializationJob, now)
	if err != nil || !current {
		t.Fatalf("ClaimMaterialization = current %t err %v", current, err)
	}
	if err := lifecycle.WaitMaterializationForActivation(ctx, materialization, ExecutionNeedsCreation, now); err != nil {
		t.Fatalf("WaitMaterializationForActivation: %v", err)
	}
	var secondActivationID string
	if err := adminDB.QueryRow(`SELECT waiting_activation_operation_id
		FROM sandbox_lifecycle_operations
		WHERE workspace_id = $1 AND operation_id = $2`, materializationJob.WorkspaceID, materializationJob.OperationID).Scan(&secondActivationID); err != nil {
		t.Fatalf("read replacement activation: %v", err)
	}
	secondActivationJob := lifecycleJobByOperationID(t, adminDB, materializationJob.WorkspaceID, secondActivationID)
	secondActivation, current, err := lifecycle.ClaimActivation(ctx, secondActivationJob, now)
	if err != nil || !current {
		t.Fatalf("ClaimActivation(replacement) = current %t err %v", current, err)
	}
	if err := lifecycle.FailActivation(ctx, secondActivation, ProviderProvedNotStarted, ProviderTerminal, "sandbox_activation_failed", "sandbox activation failed", now); err != nil {
		t.Fatalf("FailActivation: %v", err)
	}

	assertSandboxExecutionState(t, adminDB, "evt_execution_a", "terminal_unconsumed", 1)
	var materializationState, errorKind string
	if err := adminDB.QueryRow(`SELECT state, error_kind
		FROM sandbox_lifecycle_operations
		WHERE workspace_id = $1 AND operation_id = $2`, materializationJob.WorkspaceID, materializationJob.OperationID).Scan(
		&materializationState, &errorKind,
	); err != nil {
		t.Fatalf("read failed materialization: %v", err)
	}
	if materializationState != "failed" || errorKind != "sandbox_activation_failed" {
		t.Fatalf("materialization state/error = %s/%s; want failed/sandbox_activation_failed", materializationState, errorKind)
	}
}

func TestPostgreSQLSandboxActivationSuccessRequeuesWaitingMaterialization(t *testing.T) {
	runtimeDB, adminDB := newReleaseHandlerTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := context.Background()
	now := time.Now().UTC()
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	lifecycle := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
	firstJob := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_activation_operation_id")
	first, current, err := lifecycle.ClaimActivation(ctx, firstJob, now)
	if err != nil || !current {
		t.Fatalf("ClaimActivation(first) = current %t err %v", current, err)
	}
	if err := lifecycle.CompleteActivation(ctx, first, sandbox.ProviderHandle{Provider: "daytona", SandboxID: "provider_execution_store"}, now); err != nil {
		t.Fatalf("CompleteActivation(first): %v", err)
	}
	materializationJob := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_materialization_operation_id")
	materialization, current, err := lifecycle.ClaimMaterialization(ctx, materializationJob, now)
	if err != nil || !current {
		t.Fatalf("ClaimMaterialization = current %t err %v", current, err)
	}
	if err := lifecycle.WaitMaterializationForActivation(ctx, materialization, ExecutionNeedsCreation, now); err != nil {
		t.Fatalf("WaitMaterializationForActivation: %v", err)
	}
	if _, err := adminDB.Exec(`UPDATE queue_jobs SET status = 'acknowledged', acknowledged_at = $3
		WHERE workspace_id = $1 AND id = $2`, materializationJob.WorkspaceID, materializationJob.JobID, now); err != nil {
		t.Fatalf("acknowledge first materialization notification: %v", err)
	}
	var activationID string
	if err := adminDB.QueryRow(`SELECT waiting_activation_operation_id FROM sandbox_lifecycle_operations
		WHERE workspace_id = $1 AND operation_id = $2`, materializationJob.WorkspaceID, materializationJob.OperationID).Scan(&activationID); err != nil {
		t.Fatalf("read activation dependency: %v", err)
	}
	secondJob := lifecycleJobByOperationID(t, adminDB, materializationJob.WorkspaceID, activationID)
	second, current, err := lifecycle.ClaimActivation(ctx, secondJob, now)
	if err != nil || !current {
		t.Fatalf("ClaimActivation(second) = current %t err %v", current, err)
	}
	if err := lifecycle.CompleteActivation(ctx, second, sandbox.ProviderHandle{Provider: "daytona", SandboxID: "provider_execution_store_replacement"}, now); err != nil {
		t.Fatalf("CompleteActivation(second): %v", err)
	}
	var state string
	var waitID sql.NullString
	var queueJobID string
	var observedRevision, targetGeneration int64
	var targetProviderResourceID string
	if err := adminDB.QueryRow(`SELECT state, waiting_activation_operation_id, queue_job_id,
		observed_binding_revision, target_environment_generation, target_provider_resource_id
		FROM sandbox_lifecycle_operations WHERE workspace_id = $1 AND operation_id = $2`,
		materializationJob.WorkspaceID, materializationJob.OperationID,
	).Scan(&state, &waitID, &queueJobID, &observedRevision, &targetGeneration, &targetProviderResourceID); err != nil {
		t.Fatalf("read resumed materialization: %v", err)
	}
	if state != "pending" || waitID.Valid || queueJobID == materializationJob.JobID {
		t.Fatalf("resumed materialization = state %q wait %v queue %q; want pending, detached, fresh notification", state, waitID, queueJobID)
	}
	if observedRevision != 3 || targetGeneration != 1 || targetProviderResourceID != "provider_execution_store_replacement" {
		t.Fatalf("resumed materialization target = revision %d generation %d handle %q; want current binding identity", observedRevision, targetGeneration, targetProviderResourceID)
	}
	resumedJob := lifecycleJobByOperationID(t, adminDB, materializationJob.WorkspaceID, materializationJob.OperationID)
	if _, current, err := lifecycle.ClaimMaterialization(ctx, resumedJob, now.Add(time.Second)); err != nil || !current {
		t.Fatalf("ClaimMaterialization(resumed) = current %t err %v; want current target", current, err)
	}
}

func TestPostgreSQLSandboxActivationClaimAndCompletionShareLockOrder(t *testing.T) {
	runtimeDB, adminDB := newReleaseHandlerTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	now := time.Now().UTC()
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	lifecycle := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
	job := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_activation_operation_id")
	activation, current, err := lifecycle.ClaimActivation(ctx, job, now)
	if err != nil || !current {
		t.Fatalf("ClaimActivation = current %t err %v", current, err)
	}

	start := make(chan struct{})
	errs := make(chan error, 2)
	go func() {
		<-start
		errs <- lifecycle.CompleteActivation(ctx, activation, sandbox.ProviderHandle{Provider: "daytona", SandboxID: "provider_execution_store"}, now.Add(time.Second))
	}()
	go func() {
		<-start
		_, _, err := lifecycle.ClaimActivation(ctx, job, now.Add(time.Second))
		errs <- err
	}()
	close(start)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent lifecycle transition: %v", err)
		}
	}
	if ctx.Err() != nil {
		t.Fatalf("concurrent lifecycle transition timed out: %v", ctx.Err())
	}
	var state string
	if err := adminDB.QueryRow(`SELECT state FROM sandbox_lifecycle_operations
		WHERE workspace_id = $1 AND operation_id = $2`, job.WorkspaceID, job.OperationID).Scan(&state); err != nil {
		t.Fatalf("read activation state: %v", err)
	}
	if state != "completed" {
		t.Fatalf("activation state = %q; want completed", state)
	}
}

func TestPostgreSQLProactiveMaterializationSkipsColdSandbox(t *testing.T) {
	runtimeDB, adminDB := newReleaseHandlerTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	seedProactiveMaterialization(t, adminDB, now)
	lifecycle := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
	job := lifecycleJobByOperationID(t, adminDB, "ws_execution_store", "sop_proactive_materialization")
	work, current, err := lifecycle.ClaimMaterialization(context.Background(), job, now)
	if err != nil || !current {
		t.Fatalf("ClaimMaterialization = current %t err %v", current, err)
	}
	if err := lifecycle.WaitMaterializationForActivation(context.Background(), work, ExecutionNeedsActivation, now); err != nil {
		t.Fatalf("WaitMaterializationForActivation: %v", err)
	}
	var state string
	if err := adminDB.QueryRow(`SELECT state FROM sandbox_lifecycle_operations
		WHERE workspace_id = $1 AND operation_id = $2`, job.WorkspaceID, job.OperationID).Scan(&state); err != nil {
		t.Fatalf("read proactive materialization state: %v", err)
	}
	if state != "skipped_cold" {
		t.Fatalf("proactive materialization state = %q; want skipped_cold", state)
	}
	var activationCount int
	if err := adminDB.QueryRow(`SELECT count(*) FROM sandbox_lifecycle_operations
		WHERE workspace_id = $1 AND logical_sandbox_id = $2
		  AND kind IN ('create', 'start', 'replace') AND state <> 'completed'`, job.WorkspaceID, job.LogicalSandboxID).Scan(&activationCount); err != nil {
		t.Fatalf("count activations: %v", err)
	}
	if activationCount != 0 {
		t.Fatalf("cold proactive materialization created %d activation operations; want 0", activationCount)
	}
}

func TestPostgreSQLSandboxMaterializationExhaustionPreservesActivationHandoff(t *testing.T) {
	runtimeDB, adminDB := newReleaseHandlerTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	now := time.Now().UTC()
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(context.Background(), work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	lifecycle := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
	activationJob := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_activation_operation_id")
	activation, current, err := lifecycle.ClaimActivation(context.Background(), activationJob, now)
	if err != nil || !current {
		t.Fatalf("ClaimActivation = current %t err %v", current, err)
	}
	if err := lifecycle.CompleteActivation(context.Background(), activation, sandbox.ProviderHandle{Provider: "daytona", SandboxID: "provider_execution_store"}, now); err != nil {
		t.Fatalf("CompleteActivation: %v", err)
	}
	materializationJob := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_materialization_operation_id")
	materialization, current, err := lifecycle.ClaimMaterialization(context.Background(), materializationJob, now)
	if err != nil || !current {
		t.Fatalf("ClaimMaterialization = current %t err %v", current, err)
	}
	if err := lifecycle.WaitMaterializationForActivation(context.Background(), materialization, ExecutionNeedsActivation, now); err != nil {
		t.Fatalf("WaitMaterializationForActivation: %v", err)
	}
	queueJob := &queuev1.QueueJob{
		Id: materializationJob.JobID, WorkspaceId: materializationJob.WorkspaceID, Kind: queue.KindSandboxMaterialize,
		PartitionKey: queue.FormatSandboxLifecyclePartitionKey(workspace.ID(materializationJob.WorkspaceID), materializationJob.LogicalSandboxID),
		DedupeKey: queue.FormatSandboxLifecycleDedupeKey(queue.KindSandboxMaterialize, workspace.ID(materializationJob.WorkspaceID),
			materializationJob.LogicalSandboxID, materializationJob.OperationID),
	}
	if err := lifecycle.FinalizeExhaustedLifecycle(context.Background(), queueJob, queue.KindSandboxMaterialize, now.Add(time.Minute)); err != nil {
		t.Fatalf("FinalizeExhaustedLifecycle: %v", err)
	}
	var state string
	if err := adminDB.QueryRow(`SELECT state FROM sandbox_lifecycle_operations
		WHERE workspace_id = $1 AND operation_id = $2`, materializationJob.WorkspaceID, materializationJob.OperationID).Scan(&state); err != nil {
		t.Fatalf("read materialization state: %v", err)
	}
	if state != "waiting_activation" {
		t.Fatalf("materialization state = %q; want waiting_activation dependency handoff", state)
	}
	assertSandboxExecutionState(t, adminDB, "evt_execution_a", "waiting_materialization", 1)
}

type fixedSandboxResourceSource struct {
	resources sandbox.ResourceSetup
}

func (s *fixedSandboxResourceSource) ListSessionResourcesTx(context.Context, *dbconnect.Tx, workspace.ID, string) (sandbox.ResourceSetup, error) {
	return s.resources, nil
}

func readLifecycleJob(t *testing.T, db *sql.DB, eventID string, linkColumn string) SandboxLifecycleJob {
	t.Helper()
	if linkColumn != "waiting_activation_operation_id" && linkColumn != "waiting_materialization_operation_id" {
		t.Fatalf("invalid lifecycle link column %q", linkColumn)
	}
	var job SandboxLifecycleJob
	if err := db.QueryRow(`SELECT o.queue_job_id, o.session_id, o.logical_sandbox_id, o.operation_id
		FROM sandbox_lifecycle_operations o
		JOIN session_runtime_tool_results r
		  ON r.workspace_id = o.workspace_id AND r.`+linkColumn+` = o.operation_id
		WHERE o.workspace_id = 'ws_execution_store' AND r.tool_use_event_id = $1`, eventID).Scan(
		&job.JobID, &job.SessionID, &job.LogicalSandboxID, &job.OperationID,
	); err != nil {
		t.Fatalf("read lifecycle job: %v", err)
	}
	job.WorkspaceID = "ws_execution_store"
	job.AttemptCount = 1
	return job
}

func lifecycleJobByOperationID(t *testing.T, db *sql.DB, workspaceID string, operationID string) SandboxLifecycleJob {
	t.Helper()
	var job SandboxLifecycleJob
	if err := db.QueryRow(`SELECT queue_job_id, session_id, logical_sandbox_id
		FROM sandbox_lifecycle_operations
		WHERE workspace_id = $1 AND operation_id = $2`, workspaceID, operationID).Scan(
		&job.JobID, &job.SessionID, &job.LogicalSandboxID,
	); err != nil {
		t.Fatalf("read lifecycle job %s: %v", operationID, err)
	}
	job.WorkspaceID = workspaceID
	job.OperationID = operationID
	job.AttemptCount = 1
	return job
}

func seedProactiveMaterialization(t *testing.T, db *sql.DB, now time.Time) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO session_sandbox_bindings (
		workspace_id, session_id, logical_sandbox_id, environment_id,
		environment_generation, provider, provider_resource_id, binding_revision,
		created_at, updated_at
	) VALUES (
		'ws_execution_store', 'sesn_execution_store', 'sbox_proactive', 'env_execution_store',
		1, 'daytona', 'provider_proactive', 1, $1, $1
	)`, now); err != nil {
		t.Fatalf("seed proactive binding: %v", err)
	}
	partition := "sandbox-lifecycle:ws_execution_store:sbox_proactive"
	dedupe := "sandbox_materialize:ws_execution_store:sbox_proactive:sop_proactive_materialization"
	if _, err := db.Exec(`INSERT INTO sandbox_lifecycle_operations (
		workspace_id, operation_id, session_id, logical_sandbox_id,
		kind, state, observed_binding_revision, target_environment_generation, target_resource_revision,
		target_provider_resource_id, materialization_resources_json, queue_job_id, queue_kind,
		queue_partition_key, queue_dedupe_key, created_at, updated_at
	) VALUES (
		'ws_execution_store', 'sop_proactive_materialization', 'sesn_execution_store', 'sbox_proactive',
		'materialize', 'pending', 1, 1, 1, 'provider_proactive', '{}', 'qjob_proactive_materialization',
		'sandbox_materialize', $2, $3, $1, $1
	)`, now, partition, dedupe); err != nil {
		t.Fatalf("seed proactive materialization: %v", err)
	}
}

func assertSandboxExecutionState(t *testing.T, db *sql.DB, eventID string, wantState string, wantGeneration int64) {
	t.Helper()
	var state string
	var generation int64
	if err := db.QueryRow(`SELECT execution_state, execution_attempt_generation
		FROM session_runtime_tool_results
		WHERE workspace_id = 'ws_execution_store' AND tool_use_event_id = $1`, eventID).Scan(&state, &generation); err != nil {
		t.Fatalf("read execution state: %v", err)
	}
	if state != wantState || generation != wantGeneration {
		t.Fatalf("execution state/generation = %s/%d; want %s/%d", state, generation, wantState, wantGeneration)
	}
}

func ptrTime(value time.Time) *time.Time {
	return &value
}
