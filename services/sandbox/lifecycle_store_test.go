package tetralsandbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sandbox"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/workspace"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

func TestPostgreSQLSandboxLifecycleConvergesBeforeReenqueueingExecution(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	now := time.Now().UTC()
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
	if current != SandboxLifecycleApplied || activation.Kind != ActivationCreate {
		t.Fatalf("activation = %+v current=%s; want current create", activation, current)
	}
	if _, err := lifecycle.CompleteActivation(ctx, activation, sandbox.ProviderHandle{
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
	if current != SandboxLifecycleApplied || materialization.Handle.SandboxID != "provider_execution_store" {
		t.Fatalf("materialization = %+v current=%s", materialization, current)
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
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	job := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_activation_operation_id")
	if _, err := adminDB.Exec(`UPDATE session_sandbox_bindings
		SET release_requested_at = $1, release_reason = 'session_delete'
		WHERE workspace_id = $2 AND session_id = $3`, now, job.WorkspaceID, job.SessionID); err != nil {
		t.Fatalf("set release fence: %v", err)
	}
	lifecycle := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
	_, current, err := lifecycle.ClaimActivation(ctx, job, now)
	if err != nil {
		t.Fatalf("ClaimActivation: %v", err)
	}
	if current == SandboxLifecycleApplied {
		t.Fatal("release-fenced activation was claimed")
	}
}

func TestPostgreSQLSandboxLifecycleActivationCompletesAcrossPostCallReleaseFence(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	lifecycle := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
	job := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_activation_operation_id")
	activation, current, err := lifecycle.ClaimActivation(ctx, job, now)
	if err != nil || current != SandboxLifecycleApplied {
		t.Fatalf("ClaimActivation = current %s err %v", current, err)
	}
	if _, err := adminDB.Exec(`UPDATE session_sandbox_bindings
		SET release_requested_at = $1, release_reason = 'session_delete'
		WHERE workspace_id = $2 AND session_id = $3`, now, job.WorkspaceID, job.SessionID); err != nil {
		t.Fatalf("set post-call release fence: %v", err)
	}
	if _, err := lifecycle.CompleteActivation(ctx, activation, sandbox.ProviderHandle{
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
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	lifecycle := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
	activationJob := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_activation_operation_id")
	activationCtx := withLifecycleJobQueueAuthority(ctx, activationJob)
	activation, current, err := lifecycle.ClaimActivation(activationCtx, activationJob, now)
	if err != nil || current != SandboxLifecycleApplied {
		t.Fatalf("ClaimActivation = current %s err %v", current, err)
	}
	if _, err := lifecycle.CompleteActivation(ctx, activation, sandbox.ProviderHandle{Provider: "daytona", SandboxID: "provider_post_call_materialization"}, now); err != nil {
		t.Fatalf("CompleteActivation: %v", err)
	}
	materializationJob := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_materialization_operation_id")
	materialization, current, err := lifecycle.ClaimMaterialization(ctx, materializationJob, now.Add(time.Second))
	if err != nil || current != SandboxLifecycleApplied {
		t.Fatalf("ClaimMaterialization = current %s err %v", current, err)
	}
	if _, err := adminDB.Exec(`UPDATE session_sandbox_bindings
		SET release_requested_at = $1, release_reason = 'session_delete'
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
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	lifecycle := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
	job := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_activation_operation_id")
	activation, current, err := lifecycle.ClaimActivation(ctx, job, now)
	if err != nil || current != SandboxLifecycleApplied {
		t.Fatalf("ClaimActivation = current %s err %v", current, err)
	}
	if _, err := adminDB.Exec(`UPDATE session_sandbox_bindings SET binding_revision = binding_revision + 1
		WHERE workspace_id = $1 AND session_id = $2`, job.WorkspaceID, job.SessionID); err != nil {
		t.Fatalf("advance binding revision: %v", err)
	}
	if _, err := lifecycle.CompleteActivation(ctx, activation, sandbox.ProviderHandle{
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

func TestPostgreSQLSandboxLifecycleRejectsUnsubmittedRunningActivationAcrossReleaseFence(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	lifecycle := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
	job := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_activation_operation_id")
	if _, current, err := lifecycle.ClaimActivation(ctx, job, now); err != nil || current != SandboxLifecycleApplied {
		t.Fatalf("ClaimActivation(first) = current %s err %v", current, err)
	}
	if _, err := adminDB.Exec(`UPDATE session_sandbox_bindings
		SET release_requested_at = $1, release_reason = 'session_delete'
		WHERE workspace_id = $2 AND session_id = $3`, now, job.WorkspaceID, job.SessionID); err != nil {
		t.Fatalf("set release fence: %v", err)
	}
	successor := job
	successor.AttemptCount++
	successor.LeaseOwner = "sandbox-activation-successor"
	successor.LeaseToken = "lease_activation_successor"
	successor.LeaseExpiresAt = time.Now().UTC().Add(time.Hour)
	setLifecycleQueueLeaseForTest(t, adminDB, successor.JobID, successor.LeaseOwner, successor.LeaseToken, successor.AttemptCount, successor.LeaseExpiresAt)
	if work, current, err := lifecycle.ClaimActivation(ctx, successor, now.Add(time.Second)); err != nil || current == SandboxLifecycleApplied || work.Job.JobID != "" {
		t.Fatalf("ClaimActivation(redelivery) = work %+v current %s err %v; want release-fenced no-op", work, current, err)
	}
	var state string
	if err := adminDB.QueryRow(`SELECT state FROM sandbox_lifecycle_operations
		WHERE workspace_id=$1 AND operation_id=$2`, job.WorkspaceID, job.OperationID).Scan(&state); err != nil {
		t.Fatalf("read fenced activation: %v", err)
	}
	if state != "abandoned" {
		t.Fatalf("fenced activation state = %q; want abandoned", state)
	}
}

func TestSandboxActivationRunnerRejectsUnsubmittedRedeliveryAcrossReleaseFence(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	client := dbconnect.NewClientForTesting(runtimeDB)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(client, 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	queueStore := queue.NewPostgreSQLStore(client)
	first := leaseSandboxLifecycleJob(t, queueStore, queue.KindSandboxActivate, "sandbox-activation-first")
	lifecycle := NewPostgreSQLSandboxLifecycleStore(client, &fixedSandboxResourceSource{}, 30*time.Minute)
	if _, disposition, err := lifecycle.ClaimActivation(withLifecycleJobQueueAuthority(context.Background(), first), first, now); err != nil || disposition != SandboxLifecycleApplied {
		t.Fatalf("ClaimActivation(first) = disposition %s err %v", disposition, err)
	}
	if _, err := adminDB.Exec(`UPDATE session_sandbox_bindings
		SET release_requested_at=$1, release_reason='session_delete'
		WHERE workspace_id=$2 AND session_id=$3`, now, first.WorkspaceID, first.SessionID); err != nil {
		t.Fatalf("set release fence: %v", err)
	}
	reclaimSandboxLifecycleJob(t, queueStore, adminDB, first)
	adapter := &recordingLifecycleAdapter{}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxActivationJobRunner{
		Queue: sandboxProductionQueueClient(t, queueStore), Store: lifecycle, Providers: registry,
		Config: SandboxLifecycleRunnerConfig{
			WorkspaceID: first.WorkspaceID, LeaseOwner: "sandbox-activation-successor",
			MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second,
		},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	assertLifecycleRunnerReleaseFence(t, adminDB, first, adapter.calls)
}

func TestPostgreSQLSandboxLifecycleMaterializationRecordsPostCallStaleCompletion(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	lifecycle := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
	activationJob := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_activation_operation_id")
	activationCtx := withLifecycleJobQueueAuthority(ctx, activationJob)
	activation, current, err := lifecycle.ClaimActivation(activationCtx, activationJob, now)
	if err != nil || current != SandboxLifecycleApplied {
		t.Fatalf("ClaimActivation = current %s err %v", current, err)
	}
	if _, err := lifecycle.CompleteActivation(ctx, activation, sandbox.ProviderHandle{Provider: "daytona", SandboxID: "provider_stale_materialization"}, now); err != nil {
		t.Fatalf("CompleteActivation: %v", err)
	}
	materializationJob := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_materialization_operation_id")
	materialization, current, err := lifecycle.ClaimMaterialization(ctx, materializationJob, now.Add(time.Second))
	if err != nil || current != SandboxLifecycleApplied {
		t.Fatalf("ClaimMaterialization = current %s err %v", current, err)
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

func TestPostgreSQLSandboxLifecycleRejectsUnsubmittedRunningMaterializationAcrossReleaseFence(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	lifecycle := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
	activationJob := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_activation_operation_id")
	activation, current, err := lifecycle.ClaimActivation(ctx, activationJob, now)
	if err != nil || current != SandboxLifecycleApplied {
		t.Fatalf("ClaimActivation = current %s err %v", current, err)
	}
	if _, err := lifecycle.CompleteActivation(ctx, activation, sandbox.ProviderHandle{Provider: "daytona", SandboxID: "provider_redelivered_materialization"}, now); err != nil {
		t.Fatalf("CompleteActivation: %v", err)
	}
	job := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_materialization_operation_id")
	if _, current, err := lifecycle.ClaimMaterialization(ctx, job, now.Add(time.Second)); err != nil || current != SandboxLifecycleApplied {
		t.Fatalf("ClaimMaterialization(first) = current %s err %v", current, err)
	}
	var releaseOperationID string
	client := dbconnect.NewClientForTesting(runtimeDB)
	if err := client.WithWorkspaceTx(ctx, job.WorkspaceID, "sandbox.lifecycle.redelivery_release", func(tx *dbconnect.Tx) error {
		var err error
		releaseOperationID, _, err = EnsureSandboxReleaseTx(ctx, tx, job.WorkspaceID, job.SessionID, SandboxReleaseSessionDelete, "provider_redelivered_materialization", now)
		return err
	}); err != nil {
		t.Fatalf("establish release fence: %v", err)
	}
	successor := job
	successor.AttemptCount++
	successor.LeaseOwner = "sandbox-materialization-successor"
	successor.LeaseToken = "lease_materialization_successor"
	successor.LeaseExpiresAt = time.Now().UTC().Add(time.Hour)
	setLifecycleQueueLeaseForTest(t, adminDB, successor.JobID, successor.LeaseOwner, successor.LeaseToken, successor.AttemptCount, successor.LeaseExpiresAt)
	if work, current, err := lifecycle.ClaimMaterialization(ctx, successor, now.Add(2*time.Second)); err != nil || current == SandboxLifecycleApplied || work.Job.JobID != "" {
		t.Fatalf("ClaimMaterialization(redelivery) = work %+v current %s err %v; want release-fenced no-op", work, current, err)
	}
	var state string
	if err := adminDB.QueryRow(`SELECT state FROM sandbox_lifecycle_operations
		WHERE workspace_id=$1 AND operation_id=$2`, job.WorkspaceID, job.OperationID).Scan(&state); err != nil {
		t.Fatalf("read fenced materialization: %v", err)
	}
	if state != "abandoned" {
		t.Fatalf("fenced materialization state = %q; want abandoned", state)
	}
	assertSandboxReleaseQueued(t, adminDB, releaseOperationID, true)
}

func TestSandboxMaterializationRunnerRejectsUnsubmittedRedeliveryAcrossReleaseFence(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	client := dbconnect.NewClientForTesting(runtimeDB)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(client, 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	lifecycle := NewPostgreSQLSandboxLifecycleStore(client, &fixedSandboxResourceSource{}, 30*time.Minute)
	activationJob := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_activation_operation_id")
	activation, disposition, err := lifecycle.ClaimActivation(ctx, activationJob, now)
	if err != nil || disposition != SandboxLifecycleApplied {
		t.Fatalf("ClaimActivation = disposition %s err %v", disposition, err)
	}
	if _, err := lifecycle.CompleteActivation(ctx, activation, sandbox.ProviderHandle{
		Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider_runner_redelivery",
	}, now); err != nil {
		t.Fatalf("CompleteActivation: %v", err)
	}
	acknowledgeSandboxLifecycleJobForTest(t, adminDB, activationJob)
	queueStore := queue.NewPostgreSQLStore(client)
	first := leaseSandboxLifecycleJob(t, queueStore, queue.KindSandboxMaterialize, "sandbox-materialization-first")
	if _, disposition, err := lifecycle.ClaimMaterialization(withLifecycleJobQueueAuthority(context.Background(), first), first, now.Add(time.Second)); err != nil || disposition != SandboxLifecycleApplied {
		t.Fatalf("ClaimMaterialization(first) = disposition %s err %v", disposition, err)
	}
	if _, err := adminDB.Exec(`UPDATE session_sandbox_bindings
		SET release_requested_at=$1, release_reason='session_delete'
		WHERE workspace_id=$2 AND session_id=$3`, now, first.WorkspaceID, first.SessionID); err != nil {
		t.Fatalf("set release fence: %v", err)
	}
	reclaimSandboxLifecycleJob(t, queueStore, adminDB, first)
	adapter := &recordingLifecycleAdapter{}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxMaterializationJobRunner{
		Queue: sandboxProductionQueueClient(t, queueStore), Store: lifecycle, Providers: registry,
		Config: SandboxLifecycleRunnerConfig{
			WorkspaceID: first.WorkspaceID, LeaseOwner: "sandbox-materialization-successor",
			MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second,
		},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	assertLifecycleRunnerReleaseFence(t, adminDB, first, adapter.calls)
}

func TestPostgreSQLSandboxLifecycleRejectsExpiredMaterializationWriterAfterLeaseTransfer(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	now := time.Now().UTC()
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	execution := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, execution, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	lifecycle := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
	activationJob := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_activation_operation_id")
	activationCtx := withLifecycleJobQueueAuthority(ctx, activationJob)
	activation, current, err := lifecycle.ClaimActivation(activationCtx, activationJob, now)
	if err != nil || current != SandboxLifecycleApplied {
		t.Fatalf("ClaimActivation = current %s err %v", current, err)
	}
	if _, err := lifecycle.CompleteActivation(activationCtx, activation, sandbox.ProviderHandle{Provider: "daytona", SandboxID: "provider_materialization_lease"}, now); err != nil {
		t.Fatalf("CompleteActivation: %v", err)
	}
	acknowledgeSandboxLifecycleJobForTest(t, adminDB, activationJob)
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtimeDB))
	firstJob := leaseSandboxLifecycleJob(t, queueStore, queue.KindSandboxMaterialize, "materialization-worker-a")
	firstCtx := withLifecycleJobQueueAuthority(ctx, firstJob)
	first, current, err := lifecycle.ClaimMaterialization(firstCtx, firstJob, now)
	if err != nil || current != SandboxLifecycleApplied {
		t.Fatalf("ClaimMaterialization(first) = current %s err %v", current, err)
	}
	if _, err := adminDB.Exec(`UPDATE queue_jobs SET leased_until=clock_timestamp()-interval '1 second'
		WHERE workspace_id=$1 AND id=$2`, firstJob.WorkspaceID, firstJob.JobID); err != nil {
		t.Fatalf("expire materialization Queue lease: %v", err)
	}
	if reclaimed, err := queueStore.ReclaimExpiredLeases(context.Background(), queue.ReclaimExpiredLeasesRequest{
		WorkspaceID: workspace.ID(firstJob.WorkspaceID), Limit: 1,
	}); err != nil || reclaimed != 1 {
		t.Fatalf("reclaim materialization Queue lease = %d, %v; want 1,nil", reclaimed, err)
	}
	secondJob := leaseSandboxLifecycleJob(t, queueStore, queue.KindSandboxMaterialize, "materialization-worker-b")
	secondCtx := withLifecycleJobQueueAuthority(ctx, secondJob)
	second, current, err := lifecycle.ClaimMaterialization(secondCtx, secondJob, now.Add(2*time.Second))
	if err != nil || current != SandboxLifecycleApplied {
		t.Fatalf("ClaimMaterialization(second) = current %s err %v", current, err)
	}
	assertCurrentLease := func(stage string) {
		t.Helper()
		var state string
		var leaseOwner sql.NullString
		if err := adminDB.QueryRow(`SELECT state, lease_owner FROM sandbox_lifecycle_operations
			WHERE workspace_id=$1 AND operation_id=$2`, firstJob.WorkspaceID, firstJob.OperationID).Scan(&state, &leaseOwner); err != nil {
			t.Fatalf("read transferred materialization after %s: %v", stage, err)
		}
		if state != "running" || !leaseOwner.Valid || leaseOwner.String != "materialization-worker-b" {
			t.Fatalf("stale %s changed materialization = state %q lease %v", stage, state, leaseOwner)
		}
	}
	disposition, err := lifecycle.WaitMaterializationForActivation(firstCtx, first, ExecutionNeedsActivation, now.Add(3*time.Second))
	if err != nil || disposition != SandboxLifecycleLostAuthority {
		t.Fatalf("WaitMaterializationForActivation(stale) = %s, %v; want stale lifecycle authority", disposition, err)
	}
	assertCurrentLease("activation wait")
	if err := lifecycle.FailMaterialization(firstCtx, first, ProviderProvedNotStarted, ProviderTerminal, "stale_failure", "stale worker", now.Add(3*time.Second)); !errors.Is(err, errQueueLeaseLost) {
		t.Fatalf("FailMaterialization(stale) = %v; want direct Queue authority loss", err)
	}
	assertCurrentLease("failure")
	result := MaterializationResult{
		MaterializedEnvironmentGeneration: first.TargetEnvironmentGeneration,
		MaterializedResourceRevision:      first.TargetResourceRevision,
		Resources:                         sandbox.ResourceSetup{ResourceRootsJSON: "[]"},
	}
	if err := lifecycle.CompleteMaterialization(firstCtx, first, result, now.Add(3*time.Second)); !errors.Is(err, errQueueLeaseLost) {
		t.Fatalf("CompleteMaterialization(stale) = %v; want direct Queue authority loss", err)
	}
	assertCurrentLease("completion")
	result.MaterializedEnvironmentGeneration = second.TargetEnvironmentGeneration
	result.MaterializedResourceRevision = second.TargetResourceRevision
	if err := lifecycle.CompleteMaterialization(secondCtx, second, result, now.Add(4*time.Second)); err != nil {
		t.Fatalf("CompleteMaterialization(current): %v", err)
	}
}

func TestPostgreSQLSandboxMaterializationFreezesEnvironmentAndResourceTargets(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
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
	if err != nil || current != SandboxLifecycleApplied {
		t.Fatalf("ClaimActivation = current %s err %v", current, err)
	}
	if _, err := lifecycle.CompleteActivation(ctx, activation, sandbox.ProviderHandle{Provider: "daytona", SandboxID: "provider_execution_store"}, now); err != nil {
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
	if err != nil || current != SandboxLifecycleApplied {
		t.Fatalf("ClaimMaterialization = current %s err %v", current, err)
	}
	if got := materialization.Setup.Resources.Files; len(got) != 1 || got[0].ResourceID != "resource_original" {
		t.Fatalf("materialization resources = %+v; want immutable operation snapshot", got)
	}
}

func TestPostgreSQLSandboxMissingStartWaitsForCurrentEnvironmentArtifact(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	now := time.Now().UTC()
	seedReadySandboxBinding(t, adminDB, now)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	work := loadReadySandboxExecutionWork(t, coordinator)
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsActivation); err != nil {
		t.Fatalf("WaitForActivation(start): %v", err)
	}
	lifecycle := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
	job := readLifecycleJob(t, adminDB, work.Ref.ToolUseEventID, "waiting_activation_operation_id")
	activation, current, err := lifecycle.ClaimActivation(ctx, job, now)
	if err != nil || current != SandboxLifecycleApplied {
		t.Fatalf("ClaimActivation = current %s err %v", current, err)
	}
	if _, err := adminDB.Exec(`UPDATE environments SET current_generation = 2
		WHERE workspace_id = 'ws_execution_store' AND id = 'env_execution_store'`); err != nil {
		t.Fatalf("advance Environment generation: %v", err)
	}
	if _, err := adminDB.Exec(`INSERT INTO environment_artifacts (
		workspace_id, environment_id, generation, status, provider,
		normalized_config_hash, artifact_input_hash, runtime_network_policy_json,
		packages_json, created_at, updated_at
	) VALUES (
		'ws_execution_store', 'env_execution_store', 2, 'pending', 'daytona',
		'config_hash_2', 'artifact_hash_2', '{"type":"unrestricted"}', '{}', $1, $1
	)`, now); err != nil {
		t.Fatalf("seed building Environment artifact: %v", err)
	}
	seedParkedSandboxRelease(t, adminDB, "sop_release_after_artifact_wait", job.SessionID, job.LogicalSandboxID, activation.CurrentHandle.SandboxID, now)

	replacement, current, err := lifecycle.ReplaceMissingActivation(ctx, activation, now.Add(time.Second))
	if err != nil {
		t.Fatalf("ReplaceMissingActivation: %v", err)
	}
	if current != SandboxLifecycleApplied || replacement.Kind != "" {
		t.Fatalf("ReplaceMissingActivation = disposition %s replacement %+v; want applied parking without provider work", current, replacement)
	}
	var kind, state string
	var generation int64
	var queueJobID sql.NullString
	if err := adminDB.QueryRow(`SELECT kind, state, target_environment_generation, queue_job_id
		FROM sandbox_lifecycle_operations
		WHERE workspace_id = $1 AND operation_id = $2`, job.WorkspaceID, job.OperationID).Scan(
		&kind, &state, &generation, &queueJobID,
	); err != nil {
		t.Fatalf("read parked replacement: %v", err)
	}
	if kind != "replace" || state != "waiting_artifact" || generation != 2 || queueJobID.Valid {
		t.Fatalf("replacement = kind %q state %q generation %d queue %v; want replace/waiting_artifact/2/no queue", kind, state, generation, queueJobID)
	}
	assertSandboxReleaseQueued(t, adminDB, "sop_release_after_artifact_wait", true)
}

func TestPostgreSQLSandboxMissingStartAbandonsAfterReleaseFence(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	now := time.Now().UTC()
	seedReadySandboxBinding(t, adminDB, now)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	execution := loadReadySandboxExecutionWork(t, coordinator)
	if err := coordinator.WaitForActivation(ctx, execution, ExecutionNeedsActivation); err != nil {
		t.Fatalf("WaitForActivation(start): %v", err)
	}
	lifecycle := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
	job := readLifecycleJob(t, adminDB, execution.Ref.ToolUseEventID, "waiting_activation_operation_id")
	activation, disposition, err := lifecycle.ClaimActivation(ctx, job, now)
	if err != nil || disposition != SandboxLifecycleApplied {
		t.Fatalf("ClaimActivation = %s, %v", disposition, err)
	}
	seedParkedSandboxRelease(t, adminDB, "sop_release_after_missing_start_fence", job.SessionID, job.LogicalSandboxID, activation.CurrentHandle.SandboxID, now)
	if _, err := adminDB.Exec(`UPDATE session_sandbox_bindings
		SET release_requested_at=$1, release_reason='session_delete'
		WHERE workspace_id=$2 AND session_id=$3`, now, job.WorkspaceID, job.SessionID); err != nil {
		t.Fatalf("set release fence: %v", err)
	}

	replacement, disposition, err := lifecycle.ReplaceMissingActivation(ctx, activation, now.Add(time.Second))
	if err != nil || disposition != SandboxLifecycleApplied || replacement.Kind != "" {
		t.Fatalf("ReplaceMissingActivation = replacement %+v disposition %s err %v; want applied abandonment", replacement, disposition, err)
	}
	var state string
	if err := adminDB.QueryRow(`SELECT state FROM sandbox_lifecycle_operations
		WHERE workspace_id=$1 AND operation_id=$2`, job.WorkspaceID, job.OperationID).Scan(&state); err != nil {
		t.Fatalf("read fenced activation: %v", err)
	}
	if state != "abandoned" {
		t.Fatalf("fenced missing activation state = %q; want abandoned", state)
	}
	assertSandboxReleaseQueued(t, adminDB, "sop_release_after_missing_start_fence", true)
}

func TestPostgreSQLSandboxActivationFailurePropagatesThroughMaterialization(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	lifecycle := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
	firstActivationJob := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_activation_operation_id")
	firstActivation, current, err := lifecycle.ClaimActivation(ctx, firstActivationJob, now)
	if err != nil || current != SandboxLifecycleApplied {
		t.Fatalf("ClaimActivation = current %s err %v", current, err)
	}
	if _, err := lifecycle.CompleteActivation(ctx, firstActivation, sandbox.ProviderHandle{Provider: "daytona", SandboxID: "provider_execution_store"}, now); err != nil {
		t.Fatalf("CompleteActivation: %v", err)
	}
	materializationJob := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_materialization_operation_id")
	materializationCtx := leaseSandboxMaterializationJobForTest(t, runtimeDB, adminDB, &materializationJob)
	materialization, current, err := lifecycle.ClaimMaterialization(materializationCtx, materializationJob, now)
	if err != nil || current != SandboxLifecycleApplied {
		t.Fatalf("ClaimMaterialization = current %s err %v", current, err)
	}
	disposition, err := lifecycle.WaitMaterializationForActivation(materializationCtx, materialization, ExecutionNeedsCreation, now)
	if err != nil || disposition != SandboxLifecycleApplied {
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
	if err != nil || current != SandboxLifecycleApplied {
		t.Fatalf("ClaimActivation(replacement) = current %s err %v", current, err)
	}
	if _, err := lifecycle.FailActivation(ctx, secondActivation, ProviderProvedNotStarted, ProviderTerminal, "sandbox_activation_failed", "sandbox activation failed", now); err != nil {
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

func TestPostgreSQLSandboxMaterializationWaitRollsBackAfterQueueLeaseTransfer(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	now := time.Now().UTC()
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	lifecycle := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
	activationJob := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_activation_operation_id")
	activationCtx := withLifecycleJobQueueAuthority(ctx, activationJob)
	activation, disposition, err := lifecycle.ClaimActivation(activationCtx, activationJob, now)
	if err != nil || disposition != SandboxLifecycleApplied {
		t.Fatalf("ClaimActivation = %s, %v", disposition, err)
	}
	if _, err := lifecycle.CompleteActivation(activationCtx, activation, sandbox.ProviderHandle{Provider: "daytona", SandboxID: "provider_execution_store"}, now); err != nil {
		t.Fatalf("CompleteActivation: %v", err)
	}
	acknowledgeSandboxLifecycleJobForTest(t, adminDB, activationJob)
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtimeDB))
	materializationJob := leaseSandboxLifecycleJob(t, queueStore, queue.KindSandboxMaterialize, "materialization-first")
	materializationCtx := withLifecycleJobQueueAuthority(context.Background(), materializationJob)
	materialization, disposition, err := lifecycle.ClaimMaterialization(materializationCtx, materializationJob, now)
	if err != nil || disposition != SandboxLifecycleApplied {
		t.Fatalf("ClaimMaterialization = %s, %v", disposition, err)
	}
	if _, err := adminDB.Exec(`UPDATE queue_jobs SET leased_until=clock_timestamp()-interval '1 second'
		WHERE workspace_id=$1 AND id=$2`, materializationJob.WorkspaceID, materializationJob.JobID); err != nil {
		t.Fatalf("expire materialization Queue lease: %v", err)
	}
	if reclaimed, err := queueStore.ReclaimExpiredLeases(context.Background(), queue.ReclaimExpiredLeasesRequest{
		WorkspaceID: workspace.ID(materializationJob.WorkspaceID), Limit: 1,
	}); err != nil || reclaimed != 1 {
		t.Fatalf("reclaim materialization Queue lease = %d, %v; want 1,nil", reclaimed, err)
	}
	secondJob := leaseSandboxLifecycleJob(t, queueStore, queue.KindSandboxMaterialize, "materialization-second")
	disposition, err = lifecycle.WaitMaterializationForActivation(materializationCtx, materialization, ExecutionNeedsCreation, now.Add(time.Second))
	if !errors.Is(err, errQueueLeaseLost) || disposition != SandboxLifecycleLostAuthority {
		t.Fatalf("WaitMaterializationForActivation after Queue transfer = %s, %v; want direct Queue authority loss", disposition, err)
	}
	var state string
	var waitingActivationID sql.NullString
	if err := adminDB.QueryRow(`SELECT state, waiting_activation_operation_id
		FROM sandbox_lifecycle_operations WHERE workspace_id=$1 AND operation_id=$2`,
		materializationJob.WorkspaceID, materializationJob.OperationID).Scan(&state, &waitingActivationID); err != nil {
		t.Fatalf("read materialization after Queue transfer: %v", err)
	}
	if state != "running" || waitingActivationID.Valid {
		t.Fatalf("materialization after Queue transfer = %q/%v; want running without activation dependency", state, waitingActivationID)
	}
	secondCtx := withLifecycleJobQueueAuthority(context.Background(), secondJob)
	secondWork, disposition, err := lifecycle.ClaimMaterialization(secondCtx, secondJob, now.Add(time.Second))
	if err != nil || disposition != SandboxLifecycleApplied {
		t.Fatalf("ClaimMaterialization(second) = %s, %v", disposition, err)
	}
	queueClient := sandboxProductionQueueClient(t, queueStore)
	guardCtx, finishGuard, err := startQueueLeaseGuard(context.Background(), queueClient, secondJob.QueueJob, secondJob.LeaseExpiresAt, 10*time.Second, time.Minute)
	if err != nil {
		t.Fatalf("start materialization Queue lease guard: %v", err)
	}
	disposition, err = lifecycle.WaitMaterializationForActivation(guardCtx, secondWork, ExecutionNeedsCreation, now.Add(2*time.Second))
	if err != nil || disposition != SandboxLifecycleApplied {
		t.Fatalf("WaitMaterializationForActivation(current) = %s, %v", disposition, err)
	}
	if err := finishGuard(); err != nil {
		t.Fatalf("consumed materialization Queue lease guard: %v", err)
	}
	var queueState string
	if err := adminDB.QueryRow(`SELECT status FROM queue_jobs WHERE workspace_id=$1 AND id=$2`, secondJob.WorkspaceID, secondJob.JobID).Scan(&queueState); err != nil {
		t.Fatalf("read consumed materialization Queue job: %v", err)
	}
	if queueState != queue.StatusAcknowledged {
		t.Fatalf("materialization Queue state = %q; want acknowledged", queueState)
	}
}

func TestPostgreSQLSandboxActivationSuccessRequeuesWaitingMaterialization(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	now := time.Now().UTC()
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	lifecycle := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
	firstJob := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_activation_operation_id")
	first, current, err := lifecycle.ClaimActivation(ctx, firstJob, now)
	if err != nil || current != SandboxLifecycleApplied {
		t.Fatalf("ClaimActivation(first) = current %s err %v", current, err)
	}
	if _, err := lifecycle.CompleteActivation(ctx, first, sandbox.ProviderHandle{Provider: "daytona", SandboxID: "provider_execution_store"}, now); err != nil {
		t.Fatalf("CompleteActivation(first): %v", err)
	}
	acknowledgeSandboxLifecycleJobForTest(t, adminDB, firstJob)
	materializationJob := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_materialization_operation_id")
	materializationCtx := leaseSandboxMaterializationJobForTest(t, runtimeDB, adminDB, &materializationJob)
	materialization, current, err := lifecycle.ClaimMaterialization(materializationCtx, materializationJob, now)
	if err != nil || current != SandboxLifecycleApplied {
		t.Fatalf("ClaimMaterialization = current %s err %v", current, err)
	}
	disposition, err := lifecycle.WaitMaterializationForActivation(materializationCtx, materialization, ExecutionNeedsCreation, now)
	if err != nil || disposition != SandboxLifecycleApplied {
		t.Fatalf("WaitMaterializationForActivation: %v", err)
	}
	var predecessorStatus string
	if err := adminDB.QueryRow(`SELECT status FROM queue_jobs WHERE workspace_id=$1 AND id=$2`,
		materializationJob.WorkspaceID, materializationJob.JobID).Scan(&predecessorStatus); err != nil {
		t.Fatalf("read predecessor materialization notification: %v", err)
	}
	if predecessorStatus != "acknowledged" {
		t.Fatalf("predecessor materialization notification = %q; want acknowledged with waiting state", predecessorStatus)
	}
	var parkedJobID, parkedKind, parkedPartition, parkedDedupe sql.NullString
	if err := adminDB.QueryRow(`SELECT queue_job_id, queue_kind, queue_partition_key, queue_dedupe_key
		FROM sandbox_lifecycle_operations WHERE workspace_id=$1 AND operation_id=$2`,
		materializationJob.WorkspaceID, materializationJob.OperationID,
	).Scan(&parkedJobID, &parkedKind, &parkedPartition, &parkedDedupe); err != nil {
		t.Fatalf("read parked materialization Queue identity: %v", err)
	}
	if parkedJobID.Valid || parkedKind.Valid || parkedPartition.Valid || parkedDedupe.Valid {
		t.Fatalf("parked materialization retained Queue identity: %v/%v/%v/%v", parkedJobID, parkedKind, parkedPartition, parkedDedupe)
	}
	var activationID string
	if err := adminDB.QueryRow(`SELECT waiting_activation_operation_id FROM sandbox_lifecycle_operations
		WHERE workspace_id = $1 AND operation_id = $2`, materializationJob.WorkspaceID, materializationJob.OperationID).Scan(&activationID); err != nil {
		t.Fatalf("read activation dependency: %v", err)
	}
	secondJob := lifecycleJobByOperationID(t, adminDB, materializationJob.WorkspaceID, activationID)
	second, current, err := lifecycle.ClaimActivation(ctx, secondJob, now)
	if err != nil || current != SandboxLifecycleApplied {
		t.Fatalf("ClaimActivation(second) = current %s err %v", current, err)
	}
	if _, err := adminDB.Exec(`UPDATE sandbox_lifecycle_operations
		SET attempt_count=4, lease_owner='sandbox-old', lease_token='lease-old',
		    lease_expires_at=clock_timestamp()+interval '1 minute'
		WHERE workspace_id=$1 AND operation_id=$2`, materializationJob.WorkspaceID, materializationJob.OperationID); err != nil {
		t.Fatalf("seed prior materialization authority: %v", err)
	}
	if _, err := lifecycle.CompleteActivation(ctx, second, sandbox.ProviderHandle{Provider: "daytona", SandboxID: "provider_execution_store_replacement"}, now); err != nil {
		t.Fatalf("CompleteActivation(second): %v", err)
	}
	acknowledgeSandboxLifecycleJobForTest(t, adminDB, secondJob)
	var state string
	var waitID sql.NullString
	var queueJobID string
	var observedRevision, targetGeneration int64
	var targetProviderResourceID string
	var attemptCount int
	var leaseOwner, leaseToken sql.NullString
	var leaseExpiresAt sql.NullTime
	if err := adminDB.QueryRow(`SELECT state, waiting_activation_operation_id, queue_job_id,
		observed_binding_revision, target_environment_generation, target_provider_resource_id,
		attempt_count, lease_owner, lease_token, lease_expires_at
		FROM sandbox_lifecycle_operations WHERE workspace_id = $1 AND operation_id = $2`,
		materializationJob.WorkspaceID, materializationJob.OperationID,
	).Scan(&state, &waitID, &queueJobID, &observedRevision, &targetGeneration, &targetProviderResourceID,
		&attemptCount, &leaseOwner, &leaseToken, &leaseExpiresAt); err != nil {
		t.Fatalf("read resumed materialization: %v", err)
	}
	if state != "pending" || waitID.Valid || queueJobID == materializationJob.JobID {
		t.Fatalf("resumed materialization = state %q wait %v queue %q; want pending, detached, fresh notification", state, waitID, queueJobID)
	}
	if observedRevision != 3 || targetGeneration != 1 || targetProviderResourceID != "provider_execution_store_replacement" {
		t.Fatalf("resumed materialization target = revision %d generation %d handle %q; want current binding identity", observedRevision, targetGeneration, targetProviderResourceID)
	}
	if attemptCount != 0 || leaseOwner.Valid || leaseToken.Valid || leaseExpiresAt.Valid {
		t.Fatalf("resumed materialization authority = attempt %d owner %v token %v expiry %v; want fresh generation", attemptCount, leaseOwner, leaseToken, leaseExpiresAt)
	}
	var resumedQueueStatus string
	if err := adminDB.QueryRow(`SELECT status FROM queue_jobs WHERE workspace_id=$1 AND id=$2`,
		materializationJob.WorkspaceID, queueJobID).Scan(&resumedQueueStatus); err != nil {
		t.Fatalf("read resumed materialization notification: %v", err)
	}
	if resumedQueueStatus != "pending" {
		t.Fatalf("resumed materialization notification = %q; want pending", resumedQueueStatus)
	}
	var releaseJob SandboxLifecycleJob
	if err := adminDB.QueryRow(`SELECT queue_job_id FROM sandbox_lifecycle_operations
		WHERE workspace_id=$1 AND logical_sandbox_id=$2 AND kind='release' AND state='pending'`,
		materializationJob.WorkspaceID, materializationJob.LogicalSandboxID).Scan(&releaseJob.JobID); err != nil {
		t.Fatalf("read replacement release notification: %v", err)
	}
	releaseJob.WorkspaceID = materializationJob.WorkspaceID
	acknowledgeSandboxLifecycleJobForTest(t, adminDB, releaseJob)
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtimeDB))
	leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: workspace.ID(materializationJob.WorkspaceID), Kinds: []string{queue.KindSandboxMaterialize},
		LeaseOwner: "sandbox-materialization-successor", MaxJobs: 1, LeaseDuration: time.Hour,
	})
	if err != nil || len(leased) != 1 || leased[0].ID != queueJobID || leased[0].LeasedUntil == nil {
		rows, queryErr := adminDB.Query(`SELECT id, kind, status, queue_partition_sequence
			FROM queue_jobs WHERE workspace_id=$1 AND partition_key=$2 ORDER BY queue_partition_sequence`,
			materializationJob.WorkspaceID, queue.FormatSandboxLifecyclePartitionKey(workspace.ID(materializationJob.WorkspaceID), materializationJob.LogicalSandboxID))
		if queryErr != nil {
			t.Fatalf("lease resumed materialization = %#v, %v; inspect partition: %v", leased, err, queryErr)
		}
		defer func() { _ = rows.Close() }()
		var partitionJobs []string
		for rows.Next() {
			var id, kind, status string
			var sequence int64
			if scanErr := rows.Scan(&id, &kind, &status, &sequence); scanErr != nil {
				t.Fatalf("scan lifecycle partition: %v", scanErr)
			}
			partitionJobs = append(partitionJobs, fmt.Sprintf("%d:%s:%s:%s", sequence, id, kind, status))
		}
		t.Fatalf("lease resumed materialization = %#v, %v; want %s; partition=%v", leased, err, queueJobID, partitionJobs)
	}
	resumedJob := lifecycleJobByOperationID(t, adminDB, materializationJob.WorkspaceID, materializationJob.OperationID)
	resumedJob.LeaseOwner = leased[0].LeasedBy
	resumedJob.LeaseToken = leased[0].LeaseToken
	resumedJob.LeaseExpiresAt = *leased[0].LeasedUntil
	resumedJob.AttemptCount = leased[0].AttemptCount
	setLifecycleQueueLeaseForTest(t, adminDB, resumedJob.JobID, resumedJob.LeaseOwner, resumedJob.LeaseToken, resumedJob.AttemptCount, resumedJob.LeaseExpiresAt)
	resumedCtx := withSandboxQueueAuthority(context.Background(), &sandboxQueueAuthority{
		workspaceID: resumedJob.WorkspaceID, jobID: resumedJob.JobID,
		leaseToken: resumedJob.LeaseToken, leasedUntil: resumedJob.LeaseExpiresAt,
	})
	if _, current, err := lifecycle.ClaimMaterialization(resumedCtx, resumedJob, now.Add(time.Second)); err != nil || current != SandboxLifecycleApplied {
		t.Fatalf("ClaimMaterialization(resumed) = current %s err %v; want current target", current, err)
	}
}

func TestPostgreSQLSandboxActivationClaimAndCompletionShareLockOrder(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx, cancel := context.WithTimeout(sandboxTestQueueContext(t, runtimeDB), 5*time.Second)
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
	if err != nil || current != SandboxLifecycleApplied {
		t.Fatalf("ClaimActivation = current %s err %v", current, err)
	}

	start := make(chan struct{})
	errs := make(chan error, 2)
	go func() {
		<-start
		_, err := lifecycle.CompleteActivation(ctx, activation, sandbox.ProviderHandle{Provider: "daytona", SandboxID: "provider_execution_store"}, now.Add(time.Second))
		errs <- err
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

func TestPostgreSQLSandboxActivationCompletionLocksLifecycleOperationsByID(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx, cancel := context.WithTimeout(sandboxTestQueueContext(t, runtimeDB), 5*time.Second)
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
	if err != nil || current != SandboxLifecycleApplied {
		t.Fatalf("ClaimActivation = current %s err %v", current, err)
	}
	const lowerOperationID = "000_lifecycle_lock_order"
	if lowerOperationID >= job.OperationID {
		t.Fatalf("lock-order fixture IDs = %q then %q; want ascending order", lowerOperationID, job.OperationID)
	}
	if _, err := adminDB.Exec(`INSERT INTO sandbox_lifecycle_operations (
		workspace_id, operation_id, session_id, logical_sandbox_id, kind, state,
		observed_binding_revision, target_provider_resource_id, created_at, updated_at, completed_at
	) VALUES ($1, $2, $3, $4, 'start', 'completed', 1, 'provider_lock_order_prior', $5, $5, $5)`,
		job.WorkspaceID, lowerOperationID, job.SessionID, job.LogicalSandboxID, now); err != nil {
		t.Fatalf("seed lower lifecycle operation: %v", err)
	}

	blocker, err := adminDB.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin target blocker: %v", err)
	}
	defer func() { _ = blocker.Rollback() }()
	var targetOperationID string
	if err := blocker.QueryRow(`SELECT operation_id FROM sandbox_lifecycle_operations
		WHERE workspace_id=$1 AND operation_id=$2 FOR UPDATE`, job.WorkspaceID, job.OperationID).Scan(&targetOperationID); err != nil {
		t.Fatalf("lock target lifecycle operation: %v", err)
	}

	completed := make(chan error, 1)
	go func() {
		_, err := lifecycle.CompleteActivation(ctx, activation, sandbox.ProviderHandle{
			Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider_lock_order",
		}, now.Add(time.Second))
		completed <- err
	}()
	waitForSandboxLifecycleLockWait(t, adminDB)

	observer, err := adminDB.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin lower-row observer: %v", err)
	}
	if _, err := observer.Exec(`SET LOCAL lock_timeout = '100ms'`); err != nil {
		_ = observer.Rollback()
		t.Fatalf("set observer lock timeout: %v", err)
	}
	var observedOperationID string
	err = observer.QueryRow(`SELECT operation_id FROM sandbox_lifecycle_operations
		WHERE workspace_id=$1 AND operation_id=$2 FOR UPDATE`, job.WorkspaceID, lowerOperationID).Scan(&observedOperationID)
	_ = observer.Rollback()
	if err == nil {
		t.Fatal("lower lifecycle operation was not locked before the target operation wait")
	}
	if !strings.Contains(err.Error(), "lock timeout") {
		t.Fatalf("observe lower lifecycle lock error = %v; want lock timeout", err)
	}

	if err := blocker.Rollback(); err != nil {
		t.Fatalf("release target blocker: %v", err)
	}
	select {
	case err := <-completed:
		if err != nil {
			t.Fatalf("CompleteActivation after lock release: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CompleteActivation did not finish after target lock release")
	}
}

func TestPostgreSQLSandboxActivationOutcomesRejectSupersededLease(t *testing.T) {
	writers := map[string]func(context.Context, *PostgreSQLSandboxLifecycleStore, SandboxActivationWork, time.Time) (SandboxLifecycleDisposition, error){
		"complete": func(ctx context.Context, store *PostgreSQLSandboxLifecycleStore, work SandboxActivationWork, now time.Time) (SandboxLifecycleDisposition, error) {
			return store.CompleteActivation(ctx, work, work.CurrentHandle, now)
		},
		"replace_missing": func(ctx context.Context, store *PostgreSQLSandboxLifecycleStore, work SandboxActivationWork, now time.Time) (SandboxLifecycleDisposition, error) {
			_, disposition, err := store.ReplaceMissingActivation(ctx, work, now)
			return disposition, err
		},
		"observe_unknown": func(ctx context.Context, store *PostgreSQLSandboxLifecycleStore, work SandboxActivationWork, now time.Time) (SandboxLifecycleDisposition, error) {
			return store.ObserveUnknownActivation(ctx, work, "stale_observation", now)
		},
		"fail": func(ctx context.Context, store *PostgreSQLSandboxLifecycleStore, work SandboxActivationWork, now time.Time) (SandboxLifecycleDisposition, error) {
			return store.FailActivation(ctx, work, ProviderProvedNotStarted, ProviderTerminal, "stale_failure", "stale worker", now)
		},
	}
	for name, write := range writers {
		t.Run(name, func(t *testing.T) {
			runtimeDB, adminDB := newSandboxServiceTestDB(t)
			seedSandboxExecutionStoreFixture(t, adminDB)
			now := time.Now().UTC()
			seedReadySandboxBinding(t, adminDB, now)
			ctx := sandboxTestQueueContext(t, runtimeDB)
			coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
			work := loadReadySandboxExecutionWork(t, coordinator)
			if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsActivation); err != nil {
				t.Fatalf("WaitForActivation: %v", err)
			}
			client := dbconnect.NewClientForTesting(runtimeDB)
			store := NewPostgreSQLSandboxLifecycleStore(client, &fixedSandboxResourceSource{}, 30*time.Minute)
			queueStore := queue.NewPostgreSQLStore(client)
			firstJob := leaseSandboxLifecycleJob(t, queueStore, queue.KindSandboxActivate, "activation-first")
			firstCtx := withLifecycleJobQueueAuthority(ctx, firstJob)
			first, disposition, err := store.ClaimActivation(firstCtx, firstJob, now)
			if err != nil || disposition != SandboxLifecycleApplied {
				t.Fatalf("ClaimActivation(first) = %s, %v", disposition, err)
			}
			if _, err := adminDB.Exec(`UPDATE queue_jobs SET leased_until=clock_timestamp()-interval '1 second'
					WHERE workspace_id=$1 AND id=$2`, firstJob.WorkspaceID, firstJob.JobID); err != nil {
				t.Fatalf("expire first activation Queue lease: %v", err)
			}
			if reclaimed, err := queueStore.ReclaimExpiredLeases(context.Background(), queue.ReclaimExpiredLeasesRequest{
				WorkspaceID: workspace.ID(firstJob.WorkspaceID), Limit: 1,
			}); err != nil || reclaimed != 1 {
				t.Fatalf("reclaim first activation Queue lease = %d, %v; want 1,nil", reclaimed, err)
			}
			secondJob := leaseSandboxLifecycleJob(t, queueStore, queue.KindSandboxActivate, "activation-second")
			secondCtx := withLifecycleJobQueueAuthority(ctx, secondJob)

			disposition, err = write(firstCtx, store, first, now.Add(2*time.Second))
			if !errors.Is(err, errQueueLeaseLost) || disposition != SandboxLifecycleLostAuthority {
				t.Fatalf("stale %s = %s, %v; want direct Queue authority loss", name, disposition, err)
			}
			var state, token string
			if err := adminDB.QueryRow(`SELECT state, lease_token FROM sandbox_lifecycle_operations
					WHERE workspace_id=$1 AND operation_id=$2`, firstJob.WorkspaceID, firstJob.OperationID).Scan(&state, &token); err != nil {
				t.Fatalf("read retained first authority: %v", err)
			}
			if state != "running" || token != firstJob.LeaseToken {
				t.Fatalf("stale writer changed lifecycle authority = %s/%s; want running/%s", state, token, firstJob.LeaseToken)
			}
			second, disposition, err := store.ClaimActivation(secondCtx, secondJob, now.Add(3*time.Second))
			if err != nil || disposition != SandboxLifecycleApplied {
				t.Fatalf("ClaimActivation(second) = %s, %v", disposition, err)
			}
			disposition, err = write(secondCtx, store, second, now.Add(4*time.Second))
			if err != nil || disposition != SandboxLifecycleApplied {
				t.Fatalf("current %s = %s, %v; want applied", name, disposition, err)
			}
		})
	}
}

func TestPostgreSQLSandboxLifecycleFinalizersRejectSupersededLeaseAfterTerminalOutcome(t *testing.T) {
	finalizers := map[string]func(context.Context, *PostgreSQLSandboxLifecycleStore, *queuev1.QueueJob, time.Time) (SandboxLifecycleDisposition, error){
		"invalid_budget": func(ctx context.Context, store *PostgreSQLSandboxLifecycleStore, job *queuev1.QueueJob, now time.Time) (SandboxLifecycleDisposition, error) {
			return store.FinalizeInvalidLifecycle(ctx, job, queue.KindSandboxActivate, now)
		},
		"over_budget": func(ctx context.Context, store *PostgreSQLSandboxLifecycleStore, job *queuev1.QueueJob, now time.Time) (SandboxLifecycleDisposition, error) {
			return store.FinalizeExhaustedLifecycle(ctx, job, queue.KindSandboxActivate, now)
		},
		"retry_at_budget": func(ctx context.Context, store *PostgreSQLSandboxLifecycleStore, job *queuev1.QueueJob, now time.Time) (SandboxLifecycleDisposition, error) {
			return store.FinalizeExhaustedLifecycle(ctx, job, queue.KindSandboxActivate, now)
		},
	}
	for name, finalize := range finalizers {
		t.Run(name, func(t *testing.T) {
			runtimeDB, adminDB := newSandboxServiceTestDB(t)
			seedSandboxExecutionStoreFixture(t, adminDB)
			ctx := sandboxTestQueueContext(t, runtimeDB)
			now := time.Now().UTC()
			coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
			execution := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
			if err := coordinator.WaitForActivation(ctx, execution, ExecutionNeedsCreation); err != nil {
				t.Fatalf("WaitForActivation: %v", err)
			}
			base := readLifecycleJob(t, adminDB, execution.Ref.ToolUseEventID, "waiting_activation_operation_id")
			queueJob := func(attempt int32, token string) *queuev1.QueueJob {
				return &queuev1.QueueJob{
					Id: base.JobID, WorkspaceId: base.WorkspaceID, Kind: queue.KindSandboxActivate,
					PartitionKey: queue.FormatSandboxLifecyclePartitionKey(workspace.ID(base.WorkspaceID), base.LogicalSandboxID),
					DedupeKey:    queue.FormatSandboxLifecycleDedupeKey(queue.KindSandboxActivate, workspace.ID(base.WorkspaceID), base.LogicalSandboxID, base.OperationID),
					LeasedBy:     "sandbox-finalizer-test", LeaseToken: token,
					LeasedUntil:  time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano),
					AttemptCount: attempt, MaxAttempts: int32(sandboxActivationMaxAttempts),
				}
			}
			store := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
			first := queueJob(1, "lease_finalizer_first")
			second := queueJob(2, "lease_finalizer_second")
			setLifecycleQueueLeaseForTest(t, adminDB, base.JobID, second.GetLeasedBy(), second.GetLeaseToken(), int(second.GetAttemptCount()), time.Now().UTC().Add(time.Minute))
			secondCtx := withTransportQueueAuthority(ctx, second)
			firstCtx := withTransportQueueAuthority(ctx, first)
			if disposition, err := finalize(secondCtx, store, second, now); err != nil || disposition != SandboxLifecycleApplied {
				t.Fatalf("successor finalizer = %s, %v; want applied", disposition, err)
			}
			if disposition, err := finalize(secondCtx, store, second, now.Add(time.Second)); err != nil || disposition != SandboxLifecycleNotApplicable {
				t.Fatalf("same-authority finalizer replay = %s, %v; want not_applicable", disposition, err)
			}
			if disposition, err := finalize(firstCtx, store, first, now.Add(2*time.Second)); !errors.Is(err, errQueueLeaseLost) || disposition != SandboxLifecycleLostAuthority {
				t.Fatalf("superseded finalizer = %s, %v; want lost_authority with direct Queue error", disposition, err)
			}
		})
	}
}

type delayedLifecycleFinalizerStore struct {
	SandboxLifecycleStore
	leaseToken string
	entered    chan struct{}
	release    chan struct{}
}

func (s *delayedLifecycleFinalizerStore) wait(job *queuev1.QueueJob) {
	if job.GetLeaseToken() != s.leaseToken {
		return
	}
	close(s.entered)
	<-s.release
}

func (s *delayedLifecycleFinalizerStore) FinalizeInvalidLifecycle(ctx context.Context, job *queuev1.QueueJob, kind string, now time.Time) (SandboxLifecycleDisposition, error) {
	s.wait(job)
	return s.SandboxLifecycleStore.FinalizeInvalidLifecycle(ctx, job, kind, now)
}

func (s *delayedLifecycleFinalizerStore) FinalizeExhaustedLifecycle(ctx context.Context, job *queuev1.QueueJob, kind string, now time.Time) (SandboxLifecycleDisposition, error) {
	s.wait(job)
	return s.SandboxLifecycleStore.FinalizeExhaustedLifecycle(ctx, job, kind, now)
}

func TestSandboxActivationFinalizersRejectDelayedAttemptAfterSuccessorClaim(t *testing.T) {
	tests := []struct {
		name         string
		attemptCount int32
		maxAttempts  int32
		retryPath    bool
	}{
		{name: "invalid budget", attemptCount: 1, maxAttempts: 0},
		{name: "pre-claim over budget", attemptCount: 2, maxAttempts: 1},
		{name: "retry at budget", attemptCount: 1, maxAttempts: 1, retryPath: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtimeDB, adminDB := newSandboxServiceTestDB(t)
			seedSandboxExecutionStoreFixture(t, adminDB)
			ctx := sandboxTestQueueContext(t, runtimeDB)
			now := time.Now().UTC()
			client := dbconnect.NewClientForTesting(runtimeDB)
			coordinator := NewPostgreSQLSandboxExecutionCoordinator(client, 30*time.Minute)
			execution := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
			if err := coordinator.WaitForActivation(ctx, execution, ExecutionNeedsCreation); err != nil {
				t.Fatalf("WaitForActivation: %v", err)
			}
			base := readLifecycleJob(t, adminDB, execution.Ref.ToolUseEventID, "waiting_activation_operation_id")
			const firstToken = "lease_delayed_finalizer"
			store := &delayedLifecycleFinalizerStore{
				SandboxLifecycleStore: NewPostgreSQLSandboxLifecycleStore(client, &fixedSandboxResourceSource{}, 30*time.Minute),
				leaseToken:            firstToken, entered: make(chan struct{}), release: make(chan struct{}),
			}
			queueJob := func(attempt int32, maxAttempts int32, token string) *queuev1.QueueJob {
				return &queuev1.QueueJob{
					Id: base.JobID, WorkspaceId: base.WorkspaceID, Kind: queue.KindSandboxActivate,
					PartitionKey: queue.FormatSandboxLifecyclePartitionKey(workspace.ID(base.WorkspaceID), base.LogicalSandboxID),
					DedupeKey:    queue.FormatSandboxLifecycleDedupeKey(queue.KindSandboxActivate, workspace.ID(base.WorkspaceID), base.LogicalSandboxID, base.OperationID),
					PayloadJson:  `{"workspace_id":"` + base.WorkspaceID + `","session_id":"` + base.SessionID + `","logical_sandbox_id":"` + base.LogicalSandboxID + `","operation_id":"` + base.OperationID + `"}`,
					LeasedBy:     "sandbox-lifecycle-test", LeaseToken: token, LeasedUntil: now.Add(time.Minute).Format(time.RFC3339Nano),
					AttemptCount: attempt, MaxAttempts: maxAttempts,
				}
			}
			firstJob := queueJob(test.attemptCount, test.maxAttempts, firstToken)
			setLifecycleQueueLeaseForTest(t, adminDB, base.JobID, firstJob.GetLeasedBy(), firstJob.GetLeaseToken(), int(firstJob.GetAttemptCount()), now.Add(time.Minute))
			queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{firstJob}}
			adapter := &recordingLifecycleAdapter{}
			if test.retryPath {
				adapter.resolution = ProviderOutcome[ActivationResolution]{EffectBoundary: ProviderProvedNotStarted, Disposition: ProviderRetryable, ErrorKind: "provider_transition_in_progress"}
			}
			registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
			if err != nil {
				t.Fatalf("NewProviderRegistry: %v", err)
			}
			result := make(chan error, 1)
			go func() {
				result <- (&SandboxActivationJobRunner{
					Queue: queueClient, Store: store, Providers: registry,
					Config: SandboxLifecycleRunnerConfig{WorkspaceID: base.WorkspaceID, LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second},
				}).RunOnce(context.Background())
			}()
			select {
			case <-store.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("first attempt did not enter lifecycle finalizer")
			}

			successor := base
			successor.AttemptCount = int(test.attemptCount + 1)
			successor.LeaseOwner = "sandbox-lifecycle-successor"
			successor.LeaseToken = "lease_finalizer_successor"
			successor.LeaseExpiresAt = now.Add(2 * time.Minute)
			setLifecycleQueueLeaseForTest(t, adminDB, successor.JobID, successor.LeaseOwner, successor.LeaseToken, successor.AttemptCount, successor.LeaseExpiresAt)
			if _, disposition, err := store.ClaimActivation(ctx, successor, now.Add(time.Second)); err != nil || disposition != SandboxLifecycleApplied {
				t.Fatalf("successor ClaimActivation = %s, %v; want applied", disposition, err)
			}
			close(store.release)
			select {
			case err := <-result:
				if !errors.Is(err, errQueueLeaseLost) {
					t.Fatalf("delayed finalizer error = %v; want Queue authority loss", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("delayed finalizer did not return")
			}
			if len(queueClient.transitions) != 0 {
				t.Fatalf("delayed finalizer Queue transitions = %v; want none", queueClient.transitions)
			}
			var token string
			if err := adminDB.QueryRow(`SELECT lease_token FROM sandbox_lifecycle_operations
				WHERE workspace_id=$1 AND operation_id=$2`, base.WorkspaceID, base.OperationID).Scan(&token); err != nil {
				t.Fatalf("read successor lifecycle authority: %v", err)
			}
			if token != successor.LeaseToken {
				t.Fatalf("lifecycle lease token = %q; want successor %q", token, successor.LeaseToken)
			}
		})
	}
}

func TestPostgreSQLSandboxLifecycleClaimsAreMonotonic(t *testing.T) {
	t.Run("activation", func(t *testing.T) {
		runtimeDB, adminDB := newSandboxServiceTestDB(t)
		seedSandboxExecutionStoreFixture(t, adminDB)
		ctx := sandboxTestQueueContext(t, runtimeDB)
		now := time.Now().UTC()
		coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
		work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
		if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
			t.Fatalf("WaitForActivation: %v", err)
		}
		store := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
		job := readLifecycleJob(t, adminDB, work.Ref.ToolUseEventID, "waiting_activation_operation_id")
		assertLifecycleClaimAuthority(t, adminDB, job, now, func(job SandboxLifecycleJob) (SandboxLifecycleDisposition, error) {
			_, disposition, err := store.ClaimActivation(withLifecycleJobQueueAuthority(ctx, job), job, now)
			return disposition, err
		})
	})

	t.Run("materialization", func(t *testing.T) {
		runtimeDB, adminDB := newSandboxServiceTestDB(t)
		seedSandboxExecutionStoreFixture(t, adminDB)
		ctx := sandboxTestQueueContext(t, runtimeDB)
		now := time.Now().UTC()
		coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
		work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
		if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
			t.Fatalf("WaitForActivation: %v", err)
		}
		store := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
		activationJob := readLifecycleJob(t, adminDB, work.Ref.ToolUseEventID, "waiting_activation_operation_id")
		activation, disposition, err := store.ClaimActivation(ctx, activationJob, now)
		if err != nil || disposition != SandboxLifecycleApplied {
			t.Fatalf("ClaimActivation = %s, %v", disposition, err)
		}
		if _, err := store.CompleteActivation(ctx, activation, sandbox.ProviderHandle{Provider: "daytona", SandboxID: "provider_claim_monotonic"}, now); err != nil {
			t.Fatalf("CompleteActivation: %v", err)
		}
		job := readLifecycleJob(t, adminDB, work.Ref.ToolUseEventID, "waiting_materialization_operation_id")
		assertLifecycleClaimAuthority(t, adminDB, job, now, func(job SandboxLifecycleJob) (SandboxLifecycleDisposition, error) {
			_, disposition, err := store.ClaimMaterialization(withLifecycleJobQueueAuthority(ctx, job), job, now)
			return disposition, err
		})
	})

	t.Run("release", func(t *testing.T) {
		runtimeDB, adminDB := newSandboxServiceTestDB(t)
		seedSandboxExecutionStoreFixture(t, adminDB)
		ctx := sandboxTestQueueContext(t, runtimeDB)
		now := time.Now().UTC()
		seedReadySandboxBinding(t, adminDB, now)
		client := dbconnect.NewClientForTesting(runtimeDB)
		var operationID string
		if err := client.WithWorkspaceTx(context.Background(), "ws_execution_store", "sandbox.release.claim_monotonic", func(tx *dbconnect.Tx) error {
			var err error
			operationID, _, err = EnsureSandboxReleaseTx(context.Background(), tx, "ws_execution_store", "sesn_execution_store", SandboxReleaseSessionDelete, "provider_execution_store", now)
			return err
		}); err != nil {
			t.Fatalf("EnsureSandboxReleaseTx: %v", err)
		}
		store := NewPostgreSQLSandboxLifecycleStore(client, &fixedSandboxResourceSource{}, 30*time.Minute)
		job := lifecycleJobByOperationID(t, adminDB, "ws_execution_store", operationID)
		job.LeaseOwner = "sandbox-release-test"
		assertLifecycleClaimAuthority(t, adminDB, job, now, func(job SandboxLifecycleJob) (SandboxLifecycleDisposition, error) {
			_, disposition, err := store.ClaimRelease(withLifecycleJobQueueAuthority(ctx, job), job, now)
			return disposition, err
		})
	})
}

func assertLifecycleClaimAuthority(t *testing.T, db *sql.DB, job SandboxLifecycleJob, now time.Time, claim func(SandboxLifecycleJob) (SandboxLifecycleDisposition, error)) {
	t.Helper()
	successor := job
	successor.AttemptCount = 2
	successor.LeaseToken = "lease_successor"
	successor.LeaseExpiresAt = now.Add(2 * time.Minute)
	setLifecycleQueueLeaseForTest(t, db, successor.JobID, successor.LeaseOwner, successor.LeaseToken, successor.AttemptCount, successor.LeaseExpiresAt)
	if disposition, err := claim(successor); err != nil || disposition != SandboxLifecycleApplied {
		t.Fatalf("successor Claim = %s, %v", disposition, err)
	}

	lower := successor
	lower.AttemptCount = 1
	lower.LeaseToken = "lease_lower"
	if disposition, err := claim(lower); !errors.Is(err, errQueueLeaseLost) || disposition != SandboxLifecycleLostAuthority {
		t.Fatalf("lower-attempt Claim = %s, %v; want direct Queue authority loss", disposition, err)
	}
	sameAttempt := successor
	sameAttempt.LeaseToken = "lease_conflict"
	if disposition, err := claim(sameAttempt); !errors.Is(err, errQueueLeaseLost) || disposition != SandboxLifecycleLostAuthority {
		t.Fatalf("same-attempt conflicting Claim = %s, %v; want direct Queue authority loss", disposition, err)
	}
	expired := successor
	expired.AttemptCount = 3
	expired.LeaseToken = "lease_expired"
	expired.LeaseExpiresAt = now.Add(-time.Second)
	if disposition, err := claim(expired); !errors.Is(err, errQueueLeaseLost) || disposition != SandboxLifecycleLostAuthority {
		t.Fatalf("expired Claim = %s, %v; want direct Queue authority loss", disposition, err)
	}
	if disposition, err := claim(successor); err != nil || disposition != SandboxLifecycleApplied {
		t.Fatalf("same-token rejoin = %s, %v; want applied", disposition, err)
	}
}

type delayedActivationClaimStore struct {
	SandboxLifecycleStore
	attemptOneEntered chan struct{}
	allowAttemptOne   chan struct{}
	attemptTwoClaimed chan struct{}
	allowAttemptTwo   chan struct{}
}

func (s *delayedActivationClaimStore) ClaimActivation(ctx context.Context, job SandboxLifecycleJob, now time.Time) (SandboxActivationWork, SandboxLifecycleDisposition, error) {
	if job.AttemptCount == 1 {
		close(s.attemptOneEntered)
		<-s.allowAttemptOne
	}
	work, disposition, err := s.SandboxLifecycleStore.ClaimActivation(ctx, job, now)
	if job.AttemptCount == 2 && err == nil && disposition == SandboxLifecycleApplied {
		close(s.attemptTwoClaimed)
		<-s.allowAttemptTwo
	}
	return work, disposition, err
}

func TestSandboxActivationRunnerRejectsDelayedClaimAfterSuccessorTakesAuthority(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	client := dbconnect.NewClientForTesting(runtimeDB)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(client, 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	base := readLifecycleJob(t, adminDB, work.Ref.ToolUseEventID, "waiting_activation_operation_id")
	store := &delayedActivationClaimStore{
		SandboxLifecycleStore: NewPostgreSQLSandboxLifecycleStore(client, &fixedSandboxResourceSource{}, 30*time.Minute),
		attemptOneEntered:     make(chan struct{}), allowAttemptOne: make(chan struct{}),
		attemptTwoClaimed: make(chan struct{}), allowAttemptTwo: make(chan struct{}),
	}
	adapter := &recordingLifecycleAdapter{
		resolution: ProviderOutcome[ActivationResolution]{Value: ActivationResolution{
			Found: true, Handle: sandbox.ProviderHandle{Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider_successor"},
		}},
		inspection: ProviderOutcome[ExecutionReadiness]{Value: ExecutionReady},
	}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	queueJob := func(attempt int32, token string) *queuev1.QueueJob {
		expiresAt := time.Now().UTC().Add(30 * time.Second)
		return &queuev1.QueueJob{
			Id: base.JobID, WorkspaceId: base.WorkspaceID, Kind: queue.KindSandboxActivate,
			PartitionKey: queue.FormatSandboxLifecyclePartitionKey(workspace.ID(base.WorkspaceID), base.LogicalSandboxID),
			DedupeKey:    queue.FormatSandboxLifecycleDedupeKey(queue.KindSandboxActivate, workspace.ID(base.WorkspaceID), base.LogicalSandboxID, base.OperationID),
			PayloadJson:  `{"workspace_id":"` + base.WorkspaceID + `","session_id":"` + base.SessionID + `","logical_sandbox_id":"` + base.LogicalSandboxID + `","operation_id":"` + base.OperationID + `"}`,
			LeasedBy:     "sandbox-lifecycle-test", LeaseToken: token, LeasedUntil: expiresAt.Format(time.RFC3339Nano),
			AttemptCount: attempt, MaxAttempts: int32(sandboxActivationMaxAttempts),
		}
	}
	run := func(queueClient *recordingSandboxQueue) error {
		return (&SandboxActivationJobRunner{
			Queue: queueClient, Store: store, Providers: registry,
			Config: SandboxLifecycleRunnerConfig{
				WorkspaceID: base.WorkspaceID, LeaseDuration: 30 * time.Second, HeartbeatInterval: 5 * time.Second,
			},
		}).RunOnce(context.Background())
	}
	firstQueue := &recordingSandboxQueue{leased: []*queuev1.QueueJob{queueJob(1, "lease_attempt_one")}}
	secondQueue := &recordingSandboxQueue{leased: []*queuev1.QueueJob{queueJob(2, "lease_attempt_two")}}
	setLifecycleQueueLeaseForTest(t, adminDB, base.JobID, "sandbox-lifecycle-test", "lease_attempt_one", 1, time.Now().UTC().Add(30*time.Second))
	firstResult := make(chan error, 1)
	secondResult := make(chan error, 1)
	go func() { firstResult <- run(firstQueue) }()
	<-store.attemptOneEntered
	setLifecycleQueueLeaseForTest(t, adminDB, base.JobID, "sandbox-lifecycle-test", "lease_attempt_two", 2, time.Now().UTC().Add(30*time.Second))
	go func() { secondResult <- run(secondQueue) }()
	<-store.attemptTwoClaimed
	close(store.allowAttemptOne)
	if err := <-firstResult; !errors.Is(err, errQueueLeaseLost) {
		t.Fatalf("delayed attempt error = %v; want lost authority", err)
	}
	if len(firstQueue.transitions) != 0 {
		t.Fatalf("delayed attempt Queue transitions = %v; want none", firstQueue.transitions)
	}
	if len(adapter.calls) != 0 {
		t.Fatalf("provider calls before successor resumes = %v; want none from delayed attempt", adapter.calls)
	}
	close(store.allowAttemptTwo)
	if err := <-secondResult; err != nil {
		t.Fatalf("successor RunOnce: %v", err)
	}
	if len(secondQueue.transitions) != 1 || secondQueue.transitions[0] != "ack:"+base.JobID {
		t.Fatalf("successor Queue transitions = %v; want one ack", secondQueue.transitions)
	}
}

func TestPostgreSQLProactiveMaterializationSkipsColdSandbox(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	seedProactiveMaterialization(t, adminDB, now)
	lifecycle := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
	job := lifecycleJobByOperationID(t, adminDB, "ws_execution_store", "sop_proactive_materialization")
	materializationCtx := leaseSandboxMaterializationJobForTest(t, runtimeDB, adminDB, &job)
	work, current, err := lifecycle.ClaimMaterialization(materializationCtx, job, now)
	if err != nil || current != SandboxLifecycleApplied {
		t.Fatalf("ClaimMaterialization = current %s err %v", current, err)
	}
	disposition, err := lifecycle.WaitMaterializationForActivation(materializationCtx, work, ExecutionNeedsActivation, now)
	if err != nil || disposition != SandboxLifecycleApplied {
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
	var partitionKey string
	if err := db.QueryRow(`SELECT o.queue_job_id, o.session_id, o.logical_sandbox_id, o.operation_id, o.queue_partition_key
		FROM sandbox_lifecycle_operations o
		JOIN session_runtime_tool_results r
		  ON r.workspace_id = o.workspace_id AND r.`+linkColumn+` = o.operation_id
		WHERE o.workspace_id = 'ws_execution_store' AND r.tool_use_event_id = $1`, eventID).Scan(
		&job.JobID, &job.SessionID, &job.LogicalSandboxID, &job.OperationID, &partitionKey,
	); err != nil {
		t.Fatalf("read lifecycle job: %v", err)
	}
	job.WorkspaceID = "ws_execution_store"
	job.AttemptCount = 1
	job.LeaseOwner = "sandbox-lifecycle-test"
	job.LeaseToken = "lease_" + job.JobID
	job.LeaseExpiresAt = time.Now().UTC().Add(time.Hour)
	if _, err := db.Exec(`UPDATE queue_jobs
		SET status=$1, lease_token=NULL, leased_by=NULL, leased_at=NULL, leased_until=NULL,
			acknowledged_at=clock_timestamp(), updated_at=clock_timestamp()
		WHERE workspace_id=$2 AND partition_key=$3 AND status=$4 AND id<>$5`,
		queue.StatusAcknowledged, job.WorkspaceID, partitionKey, queue.StatusLeased, job.JobID,
	); err != nil {
		t.Fatalf("close predecessor lifecycle Queue lease: %v", err)
	}
	if _, err := db.Exec(`UPDATE queue_jobs
		SET status=$1, leased_by=$2, lease_token=$3, leased_at=clock_timestamp(), leased_until=$4,
			attempt_count=GREATEST(attempt_count, 1)
		WHERE workspace_id=$5 AND id=$6`,
		queue.StatusLeased, job.LeaseOwner, job.LeaseToken, job.LeaseExpiresAt, job.WorkspaceID, job.JobID,
	); err != nil {
		t.Fatalf("lease lifecycle Queue job: %v", err)
	}
	return job
}

func setLifecycleQueueLeaseForTest(t *testing.T, db *sql.DB, jobID string, owner string, token string, attempt int, expiresAt time.Time) {
	t.Helper()
	if _, err := db.Exec(`UPDATE queue_jobs
		SET status=$1, leased_by=$2, lease_token=$3, leased_at=clock_timestamp(), leased_until=$4,
			attempt_count=$5, acknowledged_at=NULL, cancelled_at=NULL, dead_lettered_at=NULL,
			last_error_kind=NULL, last_error_message=NULL
		WHERE workspace_id='ws_execution_store' AND id=$6`,
		queue.StatusLeased, owner, token, expiresAt, attempt, jobID,
	); err != nil {
		t.Fatalf("set lifecycle Queue lease: %v", err)
	}
}

func leaseSandboxLifecycleJob(t *testing.T, store *queue.PostgreSQLQueueStore, kind string, owner string) SandboxLifecycleJob {
	t.Helper()
	jobs, err := store.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: "ws_execution_store", Kinds: []string{kind}, LeaseOwner: owner,
		MaxJobs: 1, LeaseDuration: time.Minute,
	})
	if err != nil || len(jobs) != 1 || jobs[0].LeasedUntil == nil {
		t.Fatalf("lease %s lifecycle job = %#v, %v", kind, jobs, err)
	}
	transport := &queuev1.QueueJob{
		Id: jobs[0].ID, WorkspaceId: string(jobs[0].WorkspaceID), Kind: jobs[0].Kind,
		PartitionKey: jobs[0].PartitionKey, DedupeKey: jobs[0].DedupeKey,
		PayloadVersion: int32(jobs[0].PayloadVersion), PayloadJson: string(jobs[0].PayloadJSON),
		LeasedBy: jobs[0].LeasedBy, LeaseToken: jobs[0].LeaseToken,
		LeasedUntil:  jobs[0].LeasedUntil.UTC().Format(time.RFC3339Nano),
		AttemptCount: int32(jobs[0].AttemptCount), MaxAttempts: int32(jobs[0].MaxAttempts),
	}
	job, err := DecodeSandboxLifecycleJob(transport, kind)
	if err != nil {
		t.Fatalf("DecodeSandboxLifecycleJob(%s): %v", kind, err)
	}
	return job
}

func reclaimSandboxLifecycleJob(t *testing.T, store *queue.PostgreSQLQueueStore, db *sql.DB, job SandboxLifecycleJob) {
	t.Helper()
	if _, err := db.Exec(`UPDATE queue_jobs SET leased_until=clock_timestamp()-interval '1 second'
		WHERE workspace_id=$1 AND id=$2`, job.WorkspaceID, job.JobID); err != nil {
		t.Fatalf("expire lifecycle Queue job: %v", err)
	}
	count, err := store.ReclaimExpiredLeases(context.Background(), queue.ReclaimExpiredLeasesRequest{
		WorkspaceID: workspace.ID(job.WorkspaceID), Limit: 1,
	})
	if err != nil || count != 1 {
		t.Fatalf("reclaim lifecycle Queue job = %d, %v; want 1,nil", count, err)
	}
}

func assertLifecycleRunnerReleaseFence(t *testing.T, db *sql.DB, job SandboxLifecycleJob, providerCalls []string) {
	t.Helper()
	if len(providerCalls) != 0 {
		t.Fatalf("provider calls = %v; want none after release fence", providerCalls)
	}
	var operationState, queueStatus string
	if err := db.QueryRow(`SELECT state FROM sandbox_lifecycle_operations
		WHERE workspace_id=$1 AND operation_id=$2`, job.WorkspaceID, job.OperationID).Scan(&operationState); err != nil {
		t.Fatalf("read lifecycle operation: %v", err)
	}
	if err := db.QueryRow(`SELECT status FROM queue_jobs
		WHERE workspace_id=$1 AND id=$2`, job.WorkspaceID, job.JobID).Scan(&queueStatus); err != nil {
		t.Fatalf("read lifecycle Queue job: %v", err)
	}
	if operationState != "abandoned" || queueStatus != queue.StatusAcknowledged {
		t.Fatalf("release-fenced redelivery = operation %q Queue %q; want abandoned/acknowledged", operationState, queueStatus)
	}
}

func lifecycleJobByOperationID(t *testing.T, db *sql.DB, workspaceID string, operationID string) SandboxLifecycleJob {
	t.Helper()
	var job SandboxLifecycleJob
	var partitionKey string
	if err := db.QueryRow(`SELECT queue_job_id, session_id, logical_sandbox_id, queue_partition_key
		FROM sandbox_lifecycle_operations
		WHERE workspace_id = $1 AND operation_id = $2`, workspaceID, operationID).Scan(
		&job.JobID, &job.SessionID, &job.LogicalSandboxID, &partitionKey,
	); err != nil {
		t.Fatalf("read lifecycle job %s: %v", operationID, err)
	}
	job.WorkspaceID = workspaceID
	job.OperationID = operationID
	job.AttemptCount = 1
	job.LeaseOwner = "sandbox-lifecycle-test"
	job.LeaseToken = "lease_" + job.JobID
	job.LeaseExpiresAt = time.Now().UTC().Add(time.Hour)
	if _, err := db.Exec(`UPDATE queue_jobs
		SET status=$1, lease_token=NULL, leased_by=NULL, leased_at=NULL, leased_until=NULL,
			acknowledged_at=clock_timestamp(), updated_at=clock_timestamp()
		WHERE workspace_id=$2 AND partition_key=$3 AND status=$4 AND id<>$5`,
		queue.StatusAcknowledged, workspaceID, partitionKey, queue.StatusLeased, job.JobID,
	); err != nil {
		t.Fatalf("close predecessor lifecycle Queue lease: %v", err)
	}
	setLifecycleQueueLeaseForTest(t, db, job.JobID, job.LeaseOwner, job.LeaseToken, job.AttemptCount, job.LeaseExpiresAt)
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
		'materialize', 'pending', 1, 1, 1, 'provider_proactive', '{}', 'qjob_proactive_mat',
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
