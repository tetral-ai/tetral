package tetralsandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
)

func TestEnvironmentArtifactStoreBuildReadyEnqueuesFanout(t *testing.T) {
	runtime, admin := newEnvironmentArtifactStoreTestDB(t)
	ctx := sandboxTestQueueContext(t, runtime)
	seedEnvironmentArtifactStoreEnvironment(t, admin, "ws_env_store", "env_build")
	seedEnvironmentArtifact(t, admin, "ws_env_store", "env_build", 7, "pending", "", `{"pip":["pandas==2.2.0"],"apt":["git"]}`)
	store := NewEnvironmentArtifactStore(dbconnect.NewClientForTesting(runtime))
	job := leaseEnvironmentBuildJob(t, runtime, "ws_env_store", "env_build", 7, fixedEnvironmentStoreTime, 10*time.Minute)

	input, claimed, err := store.ClaimEnvironmentBuild(ctx, job, fixedEnvironmentStoreTime)
	if err != nil {
		t.Fatalf("ClaimEnvironmentBuild: %v", err)
	}
	if !claimed || input.ArtifactInputHash != "hash_packages" || input.NormalizedPackages["pip"][0] != "pandas==2.2.0" {
		t.Fatalf("input = %+v claimed=%v; want packages from durable artifact", input, claimed)
	}
	assertEnvironmentArtifactStatus(t, admin, "ws_env_store", "env_build", 7, "building", "")
	if _, claimed, err := store.ClaimEnvironmentBuild(ctx, job, fixedEnvironmentStoreTime.Add(time.Second)); err != nil || !claimed {
		t.Fatalf("ClaimEnvironmentBuild(redelivery) = claimed %t err %v; want true/nil", claimed, err)
	}

	if err := store.MarkEnvironmentBuildReady(ctx, job, "snapshot_ready", fixedEnvironmentStoreTime.Add(time.Minute)); err != nil {
		t.Fatalf("MarkEnvironmentBuildReady: %v", err)
	}
	assertEnvironmentArtifactStatus(t, admin, "ws_env_store", "env_build", 7, "ready", "snapshot_ready")
	assertQueueJobCount(t, admin, "ws_env_store", "environment_ready_fanout", 1)
}

func TestEnvironmentArtifactStoreAuthorizesProviderCreateOnlyOnce(t *testing.T) {
	runtime, admin := newEnvironmentArtifactStoreTestDB(t)
	ctx := sandboxTestQueueContext(t, runtime)
	seedEnvironmentArtifactStoreEnvironment(t, admin, "ws_env_create_fence", "env_build")
	seedEnvironmentArtifact(t, admin, "ws_env_create_fence", "env_build", 7, "pending", "", `{"apt":["git"]}`)
	store := NewEnvironmentArtifactStore(dbconnect.NewClientForTesting(runtime))
	job := leaseEnvironmentBuildJob(t, runtime, "ws_env_create_fence", "env_build", 7, fixedEnvironmentStoreTime, 10*time.Minute)
	if _, claimed, err := store.ClaimEnvironmentBuild(ctx, job, fixedEnvironmentStoreTime); err != nil || !claimed {
		t.Fatalf("ClaimEnvironmentBuild = claimed %t err %v; want true/nil", claimed, err)
	}
	authorized, err := store.AuthorizeEnvironmentArtifactCreate(ctx, job, fixedEnvironmentStoreTime.Add(time.Second))
	if err != nil || !authorized {
		t.Fatalf("first authorize = %t, %v; want true/nil", authorized, err)
	}
	authorized, err = store.AuthorizeEnvironmentArtifactCreate(ctx, job, fixedEnvironmentStoreTime.Add(2*time.Second))
	if err != nil || authorized {
		t.Fatalf("second authorize = %t, %v; want false/nil", authorized, err)
	}
	var submittedAt sql.NullTime
	if err := admin.QueryRow(`SELECT provider_create_submitted_at FROM environment_artifacts
		WHERE workspace_id=$1 AND environment_id=$2 AND generation=$3`, job.WorkspaceID, job.EnvironmentID, job.Generation).Scan(&submittedAt); err != nil {
		t.Fatalf("read provider create fence: %v", err)
	}
	if !submittedAt.Valid {
		t.Fatal("provider create fence was not persisted")
	}
}

func TestEnvironmentArtifactStoreTerminalFailureSettlesIdenticalArtifactGenerations(t *testing.T) {
	runtime, admin := newEnvironmentArtifactStoreTestDB(t)
	ctx := sandboxTestQueueContext(t, runtime)
	seedEnvironmentArtifactStoreEnvironment(t, admin, "ws_env_finalize", "env_build")
	seedEnvironmentArtifact(t, admin, "ws_env_finalize", "env_build", 7, "pending", "", `{"apt":["git"]}`)
	seedEnvironmentArtifact(t, admin, "ws_env_finalize", "env_build", 8, "pending", "", `{"apt":["git"]}`)
	store := NewEnvironmentArtifactStore(dbconnect.NewClientForTesting(runtime))
	job := leaseEnvironmentBuildJob(t, runtime, "ws_env_finalize", "env_build", 7, fixedEnvironmentStoreTime, 10*time.Minute)
	if _, claimed, err := store.ClaimEnvironmentBuild(ctx, job, fixedEnvironmentStoreTime); err != nil || !claimed {
		t.Fatalf("ClaimEnvironmentBuild = claimed %t err %v; want true/nil", claimed, err)
	}
	if err := store.MarkEnvironmentBuildTerminalFailure(ctx, job, EnvironmentArtifactFailure{
		Stage: "build_artifact", LastErrorKind: "environment_build_attempts_exhausted", Reason: "attempt budget exhausted",
	}, fixedEnvironmentStoreTime); err != nil {
		t.Fatalf("MarkEnvironmentBuildTerminalFailure: %v", err)
	}
	assertEnvironmentArtifactStatus(t, admin, "ws_env_finalize", "env_build", 7, "failed", "")
	assertEnvironmentArtifactStatus(t, admin, "ws_env_finalize", "env_build", 8, "failed", "")
}

func TestEnvironmentArtifactStoreTerminalFailureSettlesWaitingActivation(t *testing.T) {
	runtime, admin := newEnvironmentArtifactStoreTestDB(t)
	seedSandboxExecutionStoreFixture(t, admin)
	ctx := sandboxTestQueueContext(t, runtime)
	if _, err := admin.Exec(`UPDATE environment_artifacts
		SET status = 'pending', provider_artifact_ref = NULL
		WHERE workspace_id = 'ws_execution_store' AND environment_id = 'env_execution_store' AND generation = 1`); err != nil {
		t.Fatalf("mark artifact pending: %v", err)
	}
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtime), 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	var operationID string
	if err := admin.QueryRow(`SELECT waiting_activation_operation_id FROM session_runtime_tool_results
		WHERE workspace_id='ws_execution_store' AND tool_use_event_id='evt_execution_a'`).Scan(&operationID); err != nil {
		t.Fatalf("read waiting activation: %v", err)
	}
	store := NewEnvironmentArtifactStore(dbconnect.NewClientForTesting(runtime))
	job := leaseEnvironmentBuildJob(t, runtime, "ws_execution_store", "env_execution_store", 1, fixedEnvironmentStoreTime, 10*time.Minute)
	if _, claimed, err := store.ClaimEnvironmentBuild(ctx, job, fixedEnvironmentStoreTime); err != nil || !claimed {
		t.Fatalf("ClaimEnvironmentBuild = claimed %t err %v; want true/nil", claimed, err)
	}
	failure := EnvironmentArtifactFailure{
		Stage: "build_artifact", LastErrorKind: "environment_build_attempts_exhausted", Reason: "attempt budget exhausted",
	}
	if err := store.MarkEnvironmentBuildTerminalFailure(ctx, job, failure, fixedEnvironmentStoreTime); err != nil {
		t.Fatalf("MarkEnvironmentBuildTerminalFailure: %v", err)
	}
	var operationState, executionState, resultJSON string
	if err := admin.QueryRow(`SELECT state FROM sandbox_lifecycle_operations
		WHERE workspace_id='ws_execution_store' AND operation_id=$1`, operationID).Scan(&operationState); err != nil {
		t.Fatalf("read failed activation: %v", err)
	}
	if err := admin.QueryRow(`SELECT execution_state, result_json FROM session_runtime_tool_results
		WHERE workspace_id='ws_execution_store' AND tool_use_event_id='evt_execution_a'`).Scan(&executionState, &resultJSON); err != nil {
		t.Fatalf("read failed execution: %v", err)
	}
	if operationState != "failed" || executionState != "terminal_unconsumed" || !strings.Contains(resultJSON, failure.LastErrorKind) {
		t.Fatalf("terminal states = %q/%q result=%s; want failed/terminal_unconsumed with %q", operationState, executionState, resultJSON, failure.LastErrorKind)
	}
}

func TestEnvironmentArtifactStoreRearmsCreateAfterExplicitProviderRejection(t *testing.T) {
	runtime, admin := newEnvironmentArtifactStoreTestDB(t)
	ctx := sandboxTestQueueContext(t, runtime)
	seedEnvironmentArtifactStoreEnvironment(t, admin, "ws_env_create_rearm", "env_build")
	seedEnvironmentArtifact(t, admin, "ws_env_create_rearm", "env_build", 7, "pending", "", `{"apt":["git"]}`)
	store := NewEnvironmentArtifactStore(dbconnect.NewClientForTesting(runtime))
	job := leaseEnvironmentBuildJob(t, runtime, "ws_env_create_rearm", "env_build", 7, fixedEnvironmentStoreTime, 10*time.Minute)
	if _, claimed, err := store.ClaimEnvironmentBuild(ctx, job, fixedEnvironmentStoreTime); err != nil || !claimed {
		t.Fatalf("ClaimEnvironmentBuild = claimed %t err %v; want true/nil", claimed, err)
	}
	if authorized, err := store.AuthorizeEnvironmentArtifactCreate(ctx, job, fixedEnvironmentStoreTime.Add(time.Second)); err != nil || !authorized {
		t.Fatalf("first authorize = %t, %v; want true/nil", authorized, err)
	}
	if err := store.MarkEnvironmentBuildRetryableFailure(ctx, job, EnvironmentArtifactFailure{
		Stage: string(sandbox.StageBuildArtifact), LastErrorKind: string(sandbox.ProviderErrorUnavailable), Retryable: true,
	}, true, fixedEnvironmentStoreTime.Add(2*time.Second)); err != nil {
		t.Fatalf("MarkEnvironmentBuildRetryableFailure: %v", err)
	}
	if _, claimed, err := store.ClaimEnvironmentBuild(ctx, job, fixedEnvironmentStoreTime.Add(3*time.Second)); err != nil || !claimed {
		t.Fatalf("reclaim = claimed %t err %v; want true/nil", claimed, err)
	}
	if authorized, err := store.AuthorizeEnvironmentArtifactCreate(ctx, job, fixedEnvironmentStoreTime.Add(4*time.Second)); err != nil || !authorized {
		t.Fatalf("reauthorize = %t, %v; want true/nil", authorized, err)
	}
}

func TestEnvironmentArtifactReadyFanoutWakesWaitingSandboxActivation(t *testing.T) {
	runtime, admin := newEnvironmentArtifactStoreTestDB(t)
	seedSandboxExecutionStoreFixture(t, admin)
	ctx := sandboxTestQueueContext(t, runtime)
	if _, err := admin.Exec(`UPDATE environment_artifacts
		SET status = 'pending', provider_artifact_ref = NULL
		WHERE workspace_id = 'ws_execution_store' AND environment_id = 'env_execution_store' AND generation = 1`); err != nil {
		t.Fatalf("mark artifact building: %v", err)
	}
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtime), 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	var operationID string
	if err := admin.QueryRow(`SELECT waiting_activation_operation_id FROM session_runtime_tool_results
		WHERE workspace_id = 'ws_execution_store' AND tool_use_event_id = 'evt_execution_a'`).Scan(&operationID); err != nil {
		t.Fatalf("read waiting activation: %v", err)
	}
	store := NewEnvironmentArtifactStore(dbconnect.NewClientForTesting(runtime))
	job := leaseEnvironmentBuildJob(t, runtime, "ws_execution_store", "env_execution_store", 1, fixedEnvironmentStoreTime, 10*time.Minute)
	if _, claimed, err := store.ClaimEnvironmentBuild(ctx, job, fixedEnvironmentStoreTime); err != nil || !claimed {
		t.Fatalf("ClaimEnvironmentBuild = claimed %t err %v; want true/nil", claimed, err)
	}
	if err := store.MarkEnvironmentBuildReady(ctx, job, "artifact_execution_store", fixedEnvironmentStoreTime); err != nil {
		t.Fatalf("MarkEnvironmentBuildReady: %v", err)
	}
	if _, err := store.FanoutReadyEnvironment(ctx, EnvironmentReadyFanoutJob{
		JobID: job.JobID, LeaseToken: job.LeaseToken, WorkspaceID: job.WorkspaceID,
		EnvironmentID: job.EnvironmentID, Generation: job.Generation,
	}, fixedEnvironmentStoreTime); err != nil {
		t.Fatalf("FanoutReadyEnvironment: %v", err)
	}
	var state string
	var queueJobID sql.NullString
	if err := admin.QueryRow(`SELECT state, queue_job_id FROM sandbox_lifecycle_operations
		WHERE workspace_id = $1 AND operation_id = $2`, job.WorkspaceID, operationID).Scan(&state, &queueJobID); err != nil {
		t.Fatalf("read activation after fanout: %v", err)
	}
	if state != "pending" || !queueJobID.Valid {
		t.Fatalf("activation after artifact ready = state %q queue %v; want pending with notification", state, queueJobID)
	}
}

func TestEnvironmentArtifactReadyFanoutWaitsForActivePredecessorNotification(t *testing.T) {
	runtime, admin := newEnvironmentArtifactStoreTestDB(t)
	seedSandboxExecutionStoreFixture(t, admin)
	ctx := sandboxTestQueueContext(t, runtime)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtime), 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	var operationID, predecessorJobID string
	if err := admin.QueryRow(`SELECT waiting_activation_operation_id FROM session_runtime_tool_results
		WHERE workspace_id='ws_execution_store' AND tool_use_event_id='evt_execution_a'`).Scan(&operationID); err != nil {
		t.Fatalf("read activation operation: %v", err)
	}
	if err := admin.QueryRow(`SELECT queue_job_id FROM sandbox_lifecycle_operations
		WHERE workspace_id='ws_execution_store' AND operation_id=$1`, operationID).Scan(&predecessorJobID); err != nil {
		t.Fatalf("read predecessor notification: %v", err)
	}
	if _, err := admin.Exec(`UPDATE sandbox_lifecycle_operations
		SET state='waiting_artifact', queue_job_id=NULL, queue_kind=NULL,
		    queue_partition_key=NULL, queue_dedupe_key=NULL,
		    lease_owner='sandbox-old', lease_token='lease-old',
		    lease_expires_at=clock_timestamp()+interval '1 minute', attempt_count=4
		WHERE workspace_id='ws_execution_store' AND operation_id=$1`, operationID); err != nil {
		t.Fatalf("park activation for artifact: %v", err)
	}
	if _, err := admin.Exec(`UPDATE environment_artifacts SET status='ready', provider_artifact_ref='artifact_ready'
		WHERE workspace_id='ws_execution_store' AND environment_id='env_execution_store' AND generation=1`); err != nil {
		t.Fatalf("mark artifact ready: %v", err)
	}
	store := NewEnvironmentArtifactStore(dbconnect.NewClientForTesting(runtime))
	job := EnvironmentReadyFanoutJob{WorkspaceID: "ws_execution_store", EnvironmentID: "env_execution_store", Generation: 1}
	if _, err := store.FanoutReadyEnvironment(ctx, job, fixedEnvironmentStoreTime); err == nil {
		t.Fatal("fanout succeeded while the predecessor notification still owned the dedupe key")
	}
	var state string
	var queueJobID sql.NullString
	if err := admin.QueryRow(`SELECT state, queue_job_id FROM sandbox_lifecycle_operations
		WHERE workspace_id='ws_execution_store' AND operation_id=$1`, operationID).Scan(&state, &queueJobID); err != nil {
		t.Fatalf("read activation after blocked fanout: %v", err)
	}
	if state != "waiting_artifact" || queueJobID.Valid {
		t.Fatalf("activation after blocked fanout = %q/%v; want waiting_artifact without notification", state, queueJobID)
	}
	if _, err := admin.Exec(`UPDATE queue_jobs SET status='acknowledged', acknowledged_at=$2, updated_at=$2
		WHERE workspace_id='ws_execution_store' AND id=$1`, predecessorJobID, fixedEnvironmentStoreTime); err != nil {
		t.Fatalf("close predecessor notification: %v", err)
	}
	if _, err := store.FanoutReadyEnvironment(ctx, job, fixedEnvironmentStoreTime.Add(time.Second)); err != nil {
		t.Fatalf("fanout after predecessor closure: %v", err)
	}
	var currentJobID string
	if err := admin.QueryRow(`SELECT state, queue_job_id FROM sandbox_lifecycle_operations
		WHERE workspace_id='ws_execution_store' AND operation_id=$1`, operationID).Scan(&state, &currentJobID); err != nil {
		t.Fatalf("read requeued activation: %v", err)
	}
	var currentStatus string
	var attemptCount int
	var leaseOwner, leaseToken sql.NullString
	var leaseExpiresAt sql.NullTime
	if err := admin.QueryRow(`SELECT status FROM queue_jobs
		WHERE workspace_id='ws_execution_store' AND id=$1`, currentJobID).Scan(&currentStatus); err != nil {
		t.Fatalf("read requeued notification: %v", err)
	}
	if err := admin.QueryRow(`SELECT attempt_count, lease_owner, lease_token, lease_expires_at
		FROM sandbox_lifecycle_operations
		WHERE workspace_id='ws_execution_store' AND operation_id=$1`, operationID).Scan(
		&attemptCount, &leaseOwner, &leaseToken, &leaseExpiresAt,
	); err != nil {
		t.Fatalf("read requeued activation authority: %v", err)
	}
	if state != "pending" || currentJobID == predecessorJobID || currentStatus != "pending" {
		t.Fatalf("requeued activation = %q/%q/%q; want fresh pending notification", state, currentJobID, currentStatus)
	}
	if attemptCount != 0 || leaseOwner.Valid || leaseToken.Valid || leaseExpiresAt.Valid {
		t.Fatalf("requeued activation authority = attempt %d owner %v token %v expiry %v; want fresh generation", attemptCount, leaseOwner, leaseToken, leaseExpiresAt)
	}
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: "ws_execution_store", Kinds: []string{queue.KindSandboxActivate},
		LeaseOwner: "sandbox-successor", MaxJobs: 1, LeaseDuration: time.Minute,
	})
	if err != nil || len(leased) != 1 || leased[0].ID != currentJobID || leased[0].LeasedUntil == nil {
		t.Fatalf("lease fresh activation = %#v, %v; want %s", leased, err, currentJobID)
	}
	claimJob := lifecycleJobByOperationID(t, admin, "ws_execution_store", operationID)
	claimJob.LeaseOwner = leased[0].LeasedBy
	claimJob.LeaseToken = leased[0].LeaseToken
	claimJob.LeaseExpiresAt = *leased[0].LeasedUntil
	claimJob.AttemptCount = leased[0].AttemptCount
	claimCtx := withSandboxQueueAuthority(context.Background(), &sandboxQueueAuthority{
		workspaceID: "ws_execution_store", jobID: leased[0].ID,
		leaseToken: leased[0].LeaseToken, leasedUntil: *leased[0].LeasedUntil,
	})
	lifecycle := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtime), &fixedSandboxResourceSource{}, 30*time.Minute)
	if _, disposition, err := lifecycle.ClaimActivation(claimCtx, claimJob, fixedEnvironmentStoreTime.Add(2*time.Second)); err != nil || disposition != SandboxLifecycleApplied {
		t.Fatalf("ClaimActivation(fresh generation) = %s, %v; want applied", disposition, err)
	}
}

func TestEnvironmentReadyFanoutFinalizerRejectsSupersededQueueLease(t *testing.T) {
	runtime, admin := newEnvironmentArtifactStoreTestDB(t)
	seedSandboxExecutionStoreFixture(t, admin)
	setupCtx := sandboxTestQueueContext(t, runtime)
	if _, err := admin.Exec(`UPDATE environment_artifacts
		SET status='pending', provider_artifact_ref=NULL
		WHERE workspace_id='ws_execution_store' AND environment_id='env_execution_store' AND generation=1`); err != nil {
		t.Fatalf("mark artifact pending: %v", err)
	}
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtime), 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(setupCtx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	if _, err := admin.Exec(`UPDATE environment_artifacts
		SET status='ready', provider_artifact_ref='artifact_ready'
		WHERE workspace_id='ws_execution_store' AND environment_id='env_execution_store' AND generation=1`); err != nil {
		t.Fatalf("mark artifact ready: %v", err)
	}
	firstCtx, secondCtx, _, _ := supersedeSandboxQueueLease(t, runtime, admin, queue.EnqueueRequest{
		ID: queue.NewJobID(), WorkspaceID: "ws_execution_store", Kind: queue.KindEnvironmentReadyFanout,
		PartitionKey:   queue.FormatEnvironmentPartitionKey("ws_execution_store", "env_execution_store"),
		DedupeKey:      queue.FormatEnvironmentReadyFanoutDedupeKey("ws_execution_store", "env_execution_store", "1"),
		PayloadVersion: 1,
		PayloadJSON:    []byte(`{"workspace_id":"ws_execution_store","environment_id":"env_execution_store","generation":1}`),
		MaxAttempts:    3,
	})
	store := NewEnvironmentArtifactStore(dbconnect.NewClientForTesting(runtime))
	job := EnvironmentReadyFanoutJob{WorkspaceID: "ws_execution_store", EnvironmentID: "env_execution_store", Generation: 1}
	if err := store.FinalizeReadyEnvironmentFanout(firstCtx, job, fixedEnvironmentStoreTime); !errors.Is(err, errQueueLeaseLost) {
		t.Fatalf("superseded FinalizeReadyEnvironmentFanout error = %v; want Queue authority loss", err)
	}
	var state string
	if err := admin.QueryRow(`SELECT state FROM sandbox_lifecycle_operations
		WHERE workspace_id='ws_execution_store' AND kind='create'`).Scan(&state); err != nil {
		t.Fatalf("read activation after stale finalizer: %v", err)
	}
	if state != "waiting_artifact" {
		t.Fatalf("activation after stale finalizer = %q; want waiting_artifact", state)
	}
	if err := store.FinalizeReadyEnvironmentFanout(secondCtx, job, fixedEnvironmentStoreTime.Add(time.Second)); err != nil {
		t.Fatalf("successor FinalizeReadyEnvironmentFanout: %v", err)
	}
	if err := admin.QueryRow(`SELECT state FROM sandbox_lifecycle_operations
		WHERE workspace_id='ws_execution_store' AND kind='create'`).Scan(&state); err != nil {
		t.Fatalf("read activation after successor finalizer: %v", err)
	}
	if state != "failed" {
		t.Fatalf("activation after successor finalizer = %q; want failed", state)
	}
}

func TestEnvironmentArtifactFailureSettlesWaitingSandboxActivation(t *testing.T) {
	runtime, admin := newEnvironmentArtifactStoreTestDB(t)
	seedSandboxExecutionStoreFixture(t, admin)
	ctx := sandboxTestQueueContext(t, runtime)
	if _, err := admin.Exec(`UPDATE environment_artifacts
		SET status = 'pending', provider_artifact_ref = NULL
		WHERE workspace_id = 'ws_execution_store' AND environment_id = 'env_execution_store' AND generation = 1`); err != nil {
		t.Fatalf("mark artifact building: %v", err)
	}
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtime), 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	store := NewEnvironmentArtifactStore(dbconnect.NewClientForTesting(runtime))
	job := leaseEnvironmentBuildJob(t, runtime, "ws_execution_store", "env_execution_store", 1, fixedEnvironmentStoreTime, 10*time.Minute)
	if _, claimed, err := store.ClaimEnvironmentBuild(ctx, job, fixedEnvironmentStoreTime); err != nil || !claimed {
		t.Fatalf("ClaimEnvironmentBuild = claimed %t err %v; want true/nil", claimed, err)
	}
	if err := store.MarkEnvironmentBuildTerminalFailure(ctx, job, EnvironmentArtifactFailure{
		Stage: "build_artifact", LastErrorKind: "environment_artifact_failed", Reason: "artifact build failed",
	}, fixedEnvironmentStoreTime); err != nil {
		t.Fatalf("MarkEnvironmentBuildTerminalFailure: %v", err)
	}
	assertSandboxExecutionState(t, admin, "evt_execution_a", "terminal_unconsumed", 1)
}

func TestEnvironmentReadyFanoutExhaustionSettlesWaitingSandboxActivation(t *testing.T) {
	runtime, admin := newEnvironmentArtifactStoreTestDB(t)
	seedSandboxExecutionStoreFixture(t, admin)
	ctx := sandboxTestQueueContext(t, runtime)
	if _, err := admin.Exec(`UPDATE environment_artifacts
		SET status='pending', provider_artifact_ref=NULL
		WHERE workspace_id='ws_execution_store' AND environment_id='env_execution_store' AND generation=1`); err != nil {
		t.Fatalf("mark artifact building: %v", err)
	}
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtime), 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	if _, err := admin.Exec(`UPDATE environment_artifacts
		SET status='ready', provider_artifact_ref='artifact_ready'
		WHERE workspace_id='ws_execution_store' AND environment_id='env_execution_store' AND generation=1`); err != nil {
		t.Fatalf("mark artifact ready: %v", err)
	}
	store := NewEnvironmentArtifactStore(dbconnect.NewClientForTesting(runtime))
	if err := store.FinalizeReadyEnvironmentFanout(ctx, EnvironmentReadyFanoutJob{
		WorkspaceID: "ws_execution_store", EnvironmentID: "env_execution_store", Generation: 1,
	}, fixedEnvironmentStoreTime); err != nil {
		t.Fatalf("FinalizeReadyEnvironmentFanout: %v", err)
	}
	assertSandboxExecutionState(t, admin, "evt_execution_a", "terminal_unconsumed", 1)
	var state, errorKind string
	if err := admin.QueryRow(`SELECT state, error_kind FROM sandbox_lifecycle_operations
		WHERE workspace_id='ws_execution_store' AND kind='create'`).Scan(&state, &errorKind); err != nil {
		t.Fatalf("read finalized activation: %v", err)
	}
	if state != "failed" || errorKind != "environment_ready_fanout_failed" {
		t.Fatalf("finalized activation = %q/%q; want failed/environment_ready_fanout_failed", state, errorKind)
	}
}

func TestEnvironmentReadyFanoutInvalidPayloadSettlesWaitingSandboxActivation(t *testing.T) {
	runtime, admin := newEnvironmentArtifactStoreTestDB(t)
	seedSandboxExecutionStoreFixture(t, admin)
	setupCtx := sandboxTestQueueContext(t, runtime)
	if _, err := admin.Exec(`UPDATE environment_artifacts
		SET status='pending', provider_artifact_ref=NULL
		WHERE workspace_id='ws_execution_store' AND environment_id='env_execution_store' AND generation=1`); err != nil {
		t.Fatalf("mark artifact building: %v", err)
	}
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtime), 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(setupCtx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	if _, err := admin.Exec(`UPDATE environment_artifacts
		SET status='ready', provider_artifact_ref='artifact_ready'
		WHERE workspace_id='ws_execution_store' AND environment_id='env_execution_store' AND generation=1`); err != nil {
		t.Fatalf("mark artifact ready: %v", err)
	}
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	jobID := queue.NewJobID()
	if _, err := queueStore.Enqueue(context.Background(), queue.EnqueueRequest{
		ID: jobID, WorkspaceID: "ws_execution_store", Kind: queue.KindEnvironmentReadyFanout,
		PartitionKey:   queue.FormatEnvironmentPartitionKey("ws_execution_store", "env_execution_store"),
		DedupeKey:      queue.FormatEnvironmentReadyFanoutDedupeKey("ws_execution_store", "env_execution_store", "1"),
		PayloadVersion: 1,
		PayloadJSON:    []byte(`{"workspace_id":"ws_execution_store","environment_id":"env_execution_store","generation":"1"}`),
		MaxAttempts:    3,
	}); err != nil {
		t.Fatalf("enqueue environment fanout: %v", err)
	}
	if _, err := admin.Exec(`UPDATE queue_jobs SET payload_json='{}' WHERE workspace_id=$1 AND id=$2`,
		"ws_execution_store", jobID); err != nil {
		t.Fatalf("poison environment fanout payload: %v", err)
	}
	runner := &EnvironmentReadyFanoutJobRunner{
		Queue: tetralqueue.NewServer(queueStore, nil),
		Store: NewEnvironmentArtifactStore(dbconnect.NewClientForTesting(runtime)),
		Config: EnvironmentRunnerConfig{
			WorkspaceID: "ws_execution_store", LeaseOwner: "environment-fanout-test", MaxJobs: 1,
			LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second,
		},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	assertSandboxExecutionState(t, admin, "evt_execution_a", "terminal_unconsumed", 1)
	var lifecycleState, lifecycleError string
	if err := admin.QueryRow(`SELECT state, error_kind FROM sandbox_lifecycle_operations
		WHERE workspace_id='ws_execution_store' AND kind='create'`).Scan(&lifecycleState, &lifecycleError); err != nil {
		t.Fatalf("read finalized activation: %v", err)
	}
	if lifecycleState != "failed" || lifecycleError != "environment_ready_fanout_failed" {
		t.Fatalf("finalized activation = %q/%q; want failed/environment_ready_fanout_failed", lifecycleState, lifecycleError)
	}
	var queueStatus, queueError string
	if err := admin.QueryRow(`SELECT status, last_error_kind FROM queue_jobs WHERE workspace_id=$1 AND id=$2`,
		"ws_execution_store", jobID).Scan(&queueStatus, &queueError); err != nil {
		t.Fatalf("read poisoned fanout Queue job: %v", err)
	}
	if queueStatus != "dead_lettered" || queueError != "invalid_environment_ready_fanout_payload" {
		t.Fatalf("poisoned fanout Queue job = %q/%q; want dead_lettered/invalid_environment_ready_fanout_payload", queueStatus, queueError)
	}
}

func TestEnvironmentArtifactStoreBuildReadyAdvancesSameInputFollowers(t *testing.T) {
	runtime, admin := newEnvironmentArtifactStoreTestDB(t)
	ctx := context.Background()
	seedEnvironmentArtifactStoreEnvironment(t, admin, "ws_env_follow", "env_build")
	seedEnvironmentArtifact(t, admin, "ws_env_follow", "env_build", 7, "pending", "", `{"pip":["pandas==2.2.0"]}`)
	seedEnvironmentArtifact(t, admin, "ws_env_follow", "env_build", 8, "pending", "", `{"pip":["pandas==2.2.0"]}`)
	store := NewEnvironmentArtifactStore(dbconnect.NewClientForTesting(runtime))
	job := leaseEnvironmentBuildJob(t, runtime, "ws_env_follow", "env_build", 7, fixedEnvironmentStoreTime, 10*time.Minute)
	if _, claimed, err := store.ClaimEnvironmentBuild(ctx, job, fixedEnvironmentStoreTime); err != nil || !claimed {
		t.Fatalf("ClaimEnvironmentBuild = claimed %t err %v; want true/nil", claimed, err)
	}

	if err := store.MarkEnvironmentBuildReady(ctx, job, "snapshot_ready", fixedEnvironmentStoreTime); err != nil {
		t.Fatalf("MarkEnvironmentBuildReady: %v", err)
	}
	assertEnvironmentArtifactStatus(t, admin, "ws_env_follow", "env_build", 7, "ready", "snapshot_ready")
	assertEnvironmentArtifactStatus(t, admin, "ws_env_follow", "env_build", 8, "ready", "snapshot_ready")
	assertQueueJobCount(t, admin, "ws_env_follow", "environment_ready_fanout", 2)
}

func TestEnvironmentArtifactStoreRejectsExpiredWriterAfterLeaseTransfer(t *testing.T) {
	runtime, admin := newEnvironmentArtifactStoreTestDB(t)
	ctx := context.Background()
	seedEnvironmentArtifactStoreEnvironment(t, admin, "ws_env_lease", "env_build")
	seedEnvironmentArtifact(t, admin, "ws_env_lease", "env_build", 7, "pending", "", `{"apt":["git"]}`)
	store := NewEnvironmentArtifactStore(dbconnect.NewClientForTesting(runtime))
	first := leaseEnvironmentBuildJob(t, runtime, "ws_env_lease", "env_build", 7, fixedEnvironmentStoreTime, time.Second)
	if _, claimed, err := store.ClaimEnvironmentBuild(ctx, first, fixedEnvironmentStoreTime); err != nil || !claimed {
		t.Fatalf("ClaimEnvironmentBuild(first) = claimed %t err %v; want true/nil", claimed, err)
	}
	assertEnvironmentArtifactLease(t, admin, "ws_env_lease", "env_build", 7, first.JobID, first.LeaseToken, first.AttemptCount)
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	if _, err := admin.Exec(`UPDATE queue_jobs SET leased_until=clock_timestamp()-interval '1 second' WHERE workspace_id=$1 AND id=$2`, first.WorkspaceID, first.JobID); err != nil {
		t.Fatalf("expire first Queue lease: %v", err)
	}
	if reclaimed, err := queueStore.ReclaimExpiredLeases(ctx, queue.ReclaimExpiredLeasesRequest{
		WorkspaceID: firstWorkspace(first), Limit: 1,
	}); err != nil || reclaimed != 1 {
		t.Fatalf("ReclaimExpiredLeases = %d, %v; want 1/nil", reclaimed, err)
	}
	leased, err := queueStore.Lease(ctx, queue.LeaseRequest{
		WorkspaceID: firstWorkspace(first), Kinds: []string{queue.KindEnvironmentBuild}, LeaseOwner: "environment-worker-b",
		MaxJobs: 1, LeaseDuration: 10 * time.Minute, Now: time.Now().UTC().Add(time.Second),
	})
	if err != nil || len(leased) != 1 {
		t.Fatalf("Lease(second) = %#v, %v; want one job", leased, err)
	}
	second := EnvironmentBuildJob{
		JobID: leased[0].ID, LeaseToken: leased[0].LeaseToken, AttemptCount: leased[0].AttemptCount,
		WorkspaceID:   string(leased[0].WorkspaceID),
		EnvironmentID: "env_build", Generation: 7,
	}
	if _, claimed, err := store.ClaimEnvironmentBuild(ctx, second, fixedEnvironmentStoreTime.Add(2*time.Second)); err != nil || !claimed {
		t.Fatalf("ClaimEnvironmentBuild(second) = claimed %t err %v; want true/nil", claimed, err)
	}
	assertEnvironmentArtifactLease(t, admin, "ws_env_lease", "env_build", 7, second.JobID, second.LeaseToken, second.AttemptCount)
	if _, claimed, err := store.ClaimEnvironmentBuild(ctx, first, fixedEnvironmentStoreTime.Add(3*time.Second)); err != nil || claimed {
		t.Fatalf("ClaimEnvironmentBuild(stale) = claimed %t err %v; want false/nil", claimed, err)
	}
	if authorized, err := store.AuthorizeEnvironmentArtifactCreate(ctx, first, fixedEnvironmentStoreTime.Add(3*time.Second)); err != nil || authorized {
		t.Fatalf("AuthorizeEnvironmentArtifactCreate(stale) = %t, %v; want false/nil", authorized, err)
	}
	if err := store.MarkEnvironmentBuildReady(ctx, first, "snapshot_stale", fixedEnvironmentStoreTime.Add(3*time.Second)); err != nil {
		t.Fatalf("MarkEnvironmentBuildReady(stale): %v", err)
	}
	assertEnvironmentArtifactStatus(t, admin, "ws_env_lease", "env_build", 7, "building", "")
	if err := store.MarkEnvironmentBuildRetryableFailure(ctx, first, EnvironmentArtifactFailure{
		Stage: "build_artifact", LastErrorKind: "stale_failure", Retryable: true,
	}, true, fixedEnvironmentStoreTime.Add(3*time.Second)); err != nil {
		t.Fatalf("MarkEnvironmentBuildRetryableFailure(stale): %v", err)
	}
	assertEnvironmentArtifactStatus(t, admin, "ws_env_lease", "env_build", 7, "building", "")
	if err := store.MarkEnvironmentBuildTerminalFailure(ctx, first, EnvironmentArtifactFailure{
		Stage: "build_artifact", LastErrorKind: "stale_terminal", Retryable: false,
	}, fixedEnvironmentStoreTime.Add(3*time.Second)); err != nil {
		t.Fatalf("MarkEnvironmentBuildTerminalFailure(stale): %v", err)
	}
	assertEnvironmentArtifactStatus(t, admin, "ws_env_lease", "env_build", 7, "building", "")
	if err := store.MarkEnvironmentBuildReady(ctx, second, "snapshot_current", fixedEnvironmentStoreTime.Add(4*time.Second)); err != nil {
		t.Fatalf("MarkEnvironmentBuildReady(current): %v", err)
	}
	assertEnvironmentArtifactStatus(t, admin, "ws_env_lease", "env_build", 7, "ready", "snapshot_current")
}

func firstWorkspace(job EnvironmentBuildJob) workspace.ID {
	return workspace.ID(job.WorkspaceID)
}

func leaseEnvironmentBuildJob(t *testing.T, runtime *sql.DB, workspaceID string, environmentID string, generation int64, now time.Time, duration time.Duration) EnvironmentBuildJob {
	t.Helper()
	payload, err := json.Marshal(map[string]string{
		"workspace_id": workspaceID, "environment_id": environmentID, "generation": strconv.FormatInt(generation, 10),
	})
	if err != nil {
		t.Fatalf("encode environment build payload: %v", err)
	}
	ws := workspace.ID(workspaceID)
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	jobID := queue.NewJobID()
	if _, err := queueStore.Enqueue(context.Background(), queue.EnqueueRequest{
		ID: jobID, WorkspaceID: ws, Kind: queue.KindEnvironmentBuild,
		PartitionKey:   queue.FormatEnvironmentPartitionKey(ws, environmentID),
		DedupeKey:      queue.FormatEnvironmentBuildDedupeKey(ws, environmentID, strconv.FormatInt(generation, 10)),
		PayloadVersion: 1, PayloadJSON: payload, MaxAttempts: queue.DefaultMaxAttempts, Now: now,
	}); err != nil {
		t.Fatalf("enqueue environment build: %v", err)
	}
	leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: ws, Kinds: []string{queue.KindEnvironmentBuild}, LeaseOwner: "environment-worker-a",
		MaxJobs: 1, LeaseDuration: duration, Now: now,
	})
	if err != nil || len(leased) != 1 || leased[0].ID != jobID {
		t.Fatalf("lease environment build = %#v, %v; want %s", leased, err, jobID)
	}
	return EnvironmentBuildJob{
		JobID: leased[0].ID, LeaseToken: leased[0].LeaseToken, AttemptCount: leased[0].AttemptCount, WorkspaceID: workspaceID,
		EnvironmentID: environmentID, Generation: generation,
	}
}

func newEnvironmentArtifactStoreTestDB(t *testing.T) (runtime *sql.DB, admin *sql.DB) {
	t.Helper()
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Skip(storagetest.EnvTestDatabaseURL + " is not set")
	}
	return storagetest.NewPostgreSQLDBWithAdmin(t)
}

func seedEnvironmentArtifactStoreEnvironment(t *testing.T, db *sql.DB, workspaceID string, environmentID string) {
	t.Helper()
	now := fixedEnvironmentStoreTime.Format(time.RFC3339)
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO agents (workspace_id, id, name, description, version, created_at, updated_at)
		 VALUES ($1, 'agent_env_store', 'Environment Store Agent', '', 1, $2, $2)
		 ON CONFLICT (workspace_id, id) DO NOTHING`,
		workspaceID, now,
	); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at)
		 VALUES ($1, 'agentver_env_store', 'agent_env_store', 1, '{}', 'hash', $2)
		 ON CONFLICT (workspace_id, agent_id, version) DO NOTHING`,
		workspaceID, now,
	); err != nil {
		t.Fatalf("seed agent version: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO environments (workspace_id, id, name, description, config_json, current_generation, metadata_json, created_at, updated_at)
		 VALUES ($1, $2, $2, '', '{"type":"cloud","networking":{"type":"unrestricted"},"packages":{}}', 7, '{}', $3, $3)
		 ON CONFLICT (workspace_id, id) DO NOTHING`,
		workspaceID, environmentID, now,
	); err != nil {
		t.Fatalf("seed environment: %v", err)
	}
}

func seedEnvironmentArtifact(t *testing.T, db *sql.DB, workspaceID string, environmentID string, generation int64, status string, providerRef string, packagesJSON string) {
	t.Helper()
	now := fixedEnvironmentStoreTime.Format(time.RFC3339)
	_, err := db.ExecContext(context.Background(),
		`INSERT INTO environment_artifacts (
			workspace_id, environment_id, generation, status, provider,
			provider_artifact_ref, normalized_config_hash, artifact_input_hash,
			runtime_network_policy_json, packages_json, created_at, updated_at
		) VALUES ($1, $2, $3, $4, 'daytona', NULLIF($5, ''), 'hash_config', 'hash_packages',
			'{"type":"unrestricted"}', $6, $7::timestamptz, $7::timestamptz)`,
		workspaceID, environmentID, generation, status, providerRef, packagesJSON, now,
	)
	if err != nil {
		t.Fatalf("seed environment artifact: %v", err)
	}
}

func seedEnvironmentArtifactStoreSession(t *testing.T, db *sql.DB, workspaceID string, sessionID string, environmentID string) {
	t.Helper()
	now := fixedEnvironmentStoreTime.Format(time.RFC3339)
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO sessions (
			workspace_id, id, type, title, metadata_json, status, agent_id, agent_version,
			environment_id, vault_ids_json, created_at, updated_at
		) VALUES ($1, $2, 'session', NULL, '{}', 'idle', 'agent_env_store', 1, $3, '[]', $4, $4)`,
		workspaceID, sessionID, environmentID, now,
	); err != nil {
		t.Fatalf("seed session: %v", err)
	}
}

func assertEnvironmentArtifactStatus(t *testing.T, db *sql.DB, workspaceID string, environmentID string, generation int64, wantStatus string, wantProviderRef string) {
	t.Helper()
	var status string
	var providerRef sql.NullString
	if err := db.QueryRowContext(context.Background(),
		`SELECT status, provider_artifact_ref
		   FROM environment_artifacts
		  WHERE workspace_id = $1 AND environment_id = $2 AND generation = $3`,
		workspaceID, environmentID, generation,
	).Scan(&status, &providerRef); err != nil {
		t.Fatalf("read environment artifact: %v", err)
	}
	if status != wantStatus || providerRef.String != wantProviderRef {
		t.Fatalf("artifact status/ref = %q/%q; want %q/%q", status, providerRef.String, wantStatus, wantProviderRef)
	}
}

func assertEnvironmentArtifactLease(t *testing.T, db *sql.DB, workspaceID string, environmentID string, generation int64, wantJobID string, wantLeaseToken string, wantAttemptCount int) {
	t.Helper()
	var jobID, leaseToken sql.NullString
	var attemptCount sql.NullInt64
	if err := db.QueryRowContext(context.Background(),
		`SELECT lease_job_id, lease_token, lease_attempt_count
		   FROM environment_artifacts
		  WHERE workspace_id = $1 AND environment_id = $2 AND generation = $3`,
		workspaceID, environmentID, generation,
	).Scan(&jobID, &leaseToken, &attemptCount); err != nil {
		t.Fatalf("read environment artifact lease: %v", err)
	}
	if jobID.String != wantJobID || leaseToken.String != wantLeaseToken || attemptCount.Int64 != int64(wantAttemptCount) {
		t.Fatalf("artifact lease = %q/%q/%d; want %q/%q/%d", jobID.String, leaseToken.String, attemptCount.Int64, wantJobID, wantLeaseToken, wantAttemptCount)
	}
}

func assertQueueJobCount(t *testing.T, db *sql.DB, workspaceID string, kind string, want int) {
	t.Helper()
	var got int
	if err := db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM queue_jobs WHERE workspace_id = $1 AND kind = $2`,
		workspaceID, kind,
	).Scan(&got); err != nil {
		t.Fatalf("count queue jobs: %v", err)
	}
	if got != want {
		t.Fatalf("queue job count kind %s = %d; want %d", kind, got, want)
	}
}

var fixedEnvironmentStoreTime = time.Date(2026, 5, 22, 10, 0, 0, 0, time.UTC)
