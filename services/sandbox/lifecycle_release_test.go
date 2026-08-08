package tetralsandbox

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/workspace"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

func TestEnsureSandboxReleaseFencesExecutionAndSchedulesBackgroundCancellation(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	now := time.Date(2026, 7, 31, 18, 0, 0, 0, time.UTC)
	seedReadySandboxBinding(t, adminDB, now)
	retainedActivationJobID := "qjob_retained_release_dependency"
	retainedActivationPartition := queue.FormatSandboxLifecyclePartitionKey("ws_execution_store", "sbox_execution_store")
	retainedActivationDedupe := queue.FormatSandboxLifecycleDedupeKey(
		queue.KindSandboxActivate, "ws_execution_store", "sbox_execution_store", "sop_retained_release_dependency",
	)
	if _, err := adminDB.Exec(`INSERT INTO sandbox_lifecycle_operations (
		workspace_id, operation_id, session_id, logical_sandbox_id, kind, state,
		observed_binding_revision, target_provider_resource_id,
		queue_job_id, queue_kind, queue_partition_key, queue_dedupe_key,
		created_at, updated_at
	) VALUES (
		'ws_execution_store', 'sop_retained_release_dependency', 'sesn_execution_store',
		'sbox_execution_store', 'start', 'pending', 1, 'provider_execution_store',
		$1, $2, $3, $4, $5, $5
	)`, retainedActivationJobID, queue.KindSandboxActivate, retainedActivationPartition, retainedActivationDedupe, now); err != nil {
		t.Fatalf("seed retained release dependency: %v", err)
	}
	if _, err := adminDB.Exec(`INSERT INTO session_background_tasks (
		workspace_id, session_id, session_thread_id, task_id, source_tool_use_event_id,
		sandbox_id, provider, binding_revision, provider_session_id, provider_command_id,
		provider_command_metadata_json, resource_roots_json, status, reconcile_generation,
		next_poll_at, created_at, updated_at
	) VALUES (
		'ws_execution_store','sesn_execution_store','thr_execution_store','task_release',
		'evt_execution_a','sbox_execution_store','daytona',1,'provider_execution_store',
		'provider_command','{}','[]','running',1,$1,$1,$1
	)`, now); err != nil {
		t.Fatalf("seed background task: %v", err)
	}

	var operationID string
	client := dbconnect.NewClientForTesting(runtimeDB)
	if err := client.WithWorkspaceTx(context.Background(), "ws_execution_store", "sandbox.release.test", func(tx *dbconnect.Tx) error {
		var err error
		operationID, _, err = EnsureSandboxReleaseTx(
			context.Background(), tx, "ws_execution_store", "sesn_execution_store",
			SandboxReleaseSessionDelete, "provider_execution_store", now,
		)
		return err
	}); err != nil {
		t.Fatalf("EnsureSandboxReleaseTx: %v", err)
	}

	var releaseRequested sql.NullTime
	var releaseReason string
	if err := adminDB.QueryRow(`SELECT release_requested_at, release_reason
		FROM session_sandbox_bindings
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store'`).Scan(
		&releaseRequested, &releaseReason,
	); err != nil {
		t.Fatalf("read release fence: %v", err)
	}
	if !releaseRequested.Valid || releaseReason != string(SandboxReleaseSessionDelete) {
		t.Fatalf("release fence = %v/%q", releaseRequested, releaseReason)
	}
	var settledExecutions int
	if err := adminDB.QueryRow(`SELECT count(*) FROM session_runtime_tool_results
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store'
		  AND tool_kind='sandbox_tool' AND execution_state='terminal_unconsumed'
		  AND result_json::jsonb -> 'error' ->> 'kind' = 'session_deleted'`).Scan(&settledExecutions); err != nil {
		t.Fatalf("read settled executions: %v", err)
	}
	if settledExecutions != 2 {
		t.Fatalf("settled executions = %d; want 2", settledExecutions)
	}
	var taskReleaseID string
	if err := adminDB.QueryRow(`SELECT release_operation_id FROM session_background_tasks
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store' AND task_id='task_release'`).Scan(&taskReleaseID); err != nil {
		t.Fatalf("read task release fence: %v", err)
	}
	if taskReleaseID != operationID {
		t.Fatalf("task release operation = %q; want %q", taskReleaseID, operationID)
	}
	var retainedDependencyState string
	if err := adminDB.QueryRow(`SELECT state FROM sandbox_lifecycle_operations
		WHERE workspace_id='ws_execution_store' AND operation_id='sop_retained_release_dependency'`).Scan(&retainedDependencyState); err != nil {
		t.Fatalf("read retained release dependency: %v", err)
	}
	if retainedDependencyState != "abandoned" {
		t.Fatalf("retained release dependency state = %q; want abandoned", retainedDependencyState)
	}
	var releaseJobs, cancelJobs, cancelReceipts int
	if err := adminDB.QueryRow(`SELECT count(*) FROM queue_jobs
		WHERE workspace_id='ws_execution_store' AND kind='sandbox_release' AND status='pending'`).Scan(&releaseJobs); err != nil {
		t.Fatalf("read release job: %v", err)
	}
	if err := adminDB.QueryRow(`SELECT count(*) FROM queue_jobs
		WHERE workspace_id='ws_execution_store' AND kind='sandbox_background_command' AND status='pending'`).Scan(&cancelJobs); err != nil {
		t.Fatalf("read cancel job: %v", err)
	}
	if err := adminDB.QueryRow(`SELECT count(*) FROM session_runtime_tool_results
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store'
		  AND tool_kind='sandbox_background' AND background_operation_kind='cancel'
		  AND background_operation_state='pending'`).Scan(&cancelReceipts); err != nil {
		t.Fatalf("read cancel receipt: %v", err)
	}
	if releaseJobs != 1 || cancelJobs != 1 || cancelReceipts != 1 {
		t.Fatalf("release/cancel jobs/receipts = %d/%d/%d; want 1/1/1", releaseJobs, cancelJobs, cancelReceipts)
	}
}

func TestReleaseBackgroundCancellationExhaustionCreatesANewReceipt(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	seedBackgroundTaskFromExecution(t, runtimeDB, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	now := time.Date(2026, 7, 31, 18, 15, 0, 0, time.UTC)
	client := dbconnect.NewClientForTesting(runtimeDB)
	var operationID string
	if err := client.WithWorkspaceTx(context.Background(), "ws_execution_store", "sandbox.release.cancel_successor_test", func(tx *dbconnect.Tx) error {
		var err error
		operationID, _, err = EnsureSandboxReleaseTx(
			context.Background(), tx, "ws_execution_store", "sesn_execution_store",
			SandboxReleaseSessionDelete, "provider_execution_store", now,
		)
		return err
	}); err != nil {
		t.Fatalf("EnsureSandboxReleaseTx: %v", err)
	}
	initialRequestID := releaseBackgroundCancelRequestID(operationID, "task_execution", 1)
	var initialJob queuev1.QueueJob
	if err := adminDB.QueryRow(`SELECT id, workspace_id, kind, partition_key, dedupe_key, payload_json
		FROM queue_jobs WHERE workspace_id='ws_execution_store' AND kind='sandbox_background_command'
		  AND dedupe_key=$1`,
		queue.FormatSandboxBackgroundCommandDedupeKey("ws_execution_store", "sesn_execution_store", "task_execution", initialRequestID),
	).Scan(&initialJob.Id, &initialJob.WorkspaceId, &initialJob.Kind, &initialJob.PartitionKey, &initialJob.DedupeKey, &initialJob.PayloadJson); err != nil {
		t.Fatalf("read initial cancellation job: %v", err)
	}
	if err := NewPostgreSQLSandboxBackgroundCommandStore(client).FinalizeCommandExhaustion(
		ctx, &initialJob, now.Add(time.Minute),
	); err != nil {
		t.Fatalf("FinalizeCommandExhaustion: %v", err)
	}

	nextRequestID := releaseBackgroundCancelRequestID(operationID, "task_execution", 2)
	var oldState, nextState, nextKind string
	if err := adminDB.QueryRow(`SELECT background_operation_state FROM session_runtime_tool_results
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store'
		  AND background_request_id=$1`, initialRequestID).Scan(&oldState); err != nil {
		t.Fatalf("read exhausted cancellation receipt: %v", err)
	}
	if err := adminDB.QueryRow(`SELECT background_operation_state, background_operation_kind
		FROM session_runtime_tool_results
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store'
		  AND background_request_id=$1`, nextRequestID).Scan(&nextState, &nextKind); err != nil {
		t.Fatalf("read successor cancellation receipt: %v", err)
	}
	if oldState != "terminal" || nextState != "pending" || nextKind != "cancel" {
		t.Fatalf("cancel receipts = old %q next %q/%q; want terminal and pending/cancel", oldState, nextState, nextKind)
	}
	var nextJobs int
	var nextAvailableAt time.Time
	if err := adminDB.QueryRow(`SELECT count(*) OVER (), available_at FROM queue_jobs
		WHERE workspace_id='ws_execution_store' AND kind='sandbox_background_command'
		  AND dedupe_key=$1 AND status='pending'`,
		queue.FormatSandboxBackgroundCommandDedupeKey("ws_execution_store", "sesn_execution_store", "task_execution", nextRequestID),
	).Scan(&nextJobs, &nextAvailableAt); err != nil {
		t.Fatalf("read successor cancellation job: %v", err)
	}
	if nextJobs != 1 {
		t.Fatalf("successor cancellation jobs = %d; want 1", nextJobs)
	}
	wantAvailableAt := now.Add(time.Minute).Add(releaseBackgroundCancelSuccessorBackoffCap)
	if !nextAvailableAt.Equal(wantAvailableAt) {
		t.Fatalf("successor cancellation available_at = %v; want %v", nextAvailableAt, wantAvailableAt)
	}
}

func TestForegroundSettlementWakesParkedSandboxRelease(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	settlementCtx := sandboxTestQueueContext(t, runtimeDB)
	now := time.Now().UTC()
	seedReadySandboxBinding(t, adminDB, now)
	if _, err := adminDB.Exec(`UPDATE session_runtime_tool_results
		SET execution_state='running', authorized_binding_revision=1,
		    authorized_provider_resource_id='provider_execution_store', preparation_deadline=NULL
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store'
		  AND tool_use_event_id='evt_execution_a'`); err != nil {
		t.Fatalf("mark foreground execution running: %v", err)
	}

	client := dbconnect.NewClientForTesting(runtimeDB)
	var releaseOperationID string
	if err := client.WithWorkspaceTx(context.Background(), "ws_execution_store", "sandbox.release.park_test", func(tx *dbconnect.Tx) error {
		var err error
		releaseOperationID, _, err = EnsureSandboxReleaseTx(
			context.Background(), tx, "ws_execution_store", "sesn_execution_store",
			SandboxReleaseSessionDelete, "provider_execution_store", now,
		)
		return err
	}); err != nil {
		t.Fatalf("EnsureSandboxReleaseTx: %v", err)
	}
	queueStore := queue.NewPostgreSQLStore(client)
	first := leaseReleaseJob(t, queueStore, time.Now().UTC(), time.Second, "sandbox-park-first")
	firstJobID := first.JobID
	store := NewPostgreSQLSandboxLifecycleStore(client, nil, 0)
	if _, err := adminDB.Exec(`UPDATE queue_jobs SET leased_until=clock_timestamp() - interval '1 second'
		WHERE workspace_id='ws_execution_store' AND id=$1`, firstJobID); err != nil {
		t.Fatalf("expire first release notification: %v", err)
	}
	if reclaimed, err := queueStore.ReclaimExpiredLeases(context.Background(), queue.ReclaimExpiredLeasesRequest{
		WorkspaceID: workspace.ID(first.WorkspaceID), Limit: 1,
	}); err != nil || reclaimed != 1 {
		t.Fatalf("reclaim expired release notification = %d, %v; want 1,nil", reclaimed, err)
	}
	second := leaseReleaseJob(t, queueStore, time.Now().UTC(), time.Minute, "sandbox-park-second")
	firstCtx := withLifecycleJobQueueAuthority(context.Background(), first)
	disposition, err := store.ParkBlockedRelease(firstCtx, first, now.Add(time.Second))
	if !errors.Is(err, errQueueLeaseLost) || disposition != SandboxLifecycleLostAuthority {
		t.Fatalf("superseded ParkBlockedRelease = %s, %v; want direct Queue authority loss", disposition, err)
	}
	var state string
	var retainedJobID string
	var retainedQueueStatus string
	if err := adminDB.QueryRow(`SELECT o.state, o.queue_job_id, q.status
		FROM sandbox_lifecycle_operations o JOIN queue_jobs q
		  ON q.workspace_id=o.workspace_id AND q.id=o.queue_job_id
		WHERE o.workspace_id='ws_execution_store' AND o.operation_id=$1`, releaseOperationID).Scan(&state, &retainedJobID, &retainedQueueStatus); err != nil {
		t.Fatalf("read release after expired park: %v", err)
	}
	if state != "pending" || retainedJobID != firstJobID || retainedQueueStatus != queue.StatusLeased {
		t.Fatalf("release after expired park = %s/%s/%s; want pending/%s/leased", state, retainedJobID, retainedQueueStatus, firstJobID)
	}
	queueClient := sandboxProductionQueueClient(t, queueStore)
	guardCtx, finishGuard, err := startQueueLeaseGuard(context.Background(), queueClient, second.QueueJob, second.LeaseExpiresAt, 10*time.Second, time.Minute)
	if err != nil {
		t.Fatalf("start release Queue lease guard: %v", err)
	}
	parked, err := store.ParkBlockedRelease(guardCtx, second, now.Add(time.Second))
	if err != nil {
		t.Fatalf("ParkBlockedRelease: %v", err)
	}
	if parked != SandboxLifecycleApplied {
		t.Fatal("blocked release was not parked")
	}
	if err := finishGuard(); err != nil {
		t.Fatalf("consumed release Queue lease guard: %v", err)
	}
	var firstStatus string
	var parkedJobID sql.NullString
	if err := adminDB.QueryRow(`SELECT q.status, o.queue_job_id
		FROM queue_jobs q JOIN sandbox_lifecycle_operations o
		  ON o.workspace_id=q.workspace_id AND o.operation_id=$2
		WHERE q.workspace_id='ws_execution_store' AND q.id=$1`, firstJobID, releaseOperationID).Scan(&firstStatus, &parkedJobID); err != nil {
		t.Fatalf("read parked release notification: %v", err)
	}
	if firstStatus != queue.StatusAcknowledged || parkedJobID.Valid {
		t.Fatalf("parked release status/job = %q/%v; want acknowledged/NULL", firstStatus, parkedJobID)
	}
	if _, err := adminDB.Exec(`UPDATE sandbox_lifecycle_operations
		SET attempt_count=4, lease_owner='sandbox-old', lease_token='lease-old',
		    lease_expires_at=clock_timestamp()+interval '1 minute'
		WHERE workspace_id='ws_execution_store' AND operation_id=$1`, releaseOperationID); err != nil {
		t.Fatalf("seed prior release authority: %v", err)
	}
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(client, 30*time.Minute)
	if err := coordinator.SettleExecution(settlementCtx, SandboxExecutionWork{
		Ref: SandboxExecutionRef{
			WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store",
			SessionThreadID: "thr_execution_store", ToolUseEventID: "evt_execution_a",
		},
		AttemptGeneration: 1,
	}, SandboxExecutionSettlement{
		Kind: SandboxExecutionFailed, ErrorKind: "cancelled", SafeMessage: "sandbox execution was cancelled",
	}); err != nil {
		t.Fatalf("settle foreground execution: %v", err)
	}
	var secondJobID, status string
	if err := adminDB.QueryRow(`SELECT queue_job_id FROM sandbox_lifecycle_operations
		WHERE workspace_id='ws_execution_store' AND operation_id=$1`, releaseOperationID).Scan(&secondJobID); err != nil {
		t.Fatalf("read replacement release job: %v", err)
	}
	if secondJobID == firstJobID {
		t.Fatal("foreground settlement did not create a fresh release notification")
	}
	if err := adminDB.QueryRow(`SELECT status FROM queue_jobs
		WHERE workspace_id='ws_execution_store' AND id=$1`, secondJobID).Scan(&status); err != nil {
		t.Fatalf("read replacement release status: %v", err)
	}
	if status != "pending" {
		t.Fatalf("replacement release status = %q; want pending", status)
	}
	var attemptCount int
	var storedLeaseOwner, storedLeaseToken sql.NullString
	var storedLeaseExpiresAt sql.NullTime
	if err := adminDB.QueryRow(`SELECT attempt_count, lease_owner, lease_token, lease_expires_at
		FROM sandbox_lifecycle_operations
		WHERE workspace_id='ws_execution_store' AND operation_id=$1`, releaseOperationID).Scan(
		&attemptCount, &storedLeaseOwner, &storedLeaseToken, &storedLeaseExpiresAt,
	); err != nil {
		t.Fatalf("read replacement release authority: %v", err)
	}
	if attemptCount != 0 || storedLeaseOwner.Valid || storedLeaseToken.Valid || storedLeaseExpiresAt.Valid {
		t.Fatalf("replacement release authority = attempt %d owner %v token %v expiry %v; want fresh generation", attemptCount, storedLeaseOwner, storedLeaseToken, storedLeaseExpiresAt)
	}
	leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: "ws_execution_store", Kinds: []string{queue.KindSandboxRelease},
		LeaseOwner: "sandbox-successor", MaxJobs: 1, LeaseDuration: time.Minute,
	})
	if err != nil || len(leased) != 1 || leased[0].ID != secondJobID || leased[0].LeasedUntil == nil {
		t.Fatalf("lease replacement release = %#v, %v; want %s", leased, err, secondJobID)
	}
	claimJob := lifecycleJobByOperationID(t, adminDB, "ws_execution_store", releaseOperationID)
	claimJob.LeaseOwner = leased[0].LeasedBy
	claimJob.LeaseToken = leased[0].LeaseToken
	claimJob.LeaseExpiresAt = *leased[0].LeasedUntil
	claimJob.AttemptCount = leased[0].AttemptCount
	setLifecycleQueueLeaseForTest(t, adminDB, claimJob.JobID, claimJob.LeaseOwner, claimJob.LeaseToken, claimJob.AttemptCount, claimJob.LeaseExpiresAt)
	claimCtx := withSandboxQueueAuthority(context.Background(), &sandboxQueueAuthority{
		workspaceID: "ws_execution_store", jobID: leased[0].ID,
		leaseToken: leased[0].LeaseToken, leasedUntil: *leased[0].LeasedUntil,
	})
	if _, disposition, err := store.ClaimRelease(claimCtx, claimJob, now.Add(2*time.Second)); err != nil || disposition != SandboxLifecycleApplied {
		t.Fatalf("ClaimRelease(fresh generation) = %s, %v; want applied", disposition, err)
	}
}

func TestForegroundCompletionAndSandboxReleaseConvergeUnderSessionLockRace(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	now := time.Date(2026, 8, 2, 3, 0, 0, 0, time.UTC)
	seedReadySandboxBinding(t, adminDB, now)
	if _, err := adminDB.Exec(`UPDATE session_runtime_tool_results
		SET execution_state='running', authorized_binding_revision=1,
		    authorized_provider_resource_id='provider_execution_store', preparation_deadline=NULL
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store'
		  AND tool_use_event_id='evt_execution_a'`); err != nil {
		t.Fatalf("mark foreground execution running: %v", err)
	}

	locker, err := adminDB.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin Session race fence: %v", err)
	}
	defer func() { _ = locker.Rollback() }()
	var lockedSession string
	if err := locker.QueryRow(`SELECT id FROM sessions
		WHERE workspace_id='ws_execution_store' AND id='sesn_execution_store' FOR UPDATE`).Scan(&lockedSession); err != nil {
		t.Fatalf("lock Session race fence: %v", err)
	}
	var blockerPID int
	if err := locker.QueryRow(`SELECT pg_backend_pid()`).Scan(&blockerPID); err != nil {
		t.Fatalf("read Session race blocker: %v", err)
	}

	client := dbconnect.NewClientForTesting(runtimeDB)
	settlementCtx := sandboxTestQueueContext(t, runtimeDB)
	settleDone := make(chan error, 1)
	go func() {
		settleDone <- NewPostgreSQLSandboxExecutionCoordinator(client, 30*time.Minute).SettleExecution(
			settlementCtx,
			SandboxExecutionWork{Ref: SandboxExecutionRef{
				WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store",
				SessionThreadID: "thr_execution_store", ToolUseEventID: "evt_execution_a",
			}, AttemptGeneration: 1},
			SandboxExecutionSettlement{
				Kind: SandboxExecutionCompleted, ResultJSON: `{"status":"success","result":{"text":"done"}}`,
			},
		)
	}()
	type releaseResult struct {
		operationID string
		err         error
	}
	releaseDone := make(chan releaseResult, 1)
	go func() {
		var operationID string
		err := client.WithWorkspaceTx(context.Background(), "ws_execution_store", "sandbox.release.completion_race_test", func(tx *dbconnect.Tx) error {
			var err error
			operationID, _, err = EnsureSandboxReleaseTx(
				context.Background(), tx, "ws_execution_store", "sesn_execution_store",
				SandboxReleaseSessionDelete, "provider_execution_store", now,
			)
			return err
		})
		releaseDone <- releaseResult{operationID: operationID, err: err}
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiters int
		if err := adminDB.QueryRow(`WITH RECURSIVE waiters(pid) AS (
			SELECT pid FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))
			UNION
			SELECT activity.pid FROM pg_stat_activity activity
			JOIN waiters blocker ON blocker.pid = ANY(pg_blocking_pids(activity.pid))
		) SELECT count(*) FROM waiters`, blockerPID).Scan(&waiters); err != nil {
			t.Fatalf("read Session race waiters: %v", err)
		}
		if waiters >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Session race waiters = %d; want 2", waiters)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := locker.Commit(); err != nil {
		t.Fatalf("release Session race fence: %v", err)
	}
	if err := <-settleDone; err != nil {
		t.Fatalf("SettleExecution: %v", err)
	}
	released := <-releaseDone
	if released.err != nil || released.operationID == "" {
		t.Fatalf("EnsureSandboxReleaseTx = %q, %v; want operation,nil", released.operationID, released.err)
	}

	var executionState, resultJSON string
	var releaseOperations, releaseJobs int
	if err := adminDB.QueryRow(`SELECT execution_state, result_json FROM session_runtime_tool_results
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store'
		  AND tool_use_event_id='evt_execution_a'`).Scan(&executionState, &resultJSON); err != nil {
		t.Fatalf("read raced execution: %v", err)
	}
	if err := adminDB.QueryRow(`SELECT count(*) FROM sandbox_lifecycle_operations
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store'
		  AND kind='release' AND superseded_by_operation_id IS NULL`).Scan(&releaseOperations); err != nil {
		t.Fatalf("count raced release operations: %v", err)
	}
	if err := adminDB.QueryRow(`SELECT count(*) FROM queue_jobs
		WHERE workspace_id='ws_execution_store' AND kind='sandbox_release' AND status='pending'`).Scan(&releaseJobs); err != nil {
		t.Fatalf("count raced release jobs: %v", err)
	}
	if executionState != "terminal_unconsumed" || !strings.Contains(resultJSON, `"status":"success"`) ||
		releaseOperations != 1 || releaseJobs != 1 {
		t.Fatalf("race convergence = execution %q/%s releases/jobs %d/%d; want completed terminal and one queued release",
			executionState, resultJSON, releaseOperations, releaseJobs)
	}
}

func TestSuccessfulSandboxLifecycleTransitionsWakeParkedRelease(t *testing.T) {
	t.Run("activation completion", func(t *testing.T) {
		runtimeDB, adminDB := newSandboxServiceTestDB(t)
		seedSandboxExecutionStoreFixture(t, adminDB)
		ctx := sandboxTestQueueContext(t, runtimeDB)
		now := time.Now().UTC()
		coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
		execution := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
		if err := coordinator.WaitForActivation(ctx, execution, ExecutionNeedsCreation); err != nil {
			t.Fatalf("WaitForActivation: %v", err)
		}
		store := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
		job := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_activation_operation_id")
		work, disposition, err := store.ClaimActivation(ctx, job, now)
		if err != nil || disposition != SandboxLifecycleApplied {
			t.Fatalf("ClaimActivation = %s,%v", disposition, err)
		}
		seedParkedSandboxRelease(t, adminDB, "sop_release_after_activation", job.SessionID, job.LogicalSandboxID, "provider_replaced", now)
		if _, err := store.CompleteActivation(ctx, work, sandbox.ProviderHandle{Provider: "daytona", SandboxID: "provider_created"}, now.Add(time.Second)); err != nil {
			t.Fatalf("CompleteActivation: %v", err)
		}
		assertSandboxReleaseQueued(t, adminDB, "sop_release_after_activation", true)
	})

	t.Run("materialization completion", func(t *testing.T) {
		runtimeDB, adminDB := newSandboxServiceTestDB(t)
		seedSandboxExecutionStoreFixture(t, adminDB)
		ctx := sandboxTestQueueContext(t, runtimeDB)
		now := time.Now().UTC()
		coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
		execution := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
		if err := coordinator.WaitForActivation(ctx, execution, ExecutionNeedsCreation); err != nil {
			t.Fatalf("WaitForActivation: %v", err)
		}
		store := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
		activationJob := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_activation_operation_id")
		activation, disposition, err := store.ClaimActivation(ctx, activationJob, now)
		if err != nil || disposition != SandboxLifecycleApplied {
			t.Fatalf("ClaimActivation = %s,%v", disposition, err)
		}
		if _, err := store.CompleteActivation(ctx, activation, sandbox.ProviderHandle{Provider: "daytona", SandboxID: "provider_materialized"}, now.Add(time.Second)); err != nil {
			t.Fatalf("CompleteActivation: %v", err)
		}
		job := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_materialization_operation_id")
		work, disposition, err := store.ClaimMaterialization(ctx, job, now.Add(2*time.Second))
		if err != nil || disposition != SandboxLifecycleApplied {
			t.Fatalf("ClaimMaterialization = %s,%v", disposition, err)
		}
		seedParkedSandboxRelease(t, adminDB, "sop_release_after_materialization", job.SessionID, job.LogicalSandboxID, work.Handle.SandboxID, now)
		if err := store.CompleteMaterialization(ctx, work, MaterializationResult{
			MaterializedEnvironmentGeneration: work.TargetEnvironmentGeneration,
			MaterializedResourceRevision:      work.TargetResourceRevision,
			Resources:                         sandbox.ResourceSetup{ResourceCredExpiresAt: ptrTime(now.Add(time.Hour)), ResourceRootsJSON: "[]"},
		}, now.Add(3*time.Second)); err != nil {
			t.Fatalf("CompleteMaterialization: %v", err)
		}
		assertSandboxReleaseQueued(t, adminDB, "sop_release_after_materialization", true)
	})

	t.Run("materialization wait transition", func(t *testing.T) {
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
		store := NewPostgreSQLSandboxLifecycleStore(client, &fixedSandboxResourceSource{}, 30*time.Minute)
		activationJob := readLifecycleJob(t, adminDB, execution.Ref.ToolUseEventID, "waiting_activation_operation_id")
		activation, disposition, err := store.ClaimActivation(ctx, activationJob, now)
		if err != nil || disposition != SandboxLifecycleApplied {
			t.Fatalf("ClaimActivation = %s,%v", disposition, err)
		}
		handle := sandbox.ProviderHandle{Provider: "daytona", SandboxID: "provider_materialization_wait"}
		if _, err := store.CompleteActivation(ctx, activation, handle, now.Add(time.Second)); err != nil {
			t.Fatalf("CompleteActivation: %v", err)
		}
		job := readLifecycleJob(t, adminDB, execution.Ref.ToolUseEventID, "waiting_materialization_operation_id")
		materializationCtx := leaseSandboxMaterializationJobForTest(t, runtimeDB, adminDB, &job)
		work, disposition, err := store.ClaimMaterialization(materializationCtx, job, now.Add(2*time.Second))
		if err != nil || disposition != SandboxLifecycleApplied {
			t.Fatalf("ClaimMaterialization = %s,%v", disposition, err)
		}
		if err := client.WithWorkspaceTx(ctx, execution.Ref.WorkspaceID, "sandbox.release.materialization_wait_test", func(tx *dbconnect.Tx) error {
			return settleSandboxExecutionTx(ctx, tx, execution.Ref, execution.AttemptGeneration, SandboxExecutionSettlement{
				Kind: SandboxExecutionFailed, ErrorKind: "execution_cancelled", SafeMessage: "sandbox execution was cancelled",
			}, now.Add(2*time.Second))
		}); err != nil {
			t.Fatalf("settle materialization waiter: %v", err)
		}
		seedParkedSandboxRelease(t, adminDB, "sop_release_after_materialization_wait", job.SessionID, job.LogicalSandboxID, handle.SandboxID, now)
		if disposition, err := store.WaitMaterializationForActivation(materializationCtx, work, ExecutionNeedsActivation, now.Add(3*time.Second)); err != nil || disposition != SandboxLifecycleApplied {
			t.Fatalf("WaitMaterializationForActivation = %s,%v", disposition, err)
		}
		assertSandboxReleaseQueued(t, adminDB, "sop_release_after_materialization_wait", true)
		var sourceStatus string
		if err := adminDB.QueryRow(`SELECT status FROM queue_jobs WHERE workspace_id=$1 AND id=$2`, job.WorkspaceID, job.JobID).Scan(&sourceStatus); err != nil {
			t.Fatalf("read materialization Queue status: %v", err)
		}
		if sourceStatus != queue.StatusAcknowledged {
			t.Fatalf("materialization Queue status = %q; want acknowledged", sourceStatus)
		}
	})

	t.Run("preparation reinspection", func(t *testing.T) {
		runtimeDB, adminDB := newSandboxServiceTestDB(t)
		seedSandboxExecutionStoreFixture(t, adminDB)
		now := time.Now().UTC()
		seedReadySandboxBinding(t, adminDB, now)
		ctx := sandboxTestQueueContext(t, runtimeDB)
		coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
		work := loadBoundSandboxExecutionWork(t, coordinator, "evt_execution_a")
		prepared, err := coordinator.BeginPreparing(ctx, work, now.Add(time.Minute))
		if err != nil || !prepared {
			t.Fatalf("BeginPreparing = %t,%v", prepared, err)
		}
		seedParkedSandboxRelease(t, adminDB, "sop_release_after_preparation", work.Ref.SessionID, work.Binding.LogicalSandboxID, work.Binding.ProviderResourceID, now)
		if _, err := adminDB.Exec(`UPDATE session_sandbox_bindings SET helper_verified_at=NULL
			WHERE workspace_id=$1 AND session_id=$2`, work.Ref.WorkspaceID, work.Ref.SessionID); err != nil {
			t.Fatalf("expire preparation gate: %v", err)
		}
		if authorized, err := coordinator.AuthorizeRunning(ctx, work); !errors.Is(err, errSandboxExecutionReinspection) || authorized {
			t.Fatalf("AuthorizeRunning = %t,%v; want reinspection", authorized, err)
		}
		assertSandboxExecutionState(t, adminDB, work.Ref.ToolUseEventID, "pending", work.AttemptGeneration)
		assertSandboxReleaseQueued(t, adminDB, "sop_release_after_preparation", true)
	})
}

func TestSessionDeleteReleaseConvergesThroughProductionRunner(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	now := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)
	seedReadySandboxBinding(t, adminDB, now)
	client := dbconnect.NewClientForTesting(runtimeDB)
	var operationID string
	if err := client.WithWorkspaceTx(context.Background(), "ws_execution_store", "sandbox.release.runner_test", func(tx *dbconnect.Tx) error {
		var err error
		operationID, _, err = EnsureSandboxReleaseTx(
			context.Background(), tx, "ws_execution_store", "sesn_execution_store",
			SandboxReleaseSessionDelete, "provider_execution_store", now,
		)
		return err
	}); err != nil {
		t.Fatalf("EnsureSandboxReleaseTx: %v", err)
	}
	queueStore := queue.NewPostgreSQLStore(client)
	queueClient := sandboxProductionQueueClient(t, queueStore)
	adapter := &recordingLifecycleAdapter{
		releasePresenceSet: true,
		releasePresence:    ProviderOutcome[bool]{Value: false},
	}
	providers, err := NewProviderRegistry(map[string]ProviderAdapter{"daytona": adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxReleaseJobRunner{
		Queue:     queueClient,
		Store:     NewPostgreSQLSandboxLifecycleStore(client, nil, 0),
		Providers: providers,
		Config: SandboxLifecycleRunnerConfig{
			WorkspaceID: "ws_execution_store", LeaseOwner: "release-runner-test", MaxJobs: 1,
			LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second,
		},
		Clock: func() time.Time { return now.Add(time.Minute) },
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	var operationState, queueJobID, queueStatus string
	var providerResourceID sql.NullString
	if err := adminDB.QueryRow(`SELECT state, queue_job_id FROM sandbox_lifecycle_operations
		WHERE workspace_id='ws_execution_store' AND operation_id=$1`, operationID).Scan(&operationState, &queueJobID); err != nil {
		t.Fatalf("read completed release: %v", err)
	}
	if err := adminDB.QueryRow(`SELECT provider_resource_id FROM session_sandbox_bindings
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store'`).Scan(&providerResourceID); err != nil {
		t.Fatalf("read released binding: %v", err)
	}
	if err := adminDB.QueryRow(`SELECT status FROM queue_jobs WHERE workspace_id='ws_execution_store' AND id=$1`, queueJobID).Scan(&queueStatus); err != nil {
		t.Fatalf("read acknowledged release Queue row: %v", err)
	}
	if operationState != "completed" || providerResourceID.Valid || queueStatus != queue.StatusAcknowledged {
		t.Fatalf("release = state %q provider %v queue %q; want completed/NULL/acknowledged", operationState, providerResourceID, queueStatus)
	}
}

func TestSandboxLifecycleExhaustionWakesParkedRelease(t *testing.T) {
	t.Run("activation", func(t *testing.T) {
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
		store := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
		job := readLifecycleJob(t, adminDB, execution.Ref.ToolUseEventID, "waiting_activation_operation_id")
		if _, disposition, err := store.ClaimActivation(ctx, job, now); err != nil || disposition != SandboxLifecycleApplied {
			t.Fatalf("ClaimActivation = %s, %v", disposition, err)
		}
		if err := client.WithWorkspaceTx(ctx, execution.Ref.WorkspaceID, "sandbox.release.activation_exhaustion_test", func(tx *dbconnect.Tx) error {
			return settleSandboxExecutionTx(ctx, tx, execution.Ref, execution.AttemptGeneration, SandboxExecutionSettlement{
				Kind: SandboxExecutionFailed, ErrorKind: "execution_cancelled", SafeMessage: "sandbox execution was cancelled",
			}, now)
		}); err != nil {
			t.Fatalf("settle activation waiter: %v", err)
		}
		seedParkedSandboxRelease(t, adminDB, "sop_release_after_activation_exhaustion", job.SessionID, job.LogicalSandboxID, "provider_exhausted_activation", now)
		finalizeLifecycleExhaustionForTest(ctx, t, store, job, queue.KindSandboxActivate, now.Add(time.Second))
		assertSandboxReleaseQueued(t, adminDB, "sop_release_after_activation_exhaustion", true)
	})

	t.Run("materialization", func(t *testing.T) {
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
		store := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
		activationJob := readLifecycleJob(t, adminDB, execution.Ref.ToolUseEventID, "waiting_activation_operation_id")
		activation, disposition, err := store.ClaimActivation(ctx, activationJob, now)
		if err != nil || disposition != SandboxLifecycleApplied {
			t.Fatalf("ClaimActivation = %s, %v", disposition, err)
		}
		handle := sandbox.ProviderHandle{Provider: "daytona", SandboxID: "provider_materialization_exhaustion"}
		if _, err := store.CompleteActivation(ctx, activation, handle, now.Add(time.Second)); err != nil {
			t.Fatalf("CompleteActivation: %v", err)
		}
		job := readLifecycleJob(t, adminDB, execution.Ref.ToolUseEventID, "waiting_materialization_operation_id")
		if _, disposition, err := store.ClaimMaterialization(ctx, job, now.Add(2*time.Second)); err != nil || disposition != SandboxLifecycleApplied {
			t.Fatalf("ClaimMaterialization = %s, %v", disposition, err)
		}
		if err := client.WithWorkspaceTx(ctx, execution.Ref.WorkspaceID, "sandbox.release.materialization_exhaustion_test", func(tx *dbconnect.Tx) error {
			return settleSandboxExecutionTx(ctx, tx, execution.Ref, execution.AttemptGeneration, SandboxExecutionSettlement{
				Kind: SandboxExecutionFailed, ErrorKind: "execution_cancelled", SafeMessage: "sandbox execution was cancelled",
			}, now.Add(2*time.Second))
		}); err != nil {
			t.Fatalf("settle materialization waiter: %v", err)
		}
		seedParkedSandboxRelease(t, adminDB, "sop_release_after_materialization_exhaustion", job.SessionID, job.LogicalSandboxID, handle.SandboxID, now)
		finalizeLifecycleExhaustionForTest(ctx, t, store, job, queue.KindSandboxMaterialize, now.Add(3*time.Second))
		assertSandboxReleaseQueued(t, adminDB, "sop_release_after_materialization_exhaustion", true)
	})
}

func finalizeLifecycleExhaustionForTest(ctx context.Context, t *testing.T, store *PostgreSQLSandboxLifecycleStore, job SandboxLifecycleJob, kind string, now time.Time) {
	t.Helper()
	queueJob := &queuev1.QueueJob{
		Id: job.JobID, WorkspaceId: job.WorkspaceID, Kind: kind,
		PartitionKey: queue.FormatSandboxLifecyclePartitionKey(workspace.ID(job.WorkspaceID), job.LogicalSandboxID),
		DedupeKey:    queue.FormatSandboxLifecycleDedupeKey(kind, workspace.ID(job.WorkspaceID), job.LogicalSandboxID, job.OperationID),
		LeasedBy:     job.LeaseOwner, LeaseToken: job.LeaseToken,
		LeasedUntil: job.LeaseExpiresAt.Format(time.RFC3339Nano), AttemptCount: int32(job.AttemptCount), MaxAttempts: int32(job.AttemptCount),
	}
	if disposition, err := store.FinalizeExhaustedLifecycle(ctx, queueJob, kind, now); err != nil || disposition != SandboxLifecycleApplied {
		t.Fatalf("FinalizeExhaustedLifecycle(%s) = %s, %v", kind, disposition, err)
	}
}

func TestSandboxReleaseRemainsParkedWhileAnotherBlockerRuns(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	now := time.Now().UTC()
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	execution := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(ctx, execution, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	store := NewPostgreSQLSandboxLifecycleStore(dbconnect.NewClientForTesting(runtimeDB), &fixedSandboxResourceSource{}, 30*time.Minute)
	job := readLifecycleJob(t, adminDB, "evt_execution_a", "waiting_activation_operation_id")
	work, disposition, err := store.ClaimActivation(ctx, job, now)
	if err != nil || disposition != SandboxLifecycleApplied {
		t.Fatalf("ClaimActivation = %s,%v", disposition, err)
	}
	if _, err := adminDB.Exec(`UPDATE session_runtime_tool_results
		SET execution_state='running', authorized_provider_resource_id='provider_other', authorized_binding_revision=1
		WHERE workspace_id='ws_execution_store' AND tool_use_event_id='evt_execution_b'`); err != nil {
		t.Fatalf("seed second release blocker: %v", err)
	}
	seedParkedSandboxRelease(t, adminDB, "sop_release_still_blocked", job.SessionID, job.LogicalSandboxID, "provider_other", now)
	if _, err := store.CompleteActivation(ctx, work, sandbox.ProviderHandle{Provider: "daytona", SandboxID: "provider_created"}, now.Add(time.Second)); err != nil {
		t.Fatalf("CompleteActivation: %v", err)
	}
	assertSandboxReleaseQueued(t, adminDB, "sop_release_still_blocked", false)
}

func seedParkedSandboxRelease(t *testing.T, db *sql.DB, operationID string, sessionID string, logicalSandboxID string, targetHandle string, now time.Time) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO sandbox_lifecycle_operations (
		workspace_id, operation_id, session_id, logical_sandbox_id, kind, state,
		target_provider_resource_id, release_reason, created_at, updated_at
	) VALUES (
		'ws_execution_store', $1, $2, $3, 'release', 'pending', $4, 'replaced_handle', $5, $5
	)`, operationID, sessionID, logicalSandboxID, targetHandle, now); err != nil {
		t.Fatalf("seed parked Sandbox release: %v", err)
	}
}

func assertSandboxReleaseQueued(t *testing.T, db *sql.DB, operationID string, wantQueued bool) {
	t.Helper()
	var jobID sql.NullString
	if err := db.QueryRow(`SELECT queue_job_id FROM sandbox_lifecycle_operations
		WHERE workspace_id='ws_execution_store' AND operation_id=$1`, operationID).Scan(&jobID); err != nil {
		t.Fatalf("read Sandbox release queue identity: %v", err)
	}
	if jobID.Valid != wantQueued {
		t.Fatalf("Sandbox release queued = %t; want %t", jobID.Valid, wantQueued)
	}
	if wantQueued {
		var status string
		if err := db.QueryRow(`SELECT status FROM queue_jobs WHERE workspace_id='ws_execution_store' AND id=$1`, jobID.String).Scan(&status); err != nil {
			t.Fatalf("read Sandbox release Queue row: %v", err)
		}
		if status != queue.StatusPending {
			t.Fatalf("Sandbox release Queue status = %q; want pending", status)
		}
	}
}

func TestSandboxReleaseRejectsSupersededLeaseWriters(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	now := time.Date(2026, 7, 31, 19, 0, 0, 0, time.UTC)
	seedReadySandboxBinding(t, adminDB, now)
	client := dbconnect.NewClientForTesting(runtimeDB)
	if err := client.WithWorkspaceTx(context.Background(), "ws_execution_store", "sandbox.release.lease_test", func(tx *dbconnect.Tx) error {
		_, _, err := EnsureSandboxReleaseTx(
			context.Background(), tx, "ws_execution_store", "sesn_execution_store",
			SandboxReleaseSessionDelete, "provider_execution_store", now,
		)
		return err
	}); err != nil {
		t.Fatalf("EnsureSandboxReleaseTx: %v", err)
	}

	queueStore := queue.NewPostgreSQLStore(client)
	first := leaseReleaseJob(t, queueStore, now, time.Second, "release-worker-a")
	store := NewPostgreSQLSandboxLifecycleStore(client, nil, 0)
	firstCtx := withLifecycleJobQueueAuthority(context.Background(), first)
	firstWork, claimed, err := store.ClaimRelease(firstCtx, first, now)
	if err != nil || claimed != SandboxLifecycleApplied {
		t.Fatalf("ClaimRelease(first) = claimed %s err %v; want true/nil", claimed, err)
	}
	if _, err := adminDB.Exec(`UPDATE queue_jobs SET leased_until=clock_timestamp()-interval '1 second' WHERE workspace_id=$1 AND id=$2`, first.WorkspaceID, first.JobID); err != nil {
		t.Fatalf("expire first Queue lease: %v", err)
	}
	if reclaimed, err := queueStore.ReclaimExpiredLeases(context.Background(), queue.ReclaimExpiredLeasesRequest{
		WorkspaceID: workspace.ID(first.WorkspaceID), Limit: 1,
	}); err != nil || reclaimed != 1 {
		t.Fatalf("ReclaimExpiredLeases = %d, %v; want 1/nil", reclaimed, err)
	}
	second := leaseReleaseJob(t, queueStore, time.Now().UTC().Add(time.Second), time.Minute, "release-worker-b")
	secondCtx := withLifecycleJobQueueAuthority(context.Background(), second)
	secondWork, claimed, err := store.ClaimRelease(secondCtx, second, now.Add(2*time.Second))
	if err != nil || claimed != SandboxLifecycleApplied {
		t.Fatalf("ClaimRelease(second) = claimed %s err %v; want true/nil", claimed, err)
	}
	if authorized, err := store.AuthorizeRelease(secondCtx, secondWork, now.Add(3*time.Second)); err != nil || !authorized {
		t.Fatalf("AuthorizeRelease(second) = %t, %v; want true/nil", authorized, err)
	}
	if authorized, err := store.AuthorizeRelease(firstCtx, firstWork, now.Add(4*time.Second)); !errors.Is(err, errQueueLeaseLost) || authorized {
		t.Fatalf("AuthorizeRelease(stale) = %t, %v; want false/lost authority", authorized, err)
	}
	if err := store.RearmRelease(firstCtx, firstWork, now.Add(4*time.Second)); !errors.Is(err, errQueueLeaseLost) {
		t.Fatalf("RearmRelease(stale) = %v; want lost authority", err)
	}
	if err := store.ObserveUnknownRelease(firstCtx, firstWork, "stale_unknown", now.Add(4*time.Second)); !errors.Is(err, errQueueLeaseLost) {
		t.Fatalf("ObserveUnknownRelease(stale) = %v; want lost authority", err)
	}
	if err := store.FailRelease(firstCtx, firstWork, ProviderProvedNotStarted, ProviderTerminal, "stale_failure", "stale worker", now.Add(4*time.Second)); !errors.Is(err, errQueueLeaseLost) {
		t.Fatalf("FailRelease(stale) = %v; want lost authority", err)
	}
	if err := store.CompleteRelease(firstCtx, firstWork, now.Add(4*time.Second)); !errors.Is(err, errQueueLeaseLost) {
		t.Fatalf("CompleteRelease(stale) = %v; want lost authority", err)
	}
	var state, boundary string
	var leaseOwner, leaseToken sql.NullString
	if err := adminDB.QueryRow(`SELECT state, outcome_effect_boundary, lease_owner, lease_token
		FROM sandbox_lifecycle_operations
		WHERE workspace_id='ws_execution_store' AND operation_id=$1`, second.OperationID).Scan(
		&state, &boundary, &leaseOwner, &leaseToken,
	); err != nil {
		t.Fatalf("read release operation: %v", err)
	}
	if state != "running" || boundary != string(ProviderSubmitted) || leaseOwner.String != second.LeaseOwner || leaseToken.String != second.LeaseToken {
		t.Fatalf("release operation = %q/%q/%q/%q; want current running submission", state, boundary, leaseOwner.String, leaseToken.String)
	}
	if err := store.CompleteRelease(secondCtx, secondWork, now.Add(5*time.Second)); err != nil {
		t.Fatalf("CompleteRelease(second): %v", err)
	}
	if err := adminDB.QueryRow(`SELECT state FROM sandbox_lifecycle_operations
		WHERE workspace_id='ws_execution_store' AND operation_id=$1`, second.OperationID).Scan(&state); err != nil {
		t.Fatalf("read completed release operation: %v", err)
	}
	if state != "completed" {
		t.Fatalf("release state = %q; want completed", state)
	}
}

func leaseReleaseJob(t *testing.T, store *queue.PostgreSQLQueueStore, now time.Time, duration time.Duration, owner string) SandboxLifecycleJob {
	t.Helper()
	leased, err := store.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: workspace.ID("ws_execution_store"), Kinds: []string{queue.KindSandboxRelease},
		LeaseOwner: owner, MaxJobs: 1, LeaseDuration: duration, Now: time.Now().UTC(),
	})
	if err != nil || len(leased) != 1 {
		t.Fatalf("Lease release = %#v, %v; want one job", leased, err)
	}
	job := leased[0]
	transport := &queuev1.QueueJob{
		Id: job.ID, WorkspaceId: string(job.WorkspaceID), Kind: job.Kind,
		PartitionKey: job.PartitionKey, DedupeKey: job.DedupeKey, PayloadVersion: int32(job.PayloadVersion),
		PayloadJson: string(job.PayloadJSON), LeasedBy: job.LeasedBy, LeaseToken: job.LeaseToken,
		AttemptCount: int32(job.AttemptCount), MaxAttempts: int32(job.MaxAttempts),
	}
	if job.LeasedUntil != nil {
		transport.LeasedUntil = job.LeasedUntil.UTC().Format(time.RFC3339Nano)
	}
	decoded, err := DecodeSandboxLifecycleJob(transport, queue.KindSandboxRelease)
	if err != nil {
		t.Fatalf("DecodeSandboxLifecycleJob: %v", err)
	}
	return decoded
}
