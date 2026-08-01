package tetralsandbox

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

func TestEnsureSandboxReleaseFencesExecutionAndSchedulesBackgroundCancellation(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	now := time.Date(2026, 7, 31, 18, 0, 0, 0, time.UTC)
	seedReadySandboxBinding(t, adminDB, now)
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
		context.Background(), &initialJob, now.Add(time.Minute),
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
	if err := adminDB.QueryRow(`SELECT count(*) FROM queue_jobs
		WHERE workspace_id='ws_execution_store' AND kind='sandbox_background_command'
		  AND dedupe_key=$1 AND status='pending'`,
		queue.FormatSandboxBackgroundCommandDedupeKey("ws_execution_store", "sesn_execution_store", "task_execution", nextRequestID),
	).Scan(&nextJobs); err != nil {
		t.Fatalf("read successor cancellation job: %v", err)
	}
	if nextJobs != 1 {
		t.Fatalf("successor cancellation jobs = %d; want 1", nextJobs)
	}
}

func TestForegroundSettlementWakesParkedSandboxRelease(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	now := time.Date(2026, 7, 31, 18, 30, 0, 0, time.UTC)
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
	var firstJobID string
	var firstPartition, firstDedupe string
	if err := adminDB.QueryRow(`SELECT queue_job_id, queue_partition_key, queue_dedupe_key FROM sandbox_lifecycle_operations
		WHERE workspace_id='ws_execution_store' AND operation_id=$1`, releaseOperationID).Scan(&firstJobID, &firstPartition, &firstDedupe); err != nil {
		t.Fatalf("read first release job: %v", err)
	}
	const leaseToken = "lease_release_park"
	if _, err := adminDB.Exec(`UPDATE queue_jobs
		SET status='leased', lease_token=$2, leased_by='sandbox-test', leased_at=$3,
		    leased_until=$3::timestamptz + interval '1 minute', attempt_count=attempt_count+1, updated_at=$3
		WHERE workspace_id='ws_execution_store' AND id=$1`, firstJobID, leaseToken, now.Add(time.Second)); err != nil {
		t.Fatalf("lease first release notification: %v", err)
	}
	parked, err := NewPostgreSQLSandboxLifecycleStore(client, nil, 0).ParkBlockedRelease(context.Background(), SandboxLifecycleJob{
		JobID: firstJobID, LeaseToken: leaseToken, WorkspaceID: "ws_execution_store",
		SessionID: "sesn_execution_store", LogicalSandboxID: "sbox_execution_store",
		OperationID: releaseOperationID,
		QueueJob: &queuev1.QueueJob{Id: firstJobID, WorkspaceId: "ws_execution_store", Kind: queue.KindSandboxRelease,
			PartitionKey: firstPartition, DedupeKey: firstDedupe, LeaseToken: leaseToken},
	}, now.Add(time.Second))
	if err != nil {
		t.Fatalf("ParkBlockedRelease: %v", err)
	}
	if !parked {
		t.Fatal("blocked release was not parked")
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
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(client, 30*time.Minute)
	if err := coordinator.SettleExecution(context.Background(), SandboxExecutionWork{
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
	firstWork, claimed, err := store.ClaimRelease(context.Background(), first, now)
	if err != nil || !claimed {
		t.Fatalf("ClaimRelease(first) = claimed %t err %v; want true/nil", claimed, err)
	}
	if reclaimed, err := queueStore.ReclaimExpiredLeases(context.Background(), queue.ReclaimExpiredLeasesRequest{
		WorkspaceID: workspace.ID(first.WorkspaceID), Limit: 1, Now: now.Add(2 * time.Second),
	}); err != nil || reclaimed != 1 {
		t.Fatalf("ReclaimExpiredLeases = %d, %v; want 1/nil", reclaimed, err)
	}
	second := leaseReleaseJob(t, queueStore, now.Add(2*time.Second), time.Minute, "release-worker-b")
	secondWork, claimed, err := store.ClaimRelease(context.Background(), second, now.Add(2*time.Second))
	if err != nil || !claimed {
		t.Fatalf("ClaimRelease(second) = claimed %t err %v; want true/nil", claimed, err)
	}
	if authorized, err := store.AuthorizeRelease(context.Background(), secondWork, now.Add(3*time.Second)); err != nil || !authorized {
		t.Fatalf("AuthorizeRelease(second) = %t, %v; want true/nil", authorized, err)
	}
	if authorized, err := store.AuthorizeRelease(context.Background(), firstWork, now.Add(4*time.Second)); err != nil || authorized {
		t.Fatalf("AuthorizeRelease(stale) = %t, %v; want false/nil", authorized, err)
	}
	if err := store.RearmRelease(context.Background(), firstWork, now.Add(4*time.Second)); err != nil {
		t.Fatalf("RearmRelease(stale): %v", err)
	}
	if err := store.ObserveUnknownRelease(context.Background(), firstWork, "stale_unknown", now.Add(4*time.Second)); err != nil {
		t.Fatalf("ObserveUnknownRelease(stale): %v", err)
	}
	if err := store.FailRelease(context.Background(), firstWork, ProviderProvedNotStarted, ProviderTerminal, "stale_failure", "stale worker", now.Add(4*time.Second)); err != nil {
		t.Fatalf("FailRelease(stale): %v", err)
	}
	if err := store.CompleteRelease(context.Background(), firstWork, now.Add(4*time.Second)); err != nil {
		t.Fatalf("CompleteRelease(stale): %v", err)
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
	if err := store.CompleteRelease(context.Background(), secondWork, now.Add(5*time.Second)); err != nil {
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
		LeaseOwner: owner, MaxJobs: 1, LeaseDuration: duration, Now: now,
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
