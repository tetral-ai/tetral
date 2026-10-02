package jobrunner

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/session"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"
)

func TestSessionDeleteCleanupDrainsUnadoptedOutputCaptureBeforeRemovingReceipt(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_delete_output_capture"
	seedBridgeAPISession(t, admin, "default", sessionID, "thr_delete_output_capture")
	now := time.Date(2026, 7, 31, 20, 0, 0, 0, time.UTC)
	if _, err := admin.Exec(`INSERT INTO sandbox_output_capture_operations (
		workspace_id, session_id, session_thread_id, finish_idle_write_id, capture_generation,
		state, binding_id, binding_generation,
		outcome_state, outcome_digest, retain_until, created_at, updated_at, staged_at
	) VALUES ('default',$1,'thr_delete_output_capture','rwrite_delete_output_capture',1,
		'staged','bind_delete_output_capture',1,
		'staged',$2,$3,$3,$3,$3)`, sessionID, strings.Repeat("a", 64), now.Add(time.Hour)); err != nil {
		t.Fatalf("seed output capture: %v", err)
	}
	client := dbconnect.NewClientForTesting(runtime)
	const openTransportJobID = "qjob_cap"
	if err := client.WithWorkspaceTx(context.Background(), "default", "test.delete_output_capture.open_transport", func(tx *dbconnect.Tx) error {
		_, err := queue.EnqueueTx(context.Background(), tx, queue.EnqueueRequest{
			ID: openTransportJobID, WorkspaceID: "default", Kind: queue.KindSandboxToolExecute,
			PartitionKey:   queue.FormatSandboxExecutionPartitionKey("default", sessionID, "thr_delete_output_capture", "evt_delete_capture_tool"),
			DedupeKey:      queue.FormatSandboxToolExecuteDedupeKey("default", sessionID, "thr_delete_output_capture", "evt_delete_capture_tool", 1),
			PayloadVersion: 1,
			PayloadJSON:    []byte(`{"workspace_id":"default","session_id":"` + sessionID + `","session_thread_id":"thr_delete_output_capture","tool_use_event_id":"evt_delete_capture_tool"}`),
			MaxAttempts:    1, Now: now,
		})
		return err
	}); err != nil {
		t.Fatalf("seed open Sandbox transport: %v", err)
	}
	var pending bool
	if err := client.WithWorkspaceTx(context.Background(), "default", "test.delete_output_capture.ensure", func(tx *dbconnect.Tx) error {
		var err error
		pending, err = runtimecontrol.EnsureSessionOutputCaptureCleanupTx(context.Background(), tx, "default", sessionID, now)
		return err
	}); err != nil {
		t.Fatalf("ensure output capture cleanup: %v", err)
	}
	if !pending {
		t.Fatal("output capture cleanup was not held pending")
	}
	var captureState string
	var cleanupJobs int
	if err := admin.QueryRow(`SELECT state FROM sandbox_output_capture_operations
		WHERE workspace_id='default' AND session_id=$1`, sessionID).Scan(&captureState); err != nil {
		t.Fatalf("read capture behind open transport: %v", err)
	}
	if err := admin.QueryRow(`SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND kind=$1`, queue.KindSandboxOutputCaptureCleanup).Scan(&cleanupJobs); err != nil {
		t.Fatalf("count capture cleanup jobs: %v", err)
	}
	if captureState != "staged" || cleanupJobs != 0 {
		t.Fatalf("capture behind open transport = %s with %d cleanup jobs; want staged/0", captureState, cleanupJobs)
	}
	if _, err := admin.Exec(`UPDATE queue_jobs SET status='acknowledged', acknowledged_at=$2, updated_at=$2
		WHERE workspace_id='default' AND id=$1`, openTransportJobID, now.Add(time.Second)); err != nil {
		t.Fatalf("close Sandbox transport: %v", err)
	}
	if err := client.WithWorkspaceTx(context.Background(), "default", "test.delete_output_capture.ensure_after_transport", func(tx *dbconnect.Tx) error {
		var err error
		pending, err = runtimecontrol.EnsureSessionOutputCaptureCleanupTx(context.Background(), tx, "default", sessionID, now.Add(2*time.Second))
		return err
	}); err != nil {
		t.Fatalf("ensure output capture cleanup after transport: %v", err)
	}
	var queueStatus string
	if err := admin.QueryRow(`SELECT state FROM sandbox_output_capture_operations
		WHERE workspace_id='default' AND session_id=$1`, sessionID).Scan(&captureState); err != nil {
		t.Fatalf("read capture state: %v", err)
	}
	if err := admin.QueryRow(`SELECT status FROM queue_jobs
		WHERE workspace_id='default' AND kind=$1 AND payload_json::jsonb ->> 'session_id'=$2`, queue.KindSandboxOutputCaptureCleanup, sessionID).Scan(&queueStatus); err != nil {
		t.Fatalf("read capture cleanup job: %v", err)
	}
	if captureState != "cleanup_pending" || queueStatus != queue.StatusPending {
		t.Fatalf("capture cleanup = %s/%s; want cleanup_pending/pending", captureState, queueStatus)
	}
	if _, err := admin.Exec(`UPDATE queue_jobs SET status='acknowledged', acknowledged_at=$2, updated_at=$2
		WHERE workspace_id='default' AND kind=$1 AND payload_json::jsonb ->> 'session_id'=$3`, queue.KindSandboxOutputCaptureCleanup, now.Add(time.Minute), sessionID); err != nil {
		t.Fatalf("close cleanup transport: %v", err)
	}
	if _, err := admin.Exec(`UPDATE sandbox_output_capture_operations SET state='cleaned', cleaned_at=$2, updated_at=$2
		WHERE workspace_id='default' AND session_id=$1`, sessionID, now.Add(time.Minute)); err != nil {
		t.Fatalf("complete output capture cleanup: %v", err)
	}
	if err := client.WithWorkspaceTx(context.Background(), "default", "test.delete_output_capture.finish", func(tx *dbconnect.Tx) error {
		var err error
		pending, err = runtimecontrol.EnsureSessionOutputCaptureCleanupTx(context.Background(), tx, "default", sessionID, now.Add(2*time.Minute))
		return err
	}); err != nil {
		t.Fatalf("finish output capture cleanup: %v", err)
	}
	var rows int
	if err := admin.QueryRow(`SELECT count(*) FROM sandbox_output_capture_operations WHERE workspace_id='default' AND session_id=$1`, sessionID).Scan(&rows); err != nil {
		t.Fatalf("count output capture receipts: %v", err)
	}
	if pending || rows != 0 {
		t.Fatalf("finished output capture cleanup = pending %t rows %d; want false/0", pending, rows)
	}
}

func TestHasOpenSessionSandboxQueueJobsUsesOnlyOpenStatusesInTheTargetSession(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	client := dbconnect.NewClientForTesting(runtime)
	now := time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC)
	enqueue := func(id string, sessionID string) {
		t.Helper()
		if err := client.WithWorkspaceTx(context.Background(), "default", "test.open_sandbox_queue_job", func(tx *dbconnect.Tx) error {
			_, err := queue.EnqueueTx(context.Background(), tx, queue.EnqueueRequest{
				ID: id, WorkspaceID: "default", Kind: queue.KindSandboxToolExecute,
				PartitionKey:   queue.FormatSandboxExecutionPartitionKey("default", sessionID, "thr_"+sessionID, "evt_"+sessionID),
				DedupeKey:      queue.FormatSandboxToolExecuteDedupeKey("default", sessionID, "thr_"+sessionID, "evt_"+sessionID, 1),
				PayloadVersion: 1, PayloadJSON: []byte(`{"workspace_id":"default","session_id":"` + sessionID + `","session_thread_id":"thr_` + sessionID + `","tool_use_event_id":"evt_` + sessionID + `"}`),
				MaxAttempts: 5, Now: now,
			})
			return err
		}); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}
	open := func(sessionID string) bool {
		t.Helper()
		var result bool
		if err := client.WithWorkspaceTx(context.Background(), "default", "test.read_open_sandbox_queue_jobs", func(tx *dbconnect.Tx) error {
			var err error
			result, err = runtimecontrol.HasOpenSessionSandboxQueueJobsTx(context.Background(), tx, "default", sessionID)
			return err
		}); err != nil {
			t.Fatalf("read open jobs for %s: %v", sessionID, err)
		}
		return result
	}

	enqueue("qjob_target_session", "sesn_target_queue_gate")
	enqueue("qjob_other_session", "sesn_other_queue_gate")
	if !open("sesn_target_queue_gate") {
		t.Fatal("pending target Session job did not block cleanup")
	}
	if _, err := admin.Exec(`UPDATE queue_jobs SET status='leased', leased_by='worker', lease_token='lease', leased_at=$2, leased_until=$3, updated_at=$2 WHERE workspace_id='default' AND id=$1`, "qjob_target_session", now, now.Add(time.Minute)); err != nil {
		t.Fatalf("lease target job: %v", err)
	}
	if !open("sesn_target_queue_gate") {
		t.Fatal("leased target Session job did not block cleanup")
	}
	for _, terminalStatus := range []string{queue.StatusAcknowledged, queue.StatusCancelled, queue.StatusDeadLettered} {
		if _, err := admin.Exec(`UPDATE queue_jobs
			SET status=$2, leased_by=NULL, lease_token=NULL, leased_at=NULL, leased_until=NULL, updated_at=$3
			WHERE workspace_id='default' AND id=$1`, "qjob_target_session", terminalStatus, now.Add(2*time.Minute)); err != nil {
			t.Fatalf("set target job %s: %v", terminalStatus, err)
		}
		if open("sesn_target_queue_gate") {
			t.Fatalf("%s target Session job blocked cleanup", terminalStatus)
		}
	}
	if !open("sesn_other_queue_gate") {
		t.Fatal("pending other Session job was not independently visible")
	}
}

func TestSessionDeleteCleanupSupersedesFailedDisplacedSandboxRelease(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_delete_failed_release"
		cleanupID = "delcln_delete_failed_release"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, "thr_delete_failed_release")
	now := time.Date(2026, 7, 31, 20, 0, 0, 0, time.UTC)
	if _, err := admin.Exec(`UPDATE sessions SET lifecycle_state='deleted', delete_cleanup_id=$2
		WHERE workspace_id='default' AND id=$1`, sessionID, cleanupID); err != nil {
		t.Fatalf("mark session deleted: %v", err)
	}
	if _, err := admin.Exec(`INSERT INTO session_sandbox_bindings (
		workspace_id, session_id, logical_sandbox_id, environment_id, environment_generation,
		provider, provider_resource_id, binding_revision, materialized_resource_revision,
		resource_roots_json, provider_metadata_json, created_at, updated_at
	) VALUES ('default',$1,'sbox_delete_failed_release',$2,1,'daytona',
		'provider_current_release',1,1,'[]','{}',$3,$3)`, sessionID, "env_"+sessionID, now); err != nil {
		t.Fatalf("seed sandbox binding: %v", err)
	}
	if _, err := admin.Exec(`INSERT INTO sandbox_lifecycle_operations (
		workspace_id, operation_id, session_id, logical_sandbox_id, kind, state,
		target_provider_resource_id, release_reason, error_kind, safe_message,
		created_at, updated_at
	) VALUES ('default','sop_failed_displaced_release',$1,'sbox_delete_failed_release','release','failed',
		'provider_displaced_release','replaced_handle','sandbox_release_attempts_exhausted',
		'sandbox release could not be completed',$2,$2)`, sessionID, now); err != nil {
		t.Fatalf("seed failed release receipt: %v", err)
	}

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return now.Add(time.Minute) }
	result, err := store.finalizeSessionDeleteCleanup(context.Background(), RuntimeJob{
		Kind: queue.KindSessionDeleteCleanup, WorkspaceID: "default", SessionID: sessionID,
		DeleteCleanupID: cleanupID, AttemptCount: 1, MaxAttempts: 5,
	})
	if err != nil {
		t.Fatalf("finalizeSessionDeleteCleanup: %v", err)
	}
	if result.Status != RuntimeDeliveryRejected || !result.Retryable {
		t.Fatalf("cleanup result = %+v; want retryable incomplete release", result)
	}
	var successorID sql.NullString
	if err := admin.QueryRow(`SELECT superseded_by_operation_id FROM sandbox_lifecycle_operations
		WHERE workspace_id='default' AND session_id=$1 AND operation_id='sop_failed_displaced_release'`, sessionID).Scan(&successorID); err != nil {
		t.Fatalf("read failed release successor: %v", err)
	}
	if !successorID.Valid {
		t.Fatal("failed displaced release was not superseded")
	}
	var state, handle, reason string
	if err := admin.QueryRow(`SELECT state, target_provider_resource_id, release_reason
		FROM sandbox_lifecycle_operations WHERE workspace_id='default' AND operation_id=$1`, successorID.String).Scan(&state, &handle, &reason); err != nil {
		t.Fatalf("read displaced release successor: %v", err)
	}
	if state != "pending" || handle != "provider_displaced_release" || reason != "session_delete" {
		t.Fatalf("successor = %q/%q/%q; want pending displaced handle under session delete", state, handle, reason)
	}
}

func TestSessionDeleteCleanupFinalAttemptDoesNotCreateReleaseSuccessor(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_delete_final_release"
		cleanupID = "delcln_delete_final_release"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, "thr_delete_final_release")
	now := time.Date(2026, 7, 31, 21, 0, 0, 0, time.UTC)
	if _, err := admin.Exec(`UPDATE sessions SET lifecycle_state='deleted', delete_cleanup_id=$2
		WHERE workspace_id='default' AND id=$1`, sessionID, cleanupID); err != nil {
		t.Fatalf("mark session deleted: %v", err)
	}
	if _, err := admin.Exec(`INSERT INTO session_sandbox_bindings (
		workspace_id, session_id, logical_sandbox_id, environment_id, environment_generation,
		provider, provider_resource_id, binding_revision, materialized_resource_revision,
		resource_roots_json, provider_metadata_json, release_requested_at, release_reason,
		created_at, updated_at
	) VALUES ('default',$1,'sbox_delete_final_release',$2,1,'daytona',
		'provider_delete_final_release',1,1,'[]','{}',$3,'session_delete',$3,$3)`, sessionID, "env_"+sessionID, now); err != nil {
		t.Fatalf("seed sandbox binding: %v", err)
	}
	if _, err := admin.Exec(`INSERT INTO sandbox_lifecycle_operations (
		workspace_id, operation_id, session_id, logical_sandbox_id, kind, state,
		target_provider_resource_id, release_reason, error_kind, safe_message,
		created_at, updated_at
	) VALUES ('default','sop_delete_final_release',$1,'sbox_delete_final_release','release','failed',
		'provider_delete_final_release','session_delete','sandbox_release_attempts_exhausted',
		'sandbox release could not be completed',$2,$2)`, sessionID, now); err != nil {
		t.Fatalf("seed failed release: %v", err)
	}

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	result, err := store.finalizeSessionDeleteCleanup(context.Background(), RuntimeJob{
		Kind: queue.KindSessionDeleteCleanup, WorkspaceID: "default", SessionID: sessionID,
		DeleteCleanupID: cleanupID, AttemptCount: 5, MaxAttempts: 5,
	})
	if err != nil {
		t.Fatalf("finalizeSessionDeleteCleanup: %v", err)
	}
	if result.Status != RuntimeDeliveryRejected || result.Retryable || result.ErrorKind != "sandbox_release_incomplete" {
		t.Fatalf("cleanup result = %+v; want terminal incomplete release", result)
	}
	var releases int
	if err := admin.QueryRow(`SELECT count(*) FROM sandbox_lifecycle_operations
		WHERE workspace_id='default' AND session_id=$1 AND kind='release'`, sessionID).Scan(&releases); err != nil {
		t.Fatalf("count release operations: %v", err)
	}
	if releases != 1 {
		t.Fatalf("release operations = %d; final cleanup must not create a successor", releases)
	}
}

func TestSessionDeleteCleanupRetainsUnresolvedHandleCreation(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_delete_unresolved_create"
		cleanupID = "delcln_delete_unresolved_create"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, "thr_delete_unresolved_create")
	now := time.Date(2026, 7, 31, 21, 30, 0, 0, time.UTC)
	if _, err := admin.Exec(`UPDATE sessions SET lifecycle_state='deleted', delete_cleanup_id=$2
		WHERE workspace_id='default' AND id=$1`, sessionID, cleanupID); err != nil {
		t.Fatalf("mark session deleted: %v", err)
	}
	if _, err := admin.Exec(`INSERT INTO session_sandbox_bindings (
		workspace_id, session_id, logical_sandbox_id, environment_id, environment_generation,
		provider, provider_resource_id, binding_revision, materialized_resource_revision,
		resource_roots_json, provider_metadata_json, created_at, updated_at
	) VALUES ('default',$1,'sbox_delete_unresolved_create',$2,1,'daytona',
		NULL,1,0,'[]','{}',$3,$3)`, sessionID, "env_"+sessionID, now); err != nil {
		t.Fatalf("seed sandbox binding: %v", err)
	}
	if _, err := admin.Exec(`INSERT INTO sandbox_lifecycle_operations (
		workspace_id, operation_id, session_id, logical_sandbox_id, kind, state,
		observed_binding_revision, target_environment_generation, provider_create_name,
		provider_request_labels_json, outcome_effect_boundary, outcome_disposition,
		error_kind, safe_message, created_at, updated_at
	) VALUES ('default','sop_delete_unresolved_create',$1,'sbox_delete_unresolved_create',
		'create','running',1,1,'sbox_delete_unresolved_create','{}','outcome_unknown',
		'terminal','provider_timeout','sandbox creation outcome requires observation',$2,$2)`, sessionID, now); err != nil {
		t.Fatalf("seed unresolved activation: %v", err)
	}

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return now.Add(time.Minute) }
	result, err := store.finalizeSessionDeleteCleanup(context.Background(), RuntimeJob{
		Kind: queue.KindSessionDeleteCleanup, WorkspaceID: "default", SessionID: sessionID,
		DeleteCleanupID: cleanupID, AttemptCount: 1, MaxAttempts: 5,
	})
	if err != nil {
		t.Fatalf("finalizeSessionDeleteCleanup: %v", err)
	}
	if result.Status != RuntimeDeliveryRejected || !result.Retryable || result.ErrorKind != "sandbox_release_retry_later" {
		t.Fatalf("cleanup result = %+v; want retryable unresolved provider creation", result)
	}
	var operations int
	if err := admin.QueryRow(`SELECT count(*) FROM sandbox_lifecycle_operations
		WHERE workspace_id='default' AND operation_id='sop_delete_unresolved_create'`).Scan(&operations); err != nil {
		t.Fatalf("count retained activation: %v", err)
	}
	if operations != 1 {
		t.Fatalf("retained activations = %d; want 1", operations)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreDeletedSessionSilentlyStalesOrdinaryJobs(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_bridge_deleted_job_gate"
	seedBridgeAPISession(t, admin, "default", sessionID, "thr_bridge_deleted_job_gate")
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET lifecycle_state='deleted' WHERE id=$1`, sessionID); err != nil {
		t.Fatalf("mark session deleted: %v", err)
	}
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	for _, job := range []RuntimeJob{
		{JobID: "qjob_deleted_input", LeaseToken: "lease_deleted_input", Kind: queue.KindRuntimeInput, WorkspaceID: "default", SessionID: sessionID, RuntimeInputID: "rin_deleted_input", InputKind: "messages"},
		{JobID: "qjob_deleted_config", LeaseToken: "lease_deleted_config", Kind: queue.KindRuntimeConfigUpdate, WorkspaceID: "default", SessionID: sessionID, RuntimeInputID: "config_deleted", ConfigGeneration: "2"},
		{JobID: "qjob_deleted_cleanup", LeaseToken: "lease_deleted_cleanup", Kind: queue.KindCleanupSession, WorkspaceID: "default", SessionID: sessionID, RuntimeInputID: "cleanup_session:cleanup_deleted", CleanupJobID: "cleanup_deleted"},
	} {
		plan, err := store.PrepareRuntimeCommand(context.Background(), job)
		if err != nil {
			t.Fatalf("PrepareRuntimeCommand %s: %v", job.Kind, err)
		}
		if !plan.StaleAccepted || plan.CleanupSession != nil {
			t.Fatalf("deleted %s plan = %#v; want silent stale ack", job.Kind, plan)
		}
	}
}

func TestPostgreSQLSessionDeleteRevokesPausedRuntimeWorkerBeforeDurableEffects(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_delete_paused_worker"
		threadID  = "thr_delete_paused_worker"
		inputID   = "rin_delete_paused_worker"
		eventID   = "evt_delete_paused_worker"
		bindingID = "bind_delete_paused_worker"
		podUID    = "pod_delete_paused_worker"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET status='idle' WHERE workspace_id='default' AND id=$1`, sessionID); err != nil {
		t.Fatalf("mark paused-worker Session idle: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_status
		SET status='idle', running_since=NULL, idle_since=clock_timestamp()
		WHERE workspace_id='default' AND session_id=$1`, sessionID); err != nil {
		t.Fatalf("mark paused-worker residency idle: %v", err)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 1, "user.message", `{"content":[{"type":"text","text":"delete before delivery"}]}`)
	job := RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: inputID, InputKind: "messages", EventIDs: []string{eventID},
		SequenceFrom: 1, SequenceTo: 1,
		PayloadJSON: `{"workspace_id":"default","session_id":"` + sessionID + `","session_thread_id":"` + threadID + `","runtime_input_id":"` + inputID + `","event_ids":["` + eventID + `"],"sequence_from":1,"sequence_to":1,"input_kind":"messages"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, job)
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtimeDB))
	queued, err := queueStore.Enqueue(context.Background(), queue.EnqueueRequest{
		WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput,
		PartitionKey:   queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID),
		DedupeKey:      queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, inputID),
		PayloadVersion: 1, PayloadJSON: []byte(job.PayloadJSON), MaxAttempts: queue.DefaultMaxAttempts,
	})
	if err != nil {
		t.Fatalf("enqueue paused Runtime worker: %v", err)
	}
	leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput},
		LeaseOwner: "delete-paused-worker", MaxJobs: 1, LeaseDuration: time.Minute,
	})
	if err != nil || len(leased) != 1 || leased[0].ID != queued.ID {
		t.Fatalf("lease paused Runtime worker = %#v/%v; want %s", leased, err, queued.ID)
	}
	leasedJob, err := DecodeRuntimeJob(queueJobProto(leased[0]))
	if err != nil {
		t.Fatalf("decode paused Runtime worker: %v", err)
	}

	workerEntered := make(chan struct{})
	releaseWorker := make(chan struct{})
	workerDone := make(chan RuntimeDeliveryResult, 1)
	sender := &recordingRuntimeCommandSender{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}
	deliverer := RuntimePodDirectDeliverer{
		Store:  NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtimeDB), 9090),
		Sender: sender,
	}
	go func() {
		close(workerEntered)
		<-releaseWorker
		result, _ := deliverer.DeliverRuntimeJob(context.Background(), leasedJob)
		workerDone <- result
	}()
	<-workerEntered
	var sessionStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM sessions WHERE workspace_id='default' AND id=$1`, sessionID).Scan(&sessionStatus); err != nil || sessionStatus != "idle" {
		t.Fatalf("Session at paused-worker cut = %s/%v; want idle", sessionStatus, err)
	}

	sessionStore := session.NewPostgreSQLSessionStore(
		dbconnect.NewClientForTesting(runtimeDB),
		session.WithSessionDeleteSandboxRelease(func(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, id string, now time.Time) error {
			_, _, err := tetralsandbox.EnsureSandboxReleaseTx(ctx, tx, string(ws), id, tetralsandbox.SandboxReleaseSessionDelete, "", now)
			return err
		}),
	)
	if err := sessionStore.WithRuntimeMutationTx(context.Background(), workspace.DefaultID, sessionID, func(tx session.Transaction) error {
		return tx.DeleteSession(context.Background(), sessionID)
	}); err != nil {
		t.Fatalf("delete Session while Runtime worker paused: %v", err)
	}
	close(releaseWorker)
	result := <-workerDone
	if result.Status != RuntimeDeliveryAuthorityLost && result.Status != RuntimeDeliveryDuplicate {
		t.Fatalf("resumed stale worker result = %#v; want authority lost or durable duplicate", result)
	}
	if len(sender.requests) != 0 {
		t.Fatalf("resumed stale worker sent %d Runtime requests; want zero", len(sender.requests))
	}
	var queueStatus, inboxStatus string
	var postDeleteFacts int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$1),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$2),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3
		 AND type IN ('span.model_request_start','span.model_request_end','agent.tool_use','agent.tool_result'))`,
		queued.ID, inputID, sessionID).Scan(&queueStatus, &inboxStatus, &postDeleteFacts); err != nil {
		t.Fatalf("read stale worker durable effects: %v", err)
	}
	if queueStatus != queue.StatusCancelled || inboxStatus != "cancelled" || postDeleteFacts != 0 {
		t.Fatalf("stale worker custody = Queue:%s Inbox:%s facts:%d; want cancelled/cancelled/0", queueStatus, inboxStatus, postDeleteFacts)
	}
}
