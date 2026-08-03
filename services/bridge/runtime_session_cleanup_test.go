package agentruntimebridge

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
)

// This file owns cleanup-session and delete-cleanup state-machine tests.

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
		pending, err = ensureSessionOutputCaptureCleanupTx(context.Background(), tx, "default", sessionID, now)
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
		pending, err = ensureSessionOutputCaptureCleanupTx(context.Background(), tx, "default", sessionID, now.Add(2*time.Second))
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
		pending, err = ensureSessionOutputCaptureCleanupTx(context.Background(), tx, "default", sessionID, now.Add(2*time.Minute))
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
			result, err = hasOpenSessionSandboxQueueJobsTx(context.Background(), tx, "default", sessionID)
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

func TestSessionDeleteCleanupCompletesAfterConsumedAttachmentGC(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_delete_consumed_attachment"
		threadID  = "thr_delete_consumed_attachment"
		cleanupID = "delcln_consumed_attachment"
		toolUseID = "evt_delete_consumed_attachment_tool"
		resultID  = "evt_delete_consumed_attachment_result"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_delete_consumed_attachment", 1, "pod_delete_consumed_attachment")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.AttachmentBlobStore = blob.NewFakeBlobStore()
	store.Clock = func() time.Time { return time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC) }
	attachment := createBridgeTransientAttachmentForTest(t, store,
		bridgeAPIScope(sessionID, threadID, "bind_delete_consumed_attachment", 1, "pod_delete_consumed_attachment"),
		"delete_consumed_attachment", toolUseID, []byte("consumed-attachment"))
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, resultID, 1, "agent.tool_result", `{"type":"agent.tool_result"}`)
	if _, err := admin.Exec(`INSERT INTO session_runtime_tool_results (
		workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
		normalized_input_hash, tool_name, input_json, ack_status, result_json,
		model_tool_call_id, execution_state, execution_attempt_generation, result_digest,
		consumed_by_terminal_event_id, consumption_reason, created_at, updated_at
	) VALUES ('default',$1,$2,$3,'sandbox_tool',$4,'view_image','{}','committed',NULL,
		'call_delete_consumed_attachment','consumed',1,$5,$6,'conversation_tool_result',$7,$7)`,
		sessionID, threadID, toolUseID, sha256Hex(`{}`), strings.Repeat("a", 64), resultID, store.Clock()); err != nil {
		t.Fatalf("seed consumed attachment execution: %v", err)
	}
	if _, err := admin.Exec(`UPDATE session_transient_attachments
		SET status='consumed', expires_at=$2
		WHERE workspace_id='default' AND attachment_ref=$1`, attachment.GetAttachmentRef(), store.Clock().Add(-time.Minute)); err != nil {
		t.Fatalf("mark attachment consumed: %v", err)
	}
	if result, err := store.ReconcileTransientAttachments(context.Background(), 10); err != nil || result.Deleted != 1 {
		t.Fatalf("reconcile consumed attachment = %+v, %v; want one deleted", result, err)
	}
	if got := bridgeTransientAttachmentStatus(t, admin, attachment.GetAttachmentRef()); got != "deleted" {
		t.Fatalf("consumed attachment status = %q; want deleted", got)
	}
	if _, err := admin.Exec(`UPDATE sessions SET lifecycle_state='deleted', delete_cleanup_id=$2
		WHERE workspace_id='default' AND id=$1`, sessionID, cleanupID); err != nil {
		t.Fatalf("mark Session deleted: %v", err)
	}
	if _, err := admin.Exec(`DELETE FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1`, sessionID); err != nil {
		t.Fatalf("remove runtime binding before deleted-session cleanup: %v", err)
	}
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	deliveryStore.AttachmentBlobStore = store.AttachmentBlobStore
	deliveryStore.Clock = func() time.Time { return store.Clock().Add(time.Minute) }
	result, err := deliveryStore.finalizeSessionDeleteCleanup(context.Background(), RuntimeJob{
		Kind: queue.KindSessionDeleteCleanup, WorkspaceID: "default", SessionID: sessionID,
		DeleteCleanupID: cleanupID, AttemptCount: 1, MaxAttempts: 5,
	})
	if err != nil {
		t.Fatalf("finalize Session delete cleanup: %v", err)
	}
	if result.Status != RuntimeDeliveryAccepted {
		t.Fatalf("Session delete cleanup = %+v; want accepted", result)
	}
	var attachmentRows, executionRows int
	if err := admin.QueryRow(`SELECT count(*) FROM session_transient_attachments WHERE workspace_id='default' AND session_id=$1`, sessionID).Scan(&attachmentRows); err != nil {
		t.Fatalf("count deleted attachment rows: %v", err)
	}
	if err := admin.QueryRow(`SELECT count(*) FROM session_runtime_tool_results WHERE workspace_id='default' AND session_id=$1`, sessionID).Scan(&executionRows); err != nil {
		t.Fatalf("count deleted execution rows: %v", err)
	}
	if attachmentRows != 0 || executionRows != 0 {
		t.Fatalf("Session delete cleanup retained attachment/execution rows = %d/%d", attachmentRows, executionRows)
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

func TestPostgreSQLRuntimeDeliveryStoreCleanupSessionRejectsNewInputBeforeClaim(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_cleanup_preclaim", "thr_bridge_cleanup_preclaim")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_cleanup_preclaim", "bind_bridge_cleanup_preclaim", 7, "pod_uid_cleanup_preclaim")

	bridgeStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	bridgeStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 45, 0, time.UTC) }
	if _, err := finishIdleWithStagedCaptureForTest(t, admin, bridgeStore, bridgeAPIFinishIdleRequest(
		t,
		admin,
		bridgeAPIScope("sesn_bridge_cleanup_preclaim", "thr_bridge_cleanup_preclaim", "bind_bridge_cleanup_preclaim", 7, "pod_uid_cleanup_preclaim"),
		"evt_bridge_cleanup_preclaim_running",
		`{"type":"end_turn"}`,
	)); err != nil {
		t.Fatalf("FinishIdle: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_runtime_status
		    SET cleanup_job_id = 'cleanup_bridge_preclaim_1',
		        cleanup_enqueued_at = '2026-01-01T00:30:00Z'
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_cleanup_preclaim'`); err != nil {
		t.Fatalf("mark cleanup enqueued: %v", err)
	}
	seedBridgeAPIUserMessageEvent(
		t,
		admin,
		"default",
		"sesn_bridge_cleanup_preclaim",
		"thr_bridge_cleanup_preclaim",
		"sevt_cleanup_before_claim",
		nextBridgeAPIEventSequenceForTest(t, admin, "sesn_bridge_cleanup_preclaim", "thr_bridge_cleanup_preclaim"),
	)

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 31, 0, 0, time.UTC) }
	plan, err := store.PrepareRuntimeCommand(context.Background(), RuntimeJob{ //nolint:gosec // Test lease token fixture, not a secret.
		JobID:          "qjob_cleanup_bridge_preclaim",
		LeaseToken:     "lease_cleanup_bridge_preclaim",
		Kind:           queue.KindCleanupSession,
		WorkspaceID:    "default",
		SessionID:      "sesn_bridge_cleanup_preclaim",
		RuntimeInputID: "cleanup_session:cleanup_bridge_preclaim_1",
		CleanupJobID:   "cleanup_bridge_preclaim_1",
		CommandKind:    agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_CLEANUP_SESSION,
		PayloadJSON:    `{"workspace_id":"default","session_id":"sesn_bridge_cleanup_preclaim","cleanup_job_id":"cleanup_bridge_preclaim_1"}`,
	})
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand cleanup preclaim: %v", err)
	}
	if !plan.StaleAccepted || plan.Request != nil {
		t.Fatalf("cleanup preclaim plan = %#v; want stale with no Runtime command", plan)
	}
	var claimedAt sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT cleanup_claimed_at
		   FROM session_runtime_status
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_cleanup_preclaim'`).Scan(&claimedAt); err != nil {
		t.Fatalf("read claimed_at: %v", err)
	}
	if claimedAt.Valid {
		t.Fatalf("cleanup preclaim wrote cleanup_claimed_at = %v; want null", claimedAt)
	}
	var processedAt sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT processed_at
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND event_id = 'sevt_cleanup_before_claim'`).Scan(&processedAt); err != nil {
		t.Fatalf("read preclaim input: %v", err)
	}
	if processedAt.Valid {
		t.Fatalf("preclaim input processed_at = %v; want still queued for next run", processedAt)
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
		if !plan.StaleAccepted || plan.Request != nil {
			t.Fatalf("deleted %s plan = %#v; want silent stale ack", job.Kind, plan)
		}
	}
}

func TestPostgreSQLRuntimeDeliveryStoreCleanupSessionKeepsResolvingConfirmationAfterClaim(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_cleanup_confirm", "thr_bridge_cleanup_confirm_main")
	seedBridgeAPIChildThread(t, admin, "default", "sesn_bridge_cleanup_confirm", "thr_bridge_cleanup_confirm_main", "thr_bridge_cleanup_confirm_child")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_cleanup_confirm", "bind_bridge_cleanup_confirm", 7, "pod_uid_cleanup_confirm")
	seedBridgeAPIPendingApproval(t, admin, "default", "sesn_bridge_cleanup_confirm", "thr_bridge_cleanup_confirm_child", "sevt_cleanup_confirm_wait", 1)

	bridgeStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	bridgeStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 45, 0, time.UTC) }
	if _, err := finishIdleWithStagedCaptureForTest(t, admin, bridgeStore, bridgeAPIFinishIdleRequest(
		t,
		admin,
		bridgeAPIScope("sesn_bridge_cleanup_confirm", "thr_bridge_cleanup_confirm_main", "bind_bridge_cleanup_confirm", 7, "pod_uid_cleanup_confirm"),
		"evt_bridge_cleanup_confirm_running",
		`{"type":"end_turn"}`,
	)); err != nil {
		t.Fatalf("FinishIdle: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_runtime_status
		    SET cleanup_job_id = 'cleanup_bridge_confirm_1',
		        cleanup_enqueued_at = '2026-01-01T00:30:00Z'
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_cleanup_confirm'`); err != nil {
		t.Fatalf("mark cleanup enqueued: %v", err)
	}

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 31, 0, 0, time.UTC) }
	job := RuntimeJob{ //nolint:gosec // Test lease token fixture, not a secret.
		JobID:          "qjob_cleanup_bridge_confirm",
		LeaseToken:     "lease_cleanup_bridge_confirm",
		Kind:           queue.KindCleanupSession,
		WorkspaceID:    "default",
		SessionID:      "sesn_bridge_cleanup_confirm",
		RuntimeInputID: "cleanup_session:cleanup_bridge_confirm_1",
		CleanupJobID:   "cleanup_bridge_confirm_1",
		CommandKind:    agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_CLEANUP_SESSION,
		PayloadJSON:    `{"workspace_id":"default","session_id":"sesn_bridge_cleanup_confirm","cleanup_job_id":"cleanup_bridge_confirm_1"}`,
	}
	plan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand cleanup confirmation: %v", err)
	}
	if plan.StaleAccepted || plan.Request == nil {
		t.Fatalf("cleanup confirmation plan = %#v; want claimed Runtime command", plan)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_pending_tool_uses
		    SET status = 'resolving',
		        decision = 'allow',
		        updated_at = '2026-01-01T00:31:05Z'
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_cleanup_confirm'
		    AND session_thread_id = 'thr_bridge_cleanup_confirm_child'
		    AND tool_use_event_id = 'sevt_cleanup_confirm_wait'`); err != nil {
		t.Fatalf("mark pending approval resolving: %v", err)
	}
	seedBridgeAPIToolConfirmationEvent(t, admin, "default", "sesn_bridge_cleanup_confirm", "thr_bridge_cleanup_confirm_child", "sevt_cleanup_confirm_allow", 2, "sevt_cleanup_confirm_wait", "allow")

	result, err := store.FinalizeRuntimeCleanup(context.Background(), job)
	if err != nil {
		t.Fatalf("FinalizeRuntimeCleanup confirmation: %v", err)
	}
	if result.Status != RuntimeDeliveryAccepted {
		t.Fatalf("cleanup confirmation result = %#v; want accepted", result)
	}
	var pendingStatus string
	var decision sql.NullString
	var resultEventID sql.NullString
	var resolvedAt sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status, decision, result_event_id, resolved_at
		   FROM session_pending_tool_uses
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_cleanup_confirm'
		    AND tool_use_event_id = 'sevt_cleanup_confirm_wait'`).Scan(&pendingStatus, &decision, &resultEventID, &resolvedAt); err != nil {
		t.Fatalf("read resolving pending approval: %v", err)
	}
	if pendingStatus != "resolving" || !decision.Valid || decision.String != "allow" || resultEventID.Valid || resolvedAt.Valid {
		t.Fatalf("pending approval after cleanup = status %q decision %v result %v resolved %v; want resolving allow without cleanup result", pendingStatus, decision, resultEventID, resolvedAt)
	}
	var confirmationProcessedAt sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT processed_at
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND event_id = 'sevt_cleanup_confirm_allow'`).Scan(&confirmationProcessedAt); err != nil {
		t.Fatalf("read queued confirmation: %v", err)
	}
	if confirmationProcessedAt.Valid {
		t.Fatalf("post-claim confirmation processed_at = %v; want queued for next run", confirmationProcessedAt)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreCleanupSessionIgnoresPreIdleUnprocessedInputByStreamFence(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_cleanup_preidle", "thr_bridge_cleanup_preidle_main")
	seedBridgeAPIChildThread(t, admin, "default", "sesn_bridge_cleanup_preidle", "thr_bridge_cleanup_preidle_main", "thr_bridge_cleanup_preidle_child")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_cleanup_preidle", "bind_bridge_cleanup_preidle", 7, "pod_uid_cleanup_preidle")
	seedBridgeAPIUserMessageEvent(t, admin, "default", "sesn_bridge_cleanup_preidle", "thr_bridge_cleanup_preidle_child", "sevt_cleanup_preidle_superseded", 99)

	bridgeStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	bridgeStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 45, 0, time.UTC) }
	if _, err := finishIdleWithStagedCaptureForTest(t, admin, bridgeStore, bridgeAPIFinishIdleRequest(
		t,
		admin,
		bridgeAPIScope("sesn_bridge_cleanup_preidle", "thr_bridge_cleanup_preidle_main", "bind_bridge_cleanup_preidle", 7, "pod_uid_cleanup_preidle"),
		"evt_bridge_cleanup_preidle_running",
		`{"type":"end_turn"}`,
	)); err != nil {
		t.Fatalf("FinishIdle: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_runtime_status
		    SET cleanup_job_id = 'cleanup_bridge_preidle_1',
		        cleanup_enqueued_at = '2026-01-01T00:30:00Z'
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_cleanup_preidle'`); err != nil {
		t.Fatalf("mark cleanup enqueued: %v", err)
	}

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 31, 0, 0, time.UTC) }
	plan, err := store.PrepareRuntimeCommand(context.Background(), RuntimeJob{ //nolint:gosec // Test lease token fixture, not a secret.
		JobID:          "qjob_cleanup_bridge_preidle",
		LeaseToken:     "lease_cleanup_bridge_preidle",
		Kind:           queue.KindCleanupSession,
		WorkspaceID:    "default",
		SessionID:      "sesn_bridge_cleanup_preidle",
		RuntimeInputID: "cleanup_session:cleanup_bridge_preidle_1",
		CleanupJobID:   "cleanup_bridge_preidle_1",
		CommandKind:    agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_CLEANUP_SESSION,
		PayloadJSON:    `{"workspace_id":"default","session_id":"sesn_bridge_cleanup_preidle","cleanup_job_id":"cleanup_bridge_preidle_1"}`,
	})
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand cleanup preidle: %v", err)
	}
	if plan.StaleAccepted || plan.Request == nil {
		t.Fatalf("cleanup preidle plan = %#v; want claim despite pre-idle unprocessed child input", plan)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreCleanupSessionRejectsPostIdleChildInputByStreamFence(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_cleanup_child_postidle", "thr_bridge_cleanup_child_postidle_main")
	seedBridgeAPIChildThread(t, admin, "default", "sesn_bridge_cleanup_child_postidle", "thr_bridge_cleanup_child_postidle_main", "thr_bridge_cleanup_child_postidle_child")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_cleanup_child_postidle", "bind_bridge_cleanup_child_postidle", 7, "pod_uid_cleanup_child_postidle")

	bridgeStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	bridgeStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 45, 0, time.UTC) }
	if _, err := finishIdleWithStagedCaptureForTest(t, admin, bridgeStore, bridgeAPIFinishIdleRequest(
		t,
		admin,
		bridgeAPIScope("sesn_bridge_cleanup_child_postidle", "thr_bridge_cleanup_child_postidle_main", "bind_bridge_cleanup_child_postidle", 7, "pod_uid_cleanup_child_postidle"),
		"evt_bridge_cleanup_child_postidle_running",
		`{"type":"end_turn"}`,
	)); err != nil {
		t.Fatalf("FinishIdle: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_runtime_status
		    SET cleanup_job_id = 'cleanup_bridge_child_postidle_1',
		        cleanup_enqueued_at = '2026-01-01T00:30:00Z'
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_cleanup_child_postidle'`); err != nil {
		t.Fatalf("mark cleanup enqueued: %v", err)
	}
	seedBridgeAPIUserMessageEvent(t, admin, "default", "sesn_bridge_cleanup_child_postidle", "thr_bridge_cleanup_child_postidle_child", "sevt_cleanup_child_postidle_message", 1)

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 31, 0, 0, time.UTC) }
	plan, err := store.PrepareRuntimeCommand(context.Background(), RuntimeJob{ //nolint:gosec // Test lease token fixture, not a secret.
		JobID:          "qjob_cleanup_bridge_child_postidle",
		LeaseToken:     "lease_cleanup_bridge_child_postidle",
		Kind:           queue.KindCleanupSession,
		WorkspaceID:    "default",
		SessionID:      "sesn_bridge_cleanup_child_postidle",
		RuntimeInputID: "cleanup_session:cleanup_bridge_child_postidle_1",
		CleanupJobID:   "cleanup_bridge_child_postidle_1",
		CommandKind:    agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_CLEANUP_SESSION,
		PayloadJSON:    `{"workspace_id":"default","session_id":"sesn_bridge_cleanup_child_postidle","cleanup_job_id":"cleanup_bridge_child_postidle_1"}`,
	})
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand cleanup child post-idle: %v", err)
	}
	if !plan.StaleAccepted || plan.Request != nil {
		t.Fatalf("cleanup child post-idle plan = %#v; want stale without Runtime command", plan)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreCleanupSessionReschedulesWhileChildRuns(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_cleanup_tree_claim"
		mainID    = "thr_bridge_cleanup_tree_claim_main"
		childID   = "thr_bridge_cleanup_tree_claim_child"
		cleanupID = "cleanup_bridge_tree_claim_1"
	)
	seedBridgeCleanupTreeFixture(t, runtime, admin, sessionID, mainID, childID, cleanupID, false)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_threads SET status = 'running' WHERE workspace_id = 'default' AND session_id = $1 AND id = $2`,
		sessionID, childID); err != nil {
		t.Fatalf("mark child running: %v", err)
	}

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 31, 0, 0, time.UTC) }
	plan, err := store.PrepareRuntimeCommand(context.Background(), cleanupTreeJob(sessionID, cleanupID))
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand with running child: %v", err)
	}
	if !plan.StaleAccepted || plan.Request != nil {
		t.Fatalf("cleanup plan with running child = %#v; want benign skip", plan)
	}
	assertBridgeCleanupTreeRescheduled(t, admin, sessionID, "2026-01-01T01:01:00Z")

	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_threads SET status = 'idle' WHERE workspace_id = 'default' AND session_id = $1 AND id = $2`,
		sessionID, childID); err != nil {
		t.Fatalf("mark child idle: %v", err)
	}
	const nextCleanupID = "cleanup_bridge_tree_claim_2"
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_runtime_status
		    SET cleanup_job_id = $2,
		        cleanup_enqueued_at = '2026-01-01T01:02:00Z'
		  WHERE workspace_id = 'default' AND session_id = $1`,
		sessionID, nextCleanupID); err != nil {
		t.Fatalf("enqueue next cleanup: %v", err)
	}
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 1, 2, 0, 0, time.UTC) }
	plan, err = store.PrepareRuntimeCommand(context.Background(), cleanupTreeJob(sessionID, nextCleanupID))
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand after child idle: %v", err)
	}
	if plan.StaleAccepted || plan.Request == nil {
		t.Fatalf("cleanup plan after child idle = %#v; want claimed Runtime command", plan)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreCleanupSessionReschedulesWhenChildStartsBeforeFinalize(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_cleanup_tree_finalize"
		mainID    = "thr_bridge_cleanup_tree_finalize_main"
		childID   = "thr_bridge_cleanup_tree_finalize_child"
		cleanupID = "cleanup_bridge_tree_finalize_1"
	)
	seedBridgeCleanupTreeFixture(t, runtime, admin, sessionID, mainID, childID, cleanupID, false)
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 31, 0, 0, time.UTC) }
	job := cleanupTreeJob(sessionID, cleanupID)
	plan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand before child starts: %v", err)
	}
	if plan.StaleAccepted || plan.Request == nil {
		t.Fatalf("cleanup claim before child starts = %#v; want Runtime command", plan)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_threads SET status = 'running' WHERE workspace_id = 'default' AND session_id = $1 AND id = $2`,
		sessionID, childID); err != nil {
		t.Fatalf("mark child running before finalize: %v", err)
	}

	result, err := store.FinalizeRuntimeCleanup(context.Background(), job)
	if err != nil {
		t.Fatalf("FinalizeRuntimeCleanup with running child: %v", err)
	}
	if result.Status != RuntimeDeliveryDuplicate {
		t.Fatalf("finalize result with running child = %#v; want rescheduled duplicate", result)
	}
	assertBridgeCleanupTreeRescheduled(t, admin, sessionID, "2026-01-01T01:01:00Z")
	var bindingCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_runtime_bindings WHERE workspace_id = 'default' AND session_id = $1`,
		sessionID).Scan(&bindingCount); err != nil {
		t.Fatalf("count retained binding: %v", err)
	}
	if bindingCount != 1 {
		t.Fatalf("binding rows after busy finalize = %d; want 1", bindingCount)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreCleanupSessionTreeFenceClassifiesQuiescentAndBusyThreads(t *testing.T) {
	t.Run("requires action remains quiescent", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		const (
			sessionID = "sesn_bridge_cleanup_tree_requires_action"
			mainID    = "thr_bridge_cleanup_tree_requires_action_main"
			childID   = "thr_bridge_cleanup_tree_requires_action_child"
			cleanupID = "cleanup_bridge_tree_requires_action_1"
		)
		seedBridgeCleanupTreeFixture(t, runtime, admin, sessionID, mainID, childID, cleanupID, false)
		if _, err := admin.ExecContext(context.Background(),
			`UPDATE session_threads SET status = 'requires_action' WHERE workspace_id = 'default' AND session_id = $1 AND id = $2`,
			sessionID, childID); err != nil {
			t.Fatalf("mark child requires_action: %v", err)
		}
		store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
		store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 31, 0, 0, time.UTC) }
		plan, err := store.PrepareRuntimeCommand(context.Background(), cleanupTreeJob(sessionID, cleanupID))
		if err != nil {
			t.Fatalf("PrepareRuntimeCommand with requires_action child: %v", err)
		}
		if plan.StaleAccepted || plan.Request == nil {
			t.Fatalf("cleanup plan with requires_action child = %#v; want claimed Runtime command", plan)
		}
	})

	t.Run("post-idle confirmation remains a wake fence", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		const (
			sessionID = "sesn_bridge_cleanup_tree_confirmation"
			mainID    = "thr_bridge_cleanup_tree_confirmation_main"
			childID   = "thr_bridge_cleanup_tree_confirmation_child"
			cleanupID = "cleanup_bridge_tree_confirmation_1"
		)
		seedBridgeCleanupTreeFixture(t, runtime, admin, sessionID, mainID, childID, cleanupID, false)
		if _, err := admin.ExecContext(context.Background(),
			`UPDATE session_threads SET status = 'requires_action' WHERE workspace_id = 'default' AND session_id = $1 AND id = $2`,
			sessionID, childID); err != nil {
			t.Fatalf("mark child requires_action: %v", err)
		}
		seedBridgeAPIToolConfirmationEvent(t, admin, "default", sessionID, childID, "sevt_cleanup_tree_confirmation", 1, "sevt_cleanup_tree_wait", "allow")
		store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
		store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 31, 0, 0, time.UTC) }
		plan, err := store.PrepareRuntimeCommand(context.Background(), cleanupTreeJob(sessionID, cleanupID))
		if err != nil {
			t.Fatalf("PrepareRuntimeCommand with post-idle confirmation: %v", err)
		}
		if !plan.StaleAccepted || plan.Request != nil {
			t.Fatalf("cleanup plan with post-idle confirmation = %#v; want wake-fenced skip", plan)
		}
	})

	t.Run("running reviewer is busy", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		const (
			sessionID = "sesn_bridge_cleanup_tree_reviewer"
			mainID    = "thr_bridge_cleanup_tree_reviewer_main"
			reviewer  = "thr_bridge_cleanup_tree_reviewer"
			cleanupID = "cleanup_bridge_tree_reviewer_1"
		)
		seedBridgeCleanupTreeFixture(t, runtime, admin, sessionID, mainID, "", cleanupID, false)
		seedBridgeAPIInternalReviewerThread(t, admin, "default", sessionID, mainID, reviewer)
		if _, err := admin.ExecContext(context.Background(),
			`UPDATE session_threads SET status = 'running' WHERE workspace_id = 'default' AND session_id = $1 AND id = $2`,
			sessionID, reviewer); err != nil {
			t.Fatalf("mark reviewer running: %v", err)
		}
		store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
		store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 31, 0, 0, time.UTC) }
		plan, err := store.PrepareRuntimeCommand(context.Background(), cleanupTreeJob(sessionID, cleanupID))
		if err != nil {
			t.Fatalf("PrepareRuntimeCommand with running reviewer: %v", err)
		}
		if !plan.StaleAccepted || plan.Request != nil {
			t.Fatalf("cleanup plan with running reviewer = %#v; want role-blind busy skip", plan)
		}
		assertBridgeCleanupTreeRescheduled(t, admin, sessionID, "2026-01-01T01:01:00Z")
	})
}

func TestPostgreSQLRuntimeDeliveryStoreCleanupSessionFinalizesWhenRuntimePodProvenGone(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_cleanup_gone", "thr_bridge_cleanup_gone")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_cleanup_gone", "bind_bridge_cleanup_gone", 7, "pod_uid_cleanup_gone")
	seedBridgeAPIPendingApproval(t, admin, "default", "sesn_bridge_cleanup_gone", "thr_bridge_cleanup_gone", "sevt_cleanup_gone_wait", 1)

	bridgeStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	bridgeStore.AttachmentBlobStore = blob.NewFakeBlobStore()
	bridgeStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 45, 0, time.UTC) }
	attachment := createBridgeTransientAttachmentForTest(
		t, bridgeStore,
		bridgeAPIScope("sesn_bridge_cleanup_gone", "thr_bridge_cleanup_gone", "bind_bridge_cleanup_gone", 7, "pod_uid_cleanup_gone"),
		"attachment_cleanup_gone", "sevt_cleanup_gone_wait", []byte("cleanup-wait-attachment"),
	)
	resultJSON := `{"status":"success","attachment_ref":"` + attachment.GetAttachmentRef() + `"}`
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_transient_attachments
		SET status='staged', expires_at='2026-01-01T00:01:00Z'
		WHERE workspace_id='default' AND attachment_ref=$1`, attachment.GetAttachmentRef()); err != nil {
		t.Fatalf("stage cleanup-wait attachment: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_tool_results (
		workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
		normalized_input_hash, tool_name, input_json, ack_status, result_json,
		model_tool_call_id, execution_state, execution_attempt_generation, result_digest,
		created_at, updated_at
	) VALUES ('default','sesn_bridge_cleanup_gone','thr_bridge_cleanup_gone','sevt_cleanup_gone_wait','sandbox_tool',
		$1,'dangerous_tool','{}','committed',$2,
		'toolu_cleanup_wait','terminal_unconsumed',1,$3,
		'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`,
		sha256Hex(`{}`), resultJSON, sha256Hex(resultJSON)); err != nil {
		t.Fatalf("seed cleanup-wait sandbox execution: %v", err)
	}
	if _, err := finishIdleWithStagedCaptureForTest(t, admin, bridgeStore, bridgeAPIFinishIdleRequest(
		t,
		admin,
		bridgeAPIScope("sesn_bridge_cleanup_gone", "thr_bridge_cleanup_gone", "bind_bridge_cleanup_gone", 7, "pod_uid_cleanup_gone"),
		"evt_bridge_cleanup_gone_running",
		`{"type":"end_turn"}`,
	)); err != nil {
		t.Fatalf("FinishIdle: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_runtime_status
		    SET cleanup_job_id = 'cleanup_bridge_gone_1',
		        cleanup_enqueued_at = '2026-01-01T00:30:00Z'
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_cleanup_gone'`); err != nil {
		t.Fatalf("mark cleanup enqueued: %v", err)
	}

	sender := &recordingRuntimeCommandSender{response: &agentruntimev1.RuntimeInputCommandResponse{
		Status: agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED,
	}}
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 31, 0, 0, time.UTC) }
	store.TargetResolver = KubernetesRuntimeTargetResolver{
		Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
			return enginekubernetes.NewBindingVisibilitySnapshotStateForTest(true, enginekubernetes.BoundRuntimePod{
				Namespace: "tetral-agent-runtime",
				PodName:   "runtime-pod-0",
				PodUID:    "pod_uid_cleanup_gone",
				PodIP:     "10.0.0.10",
			}, enginekubernetes.BindingVisibilityAbsent)
		},
	}
	job := RuntimeJob{ //nolint:gosec // Test lease token fixture, not a secret.
		JobID:          "qjob_cleanup_bridge_gone",
		LeaseToken:     "lease_cleanup_bridge_gone",
		Kind:           queue.KindCleanupSession,
		WorkspaceID:    "default",
		SessionID:      "sesn_bridge_cleanup_gone",
		RuntimeInputID: "cleanup_session:cleanup_bridge_gone_1",
		CleanupJobID:   "cleanup_bridge_gone_1",
		CommandKind:    agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_CLEANUP_SESSION,
		PayloadJSON:    `{"workspace_id":"default","session_id":"sesn_bridge_cleanup_gone","cleanup_job_id":"cleanup_bridge_gone_1"}`,
	}
	result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
	if err != nil {
		t.Fatalf("DeliverRuntimeJob cleanup gone: %v", err)
	}
	if result.Status != RuntimeDeliveryAccepted {
		t.Fatalf("cleanup gone result = %#v; want accepted", result)
	}
	if len(sender.requests) != 0 {
		t.Fatalf("runtime cleanup commands sent = %d; want 0 when pod is proven gone", len(sender.requests))
	}
	var bindingRows int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_runtime_bindings
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_cleanup_gone'`).Scan(&bindingRows); err != nil {
		t.Fatalf("read binding rows: %v", err)
	}
	if bindingRows != 0 {
		t.Fatalf("binding rows after gone cleanup = %d; want 0", bindingRows)
	}
	var cleanupAfter sql.NullString
	var cleanupJobID sql.NullString
	var finalizedBindingID sql.NullString
	var finalizedGeneration sql.NullInt64
	if err := admin.QueryRowContext(context.Background(),
		`SELECT cleanup_after, cleanup_job_id, binding_id, binding_generation
		   FROM session_runtime_status
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_cleanup_gone'`).Scan(&cleanupAfter, &cleanupJobID, &finalizedBindingID, &finalizedGeneration); err != nil {
		t.Fatalf("read finalized runtime status: %v", err)
	}
	if cleanupAfter.Valid || cleanupJobID.Valid || finalizedBindingID.Valid || finalizedGeneration.Valid {
		t.Fatalf("gone finalized runtime status cleanup/binding markers = %v/%v/%v/%v; want all null", cleanupAfter, cleanupJobID, finalizedBindingID, finalizedGeneration)
	}
	var pendingStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status
		   FROM session_pending_tool_uses
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_cleanup_gone'
		    AND tool_use_event_id = 'sevt_cleanup_gone_wait'`).Scan(&pendingStatus); err != nil {
		t.Fatalf("read pending wait: %v", err)
	}
	if pendingStatus != "expired" {
		t.Fatalf("gone cleanup pending wait status = %q; want expired", pendingStatus)
	}
	var executionState, consumptionReason string
	var storedResult sql.NullString
	if err := admin.QueryRowContext(context.Background(), `SELECT execution_state, result_json, consumption_reason
		FROM session_runtime_tool_results
		WHERE workspace_id='default' AND session_id='sesn_bridge_cleanup_gone' AND tool_use_event_id='sevt_cleanup_gone_wait'`).Scan(
		&executionState, &storedResult, &consumptionReason,
	); err != nil {
		t.Fatalf("read cleanup-wait execution receipt: %v", err)
	}
	if executionState != "consumed" || storedResult.Valid || consumptionReason != "cleanup_wait_expired" {
		t.Fatalf("cleanup-wait execution = %q/%v/%q; want consumed thin receipt", executionState, storedResult, consumptionReason)
	}
	bridgeStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 31, 0, 0, time.UTC) }
	if result, err := bridgeStore.ReconcileTransientAttachments(context.Background(), 10); err != nil || result.Deleted != 1 {
		t.Fatalf("reconcile cleanup-wait attachment = %+v, %v; want one deleted", result, err)
	}
	if got := bridgeTransientAttachmentStatus(t, admin, attachment.GetAttachmentRef()); got != "deleted" {
		t.Fatalf("cleanup-wait attachment status = %q; want deleted", got)
	}
}

func seedBridgeCleanupTreeFixture(t *testing.T, runtime *sql.DB, admin *sql.DB, sessionID string, mainThreadID string, childThreadID string, cleanupID string, reviewer bool) {
	t.Helper()
	seedBridgeAPISession(t, admin, "default", sessionID, mainThreadID)
	if childThreadID != "" {
		if reviewer {
			seedBridgeAPIInternalReviewerThread(t, admin, "default", sessionID, mainThreadID, childThreadID)
		} else {
			seedBridgeAPIChildThread(t, admin, "default", sessionID, mainThreadID, childThreadID)
		}
	}
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_"+sessionID, 7, "pod_uid_"+sessionID)
	bridgeStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	bridgeStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 45, 0, time.UTC) }
	if _, err := finishIdleWithStagedCaptureForTest(t, admin, bridgeStore, bridgeAPIFinishIdleRequest(
		t,
		admin,
		bridgeAPIScope(sessionID, mainThreadID, "bind_"+sessionID, 7, "pod_uid_"+sessionID),
		"evt_"+sessionID+"_running",
		`{"type":"end_turn"}`,
	)); err != nil {
		t.Fatalf("FinishIdle cleanup tree fixture: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_runtime_status
		    SET cleanup_job_id = $2,
		        cleanup_enqueued_at = '2026-01-01T00:30:00Z'
		  WHERE workspace_id = 'default' AND session_id = $1`,
		sessionID, cleanupID); err != nil {
		t.Fatalf("mark cleanup tree fixture enqueued: %v", err)
	}
}

func cleanupTreeJob(sessionID string, cleanupID string) RuntimeJob {
	return RuntimeJob{ //nolint:gosec // Test lease token fixture, not a secret.
		JobID:          "qjob_" + cleanupID,
		LeaseToken:     "lease_" + cleanupID,
		Kind:           queue.KindCleanupSession,
		WorkspaceID:    "default",
		SessionID:      sessionID,
		RuntimeInputID: "cleanup_session:" + cleanupID,
		CleanupJobID:   cleanupID,
		CommandKind:    agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_CLEANUP_SESSION,
		PayloadJSON:    `{"workspace_id":"default","session_id":"` + sessionID + `","cleanup_job_id":"` + cleanupID + `"}`,
	}
}

func assertBridgeCleanupTreeRescheduled(t *testing.T, admin *sql.DB, sessionID string, wantCleanupAfter string) {
	t.Helper()
	var cleanupAfter string
	var cleanupJobID, cleanupClaimedAt, cleanupEnqueuedAt sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT cleanup_after, cleanup_job_id, cleanup_claimed_at, cleanup_enqueued_at
		   FROM session_runtime_status
		  WHERE workspace_id = 'default' AND session_id = $1`,
		sessionID).Scan(&cleanupAfter, &cleanupJobID, &cleanupClaimedAt, &cleanupEnqueuedAt); err != nil {
		t.Fatalf("read rescheduled cleanup tree state: %v", err)
	}
	if cleanupAfter != wantCleanupAfter || cleanupJobID.Valid || cleanupClaimedAt.Valid || cleanupEnqueuedAt.Valid {
		t.Fatalf("rescheduled cleanup state = after %q markers %v/%v/%v; want %q and all null",
			cleanupAfter, cleanupJobID, cleanupClaimedAt, cleanupEnqueuedAt, wantCleanupAfter)
	}
}
