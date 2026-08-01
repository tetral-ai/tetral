package agentruntimebridge

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
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

func TestPostgreSQLRuntimeDeliveryStoreCleanupSessionRejectsNewInputBeforeClaim(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_cleanup_preclaim", "thr_bridge_cleanup_preclaim")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_cleanup_preclaim", "bind_bridge_cleanup_preclaim", 7, "pod_uid_cleanup_preclaim")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_cleanup_preclaim", "prep_bridge_cleanup_preclaim")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_cleanup_preclaim", "2026-01-01T00:00:00Z")

	bridgeStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	bridgeStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 45, 0, time.UTC) }
	if _, err := bridgeStore.FinishIdle(context.Background(), bridgeAPIFinishIdleRequest(
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

func TestPostgreSQLRuntimeDeliveryStoreCleanupActiveSessionClaimsSettlesReleasesAndFinalizes(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_cleanup", "thr_bridge_cleanup")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_cleanup", "bind_bridge_cleanup", 7, "pod_uid_cleanup")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_cleanup", "prep_bridge_cleanup")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_cleanup", "2026-01-01T00:00:00Z")
	seedBridgeAPIBackgroundTask(t, admin, "default", "sesn_bridge_cleanup", "thr_bridge_cleanup", "bind_bridge_cleanup", "task_cleanup", "sevt_cleanup_tool")
	seedBridgeAPIPendingApproval(t, admin, "default", "sesn_bridge_cleanup", "thr_bridge_cleanup", "sevt_cleanup_wait", 1)
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_cleanup", "thr_bridge_cleanup", "sevt_cleanup_execution", 2, "agent.tool_use", `{"name":"exec_command","input":{"cmd":"true"},"evaluated_permission":"allow"}`)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_events SET model_request_id = 'mreq_cleanup_execution'
		  WHERE workspace_id = 'default' AND event_id = 'sevt_cleanup_execution'`); err != nil {
		t.Fatalf("stamp cleanup execution model request: %v", err)
	}
	seedBridgeAPIDurableToolMessage(t, admin, "default", "sesn_bridge_cleanup", "thr_bridge_cleanup", "mreq_cleanup_execution", "sevt_cleanup_execution", "call_cleanup_execution", "exec_command")
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_runtime_tool_results (
			workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
			normalized_input_hash, tool_name, input_json, ack_status, model_tool_call_id,
			execution_state, execution_attempt_generation, created_at, updated_at
		) VALUES (
			'default', 'sesn_bridge_cleanup', 'thr_bridge_cleanup', 'sevt_cleanup_execution', 'sandbox_tool',
			'input_hash_cleanup_execution', 'exec_command', '{"cmd":"true"}', 'committed', 'call_cleanup_execution',
			'pending', 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		)`); err != nil {
		t.Fatalf("seed cleanup Sandbox execution: %v", err)
	}
	seedBridgeAPIFileSessionResource(t, admin, "default", "sesn_bridge_cleanup", "sesrsc_cleanup_file")

	bridgeStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	bridgeStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 45, 0, time.UTC) }
	if _, err := bridgeStore.FinishIdle(context.Background(), bridgeAPIFinishIdleRequest(
		t,
		admin,
		bridgeAPIScope("sesn_bridge_cleanup", "thr_bridge_cleanup", "bind_bridge_cleanup", 7, "pod_uid_cleanup"),
		"evt_bridge_cleanup_running",
		`{"type":"end_turn"}`,
	)); err != nil {
		t.Fatalf("FinishIdle: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_runtime_status
		    SET cleanup_job_id = 'cleanup_bridge_1',
		        cleanup_enqueued_at = '2026-01-01T00:30:00Z'
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_cleanup'`); err != nil {
		t.Fatalf("mark cleanup enqueued: %v", err)
	}
	releaser := &recordingSandboxReleaseClient{result: SandboxReleaseResult{Status: SandboxReleaseReleased, SandboxStatus: "released"}}
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 31, 0, 0, time.UTC) }
	store.SandboxReleaser = releaser
	job := RuntimeJob{ //nolint:gosec // Test lease token fixture, not a secret.
		JobID:          "qjob_cleanup_bridge",
		LeaseToken:     "lease_cleanup_bridge",
		Kind:           queue.KindCleanupSession,
		WorkspaceID:    "default",
		SessionID:      "sesn_bridge_cleanup",
		RuntimeInputID: "cleanup_session:cleanup_bridge_1",
		CleanupJobID:   "cleanup_bridge_1",
		CommandKind:    agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_CLEANUP_SESSION,
		PayloadJSON:    `{"workspace_id":"default","session_id":"sesn_bridge_cleanup","cleanup_job_id":"cleanup_bridge_1"}`,
	}
	plan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand cleanup: %v", err)
	}
	if plan.StaleAccepted || plan.Request == nil || plan.Request.GetCommandKind() != agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_CLEANUP_SESSION {
		t.Fatalf("cleanup plan = %#v; want runtime cleanup command", plan)
	}
	if !strings.Contains(plan.Request.GetPayloadJson(), `"reason":"expired"`) || plan.Request.GetBindingGeneration() != 7 {
		t.Fatalf("cleanup request payload/binding = %s/%d", plan.Request.GetPayloadJson(), plan.Request.GetBindingGeneration())
	}
	var claimedAt sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT cleanup_claimed_at
		   FROM session_runtime_status
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_cleanup'`).Scan(&claimedAt); err != nil {
		t.Fatalf("read claimed_at: %v", err)
	}
	if !claimedAt.Valid {
		t.Fatal("cleanup claim did not set cleanup_claimed_at before Runtime command")
	}
	seedBridgeAPIUserMessageEvent(
		t,
		admin,
		"default",
		"sesn_bridge_cleanup",
		"thr_bridge_cleanup",
		"sevt_cleanup_after_claim",
		nextBridgeAPIEventSequenceForTest(t, admin, "sesn_bridge_cleanup", "thr_bridge_cleanup"),
	)
	retryPlan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand cleanup retry after claim: %v", err)
	}
	if retryPlan.StaleAccepted || retryPlan.Request == nil {
		t.Fatalf("cleanup retry plan after claim = %#v; want cleanup command despite newer input", retryPlan)
	}

	result, err := store.FinalizeRuntimeCleanup(context.Background(), job)
	if err != nil {
		t.Fatalf("FinalizeRuntimeCleanup: %v", err)
	}
	if result.Status != RuntimeDeliveryAccepted {
		t.Fatalf("cleanup final result = %#v; want accepted", result)
	}
	if len(releaser.requests) != 1 || releaser.requests[0].SandboxID != "sandbox_sesn_bridge_cleanup" ||
		releaser.requests[0].BindingID != "bind_bridge_cleanup" || releaser.requests[0].BindingGeneration != 7 ||
		releaser.requests[0].IdempotencyKey != "cleanup_session:cleanup_bridge_1" {
		t.Fatalf("sandbox release requests = %#v", releaser.requests)
	}
	var bindingRows int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_runtime_bindings
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_cleanup'`).Scan(&bindingRows); err != nil {
		t.Fatalf("read binding rows: %v", err)
	}
	if bindingRows != 0 {
		t.Fatalf("binding rows after cleanup = %d; want 0", bindingRows)
	}
	assertBridgeResourceStillAttached(t, admin, "sesn_bridge_cleanup", "sesrsc_cleanup_file")
	assertBridgeResourcePrefixGCMarkerAbsent(t, admin, "sesn_bridge_cleanup")
	var cleanupAfter sql.NullString
	var cleanupJobID sql.NullString
	var finalizedBindingID sql.NullString
	var finalizedGeneration sql.NullInt64
	if err := admin.QueryRowContext(context.Background(),
		`SELECT cleanup_after, cleanup_job_id, binding_id, binding_generation
		   FROM session_runtime_status
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_cleanup'`).Scan(&cleanupAfter, &cleanupJobID, &finalizedBindingID, &finalizedGeneration); err != nil {
		t.Fatalf("read finalized runtime status: %v", err)
	}
	if cleanupAfter.Valid || cleanupJobID.Valid || finalizedBindingID.Valid || finalizedGeneration.Valid {
		t.Fatalf("finalized runtime status cleanup/binding markers = %v/%v/%v/%v; want all null", cleanupAfter, cleanupJobID, finalizedBindingID, finalizedGeneration)
	}
	var taskStatus string
	var terminalEventID sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status, terminal_event_id
		   FROM session_background_tasks
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_cleanup'
		    AND task_id = 'task_cleanup'`).Scan(&taskStatus, &terminalEventID); err != nil {
		t.Fatalf("read background task: %v", err)
	}
	if taskStatus != "cancelled_by_cleanup" || !terminalEventID.Valid {
		t.Fatalf("background task status/event = %q/%v; want cancelled_by_cleanup with terminal event", taskStatus, terminalEventID)
	}
	var pendingStatus string
	var pendingResultEventID sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status, result_event_id
		   FROM session_pending_tool_uses
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_cleanup'
		    AND tool_use_event_id = 'sevt_cleanup_wait'`).Scan(&pendingStatus, &pendingResultEventID); err != nil {
		t.Fatalf("read pending wait: %v", err)
	}
	if pendingStatus != "expired" || !pendingResultEventID.Valid {
		t.Fatalf("pending wait status/result = %q/%v; want expired with result event", pendingStatus, pendingResultEventID)
	}
	var resultEventType string
	var resultPayload string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT type, payload_json
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND event_id = $1`,
		pendingResultEventID.String).Scan(&resultEventType, &resultPayload); err != nil {
		t.Fatalf("read cleanup tool result event: %v", err)
	}
	if resultEventType != "agent.tool_result" || !strings.Contains(resultPayload, `"tool_use_id":"sevt_cleanup_wait"`) ||
		!strings.Contains(resultPayload, `"is_error":true`) {
		t.Fatalf("cleanup tool result event = %q/%s; want terminal error for pending wait", resultEventType, resultPayload)
	}
	var messageKind string
	var messageData string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT kind, data_json
		   FROM session_messages
		  WHERE workspace_id = 'default'
		    AND source_event_id = $1`,
		pendingResultEventID.String).Scan(&messageKind, &messageData); err != nil {
		t.Fatalf("read cleanup tool result projection: %v", err)
	}
	if messageKind != "assistant" || !strings.Contains(messageData, `"toolUseEventId":"sevt_cleanup_wait"`) ||
		!strings.Contains(messageData, `"status":"error"`) {
		t.Fatalf("cleanup tool result projection = %q/%s; want assistant terminal tool error", messageKind, messageData)
	}
	var executionState string
	var executionResult sql.NullString
	var executionDigest sql.NullString
	var executionTerminalEventID sql.NullString
	var executionConsumptionReason sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT execution_state, result_json, result_digest, consumed_by_terminal_event_id, consumption_reason
		   FROM session_runtime_tool_results
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_cleanup'
		    AND session_thread_id = 'thr_bridge_cleanup'
		    AND tool_use_event_id = 'sevt_cleanup_execution'`).Scan(
		&executionState, &executionResult, &executionDigest, &executionTerminalEventID, &executionConsumptionReason,
	); err != nil {
		t.Fatalf("read cleanup Sandbox execution: %v", err)
	}
	if executionState != "consumed" || executionResult.Valid || !executionDigest.Valid || executionDigest.String == "" || !executionTerminalEventID.Valid ||
		!executionConsumptionReason.Valid || executionConsumptionReason.String != "cleanup_wait_expired" {
		t.Fatalf("cleanup Sandbox execution = %q/%v/%v/%v/%v; want consumed with digest and cleanup event", executionState, executionResult, executionDigest, executionTerminalEventID, executionConsumptionReason)
	}
	var afterClaimProcessedAt sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT processed_at
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND event_id = 'sevt_cleanup_after_claim'`).Scan(&afterClaimProcessedAt); err != nil {
		t.Fatalf("read post-claim input: %v", err)
	}
	if afterClaimProcessedAt.Valid {
		t.Fatalf("post-claim input processed_at = %v; want still queued for next run", afterClaimProcessedAt)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreSessionDeleteCleanupClearsHotStateBeforePreparationRelease(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_bridge_hard_delete"
	const threadID = "thr_bridge_hard_delete"
	const bindingID = "bind_bridge_hard_delete"
	const preparationID = "prep_bridge_hard_delete"
	const deleteCleanupID = "delcln_bridge_hard_delete"
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 17, "pod_uid_hard_delete")
	seedBridgeAPIPreparationReady(t, admin, "default", sessionID, preparationID)
	seedBridgeAPIActiveSandbox(t, admin, "default", sessionID, "2026-01-01T00:00:00Z")
	seedBridgeAPIBackgroundTask(t, admin, "default", sessionID, threadID, bindingID, "task_hard_delete", "sevt_hard_delete_tool")
	seedBridgeAPIPendingApproval(t, admin, "default", sessionID, threadID, "sevt_hard_delete_wait", 1)

	bridgeStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	bridgeStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 45, 0, time.UTC) }
	if _, err := bridgeStore.FinishIdle(context.Background(), bridgeAPIFinishIdleRequest(
		t,
		admin,
		bridgeAPIScope(sessionID, threadID, bindingID, 17, "pod_uid_hard_delete"),
		"evt_bridge_hard_delete_running",
		`{"type":"end_turn"}`,
	)); err != nil {
		t.Fatalf("FinishIdle: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE sessions SET lifecycle_state = 'deleted', delete_cleanup_id = $2 WHERE id = $1`, sessionID, deleteCleanupID); err != nil {
		t.Fatalf("stamp hard-delete tombstone: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_resource_prefix_gc (workspace_id, session_id, prefix, status, created_at, updated_at)
		 VALUES ('default', $1, $2, 'pending', '2026-01-01T00:01:00Z', '2026-01-01T00:01:00Z')`,
		sessionID, "workspaces/default/sessions/"+sessionID+"/"); err != nil {
		t.Fatalf("seed prefix GC marker: %v", err)
	}

	releaser := &recordingSandboxReleaseClient{result: SandboxReleaseResult{Status: SandboxReleaseReleased, SandboxStatus: "released"}}
	sender := &recordingRuntimeCommandSender{response: &agentruntimev1.RuntimeInputCommandResponse{Status: agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED}}
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC) }
	store.SandboxReleaser = releaser
	job := RuntimeJob{ //nolint:gosec // Test lease token fixture, not a secret.
		JobID: "qjob_bridge_hard_delete", LeaseToken: "lease_bridge_hard_delete", Kind: queue.KindSessionDeleteCleanup,
		WorkspaceID: "default", SessionID: sessionID, RuntimeInputID: "session_delete_cleanup:" + deleteCleanupID,
		CleanupJobID: deleteCleanupID, DeleteCleanupID: deleteCleanupID,
		CommandKind: agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_CLEANUP_SESSION,
		PayloadJSON: `{"workspace_id":"default","session_id":"` + sessionID + `","delete_cleanup_id":"` + deleteCleanupID + `"}`,
	}
	result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
	if err != nil {
		t.Fatalf("DeliverRuntimeJob hard delete: %v", err)
	}
	if result.Status != RuntimeDeliveryAccepted {
		t.Fatalf("hard-delete result = %#v; want accepted", result)
	}
	if len(sender.requests) != 1 || sender.requests[0].GetCommandKind() != agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_CLEANUP_SESSION {
		t.Fatalf("hot-clear requests = %#v; want one cleanup command", sender.requests)
	}
	if len(releaser.requests) != 1 {
		t.Fatalf("release requests = %#v; want one", releaser.requests)
	}
	release := releaser.requests[0]
	if release.Reason != "delete" || release.BindingID != "" || release.BindingGeneration != 0 ||
		release.PreparationAttemptID != preparationID || release.DeleteCleanupID != deleteCleanupID ||
		release.IdempotencyKey != queue.FormatSessionDeleteCleanupDedupeKey("default", sessionID, deleteCleanupID) {
		t.Fatalf("preparation-scoped delete release = %#v", release)
	}
	var bindingCount int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_bindings WHERE session_id = $1`, sessionID).Scan(&bindingCount); err != nil {
		t.Fatalf("count binding rows: %v", err)
	}
	if bindingCount != 0 {
		t.Fatalf("binding rows after hard-delete settlement = %d; want 0 before release completion", bindingCount)
	}
	var taskStatus, waitStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_background_tasks WHERE session_id = $1`, sessionID).Scan(&taskStatus); err != nil {
		t.Fatalf("read task status: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_pending_tool_uses WHERE session_id = $1`, sessionID).Scan(&waitStatus); err != nil {
		t.Fatalf("read wait status: %v", err)
	}
	if taskStatus != "cancelled_by_cleanup" || waitStatus != "expired" {
		t.Fatalf("delete settlement task/wait = %q/%q; want cancelled_by_cleanup/expired", taskStatus, waitStatus)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreSessionDeleteCleanupRetriesReleaseAfterBindingFinalization(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_bridge_hard_delete_retry"
	const threadID = "thr_bridge_hard_delete_retry"
	const deleteCleanupID = "delcln_bridge_hard_delete_retry"
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_bridge_hard_delete_retry", 18, "pod_uid_hard_delete_retry")
	seedBridgeAPIPreparationReady(t, admin, "default", sessionID, "prep_bridge_hard_delete_retry")
	seedBridgeAPIActiveSandbox(t, admin, "default", sessionID, "2026-01-01T00:00:00Z")
	bridgeStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	bridgeStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 45, 0, time.UTC) }
	if _, err := bridgeStore.FinishIdle(context.Background(), bridgeAPIFinishIdleRequest(
		t,
		admin,
		bridgeAPIScope(sessionID, threadID, "bind_bridge_hard_delete_retry", 18, "pod_uid_hard_delete_retry"),
		"evt_bridge_hard_delete_retry_running",
		`{"type":"end_turn"}`,
	)); err != nil {
		t.Fatalf("FinishIdle: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET lifecycle_state='deleted', delete_cleanup_id=$2 WHERE id=$1`, sessionID, deleteCleanupID); err != nil {
		t.Fatalf("stamp tombstone: %v", err)
	}
	releaser := &recordingSandboxReleaseClient{result: SandboxReleaseResult{Status: SandboxReleaseRetryLater, SandboxStatus: "releasing"}}
	sender := &recordingRuntimeCommandSender{response: &agentruntimev1.RuntimeInputCommandResponse{Status: agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED}}
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.SandboxReleaser = releaser
	job := RuntimeJob{JobID: "qjob_hard_delete_retry", LeaseToken: "lease_hard_delete_retry", Kind: queue.KindSessionDeleteCleanup,
		WorkspaceID: "default", SessionID: sessionID, RuntimeInputID: "session_delete_cleanup:" + deleteCleanupID,
		CleanupJobID: deleteCleanupID, DeleteCleanupID: deleteCleanupID, CommandKind: agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_CLEANUP_SESSION}
	deliverer := RuntimePodDirectDeliverer{Store: store, Sender: sender}
	first, err := deliverer.DeliverRuntimeJob(context.Background(), job)
	if err != nil || first.Status != RuntimeDeliveryRejected || !first.Retryable {
		t.Fatalf("first hard-delete delivery = %#v err=%v; want retryable release", first, err)
	}
	var bindings int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_bindings WHERE session_id=$1`, sessionID).Scan(&bindings); err != nil {
		t.Fatalf("count bindings: %v", err)
	}
	if bindings != 0 || len(sender.requests) != 1 || len(releaser.requests) != 1 {
		t.Fatalf("after first: bindings=%d hot-clears=%d releases=%d; want 0/1/1", bindings, len(sender.requests), len(releaser.requests))
	}
	releaser.result = SandboxReleaseResult{Status: SandboxReleaseReleased, SandboxStatus: "released"}
	second, err := deliverer.DeliverRuntimeJob(context.Background(), job)
	if err != nil || second.Status != RuntimeDeliveryAccepted {
		t.Fatalf("second hard-delete delivery = %#v err=%v; want accepted", second, err)
	}
	if len(sender.requests) != 1 || len(releaser.requests) != 2 {
		t.Fatalf("retry hot-clears/releases = %d/%d; want 1/2", len(sender.requests), len(releaser.requests))
	}
}

func TestPostgreSQLSessionDeleteCleanupCrashAfterHotClearBeforeSettlementReplaysOneSettlement(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_delete_crash_hot_clear"
	const bindingID = "bind_delete_crash_hot_clear"
	const preparationID = "prep_delete_crash_hot_clear"
	const deleteCleanupID = "delcln_delete_crash_hot_clear"
	seedBridgeAPISession(t, admin, "default", sessionID, "thr_delete_crash_hot_clear")
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 21, "pod_uid_delete_crash_hot_clear")
	seedBridgeAPIPreparationReady(t, admin, "default", sessionID, preparationID)
	seedBridgeAPIActiveSandbox(t, admin, "default", sessionID, "2026-01-01T00:00:00Z")
	seedBridgeAPIBackgroundTask(t, admin, "default", sessionID, "thr_delete_crash_hot_clear", bindingID, "task_delete_crash_hot_clear", "sevt_delete_crash_hot_clear_tool")
	seedBridgeAPIPendingApproval(t, admin, "default", sessionID, "thr_delete_crash_hot_clear", "sevt_delete_crash_hot_clear_wait", 1)
	finishBridgeDeleteFixtureIdle(t, runtime, admin, sessionID, "thr_delete_crash_hot_clear", bindingID, 21, "pod_uid_delete_crash_hot_clear")
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET lifecycle_state='deleted',delete_cleanup_id=$2 WHERE id=$1`, sessionID, deleteCleanupID); err != nil {
		t.Fatalf("tombstone session: %v", err)
	}
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	releaser := &recordingSandboxReleaseClient{result: SandboxReleaseResult{Status: SandboxReleaseReleased, SandboxStatus: "released"}}
	store.SandboxReleaser = releaser
	sender := &recordingRuntimeCommandSender{response: &agentruntimev1.RuntimeInputCommandResponse{Status: agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED}}
	job := deleteCleanupRuntimeJob(sessionID, deleteCleanupID)

	plan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil || plan.Request == nil {
		t.Fatalf("prepare hot clear = %#v err=%v", plan, err)
	}
	if _, err := sender.SendRuntimeCommand(context.Background(), plan.Target, plan.Request); err != nil {
		t.Fatalf("send hot clear: %v", err)
	}
	// Simulate process loss after the Runtime accepted the idempotent cleanup
	// command but before Bridge entered its settlement transaction.
	result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
	if err != nil || result.Status != RuntimeDeliveryAccepted {
		t.Fatalf("redelivery = %#v err=%v", result, err)
	}
	if len(sender.requests) != 2 || sender.requests[0].GetRuntimeInputId() != sender.requests[1].GetRuntimeInputId() {
		t.Fatalf("hot clear requests=%d ids=%q/%q; want same idempotent command replay", len(sender.requests), sender.requests[0].GetRuntimeInputId(), sender.requests[1].GetRuntimeInputId())
	}
	assertDeleteCleanupSettlementCounts(t, admin, sessionID, 0, 1, 1)
	if len(releaser.requests) != 1 || releaser.requests[0].PreparationAttemptID != preparationID {
		t.Fatalf("release requests=%+v; want one using preparation %s", releaser.requests, preparationID)
	}
}

func TestPostgreSQLSessionDeleteCleanupSettlementAndBindingFinalizationRollbackAtomically(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_delete_atomic_settlement"
	const bindingID = "bind_delete_atomic_settlement"
	const preparationID = "prep_delete_atomic_settlement"
	const deleteCleanupID = "delcln_delete_atomic_settlement"
	seedBridgeAPISession(t, admin, "default", sessionID, "thr_delete_atomic_settlement")
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 22, "pod_uid_delete_atomic_settlement")
	seedBridgeAPIPreparationReady(t, admin, "default", sessionID, preparationID)
	seedBridgeAPIActiveSandbox(t, admin, "default", sessionID, "2026-01-01T00:00:00Z")
	seedBridgeAPIBackgroundTask(t, admin, "default", sessionID, "thr_delete_atomic_settlement", bindingID, "task_delete_atomic_settlement", "sevt_delete_atomic_settlement_tool")
	seedBridgeAPIPendingApproval(t, admin, "default", sessionID, "thr_delete_atomic_settlement", "sevt_delete_atomic_settlement_wait", 1)
	finishBridgeDeleteFixtureIdle(t, runtime, admin, sessionID, "thr_delete_atomic_settlement", bindingID, 22, "pod_uid_delete_atomic_settlement")
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET lifecycle_state='deleted',delete_cleanup_id=$2 WHERE id=$1`, sessionID, deleteCleanupID); err != nil {
		t.Fatalf("tombstone session: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `
		CREATE FUNCTION fail_delete_binding_for_test() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'forced binding finalization failure'; END $$;
		CREATE TRIGGER fail_delete_binding_for_test BEFORE DELETE ON session_runtime_bindings FOR EACH ROW EXECUTE FUNCTION fail_delete_binding_for_test()`); err != nil {
		t.Fatalf("install binding failure trigger: %v", err)
	}
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	releaser := &recordingSandboxReleaseClient{result: SandboxReleaseResult{Status: SandboxReleaseReleased, SandboxStatus: "released"}}
	store.SandboxReleaser = releaser
	sender := &recordingRuntimeCommandSender{response: &agentruntimev1.RuntimeInputCommandResponse{Status: agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED}}
	job := deleteCleanupRuntimeJob(sessionID, deleteCleanupID)
	first, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
	if err != nil || first.Status != RuntimeDeliveryRejected || !first.Retryable {
		t.Fatalf("forced finalization failure = %#v err=%v; want retryable", first, err)
	}
	assertDeleteCleanupSettlementCounts(t, admin, sessionID, 1, 0, 0)
	if len(releaser.requests) != 0 {
		t.Fatalf("release requests before atomic settlement = %d; want 0", len(releaser.requests))
	}
	if _, err := admin.ExecContext(context.Background(), `DROP TRIGGER fail_delete_binding_for_test ON session_runtime_bindings; DROP FUNCTION fail_delete_binding_for_test()`); err != nil {
		t.Fatalf("drop binding failure trigger: %v", err)
	}
	second, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
	if err != nil || second.Status != RuntimeDeliveryAccepted {
		t.Fatalf("redelivery = %#v err=%v", second, err)
	}
	assertDeleteCleanupSettlementCounts(t, admin, sessionID, 0, 1, 1)
	if len(releaser.requests) != 1 || releaser.requests[0].PreparationAttemptID != preparationID {
		t.Fatalf("release requests=%+v; want one after atomic finalization", releaser.requests)
	}
}

func TestPostgreSQLSessionDeleteCleanupCrashAfterBindingFinalizationBeforeReleaseReplaysWithoutHotClear(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_delete_crash_before_release"
	const bindingID = "bind_delete_crash_before_release"
	const preparationID = "prep_delete_crash_before_release"
	const deleteCleanupID = "delcln_delete_crash_before_release"
	seedBridgeAPISession(t, admin, "default", sessionID, "thr_delete_crash_before_release")
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 23, "pod_uid_delete_crash_before_release")
	seedBridgeAPIPreparationReady(t, admin, "default", sessionID, preparationID)
	seedBridgeAPIActiveSandbox(t, admin, "default", sessionID, "2026-01-01T00:00:00Z")
	seedBridgeAPIBackgroundTask(t, admin, "default", sessionID, "thr_delete_crash_before_release", bindingID, "task_delete_crash_before_release", "sevt_delete_crash_before_release_tool")
	seedBridgeAPIPendingApproval(t, admin, "default", sessionID, "thr_delete_crash_before_release", "sevt_delete_crash_before_release_wait", 1)
	finishBridgeDeleteFixtureIdle(t, runtime, admin, sessionID, "thr_delete_crash_before_release", bindingID, 23, "pod_uid_delete_crash_before_release")
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET lifecycle_state='deleted',delete_cleanup_id=$2 WHERE id=$1`, sessionID, deleteCleanupID); err != nil {
		t.Fatalf("tombstone session: %v", err)
	}
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	sender := &recordingRuntimeCommandSender{response: &agentruntimev1.RuntimeInputCommandResponse{Status: agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED}}
	job := deleteCleanupRuntimeJob(sessionID, deleteCleanupID)
	first, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
	if err != nil || first.Status != RuntimeDeliveryRejected || !first.Retryable || first.ErrorKind != "sandbox_release_unavailable" {
		t.Fatalf("pre-release crash = %#v err=%v", first, err)
	}
	assertDeleteCleanupSettlementCounts(t, admin, sessionID, 0, 1, 1)
	releaser := &recordingSandboxReleaseClient{result: SandboxReleaseResult{Status: SandboxReleaseReleased, SandboxStatus: "released"}}
	store.SandboxReleaser = releaser
	second, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
	if err != nil || second.Status != RuntimeDeliveryAccepted {
		t.Fatalf("release replay = %#v err=%v", second, err)
	}
	if len(sender.requests) != 1 || len(releaser.requests) != 1 || releaser.requests[0].PreparationAttemptID != preparationID {
		t.Fatalf("hot clears=%d releases=%+v; want no hot-clear replay and one preparation release", len(sender.requests), releaser.requests)
	}
	assertDeleteCleanupSettlementCounts(t, admin, sessionID, 0, 1, 1)
}

func TestPostgreSQLRuntimeDeliveryStoreSessionDeleteCleanupTerminalFailureLeavesGCMarker(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_bridge_hard_delete_failed"
	const deleteCleanupID = "delcln_bridge_hard_delete_failed"
	seedBridgeAPISession(t, admin, "default", sessionID, "thr_bridge_hard_delete_failed")
	seedBridgeAPIPreparationReady(t, admin, "default", sessionID, "prep_bridge_hard_delete_failed")
	seedBridgeAPIActiveSandbox(t, admin, "default", sessionID, "2026-01-01T00:00:00Z")
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET lifecycle_state='deleted', delete_cleanup_id=$2 WHERE id=$1`, sessionID, deleteCleanupID); err != nil {
		t.Fatalf("stamp tombstone: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_resource_prefix_gc (workspace_id, session_id, prefix, status, created_at, updated_at)
		 VALUES ('default',$1,$2,'pending','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`, sessionID, "workspaces/default/sessions/"+sessionID+"/"); err != nil {
		t.Fatalf("seed GC marker: %v", err)
	}
	releaser := &recordingSandboxReleaseClient{result: SandboxReleaseResult{Status: SandboxReleaseFailed, SandboxStatus: "failed"}}
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.SandboxReleaser = releaser
	job := RuntimeJob{JobID: "qjob_hard_delete_failed", LeaseToken: "lease_hard_delete_failed", Kind: queue.KindSessionDeleteCleanup,
		WorkspaceID: "default", SessionID: sessionID, RuntimeInputID: "session_delete_cleanup:" + deleteCleanupID,
		CleanupJobID: deleteCleanupID, DeleteCleanupID: deleteCleanupID, CommandKind: agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_CLEANUP_SESSION}
	result, err := (RuntimePodDirectDeliverer{Store: store}).DeliverRuntimeJob(context.Background(), job)
	if err != nil || result.Status != RuntimeDeliveryRejected || result.Retryable || result.ErrorKind != "sandbox_release_failed" {
		t.Fatalf("terminal delete result = %#v err=%v", result, err)
	}
	var status string
	var lastError sql.NullString
	if err := admin.QueryRowContext(context.Background(), `SELECT status,last_error_kind FROM session_resource_prefix_gc WHERE session_id=$1`, sessionID).Scan(&status, &lastError); err != nil {
		t.Fatalf("read GC marker: %v", err)
	}
	if status != "pending" || !lastError.Valid || lastError.String != "sandbox_release_failed" {
		t.Fatalf("GC marker = %q/%v; want pending/sandbox_release_failed", status, lastError)
	}
}

func TestPostgreSQLSessionDeleteCleanupEffectiveMaxAttemptExhaustionDeadLettersWithGCPending(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_delete_max_attempts"
	const deleteCleanupID = "delcln_delete_max_attempts"
	seedBridgeAPISession(t, admin, "default", sessionID, "thr_delete_max_attempts")
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE sessions SET lifecycle_state='deleted',delete_cleanup_id=$2 WHERE id=$1`, sessionID, deleteCleanupID); err != nil {
		t.Fatalf("tombstone session: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_resource_prefix_gc (workspace_id,session_id,prefix,status,created_at,updated_at)
		 VALUES ('default',$1,$2,'pending','2026-07-14T12:00:00Z','2026-07-14T12:00:00Z')`,
		sessionID, "workspaces/default/sessions/"+sessionID+"/"); err != nil {
		t.Fatalf("seed prefix GC marker: %v", err)
	}
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	queueStore := queue.NewPostgreSQLStoreWithRetryPolicy(dbconnect.NewClientForTesting(runtime), queue.RetryPolicy{
		BaseDelay: time.Second, MaxDelay: time.Second, MaxAttempts: 2, RandomInt64: func(int64) int64 { return 0 },
	})
	job, err := queueStore.Enqueue(context.Background(), queue.EnqueueRequest{
		WorkspaceID: workspace.DefaultID, Kind: queue.KindSessionDeleteCleanup,
		PartitionKey: queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID),
		DedupeKey:    queue.FormatSessionDeleteCleanupDedupeKey(workspace.DefaultID, sessionID, deleteCleanupID),
		PayloadJSON:  []byte(`{"workspace_id":"default","session_id":"` + sessionID + `","delete_cleanup_id":"` + deleteCleanupID + `"}`),
		MaxAttempts:  0, Now: now,
	})
	if err != nil {
		t.Fatalf("enqueue delete cleanup: %v", err)
	}
	for attempt := 1; attempt <= 2; attempt++ {
		leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindSessionDeleteCleanup}, LeaseOwner: "bridge", MaxJobs: 1, LeaseDuration: time.Minute, Now: now})
		if err != nil || len(leased) != 1 {
			t.Fatalf("lease attempt %d = %+v err=%v; want one", attempt, leased, err)
		}
		if leased[0].MaxAttempts != 2 {
			t.Fatalf("effective max attempts = %d; want 2", leased[0].MaxAttempts)
		}
		ok, err := queueStore.Retry(context.Background(), queue.RetryRequest{WorkspaceID: workspace.DefaultID, JobID: job.ID, LeaseToken: leased[0].LeaseToken, ErrorKind: "sandbox_release_retry_later", Now: now})
		if err != nil || !ok {
			t.Fatalf("retry attempt %d = %v,%v", attempt, ok, err)
		}
	}
	var queueStatus, gcStatus string
	var attempts int
	if err := admin.QueryRowContext(context.Background(), `SELECT status,attempt_count FROM queue_jobs WHERE id=$1`, job.ID).Scan(&queueStatus, &attempts); err != nil {
		t.Fatalf("read queue terminal state: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_resource_prefix_gc WHERE session_id=$1`, sessionID).Scan(&gcStatus); err != nil {
		t.Fatalf("read GC state: %v", err)
	}
	if queueStatus != queue.StatusDeadLettered || attempts != 2 || gcStatus != "pending" {
		t.Fatalf("queue=%q attempts=%d GC=%q; want dead_lettered/2/pending", queueStatus, attempts, gcStatus)
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

func TestPostgreSQLRuntimeDeliveryStoreSessionDeleteCleanupClosedUnboundBranches(t *testing.T) {
	for _, tc := range []struct {
		name              string
		sandboxRow        bool
		status            string
		cleanupStatus     string
		nullHandle        bool
		releaseStatus     SandboxReleaseStatus
		wantStatus        RuntimeDeliveryStatus
		wantRetry         bool
		wantErrorKind     string
		wantProviderCalls int
	}{
		{name: "no sandbox row vacuous", sandboxRow: false, wantStatus: RuntimeDeliveryAccepted},
		{name: "creating retries", sandboxRow: true, status: "creating", wantStatus: RuntimeDeliveryRejected, wantRetry: true, wantErrorKind: "sandbox_startup_in_progress"},
		{name: "resuming retries", sandboxRow: true, status: "resuming", wantStatus: RuntimeDeliveryRejected, wantRetry: true, wantErrorKind: "sandbox_startup_in_progress"},
		{name: "active recorded handle", sandboxRow: true, status: "active", wantStatus: RuntimeDeliveryAccepted, wantProviderCalls: 1},
		{name: "stopped recorded handle", sandboxRow: true, status: "stopped", wantStatus: RuntimeDeliveryAccepted, wantProviderCalls: 1},
		{name: "archived recorded handle", sandboxRow: true, status: "archived", wantStatus: RuntimeDeliveryAccepted, wantProviderCalls: 1},
		{name: "released recorded handle", sandboxRow: true, status: "released", wantStatus: RuntimeDeliveryAccepted, wantProviderCalls: 1},
		{name: "releasing recorded handle", sandboxRow: true, status: "releasing", wantStatus: RuntimeDeliveryAccepted, wantProviderCalls: 1},
		{name: "failed recorded handle cleanup released", sandboxRow: true, status: "failed", cleanupStatus: "released", wantStatus: RuntimeDeliveryAccepted, wantProviderCalls: 1},
		{name: "legacy released recorded handle cleanup released", sandboxRow: true, status: "released", cleanupStatus: "released", wantStatus: RuntimeDeliveryAccepted, wantProviderCalls: 1},
		{name: "failed NULL handle found by name", sandboxRow: true, status: "failed", cleanupStatus: "released", nullHandle: true, releaseStatus: SandboxReleaseReleased, wantStatus: RuntimeDeliveryAccepted, wantProviderCalls: 1},
		{name: "failed NULL handle absent by name", sandboxRow: true, status: "failed", cleanupStatus: "released", nullHandle: true, releaseStatus: SandboxReleaseAlreadyReleased, wantStatus: RuntimeDeliveryAccepted, wantProviderCalls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			suffix := strings.ReplaceAll(tc.name, " ", "_")
			sessionID := "sesn_delete_branch_" + suffix
			deleteCleanupID := "delcln_delete_branch_" + suffix
			seedBridgeAPISession(t, admin, "default", sessionID, "thr_delete_branch_"+suffix)
			seedBridgeAPIPreparationReady(t, admin, "default", sessionID, "prep_delete_branch_"+suffix)
			if tc.sandboxRow {
				seedBridgeAPIActiveSandbox(t, admin, "default", sessionID, "2026-01-01T00:00:00Z")
				if _, err := admin.ExecContext(context.Background(),
					`UPDATE sandboxes SET status=$2, cleanup_status=CASE WHEN $3='' THEN 'none' ELSE $3 END,
					 released_at=CASE WHEN $2='released' THEN updated_at ELSE NULL END,
					 failed_at=CASE WHEN $2='failed' THEN updated_at ELSE NULL END,
					 startup_failure_reason=CASE WHEN $2='failed' THEN 'startup_failed' ELSE NULL END
					 WHERE session_id=$1`, sessionID, tc.status, tc.cleanupStatus); err != nil {
					t.Fatalf("seed sandbox status: %v", err)
				}
				if tc.nullHandle {
					if _, err := admin.ExecContext(context.Background(), `UPDATE sandboxes SET provider_sandbox_id=NULL,provider_metadata_json='{}' WHERE session_id=$1`, sessionID); err != nil {
						t.Fatalf("clear provider handle: %v", err)
					}
				}
			}
			if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET lifecycle_state='deleted', delete_cleanup_id=$2 WHERE id=$1`, sessionID, deleteCleanupID); err != nil {
				t.Fatalf("stamp tombstone: %v", err)
			}
			if _, err := admin.ExecContext(context.Background(),
				`INSERT INTO session_resource_prefix_gc (workspace_id,session_id,prefix,status,created_at,updated_at)
				 VALUES ('default',$1,$2,'pending','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`,
				sessionID, "workspaces/default/sessions/"+sessionID+"/"); err != nil {
				t.Fatalf("seed prefix GC marker: %v", err)
			}
			releaseStatus := tc.releaseStatus
			if releaseStatus == "" {
				releaseStatus = SandboxReleaseReleased
			}
			releaser := &recordingSandboxReleaseClient{result: SandboxReleaseResult{Status: releaseStatus, SandboxStatus: "released"}}
			store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
			store.SandboxReleaser = releaser
			job := RuntimeJob{JobID: "qjob_" + suffix, LeaseToken: "lease_" + suffix, Kind: queue.KindSessionDeleteCleanup,
				WorkspaceID: "default", SessionID: sessionID, RuntimeInputID: "session_delete_cleanup:" + deleteCleanupID,
				CleanupJobID: deleteCleanupID, DeleteCleanupID: deleteCleanupID, CommandKind: agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_CLEANUP_SESSION}
			result, err := (RuntimePodDirectDeliverer{Store: store}).DeliverRuntimeJob(context.Background(), job)
			if err != nil {
				t.Fatalf("DeliverRuntimeJob: %v", err)
			}
			if result.Status != tc.wantStatus || result.Retryable != tc.wantRetry || result.ErrorKind != tc.wantErrorKind {
				t.Fatalf("result = %#v; want status=%s retry=%v kind=%q", result, tc.wantStatus, tc.wantRetry, tc.wantErrorKind)
			}
			if len(releaser.requests) != tc.wantProviderCalls {
				t.Fatalf("release calls = %d; want %d", len(releaser.requests), tc.wantProviderCalls)
			}
			var gcStatus string
			if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_resource_prefix_gc WHERE session_id=$1`, sessionID).Scan(&gcStatus); err != nil {
				t.Fatalf("read prefix GC marker: %v", err)
			}
			if gcStatus != "pending" {
				t.Fatalf("prefix GC status = %q; want pending until the post-release GC owner runs", gcStatus)
			}
			queueJobID := "qjob_transition_" + suffix
			queueClient := &recordingQueueClient{leased: []*queuev1.QueueJob{{
				Id: queueJobID, WorkspaceId: "default", Kind: queue.KindSessionDeleteCleanup, LeaseToken: "lease_transition_" + suffix,
				PayloadJson: `{"workspace_id":"default","session_id":"` + sessionID + `","delete_cleanup_id":"` + deleteCleanupID + `"}`,
			}}}
			if err := (&JobRunner{Queue: queueClient, Workspaces: staticWorkspaceLister{workspace.DefaultID}, Deliverer: &recordingDeliverer{result: result}}).RunOnce(context.Background()); err != nil {
				t.Fatalf("JobRunner transition: %v", err)
			}
			wantTransition := "ack:" + queueJobID
			if tc.wantRetry {
				wantTransition = "retry:" + queueJobID + ":" + tc.wantErrorKind
			}
			if !reflect.DeepEqual(queueClient.transitions, []string{wantTransition}) {
				t.Fatalf("queue transitions=%v; want %q", queueClient.transitions, wantTransition)
			}
		})
	}
}

func TestPostgreSQLRuntimeDeliveryStoreCleanupSessionKeepsResolvingConfirmationAfterClaim(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_cleanup_confirm", "thr_bridge_cleanup_confirm_main")
	seedBridgeAPIChildThread(t, admin, "default", "sesn_bridge_cleanup_confirm", "thr_bridge_cleanup_confirm_main", "thr_bridge_cleanup_confirm_child")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_cleanup_confirm", "bind_bridge_cleanup_confirm", 7, "pod_uid_cleanup_confirm")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_cleanup_confirm", "prep_bridge_cleanup_confirm")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_cleanup_confirm", "2026-01-01T00:00:00Z")
	seedBridgeAPIPendingApproval(t, admin, "default", "sesn_bridge_cleanup_confirm", "thr_bridge_cleanup_confirm_child", "sevt_cleanup_confirm_wait", 1)

	bridgeStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	bridgeStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 45, 0, time.UTC) }
	if _, err := bridgeStore.FinishIdle(context.Background(), bridgeAPIFinishIdleRequest(
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

	releaser := &recordingSandboxReleaseClient{result: SandboxReleaseResult{Status: SandboxReleaseReleased, SandboxStatus: "released"}}
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 31, 0, 0, time.UTC) }
	store.SandboxReleaser = releaser
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
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_cleanup_preidle", "prep_bridge_cleanup_preidle")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_cleanup_preidle", "2026-01-01T00:00:00Z")
	seedBridgeAPIUserMessageEvent(t, admin, "default", "sesn_bridge_cleanup_preidle", "thr_bridge_cleanup_preidle_child", "sevt_cleanup_preidle_superseded", 99)

	bridgeStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	bridgeStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 45, 0, time.UTC) }
	if _, err := bridgeStore.FinishIdle(context.Background(), bridgeAPIFinishIdleRequest(
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
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_cleanup_child_postidle", "prep_bridge_cleanup_child_postidle")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_cleanup_child_postidle", "2026-01-01T00:00:00Z")

	bridgeStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	bridgeStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 45, 0, time.UTC) }
	if _, err := bridgeStore.FinishIdle(context.Background(), bridgeAPIFinishIdleRequest(
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
	releaser := &recordingSandboxReleaseClient{result: SandboxReleaseResult{Status: SandboxReleaseReleased, SandboxStatus: "released"}}
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 31, 0, 0, time.UTC) }
	store.SandboxReleaser = releaser
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
	if len(releaser.requests) != 0 {
		t.Fatalf("sandbox release requests with running child = %#v; want none", releaser.requests)
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
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_cleanup_gone", "prep_bridge_cleanup_gone")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_cleanup_gone", "2026-01-01T00:00:00Z")
	seedBridgeAPIBackgroundTask(t, admin, "default", "sesn_bridge_cleanup_gone", "thr_bridge_cleanup_gone", "bind_bridge_cleanup_gone", "task_cleanup_gone", "sevt_cleanup_gone_tool")
	seedBridgeAPIPendingApproval(t, admin, "default", "sesn_bridge_cleanup_gone", "thr_bridge_cleanup_gone", "sevt_cleanup_gone_wait", 1)

	bridgeStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	bridgeStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 45, 0, time.UTC) }
	if _, err := bridgeStore.FinishIdle(context.Background(), bridgeAPIFinishIdleRequest(
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

	releaser := &recordingSandboxReleaseClient{result: SandboxReleaseResult{Status: SandboxReleaseReleased, SandboxStatus: "released"}}
	sender := &recordingRuntimeCommandSender{response: &agentruntimev1.RuntimeInputCommandResponse{
		Status: agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED,
	}}
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 31, 0, 0, time.UTC) }
	store.SandboxReleaser = releaser
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
	if len(releaser.requests) != 1 || releaser.requests[0].SandboxID != "sandbox_sesn_bridge_cleanup_gone" ||
		releaser.requests[0].BindingID != "bind_bridge_cleanup_gone" || releaser.requests[0].BindingGeneration != 7 {
		t.Fatalf("sandbox release requests = %#v", releaser.requests)
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
	var taskStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status
		   FROM session_background_tasks
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_cleanup_gone'
		    AND task_id = 'task_cleanup_gone'`).Scan(&taskStatus); err != nil {
		t.Fatalf("read background task: %v", err)
	}
	if taskStatus != "cancelled_by_cleanup" {
		t.Fatalf("gone cleanup background task status = %q; want cancelled_by_cleanup", taskStatus)
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
	seedBridgeAPIPreparationReady(t, admin, "default", sessionID, "prep_"+sessionID)
	seedBridgeAPIActiveSandbox(t, admin, "default", sessionID, "2026-01-01T00:00:00Z")
	bridgeStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	bridgeStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 45, 0, time.UTC) }
	if _, err := bridgeStore.FinishIdle(context.Background(), bridgeAPIFinishIdleRequest(
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
