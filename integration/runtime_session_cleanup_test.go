package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
)

func TestCleanupExpiredSandboxToolAppendsNarrowResultToOriginalAssistantContext(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID       = "sesn_cleanup_narrow_tool"
		threadID        = "thr_cleanup_narrow_tool"
		modelRequestID  = "mreq_cleanup_narrow_tool"
		toolUseEventID  = "evt_cleanup_narrow_tool_use"
		modelToolCallID = "call_cleanup_narrow_tool"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_cleanup_narrow_tool", 1, "pod_cleanup_narrow_tool")
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_events (
		workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
		visibility, session_visible, model_request_id, projection_json, created_at, updated_at
	) VALUES
	('default', $1, $2, 'evt_cleanup_narrow_start', 1, 'span.model_request_start',
	 '{}', 'internal', false, $3,
	 '{"context_through_message_sequence":0,"request_kind":"agent_provider_request"}',
	 '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'),
	('default', $1, $2, $4, 2, 'agent.tool_use',
	 '{"type":"agent.tool_use","name":"Read","input":{"file_path":"README.md"},"evaluated_permission":"allow"}',
	 'public', true, $3, '{}', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'),
	('default', $1, $2, 'evt_cleanup_narrow_end', 3, 'span.model_request_end',
	 '{"type":"span.model_request_end","model_request_start_id":"evt_cleanup_narrow_start","is_error":false,"provider_context_retention":{"disposition":"completed","assistant_message_sequence":1,"tool_use_event_ids":["evt_cleanup_narrow_tool_use"],"repair_event_ids":[]}}',
	 'internal', false, $3, '{}', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		sessionID, threadID, modelRequestID, toolUseEventID,
	); err != nil {
		t.Fatalf("seed cleanup Tool turn: %v", err)
	}
	seedBridgeAPIDurableToolMessage(
		t, admin, "default", sessionID, threadID, modelRequestID,
		toolUseEventID, modelToolCallID, "Read",
	)
	tracer := &bridgeExecutionQueryTracer{}
	client := dbconnect.NewClientForTesting(storagetest.OpenRuntimeRoleDBWithTracer(t, runtime, tracer))
	scope := bridgeAPIScope(sessionID, threadID, "bind_cleanup_narrow_tool", 1, "pod_cleanup_narrow_tool")
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "evt_cleanup_narrow_idle", 4, "session.status_idle", `{"type":"session.status_idle","stop_reason":{"type":"end_turn"}}`)
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_status (
		workspace_id,session_id,status,status_event_id,binding_id,binding_generation,idle_since,cleanup_job_id,cleanup_enqueued_at,created_at,updated_at
	) VALUES ('default',$1,'idle','evt_cleanup_narrow_idle','bind_cleanup_narrow_tool',1,'2026-01-01T00:00:00Z','cleanup_narrow_tool','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z');
	`, sessionID); err != nil {
		t.Fatalf("seed cleanup idle custody: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_tool_results (
		workspace_id,session_id,session_thread_id,tool_use_event_id,tool_kind,normalized_input_hash,tool_name,input_json,ack_status,model_tool_call_id,execution_state,execution_attempt_generation,created_at,updated_at
	) VALUES ('default',$1,$2,$3,'sandbox_tool',$4,'Read','{"file_path":"README.md"}','committed',$5,'pending',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`, sessionID, threadID, toolUseEventID, runtimecontrol.Sha256Hex(`{"file_path":"README.md"}`), modelToolCallID); err != nil {
		t.Fatalf("seed pending cleanup Sandbox execution: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events SET latest_stream_position=sequence WHERE workspace_id='default' AND session_id=$1`, sessionID); err != nil {
		t.Fatalf("seed cleanup event stream positions: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_pending_tool_uses(workspace_id,session_id,session_thread_id,tool_use_event_id,model_tool_call_id,tool_name,input_json,status,created_at,updated_at) VALUES('default',$1,$2,$3,$4,'Read','{"file_path":"README.md"}','cancelled','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`, sessionID, threadID, toolUseEventID, modelToolCallID); err != nil {
		t.Fatalf("seed canceled cleanup execution route: %v", err)
	}
	deliveryStore := fixtureRuntimeDeliveryStore(client, admin, 9090)
	deliveryStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC) }
	job := leaseCleanupRuntimeJobForTest(t, runtime, cleanupTreeJob(sessionID, "cleanup_narrow_tool"))
	plan, err := deliveryStore.PrepareRuntimeCommand(context.Background(), job)
	if err != nil || plan.CleanupSession == nil || plan.StaleAccepted {
		t.Fatalf("claim narrow cleanup: %#v/%v", plan, err)
	}
	result, err := deliveryStore.FinalizeRuntimeCleanup(context.Background(), job)
	if err != nil || result.Status != jobrunner.RuntimeDeliveryAccepted || !result.QueueLeaseSettled {
		tracer.mu.Lock()
		failures := append([]error(nil), tracer.errors...)
		queries := append([]bridgeTraceEntry(nil), tracer.entries...)
		tracer.mu.Unlock()
		if len(queries) > 5 {
			queries = queries[len(queries)-5:]
		}
		t.Fatalf("settle cleanup-expired Tool: %#v/%v SQL failures: %v last queries: %+v", result, err, failures, queries)
	}
	var resultModelRequestID string
	if err := admin.QueryRowContext(context.Background(), `SELECT model_request_id
		FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
		  AND type='agent.tool_result'`, sessionID, threadID).Scan(&resultModelRequestID); err != nil {
		t.Fatalf("read cleanup Tool result request identity: %v", err)
	}
	if resultModelRequestID != modelRequestID {
		t.Fatalf("cleanup Tool result model request = %q; want %q", resultModelRequestID, modelRequestID)
	}
	var messageCount int
	var dataJSON string
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*), max(data_json)
		FROM session_messages
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2`, sessionID, threadID,
	).Scan(&messageCount, &dataJSON); err != nil {
		t.Fatalf("read cleanup Tool context: %v", err)
	}
	if messageCount != 1 {
		t.Fatalf("cleanup Tool context messages = %d; want original Assistant message only", messageCount)
	}
	stored, err := runtimecontrol.DecodeRuntimeDeclarationObject(dataJSON)
	if err != nil {
		t.Fatalf("decode cleanup Tool context: %v", err)
	}
	if len(stored) != 1 {
		t.Fatalf("cleanup Tool context = %#v; want parts only", stored)
	}
	parts, ok := stored["parts"].([]any)
	if !ok || len(parts) != 2 {
		t.Fatalf("cleanup Tool parts = %#v; want one call/result pair", stored["parts"])
	}
	resultPart, ok := parts[1].(map[string]any)
	if !ok || resultPart["type"] != "tool_result" || resultPart["modelToolCallId"] != modelToolCallID {
		t.Fatalf("cleanup Tool result = %#v; want exact narrow Tool identity", parts[1])
	}
	terminalResult, ok := resultPart["result"].(map[string]any)
	if !ok || terminalResult["type"] != "error" {
		t.Fatalf("cleanup Tool outcome = %#v; want error", resultPart["result"])
	}
	terminalError, ok := terminalResult["error"].(map[string]any)
	if !ok || terminalError["type"] != "cleanup_expired" || terminalError["message"] != "Sandbox tool execution expired during session cleanup." || terminalError["retryable"] != false {
		t.Fatalf("cleanup Tool error = %#v; want exact non-retryable cleanup_expired body", terminalResult["error"])
	}
	bridgeStore := agentruntimebridge.NewPostgreSQLBridgeAPIStore(client)
	bridgeStore.RuntimeBindingTokenHMACKey = []byte("cleanup-narrow-tool-signing-key")
	scope = declareReplacementScope(t, client, client, scope)
	loaded, err := bridgeStore.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope})
	if err != nil {
		t.Fatalf("LoadContext after cleanup Tool settlement: %v", err)
	}
	assertRuntimeDirectContextComposition(t, loaded.GetContextJson())
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
	store := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.AttachmentBlobStore = blob.NewFakeBlobStore()
	store.Clock = func() time.Time { return time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC) }
	attachment := createBridgeTransientAttachmentForTest(t, admin, store,
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
		sessionID, threadID, toolUseID, runtimecontrol.Sha256Hex(`{}`), strings.Repeat("a", 64), resultID, store.Clock()); err != nil {
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
	deliveryStore := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
	deliveryStore.AttachmentBlobStore = store.AttachmentBlobStore
	deliveryStore.Clock = func() time.Time { return store.Clock().Add(time.Minute) }
	result, err := deliveryStore.FinalizeRuntimeCleanup(context.Background(), jobrunner.RuntimeJob{
		Kind: queue.KindSessionDeleteCleanup, WorkspaceID: "default", SessionID: sessionID,
		DeleteCleanupID: cleanupID, AttemptCount: 1, MaxAttempts: 5,
	})
	if err != nil {
		t.Fatalf("finalize Session delete cleanup: %v", err)
	}
	if result.Status != jobrunner.RuntimeDeliveryAccepted {
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

func TestPostgreSQLRuntimeDeliveryStoreCleanupSessionRejectsNewInputBeforeClaim(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_cleanup_preclaim", "thr_bridge_cleanup_preclaim")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_cleanup_preclaim", "bind_bridge_cleanup_preclaim", 7, "pod_uid_cleanup_preclaim")

	bridgeStore := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
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

	store := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 31, 0, 0, time.UTC) }
	plan, err := store.PrepareRuntimeCommand(context.Background(), jobrunner.RuntimeJob{ //nolint:gosec // Test lease token fixture, not a secret.
		JobID:          "qjob_cleanup_bridge_preclaim",
		LeaseToken:     "lease_cleanup_bridge_preclaim",
		Kind:           queue.KindCleanupSession,
		WorkspaceID:    "default",
		SessionID:      "sesn_bridge_cleanup_preclaim",
		RuntimeInputID: "cleanup_session:cleanup_bridge_preclaim_1",
		CleanupJobID:   "cleanup_bridge_preclaim_1",
		PayloadJSON:    `{"workspace_id":"default","session_id":"sesn_bridge_cleanup_preclaim","cleanup_job_id":"cleanup_bridge_preclaim_1"}`,
	})
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand cleanup preclaim: %v", err)
	}
	if !plan.StaleAccepted || plan.CleanupSession != nil {
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

func TestPostgreSQLRuntimeDeliveryStoreCleanupSessionKeepsResolvingConfirmationAfterClaim(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_cleanup_confirm", "thr_bridge_cleanup_confirm_main")
	seedBridgeAPIChildThread(t, admin, "default", "sesn_bridge_cleanup_confirm", "thr_bridge_cleanup_confirm_main", "thr_bridge_cleanup_confirm_child")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_cleanup_confirm", "bind_bridge_cleanup_confirm", 7, "pod_uid_cleanup_confirm")
	seedBridgeAPIPendingApproval(t, admin, "default", "sesn_bridge_cleanup_confirm", "thr_bridge_cleanup_confirm_child", "sevt_cleanup_confirm_wait", 1)

	bridgeStore := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
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

	store := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 31, 0, 0, time.UTC) }
	job := jobrunner.RuntimeJob{ //nolint:gosec // Test lease token fixture, not a secret.
		JobID:          "qjob_cleanup_bridge_confirm",
		LeaseToken:     "lease_cleanup_bridge_confirm",
		Kind:           queue.KindCleanupSession,
		WorkspaceID:    "default",
		SessionID:      "sesn_bridge_cleanup_confirm",
		RuntimeInputID: "cleanup_session:cleanup_bridge_confirm_1",
		CleanupJobID:   "cleanup_bridge_confirm_1",
		PayloadJSON:    `{"workspace_id":"default","session_id":"sesn_bridge_cleanup_confirm","cleanup_job_id":"cleanup_bridge_confirm_1"}`,
	}
	job = leaseCleanupRuntimeJobForTest(t, runtime, job)
	plan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand cleanup confirmation: %v", err)
	}
	if plan.StaleAccepted || plan.CleanupSession == nil {
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
	if result.Status != jobrunner.RuntimeDeliveryAccepted {
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

	bridgeStore := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
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

	store := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 31, 0, 0, time.UTC) }
	plan, err := store.PrepareRuntimeCommand(context.Background(), jobrunner.RuntimeJob{ //nolint:gosec // Test lease token fixture, not a secret.
		JobID:          "qjob_cleanup_bridge_preidle",
		LeaseToken:     "lease_cleanup_bridge_preidle",
		Kind:           queue.KindCleanupSession,
		WorkspaceID:    "default",
		SessionID:      "sesn_bridge_cleanup_preidle",
		RuntimeInputID: "cleanup_session:cleanup_bridge_preidle_1",
		CleanupJobID:   "cleanup_bridge_preidle_1",
		PayloadJSON:    `{"workspace_id":"default","session_id":"sesn_bridge_cleanup_preidle","cleanup_job_id":"cleanup_bridge_preidle_1"}`,
	})
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand cleanup preidle: %v", err)
	}
	if plan.StaleAccepted || plan.CleanupSession == nil {
		t.Fatalf("cleanup preidle plan = %#v; want claim despite pre-idle unprocessed child input", plan)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreCleanupSessionRejectsPostIdleChildInputByStreamFence(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_cleanup_child_postidle", "thr_bridge_cleanup_child_postidle_main")
	seedBridgeAPIChildThread(t, admin, "default", "sesn_bridge_cleanup_child_postidle", "thr_bridge_cleanup_child_postidle_main", "thr_bridge_cleanup_child_postidle_child")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_cleanup_child_postidle", "bind_bridge_cleanup_child_postidle", 7, "pod_uid_cleanup_child_postidle")

	bridgeStore := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
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

	store := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 31, 0, 0, time.UTC) }
	plan, err := store.PrepareRuntimeCommand(context.Background(), jobrunner.RuntimeJob{ //nolint:gosec // Test lease token fixture, not a secret.
		JobID:          "qjob_cleanup_bridge_child_postidle",
		LeaseToken:     "lease_cleanup_bridge_child_postidle",
		Kind:           queue.KindCleanupSession,
		WorkspaceID:    "default",
		SessionID:      "sesn_bridge_cleanup_child_postidle",
		RuntimeInputID: "cleanup_session:cleanup_bridge_child_postidle_1",
		CleanupJobID:   "cleanup_bridge_child_postidle_1",
		PayloadJSON:    `{"workspace_id":"default","session_id":"sesn_bridge_cleanup_child_postidle","cleanup_job_id":"cleanup_bridge_child_postidle_1"}`,
	})
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand cleanup child post-idle: %v", err)
	}
	if !plan.StaleAccepted || plan.CleanupSession != nil {
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

	store := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 31, 0, 0, time.UTC) }
	plan, err := store.PrepareRuntimeCommand(context.Background(), cleanupTreeJob(sessionID, cleanupID))
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand with running child: %v", err)
	}
	if !plan.StaleAccepted || plan.CleanupSession != nil {
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
	if plan.StaleAccepted || plan.CleanupSession == nil {
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
	store := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 31, 0, 0, time.UTC) }
	job := leaseCleanupRuntimeJobForTest(t, runtime, cleanupTreeJob(sessionID, cleanupID))
	plan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand before child starts: %v", err)
	}
	if plan.StaleAccepted || plan.CleanupSession == nil {
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
	if result.Status != jobrunner.RuntimeDeliveryDuplicate {
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
		store := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
		store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 31, 0, 0, time.UTC) }
		plan, err := store.PrepareRuntimeCommand(context.Background(), cleanupTreeJob(sessionID, cleanupID))
		if err != nil {
			t.Fatalf("PrepareRuntimeCommand with requires_action child: %v", err)
		}
		if plan.StaleAccepted || plan.CleanupSession == nil {
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
		store := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
		store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 31, 0, 0, time.UTC) }
		plan, err := store.PrepareRuntimeCommand(context.Background(), cleanupTreeJob(sessionID, cleanupID))
		if err != nil {
			t.Fatalf("PrepareRuntimeCommand with post-idle confirmation: %v", err)
		}
		if !plan.StaleAccepted || plan.CleanupSession != nil {
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
		store := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
		store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 31, 0, 0, time.UTC) }
		plan, err := store.PrepareRuntimeCommand(context.Background(), cleanupTreeJob(sessionID, cleanupID))
		if err != nil {
			t.Fatalf("PrepareRuntimeCommand with running reviewer: %v", err)
		}
		if !plan.StaleAccepted || plan.CleanupSession != nil {
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

	bridgeStore := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	bridgeStore.AttachmentBlobStore = blob.NewFakeBlobStore()
	bridgeStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 45, 0, time.UTC) }
	attachment := createBridgeTransientAttachmentForTest(
		t, admin, bridgeStore,
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
		runtimecontrol.Sha256Hex(`{}`), resultJSON, runtimecontrol.Sha256Hex(resultJSON)); err != nil {
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

	sender := &recordingRuntimeCommandSender{result: jobrunner.RuntimeDeliveryResult{Status: jobrunner.RuntimeDeliveryAccepted}}
	store := jobrunner.NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090, jobrunner.KubernetesRuntimeTargetResolver{GetPod: fixtureConfirmedMissingRuntimePod,
		Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
			return enginekubernetes.NewBindingVisibilitySnapshotStateForTest(true, enginekubernetes.BoundRuntimePod{
				Namespace: "tetral-agent-runtime",
				PodName:   "runtime-pod-0",
				PodUID:    "pod_uid_cleanup_gone",
				PodIP:     "10.0.0.10",
			}, enginekubernetes.BindingVisibilityAbsent)
		},
	})
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 31, 0, 0, time.UTC) }
	job := jobrunner.RuntimeJob{ //nolint:gosec // Test lease token fixture, not a secret.
		JobID:          "qjob_cleanup_bridge_gone",
		LeaseToken:     "lease_cleanup_bridge_gone",
		Kind:           queue.KindCleanupSession,
		WorkspaceID:    "default",
		SessionID:      "sesn_bridge_cleanup_gone",
		RuntimeInputID: "cleanup_session:cleanup_bridge_gone_1",
		CleanupJobID:   "cleanup_bridge_gone_1",
		PayloadJSON:    `{"workspace_id":"default","session_id":"sesn_bridge_cleanup_gone","cleanup_job_id":"cleanup_bridge_gone_1"}`,
	}
	job = leaseCleanupRuntimeJobForTest(t, runtime, job)
	result, err := (jobrunner.RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
	if err != nil {
		t.Fatalf("DeliverRuntimeJob cleanup gone: %v", err)
	}
	if result.Status != jobrunner.RuntimeDeliveryAccepted {
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
	if pendingStatus != "pending" {
		t.Fatalf("gone cleanup pending wait status = %q; want preserved pending approval", pendingStatus)
	}
	var executionState string
	var storedResult sql.NullString
	var consumptionReason sql.NullString
	if err := admin.QueryRowContext(context.Background(), `SELECT execution_state, result_json, consumption_reason
		FROM session_runtime_tool_results
		WHERE workspace_id='default' AND session_id='sesn_bridge_cleanup_gone' AND tool_use_event_id='sevt_cleanup_gone_wait'`).Scan(
		&executionState, &storedResult, &consumptionReason,
	); err != nil {
		t.Fatalf("read cleanup-wait execution receipt: %v", err)
	}
	if executionState != "terminal_unconsumed" || !storedResult.Valid || storedResult.String != resultJSON || consumptionReason.Valid {
		t.Fatalf("cleanup-wait execution = %q/%v/%v; want recoverable terminal receipt", executionState, storedResult, consumptionReason)
	}
	bridgeStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 31, 0, 0, time.UTC) }
	if result, err := bridgeStore.ReconcileTransientAttachments(context.Background(), 10); err != nil || result.Deleted != 0 {
		t.Fatalf("reconcile cleanup-wait attachment = %+v, %v; want retained recoverable attachment", result, err)
	}
	if got := bridgeTransientAttachmentStatus(t, admin, attachment.GetAttachmentRef()); got != "staged" {
		t.Fatalf("cleanup-wait attachment status = %q; want staged", got)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreCleanupSessionPreservesApprovalForColdSettlement(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_cleanup_cold_approval"
		threadID       = "thr_cleanup_cold_approval"
		bindingID      = "bind_cleanup_cold_approval"
		podUID         = "pod_cleanup_cold_approval"
		modelRequestID = "mreq_cleanup_cold_approval"
		durableTurnID  = "evt_cleanup_cold_approval_running"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 7, podUID)
	bridgeStore := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	bridgeStore.RuntimeBindingTokenHMACKey = []byte("cleanup-cold-approval-key-32bytes")
	bridgeStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 45, 0, time.UTC) }
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 7, podUID)
	seedBridgeAPIOpenDurableTurn(t, admin, scope, durableTurnID)
	seedBridgeAPIRequestStart(
		t, bridgeStore, scope, "rwrite_cleanup_cold_approval_start", modelRequestID, "agent_provider_request", 0,
	)
	approvalInputValue := map[string]any{
		"file_path": "src/a.ts",
		"content":   strings.Repeat("界", 3000),
	}
	approvalInputJSON, err := json.Marshal(approvalInputValue)
	if err != nil {
		t.Fatalf("marshal cleanup approval input: %v", err)
	}
	toolUse, err := bridgeStore.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_cleanup_cold_approval_tool", ModelRequestId: modelRequestID,
		ToolDeclaration: bridgeToolDeclarationForTest(
			"tool-call-cleanup-cold-approval", "Write", string(approvalInputJSON), "ask", "sandbox_execute",
		),
	})
	if err != nil || toolUse.GetCommitted() == nil {
		t.Fatalf("write cleanup approval Tool Use: response=%#v err=%v", toolUse, err)
	}
	toolUseEventID := toolUse.GetCommitted().GetEventId()
	if _, err := bridgeStore.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_cleanup_cold_approval_end", ModelRequestId: modelRequestID,
		FinishReason: "tool_calls", UsageJson: `{}`,
		ProviderContextRetention: &bridgev1.ProviderContextRetention{
			Disposition: "completed", AssistantMessageSequence: toolUse.GetCommitted().AssignedMessageSequence,
			ToolUseEventIds: []string{toolUseEventID},
		},
	}); err != nil {
		t.Fatalf("seal cleanup approval request: %v", err)
	}
	if _, err := finishIdleWithStagedCaptureForTest(t, admin, bridgeStore, &bridgev1.FinishIdleRequest{
		Scope: scope, DurableTurnId: durableTurnID,
		StopReasonJson: `{"type":"requires_action","event_ids":["` + toolUseEventID + `"]}`,
	}); err != nil {
		t.Fatalf("finish cleanup approval requires-action turn: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_runtime_status
		    SET cleanup_job_id = 'cleanup_cold_approval_1',
		        cleanup_enqueued_at = '2026-01-01T00:30:00Z'
		  WHERE workspace_id = 'default' AND session_id = $1`, sessionID); err != nil {
		t.Fatalf("mark cleanup approval enqueued: %v", err)
	}

	cleanupStore := jobrunner.NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090, jobrunner.KubernetesRuntimeTargetResolver{GetPod: fixtureConfirmedMissingRuntimePod,
		Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
			return enginekubernetes.NewBindingVisibilitySnapshotStateForTest(
				true,
				enginekubernetes.BoundRuntimePod{
					Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: podUID, PodIP: "10.0.0.10",
				},
				enginekubernetes.BindingVisibilityAbsent,
			)
		},
	})
	cleanupStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 31, 0, 0, time.UTC) }
	job := leaseCleanupRuntimeJobForTest(t, runtime, jobrunner.RuntimeJob{ //nolint:gosec // Test lease token fixture, not a secret.
		Kind: queue.KindCleanupSession, WorkspaceID: "default", SessionID: sessionID,
		RuntimeInputID: "cleanup_session:cleanup_cold_approval_1", CleanupJobID: "cleanup_cold_approval_1",
		PayloadJSON: `{"workspace_id":"default","session_id":"` + sessionID + `","cleanup_job_id":"cleanup_cold_approval_1"}`,
	})
	result, err := (jobrunner.RuntimePodDirectDeliverer{
		Store:  cleanupStore,
		Sender: &recordingRuntimeCommandSender{result: jobrunner.RuntimeDeliveryResult{Status: jobrunner.RuntimeDeliveryAccepted}},
	}).DeliverRuntimeJob(context.Background(), job)
	if err != nil {
		t.Fatalf("deliver proven-gone cleanup approval: %v", err)
	}
	if result.Status != jobrunner.RuntimeDeliveryAccepted {
		t.Fatalf("cleanup approval result = %#v; want accepted", result)
	}

	const (
		recoveryBindingID = "bind_cleanup_cold_approval_recovery"
		recoveryPodUID    = "pod_cleanup_cold_approval_recovery"
	)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, recoveryBindingID, 8, recoveryPodUID)
	recoveryScope := bridgeAPIScope(sessionID, threadID, recoveryBindingID, 8, recoveryPodUID)
	loaded, err := bridgeStore.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope: recoveryScope,
	})
	if err != nil {
		t.Fatalf("LoadContext after ordinary cleanup: %v", err)
	}
	var payload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(loaded.GetContextJson()), &payload); err != nil {
		t.Fatalf("decode cleanup approval context: %v", err)
	}
	if len(payload.PendingToolUses) != 1 ||
		payload.PendingToolUses[0].ToolUseEventID != toolUseEventID ||
		payload.PendingToolUses[0].ModelRequestID != modelRequestID ||
		payload.PendingToolUses[0].Status != "pending" {
		t.Fatalf("pending approval after ordinary cleanup = %#v; want same requires-action member", payload.PendingToolUses)
	}
	if len(payload.PendingSandboxExecutions) != 0 {
		t.Fatalf("sandbox executions after ordinary approval cleanup = %#v; want none", payload.PendingSandboxExecutions)
	}
	foundCleanupPart := false
	for _, entry := range payload.Messages {
		for _, rawPart := range entry.Parts {
			var part struct {
				Type            string         `json:"type"`
				ModelToolCallID string         `json:"modelToolCallId"`
				CanonicalInput  map[string]any `json:"canonicalInput"`
			}
			if err := json.Unmarshal(rawPart, &part); err != nil {
				t.Fatalf("decode cleanup approval context part: %v", err)
			}
			if part.Type != "tool_call" || part.ModelToolCallID != "tool-call-cleanup-cold-approval" {
				continue
			}
			foundCleanupPart = true
			if !reflect.DeepEqual(part.CanonicalInput, approvalInputValue) {
				t.Fatalf("cleanup approval input value changed across LoadContext")
			}
		}
	}
	if !foundCleanupPart {
		t.Fatal("LoadContext omitted cleanup approval tool part")
	}

	setBridgeAPIPendingApprovalStatus(
		t, admin, "default", sessionID, threadID, toolUseEventID, "resolving",
	)
	confirmationEventID := "evt_cleanup_cold_approval_deny"
	confirmationSequence := nextBridgeAPIEventSequenceForTest(t, admin, sessionID, threadID)
	seedBridgeAPIEvent(
		t, admin, "default", sessionID, threadID, confirmationEventID, confirmationSequence,
		"user.tool_confirmation",
		`{"type":"user.tool_confirmation","tool_use_id":"`+toolUseEventID+`","result":"deny","deny_message":"not safe"}`,
	)
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_runtime_inbox (
			workspace_id, session_id, session_thread_id, runtime_input_id, input_kind,
			event_ids_json, sequence_from, sequence_to, status, binding_id, binding_generation,
			target_pod_uid, created_at, updated_at
		) VALUES ('default', $1, $2, 'rin_cleanup_cold_approval_deny', 'tool_confirmation', $3,
			$4, $4, 'accepted', $5, 8, $6, '2026-01-01T00:31:00Z', '2026-01-01T00:31:00Z')`,
		sessionID, threadID, `["`+confirmationEventID+`"]`, confirmationSequence, recoveryBindingID, recoveryPodUID,
	); err != nil {
		t.Fatalf("seed cleanup approval confirmation inbox: %v", err)
	}
	if _, err := bridgeStore.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope: recoveryScope, RuntimeInputId: "rin_cleanup_cold_approval_deny",
	}); err != nil {
		t.Fatalf("commit cleanup approval denial: %v", err)
	}
	terminal, err := bridgeStore.SettleToolResult(context.Background(), bridgeToolSettlementRequestForTest(
		recoveryScope,
		bridgeErrorToolSettlementForTest(toolUseEventID, "Approval denied: not safe"),
	))
	if err != nil {
		t.Fatalf("settle cleanup approval denial result: %v", err)
	}
	bridgeRequireToolSettlementOutcomeForTest(t, terminal, "committed")
	var pendingStatus string
	var resultEventID sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status, result_event_id
		   FROM session_pending_tool_uses
		  WHERE workspace_id = 'default' AND session_id = $1 AND session_thread_id = $2 AND tool_use_event_id = $3`,
		sessionID, threadID, toolUseEventID,
	).Scan(&pendingStatus, &resultEventID); err != nil {
		t.Fatalf("read cleanup approval after denial: %v", err)
	}
	if pendingStatus != "resolved" || !resultEventID.Valid {
		t.Fatalf("cleanup approval settlement = %q/%v; want resolved by a durable Event", pendingStatus, resultEventID)
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
	bridgeStore := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
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

func cleanupTreeJob(sessionID string, cleanupID string) jobrunner.RuntimeJob {
	return jobrunner.RuntimeJob{ //nolint:gosec // Test lease token fixture, not a secret.
		JobID:          "qjob_" + cleanupID,
		LeaseToken:     "lease_" + cleanupID,
		Kind:           queue.KindCleanupSession,
		WorkspaceID:    "default",
		SessionID:      sessionID,
		RuntimeInputID: "cleanup_session:" + cleanupID,
		CleanupJobID:   cleanupID,
		PayloadJSON:    `{"workspace_id":"default","session_id":"` + sessionID + `","cleanup_job_id":"` + cleanupID + `"}`,
	}
}

func leaseCleanupRuntimeJobForTest(t *testing.T, runtime *sql.DB, job jobrunner.RuntimeJob) jobrunner.RuntimeJob {
	t.Helper()
	store := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	jobID := id.New(queue.JobIDPrefix)
	partitionKey := queue.FormatSessionPartitionKey(workspace.DefaultID, job.SessionID)
	dedupeKey := queue.FormatCleanupSessionDedupeKey(workspace.DefaultID, job.SessionID, job.CleanupJobID)
	if _, err := store.Enqueue(context.Background(), queue.EnqueueRequest{
		ID: jobID, WorkspaceID: workspace.DefaultID, Kind: queue.KindCleanupSession,
		PartitionKey: partitionKey, DedupeKey: dedupeKey, PayloadVersion: 1,
		PayloadJSON: []byte(job.PayloadJSON), MaxAttempts: 2,
	}); err != nil {
		t.Fatalf("enqueue cleanup test job: %v", err)
	}
	leased, err := store.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindCleanupSession},
		LeaseOwner: "bridge-cleanup-test", MaxJobs: 1, LeaseDuration: time.Minute,
	})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease cleanup test job = %#v/%v", leased, err)
	}
	job.JobID = leased[0].ID
	job.LeaseToken = leased[0].LeaseToken
	job.PartitionKey = leased[0].PartitionKey
	job.DedupeKey = leased[0].DedupeKey
	job.AttemptCount = int32(leased[0].AttemptCount)
	job.MaxAttempts = int32(leased[0].MaxAttempts)
	return job
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
