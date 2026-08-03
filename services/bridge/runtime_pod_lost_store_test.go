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

// This file owns PostgreSQL delivery-store pod-loss repair tests.

func TestPostgreSQLRuntimeDeliveryStoreRepairsLostRuntimePodBeforeBindingReplacement(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_pod_loss", "thr_bridge_pod_loss")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_pod_loss", "bind_bridge_pod_loss_old", 7, "pod_uid_pod_loss_old")
	seedRuntimePodLostStatusFence(t, admin, "sesn_bridge_pod_loss", "bind_bridge_pod_loss_old", 7)
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, model_request_id, projection_json, created_at, updated_at
		) VALUES
		('default', 'sesn_bridge_pod_loss', 'thr_bridge_pod_loss', 'evt_pod_loss_start', 1, 'span.model_request_start',
		 '{}',
		 'internal', false, 'mrq_pod_loss',
		 '{"type":"span.model_request_start","model_request_id":"mrq_pod_loss","request_kind":"agent_provider_request"}',
		 '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'),
		('default', 'sesn_bridge_pod_loss', 'thr_bridge_pod_loss', 'evt_pod_loss_tool', 2, 'agent.tool_use',
		 '{"type":"agent.tool_use","name":"Write","input":{"file_path":"src/a.ts"},"evaluated_permission":"ask"}',
		 'public', true, 'mrq_pod_loss',
		 '{}',
		'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`); err != nil {
		t.Fatalf("seed lost request events: %v", err)
	}
	seedBridgeAPIDurableToolMessage(
		t,
		admin,
		"default",
		"sesn_bridge_pod_loss",
		"thr_bridge_pod_loss",
		"mrq_pod_loss",
		"evt_pod_loss_tool",
		"tool-call-pod-loss",
		"Write",
	)
	attachmentStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	attachmentStore.AttachmentBlobStore = blob.NewFakeBlobStore()
	attachmentStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }
	attachment := createBridgeTransientAttachmentForTest(
		t, attachmentStore,
		bridgeAPIScope("sesn_bridge_pod_loss", "thr_bridge_pod_loss", "bind_bridge_pod_loss_old", 7, "pod_uid_pod_loss_old"),
		"attachment_pod_loss", "evt_pod_loss_tool", []byte("pod-loss-attachment"),
	)
	resultJSON := `{"status":"success","attachment_ref":"` + attachment.GetAttachmentRef() + `"}`
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_transient_attachments
		SET status='staged', expires_at='2026-01-01T00:01:00Z'
		WHERE workspace_id='default' AND attachment_ref=$1`, attachment.GetAttachmentRef()); err != nil {
		t.Fatalf("stage pod-loss attachment: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_tool_results (
		workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
		normalized_input_hash, tool_name, input_json, ack_status, result_json,
		model_tool_call_id, execution_state, execution_attempt_generation, result_digest,
		created_at, updated_at
	) VALUES ('default','sesn_bridge_pod_loss','thr_bridge_pod_loss','evt_pod_loss_tool','sandbox_tool',
		$1,'Write','{"file_path":"src/a.ts"}','committed',$2,
		'tool-call-pod-loss','terminal_unconsumed',1,$3,
		'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`,
		sha256Hex(`{"file_path":"src/a.ts"}`), resultJSON, sha256Hex(resultJSON)); err != nil {
		t.Fatalf("seed pod-loss sandbox execution: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_pending_tool_uses (
			workspace_id, session_id, session_thread_id, tool_use_event_id, model_tool_call_id,
			tool_name, kind, input_json, status, expires_at, created_at, updated_at
		) VALUES (
			'default', 'sesn_bridge_pod_loss', 'thr_bridge_pod_loss', 'evt_pod_loss_tool', 'tool-call-pod-loss',
			'Write', 'approval', '{"file_path":"src/a.ts"}', 'resolving',
			'2026-01-01T00:30:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		)`); err != nil {
		t.Fatalf("seed lost pending approval: %v", err)
	}
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_pod_loss", "thr_bridge_pod_loss", "evt_pod_loss_later", 3, "user.message", `{"content":[{"type":"text","text":"after pod loss"}]}`)
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, model_request_id, projection_json, created_at, updated_at
		) VALUES
		('default', 'sesn_bridge_pod_loss', 'thr_bridge_pod_loss', 'evt_pod_loss_closed_start', 4, 'span.model_request_start',
		 '{"type":"span.model_request_start","model_request_id":"mrq_pod_loss_closed","request_kind":"agent_provider_request"}',
		 'internal', false, 'mrq_pod_loss_closed', '{}',
		 '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'),
		('default', 'sesn_bridge_pod_loss', 'thr_bridge_pod_loss', 'evt_pod_loss_closed_tool', 5, 'agent.tool_use',
		 '{"type":"agent.tool_use","name":"Read","input":{"file_path":"src/b.ts"},"evaluated_permission":"allow"}',
		 'public', false, 'mrq_pod_loss_closed', '{}',
		 '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'),
		('default', 'sesn_bridge_pod_loss', 'thr_bridge_pod_loss', 'evt_pod_loss_closed_end', 6, 'span.model_request_end',
		 '{"type":"span.model_request_end","model_request_id":"mrq_pod_loss_closed","finish_reason":"tool_calls"}',
		 'internal', false, 'mrq_pod_loss_closed', '{}',
		 '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`); err != nil {
		t.Fatalf("seed closed request with running tool: %v", err)
	}
	seedBridgeAPIDurableToolMessage(
		t,
		admin,
		"default",
		"sesn_bridge_pod_loss",
		"thr_bridge_pod_loss",
		"mrq_pod_loss_closed",
		"evt_pod_loss_closed_tool",
		"tool-call-pod-loss-closed",
		"Read",
	)

	oldBound := enginekubernetes.BoundRuntimePod{
		Namespace: "tetral-agent-runtime",
		PodName:   "runtime-pod-0",
		PodUID:    "pod_uid_pod_loss_old",
		PodIP:     "10.0.0.10",
	}
	newCandidate := enginekubernetes.BindingCandidate{
		Namespace: "tetral-agent-runtime",
		PodName:   "runtime-pod-1",
		PodUID:    "pod_uid_pod_loss_new",
		PodIP:     "10.0.0.11",
	}
	snapshot := enginekubernetes.NewBindingVisibilitySnapshotStateWithCandidatesForTest(
		true,
		oldBound,
		enginekubernetes.BindingVisibilityDeleted,
		[]enginekubernetes.BindingCandidate{newCandidate},
	)
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 4, 4, 0, time.UTC) }
	store.TargetResolver = KubernetesRuntimeTargetResolver{
		Snapshot: func() enginekubernetes.BindingVisibilitySnapshot { return snapshot },
		Clock:    store.Clock,
	}
	plan, err := store.PrepareRuntimeCommand(context.Background(), RuntimeJob{
		JobID:           "qjob_pod_loss_later",
		LeaseToken:      "lease_pod_loss_later",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       "sesn_bridge_pod_loss",
		SessionThreadID: "thr_bridge_pod_loss",
		RuntimeInputID:  "rin_pod_loss_later",
		EventIDs:        []string{"evt_pod_loss_later"},
		SequenceFrom:    3,
		SequenceTo:      3,
		InputKind:       "messages",
		CommandKind:     agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:     `{"workspace_id":"default","session_id":"sesn_bridge_pod_loss","session_thread_id":"thr_bridge_pod_loss","runtime_input_id":"rin_pod_loss_later","event_ids":["evt_pod_loss_later"],"sequence_from":3,"sequence_to":3,"input_kind":"messages"}`,
	})
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand after pod loss: %v", err)
	}
	if plan.Request == nil || plan.Request.GetTargetPodUid() != "pod_uid_pod_loss_new" {
		t.Fatalf("plan target pod uid = %#v; want replacement pod", plan.Request)
	}

	var requestEndPayload string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT payload_json
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_pod_loss'
		    AND type = 'span.model_request_end'
		    AND model_request_id = 'mrq_pod_loss'`).Scan(&requestEndPayload); err != nil {
		t.Fatalf("read pod-loss request end: %v", err)
	}
	if !strings.Contains(requestEndPayload, `"error_kind":"runtime_pod_lost"`) {
		t.Fatalf("pod-loss request end payload = %s; want runtime_pod_lost", requestEndPayload)
	}
	var toolResultEventID string
	var toolResultPayload string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT event_id, payload_json
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_pod_loss'
		    AND type = 'agent.tool_result'
		    AND payload_json::jsonb ->> 'tool_use_event_id' = 'evt_pod_loss_tool'`).Scan(&toolResultEventID, &toolResultPayload); err != nil {
		t.Fatalf("read pod-loss tool result: %v", err)
	}
	if !strings.Contains(toolResultPayload, `"reason":"runtime_pod_lost"`) || !strings.Contains(toolResultPayload, `"is_error":true`) {
		t.Fatalf("pod-loss tool result payload = %s; want model-visible runtime_pod_lost error", toolResultPayload)
	}
	var closedToolResultCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_pod_loss'
		    AND type = 'agent.tool_result'
		    AND payload_json::jsonb ->> 'tool_use_event_id' = 'evt_pod_loss_closed_tool'`).Scan(&closedToolResultCount); err != nil {
		t.Fatalf("read closed-request pod-loss tool result: %v", err)
	}
	if closedToolResultCount != 1 {
		t.Fatalf("closed-request pod-loss tool result count = %d; want 1", closedToolResultCount)
	}
	var pendingStatus string
	var resultEventID sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status, result_event_id
		   FROM session_pending_tool_uses
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_pod_loss'
		    AND tool_use_event_id = 'evt_pod_loss_tool'`).Scan(&pendingStatus, &resultEventID); err != nil {
		t.Fatalf("read pod-loss pending approval: %v", err)
	}
	if pendingStatus != "cancelled" || !resultEventID.Valid || resultEventID.String != toolResultEventID {
		t.Fatalf("pod-loss pending status/result = %q/%v; want cancelled with repair result", pendingStatus, resultEventID)
	}
	var messageCount int
	var messageData string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*), COALESCE(max(data_json), '')
		   FROM session_messages
		  WHERE workspace_id = 'default'
		    AND source_event_id = 'evt_pod_loss_tool'
		    AND last_event_id = $1`,
		toolResultEventID).Scan(&messageCount, &messageData); err != nil {
		t.Fatalf("read pod-loss terminal tool message: %v", err)
	}
	if messageCount != 1 || !strings.Contains(messageData, `"status":"completed"`) ||
		!strings.Contains(messageData, `"status":"error"`) ||
		!strings.Contains(messageData, `"message":"Tool result unavailable because the runtime pod was lost."`) {
		t.Fatalf("pod-loss terminal tool messages = %d/%s; want one repaired durable message", messageCount, messageData)
	}
	var boundPodUID string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT agent_runtime_pod_uid
		   FROM session_runtime_bindings
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_pod_loss'`).Scan(&boundPodUID); err != nil {
		t.Fatalf("read replacement binding: %v", err)
	}
	if boundPodUID != "pod_uid_pod_loss_new" {
		t.Fatalf("replacement binding pod uid = %q; want new pod", boundPodUID)
	}
	var executionState, consumptionReason string
	var storedResult sql.NullString
	if err := admin.QueryRowContext(context.Background(), `SELECT execution_state, result_json, consumption_reason
		FROM session_runtime_tool_results
		WHERE workspace_id='default' AND session_id='sesn_bridge_pod_loss' AND tool_use_event_id='evt_pod_loss_tool'`).Scan(
		&executionState, &storedResult, &consumptionReason,
	); err != nil {
		t.Fatalf("read pod-loss execution receipt: %v", err)
	}
	if executionState != "consumed" || storedResult.Valid || consumptionReason != "pod_lost" {
		t.Fatalf("pod-loss execution = %q/%v/%q; want consumed thin receipt", executionState, storedResult, consumptionReason)
	}
	attachmentStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC) }
	if result, err := attachmentStore.ReconcileTransientAttachments(context.Background(), 10); err != nil || result.Deleted != 1 {
		t.Fatalf("reconcile pod-loss attachment = %+v, %v; want one deleted", result, err)
	}
	if got := bridgeTransientAttachmentStatus(t, admin, attachment.GetAttachmentRef()); got != "deleted" {
		t.Fatalf("pod-loss attachment status = %q; want deleted", got)
	}
	var inboxStatus string
	var inboxPodUID string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status, target_pod_uid
		   FROM session_runtime_inbox
		  WHERE workspace_id = 'default'
		    AND runtime_input_id = 'rin_pod_loss_later'`).Scan(&inboxStatus, &inboxPodUID); err != nil {
		t.Fatalf("read replacement inbox: %v", err)
	}
	if inboxStatus != "delivering" || inboxPodUID != "pod_uid_pod_loss_new" {
		t.Fatalf("replacement inbox status/pod = %q/%q; want delivering on new pod", inboxStatus, inboxPodUID)
	}
}
