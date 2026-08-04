package agentruntimebridge

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
)

// This file owns PostgreSQL delivery-store pod-loss repair tests.

func TestRuntimeRepairOpenRequestDetectionScopesEndsToTheirThread(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_runtime_repair_thread_scope"
		mainThreadID   = "thr_runtime_repair_thread_scope_main"
		childThreadID  = "thr_runtime_repair_thread_scope_child"
		modelRequestID = "mreq_runtime_repair_shared"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainThreadID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainThreadID, childThreadID)
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, model_request_id, projection_json, created_at, updated_at
		) VALUES
		('default', $1, $2, 'evt_repair_main_start', 1, 'span.model_request_start', '{}',
		 'internal', false, $4, '{"context_through_message_sequence":0,"request_kind":"agent_provider_request"}', now(), now()),
		('default', $1, $3, 'evt_repair_child_start', 1, 'span.model_request_start', '{}',
		 'internal', false, $4, '{"context_through_message_sequence":0,"request_kind":"agent_provider_request"}', now(), now()),
		('default', $1, $3, 'evt_repair_child_end', 2, 'span.model_request_end', '{}',
		 'internal', false, $4, '{}', now(), now())`,
		sessionID, mainThreadID, childThreadID, modelRequestID,
	); err != nil {
		t.Fatalf("seed same-request-id sibling events: %v", err)
	}
	client := dbconnect.NewClientForTesting(runtime)
	if err := client.WithWorkspaceTx(context.Background(), "default", "test.pod_loss_thread_scope", func(tx *dbconnect.Tx) error {
		starts, err := runtimePodLostOpenRequestStartsTx(context.Background(), tx, "default", sessionID)
		if err != nil {
			return err
		}
		if len(starts) != 1 || starts[0].SessionThreadID != mainThreadID || starts[0].EventID != "evt_repair_main_start" {
			t.Fatalf("pod-loss open starts = %+v; want only main-thread start", starts)
		}
		return nil
	}); err != nil {
		t.Fatalf("query pod-loss open starts: %v", err)
	}
	if err := client.WithWorkspaceTx(context.Background(), "default", "test.termination_thread_scope", func(tx *dbconnect.Tx) error {
		starts, err := runtimeTerminationOpenRequestStartsTx(
			context.Background(),
			tx,
			bridgeAPIScope(sessionID, mainThreadID, "bind_unused", 1, "pod_unused"),
		)
		if err != nil {
			return err
		}
		if len(starts) != 1 || starts[0].SessionThreadID != mainThreadID || starts[0].EventID != "evt_repair_main_start" {
			t.Fatalf("runtime-termination open starts = %+v; want main-thread start", starts)
		}
		return nil
	}); err != nil {
		t.Fatalf("query runtime-termination open starts: %v", err)
	}
}

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
		 '{"type":"span.model_request_start","model_request_id":"mrq_pod_loss","context_through_message_sequence":0,"request_kind":"agent_provider_request"}',
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
		 'internal', false, 'mrq_pod_loss_closed',
		 '{"context_through_message_sequence":1,"request_kind":"agent_provider_request"}',
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

func TestRuntimePodLossSettlesMCPToolNamedLikeSubAgentToolWithoutConnectorReplay(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_mcp_pod_loss"
		threadID       = "thr_mcp_pod_loss"
		modelRequestID = "mreq_mcp_pod_loss"
		toolUseEventID = "evt_mcp_pod_loss_tool"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_mcp_pod_loss", 1, "pod_mcp_pod_loss")
	seedRuntimePodLostStatusFence(t, admin, sessionID, "bind_mcp_pod_loss", 1)
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, model_request_id, projection_json, created_at, updated_at
		) VALUES
		('default', $1, $2, 'evt_mcp_pod_loss_start', 1, 'span.model_request_start',
		 '{"type":"span.model_request_start","model_request_id":"mreq_mcp_pod_loss","request_kind":"agent_provider_request"}',
		 'internal', false, $3,
		 '{"context_through_message_sequence":0,"request_kind":"agent_provider_request"}',
		 '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'),
		('default', $1, $2, $4, 2, 'agent.mcp_tool_use',
		 '{"type":"agent.mcp_tool_use","name":"spawn_agent","mcp_server_name":"github","input":{"q":"x"},"evaluated_permission":"allow"}',
		 'public', true, $3, '{}', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		sessionID, threadID, modelRequestID, toolUseEventID,
	); err != nil {
		t.Fatalf("seed MCP pod-loss events: %v", err)
	}
	seedBridgeAPIDurableToolMessage(t, admin, "default", sessionID, threadID, modelRequestID, toolUseEventID, "call_mcp_pod_loss", "spawn_agent")
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_messages
		    SET data_json = jsonb_set(
		      data_json::jsonb,
		      '{parts,0,toolEvent}',
		      '{"kind":"mcp","mcpServerName":"github"}'::jsonb
		    )::text
		  WHERE workspace_id = 'default' AND session_id = $1 AND message_id = $2`,
		sessionID, "msg_"+toolUseEventID,
	); err != nil {
		t.Fatalf("stamp MCP tool projection: %v", err)
	}
	binding := runtimeBindingForDelivery{BindingID: "bind_mcp_pod_loss", BindingGeneration: 1, PodUID: "pod_mcp_pod_loss"}
	repaired, err := runRuntimePodLostRepairTransaction(
		context.Background(), runtime, sessionID, binding, time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("repair MCP pod loss: %v", err)
	}
	if repaired < 2 {
		t.Fatalf("repaired facts = %d; want at least Request End plus MCP Tool Result", repaired)
	}
	var resultEventID, payloadJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT event_id, payload_json
		   FROM session_events
		  WHERE workspace_id = 'default' AND session_id = $1
		    AND type = 'agent.mcp_tool_result'
		    AND payload_json::jsonb ->> 'mcp_tool_use_id' = $2`,
		sessionID, toolUseEventID,
	).Scan(&resultEventID, &payloadJSON); err != nil {
		t.Fatalf("read MCP pod-loss result: %v", err)
	}
	if !strings.Contains(payloadJSON, `"reason":"runtime_pod_lost"`) || !strings.Contains(payloadJSON, `"is_error":true`) {
		t.Fatalf("MCP pod-loss result = %s; want terminal runtime_pod_lost error", payloadJSON)
	}
	var lastEventID, messageJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT last_event_id, data_json FROM session_messages
		  WHERE workspace_id = 'default' AND session_id = $1 AND message_id = $2`,
		sessionID, "msg_"+toolUseEventID,
	).Scan(&lastEventID, &messageJSON); err != nil {
		t.Fatalf("read repaired MCP message: %v", err)
	}
	if lastEventID != resultEventID || !strings.Contains(messageJSON, `"status":"error"`) ||
		!strings.Contains(messageJSON, `"retryable":false`) {
		t.Fatalf("repaired MCP message last event/data = %q/%s; want non-retryable terminal error", lastEventID, messageJSON)
	}
	if _, err := runRuntimePodLostRepairTransaction(
		context.Background(), runtime, sessionID, binding, time.Date(2026, 1, 1, 0, 6, 0, 0, time.UTC),
	); err != nil {
		t.Fatalf("replay MCP pod-loss repair: %v", err)
	}
	var resultCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events
		  WHERE workspace_id = 'default' AND session_id = $1
		    AND type = 'agent.mcp_tool_result'
		    AND payload_json::jsonb ->> 'mcp_tool_use_id' = $2`,
		sessionID, toolUseEventID,
	).Scan(&resultCount); err != nil {
		t.Fatalf("count MCP pod-loss results: %v", err)
	}
	if resultCount != 1 {
		t.Fatalf("MCP pod-loss result count = %d; want exactly one", resultCount)
	}
}

func TestRuntimePodLossDetectsInternalApprovalReviewerToolUse(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_reviewer_pod_loss"
		mainThreadID   = "thr_reviewer_pod_loss_main"
		reviewerID     = "thr_reviewer_pod_loss"
		modelRequestID = "mreq_reviewer_pod_loss"
		toolUseEventID = "evt_reviewer_pod_loss_tool"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainThreadID)
	seedBridgeAPIInternalReviewerThread(t, admin, "default", sessionID, mainThreadID, reviewerID)
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, model_request_id, projection_json, created_at, updated_at
		) VALUES
		('default', $1, $2, 'evt_reviewer_pod_loss_start', 1, 'span.model_request_start',
		 '{"type":"span.model_request_start","model_request_id":"mreq_reviewer_pod_loss","request_kind":"approval_reviewer"}',
		 'internal', false, $3,
		 '{"context_through_message_sequence":0,"request_kind":"approval_reviewer"}',
		 '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'),
		('default', $1, $2, $4, 2, 'agent.tool_use',
		 '{"type":"agent.tool_use","name":"Read","input":{"file_path":"README.md"},"evaluated_permission":"allow"}',
		 'internal', false, $3, '{}',
		 '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		sessionID, reviewerID, modelRequestID, toolUseEventID,
	); err != nil {
		t.Fatalf("seed reviewer pod-loss events: %v", err)
	}
	seedBridgeAPIDurableToolMessage(
		t, admin, "default", sessionID, reviewerID, modelRequestID,
		toolUseEventID, "call_reviewer_pod_loss", "Read",
	)

	client := dbconnect.NewClientForTesting(runtime)
	var orphans []runtimeOrphanToolUse
	if err := client.WithWorkspaceTx(context.Background(), "default", "test.reviewer_pod_loss", func(tx *dbconnect.Tx) error {
		var err error
		orphans, err = runtimePodLostOrphanToolUsesTx(context.Background(), tx, "default", sessionID)
		return err
	}); err != nil {
		t.Fatalf("load reviewer pod-loss Tool Uses: %v", err)
	}
	if len(orphans) != 1 || orphans[0].EventID != toolUseEventID || orphans[0].SessionThreadID != reviewerID {
		t.Fatalf("reviewer orphan Tool Uses = %+v; want internal reviewer Read", orphans)
	}
}

func TestRuntimePodLossOrphanDetectionKeepsToolFamilyAndThreadClosed(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_pod_loss_closed_result_identity"
		threadID  = "thr_pod_loss_closed_result_identity"
		otherID   = "thr_pod_loss_closed_result_other"
		toolUseID = "evt_pod_loss_closed_result_tool"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_threads (
			workspace_id, id, session_id, parent_thread_id, role, visibility, status,
			agent_type, title, task_name, is_trunk, created_at, last_active_at, updated_at
		) VALUES ('default', $1, $2, $3, 'subagent', 'public', 'idle',
			'worker', 'worker', 'worker', false, NOW(), NOW(), NOW())`,
		otherID, sessionID, threadID,
	); err != nil {
		t.Fatalf("seed sibling thread: %v", err)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, toolUseID, 1, "agent.tool_use",
		`{"type":"agent.tool_use","name":"Read","input":{},"evaluated_permission":"allow"}`)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_events SET model_request_id = 'mreq_closed_result_identity'
		  WHERE workspace_id = 'default' AND session_id = $1 AND session_thread_id = $2 AND event_id = $3`,
		sessionID, threadID, toolUseID,
	); err != nil {
		t.Fatalf("stamp Tool Use request identity: %v", err)
	}
	seedBridgeAPIDurableToolMessage(t, admin, "default", sessionID, threadID, "mreq_closed_result_identity", toolUseID, "call_closed_result_identity", "Read")
	seedBridgeAPIEvent(t, admin, "default", sessionID, otherID, "evt_pod_loss_wrong_family_result", 1, "agent.mcp_tool_result",
		`{"type":"agent.mcp_tool_result","mcp_tool_use_id":"`+toolUseID+`","is_error":false}`)

	client := dbconnect.NewClientForTesting(runtime)
	var orphans []runtimeOrphanToolUse
	if err := client.WithWorkspaceTx(context.Background(), "default", "test.pod_loss_closed_result_identity", func(tx *dbconnect.Tx) error {
		var err error
		orphans, err = runtimePodLostOrphanToolUsesTx(context.Background(), tx, "default", sessionID)
		return err
	}); err != nil {
		t.Fatalf("load orphan Tool Uses: %v", err)
	}
	if len(orphans) != 1 || orphans[0].EventID != toolUseID || orphans[0].SessionThreadID != threadID {
		t.Fatalf("orphan Tool Uses = %+v; want ordinary Tool Use preserved across cross-Thread MCP result", orphans)
	}
}

func TestRuntimePodLossUsesDurablePrivateRequestKind(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_compaction_pod_loss"
		threadID       = "thr_compaction_pod_loss"
		modelRequestID = "mreq_compaction_pod_loss"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_compaction_pod_loss", 1, "pod_compaction_pod_loss")
	seedRuntimePodLostStatusFence(t, admin, sessionID, "bind_compaction_pod_loss", 1)
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, model_request_id, projection_json, created_at, updated_at
		) VALUES (
			'default', $1, $2, 'evt_compaction_pod_loss_start', 1, 'span.model_request_start',
			'{"type":"span.model_request_start","model_request_id":"mreq_compaction_pod_loss"}',
			'internal', false, $3,
			'{"context_through_message_sequence":0,"request_kind":"compaction_summary"}',
			'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		)`,
		sessionID, threadID, modelRequestID,
	); err != nil {
		t.Fatalf("seed compaction Request Start: %v", err)
	}
	binding := runtimeBindingForDelivery{
		BindingID: "bind_compaction_pod_loss", BindingGeneration: 1, PodUID: "pod_compaction_pod_loss",
	}
	if _, err := runRuntimePodLostRepairTransaction(
		context.Background(), runtime, sessionID, binding, time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC),
	); err != nil {
		t.Fatalf("repair compaction pod loss: %v", err)
	}
	var payloadJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT payload_json FROM session_events
		  WHERE workspace_id = 'default' AND session_id = $1 AND session_thread_id = $2
		    AND model_request_id = $3 AND type = 'span.model_request_end'`,
		sessionID, threadID, modelRequestID,
	).Scan(&payloadJSON); err != nil {
		t.Fatalf("read repaired compaction Request End: %v", err)
	}
	if !strings.Contains(payloadJSON, `"request_kind":"compaction_summary"`) {
		t.Fatalf("repaired Request End = %s; want durable compaction request kind", payloadJSON)
	}
}

func TestRuntimePodLossPreservesToolUseAwaitingApproval(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_pod_loss_pending_approval"
		threadID       = "thr_pod_loss_pending_approval"
		modelRequestID = "mreq_pod_loss_pending_approval"
		toolUseEventID = "evt_pod_loss_pending_approval_tool"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, model_request_id, projection_json, created_at, updated_at
		) VALUES
		('default', $1, $2, 'evt_pod_loss_pending_approval_start', 1, 'span.model_request_start',
		 '{"type":"span.model_request_start","model_request_id":"mreq_pod_loss_pending_approval"}',
		 'internal', false, $3, '{"context_through_message_sequence":0,"request_kind":"agent_provider_request"}', now(), now()),
		('default', $1, $2, $4, 2, 'agent.tool_use',
		 '{"type":"agent.tool_use","name":"Write","input":{"file_path":"src/a.ts"},"evaluated_permission":"ask"}',
		 'public', true, $3, '{}', now(), now()),
		('default', $1, $2, 'evt_pod_loss_pending_approval_end', 3, 'span.model_request_end',
		 '{"type":"span.model_request_end","model_request_id":"mreq_pod_loss_pending_approval","finish_reason":"tool_calls"}',
		 'internal', false, $3, '{}', now(), now())`,
		sessionID, threadID, modelRequestID, toolUseEventID,
	); err != nil {
		t.Fatalf("seed pending-approval request: %v", err)
	}
	seedBridgeAPIDurableToolMessage(
		t, admin, "default", sessionID, threadID, modelRequestID,
		toolUseEventID, "tool-call-pending-approval", "Write",
	)
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_pending_tool_uses (
			workspace_id, session_id, session_thread_id, tool_use_event_id, model_tool_call_id,
			tool_name, kind, input_json, status, expires_at, created_at, updated_at
		) VALUES ('default', $1, $2, $3, 'tool-call-pending-approval', 'Write', 'approval',
			'{"file_path":"src/a.ts"}', 'pending', now() + interval '30 minutes', now(), now())`,
		sessionID, threadID, toolUseEventID,
	); err != nil {
		t.Fatalf("seed pending approval: %v", err)
	}

	client := dbconnect.NewClientForTesting(runtime)
	if err := client.WithWorkspaceTx(context.Background(), "default", "test.pod_loss_pending_approval", func(tx *dbconnect.Tx) error {
		toolUses, err := runtimePodLostOrphanToolUsesTx(context.Background(), tx, "default", sessionID)
		if err != nil {
			return err
		}
		if len(toolUses) != 0 {
			t.Fatalf("pod-loss orphan Tool Uses = %+v; want pending approval preserved", toolUses)
		}
		return nil
	}); err != nil {
		t.Fatalf("query pod-loss orphan Tool Uses: %v", err)
	}
}

func TestRuntimePodLossRejectsMissingPrivateRequestKind(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_missing_kind_pod_loss"
		threadID  = "thr_missing_kind_pod_loss"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_missing_kind_pod_loss", 1, "pod_missing_kind_pod_loss")
	seedRuntimePodLostStatusFence(t, admin, sessionID, "bind_missing_kind_pod_loss", 1)
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, model_request_id, projection_json, created_at, updated_at
		) VALUES (
			'default', $1, $2, 'evt_missing_kind_pod_loss_start', 1, 'span.model_request_start',
			'{"type":"span.model_request_start","model_request_id":"mreq_missing_kind_pod_loss"}',
			'internal', false, 'mreq_missing_kind_pod_loss', '{}',
			'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		)`, sessionID, threadID,
	); err != nil {
		t.Fatalf("seed malformed Request Start: %v", err)
	}
	_, err := runRuntimePodLostRepairTransaction(
		context.Background(), runtime, sessionID,
		runtimeBindingForDelivery{BindingID: "bind_missing_kind_pod_loss", BindingGeneration: 1, PodUID: "pod_missing_kind_pod_loss"},
		time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC),
	)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("missing private request kind err = %v; want FailedPrecondition", err)
	}
}
