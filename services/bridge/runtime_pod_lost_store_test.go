package agentruntimebridge

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

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
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_pod_loss", "prep_bridge_pod_loss")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_pod_loss", "2026-01-01T00:04:00Z")
	seedRuntimePodLostStatusFence(t, admin, "sesn_bridge_pod_loss", "bind_bridge_pod_loss_old", 7)
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, model_request_id, projection_json, created_at, updated_at
		) VALUES
		('default', 'sesn_bridge_pod_loss', 'thr_bridge_pod_loss', 'evt_pod_loss_start', 1, 'span.model_request_start',
		 '{"type":"span.model_request_start","model_request_id":"mrq_pod_loss","request_kind":"agent_provider_request"}',
		 'internal', false, 'mrq_pod_loss',
		 '{"type":"span.model_request_start","model_request_id":"mrq_pod_loss","request_kind":"agent_provider_request"}',
		 '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'),
		('default', 'sesn_bridge_pod_loss', 'thr_bridge_pod_loss', 'evt_pod_loss_tool', 2, 'agent.tool_use',
		 '{"type":"agent.tool_use","name":"Write","input":{"file_path":"src/a.ts"},"evaluated_permission":"ask"}',
		 'public', true, 'mrq_pod_loss',
		 '{"type":"runtime_tool_projection","model_tool_call_id":"tool-call-pod-loss","tool_name":"Write","input":{"file_path":"src/a.ts"},"state":"running"}',
		 '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`); err != nil {
		t.Fatalf("seed lost request events: %v", err)
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
	seedBridgeAPIBackgroundTask(t, admin, "default", "sesn_bridge_pod_loss", "thr_bridge_pod_loss", "bind_bridge_pod_loss_old", "task_pod_loss_running", "evt_pod_loss_bg_tool")

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
	releaser := &recordingSandboxReleaseClient{
		result: SandboxReleaseResult{Status: SandboxReleaseReleased, SandboxStatus: "released"},
		beforeRelease: func(request SandboxReleaseRequest) error {
			var status string
			var bindingCount int
			var runtimeStatus string
			var exhaustedErrorCount, exhaustedIdleCount int
			if err := admin.QueryRowContext(context.Background(),
				`SELECT status FROM session_background_tasks
				  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_pod_loss' AND task_id = 'task_pod_loss_running'`).Scan(&status); err != nil {
				return err
			}
			if err := admin.QueryRowContext(context.Background(),
				`SELECT count(*) FROM session_runtime_bindings
				  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_pod_loss' AND binding_id = 'bind_bridge_pod_loss_old'`).Scan(&bindingCount); err != nil {
				return err
			}
			if err := admin.QueryRowContext(context.Background(),
				`SELECT status FROM session_runtime_status
				  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_pod_loss'`).Scan(&runtimeStatus); err != nil {
				return err
			}
			if err := admin.QueryRowContext(context.Background(),
				`SELECT
				   count(*) FILTER (WHERE type = 'session.error' AND payload_json::jsonb #>> '{error,retry_status,type}' = 'exhausted'),
				   count(*) FILTER (WHERE type = 'session.status_idle' AND payload_json::jsonb #>> '{stop_reason,type}' = 'retries_exhausted')
				 FROM session_events
				 WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_pod_loss'`).Scan(&exhaustedErrorCount, &exhaustedIdleCount); err != nil {
				return err
			}
			if status != "cancelled_by_cleanup" || bindingCount != 1 || runtimeStatus != "idle" || exhaustedErrorCount != 1 || exhaustedIdleCount != 1 {
				return fmt.Errorf("release ordering observed task=%s binding_count=%d runtime=%s exhausted_error=%d exhausted_idle=%d", status, bindingCount, runtimeStatus, exhaustedErrorCount, exhaustedIdleCount)
			}
			return nil
		},
	}
	store.SandboxReleaser = releaser
	plan, err := store.PrepareRuntimeCommand(context.Background(), RuntimeJob{
		JobID:                "qjob_pod_loss_later",
		LeaseToken:           "lease_pod_loss_later",
		Kind:                 queue.KindRuntimeInput,
		WorkspaceID:          "default",
		SessionID:            "sesn_bridge_pod_loss",
		PreparationAttemptID: "prep_bridge_pod_loss",
		SessionThreadID:      "thr_bridge_pod_loss",
		RuntimeInputID:       "rin_pod_loss_later",
		EventIDs:             []string{"evt_pod_loss_later"},
		SequenceFrom:         3,
		SequenceTo:           3,
		InputKind:            "messages",
		CommandKind:          agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJSON:          `{"workspace_id":"default","session_id":"sesn_bridge_pod_loss","session_thread_id":"thr_bridge_pod_loss","runtime_input_id":"rin_pod_loss_later","event_ids":["evt_pod_loss_later"],"sequence_from":3,"sequence_to":3,"input_kind":"messages"}`,
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
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_messages
		  WHERE workspace_id = 'default'
		    AND source_event_id = $1`,
		toolResultEventID).Scan(&messageCount); err != nil {
		t.Fatalf("read pod-loss tool result projection: %v", err)
	}
	if messageCount != 1 {
		t.Fatalf("pod-loss tool result messages = %d; want one projection", messageCount)
	}
	var taskStatus string
	var taskTerminalEventID sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status, terminal_event_id
		   FROM session_background_tasks
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_pod_loss'
		    AND task_id = 'task_pod_loss_running'`).Scan(&taskStatus, &taskTerminalEventID); err != nil {
		t.Fatalf("read pod-loss background task: %v", err)
	}
	if taskStatus != "cancelled_by_cleanup" || !taskTerminalEventID.Valid {
		t.Fatalf("pod-loss background task status/event = %q/%v; want cancelled_by_cleanup with terminal event", taskStatus, taskTerminalEventID)
	}
	var taskNotificationPayload string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT payload_json
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_pod_loss'
		    AND event_id = $1
		    AND type = 'runtime_notification'`,
		taskTerminalEventID.String).Scan(&taskNotificationPayload); err != nil {
		t.Fatalf("read pod-loss task notification: %v", err)
	}
	if !strings.Contains(taskNotificationPayload, `"task_id":"task_pod_loss_running"`) || !strings.Contains(taskNotificationPayload, `"status":"cancelled"`) {
		t.Fatalf("pod-loss task notification payload = %s; want cleanup-cancelled task notification", taskNotificationPayload)
	}
	if len(releaser.requests) != 1 || releaser.requests[0].Reason != "runtime_pod_lost" || releaser.requests[0].BindingID != "bind_bridge_pod_loss_old" {
		t.Fatalf("pod-loss sandbox release requests = %+v; want one old-binding release", releaser.requests)
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
