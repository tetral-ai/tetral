package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxrelease "github.com/tetral-ai/tetral/internal/sandbox/release"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
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
		starts, err := runtimePodLostOpenRequestStartsTx(context.Background(), tx, "default", sessionID, []string{mainThreadID, childThreadID})
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
	seedBridgeAPIChildThread(t, admin, "default", "sesn_bridge_pod_loss", "thr_bridge_pod_loss", "thr_bridge_pod_loss_closed")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_pod_loss", "bind_bridge_pod_loss_old", 7, "pod_uid_pod_loss_old")
	seedRuntimePodLostStatusFence(t, admin, "sesn_bridge_pod_loss", "bind_bridge_pod_loss_old", 7)
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, model_request_id, projection_json, created_at, updated_at
		) VALUES
		('default', 'sesn_bridge_pod_loss', 'thr_bridge_pod_loss', 'evt_pod_loss_start', 5, 'span.model_request_start',
		 '{}',
		 'internal', false, 'mrq_pod_loss',
		 '{"type":"span.model_request_start","model_request_id":"mrq_pod_loss","context_through_message_sequence":0,"request_kind":"agent_provider_request"}',
		 '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'),
		('default', 'sesn_bridge_pod_loss', 'thr_bridge_pod_loss', 'evt_pod_loss_tool', 6, 'agent.tool_use',
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
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_messages
		SET data_json = '{"parts":[{"type":"text","text":"committed text survives Pod loss"},{"type":"reasoning","text":"committed reasoning survives Pod loss","providerMetadata":{"anthropic":{"signature":"sig_pod_loss"}}},{"type":"tool_call","modelToolCallId":"tool-call-settled-before-pod-loss","toolName":"Read","canonicalInput":{"file_path":"src/b.ts"}},{"type":"tool_result","modelToolCallId":"tool-call-settled-before-pod-loss","result":{"type":"completed","output":{"text":"already durable"}}},{"type":"tool_call","modelToolCallId":"tool-call-pod-loss","toolName":"Write","canonicalInput":{"file_path":"src/a.ts"}}]}'
		WHERE workspace_id='default' AND session_id='sesn_bridge_pod_loss'
		  AND session_thread_id='thr_bridge_pod_loss' AND model_request_id='mrq_pod_loss'`); err != nil {
		t.Fatalf("seed committed Pod-loss Assistant output: %v", err)
	}
	attachmentStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	attachmentStore.AttachmentBlobStore = blob.NewFakeBlobStore()
	attachmentStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }
	attachment := createBridgeTransientAttachmentForTest(
		t, attachmentStore,
		bridgeAPIScope("sesn_bridge_pod_loss", "thr_bridge_pod_loss", "bind_bridge_pod_loss_old", 7, "pod_uid_pod_loss_old"),
		"attachment_pod_loss", "evt_pod_loss_tool", []byte("pod-loss-attachment"),
	)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_transient_attachments
		SET status='staged', expires_at='2026-01-01T00:01:00Z'
		WHERE workspace_id='default' AND attachment_ref=$1`, attachment.GetAttachmentRef()); err != nil {
		t.Fatalf("stage pod-loss attachment: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_tool_results (
		workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
		normalized_input_hash, tool_name, input_json, ack_status,
		model_tool_call_id, execution_state, execution_attempt_generation,
		authorized_binding_revision, authorized_provider_resource_id,
		created_at, updated_at
	) VALUES ('default','sesn_bridge_pod_loss','thr_bridge_pod_loss','evt_pod_loss_tool','sandbox_tool',
		$1,'Write','{"file_path":"src/a.ts"}','committed',
		'tool-call-pod-loss','running',1,1,'provider_pod_loss',
		'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`,
		sha256Hex(`{"file_path":"src/a.ts"}`)); err != nil {
		t.Fatalf("seed pod-loss sandbox execution: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_pending_tool_uses (
			workspace_id, session_id, session_thread_id, tool_use_event_id, model_tool_call_id,
			tool_name, input_json, status, created_at, updated_at
		) VALUES (
			'default', 'sesn_bridge_pod_loss', 'thr_bridge_pod_loss', 'evt_pod_loss_tool', 'tool-call-pod-loss',
			'Write', '{"file_path":"src/a.ts"}', 'resolving',
			'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		)`); err != nil {
		t.Fatalf("seed lost pending approval: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_sandbox_bindings (
		workspace_id, session_id, logical_sandbox_id, environment_id,
		environment_generation, provider, provider_resource_id, binding_revision,
		materialized_resource_revision, resource_credential_expires_at,
		resource_roots_json, helper_verified_at, created_at, updated_at
	) VALUES (
		'default','sesn_bridge_pod_loss','sbox_bridge_pod_loss','env_sesn_bridge_pod_loss',
		1,'daytona','provider_pod_loss',1,1,'2027-01-01T00:00:00Z','[]','2026-01-01T00:00:00Z',
		'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z'
	)`); err != nil {
		t.Fatalf("seed pod-loss Sandbox binding: %v", err)
	}
	client := dbconnect.NewClientForTesting(runtime)
	var releaseOperationID string
	if err := client.WithWorkspaceTx(context.Background(), "default", "test.pod_loss_release", func(tx *dbconnect.Tx) error {
		var err error
		releaseOperationID, _, err = sandboxrelease.EnsureTx(
			context.Background(), tx, "default", "sesn_bridge_pod_loss",
			sandboxrelease.SessionDelete, "provider_pod_loss", time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC),
		)
		return err
	}); err != nil {
		t.Fatalf("seed pod-loss Sandbox release: %v", err)
	}
	queueStore := queue.NewPostgreSQLStore(client)
	leasedRelease, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: "default", Kinds: []string{queue.KindSandboxRelease},
		LeaseOwner: "sandbox-pod-loss-park", MaxJobs: 1, LeaseDuration: time.Minute,
	})
	if err != nil || len(leasedRelease) != 1 {
		t.Fatalf("lease release before park = %#v, %v; want one job", leasedRelease, err)
	}
	oldReleaseJobID := leasedRelease[0].ID
	if updated, err := queueStore.Ack(context.Background(), queue.AckRequest{
		WorkspaceID: "default", JobID: oldReleaseJobID, LeaseToken: leasedRelease[0].LeaseToken,
	}); err != nil || !updated {
		t.Fatalf("ACK parked release = %t, %v; want true,nil", updated, err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE sandbox_lifecycle_operations
		SET queue_job_id=NULL, queue_kind=NULL, queue_partition_key=NULL, queue_dedupe_key=NULL,
		    lease_owner=NULL, lease_token=NULL, lease_expires_at=NULL
		WHERE workspace_id='default' AND operation_id=$1`, releaseOperationID); err != nil {
		t.Fatalf("park pod-loss Sandbox release: %v", err)
	}
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_pod_loss", "thr_bridge_pod_loss", "evt_pod_loss_later", 4, "user.message", `{"content":[{"type":"text","text":"after pod loss"}]}`)
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, model_request_id, projection_json, created_at, updated_at
		) VALUES
		('default', 'sesn_bridge_pod_loss', 'thr_bridge_pod_loss_closed', 'evt_pod_loss_closed_start', 1, 'span.model_request_start',
		 '{"type":"span.model_request_start","model_request_id":"mrq_pod_loss_closed","request_kind":"agent_provider_request"}',
		 'internal', false, 'mrq_pod_loss_closed',
		 '{"context_through_message_sequence":1,"request_kind":"agent_provider_request"}',
		 '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'),
		('default', 'sesn_bridge_pod_loss', 'thr_bridge_pod_loss_closed', 'evt_pod_loss_closed_tool', 2, 'agent.tool_use',
		 '{"type":"agent.tool_use","name":"Read","input":{"file_path":"src/b.ts"},"evaluated_permission":"allow"}',
		 'public', false, 'mrq_pod_loss_closed', '{}',
		 '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'),
		('default', 'sesn_bridge_pod_loss', 'thr_bridge_pod_loss_closed', 'evt_pod_loss_closed_end', 3, 'span.model_request_end',
		 '{"type":"span.model_request_end","model_request_id":"mrq_pod_loss_closed","model_request_start_id":"evt_pod_loss_closed_start","finish_reason":"tool_calls","is_error":false}',
		 'internal', false, 'mrq_pod_loss_closed', '{}',
		 '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`); err != nil {
		t.Fatalf("seed closed request with running tool: %v", err)
	}
	seedBridgeAPIDurableToolMessage(
		t,
		admin,
		"default",
		"sesn_bridge_pod_loss",
		"thr_bridge_pod_loss_closed",
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
	store := NewPostgreSQLRuntimeDeliveryStore(client, 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 4, 4, 0, time.UTC) }
	store.TargetResolver = KubernetesRuntimeTargetResolver{
		Snapshot: func() enginekubernetes.BindingVisibilitySnapshot { return snapshot },
		Clock:    store.Clock,
	}
	job := RuntimeJob{
		JobID:           "qjob_pod_loss_later",
		LeaseToken:      "lease_pod_loss_later",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       "sesn_bridge_pod_loss",
		SessionThreadID: "thr_bridge_pod_loss",
		RuntimeInputID:  "rin_pod_loss_later",
		EventIDs:        []string{"evt_pod_loss_later"},
		SequenceFrom:    4,
		SequenceTo:      4,
		InputKind:       "messages",
		PayloadJSON:     `{"workspace_id":"default","session_id":"sesn_bridge_pod_loss","session_thread_id":"thr_bridge_pod_loss","runtime_input_id":"rin_pod_loss_later","event_ids":["evt_pod_loss_later"],"sequence_from":4,"sequence_to":4,"input_kind":"messages"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, job)
	plan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand after pod loss: %v", err)
	}
	if plan.AcceptInput == nil || plan.AcceptInput.GetTargetPodUid() != "pod_uid_pod_loss_new" {
		t.Fatalf("plan target pod uid = %#v; want replacement pod", plan.AcceptInput)
	}
	var replacementReleaseJobID, replacementReleaseStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT o.queue_job_id, q.status
		FROM sandbox_lifecycle_operations o
		JOIN queue_jobs q ON q.workspace_id=o.workspace_id AND q.id=o.queue_job_id
		WHERE o.workspace_id='default' AND o.operation_id=$1`, releaseOperationID).Scan(
		&replacementReleaseJobID, &replacementReleaseStatus,
	); err != nil {
		t.Fatalf("read release woken by pod loss: %v", err)
	}
	if replacementReleaseJobID == oldReleaseJobID || replacementReleaseStatus != queue.StatusPending {
		t.Fatalf("pod-loss release job = %q/%q; want fresh pending job", replacementReleaseJobID, replacementReleaseStatus)
	}
	if repaired, err := store.RepairLostRuntimeBindings(context.Background(), "default"); err != nil || repaired != 0 {
		t.Fatalf("duplicate pod-loss repair = %d, %v; want 0,nil", repaired, err)
	}
	var releaseJobCount int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM queue_jobs
		WHERE workspace_id='default' AND kind=$1
		  AND payload_json::jsonb ->> 'operation_id'=$2`, queue.KindSandboxRelease, releaseOperationID).Scan(&releaseJobCount); err != nil {
		t.Fatalf("count pod-loss release jobs: %v", err)
	}
	if releaseJobCount != 2 {
		t.Fatalf("pod-loss release jobs = %d; want acknowledged predecessor plus one successor", releaseJobCount)
	}

	var requestEndEventID, requestEndPayload string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT event_id, payload_json
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_pod_loss'
		    AND type = 'span.model_request_end'
		    AND model_request_id = 'mrq_pod_loss'`).Scan(&requestEndEventID, &requestEndPayload); err != nil {
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
		    AND source_event_id = 'evt_pod_loss_tool'`).Scan(&messageCount, &messageData); err != nil {
		t.Fatalf("read pod-loss terminal tool message: %v", err)
	}
	if messageCount != 1 {
		t.Fatalf("pod-loss terminal tool messages = %d/%s; want one repaired durable message", messageCount, messageData)
	}
	assertPodLossDurableToolErrorContext(t, messageData, "tool-call-pod-loss")
	bridgeStore := NewPostgreSQLBridgeAPIStore(client)
	bridgeStore.RuntimeBindingTokenHMACKey = []byte("pod-loss-cold-context-signing-key")
	var replacementBindingID, replacementPodUID string
	var replacementGeneration int64
	if err := admin.QueryRowContext(context.Background(), `SELECT binding_id,binding_generation,agent_runtime_pod_uid
		FROM session_runtime_bindings WHERE workspace_id='default' AND session_id='sesn_bridge_pod_loss'`).Scan(
		&replacementBindingID, &replacementGeneration, &replacementPodUID,
	); err != nil {
		t.Fatalf("read replacement binding scope: %v", err)
	}
	loaded, err := bridgeStore.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: bridgeAPIScope(
		"sesn_bridge_pod_loss", "thr_bridge_pod_loss", replacementBindingID, replacementGeneration, replacementPodUID,
	)})
	if err != nil {
		t.Fatalf("LoadContext after Pod-loss repair: %v", err)
	}
	for _, preserved := range []string{"committed text survives Pod loss", "committed reasoning survives Pod loss", "sig_pod_loss"} {
		if !strings.Contains(loaded.GetContextJson(), preserved) {
			t.Fatalf("Pod-loss cold context omitted %q: %s", preserved, loaded.GetContextJson())
		}
	}
	var coldPayload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(loaded.GetContextJson()), &coldPayload); err != nil {
		t.Fatalf("decode Pod-loss cold context: %v", err)
	}
	foundRepair := false
	for _, entry := range coldPayload.ContextEntries {
		if entry.ContextKind != "assistant" || len(entry.Parts) != 6 {
			continue
		}
		gotParts := make([]any, 0, len(entry.Parts))
		for _, raw := range entry.Parts {
			var part any
			if err := json.Unmarshal(raw, &part); err != nil {
				t.Fatalf("decode repaired Assistant part: %v", err)
			}
			gotParts = append(gotParts, part)
		}
		var wantParts []any
		if err := json.Unmarshal([]byte(`[
			{"type":"text","text":"committed text survives Pod loss"},
			{"type":"reasoning","text":"committed reasoning survives Pod loss","providerMetadata":{"anthropic":{"signature":"sig_pod_loss"}}},
			{"type":"tool_call","modelToolCallId":"tool-call-settled-before-pod-loss","toolName":"Read","canonicalInput":{"file_path":"src/b.ts"}},
			{"type":"tool_result","modelToolCallId":"tool-call-settled-before-pod-loss","result":{"type":"completed","output":{"text":"already durable"}}},
			{"type":"tool_call","modelToolCallId":"tool-call-pod-loss","toolName":"Write","canonicalInput":{"file_path":"src/a.ts"}},
			{"type":"tool_result","modelToolCallId":"tool-call-pod-loss","result":{"type":"error","error":{"type":"runtime_pod_lost","message":"Tool result unavailable because the runtime pod was lost.","retryable":false}}}
		]`), &wantParts); err != nil {
			t.Fatalf("decode expected Pod-loss parts: %v", err)
		}
		if !reflect.DeepEqual(gotParts, wantParts) {
			t.Fatalf("Pod-loss repaired parts = %#v; want exact retained prefix plus one terminal result %#v", gotParts, wantParts)
		}
		foundRepair = true
	}
	if !foundRepair {
		t.Fatalf("Pod-loss cold context omitted repaired Tool pair: %#v", coldPayload.ContextEntries)
	}
	assertRuntimeDirectContextComposition(t, loaded.GetContextJson())
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_events (
		workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
		visibility, session_visible, model_request_id, projection_json, created_at, updated_at
	) SELECT workspace_id, session_id, session_thread_id, 'evt_pod_loss_rescheduled', max(sequence) + 1,
		'session.status_rescheduled', '{"type":"session.status_rescheduled"}', 'internal', false,
		'mrq_pod_loss', '{"attempt":1,"effective_deadline":"2026-01-01T00:06:01Z","provider_attempts":1,"compaction_attempts":0}', '2026-01-01T00:06:00Z', '2026-01-01T00:06:00Z'
	  FROM session_events
	 WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_pod_loss'
	 GROUP BY workspace_id, session_id, session_thread_id
	 HAVING session_thread_id = 'thr_bridge_pod_loss'`); err != nil {
		t.Fatalf("reschedule repaired request: %v", err)
	}
	rescheduleReceipt, err := marshalRequestEndReplay(requestEndDurableFacts{
		RequestEndEventID: requestEndEventID,
		Disposition:       "rescheduled",
		EffectiveDeadline: "2026-01-01T00:06:01Z",
	})
	if err != nil {
		t.Fatalf("encode repaired request reschedule receipt: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_turn_retries (
		workspace_id, session_id, session_thread_id, provider_attempts, compaction_attempts, updated_at
	) VALUES ('default', 'sesn_bridge_pod_loss', 'thr_bridge_pod_loss', 1, 0, '2026-01-01T00:06:00Z')`); err != nil {
		t.Fatalf("seed repaired request retry counter: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_bridge_operations (
		workspace_id, session_id, session_thread_id, operation, source_kind, idempotency_key,
		request_hash, ack_status, result_json, declaration_digest, receipt_json, created_at, updated_at
	) VALUES ('default', 'sesn_bridge_pod_loss', 'thr_bridge_pod_loss', 'write_request_end',
		'model_request', 'mrq_pod_loss', 'pod-loss-reschedule', 'committed', '{}',
		'pod-loss-reschedule', $1, '2026-01-01T00:06:00Z', '2026-01-01T00:06:00Z')`, rescheduleReceipt); err != nil {
		t.Fatalf("seed repaired request reschedule receipt: %v", err)
	}
	rescheduled, err := bridgeStore.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: bridgeAPIScope(
		"sesn_bridge_pod_loss", "thr_bridge_pod_loss", replacementBindingID, replacementGeneration, replacementPodUID,
	)})
	if err != nil {
		t.Fatalf("LoadContext after repaired request reschedule: %v", err)
	}
	var rescheduledPayload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(rescheduled.GetContextJson()), &rescheduledPayload); err != nil {
		t.Fatalf("decode rescheduled Pod-loss context: %v", err)
	}
	foundRescheduledRepair := false
	for _, entry := range rescheduledPayload.ContextEntries {
		for _, part := range entry.Parts {
			if strings.Contains(string(part), `"modelToolCallId":"tool-call-pod-loss"`) {
				foundRescheduledRepair = true
			}
		}
	}
	if !foundRescheduledRepair {
		t.Fatalf("rescheduled Pod-loss direct context omitted its durable repair pair: %#v", rescheduledPayload.ContextEntries)
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
		t.Fatalf("read pod-loss execution record: %v", err)
	}
	if executionState != "consumed" || storedResult.Valid || consumptionReason != "pod_lost" {
		t.Fatalf("pod-loss execution = %q/%v/%q; want consumed thin record", executionState, storedResult, consumptionReason)
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
	var messageJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT data_json FROM session_messages
		  WHERE workspace_id = 'default' AND session_id = $1 AND message_id = $2`,
		sessionID, "msg_"+toolUseEventID,
	).Scan(&messageJSON); err != nil {
		t.Fatalf("read repaired MCP message: %v", err)
	}
	assertPodLossDurableToolErrorContext(t, messageJSON, "call_mcp_pod_loss")
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

func assertPodLossDurableToolErrorContext(t *testing.T, raw, modelToolCallID string) {
	t.Helper()
	stored, err := decodeRuntimeDeclarationObject(raw)
	if err != nil {
		t.Fatalf("decode repaired Tool context: %v", err)
	}
	parts, ok := stored["parts"].([]any)
	if !ok {
		t.Fatalf("repaired Tool context parts = %#v; want an array", stored["parts"])
	}
	var resultPart map[string]any
	for _, candidate := range parts {
		part, candidateOK := candidate.(map[string]any)
		if candidateOK && part["type"] == "tool_result" && part["modelToolCallId"] == modelToolCallID {
			resultPart = part
			break
		}
	}
	if resultPart == nil || len(resultPart) != 3 || resultPart["type"] != "tool_result" || resultPart["modelToolCallId"] != modelToolCallID {
		t.Fatalf("repaired Tool result = %#v; want exact narrow result identity", resultPart)
	}
	outcome, ok := resultPart["result"].(map[string]any)
	if !ok || len(outcome) != 2 || outcome["type"] != "error" {
		t.Fatalf("repaired Tool outcome = %#v; want exact error outcome", resultPart["result"])
	}
	toolError, ok := outcome["error"].(map[string]any)
	if !ok || len(toolError) != 3 || toolError["type"] != "runtime_pod_lost" ||
		toolError["message"] != "Tool result unavailable because the runtime pod was lost." || toolError["retryable"] != false {
		t.Fatalf("repaired Tool error = %#v; want non-retryable pod-loss error", outcome["error"])
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
		orphans, err = runtimePodLostOrphanToolUsesTx(context.Background(), tx, "default", sessionID, []string{reviewerID})
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
		orphans, err = runtimePodLostOrphanToolUsesTx(context.Background(), tx, "default", sessionID, []string{threadID})
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

func TestRuntimePodLossSettlesToolUseAwaitingApproval(t *testing.T) {
	for _, testCase := range []struct {
		name             string
		approvalStatus   string
		withRequestEnd   bool
		wantEndIsError   bool
		raceConfirmation bool
	}{
		{name: "existing successful Request End", approvalStatus: "pending", withRequestEnd: true},
		{name: "missing Request End", approvalStatus: "resolving", wantEndIsError: true},
		{name: "confirmation race", approvalStatus: "resolving", withRequestEnd: true, raceConfirmation: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			suffix := strings.ReplaceAll(testCase.approvalStatus, "_", "")
			if testCase.withRequestEnd {
				suffix += "_sealed"
			} else {
				suffix += "_open"
			}
			if testCase.raceConfirmation {
				suffix += "_race"
			}
			sessionID := "sesn_pod_loss_approval_" + suffix
			threadID := "thr_pod_loss_approval_" + suffix
			modelRequestID := "mreq_pod_loss_approval_" + suffix
			toolUseEventID := "evt_pod_loss_approval_tool_" + suffix
			bindingID := "bind_pod_loss_approval_" + suffix
			binding := runtimePodLostBinding(sessionID, bindingID, 1)
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, binding.PodUID)
			seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
			if _, err := admin.ExecContext(context.Background(),
				`INSERT INTO session_events (
					workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
					visibility, session_visible, model_request_id, projection_json, created_at, updated_at
				) VALUES
				('default', $1, $2, $5, 1, 'span.model_request_start',
				 $6, 'internal', false, $3,
				 '{"context_through_message_sequence":0,"request_kind":"agent_provider_request"}', now(), now()),
				('default', $1, $2, $4, 2, 'agent.tool_use',
				 $7, 'public', true, $3, '{}', now(), now())`,
				sessionID,
				threadID,
				modelRequestID,
				toolUseEventID,
				"evt_pod_loss_approval_start_"+suffix,
				`{"type":"span.model_request_start","model_request_id":"`+modelRequestID+`"}`,
				`{"type":"agent.tool_use","name":"Write","input":{"file_path":"src/a.ts"},"evaluated_permission":"ask"}`,
			); err != nil {
				t.Fatalf("seed pending-approval request: %v", err)
			}
			if testCase.withRequestEnd {
				if _, err := admin.ExecContext(context.Background(),
					`INSERT INTO session_events (
						workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
						visibility, session_visible, model_request_id, projection_json, created_at, updated_at
					) VALUES ('default', $1, $2, $4, 3, 'span.model_request_end', $5,
						'internal', false, $3, '{}', now(), now())`,
					sessionID,
					threadID,
					modelRequestID,
					"evt_pod_loss_approval_end_"+suffix,
					`{"type":"span.model_request_end","model_request_id":"`+modelRequestID+`","model_request_start_id":"evt_pod_loss_approval_start_`+suffix+`","finish_reason":"tool_calls","is_error":false}`,
				); err != nil {
					t.Fatalf("seed approval Request End: %v", err)
				}
			}
			seedBridgeAPIDurableToolMessage(
				t, admin, "default", sessionID, threadID, modelRequestID,
				toolUseEventID, "tool-call-pod-loss-approval-"+suffix, "Write",
			)
			if _, err := admin.ExecContext(context.Background(),
				`INSERT INTO session_pending_tool_uses (
					workspace_id, session_id, session_thread_id, tool_use_event_id, model_tool_call_id,
					tool_name, input_json, status, created_at, updated_at
				) VALUES ('default', $1, $2, $3, $4, 'Write',
					'{"file_path":"src/a.ts"}', $5, now(), now())`,
				sessionID, threadID, toolUseEventID, "tool-call-pod-loss-approval-"+suffix, testCase.approvalStatus,
			); err != nil {
				t.Fatalf("seed pending approval: %v", err)
			}

			apiStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			apiStore.RuntimeBindingTokenHMACKey = []byte("bridge-pod-loss-approval-key!!")
			if testCase.raceConfirmation {
				confirmationRequest := bridgeToolSettlementRequestForTest(
					bridgeAPIScope(sessionID, threadID, bindingID, 1, binding.PodUID),
					bridgeErrorToolSettlementForTest(toolUseEventID, "Approval denied: cancel"),
				)
				start := make(chan struct{})
				repairResult := make(chan error, 1)
				confirmationResult := make(chan error, 1)
				go func() {
					<-start
					_, err := runRuntimePodLostRepairTransaction(
						context.Background(), runtime, sessionID, binding,
						time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC),
					)
					repairResult <- err
				}()
				go func() {
					<-start
					_, err := apiStore.SettleToolResult(context.Background(), confirmationRequest)
					confirmationResult <- err
				}()
				close(start)
				if err := <-repairResult; err != nil {
					t.Fatalf("repair pending approval during confirmation race: %v", err)
				}
				if err := <-confirmationResult; err != nil && status.Code(err) != codes.FailedPrecondition && status.Code(err) != codes.AlreadyExists {
					t.Fatalf("confirmation race result: %v", err)
				}
			} else if _, err := runRuntimePodLostRepairTransaction(
				context.Background(), runtime, sessionID, binding,
				time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC),
			); err != nil {
				t.Fatalf("repair pending approval after pod loss: %v", err)
			}

			var resultCount int
			var resultEventID string
			var resultPayload string
			if err := admin.QueryRowContext(context.Background(),
				`SELECT count(*), COALESCE(MAX(event_id), ''), COALESCE(MAX(payload_json), '')
				   FROM session_events
				  WHERE workspace_id = 'default' AND session_id = $1 AND session_thread_id = $2
				    AND type = 'agent.tool_result'
				    AND (payload_json::jsonb ->> 'tool_use_event_id' = $3
				         OR payload_json::jsonb ->> 'tool_use_id' = $3)`,
				sessionID, threadID, toolUseEventID,
			).Scan(&resultCount, &resultEventID, &resultPayload); err != nil {
				t.Fatalf("read approval pod-loss result: %v", err)
			}
			if resultCount != 1 || !strings.Contains(resultPayload, `"is_error":true`) {
				t.Fatalf("approval pod-loss result = %d/%s; want one terminal error", resultCount, resultPayload)
			}
			var pendingStatus string
			var pendingResultEventID sql.NullString
			if err := admin.QueryRowContext(context.Background(),
				`SELECT status, result_event_id FROM session_pending_tool_uses
				  WHERE workspace_id = 'default' AND session_id = $1 AND session_thread_id = $2
				    AND tool_use_event_id = $3`,
				sessionID, threadID, toolUseEventID,
			).Scan(&pendingStatus, &pendingResultEventID); err != nil {
				t.Fatalf("read repaired approval row: %v", err)
			}
			wantPendingStatus := "cancelled"
			if testCase.raceConfirmation && !strings.Contains(resultPayload, `"reason":"runtime_pod_lost"`) {
				wantPendingStatus = "resolved"
			}
			if pendingStatus != wantPendingStatus || !pendingResultEventID.Valid || pendingResultEventID.String != resultEventID {
				t.Fatalf("approval row = %q/%v; want %s linked to %s", pendingStatus, pendingResultEventID, wantPendingStatus, resultEventID)
			}
			var requestEndCount int
			var requestEndIsError bool
			if err := admin.QueryRowContext(context.Background(),
				`SELECT count(*), COALESCE(bool_or(COALESCE((payload_json::jsonb ->> 'is_error')::boolean, false)), false)
				   FROM session_events
				  WHERE workspace_id = 'default' AND session_id = $1 AND session_thread_id = $2
				    AND type = 'span.model_request_end' AND model_request_id = $3`,
				sessionID, threadID, modelRequestID,
			).Scan(&requestEndCount, &requestEndIsError); err != nil {
				t.Fatalf("read approval Request End: %v", err)
			}
			if requestEndCount != 1 || requestEndIsError != testCase.wantEndIsError {
				t.Fatalf("approval Request End count/error = %d/%v; want 1/%v", requestEndCount, requestEndIsError, testCase.wantEndIsError)
			}
			var failureCount int
			var retriesExhaustedCount int
			if err := admin.QueryRowContext(context.Background(),
				`SELECT
					count(*) FILTER (WHERE type = 'session.error'),
					count(*) FILTER (WHERE type = 'session.status_idle' AND payload_json::jsonb #>> '{stop_reason,type}' = 'retries_exhausted')
				   FROM session_events
				  WHERE workspace_id = 'default' AND session_id = $1 AND session_thread_id = $2`,
				sessionID, threadID,
			).Scan(&failureCount, &retriesExhaustedCount); err != nil {
				t.Fatalf("read approval pod-loss closeout: %v", err)
			}
			if failureCount != 1 || retriesExhaustedCount != 1 {
				t.Fatalf("approval pod-loss closeout error/idle = %d/%d; want 1/1", failureCount, retriesExhaustedCount)
			}

			loaded, err := apiStore.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
				Scope: bridgeAPIScope(sessionID, threadID, bindingID, 1, binding.PodUID),
			})
			if err != nil {
				t.Fatalf("LoadContext after approval pod loss: %v", err)
			}
			var payload bridgeLoadContextPayload
			if err := json.Unmarshal([]byte(loaded.GetContextJson()), &payload); err != nil {
				t.Fatalf("decode approval pod-loss context: %v", err)
			}
			if len(payload.PendingToolUses) != 0 {
				t.Fatalf("approval pod-loss pending context = %+v; want no live approval route", payload.PendingToolUses)
			}
		})
	}
}

func TestRuntimePodLossSettlesEveryPendingApprovalExactlyOnce(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID             = "sesn_pod_loss_multiple_approvals"
		threadID              = "thr_pod_loss_multiple_approvals"
		siblingThreadID       = "thr_pod_loss_sibling_approval"
		idleSiblingThreadID   = "thr_pod_loss_idle_sibling"
		modelRequestID        = "mreq_pod_loss_multiple_approvals"
		siblingModelRequestID = "mreq_pod_loss_sibling_approval"
		bindingID             = "bind_pod_loss_multiple_approvals"
	)
	binding := runtimePodLostBinding(sessionID, bindingID, 1)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, threadID, siblingThreadID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, threadID, idleSiblingThreadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, binding.PodUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_runtime_status
		    SET status = 'idle', running_since = NULL
		  WHERE workspace_id = 'default' AND session_id = $1`,
		sessionID,
	); err != nil {
		t.Fatalf("seed idle Runtime status with durable pending work: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, model_request_id, projection_json, created_at, updated_at
		) VALUES
		('default', $1, $2, 'evt_pod_loss_multiple_start', 1, 'span.model_request_start',
		 $4, 'internal', false, $3,
		 '{"context_through_message_sequence":0,"request_kind":"agent_provider_request"}', now(), now()),
		('default', $1, $2, 'evt_pod_loss_multiple_tool_pending', 2, 'agent.tool_use',
		 '{"type":"agent.tool_use","name":"Write","input":{"file_path":"src/a.ts"},"evaluated_permission":"ask"}',
		 'public', true, $3, '{}', now(), now()),
		('default', $1, $2, 'evt_pod_loss_multiple_tool_resolving', 3, 'agent.tool_use',
		 '{"type":"agent.tool_use","name":"Write","input":{"file_path":"src/b.ts"},"evaluated_permission":"ask"}',
		 'public', true, $3, '{}', now(), now()),
		('default', $1, $2, 'evt_pod_loss_multiple_end', 4, 'span.model_request_end',
		 $5, 'internal', false, $3, '{}', now(), now())`,
		sessionID,
		threadID,
		modelRequestID,
		`{"type":"span.model_request_start","model_request_id":"`+modelRequestID+`"}`,
		`{"type":"span.model_request_end","model_request_id":"`+modelRequestID+`","model_request_start_id":"evt_pod_loss_multiple_start","finish_reason":"tool_calls","is_error":false}`,
	); err != nil {
		t.Fatalf("seed multiple pending approvals request: %v", err)
	}
	seedBridgeAPIDurableToolMessage(
		t, admin, "default", sessionID, threadID, modelRequestID,
		"evt_pod_loss_multiple_tool_pending", "tool-call-pod-loss-multiple-pending", "Write",
	)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_messages
		    SET data_json = jsonb_set(
				data_json::jsonb,
				'{parts}',
				(data_json::jsonb -> 'parts') || $4::jsonb
			)::text
		  WHERE workspace_id = 'default' AND session_id = $1 AND session_thread_id = $2
		    AND model_request_id = $3 AND kind = 'assistant'`,
		sessionID,
		threadID,
		modelRequestID,
		`[{"type":"tool_call","modelToolCallId":"tool-call-pod-loss-multiple-resolving","toolName":"Write","canonicalInput":{}}]`,
	); err != nil {
		t.Fatalf("append second durable tool part: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_events
		    SET projection_json = jsonb_build_object(
		      'model_tool_call_id', 'tool-call-pod-loss-multiple-resolving',
		      'tool_name', 'Write',
		      'provider_input', payload_json::jsonb -> 'input',
		      'canonical_execution_input', payload_json::jsonb -> 'input'
		    )
		  WHERE workspace_id='default' AND session_id=$1
		    AND event_id='evt_pod_loss_multiple_tool_resolving'`,
		sessionID,
	); err != nil {
		t.Fatalf("seed second durable Tool Use identity: %v", err)
	}
	for _, tool := range []struct {
		eventID    string
		toolCallID string
		status     string
		path       string
	}{
		{eventID: "evt_pod_loss_multiple_tool_pending", toolCallID: "tool-call-pod-loss-multiple-pending", status: "pending", path: "src/a.ts"},
		{eventID: "evt_pod_loss_multiple_tool_resolving", toolCallID: "tool-call-pod-loss-multiple-resolving", status: "resolving", path: "src/b.ts"},
	} {
		if _, err := admin.ExecContext(context.Background(),
			`INSERT INTO session_pending_tool_uses (
				workspace_id, session_id, session_thread_id, tool_use_event_id, model_tool_call_id,
				tool_name, input_json, status, created_at, updated_at
			) VALUES ('default', $1, $2, $3, $4, 'Write', $5, $6, now(), now())`,
			sessionID, threadID, tool.eventID, tool.toolCallID,
			`{"file_path":"`+tool.path+`"}`, tool.status,
		); err != nil {
			t.Fatalf("seed %s approval: %v", tool.status, err)
		}
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, model_request_id, projection_json, created_at, updated_at
		) VALUES
		('default', $1, $2, 'evt_pod_loss_sibling_start', 1, 'span.model_request_start',
		 $4, 'internal', false, $3,
		 '{"context_through_message_sequence":0,"request_kind":"agent_provider_request"}', now(), now()),
		('default', $1, $2, 'evt_pod_loss_sibling_tool', 2, 'agent.tool_use',
		 '{"type":"agent.tool_use","name":"Write","input":{"file_path":"src/sibling.ts"},"evaluated_permission":"ask"}',
		 'public', true, $3, '{}', now(), now()),
		('default', $1, $2, 'evt_pod_loss_sibling_end', 3, 'span.model_request_end',
		 $5, 'internal', false, $3, '{}', now(), now())`,
		sessionID,
		siblingThreadID,
		siblingModelRequestID,
		`{"type":"span.model_request_start","model_request_id":"`+siblingModelRequestID+`"}`,
		`{"type":"span.model_request_end","model_request_id":"`+siblingModelRequestID+`","model_request_start_id":"evt_pod_loss_sibling_start","finish_reason":"tool_calls","is_error":false}`,
	); err != nil {
		t.Fatalf("seed sibling approval request: %v", err)
	}
	seedBridgeAPIDurableToolMessage(
		t, admin, "default", sessionID, siblingThreadID, siblingModelRequestID,
		"evt_pod_loss_sibling_tool", "tool-call-pod-loss-sibling", "Write",
	)
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_pending_tool_uses (
			workspace_id, session_id, session_thread_id, tool_use_event_id, model_tool_call_id,
			tool_name, input_json, status, created_at, updated_at
		) VALUES ('default', $1, $2, 'evt_pod_loss_sibling_tool', 'tool-call-pod-loss-sibling',
			'Write', '{"file_path":"src/sibling.ts"}', 'pending', now(), now())`,
		sessionID,
		siblingThreadID,
	); err != nil {
		t.Fatalf("seed sibling approval: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, model_request_id, projection_json, created_at, updated_at
		) VALUES
		('default', $1, $2, 'evt_pod_loss_idle_start', 1, 'span.model_request_start',
		 '{"type":"span.model_request_start","model_request_id":"mreq_pod_loss_idle"}',
		 'internal', false, 'mreq_pod_loss_idle',
		 '{"context_through_message_sequence":0,"request_kind":"agent_provider_request"}', now(), now()),
		('default', $1, $2, 'evt_pod_loss_idle_tool', 2, 'agent.tool_use',
		 '{"type":"agent.tool_use","name":"Read","input":{"file_path":"README.md"},"evaluated_permission":"allow"}',
		 'public', true, 'mreq_pod_loss_idle', '{}', now(), now()),
		('default', $1, $2, 'evt_pod_loss_idle_result', 3, 'agent.tool_result',
		 '{"type":"agent.tool_result","tool_use_event_id":"evt_pod_loss_idle_tool","content":[{"type":"text","text":"done"}]}',
		 'public', true, 'mreq_pod_loss_idle', '{}', now(), now()),
		('default', $1, $2, 'evt_pod_loss_idle_end', 4, 'span.model_request_end',
		 '{"type":"span.model_request_end","model_request_id":"mreq_pod_loss_idle","model_request_start_id":"evt_pod_loss_idle_start","finish_reason":"stop","is_error":false}',
		 'internal', false, 'mreq_pod_loss_idle', '{}', now(), now())`,
		sessionID,
		idleSiblingThreadID,
	); err != nil {
		t.Fatalf("seed idle sibling terminal history: %v", err)
	}

	for attempt := 1; attempt <= 2; attempt++ {
		if _, err := runRuntimePodLostRepairTransaction(
			context.Background(), runtime, sessionID, binding,
			time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC),
		); err != nil {
			t.Fatalf("repair multiple approvals attempt %d: %v", attempt, err)
		}
	}

	var terminalResults int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_events
		  WHERE workspace_id = 'default' AND session_id = $1 AND session_thread_id = $2
		    AND type = 'agent.tool_result'
		    AND payload_json::jsonb ->> 'reason' = 'runtime_pod_lost'`,
		sessionID, threadID,
	).Scan(&terminalResults); err != nil {
		t.Fatalf("count multiple approval pod-loss results: %v", err)
	}
	if terminalResults != 2 {
		t.Fatalf("multiple approval pod-loss results = %d; want exactly 2", terminalResults)
	}
	var cancelledAndLinked int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_pending_tool_uses
		  WHERE workspace_id = 'default' AND session_id = $1 AND session_thread_id = $2
		    AND status = 'cancelled' AND result_event_id IS NOT NULL`,
		sessionID, threadID,
	).Scan(&cancelledAndLinked); err != nil {
		t.Fatalf("count cancelled multiple approvals: %v", err)
	}
	if cancelledAndLinked != 2 {
		t.Fatalf("cancelled and linked approvals = %d; want exactly 2", cancelledAndLinked)
	}
	var siblingApprovalStatus string
	var siblingResultCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT p.status,
		        (SELECT count(*) FROM session_events result
		          WHERE result.workspace_id = p.workspace_id
		            AND result.session_id = p.session_id
		            AND result.session_thread_id = p.session_thread_id
		            AND result.type = 'agent.tool_result'
		            AND result.payload_json::jsonb ->> 'tool_use_event_id' = p.tool_use_event_id)
		   FROM session_pending_tool_uses p
		  WHERE p.workspace_id = 'default' AND p.session_id = $1 AND p.session_thread_id = $2
		    AND p.tool_use_event_id = 'evt_pod_loss_sibling_tool'`,
		sessionID,
		siblingThreadID,
	).Scan(&siblingApprovalStatus, &siblingResultCount); err != nil {
		t.Fatalf("read sibling approval: %v", err)
	}
	if siblingApprovalStatus != "cancelled" || siblingResultCount != 1 {
		t.Fatalf("sibling approval = %q/results %d; want cancelled/1", siblingApprovalStatus, siblingResultCount)
	}
	var idleSiblingStatus string
	var idleSiblingRepairEventCount int
	var idleSiblingHistoryCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status,
		        (SELECT count(*) FROM session_events event
		          WHERE event.workspace_id = thread.workspace_id
		            AND event.session_id = thread.session_id
		            AND event.session_thread_id = thread.id
		            AND event.type IN ('session.error', 'session.thread_status_idle')),
		        (SELECT count(*) FROM session_events event
		          WHERE event.workspace_id = thread.workspace_id
		            AND event.session_id = thread.session_id
		            AND event.session_thread_id = thread.id)
		   FROM session_threads thread
		  WHERE thread.workspace_id = 'default' AND thread.session_id = $1 AND thread.id = $2`,
		sessionID,
		idleSiblingThreadID,
	).Scan(&idleSiblingStatus, &idleSiblingRepairEventCount, &idleSiblingHistoryCount); err != nil {
		t.Fatalf("read idle sibling after pod loss: %v", err)
	}
	if idleSiblingStatus != "idle" || idleSiblingRepairEventCount != 0 || idleSiblingHistoryCount != 4 {
		t.Fatalf("idle sibling = %q/repair events %d/history %d; want idle/0/4", idleSiblingStatus, idleSiblingRepairEventCount, idleSiblingHistoryCount)
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
