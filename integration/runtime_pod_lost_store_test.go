package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	sandboxrelease "github.com/tetral-ai/tetral/internal/sandbox/release"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
)

type settlingRecoveryCommandClient struct {
	*jobrunner.RuntimePodCommandClient
	beforeRecover func(*agentruntimev1.RecoverThreadRequest) error
	response      *agentruntimev1.RecoverThreadResponse
	err           error
}

func (c *settlingRecoveryCommandClient) RecoverThread(ctx context.Context, target jobrunner.RuntimePodTarget, request *agentruntimev1.RecoverThreadRequest) (*agentruntimev1.RecoverThreadResponse, error) {
	if c.beforeRecover != nil {
		if err := c.beforeRecover(request); err != nil {
			return nil, err
		}
	}
	c.response, c.err = c.RuntimePodCommandClient.RecoverThread(ctx, target, request)
	return c.response, c.err
}

func TestPostgreSQLRuntimePodLossRetentionPreservesTerminalToolAndRepairMembers(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_pod_loss_terminal_retention"
		threadID       = "thr_pod_loss_terminal_retention"
		bindingID      = "bind_pod_loss_terminal_retention"
		podUID         = "pod_pod_loss_terminal_retention"
		modelRequestID = "mreq_pod_loss_terminal_retention"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	store := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("pod-loss-terminal-retention-signing-key")
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_pod_loss_retention_start", modelRequestID, runtimecontrol.RequestKindAgentProviderRequest, 0)
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_pod_loss_retention_tool", ModelRequestId: modelRequestID,
		ToolDeclaration: bridgeToolDeclarationForTest(
			"call_pod_loss_retention", "Read", `{"file_path":"README.md"}`, "allow", "sandbox_execute",
		),
	})
	if err != nil || toolUse.GetCommitted() == nil {
		t.Fatalf("commit terminal retention Tool Use: %#v/%v", toolUse, err)
	}
	toolUseEventID := toolUse.GetCommitted().GetEventId()
	settled, err := store.SettleToolResult(context.Background(), bridgeToolSettlementRequestForTest(
		scope,
		bridgeCompletedToolSettlementForTest(toolUseEventID, "terminal before pod loss"),
	))
	if err != nil || settled.GetCommitted() == nil {
		t.Fatalf("settle terminal retention Tool: %#v/%v", settled, err)
	}
	repairKey := "internal_invalid_tool_d83936654516581cacf8d854de2315260be5701385a4160c759d9b3ebb03b7de"
	repaired, err := store.CommitInternalToolRepair(context.Background(), &bridgev1.CommitInternalToolRepairRequest{
		Scope: scope, ModelRequestId: modelRequestID, ModelToolCallId: "call_pod_loss_repair",
		ToolName: "unknown_tool", RepairKey: repairKey, CanonicalInputJson: `{"q":"x"}`,
		Error: &bridgev1.RuntimeToolError{ErrorJson: `{"type":"provider_tool_protocol_error","message":"invalid tool","retryable":false}`},
	})
	if err != nil || repaired.GetCommitted() == nil {
		t.Fatalf("commit terminal retention repair: %#v/%v", repaired, err)
	}
	repairEventID := repaired.GetCommitted().GetRepairEventId()

	client := dbconnect.NewClientForTesting(runtime)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events
		SET payload_json = jsonb_set(payload_json::jsonb, '{repair_kind}', '"not_membership_authority"'::jsonb)::text
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, repairEventID); err != nil {
		t.Fatalf("alter repair payload marker: %v", err)
	}
	var relationOwnedRetention *bridgev1.ProviderContextRetention
	if err := client.WithWorkspaceTx(context.Background(), "default", "test.pod_loss_relation_owned_retention", func(tx *dbconnect.Tx) error {
		var loadErr error
		relationOwnedRetention, loadErr = runtimecontrol.RuntimeTerminalRequestRetentionTx(context.Background(), tx, scope, modelRequestID)
		return loadErr
	}); err != nil {
		t.Fatalf("derive relation-owned terminal pod-loss retention: %v", err)
	}
	if !slices.Equal(relationOwnedRetention.GetRepairEventIds(), []string{repairEventID}) {
		t.Fatalf("relation-owned repair retention = %#v; payload marker must not select membership", relationOwnedRetention)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events
		SET payload_json = jsonb_set(payload_json::jsonb, '{repair_kind}', '"invalid_tool"'::jsonb)::text
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, repairEventID); err != nil {
		t.Fatalf("restore repair payload marker: %v", err)
	}
	var retention *bridgev1.ProviderContextRetention
	if err := client.WithWorkspaceTx(context.Background(), "default", "test.pod_loss_terminal_retention", func(tx *dbconnect.Tx) error {
		var loadErr error
		retention, loadErr = runtimecontrol.RuntimeTerminalRequestRetentionTx(context.Background(), tx, scope, modelRequestID)
		if loadErr != nil {
			return loadErr
		}
		return nil
	}); err != nil {
		t.Fatalf("derive terminal pod-loss retention: %v", err)
	}
	if retention.GetAssistantMessageSequence() != toolUse.GetCommitted().GetAssignedMessageSequence() ||
		!slices.Equal(retention.GetToolUseEventIds(), []string{toolUseEventID}) ||
		!slices.Equal(retention.GetRepairEventIds(), []string{repairEventID}) {
		t.Fatalf("terminal pod-loss retention = %#v; want exact Assistant, Tool Use, and repair identities", retention)
	}
	// Reach the actual Bridge retention validator, then force rollback before the
	// Request End event is inserted so the following loss still closes an open request.
	if _, err := admin.ExecContext(context.Background(), `CREATE FUNCTION reject_retention_validation_end() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'retention-validation-reached'; END $$;
	CREATE TRIGGER reject_retention_validation_end BEFORE INSERT ON session_events FOR EACH ROW WHEN (NEW.type = 'span.model_request_end') EXECUTE FUNCTION reject_retention_validation_end()`); err != nil {
		t.Fatalf("install retention rollback barrier: %v", err)
	}
	_, validationErr := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_retention_validation_rollback", ModelRequestId: modelRequestID,
		FinishReason: "error", UsageJson: `{}`, IsError: true, ErrorKind: "runtime_pod_lost", ProviderContextRetention: retention,
	})
	if validationErr == nil || !strings.Contains(validationErr.Error(), "retention-validation-reached") {
		t.Fatalf("actual Bridge retention validation did not reach insert barrier: %v", validationErr)
	}
	if _, err := admin.ExecContext(context.Background(), `DROP TRIGGER reject_retention_validation_end ON session_events; DROP FUNCTION reject_retention_validation_end()`); err != nil {
		t.Fatalf("remove retention rollback barrier: %v", err)
	}
	var prematureEnds int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND model_request_id=$2 AND type='span.model_request_end'`, sessionID, modelRequestID).Scan(&prematureEnds); err != nil || prematureEnds != 0 {
		t.Fatalf("retention validation rollback ends = %d/%v; want zero", prematureEnds, err)
	}
	binding := runtimecontrol.Binding{BindingID: bindingID, BindingGeneration: 1, PodUID: podUID}
	if _, err := runRuntimePodLostRepairTransaction(
		context.Background(), runtime, sessionID, binding, time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC),
	); err != nil {
		t.Fatalf("close open Request through pod-loss owner: %v", err)
	}
	var durableAssistant int64
	var durableToolUses, durableRepairs []byte
	if err := admin.QueryRowContext(context.Background(),
		`SELECT (payload_json::jsonb #>> '{provider_context_retention,assistant_message_sequence}')::bigint,
		        payload_json::jsonb #> '{provider_context_retention,tool_use_event_ids}',
		        payload_json::jsonb #> '{provider_context_retention,repair_event_ids}'
		   FROM session_events
		  WHERE workspace_id = 'default' AND session_id = $1 AND session_thread_id = $2
		    AND model_request_id = $3 AND type = 'span.model_request_end'`,
		sessionID, threadID, modelRequestID,
	).Scan(&durableAssistant, &durableToolUses, &durableRepairs); err != nil {
		t.Fatalf("read pod-loss Request End retention: %v", err)
	}
	if durableAssistant != toolUse.GetCommitted().GetAssignedMessageSequence() ||
		string(durableToolUses) != `["`+toolUseEventID+`"]` ||
		string(durableRepairs) != `["`+repairEventID+`"]` {
		t.Fatalf("durable pod-loss retention = assistant:%d tools:%s repairs:%s", durableAssistant, durableToolUses, durableRepairs)
	}
	scope = declareReplacementScope(t, client, scope)
	loaded, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope})
	if err != nil {
		t.Fatalf("cold LoadContext after pod-loss terminalization: %v", err)
	}
	var cold bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(loaded.GetContextJson()), &cold); err != nil {
		t.Fatalf("decode pod-loss terminal context: %v", err)
	}
	memberCounts := map[string]map[string]int{}
	for _, entry := range cold.ContextEntries {
		for _, rawPart := range entry.Parts {
			var part struct {
				Type            string `json:"type"`
				ModelToolCallID string `json:"modelToolCallId"`
			}
			if err := json.Unmarshal(rawPart, &part); err != nil || part.ModelToolCallID == "" {
				continue
			}
			if memberCounts[part.ModelToolCallID] == nil {
				memberCounts[part.ModelToolCallID] = map[string]int{}
			}
			memberCounts[part.ModelToolCallID][part.Type]++
		}
	}
	for _, callID := range []string{"call_pod_loss_retention", "call_pod_loss_repair"} {
		if memberCounts[callID]["tool_call"] != 1 || memberCounts[callID]["tool_result"] != 1 {
			t.Fatalf("cold pod-loss pair %s = %#v; want one Call and one Result", callID, memberCounts[callID])
		}
	}
	if len(cold.TurnFacts.InternalRepairs) != 1 || cold.TurnFacts.InternalRepairs[0].RepairKey != repairKey {
		t.Fatalf("cold pod-loss repair facts = %#v; want exact committed repair", cold.TurnFacts.InternalRepairs)
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
	attachmentStore := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	attachmentStore.AttachmentBlobStore = blob.NewFakeBlobStore()
	attachmentStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }
	attachment := createBridgeTransientAttachmentForTest(
		t, admin, attachmentStore,
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
		runtimecontrol.Sha256Hex(`{"file_path":"src/a.ts"}`)); err != nil {
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
	seedFixtureRuntimeProcess(t, dbconnect.NewClientForTesting(admin), newCandidate.Namespace, newCandidate.PodUID)
	snapshot := enginekubernetes.NewBindingVisibilitySnapshotStateWithCandidatesForTest(
		true,
		oldBound,
		enginekubernetes.BindingVisibilityDeleted,
		[]enginekubernetes.BindingCandidate{newCandidate},
	)
	store := jobrunner.NewPostgreSQLRuntimeDeliveryStore(client, 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 4, 4, 0, time.UTC) }
	store.TargetResolver = jobrunner.KubernetesRuntimeTargetResolver{
		Snapshot:   func() enginekubernetes.BindingVisibilitySnapshot { return snapshot },
		GetPod:     fixtureConfirmedMissingRuntimePod,
		LoadClient: fixtureRuntimeLoadClient(t),
		Clock:      store.Clock,
	}
	job := jobrunner.RuntimeJob{
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
	var replacementReleaseJobID sql.NullString
	if err := admin.QueryRowContext(context.Background(), `SELECT queue_job_id
		FROM sandbox_lifecycle_operations
		WHERE workspace_id='default' AND operation_id=$1`, releaseOperationID).Scan(&replacementReleaseJobID); err != nil {
		t.Fatalf("read release custody after pod loss: %v", err)
	}
	if replacementReleaseJobID.Valid {
		t.Fatalf("pod-loss release job = %q; want release blocked by preserved Tool owner", replacementReleaseJobID.String)
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
	if releaseJobCount != 1 {
		t.Fatalf("pod-loss release jobs = %d; want only acknowledged predecessor while Tool owner survives", releaseJobCount)
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
	var liveToolResultCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_pod_loss'
		    AND type = 'agent.tool_result'
		    AND payload_json::jsonb ->> 'tool_use_event_id' = 'evt_pod_loss_tool'`).Scan(&liveToolResultCount); err != nil {
		t.Fatalf("count pod-loss tool results: %v", err)
	}
	if liveToolResultCount != 0 {
		t.Fatalf("pod-loss live Tool results = %d; want preserved nonterminal owner", liveToolResultCount)
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
	if pendingStatus != "resolving" || resultEventID.Valid {
		t.Fatalf("pod-loss pending status/result = %q/%v; want unchanged resolving owner", pendingStatus, resultEventID)
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
	if messageCount != 1 || strings.Contains(messageData, `"modelToolCallId":"tool-call-pod-loss","result"`) {
		t.Fatalf("pod-loss Tool message = %d/%s; want original call without synthetic Result", messageCount, messageData)
	}
	bridgeStore := agentruntimebridge.NewPostgreSQLBridgeAPIStore(client)
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
	var coldPayload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(loaded.GetContextJson()), &coldPayload); err != nil {
		t.Fatalf("decode Pod-loss cold context: %v", err)
	}
	if len(coldPayload.PendingSandboxExecutions) != 1 || coldPayload.PendingSandboxExecutions[0].ToolUseEventID != "evt_pod_loss_tool" ||
		coldPayload.PendingSandboxExecutions[0].ExecutionState != "running" {
		t.Fatalf("Pod-loss cold pending execution = %#v; want original running Tool identity", coldPayload.PendingSandboxExecutions)
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
	var executionState string
	var storedResult sql.NullString
	if err := admin.QueryRowContext(context.Background(), `SELECT execution_state, result_json
		FROM session_runtime_tool_results
		WHERE workspace_id='default' AND session_id='sesn_bridge_pod_loss' AND tool_use_event_id='evt_pod_loss_tool'`).Scan(
		&executionState, &storedResult,
	); err != nil {
		t.Fatalf("read pod-loss execution record: %v", err)
	}
	if executionState != "running" || storedResult.Valid {
		t.Fatalf("pod-loss execution = %q/%v; want unchanged running record", executionState, storedResult)
	}
	if got := bridgeTransientAttachmentStatus(t, admin, attachment.GetAttachmentRef()); got != "staged" {
		t.Fatalf("pod-loss attachment status = %q; want preserved staged attachment", got)
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

func TestRuntimePodLossPreservesToolUseAwaitingApproval(t *testing.T) {
	for _, testCase := range []struct {
		name                  string
		openRequest           bool
		settleBeforeWake      bool
		wantPendingAtWake     int
		wantResultCountAtWake int
	}{
		{name: "settlement before replacement wake", settleBeforeWake: true, wantPendingAtWake: 0, wantResultCountAtWake: 1},
		{name: "replacement wake before settlement", wantPendingAtWake: 1, wantResultCountAtWake: 0},
		{name: "open request settlement before replacement wake", openRequest: true, settleBeforeWake: true, wantPendingAtWake: 0, wantResultCountAtWake: 1},
		{name: "open request replacement wake before settlement", openRequest: true, wantPendingAtWake: 1, wantResultCountAtWake: 0},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			suffix := strings.ReplaceAll(testCase.name, " ", "_")
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
			seedBridgeAPIDurableToolMessage(
				t, admin, "default", sessionID, threadID, modelRequestID,
				toolUseEventID, "tool-call-pod-loss-approval-"+suffix, "Write",
			)
			if _, err := admin.ExecContext(context.Background(),
				`INSERT INTO session_pending_tool_uses (
					workspace_id, session_id, session_thread_id, tool_use_event_id, model_tool_call_id,
					tool_name, input_json, status, decision, created_at, updated_at
				) VALUES ('default', $1, $2, $3, $4, 'Write',
					'{"file_path":"src/a.ts"}', $5, $6, now(), now())`,
				sessionID, threadID, toolUseEventID, "tool-call-pod-loss-approval-"+suffix, "resolving", "allow",
			); err != nil {
				t.Fatalf("seed pending approval: %v", err)
			}
			var assistantSequence int64
			if err := admin.QueryRowContext(context.Background(), `SELECT sequence FROM session_messages
				WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND model_request_id=$3`,
				sessionID, threadID, modelRequestID,
			).Scan(&assistantSequence); err != nil {
				t.Fatalf("read pending approval Assistant sequence: %v", err)
			}
			if !testCase.openRequest {
				if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_events (
				workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
				visibility, session_visible, model_request_id, projection_json, created_at, updated_at
			) VALUES
			('default',$1,$2,$4,3,'span.model_request_end',
			 jsonb_build_object(
			   'model_request_start_id',$5::text,'is_error',false,
			   'provider_context_retention',jsonb_build_object(
			     'disposition','completed','assistant_message_sequence',$6::bigint,
			     'tool_use_event_ids',jsonb_build_array($3::text),'repair_event_ids',jsonb_build_array()
			   )
			 )::text,'internal',false,$7,'{}',now(),now()),
			('default',$1,$2,$8,4,'session.status_idle',
			 '{"stop_reason":{"type":"requires_action"}}','internal',false,NULL,'{}',now(),now()),
			('default',$1,$2,$9,5,'session.status_running',
			 '{"type":"session.status_running"}','internal',false,NULL,'{}',now(),now())`,
					sessionID, threadID, toolUseEventID,
					"evt_pod_loss_approval_end_"+suffix,
					"evt_pod_loss_approval_start_"+suffix,
					assistantSequence,
					modelRequestID,
					"evt_pod_loss_approval_idle_"+suffix,
					"evt_pod_loss_approval_running_"+suffix,
				); err != nil {
					t.Fatalf("seed completed requires-action approval journey: %v", err)
				}
			}

			apiStore := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			apiStore.RuntimeBindingTokenHMACKey = []byte("bridge-pod-loss-approval-key!!")
			deliveryStore := jobrunner.NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 0)
			if err := repairLostBindingThroughProduction(context.Background(), deliveryStore, workspace.DefaultID.String(), sessionID, binding,
				time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC),
			); err != nil {
				t.Fatalf("repair pending approval after pod loss: %v", err)
			}

			queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
			var recoveryJobCount int
			var recoveryPayloadJSON string
			if err := admin.QueryRowContext(context.Background(), `SELECT count(*), COALESCE(MAX(payload_json), '')
				FROM queue_jobs WHERE workspace_id='default' AND kind=$1 AND partition_key=$2`,
				queue.KindRuntimeRecovery, queue.FormatSessionPartitionKey("default", sessionID),
			).Scan(&recoveryJobCount, &recoveryPayloadJSON); err != nil {
				t.Fatalf("read recovery Queue job: %v", err)
			}
			var recoveryPayload map[string]json.RawMessage
			if err := json.Unmarshal([]byte(recoveryPayloadJSON), &recoveryPayload); err != nil {
				t.Fatalf("decode recovery Queue payload: %v", err)
			}
			if recoveryJobCount != 1 || len(recoveryPayload) != 3 || string(recoveryPayload["source_event_id"]) != `"`+toolUseEventID+`"` {
				t.Fatalf("recovery Queue job = %d/%s; want one exact Tool root and no envelope echoes", recoveryJobCount, recoveryPayloadJSON)
			}
			bridgeAddress, stopBridge := serveAttachmentCompositionBridge(t, apiStore)
			t.Cleanup(stopBridge)
			replacement := enginekubernetes.BindingCandidate{
				Namespace: "tetral-agent-runtime", PodName: "runtime-recovery-" + suffix,
				PodUID: "pod-recovery-" + suffix, PodIP: "127.0.0.1",
			}
			seedFixtureRuntimeProcess(t, dbconnect.NewClientForTesting(admin), replacement.Namespace, replacement.PodUID)
			runtimeProcess := startProviderRecoveryRuntime(
				t, bridgeAddress, sessionID, threadID, replacement.PodUID,
				time.Date(2026, 1, 1, 0, 5, 1, 0, time.UTC), false,
			)
			deliveryStore.RuntimeGRPCPort = runtimeProcess.port
			deliveryStore.TargetResolver = jobrunner.KubernetesRuntimeTargetResolver{LoadClient: fixtureRuntimeLoadClient(t), Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
				return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{replacement})
			}}
			settle := func(scope *bridgev1.RuntimeScope) {
				t.Helper()
				response, settleErr := apiStore.SettleToolResult(context.Background(), bridgeToolSettlementRequestForTest(
					scope,
					bridgeErrorToolSettlementForTest(toolUseEventID, "Approval denied: cancel"),
				))
				if settleErr != nil || response.GetCommitted() == nil {
					t.Fatalf("settle Tool route under replacement custody = %#v/%v; want committed", response, settleErr)
				}
			}
			sender := &settlingRecoveryCommandClient{RuntimePodCommandClient: fixtureRuntimeCommandClient(t, providerRecoveryTokenSource{})}
			if testCase.settleBeforeWake {
				sender.beforeRecover = func(request *agentruntimev1.RecoverThreadRequest) error {
					settle(bridgeAPIScope(sessionID, threadID, request.GetBindingId(), request.GetBindingGeneration(), request.GetTargetPodUid()))
					return nil
				}
			}
			runner := &jobrunner.JobRunner{
				Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID},
				Deliverer: jobrunner.RuntimePodDirectDeliverer{Store: deliveryStore, Sender: sender},
				Config:    jobrunner.JobRunnerConfig{LeaseOwner: "bridge-runtime-recovery", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
			}
			active, err := runner.RunOnceWithActivity(context.Background())
			if err != nil || !active {
				t.Fatalf("deliver Runtime recovery job = active:%t err:%v; want one accepted delivery", active, err)
			}
			if sender.err != nil || (sender.response.GetAccepted() == nil && sender.response.GetDuplicate() == nil) {
				var queueStatus, sessionStatus, runtimeStatus, sourceType string
				var lastErrorKind sql.NullString
				if readErr := admin.QueryRowContext(context.Background(), `SELECT
					(SELECT status FROM queue_jobs WHERE workspace_id='default' AND kind=$2 AND partition_key=$3),
					(SELECT last_error_kind FROM queue_jobs WHERE workspace_id='default' AND kind=$2 AND partition_key=$3),
					(SELECT status FROM sessions WHERE workspace_id='default' AND id=$1),
					(SELECT status FROM session_runtime_status WHERE workspace_id='default' AND session_id=$1),
					(SELECT type FROM session_events WHERE workspace_id='default' AND session_id=$1 AND event_id=$4)`,
					sessionID, queue.KindRuntimeRecovery, queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID), toolUseEventID,
				).Scan(&queueStatus, &lastErrorKind, &sessionStatus, &runtimeStatus, &sourceType); readErr != nil {
					t.Fatalf("read rejected Runtime recovery facts: %v", readErr)
				}
				t.Fatalf("Runtime recovery response = %#v/%v Queue=%s error=%v Session=%s Runtime=%s source=%s; want accepted or duplicate", sender.response, sender.err, queueStatus, lastErrorKind, sessionStatus, runtimeStatus, sourceType)
			}
			preloaded := runtimeProcess.recoveryResult(t)
			if preloaded.Command.SourceEventID != toolUseEventID || preloaded.Command.TargetPodUID != replacement.PodUID {
				t.Fatalf("Runtime recovery command = %#v; want exact Tool root and resolver-owned replacement", preloaded.Command)
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
			if resultCount != testCase.wantResultCountAtWake || strings.Contains(resultPayload, `"reason":"runtime_pod_lost"`) {
				t.Fatalf("approval pod-loss result = %d/%s; want %d terminal settlement Results and no synthetic repair Result; preload=%s",
					resultCount, resultPayload, testCase.wantResultCountAtWake, preloaded.LastSnapshot)
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
			wantPendingStatus := "resolving"
			if testCase.settleBeforeWake {
				wantPendingStatus = "resolved"
			}
			if pendingStatus != wantPendingStatus || pendingResultEventID.Valid != testCase.settleBeforeWake || (testCase.settleBeforeWake && pendingResultEventID.String != resultEventID) {
				t.Fatalf("approval row = %q/%v; want explicit %s state", pendingStatus, pendingResultEventID, wantPendingStatus)
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
			if requestEndCount != 1 || requestEndIsError != testCase.openRequest {
				t.Fatalf("approval Request End count/error = %d/%v; want one request with error=%t", requestEndCount, requestEndIsError, testCase.openRequest)
			}

			snapshot := string(preloaded.LastSnapshot)
			wantUnsettledOwner := testCase.wantPendingAtWake == 1
			if strings.Contains(snapshot, `"hasUnsettledToolOwner":true`) != wantUnsettledOwner ||
				(wantUnsettledOwner && !testCase.openRequest && (!strings.Contains(snapshot, `"contextKind":"assistant"`) ||
					!strings.Contains(snapshot, `"modelToolCallId":"tool-call-pod-loss-approval-`+suffix+`"`))) {
				t.Fatalf("replacement Runtime checkpoint = %s; want unsettled owner %t with exact Assistant Tool Call", snapshot, wantUnsettledOwner)
			}
			runtimeProcess.close(t)
		})
	}
}
