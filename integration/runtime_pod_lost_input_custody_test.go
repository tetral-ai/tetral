package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
)

func TestPostgreSQLMalformedAgentMailReplacementPassesReplayAndDelivers(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID  = "sesn_agent_mail_replacement_replay"
		mainID     = "thr_agent_mail_replacement_main"
		childID    = "thr_agent_mail_replacement_child"
		deliveryID = "delivery_agent_mail_replacement"
		bindingID  = "bind_agent_mail_replacement"
		podUID     = "pod_agent_mail_replacement"
	)
	now := time.Now().UTC().Add(-time.Minute)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedCompletionMailSentAt(t, admin, sessionID, mainID, childID, deliveryID, 1, now.Format(time.RFC3339Nano))

	var malformedJobID string
	if err := admin.QueryRowContext(context.Background(), `UPDATE queue_jobs
		SET payload_json=jsonb_set(payload_json::jsonb, '{session_thread_id}', to_jsonb($2::text), false)::text
		WHERE workspace_id='default' AND kind='runtime_input'
		  AND payload_json::jsonb ->> 'runtime_input_id'=$1
		RETURNING id`, "agent_mail:"+deliveryID, childID).Scan(&malformedJobID); err != nil {
		t.Fatalf("corrupt agent-mail Queue thread: %v", err)
	}
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	seedFixtureRuntimeProcess(t, dbconnect.NewClientForTesting(admin), "tetral-agent-runtime", podUID)
	deliveryStore := jobrunner.NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090, jobrunner.KubernetesRuntimeTargetResolver{LoadClient: fixtureRuntimeLoadClient(t), Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{
			Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: podUID, PodIP: "10.0.0.10",
		}})
	}})
	deliveryStore.Clock = func() time.Time { return now }
	bridgeStore := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	sender := &committingAgentMailSender{
		recordingRuntimeCommandSender: &recordingRuntimeCommandSender{result: jobrunner.RuntimeDeliveryResult{Status: jobrunner.RuntimeDeliveryAccepted}},
		bridge:                        bridgeStore,
	}
	runner := &jobrunner.JobRunner{
		Queue:      tetralqueue.NewServer(queueStore, nil),
		Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer:  jobrunner.RuntimePodDirectDeliverer{Store: deliveryStore, Sender: sender},
		Config:     jobrunner.JobRunnerConfig{MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("replace malformed agent-mail custody: %v", err)
	}
	if len(sender.requests) != 0 {
		t.Fatalf("Runtime requests after malformed lease = %d; want zero", len(sender.requests))
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("deliver canonical agent-mail replacement: %v", err)
	}
	if len(sender.requests) != 1 {
		t.Fatalf("Runtime requests after canonical replacement = %d; want one", len(sender.requests))
	}
	if sender.commitErr != nil {
		t.Fatalf("commit canonical replacement Request Start: %v", sender.commitErr)
	}
	var oldStatus string
	var deadLetters, pending, acknowledged int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$1),
		count(*) FILTER (WHERE status='dead_lettered'),
		count(*) FILTER (WHERE status='pending'),
		count(*) FILTER (WHERE status='acknowledged')
		FROM queue_jobs WHERE workspace_id='default'
		  AND payload_json::jsonb ->> 'runtime_input_id'=$2`, malformedJobID, "agent_mail:"+deliveryID,
	).Scan(&oldStatus, &deadLetters, &pending, &acknowledged); err != nil {
		t.Fatalf("read agent-mail replacement lifecycle: %v", err)
	}
	if oldStatus != queue.StatusDeadLettered || deadLetters != 1 || pending != 0 || acknowledged != 1 {
		t.Fatalf("agent-mail replacement lifecycle = old %s dead/pending/ack %d/%d/%d; want dead_lettered 1/0/1", oldStatus, deadLetters, pending, acknowledged)
	}
}

type committingAgentMailSender struct {
	*recordingRuntimeCommandSender
	bridge    *agentruntimebridge.PostgreSQLBridgeAPIStore
	commitErr error
}

func (s *committingAgentMailSender) AcceptAgentMail(
	ctx context.Context,
	target jobrunner.RuntimePodTarget,
	request *agentruntimev1.AcceptAgentMailRequest,
) (*agentruntimev1.AcceptAgentMailResponse, error) {
	response, err := s.recordingRuntimeCommandSender.AcceptAgentMail(ctx, target, request)
	if err != nil {
		return nil, err
	}
	scope := bridgeAPIScope(
		request.GetSessionId(),
		request.GetSessionThreadId(),
		request.GetBindingId(),
		request.GetBindingGeneration(),
		request.GetTargetPodUid(),
	)
	committed, err := s.bridge.CommitInputs(ctx, &bridgev1.CommitInputsRequest{
		Scope: scope, RuntimeInputId: request.GetRuntimeInputId(),
	})
	if err != nil || committed.GetCommitted() == nil {
		if err == nil {
			err = errors.New("canonical replacement input was not committed")
		}
		s.commitErr = err
		return response, err
	}
	sequences := committed.GetCommitted().GetContext().GetAssignedContextSequences()
	if len(sequences) != 1 {
		err = errors.New("canonical replacement input assigned an invalid context sequence")
		s.commitErr = err
		return response, err
	}
	boundary := sequences[0]
	_, err = s.bridge.WriteEvent(ctx, &bridgev1.WriteEventRequest{
		Scope:                         scope,
		RuntimeWriteId:                "rwrite_malformed_mail_replacement_start",
		ModelRequestId:                "mreq_malformed_mail_replacement_start",
		EventType:                     "span.model_request_start",
		PayloadJson:                   `{"type":"span.model_request_start","model_request_id":"mreq_malformed_mail_replacement_start"}`,
		ContextThroughMessageSequence: &boundary,
		RequestKind:                   runtimecontrol.RequestKindAgentProviderRequest,
	})
	s.commitErr = err
	return response, err
}

func TestPostgreSQLRuntimePodLossReplacementQueueCustodyPreservesInboxOrder(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_pod_loss_queue_order"
		threadID  = "thr_pod_loss_queue_order"
		bindingID = "bind_pod_loss_queue_order"
		podUID    = "pod_pod_loss_queue_order"
		earlyID   = "rin_z_pod_loss_early"
		middleID  = "task_notification:task_m_pod_loss_middle"
		lateID    = "agent_mail:a_pod_loss_late"
		taskID    = "task_m_pod_loss_middle"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "evt_pod_loss_early", 1, "user.message", `{"content":[{"type":"text","text":"first"}]}`)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "evt_pod_loss_late", 2, "agent.thread_message_received", bridgeInterAgentMessageJSON(
		t, "a_pod_loss_late", threadID, "evt_pod_loss_mail_source", bridgePublicMessageJSONForTest(t, "third"),
	))
	seedBridgeAPINotifiableBackgroundTask(t, admin, "default", sessionID, threadID, bindingID, taskID, "evt_pod_loss_task_source")
	taskResult := `{"task_id":"task_m_pod_loss_middle","source_tool_use_event_id":"evt_pod_loss_task_source","status":"completed","stdout":{"text":"second","truncated":false},"stderr":{"text":"","truncated":false},"exit_code":0}`
	settleBridgeAPIBackgroundTask(t, admin, sessionID, taskID, "completed", taskResult)
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, earlyID, "messages", `["evt_pod_loss_early"]`, "accepted", bindingID, podUID, 1, 1)
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, middleID, "task_notification", `[]`, "accepted", bindingID, podUID, 0, 0)
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, lateID, "agent_mail", `["evt_pod_loss_late"]`, "accepted", bindingID, podUID, 2, 2)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_inbox
		SET created_at = CASE runtime_input_id
		  WHEN $1 THEN '2026-08-10T10:00:00Z'::timestamptz
		  WHEN $2 THEN '2026-08-10T10:00:01Z'::timestamptz
		  ELSE '2026-08-10T10:00:02Z'::timestamptz
		END
		WHERE workspace_id = 'default' AND runtime_input_id IN ($1, $2, $3)`, earlyID, middleID, lateID); err != nil {
		t.Fatalf("stamp Inbox creation order: %v", err)
	}

	if handedOff := handOffLostRuntimeInputsForTest(
		t,
		runtime,
		sessionID,
		bindingID,
		podUID,
		time.Date(2026, 8, 10, 14, 0, 0, 0, time.UTC),
	); handedOff != 3 {
		t.Fatalf("ordered handoff count = %d; want 3", handedOff)
	}
	rows, err := admin.QueryContext(context.Background(), `SELECT payload_json::jsonb ->> 'runtime_input_id'
		FROM queue_jobs
		WHERE workspace_id = 'default'
		  AND kind = 'runtime_input'
		  AND payload_json::jsonb ->> 'session_id' = $1
		ORDER BY queue_partition_sequence`, sessionID)
	if err != nil {
		t.Fatalf("read replacement Queue order: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var ordered []string
	for rows.Next() {
		var runtimeInputID string
		if err := rows.Scan(&runtimeInputID); err != nil {
			t.Fatalf("scan replacement Queue order: %v", err)
		}
		ordered = append(ordered, runtimeInputID)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate replacement Queue order: %v", err)
	}
	if len(ordered) != 3 || ordered[0] != earlyID || ordered[1] != middleID || ordered[2] != lateID {
		t.Fatalf("replacement Queue order = %v; want mixed-kind Inbox creation order [%s %s %s]", ordered, earlyID, middleID, lateID)
	}
	runtimeClient := dbconnect.NewClientForTesting(runtime)
	replacementScope := declareReplacementScope(t, runtimeClient, runtimeClient, bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID))
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_inbox
		SET status='accepted',binding_id=$2,binding_generation=$4,target_pod_uid=$3
		WHERE workspace_id='default' AND session_id=$1`, sessionID, replacementScope.GetBinding().GetBindingId(), replacementScope.GetBinding().GetTargetPodUid(), replacementScope.GetBinding().GetBindingGeneration()); err != nil {
		t.Fatalf("accept replacement inputs in Queue order: %v", err)
	}

	store := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("pod-loss-context-order-test-key-32")
	scope := replacementScope
	if _, err := store.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope: scope, RuntimeInputId: earlyID,
	}); err != nil {
		t.Fatalf("commit first replacement input: %v", err)
	}
	if response, err := store.CommitTaskNotificationResult(context.Background(), bridgeTaskNotificationRequestForTest(t, scope, middleID)); err != nil || response.GetCommitted() == nil {
		t.Fatalf("commit middle replacement input: %v", err)
	}
	if _, err := store.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope: scope, RuntimeInputId: lateID,
	}); err != nil {
		t.Fatalf("commit final replacement input: %v", err)
	}
	loaded, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope})
	if err != nil {
		t.Fatalf("load replacement Context: %v", err)
	}
	var payload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(loaded.GetContextJson()), &payload); err != nil {
		t.Fatalf("decode replacement Context: %v", err)
	}
	if len(payload.ContextEntries) != 3 {
		t.Fatalf("replacement Context messages = %s; want three mixed-kind inputs", loaded.GetContextJson())
	}
	var contextTexts []string
	for _, entry := range payload.ContextEntries {
		var part struct {
			Text string `json:"text"`
		}
		if len(entry.Parts) != 1 {
			t.Fatalf("replacement Context entry parts = %#v; want one", entry.Parts)
		}
		if err := json.Unmarshal(entry.Parts[0], &part); err != nil {
			t.Fatalf("decode replacement Context part: %v; part=%s", err, entry.Parts[0])
		}
		contextTexts = append(contextTexts, part.Text)
	}
	if len(contextTexts) != 3 || contextTexts[0] != "first" || !strings.Contains(contextTexts[1], taskID) || contextTexts[2] != "third" {
		t.Fatalf("replacement Context order = %v; want user/task/agent-mail creation order; context=%s", contextTexts, loaded.GetContextJson())
	}
}

func TestPostgreSQLRuntimePodLossReplacementCommitsTheSameAcceptedInputOnce(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_pod_loss_commit"
		threadID       = "thr_pod_loss_commit"
		runtimeInputID = "rin_pod_loss_commit"
		eventID        = "evt_pod_loss_commit"
		oldBindingID   = "bind_pod_loss_commit_old"
		oldPodUID      = "pod_pod_loss_commit_old"
		originalJobID  = "qjob_pl_commit"
	)
	now := time.Date(2026, 8, 11, 3, 0, 0, 0, time.UTC)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, oldBindingID, 1, oldPodUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, oldBindingID, 1)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_status SET status='idle'
		WHERE workspace_id='default' AND session_id=$1`, sessionID); err != nil {
		t.Fatalf("seed pre-running Runtime status: %v", err)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 1, "user.message", `{"content":[{"type":"text","text":"continue after replacement"}]}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, runtimeInputID, "messages", `["`+eventID+`"]`, "accepted", oldBindingID, oldPodUID, 1, 1)
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	request := queue.EnqueueRequest{WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput, PartitionKey: queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID), DedupeKey: queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, runtimeInputID), PayloadVersion: 1, PayloadJSON: []byte(`{"workspace_id":"default","session_id":"` + sessionID + `","session_thread_id":"` + threadID + `","runtime_input_id":"` + runtimeInputID + `","event_ids":["` + eventID + `"],"sequence_from":1,"sequence_to":1,"input_kind":"messages"}`), Now: now}
	request.ID = originalJobID
	if _, err := queueStore.Enqueue(context.Background(), request); err != nil {
		t.Fatalf("enqueue original input custody: %v", err)
	}
	originalLease, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "bridge-pod-loss-original",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(time.Second),
	})
	if err != nil || len(originalLease) != 1 {
		t.Fatalf("lease original input custody = %#v, %v; want one", originalLease, err)
	}
	if updated, err := queueStore.Ack(context.Background(), queue.AckRequest{
		WorkspaceID: workspace.DefaultID, JobID: originalJobID, LeaseToken: originalLease[0].LeaseToken, Now: now.Add(2 * time.Second),
	}); err != nil || !updated {
		t.Fatalf("ACK original input custody = %t, %v; want true", updated, err)
	}

	oldPod := enginekubernetes.BoundRuntimePod{
		Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: oldPodUID, PodIP: "10.0.0.10",
	}
	replacement := enginekubernetes.BindingCandidate{
		Namespace: "tetral-agent-runtime", PodName: "runtime-pod-1", PodUID: "pod_pod_loss_commit_new", PodIP: "10.0.0.11",
	}
	seedFixtureRuntimeProcess(t, dbconnect.NewClientForTesting(admin), replacement.Namespace, replacement.PodUID)
	store := runtimePodLossSweepStore(t, runtime, nil, func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotStateWithCandidatesForTest(
			true, oldPod, enginekubernetes.BindingVisibilityDeleted, []enginekubernetes.BindingCandidate{replacement},
		)
	})
	if repaired, err := store.RepairLostRuntimeBindings(context.Background(), "default"); err != nil || repaired != 1 {
		t.Fatalf("repair accepted input after proven Pod loss = %d, %v; want one", repaired, err)
	}
	replacementLease, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "bridge-pod-loss-replacement",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(3 * time.Second),
	})
	if err != nil || len(replacementLease) != 1 || replacementLease[0].ID == originalJobID {
		t.Fatalf("lease replacement input custody = %#v, %v; want one new job", replacementLease, err)
	}
	job, err := jobrunner.DecodeRuntimeJob(queueJobProto(replacementLease[0]))
	if err != nil {
		t.Fatalf("decode replacement input custody: %v", err)
	}
	plan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil || plan.AcceptInput == nil || plan.AcceptInput.GetTargetPodUid() != replacement.PodUID {
		t.Fatalf("prepare replacement input = %#v, %v; want new Pod", plan, err)
	}
	if settled, err := store.MarkRuntimeInputAccepted(context.Background(), job, plan.AttemptedBinding); err != nil || settled {
		t.Fatalf("mark replacement input accepted = %t, %v; want accepted without deferral", settled, err)
	}
	if updated, err := queueStore.Ack(context.Background(), queue.AckRequest{
		WorkspaceID: workspace.DefaultID, JobID: replacementLease[0].ID, LeaseToken: replacementLease[0].LeaseToken, Now: now.Add(4 * time.Second),
	}); err != nil || !updated {
		t.Fatalf("ACK replacement delivery custody = %t, %v; want true", updated, err)
	}
	commit := &bridgev1.CommitInputsRequest{
		Scope: observedAttemptScope(job, plan.AttemptedBinding), RuntimeInputId: runtimeInputID,
	}
	apiStore := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	first, err := apiStore.CommitInputs(context.Background(), commit)
	if err != nil || first.GetCommitted() == nil {
		t.Fatalf("commit replacement input = %#v, %v; want committed", first, err)
	}
	replay, err := apiStore.CommitInputs(context.Background(), commit)
	if err != nil || replay.GetCommitted() == nil {
		t.Fatalf("replay replacement input commit = %#v, %v; want committed", replay, err)
	}
	var inboxStatus string
	var processed, messages, lineage int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND event_id=$2 AND processed_at IS NOT NULL),
		(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND source_event_id=$2),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$3)`,
		runtimeInputID, eventID, request.DedupeKey,
	).Scan(&inboxStatus, &processed, &messages, &lineage); err != nil {
		t.Fatalf("read replacement commit evidence: %v", err)
	}
	if inboxStatus != "committed" || processed != 1 || messages != 1 || lineage != 2 {
		t.Fatalf("replacement commit = Inbox %q processed %d messages %d lineage %d; want committed/1/1/2", inboxStatus, processed, messages, lineage)
	}
}

func handOffLostRuntimeInputsForTest(t *testing.T, runtime *sql.DB, sessionID string, bindingID string, podUID string, now time.Time) int {
	t.Helper()
	// repairLostBindingThroughProduction installs the confirmed-loss visibility
	// for its repair; the store resolves no other target.
	store := jobrunner.NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090, nil)
	if err := repairLostBindingThroughProduction(context.Background(), store, "default", sessionID, runtimecontrol.Binding{BindingID: bindingID, BindingGeneration: 1, PodUID: podUID}, now); err != nil {
		t.Fatalf("hand off lost Runtime inputs: %v", err)
	}
	var handedOff int
	if err := store.Client.WithWorkspaceReadOnlyTx(context.Background(), "default", "test.handoff_observation", func(tx *dbconnect.Tx) error {
		return tx.QueryRow(context.Background(), `SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND status='queued' AND binding_id IS NULL`, sessionID).Scan(&handedOff)
	}); err != nil {
		t.Fatalf("read handed off custody: %v", err)
	}
	return handedOff
}
func mustLeaseBridgeQueueJob(t *testing.T, store *queue.PostgreSQLQueueStore, request queue.LeaseRequest) *queue.Job {
	t.Helper()
	jobs, err := store.Lease(context.Background(), request)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("lease Queue job = %#v/%v; want exactly one", jobs, err)
	}
	return jobs[0]
}
