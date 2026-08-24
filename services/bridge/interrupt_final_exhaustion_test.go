package agentruntimebridge

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
)

func TestPostgreSQLInterruptReceiptAndFinalExhaustionChooseOneSessionWinner(t *testing.T) {
	for _, receiptFirst := range []bool{true, false} {
		name := "receipt_first"
		if !receiptFirst {
			name = "termination_first"
		}
		t.Run(name, func(t *testing.T) {
			runInterruptReceiptExhaustionRace(t, receiptFirst)
		})
	}
}

func TestPostgreSQLInterruptFinalizerLosingLeaseAfterReclaimWritesNothing(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_interrupt_stale_finalizer"
		threadID  = "thr_interrupt_stale_finalizer"
		bindingID = "bind_interrupt_stale_finalizer"
		podUID    = "pod_interrupt_stale_finalizer"
		inputID   = "rin_interrupt_stale_finalizer"
		eventID   = "evt_interrupt_stale_finalizer"
	)
	now := time.Now().UTC().Add(-time.Minute)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 1, "user.interrupt", `{}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, inputID, "interrupt_control", `["`+eventID+`"]`, "accepted", bindingID, podUID, 1, 1)

	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	enqueueInterruptExhaustionJob(t, queueStore, sessionID, threadID, inputID, "interrupt_control", eventID, 1, 1, now)
	first := mustLeaseBridgeQueueJob(t, queueStore, queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "stale-finalizer",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: now,
	})
	staleJob, err := DecodeRuntimeJob(queueJobProto(first))
	if err != nil {
		t.Fatalf("decode stale interrupt lease: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE queue_jobs SET leased_until=clock_timestamp()-interval '1 second'
		WHERE workspace_id='default' AND id=$1`, first.ID); err != nil {
		t.Fatalf("expire first interrupt lease: %v", err)
	}
	if reclaimed, err := queueStore.ReclaimExpiredLeases(context.Background(), queue.ReclaimExpiredLeasesRequest{
		WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput, Limit: 1,
	}); err != nil || reclaimed != 1 {
		t.Fatalf("reclaim first interrupt lease = %d/%v; want 1/nil", reclaimed, err)
	}
	current := mustLeaseBridgeQueueJob(t, queueStore, queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "current-finalizer",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: time.Now().UTC(),
	})
	if current.LeaseToken == first.LeaseToken {
		t.Fatal("reclaimed interrupt lease reused its old token")
	}

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	result, err := store.FinalizeRuntimeDelivery(context.Background(), staleJob, RuntimeDeliveryResult{
		Status: RuntimeDeliveryRejected, ErrorKind: "runtime_delivery_exhausted",
	})
	if err != nil || result.QueueLeaseSettled || result.Status != RuntimeDeliveryAuthorityLost {
		t.Fatalf("stale finalizer result = %+v/%v; want fenced authority loss without a false Queue settlement", result, err)
	}
	var sessionStatus, inboxStatus, queueStatus, leaseToken string
	var bindings, terminalEvents int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM sessions WHERE workspace_id='default' AND id=$1),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$2),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$3),
		(SELECT lease_token FROM queue_jobs WHERE workspace_id='default' AND id=$3),
		(SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type IN ('session.error','session.status_terminated'))`,
		sessionID, inputID, current.ID,
	).Scan(&sessionStatus, &inboxStatus, &queueStatus, &leaseToken, &bindings, &terminalEvents); err != nil {
		t.Fatalf("read stale-finalizer facts: %v", err)
	}
	if sessionStatus != "idle" || inboxStatus != "accepted" || queueStatus != queue.StatusLeased ||
		leaseToken != current.LeaseToken || bindings != 1 || terminalEvents != 0 {
		t.Fatalf("stale finalizer mutated facts: Session %s Inbox %s Queue %s/%s bindings %d terminalEvents %d",
			sessionStatus, inboxStatus, queueStatus, leaseToken, bindings, terminalEvents)
	}
}

func TestPostgreSQLJobRunnerLosingInterruptLeaseBeforeReplayCannotCancelMessages(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_interrupt_stale_runner"
		threadID       = "thr_interrupt_stale_runner"
		interruptID    = "rin_interrupt_stale_runner"
		interruptEvent = "evt_interrupt_stale_runner"
		messageID      = "rin_interrupt_stale_runner_message"
		messageEvent   = "evt_interrupt_stale_runner_message"
	)
	now := time.Now().UTC().Add(-time.Minute)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, messageEvent, 3, "user.message", `{"content":[{"type":"text","text":"before interrupt"}]}`)
	seedRuntimeInboxBirthForJob(t, admin, RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: messageID, InputKind: "messages", EventIDs: []string{messageEvent}, SequenceFrom: 3, SequenceTo: 3,
	})
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, interruptEvent, 4, "user.interrupt", `{"type":"user.interrupt"}`)
	seedRuntimeInboxBirthForJob(t, admin, RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: interruptID, InputKind: "interrupt_control", EventIDs: []string{interruptEvent}, SequenceFrom: 4, SequenceTo: 4,
	})

	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	// The interrupt is enqueued first so it owns the first lease while the
	// lower-sequence message remains a pending fence target.
	enqueueInterruptExhaustionJob(t, queueStore, sessionID, threadID, interruptID, "interrupt_control", interruptEvent, 4, 3, now)
	enqueueInterruptExhaustionJob(t, queueStore, sessionID, threadID, messageID, "messages", messageEvent, 3, queue.DefaultMaxAttempts, now.Add(time.Second))
	first := mustLeaseBridgeQueueJob(t, queueStore, queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "stale-runner",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: now,
	})
	if _, err := admin.ExecContext(context.Background(), `UPDATE queue_jobs SET leased_until=clock_timestamp()-interval '1 second'
		WHERE workspace_id='default' AND id=$1`, first.ID); err != nil {
		t.Fatalf("expire stale JobRunner lease: %v", err)
	}
	if reclaimed, err := queueStore.ReclaimExpiredLeases(context.Background(), queue.ReclaimExpiredLeasesRequest{
		WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput, Limit: 1,
	}); err != nil || reclaimed != 1 {
		t.Fatalf("reclaim stale JobRunner lease = %d/%v; want 1/nil", reclaimed, err)
	}
	current := mustLeaseBridgeQueueJob(t, queueStore, queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "current-runner",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: time.Now().UTC(),
	})
	if current.ID != first.ID || current.LeaseToken == first.LeaseToken {
		t.Fatalf("current interrupt lease = %s/%s; want reclaimed %s with a new token", current.ID, current.LeaseToken, first.ID)
	}

	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	deliverer := &postgresFinalizingDeliverer{store: deliveryStore}
	runner := &JobRunner{Queue: tetralqueue.NewServer(queueStore, nil), Deliverer: deliverer}
	if err := runner.processRuntimeJob(context.Background(), queueJobProto(first), JobRunnerConfig{}); err != nil {
		t.Fatalf("resume stale JobRunner: %v", err)
	}
	var messageQueueStatus, messageInboxStatus, interruptQueueStatus, currentToken string
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$1),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$2),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$3),
		(SELECT lease_token FROM queue_jobs WHERE workspace_id='default' AND id=$3)`,
		queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, messageID), messageID, current.ID,
	).Scan(&messageQueueStatus, &messageInboxStatus, &interruptQueueStatus, &currentToken); err != nil {
		t.Fatalf("read stale JobRunner Queue facts: %v", err)
	}
	if messageQueueStatus != queue.StatusPending || messageInboxStatus != "queued" ||
		interruptQueueStatus != queue.StatusLeased || currentToken != current.LeaseToken || deliverer.deliveries != 0 {
		t.Fatalf("stale JobRunner mutated facts: message Queue/Inbox %s/%s interrupt %s/%s deliveries %d",
			messageQueueStatus, messageInboxStatus, interruptQueueStatus, currentToken, deliverer.deliveries)
	}
}

func TestPostgreSQLInterruptLeaseLossAfterReplayStopsBeforePreparationAndSend(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_interrupt_post_replay_lease_loss"
		threadID  = "thr_interrupt_post_replay_lease_loss"
		bindingID = "bind_interrupt_post_replay_lease_loss"
		podUID    = "pod_interrupt_post_replay_lease_loss"
		inputID   = "rin_interrupt_post_replay_lease_loss"
		eventID   = "evt_interrupt_post_replay_lease_loss"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 1, "user.interrupt", `{"type":"user.interrupt"}`)
	seedRuntimeInboxBirthForJob(t, admin, RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: inputID, InputKind: "interrupt_control", EventIDs: []string{eventID}, SequenceFrom: 1, SequenceTo: 1,
	})

	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	enqueueInterruptExhaustionJob(t, queueStore, sessionID, threadID, inputID, "interrupt_control", eventID, 1, 3, time.Now().UTC().Add(-time.Minute))
	first := mustLeaseBridgeQueueJob(t, queueStore, queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "post-replay-stale-runner",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: time.Now().UTC(),
	})
	job, err := DecodeRuntimeJob(queueJobProto(first))
	if err != nil {
		t.Fatalf("decode first interrupt lease: %v", err)
	}
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	if replayed, found, err := deliveryStore.ReplayRuntimeDeliveryFinalization(context.Background(), job); err != nil || found {
		t.Fatalf("successful pre-delivery replay = %+v/%v/%v; want no receipt", replayed, found, err)
	}

	if _, err := admin.ExecContext(context.Background(), `UPDATE queue_jobs SET leased_until=clock_timestamp()-interval '1 second'
		WHERE workspace_id='default' AND id=$1`, first.ID); err != nil {
		t.Fatalf("expire post-replay lease: %v", err)
	}
	if reclaimed, err := queueStore.ReclaimExpiredLeases(context.Background(), queue.ReclaimExpiredLeasesRequest{
		WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput, Limit: 1,
	}); err != nil || reclaimed != 1 {
		t.Fatalf("reclaim post-replay lease = %d/%v; want 1/nil", reclaimed, err)
	}
	current := mustLeaseBridgeQueueJob(t, queueStore, queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "post-replay-current-runner",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: time.Now().UTC(),
	})
	if current.ID != first.ID || current.LeaseToken == first.LeaseToken {
		t.Fatalf("current interrupt lease = %s/%s; want reclaimed %s with a new token", current.ID, current.LeaseToken, first.ID)
	}

	sender := &recordingRuntimeCommandSender{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}
	result, err := (RuntimePodDirectDeliverer{Store: deliveryStore, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
	if err != nil {
		t.Fatalf("deliver stale post-replay interrupt: %v", err)
	}
	if result.Status != RuntimeDeliveryAuthorityLost || result.QueueLeaseSettled || len(sender.requests) != 0 {
		t.Fatalf("stale post-replay delivery = %+v with %d Runtime calls; want authority loss and zero calls", result, len(sender.requests))
	}

	var inboxStatus, queueStatus, leaseToken string
	var bindings, receipts, messages int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT lease_token FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$3),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$3
		 AND operation IN ('commit_inputs','write_request_end') AND source_kind='interrupt_control' AND idempotency_key=$1 AND receipt_json <> ''),
		(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$3)`,
		inputID, current.ID, sessionID,
	).Scan(&inboxStatus, &queueStatus, &leaseToken, &bindings, &receipts, &messages); err != nil {
		t.Fatalf("read post-replay stale-worker facts: %v", err)
	}
	if inboxStatus != "queued" || queueStatus != queue.StatusLeased || leaseToken != current.LeaseToken || bindings != 1 || receipts != 0 || messages != 0 {
		t.Fatalf("post-replay stale worker mutated facts: Inbox %s Queue %s/%s bindings %d receipts %d messages %d",
			inboxStatus, queueStatus, leaseToken, bindings, receipts, messages)
	}
}

func runInterruptReceiptExhaustionRace(t *testing.T, receiptFirst bool) {
	t.Helper()
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_interrupt_receipt_exhaustion_race"
		threadID  = "thr_interrupt_receipt_exhaustion_race"
		bindingID = "bind_interrupt_receipt_exhaustion_race"
		podUID    = "pod_interrupt_receipt_exhaustion_race"
		inputID   = "rin_interrupt_receipt_exhaustion_race"
		eventID   = "evt_interrupt_receipt_exhaustion_race"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 1, "user.interrupt", `{"type":"user.interrupt"}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, inputID, "interrupt_control", `["`+eventID+`"]`, "accepted", bindingID, podUID, 1, 1)
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	enqueueInterruptExhaustionJob(t, queueStore, sessionID, threadID, inputID, "interrupt_control", eventID, 1, 1, time.Now().UTC().Add(-time.Minute))
	leased := mustLeaseBridgeQueueJob(t, queueStore, queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "interrupt-race",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: time.Now().UTC(),
	})
	job, err := DecodeRuntimeJob(queueJobProto(leased))
	if err != nil {
		t.Fatalf("decode interrupt race lease: %v", err)
	}
	apiStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	commitRequest := &bridgev1.CommitInputsRequest{
		Scope: bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID), RuntimeInputId: inputID,
		InterruptLeaseRef: bridgeInterruptLeaseRef(leased),
	}

	blocker, err := admin.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin Session blocker: %v", err)
	}
	defer func() { _ = blocker.Rollback() }()
	var lockedSession string
	if err := blocker.QueryRow(`SELECT id FROM sessions WHERE workspace_id='default' AND id=$1 FOR UPDATE`, sessionID).Scan(&lockedSession); err != nil {
		t.Fatalf("lock Session: %v", err)
	}
	var blockerPID int
	if err := blocker.QueryRow(`SELECT pg_backend_pid()`).Scan(&blockerPID); err != nil {
		t.Fatalf("read blocker pid: %v", err)
	}
	type outcome struct {
		kind      string
		commit    *bridgev1.CommitInputsResponse
		finalized RuntimeDeliveryResult
		err       error
	}
	results := make(chan outcome, 2)
	startReceipt := func() {
		go func() {
			response, err := apiStore.CommitInputs(context.Background(), commitRequest)
			results <- outcome{kind: "receipt", commit: response, err: err}
		}()
	}
	startTermination := func() {
		go func() {
			result, err := deliveryStore.FinalizeRuntimeDelivery(context.Background(), job, RuntimeDeliveryResult{
				Status: RuntimeDeliveryRejected, ErrorKind: "runtime_delivery_exhausted",
			})
			results <- outcome{kind: "termination", finalized: result, err: err}
		}()
	}
	if receiptFirst {
		startReceipt()
		waitForPostgreSQLLockWaiters(t, admin, blockerPID, 1)
		startTermination()
	} else {
		startTermination()
		waitForPostgreSQLLockWaiters(t, admin, blockerPID, 1)
		startReceipt()
	}
	waitForPostgreSQLLockWaiters(t, admin, blockerPID, 2)
	if err := blocker.Commit(); err != nil {
		t.Fatalf("release Session blocker: %v", err)
	}
	var receiptOutcome, terminationOutcome outcome
	for range 2 {
		value := <-results
		if value.kind == "receipt" {
			receiptOutcome = value
		} else {
			terminationOutcome = value
		}
	}
	if terminationOutcome.err != nil {
		t.Fatalf("final exhaustion outcome: %v", terminationOutcome.err)
	}
	var inboxStatus, sessionStatus string
	var bindings int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$2),
		(SELECT status FROM sessions WHERE workspace_id='default' AND id=$1),
		(SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1)`,
		sessionID, inputID,
	).Scan(&inboxStatus, &sessionStatus, &bindings); err != nil {
		t.Fatalf("read race winner: %v", err)
	}
	if receiptFirst {
		if receiptOutcome.err != nil || receiptOutcome.commit.GetCommitted() == nil ||
			terminationOutcome.finalized.Status != RuntimeDeliveryDuplicate || inboxStatus != "committed" || sessionStatus != "idle" || bindings != 1 {
			t.Fatalf("receipt-first winner = commit %#v/%v final %+v Inbox %s Session %s bindings %d",
				receiptOutcome.commit, receiptOutcome.err, terminationOutcome.finalized, inboxStatus, sessionStatus, bindings)
		}
		return
	}
	if receiptOutcome.err != nil || receiptOutcome.commit.GetStale() == nil || !terminationOutcome.finalized.QueueLeaseSettled || inboxStatus != "cancelled" || sessionStatus != "terminated" || bindings != 0 {
		t.Fatalf("termination-first winner = commit %#v/%v final %+v Inbox %s Session %s bindings %d",
			receiptOutcome.commit, receiptOutcome.err, terminationOutcome.finalized, inboxStatus, sessionStatus, bindings)
	}
}

func TestPostgreSQLJobRunnerFinalInterruptExhaustionTerminatesSessionAndFollowers(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_interrupt_final_exhaustion"
		threadID       = "thr_interrupt_final_exhaustion"
		bindingID      = "bind_interrupt_final_exhaustion"
		podUID         = "pod_interrupt_final_exhaustion"
		turnID         = "evt_interrupt_final_exhaustion_running"
		requestID      = "mreq_interrupt_final_exhaustion"
		toolUseEventID = "evt_interrupt_final_exhaustion_tool"
		interruptID    = "rin_interrupt_final_exhaustion"
		interruptEvent = "evt_interrupt_final_exhaustion"
		followerID     = "rin_interrupt_final_exhaustion_follower"
		followerEvent  = "evt_interrupt_final_exhaustion_follower"
		secondID       = "rin_interrupt_final_exhaustion_second"
		secondEvent    = "evt_interrupt_final_exhaustion_second"
		reviewerID     = "thr_interrupt_final_exhaustion_reviewer"
		reviewID       = "arvw_interrupt_final_exhaustion"
	)
	now := time.Now().UTC().Add(-time.Minute)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIInternalReviewerThread(t, admin, "default", sessionID, threadID, reviewerID)
	reviewerAdmission, err := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime)).AdmitApprovalReviewInput(
		context.Background(),
		&bridgev1.AdmitApprovalReviewInputRequest{Scope: scope, ReviewerThreadId: reviewerID, ReviewId: reviewID},
	)
	if err != nil || reviewerAdmission.GetCommitted() == nil {
		t.Fatalf("seed final-exhaustion Reviewer custody = %#v/%v", reviewerAdmission, err)
	}
	seedBridgeAPIOpenDurableTurn(t, admin, scope, turnID)
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_status (
		workspace_id,session_id,status,running_since,active_seconds_total,binding_id,binding_generation,created_at,updated_at
	) VALUES ('default',$1,'running',$3,0,$2,1,$3,$3)`, sessionID, bindingID, now); err != nil {
		t.Fatalf("seed running Runtime status: %v", err)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "evt_interrupt_final_exhaustion_request", 2,
		"span.model_request_start", `{"type":"span.model_request_start","model_request_id":"`+requestID+`"}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events
		SET model_request_id=$3, projection_json='{"request_kind":"agent_provider_request","context_through_message_sequence":0}'
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, "evt_interrupt_final_exhaustion_request", requestID); err != nil {
		t.Fatalf("seed open Request projection: %v", err)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, toolUseEventID, 3, "agent.tool_use",
		`{"type":"agent.tool_use","name":"exec_command","input":{},"evaluated_permission":"allow"}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events
		SET visibility='public', session_visible=true, model_request_id=$3
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, toolUseEventID, requestID); err != nil {
		t.Fatalf("seed unresolved Tool visibility: %v", err)
	}
	seedBridgeAPIDurableToolMessage(t, admin, "default", sessionID, threadID, requestID, toolUseEventID, "call_interrupt_final_exhaustion", "exec_command")

	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, interruptEvent, 4, "user.interrupt", `{"type":"user.interrupt"}`)
	seedRuntimeInboxBirthForJob(t, admin, RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID, RuntimeInputID: interruptID,
		InputKind: "interrupt_control", EventIDs: []string{interruptEvent}, SequenceFrom: 4, SequenceTo: 4,
	})
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, followerEvent, 5, "user.message", `{"content":[{"type":"text","text":"after interrupt"}]}`)
	seedRuntimeInboxBirthForJob(t, admin, RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID, RuntimeInputID: followerID,
		InputKind: "messages", EventIDs: []string{followerEvent}, SequenceFrom: 5, SequenceTo: 5,
	})
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, secondEvent, 6, "user.interrupt", `{"type":"user.interrupt"}`)
	seedRuntimeInboxBirthForJob(t, admin, RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID, RuntimeInputID: secondID,
		InputKind: "interrupt_control", EventIDs: []string{secondEvent}, SequenceFrom: 6, SequenceTo: 6,
	})

	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	enqueueInterruptExhaustionJob(t, queueStore, sessionID, threadID, interruptID, "interrupt_control", interruptEvent, 4, 1, now)
	enqueueInterruptExhaustionJob(t, queueStore, sessionID, threadID, followerID, "messages", followerEvent, 5, queue.DefaultMaxAttempts, now.Add(time.Second))
	enqueueInterruptExhaustionJob(t, queueStore, sessionID, threadID, secondID, "interrupt_control", secondEvent, 6, queue.DefaultMaxAttempts, now.Add(2*time.Second))

	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	deliverer := &postgresFinalizingDeliverer{store: deliveryStore}
	runner := &JobRunner{
		Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID}, Deliverer: deliverer,
		Config: JobRunnerConfig{LeaseOwner: "interrupt-final-exhaustion", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("run final interrupt owner: %v", err)
	}
	if deliverer.deliveries != 0 {
		t.Fatalf("final interrupt Runtime deliveries = %d; want zero after receipt miss", deliverer.deliveries)
	}

	var sessionStatus, threadStatus, reviewerInboxStatus string
	var bindingRows, requestEnds, toolResults, terminalErrors, terminalStatuses, messages int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM sessions WHERE workspace_id='default' AND id=$1),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND id=$2),
		(SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_end' AND model_request_id=$3),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.tool_result' AND payload_json::jsonb->>'tool_use_event_id'=$4),
			(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.error'),
			(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.status_terminated'),
			(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$1 AND source_event_id IN ($5,$6,$7)),
			(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$8)`,
		sessionID, threadID, requestID, toolUseEventID, interruptEvent, followerEvent, secondEvent,
		reviewerAdmission.GetCommitted().GetRuntimeInputId(),
	).Scan(&sessionStatus, &threadStatus, &bindingRows, &requestEnds, &toolResults, &terminalErrors, &terminalStatuses, &messages, &reviewerInboxStatus); err != nil {
		t.Fatalf("read final interrupt terminal facts: %v", err)
	}
	if sessionStatus != "terminated" || threadStatus != "failed" || reviewerInboxStatus != "cancelled" || bindingRows != 0 || requestEnds != 1 || toolResults != 1 || terminalErrors != 1 || terminalStatuses != 1 || messages != 0 {
		t.Fatalf("terminal facts session/thread/reviewer/bindings/requestEnds/toolResults/errors/statuses/messages = %s/%s/%s/%d/%d/%d/%d/%d/%d",
			sessionStatus, threadStatus, reviewerInboxStatus, bindingRows, requestEnds, toolResults, terminalErrors, terminalStatuses, messages)
	}
	rows, err := admin.QueryContext(context.Background(), `SELECT inbox.runtime_input_id, inbox.status, job.status, event.processed_at IS NOT NULL
		FROM session_runtime_inbox inbox
		JOIN queue_jobs job ON job.workspace_id=inbox.workspace_id AND job.dedupe_key='runtime_input:' || inbox.workspace_id || ':' || inbox.session_id || ':' || inbox.runtime_input_id
		JOIN session_events event ON event.workspace_id=inbox.workspace_id AND event.event_id=(inbox.event_ids_json::jsonb->>0)
		WHERE inbox.workspace_id='default' AND inbox.session_id=$1 AND inbox.runtime_input_id IN ($2,$3,$4)
		ORDER BY inbox.runtime_input_id`, sessionID, interruptID, followerID, secondID)
	if err != nil {
		t.Fatalf("read terminal input custody: %v", err)
	}
	defer func() { _ = rows.Close() }()
	count := 0
	for rows.Next() {
		var inputID, inboxStatus, queueStatus string
		var processed bool
		if err := rows.Scan(&inputID, &inboxStatus, &queueStatus, &processed); err != nil {
			t.Fatalf("scan terminal input custody: %v", err)
		}
		if inboxStatus != "cancelled" || queueStatus != queue.StatusCancelled || processed {
			t.Fatalf("input %s custody = Inbox %s Queue %s processed %t; want cancelled/cancelled/unprocessed", inputID, inboxStatus, queueStatus, processed)
		}
		count++
	}
	if err := rows.Err(); err != nil || count != 3 {
		t.Fatalf("terminal input rows = %d/%v; want 3", count, err)
	}
	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || active || deliverer.deliveries != 0 {
		t.Fatalf("post-terminal runner = active %t deliveries %d err %v; want inert", active, deliverer.deliveries, err)
	}
}

func TestPostgreSQLJobRunnerFinalChildInterruptExhaustionPreservesSessionAndSiblings(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID       = "sesn_child_interrupt_final_exhaustion"
		mainThreadID    = "thr_child_interrupt_final_main"
		childThreadID   = "thr_child_interrupt_final_target"
		siblingThreadID = "thr_child_interrupt_final_sibling"
		bindingID       = "bind_child_interrupt_final_exhaustion"
		podUID          = "pod_child_interrupt_final_exhaustion"
		turnID          = "evt_child_interrupt_final_running"
		interruptID     = "rin_child_interrupt_final_exhaustion"
		interruptEvent  = "evt_child_interrupt_final_exhaustion"
	)
	now := time.Now().UTC().Add(-time.Minute)
	seedBridgeAPISession(t, admin, "default", sessionID, mainThreadID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainThreadID, childThreadID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainThreadID, siblingThreadID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, childThreadID, "evt_child_interrupt_final_created", 1,
		"session.thread_created", `{"type":"session.thread_created","parent_thread_id":"`+mainThreadID+`","source_tool_use_event_id":"evt_child_interrupt_final_spawn"}`)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	scope := bridgeAPIScope(sessionID, childThreadID, bindingID, 1, podUID)
	seedBridgeAPIOpenDurableTurn(t, admin, scope, turnID)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_threads
		SET status='running' WHERE workspace_id='default' AND session_id=$1 AND id IN ($2,$3)`,
		sessionID, mainThreadID, siblingThreadID); err != nil {
		t.Fatalf("seed active parent and sibling: %v", err)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, childThreadID, interruptEvent, 3, "user.interrupt", `{"type":"user.interrupt"}`)
	seedRuntimeInboxBirthForJob(t, admin, RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: childThreadID,
		RuntimeInputID: interruptID, InputKind: "interrupt_control", EventIDs: []string{interruptEvent}, SequenceFrom: 3, SequenceTo: 3,
	})

	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	enqueueInterruptExhaustionJob(t, queueStore, sessionID, childThreadID, interruptID, "interrupt_control", interruptEvent, 3, 1, now)
	deliverer := &postgresFinalizingDeliverer{store: NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)}
	runner := &JobRunner{
		Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID}, Deliverer: deliverer,
		Config: JobRunnerConfig{LeaseOwner: "child-interrupt-final-exhaustion", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("run final child interrupt owner: %v", err)
	}

	var sessionStatus, mainStatus, childStatus, siblingStatus, inboxStatus, queueStatus string
	var bindings, childTerminalEvents, sessionTerminalEvents, completionMails int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM sessions WHERE workspace_id='default' AND id=$1),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND id=$2),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND id=$3),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND id=$4),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$5),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$6),
		(SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$3 AND type='session.thread_status_terminated'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.status_terminated'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$3 AND type='agent.thread_message_sent')`,
		sessionID, mainThreadID, childThreadID, siblingThreadID, interruptID,
		queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, interruptID),
	).Scan(&sessionStatus, &mainStatus, &childStatus, &siblingStatus, &inboxStatus, &queueStatus,
		&bindings, &childTerminalEvents, &sessionTerminalEvents, &completionMails); err != nil {
		t.Fatalf("read final child interrupt facts: %v", err)
	}
	if sessionStatus != "running" || mainStatus != "running" || childStatus != "failed" || siblingStatus != "running" ||
		inboxStatus != "cancelled" || queueStatus != queue.StatusCancelled || bindings != 1 || childTerminalEvents != 1 ||
		sessionTerminalEvents != 0 || completionMails != 1 || deliverer.deliveries != 0 {
		t.Fatalf("child final exhaustion = Session %s, Threads %s/%s/%s, Inbox/Queue %s/%s, bindings/child terminal/session terminal/mail/runtime %d/%d/%d/%d/%d",
			sessionStatus, mainStatus, childStatus, siblingStatus, inboxStatus, queueStatus,
			bindings, childTerminalEvents, sessionTerminalEvents, completionMails, deliverer.deliveries)
	}
}

func TestPostgreSQLJobRunnerMalformedInterruptAtomicallyTerminatesDurableCustody(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_malformed_interrupt_terminal"
		threadID  = "thr_malformed_interrupt_terminal"
		bindingID = "bind_malformed_interrupt_terminal"
		podUID    = "pod_malformed_interrupt_terminal"
		inputID   = "rin_malformed_interrupt_terminal"
		eventID   = "evt_malformed_interrupt_terminal"
	)
	now := time.Now().UTC().Add(-time.Minute)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 1, "user.interrupt", `{"type":"user.interrupt"}`)
	seedRuntimeInboxBirthForJob(t, admin, RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: inputID, InputKind: "interrupt_control", EventIDs: []string{eventID}, SequenceFrom: 1, SequenceTo: 1,
	})
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	enqueueInterruptExhaustionJob(t, queueStore, sessionID, threadID, inputID, "interrupt_control", eventID, 1, 2, now)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_inbox
		SET event_ids_json='not-json', sequence_from=0, sequence_to=0
		WHERE workspace_id='default' AND runtime_input_id=$1`, inputID); err != nil {
		t.Fatalf("malform canonical interrupt metadata: %v", err)
	}

	deliverer := &postgresFinalizingDeliverer{store: NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)}
	runner := &JobRunner{
		Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID}, Deliverer: deliverer,
		Config: JobRunnerConfig{LeaseOwner: "malformed-interrupt-terminal", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("run first malformed interrupt attempt: %v", err)
	}
	var firstSessionStatus, firstInboxStatus, firstQueueStatus string
	var firstAttempts int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM sessions WHERE workspace_id='default' AND id=$1),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$3),
		(SELECT attempt_count FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$3)`,
		sessionID, inputID, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, inputID),
	).Scan(&firstSessionStatus, &firstInboxStatus, &firstQueueStatus, &firstAttempts); err != nil {
		t.Fatalf("read first malformed interrupt attempt: %v", err)
	}
	if firstSessionStatus != "idle" || firstInboxStatus != "queued" || firstQueueStatus != queue.StatusPending || firstAttempts != 1 {
		t.Fatalf("first malformed interrupt attempt = Session %s Inbox %s Queue %s attempts %d; want idle/queued/pending/1",
			firstSessionStatus, firstInboxStatus, firstQueueStatus, firstAttempts)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE queue_jobs SET available_at=clock_timestamp()-interval '1 second'
		WHERE workspace_id='default' AND dedupe_key=$1`, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, inputID)); err != nil {
		t.Fatalf("make final malformed interrupt attempt available: %v", err)
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("run final malformed interrupt attempt: %v", err)
	}

	var sessionStatus, threadStatus, inboxStatus, queueStatus string
	var bindings, terminalErrors, terminalStatuses, messages, attempts int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM sessions WHERE workspace_id='default' AND id=$1),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND id=$2),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$3),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$4),
		(SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1),
			(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.error'),
			(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.status_terminated'),
			(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$1),
			(SELECT attempt_count FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$4)`,
		sessionID, threadID, inputID, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, inputID),
	).Scan(&sessionStatus, &threadStatus, &inboxStatus, &queueStatus, &bindings, &terminalErrors, &terminalStatuses, &messages, &attempts); err != nil {
		t.Fatalf("read malformed interrupt atomic closeout: %v", err)
	}
	if sessionStatus != "terminated" || threadStatus != "failed" || inboxStatus != "cancelled" || queueStatus != queue.StatusCancelled ||
		bindings != 0 || terminalErrors != 1 || terminalStatuses != 1 || messages != 0 || attempts != 2 || deliverer.deliveries != 0 {
		t.Fatalf("malformed interrupt closeout = Session %s Thread %s Inbox %s Queue %s bindings/errors/statuses/messages/attempts/runtime %d/%d/%d/%d/%d/%d",
			sessionStatus, threadStatus, inboxStatus, queueStatus, bindings, terminalErrors, terminalStatuses, messages, attempts, deliverer.deliveries)
	}
}

func TestPostgreSQLJobRunnerFinalInterruptFenceRejectsPostPlanLeaseTakeover(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_interrupt_post_plan_takeover"
		threadID  = "thr_interrupt_post_plan_takeover"
		bindingID = "bind_interrupt_post_plan_takeover"
		podUID    = "pod_interrupt_post_plan_takeover"
		inputID   = "rin_interrupt_post_plan_takeover"
		eventID   = "evt_interrupt_post_plan_takeover"
	)
	now := time.Now().UTC().Add(-time.Minute)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIOpenDurableTurn(t, admin, bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID), "evt_interrupt_post_plan_turn")
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 2, "user.interrupt", `{}`)
	seedRuntimeInboxBirthForJob(t, admin, RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: inputID, InputKind: "interrupt_control", EventIDs: []string{eventID}, SequenceFrom: 2, SequenceTo: 2,
	})
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	enqueueInterruptExhaustionJob(t, queueStore, sessionID, threadID, inputID, "interrupt_control", eventID, 2, 3, now)

	baseStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	pausingStore := &postPlanInterruptFenceStore{
		PostgreSQLRuntimeDeliveryStore: baseStore,
		planned:                        make(chan struct{}), resume: make(chan struct{}),
	}
	sender := &recordingRuntimeCommandSender{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}
	runner := &JobRunner{
		Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer: postPlanInterruptDeliverer{direct: RuntimePodDirectDeliverer{Store: pausingStore, Sender: sender}},
		Config:    JobRunnerConfig{LeaseOwner: "post-plan-old", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	runResult := make(chan error, 1)
	go func() { runResult <- runner.RunOnce(context.Background()) }()
	select {
	case <-pausingStore.planned:
	case <-time.After(5 * time.Second):
		t.Fatal("old JobRunner did not reach the final pre-send fence")
	}

	var oldJobID, oldLeaseToken string
	if err := admin.QueryRowContext(context.Background(), `SELECT id, lease_token FROM queue_jobs
		WHERE workspace_id='default' AND dedupe_key=$1`, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, inputID)).
		Scan(&oldJobID, &oldLeaseToken); err != nil {
		t.Fatalf("read old planned interrupt lease: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE queue_jobs SET leased_until=clock_timestamp()-interval '1 second'
		WHERE workspace_id='default' AND id=$1 AND lease_token=$2`, oldJobID, oldLeaseToken); err != nil {
		t.Fatalf("expire post-plan interrupt lease: %v", err)
	}
	if reclaimed, err := queueStore.ReclaimExpiredLeases(context.Background(), queue.ReclaimExpiredLeasesRequest{
		WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput, Limit: 1,
	}); err != nil || reclaimed != 1 {
		t.Fatalf("reclaim post-plan interrupt lease = %d/%v; want 1/nil", reclaimed, err)
	}
	current := mustLeaseBridgeQueueJob(t, queueStore, queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "post-plan-current",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: time.Now().UTC(),
	})
	if current.ID != oldJobID || current.LeaseToken == oldLeaseToken {
		t.Fatalf("post-plan takeover identity = %s/%s; want same job and a new token", current.ID, current.LeaseToken)
	}

	type durableSnapshot struct {
		sessionStatus, inboxStatus, queueStatus, leaseToken, leasedBy string
		bindings, events, messages, operations                        int
		queueUpdatedAt                                                time.Time
	}
	readSnapshot := func() durableSnapshot {
		var snapshot durableSnapshot
		if err := admin.QueryRowContext(context.Background(), `SELECT
			(SELECT status FROM sessions WHERE workspace_id='default' AND id=$1),
			(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2),
			(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$3),
			(SELECT lease_token FROM queue_jobs WHERE workspace_id='default' AND id=$3),
			(SELECT leased_by FROM queue_jobs WHERE workspace_id='default' AND id=$3),
			(SELECT updated_at FROM queue_jobs WHERE workspace_id='default' AND id=$3),
			(SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1),
			(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1),
			(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$1),
			(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1)`,
			sessionID, inputID, oldJobID,
		).Scan(&snapshot.sessionStatus, &snapshot.inboxStatus, &snapshot.queueStatus, &snapshot.leaseToken,
			&snapshot.leasedBy, &snapshot.queueUpdatedAt, &snapshot.bindings, &snapshot.events, &snapshot.messages, &snapshot.operations); err != nil {
			t.Fatalf("read post-plan takeover snapshot: %v", err)
		}
		return snapshot
	}
	beforeResume := readSnapshot()
	close(pausingStore.resume)
	select {
	case err := <-runResult:
		if err != nil {
			t.Fatalf("old JobRunner after post-plan takeover: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("old JobRunner did not leave the final lease fence")
	}
	afterResume := readSnapshot()
	if beforeResume != afterResume || afterResume.queueStatus != queue.StatusLeased || afterResume.leaseToken != current.LeaseToken ||
		afterResume.leasedBy != "post-plan-current" || len(sender.requests) != 0 {
		t.Fatalf("old post-plan worker mutated durable facts: before=%+v after=%+v Runtime calls=%d", beforeResume, afterResume, len(sender.requests))
	}
}

type postPlanInterruptFenceStore struct {
	*PostgreSQLRuntimeDeliveryStore
	planned chan struct{}
	resume  chan struct{}
}

type postPlanInterruptDeliverer struct {
	direct RuntimePodDirectDeliverer
}

type invalidFinalizationInterruptDeliverer struct {
	direct RuntimePodDirectDeliverer
}

func (d invalidFinalizationInterruptDeliverer) DeliverRuntimeJob(ctx context.Context, job RuntimeJob) (RuntimeDeliveryResult, error) {
	return d.direct.DeliverRuntimeJob(ctx, job)
}

func (d invalidFinalizationInterruptDeliverer) FinalizeRuntimeDelivery(ctx context.Context, job RuntimeJob, result RuntimeDeliveryResult) (RuntimeDeliveryResult, error) {
	job.SessionThreadID = ""
	return d.direct.FinalizeRuntimeDelivery(ctx, job, result)
}

func (d invalidFinalizationInterruptDeliverer) ReplayRuntimeDeliveryFinalization(ctx context.Context, job RuntimeJob) (RuntimeDeliveryResult, bool, error) {
	return d.direct.ReplayRuntimeDeliveryFinalization(ctx, job)
}

func (d invalidFinalizationInterruptDeliverer) ReplaceMalformedRuntimeInputCustody(ctx context.Context, job RuntimeJob) (queue.ReplaceMalformedRuntimeInputCustodyResult, error) {
	return d.direct.ReplaceMalformedRuntimeInputCustody(ctx, job)
}

func (d invalidFinalizationInterruptDeliverer) FinalizeMalformedRuntimeInputCustody(ctx context.Context, lease MalformedRuntimeInputLease) (MalformedRuntimeInputCustodyResult, error) {
	return d.direct.FinalizeMalformedRuntimeInputCustody(ctx, lease)
}

func TestJobRunnerFinalAttemptInvalidInterruptUsesExactTerminalOwner(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID   = "sesn_invalid_final_interrupt"
		threadID    = "thr_invalid_final_interrupt"
		bindingID   = "bind_invalid_final_interrupt"
		podUID      = "pod_invalid_final_interrupt"
		inputID     = "rin_invalid_final_interrupt"
		eventID     = "evt_invalid_final_interrupt"
		followerID  = "rin_invalid_final_interrupt_follower"
		followerEvt = "evt_invalid_final_interrupt_follower"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 1, "user.interrupt", `{}`)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, followerEvt, 2, "user.message", `{"content":[{"type":"text","text":"wait behind interrupt"}]}`)
	seedRuntimeInboxBirthForJob(t, admin, RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: inputID, InputKind: "interrupt_control", EventIDs: []string{eventID}, SequenceFrom: 1, SequenceTo: 1,
	})
	seedRuntimeInboxBirthForJob(t, admin, RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: followerID, InputKind: "messages", EventIDs: []string{followerEvt}, SequenceFrom: 2, SequenceTo: 2,
	})
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	now := time.Now().UTC().Add(-time.Minute)
	enqueueInterruptExhaustionJob(t, queueStore, sessionID, threadID, inputID, "interrupt_control", eventID, 1, 1, now)
	enqueueInterruptExhaustionJob(t, queueStore, sessionID, threadID, followerID, "messages", followerEvt, 2, queue.DefaultMaxAttempts, now.Add(time.Microsecond))
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	deliverer := invalidFinalizationInterruptDeliverer{direct: RuntimePodDirectDeliverer{Store: deliveryStore}}
	runner := &JobRunner{
		Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID}, Deliverer: deliverer,
		Config: JobRunnerConfig{LeaseOwner: "invalid-final-interrupt-owner", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("final-attempt invalid interrupt: %v", err)
	}
	var sessionStatus, interruptInbox, interruptQueue, followerInbox, followerQueue string
	var attempts, lineage, events, messages int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM sessions WHERE workspace_id='default' AND id=$1),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$2),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$3 ORDER BY created_at LIMIT 1),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$4),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$5 ORDER BY created_at LIMIT 1),
		(SELECT attempt_count FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$3 ORDER BY created_at LIMIT 1),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$3),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND event_id IN ($6,$7)),
		(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$1)`,
		sessionID, inputID, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, inputID), followerID,
		queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, followerID), eventID, followerEvt,
	).Scan(&sessionStatus, &interruptInbox, &interruptQueue, &followerInbox, &followerQueue, &attempts, &lineage, &events, &messages); err != nil {
		t.Fatalf("read final-attempt invalid interrupt disposition: %v", err)
	}
	if sessionStatus != "terminated" || interruptInbox != "cancelled" || interruptQueue != queue.StatusCancelled ||
		followerInbox != "cancelled" || followerQueue != queue.StatusCancelled || attempts != 1 || lineage != 1 || events != 2 || messages != 0 {
		t.Fatalf("invalid final interrupt = Session %s interrupt %s/%s follower %s/%s attempts %d lineage %d events %d messages %d",
			sessionStatus, interruptInbox, interruptQueue, followerInbox, followerQueue, attempts, lineage, events, messages)
	}
}

func (d postPlanInterruptDeliverer) DeliverRuntimeJob(ctx context.Context, job RuntimeJob) (RuntimeDeliveryResult, error) {
	return d.direct.DeliverRuntimeJob(ctx, job)
}

func (d postPlanInterruptDeliverer) FinalizeRuntimeDelivery(ctx context.Context, job RuntimeJob, result RuntimeDeliveryResult) (RuntimeDeliveryResult, error) {
	return d.direct.FinalizeRuntimeDelivery(ctx, job, result)
}

func (d postPlanInterruptDeliverer) ReplayRuntimeDeliveryFinalization(ctx context.Context, job RuntimeJob) (RuntimeDeliveryResult, bool, error) {
	return d.direct.ReplayRuntimeDeliveryFinalization(ctx, job)
}

func (d postPlanInterruptDeliverer) ReplaceMalformedRuntimeInputCustody(ctx context.Context, job RuntimeJob) (queue.ReplaceMalformedRuntimeInputCustodyResult, error) {
	return d.direct.ReplaceMalformedRuntimeInputCustody(ctx, job)
}

func (d postPlanInterruptDeliverer) FinalizeMalformedRuntimeInputCustody(ctx context.Context, lease MalformedRuntimeInputLease) (MalformedRuntimeInputCustodyResult, error) {
	return d.direct.FinalizeMalformedRuntimeInputCustody(ctx, lease)
}

func (s *postPlanInterruptFenceStore) InterruptDeliveryAuthority(ctx context.Context, job RuntimeJob) (RuntimeInterruptDeliveryAuthority, error) {
	close(s.planned)
	select {
	case <-s.resume:
		return s.PostgreSQLRuntimeDeliveryStore.InterruptDeliveryAuthority(ctx, job)
	case <-ctx.Done():
		return RuntimeInterruptDeliveryAuthority{}, ctx.Err()
	}
}

func TestPostgreSQLMalformedInterruptFinalizationRollbackPreservesExactLease(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_malformed_interrupt_rollback"
		threadID  = "thr_malformed_interrupt_rollback"
		bindingID = "bind_malformed_interrupt_rollback"
		podUID    = "pod_malformed_interrupt_rollback"
		inputID   = "rin_malformed_interrupt_rollback"
		eventID   = "evt_malformed_interrupt_rollback"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 1, "user.interrupt", `{}`)
	seedRuntimeInboxBirthForJob(t, admin, RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: inputID, InputKind: "interrupt_control", EventIDs: []string{eventID}, SequenceFrom: 1, SequenceTo: 1,
	})
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	enqueueInterruptExhaustionJob(t, queueStore, sessionID, threadID, inputID, "interrupt_control", eventID, 1, 1, time.Now().UTC().Add(-time.Minute))
	leased := mustLeaseBridgeQueueJob(t, queueStore, queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "malformed-interrupt-rollback",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: time.Now().UTC(),
	})
	if _, err := admin.ExecContext(context.Background(), `CREATE FUNCTION fail_malformed_interrupt_terminal_event() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN IF NEW.type = 'session.error' THEN RAISE EXCEPTION 'synthetic malformed interrupt rollback'; END IF; RETURN NEW; END $$;
		CREATE TRIGGER fail_malformed_interrupt_terminal_event BEFORE INSERT ON session_events
		FOR EACH ROW EXECUTE FUNCTION fail_malformed_interrupt_terminal_event()`); err != nil {
		t.Fatalf("install malformed interrupt rollback trigger: %v", err)
	}
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	if _, err := store.FinalizeMalformedRuntimeInputCustody(context.Background(), malformedRuntimeInputLease(queueJobProto(leased))); err == nil {
		t.Fatal("malformed interrupt finalization survived injected transaction failure")
	}
	var sessionStatus, inboxStatus, queueStatus, leaseToken string
	var bindings, terminalEvents int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM sessions WHERE workspace_id='default' AND id=$1),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$3),
		(SELECT lease_token FROM queue_jobs WHERE workspace_id='default' AND id=$3),
		(SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type IN ('session.error','session.status_terminated'))`,
		sessionID, inputID, leased.ID,
	).Scan(&sessionStatus, &inboxStatus, &queueStatus, &leaseToken, &bindings, &terminalEvents); err != nil {
		t.Fatalf("read rolled-back malformed interrupt authority: %v", err)
	}
	if sessionStatus != "idle" || inboxStatus != "queued" || queueStatus != queue.StatusLeased || leaseToken != leased.LeaseToken || bindings != 1 || terminalEvents != 0 {
		t.Fatalf("rolled-back malformed interrupt = Session %s Inbox %s Queue %s/%s bindings %d terminal %d",
			sessionStatus, inboxStatus, queueStatus, leaseToken, bindings, terminalEvents)
	}
}

func TestPostgreSQLMalformedInterruptResponseLossReplaysTerminalResult(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_malformed_interrupt_replay"
		threadID  = "thr_malformed_interrupt_replay"
		bindingID = "bind_malformed_interrupt_replay"
		podUID    = "pod_malformed_interrupt_replay"
		inputID   = "rin_malformed_interrupt_replay"
		eventID   = "evt_malformed_interrupt_replay"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 1, "user.interrupt", `{}`)
	seedRuntimeInboxBirthForJob(t, admin, RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: inputID, InputKind: "interrupt_control", EventIDs: []string{eventID}, SequenceFrom: 1, SequenceTo: 1,
	})
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	enqueueInterruptExhaustionJob(t, queueStore, sessionID, threadID, inputID, "interrupt_control", eventID, 1, 1, time.Now().UTC().Add(-time.Minute))
	leased := mustLeaseBridgeQueueJob(t, queueStore, queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "malformed-interrupt-replay",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: time.Now().UTC(),
	})
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	lease := malformedRuntimeInputLease(queueJobProto(leased))
	first, err := store.FinalizeMalformedRuntimeInputCustody(context.Background(), lease)
	if err != nil || !first.Handled || !first.InterruptTerminalized || !first.QueueLeaseSettled {
		t.Fatalf("first malformed interrupt terminal result = %+v/%v", first, err)
	}
	replay, err := store.FinalizeMalformedRuntimeInputCustody(context.Background(), lease)
	if err != nil || !replay.Handled || !replay.QueueLeaseSettled || replay.InterruptTerminalized {
		t.Fatalf("replayed lost malformed interrupt response = %+v/%v; want terminal durable no-op", replay, err)
	}
	var errorsCount, statusesCount int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.error'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.status_terminated')`,
		sessionID,
	).Scan(&errorsCount, &statusesCount); err != nil {
		t.Fatalf("read malformed interrupt replay events: %v", err)
	}
	if errorsCount != 1 || statusesCount != 1 {
		t.Fatalf("malformed interrupt replay events = errors %d statuses %d; want exactly 1/1", errorsCount, statusesCount)
	}
}

func enqueueInterruptExhaustionJob(
	t *testing.T,
	store *queue.PostgreSQLQueueStore,
	sessionID string,
	threadID string,
	inputID string,
	inputKind string,
	eventID string,
	sequence int64,
	maxAttempts int,
	now time.Time,
) {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"workspace_id": "default", "session_id": sessionID, "session_thread_id": threadID,
		"runtime_input_id": inputID, "event_ids": []string{eventID},
		"sequence_from": sequence, "sequence_to": sequence, "input_kind": inputKind,
	})
	if err != nil {
		t.Fatalf("marshal runtime input %s: %v", inputID, err)
	}
	if _, err := store.Enqueue(context.Background(), queue.EnqueueRequest{
		WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput,
		PartitionKey:   queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID),
		DedupeKey:      queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, inputID),
		PayloadVersion: 1, PayloadJSON: payload, MaxAttempts: maxAttempts, Now: now,
	}); err != nil {
		t.Fatalf("enqueue runtime input %s: %v", inputID, err)
	}
}
