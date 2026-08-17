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
	if err != nil || !result.QueueLeaseSettled || result.Status != RuntimeDeliveryDuplicate {
		t.Fatalf("stale finalizer result = %+v/%v; want successful authority-loss no-op", result, err)
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
	if receiptOutcome.err == nil || !terminationOutcome.finalized.QueueLeaseSettled || inboxStatus != "cancelled" || sessionStatus != "terminated" || bindings != 0 {
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
	)
	now := time.Now().UTC().Add(-time.Minute)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
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

	var sessionStatus, threadStatus string
	var bindingRows, requestEnds, toolResults, terminalErrors, terminalStatuses, messages int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM sessions WHERE workspace_id='default' AND id=$1),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND id=$2),
		(SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_end' AND model_request_id=$3),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.tool_result' AND payload_json::jsonb->>'tool_use_event_id'=$4),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.error'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.status_terminated'),
		(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$1 AND source_event_id IN ($5,$6,$7))`,
		sessionID, threadID, requestID, toolUseEventID, interruptEvent, followerEvent, secondEvent,
	).Scan(&sessionStatus, &threadStatus, &bindingRows, &requestEnds, &toolResults, &terminalErrors, &terminalStatuses, &messages); err != nil {
		t.Fatalf("read final interrupt terminal facts: %v", err)
	}
	if sessionStatus != "terminated" || threadStatus != "failed" || bindingRows != 0 || requestEnds != 1 || toolResults != 1 || terminalErrors != 1 || terminalStatuses != 1 || messages != 0 {
		t.Fatalf("terminal facts session/thread/bindings/requestEnds/toolResults/errors/statuses/messages = %s/%s/%d/%d/%d/%d/%d/%d",
			sessionStatus, threadStatus, bindingRows, requestEnds, toolResults, terminalErrors, terminalStatuses, messages)
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
