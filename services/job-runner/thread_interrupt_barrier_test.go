package jobrunner

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestPostgreSQLThreadInterruptBarrierDefersExactInflightCustody(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID        = "sesn_interrupt_barrier_inflight"
		threadID         = "thr_interrupt_barrier_inflight"
		inputID          = "rin_interrupt_barrier_inflight"
		eventID          = "evt_interrupt_barrier_inflight"
		interruptID      = "rin_interrupt_barrier_inflight_control"
		interruptEventID = "evt_interrupt_barrier_inflight_control"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 2, "user.message", `{}`)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, interruptEventID, 3, "user.interrupt", `{}`)
	seedRuntimeInboxBirthForJob(t, admin, RuntimeJob{WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: interruptID, InputKind: "interrupt_control", EventIDs: []string{interruptEventID}, SequenceFrom: 3, SequenceTo: 3})
	job := RuntimeJob{WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID, RuntimeInputID: inputID,
		InputKind: "messages", EventIDs: []string{eventID}, SequenceFrom: 2, SequenceTo: 2}
	seedRuntimeInboxBirthForJob(t, admin, job)
	payload, err := json.Marshal(map[string]any{
		"workspace_id": "default", "session_id": sessionID, "session_thread_id": threadID,
		"runtime_input_id": inputID, "event_ids": []string{eventID}, "sequence_from": 2, "sequence_to": 2, "input_kind": "messages",
	})
	if err != nil {
		t.Fatalf("marshal runtime input: %v", err)
	}
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	now := time.Now().UTC()
	created, err := queueStore.Enqueue(context.Background(), queue.EnqueueRequest{
		ID: queue.NewJobID(), WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput,
		PartitionKey:   queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID),
		DedupeKey:      queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, inputID),
		PayloadVersion: 1, PayloadJSON: payload, MaxAttempts: queue.DefaultMaxAttempts, Now: now,
	})
	if err != nil {
		t.Fatalf("enqueue runtime input: %v", err)
	}
	leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{WorkspaceID: workspace.DefaultID,
		Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "barrier-stale-owner", MaxJobs: 1, LeaseDuration: time.Minute, Now: now})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease runtime input = %#v/%v", leased, err)
	}
	lease := leased[0]
	job.JobID, job.LeaseToken, job.Kind, job.PartitionKey, job.DedupeKey = lease.ID, lease.LeaseToken, lease.Kind, lease.PartitionKey, lease.DedupeKey
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_inbox SET status='delivering',
		binding_id='bind_barrier_deferred',binding_generation=1,target_pod_uid='pod_barrier_deferred'
		WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2`, sessionID, inputID); err != nil {
		t.Fatalf("bind input before barrier-stale response: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_inbox SET status='committed',
		binding_id='bind_released_interrupt',binding_generation=1,target_pod_uid='pod_released_interrupt',committed_at=clock_timestamp()
		WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2`, sessionID, interruptID); err != nil {
		t.Fatalf("commit interrupt Inbox before stale response finalization: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_bridge_operations (
			workspace_id,session_id,session_thread_id,operation,source_kind,idempotency_key,
			request_hash,ack_status,result_json,declaration_digest,receipt_json,created_at,updated_at
		) VALUES ('default',$1,$3,'commit_inputs','interrupt_control',$2,'released','committed','{}','released','{}',clock_timestamp(),clock_timestamp())`,
		sessionID, interruptID, threadID); err != nil {
		t.Fatalf("release interrupt barrier before stale response finalization: %v", err)
	}
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	result, err := store.FinalizeRuntimeDelivery(context.Background(), job, RuntimeDeliveryResult{Status: RuntimeDeliveryBarrierStale})
	if err != nil || result.Status != RuntimeDeliveryBarrierStale || !result.QueueLeaseSettled {
		t.Fatalf("finalize barrier stale = %#v/%v", result, err)
	}
	var queueStatus, inboxStatus string
	var attempts, messages, processed int
	var bindingID, leaseToken sql.NullString
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$1),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$2 AND runtime_input_id=$3),
		(SELECT attempt_count FROM queue_jobs WHERE workspace_id='default' AND id=$1),
		(SELECT binding_id FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$2 AND runtime_input_id=$3),
		(SELECT lease_token FROM queue_jobs WHERE workspace_id='default' AND id=$1),
		(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$2),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$2 AND event_id=$4 AND processed_at IS NOT NULL)`,
		created.ID, sessionID, inputID, eventID).Scan(&queueStatus, &inboxStatus, &attempts, &bindingID, &leaseToken, &messages, &processed); err != nil {
		t.Fatalf("read barrier-stale custody: %v", err)
	}
	if queueStatus != queue.StatusPending || inboxStatus != "queued" || attempts != 0 || bindingID.Valid || leaseToken.Valid || messages != 0 || processed != 0 {
		t.Fatalf("barrier-stale custody = %s/%s attempts=%d binding=%v lease=%v messages=%d processed=%d",
			queueStatus, inboxStatus, attempts, bindingID, leaseToken, messages, processed)
	}
	replay, err := store.FinalizeRuntimeDelivery(context.Background(), job, RuntimeDeliveryResult{Status: RuntimeDeliveryBarrierStale})
	if err != nil || replay.Status != RuntimeDeliveryAuthorityLost || replay.QueueLeaseSettled {
		t.Fatalf("stale lease replay = %#v/%v; want authority loss without mutation", replay, err)
	}
}

func TestPostgreSQLSupersededInterruptSettlesItsExactQueueLease(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_superseded_interrupt_lease"
		threadID  = "thr_superseded_interrupt_lease"
		activeID  = "rin_superseded_interrupt_active"
		staleID   = "rin_superseded_interrupt_stale"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "evt_superseded_interrupt_active", 1, "user.interrupt", `{}`)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "evt_superseded_interrupt_stale", 2, "user.interrupt", `{}`)
	seedRuntimeInboxBirthForJob(t, admin, RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: activeID, InputKind: "interrupt_control", EventIDs: []string{"evt_superseded_interrupt_active"}, SequenceFrom: 1, SequenceTo: 1,
	})
	staleJob := RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: staleID, InputKind: "interrupt_control", EventIDs: []string{"evt_superseded_interrupt_stale"}, SequenceFrom: 2, SequenceTo: 2,
	}
	seedRuntimeInboxBirthForJob(t, admin, staleJob)
	activePayload, err := json.Marshal(map[string]any{
		"workspace_id": "default", "session_id": sessionID, "session_thread_id": threadID,
		"runtime_input_id": activeID, "event_ids": []string{"evt_superseded_interrupt_active"},
		"sequence_from": 1, "sequence_to": 1, "input_kind": "interrupt_control",
	})
	if err != nil {
		t.Fatalf("marshal active interrupt payload: %v", err)
	}
	stalePayload, err := json.Marshal(map[string]any{
		"workspace_id": "default", "session_id": sessionID, "session_thread_id": threadID,
		"runtime_input_id": staleID, "event_ids": []string{"evt_superseded_interrupt_stale"},
		"sequence_from": 2, "sequence_to": 2, "input_kind": "interrupt_control",
	})
	if err != nil {
		t.Fatalf("marshal stale interrupt payload: %v", err)
	}
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	now := time.Now().UTC()
	if _, err := queueStore.Enqueue(context.Background(), queue.EnqueueRequest{
		WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput,
		PartitionKey:   queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID),
		DedupeKey:      queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, activeID),
		PayloadVersion: 1, PayloadJSON: activePayload, Priority: 100, Now: now,
	}); err != nil {
		t.Fatalf("enqueue active interrupt: %v", err)
	}
	created, err := queueStore.Enqueue(context.Background(), queue.EnqueueRequest{
		WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput,
		PartitionKey:   queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID),
		DedupeKey:      queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, staleID),
		PayloadVersion: 1, PayloadJSON: stalePayload, Priority: 100, Now: now.Add(time.Microsecond),
	})
	if err != nil {
		t.Fatalf("enqueue stale interrupt: %v", err)
	}
	leaseToken, err := queue.NewLeaseToken()
	if err != nil {
		t.Fatalf("create superseded interrupt lease token: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE queue_jobs
		SET status='leased', leased_by='stale-interrupt-owner', lease_token=$2,
		    leased_at=clock_timestamp(), leased_until=clock_timestamp()+interval '1 minute', updated_at=clock_timestamp()
		WHERE workspace_id='default' AND id=$1`, created.ID, leaseToken); err != nil {
		t.Fatalf("install superseded interrupt lease: %v", err)
	}
	staleJob.JobID, staleJob.LeaseToken, staleJob.Kind = created.ID, leaseToken, created.Kind
	staleJob.PartitionKey, staleJob.DedupeKey = created.PartitionKey, created.DedupeKey
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	plan, err := store.PrepareRuntimeCommand(context.Background(), staleJob)
	if err != nil || !plan.StaleAccepted || !plan.QueueLeaseSettled {
		t.Fatalf("superseded interrupt plan = %#v/%v; want settled stale", plan, err)
	}
	var queueStatus, inboxStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$1),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$2)`,
		created.ID, staleID).Scan(&queueStatus, &inboxStatus); err != nil {
		t.Fatalf("read stale interrupt custody: %v", err)
	}
	if queueStatus != queue.StatusCancelled || inboxStatus != "cancelled" {
		t.Fatalf("superseded interrupt custody = %s/%s; want cancelled/cancelled", queueStatus, inboxStatus)
	}
}

func TestPostgreSQLThreadInterruptBarrierIgnoresLockedTerminalHistory(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_interrupt_barrier_bounded_history"
		threadID  = "thr_interrupt_barrier_bounded_history"
		activeID  = "rin_interrupt_barrier_bounded_history_active"
		activeEvt = "evt_interrupt_barrier_bounded_history_active"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, activeEvt, 1, "user.interrupt", `{}`)
	seedRuntimeInboxBirthForJob(t, admin, RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: activeID, InputKind: "interrupt_control", EventIDs: []string{activeEvt}, SequenceFrom: 1, SequenceTo: 1,
	})
	seedActiveInterruptQueueCustody(t, runtime, sessionID, threadID, activeID, activeEvt, 1)
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_inbox (
		workspace_id, session_id, session_thread_id, runtime_input_id, input_kind, event_ids_json,
		sequence_from, sequence_to, status, created_at, updated_at
	) SELECT 'default', $1, $2, 'rin_terminal_' || value, 'interrupt_control', '[]',
		value + 10, value + 10, 'cancelled', clock_timestamp(), clock_timestamp()
		FROM generate_series(1, 1000) AS value`, sessionID, threadID); err != nil {
		t.Fatalf("seed terminal interrupt history: %v", err)
	}
	terminalLock, err := admin.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin terminal history lock: %v", err)
	}
	t.Cleanup(func() { _ = terminalLock.Rollback() })
	if _, err := terminalLock.ExecContext(context.Background(), `SELECT runtime_input_id
		FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id='rin_terminal_1' FOR UPDATE`); err != nil {
		t.Fatalf("lock terminal history row: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client := dbconnect.NewClientForTesting(runtime)
	var active bool
	if err := client.WithWorkspaceTx(ctx, "default", "bridge.interrupt_barrier_bounded_history", func(tx *dbconnect.Tx) error {
		var barrier runtimecontrol.ThreadInterruptBarrier
		var lookupErr error
		barrier, active, lookupErr = runtimecontrol.ActiveInterruptBarrierTx(ctx, tx, "default", sessionID, threadID)
		if lookupErr == nil && (!active || barrier.RuntimeInputID != activeID) {
			return errors.New("active interrupt barrier was not selected")
		}
		return lookupErr
	}); err != nil {
		t.Fatalf("active barrier waited on terminal history: %v", err)
	}
}

func seedActiveInterruptQueueCustody(
	t *testing.T,
	db *sql.DB,
	sessionID string,
	threadID string,
	runtimeInputID string,
	eventID string,
	sequence int64,
) {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"workspace_id": "default", "session_id": sessionID, "session_thread_id": threadID,
		"runtime_input_id": runtimeInputID, "event_ids": []string{eventID},
		"sequence_from": sequence, "sequence_to": sequence, "input_kind": "interrupt_control",
	})
	if err != nil {
		t.Fatalf("marshal interrupt Queue custody: %v", err)
	}
	store := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(db))
	if _, err := store.Enqueue(context.Background(), queue.EnqueueRequest{
		ID: queue.NewJobID(), WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput,
		PartitionKey:   queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID),
		DedupeKey:      queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, runtimeInputID),
		PayloadVersion: 1, PayloadJSON: payload, Priority: 100,
		MaxAttempts: queue.DefaultMaxAttempts, Now: time.Now().UTC().Add(-time.Second),
	}); err != nil {
		t.Fatalf("seed active interrupt Queue custody: %v", err)
	}
}
