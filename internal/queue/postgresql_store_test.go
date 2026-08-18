package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestPostgreSQLStoreLeasePriorityPartitionBarrierAndAckFence(t *testing.T) {
	store, admin := newPostgreSQLQueueStore(t)
	ctx := context.Background()
	ws := workspace.ID("ws_queue_barrier")
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	sessionID := "sesn_queue_a"
	otherSessionID := "sesn_queue_b"
	partition := FormatSessionPartitionKey(ws, sessionID)
	otherPartition := FormatSessionPartitionKey(ws, otherSessionID)

	mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_low",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: partition,
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, sessionID, "input_low"),
		PayloadJSON:  runtimeInputPayload(t, ws, sessionID, "thrd_queue_a", "input_low", "messages", 1, 1),
		Priority:     0,
		Now:          now,
	})
	interrupt := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_high",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: partition,
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, sessionID, "input_interrupt"),
		PayloadJSON:  runtimeInputPayload(t, ws, sessionID, "thrd_queue_a", "input_interrupt", "interrupt_control", 2, 2),
		Priority:     100,
		Now:          now.Add(time.Millisecond),
	})
	other := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_other",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: otherPartition,
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, otherSessionID, "input_other"),
		PayloadJSON:  runtimeInputPayload(t, ws, otherSessionID, "thrd_queue_b", "input_other", "messages", 1, 1),
		Priority:     0,
		Now:          now.Add(2 * time.Millisecond),
	})

	leased, err := store.Lease(ctx, LeaseRequest{
		WorkspaceID:   ws,
		Kinds:         []string{KindRuntimeInput},
		LeaseOwner:    "bridge-job-runner",
		MaxJobs:       2,
		LeaseDuration: time.Minute,
		Now:           now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	assertLeasedIDs(t, leased, []string{interrupt.ID, other.ID})
	if got := queueJobStatus(t, admin, ws, "qjob_low"); got != StatusPending {
		t.Fatalf("low-priority same-partition job status = %s; want pending barrier", got)
	}
	if ok, err := store.Ack(ctx, AckRequest{WorkspaceID: ws, JobID: interrupt.ID, LeaseToken: "stale-token", Now: now.Add(2 * time.Second)}); err != nil || ok {
		t.Fatalf("stale Ack = (%v,%v); want false,nil", ok, err)
	}
	if got := queueJobStatus(t, admin, ws, interrupt.ID); got != StatusLeased {
		t.Fatalf("stale Ack changed status to %s; want leased", got)
	}
	if ok, err := store.Ack(ctx, AckRequest{WorkspaceID: ws, JobID: interrupt.ID, LeaseToken: leased[0].LeaseToken, Now: now.Add(3 * time.Second)}); err != nil || !ok {
		t.Fatalf("Ack = (%v,%v); want true,nil", ok, err)
	}
	next, err := store.Lease(ctx, LeaseRequest{
		WorkspaceID:   ws,
		Kinds:         []string{KindRuntimeInput},
		LeaseOwner:    "bridge-job-runner",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now.Add(4 * time.Second),
	})
	if err != nil {
		t.Fatalf("second Lease: %v", err)
	}
	assertLeasedIDs(t, next, []string{"qjob_low"})
}

func TestPostgreSQLQueueNotificationPublishesOnlyCommittedNewWork(t *testing.T) {
	store, admin := newPostgreSQLQueueStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{})
	payloads := make(chan string, 4)
	done := make(chan error, 1)
	go func() {
		done <- (PostgreSQLNotificationListener{Client: store.client}).Listen(ctx, NotificationChannel, func() {
			close(ready)
		}, func(payload string) {
			payloads <- payload
		})
	}()
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("notification listener did not become ready")
	}

	ws := workspace.ID("ws_queue_notify")
	now := time.Now().UTC()
	committed := EnqueueRequest{
		ID: "qjob_notify_committed", WorkspaceID: ws, Kind: KindRuntimeInput,
		PartitionKey: FormatSessionPartitionKey(ws, "sesn_notify"),
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, "sesn_notify", "input_committed"),
		PayloadJSON:  runtimeInputPayload(t, ws, "sesn_notify", "thrd_notify", "input_committed", "messages", 1, 1),
		Now:          now,
	}
	mustEnqueue(t, store, committed)
	select {
	case payload := <-payloads:
		if payload != ConsumerClassBridge {
			t.Fatalf("notification payload = %q; want %q", payload, ConsumerClassBridge)
		}
	case <-time.After(time.Second):
		t.Fatal("committed enqueue did not publish a notification")
	}

	rollbackErr := errors.New("force rollback")
	rolledBack := committed
	rolledBack.ID = "qjob_notify_rollback"
	rolledBack.DedupeKey = FormatRuntimeInputDedupeKey(ws, "sesn_notify", "input_rolled_back")
	rolledBack.PayloadJSON = runtimeInputPayload(t, ws, "sesn_notify", "thrd_notify", "input_rolled_back", "messages", 2, 2)
	err := store.client.WithWorkspaceTx(context.Background(), string(ws), "queue.test_notify_rollback", func(tx *dbconnect.Tx) error {
		if _, err := EnqueueTx(context.Background(), tx, rolledBack); err != nil {
			return err
		}
		return rollbackErr
	})
	if !errors.Is(err, rollbackErr) {
		t.Fatalf("rolled-back enqueue error = %v", err)
	}
	if queueJobExists(t, admin, ws, rolledBack.ID) {
		t.Fatal("rolled-back queue job exists")
	}
	select {
	case payload := <-payloads:
		t.Fatalf("rolled-back enqueue published %q", payload)
	case <-time.After(100 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("listener shutdown = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("listener did not stop")
	}
}

func TestPostgreSQLQueueMultipleWakeHintsStillLeaseOneJob(t *testing.T) {
	store, _ := newPostgreSQLQueueStore(t)
	ws := workspace.ID("ws_queue_wake_race")
	now := time.Now().UTC()
	mustEnqueue(t, store, EnqueueRequest{
		ID: "qjob_wake_race", WorkspaceID: ws, Kind: KindRuntimeInput,
		PartitionKey: FormatSessionPartitionKey(ws, "sesn_wake_race"),
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, "sesn_wake_race", "input_wake_race"),
		PayloadJSON:  runtimeInputPayload(t, ws, "sesn_wake_race", "thrd_wake_race", "input_wake_race", "messages", 1, 1),
		Now:          now,
	})
	wake := NewWakeSignal()
	for range 8 {
		wake.Broadcast()
	}
	start := make(chan struct{})
	leasedCounts := make(chan int, 2)
	for index := range 2 {
		go func(owner int) {
			<-start
			jobs, err := store.Lease(context.Background(), LeaseRequest{
				WorkspaceID: ws, Kinds: []string{KindRuntimeInput}, LeaseOwner: "wake-race-" + strconv.Itoa(owner),
				MaxJobs: 1, LeaseDuration: time.Minute,
			})
			if err != nil {
				leasedCounts <- -1
				return
			}
			leasedCounts <- len(jobs)
		}(index)
	}
	close(start)
	total := 0
	for range 2 {
		count := <-leasedCounts
		if count < 0 {
			t.Fatal("concurrent Lease failed")
		}
		total += count
	}
	if total != 1 {
		t.Fatalf("jobs leased after repeated wake hints = %d; want exactly one", total)
	}
}

func TestPostgreSQLStoreLeaseAndHeartbeatUseDatabaseClock(t *testing.T) {
	store, admin := newPostgreSQLQueueStore(t)
	ctx := context.Background()
	ws := workspace.ID("ws_queue_database_clock")
	now := time.Now().UTC()
	request := sandboxToolExecuteRequest(t, ws, "sesn_database_clock", "thrd_database_clock", "evt_database_clock", "qjob_database_clock", 5, now.Add(-time.Minute))
	job := mustEnqueue(t, store, request)

	leased := mustLeaseOne(t, store, LeaseRequest{
		WorkspaceID:   ws,
		Kinds:         []string{KindSandboxToolExecute},
		LeaseOwner:    "sandbox",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now.Add(24 * time.Hour),
	})
	if leased.ID != job.ID || leased.LeasedUntil == nil {
		t.Fatalf("leased job = %+v; want %s with expiry", leased, job.ID)
	}
	if delta := leased.LeasedUntil.Sub(time.Now().UTC()); delta < 45*time.Second || delta > 75*time.Second {
		t.Fatalf("database-authored lease residual = %s; want about one minute", delta)
	}

	if _, err := admin.ExecContext(ctx,
		`UPDATE queue_jobs SET leased_until = clock_timestamp() - interval '1 second'
		  WHERE workspace_id = $1 AND id = $2`, string(ws), job.ID,
	); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
	heartbeat, err := store.Heartbeat(ctx, HeartbeatRequest{
		WorkspaceID: ws, JobID: job.ID, LeaseToken: leased.LeaseToken,
		LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatalf("Heartbeat expired lease: %v", err)
	}
	if heartbeat.Updated {
		t.Fatal("Heartbeat revived an expired lease")
	}
	if reclaimed, err := store.ReclaimExpiredLeases(ctx, ReclaimExpiredLeasesRequest{WorkspaceID: ws, Limit: 1}); err != nil || reclaimed != 1 {
		t.Fatalf("ReclaimExpiredLeases after rejected heartbeat = %d, %v; want 1,nil", reclaimed, err)
	}
	if status := queueJobStatus(t, admin, ws, job.ID); status != StatusPending {
		t.Fatalf("status after heartbeat-versus-reclaim = %q; want pending", status)
	}
}

func TestPostgreSQLStoreRejectsTerminalTransitionsAfterLeaseExpiry(t *testing.T) {
	for _, transition := range []string{"ack", "retry", "dead_letter", "defer"} {
		t.Run(transition, func(t *testing.T) {
			store, admin := newPostgreSQLQueueStore(t)
			ctx := context.Background()
			ws := workspace.ID("ws_queue_expired_" + transition)
			now := time.Now().UTC()
			sessionID := "sesn_queue_expired_" + transition
			jobID := map[string]string{"ack": "qjob_exp_ack", "retry": "qjob_exp_retry", "dead_letter": "qjob_exp_dead", "defer": "qjob_exp_defer"}[transition]
			var request EnqueueRequest
			if transition == "defer" {
				request = EnqueueRequest{
					ID: jobID, WorkspaceID: ws,
					Kind: KindRuntimeConfigUpdate, PartitionKey: FormatSessionPartitionKey(ws, sessionID),
					DedupeKey: FormatRuntimeConfigUpdateDedupeKey(ws, sessionID, "1"),
					PayloadJSON: queuePayload(t, map[string]any{
						"workspace_id": string(ws), "session_id": sessionID, "config_generation": 1,
					}),
					MaxAttempts: 3, Now: now,
				}
			} else {
				request = sandboxToolExecuteRequest(t, ws, sessionID, "thrd_queue_expired", "evt_queue_expired", jobID, 3, now)
			}
			job := mustEnqueue(t, store, request)
			leased := mustLeaseOne(t, store, LeaseRequest{
				WorkspaceID: ws, Kinds: []string{request.Kind}, LeaseOwner: "expired-transition-test",
				MaxJobs: 1, LeaseDuration: time.Minute,
			})
			expireQueueJobLease(t, admin, ws, job.ID)

			var updated bool
			var err error
			switch transition {
			case "ack":
				updated, err = store.Ack(ctx, AckRequest{WorkspaceID: ws, JobID: job.ID, LeaseToken: leased.LeaseToken, Now: now})
			case "retry":
				updated, err = store.Retry(ctx, RetryRequest{WorkspaceID: ws, JobID: job.ID, LeaseToken: leased.LeaseToken, ErrorKind: "expired_test", Now: now})
			case "dead_letter":
				updated, err = store.DeadLetter(ctx, DeadLetterRequest{WorkspaceID: ws, JobID: job.ID, LeaseToken: leased.LeaseToken, ErrorKind: "expired_test", Now: now})
			case "defer":
				updated, err = store.Defer(ctx, DeferRequest{WorkspaceID: ws, JobID: job.ID, LeaseToken: leased.LeaseToken, Now: now})
			}
			if err != nil || updated {
				t.Fatalf("expired %s = (%v,%v); want false,nil", transition, updated, err)
			}
			if status := queueJobStatus(t, admin, ws, job.ID); status != StatusLeased {
				t.Fatalf("status after expired %s = %q; want leased", transition, status)
			}
		})
	}
}

func TestAssertActiveLeaseTxRejectsExpiredAuthority(t *testing.T) {
	store, admin := newPostgreSQLQueueStore(t)
	ctx := context.Background()
	ws := workspace.ID("ws_queue_active_fence")
	now := time.Now().UTC()
	job := mustEnqueue(t, store, sandboxToolExecuteRequest(t, ws, "sesn_active_fence", "thrd_active_fence", "evt_active_fence", "qjob_active_fence", 5, now))
	leased := mustLeaseOne(t, store, LeaseRequest{
		WorkspaceID: ws, Kinds: []string{KindSandboxToolExecute}, LeaseOwner: "sandbox",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(time.Second),
	})

	assert := func(want bool) {
		t.Helper()
		if err := store.client.WithWorkspaceTx(ctx, string(ws), "queue.test_active_lease", func(tx *dbconnect.Tx) error {
			active, err := AssertActiveLeaseTx(ctx, tx, ActiveLeaseRequest{WorkspaceID: ws, JobID: job.ID, LeaseToken: leased.LeaseToken})
			if err != nil {
				return err
			}
			if active != want {
				t.Fatalf("active lease = %t; want %t", active, want)
			}
			return nil
		}); err != nil {
			t.Fatalf("AssertActiveLeaseTx: %v", err)
		}
	}
	assert(true)
	expireQueueJobLease(t, admin, ws, job.ID)
	assert(false)
}

func TestAssertActiveLeaseTxUsesClockAfterWaitingForQueueLock(t *testing.T) {
	store, admin := newPostgreSQLQueueStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ws := workspace.ID("ws_queue_active_fence_wait")
	now := time.Now().UTC()
	job := mustEnqueue(t, store, sandboxToolExecuteRequest(t, ws, "sesn_fence_wait", "thrd_fence_wait", "evt_fence_wait", "qjob_fence_wait", 5, now))
	leased := mustLeaseOne(t, store, LeaseRequest{
		WorkspaceID: ws, Kinds: []string{KindSandboxToolExecute}, LeaseOwner: "sandbox",
		MaxJobs: 1, LeaseDuration: 250 * time.Millisecond, Now: now,
	})

	blocker, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin blocker: %v", err)
	}
	defer func() { _ = blocker.Rollback() }()
	var locked string
	if err := blocker.QueryRowContext(ctx, `SELECT id FROM queue_jobs WHERE workspace_id=$1 AND id=$2 FOR UPDATE`, string(ws), job.ID).Scan(&locked); err != nil {
		t.Fatalf("lock Queue row: %v", err)
	}

	started := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- store.client.WithWorkspaceTx(ctx, string(ws), "queue.test_waiting_active_lease", func(tx *dbconnect.Tx) error {
			close(started)
			active, err := AssertActiveLeaseTx(ctx, tx, ActiveLeaseRequest{WorkspaceID: ws, JobID: job.ID, LeaseToken: leased.LeaseToken})
			if err != nil {
				return err
			}
			if active {
				return errors.New("lease remained active after the lock wait crossed its expiry")
			}
			return nil
		})
	}()
	<-started
	time.Sleep(350 * time.Millisecond)
	if err := blocker.Commit(); err != nil {
		t.Fatalf("release Queue lock: %v", err)
	}
	if err := <-result; err != nil {
		t.Fatalf("AssertActiveLeaseTx after wait: %v", err)
	}
}

func TestPostgreSQLStoreLeaseHonorsCrossKindSessionBarrier(t *testing.T) {
	store, _ := newPostgreSQLQueueStore(t)
	ctx := context.Background()
	ws := workspace.ID("ws_queue_cross_kind")
	sessionID := "sesn_cross_kind"
	now := time.Date(2026, 7, 1, 12, 30, 0, 0, time.UTC)
	partition := FormatSessionPartitionKey(ws, sessionID)

	message := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_cross_message",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: partition,
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, sessionID, "input_message"),
		PayloadJSON:  runtimeInputPayload(t, ws, sessionID, "thrd_cross_kind", "input_message", "messages", 1, 1),
		Now:          now,
	})
	config := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_cross_config",
		WorkspaceID:  ws,
		Kind:         KindRuntimeConfigUpdate,
		PartitionKey: partition,
		DedupeKey:    FormatRuntimeConfigUpdateDedupeKey(ws, sessionID, "42"),
		PayloadJSON:  []byte(`{"workspace_id":"ws_queue_cross_kind","session_id":"sesn_cross_kind","config_generation":42}`),
		Now:          now.Add(time.Millisecond),
	})
	interrupt := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_cross_interrupt",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: partition,
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, sessionID, "input_interrupt"),
		PayloadJSON:  runtimeInputPayload(t, ws, sessionID, "thrd_cross_kind", "input_interrupt", "interrupt_control", 2, 2),
		Priority:     100,
		Now:          now.Add(2 * time.Millisecond),
	})

	first := mustLeaseOne(t, store, LeaseRequest{
		WorkspaceID:   ws,
		Kinds:         []string{KindRuntimeInput, KindRuntimeConfigUpdate},
		LeaseOwner:    "bridge",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now.Add(time.Second),
	})
	if first.ID != interrupt.ID {
		t.Fatalf("first cross-kind lease = %s; want priority interrupt %s to cross message/config", first.ID, interrupt.ID)
	}
	if ok, err := store.Ack(ctx, AckRequest{WorkspaceID: ws, JobID: first.ID, LeaseToken: first.LeaseToken, Now: now.Add(2 * time.Second)}); err != nil || !ok {
		t.Fatalf("Ack first cross-kind job = (%v,%v); want true,nil", ok, err)
	}
	second := mustLeaseOne(t, store, LeaseRequest{
		WorkspaceID:   ws,
		Kinds:         []string{KindRuntimeInput, KindRuntimeConfigUpdate},
		LeaseOwner:    "bridge",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now.Add(3 * time.Second),
	})
	if second.ID != message.ID {
		t.Fatalf("second cross-kind lease = %s; want earlier message %s before config", second.ID, message.ID)
	}
	if ok, err := store.Ack(ctx, AckRequest{WorkspaceID: ws, JobID: second.ID, LeaseToken: second.LeaseToken, Now: now.Add(4 * time.Second)}); err != nil || !ok {
		t.Fatalf("Ack second cross-kind job = (%v,%v); want true,nil", ok, err)
	}
	third := mustLeaseOne(t, store, LeaseRequest{
		WorkspaceID:   ws,
		Kinds:         []string{KindRuntimeInput, KindRuntimeConfigUpdate},
		LeaseOwner:    "bridge",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now.Add(5 * time.Second),
	})
	if third.ID != config.ID {
		t.Fatalf("third cross-kind lease = %s; want config barrier %s after interrupt", third.ID, config.ID)
	}
}

func TestPostgreSQLStoreDelayedRuntimeConfigBlocksOnlyLaterOrdinaryInput(t *testing.T) {
	store, _ := newPostgreSQLQueueStore(t)
	ctx := context.Background()
	ws := workspace.ID("ws_queue_delayed_config")
	sessionID := "sesn_delayed_config"
	now := time.Date(2026, 7, 1, 12, 35, 0, 0, time.UTC)
	partition := FormatSessionPartitionKey(ws, sessionID)
	config := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_z_delayed_config",
		WorkspaceID:  ws,
		Kind:         KindRuntimeConfigUpdate,
		PartitionKey: partition,
		DedupeKey:    FormatRuntimeConfigUpdateDedupeKey(ws, sessionID, "9"),
		PayloadJSON:  []byte(`{"workspace_id":"ws_queue_delayed_config","session_id":"sesn_delayed_config","config_generation":9}`),
		AvailableAt:  now.Add(time.Minute),
		Now:          now,
	})
	ordinary := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_a_ordinary",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: partition,
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, sessionID, "input_ordinary"),
		PayloadJSON:  runtimeInputPayload(t, ws, sessionID, "thrd_delayed_config", "input_ordinary", "messages", 1, 1),
		Now:          now,
	})
	interrupt := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_0_interrupt",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: partition,
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, sessionID, "input_interrupt"),
		PayloadJSON:  runtimeInputPayload(t, ws, sessionID, "thrd_delayed_config", "input_interrupt", "interrupt_control", 2, 2),
		Priority:     100,
		Now:          now,
	})
	if config.QueuePartitionSequence >= ordinary.QueuePartitionSequence ||
		ordinary.QueuePartitionSequence >= interrupt.QueuePartitionSequence {
		t.Fatalf("partition sequences = config %d ordinary %d interrupt %d; want admission order",
			config.QueuePartitionSequence, ordinary.QueuePartitionSequence, interrupt.QueuePartitionSequence)
	}

	first := mustLeaseOne(t, store, LeaseRequest{
		WorkspaceID: ws, Kinds: []string{KindRuntimeInput, KindRuntimeConfigUpdate}, LeaseOwner: "bridge",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(time.Second),
	})
	if first.ID != interrupt.ID {
		t.Fatalf("lease before config availability = %s; want interrupt %s", first.ID, interrupt.ID)
	}
	if ok, err := store.Ack(ctx, AckRequest{WorkspaceID: ws, JobID: first.ID, LeaseToken: first.LeaseToken, Now: now.Add(2 * time.Second)}); err != nil || !ok {
		t.Fatalf("Ack interrupt = (%v,%v); want true,nil", ok, err)
	}
	leased, err := store.Lease(ctx, LeaseRequest{
		WorkspaceID: ws, Kinds: []string{KindRuntimeInput, KindRuntimeConfigUpdate}, LeaseOwner: "bridge",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(3 * time.Second),
	})
	if err != nil || len(leased) != 0 {
		t.Fatalf("ordinary input before config availability = %+v err=%v; want blocked", leased, err)
	}
	second := mustLeaseOne(t, store, LeaseRequest{
		WorkspaceID: ws, Kinds: []string{KindRuntimeInput, KindRuntimeConfigUpdate}, LeaseOwner: "bridge",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(time.Minute),
	})
	if second.ID != config.ID {
		t.Fatalf("lease at config availability = %s; want config %s", second.ID, config.ID)
	}
	if ok, err := store.Ack(ctx, AckRequest{WorkspaceID: ws, JobID: second.ID, LeaseToken: second.LeaseToken, Now: now.Add(time.Minute + time.Second)}); err != nil || !ok {
		t.Fatalf("Ack config = (%v,%v); want true,nil", ok, err)
	}
	third := mustLeaseOne(t, store, LeaseRequest{
		WorkspaceID: ws, Kinds: []string{KindRuntimeInput, KindRuntimeConfigUpdate}, LeaseOwner: "bridge",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(time.Minute + 2*time.Second),
	})
	if third.ID != ordinary.ID {
		t.Fatalf("lease after config ACK = %s; want ordinary input %s", third.ID, ordinary.ID)
	}
}

func TestPostgreSQLStoreLeaseRuntimeConfigBeforeRetryingRuntimeInput(t *testing.T) {
	runtime, _ := storagetest.NewPostgreSQLDBWithAdmin(t)
	store := NewPostgreSQLStoreWithRetryPolicy(dbconnect.NewClientForTesting(runtime), RetryPolicy{
		BaseDelay: time.Second,
		MaxDelay:  time.Minute,
		RandomInt64: func(bound int64) int64 {
			return bound - 1
		},
	})
	ctx := context.Background()
	ws := workspace.ID("ws_queue_config_retry")
	sessionID := "sesn_config_retry"
	now := time.Date(2026, 7, 1, 12, 40, 0, 0, time.UTC)
	partition := FormatSessionPartitionKey(ws, sessionID)
	message := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_mretry_input",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: partition,
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, sessionID, "input_message"),
		PayloadJSON:  runtimeInputPayload(t, ws, sessionID, "thrd_manifest_retry", "input_message", "messages", 1, 1),
		Now:          now,
	})
	leasedMessage := mustLeaseOne(t, store, LeaseRequest{
		WorkspaceID:   ws,
		Kinds:         []string{KindRuntimeInput, KindRuntimeConfigUpdate},
		LeaseOwner:    "bridge",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now.Add(time.Second),
	})
	if leasedMessage.ID != message.ID {
		t.Fatalf("first lease = %s; want message %s", leasedMessage.ID, message.ID)
	}
	manifest := mustEnqueue(t, store, EnqueueRequest{
		ID:             "qjob_mretry_config",
		WorkspaceID:    ws,
		Kind:           KindRuntimeConfigUpdate,
		PartitionKey:   partition,
		DedupeKey:      FormatRuntimeMCPManifestUpdateDedupeKey(ws, sessionID, "github", "1"),
		PayloadVersion: 2,
		PayloadJSON: queuePayload(t, map[string]any{
			"workspace_id":        string(ws),
			"session_id":          sessionID,
			"mcp_server_name":     "github",
			"manifest_generation": 1,
		}),
		Now: now.Add(2 * time.Second),
	})
	if ok, err := store.Retry(ctx, RetryRequest{
		WorkspaceID:  ws,
		JobID:        leasedMessage.ID,
		LeaseToken:   leasedMessage.LeaseToken,
		ErrorKind:    "runtime_transport_unavailable",
		ErrorMessage: "runtime input delivery will retry after configuration work",
		Now:          now.Add(2 * time.Second),
	}); err != nil || !ok {
		t.Fatalf("Retry message after manifest enqueue = (%v,%v); want true,nil", ok, err)
	}
	next := mustLeaseOne(t, store, LeaseRequest{
		WorkspaceID:   ws,
		Kinds:         []string{KindRuntimeInput, KindRuntimeConfigUpdate},
		LeaseOwner:    "bridge",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now.Add(2500 * time.Millisecond),
	})
	if next.ID != manifest.ID {
		t.Fatalf("next lease = %s; want manifest config %s before retried message", next.ID, manifest.ID)
	}
}

func TestPostgreSQLStoreLeaseCandidateWindowKeepsInterruptException(t *testing.T) {
	store, _ := newPostgreSQLQueueStore(t)
	ws := workspace.ID("ws_queue_starve")
	sessionID := "sesn_starve"
	now := time.Date(2026, 7, 1, 12, 45, 0, 0, time.UTC)
	partition := FormatSessionPartitionKey(ws, sessionID)
	config := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_starve_config",
		WorkspaceID:  ws,
		Kind:         KindRuntimeConfigUpdate,
		PartitionKey: partition,
		DedupeKey:    FormatRuntimeConfigUpdateDedupeKey(ws, sessionID, "7"),
		PayloadJSON:  []byte(`{"workspace_id":"ws_queue_starve","session_id":"sesn_starve","config_generation":7}`),
		Now:          now,
	})
	for index := 0; index < 24; index++ {
		inputID := "input_interrupt_" + string(rune('a'+index))
		mustEnqueue(t, store, EnqueueRequest{
			ID:           "qjob_starve_int_" + string(rune('a'+index)),
			WorkspaceID:  ws,
			Kind:         KindRuntimeInput,
			PartitionKey: partition,
			DedupeKey:    FormatRuntimeInputDedupeKey(ws, sessionID, inputID),
			PayloadJSON:  runtimeInputPayload(t, ws, sessionID, "thrd_starve", inputID, "interrupt_control", int64(index+1), int64(index+1)),
			Priority:     100,
			Now:          now.Add(time.Duration(index+1) * time.Millisecond),
		})
	}
	leased := mustLeaseOne(t, store, LeaseRequest{
		WorkspaceID:   ws,
		Kinds:         []string{KindRuntimeInput, KindRuntimeConfigUpdate},
		LeaseOwner:    "bridge",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now.Add(time.Second),
	})
	if leased.Kind != KindRuntimeInput || string(leased.PayloadJSON) == "" || leased.ID == config.ID {
		t.Fatalf("candidate-window lease = %#v; want an interrupt to cross config %s", leased, config.ID)
	}
}

func TestPostgreSQLStoreInterruptRetryRemainsSessionBarrierThroughFinalizationRecovery(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	store := NewPostgreSQLStoreWithRetryPolicy(dbconnect.NewClientForTesting(runtime), RetryPolicy{
		BaseDelay: time.Minute, MaxDelay: time.Minute, MaxAttempts: 2,
		RandomInt64: func(bound int64) int64 { return bound - 1 },
	})
	ctx := context.Background()
	ws := workspace.ID("ws_interrupt_retry_barrier")
	sessionID := "sesn_interrupt_retry_barrier"
	threadID := "thrd_interrupt_retry_barrier"
	now := time.Date(2026, 7, 1, 12, 47, 0, 0, time.UTC)
	partition := FormatSessionPartitionKey(ws, sessionID)
	interrupt := mustEnqueue(t, store, EnqueueRequest{
		ID: "qjob_intbar", WorkspaceID: ws, Kind: KindRuntimeInput,
		PartitionKey: partition, DedupeKey: FormatRuntimeInputDedupeKey(ws, sessionID, "rin_interrupt_barrier"),
		PayloadJSON: runtimeInputPayload(t, ws, sessionID, threadID, "rin_interrupt_barrier", "interrupt_control", 1, 1),
		Priority:    100, MaxAttempts: 2, Now: now,
	})
	mustEnqueue(t, store, EnqueueRequest{
		ID: "qjob_msgafter", WorkspaceID: ws, Kind: KindRuntimeInput,
		PartitionKey: partition, DedupeKey: FormatRuntimeInputDedupeKey(ws, sessionID, "rin_message_after_interrupt"),
		PayloadJSON: runtimeInputPayload(t, ws, sessionID, threadID, "rin_message_after_interrupt", "messages", 2, 2), Priority: 200, Now: now.Add(time.Second),
	})
	mustEnqueue(t, store, EnqueueRequest{
		ID: "qjob_sibling_after", WorkspaceID: ws, Kind: KindRuntimeInput,
		PartitionKey: partition, DedupeKey: FormatRuntimeInputDedupeKey(ws, sessionID, "rin_sibling_after_interrupt"),
		PayloadJSON: runtimeInputPayload(t, ws, sessionID, "thrd_interrupt_barrier_sibling", "rin_sibling_after_interrupt", "messages", 3, 3), Priority: 300, Now: now.Add(2 * time.Second),
	})
	mustEnqueue(t, store, EnqueueRequest{
		ID: "qjob_int2", WorkspaceID: ws, Kind: KindRuntimeInput,
		PartitionKey: partition, DedupeKey: FormatRuntimeInputDedupeKey(ws, sessionID, "rin_second_interrupt"),
		PayloadJSON: runtimeInputPayload(t, ws, sessionID, threadID, "rin_second_interrupt", "interrupt_control", 4, 4),
		Priority:    400, MaxAttempts: 2, Now: now.Add(3 * time.Second),
	})

	first := mustLeaseOne(t, store, LeaseRequest{WorkspaceID: ws, Kinds: []string{KindRuntimeInput}, LeaseOwner: "bridge-a", MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(4 * time.Second)})
	if first.ID != interrupt.ID || first.AttemptCount != 1 {
		t.Fatalf("first lease = %s attempt=%d; want interrupt attempt 1", first.ID, first.AttemptCount)
	}
	if ok, err := store.Retry(ctx, RetryRequest{WorkspaceID: ws, JobID: first.ID, LeaseToken: first.LeaseToken, ErrorKind: "runtime_transport_error", Now: now.Add(4 * time.Second)}); err != nil || !ok {
		t.Fatalf("Retry first interrupt = (%v,%v); want true,nil", ok, err)
	}
	if leased, err := store.Lease(ctx, LeaseRequest{WorkspaceID: ws, Kinds: []string{KindRuntimeInput}, LeaseOwner: "bridge-b", MaxJobs: 3, LeaseDuration: time.Minute, Now: now.Add(5 * time.Second)}); err != nil || len(leased) != 0 {
		t.Fatalf("leases during interrupt backoff = %+v err=%v; want none", leased, err)
	}

	second := mustLeaseOne(t, store, LeaseRequest{WorkspaceID: ws, Kinds: []string{KindRuntimeInput}, LeaseOwner: "bridge-b", MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(2 * time.Minute)})
	if second.ID != interrupt.ID || second.AttemptCount != 2 {
		t.Fatalf("second lease = %s attempt=%d; want same interrupt attempt 2", second.ID, second.AttemptCount)
	}
	if ok, err := store.Retry(ctx, RetryRequest{WorkspaceID: ws, JobID: second.ID, LeaseToken: second.LeaseToken, ErrorKind: "interrupt_closeout_pending", Now: now.Add(2*time.Minute + time.Second)}); err != nil || !ok {
		t.Fatalf("Retry final interrupt = (%v,%v); want retained barrier", ok, err)
	}
	recovery := mustLeaseOne(t, store, LeaseRequest{WorkspaceID: ws, Kinds: []string{KindRuntimeInput}, LeaseOwner: "bridge-c", MaxJobs: 3, LeaseDuration: time.Minute, Now: now.Add(10 * time.Minute)})
	if recovery.ID != interrupt.ID || recovery.AttemptCount != 2 {
		t.Fatalf("finalization recovery lease = %s attempt=%d; want same interrupt capped at attempt 2", recovery.ID, recovery.AttemptCount)
	}
	var status string
	var attempts int
	if err := admin.QueryRowContext(ctx, `SELECT status, attempt_count FROM queue_jobs WHERE workspace_id=$1 AND id=$2`, string(ws), interrupt.ID).Scan(&status, &attempts); err != nil {
		t.Fatalf("read finalization recovery lease: %v", err)
	}
	if status != StatusLeased || attempts != 2 {
		t.Fatalf("finalization recovery = %s attempt=%d; want leased attempt=2", status, attempts)
	}
}

func TestPostgreSQLStoreExpiredFinalInterruptLeaseReclaimsAsBarrier(t *testing.T) {
	store, admin := newPostgreSQLQueueStore(t)
	ctx := context.Background()
	ws := workspace.ID("ws_interrupt_reclaim_barrier")
	sessionID := "sesn_interrupt_reclaim_barrier"
	now := time.Date(2026, 7, 1, 12, 48, 0, 0, time.UTC)
	partition := FormatSessionPartitionKey(ws, sessionID)
	interrupt := mustEnqueue(t, store, EnqueueRequest{
		ID: "qjob_reclaim_int", WorkspaceID: ws, Kind: KindRuntimeInput,
		PartitionKey: partition, DedupeKey: FormatRuntimeInputDedupeKey(ws, sessionID, "rin_reclaim_int"),
		PayloadJSON: runtimeInputPayload(t, ws, sessionID, "thrd_reclaim", "rin_reclaim_int", "interrupt_control", 1, 1),
		Priority:    100, MaxAttempts: 1, Now: now,
	})
	mustEnqueue(t, store, EnqueueRequest{
		ID: "qjob_reclaim_msg", WorkspaceID: ws, Kind: KindRuntimeInput,
		PartitionKey: partition, DedupeKey: FormatRuntimeInputDedupeKey(ws, sessionID, "rin_reclaim_msg"),
		PayloadJSON: runtimeInputPayload(t, ws, sessionID, "thrd_reclaim", "rin_reclaim_msg", "messages", 2, 2), Now: now.Add(time.Second),
	})
	leased := mustLeaseOne(t, store, LeaseRequest{WorkspaceID: ws, Kinds: []string{KindRuntimeInput}, LeaseOwner: "bridge-old", MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(2 * time.Second)})
	if leased.ID != interrupt.ID || leased.AttemptCount != 1 {
		t.Fatalf("final interrupt lease = %s attempt=%d; want interrupt attempt 1", leased.ID, leased.AttemptCount)
	}
	if _, err := admin.ExecContext(ctx, `UPDATE queue_jobs SET leased_until=clock_timestamp()-interval '1 second' WHERE workspace_id=$1 AND id=$2`, string(ws), interrupt.ID); err != nil {
		t.Fatalf("expire interrupt lease: %v", err)
	}
	if reclaimed, err := store.ReclaimExpiredLeases(ctx, ReclaimExpiredLeasesRequest{WorkspaceID: ws}); err != nil || reclaimed != 1 {
		t.Fatalf("ReclaimExpiredLeases = %d/%v; want 1/nil", reclaimed, err)
	}
	candidates, err := store.Lease(ctx, LeaseRequest{WorkspaceID: ws, Kinds: []string{KindRuntimeInput}, LeaseOwner: "bridge-new", MaxJobs: 2, LeaseDuration: time.Minute, Now: time.Now().UTC().Add(time.Minute)})
	if err != nil || len(candidates) != 1 || candidates[0].ID != interrupt.ID {
		t.Fatalf("post-reclaim leases = %+v/%v; want only exhausted interrupt finalization recovery", candidates, err)
	}
	var status string
	var attempts int
	if err := admin.QueryRowContext(ctx, `SELECT status, attempt_count FROM queue_jobs WHERE workspace_id=$1 AND id=$2`, string(ws), interrupt.ID).Scan(&status, &attempts); err != nil {
		t.Fatalf("read reclaimed interrupt: %v", err)
	}
	if status != StatusLeased || attempts != 1 {
		t.Fatalf("reclaimed interrupt = %s attempt=%d; want re-leased capped attempt=1", status, attempts)
	}
}

func TestPostgreSQLStoreDatabaseAssignsPartitionSequence(t *testing.T) {
	store, _ := newPostgreSQLQueueStore(t)
	ws := workspace.ID("ws_queue_partition_sequence")
	sessionID := "sesn_partition_sequence"
	now := time.Date(2026, 7, 1, 12, 50, 0, 0, time.UTC)
	partition := FormatSessionPartitionKey(ws, sessionID)
	first := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_z_first",
		WorkspaceID:  ws,
		Kind:         KindRuntimeConfigUpdate,
		PartitionKey: partition,
		DedupeKey:    FormatRuntimeConfigUpdateDedupeKey(ws, sessionID, "1"),
		PayloadJSON:  []byte(`{"workspace_id":"ws_queue_partition_sequence","session_id":"sesn_partition_sequence","config_generation":1}`),
		Now:          now,
	})
	second := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_a_second",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: partition,
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, sessionID, "input_second"),
		PayloadJSON:  runtimeInputPayload(t, ws, sessionID, "thrd_partition_sequence", "input_second", "messages", 1, 1),
		Now:          now,
	})
	if first.QueuePartitionSequence != 1 || second.QueuePartitionSequence != 2 {
		t.Fatalf("partition sequences = %d, %d; want 1, 2 despite equal timestamps and reverse ids",
			first.QueuePartitionSequence, second.QueuePartitionSequence)
	}
	replay := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_replay",
		WorkspaceID:  ws,
		Kind:         KindRuntimeConfigUpdate,
		PartitionKey: partition,
		DedupeKey:    first.DedupeKey,
		PayloadJSON:  append([]byte(nil), first.PayloadJSON...),
		Now:          now.Add(time.Hour),
	})
	if replay.ID != first.ID || replay.QueuePartitionSequence != first.QueuePartitionSequence {
		t.Fatalf("dedupe replay = %s/%d; want %s/%d", replay.ID, replay.QueuePartitionSequence, first.ID, first.QueuePartitionSequence)
	}
	leased := mustLeaseOne(t, store, LeaseRequest{
		WorkspaceID: ws, Kinds: []string{KindRuntimeInput, KindRuntimeConfigUpdate}, LeaseOwner: "bridge",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(time.Second),
	})
	if leased.ID != first.ID {
		t.Fatalf("first lease = %s; want database-first config %s", leased.ID, first.ID)
	}
}

func TestEnqueueBatchTxLocksPartitionsInDeterministicOrder(t *testing.T) {
	store, admin := newPostgreSQLQueueStore(t)
	ws := workspace.ID("ws_queue_batch_lock")
	now := time.Date(2026, 7, 1, 12, 55, 0, 0, time.UTC)
	request := func(batch string, environmentID string) EnqueueRequest {
		return EnqueueRequest{
			ID:           "qjob_" + batch + "_" + environmentID,
			WorkspaceID:  ws,
			Kind:         KindEnvironmentBuild,
			PartitionKey: FormatEnvironmentPartitionKey(ws, environmentID),
			DedupeKey:    FormatEnvironmentBuildDedupeKey(ws, environmentID, batch),
			PayloadJSON: queuePayload(t, map[string]any{
				"workspace_id": string(ws), "environment_id": environmentID, "generation": batch,
			}),
			Now: now,
		}
	}
	batches := [][]EnqueueRequest{
		{request("forward", "sesn_a"), request("forward", "sesn_b")},
		{request("reverse", "sesn_b"), request("reverse", "sesn_a")},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := make(chan struct{})
	errs := make(chan error, len(batches))
	for _, batch := range batches {
		batch := batch
		go func() {
			<-start
			errs <- store.client.WithWorkspaceTx(ctx, string(ws), "queue.enqueue_batch_test", func(tx *dbconnect.Tx) error {
				_, err := EnqueueBatchTx(ctx, tx, batch)
				return err
			})
		}()
	}
	close(start)
	for range batches {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent EnqueueBatchTx: %v", err)
		}
	}
	for _, environmentID := range []string{"sesn_a", "sesn_b"} {
		rows, err := admin.QueryContext(context.Background(),
			`SELECT queue_partition_sequence
			   FROM queue_jobs
			  WHERE workspace_id = $1 AND partition_key = $2
			  ORDER BY queue_partition_sequence`,
			string(ws), FormatEnvironmentPartitionKey(ws, environmentID),
		)
		if err != nil {
			t.Fatalf("read batch sequences for %s: %v", environmentID, err)
		}
		var sequences []int64
		for rows.Next() {
			var sequence int64
			if err := rows.Scan(&sequence); err != nil {
				_ = rows.Close()
				t.Fatalf("scan batch sequence for %s: %v", environmentID, err)
			}
			sequences = append(sequences, sequence)
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("close batch sequence rows for %s: %v", environmentID, err)
		}
		if !reflect.DeepEqual(sequences, []int64{1, 2}) {
			t.Fatalf("%s sequences = %v; want [1 2]", environmentID, sequences)
		}
	}
}

func TestEnqueueBatchTxPreservesCallerResultAssociationWhileOrderingOutputs(t *testing.T) {
	store, _ := newPostgreSQLQueueStore(t)
	ws := workspace.ID("ws_queue_batch_association")
	now := time.Date(2026, 7, 1, 12, 56, 0, 0, time.UTC)
	request := func(environmentID string) EnqueueRequest {
		return EnqueueRequest{
			ID:           "qjob_" + environmentID,
			WorkspaceID:  ws,
			Kind:         KindEnvironmentBuild,
			PartitionKey: FormatEnvironmentPartitionKey(ws, environmentID),
			DedupeKey:    FormatEnvironmentBuildDedupeKey(ws, environmentID, "1"),
			PayloadJSON: queuePayload(t, map[string]any{
				"workspace_id": string(ws), "environment_id": environmentID, "generation": "1",
			}),
			Now: now,
		}
	}
	requests := []EnqueueRequest{request("env_z"), request("env_a")}
	var jobs []*Job
	if err := store.client.WithWorkspaceTx(context.Background(), string(ws), "queue.enqueue_batch_association_test", func(tx *dbconnect.Tx) error {
		var err error
		jobs, err = EnqueueBatchTx(context.Background(), tx, requests)
		return err
	}); err != nil {
		t.Fatalf("EnqueueBatchTx: %v", err)
	}
	if len(jobs) != 2 || jobs[0].ID != requests[0].ID || jobs[1].ID != requests[1].ID {
		t.Fatalf("batch result IDs = %#v; want caller order %q, %q", jobs, requests[0].ID, requests[1].ID)
	}
}

func TestPostgreSQLStoreActiveDedupeReplay(t *testing.T) {
	store, admin := newPostgreSQLQueueStore(t)
	ctx := context.Background()
	ws := workspace.ID("ws_queue_dedupe")
	now := time.Date(2026, 7, 1, 13, 0, 0, 0, time.UTC)
	environmentID := "env_queue"
	dedupe := FormatEnvironmentBuildDedupeKey(ws, environmentID, "1")

	first := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_prepare_first",
		WorkspaceID:  ws,
		Kind:         KindEnvironmentBuild,
		PartitionKey: FormatEnvironmentPartitionKey(ws, environmentID),
		DedupeKey:    dedupe,
		PayloadJSON:  []byte(`{"workspace_id":"ws_queue_dedupe","environment_id":"env_queue","generation":"1"}`),
		Now:          now,
	})
	replay := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_prepare_replay",
		WorkspaceID:  ws,
		Kind:         KindEnvironmentBuild,
		PartitionKey: FormatEnvironmentPartitionKey(ws, environmentID),
		DedupeKey:    dedupe,
		PayloadJSON:  []byte(`{"workspace_id":"ws_queue_dedupe","environment_id":"env_queue","generation":"1"}`),
		Now:          now.Add(time.Second),
	})
	if replay.ID != first.ID {
		t.Fatalf("dedupe replay ID = %s; want existing %s", replay.ID, first.ID)
	}
	if got := countQueueJobsByDedupe(t, admin, ws, dedupe); got != 1 {
		t.Fatalf("active dedupe job count = %d; want 1", got)
	}
	leased, err := store.Lease(ctx, LeaseRequest{
		WorkspaceID:   ws,
		Kinds:         []string{KindEnvironmentBuild},
		LeaseOwner:    "sandbox-service",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	assertLeasedIDs(t, leased, []string{first.ID})
	if ok, err := store.Ack(ctx, AckRequest{WorkspaceID: ws, JobID: first.ID, LeaseToken: leased[0].LeaseToken, Now: now.Add(4 * time.Second)}); err != nil || !ok {
		t.Fatalf("Ack = (%v,%v); want true,nil", ok, err)
	}
	second := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_prepare_second",
		WorkspaceID:  ws,
		Kind:         KindEnvironmentBuild,
		PartitionKey: FormatEnvironmentPartitionKey(ws, environmentID),
		DedupeKey:    dedupe,
		PayloadJSON:  []byte(`{"workspace_id":"ws_queue_dedupe","environment_id":"env_queue","generation":"1"}`),
		Now:          now.Add(5 * time.Second),
	})
	if second.ID == first.ID {
		t.Fatal("dedupe replay reused acknowledged job; active dedupe must exclude terminal jobs")
	}
}

func TestPostgreSQLStoreCancelInterruptFenceOnlyCancelsPendingOlderSameThreadMessages(t *testing.T) {
	store, admin := newPostgreSQLQueueStore(t)
	ctx := context.Background()
	ws := workspace.ID("ws_queue_cancel")
	sessionID := "sesn_cancel"
	threadID := "thrd_cancel"
	otherThreadID := "thrd_other"
	now := time.Date(2026, 7, 1, 13, 30, 0, 0, time.UTC)
	partition := FormatSessionPartitionKey(ws, sessionID)

	leasedOld := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_cancel_lease",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: partition,
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, sessionID, "input_leased_old"),
		PayloadJSON:  runtimeInputPayload(t, ws, sessionID, threadID, "input_leased_old", "messages", 1, 1),
		Now:          now,
	})
	pendingOld := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_cancel_pend",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: partition,
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, sessionID, "input_pending_old"),
		PayloadJSON:  runtimeInputPayload(t, ws, sessionID, threadID, "input_pending_old", "messages", 2, 4),
		Now:          now.Add(time.Millisecond),
	})
	equalFence := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_cancel_equal",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: partition,
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, sessionID, "input_equal_fence"),
		PayloadJSON:  runtimeInputPayload(t, ws, sessionID, threadID, "input_equal_fence", "messages", 5, 5),
		Now:          now.Add(2 * time.Millisecond),
	})
	otherThread := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_cancel_other",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: partition,
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, sessionID, "input_other_thread"),
		PayloadJSON:  runtimeInputPayload(t, ws, sessionID, otherThreadID, "input_other_thread", "messages", 2, 4),
		Now:          now.Add(3 * time.Millisecond),
	})
	toolConfirmation := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_cancel_tool",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: partition,
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, sessionID, "input_tool_confirmation"),
		PayloadJSON:  runtimeInputPayload(t, ws, sessionID, threadID, "input_tool_confirmation", "tool_confirmation", 2, 4),
		Now:          now.Add(4 * time.Millisecond),
	})
	leased := mustLeaseOne(t, store, LeaseRequest{
		WorkspaceID:   ws,
		Kinds:         []string{KindRuntimeInput},
		LeaseOwner:    "bridge",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now.Add(time.Second),
	})
	if leased.ID != leasedOld.ID {
		t.Fatalf("leased old job = %s; want %s", leased.ID, leasedOld.ID)
	}

	cancelled, err := store.Cancel(ctx, CancelRequest{
		WorkspaceID:            ws,
		SessionID:              sessionID,
		SessionThreadID:        threadID,
		InterruptFenceSequence: 5,
		Now:                    now.Add(2 * time.Second),
	})
	if err != nil || cancelled != 1 {
		t.Fatalf("Cancel interrupt fence = (%d,%v); want 1,nil", cancelled, err)
	}
	if got := queueJobStatus(t, admin, ws, leasedOld.ID); got != StatusLeased {
		t.Fatalf("leased old status = %s; want leased", got)
	}
	if got := queueJobStatus(t, admin, ws, pendingOld.ID); got != StatusCancelled {
		t.Fatalf("pending old status = %s; want cancelled", got)
	}
	for _, job := range []*Job{equalFence, otherThread, toolConfirmation} {
		if got := queueJobStatus(t, admin, ws, job.ID); got != StatusPending {
			t.Fatalf("%s status = %s; want pending", job.ID, got)
		}
	}
	if _, err := store.Cancel(ctx, CancelRequest{WorkspaceID: ws, SessionID: sessionID, SessionThreadID: otherThreadID, InterruptFenceSequence: 0, Now: now}); !IsValidationError(err) {
		t.Fatalf("Cancel invalid fence err = %v; want validation error", err)
	}
}

func TestPostgreSQLStoreRejectsNonCanonicalQueueShape(t *testing.T) {
	store, _ := newPostgreSQLQueueStore(t)
	ws := workspace.ID("ws_queue_shape")
	now := time.Date(2026, 7, 1, 13, 45, 0, 0, time.UTC)
	_, err := store.Enqueue(context.Background(), EnqueueRequest{
		ID:           "qjob_bad_shape",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: FormatSessionPartitionKey(ws, "wrong_session"),
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, "sesn_shape", "input_shape"),
		PayloadJSON:  runtimeInputPayload(t, ws, "sesn_shape", "thrd_shape", "input_shape", "messages", 1, 1),
		Now:          now,
	})
	if !IsValidationError(err) {
		t.Fatalf("non-canonical partition err = %v; want validation error", err)
	}

	_, err = store.Enqueue(context.Background(), EnqueueRequest{
		ID:           "qjob_bad_payload",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: FormatSessionPartitionKey(ws, "sesn_shape"),
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, "sesn_shape", "input_secret"),
		PayloadJSON: queuePayload(t, map[string]any{
			"workspace_id":        string(ws),
			"session_id":          "sesn_shape",
			"session_thread_id":   "thrd_shape",
			"runtime_input_id":    "input_secret",
			"event_ids":           []string{"ev_secret"},
			"sequence_from":       1,
			"sequence_to":         1,
			"input_kind":          "messages",
			"copied_user_text":    "must stay in session_events",
			"provider_credential": "must never enter queue payload",
		}),
		Now: now,
	})
	if !IsValidationError(err) {
		t.Fatalf("extra payload field err = %v; want validation error", err)
	}
}

func TestPostgreSQLStoreAcceptsRuntimeMCPManifestUpdateCanonicalShape(t *testing.T) {
	store, _ := newPostgreSQLQueueStore(t)
	ws := workspace.ID("ws_queue_mcp_manifest")
	sessionID := "sesn_manifest"
	now := time.Date(2026, 7, 1, 13, 48, 0, 0, time.UTC)
	payload := queuePayload(t, map[string]any{
		"workspace_id":        string(ws),
		"session_id":          sessionID,
		"mcp_server_name":     "github",
		"manifest_generation": 1,
	})

	job := mustEnqueue(t, store, EnqueueRequest{
		ID:             "qjob_mcp_manifest",
		WorkspaceID:    ws,
		Kind:           KindRuntimeConfigUpdate,
		PartitionKey:   FormatSessionPartitionKey(ws, sessionID),
		DedupeKey:      FormatRuntimeMCPManifestUpdateDedupeKey(ws, sessionID, "github", "1"),
		PayloadVersion: 2,
		PayloadJSON:    payload,
		Now:            now,
	})
	leased := mustLeaseOne(t, store, LeaseRequest{
		WorkspaceID:   ws,
		Kinds:         []string{KindRuntimeConfigUpdate},
		LeaseOwner:    "bridge",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now.Add(time.Second),
	})
	if leased.ID != job.ID {
		t.Fatalf("leased MCP manifest job = %s; want %s", leased.ID, job.ID)
	}
	_, err := store.Enqueue(context.Background(), EnqueueRequest{
		ID:           "qjob_mcp_bad_dedupe",
		WorkspaceID:  ws,
		Kind:         KindRuntimeConfigUpdate,
		PartitionKey: FormatSessionPartitionKey(ws, sessionID),
		DedupeKey:    FormatRuntimeConfigUpdateDedupeKey(ws, sessionID, "7"),
		PayloadJSON:  payload,
		Now:          now.Add(2 * time.Second),
	})
	if !IsValidationError(err) {
		t.Fatalf("MCP manifest bad dedupe err = %v; want validation error", err)
	}

	_, err = store.Enqueue(context.Background(), EnqueueRequest{
		ID:           "qjob_config_approve",
		WorkspaceID:  ws,
		Kind:         KindRuntimeConfigUpdate,
		PartitionKey: FormatSessionPartitionKey(ws, sessionID),
		DedupeKey:    FormatRuntimeConfigUpdateDedupeKey(ws, sessionID, "8"),
		PayloadJSON:  []byte(`{"workspace_id":"ws_queue_mcp_manifest","session_id":"sesn_manifest","config_generation":8,"approval_mode":"full_access"}`),
		Now:          now.Add(3 * time.Second),
	})
	if !IsValidationError(err) {
		t.Fatalf("runtime config content-bearing payload err = %v; want refs-only validation error", err)
	}
}

func TestPostgreSQLStoreAcceptsTaskNotificationRuntimeInputWithoutPublicEventFence(t *testing.T) {
	store, _ := newPostgreSQLQueueStore(t)
	ws := workspace.ID("ws_queue_task_notification")
	now := time.Date(2026, 7, 1, 13, 50, 0, 0, time.UTC)
	runtimeInputID := "task_notification:task_queue_notify"

	job := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_task_notify",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: FormatSessionPartitionKey(ws, "sesn_task_notify"),
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, "sesn_task_notify", runtimeInputID),
		PayloadJSON: queuePayload(t, map[string]any{
			"workspace_id":      string(ws),
			"session_id":        "sesn_task_notify",
			"session_thread_id": "thrd_task_notify",
			"runtime_input_id":  runtimeInputID,
			"event_ids":         []string{},
			"input_kind":        "task_notification",
		}),
		Now: now,
	})
	if job.ID != "qjob_task_notify" {
		t.Fatalf("task notification job id = %s", job.ID)
	}
}

func TestPostgreSQLStoreRetryDeadLetterAndReclaimExpiredLeases(t *testing.T) {
	store, admin := newPostgreSQLQueueStore(t)
	ctx := context.Background()
	ws := workspace.ID("ws_queue_retry")
	retrySessionID := "sesn_retry"
	reclaimSessionID := "sesn_reclaim"
	now := time.Date(2026, 7, 1, 14, 0, 0, 0, time.UTC)

	job := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_retry",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: FormatSessionPartitionKey(ws, retrySessionID),
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, retrySessionID, "input_1"),
		PayloadJSON:  runtimeInputPayload(t, ws, retrySessionID, "thrd_retry", "input_1", "messages", 1, 1),
		MaxAttempts:  2,
		Now:          now,
	})
	firstLease := mustLeaseOne(t, store, LeaseRequest{WorkspaceID: ws, Kinds: []string{KindRuntimeInput}, LeaseOwner: "bridge", MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(time.Second)})
	if ok, err := store.Retry(ctx, RetryRequest{WorkspaceID: ws, JobID: job.ID, LeaseToken: firstLease.LeaseToken, ErrorKind: "runtime_unavailable", Now: now.Add(2 * time.Second)}); err != nil || !ok {
		t.Fatalf("Retry first lease = (%v,%v); want true,nil", ok, err)
	}
	if got := queueJobPartitionSequence(t, admin, ws, job.ID); got != job.QueuePartitionSequence {
		t.Fatalf("partition sequence after Retry = %d; want original %d", got, job.QueuePartitionSequence)
	}
	if ok, err := store.Ack(ctx, AckRequest{WorkspaceID: ws, JobID: job.ID, LeaseToken: firstLease.LeaseToken, Now: now.Add(3 * time.Second)}); err != nil || ok {
		t.Fatalf("stale Ack after retry = (%v,%v); want false,nil", ok, err)
	}
	secondLease := mustLeaseOne(t, store, LeaseRequest{WorkspaceID: ws, Kinds: []string{KindRuntimeInput}, LeaseOwner: "bridge", MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(8 * time.Second)})
	if ok, err := store.Retry(ctx, RetryRequest{WorkspaceID: ws, JobID: job.ID, LeaseToken: secondLease.LeaseToken, ErrorKind: "invariant_failed", Now: now.Add(9 * time.Second)}); err != nil || !ok {
		t.Fatalf("Retry max attempt = (%v,%v); want true,nil", ok, err)
	}
	if got := queueJobStatus(t, admin, ws, job.ID); got != StatusDeadLettered {
		t.Fatalf("max-attempt retry status = %s; want dead_lettered", got)
	}

	expiring := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_reclaim",
		WorkspaceID:  ws,
		Kind:         KindCleanupSession,
		PartitionKey: FormatSessionPartitionKey(ws, reclaimSessionID),
		DedupeKey:    FormatCleanupSessionDedupeKey(ws, reclaimSessionID, "cleanup_1"),
		PayloadJSON:  []byte(`{"workspace_id":"ws_queue_retry","session_id":"sesn_reclaim","cleanup_job_id":"cleanup_1"}`),
		MaxAttempts:  2,
		Now:          now,
	})
	firstExpiredLease := mustLeaseOne(t, store, LeaseRequest{WorkspaceID: ws, Kinds: []string{KindCleanupSession}, LeaseOwner: "bridge-a", MaxJobs: 1, LeaseDuration: time.Second, Now: now.Add(10 * time.Second)})
	expireQueueJobLease(t, admin, ws, expiring.ID)
	if reclaimed, err := store.ReclaimExpiredLeases(ctx, ReclaimExpiredLeasesRequest{WorkspaceID: ws, Kind: KindCleanupSession}); err != nil || reclaimed != 1 {
		t.Fatalf("ReclaimExpiredLeases first = (%d,%v); want 1,nil", reclaimed, err)
	}
	if got := queueJobStatus(t, admin, ws, expiring.ID); got != StatusPending {
		t.Fatalf("first expired reclaim status = %s; want pending", got)
	}
	if updated, err := store.Ack(ctx, AckRequest{WorkspaceID: ws, JobID: expiring.ID, LeaseToken: firstExpiredLease.LeaseToken}); err != nil || updated {
		t.Fatalf("Ack reclaimed first lease = (%v,%v); want stale token rejection", updated, err)
	}
	if got := queueJobPartitionSequence(t, admin, ws, expiring.ID); got != expiring.QueuePartitionSequence {
		t.Fatalf("partition sequence after reclaim = %d; want original %d", got, expiring.QueuePartitionSequence)
	}
	secondExpiredLease := mustLeaseOne(t, store, LeaseRequest{WorkspaceID: ws, Kinds: []string{KindCleanupSession}, LeaseOwner: "bridge-b", MaxJobs: 1, LeaseDuration: time.Second, Now: time.Now().UTC().Add(time.Second)})
	expireQueueJobLease(t, admin, ws, expiring.ID)
	if reclaimed, err := store.ReclaimExpiredLeases(ctx, ReclaimExpiredLeasesRequest{WorkspaceID: ws, Kind: KindCleanupSession}); err != nil || reclaimed != 1 {
		t.Fatalf("ReclaimExpiredLeases second = (%d,%v); want 1,nil", reclaimed, err)
	}
	if got := queueJobStatus(t, admin, ws, expiring.ID); got != StatusPending {
		t.Fatalf("second expired reclaim status = %s; want pending because lease expiration alone never dead-letters", got)
	}
	if updated, err := store.Ack(ctx, AckRequest{WorkspaceID: ws, JobID: expiring.ID, LeaseToken: secondExpiredLease.LeaseToken}); err != nil || updated {
		t.Fatalf("Ack reclaimed second lease = (%v,%v); want stale token rejection", updated, err)
	}
	currentLease := mustLeaseOne(t, store, LeaseRequest{
		WorkspaceID: ws, Kinds: []string{KindCleanupSession}, LeaseOwner: "bridge-c",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: time.Now().UTC().Add(2 * time.Second),
	})
	if currentLease.AttemptCount != 3 {
		t.Fatalf("current lease attempt count = %d; want three acquisitions independent of two reclaims", currentLease.AttemptCount)
	}
	if updated, err := store.Ack(ctx, AckRequest{WorkspaceID: ws, JobID: expiring.ID, LeaseToken: currentLease.LeaseToken}); err != nil || !updated {
		t.Fatalf("Ack current lease = (%v,%v); want eventual settlement", updated, err)
	}
}

func TestPostgreSQLStoreDeferRejectsOtherJobKinds(t *testing.T) {
	t.Run("other job kinds are rejected without changing the lease", func(t *testing.T) {
		store, admin := newPostgreSQLQueueStore(t)
		ctx := context.Background()
		ws := workspace.ID("ws_queue_defer_kind")
		now := time.Date(2026, 7, 18, 13, 30, 0, 0, time.UTC)
		job := mustEnqueue(t, store, EnqueueRequest{
			ID:           "qjob_defer_runtime",
			WorkspaceID:  ws,
			Kind:         KindRuntimeInput,
			PartitionKey: FormatSessionPartitionKey(ws, "sesn_defer_kind"),
			DedupeKey:    FormatRuntimeInputDedupeKey(ws, "sesn_defer_kind", "rin_defer_kind"),
			PayloadJSON:  runtimeInputPayload(t, ws, "sesn_defer_kind", "thrd_defer_kind", "rin_defer_kind", "messages", 1, 1),
			Now:          now,
		})
		leased := mustLeaseOne(t, store, LeaseRequest{
			WorkspaceID: ws, Kinds: []string{KindRuntimeInput}, LeaseOwner: "bridge",
			MaxJobs: 1, LeaseDuration: time.Minute, Now: now,
		})
		ok, err := store.Defer(ctx, DeferRequest{
			WorkspaceID: ws, JobID: job.ID, LeaseToken: leased.LeaseToken, Now: now.Add(time.Second),
		})
		var validation *ValidationError
		if ok || !errors.As(err, &validation) {
			t.Fatalf("Defer runtime_input = (%v,%T %v); want false ValidationError", ok, err, err)
		}
		if got := queueJobStatus(t, admin, ws, job.ID); got != StatusLeased {
			t.Fatalf("runtime_input status after rejected Defer = %s; want leased", got)
		}
	})
}

func TestPostgreSQLStoreDeferCanonicalRuntimeConfigUsesScopedCounter(t *testing.T) {
	tests := []struct {
		name      string
		jobID     string
		sessionID string
		dedupeKey func(workspace.ID, string) string
		payload   func(workspace.ID, string) []byte
	}{
		{
			name:      "sdk config generation",
			jobID:     "qjob_dfr_sdk",
			sessionID: "sesn_defer_sdk_config",
			dedupeKey: func(ws workspace.ID, sessionID string) string {
				return FormatRuntimeConfigUpdateDedupeKey(ws, sessionID, "7")
			},
			payload: func(ws workspace.ID, sessionID string) []byte {
				return queuePayload(t, map[string]any{
					"workspace_id": string(ws), "session_id": sessionID, "config_generation": 7,
				})
			},
		},
		{
			name:      "mcp manifest generation",
			jobID:     "qjob_dfr_mcp",
			sessionID: "sesn_defer_mcp_config",
			dedupeKey: func(ws workspace.ID, sessionID string) string {
				return FormatRuntimeMCPManifestUpdateDedupeKey(ws, sessionID, "github", "3")
			},
			payload: func(ws workspace.ID, sessionID string) []byte {
				return queuePayload(t, map[string]any{
					"workspace_id": string(ws), "session_id": sessionID,
					"mcp_server_name": "github", "manifest_generation": 3,
				})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, admin := newPostgreSQLQueueStore(t)
			store.retryPolicy.RandomInt64 = func(bound int64) int64 { return bound - 1 }
			ctx := context.Background()
			ws := workspace.ID("ws_" + test.sessionID)
			now := time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)
			job := mustEnqueue(t, store, EnqueueRequest{
				ID:           test.jobID,
				WorkspaceID:  ws,
				Kind:         KindRuntimeConfigUpdate,
				PartitionKey: FormatSessionPartitionKey(ws, test.sessionID),
				DedupeKey:    test.dedupeKey(ws, test.sessionID),
				PayloadJSON:  test.payload(ws, test.sessionID),
				MaxAttempts:  1,
				Now:          now,
			})
			originalSequence := job.QueuePartitionSequence
			for cycle, wantDeferCount := range []int{1, 2} {
				leaseAt := now.Add(time.Duration(cycle*2) * time.Second)
				leased := mustLeaseOne(t, store, LeaseRequest{
					WorkspaceID: ws, Kinds: []string{KindRuntimeConfigUpdate}, LeaseOwner: "bridge",
					MaxJobs: 1, LeaseDuration: time.Minute, Now: leaseAt,
				})
				deferAt := leaseAt.Add(time.Second)
				if ok, err := store.Defer(ctx, DeferRequest{
					WorkspaceID: ws, JobID: job.ID, LeaseToken: leased.LeaseToken, Now: deferAt,
				}); err != nil || !ok {
					t.Fatalf("Defer cycle %d = (%v,%v); want true,nil", cycle+1, ok, err)
				}
				var (
					status       string
					attemptCount int
					deferCount   int
					sequence     int64
					availableAt  time.Time
				)
				if err := admin.QueryRowContext(ctx,
					`SELECT status, attempt_count, defer_count, queue_partition_sequence, available_at
					   FROM queue_jobs
					  WHERE workspace_id = $1 AND id = $2`,
					string(ws), job.ID,
				).Scan(&status, &attemptCount, &deferCount, &sequence, &availableAt); err != nil {
					t.Fatalf("read config defer cycle %d: %v", cycle+1, err)
				}
				wantAvailableAt := deferAt.Add(time.Duration(1<<cycle) * time.Second)
				if status != StatusPending || attemptCount != 0 || deferCount != wantDeferCount ||
					sequence != originalSequence || !availableAt.Equal(wantAvailableAt) {
					t.Fatalf("config defer cycle %d = %s attempt=%d defer=%d sequence=%d available=%s; want pending/0/%d/%d/%s",
						cycle+1, status, attemptCount, deferCount, sequence, availableAt, wantDeferCount, originalSequence, wantAvailableAt)
				}
			}
		})
	}
}

func TestPostgreSQLStoreDeferRejectsMalformedRuntimeConfigPayload(t *testing.T) {
	store, admin := newPostgreSQLQueueStore(t)
	ctx := context.Background()
	ws := workspace.ID("ws_queue_defer_malformed")
	sessionID := "sesn_defer_malformed"
	now := time.Date(2026, 7, 18, 14, 30, 0, 0, time.UTC)
	if _, err := admin.ExecContext(ctx,
		`INSERT INTO queue_partition_counters (
			workspace_id, partition_key, last_sequence, created_at, updated_at
		) VALUES ($1, $2, 1, $3, $3)`,
		string(ws), FormatSessionPartitionKey(ws, sessionID), now,
	); err != nil {
		t.Fatalf("seed partition counter: %v", err)
	}
	if _, err := admin.ExecContext(ctx,
		`INSERT INTO queue_jobs (
			id, workspace_id, kind, partition_key, queue_partition_sequence, dedupe_key,
			payload_version, status, payload_json, priority, attempt_count, max_attempts,
			defer_count, available_at, created_at, updated_at
		) VALUES ($1, $2, 'runtime_config_update', $3, 1, $4, 2, 'pending', $5, 0, 0, 1, 0, $6, $6, $6)`,
		"qjob_defer_malformed",
		string(ws),
		FormatSessionPartitionKey(ws, sessionID),
		FormatRuntimeConfigUpdateDedupeKey(ws, sessionID, "1"),
		`{"workspace_id":"ws_queue_defer_malformed","session_id":"sesn_defer_malformed","config_generation":1,"mcp_server_name":"github"}`,
		now,
	); err != nil {
		t.Fatalf("seed malformed runtime config job: %v", err)
	}
	leased := mustLeaseOne(t, store, LeaseRequest{
		WorkspaceID: ws, Kinds: []string{KindRuntimeConfigUpdate}, LeaseOwner: "bridge",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: now,
	})
	ok, err := store.Defer(ctx, DeferRequest{
		WorkspaceID: ws, JobID: leased.ID, LeaseToken: leased.LeaseToken, Now: now.Add(time.Second),
	})
	if ok || !IsValidationError(err) {
		t.Fatalf("Defer malformed runtime config = (%v,%T %v); want false ValidationError", ok, err, err)
	}
	if got := queueJobStatus(t, admin, ws, leased.ID); got != StatusLeased {
		t.Fatalf("malformed runtime config status after rejected Defer = %s; want leased", got)
	}
}
func TestQueueRetryDelayUsesFullJitterAndExponentialCap(t *testing.T) {
	var bounds []int64
	policy := normalizeRetryPolicy(RetryPolicy{
		BaseDelay:   time.Second,
		MaxDelay:    5 * time.Second,
		MaxAttempts: 7,
		RandomInt64: func(bound int64) int64 {
			bounds = append(bounds, bound)
			return bound - 1
		},
	})

	for attempt := 1; attempt <= 5; attempt++ {
		_ = queueRetryDelay(policy, attempt)
	}

	want := []int64{
		int64(time.Second) + 1,
		int64(2*time.Second) + 1,
		int64(4*time.Second) + 1,
		int64(5*time.Second) + 1,
		int64(5*time.Second) + 1,
	}
	if !reflect.DeepEqual(bounds, want) {
		t.Fatalf("jitter bounds = %v; want %v", bounds, want)
	}
}

func TestPostgreSQLStoreProjectsUnsetMaxAttemptsAndUsesItForRetry(t *testing.T) {
	store, admin := newPostgreSQLQueueStore(t)
	store.retryPolicy.MaxAttempts = 2
	store.retryPolicy.RandomInt64 = func(int64) int64 { return 0 }
	ctx := context.Background()
	ws := workspace.ID("ws_queue_default_attempts")
	now := time.Date(2026, 7, 1, 15, 0, 0, 0, time.UTC)
	job := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_default_attempts",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: FormatSessionPartitionKey(ws, "sesn_default_attempts"),
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, "sesn_default_attempts", "rin_default_attempts"),
		PayloadJSON:  runtimeInputPayload(t, ws, "sesn_default_attempts", "thrd_default_attempts", "rin_default_attempts", "messages", 1, 1),
		MaxAttempts:  0,
		Now:          now,
	})
	var storedMaxAttempts int
	if err := admin.QueryRow(`SELECT max_attempts FROM queue_jobs WHERE workspace_id = $1 AND id = $2`, string(ws), job.ID).Scan(&storedMaxAttempts); err != nil {
		t.Fatalf("read durable max_attempts: %v", err)
	}
	if storedMaxAttempts != 0 {
		t.Fatalf("durable max_attempts = %d; want unset 0", storedMaxAttempts)
	}
	first := mustLeaseOne(t, store, LeaseRequest{WorkspaceID: ws, Kinds: []string{KindRuntimeInput}, LeaseOwner: "bridge", MaxJobs: 1, LeaseDuration: time.Minute, Now: now})
	if first.MaxAttempts != 2 {
		t.Fatalf("leased max_attempts = %d; want effective default 2", first.MaxAttempts)
	}
	if ok, err := store.Retry(ctx, RetryRequest{WorkspaceID: ws, JobID: job.ID, LeaseToken: first.LeaseToken, Now: now}); err != nil || !ok {
		t.Fatalf("first Retry = (%v,%v); want true,nil", ok, err)
	}
	second := mustLeaseOne(t, store, LeaseRequest{WorkspaceID: ws, Kinds: []string{KindRuntimeInput}, LeaseOwner: "bridge", MaxJobs: 1, LeaseDuration: time.Minute, Now: now})
	if ok, err := store.Retry(ctx, RetryRequest{WorkspaceID: ws, JobID: job.ID, LeaseToken: second.LeaseToken, Now: now}); err != nil || !ok {
		t.Fatalf("second Retry = (%v,%v); want true,nil", ok, err)
	}
	if got := queueJobStatus(t, admin, ws, job.ID); got != StatusDeadLettered {
		t.Fatalf("effective-default retry status = %s; want dead_lettered", got)
	}
}

func TestPostgreSQLStoreMetricsSummarizesQueueState(t *testing.T) {
	store, _ := newPostgreSQLQueueStore(t)
	ctx := context.Background()
	ws := workspace.ID("ws_queue_metrics")
	now := time.Date(2026, 7, 1, 16, 0, 0, 0, time.UTC)

	mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_metric_old",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: FormatSessionPartitionKey(ws, "sesn_metrics_old"),
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, "sesn_metrics_old", "input_old"),
		PayloadJSON:  runtimeInputPayload(t, ws, "sesn_metrics_old", "thrd_metrics_old", "input_old", "messages", 1, 1),
		AvailableAt:  now.Add(-30 * time.Second),
		Now:          now.Add(-30 * time.Second),
	})
	retryJob := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_metrics_retry",
		WorkspaceID:  ws,
		Kind:         KindRuntimeInput,
		PartitionKey: FormatSessionPartitionKey(ws, "sesn_metrics_retry"),
		DedupeKey:    FormatRuntimeInputDedupeKey(ws, "sesn_metrics_retry", "input_retry"),
		PayloadJSON:  runtimeInputPayload(t, ws, "sesn_metrics_retry", "thrd_metrics_retry", "input_retry", "messages", 1, 1),
		MaxAttempts:  3,
		Priority:     100,
		Now:          now.Add(-20 * time.Second),
	})
	retryLease := mustLeaseOne(t, store, LeaseRequest{WorkspaceID: ws, Kinds: []string{KindRuntimeInput}, LeaseOwner: "bridge", MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(-19 * time.Second)})
	if retryLease.ID != retryJob.ID {
		t.Fatalf("retry lease id = %s; want %s", retryLease.ID, retryJob.ID)
	}
	if ok, err := store.Retry(ctx, RetryRequest{WorkspaceID: ws, JobID: retryJob.ID, LeaseToken: retryLease.LeaseToken, ErrorKind: "runtime_unavailable", Now: now.Add(-18 * time.Second)}); err != nil || !ok {
		t.Fatalf("Retry metrics job = (%v,%v); want true,nil", ok, err)
	}
	leasedCleanup := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_metric_cleanup",
		WorkspaceID:  ws,
		Kind:         KindCleanupSession,
		PartitionKey: FormatSessionPartitionKey(ws, "sesn_metrics_cleanup"),
		DedupeKey:    FormatCleanupSessionDedupeKey(ws, "sesn_metrics_cleanup", "cleanup_1"),
		PayloadJSON:  []byte(`{"workspace_id":"ws_queue_metrics","session_id":"sesn_metrics_cleanup","cleanup_job_id":"cleanup_1"}`),
		Now:          now.Add(-10 * time.Second),
	})
	cleanupLease := mustLeaseOne(t, store, LeaseRequest{WorkspaceID: ws, Kinds: []string{KindCleanupSession}, LeaseOwner: "bridge", MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(-9 * time.Second)})
	if cleanupLease.ID != leasedCleanup.ID {
		t.Fatalf("cleanup lease id = %s; want %s", cleanupLease.ID, leasedCleanup.ID)
	}
	deadJob := mustEnqueue(t, store, EnqueueRequest{
		ID:           "qjob_metrics_dead",
		WorkspaceID:  ws,
		Kind:         KindEnvironmentBuild,
		PartitionKey: FormatEnvironmentPartitionKey(ws, "env_metrics_dead"),
		DedupeKey:    FormatEnvironmentBuildDedupeKey(ws, "env_metrics_dead", "1"),
		PayloadJSON:  []byte(`{"workspace_id":"ws_queue_metrics","environment_id":"env_metrics_dead","generation":"1"}`),
		MaxAttempts:  1,
		Now:          now.Add(-8 * time.Second),
	})
	deadLease := mustLeaseOne(t, store, LeaseRequest{WorkspaceID: ws, Kinds: []string{KindEnvironmentBuild}, LeaseOwner: "sandbox", MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(-7 * time.Second)})
	if deadLease.ID != deadJob.ID {
		t.Fatalf("dead lease id = %s; want %s", deadLease.ID, deadJob.ID)
	}
	if ok, err := store.Retry(ctx, RetryRequest{WorkspaceID: ws, JobID: deadJob.ID, LeaseToken: deadLease.LeaseToken, ErrorKind: "prepare_failed", Now: now.Add(-6 * time.Second)}); err != nil || !ok {
		t.Fatalf("Retry dead metrics job = (%v,%v); want true,nil", ok, err)
	}

	snapshots, err := store.Metrics(ctx, now)
	if err != nil {
		t.Fatalf("Metrics: %v", err)
	}
	byKind := map[string]MetricsSnapshot{}
	for _, snapshot := range snapshots {
		byKind[snapshot.Kind] = snapshot
	}
	runtimeInput := byKind[KindRuntimeInput]
	if runtimeInput.PendingJobs != 2 || runtimeInput.RetryPendingJobs != 1 || runtimeInput.LeasedJobs != 0 || runtimeInput.DeadLetteredJobs != 0 {
		t.Fatalf("runtime_input metrics = %+v", runtimeInput)
	}
	if runtimeInput.ReadyLagSeconds < 29.9 || runtimeInput.ReadyLagSeconds > 30.1 {
		t.Fatalf("runtime_input ready lag = %f; want about 30s", runtimeInput.ReadyLagSeconds)
	}
	cleanup := byKind[KindCleanupSession]
	if cleanup.LeasedJobs != 1 || cleanup.PendingJobs != 0 {
		t.Fatalf("cleanup metrics = %+v", cleanup)
	}
	environmentBuild := byKind[KindEnvironmentBuild]
	if environmentBuild.DeadLetteredJobs != 1 || environmentBuild.PendingJobs != 0 {
		t.Fatalf("environment_build metrics = %+v", environmentBuild)
	}
}

func TestPostgreSQLStoreReclaimsExpiredLeasesAcrossWorkspacesWithQueueMaintenancePolicy(t *testing.T) {
	store, admin := newPostgreSQLQueueStore(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 1, 15, 0, 0, 0, time.UTC)
	for _, ws := range []workspace.ID{"ws_queue_maint_a", "ws_queue_maint_b"} {
		mustEnqueue(t, store, EnqueueRequest{
			ID:           "qjob_" + string(ws),
			WorkspaceID:  ws,
			Kind:         KindRuntimeInput,
			PartitionKey: FormatSessionPartitionKey(ws, "sesn_maint"),
			DedupeKey:    FormatRuntimeInputDedupeKey(ws, "sesn_maint", "input_1"),
			PayloadJSON:  runtimeInputPayload(t, ws, "sesn_maint", "thrd_maint", "input_1", "messages", 1, 1),
			Now:          now,
		})
		mustLeaseOne(t, store, LeaseRequest{
			WorkspaceID:   ws,
			Kinds:         []string{KindRuntimeInput},
			LeaseOwner:    "bridge",
			MaxJobs:       1,
			LeaseDuration: time.Second,
			Now:           now.Add(time.Second),
		})
		expireQueueJobLease(t, admin, ws, "qjob_"+string(ws))
	}
	reclaimed, err := store.ReclaimExpiredLeases(ctx, ReclaimExpiredLeasesRequest{
		Limit: 10,
	})
	if err != nil || reclaimed != 2 {
		t.Fatalf("global ReclaimExpiredLeases = (%d,%v); want 2,nil", reclaimed, err)
	}
	for _, ws := range []workspace.ID{"ws_queue_maint_a", "ws_queue_maint_b"} {
		if got := queueJobStatus(t, admin, ws, "qjob_"+string(ws)); got != StatusPending {
			t.Fatalf("%s reclaimed status = %s; want pending", ws, got)
		}
	}
}

func TestTargetedCancelTxRequiresExactPendingIdentity(t *testing.T) {
	store, admin := newPostgreSQLQueueStore(t)
	ctx := context.Background()
	ws := workspace.ID("ws_targeted_cancel")
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	pending := mustEnqueue(t, store, sandboxToolExecuteRequest(t, ws, "sesn_targeted_cancel", "thrd_targeted_cancel", "sevt_pending_cancel", "qjob_pending_cancel", 2, now))

	cancelled := false
	if err := store.client.WithWorkspaceTx(ctx, string(ws), "queue.test_targeted_cancel", func(tx *dbconnect.Tx) error {
		var err error
		cancelled, err = CancelTx(ctx, tx, TargetedCancelRequest{
			WorkspaceID:  ws,
			JobID:        pending.ID,
			Kind:         pending.Kind,
			PartitionKey: pending.PartitionKey,
			DedupeKey:    pending.DedupeKey,
			Now:          now.Add(time.Second),
		})
		return err
	}); err != nil || !cancelled {
		t.Fatalf("CancelTx pending = (%v,%v); want true,nil", cancelled, err)
	}
	if got := queueJobStatus(t, admin, ws, pending.ID); got != StatusCancelled {
		t.Fatalf("pending status after CancelTx = %s; want cancelled", got)
	}

	leasedJob := mustEnqueue(t, store, sandboxToolExecuteRequest(t, ws, "sesn_targeted_cancel", "thrd_targeted_cancel", "sevt_leased_cancel", "qjob_leased_cancel", 2, now.Add(2*time.Second)))
	leased := mustLeaseOne(t, store, LeaseRequest{
		WorkspaceID: ws, Kinds: []string{KindSandboxToolExecute}, LeaseOwner: "sandbox",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(3 * time.Second),
	})
	if leased.ID != leasedJob.ID {
		t.Fatalf("leased id = %s; want %s", leased.ID, leasedJob.ID)
	}
	cancelled = true
	if err := store.client.WithWorkspaceTx(ctx, string(ws), "queue.test_targeted_cancel_leased", func(tx *dbconnect.Tx) error {
		var err error
		cancelled, err = CancelTx(ctx, tx, TargetedCancelRequest{
			WorkspaceID:  ws,
			JobID:        leasedJob.ID,
			Kind:         leasedJob.Kind,
			PartitionKey: leasedJob.PartitionKey,
			DedupeKey:    leasedJob.DedupeKey,
			Now:          now.Add(4 * time.Second),
		})
		return err
	}); err != nil || cancelled {
		t.Fatalf("CancelTx leased = (%v,%v); want false,nil", cancelled, err)
	}

	if err := store.client.WithWorkspaceTx(ctx, string(ws), "queue.test_targeted_cancel_mismatch", func(tx *dbconnect.Tx) error {
		_, err := CancelTx(ctx, tx, TargetedCancelRequest{
			WorkspaceID:  ws,
			JobID:        leasedJob.ID,
			Kind:         KindSandboxActivate,
			PartitionKey: leasedJob.PartitionKey,
			DedupeKey:    leasedJob.DedupeKey,
			Now:          now.Add(5 * time.Second),
		})
		return err
	}); !IsIntegrityError(err) {
		t.Fatalf("CancelTx mismatched identity = %v; want integrity error", err)
	}

	cancelled = true
	if err := store.client.WithWorkspaceTx(ctx, string(ws), "queue.test_targeted_cancel_absent", func(tx *dbconnect.Tx) error {
		var err error
		cancelled, err = CancelTx(ctx, tx, TargetedCancelRequest{
			WorkspaceID: ws, JobID: "qjob_retention_deleted", Kind: KindSandboxActivate,
			PartitionKey: "sandbox-lifecycle:ws_targeted_cancel:sbox_absent",
			DedupeKey:    "sandbox_activate:ws_targeted_cancel:sbox_absent:sop_absent",
			Now:          now.Add(6 * time.Second),
		})
		return err
	}); err != nil || cancelled {
		t.Fatalf("CancelTx absent = (%v,%v); want false,nil", cancelled, err)
	}
}

func TestPostgreSQLStoreListsAndConditionallyDeadLettersPendingOverBudgetSandboxJobs(t *testing.T) {
	store, admin := newPostgreSQLQueueStore(t)
	ctx := context.Background()
	ws := workspace.ID("ws_over_budget")
	now := time.Date(2026, 7, 31, 13, 0, 0, 0, time.UTC)
	job := mustEnqueue(t, store, sandboxToolExecuteRequest(t, ws, "sesn_over_budget", "thrd_over_budget", "sevt_over_budget", "qjob_over_budget", 1, now))
	leased := mustLeaseOne(t, store, LeaseRequest{
		WorkspaceID: ws, Kinds: []string{KindSandboxToolExecute}, LeaseOwner: "sandbox",
		MaxJobs: 1, LeaseDuration: time.Second, Now: now,
	})
	if leased.AttemptCount != 1 || leased.MaxAttempts != 1 {
		t.Fatalf("leased budget = %d/%d; want 1/1", leased.AttemptCount, leased.MaxAttempts)
	}
	expireQueueJobLease(t, admin, ws, job.ID)
	if reclaimed, err := store.ReclaimExpiredLeases(ctx, ReclaimExpiredLeasesRequest{WorkspaceID: ws, Limit: 1}); err != nil || reclaimed != 1 {
		t.Fatalf("ReclaimExpiredLeases = (%d,%v); want 1,nil", reclaimed, err)
	}

	candidates, err := store.ListPendingAtOrOverBudget(ctx, ListPendingAtOrOverBudgetRequest{Limit: 10})
	if err != nil {
		t.Fatalf("ListPendingAtOrOverBudget: %v", err)
	}
	if len(candidates) != 1 || candidates[0].JobID != job.ID || candidates[0].AttemptCount != 1 || candidates[0].MaxAttempts != 1 {
		t.Fatalf("over-budget candidates = %#v; want %s at 1/1", candidates, job.ID)
	}

	updated := true
	if err := store.client.WithWorkspaceTx(ctx, string(ws), "queue.test_stale_dead_letter", func(tx *dbconnect.Tx) error {
		var err error
		updated, err = DeadLetterExhaustedTx(ctx, tx, DeadLetterExhaustedRequest{
			WorkspaceID: ws, JobID: job.ID, ObservedAttemptCount: 2,
			ErrorKind: "sandbox_execution_unavailable", ErrorMessage: "sandbox execution unavailable", Now: now.Add(3 * time.Second),
		})
		return err
	}); err != nil || updated {
		t.Fatalf("DeadLetterExhaustedTx stale observation = (%v,%v); want false,nil", updated, err)
	}
	if got := queueJobStatus(t, admin, ws, job.ID); got != StatusPending {
		t.Fatalf("status after stale dead-letter = %s; want pending", got)
	}

	if err := store.client.WithWorkspaceTx(ctx, string(ws), "queue.test_dead_letter", func(tx *dbconnect.Tx) error {
		var err error
		updated, err = DeadLetterExhaustedTx(ctx, tx, DeadLetterExhaustedRequest{
			WorkspaceID: ws, JobID: job.ID, ObservedAttemptCount: 1,
			ErrorKind: "sandbox_execution_unavailable", ErrorMessage: "sandbox execution unavailable", Now: now.Add(4 * time.Second),
		})
		return err
	}); err != nil || !updated {
		t.Fatalf("DeadLetterExhaustedTx = (%v,%v); want true,nil", updated, err)
	}
	if got := queueJobStatus(t, admin, ws, job.ID); got != StatusDeadLettered {
		t.Fatalf("status after dead-letter = %s; want dead_lettered", got)
	}

	invalidBudget := mustEnqueue(t, store, sandboxToolExecuteRequest(t, ws, "sesn_over_budget", "thrd_over_budget", "sevt_zero_budget", "qjob_zero_budget", 1, now.Add(5*time.Second)))
	if _, err := admin.ExecContext(ctx,
		`UPDATE queue_jobs SET max_attempts = 0 WHERE workspace_id = $1 AND id = $2`,
		string(ws), invalidBudget.ID,
	); err != nil {
		t.Fatalf("set invalid Sandbox budget: %v", err)
	}
	if _, err := store.ListPendingAtOrOverBudget(ctx, ListPendingAtOrOverBudgetRequest{Limit: 10}); !IsIntegrityError(err) {
		t.Fatalf("ListPendingAtOrOverBudget zero budget = %v; want integrity error", err)
	}
}

func TestPostgreSQLStoreAtomicallyReplacesMalformedRuntimeInputCustody(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	now := time.Now().UTC()
	const (
		sessionID      = "sesn_queue_replace_malformed"
		threadID       = "thr_queue_replace_malformed"
		runtimeInputID = "rin_queue_replace_malformed"
	)
	seedQueueRuntimeInbox(t, admin, sessionID, threadID, runtimeInputID, "messages", `["evt_canonical"]`, 7, 7)
	queued := mustEnqueue(t, store, EnqueueRequest{
		WorkspaceID: workspace.DefaultID, Kind: KindRuntimeInput,
		PartitionKey: FormatSessionPartitionKey(workspace.DefaultID, sessionID), DedupeKey: FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, runtimeInputID),
		PayloadVersion: 1, PayloadJSON: runtimeInputPayload(t, workspace.DefaultID, sessionID, threadID, runtimeInputID, "messages", 9, 9), Now: now,
	})
	leased := mustLeaseOne(t, store, LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{KindRuntimeInput}, LeaseOwner: "bridge", MaxJobs: 1, LeaseDuration: time.Minute,
	})

	outcome, err := store.ReplaceMalformedRuntimeInputCustody(context.Background(), ReplaceMalformedRuntimeInputCustodyRequest{
		WorkspaceID: workspace.DefaultID, SessionID: sessionID, RuntimeInputID: runtimeInputID,
		JobID: leased.ID, LeaseToken: leased.LeaseToken, Now: now.Add(time.Second),
	})
	if err != nil || !outcome.DeadLettered || !outcome.Replaced {
		t.Fatalf("replace malformed custody = (%+v,%v); want dead-lettered replacement", outcome, err)
	}
	var oldStatus, oldError string
	if err := admin.QueryRowContext(context.Background(), `SELECT status, last_error_kind FROM queue_jobs WHERE workspace_id='default' AND id=$1`, queued.ID).Scan(&oldStatus, &oldError); err != nil {
		t.Fatalf("read old malformed job: %v", err)
	}
	var replacements int
	var eventIDs string
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*), COALESCE(max(payload_json::jsonb ->> 'event_ids'), '')
		FROM queue_jobs WHERE workspace_id='default' AND status='pending' AND dedupe_key=$1`, FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, runtimeInputID)).Scan(&replacements, &eventIDs); err != nil {
		t.Fatalf("read canonical replacement: %v", err)
	}
	if oldStatus != StatusDeadLettered || oldError != "invalid_runtime_job_payload" || replacements != 1 || eventIDs != `["evt_canonical"]` {
		t.Fatalf("replacement state old=%s/%s pending=%d events=%s", oldStatus, oldError, replacements, eventIDs)
	}

	replayed, err := store.ReplaceMalformedRuntimeInputCustody(context.Background(), ReplaceMalformedRuntimeInputCustodyRequest{
		WorkspaceID: workspace.DefaultID, SessionID: sessionID, RuntimeInputID: runtimeInputID,
		JobID: leased.ID, LeaseToken: leased.LeaseToken, Now: now.Add(2 * time.Second),
	})
	if err != nil || replayed != (ReplaceMalformedRuntimeInputCustodyResult{}) {
		t.Fatalf("replay malformed custody = (%+v,%v); want no-op", replayed, err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM queue_jobs
		WHERE workspace_id='default' AND status='pending' AND dedupe_key=$1`, FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, runtimeInputID)).Scan(&replacements); err != nil {
		t.Fatalf("count replay replacements: %v", err)
	}
	if replacements != 1 {
		t.Fatalf("replay replacements = %d; want one", replacements)
	}
}

func TestCancelInterruptFencedMessagesUsesCanonicalFollowerIdentity(t *testing.T) {
	store, admin := newPostgreSQLQueueStore(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)
	const (
		sessionID   = "sesn_interrupt_followers"
		threadID    = "thr_interrupt_followers"
		interruptID = "rin_interrupt_followers_control"
	)
	seedQueueRuntimeInbox(t, admin, sessionID, threadID, "rin_interrupt_follower_queued", "messages", `["evt_follower_queued"]`, 1, 1)
	for _, follower := range []struct {
		id        string
		inboxKind string
		status    string
		sequence  int64
	}{
		{id: "rin_interrupt_follower_delivering", inboxKind: "messages", status: "delivering", sequence: 2},
		{id: "rin_interrupt_follower_rejection", inboxKind: "rejection", status: "queued", sequence: 3},
		{id: "rin_interrupt_follower_after", inboxKind: "messages", status: "queued", sequence: 6},
		{id: interruptID, inboxKind: "interrupt_control", status: "queued", sequence: 5},
	} {
		if _, err := admin.ExecContext(ctx, `INSERT INTO session_runtime_inbox (
			workspace_id,session_id,session_thread_id,runtime_input_id,input_kind,event_ids_json,
			sequence_from,sequence_to,status,binding_id,binding_generation,target_pod_uid,created_at,updated_at
		) VALUES ('default',$1,$2,$3,$4,'[]',$5,$5,$6,
			CASE WHEN $6='delivering' THEN 'bind_interrupt_followers' END,
			CASE WHEN $6='delivering' THEN 1 END,
			CASE WHEN $6='delivering' THEN 'pod_interrupt_followers' END,$7,$7)`,
			sessionID, threadID, follower.id, follower.inboxKind, follower.sequence, follower.status, now); err != nil {
			t.Fatalf("seed follower Inbox %s: %v", follower.id, err)
		}
	}
	jobs := make(map[string]*Job)
	for _, follower := range []struct {
		id       string
		sequence int64
		priority int
	}{
		{id: "rin_interrupt_follower_queued", sequence: 1},
		{id: "rin_interrupt_follower_delivering", sequence: 2},
		{id: "rin_interrupt_follower_rejection", sequence: 3},
		{id: "rin_interrupt_follower_after", sequence: 6},
		{id: interruptID, sequence: 5, priority: 100},
	} {
		inputKind := "messages"
		if follower.id == interruptID {
			inputKind = "interrupt_control"
		}
		jobs[follower.id] = mustEnqueue(t, store, EnqueueRequest{
			WorkspaceID: workspace.DefaultID, Kind: KindRuntimeInput,
			PartitionKey:   FormatSessionPartitionKey(workspace.DefaultID, sessionID),
			DedupeKey:      FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, follower.id),
			PayloadVersion: 1,
			PayloadJSON:    runtimeInputPayload(t, workspace.DefaultID, sessionID, threadID, follower.id, inputKind, follower.sequence, follower.sequence),
			Priority:       follower.priority, Now: now.Add(time.Duration(follower.sequence) * time.Microsecond),
		})
	}
	leased := mustLeaseOne(t, store, LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{KindRuntimeInput}, LeaseOwner: "interrupt-owner",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(time.Second),
	})
	if leased.ID != jobs[interruptID].ID {
		t.Fatalf("leased job = %s; want interrupt %s", leased.ID, jobs[interruptID].ID)
	}
	request := InterruptFenceRequest{
		Lease: ExactLeaseRequest{
			WorkspaceID: workspace.DefaultID, JobID: leased.ID, LeaseToken: leased.LeaseToken,
			Kind: leased.Kind, PartitionKey: leased.PartitionKey, DedupeKey: leased.DedupeKey,
		},
		SessionID: sessionID, SessionThreadID: threadID, InterruptFenceSequence: 5, Now: now.Add(2 * time.Second),
	}
	stale := request
	stale.Lease.LeaseToken = "lease_stale"
	if err := store.client.WithWorkspaceTx(ctx, string(workspace.DefaultID), "queue.test_interrupt_stale_lease", func(tx *dbconnect.Tx) error {
		active, cancelled, err := CancelInterruptFencedMessagesTx(ctx, tx, stale)
		if err != nil || active || cancelled != 0 {
			t.Fatalf("stale interrupt lease cancellation = %v/%d/%v; want false/0/nil", active, cancelled, err)
		}
		return nil
	}); err != nil {
		t.Fatalf("stale interrupt lease transaction: %v", err)
	}

	if err := store.client.WithWorkspaceTx(ctx, string(workspace.DefaultID), "queue.test_interrupt_followers", func(tx *dbconnect.Tx) error {
		active, cancelled, err := CancelInterruptFencedMessagesTx(ctx, tx, request)
		if err != nil || !active || cancelled != 3 {
			t.Fatalf("interrupt follower cancellation = %v/%d/%v; want true/3/nil", active, cancelled, err)
		}
		return nil
	}); err != nil {
		t.Fatalf("interrupt follower transaction: %v", err)
	}
	for _, inputID := range []string{"rin_interrupt_follower_queued", "rin_interrupt_follower_delivering", "rin_interrupt_follower_rejection"} {
		if got := queueJobStatus(t, admin, workspace.DefaultID, jobs[inputID].ID); got != StatusCancelled {
			t.Fatalf("Queue follower %s status = %s; want cancelled", inputID, got)
		}
		var inboxStatus string
		if err := admin.QueryRowContext(ctx, `SELECT status FROM session_runtime_inbox
			WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2`, sessionID, inputID).Scan(&inboxStatus); err != nil {
			t.Fatalf("read Inbox follower %s: %v", inputID, err)
		}
		if inboxStatus != "cancelled" {
			t.Fatalf("Inbox follower %s status = %s; want cancelled", inputID, inboxStatus)
		}
	}
	if got := queueJobStatus(t, admin, workspace.DefaultID, jobs["rin_interrupt_follower_after"].ID); got != StatusPending {
		t.Fatalf("post-fence Queue follower status = %s; want pending", got)
	}
}

func TestCancelInterruptFencedMessagesRollsBackBothCustodySides(t *testing.T) {
	store, admin := newPostgreSQLQueueStore(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 18, 11, 0, 0, 0, time.UTC)
	const (
		sessionID   = "sesn_interrupt_followers_rollback"
		threadID    = "thr_interrupt_followers_rollback"
		followerID  = "rin_interrupt_follower_rollback"
		interruptID = "rin_interrupt_followers_rollback_control"
	)
	seedQueueRuntimeInbox(t, admin, sessionID, threadID, followerID, "messages", `["evt_follower_rollback"]`, 1, 1)
	if _, err := admin.ExecContext(ctx, `INSERT INTO session_runtime_inbox (
		workspace_id,session_id,session_thread_id,runtime_input_id,input_kind,event_ids_json,
		sequence_from,sequence_to,status,created_at,updated_at
	) VALUES ('default',$1,$2,$3,'interrupt_control','[]',2,2,'queued',$4,$4)`, sessionID, threadID, interruptID, now); err != nil {
		t.Fatalf("seed rollback interrupt Inbox: %v", err)
	}
	follower := mustEnqueue(t, store, EnqueueRequest{
		WorkspaceID: workspace.DefaultID, Kind: KindRuntimeInput,
		PartitionKey:   FormatSessionPartitionKey(workspace.DefaultID, sessionID),
		DedupeKey:      FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, followerID),
		PayloadVersion: 1, PayloadJSON: runtimeInputPayload(t, workspace.DefaultID, sessionID, threadID, followerID, "messages", 1, 1), Now: now,
	})
	interrupt := mustEnqueue(t, store, EnqueueRequest{
		WorkspaceID: workspace.DefaultID, Kind: KindRuntimeInput,
		PartitionKey:   FormatSessionPartitionKey(workspace.DefaultID, sessionID),
		DedupeKey:      FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, interruptID),
		PayloadVersion: 1, PayloadJSON: runtimeInputPayload(t, workspace.DefaultID, sessionID, threadID, interruptID, "interrupt_control", 2, 2), Priority: 100, Now: now.Add(time.Microsecond),
	})
	leased := mustLeaseOne(t, store, LeaseRequest{WorkspaceID: workspace.DefaultID, Kinds: []string{KindRuntimeInput}, LeaseOwner: "rollback-owner", MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(time.Second)})
	if leased.ID != interrupt.ID {
		t.Fatalf("leased job = %s; want interrupt %s", leased.ID, interrupt.ID)
	}
	rollbackErr := errors.New("force interrupt follower rollback")
	err := store.client.WithWorkspaceTx(ctx, string(workspace.DefaultID), "queue.test_interrupt_followers_rollback", func(tx *dbconnect.Tx) error {
		active, cancelled, err := CancelInterruptFencedMessagesTx(ctx, tx, InterruptFenceRequest{
			Lease:     ExactLeaseRequest{WorkspaceID: workspace.DefaultID, JobID: leased.ID, LeaseToken: leased.LeaseToken, Kind: leased.Kind, PartitionKey: leased.PartitionKey, DedupeKey: leased.DedupeKey},
			SessionID: sessionID, SessionThreadID: threadID, InterruptFenceSequence: 2, Now: now.Add(2 * time.Second),
		})
		if err != nil || !active || cancelled != 1 {
			t.Fatalf("rollback cancellation = %v/%d/%v; want true/1/nil", active, cancelled, err)
		}
		return rollbackErr
	})
	if !errors.Is(err, rollbackErr) {
		t.Fatalf("rollback transaction error = %v; want %v", err, rollbackErr)
	}
	if got := queueJobStatus(t, admin, workspace.DefaultID, follower.ID); got != StatusPending {
		t.Fatalf("Queue follower after rollback = %s; want pending", got)
	}
	var inboxStatus string
	if err := admin.QueryRowContext(ctx, `SELECT status FROM session_runtime_inbox
		WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2`, sessionID, followerID).Scan(&inboxStatus); err != nil {
		t.Fatalf("read rollback Inbox follower: %v", err)
	}
	if inboxStatus != "queued" {
		t.Fatalf("Inbox follower after rollback = %s; want queued", inboxStatus)
	}
}

func TestPostgreSQLStoreRebuildsCanonicalEventlessRuntimeInputCustody(t *testing.T) {
	for _, inputKind := range []string{"agent_mail", "task_notification"} {
		t.Run(inputKind, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
			now := time.Now().UTC()
			sessionID := "sesn_queue_replace_" + inputKind
			threadID := "thr_queue_replace_" + inputKind
			runtimeInputID := inputKind + ":source_1"
			seedQueueRuntimeInbox(t, admin, sessionID, threadID, runtimeInputID, inputKind, `[]`, 0, 0)
			if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_inbox
				SET sequence_from=NULL, sequence_to=NULL WHERE workspace_id='default' AND runtime_input_id=$1`, runtimeInputID); err != nil {
				t.Fatalf("clear eventless Inbox sequence: %v", err)
			}
			mustEnqueue(t, store, EnqueueRequest{
				WorkspaceID: workspace.DefaultID, Kind: KindRuntimeInput,
				PartitionKey: FormatSessionPartitionKey(workspace.DefaultID, sessionID), DedupeKey: FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, runtimeInputID),
				PayloadVersion: 1, PayloadJSON: runtimeInputPayload(t, workspace.DefaultID, sessionID, threadID, runtimeInputID, "messages", 9, 9), Now: now,
			})
			leased := mustLeaseOne(t, store, LeaseRequest{WorkspaceID: workspace.DefaultID, Kinds: []string{KindRuntimeInput}, LeaseOwner: "bridge", MaxJobs: 1, LeaseDuration: time.Minute})

			outcome, err := store.ReplaceMalformedRuntimeInputCustody(context.Background(), ReplaceMalformedRuntimeInputCustodyRequest{
				WorkspaceID: workspace.DefaultID, SessionID: sessionID, RuntimeInputID: runtimeInputID,
				JobID: leased.ID, LeaseToken: leased.LeaseToken, Now: now.Add(time.Second),
			})
			if err != nil || !outcome.DeadLettered || !outcome.Replaced {
				t.Fatalf("replace %s custody = (%+v,%v); want dead-lettered replacement", inputKind, outcome, err)
			}
			var replacementKind, eventIDs string
			var sequenceFrom, sequenceTo int64
			if err := admin.QueryRowContext(context.Background(), `SELECT
				payload_json::jsonb ->> 'input_kind', payload_json::jsonb ->> 'event_ids',
				(payload_json::jsonb ->> 'sequence_from')::bigint, (payload_json::jsonb ->> 'sequence_to')::bigint
				FROM queue_jobs WHERE workspace_id='default' AND status='pending' AND dedupe_key=$1`,
				FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, runtimeInputID),
			).Scan(&replacementKind, &eventIDs, &sequenceFrom, &sequenceTo); err != nil {
				t.Fatalf("read %s replacement: %v", inputKind, err)
			}
			if replacementKind != inputKind || eventIDs != "[]" || sequenceFrom != 0 || sequenceTo != 0 {
				t.Fatalf("%s replacement = %s/%s/%d/%d; want canonical eventless shape", inputKind, replacementKind, eventIDs, sequenceFrom, sequenceTo)
			}
		})
	}
}

func TestPostgreSQLStoreMalformedRuntimeInputNoReplacementDispositions(t *testing.T) {
	for _, inboxStatus := range []string{"delivering", "accepted", "parked", "missing"} {
		t.Run(inboxStatus, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
			now := time.Now().UTC()
			sessionID := "sesn_queue_no_replace_" + inboxStatus
			threadID := "thr_queue_no_replace_" + inboxStatus
			runtimeInputID := "rin_queue_no_replace_" + inboxStatus
			seedQueueRuntimeInbox(t, admin, sessionID, threadID, runtimeInputID, "messages", `["evt_1"]`, 1, 1)
			if inboxStatus == "missing" {
				if _, err := admin.ExecContext(context.Background(), `DELETE FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1`, runtimeInputID); err != nil {
					t.Fatalf("delete Inbox: %v", err)
				}
			} else if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_inbox
				SET status=$2,binding_id='bind_1',binding_generation=1,target_pod_uid='pod_1'
				WHERE workspace_id='default' AND runtime_input_id=$1`, runtimeInputID, inboxStatus); err != nil {
				t.Fatalf("set Inbox status %s: %v", inboxStatus, err)
			}
			mustEnqueue(t, store, EnqueueRequest{
				WorkspaceID: workspace.DefaultID, Kind: KindRuntimeInput,
				PartitionKey: FormatSessionPartitionKey(workspace.DefaultID, sessionID), DedupeKey: FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, runtimeInputID),
				PayloadVersion: 1, PayloadJSON: runtimeInputPayload(t, workspace.DefaultID, sessionID, threadID, runtimeInputID, "messages", 2, 2), Now: now,
			})
			leased := mustLeaseOne(t, store, LeaseRequest{WorkspaceID: workspace.DefaultID, Kinds: []string{KindRuntimeInput}, LeaseOwner: "bridge", MaxJobs: 1, LeaseDuration: time.Minute})
			outcome, err := store.ReplaceMalformedRuntimeInputCustody(context.Background(), ReplaceMalformedRuntimeInputCustodyRequest{
				WorkspaceID: workspace.DefaultID, SessionID: sessionID, RuntimeInputID: runtimeInputID,
				JobID: leased.ID, LeaseToken: leased.LeaseToken, Now: now.Add(time.Second),
			})
			if err != nil || !outcome.DeadLettered || outcome.Replaced {
				t.Fatalf("%s malformed disposition = (%+v,%v); want dead-letter/no replacement", inboxStatus, outcome, err)
			}
			if inboxStatus != "missing" {
				var current string
				if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1`, runtimeInputID).Scan(&current); err != nil {
					t.Fatalf("read Inbox status: %v", err)
				}
				want := inboxStatus
				if inboxStatus == "delivering" || inboxStatus == "accepted" {
					want = "dead_lettered"
				}
				if current != want {
					t.Fatalf("Inbox status = %s; want %s", current, want)
				}
			}
		})
	}
}

func TestPostgreSQLStoreMalformedRuntimeInputIntegrityFailurePreservesLease(t *testing.T) {
	for _, test := range []struct {
		name         string
		eventIDsJSON string
		sequenceFrom int64
		sequenceTo   int64
	}{
		{name: "invalid event json", eventIDsJSON: `not-json`, sequenceFrom: 1, sequenceTo: 1},
		{name: "invalid event range", eventIDsJSON: `["evt_1"]`, sequenceFrom: 0, sequenceTo: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
			now := time.Now().UTC()
			sessionID := "sesn_queue_integrity_" + strings.ReplaceAll(test.name, " ", "_")
			threadID := "thr_queue_integrity_" + strings.ReplaceAll(test.name, " ", "_")
			runtimeInputID := "rin_queue_integrity_" + strings.ReplaceAll(test.name, " ", "_")
			seedQueueRuntimeInbox(t, admin, sessionID, threadID, runtimeInputID, "messages", test.eventIDsJSON, test.sequenceFrom, test.sequenceTo)
			mustEnqueue(t, store, EnqueueRequest{
				WorkspaceID: workspace.DefaultID, Kind: KindRuntimeInput,
				PartitionKey: FormatSessionPartitionKey(workspace.DefaultID, sessionID), DedupeKey: FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, runtimeInputID),
				PayloadVersion: 1, PayloadJSON: runtimeInputPayload(t, workspace.DefaultID, sessionID, threadID, runtimeInputID, "messages", 2, 2), Now: now,
			})
			leased := mustLeaseOne(t, store, LeaseRequest{WorkspaceID: workspace.DefaultID, Kinds: []string{KindRuntimeInput}, LeaseOwner: "bridge", MaxJobs: 1, LeaseDuration: time.Minute})
			if _, err := store.ReplaceMalformedRuntimeInputCustody(context.Background(), ReplaceMalformedRuntimeInputCustodyRequest{
				WorkspaceID: workspace.DefaultID, SessionID: sessionID, RuntimeInputID: runtimeInputID,
				JobID: leased.ID, LeaseToken: leased.LeaseToken, Now: now.Add(time.Second),
			}); !IsIntegrityError(err) {
				t.Fatalf("integrity replacement err = %v; want IntegrityError", err)
			}
			var queueStatus, leaseToken string
			if err := admin.QueryRowContext(context.Background(), `SELECT status, COALESCE(lease_token, '') FROM queue_jobs WHERE workspace_id='default' AND id=$1`, leased.ID).Scan(&queueStatus, &leaseToken); err != nil {
				t.Fatalf("read preserved lease: %v", err)
			}
			if queueStatus != StatusLeased || leaseToken != leased.LeaseToken {
				t.Fatalf("preserved lease = %s/%s; want leased/%s", queueStatus, leaseToken, leased.LeaseToken)
			}
		})
	}
}

func TestPostgreSQLStoreMalformedRuntimeInputReplacementRollsBackTogether(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	now := time.Now().UTC()
	const (
		sessionID      = "sesn_queue_replace_rollback"
		threadID       = "thr_queue_replace_rollback"
		runtimeInputID = "rin_queue_replace_rollback"
	)
	seedQueueRuntimeInbox(t, admin, sessionID, threadID, runtimeInputID, "messages", `["evt_rollback"]`, 1, 1)
	queued := mustEnqueue(t, store, EnqueueRequest{
		WorkspaceID: workspace.DefaultID, Kind: KindRuntimeInput,
		PartitionKey: FormatSessionPartitionKey(workspace.DefaultID, sessionID), DedupeKey: FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, runtimeInputID),
		PayloadVersion: 1, PayloadJSON: runtimeInputPayload(t, workspace.DefaultID, sessionID, threadID, runtimeInputID, "messages", 2, 2), Now: now,
	})
	leased := mustLeaseOne(t, store, LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{KindRuntimeInput}, LeaseOwner: "bridge", MaxJobs: 1, LeaseDuration: time.Minute,
	})
	if _, err := admin.ExecContext(context.Background(), `CREATE FUNCTION queue_fail_replacement_insert() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN IF NEW.id <> '`+queued.ID+`' THEN RAISE EXCEPTION 'synthetic replacement insert failure'; END IF; RETURN NEW; END $$;
		CREATE TRIGGER queue_fail_replacement_insert BEFORE INSERT ON queue_jobs FOR EACH ROW EXECUTE FUNCTION queue_fail_replacement_insert()`); err != nil {
		t.Fatalf("install replacement failure trigger: %v", err)
	}

	if _, err := store.ReplaceMalformedRuntimeInputCustody(context.Background(), ReplaceMalformedRuntimeInputCustodyRequest{
		WorkspaceID: workspace.DefaultID, SessionID: sessionID, RuntimeInputID: runtimeInputID,
		JobID: leased.ID, LeaseToken: leased.LeaseToken, Now: now.Add(time.Second),
	}); err == nil {
		t.Fatal("replace malformed custody succeeded despite injected enqueue failure")
	}
	var status, token string
	var pending int
	if err := admin.QueryRowContext(context.Background(), `SELECT status, COALESCE(lease_token, '') FROM queue_jobs WHERE workspace_id='default' AND id=$1`, leased.ID).Scan(&status, &token); err != nil {
		t.Fatalf("read rolled-back old job: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND status='pending' AND dedupe_key=$1`, FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, runtimeInputID)).Scan(&pending); err != nil {
		t.Fatalf("count rolled-back replacements: %v", err)
	}
	if status != StatusLeased || token != leased.LeaseToken || pending != 0 {
		t.Fatalf("rolled-back custody status=%s token=%s pending=%d; want original lease and no replacement", status, token, pending)
	}
}

func TestPostgreSQLStoreMalformedRuntimeInputReplacementRejectsStaleLeases(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *sql.DB, *Job) string
	}{
		{name: "wrong token", mutate: func(_ *testing.T, _ *sql.DB, _ *Job) string { return "qlt_wrong" }},
		{name: "expired lease", mutate: func(t *testing.T, db *sql.DB, leased *Job) string {
			t.Helper()
			if _, err := db.ExecContext(context.Background(), `UPDATE queue_jobs SET leased_until=clock_timestamp()-interval '1 second' WHERE workspace_id='default' AND id=$1`, leased.ID); err != nil {
				t.Fatalf("expire malformed lease: %v", err)
			}
			return leased.LeaseToken
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
			now := time.Now().UTC()
			sessionID := "sesn_queue_stale_" + strings.ReplaceAll(test.name, " ", "_")
			threadID := "thr_queue_stale_" + strings.ReplaceAll(test.name, " ", "_")
			runtimeInputID := "rin_queue_stale_" + strings.ReplaceAll(test.name, " ", "_")
			seedQueueRuntimeInbox(t, admin, sessionID, threadID, runtimeInputID, "messages", `["evt_stale"]`, 1, 1)
			queued := mustEnqueue(t, store, EnqueueRequest{
				WorkspaceID: workspace.DefaultID, Kind: KindRuntimeInput,
				PartitionKey: FormatSessionPartitionKey(workspace.DefaultID, sessionID), DedupeKey: FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, runtimeInputID),
				PayloadVersion: 1, PayloadJSON: runtimeInputPayload(t, workspace.DefaultID, sessionID, threadID, runtimeInputID, "messages", 2, 2), Now: now,
			})
			leased := mustLeaseOne(t, store, LeaseRequest{WorkspaceID: workspace.DefaultID, Kinds: []string{KindRuntimeInput}, LeaseOwner: "bridge", MaxJobs: 1, LeaseDuration: time.Minute})
			token := test.mutate(t, admin, leased)
			outcome, err := store.ReplaceMalformedRuntimeInputCustody(context.Background(), ReplaceMalformedRuntimeInputCustodyRequest{
				WorkspaceID: workspace.DefaultID, SessionID: sessionID, RuntimeInputID: runtimeInputID,
				JobID: leased.ID, LeaseToken: token, Now: now.Add(time.Second),
			})
			if err != nil || outcome != (ReplaceMalformedRuntimeInputCustodyResult{}) {
				t.Fatalf("stale replacement = (%+v,%v); want no-op", outcome, err)
			}
			var status string
			var replacements int
			if err := admin.QueryRowContext(context.Background(), `SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$1`, queued.ID).Scan(&status); err != nil {
				t.Fatalf("read stale old job: %v", err)
			}
			if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND status='pending' AND dedupe_key=$1`, FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, runtimeInputID)).Scan(&replacements); err != nil {
				t.Fatalf("count stale replacements: %v", err)
			}
			if status != StatusLeased || replacements != 0 {
				t.Fatalf("stale custody status=%s replacements=%d; want leased/0", status, replacements)
			}
		})
	}
}

func TestPostgreSQLStoreSweepsOnlyExpiredSandboxTerminalJobsThenEmptyCounters(t *testing.T) {
	store, admin := newPostgreSQLQueueStore(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 31, 14, 0, 0, 0, time.UTC)
	ws := workspace.ID("ws_sandbox_retention")
	oldSandbox := mustEnqueue(t, store, sandboxToolExecuteRequest(t, ws, "sesn_retention", "thrd_retention", "sevt_old_sandbox", "qjob_old_sandbox", 2, now.Add(-SandboxTerminalRetentionAge)))
	newSandbox := mustEnqueue(t, store, sandboxToolExecuteRequest(t, ws, "sesn_retention", "thrd_retention", "sevt_new_sandbox", "qjob_new_sandbox", 2, now.Add(-time.Hour)))
	nonSandbox := mustEnqueue(t, store, EnqueueRequest{
		ID: "qjob_old_runtime", WorkspaceID: ws, Kind: KindCleanupSession,
		PartitionKey: FormatSessionPartitionKey(ws, "sesn_retention"),
		DedupeKey:    FormatCleanupSessionDedupeKey(ws, "sesn_retention", "cleanup_retention"),
		PayloadJSON:  queuePayload(t, map[string]any{"workspace_id": ws, "session_id": "sesn_retention", "cleanup_job_id": "cleanup_retention"}),
		Now:          now.Add(-26 * time.Hour),
	})
	for _, job := range []*Job{oldSandbox, newSandbox, nonSandbox} {
		completedAt := job.CreatedAt
		if _, err := admin.ExecContext(ctx,
			`UPDATE queue_jobs SET status = 'acknowledged', acknowledged_at = $3, updated_at = $3 WHERE workspace_id = $1 AND id = $2`,
			string(ws), job.ID, completedAt,
		); err != nil {
			t.Fatalf("mark %s acknowledged: %v", job.ID, err)
		}
	}

	deleted, err := store.SweepSandboxTerminalJobs(ctx, SandboxTerminalSweepRequest{Now: now, Limit: 100})
	if err != nil || deleted != 1 {
		t.Fatalf("SweepSandboxTerminalJobs = (%d,%v); want 1,nil", deleted, err)
	}
	if queueJobExists(t, admin, ws, oldSandbox.ID) {
		t.Fatalf("old sandbox job %s still exists", oldSandbox.ID)
	}
	if !queueJobExists(t, admin, ws, newSandbox.ID) || !queueJobExists(t, admin, ws, nonSandbox.ID) {
		t.Fatalf("retention deleted an ineligible or non-sandbox job")
	}

	deletedCounters, err := store.SweepEmptyPartitionCounters(ctx, EmptyPartitionCounterSweepRequest{Limit: 100})
	if err != nil {
		t.Fatalf("SweepEmptyPartitionCounters: %v", err)
	}
	if deletedCounters != 1 {
		t.Fatalf("deleted counters = %d; want only the old sandbox partition", deletedCounters)
	}
	if queuePartitionCounterExists(t, admin, ws, oldSandbox.PartitionKey) {
		t.Fatalf("empty old sandbox counter still exists")
	}
	if !queuePartitionCounterExists(t, admin, ws, newSandbox.PartitionKey) || !queuePartitionCounterExists(t, admin, ws, nonSandbox.PartitionKey) {
		t.Fatalf("counter sweep deleted a nonempty partition")
	}

	corrupt := mustEnqueue(t, store, sandboxToolExecuteRequest(t, ws, "sesn_retention", "thrd_retention", "sevt_corrupt_sandbox", "qjob_corrupt_sandbox", 2, now.Add(-26*time.Hour)))
	if _, err := admin.ExecContext(ctx,
		`UPDATE queue_jobs SET status = 'acknowledged', updated_at = $3 WHERE workspace_id = $1 AND id = $2`,
		string(ws), corrupt.ID, corrupt.CreatedAt,
	); err != nil {
		t.Fatalf("mark %s terminal without timestamp: %v", corrupt.ID, err)
	}
	oldAlongsideCorrupt := mustEnqueue(t, store, sandboxToolExecuteRequest(t, ws, "sesn_retention", "thrd_retention", "sevt_old_peer", "qjob_old_peer", 2, now.Add(-27*time.Hour)))
	if _, err := admin.ExecContext(ctx,
		`UPDATE queue_jobs SET status = 'acknowledged', acknowledged_at = $3, updated_at = $3 WHERE workspace_id = $1 AND id = $2`,
		string(ws), oldAlongsideCorrupt.ID, oldAlongsideCorrupt.CreatedAt,
	); err != nil {
		t.Fatalf("mark %s acknowledged: %v", oldAlongsideCorrupt.ID, err)
	}
	deleted, err = store.SweepSandboxTerminalJobs(ctx, SandboxTerminalSweepRequest{Now: now, Limit: 100})
	if !IsIntegrityError(err) || deleted != 1 {
		t.Fatalf("SweepSandboxTerminalJobs with corrupt peer = (%d,%v); want 1,integrity error", deleted, err)
	}
	if !queueJobExists(t, admin, ws, corrupt.ID) || queueJobExists(t, admin, ws, oldAlongsideCorrupt.ID) {
		t.Fatal("terminal sweep did not retain corrupt row while deleting eligible peer")
	}
}

func TestPostgreSQLStoreSandboxTerminalSweepCoversEveryTerminalStatusAcrossWorkspacesAndCapsAt100(t *testing.T) {
	store, admin := newPostgreSQLQueueStore(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 31, 14, 30, 0, 0, time.UTC)
	workspaces := []workspace.ID{"ws_sandbox_retention_a", "ws_sandbox_retention_b"}
	statuses := []string{"acknowledged", "cancelled", "dead_lettered"}
	for index := 0; index < SandboxMaintenanceBatchLimit+1; index++ {
		ws := workspaces[index%len(workspaces)]
		jobID := "qjob_terminal_" + strconv.Itoa(index)
		job := mustEnqueue(t, store, sandboxToolExecuteRequest(t, ws, "sesn_retention_cap", "thrd_retention_cap", "sevt_"+strconv.Itoa(index), jobID, 2, now.Add(-SandboxTerminalRetentionAge)))
		statusValue := statuses[index%len(statuses)]
		if _, err := admin.ExecContext(ctx,
			`UPDATE queue_jobs
			    SET status = $3,
			        acknowledged_at = CASE WHEN $3::text = 'acknowledged' THEN $4::timestamptz ELSE NULL END,
			        cancelled_at = CASE WHEN $3::text = 'cancelled' THEN $4::timestamptz ELSE NULL END,
			        dead_lettered_at = CASE WHEN $3::text = 'dead_lettered' THEN $4::timestamptz ELSE NULL END,
			        updated_at = $4
			  WHERE workspace_id = $1 AND id = $2`,
			string(ws), job.ID, statusValue, now.Add(-SandboxTerminalRetentionAge),
		); err != nil {
			t.Fatalf("mark %s %s: %v", job.ID, statusValue, err)
		}
	}

	deleted, err := store.SweepSandboxTerminalJobs(ctx, SandboxTerminalSweepRequest{Now: now, Limit: SandboxMaintenanceBatchLimit + 50})
	if err != nil || deleted != SandboxMaintenanceBatchLimit {
		t.Fatalf("first SweepSandboxTerminalJobs = (%d,%v); want %d,nil", deleted, err, SandboxMaintenanceBatchLimit)
	}
	deleted, err = store.SweepSandboxTerminalJobs(ctx, SandboxTerminalSweepRequest{Now: now, Limit: SandboxMaintenanceBatchLimit})
	if err != nil || deleted != 1 {
		t.Fatalf("second SweepSandboxTerminalJobs = (%d,%v); want 1,nil", deleted, err)
	}
}

func TestEmptyCounterCleanupSerializesWithConcurrentEnqueue(t *testing.T) {
	store, admin := newPostgreSQLQueueStore(t)
	ctx := context.Background()
	ws := workspace.ID("ws_counter_enqueue_race")
	now := time.Date(2026, 7, 31, 15, 0, 0, 0, time.UTC)
	oldJob := mustEnqueue(t, store, sandboxToolExecuteRequest(t, ws, "sesn_counter_race", "thrd_counter_race", "sevt_counter_race", "qjob_counter_old", 2, now.Add(-26*time.Hour)))
	if _, err := admin.ExecContext(ctx,
		`UPDATE queue_jobs SET status = 'acknowledged', acknowledged_at = $3, updated_at = $3 WHERE workspace_id = $1 AND id = $2`,
		string(ws), oldJob.ID, oldJob.CreatedAt,
	); err != nil {
		t.Fatalf("mark old counter job acknowledged: %v", err)
	}
	if deleted, err := store.SweepSandboxTerminalJobs(ctx, SandboxTerminalSweepRequest{Now: now, Limit: 1}); err != nil || deleted != 1 {
		t.Fatalf("delete old counter job = (%d,%v); want 1,nil", deleted, err)
	}

	const advisoryKey int64 = 731731
	lockConn, err := admin.Conn(ctx)
	if err != nil {
		t.Fatalf("open advisory-lock connection: %v", err)
	}
	defer func() { _ = lockConn.Close() }()
	if _, err := lockConn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, advisoryKey); err != nil {
		t.Fatalf("hold counter-delete barrier: %v", err)
	}
	if _, err := admin.ExecContext(ctx, `
		CREATE FUNCTION queue_counter_delete_barrier() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF OLD.workspace_id = 'ws_counter_enqueue_race' THEN
				PERFORM pg_advisory_xact_lock(731731);
			END IF;
			RETURN OLD;
		END $$;
		CREATE TRIGGER queue_counter_delete_barrier
		BEFORE DELETE ON queue_partition_counters
		FOR EACH ROW EXECUTE FUNCTION queue_counter_delete_barrier()`); err != nil {
		t.Fatalf("install counter-delete barrier: %v", err)
	}
	t.Cleanup(func() {
		_, _ = lockConn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, advisoryKey)
		_, _ = admin.ExecContext(context.Background(), `DROP TRIGGER IF EXISTS queue_counter_delete_barrier ON queue_partition_counters`)
		_, _ = admin.ExecContext(context.Background(), `DROP FUNCTION IF EXISTS queue_counter_delete_barrier()`)
	})

	cleanupDone := make(chan error, 1)
	go func() {
		_, err := store.SweepEmptyPartitionCounters(ctx, EmptyPartitionCounterSweepRequest{Limit: 1})
		cleanupDone <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting bool
		if err := admin.QueryRowContext(ctx,
			`SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND NOT granted AND objid = $1)`,
			advisoryKey,
		).Scan(&waiting); err != nil {
			t.Fatalf("observe counter-delete barrier: %v", err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("production counter cleanup did not reach the delete barrier")
		}
		time.Sleep(10 * time.Millisecond)
	}

	request := sandboxToolExecuteRequest(t, ws, "sesn_counter_race", "thrd_counter_race", "sevt_counter_race", "qjob_counter_new", 2, now)
	enqueueDone := make(chan error, 1)
	go func() {
		_, err := store.Enqueue(ctx, request)
		enqueueDone <- err
	}()
	select {
	case err := <-enqueueDone:
		t.Fatalf("concurrent enqueue completed before counter lock released: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := lockConn.ExecContext(ctx, `SELECT pg_advisory_unlock($1)`, advisoryKey); err != nil {
		t.Fatalf("release counter-delete barrier: %v", err)
	}
	select {
	case err := <-cleanupDone:
		if err != nil {
			t.Fatalf("counter cleanup transaction: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("counter cleanup transaction timed out")
	}
	select {
	case err := <-enqueueDone:
		if err != nil {
			t.Fatalf("concurrent enqueue after counter cleanup: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent enqueue timed out")
	}
	if got := queueJobPartitionSequence(t, admin, ws, "qjob_counter_new"); got != 1 {
		t.Fatalf("new surviving partition sequence = %d; want 1", got)
	}
	if !queuePartitionCounterExists(t, admin, ws, oldJob.PartitionKey) {
		t.Fatal("concurrent enqueue did not recreate the partition counter")
	}
}

func newPostgreSQLQueueStore(t testing.TB) (*PostgreSQLQueueStore, *sql.DB) {
	t.Helper()
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	return NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime)), admin
}

func sandboxToolExecuteRequest(t testing.TB, ws workspace.ID, sessionID string, threadID string, toolUseEventID string, jobID string, maxAttempts int, now time.Time) EnqueueRequest {
	t.Helper()
	return EnqueueRequest{
		ID: jobID, WorkspaceID: ws, Kind: KindSandboxToolExecute,
		PartitionKey: FormatSandboxExecutionPartitionKey(ws, sessionID, threadID, toolUseEventID),
		DedupeKey:    FormatSandboxToolExecuteDedupeKey(ws, sessionID, threadID, toolUseEventID, 1),
		PayloadJSON: queuePayload(t, map[string]any{
			"workspace_id": ws, "session_id": sessionID, "session_thread_id": threadID,
			"tool_use_event_id": toolUseEventID,
		}),
		MaxAttempts: maxAttempts,
		Now:         now,
	}
}

func queueJobExists(t testing.TB, admin *sql.DB, ws workspace.ID, jobID string) bool {
	t.Helper()
	var exists bool
	if err := admin.QueryRow(`SELECT EXISTS (SELECT 1 FROM queue_jobs WHERE workspace_id = $1 AND id = $2)`, string(ws), jobID).Scan(&exists); err != nil {
		t.Fatalf("read queue job existence: %v", err)
	}
	return exists
}

func queuePartitionCounterExists(t testing.TB, admin *sql.DB, ws workspace.ID, partitionKey string) bool {
	t.Helper()
	var exists bool
	if err := admin.QueryRow(`SELECT EXISTS (SELECT 1 FROM queue_partition_counters WHERE workspace_id = $1 AND partition_key = $2)`, string(ws), partitionKey).Scan(&exists); err != nil {
		t.Fatalf("read queue counter existence: %v", err)
	}
	return exists
}

func mustEnqueue(t testing.TB, store *PostgreSQLQueueStore, request EnqueueRequest) *Job {
	t.Helper()
	job, err := store.Enqueue(context.Background(), request)
	if err != nil {
		t.Fatalf("Enqueue(%s): %v", request.ID, err)
	}
	return job
}

func seedQueueRuntimeInbox(
	t testing.TB,
	db *sql.DB,
	sessionID string,
	threadID string,
	runtimeInputID string,
	inputKind string,
	eventIDsJSON string,
	sequenceFrom int64,
	sequenceTo int64,
) {
	t.Helper()
	const now = "2026-01-01T00:00:00Z"
	agentID := "agent_" + sessionID
	agentVersionID := "agv_" + sessionID
	environmentID := "env_" + sessionID
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO workspaces (id, type, name, created_at) VALUES ('default', 'workspace', 'default', $1) ON CONFLICT (id) DO NOTHING`, []any{now}},
		{`INSERT INTO agents (workspace_id, id, name, version, created_at, updated_at) VALUES ('default', $1, $1, 1, $2, $2)`, []any{agentID, now}},
		{`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at) VALUES ('default', $1, $2, 1, '{}', $3, $4)`, []any{agentVersionID, agentID, "hash_" + sessionID, now}},
		{`INSERT INTO environments (workspace_id, id, name, config_json, created_at, updated_at) VALUES ('default', $1, $1, '{}', $2, $2)`, []any{environmentID, now}},
		{`INSERT INTO sessions (workspace_id, id, main_thread_id, type, status, lifecycle_state, agent_id, agent_version, environment_id, installed_tools_json, created_at, updated_at) VALUES ('default', $1, $2, 'session', 'idle', 'active', $3, 1, $4, '{}', $5, $5)`, []any{sessionID, threadID, agentID, environmentID, now}},
		{`INSERT INTO session_threads (workspace_id, id, session_id, role, visibility, status, created_at, last_active_at, updated_at) VALUES ('default', $1, $2, 'main', 'public', 'idle', $3, $3, $3)`, []any{threadID, sessionID, now}},
		{`INSERT INTO session_runtime_inbox (workspace_id, session_id, session_thread_id, runtime_input_id, input_kind, event_ids_json, sequence_from, sequence_to, status, created_at, updated_at) VALUES ('default', $1, $2, $3, $4, $5, $6, $7, 'queued', $8, $8)`, []any{sessionID, threadID, runtimeInputID, inputKind, eventIDsJSON, sequenceFrom, sequenceTo, now}},
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(context.Background(), statement.query, statement.args...); err != nil {
			t.Fatalf("seed Queue runtime Inbox: %v", err)
		}
	}
}

func runtimeInputPayload(t testing.TB, ws workspace.ID, sessionID string, threadID string, runtimeInputID string, inputKind string, sequenceFrom int64, sequenceTo int64) []byte {
	t.Helper()
	return queuePayload(t, map[string]any{
		"workspace_id":      string(ws),
		"session_id":        sessionID,
		"session_thread_id": threadID,
		"runtime_input_id":  runtimeInputID,
		"event_ids":         []string{"ev_" + runtimeInputID},
		"sequence_from":     sequenceFrom,
		"sequence_to":       sequenceTo,
		"input_kind":        inputKind,
	})
}

func queuePayload(t testing.TB, payload map[string]any) []byte {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}
	return body
}

func mustLeaseOne(t testing.TB, store *PostgreSQLQueueStore, request LeaseRequest) *Job {
	t.Helper()
	leased, err := store.Lease(context.Background(), request)
	if err != nil {
		t.Fatalf("Lease(%v): %v", request.Kinds, err)
	}
	if len(leased) != 1 {
		t.Fatalf("Lease(%v) returned %d jobs; want 1", request.Kinds, len(leased))
	}
	return leased[0]
}

func assertLeasedIDs(t testing.TB, jobs []*Job, want []string) {
	t.Helper()
	if len(jobs) != len(want) {
		t.Fatalf("leased job count = %d; want %d (%v)", len(jobs), len(want), want)
	}
	gotIDs := make([]string, 0, len(jobs))
	for index, job := range jobs {
		gotIDs = append(gotIDs, job.ID)
		if job.Status != StatusLeased || job.LeaseToken == "" || job.LeasedBy == "" || job.LeasedUntil == nil {
			t.Fatalf("leased[%d] has invalid lease shape: %#v", index, job)
		}
	}
	if !reflect.DeepEqual(gotIDs, want) {
		t.Fatalf("leased IDs = %v; want %v", gotIDs, want)
	}
}

func queueJobStatus(t testing.TB, db *sql.DB, ws workspace.ID, jobID string) string {
	t.Helper()
	var status string
	if err := db.QueryRowContext(context.Background(),
		`SELECT status FROM queue_jobs WHERE workspace_id = $1 AND id = $2`,
		string(ws),
		jobID,
	).Scan(&status); err != nil {
		t.Fatalf("read queue job status %s: %v", jobID, err)
	}
	return status
}

func expireQueueJobLease(t testing.TB, db *sql.DB, ws workspace.ID, jobID string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`UPDATE queue_jobs SET leased_until = clock_timestamp() - interval '1 second'
		  WHERE workspace_id = $1 AND id = $2 AND status = 'leased'`,
		string(ws), jobID,
	); err != nil {
		t.Fatalf("expire queue job %s: %v", jobID, err)
	}
}

func queueJobPartitionSequence(t testing.TB, db *sql.DB, ws workspace.ID, jobID string) int64 {
	t.Helper()
	var sequence int64
	if err := db.QueryRowContext(context.Background(),
		`SELECT queue_partition_sequence FROM queue_jobs WHERE workspace_id = $1 AND id = $2`,
		string(ws),
		jobID,
	).Scan(&sequence); err != nil {
		t.Fatalf("read queue job partition sequence %s: %v", jobID, err)
	}
	return sequence
}

func countQueueJobsByDedupe(t testing.TB, db *sql.DB, ws workspace.ID, dedupe string) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM queue_jobs WHERE workspace_id = $1 AND dedupe_key = $2`,
		string(ws),
		dedupe,
	).Scan(&count); err != nil {
		t.Fatalf("count queue jobs for dedupe %s: %v", dedupe, err)
	}
	return count
}
