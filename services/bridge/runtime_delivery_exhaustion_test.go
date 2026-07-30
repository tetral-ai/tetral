package agentruntimebridge

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

func TestPostgreSQLRuntimeDeliveryStoreExhaustionFenceSurvivesRepair(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_exhaust_inbox", "thr_exhaust_inbox")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_exhaust_inbox", "prep_sesn_exhaust_inbox")
	seedBridgeAPIEvent(t, admin, "default", "sesn_exhaust_inbox", "thr_exhaust_inbox", "evt_exhaust_inbox", 1, "user.message", `{"type":"user.message"}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", "sesn_exhaust_inbox", "thr_exhaust_inbox", "rin_exhaust_inbox", "messages", `["evt_exhaust_inbox"]`, "accepted", "bind_exhaust_inbox", "pod_exhaust_inbox", 1, 1)
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC) }
	job := exhaustionRuntimeJob("sesn_exhaust_inbox", "thr_exhaust_inbox", "rin_exhaust_inbox", "messages", []string{"evt_exhaust_inbox"})

	finalized, err := store.FinalizeRuntimeDelivery(context.Background(), job, retryableExhaustionResult())
	if err != nil {
		t.Fatalf("FinalizeRuntimeDelivery: %v", err)
	}
	assertExhaustedRuntimeDeliveryResult(t, finalized)
	assertRuntimeExhaustionRows(t, admin, job, "dead_lettered", 1)

	for iteration := 0; iteration < 3; iteration++ {
		replayed, found, err := store.ReplayRuntimeDeliveryFinalization(context.Background(), job)
		if err != nil || !found {
			t.Fatalf("ReplayRuntimeDeliveryFinalization iteration %d = %#v/%v/%v; want stored disposition", iteration, replayed, found, err)
		}
		assertExhaustedRuntimeDeliveryResult(t, replayed)
		finalized, err = store.FinalizeRuntimeDelivery(context.Background(), job, retryableExhaustionResult())
		if err != nil {
			t.Fatalf("FinalizeRuntimeDelivery replay %d: %v", iteration, err)
		}
		assertExhaustedRuntimeDeliveryResult(t, finalized)
		repaired, err := store.RepairRuntimeInbox(context.Background(), "default", 10)
		if err != nil || repaired != 0 {
			t.Fatalf("RepairRuntimeInbox iteration %d = %d/%v; want zero", iteration, repaired, err)
		}
	}
	assertRuntimeExhaustionRows(t, admin, job, "dead_lettered", 1)
	var repairJobs int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND kind='runtime_input'`).Scan(&repairJobs); err != nil {
		t.Fatalf("count repair queue jobs: %v", err)
	}
	if repairJobs != 0 {
		t.Fatalf("repair queue jobs = %d; want zero", repairJobs)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreExhaustionFinalizesEventAnchoredPreInboxOnce(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_exhaust_preinbox", "thr_exhaust_preinbox")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_exhaust_preinbox", "prep_sesn_exhaust_preinbox")
	seedBridgeAPIEvent(t, admin, "default", "sesn_exhaust_preinbox", "thr_exhaust_preinbox", "evt_exhaust_preinbox", 1, "user.message", `{"type":"user.message"}`)
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 1, 1, 0, 0, time.UTC) }
	job := exhaustionRuntimeJob("sesn_exhaust_preinbox", "thr_exhaust_preinbox", "rin_exhaust_preinbox", "messages", []string{"evt_exhaust_preinbox"})

	finalized, err := store.FinalizeRuntimeDelivery(context.Background(), job, retryableExhaustionResult())
	if err != nil {
		t.Fatalf("FinalizeRuntimeDelivery: %v", err)
	}
	assertExhaustedRuntimeDeliveryResult(t, finalized)
	assertRuntimeExhaustionRows(t, admin, job, "", 1)

	replayed, found, err := store.ReplayRuntimeDeliveryFinalization(context.Background(), job)
	if err != nil || !found {
		t.Fatalf("ReplayRuntimeDeliveryFinalization = %#v/%v/%v; want stored disposition", replayed, found, err)
	}
	assertExhaustedRuntimeDeliveryResult(t, replayed)
	if _, err := store.FinalizeRuntimeDelivery(context.Background(), job, retryableExhaustionResult()); err != nil {
		t.Fatalf("FinalizeRuntimeDelivery replay: %v", err)
	}
	assertRuntimeExhaustionRows(t, admin, job, "", 1)
	if repaired, err := store.RepairRuntimeInbox(context.Background(), "default", 10); err != nil || repaired != 0 {
		t.Fatalf("RepairRuntimeInbox = %d/%v; want zero", repaired, err)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreConcurrentEventAnchoredPreInboxFinalizationLinearizes(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_exhaust_concurrent_event"
	const threadID = "thr_exhaust_concurrent_event"
	const eventID = "evt_exhaust_concurrent_event"
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIPreparationReady(t, admin, "default", sessionID, "prep_"+sessionID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 1, "user.message", `{"type":"user.message"}`)
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 1, 1, 30, 0, time.UTC) }
	job := exhaustionRuntimeJob(sessionID, threadID, "rin_exhaust_concurrent_event", "messages", []string{eventID})
	locker, lockerPID := lockPostgreSQLFinalizationFence(t, admin,
		`SELECT id FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND id=$2 FOR UPDATE`,
		sessionID,
		threadID,
	)
	defer func() { _ = locker.Rollback() }()

	results := startConcurrentRuntimeFinalizations(store, job, retryableExhaustionResult())
	waitForPostgreSQLLockWaiters(t, admin, lockerPID, 2)
	if err := locker.Commit(); err != nil {
		t.Fatalf("release thread finalization fence: %v", err)
	}
	for index := 0; index < 2; index++ {
		result := <-results
		if result.err != nil {
			t.Fatalf("concurrent event finalization %d: %v", index, result.err)
		}
		assertExhaustedRuntimeDeliveryResult(t, result.result)
	}
	assertRuntimeExhaustionRows(t, admin, job, "", 1)
}

func TestPostgreSQLRuntimeDeliveryStoreExhaustionFinalizesDeliveringInbox(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_exhaust_delivering", "thr_exhaust_delivering")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_exhaust_delivering", "prep_sesn_exhaust_delivering")
	seedBridgeAPIEvent(t, admin, "default", "sesn_exhaust_delivering", "thr_exhaust_delivering", "evt_exhaust_delivering", 1, "user.message", `{"type":"user.message"}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", "sesn_exhaust_delivering", "thr_exhaust_delivering", "rin_exhaust_delivering", "messages", `["evt_exhaust_delivering"]`, "delivering", "bind_exhaust_delivering", "pod_exhaust_delivering", 1, 1)
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	job := exhaustionRuntimeJob("sesn_exhaust_delivering", "thr_exhaust_delivering", "rin_exhaust_delivering", "messages", []string{"evt_exhaust_delivering"})

	result, err := store.FinalizeRuntimeDelivery(context.Background(), job, retryableExhaustionResult())
	if err != nil {
		t.Fatalf("FinalizeRuntimeDelivery: %v", err)
	}
	assertExhaustedRuntimeDeliveryResult(t, result)
	assertRuntimeExhaustionRows(t, admin, job, "dead_lettered", 1)
}

func TestPostgreSQLRuntimeDeliveryStoreExhaustionCancelledBeforeTransactionWritesNothing(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_exhaust_cancelled", "thr_exhaust_cancelled")
	seedBridgeAPIEvent(t, admin, "default", "sesn_exhaust_cancelled", "thr_exhaust_cancelled", "evt_exhaust_cancelled", 1, "user.message", `{"type":"user.message"}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", "sesn_exhaust_cancelled", "thr_exhaust_cancelled", "rin_exhaust_cancelled", "messages", `["evt_exhaust_cancelled"]`, "accepted", "bind_exhaust_cancelled", "pod_exhaust_cancelled", 1, 1)
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	job := exhaustionRuntimeJob("sesn_exhaust_cancelled", "thr_exhaust_cancelled", "rin_exhaust_cancelled", "messages", []string{"evt_exhaust_cancelled"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := store.FinalizeRuntimeDelivery(ctx, job, retryableExhaustionResult()); err == nil {
		t.Fatal("FinalizeRuntimeDelivery succeeded with cancelled context")
	}
	var inboxStatus string
	var processedAt sql.NullString
	var errorsCount int
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1`, job.RuntimeInputID).Scan(&inboxStatus); err != nil {
		t.Fatalf("read cancelled finalization inbox: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT processed_at FROM session_events WHERE workspace_id='default' AND event_id=$1`, job.EventIDs[0]).Scan(&processedAt); err != nil {
		t.Fatalf("read cancelled finalization event: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.error'`, job.SessionID).Scan(&errorsCount); err != nil {
		t.Fatalf("count cancelled finalization errors: %v", err)
	}
	if inboxStatus != "accepted" || processedAt.Valid || errorsCount != 0 {
		t.Fatalf("cancelled finalization inbox=%q processed=%v errors=%d; want recoverable accepted/unprocessed/no-error", inboxStatus, processedAt.Valid, errorsCount)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreTaskNotificationExhaustionReplaysAndPermitsLaterPollCAS(t *testing.T) {
	for _, test := range []struct {
		name   string
		result RuntimeDeliveryResult
	}{
		{name: "final retryable failure", result: retryableExhaustionResult()},
		{name: "terminal pre-inbox failure", result: RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, ErrorKind: "sandbox_helper_protocol_error", ErrorMessage: "task result invalid"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			name := fmt.Sprintf("%s_%d", "task_exhaust", len(test.name))
			sessionID := "sesn_" + name
			threadID := "thr_" + name
			bindingID := "bind_" + name
			podUID := "pod_" + name
			taskID := "task_" + name
			runtimeInputID := "task_notification:" + taskID
			sourceEventID := "evt_tool_" + name
			queueRuntime, queueAdmin := storagetest.NewPostgreSQLDBWithAdmin(t)
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIPreparationReady(t, admin, "default", sessionID, "prep_"+sessionID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
			seedBridgeAPIBackgroundTask(t, admin, "default", sessionID, threadID, bindingID, taskID, sourceEventID)
			store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
			store.Clock = func() time.Time { return time.Date(2026, 1, 1, 1, 2, 0, 0, time.UTC) }
			job := exhaustionRuntimeJob(sessionID, threadID, runtimeInputID, "task_notification", nil)
			queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(queueRuntime))
			leased := enqueueAndLeaseTaskNotificationExhaustionJob(t, queueStore, job, time.Date(2026, 1, 1, 1, 2, 0, 0, time.UTC))
			job.JobID = leased.ID
			job.LeaseToken = leased.LeaseToken
			job.AttemptCount = int32(leased.AttemptCount)
			job.MaxAttempts = int32(leased.MaxAttempts)

			finalized, err := store.FinalizeRuntimeDelivery(context.Background(), job, test.result)
			if err != nil {
				t.Fatalf("FinalizeRuntimeDelivery: %v", err)
			}
			assertExhaustedTaskNotificationRows(t, admin, job, taskID, 1)
			if test.result.Retryable {
				assertExhaustedRuntimeDeliveryResult(t, finalized)
			} else if finalized != test.result {
				t.Fatalf("terminal pre-inbox result = %#v; want original %#v", finalized, test.result)
			}
			deliverer := &postgresFinalizingDeliverer{store: store, result: test.result}
			runner := &JobRunner{
				Queue:      &fixedLeaseQueueClient{QueueClient: tetralqueue.NewServer(queueStore), job: queueJobProto(leased)},
				Workspaces: staticWorkspaceLister{workspace.DefaultID},
				Deliverer:  deliverer,
			}
			if err := runner.RunOnce(context.Background()); err != nil {
				t.Fatalf("RunOnce task exhaustion replay: %v", err)
			}
			if deliverer.deliveries != 0 {
				t.Fatalf("task Runtime deliveries after exhaustion fence = %d; want zero", deliverer.deliveries)
			}
			assertIndependentQueueExhausted(t, queueAdmin, job.JobID)

			replayed, found, err := store.ReplayRuntimeDeliveryFinalization(context.Background(), job)
			if err != nil || !found {
				t.Fatalf("ReplayRuntimeDeliveryFinalization = %#v/%v/%v; want stored disposition", replayed, found, err)
			}
			assertExhaustedRuntimeDeliveryResult(t, replayed)
			apiStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			apiStore.Clock = store.Clock
			exhaustedReplay, err := apiStore.CommitTaskNotificationResult(context.Background(), bridgeTaskNotificationRequestForTest(
				t,
				bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID),
				runtimeInputID,
				taskID,
				taskNotificationCompletedResult(taskID, sourceEventID),
			))
			if err != nil {
				t.Fatalf("CommitTaskNotificationResult exhausted replay: %v", err)
			}
			if exhaustedReplay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_REJECTED || exhaustedReplay.GetAck().GetErrorCode() != "task_notification_delivery_exhausted" {
				t.Fatalf("exhausted CommitTaskNotificationResult ACK = %#v; want stored exhausted disposition", exhaustedReplay.GetAck())
			}
			assertExhaustedTaskNotificationRows(t, admin, job, taskID, 1)

			pollInputID := "task_notification_poll:" + taskID
			seedBridgeAPITaskNotificationInbox(t, admin, "default", sessionID, threadID, pollInputID, bindingID, podUID)
			settled, err := apiStore.CommitTaskNotificationResult(context.Background(), bridgeTaskNotificationRequestForTest(
				t,
				bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID),
				pollInputID,
				taskID,
				taskNotificationCompletedResult(taskID, sourceEventID),
			))
			if err != nil {
				t.Fatalf("CommitTaskNotificationResult later poll: %v", err)
			}
			if settled.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
				t.Fatalf("later poll ACK = %#v; want committed", settled.GetAck())
			}
			var taskStatus string
			var terminalEventID sql.NullString
			if err := admin.QueryRowContext(context.Background(), `SELECT status, terminal_event_id FROM session_background_tasks WHERE workspace_id='default' AND session_id=$1 AND task_id=$2`, sessionID, taskID).Scan(&taskStatus, &terminalEventID); err != nil {
				t.Fatalf("read later poll task settlement: %v", err)
			}
			if taskStatus != "completed" || !terminalEventID.Valid {
				t.Fatalf("later poll task status=%q terminal=%v; want completed terminal", taskStatus, terminalEventID)
			}
		})
	}
}

func TestPostgreSQLRuntimeDeliveryStoreConcurrentTaskNotificationPreInboxFinalizationLinearizes(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_task_exhaust_concurrent"
	const threadID = "thr_task_exhaust_concurrent"
	const bindingID = "bind_task_exhaust_concurrent"
	const podUID = "pod_task_exhaust_concurrent"
	const taskID = "task_exhaust_concurrent"
	const runtimeInputID = "task_notification:task_exhaust_concurrent"
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIPreparationReady(t, admin, "default", sessionID, "prep_"+sessionID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIBackgroundTask(t, admin, "default", sessionID, threadID, bindingID, taskID, "evt_tool_task_exhaust_concurrent")
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 1, 2, 30, 0, time.UTC) }
	job := exhaustionRuntimeJob(sessionID, threadID, runtimeInputID, "task_notification", nil)
	locker, lockerPID := lockPostgreSQLFinalizationFence(t, admin,
		`SELECT task_id FROM session_background_tasks WHERE workspace_id='default' AND session_id=$1 AND task_id=$2 FOR UPDATE`,
		sessionID,
		taskID,
	)
	defer func() { _ = locker.Rollback() }()

	results := startConcurrentRuntimeFinalizations(store, job, retryableExhaustionResult())
	waitForPostgreSQLLockWaiters(t, admin, lockerPID, 2)
	if err := locker.Commit(); err != nil {
		t.Fatalf("release task finalization fence: %v", err)
	}
	for index := 0; index < 2; index++ {
		result := <-results
		if result.err != nil {
			t.Fatalf("concurrent task finalization %d: %v", index, result.err)
		}
		assertExhaustedRuntimeDeliveryResult(t, result.result)
	}
	assertExhaustedTaskNotificationRows(t, admin, job, taskID, 1)
}

func TestPostgreSQLRuntimeDeliveryStoreTaskNotificationExhaustionSilentlyAcksStaleTasks(t *testing.T) {
	for _, status := range []string{"missing", "completed", "cancelled_by_cleanup"} {
		t.Run(status, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID := "sesn_task_stale_" + status
			threadID := "thr_task_stale_" + status
			taskID := "task_stale_" + status
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIPreparationReady(t, admin, "default", sessionID, "prep_"+sessionID)
			if status != "missing" {
				seedBridgeAPIBackgroundTask(t, admin, "default", sessionID, threadID, "bind_task_stale", taskID, "evt_tool_stale")
				if _, err := admin.ExecContext(context.Background(), `UPDATE session_background_tasks SET status=$3, terminal_event_id=CASE WHEN $3='completed' THEN 'evt_terminal_stale' ELSE NULL END WHERE workspace_id='default' AND session_id=$1 AND task_id=$2`, sessionID, taskID, status); err != nil {
					t.Fatalf("seed stale task status: %v", err)
				}
			}
			store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
			job := exhaustionRuntimeJob(sessionID, threadID, "task_notification:"+taskID, "task_notification", nil)

			result, err := store.FinalizeRuntimeDelivery(context.Background(), job, retryableExhaustionResult())
			if err != nil {
				t.Fatalf("FinalizeRuntimeDelivery: %v", err)
			}
			if result.Status != RuntimeDeliveryDuplicate {
				t.Fatalf("stale result = %#v; want duplicate ACK disposition", result)
			}
			var operations int
			var errorsCount int
			var inboxes int
			if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1 AND operation='commit_task_notification_result'`, sessionID).Scan(&operations); err != nil {
				t.Fatalf("count stale operations: %v", err)
			}
			if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.error'`, sessionID).Scan(&errorsCount); err != nil {
				t.Fatalf("count stale errors: %v", err)
			}
			if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1`, sessionID).Scan(&inboxes); err != nil {
				t.Fatalf("count stale inboxes: %v", err)
			}
			if operations != 0 || errorsCount != 0 || inboxes != 0 {
				t.Fatalf("stale rows operations=%d errors=%d inboxes=%d; want silent ACK", operations, errorsCount, inboxes)
			}
		})
	}
}

func TestPostgreSQLRuntimeDeliveryStoreTaskNotificationExistingInboxExhaustionUsesOperationFence(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_task_exhaust_inbox"
	const threadID = "thr_task_exhaust_inbox"
	const bindingID = "bind_task_exhaust_inbox"
	const podUID = "pod_task_exhaust_inbox"
	const taskID = "task_exhaust_inbox"
	const runtimeInputID = "task_notification:task_exhaust_inbox"
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIPreparationReady(t, admin, "default", sessionID, "prep_"+sessionID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIBackgroundTask(t, admin, "default", sessionID, threadID, bindingID, taskID, "evt_tool_task_exhaust_inbox")
	seedBridgeAPITaskNotificationInbox(t, admin, "default", sessionID, threadID, runtimeInputID, bindingID, podUID)
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	job := exhaustionRuntimeJob(sessionID, threadID, runtimeInputID, "task_notification", nil)

	result, err := store.FinalizeRuntimeDelivery(context.Background(), job, retryableExhaustionResult())
	if err != nil {
		t.Fatalf("FinalizeRuntimeDelivery: %v", err)
	}
	assertExhaustedRuntimeDeliveryResult(t, result)
	var inboxStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1`, runtimeInputID).Scan(&inboxStatus); err != nil {
		t.Fatalf("read task exhaustion inbox: %v", err)
	}
	if inboxStatus != "dead_lettered" {
		t.Fatalf("task exhaustion inbox = %q; want dead_lettered", inboxStatus)
	}
	assertExhaustedTaskNotificationRowsWithInbox(t, admin, job, taskID, 1)
}

func TestPostgreSQLRuntimeDeliveryStoreTaskNotificationTerminalFinalizationWritesOneExhaustionRecord(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_task_terminal_inbox"
	const threadID = "thr_task_terminal_inbox"
	const bindingID = "bind_task_terminal_inbox"
	const podUID = "pod_task_terminal_inbox"
	const taskID = "task_terminal_inbox"
	const runtimeInputID = "task_notification:task_terminal_inbox"
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIPreparationReady(t, admin, "default", sessionID, "prep_"+sessionID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIBackgroundTask(t, admin, "default", sessionID, threadID, bindingID, taskID, "evt_tool_task_terminal_inbox")
	seedBridgeAPITaskNotificationInbox(t, admin, "default", sessionID, threadID, runtimeInputID, bindingID, podUID)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_inbox SET status='delivering' WHERE workspace_id='default' AND runtime_input_id=$1`, runtimeInputID); err != nil {
		t.Fatalf("mark task inbox delivering: %v", err)
	}
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	job := exhaustionRuntimeJob(sessionID, threadID, runtimeInputID, "task_notification", nil)
	terminal := RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, ErrorKind: "runtime_contract_failure", ErrorMessage: "runtime rejected task notification"}

	result, err := store.FinalizeRuntimeDelivery(context.Background(), job, terminal)
	if err != nil {
		t.Fatalf("FinalizeRuntimeDelivery: %v", err)
	}
	if result != terminal {
		t.Fatalf("terminal result = %#v; want original %#v", result, terminal)
	}
	var errorsCount int
	var operations int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.error'`, sessionID).Scan(&errorsCount); err != nil {
		t.Fatalf("count terminal task errors: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1 AND operation='commit_task_notification_result'`, sessionID).Scan(&operations); err != nil {
		t.Fatalf("count terminal task operations: %v", err)
	}
	if errorsCount != 1 || operations != 1 {
		t.Fatalf("terminal task errors=%d exhaustion operations=%d; want one exhaustion event and operation", errorsCount, operations)
	}
}

func TestPostgreSQLJobRunnerExhaustionCrashWindowsConvergeAcrossDatabases(t *testing.T) {
	t.Run("failure before Bridge transaction leaves Queue lease recoverable", func(t *testing.T) {
		bridgeRuntime, bridgeAdmin := storagetest.NewPostgreSQLDBWithAdmin(t)
		queueRuntime, queueAdmin := storagetest.NewPostgreSQLDBWithAdmin(t)
		job := seedCrossDatabaseExhaustionFixture(t, bridgeAdmin, "before_bridge", true)
		bridgeStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(bridgeRuntime), 9090)
		queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(queueRuntime))
		leased := enqueueAndLeaseExhaustionJob(t, queueStore, job, time.Date(2026, 1, 1, 1, 59, 0, 0, time.UTC))
		job.JobID = leased.ID
		job.LeaseToken = leased.LeaseToken
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		if _, err := bridgeStore.FinalizeRuntimeDelivery(ctx, job, retryableExhaustionResult()); err == nil {
			t.Fatal("FinalizeRuntimeDelivery before-transaction fault succeeded")
		}
		var inboxStatus string
		var processedAt sql.NullString
		var errorsCount int
		if err := bridgeAdmin.QueryRowContext(context.Background(), `SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1`, job.RuntimeInputID).Scan(&inboxStatus); err != nil {
			t.Fatalf("read pre-transaction inbox: %v", err)
		}
		if err := bridgeAdmin.QueryRowContext(context.Background(), `SELECT processed_at FROM session_events WHERE workspace_id='default' AND event_id=$1`, job.EventIDs[0]).Scan(&processedAt); err != nil {
			t.Fatalf("read pre-transaction event: %v", err)
		}
		if err := bridgeAdmin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.error'`, job.SessionID).Scan(&errorsCount); err != nil {
			t.Fatalf("count pre-transaction errors: %v", err)
		}
		var queueStatus string
		var attemptCount int
		var maxAttempts int
		if err := queueAdmin.QueryRowContext(context.Background(), `SELECT status, attempt_count, max_attempts FROM queue_jobs WHERE workspace_id='default' AND id=$1`, job.JobID).Scan(&queueStatus, &attemptCount, &maxAttempts); err != nil {
			t.Fatalf("read recoverable Queue lease: %v", err)
		}
		if inboxStatus != "accepted" || processedAt.Valid || errorsCount != 0 || queueStatus != queue.StatusLeased || attemptCount != 1 || maxAttempts != 1 {
			t.Fatalf("pre-transaction Bridge/Queue state inbox=%q processed=%v errors=%d queue=%q attempts=%d/%d; want accepted/unprocessed/zero/leased/1/1", inboxStatus, processedAt.Valid, errorsCount, queueStatus, attemptCount, maxAttempts)
		}
	})

	t.Run("Bridge commit before Queue dead-letter", func(t *testing.T) {
		for _, arm := range []struct {
			name          string
			existingInbox bool
			wantInbox     string
		}{
			{name: "existing inbox", existingInbox: true, wantInbox: "dead_lettered"},
			{name: "event-anchored pre-inbox", existingInbox: false},
		} {
			t.Run(arm.name, func(t *testing.T) {
				bridgeRuntime, bridgeAdmin := storagetest.NewPostgreSQLDBWithAdmin(t)
				queueRuntime, queueAdmin := storagetest.NewPostgreSQLDBWithAdmin(t)
				suffix := strings.NewReplacer("-", "_", " ", "_").Replace(arm.name)
				job := seedCrossDatabaseExhaustionFixture(t, bridgeAdmin, suffix, arm.existingInbox)
				bridgeStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(bridgeRuntime), 9090)
				bridgeStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 2, 0, 0, 0, time.UTC) }
				queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(queueRuntime))
				leased := enqueueAndLeaseExhaustionJob(t, queueStore, job, time.Date(2026, 1, 1, 2, 0, 0, 0, time.UTC))
				job.JobID = leased.ID
				job.LeaseToken = leased.LeaseToken
				job.AttemptCount = int32(leased.AttemptCount)
				job.MaxAttempts = int32(leased.MaxAttempts)

				if _, err := bridgeStore.FinalizeRuntimeDelivery(context.Background(), job, retryableExhaustionResult()); err != nil {
					t.Fatalf("commit Bridge exhaustion fence: %v", err)
				}
				deliverer := &postgresFinalizingDeliverer{store: bridgeStore, result: retryableExhaustionResult()}
				queueServer := tetralqueue.NewServer(queueStore)
				queueClient := &fixedLeaseQueueClient{QueueClient: queueServer, job: queueJobProto(leased)}
				runner := &JobRunner{Queue: queueClient, Workspaces: staticWorkspaceLister{workspace.DefaultID}, Deliverer: deliverer}

				if err := runner.RunOnce(context.Background()); err != nil {
					t.Fatalf("RunOnce after Bridge crash boundary: %v", err)
				}
				if deliverer.deliveries != 0 {
					t.Fatalf("Runtime deliveries after Bridge fence = %d; want zero", deliverer.deliveries)
				}
				assertCrossDatabaseExhaustionConverged(t, bridgeAdmin, queueAdmin, bridgeStore, job, arm.wantInbox)
			})
		}
	})

	t.Run("Queue dead-letter commit with response loss", func(t *testing.T) {
		bridgeRuntime, bridgeAdmin := storagetest.NewPostgreSQLDBWithAdmin(t)
		queueRuntime, queueAdmin := storagetest.NewPostgreSQLDBWithAdmin(t)
		job := seedCrossDatabaseExhaustionFixture(t, bridgeAdmin, "queue_response_loss", true)
		bridgeStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(bridgeRuntime), 9090)
		bridgeStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 2, 1, 0, 0, time.UTC) }
		queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(queueRuntime))
		enqueueExhaustionJob(t, queueStore, job, time.Date(2026, 1, 1, 2, 1, 0, 0, time.UTC))
		deliverer := &postgresFinalizingDeliverer{store: bridgeStore, result: retryableExhaustionResult()}
		queueClient := &deadLetterResponseLossQueueClient{QueueClient: tetralqueue.NewServer(queueStore)}
		runner := &JobRunner{Queue: queueClient, Workspaces: staticWorkspaceLister{workspace.DefaultID}, Deliverer: deliverer}

		if err := runner.RunOnce(context.Background()); err == nil || !errors.Is(err, errSyntheticQueueResponseLoss) {
			t.Fatalf("RunOnce error = %v; want synthetic post-commit response loss", err)
		}
		if deliverer.deliveries != 1 {
			t.Fatalf("Runtime deliveries before Queue response loss = %d; want one", deliverer.deliveries)
		}
		job.JobID = queueClient.deadLetteredJobID
		assertCrossDatabaseExhaustionConverged(t, bridgeAdmin, queueAdmin, bridgeStore, job, "dead_lettered")
		if err := runner.RunOnce(context.Background()); err != nil {
			t.Fatalf("RunOnce after terminal Queue row: %v", err)
		}
		if deliverer.deliveries != 1 {
			t.Fatalf("Runtime deliveries after terminal Queue row = %d; want unchanged one", deliverer.deliveries)
		}
	})
}

func exhaustionRuntimeJob(sessionID string, threadID string, runtimeInputID string, inputKind string, eventIDs []string) RuntimeJob {
	return RuntimeJob{
		JobID:                "qjob_" + runtimeInputID,
		LeaseToken:           "lease_" + runtimeInputID,
		Kind:                 queue.KindRuntimeInput,
		WorkspaceID:          "default",
		SessionID:            sessionID,
		SessionThreadID:      threadID,
		RuntimeInputID:       runtimeInputID,
		PreparationAttemptID: "prep_" + sessionID,
		EventIDs:             append([]string(nil), eventIDs...),
		SequenceFrom:         1,
		SequenceTo:           1,
		InputKind:            inputKind,
		AttemptCount:         2,
		MaxAttempts:          2,
	}
}

func retryableExhaustionResult() RuntimeDeliveryResult {
	return RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, Retryable: true, ErrorKind: "runtime_busy", ErrorMessage: "runtime busy"}
}

func assertExhaustedRuntimeDeliveryResult(t *testing.T, result RuntimeDeliveryResult) {
	t.Helper()
	if result.Status != RuntimeDeliveryRejected || result.Retryable || result.ErrorKind != "runtime_delivery_exhausted" {
		t.Fatalf("finalized result = %#v; want terminal runtime_delivery_exhausted", result)
	}
}

func assertRuntimeExhaustionRows(t *testing.T, db *sql.DB, job RuntimeJob, inboxStatus string, wantErrors int) {
	t.Helper()
	var inboxCount int
	var storedInboxStatus string
	if err := db.QueryRowContext(context.Background(), `SELECT count(*), COALESCE(MAX(status), '') FROM session_runtime_inbox WHERE workspace_id=$1 AND runtime_input_id=$2`, job.WorkspaceID, job.RuntimeInputID).Scan(&inboxCount, &storedInboxStatus); err != nil {
		t.Fatalf("read exhaustion inbox: %v", err)
	}
	if (inboxStatus == "" && inboxCount != 0) || (inboxStatus != "" && (inboxCount != 1 || storedInboxStatus != inboxStatus)) {
		t.Fatalf("exhaustion inbox count/status = %d/%q; want %q", inboxCount, storedInboxStatus, inboxStatus)
	}
	var errorCount int
	var errorType string
	var retryStatus string
	if err := db.QueryRowContext(context.Background(), `SELECT count(*), COALESCE(MAX(payload_json::jsonb #>> '{error,type}'), ''), COALESCE(MAX(payload_json::jsonb #>> '{error,retry_status,type}'), '') FROM session_events WHERE workspace_id=$1 AND session_id=$2 AND type='session.error'`, job.WorkspaceID, job.SessionID).Scan(&errorCount, &errorType, &retryStatus); err != nil {
		t.Fatalf("read exhaustion errors: %v", err)
	}
	if errorCount != wantErrors || errorType != "unknown_error" || retryStatus != "exhausted" {
		t.Fatalf("exhaustion errors count/type/retry = %d/%q/%q; want %d/unknown_error/exhausted", errorCount, errorType, retryStatus, wantErrors)
	}
	var errorStreamChanges int
	if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM session_event_stream_changes changes JOIN session_events event ON event.workspace_id=changes.workspace_id AND event.event_id=changes.event_id WHERE event.workspace_id=$1 AND event.session_id=$2 AND event.type='session.error'`, job.WorkspaceID, job.SessionID).Scan(&errorStreamChanges); err != nil {
		t.Fatalf("count exhaustion error stream changes: %v", err)
	}
	if errorStreamChanges != wantErrors {
		t.Fatalf("exhaustion error stream changes = %d; want %d", errorStreamChanges, wantErrors)
	}
	for _, eventID := range job.EventIDs {
		var processedAt sql.NullString
		var revision int64
		if err := db.QueryRowContext(context.Background(), `SELECT processed_at, revision FROM session_events WHERE workspace_id=$1 AND event_id=$2`, job.WorkspaceID, eventID).Scan(&processedAt, &revision); err != nil {
			t.Fatalf("read exhaustion input event %s: %v", eventID, err)
		}
		if !processedAt.Valid || revision != 2 {
			t.Fatalf("exhaustion input event %s processed=%v revision=%d; want true/2", eventID, processedAt.Valid, revision)
		}
		var processingChanges int
		if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM session_event_stream_changes WHERE workspace_id=$1 AND event_id=$2 AND revision=2`, job.WorkspaceID, eventID).Scan(&processingChanges); err != nil {
			t.Fatalf("count exhaustion input stream changes %s: %v", eventID, err)
		}
		if processingChanges != 1 {
			t.Fatalf("exhaustion input stream changes %s = %d; want one revision-2 change", eventID, processingChanges)
		}
	}
	var runtimeStatusRows int
	if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_status WHERE workspace_id=$1 AND session_id=$2`, job.WorkspaceID, job.SessionID).Scan(&runtimeStatusRows); err != nil {
		t.Fatalf("count exhaustion runtime status rows: %v", err)
	}
	if runtimeStatusRows != 0 {
		t.Fatalf("exhaustion runtime status rows = %d; want no status mutation", runtimeStatusRows)
	}
}

func assertExhaustedTaskNotificationRows(t *testing.T, db *sql.DB, job RuntimeJob, taskID string, wantErrors int) {
	t.Helper()
	var taskStatus string
	var terminalEventID sql.NullString
	if err := db.QueryRowContext(context.Background(), `SELECT status, terminal_event_id FROM session_background_tasks WHERE workspace_id=$1 AND session_id=$2 AND task_id=$3`, job.WorkspaceID, job.SessionID, taskID).Scan(&taskStatus, &terminalEventID); err != nil {
		t.Fatalf("read exhausted task: %v", err)
	}
	var operationCount int
	var operationStatus string
	var operationError string
	if err := db.QueryRowContext(context.Background(), `SELECT count(*), COALESCE(MAX(ack_status), ''), COALESCE(MAX(error_code), '') FROM session_bridge_operations WHERE workspace_id=$1 AND session_id=$2 AND operation='commit_task_notification_result' AND runtime_input_id=$3`, job.WorkspaceID, job.SessionID, job.RuntimeInputID).Scan(&operationCount, &operationStatus, &operationError); err != nil {
		t.Fatalf("read exhausted task operation: %v", err)
	}
	var errorCount int
	if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events WHERE workspace_id=$1 AND session_id=$2 AND type='session.error' AND payload_json::jsonb #>> '{error,retry_status,type}'='exhausted'`, job.WorkspaceID, job.SessionID).Scan(&errorCount); err != nil {
		t.Fatalf("count exhausted task errors: %v", err)
	}
	var inboxCount int
	if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_inbox WHERE workspace_id=$1 AND runtime_input_id=$2`, job.WorkspaceID, job.RuntimeInputID).Scan(&inboxCount); err != nil {
		t.Fatalf("count exhausted task inbox: %v", err)
	}
	if taskStatus != "running" || terminalEventID.Valid || operationCount != 1 || operationStatus != "rejected" || operationError != "task_notification_delivery_exhausted" || errorCount != wantErrors || inboxCount != 0 {
		t.Fatalf("task exhaustion status=%q terminal=%v op=%d/%q/%q errors=%d inbox=%d; want running/no-terminal one rejected exhausted op, %d error, no inbox", taskStatus, terminalEventID.Valid, operationCount, operationStatus, operationError, errorCount, inboxCount, wantErrors)
	}
}

func assertExhaustedTaskNotificationRowsWithInbox(t *testing.T, db *sql.DB, job RuntimeJob, taskID string, wantErrors int) {
	t.Helper()
	var taskStatus string
	var terminalEventID sql.NullString
	var operationCount int
	var errorCount int
	if err := db.QueryRowContext(context.Background(), `SELECT status, terminal_event_id FROM session_background_tasks WHERE workspace_id=$1 AND session_id=$2 AND task_id=$3`, job.WorkspaceID, job.SessionID, taskID).Scan(&taskStatus, &terminalEventID); err != nil {
		t.Fatalf("read exhausted task with inbox: %v", err)
	}
	if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM session_bridge_operations WHERE workspace_id=$1 AND session_id=$2 AND operation='commit_task_notification_result' AND runtime_input_id=$3 AND error_code='task_notification_delivery_exhausted'`, job.WorkspaceID, job.SessionID, job.RuntimeInputID).Scan(&operationCount); err != nil {
		t.Fatalf("count exhausted task operation with inbox: %v", err)
	}
	if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events WHERE workspace_id=$1 AND session_id=$2 AND type='session.error' AND payload_json::jsonb #>> '{error,retry_status,type}'='exhausted'`, job.WorkspaceID, job.SessionID).Scan(&errorCount); err != nil {
		t.Fatalf("count exhausted task errors with inbox: %v", err)
	}
	if taskStatus != "running" || terminalEventID.Valid || operationCount != 1 || errorCount != wantErrors {
		t.Fatalf("task exhaustion with inbox status=%q terminal=%v operations=%d errors=%d; want running/no-terminal/one/%d", taskStatus, terminalEventID.Valid, operationCount, errorCount, wantErrors)
	}
}

func taskNotificationCompletedResult(taskID string, sourceEventID string) string {
	return fmt.Sprintf(`{"task_id":%q,"source_tool_use_event_id":%q,"status":"completed","stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false},"exit_code":0}`, taskID, sourceEventID)
}

type postgresFinalizingDeliverer struct {
	store      *PostgreSQLRuntimeDeliveryStore
	result     RuntimeDeliveryResult
	deliveries int
}

type concurrentRuntimeFinalizationResult struct {
	result RuntimeDeliveryResult
	err    error
}

func startConcurrentRuntimeFinalizations(store *PostgreSQLRuntimeDeliveryStore, job RuntimeJob, result RuntimeDeliveryResult) <-chan concurrentRuntimeFinalizationResult {
	start := make(chan struct{})
	results := make(chan concurrentRuntimeFinalizationResult, 2)
	for index := 0; index < 2; index++ {
		go func() {
			<-start
			finalized, err := store.FinalizeRuntimeDelivery(context.Background(), job, result)
			results <- concurrentRuntimeFinalizationResult{result: finalized, err: err}
		}()
	}
	close(start)
	return results
}

func lockPostgreSQLFinalizationFence(t *testing.T, db *sql.DB, query string, args ...any) (*sql.Tx, int) {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin finalization fence transaction: %v", err)
	}
	var lockedID string
	if err := tx.QueryRowContext(context.Background(), query, args...).Scan(&lockedID); err != nil {
		_ = tx.Rollback()
		t.Fatalf("lock finalization fence row: %v", err)
	}
	var backendPID int
	if err := tx.QueryRowContext(context.Background(), `SELECT pg_backend_pid()`).Scan(&backendPID); err != nil {
		_ = tx.Rollback()
		t.Fatalf("read finalization fence backend: %v", err)
	}
	return tx, backendPID
}

func waitForPostgreSQLLockWaiters(t *testing.T, db *sql.DB, blockerPID int, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiters int
		if err := db.QueryRowContext(context.Background(),
			`WITH RECURSIVE waiters(pid) AS (
				SELECT pid
				  FROM pg_stat_activity
				 WHERE $1 = ANY(pg_blocking_pids(pid))
				UNION
				SELECT activity.pid
				  FROM pg_stat_activity activity
				  JOIN waiters blocker ON blocker.pid = ANY(pg_blocking_pids(activity.pid))
			)
			SELECT count(*) FROM waiters`,
			blockerPID,
		).Scan(&waiters); err != nil {
			t.Fatalf("read PostgreSQL finalization waiters: %v", err)
		}
		if waiters >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("PostgreSQL finalization waiters did not reach %d for blocker %d", want, blockerPID)
}

func (d *postgresFinalizingDeliverer) DeliverRuntimeJob(context.Context, RuntimeJob) (RuntimeDeliveryResult, error) {
	d.deliveries++
	return d.result, nil
}

func (d *postgresFinalizingDeliverer) FinalizeRuntimeDelivery(ctx context.Context, job RuntimeJob, result RuntimeDeliveryResult) (RuntimeDeliveryResult, error) {
	return d.store.FinalizeRuntimeDelivery(ctx, job, result)
}

func (d *postgresFinalizingDeliverer) ReplayRuntimeDeliveryFinalization(ctx context.Context, job RuntimeJob) (RuntimeDeliveryResult, bool, error) {
	return d.store.ReplayRuntimeDeliveryFinalization(ctx, job)
}

func (d *postgresFinalizingDeliverer) ResolveRuntimeInputSeal(ctx context.Context, job RuntimeJob) (string, error) {
	return d.store.ResolveRuntimeInputSeal(ctx, job)
}

type fixedLeaseQueueClient struct {
	QueueClient
	job    *queuev1.QueueJob
	leased bool
}

func (c *fixedLeaseQueueClient) Lease(context.Context, *queuev1.LeaseRequest) (*queuev1.LeaseResponse, error) {
	if c.leased {
		return &queuev1.LeaseResponse{}, nil
	}
	c.leased = true
	return &queuev1.LeaseResponse{Jobs: []*queuev1.QueueJob{c.job}}, nil
}

var errSyntheticQueueResponseLoss = errors.New("synthetic queue response loss")

type deadLetterResponseLossQueueClient struct {
	QueueClient
	deadLetteredJobID string
}

func (c *deadLetterResponseLossQueueClient) DeadLetter(ctx context.Context, request *queuev1.DeadLetterRequest) (*queuev1.TransitionResponse, error) {
	response, err := c.QueueClient.DeadLetter(ctx, request)
	if err != nil {
		return response, err
	}
	c.deadLetteredJobID = request.GetJobId()
	return nil, errSyntheticQueueResponseLoss
}

func seedCrossDatabaseExhaustionFixture(t *testing.T, bridgeAdmin *sql.DB, suffix string, existingInbox bool) RuntimeJob {
	t.Helper()
	sessionID := "sesn_cross_" + suffix
	threadID := "thr_cross_" + suffix
	runtimeInputID := "rin_cross_" + suffix
	eventID := "evt_cross_" + suffix
	seedBridgeAPISession(t, bridgeAdmin, "default", sessionID, threadID)
	seedBridgeAPIPreparationReady(t, bridgeAdmin, "default", sessionID, "prep_"+sessionID)
	seedBridgeAPIEvent(t, bridgeAdmin, "default", sessionID, threadID, eventID, 1, "user.message", `{"type":"user.message"}`)
	if existingInbox {
		seedBridgeAPIRuntimeInbox(t, bridgeAdmin, "default", sessionID, threadID, runtimeInputID, "messages", fmt.Sprintf(`[%q]`, eventID), "accepted", "bind_cross", "pod_cross", 1, 1)
	}
	return exhaustionRuntimeJob(sessionID, threadID, runtimeInputID, "messages", []string{eventID})
}

func enqueueExhaustionJob(t *testing.T, store *queue.PostgreSQLQueueStore, job RuntimeJob, now time.Time) *queue.Job {
	t.Helper()
	payload := fmt.Sprintf(`{"workspace_id":"default","session_id":%q,"session_thread_id":%q,"runtime_input_id":%q,"event_ids":[%q],"sequence_from":1,"sequence_to":1,"input_kind":"messages","preparation_attempt_id":%q}`, job.SessionID, job.SessionThreadID, job.RuntimeInputID, job.EventIDs[0], job.PreparationAttemptID)
	queued, err := store.Enqueue(context.Background(), queue.EnqueueRequest{
		WorkspaceID:    workspace.DefaultID,
		Kind:           queue.KindRuntimeInput,
		PartitionKey:   queue.FormatSessionPartitionKey(workspace.DefaultID, job.SessionID),
		DedupeKey:      queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, job.SessionID, job.RuntimeInputID),
		PayloadVersion: 1,
		PayloadJSON:    []byte(payload),
		MaxAttempts:    1,
		Now:            now,
	})
	if err != nil {
		t.Fatalf("enqueue exhaustion job: %v", err)
	}
	return queued
}

func enqueueAndLeaseExhaustionJob(t *testing.T, store *queue.PostgreSQLQueueStore, job RuntimeJob, now time.Time) *queue.Job {
	t.Helper()
	enqueueExhaustionJob(t, store, job, now)
	leased, err := store.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID:   workspace.DefaultID,
		Kinds:         []string{queue.KindRuntimeInput},
		LeaseOwner:    "bridge-test",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now,
	})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease exhaustion job = %d/%v; want one", len(leased), err)
	}
	return leased[0]
}

func enqueueAndLeaseTaskNotificationExhaustionJob(t *testing.T, store *queue.PostgreSQLQueueStore, job RuntimeJob, now time.Time) *queue.Job {
	t.Helper()
	payload := fmt.Sprintf(`{"workspace_id":"default","session_id":%q,"session_thread_id":%q,"runtime_input_id":%q,"event_ids":[],"input_kind":"task_notification","preparation_attempt_id":%q}`, job.SessionID, job.SessionThreadID, job.RuntimeInputID, job.PreparationAttemptID)
	queued, err := store.Enqueue(context.Background(), queue.EnqueueRequest{
		WorkspaceID:    workspace.DefaultID,
		Kind:           queue.KindRuntimeInput,
		PartitionKey:   queue.FormatSessionPartitionKey(workspace.DefaultID, job.SessionID),
		DedupeKey:      queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, job.SessionID, job.RuntimeInputID),
		PayloadVersion: 1,
		PayloadJSON:    []byte(payload),
		MaxAttempts:    1,
		Now:            now,
	})
	if err != nil {
		t.Fatalf("enqueue task notification exhaustion job: %v", err)
	}
	leased, err := store.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID:   workspace.DefaultID,
		Kinds:         []string{queue.KindRuntimeInput},
		LeaseOwner:    "bridge-task-test",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now,
	})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease task notification exhaustion job = %d/%v; want one (enqueued %s)", len(leased), err, queued.ID)
	}
	return leased[0]
}

func queueJobProto(job *queue.Job) *queuev1.QueueJob {
	return &queuev1.QueueJob{
		Id:             job.ID,
		WorkspaceId:    string(job.WorkspaceID),
		Kind:           job.Kind,
		PartitionKey:   job.PartitionKey,
		DedupeKey:      job.DedupeKey,
		PayloadVersion: int32(job.PayloadVersion),
		PayloadJson:    string(job.PayloadJSON),
		LeaseToken:     job.LeaseToken,
		AttemptCount:   int32(job.AttemptCount),
		MaxAttempts:    int32(job.MaxAttempts),
	}
}

func assertCrossDatabaseExhaustionConverged(t *testing.T, bridgeAdmin *sql.DB, queueAdmin *sql.DB, store *PostgreSQLRuntimeDeliveryStore, job RuntimeJob, wantInbox string) {
	t.Helper()
	assertRuntimeExhaustionRows(t, bridgeAdmin, job, wantInbox, 1)
	assertIndependentQueueExhausted(t, queueAdmin, job.JobID)
	for iteration := 0; iteration < 3; iteration++ {
		if _, err := store.FinalizeRuntimeDelivery(context.Background(), job, retryableExhaustionResult()); err != nil {
			t.Fatalf("replay finalization %d: %v", iteration, err)
		}
		if repaired, err := store.RepairRuntimeInbox(context.Background(), "default", 10); err != nil || repaired != 0 {
			t.Fatalf("repair after convergence %d = %d/%v; want zero", iteration, repaired, err)
		}
	}
	assertRuntimeExhaustionRows(t, bridgeAdmin, job, wantInbox, 1)
}

func assertIndependentQueueExhausted(t *testing.T, queueAdmin *sql.DB, jobID string) {
	t.Helper()
	var queueStatus string
	var attemptCount int
	var maxAttempts int
	if err := queueAdmin.QueryRowContext(context.Background(), `SELECT status, attempt_count, max_attempts FROM queue_jobs WHERE workspace_id='default' AND id=$1`, jobID).Scan(&queueStatus, &attemptCount, &maxAttempts); err != nil {
		t.Fatalf("read independent Queue row: %v", err)
	}
	if queueStatus != queue.StatusDeadLettered || attemptCount != 1 || maxAttempts != 1 {
		t.Fatalf("Queue status/attempts = %q/%d/%d; want dead_lettered/1/1", queueStatus, attemptCount, maxAttempts)
	}
}
