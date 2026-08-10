package tetralsandbox

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
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

func TestPostgreSQLBackgroundTaskReconcileAdvancesOneGeneration(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	seedBackgroundTaskFromExecution(t, runtimeDB, adminDB)
	client := dbconnect.NewClientForTesting(runtimeDB)
	store := NewPostgreSQLSandboxBackgroundCommandStore(client)
	queueStore := queue.NewPostgreSQLStore(client)
	var jobID string
	if err := adminDB.QueryRow(`SELECT id FROM queue_jobs WHERE workspace_id='ws_execution_store'
		AND kind='sandbox_background_reconcile' AND status='pending'`).Scan(&jobID); err != nil {
		t.Fatalf("read reconcile Queue job: %v", err)
	}
	var work SandboxBackgroundTaskWork
	firstCtx, secondCtx, _, _ := supersedeExistingSandboxQueueLease(t, queueStore, adminDB,
		"ws_execution_store", queue.KindSandboxBackgroundReconcile, jobID,
		func(ctx context.Context, transport *queuev1.QueueJob) {
			job, err := DecodeSandboxBackgroundReconcileJob(transport)
			if err != nil {
				t.Fatalf("DecodeSandboxBackgroundReconcileJob: %v", err)
			}
			var current bool
			work, current, err = store.LoadReconcile(ctx, job)
			if err != nil || !current {
				t.Fatalf("LoadReconcile = current %t err %v", current, err)
			}
		})
	now := time.Now().UTC()
	nextPoll := now.Add(time.Minute)
	if err := store.AdvanceReconcile(firstCtx, work, sandboxdriver.CommandResult{ResultJSON: `{"status":"running"}`}, now, nextPoll); !errors.Is(err, errQueueLeaseLost) {
		t.Fatalf("superseded AdvanceReconcile error = %v; want Queue authority loss", err)
	}
	var generation int64
	if err := adminDB.QueryRow(`SELECT reconcile_generation FROM session_background_tasks
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store' AND task_id='task_execution'`).Scan(&generation); err != nil {
		t.Fatalf("read generation after stale advance: %v", err)
	}
	if generation != 1 {
		t.Fatalf("generation after stale advance = %d; want 1", generation)
	}
	if err := store.AdvanceReconcile(secondCtx, work, sandboxdriver.CommandResult{ResultJSON: `{"status":"running"}`}, now, nextPoll); err != nil {
		t.Fatalf("successor AdvanceReconcile: %v", err)
	}
	var storedNext time.Time
	if err := adminDB.QueryRow(`SELECT reconcile_generation, next_poll_at FROM session_background_tasks
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store' AND task_id='task_execution'`).Scan(&generation, &storedNext); err != nil {
		t.Fatalf("read task generation: %v", err)
	}
	if generation != 2 || storedNext.Sub(nextPoll).Abs() >= time.Microsecond {
		t.Fatalf("generation/next = %d/%v; want 2/%v", generation, storedNext, nextPoll)
	}
	var jobs int
	var createdAt, availableAt time.Time
	if err := adminDB.QueryRow(`SELECT count(*) OVER (), created_at, available_at FROM queue_jobs WHERE workspace_id='ws_execution_store'
		AND kind='sandbox_background_reconcile' AND dedupe_key LIKE '%:2'`).Scan(&jobs, &createdAt, &availableAt); err != nil {
		t.Fatalf("read successor: %v", err)
	}
	if jobs != 1 {
		t.Fatalf("successor jobs = %d; want 1", jobs)
	}
	if createdAt.Sub(now).Abs() >= time.Microsecond || availableAt.Sub(nextPoll).Abs() >= time.Microsecond {
		t.Fatalf("successor times = created %v available %v; want %v/%v", createdAt, availableAt, now, nextPoll)
	}
}

func TestPostgreSQLBackgroundReconcileExhaustionIgnoresPoisonPayload(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	seedBackgroundTaskFromExecution(t, runtimeDB, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	store := NewPostgreSQLSandboxBackgroundCommandStore(dbconnect.NewClientForTesting(runtimeDB))
	now := time.Now().UTC()
	job := &queuev1.QueueJob{
		Id: "qjob_poison_reconcile", WorkspaceId: "ws_execution_store", Kind: queue.KindSandboxBackgroundReconcile,
		PartitionKey: queue.FormatSandboxBackgroundPartitionKey("ws_execution_store", "sesn_execution_store", "task_execution"),
		DedupeKey:    queue.FormatSandboxBackgroundReconcileDedupeKey("ws_execution_store", "sesn_execution_store", "task_execution", 1),
		PayloadJson:  `{not-json`, LeaseToken: "lease_poison_reconcile",
	}
	if err := store.FinalizeReconcileExhaustion(ctx, job, now); err != nil {
		t.Fatalf("FinalizeReconcileExhaustion: %v", err)
	}
	var generation int64
	if err := adminDB.QueryRow(`SELECT reconcile_generation FROM session_background_tasks
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store' AND task_id='task_execution'`).Scan(&generation); err != nil {
		t.Fatalf("read task generation: %v", err)
	}
	if generation != 2 {
		t.Fatalf("reconcile generation = %d; want poison-payload exhaustion to advance to 2", generation)
	}
}

func TestPostgreSQLBackgroundTaskTerminalCASCreatesBindingNeutralNotification(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	seedBackgroundTaskFromExecution(t, runtimeDB, adminDB)
	client := dbconnect.NewClientForTesting(runtimeDB)
	store := NewPostgreSQLSandboxBackgroundCommandStore(client)
	queueStore := queue.NewPostgreSQLStore(client)
	var jobID string
	if err := adminDB.QueryRow(`SELECT id FROM queue_jobs WHERE workspace_id='ws_execution_store'
		AND kind='sandbox_background_reconcile' AND status='pending'`).Scan(&jobID); err != nil {
		t.Fatalf("read reconcile Queue job: %v", err)
	}
	var work SandboxBackgroundTaskWork
	firstCtx, secondCtx, _, _ := supersedeExistingSandboxQueueLease(t, queueStore, adminDB,
		"ws_execution_store", queue.KindSandboxBackgroundReconcile, jobID,
		func(ctx context.Context, transport *queuev1.QueueJob) {
			job, err := DecodeSandboxBackgroundReconcileJob(transport)
			if err != nil {
				t.Fatalf("DecodeSandboxBackgroundReconcileJob: %v", err)
			}
			var current bool
			work, current, err = store.LoadReconcile(ctx, job)
			if err != nil || !current {
				t.Fatalf("LoadReconcile = current %t err %v", current, err)
			}
		})
	result := sandboxdriver.CommandResult{ResultJSON: `{"status":"completed","result":{"stdout":"done"}}`, TerminalStatus: "completed"}
	wantStoredResult := normalizeSandboxProviderResult(result.ResultJSON)
	if err := store.SettleTask(firstCtx, work, result, time.Now().UTC()); !errors.Is(err, errQueueLeaseLost) {
		t.Fatalf("superseded SettleTask error = %v; want Queue authority loss", err)
	}
	var taskStatus string
	var inboxCount int
	if err := adminDB.QueryRow(`SELECT status FROM session_background_tasks
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store' AND task_id='task_execution'`).Scan(&taskStatus); err != nil {
		t.Fatalf("read task after stale settlement: %v", err)
	}
	if err := adminDB.QueryRow(`SELECT count(*) FROM session_runtime_inbox
		WHERE workspace_id='ws_execution_store' AND runtime_input_id='task_notification:task_execution'`).Scan(&inboxCount); err != nil {
		t.Fatalf("count notification after stale settlement: %v", err)
	}
	if taskStatus != "running" || inboxCount != 0 {
		t.Fatalf("task/inbox after stale settlement = %s/%d; want running/0", taskStatus, inboxCount)
	}
	if err := store.SettleTask(secondCtx, work, result, time.Now().UTC()); err != nil {
		t.Fatalf("successor SettleTask: %v", err)
	}
	var status, storedResult, digest string
	if err := adminDB.QueryRow(`SELECT status, terminal_result_json, terminal_result_digest FROM session_background_tasks
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store' AND task_id='task_execution'`).Scan(&status, &storedResult, &digest); err != nil {
		t.Fatalf("read terminal task: %v", err)
	}
	if status != "completed" || storedResult != wantStoredResult || digest == "" {
		t.Fatalf("terminal task = status %q result %q digest %q", status, storedResult, digest)
	}
	var inboxStatus string
	var bindingID sql.NullString
	if err := adminDB.QueryRow(`SELECT status, binding_id FROM session_runtime_inbox
		WHERE workspace_id='ws_execution_store' AND runtime_input_id='task_notification:task_execution'`).Scan(&inboxStatus, &bindingID); err != nil {
		t.Fatalf("read notification inbox: %v", err)
	}
	if inboxStatus != "queued" || bindingID.Valid {
		t.Fatalf("notification inbox = status %q binding %v; want queued/unbound", inboxStatus, bindingID)
	}
	var payload string
	if err := adminDB.QueryRow(`SELECT payload_json FROM queue_jobs WHERE workspace_id='ws_execution_store'
		AND kind='runtime_input' AND dedupe_key='runtime_input:ws_execution_store:sesn_execution_store:task_notification:task_execution'`).Scan(&payload); err != nil {
		t.Fatalf("read notification Queue payload: %v", err)
	}
	if strings.Contains(payload, "binding_id") {
		t.Fatalf("notification payload contains retired delivery identity: %s", payload)
	}
}

func TestPostgreSQLBackgroundTaskSettlementParksAtomicallyBehindClosedChildFence(t *testing.T) {
	for _, rollback := range []bool{false, true} {
		name := "commit"
		if rollback {
			name = "rollback"
		}
		t.Run(name, func(t *testing.T) {
			runtimeDB, adminDB := newSandboxServiceTestDB(t)
			seedSandboxExecutionStoreFixture(t, adminDB)
			seedBackgroundTaskFromExecution(t, runtimeDB, adminDB)
			if _, err := adminDB.Exec(`INSERT INTO session_runtime_bindings (
				workspace_id, session_id, binding_id, binding_generation,
				agent_runtime_namespace, agent_runtime_pod_name, agent_runtime_pod_uid,
				agent_runtime_pod_ip, bound_at, updated_at
			) VALUES (
				'ws_execution_store', 'sesn_execution_store', 'bind_execution_store', 7,
				'runtime', 'runtime-0', 'pod_uid_execution_store', '127.0.0.1', now(), now()
			)`); err != nil {
				t.Fatalf("seed Runtime binding: %v", err)
			}
			if _, err := adminDB.Exec(`UPDATE session_threads
				SET status='closed_for_runtime', closed_at=now()
				WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store' AND id='thr_execution_store'`); err != nil {
				t.Fatalf("close background task thread: %v", err)
			}
			client := dbconnect.NewClientForTesting(runtimeDB)
			now := time.Now().UTC()
			errRollback := errors.New("rollback parked notification")
			err := client.WithWorkspaceTx(context.Background(), "ws_execution_store", "test.background_task_closed_fence", func(tx *dbconnect.Tx) error {
				work, current, err := loadBackgroundTaskForUpdateTx(context.Background(), tx, "ws_execution_store", "sesn_execution_store", "task_execution")
				if err != nil || !current {
					return fmt.Errorf("load background task: current=%t err=%w", current, err)
				}
				if err := settleBackgroundTaskResultTx(context.Background(), tx, work, sandboxdriver.CommandResult{
					ResultJSON:     `{"status":"completed","result":{"stdout":"done"}}`,
					TerminalStatus: "completed",
				}, now); err != nil {
					return err
				}
				if rollback {
					return errRollback
				}
				return nil
			})
			if rollback {
				if !errors.Is(err, errRollback) {
					t.Fatalf("rollback settlement error = %v", err)
				}
			} else if err != nil {
				t.Fatalf("commit settlement: %v", err)
			}

			var taskStatus string
			if err := adminDB.QueryRow(`SELECT status FROM session_background_tasks
				WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store' AND task_id='task_execution'`).Scan(&taskStatus); err != nil {
				t.Fatalf("read task status: %v", err)
			}
			var inboxCount, queueCount int
			var inboxStatus sql.NullString
			if err := adminDB.QueryRow(`SELECT count(*), max(status) FROM session_runtime_inbox
				WHERE workspace_id='ws_execution_store' AND runtime_input_id='task_notification:task_execution'`).Scan(&inboxCount, &inboxStatus); err != nil {
				t.Fatalf("read parked notification: %v", err)
			}
			if err := adminDB.QueryRow(`SELECT count(*) FROM queue_jobs
				WHERE workspace_id='ws_execution_store'
				  AND dedupe_key='runtime_input:ws_execution_store:sesn_execution_store:task_notification:task_execution'`).Scan(&queueCount); err != nil {
				t.Fatalf("count notification Queue jobs: %v", err)
			}
			if rollback {
				if taskStatus != "running" || inboxCount != 0 || queueCount != 0 {
					t.Fatalf("rollback task/inbox/Queue = %q/%d/%d; want running/0/0", taskStatus, inboxCount, queueCount)
				}
				return
			}
			var bindingID, targetPodUID string
			var bindingGeneration int64
			if err := adminDB.QueryRow(`SELECT binding_id, binding_generation, target_pod_uid
				FROM session_runtime_inbox
				WHERE workspace_id='ws_execution_store' AND runtime_input_id='task_notification:task_execution'`).Scan(
				&bindingID, &bindingGeneration, &targetPodUID,
			); err != nil {
				t.Fatalf("read parked custody identity: %v", err)
			}
			if taskStatus != "completed" || inboxCount != 1 || inboxStatus.String != "parked" || queueCount != 0 {
				t.Fatalf("closed-fence task/inbox/Queue = %q/%d:%q/%d; want completed/1:parked/0", taskStatus, inboxCount, inboxStatus.String, queueCount)
			}
			if bindingID != "bind_execution_store" || bindingGeneration != 7 || targetPodUID != "pod_uid_execution_store" {
				t.Fatalf("parked custody identity = %q/%d/%q", bindingID, bindingGeneration, targetPodUID)
			}
		})
	}
}

func TestPostgreSQLBackgroundCommandDuplicatePollPreservesConsumedReceipt(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	seedBackgroundTaskFromExecution(t, runtimeDB, adminDB)
	const terminalResult = `{"status":"completed","result":{"stdout":"done"}}`
	const terminalEventID = "evt_background_poll_consumed"
	now := time.Now().UTC()
	if _, err := adminDB.Exec(`INSERT INTO session_events (
		workspace_id, session_id, session_thread_id, event_id, sequence, type,
		payload_json, created_at, updated_at
	) VALUES (
		'ws_execution_store', 'sesn_execution_store', 'thr_execution_store', $1, 1,
		'agent.tool_result', '{}', $2, $2
	)`, terminalEventID, now); err != nil {
		t.Fatalf("seed terminal event: %v", err)
	}
	if _, err := adminDB.Exec(`UPDATE session_background_tasks
		SET status='completed', terminal_result_json=$1, terminal_result_digest=$2,
		    terminal_at=$3, next_poll_at=NULL, updated_at=$3
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store'
		  AND task_id='task_execution'`, terminalResult, sha256Hex([]byte(terminalResult)), now); err != nil {
		t.Fatalf("settle background task: %v", err)
	}
	if _, err := adminDB.Exec(`INSERT INTO session_runtime_tool_results (
		workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
		normalized_input_hash, tool_name, input_json, ack_status, result_json, result_digest,
		consumed_by_terminal_event_id, consumption_reason,
		background_operation_kind, background_operation_state, background_request_id,
		background_task_id, background_max_output_tokens, created_at, updated_at
	) VALUES (
		'ws_execution_store', 'sesn_execution_store', 'thr_execution_store',
		'evt_background_poll_receipt', 'sandbox_background', 'hash_background_poll',
		'wait_agent', '{}', 'committed', NULL, $1, $2, 'conversation_tool_result',
		'poll', 'terminal', 'request_background_poll', 'task_execution', 0, $3, $3
	)`, sha256Hex([]byte(terminalResult)), terminalEventID, now); err != nil {
		t.Fatalf("seed consumed poll receipt: %v", err)
	}

	store := NewPostgreSQLSandboxBackgroundCommandStore(dbconnect.NewClientForTesting(runtimeDB))
	_, current, err := store.LoadCommand(sandboxTestQueueContext(t, runtimeDB), SandboxBackgroundCommandJob{
		WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store",
		TaskID: "task_execution", RequestID: "request_background_poll",
	}, now.Add(time.Second))
	if err != nil {
		t.Fatalf("LoadCommand duplicate consumed poll: %v", err)
	}
	if current {
		t.Fatal("LoadCommand current=true; want duplicate consumed receipt")
	}
	var result sql.NullString
	var state, consumedBy string
	if err := adminDB.QueryRow(`SELECT background_operation_state, result_json, consumed_by_terminal_event_id
		FROM session_runtime_tool_results
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store'
		  AND tool_use_event_id='evt_background_poll_receipt'`).Scan(&state, &result, &consumedBy); err != nil {
		t.Fatalf("read consumed poll receipt: %v", err)
	}
	if state != "terminal" || result.Valid || consumedBy != terminalEventID {
		t.Fatalf("consumed receipt = state %q result %+v event %q; want terminal/NULL/%q", state, result, consumedBy, terminalEventID)
	}
}

func TestPostgreSQLBackgroundSettlementLocksLifecycleBeforeTask(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	seedBackgroundTaskFromExecution(t, runtimeDB, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	now := time.Now().UTC()
	seedSandboxLifecycleLockRow(t, adminDB, now)
	store := NewPostgreSQLSandboxBackgroundCommandStore(dbconnect.NewClientForTesting(runtimeDB))
	work, current, err := store.LoadReconcile(ctx, SandboxBackgroundReconcileJob{
		WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store", TaskID: "task_execution", ReconcileGeneration: 1,
	})
	if err != nil || !current {
		t.Fatalf("LoadReconcile = current %t err %v", current, err)
	}
	blocker, err := adminDB.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin blocker: %v", err)
	}
	defer func() { _ = blocker.Rollback() }()
	var operationID string
	if err := blocker.QueryRow(`SELECT operation_id FROM sandbox_lifecycle_operations
		WHERE workspace_id='ws_execution_store' AND operation_id='sop_lock_order' FOR UPDATE`).Scan(&operationID); err != nil {
		t.Fatalf("lock lifecycle row: %v", err)
	}
	settled := make(chan error, 1)
	go func() {
		settled <- store.SettleTask(ctx, work, sandboxdriver.CommandResult{
			ResultJSON: `{"status":"completed","result":{"stdout":"done"}}`, TerminalStatus: "completed",
		}, now)
	}()
	waitForSandboxLifecycleLockWait(t, adminDB)
	lockCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var taskID string
	if err := blocker.QueryRowContext(lockCtx, `SELECT task_id FROM session_background_tasks
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store' AND task_id='task_execution' FOR UPDATE`).Scan(&taskID); err != nil {
		t.Fatalf("lock background task while writer waits on lifecycle: %v", err)
	}
	if err := blocker.Rollback(); err != nil {
		t.Fatalf("release blocker: %v", err)
	}
	select {
	case err := <-settled:
		if err != nil {
			t.Fatalf("SettleTask after lock release: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SettleTask did not complete after lifecycle lock release")
	}
}

func TestPostgreSQLBackgroundSettlementLocksTaskBeforeRuntimeQueue(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	seedBackgroundTaskFromExecution(t, runtimeDB, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	now := time.Now().UTC()
	partition := queue.FormatSessionPartitionKey("ws_execution_store", "sesn_execution_store")
	if _, err := adminDB.Exec(`INSERT INTO queue_partition_counters (
		workspace_id, partition_key, last_sequence, created_at, updated_at
	) VALUES ('ws_execution_store', $1, 0, $2, $2)`, partition, now); err != nil {
		t.Fatalf("seed Runtime Queue partition: %v", err)
	}
	store := NewPostgreSQLSandboxBackgroundCommandStore(dbconnect.NewClientForTesting(runtimeDB))
	work, current, err := store.LoadReconcile(ctx, SandboxBackgroundReconcileJob{
		WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store", TaskID: "task_execution", ReconcileGeneration: 1,
	})
	if err != nil || !current {
		t.Fatalf("LoadReconcile = current %t err %v", current, err)
	}
	blocker, err := adminDB.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin blocker: %v", err)
	}
	defer func() { _ = blocker.Rollback() }()
	var taskID string
	if err := blocker.QueryRow(`SELECT task_id FROM session_background_tasks
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store' AND task_id='task_execution' FOR UPDATE`).Scan(&taskID); err != nil {
		t.Fatalf("lock background task: %v", err)
	}
	settled := make(chan error, 1)
	go func() {
		settled <- store.SettleTask(ctx, work, sandboxdriver.CommandResult{
			ResultJSON: `{"status":"completed","result":{"stdout":"done"}}`, TerminalStatus: "completed",
		}, now)
	}()
	time.Sleep(200 * time.Millisecond)
	lockCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var sequence int64
	if err := blocker.QueryRowContext(lockCtx, `SELECT last_sequence FROM queue_partition_counters
		WHERE workspace_id='ws_execution_store' AND partition_key=$1 FOR UPDATE`, partition).Scan(&sequence); err != nil {
		t.Fatalf("lock Runtime Queue partition while task is held: %v", err)
	}
	if err := blocker.Commit(); err != nil {
		t.Fatalf("release background task: %v", err)
	}
	select {
	case err := <-settled:
		if err != nil {
			t.Fatalf("SettleTask after task release: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SettleTask did not complete after task release")
	}
}

func TestPostgreSQLBackgroundCommandSettlementLocksReceiptBeforeLifecycle(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	seedBackgroundTaskFromExecution(t, runtimeDB, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	now := time.Now().UTC()
	seedSandboxLifecycleLockRow(t, adminDB, now)
	const requestID = "background_request_lock_order"
	seedBackgroundCommandReceipt(t, adminDB, requestID, string(SandboxBackgroundOperationStdin), SandboxBackgroundOperationPending, now)
	store := NewPostgreSQLSandboxBackgroundCommandStore(dbconnect.NewClientForTesting(runtimeDB))
	work, current, err := store.LoadCommand(ctx, SandboxBackgroundCommandJob{
		WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store",
		TaskID: "task_execution", RequestID: requestID,
	}, now)
	if err != nil || !current {
		t.Fatalf("LoadCommand = current %t err %v", current, err)
	}
	blocker, err := adminDB.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin blocker: %v", err)
	}
	defer func() { _ = blocker.Rollback() }()
	var eventID string
	if err := blocker.QueryRow(`SELECT tool_use_event_id FROM session_runtime_tool_results
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store'
		  AND background_request_id=$1 FOR UPDATE`, requestID).Scan(&eventID); err != nil {
		t.Fatalf("lock command receipt: %v", err)
	}
	settled := make(chan error, 1)
	go func() {
		settled <- store.SettleCommand(ctx, work, "completed", sandboxdriver.CommandResult{
			ResultJSON: `{"status":"success","result":{"bytes_written":1}}`,
		}, now)
	}()
	waitForSandboxLockWait(t, adminDB, "background_request_id")
	lockCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var operationID string
	if err := blocker.QueryRowContext(lockCtx, `SELECT operation_id FROM sandbox_lifecycle_operations
		WHERE workspace_id='ws_execution_store' AND operation_id='sop_lock_order' FOR UPDATE`).Scan(&operationID); err != nil {
		t.Fatalf("lock lifecycle while writer waits on receipt: %v", err)
	}
	if err := blocker.Rollback(); err != nil {
		t.Fatalf("release receipt blocker: %v", err)
	}
	select {
	case err := <-settled:
		if err != nil {
			t.Fatalf("SettleCommand after receipt release: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SettleCommand did not complete after receipt release")
	}
}

func seedBackgroundTaskFromExecution(t *testing.T, runtimeDB *sql.DB, adminDB *sql.DB) {
	t.Helper()
	now := time.Now().UTC()
	seedReadySandboxBinding(t, adminDB, now)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	work := loadReadySandboxExecutionWork(t, coordinator)
	if ok, err := coordinator.BeginPreparing(ctx, work, now.Add(time.Minute)); err != nil || !ok {
		t.Fatalf("BeginPreparing = %t, %v", ok, err)
	}
	if ok, err := coordinator.AuthorizeRunning(ctx, work); err != nil || !ok {
		t.Fatalf("AuthorizeRunning = %t, %v", ok, err)
	}
	if err := coordinator.SettleExecution(ctx, work, SandboxExecutionSettlement{
		Kind: SandboxExecutionCompleted, ResultJSON: `{"status":"running","result":{"task_id":"task_execution"}}`,
		BackgroundTask: &sandboxdriver.BackgroundTask{TaskID: "task_execution", SourceToolUseEventID: work.Ref.ToolUseEventID, ProviderSessionID: "provider_execution_store", ProviderCommandID: "task_execution", ProviderCommandMetadataJSON: `{}`},
	}); err != nil {
		t.Fatalf("SettleExecution: %v", err)
	}
}
