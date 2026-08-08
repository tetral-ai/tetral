package tetralsandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestPostgreSQLSandboxExecutionSettlementRollsBackAfterQueueAuthorityExpires(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	client := dbconnect.NewClientForTesting(runtimeDB)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(client, 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	firstCtx, secondCtx, _, _ := supersedeSandboxQueueLease(t, runtimeDB, adminDB, queue.EnqueueRequest{
		ID: "qjob_exec_fence", WorkspaceID: workspace.ID(work.Ref.WorkspaceID), Kind: queue.KindSandboxToolExecute,
		PartitionKey: queue.FormatSandboxExecutionPartitionKey(workspace.ID(work.Ref.WorkspaceID), work.Ref.SessionID, work.Ref.SessionThreadID, work.Ref.ToolUseEventID),
		DedupeKey:    queue.FormatSandboxToolExecuteDedupeKey(workspace.ID(work.Ref.WorkspaceID), work.Ref.SessionID, work.Ref.SessionThreadID, work.Ref.ToolUseEventID, 1), PayloadVersion: 1,
		PayloadJSON: []byte(`{"workspace_id":"ws_execution_store","session_id":"sesn_execution_store","session_thread_id":"thr_execution_store","tool_use_event_id":"evt_execution_a"}`),
		MaxAttempts: 5,
	})
	err := coordinator.SettleExecution(firstCtx, work, SandboxExecutionSettlement{
		Kind: SandboxExecutionFailed, ErrorKind: "provider_failed", SafeMessage: "provider failed",
	})
	if !errors.Is(err, errQueueLeaseLost) {
		t.Fatalf("SettleExecution error = %v; want Queue authority loss", err)
	}
	var state string
	var resultJSON sql.NullString
	if err := adminDB.QueryRow(`SELECT execution_state, result_json FROM session_runtime_tool_results
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND tool_use_event_id=$4`,
		work.Ref.WorkspaceID, work.Ref.SessionID, work.Ref.SessionThreadID, work.Ref.ToolUseEventID,
	).Scan(&state, &resultJSON); err != nil {
		t.Fatalf("read execution after rejected settlement: %v", err)
	}
	if state != "pending" || resultJSON.Valid {
		t.Fatalf("execution after rejected settlement = %s/%v; want unchanged pending state", state, resultJSON)
	}
	if err := coordinator.SettleExecution(secondCtx, work, SandboxExecutionSettlement{
		Kind: SandboxExecutionFailed, ErrorKind: "provider_failed", SafeMessage: "provider failed",
	}); err != nil {
		t.Fatalf("successor SettleExecution: %v", err)
	}
	if err := adminDB.QueryRow(`SELECT execution_state, result_json FROM session_runtime_tool_results
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND tool_use_event_id=$4`,
		work.Ref.WorkspaceID, work.Ref.SessionID, work.Ref.SessionThreadID, work.Ref.ToolUseEventID,
	).Scan(&state, &resultJSON); err != nil {
		t.Fatalf("read successor settlement: %v", err)
	}
	if state != "terminal_unconsumed" || !resultJSON.Valid {
		t.Fatalf("successor settlement = %s/%v; want terminal result", state, resultJSON)
	}
}

func TestPostgreSQLSandboxExecutionCoordinatorJoinsConcurrentFirstActivation(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	seedSandboxExecutionStoreRows(t, adminDB, "evt_execution_c", "evt_execution_d")
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	ctx := sandboxTestQueueContext(t, runtimeDB)

	works := []SandboxExecutionWork{
		loadSandboxExecutionWork(t, coordinator, "evt_execution_a"),
		loadSandboxExecutionWork(t, coordinator, "evt_execution_b"),
		loadSandboxExecutionWork(t, coordinator, "evt_execution_c"),
		loadSandboxExecutionWork(t, coordinator, "evt_execution_d"),
	}
	start := make(chan struct{})
	errs := make(chan error, len(works))
	var wait sync.WaitGroup
	for _, work := range works {
		work := work
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			errs <- coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation)
		}()
	}
	close(start)
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("WaitForActivation: %v", err)
		}
	}

	var bindingCount, operationCount, queueCount, waiterCount int
	if err := adminDB.QueryRow(`SELECT count(*) FROM session_sandbox_bindings
		WHERE workspace_id = 'ws_execution_store' AND session_id = 'sesn_execution_store'`).Scan(&bindingCount); err != nil {
		t.Fatalf("count binding rows: %v", err)
	}
	if err := adminDB.QueryRow(`SELECT count(*) FROM sandbox_lifecycle_operations
		WHERE workspace_id = 'ws_execution_store' AND session_id = 'sesn_execution_store'
		  AND kind = 'create' AND state = 'pending'`).Scan(&operationCount); err != nil {
		t.Fatalf("count activation rows: %v", err)
	}
	if err := adminDB.QueryRow(`SELECT count(*) FROM queue_jobs
		WHERE workspace_id = 'ws_execution_store' AND kind = 'sandbox_activate'`).Scan(&queueCount); err != nil {
		t.Fatalf("count activation Queue rows: %v", err)
	}
	if err := adminDB.QueryRow(`SELECT count(*) FROM session_runtime_tool_results
		WHERE workspace_id = 'ws_execution_store' AND session_id = 'sesn_execution_store'
		  AND waiting_activation_operation_id IS NOT NULL`).Scan(&waiterCount); err != nil {
		t.Fatalf("count activation waiters: %v", err)
	}
	if bindingCount != 1 || operationCount != 1 || queueCount != 1 || waiterCount != 4 {
		t.Fatalf("binding/operation/queue/waiter counts = %d/%d/%d/%d; want 1/1/1/4", bindingCount, operationCount, queueCount, waiterCount)
	}
	rows, err := adminDB.Query(`SELECT DISTINCT waiting_activation_operation_id
		FROM session_runtime_tool_results
		WHERE workspace_id = 'ws_execution_store' AND session_id = 'sesn_execution_store'
		ORDER BY waiting_activation_operation_id`)
	if err != nil {
		t.Fatalf("read execution activation links: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var links []string
	for rows.Next() {
		var link string
		if err := rows.Scan(&link); err != nil {
			t.Fatalf("scan activation link: %v", err)
		}
		links = append(links, link)
	}
	if len(links) != 1 || links[0] == "" {
		t.Fatalf("activation links = %v; want one shared operation", links)
	}
	var labelsJSON string
	if err := adminDB.QueryRow(`SELECT provider_request_labels_json
		FROM sandbox_lifecycle_operations
		WHERE workspace_id = 'ws_execution_store' AND operation_id = $1`, links[0]).Scan(&labelsJSON); err != nil {
		t.Fatalf("read activation labels: %v", err)
	}
	var labels map[string]string
	if err := json.Unmarshal([]byte(labelsJSON), &labels); err != nil {
		t.Fatalf("decode activation labels: %v", err)
	}
	wantLabels := stableSandboxOwnershipLabels("ws_execution_store", "sesn_execution_store", "env_execution_store", labels["tetral.sandbox_id"])
	if !reflect.DeepEqual(labels, wantLabels) {
		t.Fatalf("activation labels = %v; want exact ownership labels %v", labels, wantLabels)
	}
}

func TestPostgreSQLSandboxActivationAttachmentLocksExecutionBeforeQueuePartition(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	if _, err := adminDB.Exec(`INSERT INTO session_sandbox_bindings (
		workspace_id, session_id, logical_sandbox_id, environment_id,
		environment_generation, provider, binding_revision,
		materialized_resource_revision, resource_roots_json,
		provider_metadata_json, created_at, updated_at
	) VALUES (
		'ws_execution_store', 'sesn_execution_store', 'sbox_lock_order_activation', 'env_execution_store',
		1, 'daytona', 1, 0, '[]', '{}', $1, $1
	)`, now); err != nil {
		t.Fatalf("seed binding: %v", err)
	}
	partition := "sandbox-lifecycle:ws_execution_store:sbox_lock_order_activation"
	if _, err := adminDB.Exec(`INSERT INTO queue_partition_counters (
		workspace_id, partition_key, last_sequence, created_at, updated_at
	) VALUES ('ws_execution_store', $1, 0, $2, $2)`, partition, now); err != nil {
		t.Fatalf("seed Queue partition: %v", err)
	}
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	work := loadBoundSandboxExecutionWork(t, coordinator, "evt_execution_a")
	ctx := sandboxTestQueueContext(t, runtimeDB)
	assertExecutionBeforeQueueLockOrder(t, adminDB, partition, func() error {
		return coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation)
	})
}

func TestPostgreSQLSandboxMaterializationAttachmentLocksExecutionBeforeQueuePartition(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	if _, err := adminDB.Exec(`INSERT INTO session_sandbox_bindings (
		workspace_id, session_id, logical_sandbox_id, environment_id,
		environment_generation, provider, provider_resource_id, binding_revision,
		materialized_resource_revision, resource_roots_json,
		provider_metadata_json, created_at, updated_at
	) VALUES (
		'ws_execution_store', 'sesn_execution_store', 'sbox_lock_order_materialize', 'env_execution_store',
		1, 'daytona', 'provider_lock_order_materialize', 1, 0, '[]', '{}', $1, $1
	)`, now); err != nil {
		t.Fatalf("seed binding: %v", err)
	}
	partition := "sandbox-lifecycle:ws_execution_store:sbox_lock_order_materialize"
	if _, err := adminDB.Exec(`INSERT INTO queue_partition_counters (
		workspace_id, partition_key, last_sequence, created_at, updated_at
	) VALUES ('ws_execution_store', $1, 0, $2, $2)`, partition, now); err != nil {
		t.Fatalf("seed Queue partition: %v", err)
	}
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	work := loadBoundSandboxExecutionWork(t, coordinator, "evt_execution_a")
	ctx := sandboxTestQueueContext(t, runtimeDB)
	assertExecutionBeforeQueueLockOrder(t, adminDB, partition, func() error {
		return coordinator.WaitForMaterialization(ctx, work)
	})
}

func TestPostgreSQLSandboxBackgroundResultLocksExecutionBeforeOutgoingQueue(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	now := time.Now().UTC()
	seedReadySandboxBinding(t, adminDB, now)
	partition := queue.FormatSandboxBackgroundPartitionKey("ws_execution_store", "sesn_execution_store", "task_lock_order")
	if _, err := adminDB.Exec(`INSERT INTO queue_partition_counters (
		workspace_id, partition_key, last_sequence, created_at, updated_at
	) VALUES ('ws_execution_store', $1, 0, $2, $2)`, partition, now); err != nil {
		t.Fatalf("seed background Queue partition: %v", err)
	}
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	work := loadReadySandboxExecutionWork(t, coordinator)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	assertExecutionBeforeQueueLockOrder(t, adminDB, partition, func() error {
		return coordinator.SettleExecution(ctx, work, SandboxExecutionSettlement{
			Kind: SandboxExecutionCompleted, ResultJSON: `{"status":"running","result":{"task_id":"task_lock_order"}}`,
			BackgroundTask: &sandboxdriver.BackgroundTask{
				TaskID: "task_lock_order", SourceToolUseEventID: work.Ref.ToolUseEventID,
				ProviderSessionID: "provider_execution_store", ProviderCommandID: "task_lock_order",
				ProviderCommandMetadataJSON: `{}`,
			},
		})
	})
}

func assertExecutionBeforeQueueLockOrder(t *testing.T, db *sql.DB, partition string, attach func() error) {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin blocker transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	var state string
	if err := tx.QueryRow(`SELECT execution_state FROM session_runtime_tool_results
		WHERE workspace_id = 'ws_execution_store' AND session_id = 'sesn_execution_store'
		  AND session_thread_id = 'thr_execution_store' AND tool_use_event_id = 'evt_execution_a'
		FOR UPDATE`).Scan(&state); err != nil {
		t.Fatalf("lock execution row: %v", err)
	}
	attachErr := make(chan error, 1)
	go func() { attachErr <- attach() }()
	time.Sleep(200 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var sequence int64
	if err := tx.QueryRowContext(ctx, `SELECT last_sequence FROM queue_partition_counters
		WHERE workspace_id = 'ws_execution_store' AND partition_key = $1 FOR UPDATE`, partition).Scan(&sequence); err != nil {
		t.Fatalf("lock Queue partition while execution is held: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit blocker transaction: %v", err)
	}
	if err := <-attachErr; err != nil {
		t.Fatalf("attach dependency after releasing execution row: %v", err)
	}
}

func TestPostgreSQLSandboxExecutionRequiresExactMaterializedRevision(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	if _, err := adminDB.Exec(`INSERT INTO session_sandbox_bindings (
		workspace_id, session_id, logical_sandbox_id, environment_id,
		environment_generation, provider, provider_resource_id, binding_revision,
		materialized_resource_revision, resource_credential_expires_at,
		resource_roots_json, helper_verified_at, created_at, updated_at
	) VALUES (
		'ws_execution_store', 'sesn_execution_store', 'sbox_execution_store', 'env_execution_store',
		1, 'daytona', 'provider_execution_store', 1, 2, $1, '[]', $2, $2, $2
	)`, now.Add(10*365*24*time.Hour), now); err != nil {
		t.Fatalf("seed over-advanced binding: %v", err)
	}
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	job := SandboxExecutionJob{Ref: SandboxExecutionRef{
		WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store",
		SessionThreadID: "thr_execution_store", ToolUseEventID: "evt_execution_a",
	}, AttemptGeneration: 1}
	work, current, err := coordinator.LoadExecution(context.Background(), job)
	if err != nil || !current {
		t.Fatalf("LoadExecution = current %t err %v", current, err)
	}
	if work.MaterializationReady {
		t.Fatal("execution accepted a materialized revision that did not exactly match the Session revision")
	}
}

func TestPostgreSQLSandboxExecutionCannotAuthorizeAfterPreparationDeadline(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	now := time.Now().UTC()
	seedReadySandboxBinding(t, adminDB, now)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	job := SandboxExecutionJob{Ref: SandboxExecutionRef{
		WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store",
		SessionThreadID: "thr_execution_store", ToolUseEventID: "evt_execution_a",
	}, AttemptGeneration: 1}
	work, current, err := coordinator.LoadExecution(context.Background(), job)
	if err != nil || !current || !work.MaterializationReady {
		t.Fatalf("LoadExecution = current %t ready %t err %v", current, work.MaterializationReady, err)
	}
	deadline := now.Add(time.Minute)
	prepared, err := coordinator.BeginPreparing(ctx, work, deadline)
	if err != nil || !prepared {
		t.Fatalf("BeginPreparing = prepared %t err %v", prepared, err)
	}
	if _, err := adminDB.Exec(`UPDATE session_runtime_tool_results
		SET preparation_deadline = CURRENT_TIMESTAMP - INTERVAL '1 second'
		WHERE workspace_id = 'ws_execution_store' AND tool_use_event_id = 'evt_execution_a'`); err != nil {
		t.Fatalf("expire preparation deadline: %v", err)
	}
	authorized, err := coordinator.AuthorizeRunning(ctx, work)
	if err != nil {
		t.Fatalf("AuthorizeRunning: %v", err)
	}
	if authorized {
		t.Fatal("execution was authorized after its preparation deadline")
	}
	assertSandboxExecutionState(t, adminDB, "evt_execution_a", "terminal_unconsumed", 1)
}

func TestPostgreSQLSandboxExecutionReturnsPreSubmissionDisappearanceToActivation(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	now := time.Now().UTC()
	seedReadySandboxBinding(t, adminDB, now)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	work := loadReadySandboxExecutionWork(t, coordinator)
	if prepared, err := coordinator.BeginPreparing(ctx, work, now.Add(time.Minute)); err != nil || !prepared {
		t.Fatalf("BeginPreparing = %t, %v", prepared, err)
	}
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation after provider disappearance: %v", err)
	}
	var state string
	var authorizedRevision sql.NullInt64
	var authorizedHandle sql.NullString
	var preparationDeadline sql.NullTime
	if err := adminDB.QueryRow(`SELECT execution_state, authorized_binding_revision,
		authorized_provider_resource_id, preparation_deadline
		FROM session_runtime_tool_results
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store'
		  AND tool_use_event_id='evt_execution_a'`).Scan(
		&state, &authorizedRevision, &authorizedHandle, &preparationDeadline,
	); err != nil {
		t.Fatalf("read execution after activation handoff: %v", err)
	}
	if state != "waiting_activation" || authorizedRevision.Valid || authorizedHandle.Valid || preparationDeadline.Valid {
		t.Fatalf("execution after activation handoff = %q/%v/%v/%v; want waiting_activation with cleared preparation authority", state, authorizedRevision, authorizedHandle, preparationDeadline)
	}
}

func TestPostgreSQLSandboxExecutionUsesDatabaseTimeForCredentialReadiness(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	now := time.Now().UTC()
	seedReadySandboxBinding(t, adminDB, now)
	if _, err := adminDB.Exec(`UPDATE session_sandbox_bindings
		SET resource_credential_expires_at = CURRENT_TIMESTAMP + INTERVAL '20 minutes'
		WHERE workspace_id = 'ws_execution_store' AND session_id = 'sesn_execution_store'`); err != nil {
		t.Fatalf("set near-expiry credential: %v", err)
	}
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	coordinator.clock = func() time.Time { return time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC) }
	work, current, err := coordinator.LoadExecution(context.Background(), SandboxExecutionJob{Ref: SandboxExecutionRef{
		WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store",
		SessionThreadID: "thr_execution_store", ToolUseEventID: "evt_execution_a",
	}, AttemptGeneration: 1})
	if err != nil || !current {
		t.Fatalf("LoadExecution = current %t err %v", current, err)
	}
	if work.MaterializationReady {
		t.Fatal("worker clock overrode the database-time credential fence")
	}
}

func TestPostgreSQLSandboxExecutionLoadsReclaimableAuthorizationStates(t *testing.T) {
	for _, state := range []string{"preparing", "running"} {
		t.Run(state, func(t *testing.T) {
			runtimeDB, adminDB := newSandboxServiceTestDB(t)
			seedSandboxExecutionStoreFixture(t, adminDB)
			now := time.Now().UTC()
			seedReadySandboxBinding(t, adminDB, now)
			if _, err := adminDB.Exec(`UPDATE session_runtime_tool_results
				SET execution_state = $1, authorized_binding_revision = 1,
				    authorized_provider_resource_id = 'provider_execution_store',
				    preparation_deadline = $2
				WHERE workspace_id = 'ws_execution_store' AND tool_use_event_id = 'evt_execution_a'`,
				state, now.Add(time.Minute)); err != nil {
				t.Fatalf("seed %s execution: %v", state, err)
			}
			coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
			work, current, err := coordinator.LoadExecution(context.Background(), SandboxExecutionJob{Ref: SandboxExecutionRef{
				WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store",
				SessionThreadID: "thr_execution_store", ToolUseEventID: "evt_execution_a",
			}, AttemptGeneration: 1})
			if err != nil || !current || work.State != state || work.AuthorizedBindingRevision != 1 || work.AuthorizedProviderResourceID != "provider_execution_store" {
				t.Fatalf("LoadExecution = work %+v current %t err %v; want reclaimable %s", work, current, err, state)
			}
		})
	}
}

func TestPostgreSQLSandboxExecutionAuthorizationRechecksDurableGates(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate string
	}{
		{name: "release fence", mutate: `UPDATE session_sandbox_bindings SET release_requested_at = CURRENT_TIMESTAMP, release_reason = 'session_delete' WHERE workspace_id = 'ws_execution_store' AND session_id = 'sesn_execution_store'`},
		{name: "resource revision", mutate: `UPDATE sessions SET sandbox_resource_revision = sandbox_resource_revision + 1 WHERE workspace_id = 'ws_execution_store' AND id = 'sesn_execution_store'`},
		{name: "credential expiry", mutate: `UPDATE session_sandbox_bindings SET resource_credential_expires_at = CURRENT_TIMESTAMP WHERE workspace_id = 'ws_execution_store' AND session_id = 'sesn_execution_store'`},
		{name: "cancellation", mutate: `UPDATE session_runtime_tool_results SET cancel_requested_at = CURRENT_TIMESTAMP WHERE workspace_id = 'ws_execution_store' AND session_id = 'sesn_execution_store' AND tool_use_event_id = 'evt_execution_a'`},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtimeDB, adminDB := newSandboxServiceTestDB(t)
			seedSandboxExecutionStoreFixture(t, adminDB)
			ctx := sandboxTestQueueContext(t, runtimeDB)
			now := time.Now().UTC()
			seedReadySandboxBinding(t, adminDB, now)
			coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
			work := loadReadySandboxExecutionWork(t, coordinator)
			prepared, err := coordinator.BeginPreparing(ctx, work, now.Add(time.Minute))
			if err != nil || !prepared {
				t.Fatalf("BeginPreparing = %t, %v", prepared, err)
			}
			if _, err := adminDB.Exec(test.mutate); err != nil {
				t.Fatalf("mutate gate: %v", err)
			}
			authorized, err := coordinator.AuthorizeRunning(ctx, work)
			if test.name == "release fence" || test.name == "cancellation" {
				if err != nil {
					t.Fatalf("AuthorizeRunning: %v", err)
				}
			} else if !errors.Is(err, errSandboxExecutionReinspection) {
				t.Fatalf("AuthorizeRunning error = %v; want reinspection", err)
			}
			if authorized {
				t.Fatal("execution crossed running authorization after a durable gate changed")
			}
			wantState := "pending"
			if test.name == "release fence" || test.name == "cancellation" {
				wantState = "terminal_unconsumed"
			}
			assertSandboxExecutionState(t, adminDB, "evt_execution_a", wantState, 1)
		})
	}
}

func TestPostgreSQLSandboxExecutionRejectsStaleActivationObservation(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	now := time.Now().UTC()
	seedReadySandboxBinding(t, adminDB, now)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	work := loadReadySandboxExecutionWork(t, coordinator)
	if _, err := adminDB.Exec(`UPDATE session_sandbox_bindings
		SET binding_revision = binding_revision + 1
		WHERE workspace_id = 'ws_execution_store' AND session_id = 'sesn_execution_store'`); err != nil {
		t.Fatalf("advance binding revision: %v", err)
	}
	if err := coordinator.WaitForActivation(context.Background(), work, ExecutionNeedsCreation); err == nil {
		t.Fatal("stale provider observation did not request a fresh execution inspection")
	}
	assertSandboxExecutionState(t, adminDB, "evt_execution_a", "pending", 1)
	var operations int
	if err := adminDB.QueryRow(`SELECT count(*) FROM sandbox_lifecycle_operations
		WHERE workspace_id = 'ws_execution_store'`).Scan(&operations); err != nil {
		t.Fatalf("count lifecycle operations: %v", err)
	}
	if operations != 0 {
		t.Fatalf("lifecycle operations = %d; want none from stale observation", operations)
	}
}

func TestPostgreSQLSandboxExecutionRejectsStaleMissingBindingObservation(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	seedReadySandboxBinding(t, adminDB, time.Now().UTC())

	err := coordinator.WaitForActivation(context.Background(), work, ExecutionNeedsCreation)
	if !errors.Is(err, errSandboxExecutionReinspection) {
		t.Fatalf("WaitForActivation error = %v; want reinspection after another worker created the binding", err)
	}
	var operations int
	if err := adminDB.QueryRow(`SELECT count(*) FROM sandbox_lifecycle_operations
		WHERE workspace_id = 'ws_execution_store'`).Scan(&operations); err != nil {
		t.Fatalf("count lifecycle operations: %v", err)
	}
	if operations != 0 {
		t.Fatalf("lifecycle operations = %d; want none from stale missing-binding observation", operations)
	}
}

func TestPostgreSQLSandboxExecutionDoesNotJoinStaleRunningActivation(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	now := time.Now().UTC()
	seedReadySandboxBinding(t, adminDB, now)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	first := loadReadySandboxExecutionWork(t, coordinator)
	if err := coordinator.WaitForActivation(ctx, first, ExecutionNeedsActivation); err != nil {
		t.Fatalf("create activation: %v", err)
	}
	if _, err := adminDB.Exec(`UPDATE sandbox_lifecycle_operations SET state = 'running'
		WHERE workspace_id = 'ws_execution_store' AND kind = 'start'`); err != nil {
		t.Fatalf("mark activation running: %v", err)
	}
	if _, err := adminDB.Exec(`UPDATE session_sandbox_bindings
		SET provider_resource_id = 'provider_replacement', binding_revision = binding_revision + 1
		WHERE workspace_id = 'ws_execution_store' AND session_id = 'sesn_execution_store'`); err != nil {
		t.Fatalf("replace binding: %v", err)
	}
	second := loadBoundSandboxExecutionWork(t, coordinator, "evt_execution_b")
	if err := coordinator.WaitForActivation(ctx, second, ExecutionNeedsActivation); !errors.Is(err, errSandboxExecutionReinspection) {
		t.Fatalf("WaitForActivation error = %v; want reinspection instead of joining stale operation", err)
	}
	assertSandboxExecutionState(t, adminDB, "evt_execution_b", "pending", 1)
}

func TestPostgreSQLSandboxExecutionDoesNotJoinStaleRunningMaterialization(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	now := time.Now().UTC()
	seedReadySandboxBinding(t, adminDB, now)
	if _, err := adminDB.Exec(`UPDATE session_sandbox_bindings SET materialized_resource_revision = 0
		WHERE workspace_id = 'ws_execution_store' AND session_id = 'sesn_execution_store'`); err != nil {
		t.Fatalf("make materialization necessary: %v", err)
	}
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	first := loadBoundSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForMaterialization(ctx, first); err != nil {
		t.Fatalf("create materialization: %v", err)
	}
	if _, err := adminDB.Exec(`UPDATE sandbox_lifecycle_operations SET state = 'running'
		WHERE workspace_id = 'ws_execution_store' AND kind = 'materialize'`); err != nil {
		t.Fatalf("mark materialization running: %v", err)
	}
	if _, err := adminDB.Exec(`UPDATE session_sandbox_bindings
		SET provider_resource_id = 'provider_replacement', binding_revision = binding_revision + 1
		WHERE workspace_id = 'ws_execution_store' AND session_id = 'sesn_execution_store'`); err != nil {
		t.Fatalf("replace binding: %v", err)
	}
	second := loadBoundSandboxExecutionWork(t, coordinator, "evt_execution_b")
	if err := coordinator.WaitForMaterialization(ctx, second); !errors.Is(err, errSandboxExecutionReinspection) {
		t.Fatalf("WaitForMaterialization error = %v; want reinspection instead of joining stale operation", err)
	}
	assertSandboxExecutionState(t, adminDB, "evt_execution_b", "pending", 1)
}

func TestPostgreSQLSandboxExecutionJoinsInFlightEarlierResourceRevision(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	now := time.Now().UTC()
	seedReadySandboxBinding(t, adminDB, now)
	if _, err := adminDB.Exec(`UPDATE session_sandbox_bindings SET materialized_resource_revision = 0
		WHERE workspace_id = 'ws_execution_store' AND session_id = 'sesn_execution_store'`); err != nil {
		t.Fatalf("make materialization necessary: %v", err)
	}
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	first := loadBoundSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForMaterialization(ctx, first); err != nil {
		t.Fatalf("create R1 materialization: %v", err)
	}
	if _, err := adminDB.Exec(`UPDATE sandbox_lifecycle_operations SET state = 'running'
		WHERE workspace_id = 'ws_execution_store' AND kind = 'materialize'`); err != nil {
		t.Fatalf("mark R1 materialization running: %v", err)
	}
	if _, err := adminDB.Exec(`UPDATE sessions SET sandbox_resource_revision = 2
		WHERE workspace_id = 'ws_execution_store' AND id = 'sesn_execution_store'`); err != nil {
		t.Fatalf("advance desired resource revision: %v", err)
	}

	second := loadBoundSandboxExecutionWork(t, coordinator, "evt_execution_b")
	if err := coordinator.WaitForMaterialization(ctx, second); err != nil {
		t.Fatalf("join R1 while R2 is desired: %v", err)
	}
	assertSandboxExecutionState(t, adminDB, "evt_execution_b", "waiting_materialization", 1)
}

func TestPostgreSQLSandboxExecutionStartsBackgroundReconciliationAtomically(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	now := time.Now().UTC()
	seedReadySandboxBinding(t, adminDB, now)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	work := loadReadySandboxExecutionWork(t, coordinator)
	if prepared, err := coordinator.BeginPreparing(ctx, work, now.Add(time.Minute)); err != nil || !prepared {
		t.Fatalf("BeginPreparing = %t, %v", prepared, err)
	}
	if authorized, err := coordinator.AuthorizeRunning(ctx, work); err != nil || !authorized {
		t.Fatalf("AuthorizeRunning = %t, %v", authorized, err)
	}
	if err := coordinator.SettleExecution(ctx, work, SandboxExecutionSettlement{
		Kind:       SandboxExecutionCompleted,
		ResultJSON: `{"status":"running","result":{"task_id":"task_execution"}}`,
		BackgroundTask: &sandboxdriver.BackgroundTask{
			TaskID: "task_execution", SourceToolUseEventID: work.Ref.ToolUseEventID,
			ProviderSessionID: "provider_execution_store", ProviderCommandID: "task_execution",
			ProviderCommandMetadataJSON: `{}`,
		},
	}); err != nil {
		t.Fatalf("SettleExecution: %v", err)
	}
	var status string
	var generation int64
	var nextPoll time.Time
	if err := adminDB.QueryRow(`SELECT status, reconcile_generation, next_poll_at
		FROM session_background_tasks
		WHERE workspace_id = 'ws_execution_store' AND session_id = 'sesn_execution_store'
		  AND task_id = 'task_execution'`).Scan(&status, &generation, &nextPoll); err != nil {
		t.Fatalf("read background task: %v", err)
	}
	if status != "running" || generation != 1 || nextPoll.IsZero() {
		t.Fatalf("background task = %q generation %d next %v; want running generation 1", status, generation, nextPoll)
	}
	var queueJobs int
	if err := adminDB.QueryRow(`SELECT count(*) FROM queue_jobs
		WHERE workspace_id = 'ws_execution_store' AND kind = 'sandbox_background_reconcile'
		  AND status = 'pending'`).Scan(&queueJobs); err != nil {
		t.Fatalf("count reconcile jobs: %v", err)
	}
	if queueJobs != 1 {
		t.Fatalf("reconcile jobs = %d; want 1", queueJobs)
	}
}

func TestPostgreSQLSandboxStartActivationUsesBoundEnvironmentGenerationWithoutCurrentArtifact(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	seedReadySandboxBinding(t, adminDB, time.Now().UTC())
	if _, err := adminDB.Exec(`UPDATE environments SET current_generation = 2
		WHERE workspace_id = 'ws_execution_store' AND id = 'env_execution_store'`); err != nil {
		t.Fatalf("advance Environment generation: %v", err)
	}
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	work := loadReadySandboxExecutionWork(t, coordinator)
	if err := coordinator.WaitForActivation(ctx, work, ExecutionNeedsActivation); err != nil {
		t.Fatalf("WaitForActivation(start): %v", err)
	}
	var kind, state, targetHandle string
	var targetGeneration sql.NullInt64
	if err := adminDB.QueryRow(`SELECT kind, state, target_provider_resource_id, target_environment_generation
		FROM sandbox_lifecycle_operations WHERE workspace_id = 'ws_execution_store'`).Scan(
		&kind, &state, &targetHandle, &targetGeneration,
	); err != nil {
		t.Fatalf("read Start activation: %v", err)
	}
	if kind != "start" || state != "pending" || targetHandle != "provider_execution_store" || targetGeneration.Valid {
		t.Fatalf("Start activation = kind %q state %q handle %q generation %v; want current bound handle without artifact dependency", kind, state, targetHandle, targetGeneration)
	}
}

func loadReadySandboxExecutionWork(t *testing.T, coordinator *PostgreSQLSandboxExecutionCoordinator) SandboxExecutionWork {
	t.Helper()
	work, current, err := coordinator.LoadExecution(context.Background(), SandboxExecutionJob{Ref: SandboxExecutionRef{
		WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store",
		SessionThreadID: "thr_execution_store", ToolUseEventID: "evt_execution_a",
	}, AttemptGeneration: 1})
	if err != nil || !current || !work.MaterializationReady {
		t.Fatalf("LoadExecution = work %+v current %t err %v; want ready", work, current, err)
	}
	return work
}

func loadSandboxExecutionWork(t *testing.T, coordinator *PostgreSQLSandboxExecutionCoordinator, eventID string) SandboxExecutionWork {
	t.Helper()
	job := SandboxExecutionJob{
		Ref: SandboxExecutionRef{
			WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store",
			SessionThreadID: "thr_execution_store", ToolUseEventID: eventID,
		},
		AttemptGeneration: 1,
	}
	work, current, err := coordinator.LoadExecution(context.Background(), job)
	if err != nil {
		t.Fatalf("LoadExecution(%s): %v", eventID, err)
	}
	if !current || work.Binding != nil {
		t.Fatalf("LoadExecution(%s) = current %t binding %+v; want current without binding", eventID, current, work.Binding)
	}
	return work
}

func loadBoundSandboxExecutionWork(t *testing.T, coordinator *PostgreSQLSandboxExecutionCoordinator, eventID string) SandboxExecutionWork {
	t.Helper()
	job := SandboxExecutionJob{
		Ref: SandboxExecutionRef{
			WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store",
			SessionThreadID: "thr_execution_store", ToolUseEventID: eventID,
		},
		AttemptGeneration: 1,
	}
	work, current, err := coordinator.LoadExecution(context.Background(), job)
	if err != nil {
		t.Fatalf("LoadExecution(%s): %v", eventID, err)
	}
	if !current || work.Binding == nil {
		t.Fatalf("LoadExecution(%s) = current %t binding %+v; want current binding", eventID, current, work.Binding)
	}
	return work
}

func seedSandboxExecutionStoreFixture(t *testing.T, db *sql.DB) {
	t.Helper()
	seedEnvironmentArtifactStoreEnvironment(t, db, "ws_execution_store", "env_execution_store")
	seedEnvironmentArtifactStoreSession(t, db, "ws_execution_store", "sesn_execution_store", "env_execution_store")
	now := "2026-07-31T00:00:00Z"
	if _, err := db.Exec(`UPDATE environments SET current_generation = 1
		WHERE workspace_id = 'ws_execution_store' AND id = 'env_execution_store'`); err != nil {
		t.Fatalf("set environment generation: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO environment_artifacts (
		workspace_id, environment_id, generation, status, provider,
		provider_artifact_ref, normalized_config_hash, artifact_input_hash,
		runtime_network_policy_json, packages_json, created_at, updated_at
	) VALUES (
		'ws_execution_store', 'env_execution_store', 1, 'ready', 'daytona',
		'artifact_execution_store', 'config_hash', 'artifact_hash',
		'{"type":"unrestricted"}', '{}', $1, $1
	)`, now); err != nil {
		t.Fatalf("seed environment artifact: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO session_threads (
		workspace_id, session_id, id, role, status, visibility,
		created_at, last_active_at, updated_at
	) VALUES (
		'ws_execution_store', 'sesn_execution_store', 'thr_execution_store',
		'main', 'idle', 'internal', $1, $1, $1
	)`, now); err != nil {
		t.Fatalf("seed session thread: %v", err)
	}
	seedSandboxExecutionStoreRows(t, db, "evt_execution_a", "evt_execution_b")
}

func seedSandboxExecutionStoreRows(t *testing.T, db *sql.DB, eventIDs ...string) {
	t.Helper()
	now := "2026-07-31T00:00:00Z"
	for _, eventID := range eventIDs {
		if _, err := db.Exec(`INSERT INTO session_runtime_tool_results (
			workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
			normalized_input_hash, tool_name, input_json, ack_status, result_json,
			model_tool_call_id, execution_state, execution_attempt_generation,
			created_at, updated_at
		) VALUES (
			'ws_execution_store', 'sesn_execution_store', 'thr_execution_store', $1,
			'sandbox_tool', $1 || '_hash', 'bash', '{"command":"true"}', 'committed', NULL,
			$1 || '_call', 'pending', 1, $2, $2
		)`, eventID, now); err != nil {
			t.Fatalf("seed execution %s: %v", eventID, err)
		}
	}
}

func seedReadySandboxBinding(t *testing.T, db *sql.DB, now time.Time) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO session_sandbox_bindings (
		workspace_id, session_id, logical_sandbox_id, environment_id,
		environment_generation, provider, provider_resource_id, binding_revision,
		materialized_resource_revision, resource_credential_expires_at,
		resource_roots_json, helper_verified_at, created_at, updated_at
	) VALUES (
		'ws_execution_store', 'sesn_execution_store', 'sbox_execution_store', 'env_execution_store',
		1, 'daytona', 'provider_execution_store', 1, 1, $1, '[]', $2, $2, $2
	)`, now.Add(2*time.Hour), now); err != nil {
		t.Fatalf("seed ready binding: %v", err)
	}
}
