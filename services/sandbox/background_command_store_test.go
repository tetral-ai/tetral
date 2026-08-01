package tetralsandbox

import (
	"context"
	"database/sql"
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
	store := NewPostgreSQLSandboxBackgroundCommandStore(dbconnect.NewClientForTesting(runtimeDB))
	job := SandboxBackgroundReconcileJob{JobID: "qjob_reconcile", LeaseToken: "lease", WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store", TaskID: "task_execution", ReconcileGeneration: 1}
	work, current, err := store.LoadReconcile(context.Background(), job)
	if err != nil || !current {
		t.Fatalf("LoadReconcile = current %t err %v", current, err)
	}
	now := time.Now().UTC()
	nextPoll := now.Add(time.Minute)
	if err := store.AdvanceReconcile(context.Background(), work, sandboxdriver.CommandResult{ResultJSON: `{"status":"running"}`}, now, nextPoll); err != nil {
		t.Fatalf("AdvanceReconcile: %v", err)
	}
	var generation int64
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
	store := NewPostgreSQLSandboxBackgroundCommandStore(dbconnect.NewClientForTesting(runtimeDB))
	now := time.Now().UTC()
	job := &queuev1.QueueJob{
		Id: "qjob_poison_reconcile", WorkspaceId: "ws_execution_store", Kind: queue.KindSandboxBackgroundReconcile,
		PartitionKey: queue.FormatSandboxBackgroundPartitionKey("ws_execution_store", "sesn_execution_store", "task_execution"),
		DedupeKey:    queue.FormatSandboxBackgroundReconcileDedupeKey("ws_execution_store", "sesn_execution_store", "task_execution", 1),
		PayloadJson:  `{not-json`, LeaseToken: "lease_poison_reconcile",
	}
	if err := store.FinalizeReconcileExhaustion(context.Background(), job, now); err != nil {
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
	store := NewPostgreSQLSandboxBackgroundCommandStore(dbconnect.NewClientForTesting(runtimeDB))
	job := SandboxBackgroundReconcileJob{JobID: "qjob_reconcile", LeaseToken: "lease", WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store", TaskID: "task_execution", ReconcileGeneration: 1}
	work, current, err := store.LoadReconcile(context.Background(), job)
	if err != nil || !current {
		t.Fatalf("LoadReconcile = current %t err %v", current, err)
	}
	result := sandboxdriver.CommandResult{ResultJSON: `{"status":"completed","result":{"stdout":"done"}}`, TerminalStatus: "completed"}
	wantStoredResult := normalizeSandboxProviderResult(result.ResultJSON)
	if err := store.SettleTask(context.Background(), work, result, time.Now().UTC()); err != nil {
		t.Fatalf("SettleTask: %v", err)
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

func seedBackgroundTaskFromExecution(t *testing.T, runtimeDB *sql.DB, adminDB *sql.DB) {
	t.Helper()
	now := time.Now().UTC()
	seedReadySandboxBinding(t, adminDB, now)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute)
	work := loadReadySandboxExecutionWork(t, coordinator)
	if ok, err := coordinator.BeginPreparing(context.Background(), work, now.Add(time.Minute)); err != nil || !ok {
		t.Fatalf("BeginPreparing = %t, %v", ok, err)
	}
	if ok, err := coordinator.AuthorizeRunning(context.Background(), work); err != nil || !ok {
		t.Fatalf("AuthorizeRunning = %t, %v", ok, err)
	}
	if err := coordinator.SettleExecution(context.Background(), work, SandboxExecutionSettlement{
		Kind: SandboxExecutionCompleted, ResultJSON: `{"status":"running","result":{"task_id":"task_execution"}}`,
		BackgroundTask: &sandboxdriver.BackgroundTask{TaskID: "task_execution", SourceToolUseEventID: work.Ref.ToolUseEventID, ProviderSessionID: "provider_execution_store", ProviderCommandID: "task_execution", ProviderCommandMetadataJSON: `{}`},
	}); err != nil {
		t.Fatalf("SettleExecution: %v", err)
	}
}
