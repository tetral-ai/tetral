package tetralsandbox

import (
	"database/sql"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

func TestPostgreSQLBackgroundSettlementParksAfterCommittedControlWithoutCloseReceipt(t *testing.T) {
	_, admin := newSandboxServiceTestDB(t)
	workload := storagetest.OpenWorkloadDB(t, admin, "sandbox")
	seedSandboxExecutionStoreFixture(t, admin)
	seedBackgroundTaskFromExecution(t, workload.DB, admin)
	// The child is still idle; a committed control input alone does not prove
	// that the close completed. No terminal Tool Result or close receipt exists.
	if _, err := admin.Exec(`INSERT INTO session_threads (
 workspace_id,session_id,id,parent_thread_id,role,task_name,status,visibility,created_at,last_active_at,updated_at
 ) VALUES ('ws_execution_store','sesn_execution_store','thr_close_child','thr_execution_store','subagent','background child','idle','internal',now(),now(),now());
 UPDATE session_background_tasks SET session_thread_id='thr_close_child'
 WHERE workspace_id='ws_execution_store' AND task_id='task_execution';
 INSERT INTO session_runtime_bindings (
   workspace_id,session_id,binding_id,binding_generation,agent_runtime_namespace,
   agent_runtime_pod_name,agent_runtime_pod_uid,agent_runtime_pod_ip,bound_at,updated_at
 ) VALUES ('ws_execution_store','sesn_execution_store','bind_close',7,'runtime','runtime-0','pod_close','127.0.0.1',now(),now());
 INSERT INTO session_events (workspace_id,session_id,session_thread_id,event_id,sequence,type,payload_json,created_at,updated_at)
 VALUES ('ws_execution_store','sesn_execution_store','thr_close_child','evt_close',1,'agent.thread_interrupt_requested',
 '{"root_child_thread_id":"thr_close_child","action":"close","source_tool_use_event_id":"evt_close_source","runtime_input_id":"close_input","disposition":"pending_control"}',now(),now());
 INSERT INTO session_runtime_inbox (
   workspace_id,session_id,session_thread_id,runtime_input_id,input_kind,status,
   binding_id,binding_generation,target_pod_uid,created_at,updated_at,committed_at
 ) VALUES ('ws_execution_store','sesn_execution_store','thr_close_child','close_input','interrupt_control','committed',
   'bind_close',7,'pod_close',now(),now(),now())`); err != nil {
		t.Fatal(err)
	}
	ctx := sandboxTestQueueContext(t, workload.DB)
	store := NewPostgreSQLSandboxBackgroundCommandStore(dbconnect.NewClientForTesting(workload.DB))
	work, current, err := store.LoadReconcile(ctx, SandboxBackgroundReconcileJob{
		WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store", TaskID: "task_execution", ReconcileGeneration: 1,
	})
	if err != nil || !current {
		t.Fatalf("load task = %t, %v", current, err)
	}
	result := sandboxdriver.CommandResult{ResultJSON: `{"status":"completed","result":{"stdout":"done"}}`, TerminalStatus: "completed"}
	settle := func() error { return store.SettleTask(ctx, work, result, time.Now().UTC()) }
	workload.RequirePrivilege(t, "session_bridge_operations", "SELECT", settle)
	assertBackgroundTaskSettlementRolledBack(t, admin)
	for range 2 {
		if err := settle(); err != nil {
			t.Fatal(err)
		}
	}
	var taskStatus, storedResult, inboxStatus, bindingID, podUID string
	var generation int64
	if err := admin.QueryRow(`SELECT task.status,task.terminal_result_json,inbox.status,inbox.binding_id,inbox.binding_generation,inbox.target_pod_uid
   FROM session_background_tasks task JOIN session_runtime_inbox inbox
   ON inbox.workspace_id=task.workspace_id AND inbox.runtime_input_id='task_notification:'||task.task_id
   WHERE task.workspace_id='ws_execution_store' AND task.task_id='task_execution'`).Scan(
		&taskStatus, &storedResult, &inboxStatus, &bindingID, &generation, &podUID); err != nil {
		t.Fatal(err)
	}
	if taskStatus != "completed" || storedResult != normalizeSandboxProviderResult(result.ResultJSON) || inboxStatus != "parked" || bindingID != "bind_close" || generation != 7 || podUID != "pod_close" {
		t.Fatalf("task/notification = %s/%s/%s/%s/%d/%s", taskStatus, storedResult, inboxStatus, bindingID, generation, podUID)
	}
	var jobs int
	if err := admin.QueryRow(`SELECT count(*) FROM queue_jobs WHERE workspace_id='ws_execution_store'
   AND dedupe_key='runtime_input:ws_execution_store:sesn_execution_store:task_notification:task_execution'`).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if jobs != 0 {
		t.Fatalf("closing child has %d runnable notification jobs", jobs)
	}
}

func assertBackgroundTaskSettlementRolledBack(t *testing.T, admin *sql.DB) {
	t.Helper()
	var status string
	var result sql.NullString
	var inboxCount, jobs int
	if err := admin.QueryRow(`SELECT status,terminal_result_json FROM session_background_tasks
   WHERE workspace_id='ws_execution_store' AND task_id='task_execution'`).Scan(&status, &result); err != nil {
		t.Fatal(err)
	}
	if err := admin.QueryRow(`SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='ws_execution_store'
   AND runtime_input_id='task_notification:task_execution'`).Scan(&inboxCount); err != nil {
		t.Fatal(err)
	}
	if err := admin.QueryRow(`SELECT count(*) FROM queue_jobs WHERE workspace_id='ws_execution_store'
   AND dedupe_key='runtime_input:ws_execution_store:sesn_execution_store:task_notification:task_execution'`).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if status != "running" || result.Valid || inboxCount != 0 || jobs != 0 {
		t.Fatalf("failed settlement retained effects: task=%s result=%v inbox=%d jobs=%d", status, result, inboxCount, jobs)
	}
}
