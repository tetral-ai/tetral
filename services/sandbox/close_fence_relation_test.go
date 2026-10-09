package tetralsandbox

import (
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

// Background-task settlement reaches the child-control fence through
// ThreadOrAncestorClosingOrClosedTx. Under the real Sandbox role, a committed
// close control whose source Tool Use has a result no longer fences the child,
// so the notification is queued with Runner custody; without that result the
// notification parks. The nonexistent-source case is covered by
// TestPostgreSQLBackgroundSettlementParksAfterCommittedControlWithoutCloseReceipt.
func TestPostgreSQLBackgroundSettlementCloseFenceFollowsSourceToolResult(t *testing.T) {
	for _, answered := range []bool{true, false} {
		name := "unanswered source"
		if answered {
			name = "answered source"
		}
		t.Run(name, func(t *testing.T) {
			_, admin := newSandboxServiceTestDB(t)
			workload := storagetest.OpenWorkloadDB(t, admin, "sandbox")
			seedSandboxExecutionStoreFixture(t, admin)
			seedBackgroundTaskFromExecution(t, workload.DB, admin)
			registerSandboxFixtureProcess(t, admin, "runtime", "pod_close")
			if _, err := admin.Exec(`INSERT INTO session_threads (
 workspace_id,session_id,id,parent_thread_id,role,task_name,status,visibility,created_at,last_active_at,updated_at
 ) VALUES ('ws_execution_store','sesn_execution_store','thr_close_child','thr_execution_store','subagent','background child','idle','internal',now(),now(),now());
 UPDATE session_background_tasks SET session_thread_id='thr_close_child'
 WHERE workspace_id='ws_execution_store' AND task_id='task_execution';
 INSERT INTO session_runtime_bindings (
   workspace_id,session_id,binding_id,binding_generation,agent_runtime_namespace,
   agent_runtime_pod_name,agent_runtime_pod_uid,agent_runtime_pod_ip,runtime_process_id,bound_at,updated_at
 ) VALUES ('ws_execution_store','sesn_execution_store','bind_close',7,'runtime','runtime-0','pod_close','127.0.0.1','process_pod_close',now(),now());
 INSERT INTO session_events (workspace_id,session_id,session_thread_id,event_id,sequence,type,payload_json,model_request_id,model_tool_call_id,created_at,updated_at)
 VALUES ('ws_execution_store','sesn_execution_store','thr_execution_store','evt_close_source',1,'agent.tool_use',
 '{"name":"close_agent","input":{"task_name":"background child"}}','mreq_close_source','call_close_source',now(),now());
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
			if answered {
				if _, err := admin.Exec(`INSERT INTO session_events (
 workspace_id,session_id,session_thread_id,event_id,sequence,type,payload_json,model_request_id,tool_use_event_id,created_at,updated_at
 ) VALUES ('ws_execution_store','sesn_execution_store','thr_execution_store','evt_close_result',2,'agent.tool_result',
 '{"tool_use_id":"evt_close_source","content":[{"type":"text","text":"closed"}],"is_error":false}','mreq_close_source','evt_close_source',now(),now())`); err != nil {
					t.Fatal(err)
				}
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
			if err := store.SettleTask(ctx, work, result, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			var inboxStatus string
			var jobs int
			if err := admin.QueryRow(`SELECT
 (SELECT status FROM session_runtime_inbox WHERE workspace_id='ws_execution_store' AND runtime_input_id='task_notification:task_execution'),
 (SELECT count(*) FROM queue_jobs WHERE workspace_id='ws_execution_store'
   AND dedupe_key='runtime_input:ws_execution_store:sesn_execution_store:task_notification:task_execution')`).Scan(&inboxStatus, &jobs); err != nil {
				t.Fatal(err)
			}
			wantStatus, wantJobs := "parked", 0
			if answered {
				wantStatus, wantJobs = "queued", 1
			}
			if inboxStatus != wantStatus || jobs != wantJobs {
				t.Fatalf("notification after settlement = Inbox %s jobs %d; want %s/%d", inboxStatus, jobs, wantStatus, wantJobs)
			}
		})
	}
}
