package sessionevent

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestAppendClientEventsChecksCommittedChildCloseWithAPIRole(t *testing.T) {
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Skip(storagetest.EnvTestDatabaseURL + " is not set")
	}
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	workload := storagetest.OpenWorkloadDB(t, admin, "api")
	const sessionID = "sesn_api_close_role"
	const childID = "thr_api_close_role"
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
	seedSessionEventThread(t, admin, workspace.DefaultID, sessionID, childID, "subagent", "public", false)
	if _, err := admin.Exec(`INSERT INTO session_events (
   workspace_id,session_id,session_thread_id,event_id,sequence,type,payload_json,created_at,updated_at
 ) VALUES ('default',$1,$2,'evt_close_request',1,'agent.thread_interrupt_requested',
   '{"root_child_thread_id":"thr_api_close_role","action":"close","source_tool_use_event_id":"evt_close_source","runtime_input_id":"close_input","disposition":"pending_control"}',now(),now());
 `, sessionID, childID); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(`INSERT INTO session_runtime_inbox (
   workspace_id,session_id,session_thread_id,runtime_input_id,input_kind,status,
   binding_id,binding_generation,target_pod_uid,created_at,updated_at,committed_at
 ) VALUES ('default',$1,$2,'close_input','interrupt_control','committed','bind_close',7,'pod_close',now(),now(),now())`, sessionID, childID); err != nil {
		t.Fatal(err)
	}
	service := newSessionEventServiceForTest(workload.DB)
	seedSessionEventPendingApproval(t, admin, workspace.DefaultID, sessionID, "evt_child_pending_tool")
	if _, err := admin.Exec(`UPDATE session_pending_tool_uses SET session_thread_id=$2
 WHERE workspace_id='default' AND session_id=$1 AND tool_use_event_id='evt_child_pending_tool'`, sessionID, childID); err != nil {
		t.Fatal(err)
	}
	request := AppendRequest{Events: []IncomingEvent{{Type: EventTypeUserToolConfirmation, ToolUseID: "evt_child_pending_tool", Result: ToolConfirmationResultDeny, DenyMessage: "not now"}}}
	appendConfirmation := func() error {
		_, err := service.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID, "idem_close_window", request)
		return err
	}
	assertNoAdmission := func() {
		t.Helper()
		if rows := readSessionEventLedgerRows(t, admin, sessionID); len(rows) != 1 {
			t.Fatalf("failed admission changed ledger: %+v", rows)
		}
		if jobs := readSessionEventQueueJobs(t, admin, sessionID); len(jobs) != 0 {
			t.Fatalf("failed admission enqueued: %+v", jobs)
		}
		assertSessionEventIdempotencyRowCount(t, admin, sessionID, 0)
		assertSessionEventPendingApprovalStatus(t, admin, sessionID, "evt_child_pending_tool", "pending")
	}
	workload.RequirePrivilege(t, "session_bridge_operations", "SELECT", appendConfirmation)
	assertNoAdmission()
	// With the privilege restored, a missing close receipt produces the intended
	// conflict rather than a database error, and still admits no work.
	var conflict *ConflictError
	if err := appendConfirmation(); !errors.As(err, &conflict) || conflict.Message != "session thread is closing" {
		t.Fatalf("close without receipt: %v", err)
	}
	assertNoAdmission()
	// Once the parent records the terminal source Tool Result, this idle child
	// is no longer fenced by that control. Rejection did not consume the key.
	if _, err := admin.Exec(`INSERT INTO session_events (
   workspace_id,session_id,session_thread_id,event_id,sequence,type,payload_json,created_at,updated_at
 ) VALUES ('default',$1,$2,'evt_close_result',1,'agent.tool_result','{"tool_use_event_id":"evt_close_source"}',now(),now())`, sessionID, sessionEventMainThreadID(sessionID)); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := appendConfirmation(); err != nil {
			t.Fatal(err)
		}
	}
	rows := readSessionEventLedgerRows(t, admin, sessionID)
	if len(rows) != 3 {
		t.Fatalf("ledger rows = %d; want control, terminal result and one tool confirmation", len(rows))
	}
	var confirmations int
	for _, row := range rows {
		if row.eventType == EventTypeUserToolConfirmation && row.sessionThreadID == childID {
			confirmations++
		}
	}
	if confirmations != 1 {
		t.Fatalf("child tool confirmations = %d; want one", confirmations)
	}
	jobs := readSessionEventQueueJobs(t, admin, sessionID)
	if len(jobs) != 1 {
		t.Fatalf("notification jobs = %d; want one", len(jobs))
	}
	assertRuntimeInputQueueJobThread(t, jobs[0], childID)
	assertSessionEventIdempotencyRowCount(t, admin, sessionID, 1)
	assertSessionEventPendingApprovalDecision(t, admin, sessionID, "evt_child_pending_tool", "resolving", "deny", "not now")
}
