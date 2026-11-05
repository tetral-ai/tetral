package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// A child thread's drive_sync input that is still queued when the child
// closes, or when it terminates, moves to the main thread: its job is
// cancelled and one job of the main thread delivers it.
func TestDriveSyncInputLeavesAClosingChildForTheMainThread(t *testing.T) {
	for _, tc := range []struct {
		name  string
		leave func(t *testing.T, f driveSyncChild)
	}{
		{"child close", closeDriveSyncChild},
		{"child termination", terminateDriveSyncChild},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDriveSyncChild(t)
			before := f.jobs(t)
			if len(before) != 1 || before[0].status != queue.StatusPending || before[0].thread != f.childID {
				t.Fatalf("jobs before the child leaves = %+v; want one pending job of the child", before)
			}
			tc.leave(t, f)
			var thread, inboxStatus string
			var bound bool
			if err := f.admin.QueryRowContext(f.ctx, `SELECT session_thread_id,status,binding_id IS NOT NULL FROM session_runtime_inbox
				WHERE workspace_id='default' AND runtime_input_id=$1`, f.inputID).Scan(&thread, &inboxStatus, &bound); err != nil {
				t.Fatal(err)
			}
			if thread != f.mainID || inboxStatus != "queued" || bound {
				t.Fatalf("drive_sync input = thread %s, %s, bound %v; want queued on the main thread %s with no binding", thread, inboxStatus, bound, f.mainID)
			}
			after := f.jobs(t)
			if len(after) != 2 || after[0].status != queue.StatusCancelled || after[1].status != queue.StatusPending || after[1].thread != f.mainID {
				t.Fatalf("jobs after the child leaves = %+v; want the child's cancelled and one pending job of the main thread", after)
			}
		})
	}
}

type driveSyncChild struct {
	ctx                                 context.Context
	runtime, admin                      *sql.DB
	sessionID, mainID, childID, inputID string
	bindingID, podUID                   string
	store                               *PostgreSQLBridgeAPIStore
}

type driveSyncJob struct{ status, thread string }

// newDriveSyncChild seeds a Session bound to a Runtime with a child thread
// and one drive sync report delivered to the child.
func newDriveSyncChild(t *testing.T) driveSyncChild {
	t.Helper()
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	f := driveSyncChild{ctx: context.Background(), runtime: runtime, admin: admin, sessionID: "sesn_dsync_close", mainID: "thr_dsync_close_main",
		childID: "thr_dsync_close_child", bindingID: "bind_dsync_close", podUID: "pod_dsync_close"}
	sessionfixture.SeedBridgeAPISession(t, admin, "default", f.sessionID, f.mainID)
	sessionfixture.SeedBridgeAPIChildThread(t, admin, "default", f.sessionID, f.mainID, f.childID)
	seedBridgeAPIEvent(t, admin, "default", f.sessionID, f.childID, "evt_dsync_close_child_created",
		sessionfixture.NextBridgeAPIEventSequenceForTest(t, admin, f.sessionID, f.childID), "session.thread_created",
		`{"type":"session.thread_created","parent_thread_id":"`+f.mainID+`","source_tool_use_event_id":"sevt_dsync_close_spawn"}`)
	seedBridgeAPIRuntimeBinding(t, admin, "default", f.sessionID, f.bindingID, 1, f.podUID)
	for _, statement := range []string{
		`INSERT INTO computers (workspace_id,session_id,id,kind,environment_id,working_directory,networking,drive_exclude,association_state,execution_state)
			VALUES ('default','sesn_dsync_close','comp_dsync_close','cloud','env_dsync_close','/workspace','{"type":"unrestricted"}','[]','attached','waiting')`,
		`INSERT INTO instances (workspace_id,id,kind,name,registration_state) VALUES ('default','inst_dsync_close','user','dsync','active')`,
		`INSERT INTO instance_credentials (workspace_id,instance_id,id,state,key_digest,expires_at,delivery)
			VALUES ('default','inst_dsync_close','ccred_dsync_close','active',sha256('ccred_dsync_close'::bytea),clock_timestamp()+interval '1 day','response')`,
		`INSERT INTO instance_connections (workspace_id,instance_id,id,credential_id,hub_pod,connected_at,lease_expires_at,stream_count)
			VALUES ('default','inst_dsync_close','conn_0000000000000000000000000000d5c1','ccred_dsync_close','pod',clock_timestamp(),clock_timestamp()+interval '45 seconds',1)`,
	} {
		if _, err := admin.ExecContext(f.ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	if err := dbconnect.NewClientForTesting(admin).WithWorkspaceTx(f.ctx, "default", "test.drive_sync_report", func(tx *dbconnect.Tx) error {
		return runtimecontrol.StoreDriveSyncReportTx(f.ctx, tx, workspace.DefaultID, runtimecontrol.DriveSyncReport{
			SessionID: f.sessionID, ConnectionID: "conn_0000000000000000000000000000d5c1", MessageID: "msg_dsync_close", BindingID: "bind_cli_dsync_close",
			ComputerID: "comp_dsync_close", DriveSyncJSON: `{"committed":0,"deleted":0,"rejected":[],"not_synced":[]}`,
		}, f.childID, time.Now().UTC())
	}); err != nil {
		t.Fatalf("store the drive sync report: %v", err)
	}
	if err := admin.QueryRowContext(f.ctx, `SELECT runtime_input_id FROM session_drive_sync_reports WHERE workspace_id='default' AND session_id=$1`,
		f.sessionID).Scan(&f.inputID); err != nil {
		t.Fatal(err)
	}
	f.store = NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	return f
}

// jobs is the input's runtime_input jobs in creation order.
func (f driveSyncChild) jobs(t *testing.T) []driveSyncJob {
	t.Helper()
	rows, err := f.admin.QueryContext(f.ctx, `SELECT status,payload_json FROM queue_jobs WHERE workspace_id='default' AND kind='runtime_input'
		AND dedupe_key=$1 ORDER BY created_at,id`, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, f.sessionID, f.inputID))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var jobs []driveSyncJob
	for rows.Next() {
		var job driveSyncJob
		var payload string
		if err := rows.Scan(&job.status, &payload); err != nil {
			t.Fatal(err)
		}
		var decoded struct {
			Thread string `json:"session_thread_id"`
		}
		if err := json.Unmarshal([]byte(payload), &decoded); err != nil {
			t.Fatal(err)
		}
		job.thread = decoded.Thread
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return jobs
}

// closeDriveSyncChild closes the child through close_agent: admission, the
// Runtime's commit of the child's interrupt control, its await and the close.
func closeDriveSyncChild(t *testing.T, f driveSyncChild) {
	t.Helper()
	parentScope := sessionfixture.BridgeAPIScope(f.sessionID, f.mainID, f.bindingID, 1, f.podUID)
	source := seedBridgeAPIChildLifecycleToolSource(t, f.admin, f.sessionID, f.mainID, "evt_dsync_close")
	admitted, err := f.store.AdmitChildInterrupt(f.ctx, &bridgev1.AdmitChildInterruptRequest{
		Scope: parentScope, SourceToolUseEventId: source, TargetChildThreadId: f.childID, Action: bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_CLOSE,
	})
	if err != nil {
		t.Fatalf("admit the child close: %v", err)
	}
	controlID := admitted.GetCommitted().GetControlOperationId()
	var controlInputID string
	if err := f.admin.QueryRowContext(f.ctx, `SELECT payload_json::jsonb ->> 'runtime_input_id' FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type=$3 AND payload_json::jsonb ->> 'control_operation_id'=$4`,
		f.sessionID, f.childID, runtimecontrol.ChildInterruptRequestedEventType, controlID).Scan(&controlInputID); err != nil {
		t.Fatalf("read the child's interrupt control: %v", err)
	}
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(f.runtime))
	leased, err := queueStore.Lease(f.ctx, queue.LeaseRequest{WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput},
		LeaseOwner: "drive-sync-close", MaxJobs: 1, LeaseDuration: time.Minute, Now: time.Now().UTC().Add(time.Hour)})
	if err != nil || len(leased) != 1 || leased[0].DedupeKey != queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, f.sessionID, controlInputID) {
		t.Fatalf("lease = %+v, %v; want the interrupt control's job", leased, err)
	}
	if _, err := f.admin.ExecContext(f.ctx, `UPDATE session_runtime_inbox SET status='accepted',binding_id=$2,binding_generation=1,target_pod_uid=$3
		WHERE workspace_id='default' AND runtime_input_id=$1`, controlInputID, f.bindingID, f.podUID); err != nil {
		t.Fatal(err)
	}
	childScope := runtimecontrol.ScopeForThread(parentScope, f.childID)
	if response, err := f.store.CommitInputs(f.ctx, &bridgev1.CommitInputsRequest{Scope: childScope, RuntimeInputId: controlInputID,
		InterruptLeaseRef: &bridgev1.InterruptLeaseRef{JobId: leased[0].ID, LeaseToken: leased[0].LeaseToken, PartitionKey: leased[0].PartitionKey, DedupeKey: leased[0].DedupeKey},
	}); err != nil || response.GetCommitted() == nil {
		t.Fatalf("commit the interrupt control = %+v, %v; want committed", response, err)
	}
	if acked, err := queueStore.Ack(f.ctx, queue.AckRequest{WorkspaceID: workspace.DefaultID, JobID: leased[0].ID, LeaseToken: leased[0].LeaseToken,
		Now: time.Now().UTC().Add(time.Hour)}); err != nil || !acked {
		t.Fatalf("ACK the interrupt control = %v, %v", acked, err)
	}
	if _, err := f.store.AwaitChildInterrupt(f.ctx, &bridgev1.AwaitChildInterruptRequest{Scope: parentScope, ControlOperationId: controlID}); err != nil {
		t.Fatalf("await the child close: %v", err)
	}
	if closed, err := f.store.CloseChildControl(f.ctx, &bridgev1.CloseChildControlRequest{Scope: parentScope, ControlOperationId: controlID}); err != nil || closed.GetCommitted() == nil {
		t.Fatalf("close the child = %+v, %v; want committed", closed, err)
	}
}

// terminateDriveSyncChild commits the Runtime-declared terminal failure of
// the child's open turn.
func terminateDriveSyncChild(t *testing.T, f driveSyncChild) {
	t.Helper()
	childScope := sessionfixture.BridgeAPIScope(f.sessionID, f.childID, f.bindingID, 1, f.podUID)
	seedBridgeAPIOpenDurableTurn(t, f.admin, childScope, "rwrite_dsync_close")
	if _, err := f.store.CommitRuntimeTermination(f.ctx, &bridgev1.CommitRuntimeTerminationRequest{Scope: childScope, RuntimeWriteId: "rwrite_dsync_close",
		FailureJson: `{"type":"runtime","code":"runtime_invalid_sequence","message":"Runtime operation failed.","retryable":false,"fatal":true,"retryStatus":{"type":"terminal"},"reason":"runtime_contract_validation"}`,
	}); err != nil {
		t.Fatalf("terminate the child: %v", err)
	}
}
