package jobrunner

import (
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// Drive sync reports of a Session no Runtime holds stay queued and place no
// Runtime; the binding of the Session's next placement delivers each once,
// with the report's task, computer and drive_sync as its payload.
func TestDriveSyncReportsWaitForTheNextRuntime(t *testing.T) {
	f := newMachineMessageFixture(t, "sesn_dsync_wait")
	for _, statement := range []string{
		`INSERT INTO instances (workspace_id,id,kind,name,registration_state) VALUES ('default','inst_dsync_wait','user','dsync','active')`,
		`INSERT INTO instance_credentials (workspace_id,instance_id,id,state,key_digest,expires_at,delivery)
			VALUES ('default','inst_dsync_wait','ccred_dsync_wait','active',sha256('ccred_dsync_wait'::bytea),clock_timestamp()+interval '1 day','response')`,
		`INSERT INTO instance_connections (workspace_id,instance_id,id,credential_id,hub_pod,connected_at,lease_expires_at,stream_count)
			VALUES ('default','inst_dsync_wait','conn_0000000000000000000000000000d5a1','ccred_dsync_wait','pod',clock_timestamp(),clock_timestamp()+interval '45 seconds',1)`,
	} {
		f.exec(statement)
	}
	const driveSync = `{"committed":0,"deleted":0,"rejected":[{"path":"/tetral/drives/drv_notes/a.txt","change":"write","reason":"limit","rule":"max_bytes"}],"not_synced":[]}`
	hub := dbconnect.NewClientForTesting(f.workloads.OpenWorkload(t, "computer_hub", nil))
	for _, report := range []runtimecontrol.DriveSyncReport{
		{MessageID: "msg_dsync_task", TaskID: "task_00000000000000a1"},
		{MessageID: "msg_dsync_untasked"},
	} {
		report.SessionID, report.ConnectionID, report.BindingID, report.ComputerID, report.DriveSyncJSON = f.sessionID, "conn_0000000000000000000000000000d5a1", "bind_cli_dsync_wait", f.computerID, driveSync
		if err := hub.WithWorkspaceTx(f.ctx, "default", "test.drive_sync_report", func(tx *dbconnect.Tx) error {
			return runtimecontrol.StoreDriveSyncReportTx(f.ctx, tx, workspace.DefaultID, report, f.mainID, time.Now().UTC())
		}); err != nil {
			t.Fatalf("store %s: %v", report.MessageID, err)
		}
	}
	if processed := f.deliverAll(); processed != 2 {
		t.Fatalf("processed %d jobs; want two", processed)
	}
	if queued, active := f.driveSyncInputs(); queued != 2 || active != 0 {
		t.Fatalf("drive_sync inputs with no Runtime: %d queued, %d active jobs; want 2 and 0", queued, active)
	}
	if _, bindings, _ := f.sessionFacts(); bindings != 0 || len(f.sender.requests) != 0 {
		t.Fatalf("reports made %d bindings and %d Runtime calls; want none", bindings, len(f.sender.requests))
	}

	placed := f.placeUserMessage("dsync_wait_user")
	if processed := f.deliverAll(); processed != 2 {
		t.Fatalf("processed %d jobs after the placement; want the two reports'", processed)
	}
	payloads := map[string]int{}
	for _, request := range f.nonStartingRequests() {
		if request.GetInputKind() != queue.RuntimeInputKindDriveSync || request.GetBindingId() != placed || request.GetSessionThreadId() != f.mainID {
			t.Fatalf("Runtime request = %v; want drive_sync on the main thread to %s", request, placed)
		}
		payloads[request.GetPayloadJson()]++
	}
	want := []string{
		`{"task_id":"task_00000000000000a1","computer_id":"` + f.computerID + `","drive_sync":` + driveSync + `}`,
		`{"task_id":null,"computer_id":"` + f.computerID + `","drive_sync":` + driveSync + `}`,
	}
	if len(payloads) != 2 || payloads[want[0]] != 1 || payloads[want[1]] != 1 {
		t.Fatalf("payloads = %v; want each of %v once", payloads, want)
	}
}

// driveSyncInputs counts the Session's queued drive_sync inputs and their
// pending or leased jobs.
func (f *machineMessageFixture) driveSyncInputs() (queued, active int) {
	f.t.Helper()
	if err := f.admin.QueryRowContext(f.ctx, `SELECT
		(SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND input_kind='drive_sync' AND status='queued'),
		(SELECT count(*) FROM queue_jobs j JOIN session_runtime_inbox i ON j.workspace_id=i.workspace_id
		   AND j.dedupe_key='runtime_input:'||i.workspace_id||':'||i.session_id||':'||i.runtime_input_id
		  WHERE i.workspace_id='default' AND i.session_id=$1 AND i.input_kind='drive_sync' AND j.status IN ('pending','leased'))`,
		f.sessionID).Scan(&queued, &active); err != nil {
		f.t.Fatal(err)
	}
	return queued, active
}
