package integration

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/integration/enginetest"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// Tests of the hub's lock order: a transaction takes the locks of the
// Sessions it changes before its instance, credential and connection rows,
// so two transactions that meet on a Session and on those rows run one after
// the other and both finish.

// awaitLockWait waits until a backend of the test database waits for a lock.
func (f *machineTools) awaitLockWait() {
	f.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for f.count(`SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock'`) == 0 {
		if time.Now().After(deadline) {
			f.t.Fatal("no transaction waits for a lock within 20 seconds")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// revocation sends DELETE of cli's credential from another goroutine, where
// the test may not stop.
func (f *machineTools) revocation(cli *enginetest.CLI) overlapResult {
	status, body, err := f.controllerSend(0, http.MethodDelete, "/v1/instances/"+cli.InstanceID+"/credentials/"+cli.CredentialID, "")
	return overlapResult{response: enginetest.Response{Status: status, Body: body}, err: err}
}

// overlapResult is what one of two overlapping paths answered.
type overlapResult struct {
	response enginetest.Response
	admitted *bridgev1.AdmitToolCallResponse
	err      error
}

// Each row starts its first path, holds it at the statement pause names
// once the path holds the locks it shares with the second, starts the
// second, which then waits for one of those locks, and releases the first:
// both answer as they would one after the other, and neither fails as a
// deadlock victim.
func TestOverlappingHubTransactionsBothFinish(t *testing.T) {
	for _, row := range []struct {
		name string
		// bridge holds the first path in Bridge's pool, not the hubs'.
		bridge bool
		pause  string
		// arrange returns the two paths and the check of their answers.
		arrange func(f *machineTools) (first, second func() overlapResult, check func(first, second overlapResult))
	}{{
		name:  "a command's report and the revocation that ends its connection",
		pause: "SET ended_at=clock_timestamp()",
		arrange: func(f *machineTools) (func() overlapResult, func() overlapResult, func(overlapResult, overlapResult)) {
			s := f.session()
			computerID, stream := s.preparedUserComputer("laptop")
			_, task := s.admitted(computerID, "sleep 600")
			instruction, _ := stream.execute()
			stream.acked(execution(stream.connectionID, "report_running", instruction, "running", 1, execEnvelope("running", task, "")), "report_running")
			revoke := func() overlapResult { return f.revocation(stream.cli) }
			report := func() overlapResult {
				response, err := stream.cli.Post(f.ctx, "/v1/client/tool-status",
					execution(stream.connectionID, "report_terminal", instruction, "terminal", 2, execEnvelope("success", task, "done")))
				return overlapResult{response: response, err: err}
			}
			return revoke, report, func(revoked, reported overlapResult) {
				if revoked.err != nil || revoked.response.Status != http.StatusNoContent {
					f.t.Fatalf("revocation = %d, %v; want 204", revoked.response.Status, revoked.err)
				}
				if failure, err := reported.response.Error(); reported.err != nil || err != nil || failure.Code != protocol.ErrorCredentialRevoked {
					f.t.Fatalf("report after the revocation = %d %s, %v; want credential_revoked", reported.response.Status, reported.response.Body, reported.err)
				}
				if status := f.row(`SELECT status FROM session_background_tasks WHERE task_id=$1`, task)["status"]; status != "unknown_outcome" {
					f.t.Fatalf("command %v after its connection's end; want unknown_outcome", status)
				}
			}
		},
	}, {
		name:   "Bridge's admission and the revocation that ends the connection it issues on",
		bridge: true,
		pause:  "UPDATE instance_connections SET last_issue_seq",
		arrange: func(f *machineTools) (func() overlapResult, func() overlapResult, func(overlapResult, overlapResult)) {
			s := f.session()
			computerID, stream := s.preparedUserComputer("laptop")
			use := s.use(protocol.ToolExecCommand, "machine_tool")
			admit := func() overlapResult {
				response, err := f.runtime.AdmitToolCall(f.ctx, &bridgev1.AdmitToolCallRequest{Scope: s.scope, ToolUseEventId: use, ComputerId: computerID,
					Tool: protocol.ToolExecCommand, InputJson: `{"cmd":"make"}`, SandboxPolicyJson: disabledNetworkPolicy})
				return overlapResult{admitted: response, err: err}
			}
			revoke := func() overlapResult { return f.revocation(stream.cli) }
			return admit, revoke, func(admitted, revoked overlapResult) {
				if admitted.err != nil || admitted.admitted.GetAdmitted() == nil {
					f.t.Fatalf("admission = %v, %v; want admitted", admitted.admitted, admitted.err)
				}
				if revoked.err != nil || revoked.response.Status != http.StatusNoContent {
					f.t.Fatalf("revocation = %d, %v; want 204", revoked.response.Status, revoked.err)
				}
				s.requireUnknownOutcome(use, runtimecontrol.MessageComputerOfflineResultUnknown)
			}
		},
	}, {
		name:  "a deregistration and an add of a user computer on its instance",
		pause: "instance_id=$2 AND ended_at IS NULL FOR UPDATE",
		arrange: func(f *machineTools) (func() overlapResult, func() overlapResult, func(overlapResult, overlapResult)) {
			cli := f.userInstance("laptop")
			s := f.session()
			s.idle()
			f.buildAPI()
			deregister := func() overlapResult {
				status, body, err := f.controllerSend(0, http.MethodDelete, "/v1/instances/"+cli.InstanceID, "")
				return overlapResult{response: enginetest.Response{Status: status, Body: body}, err: err}
			}
			add := func() overlapResult {
				status, body := f.developerSend(f.ctx, http.MethodPost, "/v1/sessions/"+s.id+"/computers",
					`{"instance_id":"`+cli.InstanceID+`","working_directory":"/Users/me/shop"}`, "add-laptop")
				return overlapResult{response: enginetest.Response{Status: status, Body: body}}
			}
			return deregister, add, func(deregistered, added overlapResult) {
				if deregistered.err != nil || deregistered.response.Status != http.StatusNoContent {
					f.t.Fatalf("deregistration = %d %s, %v; want 204", deregistered.response.Status, deregistered.response.Body, deregistered.err)
				}
				var refusal struct {
					Error struct {
						Type string `json:"type"`
					} `json:"error"`
				}
				if added.response.Status != http.StatusConflict || json.Unmarshal(added.response.Body, &refusal) != nil || refusal.Error.Type != "instance_not_attachable_error" {
					f.t.Fatalf("add during the deregistration = %d %s; want 409 instance_not_attachable_error", added.response.Status, added.response.Body)
				}
				if n := f.count(`SELECT count(*) FROM computers WHERE workspace_id=$1 AND session_id=$2`, string(f.ws), s.id); n != 0 {
					f.t.Fatalf("%d computers in the Session; want none", n)
				}
			}
		},
	}, {
		name:  "a ready with a lost change and a report of the change's Session",
		pause: "INSERT INTO computer_lost_changes",
		arrange: func(f *machineTools) (func() overlapResult, func() overlapResult, func(overlapResult, overlapResult)) {
			s := f.session()
			computerID, stream := s.preparedUserComputer("laptop")
			s.idle()
			s.requireRemoved(computerID, http.StatusAccepted, "closing")
			close, payload := stream.closeInstruction(s.id)
			stream.expire()
			next := stream.reopen()
			ready := func() overlapResult {
				response, err := next.cli.Ready(f.ctx, protocol.Ready{ConnectionID: next.connectionID, MessageID: "ready_" + next.connectionID, OS: "linux", Arch: "x86_64",
					CLIVersion: "1.0.0", Capabilities: everyTool,
					LostChanges: protocol.Some([]protocol.LostChange{{RequestMessageID: close.Envelope.MessageID, DriveID: "drv_lost", Paths: []string{"plan.md"}, PathsTotal: 1}})})
				return overlapResult{response: response, err: err}
			}
			report := func() overlapResult {
				response, err := stream.cli.Post(f.ctx, "/v1/client/session-status", protocol.Encode(protocol.SessionStatus{ConnectionID: stream.connectionID,
					MessageID: "status_late", RequestMessageID: close.Envelope.MessageID, SessionID: s.id, BindingID: payload.BindingID, Status: "closed"}))
				return overlapResult{response: response, err: err}
			}
			return ready, report, func(readied, reported overlapResult) {
				if _, err := readied.response.Grant(); readied.err != nil || err != nil {
					f.t.Fatalf("ready = %d %s, %v; want its grant", readied.response.Status, readied.response.Body, readied.err)
				}
				if failure, err := reported.response.Error(); reported.err != nil || err != nil || failure.Code != protocol.ErrorStaleConnection {
					f.t.Fatalf("report on the ended connection = %d %s, %v; want stale_connection", reported.response.Status, reported.response.Body, reported.err)
				}
				if errs := f.sessionErrors(s.id); len(errs) != 1 || errs[0]["type"] != "drive_changes_lost_error" {
					f.t.Fatalf("Session errors %v; want the lost change's", errs)
				}
			}
		},
	}, {
		name:  "a ready that fulfils a wake request and the request's claim",
		pause: "UPDATE computer_requests SET state='fulfilled'",
		arrange: func(f *machineTools) (func() overlapResult, func() overlapResult, func(overlapResult, overlapResult)) {
			s := f.session()
			computerID := s.cloudComputer()
			cli := s.cloudInstance(computerID)
			s.admitted(computerID, "one")
			wakeID := s.openRequest(computerID)
			stream := f.openStream(cli, cli.EventsQuery("", nil))
			ready := func() overlapResult {
				response, err := cli.Ready(f.ctx, protocol.Ready{ConnectionID: stream.connectionID, MessageID: "ready_" + stream.connectionID, OS: "linux", Arch: "x86_64",
					CLIVersion: "1.0.0", Capabilities: everyTool})
				return overlapResult{response: response, err: err}
			}
			claim := func() overlapResult {
				status, body, err := f.controllerSend(0, http.MethodPost, "/v1/instance_requests/"+wakeID+"/claim", `{"worker_id":"worker_1"}`)
				return overlapResult{response: enginetest.Response{Status: status, Body: body}, err: err}
			}
			return ready, claim, func(readied, claimed overlapResult) {
				if _, err := readied.response.Grant(); readied.err != nil || err != nil {
					f.t.Fatalf("ready = %d %s, %v; want its grant", readied.response.Status, readied.response.Body, readied.err)
				}
				if claimed.err != nil || claimed.response.Status != http.StatusConflict || !strings.Contains(string(claimed.response.Body), "request_not_pending_error") {
					f.t.Fatalf("claim after the ready = %d %s, %v; want 409 request_not_pending_error", claimed.response.Status, claimed.response.Body, claimed.err)
				}
				if state := f.row(`SELECT state FROM computer_requests WHERE id=$1`, wakeID)["state"]; state != "fulfilled" {
					f.t.Fatalf("wake request %v after the ready; want fulfilled", state)
				}
			}
		},
	}, {
		name:  "a closed report that turns its instance unused and a heartbeat of its connection",
		pause: "UPDATE instances SET unused=",
		arrange: func(f *machineTools) (func() overlapResult, func() overlapResult, func(overlapResult, overlapResult)) {
			s := f.session()
			removed := s.cloudComputer()
			s.cloudComputer()
			cli := s.cloudInstance(removed)
			stream := f.connect(cli, everyTool)
			stream.prepare("")
			s.idle()
			s.requireRemoved(removed, http.StatusAccepted, "closing")
			close, payload := stream.closeInstruction(s.id)
			closed := protocol.Encode(protocol.SessionStatus{ConnectionID: stream.connectionID,
				MessageID: "status_closed", RequestMessageID: close.Envelope.MessageID, SessionID: s.id, BindingID: payload.BindingID, Status: "closed"})
			stream.held(enginetest.CaseClosed, close, closed)
			report := func() overlapResult {
				response, err := stream.cli.Post(f.ctx, "/v1/client/session-status", closed)
				return overlapResult{response: response, err: err}
			}
			heartbeat := func() overlapResult {
				response, err := cli.Heartbeat(f.ctx, protocol.Heartbeat{ConnectionID: stream.connectionID, MessageID: "heartbeat_1",
					RequestMessageID: instance.HeartbeatMessageID(stream.connectionID, 1), HeartbeatSeq: 1})
				return overlapResult{response: response, err: err}
			}
			return report, heartbeat, func(reported, beat overlapResult) {
				if ack, err := reported.response.Acknowledgement(); reported.err != nil || err != nil || ack.MessageID != "status_closed" {
					f.t.Fatalf("closed report = %d %s, %v; want its ACK", reported.response.Status, reported.response.Body, reported.err)
				}
				if beat.err != nil || beat.response.Status != http.StatusOK {
					f.t.Fatalf("heartbeat = %d %s, %v; want 200", beat.response.Status, beat.response.Body, beat.err)
				}
				if unused := f.row(`SELECT unused FROM instances WHERE id=$1`, cli.InstanceID)["unused"]; unused != true {
					f.t.Fatalf("instance unused = %v after its last close; want true", unused)
				}
			}
		},
	}} {
		t.Run(row.name, func(t *testing.T) {
			f := newMachineTools(t)
			first, second, check := row.arrange(f)
			tracer := f.hubSQL
			if row.bridge {
				tracer = f.bridgeSQL
			}
			pause := tracer.pauseOn(row.pause)
			firstDone, secondDone := make(chan overlapResult, 1), make(chan overlapResult, 1)
			go func() { firstDone <- first() }()
			select {
			case <-pause.reached:
			case <-time.After(20 * time.Second):
				t.Fatalf("the first path ran no statement with %q within 20 seconds", row.pause)
			}
			go func() { secondDone <- second() }()
			f.awaitLockWait()
			close(pause.release)
			check(<-firstDone, <-secondDone)
		})
	}
}

// A computer that joins the instance between the ready's first read of its
// Sessions and its second makes the ready run again; the second run locks
// that Session too, accepts the ready and prepares the computer.
func TestAReadyRunsAgainWhenAComputerJoinsItsInstance(t *testing.T) {
	f := newMachineTools(t)
	cli := f.userInstance("laptop")
	stream := f.openStream(cli, cli.EventsQuery("", nil))
	s := f.session()
	var reads atomic.Int64
	record := func(sql string, _ time.Duration) {
		if strings.Contains(sql, "UNION SELECT session_id FROM computer_requests") {
			reads.Add(1)
		}
	}
	f.hubSQL.record.Store(&record)
	t.Cleanup(func() { f.hubSQL.record.Store(nil) })
	pause := f.hubSQL.pauseOn("FROM instances WHERE workspace_id=$1 AND id=$2 FOR NO KEY UPDATE")
	done := make(chan overlapResult, 1)
	go func() {
		response, err := cli.Ready(f.ctx, protocol.Ready{ConnectionID: stream.connectionID, MessageID: "ready_" + stream.connectionID, OS: "linux", Arch: "x86_64",
			CLIVersion: "1.0.0", Capabilities: everyTool})
		done <- overlapResult{response: response, err: err}
	}()
	select {
	case <-pause.reached:
	case <-time.After(20 * time.Second):
		t.Fatal("the ready did not lock the instance within 20 seconds")
	}
	computerID := s.userComputer(cli)
	close(pause.release)
	readied := <-done
	if _, err := readied.response.Grant(); readied.err != nil || err != nil {
		t.Fatalf("ready = %d %s, %v; want its grant", readied.response.Status, readied.response.Body, readied.err)
	}
	if n := reads.Load(); n != 4 {
		t.Fatalf("%d reads of the instance's Sessions; want two in each of two runs", n)
	}
	if prepared := stream.prepare(""); prepared.SessionID != s.id {
		t.Fatalf("session.prepare of %s; want %s's computer %s", prepared.SessionID, s.id, computerID)
	}
}
