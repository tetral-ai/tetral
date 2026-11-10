package integration

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
)

// deregister sends DELETE of the instance, which must answer 204.
func (f *machineTools) deregister(instanceID string) {
	f.t.Helper()
	if status, body := f.controller(http.MethodDelete, "/v1/instances/"+instanceID, ""); status != http.StatusNoContent {
		f.t.Fatalf("DELETE instance %s = %d %s", instanceID, status, body)
	}
}

// machineMessages counts the Session's machine messages of change.
func (s *toolSession) machineMessages(change string) int {
	s.f.t.Helper()
	return s.f.count(`SELECT count(*) FROM session_machine_messages WHERE session_id=$1 AND change=$2`, s.id, change)
}

// publicEvents returns the Session's events of eventType as the developer
// events route answers them.
func (s *toolSession) publicEvents(eventType string) []map[string]any {
	s.f.t.Helper()
	status, body := s.f.developer(http.MethodGet, "/v1/sessions/"+s.id+"/events", "")
	var page struct {
		Data []map[string]any `json:"data"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &page) != nil {
		s.f.t.Fatalf("events of %s = %d %s", s.id, status, body)
	}
	var events []map[string]any
	for _, event := range page.Data {
		if event["type"] == eventType {
			events = append(events, event)
		}
	}
	return events
}

// Deregistering a user instance that three Sessions use, one of them
// running, ends its connection and removes it from each Session: each
// computer is detached and unknown with one removed machine message, and
// no session.close is sent. A repeated DELETE changes nothing.
func TestDeregisteringAUserInstanceRemovesItFromEverySession(t *testing.T) {
	f := newMachineTools(t)
	cli := f.userInstance("laptop")
	stream := f.connect(cli, everyTool)
	var sessions []*toolSession
	var computers []string
	for i := range 3 {
		s := f.session()
		computerID := s.userComputer(cli)
		f.ownerTx(func(tx *dbconnect.Tx) error {
			_, err := computer.IssuePreparation(f.ctx, tx, f.ws, s.id, computerID)
			return err
		})
		stream.prepare("")
		if i > 0 {
			s.idle()
		}
		sessions, computers = append(sessions, s), append(computers, computerID)
	}

	for range 2 {
		f.deregister(cli.InstanceID)
		for i, s := range sessions {
			if row := f.computerRow(computers[i]); row["association_state"] != "detached" || row["execution_state"] != "unknown" {
				t.Fatalf("computer %s = %v; want detached and unknown", computers[i], row)
			}
			if n := s.machineMessages("removed"); n != 1 {
				t.Fatalf("%d removed machine messages of %s; want 1", n, s.id)
			}
		}
		if row := f.row(`SELECT ended_at IS NOT NULL AS ended, end_cause FROM instance_connections WHERE id=$1`, stream.connectionID); row["ended"] != true || row["end_cause"] != "deregistered" {
			t.Fatalf("connection = %v; want ended by the deregistration", row)
		}
		if n := f.count(`SELECT count(*) FROM connection_instructions WHERE connection_id=$1 AND type='session.close'`, stream.connectionID); n != 0 {
			t.Fatalf("%d session.close instructions; want none", n)
		}
	}
}

// Deregistering the current instance of an attached cloud computer retires
// it: the computer keeps its ID with no current instance, its Session gets
// one replaced machine message and one session.computer_replaced of four
// keys and no session.error, and its next request is new. Deregistering
// the instance of a failed new claim or of a removed cloud computer writes
// neither; a repeated DELETE writes nothing more.
func TestDeregisteringACloudInstanceReplacesItsComputer(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	computerID := s.cloudComputer()
	cli := s.cloudInstance(computerID)
	stream := f.connect(cli, everyTool)
	stream.prepare("")
	removed := s.cloudComputer()
	removedCLI := s.cloudInstance(removed)
	failed := s.cloudComputer()
	s.admitted(failed, "one")
	failedRequest := s.openRequest(failed)
	claim := f.claim(0, failedRequest, "worker-failed")
	if status, body := f.controller(http.MethodPost, "/v1/instance_requests/"+failedRequest+"/fail", ""); status != http.StatusOK {
		t.Fatalf("fail = %d %s", status, body)
	}
	s.idle()
	s.requireRemoved(removed, http.StatusNoContent, "")

	for range 2 {
		f.deregister(cli.InstanceID)
		if row := f.row(`SELECT association_state, execution_state, current_instance_id, binding_id FROM computers WHERE id=$1`, computerID); row["association_state"] != "attached" ||
			row["execution_state"] != "waiting" || row["current_instance_id"] != nil || row["binding_id"] != nil {
			t.Fatalf("computer after deregistration = %v; want attached and waiting with no current instance", row)
		}
		if n := s.machineMessages("replaced"); n != 1 {
			t.Fatalf("%d replaced machine messages; want 1", n)
		}
		notices := s.publicEvents("session.computer_replaced")
		if len(notices) != 1 {
			t.Fatalf("notices = %v; want one", notices)
		}
		var keys []string
		for key := range notices[0] {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		if strings.Join(keys, ",") != "computer_id,id,processed_at,type" || notices[0]["computer_id"] != computerID {
			t.Fatalf("notice = %v; want type, id, processed_at and computer_id %s", notices[0], computerID)
		}
		if errs := f.sessionErrors(s.id); len(errs) != 0 {
			t.Fatalf("errors = %v; want none", errs)
		}
	}
	row := f.row(`SELECT payload_json::jsonb->'row'->>'computerId' AS computer FROM session_machine_messages WHERE session_id=$1 AND change='replaced'`, s.id)
	if row["computer"] != computerID {
		t.Fatalf("replaced machine message = %v", row)
	}
	f.ownerTx(func(tx *dbconnect.Tx) error {
		request, _, err := computer.EnsureRequest(f.ctx, tx, f.ws, s.id, computerID)
		if err == nil && request.Kind != computer.RequestNew {
			t.Fatalf("next request = %+v; want new", request)
		}
		return err
	})

	f.deregister(claim.Bootstrap.InstanceID)
	f.deregister(removedCLI.InstanceID)
	if n := s.machineMessages("replaced"); n != 1 || len(s.publicEvents("session.computer_replaced")) != 1 {
		t.Fatalf("deregistering instances no attached computer uses wrote %d replaced messages; want only the first", n)
	}
}

// Deregistering the current instance of a cloud computer whose call waits
// on a wake request, pending or claimed, fails the request in the same
// transaction and ends the call not executed; a later fail answers 200
// unchanged, a claim 409, and the next call records a new request.
func TestDeregisteringFailsTheInstancesWakeRequest(t *testing.T) {
	for _, claimed := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending", true: "claimed"}[claimed], func(t *testing.T) {
			f := newMachineTools(t)
			s := f.session()
			computerID := s.cloudComputer()
			cli := s.cloudInstance(computerID)
			use, _ := s.admitted(computerID, "one")
			wake := s.openRequest(computerID)
			if claimed {
				f.claim(0, wake, "worker-wake")
			}
			f.deregister(cli.InstanceID)
			if state := f.row(`SELECT state FROM computer_requests WHERE id=$1`, wake)["state"]; state != "failed" {
				t.Fatalf("wake request = %v; want failed", state)
			}
			s.requireNotExecuted(use, runtimecontrol.CodeMachineNotStarted, runtimecontrol.MessageMachineNotStarted)
			if status, body := f.controller(http.MethodPost, "/v1/instance_requests/"+wake+"/fail", ""); status != http.StatusOK || !strings.Contains(string(body), `"failed"`) {
				t.Fatalf("later fail = %d %s; want 200 unchanged", status, body)
			}
			if status, body := f.controller(http.MethodPost, "/v1/instance_requests/"+wake+"/claim", `{"worker_id":"worker-late"}`); status != http.StatusConflict ||
				!strings.Contains(string(body), "request_not_pending_error") {
				t.Fatalf("later claim = %d %s; want 409 request_not_pending_error", status, body)
			}
			s.admitted(computerID, "two")
			if kind := f.row(`SELECT kind FROM computer_requests WHERE id=$1`, s.openRequest(computerID))["kind"]; kind != "new" {
				t.Fatalf("next request = %v; want new", kind)
			}
		})
	}
}
