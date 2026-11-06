package joint

import (
	"context"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
)

// TestPreparationErrorsAreWrittenOncePerCode adds a user computer whose
// working directory does not exist on its online machine to a new Session.
// The machine's CLI fails the preparation with cwd_not_found, creating
// nothing; the computer reads failed with that code and the Session gets one
// computer_preparation_error naming the computer and the code. The next call
// prepares the computer again with a new binding, which fails the same way:
// the call ends not executed with the preparation's error and no second
// event is written. Once the directory exists without access, the call after
// it prepares again and fails with cwd_inaccessible, the second and last
// computer_preparation_error.
func TestPreparationErrorsAreWrittenOncePerCode(t *testing.T) {
	in := requireInputs(t, needs{})
	record := newAcceptanceRecord(t, in, platformOf(in))
	e := startEngine(t, record, engineOptions{})
	controller, developer := e.controller(), e.developer()
	set := newMachines(t, in, needs{}, e, record)
	ctx := t.Context()

	registered := controller.createUserInstance(ctx, "missing-directory")
	m := set.add("user", "user-app")
	m.configure(bootstrap(e.serverURL, registered.instance.ID, registered.credential))
	m.serve()
	requireOnline(t, controller, m, registered.instance.ID, registered.credential.ID)

	missing := m.directory("missing")
	session := developer.createSession(ctx, developer.createAgent(ctx, "missing-directory"), developer.unservedCloudComputer(ctx, "missing-directory"),
		map[string]any{"instance_id": registered.instance.ID, "working_directory": missing})
	computerID := session.Computers[1].ID
	failed := developer.waitComputer(ctx, session.ID, computerID, time.Minute, "a reported preparation", func(c computerObject) bool {
		return c.ExecutionState != "waiting" && c.ExecutionState != "preparing"
	})
	if failed.ExecutionState != "failed" || failed.Error == nil || failed.Error.Code != "cwd_not_found" {
		t.Fatalf("computer %s after its preparation: %+v; want failed with cwd_not_found", computerID, failed)
	}
	if code, _ := m.shell("test", "-e", missing); code == 0 {
		t.Fatalf("%s exists after the failed preparation; the CLI creates no user computer's directory", missing)
	}
	errs := developer.sessionErrors(ctx, session.ID)
	if len(errs) != 1 || errs[0].Error.Type != "computer_preparation_error" || errs[0].Error.ComputerID != computerID ||
		errs[0].Error.Code != "cwd_not_found" || errs[0].Error.RetryStatus.Type != "retrying" {
		t.Fatalf("session.error events of %s: %+v; want one retrying computer_preparation_error of %s with cwd_not_found", session.ID, errs, computerID)
	}
	record.observe("Session %s created with user computer %s in the missing %s: the computer read failed with cwd_not_found (%s), the directory still does not exist, and the Session's events hold one session.error, computer_preparation_error of that computer with code cwd_not_found and retry_status retrying", session.ID, computerID, missing, failed.Error.Message)

	s := e.startRuntime().bind(session.ID)
	again := failedCall(t, s, developer, computerID, *failed.BindingID, "cwd_not_found")
	if code, _ := m.shell("mkdir", "-m", "000", missing); code != 0 {
		t.Fatalf("mkdir -m 000 %s on %s exited %d", missing, m.name, code)
	}
	failedCall(t, s, developer, computerID, *again.BindingID, "cwd_inaccessible")
	errs = developer.sessionErrors(ctx, session.ID)
	if len(errs) != 2 || errs[0].Error.Code != "cwd_not_found" || errs[1].Error.Type != "computer_preparation_error" ||
		errs[1].Error.ComputerID != computerID || errs[1].Error.Code != "cwd_inaccessible" {
		t.Fatalf("session.error events of %s: %+v; want computer_preparation_error with cwd_not_found, then with cwd_inaccessible", session.ID, errs)
	}
	record.observe("the Session's events hold exactly two session.error events, computer_preparation_error of %s with cwd_not_found and then with cwd_inaccessible", computerID)
}

// failedCall admits a command to the failed computer computerID, whose
// binding is previous: the call prepares it again with a new binding, which
// fails with code, and the call ends not executed with that code. It returns
// the computer as it reads after the failure.
func failedCall(t *testing.T, s *runtimeSession, developer *developerClient, computerID, previous, code string) computerObject {
	t.Helper()
	call := s.exec(computerID, "true")
	settled := s.await(call.use, 2*time.Minute)
	if settled.Settlement != "not_executed" || settled.Code != code {
		t.Fatalf("call to %s = %+v; want not_executed with %s", computerID, settled, code)
	}
	failed := developer.waitComputer(t.Context(), s.id, computerID, time.Minute, "a new failed preparation", func(c computerObject) bool {
		return c.ExecutionState == "failed" && c.BindingID != nil && *c.BindingID != previous
	})
	if failed.Error == nil || failed.Error.Code != code {
		t.Fatalf("computer %s after the call's preparation: %+v; want failed with %s", computerID, failed, code)
	}
	s.r.e.record.observe("the call prepared %s again with binding %s (was %s), which failed with %s; the call ended not executed with %s %q", computerID, *failed.BindingID, previous, code, settled.Code, settled.Message)
	return failed
}

// TestDeregisteredCloudInstanceReplacesItsComputer deregisters the current
// instance of a cloud computer whose machine is online. The Session gets no
// session.error and one session.computer_replaced of only type, id,
// processed_at and computer_id, written in the transaction of the replaced
// machine message; the computer's next call opens a new request.
func TestDeregisteredCloudInstanceReplacesItsComputer(t *testing.T) {
	e, set, record := scenario(t, engineOptions{})
	ctx := t.Context()
	developer, controller := e.developer(), e.controller()
	cloud := set.add("cloud", "provider-launcher")
	s := developer.createCloudSession(ctx, "replaced", cloud.directory("workspace"))
	rs := e.startRuntime().bind(s.sessionID)
	_, instanceID := startClaimedMachine(t, e, rs, cloud, s)
	controller.deregister(ctx, instanceID)
	cloud.waitStatus(time.Minute, "blocked", func(status map[string]any) bool { return statusField(status, "connection_state") == "blocked" })
	record.observe("deregistered cloud instance %s; its CLI is blocked", instanceID)

	var page struct {
		Data []map[string]any `json:"data"`
	}
	developer.get(ctx, "/v1/sessions/"+s.sessionID+"/events?beta=true&limit=100", &page)
	var replaced []map[string]any
	for _, event := range page.Data {
		switch event["type"] {
		case "session.computer_replaced":
			replaced = append(replaced, event)
		case "session.error":
			t.Fatalf("the Session got %v", event)
		}
	}
	if len(replaced) != 1 || len(replaced[0]) != 4 || replaced[0]["computer_id"] != s.computerID || replaced[0]["id"] == nil || replaced[0]["processed_at"] == nil {
		t.Fatalf("session.computer_replaced events %v; want one with only type, id, processed_at and computer_id %s", replaced, s.computerID)
	}
	var sameTransaction bool
	e.ownerTx(func(ctx context.Context, tx *dbconnect.Tx) error {
		return tx.QueryRow(ctx, `SELECT e.xmin=m.xmin FROM session_events e, session_machine_messages m
			WHERE e.workspace_id=$1 AND e.event_id=$2 AND m.workspace_id=$1 AND m.session_id=$3 AND m.computer_id=$4 AND m.change='replaced'`,
			harnessWorkspace, replaced[0]["id"], s.sessionID, s.computerID).Scan(&sameTransaction)
	})
	if !sameTransaction {
		t.Fatal("session.computer_replaced and the replaced machine message were written in different transactions")
	}
	record.observe("the Session's events hold no session.error and one session.computer_replaced %v, written in the transaction of the replaced machine message", replaced[0])

	_, request := waitingCall(t, e, rs, s, "pwd")
	if request.Kind != "new" {
		t.Fatalf("the next call's request %+v; want new", request)
	}
	record.observe("the computer's next call opened new request %s", request.ID)
}

// TestUnavailableIsolationIsReportedOncePerSession runs a machine whose
// platform isolation is unavailable: three commands of one Session and one of
// another, on one connection, are rejected with sandbox_unavailable, and each
// Session gets one computer_sandbox_error.
func TestUnavailableIsolationIsReportedOncePerSession(t *testing.T) {
	in := requireInputs(t, needs{platforms: []string{"linux"}})
	record := newAcceptanceRecord(t, in, platformOf(in))
	record.boundary("Linux only: isolation is made unavailable by an App Server PATH with no bwrap; macOS has no unprivileged way to take sandbox-exec away")
	e := startEngine(t, record, engineOptions{})
	set := newMachines(t, in, needs{}, e, record)
	m := unisolatedMachine(t, e, set, "unisolated")
	s1, c1, r1 := driveSession(t, e, "first", nil, m)
	s2, c2, r2 := driveSession(t, e, "second", nil, m)
	for range 3 {
		r1.requireRejected(r1.exec(c1[0], "true"), "sandbox_unavailable")
	}
	r2.requireRejected(r2.exec(c2[0], "true"), "sandbox_unavailable")
	for _, s := range []string{s1.ID, s2.ID} {
		if errs := errorsOfType(e, s, "computer_sandbox_error"); len(errs) != 1 {
			t.Fatalf("Session %s has %d computer_sandbox_error events; want 1: %+v", s, len(errs), errs)
		}
	}
	record.observe("three calls of %s and one of %s were rejected sandbox_unavailable on one connection; each Session has one computer_sandbox_error", s1.ID, s2.ID)
}
