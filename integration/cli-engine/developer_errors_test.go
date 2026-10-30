package joint

import (
	"testing"
	"time"
)

// TestPreparationErrorsAreWrittenOncePerCode adds a user computer whose
// working directory does not exist on its online machine to a new Session.
// The machine's CLI fails the preparation with cwd_not_found, creating
// nothing; the computer reads failed with that code and the Session gets one
// computer_preparation_error naming the computer and the code.
func TestPreparationErrorsAreWrittenOncePerCode(t *testing.T) {
	in := requireInputs(t, needs{})
	record := newAcceptanceRecord(t, in, platformOf(in))
	record.boundary("no call prepares the computer again: no Engine path admits a tool call to a computer, so neither the repeated cwd_not_found failure nor the cwd_inaccessible one is observed")
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
}
