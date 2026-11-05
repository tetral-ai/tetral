package integration

import (
	"encoding/json"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/integration/enginetest"
	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/drive"
	"github.com/tetral-ai/tetral/internal/protocol"
	tetralcleanup "github.com/tetral-ai/tetral/services/cleanup"
)

// createDrive creates a drive through the drive API and returns its ID.
func (f *machineTools) createDrive() string {
	f.t.Helper()
	status, body := f.developer(http.MethodPost, "/v1/drives", `{}`)
	var object struct {
		ID string `json:"id"`
	}
	if status != http.StatusCreated || json.Unmarshal(body, &object) != nil || object.ID == "" {
		f.t.Fatalf("POST /v1/drives = %d %s", status, body)
	}
	return object.ID
}

// requireAnswer checks that a developer request answers status with an
// error body of errorType.
func (f *machineTools) requireAnswer(method, path, body string, status int, errorType string) {
	f.t.Helper()
	got, response := f.developer(method, path, body)
	if got != status || !strings.Contains(string(response), `"type":"`+errorType+`"`) {
		f.t.Fatalf("%s %s = %d %s; want %d %s", method, path, got, response, status, errorType)
	}
}

func driveBody(driveID, access string) string {
	return `{"drive_id":` + quoteJSON(driveID) + `,"access":` + quoteJSON(access) + `}`
}

// bind adds a drive binding, which must answer 201 with exactly the binding.
func (s *toolSession) bind(driveID, access string) {
	s.f.t.Helper()
	status, body := s.f.developer(http.MethodPost, "/v1/sessions/"+s.id+"/drives", driveBody(driveID, access))
	requireBindingBody(s.f.t, status, body, http.StatusCreated, driveID, access)
}

// rebind changes a binding's access, which must answer 200 with exactly the
// binding.
func (s *toolSession) rebind(driveID, access string) {
	s.f.t.Helper()
	status, body := s.f.developer(http.MethodPost, "/v1/sessions/"+s.id+"/drives/"+driveID, `{"access":`+quoteJSON(access)+`}`)
	requireBindingBody(s.f.t, status, body, http.StatusOK, driveID, access)
}

// unbind removes a binding, which must answer 204.
func (s *toolSession) unbind(driveID string) {
	s.f.t.Helper()
	if status, body := s.f.developer(http.MethodDelete, "/v1/sessions/"+s.id+"/drives/"+driveID, ""); status != http.StatusNoContent {
		s.f.t.Fatalf("DELETE drive %s = %d %s; want 204", driveID, status, body)
	}
}

func requireBindingBody(t testing.TB, status int, body []byte, want int, driveID, access string) {
	t.Helper()
	var object map[string]any
	if status != want || json.Unmarshal(body, &object) != nil || !reflect.DeepEqual(object, map[string]any{"drive_id": driveID, "access": access}) {
		t.Fatalf("binding answer = %d %s; want %d with exactly {drive_id: %s, access: %s}", status, body, want, driveID, access)
	}
}

// sessionDrives reads the drives of the Session object of sessionID.
func (f *machineTools) sessionDrives(sessionID string) []protocol.DriveBinding {
	f.t.Helper()
	status, body := f.developer(http.MethodGet, "/v1/sessions/"+sessionID, "")
	return decodeSessionDrives(f.t, status, body)
}

func decodeSessionDrives(t testing.TB, status int, body []byte) []protocol.DriveBinding {
	t.Helper()
	var object struct {
		Drives []struct {
			DriveID string `json:"drive_id"`
			Access  string `json:"access"`
		} `json:"drives"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &object) != nil || object.Drives == nil {
		t.Fatalf("Session object = %d %s; want one with drives", status, body)
	}
	drives := []protocol.DriveBinding{}
	for _, d := range object.Drives {
		drives = append(drives, protocol.DriveBinding{DriveID: d.DriveID, Access: d.Access})
	}
	return drives
}

// sorted is drives in UTF-8 byte order of drive_id.
func sorted(drives ...protocol.DriveBinding) []protocol.DriveBinding {
	sort.Slice(drives, func(i, j int) bool { return drives[i].DriveID < drives[j].DriveID })
	return drives
}

func rw(driveID string) protocol.DriveBinding {
	return protocol.DriveBinding{DriveID: driveID, Access: "read_write"}
}

func ro(driveID string) protocol.DriveBinding {
	return protocol.DriveBinding{DriveID: driveID, Access: "read_only"}
}

// drivesSent counts the session.drives instructions sent for sessionID.
func (f *machineTools) drivesSent(sessionID string) int {
	f.t.Helper()
	return f.count(`SELECT count(*) FROM connection_instructions WHERE type='session.drives'
		AND convert_from(envelope,'UTF8')::jsonb#>>'{payload,session_id}'=$1`, sessionID)
}

// driveSet reads the drive set of bindingID.
func (f *machineTools) driveSet(bindingID string) []protocol.DriveBinding {
	f.t.Helper()
	var drives []protocol.DriveBinding
	f.ownerTx(func(tx *dbconnect.Tx) error {
		var err error
		drives, err = drive.BindingDriveSet(f.ctx, tx, f.ws, bindingID)
		return err
	})
	return drives
}

// bindingOf is the binding of computerID.
func (f *machineTools) bindingOf(computerID string) string {
	f.t.Helper()
	return f.computerRow(computerID)["binding_id"].(string)
}

// running starts s's turn again, so a call can be admitted.
func (s *toolSession) running() {
	s.f.t.Helper()
	s.f.exec(`UPDATE session_runtime_status SET status='running' WHERE workspace_id=$1 AND session_id=$2`, string(s.f.ws), s.id)
}

// drivesInstruction reads the next event, which must be a session.drives
// to bindingID with drives.
func (st *cliStream) drivesInstruction(bindingID string, drives []protocol.DriveBinding) enginetest.Event {
	st.f.t.Helper()
	event := st.next()
	payload, ok := event.Envelope.Payload.(protocol.SessionDrivesPayload)
	if !ok || payload.BindingID != bindingID || !reflect.DeepEqual(payload.Drives, drives) {
		st.f.t.Fatalf("event = %s; want session.drives to %s with %v", event.Data, bindingID, drives)
	}
	return event
}

// nextPrepare reads the next event, which must be a session.prepare with
// drives.
func (st *cliStream) nextPrepare(drives []protocol.DriveBinding) enginetest.Event {
	st.f.t.Helper()
	event := st.next()
	payload, ok := event.Envelope.Payload.(protocol.SessionPreparePayload)
	if !ok || !reflect.DeepEqual(payload.Drives, drives) {
		st.f.t.Fatalf("event = %s; want session.prepare with %v", event.Data, drives)
	}
	return event
}

// appliedReport is the applied report of the session.drives event as
// messageID.
func (st *cliStream) appliedReport(event enginetest.Event, messageID string) protocol.SessionStatus {
	payload := event.Envelope.Payload.(protocol.SessionDrivesPayload)
	return protocol.SessionStatus{ConnectionID: st.connectionID, MessageID: messageID, RequestMessageID: event.Envelope.MessageID,
		SessionID: payload.SessionID, BindingID: payload.BindingID, Status: "applied"}
}

// refusedReport posts report, which must answer 409 unknown_instruction.
func (st *cliStream) refusedReport(report protocol.SessionStatus) {
	st.f.t.Helper()
	response, err := st.cli.Post(st.f.ctx, "/v1/client/session-status", protocol.Encode(report))
	if err != nil {
		st.f.t.Fatal(err)
	}
	if failure, err := response.Error(); response.Status != http.StatusConflict || err != nil || failure.Code != protocol.ErrorUnknownInstruction {
		st.f.t.Fatalf("report = %d %s; want 409 unknown_instruction", response.Status, response.Body)
	}
}

// padPrepare sets computerID's drive exclusions so that its session.prepare
// with drives is size bytes, or clears them when size is 0.
func (s *toolSession) padPrepare(computerID string, drives []protocol.DriveBinding, size int) {
	s.f.t.Helper()
	exclude := []computer.DriveExclude{}
	if size > 0 {
		paths := make([]string, 65)
		for i := range paths {
			paths[i] = strings.Repeat("a", 1000)
		}
		exclude = []computer.DriveExclude{{DriveID: "drive_padding", Paths: paths}}
		over := computer.PrepareEnvelopeSize(s.id, computer.KindUser, "/Users/me/shop", exclude, nil, drives) - size
		paths[64] = strings.Repeat("a", 1000-over)
		if got := computer.PrepareEnvelopeSize(s.id, computer.KindUser, "/Users/me/shop", exclude, nil, drives); got != size {
			s.f.t.Fatalf("padded session.prepare is %d bytes; want %d", got, size)
		}
	}
	raw, err := json.Marshal(exclude)
	if err != nil {
		s.f.t.Fatal(err)
	}
	s.f.exec(`UPDATE computers SET drive_exclude=$2 WHERE id=$1`, computerID, string(raw))
}

// Creation refuses a drive listed twice, another access and a drive that is
// missing or deleted, and creates no drive; the three routes answer their statuses and
// bodies, refuse a running Session, and send one session.drives per change
// to the prepared binding and none for the same access; a bound drive cannot
// be deleted until its binding is removed; an addition after which the
// computer's session.prepare would exceed 65,536 bytes, alone or with the
// binding's unapplied drive set, is refused and writes nothing, and one after
// which it is exactly 65,536 bytes is accepted.
func TestSessionDriveRoutesAnswerTheirTable(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	s.idle()
	team, notes, deleted := f.createDrive(), f.createDrive(), f.createDrive()
	if status, body := f.developer(http.MethodDelete, "/v1/drives/"+deleted, ""); status != http.StatusNoContent {
		t.Fatalf("deleting a drive = %d %s; want 204", status, body)
	}

	create := func(drives string) (int, []byte) {
		return f.developer(http.MethodPost, "/v1/sessions", `{"agent":"agent_`+s.id+`","vault_ids":[],"computers":[{"environment_id":"env_`+s.id+`"}],"drives":`+drives+`}`)
	}
	sessions, drives := f.count(`SELECT count(*) FROM sessions`), f.count(`SELECT count(*) FROM drives`)
	for _, row := range []struct {
		name, drives string
		status       int
		errorType    string
	}{
		{"a drive listed twice", "[" + driveBody(team, "read_only") + "," + driveBody(team, "read_write") + "]", http.StatusBadRequest, "invalid_request_error"},
		{"another access", "[" + driveBody(team, "write") + "]", http.StatusBadRequest, "invalid_request_error"},
		{"no access", `[{"drive_id":` + quoteJSON(team) + `}]`, http.StatusBadRequest, "invalid_request_error"},
		{"a drive that does not exist", "[" + driveBody("drive_missing", "read_only") + "]", http.StatusNotFound, "not_found_error"},
		{"a deleted drive", "[" + driveBody(deleted, "read_only") + "]", http.StatusNotFound, "not_found_error"},
	} {
		if status, body := create(row.drives); status != row.status || !strings.Contains(string(body), `"type":"`+row.errorType+`"`) {
			t.Fatalf("creation with %s = %d %s; want %d %s", row.name, status, body, row.status, row.errorType)
		}
	}
	if f.count(`SELECT count(*) FROM sessions`) != sessions || f.count(`SELECT count(*) FROM drives`) != drives {
		t.Fatal("a refused creation left a Session or created a drive")
	}
	docs, specs := f.createDrive(), f.createDrive()
	status, body := create("[" + driveBody(specs, "read_write") + "," + driveBody(docs, "read_only") + "]")
	if got, want := decodeSessionDrives(t, status, body), sorted(rw(specs), ro(docs)); !reflect.DeepEqual(got, want) {
		t.Fatalf("created Session drives = %v; want %v", got, want)
	}

	s.running()
	f.requireAnswer(http.MethodPost, "/v1/sessions/"+s.id+"/drives", driveBody(team, "read_write"), http.StatusConflict, "invalid_request_error")
	s.idle()
	computerID, stream := s.preparedUserComputer("routes")
	bindingID := f.bindingOf(computerID)
	s.bind(team, "read_write")
	stream.drivesInstruction(bindingID, []protocol.DriveBinding{rw(team)})
	f.requireAnswer(http.MethodPost, "/v1/sessions/"+s.id+"/drives", driveBody(team, "read_only"), http.StatusConflict, "invalid_request_error")
	s.rebind(team, "read_write")
	stream.quiet(300 * time.Millisecond)
	s.rebind(team, "read_only")
	stream.drivesInstruction(bindingID, []protocol.DriveBinding{ro(team)})
	f.requireAnswer(http.MethodPost, "/v1/sessions/"+s.id+"/drives/"+notes, `{"access":"read_only"}`, http.StatusNotFound, "not_found_error")
	f.requireAnswer(http.MethodDelete, "/v1/sessions/"+s.id+"/drives/"+notes, "", http.StatusNotFound, "not_found_error")
	f.requireAnswer(http.MethodDelete, "/v1/drives/"+team, "", http.StatusConflict, "invalid_request_error")
	s.unbind(team)
	applied := stream.drivesInstruction(bindingID, []protocol.DriveBinding{})
	if status, body := f.developer(http.MethodDelete, "/v1/drives/"+team, ""); status != http.StatusNoContent {
		t.Fatalf("deleting the unbound drive = %d %s; want 204", status, body)
	}
	if got := f.sessionDrives(s.id); len(got) != 0 {
		t.Fatalf("Session drives after removing the last one = %v; want none", got)
	}
	stream.post(stream.appliedReport(applied, "applied_routes"))

	sent := f.drivesSent(s.id)
	s.padPrepare(computerID, []protocol.DriveBinding{}, 65536-20)
	f.requireAnswer(http.MethodPost, "/v1/sessions/"+s.id+"/drives", driveBody(notes, "read_only"), http.StatusBadRequest, "invalid_request_error")
	s.padPrepare(computerID, []protocol.DriveBinding{ro(notes)}, 65536)
	s.bind(notes, "read_only")
	s.padPrepare(computerID, nil, 0)
	stream.drivesInstruction(bindingID, []protocol.DriveBinding{ro(notes)})
	s.unbind(notes)
	stream.drivesInstruction(bindingID, []protocol.DriveBinding{})
	logs := f.createDrive()
	s.padPrepare(computerID, []protocol.DriveBinding{ro(logs)}, 65536)
	if got := f.driveSet(bindingID); !reflect.DeepEqual(got, []protocol.DriveBinding{ro(notes)}) {
		t.Fatalf("binding drive set = %v; want the unapplied %s", got, notes)
	}
	f.requireAnswer(http.MethodPost, "/v1/sessions/"+s.id+"/drives", driveBody(logs, "read_only"), http.StatusBadRequest, "invalid_request_error")
	if f.count(`SELECT count(*) FROM session_drive_bindings WHERE session_id=$1`, s.id) != 0 || f.drivesSent(s.id) != sent+2 {
		t.Fatal("a refused addition stored a binding or sent session.drives")
	}
	stream.quiet(300 * time.Millisecond)
}

// A drive bound only by a deleted Session can be deleted, and the drive
// cleanup, on the installed cleanup role, then removes its row with that
// Session's binding.
func TestADriveBoundOnlyByADeletedSessionIsCleanedUp(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	s.idle()
	team := f.createDrive()
	s.bind(team, "read_write")
	if status, body := f.developer(http.MethodDelete, "/v1/sessions/"+s.id, ""); status != http.StatusOK {
		t.Fatalf("DELETE of the Session = %d %s; want 200", status, body)
	}
	if status, body := f.developer(http.MethodDelete, "/v1/drives/"+team, ""); status != http.StatusNoContent {
		t.Fatalf("DELETE of a drive bound only by a deleted Session = %d %s; want 204", status, body)
	}
	runner := dbconnect.NewClientForTesting(f.workloads.OpenWorkload(t, "cleanup", nil))
	if err := tetralcleanup.RunDrives(f.ctx, runner, f.blobs, nil); err != nil {
		t.Fatalf("drive cleanup: %v", err)
	}
	if drives, bindings := f.count(`SELECT count(*) FROM drives WHERE drive_id=$1`, team),
		f.count(`SELECT count(*) FROM session_drive_bindings WHERE drive_id=$1`, team); drives != 0 || bindings != 0 {
		t.Fatalf("after the drive cleanup: %d drive rows, %d bindings; want none", drives, bindings)
	}
}

// After a removal, each prepared binding gets one session.drives and the
// binding whose preparation is outstanding none until its prepared report,
// then exactly one; a binding's drive set keeps the removed drive until its
// applied report and drops it then. A protocol-error report naming a
// session.drives changes nothing. Removing the last drive sends an empty
// session.drives, whose applied report empties the drive set.
func TestDriveChangesReachEachPreparedBinding(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	s.idle()
	team, notes := f.createDrive(), f.createDrive()
	s.bind(team, "read_write")
	s.bind(notes, "read_write")
	both := sorted(rw(team), rw(notes))
	first, firstStream := s.preparedUserComputer("first")
	second, secondStream := s.preparedUserComputer("second")
	thirdCLI := f.userInstance("third")
	thirdStream := f.connect(thirdCLI, everyTool)
	third := s.userComputer(thirdCLI)
	f.ownerTx(func(tx *dbconnect.Tx) error {
		_, err := computer.IssuePreparation(f.ctx, tx, f.ws, s.id, third)
		return err
	})
	outstanding := thirdStream.nextPrepare(both)

	s.unbind(team)
	left := []protocol.DriveBinding{rw(notes)}
	firstDrives := firstStream.drivesInstruction(f.bindingOf(first), left)
	secondDrives := secondStream.drivesInstruction(f.bindingOf(second), left)
	thirdStream.quiet(300 * time.Millisecond)
	if got := f.driveSet(f.bindingOf(first)); !reflect.DeepEqual(got, both) {
		t.Fatalf("drive set before the applied report = %v; want %v", got, both)
	}
	firstStream.post(firstStream.appliedReport(firstDrives, "applied_first"))
	if got := f.driveSet(f.bindingOf(first)); !reflect.DeepEqual(got, left) {
		t.Fatalf("drive set after the applied report = %v; want %v", got, left)
	}

	thirdStream.report(outstanding, "")
	thirdStream.drivesInstruction(f.bindingOf(third), left)
	thirdStream.quiet(300 * time.Millisecond)
	if sent := f.drivesSent(s.id); sent != 3 {
		t.Fatalf("%d session.drives sent; want one per binding", sent)
	}

	response, err := secondStream.cli.Post(f.ctx, "/v1/client/protocol-error", protocol.Encode(protocol.ProtocolErrorReport{ConnectionID: secondStream.connectionID,
		MessageID: "protocol_error_second", RequestMessageID: protocol.Some(secondDrives.Envelope.MessageID),
		Error: protocol.ProtocolError{Code: string(protocol.ProtocolInvalidMessage), Message: "unreadable"}}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := response.Acknowledgement(); err != nil {
		t.Fatalf("protocol-error report = %d %s, %v", response.Status, response.Body, err)
	}
	if got := f.driveSet(f.bindingOf(second)); !reflect.DeepEqual(got, both) ||
		f.count(`SELECT count(*) FROM connection_instructions WHERE message_id=$1 AND reported_at IS NULL`, secondDrives.Envelope.MessageID) != 1 {
		t.Fatalf("a protocol-error report changed the binding: drive set %v", got)
	}

	s.unbind(notes)
	none := []protocol.DriveBinding{}
	empty := firstStream.drivesInstruction(f.bindingOf(first), none)
	secondStream.drivesInstruction(f.bindingOf(second), none)
	thirdStream.drivesInstruction(f.bindingOf(third), none)
	if got := f.driveSet(f.bindingOf(first)); !reflect.DeepEqual(got, left) {
		t.Fatalf("drive set before the empty send's applied report = %v; want %v", got, left)
	}
	firstStream.post(firstStream.appliedReport(empty, "applied_empty"))
	if got := f.driveSet(f.bindingOf(first)); len(got) != 0 {
		t.Fatalf("drive set after the empty send's applied report = %v; want empty", got)
	}
}

// A machine offline while a drive is removed and another set read_only is
// prepared again with its last prepared drive set, also after a failed
// preparation; its prepared report sends exactly one session.drives with the
// Session's bindings, before the tool.execute of the call that waited for
// it, and the new binding keeps the old drives and access until its applied
// report, which only that binding may send. A failed preparation's drives
// never count as prepared. Of two prepared bindings on the instance, the
// latest one's drive set prepares the machine again.
func TestAnOfflineMachineIsPreparedWithItsLastDriveSet(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	s.idle()
	team, notes := f.createDrive(), f.createDrive()
	s.bind(team, "read_write")
	s.bind(notes, "read_write")
	old := sorted(rw(team), rw(notes))
	computerID, stream := s.preparedUserComputer("laptop")
	stream.expire()
	s.unbind(team)
	s.rebind(notes, "read_only")
	if sent := f.drivesSent(s.id); sent != 0 {
		t.Fatalf("%d session.drives sent to an offline machine; want none", sent)
	}

	next := stream.reopen()
	next.ready(nil)
	failed := next.nextPrepare(old)
	next.report(failed, "cwd_not_found")
	s.running()
	use, _ := s.admitted(computerID, "make")
	again := next.nextPrepare(old)
	next.report(again, "")
	bindingID := again.Envelope.Payload.(protocol.SessionPreparePayload).BindingID
	drives := next.drivesInstruction(bindingID, []protocol.DriveBinding{ro(notes)})
	if _, sent := next.execute(); sent.ToolUseEventID != use {
		t.Fatalf("tool.execute for %s; want the waiting call %s", sent.ToolUseEventID, use)
	}
	// The failed binding on the same connection may not answer it.
	wrong := next.appliedReport(drives, "applied_failed_binding")
	wrong.BindingID = failed.Envelope.Payload.(protocol.SessionPreparePayload).BindingID
	next.refusedReport(wrong)
	if got := f.driveSet(bindingID); !reflect.DeepEqual(got, old) {
		t.Fatalf("drive set before the applied report = %v; want the old %v", got, old)
	}
	next.post(next.appliedReport(drives, "applied_laptop"))
	if got := f.driveSet(bindingID); !reflect.DeepEqual(got, []protocol.DriveBinding{ro(notes)}) {
		t.Fatalf("drive set after the applied report = %v; want %s read_only", got, notes)
	}
	if sent := f.drivesSent(s.id); sent != 1 {
		t.Fatalf("%d session.drives sent; want exactly one", sent)
	}

	// A computer never prepared: its failed preparation's drives do not
	// count, so the next one carries the Session's bindings of that moment.
	freshCLI := f.userInstance("fresh")
	fresh := f.connect(freshCLI, everyTool)
	freshComputer := s.userComputer(freshCLI)
	s.admitted(freshComputer, "make")
	fresh.report(fresh.nextPrepare([]protocol.DriveBinding{ro(notes)}), "cwd_not_found")
	s.idle()
	s.rebind(notes, "read_write")
	next.drivesInstruction(bindingID, []protocol.DriveBinding{rw(notes)})
	s.running()
	s.admitted(freshComputer, "make")
	fresh.nextPrepare([]protocol.DriveBinding{rw(notes)})

	// The laptop's first binding still holds the old set, its latest only
	// notes, read_write by the unapplied send.
	next.expire()
	last := next.reopen()
	last.ready(nil)
	last.nextPrepare([]protocol.DriveBinding{rw(notes)})
}

// An applied report must name a session.drives on its connection; its
// mount failures and lost changes write one error each, and
// neither a replay, a second report of the instruction nor a ready that
// repeats an entry writes one again; a ready's entry naming another
// session.drives writes its own.
func TestAppliedReportsWriteTheirErrorsOnce(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	s.idle()
	team, notes := f.createDrive(), f.createDrive()
	s.bind(team, "read_write")
	s.bind(notes, "read_write")
	computerID, stream := s.preparedUserComputer("desk")
	bindingID := f.bindingOf(computerID)
	s.unbind(team)
	removal := stream.drivesInstruction(bindingID, []protocol.DriveBinding{rw(notes)})

	prepare := stream.appliedReport(removal, "applied_prepare")
	prepare.RequestMessageID = f.row(`SELECT prepare_message_id FROM computer_bindings WHERE binding_id=$1`, bindingID)["prepare_message_id"].(string)
	stream.refusedReport(prepare)

	report := stream.appliedReport(removal, "applied_desk")
	report.DriveMountFailures = protocol.Some([]protocol.DriveMountFailure{{DriveID: notes, Error: "the mount failed"}})
	report.LostChanges = protocol.Some([]protocol.LostChange{{RequestMessageID: removal.Envelope.MessageID, DriveID: team, Paths: []string{"plan.md"}, PathsTotal: 1}})
	stream.post(report)
	stream.post(report)
	second := report
	second.MessageID = "applied_desk_again"
	stream.post(second)
	errs := s.f.sessionErrors(s.id)
	if len(errs) != 2 || errs[0]["type"] != "drive_mount_error" || errs[0]["drive_id"] != notes || errs[0]["computer_id"] != computerID ||
		errs[1]["type"] != "drive_changes_lost_error" || errs[1]["drive_id"] != team || errs[1]["paths_total"] != float64(1) ||
		errs[1]["message"] != "1 changes to drive "+team+" could not be confirmed saved before the drive's binding changed on computer "+computerID+"." {
		t.Fatalf("session errors = %v; want one mount error and one lost-changes error", errs)
	}

	s.rebind(notes, "read_only")
	change := stream.drivesInstruction(bindingID, []protocol.DriveBinding{ro(notes)})
	stream.expire()
	next := stream.reopen()
	next.ready([]protocol.LostChange{
		{RequestMessageID: change.Envelope.MessageID, DriveID: notes, Paths: []string{"draft.md"}, PathsTotal: 1},
		{RequestMessageID: removal.Envelope.MessageID, DriveID: team, Paths: []string{"plan.md"}, PathsTotal: 1},
	})
	errs = s.f.sessionErrors(s.id)
	if len(errs) != 3 || errs[2]["type"] != "drive_changes_lost_error" || errs[2]["drive_id"] != notes {
		t.Fatalf("session errors after the ready = %v; want one more lost-changes error, for %s", errs, notes)
	}
}
