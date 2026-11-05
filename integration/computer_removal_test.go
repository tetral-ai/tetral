package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/integration/enginetest"
	"github.com/tetral-ai/tetral/internal/agent"
	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/drive"
	"github.com/tetral-ai/tetral/internal/eventstream"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxrelease "github.com/tetral-ai/tetral/internal/sandbox/release"
	"github.com/tetral-ai/tetral/internal/session"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	tetralcleanup "github.com/tetral-ai/tetral/services/cleanup"
)

// developer sends a developer API request as the fixture's API key, through
// the Session routes, the Session event list and the drive routes on the api
// workload role.
func (f *machineTools) developer(method, path, body string) (int, []byte) {
	f.t.Helper()
	if f.api == nil {
		client := dbconnect.NewClientForTesting(f.workloads.OpenWorkload(f.t, "api", nil))
		drives, err := drive.NewService(client, f.blobs, f.t.TempDir())
		if err != nil {
			f.t.Fatal(err)
		}
		budget, err := drive.NewUploadBudget(client, 1<<30, workload.NewRegistry("api"))
		if err != nil {
			f.t.Fatal(err)
		}
		store := session.NewPostgreSQLSessionStore(client, session.WithSessionDeleteSandboxRelease(
			func(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID string, now time.Time) error {
				_, _, err := sandboxrelease.EnsureTx(ctx, tx, string(ws), sessionID, sandboxrelease.SessionDelete, "", now)
				return err
			}))
		sessions := session.NewService(agent.NewService(agent.NewPostgreSQLAgentStore(client), nil), nil, nil, nil, store, nil)
		events := eventstream.NewListHandler(eventstream.NewPostgreSQLReader(client))
		f.api = httpapi.NewRouter(httpapi.NewSessionHandler(sessions), "", httpapi.WithAuthenticator(auth.AuthenticatorFunc(
			func(context.Context, string) (auth.Principal, error) { return f.principal, nil })), httpapi.WithSessionEventListHandler(events),
			httpapi.WithDriveHandler(httpapi.NewDriveHandler(drives, budget, nil, []byte("machine-tools-drive-page-key"))))
	}
	separator := "?"
	if strings.Contains(path, "?") {
		separator = "&"
	}
	request := httptest.NewRequest(method, path+separator+"beta=true", strings.NewReader(body)).WithContext(f.ctx)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Api-Key", "machine-tools-key")
	recorder := httptest.NewRecorder()
	f.api.ServeHTTP(recorder, request)
	return recorder.Code, recorder.Body.Bytes()
}

// sessionErrors returns the error of every session.error event of
// sessionID, in event order.
func (f *machineTools) sessionErrors(sessionID string) []map[string]any {
	f.t.Helper()
	rows, err := f.admin.QueryContext(f.ctx, `SELECT payload_json::jsonb->'error' FROM session_events
		WHERE session_id=$1 AND type='session.error' ORDER BY sequence`, sessionID)
	if err != nil {
		f.t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var errs []map[string]any
	for rows.Next() {
		var raw []byte
		var value map[string]any
		if err := rows.Scan(&raw); err != nil || json.Unmarshal(raw, &value) != nil {
			f.t.Fatalf("session error %s: %v", raw, err)
		}
		errs = append(errs, value)
	}
	if err := rows.Err(); err != nil {
		f.t.Fatal(err)
	}
	return errs
}

func (f *machineTools) computerRow(computerID string) map[string]any {
	f.t.Helper()
	return f.row(`SELECT association_state, execution_state, connection_id, binding_id FROM computers WHERE id=$1`, computerID)
}

// captureLogs installs telemetry whose records go to the returned buffer;
// it must run before newMachineTools, whose hubs take its logger.
func captureLogs(t *testing.T) *lockedBuffer {
	lines := &lockedBuffer{}
	t.Cleanup(workload.InstallTelemetry(workload.NewTelemetry(workload.TelemetryOptions{Service: "integration",
		Logger: workload.NewLogger(lines, "integration", "test", "unit")})))
	return lines
}

// idle ends s's turn, so the developer may change its computers.
func (s *toolSession) idle() {
	s.f.t.Helper()
	s.f.exec(`UPDATE session_runtime_status SET status='idle' WHERE workspace_id=$1 AND session_id=$2`, string(s.f.ws), s.id)
}

// remove sends DELETE of the computer and returns its status and object.
func (s *toolSession) remove(computerID string) (int, map[string]any) {
	s.f.t.Helper()
	status, body := s.f.developer(http.MethodDelete, "/v1/sessions/"+s.id+"/computers/"+computerID, "")
	var object map[string]any
	if len(body) > 0 && json.Unmarshal(body, &object) != nil {
		s.f.t.Fatalf("DELETE %s = %d %s", computerID, status, body)
	}
	return status, object
}

// requireRemoved checks that DELETE of the computer answers status and, for
// 202, the detached computer in execution state.
func (s *toolSession) requireRemoved(computerID string, status int, state string) {
	s.f.t.Helper()
	got, object := s.remove(computerID)
	if got != status {
		s.f.t.Fatalf("DELETE %s = %d %v; want %d", computerID, got, object, status)
	}
	if status == http.StatusAccepted && (object["id"] != computerID || object["association_state"] != "detached" || object["execution_state"] != state) {
		s.f.t.Fatalf("DELETE %s answered %v; want the detached computer %s", computerID, object, state)
	}
}

// requireRefused checks that DELETE of the computer answers 409
// invalid_request_error and leaves it attached.
func (s *toolSession) requireRefused(computerID string) {
	s.f.t.Helper()
	status, body := s.f.developer(http.MethodDelete, "/v1/sessions/"+s.id+"/computers/"+computerID, "")
	if status != http.StatusConflict || !strings.Contains(string(body), `"invalid_request_error"`) {
		s.f.t.Fatalf("DELETE %s = %d %s; want 409 invalid_request_error", computerID, status, body)
	}
	if row := s.f.computerRow(computerID); row["association_state"] != "attached" {
		s.f.t.Fatalf("refused removal changed the computer: %v", row)
	}
}

// sessionObject reads s's Session object.
func (s *toolSession) sessionObject() (string, []string) {
	s.f.t.Helper()
	status, body := s.f.developer(http.MethodGet, "/v1/sessions/"+s.id, "")
	return decodeSessionComputers(s.f.t, status, body)
}

func decodeSessionComputers(t testing.TB, status int, body []byte) (string, []string) {
	t.Helper()
	var object struct {
		EnvironmentID *string `json:"environment_id"`
		Computers     []struct {
			ID string `json:"id"`
		} `json:"computers"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &object) != nil || object.EnvironmentID == nil || object.Computers == nil {
		t.Fatalf("Session object = %d %s", status, body)
	}
	ids := []string{}
	for _, c := range object.Computers {
		ids = append(ids, c.ID)
	}
	return *object.EnvironmentID, ids
}

// closeInstruction reads the next event, which must be a session.close of
// sessionID.
func (st *cliStream) closeInstruction(sessionID string) (enginetest.Event, protocol.SessionClosePayload) {
	st.f.t.Helper()
	event := st.next()
	payload, ok := event.Envelope.Payload.(protocol.SessionClosePayload)
	if !ok || payload.SessionID != sessionID {
		st.f.t.Fatalf("event = %s; want session.close of %s", event.Data, sessionID)
	}
	return event, payload
}

// closed posts the closed report of close as messageID with lost and
// returns it.
func (st *cliStream) closed(close enginetest.Event, messageID string, lost []protocol.LostChange) protocol.SessionStatus {
	st.f.t.Helper()
	payload := close.Envelope.Payload.(protocol.SessionClosePayload)
	report := protocol.SessionStatus{ConnectionID: st.connectionID, MessageID: messageID, RequestMessageID: close.Envelope.MessageID,
		SessionID: payload.SessionID, BindingID: payload.BindingID, Status: "closed"}
	if lost != nil {
		report.LostChanges = protocol.Some(lost)
	}
	st.post(report)
	return report
}

// post sends report, which must be acknowledged.
func (st *cliStream) post(report protocol.SessionStatus) {
	st.f.t.Helper()
	response, err := st.cli.Post(st.f.ctx, "/v1/client/session-status", protocol.Encode(report))
	if err != nil {
		st.f.t.Fatal(err)
	}
	if ack, err := response.Acknowledgement(); err != nil || ack.MessageID != report.MessageID {
		st.f.t.Fatalf("session report = %d %s, %v", response.Status, response.Body, err)
	}
}

// expire lets the stream's lease pass and closes the stream, so that the
// instance's next stream ends the connection.
func (st *cliStream) expire() {
	st.f.t.Helper()
	st.f.exec(`UPDATE instance_connections SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, st.connectionID)
	st.close()
}

// reopen opens the stream's instance's stream again as a new connection.
func (st *cliStream) reopen() *cliStream {
	st.f.t.Helper()
	return st.f.openStream(st.cli, st.cli.EventsQuery("", nil))
}

// ready sends the stream's ready with lost, which must be granted.
func (st *cliStream) ready(lost []protocol.LostChange) {
	st.f.t.Helper()
	ready := protocol.Ready{ConnectionID: st.connectionID, MessageID: "ready_" + st.connectionID, OS: "linux", Arch: "x86_64",
		CLIVersion: "1.0.0", Capabilities: everyTool}
	if lost != nil {
		ready.LostChanges = protocol.Some(lost)
	}
	response, err := st.cli.Ready(st.f.ctx, ready)
	if err != nil {
		st.f.t.Fatal(err)
	}
	if _, err := response.Grant(); err != nil {
		st.f.t.Fatalf("ready: %v", err)
	}
}

// Removal answers 202 while the closure is closing or unknown and 204 once
// it is closed or when nothing was ever issued for the computer; the last
// cloud computer and a running Session refuse it, and a repeated DELETE
// issues no second session.close.
func TestRemovingAComputerAnswersFromItsClosure(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	prepared, stream := s.preparedUserComputer("laptop")
	offline, gone := s.preparedUserComputer("desktop")
	never := s.userComputer(f.userInstance("server"))
	cloud := s.cloudComputer()

	s.requireRefused(never)
	s.idle()
	s.requireRefused(cloud)

	s.requireRemoved(prepared, http.StatusAccepted, "closing")
	close, payload := stream.closeInstruction(s.id)
	if row := f.computerRow(prepared); payload.BindingID != row["binding_id"] {
		t.Fatalf("session.close names %s; want the computer's binding %v", payload.BindingID, row["binding_id"])
	}
	s.requireRemoved(prepared, http.StatusAccepted, "closing")
	stream.quiet(time.Second)
	stream.closed(close, "status_closed", nil)
	s.requireRemoved(prepared, http.StatusNoContent, "")
	if n := f.count(`SELECT count(*) FROM connection_instructions WHERE connection_id=$1 AND type='session.close'`, stream.connectionID); n != 1 {
		t.Fatalf("%d session.close instructions; want 1", n)
	}

	s.requireRemoved(never, http.StatusNoContent, "")
	if row := f.computerRow(never); row["association_state"] != "detached" || row["execution_state"] != "closed" {
		t.Fatalf("computer never bound = %v; want detached and closed", row)
	}

	gone.expire()
	s.requireRemoved(offline, http.StatusAccepted, "unknown")
	if n := f.count(`SELECT count(*) FROM connection_instructions WHERE connection_id=$1 AND type='session.close'`, gone.connectionID); n != 0 {
		t.Fatalf("%d session.close instructions on an expired connection; want none", n)
	}
	if n := f.count(`SELECT count(*) FROM session_machine_messages WHERE session_id=$1 AND change='removed'`, s.id); n != 3 {
		t.Fatalf("%d removed machine messages; want one per removal", n)
	}
}

// A close whose connection ends before its report makes the computer
// unknown; the instance's next accepted ready issues one late close with a
// new binding, and its closed report makes the computer closed.
func TestALostCloseIsCompletedByTheNextConnection(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	computerID, stream := s.preparedUserComputer("laptop")
	s.idle()
	s.requireRemoved(computerID, http.StatusAccepted, "closing")
	_, lost := stream.closeInstruction(s.id)

	stream.expire()
	next := stream.reopen()
	if row := f.computerRow(computerID); row["execution_state"] != "unknown" || row["binding_id"] != nil {
		t.Fatalf("computer after its connection ended = %v; want unknown with no binding", row)
	}
	if state := f.row(`SELECT state FROM computer_bindings WHERE binding_id=$1`, lost.BindingID)["state"]; state != "invalid" {
		t.Fatalf("lost close's binding = %v; want invalid", state)
	}
	s.requireRemoved(computerID, http.StatusAccepted, "unknown")

	next.ready(nil)
	late, payload := next.closeInstruction(s.id)
	if payload.BindingID == lost.BindingID {
		t.Fatal("the late close reuses the lost close's binding")
	}
	if binding := f.row(`SELECT kind, state FROM computer_bindings WHERE binding_id=$1`, payload.BindingID); binding["kind"] != "late_close" || binding["state"] != "closing" {
		t.Fatalf("late close binding = %v", binding)
	}
	next.quiet(time.Second)
	s.requireRemoved(computerID, http.StatusAccepted, "unknown")
	next.closed(late, "status_late_closed", nil)
	s.requireRemoved(computerID, http.StatusNoContent, "")
}

// Each lost_changes entry writes at most one drive_changes_lost_error per
// close and drive, from a closed report or a later ready. In a report, an
// entry naming another instance's close or another close than the report's
// writes nothing and is logged with its reason; a close lost with its
// connection has its entries written from the instance's next ready.
func TestLostChangesWriteOneErrorEach(t *testing.T) {
	logs := captureLogs(t)
	f := newMachineTools(t)
	s := f.session()
	computerID, stream := s.preparedUserComputer("laptop")
	neighbour := f.session()
	neighbourComputer := neighbour.userComputer(stream.cli)
	f.ownerTx(func(tx *dbconnect.Tx) error {
		_, err := computer.IssuePreparation(f.ctx, tx, f.ws, neighbour.id, neighbourComputer)
		return err
	})
	stream.prepare("")
	stranger := f.session()
	strangerComputer, strangerStream := stranger.preparedUserComputer("desktop")
	for _, removal := range []struct {
		s          *toolSession
		computerID string
	}{{neighbour, neighbourComputer}, {stranger, strangerComputer}, {s, computerID}} {
		removal.s.idle()
		removal.s.requireRemoved(removal.computerID, http.StatusAccepted, "closing")
	}
	neighbourClose, _ := stream.closeInstruction(neighbour.id)
	strangerClose, _ := strangerStream.closeInstruction(stranger.id)
	close, payload := stream.closeInstruction(s.id)

	paths := make([]string, 100)
	for i := range paths {
		paths[i] = fmt.Sprintf("notes/%03d.md", i)
	}
	written := []protocol.LostChange{
		{RequestMessageID: close.Envelope.MessageID, DriveID: "drv_a", Paths: paths, PathsTotal: 101},
		{RequestMessageID: close.Envelope.MessageID, DriveID: "drv_b", Paths: []string{"plan.md"}, PathsTotal: 1},
	}
	entries := append(append([]protocol.LostChange{}, written...),
		protocol.LostChange{RequestMessageID: strangerClose.Envelope.MessageID, DriveID: "drv_c", Paths: []string{"other.md"}, PathsTotal: 1},
		protocol.LostChange{RequestMessageID: neighbourClose.Envelope.MessageID, DriveID: "drv_d", Paths: []string{"shared.md"}, PathsTotal: 1})
	report := stream.closed(close, "status_closed", entries)
	stream.post(report)
	errs := f.sessionErrors(s.id)
	if len(errs) != len(written) {
		t.Fatalf("errors = %v; want one per drive", errs)
	}
	for i, entry := range written {
		got := errs[i]
		message := fmt.Sprintf("%d changes to drive %s could not be confirmed saved before computer %s was removed.", entry.PathsTotal, entry.DriveID, computerID)
		if got["type"] != "drive_changes_lost_error" || got["drive_id"] != entry.DriveID || got["computer_id"] != computerID || got["message"] != message ||
			got["paths_total"] != float64(entry.PathsTotal) || len(got["paths"].([]any)) != len(entry.Paths) || got["paths"].([]any)[0] != entry.Paths[0] ||
			got["retry_status"].(map[string]any)["type"] != "terminal" {
			t.Fatalf("error %d = %v; want entry %v", i, got, entry)
		}
	}
	for _, other := range []*toolSession{neighbour, stranger} {
		if errs := f.sessionErrors(other.id); len(errs) != 0 {
			t.Fatalf("errors of %s = %v; want none", other.id, errs)
		}
	}
	requireIgnored := func(want ...map[string]any) {
		t.Helper()
		got := diagnosticRecords(t, logs, "client.lost_changes_ignored")
		if len(got) != len(want) {
			t.Fatalf("ignored entries = %v; want %v", got, want)
		}
		for i, fields := range want {
			for key, value := range fields {
				if got[i][key] != value {
					t.Fatalf("ignored entry %d = %v; want %v", i, got[i], fields)
				}
			}
		}
	}
	fromReport := map[string]any{"connection.id": stream.connectionID, "message.id": "status_closed", "session.id": s.id, "binding.id": payload.BindingID}
	with := func(fields map[string]any, requestID, reason string) map[string]any {
		entry := map[string]any{"request.message.id": requestID, "reason": reason}
		for key, value := range fields {
			entry[key] = value
		}
		return entry
	}
	requireIgnored(with(fromReport, strangerClose.Envelope.MessageID, "unknown_instruction"),
		with(fromReport, neighbourClose.Envelope.MessageID, "request_message_id_mismatch"))

	stream.expire()
	next := stream.reopen()
	next.ready(entries)
	if errs := f.sessionErrors(s.id); len(errs) != len(written) {
		t.Fatalf("errors after the ready repeated the entries = %v; want only the report's", errs)
	}
	neighbourErrors := f.sessionErrors(neighbour.id)
	if len(neighbourErrors) != 1 || neighbourErrors[0]["drive_id"] != "drv_d" || neighbourErrors[0]["computer_id"] != neighbourComputer ||
		neighbourErrors[0]["message"] != "1 changes to drive drv_d could not be confirmed saved before computer "+neighbourComputer+" was removed." {
		t.Fatalf("errors of the lost close's Session = %v; want its entry's error", neighbourErrors)
	}
	if errs := f.sessionErrors(stranger.id); len(errs) != 0 {
		t.Fatalf("errors of the other instance's Session after the ready = %v; want none", errs)
	}
	next.closeInstruction(neighbour.id)
}

// A ready's entry naming another instance's close writes nothing, the ready
// is accepted, and the entry is logged with no Session or binding.
func TestAReadyLogsEntriesOfOtherInstances(t *testing.T) {
	logs := captureLogs(t)
	f := newMachineTools(t)
	stranger := f.session()
	strangerComputer, strangerStream := stranger.preparedUserComputer("desktop")
	stranger.idle()
	stranger.requireRemoved(strangerComputer, http.StatusAccepted, "closing")
	strangerClose, _ := strangerStream.closeInstruction(stranger.id)

	cli := f.userInstance("laptop")
	next := f.openStream(cli, cli.EventsQuery("", nil))
	next.ready([]protocol.LostChange{{RequestMessageID: strangerClose.Envelope.MessageID, DriveID: "drv_c", Paths: []string{"other.md"}, PathsTotal: 1}})
	if errs := f.sessionErrors(stranger.id); len(errs) != 0 {
		t.Fatalf("errors = %v; want none", errs)
	}
	ignored := diagnosticRecords(t, logs, "client.lost_changes_ignored")
	if len(ignored) != 1 || ignored[0]["reason"] != "unknown_instruction" || ignored[0]["request.message.id"] != strangerClose.Envelope.MessageID ||
		ignored[0]["connection.id"] != next.connectionID || ignored[0]["message.id"] != "ready_"+next.connectionID ||
		ignored[0]["session.id"] != "" || ignored[0]["binding.id"] != "" {
		t.Fatalf("ignored entries = %v; want the ready's entry", ignored)
	}
}

// A cloud computer removed while its new request is pending leaves the
// request to its own transitions: a claim of it gives an instance that is
// unused once its ready is accepted, with no preparation, and an unclaimed
// one expires after the pending expiry; no request is recorded for a
// detached computer.
func TestARemovedCloudComputerKeepsItsRequest(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	claimedComputer, unclaimedComputer := s.cloudComputer(), s.cloudComputer()
	s.cloudComputer()
	var claimedRequest, unclaimedRequest string
	f.ownerTx(func(tx *dbconnect.Tx) error {
		for _, c := range []struct {
			computerID string
			requestID  *string
		}{{claimedComputer, &claimedRequest}, {unclaimedComputer, &unclaimedRequest}} {
			request, _, err := computer.EnsureRequest(f.ctx, tx, f.ws, s.id, c.computerID)
			if err != nil {
				return err
			}
			*c.requestID = request.ID
		}
		return nil
	})
	s.idle()
	s.requireRemoved(claimedComputer, http.StatusNoContent, "")
	s.requireRemoved(unclaimedComputer, http.StatusNoContent, "")
	for _, requestID := range []string{claimedRequest, unclaimedRequest} {
		if state := f.row(`SELECT state FROM computer_requests WHERE id=$1`, requestID)["state"]; state != "pending" {
			t.Fatalf("request %s after removal = %v; want pending", requestID, state)
		}
	}
	f.ownerTx(func(tx *dbconnect.Tx) error {
		var invalid *computer.InvalidError
		if _, _, err := computer.EnsureRequest(f.ctx, tx, f.ws, s.id, unclaimedComputer); !errors.As(err, &invalid) {
			t.Fatalf("EnsureRequest of a detached computer = %v; want refused", err)
		}
		return nil
	})

	claim := f.claim(0, claimedRequest, "worker-removed")
	stream := f.connect(f.cli(0, claim.Bootstrap.InstanceID, claim.Bootstrap.CredentialID), everyTool)
	stream.quiet(time.Second)
	if state := f.row(`SELECT state FROM computer_requests WHERE id=$1`, claimedRequest)["state"]; state != "fulfilled" {
		t.Fatalf("claimed request after ready = %v; want fulfilled", state)
	}
	if unused := f.row(`SELECT unused FROM instances WHERE id=$1`, claim.Bootstrap.InstanceID)["unused"]; unused != true {
		t.Fatalf("instance of the removed computer unused = %v; want true", unused)
	}

	cfg, err := tetralcleanup.ConfigFromEnv(identityEnv{})
	if err != nil {
		t.Fatal(err)
	}
	phase := f.computerPhase(&lockedBuffer{}, nil)
	phase.ClaimTimeout, phase.PendingExpiry = cfg.RequestClaimTimeout, cfg.RequestPendingExpiry
	f.exec(`UPDATE computer_requests SET pending_since=clock_timestamp()-$2::interval WHERE id=$1`, unclaimedRequest, (cfg.RequestPendingExpiry + time.Second).String())
	if result := f.runPhase(phase); result.PendingExpired != 1 {
		t.Fatalf("phase = %+v; want the unclaimed request expired", result)
	}
	if state := f.row(`SELECT state FROM computer_requests WHERE id=$1`, unclaimedRequest)["state"]; state != "expired" {
		t.Fatalf("unclaimed request = %v; want expired", state)
	}
	if n := f.count(`SELECT count(*) FROM computer_requests WHERE computer_id IN ($1,$2)`, claimedComputer, unclaimedComputer); n != 2 {
		t.Fatalf("%d requests of the removed computers; want only the two recorded before removal", n)
	}
}

// Archiving an idle Session detaches its computers: a command that was
// running reports terminal after the close and ends with its result, waking
// nothing. Another Session on the same instance keeps its binding, and its
// command settles from its own report.
func TestArchivingClosesTheSessionsComputersOnly(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	computerID, stream := s.preparedUserComputer("laptop")
	_, task := s.admitted(computerID, "sleep 60")
	execute, _ := stream.execute()
	stream.acked(execution(stream.connectionID, "report_running", execute, "running", 1, execEnvelope("running", task, "")), "report_running")

	other := f.session()
	otherComputer := other.userComputer(stream.cli)
	f.ownerTx(func(tx *dbconnect.Tx) error {
		_, err := computer.IssuePreparation(f.ctx, tx, f.ws, other.id, otherComputer)
		return err
	})
	stream.prepare("")
	_, otherTask := other.admitted(otherComputer, "sleep 60")
	otherExecute, _ := stream.execute()
	stream.acked(execution(stream.connectionID, "report_other_running", otherExecute, "running", 1, execEnvelope("running", otherTask, "")), "report_other_running")

	s.idle()
	if status, body := f.developer(http.MethodPost, "/v1/sessions/"+s.id+"/archive", ""); status != http.StatusOK {
		t.Fatalf("archive = %d %s", status, body)
	}
	close, payload := stream.closeInstruction(s.id)
	if row := f.computerRow(computerID); row["association_state"] != "detached" || row["execution_state"] != "closing" || payload.BindingID != row["binding_id"] {
		t.Fatalf("computer after archive = %v; want detached and closing its binding", row)
	}
	if n := f.count(`SELECT count(*) FROM session_machine_messages WHERE session_id=$1`, s.id); n != 0 {
		t.Fatalf("archive wrote %d machine messages; want none", n)
	}
	stream.quiet(time.Second)

	cleanupAfter := f.row(`SELECT cleanup_after FROM session_runtime_status WHERE session_id=$1`, s.id)["cleanup_after"]
	events := f.count(`SELECT count(*) FROM session_events WHERE session_id=$1`, s.id)
	notifications := f.count(`SELECT count(*) FROM session_runtime_inbox WHERE session_id=$1 AND input_kind='task_notification'`, s.id)
	jobs := f.count(`SELECT count(*) FROM queue_jobs WHERE kind=$1 AND payload_json::jsonb->>'session_id'=$2`, queue.KindRuntimeInput, s.id)
	stream.acked(execution(stream.connectionID, "report_terminal", execute, "terminal", 2, execEnvelope("success", task, "done")), "report_terminal")
	ended := f.row(`SELECT status, terminal_result_json::jsonb->'result'->'stdout'->>'text' AS stdout FROM session_background_tasks WHERE task_id=$1`, task)
	if ended["status"] != "completed" || ended["stdout"] != "done" {
		t.Fatalf("task after archive = %v; want completed with its result", ended)
	}
	if n := f.count(`SELECT count(*) FROM session_runtime_inbox WHERE session_id=$1 AND input_kind='task_notification'`, s.id); n != notifications {
		t.Fatalf("the terminal report wrote %d task notifications; want none", n-notifications)
	}
	if n := f.count(`SELECT count(*) FROM queue_jobs WHERE kind=$1 AND payload_json::jsonb->>'session_id'=$2`, queue.KindRuntimeInput, s.id); n != jobs {
		t.Fatalf("the terminal report enqueued %d runtime input jobs; want none", n-jobs)
	}
	if n := f.count(`SELECT count(*) FROM session_events WHERE session_id=$1`, s.id); n != events {
		t.Fatalf("the terminal report wrote %d Session events; want none", n-events)
	}
	if after := f.row(`SELECT cleanup_after FROM session_runtime_status WHERE session_id=$1`, s.id)["cleanup_after"]; after != cleanupAfter {
		t.Fatalf("cleanup_after = %v; want %v", after, cleanupAfter)
	}
	stream.closed(close, "status_closed", nil)
	if row := f.computerRow(computerID); row["execution_state"] != "closed" {
		t.Fatalf("computer after its close report = %v", row)
	}

	if row := f.computerRow(otherComputer); row["association_state"] != "attached" || row["execution_state"] != "ready" {
		t.Fatalf("other Session's computer = %v; want attached and ready", row)
	}
	stream.acked(execution(stream.connectionID, "report_other_terminal", otherExecute, "terminal", 2, execEnvelope("success", otherTask, "kept")), "report_other_terminal")
	if status := f.row(`SELECT status FROM session_background_tasks WHERE task_id=$1`, otherTask)["status"]; status != "completed" {
		t.Fatalf("other Session's command = %v; want completed from its own report", status)
	}
}

// The Session object lists its attached computers: after its first cloud
// computer is removed, environment_id is the next one's and the removed
// computer stays readable by its route while closing; after archive the
// Session lists no computer and environment_id is "".
func TestTheSessionObjectFollowsItsComputers(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	first := s.cloudComputer()
	stream := f.connect(s.cloudInstance(first), everyTool)
	stream.prepare("")
	f.exec(`INSERT INTO environments (workspace_id, id, name, config_json, created_at, updated_at) VALUES ($1, $2, $2, '{}', now(), now())`, string(f.ws), "env_second_"+s.id)
	second := s.cloudComputer()
	f.exec(`UPDATE computers SET environment_id=$2 WHERE id=$1`, second, "env_second_"+s.id)
	if environmentID, computers := s.sessionObject(); environmentID != "env_"+s.id || strings.Join(computers, ",") != first+","+second {
		t.Fatalf("Session object = %s %v; want the first cloud computer's Environment and both computers", environmentID, computers)
	}

	s.idle()
	s.requireRemoved(first, http.StatusAccepted, "closing")
	if environmentID, computers := s.sessionObject(); environmentID != "env_second_"+s.id || strings.Join(computers, ",") != second {
		t.Fatalf("Session object after removal = %s %v; want the second cloud computer alone", environmentID, computers)
	}
	if status, body := f.developer(http.MethodGet, "/v1/sessions/"+s.id+"/computers/"+first, ""); status != http.StatusOK || !strings.Contains(string(body), `"detached"`) {
		t.Fatalf("GET of the closing computer = %d %s; want it detached", status, body)
	}

	status, body := f.developer(http.MethodPost, "/v1/sessions/"+s.id+"/archive", "")
	if environmentID, computers := decodeSessionComputers(t, status, body); environmentID != "" || len(computers) != 0 {
		t.Fatalf("archive answered %s %v; want no computer", environmentID, computers)
	}
	if environmentID, computers := s.sessionObject(); environmentID != "" || len(computers) != 0 {
		t.Fatalf("Session object after archive = %s %v; want no computer", environmentID, computers)
	}
}

// Deleting a Session closes each attached computer once, and each closed
// report makes its computer closed.
func TestDeletingASessionClosesEachComputer(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	laptop, laptopStream := s.preparedUserComputer("laptop")
	desktop, desktopStream := s.preparedUserComputer("desktop")
	s.idle()
	if status, body := f.developer(http.MethodDelete, "/v1/sessions/"+s.id, ""); status != http.StatusOK {
		t.Fatalf("delete = %d %s", status, body)
	}
	for _, c := range []struct {
		computerID string
		stream     *cliStream
	}{{laptop, laptopStream}, {desktop, desktopStream}} {
		close, _ := c.stream.closeInstruction(s.id)
		if row := f.computerRow(c.computerID); row["association_state"] != "detached" || row["execution_state"] != "closing" {
			t.Fatalf("computer after deletion = %v; want detached and closing", row)
		}
		c.stream.quiet(time.Second)
		c.stream.closed(close, "status_closed_"+c.computerID, nil)
		if row := f.computerRow(c.computerID); row["execution_state"] != "closed" {
			t.Fatalf("computer after its close report = %v; want closed", row)
		}
	}
}

// Admissions racing a removal of their computer: every tool.execute issued
// for it precedes its session.close on the connection, and every admission
// after the removal finds the computer detached.
func TestNoCallStartsOnARemovedComputer(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	computerID, stream := s.preparedUserComputer("laptop")
	const racing = 8
	uses := make([]string, racing+1)
	for i := range uses {
		uses[i] = s.use(protocol.ToolExecCommand, "machine_tool")
	}
	admit := func(use string) (*bridgev1.AdmitToolCallResponse, error) {
		return f.runtime.AdmitToolCall(f.ctx, &bridgev1.AdmitToolCallRequest{Scope: s.scope, ToolUseEventId: use, ComputerId: computerID,
			Tool: protocol.ToolExecCommand, InputJson: `{"cmd":"true"}`, SandboxPolicyJson: disabledNetworkPolicy})
	}
	// Admission reads no Session status; the Session reads idle so that the
	// developer may remove the computer while its calls are admitted.
	s.idle()
	var wg sync.WaitGroup
	results := make([]error, racing)
	for i := range racing {
		wg.Add(1)
		go func() {
			defer wg.Done()
			response, err := admit(uses[i])
			if err == nil && response.GetAdmitted() == nil && response.GetComputerNotAttached() == nil {
				err = fmt.Errorf("admission = %v; want admitted or computer not attached", response)
			}
			results[i] = err
		}()
	}
	s.requireRemoved(computerID, http.StatusAccepted, "closing")
	wg.Wait()
	for _, err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	if response, err := admit(uses[racing]); err != nil || response.GetComputerNotAttached() == nil {
		t.Fatalf("admission after removal = %v, %v; want computer not attached", response, err)
	}
	closeSeq := f.row(`SELECT issue_seq FROM connection_instructions WHERE connection_id=$1 AND type='session.close'`, stream.connectionID)["issue_seq"].(float64)
	if n := f.count(`SELECT count(*) FROM connection_instructions WHERE connection_id=$1 AND type='tool.execute' AND issue_seq>$2`, stream.connectionID, closeSeq); n != 0 {
		t.Fatalf("%d tool.execute instructions after the session.close; want none", n)
	}
}
