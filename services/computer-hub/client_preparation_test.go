package computerhub

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/integration/enginetest"
	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	"github.com/tetral-ai/tetral/internal/workload"
)

// testGitProxyURL is the git_proxy_url of the fixture's git tickets.
const testGitProxyURL = "https://git.tetral.test"

// userComputer seeds an idle Session and its attached user computer
// comp_<sessionID> on cli's instance, working in /Users/me/shop.
func (f *hubFixture) userComputer(cli *enginetest.CLI, sessionID string) {
	f.t.Helper()
	sessionfixture.SeedBridgeAPISession(f.t, f.admin, string(f.ws), sessionID, "sthr_"+sessionID)
	if _, err := f.admin.ExecContext(f.ctx, `INSERT INTO computers
		(workspace_id,session_id,id,kind,instance_id,current_instance_id,working_directory,networking,drive_exclude,association_state,execution_state)
		VALUES ($1,$2,$3,'user',$4,$4,'/Users/me/shop','{"type":"unrestricted"}','[]','attached','waiting')`,
		string(f.ws), sessionID, "comp_"+sessionID, cli.InstanceID); err != nil {
		f.t.Fatal(err)
	}
}

// repository gives the cloud computer comp_<sessionID> the repository url at
// /workspace/<name>, with no checkout and no identity.
func (f *hubFixture) repository(sessionID, name, url string) {
	f.t.Helper()
	resourceID := "res_" + sessionID + "_" + name
	if _, err := f.admin.ExecContext(f.ctx, `INSERT INTO session_resources (workspace_id,session_id,resource_id,type,created_at,updated_at)
		VALUES ($1,$2,$3,'github_repository',clock_timestamp(),clock_timestamp())`, string(f.ws), sessionID, resourceID); err != nil {
		f.t.Fatal(err)
	}
	if _, err := f.admin.ExecContext(f.ctx, `INSERT INTO session_github_repository_resources
		(workspace_id,session_id,resource_id,url,authorization_token_encrypted,computer_id,path) VALUES ($1,$2,$3,$4,'\x01',$5,$6)`,
		string(f.ws), sessionID, resourceID, url, "comp_"+sessionID, "/workspace/"+name); err != nil {
		f.t.Fatal(err)
	}
}

// prepareOn reads the stream's next event, which must be a session.prepare.
func prepareOn(t *testing.T, s *testStream) (enginetest.Event, protocol.SessionPreparePayload) {
	t.Helper()
	event := s.next()
	payload, ok := event.Envelope.Payload.(protocol.SessionPreparePayload)
	if !ok {
		t.Fatalf("event = %s; want session.prepare", event.Data)
	}
	return event, payload
}

// report posts the session-status report messageID answering prepare on
// cli's connection, with fill setting its status and fields.
func (f *hubFixture) report(cli *enginetest.CLI, prepare enginetest.Event, messageID string, fill func(*protocol.SessionStatus)) enginetest.Response {
	f.t.Helper()
	payload := prepare.Envelope.Payload.(protocol.SessionPreparePayload)
	status := protocol.SessionStatus{ConnectionID: prepare.Envelope.ConnectionID, MessageID: messageID, RequestMessageID: prepare.Envelope.MessageID,
		SessionID: payload.SessionID, BindingID: payload.BindingID}
	fill(&status)
	response, err := cli.Post(f.ctx, "/v1/client/session-status", protocol.Encode(status))
	if err != nil {
		f.t.Fatal(err)
	}
	return response
}

// acknowledged checks that response is the ACK of messageID.
func acknowledged(t *testing.T, response enginetest.Response, messageID string) {
	t.Helper()
	ack, err := response.Acknowledgement()
	if err != nil || response.Status != http.StatusOK || ack.MessageID != messageID {
		t.Fatalf("report %s = %d %s, %v; want its ACK", messageID, response.Status, response.Body, err)
	}
}

func prepared(cwd string) func(*protocol.SessionStatus) {
	return func(s *protocol.SessionStatus) { s.Status, s.Cwd = "prepared", protocol.Some(cwd) }
}

func failed(code string) func(*protocol.SessionStatus) {
	return func(s *protocol.SessionStatus) {
		s.Status, s.Error = "failed", protocol.Some(protocol.PreparationError{Code: code, Message: "the preparation failed: " + code})
	}
}

// sessionErrors returns the error objects of the Session's session.error
// events on its primary stream, in order.
func (f *hubFixture) sessionErrors(sessionID string) []map[string]any {
	f.t.Helper()
	rows, err := f.admin.QueryContext(f.ctx, `SELECT payload_json FROM session_events
		WHERE session_id=$1 AND session_thread_id IS NULL AND type='session.error' ORDER BY sequence`, sessionID)
	if err != nil {
		f.t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var errs []map[string]any
	for rows.Next() {
		var payload string
		var event struct {
			Error map[string]any `json:"error"`
		}
		if err := rows.Scan(&payload); err != nil || json.Unmarshal([]byte(payload), &event) != nil {
			f.t.Fatalf("session.error %s: %v", payload, err)
		}
		errs = append(errs, event.Error)
	}
	return errs
}

// row reads one row of query as a map through the admin pool.
func (f *hubFixture) row(query string, args ...any) map[string]any {
	f.t.Helper()
	var raw []byte
	if err := f.admin.QueryRowContext(f.ctx, `SELECT row_to_json(r)::text FROM (`+query+`) r`, args...).Scan(&raw); err != nil {
		f.t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		f.t.Fatal(err)
	}
	return value
}

func (f *hubFixture) bindingState(bindingID string) string {
	return f.row(`SELECT state FROM computer_bindings WHERE binding_id=$1`, bindingID)["state"].(string)
}

// CC-33, Engine side, and the late report: a failed preparation is prepared
// again with a new binding and no session.close; the same failure code again
// writes no second error; a prepared report makes the computer ready; a late
// report for an invalid or closing binding changes nothing.
func TestFailedPreparationIsPreparedAgain(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	cli := f.userInstance(time.Time{})
	f.userComputer(cli, "sesn_again")
	stream := f.open(cli)
	f.acceptReady(cli, stream, "ready_1")
	first, firstPayload := prepareOn(t, stream)
	if firstPayload.SessionID != "sesn_again" || firstPayload.Cwd != "/Users/me/shop" || firstPayload.CreateCwd.Set ||
		len(firstPayload.Repositories) != 0 || len(firstPayload.Drives) != 0 || len(firstPayload.DriveExclude) != 0 {
		t.Fatalf("user computer session.prepare = %s", first.Data)
	}
	acknowledged(t, f.report(cli, first, "status_1", failed("cwd_not_found")), "status_1")
	computerRow := f.row(`SELECT execution_state,error_code,binding_id FROM computers WHERE id='comp_sesn_again'`)
	if computerRow["execution_state"] != "failed" || computerRow["error_code"] != "cwd_not_found" || computerRow["binding_id"] != firstPayload.BindingID {
		t.Fatalf("computer after the failure = %v", computerRow)
	}
	for _, refused := range []func(*protocol.SessionStatus){
		func(s *protocol.SessionStatus) { s.Status = "closed" },                                                  // answers no session.prepare
		func(s *protocol.SessionStatus) { prepared("/Users/me/shop")(s); s.BindingID = "bind_0000000000000000" }, // another binding
	} {
		requireClientError(t, f.report(cli, first, "status_refused", refused), nil, protocol.ErrorUnknownInstruction)
	}
	if count := f.instanceRead(cli.InstanceID)["binding_count"]; count != float64(0) {
		t.Fatalf("binding_count of a failed preparation = %v; want 0", count)
	}
	errs := f.sessionErrors("sesn_again")
	if len(errs) != 1 || errs[0]["type"] != "computer_preparation_error" || errs[0]["computer_id"] != "comp_sesn_again" || errs[0]["code"] != "cwd_not_found" ||
		errs[0]["message"] != "the preparation failed: cwd_not_found" || errs[0]["retry_status"].(map[string]any)["type"] != "retrying" {
		t.Fatalf("Session errors = %v; want one computer_preparation_error", errs)
	}

	prepareAgain := func() {
		f.ownerTx(func(tx *dbconnect.Tx) error {
			issued, err := computer.PrepareAgain(f.ctx, tx, f.ws, "sesn_again", "comp_sesn_again")
			if err == nil && !issued {
				t.Fatal("PrepareAgain issued nothing")
			}
			return err
		})
	}
	prepareAgain()
	second, secondPayload := prepareOn(t, stream) // the next event: no session.close came first
	if secondPayload.BindingID == firstPayload.BindingID || f.bindingState(firstPayload.BindingID) != computer.BindingInvalid {
		t.Fatalf("preparation again = %s, first binding %s", second.Data, f.bindingState(firstPayload.BindingID))
	}
	acknowledged(t, f.report(cli, second, "status_2", failed("cwd_not_found")), "status_2")
	if errs := f.sessionErrors("sesn_again"); len(errs) != 1 {
		t.Fatalf("Session errors after the same failure again = %v; want still one", errs)
	}
	prepareAgain()
	third, thirdPayload := prepareOn(t, stream)
	acknowledged(t, f.report(cli, third, "status_3", prepared("/Users/me/shop")), "status_3")
	computerRow = f.row(`SELECT execution_state,cwd,error_code,last_failed_code,binding_id FROM computers WHERE id='comp_sesn_again'`)
	if computerRow["execution_state"] != "ready" || computerRow["cwd"] != "/Users/me/shop" || computerRow["error_code"] != nil ||
		computerRow["last_failed_code"] != nil || computerRow["binding_id"] != thirdPayload.BindingID {
		t.Fatalf("computer after the prepared report = %v", computerRow)
	}
	if count := f.instanceRead(cli.InstanceID)["binding_count"]; count != float64(1) {
		t.Fatalf("binding_count after the prepared report = %v; want 1", count)
	}

	events := f.count(`SELECT count(*) FROM session_events WHERE session_id='sesn_again'`)
	acknowledged(t, f.report(cli, first, "status_late_invalid", prepared("/Users/me/shop")), "status_late_invalid")
	if _, err := f.admin.ExecContext(f.ctx, `UPDATE computer_bindings SET state='closing' WHERE binding_id=$1`, thirdPayload.BindingID); err != nil {
		t.Fatal(err)
	}
	acknowledged(t, f.report(cli, third, "status_late_closing", prepared("/Users/me/elsewhere")), "status_late_closing")
	computerRow = f.row(`SELECT execution_state,cwd FROM computers WHERE id='comp_sesn_again'`)
	if f.bindingState(firstPayload.BindingID) != computer.BindingInvalid || f.bindingState(thirdPayload.BindingID) != computer.BindingClosing ||
		f.count(`SELECT count(*) FROM session_events WHERE session_id='sesn_again'`) != events ||
		computerRow["execution_state"] != "ready" || computerRow["cwd"] != "/Users/me/shop" {
		t.Fatalf("after late prepared reports for an invalid and a closing binding: computer %v; a binding, the computer or the Session's events changed", computerRow)
	}
}

// CC-36, Engine side: a repository_path_forbidden failure on each of two cloud
// computers writes one computer_preparation_error per computer. The cloud
// computer's session.prepare creates its cwd and names its repository with
// the platform identity.
func TestRepositoryPathFailuresWriteOneErrorPerComputer(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	for _, sessionID := range []string{"sesn_path_a", "sesn_path_b"} {
		cli := f.cloudComputer(sessionID, true)
		f.repository(sessionID, "app", "https://github.com/acme/app")
		stream := f.open(cli)
		f.acceptReady(cli, stream, "ready_1")
		prepare, payload := prepareOn(t, stream)
		if !payload.CreateCwd.Set || !payload.CreateCwd.Value || payload.Cwd != "/workspace" || len(payload.Repositories) != 1 ||
			payload.Repositories[0].URL != "https://github.com/acme/app" || payload.Repositories[0].Path != "/workspace/app" ||
			payload.Repositories[0].GitIdentity != (protocol.GitIdentity{Name: "Tetral Agent", Email: "session+" + sessionID + "@agents.tetral.ai"}) {
			t.Fatalf("cloud computer session.prepare = %s", prepare.Data)
		}
		acknowledged(t, f.report(cli, prepare, "status_1", failed("repository_path_forbidden")), "status_1")
	}
	for _, sessionID := range []string{"sesn_path_a", "sesn_path_b"} {
		if errs := f.sessionErrors(sessionID); len(errs) != 1 || errs[0]["code"] != "repository_path_forbidden" || errs[0]["computer_id"] != "comp_"+sessionID {
			t.Fatalf("%s errors = %v; want one repository_path_forbidden error of its computer", sessionID, errs)
		}
	}
}

// DE-5: a prepared report with two mount failures and one repository failure
// writes two drive_mount_error and one repository_authentication_error; its
// replay answers the same ACK and writes nothing.
func TestPreparedReportWritesEachFailureOnce(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	cli := f.cloudComputer("sesn_failures", true)
	f.repository("sesn_failures", "app", "https://github.com/acme/app")
	stream := f.open(cli)
	f.acceptReady(cli, stream, "ready_1")
	prepare, _ := prepareOn(t, stream)
	withFailures := func(s *protocol.SessionStatus) {
		prepared("/workspace")(s)
		s.DriveMountFailures = protocol.Some([]protocol.DriveMountFailure{{DriveID: "drive_a", Error: "mount a failed"}, {DriveID: "drive_b", Error: "mount b failed"}})
		s.RepositoryFailures = protocol.Some([]protocol.RepositoryFailure{{URL: "https://github.com/acme/app", Path: "/workspace/app", Cause: "authentication", Message: "bad token"}})
	}
	first := f.report(cli, prepare, "status_1", withFailures)
	acknowledged(t, first, "status_1")
	if replay := f.report(cli, prepare, "status_1", withFailures); replay.Status != http.StatusOK || string(replay.Body) != string(first.Body) {
		t.Fatalf("replay = %d %s; want the stored ACK", replay.Status, replay.Body)
	}
	errs := f.sessionErrors("sesn_failures")
	if len(errs) != 3 ||
		errs[0]["type"] != "drive_mount_error" || errs[0]["drive_id"] != "drive_a" || errs[0]["computer_id"] != "comp_sesn_failures" || errs[0]["message"] != "mount a failed" ||
		errs[1]["type"] != "drive_mount_error" || errs[1]["drive_id"] != "drive_b" ||
		errs[2]["type"] != "repository_authentication_error" || errs[2]["repository_url"] != "https://github.com/acme/app" || errs[2]["message"] != "bad token" {
		t.Fatalf("Session errors = %v; want two drive_mount_error and one repository_authentication_error", errs)
	}
}

// A user computer added while its instance's connection is online gets one
// session.prepare, from the transaction that adds it; after the CLI's
// prepared report through the session-status route, issuing again sends none.
func TestOnlineUserComputerIsPreparedOnce(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	cli := f.userInstance(time.Time{})
	stream := f.open(cli)
	f.acceptReady(cli, stream, "ready_1")
	f.userComputer(cli, "sesn_online")
	issue := func() bool {
		var issued bool
		f.ownerTx(func(tx *dbconnect.Tx) error {
			var err error
			issued, err = computer.IssuePreparation(f.ctx, tx, f.ws, "sesn_online", "comp_sesn_online")
			return err
		})
		return issued
	}
	if !issue() {
		t.Fatal("the addition's issuance sent no session.prepare")
	}
	prepare, payload := prepareOn(t, stream)
	acknowledged(t, f.report(cli, prepare, "status_1", prepared("/Users/me/shop")), "status_1")
	if issue() {
		t.Fatal("a prepared computer was prepared again")
	}
	computerRow := f.row(`SELECT execution_state,binding_id FROM computers WHERE id='comp_sesn_online'`)
	if prepares := f.count(`SELECT count(*) FROM connection_instructions WHERE type='session.prepare'`); prepares != 1 ||
		computerRow["execution_state"] != "ready" || computerRow["binding_id"] != payload.BindingID {
		t.Fatalf("after the prepared report: %d session.prepare instructions, computer %v; want 1 and ready on %s", prepares, computerRow, payload.BindingID)
	}
}

// CM-25, Engine side: a binding left preparing for 10 minutes of the test
// clock stays preparing and then takes a prepared report, or a failed one.
func TestPreparationWaitsWithoutATimeout(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	var clis []*enginetest.CLI
	var prepares []enginetest.Event
	for _, sessionID := range []string{"sesn_wait_ok", "sesn_wait_fail"} {
		cli := f.userInstance(time.Time{})
		f.userComputer(cli, sessionID)
		stream := f.open(cli)
		f.acceptReady(cli, stream, "ready_1")
		prepare, _ := prepareOn(t, stream)
		clis, prepares = append(clis, cli), append(prepares, prepare)
	}
	f.clock.Advance(10 * time.Minute)
	if waiting := f.count(`SELECT count(*) FROM computer_bindings WHERE state='preparing'`); waiting != 2 {
		t.Fatalf("preparing bindings after 10 minutes = %d; want 2", waiting)
	}
	acknowledged(t, f.report(clis[0], prepares[0], "status_1", prepared("/Users/me/shop")), "status_1")
	acknowledged(t, f.report(clis[1], prepares[1], "status_1", failed("cwd_inaccessible")), "status_1")
	if ok, bad := f.row(`SELECT execution_state FROM computers WHERE id='comp_sesn_wait_ok'`), f.row(`SELECT execution_state FROM computers WHERE id='comp_sesn_wait_fail'`); ok["execution_state"] != "ready" || bad["execution_state"] != "failed" {
		t.Fatalf("computers after the late reports = %v and %v; want ready and failed", ok, bad)
	}
}

// fetchTicket posts a git-ticket request for bindingID on connectionID.
func (f *hubFixture) fetchTicket(cli *enginetest.CLI, connectionID, messageID, bindingID string) enginetest.Response {
	f.t.Helper()
	response, err := cli.Post(f.ctx, "/v1/client/git-ticket", protocol.Encode(protocol.GitTicketRequest{ConnectionID: connectionID, MessageID: messageID, BindingID: bindingID}))
	if err != nil {
		f.t.Fatal(err)
	}
	return response
}

// CC-43, Engine side, and the tickets: each fetch issues a new ticket and
// revokes the previous one, the table holds only hashes, a retry with the same
// content is a new fetch; a binding of a user computer, of a cloud computer
// without repositories or of another connection answers 409
// unknown_instruction.
func TestGitTicketFetches(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	cli := f.cloudComputer("sesn_ticket", true)
	f.repository("sesn_ticket", "app", "https://github.com/acme/app")
	stream := f.open(cli)
	f.acceptReady(cli, stream, "ready_1")
	_, payload := prepareOn(t, stream)
	fetch := func(messageID string) string {
		response := f.fetchTicket(cli, stream.connectionID, messageID, payload.BindingID)
		answer, err := protocol.DecodeGitTicketResponse(response.Body)
		if response.Status != http.StatusOK || err != nil || response.Header.Get("Cache-Control") != "no-store" || answer.GitProxyURL != testGitProxyURL {
			t.Fatalf("fetch %s = %d %s %v, %v", messageID, response.Status, response.Body, response.Header, err)
		}
		return answer.Ticket
	}
	firstTicket, secondTicket := fetch("ticket_1"), fetch("ticket_2")
	retried := fetch("ticket_2")
	if firstTicket == secondTicket || secondTicket == retried {
		t.Fatal("a fetch returned an earlier ticket")
	}
	hash := sha256.Sum256([]byte(retried))
	if live := f.count(`SELECT count(*) FROM computer_git_tickets WHERE state='live' AND computer_id='comp_sesn_ticket' AND token_hash=$1`, hash[:]); live != 1 ||
		f.count(`SELECT count(*) FROM computer_git_tickets WHERE state='revoked' AND revoked_at IS NOT NULL`) != 2 {
		t.Fatal("tickets: want the last fetch's live and the two before it revoked")
	}
	for _, ticket := range []string{firstTicket, secondTicket, retried} {
		if f.count(`SELECT count(*) FROM computer_git_tickets t WHERE strpos(row_to_json(t)::text, $1) > 0`, ticket) != 0 {
			t.Fatal("computer_git_tickets holds a raw ticket")
		}
	}
	conflict := f.fetchTicket(cli, stream.connectionID, "ticket_2", "bind_0000000000000000")
	body, err := conflict.Error()
	if conflict.Status != http.StatusConflict || err != nil || body.Code != protocol.ErrorMessageConflict {
		t.Fatalf("reused message_id with other content = %d %s", conflict.Status, conflict.Body)
	}

	user := f.userInstance(time.Time{})
	f.userComputer(user, "sesn_ticket_user")
	userStream := f.open(user)
	f.acceptReady(user, userStream, "ready_1")
	_, userPayload := prepareOn(t, userStream)
	bare := f.cloudComputer("sesn_ticket_bare", true)
	bareStream := f.open(bare)
	f.acceptReady(bare, bareStream, "ready_1")
	_, barePayload := prepareOn(t, bareStream)
	for i, row := range []struct {
		cli       *enginetest.CLI
		stream    *testStream
		bindingID string
	}{
		{user, userStream, userPayload.BindingID}, // a user computer's preparation
		{bare, bareStream, barePayload.BindingID}, // a cloud computer without repositories
		{bare, bareStream, payload.BindingID},     // another connection's preparation
	} {
		response := f.fetchTicket(row.cli, row.stream.connectionID, fmt.Sprintf("ticket_refused_%d", i), row.bindingID)
		requireClientError(t, response, nil, protocol.ErrorUnknownInstruction)
	}
	if f.count(`SELECT count(*) FROM computer_git_tickets`) != 3 {
		t.Fatal("a refused fetch stored a ticket")
	}
}

// The session_computer cold start is built from the stored times alone: the
// request's claim on one hub server and the report on another give request_ms
// and instance_ms from the times the test stored, prepare_ms from the
// binding's stored times; the engine scope leaves out request and instance.
// A preparation again on the same connection is reloaded with prepare alone;
// a failed one on the instance's next connection records its code and
// observes nothing.
func TestSessionComputerColdStartUsesStoredTimes(t *testing.T) {
	lines := &lockedBuffer{}
	telemetry := workload.NewTelemetry(workload.TelemetryOptions{Service: ServiceName, Logger: workload.NewLogger(lines, ServiceName, "test", "unit")})
	t.Cleanup(workload.InstallTelemetry(telemetry))
	f := newHubFixture(t, 720*time.Hour)
	other := f.addPod("computer-hub-test-1")
	f.cloudComputer("sesn_cold", false)
	request := f.ensureRequest("sesn_cold")
	claim := f.claim(request.ID, "worker_1")
	cli := f.cli(claim.Bootstrap.InstanceID, claim.Bootstrap.CredentialID)
	stream := f.openClientStream(other.url, cli, cli.EventsQuery("", nil), false)
	f.acceptReady(cli, stream, "ready_1")
	prepare, payload := prepareOn(t, stream)
	// Stored times far from any process clock: a process clock in the record
	// would show.
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, statement := range []struct {
		query string
		args  []any
	}{
		{`UPDATE computer_requests SET created_at=$2,pending_since=$2,claimed_at=$3 WHERE id=$1`, []any{request.ID, base, base.Add(3 * time.Second)}},
		{`UPDATE instance_connections SET ready_accepted_at=$2 WHERE id=$1`, []any{stream.connectionID, base.Add(23 * time.Second)}},
	} {
		if _, err := f.admin.ExecContext(f.ctx, statement.query, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	at := *cli
	at.BaseURL = other.url
	acknowledged(t, f.report(&at, prepare, "status_1", prepared("/workspace")), "status_1")
	var created, reported time.Time
	if err := f.admin.QueryRowContext(f.ctx, `SELECT created_at,reported_at FROM computer_bindings WHERE binding_id=$1`, payload.BindingID).Scan(&created, &reported); err != nil {
		t.Fatal(err)
	}
	records := sessionComputerColdStarts(lines.String())
	if len(records) != 1 || records[0]["cold_start.start_kind"] != "new" || records[0]["outcome"] != "ok" || records[0]["request_ms"] != float64(3000) ||
		records[0]["instance_ms"] != float64(20000) || records[0]["prepare_ms"] != float64(reported.Sub(created).Milliseconds()) {
		t.Fatalf("cold start records = %v; want request 3000, instance 20000 and prepare %d ms", records, reported.Sub(created).Milliseconds())
	}
	samples := coldStartSamples(t, telemetry, "session_computer")
	if samples["scope total"] != 1 || samples["scope engine"] != 1 || samples["sum engine"] != reported.Sub(created).Seconds() ||
		samples["sum total"] != (23*time.Second+reported.Sub(created)).Seconds() {
		t.Fatalf("cold start samples = %v", samples)
	}

	reconnect := func(messageID string) (*testStream, enginetest.Event) {
		if _, err := f.admin.ExecContext(f.ctx, `UPDATE instance_connections SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE instance_id=$1 AND ended_at IS NULL`, cli.InstanceID); err != nil {
			t.Fatal(err)
		}
		next := f.open(cli)
		f.acceptReady(cli, next, messageID)
		event, _ := prepareOn(t, next)
		return next, event
	}
	// A preparation again on the first connection, as a call to a computer
	// whose binding failed gets.
	if _, err := f.admin.ExecContext(f.ctx, `UPDATE computer_bindings SET state='failed',error_code='cwd_not_found',error_message='gone' WHERE binding_id=$1`, payload.BindingID); err != nil {
		t.Fatal(err)
	}
	f.ownerTx(func(tx *dbconnect.Tx) error {
		_, err := computer.PrepareAgain(f.ctx, tx, f.ws, "sesn_cold", "comp_sesn_cold")
		return err
	})
	again, _ := prepareOn(t, stream)
	acknowledged(t, f.report(&at, again, "status_2", prepared("/workspace")), "status_2")
	_, failing := reconnect("ready_2")
	acknowledged(t, f.report(cli, failing, "status_3", failed("cwd_not_found")), "status_3")
	records = sessionComputerColdStarts(lines.String())
	if len(records) != 3 || records[1]["cold_start.start_kind"] != "reloaded" || records[1]["outcome"] != "ok" || records[1]["request_ms"] != nil ||
		records[1]["instance_ms"] != nil || records[1]["prepare_ms"] == nil || records[2]["outcome"] != "cwd_not_found" {
		t.Fatalf("cold start records = %v; want a reloaded one with prepare alone, then one ended by cwd_not_found", records)
	}
	if samples := coldStartSamples(t, telemetry, "session_computer"); samples["scope total"] != 2 || samples["phase prepare"] != 2 || samples["phase request"] != 1 {
		t.Fatalf("cold start samples = %v; want the failed one to observe nothing", samples)
	}
}

func sessionComputerColdStarts(logs string) []map[string]any {
	var records []map[string]any
	for _, record := range coldStartRecordsOf(logs) {
		if record["cold_start.kind"] == "session_computer" {
			records = append(records, record)
		}
	}
	return records
}

// CM-28, the event: a computer whose session.prepare is exactly 65,536 bytes
// gets an event the test client's reader accepts, no longer than that.
func TestPrepareAtTheSizeLimitFitsOneEvent(t *testing.T) {
	f := newHubFixture(t, 720*time.Hour)
	cli := f.cloudComputer("sesn_limit", true)
	exclude := []computer.DriveExclude{{DriveID: "drive_limit", Paths: nil}}
	for i := 0; computer.PrepareEnvelopeSize("sesn_limit", computer.KindCloud, "/workspace", exclude, nil, nil) < computer.PrepareEnvelopeLimit-600; i++ {
		exclude[0].Paths = append(exclude[0].Paths, longDrivePath(i, 500))
	}
	missing := computer.PrepareEnvelopeLimit - computer.PrepareEnvelopeSize("sesn_limit", computer.KindCloud, "/workspace", exclude, nil, nil)
	exclude[0].Paths = append(exclude[0].Paths, longDrivePath(len(exclude[0].Paths), missing-3)) // `,"…"` adds three bytes
	if size := computer.PrepareEnvelopeSize("sesn_limit", computer.KindCloud, "/workspace", exclude, nil, nil); size != computer.PrepareEnvelopeLimit {
		t.Fatalf("prepared size = %d; want exactly the limit", size)
	}
	if _, err := f.admin.ExecContext(f.ctx, `UPDATE computers SET drive_exclude=$2 WHERE id=$1`, "comp_sesn_limit", string(mustJSON(t, exclude))); err != nil {
		t.Fatal(err)
	}
	stream := f.open(cli)
	f.acceptReady(cli, stream, "ready_1")
	event, payload := prepareOn(t, stream)
	if len(event.Data) > computer.PrepareEnvelopeLimit || len(payload.DriveExclude) != 1 || len(payload.DriveExclude[0].Paths) != len(exclude[0].Paths) {
		t.Fatalf("session.prepare event of %d bytes with %d exclusions", len(event.Data), len(payload.DriveExclude))
	}
}

// longDrivePath is the i-th distinct drive path of n bytes, in segments of
// at most 250 bytes.
func longDrivePath(i, n int) string {
	path := []byte(fmt.Sprintf("p%05d", i) + strings.Repeat("a", n-6))
	for at := 250; at < n-1; at += 251 {
		path[at] = '/'
	}
	return string(path)
}
