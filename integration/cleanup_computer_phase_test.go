package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/workload"
	tetralcleanup "github.com/tetral-ai/tetral/services/cleanup"
)

// computerPhase is Cleanup's computer phase as the installed Cleanup role,
// with a claim timeout of 2 minutes and a pending expiry of 3 minutes read
// from its environment, logging to logs; tracer, when set, sees its queries.
func (f *machineTools) computerPhase(logs *lockedBuffer, tracer pgx.QueryTracer) tetralcleanup.ComputerPhase {
	f.t.Helper()
	cfg, err := tetralcleanup.ConfigFromEnv(identityEnv{tetralcleanup.EnvRequestClaimTimeout: "2m", tetralcleanup.EnvRequestPendingExpiry: "3m"})
	if err != nil {
		f.t.Fatal(err)
	}
	return tetralcleanup.ComputerPhase{Client: dbconnect.NewClientForTesting(f.workloads.OpenWorkload(f.t, "cleanup", tracer)),
		ClaimTimeout: cfg.RequestClaimTimeout, PendingExpiry: cfg.RequestPendingExpiry, Logger: workload.NewLogger(logs, "cleanup", "test", "unit")}
}

func (f *machineTools) runPhase(phase tetralcleanup.ComputerPhase) tetralcleanup.ComputerResult {
	f.t.Helper()
	result, err := phase.Run(f.ctx)
	if err != nil {
		f.t.Fatalf("computer phase: %v", err)
	}
	return result
}

// connectionState is the state of the instance's connection as the instance
// read serves it.
func (f *machineTools) connectionState(instanceID string) string {
	f.t.Helper()
	status, data := f.controller(http.MethodGet, "/v1/instances/"+instanceID, "")
	var object struct {
		Connection *struct {
			State string `json:"state"`
		} `json:"connection"`
	}
	if status != http.StatusOK || json.Unmarshal(data, &object) != nil || object.Connection == nil {
		f.t.Fatalf("instance read = %d %s", status, data)
	}
	return object.Connection.State
}

// rowVersions fingerprints every row the end of a connection writes, so a
// run that writes nothing leaves it unchanged.
func (f *machineTools) rowVersions() string {
	f.t.Helper()
	return f.row(`SELECT concat_ws('|',
		(SELECT string_agg(xmin::text, ',' ORDER BY id) FROM instance_connections),
		(SELECT string_agg(xmin::text, ',' ORDER BY binding_id) FROM computer_bindings),
		(SELECT string_agg(xmin::text, ',' ORDER BY id) FROM computers),
		(SELECT string_agg(xmin::text, ',' ORDER BY message_id) FROM session_tool_dispatches),
		(SELECT string_agg(xmin::text, ',' ORDER BY tool_use_event_id) FROM session_runtime_tool_results),
		(SELECT string_agg(xmin::text, ',' ORDER BY task_id) FROM session_background_tasks),
		(SELECT count(*)::text FROM session_runtime_inbox),
		(SELECT count(*)::text FROM queue_jobs),
		(SELECT string_agg(xmin::text, ',' ORDER BY id) FROM computer_requests)) AS versions`)["versions"].(string)
}

// A connection whose lease passed, with an issued and unreported tool.execute
// and a call waiting for a withheld preparation report, ends in one run: its
// instance reads offline, its bindings are invalid and their computers
// waiting, the first call settles "computer offline, result unknown" and the
// second ends not executed with "computer offline". A second run changes
// nothing.
func TestTheComputerPhaseEndsAnExpiredConnectionAndItsCalls(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	computerID, stream := s.preparedUserComputer("laptop")
	issued, _ := s.admitted(computerID, "make test")
	stream.execute()
	waiting := f.session()
	waitingComputer := waiting.userComputer(stream.cli)
	held, _ := waiting.admitted(waitingComputer, "make")
	if _, ok := stream.next().Envelope.Payload.(protocol.SessionPreparePayload); !ok {
		t.Fatal("no session.prepare for the second computer")
	}

	f.exec(`UPDATE instance_connections SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, stream.connectionID)
	phase := f.computerPhase(&lockedBuffer{}, nil)
	if result := f.runPhase(phase); result.ConnectionsEnded != 1 {
		t.Fatalf("run = %+v; want one connection ended", result)
	}
	if state := f.connectionState(stream.cli.InstanceID); state != instance.ConnectionOffline {
		t.Fatalf("instance connection %s; want offline", state)
	}
	if cause := f.row(`SELECT end_cause FROM instance_connections WHERE id=$1`, stream.connectionID)["end_cause"]; cause != instance.EndLeaseExpired {
		t.Fatalf("end cause %v; want lease_expired", cause)
	}
	if active := f.count(`SELECT count(*) FROM computer_bindings WHERE connection_id=$1 AND state<>'invalid'`, stream.connectionID); active != 0 {
		t.Fatalf("%d bindings of the ended connection are not invalid", active)
	}
	if n := f.count(`SELECT count(*) FROM computers WHERE id IN ($1,$2) AND execution_state='waiting' AND connection_id IS NULL AND binding_id IS NULL`,
		computerID, waitingComputer); n != 2 {
		t.Fatalf("%d computers waiting with no connection; want both", n)
	}
	s.requireUnknownOutcome(issued, runtimecontrol.MessageComputerOfflineResultUnknown)
	waiting.requireNotExecuted(held, runtimecontrol.CodeComputerOffline, "computer offline")
	for _, records := range [][]string{s.records(issued), waiting.records(held)} {
		if len(records) != 1 || records[0] != "tool.execute:ended" {
			t.Fatalf("records = %v; want the record ended", records)
		}
	}

	before := f.rowVersions()
	if result := f.runPhase(phase); result != (tetralcleanup.ComputerResult{}) {
		t.Fatalf("second run = %+v; want nothing", result)
	}
	if after := f.rowVersions(); after != before {
		t.Fatalf("the second run wrote rows: %s -> %s", before, after)
	}
}

// A command whose call settled on its running report is running when its
// connection's lease passes: one run ends it unknown_outcome with one
// task_notification and one runtime_input job for the calling thread, its
// terminal report afterwards gets 409 stale_connection and changes nothing,
// and a second run writes nothing. The notification of a command whose
// calling thread is closed is born parked, with no job.
func TestTheComputerPhaseEndsARunningCommandOnce(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	computerID, stream := s.preparedUserComputer("laptop")
	use, task := s.admitted(computerID, "sleep 600")
	instruction, _ := stream.execute()
	stream.acked(execution(stream.connectionID, "report_running", instruction, "running", 1, execEnvelope("running", task, "")), "report_running")
	closed := f.session()
	_, closedTask := closed.admitted(closed.userComputer(stream.cli), "sleep 600")
	stream.prepare("")
	closedInstruction, _ := stream.execute()
	stream.acked(execution(stream.connectionID, "report_closed", closedInstruction, "running", 1, execEnvelope("running", closedTask, "")), "report_closed")
	f.exec(`UPDATE session_threads SET status='closed_for_runtime' WHERE id=$1`, closed.thread)

	f.exec(`UPDATE instance_connections SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, stream.connectionID)
	phase := f.computerPhase(&lockedBuffer{}, nil)
	if result := f.runPhase(phase); result.ConnectionsEnded != 1 {
		t.Fatalf("run = %+v; want one connection ended", result)
	}
	ended := f.row(`SELECT status, terminal_result_json::jsonb->>'settlement' AS settlement, terminal_result_json::jsonb->>'message' AS message
		FROM session_background_tasks WHERE task_id=$1`, task)
	if ended["status"] != "unknown_outcome" || ended["settlement"] != "unknown_outcome" || ended["message"] != runtimecontrol.MessageComputerOfflineResultUnknown {
		t.Fatalf("background task = %v; want unknown_outcome with the computer offline record", ended)
	}
	if state, record := s.settlement(use); state != "terminal_unconsumed" || record["settlement"] != "report" {
		t.Fatalf("call = %s %v; want still settled by its running report", state, record)
	}
	inputID := queue.FormatTaskNotificationRuntimeInputID(task)
	notified := func(when string) {
		t.Helper()
		if n := f.count(`SELECT count(*) FROM session_runtime_inbox WHERE runtime_input_id=$1 AND input_kind='task_notification' AND status='queued' AND session_thread_id=$2`, inputID, s.thread); n != 1 {
			t.Fatalf("%s: %d task notifications; want one queued for the calling thread", when, n)
		}
		if n := f.count(`SELECT count(*) FROM queue_jobs WHERE kind=$1 AND payload_json::jsonb->>'runtime_input_id'=$2`, queue.KindRuntimeInput, inputID); n != 1 {
			t.Fatalf("%s: %d runtime input jobs; want 1", when, n)
		}
	}
	notified("after the run")
	closedInput := queue.FormatTaskNotificationRuntimeInputID(closedTask)
	if parked := f.row(`SELECT status, binding_id IS NOT NULL AS bound FROM session_runtime_inbox WHERE runtime_input_id=$1`, closedInput); parked["status"] != "parked" || parked["bound"] != true {
		t.Fatalf("closed thread's notification = %v; want parked on its binding", parked)
	}
	if n := f.count(`SELECT count(*) FROM queue_jobs WHERE payload_json::jsonb->>'runtime_input_id'=$1`, closedInput); n != 0 {
		t.Fatalf("%d jobs for the parked notification; want none", n)
	}

	before := f.rowVersions()
	stream.refused(execution(stream.connectionID, "report_terminal", instruction, "terminal", 2, execEnvelope("success", task, "done")),
		http.StatusConflict, protocol.ErrorStaleConnection)
	if result := f.runPhase(phase); result != (tetralcleanup.ComputerResult{}) {
		t.Fatalf("second run = %+v; want nothing", result)
	}
	if after := f.rowVersions(); after != before {
		t.Fatalf("the late report or the second run wrote rows: %s -> %s", before, after)
	}
	notified("after the late report and the second run")
	if n := f.count(`SELECT count(*) FROM session_tool_reports WHERE task_id=$1`, task); n != 1 {
		t.Fatalf("%d stored reports; want the running report only", n)
	}
}

// renewAfterDiscovery renews a connection's lease once the connection
// discovery has returned, as a heartbeat accepted between discovery and
// handling does.
type renewAfterDiscovery struct {
	f            *machineTools
	connectionID string
	once         sync.Once
}

type connectionDiscovery struct{}

func (r *renewAfterDiscovery) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, "tetral_cleanup_expired_connections") {
		return context.WithValue(ctx, connectionDiscovery{}, true)
	}
	return ctx
}

func (r *renewAfterDiscovery) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	if ctx.Value(connectionDiscovery{}) == true && data.Err == nil {
		r.once.Do(func() {
			r.f.exec(`UPDATE instance_connections SET lease_expires_at=clock_timestamp()+interval '45 seconds' WHERE id=$1`, r.connectionID)
		})
	}
}

// A connection whose ready never arrived reads registering, past its lease
// as before it, until a run ends it, and then reads offline; a connection
// whose lease was renewed after discovery found it is left online.
func TestTheComputerPhaseEndsAConnectionNeverReadyAndKeepsARenewedOne(t *testing.T) {
	f := newMachineTools(t)
	silent := f.userInstance("silent")
	f.openStream(silent, silent.EventsQuery("", nil))
	renewed := f.userInstance("renewed")
	live := f.connect(renewed, everyTool)
	f.exec(`UPDATE instance_connections SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE ended_at IS NULL`)
	if state := f.connectionState(silent.InstanceID); state != instance.ConnectionRegistering {
		t.Fatalf("silent instance connection %s before the run; want registering", state)
	}

	tracer := &renewAfterDiscovery{f: f, connectionID: live.connectionID}
	if result := f.runPhase(f.computerPhase(&lockedBuffer{}, tracer)); result.ConnectionsEnded != 1 {
		t.Fatalf("run = %+v; want only the silent connection ended", result)
	}
	if state := f.connectionState(silent.InstanceID); state != instance.ConnectionOffline {
		t.Fatalf("silent instance connection %s after the run; want offline", state)
	}
	if state := f.connectionState(renewed.InstanceID); state != instance.ConnectionOnline {
		t.Fatalf("renewed instance connection %s; want online", state)
	}
}

// A new request's claim 2 minutes old with no CLI returns to pending in one
// run: its instance becomes unused and its credential is revoked with cause
// new_claim_returned, audited with Engine's Cleanup as the actor; a claim
// 1 minute 59 seconds old is untouched.
func TestTheComputerPhaseReturnsTimedOutClaims(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	oldComputer, youngComputer := s.cloudComputer(), s.cloudComputer()
	s.admitted(oldComputer, "one")
	s.admitted(youngComputer, "one")
	oldRequest, youngRequest := s.openRequest(oldComputer), s.openRequest(youngComputer)
	old, young := f.claim(0, oldRequest, "worker_1"), f.claim(0, youngRequest, "worker_2")
	f.exec(`UPDATE computer_requests SET claimed_at=clock_timestamp()-interval '2 minutes' WHERE id=$1`, oldRequest)
	f.exec(`UPDATE computer_requests SET claimed_at=clock_timestamp()-interval '1 minute 59 seconds' WHERE id=$1`, youngRequest)

	logs := &lockedBuffer{}
	if result := f.runPhase(f.computerPhase(logs, nil)); result != (tetralcleanup.ComputerResult{ClaimsExpired: 1}) {
		t.Fatalf("run = %+v; want one claim returned", result)
	}
	requireReturned(t, f, oldRequest, old)
	if state := f.row(`SELECT state FROM computer_requests WHERE id=$1`, youngRequest)["state"]; state != "claimed" {
		t.Fatalf("young request %v; want still claimed", state)
	}
	if credential := f.row(`SELECT state FROM instance_credentials WHERE id=$1`, young.Bootstrap.CredentialID); credential["state"] != "active" {
		t.Fatalf("young claim credential %v; want active", credential)
	}
	audits := diagnosticRecords(t, logs, instance.AuditEvent)
	if len(audits) != 1 || audits[0]["credential.id"] != old.Bootstrap.CredentialID || audits[0]["operation"] != instance.AuditRevoke ||
		audits[0]["actor.credential.kind"] != "engine" || audits[0]["actor.credential.id"] != "cleanup" {
		t.Fatalf("audit records = %v; want one revocation of the returned claim by Cleanup", audits)
	}
}

// A claim whose CLI was refused at ready returns to pending with its
// instance unused after the run that follows its claim timeout.
func TestTheComputerPhaseReturnsAClaimRefusedAtReady(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	computerID := s.cloudComputer()
	s.admitted(computerID, "one")
	requestID := s.openRequest(computerID)
	claim := f.claim(0, requestID, "worker_1")
	cli := f.cli(0, claim.Bootstrap.InstanceID, claim.Bootstrap.CredentialID)
	stream := f.openStream(cli, cli.EventsQuery("", nil))
	response, err := cli.Ready(f.ctx, protocol.Ready{ConnectionID: stream.connectionID, MessageID: "ready_old", OS: "linux", Arch: "x86_64",
		CLIVersion: "0.0.0-rc.1", Capabilities: everyTool})
	if err != nil {
		t.Fatal(err)
	}
	if body, err := response.Error(); err != nil || body.Code != protocol.ErrorCLIUpgradeRequired {
		t.Fatalf("ready = %d %s; want cli_upgrade_required", response.Status, response.Body)
	}

	f.exec(`UPDATE computer_requests SET claimed_at=clock_timestamp()-interval '2 minutes' WHERE id=$1`, requestID)
	if result := f.runPhase(f.computerPhase(&lockedBuffer{}, nil)); result != (tetralcleanup.ComputerResult{ClaimsExpired: 1}) {
		t.Fatalf("run = %+v; want the refused claim returned", result)
	}
	requireReturned(t, f, requestID, claim)
}

// requireReturned checks that requestID is pending again, the claim's
// instance unused and its credential revoked with cause new_claim_returned.
func requireReturned(t *testing.T, f *machineTools, requestID string, claim claimed) {
	t.Helper()
	if state := f.row(`SELECT state FROM computer_requests WHERE id=$1`, requestID)["state"]; state != "pending" {
		t.Fatalf("request %v; want pending", state)
	}
	if unused := f.row(`SELECT unused FROM instances WHERE id=$1`, claim.Bootstrap.InstanceID)["unused"]; unused != true {
		t.Fatalf("claim instance unused = %v; want true", unused)
	}
	credential := f.row(`SELECT state, revocation_cause FROM instance_credentials WHERE id=$1`, claim.Bootstrap.CredentialID)
	if credential["state"] != "revoked" || credential["revocation_cause"] != instance.CauseNewClaimReturned {
		t.Fatalf("claim credential %v; want revoked with new_claim_returned", credential)
	}
}

// A new request pending for 3 minutes expires in one run: it is counted once
// as pending_expired in the run's one controller_requests.expired record and
// in cleanup_controller_requests_expired_total, the call waiting on it ends
// not executed with "No machine could be provided for the computer.", and
// the next call records a new request. One pending for 2 minutes 59 seconds
// is untouched.
func TestTheComputerPhaseExpiresOverduePendingRequests(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	oldComputer, youngComputer := s.cloudComputer(), s.cloudComputer()
	waiting, _ := s.admitted(oldComputer, "one")
	s.admitted(youngComputer, "one")
	oldRequest, youngRequest := s.openRequest(oldComputer), s.openRequest(youngComputer)
	f.exec(`UPDATE computer_requests SET pending_since=clock_timestamp()-interval '3 minutes' WHERE id=$1`, oldRequest)
	f.exec(`UPDATE computer_requests SET pending_since=clock_timestamp()-interval '2 minutes 59 seconds' WHERE id=$1`, youngRequest)

	logs := &lockedBuffer{}
	result := f.runPhase(f.computerPhase(logs, nil))
	if result != (tetralcleanup.ComputerResult{PendingExpired: 1}) {
		t.Fatalf("run = %+v; want one request expired", result)
	}
	if state := f.row(`SELECT state FROM computer_requests WHERE id=$1`, oldRequest)["state"]; state != "expired" {
		t.Fatalf("overdue request %v; want expired", state)
	}
	s.requireNotExecuted(waiting, runtimecontrol.CodeMachineNotProvided, "No machine could be provided for the computer.")
	if state := f.row(`SELECT state FROM computer_requests WHERE id=$1`, youngRequest)["state"]; state != "pending" {
		t.Fatalf("young request %v; want still pending", state)
	}
	summaries := diagnosticRecords(t, logs, "controller_requests.expired")
	if len(summaries) != 1 || summaries[0]["controller_requests.pending_expired"] != float64(1) || summaries[0]["controller_requests.claim_expired"] != float64(0) {
		t.Fatalf("summary records = %v; want one with one pending expiry", summaries)
	}
	metrics := tetralcleanup.NewSchedulerMetrics()
	metrics.ObserveComputerPhase(result)
	samples, err := metrics.Collector()(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	counted := map[string]float64{}
	for _, sample := range samples {
		if sample.Name == "cleanup_controller_requests_expired_total" {
			counted[sample.Labels[0].Value] = sample.Value
		}
	}
	if counted[tetralcleanup.RequestCausePendingExpired] != 1 || counted[tetralcleanup.RequestCauseClaimExpired] != 0 {
		t.Fatalf("expiry counters = %v; want one pending expiry", counted)
	}

	s.admitted(oldComputer, "two")
	if next := s.openRequest(oldComputer); next == oldRequest || f.row(`SELECT kind FROM computer_requests WHERE id=$1`, next)["kind"] != "new" {
		t.Fatalf("next request %s; want a new request", next)
	}
}

// diagnosticRecords returns the JSON records of event in logs.
func diagnosticRecords(t *testing.T, logs *lockedBuffer, event string) []map[string]any {
	t.Helper()
	var records []map[string]any
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("log line %s: %v", line, err)
		}
		if record["event"] == event || record["msg"] == event {
			records = append(records, record)
		}
	}
	return records
}
