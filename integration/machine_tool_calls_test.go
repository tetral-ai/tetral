package integration

import (
	"bytes"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/integration/enginetest"
	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	"github.com/tetral-ai/tetral/internal/workload"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
)

// A call admitted to a prepared computer reaches its CLI as one tool.execute
// that carries no computer_id, permission fields or justification and the
// admitting turn's trace; concurrent duplicate admissions record one call,
// and a resume resends the same message byte for byte.
func TestAdmittedCallReachesThePreparedComputerOnce(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	computerID, stream := s.preparedUserComputer("laptop")
	use := s.use(protocol.ToolExecCommand, "machine_tool")
	request := &bridgev1.AdmitToolCallRequest{Scope: s.scope, ToolUseEventId: use, ComputerId: computerID, Tool: protocol.ToolExecCommand,
		InputJson: `{"cmd":"make test","workdir":"/Users/me/shop","yield_time_ms":1000,"max_output_tokens":9007199254740993}`, SandboxPolicyJson: disabledNetworkPolicy,
		Limits: &bridgev1.ToolVisibleLimits{VisibleBytes: ptr(int64(4096))}}
	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	turn := metadata.AppendToOutgoingContext(f.ctx, "traceparent", "00-"+traceID+"-00f067aa0ba902b7-01")
	var wg sync.WaitGroup
	answers := make([]*bridgev1.AdmitToolCallResponse, 4)
	errs := make([]error, 4)
	for i := range answers {
		wg.Go(func() { answers[i], errs[i] = f.runtime.AdmitToolCall(turn, request) })
	}
	wg.Wait()
	admitted, duplicates := 0, 0
	for i, answer := range answers {
		switch {
		case errs[i] != nil:
			t.Fatalf("admission %d: %v", i, errs[i])
		case answer.GetAdmitted() != nil:
			admitted++
		case answer.GetDuplicate() != nil:
			duplicates++
		}
	}
	if admitted != 1 || duplicates != 3 {
		t.Fatalf("concurrent admissions: %d admitted, %d duplicate; want 1 and 3", admitted, duplicates)
	}
	if rows := f.count(`SELECT count(*) FROM session_runtime_tool_results WHERE tool_use_event_id=$1`, use); rows != 1 {
		t.Fatalf("%d tool result rows; want 1", rows)
	}
	if records := s.records(use); len(records) != 1 || records[0] != "tool.execute:ready" {
		t.Fatalf("records = %v; want one ready tool.execute", records)
	}

	event, payload := stream.execute()
	var sent map[string]any
	if err := json.Unmarshal(event.Data, &sent); err != nil {
		t.Fatal(err)
	}
	sentPayload := sent["payload"].(map[string]any)
	for _, key := range []string{"computer_id", "sandbox_permissions", "justification"} {
		if _, found := sentPayload[key]; found {
			t.Fatalf("tool.execute carries %s: %s", key, event.Data)
		}
		if _, found := sentPayload["input"].(map[string]any)[key]; found {
			t.Fatalf("tool.execute input carries %s: %s", key, event.Data)
		}
	}
	if payload.SessionID != s.id || payload.ToolUseEventID != use || payload.TaskID == "" || payload.Input.Tool() != protocol.ToolExecCommand ||
		!bytes.Contains(event.Data, []byte(`"max_output_tokens":9007199254740993`)) || !payload.Limits.Set || payload.Limits.Value.VisibleBytes.Value != 4096 {
		t.Fatalf("tool.execute = %s", event.Data)
	}
	if !event.Envelope.Traceparent.Set || !strings.Contains(event.Envelope.Traceparent.Value, traceID) {
		t.Fatalf("tool.execute traceparent %v; want the turn's trace %s", event.Envelope.Traceparent, traceID)
	}
	if trace := f.row(`SELECT trace_context FROM session_tool_dispatches WHERE message_id=$1`, event.Envelope.MessageID)["trace_context"]; !strings.Contains(trace.(string), traceID) {
		t.Fatalf("dispatch trace_context %v; want the turn's trace", trace)
	}

	if saved := f.savedEnvelope(event.Envelope.MessageID); !bytes.Equal(saved, event.Data) {
		t.Fatalf("sent tool.execute %s; want the saved envelope %s", event.Data, saved)
	}
	stream.close()
	resumed := f.resume(1, stream.cli, stream.connectionID)
	again, _ := resumed.execute()
	if again.Envelope.MessageID != event.Envelope.MessageID || !bytes.Equal(again.Data, event.Data) {
		t.Fatalf("tool.execute resent by the other hub %s; want the first one byte for byte %s", again.Data, event.Data)
	}
	if executes := f.count(`SELECT count(*) FROM connection_instructions WHERE type='tool.execute'`); executes != 1 {
		t.Fatalf("%d tool.execute instructions; want 1", executes)
	}
}

// A credential renewal on a live connection changes nothing of its work: the
// computer bound before it stays ready on its binding, and a command running
// across it ends with the terminal report sent under the new credential.
func TestARenewalKeepsTheConnectionsBindingsAndCalls(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	computerID, stream := s.preparedUserComputer("laptop")
	binding := f.computerRow(computerID)["binding_id"]
	use, task := s.admitted(computerID, "sleep 60")
	execute, _ := stream.execute()

	status, body, err := f.controllerKeyed(0, http.MethodPost, "/v1/instances/"+stream.cli.InstanceID+"/credentials", `{"delivery":"stream"}`, "renew-live")
	var issued struct {
		ID string `json:"id"`
	}
	if err != nil || status != http.StatusAccepted || json.Unmarshal(body, &issued) != nil || issued.ID == "" {
		t.Fatalf("a credential for the stream = %d %s, %v", status, body, err)
	}
	if renew, ok := stream.next().Envelope.Payload.(protocol.CredentialRenewPayload); !ok || renew.CredentialID != issued.ID {
		t.Fatalf("event = %+v; want credential.renew of %s", renew, issued.ID)
	}
	fetch, err := stream.cli.Post(f.ctx, "/v1/client/credential", protocol.Encode(protocol.CredentialRequest{ConnectionID: stream.connectionID, MessageID: "fetch_1", CredentialID: issued.ID}))
	if err != nil || fetch.Status != http.StatusOK {
		t.Fatalf("fetch = %d %s, %v", fetch.Status, fetch.Body, err)
	}
	renewed := f.resume(0, f.cli(0, stream.cli.InstanceID, issued.ID), stream.connectionID)
	if old := f.row(`SELECT state FROM instance_credentials WHERE id=$1`, stream.cli.CredentialID)["state"]; old != "revoked" {
		t.Fatalf("the earlier credential = %v after the new one's first use; want revoked", old)
	}

	report := execution(renewed.connectionID, "report_terminal", execute, "terminal", 1, execEnvelope("success", task, "done"))
	renewed.held(enginetest.CaseEnded, execute, report)
	renewed.acked(report, "report_terminal")
	if row := f.computerRow(computerID); row["execution_state"] != "ready" || row["binding_id"] != binding {
		t.Fatalf("computer after the renewal = %v; want ready on binding %v", row, binding)
	}
	if state, record := s.settlement(use); state != "terminal_unconsumed" || record["settlement"] != "report" || record["phase"] != "terminal" {
		t.Fatalf("command across the renewal = %s %v; want settled by its terminal report", state, record)
	}
}

// A second admission of a tool use that differs from the first in its input,
// sandbox policy, limits, computer or tool is refused and leaves the first
// call's row and record unchanged; an input that still carries a removed
// field is refused and records nothing; a computer the Session does not have
// attached answers computer_not_attached with no row.
func TestAdmissionRefusalsRecordNothing(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	computerID, stream := s.preparedUserComputer("laptop")
	// exec_command and view_image both accept this input, so the tool row
	// below differs from the first admission in its tool alone.
	const input = `{"cmd":"ls","path":"/Users/me/shop/shot.png"}`
	use, response, err := s.admit(computerID, protocol.ToolExecCommand, input, disabledNetworkPolicy)
	if err != nil || response.GetAdmitted() == nil {
		t.Fatalf("first admission = %v, %v", response, err)
	}
	sent, _ := stream.execute()
	before := f.row(`SELECT r.input_json, r.task_id, d.payload_json, d.state FROM session_runtime_tool_results r
		JOIN session_tool_dispatches d ON d.session_id=r.session_id AND d.tool_use_event_id=r.tool_use_event_id
		WHERE r.tool_use_event_id=$1`, use)
	first := func() *bridgev1.AdmitToolCallRequest {
		return &bridgev1.AdmitToolCallRequest{Scope: s.scope, ToolUseEventId: use, ComputerId: computerID,
			Tool: protocol.ToolExecCommand, InputJson: input, SandboxPolicyJson: disabledNetworkPolicy}
	}
	otherComputer := s.cloudComputer()
	for _, conflict := range []struct {
		name   string
		change func(*bridgev1.AdmitToolCallRequest)
	}{
		{"input", func(r *bridgev1.AdmitToolCallRequest) { r.InputJson = `{"cmd":"rm -rf /"}` }},
		{"sandbox policy", func(r *bridgev1.AdmitToolCallRequest) {
			r.SandboxPolicyJson = `{"schema_version":1,"filesystem":{"rules":[]},"network":{"mode":"direct"}}`
		}},
		{"limits", func(r *bridgev1.AdmitToolCallRequest) {
			r.Limits = &bridgev1.ToolVisibleLimits{VisibleBytes: ptr(int64(10))}
		}},
		{"computer", func(r *bridgev1.AdmitToolCallRequest) { r.ComputerId = otherComputer }},
		{"tool", func(r *bridgev1.AdmitToolCallRequest) { r.Tool = protocol.ToolViewImage }},
	} {
		request := first()
		conflict.change(request)
		if _, err := f.runtime.AdmitToolCall(f.ctx, request); status.Code(err) != codes.AlreadyExists {
			t.Fatalf("admission that changes the %s: %v; want AlreadyExists", conflict.name, err)
		}
	}
	after := f.row(`SELECT r.input_json, r.task_id, d.payload_json, d.state FROM session_runtime_tool_results r
		JOIN session_tool_dispatches d ON d.session_id=r.session_id AND d.tool_use_event_id=r.tool_use_event_id
		WHERE r.tool_use_event_id=$1`, use)
	if before["payload_json"] != after["payload_json"] || before["input_json"] != after["input_json"] || before["task_id"] != after["task_id"] || after["state"] != "ready" {
		t.Fatalf("after the conflict %v; want %v", after, before)
	}
	stream.quiet(300 * time.Millisecond)
	if executes := f.count(`SELECT count(*) FROM connection_instructions WHERE type='tool.execute'`); executes != 1 || sent.Envelope.MessageID == "" {
		t.Fatalf("%d tool.execute instructions; want the first call's alone", executes)
	}

	for _, input := range []string{`{"cmd":"ls","computer_id":"comp_other"}`, `{"cmd":"ls","sandbox_permissions":"require_escalated"}`, `{"cmd":"ls","justification":"why"}`} {
		use, _, err := s.admit(computerID, protocol.ToolExecCommand, input, disabledNetworkPolicy)
		if status.Code(err) != codes.InvalidArgument || f.count(`SELECT count(*) FROM session_runtime_tool_results WHERE tool_use_event_id=$1`, use) != 0 {
			t.Fatalf("admission of %s: %v; want InvalidArgument and no row", input, err)
		}
	}
	for _, policy := range []string{
		`{"schema_version":1,"filesystem":{"rules":[]},"network":{"mode":"open"}}`,
		``,
		`{"schema_version":1,"filesystem":{"rules":[]},"network":{"mode":"disabled"},"extra":true}`,
		`{"schema_version":2,"filesystem":{"rules":[]},"network":{"mode":"disabled"}}`,
	} {
		use, _, err = s.admit(computerID, protocol.ToolExecCommand, `{"cmd":"ls"}`, policy)
		if status.Code(err) != codes.InvalidArgument || f.count(`SELECT count(*) FROM session_runtime_tool_results WHERE tool_use_event_id=$1`, use) != 0 {
			t.Fatalf("admission with the policy %q: %v; want InvalidArgument and no row", policy, err)
		}
	}

	other := f.session()
	detached := other.cloudComputer()
	f.exec(`UPDATE computers SET association_state='detached',detached_at=clock_timestamp() WHERE id=$1`, detached)
	for _, target := range []string{"comp_missing", detached} {
		use, response, err := other.admit(target, protocol.ToolExecCommand, `{"cmd":"ls"}`, disabledNetworkPolicy)
		if err != nil || response.GetComputerNotAttached() == nil || f.count(`SELECT count(*) FROM session_runtime_tool_results WHERE tool_use_event_id=$1`, use) != 0 {
			t.Fatalf("admission to %s = %v, %v; want computer_not_attached and no row", target, response, err)
		}
	}
	if requests := f.count(`SELECT count(*) FROM computer_requests WHERE session_id=$1`, other.id); requests != 0 {
		t.Fatalf("%d requests for a detached computer; want none", requests)
	}
}

// A call to a user computer whose instance has no online connection is
// settled at once, not executed, with no record or request.
func TestOfflineUserComputerEndsTheCallAtOnce(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	computerID := s.userComputer(f.userInstance("offline"))
	use, _ := s.admitted(computerID, "ls")
	s.requireNotExecuted(use, runtimecontrol.CodeComputerOffline, "computer offline")
	if records := s.records(use); len(records) != 0 {
		t.Fatalf("records = %v; want none", records)
	}
	if requests := f.count(`SELECT count(*) FROM computer_requests WHERE session_id=$1`, s.id); requests != 0 {
		t.Fatalf("%d requests for a user computer; want none", requests)
	}
}

// A call to an online computer with no binding on its connection issues the
// preparation and waits with no time limit; the prepared report releases it
// in the transaction that stores the report, under the trace of the turn that
// admitted it, which the report's request does not carry. A failed report
// ends a waiting call with the report's error, and the next call prepares the
// computer again on a new binding, with no close between, and waits; a second
// failure ends that call with its own error. A cleanup_pending failure ends
// the call the same way, and another Session's call on the same instance then
// prepares and runs.
func TestCallWaitsForPreparationAndRunsOnThePreparedReport(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	cli := f.userInstance("laptop")
	stream := f.connect(cli, everyTool)
	computerID := s.userComputer(cli)
	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	turn := metadata.AppendToOutgoingContext(f.ctx, "traceparent", "00-"+traceID+"-00f067aa0ba902b7-01")
	use := s.use(protocol.ToolExecCommand, "machine_tool")
	if response, err := f.runtime.AdmitToolCall(turn, &bridgev1.AdmitToolCallRequest{Scope: s.scope, ToolUseEventId: use, ComputerId: computerID,
		Tool: protocol.ToolExecCommand, InputJson: `{"cmd":"make"}`, SandboxPolicyJson: disabledNetworkPolicy}); err != nil || response.GetAdmitted() == nil {
		t.Fatalf("admission = %v, %v; want admitted", response, err)
	}
	prepare := stream.next()
	payload, ok := prepare.Envelope.Payload.(protocol.SessionPreparePayload)
	if !ok {
		t.Fatalf("event = %s; want session.prepare", prepare.Data)
	}
	if records := s.records(use); len(records) != 1 || records[0] != "tool.execute:waiting_preparation" {
		t.Fatalf("records = %v; want one waiting for the preparation", records)
	}
	// Ten minutes withheld: no stored time moves the wait on.
	f.exec(`UPDATE session_tool_dispatches SET created_at=created_at-interval '10 minutes', updated_at=updated_at-interval '10 minutes' WHERE tool_use_event_id=$1`, use)
	f.exec(`UPDATE computer_bindings SET created_at=created_at-interval '10 minutes' WHERE binding_id=$1`, payload.BindingID)
	stream.quiet(500 * time.Millisecond)
	if state, _ := s.settlement(use); state != "pending" || s.records(use)[0] != "tool.execute:waiting_preparation" {
		t.Fatalf("after 10 minutes the call is %s %v; want it still waiting", state, s.records(use))
	}
	stream.report(prepare, "")
	event, sent := stream.execute()
	if sent.ToolUseEventID != use || sent.BindingID != payload.BindingID {
		t.Fatalf("tool.execute = %s; want the call on binding %s", event.Data, payload.BindingID)
	}
	if !event.Envelope.Traceparent.Set || !strings.Contains(event.Envelope.Traceparent.Value, traceID) {
		t.Fatalf("released tool.execute traceparent %v; want the admitting turn's trace %s", event.Envelope.Traceparent, traceID)
	}
	// The instruction's own row changes again when the hub records its first
	// write; the call's dispatch row, made ready beside the instruction and
	// under the same message id, keeps the release's transaction.
	if same := f.row(`SELECT (SELECT xmin::text FROM session_tool_dispatches WHERE message_id=$1 AND state='ready') = (SELECT xmin::text FROM computer_bindings WHERE binding_id=$2) AS same`,
		event.Envelope.MessageID, payload.BindingID)["same"]; same != true {
		t.Fatal("the tool.execute was not issued in the transaction that stored the prepared report")
	}

	failing := f.session()
	failingCLI := f.userInstance("failing")
	failingStream := f.connect(failingCLI, everyTool)
	failingComputer := failing.userComputer(failingCLI)
	first, _ := failing.admitted(failingComputer, "make")
	failedBinding := failingStream.prepare("cwd_not_found").BindingID
	failing.requireNotExecuted(first, "cwd_not_found", "the preparation failed: cwd_not_found")
	if records := failing.records(first); len(records) != 1 || records[0] != "tool.execute:ended" {
		t.Fatalf("records = %v; want the waiting record ended", records)
	}
	second, _ := failing.admitted(failingComputer, "make")
	secondBinding := failingStream.prepare("cwd_inaccessible").BindingID
	if secondBinding == failedBinding {
		t.Fatalf("the call after a failed preparation was prepared on the failed binding %s", failedBinding)
	}
	failing.requireNotExecuted(second, "cwd_inaccessible", "the preparation failed: cwd_inaccessible")
	again, _ := failing.admitted(failingComputer, "make")
	again2 := failingStream.next()
	if payload, ok := again2.Envelope.Payload.(protocol.SessionPreparePayload); !ok || payload.BindingID == failedBinding || payload.BindingID == secondBinding {
		t.Fatalf("event = %s; want a session.prepare on a new binding", again2.Data)
	}
	if records := failing.records(again); len(records) != 1 || records[0] != "tool.execute:waiting_preparation" {
		t.Fatalf("records = %v; want the call waiting for the new preparation", records)
	}
	failingStream.report(again2, "")
	if _, sent := failingStream.execute(); sent.ToolUseEventID != again {
		t.Fatalf("tool.execute for %s; want %s", sent.ToolUseEventID, again)
	}

	shared := f.userInstance("shared")
	sharedStream := f.connect(shared, everyTool)
	cleaning := f.session()
	held, _ := cleaning.admitted(cleaning.userComputer(shared), "make")
	sharedStream.prepare("cleanup_pending")
	cleaning.requireNotExecuted(held, "cleanup_pending", "the preparation failed: cleanup_pending")
	next := f.session()
	runs, _ := next.admitted(next.userComputer(shared), "make")
	sharedStream.prepare("")
	if _, sent := sharedStream.execute(); sent.ToolUseEventID != runs {
		t.Fatalf("tool.execute for %s; want the other Session's call %s", sent.ToolUseEventID, runs)
	}
	sharedStream.quiet(300 * time.Millisecond)
}

// Two calls to a cloud computer whose machine is not running share one
// request: a new request when the computer has no instance, a wake request
// of its current instance when that instance is offline. A Controller reads
// the request in its Environment's list and stream, created and then
// claimed. The claim's
// instance, once ready and prepared, is the computer's current one, bound
// once in the computer's working directory, and receives both calls. A call
// waiting on a request a Controller fails ends not executed, the Session
// gets no error, and the computer's next call records another new request.
func TestCloudCallsShareOneRequest(t *testing.T) {
	f := newMachineTools(t)
	for _, kind := range []string{"new", "wake"} {
		s := f.session()
		computerID := s.cloudComputer()
		var current any
		if kind == "wake" {
			current = s.cloudInstance(computerID).InstanceID
		}
		requests := f.controllerEvents("/v1/instance_requests/stream", "?environment_id=env_"+s.id)
		first, _ := s.admitted(computerID, "one")
		second, _ := s.admitted(computerID, "two")
		requestID := s.openRequest(computerID)
		if requests := f.count(`SELECT count(*) FROM computer_requests WHERE session_id=$1`, s.id); requests != 1 {
			t.Fatalf("%s: %d requests; want one shared", kind, requests)
		}
		if name, id := requests.next(); name != computer.RequestEventCreated || id != requestID {
			t.Fatalf("%s: request event %s of %s; want created of %s", kind, name, id, requestID)
		}
		var listed struct {
			Data []struct {
				ID    string `json:"id"`
				State string `json:"state"`
			} `json:"data"`
		}
		if status, body := f.controller(http.MethodGet, "/v1/instance_requests?environment_id=env_"+s.id, ""); status != http.StatusOK ||
			json.Unmarshal(body, &listed) != nil || len(listed.Data) != 1 || listed.Data[0].ID != requestID || listed.Data[0].State != "pending" {
			t.Fatalf("%s: request list = %d %s; want the pending request %s alone", kind, status, body, requestID)
		}
		if request := f.row(`SELECT kind, state, instance_id FROM computer_requests WHERE id=$1`, requestID); request["kind"] != kind || request["state"] != "pending" || request["instance_id"] != current {
			t.Fatalf("request %v; want a pending %s request of instance %v", request, kind, current)
		}
		for _, use := range []string{first, second} {
			if waiting := f.count(`SELECT count(*) FROM session_tool_dispatches WHERE tool_use_event_id=$1 AND state='waiting_instance' AND request_id=$2`, use, requestID); waiting != 1 {
				t.Fatalf("%s: call %s is not waiting on request %s", kind, use, requestID)
			}
		}
		claim := f.claim(0, requestID, "worker_"+kind)
		if name, id := requests.next(); name != computer.RequestEventClaimed || id != requestID {
			t.Fatalf("%s: request event %s of %s; want claimed of %s", kind, name, id, requestID)
		}
		if current != nil && claim.Bootstrap.InstanceID != current {
			t.Fatalf("wake claim names instance %s; want the computer's current instance %v", claim.Bootstrap.InstanceID, current)
		}
		stream := f.connect(f.cli(0, claim.Bootstrap.InstanceID, claim.Bootstrap.CredentialID), everyTool)
		for _, use := range []string{first, second} {
			if records := s.records(use); len(records) != 1 || records[0] != "tool.execute:waiting_preparation" {
				t.Fatalf("%s: after ready, records of %s = %v; want waiting for the preparation", kind, use, records)
			}
		}
		prepared := stream.prepare("")
		got := map[string]bool{}
		for range 2 {
			_, sent := stream.execute()
			got[sent.ToolUseEventID] = true
		}
		if !got[first] || !got[second] {
			t.Fatalf("%s: tool.execute for %v; want both calls", kind, got)
		}
		if read := f.instanceRead(claim.Bootstrap.InstanceID); read["kind"] != "cloud" || read["computer_id"] != computerID || read["computer_count"] != float64(1) ||
			read["binding_count"] != float64(1) || read["unused"] != false {
			t.Fatalf("%s: instance = %v; want the cloud instance of %s with one computer, one binding and in use", kind, read, computerID)
		}
		if read := s.computerRead(computerID); read["current_instance_id"] != claim.Bootstrap.InstanceID || read["execution_state"] != "ready" ||
			read["binding_id"] != prepared.BindingID || read["cwd"] != "/workspace" || prepared.Cwd != "/workspace" {
			t.Fatalf("%s: computer = %v after %+v; want ready on the claim's instance in its working directory", kind, read, prepared)
		}
	}

	failed := f.session()
	failedComputer := failed.cloudComputer()
	waiting, _ := failed.admitted(failedComputer, "one")
	failedRequest := failed.openRequest(failedComputer)
	f.claim(0, failedRequest, "worker_2")
	if status, body := f.controller("POST", "/v1/instance_requests/"+failedRequest+"/fail", ""); status != 200 {
		t.Fatalf("fail = %d %s", status, body)
	}
	failed.requireNotExecuted(waiting, runtimecontrol.CodeMachineNotProvided, "No machine could be provided for the computer.")
	if records := failed.records(waiting); len(records) != 1 || records[0] != "tool.execute:ended" {
		t.Fatalf("records = %v; want the waiting record ended", records)
	}
	failed.admitted(failedComputer, "two")
	if next := failed.openRequest(failedComputer); next == failedRequest || f.row(`SELECT kind FROM computer_requests WHERE id=$1`, next)["kind"] != "new" {
		t.Fatalf("request after the failed one = %s; want another new request", next)
	}
	if errs := f.sessionErrors(failed.id); len(errs) != 0 {
		t.Fatalf("session errors %v; want none from the failed request", errs)
	}
}

// write_stdin goes to its task's connection and binding: nonempty writes
// carry write_seq 1 and 2 with an empty poll between that carries none, a
// retransmission sends nothing new, and wait_ms and limits pass through.
// Once the task's connection has ended, a write sends nothing and settles
// connection_ended.
func TestControlsFollowTheTaskRoute(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	computerID, stream := s.preparedUserComputer("laptop")
	_, taskID := s.admitted(computerID, "cat")
	_, execute := stream.execute()
	wait := int64(250)
	limits := &bridgev1.ToolVisibleLimits{VisibleLines: ptr(int64(10))}
	type sent struct {
		use      string
		writeSeq protocol.Optional[int64]
	}
	var writes []sent
	for _, chars := range []string{"a\n", "", "b\n"} {
		use, response, err := s.control(taskID, chars, &wait, limits)
		if err != nil || response.GetAdmitted() == nil || response.GetAdmitted().GetTaskId() != taskID {
			t.Fatalf("write %q = %v, %v; want admitted to %s", chars, response, err, taskID)
		}
		event := stream.next()
		stdin, ok := event.Envelope.Payload.(protocol.StdinPayload)
		if !ok || stdin.TaskID != taskID || stdin.BindingID != execute.BindingID || stdin.ToolUseEventID != use || stdin.Chars != chars ||
			stdin.WaitMS.Value != 250 || !stdin.Limits.Set || stdin.Limits.Value.VisibleLines.Value != 10 {
			t.Fatalf("event = %s; want tool.stdin %q to task %s on binding %s", event.Data, chars, taskID, execute.BindingID)
		}
		writes = append(writes, sent{use: use, writeSeq: stdin.WriteSeq})
	}
	if writes[0].writeSeq.Value != 1 || writes[1].writeSeq.Set || writes[2].writeSeq.Value != 2 {
		t.Fatalf("write_seq = %v, %v, %v; want 1, none, 2", writes[0].writeSeq, writes[1].writeSeq, writes[2].writeSeq)
	}
	retransmitted, err := f.runtime.AdmitToolControl(f.ctx, &bridgev1.AdmitToolControlRequest{Scope: s.scope, ToolUseEventId: writes[2].use, TaskId: taskID,
		Chars: "b\n", WaitMs: &wait, Limits: limits})
	if err != nil || retransmitted.GetDuplicate() == nil {
		t.Fatalf("retransmission = %v, %v; want duplicate", retransmitted, err)
	}
	stream.quiet(300 * time.Millisecond)
	if stdins := f.count(`SELECT count(*) FROM connection_instructions WHERE type='tool.stdin'`); stdins != 3 {
		t.Fatalf("%d tool.stdin instructions; want 3", stdins)
	}
	sentEvents := map[string][]byte{}
	stream.close()
	resumed := f.resume(1, stream.cli, stream.connectionID)
	for range 4 {
		event := resumed.next()
		sentEvents[event.Envelope.MessageID] = event.Data
		if !bytes.Equal(f.savedEnvelope(event.Envelope.MessageID), event.Data) {
			t.Fatalf("resent %s differs from its saved envelope", event.Data)
		}
	}
	if stdins := f.count(`SELECT count(*) FROM connection_instructions WHERE type IN ('tool.execute','tool.stdin') AND message_id = ANY($1)`, keys(sentEvents)); stdins != 4 {
		t.Fatalf("the resume resent %d of the task's 4 instructions", stdins)
	}
	stream = resumed
	if _, _, err := s.control("task_0000000000000000", "x", nil, nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("write to an unknown task: %v; want InvalidArgument", err)
	}

	// The lease runs out and the CLI starts a new connection: the task's
	// connection has ended.
	f.exec(`UPDATE instance_connections SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, stream.connectionID)
	stream.close()
	f.connect(stream.cli, everyTool)
	use, response, err := s.control(taskID, "c\n", nil, nil)
	if err != nil || response.GetAdmitted() == nil {
		t.Fatalf("write after the connection ended = %v, %v", response, err)
	}
	s.requireNotExecuted(use, runtimecontrol.CodeConnectionEnded, "The command's machine connection has ended; the command cannot be controlled.")
	if stdins := f.count(`SELECT count(*) FROM session_tool_dispatches WHERE tool_use_event_id=$1`, use); stdins != 0 {
		t.Fatalf("%d records for a write on an ended connection; want none", stdins)
	}
}

// nearLimitText is n bytes of text dense in characters that other JSON
// encoders escape or that need escapes, as Engine's canonical form writes
// them: <, >, & and U+2028 stay as they are, a control character takes six.
func nearLimitText(n int) string {
	const unit = "<>&\u2028\\u0001x" // 1+1+1+3+6+1 bytes
	text := strings.Repeat(unit, n/len(unit))
	return text + strings.Repeat("y", n-len(text))
}

// An input of exactly 4,194,304 bytes and instruction metadata of exactly
// 65,536 bytes, measured with Engine's longest identities, are admitted and
// sent within the complete tool.execute limit, byte for byte as admitted and
// resent unchanged; one byte more of either answers too_large and records
// nothing. write_stdin chars of 32,768 bytes that each escape to six are
// sent within the complete tool.stdin limit; 32,769 answer too_large.
func TestMessageSizeLimits(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	computerID, stream := s.preparedUserComputer("laptop")

	input := func(size int) string {
		head := `{"cmd":"`
		return head + nearLimitText(size-len(head)-len(`"}`)) + `"}`
	}
	exact := input(protocol.ToolInputJSONBytes)
	if compact, err := protocol.CompactToolInput(protocol.ToolExecCommand, []byte(exact)); err != nil || string(compact) != exact {
		t.Fatalf("the input is not in compact form: %v", err)
	}
	policy := func(metadata int, use string) string {
		rule := func(path string) string {
			return `{"schema_version":1,"filesystem":{"rules":[{"path":` + quoteJSON(path) + `,"access":"read"}]},"network":{"mode":"disabled"}}`
		}
		measure := func(policy string) int {
			return len(protocol.Encode(protocol.Downstream{ConnectionID: "conn_" + strings.Repeat("0", 32), MessageID: "msg_" + strings.Repeat("0", 16),
				Traceparent: protocol.Some("00-" + strings.Repeat("0", 32) + "-" + strings.Repeat("0", 16) + "-00"),
				Payload: protocol.ExecuteInstruction{SessionID: s.id, BindingID: "bind_" + strings.Repeat("0", 16), ToolUseEventID: use,
					TaskID: "task_" + strings.Repeat("0", 16), Tool: protocol.ToolExecCommand, Input: protocol.RawObject("{}"), SandboxPolicy: protocol.RawObject(policy)}}))
		}
		path := "/rules/" + strings.Repeat("p", metadata-measure(rule("/rules/")))
		if measure(rule(path)) != metadata {
			t.Fatalf("metadata %d; want %d", measure(rule(path)), metadata)
		}
		return rule(path)
	}
	var admittedPolicy string
	admit := func(inputJSON string, metadata int) (string, *bridgev1.AdmitToolCallResponse) {
		use := s.use(protocol.ToolExecCommand, "machine_tool")
		policyJSON := disabledNetworkPolicy
		if metadata > 0 {
			policyJSON = policy(metadata, use)
		}
		admittedPolicy = policyJSON
		response, err := f.runtime.AdmitToolCall(f.ctx, &bridgev1.AdmitToolCallRequest{Scope: s.scope, ToolUseEventId: use, ComputerId: computerID,
			Tool: protocol.ToolExecCommand, InputJson: inputJSON, SandboxPolicyJson: policyJSON})
		if err != nil {
			t.Fatal(err)
		}
		return use, response
	}
	requireTooLarge := func(use string, response *bridgev1.AdmitToolCallResponse, limit string, bytes int64) {
		t.Helper()
		if response.GetTooLarge().GetLimit() != limit || response.GetTooLarge().GetLimitBytes() != bytes ||
			f.count(`SELECT count(*) FROM session_runtime_tool_results WHERE tool_use_event_id=$1`, use) != 0 {
			t.Fatalf("admission = %v; want too_large %s %d and no row", response, limit, bytes)
		}
	}

	use, response := admit(exact, protocol.MetadataJSONBytes)
	if response.GetAdmitted() == nil {
		t.Fatalf("admission at both limits = %v; want admitted", response)
	}
	event, sent := stream.execute()
	if want, err := protocol.DecodeSandboxPolicy([]byte(admittedPolicy)); err != nil || !reflect.DeepEqual(sent.SandboxPolicy, want) {
		t.Fatalf("sent sandbox_policy %+v; want the admitted policy %+v (%v)", sent.SandboxPolicy, want, err)
	}
	if len(event.Data) > protocol.ExecuteSSEJSONBytes || !bytes.Contains(event.Data, []byte(`"input":`+exact+`,`)) {
		t.Fatalf("tool.execute of %d bytes; want at most %d holding the admitted input byte for byte", len(event.Data), protocol.ExecuteSSEJSONBytes)
	}
	saved := f.row(`SELECT payload_json FROM session_tool_dispatches WHERE tool_use_event_id=$1`, use)["payload_json"].(string)
	if !bytes.Contains(event.Data, []byte(`"payload":`+saved+`,`)) {
		t.Fatal("the sent payload is not the saved one byte for byte")
	}
	if !bytes.Equal(f.savedEnvelope(event.Envelope.MessageID), event.Data) {
		t.Fatal("the first delivery differs from the saved envelope")
	}
	stream.close()
	resumed := f.resume(1, stream.cli, stream.connectionID)
	if again, _ := resumed.execute(); !bytes.Equal(again.Data, event.Data) {
		t.Fatal("the resent tool.execute differs from the first delivery")
	}
	use, response = admit(input(protocol.ToolInputJSONBytes+1), 0)
	requireTooLarge(use, response, runtimecontrol.LimitInput, protocol.ToolInputJSONBytes)
	use, response = admit(`{"cmd":"ls"}`, protocol.MetadataJSONBytes+1)
	requireTooLarge(use, response, runtimecontrol.LimitMetadata, protocol.MetadataJSONBytes)

	_, taskID := s.admitted(computerID, "cat")
	resumed.execute()
	chars := strings.Repeat("\x01", protocol.StdinUTF8Bytes)
	use2, control, err := s.control(taskID, chars, nil, nil)
	if err != nil || control.GetAdmitted() == nil {
		t.Fatalf("write of %d bytes = %v, %v; want admitted", len(chars), control, err)
	}
	stdin := resumed.next()
	if _, ok := stdin.Envelope.Payload.(protocol.StdinPayload); !ok || len(stdin.Data) > protocol.StdinSSEJSONBytes {
		t.Fatalf("tool.stdin of %d bytes; want at most %d", len(stdin.Data), protocol.StdinSSEJSONBytes)
	}
	if state, _ := s.settlement(use2); state != "pending" {
		t.Fatalf("write at the chars limit is %s; want pending", state)
	}
	use3, control, err := s.control(taskID, chars+"\x01", nil, nil)
	if err != nil || control.GetTooLarge().GetLimit() != runtimecontrol.LimitChars ||
		f.count(`SELECT count(*) FROM session_runtime_tool_results WHERE tool_use_event_id=$1`, use3) != 0 {
		t.Fatalf("write of %d bytes = %v, %v; want too_large chars and no row", len(chars)+1, control, err)
	}
}

// A CLI whose ready omits view_image gets no tool.execute for a view_image
// call, and one whose network modes omit direct or managed gets none for a
// call with that network; each call ends not executed, capability_missing.
func TestCallsOutsideTheCapabilityAreNotSent(t *testing.T) {
	f := newMachineTools(t)
	cases := []struct {
		name, tool, input, policy, message string
		capabilities                       protocol.Capabilities
	}{
		{"tool", protocol.ToolViewImage, `{"path":"/tmp/shot.png"}`, disabledNetworkPolicy, "The computer's CLI does not provide this tool.",
			toolExecution([]string{protocol.ToolExecCommand, protocol.ToolEdit}, []string{"disabled", "direct"})},
		{"network", protocol.ToolExecCommand, `{"cmd":"curl"}`, `{"schema_version":1,"filesystem":{"rules":[]},"network":{"mode":"direct"}}`,
			"The computer's CLI cannot enforce this call's sandbox policy.", toolExecution([]string{protocol.ToolExecCommand}, []string{"disabled"})},
		{"managed", protocol.ToolExecCommand, `{"cmd":"curl"}`, `{"schema_version":1,"filesystem":{"rules":[]},"network":{"mode":"managed","rules":{"allow":[],"deny":[],"listen":[]}}}`,
			"The computer's CLI cannot enforce this call's sandbox policy.", everyTool},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := f.session()
			cli := f.userInstance(c.name)
			stream := f.connect(cli, c.capabilities)
			computerID := s.userComputer(cli)
			f.ownerTx(func(tx *dbconnect.Tx) error {
				_, err := computer.IssuePreparation(f.ctx, tx, f.ws, s.id, computerID)
				return err
			})
			stream.prepare("")
			use, response, err := s.admit(computerID, c.tool, c.input, c.policy)
			if err != nil || response.GetAdmitted() == nil {
				t.Fatalf("admission = %v, %v", response, err)
			}
			s.requireNotExecuted(use, runtimecontrol.CodeCapabilityMissing, c.message)
			if records := s.records(use); len(records) != 1 || records[0] != "tool.execute:ended" {
				t.Fatalf("records = %v; want one ended", records)
			}
			stream.quiet(300 * time.Millisecond)
		})
	}
}

// A call of an approval reviewer thread to a cloud computer with no online
// connection settles computer_offline at once and records no request.
func TestReviewerCallsNeverRequestAMachine(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	computerID := s.cloudComputer()
	sessionfixture.SeedBridgeAPIInternalReviewerThread(t, f.admin, string(f.ws), s.id, s.thread, "sthr_review_"+s.id)
	reviewer := s.in("sthr_review_" + s.id)
	use, _ := reviewer.admitted(computerID, "ls")
	reviewer.requireNotExecuted(use, runtimecontrol.CodeComputerOffline, "computer offline")
	if requests, records := f.count(`SELECT count(*) FROM computer_requests WHERE session_id=$1`, s.id), reviewer.records(use); requests != 0 || len(records) != 0 {
		t.Fatalf("%d requests and records %v; want none", requests, records)
	}
}

// coldStartRecords returns the session_computer cold start records of
// computerID in lines, in order: the request and instance phase starts and
// completions, and the completions.
func coldStartRecords(t *testing.T, lines *lockedBuffer, computerID string) []map[string]any {
	t.Helper()
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(lines.String()), "\n") {
		var record map[string]any
		if line == "" || json.Unmarshal([]byte(line), &record) != nil {
			continue
		}
		if record["cold_start.kind"] == "session_computer" && record["computer.id"] == computerID && strings.HasPrefix(record["event"].(string), "cold_start.") &&
			record["cold_start.phase"] != "prepare" {
			records = append(records, record)
		}
	}
	return records
}

// describe renders records as event:phase pairs.
func describe(records []map[string]any) []string {
	var described []string
	for _, record := range records {
		phase, _ := record["cold_start.phase"].(string)
		described = append(described, strings.TrimPrefix(record["event"].(string), "cold_start.")+":"+phase)
	}
	return described
}

func durationMS(t *testing.T, row map[string]any, from, to string) float64 {
	t.Helper()
	start, err := time.Parse(time.RFC3339Nano, row[from].(string))
	if err != nil {
		t.Fatal(err)
	}
	end, err := time.Parse(time.RFC3339Nano, row[to].(string))
	if err != nil {
		t.Fatal(err)
	}
	return float64(end.Sub(start).Milliseconds())
}

// A request admission created records one request phase start, from its
// stored creation; a replay and a second call record none. The first claim
// ends it and starts the instance phase; a release and another hub's claim
// record nothing more, and the instance phase ends at the ready of the
// reclaimed instance. The summary uses the first claim: created 0, first
// claim 3 s, reclaim 20 s and ready 40 s give request 3 s and instance 37 s,
// while the claim's age runs from the reclaim.
func TestRequestPhasesFollowTheFirstClaim(t *testing.T) {
	lines := &lockedBuffer{}
	telemetry := workload.NewTelemetry(workload.TelemetryOptions{Service: "integration", Logger: workload.NewLogger(lines, "integration", "test", "unit")})
	t.Cleanup(workload.InstallTelemetry(telemetry))
	f := newMachineTools(t)
	s := f.session()
	computerID := s.cloudComputer()
	use, _ := s.admitted(computerID, "one")
	if replay, err := f.runtime.AdmitToolCall(f.ctx, &bridgev1.AdmitToolCallRequest{Scope: s.scope, ToolUseEventId: use, ComputerId: computerID,
		Tool: protocol.ToolExecCommand, InputJson: `{"cmd":"one"}`, SandboxPolicyJson: disabledNetworkPolicy}); err != nil || replay.GetDuplicate() == nil {
		t.Fatalf("replay = %v, %v; want duplicate", replay, err)
	}
	s.admitted(computerID, "two")
	requestID := s.openRequest(computerID)
	if got := describe(coldStartRecords(t, lines, computerID)); strings.Join(got, ",") != "phase_started:request" {
		t.Fatalf("records after admission = %v; want one request start", got)
	}

	f.claim(0, requestID, "worker_1")
	stored := f.row(`SELECT created_at, first_claimed_at, claimed_at FROM computer_requests WHERE id=$1`, requestID)
	records := coldStartRecords(t, lines, computerID)
	if got := describe(records); strings.Join(got, ",") != "phase_started:request,phase_completed:request,phase_started:instance" ||
		records[1]["request_ms"] != durationMS(t, stored, "created_at", "first_claimed_at") {
		t.Fatalf("records after the first claim = %v (%v); want the request ended at the stored first claim and the instance started", got, records)
	}
	if status, body := f.controller("POST", "/v1/instance_requests/"+requestID+"/release", ""); status != 200 {
		t.Fatalf("release = %d %s", status, body)
	}
	reclaimed := f.claim(1, requestID, "worker_2")
	again := f.row(`SELECT first_claimed_at, claimed_at FROM computer_requests WHERE id=$1`, requestID)
	if again["first_claimed_at"] != stored["first_claimed_at"] || again["claimed_at"] == stored["claimed_at"] {
		t.Fatalf("after the reclaim %v; want the first claim kept and the latest claim moved from %v", again, stored)
	}
	if got := describe(coldStartRecords(t, lines, computerID)); len(got) != 3 {
		t.Fatalf("records after release and reclaim = %v; want nothing more", got)
	}

	stream := f.connect(f.cli(1, reclaimed.Bootstrap.InstanceID, reclaimed.Bootstrap.CredentialID), everyTool)
	ready := f.row(`SELECT ready_accepted_at FROM instance_connections WHERE id=$1`, stream.connectionID)
	ready["first_claimed_at"] = stored["first_claimed_at"]
	records = coldStartRecords(t, lines, computerID)
	if got := describe(records); len(got) != 4 || got[3] != "phase_completed:instance" || records[3]["instance_ms"] != durationMS(t, ready, "first_claimed_at", "ready_accepted_at") {
		t.Fatalf("records after ready = %v; want the instance phase ended at ready from the first claim", got)
	}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f.exec(`UPDATE computer_requests SET created_at=$2, pending_since=$2, first_claimed_at=$3, claimed_at=$4 WHERE id=$1`, requestID, base, base.Add(3*time.Second), base.Add(20*time.Second))
	f.exec(`UPDATE instance_connections SET ready_accepted_at=$2 WHERE id=$1`, stream.connectionID, base.Add(40*time.Second))
	stream.prepare("")
	records = coldStartRecords(t, lines, computerID)
	completed := records[len(records)-1]
	if completed["event"] != "cold_start.completed" || completed["outcome"] != "ok" || completed["request_ms"] != float64(3000) || completed["instance_ms"] != float64(37000) {
		t.Fatalf("summary = %v; want request 3000 and instance 37000 ms", completed)
	}
	completions := 0
	for _, record := range records {
		if record["event"] == "cold_start.completed" {
			completions++
		}
	}
	if completions != 1 {
		t.Fatalf("%d completions; want one", completions)
	}
}

// A request that fails after its claim ends its phases with outcome
// machine_not_provided; a wake request fulfilled at ready without a claim
// ends its request phase at the fulfilment and has no instance phase; a wake
// request claimed, released and then fulfilled at ready with no other claim
// keeps the request phase of its first claim and ends the instance phase at
// that ready.
func TestEndedRequestsCloseTheirPhases(t *testing.T) {
	lines := &lockedBuffer{}
	telemetry := workload.NewTelemetry(workload.TelemetryOptions{Service: "integration", Logger: workload.NewLogger(lines, "integration", "test", "unit")})
	t.Cleanup(workload.InstallTelemetry(telemetry))
	f := newMachineTools(t)
	s := f.session()
	failed := s.cloudComputer()
	s.admitted(failed, "one")
	requestID := s.openRequest(failed)
	f.claim(0, requestID, "worker_1")
	if status, body := f.controller("POST", "/v1/instance_requests/"+requestID+"/fail", ""); status != 200 {
		t.Fatalf("fail = %d %s", status, body)
	}
	records := coldStartRecords(t, lines, failed)
	last := records[len(records)-1]
	if got := describe(records); strings.Join(got, ",") != "phase_started:request,phase_completed:request,phase_started:instance,phase_completed:instance,completed:" ||
		last["outcome"] != runtimecontrol.CodeMachineNotProvided || last["cold_start.start_kind"] != "new" || last["request_ms"] == nil || last["instance_ms"] == nil {
		t.Fatalf("records of the failed request = %v (%v); want both phases ended and machine_not_provided", got, last)
	}

	woken := s.cloudComputer()
	cli := s.cloudInstance(woken)
	s.admitted(woken, "one")
	wakeID := s.openRequest(woken)
	if kind := f.row(`SELECT kind FROM computer_requests WHERE id=$1`, wakeID)["kind"]; kind != "wake" {
		t.Fatalf("request kind %v; want wake", kind)
	}
	stream := f.connect(cli, everyTool)
	fulfilled := f.row(`SELECT r.created_at, c.ready_accepted_at FROM computer_requests r, instance_connections c WHERE r.id=$1 AND c.id=$2`, wakeID, stream.connectionID)
	records = coldStartRecords(t, lines, woken)
	if got := describe(records); strings.Join(got, ",") != "phase_started:request,phase_completed:request" ||
		records[1]["request_ms"] != durationMS(t, fulfilled, "created_at", "ready_accepted_at") {
		t.Fatalf("records of the fulfilled wake = %v (%v); want the request phase ended at the fulfilment and no instance phase", got, records)
	}
	stream.prepare("")
	records = coldStartRecords(t, lines, woken)
	if completed := records[len(records)-1]; completed["event"] != "cold_start.completed" || completed["cold_start.start_kind"] != "new" ||
		completed["instance_ms"] != nil || completed["request_ms"] == nil {
		t.Fatalf("summary of the fulfilled wake = %v; want a request phase and no instance phase", completed)
	}

	released := s.cloudComputer()
	releasedCLI := s.cloudInstance(released)
	s.admitted(released, "one")
	releasedID := s.openRequest(released)
	f.claim(0, releasedID, "worker_1")
	if status, body := f.controller("POST", "/v1/instance_requests/"+releasedID+"/release", ""); status != 200 {
		t.Fatalf("release = %d %s", status, body)
	}
	releasedStream := f.connect(releasedCLI, everyTool)
	stored := f.row(`SELECT r.created_at, r.first_claimed_at, c.ready_accepted_at FROM computer_requests r, instance_connections c WHERE r.id=$1 AND c.id=$2`,
		releasedID, releasedStream.connectionID)
	records = coldStartRecords(t, lines, released)
	if got := describe(records); strings.Join(got, ",") != "phase_started:request,phase_completed:request,phase_started:instance,phase_completed:instance" ||
		records[1]["request_ms"] != durationMS(t, stored, "created_at", "first_claimed_at") ||
		records[3]["instance_ms"] != durationMS(t, stored, "first_claimed_at", "ready_accepted_at") {
		t.Fatalf("records of the released wake = %v (%v); want the request phase ended at the first claim and the instance phase at ready", got, records)
	}
	releasedStream.prepare("")
	records = coldStartRecords(t, lines, released)
	if completed := records[len(records)-1]; completed["event"] != "cold_start.completed" || completed["outcome"] != "ok" ||
		completed["request_ms"] != records[1]["request_ms"] || completed["instance_ms"] != records[3]["instance_ms"] {
		t.Fatalf("summary of the released wake = %v; want the phases of its records", completed)
	}
}

// At a normal turn end, CancelToolCall sends one tool.cancel, for the call
// whose tool.execute was sent; a call never sent ends cancelled before start
// and a waiting write_stdin gets no cancel. An interrupt after that sends no
// second cancel; an interrupt of a turn with an uncancelled sent call sends
// one, and its results carry the interrupt's error types and messages.
func TestTurnEndAndInterruptCancelOnlySentCalls(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	sentComputer, stream := s.preparedUserComputer("sent")
	waitingCLI := f.userInstance("waiting")
	waitingStream := f.connect(waitingCLI, everyTool)
	waitingComputer := s.userComputer(waitingCLI)
	sent, taskID := s.admitted(sentComputer, "sleep 100")
	_, execute := stream.execute()
	never, _ := s.admitted(waitingComputer, "make")
	if _, ok := waitingStream.next().Envelope.Payload.(protocol.SessionPreparePayload); !ok {
		t.Fatal("the waiting computer got no session.prepare")
	}
	write, _, err := s.control(taskID, "x", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := stream.next().Envelope.Payload.(protocol.StdinPayload); !ok {
		t.Fatal("the write was not sent")
	}
	for _, use := range []string{sent, never, write, sent} {
		if response, err := f.runtime.CancelToolCall(f.ctx, &bridgev1.CancelToolCallRequest{Scope: s.scope, ToolUseEventId: use}); err != nil || response.GetCancelled() == nil {
			t.Fatalf("cancel of %s = %v, %v", use, response, err)
		}
	}
	cancel := stream.next()
	payload, ok := cancel.Envelope.Payload.(protocol.CancelPayload)
	if !ok || payload.TaskID != taskID || payload.BindingID != execute.BindingID || !strings.HasPrefix(payload.ToolUseEventID, "tcan_") || len(cancel.Data) > protocol.DefaultMessageJSONBytes {
		t.Fatalf("event = %s; want one tool.cancel of task %s within %d bytes", cancel.Data, taskID, protocol.DefaultMessageJSONBytes)
	}
	stream.quiet(300 * time.Millisecond)
	waitingStream.quiet(300 * time.Millisecond)
	s.requireUnknownOutcome(sent, "Tool execution outcome is unknown because the turn ended.")
	s.requireUnknownOutcome(write, "Tool execution outcome is unknown because the turn ended.")
	s.requireNotExecuted(never, runtimecontrol.CodeCancelledBeforeStart, "Tool execution was cancelled before it started.")
	if records := s.records(never); len(records) != 1 || records[0] != "tool.execute:ended" {
		t.Fatalf("records of the never-sent call = %v; want one ended", records)
	}
	results := s.interrupt()
	stream.quiet(300 * time.Millisecond)
	if cancels := f.count(`SELECT count(*) FROM connection_instructions WHERE type='tool.cancel'`); cancels != 1 {
		t.Fatalf("%d tool.cancel instructions after the interrupt; want the turn end's one", cancels)
	}
	requireInterruptError(t, results[sent], "tool_outcome_unknown", "Tool execution outcome is unknown because the turn ended: the call may have run; check effects before retrying.")
	for _, use := range []string{sent, never, write} {
		if state, _ := s.settlement(use); state != "consumed" {
			t.Fatalf("call %s is %s after the interrupt; want consumed", use, state)
		}
	}

	other := f.session()
	otherComputer, otherStream := other.preparedUserComputer("other")
	otherWaitingCLI := f.userInstance("other-waiting")
	otherWaiting := f.connect(otherWaitingCLI, everyTool)
	otherWaitingComputer := other.userComputer(otherWaitingCLI)
	running, runningTask := other.admitted(otherComputer, "sleep 100")
	otherStream.execute()
	unsent, _ := other.admitted(otherWaitingComputer, "make")
	otherWaiting.next()
	results = other.interrupt()
	if cancel, ok := otherStream.next().Envelope.Payload.(protocol.CancelPayload); !ok || cancel.TaskID != runningTask {
		t.Fatalf("the interrupt sent %v; want one tool.cancel of task %s", cancel, runningTask)
	}
	otherStream.quiet(300 * time.Millisecond)
	otherWaiting.quiet(300 * time.Millisecond)
	requireInterruptError(t, results[running], "tool_outcome_unknown", "Tool execution outcome is unknown because the Thread was interrupted: the call may have run; check effects before retrying.")
	requireInterruptError(t, results[unsent], "tool_dispatch_failed", "The tool call was not executed: Tool execution was cancelled before it started.")
}

// A runtime termination's terminal result closes, in its transaction, the
// machine-bound call it answers, in the terminating thread and, when the main
// thread terminates, in each sibling. A call never sent ends not executed and
// stays unsent however late its preparation completes; a sent one gets its
// task's one tool.cancel on its original route, or an ended unsent one when
// that route is gone; a write_stdin gets none; a running report the Runtime
// never committed gets the tool.cancel too; and each call is consumed by its
// one result, which wakes its waiter. Another thread's call and a background
// command whose result the Runtime committed are left alone.
func TestRuntimeTerminationClosesItsMachineCalls(t *testing.T) {
	f := newMachineTools(t)
	closed := func(s *toolSession, use string, record map[string]any) {
		t.Helper()
		got := f.row(`SELECT r.execution_state, r.consumption_reason, r.result_json, count(e.event_id) AS results,
			bool_and(e.event_id = r.consumed_by_terminal_event_id) AS linked
			FROM session_runtime_tool_results r LEFT JOIN session_events e ON e.session_id=r.session_id
			 AND e.session_thread_id=r.session_thread_id AND e.tool_use_event_id=r.tool_use_event_id AND e.type='agent.tool_result'
			WHERE r.session_id=$1 AND r.tool_use_event_id=$2 GROUP BY r.execution_state, r.consumption_reason, r.result_json`, s.id, use)
		var settled map[string]any
		_ = json.Unmarshal([]byte(got["result_json"].(string)), &settled)
		for key, want := range record {
			if settled[key] != want {
				t.Fatalf("call %s settled %v; want %s %v", use, settled, key, want)
			}
		}
		if got["execution_state"] != "consumed" || got["consumption_reason"] != "runtime_terminated" || got["results"] != float64(1) || got["linked"] != true {
			t.Fatalf("call %s = %v; want consumed by its one termination result", use, got)
		}
	}
	notExecuted := map[string]any{"settlement": "not_executed", "code": runtimecontrol.CodeCancelledBeforeStart}
	unknown := map[string]any{"settlement": "unknown_outcome", "message": runtimecontrol.MessageOutcomeUnknownTermination}
	executes := func(s *toolSession, use string) int {
		return f.count(`SELECT count(*) FROM connection_instructions i JOIN session_tool_dispatches d ON d.message_id=i.message_id
			WHERE d.session_id=$1 AND d.tool_use_event_id=$2 AND i.type='tool.execute'`, s.id, use)
	}
	cancelsOf := func(s *toolSession, task string) int {
		return f.count(`SELECT count(*) FROM session_tool_dispatches WHERE session_id=$1 AND task_id=$2 AND type='tool.cancel'`, s.id, task)
	}
	// background starts a command on computerID whose running report the
	// Runtime commits as the tool's result, so it runs in the background.
	background := func(s *toolSession, computerID string, stream *cliStream, cmd string) string {
		t.Helper()
		use, task := s.admitted(computerID, cmd)
		instruction, _ := stream.execute()
		stream.acked(execution(stream.connectionID, "report_"+task, instruction, "running", 1, execEnvelope("running", task, "")), "report_"+task)
		response, err := f.runtime.Bridge.SettleToolResult(f.ctx, sessionfixture.BridgeToolSettlementRequestForTest(s.scope,
			sessionfixture.BridgeCompletedToolSettlementForTest(use, "running in the background")))
		if err != nil {
			t.Fatal(err)
		}
		sessionfixture.BridgeRequireToolSettlementOutcomeForTest(t, response, "committed")
		return task
	}
	running := func(s *toolSession, task string) {
		t.Helper()
		if status := f.row(`SELECT status FROM session_background_tasks WHERE session_id=$1 AND task_id=$2`, s.id, task)["status"]; status != "running" || cancelsOf(s, task) != 0 {
			t.Fatalf("background task %s is %v with %d tool.cancel records; want running with none", task, status, cancelsOf(s, task))
		}
	}

	t.Run("child", func(t *testing.T) {
		s := f.session()
		c, turn := s.child("worker")
		prepared, stream := s.preparedUserComputer("child-prepared")
		gone, goneStream := s.preparedUserComputer("child-gone")
		waitingCLI := f.userInstance("child-waiting")
		waitingStream := f.connect(waitingCLI, everyTool)
		waiting := s.userComputer(waitingCLI)

		unsent, _ := c.admitted(waiting, "make")
		prepare := waitingStream.next()
		if _, ok := prepare.Envelope.Payload.(protocol.SessionPreparePayload); !ok {
			t.Fatalf("event = %s; want session.prepare", prepare.Data)
		}
		parent, _ := s.admitted(waiting, "make parent")
		noInstance, _ := c.admitted(s.cloudComputer(), "make cloud")
		ready, readyTask := c.admitted(prepared, "sleep 100")
		stream.execute()
		write, _, err := c.control(readyTask, "x", nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := stream.next().Envelope.Payload.(protocol.StdinPayload); !ok {
			t.Fatal("the write was not sent")
		}
		reported, reportedTask := c.admitted(prepared, "sleep 200")
		reportedInstruction, _ := stream.execute()
		reportedRunning := execution(stream.connectionID, "report_reported", reportedInstruction, "running", 1, execEnvelope("running", reportedTask, ""))
		reportedTerminal := execution(stream.connectionID, "report_late", reportedInstruction, "terminal", 2, execEnvelope("success", reportedTask, "done"))
		stream.held(enginetest.CaseRanThenEnded, reportedInstruction, reportedRunning, reportedTerminal)
		stream.acked(reportedRunning, "report_reported")
		committedTask := background(c, prepared, stream, "sleep 300")
		unrouted, _ := c.admitted(gone, "sleep 400")
		goneStream.execute()
		f.exec(`UPDATE instance_connections SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, goneStream.connectionID)

		notices := f.listenForResults()
		awaited := make(chan error, 1)
		go func() {
			_, err := f.runtime.AwaitToolCall(f.ctx, &bridgev1.AwaitToolCallRequest{Scope: c.scope, ToolUseEventId: unsent})
			awaited <- err
		}()
		if response := c.terminate(turn); response.GetCommitted() == nil {
			t.Fatalf("termination = %v; want committed", response)
		}
		select {
		case err := <-awaited:
			if status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("AwaitToolCall of a terminated call = %v; want FailedPrecondition", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("AwaitToolCall of a terminated call did not return within 10 seconds")
		}
		if status := f.row(`SELECT status FROM session_threads WHERE session_id=$1 AND id=$2`, s.id, c.thread)["status"]; status != "failed" {
			t.Fatalf("child thread is %v; want failed", status)
		}
		closed(c, unsent, notExecuted)
		closed(c, noInstance, notExecuted)
		closed(c, ready, unknown)
		closed(c, write, unknown)
		closed(c, unrouted, unknown)
		closed(c, reported, map[string]any{"settlement": "report", "phase": "running"})
		for use, want := range map[string][]string{
			unsent: {"tool.execute:ended"}, noInstance: {"tool.execute:ended"}, write: {"tool.stdin:ready"},
			ready: {"tool.execute:ready", "tool.cancel:ready"}, reported: {"tool.execute:ready", "tool.cancel:ready"},
			unrouted: {"tool.execute:ready", "tool.cancel:ended"}, parent: {"tool.execute:waiting_preparation"},
		} {
			if got := c.records(use); !reflect.DeepEqual(got, want) {
				t.Fatalf("records of %s = %v; want %v", use, got, want)
			}
		}
		sentCancels := map[string]bool{}
		for range 2 {
			cancel, ok := stream.next().Envelope.Payload.(protocol.CancelPayload)
			if !ok {
				t.Fatal("the termination sent no tool.cancel")
			}
			sentCancels[cancel.TaskID] = true
		}
		if !sentCancels[readyTask] || !sentCancels[reportedTask] {
			t.Fatalf("tool.cancel sent for %v; want tasks %s and %s", sentCancels, readyTask, reportedTask)
		}
		stream.quiet(300 * time.Millisecond)
		goneStream.quiet(300 * time.Millisecond)
		running(c, committedTask)
		for _, hint := range c.hints(unsent, noInstance, ready, write, unrouted) {
			notices.require(t, hint, 1)
		}
		for _, hint := range append(c.hints(reported), s.hints(parent)...) {
			notices.require(t, hint, 0)
		}

		// A late preparation sends the other thread's call alone, and a late
		// report or a repeated termination changes nothing.
		waitingStream.report(prepare, "")
		if _, payload := waitingStream.execute(); payload.ToolUseEventID != parent {
			t.Fatalf("the late preparation sent %s; want only %s", payload.ToolUseEventID, parent)
		}
		waitingStream.quiet(300 * time.Millisecond)
		if n := executes(c, unsent); n != 0 {
			t.Fatalf("%d tool.execute of the terminated call after its preparation; want 0", n)
		}
		stream.acked(reportedTerminal, "report_late")
		if response := c.terminate(turn); response.GetDuplicate() == nil {
			t.Fatalf("repeated termination = %v; want duplicate", response)
		}
		stream.quiet(300 * time.Millisecond)
		closed(c, reported, map[string]any{"settlement": "report", "phase": "running"})
		if n := cancelsOf(c, readyTask) + cancelsOf(c, reportedTask); n != 2 {
			t.Fatalf("%d tool.cancel records after the late report and repeated termination; want 2", n)
		}
	})

	t.Run("main", func(t *testing.T) {
		s := f.session()
		turn := s.openTurn()
		c, _ := s.child("sibling")
		prepared, stream := s.preparedUserComputer("main-prepared")
		waitingCLI := f.userInstance("main-waiting")
		waitingStream := f.connect(waitingCLI, everyTool)
		waiting := s.userComputer(waitingCLI)

		sibling, _ := c.admitted(waiting, "make")
		prepare := waitingStream.next()
		childTask := background(c, prepared, stream, "sleep 100")
		mainTask := background(s, prepared, stream, "sleep 200")
		if response := s.terminate(turn); response.GetCommitted() == nil {
			t.Fatalf("termination = %v; want committed", response)
		}
		if status := f.row(`SELECT status FROM session_threads WHERE session_id=$1 AND id=$2`, s.id, c.thread)["status"]; status != "terminated" {
			t.Fatalf("sibling thread is %v; want terminated", status)
		}
		closed(c, sibling, notExecuted)
		if records := c.records(sibling); !reflect.DeepEqual(records, []string{"tool.execute:ended"}) {
			t.Fatalf("records of the sibling's call = %v; want one ended tool.execute", records)
		}
		running(c, childTask)
		running(s, mainTask)
		stream.quiet(300 * time.Millisecond)

		waitingStream.report(prepare, "")
		waitingStream.quiet(300 * time.Millisecond)
		if n := executes(c, sibling); n != 0 {
			t.Fatalf("%d tool.execute of the terminated sibling's call after its preparation; want 0", n)
		}
	})

	// Job Runner terminates a Session whose interrupt it could not deliver,
	// under its own database role, through the same closeout.
	t.Run("Job Runner", func(t *testing.T) {
		s := f.session()
		c, _ := s.child("sibling")
		prepared, stream := s.preparedUserComputer("runner-prepared")
		waitingCLI := f.userInstance("runner-waiting")
		waitingStream := f.connect(waitingCLI, everyTool)
		waiting := s.userComputer(waitingCLI)

		unsent, _ := s.admitted(waiting, "make")
		prepare := waitingStream.next()
		sibling, _ := c.admitted(waiting, "make sibling")
		noInstance, _ := s.admitted(s.cloudComputer(), "make cloud")
		ready, readyTask := s.admitted(prepared, "sleep 100")
		stream.execute()
		reported, reportedTask := s.admitted(prepared, "sleep 200")
		reportedInstruction, _ := stream.execute()
		stream.acked(execution(stream.connectionID, "report_reported", reportedInstruction, "running", 1, execEnvelope("running", reportedTask, "")), "report_reported")

		sequence := sessionfixture.NextBridgeAPIEventSequenceForTest(t, f.admin, s.id, s.thread)
		eventID, inputID := "evt_interrupt_"+s.id, "rin_interrupt_"+s.id
		seedBridgeAPIEvent(t, f.admin, string(f.ws), s.id, s.thread, eventID, sequence, "user.interrupt", `{}`)
		seedRuntimeInboxBirthForJob(t, f.admin, jobrunner.RuntimeJob{WorkspaceID: string(f.ws), SessionID: s.id, SessionThreadID: s.thread,
			RuntimeInputID: inputID, InputKind: "interrupt_control", EventIDs: []string{eventID}, SequenceFrom: sequence, SequenceTo: sequence})
		queueStore := queue.NewPostgreSQLStore(f.owner)
		enqueueInterruptExhaustionJob(t, queueStore, s.id, s.thread, inputID, "interrupt_control", eventID, sequence, 1, time.Now().UTC().Add(-time.Minute))
		deliverer := &postgresFinalizingDeliverer{store: fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(f.workloads.OpenWorkload(t, "job_runner", nil)), f.admin, 9090)}
		runner := &jobrunner.JobRunner{Queue: tetralqueue.NewServer(queueStore, nil), Deliverer: deliverer,
			Config: jobrunner.JobRunnerConfig{LeaseOwner: "machine-tools-exhaustion", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour}}
		if active, err := acquireAndJoinJobRunnerActive(f.ctx, runner); err != nil || !active {
			t.Fatalf("Job Runner's final interrupt attempt ran %t: %v", active, err)
		}
		if status := f.row(`SELECT status FROM session_threads WHERE session_id=$1 AND id=$2`, s.id, c.thread)["status"]; status != "terminated" {
			t.Fatalf("sibling thread is %v; want terminated", status)
		}
		closed(s, unsent, notExecuted)
		closed(s, noInstance, notExecuted)
		closed(s, ready, unknown)
		closed(s, reported, map[string]any{"settlement": "report", "phase": "running"})
		closed(c, sibling, notExecuted)
		sentCancels := map[string]bool{}
		for range 2 {
			cancel, ok := stream.next().Envelope.Payload.(protocol.CancelPayload)
			if !ok {
				t.Fatal("the termination sent no tool.cancel")
			}
			sentCancels[cancel.TaskID] = true
		}
		if !sentCancels[readyTask] || !sentCancels[reportedTask] {
			t.Fatalf("tool.cancel sent for %v; want tasks %s and %s", sentCancels, readyTask, reportedTask)
		}
		waitingStream.report(prepare, "")
		waitingStream.quiet(300 * time.Millisecond)
		if n := executes(s, unsent) + executes(c, sibling); n != 0 {
			t.Fatalf("%d tool.execute of the terminated calls after their preparation; want 0", n)
		}
	})
}

func requireInterruptError(t *testing.T, result *bridgev1.RuntimeInterruptToolResult, errorType, message string) {
	t.Helper()
	var failure struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	}
	if result == nil || json.Unmarshal([]byte(result.GetError().GetErrorJson()), &failure) != nil || failure.Type != errorType || failure.Message != message {
		t.Fatalf("interrupt result %v; want %s %q", result, errorType, message)
	}
}

func ptr[T any](value T) *T { return &value }

func keys(m map[string][]byte) []string {
	var keys []string
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}
