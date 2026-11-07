package integration

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/integration/enginetest"
	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/runtimecontrol/resultnotify"
	"github.com/tetral-ai/tetral/internal/workload"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// Tests of POST /v1/client/tool-status: reports from a test CLI over real
// HTTP to two computer-hub servers, settlement in PostgreSQL and the wake-up
// of Bridge's AwaitToolCall.

// A report sent again with its message_id after a lost ACK gets the
// identical ACK and changes nothing more: one stored report, one settlement
// and one result notice. A protocol error sent again gets its first ACK, and
// one with no readable correlation is still recorded.
func TestAReportSentAgainGetsItsFirstAcknowledgement(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	computerID, stream := s.preparedUserComputer("laptop")
	notices := f.listenForResults()
	use, task := s.admitted(computerID, "make test")
	instruction, _ := stream.execute()
	body := execution(stream.connectionID, "report_terminal", instruction, "terminal", 1, execEnvelope("success", task, "ok"))
	first := stream.acked(body, "report_terminal")
	again := stream.acked(body, "report_terminal")
	if !bytes.Equal(first.Body, again.Body) {
		t.Fatalf("second ACK %s; want the first %s", again.Body, first.Body)
	}
	if stored := f.row(`SELECT count(*) AS n, min(body_json) AS body FROM session_tool_reports WHERE task_id=$1`, task); stored["n"] != 1.0 || stored["body"] != string(body) {
		t.Fatalf("stored reports = %v; want the one report as sent", stored)
	}
	if state, record := s.settlement(use); state != "terminal_unconsumed" || record["settlement"] != "report" || record["phase"] != "terminal" {
		t.Fatalf("call = %s %v; want settled by the terminal report", state, record)
	}
	notices.require(t, s.hints(use)[0], 1)

	unreadable := protocol.ProtocolErrorReport{ConnectionID: stream.connectionID, MessageID: "perr_unreadable",
		Error: protocol.ProtocolError{Code: string(protocol.ErrorInvalidMessage), Message: "no readable message_id"}}
	var acks [][]byte
	for range 2 {
		response, err := stream.cli.ProtocolError(f.ctx, unreadable)
		if ack, ackErr := response.Acknowledgement(); err != nil || ackErr != nil || ack.MessageID != unreadable.MessageID {
			t.Fatalf("protocol error = %d %s, %v", response.Status, response.Body, err)
		}
		acks = append(acks, response.Body)
	}
	if !bytes.Equal(acks[0], acks[1]) {
		t.Fatalf("protocol error ACKs differ: %s, %s", acks[0], acks[1])
	}
	if n := f.count(`SELECT count(*) FROM connection_protocol_errors WHERE message_id=$1 AND request_message_id IS NULL`, unreadable.MessageID); n != 1 {
		t.Fatalf("%d protocol error records; want 1", n)
	}
}

// A message_id is one message: a report's identity reused by a protocol
// error, a protocol error's reused by a report, and a report's reused with
// other content each get 409 message_conflict and change nothing.
func TestReportsAndProtocolErrorsNeverShareAnIdentity(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	computerID, stream := s.preparedUserComputer("laptop")
	use, task := s.admitted(computerID, "make test")
	instruction, _ := stream.execute()
	stream.acked(execution(stream.connectionID, "report_1", instruction, "running", 1, execEnvelope("running", task, "")), "report_1")
	conflict := func(response enginetest.Response, err error) {
		t.Helper()
		if failure, decodeErr := response.Error(); err != nil || decodeErr != nil || response.Status != http.StatusConflict || failure.Code != protocol.ErrorMessageConflict {
			t.Fatalf("answer = %d %s, %v; want 409 message_conflict", response.Status, response.Body, err)
		}
	}
	conflict(stream.cli.ProtocolError(f.ctx, protocol.ProtocolErrorReport{ConnectionID: stream.connectionID, MessageID: "report_1",
		Error: protocol.ProtocolError{Code: string(protocol.ErrorInvalidMessage), Message: "unreadable"}}))
	perr := protocol.ProtocolErrorReport{ConnectionID: stream.connectionID, MessageID: "perr_1",
		Error: protocol.ProtocolError{Code: string(protocol.ErrorInvalidMessage), Message: "unreadable"}}
	if response, err := stream.cli.ProtocolError(f.ctx, perr); err != nil || response.Status != http.StatusOK {
		t.Fatalf("protocol error = %d %s, %v", response.Status, response.Body, err)
	}
	stream.refused(execution(stream.connectionID, "perr_1", instruction, "terminal", 2, execEnvelope("success", task, "ok")), http.StatusConflict, protocol.ErrorMessageConflict)
	stream.refused(execution(stream.connectionID, "report_1", instruction, "running", 1, execEnvelope("running", task, "changed")), http.StatusConflict, protocol.ErrorMessageConflict)
	if n := f.count(`SELECT count(*) FROM connection_protocol_errors WHERE connection_id=$1`, stream.connectionID); n != 1 {
		t.Fatalf("%d protocol error records; want 1", n)
	}
	if state, record := s.settlement(use); state != "terminal_unconsumed" || record["phase"] != "running" {
		t.Fatalf("call = %s %v; want settled by the running report alone", state, record)
	}
}

// A running report settles the call and starts its background task; the
// terminal report, posted to the other hub server, ends the task and writes
// one task notification for the calling thread and its job in the same
// transaction, which also marks the tool.execute reported so it is not sent
// again. A running report after that is stored and acknowledged and
// changes nothing. A terminal report with no running report before it
// settles its call and writes no background task and no task notification.
func TestATerminalReportAfterRunningEndsTheBackgroundTaskOnce(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	computerID, stream := s.preparedUserComputer("laptop")
	use, task := s.admitted(computerID, "sleep 60")
	instruction, _ := stream.execute()
	stream.acked(execution(stream.connectionID, "report_running", instruction, "running", 1, execEnvelope("running", task, "")), "report_running")
	if state, record := s.settlement(use); state != "terminal_unconsumed" || record["settlement"] != "report" || record["phase"] != "running" {
		t.Fatalf("call = %s %v; want settled by the running report", state, record)
	}
	running := f.row(`SELECT status, provider, computer_id, connection_id, dispatch_message_id, sandbox_id FROM session_background_tasks WHERE task_id=$1`, task)
	if running["status"] != "running" || running["provider"] != "computer" || running["computer_id"] != computerID ||
		running["connection_id"] != stream.connectionID || running["dispatch_message_id"] != instruction.Envelope.MessageID || running["sandbox_id"] != nil {
		t.Fatalf("background task = %v; want running on the computer's connection", running)
	}

	other := *stream
	other.cli = f.cli(1, stream.cli.InstanceID, stream.cli.CredentialID)
	other.acked(execution(stream.connectionID, "report_terminal", instruction, "terminal", 2, execEnvelope("success", task, "done")), "report_terminal")
	ended := f.row(`SELECT status, terminal_result_json::jsonb->'result'->'stdout'->>'text' AS stdout FROM session_background_tasks WHERE task_id=$1`, task)
	if ended["status"] != "completed" || ended["stdout"] != "done" {
		t.Fatalf("background task = %v; want completed with the terminal result", ended)
	}
	inputID := queue.FormatTaskNotificationRuntimeInputID(task)
	if n := f.count(`SELECT count(*) FROM session_runtime_inbox WHERE runtime_input_id=$1 AND input_kind='task_notification' AND status='queued' AND session_thread_id=$2`, inputID, s.thread); n != 1 {
		t.Fatalf("%d task notifications; want one queued for the calling thread", n)
	}
	if n := f.count(`SELECT count(*) FROM queue_jobs WHERE kind=$1 AND payload_json::jsonb->>'runtime_input_id'=$2`, queue.KindRuntimeInput, inputID); n != 1 {
		t.Fatalf("%d runtime input jobs; want 1", n)
	}
	if records := s.records(use); len(records) != 1 || records[0] != "tool.execute:reported" {
		t.Fatalf("records = %v; want the tool.execute reported", records)
	}
	if marked := f.row(`SELECT i.reported_at IS NOT NULL AND i.xmin::text=d.xmin::text AS marked FROM connection_instructions i
		JOIN session_tool_dispatches d ON d.message_id=i.message_id WHERE i.message_id=$1`, instruction.Envelope.MessageID)["marked"]; marked != true {
		t.Fatal("the tool.execute was not marked reported in the transaction that recorded its terminal report")
	}

	stream.acked(execution(stream.connectionID, "report_late", instruction, "running", 1, execEnvelope("running", task, "late")), "report_late")
	if status := f.row(`SELECT status FROM session_background_tasks WHERE task_id=$1`, task)["status"]; status != "completed" {
		t.Fatalf("background task %v after a late running report; want completed", status)
	}
	if n := f.count(`SELECT count(*) FROM session_runtime_inbox WHERE runtime_input_id=$1`, inputID); n != 1 {
		t.Fatalf("%d task notifications after a late running report; want 1", n)
	}
	if n := f.count(`SELECT count(*) FROM session_tool_reports WHERE task_id=$1`, task); n != 3 {
		t.Fatalf("%d stored reports; want 3", n)
	}

	foreground, foregroundTask := s.admitted(computerID, "make test")
	foregroundInstruction, _ := stream.execute()
	stream.acked(execution(stream.connectionID, "report_foreground", foregroundInstruction, "terminal", 1, execEnvelope("success", foregroundTask, "ok")), "report_foreground")
	if state, record := s.settlement(foreground); state != "terminal_unconsumed" || record["settlement"] != "report" || record["phase"] != "terminal" {
		t.Fatalf("foreground call = %s %v; want settled by its terminal report", state, record)
	}
	foregroundInput := queue.FormatTaskNotificationRuntimeInputID(foregroundTask)
	if tasks, notices, jobs := f.count(`SELECT count(*) FROM session_background_tasks WHERE task_id=$1`, foregroundTask),
		f.count(`SELECT count(*) FROM session_runtime_inbox WHERE runtime_input_id=$1`, foregroundInput),
		f.count(`SELECT count(*) FROM queue_jobs WHERE kind=$1 AND payload_json::jsonb->>'runtime_input_id'=$2`, queue.KindRuntimeInput, foregroundInput); tasks+notices+jobs != 0 {
		t.Fatalf("a foreground call wrote %d background tasks, %d task notifications and %d jobs; want none", tasks, notices, jobs)
	}
}

// A control reply and the execution's terminal report are acknowledged by
// their own message_id in either order; the reply settles its write_stdin and
// the terminal report its task.
func TestControlRepliesAndExecutionReportsAreAcknowledgedApart(t *testing.T) {
	for _, replyFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "reply first", false: "terminal first"}[replyFirst], func(t *testing.T) {
			f := newMachineTools(t)
			s := f.session()
			computerID, stream := s.preparedUserComputer("laptop")
			_, task := s.admitted(computerID, "cat")
			execute, _ := stream.execute()
			stream.acked(execution(stream.connectionID, "report_running", execute, "running", 1, execEnvelope("running", task, "")), "report_running")
			stdinUse, response, err := s.control(task, "", nil, nil)
			if err != nil || response.GetAdmitted() == nil {
				t.Fatalf("write_stdin = %v, %v", response, err)
			}
			stdin := stream.next()
			reply := reportJSON(stream.connectionID, "report_reply", stdin, `"kind":"control","result":`+
				strings.Replace(execEnvelope("success", task, "bye"), `"tool":"exec_command"`, `"tool":"stdin"`, 1))
			terminal := execution(stream.connectionID, "report_terminal", execute, "terminal", 2, execEnvelope("success", task, "bye"))
			posts := [][2]any{{reply, "report_reply"}, {terminal, "report_terminal"}}
			if !replyFirst {
				posts[0], posts[1] = posts[1], posts[0]
			}
			for _, post := range posts {
				stream.acked(post[0].([]byte), post[1].(string))
			}
			if state, record := s.settlement(stdinUse); state != "terminal_unconsumed" || record["settlement"] != "reply" {
				t.Fatalf("write_stdin = %s %v; want settled by its reply", state, record)
			}
			if status := f.row(`SELECT status FROM session_background_tasks WHERE task_id=$1`, task)["status"]; status != "completed" {
				t.Fatalf("background task %v; want completed", status)
			}
		})
	}
}

// The complete limit is the call's: a view_image image of exactly
// 10,485,760 decoded bytes is accepted and its row holds the image's
// reference alone; one byte more, a false size_bytes and content of another
// type get 400 invalid_message; an exec_command report over 327,680 bytes,
// also one whose result holds an image, gets 413 request_too_large. No
// refused report is acknowledged or stored.
func TestReportSizesFollowTheCallsTool(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	computerID, stream := s.preparedUserComputer("laptop")
	image := func(size int, declared int, mime string) string {
		data := append([]byte("\x89PNG\r\n\x1a\n"), bytes.Repeat([]byte{0}, size-8)...)
		return `{"schema_version":1,"tool":"view_image","status":"success","truncated":false,"error":null,"result":{"mime":"` + mime +
			`","size_bytes":` + fmt.Sprint(declared) + `,"data_base64":"` + base64.StdEncoding.EncodeToString(data) + `"},` +
			`"metrics":{"wall_time_ms":5,"queued_ms":0,"drive_wait_ms":0,"run_ms":5,"sync_ms":0}}`
	}
	viewImage := func() (string, enginetest.Event) {
		use, response, err := s.admit(computerID, protocol.ToolViewImage, `{"path":"/Users/me/shop/shot.png"}`, disabledNetworkPolicy)
		if err != nil || response.GetAdmitted() == nil {
			t.Fatalf("view_image = %v, %v", response, err)
		}
		instruction, _ := stream.execute()
		return use, instruction
	}

	use, instruction := viewImage()
	for _, refused := range []string{image(protocol.ViewImageDecodedBytes+1, protocol.ViewImageDecodedBytes+1, "image/png"),
		image(1024, 1023, "image/png"), image(1024, 1024, "image/jpeg")} {
		stream.refused(execution(stream.connectionID, "report_image", instruction, "terminal", 1, refused), http.StatusBadRequest, protocol.ErrorInvalidMessage)
	}
	stream.acked(execution(stream.connectionID, "report_image", instruction, "terminal", 1, image(protocol.ViewImageDecodedBytes, protocol.ViewImageDecodedBytes, "image/png")), "report_image")
	stored := f.row(`SELECT octet_length(body_json) AS size, body_json::jsonb->'result'->'result' AS result FROM session_tool_reports WHERE message_id='report_image'`)
	result := stored["result"].(map[string]any)
	attachment, _ := result["attachment"].(map[string]any)
	if stored["size"].(float64) > 4096 || result["data_base64"] != nil || attachment["media_type"] != "image/png" || attachment["size_bytes"] != float64(protocol.ViewImageDecodedBytes) {
		t.Fatalf("stored result = %v (%v bytes); want the image's reference alone", result, stored["size"])
	}
	if state, record := s.settlement(use); state != "terminal_unconsumed" || strings.Contains(fmt.Sprint(record), "data_base64") {
		t.Fatalf("view_image call = %s; want settled with the reference", state)
	}
	if n := f.count(`SELECT count(*) FROM session_attachments WHERE attachment_id=$1`, attachment["attachment_id"]); n != 1 {
		t.Fatalf("%d stored images; want 1", n)
	}

	_, task := s.admitted(computerID, "make test")
	exec, _ := stream.execute()
	ordinary := execution(stream.connectionID, "report_large", exec, "terminal", 1, execEnvelope("success", task, "PAD"))
	padded := bytes.Replace(ordinary, []byte("PAD"), bytes.Repeat([]byte("v"), protocol.OrdinaryToolStatusJSONBytes+1-len(ordinary)+3), 1)
	stream.refused(padded, http.StatusRequestEntityTooLarge, protocol.ErrorRequestTooLarge)
	withImage := execution(stream.connectionID, "report_large", exec, "terminal", 1, image(290_000, 290_000, "image/png"))
	if len(withImage) < 350_000 || len(withImage) > 400_000 {
		t.Fatalf("image report of %d bytes; want about 400,000", len(withImage))
	}
	stream.refused(withImage, http.StatusRequestEntityTooLarge, protocol.ErrorRequestTooLarge)
}

// A large report upload that keeps progressing is accepted after more than
// 10 seconds, one that stalls is cut after 10 seconds without progress, and
// heartbeats on the same connection are answered at once meanwhile.
func TestSlowReportUploadsAreBoundByProgress(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	computerID, stream := s.preparedUserComputer("laptop")
	upload := func(stall bool) (enginetest.Response, time.Duration, error) {
		use, response, err := s.admit(computerID, protocol.ToolViewImage, `{"path":"/Users/me/shop/shot.png"}`, disabledNetworkPolicy)
		if err != nil || response.GetAdmitted() == nil {
			t.Fatalf("view_image = %v, %v", response, err)
		}
		_ = use
		instruction, _ := stream.execute()
		data := append([]byte("\x89PNG\r\n\x1a\n"), bytes.Repeat([]byte{7}, protocol.ViewImageDecodedBytes-8)...)
		body := execution(stream.connectionID, "report_slow_"+instruction.Envelope.MessageID, instruction, "terminal", 1,
			`{"schema_version":1,"tool":"view_image","status":"success","truncated":false,"error":null,"result":{"mime":"image/png","size_bytes":`+
				fmt.Sprint(len(data))+`,"data_base64":"`+base64.StdEncoding.EncodeToString(data)+`"},"metrics":{"wall_time_ms":5,"queued_ms":0,"drive_wait_ms":0,"run_ms":5,"sync_ms":0}}`)
		reader, writer := io.Pipe()
		go func() {
			// About 1.2 MB a second: the 14 MB body takes about 12 seconds.
			const piece = 120_000
			for sent := 0; sent < len(body); sent += piece {
				if stall && sent >= len(body)/2 {
					return // nothing more is ever written
				}
				if _, err := writer.Write(body[sent:min(sent+piece, len(body))]); err != nil {
					return
				}
				time.Sleep(100 * time.Millisecond)
			}
			_ = writer.Close()
		}()
		t.Cleanup(func() { _ = writer.Close() })
		start := time.Now()
		answer, err := stream.cli.PostStream(f.ctx, "/v1/client/tool-status", reader, int64(len(body)))
		return answer, time.Since(start), err
	}
	var beats atomic.Int64
	done := make(chan struct{})
	go func() {
		for seq := int64(1); ; seq++ {
			select {
			case <-done:
				return
			case <-time.After(time.Second):
			}
			start := time.Now()
			response, err := stream.cli.Heartbeat(f.ctx, protocol.Heartbeat{ConnectionID: stream.connectionID, MessageID: fmt.Sprintf("beat_%d", seq),
				RequestMessageID: instance.HeartbeatMessageID(stream.connectionID, seq), HeartbeatSeq: seq})
			if _, ackErr := response.Grant(); err != nil || ackErr != nil || time.Since(start) > time.Second {
				t.Errorf("heartbeat %d = %d %s, %v after %s; want its ACK within a second", seq, response.Status, response.Body, err, time.Since(start))
			}
			beats.Add(1)
		}
	}()
	defer close(done)

	response, took, err := upload(false)
	if ack, ackErr := response.Acknowledgement(); err != nil || ackErr != nil || ack.ConnectionID != stream.connectionID || took < 10*time.Second {
		t.Fatalf("progressing upload = %d %s, %v after %s; want its ACK after more than 10 seconds", response.Status, response.Body, err, took)
	}
	before := f.count(`SELECT count(*) FROM session_tool_reports`)
	response, took, err = upload(true)
	if _, ackErr := response.Acknowledgement(); ackErr == nil || took < 10*time.Second || took > 20*time.Second {
		t.Fatalf("stalled upload = %d %s, %v after %s; want it cut, unacknowledged, 10 seconds after its last byte", response.Status, response.Body, err, took)
	}
	if after := f.count(`SELECT count(*) FROM session_tool_reports`); after != before {
		t.Fatal("the stalled upload stored a report")
	}
	if beats.Load() < 15 {
		t.Fatalf("%d heartbeats answered during the uploads; want one a second", beats.Load())
	}
}

// Reports that answer no instruction issued on their connection change
// nothing: an unissued request_message_id and another binding's get 409
// unknown_instruction, a report on a connection that is no longer live 409
// stale_connection, and its call keeps the settlement its connection's end
// gave it.
func TestReportsForOtherInstructionsChangeNothing(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	computerID, stream := s.preparedUserComputer("laptop")
	use, task := s.admitted(computerID, "make test")
	instruction, _ := stream.execute()
	terminal := execEnvelope("success", task, "ok")

	unissued := instruction
	unissued.Envelope.MessageID = "msg_0000000000000000"
	stream.refused(execution(stream.connectionID, "report_unissued", unissued, "terminal", 1, terminal), http.StatusConflict, protocol.ErrorUnknownInstruction)
	otherBinding := instruction
	payload := instruction.Envelope.Payload.(protocol.ExecutePayload)
	payload.BindingID = "bind_other"
	otherBinding.Envelope.Payload = payload
	stream.refused(execution(stream.connectionID, "report_binding", otherBinding, "terminal", 1, terminal), http.StatusConflict, protocol.ErrorUnknownInstruction)

	// The lease runs out and the CLI starts a new connection.
	old := stream.connectionID
	f.exec(`UPDATE instance_connections SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, old)
	stream.close()
	f.connect(stream.cli, everyTool)
	stream.refused(execution(old, "report_old", instruction, "terminal", 1, terminal), http.StatusConflict, protocol.ErrorStaleConnection)
	if n := f.count(`SELECT count(*) FROM session_tool_reports`); n != 0 {
		t.Fatalf("%d refused reports stored; want none", n)
	}
	s.requireUnknownOutcome(use, runtimecontrol.MessageComputerOfflineResultUnknown)
}

// A rejection settles its own call once, as rejected with its code and
// message, and leaves the connection and other tasks alone: a
// resource_exhausted rejection of a new call while another task runs settles
// the new call, is logged as tool.report_rejected, and the running task's
// terminal report still ends it; a heartbeat afterwards is accepted.
func TestRejectionsSettleTheirCallOnceAndKeepTheConnection(t *testing.T) {
	lines := &lockedBuffer{}
	t.Cleanup(workload.InstallTelemetry(workload.NewTelemetry(workload.TelemetryOptions{Service: "integration", Logger: workload.NewLogger(lines, "integration", "test", "unit")})))
	f := newMachineTools(t)
	s := f.session()
	computerID, stream := s.preparedUserComputer("laptop")
	_, running := s.admitted(computerID, "sleep 60")
	original, _ := stream.execute()
	stream.acked(execution(stream.connectionID, "report_running", original, "running", 1, execEnvelope("running", running, "")), "report_running")
	use, _ := s.admitted(computerID, "make test")
	refused, _ := stream.execute()
	stream.acked(rejection(stream.connectionID, "report_rejected", refused, string(protocol.RejectionResourceExhausted)), "report_rejected")
	if state, record := s.settlement(use); state != "terminal_unconsumed" || record["settlement"] != "rejected" ||
		record["code"] != "resource_exhausted" || record["message"] != "refused: resource_exhausted" {
		t.Fatalf("rejected call = %s %v; want rejected with the CLI's code and message", state, record)
	}
	stream.acked(rejection(stream.connectionID, "report_rejected_again", refused, string(protocol.RejectionResourceExhausted)), "report_rejected_again")
	if _, record := s.settlement(use); record["code"] != "resource_exhausted" {
		t.Fatalf("a second rejection changed the call: %v", record)
	}
	stream.acked(execution(stream.connectionID, "report_terminal", original, "terminal", 2, execEnvelope("success", running, "done")), "report_terminal")
	if status := f.row(`SELECT status FROM session_background_tasks WHERE task_id=$1`, running)["status"]; status != "completed" {
		t.Fatalf("the running task is %v; want completed by its own report", status)
	}
	if response, err := stream.cli.Heartbeat(f.ctx, protocol.Heartbeat{ConnectionID: stream.connectionID, MessageID: "beat_1",
		RequestMessageID: instance.HeartbeatMessageID(stream.connectionID, 1), HeartbeatSeq: 1}); err != nil || response.Status != http.StatusOK {
		t.Fatalf("heartbeat after the rejection = %d %s, %v; want the connection live", response.Status, response.Body, err)
	}
	var warned []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(lines.String()), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) == nil && record["event"] == "tool.report_rejected" {
			warned = append(warned, record)
		}
	}
	addr := addressOf(refused)
	if len(warned) != 1 || warned[0]["level"] != "WARN" || warned[0]["error.code"] != "resource_exhausted" || warned[0]["instance.id"] != stream.cli.InstanceID ||
		warned[0]["connection.id"] != stream.connectionID || warned[0]["session.id"] != s.id || warned[0]["task.id"] != addr.taskID {
		t.Fatalf("tool.report_rejected records = %v; want one for the rejection", warned)
	}
}

// An interrupt after the hub stored a call's running report, which the
// Runtime has not committed, sends one tool.cancel for its task and gives the
// call tool_outcome_unknown.
func TestAnInterruptAfterARunningReportCancelsItsTask(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	computerID, stream := s.preparedUserComputer("laptop")
	use, task := s.admitted(computerID, "sleep 100")
	instruction, _ := stream.execute()
	stream.acked(execution(stream.connectionID, "report_running", instruction, "running", 1, execEnvelope("running", task, "")), "report_running")
	results := s.interrupt()
	if cancel, ok := stream.next().Envelope.Payload.(protocol.CancelPayload); !ok || cancel.TaskID != task {
		t.Fatalf("the interrupt sent %v; want one tool.cancel of task %s", cancel, task)
	}
	stream.quiet(300 * time.Millisecond)
	requireInterruptError(t, results[use], "tool_outcome_unknown",
		"Tool execution may have completed, but its result did not commit before interruption: the call may have run; check effects before retrying.")
}

// On one connection, three sandbox_unavailable rejections of one Session and
// one of a second Session write one computer_sandbox_error in each Session;
// binding_unavailable writes none. Neither code is logged as
// tool.report_rejected.
func TestSandboxUnavailableWritesOneComputerErrorPerSessionAndConnection(t *testing.T) {
	lines := &lockedBuffer{}
	t.Cleanup(workload.InstallTelemetry(workload.NewTelemetry(workload.TelemetryOptions{Service: "integration", Logger: workload.NewLogger(lines, "integration", "test", "unit")})))
	f := newMachineTools(t)
	s := f.session()
	computerID, stream := s.preparedUserComputer("laptop")
	for i := range 3 {
		s.admitted(computerID, fmt.Sprintf("make test %d", i))
		instruction, _ := stream.execute()
		stream.acked(rejection(stream.connectionID, fmt.Sprintf("report_%d", i), instruction, string(protocol.RejectionSandboxUnavailable)), fmt.Sprintf("report_%d", i))
	}
	s.admitted(computerID, "make lint")
	instruction, _ := stream.execute()
	stream.acked(rejection(stream.connectionID, "report_binding", instruction, string(protocol.RejectionBindingUnavailable)), "report_binding")
	other := f.session()
	otherComputer := other.userComputer(stream.cli)
	f.ownerTx(func(tx *dbconnect.Tx) error {
		_, err := computer.IssuePreparation(f.ctx, tx, f.ws, other.id, otherComputer)
		return err
	})
	stream.prepare("")
	other.admitted(otherComputer, "make test")
	instruction, _ = stream.execute()
	stream.acked(rejection(stream.connectionID, "report_other", instruction, string(protocol.RejectionSandboxUnavailable)), "report_other")
	for _, session := range []struct{ id, computer string }{{s.id, computerID}, {other.id, otherComputer}} {
		errs := f.row(`SELECT count(*) AS n, min(payload_json::jsonb->'error'->>'computer_id') AS computer, min(payload_json::jsonb->'error'->>'message') AS message
			FROM session_events WHERE session_id=$1 AND type='session.error' AND payload_json::jsonb->'error'->>'type'='computer_sandbox_error'`, session.id)
		if errs["n"] != 1.0 || errs["computer"] != session.computer || errs["message"] != "refused: sandbox_unavailable" {
			t.Fatalf("computer_sandbox_error events of %s = %v; want one for its computer", session.id, errs)
		}
		if n := f.count(`SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.error'`, session.id); n != 1 {
			t.Fatalf("%d session errors in %s; want only the sandbox error", n, session.id)
		}
	}
	for _, line := range strings.Split(strings.TrimSpace(lines.String()), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) == nil && record["event"] == "tool.report_rejected" {
			t.Fatalf("tool.report_rejected logged for %v; want none", record["error.code"])
		}
	}
}

// Each drive a report lists as unmounted writes one drive_mount_error for the
// report's computer; the report sent again writes none.
func TestUnmountedDrivesWriteOneMountErrorEach(t *testing.T) {
	f := newMachineTools(t)
	s := f.session()
	computerID, stream := s.preparedUserComputer("laptop")
	_, task := s.admitted(computerID, "ls /drives")
	instruction, _ := stream.execute()
	envelope := strings.TrimSuffix(execEnvelope("success", task, "ok"), "}") +
		`,"drive_sync":{"committed":0,"deleted":0,"rejected":[],"not_synced":[],"unmounted":[{"drive_id":"drv_notes","error":"mount failed"},{"drive_id":"drv_data","error":"no space"}]}}`
	body := execution(stream.connectionID, "report_unmounted", instruction, "terminal", 1, envelope)
	stream.acked(body, "report_unmounted")
	stream.acked(body, "report_unmounted")
	rows, err := f.admin.QueryContext(f.ctx, `SELECT payload_json::jsonb->'error'->>'drive_id', payload_json::jsonb->'error'->>'computer_id', payload_json::jsonb->'error'->>'message'
		FROM session_events WHERE session_id=$1 AND payload_json::jsonb->'error'->>'type'='drive_mount_error' ORDER BY sequence`, s.id)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var got []string
	for rows.Next() {
		var drive, computer, message string
		if err := rows.Scan(&drive, &computer, &message); err != nil {
			t.Fatal(err)
		}
		got = append(got, drive+" "+computer+" "+message)
	}
	want := []string{"drv_notes " + computerID + " mount failed", "drv_data " + computerID + " no space"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("drive_mount_error events = %v; want %v", got, want)
	}
}

// AwaitToolCall has no deadline of its own and runs on any Bridge pod: a
// waiter on a pod other than the admitting one returns at its call's notice;
// after the LISTEN connections are killed, every waiter reads its call again,
// so one settled while no notice could reach it returns and a pending one
// keeps waiting; a waiter with no notice still waits after 60 seconds and
// returns at the next notice. An await of a call already settled returns its
// settlement at once, and one of a call the interrupt closeout consumed
// answers FailedPrecondition at once.
func TestAwaitToolCallEndsOnItsNoticeOnAnyBridgePod(t *testing.T) {
	f := newMachineTools(t)
	other := f.bridgePod("pod_machine_tools") // a second Bridge API server with its own result listener
	s := f.session()
	computerID, stream := s.preparedUserComputer("laptop")
	type awaited struct {
		response *bridgev1.AwaitToolCallResponse
		err      error
	}
	// await waits on the other Bridge pod, or on the admitting one through
	// the Runtime client when admitting is set.
	await := func(use string, admitting bool) <-chan awaited {
		done := make(chan awaited, 1)
		go func() {
			request := &bridgev1.AwaitToolCallRequest{Scope: s.scope, ToolUseEventId: use}
			var response *bridgev1.AwaitToolCallResponse
			var err error
			if admitting {
				response, err = f.runtime.AwaitToolCall(f.ctx, request)
			} else {
				response, err = other.AwaitToolCall(f.ctx, request)
			}
			done <- awaited{response, err}
		}()
		return done
	}
	blocked := func(done <-chan awaited, d time.Duration) {
		t.Helper()
		select {
		case got := <-done:
			t.Fatalf("AwaitToolCall returned %v, %v; want it waiting", got.response, got.err)
		case <-time.After(d):
		}
	}
	settled := func(done <-chan awaited, use string) {
		t.Helper()
		select {
		case got := <-done:
			_, record := s.settlement(use)
			var returned map[string]any
			if got.err != nil || json.Unmarshal([]byte(got.response.GetSettled().GetSettlementJson()), &returned) != nil || !reflect.DeepEqual(returned, record) {
				t.Fatalf("AwaitToolCall = %v, %v; want the stored settlement %v", got.response, got.err, record)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("AwaitToolCall did not return within 10 seconds")
		}
	}
	report := func(instruction enginetest.Event, task string) {
		stream.acked(execution(stream.connectionID, "report_"+task, instruction, "terminal", 1, execEnvelope("success", task, "ok")), "report_"+task)
	}

	// The test CLI sends no heartbeat; the lease outlasts the long wait.
	f.exec(`UPDATE instance_connections SET lease_expires_at=clock_timestamp()+interval '5 minutes' WHERE id=$1`, stream.connectionID)
	longUse, longTask := s.admitted(computerID, "sleep 600")
	longInstruction, _ := stream.execute()
	longStarted := time.Now()
	longWait := await(longUse, true)

	use, task := s.admitted(computerID, "make test")
	instruction, _ := stream.execute()
	wait := await(use, false)
	blocked(wait, 300*time.Millisecond)
	report(instruction, task)
	settled(wait, use)

	silentUse, _ := s.admitted(computerID, "make lint")
	stream.execute()
	silentWait := await(silentUse, false)
	blocked(silentWait, 300*time.Millisecond)
	record, err := runtimecontrol.NotExecutedSettlement("test_settled", "settled with no notice")
	if err != nil {
		t.Fatal(err)
	}
	f.exec(`UPDATE session_runtime_tool_results SET execution_state='terminal_unconsumed', result_json=$2 WHERE tool_use_event_id=$1`, silentUse, record)
	blocked(silentWait, time.Second)
	f.exec(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=current_database() AND query=$1`,
		"LISTEN "+resultnotify.ExecutionResultNotificationChannel)
	settled(silentWait, silentUse)
	blocked(longWait, 300*time.Millisecond)

	blocked(longWait, time.Until(longStarted.Add(61*time.Second)))
	report(longInstruction, longTask)
	settled(longWait, longUse)

	// No notice comes for a call that settled before its await.
	settled(await(use, false), use)
	s.interrupt()
	if state, _ := s.settlement(use); state != "consumed" {
		t.Fatalf("after the interrupt closeout the call is %s; want consumed", state)
	}
	select {
	case got := <-await(use, false):
		if status.Code(got.err) != codes.FailedPrecondition {
			t.Fatalf("AwaitToolCall of a consumed call = %v, %v; want FailedPrecondition", got.response, got.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("AwaitToolCall of a consumed call did not return within 10 seconds")
	}
}

// A terminal report's metrics become, in the trace of the call's admission,
// the CLI's span execute_tool exec_command, which ends when the report was
// received and lasts wall_time_ms, with queued, drive_wait, run and sync laid
// end to end from its start, and the settlement's span tool.settle; the
// report reaches a hub server other than the one that sent the instruction.
func TestToolReportSpansFollowTheReportedMetrics(t *testing.T) {
	spans := tracetest.NewInMemoryExporter()
	t.Cleanup(workload.InstallTelemetry(workload.NewTelemetry(workload.TelemetryOptions{Service: "integration", SyncExporter: spans, SampleRatio: 1})))
	f := newMachineTools(t)
	s := f.session()
	computerID, stream := s.preparedUserComputer("laptop")
	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	turn := metadata.AppendToOutgoingContext(f.ctx, "traceparent", "00-"+traceID+"-00f067aa0ba902b7-01")
	use := s.use(protocol.ToolExecCommand, "machine_tool")
	response, err := f.runtime.AdmitToolCall(turn, &bridgev1.AdmitToolCallRequest{Scope: s.scope, ToolUseEventId: use, ComputerId: computerID,
		Tool: protocol.ToolExecCommand, InputJson: `{"cmd":"make test"}`, SandboxPolicyJson: disabledNetworkPolicy})
	if err != nil || response.GetAdmitted() == nil {
		t.Fatalf("admission = %v, %v", response, err)
	}
	task := response.GetAdmitted().GetTaskId()
	instruction, _ := stream.execute()
	parent := f.row(`SELECT trace_context FROM session_tool_dispatches WHERE message_id=$1`, instruction.Envelope.MessageID)["trace_context"].(string)

	other := *stream
	other.cli = f.cli(1, stream.cli.InstanceID, stream.cli.CredentialID)
	before := time.Now()
	other.acked(execution(stream.connectionID, "report_metrics", instruction, "terminal", 1, execEnvelope("success", task, "ok")), "report_metrics")
	after := time.Now()

	byName := map[string]tracetest.SpanStub{}
	for _, span := range spans.GetSpans() {
		if span.SpanContext.TraceID().String() == traceID {
			byName[span.Name] = span
		}
	}
	tool, ok := byName["execute_tool exec_command"]
	if !ok || tool.Parent.SpanID().String() != parent[36:52] {
		t.Fatalf("execute_tool span %v; want a child of the dispatch record's span %s", tool.Parent, parent)
	}
	if tool.EndTime.Before(before) || tool.EndTime.After(after) || tool.EndTime.Sub(tool.StartTime) != 1250*time.Millisecond {
		t.Fatalf("execute_tool spans %s to %s; want 1,250 ms ending between %s and %s", tool.StartTime, tool.EndTime, before, after)
	}
	for _, child := range []struct {
		name           string
		offset, length time.Duration
	}{{"queued", 0, 3}, {"drive_wait", 3, 40}, {"run", 43, 1180}, {"sync", 1223, 20}} {
		span, ok := byName[child.name]
		if !ok || span.Parent.SpanID() != tool.SpanContext.SpanID() ||
			span.StartTime.Sub(tool.StartTime) != child.offset*time.Millisecond || span.EndTime.Sub(span.StartTime) != child.length*time.Millisecond {
			t.Fatalf("%s span %v; want %d ms from %d ms after execute_tool's start", child.name, span, child.length, child.offset)
		}
	}
	if _, ok := byName["tool.settle"]; !ok {
		t.Fatal("no tool.settle span in the admission's trace")
	}
}

// Reports of the tool execution scenarios are stored once by their identity
// and settle each call once: one result notice per call, one stored report
// per message_id.
func TestScenarioReportsStoreOnceAndSettleOnce(t *testing.T) {
	type scenario struct {
		name string
		// run posts the scenario's reports and returns the notices of the calls
		// they settle.
		run func(t *testing.T, s *toolSession, computerID string, stream *cliStream) []resultnotify.ExecutionResultHint
	}
	terminal := func(stream *cliStream, messageID string, instruction enginetest.Event, seq int) {
		a := addressOf(instruction)
		stream.acked(execution(stream.connectionID, messageID, instruction, "terminal", seq, execEnvelope("success", a.taskID, "ok")), messageID)
	}
	for _, sc := range []scenario{
		{"concurrent copies of one report", func(t *testing.T, s *toolSession, computerID string, stream *cliStream) []resultnotify.ExecutionResultHint {
			use, _ := s.admitted(computerID, "make test")
			instruction, _ := stream.execute()
			body := execution(stream.connectionID, "report_copy", instruction, "terminal", 1, execEnvelope("success", addressOf(instruction).taskID, "ok"))
			responses, errs := make([]enginetest.Response, 4), make([]error, 4)
			var wg sync.WaitGroup
			for i := range responses {
				wg.Go(func() { responses[i], errs[i] = stream.cli.Post(s.f.ctx, "/v1/client/tool-status", body) })
			}
			wg.Wait()
			for i, response := range responses {
				if ack, err := response.Acknowledgement(); errs[i] != nil || err != nil || ack.MessageID != "report_copy" {
					t.Fatalf("copy %d = %d %s, %v; want the report's ACK", i, response.Status, response.Body, errs[i])
				}
			}
			return s.hints(use)
		}},
		{"a task_conflict rejection sent twice", func(t *testing.T, s *toolSession, computerID string, stream *cliStream) []resultnotify.ExecutionResultHint {
			use, _ := s.admitted(computerID, "make test")
			instruction, _ := stream.execute()
			for range 2 {
				stream.acked(rejection(stream.connectionID, "report_conflict", instruction, string(protocol.RejectionTaskConflict)), "report_conflict")
			}
			return s.hints(use)
		}},
		{"polls and a lost stdin reply sent again", func(t *testing.T, s *toolSession, computerID string, stream *cliStream) []resultnotify.ExecutionResultHint {
			use, task := s.admitted(computerID, "cat")
			execute, _ := stream.execute()
			stream.acked(execution(stream.connectionID, "report_running", execute, "running", 1, execEnvelope("running", task, "")), "report_running")
			uses := []string{use}
			for i := range 2 {
				poll, response, err := s.control(task, "", nil, nil)
				if err != nil || response.GetAdmitted() == nil {
					t.Fatalf("write_stdin = %v, %v", response, err)
				}
				stdin := stream.next()
				reply := reportJSON(stream.connectionID, fmt.Sprintf("report_reply_%d", i), stdin, `"kind":"control","result":`+
					strings.Replace(execEnvelope("running", task, fmt.Sprint("window ", i)), `"tool":"exec_command"`, `"tool":"stdin"`, 1))
				stream.acked(reply, fmt.Sprintf("report_reply_%d", i))
				stream.acked(reply, fmt.Sprintf("report_reply_%d", i))
				uses = append(uses, poll)
			}
			return s.hints(uses...)
		}},
		{"a partial write, and a write cancelled before its reply", func(t *testing.T, s *toolSession, computerID string, stream *cliStream) []resultnotify.ExecutionResultHint {
			use, task := s.admitted(computerID, "cat")
			execute, _ := stream.execute()
			stream.acked(execution(stream.connectionID, "report_running", execute, "running", 1, execEnvelope("running", task, "")), "report_running")
			stdinResult := strings.Replace(execEnvelope("running", task, ""), `"tool":"exec_command"`, `"tool":"stdin"`, 1)
			stdinReply := func(messageID string, instruction enginetest.Event, extra string) {
				stream.acked(reportJSON(stream.connectionID, messageID, instruction, `"kind":"control","result":`+stdinResult+extra), messageID)
			}
			write, response, err := s.control(task, "abc", nil, nil)
			if err != nil || response.GetAdmitted() == nil {
				t.Fatalf("write_stdin = %v, %v", response, err)
			}
			stdinReply("report_partial", stream.next(), `,"error":{"code":"stdin_closed","message":"stdin closed after 2 bytes"}`)
			var want map[string]any
			if err := json.Unmarshal([]byte(`{"settlement":"reply","result":`+stdinResult+
				`,"error":{"code":"stdin_closed","message":"stdin closed after 2 bytes"}}`), &want); err != nil {
				t.Fatal(err)
			}
			if _, record := s.settlement(write); !reflect.DeepEqual(record, want) {
				t.Fatalf("partial write settled %v; want its reply with the result and the reply's error %v", record, want)
			}
			late, response, err := s.control(task, "def", nil, nil)
			if err != nil || response.GetAdmitted() == nil {
				t.Fatalf("write_stdin = %v, %v", response, err)
			}
			lateStdin := stream.next()
			if _, err := s.f.runtime.CancelToolCall(s.f.ctx, &bridgev1.CancelToolCallRequest{Scope: s.scope, ToolUseEventId: late}); err != nil {
				t.Fatal(err)
			}
			stdinReply("report_late", lateStdin, "")
			s.requireUnknownOutcome(late, runtimecontrol.MessageOutcomeUnknownTurnEnd)
			if task := s.f.row(`SELECT status FROM session_background_tasks WHERE task_id=$1`, task); task["status"] != "running" {
				t.Fatalf("task after a closed stdin = %v; want running until its terminal report", task)
			}
			terminal(stream, "report_terminal", execute, 2)
			return s.hints(use, write, late)
		}},
		{"a terminal report after a turn-end cancel settled the call", func(t *testing.T, s *toolSession, computerID string, stream *cliStream) []resultnotify.ExecutionResultHint {
			use, _ := s.admitted(computerID, "make test")
			execute, _ := stream.execute()
			if _, err := s.f.runtime.CancelToolCall(s.f.ctx, &bridgev1.CancelToolCallRequest{Scope: s.scope, ToolUseEventId: use}); err != nil {
				t.Fatal(err)
			}
			stream.next() // tool.cancel
			terminal(stream, "report_terminal", execute, 1)
			return s.hints(use)
		}},
		{"a report after a stream-only resume", func(t *testing.T, s *toolSession, computerID string, stream *cliStream) []resultnotify.ExecutionResultHint {
			use, _ := s.admitted(computerID, "make test")
			execute, _ := stream.execute()
			stream.close()
			resumed := s.f.resume(1, stream.cli, stream.connectionID)
			resent, _ := resumed.execute()
			if resent.Envelope.MessageID != execute.Envelope.MessageID {
				t.Fatalf("resent %s; want %s", resent.Envelope.MessageID, execute.Envelope.MessageID)
			}
			terminal(resumed, "report_terminal", execute, 1)
			return s.hints(use)
		}},
		{"a cwd_not_found result, then a new call", func(t *testing.T, s *toolSession, computerID string, stream *cliStream) []resultnotify.ExecutionResultHint {
			use, _ := s.admitted(computerID, "make test")
			execute, _ := stream.execute()
			missing := `{"schema_version":1,"tool":"exec_command","status":"error","truncated":false,` +
				`"error":{"kind":"cwd_not_found","message":"the working directory does not exist","path":"/Users/me/shop"},"result":{},` +
				`"metrics":{"wall_time_ms":2,"queued_ms":0,"drive_wait_ms":0,"run_ms":0,"sync_ms":0}}`
			stream.acked(execution(stream.connectionID, "report_missing", execute, "terminal", 1, missing), "report_missing")
			if _, record := s.settlement(use); record["settlement"] != "report" || !strings.Contains(fmt.Sprint(record["result"]), "cwd_not_found") {
				t.Fatalf("call = %v; want settled by the cwd_not_found result", record)
			}
			next, _ := s.admitted(computerID, "make test")
			again, _ := stream.execute()
			if again.Envelope.MessageID == execute.Envelope.MessageID {
				t.Fatal("the new call replayed the failed instruction")
			}
			terminal(stream, "report_next", again, 1)
			return s.hints(use, next)
		}},
		{"two Sessions on one instance", func(t *testing.T, s *toolSession, computerID string, stream *cliStream) []resultnotify.ExecutionResultHint {
			use, _ := s.admitted(computerID, "make test")
			first, _ := stream.execute()
			other := s.f.session()
			otherComputer := other.userComputer(stream.cli)
			s.f.ownerTx(func(tx *dbconnect.Tx) error {
				_, err := computer.IssuePreparation(s.f.ctx, tx, s.f.ws, other.id, otherComputer)
				return err
			})
			stream.prepare("")
			otherUse, _ := other.admitted(otherComputer, "make test")
			second, _ := stream.execute()
			terminal(stream, "report_second", second, 1)
			if state, _ := s.settlement(use); state != "pending" {
				t.Fatalf("the other Session's report settled this call: %s", state)
			}
			terminal(stream, "report_first", first, 1)
			return append(s.hints(use), other.hints(otherUse)...)
		}},
	} {
		t.Run(sc.name, func(t *testing.T) {
			f := newMachineTools(t)
			s := f.session()
			notices := f.listenForResults()
			computerID, stream := s.preparedUserComputer("laptop")
			for _, hint := range sc.run(t, s, computerID, stream) {
				notices.require(t, hint, 1)
			}
			if stored, distinct := f.count(`SELECT count(*) FROM session_tool_reports`), f.count(`SELECT count(DISTINCT message_id) FROM session_tool_reports`); stored != distinct {
				t.Fatalf("%d stored reports for %d identities", stored, distinct)
			}
		})
	}
}
