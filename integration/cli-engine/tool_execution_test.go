package joint

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/protocol"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// TestToolExecution runs the tool execution scenarios of admission and the
// replay ledger on one Engine with three computer-hub pods.
func TestToolExecution(t *testing.T) {
	scenarioTable(t, engineOptions{hubPods: 3}, needs{},
		tableRow("policy_admission", policiesAreCheckedBeforeAnyEffect).only("linux"),
		tableRow("full_replay_ledger", aFullReplayLedgerRefusesOnlyNewCalls),
	)
}

// policiesAreCheckedBeforeAnyEffect calls a machine whose platform
// isolation is unavailable. Admission refuses a call with no policy, one with
// an unknown field and one of an unsupported version, and ends one whose
// network mode the machine cannot enforce, none reaching the machine. The
// CLI rejects the calls it gets with sandbox_unavailable, and the Session
// gets one computer_sandbox_error per connection.
func policiesAreCheckedBeforeAnyEffect(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	record.boundary("Linux only: isolation is made unavailable by an App Server PATH with no bwrap; macOS has no unprivileged way to take sandbox-exec away")
	record.boundary("the CLI's own rejection of a missing, unknown-field, unsupported-version or unenforceable policy is not reached: Engine refuses or ends those calls before it sends any tool.execute")
	m := unisolatedMachine(t, e, set, "unisolated")
	session, computers, rs := driveSession(t, e, "policies", nil, m)
	ctx := t.Context()
	for name, policy := range map[string]string{
		"missing":     "",
		"unknown":     `{"schema_version":1,"filesystem":{"rules":[]},"network":{"mode":"disabled"},"mode":"workspace"}`,
		"unsupported": `{"schema_version":2,"filesystem":{"rules":[]},"network":{"mode":"disabled"}}`,
	} {
		use := rs.use(protocol.ToolExecCommand, `{"cmd":"true"}`, "machine_tool")
		_, err := rs.r.client.AdmitToolCall(ctx, &bridgev1.AdmitToolCallRequest{Scope: rs.scope, ToolUseEventId: use, ComputerId: computers[0],
			Tool: protocol.ToolExecCommand, InputJson: `{"cmd":"true"}`, SandboxPolicyJson: policy})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("admission with the %s policy answered %v; want InvalidArgument", name, err)
		}
	}
	managed := `{"schema_version":1,"filesystem":{"rules":[]},"network":{"mode":"managed","rules":{"allow":[{"destination":{"kind":"host","value":"example.com"},"transport":"tcp","ports":[{"from":443,"to":443}]}],"deny":[],"listen":[]}}}`
	use, response := rs.admit(computers[0], protocol.ToolExecCommand, `{"cmd":"true"}`, managed)
	unenforceable := rs.await(use, time.Minute)
	if response.GetAdmitted() == nil || unenforceable.Settlement == "report" {
		t.Fatalf("the call with a managed network answered %v and settled %+v; want admitted and ended without a report", response, unenforceable)
	}
	if n := toolInstructions(t, e, session.ID); n != 0 {
		t.Fatalf("%d tool.execute instructions were written; want none", n)
	}
	record.observe("missing, unknown-field and unsupported-version policies were refused at admission; the managed-network call settled %s %s %q; no tool.execute was written", unenforceable.Settlement, unenforceable.Code, unenforceable.Message)

	rs.requireRejected(rs.exec(computers[0], "true"), "sandbox_unavailable")
	rs.requireRejected(rs.exec(computers[0], "true"), "sandbox_unavailable")
	if errs := errorsOfType(e, session.ID, "computer_sandbox_error"); len(errs) != 1 {
		t.Fatalf("%d computer_sandbox_error events on the first connection; want 1", len(errs))
	}
	developer := e.developer()
	first := developer.waitComputer(ctx, session.ID, computers[0], time.Minute, "ready", func(c computerObject) bool { return c.ExecutionState == "ready" })
	m.server.stop()
	m.waitOffline(e)
	m.server = m.serveUnisolated()
	developer.waitComputer(ctx, session.ID, computers[0], 2*time.Minute, "ready on the new connection", func(c computerObject) bool {
		return c.ExecutionState == "ready" && c.BindingID != nil && *c.BindingID != *first.BindingID
	})
	rs.requireRejected(rs.exec(computers[0], "true"), "sandbox_unavailable")
	if errs := errorsOfType(e, session.ID, "computer_sandbox_error"); len(errs) != 2 {
		t.Fatalf("%d computer_sandbox_error events after a second connection; want 2", len(errs))
	}
	record.observe("two sandbox_unavailable rejections on the first connection wrote one computer_sandbox_error; one on the next connection wrote the second")
}

// toolInstructions counts the tool.execute instructions written for the
// Session's bindings.
func toolInstructions(t *testing.T, e *engine, sessionID string) int {
	t.Helper()
	var n int
	e.ownerTx(func(ctx context.Context, tx *dbconnect.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM connection_instructions i JOIN session_tool_dispatches d ON d.workspace_id=i.workspace_id AND d.message_id=i.message_id
			WHERE i.workspace_id=$1 AND d.session_id=$2 AND i.type='tool.execute'`, harnessWorkspace, sessionID).Scan(&n)
	})
	return n
}

// toolReportOf reads the tool use and phase of a tool-status report from r's
// body and puts the body back; a request that is no tool-status report gives
// empty strings.
func toolReportOf(r *http.Request) (use, phase string) {
	if r.Method != http.MethodPost || r.URL.Path != "/v1/client/tool-status" {
		return "", ""
	}
	body, err := io.ReadAll(r.Body)
	r.Body = io.NopCloser(bytes.NewReader(body))
	var report struct {
		ToolUseEventID string `json:"tool_use_event_id"`
		Phase          string `json:"phase"`
	}
	if err != nil || json.Unmarshal(body, &report) != nil {
		return "", ""
	}
	return report.ToolUseEventID, report.Phase
}

// reportHeader carries a tool-status report's tool use and phase from the
// edge's rule to the report's answer.
const reportHeader = "X-Harness-Report"

// toolReportAnswer is one answer the edge passed on to a tool-status report.
type toolReportAnswer struct {
	phase  string
	status int
	ack    string
}

// aFullReplayLedgerRefusesOnlyNewCalls fills the replay ledger of a
// machine's connection to its 4,032 places for new operations (4,096 entries
// less the 64 kept in reserve): a command that runs until released, 4,030
// commands whose terminal reports the edge refuses with 503, so that their
// entries stay, and a command that runs until cancelled. The next
// tool.execute is not executed, with resource_exhausted, and settles once
// although the edge loses the answer to its rejection; the connection and
// the binding stay. The cancel goes through; after the stream resumes,
// Engine sends every unreported instruction again and none runs twice; the
// held command reports its outcome; the ledger takes exactly two more calls,
// so the rejection left no entry, and the close of a second Session's
// computer on the same connection goes through. Once the edge passes the
// reports on and Engine acknowledges them, a new Session on the machine runs
// a command.
func aFullReplayLedgerRefusesOnlyNewCalls(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	ctx := t.Context()
	developer := e.developer()
	m := onlineUserMachine(t, e, set, "machine")
	session, computers, rs := driveSession(t, e, "ledger", nil, m)
	other, otherComputers, _ := driveSession(t, e, "ledger-close", nil, m)
	computer, work := computers[0], m.directory("ledger")
	connection := controllerConnection(t, e, m.instanceID)
	binding := *developer.readSessionComputer(ctx, session.ID, computer).BindingID
	execInput := func(cmd string, waitMS int) string {
		return mustJSON(t, map[string]any{"cmd": cmd, "wait_ms": waitMS, "on_wait_expiry": "detach"})
	}

	var mu sync.Mutex
	// fillers holds each filler's answers; watched those of the other calls.
	fillers, watched := map[string][]toolReportAnswer{}, map[string][]toolReportAnswer{}
	var holding atomic.Bool
	holding.Store(true)
	var probeUse atomic.Value
	probeUse.Store("")
	var lost atomic.Bool
	e.edge.addRule(&edgeRule{
		match: func(r *http.Request) bool {
			use, _ := toolReportOf(r)
			mu.Lock()
			defer mu.Unlock()
			_, filler := fillers[use]
			_, watch := watched[use]
			return filler || watch
		},
		before: func(w http.ResponseWriter, r *http.Request) bool {
			use, phase := toolReportOf(r)
			mu.Lock()
			defer mu.Unlock()
			if _, filler := fillers[use]; filler && holding.Load() {
				fillers[use] = append(fillers[use], toolReportAnswer{phase: phase, status: http.StatusServiceUnavailable})
				w.WriteHeader(http.StatusServiceUnavailable)
				return true
			}
			// The proxied request's body is spent when its answer comes.
			r.Header.Set(reportHeader, use+" "+phase)
			return false
		},
		after: func(response *http.Response) error {
			body, err := io.ReadAll(response.Body)
			if err != nil {
				return err
			}
			_ = response.Body.Close()
			response.Body = io.NopCloser(bytes.NewReader(body))
			use, phase, _ := strings.Cut(response.Request.Header.Get(reportHeader), " ")
			mu.Lock()
			defer mu.Unlock()
			answer := toolReportAnswer{phase: phase, status: response.StatusCode, ack: string(body)}
			if _, filler := fillers[use]; filler {
				fillers[use] = append(fillers[use], answer)
				return nil
			}
			watched[use] = append(watched[use], answer)
			if use == probeUse.Load() && !lost.Swap(true) {
				return errLostAnswer
			}
			return nil
		},
	})
	answersOf := func(use string) []toolReportAnswer {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(watched[use])
	}
	admitWatched := func(cmd string, waitMS int) toolCall {
		t.Helper()
		input := execInput(cmd, waitMS)
		use := rs.use(protocol.ToolExecCommand, input, "machine_tool")
		mu.Lock()
		watched[use] = nil
		mu.Unlock()
		response := rs.admitUse(use, computer, protocol.ToolExecCommand, input, scenarioPolicy)
		if response.GetAdmitted() == nil {
			t.Fatalf("admission of %s = %v; want admitted", cmd, response)
		}
		record.command("test Runtime admitted exec_command %q as tool use %s", cmd, use)
		return toolCall{use: use, taskID: response.GetAdmitted().GetTaskId()}
	}
	// tryFiller admits a command that appends n to fill.log and returns its
	// tool use; the edge refuses its reports while holding. The call returns
	// at the command's exit, so its long wait only keeps the first report
	// terminal on a host slow to start the command.
	tryFiller := func(n int) (string, error) {
		input := execInput(fmt.Sprintf("echo %d >> fill.log", n), 10_000)
		use, err := rs.declare(protocol.ToolExecCommand, input, "machine_tool")
		if err != nil {
			return "", err
		}
		mu.Lock()
		fillers[use] = nil
		mu.Unlock()
		response, err := rs.tryAdmit(use, computer, protocol.ToolExecCommand, input, scenarioPolicy)
		if err == nil && response.GetAdmitted() == nil {
			err = fmt.Errorf("admission of filler %d = %v; want admitted", n, response)
		}
		return use, err
	}
	addFiller := func(n int) string {
		t.Helper()
		use, err := tryFiller(n)
		if err != nil {
			t.Fatal(err)
		}
		return use
	}
	// firstPhase waits until the edge refused a report of the filler use and
	// returns that report's phase.
	firstPhase := func(use string, timeout time.Duration) string {
		t.Helper()
		for deadline := time.Now().Add(timeout); ; time.Sleep(50 * time.Millisecond) {
			mu.Lock()
			answers := slices.Clone(fillers[use])
			mu.Unlock()
			if len(answers) > 0 {
				return answers[0].phase
			}
			if time.Now().After(deadline) {
				t.Fatalf("no report of filler %s reached the edge within %s", use, timeout)
			}
		}
	}
	lines := func(file string) int {
		t.Helper()
		code, out := m.shell("sh", "-c", `wc -l < "$1"`, "sh", work+"/"+file)
		n, err := strconv.Atoi(strings.TrimSpace(out))
		if code != 0 || err != nil {
			t.Fatalf("counting the lines of %s exited %d: %s", file, code, out)
		}
		return n
	}
	requireStayed := func(when string) {
		t.Helper()
		object := developer.readSessionComputer(ctx, session.ID, computer)
		if now := controllerConnection(t, e, m.instanceID); now != connection || object.BindingID == nil || *object.BindingID != binding || object.ExecutionState != "ready" {
			t.Fatalf("%s: connection %s, computer %+v; want connection %s and binding %s ready", when, now, object, connection, binding)
		}
	}

	held := admitWatched(`echo started >> held.starts; while [ ! -e held.release ]; do sleep 0.1; done; echo finished`, 250)
	if settled := rs.await(held.use, time.Minute); settled.Settlement != "report" || settled.Phase != "running" {
		t.Fatalf("the held command settled %+v; want a running report", settled)
	}
	const fill = 4_030
	uses := make([]string, fill)
	errs := make([]error, fill)
	started := time.Now()
	var workers sync.WaitGroup
	next := atomic.Int64{}
	for range 8 {
		workers.Go(func() {
			for n := int(next.Add(1) - 1); n < fill; n = int(next.Add(1) - 1) {
				uses[n], errs[n] = tryFiller(n)
			}
		})
	}
	workers.Wait()
	if err := errors.Join(errs...); err != nil {
		t.Fatal(err)
	}
	record.command("test Runtime admitted %d exec_command calls from 8 workers, each appending its number to fill.log", fill)
	for _, use := range uses {
		if phase := firstPhase(use, 5*time.Minute); phase != "terminal" {
			t.Fatalf("the first report of filler %s is %s; want terminal", use, phase)
		}
	}
	record.observe("the %d fillers ran and the edge refused each terminal report with 503, %s after the first admission", fill, time.Since(started).Round(time.Second))

	cancelled := admitWatched(`echo $$ > cancelled.pid; exec sleep 600`, 50000)
	for deadline := time.Now().Add(time.Minute); ; time.Sleep(100 * time.Millisecond) {
		if code, _ := m.shell("test", "-s", work+"/cancelled.pid"); code == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the command to cancel did not start within a minute")
		}
	}
	probeInput := execInput("touch probe.ran", 250)
	use := rs.use(protocol.ToolExecCommand, probeInput, "machine_tool")
	mu.Lock()
	watched[use] = nil
	mu.Unlock()
	probeUse.Store(use)
	response := rs.admitUse(use, computer, protocol.ToolExecCommand, probeInput, scenarioPolicy)
	if response.GetAdmitted() == nil {
		t.Fatalf("admission of the probe = %v; want admitted by Engine", response)
	}
	probe := toolCall{use: use, taskID: response.GetAdmitted().GetTaskId()}
	settled := rs.requireRejected(probe, "resource_exhausted")
	var answers []toolReportAnswer
	for deadline := time.Now().Add(time.Minute); len(answers) < 2; time.Sleep(100 * time.Millisecond) {
		if answers = answersOf(probe.use); time.Now().After(deadline) {
			t.Fatalf("the probe's report answers %+v; want the lost answer and a second one within a minute", answers)
		}
	}
	if len(answers) != 2 || answers[0].phase != "rejected" || answers[0].status != http.StatusOK || answers[1] != answers[0] {
		t.Fatalf("the probe's report answers %+v; want one rejected report answered twice with the same ACK", answers)
	}
	if code, _ := m.shell("test", "-e", work+"/probe.ran"); code == 0 {
		t.Fatal("the rejected probe ran")
	}
	requireStayed("after the rejection")
	record.observe("with %d calls recorded, the next call %s settled once, rejected with resource_exhausted (%q), although the edge lost the answer to its first report; the CLI sent the report again and got the same ACK; the probe did not run; connection %s and binding %s stayed", fill+2, probe.use, settled.Message, connection, binding)

	rs.cancel(cancelled)
	_, pid := m.shell("cat", work+"/cancelled.pid")
	for deadline := time.Now().Add(time.Minute); ; time.Sleep(100 * time.Millisecond) {
		reports := answersOf(cancelled.use)
		code, _ := m.shell("kill", "-0", pid)
		if code != 0 && slices.ContainsFunc(reports, func(a toolReportAnswer) bool { return a.phase == "terminal" && a.status == http.StatusOK }) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after the cancel, process %s alive %v and reports %+v; want it ended and a terminal report accepted", pid, code == 0, reports)
		}
	}
	record.observe("the cancel of %s ended its process %s and its terminal report was accepted", cancelled.use, pid)

	requests := e.edge.eventsRequests(m.instanceID)
	e.killHub(requests[len(requests)-1].backend)
	e.edge.waitEvents(t, m.instanceID, len(requests)+1, time.Minute, "the resumed stream", func(r eventsRequest) bool { return r.status == http.StatusOK })
	resent := func(use string) int {
		n := 0
		for _, event := range strings.Split(e.edge.streamed(m.instanceID), "\n\n") {
			if strings.Contains(event, `"tool.execute"`) && strings.Contains(event, use) {
				n++
			}
		}
		return n
	}
	for deadline := time.Now().Add(2 * time.Minute); resent(held.use) < 2 || resent(uses[fill-1]) < 2; time.Sleep(time.Second) {
		if time.Now().After(deadline) {
			t.Fatalf("after the resume the stream carried the held command %d times and the last filler %d times; want both sent again", resent(held.use), resent(uses[fill-1]))
		}
	}
	time.Sleep(2 * time.Second)
	if starts, filled := lines("held.starts"), lines("fill.log"); starts != 1 || filled != fill {
		t.Fatalf("after the resume held.starts has %d lines and fill.log %d; want 1 and %d", starts, filled, fill)
	}
	requireStayed("after the resume")
	record.observe("the stream resumed on another hub pod; Engine sent the held command and the fillers again, and none ran twice: held.starts has 1 line and fill.log %d", fill)

	if code, _ := m.shell("touch", work+"/held.release"); code != 0 {
		t.Fatal("releasing the held command failed")
	}
	if envelope := rs.finished(held, 2*time.Minute); envelope.Result.ExitCode == nil || *envelope.Result.ExitCode != 0 {
		t.Fatalf("the held command ended %+v; want exit 0", envelope)
	}
	// The ledger drops the held command's entry when the ACK of its terminal
	// report arrives, which the edge passes on just after Engine stores it.
	for deadline := time.Now().Add(time.Minute); !slices.ContainsFunc(answersOf(held.use), func(a toolReportAnswer) bool { return a.phase == "terminal" && a.status == http.StatusOK }); time.Sleep(100 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("the held command's reports %+v; want a terminal one accepted", answersOf(held.use))
		}
	}
	time.Sleep(time.Second)
	extra := 0
	for ; ; extra++ {
		use := addFiller(fill + extra)
		uses = append(uses, use)
		if phase := firstPhase(use, time.Minute); phase == "rejected" {
			break
		} else if phase != "terminal" || extra > 2 {
			t.Fatalf("extra filler %d's first report is %s; want the ledger full after two", extra, phase)
		}
	}
	if extra != 2 {
		t.Fatalf("the ledger took %d more calls after the held and the cancelled command ended; want 2", extra)
	}
	record.observe("the held command ended with exit 0; the ledger then took exactly %d more calls and rejected the next", extra)
	// The held command's end left its task notification pending, and a
	// Session with pending input refuses to remove a computer.
	if status, object := developer.removeComputer(ctx, other.ID, otherComputers[0]); status != http.StatusAccepted {
		t.Fatalf("removing computer %s answered %d %+v; want 202", otherComputers[0], status, object)
	}
	developer.waitClosed(ctx, other.ID, otherComputers[0], 2*time.Minute)
	requireStayed("after the close")
	record.observe("with the ledger full, computer %s of Session %s on the same connection closed; binding %s stayed", otherComputers[0], other.ID, binding)

	holding.Store(false)
	for deadline := time.Now().Add(3 * time.Minute); ; time.Sleep(time.Second) {
		mu.Lock()
		waiting := 0
		for _, use := range uses {
			if !slices.ContainsFunc(fillers[use], func(a toolReportAnswer) bool { return a.status == http.StatusOK }) {
				waiting++
			}
		}
		mu.Unlock()
		if waiting == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of the %d fillers' reports had no 200 answer 3 minutes after the edge passed them on", waiting, len(uses))
		}
	}
	after, afterComputers, afterRS := driveSession(t, e, "after", nil, m)
	afterRS.run(afterComputers[0], "true", time.Minute)
	record.observe("once the edge passed the fillers' reports on, Engine acknowledged all %d; Session %s then prepared on the machine and ran a command", len(uses), after.ID)
}
