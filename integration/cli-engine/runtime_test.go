package joint

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/tetral-ai/tetral/integration/enginetest"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/sessionrpc"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// testRuntime stands in for one Runtime pod. Bridge's API runs as its own
// process; the Runtime client runs in the test process, takes Session
// bindings through Job Runner's delivery store as the table role of Job
// Runner, and calls Bridge with a token of the Runtime's service account in
// its pod, as the Runtime does before and after each machine-bound call.
type testRuntime struct {
	e      *engine
	client *enginetest.Runtime
	// sessions numbers the bound Sessions; the rows of a table test share it.
	sessions *atomic.Int64
}

// startRuntime starts Bridge's API and the Runtime client on the first call
// and returns the same Runtime on every later one.
func (e *engine) startRuntime() *testRuntime {
	t := e.t
	t.Helper()
	if e.runtime != nil {
		return e.runtime
	}
	addresses := reserveAddresses(t, 3)
	// Bridge requires a remote MCP connector address and a gateway token;
	// no test lists remote MCP tools, so nothing serves the address.
	e.startService("bridge", "bridge-api", "bridge-api", merge(e.databaseEnv("bridge"), e.blobEnv, map[string]string{
		"TETRAL_BRIDGE_API_HTTP_ADDR":              addresses[0],
		"TETRAL_BRIDGE_API_GRPC_ADDR":              addresses[1],
		"TETRAL_BRIDGE_MCP_CONNECTOR_GRPC_ADDR":    addresses[2],
		"TETRAL_BRIDGE_GATEWAY_TOKEN_PATH":         e.identity.issue(t, "gateway", "bridge-gateway"),
		"TETRAL_HTTP_TRANSPORT":                    "plaintext",
		"TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY":    randomHex(t, 32),
		"TETRAL_INTERNAL_GRPC_AUDIENCE":            internalAudience,
		"TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS": runtimeNamespace + "/" + runtimeAccount,
	}), addresses[0])

	pod := enginetest.RuntimePod{Namespace: runtimeNamespace, Name: "agent-runtime-0", UID: "pod-agent-runtime-0"}
	token := e.identity.issueRuntime(t, pod.UID)
	bearer := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token), method, req, reply, cc, opts...)
	}
	conn, err := grpc.NewClient(addresses[1], grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithUnaryInterceptor(bearer),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(sessionrpc.MaxBridgeAPIGRPCMessageBytes), grpc.MaxCallRecvMsgSize(sessionrpc.MaxBridgeAPIGRPCMessageBytes)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	runner, err := dbconnect.OpenProtectedDSN(t.Context(), e.roleURL("job_runner"), e.databaseCA, postgresName)
	if err != nil {
		t.Fatalf("open the database as Job Runner: %v", err)
	}
	t.Cleanup(func() { _ = runner.Client.Close() })
	client, err := enginetest.NewRuntime(bridgev1.NewAgentRuntimeBridgeServiceClient(conn), runner.Client, pod, "process_joint_runtime")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	if err := client.Register(t.Context()); err != nil {
		t.Fatal(err)
	}
	e.runtime = &testRuntime{e: e, client: client, sessions: new(atomic.Int64)}
	e.record.observe("Bridge API at %s as a process; the test Runtime client in pod %s/%s registered its process and takes bindings through Job Runner's delivery store", addresses[1], pod.Namespace, pod.Name)
	return e.runtime
}

// runtimeSession is a Session whose main thread is bound to the test
// Runtime, with one model request open for its tool calls.
type runtimeSession struct {
	r       *testRuntime
	id      string
	scope   *bridgev1.RuntimeScope
	request string
	calls   atomic.Int64
}

// bind binds the Session's main thread to the Runtime and opens a model
// request on it.
func (r *testRuntime) bind(sessionID string) *runtimeSession {
	r.e.t.Helper()
	return r.bindIn(harnessWorkspace, sessionID)
}

// bindIn is bind for a Session of the workspace ws.
func (r *testRuntime) bindIn(ws, sessionID string) *runtimeSession {
	e := r.e
	e.t.Helper()
	var thread string
	e.ownerTxIn(ws, func(ctx context.Context, tx *dbconnect.Tx) error {
		return tx.QueryRow(ctx, `SELECT main_thread_id FROM sessions WHERE workspace_id=$1 AND id=$2`, ws, sessionID).Scan(&thread)
	})
	scope, err := r.client.Bind(e.t.Context(), ws, sessionID, thread)
	if err != nil {
		e.t.Fatal(err)
	}
	s := &runtimeSession{r: r, id: sessionID, scope: scope, request: fmt.Sprintf("mreq_joint_%d", r.sessions.Add(1))}
	if err := r.client.StartModelRequest(e.t.Context(), scope, "rwrite_start_"+s.request, s.request); err != nil {
		e.t.Fatal(err)
	}
	e.record.command("test Runtime bound Session %s's main thread %s (binding %s) and opened model request %s", sessionID, thread, scope.Binding.BindingId, s.request)
	return s
}

// use declares a tool use of tool with input and route capability
// capability, and returns its event ID.
func (s *runtimeSession) use(tool, input, capability string) string {
	t := s.r.e.t
	t.Helper()
	eventID, err := s.declare(tool, input, capability)
	if err != nil {
		t.Fatal(err)
	}
	return eventID
}

// declare is use returning its error, for use off the test's goroutine.
func (s *runtimeSession) declare(tool, input, capability string) (string, error) {
	callID := fmt.Sprintf("call_%s_%d", s.request, s.calls.Add(1))
	return s.r.client.DeclareToolUse(s.r.e.t.Context(), s.scope, enginetest.ToolUse{WriteID: "rwrite_" + callID, ModelRequestID: s.request,
		ModelToolCallID: callID, Tool: tool, InputJSON: input, RouteCapability: capability})
}

// toolCall is one admitted machine-bound call.
type toolCall struct {
	use    string
	taskID string
}

// admit declares and admits a call of tool with input to computerID under
// policy, and returns the tool use and Bridge's answer.
func (s *runtimeSession) admit(computerID, tool, input, policy string) (string, *bridgev1.AdmitToolCallResponse) {
	s.r.e.t.Helper()
	use := s.use(tool, input, "machine_tool")
	response := s.admitUse(use, computerID, tool, input, policy)
	s.r.e.record.command("test Runtime admitted %s %s to computer %s as tool use %s: %v", tool, input, computerID, use, response)
	return use, response
}

// admitUse admits the declared tool use use as admit does, without a record
// line, for a scenario that admits many calls or must know a call's tool use
// before Engine dispatches it.
func (s *runtimeSession) admitUse(use, computerID, tool, input, policy string) *bridgev1.AdmitToolCallResponse {
	t := s.r.e.t
	t.Helper()
	response, err := s.tryAdmit(use, computerID, tool, input, policy)
	if err != nil {
		t.Fatalf("AdmitToolCall of %s to %s: %v", tool, computerID, err)
	}
	return response
}

// tryAdmit is admitUse returning its error, for use off the test's
// goroutine.
func (s *runtimeSession) tryAdmit(use, computerID, tool, input, policy string) (*bridgev1.AdmitToolCallResponse, error) {
	return s.r.client.AdmitToolCall(s.r.e.t.Context(), &bridgev1.AdmitToolCallRequest{Scope: s.scope, ToolUseEventId: use, ComputerId: computerID,
		Tool: tool, InputJson: input, SandboxPolicyJson: policy})
}

// exec admits exec_command cmd to computerID with scenarioPolicy; the
// admission must answer admitted.
func (s *runtimeSession) exec(computerID, cmd string) toolCall {
	return s.execInput(computerID, map[string]any{"cmd": cmd})
}

// execInput admits exec_command with input, the tool.execute input, as exec
// does. Without an on_wait_expiry it sends detach, as Engine does for the
// model's exec_command.
func (s *runtimeSession) execInput(computerID string, input map[string]any) toolCall {
	t := s.r.e.t
	t.Helper()
	if _, set := input["on_wait_expiry"]; !set {
		input["on_wait_expiry"] = "detach"
	}
	use, response := s.admit(computerID, protocol.ToolExecCommand, mustJSON(t, input), scenarioPolicy)
	if response.GetAdmitted() == nil {
		t.Fatalf("admission of %v to %s = %v; want admitted", input, computerID, response)
	}
	return toolCall{use: use, taskID: response.GetAdmitted().GetTaskId()}
}

// heldCommand is a command that creates the file started, waits until the
// file release exists, then writes finished.
func heldCommand(started, release string) string {
	return "touch '" + started + "'; while [ ! -e '" + release + "' ]; do sleep 0.1; done; echo finished"
}

// heldTask admits heldCommand in the user computer's working directory dir
// with a 60-second wait and returns the call once its command has started.
func (s *runtimeSession) heldTask(m *machine, computerID, dir, name string) (toolCall, string) {
	t := s.r.e.t
	t.Helper()
	started, release := dir+"/"+name+".started", dir+"/"+name+".release"
	call := s.execInput(computerID, map[string]any{"cmd": heldCommand(started, release), "wait_ms": 60000})
	deadline := time.Now().Add(time.Minute)
	for {
		if code, _ := m.shell("test", "-e", started); code == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the command of %s did not start on %s within a minute", call.use, m.name)
		}
		time.Sleep(200 * time.Millisecond)
	}
	s.r.e.record.observe("the command of %s runs on %s and waits for %s", call.use, m.name, release)
	return call, release
}

// userComputerSession creates a Session whose computers are an unserved cloud
// computer and a user computer on instanceID in the machine's directory
// name, which it creates, and waits until the user computer is ready there.
// It returns the Session and the user computer's ID.
func userComputerSession(t *testing.T, e *engine, m *machine, instanceID, name string) (sessionObject, string) {
	t.Helper()
	ctx := t.Context()
	work := m.directory(name)
	if code, _ := m.shell("mkdir", "-p", work); code != 0 {
		t.Fatalf("mkdir %s on %s exited %d", work, m.name, code)
	}
	developer := e.developer()
	session := developer.createSession(ctx, developer.createAgent(ctx, name), developer.unservedCloudComputer(ctx, name),
		map[string]any{"instance_id": instanceID, "working_directory": work})
	computerID := session.Computers[1].ID
	ready := developer.waitComputer(ctx, session.ID, computerID, time.Minute, "ready", func(c computerObject) bool { return c.ExecutionState == "ready" })
	e.record.observe("Session %s: user computer %s ready on binding %s in %s", session.ID, computerID, *ready.BindingID, work)
	return session, computerID
}

// scenarioPolicy is the sandbox policy of the scenarios' calls: every path
// readable and writable but what the CLI itself protects, with the network
// disabled, which every machine enforces. A drive mount is writable only
// under a policy with a read_write rule.
const scenarioPolicy = `{"schema_version":1,"filesystem":{"rules":[{"path":"/","access":"read_write"}]},"network":{"mode":"disabled"}}`

// settlement is a call's settlement record as Bridge's AwaitToolCall returns
// it.
type settlement struct {
	Settlement string          `json:"settlement"`
	Phase      string          `json:"phase"`
	Code       string          `json:"code"`
	Message    string          `json:"message"`
	Result     json.RawMessage `json:"result"`
}

// await waits up to timeout for the call's settlement through Bridge.
func (s *runtimeSession) await(use string, timeout time.Duration) settlement {
	t := s.r.e.t
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	response, err := s.r.client.AwaitToolCall(ctx, &bridgev1.AwaitToolCallRequest{Scope: s.scope, ToolUseEventId: use})
	if err != nil {
		t.Fatalf("AwaitToolCall of %s within %s: %v", use, timeout, err)
	}
	if response.GetSettled() == nil {
		t.Fatalf("AwaitToolCall of %s = %v; want settled", use, response)
	}
	var record settlement
	if err := json.Unmarshal([]byte(response.GetSettled().GetSettlementJson()), &record); err != nil {
		t.Fatalf("settlement of %s %s: %v", use, response.GetSettled().GetSettlementJson(), err)
	}
	s.r.e.record.observe("tool use %s settled: %s", use, response.GetSettled().GetSettlementJson())
	return record
}

// execEnvelope is the part of an exec_command report's result envelope the
// scenarios read: the tool's status and error, the command's result and the
// drive sync point's outcome.
type execEnvelope struct {
	Status string `json:"status"`
	Error  *struct {
		Kind    string `json:"kind"`
		Message string `json:"message"`
	} `json:"error"`
	Result    execResult `json:"result"`
	DriveSync *driveSync `json:"drive_sync"`
}

// execResult is the part of an exec_command result the scenarios read.
type execResult struct {
	DurationMS int64   `json:"duration_ms"`
	ExitCode   *int64  `json:"exit_code"`
	Signal     *string `json:"signal"`
	Stdout     struct {
		Text string `json:"text"`
	} `json:"stdout"`
	Stderr struct {
		Text string `json:"text"`
	} `json:"stderr"`
}

// requireCompleted waits up to timeout for the call's terminal report and
// checks that its command exited 0 and wrote stdout, trimmed, as its
// standard output.
func (s *runtimeSession) requireCompleted(call toolCall, timeout time.Duration, stdout string) execEnvelope {
	t := s.r.e.t
	t.Helper()
	envelope := s.terminal(call, timeout)
	if envelope.Result.ExitCode == nil || *envelope.Result.ExitCode != 0 || strings.TrimSpace(envelope.Result.Stdout.Text) != stdout {
		t.Fatalf("call %s result %+v; want exit 0 with stdout %q", call.use, envelope, stdout)
	}
	return envelope
}

// terminal waits up to timeout for the call's terminal report and returns
// its result envelope.
func (s *runtimeSession) terminal(call toolCall, timeout time.Duration) execEnvelope {
	t := s.r.e.t
	t.Helper()
	settled := s.await(call.use, timeout)
	var envelope execEnvelope
	if settled.Settlement != "report" || settled.Phase != "terminal" || json.Unmarshal(settled.Result, &envelope) != nil {
		t.Fatalf("call %s settled %+v; want a terminal report", call.use, settled)
	}
	return envelope
}

// poll admits write_stdin with empty chars for the call's task with a wait
// of waitMS, and returns the result envelope of its reply.
func (s *runtimeSession) poll(call toolCall, waitMS int64, timeout time.Duration) execEnvelope {
	t := s.r.e.t
	t.Helper()
	use := s.use("write_stdin", mustJSON(t, map[string]any{"session_id": call.taskID, "chars": ""}), "machine_control")
	response, err := s.r.client.AdmitToolControl(t.Context(), &bridgev1.AdmitToolControlRequest{Scope: s.scope, ToolUseEventId: use, TaskId: call.taskID, WaitMs: &waitMS})
	if err != nil || response.GetAdmitted() == nil {
		t.Fatalf("AdmitToolControl of a poll of task %s = %v, %v; want admitted", call.taskID, response, err)
	}
	settled := s.await(use, timeout)
	var envelope execEnvelope
	if settled.Settlement != "reply" || json.Unmarshal(settled.Result, &envelope) != nil {
		t.Fatalf("poll %s of task %s settled %+v; want a reply", use, call.taskID, settled)
	}
	return envelope
}

// finished waits up to timeout for the end of the call's command and
// returns the envelope that reports it: the call's terminal report, or, when
// a running report settled the call, the terminal result Engine stored for
// its background task. An output read after the task's cleanup returns
// result_unavailable, so polling cannot be relied on to see the end.
func (s *runtimeSession) finished(call toolCall, timeout time.Duration) execEnvelope {
	t := s.r.e.t
	t.Helper()
	deadline := time.Now().Add(timeout)
	settled := s.await(call.use, timeout)
	var envelope execEnvelope
	if settled.Settlement != "report" || json.Unmarshal(settled.Result, &envelope) != nil {
		t.Fatalf("call %s settled %+v; want a report", call.use, settled)
	}
	for envelope.Status == "running" {
		var status string
		var result *string
		s.r.e.ownerTxIn(s.scope.GetWorkspaceId(), func(ctx context.Context, tx *dbconnect.Tx) error {
			return tx.QueryRow(ctx, `SELECT status, terminal_result_json FROM session_background_tasks WHERE workspace_id=$1 AND source_tool_use_event_id=$2`,
				s.scope.GetWorkspaceId(), call.use).Scan(&status, &result)
		})
		if status != "running" {
			if result == nil || json.Unmarshal([]byte(*result), &envelope) != nil {
				t.Fatalf("the background task of call %s ended %s with result %v; want a result envelope", call.use, status, result)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the command of call %s still ran after %s", call.use, timeout)
		}
		time.Sleep(time.Second)
	}
	return envelope
}

// requireNotExecuted waits up to timeout for the call's settlement and checks
// that Engine ended it not executed with code and exactly message.
func (s *runtimeSession) requireNotExecuted(call toolCall, timeout time.Duration, code, message string) {
	t := s.r.e.t
	t.Helper()
	if settled := s.await(call.use, timeout); settled.Settlement != "not_executed" || settled.Code != code || settled.Message != message {
		t.Fatalf("call %s settled %+v; want not_executed %s %q", call.use, settled, code, message)
	}
}

// markRunning writes session.status_running on the Session's main thread,
// as the Runtime does when a turn starts, which makes the Session running.
func (s *runtimeSession) markRunning() {
	t := s.r.e.t
	t.Helper()
	if _, err := s.r.client.Bridge.WriteEvent(t.Context(), &bridgev1.WriteEventRequest{
		Scope:          s.scope,
		RuntimeWriteId: "rwrite_running_" + s.request,
		EventType:      "session.status_running",
		PayloadJson:    `{"type":"session.status_running"}`,
	}); err != nil {
		t.Fatalf("write session.status_running: %v", err)
	}
	s.r.e.record.command("test Runtime wrote session.status_running for Session %s", s.id)
}

// cancel cancels the call through Bridge.
func (s *runtimeSession) cancel(call toolCall) {
	t := s.r.e.t
	t.Helper()
	response, err := s.r.client.CancelToolCall(t.Context(), &bridgev1.CancelToolCallRequest{Scope: s.scope, ToolUseEventId: call.use})
	if err != nil {
		t.Fatalf("CancelToolCall of %s: %v", call.use, err)
	}
	s.r.e.record.command("test Runtime cancelled tool use %s: %v", call.use, response)
}
