package integration

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/integration/enginetest"
	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/drive"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/internalgrpc"
	internalgrpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol/resultnotify"
	"github.com/tetral-ai/tetral/internal/sessionrpc"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	computerhub "github.com/tetral-ai/tetral/services/computer-hub"
)

// machineTools is one Engine for machine-bound tool calls on one test
// database: Bridge's gRPC API with its result listener, two computer-hub
// servers with client and instance routes sharing one blob store, and a
// Runtime client in one Runtime pod's place. Each service uses its own
// workload role; admin seeds what the public API would have created and
// reads rows for assertions.
type machineTools struct {
	t         testing.TB
	ctx       context.Context
	admin     *sql.DB
	owner     *dbconnect.Client
	bridgeDB  *dbconnect.Client
	blobs     *blob.FakeBlobStore
	ws        workspace.ID
	signer    *auth.InternalPrincipalSigner
	principal auth.Principal
	hubs      []string
	runtime   *enginetest.Runtime
	sessions  atomic.Int64
	workloads *storagetest.WorkloadDB
	api       http.Handler
	// hubSQL observes every statement of the hub servers, which share
	// hubPool, and bridgeSQL every statement of Bridge; uploadBudget is the
	// hubs' per-minute drive upload budget.
	hubSQL       *statementTracer
	bridgeSQL    *statementTracer
	hubPool      *sql.DB
	uploadBudget int64
	timeout      time.Duration
	// hubLogs holds the JSON records of the hub servers.
	hubLogs lockedBuffer
}

// machineToolsOption changes the fixture before its servers start.
type machineToolsOption func(*machineTools)

// withUploadBudget sets the hubs' per-minute drive upload budget.
func withUploadBudget(perMinute int64) machineToolsOption {
	return func(f *machineTools) { f.uploadBudget = perMinute }
}

// withTimeout sets how long the fixture's context lasts, 3 minutes by
// default.
func withTimeout(timeout time.Duration) machineToolsOption {
	return func(f *machineTools) { f.timeout = timeout }
}

const machineToolRuntimeToken = "machine-tool-runtime-token"

func newMachineTools(t testing.TB, options ...machineToolsOption) *machineTools {
	t.Helper()
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	f := &machineTools{t: t, admin: admin, owner: dbconnect.NewClientForTesting(admin), ws: workspace.DefaultID,
		hubSQL: &statementTracer{}, bridgeSQL: &statementTracer{}, uploadBudget: 1 << 40, timeout: 3 * time.Minute}
	for _, option := range options {
		option(f)
	}
	ctx, cancel := context.WithTimeout(context.Background(), f.timeout)
	t.Cleanup(cancel)
	f.ctx = ctx
	if _, err := workspace.NewSeeder(admin).Seed(ctx, f.ws, "machine"); err != nil {
		t.Fatal(err)
	}
	f.principal = auth.IndependentKeyPrincipal(workspace.Workspace{ID: f.ws}, "key_machine_tools")
	workloads := storagetest.OpenWorkloadDB(t, admin, "job_runner")
	f.workloads = workloads
	runner := dbconnect.NewClientForTesting(workloads.DB)
	f.bridgeDB = dbconnect.NewClientForTesting(workloads.OpenWorkload(t, "bridge", f.bridgeSQL))
	f.blobs = blob.NewFakeBlobStore()
	// Each hub server holds three listening connections.
	hubPool := workloads.OpenWorkload(t, "computer_hub", f.hubSQL)
	hubPool.SetMaxOpenConns(32)
	f.hubPool = hubPool
	hubDB := dbconnect.NewClientForTesting(hubPool)

	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if f.signer, err = auth.NewInternalPrincipalSigner(private); err != nil {
		t.Fatal(err)
	}
	verifier, err := auth.NewInternalPrincipalVerifier(public)
	if err != nil {
		t.Fatal(err)
	}
	for _, pod := range []string{"computer-hub-0", "computer-hub-1"} {
		f.hubs = append(f.hubs, f.startHub(hubDB, verifier, pod))
	}

	pod := enginetest.RuntimePod{Namespace: "tetral-agent-runtime", Name: "agent-runtime-0", UID: "pod_machine_tools"}
	client := f.bridgePod(pod.UID)
	if f.runtime, err = enginetest.NewRuntime(client, runner, pod, "process_machine_tools"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.runtime.Close)
	if err := f.runtime.Register(ctx); err != nil {
		t.Fatal(err)
	}
	return f
}

// bridgePod starts a Bridge API server, with its own result listener, whose
// caller is the Runtime pod podUID, and returns its client. Every server
// shares the database and the binding token key, as Bridge's pods do.
func (f *machineTools) bridgePod(podUID string) bridgev1.AgentRuntimeBridgeServiceClient {
	store := agentruntimebridge.NewPostgreSQLBridgeAPIStore(f.bridgeDB)
	store.RuntimeBindingTokenHMACKey = []byte("test-only-machine-tool-token-key")
	go func() { _ = store.RunExecutionResultListener(f.ctx) }()
	return startMachineToolBridge(f.t, store, podUID)
}

// startHub starts a computer-hub server named pod and returns its URL.
func (f *machineTools) startHub(database *dbconnect.Client, verifier *auth.InternalPrincipalVerifier, pod string) string {
	events := computerhub.NewInstanceEvents(database, computerhub.DefaultCLIVersion, nil)
	requests := computerhub.NewRequestEvents(database, nil)
	streams := computerhub.NewStreams(database, pod, computerhub.DefaultStreamMaxAge, nil, nil, nil)
	drives, err := drive.NewService(database, largeBlobSink{f.blobs}, "")
	if err != nil {
		f.t.Fatal(err)
	}
	uploads, err := drive.NewUploadBudget(database, f.uploadBudget, workload.NewRegistry(pod))
	if err != nil {
		f.t.Fatal(err)
	}
	for _, run := range []func(context.Context) error{events.Run, requests.Run, streams.Run} {
		go func() { _ = run(f.ctx) }()
	}
	logger := slog.New(slog.NewMultiHandler(workload.CurrentTelemetry().Logger().Handler(), slog.NewJSONHandler(&f.hubLogs, nil)))
	router := computerhub.NewRouter(verifier, computerhub.WithLogger(logger),
		computerhub.WithInstanceRoutes(computerhub.InstanceRoutes{Database: database, MaxCredentialLifetime: 24 * time.Hour, PageTokenKey: make([]byte, 32),
			RecommendedCLIVersion: computerhub.DefaultCLIVersion, ServerURL: "https://api.tetral.test", Events: events, Requests: requests}),
		computerhub.WithClientRoutes(computerhub.ClientRoutes{Database: database, MinCLIVersion: computerhub.DefaultCLIVersion, Streams: streams, GitProxyURL: "https://git.tetral.test",
			Blobs: f.blobs, Drives: drives, Uploads: uploads}))
	server := httptest.NewServer(computerhub.BuildHTTPHandler(workload.NewReadiness(), router))
	f.t.Cleanup(server.Close)
	return server.URL
}

// startMachineToolBridge serves store as Bridge's gRPC API with the
// production method authorizer and message bounds; the authenticator stands
// in for TokenReview and knows the Runtime client's token alone. Every call
// of the returned client carries that token.
func startMachineToolBridge(t testing.TB, store agentruntimebridge.BridgeAPIStore, podUID string) bridgev1.AgentRuntimeBridgeServiceClient {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := internalgrpc.NewServer(internalgrpc.Config{
		ServiceName:      agentruntimebridge.ServiceNameBridgeAPI,
		Listener:         listener,
		Authenticator:    machineToolIdentity{podUID: podUID},
		MethodAuthorizer: agentruntimebridge.BridgeAPIMethodAuthorizer,
		Register:         func(s *grpc.Server) { agentruntimebridge.RegisterBridgeAPI(s, store) },
		ServerOptions:    []grpc.ServerOption{grpc.MaxRecvMsgSize(sessionrpc.MaxBridgeAPIGRPCMessageBytes), grpc.MaxSendMsgSize(sessionrpc.MaxBridgeAPIGRPCMessageBytes)},
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	bearer := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+machineToolRuntimeToken), method, req, reply, cc, opts...)
	}
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithUnaryInterceptor(bearer),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(sessionrpc.MaxBridgeAPIGRPCMessageBytes), grpc.MaxCallRecvMsgSize(sessionrpc.MaxBridgeAPIGRPCMessageBytes)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return bridgev1.NewAgentRuntimeBridgeServiceClient(conn)
}

type machineToolIdentity struct{ podUID string }

func (a machineToolIdentity) Authenticate(_ context.Context, token string) (internalgrpcauth.Identity, error) {
	if token != machineToolRuntimeToken {
		return internalgrpcauth.Identity{}, status.Error(codes.Unauthenticated, "unknown token")
	}
	return internalgrpcauth.Identity{ServiceAccount: internalgrpcauth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"}, KubernetesPodUID: a.podUID}, nil
}

// toolSession is a running Session bound to the Runtime client, on its main
// thread unless a test scopes a call to another thread.
type toolSession struct {
	f         *machineTools
	id        string
	thread    string
	scope     *bridgev1.RuntimeScope
	request   string
	calls     int
	computers int
}

// nextComputer names the Session's next computer.
func (s *toolSession) nextComputer() string {
	s.computers++
	if s.computers == 1 {
		return "comp_" + s.id
	}
	return fmt.Sprintf("comp_%s_%d", s.id, s.computers)
}

// session seeds a Session, binds it to the Runtime client and marks it
// running on that binding, as Job Runner's delivery of a first input does.
func (f *machineTools) session() *toolSession {
	f.t.Helper()
	n := f.sessions.Add(1)
	s := &toolSession{f: f, id: fmt.Sprintf("sesn_machine_%d", n), thread: fmt.Sprintf("sthr_machine_%d", n)}
	sessionfixture.SeedBridgeAPISession(f.t, f.admin, string(f.ws), s.id, s.thread)
	scope, err := f.runtime.Bind(f.ctx, string(f.ws), s.id, s.thread)
	if err != nil {
		f.t.Fatal(err)
	}
	s.scope = scope
	f.exec(`UPDATE session_runtime_status SET status='running',binding_id=$2,binding_generation=$3 WHERE workspace_id=$4 AND session_id=$1`,
		s.id, scope.Binding.BindingId, scope.Binding.BindingGeneration, string(f.ws))
	s.startRequest(scope)
	return s
}

func (s *toolSession) startRequest(scope *bridgev1.RuntimeScope) {
	s.f.t.Helper()
	s.calls++
	s.request = fmt.Sprintf("mreq_%s_%s_%d", s.id, scope.SessionThreadId, s.calls)
	if err := s.f.runtime.StartModelRequest(s.f.ctx, scope, "rwrite_start_"+s.request, s.request); err != nil {
		s.f.t.Fatal(err)
	}
}

// in returns a copy of s whose calls are scoped to thread, which has its own
// model request.
func (s *toolSession) in(thread string) *toolSession {
	other := *s
	scope := proto.Clone(s.scope).(*bridgev1.RuntimeScope)
	scope.SessionThreadId = thread
	other.thread, other.scope = thread, scope
	other.startRequest(scope)
	return &other
}

// openTurn writes the thread's running event and returns its event ID, the
// durable turn a termination of the thread names.
func (s *toolSession) openTurn() string {
	s.f.t.Helper()
	running, err := s.f.runtime.Bridge.WriteEvent(s.f.ctx, &bridgev1.WriteEventRequest{Scope: s.scope,
		RuntimeWriteId: "rwrite_running_" + s.thread, EventType: "session.status_running", PayloadJson: `{"type":"session.status_running"}`})
	if err != nil || running.GetCommitted() == nil {
		s.f.t.Fatalf("open the turn of %s = %v, %v", s.thread, running, err)
	}
	return running.GetCommitted().GetEventId()
}

// child seeds a subagent thread of s's thread and returns a copy of s scoped
// to it, with its own model request, and its open turn.
func (s *toolSession) child(name string) (*toolSession, string) {
	s.f.t.Helper()
	thread := "sthr_" + name + "_" + s.id
	s.f.exec(`INSERT INTO session_threads (workspace_id,id,session_id,parent_thread_id,role,visibility,status,task_name,created_at,last_active_at,updated_at)
		VALUES ($1,$2,$3,$4,'subagent','public','idle','task_'||$2,clock_timestamp(),clock_timestamp(),clock_timestamp())`, string(s.f.ws), thread, s.id, s.thread)
	seedBridgeAPIEvent(s.f.t, s.f.admin, string(s.f.ws), s.id, thread, "evt_created_"+thread, 1, "session.thread_created",
		`{"type":"session.thread_created","parent_thread_id":`+quoteJSON(s.thread)+`,"source_tool_use_event_id":"evt_spawn_`+thread+`"}`)
	child := *s
	child.thread, child.scope = thread, proto.Clone(s.scope).(*bridgev1.RuntimeScope)
	child.scope.SessionThreadId = thread
	turn := child.openTurn()
	child.startRequest(child.scope)
	return &child, turn
}

// terminate commits the runtime termination of s's thread, whose open turn
// is turn.
func (s *toolSession) terminate(turn string) *bridgev1.CommitRuntimeTerminationResponse {
	s.f.t.Helper()
	response, err := s.f.runtime.Bridge.CommitRuntimeTermination(s.f.ctx, &bridgev1.CommitRuntimeTerminationRequest{Scope: s.scope, RuntimeWriteId: turn,
		FailureJson: `{"type":"runtime","code":"runtime_invalid_sequence","message":"Runtime operation failed.","retryable":false,"fatal":true,"retryStatus":{"type":"terminal"},"reason":"runtime_contract_validation"}`})
	if err != nil {
		s.f.t.Fatalf("terminate %s: %v", s.thread, err)
	}
	return response
}

// use declares a tool use of tool with route capability capability and
// returns its event ID.
func (s *toolSession) use(tool, capability string) string {
	s.f.t.Helper()
	s.calls++
	callID := fmt.Sprintf("call_%s_%d", s.thread, s.calls)
	input := `{"cmd":"true"}`
	if tool == "write_stdin" {
		input = `{"session_id":1,"chars":""}`
	}
	eventID, err := s.f.runtime.DeclareToolUse(s.f.ctx, s.scope, enginetest.ToolUse{WriteID: "rwrite_" + callID, ModelRequestID: s.request,
		ModelToolCallID: callID, Tool: tool, InputJSON: input, RouteCapability: capability})
	if err != nil {
		s.f.t.Fatal(err)
	}
	return eventID
}

// admit declares an exec-style tool use of tool and admits it to computerID.
func (s *toolSession) admit(computerID, tool, inputJSON, policyJSON string) (string, *bridgev1.AdmitToolCallResponse, error) {
	s.f.t.Helper()
	use := s.use(tool, "machine_tool")
	response, err := s.f.runtime.AdmitToolCall(s.f.ctx, &bridgev1.AdmitToolCallRequest{Scope: s.scope, ToolUseEventId: use, ComputerId: computerID,
		Tool: tool, InputJson: inputJSON, SandboxPolicyJson: policyJSON})
	return use, response, err
}

// admitted admits exec_command cmd to computerID with the disabled-network
// policy; the admission must answer admitted. It returns the tool use and
// task IDs.
func (s *toolSession) admitted(computerID, cmd string) (string, string) {
	s.f.t.Helper()
	use, response, err := s.admit(computerID, protocol.ToolExecCommand, `{"cmd":`+quoteJSON(cmd)+`}`, disabledNetworkPolicy)
	if err != nil || response.GetAdmitted() == nil {
		s.f.t.Fatalf("admission of %q = %v, %v; want admitted", cmd, response, err)
	}
	return use, response.GetAdmitted().GetTaskId()
}

// control declares a write_stdin tool use and admits chars to taskID.
func (s *toolSession) control(taskID, chars string, waitMS *int64, limits *bridgev1.ToolVisibleLimits) (string, *bridgev1.AdmitToolControlResponse, error) {
	s.f.t.Helper()
	use := s.use("write_stdin", "machine_control")
	response, err := s.f.runtime.AdmitToolControl(s.f.ctx, &bridgev1.AdmitToolControlRequest{Scope: s.scope, ToolUseEventId: use, TaskId: taskID,
		Chars: chars, WaitMs: waitMS, Limits: limits})
	return use, response, err
}

const disabledNetworkPolicy = `{"schema_version":1,"filesystem":{"rules":[]},"network":{"mode":"disabled"}}`

func quoteJSON(value string) string {
	data, _ := json.Marshal(value)
	return string(data)
}

// settlement returns the call's tool result row state and its settlement
// record, "" while pending.
func (s *toolSession) settlement(use string) (string, map[string]any) {
	s.f.t.Helper()
	row := s.f.row(`SELECT execution_state, result_json FROM session_runtime_tool_results WHERE session_id=$1 AND tool_use_event_id=$2`, s.id, use)
	var record map[string]any
	if raw, ok := row["result_json"].(string); ok {
		if err := json.Unmarshal([]byte(raw), &record); err != nil {
			s.f.t.Fatalf("settlement record %s: %v", raw, err)
		}
	}
	return row["execution_state"].(string), record
}

// requireNotExecuted checks that the call settled not executed with code and
// exactly message.
func (s *toolSession) requireNotExecuted(use, code, message string) {
	s.f.t.Helper()
	state, record := s.settlement(use)
	if state != "terminal_unconsumed" || record["settlement"] != "not_executed" || record["code"] != code || record["message"] != message {
		s.f.t.Fatalf("call %s = %s %v; want not_executed %s %q", use, state, record, code, message)
	}
}

func (s *toolSession) requireUnknownOutcome(use, message string) {
	s.f.t.Helper()
	state, record := s.settlement(use)
	if state != "terminal_unconsumed" || record["settlement"] != "unknown_outcome" || record["message"] != message {
		s.f.t.Fatalf("call %s = %s %v; want unknown_outcome %q", use, state, record, message)
	}
}

// records returns the call's dispatch records as type:state, in creation
// order.
func (s *toolSession) records(use string) []string {
	s.f.t.Helper()
	rows, err := s.f.admin.QueryContext(s.f.ctx, `SELECT d.type||':'||d.state FROM session_tool_dispatches d
		JOIN session_tool_dispatches e ON e.workspace_id=d.workspace_id AND e.session_id=d.session_id AND e.task_id=d.task_id AND e.type='tool.execute'
		WHERE d.session_id=$1 AND (d.tool_use_event_id=$2 OR (e.tool_use_event_id=$2 AND d.type='tool.cancel'))
		ORDER BY d.created_at, d.message_id`, s.id, use)
	if err != nil {
		s.f.t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var records []string
	for rows.Next() {
		var record string
		if err := rows.Scan(&record); err != nil {
			s.f.t.Fatal(err)
		}
		records = append(records, record)
	}
	return records
}

func (f *machineTools) exec(query string, args ...any) {
	f.t.Helper()
	if _, err := f.admin.ExecContext(f.ctx, query, args...); err != nil {
		f.t.Fatalf("%s: %v", query, err)
	}
}

func (f *machineTools) count(query string, args ...any) int {
	f.t.Helper()
	var n int
	if err := f.admin.QueryRowContext(f.ctx, query, args...).Scan(&n); err != nil {
		f.t.Fatalf("%s: %v", query, err)
	}
	return n
}

func (f *machineTools) row(query string, args ...any) map[string]any {
	f.t.Helper()
	var raw []byte
	if err := f.admin.QueryRowContext(f.ctx, `SELECT row_to_json(r)::text FROM (`+query+`) r`, args...).Scan(&raw); err != nil {
		f.t.Fatalf("%s: %v", query, err)
	}
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		f.t.Fatal(err)
	}
	return value
}

// cli returns a test CLI for the instance and credential on hub server hub,
// whose requests carry the connection key principal the edge would forward.
func (f *machineTools) cli(hub int, instanceID, credentialID string) *enginetest.CLI {
	principal := auth.ConnectionKeyPrincipal(workspace.Workspace{ID: f.ws}, instanceID, credentialID)
	return &enginetest.CLI{BaseURL: f.hubs[hub], InstanceID: instanceID, CredentialID: credentialID, Authorize: func(r *http.Request) error {
		token, err := f.signer.Mint(principal, r.Method, r.URL.Path, "req_machine_cli", time.Minute)
		r.Header.Set("X-Tetral-Internal-Principal", token)
		return err
	}}
}

// userInstance creates a user instance with one active credential.
func (f *machineTools) userInstance(name string) *enginetest.CLI {
	f.t.Helper()
	var cli *enginetest.CLI
	f.ownerTx(func(tx *dbconnect.Tx) error {
		created, err := instance.CreateUserInstance(f.ctx, tx, f.ws, name)
		if err != nil {
			return err
		}
		now, err := instance.Now(f.ctx, tx)
		if err != nil {
			return err
		}
		credential, _, err := instance.IssueCredential(f.ctx, tx, f.ws, created.ID, instance.CredentialActive, now, now.Add(time.Hour))
		cli = f.cli(0, created.ID, credential.ID)
		return err
	})
	return cli
}

func (f *machineTools) ownerTx(fn func(*dbconnect.Tx) error) {
	f.t.Helper()
	if err := f.owner.WithWorkspaceTx(f.ctx, string(f.ws), "integration.machine_tools", fn); err != nil {
		f.t.Fatal(err)
	}
}

// userComputer attaches a user computer on cli's instance.
func (s *toolSession) userComputer(cli *enginetest.CLI) string {
	s.f.t.Helper()
	computerID := s.nextComputer()
	s.f.exec(`INSERT INTO computers (workspace_id,session_id,id,kind,instance_id,current_instance_id,working_directory,networking,drive_exclude,association_state,execution_state)
		VALUES ($1,$2,$3,'user',$4,$4,'/Users/me/shop','{"type":"unrestricted"}','[]','attached','waiting')`, string(s.f.ws), s.id, computerID, cli.InstanceID)
	return computerID
}

// cloudComputer attaches a cloud computer with no instance.
func (s *toolSession) cloudComputer() string {
	s.f.t.Helper()
	computerID := s.nextComputer()
	s.f.exec(`INSERT INTO computers (workspace_id,session_id,id,kind,environment_id,working_directory,networking,drive_exclude,association_state,execution_state)
		VALUES ($1,$2,$3,'cloud',$4,'/workspace','{"type":"unrestricted"}','[]','attached','waiting')`, string(s.f.ws), s.id, computerID, "env_"+s.id)
	return computerID
}

// controller sends a Controller request route on hub server 0 and returns
// its status and body.
func (f *machineTools) controller(method, path, body string) (int, []byte) {
	f.t.Helper()
	return f.controllerOn(0, method, path, body)
}

func (f *machineTools) controllerOn(hub int, method, path, body string) (int, []byte) {
	f.t.Helper()
	status, data, err := f.controllerSend(hub, method, path, body)
	if err != nil {
		f.t.Fatal(err)
	}
	return status, data
}

// controllerSend is controllerOn for a goroutine other than the test's: it
// returns its error.
func (f *machineTools) controllerSend(hub int, method, path, body string) (int, []byte, error) {
	token, err := f.signer.Mint(f.principal, method, path, "req_machine_controller", time.Minute)
	if err != nil {
		return 0, nil, err
	}
	request, err := http.NewRequestWithContext(f.ctx, method, f.hubs[hub]+path, strings.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("X-Tetral-Internal-Principal", token)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = response.Body.Close() }()
	data, err := io.ReadAll(response.Body)
	return response.StatusCode, data, err
}

// claimed is a Controller claim's bootstrap.
type claimed struct {
	Bootstrap struct {
		InstanceID   string `json:"instance_id"`
		CredentialID string `json:"credential_id"`
	} `json:"bootstrap"`
}

// claim claims requestID on hub server hub as workerID.
func (f *machineTools) claim(hub int, requestID, workerID string) claimed {
	f.t.Helper()
	status, data := f.controllerOn(hub, http.MethodPost, "/v1/instance_requests/"+requestID+"/claim", `{"worker_id":"`+workerID+`"}`)
	var body claimed
	if status != http.StatusOK || json.Unmarshal(data, &body) != nil {
		f.t.Fatalf("claim of %s = %d %s", requestID, status, data)
	}
	return body
}

// cloudInstance gives the cloud computer a current instance with one active
// claim credential and returns its CLI; the instance has no connection.
func (s *toolSession) cloudInstance(computerID string) *enginetest.CLI {
	s.f.t.Helper()
	var cli *enginetest.CLI
	s.f.ownerTx(func(tx *dbconnect.Tx) error {
		created, err := instance.CreateCloudInstance(s.f.ctx, tx, s.f.ws, computerID, s.id)
		if err != nil {
			return err
		}
		now, err := instance.Now(s.f.ctx, tx)
		if err != nil {
			return err
		}
		credential, _, err := instance.IssueClaimCredential(s.f.ctx, tx, s.f.ws, created.ID, instance.CredentialActive, now, now.Add(time.Hour))
		if err != nil {
			return err
		}
		cli = s.f.cli(0, created.ID, credential.ID)
		_, err = tx.Exec(s.f.ctx, `UPDATE computers SET current_instance_id=$3 WHERE workspace_id=$1 AND id=$2`, string(s.f.ws), computerID, created.ID)
		return err
	})
	return cli
}

// interrupt commits an interrupt of s's thread through Bridge as the
// Runtime does once Job Runner delivered it, and returns its tool results
// by tool use.
func (s *toolSession) interrupt() map[string]*bridgev1.RuntimeInterruptToolResult {
	s.f.t.Helper()
	sequence := sessionfixture.NextBridgeAPIEventSequenceForTest(s.f.t, s.f.admin, s.id, s.thread)
	eventID, inputID := "evt_interrupt_"+s.id, "rin_interrupt_"+s.id
	seedBridgeAPIEvent(s.f.t, s.f.admin, string(s.f.ws), s.id, s.thread, eventID, sequence, "user.interrupt", `{}`)
	sessionfixture.SeedBridgeAPIRuntimeInbox(s.f.t, s.f.admin, string(s.f.ws), s.id, s.thread, inputID, "interrupt_control", `["`+eventID+`"]`, "accepted",
		s.scope.Binding.BindingId, s.scope.Binding.TargetPodUid, sequence, sequence)
	s.f.exec(`UPDATE session_runtime_inbox SET binding_generation=$2 WHERE runtime_input_id=$1`, inputID, s.scope.Binding.BindingGeneration)
	queueStore := queue.NewPostgreSQLStore(s.f.owner)
	enqueueInterruptExhaustionJob(s.f.t, queueStore, s.id, s.thread, inputID, "interrupt_control", eventID, sequence, queue.DefaultMaxAttempts, time.Now().UTC())
	lease := mustLeaseBridgeQueueJob(s.f.t, queueStore, queue.LeaseRequest{WorkspaceID: s.f.ws, Kinds: []string{queue.KindRuntimeInput},
		LeaseOwner: "machine-tools-interrupt", MaxJobs: 1, LeaseDuration: time.Minute, Now: time.Now().UTC()})
	committed, err := s.f.runtime.Bridge.CommitInputs(s.f.ctx, &bridgev1.CommitInputsRequest{Scope: s.scope, RuntimeInputId: inputID,
		InterruptLeaseRef: sessionfixture.BridgeInterruptLeaseRef(lease)})
	if err != nil || committed.GetCommitted().GetInterrupt() == nil {
		s.f.t.Fatalf("interrupt = %v, %v; want committed", committed, err)
	}
	results := map[string]*bridgev1.RuntimeInterruptToolResult{}
	for _, result := range committed.GetCommitted().GetInterrupt().GetInterruptToolResults() {
		results[result.GetToolUseEventId()] = result
	}
	return results
}

// openRequest returns the ID of the computer's open request.
func (s *toolSession) openRequest(computerID string) string {
	s.f.t.Helper()
	return s.f.row(`SELECT id FROM computer_requests WHERE session_id=$1 AND computer_id=$2 AND state IN ('pending','claimed')`, s.id, computerID)["id"].(string)
}

// cliStream is an open event stream whose events arrive on a channel.
type cliStream struct {
	f            *machineTools
	cli          *enginetest.CLI
	stream       *enginetest.Stream
	events       chan enginetest.Event
	connectionID string
}

// connect opens cli's event stream as a new connection and sends ready
// with capabilities; ready's ACK must be a grant.
func (f *machineTools) connect(cli *enginetest.CLI, capabilities protocol.Capabilities) *cliStream {
	f.t.Helper()
	s := f.openStream(cli, cli.EventsQuery("", nil))
	response, err := cli.Ready(f.ctx, protocol.Ready{ConnectionID: s.connectionID, MessageID: "ready_" + s.connectionID, OS: "linux", Arch: "x86_64",
		CLIVersion: "1.0.0", Capabilities: capabilities})
	if err != nil {
		f.t.Fatal(err)
	}
	if _, err := response.Grant(); err != nil {
		f.t.Fatalf("ready: %v", err)
	}
	return s
}

// resume opens cli's stream again for the live connection on hub server
// hub.
func (f *machineTools) resume(hub int, cli *enginetest.CLI, connectionID string) *cliStream {
	f.t.Helper()
	resume := true
	at := *cli
	at.BaseURL = f.hubs[hub]
	s := f.openStream(&at, at.EventsQuery(connectionID, &resume))
	if s.connectionID != connectionID {
		f.t.Fatalf("resumed connection %s; want %s", s.connectionID, connectionID)
	}
	return s
}

func (f *machineTools) openStream(cli *enginetest.CLI, query map[string][]string) *cliStream {
	f.t.Helper()
	stream, response, err := cli.OpenEvents(f.ctx, query)
	if err != nil || stream == nil {
		f.t.Fatalf("open events = %d %s, %v", response.Status, response.Body, err)
	}
	s := &cliStream{f: f, cli: cli, stream: stream, events: make(chan enginetest.Event, 64)}
	f.t.Cleanup(func() { _ = stream.Close() })
	go func() {
		defer close(s.events)
		for {
			event, err := stream.Next()
			if err != nil {
				return
			}
			s.events <- event
		}
	}()
	connected := s.next()
	if _, ok := connected.Envelope.Payload.(protocol.ConnectedPayload); !ok {
		f.t.Fatalf("first event = %s; want connected", connected.Data)
	}
	s.connectionID = connected.Envelope.ConnectionID
	return s
}

// next returns the stream's next event other than a heartbeat.
func (s *cliStream) next() enginetest.Event {
	s.f.t.Helper()
	timeout := time.After(20 * time.Second)
	for {
		select {
		case event, ok := <-s.events:
			if !ok {
				s.f.t.Fatal("stream ended")
			}
			if _, beat := event.Envelope.Payload.(protocol.HeartbeatPayload); beat {
				continue
			}
			return event
		case <-timeout:
			s.f.t.Fatal("no event within 20 seconds")
		}
	}
}

// quiet checks that no event other than a heartbeat arrives within d.
func (s *cliStream) quiet(d time.Duration) {
	s.f.t.Helper()
	timeout := time.After(d)
	for {
		select {
		case event, ok := <-s.events:
			if !ok {
				return
			}
			if _, beat := event.Envelope.Payload.(protocol.HeartbeatPayload); !beat {
				s.f.t.Fatalf("unexpected event %s", event.Data)
			}
		case <-timeout:
			return
		}
	}
}

// close ends the stream from the CLI's side.
func (s *cliStream) close() { _ = s.stream.Close() }

// prepare reads the next event, a session.prepare, and answers it with a
// prepared report, or with a failed report of code when code is set.
func (s *cliStream) prepare(code string) protocol.SessionPreparePayload {
	s.f.t.Helper()
	event := s.next()
	payload, ok := event.Envelope.Payload.(protocol.SessionPreparePayload)
	if !ok {
		s.f.t.Fatalf("event = %s; want session.prepare", event.Data)
	}
	s.report(event, code)
	return payload
}

// report posts the session-status report answering prepare.
func (s *cliStream) report(prepare enginetest.Event, code string) {
	s.f.t.Helper()
	payload := prepare.Envelope.Payload.(protocol.SessionPreparePayload)
	report := protocol.SessionStatus{ConnectionID: s.connectionID, MessageID: "status_" + prepare.Envelope.MessageID, RequestMessageID: prepare.Envelope.MessageID,
		SessionID: payload.SessionID, BindingID: payload.BindingID, Status: "prepared", Cwd: protocol.Some(payload.Cwd)}
	if code != "" {
		report.Status, report.Cwd = "failed", protocol.Optional[string]{}
		report.Error = protocol.Some(protocol.PreparationError{Code: code, Message: "the preparation failed: " + code})
	}
	response, err := s.cli.Post(s.f.ctx, "/v1/client/session-status", protocol.Encode(report))
	if err != nil {
		s.f.t.Fatal(err)
	}
	if ack, err := response.Acknowledgement(); err != nil || ack.MessageID != report.MessageID {
		s.f.t.Fatalf("session report = %d %s, %v", response.Status, response.Body, err)
	}
}

// execute reads the next event, which must be a tool.execute.
func (s *cliStream) execute() (enginetest.Event, protocol.ExecutePayload) {
	s.f.t.Helper()
	event := s.next()
	payload, ok := event.Envelope.Payload.(protocol.ExecutePayload)
	if !ok {
		s.f.t.Fatalf("event = %s; want tool.execute", event.Data)
	}
	return event, payload
}

// savedEnvelope returns the stored envelope of instruction messageID.
func (f *machineTools) savedEnvelope(messageID string) []byte {
	f.t.Helper()
	var envelope []byte
	if err := f.admin.QueryRowContext(f.ctx, `SELECT envelope FROM connection_instructions WHERE message_id=$1`, messageID).Scan(&envelope); err != nil {
		f.t.Fatal(err)
	}
	return envelope
}

// toolExecution is a ready's tool execution capability for tools and network
// modes; every control and sandbox policy version 1.
func toolExecution(tools, networkModes []string) protocol.Capabilities {
	return protocol.Capabilities{Drives: protocol.DriveCapability{Mount: true}, ToolExecution: protocol.Some(protocol.ToolExecutionCapability{
		Tools: tools, Controls: []string{protocol.ToolStdin, protocol.ToolCancel}, SandboxPolicyVersions: []int64{1}, NetworkModes: networkModes})}
}

var everyTool = toolExecution([]string{protocol.ToolExecCommand, protocol.ToolEdit, protocol.ToolViewImage}, []string{"disabled", "direct"})

// preparedUserComputer gives s a user computer whose CLI is online with
// every tool and prepared for s, and returns the computer and its stream.
func (s *toolSession) preparedUserComputer(name string) (string, *cliStream) {
	s.f.t.Helper()
	cli := s.f.userInstance(name)
	stream := s.f.connect(cli, everyTool)
	computerID := s.userComputer(cli)
	s.f.ownerTx(func(tx *dbconnect.Tx) error {
		_, err := computer.IssuePreparation(s.f.ctx, tx, s.f.ws, s.id, computerID)
		return err
	})
	stream.prepare("")
	return computerID, stream
}

// instructionAddress is the identity a report of instruction carries.
type instructionAddress struct {
	messageID, sessionID, bindingID, useID, taskID string
}

func addressOf(instruction enginetest.Event) instructionAddress {
	a := instructionAddress{messageID: instruction.Envelope.MessageID}
	switch p := instruction.Envelope.Payload.(type) {
	case protocol.ExecutePayload:
		a.sessionID, a.bindingID, a.useID, a.taskID = p.SessionID, p.BindingID, p.ToolUseEventID, p.TaskID
	case protocol.StdinPayload:
		a.sessionID, a.bindingID, a.useID, a.taskID = p.SessionID, p.BindingID, p.ToolUseEventID, p.TaskID
	case protocol.CancelPayload:
		a.sessionID, a.bindingID, a.useID, a.taskID = p.SessionID, p.BindingID, p.ToolUseEventID, p.TaskID
	}
	return a
}

// reportJSON is the JSON of a report with messageID answering instruction
// on connectionID, whose members after the address are rest.
func reportJSON(connectionID, messageID string, instruction enginetest.Event, rest string) []byte {
	a := addressOf(instruction)
	return []byte(`{"protocol_version":1,"connection_id":` + quoteJSON(connectionID) + `,"message_id":` + quoteJSON(messageID) +
		`,"session_id":` + quoteJSON(a.sessionID) + `,"binding_id":` + quoteJSON(a.bindingID) + `,"tool_use_event_id":` + quoteJSON(a.useID) +
		`,"task_id":` + quoteJSON(a.taskID) + `,"request_message_id":` + quoteJSON(a.messageID) + `,` + rest + `}`)
}

// execEnvelope is an exec_command result envelope of status ("success",
// "error" or "running") for taskID, with stdout and metrics when terminal.
func execEnvelope(status, taskID, stdout string) string {
	errorJSON := "null"
	if status == "error" {
		errorJSON = `{"kind":"command_failed","message":"exit 1"}`
	}
	envelope := `{"schema_version":1,"tool":"exec_command","status":"` + status + `","truncated":false,"error":` + errorJSON +
		`,"result":{"task_id":` + quoteJSON(taskID) + `,"exit_code":` + map[bool]string{true: "null", false: "0"}[status == "running"] +
		`,"signal":null,"timed_out":false,"stdout":{"text":` + quoteJSON(stdout) + `,"total_bytes":` + fmt.Sprint(len(stdout)) +
		`,"total_lines":1,"returned_bytes":` + fmt.Sprint(len(stdout)) + `,"truncated":false},` +
		`"stderr":{"text":"","total_bytes":0,"total_lines":0,"returned_bytes":0,"truncated":false},"duration_ms":12}`
	if status != "running" {
		envelope += `,"metrics":{"wall_time_ms":1250,"queued_ms":3,"drive_wait_ms":40,"run_ms":1180,"sync_ms":20}`
	}
	return envelope + `}`
}

// execution is an execution report body of phase with envelope ("" for a
// rejection, whose code is then rest).
func execution(connectionID, messageID string, instruction enginetest.Event, phase string, seq int, envelope string) []byte {
	return reportJSON(connectionID, messageID, instruction, `"kind":"execution","phase":"`+phase+`","report_seq":`+fmt.Sprint(seq)+`,"result":`+envelope)
}

func rejection(connectionID, messageID string, instruction enginetest.Event, code string) []byte {
	return reportJSON(connectionID, messageID, instruction, `"kind":"execution","phase":"rejected","error":{"code":"`+code+`","message":"refused: `+code+`"}`)
}

// toolStatus posts body to POST /v1/client/tool-status.
func (s *cliStream) toolStatus(body []byte) enginetest.Response {
	s.f.t.Helper()
	response, err := s.cli.Post(s.f.ctx, "/v1/client/tool-status", body)
	if err != nil {
		s.f.t.Fatal(err)
	}
	return response
}

// acked posts body and requires its ACK for messageID.
func (s *cliStream) acked(body []byte, messageID string) enginetest.Response {
	s.f.t.Helper()
	response := s.toolStatus(body)
	if ack, err := response.Acknowledgement(); err != nil || ack.MessageID != messageID || ack.ConnectionID != s.connectionID {
		s.f.t.Fatalf("report %s = %d %s, %v; want its ACK", messageID, response.Status, response.Body, err)
	}
	return response
}

// refused posts body and requires status and code with nothing stored.
func (s *cliStream) refused(body []byte, status int, code protocol.ErrorCode) {
	s.f.t.Helper()
	before := s.f.count(`SELECT count(*) FROM session_tool_reports`)
	response := s.toolStatus(body)
	failure, err := response.Error()
	if response.Status != status || err != nil || failure.Code != code {
		s.f.t.Fatalf("report = %d %s; want %d %s", response.Status, response.Body, status, code)
	}
	if after := s.f.count(`SELECT count(*) FROM session_tool_reports`); after != before {
		s.f.t.Fatalf("a refused report stored %d rows", after-before)
	}
}

// resultNotices counts the tool execution result notices PostgreSQL
// delivers, by hint.
type resultNotices struct {
	mu   sync.Mutex
	seen map[resultnotify.ExecutionResultHint]int
}

// listenForResults listens on the result channel until the test ends.
func (f *machineTools) listenForResults() *resultNotices {
	f.t.Helper()
	n := &resultNotices{seen: map[resultnotify.ExecutionResultHint]int{}}
	ctx, cancel := context.WithCancel(f.ctx)
	ready, done := make(chan struct{}), make(chan error, 1)
	go func() {
		done <- f.owner.Listen(ctx, "test.listen_tool_results", resultnotify.ExecutionResultNotificationChannel, func() { close(ready) }, func(payload string) {
			if hint, ok := resultnotify.ParseExecutionResultHint(payload); ok {
				n.mu.Lock()
				n.seen[hint]++
				n.mu.Unlock()
			}
		})
	}()
	f.t.Cleanup(func() { cancel(); <-done })
	select {
	case <-ready:
	case err := <-done:
		f.t.Fatalf("listen for tool results: %v", err)
	}
	return n
}

// require waits for want notices of hint and then for no more.
func (n *resultNotices) require(t *testing.T, hint resultnotify.ExecutionResultHint, want int) {
	t.Helper()
	count := func() int { n.mu.Lock(); defer n.mu.Unlock(); return n.seen[hint] }
	for deadline := time.Now().Add(5 * time.Second); count() < want && time.Now().Before(deadline); {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	if got := count(); got != want {
		t.Fatalf("notices for %s = %d; want %d", hint.ToolUseEventID, got, want)
	}
}

// hints are the notice hints of the Session's calls uses.
func (s *toolSession) hints(uses ...string) []resultnotify.ExecutionResultHint {
	hints := make([]resultnotify.ExecutionResultHint, 0, len(uses))
	for _, use := range uses {
		hints = append(hints, resultnotify.ExecutionResultHint{WorkspaceID: string(s.f.ws), SessionID: s.id,
			SessionThreadID: s.scope.GetSessionThreadId(), ToolUseEventID: use})
	}
	return hints
}
