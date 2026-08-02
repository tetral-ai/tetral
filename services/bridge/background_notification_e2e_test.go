package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	internalenvironment "github.com/tetral-ai/tetral/internal/environment"
	internalgrpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxmodel "github.com/tetral-ai/tetral/internal/sandbox"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestBackgroundTaskNotificationSurvivesQueueLossThroughProductionDeliveryRoute(t *testing.T) {
	runtimeDB, adminDB := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		workspaceID  = "default"
		sessionID    = "sesn_background_notification_e2e"
		threadID     = "thr_background_notification_e2e"
		bindingID    = "bind_background_notification_e2e"
		podUID       = "pod_background_notification_e2e"
		toolUseID    = "evt_background_notification_e2e"
		modelCallID  = "call_background_notification_e2e"
		modelRequest = "mreq_background_notification_e2e"
		taskID       = "task_background_notification_e2e"
	)
	client := dbconnect.NewClientForTesting(runtimeDB)
	seedBackgroundNotificationSession(t, client, adminDB, workspaceID, sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, adminDB, workspaceID, sessionID, bindingID, 1, podUID)
	seedBridgeAPIEvent(t, adminDB, workspaceID, sessionID, threadID, toolUseID, 1, "agent.tool_use", `{"name":"exec_command","input":{},"evaluated_permission":"allow"}`)
	if _, err := adminDB.Exec(`UPDATE session_events SET model_request_id=$2 WHERE workspace_id=$1 AND event_id=$3`, workspaceID, modelRequest, toolUseID); err != nil {
		t.Fatalf("stamp source Tool Use model request: %v", err)
	}
	seedBridgeAPIDurableToolMessage(t, adminDB, workspaceID, sessionID, threadID, modelRequest, toolUseID, modelCallID, "exec_command")

	queueStore := queue.NewPostgreSQLStore(client)
	queueConnection := startBackgroundNotificationQueueServer(t, queueStore)
	queueRPC := queuev1.NewQueueServiceClient(queueConnection)
	sandboxQueue := tetralsandbox.SandboxQueueFromGRPC(queueRPC)
	bridgeQueue := QueueClientFromGRPC(queueRPC)
	provider := &backgroundNotificationProvider{taskID: taskID}
	registry, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{sandboxdriver.DaytonaProviderName: provider})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	apiStore := NewPostgreSQLBridgeAPIStore(client)
	accepted, err := apiStore.AcceptSandboxExecution(context.Background(), &bridgev1.AcceptSandboxExecutionRequest{
		Scope:          bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID),
		ToolUseEventId: toolUseID, ModelToolCallId: modelCallID,
		NormalizedInputHash: sha256Hex(`{}`), ToolName: "exec_command", InputJson: `{}`,
	})
	if err != nil || accepted.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("AcceptSandboxExecution = %#v/%v; want committed", accepted, err)
	}
	lifecycleStore := tetralsandbox.NewPostgreSQLSandboxLifecycleStore(
		client, sandboxmodel.NewPostgreSQLStore(client), 30*time.Minute,
	)
	executionRunner := &tetralsandbox.SandboxToolExecutionJobRunner{
		Queue:       sandboxQueue,
		Coordinator: tetralsandbox.NewPostgreSQLSandboxExecutionCoordinator(client, 30*time.Minute),
		Providers:   registry,
		Media:       backgroundNotificationMedia{},
		Config: tetralsandbox.SandboxToolExecutionRunnerConfig{
			WorkspaceID: workspaceID, LeaseOwner: "sandbox-background-e2e", MaxJobs: 1,
			LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second, PreparationTimeout: 45 * time.Second,
		},
	}
	if err := executionRunner.RunOnce(context.Background()); err != nil {
		t.Fatalf("normalize source Sandbox execution into activation: %v", err)
	}
	lifecycleConfig := tetralsandbox.SandboxLifecycleRunnerConfig{
		WorkspaceID: workspaceID, LeaseOwner: "sandbox-background-e2e", MaxJobs: 1,
		LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second,
	}
	if err := (&tetralsandbox.SandboxActivationJobRunner{
		Queue: sandboxQueue, Store: lifecycleStore, Providers: registry, Config: lifecycleConfig,
	}).RunOnce(context.Background()); err != nil {
		t.Fatalf("activate source Sandbox: %v", err)
	}
	if err := (&tetralsandbox.SandboxMaterializationJobRunner{
		Queue: sandboxQueue, Store: lifecycleStore, Providers: registry, Config: lifecycleConfig,
	}).RunOnce(context.Background()); err != nil {
		t.Fatalf("materialize source Sandbox: %v", err)
	}
	if err := executionRunner.RunOnce(context.Background()); err != nil {
		t.Fatalf("run source Sandbox execution after lifecycle convergence: %v", err)
	}
	reconcileRunner := &tetralsandbox.SandboxBackgroundReconcileJobRunner{
		Queue:     sandboxQueue,
		Store:     tetralsandbox.NewPostgreSQLSandboxBackgroundCommandStore(client),
		Providers: registry,
		Config: tetralsandbox.SandboxBackgroundRunnerConfig{
			WorkspaceID: workspaceID, LeaseOwner: "sandbox-background-e2e", MaxJobs: 1,
			LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second,
		},
	}
	if err := reconcileRunner.RunOnce(context.Background()); err != nil {
		t.Fatalf("run terminal background reconciliation: %v", err)
	}
	assertBackgroundNotificationBornBindingNeutral(t, adminDB, workspaceID, sessionID, "task_notification:"+taskID)
	runtimeInputID := "task_notification:" + taskID
	var initialQueueJobID string
	var initialQueueJobs int
	if err := adminDB.QueryRow(`SELECT min(id), count(*) FROM queue_jobs
		WHERE workspace_id=$1 AND kind=$2 AND dedupe_key=$3 AND status='pending'`,
		workspaceID, queue.KindRuntimeInput, queue.FormatRuntimeInputDedupeKey(workspaceID, sessionID, runtimeInputID),
	).Scan(&initialQueueJobID, &initialQueueJobs); err != nil {
		t.Fatalf("read first notification Queue job: %v", err)
	}
	if initialQueueJobs != 1 {
		t.Fatalf("first notification Queue jobs = %d; want 1", initialQueueJobs)
	}
	if result, err := adminDB.Exec(`DELETE FROM queue_jobs WHERE workspace_id=$1 AND id=$2`, workspaceID, initialQueueJobID); err != nil {
		t.Fatalf("delete first notification Queue transport: %v", err)
	} else if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		t.Fatalf("deleted notification Queue rows = %d/%v; want 1", rows, err)
	}

	runtimeEndpoint, runtimePort := startTaskNotificationRuntimeEndpoint(t)
	if _, err := adminDB.Exec(`UPDATE session_runtime_bindings
		SET agent_runtime_pod_ip='127.0.0.1', updated_at=clock_timestamp()
		WHERE workspace_id=$1 AND session_id=$2`, workspaceID, sessionID); err != nil {
		t.Fatalf("point Runtime binding at recording endpoint: %v", err)
	}
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(client, runtimePort)
	if repaired, err := deliveryStore.RepairRuntimeInbox(context.Background(), workspaceID, 10); err != nil || repaired != 1 {
		t.Fatalf("RepairRuntimeInbox after transport loss = %d/%v; want 1/nil", repaired, err)
	}
	deliverer := RuntimePodDirectDeliverer{
		Store:  deliveryStore,
		Sender: NewRuntimePodCommandClient(backgroundNotificationTokenSource{}, grpc.WithTransportCredentials(insecure.NewCredentials())),
	}
	jobRunner := &JobRunner{
		Queue: bridgeQueue, Workspaces: staticWorkspaceLister{workspace.ID(workspaceID)}, Deliverer: deliverer,
		Config: JobRunnerConfig{LeaseOwner: "bridge-background-e2e", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second},
	}
	if err := jobRunner.RunOnce(context.Background()); err != nil {
		t.Fatalf("run production Bridge Job Runner: %v", err)
	}
	var captured *agentruntimev1.RuntimeInputCommandRequest
	select {
	case captured = <-runtimeEndpoint.requests:
	case <-time.After(5 * time.Second):
		t.Fatal("Runtime endpoint did not receive task notification")
	}
	if captured.GetCommandKind() != agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_TASK_NOTIFICATION {
		t.Fatalf("Runtime command kind = %s; want TASK_NOTIFICATION", captured.GetCommandKind())
	}
	if runtimeEndpoint.route != "AcceptTaskNotification" {
		t.Fatalf("Runtime endpoint route = %q; want AcceptTaskNotification", runtimeEndpoint.route)
	}
	var payload struct {
		TaskID               string `json:"task_id"`
		SourceToolUseEventID string `json:"source_tool_use_event_id"`
	}
	if err := json.Unmarshal([]byte(captured.GetPayloadJson()), &payload); err != nil {
		t.Fatalf("decode captured task notification payload: %v", err)
	}
	if payload.TaskID != taskID || payload.SourceToolUseEventID != toolUseID || captured.GetRuntimeInputId() != runtimeInputID {
		t.Fatalf("captured task/input identity = %q/%q/%q; want %q/%q/%q",
			payload.TaskID, payload.SourceToolUseEventID, captured.GetRuntimeInputId(), taskID, toolUseID, runtimeInputID)
	}
	commitRequest := bridgeTaskNotificationRequestForTest(
		t, runtimeScopeFromCommandRequest(captured), captured.GetRuntimeInputId(), payload.TaskID, captured.GetPayloadJson(),
	)
	committed, err := apiStore.CommitTaskNotificationResult(context.Background(), commitRequest)
	if err != nil || committed.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("CommitTaskNotificationResult = %#v/%v; want committed", committed, err)
	}
	receipts := committed.GetDeclaration().GetReceipts()
	if len(receipts) != 1 {
		t.Fatalf("task notification declaration receipts = %d; want 1", len(receipts))
	}
	if len(receipts[0].GetEvents()) != 1 {
		t.Fatalf("task notification declaration events = %d; want 1", len(receipts[0].GetEvents()))
	}
	assertBackgroundNotificationSettlement(
		t, adminDB, workspaceID, sessionID, runtimeInputID, taskID, receipts[0].GetEvents()[0].GetEventId(),
	)
	if repaired, err := deliveryStore.RepairRuntimeInbox(context.Background(), workspaceID, 10); err != nil || repaired != 0 {
		t.Fatalf("second RepairRuntimeInbox = %d/%v; want 0/nil", repaired, err)
	}
}

func seedBackgroundNotificationSession(
	t *testing.T,
	client *dbconnect.Client,
	db *sql.DB,
	workspaceID string,
	sessionID string,
	threadID string,
) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO workspaces (id, type, name, created_at)
		VALUES ($1, 'workspace', $1, clock_timestamp()) ON CONFLICT (id) DO NOTHING`, workspaceID); err != nil {
		t.Fatalf("seed background workspace: %v", err)
	}
	environmentStore := internalenvironment.NewPostgreSQLEnvironmentStore(
		client, internalenvironment.WithDefaultArtifactRef("artifact_background_notification_e2e"),
	)
	environment, err := environmentStore.Create(context.Background(), workspace.ID(workspaceID), internalenvironment.CreateEnvironmentRequest{
		Name: "background-notification-e2e",
	})
	if err != nil {
		t.Fatalf("create background environment: %v", err)
	}
	agentID := "agent_" + sessionID
	if _, err := db.Exec(`INSERT INTO agents (workspace_id, id, name, version, created_at, updated_at)
		VALUES ($1,$2,$2,1,clock_timestamp(),clock_timestamp())`, workspaceID, agentID); err != nil {
		t.Fatalf("seed background agent: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at)
		VALUES ($1,$2,$3,1,'{"tools":[{"type":"tetral_agent_toolset","family":"claude"}]}',$4,clock_timestamp())`,
		workspaceID, "agv_"+sessionID, agentID, "hash_"+sessionID); err != nil {
		t.Fatalf("seed background agent version: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO sessions (
		workspace_id,id,main_thread_id,type,status,lifecycle_state,agent_id,agent_version,environment_id,
		installed_tools_json,created_at,updated_at
	) VALUES ($1,$2,$3,'session','idle','active',$4,1,$5,
		'{"tools":[{"type":"tetral_agent_toolset","family":"claude"}]}',clock_timestamp(),clock_timestamp())`,
		workspaceID, sessionID, threadID, agentID, environment.ID); err != nil {
		t.Fatalf("seed background Session: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO session_threads (
		workspace_id,id,session_id,role,visibility,status,created_at,last_active_at,updated_at
	) VALUES ($1,$2,$3,'main','public','idle',clock_timestamp(),clock_timestamp(),clock_timestamp())`,
		workspaceID, threadID, sessionID); err != nil {
		t.Fatalf("seed background thread: %v", err)
	}
}

func startBackgroundNotificationQueueServer(t *testing.T, store *queue.PostgreSQLQueueStore) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(2 * 1024 * 1024)
	server := grpc.NewServer()
	queuev1.RegisterQueueServiceServer(server, tetralqueue.NewServer(store))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	connection, err := grpc.NewClient("passthrough:///background-notification-queue",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
	)
	if err != nil {
		t.Fatalf("dial background notification Queue: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return connection
}

func startTaskNotificationRuntimeEndpoint(t *testing.T) (*recordingTaskNotificationRuntime, int) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for Runtime endpoint: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	endpoint := &recordingTaskNotificationRuntime{requests: make(chan *agentruntimev1.RuntimeInputCommandRequest, 1)}
	server := grpc.NewServer()
	agentruntimev1.RegisterAgentRuntimePodServiceServer(server, endpoint)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	return endpoint, port
}

type recordingTaskNotificationRuntime struct {
	agentruntimev1.UnimplementedAgentRuntimePodServiceServer
	requests chan *agentruntimev1.RuntimeInputCommandRequest
	route    string
}

func (r *recordingTaskNotificationRuntime) AcceptTaskNotification(_ context.Context, request *agentruntimev1.RuntimeInputCommandRequest) (*agentruntimev1.RuntimeInputCommandResponse, error) {
	r.route = "AcceptTaskNotification"
	r.requests <- request
	return &agentruntimev1.RuntimeInputCommandResponse{
		Status:    agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED,
		SessionId: request.GetSessionId(), RuntimeInputId: request.GetRuntimeInputId(),
		BindingId: request.GetBindingId(), BindingGeneration: request.GetBindingGeneration(),
	}, nil
}

type backgroundNotificationTokenSource struct{}

func (backgroundNotificationTokenSource) Token(context.Context) (string, error) {
	return "test-token", nil
}

var _ internalgrpcauth.TokenSource = backgroundNotificationTokenSource{}

type backgroundNotificationMedia struct{}

func (backgroundNotificationMedia) MaterializeResult(_ context.Context, _ tetralsandbox.SandboxExecutionRef, _, _, result string, _ time.Time) (string, error) {
	return result, nil
}

func (backgroundNotificationMedia) RecoverResult(context.Context, tetralsandbox.SandboxExecutionRef) (tetralsandbox.SandboxMediaRecovery, error) {
	return tetralsandbox.SandboxMediaRecovery{}, nil
}

type backgroundNotificationProvider struct {
	taskID string
}

func (*backgroundNotificationProvider) InspectForExecution(context.Context, string) tetralsandbox.ProviderOutcome[tetralsandbox.ExecutionReadiness] {
	return tetralsandbox.ProviderOutcome[tetralsandbox.ExecutionReadiness]{Value: tetralsandbox.ExecutionReady}
}
func (*backgroundNotificationProvider) InspectForRelease(context.Context, string) tetralsandbox.ProviderOutcome[bool] {
	return tetralsandbox.ProviderOutcome[bool]{Value: true}
}
func (*backgroundNotificationProvider) ResolveActivation(context.Context, tetralsandbox.ActivationResolutionRequest) tetralsandbox.ProviderOutcome[tetralsandbox.ActivationResolution] {
	return tetralsandbox.ProviderOutcome[tetralsandbox.ActivationResolution]{Value: tetralsandbox.ActivationResolution{Found: false}}
}
func (*backgroundNotificationProvider) Activate(context.Context, tetralsandbox.ActivationRequest) tetralsandbox.ProviderOutcome[sandboxmodel.ProviderHandle] {
	return tetralsandbox.ProviderOutcome[sandboxmodel.ProviderHandle]{Value: sandboxmodel.ProviderHandle{
		Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider_background_notification_e2e",
	}}
}
func (*backgroundNotificationProvider) MaterializeResources(_ context.Context, request tetralsandbox.MaterializationRequest) tetralsandbox.ProviderOutcome[tetralsandbox.MaterializationResult] {
	expiresAt := time.Now().UTC().Add(2 * time.Hour)
	return tetralsandbox.ProviderOutcome[tetralsandbox.MaterializationResult]{Value: tetralsandbox.MaterializationResult{
		MaterializedEnvironmentGeneration: request.TargetEnvironmentGeneration,
		MaterializedResourceRevision:      request.TargetResourceRevision,
		Resources: sandboxmodel.ResourceSetup{
			ResourceCredExpiresAt: &expiresAt,
			ResourceRootsJSON:     `[]`,
		},
	}}
}
func (*backgroundNotificationProvider) PrepareTool(context.Context, tetralsandbox.ToolExecutionRequest) tetralsandbox.ProviderOutcome[tetralsandbox.ToolPreparationResult] {
	return tetralsandbox.ProviderOutcome[tetralsandbox.ToolPreparationResult]{Value: tetralsandbox.ToolPreparationResult{}}
}
func (p *backgroundNotificationProvider) ExecuteTool(context.Context, tetralsandbox.ToolExecutionRequest) tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution] {
	return tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution]{Value: sandboxdriver.ToolExecution{
		ResultJSON: `{"status":"running","result":{"task_id":"` + p.taskID + `"}}`,
		BackgroundTask: &sandboxdriver.BackgroundTask{
			TaskID: p.taskID, ProviderSessionID: "provider_background_e2e",
			ProviderCommandID: p.taskID, ProviderCommandMetadataJSON: `{}`,
		},
	}}
}
func (*backgroundNotificationProvider) ObserveTool(context.Context, sandboxdriver.ForegroundCommandObservation) tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution] {
	return tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution]{}
}
func (*backgroundNotificationProvider) Release(context.Context, tetralsandbox.ReleaseRequest) tetralsandbox.ProviderOutcome[tetralsandbox.ReleaseResult] {
	return tetralsandbox.ProviderOutcome[tetralsandbox.ReleaseResult]{}
}
func (*backgroundNotificationProvider) PollBackground(context.Context, sandboxdriver.CommandReference) tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult] {
	return tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult]{Value: sandboxdriver.CommandResult{
		ResultJSON:     `{"status":"completed","result":{"stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false},"exit_code":0}}`,
		TerminalStatus: "completed",
	}}
}
func (*backgroundNotificationProvider) SendBackgroundInput(context.Context, sandboxdriver.CommandInput) tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult] {
	return tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult]{}
}
func (*backgroundNotificationProvider) CancelBackground(context.Context, sandboxdriver.CommandCancel) tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult] {
	return tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult]{}
}

func assertBackgroundNotificationBornBindingNeutral(t *testing.T, db *sql.DB, workspaceID string, sessionID string, runtimeInputID string) {
	t.Helper()
	var (
		statusValue       string
		eventIDsJSON      string
		bindingID         sql.NullString
		bindingGeneration sql.NullInt64
		targetPodUID      sql.NullString
	)
	if err := db.QueryRow(`SELECT status, event_ids_json, binding_id, binding_generation, target_pod_uid
		FROM session_runtime_inbox
		WHERE workspace_id=$1 AND session_id=$2 AND runtime_input_id=$3`, workspaceID, sessionID, runtimeInputID).Scan(
		&statusValue, &eventIDsJSON, &bindingID, &bindingGeneration, &targetPodUID,
	); err != nil {
		t.Fatalf("read binding-neutral background inbox birth: %v", err)
	}
	if statusValue != "queued" || eventIDsJSON != "[]" || bindingID.Valid || bindingGeneration.Valid || targetPodUID.Valid {
		t.Fatalf("background inbox birth = status %q events %q binding %v/%v pod %v; want queued/[]/unbound",
			statusValue, eventIDsJSON, bindingID, bindingGeneration, targetPodUID)
	}
}

func assertBackgroundNotificationSettlement(
	t *testing.T,
	db *sql.DB,
	workspaceID string,
	sessionID string,
	runtimeInputID string,
	taskID string,
	wantTerminalEventID string,
) {
	t.Helper()
	var inboxStatus string
	var terminalEventID sql.NullString
	if err := db.QueryRow(`SELECT i.status, t.terminal_event_id
		FROM session_runtime_inbox i
		JOIN session_background_tasks t ON t.workspace_id=i.workspace_id AND t.session_id=i.session_id AND t.task_id=$4
		WHERE i.workspace_id=$1 AND i.session_id=$2 AND i.runtime_input_id=$3`, workspaceID, sessionID, runtimeInputID, taskID).Scan(&inboxStatus, &terminalEventID); err != nil {
		t.Fatalf("read background notification settlement: %v", err)
	}
	var events, exactEvents int
	if err := db.QueryRow(`SELECT count(*), count(*) FILTER (WHERE event_id=$3) FROM session_events
		WHERE workspace_id=$1 AND session_id=$2 AND type='runtime_notification'`, workspaceID, sessionID, wantTerminalEventID).Scan(&events, &exactEvents); err != nil {
		t.Fatalf("count background notification events: %v", err)
	}
	if inboxStatus != "committed" || !terminalEventID.Valid || terminalEventID.String != wantTerminalEventID || events != 1 || exactEvents != 1 {
		t.Fatalf("background notification settlement = inbox %q terminal %q events %d exact %d; want committed/%q/1/1",
			inboxStatus, terminalEventID.String, events, exactEvents, wantTerminalEventID)
	}
}

var _ tetralsandbox.ProviderAdapter = (*backgroundNotificationProvider)(nil)
var _ tetralsandbox.BackgroundCommandAdapter = (*backgroundNotificationProvider)(nil)
