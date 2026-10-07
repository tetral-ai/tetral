package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"reflect"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	internalsandbox "github.com/tetral-ai/tetral/internal/sandbox"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"
)

type taskNotificationRuntimeCompositionOutput struct {
	Declaration         json.RawMessage   `json:"declaration"`
	AcceptResult        json.RawMessage   `json:"acceptResult"`
	CommitResult        json.RawMessage   `json:"commitResult"`
	ProviderInvocations int               `json:"providerInvocations"`
	RequestEndCount     int               `json:"requestEndCount"`
	ProviderContexts    []json.RawMessage `json:"providerContexts"`
}

type terminalBackgroundProvider struct {
	result sandboxdriver.CommandResult
}

func (p terminalBackgroundProvider) PollBackground(context.Context, sandboxdriver.CommandReference) tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult] {
	return tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult]{Value: p.result}
}

func (terminalBackgroundProvider) SendBackgroundInput(context.Context, sandboxdriver.CommandInput) tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult] {
	return tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult]{}
}

func (terminalBackgroundProvider) CancelBackground(context.Context, sandboxdriver.CommandCancel) tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult] {
	return tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult]{}
}

func (terminalBackgroundProvider) InspectForExecution(context.Context, string) tetralsandbox.ProviderOutcome[tetralsandbox.ExecutionReadiness] {
	return tetralsandbox.ProviderOutcome[tetralsandbox.ExecutionReadiness]{}
}

func (terminalBackgroundProvider) InspectForRelease(context.Context, string) tetralsandbox.ProviderOutcome[bool] {
	return tetralsandbox.ProviderOutcome[bool]{}
}

func (terminalBackgroundProvider) ResolveActivation(context.Context, tetralsandbox.ActivationResolutionRequest) tetralsandbox.ProviderOutcome[tetralsandbox.ActivationResolution] {
	return tetralsandbox.ProviderOutcome[tetralsandbox.ActivationResolution]{}
}

func (terminalBackgroundProvider) Activate(context.Context, tetralsandbox.ActivationRequest) tetralsandbox.ProviderOutcome[internalsandbox.ProviderHandle] {
	return tetralsandbox.ProviderOutcome[internalsandbox.ProviderHandle]{}
}

func (terminalBackgroundProvider) MaterializeResources(context.Context, tetralsandbox.MaterializationRequest) tetralsandbox.ProviderOutcome[tetralsandbox.MaterializationResult] {
	return tetralsandbox.ProviderOutcome[tetralsandbox.MaterializationResult]{}
}

func (terminalBackgroundProvider) PrepareTool(context.Context, tetralsandbox.ToolExecutionRequest) tetralsandbox.ProviderOutcome[tetralsandbox.ToolPreparationResult] {
	return tetralsandbox.ProviderOutcome[tetralsandbox.ToolPreparationResult]{}
}

func (terminalBackgroundProvider) ExecuteTool(context.Context, tetralsandbox.ToolExecutionRequest) tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution] {
	return tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution]{}
}

func (terminalBackgroundProvider) ObserveTool(context.Context, sandboxdriver.ForegroundCommandObservation) tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution] {
	return tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution]{}
}

func (terminalBackgroundProvider) Release(context.Context, tetralsandbox.ReleaseRequest) tetralsandbox.ProviderOutcome[tetralsandbox.ReleaseResult] {
	return tetralsandbox.ProviderOutcome[tetralsandbox.ReleaseResult]{}
}

type runningTaskNotificationRuntime struct {
	command *exec.Cmd
	output  syncBuffer
	port    int
}

func startTaskNotificationRuntimeComposition(t *testing.T, inputPath string, request *agentruntimev1.AcceptTaskNotificationRequest, bridgeAddress string, requestStartRace bool) *runningTaskNotificationRuntime {
	t.Helper()
	readyPath := t.TempDir() + "/runtime-ready.json"
	input := map[string]any{
		"notificationJson": request.GetNotificationJson(), "workspaceId": request.GetWorkspaceId(),
		"sessionId": request.GetSessionId(), "sessionThreadId": request.GetSessionThreadId(),
		"bindingId": request.GetBindingId(), "bindingGeneration": request.GetBindingGeneration(),
		"targetPodUid": request.GetTargetPodUid(), "runtimeProcessId": request.GetRuntimeProcessId(), "runtimeInputId": request.GetRuntimeInputId(),
		"inputOrder": request.GetInputOrder(), "bridgeAddress": bridgeAddress, "readyPath": readyPath,
		"requestStartRace": requestStartRace,
	}
	rawInput, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("encode resident Runtime composition input: %v", err)
	}
	if err := os.WriteFile(inputPath, rawInput, 0o600); err != nil {
		t.Fatalf("write resident Runtime composition input: %v", err)
	}
	running := &runningTaskNotificationRuntime{}
	running.command = exec.Command("bun", "packages/runtime-pod/test/fixtures/task-notification-composition.ts", inputPath) //nolint:gosec // Fixed repository fixture and test-owned input.
	running.command.Dir = "../services/agent-runtime"
	running.command.Stdout = &running.output
	running.command.Stderr = &running.output
	if err := running.command.Start(); err != nil {
		t.Fatalf("start resident Runtime composition: %v", err)
	}
	t.Cleanup(func() {
		if running.command.ProcessState == nil {
			_ = running.command.Process.Kill()
			_ = running.command.Wait()
		}
	})
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		rawReady, readErr := os.ReadFile(readyPath)
		if readErr == nil {
			var ready struct {
				Port int `json:"port"`
			}
			if json.Unmarshal(rawReady, &ready) == nil && ready.Port > 0 {
				running.port = ready.Port
				return running
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	_ = running.command.Process.Kill()
	_ = running.command.Wait()
	t.Fatalf("resident Runtime did not become ready: %s", running.output.String())
	return nil
}

func (r *runningTaskNotificationRuntime) wait(t *testing.T) taskNotificationRuntimeCompositionOutput {
	t.Helper()
	joined := make(chan error, 1)
	go func() { joined <- r.command.Wait() }()
	var joinErr error
	select {
	case joinErr = <-joined:
	case <-time.After(20 * time.Second):
		_ = r.command.Process.Kill()
		joinErr = <-joined
		t.Fatalf("resident Runtime did not join within 20s: %v: %s", joinErr, r.output.String())
	}
	if joinErr != nil {
		t.Fatalf("resident Runtime composition: %v: %s", joinErr, r.output.String())
	}
	var composed taskNotificationRuntimeCompositionOutput
	if err := json.Unmarshal(r.output.Bytes(), &composed); err != nil {
		t.Fatalf("decode resident Runtime composition: %v: %s", err, r.output.String())
	}
	return composed
}

func (r *runningTaskNotificationRuntime) waitWithOutputCapture(t *testing.T, runtime, admin *sql.DB, sessionID, threadID string) taskNotificationRuntimeCompositionOutput {
	t.Helper()
	// A successful Request End precedes FinishIdle. Select the actual turn's
	// durable capture before serving its Queue owner; arbitrary one-shot work
	// could instead acknowledge a prior capture without releasing this turn.
	var writeID string
	var generation int
	deadline := time.Now().Add(10 * time.Second)
	for {
		err := admin.QueryRowContext(context.Background(), `SELECT finish_idle_write_id,capture_generation
			FROM sandbox_output_capture_operations WHERE workspace_id='default'
			AND session_id=$1 AND session_thread_id=$2 AND state='pending'`, sessionID, threadID).Scan(&writeID, &generation)
		if err == nil {
			break
		}
		if !errors.Is(err, sql.ErrNoRows) || time.Now().After(deadline) {
			t.Fatalf("task-notification turn did not enqueue output capture: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	var queued int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM queue_jobs
		WHERE workspace_id='default' AND kind=$1 AND status='pending'
		AND payload_json::jsonb->>'session_id'=$2
		AND payload_json::jsonb->>'finish_idle_write_id'=$3
		AND (payload_json::jsonb->>'capture_generation')::int=$4`,
		queue.KindSandboxOutputCapture, sessionID, writeID, generation).Scan(&queued); err != nil || queued != 1 {
		t.Fatalf("task-notification output-capture Queue ownership = %d/%v", queued, err)
	}
	providers, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{
		sandboxdriver.DaytonaProviderName: handoffCaptureProvider{bridgeMemoryProjectionProvider: &bridgeMemoryProjectionProvider{}},
	})
	if err != nil {
		t.Fatalf("build task-notification capture provider registry: %v", err)
	}
	worker := &tetralsandbox.SandboxOutputCaptureJobRunner{
		Queue:     tetralqueue.NewServer(queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime)), nil),
		Store:     tetralsandbox.NewPostgreSQLSandboxOutputCaptureStore(dbconnect.NewClientForTesting(runtime)),
		Providers: providers, BlobStore: blob.NewFakeBlobStore(),
		Config: tetralsandbox.SandboxOutputCaptureRunnerConfig{
			WorkspaceID: "default", LeaseOwner: "task-notification-output-capture", MaxJobs: 1,
			LeaseDuration: time.Minute, HeartbeatInterval: time.Second,
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	workerDone := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			if _, err := worker.RunOnceWithActivity(ctx); err != nil {
				if ctx.Err() != nil {
					err = nil
				}
				workerDone <- err
				return
			}
			select {
			case <-ctx.Done():
				workerDone <- nil
				return
			case <-ticker.C:
			}
		}
	}()
	defer func() {
		cancel()
		if err := <-workerDone; err != nil {
			t.Errorf("task-notification output-capture owner: %v", err)
		}
	}()
	composed := r.wait(t)
	var adopted int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM sandbox_output_capture_operations
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
		AND finish_idle_write_id=$3 AND capture_generation=$4 AND state='adopted'`,
		sessionID, threadID, writeID, generation).Scan(&adopted); err != nil || adopted != 1 {
		var facts string
		factsErr := admin.QueryRowContext(context.Background(), `SELECT jsonb_build_object(
			'capture',(SELECT jsonb_build_object('state',state,'failure_kind',failure_kind,
				'failure_detail',left(failure_detail,256)) FROM sandbox_output_capture_operations
				WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
				AND finish_idle_write_id=$3 AND capture_generation=$4),
			'queue',(SELECT COALESCE(jsonb_agg(jsonb_build_object('status',status)),'[]'::jsonb)
				FROM queue_jobs WHERE workspace_id='default' AND kind=$5
				AND payload_json::jsonb->>'session_id'=$1
				AND payload_json::jsonb->>'finish_idle_write_id'=$3
				AND (payload_json::jsonb->>'capture_generation')::int=$4))::text`,
			sessionID, threadID, writeID, generation, queue.KindSandboxOutputCapture).Scan(&facts)
		t.Fatalf("task-notification selected capture was not adopted before join: %d/%v; capture facts=%s/%v", adopted, err, facts, factsErr)
	}
	return composed
}

type taskNotificationRuntimeTokenSource struct{}

func (taskNotificationRuntimeTokenSource) Token(context.Context) (string, error) {
	return "task-notification-composition-token", nil
}

type taskNotificationLostACKStore struct {
	agentruntimebridge.BridgeAPIStore
	mu          sync.Mutex
	dropped     bool
	declaration *bridgev1.CommitTaskNotificationResultRequest
}

type taskNotificationRequestStartBarrierStore struct {
	agentruntimebridge.BridgeAPIStore
	mu         sync.Mutex
	startCount int
	entered    chan struct{}
	release    chan struct{}
}

func (s *taskNotificationRequestStartBarrierStore) WriteEvent(ctx context.Context, request *bridgev1.WriteEventRequest) (*bridgev1.WriteEventResponse, error) {
	response, err := s.BridgeAPIStore.WriteEvent(ctx, request)
	if err != nil || request.GetEventType() != "span.model_request_start" || (response.GetCommitted() == nil && response.GetDuplicate() == nil) {
		return response, err
	}
	s.mu.Lock()
	s.startCount++
	hold := s.startCount == 1
	if hold {
		close(s.entered)
	}
	s.mu.Unlock()
	if hold {
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return response, nil
}

func (s *taskNotificationLostACKStore) CommitTaskNotificationResult(ctx context.Context, request *bridgev1.CommitTaskNotificationResultRequest) (*bridgev1.CommitTaskNotificationResultResponse, error) {
	s.mu.Lock()
	s.declaration = request
	s.mu.Unlock()
	response, err := s.BridgeAPIStore.CommitTaskNotificationResult(ctx, request)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.dropped && response.GetCommitted() != nil {
		s.dropped = true
		return nil, status.Error(codes.Unavailable, "task notification acknowledgement unavailable")
	}
	return response, nil
}

func (s *taskNotificationLostACKStore) request() *bridgev1.CommitTaskNotificationResultRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.declaration
}

func (s *taskNotificationLostACKStore) didDrop() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dropped
}

func TestPostgreSQLTaskNotificationSettlesAcrossProducerRuntimeAndBridge(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_task_composition"
		threadID  = "thr_task_composition"
		bindingID = "bind_task_composition"
		podUID    = "pod_task_composition"
		taskID    = "task_composition"
		inputID   = "task_notification:task_composition"
		sourceID  = "evt_task_composition_source"
	)
	now := time.Now().UTC()
	sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPINotifiableBackgroundTask(t, admin, "default", sessionID, threadID, bindingID, taskID, sourceID)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events
		SET visibility='public', session_visible=true, model_request_id=$2
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$3`, sessionID, "mreq_"+sourceID, sourceID); err != nil {
		t.Fatalf("make background source Tool Use public: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_sandbox_bindings (
		workspace_id,session_id,logical_sandbox_id,environment_id,environment_generation,
		provider,provider_resource_id,binding_revision,materialized_resource_revision,
		resource_credential_expires_at,resource_roots_json,helper_verified_at,created_at,updated_at
	) VALUES ('default',$1,'sbox_task_composition',$2,1,
		'daytona','provider_task_composition',1,1,$3,'[]',$4,$4,$4)`,
		sessionID, "env_"+sessionID, now.Add(time.Hour), now); err != nil {
		t.Fatalf("seed Sandbox binding: %v", err)
	}
	storedResultJSON := `{"status":"completed","exit_code":0,"stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false}}`
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	reconcilePayload, err := json.Marshal(map[string]any{
		"workspace_id": "default", "session_id": sessionID, "task_id": taskID, "reconcile_generation": 1,
	})
	if err != nil {
		t.Fatalf("encode background reconcile job: %v", err)
	}
	if _, err := queueStore.Enqueue(context.Background(), queue.EnqueueRequest{
		ID: queue.NewJobID(), WorkspaceID: workspace.DefaultID, Kind: queue.KindSandboxBackgroundReconcile,
		PartitionKey:   queue.FormatSandboxBackgroundPartitionKey(workspace.DefaultID, sessionID, taskID),
		DedupeKey:      queue.FormatSandboxBackgroundReconcileDedupeKey(workspace.DefaultID, sessionID, taskID, 1),
		PayloadVersion: 1, PayloadJSON: reconcilePayload, MaxAttempts: queue.DefaultMaxAttempts, Now: now,
	}); err != nil {
		t.Fatalf("enqueue background reconcile job: %v", err)
	}
	provider, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{
		sandboxdriver.DaytonaProviderName: terminalBackgroundProvider{result: sandboxdriver.CommandResult{ResultJSON: storedResultJSON, TerminalStatus: "completed"}},
	})
	if err != nil {
		t.Fatalf("build background provider registry: %v", err)
	}
	sandboxRunner := &tetralsandbox.SandboxBackgroundReconcileJobRunner{
		Queue:     tetralqueue.NewServer(queueStore, nil),
		Store:     tetralsandbox.NewPostgreSQLSandboxBackgroundCommandStore(dbconnect.NewClientForTesting(runtime)),
		Providers: provider,
		Config:    tetralsandbox.SandboxBackgroundRunnerConfig{WorkspaceID: "default", LeaseOwner: "task-producer", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: 30 * time.Second},
		Clock:     func() time.Time { return now },
	}
	if active, err := sandboxRunner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("settle background task through producer = active:%t err:%v", active, err)
	}
	var bornInboxStatus, bornQueueStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT inbox.status, job.status
		FROM session_runtime_inbox inbox JOIN queue_jobs job
		  ON job.workspace_id=inbox.workspace_id AND job.dedupe_key=$2
		WHERE inbox.workspace_id='default' AND inbox.runtime_input_id=$1`,
		inputID, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, inputID),
	).Scan(&bornInboxStatus, &bornQueueStatus); err != nil {
		t.Fatalf("read producer-born notification custody: %v", err)
	}
	if bornInboxStatus != "queued" || bornQueueStatus != queue.StatusPending {
		t.Fatalf("producer-born notification custody = Inbox:%s Queue:%s", bornInboxStatus, bornQueueStatus)
	}

	bridgeStore := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	bridgeStore.RuntimeBindingTokenHMACKey = []byte("task-notification-composition-key")
	bridgeServerStore := &taskNotificationLostACKStore{BridgeAPIStore: bridgeStore}
	bridgeListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for task notification Bridge: %v", err)
	}
	bridgeGRPCServer := grpc.NewServer()
	agentruntimebridge.RegisterBridgeAPI(bridgeGRPCServer, bridgeServerStore)
	go func() { _ = bridgeGRPCServer.Serve(bridgeListener) }()
	t.Cleanup(func() {
		bridgeGRPCServer.Stop()
		_ = bridgeListener.Close()
	})

	runtimeRequest := &agentruntimev1.AcceptTaskNotificationRequest{
		WorkspaceId: "default", SessionId: sessionID, SessionThreadId: threadID,
		BindingId: bindingID, BindingGeneration: 1, TargetPodUid: podUID, RuntimeProcessId: "process_" + podUID,
		RuntimeInputId: inputID, InputOrder: 0,
		NotificationJson: mustCanonicalTaskNotificationPayloadJSON(t, taskID, sourceID, "completed", storedResultJSON),
	}
	runningRuntime := startTaskNotificationRuntimeComposition(t, t.TempDir()+"/task-notification-live.json", runtimeRequest, bridgeListener.Addr().String(), false)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_bindings SET agent_runtime_pod_ip='127.0.0.1'
		WHERE workspace_id='default' AND session_id=$1 AND binding_id=$2`, sessionID, bindingID); err != nil {
		t.Fatalf("align production Runtime visibility snapshot: %v", err)
	}
	seedFixtureRuntimeProcess(t, dbconnect.NewClientForTesting(admin), "tetral-agent-runtime", podUID)
	deliveryStore := jobrunner.NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), runningRuntime.port, jobrunner.KubernetesRuntimeTargetResolver{LoadClient: fixtureRuntimeLoadClient(t), Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{
			Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: podUID, PodIP: "127.0.0.1",
		}})
	}})
	runner := &jobrunner.JobRunner{
		Queue:      tetralqueue.NewServer(queueStore, nil),
		Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer: jobrunner.RuntimePodDirectDeliverer{
			Store: deliveryStore, Sender: fixtureRuntimeCommandClient(t, taskNotificationRuntimeTokenSource{}),
		},
		Config: jobrunner.JobRunnerConfig{LeaseOwner: "task-notification-composition", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	active, err := runner.RunOnceWithActivity(context.Background())
	if err != nil || !active {
		t.Fatalf("deliver task notification through generated Runtime gRPC = active:%t err:%v", active, err)
	}
	composed := runningRuntime.waitWithOutputCapture(t, runtime, admin, sessionID, threadID)
	var accepted struct {
		OK      bool `json:"ok"`
		Applied bool `json:"applied"`
		Created bool `json:"created"`
	}
	if err := json.Unmarshal(composed.AcceptResult, &accepted); err != nil || !accepted.OK || !accepted.Applied || accepted.Created {
		t.Fatalf("Runtime task-notification acceptance = %s/%v; want applied to an existing resident Thread", composed.AcceptResult, err)
	}
	if !bridgeServerStore.didDrop() {
		t.Fatal("task notification composition did not exercise committed lost-ACK replay")
	}
	declaration := bridgeServerStore.request()
	if declaration.GetRuntimeInputId() != inputID {
		t.Fatalf("resident Runtime declaration = %#v; want exact committed task input", declaration)
	}
	var committedResult struct {
		Type                     string  `json:"type"`
		AssignedContextSequences []int64 `json:"assignedContextSequences"`
	}
	if err := json.Unmarshal(composed.CommitResult, &committedResult); err != nil || committedResult.Type != "committed" || len(committedResult.AssignedContextSequences) != 1 {
		t.Fatalf("Runtime typed application = %s err:%v; want lost-ACK committed replay with one assigned context sequence", composed.CommitResult, err)
	}

	var inboxStatus, queueStatus, storedMessageText string
	var eventCount, messageCount, requestStartCount, requestEndCount int
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_runtime_inbox
		WHERE workspace_id='default' AND runtime_input_id=$1`, inputID).Scan(&inboxStatus); err != nil {
		t.Fatalf("read Inbox disposition: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM queue_jobs
		WHERE workspace_id='default' AND dedupe_key=$1`, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, inputID)).Scan(&queueStatus); err != nil {
		t.Fatalf("read Queue disposition: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND type='runtime_notification'`, sessionID).Scan(&eventCount); err != nil {
		t.Fatalf("count notification Events: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_messages
		WHERE workspace_id='default' AND session_id=$1 AND kind='runtime_notification'`, sessionID).Scan(&messageCount); err != nil {
		t.Fatalf("count notification Messages: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT data_json::jsonb #>> '{parts,0,text}'
		FROM session_messages
		WHERE workspace_id='default' AND session_id=$1 AND kind='runtime_notification'`, sessionID).Scan(&storedMessageText); err != nil {
		t.Fatalf("read stored notification Message text: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT
		count(*) FILTER (WHERE type='span.model_request_start'),
		count(*) FILTER (WHERE type='span.model_request_end')
		FROM session_events WHERE workspace_id='default' AND session_id=$1`, sessionID).
		Scan(&requestStartCount, &requestEndCount); err != nil {
		t.Fatalf("count parked notification request lifecycle: %v", err)
	}
	if inboxStatus != "committed" || queueStatus != queue.StatusAcknowledged || eventCount != 1 || messageCount != 1 || storedMessageText != runtimeRequest.GetNotificationJson() {
		t.Fatalf("durable settlement = Inbox:%s Queue:%s Events:%d Messages:%d text-match:%t",
			inboxStatus, queueStatus, eventCount, messageCount, storedMessageText == runtimeRequest.GetNotificationJson())
	}
	if composed.ProviderInvocations != 1 || composed.RequestEndCount != 1 || requestStartCount != 1 || requestEndCount != 1 {
		t.Fatalf("parked notification wake lifecycle = providers:%d runtime-ends:%d starts:%d durable-ends:%d; want 1/1/1/1",
			composed.ProviderInvocations, composed.RequestEndCount, requestStartCount, requestEndCount)
	}
	// The durable seed includes the source command's completed call/result pair.
	// Unified context preserves that Assistant before the new notification User.
	// Compare every native member and role, including the exact serialized inputs.
	expectedProviderContext := []any{
		map[string]any{
			"role": float64(2),
			"content": []any{
				map[string]any{"toolCall": map[string]any{
					"modelToolCallId": "call_" + sourceID,
					"name":            "exec_command",
					"inputJson":       "{}",
				}},
				map[string]any{"toolResult": map[string]any{
					"modelToolCallId": "call_" + sourceID,
					"completed": map[string]any{
						"outputJson": `{"text":"Background command accepted."}`,
					},
				}},
			},
		},
		map[string]any{
			"role": float64(1),
			"content": []any{
				map[string]any{"text": map[string]any{"text": runtimeRequest.GetNotificationJson()}},
			},
		},
	}
	var providerContext any
	if len(composed.ProviderContexts) != 1 || json.Unmarshal(composed.ProviderContexts[0], &providerContext) != nil ||
		!reflect.DeepEqual(providerContext, expectedProviderContext) {
		t.Fatalf("parked notification Provider context = %s; want exact source Assistant call/result and notification User: %#v", composed.ProviderContexts, expectedProviderContext)
	}
}

func TestPostgreSQLTaskNotificationWaitsBehindCommittedRequestStart(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID       = "sesn_task_request_start_race"
		threadID        = "thr_task_request_start_race"
		bindingID       = "bind_task_request_start_race"
		podUID          = "pod_task_request_start_race"
		taskID          = "task_request_start_race"
		notificationID  = "task_notification:task_request_start_race"
		sourceID        = "evt_task_request_start_race_source"
		initialEventID  = "evt_task_request_start_race_initial"
		initialInputID  = "rin_task_request_start_race_initial"
		initialSequence = int64(10)
	)
	now := time.Now().UTC()
	sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	sessionfixture.SeedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	baseStore := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	baseStore.RuntimeBindingTokenHMACKey = []byte("task-request-start-race-key")
	scope := sessionfixture.BridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIRequestStart(t, baseStore, scope, "rwrite_task_source_start", "mreq_"+sourceID, runtimecontrol.RequestKindAgentProviderRequest, 0)
	seedBridgeAPINotifiableBackgroundTask(t, admin, "default", sessionID, threadID, bindingID, taskID, sourceID)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events
		SET visibility='public', session_visible=true, model_request_id=$2
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$3`, sessionID, "mreq_"+sourceID, sourceID); err != nil {
		t.Fatalf("make background source Tool Use public: %v", err)
	}
	var sourceAssistantSequence int64
	if err := admin.QueryRowContext(context.Background(), `SELECT sequence FROM session_messages
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND source_event_id=$3`, sessionID, threadID, sourceID).Scan(&sourceAssistantSequence); err != nil {
		t.Fatalf("read background source Assistant sequence: %v", err)
	}
	if response, err := baseStore.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_task_source_end", ModelRequestId: "mreq_" + sourceID,
		FinishReason: "tool-calls", UsageJson: `{}`,
		ProviderContextRetention: &bridgev1.ProviderContextRetention{
			Disposition: "completed", AssistantMessageSequence: sessionfixture.BridgeAPIInt64(sourceAssistantSequence),
			ToolUseEventIds: []string{sourceID},
		},
	}); err != nil || response.GetCommitted() == nil {
		t.Fatalf("close background source Request = %#v/%v", response, err)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, initialEventID, initialSequence, "user.message", `{"content":[{"type":"text","text":"run before task notification"}]}`)
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_sandbox_bindings (
		workspace_id,session_id,logical_sandbox_id,environment_id,environment_generation,
		provider,provider_resource_id,binding_revision,materialized_resource_revision,
		resource_credential_expires_at,resource_roots_json,helper_verified_at,created_at,updated_at
	) VALUES ('default',$1,'sbox_task_request_start_race',$2,1,
		'daytona','provider_task_request_start_race',1,1,$3,'[]',$4,$4,$4)`,
		sessionID, "env_"+sessionID, now.Add(time.Hour), now); err != nil {
		t.Fatalf("seed Sandbox binding: %v", err)
	}

	initialJob := jobrunner.RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: initialInputID, InputKind: "messages", EventIDs: []string{initialEventID},
		SequenceFrom: initialSequence, SequenceTo: initialSequence,
		PayloadJSON: `{"workspace_id":"default","session_id":"` + sessionID + `","session_thread_id":"` + threadID + `","runtime_input_id":"` + initialInputID + `","event_ids":["` + initialEventID + `"],"sequence_from":10,"sequence_to":10,"input_kind":"messages"}`,
	}
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	seedRuntimeInboxBirthForJob(t, admin, initialJob)
	enqueueRuntimeCompositionJob(t, queueStore, sessionID, initialJob, 0)

	barrierStore := &taskNotificationRequestStartBarrierStore{
		BridgeAPIStore: baseStore,
		entered:        make(chan struct{}),
		release:        make(chan struct{}),
	}
	bridgeListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for task notification Request Start Bridge: %v", err)
	}
	bridgeGRPCServer := grpc.NewServer()
	agentruntimebridge.RegisterBridgeAPI(bridgeGRPCServer, barrierStore)
	go func() { _ = bridgeGRPCServer.Serve(bridgeListener) }()
	t.Cleanup(func() {
		bridgeGRPCServer.Stop()
		_ = bridgeListener.Close()
	})

	storedResultJSON := `{"status":"completed","exit_code":0,"stdout":{"text":"TASK_NOTIFICATION_SUCCESSOR_CANARY","truncated":false},"stderr":{"text":"","truncated":false}}`
	runtimeRequest := &agentruntimev1.AcceptTaskNotificationRequest{
		WorkspaceId: "default", SessionId: sessionID, SessionThreadId: threadID,
		BindingId: bindingID, BindingGeneration: 1, TargetPodUid: podUID, RuntimeProcessId: "process_" + podUID,
		RuntimeInputId: notificationID, InputOrder: 0,
		NotificationJson: mustCanonicalTaskNotificationPayloadJSON(t, taskID, sourceID, "completed", storedResultJSON),
	}
	runningRuntime := startTaskNotificationRuntimeComposition(t, t.TempDir()+"/task-notification-request-start.json", runtimeRequest, bridgeListener.Addr().String(), true)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_bindings SET agent_runtime_pod_ip='127.0.0.1'
		WHERE workspace_id='default' AND session_id=$1 AND binding_id=$2`, sessionID, bindingID); err != nil {
		t.Fatalf("align production Runtime visibility snapshot: %v", err)
	}
	seedFixtureRuntimeProcess(t, dbconnect.NewClientForTesting(admin), "tetral-agent-runtime", podUID)
	deliveryStore := jobrunner.NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), runningRuntime.port, jobrunner.KubernetesRuntimeTargetResolver{LoadClient: fixtureRuntimeLoadClient(t), Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{
			Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: podUID, PodIP: "127.0.0.1",
		}})
	}})
	runner := &jobrunner.JobRunner{
		Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer: jobrunner.RuntimePodDirectDeliverer{Store: deliveryStore, Sender: fixtureRuntimeCommandClient(t, taskNotificationRuntimeTokenSource{})},
		Config:    jobrunner.JobRunnerConfig{LeaseOwner: "task-request-start-race", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	if active, runErr := runner.RunOnceWithActivity(context.Background()); runErr != nil || !active {
		t.Fatalf("deliver initial input through JobRunner = active:%t err:%v", active, runErr)
	}
	select {
	case <-barrierStore.entered:
	case <-time.After(10 * time.Second):
		barrierStore.mu.Lock()
		startCount := barrierStore.startCount
		barrierStore.mu.Unlock()
		t.Fatalf("Request Start did not durably commit: starts=%d runtime=%s", startCount, runningRuntime.output.String())
	}
	reconcilePayload, err := json.Marshal(map[string]any{
		"workspace_id": "default", "session_id": sessionID, "task_id": taskID, "reconcile_generation": 1,
	})
	if err != nil {
		t.Fatalf("encode background reconcile job: %v", err)
	}
	if _, err := queueStore.Enqueue(context.Background(), queue.EnqueueRequest{
		ID: queue.NewJobID(), WorkspaceID: workspace.DefaultID, Kind: queue.KindSandboxBackgroundReconcile,
		PartitionKey:   queue.FormatSandboxBackgroundPartitionKey(workspace.DefaultID, sessionID, taskID),
		DedupeKey:      queue.FormatSandboxBackgroundReconcileDedupeKey(workspace.DefaultID, sessionID, taskID, 1),
		PayloadVersion: 1, PayloadJSON: reconcilePayload, MaxAttempts: queue.DefaultMaxAttempts, Now: now,
	}); err != nil {
		t.Fatalf("enqueue background reconcile job: %v", err)
	}
	provider, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{
		sandboxdriver.DaytonaProviderName: terminalBackgroundProvider{result: sandboxdriver.CommandResult{ResultJSON: storedResultJSON, TerminalStatus: "completed"}},
	})
	if err != nil {
		t.Fatalf("build background provider registry: %v", err)
	}
	sandboxRunner := &tetralsandbox.SandboxBackgroundReconcileJobRunner{
		Queue:     tetralqueue.NewServer(queueStore, nil),
		Store:     tetralsandbox.NewPostgreSQLSandboxBackgroundCommandStore(dbconnect.NewClientForTesting(runtime)),
		Providers: provider,
		Config:    tetralsandbox.SandboxBackgroundRunnerConfig{WorkspaceID: "default", LeaseOwner: "task-race-producer", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: 30 * time.Second},
		Clock:     func() time.Time { return now },
	}
	if active, runErr := sandboxRunner.RunOnceWithActivity(context.Background()); runErr != nil || !active {
		t.Fatalf("settle background task through producer = active:%t err:%v", active, runErr)
	}
	if active, runErr := runner.RunOnceWithActivity(context.Background()); runErr != nil || !active {
		t.Fatalf("deliver notification while Request Start ACK is held = active:%t err:%v", active, runErr)
	}
	close(barrierStore.release)
	composed := runningRuntime.wait(t)

	if composed.ProviderInvocations != 2 || composed.RequestEndCount != 2 {
		t.Fatalf("Runtime lifecycle = providers:%d ends:%d; want 2/2 after the retained Tool pair", composed.ProviderInvocations, composed.RequestEndCount)
	}
	if len(composed.ProviderContexts) != 2 || !bytes.Contains(composed.ProviderContexts[0], []byte("Background command accepted.")) || bytes.Contains(composed.ProviderContexts[0], []byte("TASK_NOTIFICATION_SUCCESSOR_CANARY")) || !bytes.Contains(composed.ProviderContexts[1], []byte("TASK_NOTIFICATION_SUCCESSOR_CANARY")) {
		t.Fatalf("Provider context cut did not defer notification: %s", composed.ProviderContexts)
	}

	rows, err := admin.QueryContext(context.Background(), `SELECT (projection_json::jsonb->>'context_through_message_sequence')::bigint
		FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
		  AND type='span.model_request_start' ORDER BY sequence`, sessionID, threadID)
	if err != nil {
		t.Fatalf("read Request Start boundaries: %v", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			t.Errorf("close Request Start boundary rows: %v", closeErr)
		}
	}()
	var boundaries []int64
	for rows.Next() {
		var boundary int64
		if err := rows.Scan(&boundary); err != nil {
			t.Fatalf("scan Request Start boundary: %v", err)
		}
		boundaries = append(boundaries, boundary)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate Request Start boundaries: %v", err)
	}
	var notificationSequence int64
	var inboxStatus, queueStatus, sessionStatus, threadStatus string
	var requestEnds, semanticErrors, terminalEvents int
	if err := admin.QueryRowContext(context.Background(), `SELECT sequence FROM session_messages
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND kind='runtime_notification'`, sessionID, threadID).Scan(&notificationSequence); err != nil {
		t.Fatalf("read notification Message sequence: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$3),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$4),
		(SELECT status FROM sessions WHERE workspace_id='default' AND id=$1),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND id=$2),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='span.model_request_end'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='session.error'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type IN ('session.status_terminated','session.thread_status_terminated'))`,
		sessionID, threadID, notificationID, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, notificationID),
	).Scan(&inboxStatus, &queueStatus, &sessionStatus, &threadStatus, &requestEnds, &semanticErrors, &terminalEvents); err != nil {
		t.Fatalf("read final task-notification lifecycle: %v", err)
	}
	if len(boundaries) != 3 || boundaries[1] >= notificationSequence || boundaries[2] < notificationSequence {
		t.Fatalf("Request Start boundaries = %v notification=%d; want frozen second cut and notification in third", boundaries, notificationSequence)
	}
	if inboxStatus != "committed" || queueStatus != queue.StatusAcknowledged || requestEnds != 3 || semanticErrors != 0 || terminalEvents != 0 || sessionStatus == "terminated" || threadStatus == "terminated" {
		t.Fatalf("final lifecycle = Inbox:%s Queue:%s Session:%s Thread:%s Ends:%d Errors:%d Terminal:%d",
			inboxStatus, queueStatus, sessionStatus, threadStatus, requestEnds, semanticErrors, terminalEvents)
	}
}

func mustCanonicalTaskNotificationPayloadJSON(t *testing.T, taskID, sourceID, statusValue, resultJSON string) string {
	t.Helper()
	payload, err := runtimecontrol.CanonicalTaskNotificationPayloadJSON(taskID, sourceID, statusValue, resultJSON)
	if err != nil {
		t.Fatalf("build canonical task notification: %v", err)
	}
	return payload
}
