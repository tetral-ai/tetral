package agentruntimebridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	internalsandbox "github.com/tetral-ai/tetral/internal/sandbox"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"
)

type taskNotificationRuntimeCompositionOutput struct {
	Declaration  json.RawMessage `json:"declaration"`
	AcceptResult json.RawMessage `json:"acceptResult"`
	CommitResult json.RawMessage `json:"commitResult"`
}

func TestTaskNotificationCanonicalShapesCrossRuntimeDeclarationBoundary(t *testing.T) {
	cases := []struct {
		name           string
		taskID         string
		terminalStatus string
		storedResult   string
		wantStatus     string
	}{
		{name: "completed empty", taskID: "task_shape", terminalStatus: "completed", storedResult: `{"status":"completed","exit_code":null,"stdout":{"text":"","truncated":false},"stderr":{"text":"","truncated":false}}`, wantStatus: "completed"},
		{name: "failed escaped Unicode", taskID: "task_<>&\u2028paragraph\u2029", terminalStatus: "failed", storedResult: "{\"status\":\"failed\",\"exit_code\":-1,\"stdout\":{\"text\":\"<line>&\\\\n雪\\u2028paragraph\\u2029\",\"truncated\":false},\"stderr\":{\"text\":\"failed\",\"truncated\":true,\"total_bytes\":24,\"total_lines\":2}}", wantStatus: "failed"},
		{name: "cancelled safe maximum", taskID: "task_shape", terminalStatus: "cancelled", storedResult: `{"status":"cancelled","exit_code":9007199254740991,"stdout":{"text":"cancelled","truncated":false,"total_bytes":9},"stderr":{"text":"","truncated":false}}`, wantStatus: "cancelled"},
		{name: "expired safe metadata", taskID: "task_shape", terminalStatus: "expired", storedResult: `{"status":"expired","stdout":{"text":"","truncated":false},"stderr":{"text":"expired","truncated":true,"original_bytes":9007199254740991,"original_lines":0}}`, wantStatus: "expired"},
		{name: "unknown lowered", taskID: "task_shape", terminalStatus: "unknown_outcome", storedResult: `{"status":"unknown_outcome","stdout":{"text":"","truncated":false},"stderr":{"text":"unknown","truncated":false}}`, wantStatus: "failed"},
		{name: "large output fitted", taskID: "task_large_shape", terminalStatus: "completed", storedResult: `{"status":"completed","exit_code":0,"stdout":{"text":"` + strings.Repeat("a", 40*1024) + `","truncated":false},"stderr":{"text":"` + strings.Repeat("b", 40*1024) + `","truncated":false}}`, wantStatus: "completed"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			payload, err := canonicalTaskNotificationPayloadJSON(testCase.taskID, "sevt_tool_shape", testCase.terminalStatus, testCase.storedResult)
			if err != nil {
				t.Fatalf("build canonical shape: %v", err)
			}
			request := &agentruntimev1.AcceptTaskNotificationRequest{
				WorkspaceId: "default", SessionId: "sesn_task_shape", SessionThreadId: "thr_task_shape",
				BindingId: "bind_task_shape", BindingGeneration: 1, TargetPodUid: "pod_task_shape",
				RuntimeInputId: "task_notification:" + testCase.taskID, InputOrder: 0,
				NotificationJson: payload,
			}
			composed, err := runTaskNotificationRuntimeComposition(context.Background(), t.TempDir()+"/shape.json", request, nil)
			if err != nil {
				t.Fatalf("run Runtime shape composition: %v", err)
			}
			declaration := &bridgev1.CommitTaskNotificationResultRequest{}
			if err := protojson.Unmarshal(composed.Declaration, declaration); err != nil {
				t.Fatalf("decode Runtime declaration: %v", err)
			}
			if declaration.GetRuntimeInputId() != request.GetRuntimeInputId() {
				t.Fatalf("Runtime declaration = %#v; want exact input target", declaration)
			}
			if len([]byte(payload)) > runtimeTaskNotificationPayloadMaxBytes {
				t.Fatalf("Runtime declaration payload bytes = %d; want <= %d", len([]byte(payload)), runtimeTaskNotificationPayloadMaxBytes)
			}
			var payloadObject map[string]any
			if err := json.Unmarshal([]byte(payload), &payloadObject); err != nil || payloadObject["status"] != testCase.wantStatus {
				t.Fatalf("canonical status = %#v/%v; want %s", payloadObject["status"], err, testCase.wantStatus)
			}
			declarationJSON := string(composed.Declaration)
			if strings.Contains(declarationJSON, "notificationJson") || strings.Contains(declarationJSON, "resultJson") || strings.Contains(declarationJSON, "stdout") || strings.Contains(declarationJSON, "stderr") {
				t.Fatalf("Runtime declaration echoed canonical task payload: %s", declarationJSON)
			}
		})
	}
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

func runTaskNotificationRuntimeComposition(
	ctx context.Context,
	inputPath string,
	request *agentruntimev1.AcceptTaskNotificationRequest,
	commitResponse *bridgev1.CommitTaskNotificationResultResponse,
) (taskNotificationRuntimeCompositionOutput, error) {
	input := map[string]any{
		"notificationJson":  request.GetNotificationJson(),
		"workspaceId":       request.GetWorkspaceId(),
		"sessionId":         request.GetSessionId(),
		"sessionThreadId":   request.GetSessionThreadId(),
		"bindingId":         request.GetBindingId(),
		"bindingGeneration": request.GetBindingGeneration(),
		"targetPodUid":      request.GetTargetPodUid(),
		"runtimeInputId":    request.GetRuntimeInputId(),
		"inputOrder":        request.GetInputOrder(),
	}
	if commitResponse != nil {
		var outcome map[string]any
		switch {
		case commitResponse.GetCommitted() != nil:
			outcome = map[string]any{"committed": assignedContextSequencesForComposition(commitResponse.GetCommitted().GetAssignedContextSequences())}
		case commitResponse.GetStale() != nil:
			outcome = map[string]any{"stale": map[string]any{}}
		case commitResponse.GetParked() != nil:
			outcome = map[string]any{"parked": map[string]any{}}
		case commitResponse.GetRejected() != nil:
			outcome = map[string]any{"rejected": map[string]any{"reason": commitResponse.GetRejected().GetReason()}}
		default:
			return taskNotificationRuntimeCompositionOutput{}, errors.New("commit response has no outcome")
		}
		input["commitResponse"] = outcome
	}
	rawInput, err := json.Marshal(input)
	if err != nil {
		return taskNotificationRuntimeCompositionOutput{}, err
	}
	if err := os.WriteFile(inputPath, rawInput, 0o600); err != nil {
		return taskNotificationRuntimeCompositionOutput{}, err
	}
	command := exec.CommandContext(ctx, "bun", "packages/runtime-pod/test/fixtures/task-notification-composition.ts", inputPath) //nolint:gosec // Fixed repository fixture and test-owned input.
	command.Dir = "../agent-runtime"
	output, err := command.CombinedOutput()
	if err != nil {
		return taskNotificationRuntimeCompositionOutput{}, errors.New(string(output))
	}
	var composed taskNotificationRuntimeCompositionOutput
	if err := json.Unmarshal(output, &composed); err != nil {
		return taskNotificationRuntimeCompositionOutput{}, err
	}
	return composed, nil
}

func assignedContextSequencesForComposition(sequences []int64) map[string]any {
	return map[string]any{"assignedContextSequences": sequences}
}

type runningTaskNotificationRuntime struct {
	command *exec.Cmd
	output  bytes.Buffer
	port    int
}

func startTaskNotificationRuntimeComposition(t *testing.T, inputPath string, request *agentruntimev1.AcceptTaskNotificationRequest, bridgeAddress string) *runningTaskNotificationRuntime {
	t.Helper()
	readyPath := t.TempDir() + "/runtime-ready.json"
	input := map[string]any{
		"notificationJson": request.GetNotificationJson(), "workspaceId": request.GetWorkspaceId(),
		"sessionId": request.GetSessionId(), "sessionThreadId": request.GetSessionThreadId(),
		"bindingId": request.GetBindingId(), "bindingGeneration": request.GetBindingGeneration(),
		"targetPodUid": request.GetTargetPodUid(), "runtimeInputId": request.GetRuntimeInputId(),
		"inputOrder": request.GetInputOrder(), "bridgeAddress": bridgeAddress, "readyPath": readyPath,
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
	running.command.Dir = "../agent-runtime"
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
	if err := r.command.Wait(); err != nil {
		t.Fatalf("resident Runtime composition: %v: %s", err, r.output.String())
	}
	var composed taskNotificationRuntimeCompositionOutput
	if err := json.Unmarshal(r.output.Bytes(), &composed); err != nil {
		t.Fatalf("decode resident Runtime composition: %v: %s", err, r.output.String())
	}
	return composed
}

type taskNotificationRuntimeTokenSource struct{}

func (taskNotificationRuntimeTokenSource) Token(context.Context) (string, error) {
	return "task-notification-composition-token", nil
}

type taskNotificationLostACKStore struct {
	BridgeAPIStore
	mu          sync.Mutex
	dropped     bool
	declaration *bridgev1.CommitTaskNotificationResultRequest
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
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
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

	bridgeStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	bridgeStore.RuntimeBindingTokenHMACKey = []byte("task-notification-composition-key")
	bridgeServerStore := &taskNotificationLostACKStore{BridgeAPIStore: bridgeStore}
	bridgeListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for task notification Bridge: %v", err)
	}
	bridgeGRPCServer := grpc.NewServer()
	RegisterBridgeAPI(bridgeGRPCServer, bridgeServerStore)
	go func() { _ = bridgeGRPCServer.Serve(bridgeListener) }()
	t.Cleanup(func() {
		bridgeGRPCServer.Stop()
		_ = bridgeListener.Close()
	})

	runtimeRequest := &agentruntimev1.AcceptTaskNotificationRequest{
		WorkspaceId: "default", SessionId: sessionID, SessionThreadId: threadID,
		BindingId: bindingID, BindingGeneration: 1, TargetPodUid: podUID,
		RuntimeInputId: inputID, InputOrder: 0,
		NotificationJson: mustCanonicalTaskNotificationPayloadJSON(t, taskID, sourceID, "completed", storedResultJSON),
	}
	runningRuntime := startTaskNotificationRuntimeComposition(t, t.TempDir()+"/task-notification-live.json", runtimeRequest, bridgeListener.Addr().String())
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_bindings SET agent_runtime_pod_ip='127.0.0.1'
		WHERE workspace_id='default' AND session_id=$1 AND binding_id=$2`, sessionID, bindingID); err != nil {
		t.Fatalf("align production Runtime visibility snapshot: %v", err)
	}
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), runningRuntime.port)
	deliveryStore.TargetResolver = KubernetesRuntimeTargetResolver{Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{
			Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: podUID, PodIP: "127.0.0.1",
		}})
	}}
	runner := &JobRunner{
		Queue:      tetralqueue.NewServer(queueStore, nil),
		Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer: RuntimePodDirectDeliverer{
			Store: deliveryStore, Sender: NewRuntimePodCommandClient(taskNotificationRuntimeTokenSource{}),
		},
		Config: JobRunnerConfig{LeaseOwner: "task-notification-composition", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	active, err := runner.RunOnceWithActivity(context.Background())
	if err != nil || !active {
		t.Fatalf("deliver task notification through generated Runtime gRPC = active:%t err:%v", active, err)
	}
	composed := runningRuntime.wait(t)
	var accepted struct {
		OK      bool `json:"ok"`
		Applied bool `json:"applied"`
	}
	if err := json.Unmarshal(composed.AcceptResult, &accepted); err != nil || !accepted.OK || !accepted.Applied {
		t.Fatalf("Runtime task-notification acceptance = %s/%v; want applied", composed.AcceptResult, err)
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
	var eventCount, messageCount int
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
	if inboxStatus != "committed" || queueStatus != queue.StatusAcknowledged || eventCount != 1 || messageCount != 1 || storedMessageText != runtimeRequest.GetNotificationJson() {
		t.Fatalf("durable settlement = Inbox:%s Queue:%s Events:%d Messages:%d text-match:%t",
			inboxStatus, queueStatus, eventCount, messageCount, storedMessageText == runtimeRequest.GetNotificationJson())
	}
}

func mustCanonicalTaskNotificationPayloadJSON(t *testing.T, taskID, sourceID, statusValue, resultJSON string) string {
	t.Helper()
	payload, err := canonicalTaskNotificationPayloadJSON(taskID, sourceID, statusValue, resultJSON)
	if err != nil {
		t.Fatalf("build canonical task notification: %v", err)
	}
	return payload
}
