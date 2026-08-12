package agentruntimebridge

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/tetral-ai/tetral/internal/dbconnect"
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
	Declaration       json.RawMessage `json:"declaration"`
	DeclarationDigest string          `json:"declarationDigest"`
	DurableMessages   []struct {
		ID    string `json:"id"`
		Parts []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"parts"`
	} `json:"durableMessages"`
	ReducerAction struct {
		Action string `json:"action"`
	} `json:"reducerAction"`
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
			request := &agentruntimev1.RuntimeInputCommandRequest{
				RequestId: "req_task_shape", WorkspaceId: "default", SessionId: "sesn_task_shape", SessionThreadId: "thr_task_shape",
				BindingId: "bind_task_shape", BindingGeneration: 1, TargetPodUid: "pod_task_shape",
				TargetPodNamespace: "engine", TargetPodName: "runtime-pod-composition", TargetPodIp: "127.0.0.1",
				RuntimeInputId: "task_notification:" + testCase.taskID, CommandKind: agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_TASK_NOTIFICATION,
				PayloadJson: payload,
			}
			composed, err := runTaskNotificationRuntimeComposition(context.Background(), t.TempDir()+"/shape.json", request, nil)
			if err != nil {
				t.Fatalf("run Runtime shape composition: %v", err)
			}
			declaration := &bridgev1.CommitTaskNotificationResultRequest{}
			if err := protojson.Unmarshal(composed.Declaration, declaration); err != nil {
				t.Fatalf("decode Runtime declaration: %v", err)
			}
			if declaration.GetResultJson() != payload {
				t.Fatalf("Runtime declaration changed canonical bytes: got %q want %q", declaration.GetResultJson(), payload)
			}
			if len([]byte(payload)) > runtimeTaskNotificationPayloadMaxBytes {
				t.Fatalf("Runtime declaration payload bytes = %d; want <= %d", len([]byte(payload)), runtimeTaskNotificationPayloadMaxBytes)
			}
			var payloadObject map[string]any
			if err := json.Unmarshal([]byte(payload), &payloadObject); err != nil || payloadObject["status"] != testCase.wantStatus {
				t.Fatalf("canonical status = %#v/%v; want %s", payloadObject["status"], err, testCase.wantStatus)
			}
			goDigest, err := taskNotificationDeclarationDigest(declaration)
			if err != nil || goDigest != composed.DeclarationDigest {
				t.Fatalf("declaration digest = Go:%s Runtime:%s err:%v", goDigest, composed.DeclarationDigest, err)
			}
		})
	}
}

type taskNotificationRuntimeCompositionSender struct {
	inputPath   string
	declaration *bridgev1.CommitTaskNotificationResultRequest
}

type taskNotificationCompositionDeliverer struct {
	inner RuntimePodDirectDeliverer
}

func (d taskNotificationCompositionDeliverer) DeliverRuntimeJob(ctx context.Context, job RuntimeJob) (RuntimeDeliveryResult, error) {
	return d.inner.DeliverRuntimeJob(ctx, job)
}
func (d taskNotificationCompositionDeliverer) ReplayRuntimeDeliveryFinalization(ctx context.Context, job RuntimeJob) (RuntimeDeliveryResult, bool, error) {
	return d.inner.ReplayRuntimeDeliveryFinalization(ctx, job)
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

func (s *taskNotificationRuntimeCompositionSender) SendRuntimeCommand(
	ctx context.Context,
	_ RuntimePodTarget,
	request *agentruntimev1.RuntimeInputCommandRequest,
) (*agentruntimev1.RuntimeInputCommandResponse, error) {
	output, err := runTaskNotificationRuntimeComposition(ctx, s.inputPath, request, nil)
	if err != nil {
		return nil, err
	}
	declaration := &bridgev1.CommitTaskNotificationResultRequest{}
	if err := protojson.Unmarshal(output.Declaration, declaration); err != nil {
		return nil, err
	}
	s.declaration = declaration
	return &agentruntimev1.RuntimeInputCommandResponse{
		Status:            agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED,
		SessionId:         request.GetSessionId(),
		RuntimeInputId:    request.GetRuntimeInputId(),
		BindingId:         request.GetBindingId(),
		BindingGeneration: request.GetBindingGeneration(),
	}, nil
}

func runTaskNotificationRuntimeComposition(
	ctx context.Context,
	inputPath string,
	request *agentruntimev1.RuntimeInputCommandRequest,
	commitResponse *bridgev1.CommitTaskNotificationResultResponse,
) (taskNotificationRuntimeCompositionOutput, error) {
	input := map[string]any{
		"payloadJson":       request.GetPayloadJson(),
		"workspaceId":       request.GetWorkspaceId(),
		"sessionId":         request.GetSessionId(),
		"sessionThreadId":   request.GetSessionThreadId(),
		"bindingId":         request.GetBindingId(),
		"bindingGeneration": request.GetBindingGeneration(),
		"targetPodUid":      request.GetTargetPodUid(),
		"runtimeInputId":    request.GetRuntimeInputId(),
	}
	if commitResponse != nil {
		declaration := commitResponse.GetDeclaration()
		receipts := make([]map[string]any, 0, len(declaration.GetReceipts()))
		for _, receipt := range declaration.GetReceipts() {
			events := make([]map[string]any, 0, len(receipt.GetEvents()))
			for _, event := range receipt.GetEvents() {
				events = append(events, map[string]any{
					"sessionThreadId": event.GetSessionThreadId(), "eventId": event.GetEventId(),
					"eventSequence": event.GetEventSequence(), "disposition": event.GetDisposition(),
				})
			}
			messages := make([]map[string]any, 0, len(receipt.GetMessages()))
			for _, message := range receipt.GetMessages() {
				parts := make([]map[string]any, 0, len(message.GetParts()))
				for _, part := range message.GetParts() {
					parts = append(parts, map[string]any{
						"partId": part.GetPartId(), "messageId": part.GetMessageId(), "partSequence": part.GetPartSequence(),
						"createdAt": part.GetCreatedAt(), "updatedAt": part.GetUpdatedAt(), "disposition": part.GetDisposition(),
					})
				}
				messages = append(messages, map[string]any{
					"sessionThreadId": message.GetSessionThreadId(), "messageId": message.GetMessageId(),
					"messageSequence": message.GetMessageSequence(), "createdAt": message.GetCreatedAt(),
					"updatedAt": message.GetUpdatedAt(), "disposition": message.GetDisposition(), "parts": parts,
				})
			}
			receipts = append(receipts, map[string]any{
				"sessionThreadId": receipt.GetSessionThreadId(), "operationKind": receipt.GetOperationKind(),
				"sourceKind": receipt.GetSourceKind(), "operationId": receipt.GetOperationId(),
				"events": events, "messages": messages, "pendingAttachmentDeltaJson": []string{},
				"prefixConsumptions": []any{}, "declarationDigest": receipt.GetDeclarationDigest(),
				"childLifecycle": []any{}, "interruptToolProjections": []any{},
			})
		}
		input["commitResponse"] = map[string]any{
			"ack": map[string]any{
				"status": commitResponse.GetAck().GetStatus(), "runtimeInputId": commitResponse.GetAck().GetRuntimeInputId(),
				"runtimeWriteId": commitResponse.GetAck().GetRuntimeWriteId(), "errorCode": commitResponse.GetAck().GetErrorCode(),
			},
			"declaration": map[string]any{
				"receipts": receipts, "observedBindingId": declaration.GetObservedBindingId(),
				"observedBindingGeneration": declaration.GetObservedBindingGeneration(),
				"applicationDisposition":    declaration.GetApplicationDisposition(),
			},
		}
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
	seedBridgeAPIDurableToolMessage(t, admin, "default", sessionID, threadID, "mreq_"+sourceID, sourceID, "toolu_"+sourceID, "exec_command")
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

	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	deliveryStore.TargetResolver = &recordingRuntimeTargetResolver{binding: runtimeBindingForDelivery{
		BindingID: bindingID, BindingGeneration: 1, Namespace: "engine", PodName: "runtime-pod-composition",
		PodUID: podUID, PodIP: "127.0.0.1",
	}}
	sender := &taskNotificationRuntimeCompositionSender{inputPath: t.TempDir() + "/task-notification.json"}
	runner := &JobRunner{
		Queue:      tetralqueue.NewServer(queueStore, nil),
		Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer:  taskNotificationCompositionDeliverer{inner: RuntimePodDirectDeliverer{Store: deliveryStore, Sender: sender}},
		Config:     JobRunnerConfig{LeaseOwner: "task-notification-composition", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	active, err := runner.RunOnceWithActivity(context.Background())
	if err != nil || !active || sender.declaration == nil {
		t.Fatalf("deliver task notification through Job Runner = active:%t declaration:%t err:%v", active, sender.declaration != nil, err)
	}

	bridgeStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	commitResponse, err := bridgeStore.CommitTaskNotificationResult(context.Background(), sender.declaration)
	if err != nil {
		t.Fatalf("commit Runtime declaration through Bridge: %v", err)
	}
	if commitResponse.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("task notification ACK = %#v; want committed", commitResponse.GetAck())
	}
	composed, err := runTaskNotificationRuntimeComposition(
		context.Background(), t.TempDir()+"/task-notification-receipt.json",
		&agentruntimev1.RuntimeInputCommandRequest{
			RequestId: sender.declaration.GetScope().GetRequestId(), WorkspaceId: "default", SessionId: sessionID,
			SessionThreadId: threadID, BindingId: bindingID, BindingGeneration: 1,
			TargetPodNamespace: "engine", TargetPodName: "runtime-pod-composition", TargetPodUid: podUID,
			TargetPodIp: "127.0.0.1", RuntimeInputId: inputID,
			CommandKind: agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_TASK_NOTIFICATION,
			PayloadJson: sender.declaration.GetResultJson(),
		},
		commitResponse,
	)
	if err != nil {
		t.Fatalf("apply task notification receipt in Runtime: %v", err)
	}
	if len(composed.DurableMessages) != 1 || len(composed.DurableMessages[0].Parts) != 1 ||
		composed.DurableMessages[0].Parts[0].Text != sender.declaration.GetResultJson() ||
		composed.ReducerAction.Action != "prepare_next_request" {
		t.Fatalf("Runtime receipt application = messages:%+v reducer:%+v", composed.DurableMessages, composed.ReducerAction)
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
	if inboxStatus != "committed" || queueStatus != queue.StatusAcknowledged || eventCount != 1 || messageCount != 1 || storedMessageText != sender.declaration.GetResultJson() {
		t.Fatalf("durable settlement = Inbox:%s Queue:%s Events:%d Messages:%d text-match:%t",
			inboxStatus, queueStatus, eventCount, messageCount, storedMessageText == sender.declaration.GetResultJson())
	}
}
