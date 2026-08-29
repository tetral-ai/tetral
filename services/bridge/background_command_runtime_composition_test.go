package agentruntimebridge

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestPostgreSQLRuntimeAbortCancelsJoinedBackgroundCommand(t *testing.T) {
	t.Run("cancelled result survives a long lost ACK replay", func(t *testing.T) {
		runPostgreSQLRuntimeAbortBackgroundCommand(t, false, false, 3100*time.Millisecond)
	})
	t.Run("natural completion wins the cancellation race", func(t *testing.T) {
		runPostgreSQLRuntimeAbortBackgroundCommand(t, true, false, 0)
	})
	t.Run("shutdown hands durable work to a replacement Runtime", func(t *testing.T) {
		runPostgreSQLRuntimeAbortBackgroundCommand(t, false, true, 0)
	})
}

func runPostgreSQLRuntimeAbortBackgroundCommand(t *testing.T, naturalCompletion bool, custodyHandoff bool, replayDelay time.Duration) {
	bunPath, err := exec.LookPath("bun")
	if err != nil {
		t.Fatalf("background command composition requires bun: %v", err)
	}
	runtimeRoot := filepath.Clean(filepath.Join("..", "agent-runtime"))
	if _, err := os.Stat(filepath.Join(runtimeRoot, "node_modules", "@grpc", "grpc-js")); err != nil {
		t.Fatalf("background command composition requires installed Runtime dependencies: %v", err)
	}

	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		workspaceID = "default"
		sessionID   = "sesn_background_abort_composition"
		threadID    = "thr_background_abort_composition"
		bindingID   = "bind_background_abort_composition"
		podUID      = "pod_background_abort_composition"
		taskID      = "task_background_abort_composition"
	)
	seedBridgeAPISession(t, admin, workspaceID, sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, workspaceID, sessionID, bindingID, 1, podUID)
	seedReadySandboxForSharedToolExecution(t, admin, workspaceID, sessionID)
	client := dbconnect.NewClientForTesting(runtimeDB)
	store := NewPostgreSQLBridgeAPIStore(client)
	store.RuntimeBindingTokenHMACKey = []byte("background-abort-composition-key")
	bridge := &backgroundAbortBridgeServer{store: store, replayDelay: replayDelay}
	_, bridgeAddress := startSandboxProductionBoundaryBridgeClient(t, bridge, podUID)
	tokenPath := filepath.Join(t.TempDir(), "runtime-token")
	if err := os.WriteFile(tokenPath, []byte("sandbox-production-runtime-token\n"), 0o600); err != nil {
		t.Fatalf("write background composition token: %v", err)
	}

	queueStore := queue.NewPostgreSQLStore(client)
	queueConnection := startBackgroundNotificationQueueServer(t, queueStore)
	provider := &backgroundAbortProvider{
		bridgeMemoryProjectionProvider: &bridgeMemoryProjectionProvider{},
		taskID:                         taskID, naturalCompletion: naturalCompletion,
	}
	registry, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{sandboxdriver.DaytonaProviderName: provider})
	if err != nil {
		t.Fatalf("create background composition provider registry: %v", err)
	}

	const startRequestID = "mreq_background_start_composition"
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_background_start_request", startRequestID, requestKindAgentProviderRequest, 0)
	startInputPath := filepath.Join(t.TempDir(), "background-start.json")
	startInput, err := json.Marshal(map[string]any{
		"address": bridgeAddress, "tokenPath": tokenPath, "workspaceId": workspaceID,
		"sessionId": sessionID, "sessionThreadId": threadID, "bindingId": bindingID,
		"bindingGeneration": 1, "targetPodUid": podUID, "modelRequestId": startRequestID,
		"modelToolCallId": "call_background_start_composition", "toolName": "exec_command",
		"providerInput": map[string]any{"cmd": "sleep 60", "yield_time_ms": 1000},
	})
	if err != nil {
		t.Fatalf("marshal background start composition: %v", err)
	}
	if err := os.WriteFile(startInputPath, startInput, 0o600); err != nil {
		t.Fatalf("write background start composition: %v", err)
	}
	startCommand, startStdout, startStderr := startBunComposition(t, runtimeRoot, bunPath,
		"packages/runtime-pod/test/fixtures/sandbox-production-composition.ts", startInputPath)

	var executeJobs []*queue.Job
	for deadline := time.Now().Add(5 * time.Second); len(executeJobs) == 0 && time.Now().Before(deadline); {
		executeJobs, err = queueStore.Lease(context.Background(), queue.LeaseRequest{
			WorkspaceID: workspace.ID(workspaceID), Kinds: []string{queue.KindSandboxToolExecute},
			LeaseOwner: "background-start-abandoned", MaxJobs: 1, LeaseDuration: time.Millisecond,
		})
		if err == nil && len(executeJobs) == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if err != nil || len(executeJobs) != 1 {
		t.Fatalf("lease background-start execution = %#v/%v", executeJobs, err)
	}
	time.Sleep(10 * time.Millisecond)
	if reclaimed, reclaimErr := queueStore.ReclaimExpiredLeases(context.Background(), queue.ReclaimExpiredLeasesRequest{
		WorkspaceID: workspace.ID(workspaceID), Kind: queue.KindSandboxToolExecute, Limit: 1,
	}); reclaimErr != nil || reclaimed != 1 {
		t.Fatalf("reclaim background-start execution = %d/%v", reclaimed, reclaimErr)
	}
	executionRunner := &tetralsandbox.SandboxToolExecutionJobRunner{
		Queue:       tetralsandbox.SandboxQueueFromGRPC(queuev1.NewQueueServiceClient(queueConnection)),
		Coordinator: tetralsandbox.NewPostgreSQLSandboxExecutionCoordinator(client, 30*time.Minute),
		Providers:   registry, Media: backgroundNotificationMedia{},
		Config: tetralsandbox.SandboxToolExecutionRunnerConfig{
			WorkspaceID: workspaceID, LeaseOwner: "background-start-owner", MaxJobs: 1,
			LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second, PreparationTimeout: 45 * time.Second,
		},
	}
	if active, runErr := executionRunner.RunOnceWithActivity(context.Background()); runErr != nil || !active {
		t.Fatalf("run background-start execution = %t/%v", active, runErr)
	}
	if err := <-startCommand; err != nil {
		t.Fatalf("run background start Runtime: %v\nstdout=%s\nstderr=%s", err, startStdout.String(), startStderr.String())
	}
	var startResult struct {
		ToolUseEventID string `json:"toolUseEventId"`
	}
	if err := json.Unmarshal(startStdout.Bytes(), &startResult); err != nil || startResult.ToolUseEventID == "" {
		t.Fatalf("decode background start Runtime result = %s/%v", startStdout.String(), err)
	}
	if provider.executeCalls.Load() != 1 {
		t.Fatalf("background start provider calls = %d; want one", provider.executeCalls.Load())
	}
	var durableTaskStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_background_tasks
		WHERE workspace_id=$1 AND session_id=$2 AND task_id=$3`, workspaceID, sessionID, taskID).Scan(&durableTaskStatus); err != nil || durableTaskStatus != "running" {
		t.Fatalf("background task after start = %q/%v; want running", durableTaskStatus, err)
	}
	reconcileRunner := &tetralsandbox.SandboxBackgroundReconcileJobRunner{
		Queue: tetralsandbox.SandboxQueueFromGRPC(queuev1.NewQueueServiceClient(queueConnection)),
		Store: tetralsandbox.NewPostgreSQLSandboxBackgroundCommandStore(client), Providers: registry,
		Config: tetralsandbox.SandboxBackgroundRunnerConfig{
			WorkspaceID: workspaceID, LeaseOwner: "background-reconcile-owner", MaxJobs: 1,
			LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second,
		},
	}
	if active, runErr := reconcileRunner.RunOnceWithActivity(context.Background()); runErr != nil || !active {
		t.Fatalf("run initial background reconcile = %t/%v", active, runErr)
	}
	// Keep the periodic observer outside this command-control proof. Both jobs
	// intentionally share a Queue partition, so a due observer owns the head.
	result, err := admin.ExecContext(context.Background(), `UPDATE queue_jobs
		SET available_at=now()+interval '1 minute'
		WHERE workspace_id=$1 AND kind=$2 AND status='pending'`, workspaceID, queue.KindSandboxBackgroundReconcile)
	if err != nil {
		t.Fatalf("park periodic background reconcile: %v", err)
	}
	if parked, rowsErr := result.RowsAffected(); rowsErr != nil || parked != 1 {
		t.Fatalf("parked periodic background reconcile jobs = %d/%v; want one", parked, rowsErr)
	}
	var startMessageSequence int64
	if err := admin.QueryRowContext(context.Background(), `SELECT sequence FROM session_messages
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND model_request_id=$4`,
		workspaceID, sessionID, threadID, startRequestID).Scan(&startMessageSequence); err != nil {
		t.Fatalf("read background start Assistant sequence: %v", err)
	}
	if response, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_background_start_end", ModelRequestId: startRequestID,
		FinishReason: "tool-calls", UsageJson: `{}`,
		ProviderContextRetention: &bridgev1.ProviderContextRetention{
			Disposition: "completed", AssistantMessageSequence: bridgeAPIInt64(startMessageSequence),
			ToolUseEventIds: []string{startResult.ToolUseEventID},
		},
	}); err != nil || response.GetCommitted() == nil {
		t.Fatalf("close background start request = %#v/%v", response, err)
	}

	const controlRequestID = "mreq_background_abort_control"
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_background_control_start", controlRequestID, requestKindAgentProviderRequest, startMessageSequence)
	runtimeBindingToken, err := store.runtimeBindingToken(scope)
	if err != nil {
		t.Fatalf("mint background composition binding token: %v", err)
	}
	abortPath := filepath.Join(t.TempDir(), "abort")
	controlInputPath := filepath.Join(t.TempDir(), "background-control.json")
	controlInputValue := map[string]any{
		"address": bridgeAddress, "tokenPath": tokenPath, "abortPath": abortPath,
		"workspaceId": workspaceID, "sessionId": sessionID, "sessionThreadId": threadID,
		"bindingId": bindingID, "bindingGeneration": 1, "targetPodUid": podUID,
		"runtimeBindingToken": runtimeBindingToken, "modelRequestId": controlRequestID,
		"modelToolCallId": "call_background_abort_control", "taskId": taskID,
	}
	if custodyHandoff {
		controlInputValue["mode"] = "custody_handoff"
	}
	controlInput, err := json.Marshal(controlInputValue)
	if err != nil {
		t.Fatalf("marshal background control composition: %v", err)
	}
	if err := os.WriteFile(controlInputPath, controlInput, 0o600); err != nil {
		t.Fatalf("write background control composition: %v", err)
	}
	controlCommand, controlStdout, controlStderr := startBunComposition(t, runtimeRoot, bunPath,
		"packages/runtime-pod/test/fixtures/background-command-abort-composition.ts", controlInputPath)

	var controlToolUseEventID string
	for deadline := time.Now().Add(5 * time.Second); controlToolUseEventID == "" && time.Now().Before(deadline); {
		err = admin.QueryRowContext(context.Background(), `SELECT tool_use_event_id FROM session_runtime_tool_results
			WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
			  AND tool_kind='sandbox_background' AND background_operation_kind='poll'`, workspaceID, sessionID, threadID).Scan(&controlToolUseEventID)
		if sql.ErrNoRows == err {
			time.Sleep(10 * time.Millisecond)
			controlToolUseEventID = ""
		}
	}
	if err != nil || controlToolUseEventID == "" {
		t.Fatalf("wait for accepted background poll = %q/%v", controlToolUseEventID, err)
	}
	if err := os.WriteFile(abortPath, []byte("abort\n"), 0o600); err != nil {
		t.Fatalf("release Runtime abort: %v", err)
	}
	if custodyHandoff {
		if err := <-controlCommand; err != nil {
			t.Fatalf("run background handoff Runtime: %v\nstdout=%s\nstderr=%s", err, controlStdout.String(), controlStderr.String())
		}
		var handoffResult struct {
			ToolUseEventID string `json:"toolUseEventId"`
			Result         struct {
				Type string `json:"type"`
			} `json:"result"`
		}
		if err := json.Unmarshal(controlStdout.Bytes(), &handoffResult); err != nil ||
			handoffResult.ToolUseEventID != controlToolUseEventID || handoffResult.Result.Type != "stale_custody" {
			t.Fatalf("background handoff Runtime result = %s/%v", controlStdout.String(), err)
		}
		var cancelRows int
		if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_tool_results
			WHERE workspace_id=$1 AND session_id=$2 AND background_operation_kind='cancel'`, workspaceID, sessionID).Scan(&cancelRows); err != nil || cancelRows != 0 {
			t.Fatalf("shutdown cancellation rows = %d/%v; want zero", cancelRows, err)
		}
		controlInputValue["mode"] = "rejoin"
		replacementInput, err := json.Marshal(controlInputValue)
		if err != nil {
			t.Fatalf("marshal replacement background control: %v", err)
		}
		replacementInputPath := filepath.Join(t.TempDir(), "background-replacement.json")
		if err := os.WriteFile(replacementInputPath, replacementInput, 0o600); err != nil {
			t.Fatalf("write replacement background control: %v", err)
		}
		replacementCommand, replacementStdout, replacementStderr := startBunComposition(t, runtimeRoot, bunPath,
			"packages/runtime-pod/test/fixtures/background-command-abort-composition.ts", replacementInputPath)
		provider.pollCompletes.Store(true)
		backgroundRunner := &tetralsandbox.SandboxBackgroundCommandJobRunner{
			Queue: tetralsandbox.SandboxQueueFromGRPC(queuev1.NewQueueServiceClient(queueConnection)),
			Store: tetralsandbox.NewPostgreSQLSandboxBackgroundCommandStore(client), Providers: registry,
			Config: tetralsandbox.SandboxBackgroundRunnerConfig{
				WorkspaceID: workspaceID, LeaseOwner: "background-handoff-owner", MaxJobs: 1,
				LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second,
			},
		}
		if active, runErr := backgroundRunner.RunOnceWithActivity(context.Background()); runErr != nil || !active {
			t.Fatalf("run handed-off background command = %t/%v", active, runErr)
		}
		if err := <-replacementCommand; err != nil {
			t.Fatalf("run replacement background Runtime: %v\nstdout=%s\nstderr=%s", err, replacementStdout.String(), replacementStderr.String())
		}
		var replacementResult struct {
			ToolUseEventID string `json:"toolUseEventId"`
			Result         struct {
				Type string `json:"type"`
			} `json:"result"`
			Settlement struct {
				Type string `json:"type"`
			} `json:"settlement"`
		}
		if err := json.Unmarshal(replacementStdout.Bytes(), &replacementResult); err != nil ||
			replacementResult.ToolUseEventID != controlToolUseEventID || replacementResult.Result.Type != "completed" ||
			replacementResult.Settlement.Type != "committed" {
			t.Fatalf("replacement background Runtime result = %s/%v", replacementStdout.String(), err)
		}
		var taskStatus string
		if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_background_tasks
			WHERE workspace_id=$1 AND session_id=$2 AND task_id=$3`, workspaceID, sessionID, taskID).Scan(&taskStatus); err != nil || taskStatus != "completed" {
			t.Fatalf("handed-off background task = %q/%v; want completed", taskStatus, err)
		}
		cancelRequests, _ := bridge.cancelTrace()
		if len(cancelRequests) != 0 || provider.cancelCalls.Load() != 0 || provider.pollCalls.Load() != 2 {
			t.Fatalf("handoff provider poll/cancel and Bridge cancel = %d/%d/%d; want 2/0/0", provider.pollCalls.Load(), provider.cancelCalls.Load(), len(cancelRequests))
		}
		return
	}

	var cancelCount int
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_tool_results
			WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
			  AND tool_kind='sandbox_background' AND background_operation_kind='cancel'`, workspaceID, sessionID, threadID).Scan(&cancelCount); err != nil {
			t.Fatalf("count accepted background cancel: %v", err)
		}
		if cancelCount == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if cancelCount != 1 {
		t.Fatalf("accepted background cancel rows = %d; want one", cancelCount)
	}
	var commandJobs int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM queue_jobs
		WHERE workspace_id=$1 AND kind=$2 AND status='pending'`, workspaceID, queue.KindSandboxBackgroundCommand).Scan(&commandJobs); err != nil || commandJobs != 2 {
		t.Fatalf("pending background command jobs = %d/%v; want poll and cancel", commandJobs, err)
	}

	backgroundRunner := &tetralsandbox.SandboxBackgroundCommandJobRunner{
		Queue: tetralsandbox.SandboxQueueFromGRPC(queuev1.NewQueueServiceClient(queueConnection)),
		Store: tetralsandbox.NewPostgreSQLSandboxBackgroundCommandStore(client), Providers: registry,
		Config: tetralsandbox.SandboxBackgroundRunnerConfig{
			WorkspaceID: workspaceID, LeaseOwner: "background-command-owner", MaxJobs: 1,
			LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second,
		},
	}
	for index := 0; index < 2; index++ {
		active, runErr := backgroundRunner.RunOnceWithActivity(context.Background())
		if runErr != nil || !active {
			t.Fatalf("run background command %d = %t/%v", index+1, active, runErr)
		}
	}
	if err := <-controlCommand; err != nil {
		t.Fatalf("run background abort Runtime: %v\nstdout=%s\nstderr=%s", err, controlStdout.String(), controlStderr.String())
	}
	var controlResult struct {
		ToolUseEventID string `json:"toolUseEventId"`
		Result         struct {
			Type string `json:"type"`
		} `json:"result"`
		Settlement struct {
			Type string `json:"type"`
		} `json:"settlement"`
	}
	wantResultType := "cancelled"
	if naturalCompletion {
		wantResultType = "completed"
	}
	if err := json.Unmarshal(controlStdout.Bytes(), &controlResult); err != nil || controlResult.ToolUseEventID != controlToolUseEventID ||
		controlResult.Result.Type != wantResultType || controlResult.Settlement.Type != "committed" {
		t.Fatalf("background abort Runtime result = %s/%v", controlStdout.String(), err)
	}
	if provider.pollCalls.Load() != 2 || provider.cancelCalls.Load() != 1 {
		t.Fatalf("background provider poll/cancel calls = %d/%d; want reconcile+control poll and one cancel", provider.pollCalls.Load(), provider.cancelCalls.Load())
	}
	cancelRequests, dropped := bridge.cancelTrace()
	if len(cancelRequests) != 2 || !dropped || !proto.Equal(cancelRequests[0], cancelRequests[1]) {
		t.Fatalf("background cancel replay = %d requests dropped=%t; want two identical requests after lost ACK", len(cancelRequests), dropped)
	}
	var cancelState, taskStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT background_operation_state FROM session_runtime_tool_results
		WHERE workspace_id=$1 AND session_id=$2 AND background_operation_kind='cancel'`, workspaceID, sessionID).Scan(&cancelState); err != nil {
		t.Fatalf("read durable background cancel: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_background_tasks
		WHERE workspace_id=$1 AND session_id=$2 AND task_id=$3`, workspaceID, sessionID, taskID).Scan(&taskStatus); err != nil {
		t.Fatalf("read cancelled background task: %v", err)
	}
	wantTaskStatus := "cancelled"
	if naturalCompletion {
		wantTaskStatus = "completed"
	}
	if cancelState != "terminal" || taskStatus != wantTaskStatus {
		t.Fatalf("durable background cancel/task = %q/%q; want terminal/%s", cancelState, taskStatus, wantTaskStatus)
	}
}

func startBunComposition(t *testing.T, workdir, bunPath, fixture, inputPath string) (<-chan error, *bytes.Buffer, *bytes.Buffer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	command := exec.CommandContext(ctx, bunPath, "run", fixture, inputPath) //nolint:gosec // fixed repository fixture and test-owned path.
	command.Dir = workdir
	command.Stdout = stdout
	command.Stderr = stderr
	if err := command.Start(); err != nil {
		cancel()
		t.Fatalf("start %s: %v", fixture, err)
	}
	finished := make(chan error, 1)
	go func() {
		defer cancel()
		finished <- command.Wait()
	}()
	return finished, stdout, stderr
}

type backgroundAbortBridgeServer struct {
	bridgev1.UnimplementedAgentRuntimeBridgeServiceServer
	store *PostgreSQLBridgeAPIStore

	mu               sync.Mutex
	cancelRequests   []*bridgev1.CancelCommandRequest
	cancelACKDropped bool
	replayDelay      time.Duration
}

func (s *backgroundAbortBridgeServer) WriteEvent(ctx context.Context, request *bridgev1.WriteEventRequest) (*bridgev1.WriteEventResponse, error) {
	return s.store.WriteEvent(ctx, request)
}

func (s *backgroundAbortBridgeServer) AcceptSandboxExecution(ctx context.Context, request *bridgev1.AcceptSandboxExecutionRequest) (*bridgev1.AcceptSandboxExecutionResponse, error) {
	return s.store.AcceptSandboxExecution(ctx, request)
}

func (s *backgroundAbortBridgeServer) AwaitSandboxExecution(ctx context.Context, request *bridgev1.AwaitSandboxExecutionRequest) (*bridgev1.AwaitSandboxExecutionResponse, error) {
	return s.store.AwaitSandboxExecution(ctx, request)
}

func (s *backgroundAbortBridgeServer) ReadCommandResult(ctx context.Context, request *bridgev1.ReadCommandResultRequest) (*bridgev1.ReadCommandResultResponse, error) {
	return s.store.ReadCommandResult(ctx, request)
}

func (s *backgroundAbortBridgeServer) CancelCommand(ctx context.Context, request *bridgev1.CancelCommandRequest) (*bridgev1.CancelCommandResponse, error) {
	s.mu.Lock()
	s.cancelRequests = append(s.cancelRequests, proto.Clone(request).(*bridgev1.CancelCommandRequest))
	s.mu.Unlock()
	response, err := s.store.CancelCommand(ctx, request)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	if !s.cancelACKDropped {
		s.cancelACKDropped = true
		s.mu.Unlock()
		return nil, status.Error(codes.Unknown, "background cancellation acknowledgement unavailable")
	}
	delay := s.replayDelay
	s.mu.Unlock()
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return response, nil
}

func (s *backgroundAbortBridgeServer) SettleToolResult(ctx context.Context, request *bridgev1.SettleToolResultRequest) (*bridgev1.SettleToolResultResponse, error) {
	return s.store.SettleToolResult(ctx, request)
}

func (s *backgroundAbortBridgeServer) cancelTrace() ([]*bridgev1.CancelCommandRequest, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*bridgev1.CancelCommandRequest(nil), s.cancelRequests...), s.cancelACKDropped
}

type backgroundAbortProvider struct {
	*bridgeMemoryProjectionProvider
	taskID            string
	naturalCompletion bool
	executeCalls      atomic.Int32
	pollCalls         atomic.Int32
	cancelCalls       atomic.Int32
	pollCompletes     atomic.Bool
}

func (p *backgroundAbortProvider) ExecuteTool(_ context.Context, _ tetralsandbox.ToolExecutionRequest) tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution] {
	p.executeCalls.Add(1)
	return tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution]{Value: sandboxdriver.ToolExecution{
		ResultJSON: `{"status":"running","result":{"task_id":"` + p.taskID + `"}}`,
		BackgroundTask: &sandboxdriver.BackgroundTask{
			TaskID: p.taskID, ProviderSessionID: "provider-background-session",
			ProviderCommandID: "provider-background-command", ProviderCommandMetadataJSON: `{}`,
		},
	}}
}

func (p *backgroundAbortProvider) PollBackground(context.Context, sandboxdriver.CommandReference) tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult] {
	p.pollCalls.Add(1)
	if p.pollCompletes.Load() {
		return tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult]{Value: sandboxdriver.CommandResult{
			ResultJSON: `{"status":"completed","stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false}}`, TerminalStatus: "completed",
		}}
	}
	return tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult]{Value: sandboxdriver.CommandResult{
		ResultJSON: `{"status":"running","result":{"task_id":"` + p.taskID + `"}}`,
	}}
}

func (*backgroundAbortProvider) SendBackgroundInput(context.Context, sandboxdriver.CommandInput) tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult] {
	return tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult]{Disposition: tetralsandbox.ProviderTerminal, ErrorKind: "unexpected_stdin", SafeMessage: "unexpected stdin"}
}

func (p *backgroundAbortProvider) CancelBackground(_ context.Context, request sandboxdriver.CommandCancel) tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult] {
	p.cancelCalls.Add(1)
	if request.Task.TaskID != p.taskID || request.Reason != "runtime_interrupted" {
		return tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult]{Disposition: tetralsandbox.ProviderTerminal, ErrorKind: "cancel_identity_mismatch", SafeMessage: "cancel identity mismatch"}
	}
	if p.naturalCompletion {
		return tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult]{Value: sandboxdriver.CommandResult{
			ResultJSON: `{"status":"completed","stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false}}`, TerminalStatus: "completed",
		}}
	}
	return tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult]{Value: sandboxdriver.CommandResult{
		ResultJSON: `{"status":"cancelled","result":{"cancelled":true}}`, TerminalStatus: "cancelled",
	}}
}
