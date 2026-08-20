package agentruntimebridge

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	internalgrpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/memory"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxmodel "github.com/tetral-ai/tetral/internal/sandbox"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"
)

// This file owns the Bridge tools protocol-family boundary.

type bridgeMemoryProjectionProvider struct {
	requests []sandboxdriver.MemoryProjectionRefresh
}

func (*bridgeMemoryProjectionProvider) InspectForExecution(context.Context, string) tetralsandbox.ProviderOutcome[tetralsandbox.ExecutionReadiness] {
	return tetralsandbox.ProviderOutcome[tetralsandbox.ExecutionReadiness]{Value: tetralsandbox.ExecutionReady}
}

func (*bridgeMemoryProjectionProvider) InspectForRelease(context.Context, string) tetralsandbox.ProviderOutcome[bool] {
	return tetralsandbox.ProviderOutcome[bool]{Value: true}
}

func (*bridgeMemoryProjectionProvider) ResolveActivation(context.Context, tetralsandbox.ActivationResolutionRequest) tetralsandbox.ProviderOutcome[tetralsandbox.ActivationResolution] {
	return tetralsandbox.ProviderOutcome[tetralsandbox.ActivationResolution]{}
}

func (*bridgeMemoryProjectionProvider) Activate(context.Context, tetralsandbox.ActivationRequest) tetralsandbox.ProviderOutcome[sandboxmodel.ProviderHandle] {
	return tetralsandbox.ProviderOutcome[sandboxmodel.ProviderHandle]{}
}

func (*bridgeMemoryProjectionProvider) MaterializeResources(context.Context, tetralsandbox.MaterializationRequest) tetralsandbox.ProviderOutcome[tetralsandbox.MaterializationResult] {
	return tetralsandbox.ProviderOutcome[tetralsandbox.MaterializationResult]{}
}

func (*bridgeMemoryProjectionProvider) PrepareTool(context.Context, tetralsandbox.ToolExecutionRequest) tetralsandbox.ProviderOutcome[tetralsandbox.ToolPreparationResult] {
	return tetralsandbox.ProviderOutcome[tetralsandbox.ToolPreparationResult]{}
}

func (*bridgeMemoryProjectionProvider) ExecuteTool(context.Context, tetralsandbox.ToolExecutionRequest) tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution] {
	return tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution]{}
}

func (*bridgeMemoryProjectionProvider) ObserveTool(context.Context, sandboxdriver.ForegroundCommandObservation) tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution] {
	return tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution]{}
}

func (*bridgeMemoryProjectionProvider) Release(context.Context, tetralsandbox.ReleaseRequest) tetralsandbox.ProviderOutcome[tetralsandbox.ReleaseResult] {
	return tetralsandbox.ProviderOutcome[tetralsandbox.ReleaseResult]{}
}

func (p *bridgeMemoryProjectionProvider) RefreshMemoryProjection(_ context.Context, request sandboxdriver.MemoryProjectionRefresh) tetralsandbox.ProviderOutcome[struct{}] {
	p.requests = append(p.requests, request)
	return tetralsandbox.ProviderOutcome[struct{}]{}
}

func TestPostgreSQLBridgeAPIStoreAcceptSandboxExecutionCommitsBeforeIndependentWait(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		workspaceID  = "default"
		sessionID    = "sesn_bridge_durable_tool"
		threadID     = "thr_bridge_durable_tool"
		toolUseID    = "evt_bridge_durable_tool"
		modelCallID  = "call_bridge_durable_tool"
		modelRequest = "mreq_bridge_durable_tool"
	)
	seedBridgeAPISession(t, admin, workspaceID, sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, workspaceID, sessionID, "bind_bridge_durable_tool", 1, "pod_uid_bridge_durable_tool")
	const reasoningEventID = "evt_bridge_durable_reasoning"
	seedBridgeAPIEvent(t, admin, workspaceID, sessionID, threadID, reasoningEventID, 1, "agent.thinking", `{}`)
	seedBridgeAPIEvent(t, admin, workspaceID, sessionID, threadID, toolUseID, 2, "agent.tool_use", `{"name":"exec_command","input":{"cmd":"printf ok"},"evaluated_permission":"allow"}`)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_events SET model_request_id = $2, projection_json = $4 WHERE workspace_id = $1 AND event_id = $3`,
		workspaceID, modelRequest, toolUseID, `{"model_tool_call_id":"`+modelCallID+`"}`,
	); err != nil {
		t.Fatalf("stamp durable tool-use model request: %v", err)
	}
	seedBridgeAPIDurableToolMessage(t, admin, workspaceID, sessionID, threadID, modelRequest, toolUseID, modelCallID, "exec_command")
	seedBridgeAPIAllowedToolRoute(t, admin, workspaceID, sessionID, threadID, toolUseID)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_messages SET source_event_id = $5
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3 AND model_request_id = $4`,
		workspaceID, sessionID, threadID, modelRequest, reasoningEventID,
	); err != nil {
		t.Fatalf("move durable assistant message ownership to its first event: %v", err)
	}

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	request := &bridgev1.AcceptSandboxExecutionRequest{
		Scope: bridgeAPIScope(sessionID, threadID, "bind_bridge_durable_tool", 1, "pod_uid_bridge_durable_tool"), ToolUseEventId: toolUseID,
	}
	accepted, err := store.AcceptSandboxExecution(context.Background(), request)
	if err != nil {
		t.Fatalf("AcceptSandboxExecution: %v", err)
	}
	if accepted.GetCommitted() == nil {
		t.Fatalf("AcceptSandboxExecution = %+v; want committed", accepted)
	}
	var executionCount, queueCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_runtime_tool_results
			  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3
			    AND tool_use_event_id = $4 AND tool_kind = 'sandbox_tool'
			    AND model_tool_call_id = $5 AND execution_state = 'pending'`,
		workspaceID, sessionID, threadID, toolUseID, modelCallID,
	).Scan(&executionCount); err != nil {
		t.Fatalf("read accepted sandbox execution: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM queue_jobs
			  WHERE workspace_id = $1 AND kind = 'sandbox_tool_execute'
			    AND partition_key = $2 AND dedupe_key = $3 AND status = 'pending'`,
		workspaceID,
		queue.FormatSandboxExecutionPartitionKey(workspace.ID(workspaceID), sessionID, threadID, toolUseID),
		queue.FormatSandboxToolExecuteDedupeKey(workspace.ID(workspaceID), sessionID, threadID, toolUseID, 1),
	).Scan(&queueCount); err != nil {
		t.Fatalf("read accepted sandbox queue job: %v", err)
	}
	if executionCount != 1 || queueCount != 1 {
		t.Fatalf("accepted execution/queue counts = %d/%d; want 1/1", executionCount, queueCount)
	}

	type result struct {
		response *bridgev1.AwaitSandboxExecutionResponse
		err      error
	}
	waitResult := make(chan result, 1)
	go func() {
		response, err := store.AwaitSandboxExecution(context.Background(), awaitSandboxExecutionRequest(request))
		waitResult <- result{response: response, err: err}
	}()
	select {
	case completed := <-waitResult:
		t.Fatalf("AwaitSandboxExecution returned before durable settlement: response=%+v err=%v", completed.response, completed.err)
	case <-time.After(50 * time.Millisecond):
	}

	const terminalResult = `{"status":"success","result":{"exit_code":0,"stdout":"ok"}}`
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_runtime_tool_results
		    SET execution_state = 'terminal_unconsumed', result_json = $5,
		        result_digest = $6, updated_at = '2026-01-01T00:00:31Z'
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3 AND tool_use_event_id = $4`,
		workspaceID, sessionID, threadID, toolUseID, terminalResult, sha256Hex(terminalResult),
	); err != nil {
		t.Fatalf("settle durable sandbox execution: %v", err)
	}
	select {
	case completed := <-waitResult:
		if completed.err != nil {
			t.Fatalf("AwaitSandboxExecution after durable settlement: %v", completed.err)
		}
		if completed.response.GetCompleted().GetResultJson() != terminalResult {
			t.Fatalf("AwaitSandboxExecution response = %+v; want durable result", completed.response)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("AwaitSandboxExecution did not observe durable sandbox settlement")
	}
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_sandbox_bindings (
		workspace_id, session_id, logical_sandbox_id, environment_id,
		environment_generation, provider, provider_resource_id, binding_revision,
		release_requested_at, release_reason, created_at, updated_at
	) VALUES ($1, $2, $3, $4, 1, 'daytona', $5, 1, $6, 'session_delete', $6, $6)`,
		workspaceID, sessionID, "sbox_"+sessionID, "env_"+sessionID, "provider_"+sessionID,
		time.Date(2026, 1, 1, 0, 0, 32, 0, time.UTC)); err != nil {
		t.Fatalf("seed release fence before accepted replay: %v", err)
	}

	replayRequest := proto.Clone(request).(*bridgev1.AcceptSandboxExecutionRequest)
	replayStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	replay, err := replayStore.AcceptSandboxExecution(context.Background(), replayRequest)
	if err != nil {
		t.Fatalf("AcceptSandboxExecution replay from another Bridge store: %v", err)
	}
	if replay.GetDuplicate() == nil {
		t.Fatalf("AcceptSandboxExecution replay = %+v; want duplicate", replay)
	}
	lockTx, err := admin.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin unrelated Session lock: %v", err)
	}
	if _, err := lockTx.ExecContext(context.Background(),
		`SELECT id FROM sessions WHERE workspace_id = $1 AND id = $2 FOR UPDATE`, workspaceID, sessionID,
	); err != nil {
		_ = lockTx.Rollback()
		t.Fatalf("hold unrelated Session lock: %v", err)
	}
	waitCtx, cancelWait := context.WithTimeout(context.Background(), 500*time.Millisecond)
	replayedResult, err := replayStore.AwaitSandboxExecution(waitCtx, awaitSandboxExecutionRequest(replayRequest))
	cancelWait()
	if rollbackErr := lockTx.Rollback(); rollbackErr != nil {
		t.Fatalf("rollback unrelated Session lock: %v", rollbackErr)
	}
	if err != nil || replayedResult.GetCompleted().GetResultJson() != terminalResult {
		t.Fatalf("AwaitSandboxExecution behind unrelated Session lock = %+v, %v; want durable result", replayedResult, err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_events SET projection_json = jsonb_set(
		  projection_json::jsonb, '{model_tool_call_id}', '"call_bridge_durable_tool_changed"'::jsonb
		)::text
		  WHERE workspace_id = $1 AND event_id = $2`, workspaceID, toolUseID,
	); err != nil {
		t.Fatalf("mutate durable Tool identity: %v", err)
	}
	if _, err := store.AcceptSandboxExecution(context.Background(), replayRequest); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("AcceptSandboxExecution model call conflict error = %v; want AlreadyExists", err)
	}
}

func TestPostgreSQLBridgeAPIStoreApplyPatchInputSplitRoundTrips(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID       = "sesn_bridge_patch_split"
		threadID        = "thr_bridge_patch_split"
		bindingID       = "bind_bridge_patch_split"
		podUID          = "pod_bridge_patch_split"
		modelRequestID  = "mreq_bridge_patch_split"
		modelToolCallID = "call_bridge_patch_split"
		rawPatch        = "*** Begin Patch\n*** Add File: note.txt\n+hello\n*** End Patch\n"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("bridge-patch-split-key-32-bytes!")
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_bridge_patch_split_start", modelRequestID, "agent_provider_request", 0)

	providerInputJSON, err := json.Marshal(rawPatch)
	if err != nil {
		t.Fatalf("marshal provider patch input: %v", err)
	}
	executionInputJSON, err := json.Marshal(map[string]string{"patch": rawPatch})
	if err != nil {
		t.Fatalf("marshal execution patch input: %v", err)
	}
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_bridge_patch_split_tool", ModelRequestId: modelRequestID,
		EventType:                   "agent.tool_use",
		PayloadJson:                 `{"type":"agent.tool_use","name":"apply_patch","input":` + string(providerInputJSON) + `,"evaluated_permission":"allow"}`,
		AssistantContextDelta:       bridgeToolCallContextDeltaForTest(modelToolCallID, "apply_patch", string(providerInputJSON)),
		CanonicalExecutionInputJson: string(executionInputJSON),
	})
	if err != nil {
		t.Fatalf("WriteEvent apply_patch: %v", err)
	}
	toolUseEventID := toolUse.GetCommitted().GetEventId()
	var durableEventPayload string
	if err := admin.QueryRowContext(context.Background(), `SELECT payload_json FROM session_events WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, toolUseEventID).Scan(&durableEventPayload); err != nil {
		t.Fatalf("load authoritative Tool Use event: %v", err)
	}
	if testJSONPathString(t, durableEventPayload, "name") != "apply_patch" ||
		!reflect.DeepEqual(testJSONPathValue(t, durableEventPayload, "input"), rawPatch) ||
		strings.Contains(durableEventPayload, "runtime_only") {
		t.Fatalf("authoritative Tool Use payload = %s", durableEventPayload)
	}

	load := func(_ string) bridgeLoadContextPayload {
		t.Helper()
		response, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope})
		if err != nil {
			t.Fatalf("LoadContext: %v", err)
		}
		var payload bridgeLoadContextPayload
		if err := json.Unmarshal([]byte(response.GetContextJson()), &payload); err != nil {
			t.Fatalf("decode LoadContext: %v", err)
		}
		return payload
	}
	assertRuntimeScalar := func(payload bridgeLoadContextPayload) {
		t.Helper()
		entries := payload.ContextEntries
		if payload.OpenRequestDraft != nil {
			entries = append(entries, bridgeRuntimeContextEntry{Parts: payload.OpenRequestDraft.Parts})
		}
		for _, entry := range entries {
			for _, rawPart := range entry.Parts {
				var part map[string]any
				if err := json.Unmarshal(rawPart, &part); err != nil {
					t.Fatalf("decode Runtime context part: %v", err)
				}
				if part["modelToolCallId"] == modelToolCallID {
					if part["type"] != "tool_call" || part["toolName"] != "apply_patch" ||
						!reflect.DeepEqual(part["canonicalInput"], rawPatch) {
						t.Fatalf("Runtime patch identity = %#v", part)
					}
					return
				}
			}
		}
		t.Fatal("LoadContext omitted apply_patch Tool Call context")
	}

	beforeAcceptance := load("rin_bridge_patch_split_before_acceptance")
	assertRuntimeScalar(beforeAcceptance)
	if len(beforeAcceptance.PendingToolUses) != 1 ||
		beforeAcceptance.PendingToolUses[0].ToolUseEventID != toolUseEventID ||
		beforeAcceptance.PendingToolUses[0].ModelRequestID != modelRequestID ||
		beforeAcceptance.PendingToolUses[0].ModelToolCallID != modelToolCallID ||
		beforeAcceptance.PendingToolUses[0].ToolName != "apply_patch" ||
		string(beforeAcceptance.PendingToolUses[0].Input) != string(executionInputJSON) ||
		beforeAcceptance.PendingToolUses[0].Status != "resolving" ||
		beforeAcceptance.PendingToolUses[0].Decision == nil ||
		*beforeAcceptance.PendingToolUses[0].Decision != "allow" {
		t.Fatalf("admitted apply_patch route = %#v; want durable allow execution identity", beforeAcceptance.PendingToolUses)
	}

	canonicalInput, _, err := canonicalRunToolInput(string(executionInputJSON))
	if err != nil {
		t.Fatalf("canonical patch input: %v", err)
	}
	if canonicalInput != string(executionInputJSON) {
		t.Fatalf("canonical patch input = %q; want %q", canonicalInput, executionInputJSON)
	}
	accepted, err := store.AcceptSandboxExecution(context.Background(), &bridgev1.AcceptSandboxExecutionRequest{
		Scope: scope, ToolUseEventId: toolUseEventID,
	})
	if err != nil {
		t.Fatalf("AcceptSandboxExecution apply_patch: %v", err)
	}
	if accepted.GetCommitted() == nil {
		t.Fatalf("AcceptSandboxExecution = %+v; want committed", accepted)
	}

	afterAcceptance := load("rin_bridge_patch_split_after_acceptance")
	assertRuntimeScalar(afterAcceptance)
	if len(afterAcceptance.PendingToolUses) != 0 || len(afterAcceptance.PendingSandboxExecutions) != 1 {
		t.Fatalf("post-acceptance recovery = approvals %#v executions %#v; want execution only",
			afterAcceptance.PendingToolUses, afterAcceptance.PendingSandboxExecutions)
	}
	execution := afterAcceptance.PendingSandboxExecutions[0]
	if execution.ToolUseEventID != toolUseEventID || execution.ModelRequestID != modelRequestID ||
		execution.ModelToolCallID != modelToolCallID || execution.ToolName != "apply_patch" ||
		string(execution.Input) != string(executionInputJSON) {
		t.Fatalf("pending apply_patch execution = %#v; want unchanged object identity", execution)
	}
}

func TestSandboxSettlementAndBridgeConsumptionConvergeUnderSessionLockRace(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		workspaceID     = "default"
		sessionID       = "sesn_sandbox_settle_consume_race"
		threadID        = "thr_sandbox_settle_consume_race"
		bindingID       = "bind_sandbox_settle_consume_race"
		podUID          = "pod_sandbox_settle_consume_race"
		toolUseEventID  = "evt_sandbox_settle_consume_race"
		modelRequestID  = "mreq_sandbox_settle_consume_race"
		modelToolCallID = "call_sandbox_settle_consume_race"
	)
	seedBridgeAPISession(t, admin, workspaceID, sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, workspaceID, sessionID, bindingID, 1, podUID)
	seedBridgeAPIEvent(t, admin, workspaceID, sessionID, threadID, toolUseEventID, 1, "agent.tool_use", `{"type":"agent.tool_use","name":"Read","input":{},"evaluated_permission":"allow"}`)
	if _, err := admin.Exec(`UPDATE session_events SET model_request_id=$2, projection_json=$4 WHERE workspace_id=$1 AND event_id=$3`,
		workspaceID, modelRequestID, toolUseEventID, `{"model_tool_call_id":"`+modelToolCallID+`"}`); err != nil {
		t.Fatalf("stamp Tool Use model request: %v", err)
	}
	seedBridgeAPIDurableToolMessage(t, admin, workspaceID, sessionID, threadID, modelRequestID, toolUseEventID, modelToolCallID, "Read")
	seedBridgeAPIAllowedToolRoute(t, admin, workspaceID, sessionID, threadID, toolUseEventID)

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	if _, err := store.AcceptSandboxExecution(context.Background(), &bridgev1.AcceptSandboxExecutionRequest{
		Scope: scope, ToolUseEventId: toolUseEventID,
	}); err != nil {
		t.Fatalf("AcceptSandboxExecution: %v", err)
	}
	seedReadySandboxForSharedToolExecution(t, admin, workspaceID, sessionID)

	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	queueConnection := startBackgroundNotificationQueueServer(t, queueStore)
	provider := newGatedBridgeToolProvider()
	registry, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{
		sandboxdriver.DaytonaProviderName: provider,
	})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &tetralsandbox.SandboxToolExecutionJobRunner{
		Queue:       tetralsandbox.SandboxQueueFromGRPC(queuev1.NewQueueServiceClient(queueConnection)),
		Coordinator: tetralsandbox.NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtime), 30*time.Minute),
		Providers:   registry,
		Media:       backgroundNotificationMedia{},
		Config: tetralsandbox.SandboxToolExecutionRunnerConfig{
			WorkspaceID: workspaceID, LeaseOwner: "sandbox-settle-consume-race", MaxJobs: 1,
			LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second, PreparationTimeout: 45 * time.Second,
		},
	}
	type runnerResult struct {
		active bool
		err    error
	}
	runnerDone := make(chan runnerResult, 1)
	go func() {
		active, err := runner.RunOnceWithActivity(context.Background())
		runnerDone <- runnerResult{active: active, err: err}
	}()
	select {
	case <-provider.started:
	case <-time.After(5 * time.Second):
		t.Fatal("Sandbox execution did not reach provider")
	}

	locker, lockerPID := lockPostgreSQLFinalizationFence(t, admin,
		`SELECT id FROM sessions WHERE workspace_id=$1 AND id=$2 FOR UPDATE`, workspaceID, sessionID)
	defer func() { _ = locker.Rollback() }()
	writeRequest := bridgeToolSettlementRequestForTest(scope, bridgeCompletedToolSettlementForTest(toolUseEventID, "done"))
	type writeResult struct {
		response *bridgev1.SettleToolResultResponse
		err      error
	}
	writeDone := make(chan writeResult, 1)
	go func() {
		response, err := store.SettleToolResult(context.Background(), writeRequest)
		writeDone <- writeResult{response: response, err: err}
	}()
	close(provider.release)
	waitForPostgreSQLLockWaiters(t, admin, lockerPID, 2)
	if err := locker.Commit(); err != nil {
		t.Fatalf("release Session race fence: %v", err)
	}
	settled := <-runnerDone
	if settled.err != nil || !settled.active {
		t.Fatalf("Sandbox execution runner = active %v, err %v; want true,nil", settled.active, settled.err)
	}
	written := <-writeDone
	if written.err != nil {
		if status.Code(written.err) != codes.FailedPrecondition {
			t.Fatalf("concurrent SettleToolResult: %v", written.err)
		}
		written.response, written.err = store.SettleToolResult(context.Background(), writeRequest)
	}
	if written.err != nil {
		t.Fatalf("SettleToolResult after Sandbox settlement: %v", written.err)
	}
	bridgeRequireToolSettlementOutcomeForTest(t, written.response, "committed")

	var executionState string
	var retainedResult sql.NullString
	var resultEvents int
	if err := admin.QueryRow(`SELECT execution_state, result_json FROM session_runtime_tool_results
		WHERE workspace_id=$1 AND session_id=$2 AND tool_use_event_id=$3`, workspaceID, sessionID, toolUseEventID).Scan(&executionState, &retainedResult); err != nil {
		t.Fatalf("read converged execution: %v", err)
	}
	if err := admin.QueryRow(`SELECT count(*) FROM session_events
		WHERE workspace_id=$1 AND session_id=$2 AND type='agent.tool_result'
		  AND payload_json::jsonb ->> 'tool_use_id'=$3`, workspaceID, sessionID, toolUseEventID).Scan(&resultEvents); err != nil {
		t.Fatalf("count converged Tool Result events: %v", err)
	}
	if executionState != "consumed" || retainedResult.Valid || resultEvents != 1 {
		t.Fatalf("converged execution = state %q result %v events %d; want consumed/NULL/1", executionState, retainedResult, resultEvents)
	}
}

type gatedBridgeToolProvider struct {
	*bridgeMemoryProjectionProvider
	started chan struct{}
	release chan struct{}
	active  atomic.Int32
	maximum atomic.Int32
}

func newGatedBridgeToolProvider() *gatedBridgeToolProvider {
	return &gatedBridgeToolProvider{
		bridgeMemoryProjectionProvider: &bridgeMemoryProjectionProvider{},
		started:                        make(chan struct{}, 4), release: make(chan struct{}),
	}
}

func (*gatedBridgeToolProvider) InspectForExecution(context.Context, string) tetralsandbox.ProviderOutcome[tetralsandbox.ExecutionReadiness] {
	return tetralsandbox.ProviderOutcome[tetralsandbox.ExecutionReadiness]{Value: tetralsandbox.ExecutionReady}
}

func (*gatedBridgeToolProvider) PrepareTool(context.Context, tetralsandbox.ToolExecutionRequest) tetralsandbox.ProviderOutcome[tetralsandbox.ToolPreparationResult] {
	return tetralsandbox.ProviderOutcome[tetralsandbox.ToolPreparationResult]{Value: tetralsandbox.ToolPreparationResult{}}
}

func (p *gatedBridgeToolProvider) ExecuteTool(ctx context.Context, _ tetralsandbox.ToolExecutionRequest) tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution] {
	current := p.active.Add(1)
	defer p.active.Add(-1)
	for {
		observed := p.maximum.Load()
		if current <= observed || p.maximum.CompareAndSwap(observed, current) {
			break
		}
	}
	p.started <- struct{}{}
	select {
	case <-p.release:
		return tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution]{Value: sandboxdriver.ToolExecution{
			ResultJSON: `{"status":"success","result":{"text":"done"}}`,
		}}
	case <-ctx.Done():
		return tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution]{
			EffectBoundary: tetralsandbox.ProviderOutcomeUnknown, Disposition: tetralsandbox.ProviderTerminal,
			ErrorKind: "provider_cancelled", SafeMessage: "provider execution was cancelled",
		}
	}
}

var _ tetralsandbox.ProviderAdapter = (*gatedBridgeToolProvider)(nil)

func TestPostgreSQLBridgeAPIStoreMemoryInputsRoundTripThroughWriteAndLoadContext(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_bridge_memory_input_roundtrip"
		threadID       = "thr_bridge_memory_input_roundtrip"
		bindingID      = "bind_bridge_memory_input_roundtrip"
		podUID         = "pod_bridge_memory_input_roundtrip"
		modelRequestID = "mreq_bridge_memory_input_roundtrip"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("memory-roundtrip-test-signing-key")
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_memory_input_roundtrip_start", modelRequestID, requestKindAgentProviderRequest, 0)
	inputs := []map[string]any{
		{"action": "create", "path": "notes/large.md", "content": "CREATE_HEAD" + strings.Repeat("\x01", 9_000) + "CREATE_TAIL"},
		{"action": "replace", "path": "notes/large.md", "old_text": "old", "new_text": "new"},
	}
	for index, input := range inputs {
		encoded, err := json.Marshal(input)
		if err != nil {
			t.Fatalf("marshal input: %v", err)
		}
		canonicalInput, _, err := canonicalRunToolInput(string(encoded))
		if err != nil {
			t.Fatalf("canonical input: %v", err)
		}
		response, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
			Scope: scope, RuntimeWriteId: fmt.Sprintf("rwrite_memory_%d", index), ModelRequestId: modelRequestID,
			EventType:                   "agent.tool_use",
			PayloadJson:                 `{"type":"agent.tool_use","name":"memory","input":` + canonicalInput + `,"evaluated_permission":"allow"}`,
			AssistantContextDelta:       bridgeToolCallContextDeltaForTest(fmt.Sprintf("call_memory_%d", index), "memory", canonicalInput),
			CanonicalExecutionInputJson: canonicalInput,
		})
		if err != nil || response.GetCommitted() == nil {
			t.Fatalf("WriteEvent %d: response=%#v err=%v", index, response, err)
		}
	}
	loaded, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope})
	if err != nil {
		t.Fatalf("LoadContext: %v", err)
	}
	var payload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(loaded.GetContextJson()), &payload); err != nil {
		t.Fatalf("decode context: %v", err)
	}
	if payload.OpenRequestDraft == nil || len(payload.OpenRequestDraft.Parts) != len(inputs) {
		t.Fatalf("open draft = %#v; want %d Tool Calls", payload.OpenRequestDraft, len(inputs))
	}
	for index, rawPart := range payload.OpenRequestDraft.Parts {
		var part map[string]any
		if err := json.Unmarshal(rawPart, &part); err != nil {
			t.Fatalf("decode part: %v", err)
		}
		if part["type"] != "tool_call" || part["toolName"] != "memory" || !reflect.DeepEqual(part["canonicalInput"], inputs[index]) {
			t.Fatalf("memory part %d = %#v", index, part)
		}
	}
}
func TestPostgreSQLBridgeAPIStoreAcceptSandboxExecutionEnforcesDurablePermission(t *testing.T) {
	for _, testCase := range []struct {
		name                string
		evaluatedPermission string
		approvalStatus      string
		approvalDecision    string
		approvalInput       string
		wantCode            codes.Code
		wantAccepted        bool
	}{
		{name: "direct allow", evaluatedPermission: "allow", wantCode: codes.OK, wantAccepted: true},
		{name: "resolved ask allow", evaluatedPermission: "ask", approvalStatus: "resolving", approvalDecision: "allow", wantCode: codes.OK, wantAccepted: true},
		{name: "deny", evaluatedPermission: "deny", wantCode: codes.FailedPrecondition},
		{name: "ask missing approval", evaluatedPermission: "ask", wantCode: codes.FailedPrecondition},
		{name: "ask denied", evaluatedPermission: "ask", approvalStatus: "resolving", approvalDecision: "deny", wantCode: codes.FailedPrecondition},
		{name: "unknown permission", evaluatedPermission: "unknown", wantCode: codes.FailedPrecondition},
		{name: "approval identity conflict", evaluatedPermission: "ask", approvalStatus: "resolving", approvalDecision: "allow", approvalInput: `{"cmd":"different"}`, wantCode: codes.AlreadyExists},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			suffix := strings.ReplaceAll(testCase.name, " ", "_")
			sessionID := "sesn_permission_" + suffix
			threadID := "thr_permission_" + suffix
			toolUseID := "evt_permission_" + suffix
			modelRequestID := "mreq_permission_" + suffix
			modelToolCallID := "call_permission_" + suffix
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_permission_"+suffix, 1, "pod_permission_"+suffix)
			seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, toolUseID, 1, "agent.tool_use",
				`{"name":"exec_command","input":{"cmd":"printf ok"},"evaluated_permission":"`+testCase.evaluatedPermission+`"}`)
			if _, err := admin.ExecContext(context.Background(),
				`UPDATE session_events SET model_request_id = $2, projection_json = $4 WHERE workspace_id = $1 AND event_id = $3`,
				"default", modelRequestID, toolUseID, `{"model_tool_call_id":"`+modelToolCallID+`"}`,
			); err != nil {
				t.Fatalf("stamp permission model request: %v", err)
			}
			seedBridgeAPIDurableToolMessage(t, admin, "default", sessionID, threadID, modelRequestID, toolUseID, modelToolCallID, "exec_command")
			if testCase.evaluatedPermission == "allow" {
				seedBridgeAPIAllowedToolRoute(t, admin, "default", sessionID, threadID, toolUseID)
			}
			if testCase.approvalStatus != "" {
				approvalInput := testCase.approvalInput
				if approvalInput == "" {
					approvalInput = `{"cmd":"printf ok"}`
				}
				if _, err := admin.ExecContext(context.Background(),
					`INSERT INTO session_pending_tool_uses (
						workspace_id, session_id, session_thread_id, tool_use_event_id, model_tool_call_id,
						tool_name, input_json, decision, status, created_at, updated_at
					) VALUES ('default', $1, $2, $3, $4, 'exec_command', $5, $6, $7,
						'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
					sessionID, threadID, toolUseID, modelToolCallID, approvalInput, testCase.approvalDecision, testCase.approvalStatus,
				); err != nil {
					t.Fatalf("seed permission approval: %v", err)
				}
			}

			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			_, err := store.AcceptSandboxExecution(context.Background(), &bridgev1.AcceptSandboxExecutionRequest{
				Scope: bridgeAPIScope(sessionID, threadID, "bind_permission_"+suffix, 1, "pod_permission_"+suffix), ToolUseEventId: toolUseID,
			})
			if status.Code(err) != testCase.wantCode {
				t.Fatalf("AcceptSandboxExecution error = %v; want %s", err, testCase.wantCode)
			}
			var executionCount, queueCount int
			if err := admin.QueryRowContext(context.Background(),
				`SELECT count(*) FROM session_runtime_tool_results WHERE workspace_id = 'default' AND session_id = $1`,
				sessionID,
			).Scan(&executionCount); err != nil {
				t.Fatalf("count permission executions: %v", err)
			}
			if err := admin.QueryRowContext(context.Background(),
				`SELECT count(*) FROM queue_jobs WHERE workspace_id = 'default' AND kind = 'sandbox_tool_execute' AND partition_key LIKE $1`,
				"%"+sessionID+"%",
			).Scan(&queueCount); err != nil {
				t.Fatalf("count permission jobs: %v", err)
			}
			wantCount := 0
			if testCase.wantAccepted {
				wantCount = 1
			}
			if executionCount != wantCount || queueCount != wantCount {
				t.Fatalf("permission execution/job counts = %d/%d; want %d/%d", executionCount, queueCount, wantCount, wantCount)
			}
		})
	}
}

func awaitSandboxExecutionRequest(request *bridgev1.AcceptSandboxExecutionRequest) *bridgev1.AwaitSandboxExecutionRequest {
	return &bridgev1.AwaitSandboxExecutionRequest{
		Scope: request.GetScope(), ToolUseEventId: request.GetToolUseEventId(),
	}
}

func TestPostgreSQLBridgeAPIStoreAcceptSandboxExecutionRejectsNewWorkAfterReleaseFence(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		workspaceID = "default"
		sessionID   = "sesn_bridge_released_tool"
		threadID    = "thr_bridge_released_tool"
		toolUseID   = "evt_bridge_released_tool"
		modelCallID = "call_bridge_released_tool"
	)
	seedBridgeAPISession(t, admin, workspaceID, sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, workspaceID, sessionID, "bind_bridge_released_tool", 1, "pod_uid_bridge_released_tool")
	seedBridgeAPIEvent(t, admin, workspaceID, sessionID, threadID, toolUseID, 1, "agent.tool_use", `{"name":"exec_command","input":{"cmd":"true"},"evaluated_permission":"allow"}`)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_events SET model_request_id = 'mreq_bridge_released_tool', projection_json = $3 WHERE workspace_id = $1 AND event_id = $2`,
		workspaceID, toolUseID, `{"model_tool_call_id":"`+modelCallID+`"}`,
	); err != nil {
		t.Fatalf("stamp durable tool-use model request: %v", err)
	}
	seedBridgeAPIDurableToolMessage(t, admin, workspaceID, sessionID, threadID, "mreq_bridge_released_tool", toolUseID, modelCallID, "exec_command")
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_sandbox_bindings (
		workspace_id, session_id, logical_sandbox_id, environment_id,
		environment_generation, provider, provider_resource_id, binding_revision,
		release_requested_at, release_reason, created_at, updated_at
	) VALUES ($1, $2, $3, $4, 1, 'daytona', $5, 1, $6, 'session_delete', $6, $6)`,
		workspaceID, sessionID, "sbox_"+sessionID, "env_"+sessionID, "provider_"+sessionID,
		time.Date(2026, 1, 1, 0, 0, 10, 0, time.UTC)); err != nil {
		t.Fatalf("seed release fence: %v", err)
	}

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	_, err := store.AcceptSandboxExecution(context.Background(), &bridgev1.AcceptSandboxExecutionRequest{
		Scope: bridgeAPIScope(sessionID, threadID, "bind_bridge_released_tool", 1, "pod_uid_bridge_released_tool"), ToolUseEventId: toolUseID,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("AcceptSandboxExecution after release fence error = %v; want FailedPrecondition", err)
	}
	var executionCount, queueCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_runtime_tool_results WHERE workspace_id = $1 AND session_id = $2 AND tool_use_event_id = $3`,
		workspaceID, sessionID, toolUseID,
	).Scan(&executionCount); err != nil {
		t.Fatalf("count fenced execution rows: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM queue_jobs WHERE workspace_id = $1 AND kind = 'sandbox_tool_execute'`, workspaceID,
	).Scan(&queueCount); err != nil {
		t.Fatalf("count fenced execution jobs: %v", err)
	}
	if executionCount != 0 || queueCount != 0 {
		t.Fatalf("release-fenced execution/queue counts = %d/%d; want 0/0", executionCount, queueCount)
	}
}

func TestPostgreSQLBridgeAPIStoreAcceptSandboxExecutionRejectsSettledToolUse(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		workspaceID = "default"
		sessionID   = "sesn_bridge_settled_tool"
		threadID    = "thr_bridge_settled_tool"
		toolUseID   = "evt_bridge_settled_tool"
	)
	seedBridgeAPISession(t, admin, workspaceID, sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, workspaceID, sessionID, "bind_bridge_settled_tool", 1, "pod_uid_bridge_settled_tool")
	seedBridgeAPIEvent(t, admin, workspaceID, sessionID, threadID, toolUseID, 1, "agent.tool_use", `{"name":"exec_command","input":{"cmd":"true"},"evaluated_permission":"allow"}`)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_events SET model_request_id = 'mreq_bridge_settled_tool', projection_json = '{"model_tool_call_id":"call_bridge_settled_tool"}' WHERE workspace_id = $1 AND event_id = $2`,
		workspaceID, toolUseID,
	); err != nil {
		t.Fatalf("stamp durable tool-use model request: %v", err)
	}
	seedBridgeAPIDurableToolMessage(t, admin, workspaceID, sessionID, threadID, "mreq_bridge_settled_tool", toolUseID, "call_bridge_settled_tool", "exec_command")
	seedBridgeAPIEvent(t, admin, workspaceID, sessionID, threadID, "evt_bridge_settled_result", 2, "agent.tool_result", `{"tool_use_event_id":"evt_bridge_settled_tool","content":[{"type":"text","text":"cancelled"}]}`)

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	_, err := store.AcceptSandboxExecution(context.Background(), &bridgev1.AcceptSandboxExecutionRequest{
		Scope: bridgeAPIScope(sessionID, threadID, "bind_bridge_settled_tool", 1, "pod_uid_bridge_settled_tool"), ToolUseEventId: toolUseID,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("AcceptSandboxExecution settled Tool Use error = %v; want FailedPrecondition", err)
	}
	var executionCount, queueCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_runtime_tool_results WHERE workspace_id = $1 AND session_id = $2 AND tool_use_event_id = $3`,
		workspaceID, sessionID, toolUseID,
	).Scan(&executionCount); err != nil {
		t.Fatalf("count fenced executions: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM queue_jobs WHERE workspace_id = $1 AND kind = 'sandbox_tool_execute'`, workspaceID,
	).Scan(&queueCount); err != nil {
		t.Fatalf("count fenced queue jobs: %v", err)
	}
	if executionCount != 0 || queueCount != 0 {
		t.Fatalf("fenced execution/queue counts = %d/%d; want 0/0", executionCount, queueCount)
	}
}

func TestInternalToolRepairKeyIsBoundedTupleSafeAndCrossLanguageStable(t *testing.T) {
	key := internalToolRepairKey("request", "call:a", "b")
	const expected = "internal_invalid_tool_6b53f75d29a34b47f5fdadebf740525a170464690d545d7deb4c9b859818b6fd"
	if key != expected {
		t.Fatalf("internalToolRepairKey() = %q; want %q", key, expected)
	}
	if key == internalToolRepairKey("request", "call", "a:b") {
		t.Fatal("internalToolRepairKey aliases distinct tuples")
	}
	if unicodeKey := internalToolRepairKey("请求", "调用", "工具"); len(unicodeKey) != len("internal_invalid_tool_")+sha256.Size*2 {
		t.Fatalf("unicode internalToolRepairKey length = %d; want fixed length", len(unicodeKey))
	}
}

func TestPostgreSQLBridgeAPIStoreCommitInternalToolRepairPersistsReplaysAndLoads(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_repair", "thr_bridge_repair")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_repair", "bind_bridge_repair", 1, "pod_uid_repair")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("internal-repair-test-signing-key")
	scope := bridgeAPIScope("sesn_bridge_repair", "thr_bridge_repair", "bind_bridge_repair", 1, "pod_uid_repair")
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_bridge_repair_start", "mreq_repair", "agent_provider_request", 0)
	repairKey := internalToolRepairKey("mreq_repair", "call_repair", "unknown_tool")
	request := &bridgev1.CommitInternalToolRepairRequest{
		Scope: scope, ModelRequestId: "mreq_repair", ModelToolCallId: "call_repair", ToolName: "unknown_tool",
		RepairKey: repairKey, CanonicalInputJson: `{"q":"x"}`,
		Error: &bridgev1.RuntimeToolError{ErrorJson: `{"type":"provider_tool_protocol_error","message":"invalid tool","retryable":false}`},
	}
	response, err := store.CommitInternalToolRepair(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitInternalToolRepair: %v", err)
	}
	if response.GetCommitted() == nil || response.GetCommitted().GetRepairEventId() == "" || response.GetCommitted().GetAssignedMessageSequence() <= 0 {
		t.Fatalf("committed repair result = %#v", response)
	}
	replay, err := store.CommitInternalToolRepair(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitInternalToolRepair replay: %v", err)
	}
	if replay.GetDuplicate() == nil || replay.GetDuplicate().GetRepairEventId() != response.GetCommitted().GetRepairEventId() ||
		replay.GetDuplicate().GetAssignedMessageSequence() != response.GetCommitted().GetAssignedMessageSequence() {
		t.Fatalf("duplicate repair result = %#v; first=%#v", replay, response)
	}
	var dataJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT data_json FROM session_messages
		  WHERE workspace_id='default' AND session_id='sesn_bridge_repair'
		    AND session_thread_id='thr_bridge_repair' AND model_request_id='mreq_repair'`,
	).Scan(&dataJSON); err != nil {
		t.Fatalf("read repair context: %v", err)
	}
	parts, err := decodeStoredRuntimeContextParts(dataJSON)
	if err != nil || len(parts) != 2 {
		t.Fatalf("repair context parts = %s err=%v", dataJSON, err)
	}
	loaded, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope})
	if err != nil {
		t.Fatalf("LoadContext after repair: %v", err)
	}
	var payload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(loaded.GetContextJson()), &payload); err != nil {
		t.Fatalf("decode LoadContext: %v", err)
	}
	if len(payload.TurnFacts.InternalRepairs) != 1 {
		t.Fatalf("repair facts = %#v", payload.TurnFacts.InternalRepairs)
	}
	fact := payload.TurnFacts.InternalRepairs[0]
	if fact.RepairKey != repairKey || fact.ModelToolCallID != "call_repair" || fact.ToolName != "unknown_tool" {
		t.Fatalf("repair direct fact = %#v", fact)
	}
	conflict := proto.Clone(request).(*bridgev1.CommitInternalToolRepairRequest)
	conflict.Error = &bridgev1.RuntimeToolError{ErrorJson: `{"type":"provider_tool_protocol_error","message":"different","retryable":false}`}
	if _, err := store.CommitInternalToolRepair(context.Background(), conflict); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("conflicting CommitInternalToolRepair err = %v; want AlreadyExists", err)
	}
}
func TestPostgreSQLBridgeAPIStoreRejectsPublicAndRepairToolCallIdentityCollision(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_collision", "sthr_collision")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_collision", "bind_collision", 1, "pod_collision")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_collision", "sthr_collision", "bind_collision", 1, "pod_collision")
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_collision_start", "mreq_collision", requestKindAgentProviderRequest, 0)
	public, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_collision_tool", ModelRequestId: "mreq_collision",
		EventType: "agent.tool_use", PayloadJson: `{"type":"agent.tool_use","name":"unknown_tool","input":{},"evaluated_permission":"allow"}`,
		AssistantContextDelta:       bridgeToolCallContextDeltaForTest("call_collision", "unknown_tool", `{}`),
		CanonicalExecutionInputJson: `{}`,
	})
	if err != nil || public.GetCommitted() == nil {
		t.Fatalf("seed public Tool Call: response=%#v err=%v", public, err)
	}
	repairKey := internalToolRepairKey("mreq_collision", "call_collision", "unknown_tool")
	_, err = store.CommitInternalToolRepair(context.Background(), &bridgev1.CommitInternalToolRepairRequest{
		Scope: scope, ModelRequestId: "mreq_collision", ModelToolCallId: "call_collision", ToolName: "unknown_tool",
		RepairKey: repairKey, CanonicalInputJson: `{}`,
		Error: &bridgev1.RuntimeToolError{ErrorJson: `{"type":"provider_tool_protocol_error","message":"invalid","retryable":false}`},
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("repair collision error = %v; want AlreadyExists", err)
	}
}
func TestPostgreSQLBridgeAPIStoreKeepsOrdinaryAssistantAndRepairMembersInOneDraft(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_mixed_draft", "sthr_mixed_draft")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_mixed_draft", "bind_mixed_draft", 1, "pod_mixed_draft")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("mixed-draft-test-signing-key")
	scope := bridgeAPIScope("sesn_mixed_draft", "sthr_mixed_draft", "bind_mixed_draft", 1, "pod_mixed_draft")
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_mixed_start", "mreq_mixed", requestKindAgentProviderRequest, 0)

	written, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_mixed_text", ModelRequestId: "mreq_mixed",
		EventType: "agent.message", PayloadJson: `{"type":"agent.message","content":"continuing"}`,
		AssistantContextDelta: bridgeTextContextDeltaForTest("continuing"),
	})
	if err != nil || written.GetCommitted() == nil {
		t.Fatalf("write ordinary Assistant member: response=%#v err=%v", written, err)
	}

	repairKey := internalToolRepairKey("mreq_mixed", "call_mixed", "unknown_tool")
	repaired, err := store.CommitInternalToolRepair(context.Background(), &bridgev1.CommitInternalToolRepairRequest{
		Scope: scope, ModelRequestId: "mreq_mixed", ModelToolCallId: "call_mixed", ToolName: "unknown_tool",
		RepairKey: repairKey, CanonicalInputJson: `{"q":"x"}`,
		Error: &bridgev1.RuntimeToolError{ErrorJson: `{"type":"provider_tool_protocol_error","message":"invalid tool","retryable":false}`},
	})
	if err != nil || repaired.GetCommitted() == nil {
		t.Fatalf("commit internal Tool repair: response=%#v err=%v", repaired, err)
	}

	var rowCount int
	var dataJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*), max(data_json)
		   FROM session_messages
		  WHERE workspace_id='default' AND session_id='sesn_mixed_draft'
		    AND session_thread_id='sthr_mixed_draft' AND model_request_id='mreq_mixed'`,
	).Scan(&rowCount, &dataJSON); err != nil {
		t.Fatalf("read mixed Assistant draft: %v", err)
	}
	parts, err := decodeStoredRuntimeContextParts(dataJSON)
	if err != nil || rowCount != 1 || len(parts) != 3 {
		t.Fatalf("mixed Assistant draft rows/parts = %d/%d data=%s err=%v; want 1/3", rowCount, len(parts), dataJSON, err)
	}
	var kinds [3]struct {
		Type string `json:"type"`
	}
	for index := range parts {
		if err := json.Unmarshal(parts[index], &kinds[index]); err != nil {
			t.Fatalf("decode mixed Assistant draft part %d: %v", index, err)
		}
	}
	if kinds[0].Type != "text" || kinds[1].Type != "tool_call" || kinds[2].Type != "tool_result" {
		t.Fatalf("mixed Assistant draft parts = %#v", parts)
	}

	loaded, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope})
	if err != nil {
		t.Fatalf("LoadContext mixed Assistant draft: %v", err)
	}
	var payload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(loaded.GetContextJson()), &payload); err != nil {
		t.Fatalf("decode mixed Assistant draft: %v", err)
	}
	if payload.OpenRequestDraft == nil || payload.OpenRequestDraft.ModelRequestID != "mreq_mixed" ||
		len(payload.OpenRequestDraft.Parts) != 3 || len(payload.TurnFacts.InternalRepairs) != 1 {
		t.Fatalf("mixed Assistant recovery = draft=%#v repairs=%#v", payload.OpenRequestDraft, payload.TurnFacts.InternalRepairs)
	}
	if payload.TurnFacts.InternalRepairs[0].RepairKey != repairKey {
		t.Fatalf("mixed Assistant repair fact = %#v", payload.TurnFacts.InternalRepairs[0])
	}
}
func TestPostgreSQLBridgeAPIStoreCommitInternalToolRepairRejectsRequestEndSeal(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_repair_after_end"
		threadID       = "thr_repair_after_end"
		modelRequestID = "mreq_repair_after_end"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_repair_after_end", 1, "pod_repair_after_end")
	scope := bridgeAPIScope(sessionID, threadID, "bind_repair_after_end", 1, "pod_repair_after_end")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_repair_after_end_start", modelRequestID, requestKindAgentProviderRequest, 0)
	if _, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_repair_after_end_close", ModelRequestId: modelRequestID,
		FinishReason: "tool_calls", UsageJson: `{}`,
	}); err != nil {
		t.Fatalf("write request end: %v", err)
	}
	request := &bridgev1.CommitInternalToolRepairRequest{
		Scope: scope, ModelRequestId: modelRequestID, ModelToolCallId: "call_repair_after_end", ToolName: "unknown_tool",
		RepairKey: internalToolRepairKey(modelRequestID, "call_repair_after_end", "unknown_tool"), CanonicalInputJson: `{}`,
		Error: &bridgev1.RuntimeToolError{ErrorJson: `{"type":"provider_tool_protocol_error","message":"invalid tool","retryable":false}`},
	}
	if _, err := store.CommitInternalToolRepair(context.Background(), request); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("post-seal internal repair err = %v; want FailedPrecondition", err)
	}
}

func TestPostgreSQLBridgeAPIStoreCommitInternalToolRepairRejectsStaleBinding(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_repair_stale", "thr_bridge_repair_stale")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_repair_stale", "bind_bridge_repair_stale", 1, "pod_uid_repair_stale")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	_, err := store.CommitInternalToolRepair(context.Background(), &bridgev1.CommitInternalToolRepairRequest{
		Scope:              bridgeAPIScope("sesn_bridge_repair_stale", "thr_bridge_repair_stale", "bind_bridge_repair_stale", 2, "pod_uid_repair_stale"),
		ModelRequestId:     "mreq_repair_stale",
		ModelToolCallId:    "call_repair_stale",
		ToolName:           "unknown_tool",
		RepairKey:          internalToolRepairKey("mreq_repair_stale", "call_repair_stale", "unknown_tool"),
		CanonicalInputJson: `{}`,
		Error:              &bridgev1.RuntimeToolError{ErrorJson: `{"type":"provider_tool_protocol_error","message":"invalid tool","retryable":false}`},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale CommitInternalToolRepair err = %v; want FailedPrecondition", err)
	}
	var messageCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_messages WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_repair_stale'`).Scan(&messageCount); err != nil {
		t.Fatalf("read stale repair message count: %v", err)
	}
	if messageCount != 0 {
		t.Fatalf("stale repair message count = %d; want 0", messageCount)
	}
}

func TestPostgreSQLBridgeAPIStoreRunMemoryMutatesDurableMemoryAndReplays(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory", "thr_bridge_memory")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory", "bind_bridge_memory", 1, "pod_uid_memory")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory", "memstore_bridge_memory")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_bridge_memory", "thr_bridge_memory", "bind_bridge_memory", 1, "pod_uid_memory")
	create := durableMemoryRequestForTest(
		t, admin, scope, "evt_tool_memory_create",
		`{"action":"create","path":"notes/todo.md","content":"one"}`,
	)
	response, err := store.RunMemory(context.Background(), create)
	if err != nil {
		t.Fatalf("RunMemory create: %v", err)
	}
	createdResult := committedMemoryResultJSON(t, response)
	assertMemoryResultStatus(t, createdResult, "completed")
	replay, err := store.RunMemory(context.Background(), create)
	if err != nil {
		t.Fatalf("RunMemory replay: %v", err)
	}
	if duplicateMemoryResultJSON(t, replay) != createdResult {
		t.Fatalf("RunMemory replay = %+v; want duplicate same result", replay)
	}

	var currentContent string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT v.content
		   FROM memories m
		   JOIN memory_versions v
		     ON v.workspace_id = m.workspace_id
		    AND v.memory_store_id = m.memory_store_id
		    AND v.memory_id = m.memory_id
		    AND v.memory_version_id = m.current_version_id
		  WHERE m.workspace_id = 'default'
		    AND m.memory_store_id = 'memstore_bridge_memory'
		    AND m.path = '/notes/todo.md'
		    AND m.deleted_at IS NULL`).Scan(&currentContent); err != nil {
		t.Fatalf("read created memory: %v", err)
	}
	if currentContent != "one" {
		t.Fatalf("created memory content = %q; want one", currentContent)
	}
	if count := countMemoryVersions(t, admin, "memstore_bridge_memory"); count != 1 {
		t.Fatalf("memory versions after replay = %d; want 1", count)
	}

	replace := durableMemoryRequestForTest(
		t, admin, scope, "evt_tool_memory_replace",
		`{"action":"replace","path":"notes/todo.md","old_text":"one","new_text":"two"}`,
	)
	replaced, err := store.RunMemory(context.Background(), replace)
	if err != nil {
		t.Fatalf("RunMemory replace: %v", err)
	}
	assertMemoryResultStatus(t, committedMemoryResultJSON(t, replaced), "completed")
	if err := admin.QueryRowContext(context.Background(),
		`SELECT v.content
		   FROM memories m
		   JOIN memory_versions v
		     ON v.workspace_id = m.workspace_id
		    AND v.memory_store_id = m.memory_store_id
		    AND v.memory_id = m.memory_id
		    AND v.memory_version_id = m.current_version_id
		  WHERE m.workspace_id = 'default'
		    AND m.memory_store_id = 'memstore_bridge_memory'
		    AND m.path = '/notes/todo.md'
		    AND m.deleted_at IS NULL`).Scan(&currentContent); err != nil {
		t.Fatalf("read replaced memory: %v", err)
	}
	if currentContent != "two" {
		t.Fatalf("replaced memory content = %q; want two", currentContent)
	}
}

func TestPostgreSQLBridgeAPIStoreRunMemoryWaitsForDurableSandboxProjection(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		workspaceID = "default"
		sessionID   = "sesn_bridge_memory_projection_queue"
		threadID    = "thr_bridge_memory_projection_queue"
		bindingID   = "bind_bridge_memory_projection_queue"
		memoryStore = "memstore_bridge_memory_projection_queue"
		memoryWrite = "evt_tool_memory_projection_queue"
		providerID  = "provider_memory_projection_queue"
	)
	seedBridgeAPISession(t, admin, workspaceID, sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, workspaceID, sessionID, bindingID, 1, "pod_uid_memory_projection_queue")
	seedBridgeAPIWritableMemoryStore(t, admin, workspaceID, sessionID, memoryStore)
	if _, err := admin.Exec(`INSERT INTO session_sandbox_bindings (
		workspace_id, session_id, logical_sandbox_id, environment_id,
		environment_generation, provider, provider_resource_id, binding_revision,
		materialized_resource_revision, resource_roots_json, provider_metadata_json,
		created_at, updated_at
	) VALUES ($1, $2, 'sbox_memory_projection_queue', $3, 1, 'daytona', $4, 1, 1, '[]', '{}', $5, $5)`,
		workspaceID, sessionID, "env_"+sessionID, providerID, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("seed sandbox binding: %v", err)
	}

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	request := durableMemoryRequestForTest(
		t, admin, bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_memory_projection_queue"), memoryWrite,
		`{"action":"create","path":"notes/queue.md","content":"durable"}`,
	)
	type callResult struct {
		response *bridgev1.RunMemoryResponse
		err      error
	}
	result := make(chan callResult, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		response, err := store.RunMemory(ctx, request)
		result <- callResult{response: response, err: err}
	}()

	deadline := time.Now().Add(3 * time.Second)
	for {
		var projectionState string
		var queueCount int
		err := admin.QueryRow(`SELECT memory_projection_state FROM session_runtime_tool_results
			WHERE workspace_id = $1 AND session_id = $2 AND tool_use_event_id = $3`, workspaceID, sessionID, memoryWrite).Scan(&projectionState)
		if err == nil {
			if err := admin.QueryRow(`SELECT count(*) FROM queue_jobs WHERE workspace_id = $1 AND kind = $2 AND dedupe_key = $3`,
				workspaceID, queue.KindSandboxMemoryProjection,
				queue.FormatSandboxMemoryProjectionDedupeKey(workspace.ID(workspaceID), memoryStore, memoryWrite)).Scan(&queueCount); err != nil {
				t.Fatalf("count memory projection jobs: %v", err)
			}
			if projectionState == memoryProjectionStatePending && queueCount == 1 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("memory projection did not become pending with one Queue job: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	projectionStore := tetralsandbox.NewPostgreSQLSandboxMemoryProjectionStore(dbconnect.NewClientForTesting(runtime))
	provider := &bridgeMemoryProjectionProvider{}
	providers, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{"daytona": provider})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	runner := &tetralsandbox.SandboxMemoryProjectionJobRunner{
		Queue: tetralqueue.NewServer(queueStore, nil), Store: projectionStore, Providers: providers,
		Config: tetralsandbox.SandboxMemoryProjectionRunnerConfig{
			WorkspaceID: workspaceID, LeaseOwner: "bridge-memory-projection-test",
			MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second,
		},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("SandboxMemoryProjectionJobRunner.RunOnce: %v", err)
	}
	if len(provider.requests) != 1 || provider.requests[0].Target.ProviderSandboxID != providerID ||
		len(provider.requests[0].Ops) != 1 || provider.requests[0].Ops[0].Kind != "upsert" ||
		provider.requests[0].Ops[0].RelativePath != "/notes/queue.md" || provider.requests[0].Ops[0].Content != "durable" {
		t.Fatalf("projection requests = %+v; want one durable memory upsert", provider.requests)
	}
	var settledState string
	if err := admin.QueryRow(`SELECT memory_projection_state FROM session_runtime_tool_results
		WHERE workspace_id = $1 AND session_id = $2 AND tool_use_event_id = $3`, workspaceID, sessionID, memoryWrite).Scan(&settledState); err != nil {
		t.Fatalf("read settled projection: %v", err)
	}
	if settledState != memoryProjectionStateRefreshed {
		t.Fatalf("settled projection state = %q; want refreshed", settledState)
	}

	completed := <-result
	if completed.err != nil {
		t.Fatalf("RunMemory: %v", completed.err)
	}
	completedJSON := committedMemoryResultJSON(t, completed.response)
	assertMemoryResultStatus(t, completedJSON, "completed")
	replay, err := store.RunMemory(context.Background(), request)
	if err != nil {
		t.Fatalf("RunMemory replay: %v", err)
	}
	if duplicateMemoryResultJSON(t, replay) != completedJSON {
		t.Fatalf("RunMemory replay = %+v; want duplicate durable result", replay)
	}
	if count := countMemoryVersions(t, admin, memoryStore); count != 1 {
		t.Fatalf("memory versions = %d; want one committed mutation", count)
	}
}

func TestPostgreSQLBridgeAPIStoreRunMemoryEnforcesDurableMemoryQuotas(t *testing.T) {
	t.Run("memory identities", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_identity_quota", "thr_bridge_memory_identity_quota")
		seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_identity_quota", "bind_bridge_memory_identity_quota", 1, "pod_uid_memory_identity_quota")
		seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_identity_quota", "memstore_bridge_memory_identity_quota")
		seedBridgeAPIMemoryIdentities(t, admin, "memstore_bridge_memory_identity_quota", memory.MaxMemoriesPerStore)

		store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
		request := durableMemoryRequestForTest(t, admin,
			bridgeAPIScope("sesn_bridge_memory_identity_quota", "thr_bridge_memory_identity_quota", "bind_bridge_memory_identity_quota", 1, "pod_uid_memory_identity_quota"),
			"evt_tool_memory_identity_quota", `{"action":"create","path":"over-limit.md","content":"x"}`)
		_, err := store.RunMemory(context.Background(), request)
		var quota *memory.QuotaError
		if !errors.As(err, &quota) {
			t.Fatalf("RunMemory create err = %T %v; want memory quota", err, err)
		}
		if count := countBridgeAPIMemories(t, admin, "memstore_bridge_memory_identity_quota"); count != memory.MaxMemoriesPerStore {
			t.Fatalf("memory count after quota rejection = %d; want %d", count, memory.MaxMemoriesPerStore)
		}
		assertNoBridgeAPIRuntimeToolResult(t, admin, "sesn_bridge_memory_identity_quota", "evt_tool_memory_identity_quota")
	})

	t.Run("versions", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_version_quota", "thr_bridge_memory_version_quota")
		seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_version_quota", "bind_bridge_memory_version_quota", 1, "pod_uid_memory_version_quota")
		seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_version_quota", "memstore_bridge_memory_version_quota")
		seedBridgeAPIMemory(t, admin, "default", "memstore_bridge_memory_version_quota", "mem_bridge_memory_version_quota", "/quota.md", "x")
		seedBridgeAPIAdditionalMemoryVersions(t, admin, "memstore_bridge_memory_version_quota", "mem_bridge_memory_version_quota", memory.MaxMemoryVersionsPerStore-1)

		store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
		requests := []struct {
			name      string
			inputJSON string
		}{
			{name: "replace", inputJSON: `{"action":"replace","path":"quota.md","old_text":"x","new_text":"y"}`},
			{name: "delete", inputJSON: `{"action":"delete","path":"quota.md","expected_text":"x"}`},
			{name: "rename", inputJSON: `{"action":"rename","path":"quota.md","new_path":"renamed.md","expected_text":"x"}`},
		}
		for _, test := range requests {
			t.Run(test.name, func(t *testing.T) {
				request := durableMemoryRequestForTest(t, admin,
					bridgeAPIScope("sesn_bridge_memory_version_quota", "thr_bridge_memory_version_quota", "bind_bridge_memory_version_quota", 1, "pod_uid_memory_version_quota"),
					"evt_tool_memory_version_quota_"+test.name, test.inputJSON)
				_, err := store.RunMemory(context.Background(), request)
				var quota *memory.QuotaError
				if !errors.As(err, &quota) {
					t.Fatalf("RunMemory %s err = %T %v; want memory version quota", test.name, err, err)
				}
				assertBridgeAPIMemoryHead(t, admin, "memstore_bridge_memory_version_quota", "/quota.md", "x")
				if count := countMemoryVersions(t, admin, "memstore_bridge_memory_version_quota"); count != memory.MaxMemoryVersionsPerStore {
					t.Fatalf("version count after %s quota rejection = %d; want %d", test.name, count, memory.MaxMemoryVersionsPerStore)
				}
				assertNoBridgeAPIRuntimeToolResult(t, admin, "sesn_bridge_memory_version_quota", "evt_tool_memory_version_quota_"+test.name)
			})
		}
	})

	t.Run("retained payload bytes", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_retained_quota", "thr_bridge_memory_retained_quota")
		seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_retained_quota", "bind_bridge_memory_retained_quota", 1, "pod_uid_memory_retained_quota")
		seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_retained_quota", "memstore_bridge_memory_retained_quota")
		seedBridgeAPIMemory(t, admin, "default", "memstore_bridge_memory_retained_quota", "mem_bridge_memory_retained_quota", "/quota.md", "x")
		seedBridgeAPIRetainedMemoryPayload(t, admin, "memstore_bridge_memory_retained_quota", "mem_bridge_memory_retained_quota", memory.MaxRetainedMemoryPayloadBytesPerStore-1)

		store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
		requests := []struct {
			name      string
			inputJSON string
		}{
			{name: "create", inputJSON: `{"action":"create","path":"over-limit.md","content":"x"}`},
			{name: "replace", inputJSON: `{"action":"replace","path":"quota.md","old_text":"x","new_text":"y"}`},
			{name: "rename", inputJSON: `{"action":"rename","path":"quota.md","new_path":"renamed.md","expected_text":"x"}`},
		}
		for _, test := range requests {
			t.Run(test.name, func(t *testing.T) {
				request := durableMemoryRequestForTest(t, admin,
					bridgeAPIScope("sesn_bridge_memory_retained_quota", "thr_bridge_memory_retained_quota", "bind_bridge_memory_retained_quota", 1, "pod_uid_memory_retained_quota"),
					"evt_tool_memory_retained_quota_"+test.name, test.inputJSON)
				_, err := store.RunMemory(context.Background(), request)
				var quota *memory.RequestTooLargeError
				if !errors.As(err, &quota) {
					t.Fatalf("RunMemory %s err = %T %v; want retained payload quota", test.name, err, err)
				}
				assertBridgeAPIMemoryHead(t, admin, "memstore_bridge_memory_retained_quota", "/quota.md", "x")
				if count := countMemoryVersions(t, admin, "memstore_bridge_memory_retained_quota"); count != 2 {
					t.Fatalf("version count after %s retained quota rejection = %d; want 2", test.name, count)
				}
				assertNoBridgeAPIRuntimeToolResult(t, admin, "sesn_bridge_memory_retained_quota", "evt_tool_memory_retained_quota_"+test.name)
			})
		}
	})
}

func TestPostgreSQLBridgeAPIStoreRunMemoryRequiresWritableSessionBinding(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_missing", "thr_bridge_memory_missing")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_missing", "bind_bridge_memory_missing", 1, "pod_uid_memory_missing")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	missing := durableMemoryRequestForTest(t, admin,
		bridgeAPIScope("sesn_bridge_memory_missing", "thr_bridge_memory_missing", "bind_bridge_memory_missing", 1, "pod_uid_memory_missing"),
		"evt_tool_memory_missing", `{"action":"create","path":"notes/missing.md","content":"one"}`)
	missingResponse, err := store.RunMemory(context.Background(), missing)
	if err != nil {
		t.Fatalf("RunMemory missing binding: %v", err)
	}
	assertMemoryToolErrorCode(t, committedMemoryResultJSON(t, missingResponse), "memory_store_not_configured")
	assertMemoryProjectionStateNull(t, admin, "sesn_bridge_memory_missing", "evt_tool_memory_missing")

	seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_detached", "thr_bridge_memory_detached")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_detached", "bind_bridge_memory_detached", 1, "pod_uid_memory_detached")
	seedBridgeAPIDetachedMemoryStoreBinding(t, admin, "default", "sesn_bridge_memory_detached", "memstore_bridge_memory_detached", "read_write", "/mnt/memory/detached")
	detached := durableMemoryRequestForTest(t, admin,
		bridgeAPIScope("sesn_bridge_memory_detached", "thr_bridge_memory_detached", "bind_bridge_memory_detached", 1, "pod_uid_memory_detached"),
		"evt_tool_memory_detached", `{"action":"create","path":"notes/detached.md","content":"one"}`)
	detachedResponse, err := store.RunMemory(context.Background(), detached)
	if err != nil {
		t.Fatalf("RunMemory detached binding: %v", err)
	}
	assertMemoryToolErrorCode(t, committedMemoryResultJSON(t, detachedResponse), "memory_store_not_configured")
	assertMemoryProjectionStateNull(t, admin, "sesn_bridge_memory_detached", "evt_tool_memory_detached")

	seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_ambiguous", "thr_bridge_memory_ambiguous")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_ambiguous", "bind_bridge_memory_ambiguous", 1, "pod_uid_memory_ambiguous")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_ambiguous", "memstore_bridge_memory_a")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_ambiguous", "memstore_bridge_memory_b")
	ambiguous := durableMemoryRequestForTest(t, admin,
		bridgeAPIScope("sesn_bridge_memory_ambiguous", "thr_bridge_memory_ambiguous", "bind_bridge_memory_ambiguous", 1, "pod_uid_memory_ambiguous"),
		"evt_tool_memory_ambiguous", `{"action":"create","path":"notes/ambiguous.md","content":"one"}`)
	ambiguousResponse, err := store.RunMemory(context.Background(), ambiguous)
	if err != nil {
		t.Fatalf("RunMemory ambiguous binding: %v", err)
	}
	assertMemoryToolErrorCode(t, committedMemoryResultJSON(t, ambiguousResponse), "memory_store_ambiguous")
	assertMemoryProjectionStateNull(t, admin, "sesn_bridge_memory_ambiguous", "evt_tool_memory_ambiguous")
}

func TestPostgreSQLBridgeAPIStoreRunMemorySkipsRefreshForValidationErrors(t *testing.T) {
	tests := []struct {
		name      string
		inputJSON string
		wantCode  string
		seed      func(t *testing.T, db *sql.DB, storeID string)
	}{
		{name: "invalid action", inputJSON: `{"action":"unknown","path":"notes/todo.md"}`, wantCode: "invalid_action"},
		{name: "invalid path absolute", inputJSON: `{"action":"create","path":"/absolute","content":"one"}`, wantCode: "invalid_path"},
		{name: "invalid path dotdot", inputJSON: `{"action":"create","path":"../bad","content":"one"}`, wantCode: "invalid_path"},
		{name: "invalid path runtime projection path", inputJSON: `{"action":"create","path":"mnt/memory/notes.md","content":"one"}`, wantCode: "invalid_path"},
		{name: "invalid path runtime projection prefix", inputJSON: `{"action":"create","path":"mnt/memory-old/notes.md","content":"one"}`, wantCode: "invalid_path"},
		{name: "invalid path too long", inputJSON: memoryCreateInputJSON(t, strings.Repeat("a", 1024), "one"), wantCode: "invalid_path"},
		{name: "missing content", inputJSON: `{"action":"create","path":"notes/todo.md"}`, wantCode: "missing_content"},
		{name: "content too large", inputJSON: memoryCreateInputJSON(t, "notes/large.md", strings.Repeat("x", 102401)), wantCode: "content_too_large"},
		{name: "missing replace text", inputJSON: `{"action":"replace","path":"notes/todo.md","old_text":"one"}`, wantCode: "missing_replace_text"},
		{name: "missing expected text delete", inputJSON: `{"action":"delete","path":"notes/todo.md"}`, wantCode: "missing_expected_text"},
		{name: "missing expected text rename", inputJSON: `{"action":"rename","path":"notes/todo.md","new_path":"notes/new.md"}`, wantCode: "missing_expected_text"},
	}
	for index, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID := "sesn_bridge_memory_validation_" + strconv.Itoa(index)
			threadID := "thr_bridge_memory_validation_" + strconv.Itoa(index)
			bindingID := "bind_bridge_memory_validation_" + strconv.Itoa(index)
			storeID := "memstore_bridge_memory_validation_" + strconv.Itoa(index)
			toolUseID := "evt_tool_memory_validation_" + strconv.Itoa(index)
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, "pod_uid_memory_validation")
			seedBridgeAPIWritableMemoryStore(t, admin, "default", sessionID, storeID)
			if tc.seed != nil {
				tc.seed(t, admin, storeID)
			}
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			request := durableMemoryRequestForTest(t, admin,
				bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_memory_validation"), toolUseID, tc.inputJSON)
			response, err := store.RunMemory(context.Background(), request)
			if err != nil {
				t.Fatalf("RunMemory validation error: %v", err)
			}
			assertMemoryToolError(t, committedMemoryResultJSON(t, response), tc.wantCode, false)
			assertMemoryProjectionStateNull(t, admin, sessionID, toolUseID)
		})
	}
}

func TestPostgreSQLBridgeAPIStoreRunMemoryRejectsInvalidInputs(t *testing.T) {
	tests := []struct {
		name       string
		inputJSON  string
		wantCode   string
		wantReread bool
	}{
		{name: "invalid action", inputJSON: `{"action":"unknown","path":"notes/todo.md"}`, wantCode: "invalid_action"},
		{name: "invalid path absolute", inputJSON: `{"action":"create","path":"/absolute","content":"one"}`, wantCode: "invalid_path"},
		{name: "invalid path dotdot", inputJSON: `{"action":"create","path":"notes/../bad","content":"one"}`, wantCode: "invalid_path"},
		{name: "invalid path runtime projection path", inputJSON: `{"action":"create","path":"mnt/memory/notes.md","content":"one"}`, wantCode: "invalid_path"},
		{name: "invalid path runtime projection prefix", inputJSON: `{"action":"create","path":"mnt/memory-old/notes.md","content":"one"}`, wantCode: "invalid_path"},
		{name: "invalid path too long", inputJSON: memoryCreateInputJSON(t, strings.Repeat("a", 1024), "one"), wantCode: "invalid_path"},
		{name: "missing content", inputJSON: `{"action":"create","path":"notes/todo.md"}`, wantCode: "missing_content"},
		{name: "content too large", inputJSON: memoryCreateInputJSON(t, "notes/large.md", strings.Repeat("x", 102401)), wantCode: "content_too_large"},
		{name: "missing replace text", inputJSON: `{"action":"replace","path":"notes/todo.md","old_text":"one"}`, wantCode: "missing_replace_text"},
		{name: "missing expected text delete", inputJSON: `{"action":"delete","path":"notes/todo.md"}`, wantCode: "missing_expected_text"},
		{name: "missing expected text rename", inputJSON: `{"action":"rename","path":"notes/todo.md","new_path":"notes/new.md"}`, wantCode: "missing_expected_text"},
		{name: "not found", inputJSON: `{"action":"delete","path":"notes/missing.md","expected_text":"gone"}`, wantCode: "not_found", wantReread: true},
	}
	for index, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID := "sesn_bridge_memory_invalid_" + strconv.Itoa(index)
			threadID := "thr_bridge_memory_invalid_" + strconv.Itoa(index)
			bindingID := "bind_bridge_memory_invalid_" + strconv.Itoa(index)
			storeID := "memstore_bridge_memory_invalid_" + strconv.Itoa(index)
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, "pod_uid_memory_invalid")
			seedBridgeAPIWritableMemoryStore(t, admin, "default", sessionID, storeID)
			seedBridgeAPIMemory(t, admin, "default", storeID, "mem_bridge_memory_invalid_"+strconv.Itoa(index), "/notes/todo.md", "one")

			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			request := durableMemoryRequestForTest(t, admin,
				bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_memory_invalid"),
				"evt_tool_memory_invalid_"+strconv.Itoa(index), tc.inputJSON)
			response, err := store.RunMemory(context.Background(), request)
			if err != nil {
				t.Fatalf("RunMemory invalid input: %v", err)
			}
			assertMemoryToolError(t, committedMemoryResultJSON(t, response), tc.wantCode, tc.wantReread)
		})
	}
}

func TestPostgreSQLBridgeAPIStoreRunMemoryRejectsOversizedReplaceBeforeDurableWrite(t *testing.T) {
	tests := []struct {
		name      string
		content   string
		inputJSON string
	}{
		{
			name:      "single replacement final content",
			content:   "one",
			inputJSON: memoryReplaceInputJSON(t, "notes/todo.md", "one", strings.Repeat("x", memoryToolContentMaxBytes+1), false),
		},
		{
			name:      "replace all final content",
			content:   "one one",
			inputJSON: memoryReplaceInputJSON(t, "notes/todo.md", "o", strings.Repeat("x", memoryToolContentMaxBytes/2), true),
		},
	}
	for index, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID := "sesn_bridge_memory_replace_cap_" + strconv.Itoa(index)
			threadID := "thr_bridge_memory_replace_cap_" + strconv.Itoa(index)
			bindingID := "bind_bridge_memory_replace_cap_" + strconv.Itoa(index)
			storeID := "memstore_bridge_memory_replace_cap_" + strconv.Itoa(index)
			toolUseID := "evt_tool_memory_replace_cap_" + strconv.Itoa(index)
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, "pod_uid_memory_replace_cap")
			seedBridgeAPIWritableMemoryStore(t, admin, "default", sessionID, storeID)
			seedBridgeAPIMemory(t, admin, "default", storeID, "mem_bridge_memory_replace_cap_"+strconv.Itoa(index), "/notes/todo.md", tc.content)
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			request := durableMemoryRequestForTest(t, admin,
				bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_memory_replace_cap"), toolUseID, tc.inputJSON)
			response, err := store.RunMemory(context.Background(), request)
			if err != nil {
				t.Fatalf("RunMemory oversized replace: %v", err)
			}
			assertMemoryToolError(t, committedMemoryResultJSON(t, response), "content_too_large", false)
			assertMemoryProjectionStateNull(t, admin, sessionID, toolUseID)
			if count := countMemoryVersions(t, admin, storeID); count != 1 {
				t.Fatalf("memory versions after oversized replace = %d; want seed version only", count)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreRunMemoryDeletesWithExpectedText(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_delete", "thr_bridge_memory_delete")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_delete", "bind_bridge_memory_delete", 1, "pod_uid_memory_delete")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_delete", "memstore_bridge_memory_delete")
	seedBridgeAPIMemory(t, admin, "default", "memstore_bridge_memory_delete", "mem_bridge_memory_delete", "/notes/delete.md", "delete me")
	seedBridgeAPIMemory(t, admin, "default", "memstore_bridge_memory_delete", "mem_bridge_memory_delete_other", "/notes/other.md", "keep me")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_bridge_memory_delete", "thr_bridge_memory_delete", "bind_bridge_memory_delete", 1, "pod_uid_memory_delete")
	deleteRequest := durableMemoryRequestForTest(t, admin, scope, "evt_tool_memory_delete",
		`{"action":"delete","path":"notes/delete.md","expected_text":"delete me"}`)
	deleted, err := store.RunMemory(context.Background(), deleteRequest)
	if err != nil {
		t.Fatalf("RunMemory delete: %v", err)
	}
	assertMemoryResultStatus(t, committedMemoryResultJSON(t, deleted), "completed")
	assertMemoryDeleted(t, admin, "memstore_bridge_memory_delete", "/notes/delete.md")

	wrongDeleteRequest := durableMemoryRequestForTest(t, admin, scope, "evt_tool_memory_delete_wrong",
		`{"action":"delete","path":"notes/other.md","expected_text":"wrong"}`)
	wrongDelete, err := store.RunMemory(context.Background(), wrongDeleteRequest)
	if err != nil {
		t.Fatalf("RunMemory wrong delete: %v", err)
	}
	assertMemoryToolError(t, committedMemoryResultJSON(t, wrongDelete), "expected_text_mismatch", true)
}

func TestPostgreSQLBridgeAPIStoreRunMemoryRenamesWithExpectedText(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_rename", "thr_bridge_memory_rename")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_rename", "bind_bridge_memory_rename", 1, "pod_uid_memory_rename")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_rename", "memstore_bridge_memory_rename")
	seedBridgeAPIMemory(t, admin, "default", "memstore_bridge_memory_rename", "mem_bridge_memory_rename", "/notes/old.md", "rename me")
	seedBridgeAPIMemory(t, admin, "default", "memstore_bridge_memory_rename", "mem_bridge_memory_rename_collision", "/notes/existing.md", "existing")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_bridge_memory_rename", "thr_bridge_memory_rename", "bind_bridge_memory_rename", 1, "pod_uid_memory_rename")

	wrongRenameRequest := durableMemoryRequestForTest(t, admin, scope, "evt_tool_memory_rename_wrong",
		`{"action":"rename","path":"notes/old.md","new_path":"notes/wrong.md","expected_text":"wrong"}`)
	wrongRename, err := store.RunMemory(context.Background(), wrongRenameRequest)
	if err != nil {
		t.Fatalf("RunMemory wrong rename: %v", err)
	}
	assertMemoryToolError(t, committedMemoryResultJSON(t, wrongRename), "expected_text_mismatch", true)

	collisionRequest := durableMemoryRequestForTest(t, admin, scope, "evt_tool_memory_rename_collision",
		`{"action":"rename","path":"notes/old.md","new_path":"notes/existing.md","expected_text":"rename me"}`)
	collision, err := store.RunMemory(context.Background(), collisionRequest)
	if err != nil {
		t.Fatalf("RunMemory rename collision: %v", err)
	}
	assertMemoryToolError(t, committedMemoryResultJSON(t, collision), "path_exists", true)

	renameRequest := durableMemoryRequestForTest(t, admin, scope, "evt_tool_memory_rename",
		`{"action":"rename","path":"notes/old.md","new_path":"notes/new.md","expected_text":"rename me"}`)
	renamed, err := store.RunMemory(context.Background(), renameRequest)
	if err != nil {
		t.Fatalf("RunMemory rename: %v", err)
	}
	renameResult := committedMemoryResultJSON(t, renamed)
	assertMemoryResultStatus(t, renameResult, "completed")
	if testJSONPathString(t, renameResult, "path") != "notes/old.md" {
		t.Fatalf("rename result = %s; want path notes/old.md", renameResult)
	}
	if testJSONPathString(t, renameResult, "new_path") != "notes/new.md" {
		t.Fatalf("rename result = %s; want new_path notes/new.md", renameResult)
	}
	assertMemoryCurrentPathContentAndOperation(t, admin, "memstore_bridge_memory_rename", "mem_bridge_memory_rename", "/notes/new.md", "rename me", "modified")
}

func TestPostgreSQLBridgeAPIStoreRunMemoryReportsStaleReplace(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_replace_stale", "thr_bridge_memory_replace_stale")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_replace_stale", "bind_bridge_memory_replace_stale", 1, "pod_uid_memory_replace_stale")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_replace_stale", "memstore_bridge_memory_replace_stale")
	seedBridgeAPIMemory(t, admin, "default", "memstore_bridge_memory_replace_stale", "mem_bridge_memory_replace_stale", "/notes/repeat.md", "one one")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_bridge_memory_replace_stale", "thr_bridge_memory_replace_stale", "bind_bridge_memory_replace_stale", 1, "pod_uid_memory_replace_stale")
	run := func(eventID, inputJSON string) string {
		t.Helper()
		request := durableMemoryRequestForTest(t, admin, scope, eventID, inputJSON)
		response, err := store.RunMemory(context.Background(), request)
		if err != nil {
			t.Fatalf("RunMemory %s: %v", eventID, err)
		}
		return committedMemoryResultJSON(t, response)
	}
	missing := run("evt_tool_memory_replace_missing",
		`{"action":"replace","path":"notes/repeat.md","old_text":"absent","new_text":"two"}`)
	assertMemoryToolError(t, missing, "old_text_not_found", true)

	nonUnique := run("evt_tool_memory_replace_nonunique",
		`{"action":"replace","path":"notes/repeat.md","old_text":"one","new_text":"two"}`)
	assertMemoryToolError(t, nonUnique, "old_text_not_unique", true)

	replaced := run("evt_tool_memory_replace_all",
		`{"action":"replace","path":"notes/repeat.md","old_text":"one","new_text":"two","replace_all":true}`)
	assertMemoryResultStatus(t, replaced, "completed")
	assertMemoryCurrentPathAndContent(t, admin, "memstore_bridge_memory_replace_stale", "mem_bridge_memory_replace_stale", "/notes/repeat.md", "two two")
}

func TestPostgreSQLBridgeAPIStoreRunMemoryRejectsPrefixConflictingPaths(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_prefix", "thr_bridge_memory_prefix")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_prefix", "bind_bridge_memory_prefix", 1, "pod_uid_memory_prefix")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_prefix", "memstore_bridge_memory_prefix")
	seedBridgeAPIMemory(t, admin, "default", "memstore_bridge_memory_prefix", "mem_bridge_memory_prefix_a", "/a", "a")
	seedBridgeAPIMemory(t, admin, "default", "memstore_bridge_memory_prefix", "mem_bridge_memory_prefix_parent_child", "/parent/child", "child")
	seedBridgeAPIMemory(t, admin, "default", "memstore_bridge_memory_prefix", "mem_bridge_memory_prefix_parent_other", "/parent/other", "other")
	seedBridgeAPIMemory(t, admin, "default", "memstore_bridge_memory_prefix", "mem_bridge_memory_prefix_percent", "/literal_%", "percent")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	scope := bridgeAPIScope("sesn_bridge_memory_prefix", "thr_bridge_memory_prefix", "bind_bridge_memory_prefix", 1, "pod_uid_memory_prefix")
	run := func(eventID, inputJSON string) string {
		t.Helper()
		request := durableMemoryRequestForTest(t, admin, scope, eventID, inputJSON)
		response, err := store.RunMemory(context.Background(), request)
		if err != nil {
			t.Fatalf("RunMemory %s: %v", eventID, err)
		}
		return committedMemoryResultJSON(t, response)
	}

	descendant := run("evt_tool_memory_prefix_descendant", `{"action":"create","path":"a/b","content":"child"}`)
	assertMemoryToolError(t, descendant, "path_exists", true)
	if got := testJSONPathString(t, descendant, "message"); got != "memory path is inside an existing memory" {
		t.Fatalf("descendant conflict message = %q; want distinguishing descendant message", got)
	}
	assertMemoryPathConflictResult(t, descendant, []memoryPathConflictWireHead{{MemoryID: "mem_bridge_memory_prefix_a", Path: "/a"}}, 1, false)

	ancestorCreate := run("evt_tool_memory_prefix_create_ancestor", `{"action":"create","path":"parent","content":"root"}`)
	assertMemoryToolError(t, ancestorCreate, "path_exists", true)
	if got := testJSONPathString(t, ancestorCreate, "message"); got != "memory path would contain an existing memory" {
		t.Fatalf("ancestor create conflict message = %q; want distinguishing ancestor message", got)
	}
	assertMemoryPathConflictResult(t, ancestorCreate, []memoryPathConflictWireHead{
		{MemoryID: "mem_bridge_memory_prefix_parent_child", Path: "/parent/child"},
		{MemoryID: "mem_bridge_memory_prefix_parent_other", Path: "/parent/other"},
	}, 2, false)

	exactRename := run("evt_tool_memory_prefix_rename_exact",
		`{"action":"rename","path":"literal_%","new_path":"a","expected_text":"percent"}`)
	assertMemoryToolError(t, exactRename, "path_exists", true)
	if got := testJSONPathString(t, exactRename, "message"); got != "memory target path already exists" {
		t.Fatalf("exact rename conflict message = %q; want exact collision message", got)
	}
	assertMemoryPathConflictResult(t, exactRename, []memoryPathConflictWireHead{{MemoryID: "mem_bridge_memory_prefix_a", Path: "/a"}}, 1, false)

	descendantRename := run("evt_tool_memory_prefix_rename_descendant",
		`{"action":"rename","path":"literal_%","new_path":"a/b","expected_text":"percent"}`)
	assertMemoryToolError(t, descendantRename, "path_exists", true)
	if got := testJSONPathString(t, descendantRename, "message"); got != "memory target path is inside an existing memory" {
		t.Fatalf("descendant rename conflict message = %q; want distinguishing descendant message", got)
	}
	assertMemoryPathConflictResult(t, descendantRename, []memoryPathConflictWireHead{{MemoryID: "mem_bridge_memory_prefix_a", Path: "/a"}}, 1, false)

	ancestorRename := run("evt_tool_memory_prefix_rename_ancestor",
		`{"action":"rename","path":"literal_%","new_path":"parent","expected_text":"percent"}`)
	assertMemoryToolError(t, ancestorRename, "path_exists", true)
	if got := testJSONPathString(t, ancestorRename, "message"); got != "memory target path would contain an existing memory" {
		t.Fatalf("ancestor rename conflict message = %q; want distinguishing ancestor message", got)
	}
	assertMemoryPathConflictResult(t, ancestorRename, []memoryPathConflictWireHead{
		{MemoryID: "mem_bridge_memory_prefix_parent_child", Path: "/parent/child"},
		{MemoryID: "mem_bridge_memory_prefix_parent_other", Path: "/parent/other"},
	}, 2, false)

	underscore := run("evt_tool_memory_prefix_underscore",
		`{"action":"create","path":"literal_X","content":"underscore is literal"}`)
	assertMemoryResultStatus(t, underscore, "completed")
}

func TestPostgreSQLBridgeAPIStoreRunMemoryBoundsPathConflictWire(t *testing.T) {
	for _, conflictTotal := range []int{MaxMemoryPathConflicts, MaxMemoryPathConflicts + 1} {
		t.Run(strconv.Itoa(conflictTotal), func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			suffix := strconv.Itoa(conflictTotal)
			sessionID := "sesn_bridge_memory_conflict_bound_" + suffix
			threadID := "thr_bridge_memory_conflict_bound_" + suffix
			storeID := "memstore_bridge_memory_conflict_bound_" + suffix
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_bridge_memory_conflict_bound", 1, "pod_uid_memory_conflict_bound")
			seedBridgeAPIWritableMemoryStore(t, admin, "default", sessionID, storeID)

			targetPath := "/root/target"
			seedBridgeAPIMemory(t, admin, "default", storeID, "mem_00_exact", targetPath, "exact")
			seedBridgeAPIMemory(t, admin, "default", storeID, "mem_01_ancestor", "/root", "ancestor")
			seedBridgeAPIMemory(t, admin, "default", storeID, "mem_99_deep", targetPath+"/deep/descendant", "deep")
			for index := 0; index < conflictTotal-3; index++ {
				memoryID := fmt.Sprintf("mem_%02d_descendant", index+2)
				pathValue := fmt.Sprintf("%s/child-%02d", targetPath, index)
				seedBridgeAPIMemory(t, admin, "default", storeID, memoryID, pathValue, memoryID)
			}
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
			request := durableMemoryRequestForTest(t, admin,
				bridgeAPIScope(sessionID, threadID, "bind_bridge_memory_conflict_bound", 1, "pod_uid_memory_conflict_bound"),
				"evt_tool_memory_conflict_bound_"+suffix, `{"action":"create","path":"root/target","content":"rejected"}`)
			response, err := store.RunMemory(context.Background(), request)
			if err != nil {
				t.Fatalf("RunMemory: %v", err)
			}
			resultJSON := committedMemoryResultJSON(t, response)
			assertMemoryToolError(t, resultJSON, "path_exists", true)

			wantHeads := []memoryPathConflictWireHead{
				{MemoryID: "mem_00_exact", Path: targetPath},
				{MemoryID: "mem_01_ancestor", Path: "/root"},
				{MemoryID: "mem_99_deep", Path: targetPath + "/deep/descendant"},
			}
			for index := 0; len(wantHeads) < min(conflictTotal, MaxMemoryPathConflicts); index++ {
				wantHeads = append(wantHeads, memoryPathConflictWireHead{
					MemoryID: fmt.Sprintf("mem_%02d_descendant", index+2),
					Path:     fmt.Sprintf("%s/child-%02d", targetPath, index),
				})
			}
			assertMemoryPathConflictResult(t, resultJSON, wantHeads, conflictTotal, conflictTotal > MaxMemoryPathConflicts)

		})
	}
}

func TestPostgreSQLBridgeAPIStoreAcceptSandboxExecutionReplayIsIdentityFenced(t *testing.T) {
	t.Run("durable identity and cross-kind conflicts", testPostgreSQLAcceptSandboxExecutionIdentityFencing)
}

func TestCanonicalRunToolInputMatchesJavaScriptStringifyEscaping(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "shell operators",
			raw:  `{"command":"printf '<>&' && cat <in >out"}`,
			want: `{"command":"printf '<>&' && cat <in >out"}`,
		},
		{
			name: "unicode line separators",
			raw:  "{\"command\":\"before\u2028middle\u2029after\"}",
			want: "{\"command\":\"before\u2028middle\u2029after\"}",
		},
		{
			name: "literal backslash u2028",
			raw:  `{"command":"\\u2028"}`,
			want: `{"command":"\\u2028"}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			canonical, hash, err := canonicalRunToolInput(test.raw)
			if err != nil {
				t.Fatalf("canonicalRunToolInput: %v", err)
			}
			if canonical != test.want || hash != sha256Hex(test.want) {
				t.Fatalf("canonical/hash = %q/%q; want JavaScript bytes %q/%q", canonical, hash, test.want, sha256Hex(test.want))
			}
		})
	}

	first, firstHash, err := canonicalRunToolInput(`{"workdir":"/workspace","cmd":"printf <ok>"}`)
	if err != nil {
		t.Fatalf("canonical first: %v", err)
	}
	second, secondHash, err := canonicalRunToolInput("{ \"cmd\" : \"printf <ok>\", \"workdir\" : \"/workspace\" }")
	if err != nil {
		t.Fatalf("canonical reordered: %v", err)
	}
	if first != second || firstHash != secondHash {
		t.Fatalf("reordered canonical/hash = %q/%q vs %q/%q; want equivalent", first, firstHash, second, secondHash)
	}
}

func TestCanonicalRunToolInputSharedCrossLanguageVectors(t *testing.T) {
	encoded, err := os.ReadFile(filepath.Join(repoRootFromBridgeTest(t), "testdata", "run-tool-canonical-vectors.json"))
	if err != nil {
		t.Fatalf("read shared canonical vectors: %v", err)
	}
	var vectors []struct {
		Name      string   `json:"name"`
		Inputs    []string `json:"inputs"`
		Canonical string   `json:"canonical"`
	}
	if err := json.Unmarshal(encoded, &vectors); err != nil {
		t.Fatalf("decode shared canonical vectors: %v", err)
	}
	for _, vector := range vectors {
		t.Run(vector.Name, func(t *testing.T) {
			for _, input := range vector.Inputs {
				canonical, hash, err := canonicalRunToolInput(input)
				if err != nil {
					t.Fatalf("canonicalRunToolInput(%q): %v", input, err)
				}
				if canonical != vector.Canonical || hash != sha256Hex(vector.Canonical) {
					t.Fatalf("canonical/hash = %q/%q; want shared vector %q/%q", canonical, hash, vector.Canonical, sha256Hex(vector.Canonical))
				}
			}
		})
	}
	if _, _, err := canonicalRunToolInput(strings.Repeat("[", 257) + "0" + strings.Repeat("]", 257)); err == nil || !strings.Contains(err.Error(), "nesting exceeds") {
		t.Fatalf("over-depth canonical error = %v; want shared closed nesting bound", err)
	}
	if _, _, err := canonicalRunToolInput(`{"unterminated":`); err == nil {
		t.Fatal("malformed canonical input accepted")
	}
}

func TestVerifyRuntimeScopeRejectsRuntimePodUIDMismatchFromIdentity(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_scope_identity", "thr_bridge_scope_identity")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_scope_identity", "bind_bridge_scope_identity", 1, "pod_uid_scope_identity")

	client := dbconnect.NewClientForTesting(runtime)
	scope := bridgeAPIScope("sesn_bridge_scope_identity", "thr_bridge_scope_identity", "bind_bridge_scope_identity", 1, "pod_uid_scope_identity")
	for _, test := range []struct {
		name     string
		ctx      context.Context
		wantCode codes.Code
	}{
		{
			name:     "no grpc identity keeps direct store tests usable",
			ctx:      context.Background(),
			wantCode: codes.OK,
		},
		{
			name: "bridge service account without pod uid is not a runtime caller",
			ctx: internalgrpcauth.ContextWithIdentity(context.Background(), internalgrpcauth.Identity{
				ServiceAccount: internalgrpcauth.ServiceAccount{Namespace: "tetral-system", Name: "bridge"},
			}),
			wantCode: codes.OK,
		},
		{
			name: "runtime pod with matching tokenreview pod uid",
			ctx: internalgrpcauth.ContextWithIdentity(context.Background(), internalgrpcauth.Identity{
				ServiceAccount:   internalgrpcauth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"},
				KubernetesPodUID: "pod_uid_scope_identity",
			}),
			wantCode: codes.OK,
		},
		{
			name: "runtime pod without tokenreview pod uid",
			ctx: internalgrpcauth.ContextWithIdentity(context.Background(), internalgrpcauth.Identity{
				ServiceAccount: internalgrpcauth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"},
			}),
			wantCode: codes.PermissionDenied,
		},
		{
			name: "runtime pod cannot claim another target pod uid",
			ctx: internalgrpcauth.ContextWithIdentity(context.Background(), internalgrpcauth.Identity{
				ServiceAccount:   internalgrpcauth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"},
				KubernetesPodUID: "pod_uid_other",
			}),
			wantCode: codes.PermissionDenied,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := client.WithWorkspaceTx(test.ctx, "default", "bridge_api.verify_runtime_scope_identity", func(tx *dbconnect.Tx) error {
				return verifyRuntimeScopeTx(test.ctx, tx, scope)
			})
			if status.Code(err) != test.wantCode {
				t.Fatalf("verifyRuntimeScopeTx error = %v; want %s", err, test.wantCode)
			}
			err = client.WithWorkspaceTx(test.ctx, "default", "bridge_api.verify_runtime_scope_identity_readonly", func(tx *dbconnect.Tx) error {
				return verifyRuntimeScopeReadOnlyTx(test.ctx, tx, scope)
			})
			if status.Code(err) != test.wantCode {
				t.Fatalf("verifyRuntimeScopeReadOnlyTx error = %v; want %s", err, test.wantCode)
			}
		})
	}
}

func TestVerifyRuntimeScopeRejectsDeletedSessionWithLiveBinding(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_deleted_scope", "thr_bridge_deleted_scope")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_deleted_scope", "bind_bridge_deleted_scope", 1, "pod_uid_deleted_scope")
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE sessions SET lifecycle_state = 'deleted' WHERE workspace_id = 'default' AND id = 'sesn_bridge_deleted_scope'`); err != nil {
		t.Fatalf("delete session: %v", err)
	}

	client := dbconnect.NewClientForTesting(runtime)
	scope := bridgeAPIScope("sesn_bridge_deleted_scope", "thr_bridge_deleted_scope", "bind_bridge_deleted_scope", 1, "pod_uid_deleted_scope")
	err := client.WithWorkspaceTx(context.Background(), "default", "bridge_api.verify_deleted_runtime_scope", func(tx *dbconnect.Tx) error {
		return verifyRuntimeScopeTx(context.Background(), tx, scope)
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("verifyRuntimeScopeTx error = %v; want FailedPrecondition", err)
	}
	var bindingRows int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_runtime_bindings WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_deleted_scope'`).Scan(&bindingRows); err != nil {
		t.Fatalf("count retained cleanup binding: %v", err)
	}
	if bindingRows != 1 {
		t.Fatalf("binding rows = %d; want retained for cleanup finalization", bindingRows)
	}
}

func TestPostgreSQLBridgeAPIStoreDurableToolOperationsReturnTypedStaleCustody(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_tool_typed_stale"
		threadID  = "thr_tool_typed_stale"
		bindingID = "bind_tool_typed_stale"
		podUID    = "pod_tool_typed_stale"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	stale := bridgeAPIScope(sessionID, threadID, bindingID, 2, podUID)

	accepted, err := store.AcceptSandboxExecution(context.Background(), &bridgev1.AcceptSandboxExecutionRequest{Scope: stale, ToolUseEventId: "evt_tool"})
	if err != nil || accepted.GetStale() == nil {
		t.Fatalf("AcceptSandboxExecution stale = %#v/%v", accepted, err)
	}
	awaited, err := store.AwaitSandboxExecution(context.Background(), &bridgev1.AwaitSandboxExecutionRequest{Scope: stale, ToolUseEventId: "evt_tool"})
	if err != nil || awaited.GetStale() == nil {
		t.Fatalf("AwaitSandboxExecution stale = %#v/%v", awaited, err)
	}
	read, err := store.ReadCommandResult(context.Background(), &bridgev1.ReadCommandResultRequest{Scope: stale, TaskId: "task_1", OperationId: "op_read", ToolUseEventId: "evt_read"})
	if err != nil || read.GetStale() == nil {
		t.Fatalf("ReadCommandResult stale = %#v/%v", read, err)
	}
	sent, err := store.SendCommandInput(context.Background(), &bridgev1.SendCommandInputRequest{Scope: stale, TaskId: "task_1", OperationId: "op_send", ToolUseEventId: "evt_send", InputJson: `{"chars":"x"}`})
	if err != nil || sent.GetStale() == nil {
		t.Fatalf("SendCommandInput stale = %#v/%v", sent, err)
	}
	cancelled, err := store.CancelCommand(context.Background(), &bridgev1.CancelCommandRequest{Scope: stale, TaskId: "task_1", OperationId: "op_cancel", ToolUseEventId: "evt_cancel"})
	if err != nil || cancelled.GetStale() == nil {
		t.Fatalf("CancelCommand stale = %#v/%v", cancelled, err)
	}
	memoryResult, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{Scope: stale, ToolUseEventId: "evt_memory"})
	if err != nil || memoryResult.GetStale() == nil {
		t.Fatalf("RunMemory stale = %#v/%v", memoryResult, err)
	}
}

func TestPostgreSQLBridgeAPIStoreAcceptAndAwaitSandboxExecutionByDurableTarget(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		workspaceID = "default"
		sessionID   = "sesn_sandbox_durable_target"
		threadID    = "thr_sandbox_durable_target"
		bindingID   = "bind_sandbox_durable_target"
		podUID      = "pod_sandbox_durable_target"
	)
	seedBridgeAPISession(t, admin, workspaceID, sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, workspaceID, sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	toolUseEventID := writeDurableOrdinaryToolUseForTest(
		t, store, scope, "mreq_sandbox_durable_target", "call_sandbox_durable_target",
		"exec_command", `{"cmd":"printf ok"}`,
	)

	request := &bridgev1.AcceptSandboxExecutionRequest{Scope: scope, ToolUseEventId: toolUseEventID}
	accepted, err := store.AcceptSandboxExecution(context.Background(), request)
	if err != nil || accepted.GetCommitted() == nil {
		t.Fatalf("AcceptSandboxExecution = %#v/%v; want committed", accepted, err)
	}
	var executionCount, queueCount int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_tool_results
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND tool_use_event_id=$4
		  AND tool_kind='sandbox_tool' AND tool_name='exec_command'
		  AND input_json='{"cmd":"printf ok"}' AND execution_state='pending'`,
		workspaceID, sessionID, threadID, toolUseEventID,
	).Scan(&executionCount); err != nil {
		t.Fatalf("read sandbox execution: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM queue_jobs
		WHERE workspace_id=$1 AND kind='sandbox_tool_execute' AND partition_key=$2 AND dedupe_key=$3`,
		workspaceID,
		queue.FormatSandboxExecutionPartitionKey(workspace.ID(workspaceID), sessionID, threadID, toolUseEventID),
		queue.FormatSandboxToolExecuteDedupeKey(workspace.ID(workspaceID), sessionID, threadID, toolUseEventID, 1),
	).Scan(&queueCount); err != nil {
		t.Fatalf("read sandbox queue job: %v", err)
	}
	if executionCount != 1 || queueCount != 1 {
		t.Fatalf("execution/queue = %d/%d; want 1/1", executionCount, queueCount)
	}

	type awaitResult struct {
		response *bridgev1.AwaitSandboxExecutionResponse
		err      error
	}
	done := make(chan awaitResult, 1)
	go func() {
		response, err := store.AwaitSandboxExecution(context.Background(), &bridgev1.AwaitSandboxExecutionRequest{
			Scope: scope, ToolUseEventId: toolUseEventID,
		})
		done <- awaitResult{response: response, err: err}
	}()
	select {
	case result := <-done:
		t.Fatalf("AwaitSandboxExecution returned before settlement: %#v/%v", result.response, result.err)
	case <-time.After(50 * time.Millisecond):
	}

	const terminalJSON = `{"status":"success","result":{"exit_code":0,"stdout":"ok"}}`
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_tool_results
		SET execution_state='terminal_unconsumed', result_json=$5, result_digest=$6, updated_at=now()
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND tool_use_event_id=$4`,
		workspaceID, sessionID, threadID, toolUseEventID, terminalJSON, sha256Hex(terminalJSON),
	); err != nil {
		t.Fatalf("settle sandbox execution: %v", err)
	}
	select {
	case result := <-done:
		if result.err != nil || result.response.GetCompleted().GetResultJson() != terminalJSON {
			t.Fatalf("AwaitSandboxExecution = %#v/%v; want durable result", result.response, result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("AwaitSandboxExecution did not observe durable settlement")
	}

	replayed, err := store.AcceptSandboxExecution(context.Background(), request)
	if err != nil || replayed.GetDuplicate() == nil {
		t.Fatalf("AcceptSandboxExecution replay = %#v/%v; want duplicate", replayed, err)
	}
	replayedResult, err := store.AwaitSandboxExecution(context.Background(), &bridgev1.AwaitSandboxExecutionRequest{
		Scope: scope, ToolUseEventId: toolUseEventID,
	})
	if err != nil || replayedResult.GetCompleted().GetResultJson() != terminalJSON {
		t.Fatalf("AwaitSandboxExecution replay = %#v/%v", replayedResult, err)
	}
}

func TestPostgreSQLBridgeAPIStoreAcceptSandboxExecutionReplayUsesDurableIdentityFence(t *testing.T) {
	testPostgreSQLAcceptSandboxExecutionIdentityFencing(t)
}

func TestPostgreSQLBridgeAPIStoreRunMemoryUsesDurableInputAndReplays(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_memory_durable_target"
		threadID  = "thr_memory_durable_target"
		bindingID = "bind_memory_durable_target"
		podUID    = "pod_memory_durable_target"
		storeID   = "memstore_memory_durable_target"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIWritableMemoryStore(t, admin, "default", sessionID, storeID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	toolUseEventID := writeDurableOrdinaryToolUseForTest(
		t, store, scope, "mreq_memory_durable_target", "call_memory_durable_target",
		"memory", `{"action":"create","path":"notes/todo.md","content":"one"}`,
	)
	request := &bridgev1.RunMemoryRequest{Scope: scope, ToolUseEventId: toolUseEventID}
	committed, err := store.RunMemory(context.Background(), request)
	if err != nil || committed.GetCommitted() == nil {
		t.Fatalf("RunMemory = %#v/%v; want committed", committed, err)
	}
	assertMemoryResultStatus(t, committed.GetCommitted().GetResultJson(), "completed")
	replayed, err := store.RunMemory(context.Background(), request)
	if err != nil || replayed.GetDuplicate().GetResultJson() != committed.GetCommitted().GetResultJson() {
		t.Fatalf("RunMemory replay = %#v/%v; want duplicate exact result", replayed, err)
	}

	var content string
	if err := admin.QueryRowContext(context.Background(), `SELECT v.content
		FROM memories m JOIN memory_versions v
		  ON v.workspace_id=m.workspace_id AND v.memory_store_id=m.memory_store_id
		 AND v.memory_id=m.memory_id AND v.memory_version_id=m.current_version_id
		WHERE m.workspace_id='default' AND m.memory_store_id=$1 AND m.path='/notes/todo.md' AND m.deleted_at IS NULL`,
		storeID,
	).Scan(&content); err != nil {
		t.Fatalf("read durable memory: %v", err)
	}
	if content != "one" {
		t.Fatalf("durable memory content = %q; want one", content)
	}

	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events
		SET payload_json='{"type":"agent.tool_use","name":"memory","input":{"action":"create","path":"notes/other.md","content":"two"},"evaluated_permission":"allow"}'
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, toolUseEventID); err != nil {
		t.Fatalf("mutate public memory Tool input: %v", err)
	}
	if replayed, err := store.RunMemory(context.Background(), request); err != nil || replayed.GetDuplicate().GetResultJson() != committed.GetCommitted().GetResultJson() {
		t.Fatalf("RunMemory after public input change = %#v/%v; want duplicate exact result", replayed, err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events
		SET projection_json = jsonb_set(
			projection_json::jsonb,
			'{canonical_execution_input}',
			'{"action":"create","path":"notes/other.md","content":"two"}'::jsonb
		)
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, toolUseEventID); err != nil {
		t.Fatalf("mutate canonical memory Tool input: %v", err)
	}
	if _, err := store.RunMemory(context.Background(), request); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("RunMemory after canonical execution identity change = %v; want AlreadyExists", err)
	}
}

func TestPostgreSQLBridgeAPIStoreRunMemoryRejectsMissingDurableTarget(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_memory_missing_target", "thr_memory_missing_target")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_memory_missing_target", "bind_memory_missing_target", 1, "pod_memory_missing_target")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	_, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:          bridgeAPIScope("sesn_memory_missing_target", "thr_memory_missing_target", "bind_memory_missing_target", 1, "pod_memory_missing_target"),
		ToolUseEventId: "evt_memory_missing_target",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("RunMemory missing durable target = %v; want FailedPrecondition", err)
	}
}

func writeDurableOrdinaryToolUseForTest(
	t *testing.T,
	store *PostgreSQLBridgeAPIStore,
	scope *bridgev1.RuntimeScope,
	modelRequestID string,
	modelToolCallID string,
	toolName string,
	inputJSON string,
) string {
	t.Helper()
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_"+modelRequestID+"_start", modelRequestID, "agent_provider_request", 0)
	payloadJSON := `{"type":"agent.tool_use","name":"` + toolName + `","input":` + inputJSON + `,"evaluated_permission":"allow"}`
	response, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_" + modelRequestID + "_tool", ModelRequestId: modelRequestID,
		EventType: "agent.tool_use", PayloadJson: payloadJSON, SessionVisible: true,
		AssistantContextDelta:       bridgeToolCallContextDeltaForTest(modelToolCallID, toolName, inputJSON),
		CanonicalExecutionInputJson: inputJSON,
	})
	if err != nil || response.GetCommitted() == nil {
		t.Fatalf("write durable ordinary Tool use: response=%#v err=%v", response, err)
	}
	return response.GetCommitted().GetEventId()
}

func durableMemoryRequestForTest(
	t *testing.T,
	db *sql.DB,
	scope *bridgev1.RuntimeScope,
	toolUseEventID string,
	inputJSON string,
) *bridgev1.RunMemoryRequest {
	t.Helper()
	if !json.Valid([]byte(inputJSON)) {
		t.Fatalf("durable memory Tool input is invalid JSON: %s", inputJSON)
	}
	sequence := nextBridgeAPIEventSequenceForTest(t, db, scope.GetSessionId(), scope.GetSessionThreadId())
	seedBridgeAPIEvent(
		t,
		db,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		toolUseEventID,
		sequence,
		"agent.tool_use",
		`{"type":"agent.tool_use","name":"memory","input":`+inputJSON+`,"evaluated_permission":"allow"}`,
	)
	modelRequestID := "mreq_" + toolUseEventID
	modelToolCallID := "call_" + toolUseEventID
	projectionJSON, err := json.Marshal(map[string]any{
		"model_tool_call_id":        modelToolCallID,
		"tool_name":                 "memory",
		"provider_input":            json.RawMessage(inputJSON),
		"canonical_execution_input": json.RawMessage(inputJSON),
	})
	if err != nil {
		t.Fatalf("marshal durable memory Tool projection: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`UPDATE session_events SET model_request_id=$3, projection_json=$4
		  WHERE workspace_id=$1 AND event_id=$2`,
		scope.GetWorkspaceId(), toolUseEventID, modelRequestID,
		string(projectionJSON),
	); err != nil {
		t.Fatalf("stamp durable memory Tool facts: %v", err)
	}
	return &bridgev1.RunMemoryRequest{Scope: scope, ToolUseEventId: toolUseEventID}
}

func committedMemoryResultJSON(t *testing.T, response *bridgev1.RunMemoryResponse) string {
	t.Helper()
	if response.GetCommitted() == nil {
		t.Fatalf("RunMemory response = %#v; want committed", response)
	}
	return response.GetCommitted().GetResultJson()
}

func duplicateMemoryResultJSON(t *testing.T, response *bridgev1.RunMemoryResponse) string {
	t.Helper()
	if response.GetDuplicate() == nil {
		t.Fatalf("RunMemory response = %#v; want duplicate", response)
	}
	return response.GetDuplicate().GetResultJson()
}
