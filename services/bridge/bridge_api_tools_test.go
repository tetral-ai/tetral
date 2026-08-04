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
		`UPDATE session_events SET model_request_id = $2 WHERE workspace_id = $1 AND event_id = $3`,
		workspaceID, modelRequest, toolUseID,
	); err != nil {
		t.Fatalf("stamp durable tool-use model request: %v", err)
	}
	seedBridgeAPIDurableToolMessage(t, admin, workspaceID, sessionID, threadID, modelRequest, toolUseID, modelCallID, "exec_command")
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
		Scope:               bridgeAPIScope(sessionID, threadID, "bind_bridge_durable_tool", 1, "pod_uid_bridge_durable_tool"),
		ToolUseEventId:      toolUseID,
		ModelToolCallId:     modelCallID,
		NormalizedInputHash: sha256Hex(`{"cmd":"printf ok"}`),
		ToolName:            "exec_command",
		InputJson:           `{"cmd":"printf ok"}`,
	}
	accepted, err := store.AcceptSandboxExecution(context.Background(), request)
	if err != nil {
		t.Fatalf("AcceptSandboxExecution: %v", err)
	}
	if accepted.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("AcceptSandboxExecution ack = %s; want committed", accepted.GetAck().GetStatus())
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
		if completed.response.GetResultJson() != terminalResult {
			t.Fatalf("AwaitSandboxExecution response = %+v; want durable result", completed.response)
		}
		if completed.response.GetResultDigest() != sha256Hex(terminalResult) {
			t.Fatalf("AwaitSandboxExecution digest = %q; want durable result digest", completed.response.GetResultDigest())
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
	replayRequest.Scope.RequestId = "req_bridge_durable_tool_replay"
	replayStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	replay, err := replayStore.AcceptSandboxExecution(context.Background(), replayRequest)
	if err != nil {
		t.Fatalf("AcceptSandboxExecution replay from another Bridge store: %v", err)
	}
	if replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE {
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
	if err != nil || replayedResult.GetResultJson() != terminalResult {
		t.Fatalf("AwaitSandboxExecution behind unrelated Session lock = %+v, %v; want durable result", replayedResult, err)
	}
	conflict := proto.Clone(replayRequest).(*bridgev1.AcceptSandboxExecutionRequest)
	conflict.Scope.RequestId = "req_bridge_durable_tool_conflict"
	conflict.ModelToolCallId = "call_bridge_durable_tool_changed"
	if _, err := store.AcceptSandboxExecution(context.Background(), conflict); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("AcceptSandboxExecution model call conflict error = %v; want AlreadyExists", err)
	}
}

func TestPostgreSQLBridgeAPIStoreAcceptSandboxExecutionFromSharedAssistantProjection(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		workspaceID    = "default"
		sessionID      = "sesn_bridge_shared_tools"
		threadID       = "thr_bridge_shared_tools"
		bindingID      = "bind_bridge_shared_tools"
		podUID         = "pod_bridge_shared_tools"
		modelRequestID = "mreq_bridge_shared_tools"
	)
	seedBridgeAPISession(t, admin, workspaceID, sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, workspaceID, sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("bridge-shared-tool-projection-key")
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_bridge_shared_start", modelRequestID, "agent_provider_request", 0)

	encoded, err := os.ReadFile(filepath.Join(repoRootFromBridgeTest(t), "testdata", "stable-reasoning-anchor-vector.json"))
	if err != nil {
		t.Fatalf("read stable reasoning fixture: %v", err)
	}
	var fixture struct {
		Parts []struct {
			ReasoningPartID string `json:"reasoning_part_id"`
			ProviderPartID  string `json:"provider_part_id"`
			PartSequence    int32  `json:"part_sequence"`
			Text            string `json:"text"`
			MetadataJSON    string `json:"metadata_json"`
			Truncated       bool   `json:"truncated"`
		} `json:"stable_reasoning_parts"`
	}
	if err := json.Unmarshal(encoded, &fixture); err != nil {
		t.Fatalf("decode stable reasoning fixture: %v", err)
	}
	if len(fixture.Parts) != 2 {
		t.Fatalf("stable reasoning fixture parts = %d; want 2", len(fixture.Parts))
	}
	reasoning := make([]*bridgev1.StableReasoningPart, 0, len(fixture.Parts))
	draftParts := make([]bridgeRuntimePartDraftForTest, 0, len(fixture.Parts)+4)
	for _, part := range fixture.Parts {
		reasoning = append(reasoning, &bridgev1.StableReasoningPart{
			ReasoningPartId: part.ReasoningPartID,
			ProviderPartId:  part.ProviderPartID,
			PartSequence:    part.PartSequence,
			Text:            part.Text,
			MetadataJson:    part.MetadataJSON,
			Truncated:       part.Truncated,
		})
		draftParts = append(draftParts, bridgeRuntimePartDraftForTest{
			kind: "reasoning",
			json: fmt.Sprintf(
				`{"type":"reasoning","providerPartId":%q,"providerMetadata":%s,"text":%q,"truncated":%t,"status":"completed"}`,
				part.ProviderPartID, part.MetadataJSON, part.Text, part.Truncated,
			),
		})
	}

	type acceptedTool struct {
		eventID  string
		callID   string
		toolName string
		input    string
		hash     string
	}
	inputs := []string{
		`{"cmd":"printf one"}`,
		`{"cmd":"printf two"}`,
		`{"cmd":"` + strings.Repeat("x", 9000) + `"}`,
		`{"cmd":"printf four"}`,
	}
	tools := make([]acceptedTool, 0, len(inputs))
	for index, input := range inputs {
		canonicalInput, inputHash, err := canonicalRunToolInput(input)
		if err != nil {
			t.Fatalf("canonical tool input %d: %v", index, err)
		}
		callID := fmt.Sprintf("call_bridge_shared_%d", index+1)
		runtimeWriteID := fmt.Sprintf("rwrite_bridge_shared_%d", index+1)
		var exactInput any
		if err := json.Unmarshal([]byte(canonicalInput), &exactInput); err != nil {
			t.Fatalf("decode canonical tool input %d: %v", index, err)
		}
		inputValue := map[string]any{"value": exactInput, "preview": canonicalInput, "truncated": false}
		if index == 2 {
			inputValue = map[string]any{"preview": canonicalInput[:8192], "truncated": true}
		}
		partJSON, err := json.Marshal(map[string]any{
			"type":       "tool",
			"toolCallId": callID,
			"toolName":   "exec_command",
			"state": map[string]any{
				"status": "running",
				"input":  inputValue,
			},
		})
		if err != nil {
			t.Fatalf("marshal tool part %d: %v", index, err)
		}
		draftParts = append(draftParts, bridgeRuntimePartDraftForTest{kind: "tool", json: string(partJSON)})
		request := &bridgev1.WriteEventRequest{
			Scope:          scope,
			RuntimeWriteId: runtimeWriteID,
			ModelRequestId: modelRequestID,
			EventType:      "agent.tool_use",
			PayloadJson:    `{"type":"agent.tool_use","name":"exec_command","input":` + canonicalInput + `,"evaluated_permission":"ask"}`,
			Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
				t, scope, runtimeWriteID, "agent.tool_use", "streaming", draftParts...,
			)},
		}
		if index == 0 {
			request.StableReasoningParts = reasoning
		}
		written, err := store.WriteEvent(context.Background(), request)
		if err != nil {
			t.Fatalf("WriteEvent shared tool %d: %v", index, err)
		}
		tools = append(tools, acceptedTool{
			eventID: written.GetEventId(), callID: callID, toolName: "exec_command", input: canonicalInput, hash: inputHash,
		})
		var stamped map[string]any
		if err := json.Unmarshal(partJSON, &stamped); err != nil {
			t.Fatalf("decode tool part %d: %v", index, err)
		}
		stamped["toolUseEventId"] = written.GetEventId()
		stamped["toolEvent"] = map[string]any{"kind": "tool"}
		stampedJSON, err := json.Marshal(stamped)
		if err != nil {
			t.Fatalf("stamp tool part %d: %v", index, err)
		}
		draftParts[len(draftParts)-1].json = string(stampedJSON)
	}
	var reboundPart map[string]any
	if err := json.Unmarshal([]byte(draftParts[len(fixture.Parts)].json), &reboundPart); err != nil {
		t.Fatalf("decode stamped tool part for rebound attempt: %v", err)
	}
	delete(reboundPart, "toolUseEventId")
	delete(reboundPart, "toolEvent")
	reboundJSON, err := json.Marshal(reboundPart)
	if err != nil {
		t.Fatalf("marshal rebound tool part: %v", err)
	}
	reboundParts := append([]bridgeRuntimePartDraftForTest(nil), draftParts...)
	reboundParts[len(fixture.Parts)].json = string(reboundJSON)
	if _, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          scope,
		RuntimeWriteId: "rwrite_bridge_shared_rebind",
		ModelRequestId: modelRequestID,
		EventType:      "agent.tool_use",
		PayloadJson:    `{"type":"agent.tool_use","name":"exec_command","input":` + tools[0].input + `,"evaluated_permission":"ask"}`,
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
			t, scope, "rwrite_bridge_shared_rebind", "agent.tool_use", "streaming", reboundParts...,
		)},
	}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("WriteEvent rebound stamped tool part error = %v; want AlreadyExists", err)
	}

	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_pending_tool_uses
		    SET status = 'resolving', decision = 'allow', updated_at = clock_timestamp()
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3`,
		workspaceID, sessionID, threadID,
	); err != nil {
		t.Fatalf("resolve shared tool approvals: %v", err)
	}
	loaded, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope: scope, RuntimeInputId: "rin_bridge_shared_tools_cold",
	})
	if err != nil {
		t.Fatalf("LoadContext shared tools: %v", err)
	}
	var contextPayload struct {
		PendingToolUses []struct {
			ToolUseEventID string          `json:"toolUseEventId"`
			Input          json.RawMessage `json:"input"`
		} `json:"pendingToolUses"`
	}
	if err := json.Unmarshal([]byte(loaded.GetContextJson()), &contextPayload); err != nil {
		t.Fatalf("decode shared tool context: %v", err)
	}
	if len(contextPayload.PendingToolUses) != len(tools) {
		t.Fatalf("cold pending tools = %d; want %d", len(contextPayload.PendingToolUses), len(tools))
	}
	loadedInput := make(map[string]string, len(contextPayload.PendingToolUses))
	for _, pending := range contextPayload.PendingToolUses {
		loadedInput[pending.ToolUseEventID] = string(pending.Input)
	}
	for _, tool := range tools {
		if loadedInput[tool.eventID] != tool.input {
			t.Fatalf("cold approval input for %s differs from exact durable input", tool.eventID)
		}
	}

	baseRequest := func(tool acceptedTool) *bridgev1.AcceptSandboxExecutionRequest {
		return &bridgev1.AcceptSandboxExecutionRequest{
			Scope: scope, ToolUseEventId: tool.eventID, ModelToolCallId: tool.callID,
			NormalizedInputHash: tool.hash, ToolName: tool.toolName, InputJson: tool.input,
		}
	}
	for name, mutate := range map[string]func(*bridgev1.AcceptSandboxExecutionRequest){
		"tool use event": func(request *bridgev1.AcceptSandboxExecutionRequest) { request.ToolUseEventId = tools[0].eventID },
		"model tool call": func(request *bridgev1.AcceptSandboxExecutionRequest) {
			request.ModelToolCallId = "call_bridge_shared_changed"
		},
		"tool name": func(request *bridgev1.AcceptSandboxExecutionRequest) { request.ToolName = "Read" },
		"canonical input": func(request *bridgev1.AcceptSandboxExecutionRequest) {
			request.InputJson = `{"cmd":"changed"}`
			request.NormalizedInputHash = sha256Hex(request.InputJson)
		},
	} {
		t.Run("reject changed "+name, func(t *testing.T) {
			request := baseRequest(tools[3])
			mutate(request)
			if _, err := store.AcceptSandboxExecution(context.Background(), request); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("AcceptSandboxExecution changed %s error = %v; want FailedPrecondition", name, err)
			}
		})
	}
	settledParts := append([]bridgeRuntimePartDraftForTest(nil), draftParts...)
	for index, tool := range tools {
		accepted, err := store.AcceptSandboxExecution(context.Background(), baseRequest(tool))
		if err != nil {
			t.Fatalf("AcceptSandboxExecution shared tool %d: %v", index, err)
		}
		if accepted.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
			t.Fatalf("shared tool %d ack = %s; want committed", index, accepted.GetAck().GetStatus())
		}
	}
	var messageCount, partCount, executionCount, queueCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*), COALESCE(sum(jsonb_array_length(data_json::jsonb->'parts')), 0)
		   FROM session_messages
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3 AND model_request_id = $4`,
		workspaceID, sessionID, threadID, modelRequestID,
	).Scan(&messageCount, &partCount); err != nil {
		t.Fatalf("count shared projection: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_runtime_tool_results
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3 AND tool_kind = 'sandbox_tool'`,
		workspaceID, sessionID, threadID,
	).Scan(&executionCount); err != nil {
		t.Fatalf("count shared executions: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM queue_jobs WHERE workspace_id = $1 AND kind = 'sandbox_tool_execute'`, workspaceID,
	).Scan(&queueCount); err != nil {
		t.Fatalf("count shared execution jobs: %v", err)
	}
	if messageCount != 1 || partCount != 6 || executionCount != 4 || queueCount != 4 {
		t.Fatalf("shared message/parts/executions/jobs = %d/%d/%d/%d; want 1/6/4/4", messageCount, partCount, executionCount, queueCount)
	}
	seedReadySandboxForSharedToolExecution(t, admin, workspaceID, sessionID)
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	queueConnection := startBackgroundNotificationQueueServer(t, queueStore)
	provider := newGatedBridgeToolProvider()
	registry, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{sandboxdriver.DaytonaProviderName: provider})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	pool, err := tetralsandbox.NewWorkspaceConsumerPool(4)
	if err != nil {
		t.Fatalf("NewWorkspaceConsumerPool: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	runDone := make(chan error, 1)
	go func() {
		runDone <- tetralsandbox.RunWorkspaceConsumerGroup(
			runCtx,
			4,
			pool,
			staticWorkspaceLister{workspace.ID(workspaceID)},
			time.Millisecond,
			func(cycleCtx context.Context, discovered workspace.ID) (bool, error) {
				return (&tetralsandbox.SandboxToolExecutionJobRunner{
					Queue:       tetralsandbox.SandboxQueueFromGRPC(queuev1.NewQueueServiceClient(queueConnection)),
					Coordinator: tetralsandbox.NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtime), 30*time.Minute),
					Providers:   registry,
					Media:       backgroundNotificationMedia{},
					Config: tetralsandbox.SandboxToolExecutionRunnerConfig{
						WorkspaceID: discovered.String(), LeaseOwner: "sandbox-shared-projection", MaxJobs: 1,
						LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second, PreparationTimeout: 45 * time.Second,
					},
				}).RunOnceWithActivity(cycleCtx)
			},
		)
	}()
	for range 4 {
		select {
		case <-provider.started:
		case <-time.After(5 * time.Second):
			t.Fatal("four accepted Sandbox executions did not overlap at the provider")
		}
	}
	if provider.maximum.Load() != 4 {
		t.Fatalf("shared projection provider concurrency = %d; want 4", provider.maximum.Load())
	}
	close(provider.release)
	deadline := time.Now().Add(5 * time.Second)
	for {
		var terminal, acknowledged int
		if err := admin.QueryRow(`SELECT count(*) FROM session_runtime_tool_results
			WHERE workspace_id=$1 AND session_id=$2 AND execution_state='terminal_unconsumed'`, workspaceID, sessionID).Scan(&terminal); err != nil {
			t.Fatalf("count shared terminal executions: %v", err)
		}
		if err := admin.QueryRow(`SELECT count(*) FROM queue_jobs
			WHERE workspace_id=$1 AND kind=$2 AND status='acknowledged'`, workspaceID, queue.KindSandboxToolExecute).Scan(&acknowledged); err != nil {
			t.Fatalf("count shared acknowledged executions: %v", err)
		}
		if terminal == 4 && acknowledged == 4 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("shared terminal/acknowledged executions = %d/%d; want 4/4", terminal, acknowledged)
		}
		time.Sleep(10 * time.Millisecond)
	}
	for index, tool := range tools {
		result, err := store.AwaitSandboxExecution(context.Background(), awaitSandboxExecutionRequest(baseRequest(tool)))
		if err != nil {
			t.Fatalf("AwaitSandboxExecution shared tool %d: %v", index, err)
		}
		if result.GetResultJson() == "" || result.GetResultDigest() == "" {
			t.Fatalf("shared tool %d result/digest is empty", index)
		}
		var exactInput any
		if err := json.Unmarshal([]byte(tool.input), &exactInput); err != nil {
			t.Fatalf("decode settled tool input %d: %v", index, err)
		}
		inputValue := map[string]any{"value": exactInput, "preview": tool.input, "truncated": false}
		if len(tool.input) > 8192 {
			inputValue = map[string]any{"preview": tool.input[:8192], "truncated": true}
		}
		partJSON, err := json.Marshal(map[string]any{
			"type": "tool", "toolCallId": tool.callID, "toolName": tool.toolName,
			"toolUseEventId": tool.eventID, "toolEvent": map[string]any{"kind": "tool"},
			"state": map[string]any{
				"status": "completed", "input": inputValue,
				"output": map[string]any{"text": "done", "truncated": false},
			},
		})
		if err != nil {
			t.Fatalf("marshal settled tool part %d: %v", index, err)
		}
		settledParts[len(reasoning)+index].json = string(partJSON)
		runtimeWriteID := fmt.Sprintf("rwrite_bridge_shared_result_%d", index+1)
		digest := result.GetResultDigest()
		messageStatus := "streaming"
		if index == len(tools)-1 {
			messageStatus = "completed"
		}
		written, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
			Scope: scope, RuntimeWriteId: runtimeWriteID, ModelRequestId: modelRequestID,
			EventType:           "agent.tool_result",
			PayloadJson:         `{"type":"agent.tool_result","tool_use_event_id":"` + tool.eventID + `","content":[{"type":"text","text":"done"}]}`,
			SandboxResultDigest: &digest,
			Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
				t, scope, runtimeWriteID, "agent.tool_result", messageStatus, settledParts...,
			)},
		})
		if err != nil {
			t.Fatalf("WriteEvent shared tool result %d: %v", index, err)
		}
		if written.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
			t.Fatalf("shared tool result %d ack = %s; want committed", index, written.GetAck().GetStatus())
		}
	}
	var resultEvents, consumedExecutions int
	if err := admin.QueryRow(`SELECT count(*) FROM session_events
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
		  AND model_request_id=$4 AND type='agent.tool_result'`, workspaceID, sessionID, threadID, modelRequestID).Scan(&resultEvents); err != nil {
		t.Fatalf("count shared Tool Result events: %v", err)
	}
	if err := admin.QueryRow(`SELECT count(*) FROM session_runtime_tool_results
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
		  AND execution_state='consumed'`, workspaceID, sessionID, threadID).Scan(&consumedExecutions); err != nil {
		t.Fatalf("count consumed shared executions: %v", err)
	}
	if resultEvents != 4 || consumedExecutions != 4 {
		t.Fatalf("shared result events/consumed executions = %d/%d; want 4/4", resultEvents, consumedExecutions)
	}
	cancelRun()
	if err := <-runDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("RunWorkspaceConsumerGroup error = %v; want context cancellation", err)
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
	if _, err := admin.Exec(`UPDATE session_events SET model_request_id=$2 WHERE workspace_id=$1 AND event_id=$3`, workspaceID, modelRequestID, toolUseEventID); err != nil {
		t.Fatalf("stamp Tool Use model request: %v", err)
	}
	seedBridgeAPIDurableToolMessage(t, admin, workspaceID, sessionID, threadID, modelRequestID, toolUseEventID, modelToolCallID, "Read")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	if _, err := store.AcceptSandboxExecution(context.Background(), &bridgev1.AcceptSandboxExecutionRequest{
		Scope: scope, ToolUseEventId: toolUseEventID, ModelToolCallId: modelToolCallID,
		NormalizedInputHash: sha256Hex(`{}`), ToolName: "Read", InputJson: `{}`,
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
	const resultJSON = `{"status":"success","result":{"text":"done"}}`
	resultDigest := sha256Hex(resultJSON)
	writeRequest := &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_sandbox_settle_consume_race", ModelRequestId: modelRequestID,
		EventType:           "agent.tool_result",
		PayloadJson:         `{"type":"agent.tool_result","tool_use_event_id":"` + toolUseEventID + `","content":[{"type":"text","text":"done"}]}`,
		SandboxResultDigest: &resultDigest,
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
			t, scope, "rwrite_sandbox_settle_consume_race", "agent.tool_result", "completed",
			bridgeRuntimePartDraftForTest{kind: "tool", json: `{"type":"tool","toolCallId":"` + modelToolCallID + `","toolName":"Read","toolUseEventId":"` + toolUseEventID + `","toolEvent":{"kind":"tool"},"state":{"status":"completed","input":{"value":{},"preview":"{}","truncated":false},"output":{"text":"done","truncated":false}}}`},
		)},
	}
	type writeResult struct {
		response *bridgev1.WriteEventResponse
		err      error
	}
	writeDone := make(chan writeResult, 1)
	go func() {
		response, err := store.WriteEvent(context.Background(), writeRequest)
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
			t.Fatalf("concurrent WriteEvent: %v", written.err)
		}
		written.response, written.err = store.WriteEvent(context.Background(), writeRequest)
	}
	if written.err != nil {
		t.Fatalf("WriteEvent after Sandbox settlement: %v", written.err)
	}
	if written.response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("WriteEvent status = %s; want committed", written.response.GetAck().GetStatus())
	}

	var executionState string
	var retainedResult sql.NullString
	var resultEvents int
	if err := admin.QueryRow(`SELECT execution_state, result_json FROM session_runtime_tool_results
		WHERE workspace_id=$1 AND session_id=$2 AND tool_use_event_id=$3`, workspaceID, sessionID, toolUseEventID).Scan(&executionState, &retainedResult); err != nil {
		t.Fatalf("read converged execution: %v", err)
	}
	if err := admin.QueryRow(`SELECT count(*) FROM session_events
		WHERE workspace_id=$1 AND session_id=$2 AND type='agent.tool_result'
		  AND payload_json::jsonb ->> 'tool_use_event_id'=$3`, workspaceID, sessionID, toolUseEventID).Scan(&resultEvents); err != nil {
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

func seedReadySandboxForSharedToolExecution(t *testing.T, db *sql.DB, workspaceID string, sessionID string) {
	t.Helper()
	environmentID := "env_" + sessionID
	if _, err := db.Exec(`UPDATE environments SET current_generation=1 WHERE workspace_id=$1 AND id=$2`, workspaceID, environmentID); err != nil {
		t.Fatalf("set shared-tool environment generation: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO environment_artifacts (
		workspace_id, environment_id, generation, status, provider, provider_artifact_ref,
		normalized_config_hash, artifact_input_hash, runtime_network_policy_json, packages_json,
		created_at, updated_at
	) VALUES ($1, $2, 1, 'ready', 'daytona', 'artifact_shared_tool_execution',
		'config_hash', 'artifact_hash', '{"type":"unrestricted"}', '{}', clock_timestamp(), clock_timestamp())`, workspaceID, environmentID); err != nil {
		t.Fatalf("seed shared-tool environment artifact: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO session_sandbox_bindings (
		workspace_id, session_id, logical_sandbox_id, environment_id, environment_generation,
		provider, provider_resource_id, binding_revision, materialized_resource_revision,
		resource_credential_expires_at, resource_roots_json, provider_metadata_json,
		helper_verified_at, created_at, updated_at
	) VALUES ($1, $2, $3, $4, 1, 'daytona', $5, 1, 1,
		clock_timestamp()+interval '2 hours', '[]', '{}', clock_timestamp(), clock_timestamp(), clock_timestamp())`,
		workspaceID, sessionID, "sbox_"+sessionID, environmentID, "provider_"+sessionID); err != nil {
		t.Fatalf("seed ready shared-tool Sandbox binding: %v", err)
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
				`UPDATE session_events SET model_request_id = $2 WHERE workspace_id = $1 AND event_id = $3`,
				"default", modelRequestID, toolUseID,
			); err != nil {
				t.Fatalf("stamp permission model request: %v", err)
			}
			seedBridgeAPIDurableToolMessage(t, admin, "default", sessionID, threadID, modelRequestID, toolUseID, modelToolCallID, "exec_command")
			if testCase.approvalStatus != "" {
				approvalInput := testCase.approvalInput
				if approvalInput == "" {
					approvalInput = `{"cmd":"printf ok"}`
				}
				if _, err := admin.ExecContext(context.Background(),
					`INSERT INTO session_pending_tool_uses (
						workspace_id, session_id, session_thread_id, tool_use_event_id, model_tool_call_id,
						tool_name, kind, input_json, decision, status, expires_at, created_at, updated_at
					) VALUES ('default', $1, $2, $3, $4, 'exec_command', 'approval', $5, $6, $7,
						'2026-01-01T01:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
					sessionID, threadID, toolUseID, modelToolCallID, approvalInput, testCase.approvalDecision, testCase.approvalStatus,
				); err != nil {
					t.Fatalf("seed permission approval: %v", err)
				}
			}

			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			_, err := store.AcceptSandboxExecution(context.Background(), &bridgev1.AcceptSandboxExecutionRequest{
				Scope:               bridgeAPIScope(sessionID, threadID, "bind_permission_"+suffix, 1, "pod_permission_"+suffix),
				ToolUseEventId:      toolUseID,
				ModelToolCallId:     modelToolCallID,
				NormalizedInputHash: sha256Hex(`{"cmd":"printf ok"}`),
				ToolName:            "exec_command",
				InputJson:           `{"cmd":"printf ok"}`,
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
		NormalizedInputHash: request.GetNormalizedInputHash(), ToolName: request.GetToolName(),
		InputJson: request.GetInputJson(), ModelToolCallId: request.GetModelToolCallId(),
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
		`UPDATE session_events SET model_request_id = 'mreq_bridge_released_tool' WHERE workspace_id = $1 AND event_id = $2`,
		workspaceID, toolUseID,
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
		Scope:               bridgeAPIScope(sessionID, threadID, "bind_bridge_released_tool", 1, "pod_uid_bridge_released_tool"),
		ToolUseEventId:      toolUseID,
		ModelToolCallId:     modelCallID,
		NormalizedInputHash: sha256Hex(`{"cmd":"true"}`),
		ToolName:            "exec_command",
		InputJson:           `{"cmd":"true"}`,
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
		`UPDATE session_events SET model_request_id = 'mreq_bridge_settled_tool' WHERE workspace_id = $1 AND event_id = $2`,
		workspaceID, toolUseID,
	); err != nil {
		t.Fatalf("stamp durable tool-use model request: %v", err)
	}
	seedBridgeAPIDurableToolMessage(t, admin, workspaceID, sessionID, threadID, "mreq_bridge_settled_tool", toolUseID, "call_bridge_settled_tool", "exec_command")
	seedBridgeAPIEvent(t, admin, workspaceID, sessionID, threadID, "evt_bridge_settled_result", 2, "agent.tool_result", `{"tool_use_event_id":"evt_bridge_settled_tool","content":[{"type":"text","text":"cancelled"}]}`)

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	_, err := store.AcceptSandboxExecution(context.Background(), &bridgev1.AcceptSandboxExecutionRequest{
		Scope:               bridgeAPIScope(sessionID, threadID, "bind_bridge_settled_tool", 1, "pod_uid_bridge_settled_tool"),
		ToolUseEventId:      toolUseID,
		ModelToolCallId:     "call_bridge_settled_tool",
		NormalizedInputHash: sha256Hex(`{"cmd":"true"}`),
		ToolName:            "exec_command",
		InputJson:           `{"cmd":"true"}`,
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
	scope := bridgeAPIScope("sesn_bridge_repair", "thr_bridge_repair", "bind_bridge_repair", 1, "pod_uid_repair")
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_bridge_repair_start", "mreq_repair", "agent_provider_request", 0)
	repairKey := internalToolRepairKey("mreq_repair", "call_repair", "unknown_tool")
	request := &bridgev1.CommitInternalToolRepairRequest{
		Scope:           scope,
		ModelRequestId:  "mreq_repair",
		ModelToolCallId: "call_repair",
		ToolName:        "unknown_tool",
		Drafts: []*bridgev1.RuntimeMessageDraft{
			bridgeInternalToolRepairDraftForTest("default", "sesn_bridge_repair", "thr_bridge_repair", repairKey, "call_repair", "unknown_tool", "invalid tool"),
		},
	}
	response, err := store.CommitInternalToolRepair(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitInternalToolRepair: %v", err)
	}
	if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("ack = %s; want committed", response.GetAck().GetStatus())
	}
	if len(response.GetDeclaration().GetReceipts()) != 1 {
		t.Fatalf("declaration receipts = %d; want 1", len(response.GetDeclaration().GetReceipts()))
	}
	receipt := response.GetDeclaration().GetReceipts()[0]
	if receipt.GetOperationKind() != bridgeOpCommitInternalToolRepair ||
		receipt.GetSourceKind() != "internal_tool_repair" ||
		receipt.GetSourceId() != repairKey ||
		len(receipt.GetEvents()) != 1 ||
		len(receipt.GetMessages()) != 1 ||
		len(receipt.GetMessages()[0].GetParts()) != 1 {
		t.Fatalf("repair receipt = %+v; want one complete repair declaration", receipt)
	}
	replay, err := store.CommitInternalToolRepair(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitInternalToolRepair replay: %v", err)
	}
	if replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE {
		t.Fatalf("replay ack = %s; want duplicate", replay.GetAck().GetStatus())
	}
	if !proto.Equal(replay.GetDeclaration(), response.GetDeclaration()) {
		t.Fatal("replayed repair declaration differs from the committed receipt")
	}

	var messageID string
	var kind string
	var sourceEventID sql.NullString
	var storedRepairKey string
	var dataJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT message_id, kind, source_event_id, repair_key, data_json
		   FROM session_messages
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_repair'
		    AND session_thread_id = 'thr_bridge_repair'
		    AND repair_key IS NOT NULL`).Scan(&messageID, &kind, &sourceEventID, &storedRepairKey, &dataJSON); err != nil {
		t.Fatalf("read repair row: %v", err)
	}
	if messageID != receipt.GetMessages()[0].GetMessageId() || kind != "assistant" || !sourceEventID.Valid || storedRepairKey != repairKey {
		t.Fatalf("repair row message=%q kind=%q source=%v key=%q; want event-owned assistant repair", messageID, kind, sourceEventID.Valid, storedRepairKey)
	}
	var durable map[string]any
	if err := json.Unmarshal([]byte(dataJSON), &durable); err != nil {
		t.Fatalf("unmarshal repair data: %v", err)
	}
	part := durable["parts"].([]any)[0].(map[string]any)
	if _, ok := part["toolUseEventId"]; ok {
		t.Fatalf("repair part persisted public toolUseEventId in %s", dataJSON)
	}

	store.RuntimeBindingTokenHMACKey = []byte("bridge-runtime-binding-token-test-key-32")
	contextResponse, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope:          bridgeAPIScope("sesn_bridge_repair", "thr_bridge_repair", "bind_bridge_repair", 1, "pod_uid_repair"),
		RuntimeInputId: "rin_bridge_repair_reload",
	})
	if err != nil {
		t.Fatalf("LoadContext after repair: %v", err)
	}
	var payload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(contextResponse.GetContextJson()), &payload); err != nil {
		t.Fatalf("parse LoadContext after repair: %v", err)
	}
	if len(payload.Messages) != 1 ||
		!strings.Contains(string(payload.Messages[0]), `"id":"`+messageID+`"`) ||
		!strings.Contains(string(payload.Messages[0]), `"toolCallId":"call_repair"`) {
		t.Fatalf("LoadContext repair messages = %s; want invalid call followed by durable repair row", contextResponse.GetContextJson())
	}

	conflict := proto.Clone(request).(*bridgev1.CommitInternalToolRepairRequest)
	conflict.Drafts[0].Parts[0].PartJson = strings.ReplaceAll(
		conflict.Drafts[0].Parts[0].GetPartJson(),
		"invalid tool",
		"different invalid tool",
	)
	if _, err := store.CommitInternalToolRepair(context.Background(), conflict); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("conflicting CommitInternalToolRepair err = %v; want AlreadyExists", err)
	}
}

func TestPostgreSQLBridgeAPIStoreRejectsPublicAndRepairToolCallIdentityCollision(t *testing.T) {
	for _, order := range []string{"public_then_repair", "repair_then_public"} {
		t.Run(order, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID := "sesn_bridge_tool_collision_" + order
			threadID := "thr_bridge_tool_collision_" + order
			bindingID := "bind_bridge_tool_collision_" + order
			podUID := "pod_bridge_tool_collision_" + order
			modelRequestID := "mreq_bridge_tool_collision_" + order
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
			seedBridgeAPIRequestStart(t, store, scope, "rwrite_tool_collision_start_"+order, modelRequestID, "agent_provider_request", 0)

			writePublic := func(writeID string) error {
				_, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
					Scope: scope, RuntimeWriteId: writeID, ModelRequestId: modelRequestID,
					EventType: "agent.tool_use", PayloadJson: `{"type":"agent.tool_use","name":"unknown_tool","input":{},"evaluated_permission":"allow"}`,
					Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
						t, scope, writeID, "agent.tool_use", "streaming",
						bridgeRuntimePartDraftForTest{kind: "tool", json: `{"type":"tool","toolCallId":"call_collision","toolName":"unknown_tool","state":{"status":"running","input":{"value":{},"preview":"{}","truncated":false}}}`},
					)},
				})
				return err
			}
			writeRepair := func() error {
				repairKey := internalToolRepairKey(modelRequestID, "call_collision", "unknown_tool")
				_, err := store.CommitInternalToolRepair(context.Background(), &bridgev1.CommitInternalToolRepairRequest{
					Scope: scope, ModelRequestId: modelRequestID, ModelToolCallId: "call_collision", ToolName: "unknown_tool",
					Drafts: []*bridgev1.RuntimeMessageDraft{bridgeInternalToolRepairDraftForTest(
						"default", sessionID, threadID, repairKey, "call_collision", "unknown_tool", "invalid tool",
					)},
				})
				return err
			}

			if order == "public_then_repair" {
				if err := writePublic("rwrite_tool_collision_public_first"); err != nil {
					t.Fatalf("first public Tool Use: %v", err)
				}
				if err := writeRepair(); status.Code(err) != codes.AlreadyExists {
					t.Fatalf("colliding internal repair err = %v; want AlreadyExists", err)
				}
				return
			}
			if err := writeRepair(); err != nil {
				t.Fatalf("first internal repair: %v", err)
			}
			if err := writePublic("rwrite_tool_collision_public_second"); status.Code(err) != codes.AlreadyExists {
				t.Fatalf("colliding public Tool Use err = %v; want AlreadyExists", err)
			}
		})
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
	boundary := int64(0)
	start, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_repair_after_end_start", ModelRequestId: modelRequestID,
		EventType: "span.model_request_start", PayloadJson: `{"type":"span.model_request_start","model_request_id":"` + modelRequestID + `"}`,
		ContextThroughMessageSequence: &boundary, RequestKind: "agent_provider_request",
	})
	if err != nil {
		t.Fatalf("write request start: %v", err)
	}
	if _, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_repair_after_end_close", ModelRequestId: modelRequestID,
		ModelRequestStartEventId: start.GetEventId(), FinishReason: "tool_calls", UsageJson: `{}`,
		RequestKind: "agent_provider_request",
	}); err != nil {
		t.Fatalf("write request end: %v", err)
	}
	repairKey := internalToolRepairKey(modelRequestID, "call_repair_after_end", "unknown_tool")
	request := &bridgev1.CommitInternalToolRepairRequest{
		Scope: scope, ModelRequestId: modelRequestID, ModelToolCallId: "call_repair_after_end", ToolName: "unknown_tool",
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeInternalToolRepairDraftForTest(
			"default", sessionID, threadID, repairKey, "call_repair_after_end", "unknown_tool", "invalid tool",
		)},
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
		Scope:           bridgeAPIScope("sesn_bridge_repair_stale", "thr_bridge_repair_stale", "bind_bridge_repair_stale", 2, "pod_uid_repair_stale"),
		ModelRequestId:  "mreq_repair_stale",
		ModelToolCallId: "call_repair_stale",
		ToolName:        "unknown_tool",
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeInternalToolRepairDraftForTest(
			"default",
			"sesn_bridge_repair_stale",
			"thr_bridge_repair_stale",
			internalToolRepairKey("mreq_repair_stale", "call_repair_stale", "unknown_tool"),
			"call_repair_stale",
			"unknown_tool",
			"invalid tool",
		)},
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
	create := &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory", "thr_bridge_memory", "bind_bridge_memory", 1, "pod_uid_memory"),
		ToolUseEventId:      "evt_tool_memory_create",
		NormalizedInputHash: "hash_create",
		Operation:           "create",
		InputJson:           `{"action":"create","path":"notes/todo.md","content":"one"}`,
	}
	response, err := store.RunMemory(context.Background(), create)
	if err != nil {
		t.Fatalf("RunMemory create: %v", err)
	}
	assertMemoryResultStatus(t, response.GetResultJson(), "completed")
	replay, err := store.RunMemory(context.Background(), create)
	if err != nil {
		t.Fatalf("RunMemory replay: %v", err)
	}
	if replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE || replay.GetResultJson() != response.GetResultJson() {
		t.Fatalf("RunMemory replay = %+v; want duplicate same result", replay)
	}
	reorderedReplay := proto.Clone(create).(*bridgev1.RunMemoryRequest)
	reorderedReplay.InputJson = `{"content":"one","path":"notes/todo.md","action":"create"}`
	reordered, err := store.RunMemory(context.Background(), reorderedReplay)
	if err != nil {
		t.Fatalf("RunMemory reordered JSON replay: %v", err)
	}
	if reordered.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE || reordered.GetResultJson() != response.GetResultJson() {
		t.Fatalf("RunMemory reordered replay = %+v; want duplicate same result", reordered)
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

	conflict := proto.Clone(create).(*bridgev1.RunMemoryRequest)
	conflict.NormalizedInputHash = "different_hash"
	if _, err := store.RunMemory(context.Background(), conflict); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("conflicting RunMemory err = %v; want AlreadyExists", err)
	}

	replace := &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory", "thr_bridge_memory", "bind_bridge_memory", 1, "pod_uid_memory"),
		ToolUseEventId:      "evt_tool_memory_replace",
		NormalizedInputHash: "hash_replace",
		Operation:           "replace",
		InputJson:           `{"action":"replace","path":"notes/todo.md","old_text":"one","new_text":"two"}`,
	}
	replaced, err := store.RunMemory(context.Background(), replace)
	if err != nil {
		t.Fatalf("RunMemory replace: %v", err)
	}
	assertMemoryResultStatus(t, replaced.GetResultJson(), "completed")
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
	request := &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_memory_projection_queue"),
		ToolUseEventId:      memoryWrite,
		NormalizedInputHash: "hash_memory_projection_queue",
		Operation:           "create",
		InputJson:           `{"action":"create","path":"notes/queue.md","content":"durable"}`,
	}
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
		Queue: tetralqueue.NewServer(queueStore), Store: projectionStore, Providers: providers,
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
	assertMemoryResultStatus(t, completed.response.GetResultJson(), "completed")
	replay, err := store.RunMemory(context.Background(), request)
	if err != nil {
		t.Fatalf("RunMemory replay: %v", err)
	}
	if replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE || replay.GetResultJson() != completed.response.GetResultJson() {
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
		_, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
			Scope:               bridgeAPIScope("sesn_bridge_memory_identity_quota", "thr_bridge_memory_identity_quota", "bind_bridge_memory_identity_quota", 1, "pod_uid_memory_identity_quota"),
			ToolUseEventId:      "evt_tool_memory_identity_quota",
			NormalizedInputHash: "hash_memory_identity_quota",
			Operation:           "create",
			InputJson:           `{"action":"create","path":"over-limit.md","content":"x"}`,
		})
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
			operation string
			inputJSON string
		}{
			{name: "replace", operation: "replace", inputJSON: `{"action":"replace","path":"quota.md","old_text":"x","new_text":"y"}`},
			{name: "delete", operation: "delete", inputJSON: `{"action":"delete","path":"quota.md","expected_text":"x"}`},
			{name: "rename", operation: "rename", inputJSON: `{"action":"rename","path":"quota.md","new_path":"renamed.md","expected_text":"x"}`},
		}
		for _, test := range requests {
			t.Run(test.name, func(t *testing.T) {
				_, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
					Scope:               bridgeAPIScope("sesn_bridge_memory_version_quota", "thr_bridge_memory_version_quota", "bind_bridge_memory_version_quota", 1, "pod_uid_memory_version_quota"),
					ToolUseEventId:      "evt_tool_memory_version_quota_" + test.name,
					NormalizedInputHash: "hash_memory_version_quota_" + test.name,
					Operation:           test.operation,
					InputJson:           test.inputJSON,
				})
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
			operation string
			inputJSON string
		}{
			{name: "create", operation: "create", inputJSON: `{"action":"create","path":"over-limit.md","content":"x"}`},
			{name: "replace", operation: "replace", inputJSON: `{"action":"replace","path":"quota.md","old_text":"x","new_text":"y"}`},
			{name: "rename", operation: "rename", inputJSON: `{"action":"rename","path":"quota.md","new_path":"renamed.md","expected_text":"x"}`},
		}
		for _, test := range requests {
			t.Run(test.name, func(t *testing.T) {
				_, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
					Scope:               bridgeAPIScope("sesn_bridge_memory_retained_quota", "thr_bridge_memory_retained_quota", "bind_bridge_memory_retained_quota", 1, "pod_uid_memory_retained_quota"),
					ToolUseEventId:      "evt_tool_memory_retained_quota_" + test.name,
					NormalizedInputHash: "hash_memory_retained_quota_" + test.name,
					Operation:           test.operation,
					InputJson:           test.inputJSON,
				})
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
	missing := &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_missing", "thr_bridge_memory_missing", "bind_bridge_memory_missing", 1, "pod_uid_memory_missing"),
		ToolUseEventId:      "evt_tool_memory_missing",
		NormalizedInputHash: "hash_memory_missing",
		Operation:           "create",
		InputJson:           `{"action":"create","path":"notes/missing.md","content":"one"}`,
	}
	missingResponse, err := store.RunMemory(context.Background(), missing)
	if err != nil {
		t.Fatalf("RunMemory missing binding: %v", err)
	}
	assertMemoryToolErrorCode(t, missingResponse.GetResultJson(), "memory_store_not_configured")
	assertMemoryProjectionStateNull(t, admin, "sesn_bridge_memory_missing", "evt_tool_memory_missing")

	seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_detached", "thr_bridge_memory_detached")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_detached", "bind_bridge_memory_detached", 1, "pod_uid_memory_detached")
	seedBridgeAPIDetachedMemoryStoreBinding(t, admin, "default", "sesn_bridge_memory_detached", "memstore_bridge_memory_detached", "read_write", "/mnt/memory/detached")
	detached := &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_detached", "thr_bridge_memory_detached", "bind_bridge_memory_detached", 1, "pod_uid_memory_detached"),
		ToolUseEventId:      "evt_tool_memory_detached",
		NormalizedInputHash: "hash_memory_detached",
		Operation:           "create",
		InputJson:           `{"action":"create","path":"notes/detached.md","content":"one"}`,
	}
	detachedResponse, err := store.RunMemory(context.Background(), detached)
	if err != nil {
		t.Fatalf("RunMemory detached binding: %v", err)
	}
	assertMemoryToolErrorCode(t, detachedResponse.GetResultJson(), "memory_store_not_configured")
	assertMemoryProjectionStateNull(t, admin, "sesn_bridge_memory_detached", "evt_tool_memory_detached")

	seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_ambiguous", "thr_bridge_memory_ambiguous")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_ambiguous", "bind_bridge_memory_ambiguous", 1, "pod_uid_memory_ambiguous")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_ambiguous", "memstore_bridge_memory_a")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_ambiguous", "memstore_bridge_memory_b")
	ambiguous := &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_ambiguous", "thr_bridge_memory_ambiguous", "bind_bridge_memory_ambiguous", 1, "pod_uid_memory_ambiguous"),
		ToolUseEventId:      "evt_tool_memory_ambiguous",
		NormalizedInputHash: "hash_memory_ambiguous",
		Operation:           "create",
		InputJson:           `{"action":"create","path":"notes/ambiguous.md","content":"one"}`,
	}
	ambiguousResponse, err := store.RunMemory(context.Background(), ambiguous)
	if err != nil {
		t.Fatalf("RunMemory ambiguous binding: %v", err)
	}
	assertMemoryToolErrorCode(t, ambiguousResponse.GetResultJson(), "memory_store_ambiguous")
	assertMemoryProjectionStateNull(t, admin, "sesn_bridge_memory_ambiguous", "evt_tool_memory_ambiguous")
}

func TestPostgreSQLBridgeAPIStoreRunMemoryRejectsOperationActionMismatchBeforeDurableWrite(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_action_mismatch", "thr_bridge_memory_action_mismatch")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_action_mismatch", "bind_bridge_memory_action_mismatch", 1, "pod_uid_memory_action_mismatch")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_action_mismatch", "memstore_bridge_memory_action_mismatch")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	_, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_action_mismatch", "thr_bridge_memory_action_mismatch", "bind_bridge_memory_action_mismatch", 1, "pod_uid_memory_action_mismatch"),
		ToolUseEventId:      "evt_tool_memory_action_mismatch",
		NormalizedInputHash: "hash_memory_action_mismatch",
		Operation:           "delete",
		InputJson:           `{"action":"create","path":"notes/mismatch.md","content":"one"}`,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("RunMemory mismatch error = %v; want InvalidArgument", err)
	}
	assertNoRuntimeToolResult(t, admin, "sesn_bridge_memory_action_mismatch", "evt_tool_memory_action_mismatch")
	if count := countMemoryVersions(t, admin, "memstore_bridge_memory_action_mismatch"); count != 0 {
		t.Fatalf("memory versions after action mismatch = %d; want 0", count)
	}
}

func TestPostgreSQLBridgeAPIStoreRunMemorySkipsRefreshForValidationErrors(t *testing.T) {
	tests := []struct {
		name      string
		operation string
		inputJSON string
		wantCode  string
		seed      func(t *testing.T, db *sql.DB, storeID string)
	}{
		{name: "invalid input non object", operation: "create", inputJSON: `[]`, wantCode: "invalid_input"},
		{name: "invalid input null", operation: "create", inputJSON: `null`, wantCode: "invalid_input"},
		{name: "invalid action", operation: "unknown", inputJSON: `{"action":"unknown","path":"notes/todo.md"}`, wantCode: "invalid_action"},
		{name: "invalid path absolute", operation: "create", inputJSON: `{"action":"create","path":"/absolute","content":"one"}`, wantCode: "invalid_path"},
		{name: "invalid path dotdot", operation: "create", inputJSON: `{"action":"create","path":"../bad","content":"one"}`, wantCode: "invalid_path"},
		{name: "invalid path runtime projection path", operation: "create", inputJSON: `{"action":"create","path":"mnt/memory/notes.md","content":"one"}`, wantCode: "invalid_path"},
		{name: "invalid path runtime projection prefix", operation: "create", inputJSON: `{"action":"create","path":"mnt/memory-old/notes.md","content":"one"}`, wantCode: "invalid_path"},
		{name: "invalid path too long", operation: "create", inputJSON: memoryCreateInputJSON(t, strings.Repeat("a", 1024), "one"), wantCode: "invalid_path"},
		{name: "missing content", operation: "create", inputJSON: `{"action":"create","path":"notes/todo.md"}`, wantCode: "missing_content"},
		{name: "content too large", operation: "create", inputJSON: memoryCreateInputJSON(t, "notes/large.md", strings.Repeat("x", 102401)), wantCode: "content_too_large"},
		{name: "missing replace text", operation: "replace", inputJSON: `{"action":"replace","path":"notes/todo.md","old_text":"one"}`, wantCode: "missing_replace_text"},
		{name: "missing expected text delete", operation: "delete", inputJSON: `{"action":"delete","path":"notes/todo.md"}`, wantCode: "missing_expected_text"},
		{name: "missing expected text rename", operation: "rename", inputJSON: `{"action":"rename","path":"notes/todo.md","new_path":"notes/new.md"}`, wantCode: "missing_expected_text"},
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
			response, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
				Scope:               bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_memory_validation"),
				ToolUseEventId:      toolUseID,
				NormalizedInputHash: "hash_memory_validation_" + strconv.Itoa(index),
				Operation:           tc.operation,
				InputJson:           tc.inputJSON,
			})
			if err != nil {
				t.Fatalf("RunMemory validation error: %v", err)
			}
			assertMemoryToolError(t, response.GetResultJson(), tc.wantCode, false)
			assertMemoryProjectionStateNull(t, admin, sessionID, toolUseID)
		})
	}
}

func TestPostgreSQLBridgeAPIStoreRunMemoryRejectsInvalidInputs(t *testing.T) {
	tests := []struct {
		name       string
		operation  string
		inputJSON  string
		wantCode   string
		wantReread bool
	}{
		{name: "invalid input non object", operation: "create", inputJSON: `[]`, wantCode: "invalid_input"},
		{name: "invalid action", operation: "unknown", inputJSON: `{"action":"unknown","path":"notes/todo.md"}`, wantCode: "invalid_action"},
		{name: "invalid path absolute", operation: "create", inputJSON: `{"action":"create","path":"/absolute","content":"one"}`, wantCode: "invalid_path"},
		{name: "invalid path dotdot", operation: "create", inputJSON: `{"action":"create","path":"notes/../bad","content":"one"}`, wantCode: "invalid_path"},
		{name: "invalid path runtime projection path", operation: "create", inputJSON: `{"action":"create","path":"mnt/memory/notes.md","content":"one"}`, wantCode: "invalid_path"},
		{name: "invalid path runtime projection prefix", operation: "create", inputJSON: `{"action":"create","path":"mnt/memory-old/notes.md","content":"one"}`, wantCode: "invalid_path"},
		{name: "invalid path too long", operation: "create", inputJSON: memoryCreateInputJSON(t, strings.Repeat("a", 1024), "one"), wantCode: "invalid_path"},
		{name: "missing content", operation: "create", inputJSON: `{"action":"create","path":"notes/todo.md"}`, wantCode: "missing_content"},
		{name: "content too large", operation: "create", inputJSON: memoryCreateInputJSON(t, "notes/large.md", strings.Repeat("x", 102401)), wantCode: "content_too_large"},
		{name: "missing replace text", operation: "replace", inputJSON: `{"action":"replace","path":"notes/todo.md","old_text":"one"}`, wantCode: "missing_replace_text"},
		{name: "missing expected text delete", operation: "delete", inputJSON: `{"action":"delete","path":"notes/todo.md"}`, wantCode: "missing_expected_text"},
		{name: "missing expected text rename", operation: "rename", inputJSON: `{"action":"rename","path":"notes/todo.md","new_path":"notes/new.md"}`, wantCode: "missing_expected_text"},
		{name: "not found", operation: "delete", inputJSON: `{"action":"delete","path":"notes/missing.md","expected_text":"gone"}`, wantCode: "not_found", wantReread: true},
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
			response, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
				Scope:               bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_memory_invalid"),
				ToolUseEventId:      "evt_tool_memory_invalid_" + strconv.Itoa(index),
				NormalizedInputHash: "hash_memory_invalid_" + strconv.Itoa(index),
				Operation:           tc.operation,
				InputJson:           tc.inputJSON,
			})
			if err != nil {
				t.Fatalf("RunMemory invalid input: %v", err)
			}
			assertMemoryToolError(t, response.GetResultJson(), tc.wantCode, tc.wantReread)
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
			response, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
				Scope:               bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_memory_replace_cap"),
				ToolUseEventId:      toolUseID,
				NormalizedInputHash: "hash_memory_replace_cap_" + strconv.Itoa(index),
				Operation:           "replace",
				InputJson:           tc.inputJSON,
			})
			if err != nil {
				t.Fatalf("RunMemory oversized replace: %v", err)
			}
			assertMemoryToolError(t, response.GetResultJson(), "content_too_large", false)
			assertMemoryProjectionStateNull(t, admin, sessionID, toolUseID)
			if count := countMemoryVersions(t, admin, storeID); count != 1 {
				t.Fatalf("memory versions after oversized replace = %d; want seed version only", count)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreRunMemoryReadsLegacyOversizedRows(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_legacy_large", "thr_bridge_memory_legacy_large")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_legacy_large", "bind_bridge_memory_legacy_large", 1, "pod_uid_memory_legacy_large")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_legacy_large", "memstore_bridge_memory_legacy_large")
	legacyContent := strings.Repeat("x", memoryToolContentMaxBytes+1)
	seedBridgeAPIMemory(t, admin, "default", "memstore_bridge_memory_legacy_large", "mem_bridge_memory_legacy_large", "/notes/legacy-large.md", legacyContent)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	response, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_legacy_large", "thr_bridge_memory_legacy_large", "bind_bridge_memory_legacy_large", 1, "pod_uid_memory_legacy_large"),
		ToolUseEventId:      "evt_tool_memory_legacy_large",
		NormalizedInputHash: "hash_memory_legacy_large",
		Operation:           "delete",
		InputJson:           memoryDeleteInputJSON(t, "notes/legacy-large.md", legacyContent),
	})
	if err != nil {
		t.Fatalf("RunMemory legacy oversized delete: %v", err)
	}
	assertMemoryResultStatus(t, response.GetResultJson(), "completed")
	if count := countMemoryVersions(t, admin, "memstore_bridge_memory_legacy_large"); count != 2 {
		t.Fatalf("memory versions after legacy oversized delete = %d; want seed plus delete", count)
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
	deleted, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_delete", "thr_bridge_memory_delete", "bind_bridge_memory_delete", 1, "pod_uid_memory_delete"),
		ToolUseEventId:      "evt_tool_memory_delete",
		NormalizedInputHash: "hash_memory_delete",
		Operation:           "delete",
		InputJson:           `{"action":"delete","path":"notes/delete.md","expected_text":"delete me"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory delete: %v", err)
	}
	assertMemoryResultStatus(t, deleted.GetResultJson(), "completed")
	assertMemoryDeleted(t, admin, "memstore_bridge_memory_delete", "/notes/delete.md")

	wrongDelete, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_delete", "thr_bridge_memory_delete", "bind_bridge_memory_delete", 1, "pod_uid_memory_delete"),
		ToolUseEventId:      "evt_tool_memory_delete_wrong",
		NormalizedInputHash: "hash_memory_delete_wrong",
		Operation:           "delete",
		InputJson:           `{"action":"delete","path":"notes/other.md","expected_text":"wrong"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory wrong delete: %v", err)
	}
	assertMemoryToolError(t, wrongDelete.GetResultJson(), "expected_text_mismatch", true)
}

func TestPostgreSQLBridgeAPIStoreRunMemoryRenamesWithExpectedText(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_rename", "thr_bridge_memory_rename")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_rename", "bind_bridge_memory_rename", 1, "pod_uid_memory_rename")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_rename", "memstore_bridge_memory_rename")
	seedBridgeAPIMemory(t, admin, "default", "memstore_bridge_memory_rename", "mem_bridge_memory_rename", "/notes/old.md", "rename me")
	seedBridgeAPIMemory(t, admin, "default", "memstore_bridge_memory_rename", "mem_bridge_memory_rename_collision", "/notes/existing.md", "existing")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))

	wrongRename, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_rename", "thr_bridge_memory_rename", "bind_bridge_memory_rename", 1, "pod_uid_memory_rename"),
		ToolUseEventId:      "evt_tool_memory_rename_wrong",
		NormalizedInputHash: "hash_memory_rename_wrong",
		Operation:           "rename",
		InputJson:           `{"action":"rename","path":"notes/old.md","new_path":"notes/wrong.md","expected_text":"wrong"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory wrong rename: %v", err)
	}
	assertMemoryToolError(t, wrongRename.GetResultJson(), "expected_text_mismatch", true)

	collision, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_rename", "thr_bridge_memory_rename", "bind_bridge_memory_rename", 1, "pod_uid_memory_rename"),
		ToolUseEventId:      "evt_tool_memory_rename_collision",
		NormalizedInputHash: "hash_memory_rename_collision",
		Operation:           "rename",
		InputJson:           `{"action":"rename","path":"notes/old.md","new_path":"notes/existing.md","expected_text":"rename me"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory rename collision: %v", err)
	}
	assertMemoryToolError(t, collision.GetResultJson(), "path_exists", true)

	renamed, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_rename", "thr_bridge_memory_rename", "bind_bridge_memory_rename", 1, "pod_uid_memory_rename"),
		ToolUseEventId:      "evt_tool_memory_rename",
		NormalizedInputHash: "hash_memory_rename",
		Operation:           "rename",
		InputJson:           `{"action":"rename","path":"notes/old.md","new_path":"notes/new.md","expected_text":"rename me"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory rename: %v", err)
	}
	assertMemoryResultStatus(t, renamed.GetResultJson(), "completed")
	if testJSONPathString(t, renamed.GetResultJson(), "path") != "notes/old.md" {
		t.Fatalf("rename result = %s; want path notes/old.md", renamed.GetResultJson())
	}
	if testJSONPathString(t, renamed.GetResultJson(), "new_path") != "notes/new.md" {
		t.Fatalf("rename result = %s; want new_path notes/new.md", renamed.GetResultJson())
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
	missing, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_replace_stale", "thr_bridge_memory_replace_stale", "bind_bridge_memory_replace_stale", 1, "pod_uid_memory_replace_stale"),
		ToolUseEventId:      "evt_tool_memory_replace_missing",
		NormalizedInputHash: "hash_memory_replace_missing",
		Operation:           "replace",
		InputJson:           `{"action":"replace","path":"notes/repeat.md","old_text":"absent","new_text":"two"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory replace missing: %v", err)
	}
	assertMemoryToolError(t, missing.GetResultJson(), "old_text_not_found", true)

	nonUnique, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_replace_stale", "thr_bridge_memory_replace_stale", "bind_bridge_memory_replace_stale", 1, "pod_uid_memory_replace_stale"),
		ToolUseEventId:      "evt_tool_memory_replace_nonunique",
		NormalizedInputHash: "hash_memory_replace_nonunique",
		Operation:           "replace",
		InputJson:           `{"action":"replace","path":"notes/repeat.md","old_text":"one","new_text":"two"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory replace nonunique: %v", err)
	}
	assertMemoryToolError(t, nonUnique.GetResultJson(), "old_text_not_unique", true)

	replaced, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_replace_stale", "thr_bridge_memory_replace_stale", "bind_bridge_memory_replace_stale", 1, "pod_uid_memory_replace_stale"),
		ToolUseEventId:      "evt_tool_memory_replace_all",
		NormalizedInputHash: "hash_memory_replace_all",
		Operation:           "replace",
		InputJson:           `{"action":"replace","path":"notes/repeat.md","old_text":"one","new_text":"two","replace_all":true}`,
	})
	if err != nil {
		t.Fatalf("RunMemory replace all: %v", err)
	}
	assertMemoryResultStatus(t, replaced.GetResultJson(), "completed")
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
	descendant, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_prefix", "thr_bridge_memory_prefix", "bind_bridge_memory_prefix", 1, "pod_uid_memory_prefix"),
		ToolUseEventId:      "evt_tool_memory_prefix_descendant",
		NormalizedInputHash: "hash_memory_prefix_descendant",
		Operation:           "create",
		InputJson:           `{"action":"create","path":"a/b","content":"child"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory descendant conflict: %v", err)
	}
	assertMemoryToolError(t, descendant.GetResultJson(), "path_exists", true)
	if got := testJSONPathString(t, descendant.GetResultJson(), "message"); got != "memory path is inside an existing memory" {
		t.Fatalf("descendant conflict message = %q; want distinguishing descendant message", got)
	}
	assertMemoryPathConflictResult(t, descendant.GetResultJson(), []memoryPathConflictWireHead{{MemoryID: "mem_bridge_memory_prefix_a", Path: "/a"}}, 1, false)

	ancestorCreate, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_prefix", "thr_bridge_memory_prefix", "bind_bridge_memory_prefix", 1, "pod_uid_memory_prefix"),
		ToolUseEventId:      "evt_tool_memory_prefix_create_ancestor",
		NormalizedInputHash: "hash_memory_prefix_create_ancestor",
		Operation:           "create",
		InputJson:           `{"action":"create","path":"parent","content":"root"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory create ancestor conflict: %v", err)
	}
	assertMemoryToolError(t, ancestorCreate.GetResultJson(), "path_exists", true)
	if got := testJSONPathString(t, ancestorCreate.GetResultJson(), "message"); got != "memory path would contain an existing memory" {
		t.Fatalf("ancestor create conflict message = %q; want distinguishing ancestor message", got)
	}
	assertMemoryPathConflictResult(t, ancestorCreate.GetResultJson(), []memoryPathConflictWireHead{
		{MemoryID: "mem_bridge_memory_prefix_parent_child", Path: "/parent/child"},
		{MemoryID: "mem_bridge_memory_prefix_parent_other", Path: "/parent/other"},
	}, 2, false)

	exactRename, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_prefix", "thr_bridge_memory_prefix", "bind_bridge_memory_prefix", 1, "pod_uid_memory_prefix"),
		ToolUseEventId:      "evt_tool_memory_prefix_rename_exact",
		NormalizedInputHash: "hash_memory_prefix_rename_exact",
		Operation:           "rename",
		InputJson:           `{"action":"rename","path":"literal_%","new_path":"a","expected_text":"percent"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory exact rename conflict: %v", err)
	}
	assertMemoryToolError(t, exactRename.GetResultJson(), "path_exists", true)
	if got := testJSONPathString(t, exactRename.GetResultJson(), "message"); got != "memory target path already exists" {
		t.Fatalf("exact rename conflict message = %q; want exact collision message", got)
	}
	assertMemoryPathConflictResult(t, exactRename.GetResultJson(), []memoryPathConflictWireHead{{MemoryID: "mem_bridge_memory_prefix_a", Path: "/a"}}, 1, false)

	descendantRename, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_prefix", "thr_bridge_memory_prefix", "bind_bridge_memory_prefix", 1, "pod_uid_memory_prefix"),
		ToolUseEventId:      "evt_tool_memory_prefix_rename_descendant",
		NormalizedInputHash: "hash_memory_prefix_rename_descendant",
		Operation:           "rename",
		InputJson:           `{"action":"rename","path":"literal_%","new_path":"a/b","expected_text":"percent"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory descendant rename conflict: %v", err)
	}
	assertMemoryToolError(t, descendantRename.GetResultJson(), "path_exists", true)
	if got := testJSONPathString(t, descendantRename.GetResultJson(), "message"); got != "memory target path is inside an existing memory" {
		t.Fatalf("descendant rename conflict message = %q; want distinguishing descendant message", got)
	}
	assertMemoryPathConflictResult(t, descendantRename.GetResultJson(), []memoryPathConflictWireHead{{MemoryID: "mem_bridge_memory_prefix_a", Path: "/a"}}, 1, false)

	ancestorRename, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_prefix", "thr_bridge_memory_prefix", "bind_bridge_memory_prefix", 1, "pod_uid_memory_prefix"),
		ToolUseEventId:      "evt_tool_memory_prefix_rename_ancestor",
		NormalizedInputHash: "hash_memory_prefix_rename_ancestor",
		Operation:           "rename",
		InputJson:           `{"action":"rename","path":"literal_%","new_path":"parent","expected_text":"percent"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory ancestor rename conflict: %v", err)
	}
	assertMemoryToolError(t, ancestorRename.GetResultJson(), "path_exists", true)
	if got := testJSONPathString(t, ancestorRename.GetResultJson(), "message"); got != "memory target path would contain an existing memory" {
		t.Fatalf("ancestor rename conflict message = %q; want distinguishing ancestor message", got)
	}
	assertMemoryPathConflictResult(t, ancestorRename.GetResultJson(), []memoryPathConflictWireHead{
		{MemoryID: "mem_bridge_memory_prefix_parent_child", Path: "/parent/child"},
		{MemoryID: "mem_bridge_memory_prefix_parent_other", Path: "/parent/other"},
	}, 2, false)

	underscore, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_prefix", "thr_bridge_memory_prefix", "bind_bridge_memory_prefix", 1, "pod_uid_memory_prefix"),
		ToolUseEventId:      "evt_tool_memory_prefix_underscore",
		NormalizedInputHash: "hash_memory_prefix_underscore",
		Operation:           "create",
		InputJson:           `{"action":"create","path":"literal_X","content":"underscore is literal"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory literal underscore: %v", err)
	}
	assertMemoryResultStatus(t, underscore.GetResultJson(), "completed")
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
			response, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
				Scope:               bridgeAPIScope(sessionID, threadID, "bind_bridge_memory_conflict_bound", 1, "pod_uid_memory_conflict_bound"),
				ToolUseEventId:      "evt_tool_memory_conflict_bound_" + suffix,
				NormalizedInputHash: "hash_memory_conflict_bound_" + suffix,
				Operation:           "create",
				InputJson:           `{"action":"create","path":"root/target","content":"rejected"}`,
			})
			if err != nil {
				t.Fatalf("RunMemory: %v", err)
			}
			assertMemoryToolError(t, response.GetResultJson(), "path_exists", true)

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
			assertMemoryPathConflictResult(t, response.GetResultJson(), wantHeads, conflictTotal, conflictTotal > MaxMemoryPathConflicts)

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
