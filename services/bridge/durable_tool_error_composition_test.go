package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"reflect"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"
)

func TestPostgreSQLInvalidToolRepairRunsFromRuntimeClassificationThroughProviderWire(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_invalid_tool_production"
		threadID  = "sthr_invalid_tool_production"
		bindingID = "bind_invalid_tool_production"
		podUID    = "pod_invalid_tool_production"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIProjectedUserMessage(t, admin, sessionID, threadID, "msg_invalid_tool_production", "sevt_invalid_tool_production", 1)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_messages
		SET data_json='{"parts":[{"type":"text","text":"continue"}]}'
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND sequence=1`, sessionID, threadID); err != nil {
		t.Fatalf("seed invalid-tool Runtime context: %v", err)
	}
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("invalid-tool-production-signing-key")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for Runtime repair composition: %v", err)
	}
	server := grpc.NewServer()
	RegisterBridgeAPI(server, store)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	runtimeRepair := runRuntimeInvalidToolRepairComposition(t, listener.Addr().String(), map[string]any{
		"workspaceId": "default", "sessionId": sessionID, "sessionThreadId": threadID,
		"bindingId": bindingID, "bindingGeneration": 1, "targetPodUid": podUID,
		"runtimeBindingToken": "fixture-binding-token",
	})
	if runtimeRepair.ResultType != "completed" || len(runtimeRepair.StoreOrder) != 1 ||
		runtimeRepair.StoreOrder[0] != "store:internal-tool-repair" ||
		len(runtimeRepair.PublicToolEvents) != 0 || runtimeRepair.RunToolCalls != 0 ||
		runtimeRepair.AcceptSandboxExecutionCalls != 0 || runtimeRepair.AwaitSandboxExecutionCalls != 0 {
		t.Fatalf("Runtime invalid-tool ownership = %+v", runtimeRepair)
	}
	const expectedMessage = "disabled or unknown tool call: exec_command"
	if runtimeRepair.Repair.Error.Type != "runtime_invalid_sequence" ||
		runtimeRepair.Repair.Error.Message != expectedMessage || runtimeRepair.Repair.Error.Retryable {
		t.Fatalf("Runtime unavailable-Tool error = %+v; want baseline provider-visible text", runtimeRepair.Repair.Error)
	}

	var toolUses, repairResults, executionRows, pendingRows, queueRows int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type IN ('agent.tool_use','agent.mcp_tool_use')),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.tool_result' AND payload_json::jsonb ->> 'repair_kind'='invalid_tool'),
		(SELECT count(*) FROM session_runtime_tool_results WHERE workspace_id='default' AND session_id=$1),
		(SELECT count(*) FROM session_pending_tool_uses WHERE workspace_id='default' AND session_id=$1),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default')`, sessionID).
		Scan(&toolUses, &repairResults, &executionRows, &pendingRows, &queueRows); err != nil {
		t.Fatalf("read invalid-tool durable census: %v", err)
	}
	if toolUses != 0 || repairResults != 1 || executionRows != 0 || pendingRows != 0 || queueRows != 0 {
		t.Fatalf("invalid-tool durable census uses/results/executions/pending/queue = %d/%d/%d/%d/%d", toolUses, repairResults, executionRows, pendingRows, queueRows)
	}

	loaded, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope: bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID),
	})
	if err != nil {
		t.Fatalf("cold-load invalid-tool repair: %v", err)
	}
	requests := runRuntimeProviderComposition(t, loaded.GetContextJson())
	wires := runInvalidToolProviderWireComposition(t, requests)
	if len(wires) != 3 {
		t.Fatalf("invalid-tool provider wire families = %d; want 3", len(wires))
	}
	families := map[string]bool{}
	for _, wire := range wires {
		families[wire.Family] = true
		if wire.CallID != runtimeRepair.Repair.ModelToolCallID ||
			wire.ToolName != runtimeRepair.Repair.ToolName || wire.ErrorMessage != expectedMessage ||
			wire.CallIndex >= wire.ResultIndex || wire.Pathname == "" {
			t.Fatalf("invalid-tool provider wire = %+v", wire)
		}
	}
	for _, family := range []string{"anthropic", "openai", "openai-compatible"} {
		if !families[family] {
			t.Fatalf("invalid-tool provider wire omitted %s", family)
		}
	}
}

type runtimeInvalidToolRepairComposition struct {
	ResultType string `json:"resultType"`
	Repair     struct {
		ModelRequestID  string `json:"modelRequestId"`
		ModelToolCallID string `json:"modelToolCallId"`
		ToolName        string `json:"toolName"`
		Error           struct {
			Type      string `json:"type"`
			Message   string `json:"message"`
			Retryable bool   `json:"retryable"`
		} `json:"error"`
	} `json:"repair"`
	StoreOrder                  []string `json:"storeOrder"`
	PublicToolEvents            []string `json:"publicToolEvents"`
	RunToolCalls                int      `json:"runToolCalls"`
	AcceptSandboxExecutionCalls int      `json:"acceptSandboxExecutionCalls"`
	AwaitSandboxExecutionCalls  int      `json:"awaitSandboxExecutionCalls"`
}

func runRuntimeInvalidToolRepairComposition(t *testing.T, address string, identity map[string]any) runtimeInvalidToolRepairComposition {
	t.Helper()
	identityJSON, err := json.Marshal(identity)
	if err != nil {
		t.Fatalf("encode Runtime repair identity: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "bun", "packages/runtime-pod/test/fixtures/invalid-tool-repair-declaration.ts", address, string(identityJSON)) //nolint:gosec // Fixed production composition fixture and test-owned address.
	command.Dir = "../agent-runtime"
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run Runtime invalid-tool composition: %v: %s", err, output)
	}
	var result runtimeInvalidToolRepairComposition
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode Runtime invalid-tool composition: %v: %s", err, output)
	}
	return result
}

func runRuntimeProviderComposition(t *testing.T, contextJSON string) []json.RawMessage {
	t.Helper()
	input, err := json.Marshal(map[string]any{"contextJson": contextJSON, "providerComposition": true})
	if err != nil {
		t.Fatalf("encode Runtime provider composition: %v", err)
	}
	inputPath := t.TempDir() + "/runtime-provider-composition.json"
	if err := os.WriteFile(inputPath, input, 0o600); err != nil {
		t.Fatalf("write Runtime provider composition: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "bun", "packages/runtime-pod/test/fixtures/cold-checkpoint-composition.ts", inputPath) //nolint:gosec // Fixed production composition fixture and test-owned input.
	command.Dir = "../agent-runtime"
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run Runtime provider composition: %v: %s", err, output)
	}
	var result struct {
		ProviderComposition struct {
			Strategies []struct {
				ProviderFamily string `json:"providerFamily"`
				Validation     struct {
					Ok bool `json:"ok"`
				} `json:"validation"`
				ProviderRequest json.RawMessage `json:"providerRequest"`
			} `json:"strategies"`
		} `json:"providerComposition"`
	}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode Runtime provider composition: %v: %s", err, output)
	}
	requests := make([]json.RawMessage, 0, len(result.ProviderComposition.Strategies))
	for _, strategy := range result.ProviderComposition.Strategies {
		if !strategy.Validation.Ok || len(strategy.ProviderRequest) == 0 {
			t.Fatalf("Runtime Provider composition for %s was invalid or absent: %s", strategy.ProviderFamily, output)
		}
		requests = append(requests, strategy.ProviderRequest)
	}
	if len(requests) == 0 {
		t.Fatal("Runtime Provider composition returned no strategies")
	}
	return requests
}

type invalidToolProviderWireComposition struct {
	Family       string `json:"family"`
	Pathname     string `json:"pathname"`
	CallID       string `json:"callId"`
	ToolName     string `json:"toolName"`
	ErrorMessage string `json:"errorMessage"`
	CallIndex    int    `json:"callIndex"`
	ResultIndex  int    `json:"resultIndex"`
}

func runInvalidToolProviderWireComposition(t *testing.T, requests []json.RawMessage) []invalidToolProviderWireComposition {
	t.Helper()
	input, err := json.Marshal(map[string]any{"requests": requests})
	if err != nil {
		t.Fatalf("encode provider wire composition: %v", err)
	}
	inputPath := t.TempDir() + "/invalid-tool-provider-requests.json"
	if err := os.WriteFile(inputPath, input, 0o600); err != nil {
		t.Fatalf("write provider wire composition: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "bun", "packages/provider-gateway/test/fixtures/invalid-tool-repair-wire.ts", inputPath) //nolint:gosec // Fixed provider composition fixture and test-owned input.
	command.Dir = "../gateway"
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run invalid-tool provider wire composition: %v: %s", err, output)
	}
	var result []invalidToolProviderWireComposition
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode invalid-tool provider wire composition: %v: %s", err, output)
	}
	return result
}

func TestPostgreSQLDurableToolErrorSettlesIntoNarrowColdContext(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_durable_error", "sthr_durable_error")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_durable_error", "bind_durable_error", 1, "pod_durable_error")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("durable-error-test-signing-key")
	client := startActorProductionBridge(t, runtimeDB)
	scope := bridgeAPIScope("sesn_durable_error", "sthr_durable_error", "bind_durable_error", 1, "pod_durable_error")
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_durable_error_start", "mreq_durable_error", requestKindAgentProviderRequest, 0)

	toolUse, err := client.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_durable_error_use", ModelRequestId: "mreq_durable_error",
		ToolDeclaration: bridgeSignedReasoningToolDeclarationForTest(
			"call_durable_error", "Read", `{"file_path":"/missing.txt"}`, "allow",
		),
	})
	if err != nil || toolUse.GetCommitted() == nil {
		t.Fatalf("write Tool Use: response=%#v err=%v", toolUse, err)
	}
	if _, err := client.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_durable_error_end", ModelRequestId: "mreq_durable_error",
		FinishReason: "tool-calls", UsageJson: `{}`,
		ProviderContextRetention: &bridgev1.ProviderContextRetention{
			Disposition: "completed", AssistantMessageSequence: toolUse.GetCommitted().AssignedMessageSequence,
			ToolUseEventIds: []string{toolUse.GetCommitted().GetEventId()},
		},
	}); err != nil {
		t.Fatalf("write Request End: %v", err)
	}
	accepted, err := client.AcceptSandboxExecution(context.Background(), &bridgev1.AcceptSandboxExecutionRequest{
		Scope: scope, ToolUseEventId: toolUse.GetCommitted().GetEventId(),
	})
	if err != nil || accepted.GetCommitted() == nil {
		t.Fatalf("accept missing-file Read for hot receipt proof: response=%#v err=%v", accepted, err)
	}
	settleSandboxExecutionForHotReceiptProof(t, runtimeDB, admin, scope, toolUse.GetCommitted().GetEventId(),
		`{"status":"tool_error","error":{"kind":"not_found","message":"path does not exist"},"result":{}}`)
	baseLoaded, err := client.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope})
	if err != nil {
		t.Fatalf("LoadContext pending Tool state: %v", err)
	}

	adapter := runRuntimeDurableToolErrorDeclaration(t, map[string]any{
		"workspaceId": "default", "sessionId": "sesn_durable_error", "sessionThreadId": "sthr_durable_error",
		"bindingId": "bind_durable_error", "bindingGeneration": 1, "targetPodUid": "pod_durable_error",
		"modelRequestId": "mreq_durable_error", "modelToolCallId": "call_durable_error",
		"toolUseEventId": toolUse.GetCommitted().GetEventId(),
	})
	if adapter.AdapterResult.Type != "error" || adapter.AdapterResult.Error.Message == "" ||
		adapter.RuntimeSettlement.Type != "error" || adapter.ErrorJSON == "" {
		t.Fatalf("ordinary missing-file Read adapter = %+v", adapter)
	}
	settlementRequest := bridgeToolSettlementRequestForTest(
		scope,
		&bridgev1.RuntimeToolSettlement{
			ToolUseEventId: toolUse.GetCommitted().GetEventId(),
			Outcome:        &bridgev1.RuntimeToolSettlement_Error{Error: &bridgev1.RuntimeToolError{ErrorJson: adapter.ErrorJSON}},
		},
	)
	settled, err := client.SettleToolResult(context.Background(), settlementRequest)
	if err != nil {
		t.Fatalf("settle durable Tool error: %v", err)
	}
	bridgeRequireToolSettlementOutcomeForTest(t, settled, "committed")
	replayed, err := client.SettleToolResult(context.Background(), settlementRequest)
	if err != nil {
		t.Fatalf("replay durable Tool error after lost acknowledgement: %v", err)
	}
	bridgeRequireToolSettlementOutcomeForTest(t, replayed, "duplicate")

	var dataJSON string
	if err := admin.QueryRowContext(context.Background(), `SELECT data_json FROM session_messages
		WHERE workspace_id='default' AND session_id='sesn_durable_error'
		  AND session_thread_id='sthr_durable_error' AND model_request_id='mreq_durable_error'`).Scan(&dataJSON); err != nil {
		t.Fatalf("read durable Tool error context: %v", err)
	}
	parts, err := decodeStoredRuntimeContextParts(dataJSON)
	if err != nil || len(parts) != 3 {
		t.Fatalf("durable Tool error context = %s err=%v", dataJSON, err)
	}
	var result struct {
		Type            string `json:"type"`
		ModelToolCallID string `json:"modelToolCallId"`
		Result          struct {
			Type  string          `json:"type"`
			Error json.RawMessage `json:"error"`
		} `json:"result"`
	}
	if err := json.Unmarshal(parts[2], &result); err != nil {
		t.Fatalf("decode durable Tool result: %v", err)
	}
	var gotError, wantError map[string]any
	if err := json.Unmarshal(result.Result.Error, &gotError); err != nil {
		t.Fatalf("decode stored durable Tool error: %v", err)
	}
	if err := json.Unmarshal([]byte(adapter.ErrorJSON), &wantError); err != nil {
		t.Fatalf("decode expected durable Tool error: %v", err)
	}
	if result.Type != "tool_result" || result.ModelToolCallID != "call_durable_error" ||
		result.Result.Type != "error" || !reflect.DeepEqual(gotError, wantError) {
		t.Fatalf("durable Tool result = %#v raw_error=%s", result, result.Result.Error)
	}

	loaded, err := client.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope})
	if err != nil {
		t.Fatalf("LoadContext durable Tool error: %v", err)
	}
	var payload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(loaded.GetContextJson()), &payload); err != nil {
		t.Fatalf("decode cold context: %v", err)
	}
	if payload.OpenRequestDraft != nil || len(payload.ContextEntries) != 1 || len(payload.ContextEntries[0].Parts) != 3 {
		t.Fatalf("cold durable Tool context = entries=%#v draft=%#v", payload.ContextEntries, payload.OpenRequestDraft)
	}
	if string(payload.ContextEntries[0].Parts[2]) != string(parts[2]) {
		t.Fatalf("cold durable Tool result = %s; stored=%s", payload.ContextEntries[0].Parts[2], parts[2])
	}
	assertRuntimeHotColdToolComposition(
		t,
		baseLoaded.GetContextJson(),
		loaded.GetContextJson(),
		toolUse.GetCommitted().GetAssignedMessageSequence(),
		payload,
		toolUse.GetCommitted().GetEventId(),
		"call_durable_error",
		adapter.RuntimeSettlement,
	)
}

type runtimeDurableToolErrorDeclaration struct {
	ErrorJSON     string `json:"errorJson"`
	AdapterResult struct {
		Type  string `json:"type"`
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	} `json:"adapterResult"`
	RuntimeSettlement struct {
		Type  string         `json:"type"`
		Error map[string]any `json:"error"`
	} `json:"runtimeSettlement"`
}

func runRuntimeDurableToolErrorDeclaration(t *testing.T, input map[string]any) runtimeDurableToolErrorDeclaration {
	t.Helper()
	encoded, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("encode Runtime durable Tool error declaration: %v", err)
	}
	inputPath := t.TempDir() + "/runtime-durable-tool-error.json"
	if err := os.WriteFile(inputPath, encoded, 0o600); err != nil {
		t.Fatalf("write Runtime durable Tool error declaration: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "bun", "packages/runtime-pod/test/fixtures/durable-tool-error-declaration.ts", inputPath) //nolint:gosec // Fixed production composition fixture and test-owned input.
	command.Dir = "../agent-runtime"
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run Runtime durable Tool error declaration: %v: %s", err, output)
	}
	var result runtimeDurableToolErrorDeclaration
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode Runtime durable Tool error declaration: %v: %s", err, output)
	}
	return result
}

func TestPostgreSQLDurableToolCompletionStoresOnlyFinalProviderVisibleText(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID        = "sesn_durable_truncation"
		threadID         = "sthr_durable_truncation"
		bindingID        = "bind_durable_truncation"
		podUID           = "pod_durable_truncation"
		modelRequestID   = "mreq_durable_truncation"
		modelToolCallID  = "call_durable_truncation"
		originalText     = "partial output"
		truncationNotice = "\n\n[Tool output was truncated to fit the provider context limit.]"
		finalText        = originalText + truncationNotice
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("durable-truncation-test-signing-key")
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_durable_truncation_start", modelRequestID, requestKindAgentProviderRequest, 0)

	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_durable_truncation_use", ModelRequestId: modelRequestID,
		ToolDeclaration: bridgeToolDeclarationForTest(modelToolCallID, "Read", `{}`, "allow", "sandbox_execute"),
	})
	if err != nil || toolUse.GetCommitted() == nil {
		t.Fatalf("write truncated Tool Use: response=%#v err=%v", toolUse, err)
	}
	if _, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_durable_truncation_end", ModelRequestId: modelRequestID,
		FinishReason: "tool-calls", UsageJson: `{}`,
		ProviderContextRetention: &bridgev1.ProviderContextRetention{
			Disposition: "completed", AssistantMessageSequence: toolUse.GetCommitted().AssignedMessageSequence,
			ToolUseEventIds: []string{toolUse.GetCommitted().GetEventId()},
		},
	}); err != nil {
		t.Fatalf("write truncated Tool Request End: %v", err)
	}
	accepted, err := store.AcceptSandboxExecution(context.Background(), &bridgev1.AcceptSandboxExecutionRequest{
		Scope: scope, ToolUseEventId: toolUse.GetCommitted().GetEventId(),
	})
	if err != nil || accepted.GetCommitted() == nil {
		t.Fatalf("accept truncated Read for hot receipt proof: response=%#v err=%v", accepted, err)
	}
	settleSandboxExecutionForHotReceiptProof(t, runtimeDB, admin, scope, toolUse.GetCommitted().GetEventId(),
		`{"status":"success","result":{"content":"partial output","start_line":1,"returned_lines":1,"truncated":true,"line_truncations":0}}`)
	baseLoaded, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope})
	if err != nil {
		t.Fatalf("LoadContext pending truncated Tool: %v", err)
	}

	settled, err := store.SettleToolResult(context.Background(), bridgeToolSettlementRequestForTest(scope, &bridgev1.RuntimeToolSettlement{
		ToolUseEventId: toolUse.GetCommitted().GetEventId(),
		Outcome: &bridgev1.RuntimeToolSettlement_Completed{Completed: &bridgev1.RuntimeToolCompleted{
			OutputJson: `{"text":"partial output\n\n[Tool output was truncated to fit the provider context limit.]","truncated":true}`,
		}},
	}))
	if err != nil || settled.GetCommitted() == nil {
		t.Fatalf("settle truncated Tool completion: response=%#v err=%v", settled, err)
	}

	var dataJSON, projectionJSON string
	if err := admin.QueryRowContext(context.Background(), `SELECT data_json FROM session_messages
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND model_request_id=$3`,
		sessionID, threadID, modelRequestID).Scan(&dataJSON); err != nil {
		t.Fatalf("read durable truncated Tool context: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT projection_json FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='agent.tool_result'`,
		sessionID, threadID).Scan(&projectionJSON); err != nil {
		t.Fatalf("read truncated Tool Event projection: %v", err)
	}
	parts, err := decodeStoredRuntimeContextParts(dataJSON)
	if err != nil || len(parts) != 2 {
		t.Fatalf("durable truncated Tool context = %s err=%v", dataJSON, err)
	}
	var result struct {
		Result struct {
			Output map[string]any `json:"output"`
		} `json:"result"`
	}
	if err := json.Unmarshal(parts[1], &result); err != nil {
		t.Fatalf("decode durable truncated Tool result: %v", err)
	}
	if !reflect.DeepEqual(result.Result.Output, map[string]any{"text": finalText}) {
		t.Fatalf("durable completed output = %#v; want final provider-visible text only", result.Result.Output)
	}
	var projection struct {
		Output struct {
			Text      string `json:"text"`
			Truncated bool   `json:"truncated"`
		} `json:"output"`
	}
	if err := json.Unmarshal([]byte(projectionJSON), &projection); err != nil || projection.Output.Text != finalText || !projection.Output.Truncated {
		t.Fatalf("Tool Event truncation evidence = %#v err=%v", projection, err)
	}

	loaded, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope})
	if err != nil {
		t.Fatalf("LoadContext durable truncated Tool: %v", err)
	}
	var payload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(loaded.GetContextJson()), &payload); err != nil {
		t.Fatalf("decode cold truncated Tool context: %v", err)
	}
	if len(payload.ContextEntries) != 1 || len(payload.ContextEntries[0].Parts) != 2 || string(payload.ContextEntries[0].Parts[1]) != string(parts[1]) {
		t.Fatalf("cold truncated Tool result diverged: cold=%#v stored=%s", payload.ContextEntries, parts[1])
	}
	assertRuntimeHotColdToolComposition(
		t,
		baseLoaded.GetContextJson(),
		loaded.GetContextJson(),
		toolUse.GetCommitted().GetAssignedMessageSequence(),
		payload,
		toolUse.GetCommitted().GetEventId(),
		modelToolCallID,
		map[string]any{"type": "completed", "output": map[string]any{"text": originalText, "truncated": true}},
	)
}

func TestPostgreSQLDurableToolCancellationKeepsInternalErrorOutOfConversation(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_durable_cancel"
		threadID       = "sthr_durable_cancel"
		bindingID      = "bind_durable_cancel"
		podUID         = "pod_durable_cancel"
		modelRequestID = "mreq_durable_cancel"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("durable-cancellation-test-signing-key")
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_durable_cancel_start", modelRequestID, requestKindAgentProviderRequest, 0)
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_durable_cancel_use", ModelRequestId: modelRequestID,
		ToolDeclaration: bridgeToolDeclarationForTest("call_durable_cancel", "Read", `{}`, "allow", "sandbox_execute"),
	})
	if err != nil || toolUse.GetCommitted() == nil {
		t.Fatalf("write cancelled Tool Use: response=%#v err=%v", toolUse, err)
	}
	if _, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_durable_cancel_end", ModelRequestId: modelRequestID,
		FinishReason: "tool-calls", UsageJson: `{}`,
		ProviderContextRetention: &bridgev1.ProviderContextRetention{
			Disposition: "completed", AssistantMessageSequence: toolUse.GetCommitted().AssignedMessageSequence,
			ToolUseEventIds: []string{toolUse.GetCommitted().GetEventId()},
		},
	}); err != nil {
		t.Fatalf("seal cancelled Tool request: %v", err)
	}
	accepted, err := store.AcceptSandboxExecution(context.Background(), &bridgev1.AcceptSandboxExecutionRequest{
		Scope: scope, ToolUseEventId: toolUse.GetCommitted().GetEventId(),
	})
	if err != nil || accepted.GetCommitted() == nil {
		t.Fatalf("accept cancelled Read for hot receipt proof: response=%#v err=%v", accepted, err)
	}
	settleSandboxExecutionForHotReceiptProof(t, runtimeDB, admin, scope, toolUse.GetCommitted().GetEventId(),
		`{"status":"cancelled","error":{"kind":"cancelled","message":"cancelled"},"result":{}}`)
	baseLoaded, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope})
	if err != nil {
		t.Fatalf("LoadContext pending cancelled Tool: %v", err)
	}
	const cancellationError = `{"type":"runtime_shutdown","message":"internal cancellation detail","retryable":false}`
	settled, err := store.SettleToolResult(context.Background(), bridgeToolSettlementRequestForTest(scope, &bridgev1.RuntimeToolSettlement{
		ToolUseEventId: toolUse.GetCommitted().GetEventId(),
		Outcome: &bridgev1.RuntimeToolSettlement_Cancelled{Cancelled: &bridgev1.RuntimeToolCancelled{
			ErrorJson: bridgeString(cancellationError),
		}},
	}))
	if err != nil || settled.GetCommitted() == nil {
		t.Fatalf("settle cancelled Tool: response=%#v err=%v", settled, err)
	}

	var dataJSON, projectionJSON string
	if err := admin.QueryRowContext(context.Background(), `SELECT data_json FROM session_messages
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND model_request_id=$3`,
		sessionID, threadID, modelRequestID).Scan(&dataJSON); err != nil {
		t.Fatalf("read cancelled Tool context: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT projection_json FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='agent.tool_result'`,
		sessionID, threadID).Scan(&projectionJSON); err != nil {
		t.Fatalf("read cancelled Tool projection: %v", err)
	}
	parts, err := decodeStoredRuntimeContextParts(dataJSON)
	if err != nil || len(parts) != 2 {
		t.Fatalf("cancelled Tool context = %s err=%v", dataJSON, err)
	}
	var contextResult struct {
		Result map[string]any `json:"result"`
	}
	if err := json.Unmarshal(parts[1], &contextResult); err != nil || !reflect.DeepEqual(contextResult.Result, map[string]any{"type": "cancelled"}) {
		t.Fatalf("provider-visible cancellation = %#v err=%v", contextResult.Result, err)
	}
	var projection struct {
		Error map[string]any `json:"error"`
	}
	if err := json.Unmarshal([]byte(projectionJSON), &projection); err != nil || projection.Error["message"] != "internal cancellation detail" {
		t.Fatalf("internal cancellation evidence = %#v err=%v", projection.Error, err)
	}
	loaded, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope})
	if err != nil {
		t.Fatalf("LoadContext cancelled Tool: %v", err)
	}
	var payload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(loaded.GetContextJson()), &payload); err != nil || len(payload.ContextEntries) != 1 || len(payload.ContextEntries[0].Parts) != 2 || string(payload.ContextEntries[0].Parts[1]) != string(parts[1]) {
		t.Fatalf("cold cancellation context diverged: entries=%#v err=%v", payload.ContextEntries, err)
	}
	assertRuntimeHotColdToolComposition(
		t,
		baseLoaded.GetContextJson(),
		loaded.GetContextJson(),
		toolUse.GetCommitted().GetAssignedMessageSequence(),
		payload,
		toolUse.GetCommitted().GetEventId(),
		"call_durable_cancel",
		map[string]any{
			"type": "cancelled",
			"error": map[string]any{
				"type": "runtime", "code": "unknown", "message": "internal cancellation detail",
				"retryable": false, "fatal": false,
			},
		},
	)
}

func TestPostgreSQLToolSettlementUsesDirectDurableToolAuthority(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_direct_tool_authority"
		threadID       = "sthr_direct_tool_authority"
		modelRequestID = "mreq_direct_tool_authority"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_direct_tool_authority", 1, "pod_direct_tool_authority")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	scope := bridgeAPIScope(sessionID, threadID, "bind_direct_tool_authority", 1, "pod_direct_tool_authority")
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_direct_tool_start", modelRequestID, requestKindAgentProviderRequest, 0)
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_direct_tool_use", ModelRequestId: modelRequestID,
		ToolDeclaration: bridgeToolDeclarationForTest(
			"call_direct_tool_authority", "Read", `{"file_path":"/owned.txt"}`, "allow", "sandbox_execute",
		),
	})
	if err != nil || toolUse.GetCommitted() == nil {
		t.Fatalf("write direct Tool authority: %#v/%v", toolUse, err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_messages
		    SET data_json=jsonb_set(data_json::jsonb,'{parts,0,toolName}','"non_authoritative"'::jsonb)::text
		  WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND model_request_id=$3`,
		sessionID, threadID, modelRequestID,
	); err != nil {
		t.Fatalf("mutate non-authoritative Assistant projection: %v", err)
	}
	settled, err := store.SettleToolResult(context.Background(), bridgeToolSettlementRequestForTest(scope, &bridgev1.RuntimeToolSettlement{
		ToolUseEventId: toolUse.GetCommitted().GetEventId(),
		Outcome: &bridgev1.RuntimeToolSettlement_Error{Error: &bridgev1.RuntimeToolError{
			ErrorJson: `{"type":"provider_tool_protocol_error","message":"read failed","retryable":false}`,
		}},
	}))
	if err != nil || settled.GetCommitted() == nil {
		t.Fatalf("settle from direct Tool facts: %#v/%v", settled, err)
	}
	var projection struct {
		ModelToolCallID string `json:"model_tool_call_id"`
		ToolName        string `json:"tool_name"`
		State           string `json:"state"`
	}
	var projectionJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT projection_json FROM session_events
		  WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
		    AND type='agent.tool_result'`, sessionID, threadID,
	).Scan(&projectionJSON); err != nil || json.Unmarshal([]byte(projectionJSON), &projection) != nil {
		t.Fatalf("read direct Tool Result projection: %s/%v", projectionJSON, err)
	}
	if projection.ModelToolCallID != "call_direct_tool_authority" || projection.ToolName != "Read" || projection.State != "error" {
		t.Fatalf("Tool Result authority = %#v; want immutable Tool Use facts", projection)
	}
}

func assertRuntimeHotColdToolComposition(
	t *testing.T,
	baseContextJSON string,
	coldContextJSON string,
	assistantMessageSequence int64,
	coldPayload bridgeLoadContextPayload,
	toolUseEventID string,
	modelToolCallID string,
	settlement any,
) {
	t.Helper()
	input, err := json.Marshal(map[string]any{
		"contextJson":         coldContextJSON,
		"providerComposition": true,
		"hotScenario": map[string]any{
			"baseContextJson":          baseContextJSON,
			"assistantMessageSequence": assistantMessageSequence,
			"toolUseEventId":           toolUseEventID,
			"modelToolCallId":          modelToolCallID,
			"settlement":               settlement,
			"pendingToolUses":          coldPayload.PendingToolUses,
			"pendingSandboxExecutions": coldPayload.PendingSandboxExecutions,
		},
	})
	if err != nil {
		t.Fatalf("encode hot/cold Runtime composition: %v", err)
	}
	inputPath := t.TempDir() + "/runtime-hot-cold-tool.json"
	if err := os.WriteFile(inputPath, input, 0o600); err != nil {
		t.Fatalf("write hot/cold Runtime composition: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "bun", "packages/runtime-pod/test/fixtures/cold-checkpoint-composition.ts", inputPath) //nolint:gosec // Fixed repository fixture and test-owned input.
	command.Dir = "../agent-runtime"
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run Runtime hot/cold Tool composition: %v: %s", err, output)
	}
	var composed struct {
		Checkpoint          any                        `json:"checkpoint"`
		ToolRouteView       any                        `json:"toolRouteView"`
		NextStep            map[string]any             `json:"nextStep"`
		ProviderComposition runtimeProviderComposition `json:"providerComposition"`
		Hot                 struct {
			Checkpoint          any                        `json:"checkpoint"`
			ToolRouteView       any                        `json:"toolRouteView"`
			NextStep            map[string]any             `json:"nextStep"`
			ProviderComposition runtimeProviderComposition `json:"providerComposition"`
			ToolPart            any                        `json:"toolPart"`
		} `json:"hot"`
	}
	if err := json.Unmarshal(output, &composed); err != nil {
		t.Fatalf("decode Runtime hot/cold Tool composition: %v: %s", err, output)
	}
	if !reflect.DeepEqual(composed.Checkpoint, composed.Hot.Checkpoint) ||
		!reflect.DeepEqual(composed.ToolRouteView, composed.Hot.ToolRouteView) ||
		len(composed.NextStep) == 0 || len(composed.Hot.NextStep) == 0 ||
		!reflect.DeepEqual(composed.NextStep, composed.Hot.NextStep) ||
		!reflect.DeepEqual(composed.ProviderComposition, composed.Hot.ProviderComposition) || composed.Hot.ToolPart == nil {
		t.Fatalf("Runtime hot/cold Tool composition diverged: %s", output)
	}
	assertNoInventedAssistantText(t, composed.ProviderComposition)
	assertNoInventedAssistantText(t, composed.Hot.ProviderComposition)
	assertProviderCompositionToolOrder(t, composed.ProviderComposition, []string{modelToolCallID})
	assertProviderCompositionToolOrder(t, composed.Hot.ProviderComposition, []string{modelToolCallID})
}

type runtimeProviderComposition struct {
	CarrierMessages []struct {
		Role    int `json:"role"`
		Content []struct {
			Text *struct {
				Text string `json:"text"`
			} `json:"text"`
			Reasoning *struct {
				Text string `json:"text"`
			} `json:"reasoning"`
			ToolCall *struct {
				ModelToolCallID string `json:"modelToolCallId"`
				Name            string `json:"name"`
				InputJSON       string `json:"inputJson"`
			} `json:"toolCall"`
			ToolResult *struct {
				ModelToolCallID string `json:"modelToolCallId"`
				Completed       *struct {
					OutputJSON string `json:"outputJson"`
				} `json:"completed"`
				Error *struct {
					ErrorJSON string `json:"errorJson"`
				} `json:"error"`
				Cancelled *struct{} `json:"cancelled"`
			} `json:"toolResult"`
		} `json:"content"`
	} `json:"carrierMessages"`
	Strategies []struct {
		ProviderID     string `json:"providerId"`
		ModelID        string `json:"modelId"`
		ProviderFamily string `json:"providerFamily"`
		Validation     struct {
			OK bool `json:"ok"`
		} `json:"validation"`
		ProviderRequest json.RawMessage `json:"providerRequest"`
		LoweredMessages []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"loweredMessages"`
	} `json:"strategies"`
}

type runtimeColdContextComposition struct {
	NextStep struct {
		Action          string   `json:"action"`
		ToolUseEventIDs []string `json:"toolUseEventIds"`
	} `json:"nextStep"`
	ProviderComposition runtimeProviderComposition `json:"providerComposition"`
}

func runRuntimeColdContextComposition(t *testing.T, contextJSON string, composeProvider bool) runtimeColdContextComposition {
	t.Helper()
	input, err := json.Marshal(map[string]any{
		"contextJson":         contextJSON,
		"providerComposition": composeProvider,
	})
	if err != nil {
		t.Fatalf("encode Runtime cold context composition: %v", err)
	}
	inputPath := t.TempDir() + "/runtime-cold-context.json"
	if err := os.WriteFile(inputPath, input, 0o600); err != nil {
		t.Fatalf("write Runtime cold context composition: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "bun", "packages/runtime-pod/test/fixtures/cold-checkpoint-composition.ts", inputPath) //nolint:gosec // Fixed repository fixture and test-owned input.
	command.Dir = "../agent-runtime"
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run Runtime cold context composition: %v: %s", err, output)
	}
	var composed runtimeColdContextComposition
	if err := json.Unmarshal(output, &composed); err != nil {
		t.Fatalf("decode Runtime cold context composition: %v: %s", err, output)
	}
	return composed
}

type loweredProviderToolPart struct {
	Type       string `json:"type"`
	ToolCallID string `json:"toolCallId"`
	ToolName   string `json:"toolName"`
	Input      any    `json:"input"`
	Output     any    `json:"output"`
	IsError    bool   `json:"isError"`
}

func assertProviderCompositionToolOrder(t *testing.T, composition runtimeProviderComposition, expectedIDs []string) {
	t.Helper()
	if len(composition.Strategies) != 7 {
		t.Fatalf("Runtime provider strategies = %d; want the seven approved model paths", len(composition.Strategies))
	}
	type carrierCall struct {
		name  string
		input any
		index int
	}
	type carrierResult struct {
		output  any
		isError bool
		index   int
	}
	calls := map[string]carrierCall{}
	results := map[string]carrierResult{}
	callOrder := make([]string, 0, len(expectedIDs))
	resultOrder := make([]string, 0, len(expectedIDs))
	carrierIndex := 0
	for _, message := range composition.CarrierMessages {
		for _, item := range message.Content {
			if item.ToolCall != nil {
				var input any
				if err := json.Unmarshal([]byte(item.ToolCall.InputJSON), &input); err != nil {
					t.Fatalf("decode carrier Tool Call %s input: %v", item.ToolCall.ModelToolCallID, err)
				}
				calls[item.ToolCall.ModelToolCallID] = carrierCall{name: item.ToolCall.Name, input: input, index: carrierIndex}
				callOrder = append(callOrder, item.ToolCall.ModelToolCallID)
			}
			if item.ToolResult != nil {
				var output any
				isError := false
				switch {
				case item.ToolResult.Completed != nil:
					if err := json.Unmarshal([]byte(item.ToolResult.Completed.OutputJSON), &output); err != nil {
						t.Fatalf("decode carrier Tool Result %s output: %v", item.ToolResult.ModelToolCallID, err)
					}
				case item.ToolResult.Error != nil:
					if err := json.Unmarshal([]byte(item.ToolResult.Error.ErrorJSON), &output); err != nil {
						t.Fatalf("decode carrier Tool Result %s error: %v", item.ToolResult.ModelToolCallID, err)
					}
					isError = true
				case item.ToolResult.Cancelled != nil:
					output = map[string]any{
						"type": "text",
						"text": "[tool execution cancelled]",
					}
					isError = true
				default:
					t.Fatalf("carrier Tool Result %s has no terminal outcome", item.ToolResult.ModelToolCallID)
				}
				results[item.ToolResult.ModelToolCallID] = carrierResult{output: output, isError: isError, index: carrierIndex}
				resultOrder = append(resultOrder, item.ToolResult.ModelToolCallID)
			}
			carrierIndex++
		}
	}
	if !reflect.DeepEqual(callOrder, expectedIDs) || !reflect.DeepEqual(resultOrder, expectedIDs) {
		t.Fatalf("Runtime carrier Tool order calls/results = %v/%v; want %v/%v", callOrder, resultOrder, expectedIDs, expectedIDs)
	}
	for _, id := range expectedIDs {
		if calls[id].index >= results[id].index {
			t.Fatalf("Runtime carrier Tool Result %s precedes its Call", id)
		}
	}

	for _, strategy := range composition.Strategies {
		label := strategy.ProviderID + "/" + strategy.ModelID
		if !strategy.Validation.OK || len(strategy.ProviderRequest) == 0 {
			t.Fatalf("%s Runtime ProviderRequest = validation:%t bytes:%d; want one valid request", label, strategy.Validation.OK, len(strategy.ProviderRequest))
		}
		wireCalls := make([]loweredProviderToolPart, 0, len(expectedIDs))
		wireResults := make([]loweredProviderToolPart, 0, len(expectedIDs))
		callEnvelope := -1
		resultEnvelope := -1
		for messageIndex, message := range strategy.LoweredMessages {
			if len(message.Content) == 0 || message.Content[0] != '[' {
				continue
			}
			var parts []loweredProviderToolPart
			if err := json.Unmarshal(message.Content, &parts); err != nil {
				t.Fatalf("decode %s lowered content: %v", label, err)
			}
			for _, part := range parts {
				switch part.Type {
				case "tool-call":
					if callEnvelope >= 0 && callEnvelope != messageIndex {
						t.Fatalf("%s duplicated Assistant Tool Call envelope", label)
					}
					callEnvelope = messageIndex
					wireCalls = append(wireCalls, part)
				case "tool-result":
					if resultEnvelope >= 0 && resultEnvelope != messageIndex {
						t.Fatalf("%s duplicated Tool Result envelope", label)
					}
					resultEnvelope = messageIndex
					wireResults = append(wireResults, part)
				}
			}
		}
		if callEnvelope < 0 || resultEnvelope <= callEnvelope || len(wireCalls) != len(expectedIDs) || len(wireResults) != len(expectedIDs) {
			t.Fatalf("%s Tool envelopes/order calls/results = %d/%d at %d/%d", label, len(wireCalls), len(wireResults), callEnvelope, resultEnvelope)
		}
		for index, id := range expectedIDs {
			call := wireCalls[index]
			result := wireResults[index]
			carrierCall := calls[id]
			carrierResult := results[id]
			if call.ToolCallID != id || call.ToolName != carrierCall.name || !reflect.DeepEqual(call.Input, carrierCall.input) {
				t.Fatalf("%s Tool Call %d = %+v; want %s/%s/%v", label, index, call, id, carrierCall.name, carrierCall.input)
			}
			if result.ToolCallID != id || result.ToolName != carrierCall.name || !reflect.DeepEqual(result.Output, carrierResult.output) || result.IsError != carrierResult.isError {
				t.Fatalf("%s Tool Result %d = %+v; want %s/%s/%v error=%t", label, index, result, id, carrierCall.name, carrierResult.output, carrierResult.isError)
			}
		}
	}
}

func assertNoInventedAssistantText(t *testing.T, composition runtimeProviderComposition) {
	t.Helper()
	declaredText := make(map[string]int)
	for _, message := range composition.CarrierMessages {
		if message.Role != 2 {
			continue
		}
		for _, part := range message.Content {
			if part.Text != nil {
				declaredText[part.Text.Text]++
			}
			if part.Reasoning != nil {
				declaredText[part.Reasoning.Text]++
			}
		}
	}
	for _, strategy := range composition.Strategies {
		actual := make([]string, 0)
		remaining := make(map[string]int, len(declaredText))
		for text, count := range declaredText {
			remaining[text] = count
		}
		for _, message := range strategy.LoweredMessages {
			if message.Role != "assistant" || len(message.Content) == 0 || string(message.Content) == "null" {
				continue
			}
			var text string
			if err := json.Unmarshal(message.Content, &text); err == nil {
				actual = append(actual, text)
				continue
			}
			if message.Content[0] != '[' {
				t.Fatalf("decode %s Assistant content: %s", strategy.ProviderFamily, message.Content)
			}
			var parts []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			}
			if err := json.Unmarshal(message.Content, &parts); err != nil {
				t.Fatalf("decode %s Assistant content: %v", strategy.ProviderFamily, err)
			}
			for _, part := range parts {
				if part.Type == "text" {
					actual = append(actual, part.Text)
				}
			}
		}
		for _, text := range actual {
			if remaining[text] == 0 {
				t.Fatalf("%s lowered Assistant text %q was not declared by Runtime; carrier text/reasoning = %v", strategy.ProviderFamily, text, declaredText)
			}
			remaining[text]--
		}
	}
}

func settleSandboxExecutionForHotReceiptProof(
	t *testing.T,
	runtimeDB *sql.DB,
	adminDB *sql.DB,
	scope *bridgev1.RuntimeScope,
	toolUseEventID string,
	resultJSON string,
) {
	t.Helper()
	seedReadySandboxForSharedToolExecution(t, adminDB, scope.GetWorkspaceId(), scope.GetSessionId())
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtimeDB))
	queueConnection := startBackgroundNotificationQueueServer(t, queueStore)
	provider := &hotReceiptSandboxProvider{
		bridgeMemoryProjectionProvider: &bridgeMemoryProjectionProvider{},
		resultJSON:                     resultJSON,
	}
	registry, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{
		sandboxdriver.DaytonaProviderName: provider,
	})
	if err != nil {
		t.Fatalf("create Sandbox provider registry: %v", err)
	}
	runner := &tetralsandbox.SandboxToolExecutionJobRunner{
		Queue:       tetralsandbox.SandboxQueueFromGRPC(queuev1.NewQueueServiceClient(queueConnection)),
		Coordinator: tetralsandbox.NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute),
		Providers:   registry,
		Media:       backgroundNotificationMedia{},
		Config: tetralsandbox.SandboxToolExecutionRunnerConfig{
			WorkspaceID: scope.GetWorkspaceId(), LeaseOwner: "hot-receipt-proof", MaxJobs: 1,
			LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second, PreparationTimeout: 45 * time.Second,
		},
	}
	active, err := runner.RunOnceWithActivity(context.Background())
	if err != nil || !active {
		t.Fatalf("run Sandbox execution through its production owner = active %v, error %v; want true, nil", active, err)
	}
}

type hotReceiptSandboxProvider struct {
	*bridgeMemoryProjectionProvider
	resultJSON string
}

func (*hotReceiptSandboxProvider) PrepareTool(context.Context, tetralsandbox.ToolExecutionRequest) tetralsandbox.ProviderOutcome[tetralsandbox.ToolPreparationResult] {
	return tetralsandbox.ProviderOutcome[tetralsandbox.ToolPreparationResult]{Value: tetralsandbox.ToolPreparationResult{}}
}

func (p *hotReceiptSandboxProvider) ExecuteTool(context.Context, tetralsandbox.ToolExecutionRequest) tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution] {
	return tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution]{Value: sandboxdriver.ToolExecution{ResultJSON: p.resultJSON}}
}

var _ tetralsandbox.ProviderAdapter = (*hotReceiptSandboxProvider)(nil)

func TestPostgreSQLBridgeRejectsNonDurableToolErrorBeforeMutation(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_reject_error", "sthr_reject_error")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_reject_error", "bind_reject_error", 1, "pod_reject_error")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	scope := bridgeAPIScope("sesn_reject_error", "sthr_reject_error", "bind_reject_error", 1, "pod_reject_error")
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_reject_error_start", "mreq_reject_error", requestKindAgentProviderRequest, 0)
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_reject_error_use", ModelRequestId: "mreq_reject_error",
		ToolDeclaration: bridgeToolDeclarationForTest("call_reject_error", "Read", `{}`, "allow", "sandbox_execute"),
	})
	if err != nil || toolUse.GetCommitted() == nil {
		t.Fatalf("write Tool Use: response=%#v err=%v", toolUse, err)
	}
	var beforeEvents, beforeOperations int
	var beforeContext string
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id='sesn_reject_error'),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id='sesn_reject_error'),
		(SELECT data_json FROM session_messages WHERE workspace_id='default' AND session_id='sesn_reject_error'
		  AND model_request_id='mreq_reject_error')`).Scan(&beforeEvents, &beforeOperations, &beforeContext); err != nil {
		t.Fatalf("read pre-rejection state: %v", err)
	}

	_, err = store.SettleToolResult(context.Background(), bridgeToolSettlementRequestForTest(
		scope,
		&bridgev1.RuntimeToolSettlement{
			ToolUseEventId: toolUse.GetCommitted().GetEventId(),
			Outcome: &bridgev1.RuntimeToolSettlement_Error{Error: &bridgev1.RuntimeToolError{
				ErrorJson: `{"type":"runtime","code":"provider_tool_protocol_error","message":"Read failed","retryable":false,"fatal":true}`,
			}},
		},
	))
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("non-durable Tool error write err=%v; want InvalidArgument", err)
	}
	var afterEvents, afterOperations int
	var afterContext string
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id='sesn_reject_error'),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id='sesn_reject_error'),
		(SELECT data_json FROM session_messages WHERE workspace_id='default' AND session_id='sesn_reject_error'
		  AND model_request_id='mreq_reject_error')`).Scan(&afterEvents, &afterOperations, &afterContext); err != nil {
		t.Fatalf("read post-rejection state: %v", err)
	}
	if beforeEvents != afterEvents || beforeOperations != afterOperations || beforeContext != afterContext {
		t.Fatalf("rejected Tool error mutated durable state")
	}
}

func TestPostgreSQLMultiToolOutOfOrderSettlementColdComposition(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_multi_tool_cold"
		threadID  = "sthr_multi_tool_cold"
		bindingID = "bind_multi_tool_cold"
		podUID    = "pod_multi_tool_cold"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_multi_start", "mreq_multi", requestKindAgentProviderRequest, 0)
	client := startActorProductionBridge(t, runtimeDB)
	writeCall := func(writeID, callID, toolName string) *bridgev1.WriteEventResponse {
		t.Helper()
		response, err := client.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
			Scope: scope, RuntimeWriteId: writeID, ModelRequestId: "mreq_multi",
			ToolDeclaration: bridgeToolDeclarationForTest(callID, toolName, `{}`, "allow", "sandbox_execute"),
		})
		if err != nil || response.GetCommitted() == nil {
			t.Fatalf("write %s: response=%#v err=%v", callID, response, err)
		}
		return response
	}
	callA := writeCall("rwrite_multi_a", "call_multi_a", "Read")
	callB := writeCall("rwrite_multi_b", "call_multi_b", "Glob")
	if callA.GetCommitted().GetAssignedMessageSequence() != callB.GetCommitted().GetAssignedMessageSequence() {
		t.Fatalf("multi-Tool calls forked Assistant rows: A=%d B=%d", callA.GetCommitted().GetAssignedMessageSequence(), callB.GetCommitted().GetAssignedMessageSequence())
	}
	if _, err := client.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_multi_end", ModelRequestId: "mreq_multi", FinishReason: "tool-calls", UsageJson: `{}`,
		ProviderContextRetention: &bridgev1.ProviderContextRetention{
			Disposition: "completed", AssistantMessageSequence: callA.GetCommitted().AssignedMessageSequence,
			ToolUseEventIds: []string{
				callA.GetCommitted().GetEventId(),
				callB.GetCommitted().GetEventId(),
			},
		},
	}); err != nil {
		t.Fatalf("seal multi-Tool Request: %v", err)
	}
	loadParts := func() ([]string, []string, string) {
		t.Helper()
		loaded, err := client.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope})
		if err != nil {
			t.Fatalf("LoadContext multi-Tool state: %v", err)
		}
		var payload bridgeLoadContextPayload
		if err := json.Unmarshal([]byte(loaded.GetContextJson()), &payload); err != nil || len(payload.ContextEntries) != 1 {
			t.Fatalf("decode multi-Tool context: entries=%#v err=%v", payload.ContextEntries, err)
		}
		calls := make([]string, 0, 2)
		results := make([]string, 0, 2)
		for _, raw := range payload.ContextEntries[0].Parts {
			var part struct {
				Type            string `json:"type"`
				ModelToolCallID string `json:"modelToolCallId"`
			}
			if err := json.Unmarshal(raw, &part); err != nil {
				t.Fatalf("decode multi-Tool part: %v", err)
			}
			switch part.Type {
			case "tool_call":
				calls = append(calls, part.ModelToolCallID)
			case "tool_result":
				results = append(results, part.ModelToolCallID)
			}
		}
		return calls, results, loaded.GetContextJson()
	}
	calls, results, _ := loadParts()
	if !reflect.DeepEqual(calls, []string{"call_multi_a", "call_multi_b"}) || len(results) != 0 {
		t.Fatalf("pending multi-Tool state calls/results = %v/%v", calls, results)
	}
	if settled, err := client.SettleToolResult(context.Background(), bridgeToolSettlementRequestForTest(
		scope, bridgeCompletedToolSettlementForTest(callB.GetCommitted().GetEventId(), "B complete"),
	)); err != nil || settled.GetCommitted() == nil {
		t.Fatalf("settle B first: response=%#v err=%v", settled, err)
	}
	_, results, _ = loadParts()
	if !reflect.DeepEqual(results, []string{"call_multi_b"}) {
		t.Fatalf("out-of-order intermediate results = %v; want only B", results)
	}
	if settled, err := client.SettleToolResult(context.Background(), bridgeToolSettlementRequestForTest(
		scope, bridgeCompletedToolSettlementForTest(callA.GetCommitted().GetEventId(), "A complete"),
	)); err != nil || settled.GetCommitted() == nil {
		t.Fatalf("settle A second: response=%#v err=%v", settled, err)
	}
	_, results, finalContextJSON := loadParts()
	if !reflect.DeepEqual(results, []string{"call_multi_b", "call_multi_a"}) {
		t.Fatalf("durable out-of-order results = %v; want B then A", results)
	}
	assertRuntimeDirectContextComposition(t, finalContextJSON)
}
