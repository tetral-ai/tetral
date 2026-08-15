package agentruntimebridge

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"reflect"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

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
		EventType:             "agent.tool_use",
		PayloadJson:           `{"type":"agent.tool_use","name":"Read","input":{"file_path":"/missing.txt"},"evaluated_permission":"allow"}`,
		AssistantContextDelta: bridgeToolCallContextDeltaForTest("call_durable_error", "Read", `{"file_path":"/missing.txt"}`),
	})
	if err != nil || toolUse.GetCommitted() == nil {
		t.Fatalf("write Tool Use: response=%#v err=%v", toolUse, err)
	}
	if _, err := client.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_durable_error_end", ModelRequestId: "mreq_durable_error",
		FinishReason: "tool-calls", UsageJson: `{}`,
	}); err != nil {
		t.Fatalf("write Request End: %v", err)
	}
	baseLoaded, err := client.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope})
	if err != nil {
		t.Fatalf("LoadContext pending Tool state: %v", err)
	}

	const errorJSON = `{"type":"provider_tool_protocol_error","message":"Read failed","retryable":false}`
	settled, err := client.SettleToolResult(context.Background(), bridgeToolSettlementRequestForTest(
		scope,
		&bridgev1.RuntimeToolSettlement{
			ToolUseEventId: toolUse.GetCommitted().GetEventId(),
			Outcome:        &bridgev1.RuntimeToolSettlement_Error{Error: &bridgev1.RuntimeToolError{ErrorJson: errorJSON}},
		},
	))
	if err != nil {
		t.Fatalf("settle durable Tool error: %v", err)
	}
	bridgeRequireToolSettlementOutcomeForTest(t, settled, "committed")

	var dataJSON string
	if err := admin.QueryRowContext(context.Background(), `SELECT data_json FROM session_messages
		WHERE workspace_id='default' AND session_id='sesn_durable_error'
		  AND session_thread_id='sthr_durable_error' AND model_request_id='mreq_durable_error'`).Scan(&dataJSON); err != nil {
		t.Fatalf("read durable Tool error context: %v", err)
	}
	parts, err := decodeStoredRuntimeContextParts(dataJSON)
	if err != nil || len(parts) != 2 {
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
	if err := json.Unmarshal(parts[1], &result); err != nil {
		t.Fatalf("decode durable Tool result: %v", err)
	}
	var gotError, wantError map[string]any
	if err := json.Unmarshal(result.Result.Error, &gotError); err != nil {
		t.Fatalf("decode stored durable Tool error: %v", err)
	}
	if err := json.Unmarshal([]byte(errorJSON), &wantError); err != nil {
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
	if payload.OpenRequestDraft != nil || len(payload.ContextEntries) != 1 || len(payload.ContextEntries[0].Parts) != 2 {
		t.Fatalf("cold durable Tool context = entries=%#v draft=%#v", payload.ContextEntries, payload.OpenRequestDraft)
	}
	if string(payload.ContextEntries[0].Parts[1]) != string(parts[1]) {
		t.Fatalf("cold durable Tool result = %s; stored=%s", payload.ContextEntries[0].Parts[1], parts[1])
	}
	assertRuntimeHotColdToolComposition(t, baseLoaded.GetContextJson(), loaded.GetContextJson(), toolUse.GetCommitted().GetAssignedMessageSequence(), payload)
}

func assertRuntimeHotColdToolComposition(
	t *testing.T,
	baseContextJSON string,
	coldContextJSON string,
	assistantMessageSequence int64,
	coldPayload bridgeLoadContextPayload,
) {
	t.Helper()
	input, err := json.Marshal(map[string]any{
		"contextJson":         coldContextJSON,
		"providerComposition": true,
		"hotScenario": map[string]any{
			"baseContextJson":          baseContextJSON,
			"kind":                     "tool_settlement",
			"assistantMessageSequence": assistantMessageSequence,
			"modelToolCallId":          "call_durable_error",
			"settlement": map[string]any{"type": "error", "error": map[string]any{
				"type": "runtime", "code": "provider_tool_protocol_error", "message": "Read failed", "retryable": false, "fatal": false,
			}},
			"turnFacts":                coldPayload.TurnFacts,
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
		Checkpoint          any `json:"checkpoint"`
		ToolRouteView       any `json:"toolRouteView"`
		ReducerAction       any `json:"reducerAction"`
		ProviderComposition any `json:"providerComposition"`
		Hot                 struct {
			Checkpoint          any `json:"checkpoint"`
			ToolRouteView       any `json:"toolRouteView"`
			ReducerAction       any `json:"reducerAction"`
			ProviderComposition any `json:"providerComposition"`
			ToolPart            any `json:"toolPart"`
		} `json:"hot"`
	}
	if err := json.Unmarshal(output, &composed); err != nil {
		t.Fatalf("decode Runtime hot/cold Tool composition: %v: %s", err, output)
	}
	if !reflect.DeepEqual(composed.Checkpoint, composed.Hot.Checkpoint) ||
		!reflect.DeepEqual(composed.ToolRouteView, composed.Hot.ToolRouteView) ||
		!reflect.DeepEqual(composed.ReducerAction, composed.Hot.ReducerAction) ||
		!reflect.DeepEqual(composed.ProviderComposition, composed.Hot.ProviderComposition) || composed.Hot.ToolPart == nil {
		t.Fatalf("Runtime hot/cold Tool composition diverged: %s", output)
	}
}

func TestPostgreSQLBridgeRejectsNonDurableToolErrorBeforeMutation(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_reject_error", "sthr_reject_error")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_reject_error", "bind_reject_error", 1, "pod_reject_error")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	scope := bridgeAPIScope("sesn_reject_error", "sthr_reject_error", "bind_reject_error", 1, "pod_reject_error")
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_reject_error_start", "mreq_reject_error", requestKindAgentProviderRequest, 0)
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_reject_error_use", ModelRequestId: "mreq_reject_error",
		EventType:             "agent.tool_use",
		PayloadJson:           `{"type":"agent.tool_use","name":"Read","input":{},"evaluated_permission":"allow"}`,
		AssistantContextDelta: bridgeToolCallContextDeltaForTest("call_reject_error", "Read", `{}`),
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
			EventType:             "agent.tool_use",
			PayloadJson:           `{"type":"agent.tool_use","name":"` + toolName + `","input":{},"evaluated_permission":"allow"}`,
			AssistantContextDelta: bridgeToolCallContextDeltaForTest(callID, toolName, `{}`),
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
