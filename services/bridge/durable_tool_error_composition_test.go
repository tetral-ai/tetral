package agentruntimebridge

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// This composition owns the Runtime declaration -> Bridge persistence -> Runtime cold-load
// contract for terminal Tool errors. It deliberately obtains error_json from the production
// Runtime adapter instead of constructing the durable error shape in Go.
func TestPostgreSQLDurableToolErrorDeclarationColdLoadsAndReducerContinues(t *testing.T) {
	t.Run("ordinary Read error", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		const (
			sessionID    = "sesn_durable_tool_error"
			threadID     = "thr_durable_tool_error"
			bindingID    = "bind_durable_tool_error"
			podUID       = "pod_durable_tool_error"
			modelRequest = "mreq_durable_tool_error"
			modelCall    = "call_durable_tool_error"
		)
		seedBridgeAPISession(t, admin, "default", sessionID, threadID)
		seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
		store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
		store.RuntimeBindingTokenHMACKey = []byte("durable-tool-error-composition-key")
		scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
		requestStart := seedBridgeAPIRequestStart(t, store, scope, "rwrite_durable_tool_error_start", modelRequest, "agent_provider_request", 0)
		toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
			Scope: scope, RuntimeWriteId: "rwrite_durable_tool_error_use", ModelRequestId: modelRequest,
			EventType: "agent.tool_use", PayloadJson: `{"type":"agent.tool_use","name":"Read","input":{"file_path":"/missing.txt"},"evaluated_permission":"allow"}`,
			AssistantPartAppend: bridgeRuntimeOutputAppendForTest(
				t, scope, "rwrite_durable_tool_error_use", "agent.tool_use", "streaming",
				bridgeRuntimePartCreateForTest{kind: "tool", json: `{"type":"tool","toolCallId":"` + modelCall + `","toolName":"Read","toolEvent":{"kind":"tool"},"state":{"status":"running","input":{"value":{"file_path":"/missing.txt"},"preview":"{\"file_path\":\"/missing.txt\"}","truncated":false}}}`},
			),
		})
		if err != nil {
			t.Fatalf("write ordinary Read Tool Use: %v", err)
		}
		if _, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
			Scope: scope, RuntimeWriteId: "rwrite_durable_tool_error_end", ModelRequestId: modelRequest,
			ModelRequestStartEventId: requestStart.GetEventId(), RequestKind: "agent_provider_request",
			FinishReason: "tool-calls", UsageJson: `{}`,
		}); err != nil {
			t.Fatalf("write ordinary Read request end: %v", err)
		}
		hotBase, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
			Scope: scope, RuntimeInputId: "rin_durable_tool_error_hot_base",
		})
		if err != nil {
			t.Fatalf("LoadContext before ordinary Tool settlement: %v", err)
		}

		declared := runRuntimeDurableToolErrorDeclaration(t, map[string]any{
			"workspaceId": "default", "sessionId": sessionID, "sessionThreadId": threadID,
			"bindingId": bindingID, "bindingGeneration": 1, "targetPodUid": podUID,
			"modelRequestId": modelRequest, "toolUseEventId": toolUse.GetEventId(),
		})
		committed, err := store.SettleToolResult(context.Background(), bridgeToolSettlementRequestForTest(
			scope,
			&bridgev1.RuntimeToolSettlement{
				ToolUseEventId: declared.ToolUseEventID,
				Outcome:        &bridgev1.RuntimeToolSettlement_Error{Error: &bridgev1.RuntimeToolError{ErrorJson: declared.ErrorJSON}},
			},
		))
		if err != nil {
			t.Fatalf("commit ordinary Read Tool error: response=%#v err=%v", committed, err)
		}
		bridgeRequireToolSettlementOutcomeForTest(t, committed, "committed")
		loaded, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
			Scope: scope, RuntimeInputId: "rin_durable_tool_error_cold",
		})
		if err != nil {
			t.Fatalf("LoadContext after ordinary Tool error: %v", err)
		}
		checkpoint := runColdCheckpointCompositionInput(t, map[string]any{
			"contextJson": loaded.GetContextJson(),
			"hotScenario": map[string]any{
				"kind": "tool_settlement", "baseContextJson": hotBase.GetContextJson(),
				"sessionId": sessionID, "sessionThreadId": threadID,
				"settlement": declared.RuntimeSettlement, "toolUseEventId": toolUse.GetEventId(),
				"pendingToolUses": []map[string]any{{
					"toolUseEventId": toolUse.GetEventId(), "modelRequestId": modelRequest,
					"modelToolCallId": modelCall, "toolName": "Read", "input": map[string]any{"file_path": "/missing.txt"},
					"decision": "allow", "status": "resolving",
				}},
			},
		})
		if checkpoint.Checkpoint.Request == nil || checkpoint.Checkpoint.Request.ModelRequestID != modelRequest ||
			checkpoint.Checkpoint.Request.ToolMemberCount != 1 || checkpoint.ReducerAction != "prepare_next_request" {
			t.Fatalf("ordinary Tool error cold checkpoint = %+v; want one terminal member and next request", checkpoint)
		}
		if !reflect.DeepEqual(checkpoint.Semantic, checkpoint.HotSemantic) {
			t.Fatalf("hot/cold Tool settlement reducer semantics differ: cold=%#v hot=%#v", checkpoint.Semantic, checkpoint.HotSemantic)
		}

		var durableMessageJSON string
		if err := admin.QueryRowContext(context.Background(), `SELECT data_json FROM session_messages
			WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND model_request_id=$3`,
			sessionID, threadID, modelRequest).Scan(&durableMessageJSON); err != nil {
			t.Fatalf("read ordinary Tool error projection: %v", err)
		}
		var durableMessage struct {
			Parts []struct {
				ToolCallID     string         `json:"toolCallId"`
				ToolUseEventID string         `json:"toolUseEventId"`
				State          map[string]any `json:"state"`
			} `json:"parts"`
		}
		if err := json.Unmarshal([]byte(durableMessageJSON), &durableMessage); err != nil || len(durableMessage.Parts) != 1 {
			t.Fatalf("decode ordinary Tool error projection: err=%v message=%s", err, durableMessageJSON)
		}
		part := durableMessage.Parts[0]
		if part.ToolCallID != modelCall || part.ToolUseEventID != toolUse.GetEventId() || part.State["status"] != "error" {
			t.Fatalf("cold Tool identity/state = call %q use %q state %#v", part.ToolCallID, part.ToolUseEventID, part.State)
		}
		hotState, _ := checkpoint.HotToolPart["state"].(map[string]any)
		if checkpoint.HotToolPart["toolCallId"] != modelCall || checkpoint.HotToolPart["toolUseEventId"] != toolUse.GetEventId() ||
			!reflect.DeepEqual(hotState, part.State) {
			t.Fatalf("hot/cold terminal Tool parts differ: hot=%#v cold_call=%q cold_use=%q cold_state=%#v",
				checkpoint.HotToolPart, part.ToolCallID, part.ToolUseEventID, part.State)
		}
		var declaredError map[string]any
		if err := json.Unmarshal([]byte(declared.ErrorJSON), &declaredError); err != nil {
			t.Fatalf("decode Runtime-declared Tool error: %v", err)
		}
		storedError, ok := part.State["error"].(map[string]any)
		if !ok || !reflect.DeepEqual(storedError, declaredError) || !reflect.DeepEqual(storedError, declared.ExpectedDurableError) {
			t.Fatalf("stored Tool error = %#v; Runtime declaration = %#v; hot projection = %#v", storedError, declaredError, declared.ExpectedDurableError)
		}
		if len(storedError) != 3 || storedError["type"] == nil || storedError["message"] == nil || storedError["retryable"] == nil {
			t.Fatalf("stored Tool error keys = %#v; want exactly type/message/retryable", storedError)
		}
		for _, forbidden := range []string{"code", "fatal", "operation", "reason"} {
			if _, exists := storedError[forbidden]; exists {
				t.Fatalf("stored Tool error retained Runtime lifecycle field %q: %#v", forbidden, storedError)
			}
		}

		const (
			nextRuntimeInputID = "rin_durable_tool_error_next_input"
			nextInputEventID   = "evt_durable_tool_error_next_input"
		)
		var nextEventSequence int64
		if err := admin.QueryRowContext(context.Background(), `SELECT COALESCE(max(sequence), 0) + 1 FROM session_events
			WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2`, sessionID, threadID).Scan(&nextEventSequence); err != nil {
			t.Fatalf("select next input Event sequence: %v", err)
		}
		seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, nextInputEventID, nextEventSequence, "user.message", `{"content":[{"type":"text","text":"continue"}]}`)
		seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, nextRuntimeInputID, "messages", `["`+nextInputEventID+`"]`, "delivering", bindingID, podUID, nextEventSequence, nextEventSequence)
		accepted, err := store.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
			Scope: scope, RuntimeInputId: nextRuntimeInputID, InputKind: "messages",
			EventIds: []string{nextInputEventID}, SequenceFrom: nextEventSequence, SequenceTo: nextEventSequence,
			MessageCreates: []*bridgev1.RuntimeMessageCreate{bridgeUserInputCreateForTest(
				"default", sessionID, threadID, nextRuntimeInputID, nextInputEventID, "continue",
			)},
		})
		if err != nil || accepted.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
			t.Fatalf("accept next input after Tool-error cold load: response=%#v err=%v", accepted, err)
		}
		resumed, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
			Scope: scope, RuntimeInputId: "rin_durable_tool_error_resumed_cold",
		})
		if err != nil {
			t.Fatalf("LoadContext after accepting next input: %v", err)
		}
		resumedCheckpoint := runColdCheckpointComposition(t, resumed.GetContextJson())
		if len(resumedCheckpoint.Checkpoint.PendingInputMessageIDs) != 1 || resumedCheckpoint.ReducerAction != "prepare_next_request" {
			t.Fatalf("resumed Tool-error checkpoint = %+v; want one accepted input and reducer continuation", resumedCheckpoint)
		}

	})

	t.Run("completed-only control", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		const (
			sessionID    = "sesn_durable_tool_completed_control"
			threadID     = "thr_durable_tool_completed_control"
			modelRequest = "mreq_durable_tool_completed_control"
		)
		seedBridgeAPISession(t, admin, "default", sessionID, threadID)
		seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_durable_tool_completed", 1, "pod_durable_tool_completed")
		store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
		store.RuntimeBindingTokenHMACKey = []byte("durable-tool-completed-control-key")
		scope := bridgeAPIScope(sessionID, threadID, "bind_durable_tool_completed", 1, "pod_durable_tool_completed")
		requestStart := seedBridgeAPIRequestStart(t, store, scope, "rwrite_durable_tool_completed_start", modelRequest, "agent_provider_request", 0)
		toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
			Scope: scope, RuntimeWriteId: "rwrite_durable_tool_completed_use", ModelRequestId: modelRequest,
			EventType: "agent.tool_use", PayloadJson: `{"type":"agent.tool_use","name":"Read","input":{"file_path":"/present.txt"},"evaluated_permission":"allow"}`,
			AssistantPartAppend: bridgeRuntimeOutputAppendForTest(
				t, scope, "rwrite_durable_tool_completed_use", "agent.tool_use", "streaming",
				bridgeRuntimePartCreateForTest{kind: "tool", json: `{"type":"tool","toolCallId":"call_durable_tool_completed","toolName":"Read","toolEvent":{"kind":"tool"},"state":{"status":"running","input":{"value":{"file_path":"/present.txt"},"preview":"{\"file_path\":\"/present.txt\"}","truncated":false}}}`},
			),
		})
		if err != nil {
			t.Fatalf("write completed control Tool Use: %v", err)
		}
		if _, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
			Scope: scope, RuntimeWriteId: "rwrite_durable_tool_completed_end", ModelRequestId: modelRequest,
			ModelRequestStartEventId: requestStart.GetEventId(), RequestKind: "agent_provider_request",
			FinishReason: "tool-calls", UsageJson: `{}`,
		}); err != nil {
			t.Fatalf("write completed control request end: %v", err)
		}
		if response, err := store.SettleToolResult(context.Background(), bridgeToolSettlementRequestForTest(
			scope,
			bridgeCompletedToolSettlementForTest(toolUse.GetEventId(), "present"),
		)); err != nil {
			t.Fatalf("commit completed control Tool result: %v", err)
		} else {
			bridgeRequireToolSettlementOutcomeForTest(t, response, "committed")
		}
		loaded, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope, RuntimeInputId: "rin_durable_tool_completed_cold"})
		if err != nil {
			t.Fatalf("LoadContext completed control: %v", err)
		}
		checkpoint := runColdCheckpointComposition(t, loaded.GetContextJson())
		if checkpoint.Checkpoint.Request == nil || checkpoint.Checkpoint.Request.ToolMemberCount != 1 || checkpoint.ReducerAction != "prepare_next_request" {
			t.Fatalf("completed Tool control checkpoint = %+v", checkpoint)
		}
	})
}

func TestPostgreSQLBridgeRejectsNonDurableToolErrorsBeforeMutation(t *testing.T) {
	tests := []struct {
		name       string
		errorJSON  string
		settlement func(string, string) *bridgev1.RuntimeToolSettlement
	}{
		{
			name:      "complete Runtime failure",
			errorJSON: `{"type":"runtime","code":"provider_tool_protocol_error","message":"Read failed","retryable":false,"fatal":true}`,
			settlement: func(toolUseEventID, errorJSON string) *bridgev1.RuntimeToolSettlement {
				return &bridgev1.RuntimeToolSettlement{ToolUseEventId: toolUseEventID, Outcome: &bridgev1.RuntimeToolSettlement_Error{Error: &bridgev1.RuntimeToolError{ErrorJson: errorJSON}}}
			},
		},
		{
			name:      "cancelled error with extra field",
			errorJSON: `{"type":"provider_cancelled","message":"Read cancelled","retryable":false,"fatal":false}`,
			settlement: func(toolUseEventID, errorJSON string) *bridgev1.RuntimeToolSettlement {
				return &bridgev1.RuntimeToolSettlement{ToolUseEventId: toolUseEventID, Outcome: &bridgev1.RuntimeToolSettlement_Cancelled{Cancelled: &bridgev1.RuntimeToolCancelled{ErrorJson: &errorJSON}}}
			},
		},
	}
	for testIndex, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID := "sesn_reject_nondurable_tool_error_" + string(rune('a'+testIndex))
			threadID := "thr_reject_nondurable_tool_error_" + string(rune('a'+testIndex))
			modelRequest := "mreq_reject_nondurable_tool_error_" + string(rune('a'+testIndex))
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_reject_nondurable", 1, "pod_reject_nondurable")
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			scope := bridgeAPIScope(sessionID, threadID, "bind_reject_nondurable", 1, "pod_reject_nondurable")
			seedBridgeAPIRequestStart(t, store, scope, "rwrite_reject_nondurable_start", modelRequest, "agent_provider_request", 0)
			toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
				Scope: scope, RuntimeWriteId: "rwrite_reject_nondurable_use", ModelRequestId: modelRequest,
				EventType: "agent.tool_use", PayloadJson: `{"type":"agent.tool_use","name":"Read","input":{"file_path":"/missing.txt"},"evaluated_permission":"allow"}`,
				AssistantPartAppend: bridgeRuntimeOutputAppendForTest(
					t, scope, "rwrite_reject_nondurable_use", "agent.tool_use", "streaming",
					bridgeRuntimePartCreateForTest{kind: "tool", json: `{"type":"tool","toolCallId":"call_reject_nondurable","toolName":"Read","toolEvent":{"kind":"tool"},"state":{"status":"running","input":{"value":{"file_path":"/missing.txt"},"preview":"missing","truncated":false}}}`},
				),
			})
			if err != nil {
				t.Fatalf("seed Tool Use: %v", err)
			}
			var beforeEvents, beforeMessages, beforeOperations int
			var beforeMessageJSON string
			if err := admin.QueryRowContext(context.Background(), `SELECT
				(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1),
				(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$1),
				(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1),
				(SELECT data_json FROM session_messages WHERE workspace_id='default' AND session_id=$1 AND model_request_id=$2)`, sessionID, modelRequest).
				Scan(&beforeEvents, &beforeMessages, &beforeOperations, &beforeMessageJSON); err != nil {
				t.Fatalf("read pre-rejection durable state: %v", err)
			}
			_, err = store.SettleToolResult(context.Background(), bridgeToolSettlementRequestForTest(
				scope,
				test.settlement(toolUse.GetEventId(), test.errorJSON),
			))
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("non-durable Tool error write err=%v; want InvalidArgument", err)
			}
			var afterEvents, afterMessages, afterOperations int
			var afterMessageJSON string
			if err := admin.QueryRowContext(context.Background(), `SELECT
				(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1),
				(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$1),
				(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1),
				(SELECT data_json FROM session_messages WHERE workspace_id='default' AND session_id=$1 AND model_request_id=$2)`, sessionID, modelRequest).
				Scan(&afterEvents, &afterMessages, &afterOperations, &afterMessageJSON); err != nil {
				t.Fatalf("read post-rejection durable state: %v", err)
			}
			if beforeEvents != afterEvents || beforeMessages != afterMessages || beforeOperations != afterOperations || beforeMessageJSON != afterMessageJSON {
				t.Fatalf("rejected Tool error mutated durable state: before=%d/%d/%d after=%d/%d/%d message_changed=%t",
					beforeEvents, beforeMessages, beforeOperations, afterEvents, afterMessages, afterOperations, beforeMessageJSON != afterMessageJSON)
			}
		})
	}
}

type runtimeDurableToolErrorDeclaration struct {
	EventType            string          `json:"eventType"`
	PayloadJSON          string          `json:"payloadJson"`
	ToolUseEventID       string          `json:"toolUseEventId"`
	ErrorJSON            string          `json:"errorJson"`
	RuntimeSettlement    json.RawMessage `json:"runtimeSettlement"`
	ExpectedDurableError map[string]any  `json:"expectedDurableError"`
}

func runRuntimeDurableToolErrorDeclaration(t *testing.T, input any) runtimeDurableToolErrorDeclaration {
	t.Helper()
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("encode Runtime Tool error declaration input: %v", err)
	}
	inputPath := filepath.Join(t.TempDir(), "durable-tool-error-declaration.json")
	if err := os.WriteFile(inputPath, raw, 0o600); err != nil {
		t.Fatalf("write Runtime Tool error declaration input: %v", err)
	}
	command := exec.CommandContext(context.Background(), "bun", "packages/runtime-pod/test/fixtures/durable-tool-error-declaration.ts", inputPath) //nolint:gosec // Fixed repository fixture and test-owned path.
	command.Dir = "../agent-runtime"
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run Runtime Tool error declaration fixture: %v: %s", err, output)
	}
	var declaration runtimeDurableToolErrorDeclaration
	if err := json.Unmarshal(output, &declaration); err != nil {
		t.Fatalf("decode Runtime Tool error declaration: %v: %s", err, output)
	}
	return declaration
}
