package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"
)

// The Runtime's tests hold that a call to a Tool outside the family's catalog
// becomes this internal repair; here Bridge stores what it declares and
// restores it as one call/error pair between the two Assistant texts.
func TestPostgreSQLInvalidToolRepairStoresOnlyItsPair(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_invalid_tool_production"
		threadID  = "sthr_invalid_tool_production"
		bindingID = "bind_invalid_tool_production"
		podUID    = "pod_invalid_tool_production"
	)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIProjectedUserMessage(t, admin, sessionID, threadID, "msg_invalid_tool_production", "sevt_invalid_tool_production", 1)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_messages
		SET data_json='{"parts":[{"type":"text","text":"continue"}]}'
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND sequence=1`, sessionID, threadID); err != nil {
		t.Fatalf("seed invalid-tool Runtime context: %v", err)
	}
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("invalid-tool-production-signing-key")
	scope := sessionfixture.BridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	ctx := context.Background()
	text := func(writeID, modelRequestID, eventID, value string) {
		t.Helper()
		payload, _ := json.Marshal(map[string]any{"type": "agent.message", "content": []any{map[string]any{"type": "text", "text": value}}})
		response, err := store.WriteEvent(ctx, &bridgev1.WriteEventRequest{Scope: scope, RuntimeWriteId: writeID, ModelRequestId: modelRequestID, EventType: "agent.message", PayloadJson: string(payload), PreallocatedEventId: &eventID, AssistantContextDelta: &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_Text{Text: &bridgev1.RuntimeContextText{Text: value}}}}}})
		if err != nil || response.GetCommitted() == nil {
			t.Fatalf("write Assistant text %q: %v/%v", value, response, err)
		}
	}
	end := func(writeID, modelRequestID, finishReason string, sequence int64, repairs ...string) {
		t.Helper()
		if _, err := store.WriteRequestEnd(ctx, &bridgev1.WriteRequestEndRequest{Scope: scope, RuntimeWriteId: writeID, ModelRequestId: modelRequestID, FinishReason: finishReason, UsageJson: `{}`, ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "completed", AssistantMessageSequence: &sequence, RepairEventIds: repairs}}); err != nil {
			t.Fatalf("end %s: %v", modelRequestID, err)
		}
	}
	if _, err := store.WriteEvent(ctx, &bridgev1.WriteEventRequest{Scope: scope, RuntimeWriteId: "running", EventType: "session.status_running", PayloadJson: `{"type":"session.status_running"}`}); err != nil {
		t.Fatal(err)
	}
	seedBridgeAPIRequestStart(t, store, scope, "start-repair", "model_request-2", runtimecontrol.RequestKindAgentProviderRequest, 1)
	text("text-continuing", "model_request-2", "evt_fb0da9fcbcc2a8b2e83a85dccc724029", "continuing")
	repair, err := store.CommitInternalToolRepair(ctx, &bridgev1.CommitInternalToolRepairRequest{
		Scope: scope, ModelRequestId: "model_request-2", ModelToolCallId: "call_invalid_tool_repair_fixture", ToolName: "exec_command",
		CanonicalInputJson: `{"command":"must not execute"}`,
		Error:              &bridgev1.RuntimeToolError{ErrorJson: `{"type":"runtime_invalid_sequence","message":"disabled or unknown tool call: exec_command","retryable":false}`},
		// The key the Runtime derived for this repair; Bridge recomputes it.
		RepairKey: "internal_invalid_tool_77eb08327ed819a277174c3535590ef63a6f2b6abcca327186620fb7d0b6df2c",
	})
	if err != nil || repair.GetCommitted() == nil {
		t.Fatalf("commit invalid-tool repair: %v/%v", repair, err)
	}
	end("end-repair", "model_request-2", "tool-calls", 2, repair.GetCommitted().GetRepairEventId())
	seedBridgeAPIRequestStart(t, store, scope, "start-continued", "mreq_continued", runtimecontrol.RequestKindAgentProviderRequest, 2)
	text("text-continued", "mreq_continued", "evt_faa045888cdef9cb214327292de0a493", "continued")
	end("end-continued", "mreq_continued", "stop", 3)

	var toolUses, repairResults, executionRows, pendingRows, queueRows int
	if err := admin.QueryRowContext(ctx, `SELECT
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
	loaded, err := store.LoadContext(ctx, &bridgev1.LoadContextRequest{Scope: scope})
	if err != nil {
		t.Fatalf("cold-load invalid-tool repair: %v", err)
	}
	pinBridgeContext(t, "invalid-tool-repair-continued", loaded.GetContextJson())
}

func TestPostgreSQLDurableToolErrorSettlesIntoNarrowColdContext(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", "sesn_durable_error", "sthr_durable_error")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_durable_error", "bind_durable_error", 1, "pod_durable_error")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("durable-error-test-signing-key")
	client := startActorProductionBridge(t, runtimeDB)
	scope := sessionfixture.BridgeAPIScope("sesn_durable_error", "sthr_durable_error", "bind_durable_error", 1, "pod_durable_error")
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_durable_error_start", "mreq_durable_error", runtimecontrol.RequestKindAgentProviderRequest, 0)

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

	pinBridgeContext(t, "durable-tool-error-awaiting-result", baseLoaded.GetContextJson())
	// The Runtime's declaration of that missing-file Read; its tests settle the
	// same Tool hot and must reach the context Bridge restores from this.
	const declaredError = `{"type":"runtime_invalid_sequence","message":"path does not exist\n\nPartial result:\n{}","retryable":false}`
	settlementRequest := sessionfixture.BridgeToolSettlementRequestForTest(
		scope,
		&bridgev1.RuntimeToolSettlement{
			ToolUseEventId: toolUse.GetCommitted().GetEventId(),
			Outcome:        &bridgev1.RuntimeToolSettlement_Error{Error: &bridgev1.RuntimeToolError{ErrorJson: declaredError}},
		},
	)
	settled, err := client.SettleToolResult(context.Background(), settlementRequest)
	if err != nil {
		t.Fatalf("settle durable Tool error: %v", err)
	}
	sessionfixture.BridgeRequireToolSettlementOutcomeForTest(t, settled, "committed")
	replayed, err := client.SettleToolResult(context.Background(), settlementRequest)
	if err != nil {
		t.Fatalf("replay durable Tool error after lost acknowledgement: %v", err)
	}
	sessionfixture.BridgeRequireToolSettlementOutcomeForTest(t, replayed, "duplicate")

	var dataJSON string
	if err := admin.QueryRowContext(context.Background(), `SELECT `+sessionfixture.MessageContentSQL+` FROM session_messages m
		WHERE workspace_id='default' AND session_id='sesn_durable_error'
		  AND session_thread_id='sthr_durable_error' AND model_request_id='mreq_durable_error'`).Scan(&dataJSON); err != nil {
		t.Fatalf("read durable Tool error context: %v", err)
	}
	parts, err := runtimecontrol.DecodeStoredRuntimeContextParts(dataJSON)
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
	if err := json.Unmarshal([]byte(declaredError), &wantError); err != nil {
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
	// End seals the selected Assistant; settling its Tool does not finish the
	// durable turn. Until FinishIdle, the reference still names that same row.
	wantMessageSequence := toolUse.GetCommitted().GetAssignedMessageSequence()
	if payload.CurrentRequestMessage == nil ||
		payload.CurrentRequestMessage.ModelRequestID != "mreq_durable_error" ||
		payload.CurrentRequestMessage.AssistantMessageSequence != wantMessageSequence ||
		len(payload.Messages) != 1 || payload.Messages[0].ContextKind != "assistant" ||
		payload.Messages[0].MessageSequence != wantMessageSequence || len(payload.Messages[0].Parts) != 3 {
		t.Fatalf("cold durable Tool context = entries=%#v currentRequestMessage=%#v; want sealed mreq_durable_error Assistant sequence %d", payload.Messages, payload.CurrentRequestMessage, wantMessageSequence)
	}
	if string(payload.Messages[0].Parts[2]) != string(parts[2]) {
		t.Fatalf("cold durable Tool result = %s; stored=%s", payload.Messages[0].Parts[2], parts[2])
	}
	pinBridgeContext(t, "durable-tool-error-settled", loaded.GetContextJson())
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
	sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("durable-truncation-test-signing-key")
	scope := sessionfixture.BridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_durable_truncation_start", modelRequestID, runtimecontrol.RequestKindAgentProviderRequest, 0)

	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_durable_truncation_use", ModelRequestId: modelRequestID,
		ToolDeclaration: sessionfixture.BridgeToolDeclarationForTest(modelToolCallID, "Read", `{}`, "allow", "sandbox_execute"),
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

	settled, err := store.SettleToolResult(context.Background(), sessionfixture.BridgeToolSettlementRequestForTest(scope, &bridgev1.RuntimeToolSettlement{
		ToolUseEventId: toolUse.GetCommitted().GetEventId(),
		Outcome: &bridgev1.RuntimeToolSettlement_Completed{Completed: &bridgev1.RuntimeToolCompleted{
			OutputJson: `{"text":"partial output\n\n[Tool output was truncated to fit the provider context limit.]","truncated":true}`,
		}},
	}))
	if err != nil || settled.GetCommitted() == nil {
		t.Fatalf("settle truncated Tool completion: response=%#v err=%v", settled, err)
	}

	var dataJSON, projectionJSON string
	if err := admin.QueryRowContext(context.Background(), `SELECT `+sessionfixture.MessageContentSQL+` FROM session_messages m
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND model_request_id=$3`,
		sessionID, threadID, modelRequestID).Scan(&dataJSON); err != nil {
		t.Fatalf("read durable truncated Tool context: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT projection_json FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='agent.tool_result'`,
		sessionID, threadID).Scan(&projectionJSON); err != nil {
		t.Fatalf("read truncated Tool Event projection: %v", err)
	}
	parts, err := runtimecontrol.DecodeStoredRuntimeContextParts(dataJSON)
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
	if len(payload.Messages) != 1 || len(payload.Messages[0].Parts) != 2 || string(payload.Messages[0].Parts[1]) != string(parts[1]) {
		t.Fatalf("cold truncated Tool result diverged: cold=%#v stored=%s", payload.Messages, parts[1])
	}
	pinBridgeContext(t, "durable-tool-truncated-awaiting-result", baseLoaded.GetContextJson())
	pinBridgeContext(t, "durable-tool-truncated-settled", loaded.GetContextJson())
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
	sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("durable-cancellation-test-signing-key")
	scope := sessionfixture.BridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_durable_cancel_start", modelRequestID, runtimecontrol.RequestKindAgentProviderRequest, 0)
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_durable_cancel_use", ModelRequestId: modelRequestID,
		ToolDeclaration: sessionfixture.BridgeToolDeclarationForTest("call_durable_cancel", "Read", `{}`, "allow", "sandbox_execute"),
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
	settled, err := store.SettleToolResult(context.Background(), sessionfixture.BridgeToolSettlementRequestForTest(scope, &bridgev1.RuntimeToolSettlement{
		ToolUseEventId: toolUse.GetCommitted().GetEventId(),
		Outcome: &bridgev1.RuntimeToolSettlement_Cancelled{Cancelled: &bridgev1.RuntimeToolCancelled{
			ErrorJson: bridgeString(cancellationError),
		}},
	}))
	if err != nil || settled.GetCommitted() == nil {
		t.Fatalf("settle cancelled Tool: response=%#v err=%v", settled, err)
	}

	var dataJSON, projectionJSON string
	if err := admin.QueryRowContext(context.Background(), `SELECT `+sessionfixture.MessageContentSQL+` FROM session_messages m
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND model_request_id=$3`,
		sessionID, threadID, modelRequestID).Scan(&dataJSON); err != nil {
		t.Fatalf("read cancelled Tool context: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT projection_json FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='agent.tool_result'`,
		sessionID, threadID).Scan(&projectionJSON); err != nil {
		t.Fatalf("read cancelled Tool projection: %v", err)
	}
	parts, err := runtimecontrol.DecodeStoredRuntimeContextParts(dataJSON)
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
	if err := json.Unmarshal([]byte(loaded.GetContextJson()), &payload); err != nil || len(payload.Messages) != 1 || len(payload.Messages[0].Parts) != 2 || string(payload.Messages[0].Parts[1]) != string(parts[1]) {
		t.Fatalf("cold cancellation context diverged: entries=%#v err=%v", payload.Messages, err)
	}
	pinBridgeContext(t, "durable-tool-cancelled-awaiting-result", baseLoaded.GetContextJson())
	pinBridgeContext(t, "durable-tool-cancelled-settled", loaded.GetContextJson())
}

func TestPostgreSQLToolSettlementUsesDirectDurableToolAuthority(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_direct_tool_authority"
		threadID       = "sthr_direct_tool_authority"
		modelRequestID = "mreq_direct_tool_authority"
	)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_direct_tool_authority", 1, "pod_direct_tool_authority")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	scope := sessionfixture.BridgeAPIScope(sessionID, threadID, "bind_direct_tool_authority", 1, "pod_direct_tool_authority")
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_direct_tool_start", modelRequestID, runtimecontrol.RequestKindAgentProviderRequest, 0)
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_direct_tool_use", ModelRequestId: modelRequestID,
		ToolDeclaration: sessionfixture.BridgeToolDeclarationForTest(
			"call_direct_tool_authority", "Read", `{"file_path":"/owned.txt"}`, "allow", "sandbox_execute",
		),
	})
	if err != nil || toolUse.GetCommitted() == nil {
		t.Fatalf("write direct Tool authority: %#v/%v", toolUse, err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_message_parts part
		    SET data_json=jsonb_set(part.data_json::jsonb,'{toolName}','"non_authoritative"'::jsonb)::text
		   FROM session_messages m
		  WHERE m.workspace_id='default' AND m.session_id=$1 AND m.session_thread_id=$2 AND m.model_request_id=$3
		    AND part.workspace_id=m.workspace_id AND part.message_id=m.message_id AND part.part_index=0`,
		sessionID, threadID, modelRequestID,
	); err != nil {
		t.Fatalf("mutate non-authoritative Assistant projection: %v", err)
	}
	settled, err := store.SettleToolResult(context.Background(), sessionfixture.BridgeToolSettlementRequestForTest(scope, &bridgev1.RuntimeToolSettlement{
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

func settleSandboxExecutionForHotReceiptProof(
	t *testing.T,
	runtimeDB *sql.DB,
	adminDB *sql.DB,
	scope *bridgev1.RuntimeScope,
	toolUseEventID string,
	resultJSON string,
) {
	t.Helper()
	sessionfixture.SeedReadySandboxForSharedToolExecution(t, adminDB, scope.GetWorkspaceId(), scope.GetSessionId())
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

func TestPostgreSQLBridgeRejectsNonDurableToolErrorBeforeMutation(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", "sesn_reject_error", "sthr_reject_error")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_reject_error", "bind_reject_error", 1, "pod_reject_error")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	scope := sessionfixture.BridgeAPIScope("sesn_reject_error", "sthr_reject_error", "bind_reject_error", 1, "pod_reject_error")
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_reject_error_start", "mreq_reject_error", runtimecontrol.RequestKindAgentProviderRequest, 0)
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_reject_error_use", ModelRequestId: "mreq_reject_error",
		ToolDeclaration: sessionfixture.BridgeToolDeclarationForTest("call_reject_error", "Read", `{}`, "allow", "sandbox_execute"),
	})
	if err != nil || toolUse.GetCommitted() == nil {
		t.Fatalf("write Tool Use: response=%#v err=%v", toolUse, err)
	}
	var beforeEvents, beforeOperations int
	var beforeContext string
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id='sesn_reject_error'),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id='sesn_reject_error'),
		(SELECT `+sessionfixture.MessageContentSQL+` FROM session_messages m WHERE workspace_id='default' AND session_id='sesn_reject_error'
		  AND model_request_id='mreq_reject_error')`).Scan(&beforeEvents, &beforeOperations, &beforeContext); err != nil {
		t.Fatalf("read pre-rejection state: %v", err)
	}

	_, err = store.SettleToolResult(context.Background(), sessionfixture.BridgeToolSettlementRequestForTest(
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
		(SELECT `+sessionfixture.MessageContentSQL+` FROM session_messages m WHERE workspace_id='default' AND session_id='sesn_reject_error'
		  AND model_request_id='mreq_reject_error')`).Scan(&afterEvents, &afterOperations, &afterContext); err != nil {
		t.Fatalf("read post-rejection state: %v", err)
	}
	if beforeEvents != afterEvents || beforeOperations != afterOperations || beforeContext != afterContext {
		t.Fatalf("rejected Tool error mutated durable state")
	}
}

func TestPostgreSQLMultiToolOutOfOrderSettlementContext(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_multi_tool_cold"
		threadID  = "sthr_multi_tool_cold"
		bindingID = "bind_multi_tool_cold"
		podUID    = "pod_multi_tool_cold"
	)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	scope := sessionfixture.BridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_multi_start", "mreq_multi", runtimecontrol.RequestKindAgentProviderRequest, 0)
	client := startActorProductionBridge(t, runtimeDB)
	writeCall := func(writeID, callID, toolName string) *bridgev1.WriteEventResponse {
		t.Helper()
		response, err := client.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
			Scope: scope, RuntimeWriteId: writeID, ModelRequestId: "mreq_multi",
			ToolDeclaration: sessionfixture.BridgeToolDeclarationForTest(callID, toolName, `{}`, "allow", "sandbox_execute"),
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
		if err := json.Unmarshal([]byte(loaded.GetContextJson()), &payload); err != nil || len(payload.Messages) != 1 {
			t.Fatalf("decode multi-Tool context: entries=%#v err=%v", payload.Messages, err)
		}
		calls := make([]string, 0, 2)
		results := make([]string, 0, 2)
		for _, raw := range payload.Messages[0].Parts {
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
	if settled, err := client.SettleToolResult(context.Background(), sessionfixture.BridgeToolSettlementRequestForTest(
		scope, sessionfixture.BridgeCompletedToolSettlementForTest(callB.GetCommitted().GetEventId(), "B complete"),
	)); err != nil || settled.GetCommitted() == nil {
		t.Fatalf("settle B first: response=%#v err=%v", settled, err)
	}
	_, results, _ = loadParts()
	if !reflect.DeepEqual(results, []string{"call_multi_b"}) {
		t.Fatalf("out-of-order intermediate results = %v; want only B", results)
	}
	if settled, err := client.SettleToolResult(context.Background(), sessionfixture.BridgeToolSettlementRequestForTest(
		scope, sessionfixture.BridgeCompletedToolSettlementForTest(callA.GetCommitted().GetEventId(), "A complete"),
	)); err != nil || settled.GetCommitted() == nil {
		t.Fatalf("settle A second: response=%#v err=%v", settled, err)
	}
	_, results, finalContextJSON := loadParts()
	if !reflect.DeepEqual(results, []string{"call_multi_b", "call_multi_a"}) {
		t.Fatalf("durable out-of-order results = %v; want B then A", results)
	}
	pinBridgeContext(t, "multi-tool-settled-out-of-order", finalContextJSON)
}
