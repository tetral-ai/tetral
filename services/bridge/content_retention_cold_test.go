package agentruntimebridge

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"
)

// The historical User is fixed setup. Both Assistants, Tool facts, accepted
// executions, terminal outcomes and context eligibility use their real owners.
func TestPostgreSQLAbnormalContentRetentionCold(t *testing.T) {
	for _, variant := range []string{"end-then-last-result", "reload-pending", "committed-result-ack-loss"} {
		t.Run(variant, func(t *testing.T) {
			f := newContentDeclarationFixture(t)
			f.store.RuntimeBindingTokenHMACKey = []byte("content-retention-cold-test-key")
			seedBridgeAPIProjectedUserMessage(t, f.admin, f.scope.SessionId, f.scope.SessionThreadId, "prior-message", "prior-event", 1)
			if _, err := f.admin.Exec(`UPDATE session_messages SET data_json='{"parts":[{"type":"text","text":"prior-user"}]}' WHERE session_id=$1 AND sequence=1`, f.scope.SessionId); err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.WriteEvent(f.ctx, &bridgev1.WriteEventRequest{Scope: f.scope, RuntimeWriteId: "running", EventType: "session.status_running", PayloadJson: `{"type":"session.status_running"}`}); err != nil {
				t.Fatal(err)
			}
			seedBridgeAPIRequestStart(t, f.store, f.scope, "prior-start", "prior", runtimecontrol.RequestKindAgentProviderRequest, 1)
			priorSequence := contentRetentionText(t, f, "prior", "prior-answer", "evt_00000000000000000000000000000021")
			if priorSequence != 2 {
				t.Fatalf("prior Assistant sequence=%d", priorSequence)
			}
			if _, err := f.store.WriteRequestEnd(f.ctx, &bridgev1.WriteRequestEndRequest{Scope: f.scope, RuntimeWriteId: "prior-end", ModelRequestId: "prior", FinishReason: "stop", UsageJson: `{}`, ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "completed", AssistantMessageSequence: &priorSequence}}); err != nil {
				t.Fatal(err)
			}
			seedBridgeAPIRequestStart(t, f.store, f.scope, "start", "request", runtimecontrol.RequestKindAgentProviderRequest, 2)
			sequence := contentRetentionText(t, f, "request", "alpha", "evt_00000000000000000000000000000022")
			if sequence != 3 {
				t.Fatalf("current Assistant sequence=%d", sequence)
			}
			seedReadySandboxForSharedToolExecution(t, f.admin, f.scope.WorkspaceId, f.scope.SessionId)
			sibling := contentRetentionTool(t, f, "call-sibling", "/workspace/sibling.txt", "unrelated-sibling-reason", "sibling-signature")
			contentRetentionExecute(t, f, sibling, "sibling-result")
			if _, err := f.store.SettleToolResult(f.ctx, bridgeToolSettlementRequestForTest(f.scope, bridgeCompletedToolSettlementForTest(sibling, "sibling-result"))); err != nil {
				t.Fatal(err)
			}
			selected := contentRetentionTool(t, f, "call-read-note", "/workspace/note.txt", "reason-before-tool", "fixture-signature-tool")
			if _, err := f.store.AcceptSandboxExecution(f.ctx, &bridgev1.AcceptSandboxExecutionRequest{Scope: f.scope, ToolUseEventId: selected}); err != nil {
				t.Fatal(err)
			}
			contentRetentionText(t, f, "request", "unrelated-after", "evt_00000000000000000000000000000023")
			if _, err := f.store.WriteRequestEnd(f.ctx, &bridgev1.WriteRequestEndRequest{Scope: f.scope, RuntimeWriteId: "failed-end", ModelRequestId: "request", IsError: true, ErrorKind: "provider_error", FinishReason: "error", UsageJson: `{}`, ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "failed", AssistantMessageSequence: &sequence, ToolUseEventIds: []string{selected}}}); err != nil {
				t.Fatal(err)
			}
			const prior = `[{"messageSequence":1,"contextKind":"user","parts":[{"type":"text","text":"prior-user"}]},{"messageSequence":2,"contextKind":"assistant","parts":[{"type":"text","text":"prior-answer"}]}]`
			pending := contentRetentionLoad(t, f)
			assertContentLiteralJSON(t, pending.ColdProductionEntries, prior)
			if pending.NextStep.Action != "resume_tool_routes" || !reflect.DeepEqual(pending.NextStep.ToolUseEventIDs, []string{selected}) || len(pending.ToolRouteView.Routes) != 1 || pending.ToolRouteView.Routes[0].ToolUseEventID != selected || pending.ToolRouteView.Routes[0].Disposition != "resume_sandbox_execution" {
				t.Fatalf("pending accepted route=%+v/%+v", pending.NextStep, pending.ToolRouteView)
			}
			assertContentLiteralJSON(t, pending.ColdProductionCurrentRequestMessage, `{"modelRequestId":"request","assistantMessageSequence":3}`)
			var references []struct {
				ToolUseEventID           string `json:"toolUseEventId"`
				ModelRequestID           string `json:"modelRequestId"`
				ModelToolCallID          string `json:"modelToolCallId"`
				AssistantMessageSequence int64  `json:"assistantMessageSequence"`
				Disposition              string `json:"disposition"`
			}
			if json.Unmarshal(pending.ColdProductionActiveToolReferences, &references) != nil || len(references) != 1 || references[0].ToolUseEventID != selected || references[0].ModelRequestID != "request" || references[0].ModelToolCallID != "call-read-note" || references[0].AssistantMessageSequence != 3 || references[0].Disposition != "resume_sandbox_execution" {
				t.Fatalf("installed Tool references=%s", pending.ColdProductionActiveToolReferences)
			}
			if variant == "reload-pending" {
				again := contentRetentionLoad(t, f)
				assertContentLiteralJSON(t, again.ColdProductionEntries, prior)
				if !reflect.DeepEqual(again.ToolRouteView, pending.ToolRouteView) {
					t.Fatal("fresh residency changed pending routes")
				}
			}
			// Only the selected accepted execution remains queued. This runs its actual
			// Sandbox worker; the external adapter supplies a fixed Read completion.
			contentRetentionRunQueued(t, f, "fixture-note")
			var terminal int
			if err := f.admin.QueryRow(`SELECT count(*) FROM session_runtime_tool_results WHERE session_id=$1 AND tool_use_event_id=$2 AND execution_state='terminal_unconsumed'`, f.scope.SessionId, selected).Scan(&terminal); err != nil || terminal != 1 {
				t.Fatalf("selected terminal-unconsumed=%d/%v", terminal, err)
			}
			terminalLoad := contentRetentionLoad(t, f)
			assertContentLiteralJSON(t, terminalLoad.ColdProductionEntries, prior)
			if !reflect.DeepEqual(terminalLoad.ToolRouteView, pending.ToolRouteView) {
				t.Fatal("unconsumed terminal result changed the recovery route")
			}
			request := bridgeToolSettlementRequestForTest(f.scope, bridgeCompletedToolSettlementForTest(selected, "fixture-note"))
			if variant == "committed-result-ack-loss" {
				fault := &contentSettlementResponseFault{BridgeAPIServer: NewBridgeAPIServer(f.store), lose: true}
				client, _ := startSandboxProductionBoundaryBridgeClient(t, fault, f.scope.Binding.TargetPodUid)
				ctx := metadata.AppendToOutgoingContext(f.ctx, "authorization", "Bearer sandbox-production-runtime-token")
				response, err := client.SettleToolResult(ctx, request)
				if status.Code(err) != codes.Unavailable || response != nil || !fault.committed() {
					t.Fatalf("real committed result ACK loss=%v/%v", response, err)
				}
			} else if response, err := f.store.SettleToolResult(f.ctx, request); err != nil || response.GetCommitted() == nil {
				t.Fatalf("last result=%v/%v", response, err)
			}
			// A fresh Runtime process installs only facts loaded after the true commit;
			// no hot ACK or speculative result is supplied to it.
			final := contentRetentionLoad(t, f)
			const retained = `[{"messageSequence":1,"contextKind":"user","parts":[{"type":"text","text":"prior-user"}]},{"messageSequence":2,"contextKind":"assistant","parts":[{"type":"text","text":"prior-answer"}]},{"messageSequence":3,"contextKind":"assistant","parts":[{"type":"reasoning","text":"reason-before-tool","providerMetadata":{"anthropic":{"signature":"fixture-signature-tool"}}},{"type":"tool_call","modelToolCallId":"call-read-note","toolName":"Read","canonicalInput":{"file_path":"/workspace/note.txt"}},{"type":"tool_result","modelToolCallId":"call-read-note","result":{"type":"completed","output":{"text":"fixture-note"}}}]}]`
			assertContentLiteralJSON(t, final.ColdProductionEntries, retained)
			if len(final.ToolRouteView.Routes) != 0 {
				t.Fatalf("settled Tool route retained=%+v", final.ToolRouteView)
			}
			assertContentLiteralJSON(t, final.ColdProductionActiveToolReferences, `[]`)
			var uses, results int
			if err := f.admin.QueryRow(`SELECT count(*) FILTER(WHERE type='agent.tool_use'),count(*) FILTER(WHERE type='agent.tool_result') FROM session_events WHERE session_id=$1`, f.scope.SessionId).Scan(&uses, &results); err != nil || uses != 2 || results != 2 {
				t.Fatalf("audit sibling/selected events=%d/%d/%v", uses, results, err)
			}
			before := f.snapshot(t)
			duplicate, err := f.store.SettleToolResult(f.ctx, request)
			if err != nil || duplicate.GetDuplicate() == nil || f.snapshot(t) != before {
				t.Fatal("post-reload settlement replay altered durable identity")
			}
		})
	}
}

func contentRetentionText(t *testing.T, f contentDeclarationFixture, request, text, event string) int64 {
	t.Helper()
	payload, _ := json.Marshal(map[string]any{"type": "agent.message", "content": []any{map[string]any{"type": "text", "text": text}}})
	response, err := f.store.WriteEvent(f.ctx, &bridgev1.WriteEventRequest{Scope: f.scope, RuntimeWriteId: text, ModelRequestId: request, EventType: "agent.message", PayloadJson: string(payload), PreallocatedEventId: &event, AssistantContextDelta: &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_Text{Text: &bridgev1.RuntimeContextText{Text: text}}}}}})
	if err != nil || response.GetCommitted() == nil {
		t.Fatalf("write literal text=%v/%v", response, err)
	}
	return response.GetCommitted().GetAssignedMessageSequence()
}
func contentRetentionTool(t *testing.T, f contentDeclarationFixture, call, path, reason, signature string) string {
	t.Helper()
	input, _ := json.Marshal(map[string]string{"file_path": path})
	declaration := bridgeSignedReasoningToolDeclarationForTest(call, "Read", string(input), "allow")
	declaration.LeadingReasoning[0].Text = reason
	metadata, _ := json.Marshal(map[string]any{"anthropic": map[string]string{"signature": signature}})
	declaration.LeadingReasoning[0].ProviderMetadataJson = bridgeString(string(metadata))
	response, err := f.store.WriteEvent(f.ctx, &bridgev1.WriteEventRequest{Scope: f.scope, RuntimeWriteId: call, ModelRequestId: "request", ToolDeclaration: declaration})
	if err != nil || response.GetCommitted() == nil {
		t.Fatalf("write literal Tool=%v/%v", response, err)
	}
	return response.GetCommitted().GetEventId()
}
func contentRetentionExecute(t *testing.T, f contentDeclarationFixture, tool, result string) {
	t.Helper()
	if response, err := f.store.AcceptSandboxExecution(f.ctx, &bridgev1.AcceptSandboxExecutionRequest{Scope: f.scope, ToolUseEventId: tool}); err != nil || response.GetCommitted() == nil {
		t.Fatalf("accept actual Tool=%v/%v", response, err)
	}
	contentRetentionRunQueued(t, f, result)
}
func contentRetentionRunQueued(t *testing.T, f contentDeclarationFixture, result string) {
	t.Helper()
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(f.workload.OpenWorkload(t, "queue", nil)))
	connection := startBackgroundNotificationQueueServer(t, queueStore)
	raw, _ := json.Marshal(map[string]any{"status": "success", "result": map[string]string{"content": result}})
	provider := &hotReceiptSandboxProvider{bridgeMemoryProjectionProvider: &bridgeMemoryProjectionProvider{}, resultJSON: string(raw)}
	registry, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{sandboxdriver.DaytonaProviderName: provider})
	if err != nil {
		t.Fatal(err)
	}
	runner := &tetralsandbox.SandboxToolExecutionJobRunner{Queue: tetralsandbox.SandboxQueueFromGRPC(queuev1.NewQueueServiceClient(connection)), Coordinator: tetralsandbox.NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(f.workload.OpenWorkload(t, "sandbox", nil)), 30*time.Minute), Providers: registry, Media: backgroundNotificationMedia{}, Config: tetralsandbox.SandboxToolExecutionRunnerConfig{WorkspaceID: f.scope.WorkspaceId, LeaseOwner: "content-retention", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Second, PreparationTimeout: time.Second}}
	active, err := runner.RunOnceWithActivity(f.ctx)
	if err != nil || !active {
		t.Fatalf("real queued Sandbox completion=%v/%v", active, err)
	}
}
func contentRetentionLoad(t *testing.T, f contentDeclarationFixture) runtimeColdContextComposition {
	t.Helper()
	before := f.snapshot(t)
	loaded, err := f.store.LoadContext(f.ctx, &bridgev1.LoadContextRequest{Scope: f.scope})
	if err != nil {
		t.Fatal(err)
	}
	restored := runRuntimeColdContextComposition(t, loaded.GetContextJson(), false)
	if f.snapshot(t) != before {
		t.Fatal("cold preload mutated durable facts")
	}
	return restored
}
func assertContentLiteralJSON(t *testing.T, raw json.RawMessage, literal string) {
	t.Helper()
	var actual, want any
	if json.Unmarshal(raw, &actual) != nil || json.Unmarshal([]byte(literal), &want) != nil || !reflect.DeepEqual(actual, want) {
		t.Fatalf("literal content differs: actual=%s want=%s", raw, literal)
	}
}
