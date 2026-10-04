package agentruntimebridge

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/sessionevent"

	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// This checks the real SQL/role/transaction boundary independently of the
// shared pure selection truth table. Runtime normalization is checked by the
// Runtime composition tests, not inferred from this transport projection.
func TestPostgreSQLContentColdContext(t *testing.T) {
	t.Run("authoritative-installed-builtin-policy", func(t *testing.T) {
		raw, err := os.ReadFile("../agent-runtime/packages/protocol/testdata/installed-builtin-policy.json")
		if err != nil {
			t.Fatal(err)
		}
		var cases []struct {
			Name           string           `json:"name"`
			InstalledTools []map[string]any `json:"installedTools"`
		}
		if err := json.Unmarshal(raw, &cases); err != nil || len(cases) != 2 {
			t.Fatalf("installed policy literal: %v", err)
		}
		for _, vector := range cases {
			t.Run(vector.Name, func(t *testing.T) {
				f := newContentDeclarationFixture(t)
				f.store.RuntimeBindingTokenHMACKey = []byte("content-cold-context-test-signing-key")
				// The stale agent declaration deliberately disagrees. The installed
				// snapshot owns the cold family, defaults and per-tool overrides.
				seedBridgeAPIAgentConfig(t, f.admin, "default", f.scope.SessionId, `{"name":"agent","model":"anthropic/claude-opus-4-8","tools":[{"type":"tetral_agent_toolset","family":"gpt","default_config":{"permission_policy":{"type":"always_allow"}}}],"skills":[],"metadata":{}}`)
				installed, err := json.Marshal(map[string]any{"tools": vector.InstalledTools, "mcp_servers": []any{}})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.admin.Exec(`UPDATE sessions SET installed_tools_json=$1,approval_mode='ask_for_approval' WHERE workspace_id='default' AND id=$2`, string(installed), f.scope.SessionId); err != nil {
					t.Fatal(err)
				}
				loaded, err := f.store.LoadContext(f.ctx, &bridgev1.LoadContextRequest{Scope: f.scope})
				if err != nil {
					t.Fatal(err)
				}
				var payload struct {
					RuntimeConfig struct {
						InstalledTools []map[string]any `json:"installedTools"`
						ApprovalMode   string           `json:"approvalMode"`
					} `json:"runtimeConfig"`
				}
				if err := json.Unmarshal([]byte(loaded.GetContextJson()), &payload); err != nil {
					t.Fatal(err)
				}
				if payload.RuntimeConfig.ApprovalMode != "ask_for_approval" || !reflect.DeepEqual(payload.RuntimeConfig.InstalledTools, vector.InstalledTools) {
					t.Fatalf("actual cold installed policy differs from authoritative literal: mode=%s installed=%v", payload.RuntimeConfig.ApprovalMode, payload.RuntimeConfig.InstalledTools)
				}
			})
		}
	})
	runContentColdContextVariants(t, false)
}

func TestPostgreSQLRuntimeContextRecovery(t *testing.T) {
	runContentColdContextVariants(t, true)
}

func runContentColdContextVariants(t *testing.T, compose bool) {
	variants := []string{"open", "success-end-pending-tool", "abnormal-end-retained-pending-tool", "closed", "exhausted", "latest-no-content", "requires-action", "requires-action-reopened", "terminated-open", "terminated-after-end"}
	if compose {
		variants = []string{"open", "success-end-pending-tool", "abnormal-end-retained-pending-tool", "closed", "exhausted", "latest-no-content", "no-content-end", "decided-allow", "decided-deny"}
	}
	for _, variant := range variants {
		t.Run(variant, func(t *testing.T) {
			f := newContentDeclarationFixture(t)
			f.store.RuntimeBindingTokenHMACKey = []byte("content-cold-context-test-signing-key")
			lifecycle := func(writeID, eventType, payload string) string {
				t.Helper()
				response, err := f.store.WriteEvent(f.ctx, &bridgev1.WriteEventRequest{Scope: f.scope, RuntimeWriteId: writeID, EventType: eventType, PayloadJson: payload})
				if err != nil {
					t.Fatal(err)
				}
				return response.GetCommitted().GetEventId()
			}
			durableTurnID := lifecycle("running", "session.status_running", `{"type":"session.status_running"}`)
			var exhaustedFailureID string
			f.start(t)
			var sequence int64
			var pendingToolID string
			if variant != "no-content-end" {
				written, err := f.store.WriteEvent(f.ctx, f.request("agent.message", "alpha", "evt_00000000000000000000000000000001"))
				if err != nil {
					t.Fatal(err)
				}
				sequence = written.GetCommitted().GetAssignedMessageSequence()
			}
			switch variant {
			case "no-content-end":
				f.end(t, nil)
			case "success-end-pending-tool", "abnormal-end-retained-pending-tool", "decided-allow", "decided-deny":
				tool, err := f.store.WriteEvent(f.ctx, &bridgev1.WriteEventRequest{Scope: f.scope, RuntimeWriteId: "tool", ModelRequestId: "request", ToolDeclaration: bridgeToolDeclarationForTest("call-a", "Read", `{"path":"a"}`, "ask", "sandbox_execute")})
				if err != nil {
					t.Fatal(err)
				}
				pendingToolID = tool.GetCommitted().GetEventId()
				end := &bridgev1.WriteRequestEndRequest{Scope: f.scope, RuntimeWriteId: "end", ModelRequestId: "request", FinishReason: "tool_calls", UsageJson: `{}`, ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "completed", AssistantMessageSequence: &sequence, ToolUseEventIds: []string{tool.GetCommitted().GetEventId()}}}
				if variant == "abnormal-end-retained-pending-tool" {
					end.IsError = true
					end.ErrorKind = "provider_error"
					end.FinishReason = "error"
					end.ProviderContextRetention.Disposition = "failed"
				}
				if _, err := f.store.WriteRequestEnd(f.ctx, end); err != nil {
					t.Fatal(err)
				}
			case "terminated-open", "terminated-after-end":
				if variant == "terminated-after-end" {
					f.end(t, &sequence)
				}
				terminated, err := f.store.CommitRuntimeTermination(f.ctx, &bridgev1.CommitRuntimeTerminationRequest{
					Scope: f.scope, RuntimeWriteId: durableTurnID,
					FailureJson: `{"type":"runtime","code":"runtime_invalid_sequence","message":"Runtime operation failed.","retryable":false,"fatal":true,"retryStatus":{"type":"terminal"},"reason":"runtime_contract_validation"}`,
				})
				if err != nil || terminated.GetCommitted() == nil {
					t.Fatalf("actual Runtime termination = %v/%v", terminated, err)
				}
			case "closed", "exhausted", "latest-no-content", "requires-action", "requires-action-reopened":
				f.end(t, &sequence)
				// The capture dependency is explicitly test-staged here. FinishIdle
				// itself performs the real closeout and status transaction; this
				// focused query test makes no output-capture lifecycle claim.
				stopReason := `{"type":"end_turn"}`
				if variant == "requires-action" || variant == "requires-action-reopened" {
					stopReason = `{"type":"requires_action"}`
				}
				if variant == "exhausted" {
					exhaustedFailureID = lifecycle("exhausted-error", "session.error", `{"type":"session.error","error":{"type":"unknown_error","message":"The request exhausted its retries.","retry_status":{"type":"exhausted"}}}`)
					stopReason = `{"type":"retries_exhausted"}`
				}
				idle, err := finishIdleWithStagedCaptureForTest(t, f.admin, f.store, &bridgev1.FinishIdleRequest{
					Scope: f.scope, DurableTurnId: durableTurnID, StopReasonJson: stopReason,
				})
				if err != nil || idle.GetCommitted() == nil {
					t.Fatalf("actual FinishIdle = %v/%v", idle, err)
				}
				if variant == "requires-action-reopened" {
					lifecycle("running-reopened", "session.status_running", `{"type":"session.status_running"}`)
				}
				if variant == "latest-no-content" {
					lifecycle("running-again", "session.status_running", `{"type":"session.status_running"}`)
					if _, err := f.store.WriteEvent(f.ctx, &bridgev1.WriteEventRequest{Scope: f.scope, RuntimeWriteId: "new-start", ModelRequestId: "request-b", EventType: "span.model_request_start", PayloadJson: `{"type":"span.model_request_start"}`, RequestKind: "agent_provider_request", ContextThroughMessageSequence: &sequence}); err != nil {
						t.Fatal(err)
					}
				}
			}
			if variant == "decided-allow" || variant == "decided-deny" {
				decision := sessionevent.ToolConfirmationResultAllow
				if variant == "decided-deny" {
					decision = sessionevent.ToolConfirmationResultDeny
				}
				api := f.workload.OpenWorkload(t, "api", nil)
				born, err := sessionevent.NewService(sessionevent.NewPostgreSQLStore(dbconnect.NewClientForTesting(api))).AppendClientEvents(f.ctx, "default", f.scope.SessionId, "cold-decision", sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{Type: sessionevent.EventTypeUserToolConfirmation, ToolUseID: pendingToolID, Result: decision}}})
				if err != nil || len(born.Data) != 1 {
					t.Fatalf("owning public confirmation=%v/%v", born, err)
				}
			}
			before := f.snapshot(t)
			loaded, err := f.store.LoadContext(f.ctx, &bridgev1.LoadContextRequest{Scope: f.scope})
			if variant == "terminated-open" || variant == "terminated-after-end" {
				// Public cold admission remains fenced for a terminal Session.
				// Exercise the actual scoped projection query separately, under
				// the restricted Bridge role, without weakening that RPC fence.
				if status.Code(err) != codes.FailedPrecondition || loaded != nil {
					t.Fatalf("terminal LoadContext admission = %v/%v", loaded, err)
				}
				var contextJSON string
				err = f.store.withScopeReadOnlyTx(f.ctx, f.scope, "agentruntimebridge.content_terminal_projection", func(tx *dbconnect.Tx) error {
					var err error
					contextJSON, err = loadThreadContextJSONTx(f.ctx, tx, f.scope, f.store.ProviderRescheduleBudget, f.store.CompactionRescheduleBudget)
					return err
				})
				loaded = &bridgev1.LoadContextResponse{ContextJson: contextJSON}
			}
			if err != nil {
				t.Fatal(err)
			}
			var wire map[string]json.RawMessage
			if err := json.Unmarshal([]byte(loaded.GetContextJson()), &wire); err != nil {
				t.Fatal(err)
			}
			if _, ok := wire["messages"]; !ok {
				t.Fatal("messages absent")
			}
			if _, ok := wire["currentRequestMessage"]; !ok {
				t.Fatal("current request reference absent")
			}
			for _, retired := range []string{"contextEntries", "openRequestDraft"} {
				if _, ok := wire[retired]; ok {
					t.Fatalf("retired envelope field %s remains", retired)
				}
			}
			var payload bridgeLoadContextPayload
			if err := json.Unmarshal([]byte(loaded.GetContextJson()), &payload); err != nil {
				t.Fatal(err)
			}
			wantMessages := 1
			if variant == "terminated-open" || variant == "no-content-end" {
				wantMessages = 0 // Abnormal unretained Assistant remains audit-only.
			}
			if len(payload.Messages) != wantMessages || (wantMessages > 0 && (payload.Messages[0].MessageSequence != sequence || payload.Messages[0].ContextKind != "assistant")) {
				t.Fatalf("durable Messages = %#v", payload.Messages)
			}
			var openTurnID *string
			if err := f.store.withScopeTx(f.ctx, f.scope, "agentruntimebridge.content_cold_diagnostic", func(tx *dbconnect.Tx) error {
				var err error
				openTurnID, err = runtimecontrol.LoadOpenDurableTurnIDTx(f.ctx, tx, f.scope)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			facts, _ := json.Marshal(payload.TurnFacts)
			openTurnJSON, _ := json.Marshal(openTurnID)
			t.Logf("owning state threadStatus=%s durableTurnID=%s facts=%s", payload.Thread.Status, openTurnJSON, facts)
			if (variant == "closed" || variant == "exhausted") && (payload.Thread.Status != "idle" || openTurnID != nil) {
				t.Fatalf("FinishIdle did not close durable state: status=%s open=%v", payload.Thread.Status, openTurnID)
			}
			if variant == "closed" || variant == "exhausted" || variant == "latest-no-content" || variant == "terminated-open" || variant == "terminated-after-end" || variant == "no-content-end" {
				if payload.CurrentRequestMessage != nil {
					t.Fatalf("historic Assistant selected = %#v", payload.CurrentRequestMessage)
				}
			} else if payload.CurrentRequestMessage == nil || payload.CurrentRequestMessage.ModelRequestID != "request" || payload.CurrentRequestMessage.AssistantMessageSequence != sequence {
				t.Fatalf("current Assistant = %#v", payload.CurrentRequestMessage)
			}
			if variant == "success-end-pending-tool" || variant == "abnormal-end-retained-pending-tool" {
				if len(payload.PendingToolUses) != 1 {
					t.Fatalf("pending Tool custody = %#v", payload.PendingToolUses)
				}
			}
			if variant == "terminated-open" || variant == "terminated-after-end" {
				var hasFailure, hasCloseout bool
				for _, event := range payload.TurnFacts.Events {
					hasFailure = hasFailure || event.Failure != nil
					hasCloseout = hasCloseout || event.Type == "session.status_terminated"
				}
				if openTurnID != nil || !hasFailure || !hasCloseout {
					t.Fatal("terminal closeout or exact paired failure absent from cold facts")
				}
			}
			if variant == "exhausted" {
				var failureIDs []string
				for _, event := range payload.TurnFacts.Events {
					if event.Failure != nil && event.Failure.RetryStatus == "exhausted" {
						failureIDs = append(failureIDs, event.EventID)
					}
				}
				if !reflect.DeepEqual(failureIDs, []string{exhaustedFailureID}) {
					t.Fatalf("exhausted closeout lost its exact failure: %v", failureIDs)
				}
			}
			if compose {
				restored := runRuntimeColdContextComposition(t, loaded.GetContextJson(), false)
				wantAction := map[string]string{"open": "await_request_end", "latest-no-content": "await_request_end", "closed": "await_input", "exhausted": "await_input", "success-end-pending-tool": "finish_idle", "abnormal-end-retained-pending-tool": "finish_idle", "no-content-end": "finish_idle", "decided-allow": "resume_tool_routes", "decided-deny": "resume_tool_routes"}[variant]
				if restored.NextStep.Action != wantAction {
					t.Fatalf("restored nextStep=%s want%s", restored.NextStep.Action, wantAction)
				}
				if pendingToolID != "" {
					disposition := "requires_user_action"
					if variant == "decided-allow" || variant == "decided-deny" {
						disposition = "resume_approval_settlement"
					}
					literal, err := json.Marshal([]map[string]any{{"toolUseEventId": pendingToolID, "modelRequestId": "request", "modelToolCallId": "call-a", "assistantMessageSequence": sequence, "disposition": disposition}})
					if err != nil {
						t.Fatal(err)
					}
					assertContentLiteralJSON(t, restored.ColdProductionActiveToolReferences, string(literal))
				}
				if variant == "no-content-end" {
					assertContentLiteralJSON(t, restored.ColdProductionEntries, `[]`)
					assertContentLiteralJSON(t, restored.ColdProductionCurrentRequestMessage, `null`)
					if len(restored.ToolRouteView.Routes) != 0 {
						t.Fatal("empty End restored a Tool route")
					}
				}
				t.Logf("actual Runtime cold preload variant=%s nextStep=%s", variant, restored.NextStep.Action)
			}
			if f.snapshot(t) != before {
				t.Fatal("cold loading changed content/Events/receipts")
			}
		})
	}
}
