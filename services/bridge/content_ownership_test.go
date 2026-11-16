package agentruntimebridge

import (
	"testing"

	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// The Runtime's tests corrupt this legal envelope one ownership fact at a time;
// its reasoning, signature and input carry sentinels its diagnostics must not
// disclose.
func TestPostgreSQLContentOwnershipEnvelope(t *testing.T) {
	f := newContentDeclarationFixture(t)
	f.store.RuntimeBindingTokenHMACKey = []byte("content-ownership-test-binding-key")
	if _, err := f.store.WriteEvent(f.ctx, &bridgev1.WriteEventRequest{Scope: f.scope, RuntimeWriteId: "running", EventType: "session.status_running", PayloadJson: `{"type":"session.status_running"}`}); err != nil {
		t.Fatal(err)
	}
	f.start(t)
	request := f.request("agent.message", "text", "evt_00000000000000000000000000000031")
	request.AssistantContextDelta.Parts[0].GetReasoning().Text = "fixture-sensitive-content"
	signature := `{"anthropic":{"signature":"fixture-secret-sentinel"}}`
	request.AssistantContextDelta.Parts[0].GetReasoning().ProviderMetadataJson = &signature
	text, err := f.store.WriteEvent(f.ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	declaration := sessionfixture.BridgeToolDeclarationForTest("call-ownership", "Read", `{"file_path":"fixture-sensitive-content"}`, "ask", "sandbox_execute")
	tool, err := f.store.WriteEvent(f.ctx, &bridgev1.WriteEventRequest{Scope: f.scope, RuntimeWriteId: "tool", ModelRequestId: "request", ToolDeclaration: declaration})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.WriteRequestEnd(f.ctx, &bridgev1.WriteRequestEndRequest{Scope: f.scope, RuntimeWriteId: "end", ModelRequestId: "request", FinishReason: "tool_calls", UsageJson: `{}`, ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "completed", AssistantMessageSequence: text.GetCommitted().AssignedMessageSequence, ToolUseEventIds: []string{tool.GetCommitted().GetEventId()}}}); err != nil {
		t.Fatal(err)
	}
	loaded, err := f.store.LoadContext(f.ctx, &bridgev1.LoadContextRequest{Scope: f.scope})
	if err != nil {
		t.Fatal(err)
	}
	pinBridgeContext(t, "ownership-pending-ask-tool", loaded.GetContextJson())
}
