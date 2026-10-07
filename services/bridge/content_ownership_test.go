package agentruntimebridge

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// Each malformed envelope starts with the same legal Bridge-generated state.
// Corruption is confined to the returned fixture data; the original database
// remains an independent oracle before and after Runtime reconstruction.
func TestPostgreSQLRuntimeContentOwnershipValidation(t *testing.T) {
	for _, variant := range []string{"valid", "missing-message", "missing-call", "wrong-request-pair", "competing-route-owners", "sealed-without-route"} {
		t.Run(variant, func(t *testing.T) {
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
			before := f.snapshot(t)
			loaded, err := f.store.LoadContext(f.ctx, &bridgev1.LoadContextRequest{Scope: f.scope})
			if err != nil {
				t.Fatal(err)
			}
			var wire map[string]any
			if err := json.Unmarshal([]byte(loaded.GetContextJson()), &wire); err != nil {
				t.Fatal(err)
			}
			switch variant {
			case "missing-message":
				wire["messages"] = []any{}
			case "missing-call":
				message := wire["messages"].([]any)[0].(map[string]any)
				var retained []any
				for _, part := range message["parts"].([]any) {
					if part.(map[string]any)["type"] != "tool_call" {
						retained = append(retained, part)
					}
				}
				message["parts"] = retained
			case "wrong-request-pair":
				wire["currentRequestMessage"].(map[string]any)["modelRequestId"] = "other-request"
			case "competing-route-owners":
				wire["pendingSandboxExecutions"] = []any{map[string]any{"toolUseEventId": tool.GetCommitted().GetEventId(), "modelRequestId": "request", "modelToolCallId": "call-ownership", "toolName": "Read", "input": map[string]any{"file_path": "fixture-sensitive-content"}, "executionState": "running"}}
			case "sealed-without-route":
				wire["pendingToolUses"] = []any{}
			}
			corrupted, err := json.Marshal(wire)
			if err != nil {
				t.Fatal(err)
			}
			output := runContentOwnershipPreload(t, string(corrupted))
			if strings.Contains(string(output), "fixture-sensitive-content") || strings.Contains(string(output), "fixture-secret-sentinel") {
				t.Fatal("Runtime ownership diagnostic disclosed fixture content")
			}
			var result struct {
				Preload struct {
					OK     bool   `json:"ok"`
					Reason string `json:"reason"`
				} `json:"preload"`
				Observed *bool          `json:"observed"`
				Calls    map[string]int `json:"calls"`
			}
			if err := json.Unmarshal(output, &result); err != nil {
				t.Fatalf("invalid ownership observation: %v", err)
			}
			if len(result.Calls) != 3 || result.Observed == nil {
				t.Fatal("ownership counters or residency unavailable")
			}
			for _, key := range []string{"inputs", "tools", "provider"} {
				if _, ok := result.Calls[key]; !ok {
					t.Fatal("required ownership counter absent")
				}
			}
			if result.Calls["inputs"] != 0 || result.Calls["tools"] != 0 || result.Calls["provider"] != 0 {
				t.Fatal("record reconstruction dispatched work")
			}
			if variant == "valid" {
				if !result.Preload.OK || !*result.Observed {
					t.Fatalf("valid cold ownership rejected: %s", output)
				}
			} else if result.Preload.OK || *result.Observed || result.Preload.Reason == "" {
				t.Fatalf("invalid cold ownership was installed: %s", output)
			}
			if f.snapshot(t) != before {
				t.Fatal("ownership validation mutated durable content/Events/receipts")
			}
			t.Logf("ownership variant=%s observation=%s", variant, output)
		})
	}
}

func runContentOwnershipPreload(t *testing.T, contextJSON string) []byte {
	t.Helper()
	input, err := json.Marshal(map[string]any{"contextJson": contextJSON, "ownershipValidation": true})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "ownership.json")
	if err := os.WriteFile(path, input, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "bun", "packages/runtime-pod/test/fixtures/cold-checkpoint-composition.ts", path) //nolint:gosec // Fixed fixture and private input.
	command.Dir = "../agent-runtime"
	output, err := command.CombinedOutput()
	if err != nil {
		if strings.Contains(string(output), "fixture-sensitive-content") || strings.Contains(string(output), "fixture-secret-sentinel") {
			t.Fatal("failed ownership fixture disclosed content")
		}
		t.Fatalf("actual ownership preload failed: %v: %s", err, output)
	}
	if len(output) > 4096 {
		t.Fatal("ownership diagnostic exceeded bounded envelope")
	}
	return output
}
