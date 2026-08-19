package agentruntimebridge

import (
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestRuntimeContextDeltaAcceptsOnlyNarrowProviderParts(t *testing.T) {
	delta := &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{
		{Content: &bridgev1.RuntimeContextPart_Text{Text: &bridgev1.RuntimeContextText{Text: "done"}}},
		{Content: &bridgev1.RuntimeContextPart_Reasoning{Reasoning: &bridgev1.RuntimeContextReasoning{Text: "why", ProviderMetadataJson: bridgeString(`{"provider":"x"}`)}}},
		{Content: &bridgev1.RuntimeContextPart_ToolCall{ToolCall: &bridgev1.RuntimeContextToolCall{ModelToolCallId: "call_1", ToolName: "read", ProviderInputJson: `{"path":"a"}`}}},
		{Content: &bridgev1.RuntimeContextPart_ToolResult{ToolResult: &bridgev1.RuntimeContextToolResult{ModelToolCallId: "call_1", Outcome: &bridgev1.RuntimeContextToolResult_Error{Error: &bridgev1.RuntimeContextToolError{ErrorJson: `{"type":"tool_failure","message":"safe","retryable":false}`}}}}},
	}}
	parts, err := canonicalRuntimeContextParts(delta)
	if err != nil {
		t.Fatalf("canonicalRuntimeContextParts: %v", err)
	}
	if got := []string{parts[0]["type"].(string), parts[1]["type"].(string), parts[2]["type"].(string), parts[3]["type"].(string)}; strings.Join(got, ",") != "text,reasoning,tool_call,tool_result" {
		t.Fatalf("part kinds = %v", got)
	}
	for index, part := range parts {
		for _, forbidden := range []string{"id", "messageId", "sequence", "createdAt", "updatedAt", "completedAt", "status", "origin"} {
			if _, exists := part[forbidden]; exists {
				t.Fatalf("part %d contains Bridge-owned/unused field %q: %#v", index, forbidden, part)
			}
		}
	}
}

func TestRuntimeContextDeltaRejectsUnknownAndOversizedValues(t *testing.T) {
	tests := []struct {
		name  string
		delta *bridgev1.RuntimeContextDelta
	}{
		{name: "missing part", delta: &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{nil}}},
		{name: "invalid input", delta: &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_ToolCall{ToolCall: &bridgev1.RuntimeContextToolCall{ModelToolCallId: "call", ToolName: "read", ProviderInputJson: `{`}}}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := canonicalRuntimeContextParts(test.delta); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("error = %v; want InvalidArgument", err)
			}
		})
	}
}

func TestRuntimeContextDeltaEnforcesExactJSONByteBounds(t *testing.T) {
	tests := []struct {
		name      string
		maxBytes  int
		emptyJSON string
		delta     func(string) *bridgev1.RuntimeContextDelta
	}{
		{
			name: "provider metadata", maxBytes: runtimeProviderMetadataMaxBytes, emptyJSON: `{"x":""}`,
			delta: func(raw string) *bridgev1.RuntimeContextDelta {
				return &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_Reasoning{Reasoning: &bridgev1.RuntimeContextReasoning{Text: "why", ProviderMetadataJson: &raw}}}}}
			},
		},
		{
			name: "Tool input", maxBytes: runtimeToolInputJSONMaxBytes, emptyJSON: `{"x":""}`,
			delta: func(raw string) *bridgev1.RuntimeContextDelta {
				return &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_ToolCall{ToolCall: &bridgev1.RuntimeContextToolCall{ModelToolCallId: "call", ToolName: "read", ProviderInputJson: raw}}}}}
			},
		},
		{
			name: "Tool output", maxBytes: runtimeToolOutputJSONMaxBytes, emptyJSON: `{"text":""}`,
			delta: func(raw string) *bridgev1.RuntimeContextDelta {
				return &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_ToolResult{ToolResult: &bridgev1.RuntimeContextToolResult{ModelToolCallId: "call", Outcome: &bridgev1.RuntimeContextToolResult_Completed{Completed: &bridgev1.RuntimeContextToolCompleted{OutputJson: raw}}}}}}}
			},
		},
		{
			name: "Tool error", maxBytes: runtimeToolOutputJSONMaxBytes, emptyJSON: `{"error":{"type":"tool_failure","message":"","retryable":false}}`,
			delta: func(raw string) *bridgev1.RuntimeContextDelta {
				errorJSON := strings.TrimPrefix(strings.TrimSuffix(raw, "}"), `{"error":`)
				return &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_ToolResult{ToolResult: &bridgev1.RuntimeContextToolResult{ModelToolCallId: "call", Outcome: &bridgev1.RuntimeContextToolResult_Error{Error: &bridgev1.RuntimeContextToolError{ErrorJson: errorJSON}}}}}}}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exact := strings.Replace(test.emptyJSON, `""`, `"`+strings.Repeat("x", test.maxBytes-len(test.emptyJSON))+`"`, 1)
			if len(exact) != test.maxBytes {
				t.Fatalf("exact JSON bytes = %d; want %d", len(exact), test.maxBytes)
			}
			if _, err := canonicalRuntimeContextParts(test.delta(exact)); err != nil {
				t.Fatalf("exact limit rejected: %v", err)
			}
			over := strings.Replace(test.emptyJSON, `""`, `"`+strings.Repeat("x", test.maxBytes-len(test.emptyJSON)+1)+`"`, 1)
			if _, err := canonicalRuntimeContextParts(test.delta(over)); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("one byte over error = %v; want InvalidArgument", err)
			}
		})
	}
}

func TestRuntimeContextTextAndIdentifiersMatchGatewayByteBounds(t *testing.T) {
	for _, test := range []struct {
		name  string
		delta func(string) *bridgev1.RuntimeContextDelta
	}{
		{
			name: "text",
			delta: func(value string) *bridgev1.RuntimeContextDelta {
				return &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_Text{Text: &bridgev1.RuntimeContextText{Text: value}}}}}
			},
		},
		{
			name: "reasoning",
			delta: func(value string) *bridgev1.RuntimeContextDelta {
				return &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_Reasoning{Reasoning: &bridgev1.RuntimeContextReasoning{Text: value}}}}}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			exact := strings.Repeat("x", runtimeContextTextJSONMaxBytes-2)
			if runtimeJSONBytes(exact) != runtimeContextTextJSONMaxBytes {
				t.Fatalf("exact JSON-string bytes = %d", runtimeJSONBytes(exact))
			}
			if _, err := canonicalRuntimeContextParts(test.delta(exact)); err != nil {
				t.Fatalf("exact text bound rejected: %v", err)
			}
			if _, err := canonicalRuntimeContextParts(test.delta(exact + "x")); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("one byte over error = %v; want InvalidArgument", err)
			}
		})
	}

	for _, size := range []int{runtimeContextIdentifierMaxBytes, runtimeContextIdentifierMaxBytes + 1} {
		identifier := strings.Repeat("i", size)
		delta := &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_ToolCall{ToolCall: &bridgev1.RuntimeContextToolCall{
			ModelToolCallId: identifier, ToolName: "Read", ProviderInputJson: `{}`,
		}}}}}
		_, err := canonicalRuntimeContextParts(delta)
		if size == runtimeContextIdentifierMaxBytes && err != nil {
			t.Fatalf("exact identifier bound rejected: %v", err)
		}
		if size > runtimeContextIdentifierMaxBytes && status.Code(err) != codes.InvalidArgument {
			t.Fatalf("oversized identifier error = %v; want InvalidArgument", err)
		}
	}
}

func TestStoredRuntimeContextAcceptsProviderIdentifiersWithoutSensitiveTextClassification(t *testing.T) {
	parts, err := decodeStoredRuntimeContextParts(`{"parts":[{"type":"tool_call","modelToolCallId":"dummy-call-1","toolName":"Read","canonicalInput":{}},{"type":"tool_result","modelToolCallId":"dummy-call-1","result":{"type":"completed","output":{"text":"ok"}}}]}`)
	if err != nil {
		t.Fatalf("decode durable provider identifiers: %v", err)
	}
	if len(parts) != 2 {
		t.Fatalf("parts = %d; want 2", len(parts))
	}
}

func TestRuntimeContextDeltaRejectsUnpairedUnicodeSurrogates(t *testing.T) {
	invalid := []struct {
		name  string
		delta *bridgev1.RuntimeContextDelta
	}{
		{
			name:  "metadata",
			delta: &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_Reasoning{Reasoning: &bridgev1.RuntimeContextReasoning{Text: "why", ProviderMetadataJson: bridgeString(`{"x":"\ud800"}`)}}}}},
		},
		{
			name:  "input",
			delta: &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_ToolCall{ToolCall: &bridgev1.RuntimeContextToolCall{ModelToolCallId: "call", ToolName: "read", ProviderInputJson: `{"x":"\udc00"}`}}}}},
		},
		{
			name:  "output",
			delta: &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_ToolResult{ToolResult: &bridgev1.RuntimeContextToolResult{ModelToolCallId: "call", Outcome: &bridgev1.RuntimeContextToolResult_Completed{Completed: &bridgev1.RuntimeContextToolCompleted{OutputJson: `{"text":"\ud800"}`}}}}}}},
		},
		{
			name:  "error",
			delta: &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_ToolResult{ToolResult: &bridgev1.RuntimeContextToolResult{ModelToolCallId: "call", Outcome: &bridgev1.RuntimeContextToolResult_Error{Error: &bridgev1.RuntimeContextToolError{ErrorJson: `{"type":"tool_failure","message":"\udc00","retryable":false}`}}}}}}},
		},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			if _, err := canonicalRuntimeContextParts(test.delta); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("unpaired surrogate error = %v; want InvalidArgument", err)
			}
		})
	}
	paired := &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_Reasoning{Reasoning: &bridgev1.RuntimeContextReasoning{Text: "why", ProviderMetadataJson: bridgeString(`{"x":"\ud83d\ude00"}`)}}}}}
	if _, err := canonicalRuntimeContextParts(paired); err != nil {
		t.Fatalf("paired surrogate rejected: %v", err)
	}
}

func bridgeString(value string) *string { return &value }
