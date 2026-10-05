package agentruntimebridge

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestModelContentDeclarationDigestPreservesSuppliedIdentity(t *testing.T) {
	// These canonical bytes and hashes were written independently of the
	// declaration encoder. Keep payload and private reasoning identities in
	// the same digest without making reasoning a public thinking payload.
	tests := []struct {
		name    string
		request *bridgev1.WriteEventRequest
		literal string
		hash    string
	}{
		{
			name: "text with signed reasoning",
			request: &bridgev1.WriteEventRequest{
				Scope: &bridgev1.RuntimeScope{SessionThreadId: "thread"}, RuntimeWriteId: "write", ModelRequestId: "request",
				EventType: "agent.message", PayloadJson: `{"type":"agent.message","content":[{"type":"text","text":"alpha"}]}`,
				PreallocatedEventId: bridgeString("evt_00000000000000000000000000000001"),
				AssistantContextDelta: &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{
					{Content: &bridgev1.RuntimeContextPart_Reasoning{Reasoning: &bridgev1.RuntimeContextReasoning{Text: "reason-before-text", ProviderMetadataJson: bridgeString(`{"anthropic":{"signature":"fixture-signature-text"}}`)}}},
					{Content: &bridgev1.RuntimeContextPart_Text{Text: &bridgev1.RuntimeContextText{Text: "alpha"}}},
				}},
			},
			literal: `{"assistant_context_delta":{"parts":[{"providerMetadata":{"anthropic":{"signature":"fixture-signature-text"}},"text":"reason-before-text","type":"reasoning"},{"text":"alpha","type":"text"}]},"event_type":"agent.message","model_request_id":"request","operation_kind":"write_event","payload":{"content":[{"text":"alpha","type":"text"}],"type":"agent.message"},"preallocated_event_id":"evt_00000000000000000000000000000001","runtime_write_id":"write","session_thread_id":"thread"}`,
			hash:    "7e7b167979662fa4318b04685ad443dee0680d86109ef3edf3c76b82747e7c6e",
		},
		{
			name: "content-free thinking",
			request: &bridgev1.WriteEventRequest{
				Scope: &bridgev1.RuntimeScope{SessionThreadId: "thread"}, RuntimeWriteId: "thinking-write", ModelRequestId: "request",
				EventType: "agent.thinking", PayloadJson: `{"type":"agent.thinking"}`, PreallocatedEventId: bridgeString("evt_00000000000000000000000000000002"),
			},
			literal: `{"assistant_context_delta":null,"event_type":"agent.thinking","model_request_id":"request","operation_kind":"write_event","payload":{"type":"agent.thinking"},"preallocated_event_id":"evt_00000000000000000000000000000002","runtime_write_id":"thinking-write","session_thread_id":"thread"}`,
			hash:    "f2906fe1e77636e8350c969a11297a6ef0268d8ac4ad5b299d7853c5b8465f43",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := fmt.Sprintf("%x", sha256.Sum256([]byte(test.literal))); got != test.hash {
				t.Fatalf("independent literal hash = %s; want %s", got, test.hash)
			}
			got, err := writeEventDeclarationDigest(test.request, test.request.PayloadJson, "[]")
			if err != nil || got != test.hash {
				t.Fatalf("declaration digest = %s/%v; want fixed %s", got, err, test.hash)
			}
			changed := proto.Clone(test.request).(*bridgev1.WriteEventRequest)
			changed.PreallocatedEventId = bridgeString("evt_00000000000000000000000000000003")
			next, err := writeEventDeclarationDigest(changed, changed.PayloadJson, "[]")
			if err != nil || next == got {
				t.Fatalf("changed supplied identity retained digest = %s/%v", next, err)
			}
		})
	}
}

func TestWriteEventRejectsInvalidSuppliedIdentityBeforePersistence(t *testing.T) {
	valid := "evt_00000000000000000000000000000001"
	for _, eventType := range []string{"agent.message", "agent.thinking"} {
		for _, value := range []*string{nil, bridgeString(""), bridgeString("evt_1"), bridgeString("evt_0000000000000000000000000000000A"), bridgeString(valid + " ")} {
			name := eventType + "/missing"
			if value != nil {
				name = eventType + "/" + *value
			}
			t.Run(name, func(t *testing.T) {
				// No database client exists. A validation failure must precede
				// every operation receipt read as well as every new mutation.
				_, err := (&PostgreSQLBridgeAPIStore{}).WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
					RuntimeWriteId: "write", ModelRequestId: "request", EventType: eventType, PayloadJson: `{}`, PreallocatedEventId: value,
				})
				if status.Code(err) != codes.InvalidArgument {
					t.Fatalf("error = %v; want InvalidArgument", err)
				}
			})
		}
	}
	for _, request := range []*bridgev1.WriteEventRequest{
		{RuntimeWriteId: "write", EventType: "agent.thinking", PayloadJson: `{}`, PreallocatedEventId: &valid},
		{RuntimeWriteId: "write", EventType: "span.model_request_start", ModelRequestId: "request", PayloadJson: `{}`, PreallocatedEventId: &valid},
		{RuntimeWriteId: "write", ToolDeclaration: &bridgev1.RuntimeToolDeclaration{}, ModelRequestId: "request", PreallocatedEventId: bridgeString("")},
		{RuntimeWriteId: "write", EventType: "agent.tool_result", PayloadJson: `{}`, PreallocatedEventId: &valid},
	} {
		_, err := (&PostgreSQLBridgeAPIStore{}).WriteEvent(context.Background(), request)
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("forbidden identity error = %v; want InvalidArgument", err)
		}
	}
}

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
			name: "provider metadata", maxBytes: runtimecontrol.RuntimeProviderMetadataMaxBytes, emptyJSON: `{"x":""}`,
			delta: func(raw string) *bridgev1.RuntimeContextDelta {
				return &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_Reasoning{Reasoning: &bridgev1.RuntimeContextReasoning{Text: "why", ProviderMetadataJson: &raw}}}}}
			},
		},
		{
			name: "Tool input", maxBytes: runtimecontrol.RuntimeToolInputJSONMaxBytes, emptyJSON: `{"x":""}`,
			delta: func(raw string) *bridgev1.RuntimeContextDelta {
				return &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_ToolCall{ToolCall: &bridgev1.RuntimeContextToolCall{ModelToolCallId: "call", ToolName: "read", ProviderInputJson: raw}}}}}
			},
		},
		{
			name: "Tool output", maxBytes: runtimecontrol.RuntimeToolOutputJSONMaxBytes, emptyJSON: `{"text":""}`,
			delta: func(raw string) *bridgev1.RuntimeContextDelta {
				return &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_ToolResult{ToolResult: &bridgev1.RuntimeContextToolResult{ModelToolCallId: "call", Outcome: &bridgev1.RuntimeContextToolResult_Completed{Completed: &bridgev1.RuntimeContextToolCompleted{OutputJson: raw}}}}}}}
			},
		},
		{
			name: "Tool error", maxBytes: runtimecontrol.RuntimeToolOutputJSONMaxBytes, emptyJSON: `{"error":{"type":"tool_failure","message":"","retryable":false}}`,
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

func TestRuntimeToolProjectionEnforcesCanonicalExecutionInputBound(t *testing.T) {
	exact := `{"patch":"` + strings.Repeat("x", runtimecontrol.RuntimeToolInputJSONMaxBytes-len(`{"patch":""}`)) + `"}`
	if len(exact) != runtimecontrol.RuntimeToolInputJSONMaxBytes {
		t.Fatalf("exact canonical execution input bytes = %d", len(exact))
	}
	if _, err := normalizeRuntimeToolDeclaration(bridgeToolDeclarationForTest("call", "apply_patch", exact, "allow", "sandbox_execute")); err != nil {
		t.Fatalf("exact canonical execution input rejected: %v", err)
	}
	over := `{"patch":"` + strings.Repeat("x", runtimecontrol.RuntimeToolInputJSONMaxBytes-len(`{"patch":""}`)+1) + `"}`
	if _, err := normalizeRuntimeToolDeclaration(bridgeToolDeclarationForTest("call", "apply_patch", over, "allow", "sandbox_execute")); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("oversized canonical execution input error = %v; want InvalidArgument", err)
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
			exact := strings.Repeat("x", runtimecontrol.RuntimeContextTextJSONMaxBytes-2)
			if runtimecontrol.RuntimeJSONBytes(exact) != runtimecontrol.RuntimeContextTextJSONMaxBytes {
				t.Fatalf("exact JSON-string bytes = %d", runtimecontrol.RuntimeJSONBytes(exact))
			}
			if _, err := canonicalRuntimeContextParts(test.delta(exact)); err != nil {
				t.Fatalf("exact text bound rejected: %v", err)
			}
			if _, err := canonicalRuntimeContextParts(test.delta(exact + "x")); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("one byte over error = %v; want InvalidArgument", err)
			}
		})
	}

	for _, size := range []int{runtimecontrol.RuntimeContextIdentifierMaxBytes, runtimecontrol.RuntimeContextIdentifierMaxBytes + 1} {
		identifier := strings.Repeat("i", size)
		delta := &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_ToolCall{ToolCall: &bridgev1.RuntimeContextToolCall{
			ModelToolCallId: identifier, ToolName: "Read", ProviderInputJson: `{}`,
		}}}}}
		_, err := canonicalRuntimeContextParts(delta)
		if size == runtimecontrol.RuntimeContextIdentifierMaxBytes && err != nil {
			t.Fatalf("exact identifier bound rejected: %v", err)
		}
		if size > runtimecontrol.RuntimeContextIdentifierMaxBytes && status.Code(err) != codes.InvalidArgument {
			t.Fatalf("oversized identifier error = %v; want InvalidArgument", err)
		}
	}
}

func TestStoredRuntimeContextAcceptsProviderIdentifiersWithoutSensitiveTextClassification(t *testing.T) {
	parts, err := runtimecontrol.DecodeStoredRuntimeContextParts(`{"parts":[{"type":"tool_call","modelToolCallId":"dummy-call-1","toolName":"Read","canonicalInput":{}},{"type":"tool_result","modelToolCallId":"dummy-call-1","result":{"type":"completed","output":{"text":"ok"}}}]}`)
	if err != nil {
		t.Fatalf("decode durable provider identifiers: %v", err)
	}
	if len(parts) != 2 {
		t.Fatalf("parts = %d; want 2", len(parts))
	}
}

func TestRuntimeContextSeparatorBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name           string
		limit          int
		prefix, suffix string
		delta          func(string, string) *bridgev1.RuntimeContextDelta
	}{
		{"text", runtimecontrol.RuntimeContextTextJSONMaxBytes, `"`, `"`, func(text, _ string) *bridgev1.RuntimeContextDelta {
			return &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_Text{Text: &bridgev1.RuntimeContextText{Text: text}}}}}
		}},
		{"input", runtimecontrol.RuntimeToolInputJSONMaxBytes, `{"x":"`, `"}`, func(_, wire string) *bridgev1.RuntimeContextDelta {
			return &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_ToolCall{ToolCall: &bridgev1.RuntimeContextToolCall{ModelToolCallId: "call", ToolName: "Read", ProviderInputJson: wire}}}}}
		}},
		{"metadata", runtimecontrol.RuntimeProviderMetadataMaxBytes, `{"x":"`, `"}`, func(_, wire string) *bridgev1.RuntimeContextDelta {
			return &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_Reasoning{Reasoning: &bridgev1.RuntimeContextReasoning{Text: "reason", ProviderMetadataJson: &wire}}}}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bodySize := tc.limit - len(tc.prefix) - len(tc.suffix)
			// Independent JSON.stringify oracle: literal backslash-u is seven
			// bytes, the actual separator is three, and each ASCII filler is one.
			text := strings.Repeat(`\u2028`+"\u2029", bodySize/10) + strings.Repeat("x", bodySize%10)
			body := strings.Repeat(`\\u2028`+"\u2029", bodySize/10) + strings.Repeat("x", bodySize%10)
			wire := tc.prefix + body + tc.suffix
			if len(wire) != tc.limit {
				t.Fatalf("independent wire size=%d want%d", len(wire), tc.limit)
			}
			if _, err := canonicalRuntimeContextParts(tc.delta(text, wire)); err != nil {
				t.Fatalf("exact bound rejected: %v", err)
			}
			if _, err := canonicalRuntimeContextParts(tc.delta(text+"x", tc.prefix+body+"x"+tc.suffix)); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("one byte over bound error=%v want InvalidArgument", err)
			}
		})
	}
}

func TestStableReasoningBudgetCountsJSONStringifyMetadata(t *testing.T) {
	metadata := map[string]any{"a": `\u2028`, "b": "\u2029"}
	const wire = "{\"a\":\"\\\\u2028\",\"b\":\"\u2029\"}"
	if len(wire) != 25 {
		t.Fatal("independent metadata wire literal must be 25 bytes")
	}
	text := strings.Repeat("x", MaxStableReasoningBytesPerRequest-25)
	part := map[string]any{"type": "reasoning", "text": text, "providerMetadata": metadata}
	if err := validateStableReasoningBudget([]any{part}); err != nil {
		t.Fatalf("exact aggregate JSON.stringify budget rejected: %v", err)
	}
	part["text"] = text + "x"
	if err := validateStableReasoningBudget([]any{part}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("one byte over aggregate budget error=%v want InvalidArgument", err)
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

func TestRuntimeDeclarationStringifyPreservesSpansAndSeparatorEscapes(t *testing.T) {
	span := strings.Repeat("x", 2048)
	for _, test := range []struct {
		name, encoded, want string
	}{
		{name: "unescaped span", encoded: span, want: span},
		{
			name:    "spans around separators and escaped backslashes",
			encoded: span + `\u2028` + span + `\\u2028` + span + `\u2029` + span,
			want:    span + "\u2028" + span + `\\u2028` + span + "\u2029" + span,
		},
		{
			name:    "odd and even backslash parity",
			encoded: span + `\\\u2028` + span + `\\\\u2029` + span,
			want:    span + `\\` + "\u2028" + span + `\\\\u2029` + span,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if string(runtimecontrol.RestoreJSONStringifySeparatorEscapes([]byte(test.encoded))) != test.want {
				t.Fatal("Runtime declaration stringify bytes differ")
			}
		})
	}
}

// Raw execution/provider tokens participate in custody identity, while provider
// context decodes duplicate keys and keeps json.Number spellings for storage.
func TestPreparedToolDeclarationPreservesRawIdentityAndDecodedContext(t *testing.T) {
	declaration := bridgeToolDeclarationForTest("call", "read", `{"n":-0,"a":2,"\u0061":1.00,"large":9007199254740993,"e":1e+00}`, "allow", "sandbox_execute")
	prepared, err := normalizeRuntimeToolDeclaration(declaration)
	if err != nil {
		t.Fatal(err)
	}
	wantRaw := `{"\u0061":1.00,"a":2,"e":1e+00,"large":9007199254740993,"n":-0}`
	if string(prepared.projection.ProviderInput) != wantRaw || string(prepared.projection.CanonicalExecutionInput) != wantRaw {
		t.Fatal("raw canonical tokens changed")
	}
	parts, err := runtimeAssistantContextParts(runtimeToolContextDelta(prepared.projection), &prepared)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(parts)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `[{"canonicalInput":{"a":2,"e":1e+00,"large":9007199254740993,"n":-0},"modelToolCallId":"call","toolName":"read","type":"tool_call"}]` {
		t.Fatalf("decoded context = %s", encoded)
	}
	request := &bridgev1.WriteEventRequest{Scope: &bridgev1.RuntimeScope{SessionThreadId: "thread"}, RuntimeWriteId: "write", ModelRequestId: "request"}
	digest, err := writeToolDeclarationDigest(request, prepared)
	if err != nil {
		t.Fatal(err)
	}
	if digest != "1866d504f2792ea14fe3ae8fab3da2c79105ac80a7ffb07db147bb4be9a3eae6" {
		t.Fatalf("digest = %s; want independently fixed custody identity", digest)
	}
}
func TestPreparedToolContextOwnsDecodedReasoningAndInput(t *testing.T) {
	declaration := bridgeToolDeclarationForTest("call", "read", `{"x":{"y":1}}`, "allow", "sandbox_execute")
	declaration.LeadingReasoning = []*bridgev1.RuntimeContextReasoning{{Text: "original", ProviderMetadataJson: bridgeString(`{"x":{"y":2}}`)}}
	prepared, err := normalizeRuntimeToolDeclaration(declaration)
	if err != nil {
		t.Fatal(err)
	}
	declaration.PublicExecutionInputJson = `{"changed":true}`
	declaration.LeadingReasoning[0].Text = "changed"
	*declaration.LeadingReasoning[0].ProviderMetadataJson = `{"changed":true}`
	encoded, err := json.Marshal(prepared.contextParts)
	if err != nil {
		t.Fatal(err)
	}
	want := `[{"providerMetadata":{"x":{"y":2}},"text":"original","type":"reasoning"},{"canonicalInput":{"x":{"y":1}},"modelToolCallId":"call","toolName":"read","type":"tool_call"}]`
	if string(encoded) != want {
		t.Fatalf("prepared context changed through request aliases: %s", encoded)
	}
}

func toolDeclarationDigestObjectForTest(request *bridgev1.WriteEventRequest, prepared preparedRuntimeToolDeclaration) map[string]any {
	declaration := prepared.projection
	contextDelta := map[string]any{"parts": prepared.contextParts}
	return map[string]any{
		"assistant_context_delta": contextDelta,
		"evaluated_permission":    declaration.EvaluatedPermission,
		"event_type":              declaration.EventType,
		"mcp_server_name":         nullableDeclarationString(declaration.MCPServerName),
		"model_request_id":        request.GetModelRequestId(),
		"model_tool_call_id":      declaration.ModelToolCallID,
		"operation_kind":          bridgeOpWriteEvent,
		"provider_input":          declaration.ProviderInput,
		"public_execution_input":  declaration.CanonicalExecutionInput,
		"route_capability":        declaration.RouteCapability,
		"runtime_write_id":        request.GetRuntimeWriteId(),
		"session_thread_id":       request.GetScope().GetSessionThreadId(),
		"tool_name":               declaration.ToolName,
	}
}

func referenceWriteToolDeclarationDigest(request *bridgev1.WriteEventRequest, prepared preparedRuntimeToolDeclaration) (string, error) {
	declaration := prepared.projection
	contextDelta := map[string]any{"parts": prepared.contextParts}
	raw, err := marshalRuntimeDeclarationObject(map[string]any{
		"assistant_context_delta": contextDelta,
		"evaluated_permission":    declaration.EvaluatedPermission,
		"event_type":              declaration.EventType,
		"mcp_server_name":         nullableDeclarationString(declaration.MCPServerName),
		"model_request_id":        request.GetModelRequestId(),
		"model_tool_call_id":      declaration.ModelToolCallID,
		"operation_kind":          bridgeOpWriteEvent,
		"provider_input":          declaration.ProviderInput,
		"public_execution_input":  declaration.CanonicalExecutionInput,
		"route_capability":        declaration.RouteCapability,
		"runtime_write_id":        request.GetRuntimeWriteId(),
		"session_thread_id":       request.GetScope().GetSessionThreadId(),
		"tool_name":               declaration.ToolName,
	})
	if err != nil {
		return "", err
	}
	canonical, err := runtimecontrol.CanonicalRunToolJSON(string(raw))
	if err != nil {
		return "", err
	}
	return runtimecontrol.Sha256Hex(canonical), nil
}

func TestPreparedToolDeclarationDigestEncodingEquivalence(t *testing.T) {
	request := &bridgev1.WriteEventRequest{Scope: &bridgev1.RuntimeScope{SessionThreadId: "thread"}, RuntimeWriteId: "write", ModelRequestId: "request"}
	type vector struct {
		name, input string
		provider    *string
		reasoning   bool
		mcp         bool
	}
	p := func(s string) *string { return &s }
	vectors := []vector{
		{name: "ordinary", input: `{"value":"ok"}`},
		{name: "independent_raw_number_literal", input: `{"n":-0,"a":2,"\u0061":1.00,"large":9007199254740993,"e":1e+00}`},
		{name: "duplicates", input: `{"a":1,"a":2,"z":[1e+00,-0]}`},
		{name: "escaped_keys", input: `{"\u0061":1,"a":2,"\\u2028":"\\u2029"}`},
		{name: "genuine_separator_escapes", input: `{"x":"\u2028\u2029"}`},
		{name: "literal_backslash_separator", input: `{"x":"\\u2028\\u2029"}`},
		{name: "odd_even_slash_separator", input: `{"x":"\\\u2028\\\\u2029"}`},
		{name: "actual_separator", input: "{\"x\":\"\u2028\u2029\"}"},
		{name: "html_quote_control", input: `{"x":"<>&\"\\\n\t"}`},
		{name: "paired_surrogate", input: `{"x":"\ud83d\ude00"}`},
		{name: "distinct_object", input: `{"x":1}`, provider: p(`{"b":"\u2028","a":-0}`)},
		{name: "distinct_scalar", input: `{"x":1}`, provider: p(`"\u2028\\u2029"`)},
		{name: "distinct_array", input: `{"x":1}`, provider: p(`[1,-0,"\u2028"]`)},
		{name: "signed_reasoning", input: `{"x":"\u2028"}`, reasoning: true},
		{name: "mcp_signed_distinct", input: `{"x":1}`, provider: p(`{"x":"\u2029"}`), reasoning: true, mcp: true},
		{name: "depth_at", input: strings.Repeat(`{"x":`, 256) + `0` + strings.Repeat(`}`, 256)},
		{name: "depth_above", input: strings.Repeat(`{"x":`, 257) + `0` + strings.Repeat(`}`, 257)},
		{name: "lone_surrogate", input: `{"x":"\ud800"}`},
		{name: "empty_input", input: ``}, {name: "invalid_input", input: `{"x":}`},
		{name: "input_limit_at", input: `{"x":"` + strings.Repeat("x", runtimecontrol.RuntimeToolInputJSONMaxBytes-8) + `"}`},
		{name: "input_limit_above", input: `{"x":"` + strings.Repeat("x", runtimecontrol.RuntimeToolInputJSONMaxBytes-7) + `"}`},
	}
	for _, v := range vectors {
		t.Run(v.name, func(t *testing.T) {
			d := bridgeToolDeclarationForTest("call", "read", v.input, "allow", "sandbox_execute")
			d.DistinctProviderInputJson = v.provider
			if v.reasoning {
				metadata := `{"signature":"opaque\u2028\\u2029","nested":{"key":"\u2029"}}`
				d.LeadingReasoning = []*bridgev1.RuntimeContextReasoning{{Text: "why\u2028" + "\u2029", ProviderMetadataJson: &metadata}}
			}
			if v.mcp {
				server := "fixture"
				d.EventKind = bridgev1.RuntimeToolEventKind_RUNTIME_TOOL_EVENT_KIND_MCP
				d.McpServerName = &server
				d.RouteCapability = "mcp_execute"
			}
			prepared, err := normalizeRuntimeToolDeclaration(d)
			expectedNegative := v.name == "depth_above" || v.name == "lone_surrogate" || v.name == "empty_input" || v.name == "invalid_input" || v.name == "input_limit_above"
			if expectedNegative {
				if err == nil || status.Code(err) != codes.InvalidArgument {
					t.Fatal("independent invalid domain must reject normalization")
				}
				if prepared.rawInputsNormalized {
					t.Fatal("failed normalization must not confer trust")
				}
				return
			}
			if err != nil {
				t.Fatal("positive normalization unexpectedly rejected")
			}
			if !prepared.rawInputsNormalized {
				t.Fatal("successful normalization must establish fresh raw ownership")
			}
			object := toolDeclarationDigestObjectForTest(request, prepared)
			baseline, e1 := marshalRuntimeDeclarationObject(object)
			candidate, e2 := marshalPreparedRuntimeToolDeclarationObject(object, prepared)
			if fmt.Sprint(e1) != fmt.Sprint(e2) || !bytes.Equal(baseline, candidate) {
				t.Fatal("PRE-canonical envelope bytes/error differ")
			}

			if v.name == "genuine_separator_escapes" {
				for _, member := range []string{"provider_input", "public_execution_input"} {
					want := `"` + member + `":{"x":"` + "\u2028\u2029" + `"}`
					if !bytes.Contains(candidate, []byte(want)) {
						t.Fatal("independent genuine separator envelope bytes differ")
					}
				}
			}
			if v.name == "literal_backslash_separator" {
				for _, member := range []string{"provider_input", "public_execution_input"} {
					want := `"` + member + `":{"x":"\\u2028\\u2029"}`
					if !bytes.Contains(candidate, []byte(want)) {
						t.Fatal("independent literal-backslash envelope bytes differ")
					}
				}
			}
			oldCanonical, e1 := runtimecontrol.CanonicalRunToolJSON(string(baseline))
			newCanonical, e2 := runtimecontrol.CanonicalRunToolJSON(string(candidate))
			if fmt.Sprint(e1) != fmt.Sprint(e2) || oldCanonical != newCanonical {
				t.Fatal("canonical bytes/error differ")
			}
			oldDigest, e1 := referenceWriteToolDeclarationDigest(request, prepared)
			newDigest, e2 := writeToolDeclarationDigest(request, prepared)
			if fmt.Sprint(e1) != fmt.Sprint(e2) || oldDigest != newDigest {
				t.Fatal("digest/error differ")
			}
			if v.name == "depth_at" && e1 == nil {
				t.Fatal("outer digest must preserve nesting-bound rejection")
			}
			if v.name == "independent_raw_number_literal" {
				const want = `{"assistant_context_delta":{"parts":[{"canonicalInput":{"a":2,"e":1e+00,"large":9007199254740993,"n":-0},"modelToolCallId":"call","toolName":"read","type":"tool_call"}]},"evaluated_permission":"allow","event_type":"agent.tool_use","mcp_server_name":null,"model_request_id":"request","model_tool_call_id":"call","operation_kind":"write_event","provider_input":{"\u0061":1.00,"a":2,"e":1e+00,"large":9007199254740993,"n":-0},"public_execution_input":{"\u0061":1.00,"a":2,"e":1e+00,"large":9007199254740993,"n":-0},"route_capability":"sandbox_execute","runtime_write_id":"write","session_thread_id":"thread","tool_name":"read"}`
				if string(candidate) != want || newDigest != "1866d504f2792ea14fe3ae8fab3da2c79105ac80a7ffb07db147bb4be9a3eae6" {
					t.Fatal("independent byte/digest literal differs")
				}
			}
		})
	}

	t.Run("fresh_owner_but_unowned_or_changed_envelope", func(t *testing.T) {
		prepared, err := normalizeRuntimeToolDeclaration(bridgeToolDeclarationForTest("call", "read", `{"x":1}`, "allow", "sandbox_execute"))
		if err != nil {
			t.Fatal("normalize valid owner")
		}
		for _, change := range []func(map[string]any){
			func(v map[string]any) { v["provider_input"] = json.RawMessage(`{"bad":}`) },
			func(v map[string]any) { v["public_execution_input"] = json.RawMessage(`{"x":"\u2028"}`) },
			func(v map[string]any) { v["future_member"] = true },
			func(v map[string]any) { delete(v, "tool_name") },
		} {
			object := toolDeclarationDigestObjectForTest(request, prepared)
			change(object)
			a, e1 := marshalRuntimeDeclarationObject(object)
			b, e2 := marshalPreparedRuntimeToolDeclarationObject(object, prepared)
			if fmt.Sprint(e1) != fmt.Sprint(e2) || !bytes.Equal(a, b) {
				t.Fatal("unowned or changed envelope must retain baseline byte/error behavior")
			}
		}
	})
	t.Run("nil_normalization", func(t *testing.T) {
		prepared, err := normalizeRuntimeToolDeclaration(nil)
		if err == nil || prepared.rawInputsNormalized {
			t.Fatal("nil normalization cannot confer trust")
		}
	})
	// Arbitrary/unprepared bytes never acquire fast-path eligibility. Preserve nil,
	// malformed RawMessage and non-JSON values through the exact baseline encoder.
	for _, raw := range []json.RawMessage{nil, {}, json.RawMessage(`{"x":}`), json.RawMessage(`"\ud800"`), json.RawMessage(`{"x":"\u2028"}`)} {
		t.Run("unprepared_raw", func(t *testing.T) {
			prepared := preparedRuntimeToolDeclaration{projection: runtimecontrol.ToolProjection{ProviderInput: raw, CanonicalExecutionInput: raw}}
			object := toolDeclarationDigestObjectForTest(request, prepared)
			a, e1 := marshalRuntimeDeclarationObject(object)
			b, e2 := marshalPreparedRuntimeToolDeclarationObject(object, prepared)
			if fmt.Sprint(e1) != fmt.Sprint(e2) || !bytes.Equal(a, b) {
				t.Fatal("unprepared byte/error behavior differs")
			}
		})
	}
	for _, value := range []any{math.NaN(), func() {}, make(chan int)} {
		t.Run("unprepared_encoding_error", func(t *testing.T) {
			prepared := preparedRuntimeToolDeclaration{contextParts: []map[string]any{{"invalid": value}}}
			object := toolDeclarationDigestObjectForTest(request, prepared)
			a, e1 := marshalRuntimeDeclarationObject(object)
			b, e2 := marshalPreparedRuntimeToolDeclarationObject(object, prepared)
			if e1 == nil || fmt.Sprint(e1) != fmt.Sprint(e2) || !bytes.Equal(a, b) {
				t.Fatal("baseline exceptional encoder behavior differs")
			}
		})
	}
}
