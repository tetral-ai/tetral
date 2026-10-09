package runtimecontrol

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestRuntimeJSONBytesPreservesLiteralAndUnicodeSeparators(t *testing.T) {
	// Expected wire bytes are literal JSON.stringify output, independently
	// checked with Bun. Literal backslash-u text must not become a separator.
	for _, tc := range []struct {
		name  string
		value any
		wire  string
		size  int
	}{
		{"literal-line", `\u2028`, `"\\u2028"`, 9},
		{"literal-paragraph", `\u2029`, `"\\u2029"`, 9},
		{"line", "\u2028", "\"\u2028\"", 5},
		{"paragraph", "\u2029", "\"\u2029\"", 5},
		{"two-backslashes", `\\u2028`, `"\\\\u2028"`, 11},
		{"three-backslashes", `\\\u2028`, `"\\\\\\u2028"`, 13},
		{"four-backslashes", `\\\\u2029`, `"\\\\\\\\u2029"`, 15},
		{"mixed", `x\u2028` + "\u2029" + `\u2029`, "\"x\\\\u2028\u2029\\\\u2029\"", 20},
		{"metadata", map[string]any{"a": `\u2028`, "b": "\u2029"}, "{\"a\":\"\\\\u2028\",\"b\":\"\u2029\"}", 25},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if len(tc.wire) != tc.size {
				t.Fatalf("independent wire literal size=%d want%d", len(tc.wire), tc.size)
			}
			if got := RuntimeJSONBytes(tc.value); got != tc.size {
				t.Fatalf("JSON.stringify bytes=%d want%d", got, tc.size)
			}
			var encoded bytes.Buffer
			e := json.NewEncoder(&encoded)
			e.SetEscapeHTML(false)
			if err := e.Encode(tc.value); err != nil {
				t.Fatal(err)
			}
			if got := string(RestoreJSONStringifySeparatorEscapes(bytes.TrimSuffix(encoded.Bytes(), []byte{'\n'}))); got != tc.wire {
				t.Fatalf("separator restoration %q want%q", got, tc.wire)
			}
		})
	}
}

func TestStoredRuntimeContextSeparatorByteBounds(t *testing.T) {
	// A literal backslash-u escape encodes to seven bytes; the actual separator
	// encodes to three. The wire oracle is assembled without the Go encoder.
	for _, tc := range []struct {
		name           string
		limit          int
		prefix, suffix string
		part           func(string) map[string]any
	}{
		{"text", RuntimeContextTextJSONMaxBytes, `"`, `"`, func(text string) map[string]any {
			return map[string]any{"type": "text", "text": text}
		}},
		{"input", RuntimeToolInputJSONMaxBytes, `{"x":"`, `"}`, func(text string) map[string]any {
			return map[string]any{"type": "tool_call", "modelToolCallId": "call", "toolName": "Read", "canonicalInput": map[string]any{"x": text}}
		}},
		{"metadata", RuntimeProviderMetadataMaxBytes, `{"x":"`, `"}`, func(text string) map[string]any {
			return map[string]any{"type": "reasoning", "text": "reason", "providerMetadata": map[string]any{"x": text}}
		}},
		{"output", RuntimeToolOutputJSONMaxBytes, `{"text":"`, `"}`, func(text string) map[string]any {
			return map[string]any{"type": "tool_result", "modelToolCallId": "call", "result": map[string]any{"type": "completed", "output": map[string]any{"text": text}}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bodySize := tc.limit - len(tc.prefix) - len(tc.suffix)
			text := strings.Repeat(`\u2028`+"\u2029", bodySize/10) + strings.Repeat("x", bodySize%10)
			wire := tc.prefix + strings.Repeat(`\\u2028`+"\u2029", bodySize/10) + strings.Repeat("x", bodySize%10) + tc.suffix
			if len(wire) != tc.limit {
				t.Fatalf("independent wire length=%d want%d", len(wire), tc.limit)
			}
			if err := ValidateStoredRuntimeContextPart(tc.part(text)); err != nil {
				t.Fatalf("exact JSON.stringify bound rejected: %v", err)
			}
			if err := ValidateStoredRuntimeContextPart(tc.part(text + "x")); err == nil {
				t.Fatal("one byte over JSON.stringify bound accepted")
			}
		})
	}
}
