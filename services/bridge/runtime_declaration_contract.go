package agentruntimebridge

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"regexp"
	"strings"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

const (
	runtimeProviderMetadataMaxBytes  = 16 * 1024
	runtimeToolOutputJSONMaxBytes    = 512 * 1024
	runtimeToolInputJSONMaxBytes     = 4 * 1024 * 1024
	runtimeContextTextJSONMaxBytes   = 16 * 1024 * 1024
	runtimeContextIdentifierMaxBytes = 128
)

var runtimeSensitiveTextPatterns = []*regexp.Regexp{
	regexp.MustCompile(`\b[A-Z0-9_]*(?:TOKEN|CREDENTIAL|SECRET)[A-Z0-9_]*CANARY\b`),
	regexp.MustCompile(`(?i)\b(?:sk|dummy)[-_][A-Za-z0-9._-]+\b`),
	regexp.MustCompile(`(?i)\b(?:postgres|postgresql|mysql|redis)://[^\s"'<>]+`),
	regexp.MustCompile(`(?i)\bselect\s+.+?\s+from\s+\S+`),
	regexp.MustCompile(`(?i)\bauthorization\s*:\s*bearer\s+[^\s"'<>]+`),
	regexp.MustCompile(`(?i)\b(?:x-api-key|api-key|cookie|set-cookie)\s*:\s*[^\n\r]+`),
	regexp.MustCompile(`(?i)\bsystem prompt raw backend payload marker\b`),
	regexp.MustCompile(`(?i)\braw backend payload marker\b`),
	regexp.MustCompile(`(?i)\braw provider payload marker\b`),
	regexp.MustCompile(`(?i)\braw-secret-body\b`),
	regexp.MustCompile(`/tmp/[^\s"'<>]+`),
	regexp.MustCompile(`(?i)\bhttps?://[^\s"'<>]+`),
	regexp.MustCompile(`(?i)\bprompt text\b`),
	regexp.MustCompile(`(?i)\btool input\b`),
	regexp.MustCompile(`(?i)\btool output\b`),
	regexp.MustCompile(`(?i)\bstack trace\b`),
	regexp.MustCompile(`(?i)\bcause value\b`),
}

// Runtime declares only provider-visible context. Bridge validates storage
// bounds and the closed wire union, then persists the exact normalized facts.
func canonicalRuntimeContextParts(delta *bridgev1.RuntimeContextDelta) ([]map[string]any, error) {
	if delta == nil || len(delta.GetParts()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "runtime context delta is empty")
	}
	parts := make([]map[string]any, 0, len(delta.GetParts()))
	for _, declared := range delta.GetParts() {
		if declared == nil {
			return nil, status.Error(codes.InvalidArgument, "runtime context part is invalid")
		}
		var part map[string]any
		switch content := declared.GetContent().(type) {
		case *bridgev1.RuntimeContextPart_Text:
			if content.Text == nil {
				return nil, status.Error(codes.InvalidArgument, "runtime text context is invalid")
			}
			if runtimeJSONBytes(content.Text.GetText()) > runtimeContextTextJSONMaxBytes {
				return nil, status.Error(codes.InvalidArgument, "runtime text context exceeds its provider bound")
			}
			part = map[string]any{"type": "text", "text": content.Text.GetText()}
		case *bridgev1.RuntimeContextPart_Reasoning:
			if content.Reasoning == nil {
				return nil, status.Error(codes.InvalidArgument, "runtime reasoning context is invalid")
			}
			if runtimeJSONBytes(content.Reasoning.GetText()) > runtimeContextTextJSONMaxBytes {
				return nil, status.Error(codes.InvalidArgument, "runtime reasoning context exceeds its provider bound")
			}
			part = map[string]any{"type": "reasoning", "text": content.Reasoning.GetText()}
			if content.Reasoning.ProviderMetadataJson != nil {
				metadata, err := decodeRuntimeDeclarationObject(content.Reasoning.GetProviderMetadataJson())
				if err != nil || runtimeJSONBytes(metadata) > runtimeProviderMetadataMaxBytes {
					return nil, status.Error(codes.InvalidArgument, "runtime reasoning metadata is invalid")
				}
				part["providerMetadata"] = metadata
			}
		case *bridgev1.RuntimeContextPart_ToolCall:
			call := content.ToolCall
			if call == nil || !runtimeAlreadyCanonicalIdentifier(call.GetModelToolCallId()) || !runtimeAlreadyCanonicalIdentifier(call.GetToolName()) {
				return nil, status.Error(codes.InvalidArgument, "runtime tool call context is invalid")
			}
			input, err := decodeRuntimeDeclarationValue(call.GetCanonicalInputJson())
			if err != nil || runtimeJSONBytes(input) > runtimeToolInputJSONMaxBytes {
				return nil, status.Error(codes.InvalidArgument, "runtime tool call input is invalid")
			}
			part = map[string]any{
				"type": "tool_call", "modelToolCallId": call.GetModelToolCallId(),
				"toolName": call.GetToolName(), "canonicalInput": input,
			}
		case *bridgev1.RuntimeContextPart_ToolResult:
			result := content.ToolResult
			if result == nil || !runtimeAlreadyCanonicalIdentifier(result.GetModelToolCallId()) {
				return nil, status.Error(codes.InvalidArgument, "runtime tool result context is invalid")
			}
			outcome, err := canonicalRuntimeToolResultOutcome(result)
			if err != nil {
				return nil, err
			}
			part = map[string]any{
				"type": "tool_result", "modelToolCallId": result.GetModelToolCallId(), "result": outcome,
			}
		default:
			return nil, status.Error(codes.InvalidArgument, "runtime context part is invalid")
		}
		parts = append(parts, part)
	}
	return parts, nil
}

func runtimeContextPartKind(part *bridgev1.RuntimeContextPart) string {
	if part == nil {
		return ""
	}
	switch part.GetContent().(type) {
	case *bridgev1.RuntimeContextPart_Text:
		return "text"
	case *bridgev1.RuntimeContextPart_Reasoning:
		return "reasoning"
	case *bridgev1.RuntimeContextPart_ToolCall:
		return "tool_call"
	case *bridgev1.RuntimeContextPart_ToolResult:
		return "tool_result"
	default:
		return ""
	}
}

func canonicalRuntimeContextDelta(delta *bridgev1.RuntimeContextDelta) (any, error) {
	if delta == nil {
		return nil, nil
	}
	parts, err := canonicalRuntimeContextParts(delta)
	if err != nil {
		return nil, err
	}
	return map[string]any{"parts": parts}, nil
}

func canonicalRuntimeToolResultOutcome(result *bridgev1.RuntimeContextToolResult) (map[string]any, error) {
	switch value := result.GetOutcome().(type) {
	case *bridgev1.RuntimeContextToolResult_Completed:
		if value.Completed == nil {
			return nil, status.Error(codes.InvalidArgument, "runtime tool completion is invalid")
		}
		output, err := decodeRuntimeDeclarationValue(value.Completed.GetOutputJson())
		if err != nil || validateRuntimeContextToolOutput(output) != nil {
			return nil, status.Error(codes.InvalidArgument, "runtime tool completion is invalid")
		}
		return map[string]any{"type": "completed", "output": output}, nil
	case *bridgev1.RuntimeContextToolResult_Error:
		if value.Error == nil {
			return nil, status.Error(codes.InvalidArgument, "runtime tool error is invalid")
		}
		failure, err := decodeRuntimeToolErrorJSON(value.Error.GetErrorJson())
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "runtime tool error is invalid")
		}
		return map[string]any{"type": "error", "error": failure}, nil
	case *bridgev1.RuntimeContextToolResult_Cancelled:
		if value.Cancelled == nil {
			return nil, status.Error(codes.InvalidArgument, "runtime tool cancellation is invalid")
		}
		return map[string]any{"type": "cancelled"}, nil
	default:
		return nil, status.Error(codes.InvalidArgument, "runtime tool result outcome is missing")
	}
}

func canonicalRuntimeToolError(value *bridgev1.RuntimeToolError) (map[string]any, error) {
	if value == nil {
		return nil, status.Error(codes.InvalidArgument, "runtime tool error is invalid")
	}
	failure, err := decodeRuntimeToolErrorJSON(value.GetErrorJson())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "runtime tool error is invalid")
	}
	return failure, nil
}

func runtimeContextDataJSON(parts []map[string]any) (string, error) {
	return marshalBridgeJSON(map[string]any{"parts": parts})
}

func decodeStoredRuntimeContextParts(raw string) ([]json.RawMessage, error) {
	stored, err := decodeRuntimeDeclarationObject(raw)
	if err != nil || requireRuntimeObjectFields(stored, []string{"parts"}, []string{"parts"}) != nil {
		return nil, status.Error(codes.FailedPrecondition, "durable context entry is malformed")
	}
	values, ok := stored["parts"].([]any)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "durable context entry is malformed")
	}
	parts := make([]json.RawMessage, 0, len(values))
	for _, value := range values {
		part, ok := value.(map[string]any)
		if !ok || validateStoredRuntimeContextPart(part) != nil {
			return nil, status.Error(codes.FailedPrecondition, "durable context part is malformed")
		}
		encoded, err := json.Marshal(part)
		if err != nil {
			return nil, err
		}
		parts = append(parts, encoded)
	}
	return parts, nil
}

func validateStoredRuntimeContextPart(part map[string]any) error {
	kind, _ := part["type"].(string)
	switch kind {
	case "text":
		if requireRuntimeObjectFields(part, []string{"type", "text"}, []string{"type", "text"}) != nil {
			return fmt.Errorf("invalid text")
		}
		text, ok := part["text"].(string)
		if !ok || runtimeJSONBytes(text) > runtimeContextTextJSONMaxBytes {
			return fmt.Errorf("invalid text")
		}
	case "reasoning":
		if requireRuntimeObjectFields(part, []string{"type", "text"}, []string{"type", "text", "providerMetadata"}) != nil {
			return fmt.Errorf("invalid reasoning")
		}
		text, ok := part["text"].(string)
		if !ok || runtimeJSONBytes(text) > runtimeContextTextJSONMaxBytes {
			return fmt.Errorf("invalid reasoning")
		}
		if metadata, ok := part["providerMetadata"]; ok {
			if _, ok := metadata.(map[string]any); !ok || runtimeJSONBytes(metadata) > runtimeProviderMetadataMaxBytes {
				return fmt.Errorf("invalid reasoning metadata")
			}
		}
	case "tool_call":
		if requireRuntimeObjectFields(part,
			[]string{"type", "modelToolCallId", "toolName", "canonicalInput"},
			[]string{"type", "modelToolCallId", "toolName", "canonicalInput"},
		) != nil || !runtimeAlreadyCanonicalIdentifier(part["modelToolCallId"]) || !runtimeAlreadyCanonicalIdentifier(part["toolName"]) ||
			!runtimeJSONValue(part["canonicalInput"]) || runtimeJSONBytes(part["canonicalInput"]) > runtimeToolInputJSONMaxBytes {
			return fmt.Errorf("invalid tool call")
		}
	case "tool_result":
		if requireRuntimeObjectFields(part,
			[]string{"type", "modelToolCallId", "result"},
			[]string{"type", "modelToolCallId", "result"},
		) != nil || !runtimeAlreadyCanonicalIdentifier(part["modelToolCallId"]) {
			return fmt.Errorf("invalid tool result")
		}
		result, ok := part["result"].(map[string]any)
		if !ok || validateStoredRuntimeToolResult(result) != nil {
			return fmt.Errorf("invalid tool result")
		}
	default:
		return fmt.Errorf("invalid context part kind")
	}
	return nil
}

func validateStoredRuntimeToolResult(result map[string]any) error {
	statusValue, _ := result["type"].(string)
	switch statusValue {
	case "completed":
		if requireRuntimeObjectFields(result, []string{"type", "output"}, []string{"type", "output"}) != nil || validateRuntimeContextToolOutput(result["output"]) != nil {
			return fmt.Errorf("invalid completion")
		}
	case "error":
		if requireRuntimeObjectFields(result, []string{"type", "error"}, []string{"type", "error"}) != nil || validateRuntimeToolError(result["error"]) != nil {
			return fmt.Errorf("invalid error")
		}
	case "cancelled":
		if requireRuntimeObjectFields(result, []string{"type"}, []string{"type"}) != nil {
			return fmt.Errorf("invalid cancellation")
		}
	default:
		return fmt.Errorf("invalid result status")
	}
	return nil
}

func validateRuntimeBoundedText(value any) error {
	output, ok := value.(map[string]any)
	if !ok || requireRuntimeObjectFields(output, []string{"text", "truncated"}, []string{"text", "truncated"}) != nil {
		return fmt.Errorf("invalid bounded text")
	}
	if _, ok := output["text"].(string); !ok {
		return fmt.Errorf("invalid bounded text")
	}
	if _, ok := output["truncated"].(bool); !ok || runtimeJSONBytes(map[string]any{"text": output["text"]}) > runtimeToolOutputJSONMaxBytes {
		return fmt.Errorf("invalid bounded text")
	}
	return nil
}

func validateRuntimeContextToolOutput(value any) error {
	output, ok := value.(map[string]any)
	if !ok || requireRuntimeObjectFields(output, []string{"text"}, []string{"text"}) != nil {
		return fmt.Errorf("invalid context Tool output")
	}
	if _, ok := output["text"].(string); !ok || runtimeJSONBytes(output) > runtimeToolOutputJSONMaxBytes {
		return fmt.Errorf("invalid context Tool output")
	}
	return nil
}

func validateRuntimeToolError(value any) error {
	toolError, ok := value.(map[string]any)
	if !ok || requireRuntimeObjectFields(toolError, []string{"type", "message"}, []string{"type", "message", "retryable"}) != nil ||
		!runtimeAlreadyCanonicalIdentifier(toolError["type"]) || !runtimeAlreadyCanonicalText(toolError["message"]) ||
		runtimeJSONBytes(map[string]any{"error": toolError}) > runtimeToolOutputJSONMaxBytes {
		return fmt.Errorf("invalid Tool error")
	}
	if retryable, exists := toolError["retryable"]; exists {
		if _, ok := retryable.(bool); !ok {
			return fmt.Errorf("invalid Tool error retryability")
		}
	}
	return nil
}

func decodeRuntimeDeclarationObject(raw string) (map[string]any, error) {
	value, err := decodeRuntimeDeclarationValue(raw)
	if err != nil {
		return nil, err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid object")
	}
	return object, nil
}

func decodeRuntimeDeclarationValue(raw string) (any, error) {
	if !validRuntimeJSONUnicodeEscapes(raw) {
		return nil, fmt.Errorf("invalid JSON Unicode escape")
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("invalid JSON")
	}
	if err := decoder.Decode(new(any)); err != io.EOF || !runtimeJSONValue(value) {
		return nil, fmt.Errorf("trailing or invalid JSON")
	}
	return value, nil
}

func validRuntimeJSONUnicodeEscapes(raw string) bool {
	inString := false
	for index := 0; index < len(raw); index++ {
		switch raw[index] {
		case '"':
			inString = !inString
		case '\\':
			if !inString || index+1 >= len(raw) {
				continue
			}
			if raw[index+1] != 'u' {
				index++
				continue
			}
			code, ok := runtimeJSONUnicodeEscape(raw, index)
			if !ok {
				return false
			}
			if code >= 0xd800 && code <= 0xdbff {
				low, ok := runtimeJSONUnicodeEscape(raw, index+6)
				if !ok || low < 0xdc00 || low > 0xdfff {
					return false
				}
				index += 11
				continue
			}
			if code >= 0xdc00 && code <= 0xdfff {
				return false
			}
			index += 5
		}
	}
	return true
}

func runtimeJSONUnicodeEscape(raw string, slash int) (uint16, bool) {
	if slash < 0 || slash+6 > len(raw) || raw[slash] != '\\' || raw[slash+1] != 'u' {
		return 0, false
	}
	var value uint16
	for _, digit := range []byte(raw[slash+2 : slash+6]) {
		value <<= 4
		switch {
		case digit >= '0' && digit <= '9':
			value += uint16(digit - '0')
		case digit >= 'a' && digit <= 'f':
			value += uint16(digit-'a') + 10
		case digit >= 'A' && digit <= 'F':
			value += uint16(digit-'A') + 10
		default:
			return 0, false
		}
	}
	return value, true
}

func requireRuntimeObjectFields(value map[string]any, required, allowed []string) error {
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, field := range allowed {
		allowedSet[field] = struct{}{}
	}
	for field := range value {
		if _, ok := allowedSet[field]; !ok {
			return fmt.Errorf("unknown field")
		}
	}
	for _, field := range required {
		if _, ok := value[field]; !ok {
			return fmt.Errorf("missing field")
		}
	}
	return nil
}

func runtimeJSONValue(value any) bool {
	switch typed := value.(type) {
	case nil, bool, string:
		return true
	case json.Number:
		number, err := typed.Float64()
		return err == nil && !math.IsInf(number, 0) && !math.IsNaN(number)
	case float64:
		return !math.IsInf(typed, 0) && !math.IsNaN(typed)
	case []any:
		for _, member := range typed {
			if !runtimeJSONValue(member) {
				return false
			}
		}
		return true
	case map[string]any:
		for _, member := range typed {
			if !runtimeJSONValue(member) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func runtimeAlreadyCanonicalIdentifier(value any) bool {
	text, ok := value.(string)
	return ok && text != "" && utf8.ValidString(text) && len([]byte(text)) <= runtimeContextIdentifierMaxBytes
}

func runtimeAlreadyCanonicalText(value any) bool {
	text, ok := value.(string)
	if !ok {
		return false
	}
	for _, pattern := range runtimeSensitiveTextPatterns {
		if pattern.MatchString(text) {
			return false
		}
	}
	return true
}

func runtimeJSONBytes(value any) int {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return math.MaxInt
	}
	encoded := bytes.TrimSuffix(buffer.Bytes(), []byte{'\n'})
	encoded = bytes.ReplaceAll(encoded, []byte(`\u2028`), []byte("\u2028"))
	encoded = bytes.ReplaceAll(encoded, []byte(`\u2029`), []byte("\u2029"))
	return len(encoded)
}
