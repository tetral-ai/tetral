package agentruntimebridge

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

const (
	runtimeProviderMetadataMaxBytes = 16 * 1024
	runtimeToolOutputJSONMaxBytes   = 512 * 1024
	runtimeToolInputJSONMaxBytes    = 4 * 1024 * 1024
	runtimePreviewTextMaxBytes      = 8 * 1024
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

// Runtime owns declaration semantics and sanitizes transform-bearing values.
// Bridge accepts only the exact already-canonical wire shape, then adds durable
// identity in the surrounding transaction without rewriting declared values.
func validateRuntimeMessageCreate(create *bridgev1.RuntimeMessageCreate) (map[string]any, error) {
	if create == nil || create.GetMessageKind() == bridgev1.RuntimeMessageCreateKind_RUNTIME_MESSAGE_CREATE_KIND_UNSPECIFIED {
		return nil, status.Error(codes.InvalidArgument, "runtime message create identity is invalid")
	}
	message, err := decodeRuntimeDeclarationObject(create.GetMessageInfoJson())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "runtime message create info is invalid")
	}
	if err := requireRuntimeObjectFields(message,
		[]string{"role", "origin", "status"},
		[]string{"role", "origin", "status", "error", "finishReason", "usage", "responseId"},
	); err != nil {
		return nil, status.Error(codes.InvalidArgument, "runtime message create info is invalid")
	}
	if !runtimeStringIn(message["role"], "user", "assistant") ||
		!runtimeStringIn(message["origin"], "user", "agent", "runtime") ||
		!runtimeStringIn(message["status"], "streaming", "completed", "failed", "cancelled") {
		return nil, status.Error(codes.InvalidArgument, "runtime message create info is invalid")
	}
	if value, ok := message["responseId"]; ok && !runtimeNonEmptyString(value) {
		return nil, status.Error(codes.InvalidArgument, "runtime message create info is invalid")
	}
	if value, ok := message["finishReason"]; ok && !runtimeStringIn(value, "stop", "length", "content-filter", "tool-calls", "error", "cancelled", "other", "unknown") {
		return nil, status.Error(codes.InvalidArgument, "runtime message create info is invalid")
	}
	if value, ok := message["usage"]; ok {
		if err := validateRuntimeUsage(value); err != nil {
			return nil, status.Error(codes.InvalidArgument, "runtime message create info is invalid")
		}
	}
	if value, ok := message["error"]; ok {
		if err := validateRuntimeFailure(value); err != nil {
			return nil, status.Error(codes.InvalidArgument, "runtime message create info is not canonical")
		}
	}
	for _, part := range create.GetParts() {
		if _, err := validateRuntimePartCreate(part); err != nil {
			return nil, err
		}
	}
	return message, nil
}

// validateRuntimePartCreate is the single part-shape contract shared by new
// messages, incremental Assistant appends, and trailing request-end appends.
// Their independent lineage and transaction rules remain in their owning RPCs.
func validateRuntimePartCreate(create *bridgev1.RuntimePartCreate) (map[string]any, error) {
	if create == nil || !validRuntimePartKind(create.GetPartKind()) {
		return nil, status.Error(codes.InvalidArgument, "runtime part is invalid")
	}
	part, err := decodeRuntimeDeclarationObject(create.GetPartJson())
	if err != nil || part["type"] != create.GetPartKind() {
		return nil, status.Error(codes.InvalidArgument, "runtime part is invalid")
	}
	optionalTimes := []string{"startedAt", "completedAt"}
	switch create.GetPartKind() {
	case "text":
		if err := requireRuntimeObjectFields(part,
			[]string{"type", "text", "truncated", "status"},
			append([]string{"type", "text", "truncated", "status"}, optionalTimes...),
		); err != nil || !runtimeString(part["text"]) || !runtimeBool(part["truncated"]) || !runtimePartStatus(part["status"]) {
			return nil, status.Error(codes.InvalidArgument, "runtime text part is invalid")
		}
	case "reasoning":
		if err := requireRuntimeObjectFields(part,
			[]string{"type", "text", "truncated", "status"},
			append([]string{"type", "providerPartId", "providerMetadata", "text", "truncated", "status"}, optionalTimes...),
		); err != nil || !runtimeString(part["text"]) || !runtimeBool(part["truncated"]) || !runtimePartStatus(part["status"]) {
			return nil, status.Error(codes.InvalidArgument, "runtime reasoning part is invalid")
		}
		if value, ok := part["providerPartId"]; ok && !runtimeNonEmptyString(value) {
			return nil, status.Error(codes.InvalidArgument, "runtime reasoning part is invalid")
		}
		if value, ok := part["providerMetadata"]; ok {
			if _, ok := value.(map[string]any); !ok || !runtimeJSONValue(value) || runtimeJSONBytes(value) > runtimeProviderMetadataMaxBytes {
				return nil, status.Error(codes.InvalidArgument, "runtime reasoning metadata is invalid")
			}
		}
	case "tool":
		if err := requireRuntimeObjectFields(part,
			[]string{"type", "toolCallId", "toolName", "state"},
			append([]string{"type", "toolCallId", "toolName", "toolUseEventId", "toolEvent", "state"}, optionalTimes...),
		); err != nil || !runtimeNonEmptyString(part["toolCallId"]) || !runtimeNonEmptyString(part["toolName"]) {
			return nil, status.Error(codes.InvalidArgument, "runtime Tool part is invalid")
		}
		if value, ok := part["toolUseEventId"]; ok && !runtimeNonEmptyString(value) {
			return nil, status.Error(codes.InvalidArgument, "runtime Tool part is invalid")
		}
		if value, ok := part["toolEvent"]; ok {
			if err := validateRuntimeToolEvent(value); err != nil {
				return nil, status.Error(codes.InvalidArgument, "runtime Tool event is invalid")
			}
		}
		if err := validateRuntimeToolState(part["state"]); err != nil {
			return nil, status.Error(codes.InvalidArgument, "runtime Tool state is invalid")
		}
	case "step-start":
		if err := requireRuntimeObjectFields(part, []string{"type"}, []string{"type", "stepIndex"}); err != nil {
			return nil, status.Error(codes.InvalidArgument, "runtime step-start part is invalid")
		}
		if value, ok := part["stepIndex"]; ok && !runtimeNonNegativeInteger(value) {
			return nil, status.Error(codes.InvalidArgument, "runtime step-start part is invalid")
		}
	case "step-finish":
		if err := requireRuntimeObjectFields(part, []string{"type", "finishReason"}, []string{"type", "stepIndex", "finishReason", "usage"}); err != nil ||
			!runtimeStringIn(part["finishReason"], "stop", "length", "content-filter", "tool-calls", "error", "cancelled", "other", "unknown") {
			return nil, status.Error(codes.InvalidArgument, "runtime step-finish part is invalid")
		}
		if value, ok := part["stepIndex"]; ok && !runtimeNonNegativeInteger(value) {
			return nil, status.Error(codes.InvalidArgument, "runtime step-finish part is invalid")
		}
		if value, ok := part["usage"]; ok {
			if err := validateRuntimeUsage(value); err != nil {
				return nil, status.Error(codes.InvalidArgument, "runtime step-finish usage is invalid")
			}
		}
	}
	for _, field := range optionalTimes {
		if value, ok := part[field]; ok && !runtimeTimestamp(value) {
			return nil, status.Error(codes.InvalidArgument, "runtime part timestamp is invalid")
		}
	}
	return part, nil
}

func validateRuntimeUsage(value any) error {
	usage, ok := value.(map[string]any)
	if !ok || requireRuntimeObjectFields(usage,
		[]string{"inputTokens", "outputTokens", "reasoningTokens", "cacheReadTokens", "cacheWriteTokens"},
		[]string{"inputTokens", "outputTokens", "reasoningTokens", "cacheReadTokens", "cacheWriteTokens", "totalTokens", "unknownTokens", "providerUsageJson"},
	) != nil {
		return fmt.Errorf("invalid usage")
	}
	for _, field := range []string{"inputTokens", "outputTokens", "reasoningTokens", "cacheReadTokens", "cacheWriteTokens", "totalTokens", "unknownTokens"} {
		if member, exists := usage[field]; exists && !runtimeNonNegativeInteger(member) {
			return fmt.Errorf("invalid usage counter")
		}
	}
	if member, exists := usage["providerUsageJson"]; exists && !runtimeString(member) {
		return fmt.Errorf("invalid provider usage")
	}
	return nil
}

func validateRuntimeFailure(value any) error {
	failure, ok := value.(map[string]any)
	if !ok || requireRuntimeObjectFields(failure,
		[]string{"type", "code", "message", "retryable", "fatal"},
		[]string{"type", "code", "message", "retryable", "fatal", "retryStatus", "operation", "reason", "constraint", "status", "attemptedStatus", "messageId", "partId", "sessionId", "providerId", "modelId", "statusCode", "retryAfterMs"},
	) != nil || !runtimeStringIn(failure["type"], "provider", "message-store", "session-event-writer", "session-binding", "runtime") ||
		!runtimeStringIn(failure["code"],
			"credential_required", "platform_keys_exhausted", "context_overflow", "provider_request_invalid", "provider_plan_required", "provider_key_unavailable", "provider_quota_exhausted", "provider_auth_failed", "provider_rate_limited", "provider_quota_exceeded", "provider_context_overflow", "provider_model_not_found", "provider_invalid_request", "provider_tool_protocol_error", "provider_timeout", "provider_stream_error", "provider_unavailable", "provider_cancelled", "attachment_unavailable", "provider_unknown",
			"unavailable", "timeout", "conflict", "constraint_violation", "not_found", "serialization_failure", "schema_mismatch", "unknown", "ack_mismatch", "gateway_protocol_error", "gateway_stream_error", "gateway_unavailable", "runtime_invalid_sequence", "runtime_persistence_exhausted",
		) || !runtimeAlreadyCanonicalText(failure["message"]) || !runtimeBool(failure["retryable"]) || !runtimeBool(failure["fatal"]) {
		return fmt.Errorf("invalid failure")
	}
	for _, field := range []string{"constraint", "messageId", "partId", "sessionId", "providerId", "modelId"} {
		if member, exists := failure[field]; exists && !runtimeAlreadyCanonicalIdentifier(member) {
			return fmt.Errorf("uncanonical failure identity")
		}
	}
	if member, exists := failure["operation"]; exists && !runtimeStringIn(member, "commitInternalToolRepair") {
		return fmt.Errorf("invalid failure operation")
	}
	if member, exists := failure["reason"]; exists && !runtimeStringIn(member, "aborted", "bounded", "gateway_transport_completion_deadline", "runtime_contract_validation", "runtime_input_commit_exhausted", "runtime_shutdown", "timeout", "write_acknowledgement_mismatch") {
		return fmt.Errorf("invalid failure reason")
	}
	for _, field := range []string{"status", "attemptedStatus"} {
		if member, exists := failure[field]; exists && !runtimeStringIn(member, "completed", "failed", "cancelled") {
			return fmt.Errorf("invalid failure status")
		}
	}
	for _, field := range []string{"statusCode", "retryAfterMs"} {
		if member, exists := failure[field]; exists && !runtimeNonNegativeInteger(member) {
			return fmt.Errorf("invalid failure counter")
		}
	}
	if member, exists := failure["retryStatus"]; exists {
		statusValue, ok := member.(map[string]any)
		if !ok {
			return fmt.Errorf("invalid retry status")
		}
		kind, _ := statusValue["type"].(string)
		allowed := []string{"type"}
		required := []string{"type"}
		if kind == "retrying" {
			allowed = append(allowed, "attempt")
			required = append(required, "attempt")
		}
		if (kind != "retrying" && kind != "exhausted" && kind != "terminal") || requireRuntimeObjectFields(statusValue, required, allowed) != nil ||
			(kind == "retrying" && !runtimeNonNegativeInteger(statusValue["attempt"])) {
			return fmt.Errorf("invalid retry status")
		}
	}
	return nil
}

func validateRuntimeToolEvent(value any) error {
	event, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("invalid Tool event")
	}
	kind, _ := event["kind"].(string)
	if kind == "tool" {
		return requireRuntimeObjectFields(event, []string{"kind"}, []string{"kind"})
	}
	if kind == "mcp" && runtimeNonEmptyString(event["mcpServerName"]) {
		return requireRuntimeObjectFields(event, []string{"kind", "mcpServerName"}, []string{"kind", "mcpServerName"})
	}
	return fmt.Errorf("invalid Tool event")
}

func validateRuntimeToolState(value any) error {
	state, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("invalid Tool state")
	}
	statusValue, _ := state["status"].(string)
	switch statusValue {
	case "pending":
		return requireRuntimeObjectFields(state, []string{"status"}, []string{"status"})
	case "running":
		if requireRuntimeObjectFields(state, []string{"status", "input"}, []string{"status", "input"}) != nil {
			return fmt.Errorf("invalid running state")
		}
		return validateRuntimeBoundedJSON(state["input"])
	case "completed":
		if requireRuntimeObjectFields(state, []string{"status", "input", "output"}, []string{"status", "input", "output"}) != nil || validateRuntimeBoundedJSON(state["input"]) != nil || validateRuntimeBoundedText(state["output"]) != nil {
			return fmt.Errorf("invalid completed state")
		}
	case "error":
		if requireRuntimeObjectFields(state, []string{"status", "error"}, []string{"status", "input", "error"}) != nil || validateRuntimeToolError(state["error"]) != nil {
			return fmt.Errorf("invalid error state")
		}
		if member, exists := state["input"]; exists && validateRuntimeBoundedJSON(member) != nil {
			return fmt.Errorf("invalid error input")
		}
	case "cancelled":
		if requireRuntimeObjectFields(state, []string{"status"}, []string{"status", "input", "error"}) != nil {
			return fmt.Errorf("invalid cancelled state")
		}
		if member, exists := state["input"]; exists && validateRuntimeBoundedJSON(member) != nil {
			return fmt.Errorf("invalid cancelled input")
		}
		if member, exists := state["error"]; exists && validateRuntimeToolError(member) != nil {
			return fmt.Errorf("invalid cancelled error")
		}
	default:
		return fmt.Errorf("invalid Tool state")
	}
	return nil
}

func validateRuntimeBoundedJSON(value any) error {
	input, ok := value.(map[string]any)
	if !ok || requireRuntimeObjectFields(input, []string{"value", "preview", "truncated"}, []string{"value", "preview", "truncated"}) != nil ||
		!runtimeString(input["preview"]) || len([]byte(input["preview"].(string))) > runtimePreviewTextMaxBytes || !runtimeBool(input["truncated"]) ||
		!runtimeJSONValue(input["value"]) || runtimeJSONBytes(input["value"]) > runtimeToolInputJSONMaxBytes {
		return fmt.Errorf("invalid bounded JSON")
	}
	return nil
}

func validateRuntimeBoundedText(value any) error {
	output, ok := value.(map[string]any)
	if !ok || requireRuntimeObjectFields(output, []string{"text", "truncated"}, []string{"text", "truncated"}) != nil || !runtimeString(output["text"]) || !runtimeBool(output["truncated"]) {
		return fmt.Errorf("invalid bounded text")
	}
	if runtimeJSONBytes(map[string]any{"text": output["text"]}) > runtimeToolOutputJSONMaxBytes {
		return fmt.Errorf("bounded text exceeds limit")
	}
	return nil
}

func validateRuntimeToolError(value any) error {
	toolError, ok := value.(map[string]any)
	if !ok || requireRuntimeObjectFields(toolError, []string{"type", "message"}, []string{"type", "message", "retryable"}) != nil ||
		!runtimeAlreadyCanonicalIdentifier(toolError["type"]) || !runtimeAlreadyCanonicalText(toolError["message"]) {
		return fmt.Errorf("invalid Tool error")
	}
	if retryable, exists := toolError["retryable"]; exists && !runtimeBool(retryable) {
		return fmt.Errorf("invalid Tool error retryability")
	}
	return nil
}

func decodeRuntimeDeclarationObject(raw string) (map[string]any, error) {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	var value map[string]any
	if err := decoder.Decode(&value); err != nil || value == nil {
		return nil, fmt.Errorf("invalid object")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("trailing JSON")
	}
	return value, nil
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

func runtimeString(value any) bool { _, ok := value.(string); return ok }
func runtimeNonEmptyString(value any) bool {
	valueString, ok := value.(string)
	return ok && valueString != ""
}
func runtimeBool(value any) bool { _, ok := value.(bool); return ok }
func runtimePartStatus(value any) bool {
	return runtimeStringIn(value, "streaming", "completed", "failed", "cancelled")
}

func runtimeStringIn(value any, allowed ...string) bool {
	valueString, ok := value.(string)
	if !ok {
		return false
	}
	for _, candidate := range allowed {
		if valueString == candidate {
			return true
		}
	}
	return false
}

var runtimeJSONIntegerLexemePattern = regexp.MustCompile(`^(0|[1-9][0-9]*)(?:\.([0-9]+))?(?:[eE]([+-]?[0-9]+))?$`)

// Runtime declarations cross a JavaScript boundary, so integer acceptance is
// decided from the original JSON number lexeme. This preserves integral
// exponent notation while preventing float decoding from rounding a fraction
// or unsafe integer into Runtime's non-negative safe-integer domain.
func runtimeNonNegativeInteger(value any) bool {
	number, ok := value.(json.Number)
	if !ok {
		return false
	}
	parts := runtimeJSONIntegerLexemePattern.FindStringSubmatch(number.String())
	if parts == nil {
		return false
	}
	allDigits := parts[1] + parts[2]
	digits := strings.TrimLeft(allDigits, "0")
	if digits == "" {
		return true
	}
	exponent := int64(0)
	if parts[3] != "" {
		var err error
		exponent, err = strconv.ParseInt(parts[3], 10, 64)
		if err != nil {
			return false
		}
	}
	fractionDigits := int64(len(parts[2]))
	if exponent >= fractionDigits {
		trailingZeros := exponent - fractionDigits
		if trailingZeros > 16 || int64(len(digits))+trailingZeros > 16 {
			return false
		}
		digits += strings.Repeat("0", int(trailingZeros))
	} else {
		// A nonzero value whose decimal point falls before all of its digits is
		// fractional. Otherwise every discarded digit must be zero.
		if exponent < fractionDigits-int64(len(allDigits)) {
			return false
		}
		discard := int(fractionDigits - exponent)
		if discard > len(allDigits) || allDigits[len(allDigits)-discard:] != strings.Repeat("0", discard) {
			return false
		}
		digits = strings.TrimLeft(allDigits[:len(allDigits)-discard], "0")
		if digits == "" {
			return true
		}
	}
	const maxSafeIntegerLexeme = "9007199254740991"
	return len(digits) < len(maxSafeIntegerLexeme) ||
		(len(digits) == len(maxSafeIntegerLexeme) && digits <= maxSafeIntegerLexeme)
}

func runtimeTimestamp(value any) bool {
	text, ok := value.(string)
	if !ok {
		return false
	}
	_, err := time.Parse(time.RFC3339Nano, text)
	return err == nil
}

func runtimeJSONValue(value any) bool {
	switch typed := value.(type) {
	case nil, bool, string:
		return true
	case json.Number:
		number, err := typed.Float64()
		return err == nil && !math.IsInf(number, 0) && !math.IsNaN(number)
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
	return runtimeNonEmptyString(value) && runtimeAlreadyCanonicalText(value)
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
	// encoding/json escapes these two valid JSON characters for JavaScript
	// embedding safety. Runtime's JSON.stringify wire contract emits their
	// UTF-8 bytes directly, so measure that same representation at the bound.
	encoded = bytes.ReplaceAll(encoded, []byte(`\u2028`), []byte("\u2028"))
	encoded = bytes.ReplaceAll(encoded, []byte(`\u2029`), []byte("\u2029"))
	return len(encoded)
}
