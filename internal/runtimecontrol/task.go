package runtimecontrol

import (
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"
)

func nullableSafeIntegerRaw(raw json.RawMessage, field string, nonNegative bool) (any, error) {
	value, err := nullableIntegerRaw(raw, field)
	if err != nil || value == nil {
		return value, err
	}
	integer := value.(int64)
	if integer < -9007199254740991 || integer > 9007199254740991 || (nonNegative && integer < 0) {
		return nil, errors.New(field + " must be a safe integer or null")
	}
	return integer, nil
}

func TaskNotificationRejectionCode(errorCode string) bool {
	switch errorCode {
	case "task_notification_result_invalid", "task_notification_message_invalid", "task_notification_payload_mismatch", "task_notification_stale":
		return true
	default:
		return false
	}
}

func ValidBackgroundTaskTerminalStatus(status string) bool {
	switch status {
	case "completed", "failed", "cancelled", "expired", "unknown_outcome":
		return true
	default:
		return false
	}
}

func CanonicalTaskNotificationPayloadJSON(taskID string, sourceToolUseEventID string, terminalStatus string, resultJSON string) (string, error) {
	value, err := canonicalTaskNotificationPayloadValue(taskID, sourceToolUseEventID, terminalStatus, resultJSON)
	if err != nil {
		return "", err
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func canonicalTaskNotificationPayloadValue(taskID string, sourceToolUseEventID string, terminalStatus string, resultJSON string) (map[string]any, error) {
	if taskID == "" || sourceToolUseEventID == "" {
		return nil, errors.New("task notification source identity is incomplete")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal([]byte(resultJSON), &object); err != nil || object == nil {
		return nil, errors.New("task notification result must be a JSON object")
	}
	factObject := object
	if rawResult, ok := object["result"]; ok {
		var nested map[string]json.RawMessage
		if err := json.Unmarshal(rawResult, &nested); err == nil && nested != nil {
			factObject = nested
		}
	}
	canonical := map[string]any{
		"task_id":                  taskID,
		"source_tool_use_event_id": sourceToolUseEventID,
		"status":                   RuntimeTaskNotificationStatus(terminalStatus),
	}
	if canonical["status"] == "" {
		return nil, errors.New("task notification result status is invalid")
	}
	if raw, ok := factObject["exit_code"]; ok {
		value, err := nullableSafeIntegerRaw(raw, "task notification result exit_code", false)
		if err != nil {
			return nil, err
		}
		canonical["exit_code"] = value
	}
	for _, field := range []string{"stdout", "stderr"} {
		raw, ok := factObject[field]
		if !ok {
			return nil, errors.New("task notification result " + field + " is required")
		}
		stream, err := canonicalTaskNotificationStream(raw, "task notification result "+field)
		if err != nil {
			return nil, err
		}
		canonical[field] = stream
	}
	if err := fitTaskNotificationPayload(canonical); err != nil {
		return nil, err
	}
	return canonical, nil
}

func fitTaskNotificationPayload(payload map[string]any) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if len(encoded) <= RuntimeTaskNotificationPayloadMaxBytes {
		return nil
	}
	stdout, stdoutOK := payload["stdout"].(map[string]any)
	stderr, stderrOK := payload["stderr"].(map[string]any)
	if !stdoutOK || !stderrOK {
		return errors.New("task notification streams are invalid")
	}
	stdoutText, stdoutOK := stdout["text"].(string)
	stderrText, stderrOK := stderr["text"].(string)
	if !stdoutOK || !stderrOK {
		return errors.New("task notification stream text is invalid")
	}
	stdoutBytes := len([]byte(stdoutText))
	stderrBytes := len([]byte(stderrText))
	bestStdout, bestStderr := "", ""
	bestFound := false
	for low, high := 0, stdoutBytes+stderrBytes; low <= high; {
		candidateBytes := low + (high-low)/2
		stdoutBudget, stderrBudget := splitTaskNotificationBudget(candidateBytes, stdoutBytes, stderrBytes)
		candidateStdout := taskNotificationHeadTail(stdoutText, stdoutBudget)
		candidateStderr := taskNotificationHeadTail(stderrText, stderrBudget)
		stdout["text"] = candidateStdout
		stderr["text"] = candidateStderr
		candidate, marshalErr := json.Marshal(payload)
		if marshalErr != nil {
			return marshalErr
		}
		if len(candidate) <= RuntimeTaskNotificationPayloadMaxBytes {
			bestStdout, bestStderr, bestFound = candidateStdout, candidateStderr, true
			low = candidateBytes + 1
		} else {
			high = candidateBytes - 1
		}
	}
	if !bestFound {
		return errors.New("task notification metadata exceeds runtime payload limit")
	}
	stdout["text"] = bestStdout
	stderr["text"] = bestStderr
	if bestStdout != stdoutText {
		stdout["truncated"] = true
	}
	if bestStderr != stderrText {
		stderr["truncated"] = true
	}
	return nil
}

func splitTaskNotificationBudget(total int, stdoutBytes int, stderrBytes int) (int, int) {
	visibleBytes := stdoutBytes + stderrBytes
	if visibleBytes == 0 || total <= 0 {
		return 0, 0
	}
	stdoutBudget := total * stdoutBytes / visibleBytes
	if stdoutBytes > 0 && stdoutBudget == 0 {
		stdoutBudget = 1
	}
	if stdoutBudget > total {
		stdoutBudget = total
	}
	return stdoutBudget, total - stdoutBudget
}

func taskNotificationHeadTail(value string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len([]byte(value)) <= maxBytes {
		return value
	}
	headBudget := maxBytes / 2
	tailBudget := maxBytes - headBudget
	headEnd := 0
	for index := range value {
		if index > headBudget {
			break
		}
		headEnd = index
	}
	if headBudget >= len(value) {
		headEnd = len(value)
	}
	tailStart := len(value)
	used := 0
	for tailStart > headEnd {
		_, size := utf8.DecodeLastRuneInString(value[:tailStart])
		if size == 0 || used+size > tailBudget {
			break
		}
		used += size
		tailStart -= size
	}
	return value[:headEnd] + value[tailStart:]
}

func canonicalTaskNotificationStream(raw json.RawMessage, field string) (map[string]any, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil || object == nil {
		return nil, errors.New(field + " must be a JSON object")
	}
	text, err := requiredStringRaw(object["text"], field+" text")
	if err != nil {
		return nil, err
	}
	truncated, err := requiredBoolRaw(object["truncated"], field+" truncated")
	if err != nil {
		return nil, err
	}
	canonical := map[string]any{
		"text":      text,
		"truncated": truncated,
	}
	for _, optional := range []struct {
		output string
		input  string
	}{
		{output: "original_bytes", input: "original_bytes"},
		{output: "original_bytes", input: "total_bytes"},
		{output: "original_lines", input: "original_lines"},
		{output: "original_lines", input: "total_lines"},
	} {
		if _, exists := canonical[optional.output]; exists {
			continue
		}
		rawOptional, ok := object[optional.input]
		if !ok {
			continue
		}
		value, err := nullableSafeIntegerRaw(rawOptional, field+" "+optional.input, true)
		if err != nil {
			return nil, err
		}
		canonical[optional.output] = value
	}
	return canonical, nil
}

func requiredStringRaw(raw json.RawMessage, field string) (string, error) {
	var value string
	if len(raw) == 0 || strings.TrimSpace(string(raw)) == "null" || json.Unmarshal(raw, &value) != nil {
		return "", errors.New(field + " must be a string")
	}
	return value, nil
}

func requiredBoolRaw(raw json.RawMessage, field string) (bool, error) {
	var value bool
	if len(raw) == 0 || strings.TrimSpace(string(raw)) == "null" || json.Unmarshal(raw, &value) != nil {
		return false, errors.New(field + " must be a boolean")
	}
	return value, nil
}

func nullableIntegerRaw(raw json.RawMessage, field string) (any, error) {
	if strings.TrimSpace(string(raw)) == "null" {
		return nil, nil
	}
	var value int64
	if len(raw) == 0 || json.Unmarshal(raw, &value) != nil {
		return nil, errors.New(field + " must be an integer or null")
	}
	return value, nil
}
