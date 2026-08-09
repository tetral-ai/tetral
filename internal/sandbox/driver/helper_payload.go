package driver

import (
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
)

const (
	helperMaxBlockingWaitMS = 50000
	helperMinYieldWaitMS    = 250
	helperMaxYieldWaitMS    = 30000
	bashDefaultTimeoutMS    = 120000
	bashMaxTimeoutMS        = 600000
)

type helperRunToolComposition struct {
	pollUntilTerminal bool
}

func helperSubcommandForToolName(toolName string) (string, error) {
	switch toolName {
	case "Bash", "bash", "exec_command":
		return helperSubcommandExec, nil
	case "Read", "read":
		return helperSubcommandRead, nil
	case "Write", "write":
		return helperSubcommandWrite, nil
	case "Edit", "edit":
		return helperSubcommandEdit, nil
	case "apply_patch":
		return helperSubcommandApplyPatch, nil
	case "Grep", "grep":
		return helperSubcommandGrep, nil
	case "Glob", "glob":
		return helperSubcommandGlob, nil
	case "view_image":
		return helperSubcommandViewImage, nil
	default:
		return "", fmt.Errorf("unsupported sandbox tool %q", toolName)
	}
}

type preHelperToolResult struct {
	resultJSON string
}

func (e *preHelperToolResult) Error() string {
	return "sandbox tool resolved before helper execution"
}

func helperRunToolInput(helperCommand string, toolName string, rawInputJSON string, toolUseEventID string) (any, error) {
	input, _, err := helperRunToolInputForInvocation(helperCommand, toolName, rawInputJSON, toolUseEventID)
	return input, err
}

func helperRunToolInputForInvocation(helperCommand string, toolName string, rawInputJSON string, toolUseEventID string) (any, helperRunToolComposition, error) {
	if helperCommand == helperSubcommandApplyPatch {
		var object map[string]any
		if err := decodeJSONObject(rawInputJSON, &object); err == nil && len(object) == 1 {
			if patch, ok := object["patch"].(string); ok {
				return map[string]any{"patch": patch}, helperRunToolComposition{}, nil
			}
		}
		return nil, helperRunToolComposition{}, errors.New("apply_patch input must be exactly one string patch field")
	}
	if strings.TrimSpace(rawInputJSON) == "" {
		return map[string]any{}, helperRunToolComposition{}, nil
	}
	var object map[string]any
	if err := decodeJSONObject(rawInputJSON, &object); err != nil {
		if !json.Valid([]byte(rawInputJSON)) {
			return nil, helperRunToolComposition{}, errors.New("tool input must be JSON")
		}
		return json.RawMessage(rawInputJSON), helperRunToolComposition{}, nil
	}
	var composition helperRunToolComposition
	switch helperCommand {
	case helperSubcommandExec:
		var err error
		composition, err = adaptExecInput(object, toolName, toolUseEventID)
		if err != nil {
			return nil, helperRunToolComposition{}, err
		}
	case helperSubcommandRead, helperSubcommandWrite, helperSubcommandEdit:
		adaptFilePathInput(object)
	}
	return object, composition, nil
}

func helperStdinInput(taskID string, rawInputJSON string) (map[string]any, error) {
	if taskID == "" {
		return nil, errors.New("command task id is required")
	}
	input := map[string]any{"task_id": taskID}
	rawInputJSON = strings.TrimSpace(rawInputJSON)
	if rawInputJSON == "" {
		rawInputJSON = "{}"
	}
	var raw map[string]any
	if err := decodeJSONObject(rawInputJSON, &raw); err != nil {
		return nil, errors.New("stdin input must be a JSON object")
	}
	if chars, ok := raw["chars"].(string); ok {
		input["chars"] = chars
	}
	if wait, ok := helperWaitMSFromInput(raw); ok {
		input["wait_ms"] = wait
	}
	if maxOutputTokens, ok := positiveIntFromJSONValue(raw["max_output_tokens"]); ok {
		input["max_output_tokens"] = maxOutputTokens
	}
	if writeSeq, ok := positiveIntFromJSONValue(raw["write_seq"]); ok {
		input["write_seq"] = writeSeq
	}
	return input, nil
}

func helperWaitMSFromInput(input map[string]any) (int, bool) {
	value, ok := input["yield_time_ms"]
	if !ok {
		return 0, false
	}
	switch typed := value.(type) {
	case float64:
		return clampHelperYieldWaitMS(int(typed)), true
	case int:
		return clampHelperYieldWaitMS(typed), true
	case json.Number:
		parsed, err := typed.Int64()
		return clampHelperYieldWaitMS(int(parsed)), err == nil
	default:
		return 0, false
	}
}

func clampHelperYieldWaitMS(value int) int {
	if value < helperMinYieldWaitMS {
		return helperMinYieldWaitMS
	}
	if value > helperMaxYieldWaitMS {
		return helperMaxYieldWaitMS
	}
	return value
}

func newHelperPayload(target ToolTarget, helperCommand string, toolUseEventID string, input any) (map[string]any, error) {
	if toolUseEventID == "" {
		return nil, errors.New("helper payload tool_use_event_id is required")
	}
	roots, err := helperRoots(target.ResourceRootsJSON)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"schema_version":    1,
		"tool":              helperCommand,
		"tool_use_event_id": toolUseEventID,
		"workspace_root":    helperWorkspaceRoot,
		"roots":             roots,
		"limits":            helperLimits(helperCommand, input),
		"input":             input,
	}, nil
}

func helperRoots(resourceRootsJSON string) ([]protocol.Root, error) {
	roots := []protocol.Root{
		{Path: helperWorkspaceRoot, Mode: "read_write"},
		{Path: helperSessionUploadsRoot, Mode: "read_write"},
		{Path: helperSessionOutputsRoot, Mode: "read_write"},
		{Path: helperMemoryRoot, Mode: "read"},
		{Path: helperSkillsRoot, Mode: "read"},
	}
	if strings.TrimSpace(resourceRootsJSON) == "" {
		return roots, nil
	}
	var resourceRoots []protocol.Root
	if err := json.Unmarshal([]byte(resourceRootsJSON), &resourceRoots); err != nil {
		return nil, fmt.Errorf("resource roots snapshot is invalid: %w", err)
	}
	seen := map[string]struct{}{}
	for _, root := range roots {
		seen[root.Path] = struct{}{}
	}
	for _, root := range resourceRoots {
		cleaned := path.Clean(root.Path)
		if !path.IsAbs(cleaned) || cleaned == "/" {
			return nil, fmt.Errorf("resource root path %q is invalid", root.Path)
		}
		if root.Mode != "read" {
			return nil, fmt.Errorf("resource root %q mode %q is invalid", root.Path, root.Mode)
		}
		if _, ok := seen[cleaned]; ok {
			continue
		}
		seen[cleaned] = struct{}{}
		roots = append(roots, protocol.Root{Path: cleaned, Mode: "read"})
	}
	return roots, nil
}

func adaptExecInput(input map[string]any, toolName string, taskID string) (helperRunToolComposition, error) {
	if toolName == "exec_command" {
		for field := range input {
			switch field {
			case "cmd", "workdir", "yield_time_ms", "max_output_tokens":
			default:
				return helperRunToolComposition{}, unsupportedArgumentResult(field, "exec_command input does not allow this argument")
			}
		}
	}
	composition := helperRunToolComposition{}
	if toolName == "Bash" || toolName == "bash" {
		for field := range input {
			switch field {
			case "command", "cwd", "timeout", "run_in_background":
			default:
				return helperRunToolComposition{}, unsupportedArgumentResult(field, "Bash input does not allow this argument")
			}
		}
		command, ok := input["command"].(string)
		if !ok {
			return helperRunToolComposition{}, unsupportedArgumentResult("command", "Bash command must be a non-null string")
		}
		input["cmd"] = command
		delete(input, "command")
		if cwd, present := input["cwd"]; present {
			if _, ok := cwd.(string); !ok {
				return helperRunToolComposition{}, unsupportedArgumentResult("cwd", "Bash cwd must be a non-null string")
			}
		}
		runInBackground := false
		if background, present := input["run_in_background"]; present {
			var ok bool
			runInBackground, ok = background.(bool)
			if !ok {
				return helperRunToolComposition{}, unsupportedArgumentResult("run_in_background", "Bash run_in_background must be a non-null boolean")
			}
		}
		waitMS := bashDefaultTimeoutMS
		if rawTimeout, present := input["timeout"]; present {
			explicitWaitMS, ok := strictJSONInteger(rawTimeout)
			if !ok || explicitWaitMS < 0 || explicitWaitMS > bashMaxTimeoutMS {
				return helperRunToolComposition{}, unsupportedArgumentResult("timeout", "Bash timeout must be an integer from 0 through 600000 milliseconds")
			}
			waitMS = explicitWaitMS
		}
		delete(input, "timeout")
		if runInBackground {
			input["wait_ms"] = helperMinYieldWaitMS
			input["on_wait_expiry"] = "detach"
			input["task_lifetime_ms"] = waitMS
		} else if waitMS > helperMaxBlockingWaitMS {
			input["wait_ms"] = helperMaxBlockingWaitMS
			input["on_wait_expiry"] = "detach"
			input["task_lifetime_ms"] = waitMS
			composition.pollUntilTerminal = true
		} else if _, hasWaitMS := input["wait_ms"]; !hasWaitMS {
			input["wait_ms"] = waitMS
		}
		if background, ok := input["run_in_background"].(bool); ok {
			if background {
				input["on_wait_expiry"] = "detach"
			} else if _, hasMode := input["on_wait_expiry"]; !hasMode {
				input["on_wait_expiry"] = "kill"
			}
			delete(input, "run_in_background")
		}
	}
	if workdir, ok := input["workdir"]; ok {
		if _, hasCWD := input["cwd"]; !hasCWD {
			input["cwd"] = workdir
		}
		delete(input, "workdir")
	}
	if waitMS, ok := helperWaitMSFromInput(input); ok {
		if _, hasWaitMS := input["wait_ms"]; !hasWaitMS {
			input["wait_ms"] = waitMS
		}
		delete(input, "yield_time_ms")
	}
	if _, hasMode := input["on_wait_expiry"]; !hasMode && (toolName == "exec_command" || toolName == "") {
		input["on_wait_expiry"] = "detach"
	}
	if input["on_wait_expiry"] == "detach" {
		if _, hasTaskID := input["task_id"]; !hasTaskID && taskID != "" {
			input["task_id"] = taskID
		}
	}
	return composition, nil
}

func strictJSONInteger(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, true
	case json.Number:
		parsed, err := typed.Int64()
		if err != nil || int64(int(parsed)) != parsed {
			return 0, false
		}
		return int(parsed), true
	default:
		return 0, false
	}
}

func unsupportedArgumentResult(argument string, message string) error {
	result, err := json.Marshal(map[string]any{
		"status":     "tool_error",
		"error_code": "unsupported_argument",
		"argument":   argument,
		"message":    message,
		"retryable":  false,
	})
	if err != nil {
		return err
	}
	return &preHelperToolResult{resultJSON: string(result)}
}

func adaptFilePathInput(input map[string]any) {
	filePath, hasFilePath := input["file_path"]
	if !hasFilePath {
		delete(input, "path")
		return
	}
	input["path"] = filePath
}

func helperLimits(helperCommand string, input any) protocol.Limits {
	limits := protocol.Limits{VisibleBytes: helperVisibleBytes, VisibleLines: helperVisibleLines}
	if helperCommand == helperSubcommandRead {
		limits.VisibleBytes = 200_000
	}
	inputMap, ok := input.(map[string]any)
	if !ok {
		return limits
	}
	tokens, ok := positiveIntFromJSONValue(inputMap["max_output_tokens"])
	if !ok {
		return limits
	}
	visibleBytes := tokens * 4
	if visibleBytes > 0 && visibleBytes < limits.VisibleBytes {
		limits.VisibleBytes = visibleBytes
	}
	return limits
}

func positiveIntFromJSONValue(value any) (int, bool) {
	switch typed := value.(type) {
	case float64:
		if typed <= 0 {
			return 0, false
		}
		return int(typed), true
	case int:
		return typed, typed > 0
	case json.Number:
		parsed, err := typed.Int64()
		return int(parsed), err == nil && parsed > 0
	default:
		return 0, false
	}
}

func decodeJSONObject(raw string, out *map[string]any) error {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if *out == nil {
		return errors.New("json object is null")
	}
	return nil
}
