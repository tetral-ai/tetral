package driver

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/daytonaio/daytona/libs/sdk-go/pkg/daytona"
	daytonaerrors "github.com/daytonaio/daytona/libs/sdk-go/pkg/errors"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/options"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/types"

	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
)

func (e *DaytonaHelperExecutor) runToolLifecycleForTest(ctx context.Context, invocation ToolInvocation) (ToolExecution, error) {
	prepared, err := e.PrepareTool(ctx, invocation)
	if err != nil {
		return ToolExecution{}, err
	}
	result, err := e.SubmitPreparedTool(ctx, prepared)
	for err == nil && result.ForegroundObservation != nil {
		result, err = e.ObserveForegroundTool(ctx, *result.ForegroundObservation)
	}
	return result, err
}

func TestDaytonaTransientRetryIsBoundedAndStatusScoped(t *testing.T) {
	t.Run("rate limit then success", func(t *testing.T) {
		attempts := 0
		var waits []time.Duration
		got, err := retryDaytonaTransient(context.Background(), func() (string, error) {
			attempts++
			if attempts < 3 {
				return "", daytonaerrors.NewDaytonaRateLimitError("busy", nil)
			}
			return "ok", nil
		}, func(_ context.Context, delay time.Duration) error {
			waits = append(waits, delay)
			return nil
		})
		if err != nil || got != "ok" || attempts != 3 {
			t.Fatalf("retry result = %q, %v attempts=%d; want ok after 3", got, err, attempts)
		}
		if !reflect.DeepEqual(waits, []time.Duration{100 * time.Millisecond, 200 * time.Millisecond}) {
			t.Fatalf("retry waits = %v", waits)
		}
	})

	t.Run("server failures stop at bound", func(t *testing.T) {
		attempts := 0
		_, err := retryDaytonaTransient(context.Background(), func() (string, error) {
			attempts++
			return "", daytonaerrors.NewDaytonaServerError("down", http.StatusServiceUnavailable, nil)
		}, func(context.Context, time.Duration) error { return nil })
		if err == nil || attempts != 3 {
			t.Fatalf("bounded retries err=%v attempts=%d; want error after 3", err, attempts)
		}
	})

	t.Run("deterministic errors do not retry", func(t *testing.T) {
		attempts := 0
		_, err := retryDaytonaTransient(context.Background(), func() (string, error) {
			attempts++
			return "", daytonaerrors.NewDaytonaValidationError("bad", nil)
		}, func(context.Context, time.Duration) error { return nil })
		if err == nil || attempts != 1 {
			t.Fatalf("deterministic retry err=%v attempts=%d; want one", err, attempts)
		}
	})
}

func TestDaytonaHelperUploadRetryReplaysTheCompletePayload(t *testing.T) {
	client := newRecordingMemoryProjectionClient()
	client.fileSystem.uploadErrors = []error{
		daytonaerrors.NewDaytonaRateLimitError("busy", nil),
		nil,
	}
	client.process.results = []string{
		"",
		`{"schema_version":1,"tool":"read","status":"success","truncated":false,"error":null,"result":{"content":"ok"}}`,
	}
	executor := NewDaytonaHelperExecutorForClient(client)

	result, err := executor.runToolLifecycleForTest(context.Background(), ToolInvocation{
		Target:         ToolTarget{ProviderSandboxID: "provider_sandbox"},
		ToolUseEventID: "evt_retry_upload",
		ToolName:       "Read",
		InputJSON:      `{"file_path":"/workspace/file.txt"}`,
	})
	if err != nil {
		t.Fatalf("RunTool after transient upload failure: %v", err)
	}
	if !strings.Contains(result.ResultJSON, `"content":"ok"`) {
		t.Fatalf("RunTool result = %s; want helper success", result.ResultJSON)
	}
	if len(client.fileSystem.uploads) != 2 {
		t.Fatalf("upload attempts = %d; want 2", len(client.fileSystem.uploads))
	}
	if client.fileSystem.uploads[0].body == "" || client.fileSystem.uploads[0].body != client.fileSystem.uploads[1].body {
		t.Fatalf("retried upload bodies differ: first=%q second=%q", client.fileSystem.uploads[0].body, client.fileSystem.uploads[1].body)
	}
}

func TestDaytonaHelperDoesNotReplayThePayloadConsumingCommand(t *testing.T) {
	client := newRecordingMemoryProjectionClient()
	serverErr := daytonaerrors.NewDaytonaServerError("response lost", http.StatusServiceUnavailable, nil)
	client.process.errors = []error{nil, serverErr}
	executor := NewDaytonaHelperExecutorForClient(client)

	_, err := executor.runToolLifecycleForTest(context.Background(), ToolInvocation{
		Target:         ToolTarget{ProviderSandboxID: "provider_sandbox"},
		ToolUseEventID: "evt_single_execute",
		ToolName:       "Read",
		InputJSON:      `{"file_path":"/workspace/file.txt"}`,
	})
	if !errors.Is(err, serverErr) {
		t.Fatalf("RunTool err = %v; want original helper command error", err)
	}
	if len(client.process.commands) != 2 {
		t.Fatalf("process commands = %d; want permission setup plus one payload-consuming helper command", len(client.process.commands))
	}
	if !strings.Contains(client.process.commands[1], "--payload") {
		t.Fatalf("final command = %q; want helper payload command", client.process.commands[1])
	}
}

func TestDaytonaHelperPreparationDoesNotInvokeUserTool(t *testing.T) {
	client := newRecordingMemoryProjectionClient()
	client.process.results = []string{
		"",
		`{"schema_version":1,"tool":"read","status":"success","truncated":false,"error":null,"result":{"content":"ok"}}`,
	}
	executor := NewDaytonaHelperExecutorForClient(client)
	prepared, err := executor.PrepareTool(context.Background(), ToolInvocation{
		Target:         ToolTarget{ProviderSandboxID: "provider_sandbox"},
		ToolUseEventID: "evt_prepare_boundary",
		ToolName:       "Read",
		InputJSON:      `{"file_path":"/workspace/file.txt"}`,
	})
	if err != nil {
		t.Fatalf("PrepareTool: %v", err)
	}
	if len(client.process.commands) != 1 || strings.Contains(client.process.commands[0], "--payload") {
		t.Fatalf("daytona commands = %v; want only permission freeze", client.process.commands)
	}
	if _, err := executor.SubmitPreparedTool(context.Background(), prepared); err != nil {
		t.Fatalf("SubmitPreparedTool: %v", err)
	}
	if len(client.process.commands) != 2 || !strings.Contains(client.process.commands[1], "--payload") {
		t.Fatalf("commands after execution = %v; want one user-facing helper invocation", client.process.commands)
	}
}

func TestDaytonaPreparedToolDoesNotReinspectProviderBeforeSubmission(t *testing.T) {
	client := newRecordingMemoryProjectionClient()
	client.process.results = []string{
		"",
		`{"schema_version":1,"tool":"read","status":"success","truncated":false,"error":null,"result":{"content":"ok"}}`,
	}
	executor := NewDaytonaHelperExecutorForClient(client)
	prepared, err := executor.PrepareTool(context.Background(), ToolInvocation{
		Target:         ToolTarget{ProviderSandboxID: "provider_sandbox"},
		ToolUseEventID: "evt_submission_boundary",
		ToolName:       "Read",
		InputJSON:      `{"file_path":"/workspace/file.txt"}`,
	})
	if err != nil {
		t.Fatalf("PrepareTool: %v", err)
	}
	client.err = errors.New("provider inspection unavailable after authorization")
	if _, err := executor.SubmitPreparedTool(context.Background(), prepared); err != nil {
		t.Fatalf("SubmitPreparedTool performed a second provider inspection: %v", err)
	}
	if len(client.process.commands) != 2 || !strings.Contains(client.process.commands[1], "--payload") {
		t.Fatalf("commands after execution = %v; want exactly one prepared user command", client.process.commands)
	}
}

func TestDaytonaToolPreparationPreservesTransientPreSubmissionFailure(t *testing.T) {
	client := newRecordingMemoryProjectionClient()
	client.err = daytonaerrors.NewDaytonaRateLimitError("busy", nil)
	executor := NewDaytonaHelperExecutorForClient(client)
	_, err := executor.PrepareTool(context.Background(), ToolInvocation{
		Target:         ToolTarget{ProviderSandboxID: "provider_sandbox"},
		ToolUseEventID: "evt_transient_pre_submission",
		ToolName:       "Read",
		InputJSON:      `{"file_path":"/workspace/file.txt"}`,
	})
	var providerErr *sandbox.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Stage != sandbox.StageExecuteTool ||
		providerErr.Kind != sandbox.ProviderErrorUnavailable || !providerErr.Retryable {
		t.Fatalf("PrepareTool error = %T %v; want retryable pre-submission provider failure", err, err)
	}
}

func TestStripProviderMetadataFromResultRemovesGenericProviderMetadata(t *testing.T) {
	result := stripProviderMetadataFromResult(`{"status":"completed","provider_metadata":{"raw":"secret"},"nested":{"provider_metadata_json":"{\"raw\":\"secret\"}","items":[{"provider_command_id":"cmd_1","text":"ok"}]}}`)
	for _, forbidden := range []string{"provider_metadata", "provider_metadata_json", "provider_command_id", "secret", "cmd_1"} {
		if strings.Contains(result, forbidden) {
			t.Fatalf("provider metadata leaked %q in %s", forbidden, result)
		}
	}
	if !strings.Contains(result, `"text":"ok"`) {
		t.Fatalf("safe result content was removed: %s", result)
	}
}

func TestHelperSubcommandForToolNameUsesContractVocabulary(t *testing.T) {
	tests := map[string]string{
		"Bash":         "exec",
		"exec_command": "exec",
		"Read":         "read",
		"Write":        "write",
		"Edit":         "edit",
		"apply_patch":  "apply_patch",
		"Grep":         "grep",
		"Glob":         "glob",
		"view_image":   "view_image",
	}
	for toolName, want := range tests {
		got, err := helperSubcommandForToolName(toolName)
		if err != nil || got != want {
			t.Fatalf("helperSubcommandForToolName(%q) = %q, %v; want %q", toolName, got, err, want)
		}
	}
	if _, err := helperSubcommandForToolName("capture_outputs"); err == nil {
		t.Fatal("helperSubcommandForToolName accepted capture_outputs; want unsupported")
	}
}

func TestNewHelperPayloadIncludesContractRootsAndResourceSnapshot(t *testing.T) {
	input, err := helperRunToolInput("read", "Read", `{"file_path":"/workspace/data/report.csv"}`, "evt_tool")
	if err != nil {
		t.Fatalf("helperRunToolInput read: %v", err)
	}
	payload, err := newHelperPayload(ToolTarget{
		ResourceRootsJSON: `[{"path":"/workspace/data/report.csv","mode":"read"},{"path":"/mnt/session/uploads/file.csv","mode":"read"}]`,
	}, "read", "evt_tool", input)
	if err != nil {
		t.Fatalf("newHelperPayload: %v", err)
	}
	if payload["schema_version"] != 1 || payload["tool"] != "read" || payload["tool_use_event_id"] != "evt_tool" || payload["workspace_root"] != "/workspace" {
		t.Fatalf("payload common fields = %#v; want helper contract envelope", payload)
	}
	payloadInput, ok := payload["input"].(map[string]any)
	if !ok || payloadInput["path"] != "/workspace/data/report.csv" {
		t.Fatalf("payload input = %#v; want Read file_path adapted to path", payload["input"])
	}
	roots, ok := payload["roots"].([]protocol.Root)
	if !ok {
		t.Fatalf("payload roots type = %T; want []protocol.Root", payload["roots"])
	}
	want := []protocol.Root{
		{Path: "/workspace", Mode: "read_write"},
		{Path: "/mnt/session/uploads", Mode: "read_write"},
		{Path: "/mnt/session/outputs", Mode: "read_write"},
		{Path: "/mnt/memory", Mode: "read"},
		{Path: "/skills", Mode: "read"},
		{Path: "/workspace/data/report.csv", Mode: "read"},
		{Path: "/mnt/session/uploads/file.csv", Mode: "read"},
	}
	if gotJSON, _ := json.Marshal(roots); string(gotJSON) != `[{"path":"/workspace","mode":"read_write"},{"path":"/mnt/session/uploads","mode":"read_write"},{"path":"/mnt/session/outputs","mode":"read_write"},{"path":"/mnt/memory","mode":"read"},{"path":"/skills","mode":"read"},{"path":"/workspace/data/report.csv","mode":"read"},{"path":"/mnt/session/uploads/file.csv","mode":"read"}]` {
		t.Fatalf("payload roots = %+v; want %+v", roots, want)
	}
	limits, ok := payload["limits"].(protocol.Limits)
	if !ok || limits.VisibleBytes != 200_000 || limits.VisibleLines != 2000 {
		t.Fatalf("payload limits = %#v; want read helper visible caps", payload["limits"])
	}
}

func TestRuntimeUserShellCommandUsesConfiguredRuntimeUser(t *testing.T) {
	command := runtimeUserShellCommand(shellQuote(helperPath) + " health")
	for _, required := range []string{
		"sudo -H -n -u " + shellQuote(RuntimeUser) + " sh -lc",
		"/usr/local/bin/sandbox",
		"health",
	} {
		if !strings.Contains(command, required) {
			t.Fatalf("runtime user command = %s; missing %q", command, required)
		}
	}
}

func TestHelperPayloadIdentityIsSeparateFromRuntimeUser(t *testing.T) {
	if helperUser == RuntimeUser {
		t.Fatalf("helperUser = %q; want distinct helper/payload identity from runtime user", helperUser)
	}
	command := "chown -R " + shellQuote(helperUser) + ":" + shellQuote(helperUser) + " " + shellQuote(payloadRootPath)
	if strings.Contains(command, shellQuote(RuntimeUser)+":"+shellQuote(RuntimeUser)) {
		t.Fatalf("payload ownership command = %s; want helper identity, not runtime user", command)
	}
}

func TestCanonicalSandboxBaseDirectoryCommandCreatesDraftRoots(t *testing.T) {
	command := canonicalSandboxBaseDirectoryCommand()
	for _, fragment := range []string{
		"sudo install -d -m 0755 -o '" + RuntimeUser + "' -g '" + RuntimeUser + "' '/workspace'",
		"sudo install -d -m 0755 -o '" + RuntimeUser + "' -g '" + RuntimeUser + "' '/mnt/session/uploads'",
		"sudo install -d -m 0755 -o '" + RuntimeUser + "' -g '" + RuntimeUser + "' '/mnt/session/outputs'",
		"sudo install -d -m 0755 -o root -g root '/mnt/memory'",
		"sudo install -d -m 0755 -o root -g root '/skills'",
		"sudo install -d -m 0700 -o '" + RuntimeUser + "' -g '" + RuntimeUser + "' '/tmp/tetral-runtime'",
		"sudo install -d -m 0700 -o '" + RuntimeUser + "' -g '" + RuntimeUser + "' '/tmp/tetral-runtime/rclone-cache'",
	} {
		if !strings.Contains(command, fragment) {
			t.Fatalf("base directory command missing %q in:\n%s", fragment, command)
		}
	}
	// Daytona executes commands as the runtime user; a bare install cannot
	// create directories under root-owned /mnt or chown to root.
	for _, part := range strings.Split(command, " && ") {
		if !strings.HasPrefix(part, "sudo install ") {
			t.Fatalf("base directory command has an unprivileged part %q in:\n%s", part, command)
		}
	}
	if strings.Contains(command, "-o root -g root '/mnt/session/uploads'") {
		t.Fatalf("base directory command makes uploads root-owned; nested file resources need runtime-writable parents:\n%s", command)
	}
}

func TestDaytonaHelperExecutorReturnsHelperFailureForNonAuthoritativeEnvelope(t *testing.T) {
	tests := []struct {
		name      string
		exitCode  int
		stdout    string
		wantCause bool
	}{
		{
			name:     "nonzero exit",
			exitCode: 7,
		},
		{
			name:      "unparseable stdout",
			stdout:    "not-json",
			wantCause: true,
		},
		{
			name:      "invalid result json",
			stdout:    `{"status":"ok","result_json":"not-json"}`,
			wantCause: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := newRecordingMemoryProjectionClient()
			client.process.exitCodes = []int{0, tc.exitCode}
			client.process.results = []string{"", tc.stdout}
			executor := NewDaytonaHelperExecutorForClient(client)

			_, err := executor.runToolLifecycleForTest(context.Background(), ToolInvocation{
				Target:         ToolTarget{ProviderSandboxID: "provider_sandbox"},
				ToolUseEventID: "evt_read",
				ToolName:       "Read",
				InputJSON:      `{"file_path":"/workspace/file.txt"}`,
			})
			var helperErr *HelperFailureError
			if !errors.As(err, &helperErr) {
				t.Fatalf("RunTool error = %T %v; want HelperFailureError", err, err)
			}
			if tc.wantCause && helperErr.Unwrap() == nil {
				t.Fatalf("HelperFailureError cause is nil; want parse cause")
			}
		})
	}
}

func TestHelperRunToolInputAdaptsExecCommandFields(t *testing.T) {
	input, err := helperRunToolInput("exec", "exec_command", `{"cmd":"pwd","workdir":"/workspace/app","yield_time_ms":750,"max_output_tokens":100}`, "evt_exec")
	if err != nil {
		t.Fatalf("helperRunToolInput exec: %v", err)
	}
	payload, ok := input.(map[string]any)
	if !ok {
		t.Fatalf("exec input type = %T; want map", input)
	}
	if payload["cwd"] != "/workspace/app" || payload["wait_ms"] != 750 || payload["task_id"] != "evt_exec" || payload["on_wait_expiry"] != "detach" {
		t.Fatalf("exec input = %#v; want helper exec shape", payload)
	}
	limits := helperLimits("exec", payload)
	if limits.VisibleBytes != 400 || limits.VisibleLines != helperVisibleLines {
		t.Fatalf("exec limits = %+v; want max_output_tokens lower visible bytes", limits)
	}
}

func TestHelperRunToolInputRejectsExecCommandClosedSetViolationsAtRuntimeBoundary(t *testing.T) {
	for _, inputJSON := range []string{
		`{"cmd":"pwd","command":"whoami"}`,
		`{"cmd":"pwd","timeout":1000}`,
		`{"cmd":"pwd","run_in_background":true}`,
		`{"cmd":"pwd","unknown":true}`,
	} {
		t.Run(inputJSON, func(t *testing.T) {
			input, err := helperRunToolInput("exec", "exec_command", inputJSON, "evt_exec_closed")
			if err == nil {
				t.Fatalf("helperRunToolInput(%s) = %#v; want closed-set rejection", inputJSON, input)
			}
			var result *preHelperToolResult
			if !errors.As(err, &result) || !strings.Contains(result.resultJSON, `"error_code":"unsupported_argument"`) {
				t.Fatalf("helperRunToolInput(%s) err = %T %v; want pre-helper unsupported_argument", inputJSON, err, err)
			}
		})
	}
}

func TestHelperRunToolInputClampsCodexYieldTime(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want int
	}{
		{name: "below minimum", raw: `{"cmd":"pwd","yield_time_ms":1}`, want: helperMinYieldWaitMS},
		{name: "above maximum", raw: `{"cmd":"pwd","yield_time_ms":99999}`, want: helperMaxYieldWaitMS},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			input, err := helperRunToolInput("exec", "exec_command", tc.raw, "evt_exec")
			if err != nil {
				t.Fatalf("helperRunToolInput exec: %v", err)
			}
			payload, ok := input.(map[string]any)
			if !ok {
				t.Fatalf("exec input type = %T; want map", input)
			}
			if payload["wait_ms"] != tc.want {
				t.Fatalf("wait_ms = %v; want %d in %#v", payload["wait_ms"], tc.want, payload)
			}
		})
	}
}

func TestHelperRunToolInputRejectsTTYBeforeHelper(t *testing.T) {
	input, err := helperRunToolInput("exec", "exec_command", `{"cmd":"pwd","tty":true}`, "evt_exec")
	if err == nil {
		t.Fatalf("helperRunToolInput tty returned input %#v; want pre-helper tool result", input)
	}
	var result *preHelperToolResult
	if !errors.As(err, &result) {
		t.Fatalf("helperRunToolInput tty err = %T %v; want preHelperToolResult", err, err)
	}
	if !strings.Contains(result.resultJSON, `"status":"tool_error"`) ||
		!strings.Contains(result.resultJSON, `"error_code":"unsupported_argument"`) ||
		!strings.Contains(result.resultJSON, `"argument":"tty"`) ||
		!strings.Contains(result.resultJSON, `"message":"exec_command input does not allow this argument"`) {
		t.Fatalf("tty result = %s; want unsupported argument tool error", result.resultJSON)
	}
}

func TestDaytonaRunToolReturnsUnsupportedArgumentWithoutHelperExecution(t *testing.T) {
	client := newRecordingMemoryProjectionClient()
	executor := NewDaytonaHelperExecutorForClient(client)

	result, err := executor.runToolLifecycleForTest(context.Background(), ToolInvocation{
		Target:         ToolTarget{ProviderSandboxID: "provider_sandbox"},
		ToolUseEventID: "evt_exec",
		ToolName:       "exec_command",
		InputJSON:      `{"cmd":"pwd","tty":true}`,
	})
	if err != nil {
		t.Fatalf("RunTool tty: %v", err)
	}
	if len(client.process.commands) != 0 {
		t.Fatalf("helper commands = %v; want no helper execution", client.process.commands)
	}
	if !strings.Contains(result.ResultJSON, `"unsupported_argument"`) {
		t.Fatalf("result = %s; want unsupported argument tool result", result.ResultJSON)
	}
}

func TestHelperRunToolInputAdaptsBashCommandFields(t *testing.T) {
	input, err := helperRunToolInput("exec", "Bash", `{"command":"make test","cwd":"/workspace/app","timeout":1200,"run_in_background":true}`, "evt_bash")
	if err != nil {
		t.Fatalf("helperRunToolInput Bash: %v", err)
	}
	payload, ok := input.(map[string]any)
	if !ok {
		t.Fatalf("Bash input type = %T; want map", input)
	}
	if payload["cmd"] != "make test" || payload["cwd"] != "/workspace/app" || payload["wait_ms"] != helperMinYieldWaitMS || payload["on_wait_expiry"] != "detach" || payload["task_id"] != "evt_bash" || payload["task_lifetime_ms"] != 1200 {
		t.Fatalf("Bash input = %#v; want helper exec shape", payload)
	}
	for _, providerOnly := range []string{"command", "timeout", "run_in_background"} {
		if _, ok := payload[providerOnly]; ok {
			t.Fatalf("Bash input retained provider-only field %q: %#v", providerOnly, payload)
		}
	}
}

func TestHelperRunToolInputRejectsClaudeBashAliasesBeforeHelper(t *testing.T) {
	for _, input := range []string{
		`{"command":"pwd","timeout_ms":1000}`,
		`{"cmd":"pwd"}`,
		`{"command":"pwd","workdir":"/workspace"}`,
		`{"command":"pwd","yield_time_ms":1000}`,
	} {
		t.Run(input, func(t *testing.T) {
			got, err := helperRunToolInput("exec", "Bash", input, "evt_bash_alias")
			if err == nil {
				t.Fatalf("helperRunToolInput(%s) = %#v; want closed-schema rejection", input, got)
			}
			var result *preHelperToolResult
			if !errors.As(err, &result) || !strings.Contains(result.resultJSON, `"error_code":"unsupported_argument"`) {
				t.Fatalf("helperRunToolInput(%s) err = %T %v; want unsupported argument result", input, err, err)
			}
		})
	}
}

func TestHelperRunToolInputComposesLongForegroundBashTimeout(t *testing.T) {
	input, composition, err := helperRunToolInputForInvocation("exec", "Bash", `{"command":"make test","cwd":"/workspace/app","timeout":120000}`, "evt_bash")
	if err != nil {
		t.Fatalf("helperRunToolInputForInvocation Bash: %v", err)
	}
	payload, ok := input.(map[string]any)
	if !ok {
		t.Fatalf("Bash input type = %T; want map", input)
	}
	if payload["cmd"] != "make test" ||
		payload["cwd"] != "/workspace/app" ||
		payload["wait_ms"] != helperMaxBlockingWaitMS ||
		payload["on_wait_expiry"] != "detach" ||
		payload["task_id"] != "evt_bash" ||
		payload["task_lifetime_ms"] != 120000 {
		t.Fatalf("Bash input = %#v; want long-timeout detach composition", payload)
	}
	if !composition.pollUntilTerminal {
		t.Fatalf("composition = %+v; want foreground poll until terminal", composition)
	}
	for _, providerOnly := range []string{"command", "timeout", "run_in_background"} {
		if _, ok := payload[providerOnly]; ok {
			t.Fatalf("Bash input retained provider-only field %q: %#v", providerOnly, payload)
		}
	}
}

func TestHelperRunToolInputComposesLongBackgroundBashLifetime(t *testing.T) {
	input, composition, err := helperRunToolInputForInvocation("exec", "Bash", `{"command":"make test","timeout":120000,"run_in_background":true}`, "evt_bash")
	if err != nil {
		t.Fatalf("helperRunToolInputForInvocation Bash: %v", err)
	}
	payload, ok := input.(map[string]any)
	if !ok {
		t.Fatalf("Bash input type = %T; want map", input)
	}
	if payload["wait_ms"] != helperMinYieldWaitMS ||
		payload["on_wait_expiry"] != "detach" ||
		payload["task_id"] != "evt_bash" ||
		payload["task_lifetime_ms"] != 120000 {
		t.Fatalf("Bash input = %#v; want bounded background lifetime", payload)
	}
	if composition.pollUntilTerminal {
		t.Fatalf("composition = %+v; background Bash should return a running handle", composition)
	}
}

func TestHelperRunToolInputStrictlyRejectsInvalidBashFieldValues(t *testing.T) {
	for _, raw := range []string{
		`{"command":"pwd","cwd":null}`,
		`{"command":"pwd","cwd":42}`,
		`{"command":"pwd","timeout":null}`,
		`{"command":"pwd","timeout":"1000"}`,
		`{"command":"pwd","timeout":1.5}`,
		`{"command":"pwd","timeout":-1}`,
		`{"command":"pwd","timeout":600001}`,
		`{"command":"pwd","run_in_background":null}`,
		`{"command":"pwd","run_in_background":"true"}`,
	} {
		t.Run(raw, func(t *testing.T) {
			input, _, err := helperRunToolInputForInvocation("exec", "Bash", raw, "evt_bash")
			if err == nil {
				t.Fatalf("helperRunToolInputForInvocation(%s) = %#v; want strict rejection", raw, input)
			}
			var result *preHelperToolResult
			if !errors.As(err, &result) || !strings.Contains(result.resultJSON, `"error_code":"unsupported_argument"`) {
				t.Fatalf("helperRunToolInputForInvocation(%s) err = %T %v; want pre-helper tool error", raw, err, err)
			}
		})
	}
}

func TestDaytonaPreparedForegroundSubmissionExposesDurableObservationState(t *testing.T) {
	client := newRecordingMemoryProjectionClient()
	client.process.results = []string{
		"",
		`{"schema_version":1,"tool":"exec","status":"running","truncated":false,"error":null,"result":{"task_id":"evt_durable","stdout":{"text":"started\n","total_bytes":8,"total_lines":1,"returned_bytes":8,"truncated":false},"stderr":{"text":"","total_bytes":0,"total_lines":0,"returned_bytes":0,"truncated":false}}}`,
		"",
		`{"schema_version":1,"tool":"poll","status":"success","truncated":false,"error":null,"result":{"exit_code":0,"stdout":{"text":"done\n","total_bytes":13,"total_lines":2,"returned_bytes":5,"truncated":false},"stderr":{"text":"","total_bytes":0,"total_lines":0,"returned_bytes":0,"truncated":false}}}`,
	}
	executor := NewDaytonaHelperExecutorForClient(client)
	prepared, err := executor.PrepareTool(context.Background(), ToolInvocation{
		Target: ToolTarget{ProviderSandboxID: "provider_sandbox"}, ToolUseEventID: "evt_durable",
		ToolName: "Bash", InputJSON: `{"command":"sleep 120","timeout":120000}`,
	})
	if err != nil {
		t.Fatalf("PrepareTool: %v", err)
	}
	submitted, err := executor.SubmitPreparedTool(context.Background(), prepared)
	if err != nil {
		t.Fatalf("SubmitPreparedTool: %v", err)
	}
	if submitted.ForegroundObservation == nil || submitted.ForegroundObservation.Reference.Task.TaskID != "evt_durable" {
		t.Fatalf("submission = %+v; want durable foreground command reference", submitted)
	}
	if len(client.fileSystem.uploads) != 1 {
		t.Fatalf("uploads after submit = %d; want no poll before durable reference", len(client.fileSystem.uploads))
	}
	observed, err := executor.ObserveForegroundTool(context.Background(), *submitted.ForegroundObservation)
	if err != nil {
		t.Fatalf("ObserveForegroundTool: %v", err)
	}
	if observed.ForegroundObservation != nil || !strings.Contains(observed.ResultJSON, "started\\n") || !strings.Contains(observed.ResultJSON, "done\\n") {
		t.Fatalf("observed = %+v; want terminal aggregate", observed)
	}
}

func TestDaytonaRunToolAggregatesAllForegroundDetachSnapshotsWithBoundedHeadAndLatestTail(t *testing.T) {
	client := newRecordingMemoryProjectionClient()
	initial := strings.Repeat("a", 30000)
	intermediate := strings.Repeat("b", 30000)
	terminal := strings.Repeat("c", 30000)
	client.process.results = []string{
		"",
		testHelperCommandEnvelope(t, "exec", "running", "evt_bash_aggregate", nil, initial, 30000),
		"",
		testHelperCommandEnvelope(t, "poll", "running", "evt_bash_aggregate", nil, intermediate, 60000),
		"",
		testHelperCommandEnvelope(t, "poll", "success", "evt_bash_aggregate", intPtr(0), terminal, 90000),
	}
	executor := NewDaytonaHelperExecutorForClient(client)

	result, err := executor.runToolLifecycleForTest(context.Background(), ToolInvocation{
		Target:         ToolTarget{ProviderSandboxID: "provider_sandbox"},
		ToolUseEventID: "evt_bash_aggregate",
		ToolName:       "Bash",
		InputJSON:      `{"command":"long command","timeout":120000}`,
	})
	if err != nil {
		t.Fatalf("RunTool aggregate long Bash: %v", err)
	}
	var envelope struct {
		Result struct {
			Stdout foregroundStreamSnapshot `json:"stdout"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(result.ResultJSON), &envelope); err != nil {
		t.Fatalf("decode aggregate result: %v", err)
	}
	stdout := envelope.Result.Stdout
	const loweredVisibleBytes = helperVisibleBytes
	if stdout.TotalBytes != 90000 || stdout.ReturnedBytes != loweredVisibleBytes || !stdout.Truncated {
		t.Fatalf("stdout bounds = %+v; want cumulative 90000 bytes with %d returned and truncation", stdout, loweredVisibleBytes)
	}
	wantText := strings.Repeat("a", loweredVisibleBytes/2) + foregroundTruncationMarker(90000-loweredVisibleBytes) + strings.Repeat("c", loweredVisibleBytes/2)
	if stdout.Text != wantText {
		t.Fatalf("stdout head/tail = prefix %q suffix %q len %d; want initial head and terminal tail", stdout.Text[:16], stdout.Text[len(stdout.Text)-16:], len(stdout.Text))
	}
	if strings.Contains(stdout.Text, strings.Repeat("b", 16)) {
		t.Fatal("stdout retained intermediate middle after the bounded latest tail advanced")
	}
	for index, upload := range client.fileSystem.uploads[1:] {
		if !strings.Contains(upload.body, `"visible_bytes":51200`) {
			t.Fatalf("poll upload %d = %s; want the default bounded output cap", index, upload.body)
		}
	}
}

func TestForegroundStreamAccumulatorBoundsInitialHeadAndLatestTailByLines(t *testing.T) {
	accumulator := newForegroundStreamAccumulator(helperVisibleBytes, helperVisibleLines)
	initial := strings.Repeat("head\n", 1200)
	intermediate := strings.Repeat("middle\n", 1200)
	terminal := strings.Repeat("tail\n", 1200)
	accumulator.add(foregroundStreamSnapshot{Text: initial, TotalBytes: int64(len(initial)), TotalLines: 1200, ReturnedBytes: len(initial)})
	accumulator.add(foregroundStreamSnapshot{Text: intermediate, TotalBytes: int64(len(initial) + len(intermediate)), TotalLines: 2400, ReturnedBytes: len(intermediate)})
	got := accumulator.add(foregroundStreamSnapshot{Text: terminal, TotalBytes: int64(len(initial) + len(intermediate) + len(terminal)), TotalLines: 3600, ReturnedBytes: len(terminal)})

	wantReturned := len(strings.Repeat("head\n", 1000)) + len(strings.Repeat("tail\n", 1000))
	if got.TotalLines != 3600 || got.ReturnedBytes != wantReturned || !got.Truncated {
		t.Fatalf("line-bounded snapshot = %+v; want 3600 cumulative lines and %d returned bytes", got, wantReturned)
	}
	if !strings.HasPrefix(got.Text, strings.Repeat("head\n", 1000)) || !strings.HasSuffix(got.Text, strings.Repeat("tail\n", 1000)) {
		t.Fatalf("line-bounded text prefix/suffix mismatch: len=%d", len(got.Text))
	}
	if strings.Contains(got.Text, "middle") {
		t.Fatal("line-bounded snapshot retained intermediate output instead of the latest tail")
	}
}

func TestDaytonaCommandFollowupPayloadUsesCurrentToolUseEventID(t *testing.T) {
	client := newRecordingMemoryProjectionClient()
	client.process.results = []string{
		"",
		`{"schema_version":1,"tool":"poll","status":"success","truncated":false,"error":null,"result":{"exit_code":0,"stdout":"done","stderr":""}}`,
	}
	executor := NewDaytonaHelperExecutorForClient(client)

	result, err := executor.ReadCommandResult(context.Background(), CommandReference{
		Target:          ToolTarget{ProviderSandboxID: "provider_sandbox"},
		ToolUseEventID:  "evt_followup_poll",
		MaxOutputTokens: 100,
		Task: BackgroundTask{
			TaskID:               "task_live_1",
			SourceToolUseEventID: "evt_exec_source",
			ProviderSessionID:    "provider_sandbox",
			ProviderCommandID:    "task_live_1",
		},
	})
	if err != nil {
		t.Fatalf("ReadCommandResult: %v", err)
	}
	if result.TerminalStatus != "completed" {
		t.Fatalf("terminal status = %q; want completed", result.TerminalStatus)
	}
	if len(client.fileSystem.uploads) != 1 {
		t.Fatalf("uploads = %d; want one poll payload", len(client.fileSystem.uploads))
	}
	upload := client.fileSystem.uploads[0]
	if upload.path != payloadStageRootPath+"/evt_followup_poll/"+payloadFileName {
		t.Fatalf("payload path = %q; want current follow-up tool-use event stage directory", upload.path)
	}
	for _, required := range []string{
		`"tool":"poll"`,
		`"tool_use_event_id":"evt_followup_poll"`,
		`"task_id":"task_live_1"`,
		`"max_output_tokens":100`,
		`"visible_bytes":400`,
	} {
		if !strings.Contains(upload.body, required) {
			t.Fatalf("poll payload missing %s in %s", required, upload.body)
		}
	}
	if strings.Contains(upload.path, "task_live_1") {
		t.Fatalf("payload path = %q; must not use provider task id", upload.path)
	}
	if strings.Contains(upload.path, "evt_exec_source") || strings.Contains(upload.body, `"tool_use_event_id":"evt_exec_source"`) {
		t.Fatalf("payload = %q/%s; must not reuse original exec tool-use event id for follow-up", upload.path, upload.body)
	}
}

func TestTerminalStatusFromResultUsesHelperContractVocabulary(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "running",
			raw:  `{"schema_version":1,"tool":"poll","status":"running","result":{"task_id":"task_1"}}`,
		},
		{
			name: "cancelled",
			raw:  `{"schema_version":1,"tool":"poll","status":"success","result":{"cancelled":true}}`,
			want: "cancelled",
		},
		{
			name: "expired",
			raw:  `{"schema_version":1,"tool":"poll","status":"success","result":{"timed_out":true}}`,
			want: "expired",
		},
		{
			name: "completed",
			raw:  `{"schema_version":1,"tool":"poll","status":"success","result":{"exit_code":0}}`,
			want: "completed",
		},
		{
			name: "failed exit",
			raw:  `{"schema_version":1,"tool":"poll","status":"success","result":{"exit_code":2}}`,
			want: "failed",
		},
		{
			name: "failed signal",
			raw:  `{"schema_version":1,"tool":"poll","status":"success","result":{"signal":"TERM"}}`,
			want: "failed",
		},
		{
			name: "lost task",
			raw:  `{"schema_version":1,"tool":"poll","status":"error","error":{"kind":"task_lost","message":"task supervisor is not reachable"},"result":{}}`,
			want: "failed",
		},
		{
			name: "other helper error is not a terminal task observation",
			raw:  `{"schema_version":1,"tool":"poll","status":"error","error":{"kind":"helper_failure","message":"helper is unavailable"},"result":{}}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := terminalStatusFromResult(tc.raw); got != tc.want {
				t.Fatalf("terminalStatusFromResult = %q; want %q", got, tc.want)
			}
		})
	}
}

func TestHelperRunToolInputAdaptsWriteFilePath(t *testing.T) {
	input, err := helperRunToolInput("write", "Write", `{"file_path":"/workspace/note.txt","content":"hello"}`, "evt_write")
	if err != nil {
		t.Fatalf("helperRunToolInput write: %v", err)
	}
	payload, ok := input.(map[string]any)
	if !ok || payload["path"] != "/workspace/note.txt" || payload["content"] != "hello" {
		t.Fatalf("write input = %#v; want file_path adapted to path", input)
	}
}

func TestHelperRunToolInputAdaptsEditFilePath(t *testing.T) {
	input, err := helperRunToolInput("edit", "Edit", `{"file_path":"/workspace/note.txt","old_string":"a","new_string":"b"}`, "evt_edit")
	if err != nil {
		t.Fatalf("helperRunToolInput edit: %v", err)
	}
	payload, ok := input.(map[string]any)
	if !ok || payload["path"] != "/workspace/note.txt" || payload["old_string"] != "a" || payload["new_string"] != "b" {
		t.Fatalf("edit input = %#v; want file_path adapted to path", input)
	}
}

func TestHelperRunToolInputDoesNotHonorRawFilePathAliases(t *testing.T) {
	for _, tc := range []struct {
		name    string
		command string
		tool    string
		raw     string
	}{
		{name: "read", command: "read", tool: "Read", raw: `{"path":"/workspace/note.txt"}`},
		{name: "write", command: "write", tool: "Write", raw: `{"path":"/workspace/note.txt","content":"hello"}`},
		{name: "edit", command: "edit", tool: "Edit", raw: `{"path":"/workspace/note.txt","old_string":"a","new_string":"b"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input, err := helperRunToolInput(tc.command, tc.tool, tc.raw, "evt_file")
			if err != nil {
				t.Fatalf("helperRunToolInput %s: %v", tc.name, err)
			}
			payload, ok := input.(map[string]any)
			if !ok {
				t.Fatalf("%s input type = %T; want map", tc.name, input)
			}
			if _, present := payload["path"]; present {
				t.Fatalf("%s input retained undeclared path alias: %#v", tc.name, payload)
			}
		})
	}
}

func TestParseHelperResultPreservesContractEnvelope(t *testing.T) {
	result, err := parseHelperResult("read", `{"schema_version":1,"tool":"read","status":"success","truncated":false,"error":null,"result":{"content":"hello","provider_command_id":"secret"},"metrics":{"wall_time_ms":1}}`)
	if err != nil {
		t.Fatalf("parseHelperResult: %v", err)
	}
	if !strings.Contains(result.ResultJSON, `"status":"success"`) || !strings.Contains(result.ResultJSON, `"content":"hello"`) {
		t.Fatalf("result json = %s; want full contract envelope", result.ResultJSON)
	}
	if strings.Contains(result.ResultJSON, "provider_command_id") || strings.Contains(result.ResultJSON, "secret") {
		t.Fatalf("provider metadata leaked in %s", result.ResultJSON)
	}
}

func TestParseHelperResultRejectsLegacyEnvelope(t *testing.T) {
	if _, err := parseHelperResult("read", `{"status":"ok","result":{"content":"legacy"}}`); err == nil {
		t.Fatal("parseHelperResult accepted legacy helper envelope; want schema v1 rejection")
	}
}

func TestParseHelperResultRejectsEnvelopeToolMismatch(t *testing.T) {
	if _, err := parseHelperResult("read", `{"schema_version":1,"tool":"write","status":"success","truncated":false,"error":null,"result":{"content":"ok"}}`); err == nil {
		t.Fatal("parseHelperResult accepted mismatched helper tool")
	}
}

func TestParseHelperResultRejectsForbiddenLegacyFields(t *testing.T) {
	for _, key := range []string{"error_kind", "result_json", "terminal_status", "background_task"} {
		t.Run(key, func(t *testing.T) {
			raw := `{"schema_version":1,"tool":"read","status":"success","truncated":false,"error":null,"result":{"content":"ok"},"` + key + `":{}}`
			if _, err := parseHelperResult("read", raw); err == nil {
				t.Fatalf("parseHelperResult accepted forbidden helper key %q", key)
			}
		})
	}
}

func TestParseHelperResultRejectsErrorObjectOnNonErrorStatus(t *testing.T) {
	for _, status := range []string{"success", "running"} {
		t.Run(status, func(t *testing.T) {
			raw := `{"schema_version":1,"tool":"read","status":"` + status + `","truncated":false,"error":{"kind":"helper_failure","message":"should not be present"},"result":{"content":"ok"}}`
			if _, err := parseHelperResult("read", raw); err == nil {
				t.Fatalf("parseHelperResult accepted %s envelope with error object", status)
			}
		})
	}
}

func TestDaytonaHelperExecutorRejectsInvalidPayloadIDBeforeSandboxFilesystem(t *testing.T) {
	client := newRecordingMemoryProjectionClient()
	executor := NewDaytonaHelperExecutorForClient(client)

	_, err := executor.runToolLifecycleForTest(context.Background(), ToolInvocation{
		Target:         ToolTarget{ProviderSandboxID: "provider_sandbox"},
		ToolUseEventID: "../escape",
		ToolName:       "Read",
		InputJSON:      `{"file_path":"/workspace/file.txt"}`,
	})
	if err == nil || !strings.Contains(err.Error(), "helper payload id") {
		t.Fatalf("RunTool err = %v; want invalid helper payload id", err)
	}
	if len(client.process.commands) != 0 {
		t.Fatalf("sandbox commands = %v; want no filesystem/process work for invalid payload id", client.process.commands)
	}
}

func TestDaytonaHelperExecutorFailsBeforeHelperWhenPayloadPermissionCommandFails(t *testing.T) {
	client := newRecordingMemoryProjectionClient()
	client.process.exitCodes = []int{7}
	executor := NewDaytonaHelperExecutorForClient(client)

	_, err := executor.runToolLifecycleForTest(context.Background(), ToolInvocation{
		Target:         ToolTarget{ProviderSandboxID: "provider_sandbox"},
		ToolUseEventID: "evt_read",
		ToolName:       "Read",
		InputJSON:      `{"file_path":"/workspace/file.txt"}`,
	})
	var providerErr *sandbox.ProviderError
	if !errors.As(err, &providerErr) {
		t.Fatalf("RunTool err = %T %v; want ProviderError", err, err)
	}
	if providerErr.Provider != DaytonaProviderName || providerErr.Stage != sandbox.StageExecuteTool ||
		providerErr.Kind != sandbox.ProviderErrorUnknown || !providerErr.Retryable ||
		providerErr.SafeMessage != "sandbox helper payload permission command failed" {
		t.Fatalf("RunTool provider error = %+v; want safe retryable execute-tool failure", providerErr)
	}
	if len(client.process.commands) != 1 {
		t.Fatalf("sandbox commands = %v; want permission command only", client.process.commands)
	}
	for _, required := range []string{
		"sudo -n sh -c ",
		"install -d -m 0700 -o " + shellQuote(helperUser) + " -g " + shellQuote(helperUser) + " -- " + shellQuote(payloadRootPath),
		"mv -T -- " + shellQuote(payloadStageRootPath+"/evt_read") + " " + shellQuote(payloadRootPath+"/evt_read"),
		"chown -R " + shellQuote(helperUser) + ":" + shellQuote(helperUser) + " -- " + shellQuote(payloadRootPath+"/evt_read"),
		"chmod 0700 -- " + shellQuote(payloadRootPath+"/evt_read"),
		"chmod 0600 -- " + shellQuote(payloadRootPath+"/evt_read/"+payloadFileName),
	} {
		if !strings.Contains(client.process.commands[0], shellEmbedded(required)) {
			t.Fatalf("permission command = %q; missing %q", client.process.commands[0], required)
		}
	}
	if len(client.fileSystem.deleted) != 1 || client.fileSystem.deleted[0] != payloadStageRootPath+"/evt_read" {
		t.Fatalf("deleted payload dirs = %v; want cleanup of the evt_read stage dir", client.fileSystem.deleted)
	}
}

// shellEmbedded rewrites a fragment to how it appears inside the freeze
// command's outer sudo sh -c quoting, where every inner single quote becomes
// the '"'"' escape sequence.
func shellEmbedded(fragment string) string {
	return strings.ReplaceAll(fragment, "'", `'"'"'`)
}

func TestSynthesizeHelperBackgroundTaskFromRunningEnvelope(t *testing.T) {
	result := helperResult{
		ResultJSON: `{"schema_version":1,"tool":"exec","status":"running","result":{"task_id":"task_1"}}`,
	}
	task := synthesizeHelperBackgroundTask(ToolTarget{ProviderSandboxID: "provider_sandbox"}, result)
	if task == nil || task.TaskID != "task_1" || task.ProviderSessionID != "provider_sandbox" || task.ProviderCommandID != "task_1" || task.ProviderCommandMetadataJSON != `{}` {
		t.Fatalf("background task = %+v; want synthesized driver metadata", task)
	}
	if got := synthesizeHelperBackgroundTask(ToolTarget{ProviderSandboxID: "provider_sandbox"}, helperResult{ResultJSON: `{"status":"success","result":{"task_id":"task_1"}}`}); got != nil {
		t.Fatalf("success synthesized task = %+v; want nil", got)
	}
}

func TestHelperRunToolInputAdaptsApplyPatchJSONString(t *testing.T) {
	input, err := helperRunToolInput("apply_patch", "apply_patch", `"*** Begin Patch\n*** End Patch\n"`, "evt_patch")
	if err != nil {
		t.Fatalf("helperRunToolInput apply_patch: %v", err)
	}
	payload, ok := input.(map[string]any)
	if !ok || payload["patch"] != "*** Begin Patch\n*** End Patch\n" {
		t.Fatalf("apply_patch input = %#v; want patch object", input)
	}
}

func TestHelperStdinInputMapsYieldTimeWriteSeqAndMaxOutputTokens(t *testing.T) {
	input, err := helperStdinInput("task_1", `{"session_id":"task_1","chars":"hello","yield_time_ms":750,"write_seq":7,"max_output_tokens":100}`)
	if err != nil {
		t.Fatalf("helperStdinInput: %v", err)
	}
	if input["task_id"] != "task_1" || input["chars"] != "hello" || input["wait_ms"] != 750 || input["write_seq"] != 7 || input["max_output_tokens"] != 100 {
		t.Fatalf("stdin input = %#v; want helper stdin shape", input)
	}
}

func TestHelperStdinInputClampsYieldTime(t *testing.T) {
	input, err := helperStdinInput("task_1", `{"chars":"hello","yield_time_ms":50000}`)
	if err != nil {
		t.Fatalf("helperStdinInput: %v", err)
	}
	if input["wait_ms"] != helperMaxYieldWaitMS {
		t.Fatalf("stdin wait_ms = %v; want %d", input["wait_ms"], helperMaxYieldWaitMS)
	}
}

func TestOutputCaptureUsesSandboxInternalHelperMode(t *testing.T) {
	data, err := os.ReadFile("daytona_output_capture.go")
	if err != nil {
		t.Fatalf("read daytona_output_capture.go: %v", err)
	}
	if !strings.Contains(string(data), "__capture") {
		t.Fatal("daytona output capture does not invoke the Sandbox-internal helper mode")
	}
	if _, err := outputCaptureChildPath("/mnt/session/outputs", "../escape"); err == nil {
		t.Fatal("outputCaptureChildPath accepted escape name")
	}
}

func TestProviderErrorForHealthResponseMapsHelperEnvelope(t *testing.T) {
	if err := providerErrorForHealthResponse(`{"schema_version":1,"tool":"health","status":"success","truncated":false,"error":null,"result":{"status":"ok","version":"test","checks":[]}}`, 0); err != nil {
		t.Fatalf("providerErrorForHealthResponse ok: %v", err)
	}

	err := providerErrorForHealthResponse(`{"schema_version":1,"tool":"health","status":"error","truncated":false,"error":{"kind":"helper_failure","message":"sandbox helper health check failed"},"result":{"status":"error","version":"test","checks":[{"name":"rclone","ok":false}]}}`, 0)
	var providerErr *sandbox.ProviderError
	if !errors.As(err, &providerErr) {
		t.Fatalf("error = %T %v; want ProviderError", err, err)
	}
	if providerErr.Stage != sandbox.StageCheckBaseTemplate || providerErr.Kind != sandbox.ProviderErrorUnavailable || providerErr.Retryable {
		t.Fatalf("provider error = %+v; want nonretryable check_base_template unavailable", providerErr)
	}

	err = providerErrorForHealthResponse(`not-json`, 0)
	if !errors.As(err, &providerErr) || providerErr.Stage != sandbox.StageCheckBaseTemplate || !providerErr.Retryable {
		t.Fatalf("invalid envelope error = %+v; want retryable check_base_template provider error", err)
	}

	err = providerErrorForHealthResponse(`{"status":"ok","result":{"status":"ok","checks":[]}}`, 0)
	if !errors.As(err, &providerErr) || providerErr.Stage != sandbox.StageCheckBaseTemplate || !providerErr.Retryable {
		t.Fatalf("legacy envelope error = %+v; want retryable non-authoritative provider error", err)
	}

	err = providerErrorForHealthResponse("", 2)
	if !errors.As(err, &providerErr) || providerErr.Stage != sandbox.StageCheckBaseTemplate || !providerErr.Retryable {
		t.Fatalf("nonzero exit error = %+v; want retryable check_base_template provider error", err)
	}
}

func TestDaytonaCreateSandboxLowersNetworkPolicyAtCreation(t *testing.T) {
	tests := []struct {
		name          string
		network       sandbox.NetworkSetup
		wantBlockAll  bool
		wantAllowList *string
	}{
		{
			name:         "unrestricted",
			network:      sandbox.NetworkSetup{Type: "unrestricted"},
			wantBlockAll: false,
		},
		{
			name:         "blocked",
			network:      sandbox.NetworkSetup{Type: "blocked"},
			wantBlockAll: true,
		},
		{
			name:          "cidr allow list",
			network:       sandbox.NetworkSetup{Type: "cidr_allow_list", NetworkAllowList: "10.0.0.0/8,2001:db8::/32"},
			wantBlockAll:  false,
			wantAllowList: stringPtr("10.0.0.0/8,2001:db8::/32"),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := &recordingDaytonaLifecycleClient{}
			provider := NewDaytonaLifecycleProviderForClient(client, 45*time.Second)
			_, err := provider.CreateSandbox(context.Background(), sandbox.CreateSandboxRequest{
				Setup: sandbox.SandboxSetup{
					SandboxID:           "sandbox_test",
					ProviderArtifactRef: "snapshot_env_test",
					Network:             tc.network,
				},
			})
			if err != nil {
				t.Fatalf("CreateSandbox: %v", err)
			}
			params, ok := client.createParams.(types.SnapshotParams)
			if !ok {
				t.Fatalf("create params = %T; want types.SnapshotParams", client.createParams)
			}
			if params.Snapshot != "snapshot_env_test" {
				t.Fatalf("Snapshot = %q; want snapshot_env_test", params.Snapshot)
			}
			if params.NetworkBlockAll != tc.wantBlockAll {
				t.Fatalf("NetworkBlockAll = %v; want %v", params.NetworkBlockAll, tc.wantBlockAll)
			}
			switch {
			case tc.wantAllowList == nil && params.NetworkAllowList != nil:
				t.Fatalf("NetworkAllowList = %q; want nil", *params.NetworkAllowList)
			case tc.wantAllowList != nil && params.NetworkAllowList == nil:
				t.Fatalf("NetworkAllowList is nil; want %q", *tc.wantAllowList)
			case tc.wantAllowList != nil && *params.NetworkAllowList != *tc.wantAllowList:
				t.Fatalf("NetworkAllowList = %q; want %q", *params.NetworkAllowList, *tc.wantAllowList)
			}
		})
	}
}

func TestDaytonaCreateSandboxLowersBoundedLifecycleIntervals(t *testing.T) {
	client := &recordingDaytonaLifecycleClient{}
	provider, err := newDaytonaLifecycleProvider(client, LifecyclePolicy{
		StopTimeout:         30 * time.Second,
		AutoStopInterval:    30*time.Minute + time.Second,
		AutoArchiveInterval: 24 * time.Hour,
		AutoDeleteInterval:  30 * 24 * time.Hour,
	}, 45*time.Second)
	if err != nil {
		t.Fatalf("newDaytonaLifecycleProvider: %v", err)
	}
	_, err = provider.CreateSandbox(context.Background(), sandbox.CreateSandboxRequest{Setup: sandbox.SandboxSetup{
		SandboxID:           "sandbox_lifecycle",
		ProviderArtifactRef: "snapshot_lifecycle",
	}})
	if err != nil {
		t.Fatalf("CreateSandbox: %v", err)
	}
	params := client.createParams.(types.SnapshotParams)
	if params.AutoStopInterval == nil || *params.AutoStopInterval != 31 {
		t.Fatalf("AutoStopInterval = %v; want ceil(30m1s) = 31", params.AutoStopInterval)
	}
	if params.AutoArchiveInterval == nil || *params.AutoArchiveInterval != 1440 {
		t.Fatalf("AutoArchiveInterval = %v; want 1440", params.AutoArchiveInterval)
	}
	if params.AutoDeleteInterval == nil || *params.AutoDeleteInterval != 43200 {
		t.Fatalf("AutoDeleteInterval = %v; want 43200", params.AutoDeleteInterval)
	}
}

func TestDaytonaCreateSandboxLowersOnlyStableOwnershipLabels(t *testing.T) {
	client := &recordingDaytonaLifecycleClient{}
	provider := NewDaytonaLifecycleProviderForClient(client, 45*time.Second)
	_, err := provider.CreateSandbox(context.Background(), sandbox.CreateSandboxRequest{Setup: sandbox.SandboxSetup{
		WorkspaceID: "ws_labels", SessionID: "sesn_labels", EnvironmentID: "env_labels",
		SandboxID: "sbox_labels", LifecycleOperationID: "sop_attempt", EnvironmentGeneration: 9,
		ProviderArtifactRef: "snapshot_labels",
	}})
	if err != nil {
		t.Fatalf("CreateSandbox: %v", err)
	}
	params := client.createParams.(types.SnapshotParams)
	want := map[string]string{
		"tetral.workspace_id":    "ws_labels",
		"tetral.session_id":      "sesn_labels",
		"tetral.environment_id":  "env_labels",
		"tetral.sandbox_id":      "sbox_labels",
		"tetral.lifecycle_owner": "sandbox",
	}
	if !reflect.DeepEqual(params.Labels, want) {
		t.Fatalf("Create labels = %v; want stable ownership %v", params.Labels, want)
	}
}

func TestDaytonaResolveSandboxRequiresExactStableIdentity(t *testing.T) {
	labels := map[string]string{
		"tetral.workspace_id":    "ws_resolve",
		"tetral.session_id":      "sesn_resolve",
		"tetral.environment_id":  "env_resolve",
		"tetral.sandbox_id":      "sbox_resolve",
		"tetral.lifecycle_owner": "sandbox",
	}
	tests := []struct {
		name                  string
		got                   *daytona.Sandbox
		wantFound             bool
		wantError             bool
		wantOwnershipMismatch bool
	}{
		{
			name: "exact ownership",
			got: &daytona.Sandbox{
				ID: "provider_resolve", Name: "sbox_resolve", Labels: labels,
			},
			wantFound: true,
		},
		{
			name: "exact ownership with sdk language label",
			got: &daytona.Sandbox{
				ID: "provider_resolve", Name: "sbox_resolve",
				Labels: map[string]string{
					"tetral.workspace_id":          "ws_resolve",
					"tetral.session_id":            "sesn_resolve",
					"tetral.environment_id":        "env_resolve",
					"tetral.sandbox_id":            "sbox_resolve",
					"tetral.lifecycle_owner":       "sandbox",
					types.CodeToolboxLanguageLabel: string(types.CodeLanguagePython),
				},
			},
			wantFound: true,
		},
		{
			name: "malformed sdk language label",
			got: &daytona.Sandbox{
				ID: "provider_resolve", Name: "sbox_resolve",
				Labels: map[string]string{
					"tetral.workspace_id":          "ws_resolve",
					"tetral.session_id":            "sesn_resolve",
					"tetral.environment_id":        "env_resolve",
					"tetral.sandbox_id":            "sbox_resolve",
					"tetral.lifecycle_owner":       "sandbox",
					types.CodeToolboxLanguageLabel: "javascript",
				},
			},
			wantError:             true,
			wantOwnershipMismatch: true,
		},
		{
			name: "wrong stable name",
			got: &daytona.Sandbox{
				ID: "provider_resolve", Name: "different", Labels: labels,
			},
			wantError:             true,
			wantOwnershipMismatch: true,
		},
		{
			name: "extra ownership label",
			got: &daytona.Sandbox{
				ID: "provider_resolve", Name: "sbox_resolve",
				Labels: map[string]string{
					"tetral.workspace_id":    "ws_resolve",
					"tetral.session_id":      "sesn_resolve",
					"tetral.environment_id":  "env_resolve",
					"tetral.sandbox_id":      "sbox_resolve",
					"tetral.lifecycle_owner": "sandbox",
					"unexpected":             "label",
				},
			},
			wantError:             true,
			wantOwnershipMismatch: true,
		},
	}
	for _, key := range []string{
		"tetral.workspace_id",
		"tetral.session_id",
		"tetral.environment_id",
		"tetral.sandbox_id",
		"tetral.lifecycle_owner",
	} {
		missing := maps.Clone(labels)
		delete(missing, key)
		tests = append(tests, struct {
			name                  string
			got                   *daytona.Sandbox
			wantFound             bool
			wantError             bool
			wantOwnershipMismatch bool
		}{
			name:                  "missing " + key,
			got:                   &daytona.Sandbox{ID: "provider_resolve", Name: "sbox_resolve", Labels: missing},
			wantError:             true,
			wantOwnershipMismatch: true,
		})
		wrong := maps.Clone(labels)
		wrong[key] += "_wrong"
		tests = append(tests, struct {
			name                  string
			got                   *daytona.Sandbox
			wantFound             bool
			wantError             bool
			wantOwnershipMismatch bool
		}{
			name:                  "wrong " + key,
			got:                   &daytona.Sandbox{ID: "provider_resolve", Name: "sbox_resolve", Labels: wrong},
			wantError:             true,
			wantOwnershipMismatch: true,
		})
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			provider := NewDaytonaLifecycleProviderForClient(&recordingDaytonaLifecycleClient{getResult: test.got}, 45*time.Second)
			handle, found, err := provider.ResolveSandbox(context.Background(), "sbox_resolve", labels)
			if (err != nil) != test.wantError || found != test.wantFound {
				t.Fatalf("ResolveSandbox = (%+v, %t, %v); want found=%t error=%t", handle, found, err, test.wantFound, test.wantError)
			}
			if errors.Is(err, ErrSandboxOwnershipMismatch) != test.wantOwnershipMismatch {
				t.Fatalf("ResolveSandbox ownership mismatch = %t; want %t (err %v)", errors.Is(err, ErrSandboxOwnershipMismatch), test.wantOwnershipMismatch, err)
			}
			if found && handle.SandboxID != "provider_resolve" {
				t.Fatalf("resolved handle = %+v", handle)
			}
		})
	}
}

func TestDaytonaLifecycleIntervalBoundsMaximumDuration(t *testing.T) {
	got, err := daytonaIntervalMinutes(time.Duration(1<<63-1), "auto-delete")
	if err != nil {
		t.Fatalf("daytonaIntervalMinutes: %v", err)
	}
	if got == nil || *got <= 0 || int64(*got) > 2147483647 {
		t.Fatalf("daytonaIntervalMinutes = %v; want positive int32-bounded minutes", got)
	}
}

func TestDaytonaInspectStateRejectsMissingProviderResponse(t *testing.T) {
	client := &recordingDaytonaLifecycleClient{returnNilGet: true}
	provider := NewDaytonaLifecycleProviderForClient(client, 45*time.Second)
	_, err := provider.InspectState(context.Background(), "provider_missing_response")
	var providerErr *sandbox.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Kind != sandbox.ProviderErrorMalformedResponse || providerErr.Retryable {
		t.Fatalf("InspectState error = %T %v; want non-retryable malformed response", err, err)
	}
}

func TestDaytonaReleaseSandboxDeletesTheProviderResource(t *testing.T) {
	provider, err := newDaytonaLifecycleProvider(&recordingDaytonaLifecycleClient{}, LifecyclePolicy{
		StopTimeout: 30 * time.Second,
	}, 45*time.Second)
	if err != nil {
		t.Fatalf("newDaytonaLifecycleProvider: %v", err)
	}
	calls := []string{}
	provider.deleteSandbox = func(_ context.Context, _ *daytona.Sandbox, timeout time.Duration) error {
		calls = append(calls, "delete:"+timeout.String())
		return nil
	}

	if err := provider.ReleaseSandbox(context.Background(), sandbox.ProviderHandle{SandboxID: "provider_sandbox_test"}); err != nil {
		t.Fatalf("ReleaseSandbox: %v", err)
	}
	want := []string{"delete:30s"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("lifecycle calls = %v; want %v", calls, want)
	}
}

func TestDaytonaReleaseSandboxClassifiesLookupBeforeDelete(t *testing.T) {
	provider := NewDaytonaLifecycleProviderForClient(&recordingDaytonaLifecycleClient{
		getErr: errors.New("temporary provider lookup failure"),
	}, 45*time.Second)
	err := provider.ReleaseSandbox(context.Background(), sandbox.ProviderHandle{SandboxID: "provider_sandbox_test"})
	if !ProviderOperationWasNotSubmitted(err) {
		t.Fatalf("ReleaseSandbox error = %T %v; want pre-submission marker", err, err)
	}
	var providerErr *sandbox.ProviderError
	if !errors.As(err, &providerErr) || !providerErr.Retryable {
		t.Fatalf("ReleaseSandbox error = %T %v; want retryable provider error", err, err)
	}
}

func TestDaytonaReleaseSandboxClassifiesDeleteSubmissionBoundary(t *testing.T) {
	tests := []struct {
		name             string
		deleteErr        error
		wantNotSubmitted bool
	}{
		{name: "explicit rejection", deleteErr: daytonaerrors.NewDaytonaRateLimitError("busy", nil), wantNotSubmitted: true},
		{name: "ambiguous transport failure", deleteErr: errors.New("provider connection closed"), wantNotSubmitted: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			provider := NewDaytonaLifecycleProviderForClient(&recordingDaytonaLifecycleClient{}, 45*time.Second)
			provider.deleteSandbox = func(context.Context, *daytona.Sandbox, time.Duration) error {
				return test.deleteErr
			}
			err := provider.ReleaseSandbox(context.Background(), sandbox.ProviderHandle{SandboxID: "provider_sandbox_test"})
			if err == nil {
				t.Fatal("ReleaseSandbox succeeded despite delete failure")
			}
			if got := ProviderOperationWasNotSubmitted(err); got != test.wantNotSubmitted {
				t.Fatalf("ProviderOperationWasNotSubmitted = %t; want %t", got, test.wantNotSubmitted)
			}
		})
	}
}

func TestDaytonaStartSandboxClassifiesLookupBeforeStart(t *testing.T) {
	provider := NewDaytonaLifecycleProviderForClient(&recordingDaytonaLifecycleClient{
		getErr: errors.New("temporary provider lookup failure"),
	}, 45*time.Second)
	err := provider.StartSandbox(context.Background(), sandbox.ProviderHandle{SandboxID: "provider_sandbox_test"})
	if !ProviderOperationWasNotSubmitted(err) {
		t.Fatalf("StartSandbox error = %T %v; want pre-submission marker", err, err)
	}
}

func TestDaytonaCreateSandboxRejectsMalformedNetworkPolicy(t *testing.T) {
	provider := NewDaytonaLifecycleProviderForClient(&recordingDaytonaLifecycleClient{}, 45*time.Second)
	for _, network := range []sandbox.NetworkSetup{
		{Type: "cidr_allow_list"},
		{Type: "cidr_allow_list", NetworkAllowList: "github.com"},
		{Type: "cidr_allow_list", NetworkAllowList: "10.0.0.0/8,"},
	} {
		_, err := provider.CreateSandbox(context.Background(), sandbox.CreateSandboxRequest{
			Setup: sandbox.SandboxSetup{
				SandboxID:           "sandbox_test",
				ProviderArtifactRef: "snapshot_env_test",
				Network:             network,
			},
		})
		var providerErr *sandbox.ProviderError
		if !errors.As(err, &providerErr) || providerErr.Stage != sandbox.StageCreateSandbox || providerErr.Kind != sandbox.ProviderErrorInvalidRequest {
			t.Fatalf("CreateSandbox(%+v) error = %T %v; want create_sandbox invalid_request", network, err, err)
		}
		if !ProviderOperationWasNotSubmitted(err) {
			t.Fatalf("CreateSandbox(%+v) error = %T %v; want pre-submission marker", network, err, err)
		}
	}
}

func TestDaytonaCreateSandboxMarksRateLimitRejectionAsNotSubmitted(t *testing.T) {
	client := &recordingDaytonaLifecycleClient{
		createErr: daytonaerrors.NewDaytonaRateLimitError("busy", nil),
	}
	provider := NewDaytonaLifecycleProviderForClient(client, time.Minute)
	_, err := provider.CreateSandbox(context.Background(), sandbox.CreateSandboxRequest{Setup: sandbox.SandboxSetup{
		SandboxID:           "sbox_rate_limited",
		ProviderArtifactRef: "snapshot_rate_limited",
	}})
	if err == nil {
		t.Fatal("CreateSandbox succeeded despite provider rate limit")
	}
	if !ProviderOperationWasNotSubmitted(err) {
		t.Fatalf("CreateSandbox error = %T %v; want not-submitted marker", err, err)
	}
}

type recordingDaytonaLifecycleClient struct {
	createParams any
	createErr    error
	getResult    *daytona.Sandbox
	returnNilGet bool
	getErr       error
}

func (c *recordingDaytonaLifecycleClient) Create(_ context.Context, params any, _ ...func(*options.CreateSandbox)) (*daytona.Sandbox, error) {
	c.createParams = params
	if c.createErr != nil {
		return nil, c.createErr
	}
	return &daytona.Sandbox{ID: "provider_sandbox_test"}, nil
}

func (c *recordingDaytonaLifecycleClient) Get(context.Context, string) (*daytona.Sandbox, error) {
	if c.getErr != nil {
		return nil, c.getErr
	}
	if c.returnNilGet {
		return nil, nil
	}
	if c.getResult != nil {
		return c.getResult, nil
	}
	return &daytona.Sandbox{ID: "provider_sandbox_test"}, nil
}

func stringPtr(value string) *string {
	return &value
}

func intPtr(value int) *int {
	return &value
}

func testHelperCommandEnvelope(t *testing.T, tool string, status string, taskID string, exitCode *int, stdout string, totalBytes int64) string {
	t.Helper()
	result := map[string]any{
		"task_id": taskID,
		"stdout": foregroundStreamSnapshot{
			Text:          stdout,
			TotalBytes:    totalBytes,
			TotalLines:    int64(strings.Count(stdout, "\n")),
			ReturnedBytes: len(stdout),
			Truncated:     false,
		},
		"stderr": foregroundStreamSnapshot{},
	}
	if exitCode != nil {
		result["exit_code"] = *exitCode
	}
	body, err := json.Marshal(map[string]any{
		"schema_version": 1,
		"tool":           tool,
		"status":         status,
		"truncated":      false,
		"error":          nil,
		"result":         result,
	})
	if err != nil {
		t.Fatalf("marshal helper command envelope: %v", err)
	}
	return string(body)
}
