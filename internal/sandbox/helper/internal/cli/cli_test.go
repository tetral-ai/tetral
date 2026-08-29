package cli

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/sandbox/helper/internal/health"
	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
)

func TestMainKeepsStderrDiagnosticsOutOfStdout(t *testing.T) {
	if os.Getenv("TETRAL_HELPER_STDERR_PURITY") == "1" {
		var stdout bytes.Buffer
		code := Main(context.Background(), []string{"not-a-tool"}, &stdout)
		_, _ = os.Stderr.WriteString("diagnostic that must not reach stdout\n")
		if code != 1 || stdout.Len() != 0 {
			_, _ = os.Stdout.Write(stdout.Bytes())
			os.Exit(2)
		}
		os.Exit(0)
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestMainKeepsStderrDiagnosticsOutOfStdout")
	cmd.Env = append(os.Environ(), "TETRAL_HELPER_STDERR_PURITY=1")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("stderr purity helper failed: %v stdout=%q stderr=%q", err, stdout.String(), stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("helper stdout = %q; want no stderr diagnostics on stdout", stdout.String())
	}
}

func TestRunReadEmitsContractEnvelopeAndUnlinksPayload(t *testing.T) {
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "note.txt"), []byte("hello\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	payloadPath := writeCLIPayload(t, protocol.Payload{
		SchemaVersion:  protocol.SchemaVersion,
		Tool:           "read",
		ToolUseEventID: "evt_read_1",
		WorkspaceRoot:  workspace,
		Roots:          []protocol.Root{{Path: workspace, Mode: protocol.RootModeReadWrite}},
		Limits:         protocol.Limits{VisibleBytes: 256 * 1024, VisibleLines: 2000},
		Input:          json.RawMessage(`{"path":"note.txt"}`),
	})
	var stdout bytes.Buffer
	if code := runRead(&stdout, payloadPath); code != 0 {
		t.Fatalf("runRead exit = %d; want 0", code)
	}
	if _, err := os.Stat(payloadPath); !os.IsNotExist(err) {
		t.Fatalf("payload stat err = %v; want removed payload", err)
	}
	var envelope protocol.Envelope
	if err := json.Unmarshal(stdout.Bytes(), &envelope); err != nil {
		t.Fatalf("decode envelope: %v\n%s", err, stdout.String())
	}
	assertNoForbiddenHelperEnvelopeKeys(t, stdout.Bytes())
	if envelope.SchemaVersion != protocol.SchemaVersion || envelope.Tool != "read" || envelope.Status != protocol.ToolStatusSuccess {
		t.Fatalf("envelope = %+v; want read success contract envelope", envelope)
	}
	var result map[string]any
	if err := json.Unmarshal(envelope.Result, &result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if result["content"] != "hello\n" || result["returned_lines"] != float64(1) {
		t.Fatalf("result = %+v; want read content", result)
	}
}

func TestHiddenSupervisorEntrypointsRejectDirectInvocation(t *testing.T) {
	if mode := os.Getenv("TETRAL_HELPER_DIRECT_SUPERVISOR_MODE"); mode != "" {
		cwd := t.TempDir()
		args := []string{"--task-id", "task_direct", "--cmd", "true", "--cwd", cwd, "--env-json", `[]`, "--lifetime-ms", "1000"}
		var stdout bytes.Buffer
		if code := Main(context.Background(), append([]string{mode}, args...), &stdout); code != 1 || stdout.Len() != 0 {
			os.Exit(2)
		}
		os.Exit(0)
	}

	for _, mode := range []string{"__supervise", "__supervise-bootstrap"} {
		t.Run(mode, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=TestHiddenSupervisorEntrypointsRejectDirectInvocation")
			cmd.Env = append(os.Environ(), "TETRAL_HELPER_DIRECT_SUPERVISOR_MODE="+mode)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("Main(%s) subprocess failed: %v output=%q", mode, err, output)
			}
		})
	}
}

func TestRunReadTargetIOFailureEmitsToolErrorEnvelope(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("chmod 000 remains readable as root")
	}
	workspace := t.TempDir()
	secretPath := filepath.Join(workspace, "secret.txt")
	if err := os.WriteFile(secretPath, []byte("secret\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	if err := os.Chmod(secretPath, 0); err != nil {
		t.Fatalf("chmod fixture: %v", err)
	}
	defer func() { _ = os.Chmod(secretPath, 0o600) }()
	payloadPath := writeCLIPayload(t, protocol.Payload{
		SchemaVersion:  protocol.SchemaVersion,
		Tool:           "read",
		ToolUseEventID: "evt_read_io_failure",
		WorkspaceRoot:  workspace,
		Roots:          []protocol.Root{{Path: workspace, Mode: protocol.RootModeReadWrite}},
		Limits:         protocol.Limits{VisibleBytes: 256 * 1024, VisibleLines: 2000},
		Input:          json.RawMessage(`{"path":"secret.txt"}`),
	})

	var stdout bytes.Buffer
	if code := runRead(&stdout, payloadPath); code != 0 {
		t.Fatalf("runRead exit = %d; want contract envelope", code)
	}
	var envelope protocol.Envelope
	if err := json.Unmarshal(stdout.Bytes(), &envelope); err != nil {
		t.Fatalf("decode envelope: %v\n%s", err, stdout.String())
	}
	if envelope.SchemaVersion != protocol.SchemaVersion || envelope.Tool != "read" || envelope.Status != protocol.ToolStatusError {
		t.Fatalf("envelope = %+v; want read tool error envelope", envelope)
	}
	if envelope.Error == nil || envelope.Error.Kind != "not_found" || envelope.Error.Message != "path could not be read" || envelope.Error.Path != "secret.txt" {
		t.Fatalf("envelope error = %+v; want unreadable target tool error", envelope.Error)
	}
	if _, err := os.Stat(payloadPath); !os.IsNotExist(err) {
		t.Fatalf("payload stat err = %v; want removed payload", err)
	}
}

func TestRunReadPrivilegeDropFailureExitsWithoutHelperFailureEnvelope(t *testing.T) {
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "note.txt"), []byte("hello\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	payloadPath := writeCLIPayload(t, protocol.Payload{
		SchemaVersion:  protocol.SchemaVersion,
		Tool:           "read",
		ToolUseEventID: "evt_read_drop_failure",
		WorkspaceRoot:  workspace,
		Roots:          []protocol.Root{{Path: workspace, Mode: protocol.RootModeReadWrite}},
		Limits:         protocol.Limits{VisibleBytes: 256 * 1024, VisibleLines: 2000},
		Input:          json.RawMessage(`{"path":"note.txt"}`),
	})
	oldEUID := currentEUID
	oldStat := statRuntimeIdentity
	t.Cleanup(func() {
		currentEUID = oldEUID
		statRuntimeIdentity = oldStat
	})
	actualEUID := os.Geteuid()
	euidCalls := 0
	currentEUID = func() int {
		euidCalls++
		if euidCalls <= 2 {
			return actualEUID
		}
		return 0
	}
	statRuntimeIdentity = func(string) (runtimeIdentity, error) {
		return runtimeIdentity{}, errors.New("runtime root owner missing")
	}

	var stdout bytes.Buffer
	if code := runRead(&stdout, payloadPath); code != 1 {
		t.Fatalf("runRead exit = %d; want helper failure nonzero; stdout=%q", code, stdout.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q; want no helper_failure tool envelope", stdout.String())
	}
	if _, err := os.Stat(payloadPath); !os.IsNotExist(err) {
		t.Fatalf("payload stat err = %v; want removed payload", err)
	}
}

func TestMainInvalidInvocationExitsNonzeroWithoutLegacyEnvelope(t *testing.T) {
	cases := [][]string{
		{"read"},
		{"unknown", "--payload", "/tmp/payload.json"},
	}
	if rawIndex := os.Getenv("TETRAL_HELPER_INVALID_INVOCATION_INDEX"); rawIndex != "" {
		index, err := strconv.Atoi(rawIndex)
		if err != nil || index < 0 || index >= len(cases) {
			os.Exit(2)
		}
		var stdout bytes.Buffer
		if code := Main(context.Background(), cases[index], &stdout); code == 0 || stdout.Len() != 0 {
			os.Exit(2)
		}
		os.Exit(0)
	}

	for index, args := range cases {
		cmd := exec.Command(os.Args[0], "-test.run=TestMainInvalidInvocationExitsNonzeroWithoutLegacyEnvelope")
		cmd.Env = append(os.Environ(), "TETRAL_HELPER_INVALID_INVOCATION_INDEX="+strconv.Itoa(index))
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("Main(%v) subprocess failed: %v output=%q", args, err, output)
		}
	}
}

func TestRunExecEmitsForegroundContractEnvelopeAndUnlinksPayload(t *testing.T) {
	workspace := t.TempDir()
	payloadPath := writeCLIPayload(t, protocol.Payload{
		SchemaVersion:  protocol.SchemaVersion,
		Tool:           "exec",
		ToolUseEventID: "evt_exec_1",
		WorkspaceRoot:  workspace,
		Roots:          []protocol.Root{{Path: workspace, Mode: protocol.RootModeReadWrite}},
		Limits:         protocol.Limits{VisibleBytes: 50 * 1024, VisibleLines: 2000},
		Input:          json.RawMessage(`{"cmd":"printf hello","on_wait_expiry":"kill","wait_ms":1000}`),
	})
	var stdout bytes.Buffer
	if code := runExec(&stdout, payloadPath); code != 0 {
		t.Fatalf("runExec exit = %d; want 0", code)
	}
	if _, err := os.Stat(payloadPath); !os.IsNotExist(err) {
		t.Fatalf("payload stat err = %v; want removed payload", err)
	}
	var envelope protocol.Envelope
	if err := json.Unmarshal(stdout.Bytes(), &envelope); err != nil {
		t.Fatalf("decode envelope: %v\n%s", err, stdout.String())
	}
	if envelope.SchemaVersion != protocol.SchemaVersion || envelope.Tool != "exec" || envelope.Status != protocol.ToolStatusSuccess {
		t.Fatalf("envelope = %+v; want exec success contract envelope", envelope)
	}
	var result map[string]any
	if err := json.Unmarshal(envelope.Result, &result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	stdoutResult := result["stdout"].(map[string]any)
	if stdoutResult["text"] != "hello" || result["timed_out"] != false {
		t.Fatalf("result = %+v; want stdout hello", result)
	}
}

func TestDropPrivilegesForToolSwitchesRootHelperToRuntimeUser(t *testing.T) {
	oldEUID := currentEUID
	oldStat := statRuntimeIdentity
	oldSetGID := setRuntimeGID
	oldSetUID := setRuntimeUID
	oldClearGroups := clearRuntimeGroups
	oldNormalizeEnv := normalizeRuntimeEnv
	t.Cleanup(func() {
		currentEUID = oldEUID
		statRuntimeIdentity = oldStat
		setRuntimeGID = oldSetGID
		setRuntimeUID = oldSetUID
		clearRuntimeGroups = oldClearGroups
		normalizeRuntimeEnv = oldNormalizeEnv
	})

	currentEUID = func() int { return 0 }
	statRuntimeIdentity = func(path string) (runtimeIdentity, error) {
		if path != "/workspace" {
			t.Fatalf("stat runtime root = %q; want workspace root", path)
		}
		return runtimeIdentity{uid: 4242, gid: 4343}, nil
	}
	var calls []string
	clearRuntimeGroups = func() error {
		calls = append(calls, "groups:clear")
		return nil
	}
	setRuntimeGID = func(gid int) error {
		calls = append(calls, "gid:"+strconv.Itoa(gid))
		return nil
	}
	setRuntimeUID = func(uid int) error {
		calls = append(calls, "uid:"+strconv.Itoa(uid))
		return nil
	}
	normalizeRuntimeEnv = func() error {
		calls = append(calls, "env:normalize")
		return nil
	}

	if toolErr := dropPrivilegesForTool("/workspace"); toolErr != nil {
		t.Fatalf("dropPrivilegesForTool error = %+v", toolErr)
	}
	if strings.Join(calls, ",") != "groups:clear,gid:4343,uid:4242,env:normalize" {
		t.Fatalf("drop calls = %v; want groups, gid, uid, then runtime environment", calls)
	}
}

func TestDropPrivilegesForToolFailsClosedWhenRuntimeEnvironmentCannotBeNormalized(t *testing.T) {
	oldEUID := currentEUID
	oldStat := statRuntimeIdentity
	oldSetGID := setRuntimeGID
	oldSetUID := setRuntimeUID
	oldClearGroups := clearRuntimeGroups
	oldNormalizeEnv := normalizeRuntimeEnv
	t.Cleanup(func() {
		currentEUID = oldEUID
		statRuntimeIdentity = oldStat
		setRuntimeGID = oldSetGID
		setRuntimeUID = oldSetUID
		clearRuntimeGroups = oldClearGroups
		normalizeRuntimeEnv = oldNormalizeEnv
	})
	currentEUID = func() int { return 0 }
	statRuntimeIdentity = func(string) (runtimeIdentity, error) {
		return runtimeIdentity{uid: 4242, gid: 4343}, nil
	}
	clearRuntimeGroups = func() error { return nil }
	setRuntimeGID = func(int) error { return nil }
	setRuntimeUID = func(int) error { return nil }
	normalizeRuntimeEnv = func() error { return errors.New("environment rejected") }

	toolErr := dropPrivilegesForTool("/workspace")
	if toolErr == nil || toolErr.Kind != health.HelperFailureKind || !strings.Contains(toolErr.Message, "environment normalization") {
		t.Fatalf("dropPrivilegesForTool error = %+v; want catastrophic environment failure", toolErr)
	}
}

func TestDropPrivilegesForToolFailsClosedWhenSupplementaryGroupsCannotBeCleared(t *testing.T) {
	oldEUID := currentEUID
	oldStat := statRuntimeIdentity
	oldSetGID := setRuntimeGID
	oldSetUID := setRuntimeUID
	oldClearGroups := clearRuntimeGroups
	t.Cleanup(func() {
		currentEUID = oldEUID
		statRuntimeIdentity = oldStat
		setRuntimeGID = oldSetGID
		setRuntimeUID = oldSetUID
		clearRuntimeGroups = oldClearGroups
	})
	currentEUID = func() int { return 0 }
	statRuntimeIdentity = func(string) (runtimeIdentity, error) {
		return runtimeIdentity{uid: 4242, gid: 4343}, nil
	}
	clearRuntimeGroups = func() error { return errors.New("setgroups denied") }
	setRuntimeGID = func(int) error { t.Fatal("setgid ran after setgroups failure"); return nil }
	setRuntimeUID = func(int) error { t.Fatal("setuid ran after setgroups failure"); return nil }

	toolErr := dropPrivilegesForTool("/workspace")
	if toolErr == nil || toolErr.Kind != health.HelperFailureKind || !strings.Contains(toolErr.Message, "supplementary group") {
		t.Fatalf("dropPrivilegesForTool error = %+v; want catastrophic supplementary-group failure", toolErr)
	}
}

func TestDropPrivilegesForToolRejectsRootRuntimeIdentity(t *testing.T) {
	oldEUID := currentEUID
	oldStat := statRuntimeIdentity
	t.Cleanup(func() {
		currentEUID = oldEUID
		statRuntimeIdentity = oldStat
	})
	currentEUID = func() int { return 0 }
	statRuntimeIdentity = func(string) (runtimeIdentity, error) {
		return runtimeIdentity{uid: 0, gid: 0}, nil
	}

	toolErr := dropPrivilegesForTool("/workspace")
	if toolErr == nil || toolErr.Kind != "helper_failure" || !strings.Contains(toolErr.Message, "must not be root") {
		t.Fatalf("dropPrivilegesForTool error = %+v; want root runtime identity rejection", toolErr)
	}
}

func TestBuiltHelperDetachedExecReturnsPromptly(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("detached helper authorization requires the production root helper identity")
	}
	runtimeRoot := filepath.Dir(payloadRoot)
	if _, err := os.Lstat(runtimeRoot); !os.IsNotExist(err) {
		t.Fatalf("root helper proof requires an unused runtime root, stat error = %v", err)
	}
	if err := os.MkdirAll(runtimeRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(runtimeRoot) })
	const runtimeUID, runtimeGID = 65534, 65534
	if err := os.Chown(runtimeRoot, runtimeUID, runtimeGID); err != nil {
		t.Fatal(err)
	}
	bin := buildSandboxHelper(t)
	workspace, err := os.MkdirTemp("/tmp", "tetral-cli-runtime-workspace-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(workspace) })
	if err := os.Chown(workspace, runtimeUID, runtimeGID); err != nil {
		t.Fatal(err)
	}
	taskID := "task_cli_real_detach"
	_ = os.RemoveAll(filepath.Join("/tmp/tetral-runtime/tasks", taskID))
	t.Cleanup(func() { _ = os.RemoveAll(filepath.Join("/tmp/tetral-runtime/tasks", taskID)) })
	payloadPath := writeCLIPayload(t, protocol.Payload{
		SchemaVersion:  protocol.SchemaVersion,
		Tool:           "exec",
		ToolUseEventID: "evt_exec_real_detach",
		WorkspaceRoot:  workspace,
		Roots:          []protocol.Root{{Path: workspace, Mode: protocol.RootModeReadWrite}},
		Limits:         protocol.Limits{VisibleBytes: 50 * 1024, VisibleLines: 2000},
		Input:          json.RawMessage(`{"cmd":"sleep 5","on_wait_expiry":"detach","task_id":"` + taskID + `","wait_ms":250,"task_lifetime_ms":30000}`),
	})
	startedAt := time.Now()
	output, err := exec.Command(bin, "exec", "--payload", payloadPath).Output()
	if err != nil {
		t.Fatalf("exec detach helper: %v\n%s", err, string(output))
	}
	if elapsed := time.Since(startedAt); elapsed > 1500*time.Millisecond {
		t.Fatalf("detach elapsed = %s; supervisor likely held stdio open", elapsed)
	}
	if _, err := os.Stat(payloadPath); !os.IsNotExist(err) {
		t.Fatalf("payload stat err = %v; want removed payload", err)
	}
	var envelope protocol.Envelope
	if err := json.Unmarshal(output, &envelope); err != nil {
		t.Fatalf("decode exec envelope: %v\n%s", err, string(output))
	}
	if envelope.Status != protocol.ToolStatusRunning {
		t.Fatalf("exec status/error/result = %s/%+v/%s; want running", envelope.Status, envelope.Error, envelope.Result)
	}
	cancelPayload := writeCLIPayload(t, protocol.Payload{
		SchemaVersion:  protocol.SchemaVersion,
		Tool:           "cancel",
		ToolUseEventID: "evt_cancel_real_detach",
		WorkspaceRoot:  workspace,
		Roots:          []protocol.Root{{Path: workspace, Mode: protocol.RootModeReadWrite}},
		Limits:         protocol.Limits{VisibleBytes: 50 * 1024, VisibleLines: 2000},
		Input:          json.RawMessage(`{"task_id":"` + taskID + `"}`),
	})
	cancelOutput, err := exec.Command(bin, "cancel", "--payload", cancelPayload).Output()
	if err != nil {
		t.Fatalf("cancel helper: %v\n%s", err, string(cancelOutput))
	}
	var cancelEnvelope protocol.Envelope
	if err := json.Unmarshal(cancelOutput, &cancelEnvelope); err != nil {
		t.Fatalf("decode cancel envelope: %v\n%s", err, string(cancelOutput))
	}
	assertNoForbiddenHelperEnvelopeKeys(t, cancelOutput)
	var cancelResult struct {
		Cancelled bool `json:"cancelled"`
	}
	if err := json.Unmarshal(cancelEnvelope.Result, &cancelResult); err != nil {
		t.Fatalf("decode cancel result: %v", err)
	}
	if cancelEnvelope.Status != protocol.ToolStatusSuccess || !cancelResult.Cancelled {
		t.Fatalf("cancel envelope = %+v; want cancelled success", cancelEnvelope)
	}
}

func TestBuiltHelperRejectsForgedSupervisorPipeWithoutStartingTask(t *testing.T) {
	bin := buildSandboxHelper(t)
	taskID := "task_forged_supervisor_pipe"
	taskPath := filepath.Join("/tmp/tetral-runtime/tasks", taskID)
	_ = os.RemoveAll(taskPath)
	t.Cleanup(func() { _ = os.RemoveAll(taskPath) })
	readFile, writeFile, err := os.Pipe()
	if err != nil {
		t.Fatalf("create forged supervisor pipe: %v", err)
	}
	if _, err := writeFile.WriteString("tetral-supervisor-reexec-v2\n"); err != nil {
		t.Fatalf("write forged supervisor marker: %v", err)
	}
	_ = writeFile.Close()
	defer func() { _ = readFile.Close() }()
	command := exec.Command(bin,
		"__supervise-bootstrap",
		"--task-id", taskID,
		"--cmd", "touch /tmp/forged-supervisor-child",
		"--cwd", t.TempDir(),
		"--env-json", "[]",
		"--lifetime-ms", "1000",
	)
	command.ExtraFiles = []*os.File{readFile}
	command.Env = append(os.Environ(), "TETRAL_SUPERVISOR_AUTH_FD=3")
	if err := command.Run(); err == nil {
		t.Fatal("built helper accepted a runtime-forgeable supervisor pipe")
	}
	if _, err := os.Stat(taskPath); !os.IsNotExist(err) {
		t.Fatalf("forged supervisor created task state: %v", err)
	}
}

func TestBuiltHelperSubcommandsEmitSingleJSONEnvelope(t *testing.T) {
	bin := buildSandboxHelper(t)

	runPayload := func(t *testing.T, payload protocol.Payload) protocol.Envelope {
		t.Helper()
		payloadPath := writeCLIPayload(t, payload)
		output, err := exec.Command(bin, payload.Tool, "--payload", payloadPath).Output()
		if err != nil {
			t.Fatalf("%s helper: %v\n%s", payload.Tool, err, string(output))
		}
		if _, err := os.Stat(payloadPath); !os.IsNotExist(err) {
			t.Fatalf("%s payload stat err = %v; want removed payload", payload.Tool, err)
		}
		envelope := decodeSingleEnvelope(t, output)
		assertNoForbiddenHelperEnvelopeKeys(t, output)
		if envelope.Tool != payload.Tool {
			t.Fatalf("envelope tool = %q; want %q", envelope.Tool, payload.Tool)
		}
		return envelope
	}

	t.Run("exec", func(t *testing.T) {
		workspace := t.TempDir()
		eventID := "evt_built_exec"
		payloadPath := cliPayloadPath(t, eventID)
		inputBody, err := json.Marshal(map[string]any{
			"cmd":            `if [ -e ` + strconv.Quote(payloadPath) + ` ]; then printf still-present; else printf unlinked; fi`,
			"on_wait_expiry": "kill",
			"wait_ms":        1000,
		})
		if err != nil {
			t.Fatalf("marshal exec input: %v", err)
		}
		body, err := json.Marshal(protocol.Payload{
			SchemaVersion:  protocol.SchemaVersion,
			Tool:           "exec",
			ToolUseEventID: eventID,
			WorkspaceRoot:  workspace,
			Roots:          []protocol.Root{{Path: workspace, Mode: protocol.RootModeReadWrite}},
			Limits:         protocol.Limits{VisibleBytes: 50 * 1024, VisibleLines: 2000},
			Input:          inputBody,
		})
		if err != nil {
			t.Fatalf("marshal exec payload: %v", err)
		}
		if err := os.WriteFile(payloadPath, body, 0o600); err != nil {
			t.Fatalf("write exec payload: %v", err)
		}
		output, err := exec.Command(bin, "exec", "--payload", payloadPath).Output()
		if err != nil {
			t.Fatalf("exec helper: %v\n%s", err, string(output))
		}
		if _, err := os.Stat(payloadPath); !os.IsNotExist(err) {
			t.Fatalf("exec payload stat err = %v; want removed payload", err)
		}
		envelope := decodeSingleEnvelope(t, output)
		if envelope.Status != protocol.ToolStatusSuccess {
			t.Fatalf("exec envelope = %+v; want success", envelope)
		}
		var result map[string]any
		if err := json.Unmarshal(envelope.Result, &result); err != nil {
			t.Fatalf("decode exec result: %v", err)
		}
		stdoutResult := result["stdout"].(map[string]any)
		if stdoutResult["text"] != "unlinked" {
			t.Fatalf("exec stdout = %+v; want payload already unlinked before command", stdoutResult)
		}
	})

	for _, tc := range []struct {
		name    string
		tool    string
		input   string
		assert  func(*testing.T, protocol.Envelope, string)
		fixture func(*testing.T, string)
	}{
		{
			name:  "stdin",
			tool:  "stdin",
			input: `{"task_id":"task_built_missing","chars":"x","write_seq":1,"wait_ms":0}`,
			assert: func(t *testing.T, envelope protocol.Envelope, _ string) {
				t.Helper()
				assertEnvelopeErrorKind(t, envelope, "task_not_found")
			},
		},
		{
			name:  "poll",
			tool:  "poll",
			input: `{"task_id":"task_built_missing","wait_ms":0}`,
			assert: func(t *testing.T, envelope protocol.Envelope, _ string) {
				t.Helper()
				assertEnvelopeErrorKind(t, envelope, "task_not_found")
			},
		},
		{
			name:  "cancel",
			tool:  "cancel",
			input: `{"task_id":"task_built_missing","wait_ms":0}`,
			assert: func(t *testing.T, envelope protocol.Envelope, _ string) {
				t.Helper()
				assertEnvelopeErrorKind(t, envelope, "task_not_found")
			},
		},
		{
			name:  "read",
			tool:  "read",
			input: `{"path":"note.txt"}`,
			fixture: func(t *testing.T, workspace string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(workspace, "note.txt"), []byte("hello\n"), 0o600); err != nil {
					t.Fatalf("write read fixture: %v", err)
				}
			},
			assert: func(t *testing.T, envelope protocol.Envelope, _ string) {
				t.Helper()
				assertEnvelopeStatus(t, envelope, protocol.ToolStatusSuccess)
			},
		},
		{
			name:  "write",
			tool:  "write",
			input: `{"path":"created.txt","content":"created"}`,
			assert: func(t *testing.T, envelope protocol.Envelope, workspace string) {
				t.Helper()
				assertEnvelopeStatus(t, envelope, protocol.ToolStatusSuccess)
				if got, err := os.ReadFile(filepath.Join(workspace, "created.txt")); err != nil || string(got) != "created" {
					t.Fatalf("created file = %q, %v; want created", string(got), err)
				}
			},
		},
		{
			name:  "edit",
			tool:  "edit",
			input: `{"path":"edit.txt","old_string":"old","new_string":"new"}`,
			fixture: func(t *testing.T, workspace string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(workspace, "edit.txt"), []byte("old value"), 0o600); err != nil {
					t.Fatalf("write edit fixture: %v", err)
				}
			},
			assert: func(t *testing.T, envelope protocol.Envelope, workspace string) {
				t.Helper()
				assertEnvelopeStatus(t, envelope, protocol.ToolStatusSuccess)
				if got, err := os.ReadFile(filepath.Join(workspace, "edit.txt")); err != nil || string(got) != "new value" {
					t.Fatalf("edited file = %q, %v; want new value", string(got), err)
				}
			},
		},
		{
			name:  "apply_patch",
			tool:  "apply_patch",
			input: `{"patch":"*** Begin Patch\n*** Add File: patched.txt\n+patched\n*** End Patch"}`,
			assert: func(t *testing.T, envelope protocol.Envelope, workspace string) {
				t.Helper()
				assertEnvelopeStatus(t, envelope, protocol.ToolStatusSuccess)
				if got, err := os.ReadFile(filepath.Join(workspace, "patched.txt")); err != nil || string(got) != "patched\n" {
					t.Fatalf("patched file = %q, %v; want patched", string(got), err)
				}
			},
		},
		{
			name:  "grep",
			tool:  "grep",
			input: `{"pattern":"needle","mode":"files"}`,
			fixture: func(t *testing.T, workspace string) {
				t.Helper()
				requireRG(t)
				if err := os.WriteFile(filepath.Join(workspace, "needle.txt"), []byte("needle\n"), 0o600); err != nil {
					t.Fatalf("write grep fixture: %v", err)
				}
			},
			assert: func(t *testing.T, envelope protocol.Envelope, _ string) {
				t.Helper()
				assertEnvelopeStatus(t, envelope, protocol.ToolStatusSuccess)
			},
		},
		{
			name:  "glob",
			tool:  "glob",
			input: `{"pattern":"*.go"}`,
			fixture: func(t *testing.T, workspace string) {
				t.Helper()
				requireRG(t)
				if err := os.WriteFile(filepath.Join(workspace, "main.go"), []byte("package main\n"), 0o600); err != nil {
					t.Fatalf("write glob fixture: %v", err)
				}
			},
			assert: func(t *testing.T, envelope protocol.Envelope, _ string) {
				t.Helper()
				assertEnvelopeStatus(t, envelope, protocol.ToolStatusSuccess)
			},
		},
		{
			name:  "view_image",
			tool:  "view_image",
			input: `{"path":"image.bin"}`,
			fixture: func(t *testing.T, workspace string) {
				t.Helper()
				body := append([]byte{0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a}, []byte("png-body")...)
				if err := os.WriteFile(filepath.Join(workspace, "image.bin"), body, 0o600); err != nil {
					t.Fatalf("write image fixture: %v", err)
				}
			},
			assert: func(t *testing.T, envelope protocol.Envelope, _ string) {
				t.Helper()
				assertEnvelopeStatus(t, envelope, protocol.ToolStatusSuccess)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			workspace := t.TempDir()
			if tc.fixture != nil {
				tc.fixture(t, workspace)
			}
			envelope := runPayload(t, protocol.Payload{
				SchemaVersion:  protocol.SchemaVersion,
				Tool:           tc.tool,
				ToolUseEventID: "evt_built_" + strings.ReplaceAll(tc.name, "_", ""),
				WorkspaceRoot:  workspace,
				Roots:          []protocol.Root{{Path: workspace, Mode: protocol.RootModeReadWrite}},
				Limits:         protocol.Limits{VisibleBytes: 50 * 1024, VisibleLines: 2000},
				Input:          json.RawMessage(tc.input),
			})
			tc.assert(t, envelope, workspace)
		})
	}

	t.Run("health", func(t *testing.T) {
		command := exec.Command(bin, "health")
		command.Env = append(os.Environ(), "PATH="+filepath.Dir(bin)+string(os.PathListSeparator)+os.Getenv("PATH"))
		output, err := command.Output()
		if err != nil {
			t.Fatalf("health helper: %v\n%s", err, string(output))
		}
		envelope := decodeSingleEnvelope(t, output)
		if envelope.Tool != "health" || envelope.Status == "" {
			t.Fatalf("health envelope = %+v; want health envelope", envelope)
		}
	})
}

func TestBuiltHelperMalformedPayloadAndExitDiscipline(t *testing.T) {
	bin := buildSandboxHelper(t)

	t.Run("malformed payload emits authoritative error envelope and leaves payload", func(t *testing.T) {
		payloadPath := cliPayloadPath(t, "evt_built_malformed")
		if err := os.WriteFile(payloadPath, []byte(`{"schema_version":`), 0o600); err != nil {
			t.Fatalf("write malformed payload: %v", err)
		}
		output, err := exec.Command(bin, "read", "--payload", payloadPath).Output()
		if err != nil {
			t.Fatalf("read malformed helper: %v\n%s", err, string(output))
		}
		envelope := decodeSingleEnvelope(t, output)
		assertEnvelopeErrorKind(t, envelope, "invalid_input")
		if _, err := os.Stat(payloadPath); err != nil {
			t.Fatalf("malformed payload stat err = %v; want left for Bridge cleanup", err)
		}
	})

	t.Run("bad invocation exits nonzero without envelope", func(t *testing.T) {
		output, err := exec.Command(bin, "read").CombinedOutput()
		if err == nil {
			t.Fatalf("bad invocation exit = 0; want nonzero")
		}
		if len(output) != 0 {
			t.Fatalf("bad invocation output = %q; want no envelope", string(output))
		}
	})
}

func TestRunCommandFollowupsEmitErrorEnvelopesAndUnlinkPayload(t *testing.T) {
	for _, tc := range []struct {
		tool  string
		run   func(io.Writer, string) int
		input string
	}{
		{tool: "stdin", run: runStdin, input: `{"task_id":"task_missing","chars":"x","write_seq":1,"wait_ms":0}`},
		{tool: "poll", run: runPoll, input: `{"task_id":"task_missing","wait_ms":0}`},
		{tool: "cancel", run: runCancel, input: `{"task_id":"task_missing","wait_ms":0}`},
	} {
		t.Run(tc.tool, func(t *testing.T) {
			workspace := t.TempDir()
			payloadPath := writeCLIPayload(t, protocol.Payload{
				SchemaVersion:  protocol.SchemaVersion,
				Tool:           tc.tool,
				ToolUseEventID: "evt_" + tc.tool + "_1",
				WorkspaceRoot:  workspace,
				Roots:          []protocol.Root{{Path: workspace, Mode: protocol.RootModeReadWrite}},
				Limits:         protocol.Limits{VisibleBytes: 50 * 1024, VisibleLines: 2000},
				Input:          json.RawMessage(tc.input),
			})
			var stdout bytes.Buffer
			if code := tc.run(&stdout, payloadPath); code != 0 {
				t.Fatalf("%s exit = %d; want 0", tc.tool, code)
			}
			if _, err := os.Stat(payloadPath); !os.IsNotExist(err) {
				t.Fatalf("payload stat err = %v; want removed payload", err)
			}
			var envelope protocol.Envelope
			if err := json.Unmarshal(stdout.Bytes(), &envelope); err != nil {
				t.Fatalf("decode envelope: %v\n%s", err, stdout.String())
			}
			assertNoForbiddenHelperEnvelopeKeys(t, stdout.Bytes())
			if envelope.SchemaVersion != protocol.SchemaVersion || envelope.Tool != tc.tool || envelope.Status != protocol.ToolStatusError ||
				envelope.Error == nil || envelope.Error.Kind != "task_not_found" {
				t.Fatalf("envelope = %+v; want %s task_not_found error", envelope, tc.tool)
			}
		})
	}
}

func TestRunWriteEmitsContractEnvelopeAndUnlinksPayload(t *testing.T) {
	workspace := t.TempDir()
	payloadPath := writeCLIPayload(t, protocol.Payload{
		SchemaVersion:  protocol.SchemaVersion,
		Tool:           "write",
		ToolUseEventID: "evt_write_1",
		WorkspaceRoot:  workspace,
		Roots:          []protocol.Root{{Path: workspace, Mode: protocol.RootModeReadWrite}},
		Input:          json.RawMessage(`{"path":"note.txt","content":"hello"}`),
	})
	var stdout bytes.Buffer
	if code := runWrite(&stdout, payloadPath); code != 0 {
		t.Fatalf("runWrite exit = %d; want 0", code)
	}
	if _, err := os.Stat(payloadPath); !os.IsNotExist(err) {
		t.Fatalf("payload stat err = %v; want removed payload", err)
	}
	if got, err := os.ReadFile(filepath.Join(workspace, "note.txt")); err != nil || string(got) != "hello" {
		t.Fatalf("written file = %q, %v; want hello", string(got), err)
	}
	var envelope protocol.Envelope
	if err := json.Unmarshal(stdout.Bytes(), &envelope); err != nil {
		t.Fatalf("decode envelope: %v\n%s", err, stdout.String())
	}
	if envelope.SchemaVersion != protocol.SchemaVersion || envelope.Tool != "write" || envelope.Status != protocol.ToolStatusSuccess {
		t.Fatalf("envelope = %+v; want write success contract envelope", envelope)
	}
	var result map[string]any
	if err := json.Unmarshal(envelope.Result, &result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if result["created"] != true || result["bytes_written"] != float64(5) {
		t.Fatalf("result = %+v; want created byte count", result)
	}
}

func TestRunEditEmitsContractEnvelopeAndUnlinksPayload(t *testing.T) {
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "note.txt"), []byte("hello world"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	payloadPath := writeCLIPayload(t, protocol.Payload{
		SchemaVersion:  protocol.SchemaVersion,
		Tool:           "edit",
		ToolUseEventID: "evt_edit_1",
		WorkspaceRoot:  workspace,
		Roots:          []protocol.Root{{Path: workspace, Mode: protocol.RootModeReadWrite}},
		Input:          json.RawMessage(`{"path":"note.txt","old_string":"world","new_string":"there"}`),
	})
	var stdout bytes.Buffer
	if code := runEdit(&stdout, payloadPath); code != 0 {
		t.Fatalf("runEdit exit = %d; want 0", code)
	}
	if _, err := os.Stat(payloadPath); !os.IsNotExist(err) {
		t.Fatalf("payload stat err = %v; want removed payload", err)
	}
	if got, err := os.ReadFile(filepath.Join(workspace, "note.txt")); err != nil || string(got) != "hello there" {
		t.Fatalf("edited file = %q, %v; want hello there", string(got), err)
	}
	var envelope protocol.Envelope
	if err := json.Unmarshal(stdout.Bytes(), &envelope); err != nil {
		t.Fatalf("decode envelope: %v\n%s", err, stdout.String())
	}
	if envelope.SchemaVersion != protocol.SchemaVersion || envelope.Tool != "edit" || envelope.Status != protocol.ToolStatusSuccess {
		t.Fatalf("envelope = %+v; want edit success contract envelope", envelope)
	}
	var result map[string]any
	if err := json.Unmarshal(envelope.Result, &result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if result["replacements"] != float64(1) || result["bytes_written"] != float64(len("hello there")) {
		t.Fatalf("result = %+v; want replacement byte count", result)
	}
}

func TestRunApplyPatchEmitsContractEnvelopeAndUnlinksPayload(t *testing.T) {
	workspace := t.TempDir()
	payloadPath := writeCLIPayload(t, protocol.Payload{
		SchemaVersion:  protocol.SchemaVersion,
		Tool:           "apply_patch",
		ToolUseEventID: "evt_patch_1",
		WorkspaceRoot:  workspace,
		Roots:          []protocol.Root{{Path: workspace, Mode: protocol.RootModeReadWrite}},
		Input:          json.RawMessage(`{"patch":"*** Begin Patch\n*** Add File: note.txt\n+hello\n*** End Patch"}`),
	})
	var stdout bytes.Buffer
	if code := runApplyPatch(&stdout, payloadPath); code != 0 {
		t.Fatalf("runApplyPatch exit = %d; want 0", code)
	}
	if _, err := os.Stat(payloadPath); !os.IsNotExist(err) {
		t.Fatalf("payload stat err = %v; want removed payload", err)
	}
	if got, err := os.ReadFile(filepath.Join(workspace, "note.txt")); err != nil || string(got) != "hello\n" {
		t.Fatalf("patched file = %q, %v; want hello", string(got), err)
	}
	var envelope protocol.Envelope
	if err := json.Unmarshal(stdout.Bytes(), &envelope); err != nil {
		t.Fatalf("decode envelope: %v\n%s", err, stdout.String())
	}
	if envelope.SchemaVersion != protocol.SchemaVersion || envelope.Tool != "apply_patch" || envelope.Status != protocol.ToolStatusSuccess {
		t.Fatalf("envelope = %+v; want apply_patch success contract envelope", envelope)
	}
	var result map[string]any
	if err := json.Unmarshal(envelope.Result, &result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	added := result["added"].([]any)
	if len(added) != 1 || added[0] != "note.txt" {
		t.Fatalf("result = %+v; want added note.txt", result)
	}
	for _, key := range []string{"modified", "deleted", "moved"} {
		if values, ok := result[key].([]any); !ok || len(values) != 0 {
			t.Fatalf("result[%s] = %#v; want empty array", key, result[key])
		}
	}
}

func TestRunGrepEmitsContractEnvelopeAndUnlinksPayload(t *testing.T) {
	requireRG(t)
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "note.txt"), []byte("needle\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	payloadPath := writeCLIPayload(t, protocol.Payload{
		SchemaVersion:  protocol.SchemaVersion,
		Tool:           "grep",
		ToolUseEventID: "evt_grep_1",
		WorkspaceRoot:  workspace,
		Roots:          []protocol.Root{{Path: workspace, Mode: protocol.RootModeReadWrite}},
		Input:          json.RawMessage(`{"pattern":"needle","mode":"files"}`),
	})
	var stdout bytes.Buffer
	if code := runGrep(&stdout, payloadPath); code != 0 {
		t.Fatalf("runGrep exit = %d; want 0", code)
	}
	if _, err := os.Stat(payloadPath); !os.IsNotExist(err) {
		t.Fatalf("payload stat err = %v; want removed payload", err)
	}
	var envelope protocol.Envelope
	if err := json.Unmarshal(stdout.Bytes(), &envelope); err != nil {
		t.Fatalf("decode envelope: %v\n%s", err, stdout.String())
	}
	if envelope.SchemaVersion != protocol.SchemaVersion || envelope.Tool != "grep" || envelope.Status != protocol.ToolStatusSuccess {
		t.Fatalf("envelope = %+v; want grep success contract envelope", envelope)
	}
	var result map[string]any
	if err := json.Unmarshal(envelope.Result, &result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if result["mode"] != "files" || !strings.Contains(result["text"].(string), "note.txt") {
		t.Fatalf("grep result = %+v; want matching file", result)
	}
}

func TestRunGlobEmitsContractEnvelopeAndUnlinksPayload(t *testing.T) {
	requireRG(t)
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "note.go"), []byte("package main\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	payloadPath := writeCLIPayload(t, protocol.Payload{
		SchemaVersion:  protocol.SchemaVersion,
		Tool:           "glob",
		ToolUseEventID: "evt_glob_1",
		WorkspaceRoot:  workspace,
		Roots:          []protocol.Root{{Path: workspace, Mode: protocol.RootModeReadWrite}},
		Input:          json.RawMessage(`{"pattern":"*.go"}`),
	})
	var stdout bytes.Buffer
	if code := runGlob(&stdout, payloadPath); code != 0 {
		t.Fatalf("runGlob exit = %d; want 0", code)
	}
	if _, err := os.Stat(payloadPath); !os.IsNotExist(err) {
		t.Fatalf("payload stat err = %v; want removed payload", err)
	}
	var envelope protocol.Envelope
	if err := json.Unmarshal(stdout.Bytes(), &envelope); err != nil {
		t.Fatalf("decode envelope: %v\n%s", err, stdout.String())
	}
	if envelope.SchemaVersion != protocol.SchemaVersion || envelope.Tool != "glob" || envelope.Status != protocol.ToolStatusSuccess {
		t.Fatalf("envelope = %+v; want glob success contract envelope", envelope)
	}
	var result map[string]any
	if err := json.Unmarshal(envelope.Result, &result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	paths, ok := result["paths"].([]any)
	if !ok {
		t.Fatalf("glob paths = %#v; want JSON array", result["paths"])
	}
	if len(paths) != 1 || paths[0] != "note.go" {
		t.Fatalf("glob paths = %+v; want note.go", paths)
	}
}

func TestRunViewImageEmitsContractEnvelopeAndUnlinksPayload(t *testing.T) {
	workspace := t.TempDir()
	body := append([]byte{0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a}, []byte("png-body")...)
	if err := os.WriteFile(filepath.Join(workspace, "image.anything"), body, 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	payloadPath := writeCLIPayload(t, protocol.Payload{
		SchemaVersion:  protocol.SchemaVersion,
		Tool:           "view_image",
		ToolUseEventID: "evt_view_image_1",
		WorkspaceRoot:  workspace,
		Roots:          []protocol.Root{{Path: workspace, Mode: protocol.RootModeReadWrite}},
		Input:          json.RawMessage(`{"path":"image.anything"}`),
	})
	var stdout bytes.Buffer
	if code := runViewImage(&stdout, payloadPath); code != 0 {
		t.Fatalf("runViewImage exit = %d; want 0", code)
	}
	if _, err := os.Stat(payloadPath); !os.IsNotExist(err) {
		t.Fatalf("payload stat err = %v; want removed payload", err)
	}
	var envelope protocol.Envelope
	if err := json.Unmarshal(stdout.Bytes(), &envelope); err != nil {
		t.Fatalf("decode envelope: %v\n%s", err, stdout.String())
	}
	if envelope.SchemaVersion != protocol.SchemaVersion || envelope.Tool != "view_image" || envelope.Status != protocol.ToolStatusSuccess {
		t.Fatalf("envelope = %+v; want view_image success contract envelope", envelope)
	}
	if envelope.Truncated == nil || *envelope.Truncated {
		t.Fatalf("truncated = %v; want explicit false", envelope.Truncated)
	}
	var result map[string]any
	if err := json.Unmarshal(envelope.Result, &result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if result["mime"] != "image/png" || result["size_bytes"] != float64(len(body)) || result["data_base64"] != base64.StdEncoding.EncodeToString(body) {
		t.Fatalf("result = %+v; want PNG mime, size and base64 body", result)
	}
}

func TestLoadPayloadLeavesMalformedPayloadForBridgeCleanup(t *testing.T) {
	payloadPath := cliPayloadPath(t, "evt_malformed")
	if err := os.WriteFile(payloadPath, []byte(`{"schema_version":`), 0o600); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	_, toolErr := loadPayload(payloadPath, "read")
	if toolErr == nil || toolErr.Kind != "invalid_input" {
		t.Fatalf("loadPayload error = %+v; want invalid_input", toolErr)
	}
	if _, err := os.Stat(payloadPath); err != nil {
		t.Fatalf("payload removed on parse failure: %v", err)
	}
}

func TestLoadPayloadRejectsNonCanonicalPayloadPath(t *testing.T) {
	payloadPath := filepath.Join(t.TempDir(), "payload.json")
	body, err := json.Marshal(protocol.Payload{
		SchemaVersion:  protocol.SchemaVersion,
		Tool:           "read",
		ToolUseEventID: "evt_noncanonical",
		WorkspaceRoot:  t.TempDir(),
		Roots:          []protocol.Root{{Path: t.TempDir(), Mode: protocol.RootModeReadWrite}},
		Input:          json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	if err := os.WriteFile(payloadPath, body, 0o600); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	_, toolErr := loadPayload(payloadPath, "read")
	if toolErr == nil || toolErr.Kind != "invalid_input" || !strings.Contains(toolErr.Message, "outside helper payload root") {
		t.Fatalf("loadPayload error = %+v; want payload root invalid_input", toolErr)
	}
	if _, err := os.Stat(payloadPath); err != nil {
		t.Fatalf("noncanonical payload was removed: %v", err)
	}
}

func TestLoadPayloadRejectsPayloadPathIdentityMismatch(t *testing.T) {
	payload := protocol.Payload{
		SchemaVersion:  protocol.SchemaVersion,
		Tool:           "read",
		ToolUseEventID: "evt_payload_body",
		WorkspaceRoot:  t.TempDir(),
		Roots:          []protocol.Root{{Path: t.TempDir(), Mode: protocol.RootModeReadWrite}},
		Input:          json.RawMessage(`{}`),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	payloadPath := cliPayloadPath(t, "evt_payload_path")
	if err := os.WriteFile(payloadPath, body, 0o600); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	_, toolErr := loadPayload(payloadPath, "read")
	if toolErr == nil || toolErr.Kind != "invalid_input" || !strings.Contains(toolErr.Message, "does not match") {
		t.Fatalf("loadPayload error = %+v; want identity mismatch invalid_input", toolErr)
	}
	if _, err := os.Stat(payloadPath); !os.IsNotExist(err) {
		t.Fatalf("payload stat err = %v; want parsed mismatched payload removed", err)
	}
}

func TestOpenProtectedPayloadRejectsWritableRoot(t *testing.T) {
	payloadPath := cliPayloadPath(t, "evt_writable_root")
	if err := os.WriteFile(payloadPath, []byte(`{}`), 0o600); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	if err := os.Chmod(payloadRoot, 0o770); err != nil {
		t.Fatalf("make payload root group-writable: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(payloadRoot, 0o700) })

	if _, _, _, err := openProtectedPayload(payloadPath, "evt_writable_root"); err == nil || !strings.Contains(err.Error(), "payload root is not protected") {
		t.Fatalf("openProtectedPayload error = %v; want protected-root rejection", err)
	}
}

func TestOpenProtectedPayloadRejectsSymlink(t *testing.T) {
	payloadPath := cliPayloadPath(t, "evt_symlink_payload")
	target := filepath.Join(t.TempDir(), "replacement.json")
	if err := os.WriteFile(target, []byte(`{}`), 0o600); err != nil {
		t.Fatalf("write symlink target: %v", err)
	}
	if err := os.Symlink(target, payloadPath); err != nil {
		t.Fatalf("create payload symlink: %v", err)
	}

	if _, _, _, err := openProtectedPayload(payloadPath, "evt_symlink_payload"); err == nil {
		t.Fatal("openProtectedPayload accepted a symlink payload")
	}
}

func TestOpenProtectedPayloadRejectsNonRegularFile(t *testing.T) {
	payloadPath := cliPayloadPath(t, "evt_directory_payload")
	if err := os.Mkdir(payloadPath, 0o700); err != nil {
		t.Fatalf("create directory payload: %v", err)
	}

	if _, _, _, err := openProtectedPayload(payloadPath, "evt_directory_payload"); err == nil {
		t.Fatal("openProtectedPayload accepted a directory payload")
	}
}

func TestOpenProtectedPayloadBindsReadToValidatedFileDescriptor(t *testing.T) {
	payloadPath := cliPayloadPath(t, "evt_bound_payload")
	if err := os.WriteFile(payloadPath, []byte("original"), 0o600); err != nil {
		t.Fatalf("write original payload: %v", err)
	}
	file, rootDir, _, err := openProtectedPayload(payloadPath, "evt_bound_payload")
	if err != nil {
		t.Fatalf("open protected payload: %v", err)
	}
	defer func() { _ = file.Close() }()
	defer func() { _ = rootDir.Close() }()

	oldPath := payloadPath + ".old"
	if err := os.Rename(payloadPath, oldPath); err != nil {
		t.Fatalf("rename validated payload: %v", err)
	}
	if err := os.WriteFile(payloadPath, []byte("replacement"), 0o600); err != nil {
		t.Fatalf("write replacement payload: %v", err)
	}
	body, err := io.ReadAll(file)
	if err != nil {
		t.Fatalf("read bound payload: %v", err)
	}
	if string(body) != "original" {
		t.Fatalf("bound payload body = %q; want original validated object", body)
	}
}

func TestRunReadStaysUnderContractEnvelopeCap(t *testing.T) {
	workspace := t.TempDir()
	body := strings.Repeat(strings.Repeat(`"`, 120)+"\n", 3000)
	if err := os.WriteFile(filepath.Join(workspace, "large.txt"), []byte(body), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	payloadPath := writeCLIPayload(t, protocol.Payload{
		SchemaVersion:  protocol.SchemaVersion,
		Tool:           "read",
		ToolUseEventID: "evt_read_large",
		WorkspaceRoot:  workspace,
		Roots:          []protocol.Root{{Path: workspace, Mode: protocol.RootModeReadWrite}},
		Limits:         protocol.Limits{VisibleBytes: 256 * 1024, VisibleLines: 2000},
		Input:          json.RawMessage(`{"path":"large.txt"}`),
	})
	var stdout bytes.Buffer
	if code := runRead(&stdout, payloadPath); code != 0 {
		t.Fatalf("runRead exit = %d; want 0", code)
	}
	if stdout.Len() > 256*1024 {
		t.Fatalf("envelope bytes = %d; want <= 256 KiB", stdout.Len())
	}
	var envelope protocol.Envelope
	if err := json.Unmarshal(stdout.Bytes(), &envelope); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if envelope.Truncated == nil || !*envelope.Truncated {
		t.Fatalf("envelope truncated = %v; want true", envelope.Truncated)
	}
	var result map[string]any
	if err := json.Unmarshal(envelope.Result, &result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if returned := int(result["returned_lines"].(float64)); returned <= 0 || returned >= 2000 {
		t.Fatalf("returned_lines = %d; want fitting to keep a non-empty prefix below the original window", returned)
	}
}

func TestRunReadAllowsOversizedMediaEnvelopeAcrossCap(t *testing.T) {
	workspace := t.TempDir()
	body := append([]byte{0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a}, bytes.Repeat([]byte("x"), protocol.MaxEnvelopeBytes)...)
	if err := os.WriteFile(filepath.Join(workspace, "report.png"), body, 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	payloadPath := writeCLIPayload(t, protocol.Payload{
		SchemaVersion:  protocol.SchemaVersion,
		Tool:           "read",
		ToolUseEventID: "evt_read_large_media",
		WorkspaceRoot:  workspace,
		Roots:          []protocol.Root{{Path: workspace, Mode: protocol.RootModeReadWrite}},
		Limits:         protocol.Limits{VisibleBytes: 256 * 1024, VisibleLines: 2000},
		Input:          json.RawMessage(`{"path":"report.png"}`),
	})
	var stdout bytes.Buffer
	if code := runRead(&stdout, payloadPath); code != 0 {
		t.Fatalf("runRead exit = %d; want 0", code)
	}
	if stdout.Len() <= protocol.MaxEnvelopeBytes {
		t.Fatalf("envelope bytes = %d; want > %d for media cap exemption", stdout.Len(), protocol.MaxEnvelopeBytes)
	}
	var envelope protocol.Envelope
	if err := json.Unmarshal(stdout.Bytes(), &envelope); err != nil {
		t.Fatalf("decode envelope: %v\n%s", err, stdout.String())
	}
	if envelope.SchemaVersion != protocol.SchemaVersion || envelope.Tool != "read" || envelope.Status != protocol.ToolStatusSuccess {
		t.Fatalf("envelope = %+v; want read success contract envelope", envelope)
	}
	if envelope.Truncated == nil || *envelope.Truncated {
		t.Fatalf("truncated = %v; want explicit false for media result", envelope.Truncated)
	}
	var result map[string]any
	if err := json.Unmarshal(envelope.Result, &result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if _, ok := result["content"]; ok {
		t.Fatalf("result = %+v; want media-only result without text content", result)
	}
	if result["mime"] != "image/png" || result["size_bytes"] != float64(len(body)) || result["data_base64"] != base64.StdEncoding.EncodeToString(body) {
		t.Fatalf("result = %+v; want exact image media payload", result)
	}
}

func TestLoadPayloadRejectsToolMismatchAfterParse(t *testing.T) {
	payloadPath := writeCLIPayload(t, protocol.Payload{
		SchemaVersion:  protocol.SchemaVersion,
		Tool:           "write",
		ToolUseEventID: "evt_read_1",
		WorkspaceRoot:  t.TempDir(),
		Input:          json.RawMessage(`{}`),
	})
	_, toolErr := loadPayload(payloadPath, "read")
	if toolErr == nil || toolErr.Kind != "invalid_input" {
		t.Fatalf("loadPayload error = %+v; want invalid_input", toolErr)
	}
	if _, err := os.Stat(payloadPath); !os.IsNotExist(err) {
		t.Fatalf("payload stat err = %v; want parsed payload removed", err)
	}
}

func TestLoadPayloadRejectsMissingOrNullInput(t *testing.T) {
	workspace := t.TempDir()
	for name, inputFragment := range map[string]string{
		"missing": "",
		"null":    `,"input":null`,
	} {
		t.Run(name, func(t *testing.T) {
			eventID := "evt_" + name + "_input"
			payloadPath := cliPayloadPath(t, eventID)
			body := `{"schema_version":1,"tool":"read","tool_use_event_id":` + strconv.Quote(eventID) + `,"workspace_root":` + strconv.Quote(workspace) + `,"roots":[{"path":` + strconv.Quote(workspace) + `,"mode":"read_write"}]` + inputFragment + `}`
			if err := os.WriteFile(payloadPath, []byte(body), 0o600); err != nil {
				t.Fatalf("write payload: %v", err)
			}
			_, toolErr := loadPayload(payloadPath, "read")
			if toolErr == nil || toolErr.Kind != "invalid_input" || !strings.Contains(toolErr.Message, "input is required") {
				t.Fatalf("loadPayload error = %+v; want input required invalid_input", toolErr)
			}
			if _, err := os.Stat(payloadPath); !os.IsNotExist(err) {
				t.Fatalf("payload stat err = %v; want parsed invalid payload removed", err)
			}
		})
	}
}

func TestLoadPayloadRejectsMissingRootsForCommandFollowup(t *testing.T) {
	workspace := t.TempDir()
	payloadPath := writeCLIPayload(t, protocol.Payload{
		SchemaVersion:  protocol.SchemaVersion,
		Tool:           "poll",
		ToolUseEventID: "evt_poll_missing_roots",
		WorkspaceRoot:  workspace,
		Input:          json.RawMessage(`{"task_id":"task_1"}`),
	})
	_, toolErr := loadPayload(payloadPath, "poll")
	if toolErr == nil || toolErr.Kind != "invalid_input" || !strings.Contains(toolErr.Message, "workspace_root must appear") {
		t.Fatalf("loadPayload error = %+v; want common roots invalid_input", toolErr)
	}
	if _, err := os.Stat(payloadPath); !os.IsNotExist(err) {
		t.Fatalf("payload stat err = %v; want parsed payload removed", err)
	}
}

func writeCLIPayload(t *testing.T, payload protocol.Payload) string {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	payloadPath := cliPayloadPath(t, payload.ToolUseEventID)
	if err := os.WriteFile(payloadPath, body, 0o600); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	return payloadPath
}

func buildSandboxHelper(t *testing.T) string {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "tetral-cli-helper-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	if err := os.Chmod(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(directory, "sandbox")
	build := exec.Command("go", "build", "-o", bin, "../../cmd/sandbox")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build helper: %v\n%s", err, string(output))
	}
	return bin
}

func cliPayloadPath(t *testing.T, toolUseEventID string) string {
	t.Helper()
	if !helperIDPattern.MatchString(toolUseEventID) {
		t.Fatalf("test payload id %q does not match helper id shape", toolUseEventID)
	}
	dir := filepath.Join(payloadRoot, toolUseEventID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("create payload dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "payload.json")
}

func decodeSingleEnvelope(t *testing.T, output []byte) protocol.Envelope {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(output))
	var envelope protocol.Envelope
	if err := decoder.Decode(&envelope); err != nil {
		t.Fatalf("decode envelope: %v\n%s", err, string(output))
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		t.Fatalf("stdout = %q; want exactly one JSON document", string(output))
	}
	if envelope.SchemaVersion != protocol.SchemaVersion {
		t.Fatalf("envelope schema_version = %d; want %d", envelope.SchemaVersion, protocol.SchemaVersion)
	}
	return envelope
}

func assertEnvelopeStatus(t *testing.T, envelope protocol.Envelope, status string) {
	t.Helper()
	if envelope.Status != status {
		t.Fatalf("envelope = %+v; want status %s", envelope, status)
	}
}

func assertEnvelopeErrorKind(t *testing.T, envelope protocol.Envelope, kind protocol.ErrorKind) {
	t.Helper()
	assertEnvelopeStatus(t, envelope, protocol.ToolStatusError)
	if envelope.Error == nil || envelope.Error.Kind != kind {
		t.Fatalf("envelope error = %+v; want kind %s", envelope.Error, kind)
	}
}

func requireRG(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("rg"); err != nil {
		t.Skip("rg is required for search helper tests")
	}
}

func assertNoForbiddenHelperEnvelopeKeys(t *testing.T, body []byte) {
	t.Helper()
	for _, key := range []string{`"error_kind"`, `"result_json"`, `"terminal_status"`, `"background_task"`} {
		if bytes.Contains(body, []byte(key)) {
			t.Fatalf("helper envelope contains forbidden key %s: %s", key, string(body))
		}
	}
}
