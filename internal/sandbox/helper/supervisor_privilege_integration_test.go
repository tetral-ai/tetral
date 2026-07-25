package helper_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

var privilegeTestRuntimeRoot = fmt.Sprintf("/tmp/tetral-runtime-privilege-%d", os.Getpid())

type privilegeTestEnvelope struct {
	Status string `json:"status"`
	Error  *struct {
		Kind    string `json:"kind"`
		Message string `json:"message"`
	} `json:"error"`
	Result struct {
		TaskID string `json:"task_id"`
		Stdout struct {
			Text string `json:"text"`
		} `json:"stdout"`
	} `json:"result"`
}

// TestSupervisorKeepsDetachedTaskAuthorizationAfterPrivilegeDrop is the real
// Linux/root lane proof for the root launcher -> dropped bootstrap -> dropped
// final-supervisor chain. It deliberately uses fresh helper processes for all
// task control operations; the bootstrap capability is never involved in
// stdin, poll, or cancel authorization.
func TestSupervisorKeepsDetachedTaskAuthorizationAfterPrivilegeDrop(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("the required supervisor privilege proof runs on the Linux/root CI lane")
	}
	if os.Geteuid() != 0 {
		t.Skip("the required supervisor privilege proof runs on the Linux/root CI lane")
	}

	helperDir, err := os.MkdirTemp("/tmp", "tetral-helper-bin-*")
	if err != nil {
		t.Fatalf("mkdir helper binary dir: %v", err)
	}
	if err := os.Chmod(helperDir, 0o755); err != nil {
		t.Fatalf("chmod helper binary dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(helperDir) })
	helper := filepath.Join(helperDir, "sandbox")
	linkerValues := fmt.Sprintf(
		"-X github.com/tetral-ai/tetral/internal/sandbox/helper/internal/task.runtimeRoot=%s -X github.com/tetral-ai/tetral/internal/sandbox/helper/internal/cli.payloadRoot=%s",
		privilegeTestRuntimeRoot,
		filepath.Join(privilegeTestRuntimeRoot, "tool-payloads"),
	)
	build := exec.Command("go", "build", "-ldflags", linkerValues, "-o", helper, "./cmd/sandbox")
	build.Dir = helperRoot(t)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build real helper: %v\n%s", err, output)
	}

	runtimeUID, runtimeGID := 65534, 65534
	wrongUID, wrongGID := 65533, 65533
	_, runtimeRootErr := os.Lstat(privilegeTestRuntimeRoot)
	runtimeRootCreated := os.IsNotExist(runtimeRootErr)
	workspace, err := os.MkdirTemp("/tmp", "tetral-runtime-workspace-*")
	if err != nil {
		t.Fatalf("mkdir runtime workspace: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(workspace) })
	if err := os.Chown(workspace, runtimeUID, runtimeGID); err != nil {
		t.Fatalf("chown runtime workspace: %v", err)
	}
	wrongWorkspace, err := os.MkdirTemp("/tmp", "tetral-wrong-workspace-*")
	if err != nil {
		t.Fatalf("mkdir wrong workspace: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(wrongWorkspace) })
	if err := os.Chown(wrongWorkspace, wrongUID, wrongGID); err != nil {
		t.Fatalf("chown wrong workspace: %v", err)
	}

	tasksRoot := filepath.Join(privilegeTestRuntimeRoot, "tasks")
	if _, err := os.Lstat(tasksRoot); err == nil {
		t.Fatalf("required root proof needs an unused %s", tasksRoot)
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat tasks root: %v", err)
	}
	if err := os.MkdirAll(tasksRoot, 0o700); err != nil {
		t.Fatalf("mkdir tasks root: %v", err)
	}
	if err := os.Chown(tasksRoot, runtimeUID, runtimeGID); err != nil {
		t.Fatalf("chown tasks root: %v", err)
	}
	if err := os.Chown(privilegeTestRuntimeRoot, runtimeUID, runtimeGID); err != nil {
		t.Fatalf("chown runtime root: %v", err)
	}
	if err := os.Chmod(privilegeTestRuntimeRoot, 0o700); err != nil {
		t.Fatalf("chmod runtime root: %v", err)
	}
	t.Cleanup(func() {
		_ = os.RemoveAll(tasksRoot)
		if runtimeRootCreated {
			_ = os.RemoveAll(privilegeTestRuntimeRoot)
		}
	})

	payloadRoot := filepath.Join(privilegeTestRuntimeRoot, "tool-payloads")
	_, payloadRootErr := os.Lstat(payloadRoot)
	payloadRootCreated := os.IsNotExist(payloadRootErr)
	if err := os.MkdirAll(payloadRoot, 0o700); err != nil {
		t.Fatalf("mkdir payload root: %v", err)
	}
	if err := os.Chown(payloadRoot, 0, 0); err != nil {
		t.Fatalf("chown payload root: %v", err)
	}
	if err := os.Chmod(payloadRoot, 0o700); err != nil {
		t.Fatalf("chmod payload root: %v", err)
	}
	if payloadRootCreated && !runtimeRootCreated {
		t.Cleanup(func() { _ = os.RemoveAll(payloadRoot) })
	}

	taskID := fmt.Sprintf("task_privilege_%d", os.Getpid())
	command := "printf 'uid=%s gid=%s groups=%s\\n' \"$(id -u)\" \"$(id -g)\" \"$(id -G)\"; read line; printf 'stdin=%s\\n' \"$line\"; while :; do sleep 1; done"
	execEnvelope := runPrivilegeHelper(t, helper, payloadRoot, "exec", "evt_privilege_exec", workspace, map[string]any{
		"cmd": command, "wait_ms": 250, "on_wait_expiry": "detach", "task_id": taskID, "task_lifetime_ms": 10_000,
	})
	if execEnvelope.Status != "running" || execEnvelope.Result.TaskID != taskID {
		errorKind := ""
		if execEnvelope.Error != nil {
			errorKind = execEnvelope.Error.Kind + ": " + execEnvelope.Error.Message
		}
		t.Fatalf("detached exec status/error/task = %q/%q/%q; want running task %s", execEnvelope.Status, errorKind, execEnvelope.Result.TaskID, taskID)
	}

	taskDir := filepath.Join(tasksRoot, taskID)
	assertOwnedMode(t, taskDir, runtimeUID, runtimeGID, 0o700)
	assertOwnedMode(t, filepath.Join(taskDir, "control.sock"), runtimeUID, runtimeGID, 0o600)
	meta := readPrivilegeTaskMeta(t, filepath.Join(taskDir, "meta.json"))
	if meta.ChildPGID <= 0 {
		t.Fatalf("supervisor child pgid = %d; want final-supervisor startup", meta.ChildPGID)
	}

	stdinEnvelope := runPrivilegeHelper(t, helper, payloadRoot, "stdin", "evt_privilege_stdin", workspace, map[string]any{
		"task_id": taskID, "chars": "hello-from-fresh-helper\n", "wait_ms": 250, "write_seq": 1,
	})
	combinedOutput := execEnvelope.Result.Stdout.Text + stdinEnvelope.Result.Stdout.Text
	pollEnvelope := runPrivilegeHelper(t, helper, payloadRoot, "poll", "evt_privilege_poll", workspace, map[string]any{
		"task_id": taskID, "wait_ms": 250,
	})
	combinedOutput += pollEnvelope.Result.Stdout.Text
	wantIdentity := fmt.Sprintf("uid=%d gid=%d groups=%d", runtimeUID, runtimeGID, runtimeGID)
	if !strings.Contains(combinedOutput, wantIdentity) {
		t.Fatalf("detached output = %q; want actual cleared-groups identity %q", combinedOutput, wantIdentity)
	}
	if !strings.Contains(combinedOutput, "stdin=hello-from-fresh-helper") {
		t.Fatalf("detached output = %q; want stdin round trip", combinedOutput)
	}

	unauthorizedAccesses := 0
	if envelope, exitCode := runPrivilegeHelperResult(t, helper, payloadRoot, "poll", "evt_privilege_wrong_identity", wrongWorkspace, map[string]any{
		"task_id": taskID, "wait_ms": 0,
	}); exitCode == 0 && (envelope.Status == "running" || envelope.Status == "success") {
		unauthorizedAccesses++
	}
	if envelope, exitCode := runPrivilegeHelperResult(t, helper, payloadRoot, "poll", "evt_privilege_wrong_task", workspace, map[string]any{
		"task_id": taskID + "_wrong", "wait_ms": 0,
	}); exitCode == 0 && (envelope.Status == "running" || envelope.Status == "success") {
		unauthorizedAccesses++
	}
	if unauthorizedAccesses != 0 {
		t.Fatalf("unauthorized task access count = %d; want zero", unauthorizedAccesses)
	}

	cancelEnvelope := runPrivilegeHelper(t, helper, payloadRoot, "cancel", "evt_privilege_cancel", workspace, map[string]any{"task_id": taskID})
	if cancelEnvelope.Status != "success" {
		t.Fatalf("cancel = %+v; want terminal success", cancelEnvelope)
	}
	terminalEnvelope := runPrivilegeHelper(t, helper, payloadRoot, "poll", "evt_privilege_terminal", workspace, map[string]any{"task_id": taskID, "wait_ms": 0})
	if terminalEnvelope.Status != "success" {
		t.Fatalf("terminal observation = %+v; want success", terminalEnvelope)
	}
	if _, err := os.Stat(filepath.Join(taskDir, "reservation")); !os.IsNotExist(err) {
		t.Fatalf("live reservation stat error = %v; want released", err)
	}

	old := time.Now().Add(-25 * time.Hour)
	if err := os.Chtimes(filepath.Join(taskDir, "exit.json"), old, old); err != nil {
		t.Fatalf("age terminal record: %v", err)
	}
	_, _ = runPrivilegeHelperResult(t, helper, payloadRoot, "poll", "evt_privilege_sweep", workspace, map[string]any{"task_id": "task_missing_after_terminal", "wait_ms": 0})
	if _, err := os.Stat(taskDir); !os.IsNotExist(err) {
		t.Fatalf("terminal task directory stat error = %v; want sweepable", err)
	}
}

func runPrivilegeHelper(t *testing.T, helper string, payloadRoot string, tool string, eventID string, workspace string, input map[string]any) privilegeTestEnvelope {
	t.Helper()
	envelope, exitCode := runPrivilegeHelperResult(t, helper, payloadRoot, tool, eventID, workspace, input)
	if exitCode != 0 {
		entries, _ := os.ReadDir(filepath.Join(privilegeTestRuntimeRoot, "tasks"))
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Fatalf("helper %s exit = %d tasks=%v; want envelope success", tool, exitCode, names)
	}
	return envelope
}

func runPrivilegeHelperResult(t *testing.T, helper string, payloadRoot string, tool string, eventID string, workspace string, input map[string]any) (privilegeTestEnvelope, int) {
	t.Helper()
	payloadDir := filepath.Join(payloadRoot, eventID)
	if err := os.Mkdir(payloadDir, 0o700); err != nil {
		t.Fatalf("mkdir payload dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(payloadDir) }()
	payloadPath := filepath.Join(payloadDir, "payload.json")
	payload := map[string]any{
		"schema_version":    1,
		"tool":              tool,
		"tool_use_event_id": eventID,
		"workspace_root":    workspace,
		"roots":             []map[string]any{{"path": workspace, "mode": "read_write"}},
		"input":             input,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal helper payload: %v", err)
	}
	if err := os.WriteFile(payloadPath, body, 0o600); err != nil {
		t.Fatalf("write helper payload: %v", err)
	}
	cmd := exec.Command(helper, tool, "--payload", payloadPath)
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	err = cmd.Run()
	exitCode := 0
	if err != nil {
		var exitErr *exec.ExitError
		if !strings.Contains(err.Error(), "exit status") || !asExitError(err, &exitErr) {
			t.Fatalf("run helper %s: %v", tool, err)
		}
		exitCode = exitErr.ExitCode()
	}
	var envelope privilegeTestEnvelope
	if stdout.Len() > 0 {
		if decodeErr := json.Unmarshal(stdout.Bytes(), &envelope); decodeErr != nil {
			t.Fatalf("decode helper %s output %q: %v", tool, stdout.String(), decodeErr)
		}
	}
	return envelope, exitCode
}

func asExitError(err error, target **exec.ExitError) bool {
	value, ok := err.(*exec.ExitError)
	if ok {
		*target = value
	}
	return ok
}

func assertOwnedMode(t *testing.T, path string, uid int, gid int, mode os.FileMode) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("stat %s has no uid/gid", path)
	}
	if int(stat.Uid) != uid || int(stat.Gid) != gid || info.Mode().Perm() != mode {
		t.Fatalf("%s owner/mode = %d:%d %04o; want %d:%d %04o", path, stat.Uid, stat.Gid, info.Mode().Perm(), uid, gid, mode)
	}
}

func readPrivilegeTaskMeta(t *testing.T, path string) struct {
	ChildPGID int `json:"child_pgid"`
} {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read supervisor metadata: %v", err)
	}
	var meta struct {
		ChildPGID int `json:"child_pgid"`
	}
	if err := json.Unmarshal(body, &meta); err != nil {
		t.Fatalf("decode supervisor metadata: %v", err)
	}
	return meta
}

func TestSupervisorHiddenEntrypointsRejectMissingMalformedAndUserForgedCapabilities(t *testing.T) {
	if runtime.GOOS != "linux" {
		return
	}
	helper := filepath.Join(t.TempDir(), "sandbox")
	build := exec.Command("go", "build", "-o", helper, "./cmd/sandbox")
	build.Dir = helperRoot(t)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build real helper: %v\n%s", err, output)
	}
	args := []string{"__supervise", "--task-id", "task_forged_cap", "--cmd", "true", "--cwd", t.TempDir(), "--env-json", "[]", "--lifetime-ms", "1000"}
	for _, tc := range []struct {
		name string
		file func(*testing.T) *os.File
	}{
		{name: "missing"},
		{name: "regular_file", file: func(t *testing.T) *os.File {
			file, err := os.CreateTemp(t.TempDir(), "cap-*")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := file.WriteString("tetral-supervisor-reexec-v2\n"); err != nil {
				t.Fatal(err)
			}
			if _, err := file.Seek(0, 0); err != nil {
				t.Fatal(err)
			}
			return file
		}},
		{name: "pipe", file: func(t *testing.T) *os.File {
			readFile, writeFile, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := writeFile.WriteString("tetral-supervisor-reexec-v2\n"); err != nil {
				t.Fatal(err)
			}
			_ = writeFile.Close()
			return readFile
		}},
		{name: "socket", file: func(t *testing.T) *os.File {
			fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
			if err != nil {
				t.Fatal(err)
			}
			_ = unix.Close(fds[1])
			return os.NewFile(uintptr(fds[0]), "forged-supervisor-socket")
		}},
		{name: "sealed_malformed_marker", file: func(t *testing.T) *os.File {
			fd, err := unix.MemfdCreate("forged-supervisor-cap", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
			if err != nil {
				t.Fatal(err)
			}
			file := os.NewFile(uintptr(fd), "forged-supervisor-cap")
			if _, err := file.WriteString("not-the-authorized-marker\n"); err != nil {
				t.Fatal(err)
			}
			if err := unix.Fchmod(fd, 0o400); err != nil {
				t.Fatal(err)
			}
			seals := unix.F_SEAL_SEAL | unix.F_SEAL_SHRINK | unix.F_SEAL_GROW | unix.F_SEAL_WRITE
			if _, err := unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, seals); err != nil {
				t.Fatal(err)
			}
			if _, err := file.Seek(0, 0); err != nil {
				t.Fatal(err)
			}
			return file
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command(helper, args...)
			if tc.file != nil {
				file := tc.file(t)
				defer func() { _ = file.Close() }()
				cmd.ExtraFiles = []*os.File{file}
				cmd.Env = append(os.Environ(), "TETRAL_SUPERVISOR_AUTH_FD=3")
			}
			if err := cmd.Run(); err == nil {
				t.Fatal("forged supervisor capability started hidden entrypoint")
			}
		})
	}
	if err := exec.Command(helper, "reap").Run(); err == nil {
		t.Fatal("uncontracted reap subcommand was accepted")
	}
}

func TestPrivilegeProofIdentityConstantsAreNonRoot(t *testing.T) {
	for _, value := range []int{65534, 65533} {
		if value == 0 {
			t.Fatalf("identity %s unexpectedly root", strconv.Itoa(value))
		}
	}
}
