package task

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/sandbox/helper/internal/bound"
	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
)

const pipeDrainGuardFillBytes = 64 * 1024

func TestForegroundExecGuardDrainsFinalPipeBytesBeforeWait(t *testing.T) {
	fs := newExecFixture(t)
	startPipeDrainCPUSpinners(t)
	fillPath := writePipeDrainGuardFill(t)

	for i := 0; i < 50; i++ {
		sentinel := fmt.Sprintf("foreground-final-sentinel-%02d", i)
		response := RunExec(fs.payload(map[string]any{
			"cmd":     pipeDrainGuardCommand(fillPath, sentinel),
			"wait_ms": 5000,
		}))
		if response.Status != protocol.ToolStatusSuccess || response.Result.ExitCode == nil || *response.Result.ExitCode != 0 {
			t.Fatalf("iteration %d response = %+v; want terminal success", i, response)
		}
		if !strings.HasSuffix(response.Result.Stdout.Text, sentinel+"\n") {
			t.Fatalf("iteration %d stdout suffix = %q; want final sentinel %q", i, tailForFailure(response.Result.Stdout.Text), sentinel)
		}
	}
}

func TestRunExecForegroundReturnsPromptlyWithBackgroundPipeHolder(t *testing.T) {
	fs := newExecFixture(t)
	startedAt := time.Now()
	response := RunExec(fs.payload(map[string]any{
		"cmd":     "printf 'foreground-sentinel\\n'; sleep 5 & printf '%s' \"$!\" > sleeper.pid",
		"wait_ms": 5000,
	}))
	elapsed := time.Since(startedAt)
	sleeperPID := waitForChildPIDFile(t, filepath.Join(fs.workspace, "sleeper.pid"))
	t.Cleanup(func() { _ = syscall.Kill(sleeperPID, syscall.SIGKILL) })

	if response.Status != protocol.ToolStatusSuccess || response.Result.ExitCode == nil || *response.Result.ExitCode != 0 || response.Result.TimedOut {
		t.Fatalf("response = %+v; want prompt terminal success", response)
	}
	if !strings.Contains(response.Result.Stdout.Text, "foreground-sentinel") {
		t.Fatalf("stdout = %q; want pre-exit sentinel", response.Result.Stdout.Text)
	}
	if elapsed >= 4*time.Second {
		t.Fatalf("foreground return took %s; want bounded drain before the 5s background holder exits", elapsed)
	}
}

func TestRunExecForegroundCapturesSeparatedStreamsAndEnv(t *testing.T) {
	fs := newExecFixture(t)
	fs.mkdir("work")
	waitMS := 5000
	payload := fs.payload(map[string]any{
		"cmd":     "printf \"$FOO:$TERM:$NO_COLOR:$PAGER:$GIT_PAGER:$(pwd)\"; printf err >&2",
		"cwd":     "work",
		"env":     map[string]any{"FOO": "bar", "PAGER": "less"},
		"wait_ms": waitMS,
	})
	response := RunExec(payload)
	if response.Status != protocol.ToolStatusSuccess || response.Error != nil {
		t.Fatalf("RunExec response = %+v; want success", response)
	}
	if response.Result.ExitCode == nil || *response.Result.ExitCode != 0 || response.Result.Signal != nil || response.Result.TimedOut {
		t.Fatalf("result = %+v; want exit 0", response.Result)
	}
	if !strings.Contains(response.Result.Stdout.Text, "bar:dumb:1:cat:cat:"+filepath.Join(fs.workspace, "work")) {
		t.Fatalf("stdout = %q; want env hygiene and cwd", response.Result.Stdout.Text)
	}
	if response.Result.Stderr.Text != "err" {
		t.Fatalf("stderr = %q; want separated stderr", response.Result.Stderr.Text)
	}
}

func TestForegroundShellWrapperUsesTetralRuntimeRoot(t *testing.T) {
	if strings.Contains(foregroundShellWrapper, "TMPDIR") || strings.Contains(foregroundShellWrapper, "/tmp/tetral-helper-foreground") {
		t.Fatalf("foreground shell wrapper uses host/tmp-derived state path:\n%s", foregroundShellWrapper)
	}
	if !strings.Contains(foregroundShellWrapper, `tetral_runtime_root="/tmp/tetral-runtime/foreground"`) {
		t.Fatalf("foreground shell wrapper does not pin state under /tmp/tetral-runtime:\n%s", foregroundShellWrapper)
	}
}

func TestStartSupervisorProcessFailsClosedWhenDevNullUnavailable(t *testing.T) {
	original := openSupervisorDevNull
	openSupervisorDevNull = func() (*os.File, error) { return nil, os.ErrPermission }
	t.Cleanup(func() { openSupervisorDevNull = original })

	err := startSupervisorProcess("task_no_devnull", "true", t.TempDir(), nil, 1000)
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("startSupervisorProcess err = %v; want devnull open failure", err)
	}
}

func TestInitialStreamAccumulatorAdvancesTailAfterTruncatedPollSnapshot(t *testing.T) {
	accumulator := newInitialStreamAccumulator()
	first := accumulator.update(bound.StreamSnapshot{
		Text:          "ab\n[... 2 bytes truncated ...]\ncd",
		TotalBytes:    6,
		ReturnedBytes: 4,
		Truncated:     true,
	}, 4, 2000)
	second := accumulator.update(bound.StreamSnapshot{
		Text:          "ef",
		TotalBytes:    8,
		ReturnedBytes: 2,
		Truncated:     false,
	}, 4, 2000)

	if first.Text != "ab\n[... 2 bytes truncated ...]\ncd" || !first.Truncated {
		t.Fatalf("first snapshot = %+v; want original truncated poll snapshot", first)
	}
	if second.Text != "ab\n[... 4 bytes truncated ...]\nef" || second.TotalBytes != 8 || second.ReturnedBytes != 4 || !second.Truncated {
		t.Fatalf("second snapshot = %+v; want first head and latest tail with cumulative totals", second)
	}
}

func TestInitialStreamAccumulatorKeepsAdvancingBoundedTailAcrossPollSnapshots(t *testing.T) {
	accumulator := newInitialStreamAccumulator()
	snapshots := []bound.StreamSnapshot{
		{Text: "ab\n[... 6 bytes truncated ...]\nyz", TotalBytes: 10, ReturnedBytes: 4, Truncated: true},
		{Text: "12", TotalBytes: 12, ReturnedBytes: 2},
		{Text: "34", TotalBytes: 14, ReturnedBytes: 2},
	}

	var got bound.StreamSnapshot
	for _, snapshot := range snapshots {
		got = accumulator.update(snapshot, 4, 2000)
	}

	if got.Text != "ab\n[... 10 bytes truncated ...]\n34" || got.TotalBytes != 14 || got.ReturnedBytes != 4 || !got.Truncated {
		t.Fatalf("final snapshot = %+v; want bounded first head and newest tail", got)
	}
	if strings.Count(got.Text, "bytes truncated") != 1 {
		t.Fatalf("final text = %q; want exactly one synthetic truncation marker", got.Text)
	}
}

func TestInitialStreamAccumulatorPreservesEarlierHeadWhenLaterPollTruncates(t *testing.T) {
	accumulator := newInitialStreamAccumulator()
	accumulator.update(bound.StreamSnapshot{
		Text:          "01",
		TotalBytes:    2,
		ReturnedBytes: 2,
	}, 8, 2000)
	got := accumulator.update(bound.StreamSnapshot{
		Text:          "ab\n[... 6 bytes truncated ...]\nyz",
		TotalBytes:    12,
		ReturnedBytes: 4,
		Truncated:     true,
	}, 8, 2000)

	if got.Text != "01ab\n[... 6 bytes truncated ...]\nyz" || got.TotalBytes != 12 || got.ReturnedBytes != 6 || !got.Truncated {
		t.Fatalf("snapshot = %+v; want pre-truncation head retained before seam", got)
	}
}

func TestRunExecForegroundTimeoutKillsProcessGroup(t *testing.T) {
	fs := newExecFixture(t)
	waitMS := 250
	response := RunExec(fs.payload(map[string]any{
		"cmd":              "sleep 5",
		"wait_ms":          waitMS,
		"on_wait_expiry":   "kill",
		"task_lifetime_ms": 1000,
	}))
	if response.Status != protocol.ToolStatusError || response.Error == nil || response.Error.Kind != "timeout" {
		t.Fatalf("RunExec timeout response = %+v; want timeout error", response)
	}
	if !response.Result.TimedOut {
		t.Fatalf("timed_out = false; want true")
	}
}

func TestRunExecForegroundStdoutCloseKillsProcessGroup(t *testing.T) {
	fs := newExecFixture(t)
	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	t.Cleanup(func() {
		_ = readEnd.Close()
		_ = writeEnd.Close()
	})
	done := make(chan Response, 1)
	go func() {
		done <- runExec(fs.payload(map[string]any{
			"cmd":     "trap 'touch transport-gone; exit 143' TERM; while :; do sleep 1; done",
			"wait_ms": 50000,
		}), func() uintptr { return writeEnd.Fd() })
	}()
	time.Sleep(150 * time.Millisecond)
	if err := readEnd.Close(); err != nil && !errors.Is(err, os.ErrClosed) && !errors.Is(err, syscall.EBADF) {
		t.Fatalf("close read end: %v", err)
	}
	select {
	case response := <-done:
		if !response.HelperFailure {
			t.Fatalf("RunExec response = %+v; want helper failure when stdout transport closes", response)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunExec did not stop after stdout transport closed")
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(fs.workspace, "transport-gone")); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("child did not observe SIGTERM after stdout transport close")
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func TestRunExecForegroundJoinsStdoutWatcherBeforeReturn(t *testing.T) {
	fs := newExecFixture(t)
	var hookCalls atomic.Int64
	response := runExec(fs.payload(map[string]any{
		"cmd":     "true",
		"wait_ms": 5000,
	}), func() uintptr {
		hookCalls.Add(1)
		return os.Stdout.Fd()
	})
	if response.Status != protocol.ToolStatusSuccess {
		t.Fatalf("runExec response = %+v; want success", response)
	}
	returnedCalls := hookCalls.Load()
	time.Sleep(2 * stdoutWatchPoll)
	if got := hookCalls.Load(); got != returnedCalls {
		t.Fatalf("stdout watcher hook calls after return = %d; want joined at %d", got, returnedCalls)
	}
}

func TestRunExecPdeathsigKillsChild(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("PDEATHSIG is Linux-specific")
	}
	if os.Getenv("TETRAL_PDEATHSIG_HELPER") == "1" {
		runPdeathsigHelper(t)
		return
	}

	workspace := t.TempDir()
	helper := exec.Command(os.Args[0], "-test.run=TestRunExecPdeathsigKillsChild")
	helper.Env = append(os.Environ(),
		"TETRAL_PDEATHSIG_HELPER=1",
		"TETRAL_PDEATHSIG_WORKSPACE="+workspace,
	)
	if err := helper.Start(); err != nil {
		t.Fatalf("start pdeathsig helper: %v", err)
	}

	childPID := waitForChildPIDFile(t, filepath.Join(workspace, "child.pid"))
	if err := helper.Process.Kill(); err != nil {
		t.Fatalf("kill pdeathsig helper: %v", err)
	}
	_ = helper.Wait()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !linuxProcessRunning(childPID) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	_ = syscall.Kill(childPID, syscall.SIGKILL)
	t.Fatalf("child pid %d still running after helper death; want PDEATHSIG cleanup", childPID)
}

func TestRunExecPdeathsigKillsForegroundDescendant(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("foreground descendant cleanup is Linux-specific")
	}
	if os.Getenv("TETRAL_PDEATHSIG_DESCENDANT_HELPER") == "1" {
		runPdeathsigDescendantHelper(t)
		return
	}

	workspace := t.TempDir()
	helper := exec.Command(os.Args[0], "-test.run=TestRunExecPdeathsigKillsForegroundDescendant")
	helper.Env = append(os.Environ(),
		"TETRAL_PDEATHSIG_DESCENDANT_HELPER=1",
		"TETRAL_PDEATHSIG_WORKSPACE="+workspace,
	)
	if err := helper.Start(); err != nil {
		t.Fatalf("start descendant pdeathsig helper: %v", err)
	}

	descendantPID := waitForChildPIDFile(t, filepath.Join(workspace, "descendant.pid"))
	if err := helper.Process.Kill(); err != nil {
		t.Fatalf("kill descendant pdeathsig helper: %v", err)
	}
	_ = helper.Wait()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if !linuxProcessRunning(descendantPID) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	_ = syscall.Kill(descendantPID, syscall.SIGKILL)
	t.Fatalf("foreground descendant pid %d still running after helper death; want process-group cleanup", descendantPID)
}

func runPdeathsigHelper(t *testing.T) {
	t.Helper()
	workspace := os.Getenv("TETRAL_PDEATHSIG_WORKSPACE")
	if workspace == "" {
		t.Fatal("TETRAL_PDEATHSIG_WORKSPACE is required")
	}
	input, err := json.Marshal(map[string]any{
		"cmd":     `printf "$$" > child.pid; exec sleep 30`,
		"cwd":     workspace,
		"wait_ms": 50000,
	})
	if err != nil {
		t.Fatalf("marshal pdeathsig input: %v", err)
	}
	response := RunExec(protocol.Payload{
		SchemaVersion:  protocol.SchemaVersion,
		Tool:           "exec",
		ToolUseEventID: "evt_pdeathsig",
		WorkspaceRoot:  workspace,
		Roots:          []protocol.Root{{Path: workspace, Mode: protocol.RootModeReadWrite}},
		Input:          input,
	})
	t.Fatalf("RunExec returned before parent test killed helper: %+v", response)
}

func runPdeathsigDescendantHelper(t *testing.T) {
	t.Helper()
	workspace := os.Getenv("TETRAL_PDEATHSIG_WORKSPACE")
	if workspace == "" {
		t.Fatal("TETRAL_PDEATHSIG_WORKSPACE is required")
	}
	input, err := json.Marshal(map[string]any{
		"cmd":     `sleep 30 & printf "$!" > descendant.pid; wait`,
		"cwd":     workspace,
		"wait_ms": 50000,
	})
	if err != nil {
		t.Fatalf("marshal descendant pdeathsig input: %v", err)
	}
	response := RunExec(protocol.Payload{
		SchemaVersion:  protocol.SchemaVersion,
		Tool:           "exec",
		ToolUseEventID: "evt_pdeathsig_descendant",
		WorkspaceRoot:  workspace,
		Roots:          []protocol.Root{{Path: workspace, Mode: protocol.RootModeReadWrite}},
		Input:          input,
	})
	t.Fatalf("RunExec returned before parent test killed helper: %+v", response)
}

func waitForChildPIDFile(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		body, err := os.ReadFile(path)
		if err == nil {
			pid, parseErr := strconv.Atoi(strings.TrimSpace(string(body)))
			if parseErr != nil {
				t.Fatalf("parse child pid %q: %v", string(body), parseErr)
			}
			return pid
		}
		lastErr = err
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("child pid file %s did not appear: %v", path, lastErr)
	return 0
}

func linuxProcessRunning(pid int) bool {
	if err := syscall.Kill(pid, 0); err != nil {
		return false
	}
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err == nil {
		fields := strings.Fields(string(stat))
		if len(fields) > 2 && fields[2] == "Z" {
			return false
		}
	}
	return true
}

func startPipeDrainCPUSpinners(t *testing.T) {
	t.Helper()
	var stop atomic.Bool
	var wait sync.WaitGroup
	wait.Add(2 * runtime.NumCPU())
	for i := 0; i < 2*runtime.NumCPU(); i++ {
		go func() {
			defer wait.Done()
			for spins := 0; !stop.Load(); spins++ {
				if spins%1024 == 0 {
					runtime.Gosched()
				}
			}
		}()
	}
	t.Cleanup(func() {
		stop.Store(true)
		wait.Wait()
	})
}

func writePipeDrainGuardFill(t *testing.T) string {
	t.Helper()
	body := bytes.Repeat([]byte{'x'}, pipeDrainGuardFillBytes)
	body[len(body)-1] = '\n'
	path := filepath.Join(t.TempDir(), "pipe-fill")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("write pipe fill: %v", err)
	}
	return path
}

func pipeDrainGuardCommand(fillPath string, sentinel string) string {
	return fmt.Sprintf(
		"dd if=%s bs=%d count=1 2>/dev/null; printf %%s %s",
		shellQuoteForTaskTest(fillPath),
		pipeDrainGuardFillBytes,
		shellQuoteForTaskTest(sentinel+"\n"),
	)
}

func shellQuoteForTaskTest(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

func tailForFailure(value string) string {
	const maxBytes = 256
	if len(value) <= maxBytes {
		return value
	}
	return value[len(value)-maxBytes:]
}

func TestRunExecRejectsInvalidCWD(t *testing.T) {
	fs := newExecFixture(t)
	fs.write("file.txt", "x")
	response := RunExec(fs.payload(map[string]any{"cmd": "pwd", "cwd": "file.txt"}))
	if response.Status != protocol.ToolStatusError || response.Error == nil || response.Error.Kind != "not_a_directory" {
		t.Fatalf("file cwd response = %+v; want not_a_directory", response)
	}
	invalidTask := RunExec(fs.payload(map[string]any{
		"cmd":            "pwd",
		"cwd":            "missing",
		"on_wait_expiry": "detach",
		"task_id":        "../bad",
	}))
	if invalidTask.Status != protocol.ToolStatusError || invalidTask.Error == nil || invalidTask.Error.Kind != "invalid_input" {
		t.Fatalf("invalid detach task response = %+v; want task_id invalid_input before cwd validation", invalidTask)
	}
}

type execFixture struct {
	t         *testing.T
	workspace string
}

func newExecFixture(t *testing.T) execFixture {
	t.Helper()
	return execFixture{t: t, workspace: t.TempDir()}
}

func (f execFixture) payload(input map[string]any) protocol.Payload {
	f.t.Helper()
	return f.payloadWithLimits(input, protocol.Limits{VisibleBytes: 50 * 1024, VisibleLines: 2000})
}

func (f execFixture) payloadWithLimits(input map[string]any, limits protocol.Limits) protocol.Payload {
	f.t.Helper()
	body, err := json.Marshal(input)
	if err != nil {
		f.t.Fatalf("marshal input: %v", err)
	}
	return protocol.Payload{
		SchemaVersion:  protocol.SchemaVersion,
		Tool:           "exec",
		ToolUseEventID: "evt_exec",
		WorkspaceRoot:  f.workspace,
		Roots:          []protocol.Root{{Path: f.workspace, Mode: protocol.RootModeReadWrite}},
		Limits:         limits,
		Input:          body,
	}
}

func (f execFixture) mkdir(name string) {
	f.t.Helper()
	if err := os.MkdirAll(filepath.Join(f.workspace, filepath.FromSlash(name)), 0o755); err != nil {
		f.t.Fatalf("mkdir fixture: %v", err)
	}
}

func (f execFixture) write(name string, content string) {
	f.t.Helper()
	path := filepath.Join(f.workspace, filepath.FromSlash(name))
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		f.t.Fatalf("write fixture: %v", err)
	}
}
