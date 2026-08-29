package task

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/tetral-ai/tetral/internal/sandbox/helper/internal/bound"
	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
)

func TestDetachedExecPollsTerminalIdempotently(t *testing.T) {
	fs := newExecFixture(t)
	withTestSupervisor(t)
	taskID := "task_detach_terminal"
	response := RunExec(fs.payload(map[string]any{
		"cmd":            "printf done",
		"on_wait_expiry": "detach",
		"task_id":        taskID,
		"wait_ms":        250,
	}))
	if response.Status != protocol.ToolStatusRunning && response.Status != protocol.ToolStatusSuccess {
		t.Fatalf("detach response = %+v; want running or fast terminal success", response)
	}
	terminal := waitForTerminalPoll(t, fs, taskID)
	again := RunPoll(fs.payloadWithTool("poll", map[string]any{"task_id": taskID, "wait_ms": 0}))
	if again.Status != protocol.ToolStatusSuccess ||
		again.Result.ExitCode == nil || terminal.Result.ExitCode == nil ||
		*again.Result.ExitCode != *terminal.Result.ExitCode ||
		again.Result.Stdout.Text != terminal.Result.Stdout.Text {
		t.Fatalf("terminal replay = %+v; want same terminal state as %+v", again, terminal)
	}
	if _, err := os.Stat(exitPath(taskID)); err != nil {
		t.Fatalf("exit.json missing: %v", err)
	}
	if _, err := os.Stat(stdoutLogPath(taskID)); err != nil {
		t.Fatalf("stdout.log missing: %v", err)
	}
}

func TestSupervisorGuardDrainsFinalPipeBytesBeforeWait(t *testing.T) {
	fs := newExecFixture(t)
	withTestSupervisor(t)
	fillPath := writePipeDrainGuardFill(t)

	for i := 0; i < 50; i++ {
		taskID := fmt.Sprintf("task_pipe_drain_guard_%02d", i)
		sentinel := fmt.Sprintf("supervisor-final-sentinel-%02d", i)
		releasePath := filepath.Join(t.TempDir(), "release")
		response := RunExec(fs.payload(map[string]any{
			"cmd":            pipeDrainGuardCommandAfterFile(releasePath, fillPath, sentinel),
			"on_wait_expiry": "detach",
			"task_id":        taskID,
			"wait_ms":        250,
		}))
		if response.Status != protocol.ToolStatusRunning {
			t.Fatalf("iteration %d response = %+v; want running before release", i, response)
		}
		stopPressure := startPipeDrainCPUSpinners(t)
		if err := os.WriteFile(releasePath, nil, 0o600); err != nil {
			t.Fatalf("iteration %d release supervisor command: %v", i, err)
		}
		waitForExitRecord(t, taskID)
		stopPressure()
		exitBody, err := os.ReadFile(exitPath(taskID))
		if err != nil {
			t.Fatalf("iteration %d read exit record: %v", i, err)
		}
		var record exitRecord
		if err := json.Unmarshal(exitBody, &record); err != nil {
			t.Fatalf("iteration %d decode exit record: %v", i, err)
		}
		if record.ExitCode == nil || *record.ExitCode != 0 {
			t.Fatalf("iteration %d exit record = %+v; want success", i, record)
		}
		logBody, err := os.ReadFile(stdoutLogPath(taskID))
		if err != nil {
			t.Fatalf("iteration %d read stdout log: %v", i, err)
		}
		if !strings.HasSuffix(string(logBody), sentinel+"\n") {
			t.Fatalf("iteration %d stdout suffix = %q; want final sentinel %q", i, tailForFailure(string(logBody)), sentinel)
		}
	}
}

func TestControlSocketUsesContractWireShape(t *testing.T) {
	fs := newExecFixture(t)
	withTestSupervisor(t)
	taskID := "task_socket_shape"
	response := RunExec(fs.payload(map[string]any{
		"cmd":            "sleep 10",
		"on_wait_expiry": "detach",
		"task_id":        taskID,
		"wait_ms":        250,
	}))
	if response.Status != protocol.ToolStatusRunning {
		t.Fatalf("detach response = %+v; want running", response)
	}
	conn, err := net.DialTimeout("unix", sockPath(taskID), time.Second)
	if err != nil {
		t.Fatalf("dial control socket: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if err := json.NewEncoder(conn).Encode(controlRequest{SchemaVersion: protocol.SchemaVersion, Op: "poll", WaitMS: 0}); err != nil {
		t.Fatalf("encode control request: %v", err)
	}
	var raw map[string]json.RawMessage
	if err := json.NewDecoder(conn).Decode(&raw); err != nil {
		t.Fatalf("decode control response: %v", err)
	}
	if _, ok := raw["result"]; ok {
		t.Fatalf("control response = %v; must not wrap facts under result", raw)
	}
	var state string
	if err := json.Unmarshal(raw["state"], &state); err != nil || state != "running" {
		t.Fatalf("state = %q err=%v; want running", state, err)
	}
	if _, ok := raw["stdout"]; !ok {
		t.Fatalf("control response = %v; want top-level stdout", raw)
	}
	cancel := RunCancel(fs.payloadWithTool("cancel", map[string]any{"task_id": taskID}))
	if cancel.Status != protocol.ToolStatusSuccess {
		t.Fatalf("cancel response = %+v; want success", cancel)
	}
}

func TestControlTaskRechecksTerminalRecordAfterRequestEncodeFailure(t *testing.T) {
	withTestRuntime(t)
	taskID := "task_encode_terminal_race"
	if err := os.MkdirAll(taskDir(taskID), 0o700); err != nil {
		t.Fatalf("mkdir task: %v", err)
	}
	listener, err := net.Listen("unix", sockPath(taskID))
	if err != nil {
		t.Fatalf("listen control socket: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	original := encodeControlRequest
	t.Cleanup(func() { encodeControlRequest = original })
	encodeCalled := false
	encodeControlRequest = func(net.Conn, controlRequest) error {
		encodeCalled = true
		exitCode := 0
		startedAt := time.Now().Add(-time.Second)
		endedAt := time.Now()
		record := exitRecord{
			ExitCode:   &exitCode,
			StartedAt:  startedAt.UTC().Format(time.RFC3339Nano),
			EndedAt:    endedAt.UTC().Format(time.RFC3339Nano),
			Stdout:     emptySnapshot(taskID),
			Stderr:     emptySnapshot(taskID),
			DurationMS: endedAt.Sub(startedAt).Milliseconds(),
		}
		if err := writeJSONAtomic(exitPath(taskID), record); err != nil {
			return err
		}
		return errors.New("injected request encode failure")
	}

	result, toolErr := controlTask(taskID, controlRequest{
		SchemaVersion: protocol.SchemaVersion,
		Op:            "poll",
	}, protocol.Limits{})
	if !encodeCalled {
		t.Fatal("request encoder was not reached")
	}
	if toolErr != nil || result.ExitCode == nil || *result.ExitCode != 0 {
		t.Fatalf("result = %+v error = %+v; want persisted terminal success", result, toolErr)
	}
}

func TestControlTaskReportsLostAfterRequestEncodeFailureWithoutTerminalRecord(t *testing.T) {
	withTestRuntime(t)
	taskID := "task_encode_lost"
	if err := os.MkdirAll(taskDir(taskID), 0o700); err != nil {
		t.Fatalf("mkdir task: %v", err)
	}
	listener, err := net.Listen("unix", sockPath(taskID))
	if err != nil {
		t.Fatalf("listen control socket: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	original := encodeControlRequest
	t.Cleanup(func() { encodeControlRequest = original })
	encodeControlRequest = func(net.Conn, controlRequest) error {
		return errors.New("injected request encode failure")
	}

	_, toolErr := controlTask(taskID, controlRequest{
		SchemaVersion: protocol.SchemaVersion,
		Op:            "poll",
	}, protocol.Limits{})
	if toolErr == nil || toolErr.Kind != "task_lost" || toolErr.Message != "task control request failed" {
		t.Fatalf("error = %+v; want task_lost request failure", toolErr)
	}
}

func TestSupervisorEntrypointsRejectInvalidTaskIDBeforeRuntimePath(t *testing.T) {
	withTestRuntime(t)
	cwd := t.TempDir()
	args := []string{"--task-id", "../bad", "--cmd", "true", "--cwd", cwd, "--env-json", `[]`, "--lifetime-ms", "1000"}
	if code := RunSupervisor(args); code != 2 {
		t.Fatalf("RunSupervisor exit = %d; want 2", code)
	}
	if code := RunSupervisorBootstrap(args); code != 2 {
		t.Fatalf("RunSupervisorBootstrap exit = %d; want 2", code)
	}
	if _, err := os.Stat(filepath.Join(currentRuntimeRoot(), "bad")); !os.IsNotExist(err) {
		t.Fatalf("escaped runtime path stat err = %v; want absent", err)
	}
}

func TestSupervisorEntrypointAuthorizationRejectsRegularFileMarker(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "auth-*")
	if err != nil {
		t.Fatalf("create auth fixture: %v", err)
	}
	if _, err := file.WriteString(supervisorAuthMarker); err != nil {
		t.Fatalf("write auth fixture: %v", err)
	}
	if _, err := file.Seek(0, 0); err != nil {
		t.Fatalf("seek auth fixture: %v", err)
	}
	t.Cleanup(func() { _ = file.Close() })
	dupFD, err := unix.Dup(int(file.Fd()))
	if err != nil {
		t.Fatalf("dup auth fixture fd: %v", err)
	}
	t.Setenv(supervisorAuthFDEnv, fmt.Sprint(dupFD))

	if SupervisorEntrypointAuthorized() {
		t.Fatal("regular-file supervisor auth marker was accepted")
	}
}

func TestSupervisorEntrypointAuthorizationRejectsForgedPipeMarker(t *testing.T) {
	readFile, writeFile, err := os.Pipe()
	if err != nil {
		t.Fatalf("create forged auth pipe: %v", err)
	}
	if _, err := io.WriteString(writeFile, supervisorAuthMarker); err != nil {
		t.Fatalf("write forged auth marker: %v", err)
	}
	_ = writeFile.Close()
	t.Cleanup(func() { _ = readFile.Close() })
	dupFD, err := unix.Dup(int(readFile.Fd()))
	if err != nil {
		t.Fatalf("dup forged auth pipe: %v", err)
	}
	t.Setenv(supervisorAuthFDEnv, fmt.Sprint(dupFD))

	if SupervisorEntrypointAuthorized() {
		t.Fatal("runtime-forgeable supervisor auth pipe was accepted")
	}
}

func TestSupervisorSetupFailureTerminatesStartedChild(t *testing.T) {
	withTestRuntime(t)
	for _, tc := range []struct {
		name       string
		failListen bool
	}{
		{name: "listener", failListen: true},
		{name: "metadata"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			originalListen := listenSupervisorControl
			originalWriteMeta := writeSupervisorMeta
			originalStarted := supervisorProcessStarted
			t.Cleanup(func() {
				listenSupervisorControl = originalListen
				writeSupervisorMeta = originalWriteMeta
				supervisorProcessStarted = originalStarted
			})
			if tc.failListen {
				listenSupervisorControl = func(string, string) (net.Listener, error) {
					return nil, errors.New("injected listener failure")
				}
			} else {
				writeSupervisorMeta = func(string, any) error {
					return errors.New("injected metadata failure")
				}
			}
			startedPID := 0
			supervisorProcessStarted = func(pid int) { startedPID = pid }
			taskID := "task_setup_failure_" + tc.name

			if code := runSupervisor(taskID, "sleep 30", t.TempDir(), os.Environ(), time.Minute); code != 1 {
				t.Fatalf("runSupervisor exit = %d; want setup failure", code)
			}
			if startedPID <= 0 {
				t.Fatal("supervisor child did not start before injected setup failure")
			}
			if err := syscall.Kill(startedPID, 0); !errors.Is(err, syscall.ESRCH) {
				t.Fatalf("started child pid %d still exists after setup failure: %v", startedPID, err)
			}
		})
	}
}

func TestDetachedExecPollReturnsWhenOutputArrivesBeforeWaitDeadline(t *testing.T) {
	fs := newExecFixture(t)
	withTestSupervisor(t)
	taskID := "task_poll_output_wait"
	response := RunExec(fs.payload(map[string]any{
		"cmd":            "sleep 0.35; /bin/echo ready; sleep 10",
		"on_wait_expiry": "detach",
		"task_id":        taskID,
		"wait_ms":        0,
	}))
	if response.Status != protocol.ToolStatusRunning {
		t.Fatalf("detach response = %+v; want running", response)
	}

	started := time.Now()
	poll := RunPoll(fs.payloadWithTool("poll", map[string]any{"task_id": taskID, "wait_ms": 1000}))
	elapsed := time.Since(started)
	if poll.Status != protocol.ToolStatusRunning || !strings.Contains(poll.Result.Stdout.Text, "ready") {
		t.Fatalf("poll response = %+v; want running response with ready output", poll)
	}
	if elapsed >= 800*time.Millisecond {
		t.Fatalf("poll elapsed %s; want return before wait deadline after output arrives", elapsed)
	}

	cancel := RunCancel(fs.payloadWithTool("cancel", map[string]any{"task_id": taskID}))
	if cancel.Status != protocol.ToolStatusSuccess {
		t.Fatalf("cancel response = %+v; want success", cancel)
	}
}

func TestSupervisorPollDrainsPendingOutputInMemory(t *testing.T) {
	state := newSupervisorState("task_poll_memory", "cmd")
	state.mu.Lock()
	_, _ = state.stdoutPending.Write([]byte("ready\n"))
	state.mu.Unlock()

	result := state.handlePoll(controlRequest{WaitMS: 1000, VisibleBytes: 50 * 1024, VisibleLines: 2000})

	if result.Stdout.Text != "ready\n" || result.Stdout.ReturnedBytes != len("ready\n") {
		t.Fatalf("poll result = %+v; want pending ready output", result)
	}
}

func TestDetachedExecTerminalReturnsOnlyUndrainedOutputAfterRunningDrain(t *testing.T) {
	fs := newExecFixture(t)
	withTestSupervisor(t)
	taskID := "task_terminal_fact"
	response := RunExec(fs.payload(map[string]any{
		"cmd":            "printf 'first\\n'; sleep 0.3; printf 'second\\n'",
		"on_wait_expiry": "detach",
		"task_id":        taskID,
		"wait_ms":        250,
	}))
	if response.Status != protocol.ToolStatusRunning {
		t.Fatalf("detach response = %+v; want running", response)
	}
	if !strings.Contains(response.Result.Stdout.Text, "first") {
		t.Fatalf("initial running stdout = %q; want first output", response.Result.Stdout.Text)
	}
	waitForExitRecord(t, taskID)
	terminal := RunPoll(fs.payloadWithTool("poll", map[string]any{"task_id": taskID, "wait_ms": 0}))
	if terminal.Status != protocol.ToolStatusSuccess || strings.Contains(terminal.Result.Stdout.Text, "first") || !strings.Contains(terminal.Result.Stdout.Text, "second") {
		t.Fatalf("terminal response = %+v; want only undrained second output", terminal)
	}
	again := RunPoll(fs.payloadWithTool("poll", map[string]any{"task_id": taskID, "wait_ms": 0}))
	if again.Status != protocol.ToolStatusSuccess || again.Result.Stdout.Text != terminal.Result.Stdout.Text {
		t.Fatalf("terminal replay stdout = %q; want stable %q", again.Result.Stdout.Text, terminal.Result.Stdout.Text)
	}
	logBody, err := os.ReadFile(stdoutLogPath(taskID))
	if err != nil {
		t.Fatalf("read stdout.log: %v", err)
	}
	if !strings.Contains(string(logBody), "first") || !strings.Contains(string(logBody), "second") {
		t.Fatalf("stdout.log = %q; want durable full capture", string(logBody))
	}
	exitBody, err := os.ReadFile(exitPath(taskID))
	if err != nil {
		t.Fatalf("read exit.json: %v", err)
	}
	var record exitRecord
	if err := json.Unmarshal(exitBody, &record); err != nil {
		t.Fatalf("decode exit.json: %v", err)
	}
	if record.ExitCode == nil || *record.ExitCode != 0 || record.Stdout != terminal.Result.Stdout || record.Stderr != terminal.Result.Stderr {
		t.Fatalf("exit record = %+v terminal = %+v; want field-for-field terminal streams", record, terminal.Result)
	}
}

func TestDetachedExecWaitsForTerminalAfterEarlyOutput(t *testing.T) {
	fs := newExecFixture(t)
	withTestSupervisor(t)
	response := RunExec(fs.payload(map[string]any{
		"cmd":            "printf 'early\\n'; sleep 0.05; printf 'done\\n'",
		"on_wait_expiry": "detach",
		"task_id":        "task_early_terminal",
		"wait_ms":        500,
	}))
	if response.Status != protocol.ToolStatusSuccess || response.Result.ExitCode == nil || *response.Result.ExitCode != 0 {
		t.Fatalf("detach response = %+v; want terminal success within wait_ms", response)
	}
	if !strings.Contains(response.Result.Stdout.Text, "early") || !strings.Contains(response.Result.Stdout.Text, "done") {
		t.Fatalf("terminal stdout = %q; want early and done", response.Result.Stdout.Text)
	}
}

func TestDetachedExecMirrorsLogWhileRunning(t *testing.T) {
	fs := newExecFixture(t)
	withTestSupervisor(t)
	taskID := "task_running_log"
	response := RunExec(fs.payload(map[string]any{
		"cmd":            "printf 'ready\\n'; sleep 10",
		"on_wait_expiry": "detach",
		"task_id":        taskID,
		"wait_ms":        250,
	}))
	if response.Status != protocol.ToolStatusRunning {
		t.Fatalf("detach response = %+v; want running", response)
	}
	body, err := os.ReadFile(stdoutLogPath(taskID))
	if err != nil {
		t.Fatalf("read running stdout.log: %v", err)
	}
	if !strings.Contains(string(body), "ready") {
		t.Fatalf("running stdout.log = %q; want ready output mirrored before terminal", string(body))
	}
	cancel := RunCancel(fs.payloadWithTool("cancel", map[string]any{"task_id": taskID}))
	if cancel.Status != protocol.ToolStatusSuccess {
		t.Fatalf("cancel response = %+v; want success", cancel)
	}
}

func TestDetachedExecNoOutputCreatesLogFiles(t *testing.T) {
	fs := newExecFixture(t)
	withTestSupervisor(t)
	taskID := "task_empty_logs"
	response := RunExec(fs.payload(map[string]any{
		"cmd":            "true",
		"on_wait_expiry": "detach",
		"task_id":        taskID,
		"wait_ms":        500,
	}))
	if response.Status != protocol.ToolStatusSuccess {
		response = waitForTerminalPoll(t, fs, taskID)
	}
	for _, path := range []string{stdoutLogPath(taskID), stderrLogPath(taskID)} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}
		if info.Mode().Perm() != 0o600 || info.Size() != 0 {
			t.Fatalf("%s mode/size = %v/%d; want 0600 empty", path, info.Mode().Perm(), info.Size())
		}
	}
}

func TestStreamMirrorFinalizeUsesHeadSeamTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stdout.log")
	mirror := newStreamMirror(path)
	produced := []byte(strings.Repeat("A", 600000) + strings.Repeat("B", 600000) + "\n")
	if err := mirror.appendProduced(produced); err != nil {
		t.Fatalf("appendProduced: %v", err)
	}
	buffer := bound.NewCapBuffer(bound.DefaultStreamStoreBytes)
	if _, err := buffer.Write(produced); err != nil {
		t.Fatalf("write buffer: %v", err)
	}
	if err := mirror.finalize(storedCaptureText(buffer)); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	logBody, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read stdout.log: %v", err)
	}
	logText := string(logBody)
	if len(logBody) > bound.DefaultStreamStoreBytes+128 || !strings.HasPrefix(logText, "A") ||
		!strings.Contains(logText, "bytes truncated") || !strings.HasSuffix(logText, "B\n") {
		t.Fatalf("stdout.log len=%d prefix=%q suffix=%q; want bounded head/seam/tail", len(logBody), logText[:min(len(logText), 16)], logText[max(0, len(logText)-16):])
	}
}

func TestDetachedExecAndPollRespectLowerVisibleLimits(t *testing.T) {
	fs := newExecFixture(t)
	withTestSupervisor(t)
	taskID := "task_lower_limits"
	response := RunExec(fs.payloadWithLimits(map[string]any{
		"cmd":            "printf 'abcdef'; sleep 10",
		"on_wait_expiry": "detach",
		"task_id":        taskID,
		"wait_ms":        250,
	}, protocol.Limits{VisibleBytes: 4, VisibleLines: 2000}))
	if response.Status != protocol.ToolStatusRunning || response.Result.Stdout.ReturnedBytes > 4 || !response.Result.Stdout.Truncated {
		t.Fatalf("detach response = %+v; want lower-limited running stdout", response)
	}
	cancel := RunCancel(fs.payloadWithToolLimits("cancel", map[string]any{"task_id": taskID}, protocol.Limits{VisibleBytes: 4, VisibleLines: 2000}))
	if cancel.Status != protocol.ToolStatusSuccess || cancel.Result.Stdout.Text != "" || cancel.Result.Stdout.Truncated {
		t.Fatalf("cancel response = %+v; want terminal stdout already drained", cancel)
	}
	again := RunPoll(fs.payloadWithToolLimits("poll", map[string]any{"task_id": taskID, "wait_ms": 0}, protocol.Limits{VisibleBytes: 4, VisibleLines: 2000}))
	if again.Status != protocol.ToolStatusSuccess || again.Result.Stdout.Text != "" || again.Result.Stdout.Truncated {
		t.Fatalf("terminal poll = %+v; want stable drained terminal stdout", again)
	}
}

func TestDetachedExecStdinWriteSeqAndPollDrain(t *testing.T) {
	fs := newExecFixture(t)
	withTestSupervisor(t)
	taskID := "task_stdin_seq"
	response := RunExec(fs.payload(map[string]any{
		"cmd":            "while IFS= read -r line; do printf \"got:$line\\n\"; done",
		"on_wait_expiry": "detach",
		"task_id":        taskID,
		"wait_ms":        250,
	}))
	if response.Status != protocol.ToolStatusRunning {
		t.Fatalf("detach response = %+v; want running", response)
	}
	seq := int64(1)
	write := RunStdin(fs.payloadWithTool("stdin", map[string]any{"task_id": taskID, "chars": "one\n", "write_seq": seq, "wait_ms": 250}))
	if write.Status != protocol.ToolStatusRunning || write.Result.WrittenBytes != len("one\n") || !strings.Contains(write.Result.Stdout.Text, "got:one") {
		t.Fatalf("stdin write response = %+v; want got:one", write)
	}
	firstTotal := int64(len("got:one\n"))
	if write.Result.Stdout.TotalBytes != firstTotal {
		t.Fatalf("first stdout total = %d; want %d", write.Result.Stdout.TotalBytes, firstTotal)
	}
	replay := RunStdin(fs.payloadWithTool("stdin", map[string]any{"task_id": taskID, "chars": "one\n", "write_seq": seq, "wait_ms": 0}))
	if replay.Status != protocol.ToolStatusRunning || replay.Result.WrittenBytes != 0 || strings.Contains(replay.Result.Stdout.Text, "got:one") {
		t.Fatalf("stdin replay response = %+v; want dropped replay with drained output", replay)
	}
	seq = 2
	second := RunStdin(fs.payloadWithTool("stdin", map[string]any{"task_id": taskID, "chars": "two\n", "write_seq": seq, "wait_ms": 250}))
	if second.Status != protocol.ToolStatusRunning || !strings.Contains(second.Result.Stdout.Text, "got:two") || strings.Contains(second.Result.Stdout.Text, "got:one") {
		t.Fatalf("second stdin response = %+v; want only got:two text", second)
	}
	wantTotal := int64(len("got:one\n") + len("got:two\n"))
	if second.Result.Stdout.TotalBytes != wantTotal {
		t.Fatalf("second stdout total = %d; want cumulative %d", second.Result.Stdout.TotalBytes, wantTotal)
	}
	cancel := RunCancel(fs.payloadWithTool("cancel", map[string]any{"task_id": taskID}))
	if cancel.Status != protocol.ToolStatusSuccess || !cancel.Result.Cancelled {
		t.Fatalf("cancel response = %+v; want terminal cancelled", cancel)
	}
}

func TestWriteTaskStdinReturnsWhenPipeBackpressures(t *testing.T) {
	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	t.Cleanup(func() {
		_ = readEnd.Close()
		_ = writeEnd.Close()
	})
	fd := int(writeEnd.Fd())
	if err := unix.SetNonblock(fd, true); err != nil {
		t.Fatalf("set nonblocking pipe: %v", err)
	}
	fill := strings.Repeat("p", 4096)
	for {
		_, err := unix.Write(fd, []byte(fill))
		if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
			break
		}
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			t.Fatalf("prefill pipe: %v", err)
		}
	}
	startedAt := time.Now()
	written, err := writeTaskStdin(writeEnd, strings.Repeat("x", maxStdinBytes), 50*time.Millisecond)
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("writeTaskStdin error = %v, written=%d; want deadline on backpressured pipe", err, written)
	}
	if elapsed := time.Since(startedAt); elapsed > time.Second {
		t.Fatalf("writeTaskStdin elapsed = %s; want bounded return under pipe backpressure", elapsed)
	}
	if written != 0 {
		t.Fatalf("written = %d; want no bytes written into already-backpressured pipe", written)
	}
}

func TestDetachedExecStdinWaitMSCollectsWindowOutput(t *testing.T) {
	fs := newExecFixture(t)
	withTestSupervisor(t)
	taskID := "task_stdin_wait_window"
	response := RunExec(fs.payload(map[string]any{
		"cmd":            "while IFS= read -r line; do printf immediate:$line; sleep 0.05; printf ':delayed\\n'; done",
		"on_wait_expiry": "detach",
		"task_id":        taskID,
		"wait_ms":        250,
	}))
	if response.Status != protocol.ToolStatusRunning {
		t.Fatalf("detach response = %+v; want running", response)
	}
	write := RunStdin(fs.payloadWithTool("stdin", map[string]any{"task_id": taskID, "chars": "one\n", "write_seq": 1, "wait_ms": 200}))
	if write.Status != protocol.ToolStatusRunning || !strings.Contains(write.Result.Stdout.Text, "immediate:one:delayed") {
		t.Fatalf("stdin wait response = %+v; want immediate and delayed output from wait_ms window", write)
	}
	cancel := RunCancel(fs.payloadWithTool("cancel", map[string]any{"task_id": taskID}))
	if cancel.Status != protocol.ToolStatusSuccess {
		t.Fatalf("cancel response = %+v; want success", cancel)
	}
}

func TestDetachedExecCtrlCSendsSIGINT(t *testing.T) {
	fs := newExecFixture(t)
	withTestSupervisor(t)
	taskID := "task_ctrl_c"
	response := RunExec(fs.payload(map[string]any{
		"cmd":            "trap 'printf trapped\\\\n; exit 130' INT; while :; do sleep 1; done",
		"on_wait_expiry": "detach",
		"task_id":        taskID,
		"wait_ms":        250,
	}))
	if response.Status != protocol.ToolStatusRunning {
		t.Fatalf("detach response = %+v; want running", response)
	}
	interrupt := RunStdin(fs.payloadWithTool("stdin", map[string]any{"task_id": taskID, "chars": "\u0003", "write_seq": 1, "wait_ms": 500}))
	sawTrapOutput := strings.Contains(interrupt.Result.Stdout.Text, "trapped")
	terminal := interrupt
	if terminal.Status != protocol.ToolStatusSuccess {
		terminal = waitForTerminalPoll(t, fs, taskID)
	}
	if terminal.Result.ExitCode == nil || *terminal.Result.ExitCode != 130 {
		t.Fatalf("terminal interrupt response = %+v; want trapped SIGINT exit 130", terminal)
	}
	if !sawTrapOutput && !strings.Contains(terminal.Result.Stdout.Text, "trapped") {
		t.Fatalf("interrupt/terminal responses = %+v / %+v; want trapped SIGINT output surfaced before drain", interrupt, terminal)
	}
	logBody, err := os.ReadFile(stdoutLogPath(taskID))
	if err != nil {
		t.Fatalf("read stdout.log: %v", err)
	}
	if !strings.Contains(string(logBody), "trapped") {
		t.Fatalf("stdout.log = %q; want durable trapped SIGINT output", string(logBody))
	}
}

func TestDetachedExecStdinToTerminalTaskReportsTaskExited(t *testing.T) {
	fs := newExecFixture(t)
	withTestSupervisor(t)
	taskID := "task_stdin_exited"
	response := RunExec(fs.payload(map[string]any{
		"cmd":            "printf done",
		"on_wait_expiry": "detach",
		"task_id":        taskID,
		"wait_ms":        250,
	}))
	if response.Status != protocol.ToolStatusRunning && response.Status != protocol.ToolStatusSuccess {
		t.Fatalf("detach response = %+v; want running or terminal", response)
	}
	_ = waitForTerminalPoll(t, fs, taskID)
	write := RunStdin(fs.payloadWithTool("stdin", map[string]any{"task_id": taskID, "chars": "after\n", "write_seq": 1, "wait_ms": 250}))
	if write.Status != protocol.ToolStatusError || write.Error == nil || write.Error.Kind != "task_exited" {
		t.Fatalf("stdin terminal response = %+v; want task_exited", write)
	}
	if write.Result.ExitCode == nil || *write.Result.ExitCode != 0 {
		t.Fatalf("stdin terminal result = %+v; want completed exit code", write.Result)
	}
	if write.Error.Detail == nil || write.Error.Detail["terminal"] == nil {
		t.Fatalf("stdin terminal detail = %+v; want terminal detail", write.Error.Detail)
	}
}

func TestTerminalPersistenceFailureDoesNotPublishTerminal(t *testing.T) {
	fs := newExecFixture(t)
	withTestSupervisor(t)
	original := writeTerminalJSONAtomic
	writeTerminalJSONAtomic = func(path string, value any) error {
		if filepath.Base(path) == "exit.json" {
			return errors.New("injected exit write failure")
		}
		return original(path, value)
	}
	t.Cleanup(func() { writeTerminalJSONAtomic = original })
	taskID := "task_persist_failure"
	response := RunExec(fs.payload(map[string]any{
		"cmd":            "printf done",
		"on_wait_expiry": "detach",
		"task_id":        taskID,
		"wait_ms":        250,
	}))
	if response.Status == protocol.ToolStatusSuccess {
		t.Fatalf("detach response = %+v; must not publish terminal when exit.json cannot persist", response)
	}
	deadline := time.Now().Add(3 * time.Second)
	var poll Response
	for time.Now().Before(deadline) {
		poll = RunPoll(fs.payloadWithTool("poll", map[string]any{"task_id": taskID, "wait_ms": 50}))
		if poll.Status == protocol.ToolStatusError && poll.Error != nil && poll.Error.Kind == "task_lost" {
			break
		}
	}
	if poll.Status != protocol.ToolStatusError || poll.Error == nil || poll.Error.Kind != "task_lost" {
		t.Fatalf("poll after persist failure = %+v; want task_lost", poll)
	}
	if _, err := os.Stat(exitPath(taskID)); !os.IsNotExist(err) {
		t.Fatalf("exit.json stat err = %v; want absent failed terminal record", err)
	}
	if _, err := os.Stat(sockPath(taskID)); err != nil {
		t.Fatalf("control.sock stat err = %v; want socket retained for failure discovery", err)
	}
}

func TestDetachedExecCancelReportsTaskLostWhenTerminalPersistenceFails(t *testing.T) {
	fs := newExecFixture(t)
	withTestSupervisor(t)
	original := writeTerminalJSONAtomic
	writeTerminalJSONAtomic = func(path string, value any) error {
		if filepath.Base(path) == "exit.json" {
			return errors.New("injected exit write failure")
		}
		return original(path, value)
	}
	t.Cleanup(func() { writeTerminalJSONAtomic = original })
	taskID := "task_cancel_persist_failure"
	response := RunExec(fs.payload(map[string]any{
		"cmd":            "trap '' TERM; while :; do sleep 1; done",
		"on_wait_expiry": "detach",
		"task_id":        taskID,
		"wait_ms":        250,
	}))
	if response.Status != protocol.ToolStatusRunning {
		t.Fatalf("detach response = %+v; want running", response)
	}
	startedAt := time.Now()
	cancel := RunCancel(fs.payloadWithTool("cancel", map[string]any{"task_id": taskID}))
	if cancel.Status != protocol.ToolStatusError || cancel.Error == nil || cancel.Error.Kind != "task_lost" {
		t.Fatalf("cancel response = %+v; want bounded task_lost when terminal persistence fails", cancel)
	}
	if elapsed := time.Since(startedAt); elapsed > 5*time.Second {
		t.Fatalf("cancel elapsed = %s; want bounded wait after SIGKILL", elapsed)
	}
}

func TestDetachedExecCancelIsIdempotent(t *testing.T) {
	fs := newExecFixture(t)
	withTestSupervisor(t)
	taskID := "task_cancel_idempotent"
	response := RunExec(fs.payload(map[string]any{
		"cmd":            "sleep 10",
		"on_wait_expiry": "detach",
		"task_id":        taskID,
		"wait_ms":        250,
	}))
	if response.Status != protocol.ToolStatusRunning {
		t.Fatalf("detach response = %+v; want running", response)
	}
	first := RunCancel(fs.payloadWithTool("cancel", map[string]any{"task_id": taskID}))
	if first.Status != protocol.ToolStatusSuccess || first.Result.WasRunning == nil || !*first.Result.WasRunning {
		t.Fatalf("first cancel = %+v; want was_running true", first)
	}
	second := RunCancel(fs.payloadWithTool("cancel", map[string]any{"task_id": taskID}))
	if second.Status != protocol.ToolStatusSuccess || second.Result.WasRunning == nil || *second.Result.WasRunning {
		t.Fatalf("second cancel = %+v; want was_running false", second)
	}
}

func TestDetachedExecCancelEscalatesToSIGKILL(t *testing.T) {
	fs := newExecFixture(t)
	withTestSupervisor(t)
	taskID := "task_cancel_escalates"
	response := RunExec(fs.payload(map[string]any{
		"cmd":            "trap '' TERM; while :; do sleep 1; done",
		"on_wait_expiry": "detach",
		"task_id":        taskID,
		"wait_ms":        250,
	}))
	if response.Status != protocol.ToolStatusRunning {
		t.Fatalf("detach response = %+v; want running", response)
	}
	startedAt := time.Now()
	cancel := RunCancel(fs.payloadWithTool("cancel", map[string]any{"task_id": taskID}))
	if cancel.Status != protocol.ToolStatusSuccess || !cancel.Result.Cancelled || cancel.Result.Signal == nil || *cancel.Result.Signal != "killed" {
		t.Fatalf("cancel response = %+v; want cancelled killed terminal", cancel)
	}
	if elapsed := time.Since(startedAt); elapsed < killGrace {
		t.Fatalf("cancel elapsed = %s; want SIGTERM grace before SIGKILL", elapsed)
	}
}

func TestDetachedExecLifetimeExpiryRecordsTimeout(t *testing.T) {
	fs := newExecFixture(t)
	withTestSupervisor(t)
	taskID := "task_lifetime_expiry"
	response := RunExec(fs.payload(map[string]any{
		"cmd":              "sleep 10",
		"on_wait_expiry":   "detach",
		"task_id":          taskID,
		"wait_ms":          250,
		"task_lifetime_ms": 250,
	}))
	if response.Status != protocol.ToolStatusRunning && response.Status != protocol.ToolStatusSuccess {
		t.Fatalf("detach response = %+v; want running or expired terminal", response)
	}
	terminal := response
	if terminal.Status != protocol.ToolStatusSuccess {
		terminal = waitForTerminalPoll(t, fs, taskID)
	}
	if terminal.Status != protocol.ToolStatusSuccess || !terminal.Result.TimedOut {
		t.Fatalf("terminal = %+v; want timed_out expired", terminal)
	}
}

func TestDetachedSupervisorWaitsForInGroupGrandchildUntilLifetime(t *testing.T) {
	fs := newExecFixture(t)
	withTestSupervisor(t)
	taskID := "task_in_group_lifetime"
	lifetime := 300 * time.Millisecond
	startedAt := time.Now()
	response := RunExec(fs.payload(map[string]any{
		"cmd":              "printf 'before-group-eof\\n'; sleep 30 &",
		"on_wait_expiry":   "detach",
		"task_id":          taskID,
		"wait_ms":          50,
		"task_lifetime_ms": lifetime.Milliseconds(),
	}))
	if response.Status != protocol.ToolStatusRunning && response.Status != protocol.ToolStatusSuccess {
		t.Fatalf("detach response = %+v; want running or terminal success", response)
	}
	waitForExitRecord(t, taskID)
	elapsed := time.Since(startedAt)
	terminal := RunPoll(fs.payloadWithTool("poll", map[string]any{"task_id": taskID, "wait_ms": 0}))
	if terminal.Status != protocol.ToolStatusSuccess || !terminal.Result.TimedOut {
		t.Fatalf("terminal = %+v; want lifetime timeout", terminal)
	}
	if elapsed < lifetime-100*time.Millisecond || elapsed > lifetime+killGrace+time.Second {
		t.Fatalf("terminal elapsed = %s; want lifetime-bounded group EOF", elapsed)
	}
	logBody, err := os.ReadFile(stdoutLogPath(taskID))
	if err != nil {
		t.Fatalf("read stdout log: %v", err)
	}
	if !strings.Contains(string(logBody), "before-group-eof") {
		t.Fatalf("stdout log = %q; want bytes written before group EOF", string(logBody))
	}
}

func TestDetachedSupervisorEscapedGrandchildIsBoundedByLifetime(t *testing.T) {
	if _, err := exec.LookPath("setsid"); err != nil {
		t.Skip("setsid is required for escaped-grandchild coverage")
	}
	fs := newExecFixture(t)
	withTestSupervisor(t)
	taskID := "task_escaped_lifetime"
	lifetime := 300 * time.Millisecond
	startedAt := time.Now()
	response := RunExec(fs.payload(map[string]any{
		"cmd":              "setsid /bin/sh -c 'printf \"%s\" \"$$\" > escaped.pid; sleep 30' &",
		"on_wait_expiry":   "detach",
		"task_id":          taskID,
		"wait_ms":          50,
		"task_lifetime_ms": lifetime.Milliseconds(),
	}))
	if response.Status != protocol.ToolStatusRunning && response.Status != protocol.ToolStatusSuccess {
		t.Fatalf("detach response = %+v; want running or terminal success", response)
	}
	escapedPID := waitForChildPIDFile(t, filepath.Join(fs.workspace, "escaped.pid"))
	t.Cleanup(func() { _ = syscall.Kill(escapedPID, syscall.SIGKILL) })
	waitForExitRecord(t, taskID)
	elapsed := time.Since(startedAt)
	terminal := RunPoll(fs.payloadWithTool("poll", map[string]any{"task_id": taskID, "wait_ms": 0}))
	if terminal.Status != protocol.ToolStatusSuccess || !terminal.Result.TimedOut {
		t.Fatalf("terminal = %+v; want lifetime timeout", terminal)
	}
	if elapsed > lifetime+killGrace+time.Second {
		t.Fatalf("terminal elapsed = %s; want escaped holder bounded by lifetime plus kill grace", elapsed)
	}
}

func TestDetachedExecNullOrAbsentTaskLifetimeUsesDefault(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input map[string]any
	}{
		{
			name: "absent",
			input: map[string]any{
				"cmd":            "sleep 10",
				"on_wait_expiry": "detach",
				"task_id":        "task_default_lifetime_absent",
				"wait_ms":        250,
			},
		},
		{
			name: "null",
			input: map[string]any{
				"cmd":              "sleep 10",
				"on_wait_expiry":   "detach",
				"task_id":          "task_default_lifetime_null",
				"wait_ms":          250,
				"task_lifetime_ms": nil,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := newExecFixture(t)
			withTestSupervisor(t)
			response := RunExec(fs.payload(tc.input))
			if response.Status != protocol.ToolStatusRunning {
				t.Fatalf("detach response = %+v; want running", response)
			}
			taskID := tc.input["task_id"].(string)
			metaBytes, err := os.ReadFile(metaPath(taskID))
			if err != nil {
				t.Fatalf("read task meta: %v", err)
			}
			var meta metaRecord
			if err := json.Unmarshal(metaBytes, &meta); err != nil {
				t.Fatalf("decode task meta: %v", err)
			}
			startedAt, err := time.Parse(time.RFC3339Nano, meta.StartedAt)
			if err != nil {
				t.Fatalf("parse started_at: %v", err)
			}
			deadlineAt, err := time.Parse(time.RFC3339Nano, meta.LifetimeDeadlineAt)
			if err != nil {
				t.Fatalf("parse lifetime_deadline_at: %v", err)
			}
			if got := deadlineAt.Sub(startedAt); got != time.Duration(defaultTaskLifetimeMS)*time.Millisecond {
				t.Fatalf("task lifetime = %s; want default %s", got, time.Duration(defaultTaskLifetimeMS)*time.Millisecond)
			}
			cancel := RunCancel(fs.payloadWithTool("cancel", map[string]any{"task_id": taskID}))
			if cancel.Status != protocol.ToolStatusSuccess {
				t.Fatalf("cancel response = %+v; want success", cancel)
			}
		})
	}
}

func TestDetachedExecRejectsTaskLimit(t *testing.T) {
	fs := newExecFixture(t)
	withTestRuntime(t)
	var listeners []net.Listener
	t.Cleanup(func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	})
	for i := 0; i < maxLiveTasks; i++ {
		taskID := fmt.Sprintf("task_live_%02d", i)
		if err := os.MkdirAll(taskDir(taskID), 0o700); err != nil {
			t.Fatalf("mkdir live task: %v", err)
		}
		listener, err := net.Listen("unix", sockPath(taskID))
		if err != nil {
			t.Fatalf("listen live task: %v", err)
		}
		listeners = append(listeners, listener)
	}
	response := RunExec(fs.payload(map[string]any{
		"cmd":            "sleep 10",
		"on_wait_expiry": "detach",
		"task_id":        "task_live_overflow",
		"wait_ms":        250,
	}))
	if response.Status != protocol.ToolStatusError || response.Error == nil || response.Error.Kind != "task_limit" {
		t.Fatalf("task limit response = %+v; want task_limit", response)
	}
}

func TestExpiredStartupReservationDoesNotSweepLiveSupervisorSocket(t *testing.T) {
	withTestRuntime(t)
	taskID := "task_stale_reservation_live_socket"
	if err := os.MkdirAll(taskDir(taskID), 0o700); err != nil {
		t.Fatalf("mkdir task: %v", err)
	}
	if err := os.WriteFile(reservationPath(taskID), []byte("reserved"), 0o600); err != nil {
		t.Fatalf("write reservation: %v", err)
	}
	stale := time.Now().Add(-taskReservationMaxAge - time.Minute)
	if err := os.Chtimes(reservationPath(taskID), stale, stale); err != nil {
		t.Fatalf("age reservation: %v", err)
	}
	listener, err := net.Listen("unix", sockPath(taskID))
	if err != nil {
		t.Fatalf("listen live supervisor socket: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	accepted := make(chan struct{})
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr == nil {
			_ = conn.Close()
		}
		close(accepted)
	}()

	sweepOldTasks(time.Now())
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("sweep did not probe live supervisor socket")
	}
	if _, err := os.Stat(taskDir(taskID)); err != nil {
		t.Fatalf("live supervisor task directory stat = %v; want preserved", err)
	}
	if got := liveTaskCount(); got != 1 {
		t.Fatalf("liveTaskCount = %d; want live socket with stale reservation counted once", got)
	}
}

func TestDetachedExecReplayWaitsForStartupReservation(t *testing.T) {
	fs := newExecFixture(t)
	withTestRuntime(t)
	original := startSupervisor
	releaseStart := make(chan struct{})
	started := make(chan struct{})
	var releaseOnce sync.Once
	startSupervisor = func(taskID string, cmdText string, cwd string, env []string, lifetimeMS int) error {
		close(started)
		<-releaseStart
		go runSupervisor(taskID, cmdText, cwd, env, time.Duration(lifetimeMS)*time.Millisecond)
		return nil
	}
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseStart) })
		startSupervisor = original
	})
	taskID := "task_startup_replay"
	firstDone := make(chan Response, 1)
	go func() {
		firstDone <- RunExec(fs.payload(map[string]any{
			"cmd":            "sleep 10",
			"on_wait_expiry": "detach",
			"task_id":        taskID,
			"wait_ms":        250,
		}))
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("supervisor start hook was not reached")
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		releaseOnce.Do(func() { close(releaseStart) })
	}()
	replay := RunExec(fs.payload(map[string]any{
		"cmd":            "sleep 10",
		"on_wait_expiry": "detach",
		"task_id":        taskID,
		"wait_ms":        250,
	}))
	if replay.Status == protocol.ToolStatusError {
		t.Fatalf("startup replay response = %+v; want wait for supervisor state, not task_lost", replay)
	}
	select {
	case first := <-firstDone:
		if first.Status == protocol.ToolStatusError {
			t.Fatalf("first detach response = %+v; want running or terminal", first)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("first detach did not finish after supervisor start")
	}
	cancel := RunCancel(fs.payloadWithTool("cancel", map[string]any{"task_id": taskID}))
	if cancel.Status != protocol.ToolStatusSuccess {
		t.Fatalf("cancel response = %+v; want success", cancel)
	}
}

func TestRunSupervisorBootstrapFailsClosedWhenDevNullUnavailable(t *testing.T) {
	original := openSupervisorDevNull
	openSupervisorDevNull = func() (*os.File, error) { return nil, os.ErrPermission }
	t.Cleanup(func() { openSupervisorDevNull = original })

	code := RunSupervisorBootstrap([]string{
		"--task-id", "task_bootstrap_no_devnull",
		"--cmd", "true",
		"--cwd", t.TempDir(),
		"--env-json", "[]",
		"--lifetime-ms", "1000",
	})
	if code != 1 {
		t.Fatalf("RunSupervisorBootstrap exit = %d; want startup failure", code)
	}
}

func TestDetachedExecConcurrentTaskLimitReservation(t *testing.T) {
	fs := newExecFixture(t)
	withTestSupervisor(t)
	var wg sync.WaitGroup
	responses := make([]Response, maxLiveTasks+1)
	for i := range responses {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			responses[index] = RunExec(fs.payload(map[string]any{
				"cmd":            "sleep 10",
				"on_wait_expiry": "detach",
				"task_id":        fmt.Sprintf("task_concurrent_%02d", index),
				"wait_ms":        250,
			}))
		}(i)
	}
	wg.Wait()
	taskLimit := 0
	running := 0
	for i, response := range responses {
		if response.Status == protocol.ToolStatusError && response.Error != nil && response.Error.Kind == "task_limit" {
			taskLimit++
			continue
		}
		if response.Status == protocol.ToolStatusRunning {
			running++
			_ = RunCancel(fs.payloadWithTool("cancel", map[string]any{"task_id": fmt.Sprintf("task_concurrent_%02d", i)}))
			continue
		}
		t.Fatalf("response[%d] = %+v; want running or task_limit", i, response)
	}
	if taskLimit != 1 || running != maxLiveTasks {
		t.Fatalf("taskLimit/running = %d/%d; want 1/%d", taskLimit, running, maxLiveTasks)
	}
}

func TestLostTaskDirsDoNotExhaustTaskLimit(t *testing.T) {
	fs := newExecFixture(t)
	withTestSupervisor(t)
	for i := 0; i < maxLiveTasks; i++ {
		if err := os.MkdirAll(taskDir(fmt.Sprintf("task_lost_%02d", i)), 0o700); err != nil {
			t.Fatalf("mkdir lost task: %v", err)
		}
	}
	response := RunExec(fs.payload(map[string]any{
		"cmd":            "sleep 10",
		"on_wait_expiry": "detach",
		"task_id":        "task_lost_dirs_do_not_cap",
		"wait_ms":        250,
	}))
	if response.Status == protocol.ToolStatusError && response.Error != nil && response.Error.Kind == "task_limit" {
		t.Fatalf("lost task dirs exhausted cap: %+v", response)
	}
	if response.Status == protocol.ToolStatusRunning {
		cancel := RunCancel(fs.payloadWithTool("cancel", map[string]any{"task_id": "task_lost_dirs_do_not_cap"}))
		if cancel.Status != protocol.ToolStatusSuccess {
			t.Fatalf("cancel response = %+v; want success", cancel)
		}
	}
}

func TestTaskNotFoundAndTaskLost(t *testing.T) {
	fs := newExecFixture(t)
	withTestRuntime(t)
	missing := RunPoll(fs.payloadWithTool("poll", map[string]any{"task_id": "task_missing"}))
	if missing.Status != protocol.ToolStatusError || missing.Error == nil || missing.Error.Kind != "task_not_found" {
		t.Fatalf("missing poll = %+v; want task_not_found", missing)
	}
	lostID := "task_lost_case"
	if err := os.MkdirAll(taskDir(lostID), 0o700); err != nil {
		t.Fatalf("mkdir lost task: %v", err)
	}
	lost := RunPoll(fs.payloadWithTool("poll", map[string]any{"task_id": lostID, "wait_ms": 0}))
	if lost.Status != protocol.ToolStatusError || lost.Error == nil || lost.Error.Kind != "task_lost" {
		t.Fatalf("lost poll = %+v; want task_lost", lost)
	}
}

func TestTaskSweepAndIDValidation(t *testing.T) {
	fs := newExecFixture(t)
	withTestRuntime(t)
	invalid := RunPoll(fs.payloadWithTool("poll", map[string]any{"task_id": "../bad", "wait_ms": 0}))
	if invalid.Status != protocol.ToolStatusError || invalid.Error == nil || invalid.Error.Kind != "invalid_input" {
		t.Fatalf("invalid task poll = %+v; want invalid_input", invalid)
	}
	oldID := "task_old_terminal"
	newID := "task_new_terminal"
	for _, taskID := range []string{oldID, newID} {
		if err := os.MkdirAll(taskDir(taskID), 0o700); err != nil {
			t.Fatalf("mkdir task: %v", err)
		}
		if err := os.WriteFile(exitPath(taskID), []byte(`{"exit_code":0,"signal":null,"timed_out":false,"cancelled":false,"stdout":{"text":"","total_bytes":0,"total_lines":0,"returned_bytes":0,"truncated":false},"stderr":{"text":"","total_bytes":0,"total_lines":0,"returned_bytes":0,"truncated":false},"duration_ms":0}`), 0o600); err != nil {
			t.Fatalf("write exit: %v", err)
		}
	}
	oldTime := time.Now().Add(-25 * time.Hour)
	if err := os.Chtimes(exitPath(oldID), oldTime, oldTime); err != nil {
		t.Fatalf("chtimes old exit: %v", err)
	}
	sweepOldTasks(time.Now())
	if _, err := os.Stat(taskDir(oldID)); !os.IsNotExist(err) {
		t.Fatalf("old terminal task stat err = %v; want swept", err)
	}
	if _, err := os.Stat(taskDir(newID)); err != nil {
		t.Fatalf("new terminal task stat err = %v; want retained", err)
	}
}

func withTestSupervisor(t *testing.T) {
	t.Helper()
	withTestRuntime(t)
	original := startSupervisor
	var mu sync.Mutex
	var wg sync.WaitGroup
	taskIDs := make(map[string]struct{})
	startSupervisor = func(taskID string, cmdText string, cwd string, env []string, lifetimeMS int) error {
		mu.Lock()
		taskIDs[taskID] = struct{}{}
		mu.Unlock()
		wg.Add(1)
		go func() {
			defer wg.Done()
			runSupervisor(taskID, cmdText, cwd, env, time.Duration(lifetimeMS)*time.Millisecond)
		}()
		return nil
	}
	t.Cleanup(func() {
		startSupervisor = original
		mu.Lock()
		ids := make([]string, 0, len(taskIDs))
		for taskID := range taskIDs {
			ids = append(ids, taskID)
		}
		mu.Unlock()
		for _, taskID := range ids {
			killTestTaskGroup(taskID)
			_ = os.RemoveAll(taskDir(taskID))
		}
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatalf("test supervisors did not exit after cleanup")
		}
	})
}

func killTestTaskGroup(taskID string) {
	body, err := os.ReadFile(metaPath(taskID))
	if err != nil {
		return
	}
	var meta metaRecord
	if err := json.Unmarshal(body, &meta); err != nil || meta.ChildPGID <= 0 {
		return
	}
	killProcessGroup(meta.ChildPGID)
}

func withTestRuntime(t *testing.T) {
	t.Helper()
	originalRoot := currentRuntimeRoot()
	shortRoot, err := os.MkdirTemp("/tmp", "ttr-*")
	if err != nil {
		t.Fatalf("mkdir short runtime root: %v", err)
	}
	setRuntimeRoot(filepath.Join(shortRoot, "rt"))
	root := filepath.Join(currentRuntimeRoot(), "tasks")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("mkdir tasks root: %v", err)
	}
	t.Cleanup(func() {
		setRuntimeRoot(originalRoot)
		_ = os.RemoveAll(shortRoot)
	})
}

func waitForTerminalPoll(t *testing.T, fs execFixture, taskID string) Response {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last Response
	for time.Now().Before(deadline) {
		last = RunPoll(fs.payloadWithTool("poll", map[string]any{"task_id": taskID, "wait_ms": 50}))
		if last.Status == protocol.ToolStatusSuccess {
			return last
		}
		if last.Status == protocol.ToolStatusError {
			t.Fatalf("poll error = %+v", last)
		}
	}
	t.Fatalf("task %s did not become terminal; last=%+v", taskID, last)
	return Response{}
}

func waitForExitRecord(t *testing.T, taskID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(exitPath(taskID)); err == nil {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("task %s did not persist exit record", taskID)
}

func (f execFixture) payloadWithTool(tool string, input map[string]any) protocol.Payload {
	f.t.Helper()
	return f.payloadWithToolLimits(tool, input, protocol.Limits{VisibleBytes: 50 * 1024, VisibleLines: 2000})
}

func (f execFixture) payloadWithToolLimits(tool string, input map[string]any, limits protocol.Limits) protocol.Payload {
	f.t.Helper()
	body, err := json.Marshal(input)
	if err != nil {
		f.t.Fatalf("marshal input: %v", err)
	}
	return protocol.Payload{
		SchemaVersion:  protocol.SchemaVersion,
		Tool:           tool,
		ToolUseEventID: "evt_" + tool,
		WorkspaceRoot:  f.workspace,
		Roots:          []protocol.Root{{Path: f.workspace, Mode: protocol.RootModeReadWrite}},
		Limits:         limits,
		Input:          body,
	}
}
