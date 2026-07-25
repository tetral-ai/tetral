package task

import (
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/tetral-ai/tetral/internal/sandbox/helper/internal/bound"
	"github.com/tetral-ai/tetral/internal/sandbox/helper/internal/pathsafe"
	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
)

const (
	// maxExecWaitMS is the 50 s ceiling wait_ms clamps to ([250, 50000]). The
	// value is the per-invocation transport budget: the client wall-clocks a
	// call at 60 s, and 50 s leaves closeout grace inside that cap. A
	// provider timeout longer than 50 s is not a longer block here; it is a
	// caller-side detach + bounded-poll composition.
	defaultExecWaitMS = 10000
	minExecWaitMS     = 250
	maxExecWaitMS     = 50000
	killGrace         = 2 * time.Second
	stdoutWatchPoll   = 100 * time.Millisecond

	// defaultTaskLifetimeMS / maxTaskLifetimeMS bound a detached task's hard
	// lifetime to 30 min default, 60 min max. An absent or null
	// task_lifetime_ms means the default, never unlimited, so every detached
	// task has a hard lifetime the supervisor enforces; an unattended sandbox
	// cannot accumulate immortal processes.
	//
	// defaultPollWaitMS is the poll default (5 s), clamped [0, 50000]. The
	// Codex-family empty-poll window of up to 300 s is not a single long block
	// here; it is a caller-side composition of repeated bounded polls.
	//
	// maxStdinBytes caps chars per stdin call at 32 KiB; more is invalid_input.
	//
	// maxLiveTasks caps live supervisors per sandbox at 16; a detach beyond it
	// returns task_limit. The helper needs its own cap because the scheduler's
	// maxConcurrentTools bounds concurrent tool CALLS, not resident background
	// tasks, and a background task outlives the call that launched it.
	defaultTaskLifetimeMS = 1800000
	maxTaskLifetimeMS     = 3600000
	defaultPollWaitMS     = 5000
	defaultStdinWaitMS    = 250
	maxStdinBytes         = 32 * 1024
	maxLiveTasks          = 16
)

const foregroundShellWrapper = `
tetral_shell_pid=$$
tetral_runtime_root="/tmp/tetral-runtime/foreground"
mkdir -p "$tetral_runtime_root"
tetral_done="${tetral_runtime_root}/${tetral_shell_pid}.done"
rm -f "$tetral_done"
(
  while kill -0 "$tetral_shell_pid" 2>/dev/null; do
    if [ -e "$tetral_done" ]; then
      exit 0
    fi
    sleep 0.1
  done
  kill -TERM -- "-$tetral_shell_pid" 2>/dev/null || true
  sleep 2
  kill -KILL -- "-$tetral_shell_pid" 2>/dev/null || true
) &
tetral_watch_pid=$!
trap 'tetral_status=$?; : > "$tetral_done" 2>/dev/null || true; kill "$tetral_watch_pid" 2>/dev/null || true; wait "$tetral_watch_pid" 2>/dev/null || true; rm -f "$tetral_done" 2>/dev/null || true; exit "$tetral_status"' EXIT HUP INT TERM
"$1" -c "$2"
`

type ExecResult struct {
	TaskID       *string              `json:"task_id"`
	ExitCode     *int                 `json:"exit_code"`
	Signal       *string              `json:"signal"`
	TimedOut     bool                 `json:"timed_out"`
	Cancelled    bool                 `json:"cancelled,omitempty"`
	WasRunning   *bool                `json:"was_running,omitempty"`
	WrittenBytes int                  `json:"written_bytes,omitempty"`
	Stdout       bound.StreamSnapshot `json:"stdout"`
	Stderr       bound.StreamSnapshot `json:"stderr"`
	DurationMS   int64                `json:"duration_ms"`
}

type Response struct {
	Status        string
	Result        ExecResult
	Error         *protocol.ToolError
	HelperFailure bool
}

type execInput struct {
	Cmd            string         `json:"cmd"`
	CWD            *string        `json:"cwd"`
	Env            map[string]any `json:"env"`
	WaitMS         *int           `json:"wait_ms"`
	OnWaitExpiry   string         `json:"on_wait_expiry"`
	TaskID         *string        `json:"task_id"`
	TaskLifetimeMS *int           `json:"task_lifetime_ms"`
}

type FollowInput struct {
	TaskID   string `json:"task_id"`
	Chars    string `json:"chars,omitempty"`
	WaitMS   *int   `json:"wait_ms,omitempty"`
	WriteSeq *int64 `json:"write_seq,omitempty"`
	Reason   string `json:"reason,omitempty"`
}

type processWaitResult struct {
	state *os.ProcessState
	err   error
}

func RunExec(payload protocol.Payload) Response {
	return runExec(payload, func() uintptr { return os.Stdout.Fd() })
}

// runExec dispatches on on_wait_expiry, a two-mode machine over what happens
// when the child is still alive at wait_ms:
//
//	mode    | at wait_ms                | terminal shape             | requires
//	--------|---------------------------|----------------------------|----------
//	kill    | SIGTERM group, 2s, SIGKILL| error(timeout) + partial   | (default)
//	(default)|                          | output in result           |
//	detach  | leave under a supervisor  | running + result.task_id   | task_id
//
// kill returns the captured output alongside the timeout error. detach hands
// the child to a supervisor (runDetached) and returns immediately with a
// task_id the caller polls; it requires task_id in the payload.
func runExec(payload protocol.Payload, helperStdoutFD func() uintptr) Response {
	var input execInput
	if err := json.Unmarshal(payload.Input, &input); err != nil {
		return errorResponse("invalid_input", "input must be an object", ExecResult{})
	}
	if strings.TrimSpace(input.Cmd) == "" {
		return errorResponse("invalid_input", "cmd is required", ExecResult{})
	}
	mode := input.OnWaitExpiry
	if mode == "" {
		mode = "kill"
	}
	if mode == "detach" {
		if input.TaskID == nil || *input.TaskID == "" {
			return errorResponse("invalid_input", "task_id is required for detach", ExecResult{})
		}
		if !helperIDPattern.MatchString(*input.TaskID) {
			return errorResponse("invalid_input", "task_id has invalid shape", ExecResult{})
		}
	}
	cwdInput := ""
	if input.CWD != nil {
		cwdInput = *input.CWD
	}
	cwd, err := pathsafe.ResolveExecCWD(payload.WorkspaceRoot, cwdInput)
	if err != nil {
		return Response{Status: protocol.ToolStatusError, Error: pathsafe.ToolError(err)}
	}
	env, toolErr := execEnv(input.Env)
	if toolErr != nil {
		return Response{Status: protocol.ToolStatusError, Error: toolErr}
	}
	waitMS := clampInt(defaultInt(input.WaitMS, defaultExecWaitMS), minExecWaitMS, maxExecWaitMS)
	if mode == "detach" {
		lifetimeMS := clampInt(defaultInt(input.TaskLifetimeMS, defaultTaskLifetimeMS), minExecWaitMS, maxTaskLifetimeMS)
		return runDetached(payload, input, cwd.Path, env, waitMS, lifetimeMS)
	}
	if mode != "kill" {
		return errorResponse("invalid_input", "on_wait_expiry must be kill or detach", ExecResult{})
	}
	return runForeground(input.Cmd, cwd.Path, env, time.Duration(waitMS)*time.Millisecond, payload.Limits, helperStdoutFD)
}

func RunStdin(payload protocol.Payload) Response {
	var input FollowInput
	if err := json.Unmarshal(payload.Input, &input); err != nil {
		return errorResponse("invalid_input", "input must be an object", ExecResult{})
	}
	if input.TaskID == "" {
		return errorResponse("invalid_input", "task_id is required", ExecResult{})
	}
	if input.Chars == "" {
		return errorResponse("invalid_input", "chars is required", ExecResult{})
	}
	if len([]byte(input.Chars)) > maxStdinBytes {
		return errorResponse("invalid_input", "chars exceeds 32 KiB", ExecResult{})
	}
	if input.WriteSeq == nil || *input.WriteSeq <= 0 {
		return errorResponse("invalid_input", "positive write_seq is required", ExecResult{})
	}
	waitMS := clampInt(defaultInt(input.WaitMS, defaultStdinWaitMS), defaultStdinWaitMS, 30000)
	result, toolErr := controlTask(input.TaskID, controlRequest{
		SchemaVersion: protocol.SchemaVersion,
		Op:            "stdin",
		Chars:         input.Chars,
		WaitMS:        waitMS,
		WriteSeq:      input.WriteSeq,
		VisibleBytes:  limitVisibleBytes(payload.Limits),
		VisibleLines:  limitVisibleLines(payload.Limits),
	}, payload.Limits)
	if toolErr != nil {
		if isTerminalResult(result) {
			return Response{Status: protocol.ToolStatusError, Error: toolErr, Result: result}
		}
		return Response{Status: protocol.ToolStatusError, Error: toolErr, Result: result}
	}
	if isTerminalResult(result) {
		return Response{Status: protocol.ToolStatusSuccess, Result: result}
	}
	return Response{Status: protocol.ToolStatusRunning, Result: result}
}

func RunPoll(payload protocol.Payload) Response {
	var input FollowInput
	if err := json.Unmarshal(payload.Input, &input); err != nil {
		return errorResponse("invalid_input", "input must be an object", ExecResult{})
	}
	if input.TaskID == "" {
		return errorResponse("invalid_input", "task_id is required", ExecResult{})
	}
	waitMS := clampInt(defaultInt(input.WaitMS, defaultPollWaitMS), 0, maxExecWaitMS)
	result, toolErr := controlTask(input.TaskID, controlRequest{
		SchemaVersion: protocol.SchemaVersion,
		Op:            "poll",
		WaitMS:        waitMS,
		VisibleBytes:  limitVisibleBytes(payload.Limits),
		VisibleLines:  limitVisibleLines(payload.Limits),
	}, payload.Limits)
	if toolErr != nil {
		return Response{Status: protocol.ToolStatusError, Error: toolErr, Result: result}
	}
	if isTerminalResult(result) {
		return Response{Status: protocol.ToolStatusSuccess, Result: result}
	}
	return Response{Status: protocol.ToolStatusRunning, Result: result}
}

func RunCancel(payload protocol.Payload) Response {
	var input FollowInput
	if err := json.Unmarshal(payload.Input, &input); err != nil {
		return errorResponse("invalid_input", "input must be an object", ExecResult{})
	}
	if input.TaskID == "" {
		return errorResponse("invalid_input", "task_id is required", ExecResult{})
	}
	result, toolErr := controlTask(input.TaskID, controlRequest{
		SchemaVersion: protocol.SchemaVersion,
		Op:            "cancel",
		WaitMS:        maxExecWaitMS,
		VisibleBytes:  limitVisibleBytes(payload.Limits),
		VisibleLines:  limitVisibleLines(payload.Limits),
	}, payload.Limits)
	if toolErr != nil {
		return Response{Status: protocol.ToolStatusError, Error: toolErr, Result: result}
	}
	if result.WasRunning == nil {
		wasRunning := false
		result.WasRunning = &wasRunning
	}
	return Response{Status: protocol.ToolStatusSuccess, Result: result}
}

func runForeground(cmdText string, cwd string, env []string, wait time.Duration, limits protocol.Limits, helperStdoutFD func() uintptr) Response {
	startedAt := time.Now()
	shell := "/bin/bash"
	if _, err := os.Stat(shell); err != nil {
		shell = "/bin/sh"
	}
	cmd := exec.Command(shell, "-c", foregroundShellWrapper, "tetral-helper-exec", shell, cmdText) //nolint:gosec // The exec tool intentionally runs caller-supplied commands inside the sandbox helper boundary.
	cmd.Dir = cwd
	cmd.Env = env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Pdeathsig: syscall.SIGKILL}

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return errorResponse("exec_spawn_failed", "stdout pipe could not be created", ExecResult{})
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return errorResponse("exec_spawn_failed", "stderr pipe could not be created", ExecResult{})
	}
	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return errorResponse("exec_spawn_failed", "stdin pipe could not be created", ExecResult{})
	}
	stdoutBuffer := bound.NewCapBuffer(bound.DefaultStreamStoreBytes)
	stderrBuffer := bound.NewCapBuffer(bound.DefaultStreamStoreBytes)
	if err := cmd.Start(); err != nil {
		_ = stdinPipe.Close()
		return errorResponse("exec_spawn_failed", "process could not start", ExecResult{})
	}
	var copyWG sync.WaitGroup
	copyWG.Add(2)
	go copyStream(&copyWG, stdoutBuffer, stdoutPipe)
	go copyStream(&copyWG, stderrBuffer, stderrPipe)
	forceClosePipes := sync.OnceFunc(func() {
		_ = stdoutPipe.Close()
		_ = stderrPipe.Close()
	})
	stdoutClosed := make(chan struct{}, 1)
	stopStdoutWatch := make(chan struct{})
	var stdoutWatchWG sync.WaitGroup
	stdoutWatchWG.Add(1)
	go func() {
		defer stdoutWatchWG.Done()
		watchHelperStdoutClosed(stopStdoutWatch, stdoutClosed, helperStdoutFD)
	}()
	defer func() {
		close(stopStdoutWatch)
		stdoutWatchWG.Wait()
	}()
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	defer signal.Stop(signalCh)
	waitDone := make(chan processWaitResult, 1)
	go func() {
		state, err := cmd.Process.Wait()
		waitDone <- processWaitResult{state: state, err: err}
	}()
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case waited := <-waitDone:
		_ = stdinPipe.Close()
		waitForCopies(&copyWG, killGrace)
		forceClosePipes()
		copyWG.Wait()
		return terminalResponse(waited.state, waited.err, false, stdoutBuffer, stderrBuffer, startedAt, limits)
	case <-stdoutClosed:
		terminateProcessGroup(cmd.Process.Pid)
		_ = stdinPipe.Close()
		select {
		case <-waitDone:
		case <-time.After(killGrace):
			killProcessGroup(cmd.Process.Pid)
		}
		forceClosePipes()
		copyWG.Wait()
		return helperFailureResponse()
	case sig := <-signalCh:
		terminateProcessGroup(cmd.Process.Pid)
		_ = stdinPipe.Close()
		select {
		case <-waitDone:
		case <-time.After(killGrace):
			killProcessGroup(cmd.Process.Pid)
		}
		forceClosePipes()
		copyWG.Wait()
		exitForSignal(sig)
		return Response{}
	case <-timer.C:
		terminateProcessGroup(cmd.Process.Pid)
		_ = stdinPipe.Close()
		var waited processWaitResult
		exited := false
		select {
		case waited = <-waitDone:
			exited = true
		case <-time.After(killGrace):
		}
		killProcessGroup(cmd.Process.Pid)
		forceClosePipes()
		if !exited {
			select {
			case waited = <-waitDone:
			case <-time.After(killGrace):
			}
		}
		copyWG.Wait()
		result := execResult(waited.state, waited.err, true, stdoutBuffer, stderrBuffer, startedAt, limits)
		return Response{Status: protocol.ToolStatusError, Error: &protocol.ToolError{Kind: "timeout", Message: "command timed out"}, Result: result}
	}
}

func watchHelperStdoutClosed(done <-chan struct{}, closed chan<- struct{}, helperStdoutFD func() uintptr) {
	for {
		select {
		case <-done:
			return
		default:
		}
		if helperStdoutClosed(stdoutWatchPoll, helperStdoutFD()) {
			select {
			case closed <- struct{}{}:
			default:
			}
			return
		}
	}
}

func helperStdoutClosed(timeout time.Duration, fd uintptr) bool {
	pollTimeoutMS := int(timeout / time.Millisecond)
	if pollTimeoutMS <= 0 {
		pollTimeoutMS = 1
	}
	pollFDs := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLOUT}}
	for {
		_, err := unix.Poll(pollFDs, pollTimeoutMS)
		if err == nil {
			break
		}
		if err == unix.EINTR {
			continue
		}
		return true
	}
	return pollFDs[0].Revents&(unix.POLLERR|unix.POLLHUP|unix.POLLNVAL) != 0
}

var startSupervisor = startSupervisorProcess
var openSupervisorDevNull = func() (*os.File, error) { return os.OpenFile(os.DevNull, os.O_RDWR, 0) } //nolint:gosec // Opening /dev/null for detached supervisor stdio.

func runDetached(payload protocol.Payload, input execInput, cwd string, env []string, waitMS int, lifetimeMS int) Response {
	taskID := *input.TaskID
	if !helperIDPattern.MatchString(taskID) {
		return errorResponse("invalid_input", "task_id has invalid shape", ExecResult{})
	}
	lockFile, err := acquireTaskRootLock()
	if err != nil {
		return helperFailureResponse()
	}
	defer func() { releaseTaskRootLock(lockFile) }()
	sweepOldTasks(time.Now())
	// Idempotent replay: if this task_id already has a task directory, return
	// its current state (running or terminal) instead of spawning a second
	// process. This is what makes a caller's RunTool replay of the same detach
	// safe at the helper layer.
	taskDir := taskDir(taskID)
	if _, err := os.Stat(taskDir); err == nil {
		if reservationIsLive(taskID, time.Now()) {
			releaseTaskRootLock(lockFile)
			lockFile = nil
			if waitForTaskStartupState(taskID, payload.Limits, 2*time.Second) {
				return replayTask(taskID, payload.Limits)
			}
			return errorResponse("task_lost", "task supervisor is not reachable", ExecResult{})
		}
		return replayTask(taskID, payload.Limits)
	}
	if liveTaskCount() >= maxLiveTasks {
		return errorResponse("task_limit", "too many live background tasks", ExecResult{})
	}
	if err := os.MkdirAll(taskDir, 0o700); err != nil {
		return helperFailureResponse()
	}
	if err := os.WriteFile(reservationPath(taskID), []byte(strconv.FormatInt(time.Now().UnixNano(), 10)), 0o600); err != nil {
		_ = os.RemoveAll(taskDir)
		return helperFailureResponse()
	}
	releaseTaskRootLock(lockFile)
	lockFile = nil
	if err := startSupervisor(taskID, input.Cmd, cwd, env, lifetimeMS); err != nil {
		_ = os.RemoveAll(taskDir)
		return errorResponse("exec_spawn_failed", "supervisor could not start", ExecResult{})
	}
	if !waitForFile(metaPath(taskID), 2*time.Second) || !waitForFile(sockPath(taskID), 2*time.Second) {
		_ = os.RemoveAll(taskDir)
		return helperFailureResponse()
	}
	_ = os.Remove(reservationPath(taskID))
	deadline := time.Now().Add(time.Duration(waitMS) * time.Millisecond)
	taskIDValue := taskID
	visibleBytes := limitVisibleBytes(payload.Limits)
	visibleLines := limitVisibleLines(payload.Limits)
	initialStdout := newInitialStreamAccumulator()
	initialStderr := newInitialStreamAccumulator()
	result := ExecResult{
		TaskID:   &taskIDValue,
		ExitCode: nil,
		Signal:   nil,
		Stdout:   emptySnapshot(taskID),
		Stderr:   emptySnapshot(taskID),
	}
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		pollMS := int(remaining / time.Millisecond)
		if pollMS > defaultPollWaitMS {
			pollMS = defaultPollWaitMS
		}
		snapshot, toolErr := controlTask(taskID, controlRequest{SchemaVersion: protocol.SchemaVersion, Op: "poll", WaitMS: pollMS, VisibleBytes: visibleBytes, VisibleLines: visibleLines}, payload.Limits)
		if toolErr != nil {
			return Response{Status: protocol.ToolStatusError, Error: toolErr, Result: result}
		}
		snapshot.TaskID = &taskIDValue
		result = snapshot
		if result.ExitCode != nil || result.Signal != nil || result.TimedOut || result.Cancelled {
			result.Stdout = initialStdout.update(result.Stdout, visibleBytes, visibleLines)
			result.Stderr = initialStderr.update(result.Stderr, visibleBytes, visibleLines)
			return Response{Status: protocol.ToolStatusSuccess, Result: result}
		}
		result.Stdout = initialStdout.update(result.Stdout, visibleBytes, visibleLines)
		result.Stderr = initialStderr.update(result.Stderr, visibleBytes, visibleLines)
	}
	return Response{Status: protocol.ToolStatusRunning, Result: result}
}

type initialStreamAccumulator struct {
	buffer     *bound.CapBuffer
	truncated  bool
	seamOffset int
	captured   int
}

func newInitialStreamAccumulator() *initialStreamAccumulator {
	return &initialStreamAccumulator{buffer: bound.NewCapBuffer(bound.DefaultStreamStoreBytes)}
}

func (a *initialStreamAccumulator) update(latest bound.StreamSnapshot, visibleBytes int, visibleLines int) bound.StreamSnapshot {
	captured := latest.Text
	if latest.Truncated {
		if head, tail, ok := splitTruncatedSnapshot(latest.Text, latest.ReturnedBytes); ok {
			captured = head + tail
			if !a.truncated {
				a.seamOffset = a.captured + len(head)
			}
		}
		a.truncated = true
	}
	_, _ = a.buffer.Write([]byte(captured))
	a.captured += len(captured)
	snapshot := initialRunningSnapshot(a.buffer, latest, visibleBytes, visibleLines)
	if !a.truncated {
		return snapshot
	}

	omitted := latest.TotalBytes - int64(snapshot.ReturnedBytes)
	if omitted < 0 {
		omitted = 0
	}
	head, tail, ok := splitTruncatedSnapshot(snapshot.Text, snapshot.ReturnedBytes)
	if !ok {
		seam := min(a.seamOffset, len(snapshot.Text))
		head, tail = snapshot.Text[:seam], snapshot.Text[seam:]
	}
	snapshot.Text = head + truncationMarker(omitted) + tail
	snapshot.Truncated = true
	return snapshot
}

func splitTruncatedSnapshot(text string, returnedBytes int) (string, string, bool) {
	markerBytes := len(text) - returnedBytes
	if markerBytes <= 0 {
		return "", "", false
	}
	const prefix = "\n[... "
	const suffix = " bytes truncated ...]\n"
	for start := 0; start+markerBytes <= len(text); {
		relative := strings.Index(text[start:], prefix)
		if relative < 0 {
			break
		}
		start += relative
		end := start + markerBytes
		marker := text[start:end]
		countText, hasPrefix := strings.CutPrefix(marker, prefix)
		countText, hasSuffix := strings.CutSuffix(countText, suffix)
		if hasPrefix && hasSuffix {
			if count, err := strconv.ParseInt(countText, 10, 64); err == nil && count >= 0 {
				return text[:start], text[end:], true
			}
		}
		start += len(prefix)
	}
	return "", "", false
}

func truncationMarker(omitted int64) string {
	return "\n[... " + strconv.FormatInt(omitted, 10) + " bytes truncated ...]\n"
}

func initialRunningSnapshot(buffer *bound.CapBuffer, latest bound.StreamSnapshot, visibleBytes int, visibleLines int) bound.StreamSnapshot {
	snapshot := buffer.Snapshot(visibleBytes, visibleLines)
	snapshot.TotalBytes = latest.TotalBytes
	snapshot.TotalLines = latest.TotalLines
	snapshot.Truncated = snapshot.Truncated || latest.Truncated
	return snapshot
}

func startSupervisorProcess(taskID string, cmdText string, cwd string, env []string, lifetimeMS int) error {
	encodedEnv, err := json.Marshal(env)
	if err != nil {
		return err
	}
	args := []string{"__supervise-bootstrap", "--task-id", taskID, "--cmd", cmdText, "--cwd", cwd, "--env-json", string(encodedEnv), "--lifetime-ms", strconv.Itoa(lifetimeMS)}
	supervisor := exec.Command(os.Args[0], args...) //nolint:gosec // Re-execing this helper binary with validated task args starts the bounded supervisor.
	supervisor.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	devNull, err := openSupervisorDevNull()
	if err != nil {
		return err
	}
	defer func() { _ = devNull.Close() }()
	cleanupAuth, err := attachSupervisorEntrypointAuth(supervisor)
	if err != nil {
		return err
	}
	defer cleanupAuth()
	supervisor.Stdin = devNull
	supervisor.Stdout = devNull
	supervisor.Stderr = devNull
	if err := supervisor.Start(); err != nil {
		return err
	}
	return supervisor.Process.Release()
}

func exitForSignal(sig os.Signal) {
	if value, ok := sig.(syscall.Signal); ok {
		os.Exit(128 + int(value))
	}
	os.Exit(1)
}

func terminalResponse(state *os.ProcessState, waitErr error, timedOut bool, stdout *bound.CapBuffer, stderr *bound.CapBuffer, startedAt time.Time, limits protocol.Limits) Response {
	result := execResult(state, waitErr, timedOut, stdout, stderr, startedAt, limits)
	return Response{Status: protocol.ToolStatusSuccess, Result: result}
}

func execResult(state *os.ProcessState, waitErr error, timedOut bool, stdout *bound.CapBuffer, stderr *bound.CapBuffer, startedAt time.Time, limits protocol.Limits) ExecResult {
	exitCode, signal := exitFacts(state, waitErr)
	return ExecResult{
		TaskID:     nil,
		ExitCode:   exitCode,
		Signal:     signal,
		TimedOut:   timedOut,
		Stdout:     stdout.Snapshot(limitVisibleBytes(limits), limitVisibleLines(limits)),
		Stderr:     stderr.Snapshot(limitVisibleBytes(limits), limitVisibleLines(limits)),
		DurationMS: time.Since(startedAt).Milliseconds(),
	}
}

func exitFacts(state *os.ProcessState, waitErr error) (*int, *string) {
	if state == nil {
		if waitErr != nil {
			value := "unknown"
			return nil, &value
		}
		return nil, nil
	}
	status, ok := state.Sys().(syscall.WaitStatus)
	if !ok {
		code := state.ExitCode()
		return &code, nil
	}
	if status.Signaled() {
		value := status.Signal().String()
		return nil, &value
	}
	code := status.ExitStatus()
	return &code, nil
}

func copyStream(wg *sync.WaitGroup, buffer *bound.CapBuffer, reader io.Reader) {
	defer wg.Done()
	chunk := make([]byte, 8*1024)
	for {
		n, err := reader.Read(chunk)
		if n > 0 {
			_, _ = buffer.Write(chunk[:n])
		}
		if err != nil {
			return
		}
	}
}

func terminateProcessGroup(pid int) {
	if pid > 0 {
		_ = syscall.Kill(-pid, syscall.SIGTERM)
	}
}

func killProcessGroup(pid int) {
	if pid > 0 {
		_ = syscall.Kill(-pid, syscall.SIGKILL)
	}
}

func waitForCopies(wg *sync.WaitGroup, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
	}
}

func execEnv(input map[string]any) ([]string, *protocol.ToolError) {
	values := map[string]string{}
	for _, entry := range os.Environ() {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			values[key] = value
		}
	}
	for key, raw := range input {
		value, ok := raw.(string)
		if !ok {
			return nil, &protocol.ToolError{Kind: "invalid_input", Message: "env values must be strings"}
		}
		values[key] = value
	}
	values["TERM"] = "dumb"
	values["NO_COLOR"] = "1"
	values["PAGER"] = "cat"
	values["GIT_PAGER"] = "cat"
	out := make([]string, 0, len(values))
	for key, value := range values {
		out = append(out, key+"="+value)
	}
	return out, nil
}

func errorResponse(kind string, message string, result ExecResult) Response {
	return Response{Status: protocol.ToolStatusError, Error: &protocol.ToolError{Kind: kind, Message: message}, Result: result}
}

func helperFailureResponse() Response {
	return Response{HelperFailure: true}
}

func defaultInt(value *int, fallback int) int {
	if value == nil {
		return fallback
	}
	return *value
}

func clampInt(value int, minValue int, maxValue int) int {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

func limitVisibleBytes(limits protocol.Limits) int {
	if limits.VisibleBytes > 0 && limits.VisibleBytes < 50*1024 {
		return limits.VisibleBytes
	}
	return 50 * 1024
}

func limitVisibleLines(limits protocol.Limits) int {
	if limits.VisibleLines > 0 && limits.VisibleLines < 2000 {
		return limits.VisibleLines
	}
	return 2000
}
