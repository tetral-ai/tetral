package task

import (
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net"
	"os"
	"os/exec"
	"regexp"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/tetral-ai/tetral/internal/sandbox/helper/internal/bound"
	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
)

// taskReservationMaxAge (2 min) bounds how long a startup reservation is
// trusted without a live control socket. It guards the window where a launcher
// dies after the supervisor has published control.sock but before removing the
// reservation file: a reservation older than this is swept only when its socket
// is not live, so a live supervisor's task is never orphaned. The window it
// covers is explained where it is enforced, at reservationIsLive in store.go.
const taskReservationMaxAge = 2 * time.Minute
const stdinWriteDeadline = 250 * time.Millisecond

var (
	runtimeRoot   = "/tmp/tetral-runtime"
	runtimeRootMu sync.RWMutex
)

var helperIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// controlRequest / controlResponse are the control.sock wire protocol between
// an invoking helper and a live supervisor: one connection per operation, one
// JSON document each way, then close, versioned by schema_version. This
// protocol is helper-internal — the Sandbox adapter never sees it. The invoking
// helper (controlTask) maps a controlResponse back onto the standard result
// envelope, so the socket shape and the adapter-facing envelope are independent.
type controlRequest struct {
	SchemaVersion int    `json:"schema_version"`
	Op            string `json:"op"`
	Chars         string `json:"chars,omitempty"`
	WaitMS        int    `json:"wait_ms,omitempty"`
	WriteSeq      *int64 `json:"write_seq,omitempty"`
	VisibleBytes  int    `json:"visible_bytes,omitempty"`
	VisibleLines  int    `json:"visible_lines,omitempty"`
}

type controlResponse struct {
	State        string               `json:"state,omitempty"`
	ExitCode     *int                 `json:"exit_code"`
	Signal       *string              `json:"signal"`
	TimedOut     bool                 `json:"timed_out"`
	Cancelled    bool                 `json:"cancelled"`
	WasRunning   *bool                `json:"was_running,omitempty"`
	WrittenBytes int                  `json:"written_bytes,omitempty"`
	Stdout       bound.StreamSnapshot `json:"stdout"`
	Stderr       bound.StreamSnapshot `json:"stderr"`
	DurationMS   int64                `json:"duration_ms"`
	Error        *protocol.ToolError  `json:"error,omitempty"`
}

type exitRecord struct {
	ExitCode   *int                 `json:"exit_code"`
	Signal     *string              `json:"signal"`
	TimedOut   bool                 `json:"timed_out"`
	Cancelled  bool                 `json:"cancelled"`
	StartedAt  string               `json:"started_at"`
	EndedAt    string               `json:"ended_at"`
	Stdout     bound.StreamSnapshot `json:"stdout"`
	Stderr     bound.StreamSnapshot `json:"stderr"`
	DurationMS int64                `json:"duration_ms"`
}

type metaRecord struct {
	TaskID             string `json:"task_id"`
	ChildPGID          int    `json:"child_pgid"`
	Cmd                string `json:"cmd"`
	StartedAt          string `json:"started_at"`
	LifetimeDeadlineAt string `json:"lifetime_deadline_at"`
	LastWriteSeq       int64  `json:"last_write_seq"`
}

type supervisorArgs struct {
	taskID     string
	cmdText    string
	cwd        string
	env        []string
	lifetimeMS int
}

type supervisorState struct {
	mu                sync.Mutex
	cond              *sync.Cond
	streamWG          sync.WaitGroup
	forceCloseStreams func()
	taskID            string
	cmdText           string
	startedAt         time.Time

	stdin io.WriteCloser
	pgid  int

	stdoutFull    *bound.CapBuffer
	stderrFull    *bound.CapBuffer
	stdoutPending *bound.CapBuffer
	stderrPending *bound.CapBuffer
	stdoutMirror  *streamMirror
	stderrMirror  *streamMirror

	terminal     bool
	exitCode     *int
	signal       *string
	timedOut     bool
	cancelled    bool
	endedAt      time.Time
	lastWriteSeq int64
	persistErr   *protocol.ToolError
}

var (
	listenSupervisorControl  = net.Listen
	writeSupervisorMeta      = writeJSONAtomic
	supervisorProcessStarted = func(int) {}
	encodeControlRequest     = func(conn net.Conn, request controlRequest) error {
		return json.NewEncoder(conn).Encode(request)
	}
)

func newSupervisorState(taskID string, cmdText string) *supervisorState {
	state := &supervisorState{
		forceCloseStreams: func() {},
		taskID:            taskID,
		cmdText:           cmdText,
		startedAt:         time.Now(),
		stdoutFull:        bound.NewCapBuffer(bound.DefaultStreamStoreBytes),
		stderrFull:        bound.NewCapBuffer(bound.DefaultStreamStoreBytes),
		stdoutPending:     bound.NewCapBuffer(bound.DefaultStreamStoreBytes),
		stderrPending:     bound.NewCapBuffer(bound.DefaultStreamStoreBytes),
		stdoutMirror:      newStreamMirror(stdoutLogPath(taskID)),
		stderrMirror:      newStreamMirror(stderrLogPath(taskID)),
	}
	state.cond = sync.NewCond(&state.mu)
	return state
}

// RunSupervisorBootstrap is the middle stage of the detached-supervisor
// double-fork: the invoking helper spawns __supervise-bootstrap (setsid, stdio
// to /dev/null), which re-execs __supervise (setsid, stdio to /dev/null) into
// runSupervisor, which chdirs to /. The stdio redirect at each stage is
// load-bearing, not hygiene: the invoking helper's own stdout is the
// transport response pipe, and any write-end inherited by the detached tree
// would hold that response's drain open (an observed multi-second stall) and
// stall every detach. A supervisor that skips the redirect breaks detach.
func RunSupervisorBootstrap(args []string) int {
	if _, exitCode := parseSupervisorArgs(args); exitCode != 0 {
		return exitCode
	}
	supervisor := exec.Command(os.Args[0], append([]string{"__supervise"}, args...)...) //nolint:gosec // Re-execing this helper binary with validated supervisor args is the task supervisor contract.
	supervisor.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	devNull, err := openSupervisorDevNull()
	if err != nil {
		return 1
	}
	defer func() { _ = devNull.Close() }()
	cleanupAuth, err := attachSupervisorEntrypointAuth(supervisor)
	if err != nil {
		return 1
	}
	defer cleanupAuth()
	supervisor.Stdin = devNull
	supervisor.Stdout = devNull
	supervisor.Stderr = devNull
	if err := supervisor.Start(); err != nil {
		return 1
	}
	if err := supervisor.Process.Release(); err != nil {
		return 1
	}
	return 0
}

func RunSupervisor(args []string) int {
	parsed, exitCode := parseSupervisorArgs(args)
	if exitCode != 0 {
		return exitCode
	}
	return runSupervisor(parsed.taskID, parsed.cmdText, parsed.cwd, parsed.env, time.Duration(parsed.lifetimeMS)*time.Millisecond)
}

func parseSupervisorArgs(args []string) (supervisorArgs, int) {
	fs := flag.NewFlagSet("__supervise", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	taskID := fs.String("task-id", "", "")
	cmdText := fs.String("cmd", "", "")
	cwd := fs.String("cwd", "", "")
	envJSON := fs.String("env-json", "[]", "")
	lifetimeMS := fs.Int("lifetime-ms", defaultTaskLifetimeMS, "")
	if err := fs.Parse(args); err != nil {
		return supervisorArgs{}, 2
	}
	if *taskID == "" || *cmdText == "" || *cwd == "" {
		return supervisorArgs{}, 2
	}
	if !helperIDPattern.MatchString(*taskID) {
		return supervisorArgs{}, 2
	}
	if *lifetimeMS <= 0 || *lifetimeMS > maxTaskLifetimeMS {
		return supervisorArgs{}, 2
	}
	var env []string
	if err := json.Unmarshal([]byte(*envJSON), &env); err != nil {
		return supervisorArgs{}, 2
	}
	return supervisorArgs{taskID: *taskID, cmdText: *cmdText, cwd: *cwd, env: env, lifetimeMS: *lifetimeMS}, 0
}

func runSupervisor(taskID string, cmdText string, cwd string, env []string, lifetime time.Duration) int {
	_ = os.Chdir("/")
	taskDir := taskDir(taskID)
	if err := os.MkdirAll(taskDir, 0o700); err != nil {
		return 1
	}
	state := newSupervisorState(taskID, cmdText)
	if err := state.stdoutMirror.ensure(); err != nil {
		return 1
	}
	if err := state.stderrMirror.ensure(); err != nil {
		return 1
	}
	shell := "/bin/bash"
	if _, err := os.Stat(shell); err != nil {
		shell = "/bin/sh"
	}
	cmd := exec.Command(shell, "-c", cmdText) //nolint:gosec // The exec task intentionally runs caller-supplied commands inside the sandbox helper boundary.
	cmd.Dir = cwd
	cmd.Env = env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return 1
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return 1
	}
	state.forceCloseStreams = sync.OnceFunc(func() {
		_ = stdoutPipe.Close()
		_ = stderrPipe.Close()
	})
	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return 1
	}
	if file, ok := stdinPipe.(*os.File); ok {
		_ = unix.SetNonblock(int(file.Fd()), true)
	}
	if err := cmd.Start(); err != nil {
		return 1
	}
	supervisorProcessStarted(cmd.Process.Pid)
	state.stdin = stdinPipe
	state.pgid = cmd.Process.Pid
	setupComplete := false
	defer func() {
		if !setupComplete {
			closeStartedSupervisorChild(cmd, stdinPipe, stdoutPipe, stderrPipe)
		}
	}()
	listener, err := listenSupervisorControl("unix", sockPath(taskID))
	if err != nil {
		return 1
	}
	defer func() { _ = listener.Close() }()
	if err := os.Chmod(sockPath(taskID), 0o600); err != nil {
		return 1
	}
	if err := writeSupervisorMeta(metaPath(taskID), metaRecord{
		TaskID:             taskID,
		ChildPGID:          state.pgid,
		Cmd:                cmdText,
		StartedAt:          state.startedAt.UTC().Format(time.RFC3339Nano),
		LifetimeDeadlineAt: state.startedAt.Add(lifetime).UTC().Format(time.RFC3339Nano),
	}); err != nil {
		return 1
	}
	setupComplete = true
	state.streamWG.Add(2)
	go state.copyStream(stdoutPipe, true)
	go state.copyStream(stderrPipe, false)
	go func() {
		state.streamWG.Wait()
		err := cmd.Wait()
		state.setTerminal(cmd.ProcessState, err, false, false)
	}()
	if lifetime > 0 {
		go func() {
			timer := time.NewTimer(lifetime)
			defer timer.Stop()
			<-timer.C
			state.mu.Lock()
			terminal := state.terminal
			state.mu.Unlock()
			if !terminal {
				state.mu.Lock()
				state.timedOut = true
				state.mu.Unlock()
				terminateProcessGroup(state.pgid)
				time.Sleep(killGrace)
				killProcessGroup(state.pgid)
				state.forceCloseStreams()
			}
		}()
	}
	for {
		state.mu.Lock()
		terminal := state.terminal
		state.mu.Unlock()
		if terminal {
			// Shutdown ordering is load-bearing: setTerminal sets terminal
			// only after exit.json is durably written, so the socket is never
			// unlinked before the terminal record exists. A client that finds
			// the socket gone must re-check exit.json before concluding
			// task_lost (see controlTask).
			_ = os.Remove(sockPath(taskID))
			return 0
		}
		if _, err := os.Stat(taskDir); errors.Is(err, os.ErrNotExist) {
			return 0
		}
		if unix, ok := listener.(*net.UnixListener); ok {
			_ = unix.SetDeadline(time.Now().Add(200 * time.Millisecond))
		}
		conn, err := listener.Accept()
		if err != nil {
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				continue
			}
			return 0
		}
		handleControlConn(conn, state)
	}
}

func closeStartedSupervisorChild(cmd *exec.Cmd, stdin io.WriteCloser, stdout io.Reader, stderr io.Reader) {
	_ = stdin.Close()
	var drainWG sync.WaitGroup
	drainWG.Add(2)
	go func() { defer drainWG.Done(); _, _ = io.Copy(io.Discard, stdout) }()
	go func() { defer drainWG.Done(); _, _ = io.Copy(io.Discard, stderr) }()
	waitDone := make(chan struct{})
	go func() { _ = cmd.Wait(); close(waitDone) }()
	terminateProcessGroup(cmd.Process.Pid)
	select {
	case <-waitDone:
	case <-time.After(killGrace):
		killProcessGroup(cmd.Process.Pid)
		select {
		case <-waitDone:
		case <-time.After(killGrace):
		}
	}
	waitForCopies(&drainWG, killGrace)
}

func (s *supervisorState) copyStream(reader io.Reader, stdout bool) {
	defer s.streamWG.Done()
	chunk := make([]byte, 8*1024)
	for {
		n, err := reader.Read(chunk)
		if n > 0 {
			s.mu.Lock()
			if stdout {
				_, _ = s.stdoutFull.Write(chunk[:n])
				_, _ = s.stdoutPending.Write(chunk[:n])
				s.recordPersistErrorLocked(s.stdoutMirror.appendProduced(chunk[:n]))
			} else {
				_, _ = s.stderrFull.Write(chunk[:n])
				_, _ = s.stderrPending.Write(chunk[:n])
				s.recordPersistErrorLocked(s.stderrMirror.appendProduced(chunk[:n]))
			}
			s.cond.Broadcast()
			s.mu.Unlock()
		}
		if err != nil {
			return
		}
	}
}

func (s *supervisorState) setTerminal(state *os.ProcessState, waitErr error, timedOut bool, cancelled bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if timedOut {
		s.timedOut = true
	}
	if cancelled {
		s.cancelled = true
	}
	s.exitCode, s.signal = exitFacts(state, waitErr)
	s.endedAt = time.Now()
	record := s.exitRecordLocked()
	stdoutLog := storedCaptureText(s.stdoutFull)
	stderrLog := storedCaptureText(s.stderrFull)
	if err := s.stdoutMirror.finalize(stdoutLog); err != nil {
		s.recordPersistErrorLocked(err)
	}
	if err := s.stderrMirror.finalize(stderrLog); err != nil {
		s.recordPersistErrorLocked(err)
	}
	if err := writeTerminalJSONAtomic(exitPath(s.taskID), record); err != nil {
		s.recordPersistErrorLocked(err)
	}
	if s.persistErr != nil {
		s.cond.Broadcast()
		return
	}
	s.terminal = true
	s.cond.Broadcast()
}

func (s *supervisorState) recordPersistErrorLocked(err error) {
	if err != nil && s.persistErr == nil {
		s.persistErr = &protocol.ToolError{Kind: "task_lost", Message: "task terminal state could not be persisted"}
	}
}

func (s *supervisorState) exitRecordLocked() exitRecord {
	return exitRecord{
		ExitCode:   s.exitCode,
		Signal:     s.signal,
		TimedOut:   s.timedOut,
		Cancelled:  s.cancelled,
		StartedAt:  s.startedAt.UTC().Format(time.RFC3339Nano),
		EndedAt:    s.endedAt.UTC().Format(time.RFC3339Nano),
		Stdout:     s.stdoutPending.Snapshot(50*1024, 2000),
		Stderr:     s.stderrPending.Snapshot(50*1024, 2000),
		DurationMS: s.endedAt.Sub(s.startedAt).Milliseconds(),
	}
}

func storedCaptureText(buffer *bound.CapBuffer) string {
	return buffer.Snapshot(bound.DefaultStreamStoreBytes, int(^uint(0)>>1)).Text
}

func handleControlConn(conn net.Conn, state *supervisorState) {
	defer func() { _ = conn.Close() }()
	var request controlRequest
	if err := json.NewDecoder(io.LimitReader(conn, 64*1024)).Decode(&request); err != nil {
		_ = json.NewEncoder(conn).Encode(controlResponse{Error: &protocol.ToolError{Kind: "invalid_input", Message: "control request is malformed"}})
		return
	}
	if request.SchemaVersion != protocol.SchemaVersion {
		_ = json.NewEncoder(conn).Encode(controlResponse{Error: &protocol.ToolError{Kind: "invalid_input", Message: "unsupported control schema_version"}})
		return
	}
	result, toolErr := state.handleControl(request)
	_ = json.NewEncoder(conn).Encode(controlResponseFromResult(result, toolErr))
}

func (s *supervisorState) handleControl(request controlRequest) (ExecResult, *protocol.ToolError) {
	if toolErr := s.persistFailure(); toolErr != nil {
		return ExecResult{}, toolErr
	}
	switch request.Op {
	case "poll":
		return s.handlePoll(request), nil
	case "stdin":
		return s.handleStdin(request)
	case "cancel":
		return s.handleCancel(request)
	default:
		return ExecResult{}, &protocol.ToolError{Kind: "invalid_input", Message: "unsupported control op"}
	}
}

func (s *supervisorState) persistFailure() *protocol.ToolError {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.persistErr
}

func controlResponseFromResult(result ExecResult, toolErr *protocol.ToolError) controlResponse {
	state := "running"
	if isTerminalResult(result) {
		state = "terminal"
	}
	return controlResponse{
		State:        state,
		ExitCode:     result.ExitCode,
		Signal:       result.Signal,
		TimedOut:     result.TimedOut,
		Cancelled:    result.Cancelled,
		WasRunning:   result.WasRunning,
		WrittenBytes: result.WrittenBytes,
		Stdout:       result.Stdout,
		Stderr:       result.Stderr,
		DurationMS:   result.DurationMS,
		Error:        toolErr,
	}
}

func resultFromControlResponse(taskID string, response controlResponse) ExecResult {
	return ExecResult{
		TaskID:       &taskID,
		ExitCode:     response.ExitCode,
		Signal:       response.Signal,
		TimedOut:     response.TimedOut,
		Cancelled:    response.Cancelled,
		WasRunning:   response.WasRunning,
		WrittenBytes: response.WrittenBytes,
		Stdout:       response.Stdout,
		Stderr:       response.Stderr,
		DurationMS:   response.DurationMS,
	}
}

func controlVisibleLimits(request controlRequest) (int, int) {
	visibleBytes := request.VisibleBytes
	if visibleBytes <= 0 || visibleBytes > 50*1024 {
		visibleBytes = 50 * 1024
	}
	visibleLines := request.VisibleLines
	if visibleLines <= 0 || visibleLines > 2000 {
		visibleLines = 2000
	}
	return visibleBytes, visibleLines
}

func (s *supervisorState) handlePoll(request controlRequest) ExecResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	visibleBytes, visibleLines := controlVisibleLimits(request)
	s.waitForPollOutputLocked(time.Duration(request.WaitMS) * time.Millisecond)
	if s.terminal {
		return s.terminalResultLocked(nil, visibleBytes, visibleLines)
	}
	return s.runningResultLocked(s.stdoutPending.DrainSnapshot(visibleBytes, visibleLines), s.stderrPending.DrainSnapshot(visibleBytes, visibleLines), 0)
}

// handleStdin applies one stdin write, with two rules that make replays safe
// and Ctrl-C work:
//
//   - write_seq is a monotone per-task sequence recorded in meta.json. A call
//     whose write_seq is <= the last applied is dropped (its output is still
//     drained and returned), so a lost-response replay of the same write never
//     double-writes to the child. A strictly greater write_seq is a new write.
//   - The chars value exactly equal to the one-character ETX string (U+0003) is not written to stdin;
//     it sends SIGINT to the task's process group, giving Codex-family models
//     Ctrl-C interrupt semantics. Any other value — including one that merely
//     contains ETX among other characters — is written verbatim.
func (s *supervisorState) handleStdin(request controlRequest) (ExecResult, *protocol.ToolError) {
	if len([]byte(request.Chars)) > maxStdinBytes {
		return ExecResult{}, &protocol.ToolError{Kind: "invalid_input", Message: "chars exceeds 32 KiB"}
	}
	s.mu.Lock()
	visibleBytes, visibleLines := controlVisibleLimits(request)
	if s.terminal {
		result := s.terminalResultLocked(nil, visibleBytes, visibleLines)
		s.mu.Unlock()
		return result, &protocol.ToolError{Kind: "task_exited", Message: "task already exited", Detail: map[string]any{"terminal": result}}
	}
	if request.WriteSeq != nil && *request.WriteSeq <= s.lastWriteSeq {
		s.waitForOutputLocked(time.Duration(request.WaitMS) * time.Millisecond)
		result := s.runningResultLocked(s.stdoutPending.DrainSnapshot(visibleBytes, visibleLines), s.stderrPending.DrainSnapshot(visibleBytes, visibleLines), 0)
		s.mu.Unlock()
		return result, nil
	}
	stdin := s.stdin
	pgid := s.pgid
	s.mu.Unlock()
	written := 0
	if request.Chars == "\u0003" {
		_ = syscall.Kill(-pgid, syscall.SIGINT)
	} else {
		n, err := writeTaskStdin(stdin, request.Chars, stdinWriteDeadline)
		written = n
		if err != nil {
			_ = stdin.Close()
			s.mu.Lock()
			if written > 0 && request.WriteSeq != nil && *request.WriteSeq > s.lastWriteSeq {
				s.lastWriteSeq = *request.WriteSeq
				_ = updateMetaWriteSeq(s.taskID, s.lastWriteSeq)
			}
			result := s.runningResultLocked(s.stdoutPending.DrainSnapshot(visibleBytes, visibleLines), s.stderrPending.DrainSnapshot(visibleBytes, visibleLines), written)
			s.mu.Unlock()
			return result, &protocol.ToolError{Kind: "stdin_closed", Message: "task stdin is closed or not accepting input"}
		}
	}
	s.mu.Lock()
	if request.WriteSeq != nil && *request.WriteSeq > s.lastWriteSeq {
		s.lastWriteSeq = *request.WriteSeq
		_ = updateMetaWriteSeq(s.taskID, s.lastWriteSeq)
	}
	s.waitForOutputLocked(time.Duration(request.WaitMS) * time.Millisecond)
	if s.terminal {
		result := s.terminalResultLocked(&written, visibleBytes, visibleLines)
		s.mu.Unlock()
		return result, nil
	}
	result := s.runningResultLocked(s.stdoutPending.DrainSnapshot(visibleBytes, visibleLines), s.stderrPending.DrainSnapshot(visibleBytes, visibleLines), written)
	s.mu.Unlock()
	return result, nil
}

func writeTaskStdin(stdin io.WriteCloser, chars string, timeout time.Duration) (int, error) {
	if chars == "" {
		return 0, nil
	}
	if file, ok := stdin.(*os.File); ok {
		return writeTaskStdinFile(file, []byte(chars), timeout)
	}
	type writeResult struct {
		n   int
		err error
	}
	done := make(chan writeResult, 1)
	go func() {
		n, err := io.WriteString(stdin, chars)
		done <- writeResult{n: n, err: err}
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case result := <-done:
		return result.n, result.err
	case <-timer.C:
		_ = stdin.Close()
		return 0, os.ErrDeadlineExceeded
	}
}

func writeTaskStdinFile(file *os.File, body []byte, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	written := 0
	fd := int(file.Fd())
	_ = unix.SetNonblock(fd, true)
	for written < len(body) {
		n, err := unix.Write(fd, body[written:])
		if n > 0 {
			written += n
			continue
		}
		if err == nil {
			continue
		}
		if err == unix.EINTR {
			continue
		}
		if err != unix.EAGAIN && err != unix.EWOULDBLOCK {
			return written, err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return written, os.ErrDeadlineExceeded
		}
		pollMS := int(remaining / time.Millisecond)
		if pollMS < 1 {
			pollMS = 1
		}
		if pollMS > 25 {
			pollMS = 25
		}
		_, pollErr := unix.Poll([]unix.PollFd{{Fd: int32(fd), Events: unix.POLLOUT}}, pollMS)
		if pollErr == unix.EINTR {
			continue
		}
		if pollErr != nil {
			return written, pollErr
		}
	}
	return written, nil
}

func (s *supervisorState) handleCancel(request controlRequest) (ExecResult, *protocol.ToolError) {
	s.mu.Lock()
	visibleBytes, visibleLines := controlVisibleLimits(request)
	if s.terminal {
		wasRunning := false
		result := s.terminalResultLocked(nil, visibleBytes, visibleLines)
		result.WasRunning = &wasRunning
		s.mu.Unlock()
		return result, nil
	}
	s.cancelled = true
	pgid := s.pgid
	s.mu.Unlock()
	terminateProcessGroup(pgid)
	s.mu.Lock()
	if !s.waitForTerminalLocked(killGrace) {
		if s.persistErr != nil {
			toolErr := s.persistErr
			s.mu.Unlock()
			return ExecResult{}, toolErr
		}
		s.mu.Unlock()
		killProcessGroup(pgid)
		s.forceCloseStreams()
		s.mu.Lock()
		if !s.waitForTerminalLocked(killGrace) {
			if s.persistErr != nil {
				toolErr := s.persistErr
				s.mu.Unlock()
				return ExecResult{}, toolErr
			}
			s.mu.Unlock()
			return ExecResult{}, &protocol.ToolError{Kind: "task_lost", Message: "task did not report terminal state after cancellation"}
		}
	}
	wasRunning := true
	result := s.terminalResultLocked(nil, visibleBytes, visibleLines)
	result.WasRunning = &wasRunning
	s.mu.Unlock()
	return result, nil
}

func (s *supervisorState) waitForTerminalLocked(timeout time.Duration) bool {
	if timeout <= 0 || s.terminal {
		return s.terminal
	}
	deadline := time.Now().Add(timeout)
	timer := time.AfterFunc(timeout, func() {
		s.mu.Lock()
		s.cond.Broadcast()
		s.mu.Unlock()
	})
	defer timer.Stop()
	for !s.terminal && s.persistErr == nil && time.Now().Before(deadline) {
		s.cond.Wait()
	}
	return s.terminal
}

func (s *supervisorState) waitForOutputLocked(timeout time.Duration) {
	if timeout <= 0 || s.terminal {
		return
	}
	deadline := time.Now().Add(timeout)
	timer := time.AfterFunc(timeout, func() {
		s.mu.Lock()
		s.cond.Broadcast()
		s.mu.Unlock()
	})
	defer timer.Stop()
	for !s.terminal && time.Now().Before(deadline) {
		s.cond.Wait()
	}
}

func (s *supervisorState) waitForPollOutputLocked(timeout time.Duration) {
	if timeout <= 0 || s.terminal || s.stdoutPending.WindowBytes() > 0 || s.stderrPending.WindowBytes() > 0 {
		return
	}
	stdoutBytes := s.stdoutPending.WindowBytes()
	stderrBytes := s.stderrPending.WindowBytes()
	deadline := time.Now().Add(timeout)
	timer := time.AfterFunc(timeout, func() {
		s.mu.Lock()
		s.cond.Broadcast()
		s.mu.Unlock()
	})
	defer timer.Stop()
	for !s.terminal &&
		s.stdoutPending.WindowBytes() == stdoutBytes &&
		s.stderrPending.WindowBytes() == stderrBytes &&
		time.Now().Before(deadline) {
		s.cond.Wait()
	}
}

func (s *supervisorState) runningResultLocked(stdout bound.StreamSnapshot, stderr bound.StreamSnapshot, written int) ExecResult {
	taskID := s.taskID
	return ExecResult{
		TaskID:       &taskID,
		ExitCode:     nil,
		Signal:       nil,
		TimedOut:     false,
		Stdout:       stdout,
		Stderr:       stderr,
		WrittenBytes: written,
		DurationMS:   time.Since(s.startedAt).Milliseconds(),
	}
}

func (s *supervisorState) terminalResultLocked(written *int, visibleBytes int, visibleLines int) ExecResult {
	taskID := s.taskID
	value := ExecResult{
		TaskID:     &taskID,
		ExitCode:   s.exitCode,
		Signal:     s.signal,
		TimedOut:   s.timedOut,
		Cancelled:  s.cancelled,
		Stdout:     s.stdoutPending.Snapshot(visibleBytes, visibleLines),
		Stderr:     s.stderrPending.Snapshot(visibleBytes, visibleLines),
		DurationMS: s.endedAt.Sub(s.startedAt).Milliseconds(),
	}
	if written != nil {
		value.WrittenBytes = *written
	}
	return value
}

// controlTask is the invoking-helper side of stdin/poll/cancel. It discovers
// the task's state from four observations of the task directory and socket:
//
//	observation                       | state    | result
//	----------------------------------|----------|--------------------------
//	no task directory                 | unknown  | task_not_found
//	control.sock live (dial succeeds) | running  | serve the op over the sock
//	sock dead, exit.json present      | terminal | serve the terminal state
//	sock dead, no exit.json           | died     | task_lost
//
// Because exit.json is written before control.sock is unlinked, a connection
// that dies at any step after a successful dial re-checks exit.json (via
// readExitResultEventually) before concluding task_lost; the race resolves to
// the terminal state when the record is present.
//
// A terminal poll never consumes exit.json, so repeated polls return the
// identical terminal envelope. That terminal reply is the fact source the
// caller maps into its background-task settlement: totals become original_*,
// timed_out becomes expired, and cancelled stays cancelled.
func controlTask(taskID string, request controlRequest, limits protocol.Limits) (ExecResult, *protocol.ToolError) {
	if !helperIDPattern.MatchString(taskID) {
		return ExecResult{}, &protocol.ToolError{Kind: "invalid_input", Message: "task_id has invalid shape"}
	}
	sweepOldTasks(time.Now())
	if _, err := os.Stat(taskDir(taskID)); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ExecResult{}, &protocol.ToolError{Kind: "task_not_found", Message: "task was not found"}
		}
		return ExecResult{}, &protocol.ToolError{Kind: "task_lost", Message: "task directory could not be inspected"}
	}
	if terminal, ok := readExitResult(taskID, limits); ok {
		return terminalResultForOp(request.Op, terminal)
	}
	conn, err := net.DialTimeout("unix", sockPath(taskID), time.Second)
	if err != nil {
		if terminal, ok := readExitResultEventually(taskID, limits, 250*time.Millisecond); ok {
			return terminalResultForOp(request.Op, terminal)
		}
		return ExecResult{}, &protocol.ToolError{Kind: "task_lost", Message: "task supervisor is not reachable"}
	}
	defer func() { _ = conn.Close() }()
	if err := encodeControlRequest(conn, request); err != nil {
		if terminal, ok := readExitResultEventually(taskID, limits, 250*time.Millisecond); ok {
			return terminalResultForOp(request.Op, terminal)
		}
		return ExecResult{}, &protocol.ToolError{Kind: "task_lost", Message: "task control request failed"}
	}
	var response controlResponse
	if err := json.NewDecoder(conn).Decode(&response); err != nil {
		if terminal, ok := readExitResultEventually(taskID, limits, 250*time.Millisecond); ok {
			return terminalResultForOp(request.Op, terminal)
		}
		return ExecResult{}, &protocol.ToolError{Kind: "task_lost", Message: "task control response failed"}
	}
	result := resultFromControlResponse(taskID, response)
	if response.Error != nil {
		return result, response.Error
	}
	return result, nil
}

func terminalResultForOp(op string, result ExecResult) (ExecResult, *protocol.ToolError) {
	if op == "stdin" {
		return result, &protocol.ToolError{Kind: "task_exited", Message: "task already exited", Detail: map[string]any{"terminal": result}}
	}
	return result, nil
}
