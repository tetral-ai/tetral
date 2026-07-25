// This file owns the helper task store's on-disk paths, persistence, replay,
// and reservation lock; daemon control remains in supervisor.go.
package task

import (
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/tetral-ai/tetral/internal/sandbox/helper/internal/bound"
	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
)

var (
	appendTerminalLog       = appendFile
	writeTerminalJSONAtomic = writeJSONAtomic
)

type streamMirror struct {
	path        string
	headLimit   int64
	headWritten int64
}

// newStreamMirror sets headLimit to half the stored stream cap (512 KiB =
// DefaultStreamStoreBytes/2). The mirror appends the head half live as the
// stream is produced, and appends the seam marker plus the in-memory tail half
// at EOF or bounded escape (finalize). So stdout.log / stderr.log end holding
// the same head+tail capture as the in-memory ring: at most 1 MiB plus the
// marker per stream.
func newStreamMirror(path string) *streamMirror {
	return &streamMirror{path: path, headLimit: int64(bound.DefaultStreamStoreBytes / 2)}
}

func (m *streamMirror) appendProduced(chunk []byte) error {
	if len(chunk) == 0 || m.headWritten >= m.headLimit {
		return nil
	}
	n := int64(len(chunk))
	if remaining := m.headLimit - m.headWritten; n > remaining {
		n = remaining
	}
	if err := appendTerminalLog(m.path, chunk[:n]); err != nil {
		return err
	}
	m.headWritten += n
	return nil
}

func (m *streamMirror) finalize(finalText string) error {
	if err := m.ensure(); err != nil {
		return err
	}
	return os.WriteFile(m.path, []byte(finalText), 0o600) //nolint:gosec // path is the supervisor-owned task log path derived from a validated task ID.
}

func (m *streamMirror) ensure() error {
	if err := os.MkdirAll(filepath.Dir(m.path), 0o700); err != nil {
		return err
	}
	file, err := os.OpenFile(m.path, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	return file.Close()
}

func appendFile(path string, body []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600) //nolint:gosec // path is the supervisor-owned task log path derived from a validated task ID.
	if err != nil {
		return err
	}
	if _, err := file.Write(body); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func replayTask(taskID string, limits protocol.Limits) Response {
	result, toolErr := controlTask(taskID, controlRequest{SchemaVersion: protocol.SchemaVersion, Op: "poll", WaitMS: 0, VisibleBytes: limitVisibleBytes(limits), VisibleLines: limitVisibleLines(limits)}, limits)
	if toolErr != nil {
		return Response{Status: protocol.ToolStatusError, Error: toolErr, Result: result}
	}
	if isTerminalResult(result) {
		return Response{Status: protocol.ToolStatusSuccess, Result: result}
	}
	return Response{Status: protocol.ToolStatusRunning, Result: result}
}

func isTerminalResult(result ExecResult) bool {
	return result.ExitCode != nil || result.Signal != nil || result.TimedOut || result.Cancelled
}

func readExitResult(taskID string, limits protocol.Limits) (ExecResult, bool) {
	body, err := os.ReadFile(exitPath(taskID))
	if err != nil {
		return ExecResult{}, false
	}
	var record exitRecord
	if err := json.Unmarshal(body, &record); err != nil {
		return ExecResult{}, false
	}
	taskIDValue := taskID
	return ExecResult{
		TaskID:     &taskIDValue,
		ExitCode:   record.ExitCode,
		Signal:     record.Signal,
		TimedOut:   record.TimedOut,
		Cancelled:  record.Cancelled,
		Stdout:     projectStoredSnapshot(record.Stdout, stdoutLogPath(taskID), limits),
		Stderr:     projectStoredSnapshot(record.Stderr, stderrLogPath(taskID), limits),
		DurationMS: record.DurationMS,
	}, true
}

func readExitResultEventually(taskID string, limits protocol.Limits, timeout time.Duration) (ExecResult, bool) {
	deadline := time.Now().Add(timeout)
	for {
		if terminal, ok := readExitResult(taskID, limits); ok {
			return terminal, true
		}
		if time.Now().After(deadline) {
			return ExecResult{}, false
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForTaskStartupState(taskID string, limits protocol.Limits, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if _, ok := readExitResult(taskID, limits); ok {
			return true
		}
		if socketIsLive(sockPath(taskID)) {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func projectStoredSnapshot(record bound.StreamSnapshot, _ string, limits protocol.Limits) bound.StreamSnapshot {
	visibleBytes := limitVisibleBytes(limits)
	visibleLines := limitVisibleLines(limits)
	if visibleBytes == 50*1024 && visibleLines == 2000 {
		return record
	}
	buffer := bound.NewCapBuffer(bound.DefaultStreamStoreBytes)
	_, _ = buffer.Write([]byte(record.Text))
	snapshot := buffer.Snapshot(visibleBytes, visibleLines)
	snapshot.TotalBytes = record.TotalBytes
	snapshot.TotalLines = record.TotalLines
	snapshot.Truncated = snapshot.Truncated || record.Truncated
	return snapshot
}

// The task-directory layout is fixed and fully derivable from task_id alone:
// tasks/<task_id>/ holds meta.json (child pgid, start time, cmd, lifetime
// deadline, last write_seq), control.sock (the stdin/poll/cancel socket),
// stdout.log / stderr.log (the head+tail stream mirror), and exit.json (the
// terminal record). Because the paths are deterministic, the caller can locate
// these settlement files itself without the helper ever emitting a task-dir
// path in a result envelope, which the envelope forbids.
func taskDir(taskID string) string {
	return filepath.Join(currentRuntimeRoot(), "tasks", taskID)
}

func tasksRoot() string {
	return filepath.Join(currentRuntimeRoot(), "tasks")
}

func currentRuntimeRoot() string {
	runtimeRootMu.RLock()
	defer runtimeRootMu.RUnlock()
	return runtimeRoot
}

func setRuntimeRoot(root string) {
	runtimeRootMu.Lock()
	defer runtimeRootMu.Unlock()
	runtimeRoot = root
}

func taskLockPath() string {
	return filepath.Join(tasksRoot(), ".task-limit.lock")
}

func reservationPath(taskID string) string {
	return filepath.Join(taskDir(taskID), "reservation")
}

func metaPath(taskID string) string {
	return filepath.Join(taskDir(taskID), "meta.json")
}

func sockPath(taskID string) string {
	return filepath.Join(taskDir(taskID), "control.sock")
}

func exitPath(taskID string) string {
	return filepath.Join(taskDir(taskID), "exit.json")
}

func stdoutLogPath(taskID string) string {
	return filepath.Join(taskDir(taskID), "stdout.log")
}

func stderrLogPath(taskID string) string {
	return filepath.Join(taskDir(taskID), "stderr.log")
}

func acquireTaskRootLock() (*os.File, error) {
	if err := os.MkdirAll(tasksRoot(), 0o700); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(taskLockPath(), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func releaseTaskRootLock(file *os.File) {
	if file == nil {
		return
	}
	_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	_ = file.Close()
}

func waitForFile(path string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return true
		}
		time.Sleep(25 * time.Millisecond)
	}
	return false
}

func writeJSONAtomic(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	tmp := path + ".tmp-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if err := os.WriteFile(tmp, body, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func updateMetaWriteSeq(taskID string, seq int64) error {
	body, err := os.ReadFile(metaPath(taskID))
	if err != nil {
		return err
	}
	var meta metaRecord
	if err := json.Unmarshal(body, &meta); err != nil {
		return err
	}
	meta.LastWriteSeq = seq
	return writeJSONAtomic(metaPath(taskID), meta)
}

func liveTaskCount() int {
	entries, err := os.ReadDir(tasksRoot())
	if err != nil {
		return 0
	}
	count := 0
	for _, entry := range entries {
		if entry.IsDir() {
			if _, err := os.Stat(exitPath(entry.Name())); errors.Is(err, os.ErrNotExist) && (socketIsLive(sockPath(entry.Name())) || reservationIsLive(entry.Name(), time.Now())) {
				count++
			}
		}
	}
	return count
}

func socketIsLive(path string) bool {
	conn, err := net.DialTimeout("unix", path, 50*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func reservationIsLive(taskID string, now time.Time) bool {
	info, err := os.Stat(reservationPath(taskID))
	if err != nil {
		return false
	}
	if now.Sub(info.ModTime()) > taskReservationMaxAge {
		// A launcher can die after the supervisor has published its socket but
		// before removing the startup reservation. The live supervisor owns the
		// task directory at that point; sweeping it would orphan its child and
		// defeat lifetime enforcement.
		if socketIsLive(sockPath(taskID)) {
			return true
		}
		_ = os.RemoveAll(taskDir(taskID))
		return false
	}
	return true
}

func sweepOldTasks(now time.Time) {
	entries, err := os.ReadDir(tasksRoot())
	if err != nil {
		return
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		info, err := os.Stat(exitPath(entry.Name()))
		if err == nil && now.Sub(info.ModTime()) > 24*time.Hour {
			_ = os.RemoveAll(taskDir(entry.Name()))
			continue
		}
		if errors.Is(err, os.ErrNotExist) {
			_ = reservationIsLive(entry.Name(), now)
		}
	}
}

func emptySnapshot(taskID string) bound.StreamSnapshot {
	return bound.StreamSnapshot{Text: "", TotalBytes: 0, TotalLines: 0, ReturnedBytes: 0, Truncated: false}
}
