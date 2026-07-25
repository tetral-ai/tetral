// Package task owns command execution and its follow-ups: foreground exec, the
// detached-task supervisor, the task directories under
// /tmp/tetral-runtime/tasks, the control-socket protocol between an invoking
// helper and a supervisor, and the stdin / poll / cancel operations.
//
// OWNS:
//   - Foreground exec (runForeground) and its signal/timeout handling.
//   - The detached supervisor (runSupervisor) and its double-fork launch.
//   - Task directories keyed by task_id (meta.json, control.sock,
//     stdout.log/stderr.log, exit.json) — see store.go.
//   - The control-socket wire protocol (stdin / poll / cancel).
//   - The task-limit reservation lock .task-limit.lock and the 24h sweep.
//   - The exec/stdin/poll/cancel timing and lifetime constants (see exec.go).
//
// STATE MACHINE (high level; the per-site tables live in exec.go and
// supervisor.go): a foreground exec runs to exit or is killed at wait_ms. A
// detached exec spawns a supervisor that owns the child; the task then moves
// running -> terminal, with terminal facts settled durably in exit.json before
// control.sock is unlinked. stdin/poll/cancel discover current state from the
// task directory and the control socket.
//
// INVARIANTS:
//   - Class law for every helper subprocess pipe (foreground exec, the
//     supervisor's child, rg in the search package): the pipe readers drain to
//     EOF BEFORE the process is reaped, and every kill/timeout path
//     force-closes the parent read ends after its SIGKILL so a blocked reader
//     always unblocks inside that path's budget. Reaping is never the mechanism
//     that closes a pipe a reader is still draining, so no pre-exit byte is
//     dropped.
//   - The helper never initiates a notification and needs no cleanup hook.
//     Terminal facts are durable in exit.json plus the stream mirror files, so
//     a late notification is built by the caller from a read of the task
//     directory. Cleanup terminates running tasks by releasing the sandbox
//     (supervisors, children, and task dirs die with it); the 24h task-directory
//     sweep only matters for a long-lived sandbox that keeps running.
//   - .task-limit.lock is the single kernel flock the helper ever holds. It is
//     held only across the reservation critical section (old-task sweep, live
//     count against the 16-task cap, task-dir and reservation-file creation)
//     and released before the supervisor is spawned; the kernel drops it if the
//     holder dies, so it cannot go stale. It guards the task-limit reservation
//     only — never workspace paths or file content. The helper's no-lock rule
//     for file operations is unchanged by this one exception.
//   - Serial service on control.sock (one connection at a time) is
//     defense-in-depth, not an ordering promise. Same-task_id ordering is
//     guaranteed upstream by the scheduler's exclusive key; do not lean on the
//     accept loop to order concurrent connections.
//
// UPDATE-WITH:
//   - internal/sandbox/helper/internal/task/exec.go
//   - internal/sandbox/helper/internal/task/supervisor.go
//   - internal/sandbox/helper/internal/task/store.go
//   - internal/sandbox/helper/internal/search/search.go (shares the pipe-drain
//     class law)
package task
