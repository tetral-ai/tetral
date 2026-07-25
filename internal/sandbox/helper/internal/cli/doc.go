// Package cli is the helper's process front door: it dispatches subcommands,
// loads and unlinks the payload, drops privilege, and enforces the stdout /
// exit-code discipline that the caller relies on.
//
// The dispatch surface (one Main switch) is: the model tool subcommands (exec,
// stdin, poll, cancel, read, write, edit, apply_patch, grep, glob,
// view_image), the health check, and three hidden entrypoints that are not
// model tools and take no --payload — __supervise and __supervise-bootstrap
// (detached-task supervision) and __capture (idle-output capture mode).
//
// OWNS:
//   - Subcommand dispatch and argv shape (sandbox <tool> --payload <path>).
//   - Payload load, integrity checks, and unlink.
//   - Privilege drop to the workspace runtime identity.
//   - stdout / exit-code discipline and the stderr redirect.
//   - maxPayloadBytes and helperIDPattern (see cli.go).
//
// STATE MACHINE: none of its own; each invocation runs one operation and exits.
//
// INVARIANTS:
//   - stdout carries exactly one JSON envelope and nothing else. Because the
//     transport merges the process's stdout and stderr into one string, a
//     single stray stderr line would corrupt envelope parsing; the helper
//     therefore redirects its own stderr to /tmp/tetral-runtime/logs/helper.log
//     at startup (append, best-effort, falling back to /dev/null, never to the
//     transport).
//   - Exit 0 means an envelope was emitted and is authoritative, including an
//     envelope with status = error. A non-zero exit means the helper failed
//     before it could emit an envelope; the caller synthesizes a helper_failure
//     result for that case. The exit code, not the envelope, is what tells the
//     caller which happened.
//   - The payload is opened as the helper identity: under a root verified to be
//     a directory owned by the current euid with no group/other write bits, the
//     file is opened with openat2 no-follow (RESOLVE_BENEATH |
//     RESOLVE_NO_SYMLINKS) and fstat-checked to be a regular file owned by the
//     euid with no group/other bits. The helper reads it, unlinks it, and only
//     then drops to the workspace-root runtime user before any tool effect, so
//     the model's runtime user never reads the payload.
//   - SIGTERM, SIGINT, or SIGHUP, or a closed stdout pipe (which is treated as
//     SIGTERM), kills the foreground child process group and exits non-zero
//     with no envelope. Detached supervisors are exempt: they are owned by a
//     supervisor process, not by the invoking helper.
//
// UPDATE-WITH:
//   - internal/sandbox/helper/internal/cli/cli.go
//   - internal/sandbox/helper/internal/task/exec.go (foreground signal handling)
//   - internal/sandbox/helper/internal/task/supervisor_auth.go (entrypoint auth)
package cli
