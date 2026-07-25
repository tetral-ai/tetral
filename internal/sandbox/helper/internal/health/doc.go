// Package health implements the health subcommand run during sandbox
// preparation.
//
// OWNS:
//   - The base-template invariant checks and the health envelope shape.
//
// STATE MACHINE: none; one pass over the checks per invocation.
//
// INVARIANTS:
//   - The checked set is the full base-template invariant set: sandbox on PATH,
//     rg on PATH and runnable, the apply_patch self-test, /tmp/tetral-runtime
//     at mode 0700, rclone on PATH, fusermount3 present and setuid,
//     /etc/fuse.conf containing user_allow_other, and a working /proc/self/fd
//     same-inode file and directory capture reopen. This set must stay equal to
//     the invariants the base-template build actually prepares, so it co-updates
//     with the template build — the two must never drift apart.
//   - Any failed item makes the whole result an error with kind helper_failure,
//     and preparation fails on that error.
//
// UPDATE-WITH:
//   - internal/sandbox/helper/internal/health/health.go
package health
