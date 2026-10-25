# state

The App Server's files and time belong here: the state root and the names in
it, the control socket's path, the checks that the control paths are private,
the private creation and atomic replacement of files, the configuration and
credential files, the files of the Report outbox, the clock and the test
seams.

## Design rules

- It depends on no other workspace crate, holds no connection or Session logic
  and records nothing.
- The CLI reaches the root's control files through `TETRAL_HOME` as given,
  without a trailing `/` or a `.` component (`StateRoot`). A path it builds
  for others, the control socket's among them, starts from the root's
  canonical path, resolved when it is built.
- A check of a control path reads it without following a symbolic link and
  creates nothing (`private::check_root`); ancestors of the root may be
  symbolic links.
- Directories are created with mode `0700` and files with mode `0600`, opened
  with `O_NOFOLLOW` (`files`). A file is replaced only through a temporary
  file in its directory (`files::replace`), and the next command to take the
  state-root lock removes the temporary files that an interrupted replacement
  left (`files::remove_interrupted_replacements`). Every operating-system
  failure under the root is an `IoFailure` naming its path.
- An unknown member of `config.toml` is ignored (`config::Config`).
- Every timer and deadline of the CLI reads `seams::now`, a clock that counts
  the time the machine sleeps, never `std::time::Instant` (`seams`). A task
  waits for a deadline with `seams::sleep_until`, which reads the clock again
  at least every `seams::RECHECK_INTERVAL`.
- Its test seams, `seams::check_harness`, `seams::barrier`,
  `seams::barrier_blocking`, `seams::count` and `seams::harness_variable`,
  which hands the harness to a child process with an otherwise empty
  environment, do nothing unless the crate feature `test-seams` is on
  (`seams`).

## The state root and the control socket

| Path under the root | Kind | Mode |
|---|---|---|
| `config.toml` | The app the root belongs to, Engine's URL, the instance and the local options (`config::Config`) | `0600` |
| `credentials.json` | The bootstrap object, connection key included; its format is that of `protocol::bootstrap` | `0600` |
| `run/` | Directory | `0700` |
| `run/app-server.lock` | The lock that the one process owning the root holds; never unlinked or replaced | `0600` |
| `outbox/` | Directory, deleted and created again empty at each App Server start (`outbox::reset`) | `0700` |
| `outbox/<n>/<message_id>.json` | A report frozen before its first send, in the directory of its connection's number `<n>`, which goes when that connection ends (`outbox`) | `0600` |

The root itself has mode `0700`, and the check accepts no permission bit for
group or others on any of these paths. `server run` also creates `logs/` for
its log files (`paths::LOGS`, written by `observe::LogFile`), which the root
checks do not cover, nor `sessions/<session_id>/`, a Session's files
(`paths::session_directory`), which holds its tasks' directories. The App
Server's socket lies outside the root, at `S/<h>.sock`: `S` is
`/tmp/tetral-sock-<uid>` on Linux and `/private/tmp/tetral-sock-<uid>` on macOS, `<uid>` the process's real user ID,
and `<h>` the first 16 lowercase hexadecimal characters of the SHA-256 of the
root's canonical path (`paths::socket_path`). The path is at most 57 bytes.
Once the root exists, `S` must be a directory of the current user with mode
exactly `0700` and the socket a socket of the current user with mode `0600`
(`private::check_socket`). The App Server creates a missing `S` with that
mode and refuses one of another owner or mode, or a symbolic link, leaving it
as it is (`private::create_socket_dir`).

## Where things are

- `lib.rs`: this documentation and the print lints that `cli/README.md` states.
- `paths.rs`: `StateRoot`, the names in the root, `socket_dir` and
  `socket_path`.
- `private.rs`: `check_root`, `check_socket`, `create_socket_dir` and the
  reasons a control path is unsafe.
- `files.rs`: `IoFailure`, private directory creation, private opening,
  bounded reads, `replace` and `remove_interrupted_replacements`.
- `config.rs`: `Config`, its reading and its replacement.
- `credentials.rs`: the bounded reading and the replacement of
  `credentials.json`.
- `outbox.rs`: the reset of `outbox/` and its report files.
- `seams.rs`: the clock, `sleep_until`, the barriers, the counters and, under
  `test-seams`, the process side of the seam protocol.
