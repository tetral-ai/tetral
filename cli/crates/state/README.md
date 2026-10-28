# state

The App Server's files and time belong here: the state root and the names in
it, the control socket's path, the checks that the control paths are private,
the private creation and atomic replacement of files, the configuration and
credential files, the clock and the test
seams.

## Design rules

- It depends on no other workspace crate, holds no connection or Session logic
  and records nothing.
- The CLI reaches the root's control files through `TETRAL_HOME` as given,
  without a trailing `/` or a `.` component (`StateRoot`). A path it builds
  for others, the control socket's among them, starts from the root's
  canonical path, resolved when it is built.
- Commands using the root require both the given and resolved root paths to
  be UTF-8. The shared check rejects unsupported paths as
  `unsafe_control_path` before configuration is read or files are created.
  For a missing root, it resolves existing ancestors and projects missing
  directories without creating them, preserving symbolic-link and `..`
  semantics. Startup repeats the resolved-path check while holding the lock.
  This narrows accepted root paths; working-directory encoding rules are
  unchanged. `--version` alone still reads no environment or files.
- A check of a control path reads it without following a symbolic link and
  creates nothing (`private::check_root`); ancestors of the root may be
  symbolic links.
- Directories are created with mode `0700` and files with mode `0600`, opened
  with `O_NOFOLLOW` (`files`). A file is replaced only through a temporary
  file in its directory (`files::replace`), and the next command to take the
  state-root lock removes the temporary files that an interrupted replacement
  left (`files::remove_interrupted_replacements`). Every operating-system
  failure under the root is an `IoFailure` naming its path.
- An unknown member of `config.toml` is ignored (`config::Config`). Configure
  encodes the complete merged configuration and checks its 65,536-byte limit
  before replacing either file. The same encoded bytes are then written.
  An app field that cannot fit by itself is `invalid_arguments`; an oversized
  merged configuration is `invalid_bootstrap`.
- Atomic replacement keeps old content only when failure precedes rename.
  Parent-directory open or sync failure after rename leaves new content
  visible with unconfirmed durability. Credentials and configuration are
  separate replacements, not a two-file transaction.
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

Only `test-seams` builds accept `TETRAL_TEST_SOCKET_PARENT` for child
processes. In-process test clients use `paths::TestSocketContext` and
`private::check_socket_in` explicitly, without modifying global environment.
The override changes only `S`'s parent: uid naming, root hashing, owner/type
checks and modes remain unchanged. Invalid test parents fail instead of
falling back to the user's directory. Production builds always use the
platform parent and contain no override entry point.

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
- `seams.rs`: the clock, `sleep_until`, the barriers, the counters and, under
  `test-seams`, the process side of the seam protocol.
