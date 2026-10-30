# execution

Tool execution on this machine belongs here: one **Task supervisor** per task,
which owns that task's process tree; the translation of a call's
`sandbox_policy` into the operating system's isolation; and the tools.

## Design rules

- Supervisors follow the owner rules of `cli/README.md`.
- The executable path comes from App Server startup's canonical version
  directory. The outer sandbox launcher and the `exec_command`, `edit` and
  `view_image` entries all use that path; bundled helper lookup starts beside
  that executable. Changing an installer's `current` symlink cannot switch
  a running server to another version. The application or Controller must
  retain that version's executable and helpers unchanged until its servers
  exit (`appserver/README.md`); tool execution does not manage upgrades.
- A call's permissions are decided before any side effect, read no user file
  and start no process. `policy::TaskPermissions::admit` validates the
  `sandbox_policy` value, where the first failing check decides between
  `invalid_input` and `unsupported_policy`, and translates it.
- Every call, however broad its grant, runs under one restricted Codex
  `PermissionProfile`: the policy's rules one to one, plus mandatory
  protections that no rule reaches (`policy/translate.rs`). A task's
  permissions are built once and never rebuilt.
- Every process of a tool call starts only through `isolation::Isolation`:
  `check` at each execution, then `launch`, which only an `Available` from
  that check can call. The launch has Codex's sandbox manager compile the
  task's profile, filesystem and network together, into one `bwrap` and
  seccomp command with the preference `Require`, so that no grant runs
  unsandboxed, and starts it with a controlled launcher environment,
  as the leader of a new session and process group, with only its pipes,
  the launch-status descriptor 3 and, for exec, environment-data descriptor 4
  (`isolation/mod.rs`).
- The `bwrap` is the one Codex's helper picks for the same command: the
  system `bwrap` on `PATH` that passes the `--help` probe, otherwise the
  bundled one beside `tetral`. A valid, nonzero `CODEX_BWRAP_SHA256` checks
  its digest; the existing development mode with a missing or all-zero
  digest skips that check. None,
  WSL1 or a refusal of user namespaces rejects the call
  `sandbox_unavailable`; a command the transform cannot build rejects it
  `unsupported_policy`; neither starts a process (`isolation/linux.rs`).
  Every `bwrap` it could pick, like the running `tetral`, is a trusted
  program that each task's profile keeps read-only
  (`isolation::trusted_programs`).
- No `bwrap` that a task could have planted is ever run: while a `bwrap` on
  `PATH` that this user can modify or replace comes before every one it
  cannot, the availability check rejects the call `sandbox_unavailable`
  naming that path, before any probe, and `tetral` as `codex-linux-sandbox`
  ends the command with that message before the launch. The check walks the
  App Server's `PATH`; the helper uses that same trusted launcher `PATH`,
  independently of the task's requested environment (`isolation::planted_launcher`,
  `isolation/linux.rs`).
- A missing protected path stays uncreatable. The Codex crates mount an
  empty read-only entry at a missing top-level `.git` of a writable path, and
  at a missing `read` or `deny` path inside one, which `bwrap` must first
  create; where no process of the user could create it, the launch profile
  leaves the protection to the operating system instead
  (`isolation/linux.rs`, `launch_profile`).
- Linux launch profiles also deny existing host D-Bus and systemd control
  sockets: `/run/dbus/system_bus_socket`, `/run/systemd/private`, and the
  effective user's `/run/user/<uid>/bus` and `systemd/private`. Discovery
  checks their `/var/run` spellings, `bus` and `systemd/private` under an
  absolute App Server `XDG_RUNTIME_DIR`, and absolute `unix:path=` addresses
  in its `DBUS_SESSION_BUS_ADDRESS` and `DBUS_SYSTEM_BUS_ADDRESS`. Percent
  escapes and address lists are decoded before lookup. Existing sockets are
  canonicalized and deduplicated; a symlink such as `/var/run` reaches the
  same masked target. These explicit denies survive broader read or write
  grants, without changing the task's network policy. Programs that need
  these host services cannot use those endpoints from a task
  (`isolation/host_ipc.rs`).
  This is bounded filesystem masking, not comprehensive host-service
  isolation: abstract Unix sockets, TCP or autolaunch D-Bus addresses,
  unlisted service endpoints, undiscovered bind-mount aliases, and sockets
  first appearing after discovery are not covered. Missing paths and paths
  that are not sockets are omitted, so discovery creates no host entries or
  stops any service. Runtime denial and ordinary-network controls still need
  behavioral acceptance; unit tests establish candidate selection, address
  parsing and deny precedence only.
- No report is handed to the Report outbox that Engine could never accept:
  an error's `message` and `path` are cut to `ERROR_MESSAGE_MAX_BYTES` and
  `ERROR_PATH_MAX_BYTES` on UTF-8 boundaries and its `detail` rebuilt within
  `ERROR_DETAIL_MAX_BYTES`, and a
  process result over the ordinary limits is rendered again from its stored
  output with half the visible byte budget until it fits, its counters and
  exit facts unchanged (`report`).
- A stream's stored output stays bounded however much the command writes,
  so reading its pipe never stops: the cumulative capture, which no read
  consumes, and the window, which each `running` report and `tool.stdin`
  reply takes, each keep `STORED_HALF_BYTES` of head and of tail and count
  the bytes between. A snapshot splits a byte and a line budget between head
  and tail. Adjacent ranges are decoded together; invalid or incomplete
  input bytes are ignored while all valid fragments remain. Only bytes
  omitted by storage or display limits produce the marker
  `\n[... <n> bytes truncated ...]\n`; raw input counters remain unchanged
  (`capture`).
- A reply returns at most `VISIBLE_BYTES` and `VISIBLE_LINES` of each stream;
  an instruction's `limits` can only lower them (`limits`).
- A task is named during its admission, before its policy is translated,
  so that its profile names its private temporary directory in `T`; a
  rejected instruction keeps no name (`store`).
- Before its launch a task gets `sessions/<session_id>/tasks/<task name>/`
  with `stdout.log` and `stderr.log`, then its temporary directory in `T`,
  which must be a directory of this user with no permission for group or
  others; a failure names the path (`store::TaskFiles::create`). Each log
  receives its stream's first `LOG_HEAD_BYTES` as they arrive and, at the
  end, the capture's stored text (`store::Log`). Both directories are cleaned once the
  processes are gone and reports are acknowledged or dropped. Removal restores
  only the owning user's directory permissions within the task temporary tree
  and never follows symbolic links. Each directory is attempted independently;
  failures record its task, cleanup part and OS error, without changing the
  tool result or retaining its output for another attempt (`TaskFiles::remove`).
- Each probe of a `bwrap` candidate is bounded by `PROBE_TIMEOUT` and
  `PROBE_OUTPUT_LIMIT_BYTES`, which `Isolation::with_probe_bounds`
  overrides.
- A task's supervisor owns it from its admission to its cleanup: `staged`
  until its Session binding owner starts it, `starting` while it checks the
  isolation, creates the task's files and launches the command, `running`
  from the command's actual start, `ended` with its `terminal` report and
  ready for cleanup once its processes are confirmed gone and every report,
  control replies included, is acknowledged or dropped. After attempting file
  cleanup, the full supervisor transfers its existing mailbox to a minimal
  `FinishedTask`, retaining only stream counters, exit facts and write records
  for late controls. A cleanup failure is logged without retaining execution
  or scheduling another attempt. A stream holds its read buffer only while
  its pipe is open. The execution supervisor reads both
  pipes all the time into the capture and the logs, so no command blocks on
  its output, and every wait reads the seam clock (`supervisor`).
- The supervisor answers the task's `tool.cancel` at receipt in every
  state, and its `tool.stdin` in every state but `staged` and the
  availability check that begins `starting`, during which that `tool.stdin`
  waits and the task's other messages are still read: a cancel or the
  binding's close prevents the start and makes the task's `terminal` report
  at once, and the connection's end discards the task without one. The
  binding's close ends a running task as a cancel does. Each waiting reply is a timer and a waiter of the
  supervisor, which meanwhile keeps reading output and handling the exit and
  other controls (`supervisor`).
- A read replies at once when a window holds output or the task has ended,
  otherwise at the first new output, the exit or the end of its wait; a
  write's reply waits its wait, ending early only at the exit. Both take
  their waits' defaults and clamps from `stdin::Request`.
- A task's writes are handled one at a time in arrival order. Each
  `write_seq` is recorded with the SHA-256 of its `chars`, the bytes
  written and its error before its reply, so that a repeat reuses the
  record and never writes again (`stdin::WriteRecords`). A write that fails
  or outlasts `WRITE_TIMEOUT` closes the task's stdin for good; `chars` of
  exactly U+0003 send SIGINT the way the termination procedure sends SIGTERM
  (`terminate::interrupt`).
- A cancel is answered at receipt, with the cumulative capture; the task's
  state decides the reply's `cancelled` and `was_running`
  (`cancel::Receipt`). Cancelling a running task stops its writes, leaves its
  stdin open and begins the termination procedure (`supervisor`).
- Of `exec_command`, `write_stdin`, `edit` and `view_image`, the error
  kinds, messages, clamps and validation that the contract does not state
  are those of the Go Helper in `internal/sandbox/helper/`, case by case
  (`exec`, `stdin`, `child`).
- `exec_command` runs as `tetral __tetral-task exec <directory> <shell>
  -c <cmd>` through the restricted path; inside the sandbox the launcher entry
  checks and enters the directory, with no fallback, and executes
  `[<shell>, "-c", <cmd>]`. It reports on the launch-status pipe at
  descriptor 3: `child::STARTED` right before the exec, whose arrival is the
  task's actual start, or a failure object. The pipe's end tells nothing,
  since the sandbox's own stages hold it open (`child`).
- `edit` and `view_image` run only as the restricted children `tetral
  __tetral-task edit` and `view-image`, through the same launch as a
  command and never in the App Server. The
  child writes `child::STARTED` at its own start, reads one request
  `{cwd, input}` from stdin, joins a relative path to `cwd` only after the
  checks of a command's directory, and writes one result envelope to
  stdout (`child`). The supervisor takes a result once the child's stdout
  has ended, within `file_tools::RESULT_MAX_BYTES`, and only a finished,
  untruncated envelope of the call's tool whose image, if any, passes
  image acceptance with its decoded size and format; anything else is
  `helper_failure` (`file_tools`). A cancel ends the child through the
  termination procedure and the call ends `cancelled`, after the call
  started once the child wrote its start; a result received first stands
  (`supervisor`).
- An edit reads at most `child::edit::MAX_BYTES` and one byte more, finds
  `old_string` exactly or, only when that finds nothing, with curly quotes
  mapped to ASCII, which locates spans and changes no written byte, and
  writes the validated replacement directly to the existing file. It needs
  no writable parent directory, creates no file, and does not restore special
  permission bits or explicitly sync the write. A failed or interrupted write
  can leave partial content (`child/edit.rs`).
- An image is recognized by its first bytes and refused from its header,
  by `protocol::image` acceptance of its `stat` size and dimensions, before
  the rest is read; it is read whole, bounded at that size and one byte
  more, and never cut or split (`child/view_image.rs`).
- A command's environment is the App Server's without any `TETRAL_` name,
  then the call's `env`, then entries that `env` cannot override and
  `TMPDIR`. An inherited entry whose name or value is not UTF-8 is left out.
  The parent serializes this environment into an anonymous seekable file;
  descriptor 4 carries it through setup. The isolated exec entry reads and
  closes it, then installs the environment only for the final program exec.
  Launchers and file-tool entries receive only the App Server's trusted
  `PATH` and `HOME`, never the command's environment. Under `test-seams`
  only, a launcher whose network is not `disabled` also gets
  `TETRAL_TEST_SEAMS`; it is absent from the final command's environment.
  The seccomp filter of `disabled` refuses its connection to the harness
  (`env`, `isolation`, `child/exec.rs`).
- A call's waits take their defaults and clamps from `exec::ExecCall`. On
  Linux an exit status from 129 to 192 is a death by signal `status - 128`,
  which the sandbox's stages report that way (`exec::ExitFacts::observed`).
- The termination procedure sends a task SIGTERM, on Linux to its PID
  namespace's first process, which passes it on to the command's process
  group, SIGKILL after a grace, and waits a bounded time for the exit; at a
  connection's end it starts with SIGKILL. A sent signal is never taken as
  the exit, and after SIGKILL the pipes are read only until that wait ends
  (`terminate`, `supervisor`).
- A task stays tracked until its processes are confirmed gone: on Linux, no
  process this user can inspect in its PID namespace, checked every
  `cleanup::CHECK_INTERVAL`. When the task's child reports `child::STARTED`
  on descriptor 3, or, without that report, while the task is being ended,
  the supervisor finds the namespace's first process and writes
  `execution/<task name>.json` with the boot ID, that process's ID and start
  time and the namespace's inode, and reports the namespace to the binding
  owner; it holds the namespace open until confirmation, so that no later
  namespace takes the inode. Confirmation deletes the record, and the task's
  files wait for it (`process`, `cleanup::Record`, `supervisor`).
- At its start an App Server reads the records an earlier one left: one of
  another boot is confirmed at once and signals nothing; otherwise the
  recorded process, the same ID with the same start time, gets SIGKILL, and
  the record is confirmed once it and every inspectable process of its
  namespace are gone. Until then its Session is `cleanup_pending`; its
  Session's last confirmation removes the task directories the earlier
  tasks left. The check owns the records and is the only writer of which
  Sessions they keep `cleanup_pending`; readers see what it last published
  (`cleanup::Check`, `cleanup::Leftovers`). The check's interval and a
  record's size bound are `cleanup::Limits`, which `cleanup::read_with`
  overrides; a supervisor's waits, its check interval and the
  launch-status bound are `supervisor::Bounds`.
- A connection's end discards a staged task without a report and ends a
  running one; the shutdown ends every task through the termination
  procedure, and once it has begun, the end of the connection that it
  brings counts as the shutdown (`supervisor`).
- The initial wait runs from the spawn. One that ends before the command's
  start, whatever `on_wait_expiry` says, one under `kill`, and a detached
  command's lifetime end the command through the termination procedure
  (`supervisor`).

## Where things are

- `supervisor.rs`: `spawn`, a task's `TaskSpec` and its `TaskListener`, the
  shared `Context` and its `Bounds`, `SupervisorMessage`, `Control`
  and `SupervisorMailbox`; the start, the running loop, the reports, the
  controls with their waiters and replies, the confirmation and the
  cleanup.
- `stdin.rs`: `Request`, what a `tool.stdin` asks for with its wait, and
  `WriteRecords`, a task's handled writes.
- `cancel.rs`: `Receipt`, the state in which a cancel finds its task.
- `capability.rs`: the `tool_execution` fragment of ready from the startup
  checks (`StartupChecks`).
- `exec.rs`: `ExecCall`, a call's directory, waits and expiry; `shell`;
  `ExitFacts`; `Outcome`, the result envelope of a report.
- `env.rs`: `command_environment`.
- `child/mod.rs`: the launcher entry `__tetral-task`, a file tool's
  `FileRequest` and its envelope, and the directory checks;
  `child/exec.rs`, `child/edit.rs` and `child/view_image.rs`: its `exec`,
  `edit` and `view-image` kinds.
- `file_tools.rs`: `FileCall`, a file tool's child arguments, request,
  result and the envelopes of its failure and its cancel; `Pipes`, the
  child's stdin and stdout as the supervisor drives them.
- `terminate.rs`: `Termination`, its `Steps` and `interrupt`.
- `process/mod.rs`, `process/linux.rs`: a task's `Leader`, its PID
  namespace, held open (`hold_namespace`), and whether the namespace is
  empty, start times and the boot ID.
- `cleanup.rs`: a task's `Record` and its path; `read`, `Check`, the records
  an earlier App Server left and their checks, and `Leftovers`, which
  Sessions they keep `cleanup_pending`.
- `policy/mod.rs`: `TaskPermissions`, a task's stored permissions, and
  `PolicyRejection`.
- `policy/validate.rs`: the validation steps, path normalization and the
  network rules' host, CIDR and address forms.
- `policy/translate.rs`: `TranslationInputs` and the profile's entries.
- `isolation/mod.rs`: `Isolation`, its `check` and `launch`, `Available`,
  `Command`, `Process`, `PASSED_DESCRIPTOR`, `LaunchError` and
  `trusted_programs`.
- `isolation/linux.rs`: the choice of `bwrap` and its candidates, the walk
  for a planted `bwrap`, the digest check of the bundled one, the WSL1 test,
  the namespace probe and the launch profile.
- `isolation/host_ipc.rs`: known filesystem service endpoints and the
  mandatory denies appended to Linux launch profiles.
- `capture.rs`: `StreamCapture`, a stream's two buffers and counters, the
  `Window` taken from it, and their snapshots.
- `limits.rs`: `Visible`, the byte and line budget of each stream in a reply.
- `report.rs`: `Report`, its `Correlation` and `Body`, the error bounds
  (`bounded`) and the size fitting of a process result (`fit`).
- `store.rs`: a task's name, the paths of `T`, of a task's directory and of
  its temporary directory, `TaskFiles` with its `Log`s, `clear_task_parent`
  and `clear_session_tasks`.
- `isolation/harness.rs`: the test entry `tetral-isolation-harness`, under the
  feature `test-seams`: one command through admission, the check and the
  launch.
- Tests: `tests/capture.rs`, the capture buffer's retention, counters,
  snapshot budgets, UTF-8 boundaries and windows; `tests/exec_call.rs`, an
  `exec_command` call's defaults and clamps and the exit facts of a wait
  status; `tests/file_tool_results.rs`, the result a file tool's supervisor
  reads from a real child, up to 14,680,064 bytes, and each `helper_failure`;
  `tests/policy_table.rs`, each policy's rejection code and each profile's
  entries with Codex's resolution of them; `tests/report_bounds.rs`, a
  process result fitted to the ordinary limits and an error's bounds;
  `tests/visible_limits.rs`, an instruction's `limits` against each
  stream's budget. Component integration tests in `isolation/mod.rs` check
  inherited descriptors at exec with and without `close_range`;
  `child/edit.rs` checks a real file's bounded read. Tests using files or
  subprocesses are component integration evidence regardless of Rust module
  placement.

A normal result must enter the App Server's retained-report budget before it
can be queued. If it does not fit, execution releases that new body and uses
a compact `resource_exhausted` error. A running result that cannot be retained
starts the normal termination procedure; the terminal error waits for process
confirmation. Control errors preserve already observed write and cancel facts.
Already admitted reports are never rewritten to make room.

After processes are confirmed gone, all prior reports have a terminal outcome,
and cleanup has been attempted, the execution supervisor is released. A small
`FinishedTask` keeps the existing receiver, end facts, output counters and write
records for late controls. It holds no process, task specification, sandbox
policy or output text. These protocol records remain until their binding drops
them; this is not a constant-RSS or timed-eviction guarantee.
