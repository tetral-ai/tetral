# tetral-tests

Tests of the workspace as a whole, and the harness of the cross-process
tests: those that run the `tetral` binary and the reference control server of
`cli/reference/` as separate processes, over real HTTP and SSE, on real
temporary directories. No workspace crate depends on this package.

Run Cargo from `cli/`, so its pinned toolchain applies. For this package:

```sh
TETRAL_ACCEPTANCE_DIR=/absolute/path/to/acceptance cargo test --locked -p tetral-tests
```

The records stay in that directory, with one file per test executable and
run. A filtered run records its test arguments and covers only that selection.
The cross-process cases also need Go for the reference server and the host
facilities described below. `cargo test --locked -p tetral-tests --lib` runs
the isolated harness tests without starting the CLI.

## Design rules

- A cross-process test runs `binaries::tetral()`, built once per test process
  with `cargo build --locked -p tetral --features test-seams --target-dir
  target/test-seams` in `cli/`, so that its seams answer to the test, and
  `binaries::reference_server()`, built with `go build` at the repository
  root. It never runs a binary of the target directory that `cargo test`
  itself holds.
- A test starts `tetral` as a child process whose environment holds only what
  the test sets (`run::Tetral`), its state on a temporary directory, and wires
  it to the test's `Seams` socket when the test drives its clock or barriers.
  A run that has not exited within `seams::WAIT_LIMIT` is killed and fails the
  test (`run::finish`).
- Time and ordering in a test come from the seams (`seams::Seams`): the
  `tetral` under test reads its clock from the test and waits at the barriers
  the test holds, and the reference control server's clock moves only through
  its `clock` control. A sleep is never the evidence that something did not
  happen; `seams::WAIT_LIMIT` bounds only a wait for something that must
  happen.
- A test of real restricted processes runs `tetral` with `argv[0]`
  `tetral-isolation-harness`, which starts one command through the
  isolation's check and launch and answers with its outcome, and observes
  from outside every sandbox what the command reached. It needs a host that
  allows unprivileged user namespaces and a `bwrap` on `/usr/bin` or `/bin`.
  A case that needs a `tetral` built with a bundled `bwrap`'s digest uses
  `binaries::tetral_with_bwrap_digest`, built into its own target directory.
- Each run of a test file of `tests/` writes its acceptance record into the
  directory that `TETRAL_ACCEPTANCE_DIR` names (`record`), and fails at once
  when it is unset. Cases start as `incomplete`; call `case.pass()` only after
  their assertions. An unfinished case stays incomplete on an ordinary
  return or becomes failed during a panic. Completed cases keep their own
  outcome when a later case fails. Each execution has a separate run identity
  and records its revision, uncommitted changes, platform, toolchain and test
  arguments. Set `TETRAL_ACCEPTANCE_CONTEXT` to describe a targeted mutation
  or another special run; include what changed and which tests were selected.
  A case that needs a non-root user is recorded as not run, with its reason,
  when a local test runs as root; under GitHub Actions this fails the required
  case (`record::Case::runs_as_non_root`). No case skips silently.
- A test reads the one result object of a command's stdout (`run::Run`). A
  case that a command must not change compares a snapshot of the root's
  paths, kinds, owners, modes and bytes taken before and after it
  (`snapshot`).
- Each test uses `socket_directory::isolated()` to own a short temporary
  parent for `S`. Related child commands capture this context and receive
  `TETRAL_TEST_SOCKET_PARENT`; SDK clients receive an explicit
  `state::paths::TestSocketContext`. Only `test-seams` builds expose the
  override. The uid directory, root hash and permission checks remain the
  production rules. Tests never rename or delete the user's shared `S`, and
  never mutate the test process's environment. Parent cleanup waits until
  the scope and all related child handles have released it.
- `run::RunningChild` drains stdout and stderr concurrently from spawn,
  retains at most 16 MiB per stream and keeps draining beyond that limit.
  An acceptance run fails explicitly if capture was truncated or did not
  reach EOF; it never treats partial diagnostics as complete evidence.
  Child handles reap on drop, including startup failure and panic. Pipe
  workers use cancellable nonblocking I/O, with at most one second of
  shared reader completion waiting after exit. A log-backpressure case
  must opt into `Tetral::stderr_backpressure()`, which fills the pipe with
  blank lines before spawn and waits for explicit draining; ordinary tests drain.
  Harness output tests use Python 3 and exceed pipe capacity on both streams.
- A case that needs `S` owned by another user, or a `bwrap` that this user
  can neither modify nor replace, has root make it with `sudo -n`; where that
  fails the case is recorded as not run, and under GitHub Actions the test
  fails instead (`other_user::sudo`, `other_user::RootDirectory`). A state
  root owned by another user needs no privilege: such a case uses a
  directory that root already owns (`other_user::directory`).

## Where things are

- `src/lib.rs`: `workspace_dir` and `repository_root`, where the builds run.
- `src/binaries.rs`: the builds of `tetral`, with and without a bundled
  `bwrap`'s digest, and of the reference control server.
- `src/reference.rs`: `ReferenceServer`, the server as a process, its startup
  input, its controls and its request log.
- `src/run.rs`: `Tetral`, a command line of the `tetral` under test or of a
  link to it, under another `argv[0]` when it selects an entry point, and
  `Run`, its exit status, output and result object.
- `src/seams.rs`: `Seams`, the harness side of the seam protocol, which
  `state::seams` defines; a held barrier lets all pass at its release, or one
  at a time (`Seams::release_one`).
- `src/record.rs`: `Record`, `Case` and the `record!` macro.
- `src/snapshot.rs`: `snapshot`, every path under a directory with its kind,
  owner, mode and bytes.
- `src/socket_directory.rs`: owned temporary socket parents and per-test scopes.
- `src/other_user.rs`: a directory that another user owns, `sudo -n` under
  the rule above, and `RootDirectory`, a directory that root makes in
  `/tmp`.
- `src/app_server.rs`: `AppServer`, a running `server run`, and `request`, one
  raw HTTP/1.1 request to the local API over a socket whose path it derives
  apart from the code under test (`socket_path`).
- `src/machine.rs`: `Machine`, a configured root and its App Server on its
  own seam clock against a reference control server that knows the test
  credential, the helpers that drive and read that server, and the seam
  counters of the Connection owner; `Machine::start_with_tools` gives the App
  Server the protocol's `tool_execution` fixture to advertise, through the
  test seam `TETRAL_TEST_TOOL_EXECUTION`, and the system `PATH` for its
  tasks, and `Machine::start_with_tools_from` another `tetral` build and
  `PATH`; `Machine::start_under` runs the App Server under another program,
  such as `bwrap` with mounts that change what it finds; `task_parent`
  locates a root's `T` apart from the code under test.
- `src/tasks.rs`: `TaskMachine`, such a machine with one prepared Session,
  also on another `tetral` build and `PATH` (`TaskMachine::start_from`),
  which issues tool instructions and controls, reads their reports and
  replies, follows the seams of a task's start, initial wait, output, writes,
  waiting replies, termination steps, observed exit and the checks of an
  ended task's processes, waits for the App Server's exit after a shutdown,
  and removes `T` when it ends.
- `tests/configure.rs`: `tetral configure` and `tetral --version`: the refused
  arguments, bootstraps, apps and control paths with the root unchanged, the
  size limit and private modes of a new root, each failure code, configure
  over a broken pair, a newer `config.toml` and interrupted replacements, and
  no key in the output or the argument list; against the reference control
  server, the credential a `server run` authenticates with after an
  interrupted configure, configure beside an online App Server, a broken
  configuration that sends no request, and no key in what a connecting
  `server run` writes or shows.
- `tests/connection.rs`: the Connection owner against the reference control
  server: refused credentials and versions, a credential expired before
  start, the attempt timeout and the backoff, staging until the ready ACK and
  its limit, a stream lost before that ACK, grants from first sends,
  conflicts and predecessors, a deadline passed in one clock step, answers of
  an ended connection, a heartbeat ACK taken after the connection deadline,
  the event size and protocol version, the startup phases in ready, and a
  redirect that is not followed.
- `tests/ledger_outbox.rs`: the replay ledger and the Report outbox against
  the reference control server: refused reports never sent again and a
  refusal that ends the connection, a report whose ACK was lost sent again
  byte for byte and recorded once, a Session instruction delivered again
  routed once and its `message_id` with other content a conflict, and the
  correlation of protocol errors. A preparation rejected before any probe
  keeps one final report identity across a lost ACK and instruction replay;
  another instruction for the same binding owns a separate result.
- `tests/preparation.rs`: Session preparation against the reference control
  server: each failed check and what it creates or never creates, links into
  the state root and to a project, `create_cwd`, a repeated, held, retargeted
  and changed preparation and its ACK, two App Servers with the same
  Sessions, a removed directory and a new connection, a probe result after a
  new connection and a resumed stream, the probe slots, the order in which
  waiting preparations get them, their timeouts and the rejection episode
  they record, 128 Session records, a held probe under heartbeats and
  shutdown, the listing of a staged and of a failed preparation, reports
  refused as answering no issued instruction, a new binding refused by a
  full replay ledger, and a preparation that names an earlier connection.
- `tests/tool_admission.rs`: tool instructions against the reference control
  server: one event of up to 4,259,840 bytes read and a larger one that
  closes the stream unread, and the budgets of `input`, `chars` and metadata
  and the 65,536 bytes of `tool.cancel` and `session.close`, each instruction
  with its own report or protocol error; with a prepared Session, an
  instruction delivered again or its payload issued again, a `task_id`
  issued again with other content, tasks staged behind a withheld
  `prepared` ACK and the controls they held when their start fails or is
  rejected, after which the task leaves the registry, a
  cancel and a close while a task's availability check is held, another
  Session's preparation while the admission of a Session without an owner
  is held, the
  replay-ledger entries of tasks, kept until a resume
  or freed by each ACK, also of an instruction that joined a task, a
  running task's write and cancel with no entry free, which leave it
  unchanged, and, with only the ledger's reserve free, new tasks
  refused, as one `pressure.rejecting_started` of `tetral_tasks` in the App
  Server's log, while a running task's controls, its binding's close and
  its report are recorded.
- `tests/capability.rs`: the `tool_execution` that ready advertises from
  the App Server's own startup checks, with all tools and without a shell.
- `tests/tool_reports.rs`: the reports of tool instructions: an ACK that
  names another report and one without `accepted` release nothing, and the
  report is sent again byte for byte; a media report that the server reads
  slowly completes past 10 seconds within its total while heartbeats keep
  their timing, and one whose answer is withheld fails 10 seconds after its
  body is acknowledged and is sent again unchanged.
- `tests/edit_view_image.rs`: `edit` and `view_image` through their
  restricted children: each step of an edit, the curly-quote fallback, the
  file's ordinary permission bits kept, the images at and past image acceptance's limits, files
  that are no image, files the policy denies or the CLI protects, a missing
  file, a directory and an overlong path, each failure of the working
  directory, the input budget and a cancel after the child's start.
- `tests/exec_command.rs`: `exec_command`: its reports and their sequence,
  the initial wait under `kill` and `detach`, its clamp and a start that
  never comes within it, a detached command's lifetime, the working
  directory and its errors, the environment, the exit facts, the end of a
  task's processes, the metrics and the trace context.
- `tests/stdin_cancel.rs`: `tool.stdin` and `tool.cancel`: writes recorded
  by `write_seq`, repeated and stale writes and a lost reply sent again,
  failed controls, a write after the task's cleanup, the waits of reads and
  writes, their clamps and a lifetime they never extend, windows and a lost read reply, a reply and the `terminal` report
  of an exit during its wait acknowledged apart, windows, counters and a
  control's `limits` in replies, a full pipe, writes sent together, escaped
  `chars`, U+0003, and cancels: their cumulative output, the one
  termination that repeated cancels join, and a cancel during a blocked
  write.
- `tests/task_storage.rs`: a task's output beyond what a stream keeps and
  its result marked `truncated`, files
  that cannot be created and a log that cannot be written, task files that
  no other task reaches, `TMPDIR` with files and Unix sockets, the files'
  removal after the ACKs with no descriptor of the task's PID namespace
  left in the App Server, and `T` after the App Server's death.
- `tests/termination.rs`: the end of tasks: a cancel's grace for the
  command's TERM trap, a cancelled task's report at the SIGKILL after the
  grace while a process stays blocked on a FUSE mount, and at the end of
  the wait after the SIGKILL when the task's exit is never observed, a
  shutdown that ends a running task through its TERM trap before the App
  Server exits, a connection's end that discards staged tasks and a task
  whose availability check is held and kills running ones at once, resumes
  that keep a task running,
  the deadline of a withheld resume and of a clock step, an old
  connection's waiter and output after a new one, processes blocked on a
  FUSE mount that keep their Session `cleanup_pending` while others
  prepare, and the records an App Server killed with SIGKILL leaves, also
  one of another boot.
- `tests/local_service.rs`: `server run`, `server status` and `server stop`
  and the local API: status while Engine is unreachable, the wrong routes,
  shutdown on request, on each signal and with a handler blocked, one owner
  per root, restart after SIGKILL with the stale socket replaced, 16 SDK
  clients at once, the socket's path,
  modes and hourly modification time, an unsafe `S`, every result and failure
  code with the root unchanged, a log file that cannot be opened, and no key
  in status. An early command failure with prefilled stderr proves the caller
  waits for its final queued record when drained and exits at the shared
  flush deadline if the pipe stays full.
- `tests/crate_graph.rs`: the rules of the crate graph, checked over the
  output of `cargo metadata`, and the rule that no library crate prints,
  checked over the library crates' sources.
- `tests/release_seams.rs`: a release build of `tetral` holds no test seam.
- `tests/isolation_linux.rs`: real restricted processes on Linux: each grant
  against the credential, the control socket, a second Session's files, the
  `tetral` executable, `.git` and the metadata names, symbolic links, path
  aliases and inherited descriptors, a missing deny path in a directory of
  another user, a rule for a regular file and a deny path under one, each
  of them again as `exec_command` calls of an App Server against its own
  paths and, where a probe reads or writes one file, as `view_image` and
  `edit` calls, and the cua-driver that `config.toml` records;
  `disabled` and `direct` against TCP, UDP and HTTP on the loopback and the
  LAN address, and a `disabled` task across 20 seconds of the clock while
  the heartbeats are ACKed and a `direct` task connects; each unusable
  `bwrap`, one that the test user could have planted among them, rejected
  before any process starts, by the harness and as a `tool.execute`; and the
  bundled `bwrap` with its digest.
