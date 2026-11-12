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
  `binaries::reference_server()`, built with `go build` inside
  `cli/reference`, its own Go module. It never runs a binary of the target directory that `cargo test`
  itself holds. All nested fixture Cargo builds use `binaries::cargo_command`,
  which removes the harness's inherited `CARGO_MANIFEST_DIR`, `CARGO_PKG_NAME`
  and four version-component variables. `ring` tracks these incoming values;
  removing them preserves reuse of direct builds while Cargo supplies each
  package's own metadata. Toolchain selection and user build settings remain
  inherited.
- Repository preparation process cases run on Linux. Both Linux and macOS
  support preparation isolation; these cases cover ordinary configured Git,
  protected resources, mount availability and the complete child lifetime.
  The Linux runs do not certify automatic repository preparation on macOS.
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
- The stand-ins give only what Engine can give. The reference control server
  refuses an instruction or event that fails the protocol's decoder; a test
  that makes one invalid on purpose issues it inside
  `ReferenceServer::outside_contract`. A test's in-process stand-in for the
  drive transfer is wrapped in `drive::transfer::Checked`, which panics on an
  answer that the HTTP client would refuse.
- A test of real restricted processes runs `tetral` with `argv[0]`
  `tetral-isolation-harness`, which starts one command through the
  isolation's check and launch and answers with its outcome, and observes
  from outside every sandbox what the command reached. On Linux it needs a
  host that allows unprivileged user namespaces and bubblewrap 0.13.0 or
  later as `bwrap` on `/usr/bin` or `/bin` (`scripts/install-bubblewrap.sh`
  installs it); on macOS, `/usr/bin/sandbox-exec` and `python3`.
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
- `src/git_server.rs`: `GitServer`, a loopback Git smart-HTTP server over
  HTTPS with a test certificate that stands in for git-proxy: it serves only
  `/github.com/<owner>/<repo>`, accepts only the current tickets the
  reference control server issued (`ReferenceServer::current_tickets`),
  records each request, answers a clone with a fault and holds a pack
  response until released.
- `src/sessions.rs`: issuing `session.prepare`, the reports that answer it,
  the App Server's Sessions and a new connection between preparations.
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
- `src/small_volume.rs`: `on_small_volume`, which runs a test with a
  directory on an empty volume of 7 GiB: on Linux the test runs again as the
  root of a user and mount namespace of its own that holds a tmpfs, on a Mac
  it attaches a sparse disk image; and `free_space`, the volume's free bytes
  as the App Server reads them.
- `src/other_user.rs`: a directory that another user owns, `sudo -n` under
  the rule above, and `RootDirectory`, a directory that root makes in
  `/tmp`.
- `src/app_server.rs`: `AppServer`, a running `server run`, started under a
  soft limit of 256 open descriptors that the process sets and reads back
  before it becomes `tetral`, which on macOS,
  when dropped while it runs, also kills the process groups its task records
  name, since no task dies with it there; and `request`, one raw HTTP/1.1
  request to the local API over a socket whose path it derives apart from
  the code under test (`socket_path`).
- `src/machine.rs`: `Machine`, a configured root and its App Server on its
  own seam clock against a reference control server that knows the test
  credential, the helpers that drive and read that server, and the seam
  counters of the Connection owner; `Machine::start_with_tools` gives the App
  Server the protocol's `tool_execution` fixture to advertise, through the
  test seam `TETRAL_TEST_TOOL_EXECUTION`, and the system `PATH` for its
  tasks, and `Machine::start_with_tools_from` another `tetral` build and
  `PATH`; `Machine::start_with` adds variables to the App Server's
  environment; `Machine::start_under` runs the App Server under another program,
  such as `bwrap` with mounts that change what it finds; `task_parent`
  locates a root's `T` apart from the code under test.
- `src/tasks.rs`: `TaskMachine`, such a machine with one prepared Session,
  also on another `tetral` build and `PATH` (`TaskMachine::start_from`),
  with more environment (`TaskMachine::start_with_environment`, and
  `start_prepared_with_environment` with `server run` arguments and a step
  on the root before the App Server starts) or with the platform's isolation unusable
  (`TaskMachine::start_without_isolation`), which
  issues tool instructions, `mcp.call` and controls, reads their reports and
  replies, prepares a binding whose `prepared` ACK is withheld, fills the
  replay ledger, resumes the stream, follows the seams of a task's start,
  initial wait, output, writes, waiting replies, termination steps, observed
  exit and the checks of an ended task's processes, moves the App Server's
  clock in steps that keep the connection online, also until something
  happens (`step`, `advance_until`) or together with the reference control
  server's clock (`step_together`), ends the connection and waits for the
  next (`end_connection`, `reconnect`), starts the App Server again after
  it exited (`start_again`), waits for the App
  Server's exit after a shutdown, and removes `T` when it ends; and the
  processes of the machine as `/proc` on Linux or `ps` on macOS lists them,
  apart from the code under test.
- `src/drives.rs`: `DriveMachine`, a `TaskMachine` whose prepared binding
  holds drives on real mounts under a directory of its own (in the host's
  memory for `start_seeded_in_memory`, where Linux has `/dev/shm`, or under
  a directory the test names for `start_seeded_under`), also
  with more `server run` arguments, or on a machine that cannot mount until
  its App Server starts again (`start_unable_to_mount`), unmounted
  when it ends, which runs commands in the binding and reads their drive sync
  outcome, in the foreground or going on in the background; another writer's
  commits, a drive's state, its requests and its accepted commits on the
  reference control server; the seam counter of the final uploads begun and
  the seam barrier before a commit's accepted copies are verified.
- `src/cases.rs`: the cases of one direction of
  `protocol/behaviour-v1.cases.json`, and `Bound`, the matching of a message
  against what a case writes, with the references and matchers that the
  CLI-direction cases use. A message the CLI sent binds no reference.
- `src/isolation.rs`: the isolation suites' shared world of canonical
  directories, run through the isolation harness (`World`) or as tool calls
  of an App Server against its own paths (`Served`), and the probe tables and
  network cases that both platforms run.
- `src/mcp.rs`: `TestDriver`, the test MCP server as a driver: its scenario,
  which a test may replace for the processes started after, the record of
  each of its processes (what it started with, the messages it received
  with their arrival times, the calls it answered) and their control
  sockets; `online_with`, an App Server with that driver; `tetral
  computer-use enable` and `disable` against a machine, and
  `record_before_start` on a root no App Server owns.
- `src/bin/mcp-test-server.rs`: the test MCP server, a scriptable stdio MCP
  server run as `<driver> mcp`: its `initialize`, its `tools/list` pages and
  how it answers each `tools/call`, held at barriers, never answered, by an
  exit, with an effect file, and with media of given sizes; run as
  `<driver> --version`, the line it prints and its exit status, or no exit
  until its parent process ends.
- `src/bin/failing-launcher.rs`: a `bwrap` that launches nothing, writing the
  file `stderr` beside it to stderr and exiting 1.
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
  its limit (with the preparations a server sends after it, 70 prepared on
  one connection), a stream lost before that ACK, grants from first sends,
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
- `tests/renewal.rs`: credential renewal against the reference control
  server: a connection that ends before the fetch, fetches answered
  `404 not_found` or with a malformed key, a credential that cannot be
  written, a restart right after the write, and reports in flight, held
  back or submitted across the resume with the new credential, and bounded
  owner wakes while a due heartbeat retry waits for the switch. The old-key
  refusal is released before the CLI applies the new connected event. Distinct
  credential expiry, connection end during switching, a new connection returned
  by a resume, and a notice received during an in-flight fetch are covered.
- `tests/close_drives.rs`: Session close with real FUSE writes, final-upload
  waits, connection loss and late close, including return of running and queued
  upload capacity so another Session can progress.
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
- `tests/repositories.rs`: a cloud computer's repositories against the
  reference control server and `GitServer` with real `git`: repository paths
  in the state root, a refused or empty ticket, a clone of a default branch,
  a branch and a commit, a missing branch, an existing folder, a reuse with
  the new ticket, unchanged global Git configuration, the cause of each
  failed clone and its message's limit, a repository inside one whose clone
  failed (no Git work, and both cloned at the next preparation), a user
  computer that runs no `git`,
  and a clone that finishes after its connection ended without starting the
  next repository. It also covers cwd inside a new checkout, failed-checkout
  retry, owned staging recovery, refreshed proxy and identity configuration,
  preparation activity through idle exit, the real Git low-speed timeout,
  tool-written hooks/filters/helpers under isolation, live Drive permissions,
  the record of a launcher that exits before Git runs, and complete Git
  descendant cleanup on stop and parent death.
  The low-speed process case sets `TETRAL_TEST_GIT_LOW_SPEED_SECONDS` on the
  `test-seams` executable to construct Session timing with a shorter window;
  it still exercises real paced/stalled Git HTTP responses. Production does
  not read this variable; the release-seam test checks its absence.
- `tests/idle_exit.rs`: duration units, duplicate activity, cancellation and
  the single idle-exit record. Real pending Drive uploads, the shared deadline
  before Session collection and activity withdrawal are composed with the
  actual owners in `appserver/src/idle/drive_tests.rs`.
- `tests/drive_mounts.rs`, `tests/session_drives.rs` and
  `tests/drive_sync_outcome.rs`: capability advertisement, mount and binding
  reports, Drive environment removal and tool-result fitting, and
  `fusermount3`'s output kept off the App Server's stdout and stderr. Drive
  and Session owner tests separately exercise held transfers and
  tool-admission ordering; real FUSE (Linux) and NFS (macOS) tests in the
  drive crate own kernel file and handle behavior.
- `tests/drive_transfer.rs`, `tests/drive_files.rs`,
  `tests/drive_bindings.rs` and `tests/drive_sync.rs`: drive transfer of the
  real App Server against the reference control server, through real mounts:
  uploads and the commit of one command's writes, what a command dispatched
  after a commit reads, also after a resume; file names, the executable bit,
  empty folders, commit bases and thousands of deletions; a binding's
  access, added, removed and changed drives, also after a kill and a failed
  preparation, excluded paths and Git repositories in a drive; and
  what a command gets when another writer deletes or writes the same file,
  or an upload is refused, while it syncs.
- `tests/behaviour_cases.rs`: every CLI-direction case of
  `protocol/behaviour-v1.cases.json` against `tetral server run`: the
  instruction the reference control server sent and the requests `tetral`
  sent for it, in order, held to the case. A case, state, trigger or matcher
  it does not implement fails it. A connection given with `mount: false` or
  without `tools` runs on a machine that may advertise either; the file's
  orientation says why that proves the same.
- `tests/drive_downloads.rs`: what a command's read gets when its file's
  download stalls (the running result lists it, and the rest is asked for
  after ten seconds of the App Server's clock), is answered `503` (tried
  again until it passes) or is refused as missing or denied (an I/O error
  with the clock standing); two Sessions that read one file of 64 MiB
  download and store it once. On a Mac: eight greps at a time find in 20,000
  files of a mount the 400 lines they find in a local copy; a read whose
  download takes 190 seconds, each second of both clocks a second of the
  wall clock, ends with the content and no error, and of two reads waiting
  through 20 seconds in which the App Server is stopped the cancelled one's
  process ends and the other reads its file.
- `tests/drive_uploads.rs`: a background write is uploaded two seconds of the
  App Server's clock after it and committed with no tool call (on a Mac at
  the next sync point); a result waits 30 seconds for uploads answered `503`
  and lists the file `uploading`, the next result does not wait, and the
  file is committed once uploads pass; an upload refused `400`, a commit
  answered by a `200` that fails its schema and an upload held past the wait
  and then refused are each listed `upload_pending`, logged as a fault and
  tried again only at the next sync point; a commit whose answer is lost is
  sent again unchanged before the commit of a rewrite made meanwhile; a
  removed drive's `applied` report comes 30 seconds after the instruction
  with the change whose upload never ended in `lost_changes`.
- `tests/drive_upload_limits.rs`: a file of 500,000,001 bytes, whose upload
  is answered `413`, is listed rejected for `limit` with the server's message
  as `rule`, while one of 500,000,000 bytes is committed and counted by a
  result whose commit time passed between the commit's answer and the
  verification of the accepted copy, which the test holds at its seam; an
  upload the server reads slower than a result waits is listed `uploading`,
  holds no later command and is committed alone once it ends; an upload the
  workspace's budget refuses with `429` is listed `uploading` and sent again
  with the last second of its `Retry-After`; 9,990 deletions and 11 writes
  at one sync point go in a commit of 10,000 changes, the deletions first and
  each kind in path order, and a next one of the last write.
- `tests/drive_idle_exit.rs`: under `--idle-exit 1m` a file a process
  outside the CLI wrote after the last command is committed before the App
  Server exits, after the 60th second; with its upload held the App Server
  exits with the 30th second of its final upload without a commit, and the
  next start commits the file once the Session is prepared again.
- `tests/drive_lost_changes.rs`: a close that reaches a restarted App Server
  whose Session holds changes not uploaded is answered `closed` with each
  drive's `lost_changes`, at most 100 paths in byte order with the count of
  all, no upload is sent and the Session's data is deleted; with paths of
  1,000 bytes the report lists as many as fit in 65,536 bytes; a close of a
  Session with a command that ignores its end signal stays `closing` until
  the 30th second after it and then reports the change whose upload the
  server held as lost, and not the one whose commit was rejected for another
  writer's version; the lost change of a removed drive whose report was never
  accepted comes with the ready of the App Server started again after a kill
  and with no later report or ready; a close's lost change whose report was
  never accepted is listed by each next connection's ready until one is
  acknowledged, and a close sent then is answered without it; a close whose
  connection ends in its final upload leaves the change to the close sent on
  the next connection.
- `tests/drive_mount_points.rs`: after a kill of the App Server the next
  start mounts over a stale mount and over a mount point that holds a file,
  and reports a mount point replaced by a file as a mount failure in its
  preparation, once for the state root; a drive unmounted outside the CLI is
  mounted again before the next command, and one whose mount point is then
  blocked is listed `unmounted` by one result only; each Session's commands
  see its own drives under `TETRAL_DRIVES`.
- `tests/drive_session_data.rs`: a Session's data stays through a minute
  idle, a kill of the App Server and its connection's end, each new
  binding's command reading a file with no second download; changes written
  outside a command then wait for the close, which commits the new file,
  leaves another writer's version of the rewritten one and deletes the
  data. A file listed `uploading` while uploads fail holds no command of the
  next connection or of the App Server started after a kill, and is
  committed once uploads pass; a commit whose answer was lost before a kill
  is sent again unchanged at the next start, before the commit of a rewrite
  made meanwhile. On Linux, a connection's end fails an `O_DIRECT` read and
  a block write that wait for a download held after 1,052,673 bytes, and a
  new read, with `EIO`; the next preparation starts no download and the next
  read asks for the rest with `Range`.
- `tests/drive_cache_eviction.rs`: on a state root whose volume of 7 GiB
  is filled until 96 MiB less than 5 GiB is free, the cache keeps three
  files of 64 MiB while each was read within the last minute; once every
  read is more than a minute ago, the 10th second of the App Server's clock
  deletes the two read first and keeps the last, which a read then finds
  without a download and whose read keeps it (Linux); filled until 256 MiB
  less is free, every committed content is deleted and downloaded again at
  its next read. A change no command commits stays as written throughout.
  The test sets the files' last read times itself.
- `tests/drive_no_mount.rs`: on a machine that cannot mount (a Mac without
  `tetral drives enable`, Linux whose `fusermount3` fails as it does without
  `/dev/fuse`) ready says so, a preparation succeeds with each drive as a
  mount failure and `TETRAL_DRIVES` is unset in the Session's commands; the
  App Server started again once the machine can mount says so in its ready,
  and the Session prepared again mounts both drives with no failure.
- `tests/drive_catch_up.rs`: a drive whose file listing is refused and whose
  change listing then fails its schema stays unmounted beside the Session's
  other drive until they pass; a first mount of three pages holds the commits
  another writer made between them (a random sequence, its seed in the
  acceptance record); a catch-up passes the record of a removed version, and
  an expired cursor makes the machine list the drive again, keeping a change
  not yet uploaded and its cached content.
- `tests/drives_enable.rs`: `drives enable` while an App Server owns the
  root; on Linux the unchanged root; on a Mac the allowed, denied
  (`TETRAL_TEST_CHECK_READ=denied`) and signalled (`never`) test reads and a
  `mount` that writes output and exits 32 (`TETRAL_TEST_MOUNT`), each leaving
  no mount and only the allowed read changing `config.toml`, the failing
  `mount`'s output off the command's stdout and stderr.
- `tests/local_deadlines.rs`: diagnostic publication, rotated logs and the
  shared local-request deadline, including a successful response just before
  it. `appserver/src/local_api/transfer_tests.rs` composes a real HTTP upload,
  DriveSync retry/commit and the local metrics route to observe an upload 429.
  It does not certify Engine workspace-budget enforcement.
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
  or freed by each ACK, also of an instruction that joined a task. A final
  ACK applied by the Connection owner after a resume retains the earlier
  stream's entry: replay starts nothing twice, a full ledger refuses a new
  task, and the next resume admits one. The tests also cover a
  running task's write and cancel with no entry free, which leave it
  unchanged, and, with only the ledger's reserve free, new tasks
  refused, as one `pressure.rejecting_started` of `tetral_tasks` in the App
  Server's log, while a running task's controls, its binding's close and
  its report are recorded. Five thousand tasks on one stream, each issued
  after the previous one's terminal ACK, are all admitted; that case runs
  on Linux only, since the admission is the same on macOS, where its
  Seatbelt launches of the debug build would take most of the run.
- `tests/computer_use_commands.rs`: `tetral computer-use enable` and
  `disable` without and with a running App Server: the record each writes,
  failure codes and their configuration effects, the wait behind a held lock,
  `POST /v1/computer-use` refusing a malformed body, the test MCP server
  started and stopped while the App Server keeps its process and connection,
  and the driver's version in `GET /v1/status` at a start and at enables,
  with the warning for an untested or unread one and the version read's
  exit-status rule and bound. Shutdown gives active and queued setting requests
  explicit retry errors: the active setting may already be written, and the
  queued one remains unwritten. Neither this error nor a post-connect EOF
  invokes the offline root-lock write path; explicit reapplication after
  restart succeeds.
- `tests/mcp_servers.rs`: local MCP servers with the test MCP server as
  Computer Use's driver: the tool-list reports, their listing, start and
  limits, the ready capability, the server's messages and its processes. The
  local listing byte bound is checked before a valid next cursor is requested.
- `tests/mcp_calls.rs`: `mcp.call` with the test MCP server as Computer Use's
  driver: admission before any `tools/call`, the one `tools/call` and one
  report of each call, results, their media and error bounds,
  cancellation and the 300-second timeout, an idle exit that waits for a
  call answered after 90 seconds, a result that does not fit the report
  budget filled by withheld ACKs beside another binding's running command
  (about 270 MB of reports), a resume, a resend and an ended
  connection, a closed binding beside another, a disable during a call, a
  server exit, a binding connection's opening that its waiting calls share,
  one that cannot open, and a stdout line over its bound. Public metrics count
  unfinished admitted MCP calls beside supervised work even without idle exit.
  Opening time belongs to the queue phase; the run phase and call deadline
  start at the write. Refused images do not consume the accepted-media budget.
- `tests/cua_driver.rs`: Computer Use with the pinned cua-driver release on
  Linux and macOS, ignored by default and run one test at a time with
  `cargo test --test cua_driver -- --ignored --test-threads=1` and
  `TETRAL_TEST_CUA_DRIVER` naming the release's executable, failing
  without it: an unattended start whose tool list equals a direct listing
  of the release and whose status shows the tested version; a driver process per binding, two bindings' concurrent
  typing into one process, each answer the driver's own, and a binding's
  close ending only its process (on Linux a desktop session and `xmessage`,
  where both calls succeed; on macOS the installed `CuaDriver.app` with its
  grants and TextEdit, where the release types one call and refuses the
  other with `input_busy`); on
  macOS a disable during typing that cancels the call, rejects the next
  and keeps the typed text; and on Linux the driver's own answers without
  a desktop.
- `tests/capability.rs`: the `tool_execution` that ready advertises from
  the App Server's own startup checks, with all tools and without a shell.
- `tests/versioned_launch.rs`: on Linux, a normal `current` symlink switch
  between two intact version directories. After the switch, the old server
  prepares a Session and runs all three tools; existing barriers and `/proc`
  show its actual tool entries and bundled bubblewrap still come from the
  old directory. After orderly shutdown, a server started through `current`
  uses the new directory. Both servers advertise their normal capabilities;
  no live executable or helper is overwritten or removed.
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
  directory and its errors, the environment, the descriptor limit the App
  Server raised, the exit facts, the end of a task's processes, the metrics
  and the trace context.
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
  grace while a process stays blocked on a FUSE mount (Linux), and at the end of
  the wait after the SIGKILL when the task's exit is never observed, a
  shutdown that ends a running task through its TERM trap before the App
  Server exits, a connection's end that discards staged tasks and a task
  whose availability check is held and kills running ones at once, resumes
  that keep a task running,
  the deadline of a withheld resume and of a clock step, an old
  connection's waiter and output after a new one, processes blocked on a
  FUSE mount that keep their Session `cleanup_pending` while others
  prepare (Linux), a read blocked on an NFS hard mount that SIGKILL ends
  with its group (macOS, where the kernel ends the NFS wait of a killed
  thread), and the records an App Server killed with SIGKILL leaves, also
  one of another boot, on Linux one whose namespace inode a later namespace
  took and one whose first process still runs, and, on macOS, ones whose
  leader ID or group a later process or another user's group took.
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
- `tests/release_archive.rs`: a release archive that `scripts/package.sh`
  wrote, named by `TETRAL_TEST_ARCHIVE`, with the digest the script printed
  in `TETRAL_TEST_ARCHIVE_SHA256`: its entries and their modes, the digest
  check passing and failing with one byte flipped, complete Rust licence bodies
  obtained independently of the rendering template, exact pinned Codex/Moka/musl
  notices and target package inventory, and its `tetral` reaching
  `online` against the reference control server with the workspace's
  version; on Linux its `tetral` and `bwrap` have no `PT_INTERP` and no
  `DT_NEEDED`, an `exec_command` runs under the archive's `bwrap` with no
  `bwrap` on `PATH`, and with one byte of that `bwrap` changed the next is
  rejected `sandbox_unavailable`. Ignored by default; run it with
  `cargo test --test release_archive -- --ignored` on a machine of the
  archive's target.
- `tests/isolation_linux.rs`: real restricted processes on Linux: each grant
  against the credential, the control socket, a second Session's files, the
  `tetral` executable, `.git` and the metadata names, symbolic links, path
  aliases and inherited descriptors, a missing deny path in a directory of
  another user, a rule for a regular file and a deny path under one, each
  of them again as `exec_command` calls of an App Server against its own
  paths and, where a probe reads or writes one file, as `view_image` and
  `edit` calls, and the cua-driver that `config.toml` records, also by an
  enable on the running App Server;
  `disabled` and `direct` against TCP, UDP and HTTP on the loopback and the
  LAN address, and a `disabled` task across 20 seconds of the clock while
  the heartbeats are ACKed and a `direct` task connects; each unusable
  `bwrap`, one that the test user could have planted among them, rejected
  before any process starts, by the harness and as a `tool.execute`; and the
  bundled `bwrap` with its digest.
- `tests/isolation_macos.rs`: real restricted processes on macOS under
  `sandbox-exec`: the shared probe tables by the harness and as calls of an
  App Server; under `direct`, Unix-socket connections to the control socket
  and to another task's socket refused while a socket in the task's own
  temporary directory works; with drives enabled, under `direct`, a TCP
  connection to the NFS server refused while another loopback listener and
  the Session's own mounted drive work; the shared network cases; and a
  `sandbox-exec` that cannot apply a profile rejecting the call before any
  process starts.
