# tetral-tests

Tests of the workspace as a whole, and the harness of the cross-process
tests: those that run the `tetral` binary and the reference control server of
`cli/reference/` as separate processes, over real HTTP and SSE, on real
temporary directories. No workspace crate depends on this package.

## Design rules

- A cross-process test runs `binaries::tetral()`, built once per test process
  with `cargo build -p tetral --features test-seams --target-dir
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
- Each run of a test file of `tests/` writes its acceptance record into the
  directory that `TETRAL_ACCEPTANCE_DIR` names (`record`), and fails at once
  when it is unset. A case that needs a non-root user is recorded as not run,
  with its reason, when the test runs as root
  (`record::Case::runs_as_non_root`); no case skips silently.
- A test reads the one result object of a command's stdout (`run::Run`). A
  case that a command must not change compares a snapshot of the root's
  paths, kinds, owners, modes and bytes taken before and after it
  (`snapshot`).
- `S`, the directory of control sockets, is shared by every App Server of the
  user on the machine. A test that may meet `S` holds a shared lock on a file
  beside it for as long as it runs, and a test that replaces `S` holds that
  lock exclusively for as long as `S` is replaced and puts `S` back when it
  releases it (`socket_directory::ReplacedSocketDir`). A case that needs `S` owned by another user makes it as
  root with `sudo -n`; where that fails the case is recorded as not run, and
  under GitHub Actions the test fails instead. A state root owned by another
  user needs no privilege: such a case uses a directory that root already owns
  (`other_user::directory`).

## Where things are

- `src/lib.rs`: `workspace_dir` and `repository_root`, where the builds run.
- `src/binaries.rs`: the builds of `tetral` and the reference control server.
- `src/reference.rs`: `ReferenceServer`, the server as a process, its startup
  input, its controls and its request log.
- `src/run.rs`: `Tetral`, a command line of the `tetral` under test, and
  `Run`, its exit status, output and result object.
- `src/seams.rs`: `Seams`, the harness side of the seam protocol, which
  `state::seams` defines; a held barrier lets all pass at its release, or one
  at a time (`Seams::release_one`).
- `src/record.rs`: `Record`, `Case` and the `record!` macro.
- `src/snapshot.rs`: `snapshot`, every path under a directory with its kind,
  owner, mode and bytes.
- `src/socket_directory.rs`: the locks on `S` and its replacements for a case.
- `src/other_user.rs`: a directory that another user owns.
- `src/app_server.rs`: `AppServer`, a running `server run`, and `request`, one
  raw HTTP/1.1 request to the local API over a socket whose path it derives
  apart from the code under test (`socket_path`).
- `src/machine.rs`: `Machine`, a configured root and its App Server on its
  own seam clock against a reference control server that knows the test
  credential, the helpers that drive and read that server, and the seam
  counters of the Connection owner.
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
  an ended connection, the event size and protocol version, the startup
  phases in ready, and a redirect that is not followed.
- `tests/ledger_outbox.rs`: the replay ledger and the Report outbox against
  the reference control server: refused reports never sent again and a
  refusal that ends the connection, a report whose ACK was lost sent again
  byte for byte and recorded once, a Session instruction delivered again
  routed once and its `message_id` with other content a conflict, and the
  correlation of protocol errors.
- `tests/preparation.rs`: Session preparation against the reference control
  server: each failed check and what it creates or never creates, links into
  the state root and to a project, `create_cwd`, a repeated, held, retargeted
  and changed preparation and its ACK, two App Servers with the same
  Sessions, a removed directory and a new connection, a probe result after a
  new connection and a resumed stream, the probe slots, their timeouts and
  the rejection episode they record, 128 Session records, a held probe under
  heartbeats and shutdown, reports refused as answering no issued
  instruction, a new binding refused by a full replay ledger, and a
  preparation that names an earlier connection.
- `tests/local_service.rs`: `server run`, `server status` and `server stop`
  and the local API: status while Engine is unreachable, the wrong routes,
  shutdown on request, on each signal and with a handler blocked, one owner
  per root, restart after SIGKILL with the frozen reports deleted, 16 SDK
  clients at once, the socket's path,
  modes and hourly modification time, an unsafe `S`, every result and failure
  code with the root unchanged, a log file that cannot be opened, and no key
  in status.
- `tests/crate_graph.rs`: the rules of the crate graph, checked over the
  output of `cargo metadata`, and the rule that no library crate prints,
  checked over the library crates' sources.
- `tests/release_seams.rs`: a release build of `tetral` holds no test seam.
