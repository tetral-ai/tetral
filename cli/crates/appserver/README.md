# appserver

The App Server process belongs here: the state-root lock, through which one
App Server at a time owns the state root; the local API server; startup and
shutdown; and the lifecycle task, which holds whether the App Server is running
or stopping, the shutdown sequence and the idle timer. Creating the
implementation of every interface between the peer crates (`connection`,
`session`, `execution`, `drive` and `mcp`) and starting each owner with the
ones it uses belong here too.

## Design rules

- Startup resolves the running executable to one canonical path and shares
  it with capability checks, directory probes, policy translation and every
  tool supervisor (`Started::tetral`). The application or Controller installs
  an upgrade in a separate version directory and keeps the old executable
  and bundled helpers present and unchanged until every App Server using
  that version exits. Changing an external `current` symlink affects later
  starts; it does not redirect a running server's children. Restart from the
  new version to switch, then remove an old version only when unused. The
  CLI does not copy itself at startup or own an updater, and this contract
  does not support overwriting or unlinking a live version.
- Every task here that holds App Server state follows the owner rules of
  `cli/README.md`. The lifecycle task owns the server state; local API
  handlers and the signal task only send it messages. A status is read from
  the lifecycle task, the Connection owner, which owns the connection's
  fields, the MCP manager, which publishes `cua_driver`, the records an
  earlier App Server left, and the fields that do not change while the App
  Server runs (`local_api::Sources`). A blocked
  connection's error takes precedence over `cleanup_pending`; otherwise
  unconfirmed old processes take precedence over connection errors. Display
  priority never clears the restrictions owned by cleanup.
- The capabilities that ready advertises are assembled once at start from
  the providers this build has (`capabilities`). `drives.mount` is true
  when the mount probe at start succeeded (`drive::mount::probe`): on
  Linux the FUSE probe, which creates `run/fuse-probe` and mounts and
  unmounts an empty filesystem there; on macOS a recorded
  `drives_enabled = true` and a listening NFS server. Otherwise its reason
  (`no FUSE: <error>`, `drives not enabled` or `NFS server: <error>`) is the
  one every bound drive fails with, and the start goes on. `tool_execution` lists the
  tools whose startup checks pass: all three while the resolved `tetral`
  is an executable regular file, and `exec_command` only while
  `/bin/bash` or `/bin/sh` is executable; with none, it lists no tool and
  only the control `cancel`. Every ready carries `local_mcp`, whether or not
  a local server runs, since the tool-list reports alone say which local
  tools this computer offers. A `test-seams` build advertises instead the
  fragment in `TETRAL_TEST_TOOL_EXECUTION` when it is set; a value that does
  not decode as one beside `local_mcp` fails the start.
- Startup runs its steps in order and a failure leaves every later step
  undone: the lock, the removal of the temporary files that interrupted
  replacements left, the configuration read under it, the
  entries of `T` deleted when `T` passes its checks
  (`execution::store::clear_task_parent`), the records of `execution/` read
  before the first connection, whose checks then go on every second
  (`execution::cleanup::Check`), the socket directory `S` created
  and checked, a socket left by an App Server that died removed, and
  the socket bound with mode `0600` before it accepts a connection
  (`startup`). A held lock is `Running` and touches nothing in `S`.
  `SocketOwnership` removes this process's socket before releasing its root
  lock on normal shutdown and on setup failure; the listener closes first.
  No report directory is created or reset. Signal handlers are installed
  before actors and the local listener start. Before installation, signals
  keep their system default behavior.
- Only a held lock or an answering socket shows that an App Server runs; a
  lock file, a socket file or a process ID alone shows nothing (`lock`).
- A local API handler reads only the state the owners report and never waits
  for Engine or touches the filesystem, except `POST /v1/computer-use`,
  which checks its body, answering `400` with `invalid_arguments` for one it
  cannot take, and then waits for the MCP manager (`mcp`) to record the
  change in `config.toml` and apply it; a `config.toml` that is missing or
  does not parse gets `500` with `invalid_configuration`, and an
  operating-system error on it `500` with `io_failed` naming the path, both
  changing nothing; a request still waiting when the shutdown starts gets
  no answer, its connection closing so that the client finds no App Server;
  `GET /v1/sessions` lists what the Session binding owners and the
  Connection owner's staging hold, and `GET /v1/metrics` the local metrics
  in the text format 0.0.4 (`observe::render`), with
  `tetral_startup_phase_seconds` set from the phases of `observe::STARTUP`
  once a ready was accepted (`local_api`).
- The first of `POST /v1/shutdown`, SIGINT and SIGTERM starts the shutdown;
  later ones change nothing. The sequence stops the Connection owner, which
  ends every task and answers once each task's processes are confirmed
  gone, and at the same time the MCP manager, which stops every local
  server's processes, then stops the listener, removes the socket and then releases the lock,
  waiting for the Connection owner, the MCP manager and the handlers only
  until
  `SHUTDOWN_DEADLINE` after
  the trigger on the seam clock; it counts the start of its wait for the
  handlers as the test seam counter `shutdown.waiting` (`shutdown`). Every
  stage shares that deadline; late listener registration stops immediately.
  At the deadline the server stops waiting, closes the listener and completes
  socket/lock cleanup. Its final status uses the connection's published
  snapshot, so it does not start another owner request after the deadline.
  Unconfirmed task records remain for the next start; the deadline does not
  prove all processes have been reaped. Log flushing receives the remaining
  budget and has its own one-second maximum.
- With `--idle-exit`, the idle timer keeps the idle start: the App
  Server's start, moved to the arrival of every `tool.execute`,
  `tool.stdin`, `tool.cancel` and `mcp.call` the Connection owner reads and
  to the exit of every `exec_command` command and the end of every local MCP
  call, which the Connection owner, each Task supervisor and the MCP manager
  report through `protocol::ports::ActivitySink`. Session preparation starts
  and ends are counted separately through that same sink. The idle time
  counts only while no command, MCP call or preparation work runs; each
  completion restarts a full interval. Duplicate preparations and
  report ACK waits add no busy interval. When it reaches the
  duration, every Session's drive sync uploads and commits its pending
  changes as a final upload in keep mode with one deadline captured 30 seconds
  after admission, before collecting Session state, and then
  the shutdown above starts. A tool instruction or newly started preparation
  received before that upload ends releases it, so its uploads and commits go on as ordinary ones,
  and moves the idle start; the timer handles its messages in arrival
  order. The test seam barrier `idle.uploading` holds the upload's end and
  the counter `idle.checked` counts each reading of the clock and each
  message (`idle`, run by the lifecycle task). Without the option no timer
  runs. A completed idle exit emits `server.idle_exit` with the instance,
  current credential and held connection identities; a withdrawn exit does
  not. Owner tests compose this timer with real Drive syncs and SQLite to
  verify committed bytes, deadline retention and transfer continuation.
- While it runs, the App Server sets the socket's modification time to now
  every `SOCKET_TOUCH_INTERVAL` of the seam clock and counts each as the test
  seam counter `socket.touched` (`startup::keep_socket_fresh`).

## Entry points

- `startup::lock(root)` and then `startup::serve(root, lock)` start an App
  Server and give `Started`; `lifecycle::run(started, fixed, times,
  idle_exit)` starts the Connection owner and its Report outbox with the
  Session binding owners' factory (`session::Factory`), given the state root
  as given and canonical and the running executable for the directory probe,
  and with `tasks::Supervisors`, which gives each admitted `exec_command`
  task its Task supervisor, the Content cache owner (`drive::cache`)
  with the connection's drive transfer client, and the MCP manager (`mcp`),
  which the Connection owner tells of each connection that comes online or
  ends, which the Session binding owners hand each `mcp.call` and whose
  recorded driver names their tasks' trusted programs, and which starts
  Computer Use when `config.toml` records it enabled, before any instruction
  is admitted, beside the connection and without delaying it; serves the App
  Server until the shutdown sequence ends and gives `lifecycle::Ended`: the
  status as of then and the time left before the shutdown's deadline.
  `lifecycle::Times` holds the durations, including the idle final-upload
  budget, each a constant but for tests.

## Where things are

- `lib.rs`: this documentation and the print lints that `cli/README.md` states.
- `lock.rs`: the state-root lock, `run/app-server.lock` held with `flock`
  without waiting (`acquire`, `RootLock`).
- `configuration.rs`: the configuration an App Server starts from, read
  under the lock and refused when the two files disagree (`read`).
- `startup.rs`: the start steps (`lock`, `serve`, `StartError`) and the
  socket's hourly modification time (`keep_socket_fresh`).
- `capabilities.rs`: the capabilities of ready (`assemble`) and the startup
  checks of tool execution.
- `lifecycle.rs`: the lifecycle task (`spawn`, `Lifecycle`) and `run`, which
  starts the Connection owner, the Content cache owner and the MCP manager
  and joins the local API, the signals and the socket's freshness.
- `local_api.rs`: the routes (`router`) and their server (`serve`), which
  closes a connection without writing a response the handler marks
  `NoAnswer`, the status fields that do not change while the App Server
  runs (`FixedStatus`), what the handlers read and reach (`Sources`),
  `current_status` and the body check of `POST /v1/computer-use`.
- `shutdown.rs`: the shutdown sequence and its deadline.
- `idle.rs`: the idle timer and the upload before the exit, which the
  lifecycle task runs (`IdleExit`), and its `ActivitySink` on `Lifecycle`.
- `tasks.rs`: `Supervisors`, the Session binding owners' `TaskStarter`.

The local metrics composition test sends a real HTTP upload through
`HttpDriveTransfer` and `DriveSync`, returns one `429 rate_limited`, and reads
its rejection count through `/v1/metrics`. It verifies retry and commit with
one increment. This fixture covers CLI wiring; Engine admission and workspace
limits remain cross-process integration responsibilities.
