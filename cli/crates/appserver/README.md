# appserver

The App Server process belongs here: the state-root lock, through which one
App Server at a time owns the state root; the local API server; startup and
shutdown; and the lifecycle task, which holds whether the App Server is running
or stopping, the shutdown sequence and the idle timer. Creating the
implementation of every interface between the peer crates (`connection`,
`session`, `execution`, `drive` and `mcp`) and starting each owner with the
ones it uses belong here too.

## Design rules

- Every task here that holds App Server state follows the owner rules of
  `cli/README.md`. The lifecycle task owns the server state; local API
  handlers and the signal task only send it messages. A status is read from
  the lifecycle task, the Connection owner, which owns the connection's
  fields, the records an earlier App Server left, which make `last_error`
  `cleanup_pending` while one is unconfirmed, and the fields that do not
  change while the App Server runs (`local_api::StatusSources`).
- The capabilities that ready advertises are assembled once at start from
  the providers this build has (`capabilities`). A `test-seams` build
  advertises as `tool_execution` the fragment in
  `TETRAL_TEST_TOOL_EXECUTION`; a value that does not decode as one fails
  the start.
- Startup runs its steps in order and a failure leaves every later step
  undone: the lock, the removal of the temporary files that interrupted
  replacements left, the configuration read under it, `outbox/` emptied, the
  entries of `T` deleted when `T` passes its checks
  (`execution::store::clear_task_parent`), the records of `execution/` read
  before the first connection, whose checks then go on every second
  (`execution::cleanup::Check`), the socket directory `S` created
  and checked, a socket left by an App Server that died removed, and
  the socket bound with mode `0600` before it accepts a connection
  (`startup`). A held lock is `Running` and touches nothing in `S`.
- Only a held lock or an answering socket shows that an App Server runs; a
  lock file, a socket file or a process ID alone shows nothing (`lock`).
- A local API handler reads only the state the owners report and never waits
  for Engine or touches the filesystem; `GET /v1/sessions` lists what the
  Session binding owners and the Connection owner's staging hold
  (`local_api`).
- The first of `POST /v1/shutdown`, SIGINT and SIGTERM starts the shutdown;
  later ones change nothing. The sequence stops the Connection owner, which
  ends every task and answers once each task's processes are confirmed
  gone, stops the listener, removes the socket and then releases the lock,
  waiting for the Connection owner and the handlers only until
  `SHUTDOWN_DEADLINE` after
  the trigger on the seam clock; it counts the start of its wait for the
  handlers as the test seam counter `shutdown.waiting` (`shutdown`).
- While it runs, the App Server sets the socket's modification time to now
  every `SOCKET_TOUCH_INTERVAL` of the seam clock and counts each as the test
  seam counter `socket.touched` (`startup::keep_socket_fresh`).

## Entry points

- `startup::lock(root)` and then `startup::serve(root, lock)` start an App
  Server and give `Started`; `lifecycle::run(started, fixed, times)` starts
  the Connection owner and its Report outbox with the Session binding
  owners' factory (`session::Factory`), given the state root as given and
  canonical and the running executable for the directory probe, and with
  `tasks::Supervisors`, which gives each admitted `exec_command` task its
  Task supervisor, serves the App Server until the shutdown sequence
  ends and gives `lifecycle::Ended`: the status as of then and the time left
  before the shutdown's deadline. `lifecycle::Times` holds the durations,
  each a constant but for tests.

## Where things are

- `lib.rs`: this documentation and the print lints that `cli/README.md` states.
- `lock.rs`: the state-root lock, `run/app-server.lock` held with `flock`
  without waiting (`acquire`, `RootLock`).
- `configuration.rs`: the configuration an App Server starts from, read
  under the lock and refused when the two files disagree (`read`).
- `startup.rs`: the start steps (`lock`, `serve`, `StartError`) and the
  socket's hourly modification time (`keep_socket_fresh`).
- `capabilities.rs`: the capabilities of ready (`assemble`).
- `lifecycle.rs`: the lifecycle task (`spawn`, `Lifecycle`) and `run`, which
  starts the Connection owner and joins the local API, the signals and the
  socket's freshness.
- `local_api.rs`: the routes (`router`), the status fields that do not change
  while the App Server runs (`FixedStatus`), the sources of a status
  (`StatusSources`) and `current_status`.
- `shutdown.rs`: the shutdown sequence and its deadline.
- `tasks.rs`: `Supervisors`, the Session binding owners' `TaskStarter`.
