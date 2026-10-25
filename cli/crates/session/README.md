# session

The Session binding owners belong here, one task per Session on this machine,
each the owner of its Session's record, bindings and their states and of the
admission of new work; and the directory probe and repository clones.

A **Session record** is what this machine keeps about one Session from the
first `session.prepare` its owner handles until a close deletes its local
data: at most one binding on the current connection and the binding it had on
an ended connection, `unavailable` until a new one replaces it. A **binding**
is in one of the states `preparing`, `prepared_pending_ack`, `prepared`,
`failed`, `closing` and `unavailable`, with its requested and canonical `cwd`
and its latest preparation error. A **preparation operation** is one attempt
to prepare a binding; each has its own number, and a result of an operation
that is no longer the binding's is discarded.

## Design rules

- A `session.close` and a command for the same binding are ordered by the
  arrival order of the owner's mailbox: a command starts only in the
  Session's current binding before its close is handled
  (`SessionOwner::admits`), so a command handled after the close never
  starts and one handled before it has already started; a close of another
  binding changes nothing for it.
- An owner starts admitted commands only from its handler, through the
  `TaskStarter` it is given, so a start never races its binding's close.
- Owners follow the owner rules of `cli/README.md`. An owner reaches the
  Connection owner only through `protocol::ports::ConnectionPort` and hands
  its reports to `protocol::ports::ReportSink`.
- A preparation's result is committed only while its connection, binding and
  operation are all still current (`prepare`). A binding enters `prepared`
  only on the ACK of a report of its result (`SessionOwner::report_outcome`).
- The cases of a `session.prepare` against the Session's binding are one
  decision (`cases::decide`).
- The App Server never inspects a target directory before a probe and never
  changes its own working directory: a `cwd` at or below the state root,
  compared lexically, fails before any filesystem access
  (`prepare::inside_state_root`); everything else is checked by the directory
  probe, a child process of the running executable (`probe_child`).
- A probe is spawned only with a probe slot of the Connection owner, which it
  holds until it is reaped. Its task alone kills its process group, and only
  before reaping it (`probe`). The owner reads the seam clock for the probe's
  `PROBE_LIMIT` whenever it wakes and wakes at least once per recheck interval
  while a probe runs; it acts on a passed limit before the message it woke
  for, so a result taken after the limit is discarded (`owner`).

## Entry points

- `spawn(starter)`: an owner that only admits commands.
- `Factory::new(shared, starter)`: the `protocol::ports::SessionFactory` that
  creates an owner per Session, with the state root's paths and the running
  executable (`RootPaths`) and its durations (`Times`).
- `probe_child::run(input)`: the probe's whole work, from its stdin to its
  one line of stdout, for `tetral` run as `probe_child::PROBE_NAME`.

## Where things are

- `owner.rs`: `spawn`, `Factory`, `SessionMessage`, `SessionMailbox`, the
  owner's loop and state, the admission of commands (`Admission`,
  `TaskStart`, `TaskStarter`), and a close, a connection's end and the
  shutdown as they end a preparation.
- `record.rs`: `Record`, `Binding` and the entries of `GET /v1/sessions`.
- `cases.rs`: the cases of a `session.prepare` (`Case`, `decide`).
- `prepare.rs`: the steps of a preparation, from the connection check to the
  report and its ACK.
- `probe.rs`: the spawn and supervision of one probe (`spawn`, `Probe`,
  `PROBE_LIMIT`).
- `probe_child.rs`: the probe's checks and the creation of a missing `cwd`.
