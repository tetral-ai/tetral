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
  arrival order of the owner's mailbox: a command starts only in a binding
  that is `prepared` or `prepared_pending_ack`, so a command handled after
  the close is rejected `binding_unavailable` and one handled before it has
  already started or is staged, which the close prevents; a close of another
  binding changes nothing for it.
- A `tool.execute` is admitted in fixed steps, the first failing one
  deciding its `rejected` report, before any side effect: its task identity
  in the binding's registry (an equal payload joins the task, another is
  `task_conflict`), the replay ledger's room, its schema, budgets and tool
  against the ready's list, its policy, its binding. Step 3 judges
  everything but the policy, which step 4 judges by `execution::policy`,
  translated with the paths of a task name chosen just before it
  (`execution::store`); only a registered task keeps its name (`tools`).
- A task admitted while its binding waits for the ACK of its `prepared`
  report waits in the binding's staging queue; that ACK starts the queue in
  arrival order before the binding's next instruction, and a close, a cancel
  of the task or the connection's end prevents a staged start; after a close
  or a cancel the task's supervisor makes its `terminal` report (`tools`).
- A control finds its task only in the registry of the binding it names on
  its own connection; an absent task is `task_not_found`, a `tool.stdin` of
  a task that is not an `exec_command` is `unsupported_control`, a
  `tool.cancel` of a staged task takes it out of the staging queue, and
  every other control goes to the task's supervisor (`tools`).
- An owner starts admitted commands only from its handler, through the
  `TaskStarter` it is given, so a start never races its binding's close: the
  starter gives each admitted task its supervisor at admission, `staged`,
  and the owner starts it once its binding is `prepared`. A task carries
  its directory under its Session and its temporary directory, both named
  from the task's name chosen at admission, and the Session's Report outbox
  port (`tools::TaskStart`).
- A task's supervisor reports its PID namespace and its confirmed end to
  the owner. A connection's end sends every task of its binding the end
  (staged ones never start, running ones end at once) and keeps each started
  task whose processes are not yet confirmed gone; while one is kept, or a
  record an earlier App Server left for the Session is unconfirmed
  (`execution::cleanup::Leftovers`), a preparation of the Session fails with
  `cleanup_pending`, checked right after the connection it names. The
  shutdown sends every task the end through the termination procedure, and
  once each started task's processes are confirmed gone the owner tells the
  Connection owner (`ConnectionPort::tasks_gone`). A host process belongs to
  the task whose reported namespace holds it
  (`protocol::ports::TaskOwnership`, `tools`).
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

- `Factory::new(shared, starter)`: the `protocol::ports::SessionFactory` that
  creates an owner per Session, with the state root's paths and the running
  executable (`RootPaths`), its durations (`Times`) and what tool admission
  reads (`ToolContext`), and rejects the tool instructions of a Session that
  has no owner.
- `probe_child::read_input` and `probe_child::run(input)`: the probe's whole
  work, from its stdin to its one line of stdout, for `tetral` run as
  `probe_child::PROBE_NAME`.

## Where things are

- `owner.rs`: `Factory`, `SessionMessage`, `SessionMailbox`, the owner's
  loop and state, `TaskStarter`, and a close, a connection's end and the
  shutdown as they end a preparation.
- `tools.rs`: the admission of tool instructions (`ToolContext`,
  `TaskStart`), a binding's task registry and staging queue (`Tasks`),
  their reports, the tasks' events (`TaskKey`, `TaskEvent`),
  `cleanup_pending` and the owner of a host process.
- `record.rs`: `Record`, `Binding` and the entries of `GET /v1/sessions`.
- `cases.rs`: the cases of a `session.prepare` (`Case`, `decide`).
- `prepare.rs`: the steps of a preparation, from the connection check to the
  report and its ACK.
- `probe.rs`: the spawn and supervision of one probe (`spawn`, `Probe`,
  `PROBE_LIMIT`).
- `probe_child.rs`: the probe's checks and the creation of a missing `cwd`.
