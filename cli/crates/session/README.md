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
  in the binding's registry (an equal payload joins the task, whose final
  report, its `terminal` or `rejected`, then ends it too, so that its
  replay-ledger entry goes with that report's ACK; another is
  `task_conflict`), the replay ledger's room, its schema, budgets and tool
  against the ready's list with an `exec_command`'s `input.task_id` equal to
  the outer one, its policy, its binding. Step 3 judges
  everything but the policy, which step 4 judges by `execution::policy`,
  translated with the paths of a task name chosen just before it
  (`execution::store`); only a registered task keeps its name (`tools`).
- A task admitted while its binding waits for the ACK of its `prepared`
  report waits in the binding's staging queue; that ACK starts the queue in
  arrival order before the binding's next instruction, and a close, a cancel
  of the task or the connection's end prevents a staged start; after a close
  or a cancel the task's supervisor makes its `terminal` report (`tools`).
- A control finds its task only in the registry of the binding it names on
  its own connection; an absent task is `task_not_found`, and a task whose
  start its supervisor rejected leaves the registry; a `tool.stdin` of
  a task that is not an `exec_command` is `unsupported_control`, a
  `tool.cancel` of a staged task takes it out of the staging queue, and
  every other control goes to the task's supervisor (`tools`).
- An owner starts admitted commands only from its handler, through the
  `TaskStarter` it is given, so a start never races its binding's close: the
  starter gives each admitted task its supervisor at admission, `staged`,
  and the owner starts it once its binding is `prepared`. A task carries
  its directory under its Session and its temporary directory, both named
  from the task's name chosen at admission, and the Session's Report outbox
  port (`tools::TaskStart`). The owner tells the Connection owner each
  `tool.execute` it stages, each that leaves the queue without starting,
  each task's start and the end of each started task, its processes
  confirmed gone or its start rejected, and each `tool.execute` it
  rejects `resource_exhausted` (`ConnectionPort`, `tools`).
- A close of the binding on the current connection prevents its staged
  starts and `session.drives`, has every running task of the binding end as
  at a cancel, and waits until the binding's tasks, probes and clones are
  gone and a `session.drives` being handled has handed its `applied` report
  to the outbox; then the final upload of the Session's drives, ending 30
  seconds after the close's admission, the drives' unmount, the deletion of
  the repositories, the tasks' temporary directories and
  `sessions/<session_id>/`, and the `closed` report with the abandoned
  changes. Each repeat of a close that is still running gets the same
  report; a repeat after it gets the saved report, kept until that
  connection ends. A close of a binding that a new binding of the same
  connection replaced is `closed` at once (`close`).
- A late close, for a Session with no binding on the close's connection,
  waits for every managed descendant of the Session, keeps the pending
  changes of its sync state as `lost_changes` under the close's
  `message_id`, deletes the Session's data as above without uploading and
  reports `closed`. The connection's end during an ordinary close before
  its report ends its final upload in keep mode: nothing is abandoned,
  deleted or reported, and the late close that follows joins the wait.
  An owner created for the close of a Session with no record asks the
  Connection owner to end it once its reports have their outcomes
  (`close`).
- A task's supervisor reports its PID namespace and its confirmed end to
  the owner. A connection's end sends every task of its binding the end
  (staged ones never start, running ones end at once) and keeps each started
  task whose processes are not yet confirmed gone; while one is kept, or a
  record an earlier App Server left for the Session is unconfirmed
  (`execution::cleanup::Leftovers`), a preparation of the Session fails with
  `cleanup_pending`, checked right after the connection it names. The
  shutdown first detaches every mount of the Session's drive sync
  (`fusermount3 -u -z`), then sends every task the end through the
  termination procedure, and
  once each started task's processes are confirmed gone the owner tells the
  Connection owner (`ConnectionPort::tasks_gone`). A host process belongs to
  the task whose reported namespace holds it
  (`protocol::ports::TaskOwnership`, `tools`).
- Owners follow the owner rules of `cli/README.md`. An owner reaches the
  Connection owner only through `protocol::ports::ConnectionPort` and hands
  its reports to `protocol::ports::ReportSink`. The tool instruction of a
  Session without an owner is rejected on a blocking thread, since its
  admission reads the filesystem (`Factory::unbound_tool`).
- A preparation's result is committed only while its connection, binding and
  operation are all still current (`prepare`). A binding enters `prepared`
  only on the ACK of a report of its result (`SessionOwner::report_outcome`).
  These control replies can use the Report outbox's reserved body budget when
  ordinary capacity is full. Exhausting both pools ends the logical connection;
  dropping a reply never advances the binding to `prepared`.
- The cases of a `session.prepare` against the Session's binding are one
  decision (`cases::decide`).
- The App Server never inspects a target directory before a probe and never
  changes its own working directory: a `cwd` at or below the state root,
  compared lexically, fails before any filesystem access
  (`prepare::inside_state_root`); everything else is checked by the directory
  probe, a child process of the running executable (`probe_child`).
- The probe uses the same canonical executable path that App Server startup
  resolved for tool launches. A later change to the installer's `current`
  symlink does not change that path; the installer retains the version's
  executable and helpers unchanged until its App Servers exit, as
  `appserver/README.md` defines. Probe input contains validated UTF-8 strings and is encoded
  before spawn. Directory creation uses `O_PATH | O_DIRECTORY` search
  handles, retaining `O_NOFOLLOW` for new components; mkdir and final chdir
  enforce write/search permissions without requiring directory read access.
  Creation preserves umask and existing modes. Failure after creation starts
  can leave directory prefixes, which are not rolled back. A failed binding
  retains its result; retrying directory preparation requires a new binding.
- A probe is spawned only with a probe slot of the Connection owner, which it
  holds until it is reaped. Its task alone kills its process group, and only
  before reaping it (`probe`). The owner reads the seam clock for the probe's
  `PROBE_LIMIT` whenever it wakes and wakes at least once per recheck interval
  while a probe runs; it acts on a passed limit before the message it woke
  for, so a result taken after the limit is discarded (`owner`).
- A cloud computer's repositories are checked by the probe too: a `path` at
  or below the state root, lexically or by its resolved path, fails the
  preparation, and so does an existing entry at a `path` that
  `sessions/<session_id>/repositories.json` does not list (`prepare`). A
  preparation with repositories then asks the Connection owner for one git
  ticket and clones or reuses each repository in order, one `git` child at
  a time in its own process group with the App Server's environment; the
  ticket, the URL rewrite to git-proxy and the commit identity go only into
  each repository's local configuration, and the ticket into no log or
  report (`repositories`). A clone that succeeds is listed in
  `repositories.json` even when its preparation is no longer current, so
  that the next preparation reuses it; a preparation with no repositories
  runs no `git`.
- A preparation that binds drives mounts them before its clones, through the
  Session's drive sync, which the owner opens from
  `sessions/<session_id>/sync-state.db` at the first such preparation once
  the App Server's drive context exists (`drives::DriveContext`). The
  report lists the new mount failures in `drive_mount_failures`, and the
  drive sync keeps their texts when the report is built. A sync state that
  cannot be opened fails every bound drive with its error, once per drive
  and text in the App Server process, and is opened again at the next
  preparation. The drive sync's requests carry the binding once it is
  `prepared`, and none after its connection ends (`drives`).

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
- `close.rs`: the steps of an ordinary and a late close, the saved
  `closed` report, and the deletion of the Session's local data.
- `tools.rs`: the admission of tool instructions (`ToolContext`,
  `TaskStart`), a binding's task registry and staging queue (`Tasks`),
  their reports, the tasks' events (`TaskKey`, `TaskEvent`),
  `cleanup_pending` and the owner of a host process.
- `drives.rs`: the drive sync of a preparation's mounts, and a
  `session.drives`: its queueing, the hold of later commands, its handling
  on the drive sync and its `applied` report.
- `record.rs`: `Record`, `Binding` and the entries of `GET /v1/sessions`.
- `cases.rs`: the cases of a `session.prepare` (`Case`, `decide`).
- `prepare.rs`: the steps of a preparation, from the connection check to the
  report and its ACK.
- `probe.rs`: the spawn and supervision of one probe (`spawn`, `Probe`,
  `PROBE_LIMIT`).
- `probe_child.rs`: the probe's checks, the creation of a missing `cwd` and
  each repository path's check.
- `repositories.rs`: `repositories.json`, the clone and reuse of each
  repository, the cause and message of each failure, the fitting of a
  report's failures to its limit, and the test seam counter `git.spawn`.
