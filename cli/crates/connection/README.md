# connection

Every request to Engine belongs here, with two owners, each one task per App
Server:

- the **Connection owner**, which owns the connection to Engine, from
  authentication and the event stream to the classification and routing of
  Session instructions, and also the count of Session records and the
  **probe slots**, the right to run one directory probe;
- the **Report outbox**, which owns every report that answers or follows an
  instruction, from the moment it is frozen until Engine acknowledges it.

## Design rules

- The peer crates reach Engine only through the interfaces whose
  implementations belong here, as the peer rule of `cli/README.md` states.
- Its owners follow the owner rules of `cli/README.md`. The Connection owner
  runs every request and every stream read in a task of its own, tagged
  with a number unique in the process, and applies a result only while its
  tag is current, so work of an ended attempt or connection never changes a
  later one (`owner`).
- No deadline is waited for with one timer: the owner reads the seam clock
  whenever it wakes and, while anything waits for a time, wakes at least once
  per recheck interval, so a deadline that passed while the machine slept is
  acted on within that interval of waking (`owner`).
- A connection is new or resumed only by a valid `connected`: one that
  passes the schema, whose `resumed` is `true` only for a resume of the same
  connection (`attempt::AttemptKind::connected`).
- Ready and each heartbeat keep their `message_id`, sequence and first send
  time through every retry, and their ACK grants a deadline from that first
  send (`ready::grant`). Both pass their own decoder before their first send
  (`ready::checked`).
- From the moment ready is built until its matching ACK, Session
  instructions wait in arrival order, at most `staging::STAGING_MAX`
  (`staging`).
- A stream is bounded before it is parsed: a line longer than the event
  limit plus `data: ` or an event whose data is over the limit closes it
  (`stream`).
- Each event after a stream's first valid `connected` is handled in one
  order: its envelope's schema, then for a Session instruction its
  identities (`classify`), the replay ledger, the payload's schema of a
  `session.close` or `session.drives`, and routing, where a
  `session.prepare` is admitted (below); the owners of the other Session
  instructions decide their payloads' schema failures (`owner`). A
  connection message whose `connection_id` is not the stream's is recorded
  as `protocol.foreign_connection` and ignored.
- An instruction's content fingerprint is the SHA-256 of its envelope with
  object members sorted by name and no whitespace, so member order and
  whitespace are no change (`protocol::fingerprint`). The same `message_id`
  and fingerprint starts nothing; another fingerprint is a `message_conflict`.
- A protocol error is `message_conflict` or `invalid_message` with a fixed
  text for its class, carries `request_message_id` only when the envelope's
  `message_id` reads as an ID, never the invalid content or a guessed
  identity, and goes through the Report outbox (`protocol_error`).
- The transport follows no redirect and never records a request's headers or
  body (`http`). Reports go over connections of their own, whose socket send
  buffer is fixed before connecting, through the proxy that the CLI's other
  requests take (`http::Uploads`).
- A report is frozen under `outbox/` before its first send, and each retry
  sends that file again, until its matching ACK, a refusal or its
  connection's end, which cancels its uploads and deletes its directory; no
  report is sent on another connection (`outbox`). An attempt ends at its
  total, which grows with the size of a size-scaled report, and earlier
  after 10 seconds without progress (`report_deadline`, `http`).
- A `session.prepare` is admitted before it reaches its Session binding
  owner: a payload that fails its schema gets `invalid_input`, and a Session
  without a record while `sessions::RECORDS_MAX` exist or a new operation
  the ledger could not record gets `resource_exhausted`, each in a `failed`
  report with no record and no owner created; it is a new operation when
  its binding was not yet routed on the connection (`owner`). A
  `session.prepare` creates its Session's owner when it has none
  (`sessions`).
- At most `probe_slots::PROBES_MAX` probes hold a slot; a request that finds
  none free waits in arrival order and is answered `Exhausted` after
  `probe_slots::PROBE_WAIT`, and every waiting request is `Cancelled` at the
  connection's end and at the stop. A slot comes back only when its owner
  returns it after reaping its probe, and the stop is answered only once no
  probe holds one and every Session binding owner has reported its tasks'
  processes confirmed gone, so that the shutdown waits for them up to its
  deadline (`probe_slots`, `owner`). The stop has the owners end their tasks
  before it ends the connection, so that the tasks get the termination
  procedure's grace (`owner`).
- A stream reads one event of up to `stream::EVENT_MAX_BYTES` until the CLI
  starts submitting a ready that advertises `tool_execution`, and of up to
  `stream::TOOL_EVENT_MAX_BYTES` from then on, a resumed stream of that
  connection from its start; a larger event closes the stream unread
  (`stream::EventLimit`, `owner`).
- A tool instruction goes to its Session's owner, which decodes and admits
  it, or, for a Session without one, to the factory, which rejects it; it
  creates no owner and no record (`sessions`).
- The replay ledger records a new operation only while its reserve stays
  free after recording it; the rest may use the reserve; it never removes an
  entry to make room, and an entry goes only once its instruction's final
  report is acknowledged and no stream can deliver the instruction again
  (`ledger`).

## State

The Connection owner owns the connection's phase (`owner::Phase`: waiting
for an attempt, connecting, a held connection in `registering`, `online` or
`resuming`, `blocked` and `stopped`), the reconnect backoff, the last
connection held, which a new connection names as its predecessor, the staged
instructions, the held connection's replay ledger and the bindings it routed
a `session.prepare` for, the handle of each Session binding owner and whether
it holds a record, the probe slots, and the status fields
`connection_state`, `connection_id`, `credential_id`, `expires_at` and
`last_error`.

The Report outbox owns the current connection's number and identity and its
pending reports, each with its `r<n>` identity, its frozen file, its backoff
and its running attempt.

## Entry points

- `Connection::spawn(Config)` starts the Connection owner and its Report
  outbox, with the `protocol::ports::SessionFactory` that creates each
  Session binding owner; `status()` gives its `ConnectionStatus`,
  `sessions()` the entries of `GET /v1/sessions`, and `stop()` stops it,
  after which it stays `stopped`. It routes each Session instruction as an
  `Instruction`: the instruction as read, whether the ledger recorded it and
  as a new operation, and the span its work runs in; a `session.prepare`, a
  `session.close` and a tool instruction (`protocol::ports::ToolCall`) go to
  their Session's owner.
- `Times` holds the owner's durations and limits, each a constant but for
  tests.
- `Outbox::spawn(OutboxConfig)` starts the Report outbox, which implements
  `protocol::ports::ReportSink`; the Connection owner tells it each
  connection's start and end, and it answers with `OutboxEvent`s.
  `OutboxTimes` holds its durations, each a constant but for tests, and its
  `Clock` is the CLI's clock but for tests.

## Where things are

| File | Holds |
|---|---|
| `src/owner.rs` | The Connection owner: its phases, timers, the answers to the events request, ready and heartbeats, `disconnect`, the order of an event's handling and the ledger's admission (`admit`), staging and routing, the admission of a `session.prepare` and its `failed` report, the record `protocol.foreign_connection`, and the test seam counters `connection.attempt`, `connection.checked`, `connection.granted`, `connection.answered` and `route.session` |
| `src/sessions.rs` | The Session binding owners as the Connection owner reaches them (`Owners`), `RECORDS_MAX`, and its side of `protocol::ports::ConnectionPort` |
| `src/probe_slots.rs` | The probe slots, their waiting requests, `PROBES_MAX` and `PROBE_WAIT`, and the pressure point `tetral_probes`: its series and its rejection episodes |
| `src/classify.rs` | The reading of an event's envelope and identities, and the content fingerprint |
| `src/protocol_error.rs` | The classes of protocol errors, their codes and texts, and their reports |
| `src/attempt.rs` | One attempt's query by its kind, the validity of `connected`, and the task that reads its stream; `ATTEMPT_TOTAL` |
| `src/ready.rs` | Ready with its startup phases, the sender check, and the grant of an ACK |
| `src/heartbeat.rs` | Which heartbeat events start a send and which ACKs may set the deadline |
| `src/staging.rs` | The staging queue and `STAGING_MAX` |
| `src/ledger.rs` | The replay ledger, its limits and reserve, and when an entry goes |
| `src/backoff.rs` | The backoff of reconnects and of each message's retries |
| `src/http.rs` | `Transport`, the sorting of answers (`Answer`, `Failure`), `REQUEST_TOTAL`, a report's attempt bounded by its progress (`ReportRequest`), and the client of report uploads (`Uploads`) |
| `src/outbox.rs` | The Report outbox: identities `r<n>`, freezing, sending, retries, ACKs and refusals, connection ends, the record `report.dropped`, and the test seam counter `outbox.answered` |
| `src/report_deadline.rs` | A report attempt's total and `STALL_LIMIT` |
| `src/clock.rs` | The clock the outbox reads |
| `src/stream.rs` | The bounded reading of an event stream (`Events`), its shared limit (`EventLimit`), `EVENT_MAX_BYTES` and `TOOL_EVENT_MAX_BYTES` |
| `src/status.rs` | `ConnectionStatus`, the record `connection.state_changed` and the metrics `tetral_connection_state` and `tetral_reconnects_total` |
