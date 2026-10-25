# connection

Every request to Engine belongs here, with two owners, each one task per App
Server:

- the **Connection owner**, which owns the connection to Engine, from
  authentication and the event stream to the routing of Session instructions;
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
- The transport follows no redirect and never records a request's headers or
  body (`http`).

## State

The Connection owner owns the connection's phase (`owner::Phase`: waiting
for an attempt, connecting, a held connection in `registering`, `online` or
`resuming`, `blocked` and `stopped`), the reconnect backoff, the last
connection held, which a new connection names as its predecessor, the staged
instructions and the status fields `connection_state`, `connection_id`,
`credential_id`, `expires_at` and `last_error`.

## Entry points

- `Connection::spawn(Config)` starts the Connection owner; `status()` gives
  its `ConnectionStatus` and `stop()` stops it, after which it stays
  `stopped`.
- `Times` holds the owner's durations and limits, each a constant but for
  tests.
- `spawn_report_outbox`, `OutboxMessage` and `OutboxMailbox`: the Report
  outbox.

## Where things are

| File | Holds |
|---|---|
| `src/owner.rs` | The Connection owner: its phases, timers, the answers to the events request, ready and heartbeats, `disconnect`, staging and routing, and the test seam counters `connection.attempt`, `connection.checked`, `connection.granted`, `connection.answered` and `route.session` |
| `src/attempt.rs` | One attempt's query by its kind, the validity of `connected`, and the task that reads its stream; `ATTEMPT_TOTAL` |
| `src/ready.rs` | Ready with its startup phases, the sender check, and the grant of an ACK |
| `src/heartbeat.rs` | Which heartbeat events start a send and which ACKs may set the deadline |
| `src/staging.rs` | The staging queue and `STAGING_MAX` |
| `src/backoff.rs` | The backoff of reconnects and of each message's retries |
| `src/http.rs` | `Transport`, the sorting of answers (`Answer`, `Failure`) and `REQUEST_TOTAL` |
| `src/stream.rs` | The bounded reading of an event stream (`Events`) and `EVENT_MAX_BYTES` |
| `src/status.rs` | `ConnectionStatus`, the record `connection.state_changed` and the metrics `tetral_connection_state` and `tetral_reconnects_total` |
| `src/lib.rs` | The Report outbox's skeleton |
