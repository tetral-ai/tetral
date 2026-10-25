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
- Its owners follow the owner rules of `cli/README.md`.
- Session instructions that wait for the ready ACK keep their arrival order,
  at most `staging::STAGING_MAX` of them (`staging`).
- A stream is bounded before it is parsed: a line longer than the event
  limit plus `data: ` or an event whose data is over the limit closes it
  (`stream`).

## Where things are

- `lib.rs`: `spawn_connection_owner`, `ConnectionMessage` and
  `ConnectionMailbox`; `spawn_report_outbox`, `OutboxMessage` and
  `OutboxMailbox`.
- `backoff.rs`: the backoff of reconnects and of each message's retries.
- `staging.rs`: the staging queue and `STAGING_MAX`.
- `stream.rs`: the bounded reading of an event stream (`Events`) and
  `EVENT_MAX_BYTES`.
