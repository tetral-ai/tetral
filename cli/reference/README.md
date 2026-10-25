# reference

The reference control server belongs here: Engine's side of the client
connection between `tetral` and Engine, which the CLI's cross-process tests
run `tetral` against. It serves the protocol's endpoints over real HTTP and
SSE with its state in memory, and serves fault-injection controls on a second
listener. It runs no Agent loop and no ToolGate, and stands for no production
identity, persistence or routing. It imports only the Go standard library and
its own protocol types in `wire/`; `tetral` never depends on it.

## Design rules

- `Server.mu` guards the server's state, and the `Clock` has its own lock.
  Every time the server reads comes from that clock, which moves only when the
  `clock` control advances it; before that control answers, `due` does what
  the advance makes due.
- Each upstream request passes the steps of `serve` in its order, and the
  first refusal answers with its code; every request, refused or not, enters
  the request log, an upstream message with the SHA-256 of its body.
- A connection's replay ledger holds the answer to each accepted message: a
  retry with the same content, compared as JSON with sorted member names, gets
  that answer again, and other content under the same `message_id` gets
  `409 message_conflict`. The credential and git-ticket fetches are not
  replayed; each is a new fetch.
- A Session instruction is queued until the connection's ready is accepted,
  and past 64 Session instructions until the connection's first Session report
  is stored (`StagingLimit`). An instruction's payload is sent as the test
  gives it, so a test can send one that fails its schema; the server reads its
  identities only to accept the reports that answer it and, once a prepare is
  issued, to grant its binding's Git tickets.
- A tool-status report is accepted only when it answers a tool instruction
  issued on its connection, with that instruction's Session, binding,
  `tool_use_event_id` and `task_id` and the kind that answers its type
  (`409 unknown_instruction` otherwise), and only within the byte limits of
  its kind: a body over 14,680,064 bytes or an ordinary report over 327,680
  is `413 request_too_large`; a result envelope over 262,144 bytes (for
  `view_image`, with `data_base64` emptied) or metadata over 65,536 is
  `400 invalid_message`. A terminal or rejected execution report and a
  control's reply mark the instruction reported (`tools.go`).
- A stream gets a `heartbeat` every 15 seconds of the server's clock
  (`HeartbeatInterval`), on a resumed stream first right after `connected`,
  and a stream-only `disconnect` at the maximum stream age. A grant lasts at
  most 45 seconds (`GrantMax`), capped by the credential's expiry.
- The control listener is a separate `http.Handler` (`ControlHandler`); no
  protocol request reaches a control, and no control is part of the protocol.

## Startup

`cmd/tetral-reference` reads one JSON object from stdin (`Config`): the two
listen addresses (`127.0.0.1:0` by default), the test instances' credentials
`[{instance_id, credential_id, connection_key, expires_at, state}]`, the
minimum CLI version, the maximum stream age (30 minutes by default), the git
proxy origin and the clock's start. It prints one JSON object with the two
bound addresses (`Addresses`) to stdout and serves until it gets `SIGINT` or
`SIGTERM`. The listener that serves Engine's side keeps a socket receive
buffer of 65,536 bytes, so that a `throttle` slows the sender as a slow link
does.

## Controls

Each control is `POST /control/<name>` with a JSON body, answered with a JSON
object or `400` with the reason; an unknown `<name>` gets `404`.

| Control | Effect |
|---|---|
| `credential` | `add` a credential (a pending one with `renew` is offered on the instance's stream as `credential.renew`), `revoke` or `expire` one |
| `prepare`, `close` | Issue `session.prepare` or `session.close` with the given payload on the instance's live connection |
| `execute`, `stdin`, `cancel` | Issue `tool.execute`, `tool.stdin` or `tool.cancel` with the given payload, sizes over the protocol's budgets included, and the given `extra` envelope members |
| `send` | Write any event on the instance's stream as given (`event`, or raw `data`), or write an issued instruction again (`resend`); nothing is issued |
| `events` | Make the next `count` events requests hold their headers (`hold_headers`), send headers and no event (`headers_only`), send comments only (`comments_only`), or connect, with their headers and `connected`, only once the clock has advanced by `delay_ms` from this control (`delay_connected`) |
| `answer` | For the next `count` requests to an endpoint, or to one `message_id` there: `delay` by `delay_ms` of the clock, `drop` after accepting, `withhold` until `release` (of every withheld answer, or of an `endpoint`'s or a `message_id`'s, in any order), answer for a `wrong_identity`, answer the ACK `without_accepted`, answer `200` with a body that fails its schema (`schema_fail`), or answer an `error` code with the status the client error table gives it or with a given `status`, which a code outside the table needs; or, for the next `count` requests to an endpoint, read the body at `bytes_per_second` of the clock (`throttle`), so that a clock held still stalls it, the bytes read so far in the state's `throttled` |
| `dispatch_before_ready_ack` | Send these Session instructions after the instance's next ready is accepted and before its ACK is answered, past the staging limit |
| `break_stream` | Close the instance's stream; its connection keeps its lease |
| `clock` | Advance the clock by `advance_ms`, then do what is due |
| `state` | Read the credentials, the connections with their leases, bindings and issued instructions, the git tickets, the held answers, the throttled reads, the received messages, the sent events and the request log (`Snapshot`) |

## Where things are

- `reference.go`: `Config`, `Credential`, `Addresses`, the limits, `Server`,
  `New` and `Handler`.
- `clock.go`: `Clock`.
- `leases.go`: credentials, instances and connections, authentication, the
  predecessor table of `connect`, grants and version checks.
- `ledger.go`: the replay ledger, bindings and the issue, queueing and resend
  of Session instructions.
- `server.go`: the event stream and its heartbeats, `due`, `serve`, held
  answers and the upstream endpoints.
- `tools.go`: the tool-status endpoint and its size checks.
- `renewal.go`: credential renewal and the credential endpoint.
- `gitticket.go`: the git-ticket endpoint.
- `control.go`: `ControlHandler`, the controls and `Snapshot`.
- `server_test.go` holds the refusals and protocol rules; `control_test.go`
  one test per control; `tools_test.go` the tool controls and the
  tool-status endpoint; `helpers_test.go` the HTTP and SSE test client;
  `cmd/tetral-reference/main_test.go` the default addresses of the command
  run as a process.
