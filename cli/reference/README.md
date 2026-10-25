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
  the request log.
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
`SIGTERM`.

## Controls

Each control is `POST /control/<name>` with a JSON body, answered with a JSON
object or `400` with the reason.

| Control | Effect |
|---|---|
| `credential` | `add` a credential (a pending one with `renew` is offered on the instance's stream as `credential.renew`), `revoke` or `expire` one |
| `prepare`, `close` | Issue `session.prepare` or `session.close` with the given payload on the instance's live connection |
| `send` | Write any event on the instance's stream as given (`event`, or raw `data`), or write an issued instruction again (`resend`); nothing is issued |
| `events` | Make the next `count` events requests hold their headers (`hold_headers`), send headers and no event (`headers_only`) or send comments only (`comments_only`) |
| `answer` | For the next `count` requests to an endpoint, or to one `message_id` there: `delay` by `delay_ms` of the clock, `drop` after accepting, `withhold` until `release`, answer for a `wrong_identity`, answer `200` with a body that fails its schema (`schema_fail`), or answer an `error` code with the status the client error table gives it or with a given `status`, which a code outside the table needs |
| `dispatch_before_ready_ack` | Send these Session instructions after the instance's next ready is accepted and before its ACK is answered, past the staging limit |
| `break_stream` | Close the instance's stream; its connection keeps its lease |
| `clock` | Advance the clock by `advance_ms`, then do what is due |
| `state` | Read the credentials, the connections with their leases, bindings and issued instructions, the git tickets, the held answers, the received messages, the sent events and the request log (`Snapshot`) |

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
- `renewal.go`: credential renewal and the credential endpoint.
- `gitticket.go`: the git-ticket endpoint.
- `control.go`: `ControlHandler`, the controls and `Snapshot`.
- `server_test.go` holds the refusals and protocol rules; `control_test.go`
  one test per control; `helpers_test.go` the HTTP and SSE test client;
  `cmd/tetral-reference/main_test.go` the default addresses of the command
  run as a process.
