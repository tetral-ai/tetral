# reference

The reference control server belongs here: Engine's side of the client
connection between `tetral` and Engine, which the CLI's cross-process tests
run `tetral` against. It serves the protocol's endpoints over real HTTP and
SSE with its state in memory, and serves fault-injection controls on a second
listener. It runs no Agent loop and no ToolGate, and stands for no production
identity, persistence or routing. It is a Go module of its own
(`go.mod` here), with no package of Engine's module: it imports the Go
standard library, `golang.org/x/text` for Unicode normalization and case
folding, and its own protocol types in `wire/`; `tetral` never depends on it.

Run its checks inside this directory, or all three with
`python3 scripts/verify-cli.py reference --evidence-dir <directory>` from the
repository root, as CLI Verification does:

```sh
go vet ./...
golangci-lint run ./...
go test -race ./...
```

## Design rules

- `Server.mu` guards the server's state, and the `Clock` has its own lock.
  Every time the server reads comes from that clock, which moves only when the
  `clock` control advances it; before that control answers, `due` does what
  the advance makes due.
- Each upstream request passes the steps of `serve` in its order, and the
  first refusal answers with its code; every request, refused or not, enters
  the request log, an upstream message with the SHA-256 and the size of its
  body as read, at most one byte over the endpoint's limit.
- A connection's replay ledger holds the answer to each accepted message: a
  retry with the same content, compared as JSON with sorted member names, gets
  that answer again, and other content under the same `message_id` gets
  `409 message_conflict`. The credential and git-ticket fetches are not
  replayed; each is a new fetch.
- A disconnect ends the live connection as its lease's end does, closing its
  stream, and is answered with its ACK; a copy then finds no live connection
  and gets `409 stale_connection`.
- A Session instruction is queued until the connection's ready is accepted,
  and past 64 Session instructions until the connection's first Session report
  is stored (`StagingLimit`). An instruction's payload is sent as the compact
  JSON of what the test gives, its strings byte for byte with no character
  escaped again, so a test can send one that sits at a byte limit; the
  server reads its identities and drives to accept the reports that answer it,
  to authorize its binding's drive transfer and, once a prepare is issued, to
  grant its binding's Git tickets.
- Every message the server sends and every answer it gives passes `wire`'s
  decoder for its type before it leaves, as the bytes that leave, a payload
  that a test hands to a control included: the control refuses an invalid one
  with the decoder's error. One the server builds itself that fails is listed
  in `failures` and is not sent; an answer is then a `500`. A test that needs a message outside the contract marks it with
  `outside_contract` (the instruction controls, `send`, each instruction of
  `dispatch_before_ready_ack`, an `error` of `answer`); `schema_fail` of
  `answer` is such a mark by its name. An error answer is checked as a client
  checks it: its body, the status of its code, on a drive transfer endpoint
  the codes that endpoint gives, and the `Retry-After` of a `429`; a `5xx`
  has no defined body, so any passes.
- The drive store (`drive_store.go`) implements what `protocol/README.md`,
  "Drive transfer", states and nothing it leaves to Engine: the check order,
  the content check, the commit's decision rules, one seq per commit that made
  a version, list paging with `start_seq` on the first page only, the `410`
  for a cursor from which a removed record would be listed, `Range`, and
  `drive.changed` after every such commit and at every resume before the
  resent instructions. A binding's drive set is what `session.prepare` sent
  and, after a `session.drives`, also the set before it until its `applied`
  report is stored. Another writer's commit, a drive's own limits, the removal
  of a version, the ageing of change records and the upload budget are set by
  controls. Not implemented, so no test may rely on it: the workspace's limits
  of 500 GB and 100,000 files, the stream-safe rule of drive paths, redacted
  versions, deleted drives, the cleanup of uncommitted content and the merging
  of notices. Content is kept in memory; another writer's large file is
  generated from a seed and never stored. A request that the store cannot
  answer within the contract is answered `500`, and it and every error
  answer outside the contract that no test marked are listed in the state's
  `failures`; the CLI's harness and this module's tests fail a test that
  leaves one listed.
- A tool-status report is accepted only when it answers a tool instruction
  issued on its connection, with that instruction's Session, binding,
  `tool_use_event_id` and `task_id`, the kind that answers its type and, for
  an execution result, the tool that was issued, `mcp` for an `mcp.call`
  (`409 unknown_instruction` otherwise), and only within the byte limits of
  that tool: a body over 14,680,064 bytes, or a report of any tool but
  `view_image` and `mcp` over 327,680, is `413 request_too_large`; a result
  envelope over 262,144 bytes (with its media strings emptied: `data_base64`
  of `view_image`, every `data` of an `image` or `audio` block and every
  `blob` of a `resource` block of `mcp`), metadata over 65,536, a
  `view_image` image whose `data_base64` is not standard Base64 without line
  breaks or decodes to more than 10,485,760 bytes or to another count than
  `size_bytes`, and `mcp` media strings that are not standard Base64 without
  line breaks or together decode to more than 10,485,760 bytes are `400
  invalid_message`, as is a report of more than 524,288 JSON values. A
  terminal or rejected execution report and a control's
  reply mark the instruction reported (`tools.go`, `mcp.go`).
- A tool-list report is accepted only from the instance's live connection:
  a body over 4,194,304 bytes is `413 request_too_large`, one that holds more
  than 524,288 JSON values, fails its schema or names a tool twice `400
  invalid_message`. It replaces the row of
  its instance and server when the row is absent or came from another
  connection, or when its `seq` is higher than the row's; otherwise it is
  acknowledged and changes nothing (`mcp.go`).
- A stream gets a `heartbeat` every 15 seconds of the server's clock
  (`HeartbeatInterval`), on a resumed stream first right after `connected`,
  and a stream-only `disconnect` at the maximum stream age. A grant lasts at
  most 45 seconds (`GrantMax`), capped by the credential's expiry.
- A stream whose socket has not taken an event whole within 10 seconds of
  real time (`DefaultStreamStall`) ends; its connection keeps its lease.
- The control listener is a separate `http.Handler` (`ControlHandler`); no
  protocol request reaches a control, and no control is part of the protocol.

## Startup

`cmd/tetral-reference` reads one JSON object from stdin (`Config`): the two
listen addresses (`127.0.0.1:0` by default), the test instances' credentials
`[{instance_id, credential_id, connection_key, expires_at, state}]`, the
minimum CLI version, the maximum stream age (30 minutes by default), the
stream's stall time (10 seconds by default), the git proxy origin and the
clock's start. It prints one JSON object with the two
bound addresses (`Addresses`) to stdout and serves until it gets `SIGINT` or
`SIGTERM`. The listener that serves Engine's side keeps a socket receive
buffer of 65,536 bytes, so that a `throttle` slows the sender as a slow link
does, and gives each stream's socket a send buffer of 65,536 bytes, so that a
reader that stops reading stalls its stream.

## Controls

Each control is `POST /control/<name>` with a JSON body, answered with a JSON
object or `400` with the reason; an unknown `<name>` gets `404`.

| Control | Effect |
|---|---|
| `credential` | `add` a credential (a pending one with `renew` is offered on the instance's stream as `credential.renew`), `revoke` or `expire` one |
| `prepare`, `close`, `drives` | Issue `session.prepare`, `session.close` or `session.drives` with the given payload on the instance's live connection; one marked `outside_contract` may fail its schema |
| `execute`, `stdin`, `cancel`, `mcp_call` | Issue `tool.execute`, `tool.stdin`, `tool.cancel` or `mcp.call` with the given payload and the given `extra` envelope members; one marked `outside_contract` may fail its schema or pass the protocol's budgets |
| `send` | Write an event on the instance's stream as given (`event` as its compact JSON with no character escaped again, or raw `data`), or write an issued instruction again (`resend`); with `count`, the same `event` is written that many times, each once the stream's buffer has room; nothing is issued; an event that is no message of the protocol needs `outside_contract` |
| `events` | Make the next `count` events requests hold their headers (`hold_headers`), send headers and no event (`headers_only`), send comments only (`comments_only`), connect, with their headers and `connected`, only once the clock has advanced by `delay_ms` from this control (`delay_connected`), or send a `connected` whose JSON is padded with spaces to `data_bytes` (`pad_connected`) |
| `answer` | For the next `count` requests to an endpoint, or to one `message_id` there: `delay` by `delay_ms` of the clock, `drop` after accepting, `withhold` until `release` (of every withheld answer, or of an `endpoint`'s or a `message_id`'s, in any order), answer for a `wrong_identity`, answer the ACK `without_accepted`, answer `200` with a body that fails its schema (`schema_fail`), hold the request before it is authenticated until `release` and then serve it as it would be served then (`hold_request`, per endpoint), or answer an `error` code with the status the client error table gives it or with a given `status`, which a code outside the table needs, with `retry_after_s` as the `Retry-After` of a `rate_limited` and `missing_sha256` for a `content_missing`, refused when its endpoint cannot give that answer unless it is a `5xx` or marked `outside_contract`; or, for the next `count` requests to an endpoint, read the body at `bytes_per_second` of the clock (`throttle`), so that a clock held still stalls it, the bytes read so far in the state's `throttled`. The drive transfer endpoints are named `list_files`, `list_changes`, `upload_content`, `commit` and `download_content`; they take the faults that fit a request without `message_id` and an answer that is no ACK, a `throttle` of `download_content` paces the content it writes, a fault with a `session_id` is for the reports of that Session only, a fault or a `release` with a `drive_id` is for that drive's requests only, and a fault with a `content_sha256` for the uploads or downloads of that content only. `clear` removes the faults not used up, of every endpoint or of the `endpoint` or `drive_id` it names |
| `drive_commit` | Commit `changes` to `drive_id` as another writer (`api_key_id`), without a base, deletions first, under one seq: each change deletes its `path` (`delete`), writes `text`, or writes content `generated` from `{size_bytes, seed}`; it answers the seq and each path's version and content hash |
| `drive_limits` | Set a drive's own `max_bytes` and `max_files`; one left out is none |
| `drive_remove_version` | Make a drive keep `version_id` no longer: its record stays listed and its content is found only while another kept version names it |
| `drive_expire` | Remove the change records of a drive's commits before `before_seq`, as their 30 days do |
| `drive_budget` | Set the workspace's upload budget to `bytes_per_minute` and fill it |
| `drive_state` | Read one drive: its seq, current files, versions with their writers, listed records and held content hashes (`DriveState`) |
| `dispatch_before_ready_ack` | Send these Session instructions after the instance's next ready is accepted and before its ACK is answered, past the staging limit |
| `sent_event` | Read the data of one event the server wrote, as written, by its `index` in the state's events |
| `break_stream` | Close the instance's stream; its connection keeps its lease |
| `clock` | Advance the clock by `advance_ms`, then do what is due |
| `state` | Read the credentials, the connections with their leases, bindings (each with its drive sets) and issued instructions, the git tickets, the held answers, the throttled reads, the received messages (a commit's body among them), the sent events, the request log, each request with the credential its key names, the tool-list rows and the `failures` (`Snapshot`) |

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
- `drive_store.go`: drives, versions, change records and content, the
  commit's decisions, the upload budget, the notices and a binding's drive
  sets.
- `drives.go`: the five drive transfer endpoints with their check order, and
  the drive-sync endpoint.
- `drive_controls.go`: the drive controls and `DriveState`.
- `mcp.go`: the tool-list endpoint and its rows, and the media of an `mcp`
  result.
- `control.go`: `ControlHandler`, the controls and `Snapshot`.
- `server_test.go` holds the refusals and protocol rules; `control_test.go`
  one test per control; `session_drives_test.go` the `applied` report of an
  issued `session.drives`; `drives_test.go` the refusal of invalid unmarked
  control payloads, the faults on the drive transfer endpoints and generated
  content; `tools_test.go` the tool controls and the
  tool-status endpoint; `mcp_test.go` the tool-list endpoint; `helpers_test.go` the HTTP and SSE test client;
  `behaviour_test.go` runs the Engine-direction cases of
  `protocol/behaviour-v1.cases.json` against the server, as Engine's tests
  run them against Engine, with each state and stimulus brought about by the
  controls, and `behaviour_cases_test.go` holds the case file's format and
  its matching rules;
  `cmd/tetral-reference/main_test.go` the default addresses of the command
  run as a process.
