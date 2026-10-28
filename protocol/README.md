# CLI–Engine protocol, version 1

This folder defines the protocol between `tetral`, the Rust CLI that runs on a
user's own computer or on a cloud machine, and Engine, the Tetral server. In
Engine, the service `computer-hub` serves every route the CLI calls; its
replicas are hub pods, and any hub pod may serve a request or hold a stream.

The CLI opens every connection. It opens an HTTPS server-sent-events (SSE)
stream to Engine, on which Engine sends instructions, and it sends HTTPS
requests to Engine for its reports and for drive content. Engine never opens a
connection to a machine, and a machine exposes no port to Engine. On the
machine, the CLI starts its one local MCP server and talks to it over the
server's stdin and stdout; neither JSON-RPC nor raw stdio bytes cross the
connection to Engine.

| File | Holds |
|---|---|
| [`connection-v1.schema.json`](connection-v1.schema.json) | The query of `GET /v1/client/events`; the request and response bodies of the `/v1/client/*` routes; the error body of every `/v1/client/*` route; the SSE envelope with the connection message types; ready with its capabilities and `lost_changes`; the Session reports; the local MCP tool-list report. It refers to the tool execution schema for the tool and local MCP messages, ACKs and protocol-error reports |
| [`connection-v1.fixtures.json`](connection-v1.fixtures.json) | Valid and invalid example messages of the connection schema |
| [`tool-execution-v1.schema.json`](tool-execution-v1.schema.json) | The tool instructions and `mcp.call`; `sandbox_policy`; tool reports and their results; the drive sync report; the ACK; the protocol-error report; the `tool_execution` and `local_mcp` parts of the ready capabilities |
| [`tool-execution-v1.fixtures.json`](tool-execution-v1.fixtures.json) | Valid and invalid example messages of the tool execution schema |
| [`drive-transfer-v1.schema.json`](drive-transfer-v1.schema.json) | The drive transfer endpoints: their path and query parameters, headers, bodies, responses, limits and endpoint table. It refers to the connection schema for the error body and `drive.changed`, and to the tool execution schema for drive writers and rejection reasons |
| [`drive-transfer-v1.fixtures.json`](drive-transfer-v1.fixtures.json) | Valid and invalid example messages of the drive transfer schema |

The schemas define structure and types. This README adds what JSON Schema cannot
express: byte limits, ordering, the relationships between fields and between a
request and its answer, and the lifecycle of connections, bindings, tasks and
drive changes. It names a schema definition by its file and JSON pointer, such
as `connection-v1.schema.json#/$defs/Ready`, and names a field only where a rule
needs it.

Each fixture file is a JSON array of example messages, valid and invalid, that
every implementation of the protocol types is tested against. A fixture is
`{name, schema, valid, value}`: `schema` names the definition of the matching
schema file that `value` is decoded as, and `valid` says whether a decoder must
accept it. Each valid fixture decodes into the types and encodes back to JSON
that the schema accepts; each invalid fixture is rejected by the decoder. An
error-body fixture also carries the HTTP status it arrives with (`http_status`),
or, for drive transfer, its `endpoint` and `status`; the decoder rejects a code
that the [error table](#error-responses) or
`drive-transfer-v1.schema.json#/$defs/DriveTransferEndpoints` does not pair with
that status and endpoint. A drive transfer answer fixture may carry `pair`,
which names by fixture name the request fixtures it answers: `query` for a
listing, `path_params` and `request_headers` for an upload, `request_headers`
for a partial download and `request` for a commit. It is decoded together with
them, and an invalid one breaks a relation between the answer and that request
that [drive transfer](#drive-transfer) states. An invalid fixture also says
why it is invalid, for its reader; no decoder reads this. In the connection
file, `reason` names the schema keyword and the JSON pointer it fails, or, as
`semantic`, the rule of this README. In the drive transfer file, `expect`
lists the checks it fails, each as `<keyword>@<pointer>` with `semantic` for
a rule of this README, and an invalid request adds `error`, the `status` and
`code` that Engine answers it with.

The protocol has five parts:

All JSON readers enforce at most 127 nested objects or arrays in the complete
message. The outer container is level one; strings and scalars add no level.
Envelope fields and unknown fields share this limit. It applies independently
of byte limits and is enforced before typed decoding, including fields that
typed decoding ignores. Senders account for envelope wrapping when validating
an accepted payload; they reject excessive input rather than truncating it.

- [Client connection](#client-connection): the stream, ready, heartbeats,
  credentials, Session preparation and close, and errors.
- [Tool execution](#tool-execution): tool instructions, `sandbox_policy`,
  reports and results.
- [MCP proxy](#mcp-proxy): the local MCP server, its tool lists and `mcp.call`.
- [Drive transfer](#drive-transfer): the HTTP endpoints for drive listing,
  catch-up, content and commits.
- [Session drives on a machine](#session-drives-on-a-machine): drive mounts,
  sync rules and conflicts.

## Client connection

Messages: the definitions of `connection-v1.schema.json` named below. Fixtures:
`connection-v1.fixtures.json`.

### Terms and identities

| Term | Meaning |
|---|---|
| App Server | The CLI's long-running process on a machine. It holds the connection to Engine, runs tools, serves drives and proxies the local MCP server |
| Launcher | The program that installs the CLI's configuration and starts and stops the CLI: the developer's app on a user computer, the Controller on a cloud computer |
| Controller | A system outside Engine (a sandbox vendor, a developer or a product backend) that provisions cloud machines and asks Engine for their credentials. It takes no part in this protocol |
| State root | The CLI's private directory, `$TETRAL_HOME`, one per app account |
| Session | One agent Session. It has at least one cloud computer and optional user computers |
| Computer | One machine of one Session: a cloud computer, built from an Environment (a machine template), or a user computer, which names a registered instance. Engine records each computer's current instance |
| Instance | One registered real machine in one workspace |
| Connection | One authenticated connection of an instance, with a server lease. It owns bindings and tasks and outlives any one stream. An instance has at most one live connection |
| Stream | One SSE response of `GET /v1/client/events`. A stream can end while its connection lives on, and the CLI resumes the connection on a new stream |
| Binding | One preparation of a computer's current instance for one Session on one connection |
| Task | One execution attempt, assigned to its original instance, connection and binding |
| Drive | A workspace-level cloud folder that Sessions bind and machines mount |

| Identity | Issued by | Names |
|---|---|---|
| `instance_id` | Engine | One instance. Stable across credential rotation, reconnection and restarts |
| `credential_id` | Engine | One connection credential of exactly one instance |
| `connection_id` | Engine | One connection. Random, kept only in the CLI's memory, delivered only over the authenticated stream |
| `binding_id` | Engine | One binding |
| `task_id` | Engine | One task. Controls cannot retarget it |
| `session_id` | Engine | One Session |
| `computer_id` | Engine | One computer of one Session |
| `tool_use_event_id` | Engine | One logical tool call; a control has its own |
| `message_id` | The sender | One protocol message on one connection, in either direction. Engine's downstream IDs are unique across all its connections |
| `drive_id`, `version_id` | Engine | One drive; one version of one drive file |

- Every identity is an opaque nonempty string without U+0000
  (`connection-v1.schema.json#/$defs/Id`).
- No side puts a connection, binding, task, call or message ID directly into a
  filesystem path. The CLI may map them to safe internal names without changing
  them on the wire.
- The CLI uses `session_id` and `drive_id` verbatim as folder names. It checks
  each one at `session.prepare`, `session.close` and `session.drives`: an ID
  that contains `/` or is `.` or `..` is refused
  (`connection-v1.schema.json#/$defs/SafeId`), as each of those messages states
  below.

```text
Engine:      instance registration ────────────────────────── deregistration
Connection:     connection A ────────────── ends      connection B ──
Streams:        pod 1 ──┤ pod 2 ─────┤ pod 1 ┤         pod 3 ──
Session S1:        binding A1 ───────────── ends         binding B1 ──
Session S2:        binding A2 ───────────── ends         binding B2 ──
```

A stream moves between hub pods without affecting its connection. Every new
connection needs new bindings. Task controls stay with the original task,
instance, connection and binding. A lost result never authorizes retrying a
possibly completed effect.

### Endpoints

The CLI sends every request; Engine answers.

- Every request carries `Authorization: Bearer <connection_key>` with the CLI's
  current credential. Engine authenticates the key against the instance and
  credential, then validates the connection and, for a report, the issued
  instruction it answers. A supplied `instance_id` alone proves nothing.
- A connection key is accepted only on `/v1/client/*` routes, and only for its
  own instance's connection and the instructions assigned there. A workspace API
  key is never accepted on a `/v1/client/*` route.

| Endpoint | The CLI sends it | Request | Success |
|---|---|---|---|
| `GET /v1/client/events` | To open a stream: a new connection or a resume | Query `connection-v1.schema.json#/$defs/EventsQuery`: `protocol_version=1`, `instance_id`, `credential_id`, optional `previous_connection_id`, optional `resume` (`true` or `false`, default `false`) | `200` SSE stream whose first event is `connected`; events `connection-v1.schema.json#/$defs/Downstream` |
| `POST /v1/client/ready` | After `connected` on a new connection | `connection-v1.schema.json#/$defs/Ready` | `connection-v1.schema.json#/$defs/GrantAcknowledgement` |
| `POST /v1/client/heartbeat` | In answer to each `heartbeat` | `connection-v1.schema.json#/$defs/Heartbeat`: `request_message_id` is the `heartbeat` message, `heartbeat_seq` its sequence | `connection-v1.schema.json#/$defs/GrantAcknowledgement` |
| `POST /v1/client/session-status` | In answer to `session.prepare`, `session.close` and `session.drives` | `connection-v1.schema.json#/$defs/SessionStatus` | `connection-v1.schema.json#/$defs/Acknowledgement` |
| `POST /v1/client/tool-status` | Tool and local MCP reports ([reports](#reports)) | `tool-execution-v1.schema.json#/$defs/ToolStatus` | ACK |
| `POST /v1/client/drive-sync` | Drive rejections that no tool result reports ([drive sync report](#drive-sync-report)) | `tool-execution-v1.schema.json#/$defs/DriveSyncReport` | ACK |
| `POST /v1/client/mcp-tools` | A local MCP server's tool list ([tool lists](#tool-lists)) | `connection-v1.schema.json#/$defs/McpTools` | ACK |
| `POST /v1/client/credential` | After `credential.renew` ([credentials](#credentials)) | `connection-v1.schema.json#/$defs/CredentialRequest`: `credential_id` is the pending credential | `200` `connection-v1.schema.json#/$defs/CredentialResponse` with `Cache-Control: no-store` |
| `POST /v1/client/git-ticket` | At a cloud computer's preparation with repositories ([git ticket](#git-ticket)) | `connection-v1.schema.json#/$defs/GitTicketRequest`: `binding_id` is the binding being prepared | `200` `connection-v1.schema.json#/$defs/GitTicketResponse` with `Cache-Control: no-store` |
| `POST /v1/client/protocol-error` | A downstream message it cannot accept ([protocol errors](#protocol-errors)) | `tool-execution-v1.schema.json#/$defs/ProtocolErrorReport` | ACK |

The drive transfer endpoints are in [drive transfer](#drive-transfer).

- **Common upstream fields.** Every POST body except the drive transfer commit
  holds `protocol_version` (`1`), `connection_id` (the live connection) and
  `message_id` (unique among this connection's upstream messages; a retry keeps
  it).
- **ACK.** `{connection_id, message_id, accepted: true}`
  (`tool-execution-v1.schema.json#/$defs/Acknowledgement`), matching the
  request. A ready or heartbeat ACK adds `valid_for_ms`, an integer from 1 to
  45,000: the validity Engine granted when it first accepted that message. The
  CLI checks the matching identity before it advances any state; an ACK for
  another identity acknowledges nothing.
- **A `2xx` answer that fails its schema counts as no answer.** An ACK then
  acknowledges nothing and the CLI sends the same message again; a credential
  response is handled like `404 not_found`; a git-ticket response like a failed
  fetch.

### Downstream messages

Each SSE event's `data` holds exactly one JSON envelope,
`connection-v1.schema.json#/$defs/Downstream`: `{protocol_version: 1,
message_id, connection_id, type, payload}` and an optional `traceparent`. The
SSE `event` and `id` fields are not used, and SSE comments carry nothing. Engine
sends every downstream message.

| `type` | Payload | Meaning | The CLI answers |
|---|---|---|---|
| `connected` | `connection-v1.schema.json#/$defs/ConnectedPayload` | First event of every stream. The envelope's `connection_id` is the connection; `resumed` is `true` when an existing connection resumed on this stream and `false` when Engine allocated a new connection | With ready on a new connection; with nothing on a resumed one |
| `heartbeat` | `connection-v1.schema.json#/$defs/HeartbeatPayload` | Sent every 15 seconds, only after Engine accepted the connection's ready, and on a stream whose `connected` carries `resumed: true` first right after `connected`. `heartbeat_seq` is a positive integer that increases on each heartbeat of the connection | `POST /v1/client/heartbeat` |
| `session.prepare` | `connection-v1.schema.json#/$defs/SessionPreparePayload` | Prepare a binding ([Session preparation](#session-preparation)) | Session report `prepared` or `failed` |
| `session.close` | `connection-v1.schema.json#/$defs/SessionClosePayload` | Close a binding ([Session close](#session-close)) | Session report `closed` |
| `session.drives` | `connection-v1.schema.json#/$defs/SessionDrivesPayload` | The Session's drive bindings changed ([drive binding changes](#drive-binding-changes)) | Session report `applied` |
| `credential.renew` | `connection-v1.schema.json#/$defs/CredentialRenewPayload` | A pending credential to fetch; never a key ([credentials](#credentials)) | `POST /v1/client/credential` |
| `disconnect` | `connection-v1.schema.json#/$defs/DisconnectPayload` | `{scope: "stream"}`: only this stream ends and the CLI resumes the same connection at once. `{scope: "connection", retryable, code}`: the connection ends; with `retryable: true` the CLI connects again under the reconnect backoff; with `retryable: false` it stops (`blocked`) until a credential change or until the conflict is resolved. `code` is a code of the [error table](#error-responses) | Nothing |
| `drive.changed` | `connection-v1.schema.json#/$defs/DriveChangedPayload` | New change records exist for a drive the instance has bound ([drive change notice](#drive-change-notice)) | Nothing; no ACK |
| `tool.execute`, `tool.stdin`, `tool.cancel` | `tool-execution-v1.schema.json#/$defs/Downstream` | Tool execution ([instructions](#instructions)) | Tool reports |
| `mcp.call` | `tool-execution-v1.schema.json#/$defs/McpCallPayload` | A local MCP call ([call instruction](#call-instruction)) | One tool report |

- Session instructions are `session.prepare`, `session.close`, `session.drives`
  and the tool and local MCP instructions.
- Engine assigns each downstream `message_id`, unique across all of Engine's
  connections. A resent instruction keeps its `message_id` and content.
- **`traceparent`.** Engine sets it on every Session instruction whose work has
  a trace, and keeps it when it resends the instruction. Its value is a W3C
  Trace Context `traceparent`, version `00`: `00-<32 hex trace ID>-<16 hex
  parent ID>-<2 hex flags>`, lowercase, with neither ID all zeros; `tracestate`
  is not used. The CLI uses it only when it matches that format and otherwise
  ignores it; it never rejects an envelope for a `traceparent` string it cannot
  use, and handles the instruction the same either way. For an instruction with
  a usable `traceparent`, the CLI sends the value as the `traceparent` header on
  every request it makes for that instruction: its reports, and the uploads and
  commits of its [sync point](#uploads). Uploads and downloads outside an
  instruction's sync point carry none.
- The CLI sends no trace spans. It reports durations instead: its startup phases
  in ready (`startup`) and each call's phases in its terminal report (`metrics`,
  [result envelope](#result-envelope)).

### Ready and versions

Ready is `connection-v1.schema.json#/$defs/Ready`, with
`connection-v1.schema.json#/$defs/Capabilities`,
`connection-v1.schema.json#/$defs/DriveCapability`, the tool parts
`tool-execution-v1.schema.json#/$defs/ToolExecutionCapability` and
`tool-execution-v1.schema.json#/$defs/LocalMcpCapability` (both together are
`tool-execution-v1.schema.json#/$defs/ReadyExtension`), and
`connection-v1.schema.json#/$defs/LostChanges`.

| Field | Rule |
|---|---|
| `os` | `linux` or `macos` |
| `arch` | `x86_64` or `aarch64` |
| `cli_version` | The CLI release version, SemVer |
| `capabilities.session_preparation` | `true` |
| `capabilities.drives.mount` | Whether this machine can mount drives now: `false` on a Mac whose drives are not enabled, on Linux without FUSE and in a container without `/dev/fuse` |
| `capabilities.tool_execution` | The tool capability ([ready capability declaration](#ready-capability-declaration)). Absent when the CLI executes no tools and proxies no local MCP server; a `tool_execution` that lists no tools requires `local_mcp` |
| `capabilities.local_mcp` | `{schema_version: 1}`. Absent when the CLI proxies no local MCP server; present only together with a `tool_execution` whose `controls` include `cancel` |
| `startup` | In every ready an App Server process sends until one of them is accepted, and in none after: `{config_ms, connect_ms, ready_ms}`, integers from 0 to 2,147,483,647 (a longer phase is sent as 2,147,483,647), measured with the monotonic clock: from the process start to its configuration loaded, from then to the first `connected` event, and from that event to sending this ready |
| `lost_changes` | Only when entries from an unacknowledged Session report remain ([final upload](#final-upload)) |

- The CLI advertises only capabilities that its startup checks establish as
  available, and no global working directory.
- **Protocol version.** `protocol_version` is `1` in the `GET /v1/client/events`
  query, in every upstream body and in every downstream envelope. Engine answers
  an upstream `protocol_version` other than `1` with `400 invalid_message`. The
  CLI rejects a downstream envelope whose `protocol_version` is not `1` (a
  [protocol error](#protocol-errors)).
- **CLI version.** Engine keeps a minimum supported and a recommended CLI
  version. A ready from a CLI older than the minimum gets `403
  cli_upgrade_required`: the CLI stops connecting, and the instance shows
  offline with that error. A CLI older than the recommended version works
  normally, and the instance shows that an update is available
  (`cli_update_available`).
- **Capability versions.** `capabilities.tool_execution.schema_version` (`1`)
  versions the tool messages, `capabilities.local_mcp.schema_version` (`1`) the
  local MCP messages, `sandbox_policy.schema_version` (`1`, listed in
  `sandbox_policy_versions`) the permissions, and the result envelope's
  `schema_version` (`1`) the tool results. None of them is the connection's
  `protocol_version`.
- The CLI ignores unknown optional fields, and needs a negotiated capability for
  any instruction type beyond those of this README.

### Connection lifecycle

```text
disconnected -> connecting -> registering -> online
      ^             |              |           |
      +------------ retryable failure ----------+

online -> resuming -> online        stream lost, then connected with resumed=true
registering -> resuming -> registering   stream lost before the ready ACK, then connected with resumed=true (online once the ACK arrived)
resuming -> disconnected            deadline passed, or connected with resumed=false
credential failure or connection conflict -> blocked
shutdown -> stopping -> stopped
```

- `connecting`: the CLI opens the SSE request and waits for a valid `connected`
  within the connection-attempt deadline.
- `registering`: it checks its capabilities, submits ready and waits for the
  matching ACK. It enters `online` only after that ACK.
- `resuming`: the connection, its bindings and its tasks stay valid until the
  connection deadline; the CLI does not repeat ready. Entered from
  `registering`, the deadline is the registration deadline, a ready not yet
  acknowledged keeps its retries and the staged instructions stay.
- `blocked`: the CLI makes no automatic connection attempt. Its local status
  stays available to the launcher.

| Timer | Value |
|---|---|
| Connection attempt | 10 seconds in total for dial and TLS, response headers and the first valid `connected`. Comments or other bytes do not reset it |
| Heartbeat interval | Engine sends a `heartbeat` every 15 seconds; on a resumed stream the first one right after `connected` |
| Connection validity | Each grant is at most 45 seconds, capped by the credential's remaining lifetime. Only an ACK of a ready or heartbeat renews it |
| Registration deadline | The ready ACK within 45 seconds of `connected`; otherwise the CLI abandons the connection ([registration failure](#registration-failure)) |
| Ordinary HTTP request | 10 seconds in total; retries stay within the connection's deadlines. Tool reports and local MCP tool-list reports use longer totals ([large-report deadlines](#large-report-deadlines)); drive transfer requests use their own ([drive transfer](#common-rules)) |
| SSE request | No whole-request timeout; a failure shows as stream closure or expired validity |
| Reconnect delay | Exponential backoff: the base delay starts at 1 second and doubles after each failed attempt up to 30 seconds; each wait is the base delay plus random jitter of up to 20% of it. The base delay returns to 1 second after one confirmed heartbeat on the new online connection. A stream-only `disconnect` resumes at once. Resume attempts use the backoff and stop at the connection deadline |
| Stream maximum age | An Engine setting, default 30 minutes, plus random jitter of up to 10%, counted from the stream's `connected`. At that age Engine ends the stream with a stream-only `disconnect` |
| Silent connection end | When a connection ends silently, with no `disconnect`, Engine ends it within 1 minute after its lease expires: at most 45 seconds plus 1 minute after the last grant |
| Competing connection | `409 connection_conflict` is retried until 45 seconds after the App Server started; a conflict after that blocks the CLI |

- The registration timer starts at `connected`, which also cancels the
  connection-attempt timer. Neither timer applies to the stream after readiness.
- **Clock.** The CLI's monotonic clock counts the time the machine sleeps:
  `CLOCK_BOOTTIME` on Linux and `clock_gettime(CLOCK_MONOTONIC)` on macOS. The
  connection deadline, grants, the registration deadline and the reconnect
  backoff read it. The CLI checks each deadline against it at least once per
  second rather than waiting for it with one timer, so a deadline that passes
  while the machine sleeps ends the connection, and with it the tasks, within 1
  second of waking.
- **Grant anchoring.** Each ready or heartbeat is anchored at its first send
  time on the monotonic clock. On its ACK, the CLI's connection deadline is that
  first send time plus `valid_for_ms`. A retry keeps the `message_id`, the
  sequence and the first send time. A duplicate never renews Engine's lease or
  moves the CLI's deadline. The CLI ignores an ACK of an older heartbeat;
  sending a heartbeat alone renews nothing. The CLI rejects a nonpositive
  `valid_for_ms` and an ACK whose deadline has already passed; neither
  authorizes dispatch. A `valid_for_ms` outside 1 to 45,000 fails the schema, so
  that ACK counts as no answer and the CLI sends the same message again.
- **Engine's checks.** Engine answers `400 invalid_message` to a ready under a
  new `message_id` on a connection whose ready it already accepted or refused,
  to a heartbeat before it accepted the connection's ready, and to a heartbeat
  under a new `message_id` whose `heartbeat_seq` is not above that of the last
  heartbeat it accepted on the connection. Each records nothing and renews
  nothing.
- **Staging during registration.** Engine may dispatch Session instructions once
  it has accepted ready, and issues at most 64 Session instructions on a
  connection until it has stored the connection's first Session report. The CLI
  sends that report only after it received the ready ACK. The HTTP ACK and the
  SSE instructions can arrive in either order. From the moment the CLI starts
  submitting ready, it stages Session instructions in a queue of at most 64;
  after the matching ready ACK it handles them in SSE arrival order, before
  later Session instructions. It discards them when registration fails or the
  connection is invalidated. A Session instruction that arrives before the CLI
  starts submitting ready is a protocol error (`invalid_message`). `heartbeat`
  and `disconnect` are handled while instructions are staged.
- The CLI schedules SSE reads, heartbeats and Session work independently: slow
  directory checks and report retries never delay a heartbeat. Before it commits
  any asynchronous result, it checks that its connection and operation are still
  current; work of an ended connection never updates a new one.

### Registration failure

When the ready ACK has not arrived within 45 seconds of `connected`, or a 65th
Session instruction arrives for staging, the CLI abandons the connection
(`registering` → `disconnected`). It closes the stream, discards the staged
instructions and connects again as a new connection, with
`previous_connection_id` set to the abandoned connection and `resume=false`,
under the reconnect backoff and the [predecessor
rules](#stream-resume-and-predecessors).

- From `connected`, a connection holds a lease of one 45-second grant, capped by
  the credential's expiry; only an ACK of the ready or of a later heartbeat
  extends it. The CLI's next attempt gets `409 previous_connection_active` while
  that lease lasts, and a new connection after it expires.
- Engine ends a connection whose ready it never accepted as it ends any
  connection whose lease expired ([connection end](#connection-end)). Until then
  the instance shows `registering`, or `offline` with the error after a ready
  refused with `403 cli_upgrade_required`.

### Stream resume and predecessors

When the stream fails before the connection deadline, the CLI cancels the old
stream's reads and opens a new stream with `previous_connection_id` set to its
connection and `resume=true` (`connection-v1.schema.json#/$defs/EventsQuery`).
After the deadline it ends its tasks
([disconnection](#disconnection-and-task-lifecycle)) and connects with the same
`previous_connection_id` and `resume=false`. The stream's `connected` carries
`resumed` (`connection-v1.schema.json#/$defs/ConnectedPayload`), and a refusal
is `connection-v1.schema.json#/$defs/ErrorBody`, which adds `retry_after_ms`
for `409 previous_connection_active`.

Engine authenticates the instance and credential, then applies one of these
atomically:

| The instance's active connection | Result |
|---|---|
| None, or its lease has expired | Engine retires the old identity and allocates a new `connection_id`; `connected` carries `resumed: false` |
| Matches `previous_connection_id`, still valid, `resume=true` | Engine moves the lease to this stream's hub pod without renewing it. The stream's first `heartbeat` follows `connected` at once, so the CLI's answer renews the lease. A pending credential of the same instance completes a [renewal](#credentials) here. `connected` carries the same `connection_id` and `resumed: true`. Engine resends every Session and tool instruction issued on this connection that has no persisted report. The previous hub pod closes its stream |
| Matches `previous_connection_id`, still valid, `resume=false` | `409 previous_connection_active` with `retry_after_ms` equal to its remaining lease (0 to 45,000); nothing is renewed or replaced |
| Does not match the supplied predecessor, or no predecessor is supplied | `409 connection_conflict`; the active connection is not replaced |

- Only the process that holds a connection can resume it. No request takes over
  another holder's lease.
- A new connection repeats ready and needs fresh preparations; old bindings stay
  invalid.
- Engine may end any stream at any time with a stream-only `disconnect`, to
  rebalance streams, roll out or scale in, and a hub pod can fail. In both cases
  the CLI resumes the same connection within the lease, usually on another hub
  pod; bindings and running tasks are unaffected, and the new pod resends the
  instructions that have no persisted report.
- Engine writes each instruction at most once on one stream. It writes an
  instruction again only on a later stream of the same connection, and never
  once it has stored the instruction's final report.

### Connection end

A connection ends on lease expiry, revocation, conflict or a connection-ending
`disconnect`. At the CLI it also ends when the CLI abandons it before its ready
is acknowledged ([registration failure](#registration-failure)).

- **The CLI** stops the connection's tasks
  ([disconnection](#disconnection-and-task-lifecycle)), marks its bindings
  `unavailable` and keeps the App Server running. It connects again. After the
  new connection's ready, Engine issues new preparations, and a late close (a
  `session.close` with a new `binding_id`, [Session close](#session-close)) for
  each computer that was removed from its Session while Engine could not confirm
  the close on this instance (a detached computer whose current instance is this
  one and whose `execution_state` is `unknown`).
- **Engine** shows the instance offline and invalidates the Session bindings on
  it. It settles every call whose instruction it sent on the connection and
  whose final report (a `terminal` or `rejected` report, or a control's reply)
  it has not stored as "computer offline, result unknown", and every call still
  waiting for a preparation report on the connection as not executed with
  "computer offline". It ends every background command of the connection, one
  whose call settled on its `running` report and whose `terminal` report it has
  not stored, with an unknown outcome, and tells the agent once that the command
  ended with an unknown outcome because its machine connection ended.
- Engine's lease expiry fences future authority; it is not evidence that the
  machine stopped. Execution recovery across connections does not exist.

### Credentials

- **Keys.** A connection key starts with `tetral_ck_`
  (`connection-v1.schema.json#/$defs/ConnectionKey`) and holds at least 256
  random bits. Engine stores only its digest and metadata, and the plaintext
  appears in exactly one response. A credential is in state `pending`, `active`,
  `revoked` or `expired`; a `pending` credential becomes `active` at its first
  authenticated use, which revokes the credentials it replaces. Each grant is
  capped at the credential's expiry; when no positive grant remains before it,
  requests get `401 credential_expired`.
- **First credential.** The CLI receives its first credential with its
  configuration from its launcher, outside this protocol. A credential issued
  with `delivery: response` reaches the machine the same way: the launcher
  stops the CLI, installs the key and starts it again.
- **Live renewal.** A credential issued with `delivery: stream`, which needs an
  online connection, is delivered over that connection, without interrupting
  tasks:
  1. Engine creates the credential in state `pending` and sends
     `credential.renew {credential_id}`
     (`connection-v1.schema.json#/$defs/CredentialRenewPayload`) on the
     instance's connection. It sends
     it again after each resume while that credential is still `pending`, and
     never on a new connection: a `pending` credential whose connection ended
     before the fetch stays `pending` until it expires or is revoked, and its
     issuer revokes it and issues another.
  2. The CLI calls `POST /v1/client/credential`
     (`connection-v1.schema.json#/$defs/CredentialRequest`, answered by
     `connection-v1.schema.json#/$defs/CredentialResponse`) with its current
     credential, its live `connection_id` and the pending `credential_id`. Engine generates the
     key when it answers the first successful fetch, stores only its digest, and
     returns the key in that response only, with `Cache-Control: no-store` and
     in no response cache. A request whose `connection_id` is not the live
     connection gets `409 stale_connection`, so a copied credential file alone
     never obtains a renewal. The response's `expires_at`
     (`connection-v1.schema.json#/$defs/Rfc3339Utc`) must also name a date and
     time that exist; a decoder refuses one such as `2026-02-30T00:00:00Z`.
  3. The CLI writes the new credential to its credential file atomically, then
     resumes its stream with it: `resume=true`, the same
     `previous_connection_id` and the new `credential_id`. After `connected`
     with `resumed: true`, every request uses the new credential.
  4. Engine commits at the first authenticated use of the pending credential:
     that resume or, after a CLI restart, a new connection. In the transaction
     that moves the lease it makes the new credential the connection's
     credential, caps later grants at its expiry and revokes the old credential.
     A later use of the old credential gets `401 credential_revoked` and is
     recorded as a reuse, which means the credential file was copied.
- If the CLI cannot persist the new credential, it keeps the current one and the
  pending credential expires unused. Bindings and tasks stay on the connection
  throughout.
- `POST /v1/client/credential` returns `404 not_found`, never `401` or `403`,
  when `credential_id` is unknown, belongs to another instance, is already
  `active`, has expired or been revoked, or was already fetched. A retry after
  the first successful response therefore gets `404 not_found`, because the
  plaintext is gone; the replay rule does not apply to this route. The CLI does
  not retry a `404 not_found`, keeps its current credential and logs the
  rejection. A fetched credential that is never used stays `pending` until it
  expires or is revoked, and its issuer revokes it and issues another, as for
  an unfetched one.

### Replay and authorization

- Within one connection, a retry of the same `message_id` with the same content
  returns the original result, except on `POST /v1/client/git-ticket` and `POST
  /v1/client/credential`. The same `message_id` with different content gets `409
  message_conflict` and repeats nothing.
- Engine accepts a Session report only for an instruction and binding issued on
  that connection. A late preparation report never reactivates a closed or
  invalid binding.
- A new connection gets a new identity and new bindings; a resumed stream keeps
  its connection and bindings. Neither restarts an execution: Engine resends
  every instruction that has no persisted report, and the CLI deduplicates by
  `message_id`.
- **Unparseable downstream messages.** A downstream message cannot be parsed
  when it is not one JSON object, its envelope fails the schema, or it is a
  Session instruction whose payload lacks a nonempty `session_id` or
  `binding_id`, or, for a tool or local MCP instruction, a nonempty
  `tool_use_event_id` or `task_id`.
- **Other schema failures.** Any other schema failure of a `session.prepare`
  fails it with `invalid_input`; any other schema failure of a tool or local MCP
  instruction rejects it with `invalid_input` ([reports](#reports)). Any schema
  failure of a `session.close` or `session.drives`, and of a `heartbeat`,
  `credential.renew`, `disconnect` or `drive.changed` payload, is a protocol
  error.

### Protocol errors

The CLI reports these through `POST /v1/client/protocol-error`
(`tool-execution-v1.schema.json#/$defs/ProtocolErrorReport`):

- a downstream message that cannot be parsed;
- a message that reuses a `message_id` with different content
  (`message_conflict`; an SSE message cannot receive an HTTP `409`);
- a Session instruction that arrives before the CLI starts submitting ready;
- a `session.close` or `session.drives` that fails the schema, an unsafe
  `session_id` or `drive_id` included;
- a `session.drives` whose `binding_id` names no `session.prepare` the CLI
  received on this connection;
- a `heartbeat`, `credential.renew`, `disconnect` or `drive.changed` whose
  payload fails the schema.

Before the stream's first valid `connected` the CLI reports nothing; the
connection attempt goes on until its deadline.

| Code (`tool-execution-v1.schema.json#/$defs/ProtocolErrorCode`) | Meaning |
|---|---|
| `message_conflict` | A message reused an earlier message's `message_id` with different content |
| `invalid_message` | Every other protocol error |

- The report holds the common fields, `error {code, message}`, and
  `request_message_id` when the original `message_id` can be read. It has its
  own `message_id`, never echoes the invalid content and never guesses Session
  or task identities.
- Engine validates the instance, credential and connection, persists the
  protocol error and ACKs. A missing or unknown correlation never prevents
  recording it. The CLI retries the report under the normal rules.
- A protocol error never changes task state and never creates a tool result. One
  that cannot be tied to a call is a connection-level record; each pending call
  keeps its own lifecycle.

### Session preparation

`session.prepare` (`connection-v1.schema.json#/$defs/SessionPreparePayload`,
with `connection-v1.schema.json#/$defs/DriveBinding`,
`connection-v1.schema.json#/$defs/DriveExclude`,
`connection-v1.schema.json#/$defs/Repository`,
`connection-v1.schema.json#/$defs/Checkout` and
`connection-v1.schema.json#/$defs/GitIdentity`) asks the CLI to prepare one
binding. Engine keeps it within the 64 KiB event limit: it refuses a Session
creation, a computer addition or a drive binding change after which a computer's
`session.prepare` would exceed 64 KiB.

| Field | Rule |
|---|---|
| `cwd` | Required, nonempty and absolute: the computer's `working_directory` at its first preparation, and the canonical `cwd` the CLI confirmed at a preparation on an earlier connection |
| `create_cwd` | `true` for a cloud computer; absent or `false` for a user computer |
| `drives` | Drive bindings `{drive_id, access}`, `access` `read_only` or `read_write`: the drive set of the computer's latest binding on this instance whose `prepared` report Engine stored, when one exists, and otherwise the Session's drive bindings; empty when the set holds none. A `drive_id` appears once. When they differ from the Session's drive bindings, Engine sends `session.drives` once it stores the `prepared` report ([drive binding changes](#drive-binding-changes)) |
| `drive_exclude` | All of this computer's excluded drive paths ([excluded paths](#excluded-paths)), entries for drives not in `drives` included; an entry takes effect while its drive is bound |
| `repositories` | A cloud computer's repositories; always empty for a user computer. `path` is the resolved absolute clone path, unique per computer; `checkout` is `null`, `{type: "branch", name}` or `{type: "commit", sha}` with `sha` 40 lowercase hexadecimal characters; `git_identity` is the commit identity, the platform identity when the developer gave none. It never carries the repository's authorization token |

`session.prepare` never carries a ticket or a token.

#### Git ticket

A cloud computer reaches its repositories through Engine's git-proxy, which
holds the repository token; the machine holds only a ticket.

- At each preparation of a cloud computer whose `repositories` is not empty, the
  CLI fetches a git ticket with `POST /v1/client/git-ticket` on its live
  connection, with the `binding_id` of that `session.prepare`, before it clones
  (`connection-v1.schema.json#/$defs/GitTicketRequest`, answered by
  `connection-v1.schema.json#/$defs/GitTicketResponse`).
- Engine creates the ticket at that fetch, already accepted by git-proxy, stores
  only its hash and returns `{git_proxy_url, ticket}` with `Cache-Control:
  no-store`.
- A ticket belongs to one cloud computer, not to the Session. Each fetch issues
  a new ticket and revokes that computer's previous one. A retry with the same
  `message_id` is a new fetch: the replay rule's original result does not apply
  to this route.
- A `binding_id` that names no `session.prepare` issued on this connection for a
  cloud computer with repositories gets `409 unknown_instruction`.
- A fetch that fails after the connection's retry rules ([error
  responses](#error-responses)) fails every repository of that computer: the
  preparation goes on without them and lists each in `repository_failures` with
  cause `clone` and the fetch error as `message`.
- `git_proxy_url` is an `https` origin (scheme, host and optional port) with no
  path and no trailing `/`. The CLI reaches the repository
  `https://github.com/<owner>/<repo>` at
  `<git_proxy_url>/github.com/<owner>/<repo>` and sends the ticket in every
  request as the header `X-Tetral-Git-Ticket: <ticket>`, never in the URL.
  git-proxy accepts a ticket only in that header.
- The CLI writes the ticket into the Git configuration of each repository it
  clones or reuses, never into the global Git configuration.
- On a user computer, Git uses the user's own stored credentials and identity:
  the CLI fetches no ticket, uses no git-proxy and changes no Git configuration.

#### Preparation steps

The CLI prepares in this order:

1. It checks that the connection is online and that the instruction's
   identities, binding and input are valid. An instruction whose `connection_id`
   is not the CLI's current online connection fails with `stale_connection`. A
   missing, empty or relative `cwd` fails with `invalid_input`, and so do a
   `session_id`, or a `drive_id` in `drives`, that contains `/` or is `.` or
   `..`, and any other schema failure of the payload.
2. It rejects a `cwd` that lies lexically inside the state root
   (`cwd_forbidden`), without filesystem access to the target.
3. It checks the directory in a supervised probe child: resolve symbolic links,
   check existence and type, reject a resolved path inside the state root, check
   access as the App Server's user, and enter the directory. With `create_cwd:
   true` the probe first creates a missing directory. Without it, a missing
   directory fails; the CLI never creates it and never falls back to another
   directory. The probe runs no user command and writes no project file. It ends
   within 5 seconds or fails with `cwd_check_timeout`; a late result is
   discarded.
4. It mounts each drive in `drives` where this machine can mount ([drives on a
   machine](#drives-on-a-machine)). On a cloud computer it fetches the [git
   ticket](#git-ticket) and clones each repository to its `path`. A `path` that
   lies inside the state root, lexically or after resolving symbolic links as in
   steps 2 and 3, fails the preparation with `repository_path_forbidden` before
   anything is cloned. A folder at `path` that this Session cloned earlier on
   this machine, before a wake or a failed preparation, is reused; any other
   existing entry at `path` fails the preparation with `repository_path_exists`
   and is never overwritten. With `checkout: null` the clone checks out the
   remote's default branch; with `branch` it checks out that branch; with
   `commit` it checks out that commit, detached. A checkout that fails is a
   repository failure with cause `checkout`.
5. It commits the result only if the connection, the binding and the preparation
   operation are all still current, keeps the canonical `cwd` as the binding's
   default directory, and reports.
6. It enters `prepared` after the matching ACK. When the ACK is lost it retries
   the same report; it never creates another binding.

A binding at the CLI is `staged` (waiting behind ready), `preparing`,
`prepared_pending_ack` (reported, ACK not yet received), `prepared`, `failed`,
closing, closed or `unavailable` (its connection ended).

| Case | Behaviour |
|---|---|
| The same preparation again (same binding and payload) | Joins the running preparation or returns its saved result, the canonical `cwd` included; a changed symbolic link is not resolved again |
| The same binding with a different payload | `binding_conflict` |
| Another binding for a Session whose binding on this connection is not `failed` | `binding_conflict`; an explicit close comes first |
| A new binding for a Session whose binding on this connection is `failed` | Replaces the failed binding; the Session's local data and clones stay |
| Two Sessions with one working directory | Both prepare, with distinct bindings; ordinary files are shared |
| A Session whose earlier execution on this instance is still being cleaned up | `cleanup_pending` |
| 128 Session records already retained | `resource_exhausted`; closes and retries of existing operations still work |
| 8 directory probes already running | The preparation waits for a slot, in arrival order, and spawns nothing while it waits; after 5 seconds of waiting it fails with `resource_exhausted` |
| A close while the directory check runs | The preparation is cancelled; its late completion never recreates the binding |
| A close while the report awaits its ACK | Report retries stop and the binding closes; Engine never reactivates the binding from a late report |
| A new connection | Every Session needs a new binding and a new preparation; an `unavailable` record alone does not block it |
| An App Server restart | Bindings are not restored; Engine prepares again. Each Session's drive sync state is kept and its pending changes upload as usual |

Preparation error codes (`connection-v1.schema.json#/$defs/PreparationError`).
Callers branch on the code, never on the message.

| Code | Meaning |
|---|---|
| `cwd_not_found` | The working directory is missing (and `create_cwd` is not `true`) |
| `cwd_not_directory` | The working directory is not a directory |
| `cwd_forbidden` | The working directory lies inside the state root, lexically or after resolving symbolic links |
| `cwd_inaccessible` | Access to the working directory is denied, or its resolved path is not UTF-8 |
| `cwd_check_timeout` | The directory probe did not end within 5 seconds |
| `invalid_input` | A missing, empty or relative `cwd`, an unsafe `session_id` or `drive_id`, or any other schema failure of the payload |
| `stale_connection` | The instruction's `connection_id` is not the CLI's current online connection |
| `binding_conflict` | The same binding with a different payload, or another binding for a Session whose binding on this connection is not `failed` |
| `cleanup_pending` | An earlier execution of this Session on this instance is still being cleaned up |
| `resource_exhausted` | 128 Session records are retained, a probe slot did not free within 5 seconds, or the replay ledger cannot record the operation |
| `repository_path_exists` | An entry this Session did not clone exists at a repository `path` |
| `repository_path_forbidden` | A repository `path` lies inside the state root |

- `prepared` records a completed handshake, not continuous directory health. The
  CLI never watches the directory and never closes the binding, invalidates it
  or disconnects because the directory disappears; each operation checks its own
  directories ([tool inputs](#tool-inputs)).
- A changed working directory needs a new binding.

#### Preparation report

`POST /v1/client/session-status`
(`connection-v1.schema.json#/$defs/SessionStatus`, with
`connection-v1.schema.json#/$defs/PreparationError`,
`connection-v1.schema.json#/$defs/DriveMountFailures` and
`connection-v1.schema.json#/$defs/RepositoryFailures`):

| Field | Rule |
|---|---|
| `request_message_id` | The `session.prepare` message |
| `session_id`, `binding_id` | As addressed |
| `status` | `prepared` or `failed` |
| `cwd` | With `prepared`: the canonical absolute working directory |
| `error` | With `failed`: `{code, message}` with a preparation error code |
| `drive_mount_failures` | With `prepared`, when not empty: each bound drive whose mount failed as a new failure, with the operating system's error ([mount failures](#mount-failures)); a `drive_id` appears once |
| `repository_failures` | With `prepared` on a cloud computer, when not empty: each repository that did not clone or check out; a `path` appears once. `message` is Git's message, or the error of a failed git-ticket fetch |

| Repository failure `cause` | Meaning | Engine records |
|---|---|---|
| `clone` | The clone failed, a failed git-ticket fetch included | `repository_clone_error` |
| `not_found` | The repository does not exist | `repository_not_found_error` |
| `authentication` | Authentication failed | `repository_authentication_error` |
| `forbidden` | Access was refused | `repository_forbidden_error` |
| `checkout` | The checkout failed | `repository_checkout_error` |

A repository that does not clone or check out leaves the Session running without
it.

What Engine does with the report:

- It marks the computer `ready` only after persisting a matching `prepared`
  report; the canonical `cwd` becomes the computer's `cwd` and the `cwd` of its
  later preparations. A `failed` report marks the computer `failed` with the
  error.
- In the transaction that stores the report, it records for the developer one
  `computer_preparation_error` for a `failed` report, one `drive_mount_error`
  per entry of `drive_mount_failures`, and one repository error per entry of
  `repository_failures`.
- After a failed preparation, the computer's next machine-bound call makes
  Engine invalidate the failed binding and send a new `session.prepare` with a
  new `binding_id`, which replaces it, without a `session.close`. The call waits
  for that preparation and fails with its error if it fails again. A new
  connection also prepares the computer again.

### Session close

`session.close {session_id, binding_id}`
(`connection-v1.schema.json#/$defs/SessionClosePayload`) closes one binding on
the receiving instance. It never closes the Session, another computer's binding
for that Session, or another Session on this instance.

- **Ordinary close.** A close for a binding whose `session.prepare` the CLI
  received on this connection (`staged`, `preparing`, `prepared_pending_ack`,
  `prepared` or `failed`). The CLI stops admission for the binding at once,
  cancels pending starts and the preparation, and ends the binding's tasks,
  background ones included. Once those tasks have exited, it runs the [final
  upload](#final-upload) of every drive of the Session, then unmounts the drives
  and deletes the Session's directory and its repository clones.
- **Late close.** A `session.close` whose `binding_id` names no
  `session.prepare` the CLI received on this connection is a late close, for a
  computer whose closure Engine could not confirm ([connection
  end](#connection-end)). The CLI deletes the Session's local data on this
  machine without uploading, and every uncommitted change that the Session's
  sync state holds counts as abandoned.
- A `session.close` whose `session_id` contains `/` or is `.` or `..` is a
  protocol error (`invalid_message`) and changes nothing, also as a late close.
- **Report.** The CLI reports `POST /v1/client/session-status`
  (`connection-v1.schema.json#/$defs/SessionStatus`) with
  `request_message_id` set to the close message, the addressed `session_id` and
  `binding_id`, `status: closed`, no `cwd`, and, when any change was abandoned,
  `lost_changes`. It reports only once the binding admits nothing, binding
  access is removed and every managed descendant has exited: tool processes (on
  macOS those of each task's process group) and directory probes included; a
  sent signal is not enough. Meanwhile heartbeats and other bindings stay
  responsive.
- A repeated close joins the pending cleanup or returns the saved `closed`
  result, `lost_changes` included; it never reports `closed` while cleanup is
  pending.
- Closure and tool-result settlement are independent. A closed binding accepts
  no new work, but the still-valid connection may finish reporting tasks issued
  before the close. Engine accepts those reports only against their recorded
  instructions, without restoring the computer. The CLI keeps each tool report
  until its own ACK or the connection's end; a close ACK never releases a tool
  report.
- **Connection end during a close.** When the connection ends before the CLI has
  built the close report, the CLI stops that close: its final upload ends at
  once, nothing is abandoned, nothing is deleted and no report is sent. The late
  close that Engine sends on the next connection then abandons every pending
  change under its own `message_id`, deletes the Session's local data and
  reports them. When the connection ends after the CLI built the report and
  before Engine durably accepts it, the CLI reports that close's abandoned
  changes as the [final upload](#final-upload) states. Either way the computer's
  closure becomes `unknown`, that close records no `drive_changes_lost_error`,
  and the CLI never synthesizes a close on another connection.
- After a new connection, a replacement binding for the same Session is refused
  with `cleanup_pending` while the old execution's cleanup is unconfirmed. Other
  Sessions continue.

### Drive binding changes

`session.drives {session_id, binding_id, drives}`
(`connection-v1.schema.json#/$defs/SessionDrivesPayload`) carries the Session's
new drive bindings, each `drive_id` once.

- **When Engine sends it.** When a change of the Session's drive bindings
  commits, Engine sends `session.drives` to every prepared binding of the
  Session. A binding whose preparation is outstanding when a change commits gets
  `session.drives` once Engine stores its `prepared` report, when the `drives`
  of its `session.prepare` differ from the Session's drive bindings at that
  moment; Engine issues that `session.drives` before every tool instruction that
  the report releases. A computer whose machine is offline when a change commits
  gets none: its next `session.prepare` on the same instance carries the drive
  set of its last prepared binding there, so the `session.drives` that follows
  its `prepared` report runs the final upload of each removed or changed drive.
- **Refused messages.** A `session.drives` whose `session_id` or any `drive_id`
  contains `/` or is `.` or `..`, or whose `binding_id` names no
  `session.prepare` the CLI received on this connection, is a protocol error
  (`invalid_message`) and changes nothing.
- **Waiting and ignoring.** A `session.drives` for a binding whose preparation
  the CLI has not yet seen acknowledged (`staged`, `preparing` or
  `prepared_pending_ack`) waits behind the preparation ACK, as tool instructions
  do. One for a binding the CLI is closing is ignored and logged: the close's
  final upload covers every drive.
- **Handling.** The CLI handles `session.drives`
  (`connection-v1.schema.json#/$defs/SessionDrivesPayload`) in stream order,
  before any later `tool.execute`. It runs the [final upload](#final-upload) of each
  removed drive, and of each drive whose access changed under its old access.
  Then it unmounts each removed drive, mounts each added drive and remounts each
  drive whose access changed, which then shows the drive's version at every path
  whose change was abandoned. A mount that `session.drives` removes or remounts
  is detached lazily: later I/O under it by a background command still running
  fails with `EIO` and is not uploaded.
- **Report.** `POST /v1/client/session-status`
  (`connection-v1.schema.json#/$defs/SessionStatus`) with `request_message_id`
  set to that message, the addressed `session_id` and `binding_id`, `status:
  "applied"`, `drive_mount_failures` for each new mount failure and
  `lost_changes` when a change was abandoned.
- **Drive set.** A binding's drive set, which authorizes its [drive
  transfer](#drive-transfer) requests, is the drives and access that Engine last
  sent to that binding in `session.prepare` or `session.drives`. A removed
  drive, and the old access of a drive whose access changed, stay in the set
  until Engine stores that `session.drives` report.
- The agent is told of the change at the start of its next turn.

### Final upload

The final upload is a binding's last upload of a drive's changes: at an ordinary
[Session close](#session-close) for every drive of the Session, and at a
`session.drives` for each drive it removes or whose access it changes, under the
old access.

- The CLI uploads and commits every uncommitted change of the drives it covers
  under the [sync rules](#uploads), changes listed as `uploading` or
  `upload_pending` included. It retries temporary failures ([drive
  transfer](#common-rules)); a change whose upload or commit ends in a fault is
  not tried again.
- A change that Engine rejects keeps the drive's version, as every rejection
  does; it is decided, not abandoned.
- **Deadline.** The final upload ends once every change it covers is accepted,
  rejected or has ended in a fault, and at the latest 30 seconds after the CLI
  admitted the close or began handling the `session.drives`, however long the
  tasks took to exit. The CLI then stops every upload and commit of the drives
  it covers, a commit in flight included: every change that no commit answer
  accepted or rejected by then is abandoned. A change of a commit whose answer
  has not arrived is abandoned too, although Engine may have applied it;
  `lost_changes` names the changes the CLI could not confirm saved.
- A final upload that the connection's end interrupts abandons nothing: a
  close's as [Session close](#session-close) states, and a `session.drives`'s,
  whose handling then stops with its mounts unchanged and no report. The next
  connection's preparation carries the binding's unapplied drive set, and the
  `session.drives` that follows its `prepared` report runs the final upload
  again.
- The CLI drops every abandoned change from the Session's sync state; an
  abandoned change is never uploaded or committed again.
- **`lost_changes`** (`connection-v1.schema.json#/$defs/LostChange`). The report
  of that close or `session.drives`, and of a late close for the changes it
  counts as abandoned, lists them: one entry per drive with an abandoned change,
  in UTF-8 byte order of `drive_id`. `request_message_id` is the `message_id` of
  that `session.close` or `session.drives`. `paths` holds up to 100
  drive-relative paths of that drive's abandoned changes in UTF-8 byte order,
  and `paths_total` counts all of them. `lost_changes` is omitted when nothing
  was abandoned.
- **Fitting.** The report stays within the 64 KiB limit of one HTTP JSON body.
  When it would not fit, the CLI first keeps every entry with empty `paths`, in
  entry order while they fit, then adds each entry's paths in UTF-8 byte order,
  entry after entry in entry order, until the next path would not fit.
  `paths_total` always counts every abandoned change of its drive.
- **Carrying entries over.** Until Engine acknowledges that report, the CLI
  keeps its `lost_changes` entries in the state root outside every Session's
  directory, where they survive an App Server restart and the deletion of the
  Session's data. When the connection ends before that acknowledgement, the CLI
  lists the kept entries in `lost_changes` of the ready of its next connection,
  in the order it kept them, fitted into the 64 KiB limit by the same rule; an
  entry that does not fit is dropped. It drops the kept entries once Engine
  acknowledges that ready. No other report lists them again.
- **Engine.** `computer-hub` records at most one `drive_changes_lost_error` per
  (`request_message_id`, `drive_id`), in the transaction that stores the close
  or `session.drives` report or the ready that carries the entry, with the
  entry's `drive_id`, `paths` and `paths_total` and the `computer_id` of the
  computer to which Engine sent that instruction. A repeated entry, in a
  replayed report or a later ready, records nothing, and a report Engine never
  stores records no error. Engine logs and skips an entry whose
  `request_message_id` names no `session.close` or `session.drives` that Engine
  sent to this instance, or, in a close or `session.drives` report, differs from
  the report's `request_message_id`; the report or ready is still accepted.

### Resource limits

Input and concurrency limits are checked before the corresponding allocation
or admission. Retained-report capacity is checked against the complete encoded
body before it is retained for delivery; it does not reserve a task's eventual
output before execution.

| Resource | Limit | Overflow |
|---|---|---|
| Bootstrap JSON, one HTTP JSON body, one SSE event | 64 KiB (65,536 bytes) each | Bootstrap and HTTP: rejected (`413 request_too_large` from Engine). SSE: the CLI closes the stream without processing the event |
| One local MCP tool-list report, one server per report | 4 MiB (4,194,304 bytes) | The CLI never sends a larger report; its listing fails instead ([tool lists](#tool-lists)). Engine answers a larger body with `413 request_too_large` |
| Session instructions staged during registration | 64 | The CLI abandons the connection ([registration failure](#registration-failure)) |
| Retained Session records | 128 | `resource_exhausted` for a new Session |
| Running directory probes | 8 | The next preparation waits up to 5 seconds for a slot, in arrival order, then fails with `resource_exhausted`; a timed-out probe counts until it is reaped |
| Directory probe duration | 5 seconds | `cwd_check_timeout`; the late result is discarded |
| Retained encoded report bodies per App Server | 256 MiB (268,435,456 bytes), including 8 MiB (8,388,608 bytes) reserved for compact tool errors and non-tool control replies | An ordinary result that cannot be retained becomes a compact `resource_exhausted` error. If even the reserve cannot retain a reply, the logical connection ends ([delivery](#acknowledgement-deduplication-and-delivery)) |
| Per-connection replay ledger | 4,096 entries or 16 MiB (16,777,216 bytes), whichever comes first | `resource_exhausted` for a new operation that cannot be recorded safely ([replay-ledger exhaustion](#replay-ledger-exhaustion)) |

- A Session record exists from the first `session.prepare` for that `session_id`
  until a close deletes the Session's local data.
- The CLI keeps only the latest heartbeat sequence and its ACK, never a ledger
  entry per heartbeat.
- It discards a connection's replay ledger and registration queue when that
  connection becomes invalid, and keeps rejecting stale results throughout
  cleanup.
- **Replay ledger.** It holds one entry per downstream instruction the CLI
  recorded; an entry's size is its encoded record, and input and report bodies
  are stored apart. The CLI drops an instruction's entry when the ACK of its
  final report arrives (a `tool.execute` or `mcp.call` with its `terminal` or
  `rejected` report, a control with its reply, a Session instruction with its
  Session report) and the current stream has already delivered that instruction,
  since Engine writes no instruction twice on one stream. At each resume it
  drops the remaining entries of the instructions whose final report Engine
  acknowledged before the CLI opened the new stream. Dropping an entry removes
  only its deduplication record; binding and Session records stay, so a later
  `session.close` of that binding is an ordinary close.
- **Reserve.** Replay-ledger exhaustion is an admission failure, not a
  connection loss: it never causes a disconnect, a reconnect, a binding
  invalidation or the end of admitted tasks. The CLI keeps 64 entries and 1 MiB
  (1,048,576 bytes) of the ledger in reserve for controls, closes and reports of
  admitted work: it records a new operation only while at least 64 entries and 1
  MiB stay free after it, and refuses it with `resource_exhausted` otherwise.
  Apart from the two drops above, it never makes room by evicting deduplication
  records, and it never reapplies a duplicate instruction.
- Once `tool_execution` is negotiated, the tool messages use the sizes and
  deadlines of [message sizes](#message-sizes-and-encoding) instead of the 64
  KiB and 10-second defaults. The drive transfer endpoints use their own body
  limits and deadlines ([drive transfer](#drive-transfer)).

### Error responses

An error response has the body `connection-v1.schema.json#/$defs/ErrorBody`,
`{code, message}`, with `code` from
`connection-v1.schema.json#/$defs/ErrorCode`. `message` is displayable and never
holds a credential. `409 previous_connection_active` adds `retry_after_ms` (0 to
45,000), `409 content_missing` adds `missing_sha256` (1 to 10,000 distinct
hashes), and `429 rate_limited` comes with the `Retry-After` header.

| Status and code | Meaning | What the CLI does |
|---|---|---|
| `400 invalid_message` | The body is not valid JSON; the body, a query or path parameter or a header fails the endpoint's schema; `protocol_version` is not `1`; a ready under a new `message_id` on a connection whose ready Engine already accepted or refused; a heartbeat before Engine accepted the connection's ready, or one under a new `message_id` whose `heartbeat_seq` is not above the last accepted one; a local MCP tool-list report that names one tool twice; on the drive transfer endpoints also a content upload without `Content-Length`, and a commit with no change or with one path twice | Never retries it automatically |
| `400 content_hash_mismatch` | A content upload whose bytes do not hash to the SHA-256 in its URL | Reads the file again and uploads its current content |
| `401 credential_invalid` | The key does not authenticate the named instance and credential | Invalidates the current connection, begins that connection's local cleanup and stops automatic connection attempts (`blocked`) |
| `401 credential_expired` | The credential has expired, or no positive lease grant remains before its expiry | As for `401 credential_invalid` |
| `401 credential_revoked` | The credential has been revoked | As for `401 credential_invalid` |
| `403 instance_unavailable` | The instance is deregistered, or the credential's scope forbids the request | As for `401 credential_invalid` |
| `403 cli_upgrade_required` | A ready from a CLI older than Engine's minimum supported version | As for `401 credential_invalid`: it stops connecting, and the instance shows offline with this error |
| `403 drive_access_denied` | A drive transfer request for a drive outside the binding's drive set, or a content upload or commit without `read_write` access | Ends only that request; permanent ([common rules](#common-rules)) |
| `404 not_found` | `POST /v1/client/credential` names a credential the CLI cannot fetch | Does not retry; keeps its current credential and logs the rejection |
| `404 content_not_found` | A content download whose hash no kept, unredacted version of that drive names | Permanent: the read fails at once with an I/O error |
| `409 connection_conflict` | The instance has another active connection than the supplied predecessor, or none was supplied | Retries under the reconnect backoff until 45 seconds after its App Server started; a conflict after that makes it report a competing active connection (`blocked`) and stop automatic competition |
| `409 previous_connection_active` | `resume=false` names the instance's still valid connection | Waits `retry_after_ms`, at least 1 second, cancellably, then retries |
| `409 stale_connection` | `connection_id` names a connection that is not the instance's live connection | Invalidates that connection and reconnects; on drive transfer it then retries the request with the binding of the next preparation |
| `409 stale_binding` | A drive transfer request whose `binding_id` is not a prepared or closing binding of the connection | Retries the request with the binding of the next preparation |
| `409 content_missing` | A commit names content the drive does not hold; nothing is applied | Uploads that content again and, when any change remains, sends the commit again without the writes that name those hashes ([commit](#commit)) |
| `409 message_conflict` | `message_id` was used on this connection with different content | Never retries it automatically |
| `409 unknown_instruction` | A report answers an instruction, binding or task that was not issued on this connection; a git-ticket fetch for a `binding_id` that names no such preparation; a drive sync report's `task_id` follows [drive sync report](#drive-sync-report) | Never retries it automatically |
| `410 drive_cursor_expired` | A change listing whose cursor is below the drive's newest removed change record | Lists the drive again as at a first mount, keeping its pending changes and content cache ([catch-up rules](#catch-up-rules)) |
| `413 request_too_large` | The body exceeds its limit or its complete limit of [message sizes](#message-sizes-and-encoding), or a commit exceeds its limits; a member over its own limit inside a body within its limit is `400 invalid_message`. Engine sends no ACK | Never retries it automatically |
| `413 file_too_large` | A content upload whose `Content-Length` exceeds 500,000,000 | Turns it into a `limit` rejection of that file, with the message as `rule` |
| `416 range_not_satisfiable` | A content download whose `Range` selects no byte or names more than one range | Permanent ([download content](#download-content)) |
| `429 rate_limited` | A content upload over the workspace's [upload budget](#drive-limits-and-the-upload-budget) | Temporary: sends the request again no sooner than its `Retry-After` seconds |
| `5xx` | A temporary Engine failure; its body is not defined | Retries under the connection's deadlines. On drive transfer only `500`, `502`, `503` and `504` are temporary; any other status is permanent |

- Network failures and timeouts are retried under the connection's deadlines.
- On the drive transfer endpoints, the CLI handles a `401`, a `403
  instance_unavailable` and a `409 stale_connection` as above, and every other
  status as [drive transfer](#common-rules) states.

## Tool execution

Messages: `tool-execution-v1.schema.json#/$defs/Downstream` with
`tool-execution-v1.schema.json#/$defs/ExecutePayload`,
`tool-execution-v1.schema.json#/$defs/StdinPayload` and
`tool-execution-v1.schema.json#/$defs/CancelPayload`; reports
`tool-execution-v1.schema.json#/$defs/ToolStatus`,
`tool-execution-v1.schema.json#/$defs/DriveSyncReport` and
`tool-execution-v1.schema.json#/$defs/Acknowledgement`; the capability
`tool-execution-v1.schema.json#/$defs/ToolExecutionCapability`. Fixtures:
`tool-execution-v1.fixtures.json`. A bare definition name in this part, such as
`DriveRejection`, is a definition of `tool-execution-v1.schema.json`.

Engine decides what may run; the CLI owns each admitted execution from admission
until its reports are acknowledged; restricted child processes perform the
operation. The CLI runs no Agent loop and no approval.

### Versioning and decoding

- Envelopes keep `protocol_version: 1`. The CLI declares the tool messages in
  ready as `capabilities.tool_execution.schema_version: 1`, a number that is
  neither the connection's protocol version nor `sandbox_policy.schema_version`
  nor the result envelope's `schema_version`.
- Input is UTF-8 JSON. Integers are signed 64-bit, and a decoder never
  round-trips them through floating point. A decoder rejects duplicate object
  keys, invalid Unicode (a lone surrogate included), nonfinite numbers and
  integer overflow.
- Missing, `null` and out-of-range timing fields follow the defaults and clamps
  of [tool inputs](#tool-inputs) and [controls](#controls). A schema `default`
  annotation never rewrites an instruction.
- A retransmission keeps its fields, identities and content. Deduplication
  compares more than `task_id`: JSON whitespace and object-key order are not
  changes, while an omitted field and an explicitly different value are.
- Unknown optional envelope fields are accepted when they change no meaning. New
  required meaning needs a declared capability or a new version.
  `sandbox_policy` and the managed-network rules are closed: an unknown field is
  rejected, so a misspelled field never creates a grant. The CLI validates
  everything before any side effect.

### Routing

- Engine resolves the model's computer choice before it builds an instruction.
  Tool input carries no `computer_id`: Engine removes it, together with the
  model-facing escalation fields, before dispatch.
- The authenticated connection identifies the instance. Each binding belongs to
  (instance, connection, Session); one Session may have bindings on several
  instances, and the CLI acts only on the bindings addressed to its own
  connection.
- `tool.stdin` and `tool.cancel` keep the original task's route and never move a
  task to another computer.

### Instructions

Engine sends all three in the SSE envelope. Each payload contains `session_id`,
`binding_id`, `tool_use_event_id` and `task_id`.

| Type | Model tool | Payload | Required fields | Optional fields |
|---|---|---|---|---|
| `tool.execute` | `exec_command`, `edit`, `view_image` | `tool-execution-v1.schema.json#/$defs/ExecutePayload` | `tool`, `input` (an object), `sandbox_policy` | `limits` |
| `tool.stdin` | `write_stdin` | `tool-execution-v1.schema.json#/$defs/StdinPayload` | `chars`, which may be empty | `write_seq` (a positive integer, required when `chars` is not empty), `wait_ms`, `limits` |
| `tool.cancel` | None; Engine sends it to interrupt a task | `tool-execution-v1.schema.json#/$defs/CancelPayload` | None | `reason`, `limits` |

- For `tool.stdin` and `tool.cancel`, `tool_use_event_id` identifies the control
  call and `task_id` the original execution.
- **`limits`** (`tool-execution-v1.schema.json#/$defs/VisibleLimits`) is
  `{visible_bytes?, visible_lines?}`. A missing, `null` or nonpositive value
  keeps the tool's default, 50 KiB (51,200 bytes) and 2,000 lines per stream per
  reply. A positive value may lower that limit, never raise it. Execute limits
  apply to that execution's `running` and `terminal` reports. A control's limits
  apply only to that control's reply; they never rewrite the execution's limits
  or shrink its cumulative logs. Text limits never truncate media.
- A valid `tool.execute` is itself Engine's authorization to run. The CLI starts
  after its local admission checks, without another approval and without waiting
  for any report ACK.
- **Staging behind preparation.** Engine may send a tool instruction as soon as
  it has accepted the binding's `prepared` report, while the CLI still waits for
  that report's ACK (`prepared_pending_ack`). The CLI stages such instructions
  in a per-binding queue and starts them only after the matching ACK makes the
  binding `prepared`. A close, a cancel or the connection's end prevents a
  staged task from starting and never waits behind other tasks' ACKs. Staged
  instructions count against the connection's admission capacity, the replay
  ledger; a staged instruction that does not fit gets a `rejected` report with
  `resource_exhausted`, correlated to that instruction.

### Tool inputs

| Tool | Input | Required input | Optional input and meaning |
|---|---|---|---|
| `exec_command` | `tool-execution-v1.schema.json#/$defs/ExecInput` | `cmd`, a string with at least one non-whitespace character | `cwd`; `env`, a string map; `wait_ms`; `on_wait_expiry`; `task_lifetime_ms`. An `input.task_id`, when present, must equal the outer `task_id`, or the instruction is rejected |
| `edit` | `tool-execution-v1.schema.json#/$defs/EditInput` | `path`; `old_string`, nonempty | `new_string` (missing or `null` replaces with the empty string); `replace_all` (default `false`) |
| `view_image` | `tool-execution-v1.schema.json#/$defs/ViewImageInput` | `path` | None |

**`exec_command` timing.**

- `wait_ms`, the initial wait: default 10,000 ms, clamped to 250–50,000 ms.
- `on_wait_expiry`: `kill` (the default, also for an empty string or `null`)
  ends the command at the wait's end and returns `timed_out` with the partial
  output; `detach` returns a `running` report, and the CLI keeps managing the
  task and reports its completion itself.
- `task_lifetime_ms`, a detached command's lifetime measured from its actual
  start: default 1,800,000 ms (30 minutes), clamped to 250–3,600,000 ms.
  `tool.stdin` never extends it.
- Foreground and background commands share one task management; the only
  difference is whether the task still runs when the first report is made.
- For the model's `exec_command`, Engine sends `on_wait_expiry: "detach"` and
  omits `task_lifetime_ms`, so a command still running when its wait ends keeps
  running for at most 30 minutes from its start. Engine sends the model's
  `workdir` as `cwd`, and the computer's `working_directory` when the model
  gives none.

**`edit`.** It validates and builds the replacement in memory, then writes directly to the existing file. It requires no writable parent directory and creates no sibling temporary file. A failed or interrupted write can leave partial content; no special permission restoration or explicit sync is promised. Each failure is a terminal result with
`status: error`, `result: {}` and an `error.kind` that names it:

| `error.kind` | When |
|---|---|
| `invalid_input` | `old_string` equals `new_string`, or the file is not valid UTF-8 |
| `too_large` | The file is over 32 MiB (33,554,432 bytes), or the edited content would be |
| `match_not_found` | No span matches |
| `match_ambiguous` | More than one span matches and `replace_all` is not `true` |

Matching is an exact substring scan. Only when it finds nothing, one fallback
pass maps U+2018, U+2019, U+201A and U+201B to `'` and U+201C, U+201D, U+201E
and U+201F to `"` in both the file and `old_string` and scans again. The
fallback only locates spans: the bytes written are the file's original bytes
outside the replaced spans and `new_string` as given. No other normalization
applies.

**`view_image`.** It reads a PNG, JPEG, GIF or WebP image, recognized by its
content. Any other file, a PDF included, returns an explicit error, and so does
an image outside [image acceptance](#image-acceptance). A PDF is read through
`exec_command` with tools on the machine: text extraction, and page rendering to
PNG for `view_image`.

**Working directory and paths.**

| Operation | Behaviour |
|---|---|
| A new command without `cwd` | Starts in the binding's canonical working directory; an earlier command's `cd` never changes it |
| A relative `cwd` or tool path | Resolved against the binding's working directory |
| An absolute `cwd` or tool path | Used as given, subject to the call's `sandbox_policy` |
| A per-call `cwd` | Affects only that call; never the binding, another call or the App Server's own directory |
| `tool.stdin` to a running command | The command keeps its own directory |

- A path or a `cwd` grants no permission.
- Each operation checks the directories it depends on when it starts. A missing
  working directory gives the terminal error `cwd_not_found`, a non-directory
  `cwd_not_directory`, and denied access `cwd_inaccessible`, also when entering
  the directory fails at process launch. The CLI never falls back to another
  directory, never recreates the missing one and never reruns the failed call.
  Relative operations and commands using the default directory fail while it is
  missing; an operation on an independent absolute path proceeds when its target
  and grant are valid. Restoring the directory lets a new call succeed.
- The CLI's credentials and control environment variables never reach a child,
  through inheritance or `env` overrides.
- Every command of a Session runs with `TETRAL_DRIVES` set as [drives on a
  machine](#drives-on-a-machine) states.
- There are no `read`, `write`, `apply_patch`, `grep` or `glob` tools; reads,
  new files and searches go through `exec_command`.

### Controls

- **`tool.stdin` with empty `chars`** reads the task's output and state without
  writing and without starting a process. `wait_ms`: default 5,000 ms, clamped
  to 0–50,000 ms; `0` answers at once. It returns at once when output is
  available or the task has ended; otherwise it waits for new output, the task's
  exit or the wait's end. At the wait's end it reports the actual running state
  and current output; it never ends the task or extends its lifetime.
- **`tool.stdin` with nonempty `chars`** writes at most 32 KiB (32,768 bytes) of
  raw UTF-8 to the task, then waits and returns the bounded output,
  `written_bytes` and the task state. `wait_ms`: default 250 ms, clamped to
  250–30,000 ms; the task's exit returns early. A `chars` of exactly one U+0003
  sends SIGINT to the process group; any other content is written unchanged. A
  partial write reports the actual byte count. No terminal (PTY) is provided.
- **The model's `write_stdin`.** With empty `chars`, Engine omits `wait_ms` when
  the model gives no `yield_time_ms`, and otherwise sends `max(yield_time_ms,
  5000)`. With nonempty `chars`, Engine sends `yield_time_ms` as `wait_ms`, and
  omits `wait_ms` when the model gives none, so the 250 ms default applies. The
  clamps above apply in both cases.
- **Write sequence.** Engine numbers a task's nonempty writes from 1, adding 1
  for each, and allocates the number durably when it records the `write_stdin`
  call, so every retransmission carries the same `write_seq`. Writes to one task
  are serialized in arrival order. The CLI records each `write_seq` with its
  content and actual outcome. A duplicate message replays its original report
  without writing again or reading output again. A `write_seq` reused with
  different content is a conflict (`write_conflict`). Another control message
  that repeats a processed `write_seq` with identical content gets a new reply
  that reuses the recorded write result, with no new write and no new output
  read. A `write_seq` at or below the highest one recorded for the task that
  matches no recorded write is rejected with `invalid_input`. Retries never
  repeat bytes already written.
- `tool.stdin` works only on an `exec_command` task; for any other task it
  returns `unsupported_control`.
- **Output windows.** The first `running` report and the stdin replies share one
  per-task incremental output window. To make a reply, the CLI selects a bounded
  window and advances the window position. If admitted for delivery, the
  encoded report is immutable and retained in memory; later output goes into
  the next window. A retry resends those same bytes and reads nothing. Cumulative bounded logs (head and tail, with markers and
  counters) are kept separately, and the terminal output comes from them, so
  reads never consume output the final report needs. Discarded middle output
  cannot be recovered.
- **`tool.cancel`** asks the CLI to end the task; the CLI owns termination and
  exit collection. It records the intent at once, stops further writes and
  interrupts a blocked write. The cancel's reply confirms receipt, not exit; the
  terminal report gives the outcome. Cancellation applies to every admitted
  task.
- **Turn end.** When the Agent's turn ends or is interrupted, Engine sends
  `tool.cancel` for every `exec_command`, `edit`, `view_image` and local MCP
  call still waiting for its result. A `write_stdin` call still waiting for its
  reply gets no `tool.cancel`: Engine stops waiting for it, acknowledges and
  drops its late reply, and the command keeps running. Commands whose `running`
  result reached the Agent keep running; a command whose `running` report Engine
  stored but whose result the Agent had not received when its turn was
  interrupted gets `tool.cancel`.
- Waiting for output never blocks SSE reading, heartbeats, other tasks or this
  task's cancellation. Waiting controls finish on their own and report what
  actually happened. A task's exit produces exactly one terminal report whatever
  controls are pending. Control replies and the terminal report may arrive in
  any order; each is acknowledged by its own identity and neither replaces the
  other. A stdin failure caused by the task's exit may carry the known terminal
  outcome but never replaces the terminal report.
- After the task exits, admitted controls still save their reports, and the CLI
  keeps the data that unacknowledged reports need. A new output read after
  cleanup returns `result_unavailable`. A control for a task that does not exist
  on this binding returns `task_not_found`; another Session's task can never be
  controlled.

### Permissions: `sandbox_policy`

Each `tool.execute` carries the call's complete effective permissions as
`sandbox_policy` (`tool-execution-v1.schema.json#/$defs/SandboxPolicy`, with
`tool-execution-v1.schema.json#/$defs/FilesystemPolicy`,
`tool-execution-v1.schema.json#/$defs/FilesystemRule`,
`tool-execution-v1.schema.json#/$defs/NetworkPolicy`,
`tool-execution-v1.schema.json#/$defs/ManagedNetworkRules`,
`tool-execution-v1.schema.json#/$defs/NetworkRule`,
`tool-execution-v1.schema.json#/$defs/NetworkDestination`,
`tool-execution-v1.schema.json#/$defs/NetworkListen` and
`tool-execution-v1.schema.json#/$defs/PortRange`). Engine's permission component
(ToolGate) decides them; the CLI validates them, adds the protections below and
enforces them on the tool process and all its children. No label, mode name or
platform script is sent, and the CLI infers no permission from missing fields.
`schema_version` (`1`), `filesystem` and `network` are all required.

```json
{
  "schema_version": 1,
  "filesystem": {
    "rules": [
      {"path": "/", "access": "read"},
      {"path": "/srv/project", "access": "read_write"}
    ]
  },
  "network": {"mode": "disabled"}
}
```

**Filesystem rules.**

- `filesystem.rules` is an array of `{path, access}`: an absolute path and
  `deny`, `read` or `read_write`. A directory rule covers its descendants; the
  more specific path wins. Two rules with different access for one path are
  rejected; list order never decides. There are no wildcards. A user path that
  no rule covers is denied; an empty array grants no ordinary path.
- Inside each `read_write` rule's path, the top-level `.git`, and the git
  directory that a `.git` file there points to, stay read-only. This belongs to
  the rule, not to the protections below: a `read_write` rule for `/` leaves
  only `/.git` read-only. No other folder name is kept read-only.
- Engine never sends rules for the Session's drive mounts, because only the CLI
  knows their paths; the CLI adds them as below. No tool parameter selects
  another Session's mount.

The CLI adds these protections before every execution. No rule overrides them,
however broad:

| Resource | Tool access |
|---|---|
| Tetral credentials, control state and internal communication endpoints | Denied |
| Other Sessions' drive mounts, temporary files and results | Denied |
| This Session's drive mounts | Read-only or read-write according to each drive binding's `access`; read-only for every mount when the policy has no `read_write` rule |
| The content cache and each Session's sync state | Denied; only the CLI's sync component uses them |
| Trusted CLI and tool programs | Readable and executable, not writable |
| This task's private temporary directory | Read-write, isolated from other tasks |

Intentionally shared project directories are not Session-private;
`sandbox_policy` decides access to them.

**Network rules.** `network.mode` is one of:

| Mode | Behaviour |
|---|---|
| `disabled` | The operating system blocks networking for the tool process and its children |
| `direct` | The host network is used with no additional denial: TCP, UDP, public networks, LAN, localhost and local services as the host allows. No proxy and no public-only rule |
| `managed` | Access only through a trusted proxy that enforces the rules, with direct bypass prohibited |

- `disabled` and `direct` carry no rules. `managed` requires `rules` with three
  arrays, `allow`, `deny` and `listen`; missing rules never mean `direct`.
- An `allow` or `deny` entry is `{destination, transport, ports}`. `destination`
  is `{kind: "host", value}` or `{kind: "cidr", value}`; `transport` is `tcp` or
  `udp`; `ports` is a nonempty array of inclusive `{from, to}` ranges within
  1–65,535 with `from` not greater than `to`.
- A host is `*`, a name, or `*.` followed by a name. A name is converted as a
  whole with UTS #46 ToASCII (nontransitional processing, UseSTD3ASCIIRules,
  CheckHyphens, CheckBidi and CheckJoiners), which also lowercases it and maps
  the full stops U+3002, U+FF0E and U+FF61 to `.`; then one trailing dot is
  removed. A value holding `/`, `:`, `@`, `?` or `#`, a name that the
  conversion rejects, and a result longer than 253 characters, with a label
  outside 1 to 63 letters, digits and hyphens or beginning or ending with a
  hyphen, or in the form of an IPv4 address, are not hosts. `*.example.com`
  matches subdomains, not `example.com`. URLs, credentials, paths and regular
  expressions are not hosts.
- An IP destination is a CIDR: an IPv4 address in dotted decimal without
  leading zeros or an IPv6 address in RFC 4291 text without a zone, then `/`
  and a decimal prefix length without leading zeros, at most 32 or 128, with
  no host bits set; `/32` or `/128` names one address. The netmask form, such
  as `10.0.0.0/255.0.0.0`, is not a CIDR. A `listen` `address` is an IPv4 or
  IPv6 address in the same text, without a prefix or a zone.
- Host rules match the requested name; CIDR rules match the actual connection
  address. Any `deny` match rejects; otherwise an `allow` match is required. An
  alias domain is not the same host. The proxy resolves DNS itself and checks
  the resolved addresses before every connection. An IPv4-mapped IPv6 address is
  the same address.
- A `listen` entry is `{address, transport, ports}` for a permitted local
  endpoint. Listening grants never cover Tetral's control endpoints and never
  promise external reachability.
- An empty `allow` prohibits ordinary outbound access; an empty `deny` adds
  nothing; an empty `listen` grants no listening. There is no global public-only
  or TCP-only rule.
- The CLI advertises `managed` only once it can enforce it ([ready capability
  declaration](#ready-capability-declaration)); until the managed-network proxy
  exists it advertises only `disabled` and `direct`, and Engine never sends
  `managed` to it. The CLI rejects a requested capability it cannot enforce
  before execution and never falls back to `direct`. DNS and bridging inside the
  proxy serve only authorized destinations.
- Network rules apply to the tool process and its children, never to the CLI's
  own connection to Engine: a network-disabled tool still has its results and
  heartbeats reported. An actual network failure is returned as a tool result,
  not as a denial.

**Binding and rejection.**

- Permissions are bound to the `session_id`, `binding_id` and `task_id` of their
  instruction. A retransmission keeps them. Different permissions need a new
  authorization and a new `task_id`.
- A settings change affects only later authorizations; it never rewrites an
  issued one, a task not yet started included. Engine withdraws an issued
  authorization only with `tool.cancel`; a running process never changes its
  permissions.
- Engine merges any approved additional permission into one complete policy for
  a new task. The CLI has no elevation request, no permission request message
  and no local approval state, and never merges partial grants.
- Every call, however broad its grant, runs through the same restricted path. No
  grant disables isolation.
- The CLI rejects an execution, before any side effect, when `sandbox_policy` is
  missing or invalid, its `schema_version` is unsupported, or the machine cannot
  enforce it. When the platform isolation itself is unavailable, the rejection's
  code is `sandbox_unavailable`.
- A local MCP call carries no `sandbox_policy` ([MCP proxy](#mcp-proxy)).

### Reports

Every tool report is `POST /v1/client/tool-status`
(`tool-execution-v1.schema.json#/$defs/ToolStatus`:
`tool-execution-v1.schema.json#/$defs/ExecutionReport` or
`tool-execution-v1.schema.json#/$defs/ControlReport`). It carries `kind` and the
identities of the instruction it answers, `request_message_id` being that
instruction's `message_id`. The CLI sends it; Engine ACKs it.

| `kind` | Fields | Answers |
|---|---|---|
| `execution`, `phase: running` or `terminal` | `report_seq` (a positive integer) and `result`; no outer `error`. A `terminal` report's `result` carries `metrics` | The original `tool.execute` or `mcp.call`, its later completion included |
| `execution`, `phase: rejected` | `error`; no `report_seq` and no `result` | The instruction that was not admitted |
| `control` | `result` or `error`, or both when effects preceded the error (a partial write); no `phase` and no `report_seq` | Its own `tool.stdin` or `tool.cancel` |

- The outer `error` is `{code, message}`
  (`tool-execution-v1.schema.json#/$defs/ProtocolError`). The inner tool error
  is `{kind, message, path?, detail?}`
  (`tool-execution-v1.schema.json#/$defs/ToolError`). A failure after execution
  started, a nonzero exit, a timeout or a cancellation is a `terminal` result,
  never `rejected`.
- `rejected` means this instruction was not admitted. It never marks another
  task failed and never uses that task's report sequence.
- `running` and `terminal` reports use `report_seq` from 1. A task that ends
  within its first wait sends `terminal` with `report_seq: 1`. A task still
  running then sends `running` with `report_seq: 1` and, on exit, `terminal`
  with `report_seq: 2`. Its `request_message_id` and `tool_use_event_id` are the
  original instruction's, never a later control's. A `running` report refused
  by retained-report capacity is never published and consumes no sequence;
  its eventual compact terminal error is the first report, with `report_seq: 1`.
- Background completion needs no polling: the CLI observes the exit, collects
  the bounded output, saves the report and sends `terminal` on its own.
  Generating and sending `terminal` never waits for the `running` report's ACK.
  A `running` ACK confirms that report's persistence, not completion.
- Reports may arrive in any order and are saved and acknowledged independently.
  A late `running` never overwrites `terminal`; `terminal` never returns to
  `running`. An older sequence alone never justifies dropping an unsaved reply.
- Control replies and the execution's reports are separate: a rejected write
  does not fail the task, and a cancel receipt does not prove exit. The two
  report kinds never exchange `tool_use_event_id` for one task. A nonempty inner
  `task_id` equals the outer one.

Rejection codes (`tool-execution-v1.schema.json#/$defs/RejectionCode`). Engine
settles every `rejected` call once, as not executed, with the code and message.

| Code | Meaning | Engine also |
|---|---|---|
| `invalid_input` | The input or policy fails validation, an unknown tool, an inner task identity conflict, or any other schema failure of the instruction | |
| `task_conflict` | The `task_id` was admitted with a different call, input or permissions; the original task, grant and result are kept | |
| `unsupported_policy` | A `sandbox_policy` version, network mode or managed capability the CLI cannot enforce | |
| `sandbox_unavailable` | The platform isolation itself is unavailable | Records one `computer_sandbox_error` for the developer at the first such rejection on each connection |
| `binding_unavailable` | The binding is not `prepared` on this connection | |
| `resource_exhausted` | The replay ledger cannot record the new instruction safely ([replay-ledger exhaustion](#replay-ledger-exhaustion)) | Keeps the resource-exhaustion reason; the rejection never authorizes an automatic new attempt |
| `mcp_server_unavailable` | An `mcp.call` whose server cannot be reached ([MCP admission](#mcp-admission)) | Settles the call as "tool not available" ([settlement of local MCP calls](#settlement-of-local-mcp-calls)) |

Control error codes (`tool-execution-v1.schema.json#/$defs/ControlErrorCode`). A
control error answers only its control and never changes the target execution.

| Code | Meaning |
|---|---|
| `unsupported_control` | `tool.stdin` for a task that is not an `exec_command` |
| `task_not_found` | The task does not exist on this binding |
| `write_conflict` | A `write_seq` reused with different content |
| `invalid_input` | A `write_seq` at or below the highest recorded one that matches no recorded write, or any schema failure of the control |
| `result_unavailable` | A new output read after the task's cleanup |
| `stdin_closed` | A nonempty write to a task whose process exited or closed its stdin |
| `resource_exhausted` | The control cannot be admitted |

### Result envelope

`result` is the result envelope,
`tool-execution-v1.schema.json#/$defs/HelperEnvelope`, with `schema_version: 1`.

- `tool` is `exec_command`, `edit`, `view_image` or `mcp` in execution reports,
  and `stdin` or `cancel` in control reports.
- `status` is `success`, `error` or `running`. `running` occurs only for
  `exec_command`, `stdin` and `cancel`, and an execution report's `phase` is
  `running` exactly when `status` is `running`.
- `error` is `null` unless `status` is `error`. `drive_sync` follows [drive sync
  outcome](#drive-sync-outcome).
- **`metrics`.** Every `terminal` execution report's `result` carries
  `{wall_time_ms, queued_ms, drive_wait_ms, run_ms, sync_ms}`, integer
  milliseconds on the CLI's monotonic clock: `wall_time_ms` from receiving the
  instruction to building the report; `queued_ms` from receiving it until it
  leaves admission and any staging queue; `drive_wait_ms` the drive check before
  the work starts; `run_ms` the work itself (from process start to exit for
  `exec_command`, the edit, the image read or the MCP call); `sync_ms` the sync
  point after it. A phase that does not occur is `0`. `running` and control
  reports may omit `metrics`. Engine builds the CLI's part of the call's trace
  from these values.

| Inner `result` of | Definition | Rule |
|---|---|---|
| `exec_command`, `stdin`, `cancel` | `tool-execution-v1.schema.json#/$defs/ExecResult`, with `tool-execution-v1.schema.json#/$defs/StreamSnapshot` for `stdout` and `stderr` | `cancelled`, `was_running` and `written_bytes` appear when they apply |
| `edit` | `tool-execution-v1.schema.json#/$defs/EditResult` | |
| `view_image` | `tool-execution-v1.schema.json#/$defs/MediaResult` | `mime` is `image/png`, `image/jpeg`, `image/gif` or `image/webp` |
| `mcp` | `tool-execution-v1.schema.json#/$defs/McpResult` | The server's `CallToolResult`, or `{}` with an error ([MCP result](#mcp-result)) |

- Each reply returns at most 50 KiB and 2,000 lines per stream, with truncation
  markers and counters, under head and tail retention.
- Invalid and incomplete UTF-8 bytes are omitted without replacing them or
  hiding later valid text. That decoding alone adds no truncation marker.
  Limit truncation counts omitted original byte ranges once; a complete
  character split by a retention boundary is omitted as a whole character.
- Trimming never changes identities, report sequences, the phase, the exit
  state, counters or `drive_sync`; their space is reserved first. Media is never
  truncated.
- A `view_image` result holds exactly one complete image: `status: success`,
  `truncated: false`, `size_bytes` equal to the decoded size, canonical Base64.
  Base64 is never truncated, and one image is never split across reports.

### Image acceptance

An image reaches the Agent only when every supported model accepts it. The CLI
reads an image's format, width and height from its header, for PNG, JPEG, GIF
and WebP, and accepts the image when all of these hold:

- its decoded size is at most 7,500,000 bytes, so its Base64 is at most
  10,000,000 bytes;
- its width and its height are each at most 8,000 pixels;
- it covers at most 30,000 patches of 32×32 pixels: ⌈width ÷ 32⌉ × ⌈height ÷ 32⌉
  ≤ 30,000.

A `view_image` of an image that is not accepted ends `status: error` with kind
`too_large` and the message `image is <size_bytes> bytes and <width>x<height>
pixels; the limit is 7500000 bytes, 8000 pixels per side and 30000 patches of
32x32 pixels. Shrink or compress the image, then view it again.` In a local MCP
result, each image that is not accepted becomes one text block ([MCP
media](#mcp-media)). These limits are separate from the byte budgets of [message
sizes](#message-sizes-and-encoding), which bound transport only.

### Drive sync outcome

Tool calls check and sync the Session's drives ([sync rules](#uploads)):

- **The check before each `exec_command` or `view_image`.** Before an
  `exec_command` starts or a `view_image` reads, the CLI mounts again any of the
  Session's drives that are not mounted and applies pending remote changes
  ([catch-up rules](#catch-up-rules)). It does not wait for this machine's own
  uploads.
- **The sync point of each `exec_command`, `write_stdin` or `edit` result.**
  Before such a result is reported, the CLI commits the Session's drive changes
  as [uploads](#uploads) states.

The result envelope carries `drive_sync`
(`tool-execution-v1.schema.json#/$defs/DriveSyncOutcome`, with
`tool-execution-v1.schema.json#/$defs/DriveRejection`,
`tool-execution-v1.schema.json#/$defs/DriveNotSynced`,
`tool-execution-v1.schema.json#/$defs/DriveDownload`,
`tool-execution-v1.schema.json#/$defs/DriveMountFailure` and
`tool-execution-v1.schema.json#/$defs/DriveWriter`) when any drive file was
created, modified or deleted, anything was rejected or skipped, a drive's mount
failed as a new failure, or, in a `running` result, any of the Session's drive
files is downloading. Paths are absolute paths on the machine.

| Field | Meaning |
|---|---|
| `committed` | Number of drive files created or modified |
| `deleted` | Number of drive files deleted |
| `rejected` | Up to 100 `DriveRejection` entries. `change` is `write` or `delete`; `reason` is below. A `conflict` carries the path's current version (a deletion included) and its writer, and neither when the drive keeps no version of the path; a `path_rule` or `limit` carries Engine's message naming the rule or limit in `rule`. For every entry the CLI has already restored the drive's state on the machine: the current version, or no file where the drive has none |
| `rejected_total` | Number of rejected files, present only when entries were left out |
| `not_synced` | Up to 100 `DriveNotSynced` entries, with `reason` below |
| `not_synced_total` | Present only when entries were left out |
| `downloading` | Only in a `running` result: up to 100 `DriveDownload` entries for every drive file of the Session still downloading, because the CLI cannot tell which process a read came from |
| `downloading_total` | Present only when entries were left out |
| `unmounted` | `DriveMountFailure` entries for drives whose mount failed at this call's check as a new failure, with the operating system's error. Only in the first result of an `exec_command` or `view_image`; never in a control result or a drive sync report |

| Rejection `reason` | Meaning |
|---|---|
| `conflict` | The drive's version of the path moved on since the change's base ([the drive wins](#uploads)) |
| `path_rule` | The path breaks a [drive path rule](#drive-paths) |
| `limit` | The change raises bytes or files beyond a [limit](#drive-limits-and-the-upload-budget) |

| `not_synced` `reason` | Meaning |
|---|---|
| `git_repository` | The path is inside a directory that contains `.git`, which is never synced |
| `symlink` | A symbolic link, which is never synced |
| `special_file` | A file that is not a regular file, which is never synced |
| `uploading` | The change was not committed at this sync point; its upload and commit go on |
| `upload_pending` | Its upload or commit failed with a fault; the change stays on the machine until the CLI tries it again |

`drive_sync` takes at most 64 KiB (65,536 bytes) of compact JSON. When entries
do not fit, the CLI keeps the first entries that fit, in this order:
`unmounted`, then `rejected`, then `not_synced`, then `downloading`, each list
in path order, and reports the full counts in the totals. A `drive_sync` may
hold only totals when not even one entry fits.

### Drive sync report

A rejection that no tool result reports ([uploads](#uploads)) goes upstream as
`POST /v1/client/drive-sync`
(`tool-execution-v1.schema.json#/$defs/DriveSyncReport`), with an optional
`task_id` and a `drive_sync` that never holds `downloading` or `unmounted`. It
follows the tool-status ACK and retry rules.

- The CLI records on each pending change the task that last wrote it, and sends
  one report per task, with that task's `task_id`. On a Mac, where the mount
  does not learn the writing process, no change records a task. The rejections
  of changes with no recorded task go in a report without `task_id`.
- The CLI keeps each pending change's task in the Session's sync state, across
  connections and App Server restarts. Engine accepts a `task_id` that names a
  task it issued for that Session on any connection; only `binding_id` must
  belong to this connection.
- A drive sync report (`tool-execution-v1.schema.json#/$defs/DriveSyncReport`)
  is built fresh on the current connection and may name a task of an earlier
  connection.
- Engine delivers a report with `task_id` to the thread that made that task's
  call, or to the main thread when that thread is closed, and a report without
  `task_id` to the main thread. It delivers each `message_id` once and never
  starts a run: a running Session's thread gets it before its next model call,
  an idle Session's at its next turn.

### Acknowledgement, deduplication and delivery

- The ACK (`tool-execution-v1.schema.json#/$defs/Acknowledgement`) names the
  report's connection and message. The CLI releases a report only after its
  matching ACK; a wrong or late ACK for another identity releases nothing.
- Engine verifies that a report belongs to an instruction, connection and
  binding it issued (a drive sync report's `task_id` follows the [drive sync
  report](#drive-sync-report) rule), persists it, then ACKs. For a duplicate
  report it returns the original ACK without settling again.
- The CLI completes task registration and duplicate checks before any side
  effect, for every tool and control.

| Situation | CLI behaviour |
|---|---|
| An identical instruction arrives again | Reuses the running task or saved report; starts nothing and writes no input again |
| The same `task_id` with a different call, input or permissions | Rejects the new instruction (`task_conflict`) and keeps the original task, grant and result |
| The same `message_id` with different content | Reports a protocol error (`message_conflict`) |
| A report upload fails or its ACK is lost | Resends the saved report with the same `message_id`, sequence and content; never re-executes and never rereads output |

- The CLI encodes each admitted report once and retains its immutable body in
  bounded memory for sending and retries; it writes no report body to disk and
  cannot recover those bodies after a CLI restart. Releasing a report keeps the
  connection's deduplication records and acknowledged report identities until
  the CLI drops them as [resource limits](#resource-limits) state, so a valid
  duplicate instruction never executes again. Acknowledged bodies are not kept
  for replay; Engine uses the reports it persisted.
- Ordinary results, including successful tool-control replies, cannot consume
  the 8 MiB reserve. Compact tool errors and non-tool control replies try the
  ordinary capacity first, then the reserve. A refused ordinary result becomes
  a compact `resource_exhausted` error. If a running command's report is refused,
  the CLI stops the command and waits for confirmed process disappearance
  before publishing its terminal error; this does not undo work already done.
  If even the reserve cannot retain a reply, the CLI ends the logical connection
  and drops pending reports, so results not confirmed by Engine may remain
  unknown. The budget bounds retained encoded bodies, not total process memory.
- After a task completes and its reports are acknowledged, the CLI cleans up its
  releasable logs and temporary files. It never deletes project files written by
  commands.

### Settlement

Engine settles each model call exactly once:

| Situation | Outcome |
|---|---|
| The original task runs normally and a later conflicting message is rejected | The call gets the original task's actual result |
| The call is confirmed not executed (a `rejected` report, or a dispatch that confirmably never reached admission) | An error stating that the call failed to dispatch and did not run |
| Execution may have happened but no reliable result exists | An unknown outcome stating that effects may exist; never rerun automatically |
| A protocol error cannot be tied to a call | A connection-level record; each pending call keeps its own lifecycle |

### Disconnection and task lifecycle

A task belongs to the connection and binding that admitted it.

| Event | CLI behaviour |
|---|---|
| A report upload fails temporarily while the connection is valid | Keeps and retries the report; never re-executes or cancels because of it |
| The stream closes while the connection is valid | Keeps tasks, bindings and report delivery; resumes the stream; deduplicates resent instructions by `message_id` |
| The connection ends: its deadline passes without a resume, a connection-ending `disconnect`, or an invalid connection | Stops admission and writes on that connection, discards staged tasks, and begins ending running tasks at once; nothing runs past the connection deadline |
| One binding closes | Prevents its pending tasks from starting and cancels its running tasks; other Sessions continue |
| A task is cancelled | Affects only that task; reports the cancel receipt separately from the actual end |
| A new connection is online | Uses new identities and new bindings; old tasks, reports and callbacks never update them or trigger re-execution |
| The App Server shuts down | Stops admission and cancels all tasks within the bounded shutdown |

- Network loss longer than the lease ends tasks and can lose undelivered
  results. Effects already made are not rolled back; a completed file write is
  neither undone nor repeated.
- On macOS a task's managed descendants are the processes of its process group.
  A process that leaves the group, for example through `setsid` or a double
  fork, is neither ended nor waited for: it keeps running after its task ends,
  under the restrictions it inherited, and no report names it. The cleanup of a
  task's descendants and a close's wait for every managed descendant cover the
  group only.
- A cancellation or termination signal does not confirm exit. While an old
  execution may still run, the CLI admits no replacement execution for that
  Session on that instance (`cleanup_pending` at preparation); other Sessions
  and connection upkeep continue. When completion races a cancellation, the
  observed terminal outcome stands; a later disconnection never rewrites a
  completed success as cancelled.
- No report crosses connections. When a connection becomes invalid, the CLI
  stops retrying its reports and may delete them; it never submits them on a new
  connection.
- Results Engine already persisted stay valid even when their ACKs were lost.
  Without a reliable terminal report, Engine never infers non-execution or
  successful cancellation from a disconnection: it settles the call as an
  unknown outcome and never reruns it, also for tasks that completed locally but
  whose results never arrived.
- An App Server restart recovers neither tasks nor pending reports. Process
  cleanup and result delivery are independent facts.

### Ready capability declaration

`capabilities.tool_execution` in ready
(`tool-execution-v1.schema.json#/$defs/ToolExecutionCapability`, with
`tool-execution-v1.schema.json#/$defs/ManagedNetworkCapability` and
`tool-execution-v1.schema.json#/$defs/TransportLimits`):

| Field | Meaning |
|---|---|
| `schema_version` | `1` |
| `tools` | Locally available tools among `exec_command`, `edit`, `view_image` |
| `controls` | Available controls among `stdin`, `cancel` |
| `sandbox_policy_versions` | Enforceable `sandbox_policy` versions: `[1]` |
| `network_modes` | Enforceable modes among `disabled`, `direct`, `managed` |
| `managed_network` | Present exactly when `managed` is advertised: `{transports, listen}`, the enforceable subset of `tcp` and `udp`, and whether listening rules can be enforced. A proxy program's UDP support alone does not establish support through the isolated channel |
| `limits` | The exact byte limits of [message sizes](#message-sizes-and-encoding), with the fixed field names and values of `tool-execution-v1.schema.json#/$defs/TransportLimits` |

- The CLI advertises only what its startup checks establish. Advertising any
  tool requires `cancel`; advertising `exec_command` requires both `stdin` and
  `cancel`.
- Engine dispatches only what the capability intersection supports, and sends no
  tool instruction to a CLI without `tool_execution`. A missing tool, policy
  version or network capability is never discovered only after execution. A
  capability is not an authorization; each `sandbox_policy` still states its
  scope.

### Message sizes and encoding

Sizes are bytes of complete compact UTF-8 JSON: the JSON text without
insignificant whitespace, every string and number token counted as written,
escapes included, with `input` and `result` embedded as objects, never as JSON
strings. Base64 uses the
standard alphabet without line breaks. A message's metadata is its JSON after
replacing large content with an empty value of the same type (`input` and an
`mcp.call`'s `arguments` with `{}`, `chars` with `""`, a report's `result` with
`{}`); identities, permissions, errors and unknown optional fields together take
at most 64 KiB (`metadata_json_bytes`).

Engine measures `tool.execute` input using `runtimecontrol.MarshalDataJSON`
and embeds that same measured `json.RawMessage` in the instruction. It encodes
the complete instruction once and preserves those exact bytes through storage,
first delivery and replay. Stored text or bytes must not be reconstructed from
`jsonb`, since re-encoding can change whitespace and escaping. The designated
encoder disables HTML escaping and adds no newline; all remaining escapes,
including U+2028 and U+2029, count toward the limits. Boundary tests must cover
input, metadata and complete-envelope limits with dense `<`, `>`, `&` and
U+2028 content, and compare the measured, stored and transmitted bytes.
These are sender requirements; receiver validation alone does not establish
that persistence and replay preserve the measured representation.

| Object | Limit in bytes | `TransportLimits` field |
|---|---|---|
| `tool.execute` `input` JSON | 4 MiB = 4,194,304 | `tool_input_json_bytes` |
| Complete `tool.execute` SSE JSON | 4 MiB + 64 KiB = 4,259,840 | `execute_sse_json_bytes` |
| `tool.stdin` `chars`, raw UTF-8 | 32 KiB = 32,768 | `stdin_utf8_bytes` |
| Complete `tool.stdin` SSE JSON | 256 KiB = 262,144 (6 bytes of worst-case escaping per byte plus 64 KiB) | `stdin_sse_json_bytes` |
| `tool.cancel`, ready, ACK, protocol-error and every message not listed here or in [call instruction](#call-instruction) | 64 KiB = 65,536 | `default_message_json_bytes` |
| Message metadata | 64 KiB = 65,536 | `metadata_json_bytes` |
| Ordinary result envelope | 256 KiB = 262,144 | `helper_result_json_bytes` |
| `drive_sync` JSON | 64 KiB = 65,536 | `drive_sync_json_bytes` |
| Complete ordinary tool-status | 320 KiB = 327,680 | `ordinary_tool_status_json_bytes` |
| Complete drive sync report | 128 KiB = 131,072 | `drive_sync_report_json_bytes` |
| Decoded `view_image` image; all decoded media of one `mcp` result | 10 MiB = 10,485,760 | `view_image_decoded_bytes` |
| Complete media tool-status | 14 MiB = 14,680,064 | `media_tool_status_json_bytes` |

- A route's read limit counts raw bytes, whitespace included; every other limit
  counts compact JSON. Engine answers a request body over its complete limit (a
  complete ordinary or media tool-status, a complete drive sync report, a local
  MCP tool-list report, and the 64 KiB of every other message) with `413
  request_too_large`, and a member over its own limit inside a body within its
  limit (a result envelope, `drive_sync`, decoded media, metadata) with `400
  invalid_message`; neither gets an ACK.
- Media is identified by the original tool and result type; no field can enlarge
  a bound. The receiver validates the Base64, the decoded byte count,
  `size_bytes`, the MIME type and the correspondence to the original task; a
  declared size alone is never trusted.
- The CLI first reads one SSE event within 4,259,840 bytes, then applies the
  type-specific and metadata budgets; it never applies a 64 KiB limit before it
  knows the type. These extensions apply only after `tool_execution` is
  negotiated; before that, every message keeps the 64 KiB limit. An oversized
  SSE event closes the stream without executing any part of it.
- Engine reads at most 14 MiB of a tool-status body, then validates the actual
  type, the original call and each layer's budget; it relies neither on
  `Content-Length` alone nor on unbounded decoding. An oversized body gets `413
  request_too_large` and no ACK.
- Senders enforce the same bounds before saving and sending, so no report is
  made that could never be accepted. Every hop (HTTP, SSE, the reference control
  server, reverse proxies and the CLI's internal worker framing) respects them. Compression never changes
  the decompressed limits; unsupported compression is rejected.

### Large-report deadlines

- Every upstream report whose limit exceeds 64 KiB gets a total deadline of `10
  seconds × max(1, ceil(complete JSON bytes / 65,536))`: a tool-status or drive
  sync report once `tool_execution` is negotiated, and a local MCP tool-list
  report. That is 10 seconds up to 64 KiB, 20 seconds for a 128 KiB drive sync
  report, 640 seconds for a 4 MiB tool-list report and 2,240 seconds for a 14
  MiB report.
- The connection and handshake, any interval of the upload with no byte
  progress, and the wait for the ACK response each keep a 10-second bound, and
  the total always applies. A slow upload with continuous progress may exceed 10
  seconds; a stalled one fails early.
- The CLI sends the exact `Content-Length` of the saved report; Engine checks it
  while bounding its actual reads. Heartbeats use their own requests and
  deadlines, never a queue behind large reports.
- A timeout ends only that delivery attempt; the CLI retries the same report
  without rerunning the tool. The connection's end cancels the upload at once.

### Replay-ledger exhaustion

- The per-connection replay ledger ([resource limits](#resource-limits)) holds
  instruction identities, content fingerprints, execution ownership, report
  identities and storage-path metadata; input and report bodies are stored apart
  from it. It is not a global output or concurrency quota.
- Existing identities are checked before admission: a duplicate of admitted work
  follows its existing task and report, also when ordinary capacity is
  exhausted.
- A new `tool.execute` that cannot be recorded safely has no side effect and
  gets a `rejected` report with `resource_exhausted`, correlated to that
  instruction. Engine persists and ACKs it and settles the call once, as not
  executed, keeping the resource-exhaustion reason. A new control that cannot be
  admitted gets its own control error; the target execution is unaffected.
- Existing report delivery, completion, cancellation and binding closure use the
  ledger's reserve; heartbeats use no ledger entry. No call is left hanging,
  rejection bookkeeping stays bounded, and a rejection never authorizes an
  automatic new attempt.

## MCP proxy

Messages: `tool-execution-v1.schema.json#/$defs/McpCallPayload` (in
`tool-execution-v1.schema.json#/$defs/Downstream`); the reports
`tool-execution-v1.schema.json#/$defs/ExecutionReport` and
`tool-execution-v1.schema.json#/$defs/ControlReport` with
`tool-execution-v1.schema.json#/$defs/McpResult` and
`tool-execution-v1.schema.json#/$defs/McpToolError`; the capability
`tool-execution-v1.schema.json#/$defs/LocalMcpCapability`; the tool-list report
`connection-v1.schema.json#/$defs/McpTools`, with
`connection-v1.schema.json#/$defs/McpTool`,
`connection-v1.schema.json#/$defs/McpServerInfo` and
`connection-v1.schema.json#/$defs/McpListError`. Fixtures:
`tool-execution-v1.fixtures.json` for the call, its reports and the capability;
`connection-v1.fixtures.json` for the tool-list report.

A machine exposes its local MCP server, Computer Use (`cua`), to Engine through
the CLI. The App Server embeds an MCP client that talks to each local server
over stdio. Engine learns a server's tools from the tool lists the CLI reports
and calls them with `mcp.call` instructions, which the CLI answers with tool
reports. The proxy needs no inbound port on the machine, and neither JSON-RPC
nor raw stdio bytes cross the connection to Engine. Engine sends no `initialize`
and no `tools/list` for a local server; its own MCP client serves only remote
servers.

### MCP routing

| Execution path | Target parameter the model gives |
|---|---|
| `exec_command`, `edit`, `view_image` through the CLI | `computer_id`, required even when the Session has one computer; there is no default. `write_stdin` follows its command's machine |
| A local MCP tool through the CLI | `computer_id`, required even when one computer provides the tool |
| A remote MCP server that Engine connects to directly | None |

- Local MCP tools, like every MCP tool, are reachable only from the model's Code
  Mode scripts. Computer Use's tool definitions come from the tool lists the CLI
  reports. The model never manages MCP connections and never supplies an
  internal server ID.
- Engine checks the Session's association with, and authorization for, the
  target computer, then routes the call to that computer's current instance,
  connection and binding. Engine removes `computer_id` before the call reaches
  the server, so a server's native input schema is never modified. The CLI keeps
  each tool's provenance and its MCP connection mappings internally.
- The CLI manages separate MCP connections and driver session mappings for each
  binding; another binding never borrows them. The underlying server process may
  be shared. Closing one binding releases its connections, tasks and driver
  session while other bindings on the shared server continue.
- Each instance reports the tools it actually supports. Nothing assumes one
  machine's inventory for every platform.
- A local MCP call carries no `sandbox_policy`. The CLI starts local servers
  without a sandbox, and on macOS the system starts `CuaDriver.app`. A server
  keeps the permissions the user gave it; the control is Engine's per-call
  decision.
- Computer Use's tools include `get_desktop_state`, `get_window_state`, `click`
  and `type_text`. The input of `get_desktop_state` has the optional fields
  `session`, `screenshot_out_file` and `max_image_dimension`, rejects unknown
  fields, and contains neither `computer_id` nor a server ID. Its `session` is a
  driver lifecycle label, not a Tetral Session and not an authorization.
- Invocation, result acknowledgement and unknown outcomes after a disconnection
  follow [tool execution](#disconnection-and-task-lifecycle). A lost result is
  never replayed automatically, desktop operations are never replayed across
  connections, and a new tool list never authorizes replaying an operation
  already sent. MCP errors stay identifiable as MCP errors in the result.

```text
Script: await tools.cua.get_desktop_state({ computer_id: 'computer_2', max_image_dimension: 1280 })
  -> Engine checks computer_2 and selects the binding on its current instance
  -> that instance's CLI selects its connection to the cua server
  -> local MCP: get_desktop_state(max_image_dimension=1280)
```

### The local server

The only local server is `cua`, which the App Server starts as `<driver> mcp`,
where `<driver>` is the configured Computer Use driver program; no other local
server can be configured on a machine. The App Server reaches it over the
server's stdin and stdout. Its MCP client is rmcp `=3.5.1`.

- **Start.** On each MCP connection to a server, the client sends `initialize`
  with `protocolVersion: "2025-06-18"`, `clientInfo: {name: "tetral-cli",
  version: <CLI version>}` and `capabilities: {}`, then
  `notifications/initialized`. The server's start fails with `mcp_start_failed`
  when its process does not spawn, when no `initialize` result arrives within 30
  seconds of the spawn, or when rmcp rejects the protocol version of that
  result.
- **Listing.** After the start, and after each
  `notifications/tools/list_changed` that the server sends on any of its MCP
  connections, the App Server calls `tools/list` and follows `nextCursor` to the
  end, within 30 seconds in total. Notifications that arrive while a listing
  runs make exactly one more listing run after it. A listing fails with
  `mcp_list_failed` on a JSON-RPC error, at the 30-second deadline, on a cursor
  over 65,536 bytes, on a cursor that the listing already received, on more than
  100 pages, on more than 2,048 tools, on two tools with one name, or when its
  report would exceed 4,194,304 bytes.
- **Messages from the server.** `notifications/message` goes to the App Server's
  log. `notifications/progress` and the resource and prompt `list_changed`
  notifications are ignored. `ping` gets an empty result. `roots/list`,
  `sampling/createMessage` and `elicitation/create` get the JSON-RPC error
  `-32601`.
- **Exit.** A server whose process ends without the App Server stopping it has
  exited (`mcp_server_exited`). The App Server never restarts a server: it
  starts one only when the App Server starts and, for `cua`, when Computer Use
  is enabled.
- A failed start, a failed listing and an exit reach Engine only in the server's
  tool list, and fail only the calls that need that server. None of them is a
  developer-visible Session error (`session.error`).

### Tool lists

Engine stores local MCP tool lists; it never stores remote ones.

- The App Server reports a local server's tool list with `POST
  /v1/client/mcp-tools` (`connection-v1.schema.json#/$defs/McpTools`):
  - after each completed listing;
  - with `tools: []` and an `error` when the server's start or a listing fails,
    or the server exits;
  - with `tools: []` and `error: null` when Computer Use is disabled.
- On each new connection to Engine, the App Server reports every local server
  again with its latest list or error; a resumed stream reports nothing again.
- The body holds the common fields, `server`, `seq`, `server_info`, `tools` and
  `error`:
  - `server` is the server's fixed name, the same on every platform and release;
    Computer Use is `cua`.
  - `seq` is `1` in the first report of that server that the App Server sends on
    the connection, and 1 higher in each later one it sends there.
  - `server_info` is `{name, title, version}` from the `serverInfo` of the
    server's `initialize` result, with `title: null` when the server gives none.
    It is `null` in a report sent for a failure, an exit or a disable.
  - `tools` is the server's complete list, at most 2,048 tools. Each entry holds
    those of `name`, `title`, `description`, `inputSchema`, `outputSchema` and
    `annotations` that the server returned, with their values unchanged; the CLI
    drops every other field. Each `name` appears once; Engine answers a report
    that names one tool twice with `400 invalid_message` and changes nothing. It
    is empty in a report sent for a failure, an exit or a disable.
  - `error` is `null`, or `{code, message}` with a code below and the App
    Server's description of the failure as `message`.
- The App Server keeps at most one report per server in flight and, behind it,
  only the newest unsent report of that server; it drops an older unsent one.
- One report holds one server's list and is at most 4 MiB (4,194,304 bytes); the
  64 KiB default and the 10-second ordinary total do not apply ([large-report
  deadlines](#large-report-deadlines)). The App Server measures each report
  before sending it and never sends a larger one: that listing fails instead.
  Engine answers a larger body with `413 request_too_large` and changes nothing.
- Engine accepts a report on any hub pod, only from the instance's current
  connection: a report from another connection gets `409 stale_connection`.
  Engine keeps one row per instance and server, with the report's connection,
  `seq` and body. A report replaces the row when the row is absent or came from
  another connection, or when the report's `seq` is higher than the row's;
  otherwise it is acknowledged and changes nothing.
- The row stays while the instance is offline or asleep, and is deleted in the
  transaction that deregisters the instance.
- A row with `tools: []` offers no tool of that server on that computer: search
  finds none of them, and a call to one is not dispatched.
- No discovery runs before input delivery, and no input waits for a tool list.
- An Agent uses a local server only after declaring it in its `mcp_servers` as
  `{type: "local", name}`. A reported server that the Agent does not declare is
  neither searchable nor callable.

| Tool-list `error.code` (`connection-v1.schema.json#/$defs/McpListError`) | Meaning | Engine |
|---|---|---|
| `mcp_start_failed` | The server's process did not spawn, no `initialize` result arrived within 30 seconds of the spawn, or rmcp rejected its protocol version | Stores the empty list: the server's tools are not offered on that computer |
| `mcp_list_failed` | A listing failed as stated under listing | As above |
| `mcp_server_exited` | The server's process ended without the App Server stopping it | As above |

### Call instruction

`mcp.call` is a downstream instruction in the SSE envelope, with the payload
`tool-execution-v1.schema.json#/$defs/McpCallPayload`.

```json
{
  "protocol_version": 1,
  "message_id": "msg_example",
  "connection_id": "conn_example",
  "type": "mcp.call",
  "payload": {
    "session_id": "sess_example",
    "binding_id": "bind_example",
    "tool_use_event_id": "call_example",
    "task_id": "task_example",
    "server": "cua",
    "tool": "get_desktop_state",
    "arguments": {"max_image_dimension": 1280}
  }
}
```

- `server` is a reported server's name, `tool` a tool `name` from its list, and
  `arguments` the call's input object with `computer_id` removed.
- `arguments` is at most 64 KiB (65,536 bytes) of compact JSON, and the complete
  SSE event at most 128 KiB (131,072 bytes). The metadata rule of [message
  sizes](#message-sizes-and-encoding) applies, with `arguments` as the large
  content. Engine checks `arguments` before dispatch.
- The payload carries no `limits` and no `sandbox_policy`.
- The CLI declares local MCP in ready as `capabilities.local_mcp:
  {schema_version: 1}`, only together with a `tool_execution` capability whose
  `controls` include `cancel`. Engine sends `mcp.call` only on a connection
  whose ready declared it.
- A local MCP call is a task of [tool execution](#tool-execution): staging,
  deduplication, acknowledgement, `tool.cancel`, cancellation at turn end and
  disconnection follow tool execution. This section states what differs.

### MCP admission

The App Server checks each `mcp.call` in this order, before any side effect:

1. It stages the call behind the preparation ACK, and rejects it for a binding
   that is not `prepared` (`binding_unavailable`), as for `tool.execute`.
2. It deduplicates the call as for `tool.execute`.
3. `arguments` must be an object of at most 65,536 bytes; otherwise the call is
   `rejected` with `invalid_input`.
4. The server must be running, and the binding's MCP connection to it must open
   within 30 seconds; otherwise the call is `rejected` with
   `mcp_server_unavailable`.
5. A full replay ledger makes the call `rejected` with `resource_exhausted`.

It then writes exactly one `tools/call {name, arguments}`, with no `_meta`, on
the binding's MCP connection.

### MCP result

The App Server answers each admitted call with exactly one tool report
(`tool-execution-v1.schema.json#/$defs/ExecutionReport`): `kind: execution`,
`phase: terminal`, `report_seq: 1`. It never sends a `running`
report and never a `drive_sync`. Its result envelope has `tool: "mcp"`,
`truncated: false` and `metrics`, with `run_ms` the MCP call and `drive_wait_ms`
and `sync_ms` `0`.

- `status` is `success` whenever the server answered with a `CallToolResult`,
  `isError: true` included. `result` is then that `CallToolResult` without its
  top-level `_meta`, every other field unchanged
  (`tool-execution-v1.schema.json#/$defs/McpResult`). Nothing in it is
  truncated; the [media](#mcp-media) rules bound it.
- Otherwise `status` is `error`, `result` is `{}` and `error` is `{kind,
  message, detail?}` with one of these kinds
  (`tool-execution-v1.schema.json#/$defs/McpToolError`):

| `kind` | When | `detail` |
|---|---|---|
| `mcp_error` | The server answered with a JSON-RPC error | `{code, message, data?}`, copied from that error |
| `mcp_timeout` | No answer 300 seconds after `tools/call` was written | None |
| `mcp_server_exited` | The MCP connection ended while the call was in flight | None |
| `cancelled` | A cancellation ended the call | `{sent}`: `true` when `tools/call` was written |
| `mcp_result_too_large` | The result exceeds a [media](#mcp-media) bound | `{media_bytes, envelope_bytes, report_bytes}`, the three measured sizes |
| `mcp_invalid_result` | The answer is not a valid `CallToolResult`, or one of its media strings is not valid Base64 | None |

### Cancellation and connection events

| Trigger | App Server action | Reports |
|---|---|---|
| `tool.cancel` before `tools/call` is written | Writes nothing to the server | `cancel` reply with `cancelled: true`, `was_running: false`; terminal `cancelled` with `sent: false` |
| `tool.cancel` while the answer is awaited | Sends `notifications/cancelled {requestId, reason: "cancelled by Engine"}` and discards any later answer | `cancel` reply with `cancelled: true`, `was_running: true`; terminal `cancelled` with `sent: true` |
| `tool.cancel` after the answer arrived | Nothing | `cancel` reply with `cancelled: false`, `was_running: false`; the terminal report of the answer stands |
| 300 seconds after `tools/call` was written, with no answer | Sends `notifications/cancelled {requestId, reason: "request timeout"}` and discards any later answer | Terminal `mcp_timeout` |
| The MCP connection ends while the call is in flight | Nothing | Terminal `mcp_server_exited` |
| The binding closes, Computer Use is disabled, or the App Server shuts down | Cancels each in-flight call of that binding, of `cua`, or of every server, as `tool.cancel` does, and closes the MCP connections those calls used | Terminal `cancelled`; after a disable, a later `cua` call is `rejected` with `mcp_server_unavailable` |
| The stream is lost and resumed | Nothing; the call continues | As usual |
| The connection ends | Cancels each in-flight call as `tool.cancel` does | None on another connection; Engine settles the call as an unknown outcome and dispatches nothing again |

- The `cancel` reply is a control report
  (`tool-execution-v1.schema.json#/$defs/ControlReport`) whose result envelope
  (`tool-execution-v1.schema.json#/$defs/HelperEnvelope`) has `tool: "cancel"`,
  `status: "success"`, `truncated: false` and `error: null`, and whose inner
  result (`tool-execution-v1.schema.json#/$defs/ExecResult`) holds the call's
  `task_id`, `cancelled` and `was_running` as the table states, `exit_code`
  and `signal` `null`, `timed_out` `false`, `duration_ms` 0 and no
  `written_bytes`, with `stdout` and `stderr`
  (`tool-execution-v1.schema.json#/$defs/StreamSnapshot`) each empty: `text`
  `""`, every count 0 and `truncated` `false`.
- A repeated `tool.cancel` joins the first.
- A server may ignore `notifications/cancelled`, so a cancelled call that was
  sent may still take effect.

### Settlement of local MCP calls

Engine settles each local MCP call once. The script is the model's Code Mode
script; `agent.mcp_tool_result` is the Session event that records the call's
result.

| Outcome | The script | `agent.mcp_tool_result` |
|---|---|---|
| `success` | Returns `{content, structuredContent, isError}` | `is_error` is `isError`, `false` when absent |
| Not dispatched because `arguments` exceeds 65,536 bytes | Throws an invalid-input error | `is_error: true` |
| Not dispatched because the connection's ready did not declare `local_mcp` or the computer's row lists no such tool; `rejected` with `mcp_server_unavailable` | Throws `tool not available on <computer_id>` | `is_error: true` |
| `rejected` with any other code; `cancelled` with `sent: false` | Throws the not-executed error `tool_dispatch_failed` | `is_error: true` |
| `mcp_error` | Throws with the JSON-RPC `code` and `message` | `is_error: true` |
| `mcp_timeout`; `mcp_server_exited`; `cancelled` with `sent: true` | Throws `may have run; check effects before retrying` | `is_error: true` |
| `mcp_result_too_large`; `mcp_invalid_result` | Throws `ran; result could not be returned: <message>`, with the error's `message` | `is_error: true` |

### MCP media

- **Image acceptance.** Before measuring, the App Server replaces each image
  that not every supported model accepts ([image acceptance](#image-acceptance))
  by one block `{type: "text", text: "Image omitted: it is <N> bytes and
  <width>x<height> pixels, larger than the supported models accept (at most
  7500000 bytes, 8000 pixels per side and 30000 patches of 32x32 pixels)."}` in
  its place, `<N>` being its decoded size. An image here is an `image` block or
  a `resource` blob whose `mimeType` is `image/png`, `image/jpeg`, `image/gif`
  or `image/webp` and whose header shows that format with its width and height.
  Every other block of the result is kept. Computer Use screenshots follow the
  same rule.
- **Bounds.** Then the App Server checks the `CallToolResult` against the media
  bounds of [message sizes](#message-sizes-and-encoding):
  - every `data` of an `image` or `audio` block and every `blob` of a `resource`
    block together decode to at most 10,485,760 bytes;
  - the result envelope with those strings emptied is at most 262,144 bytes;
  - the complete report is at most 14,680,064 bytes, under the [large-report
    deadlines](#large-report-deadlines).
- Over any bound, the App Server discards the result and reports
  `mcp_result_too_large`; nothing is truncated. It decodes those strings as
  standard Base64 with padding and no line breaks; a string that does not decode
  makes the result `mcp_invalid_result`.
- **Engine** processes the result in the transaction that stores the report:
  - It decodes the media strings strictly.
  - It stores each image (an `image` block or a `resource` blob whose `mimeType`
    is `image/png`, `image/jpeg`, `image/gif` or `image/webp` and whose bytes
    are of that type) and puts an attachment reference to the stored image in
    place of its bytes.
  - It replaces each `audio` block, each other blob and each `image` block whose
    type or bytes do not match with one text block `{type: "text", text:
    "[Binary MCP content omitted: <mimeType>, <N> bytes]"}`, where `<N>` is the
    decoded size.
  - `text` blocks, `resource_link` blocks, text resources and
    `structuredContent` pass unchanged.
  - When storing fails, Engine answers `5xx` without an ACK, and the App Server
    sends the same report again with the same `message_id`.
- Image bytes never reach the script or the model; both get the attachment
  reference.

## Drive transfer

Messages: the definitions of `drive-transfer-v1.schema.json` named below; the
endpoint table `drive-transfer-v1.schema.json#/$defs/DriveTransferEndpoints` and
the limits `drive-transfer-v1.schema.json#/$defs/DriveTransferLimits`. Fixtures:
`drive-transfer-v1.fixtures.json`. A bare definition name in this part, such as
`CommitRequest`, is a definition of `drive-transfer-v1.schema.json`.

These endpoints carry drive content between the CLI and `computer-hub`: file
listing, change catch-up, content upload and download, and commits. Engine
signals new changes with `drive.changed` on the stream. When the machine reads,
writes and commits is in [Session drives on a
machine](#session-drives-on-a-machine).

```text
CLI (one drive sync per Session and machine)          computer-hub (any hub pod)
 first mount   ─ GET files (pages) ──────────────────►  current files + start_seq
               ─ GET changes after start_seq ────────►  change records (seq, index)
 command runs  ─ PUT content/{sha256} (new only) ────►  hash while streaming, keep if equal
 sync point    ─ POST commits {changes} ─────────────►  one transaction: decide each file, one seq, usage, notify
 mount read    ─ GET content/{sha256} [+Range] ──────►  stream the content
 stream        ◄─ drive.changed {drive_id, seq} ─────  hub pod holding the stream, after every commit
```

### Common rules

- `computer-hub` serves every endpoint here. Each request authenticates with the
  instance's connection key, as every `/v1/client/*` request does. Each request
  carries `protocol_version=1`, `connection_id` and `binding_id`: in the query
  for GET and PUT (`drive-transfer-v1.schema.json#/$defs/CommonQuery`), in the
  body for POST. These requests carry no `message_id`.
- **Check order.** The hub checks each request in this order, and the first
  check that fails answers it:
  1. **Shape.** Query and path parameters, headers and the JSON body follow the
     schema: a missing `connection_id` or `binding_id`, a malformed hash, a
     wrong `Content-Type`, a malformed `Content-Length`, a malformed `Range` and
     a path that appears twice in a commit's `changes` included, apart from the
     size limits of step 5. An integer in a query or
     header value (`drive-transfer-v1.schema.json#/$defs/Decimal`) is a
     canonical decimal no greater than 9223372036854775807. Otherwise: `400
     invalid_message`.
  2. **Connection.** `connection_id` must be the authenticated instance's live
     connection. Otherwise: `409 stale_connection`.
  3. **Binding.** `binding_id` must name a binding of that connection whose
     `prepared` report Engine has stored and whose `closed` report it has not
     stored (a prepared or closing binding). Otherwise: `409 stale_binding`.
  4. **Drive access.** `drive_id` must be in that binding's drive set ([drive
     binding changes](#drive-binding-changes)) with the needed access: any
     access for GET, `read_write` for PUT and commit. Otherwise: `403
     drive_access_denied`.
  5. **Size and budget.** A commit's number of changes; a content upload's
     `Content-Length`, then the workspace's upload budget. A request that fails
     an earlier check draws nothing from the budget.
- A commit body over 16 MiB is answered `413 request_too_large` when the hub's
  read passes 16 MiB, before step 2, because the `connection_id` and
  `binding_id` that steps 2 to 4 check are inside the body.
- Every version a commit creates has the writer `session_actor {session_id,
  computer_id}` (`tool-execution-v1.schema.json#/$defs/DriveWriter`); the hub
  derives both IDs from the binding. The commit body and each of its changes are
  closed: a commit that carries a writer, or any other field not defined here,
  is rejected with `400 invalid_message`.
- Hashes are SHA-256 as 64 lowercase hexadecimal characters. Paths are
  drive-relative. Every path Engine returns follows the [drive path
  rules](#drive-paths). A commit checks each change's path per change: a path
  that breaks the rules rejects only that change, never the request.
- A list page's response body is at most 4 MiB (4,194,304 bytes).
- Errors are `{code, message}` (`drive-transfer-v1.schema.json#/$defs/Error`,
  the connection's error body) and never contain credentials; every code is in
  the [error table](#error-responses), and
  `drive-transfer-v1.schema.json#/$defs/DriveTransferEndpoints` lists each
  endpoint's status and code pairs. A `401`, a `403 instance_unavailable` and a
  `409 stale_connection` follow the connection's rules. A `403
  drive_access_denied` ends only that request.
- **Retries.** The CLI retries temporary failures: network errors, timeouts,
  `429`, `500`, `502`, `503` and `504`. After a `429` it sends the request again
  no sooner than its `Retry-After` seconds. After a `409 stale_connection` or
  `409 stale_binding` it retries the request with the binding of the next
  preparation. On a `400 content_hash_mismatch` it reads the file again and
  uploads its current content. Every other status is permanent, and so is a
  `2xx` answer that fails the schema or a relation stated here to its request,
  such as one commit result per change; the CLI logs such an answer.
  [Uploads](#uploads) and [reads](#reads) state what the agent then sees.
- **Deadlines** (`DriveTransferLimits`):
  - Connect and TLS: 10 seconds.
  - No byte progress in either direction: 10 seconds.
  - Response headers after the request body is sent: 10 seconds.
  - Total for PUT and commit: 10 seconds × max(1, ceil(request bytes / 65,536)).
  - The GET requests (list files, list changes and download content) have no
    total deadline.

| Endpoint | Request | Success |
|---|---|---|
| `GET /v1/client/drives/{drive_id}/files` | Path `drive-transfer-v1.schema.json#/$defs/DrivePathParams`; query `drive-transfer-v1.schema.json#/$defs/ListFilesQuery` | `200` `drive-transfer-v1.schema.json#/$defs/ListFilesResponse` with `drive-transfer-v1.schema.json#/$defs/FileEntry` |
| `GET /v1/client/drives/{drive_id}/changes` | Path `DrivePathParams`; query `drive-transfer-v1.schema.json#/$defs/ListChangesQuery` | `200` `drive-transfer-v1.schema.json#/$defs/ListChangesResponse` with `drive-transfer-v1.schema.json#/$defs/ChangeRecord` |
| `PUT /v1/client/drives/{drive_id}/content/{sha256}` | Path `drive-transfer-v1.schema.json#/$defs/ContentPathParams`; query `CommonQuery`; headers `drive-transfer-v1.schema.json#/$defs/UploadContentRequestHeaders`; raw body | `200` `drive-transfer-v1.schema.json#/$defs/UploadContentResponse` |
| `POST /v1/client/drives/{drive_id}/commits` | Path `DrivePathParams`; body `drive-transfer-v1.schema.json#/$defs/CommitRequest` with `drive-transfer-v1.schema.json#/$defs/CommitChange`, `drive-transfer-v1.schema.json#/$defs/WriteChange` and `drive-transfer-v1.schema.json#/$defs/DeleteChange` | `200` `drive-transfer-v1.schema.json#/$defs/CommitResponse` with `drive-transfer-v1.schema.json#/$defs/CommitResult`, `drive-transfer-v1.schema.json#/$defs/AcceptedResult`, `drive-transfer-v1.schema.json#/$defs/RejectedResult` and `drive-transfer-v1.schema.json#/$defs/CurrentVersion` |
| `GET /v1/client/drives/{drive_id}/content/{sha256}` | Path `ContentPathParams`; query `CommonQuery`; headers `drive-transfer-v1.schema.json#/$defs/DownloadContentRequestHeaders` | `200` with the full raw content, or `206` with `drive-transfer-v1.schema.json#/$defs/PartialContentResponseHeaders` |

A `429` comes with
`drive-transfer-v1.schema.json#/$defs/RateLimitedResponseHeaders`.

### List files

`GET /v1/client/drives/{drive_id}/files?…[&after_path=]`

- Returns `200` with `drive-transfer-v1.schema.json#/$defs/ListFilesResponse`
  for the query `drive-transfer-v1.schema.json#/$defs/ListFilesQuery`: a page
  of `FileEntry` items, `has_more`, and `start_seq` as below.
- `has_more: true` comes only with at least one item; a page with no item and
  `has_more: true` is refused.
- Lists current files in UTF-8 byte order of `path`, after `after_path`
  (exclusive; the last path of the previous page), at most 1,000 per page.
- `start_seq` appears on the page requested without `after_path`, and only
  there. It is the drive's latest seq, read before that page, or 0 when the
  drive has no commit.
- Pages are not a snapshot. The catch-up from `start_seq` that follows
  ([catch-up rules](#catch-up-rules)) makes the result equal to the drive.

### List changes

`GET /v1/client/drives/{drive_id}/changes?…&after_seq=N[&after_index=I]`

- Returns `200` with `drive-transfer-v1.schema.json#/$defs/ListChangesResponse`
  for the query `drive-transfer-v1.schema.json#/$defs/ListChangesQuery`: a page
  of `ChangeRecord` items and `has_more`.
- `has_more: true` comes only with at least one record; a page with no record
  and `has_more: true` is refused.
- Records come in `(seq, index)` order, strictly after the cursor. Without
  `after_index`, the page starts after the whole commit N.
- At most 1,000 records per page. A page may end inside a commit.
- `index` numbers one commit's records from 0, with deletions first.
- `operation` is `created`, `modified` or `deleted`. A deletion has
  `content_sha256: null`, `size_bytes: 0` and `executable: false`.
- Applying a record sets its path to that record's state.
- Seq numbers per drive start at 1, and `after_seq=0` means from the beginning.
- A change record is listed for 30 days after it is written; a record whose
  version was removed is still listed, and its content returns `404
  content_not_found`. A cursor below the drive's newest removed record, that is,
  one from which a removed record would be listed, returns `410
  drive_cursor_expired`; only a reader whose cursor is more than 30 days old
  gets it.
- The CLI downloads content only when a file is read, after catch-up has applied
  every record through its target seq, so it never needs the content of a record
  that a later record of the same path supersedes.

### Drive change notice

`drive.changed {drive_id, seq}` is a downstream message in the standard envelope
(`connection-v1.schema.json#/$defs/DriveChangedPayload`; as a whole message
`drive-transfer-v1.schema.json#/$defs/DriveChanged`), where `seq` is the
commit's seq.

- After every commit, from the developer API or from `computer-hub`, the hub pod
  that holds the stream sends it to every connection that meets both conditions:
  - its ready declared drive mounting (`capabilities.drives.mount: true`);
  - it has a prepared or closing binding whose drive set includes `drive_id`.
- The pod may merge several pending notices for one drive and stream into one
  notice with the highest seq.
- There is no ACK, and the notice is never resent after a resume.
- The pod subscribes a stream to drive notices before it sends `connected`.
- On one stream, the pod writes `drive.changed` notices and instructions in the
  commit order of the transactions that caused them.
- The notice fits the 64 KiB (65,536 bytes) SSE default.

### Upload content

`PUT /v1/client/drives/{drive_id}/content/{sha256}?…`

- The body is the raw bytes, with `Content-Type: application/octet-stream` and
  the headers `drive-transfer-v1.schema.json#/$defs/UploadContentRequestHeaders`;
  the answer is `drive-transfer-v1.schema.json#/$defs/UploadContentResponse`.
- `Content-Length` is required. In step 5, a missing one gets `400
  invalid_message`, and one above 500,000,000 gets `413 file_too_large`, before
  any byte is read.
- The hub then applies the workspace's [upload
  budget](#drive-limits-and-the-upload-budget), before any byte is read; a
  refusal is `429 rate_limited` with `Retry-After`.
- The hub streams the body, counting bytes and computing SHA-256.
  - If the hash is not `{sha256}`: nothing is kept, and the hub returns `400
    content_hash_mismatch`.
  - Otherwise the drive holds the content, and the hub returns `200
    {content_sha256, size_bytes}`, with `content_sha256` equal to the URL hash
    and `size_bytes` the bytes received. The same `200` comes back when the
    drive already held the content.
- Content that no version names stays committable until Engine's daily cleanup
  deletes it: the cleanup deletes stored content that no kept version names and
  that was not uploaded in the last 24 hours.
- The CLI sends no PUT for a hash its file table shows the drive holds. A rename
  therefore uploads no content. Content held only by another drive is always
  uploaded.
- The CLI turns a `413 file_too_large` into a `limit` rejection of that file,
  with the error's message as `rule`.

### Commit

`POST /v1/client/drives/{drive_id}/commits`

- The body is `drive-transfer-v1.schema.json#/$defs/CommitRequest`, answered
  by `drive-transfer-v1.schema.json#/$defs/CommitResponse`; each change is a
  `WriteChange` (`change:
  "write"`, whose `base_sha256` is a hash or `null`) or a `DeleteChange`
  (`change: "delete"`, whose `base_sha256` is a hash). A `null` base means the
  file must not exist.
- **Limits.** 1 to 10,000 changes and at most 16 MiB (16,777,216 bytes) of
  compact JSON. Empty `changes`: `400 invalid_message`. More than 10,000 changes
  or more than 16 MiB: `413 request_too_large`. A path that appears twice: `400
  invalid_message`.
- **Splitting.** When the changes the CLI commits at one time exceed either
  limit, it sends consecutive commits. It orders the changes deletions first,
  then writes, each group in UTF-8 byte order of `path`; it fills each commit
  with the next changes while the commit stays within both limits; and it sends
  each commit after Engine answered the previous one. Each commit is atomic with
  its own seq.
- **Decision.** Engine runs the commit as one database transaction, serialized
  per drive with every developer API commit:
  - a. **Content check.** If any write names content this drive does not hold,
    nothing is applied. The hub returns `409 content_missing`, whose error body
    adds `missing_sha256: [...]`. The CLI uploads that content again and, when
    any change remains, sends the commit again without the writes that name
    those hashes; a sync point's commit is sent again only after its result is
    reported ([uploads](#uploads)). Those writes are committed once their
    content is uploaded, as a change still uploading is.
  - b. **Order.** Deletions are decided first, then writes, each group in
    request order. Each change is decided on its own, by the first rule that
    matches:
    1. The current state already equals the change: same `content_sha256` and
       `executable`, or, for a delete, no current file. Outcome: accepted, no
       new version.
    2. A [path rule](#drive-paths) fails. Outcome: rejected `path_rule`, with
       Engine's message in `rule`.
    3. The base differs from the current state: a `null` base and an existing
       file, or a hash base and a different or absent file. Outcome: rejected
       `conflict`.
    4. The change raises bytes or files beyond a limit: the drive's own limits,
       or the workspace's 500,000,000,000 bytes or 100,000 files, with usage
       counted after the commit's earlier accepted changes. A deletion, or a
       write that raises neither count, never matches, also while usage is above
       a lowered limit. Outcome: rejected `limit`, with the limit named in
       `rule`.
    5. Otherwise: accepted, with a new version.
  - c. **Visibility.** All new versions share one new seq and become visible
    together. Usage is updated in the same transaction, which also notifies the
    hub pods.
- **Response.** `200` with `drive-transfer-v1.schema.json#/$defs/CommitResponse`:
  `seq` and `results` (`drive-transfer-v1.schema.json#/$defs/CommitResult`), in
  request order, one per change:
  - Accepted (`drive-transfer-v1.schema.json#/$defs/AcceptedResult`): `version_id` is the new version, or, when the
    path already held the change (rule b.1), its current version, a deletion
    included. `version_id` is null only for a delete of a path of which the
    drive keeps no version.
  - Rejected (`drive-transfer-v1.schema.json#/$defs/RejectedResult`): `reason`,
    `rule` for `path_rule` and `limit`, and `current`
    (`drive-transfer-v1.schema.json#/$defs/CurrentVersion`), the path's current
    version with its
    writer; for a deletion it holds `content_sha256: null`, `size_bytes: 0` and
    `executable: false`, as a change record does. It is absent when the drive
    keeps no version of the path.
  - `seq` is null when the commit created no version.
- The response is at most 16 MiB (16,777,216 bytes). Each `rule` is at most 512
  UTF-8 bytes.
- The CLI reports each rejection as a `drive_sync` entry ([drive sync
  outcome](#drive-sync-outcome)) with `reason` and `rule` as returned; for a
  `conflict` it sets `current_version_id` to `current.version_id` and
  `current_writer` to `current.writer`, and leaves both out when `current` is
  absent.
- **Resending.** A commit sent again after Engine applied it gets each change it
  applied accepted with no new version (rule b.1), unless another writer changed
  that path in between. Commits therefore carry no `message_id` and keep no
  replay record.

### Download content

`GET /v1/client/drives/{drive_id}/content/{sha256}?…`

- Takes the headers
  `drive-transfer-v1.schema.json#/$defs/DownloadContentRequestHeaders`; a `206`
  carries `drive-transfer-v1.schema.json#/$defs/PartialContentResponseHeaders`.
- Takes an optional `Range` with one range: `bytes=a-b`, `bytes=a-`, or
  `bytes=-n` for the last n bytes, from the start when n exceeds the size.
  Without `Range`, it returns `200` with the full content. Ranges follow HTTP
  (RFC 9110):
  - A range that starts inside the content returns `206` with `Content-Range:
    bytes a-b/size`, cut at the end when it ends past it.
  - A range that selects no byte (one that starts at or past the end,
    `bytes=-0`, or any range on an empty file), and a `Range` with more than one
    range, return `416 range_not_satisfiable`.
  - Any other `Range` value, a range whose end is below its start included, is
    malformed: `400 invalid_message`.
- `404 content_not_found` when no kept, unredacted version of this drive names
  this hash. A hash that only another drive holds also returns `404`, so knowing
  a hash never yields another drive's content.
- The CLI checks the SHA-256 of the whole content before it puts the content
  into its content cache. On a mismatch it discards the content, and the read
  fails with an I/O error.

### Catch-up rules

The CLI follows these rules; the guarantee below depends on them.

- **First mount** of a drive that has no sync state for this Session: fetch
  every page of [list files](#list-files), then call [list
  changes](#list-changes) from `start_seq` until `has_more` is false. After a
  `410 drive_cursor_expired`, do the same, keeping the pending changes and the
  content cache.
- **When to call list changes from `applied_seq`** (the seq through which the
  CLI has applied the drive's records):
  - after every `connected`, resumed or not;
  - on every `drive.changed` whose seq is above `applied_seq`;
  - at every later mount.
- **`applied_seq`** moves past a commit only after all of that commit's records
  are applied. A record for a path that holds a change of this machine not yet
  committed updates only the CLI's file table, which records the drive's version
  of each path; neither the local file ([remote changes](#remote-changes)) nor
  the pending change's base ([uploads](#uploads)) changes.
- **Before a command.** Before an `exec_command` starts or a `view_image` reads,
  catch-up for each mounted drive must be complete in two ways:
  - through the highest seq received in `drive.changed` for that drive;
  - through the catch-up that follows the latest `connected`.
- **Guarantee.** A commit that committed before a command's `tool.execute` was
  dispatched is visible to that command, except at a path where the machine
  holds a change not yet committed.
- **Failed listing.** A list files or list changes request that fails
  permanently, such as a `403 drive_access_denied` or a `2xx` answer that fails
  the schema, leaves that drive unmounted on that machine. It is a new [mount
  failure](#mount-failures) with that error, and the CLI tries again at the next
  check. A `410 drive_cursor_expired` is not such a failure: the first-mount
  rule handles it.
- **Excluded paths.** The hub returns the whole drive, and the CLI drops the
  paths in its computer's `drive_exclude`.

## Session drives on a machine

Messages: the drive fields of
`connection-v1.schema.json#/$defs/SessionPreparePayload` and
`connection-v1.schema.json#/$defs/SessionDrivesPayload`, the `applied` and
`closed` forms of `connection-v1.schema.json#/$defs/SessionStatus` with
`connection-v1.schema.json#/$defs/LostChange`,
`tool-execution-v1.schema.json#/$defs/DriveSyncOutcome`,
`tool-execution-v1.schema.json#/$defs/DriveSyncReport`, and the drive transfer
endpoints. Fixtures: `connection-v1.fixtures.json`,
`tool-execution-v1.fixtures.json` and `drive-transfer-v1.fixtures.json`.

The files a Session shares across its machines, except Git repositories, live in
cloud drives that its machines mount. The agent reads and writes them as
ordinary files on whichever machine it uses; the CLI serves each drive on the
machine and keeps it consistent with Engine; Engine decides every conflict and
the CLI follows its answers. A drive's Memory and Skills are ordinary files in
it.

### What a drive holds

- A drive holds files at paths relative to its root. A file is identified by its
  drive and path; there is no file ID, and Engine never stores machine paths.
- Every change to a file, its deletion included, is kept as a version with a
  SHA-256 content hash (none for a deletion), the executable bit, a time and its
  writer, addressed by `version_id`. A version is a whole file: any change
  uploads the whole file again; partial uploads do not exist.
- Changes become visible in commits. All file changes of one commit become
  visible at the same moment and share one sequence number (seq). Every
  committed change appends a change record. The change list is the source of
  truth; a stream only signals that more records exist.
- A drive stores regular files only. A folder exists while it holds a file:
  empty folders are not synced, and deleting a folder's last file removes the
  folder on the other machines. Symbolic links and special files are never
  synced.
- Of a file's permissions only the executable bit is kept. Other permission
  bits, owners and extended attributes are not kept.
- `node_modules/`, `.venv/` and `__pycache__/` are never synced; this list is
  fixed. A directory that contains `.git` is a Git repository and is never
  synced; the result of the command that created it says so (`not_synced` with
  `git_repository`).
- A drive keeps a path's old versions for at most 30 days and at most 10 per
  file; a deletion that is still its path's current version is kept for 30 days,
  after which the drive keeps no version of the path. A change record is kept
  for 30 days after it is written.

### Drive paths

Every write, through the developer API or from a machine, checks its path:

1. Relative to the drive root, with no leading `/`; at most 1,024 bytes; valid
   UTF-8 in NFC and in the Stream-Safe Text Format of UAX #15 (no run of more
   than 30 non-starters in its NFKD form); no control or format characters; no
   empty, `.` or `..` segments.
2. At most 255 bytes per segment.
3. No two paths in one drive that are equal under Unicode full case folding,
   such as `report.md` and `Report.md`, or `Straße.md` and `STRASSE.md`.
4. No file above or below another file: while `a/b` is a file, `a/b/c` cannot be
   created, and while `a/b/c` exists, `a/b` cannot be created as a file.

On a machine, creating or renaming a file or folder fails with `EINVAL` when its
name is not valid UTF-8 or its path breaks rule 1 or 2, NFC aside (the
stream-safe limit still applies): the CLI converts names to NFC before upload. A path that breaks rule 3 or 4 is a
`path_rule` rejection. Within one commit, deletions apply before creations, so a
rename that changes only case succeeds.
`connection-v1.schema.json#/$defs/DrivePath`,
`connection-v1.schema.json#/$defs/ChangePath` and
`drive-transfer-v1.schema.json#/$defs/DrivePath` hold the part of these rules a
pattern can check.

### Drives on a machine

- A machine can mount drives on Linux with FUSE, in a container that has
  `/dev/fuse`, and on a Mac whose drives are enabled. The CLI reports this in
  ready as `capabilities.drives.mount`.
- A machine that cannot mount has no drives. Engine prepares the Session there
  without drives, and at that preparation the CLI lists every bound drive as a
  mount failure with the reason (drives not enabled, no FUSE), so the developer
  gets one `drive_mount_error` per drive. Once mounting works, the CLI connects
  again and mounts the drives.
- Every machine of the Session that can mount mounts every bound drive, without
  the paths its `drive_exclude` leaves out. Every such machine sees the same
  committed content. A running Session sees every committed change before its
  next command ([catch-up rules](#catch-up-rules)), except at a path where its
  machine holds a change not yet committed ([remote changes](#remote-changes));
  changes still uploading are listed in the result that reported them
  ([uploads](#uploads)).
- Each Session has its own mount of each drive, so Sessions that bind the same
  drive can differ in access and excluded paths, and each change is attributed
  to the Session whose mount received it. Content is stored once per machine.
- Drives are never downloaded in full: a listing appears at once and content
  downloads on first read.
- While free space on the state root's volume is below 5 GiB, the CLI evicts the
  least recently read drive content that is already committed; an evicted file
  downloads again on its next read. Changes not yet committed are never evicted.
- **`TETRAL_DRIVES`.** The CLI sets `TETRAL_DRIVES` in the environment of every
  command of the Session to `$TETRAL_HOME/sessions/<session_id>/drives` whenever
  the Session binds drives and the machine can mount, even when one drive failed
  to mount. Each mounted drive is the folder `<drive_id>` there. Both folder
  names are the literal IDs. `TETRAL_DRIVES` is unset on a machine that cannot
  mount.
- A drive's Memory folder is `$TETRAL_DRIVES/<drive_id>/<memory path>` and its
  Skills folder `$TETRAL_DRIVES/<drive_id>/<skills_path>`. The model never sees
  an absolute drive path; it gets absolute paths from its own commands, for
  example `find "$TETRAL_DRIVES"/drive_team_ai/skills -name SKILL.md`, and uses
  them in `edit` and `view_image`.
- The CLI checks each bound drive's mount when it prepares the Session on the
  machine and at the check before each `exec_command` starts or `view_image`
  reads. A drive found unmounted, for example unmounted outside the CLI, is
  mounted again; files written while it was unmounted are not recovered.

### Mount failures

- A mount that fails (the mount point is not a directory, permission denied,
  disk full, the system refuses the mount, or a list request fails permanently
  as the [catch-up rules](#catch-up-rules) state) leaves that drive unavailable
  on that machine. The Session's other drives and its commands go on, and the
  CLI tries again at every check. The agent finds `$TETRAL_DRIVES/<drive_id>`
  missing on that machine.
- The CLI keeps each bound drive's mount state on the machine and reports a
  failure only when it is new: at the previous check the drive was mounted, or
  failed with a different error. It keeps each drive's last reported mount error
  in the Session's sync-state database, which survives App Server restarts, so a
  lasting failure is reported once per Session per machine; a replaced machine
  reports it again.
- **Where it is reported**
  (`tool-execution-v1.schema.json#/$defs/DriveMountFailure`): at preparation in
  the preparation report's `drive_mount_failures`; at the check before an
  `exec_command` or `view_image` in `drive_sync.unmounted` of that call's first
  result; at a `session.drives` in that instruction's report.
- For each reported failure Engine records one `drive_mount_error` for the
  developer, with `drive_id` and `computer_id`, in the transaction that stores
  the report. Engine keeps no mount state, so a replayed report records nothing
  twice.

### Excluded paths

A computer may leave out paths of a bound drive on its machine with
`drive_exclude: [{drive_id, paths}]`, fixed when the computer is added. It
cannot leave out a whole drive. An entry that names a drive the Session does not
bind has no effect until that drive is bound. Each path follows the drive path
rules, may end with `/`, and leaves out that file or folder and everything below
it.

On that machine an excluded path does not exist in the drive. Creating or
writing a file at it or below it, renaming a file or folder to it or below it,
and making a folder at it or below it fail with the permission error `EACCES`.
Nothing is kept on the machine or uploaded.

### Reads

- A read through a mount returns the drive's current version that the CLI has
  applied.
- A read waits until its content has downloaded, however long that takes; no
  time limit fails it. The CLI retries temporary download failures ([common
  rules](#common-rules)) for as long as they last. A permanent failure, such as
  Engine denying access, fails the read at once with an I/O error. If the CLI
  stops answering, a command waiting on a read keeps waiting until it is
  cancelled.
- While a command still runs, its `running` result lists the Session's downloads
  in progress (`drive_sync.downloading`).

### Uploads

- **Content upload.** While any command of the Session runs, foreground or
  background, the CLI uploads a changed drive file's content once the file has
  not been written for 2 seconds, or at once when the command that wrote it
  exits. A file written with longer pauses may first upload half-written; its
  complete content follows. When a file changes again while its earlier content
  is still uploading, the CLI abandons that upload and uploads the latest
  content. Uploaded content is visible to no one until it is committed.
- **Sync point.** Each `exec_command`, `write_stdin` or `edit` result has a sync
  point, which the CLI runs before it reports the result. The sync point covers
  every change of the Session not yet committed when it starts, background
  writes included, except:
  - a change that an earlier result listed as `uploading`, whose file has not
    changed since that result and whose upload or commit has not ended in a
    fault since; it is neither waited for nor listed again;
  - a change made by another call that has not reported yet; it waits for that
    call's sync point.

  The CLI starts the upload of every changed drive file whose upload has not
  started. A result whose command has exited, and an `edit` result, then wait at
  most 30 seconds in total for the content of every covered change, whether
  uploads are slow or failing. A `running` result, which reports a command still
  running (every `write_stdin` reply to a running command included), does not
  wait. The CLI then commits every covered change whose content is uploaded, and
  every covered deletion, which needs no content, and reports the result after
  Engine answers those commits. If a commit of the same drive mount is in flight
  when the wait ends, the CLI does not wait for its answer: it lists that
  drive's covered changes in `not_synced` as `uploading`, and commits them after
  that answer.
- **Still uploading.** A result also lists in `not_synced` as `uploading` each
  covered change whose content is not uploaded when the wait ends, and every
  change of its sync point's own commit when that commit's answer did not arrive
  or was `409 content_missing`; the CLI does not send that commit again before
  reporting. Such a change keeps uploading and is committed once it is due. A
  result lists only the changes its sync point covers. The rejection of a change
  that a result listed as `uploading`, and of any change that no sync point
  covers, goes upstream in a [drive sync report](#drive-sync-report), also when
  a sync point's commit carried it.
- **Background writes.** A change that a command makes after its first report is
  a background write.
- **The next command** does not wait for this machine's own uploads. Before it
  starts, the CLI applies every change that other writers committed.
- **Due changes.** A background write, a change an earlier result listed as
  `uploading` or `upload_pending`, and every change pending at an App Server
  restart or a new connection is due once its content is uploaded (a deletion at
  once); any other change becomes due at the sync point of the call that made
  it. A change whose last upload or commit ended in a fault is not due before
  the CLI tries it again. On a Mac, where the mount does not learn the writing
  process, a background write is due at the next sync point instead.
- **Commits.** The CLI keeps at most one commit in flight for each drive mount,
  and builds the next commit after the previous one's answer from every change
  then due; commits of different drive mounts are sent in parallel. Each file is
  decided on its own: accepted changes become new versions even when another
  change of the same commit is rejected. The changes one sync point commits in
  one drive form one commit, or consecutive commits when they exceed a commit's
  limits ([commit](#commit)).
- **Base.** Every change carries as its base the hash recorded with the pending
  change: the version the machine had applied or had accepted when the file was
  first changed after its last commit or rejection, or "must not exist" when the
  path then had no file. A file first changed while a commit that carries its
  path is in flight takes its base from that commit's answer instead: when the
  commit is accepted, the content it committed, or "must not exist" after a
  deletion; when it is rejected, the drive's version replaces the file, which
  discards the later change. Applying a remote record at that path while the
  change is pending does not change its base.
- **Lost answers.** A commit whose answer did not arrive (because of a temporary
  failure, a `409 stale_connection` or `409 stale_binding`, the end of its
  connection before the answer, or an App Server crash before the CLI recorded
  the answer) is sent again unchanged, with the binding of the current
  preparation, before any later commit of that drive mount; a change made
  meanwhile at one of its paths waits for the next commit. Each change that the
  first sending applied is then accepted with no new version, unless another
  writer changed its path in between.
- **Faults.** A permanent error of an upload or a commit, other than a conflict,
  a path rule or a limit, is a fault: the change stays pending on the machine,
  and the CLI tries it again only at the sync point of the next `exec_command`,
  `write_stdin` or `edit` result, at an App Server restart, at a new connection,
  at a [final upload](#final-upload) and before the App Server's idle exit. Each
  sync point whose attempt fails this way lists the change in `not_synced` as
  `upload_pending`. The CLI and Engine each log the error code.
- **The drive wins.** When the drive's version has moved on, the change is
  rejected (`conflict`). The CLI replaces the local file with the current
  version and tells the agent which file was rejected, which version the drive
  now holds, who wrote it, and that its change was not saved; if the change
  still applies, the agent reads the file again and redoes it. A path that
  breaks the path rules (`path_rule`) and a file over a limit (`limit`) are
  rejected the same way: the file returns to the drive's state, a new file is
  removed, and the agent is told the rule or the limit. There are no conflict
  copies.
- After a restart or a new connection, the pending changes upload and commit
  without holding any command; rejections reach the agent through a drive sync
  report at its next turn.
- Changes not yet committed are lost when the machine is lost.
- A computer removal, a Session archive or deletion, and a `session.drives` that
  removes a drive or changes its access end with the CLI's [final
  upload](#final-upload).

### Deletes and renames

- A deletion is committed like a change: it carries the hash it was based on,
  and a mismatch rejects it. The CLI then restores the current version locally
  and tells the agent that the file was not deleted because it changed in the
  drive meanwhile.
- A rename is a deletion of the old path plus a creation at the new path with
  "must not exist". The content is not uploaded again. Each half is decided on
  its own; a rejected creation is handled like any rejected upload.
- Deletions are never throttled or delayed. The tool result states how many
  drive files the command deleted (`drive_sync.deleted`); the developer can
  restore them.

### Remote changes

- After each commit, through the developer API or from a machine, Engine sends a
  [drive change notice](#drive-change-notice) to the machines that mount the
  drive.
- The CLI applies remote changes as it catches up ([catch-up
  rules](#catch-up-rules)); before each command it has applied every change
  committed before that command was dispatched.
- A path that holds a change of this machine not yet committed keeps its local
  content when catch-up brings a remote change for it, until Engine decides the
  local change; a rejection then brings the drive's version and the agent is
  told. A remote change therefore never replaces a local change without the
  agent being told.
- At a path that is never synced on this machine, a remote change replaces the
  local file only when this machine has not written it since the version it
  applied.

### Content transfer

- Uploads, downloads, commits and change catch-up between the CLI and Engine
  pass through `computer-hub` with the CLI's connection credential, over the
  [drive transfer](#drive-transfer) endpoints. Machines never hold storage
  credentials or storage URLs.

### Drive limits and the upload budget

| Limit | Value | Field of `drive-transfer-v1.schema.json#/$defs/DriveTransferLimits` |
|---|---|---|
| One file | 500,000,000 bytes (500 MB) | `file_max_bytes` |
| One workspace, over all its drives | 500,000,000,000 bytes (500 GB) and 100,000 files | `workspace_max_bytes`, `workspace_max_files` |
| One drive | An optional lower cap the developer sets, `{max_bytes, max_files}` | |
| Drive path; one path segment | 1,024 UTF-8 bytes; 255 UTF-8 bytes | `path_max_utf8_bytes`, `path_segment_max_utf8_bytes` |
| List page | 1,000 entries and 4,194,304 bytes | `list_page_max_items`, `list_page_max_json_bytes` |
| Commit | 1 to 10,000 changes; request and response each at most 16,777,216 bytes | `commit_min_changes`, `commit_max_changes`, `commit_request_max_json_bytes`, `commit_response_max_json_bytes` |
| A rejection's `rule` | 512 UTF-8 bytes | `rule_max_utf8_bytes` |
| `drive.changed` event | 65,536 bytes | `drive_changed_sse_max_json_bytes` |
| Upload budget | 500,000,000 bytes per minute per workspace (default) | `upload_budget_default_bytes_per_minute` |
| An upload's budget count | At least 65,536 bytes, at most the per-minute amount | `upload_budget_min_count_bytes` |
| `Retry-After` of `429 rate_limited` | 1 to 60 seconds | `retry_after_min_seconds`, `retry_after_max_seconds` |
| Connect and TLS; no progress; response headers | 10,000 ms each | `connect_tls_timeout_ms`, `no_progress_timeout_ms`, `response_headers_timeout_ms` |
| PUT and commit total | 10,000 ms per started 65,536 request bytes, at least 10,000 ms | `put_and_commit_total_timeout_ms_per_65536_request_bytes` |

- Usage counts the size and number of a drive's current files; old versions do
  not count, and two files with the same content count twice. A write that
  raises bytes or files beyond a limit is a `limit` rejection. A deletion, or a
  write that raises neither count, is never rejected for a limit, also while
  usage is above a lowered limit.
- **Upload budget.** Each workspace has an upload budget, an Engine setting with
  default 500,000,000 bytes per minute, shared by every Engine service. It
  counts the content uploads to the workspace's drives from machines and from
  the developer API. A machine's upload counts its `Content-Length`
  (`drive-transfer-v1.schema.json#/$defs/UploadContentRequestHeaders`), at
  least 65,536 bytes and at most the per-minute amount. The budget holds at most the
  per-minute amount; it starts full and refills continuously at the per-minute
  amount every 60 seconds.
- Engine checks each upload before reading its body. When the budget holds at
  least the request's count, Engine takes the count and serves the request; the
  count is not given back when the request then fails. Otherwise Engine reads
  none of the body and answers `429 rate_limited` with `Retry-After`: the number
  of seconds until the budget holds the request's count, rounded up to a whole
  number and at least 1.
- A machine's refused upload is a temporary failure: the CLI sends it again
  after the delay, and a result whose wait ends first lists the change as still
  uploading. The refusal records no developer-visible Session error.

### Drive permissions on the machine

`sandbox_policy` never names drive mounts; the CLI adds them ([filesystem
rules](#permissions-sandbox_policy)):

- Each Session's processes get read access, or read and write access, to that
  Session's mount of each drive according to its drive binding; read-only for
  every mount when the call's policy has no `read_write` rule. A write to
  read-only content fails at the operating system, bash included.
- Session processes cannot read or write anything else under the state root: not
  the credentials, the content cache or the sync state, and not another
  Session's mounts. The CLI's sync component runs outside the Session's policy.
