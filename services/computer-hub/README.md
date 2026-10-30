# computer-hub

## Responsibilities

The service between Engine and the machines it uses. It serves every path
under `/v1/client/` (the routes a CLI calls with its connection key), and the
Controller and developer routes under `/v1/instance_requests` and
`/v1/instances`; `api` keeps every other `/v1` route. The public edge sends the
three prefixes here after `auth`'s Check.

The service owns no table. It reads and writes through `internal/instance`
(instances, their connection credentials, the idempotency records of the
issuing routes, connections, their replay ledger, protocol-error reports and
Session instructions), `internal/computer` (Controller requests, the computer
whose request a ready fulfils, preparation bindings and their reports, and
the Session errors a report writes) and `internal/gitticket` (computer git
tickets), and runs the steps of
`internal/hubflow` that span packages. It connects to PostgreSQL as the
serving role `computer_hub` and holds the blob store open through
`internal/blob` for drive content and images.

## Routes and credential classes

Connection keys and workspace credentials (API keys and access tokens) are
separate classes, decided first at the edge and again here:

| Path | Accepted principal | Any other principal |
|---|---|---|
| under `/v1/client/` | `connection_key`, authority `instance_connection`, scope `{workspace_id, type: "instance", id}`, operation `client.connect` | `401` with the client error body, code `credential_invalid` |
| `/v1/instance_requests`, `/v1/instances` and below | a workspace credential, through the shared operation gate (`httpapi.DeclarePublicOperation`) | a `connection_key` principal: `401` `authentication_error`, also on a path with no route |

Every request first verifies the signed internal principal
(`X-Tetral-Internal-Principal`) against the actual method and path; a missing
or invalid one is answered as a credential failure in the body of its path's
surface. A client route that does not exist answers `404 not_found` with the
client error body; a management path that does not exist answers `404
not_found_error`.

Error bodies:

- Client routes answer `{code, message}` with a code of the client error table;
  `message` is displayable and never holds a credential. The protocol defines
  no body for a `5xx`, which keeps the API envelope.
- Management routes answer the API error envelope through
  `httpapi.WriteError`.

## Instance routes

`POST /v1/instances`, `GET /v1/instances`, `GET /v1/instances/stream`,
`GET /v1/instances/{instance_id}`, `DELETE /v1/instances/{instance_id}`, and
`POST|GET /v1/instances/{instance_id}/credentials` and
`DELETE /v1/instances/{instance_id}/credentials/{credential_id}` run under the
operations `instances.*` and `instance_credentials.*`.

- Bodies are read by `httpapi.ReadJSONBody` (at most 1 MiB, else `413
  request_too_large`; U+0000 refused; a body that cannot be read `400
  invalid_request_error`) only after the instance and key checks below, and
  must be one JSON object without repeated or unknown keys.
- The issuing routes require `Idempotency-Key` once, 1 to 255 bytes after
  trimming, checked before any idempotency record is read. Creation checks the
  key, the record, then the body; issuance checks that the instance exists, the
  key, the record, a deregistered instance (`403 instance_unavailable_error`),
  `delivery`, `expires_at`, then the credential limit (`409
  credential_limit_error`). A replay with an equal canonical fingerprint
  answers the stored status with the current objects and `connection_key:
  null`; a different request with the key answers `409
  idempotency_conflict_error`. A record is written with the effect, and a
  transaction that loses the record's unique key to a concurrent identical
  request runs once more and replays.
- `expires_at` is absent (issuance plus `TETRAL_COMPUTER_HUB_MAX_CREDENTIAL_LIFETIME`)
  or an RFC 3339 time ending in `Z`, later than issuance and at most the
  maximum lifetime after it, stored to the microsecond; issuance time is the
  database clock of the transaction.
- `delivery: response` creates an `active` credential and answers `201` with
  its key. `delivery: stream`, also when `delivery` is absent, is a live
  renewal: when the instance's connection is `online` (not ended, a live
  lease, an accepted ready, held `FOR SHARE`), the transaction creates a
  `pending` credential without a digest that renews that connection and
  replaces its credential, notifies the stream's pod, which writes
  `credential.renew`, and answers `202` with `connection_key: null`; with no
  online connection, after the credential limit, `409
  instance_offline_error` and nothing written. A renewal whose connection
  ends stays `pending` until it expires or is revoked.
- Revocation and deregistration answer `204` and change nothing when
  repeated; deregistration revokes every credential not yet revoked with cause
  `deregistration` and keeps the instance as a tombstone. Revoking the live
  connection's credential, or deregistering its instance, ends that connection
  in the same transaction (`hubflow.EndConnection`, causes `revoked` and
  `deregistered`), holding its row `FOR UPDATE`.
- The instance object's `connection` is the latest connection, kept after it
  ends: `online` with an accepted ready and a live lease, `registering` while
  its ready is neither accepted nor refused and it has not ended, otherwise
  `offline`; `error` is a refused ready's `{code, message}`; `capabilities`
  come from the instance's latest accepted ready. `cli_version` is the latest
  ready's, accepted or refused, and `cli_update_available` compares it with
  `TETRAL_COMPUTER_HUB_RECOMMENDED_CLI_VERSION`. A credential is `in_use`
  when the live connection uses it.
- Lists take `limit` (default 20, at most 100, a positive integer given once)
  and `page`, a token signed with `TETRAL_COMPUTER_HUB_PAGE_TOKEN_KEY` that
  binds the workspace, the route, the filters and the order; items come in
  creation order and `next_page` is `null` after the last page.
- A missing or other-workspace object answers `404 not_found_error`; any other
  failure than the ones above is a database failure, `503` with nothing
  changed. The key appears only in the first successful issuance response;
  nothing logs a request or response body.
- `GET /v1/instances/stream` answers SSE: `event: credential_reused` or
  `event: unused`, `data:` the instance object. A writer that produces an
  event calls `instance.NotifyInstanceEvent` in its transaction
  (`pg_notify` on `tetral_hub_instance_events`); every pod listens on that
  channel (`InstanceEvents.Run`, reconnecting after a failure, which writes
  `instance_events.listener_disconnected`), reads the instance object of each
  notice and queues the event for its own subscribers in that workspace.
  Delivery is best effort: a subscriber more than 64 events behind is closed,
  a reconnecting subscriber gets no backlog, every stream ends at shutdown,
  and every open stream ends when the pod listens again after a failure,
  since the notices sent meanwhile are lost, so that its Controller reads
  the instances again.
- `last_credential_reuse` is the latest use of a credential that an
  activation revoked (client routes below).
- Each issued, fetched, activated or revoked credential writes one
  `instance_credential_audit` record (`log.type` `audit`) after the commit,
  with the actor's credential kind and ID, `instance.id`, `credential.id` and
  `operation` `issue`, `fetch`, `activate` or `revoke`; the client routes'
  actor is the connection key.

## Controller request routes

A cloud computer without a usable instance holds at most one open request,
which tool-call admission records (`computer.EnsureRequest`): `new` without a
current instance, `wake` for an offline current instance.
`GET /v1/instance_requests`, `GET /v1/instance_requests/stream` and
`POST /v1/instance_requests/{request_id}/claim`, `…/release` and `…/fail` run
under the operations `instance_requests.*`; claim, release and fail lock the
request row `FOR UPDATE`.

- The list and the stream require `environment_id` once, and the list accepts
  `state` only as `pending`; anything else answers `400
  invalid_request_error`. The list pages the Environment's pending requests in
  creation order as the instance lists do, with both filters bound into the
  token.
- Claim takes `{worker_id}`, a nonempty string. A pending `new` request gets, in
  one transaction, a new active unnamed `cloud` instance of the computer with
  an `active` credential of delivery `claim` expiring after the maximum
  lifetime, and answers `request`, `bootstrap` (`server_url` from
  `TETRAL_COMPUTER_HUB_SERVER_URL`, `instance_id`, `credential_id`,
  `connection_key`, `expires_at`) and the computer's `working_directory`. A
  pending `wake` request first revokes its instance's never-used `pending`
  credentials with cause `request_ended` and then issues a `pending` claim
  credential, without `working_directory`; at the credential limit the
  transaction rolls back and answers `409 credential_limit_error`. The worker
  holding a claim gets the same IDs again with `connection_key: null`, another
  worker `409 request_already_claimed_error`, and an ended request `409
  request_not_pending_error`.
- Release returns a claimed request to `pending` and fail ends it as `failed`,
  each through `internal/hubflow` (`ReturnRequestToPending`, `FailRequest`),
  which also ends the live connection of the claim credential the writer
  revoked; a request already in the target state answers `200` unchanged, any
  other `409 request_not_claimed_error`. A returned or failed `new` claim
  revokes its credential (causes `new_claim_returned`, `request_ended`), which
  retires its instance; a failed `wake` claim revokes its credential only
  while it is still `pending`.
- An accepted ready fulfils the requests of its instance
  (`hubflow.AcceptReady`): a claimed `new` request, whose instance becomes the
  computer's current instance, and a pending or claimed `wake` request.
- The first use of a `claim` credential activates it and revokes every other
  usable credential of the instance with cause `pending_activation`, so a
  later use of one is recorded as reuse.
- `instances.unused` changes only through `instance.SetUnused`, which
  `computer.RefreshUnused` calls after each claim, return, fail and fulfilment.
- Each transition calls `pg_notify` on `tetral_hub_instance_requests` in its
  transaction; every pod listens (`RequestEvents.Run`, which writes
  `request_events.listener_disconnected` after a failure) and writes
  `created`, `claimed`, `pending` or `expired` with the request object to its
  subscribers of that workspace and Environment, best effort as the instance
  stream is: a pod that listens again after a failure ends every open request
  stream, and its Controller lists the requests again.

## Client connection routes

`GET /v1/client/events`, `POST /v1/client/ready`, `POST /v1/client/heartbeat`,
`POST /v1/client/protocol-error`, `POST /v1/client/credential`, `POST
/v1/client/session-status` and `POST /v1/client/git-ticket` check each
request in this order, the first failing check answering (`client_checks.go`):

1. size: at most 65,536 bytes of a JSON body are read; a longer body answers
   `413 request_too_large`, with no ACK and nothing recorded;
2. shape: the `internal/protocol` decoder of the body or query, `400
   invalid_message` (`protocol_version` other than `1` included);
3. identity, on `GET /v1/client/events` only: the query's `instance_id` and
   `credential_id` must be the principal's, else `401 credential_invalid`;
4. authority: the instance and credential rows locked `FOR SHARE` in the
   request's transaction, except that a ready, which sets the instance's
   `cli_version`, and a use of a credential an activation revoked lock the
   instance `FOR NO KEY UPDATE`, chosen by an unlocked read of the
   credential first; a use that an activation overtook between that read
   and the locks runs its transaction again. A deregistered instance answers `403
   instance_unavailable`, a revoked credential `401 credential_revoked`, an
   expired one `401 credential_expired` and a `pending` one `401
   credential_invalid`, except on `GET /v1/client/events`, which activates
   it (below). A use of a credential revoked with cause
   `pending_activation` also sets the instance's `last_credential_reuse` to
   it and the database clock and sends `credential_reused` on the instance
   stream, and that refusal commits; a credential revoked by `DELETE` or
   deregistration records nothing;
5. connection, on every POST: `connection_id` must name the instance's live
   connection, not ended and with its lease later than the database clock,
   else `409 stale_connection`;
6. the replay ledger: the same `message_id` with an equal canonical
   fingerprint on the same route returns the stored answer byte for byte and
   applies nothing; with another fingerprint or route, `409 message_conflict`;
7. the route's own checks.

A database failure answers `503` and changes nothing. Every transaction locks
the instance, then credentials, then the connection, each at once in the mode
its writes need (`internal/instance`):

- a protocol-error report locks the connection `FOR UPDATE`, so a copy in
  flight waits and gets the stored ACK;
- a credential fetch reads the credential it names without a lock and, when
  it is `pending` and the principal is `active`, locks it `FOR UPDATE` before
  the principal's, the order of the renewal's activation; otherwise it
  leaves it unlocked;
- a ready or heartbeat reads the connection without a lock first and, when
  the connection's credential, which caps the grant, is not the principal's,
  locks it `FOR SHARE` after the principal's and before the connection.

The ledger records the messages Engine accepts and the refused ready; a
message refused by an earlier check records nothing. The connection row holds
the ready's record and the latest heartbeat's; other messages are rows of
`connection_upstream_messages`, deleted when the connection ends.

- `GET /v1/client/events` locks the instance's connection that has not ended
  `FOR UPDATE` and applies the predecessor rules in one transaction. With no
  live connection it ends one whose lease passed (`hubflow.EndConnection`,
  cause `lease_expired`) and creates a connection: `conn_` and 32 hexadecimal
  characters, the principal's credential, this pod as `hub_pod`, a lease of the
  earlier of 45 seconds and the credential's expiry; `connected` carries
  `resumed: false`. A live connection that `previous_connection_id` names with
  `resume=true` resumes (`instance.ResumeConnection`): `hub_pod` becomes this
  pod and `stream_count` increases, the lease stays, the previous pod is
  notified and closes its stream without a `disconnect`, and `connected`
  carries the same connection and `resumed: true`. With `resume=false` it
  answers `409 previous_connection_active` with `retry_after_ms`, the
  remaining lease in milliseconds rounded up; without its ID, `409
  connection_conflict`. A refusal changes nothing. A `pending` credential,
  a renewal whose key was fetched, is activated in the transaction that
  resumes the connection or creates the new one: it becomes `active`
  (`used_at`), the connection's credential, so later grants are capped at its
  expiry, and the credential it replaces is revoked with cause
  `pending_activation` unless it has expired, so a later use of an expired one
  answers `401 credential_expired` and records no reuse; a refused request
  activates nothing. A `200` has
  `Content-Type: text/event-stream` and `Cache-Control: no-store`. A
  draining pod answers `503`.
- `POST /v1/client/ready` on a connection whose ready is unanswered: a CLI
  older than `TETRAL_COMPUTER_HUB_MIN_CLI_VERSION` by semantic version
  precedence is refused with `403 cli_upgrade_required`, recorded on the
  connection, whose lease is not renewed; otherwise the grant is the earlier of
  45 seconds and the connection credential's expiry, refused with `401
  credential_expired` below one millisecond, and `hubflow.AcceptReady` stores
  it with the ready's `os`, `arch`, `cli_version` and `capabilities` and
  notices the stream's pod. The ACK's `valid_for_ms` is the grant in whole
  milliseconds. A second ready under a new `message_id` answers `400
  invalid_message`.
- `POST /v1/client/heartbeat` is accepted after the ready, when
  `request_message_id` is `hb_<connection_id>_<heartbeat_seq>` and
  `heartbeat_seq` is above the latest accepted one; it renews the lease by
  the grant rule of ready. Any other new heartbeat answers `400
  invalid_message` and renews nothing.
- `POST /v1/client/protocol-error` stores the report in
  `connection_protocol_errors`, as given, and writes one `WARN`
  `client.protocol_error` record with `connection.id`, `message.id`,
  `error.code` and `request.message.id`, never the report's message.
- `POST /v1/client/credential` fetches a renewal's key once: it generates the
  key, stores its digest and `fetched_at`, and answers `200` with
  `{credential_id, connection_key, expires_at}` and `Cache-Control: no-store`.
  An unknown credential, another instance's, one delivered in a response or
  renewing another connection than the request's (its `renewal_connection_id`),
  one that is not `pending`, is revoked or expired, or was already fetched
  answers `404 not_found` and changes nothing. The ledger keeps the fetch's route and
  fingerprint but no answer, so a retry with the same `message_id` and content
  is checked again and answers `404`.
- An accepted ready, after it fulfils its requests, issues a
  `session.prepare` (`computer.IssuePreparation`) for every `attached`
  computer whose current instance is the readying one and which holds no
  `preparing`, `prepared` or `failed` binding on that connection; Session
  creation and the computer add route issue one for each new computer whose
  instance is already online.
- `POST /v1/client/session-status` takes the Session's runtime mutation lock
  first, the order of every writer of a Session event. A `prepared` or
  `failed` report whose `request_message_id` names a `session.prepare` issued
  on this connection for its `session_id` and `binding_id` is stored with its
  ACK in the ledger, marks the instruction reported and the connection's
  first Session report, and applies to a `preparing` binding: `prepared` makes
  the binding `prepared` and the computer `ready` with the report's `cwd`,
  `failed` makes both `failed`; the same transaction writes the report's
  `session.error` events (one `computer_preparation_error` per new failure
  code, one `drive_mount_error` per mount failure and, on a cloud computer,
  one repository error per repository failure). A binding in any other state
  changes nothing. Any other report answers `409 unknown_instruction`; a
  replay returns the stored ACK and writes nothing.
- `POST /v1/client/git-ticket` issues a new ticket for the cloud computer with
  repositories whose preparation on this connection `binding_id` names,
  revokes the computer's earlier ticket, stores only its SHA-256, and answers
  `200` with `{git_proxy_url, ticket}` and `Cache-Control: no-store`; any other
  `binding_id` answers `409 unknown_instruction`. The ledger keeps the route
  and fingerprint but no answer, so a retry with the same content is a new
  fetch.

Streams (`stream.go`): each event is one `data:` line holding one envelope
and a blank line, flushed as it is written, with no `event:`, `id:` or
comment. A write has no total deadline: it is made in pieces of at most
16 KiB, and a piece that the stream's socket does not accept within 10
seconds breaks the stream (`streamStallTimeout`), so a reader that frees no
room in that socket's send buffer for 10 seconds loses its stream. `connected` and
`disconnect` get a new `msg_` ID. Once the connection's ready is accepted the
stream writes `heartbeat` every 15 seconds, the first one 15 seconds after it
learns of the acceptance, or right after `connected` on a resumed stream of an
accepted connection; the n-th heartbeat of the stream counted s carries
`heartbeat_seq` s × 1,048,576 + n and `message_id`
`hb_<connection_id>_<heartbeat_seq>`. A stream is registered after its
connection commits, and its first pass reads what changed meanwhile. At
`TETRAL_COMPUTER_HUB_STREAM_MAX_AGE` plus a uniform extra of 0 to a tenth of
it, drawn per stream and counted from `connected`, the stream writes
`disconnect` with `scope: "stream"` and closes. On shutdown the pod turns
unready, refuses new streams, writes that `disconnect` to each of its n
streams, the i-th at i/n of `TETRAL_COMPUTER_HUB_STREAM_DRAIN_WINDOW`, and
only then stops; a stream whose pass is writing when its drain is due is
closed at the window's end without a `disconnect`. Each listener drains
within the window plus 10 seconds.

Instruction writes (`dispatch.go`): a stream writes renewals and Session
instructions in passes, one at its start and one each time a notice or the listener's return
wakes it. A pass chooses what to write inside a transaction that reads the
connection without a lock and then locks the instance, the connection's
credential and the connection `FOR SHARE`, in that order, and writes it only
after that transaction commits. An
ended connection writes `disconnect` with `scope: "connection"`, `retryable:
false` and `code` `credential_revoked` (revoked) or `instance_unavailable`
(deregistered), none for `lease_expired`, and closes the stream; a
connection whose `hub_pod` or `stream_count` names another stream closes it
without one. While the lease has passed, the credential is revoked or
expired, the instance is deregistered or the ready is unaccepted, the pass
chooses nothing. Otherwise it chooses `credential.renew` once per stream for
each `pending`, unexpired renewal of the connection, under `message_id`
`renew_<credential_id>` on every stream; then, in issue order and each once
per stream, the stored envelope of every instruction without `reported_at`
whose `issue_seq` is at most the stream's bound, and until
`first_session_report_at` is set only `issue_seq` 1 to 64. The bound is the
highest `issue_seq` a notice released; a stream start and the listener's
return release all the connection row counts. Before each instruction a pass
writes, and before the pass that follows one that wrote, the stream writes a
heartbeat that fell due; one that falls due while an instruction is being
written waits until that instruction is written. While a pass writes, a notice
that finds the connection, read without a lock, ended or held by another
stream, or the stream's close, cuts the write and ends the stream; any other
write in progress ends by its stall timeout. A pass's
transaction has 10 seconds. A pass whose transaction fails, by a database
error or by that deadline, runs again after 1 second, the wait doubling after
each failed pass up to 30 seconds and multiplied by 1 + r, r uniform in 0 to
0.2; a pass that commits returns the wait to 1 second. A pass that wrote
instructions is followed at once by one whose transaction records their
first write to any stream: it sets `first_written_at`, observes
`computerhub_instructions_queue_wait_seconds` and, when the instruction has a
`trace_context`, is one span `instruction.deliver` in that trace with
`connection.id`, `message.id` and `message.type`, each once that transaction
commits; when it fails, the stream's next pass records the write without
writing the instruction again. Pending recording retains only message/type/trace
metadata, independently of the unreported delivery selection: a report stored
during the writing pass does not discard its successful write. Entries are
removed only after recording commits, and an already recorded write emits no
second observation.

Connection notices (`notice.go`): each pod listens on `tetral_hub_` and the
first 16 hexadecimal characters of the SHA-256 of
`TETRAL_COMPUTER_HUB_POD_NAME`. Every writer that changes what a stream must
write (`instance.IssueInstruction`, `instance.MarkFirstSessionReport`,
`instance.EndConnection`, `hubflow.AcceptReady`) notifies the channel of the
connection's `hub_pod` with `{"workspace_id", "connection_id", "issue_seq"}`
in its transaction, and `instance.ResumeConnection` the previous pod's. A
notice for a stream the pod holds raises the stream's bound to its
`issue_seq` and wakes it; each time the listening connection is in place,
the first time included, every held stream is woken to write all its
unreported instructions, since notices sent while none was in place are lost.
A listener disconnect writes `connection_notices.listener_disconnected`.
Delivery of a `disconnect` is best effort.

Metrics: `computerhub_open_streams`, the streams this pod holds, which the
autoscaler averages; `computerhub_connections_started_total{kind}`, each
`connected` written, `new` or `resumed`;
`computerhub_streams_ended_total{cause}`, each stream the pod held, once when
it ends, by the first cause that ended it: `drain` (the pod's drain or
shutdown closed it, with or without its `disconnect`), `age_limit`,
`stalled` (the socket accepted no byte of a write for 10 seconds),
`connection_ended` (lease expiry, revocation or deregistration), `moved` (it
resumed on another stream) or `client_closed` (the client closed it, or a
read or write failed otherwise); the pressure point
`computerhub_instructions`: `_in_progress` (written, final report not
stored), `_queued` (stored, never written) and `_oldest_queued_seconds` (by
the database clock), read at scrape over the unreported instructions of every
live connection in every Workspace through
`tetral_computer_hub_instruction_pressure`, so every pod reports the same
totals and dashboards and alerts aggregate them with `max`, never `sum`; and
`_queue_wait_seconds`. The hub refuses no instruction, so the
point has no rejection counter. The pressure point
`computerhub_controller_requests` reads `_queued` (pending), `_in_progress`
(claimed, not yet fulfilled) and `_oldest_queued_seconds` the same way through
`tetral_computer_hub_request_pressure`; `_queue_wait_seconds` is observed from
creation to claim by the pod that handles the claim, and
`_rejected_total{cause}` counts each Controller `fail` as `failed`, with the
episode records `pressure.rejecting_started` and `pressure.rejecting_ended`.

A ready that carries `startup` and is answered for the first time records the
`cli` cold start: the CLI's `config`, `connect` and `ready` phases with the
reported durations, and `register`, from receiving the ready to answering it
(`workload.BeginReportedColdStart`); an accepted ready completes it, a refused
one ends it with `cli_upgrade_required` and observes nothing.

A stored preparation report ends the computer's `session_computer` cold
start (`workload.RecordStoredColdStart`). Every phase is the difference of two
database clock times stored by the transaction that made the transition:
`request` (the request's `created_at` to its `claimed_at`) and `instance`
(that `claimed_at` to the `ready_accepted_at` of the instance's first
connection whose ready was accepted after the claim), present only for the
computer's first preparation on that connection, and `prepare` (the binding's `created_at` to
its `reported_at`). `start_kind` is `new` for the computer's first preparation
and `reloaded` for a later one. A `prepared` report records `outcome` `ok` and
observes both histograms, the `engine` scope leaving out `request` and
`instance`; a `failed` report records the error code and observes nothing.

Controllers authenticate with the workspace API key; there is no
Controller-specific credential. Every management route acts only in the
principal's workspace, under that workspace's row-level security; no body field
names a workspace.

## Configuration and ports

| Variable | Default | Meaning |
|---|---|---|
| `TETRAL_COMPUTER_HUB_HTTP_ADDR` | `:8080` | public listener, behind the edge; `/health` and `/ready` also answer here, `/metrics` does not |
| `TETRAL_COMPUTER_HUB_METRICS_ADDR` | `:8081` | metrics, health and readiness; must differ from the public address |
| `TETRAL_DATABASE_URL` | required | the `computer_hub` serving DSN |
| `TETRAL_DATABASE_TLS_CA_PATH`, `TETRAL_DATABASE_TLS_SERVER_NAME` | | database TLS, as every serving workload |
| `TETRAL_AUTH_INTERNAL_PRINCIPAL_PUBLIC_KEY_B64` | required | Auth's Ed25519 public key for the signed internal principal |
| `TETRAL_COMPUTER_HUB_MAX_CREDENTIAL_LIFETIME` | `720h` | how far after issuance an issued credential's `expires_at` may lie, and the lifetime of one issued without it; a positive Go duration, Helm `computerHub.maxCredentialLifetime` |
| `TETRAL_COMPUTER_HUB_PAGE_TOKEN_KEY` | required | 32 bytes as 64 hexadecimal characters that sign list page tokens; every pod reads the same key from the Secret Helm `secrets.computerHubPageToken` names, key `page-token-key` |
| `TETRAL_COMPUTER_HUB_SERVER_URL` | required | the Engine base URL a claim's bootstrap names as `server_url`: an absolute `http` or `https` URL with a host; Helm sets `https://` and `edge.apiHost` |
| `TETRAL_COMPUTER_HUB_GIT_PROXY_URL` | required | git-proxy's public origin, the `git_proxy_url` of every git ticket: an `https` origin (scheme, host and optional port) with no path and no trailing `/`; Helm sets `https://` and `gitProxyHost` |
| `TETRAL_COMPUTER_HUB_MIN_CLI_VERSION` | `0.0.0` | a ready from a CLI older than this semantic version is refused with `cli_upgrade_required`; Helm `computerHub.minCLIVersion` |
| `TETRAL_COMPUTER_HUB_RECOMMENDED_CLI_VERSION` | `0.0.0` | an instance whose CLI is older than this semantic version reads `cli_update_available: true`; never lower than the minimum; Helm `computerHub.recommendedCLIVersion` |
| `TETRAL_COMPUTER_HUB_POD_NAME` | required | this pod's name, from the downward API: the `hub_pod` of the connections whose streams it holds and the source of its connection notice channel |
| `TETRAL_COMPUTER_HUB_STREAM_MAX_AGE` | `30m` | a stream's age limit before its stream-only `disconnect`, plus a random extra of up to a tenth of it drawn per stream; a positive Go duration, Helm `computerHub.streamMaxAge` |
| `TETRAL_COMPUTER_HUB_STREAM_DRAIN_WINDOW` | `10s` | the time over which a stopping pod spreads the stream-only `disconnect`s of its streams; a positive Go duration of at most `15s`, so the drain fits the 30-second termination grace period; Helm `computerHub.streamDrainWindow` |
| `TETRAL_COMPUTER_HUB_DRIVE_UPLOAD_BUDGET_BYTES_PER_MINUTE` | `500000000` | the per-minute amount of each Workspace's drive upload budget, shared with `api` through `drive_upload_budgets` (`internal/drive`); an integer of at least `65536`, the least a request counts; Helm `drives.uploadBudgetBytesPerMinute` |
| `TETRAL_BLOB_*` | | the blob store settings `api` and Bridge use (`internal/blob`) |
| `TETRAL_HTTP_TRANSPORT`, `TETRAL_HTTP_TLS_*` | | the public listener's native TLS from the edge (`internal/transportsecurity`) |

Startup reads the configuration, opens the database, verifies the schema and
the serving role, opens the blob store, and then serves; each failure is one
redacted startup record. Shutdown drains the streams over the drain window
and both listeners within the window plus 10 seconds.

Replicas (`k8s/hpa.yaml`, `k8s/deployment.yaml`): any pod serves any request,
and a pod holds only the streams it serves, so a CLI resumes on another pod
when its pod drains or dies. A HorizontalPodAutoscaler keeps 2 to 10 pods (Helm
`computerHub.autoscaling.minReplicas`, `maxReplicas`) at an average of 1,000
`computerhub_open_streams` per pod (`targetOpenStreamsPerPod`) through
prometheus-adapter, and the Deployment sets no replica count, so an upgrade
keeps the count it chose; the pod's readiness probe and its 30-second
termination grace period carry the drain, which is why Helm refuses a
`computerHub.streamDrainWindow` over 15 seconds.

## Seams

- `ConfigFromEnv` parses every `TETRAL_COMPUTER_HUB_*` setting and the
  principal key.
- `NewRouter(verifier, ...)` is the public route table with its class checks;
  `WithInstanceRoutes` serves the instance and request routes over a database
  client and the `InstanceEvents` and `RequestEvents` fan-outs, which `Run`
  starts; `WithClientRoutes` serves
  the client routes over a database client and the pod's `Streams`, whose
  `Run` listens for connection notices and whose `StreamClock` tests drive;
  `BuildHTTPHandler` adds health and readiness. Tests mint principals with
  `auth.InternalPrincipalSigner` and send them here directly.
- `Run(ctx, cfg, Runtime)` serves the two listeners over a verified database
  client.

## Testing guide

- `config_test.go`: the required settings, overrides and each rejected setting.
- `run_test.go`: `Run` assembles the service and hands its public and metrics
  listeners, each answering readiness, to the workload runner.
- `instance_routes_test.go`, over HTTP as the `computer_hub` role with minted
  principals: a lost creation response replayed without a second instance and
  a changed request refused; the name and `Idempotency-Key` bounds, a refused
  key reading no record; ten concurrent issuances against the credential
  limit; no key outside its issuance response, in reads, lists, error bodies,
  idempotency rows, logs or audit records; the states and fields of instance
  and credential reads; repeated revocation and deregistration; expiry bounds
  and an omitted expiry resolved once; paging 101 instances.
- `instance_stream_test.go`: two streams in two workspaces each receive only
  their own workspace's noticed event.
- `request_routes_test.go`, over HTTP with requests recorded in-process by
  `EnsureRequest`: two workers claiming a `new` and a `wake` request at once,
  the repeated claim and the claim after release; every other cell of the
  state table; a `wake` claim at the credential limit changing nothing; a
  returned `new` claim retiring its instance and an expired request making way
  for the next; a refused ready leaving the claim and a Controller's fail
  ending it; readies fulfilling a `wake` and a `new` request; reuse recorded
  for a credential a `wake` claim replaced and not for a returned claim's;
  malformed queries; paging one Environment's pending requests; one
  Environment's stream events through another pod; the queue wait, the
  pressure gauges read through two pools, and a fail counted once.
- `client_routes_test.go`, over HTTP and SSE with the test CLI of
  `integration/enginetest` and a hand-driven `StreamClock`: each credential
  failure on the events route with no connection written; grants capped by the
  credential's expiry and by 45 seconds, and a heartbeat after the expiry
  refused; replayed ready and heartbeat answering their original bytes with
  the lease unchanged, and a second ready, an early heartbeat and a stale one
  refused without a change; a report on an ended connection stale; a
  protocol-error report replayed, conflicting and logged without its message;
  `protocol_version` 2 refused and an unknown optional member accepted; a CLI
  below the minimum refused and read offline with its error; the connection
  states of instance reads and `in_use`; the 65,536-byte body limit; the
  heartbeat schedule and sequences of a first and a resumed stream; a
  revocation or deregistration ending the live connection with its
  disconnect.
- `client_failure_test.go`: first ready/heartbeat cross-route replay conflicts
  return typed 409 before response reconstruction. Authority and replay-ledger
  query failures return 503 without accepting a message or changing the lease
  or replay ledger.
- `instruction_delivery_test.go`, with two hub servers of different pod names
  on one database: a resume on the other pod writing each unreported
  instruction once more as stored, the previous pod's stream closed without a
  disconnect and the lease kept; a resume with 5 seconds of lease left whose
  first heartbeat's answer renews 45 seconds past it; a resume of another
  connection and a fresh start refused with nothing replaced;
  `previous_connection_active` with the remaining lease, then a new connection
  once it passed; the stream-only disconnect between 10 and 11 minutes of a
  10-minute age; each stream of a stopping pod held in a pass getting its
  stream-only disconnect and counted once as `drain`; a notice on the
  holding pod's channel only, and an
  instruction whose notice was lost written after a listener restart and a
  resume; 64 of 70 instructions before the first Session report; a
  revocation waiting for a pass that holds the row and ending the stream,
  and a pass after a revocation choosing nothing; the stored traceparent on a resend,
  one `instruction.deliver` span and the started counter; the pressure gauges
  of a written and a queued instruction whose stream is gone.
- `slow_reader_test.go`, with a test CLI whose socket receive buffer is
  4 KiB: a 16 MB backlog read at 1 MiB/s arriving whole on one stream; one
  read for more than 45 seconds keeping its connection, the two heartbeats
  that fell due written between its edits and answered; a reader that stops
  losing its stream 10 seconds after its last byte, counted once as
  `stalled`, and then resuming on a second hub server; a revocation
  committing while a write is blocked and closing the stream.
- `credential_renewal_test.go`: a stream issuance on an online connection
  answering `202` with one notice and one `credential.renew`, its replay `202`,
  and `409 instance_offline_error` with nothing written without a connection
  or before the ready is accepted; a renewal of an ended connection kept
  `pending` and never written on the next connection; `404 not_found` for every
  credential that may not be fetched and for the retry of a fetch whose answer
  was lost, `409 message_conflict` for its `message_id` with other content; a
  heartbeat with the fetched credential refused, then the resume that
  activates it and revokes the old one in one commit, with grants capped by
  the new expiry; a new connection with it after the old lease passed
  activates it too, revoking the old credential unless it has expired; a use
  of the revoked old credential recorded with one `credential_reused` event,
  a use of a deleted or an expired one not.
- `client_cold_start_test.go`, with the process telemetry installed: one `cli`
  cold start with the reported phases for the first answered ready, none for a
  replay or a ready without `startup`, and a refused one observing nothing.
- `integration/computer_hub_credential_class_test.go`: each credential class on
  the other class's routes, through `auth`'s Check and a Go edge with the
  production route rules, and straight at the hub with principals Check never
  mints there; every attempt answers `401` in its surface's body and changes no
  row of any table.
- `services/auth/ext_authz_test.go`: the class rules at the Check boundary,
  with the number of lookup statements per request.

## Process diagnostics

The service records through `internal/workload` with the shared HTTP request
records and metrics, the cold start of a service pod, and the `auth.*`
decision records of the signed principal and the operation gate, and the
`instance_credential_audit` record, the three notice listeners' disconnect
records and `client.protocol_error`, declared in
[`observability.json`](observability.json),
and the `cli` cold start reported by each first answered ready. Every record
of a request the principal check admitted carries its `workspace.id` and
`request.id` (`routes.go`). A `POST
/v1/client/heartbeat` writes no request record, starts no trace and observes no
request histogram; a `GET /v1/client/events` stream writes its record and
stream sample when it ends but starts no span (`routes.go`).
