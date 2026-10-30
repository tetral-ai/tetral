# api

## Responsibilities

The request/response half of the platform's public API surface: every SDK
route that answers immediately terminates here. The service validates the
request, writes durable rows, and returns — it executes nothing. Every request
arrives already authenticated: the edge called `auth`, stripped the raw
API key or Bearer token, and injected a signed internal principal; this service verifies that
principal and takes workspace authority from it alone. When admitted work needs
anything to happen later — Sandbox lifecycle work, an environment build, cleanup — that
fact leaves this service in exactly one vehicle: a queue job written in the same
PostgreSQL transaction as the business rows. The HTTP layer lives in
`internal/httpapi`; each public surface has a domain package behind it
(`internal/session`, `internal/environment`, `internal/vault`,
`internal/memory`, `internal/files`, `internal/skill`), secrets are encrypted by
`internal/encryption`, and the process is assembled in
`services/api` (`wiring.go`, `routes.go`, `bootstrap.go`). Those
`internal/…` domain packages are shared packages at the repository root, not
under `services/api/internal/`; this service wires them together but does
not own their trees.

### Replica and request lifetime

API replicas use the same durable database and object store with independent
client pools. A Session created or updated through one replica is visible
through another after commit; the signed principal and database workspace
scope determine access on every request. The existing event append
`Idempotency-Key` contract preserves the same event/inbox/Queue identity after
a response is lost, for 24 hours after the original admission. It does not add replay rights to ordinary writes such as
Session creation. An uncertain ordinary write must be reconciled through its
existing read surface rather than automatically submitted again.

Shutdown removes readiness and new HTTP admission, lets admitted requests
finish within the service budget, then cancels and joins remaining handlers
before closing shared-store clients. An event append canceled before commit
rolls back its event, inbox and Queue writes together. The replica composition
uses the pinned SDK, two Auth/API instances and independent database
observations to exercise these boundaries; cluster routing remains deployment
validation.

### Boot requirements

Booting requires `TETRAL_DATABASE_URL`, `ENGINE_VAULT_KEY`,
`TETRAL_AUTH_INTERNAL_PRINCIPAL_PUBLIC_KEY_B64`, and
`TETRAL_DEFAULT_ENVIRONMENT_ARTIFACT_REF`; the rest are optional or test-only
as noted.

| Variable | Purpose |
|----------|---------|
| `TETRAL_DATABASE_URL` | PostgreSQL DSN for the restricted API serving role. Production connections require verified TLS with the explicit CA and server name; a DSN cannot enable plaintext fallback. PostgreSQL 18 is the tested target. |
| `TETRAL_DATABASE_TLS_CA_PATH` / `TETRAL_DATABASE_TLS_SERVER_NAME` | Required public CA bundle path and expected database DNS name. New connections use validated mounted trust. |
| `TETRAL_BLOB_TLS_CA_PATH` / `TETRAL_BLOB_TLS_SERVER_NAME` | Required public CA bundle and expected endpoint name when production object storage is configured. |
| `TETRAL_TEST_DATABASE_URL` | Test-only administrative DSN for focused `go test`; not read by the running server. The helper clones one immutable schema template into a private database and unique NOBYPASSRLS login per test. |
| `ENGINE_DATA_DIR` | Optional; defaults to `/var/tetral`. Local filesystem state root — control-plane records never live here, so moving it migrates no SQL data. |
| `ENGINE_VAULT_KEY` | 32-byte hex AES key encrypting vault credential secrets at rest; never carries request authentication. |
| `TETRAL_AUTH_INTERNAL_PRINCIPAL_PUBLIC_KEY_B64` | Base64-encoded Ed25519 public key used to verify the signed internal principal injected by the auth service. |
| `TETRAL_DEFAULT_ENVIRONMENT_ARTIFACT_REF` | Prebuilt provider artifact reference used for environments with no custom packages. |
| `TETRAL_API_DRIVE_UPLOAD_BUDGET_BYTES_PER_MINUTE` | Optional; defaults to `500000000`. The per-minute amount of each Workspace's drive upload budget, shared with the computer-hub through `drive_upload_budgets` (`internal/drive`); startup refuses a value below `65536`, the least a request counts. Helm `drives.uploadBudgetBytesPerMinute`. |
| `ENGINE_PORT` | Optional legacy fallback. Read only when `TETRAL_API_HTTP_ADDR` is unset, and then only for the port half — the listener becomes `:$ENGINE_PORT`. Setting `TETRAL_API_HTTP_ADDR` makes it inert. |

The public API listener binds `:8080` (`TETRAL_API_HTTP_ADDR`); metrics bind
`:8081` (`TETRAL_API_METRICS_ADDR`); the two addresses must differ.

Startup verifies the installed schema and the restricted serving role before
opening the public listener. It never migrates schema or installs roles, and
receives no administrative or migration-owner credential. An unprepared,
outdated, or incompatible database stops startup with a safe `startup.failed`
log; it does not trigger a repair.

Run the release's standalone `tetral-db-prepare` command before starting updated
services. See [database preparation](../../database/README.md) for ordering,
failure handling and migration diagnostics.

### Workspace isolation

Isolation rests on signed principal binding, `workspace_id` in every primary
key, and a database-enforced isolation policy. The auth middleware attaches
the resolved workspace to request context from the signed internal principal,
and every store query predicates on `workspace_id`. Independently of
application code, the PostgreSQL schema enables and forces a
`workspace_isolation` policy on every workspace-owned table
(`USING (workspace_id = current_setting('tetral.workspace_id', true))`), so a
query that omits its predicate still cannot resolve rows from another
workspace. The `true` (missing_ok) second argument makes an unset setting
resolve to `NULL` — matching zero rows — instead of raising, which is what
makes the guarantee fail closed. A new workspace-scoped table therefore needs
both the `workspace_id` primary-key column and the matching policy; the
primary key alone does not filter a predicate-less query. The engine refuses
to start under a runtime role that can bypass those policies — a superuser or
a role with the bypass attribute — so the guarantee cannot be silently
disabled.

Workspace is the sole PostgreSQL tenant boundary. Session and Thread IDs remain
explicit keys, foreign keys, query predicates, and lifecycle ownership facts;
they are not additional RLS settings. The canonical live-catalog verifier also
rejects missing FORCE RLS, altered policies, extra permissive policies, or an
unsafe effective serving role before readiness.

### Public `/v1` surface status

All public endpoints require edge authentication; `GET /health` is
unauthenticated.

| Group | Status |
|-------|--------|
| Sessions | Live create/list/get/update/archive/delete, session resources, and session computers (add, get) |
| Session threads | Live read surface under `/v1/sessions/{id}/threads` (list, get, archive) |
| Session events | Live `POST /v1/sessions/{id}/events` (the admission-capped agent/session input path) and `GET` list. The SSE surfaces are owned by the `event-stream` service |
| Agents | Live create/list/get (optionally at a specific version via a query parameter)/update/archive/version list. A version is read through the get route's version selector |
| Environments | Live create/list/get/update/archive/delete |
| Vaults + Credentials | Live create/list/get/update/archive/delete |
| Memory Stores | Live create/list/get/update/archive/delete, memory rows, and versions |
| Files | All five `/v1/files` routes live when a BlobStore is configured; without one, every `/v1/files` route returns 501 — not only uploads |
| Skills | All nine `/v1/skills` routes live when a BlobStore is configured; without one, every `/v1/skills` route returns 501 — not only uploads |
| Drives | All `/v1/drives` routes of the developer drive API live when a BlobStore is configured; without one they are not registered |
| Models | Live `GET /v1/models` and `GET /v1/models/{model_id}`, served from the in-code catalog |
| API keys | Owned by the `auth` service |
| Health (`GET /health`) | Live (200), unauthenticated |

### Session computers

`internal/httpapi/computer_handler.go` and `internal/session/computers.go`
serve the computers of a Session on `internal/computer`, the one writer of new
computers. Session creation takes `computers`, or the top-level
`environment_id` as a one-computer list (a cloud computer at `/workspace`
holding the `github_repository` entries of `resources`); `computers` with
`environment_id`, or with a `github_repository` resource, answers `400`, and
every Session needs at least one cloud computer. Each entry is checked in
this order, and nothing is written when one fails: exactly one of
`environment_id` and `instance_id`, the Environment or instance it names,
`working_directory`, `networking`, `repositories` and `drive_exclude`. A
failure answers `400 invalid_request_error`, `404 not_found_error` (an
unknown Environment or instance), `409 instance_not_attachable_error` (a
cloud or deregistered instance) or `409 computer_already_attached_error` (an
instance named twice or already attached to the Session). A computer's `networking` copies its
Environment's value at addition. Its repositories are stored as
`github_repository` resource rows with `computer_id` and the resolved `path`
(`mount_path`, or `<working_directory>/<name>`) and are fixed: `POST
/resources` with a `github_repository` and `DELETE` of a repository resource
answer `400`; token rotation stays allowed.

`POST /v1/sessions/{session_id}/computers` adds one entry to an idle Session
and answers `201` with the computer object. It requires `Idempotency-Key`
(trimmed, 1 to 255 bytes, scoped to the credential, method and path, kept in
`computer_idempotency_records` for the Session's life); a replay answers the
same computer, a changed body `409 idempotency_conflict_error`. Session
creation takes the same optional key and replays the same Session. `GET
/v1/sessions/{session_id}/computers/{computer_id}` answers an `attached`
computer, or a `detached` one still `closing` or `unknown`; otherwise `404`.
Every Session object carries `computers`, its `attached` computers in
addition order, and `environment_id`, the first attached cloud computer's
Environment or `""`. An instance's `computer_count` counts the computers
attached to it.

### Drives

`internal/httpapi/drive_handler.go` serves the developer drive API on
`internal/drive`; `beta` is accepted and never required. The writer of every
version a route creates or redacts is the request's credential: `api_actor`
for an API key, `user_actor` or `service_account_actor` for a direct token.

- Lists take `limit` (default 20, above 100 treated as 100) and `page`, and
  return `{data, next_page}`: drives by creation, files by UTF-8 byte order of
  `path`, Skills by UTF-8 byte order of `name`, a path's versions oldest first. The page token is signed with the API
  page-token secret and binds the Workspace, the list, the drive and its
  filter. `drives.changes.list` takes `after` (whole commits), `session_id` and
  `limit` 1 to 1,000, and returns `{data, has_more}`.
- `drives.files.upload` refuses a declared `Content-Length` above 501,000,000
  bytes with `413` before it takes from the upload budget, then takes the
  budget (`429 rate_limit_error` with `Retry-After` when refused), then streams
  the `file` part into the drive and writes it in one `all_or_nothing` commit.
- `drives.skills.upload` (`internal/httpapi/drive_skill_handler.go`) refuses
  a declared `Content-Length` above 32,505,856 bytes the same way, then takes
  the budget, stages the one part `archive` (at most 30,000,000 bytes) and
  checks it with `internal/drive/skillpackage`, the only place Engine checks
  `SKILL.md`. A valid archive replaces `<skills_path><name>/` in one
  `all_or_nothing` commit. `drives.skills.list` and `drives.skills.retrieve`
  check each Skill's `SKILL.md` when they read it and report a failed rule in
  `error`.
- `drives.changes.stream` is SSE: each pod holds one LISTEN connection on
  `tetral_drive_changed`, opened when the router is built and closed with it;
  it reconnects after a failure, which writes
  `drive_changes.listener_disconnected`. Every event is `event: drive.changed`
  with `{drive_id, seq}`: a stream first sends the drive's `last_seq` when it
  is above 0, then one per commit notice; notices a slow client has not
  received merge into the highest `seq`. After a LISTEN reconnect every stream
  gets its drive's `last_seq` again.
- The upload budget's pressure point exports
  `drive_upload_budget_rejected_total{cause="budget"}` on the metrics listener.

### Files

Files routes require `beta=true`. Lists accept `limit` (1–1000, default 20),
`page` and `scope_id`, and return `{data, next_page}` in ascending storage order.
The signed page token uses the API page-token secret and binds both workspace
and scope. Deleting the last file on a page does not invalidate continuation.
`after_id`, `before_id` and `ids` return 400 `invalid_request_error`.

Uploads without a filename, including an empty filename, use `unnamed` plus
a fixed extension for known stored MIME types; unknown MIME types add no
extension. Path-qualified filenames retain only the basename, and a name with
no last segment (such as `dir/..`) also becomes `unnamed`. Control characters or
invalid UTF-8 anywhere in the supplied name return 400; the 255-character limit
applies to the basename. Expiration
configuration (`expires_in_seconds`) is rejected. Metadata omits `expires_at`.

## States & lifecycle

### Admission gate (every request)

| Step | Rule | Failure |
| --- | --- | --- |
| Principal | Verify the `auth` signed principal's signature, audience, expiry, method, and path. The token is at most 32 KiB of strict JSON, issued no more than five seconds ahead of the receiver clock, with a lifetime of at most five minutes. Workspace authority comes from `Principal.workspace_id` only. | `401 authentication_error` |
| Workspace header | A supplied `anthropic-workspace-id` must have exactly one nonempty value equal to the verified principal workspace. A missing header is allowed; the header never supplies authority. API, Auth API-key routes and Event Stream share this check before operation authorization, with one denial audit event. | `403 permission_error` |
| Operation | Every registered business route, including authenticated stubs, declares a semantic action from `internal/auth`. The common Authorizer checks the verified principal's explicit operation ceiling against its typed workspace before any resource lookup. | `403 permission_error` |
| Request text | Public request text must not contain U+0000, because Engine stores text in PostgreSQL, which cannot store it. After the Operation step, `httpapi.PublicOperationHandler` rejects a decoded path, query parameter name or query value that contains U+0000 or is not valid UTF-8, on every business route of api, auth's `/v1/api_keys` and event-stream (a path that is not valid UTF-8 normally fails the Principal step's path match first, with 401). Each JSON body reader rejects U+0000 in any string or object key. Both checks run before any lookup or write, so a rejected request stores nothing and writes no idempotency receipt. API key `name` and SKILL.md `description` are checked by their owners. Memory store and memory version page tokens are not signed, so their decoder also rejects a position the encoder never writes: an ID with U+0000, or a time that is not a canonical UTC RFC3339 time from year 1 on. File upload content and skill package file bodies may contain U+0000. The official API does not document how it handles U+0000. | `400 invalid_request_error` |
| Selector binding | Client-supplied `workspace_id`, `session_id`, `vault_id`, etc. are selectors, never authority. Every query binds `Principal.workspace_id` plus the route selector. | — |
| Cross-workspace / missing | Never falls back to another workspace, default resource, or default credential. | `404 not_found_error`, or `403 permission_error` where an authenticated-but-forbidden action exists |
| Beta marker | Every `client.beta.*` route requires `beta=true`; omitted, repeated, or altered is rejected. `/v1/models` accepts the marker optionally. | `400 invalid_request_error` |
| Input values | The route's query or body decoder rejects a malformed value before any lookup. A `created_at[...]` list filter must be an RFC3339 timestamp as Go's `time.RFC3339` parses it, the form the SDK sends: uppercase `T` and `Z`, seconds 00–59 with an optional fraction, and `Z` or a `±hh:mm` offset; the memory store, memory version and event lists pass it to PostgreSQL as four-digit-year UTC text, so they also require a UTC year of 1–9999. An Agent version (`version`, `agent_version`, Session `agent.version`) must be an integer from 1 to 2147483647, the positive range of the PostgreSQL `integer` it is stored as. An Environment `name` is at most 256 Unicode code points, the Agent `name` cap, and a credential URL (`auth.mcp_server_url`, `auth.refresh.token_endpoint`) is at most 2048 bytes after canonicalization; these caps keep the Environment name and MCP server URL unique btree index entries within PostgreSQL's size limit. | `400 invalid_request_error` naming the field |

Tenant isolation rests on signed principal binding with `workspace_id` in every
primary key; there is no reliance on connection-level row filtering.

### Session lifecycle_state (durable axis)

| State | Entered by | Public visibility | Exit |
| --- | --- | --- | --- |
| `admitted` | reserved | — | → `active`. Validated but unused in production; new rows are created `active`. |
| `active` | create | visible | archive → `archiving`; delete → `deleted` |
| `archiving` | archive admitted | visible | → `archived` (async), stamps `archived_at` |
| `archived` | archive settled | visible | terminal |
| `deleted` | delete admitted (synchronous tombstone) | `404` | domain-owned GC |

There is no `deleting` state — delete tombstones synchronously and enqueues its
cleanup atomically. `lifecycle_state` is one of three distinct durable axes;
mutations also read the projected Session status, which combines stored
`sessions.status` (`idle`, `rescheduling`, or `terminated`) with the live
`session_runtime_status.status` row (`running` while any thread of any role is
`running` or `rescheduling`, `idle` when none is; written only by
`runtimecontrol.RefreshSessionStatusTx`). A session mutation — `update`,
resource add, resource delete, computer addition, archive — requires the
projected status to equal `idle` exactly, read under the Session's runtime
mutation lock: any non-`idle` status (`running`,
`rescheduling`, or `terminated`) is rejected, and so is a `lifecycle_state` of
`archiving`. A non-idle or archiving target returns
`409 invalid_request_error`. Session
delete is likewise refused while the runtime status is `running` or
`rescheduling`.

### Session field mutability

| Field | Rule |
| --- | --- |
| `agent` version | canonicalized and pinned to an immutable version at create |
| `vault_ids` | create-time-only; must be supplied explicitly (empty list allowed); any update payload carrying it is `400 "field is immutable"` |
| `providers` (credential selector) | create-time-only; any update payload carrying it — an entry, `{}`, anything — is `400`; only an omitted field passes. Fixes the session's model-supply route for life; changing it means a new session. Credential *repair* (rotating the same `credential_id` through the Vault update path) is unaffected. Omitted or `{}` means platform-hosted provider access (no credential row). |
| `title`, `metadata`, runtime-visible `agent.tools` / `agent.mcp_servers` | mutable only when the session locks `idle` |
| `github_repository` `authorization_token` | rotatable in any run state — a pure metadata write, encrypted at rest, never echoed; rotation never touches `git_identity` |
| `github_repository` `git_identity` | create-time-only per resource, immutable for the Session, returned unredacted; it is commit authorship configuration, not authentication material |

When `providers` names a credential at create, admission validates all six of:
(1) exactly one provider entry is present; (2) the provider key equals the Agent
snapshot's pinned `provider_id`; (3) the `credential_id` exists, belongs to the
authenticated workspace, and is live; (4) `credential.auth.type` is
`provider_api_key` or `provider_oauth`; (5) `credential.auth.provider_id` matches
the provider key; (6) `credential.vault_id` is in the session's immutable
`vault_ids`. If `providers` names a credential and the session has no bound
`vault_ids`, the request is rejected. These gates block cross-workspace and
mismatched-provider credential use; credential material never enters the sandbox.

The public `model` field takes the `provider/model` form (a shorthand string or
`{ id, speed? }`); providerless IDs are rejected at admission. The engine is the
sole model gatekeeper — admission rejects any ID outside the supported set with
a message enumerating the allowed set: `openai/gpt-5.5`, `openai/gpt-5.6-sol`,
`anthropic/claude-opus-4-8`, `anthropic/claude-fable-5`,
`deepseek/deepseek-v4-pro`, `moonshotai/kimi-k3`, `zai/glm-5.2`. `speed` accepts
only `standard`; `speed = fast` is `400`. `multiagent` accepts only omitted or
`null`. `deployment_id` is always `null`. `budget` is always `null`; budget
requests remain unsupported.

### Thread lifecycle

Public Session Thread objects include `workflow_run_id: null`; Engine does not
execute workflow runs.

Public thread APIs operate on threads with `visibility = public`; internal
approval-reviewer threads are never returned. Thread status is one of `running`,
`idle`, `rescheduling`, `terminated`.

| Archive target | Result |
| --- | --- |
| `running` / `rescheduling`, or an active pending runtime input, or an unresolved external wait | `409 invalid_request_error` |
| primary thread (`sessions.main_thread_id`) while session runtime status ≠ `idle` | `409 invalid_request_error` |
| already archived | idempotent — returns the current archived thread DTO |
| missing or invisible | `404 not_found_error` |

Once the primary thread is archived its input lane is closed: `user.message`
admission targeting an archived `main_thread_id` is rejected at the session
admission fence.

### Events admission (`internal/sessionevent`)

`POST /v1/sessions/{session_id}/events` is the write half of the public Events
API, served here. The read half — the `GET` list snapshots and the two SSE
streams — is documented by the **event-stream** service; see
`services/event-stream/README.md` (the list endpoints are compiled into this
service through the shared `internal/eventstream` reader, the SSE streams into
the event-stream binary, as the *Event boundary* seam below records).
The route accepts the SDK batch shape `{events: […]}` and returns `{data:
[Event…]}` with the flattened fork-SDK event union (`type`, `id`,
`processed_at`, and the variant's fields at top level). Admission validates the
whole batch before any row is written; it runs behind the session admission
fence and writes any `queue_jobs` in the same transaction (see the *Admission
fence + same-transaction enqueue* seam). The input-event union and per-type
rejections are pinned by the `T-COMPAT-EVIN-*` cases in the forked SDK
repository's compatibility registry; this README does not restate that matrix.

| Input `type` | Admission in this stage |
| --- | --- |
| `user.message` | Supported. Writes `session_events` and `queue_jobs(kind = runtime_input, input_kind = messages)` in the same transaction. Targets `sessions.main_thread_id`. |
| `user.interrupt` | Supported. Writes `input_kind = interrupt_control` work in the same transaction. Target resolution below. |
| `user.tool_confirmation` | Supported only when it resolves a current pending approval row; `input_kind = tool_confirmation`. |
| `user.custom_tool_result` | `400 invalid_request_error` "custom tools are not supported in this stage". |
| any other type (includes `user.tool_result`, `user.define_outcome`, `system.message`) | `400 invalid_request_error` "event type must be user.message, user.interrupt, or user.tool_confirmation". |

A rejected input writes no `session_events` row, pending-approval row, or queue
job.

`user.message` content is the SDK block array. Admitted block types are `text`,
`image`, and `document`; the media types admit only a file reference:

| Check | Rule / failure |
| --- | --- |
| Block type | `text`, `image`, `document`; any other → `400` "content block type must be text, image, or document". |
| `image` / `document` source | Sole admitted source is `{type: "file", file_id}`; a `base64` or `url` source → `400` "<source type> source is not supported; upload the bytes via /v1/files and reference the file_id"; a blank `file_id` → `400`. |
| File existence / MIME / size / pages | Validated in the same admission transaction by `files.ValidateEventAttachments` — workspace-scoped existence, MIME allow-list, per-file image cap, and the per-request PDF byte/page aggregates. The caps live in the File domain section's user.message media table below; this section does not restate them. |
| Attachment count | ≤ `sessionevent.MaxProviderRequestAttachments` (32) file-backed references across the batch → `400` "events request exceeds the file attachment limit". A file referenced twice counts twice (fails safe). |
| Batch size | ≤ `TETRAL_SESSION_EVENT_MAX_EVENTS_PER_REQUEST` (default 32) events → `400` "events exceeds the per-request limit". |
| Body bytes | ≤ `TETRAL_SESSION_EVENT_BODY_BYTE_CAP` (default 1 MiB) → `413 invalid_request_error` "request body too large". |

Both admission caps are startup env config validated positive
(`services/api/cmd/tetral-api`; zero or negative fails closed before the
process serves). Admitted blocks are stored verbatim and echoed with `file_id`
preserved.

Batch order is preserved in per-thread `session_events.sequence` and the
session-global `insert_stream_position`, both allocated in batch order inside
the admission transaction; if any event is invalid the whole request fails and
no partial rows or queue jobs commit.

Target resolution:

| Event | Target |
| --- | --- |
| `user.message` | `sessions.main_thread_id`. An archived primary thread is rejected at the session fence (Thread lifecycle above). |
| `user.interrupt` with `session_thread_id` | that one thread, validated `visibility = 'public'` (`resolveInterruptTargetThreadID`). |
| `user.interrupt` without `session_thread_id` | `sessions.main_thread_id`; one Stop creates one main-Thread interrupt custody and fences only that Thread's delivery and mutation. |
| `user.tool_confirmation` | `session_thread_id` resolved from the referenced pending approval row — never client-chosen. |

`Idempotency-Key` law:

| Header state | Behavior |
| --- | --- |
| omitted | admission reads and writes no idempotency receipt; every such request is a new admission with no cross-retry deduplication. |
| supplied | must appear at most once, be non-blank, and be ≤ 255 bytes; a violation is `400 invalid_request_error`. |
| replay — same key, identical canonical request, receipt live | `200` with the stored admitted events; no new rows or queue jobs; the receipt's timestamps do not move. |
| same key, different canonical request, receipt live | `409 invalid_request_error`. |
| same key, receipt expired | an ordinary new admission that replaces the receipt in the same transaction. |

A supplied key's receipt is locked `FOR UPDATE` after the Session fences, and
only then is the database clock read, in a separate statement, as the
admission time. The receipt is live precisely while admission time <
`created_at + 24 hours`, whether or not Cleanup has deleted it yet. An expired
receipt is deleted and the new receipt is written with `created_at =
updated_at =` that admission time; a failed admission rolls both back. Cleanup
deletes receipts older than 24 hours in bounded batches; when it holds a
receipt first, admission waits and then finds none, and when admission holds
it first, Cleanup skips it (`services/cleanup/README.md`).

`session_event_idempotency_keys` stores the key's SHA-256 digest, never the
raw key; a separate `canonical_request_hash` over the decoded batch (order,
per-event thread selector, tool-use references, types, payloads); and the
complete admitted response events — ID, Thread, sequence, type, payload and
creation time — that a replay returns. The selector
in that hash is the interrupt *intent* (`session`, `thread:<id>`,
`tool_use:<id>`, or `primary`), never a resolved database identifier —
idempotency lookup precedes main-thread resolution, so a bare interrupt replays identically
regardless of threads added or archived between attempts. The idempotency row
is written in the same transaction as the events, after the session fence, so a
crash before commit leaves no reservation and a retry is a fresh admission.

Events API errors use the shared SDK envelope:

| Situation | HTTP status and error type |
| --- | --- |
| Unsupported event type, invalid batch shape, invalid content block or non-file media source, invalid file reference, over-count batch, or a Cloud-rejected `user.tool_result` / `user.custom_tool_result` | `400 invalid_request_error` |
| Missing or invisible session, thread, or referenced event | `404 not_found_error` |
| Pending approval missing, expired, already `resolving`, or the thread blocked by an unresolved external wait | `409 invalid_request_error` |
| `Idempotency-Key` reused with a different canonical request | `409 invalid_request_error` |
| Request body over `TETRAL_SESSION_EVENT_BODY_BYTE_CAP` | `413 invalid_request_error` |
| Admission transaction, `session_events` write, or same-transaction `queue_jobs` write fails | `5xx api_error` |

### Resource lifecycle (session-scoped)

Resources are public session-scoped objects; bytes live in object storage, and
metadata lives in Postgres. Types are `file`, `github_repository`, and
`memory_store`.

| Route | Behavior |
| --- | --- |
| `POST /resources` | Add a `file` resource (a `github_repository` → `400`; repositories come with their computer) referencing an already-uploaded file plus optional `mount_path`. The session must be `idle`. The transaction increments the resource revision; when a current Sandbox handle exists it also enqueues materialization, otherwise the first approved Sandbox tool materializes the latest revision during lazy activation. Non-idle or archiving sessions return `409`. |
| `POST /resources/{id}` | `github_repository` token rotation only — write-only `{authorization_token}`, encrypted at rest, returned redacted; admitted in any run state. A `file` or `memory_store` resource carries no mutable field → `400`. |
| `DELETE /resources/{id}` | Tombstone/detach for `file` and `memory_store` resources (a `github_repository` → `400`). The session must be `idle`. A resource with no materialized Sandbox handle detaches immediately; otherwise the row becomes deletion-pending and the same transaction advances the resource revision and enqueues materialization. Non-idle or archiving sessions return `409`. |

`memory_store` resources are attached explicitly by ID at session create — never
silently defaulted and never added through `POST /resources`. A declared `file`
resource materializes read-only at its resolved `mount_path`, or at the default
`/mnt/session/uploads/<session_file_id>`.

A `github_repository` resource admits `url`, `authorization_token`, optional
`mount_path`, optional `checkout`, and optional `git_identity`
(`{name, email}`, both required when the object is present). `git_identity`
declares the default Git author and committer for that mounted repository only:
admission requires nonempty UTF-8 values, at most 256 bytes for `name` and 254
bytes for `email` (defined by `gitidentity.MaxNameBytes` and
`gitidentity.MaxEmailBytes` in [the shared validator](../../internal/gitidentity/validation.go)).
It rejects controls, format characters, angle brackets,
leading/trailing name whitespace, all email whitespace, and leading/trailing
`,`, `:`, `;`, double quotes, single quotes, or backslashes that Git would trim.
Email requires exactly one `@` with nonempty parts on both sides. Unicode names,
internal punctuation, and periods are preserved; values are never silently
sanitized. Invalid values fail before any Session or Sandbox exists. The
identity is persisted with the resource, returned unredacted from Session and
resource reads, and cannot be changed afterward. It grants no
repository access — `authorization_token` remains the sole clone/push
credential. An omitted `git_identity` keeps the session-scoped platform
identity (`Tetral Agent <session+<session_id>@agents.tetral.ai>`).

### Vault / Credential / Memory store / Environment archive

Each of these surfaces carries the durable soft-archive axis: `POST .../archive`
stamps `archived_at`, and list endpoints toggle visibility with
`include_archived`. Delete tombstones the row. Credentials additionally support
`.../mcp_oauth_validate` as a bounded read-only diagnostic; a credential still
referenced by any un-GC'd session cannot be deleted (mapped
`409 invalid_request_error`, never a raw constraint 500).

### File domain (`internal/files`)

Public `/v1/files` uploads store bytes in `internal/blob` (BlobStore) and metadata
in PostgreSQL. The same domain serves two internal admission roles: session-file
identities and user.message media attachments.

| Surface | Rule |
| --- | --- |
| Upload (`POST /v1/files`) | Per-file cap 500 MB (`files.MaxFileBytes`); the multipart route cap is 501 MB (`files.RouteMultipartBytes` — one file plus a 1 MB envelope). Over cap → `request_too_large`. Filename is metadata only; bytes are content-addressed by SHA-256, and identical bytes share one `file_objects` row. |
| PDF page count | Cached tri-state on `file_objects.pdf_page_count`: `NULL` (non-PDF or legacy), `-1` (uncountable — corrupt or encrypted), `>= 0` (counted). Counting is confined to `internal/files/pdfcount`. |
| Session-file identity | An uploaded file linked to a session materializes read-only at its `mount_path` (or `/mnt/session/uploads/<session_file_id>`); the identity is created/tombstoned inside the session admission transaction (`CreateSessionFileIdentity` / `TombstoneSessionFileIdentity`) and shares the source object without re-uploading bytes. The storage-internal `ObjectKey` never appears in a public DTO. |

user.message media admission runs in `PostgreSQLFileStore.ValidateEventAttachments`,
called by `internal/sessionevent` inside the event-admission transaction:

| Block type | MIME allow-list | Bound |
| --- | --- | --- |
| `image` | `image/jpeg`, `image/png`, `image/gif`, `image/webp` | per-file ≤ 10 MB (`files.MaxEventImageBytes`); image sizes are never summed |
| `document` | `application/pdf`, `text/plain` | PDF: per-request aggregate ≤ 32 MB (`files.MaxEventPDFBytesPerRequest`) and ≤ 600 pages (`files.MaxEventPDFPagesPerRequest`). `text/plain` carries no admission byte cap. |

One events request carries at most 32 file-backed media references across its
events (`sessionevent.MaxProviderRequestAttachments`, enforced in
`internal/sessionevent`) — a hard `400` at admission, never a silent later drop. A file referenced twice contributes its bytes and pages
twice — a duplicate can push a request over a cap but never under it (fails safe),
and a duplicate reference also counts twice toward the attachment-count cap. A
legacy PDF whose page
count is not yet cached raises `EventAttachmentPageCountRequiredError`; the domain
primes the immutable count once (`PrimeEventAttachmentPDFCounts`, committed
independently of admission so the cache survives a later rejection) and the caller
retries.

### Skill packages (`internal/skill`)

The beta-gated `/v1/skills` family stores each skill version as a deterministic
normalized zip in BlobStore; the version's `name`/`description` come from a root
`SKILL.md` frontmatter. Ingestion is bounded at every stage
(`internal/skill/package.go`):

| Bound | Value |
| --- | --- |
| Request-wide upload bytes | 30 MB (`MaxUploadFileBytes`) |
| Upload parts / package entries | 1000 (`MaxUploadFileParts`, `MaxFileCount`) |
| Per-entry bytes | 10 MiB (`MaxPackageEntryBytes`) |
| Expanded package bytes | 200 MiB (`MaxPackageExpandedBytes`) |
| Normalized zip bytes | 31 MiB (`MaxNormalizedZipBytes`) |
| Display name | ≤ 255 runes, one line, no control characters (`maxDisplayNameRunes`) |

A root `SKILL.md` is required. Only the bounded segment between the opening and
closing `---` delimiters reaches the YAML parser, capped at 32 KiB
(`MaxFrontmatterBytes`); it decodes strictly into exactly two fields — `name`
(≤ 64 runes, `MaxNameRunes`) and `description` (≤ 1024 runes,
`MaxDescriptionRunes`). Anchors, aliases, explicit tags, merge keys, extra keys,
non-scalar values, and multi-document streams are rejected. A `description`
containing U+0000, which a YAML `\0` escape can produce, is rejected with `400`
because Engine stores it in PostgreSQL, which cannot store U+0000. The
`gopkg.in/yaml.v3` parser is confined to this package
(`internal/skill/dependency_test.go`).

Skill responses expose `display_name`, `latest_version_id`, and
`source: {type: "custom"}`. Multipart creation accepts `display_name`; omission
uses the frontmatter `name`. `display_title` is rejected. Every
version of a skill must keep its first version's frontmatter name. Versions
use `skver_` IDs for both storage version fields and public addressing. Version
responses omit `version` and `directory`; lists return `next_page` without
`has_more`. Only version GET accepts `latest`; content and deletion require a
concrete version ID.

Session creation resolves Agent Skill references, including `latest`, under the
shared workspace Skill registry lock in the same transaction that saves the
Session, so Session creations do not wait for each other but never interleave
with a Skill upload or deletion.
The concrete references persist in `sessions.resolved_skills_json` (`[]` without
skills). Session and Thread responses, Sandbox materialization, and Bridge
Runtime configuration all use these saved references. Agent configuration keeps
`latest` for future Sessions. Uploading a version or deleting a version or its
parent never changes an existing Session's package content.

Deleting a skill atomically soft-deletes all its versions without checking Agent
references. Existing Sessions retain access to their saved packages; new Agent
writes and Session creation reject deleted references. Reading a historical
Agent remains valid. Deleting the last version alone returns 400; deleting the
latest of several versions selects the previous active version for future
Sessions.

### Memory store durable domain (`internal/memory`)

PostgreSQL is the source of truth for Memory Stores. The store is projected
**read-only** into the sandbox at `/mnt/memory`; that projection is disposable —
repaired by re-materialization, never trusted. Sandbox Service refreshes the
projection and executes the model-facing memory tool; Bridge records its
refs-only request and consumes the durable result. This service
owns the durable rows: `memory_stores`, `memories` (current head per path, no
content), and `memory_versions` (immutable content, reached from
`memories.current_version_id`).

| Rule | Behavior |
| --- | --- |
| Content bound | ≤ 102400 bytes at create and update (`memoryContentMaxBytes`) → `request_too_large`. |
| Path validation | `ValidatePath`: leading `/`, non-empty NFC segments, no `.`/`..`, no control/format characters. The `/mnt/memory` prefix is rejected — it is the read-only projection path, not a writable memory path; memories change only through the store id, never by writing files under the projection. |
| Path conflict | Creating or repathing to a path that equals, is an ancestor of, or is a descendant of an active head is a `PathConflictError` → `409 invalid_request_error`, carrying the conflicting head. The existence check is exact SQL, not `LIKE`, so literal `%`/`_` in paths never false-positive. |
| Optimistic concurrency | Update and delete may carry a content-hash precondition (`expected_content_sha256` / `MemoryPrecondition`); a mismatch is a `PreconditionFailedError` → `409 invalid_request_error`. |
| Version redaction | `memory_versions` are redactable, but redacting the current live head is refused — an active memory always keeps non-NULL content. |

Memory store responses always carry a string `name`. Updating a store with
`name: null` returns 400 `invalid_request_error`; omission preserves its name.

Memory version actors describe the credential that performed each write and
redaction. Independent and identity-derived API keys use `api_actor` with the
actual `api_key_id`; direct human tokens use `user_actor` with `user_id`; direct
service tokens use `service_account_actor` with `service_account_id`. Human/service IDs are stable
Engine identity IDs, not Anthropic `svac_` IDs; PostgreSQL still stores the
`service_actor` type. Runtime writes retain `session_actor` with `session_id`.
Exactly one actor ID is valid for its discriminator in both the model and
PostgreSQL constraints. Reads require a verified authorized identity and do
not require an API-key ID. The SDK version pinned by this repository types all
four actor variants, including `service_account_actor`; see
[authentication](../../docs/authentication.md#verification) for the typed SDK
verification.

The store's soft-archive axis and its create-time-only attachment (a
`memory_store` resource is attached by ID at session create, never defaulted and
never added through `POST /resources`) are covered above.

## Seams

### Signed-principal boundary (owner: auth)

- **Contract.** Requests reach this service only with a valid
  `X-Tetral-Internal-Principal` injected by the edge. This service verifies the
  signature, audience (`tetral-public-api`), expiry, method, and path, then
  attaches workspace, typed identity, credential and authority to the request
  context. API-key credentials carry their actual key ID; direct human and
  service tokens carry stable Engine identity IDs. Raw public API keys and
  Bearer tokens never reach this service.
- **Operation and resource ownership.** Actual router registrations declare
  semantic actions and use the common Authorizer. A denied operation does not
  query resources. Allowed collection/create actions use the verified typed
  workspace. Individual reads authorize their canonical service result before
  disclosure; nested lists authorize a parent proven by the successful
  tenant-scoped list. Mutations resolve the target's trusted identity before
  effects, then recheck eligibility in their own transactions; the lookup
  takes no lock.
  Session-family mutations resolve identity only: one read-only SELECT of the
  canonical ID, without assembling the public object, usage, agent version or
  provider authentication. Update, archive, event admission and resource
  attachment accept any Session that is not deleted, so an archived or
  archiving Session reaches its own archive or conflict result. Thread archive
  accepts a public, non-reviewer Thread of that Session, archived or not.
  Resource update accepts an attached Resource of that Session whose deletion
  has not been requested; a file Resource also needs its live Session-scoped
  file. A target outside these rules, including another workspace's row, is
  `404` before any effect.
  Session creation and resource attachment retain their service-owned checks
  of all referenced agents, environments, vaults, files and memory stores.
  Deletion lookups retain already-deleted Session rows and pending resource
  deletion facts so authorization preserves the existing idempotent Session
  delete and in-progress resource conflict behavior.
  `workspace_full_access` is the only assignable role. Future resource policy
  filtering and pagination semantics are not implemented by this boundary.
- **Invariant a replacement must preserve.** No public handler may read identity
  from the request body or from client-supplied `X-Tetral-*` headers. Workspace
  authority is the verified principal alone.
- **Conformance.** `internal/httpapi/router_test.go` and the SDK-compat suites
  exercise the authenticated path; `auth` owns the principal-minting
  contract.

### SDK-compatibility surface

- **Models.** List and retrieve return `lifecycle: "active"`,
  `deprecated_at: null` and `retires_at: null`. Lifecycle means the Engine
  currently provides the model, not the provider's retirement schedule.
  `line` is `"opus"` for `anthropic/claude-opus-4-8`, `"fable"` for
  `anthropic/claude-fable-5`, and `null` for the other catalog entries.
- **MCP OAuth validation.** The refresh result's `http_response` is present
  only when `status` is `failed`. It then carries the token endpoint's status
  code and truncation flag, while `body` and `content_type` are always empty
  strings; the official API returns a scrubbed body instead. It is explicitly
  `null` for `succeeded`, `connect_error` and `no_refresh_token`. As in the
  official API, `mcp_probe` is `null` when the probe succeeds, and `refresh` is
  `null` unless a token exchange ran or the probe got a 401 with no stored
  refresh token (`no_refresh_token`). Engine refreshes before probing when the
  stored access token is expired or has no valid expiry time, and does not
  refresh after a 401; the official API refreshes after a 401.
- **Contract.** Errors use the SDK envelope
  `{"type":"error","error":{"type","message"},"request_id"}`, with the HTTP
  status chosen independently of the error `type`. The error-type set is exactly
  what `internal/httpapi.classifyError` emits — eight types:
  `invalid_request_error`, `request_too_large`, `authentication_error`,
  `permission_error`, `not_found_error`, `conflict_error`, `not_implemented`,
  `api_error`. Two pagination families
  exist and a route keeps its SDK family: the `Page` shape
  (`has_more`/`first_id`/`last_id`, plain id cursors) and the `PageCursor` shape
  (`next_page`/`page`, signed opaque cursors bound to workspace, filters,
  ordering, and view). An empty `page`, which is how the SDK sends a null
  cursor, asks for the first page; any other empty query value returns 400.
- **Invariant.** Unsupported SDK *routes* (`/v1/messages`, user profiles,
  webhooks, deployments and deployment runs, self-host environment work APIs) are
  registered explicitly and answer `400 "unsupported SDK surface for this stage"` — a closed
  door, never an inert row that leaks partial behavior. The `multiagent` *field*
  is a different case (it is not a route): the create/update param accepts only
  omitted or `null`; a non-null value is rejected `400 invalid_request_error
  "multiagent must be null when provided"`; and the response `agent.multiagent`
  is always `null`.
- **Conformance.** `internal/session/sdk_compatibility_test.go`,
  `internal/vault/sdk_compatibility_test.go`,
  `internal/environment/sdk_compatibility_test.go`,
  `internal/memory/sdk_compatibility_test.go`, and
  `internal/httpapi/sdk_compatibility_session_test.go` pin the wire shapes;
  `internal/session/pagination_test.go` pins the cursor binding.

### Admission fence + same-transaction enqueue (owner: this service)

Event admission also reads the shared child-close fence. A tool confirmation
targets its pending Tool Use's thread, so a child confirmation can reach the
committed-close branch even though ordinary user messages target Main. The API
role needs `session_bridge_operations SELECT` to inspect the close receipt;
Bridge remains its writer. Without a receipt or terminal source Tool Result,
the close fence rejects admission without persisting an event or Queue job.

- **Contract.** Work leaves this service only as durable facts. The recurring
  pattern is one PostgreSQL transaction containing both the business rows and the
  `queue_jobs` rows: an admitted `user.message` writes its events and the
  `runtime_input` job together; environment admission enqueues
  `environment_build`; Session delete writes the tombstone,
  cleanup identity, and Sandbox release request atomically.
- **Invariant.** Runtime input never depends on Sandbox readiness. Session create
  records the Environment and resource declarations but does not allocate or
  inspect a provider resource. Lazy activation begins only after an approved
  Sandbox Tool Use has durable custody.
  The production path is
  `internal/httpapi.SessionHandler -> internal/session.Service ->
  internal/session.PostgreSQLSessionStore -> internal/dbconnect.Client`; the HTTP
  layer must not wire a session manager into `/v1/sessions`.
- **Conformance.** `internal/session/postgresql_transaction_proof_test.go` and
  `internal/session/postgresql_store_controlplane_test.go`.

### Master-key encryption of write-only secrets

- **Contract.** This service is one of the platform master-key holders. The
  primitive is AES-256-GCM with random nonces
  (`internal/encryption.AES256GCMEncryptor`, aliased as `internal/vault.Encryptor`
  and constructed from a hex-encoded 256-bit master key). It encrypts inbound
  secret material on the way in — vault credential secrets and the
  `github_repository` `authorization_token` — and decrypts only where a read or
  update path requires plaintext (presence flags, OAuth rotation, the bounded
  diagnostic). Secret fields are write-only in and redacted out; refreshed OAuth
  material re-enters through the same credential update path.
- **Invariant a replacement must preserve.** Secret material never appears in any
  response or log. The redacted response keeps routing metadata only (`type`,
  `mcp_server_url`, non-secret `refresh` fields) and omits every secret
  (`access_token`, `refresh_token`, `client_secret`, `token`).
- **Conformance.** `internal/vault/encryption_boundary_test.go`,
  `internal/vault/validation_security_test.go`,
  `internal/encryption/aesgcm_test.go`.

### Media attachment admission (owner: this service, `internal/files`)

- **Contract.** `ValidateEventAttachments` is the single gate for user.message
  media blocks. It runs inside the caller's admission transaction, validates each
  reference against the MIME allow-list and the byte/page bounds above, and
  returns a typed `ValidationError` (mapped `invalid_request_error`) or
  `RequestTooLargeError`. A replacement must keep admission byte/page enforcement
  in the same transaction as event commit so a rejected batch commits nothing.
- **Invariant a replacement must preserve.** Image caps are per-file; PDF byte and
  page caps are per-request aggregates summed over document references as written
  (duplicate references fail safe). Legacy PDF page counts are primed exactly once
  into an immutable cache column and never recomputed thereafter.
- **Conformance.** `internal/files/postgresql_store_internal_test.go` (lock order,
  once-only priming), `internal/sessionevent/service_test.go` (batch limits,
  lazy legacy-PDF counting, upload-first rejection of non-file media sources).

### Environment admission

- **Contract.** The Environment API is Cloud-only: admission requires
  `config.type = cloud` and rejects any other config type with `400`. When
  `config` is omitted on create, the API creates a Cloud environment with
  `networking.type = unrestricted` and empty `packages` (the canonical default).
- **Packages.** `config.packages` carries per-manager arrays over a closed
  manager set — `apt`, `cargo`, `gem`, `go`, `npm`, `pip`. `packages` is the
  sole artifact input: a packages change yields a new `artifact_input_hash` and
  enqueues an `environment_build` job for the provider artifact. A session stores
  its selected Environment generation; the first approved Sandbox tool waits for
  that generation's ready provider artifact while lazily activating its Sandbox.
  That first tool may therefore include provider inspection and activation
  latency, or return a typed tool failure when the artifact or provider resource
  cannot be made usable. Provider build progress is observed asynchronously
  without consuming failure retries. Sandbox persists a warning threshold
  (default 10 minutes) and hard build-wait deadline (default 30 minutes), then
  settles waiting calls with a distinct Engine timeout if necessary. Restart
  does not reset those times; old failed artifacts are not automatically
  recovered. See the Sandbox service's Environment builds contract.
- **Networking.** Networking accepts exactly the Daytona-backed shape —
  `unrestricted`, `blocked`, or `cidr_allow_list` with a comma-separated CIDR
  `network_allow_list`. Domain-name allow-lists are rejected; the backend never
  resolves domains into CIDRs on the caller's behalf. The selected policy is
  applied when a Sandbox is created for an Environment generation. Updating an
  Environment affects the next Sandbox created from that generation; it does
  not mutate a running Sandbox.
- **Operator prerequisite.** The current Daytona-backed deployment requires a
  Daytona Tier 3 or higher account.
- **Invariant.** Durable rows using retired networking shapes are a rollout
  blocker — a blocking preflight fails closed before serving until an operator
  remediates each row.
- **Conformance.** `internal/environment/input_test.go`,
  `internal/environment/networking_preflight.go` and its tests.

### Event boundary (owner: event-stream)

- The inbound admission `POST /v1/sessions/{session_id}/events`
  (`sessionEventHandler.appendClientEvents`) and the two GET event-list surfaces
  are served here through `internal/eventstream`'s list handler, because they are
  part of this service's admission and queue partnership. The SSE stream routes
  and the public Events API contract belong to the **event-stream** service.
  Inbound admission is batch-atomic (the whole batch validates before any row
  commits), preserves per-thread and session-global order, and dedupes retries by
  optional `Idempotency-Key`.

### Outbound boundary

- This service never calls Runtime Pod, Bridge, Gateway, or the Sandbox Service —
  it holds no gRPC clients to any of them. The queue and the database are its
  only outbound facts. It returns after durable admission, never after execution;
  Sandbox lifecycle work and Environment builds are asynchronous by design.

## Testing guide

| Suite | Proves |
| --- | --- |
| `internal/httpapi/router_test.go`, `internal/httpapi/session_handler_test.go` | routing, principal-gated admission, per-route status/error mapping |
| `internal/httpapi/sdk_compatibility_session_test.go`, `.../sdk_compatibility_session_integration_test.go` | the public Session wire shape matches the SDK |
| `internal/session/sdk_compatibility_test.go` | Session DTO/field/immutability rules (`vault_ids` and `providers` create-time-only) |
| `internal/session/postgresql_transaction_proof_test.go` | business rows and `queue_jobs` commit in one transaction; the admission fence holds |
| `internal/session/pagination_test.go`, `.../postgresql_store_pagination_filters_test.go`, `internal/httpapi/session_page_token_integration_test.go` | signed `PageCursor` binding and list filters |
| `internal/vault/sdk_compatibility_test.go`, `internal/vault/encryption_boundary_test.go`, `internal/vault/validation_security_test.go` | credential wire shape, write-only secret redaction, encryption at rest |
| `internal/environment/sdk_compatibility_test.go`, `internal/environment/input_test.go` | Cloud-only Environment admission and CIDR-only networking |
| `internal/memory/sdk_compatibility_test.go`, `internal/memory/memory_lifecycle_test.go`, `internal/memory/memory_conflict_precedence_test.go` | Memory Stores public surface, content bound, exact-and-prefix path conflict, precondition semantics |
| `internal/files/postgresql_store_test.go`, `internal/files/postgresql_store_internal_test.go`, `internal/files/staging_test.go`, `internal/httpapi/file_handler_test.go` | upload caps and quotas, PDF page-count tri-state cache, attachment admission lock order, session-file identities, `/v1/files` routing |
| `internal/skill/frontmatter_test.go`, `internal/skill/package_test.go`, `internal/skill/dependency_test.go`, `internal/httpapi/skill_handler_test.go` | SKILL.md frontmatter rules, normalized-package byte caps, YAML confinement, `/v1/skills` beta-gated routing |
| `internal/drive/commit_test.go`, `.../redact_test.go`, `.../retention_test.go`, `.../upload_budget_test.go`, `internal/httpapi/drive_handler_test.go` | the developer drive API: commits and path rules, version reads and redaction, restore, the change list and its stream, retention and the upload budget |
| `internal/httpapi/drive_skill_handler_test.go` | Skills published from a drive: archive rules, entries and republishing |
| `internal/httpapi/session_computer_integration_test.go` | a Session's computers: the entry checks, additions only to an idle Session, the computers at creation, fixed repositories, one `session.prepare` for an online user computer and the `session.prepare` size limit |
| `internal/httpapi/session_event_handler_test.go`, `internal/sessionevent/service_test.go`, `internal/sessionevent/idempotency_retention_test.go` | batch-atomic event admission, per-type rejection and file-source ladder, targeted interrupt custody, idempotency-key replay/conflict, no receipt access without a key, the exact 24-hour expiry at the post-lock database admission time, rollback of a replacement, and replacement versus Cleanup pruning in either order |
| `services/api/production_wiring_static_test.go`, `.../startup_config_error_test.go`, `.../startup_config_surface_test.go` | the assembled process wires the real domain services and fails closed on invalid startup config |
| `services/api/tetralapi_test.go`, `.../startup_test.go` | end-to-end service bootstrap |

If a PR changes the admission fence, a route family's public surface, the
pagination or error contracts, the create-time-only field rules, the
same-transaction enqueue pattern, the upload/attachment caps, the SKILL.md
ingestion rules, or the memory content-bound/path-conflict/precondition rules in
this folder, it updates the matching section here.

## Process diagnostics

The command follows the shared [Go process diagnostic contract](../../internal/workload/README.md#diagnostics)
for the restart-only `TETRAL_LOG_*` controls, the default Info level, bounded
suppression summaries, diagnostic drop and sink-failure metrics, and the
diagnostic close after listeners and business resources.

HTTP boundary records (`http.request.completed` for every request and
`http.request.failed` for a 5xx) and authorization
decision records carry `request.id`, the API's own `req_` ID that is also the
`request-id` response header and error `request_id`. After the Auth-signed
principal verifies, they also carry its signed request ID as `edge.request.id`,
which equals the `request.id` of the Auth Check record for the same edge
request. An incoming `X-Request-Id` header never supplies either value.

The production application closes its database and object-client trust observers
after HTTP requests join. A router returned by `BuildRouter` implements
`io.Closer` for object clients it created; explicitly supplied test clients remain
caller-owned. Construction failure closes any newly created object client.

## Public backend TLS

`TETRAL_HTTP_TRANSPORT` defaults to `plaintext` for the standard routed profile.
The hardened profile selects `native-mtls` on the existing business listener and
requires `TETRAL_HTTP_TLS_CA_PATH`, `TETRAL_HTTP_TLS_CERT_PATH`,
`TETRAL_HTTP_TLS_KEY_PATH` and the exact `TETRAL_HTTP_TLS_EDGE_CLIENT_URI`.
The server verifies the edge client certificate and full role URI before HTTP
admission; Envoy verifies this service's DNS/server identity independently.
There is no additional plaintext business listener. Health/metrics use their
separate restricted listener.

Mount the complete CA/certificate/key directories read-only without `subPath`.
The shared validated loader activates valid generations for fresh TLS handshakes,
preserves established requests during leaf renewal, and retains valid last-known-good
material after malformed replacement. Its observer closes only after the public
listener and admitted requests join. CA replacement follows the
[native CA rotation order](../../deploy/cert-manager/README.md); at this
receiver, old connections drain through the service's bounded drain/join and
restart, because removing trust alone does not retire an established connection.
The [deployment guide](../../deploy/helm/tetral/README.md) owns Secret
projections, native role issuance and profile prerequisites.
