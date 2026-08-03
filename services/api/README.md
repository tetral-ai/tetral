# api

## Responsibilities

The request/response half of the platform's public API surface: every SDK
route that answers immediately terminates here. The service validates the
request, writes durable rows, and returns — it executes nothing. Every request
arrives already authenticated: the edge called `auth`, stripped the raw
API key, and injected a signed internal principal; this service verifies that
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

### Boot requirements

Booting requires `TETRAL_DATABASE_URL`, `ENGINE_VAULT_KEY`,
`TETRAL_AUTH_INTERNAL_PRINCIPAL_PUBLIC_KEY_B64`, and
`TETRAL_DEFAULT_ENVIRONMENT_ARTIFACT_REF`; the rest are optional or test-only
as noted.

| Variable | Purpose |
|----------|---------|
| `TETRAL_DATABASE_URL` | PostgreSQL DSN for the control-plane database. TLS settings are honored verbatim; the engine never overrides `sslmode`. PostgreSQL 18 is the tested target. |
| `TETRAL_TEST_DATABASE_URL` | Test-only. DSN for `go test`; not read by the running server. The test helper provisions an isolated schema per test under a least-privilege role. |
| `ENGINE_DATA_DIR` | Optional; defaults to `/var/tetral`. Local filesystem state root — control-plane records never live here, so moving it migrates no SQL data. |
| `ENGINE_VAULT_KEY` | 32-byte hex AES key encrypting vault credential secrets at rest; never carries request authentication. |
| `TETRAL_AUTH_INTERNAL_PRINCIPAL_PUBLIC_KEY_B64` | Base64-encoded Ed25519 public key used to verify the signed internal principal injected by the auth service. |
| `TETRAL_DEFAULT_ENVIRONMENT_ARTIFACT_REF` | Prebuilt provider artifact reference used for environments with no custom packages. |
| `ENGINE_PORT` | Optional legacy fallback. Read only when `TETRAL_API_HTTP_ADDR` is unset, and then only for the port half — the listener becomes `:$ENGINE_PORT`. Setting `TETRAL_API_HTTP_ADDR` makes it inert. |

The public API listener binds `:8080` (`TETRAL_API_HTTP_ADDR`); metrics bind
`:8081` (`TETRAL_API_METRICS_ADDR`); the two addresses must differ.

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

### Public `/v1` surface status

All public endpoints require edge authentication; `GET /health` is
unauthenticated.

| Group | Status |
|-------|--------|
| Sessions | Live create/list/get/update/archive/delete and session resources |
| Session threads | Live read surface under `/v1/sessions/{id}/threads` (list, get, archive) |
| Session events | Live `POST /v1/sessions/{id}/events` (the admission-capped agent/session input path) and `GET` list. The SSE surfaces are owned by the `event-stream` service |
| Agents | Live create/list/get (optionally at a specific version via a query parameter)/update/archive/version list. A version is read through the get route's version selector |
| Environments | Live create/list/get/update/archive/delete |
| Vaults + Credentials | Live create/list/get/update/archive/delete |
| Memory Stores | Live create/list/get/update/archive/delete, memory rows, and versions |
| Files | All five `/v1/files` routes live when a BlobStore is configured; without one, every `/v1/files` route returns 501 — not only uploads |
| Skills | All nine `/v1/skills` routes live when a BlobStore is configured; without one, every `/v1/skills` route returns 501 — not only uploads |
| Models | Live `GET /v1/models` and `GET /v1/models/{model_id}`, served from the in-code catalog |
| API keys | Owned by the `auth` service |
| Health (`GET /health`) | Live (200), unauthenticated |

## States & lifecycle

### Admission gate (every request)

| Step | Rule | Failure |
| --- | --- | --- |
| Principal | Verify the `auth` signed principal's signature, audience, expiry, method, and path. Workspace authority comes from `Principal.workspace_id` only. | `401 authentication_error` |
| Selector binding | Client-supplied `workspace_id`, `session_id`, `vault_id`, etc. are selectors, never authority. Every query binds `Principal.workspace_id` plus the route selector. | — |
| Cross-workspace / missing | Never falls back to another workspace, default resource, or default credential. | `404 not_found_error`, or `403 permission_error` where an authenticated-but-forbidden action exists |
| Beta marker | Every `client.beta.*` route requires `beta=true`; omitted, repeated, or altered is rejected. `/v1/models` accepts the marker optionally. | `400 invalid_request_error` |

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
mutations also read `session_runtime_status.status` (one of
`idle`, `running`, `rescheduling`, `terminated`, with cleanup tracked by
separate columns). A session mutation — `update`, resource add, resource
delete — requires the runtime status to equal `idle` exactly: any non-`idle`
status (`running`, `rescheduling`, or `terminated`) is rejected, and so is a
`lifecycle_state` of `archiving`. A non-idle or archiving target returns
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
| `github_repository` `authorization_token` | rotatable in any run state — a pure metadata write, encrypted at rest, never echoed |

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
`null`. `deployment_id` is always `null`.

### Thread lifecycle

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
| `user.interrupt` without `session_thread_id` | fanned out at admission to every `visibility = 'public'` AND `archived_at IS NULL` thread (`listInterruptTargetThreadIDs`); Runtime receives explicit per-thread `interrupt_control` commands and never infers fanout scope. |
| `user.tool_confirmation` | `session_thread_id` resolved from the referenced pending approval row — never client-chosen. |

`Idempotency-Key` law:

| Header state | Behavior |
| --- | --- |
| omitted | admission mints a random server-side key (`id.New("idem_")`); the request gets no cross-retry deduplication. |
| supplied | must appear at most once, be non-blank, and be ≤ 255 bytes; a violation is `400 invalid_request_error`. |
| replay — same key, identical canonical request | `200` with the stored admitted events; no new rows or queue jobs. |
| same key, different canonical request | `409 invalid_request_error`. |

Only digests are stored in `session_event_idempotency_keys`: the key's SHA-256
digest plus a separate `canonical_request_hash` over the decoded batch (order,
per-event thread selector, tool-use references, types, payloads). The selector
in that hash is the interrupt *intent* (`session`, `thread:<id>`,
`tool_use:<id>`, or `primary`), never the expanded thread set — idempotency
lookup precedes fanout, so a session-wide interrupt replays identically
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
| `POST /resources` | Add a `file` resource referencing an already-uploaded file plus optional `mount_path`. The session must be `idle`. The transaction increments the resource revision; when a current Sandbox handle exists it also enqueues materialization, otherwise the first approved Sandbox tool materializes the latest revision during lazy activation. Non-idle or archiving sessions return `409`. |
| `POST /resources/{id}` | `github_repository` token rotation only — write-only `{authorization_token}`, encrypted at rest, returned redacted; admitted in any run state. A `file` or `memory_store` resource carries no mutable field → `400`. |
| `DELETE /resources/{id}` | Tombstone/detach for every type. The session must be `idle`. A resource with no materialized Sandbox handle detaches immediately; otherwise the row becomes deletion-pending and the same transaction advances the resource revision and enqueues materialization. Non-idle or archiving sessions return `409`. |

`memory_store` resources are attached explicitly by ID at session create — never
silently defaulted and never added through `POST /resources`. A declared `file`
resource materializes read-only at its resolved `mount_path`, or at the default
`/mnt/session/uploads/<session_file_id>`.

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
| Display title | ≤ 1024 runes (`maxDisplayTitleRunes`) |

A root `SKILL.md` is required. Only the bounded segment between the opening and
closing `---` delimiters reaches the YAML parser, capped at 32 KiB
(`MaxFrontmatterBytes`); it decodes strictly into exactly two fields — `name`
(≤ 64 runes, `MaxNameRunes`) and `description` (≤ 1024 runes,
`MaxDescriptionRunes`). Anchors, aliases, explicit tags, merge keys, extra keys,
non-scalar values, and multi-document streams are rejected. The
`gopkg.in/yaml.v3` parser is confined to this package
(`internal/skill/dependency_test.go`).

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

The store's soft-archive axis and its create-time-only attachment (a
`memory_store` resource is attached by ID at session create, never defaulted and
never added through `POST /resources`) are covered above.

## Seams

### Signed-principal boundary (owner: auth)

- **Contract.** Requests reach this service only with a valid
  `X-Tetral-Internal-Principal` injected by the edge. This service verifies the
  signature, audience (`tetral-public-api`), expiry, method, and path, then
  attaches `Principal { workspace_id, api_key_id }` to the request context.
  Raw public API keys never reach this service.
- **Invariant a replacement must preserve.** No public handler may read identity
  from the request body or from client-supplied `X-Tetral-*` headers. Workspace
  authority is the verified principal alone.
- **Conformance.** `internal/httpapi/router_test.go` and the SDK-compat suites
  exercise the authenticated path; `auth` owns the principal-minting
  contract.

### SDK-compatibility surface

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
  ordering, and view).
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
  cannot be made usable.
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
| `internal/httpapi/session_event_handler_test.go`, `internal/sessionevent/service_test.go` | batch-atomic event admission, per-type rejection and file-source ladder, interrupt fanout, idempotency-key mint/replay/conflict |
| `services/api/production_wiring_static_test.go`, `.../startup_config_error_test.go`, `.../startup_config_surface_test.go` | the assembled process wires the real domain services and fails closed on invalid startup config |
| `services/api/tetralapi_test.go`, `.../startup_test.go` | end-to-end service bootstrap |

If a PR changes the admission fence, a route family's public surface, the
pagination or error contracts, the create-time-only field rules, the
same-transaction enqueue pattern, the upload/attachment caps, the SKILL.md
ingestion rules, or the memory content-bound/path-conflict/precondition rules in
this folder, it updates the matching section here.
