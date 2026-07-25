# event-stream

## Responsibilities

Read-only public access to a session's event history. The deployed binary
(`cmd/event-stream`) serves the two Server-Sent Events endpoints — a
session-level stream and a per-thread stream — that let an SDK client follow a
live session:

```text
GET /v1/sessions/{session_id}/events/stream
GET /v1/sessions/{session_id}/threads/{thread_id}/stream
```

The matching list endpoints (`GET /v1/sessions/{session_id}/events` and
`GET /v1/sessions/{session_id}/threads/{thread_id}/events`) share the reader,
query-parsing, and page-token code in `internal/eventstream` but are compiled
into and served by `api`, not this binary. Both stream and list surfaces
live behind a signed internal principal: `auth` verifies the
Edge-authenticated caller, and this service reads the principal's `workspace_id`
as the request scope. Every query is keyed on that `workspace_id`, and
`workspace_id` is the leading column of every table's primary key, so one
workspace never reads another's rows.

This service owns no durable tables. It reads four tables, writes nothing, and
never calls Runtime Pod, Bridge, Gateway, or Sandbox Service. Its only
dependency is PostgreSQL. It does not admit events, consume queue jobs, execute
tools, or drive Runtime — event admission (`POST /events`) and the runtime
writers that stamp `processed_at` and append change rows all live outside this
folder.

The wire shape is the flattened fork-SDK event union: `type`, `id`,
`processed_at`, and the type-specific fields at top level, with no generic
`payload` envelope and no `session_id`/`thread_id` envelope fields. It is
produced by the shared `internal/eventwire` projection
(`MarshalPublicEvent`), not by this folder. The set of event types this reader
projects, and which are session-observable versus produced-but-not-emitted, is
the SDK compatibility surface — the `T-COMPAT-EVOUT-*` cases in the forked
SDK repository's compatibility registry
(`tests/compatibility/compat-cases.json`). This README does not
restate that matrix.

## States & lifecycle

### Two cursor sources

Streams and lists never share a cursor. A stream pages the multi-revision
change log; a list is a snapshot over event bodies. Paging a list over the
change log would surface an event twice and break the SDK list shape.

| Surface | Endpoint | Source table | Paging key | Each event | Served by |
| --- | --- | --- | --- | --- | --- |
| Session stream | `/events/stream` | `session_event_stream_changes` | `stream_position` (session-global, monotonic) | may re-deliver on a revision bump | this binary |
| Thread stream | `/threads/{id}/stream` | `session_event_stream_changes` | `stream_position` | may re-deliver | this binary |
| Session list | `/events` | `session_events` | `insert_stream_position` (`event_id` tie-break) | once, at latest revision | `api` |
| Thread list | `/threads/{id}/events` | `session_events` | thread-local `sequence` | once, at latest revision | `api` |

### Durable ledger keys (all writers external)

Every writer named here lives outside this package (the append path in
`internal/sessionevent`, `internal/session`, and
`services/bridge`). This service only reads.

| Key | Read by (this reader) | Rule |
| --- | --- | --- |
| `session_event_stream_changes.stream_position` | change feed + `Current*StreamPosition` (via `MAX`) | append-only, strictly increasing per `(workspace_id, session_id)`; rows are never rewritten |
| `session_events.insert_stream_position` | session list ordering + session cursor | set once to the first `stream_position` the event appears at, immutable thereafter |
| `session_events.sequence` | thread list ordering + thread cursor | unique and stable per `(workspace_id, session_id, session_thread_id)`; never compared across threads |
| `session_events.revision` | delivered as a same-`id` update on the stream | starts at 1, bumps when an existing public row's read state changes (e.g. `processed_at` stamped after Runtime commits an accepted input) |

A queued inbound event carries `processed_at = NULL`. When Runtime later commits
it, `processed_at` is stamped and `revision` bumps, appending a new change row
at a higher `stream_position`. On the stream this re-emits the same-`id` event
as an update; on a list it is simply the one row at its latest revision. The
change log exists so that `processed_at`, terminal-state, and revision updates
cannot be missed by a subscriber that already advanced past the original event.

### SSE stream loop

For a session stream the handler first resolves the current high-water cursor
(`MAX(stream_position)` over the visible change set), then flushes the SSE
response headers. Change rows that already existed before that opening mark are
never replayed. The thread stream is the same loop scoped to one
`session_thread_id`.

| State | Trigger | Action |
| --- | --- | --- |
| Open | valid principal and `beta=true` | resolve high-water cursor, flush headers (`200`) |
| Poll | each iteration | fetch change rows past the cursor in bounded batches (≤ `defaultStreamBatchSize` = 100) |
| Emit | rows present | per row: `event: <event.type>` + `data: <public Event JSON>`, advance cursor to that row's `stream_position` |
| Heartbeat | no rows this poll | write a `: heartbeat` comment frame, flush, sleep (`defaultStreamPollEvery` = 1s), re-poll |
| Close (deleted) | an emitted event's type is `session.deleted` | return; the server closes and sends nothing further |
| Close (disconnect) | client context done at the poll sleep | return |
| Close (read/marshal/write error) | error mid-loop, after headers flushed | return silently — the client sees the connection close with no error frame and no further bytes |

The heartbeat comment frame is required behavior: an idle session produces no
change rows, and without a periodic byte an intermediary can cut a healthy but
silent stream. The heartbeat and poll intervals are deployment tuning; the
heartbeat's existence is not.

The heartbeat only survives an intermediary that does not buffer. The SSE
ingress must carry the same long-read / no-proxy-buffering annotations the
git-proxy ingress already carries: without no-proxy-buffering at the ingress an
intermediary buffers SSE frames and the stream is broken regardless of the
heartbeat.

### Read scope by endpoint

Both scopes read `visibility = 'public'` rows only; internal-visibility rows
and approval-reviewer threads are excluded from both.

| Scope | Row filter | Feed shape |
| --- | --- | --- |
| Session (list + stream) | `visibility = 'public'` AND `session_visible = TRUE` AND (`session_thread_id IS NULL` OR the thread is `visibility = 'public'` and `role <> 'approval_reviewer'`) | the main thread plus the cross-posted child-thread events that are session-observable — never every child thread flattened into one feed |
| Thread (list + stream) | `visibility = 'public'` on the one named thread; a missing, non-public, or approval-reviewer thread reads as `404` (`ensureReadableThreadTx`) | one named thread; `session_visible` is intentionally not applied, so a public-but-session-hidden child event is still returned here |

The session-visible cross-post set is closed: `agent.thread_message_sent`,
`agent.thread_message_received`, `session.thread_created`,
`session.thread_status_running`, `session.thread_status_idle`,
`session.thread_status_rescheduled`, `session.thread_status_terminated`. All
other child-thread events are written `session_visible = false`. A deleted
session or thread reads as `404` and never confirms a foreign one
(`ensureReadableSessionTx` / `ensureReadableThreadTx` gate every read on
`sessions.lifecycle_state`).

### Startup and process lifecycle

| Step | Gate |
| --- | --- |
| Config | `TETRAL_EVENT_STREAM_HTTP_ADDR` and `TETRAL_EVENT_STREAM_METRICS_ADDR` must differ; `TETRAL_AUTH_INTERNAL_PRINCIPAL_PUBLIC_KEY_B64` is required |
| `VerifySchema` | database schema matches before serving traffic |
| `VerifyRuntimeRole` | the connection uses the read-only runtime role |
| `MarkReady` | readiness is marked only after both verifications pass |

The main port also answers `/health` and `/ready`; `/metrics` is `404` there
and served on a separate metrics port (`buildHTTPHandler`). The pod mounts no
Kubernetes service-account token (`automountServiceAccountToken: false`) and
runs `readOnlyRootFilesystem` as a non-root user.

## Seams

### Stream reader (`Reader`, `eventstream.go`)

The SSE handler depends on the `Reader` interface, not on PostgreSQL directly:

```go
CurrentStreamPosition(ctx, ws, sessionID) (int64, error)
ListSessionEventChanges(ctx, ws, sessionID, after, limit) ([]StreamChange, error)
CurrentThreadStreamPosition(ctx, ws, sessionID, threadID) (int64, error)
ListThreadEventChanges(ctx, ws, sessionID, threadID, after, limit) ([]StreamChange, error)
```

- **Lifecycle**: constructed once at startup (`NewPostgreSQLReader`), shared
  across requests; each method opens its own workspace-scoped read-only
  transaction.
- **Invariants a replacement must preserve**: issue no writes and touch no
  queue, tool, or Runtime surface; scope every query on `workspace_id`; apply
  the session read filter (`visibility = 'public'` + `session_visible = TRUE` +
  the public non-reviewer thread gate) on the session methods and the
  thread-scoped filter (without `session_visible`) on the thread methods;
  return change rows past `after` ordered by ascending `stream_position`;
  compute the high-water head as `MAX(stream_position)` over the same visible
  set.
- **Conformance**: `TestPostgreSQLReaderListsAndStreamsPublicSessionVisibleEvents`,
  `TestEventStreamSessionSSEProjectsAllPublicChildEventVariants`,
  `TestEventStreamThreadSSEProjectsAllPublicChildEventVariants`,
  `TestPostgreSQLReaderRedactsStableReasoningLedgerFromListAndStream`.

### List reader (`ListReader`, `list.go`, hosted by `api`)

`ListSessionEvents` / `ListThreadEvents` back the list endpoints. The same
package parses query parameters: `limit` (default 20, hard cap
`maxListLimit` = 100 — a larger or non-positive `limit` is `400`), `page`,
`order` (`asc`/`desc`), a `types` event-type filter, and
`created_at[gt|gte|lt|lte]` admission-time bounds; `beta=true` is required and
any unknown parameter is `400`.

- **Invariants a replacement must preserve**: page over `session_events`, never
  the change log; order and page the session list by `insert_stream_position`
  (`event_id` tie-break) and the thread list by thread-local `sequence`; return
  each event once at its latest revision; emit an opaque `next_page` token.
- **Conformance**: `TestEventStreamListReturnsPublicEventEnvelope`,
  `TestPostgreSQLReaderSessionListUsesInsertPositionForCrossThreadOrdering`,
  `TestPostgreSQLReaderSessionListPaginationStableAcrossRevisionBump`,
  `TestEventStreamSessionListDecodesSDKFiltersAndRejectsUnknownParameters`,
  `TestEventStreamServiceRouterDoesNotServeListRoutes` (this binary serves
  streams only).

### Public wire projection (`internal/eventwire.MarshalPublicEvent`)

Both readers hand every row body to this projection, which flattens the durable
type-specific payload into the fork-SDK Event union while keeping the row-owned
`id`, `type`, and `processed_at` authoritative and dropping internal transport
fields (internal queue IDs, runtime input IDs, partition keys, Bridge delivery
IDs). Redaction is per-type field selection, not a blanket strip: each event
type retains only its wire-owned fields, so `span.model_request_end` keeps
`model_usage` while dropping the rest of its internal payload.

- **Invariants a replacement must preserve**: no generic `payload` or
  `session_id`/`thread_id` envelope on the wire; row metadata wins over payload
  copies; internal-only fields never surface.
- **Conformance**:
  `TestMarshalPublicEventFlattensPayloadAndKeepsRowMetadataAuthoritative`,
  `TestMarshalPublicEventProjectsBridgeChildVariantsWithExactLineage`,
  `TestMarshalPublicEventRedactsInternalModelRequestEndFields`,
  `TestMarshalPublicEventOmitsPrimaryThreadAgentNameAliases`.

### Signed list page token (`pagination.go`)

The list `page` token is opaque and HMAC-SHA256 signed with a 32-byte secret
(`WithPageTokenSecret`). It is version 3, resource `session_events`, and bound
to the workspace, session, thread, order, and filters, so a token cannot be
replayed against a different query, workspace, or filter set.

- **Invariants a replacement must preserve**: reject a token whose version,
  resource, scope, or filters do not match the current request; reject a
  tampered signature.
- **Conformance**: `TestPostgreSQLReaderRejectsOldSessionListCursorVersion`,
  `TestPostgreSQLReaderRejectsTamperedAndWrongScopePageTokens`,
  `TestPostgreSQLReaderSessionListFiltersByTypeAndCreatedAt`.

### Internal principal boundary (`auth.InternalPrincipalVerifier`)

Both routers mount `auth.InternalPrincipalMiddleware`. A request without a
valid signed principal is rejected before any query runs, and the principal's
`workspace_id` (via `workspace.MustIDFromContext`) is the sole request scope —
request bodies and path parameters never supply identity.

- **Invariants a replacement must preserve**: no route reachable without a
  verified principal; `workspace_id` derived only from the principal.
- **Conformance**: `TestEventStreamRoutesRequireSignedInternalPrincipal`,
  `TestEventStreamRoutesRequireExactlyOneBetaMarkerBeforeReaderAccess`.

## Testing guide

| Suite | Location | Proves |
| --- | --- | --- |
| `TestPostgreSQLReader*` | `internal/eventstream/eventstream_test.go` | read-only PostgreSQL behavior: public/session-visible filtering, cross-thread ordering by `insert_stream_position`, thread ordering by `sequence`, pagination stable across a revision bump, page-token scope/version rejection, type and `created_at` filters, stable-reasoning redaction |
| `TestEventStream*SSE*` / `TestIdleEventStreamEmitsHeartbeat*` / `TestEventStream*StartsAt*HighWater*` | `internal/eventstream/eventstream_test.go` | stream loop: start at current high-water, close on `session.deleted`, heartbeat before the next idle poll, thread-scoped high-water |
| `TestEventStreamList*` / `TestEventStreamServiceRouterDoesNotServeListRoutes` | `internal/eventstream/eventstream_test.go` | list envelope, SDK filter decoding, unknown-parameter rejection, and that this binary serves streams only |
| `TestEventStreamRoutesRequire*` | `internal/eventstream/eventstream_test.go` | signed-principal enforcement and the exact-`beta=true` gate |
| `TestEventStreamBoundaryLogsServerErrorsOnly` | `internal/eventstream/eventstream_test.go` | logging redaction: client errors are not logged as server errors |
| `TestMarshalPublicEvent*` | `internal/eventwire/public_event_test.go` | wire projection: flattened union, authoritative row metadata, internal-field redaction, child-variant lineage |
| `TestEventStreamProductionCodeKeepsReadOnlyRuntimeBoundary` / `TestEventStreamProductionCodeDoesNotImportExecutionOwners` | `services/event-stream/static_test.go` | static guard: this service imports no execution/writer package and stays read-only |
| `TestEventStreamCommand*` | `services/event-stream/cmd/event-stream/main_test.go` | startup: schema/runtime-role verification before serving, required internal-principal key, scoped routes, `/metrics` off the main port, redacted startup-failure logs |

If a PR changes the stream loop, the two cursor sources, the read-scope
filtering, the public wire projection, or the page-token shape, it updates the
matching section here.
