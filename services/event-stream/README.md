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
never calls Runtime Pod, Bridge, Gateway, or Sandbox Service. PostgreSQL is its authoritative dependency. Optional Core NATS subscriptions
carry best-effort Session previews; no NATS payload is persisted. It does not admit events, consume queue jobs, execute
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

The production database connection requires `TETRAL_DATABASE_TLS_CA_PATH` and
`TETRAL_DATABASE_TLS_SERVER_NAME`. It verifies trust and hostname with no
plaintext fallback. New connections load the current validated trust generation;
shutdown joins requests/work before closing the database and trust observer.

## States & lifecycle

### Discovery failure before model execution

When a configured MCP directory is missing or unready, Bridge may reject a user
input after its bounded discovery attempts fail. The input is marked processed
without model execution, and one `session.error` reports the server and
`retry_status: exhausted` for that input's discovery budget. An otherwise
inactive main session also emits `session.status_idle`; there need not be a
preceding running event for this input. Idle and processed markers therefore
do not prove that a model request ran. Other active threads are not ended by
this settlement. The session remains usable: a later distinct user input can
retry discovery and execute once the configured directory is ready. While the
directory remains unavailable, later messages can fail the same way.

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

Session viewers may request `event_deltas[]=agent.message`,
`event_deltas[]=agent.thinking`, or both. Repeated valid types are deduplicated;
unsupported values return `400` before SSE headers. Omission means formal-only
SSE and creates no preview subscription. Every Thread SSE endpoint remains
formal-only and rejects preview options, including a Thread endpoint naming the
main thread. Thinking previews are content-free starts; text previews use the
SDK's `event_start` and `event_delta` wrappers. They carry no durable envelope,
internal scope, or private sequence fields.

For a session stream the handler first resolves the current high-water cursor
(`MAX(stream_position)` over the visible change set), then flushes the SSE
response headers. Change rows that already existed before that opening mark are
never replayed. The thread stream is the same loop scoped to one
`session_thread_id`.

| State | Trigger | Action |
| --- | --- | --- |
| Open | valid principal and `beta=true` | resolve high-water cursor, flush headers (`200`) |
| Poll | the poll interval is due; or the previous poll returned rows or an End group; or an admitted preview's Start is not yet behind the cursor; or preview loss was observed | fetch change rows past the cursor in bounded batches (≤ `defaultStreamBatchSize` = 100) |
| Emit | ordinary rows present | per row: `event: <event.type>` + `data: <public Event JSON>`, advance the durable cursor only after a successful write |
| Defer generated text | `agent.message` correlated to a model request | consume its change position without emitting or closing its preview; the SQL change query returns identity metadata with no text payload |
| Publish request text | visible durable `span.model_request_end` | release the ordinary batch and suffix; read one complete committed text per page in stored sequence order, emit each full original event, then End; query discarded suffix again after the End cursor |
| Heartbeat | the heartbeat timer is due (first iteration, then every `TETRAL_EVENT_STREAM_HEARTBEAT_INTERVAL_MS`), independent of rows | write a `: heartbeat` comment frame and flush |
| Wait | the poll returned no rows, or no poll was due | wait for the independent poll/heartbeat timers (both initially 1s), a preview wake or cancellation; a preview wake runs the bounded preview slice without a formal change poll. After rows or an End group the loop re-polls without waiting |
| Close (deleted) | an emitted event's type is `session.deleted` | return; the server closes and sends nothing further |
| Close (disconnect) | client context done at the wait | return |
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

Generated text remains immediately readable in history lists after commit. Its
SSE publication waits for the corresponding durable End on both preview and
formal-only connections. Normal, error, interrupted, and recovery Ends expand
only complete committed messages between their exact Start and End. A stream
opened during a request receives its earlier committed messages when that
future End arrives; a stream opened after End uses history recovery. Tool and
permission events continue through ordinary delivery while text waits.

The End group retains one current complete text body, its transient public JSON
encoding, and the End descriptor. No later formal event overtakes the group.
Read or write failure closes the connection; no new durable publication state or
preview replay cache is introduced. Lists retain their existing keys and
committed-state semantics.

### Preview subscription and loss

A process-owned hub shares one ordinary NATS subscription per authorized
workspace/Session among local opted-in viewers. Separate Event Stream processes
receive ordinary fan-out; there is no queue group. Last-local-viewer removal
closes that subscription, concurrent joins cannot reuse it while closing, and
shutdown joins the dispatcher before closing its transport and database.

Subscription setup occurs after scope authorization and before the final
opening high-water mark. A one-second setup budget cannot hold formal SSE open
indefinitely: unavailable NATS degrades previews internally, while PostgreSQL
remains active. The subscriber restores desired subscriptions after reconnect;
only newly started requests can become eligible, with no old preview replay.

Private `request_open` admission reads the exact scoped database Start and its
thread row. Eligible requests require `role = main`, `visibility = public`,
`request_kind = agent_provider_request`, a Start position after this viewer's
opening mark, and no matching durable End. Child/reviewer/compaction and foreign
scope frames are rejected. Identity is cached for the request, rather than read
for every delta. The writer catches up formal changes through Start before
sending previews.

Each event sends a contiguous prefix after its observed start. Lost, duplicate,
or reordered sequence stops the preview conservatively. Unknown-event deltas
are dropped. Viewer overflow affects that viewer; subscription loss invalidates
all affected viewers. Formal events close matching IDs even when a preview
start is delayed; the exact primary request End releases its event state and
prevents late reopening. Thinking bodies, signatures, tool inputs and raw
provider metadata never enter preview frames. A missing final tail may be
undetectable and is not counted as detected loss.

One response writer owns formal data, previews and heartbeats. Every write and
flush gets a ten-second deadline; request cancellation retires an active blocked
write and joins that cancellation watcher. Formal changes are polled at least
once per poll interval, and at once after a poll that made progress, when an
admitted preview's Start is not yet behind the cursor, or after preview loss;
preview wakes never add a formal change poll per delta. A preview flood
therefore cannot delay formal lifecycle or End-group delivery beyond one poll
interval, the same bound formal-only viewers have, and previews of a request
may continue for up to that interval after its End commits; they remain
prefixes and the emitted End closes them.

Session deletion remains observable on an existing Session feed; deleted
sessions cannot open new feeds. An End group whose End precedes
`session.deleted` is still published on that existing Session feed. Preview
loss detected after deletion stops previews and releases the subscription,
while formal delivery continues until `session.deleted`. Thread loss of
readability closes its feed and all departing viewers release their references.

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
session reads as `404` when a stream opens, on lists and on every Thread read,
and never confirms a foreign one (`ensureReadableSessionTx` /
`ensureReadableThreadTx` gate those reads on `sessions.lifecycle_state`). An
already-open Session feed is the exception: `ensureReadableSessionFeedTx` keeps
it readable until its `session.deleted` change is behind the cursor, and its
End-group expansion applies the same gate at the End's own position, so an End
committed before the deletion change is still published.

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
ReadPreviewRequest(ctx, ws, sessionID, threadID, modelRequestID, startEventID) (PreviewRequest, error)
ListRequestFinalMessages(ctx, scope, endEventID, afterSequence, 1) ([]RequestFinalMessage, error)
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
  set; generated model text changes carry no selected payload body; request
  pages verify their exact database End/Start and preserve endpoint visibility.
  A request-final page contains at most one complete event.
- **Conformance**: `TestPostgreSQLReaderListsAndStreamsPublicSessionVisibleEvents`,
  `TestEventStreamSessionSSEProjectsAllPublicChildEventVariants`,
  `TestEventStreamThreadSSEProjectsAllPublicChildEventVariants`,
  `TestPostgreSQLRequestFinalMessagesAndPreviewAdmission`,
  `TestPostgreSQLSessionChangeLifecyclePreservesDeletion` (including an End
  committed before the deletion change on an open Session feed). The
  `TestPostgreSQLPublicStreamingIdentity` and
  `TestPostgreSQLPublicStreamingVisibility` integration cases keep private
  reasoning and tool-input markers out of preview frames and thinking events.

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
| `TestPostgreSQLReader*` | `internal/eventstream/eventstream_test.go` | read-only PostgreSQL behavior: public/session-visible filtering, cross-thread ordering by `insert_stream_position`, thread ordering by `sequence`, pagination stable across a revision bump, page-token scope/version rejection, type and `created_at` filters |
| `TestEventStream*SSE*` / `TestIdleEventStreamEmitsHeartbeat*` / `TestEventStream*StartsAt*HighWater*` | `internal/eventstream/eventstream_test.go` | stream loop: start at current high-water, close on `session.deleted`, heartbeat before the next idle poll, thread-scoped high-water |
| `TestEventStreamList*` / `TestEventStreamServiceRouterDoesNotServeListRoutes` | `internal/eventstream/eventstream_test.go` | list envelope, SDK filter decoding, unknown-parameter rejection, and that this binary serves streams only |
| `TestEventStreamRoutesRequire*` | `internal/eventstream/eventstream_test.go` | signed-principal enforcement and the exact-`beta=true` gate |
| `TestEventStreamBoundaryLogsServerErrorsOnly` | `internal/eventstream/eventstream_test.go` | logging redaction: client errors are not logged as server errors |
| `TestPostgreSQLRequestFinalMessagesAndPreviewAdmission` / `TestPostgreSQLSessionChangeLifecyclePreservesDeletion` | `internal/eventstream/request_final_messages_test.go` | actual read-only serving role: exact scope/Start/End, metadata-only changes, one-message pages, committed list bodies, cancellation and deletion visibility, and an End group committed before deletion staying expandable only on the open Session feed |
| `TestPostgreSQLRequestEndProjectionResidency` | `services/event-stream/preview_writer_test.go` | real PostgreSQL reader and response writer for Session and Thread: three large complete messages whose change descriptors carry no body; the first End-group body write held at the response sink, where the reader-returned change arrays (including the unconsumed suffix) no longer reference payloads, exactly one End page has been requested and the current encoding is in flight; then exact End/suffix order (with the Session marked deleted at the held write on Session feeds), or cancellation at the held write without a later page |
| `TestStreamLoop*` | `services/event-stream/preview_writer_test.go` | stream loop with controlled reads: a multi-batch backlog, End group and suffix drain without a poll-interval wait; preview wakes add no formal poll per delta; a formal row committed during a delta flood is still delivered; preview loss after the session became unreadable releases the subscription and still delivers `session.deleted` |
| `TestNATSNative*` / `TestNATSSubscriber*` | `services/event-stream/preview_native_queue_test.go` / `preview_nats_test.go` | pinned official client over controlled TCP: shared process queue/reservations, at/over byte and count bounds, unaffected/future healthy controls, oversized frame/broker ceiling rejection, current callback and connection-attempt joins; real broker/TLS/SDK coverage is separate integration evidence |
| `TestPreviewHubExact*` / `TestPreviewViewerExact*` / `TestPreviewHubIngressCount*` / `TestPreviewViewerCount*` | `services/event-stream/preview_bounds_test.go` | independently padded limit−1/exact/+1 encoded byte boundaries for ingress, fanout, viewer queue/current write/encoding and aggregate encoding; independent current-ingress/current-write count limits, unaffected viewers and joined cleanup |
| `TestNATSNativeExactProcessByteReservationBoundary` / `TestNATSNativeHeartbeatOptionsConsumeTypedEnvironment` / `TestNATSHeartbeatConfigRangesAndFailFast` | `services/event-stream/preview_native_queue_test.go` / `config_test.go` | native byte reservation threshold−1/exact/+1 with frame counts nonbinding; environment defaults/overrides reach real pinned-client ping options; invalid local settings fail before startup |
| `TestPreviewProcessShutdownCancelsAndJoinsLiveSSEReads` | `services/event-stream/preview_shutdown_test.go` | live TCP Session/Thread/opt-in SSE: opened-header/read barriers, a live preview viewer and subscription only for the opted-in Session, process-context reader cancellation, writer/callback joins and zero ownership gauges |
| `TestSSEWriter*` | `services/event-stream/preview_sse_writer_test.go` | held response body Write borrows the exact reserved slice; one deadline/flush, exact SSE framing and partial/error sent-byte accounting |
| `TestPreviewMetrics*` | `services/event-stream/preview_metrics_test.go` | balanced ownership gauges, fixed latency bucket observations and labels, End/cancel/reset sequencer cleanup |
| `TestPreviewHub*` / `TestPreviewWriter*` / `TestStreamQuery*` | `services/event-stream/preview_*_test.go` | transport component: fan-out/refcounts, last-unsubscribe race (no Subscribe while the previous subscription is still closing), queued/in-flight accounting, sequence prefixes, exact closure, eligible identity, classified unavailable admission and bounded state |
| `TestPreviewProtocol*` / `TestPreviewDecoder*` / `TestPreviewEncodedFrameByteBound` | `internal/eventwire/preview_event_test.go` | shared private/public fixtures, scope/vocabulary rejection, Unicode safety and encoded-size boundary |
| `TestMarshalPublicEvent*` | `internal/eventwire/public_event_test.go` | wire projection: flattened union, authoritative row metadata, internal-field redaction, child-variant lineage |
| `TestEventStreamProductionCodeKeepsReadOnlyRuntimeBoundary` / `TestEventStreamProductionCodeDoesNotImportExecutionOwners` | `services/event-stream/static_test.go` | static guard: this service imports no execution/writer package and stays read-only |
| `TestEventStreamCommand*` | `services/event-stream/cmd/event-stream/main_test.go` | startup: schema/runtime-role verification before serving, required internal-principal key, scoped routes, `/metrics` off the main port, redacted startup-failure logs |

If a PR changes the stream loop, the two cursor sources, the read-scope
filtering, the public wire projection, or the page-token shape, it updates the
matching section here.

## Preview process configuration

All settings are parsed and validated once before serving. With every
`TETRAL_NATS_*` setting absent, preview transport is disabled and formal SSE
continues normally. To enable it, supply comma-separated `TETRAL_NATS_SERVERS`,
`TETRAL_NATS_USER_PATH`, and `TETRAL_NATS_PASSWORD_PATH`.
`TETRAL_NATS_CONNECT_TIMEOUT_MS` bounds one whole connection attempt, and
`TETRAL_NATS_RECONNECT_WAIT_MS` is the supervisor's retry interval for building
a fresh connection; the official client itself never reconnects. Each defaults
to 1000 and accepts positive millisecond values up to 5000. In the typed
`NATSConfig`, a zero timeout selects that default and a negative one fails
before startup. Credentials are loaded from mounted files; server URLs cannot
carry credentials, query strings or paths. Broker unavailability does not
withdraw core-service readiness.

The Go subscriber's native broker heartbeat uses `TETRAL_NATS_PING_INTERVAL_MS`
(default 120000 milliseconds, range 1–3600000) and
`TETRAL_NATS_MAX_PING_OUT` (default 2 outstanding pings, range 1–16).
These preserve the pinned Go client's defaults; the Gateway publisher has its
own defaults. The typed `NATSConfig.PingInterval` and
`NATSConfig.MaxPingsOutstanding` reach the official client's ping options on
every fresh connection. Invalid environment or typed settings fail before
credentials or network startup. Broker pings are independent of the public SSE
heartbeat and the supervisor's reconnect delay.

The protected transport also requires all three mounted references:
`TETRAL_NATS_TLS_CA_PATH`, `TETRAL_NATS_TLS_CERT_PATH`, and
`TETRAL_NATS_TLS_KEY_PATH`. Partial TLS configuration fails startup. The official
Go client performs TLS before INFO, validates the DNS name of each seed and
advertised reconnect destination, and obtains a fresh validated trust/certificate
snapshot for new connections. Trust retirement closes affected established
connections; malformed reload retains only valid last-known-good credentials,
and expiry cannot enable plaintext. Long-lived SSE work also receives the command's process shutdown context.
Shutdown cancels its SQL readers and response writer before the shared HTTP
drain, joins its cancellation callback, and releases its viewer/sequencer state.
The process supervisor and native dispatcher join before their
credential observer closes. The supervisor creates a fresh official-client
connection after loss, verifies its advertised `max_payload` before restoring
desired subscriptions, and retains up to 64 validated seed/discovered addresses
(each at most 2048 bytes) so another advertised DNS destination remains usable
when the original seed is down. It never replays old native subscription identities.

| Setting | Initial value |
| --- | --- |
| `TETRAL_EVENT_STREAM_POLL_INTERVAL_MS` | 1000 |
| `TETRAL_EVENT_STREAM_HEARTBEAT_INTERVAL_MS` | 1000 |
| `TETRAL_EVENT_STREAM_WRITE_TIMEOUT_MS` | 10000 |
| `TETRAL_EVENT_STREAM_PREVIEW_SETUP_TIMEOUT_MS` | 1000 |
| `TETRAL_EVENT_STREAM_HUB_MAX_BYTES` / `HUB_MAX_FRAMES` | 8388608 / 2048 |
| `TETRAL_EVENT_STREAM_VIEWER_MAX_BYTES` / `VIEWER_MAX_FRAMES` | 262144 / 128 |
| `TETRAL_EVENT_STREAM_SUBSCRIPTION_MAX_BYTES` / `SUBSCRIPTION_MAX_FRAMES` | 8388608 / 2048 |
| `TETRAL_EVENT_STREAM_ACTIVE_REQUESTS` | 32 |

Timer values are positive milliseconds; poll/heartbeat/write values accept up
to 60000 and preview setup up to 10000. Byte/count values are positive bounded
integers; viewer limits cannot exceed hub limits. Encoded queued, in-flight and
preview encoding bytes stay charged until release. The hub budget is one
process application budget shared by ingress, every viewer queue, in-flight
frames and public encoding. The `SUBSCRIPTION_MAX_*` settings independently
bound the **whole process native receive storage**, rather than each Session.

The pinned official client uses one shared `ChanSubscribe` channel across all
Session subscriptions. The broker ceiling is 1 MiB; a fresh connection
advertising a larger or invalid maximum is rejected before `SUB`. Native storage
reserves two broker-sized payloads for parser/admission copies and one for the
current dispatcher message. Buffered slots are
`min(floor(SUBSCRIPTION_MAX_BYTES / 1048576) - 3, SUBSCRIPTION_MAX_FRAMES - 3)`.
Enabling NATS therefore requires at least 3145728 native bytes and three native
frame credits; zero buffered slots is allowed at that minimum. Defaults reserve
five buffered slots, one current message and two parser/admission payloads,
for at most 8 MiB native storage. This fixed process bound does not limit the
number of watched Sessions or grow with their subscriptions. Native overflow
replaces only affected subscription identities and invalidates their previews;
other subscriptions and future eligible opens remain live.

The default aggregate encoded pending-payload bound is 16 MiB (8 MiB hub plus
8 MiB native), with a configured combined count ceiling of 4096. Native
worst-case message size derives a smaller actual default occupancy of eight
ownership slots. Native reservations are conservative, separately reported
from measured application bytes and the current native payload. Ordinary
durable change-page payloads, the single complete final body and its encoding
remain separately bounded by their owning content/read contracts. The strict
single-pass decoder has one current encoded scratch buffer of at most
`2 * 262144 + 512` bytes, separately from pending payload storage. Duplicate-key
tracking retains only the twelve allowed field names. Public wrapper size is
calculated without output allocation, reserved against both hub/viewer limits,
then encoded directly into that one exact output slice, including UTF-8,
control-character and Go HTML escaping. The single SSE writer sends the small
complete event header, the original reserved encoded slice and the terminator
separately under one deadline, then flushes once. It never formats/copies a
complete encoded body into another SSE buffer. Sent-byte counters include each
actual successful/partial Write, and a failed part stops the frame before flush.
The fixed private frame limit is 256 KiB, and each active request retains at most 4096 preview event
identities. Capacity failure stops previews while formal delivery continues.

Preview metrics expose balanced active SSE/viewer/subscription/request and pending-byte gauges,
separate native reservation/queued-current count/current-byte gauges,
invalid-frame and stop counters, written formal/preview event counts, sent
bytes, slow writers and broker lifecycle. No event/request identity is a metric
label. Counters describe local observations, not guaranteed subscriber delivery. Fixed
latency bucket counters, sums and counts use monotonic elapsed time: formal
selection/read through successful response flush (End includes its final
publication group), and local hub ingress through successful preview flush.
The constant `le` bucket label is the only latency label; these observations
do not measure provider, broker or client end-to-end latency.

## Process diagnostics

The command follows the shared [Go process diagnostic contract](../../internal/workload/README.md#diagnostics)
for the restart-only `TETRAL_LOG_*` controls, the default Info level, bounded
suppression summaries, diagnostic drop and sink-failure metrics, and the
diagnostic close after listeners and business resources.

Preview diagnostics report the first unavailable/disconnect/recovery transition
and classified sequence/capacity/write loss, and an admission read that fails
for a reason other than a missing or invalid request identity
(`admission_unavailable`; missing or ineligible identity stays silent).
Records identify the operation, scoped request where known, bounded reason and
`formal_active` outcome; they
contain no text, tool inputs, credentials, provider metadata or raw errors.
Routine previews and polls do not produce per-fragment records. The process
logger bounds repeated diagnostics and emits suppression summaries independently
of business delivery.
