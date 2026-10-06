# gateway

This workspace owns the Provider Gateway and MCP Connector Bun processes in
`packages/provider-gateway` and `packages/mcp-connector`. Each runs in its own
Deployment with its own ServiceAccount, credentials, probes, metrics Service and
replica setting. Package-owned manifests live under `k8s/provider-gateway/` and
`k8s/mcp-connector/`. The Go Web Connector runs independently under
`services/web-connector`; its implementation and manifests belong there.
Shared protocol, lowering and schema packages remain in this workspace.

Provider Gateway, MCP, and Web each use an ordinary ClusterIP Service. Routed
mesh upstreams select replicas for each new request while preserving an admitted
stream on its original upstream. Provider autoscaling targets only `provider-gateway`, with
its existing minimum of two, maximum of ten and CPU target of 70 percent.

## Responsibilities

`provider-gateway` terminates the Runtime-to-Gateway gRPC protocol
(`ProviderGatewayService.StreamProviderRequest`), lowers a Tetral-internal
request into the native API of a **closed seven-model catalog**, streams the
provider response back as a normalized internal event set, and classifies
provider failures into a bounded taxonomy. It resolves the request's credential
by `request_kind` alone — never by inspecting messages or tools — decrypts it in
process, runs a platform key pool for platform-hosted access, and resolves media
attachment references at lowering time. It is a **stateless pure streaming
transformer**: it writes no `session_events`, `session_messages`, or
`sessions.usage`, holds no cross-turn state, and scales by replica count. Exactly
one durable write is sanctioned — provider OAuth rotation write-back through the
credential update path. It never chooses or replaces a model; it uses exactly the
`ModelRef` the runtime supplied.

The seven catalog models (`packages/provider-gateway/src/providers/catalog.ts`):
`anthropic/claude-opus-4-8`, `anthropic/claude-fable-5`, `openai/gpt-5.5`,
`openai/gpt-5.6-sol`, `deepseek/deepseek-v4-pro`, `moonshotai/kimi-k3`,
`zai/glm-5.2`. Any other model — including other models of the same providers —
fails closed with `provider-error(code = provider_unavailable, retryable =
false)`.

## States & lifecycle

### Request turn (`packages/provider-gateway/src/service.ts`)

`ProviderGatewayServiceShell` drives one turn as an ordered pipeline; each stage
runs only after the prior succeeds.

| Stage | Action | Failure |
| --- | --- | --- |
| Authenticate | Verify the runtime workload token and the per-thread `runtime_binding_token` (scope triple, binding id/generation, pod UID, Runtime process ID, expiry) | gRPC `UNAUTHENTICATED`/`PERMISSION_DENIED` before any credential is resolved |
| Readiness | Reject if the process is not ready | gRPC `UNAVAILABLE` ("gateway service not ready") — transient, runtime retries |
| Admission | Bounded concurrent in-flight turns (`TurnAdmissionGate`, default 8, `TETRAL_GATEWAY_MAX_CONCURRENT_TURNS`) | fast retryable `provider-error` rather than event-loop queueing |
| Validate | `validateProviderRequest` (pure, in `packages/protocol`) | deterministic `INVALID_ARGUMENT` — **non-retryable** for that turn |
| Catalog lookup | Model → rules + client | unknown model → `provider_unavailable` (non-retryable) |
| Lower | `lowerProviderRequest` (hop ①, pure, once per turn) | lowering failure closes the turn |
| Credential + call | Resolve credential, attempt the provider stream inside the pool failover loop | see credential and pool tables |
| Raise + assemble + write | Private normalized SDK records → `ProviderBlockAssembler` → complete protobuf frames; one frame held through its write callback and any required drain | partial blocks discarded; complete prefix retained |

The v2 wire carries `ThinkingStarted`, `TextComplete`, `ReasoningComplete`,
`ToolCallComplete`, Finish, ProviderError, and the pre-stream attachment-rejection
report. Every frame has a consecutive `frame_sequence` beginning at one. Requests
must explicitly set `output_contract_version = 2`; absent, older, or unknown
versions fail before attachment, credential, or provider work. There is no fragment
wire fallback. Runtime supplies the acknowledged `model_request_start_event_id`,
thread role, and visibility from its loaded identity.

Fragment starts, deltas, ends, and streamed tool arguments stay inside Gateway's
private normalized union. The assembler checks their lifecycle, immutable tool
names, completion uniqueness, Unicode scalars, final metadata, and successful EOF.
Finish cannot hide an open text/reasoning block or tool input awaiting its complete
call. A provider attempt closes the credential failover fence at its first private
normalized event, even when the assembler has emitted no complete frame yet.

Text IDs are allocated on first nonempty content; empty text blocks produce no
frame. Thinking IDs are allocated at reasoning start; `ThinkingStarted` contains
only the provider part ID and event ID. Signed empty reasoning remains complete
content. The injected best-effort preview offer admits only main/public ordinary
requests, never includes reasoning bodies or signatures, and cannot change formal
delivery when disabled or throwing. Public preview publication belongs to the
public event-stream owner.

Finish and ProviderError are mutually exclusive terminal frames. Usage rides
Finish only. Attachment rejections are nonterminal and appear at most once before
provider streaming. Incomplete content is discarded on failure; delivered complete
frames retain their normal Runtime settlement semantics.

### Credential resolution by `request_kind`

`packages/provider-gateway/src/providers/credentials.ts`. `platformHosted`
providers are `anthropic`, `openai`, `deepseek`; `moonshotai` and `zai` are
session-key only.

| `request_kind` | Source | Fail-closed rule |
| --- | --- | --- |
| `agent_provider_request` | Bound `session_provider_auth` credential by `(workspace_id, session_id)` when present; else platform pool for a `platformHosted` provider | A bound credential that is missing/revoked/archived/wrong-provider/undecryptable/refresh-failed fails closed with **no** fallback to platform access; a no-credential request on a non-hosted provider → `credential_required` |
| `compaction_summary` | Resolved identically to `agent_provider_request` — the session's current model on the session's own credential path | Same as above; failure follows the compaction failure path |
| `approval_reviewer` | Platform-owned reviewer credential from the pool; its request schema is lowered by the selected route's `native_json_schema` or `json_object` capability | Unsupported routes fail before provider construction; Reviewer failure never touches the user's session credential |
| `approval_reviewer_compaction` | Resolved identically to `approval_reviewer` (platform reviewer credential); lowered like a compaction otherwise | Never falls back to the session credential |

Decrypted plaintext exists only in process memory and inside TLS to the
provider — never logged, never in any `ProviderStreamEvent`, never returned to
Runtime/Bridge, never persisted here.

### Platform key state machine (per-replica, in-memory)

`packages/provider-gateway/src/providers/pool.ts`. The pool reads
`platform_provider_keys` **read-only** (30 s cache); rows are written by an
operator, never by the serving path.

| From | Trigger | To | Notes |
| --- | --- | --- | --- |
| `ACTIVE` | 429 rate-limit | `COOLING` | ttl = retry-after else 5 s, capped at 60 s; returns to `ACTIVE` on expiry |
| `ACTIVE` | 401/403 auth, or quota/billing exhausted | `QUARANTINED` | process-lifetime; emits a structured alert log + metric |
| any | operator `UPDATE status='disabled'` | out of pool | the only durable disable; the gateway never writes the table |

Selection keeps the highest-priority tier, weighted-random within it (weight
floor `weight + 10`), and fails over across keys **only before the first
provider-originated event** (up to `min(healthy keys, 3)` attempts, no backoff
between different keys). After the first provider byte there is no key switch and
no retry — the terminal event is forwarded and the runtime owns recovery, while
an evidence-backed platform-key failure is still recorded so a later turn does
not select that key. The
pre-stream `attachment-rejections` event is gateway-originated and does not close
the failover window. User-credential sessions never enter the pool (one key,
dead is dead). An empty tier → `provider-error(code = platform_keys_exhausted,
retryable = true, retry-after = shortest remaining cooldown)`. If a turn first
quarantines an unusable key and no healthy key remains, the outward failure is
the generic non-retryable `provider_unavailable`; the failed credential is not
selected again, and the Runtime closes only that turn.

### Error classification (`packages/lowering/src/errors.ts`)

| Class | Mapping |
| --- | --- |
| 5xx, network/connection, timeout classes | `retryable = true` regardless of the SDK's own flag |
| OpenAI-family 404 | `retryable = true` (documented provider misbehavior) |
| Anthropic 400 `invalid_request_error` with the verified credit-exhaustion discriminator | quarantine the failed platform key before generic shape classification; outward pool exhaustion stays generic |
| DeepSeek status-less body whose captured message is exactly `Insufficient Balance` | quarantine the failed platform key even though transport supplied no status; never generalize other body text or other providers |
| 400/422 request-shape | fail fast to caller, do **not** rotate the key |
| no status without that captured DeepSeek signal | `provider_stream_error`, `retryable = true`; Runtime owns the existing bounded turn retry |
| context overflow (413, `context_length_exceeded`, message-pattern) | `code = context_overflow`, `retryable = false` (arms runtime reactive compaction) |
| subscription/entitlement (`usage_not_included`, `insufficient_quota`, `invalid_prompt`) | `retryable = false`, human-actionable public message |
| 429 on providers that overload it (openai/moonshotai/zai) | parse the body `code`/`type` to split transient rate-limit (`COOLING`) from terminal quota (`QUARANTINE`) |

`provider-error.error` carries only bounded fields (code, public message,
retryable, fatal, status code, retry-after). Credentials, raw headers/bodies,
stack traces, and signed URLs are stripped before the event leaves the process.
Platform-key failover never repeats the failed key. A rejected session-supplied
credential receives one attempt and credential-specific safe wording; it never
enters the platform pool. The gateway does not own Runtime turn retries.

### Process lifecycle

Ops plane is bare `Bun.serve` on a separate port (`/healthz`, `/readyz`,
`/metrics`; `packages/provider-gateway/src/http-server.ts`). SIGTERM withdraws
readiness and admission immediately. Provider and MCP allow a configured 30-second business drain
(`TETRAL_DRAIN_TIMEOUT_MS`), followed by a configured five-second
cancellation and join phase (`TETRAL_SERVICE_CANCEL_JOIN_TIMEOUT_MS`).
Both phases and the five-second proxy cleanup allocation must fit the
60-second Pod grace. Admitted requests may finish during drain; cancellation
then aborts provider and attachment work and joins remaining stream, SDK, and
unary workers, so an aborted provider request can still write its terminal
error. When that cancellation and join window expires, Provider Gateway
force-closes its remaining streams, including writes held by HTTP/2 flow
control, so their workers can join; Runtime treats the closed stream as an
interruption under its recovery contract. A missed join deadline
is reported after the owned workers join, so dependency closure cannot
overtake work. Listener and client close reuse the same remaining budget. SQL closes last, after producers join,
with a five-second allocation clipped to the remaining application deadline.
A cleanup failure does not skip other owned resources. Reusable service owners
retain an unjoined worker and its dependencies; they do not terminate the host
process. Executable commands arm one absolute deadline from the configured drain
and join phases (35 seconds by default), covering signal and command-finally
cleanup. If work cannot join, the command reports incomplete shutdown and exits
nonzero at that deadline. SQL and other required dependencies remain owned until
process termination, with no fabricated successful cleanup. Silent or throwing
diagnostic sinks cannot extend the deadline.

Provider request timeout starts at ingress and covers authentication,
credential preparation, every attachment metadata/chunk RPC, provider headers,
and response consumption. Each downstream call receives the remaining absolute
budget and real cancellation. Shutdown joins actual unary callbacks before
closing their channel. MCP similarly retains pending credential transactions,
SDK connects, notifications, and calls after their public timeout or cache
eviction until they actually join; each SDK client closes once. A reusable MCP
client close that exceeds its deadline reports the original timeout only after
those joins, so command cleanup cannot advance to SQL closure early. The
executable enforces its shared absolute deadline if work cannot join.

MCP's typed Bridge policies mirror the Runtime descriptor: manifest notification
five seconds and claim/commit/relinquish ten seconds. SDK defaults remain
credential 15 seconds, connect ten seconds, call and discovery 120 seconds, and
idle eviction 1,800 seconds. `TETRAL_BRIDGE_<METHOD>_TIMEOUT_MS` and
`TETRAL_MCP_{CREDENTIAL,CONNECT,CALL,DISCOVERY,EXECUTION}_TIMEOUT_MS` configure these actual
operations. Phase ceilings clip to the shared execution allowance. The execution
allowance is at most 170 seconds and first Commit is at most ten seconds within
the 180-second claim lease. A lost immutable result-commit response can
rejoin its named receipt; an unknown execution outcome cannot be replayed or
converted into a fabricated result.

## Seams

Each boundary below is independently replaceable. A replacement is conformant if
it preserves the stated invariants and passes the named suites.

### Runtime-to-Gateway protocol

- **Contract.** `ProviderGatewayService.StreamProviderRequest(ProviderRequest)
  returns (stream ProviderStreamEvent)`, defined in
  `proto/tetral/provider_gateway/v1/provider_gateway.proto`. Pure validators
  `validateProviderRequest` / `validateProviderStreamEvent` live in
  `packages/protocol/src/bounds.ts`; the binding-token verifier lives in
  `packages/protocol/src/binding-token.ts`.
- **Lifecycle.** `ProviderRequest` is a complete snapshot for one turn; there is
  no cross-turn protocol state.
- **Invariants.** `ProviderRequest` has no credential field, by design. The
  request channel is pinned at 64 MiB at both ends and is exercised with the
  1,050,000-token catalog-capacity vectors, including escape-dense tool-result
  history. Provider stream events have an
  independent 32 MiB encoded protobuf carrier, prechecked before send. The response
  server explicitly sets the native HTTP2 session accounting budget to 256 MiB;
  grpc-js's MAX_SAFE_INTEGER default resets multiplexed large responses on declared
  Bun 1.3.14 in the bounded primitive control. This budget is not a JavaScript heap/RSS cap. Complete text
  retains the 16 MiB canonical JSON limit, tool input 4 MiB, metadata 16 KiB, and
  reasoning 16 parts / 2 MiB per request. These shared contracts are defined once in
  `packages/protocol/src/content-limits.json`, imported by TS and checked against
  the Go Bridge projection. Provider deltas and raw
  provider chunks are hot-only and never forwarded; the gateway never writes
  events, messages, or usage; credentials never reach Runtime or Bridge; a
  deterministic request-shape rejection is `INVALID_ARGUMENT` and non-retryable.
- **Conformance.** `packages/protocol/test/unit`, plus
  `packages/provider-gateway/test/unit/service.test.ts`,
  `grpc-server.test.ts`, `bounds.test.ts`.

### Provider lowering rules

- **Contract.** `ProviderRules` interface (`packages/lowering/src/rules/rules.ts`)
  with one implementation per provider (`anthropic.ts`, `openai.ts`,
  `deepseek.ts`, `moonshotai.ts`, `zai.ts`; registry in `rules/index.ts`). The
  interface is a fixed set of dimensions — reasoning handling, tool-call-id
  scrubbing, cache-control, media support, effort, request/tool options,
  provider options, headers, sampling, output-token strategy (`clamp`/`omit`),
  schema strategy (`passthrough`/`openai-codex`/`moonshot`), request output
  schema, and provider-specific error rules. The transforms run in
  `lowerProviderRequest` (hop ①), `ProviderStreamRaiser` (hop ①′),
  `normalizeProviderUsage`, and the `errors.ts` classifiers.
- **Lifecycle.** Pure functions with no SDK and no network — the
  `@tetral/gateway-lowering` package declares no gRPC, Postgres, or network
  dependency; purity is enforced by the package boundary.
- **Invariants.** No invention: every rule cell is anchored to an upstream
  behavioral reference or an explicit protocol mandate. The transform order —
  base render → media/unsupported-parts → history normalization → wrapping and
  caching → capability switches, with sampling and schema computed alongside — is
  normative; reordering changes wire bytes. Reasoning provenance metadata
  round-trips byte-exact so cold reloads do not downgrade reasoning. Usage
  raising splits by wire family (anthropic-wire vs openai-wire) — getting the
  split wrong silently corrupts one family's usage. One Runtime Assistant
  context entry lowers to one provider Assistant message: ordered text,
  reasoning, and concurrent Tool Calls stay grouped, while Tool Results use the
  provider protocol's result messages. Provider-required Tool-call-ID scrubbing
  is deterministic and one-to-one. Anthropic collisions retain the first
  scrubbed ID, then allocate `_2`, `_3`, and later decimal suffixes while
  truncating the base as needed to keep the 128-character limit; this never
  splits the Assistant message. Tool cancellation lowers only the exact
  `{type:"cancelled"}` conversation result. The seven-model set is closed.
- **Conformance.** `packages/lowering/test/unit/*-request.test.ts` (per-rule,
  table-driven), `stream.test.ts`, `usage.test.ts`, `errors.test.ts`,
  `rules-invariants.test.ts`; `packages/lowering/test/rules-coverage.test.ts`
  enforces that every rule id has a matching test name; the golden wire suite
  pins outbound bytes.

### Model catalog

- **Contract.** `GatewayModelCatalog`, `lookupGatewayModel`, and
  `routeEffectiveGatewayModelLimits` in
  `packages/provider-gateway/src/providers/catalog.ts` — the seven
  `(provider_id, model_id)` pairs, each with supply mode, `platformHosted` flag,
  base URL, and per-model output/context limits. `finish` carries the
  route-effective limits so the runtime learns the route it is actually on (the
  same model can carry different effective windows on different supply routes).
- **Lifecycle.** Static registry read at request time.
- **Invariants.** Closed set — a model absent from the catalog fails closed; the
  catalog is defense in depth (admission also pins the set). Adding a supply mode
  is a code change, not configuration; no dynamic catalogs, on-demand installs,
  or plugin hooks.
- **Conformance.** `packages/provider-gateway/test/unit/catalog.test.ts`.

### Credential resolution and decryption

- **Contract.** The resolver and `GatewayCredentialStore` in
  `packages/provider-gateway/src/providers/credentials.ts`; AES-256-GCM decrypt
  in `packages/provider-gateway/src/providers/crypto.ts`
  (`decryptAES256GCM`/`encryptAES256GCM`). The framing must match the Go writer
  at `internal/encryption/aesgcm.go`: 12-byte random nonce prefix + 16-byte
  tag suffix, no AAD, raw BYTEA. Reads `session_provider_auth` keyed on
  `(workspace_id, session_id)`.
- **Lifecycle.** One credential read per turn; the single sanctioned durable
  write is OAuth rotation write-back
  (`providers/openai-oauth-refresh.ts`) under a row-level single-flight lock with
  a compare-and-set precondition.
- **Invariants.** The full fail-closed enumeration —
  missing/revoked/archived/wrong-provider/undecryptable/expired-refresh-failed,
  rotation write-back permanently failed, and no-credential on a non-hosted
  provider — each maps to a bounded public `provider-error` with `retryable =
  false` and leaks no internal step. A session credential never falls back to
  platform access. Plaintext lifecycle per the responsibilities note.
- **Conformance.** `credentials.test.ts`, `credentials-postgresql.test.ts`,
  `openai-oauth-refresh.test.ts`, and `leak-guards.test.ts` (sentinel scan of
  every captured log/event/error channel).

### Platform key pool

- **Contract.** `PlatformKeyPool` and the failure classifier in
  `packages/provider-gateway/src/providers/pool.ts`, reading
  `platform_provider_keys` (encrypted key, weight, priority, one `cache_scope`
  per provider). The operator tool that populates the table is a separate
  deliverable script.
- **Lifecycle.** Whole-pool read on a 30 s cache; per-replica cooldown/quarantine
  memory is the only pool state in the process.
- **Invariants.** The gateway is read-only on the table. All active keys of one
  provider must share one `cache_scope` (provider caches are scoped to
  workspace/organization, not to the key) or the process refuses to start — a
  cross-scope pool destroys cache hits. No user-visible failure occurs while a
  healthy key exists. User-credential sessions never enter the pool.
- **Onboarding a platform-hosted provider.** Only relevant when the new model's
  provider is `platformHosted`. The operator script populates
  `platform_provider_keys`, so extending it to a new provider means seeding that
  provider's keys with a chosen `cache_scope` — pick the scope the provider's
  prompt cache is keyed on (workspace or organization) and use it for every key
  of that provider, since a cross-scope pool refuses to start. The
  `provider_id` `CHECK` on the table must already have been widened.
- **Conformance.** `packages/provider-gateway/test/unit/platform-pool.test.ts`,
  `platform-key-cli.test.ts` (operator-tooling ↔ runtime decrypt round-trip).

### SDK clients and provider transports

- **Contract.** `ProviderClientRegistry` in
  `packages/provider-gateway/src/providers/clients.ts` constructs the AI SDK
  instance and injects a custom `fetch` (header + inter-chunk timeouts, abort →
  body-stream error, egress allowlist, and manual redirect following with
  cross-origin credential stripping). The one divergent transport is
  `providers/openai-oauth.ts` (authorization swap, subscription-URL rewrite,
  system text carried as the call's `instructions`).
- **Lifecycle.** One SDK stream per attempt; SDK client retries are disabled.
  The native iterator consumes terminal EOF and awaits closure on exit. Provider
  body cancellation is joined and releases its reader lock; no unread response
  body remains owned after cleanup.
- **Input scheduling.** The response wrapper reads on demand and forwards each
  source chunk in ordered segments of at most 16 KiB. At pull boundaries it yields
  to the event loop after 256 KiB, 128 segments, or 8 ms, so already-buffered SDK
  input still gives cancellation and timers an opportunity to run. These are
  scheduling checkpoints, not a latency guarantee or an admission limit. Only a
  new source read resets network inactivity; segmenting an existing chunk does
  not. Cancellation clears the pending chunk and joins reader cancellation.
- **Write custody.** Cancellation, transport error, and the absolute deadline stop
  further frames promptly. A submitted frame stays charged until its callback
  settles or the actual Writable closes; successful delivery also requires any
  needed drain. Handler and server shutdown join these custody promises. A close
  witness releases application ownership; it does not claim peer consumption.
- **Provider conversion and retention.** `providers/model-stream.ts` calls the pinned
  official `LanguageModelV3.doStream` adapters directly. It converts resolved
  prompts, declarations, schemas and provider options, safely parses Tool inputs,
  and translates usage and signed reasoning events one at a time. Unused SDK
  recorded content, step results and stream tees are absent. Completed content
  and record counts are diagnostics, not request-lifetime rejection rules.
  `providers/block-assembler.ts` coalesces text/reasoning segments and bounds live
  content, open blocks, identities, segments and request-wide reasoning. Existing
  canonical per-block, Tool and metadata bounds still apply. Required metadata
  overflow fails explicitly rather than silently dropping a signature.
- **HTTP ownership.** `providers/transport.ts` uses pinned official Undici's public
  dispatcher request API and its pinned experimental decompression interceptor.
  Response reads follow downstream demand, including gzip/error bodies; they do
  not pump unread response history into Bun's native fetch buffer. Egress,
  credentials, OAuth rewrites, deadlines and manual redirects remain owned by
  the client wrappers. Discarded redirect bodies are cancelled and joined.
  Shutdown closes the process-owned dispatcher after request operations join,
  before SQL closes. This changes neither deployment settings nor admission.
- **Operating policy.** Constructor-injected active assembly bounds are owned by
  `providers/resource-policy.ts`. Resource exhaustion produces a fatal,
  nonretryable request error, never a truncated successful block. No raw-record,
  completed-content history or attachment-envelope limit is introduced here.
- **Observations.** The service emits content-free `provider.stage_completed`
  samples for dispatch → first private content fragment, first fragment → first
  complete semantic frame, and complete frame → local write callback. Each sample
  carries a closed outcome/content kind, duration, canonical bytes, and encoded
  frame bytes. Missing-content failure/cancellation samples retain the denominator.
  Live assembly gauges and pending complete-frame bytes distinguish ownership
  release from allocator RSS. Frame ownership lasts through callback and any
  required drain. Logging and metric sink failures cannot affect delivery.
- **Liveness.** First-event and inter-event watchdogs bound transport stalls.
  Independently, a 60-second semantic-progress watchdog is measured from the
  last non-empty text/reasoning delta or Tool Call/input delta. Metadata,
  keepalive traffic, and start/end markers do not re-arm it; expiry uses the
  existing retryable provider-stream timeout projection.
- **Invariants.** Raw-wire access is confined to the enumerated file-and-purpose
  points — a raw-wire mutation elsewhere is a boundary violation. Provider
  fetches may target only catalog base URLs plus the OAuth issuer/subscription
  endpoints (the app-layer allowlist, fully separate from the web tool's
  SSRF classification, which lives in the `web-connector` service). AI SDK
  versions are pinned; workspace overrides keep the patched provider utilities
  and their matching provider types consistent across the pinned adapters. Abort
  must deterministically error the stream so the official adapter stream cannot hang.
- **Conformance.** `packages/provider-gateway/test/unit/clients.test.ts`,
  `packages/provider-gateway/test/unit/provider-response-bounds.test.ts`
  (buffered SDK responses reject an advertised size above the upstream cap and
  cancel the body; adapter error paths preserve safe retryable failures),
  the golden wire suite
  (`packages/provider-gateway/test/golden/*` — captured outbound request bytes
  and headers, plus recorded SSE replay per provider including cache-hit usage
  numbers), and the cancellation/timeout cases in `service.test.ts` /
  `grpc-server.test.ts`.

### Attachment resolution

- **Contract.** `BridgeAPIAttachmentResolver`
  (`packages/provider-gateway/src/attachments.ts`) resolves each
  `ProviderRequestAttachment` before lowering: transient refs (tool-produced
  media) through Bridge with the full scope quadruple validated server-side;
  user-supplied file-backed pairs through Bridge (the resolve owner) in two
  phases (a zero-byte metadata preflight, then bounded offset-addressed chunk
  reads) with the byte envelope gated on the summed metadata.
- **Lifecycle.** Read-only resolution per turn; the gateway holds no Files
  credentials and GCs nothing.
- **Invariants.** At most 32 references per request, both origins counted
  together. A dead or over-envelope reference is dropped per-ref and reported on
  the pre-stream `attachment-rejections` event (the provider call proceeds with
  the valid subset); a resolver infrastructure outage stays whole-request
  retryable; a stored blob that disagrees with its durable record is a fatal
  integrity error, never a silent drop.
- **Conformance.** `packages/provider-gateway/test/unit/attachments.test.ts`.

### Startup schema verification

- **Contract.** `verifyPostgreSQLReadiness` in `packages/schema/src/verify.ts`
  checks the migration stamp, exact live Workspace-RLS catalog, and effective
  serving role after SQL-client construction and before any SQL-backed store or
  resolver is built.
- **Invariants.** The separate migration owner constructs schema. The
  `provider_gateway` and `mcp_connector` serving roles are distinct,
  NOSUPERUSER and NOBYPASSRLS; both fail closed through a
  stable error that retains no role name, DSN, credential, or driver text when
  the stamp, live policies, or role posture differs from the repository-owned
  contract.
- **Conformance.** `packages/provider-gateway/test/unit/schema-startup.test.ts`,
  and `static-boundaries.test.ts` for the cross-package import guardrails.

## Testing guide

| Suite | Proves |
| --- | --- |
| `packages/lowering/test/unit/*-request.test.ts` | Each provider's request-lowering rule cells (table-driven, one case set per rule id) |
| `packages/lowering/test/unit/stream.test.ts` | SDK stream part → private normalized record mapping, dropped parts, ordering/terminal negatives |
| `packages/lowering/test/unit/usage.test.ts` | Usage normalization including the anthropic-wire vs openai-wire family split and cache-hit numbers |
| `packages/lowering/test/unit/errors.test.ts` | Error classification and the retryable overrides (5xx, in-stream 5xx, context overflow, entitlement) |
| `packages/lowering/test/rules-coverage.test.ts` | Every enumerated rule id has at least one test — the matrix-to-test mapping, enforced by CI |
| `packages/provider-gateway/test/golden/*` | Byte-level outbound request (cache placement, thinking envelopes, `store:false`/encrypted include, beta headers, OAuth swap+rewrite, schema surgery, absent/omitted fields) and SSE replay → event sequence with usage |
| `credentials*.test.ts`, `openai-oauth-refresh.test.ts` | Fail-closed enumeration, positive resolution to the provider-native credential header, Go↔TS decryption round-trip, OAuth single-flight refresh and rotation CAS |
| `platform-pool.test.ts`, `platform-key-cli.test.ts` | Body-level classification, cool/quarantine transitions, pre-first-byte failover and switch cap, cooldown clamp, weighted selection, cache-scope startup refusal, operator-CLI round-trip |
| `leak-guards.test.ts` | No key/token sentinel appears in any captured log, event, or error payload |
| `service.test.ts`, `grpc-server.test.ts` | End-to-end streaming turn, drain backpressure, cancellation, header/inter-chunk and semantic-progress timeouts, admission cap, and the Bun/grpc-js tripwire (many private records → complete frames + trailers + clean status) |
| `http-server.test.ts` | Ops-route responses and readiness-first graceful shutdown |
| `attachments.test.ts` | Transient and file-backed resolution, per-ref rejection reporting, and the integrity-mismatch fatal path |
| `schema-startup.test.ts`, `static-boundaries.test.ts` | Migration-registry verification and cross-package boundary guards |

Run from `services/gateway`. The whole workspace suite (both workload packages
plus the shared packages) is `bun run test` — the `test` script in
`package.json`, the same set the `gateway-ts` CI job runs. For a single suite,
`bun test` filters by path substring per package (e.g. `bun test
packages/provider-gateway/test/unit/service.test.ts`), matching the
`agent-runtime` idiom. Golden fixtures are checked in; regeneration is an
explicit reviewed action, never a side effect of a failing run.

## mcp-connector

### Responsibilities

`mcp-connector` is an independent TypeScript/Bun workload in this workspace. It
terminates tool calls of `kind = mcp` on its own gRPC port (`McpConnectorService`,
defined beside the provider service in
`proto/tetral/provider_gateway/v1/provider_gateway.proto`), holds the MCP client
sessions to registered GitHub and Slack server adapters, discovers each server's
tools, resolves the per-call MCP credential from the session's vault, executes
the tool, maps the result through a closed content formatter, and classifies
failures into a bounded taxonomy. It contains no provider-lowering code and never
talks to a model provider. It owns no writable store: replay records, transient
attachments, and durable manifests are all Bridge-owned, and the connector is
read-only on the attachment store — the single durable write it performs is the
single-flight OAuth refresh write-back to one `credentials` row (workspace +
vault + credential scoped). Its files sit under `packages/mcp-connector/src`; the
package depends only on `packages/protocol` and is statically forbidden from
importing `packages/lowering` or `packages/provider-gateway`
(`static-boundaries.test.ts`).

The immutable adapter registry (`packages/mcp-connector/src/adapters/registry.ts`)
admits GitHub at `https://api.githubcopilot.com/mcp/` and Slack at
`https://mcp.slack.com/mcp`. Endpoint matching removes at most one trailing slash;
it preserves case, authority, query, fragment and path spelling. Adding a server
requires a registered adapter. Adapters own endpoints and service headers; the
generic client owns bearer Authorization; the pinned SDK owns MCP protocol and session headers.

`SQLMcpServerResolver` reads Workspace-scoped
`sessions.installed_tools_json.mcp_servers`. Configured names such as `work-slack`
are independent of adapter IDs and tool names. One attempt binds that resolved
endpoint to credential selection, refresh and transport construction. GitHub's
adapter pins `X-MCP-Toolsets: default,actions`; Slack sends no GitHub header.
Every new transport combines adapter headers with Vault bearer authorization.

`default` is GitHub's supported alias for its baseline issue/PR toolsets, so
those tools stay available without enumerating constituents; `actions` adds
exactly the four Actions tools: `actions_list` (list workflows, runs, jobs, and
artifacts), `actions_get` (workflow/run/job details, artifacts, usage and
log-download information), `get_job_logs` (job or failed-job logs), and
`actions_run_trigger` (start a workflow, rerun a run or its failed jobs, cancel
a run, delete run logs). Tool availability is not scheduling: the connector adds
no automatic CI retry or deployment policy, and every Actions call flows through
the same `RunMcpTool` pipeline as any other MCP tool.

### States & lifecycle

#### `RunMcpTool` turn (`packages/mcp-connector/src/service.ts`)

`McpConnectorServiceShell` drives one tool call as an ordered pipeline, with a
Bridge-backed durable reservation bracketing the external side effect. The
`RunMcpTool` caller is the Runtime pod; `ListMcpTools` is called by Bridge
(connector-change discovery) and Job Runner (initial discovery). All caller
identities are TokenReview-authenticated (`KubernetesTokenReviewClient`,
`packages/mcp-connector/src/auth.ts`); `RunMcpTool` additionally verifies the
per-thread runtime binding token.

| Stage | Action | Failure |
| --- | --- | --- |
| Authenticate | TokenReview the Runtime workload token, then verify the binding token | gRPC `Unauthenticated` / `PermissionDenied` before any side effect |
| Validate | `validateRunMcpToolRequest` (`bounds.ts`) | gRPC `INVALID_ARGUMENT` |
| Claim | Create one execution-attempt `claimId`, then call `ClaimMcpToolResult` with `(scope, tool_use_event_id, claimId)`; Bridge loads the durable server, tool, and canonical input and compares its normalized hash internally | same-claim replay renews the lease; an unexpired different claim remains in flight; an expired lease admits a new claim; a terminal result replays directly |
| Resolve Server + credential | Match one session-vault credential (table below) | fail closed, no MCP call is made |
| Establish + execute | Complete SDK connect and every discovery page before caching or calling; call within `MCP_CALL_TIMEOUT_SECONDS` (120) | reconnect/auth policy below; timeout → `mcp_timeout` |
| Format | `formatMcpToolResult` → `result_text` + at most one attachment, decoded bytes held in memory only | bounds rejection before commit |
| Commit | `CommitMcpToolResult` with the same `claimId` plus result/media; Bridge fences the current claimant, creates transient-attachment rows, and persists the refs-only result in one transaction | stale claimant → custody lost; post-effect commit failure → retryable `runtime_error` |
| Relinquish | `RelinquishMcpToolResult` with the same `claimId`, only after a deterministic post-acquisition failure has proved no result commit is uncertain | exact active claim is deleted and may be immediately reacquired; stored/different claims return stale; lost ACK replays duplicate |

Local execution ownership is keyed by `(Tool target, claimId)`, never by the
Tool target alone. Every post-acquisition validation and external execution is
inside exact-claim cleanup: deterministic rejection terminally settles or
relinquishes that claim, and an expired-lease takeover cannot be suppressed by
an older uncertain attempt.

`MCP isError: true` maps to `status = tool_error` with the formatted error as
`result_text` so the model can self-repair; transport/auth failures after the
reconnect policy map to `status = runtime_error`.

#### Credential resolution by vault match (`packages/mcp-connector/src/credential.ts`)

`SQLMcpCredentialResolver` searches the session's immutable vault set for a
credential whose `auth_public_json.mcp_server_url` equals the resolved registered endpoint
(single-trailing-slash normalized). Eligible auth types are `mcp_oauth` and
`static_bearer`; archived credentials are treated as absent. A "usable"
credential decrypts and is either unexpired or refreshable. Resolved material is
delivered as `Authorization: Bearer <token>`.

| Match outcome | Resolution |
| --- | --- |
| zero matching | fail closed `credential_required` — no credential exists to try |
| exactly one, usable | use it |
| two matching | fail closed `ambiguous` |
| matching but archived / wrong `mcp_server_url` | treated as absent (folds into `credential_required`) |
| matching but undecryptable | fail closed `undecryptable` |
| `mcp_oauth` expired, refresh block present | single-flight refresh, then use |
| `mcp_oauth` expired, refresh fails | fail closed `refresh_failed` — the row is not mutated |

The bounded error set is `McpCredentialError` (`credential_required |
ambiguous | undecryptable | expired | refresh_failed`); an uncaught
selection-query failure is outside it. Decrypted plaintext exists only in process
memory and inside TLS to the server — never logged, never returned to Runtime,
never persisted here.

#### Single-flight OAuth refresh (`packages/mcp-connector/src/credential-update-path.ts`)

OAuth refresh tokens can rotate on use, so two consumers must never burn one
rotating token concurrently. `SQLVaultMcpCredentialUpdatePath` serializes
refresh on a `SELECT … FOR UPDATE` (or advisory lock) keyed by `(workspace_id,
vault_id, credential_id)` for the refresh HTTP call plus write-back. Losers block,
re-read the row, and use the newer material without refreshing once `expires_at`
has moved forward. Proactive refresh at resolution triggers only when `expires_at
<= now + REFRESH_SKEW_SECONDS` (60 s, `credential-constants.ts`); a reactive path
(typed HTTP 401/403) invokes one single-flight refresh regardless of expiry. A
refresh failure marks the resolution `refresh_failed` and leaves the row
unmutated. Each cold operation permits one handshake refresh and one operation refresh.
Cold execution shares its operation allowance between readiness listing and the
call; spending it during listing forbids another call refresh. Rebuild after an
operation refresh permits no further refresh, including during connect/list.
Pending initialization shares spent allowances with every waiter, including
late joiners; a new independent operation on a ready entry gets a fresh operation
allowance. OAuth uses the configured token endpoint and confidential-client
policy. An HTTP-200 `{ok:false}` token response fails without row mutation.

#### MCP client connection (`packages/mcp-connector/src/client.ts`)

`McpSDKClient` uses the pinned SDK's `StreamableHTTPClientTransport` and SDK
output validators. Connect plus a successful all-page listing is the readiness
barrier. Cache identity is `(workspace_id, session_id, configured_server_name,
vault_id, credential_id, sha256(token))` with unambiguous framing. An installed
endpoint change retires a mismatched ready/pending entry even under the same key.
New credential material also retires superseded pending initialization in that
scope; its waiters fail with `mcp_connection_failed` without replay, while aliases
of the same refreshed opening keep their shared ownership and spent allowances.
Concurrent openings coalesce. Each waiter owns its cancellation/deadline; one
leaving preserves initialization for others, while all leaving aborts it. Failed
initialization never publishes a ready client. Idle expiry closes and evicts.
Only the SDK retains output schema metadata; full definitions remain local to
initialization, explicit discovery or manifest reporting. Warm calls do not list.
A native SDK request timeout retires the transport when that execution is its
sole owner, even if the total execution timer has not fired. Warm discovery and
other concurrent operations retain ownership of their shared ready client.

| State | Trigger | Transition |
| --- | --- | --- |
| Establish | first use (discovery or first call) | `initialize` handshake within 10 s plus complete listing before readiness |
| Idle close | `MCP_SESSION_IDLE_SECONDS` (1800) without a call | close the client |
| Reconnect | connection loss | 3 attempts, backoff 1 s / 4 s / 16 s (`MCP_RECONNECT_DELAYS_MS`, `MCP_RECONNECT_MAX_RETRIES`); in-flight call reports `retry_status: retrying` then `exhausted` |
| Auth retry | typed HTTP 401/403 or pinned SDK auth error | bounded handshake/operation allowances above; repeated auth failure is terminal |
| Exhaustion settlement | terminal reconnect exhaustion | the connector's own `Client.onerror` synthesizes `mcp_connection_failed` / `retry_status = exhausted`, settles every in-flight call on that client exactly once, evicts the cached entry, and clears the idle timer; late responses are ignored |

With default ceilings and sufficient shared/caller time, exhaustion settlement
(~21 s after loss) precedes the 120 s call ceiling. A shorter shared budget or
caller deadline can expire sooner with `mcp_timeout`. The first terminal
outcome determines the classification. The SDK fires `onerror` but never `onclose`, so a client→entry
index performs the eviction; without it the dead client would stay cached.

#### Durable idempotency and reservation lease

The connector owns no replay store; records live in the Bridge-owned
`session_runtime_tool_results` table (`tool_kind = mcp`) reached through three
TokenReview-authenticated Bridge RPCs the `tetral-system/mcp-connector`
ServiceAccount may call
(`BridgeAPIMcpToolResultIdempotencyStore`, `packages/mcp-connector/src/bridge-client.ts`).
`CommitMcpToolResult` persists the refs-only result and, in the same Bridge
transaction, creates the transient-attachment rows from a bounded inline-media
leg — so attachment creation and commit cannot be split by a crash and no orphan
row can outlive a failed commit.

| Claim outcome | Settlement |
| --- | --- |
| stored result, hash match | replay it; no MCP call is made |
| stored result, hash mismatch | fatal tool-delivery conflict (`mcp_claim_conflict`) |
| live unexpired reservation | `mcp_in_flight`, retryable `runtime_error` |
| none | insert the reservation (create-only), execute |

`MCP_CLAIM_LEASE_SECONDS` (180) bounds a reservation so a connector crash mid-call
cannot strand the call. Two properties keep the fence honest: the Claim RPC
carries `MCP_CLAIM_RPC_TIMEOUT_MS` (below the lease) so a delayed acknowledgement
becomes `DEADLINE_EXCEEDED` rather than acting on a superseded reservation; and
Commit fences on the reservation owner, so at most one result is ever persisted.
A monotonic 170-second preparation/execution budget starts before Claim dispatch
and covers credentials, connect, discovery, synchronous manifest preparation,
call and formatting. Every phase/retry is clipped by its phase ceiling and the
remaining shared budget; shorter caller deadlines are preserved. Expiry stops
new external dispatch and cancels owned work. The first Commit attempt retains a
10-second reserve within the 180-second lease. A lost Commit ACK continues the
same immutable result/claim receipt recovery beyond that reserve; it never grants
another external invocation or extends the lease. On replay a media attachment whose transient ref is
no longer resolvable renders an omission line `[MCP attachment unavailable:
<mime> (<size>)]` rather than serving stale bytes.

A finite-bound `RunMcpTool` caller cancellation settles `mcp_timeout`, including
early abandonment while the reconstructed server deadline still has time.
Transport cancellation carries no client reason; settlement retains the original
claim and does not authorize another external invocation.

#### Discovery and manifest delivery

The connector alone can reach the server, so it **produces** the manifest; Bridge
**accepts and enqueues** it, and Job Runner **delivers** it (Bridge and Job Runner
can reach the MCP Connector; the connector cannot reach Runtime). `ListMcpTools` returns each tool's `{name, description, input_schema}`
verbatim, plus a `manifest_etag` (content hash via `manifestEtag`) and
`omitted_tools` (platform-tool name collisions the connector filtered via
`filterManifestTools`, `reason = builtin_name_collision`). Bridge captures the
manifest to a durable `session_mcp_manifests` row before enqueuing delivery, assigns a
monotonic `manifest_generation`, and enforces the 256 KiB per-server manifest
bound at acceptance. Supersession keys on generation monotonicity, never on etag
inequality — a flapping A→B→A etag must not clobber newer state — and the etag is
identity-only, so the family-filtered delivered subset need not re-hash. At
execution-created readiness, the Connector client reports its complete
operation-local list after publication; every `tools/list_changed` notification triggers a re-list, and each
successful re-list is reported to Bridge even when its etag matches an earlier
notification. Bridge verification can re-list the published client without initializer recursion. The initial upstream list precedes notification retry and is
non-mutating on failure. The connector retries a within-cap notification with 4
total attempts (`MCP_MANIFEST_NOTIFY_RETRY_DELAYS_MS`, 1 s / 4 s / 16 s). Notify
exhaustion is a structured connector log only, never a readiness flip; the next
notification re-triggers. Bridge alone decides whether the durable manifest is
current, requires a readiness restore, or advances generation. Bridge treats a
durably committed over-cap transition
as terminal and returns it without connector retry. The durable row carries a
`(readiness, diagnostic)` pair orthogonal to content: an over-cap manifest is
written `unready` and contributes no tools while its last-accepted content is
preserved; notification refresh failure leaves the row and Queue unchanged. Restore is
readiness-aware, so a re-notify matching the stored etag while `unready` is a
restore (not a duplicate no-op).

The connector invokes inherited SDK `listTools()` once per complete discovery.
`DiscoverySDKClient` overrides the public `request()` method for `tools/list`
only: each page uses `super.request()`, validates the response, and appends to a
request-local array. The complete result returns to SDK `listTools()`, which
updates output validators and task metadata once for the entire directory. A
failed page leaves the previous SDK metadata intact. Other protocol methods
retain the SDK implementation; no private SDK fields are modified. SDK clients
remain isolated by workspace, session, server, credential identity and token.

Each discovery starts cursorless and follows every present opaque `nextCursor`,
including an empty string, until the field is absent. Repeated cursors, more
than 100 pages or 1024 tools, and raw definitions exceeding 1 MiB fail discovery.
UTF-8 JSON bytes are counted while accumulating tools, including SDK-only
metadata. This limits retained definitions; it is not a hard limit on the HTTP
body being parsed. Bridge separately limits its canonical projection to 256 KiB.
The shared 120-second discovery deadline includes credential resolution,
connection, all pages and bounded authentication refresh. A caller's (Bridge or
Job Runner) gRPC deadline can shorten it; cancellation stops further page requests without closing the
shared client or canceling unrelated calls. Authentication restart begins again
without a cursor, within the same deadline.

Protocol/bound failures use a discovery-specific error and the existing typed
`manifest_invalid` trailer, with safe reason/page/tool counts in operator logs.
SDK output-schema rejection settles non-success without refresh or re-execution. JSON-RPC 401/403 and SDK validation errors containing those numbers are not authentication provenance; HTTP 500 bodies mentioning 401 are not authentication. JSON-RPC invalid-parameter errors retain their original classification; an
unregistered-endpoint rejection or rejected request is not logged as a pagination-bound
violation. Discovery timeouts name tool discovery, including SDK timeout paths;
tool-call timeouts continue to name the tool call. During discovery, terminal or
exhausted connection failures map to the `server_unavailable` trailer, including
unavailable credential refresh. These end one connector operation; Bridge owns
the separate input-level retry budget.
Bridge owns whole-discovery retries for an input and the final decision to
execute or fail that input; the connector never presents a partial directory as
successful. SDK metadata update and Bridge's later database acceptance are
separate commits, not a distributed transaction. A transport failure during a
refresh preserves the previously accepted durable manifest; existing invalid
or over-cap acceptance remains fail-closed. No discovery retry is added to
`tools/call` or Actions workflow writes.

Tool selection is independent of call authorization. The three Actions read
tools are published upstream with public-read visibility, so a credential
without `repo` scope does not hide their definitions; any call — including any
`actions_run_trigger` operation — can still be denied by GitHub at execution
time, and that denial returns as a model-visible `tool_error` result, never a
success. The connector performs no Actions permission preflight and infers no
missing scope from an absent tool.

The explicit GitHub/Slack live runner and environment adapter contract are
[documented separately](packages/mcp-connector/test/live/README.md). Its offline
validation is part of local checks; actual deployment binding and external
execution supply separate live evidence.

#### Tool-system mapping

| Concern | Rule |
| --- | --- |
| Definition | MCP `{name, description, inputSchema}` verbatim |
| Route | `gateway`, `kind = mcp` |
| Scheduler | `parallel_safe`, no conflict key (server-side effects remain the external service's concurrency domain) |
| Name collision | the connector filters platform-tool collisions into `omitted_tools` and logs a warning; family-builtin collisions are Bridge's to filter, so the connector stays family-blind |
| Schema | the same per-provider schema transform every tool gets in lowering; no MCP-specific branch |

#### Result formatter (`packages/mcp-connector/src/formatter.ts`, closed table)

`formatMcpToolResult` maps the SDK `CallToolResult` content union. A result
carries at most one media attachment with aggregate decoded bytes ≤
`MCP_BLOB_MAX_BYTES` (10 MiB); `result_text` is capped at
`MCP_RESULT_TEXT_MAX_BYTES` (50 KiB) / `MCP_RESULT_TEXT_MAX_LINES` (2000) with a
truncation marker.

| Content item | Mapping |
| --- | --- |
| `text` | appended to `result_text` |
| `image` | decoded and bounded, carried to Bridge for the in-transaction transient write, surfaces to Runtime refs-only; a placeholder line names it |
| `resource` with `text` | appended to `result_text` |
| `resource` with `blob`, mime ∈ `MCP_ATTACHMENT_MIME_ALLOWLIST`, size ≤ 10 MiB | refs-only attachment, same commit-carried path as `image` |
| `resource` with `blob`, otherwise | omission line `[Binary MCP resource omitted: … ]` |
| any other content type | omission line `[Unsupported MCP content omitted: <type>]` |

`structuredContent` folds into `result_text` as canonical JSON **only when
`content[]` produced no text** (servers mirror it as a `text` block for backward
compatibility, so appending unconditionally would duplicate it).

#### Error taxonomy (`packages/mcp-connector/src/errors.ts`, closed)

`McpConnectorErrorCode` is a closed union mapped to the protocol enum by
`mcpErrorKind`. Every `RunMcpTool` produces exactly one terminal record;
`mcp_in_flight` is its own kind and is never logged as `mcp_connection_failed`.

| `error_kind` | Trigger | Delivery |
| --- | --- | --- |
| `mcp_tool_error` | MCP `isError: true` | `tool_error` result — model-visible |
| `mcp_invalid_input` | server rejects arguments (JSON-RPC invalid params) | `tool_error` result — model-visible |
| `mcp_connection_failed` | reconnect exhausted / terminal | `session.error` wrapping `mcp_connection_failed_error`; call settles `runtime_error` |
| `mcp_authentication_failed` | auth retry failed (an existing credential was rejected) | `session.error` wrapping `mcp_authentication_failed_error`; call settles `runtime_error` |
| `mcp_credential_required` | zero matching credential to try | `session.error` wrapping `mcp_authentication_failed_error` with `retry_status = terminal`; call settles `runtime_error` |
| `mcp_timeout` | shared preparation/execution allowance or an owning phase ceiling exhausted, or a finite-bound execution caller expired or abandoned its request (including before external dispatch) | `tool_error` result naming the timeout |
| `mcp_claim_conflict` | Claim stored-result hash mismatch | `session.error` wrapping `unknown_error` with `retry_status = terminal`; call settles `runtime_error` |
| `mcp_in_flight` | live unexpired reservation on claim | retryable `runtime_error`, no `session.error` |
| `mcp_commit_failed` | post-effect Commit/store failure after the side effect ran | retryable `runtime_error`, no `session.error` |
| `mcp_custody_lost` | Claim or Commit finds that the requesting Runtime binding or process no longer holds custody (stale custody) | `runtime_error` with no `retry_status` and no `session.error`; Runtime treats the response as stale custody and does not settle the call |
| `mcp_internal_error` | an unclassified connector-side exception during execution | retryable `runtime_error`, no `session.error`, no `retry_status` |

#### Event mapping

For tool execution, Runtime Core writes the public events; the connector supplies `mcp_server_name`,
`retry_status`, and result payloads through the `RunMcpTool` envelope. A gated
call emits `agent.mcp_tool_use`; the settlement emits `agent.mcp_tool_result`
linked by `mcp_tool_use_id`. Each error surfaces as `session.error` wrapping a
fork-SDK inner member (`mcp_connection_failed_error`,
`mcp_authentication_failed_error`, or `unknown_error`), always additive to — never
a substitute for — the exactly-one `agent.mcp_tool_result` settlement. The public
wire carries no field distinguishing a missing credential from a rejected one;
both settle terminal and both demand the same client action (fix the GitHub Vault
credential), so the distinction lives only on the internal `error_kind`, the log,
and metrics.

### Seams

Each boundary below is independently replaceable; a replacement is conformant if
it preserves the stated invariants and passes the named suites.

#### `McpConnectorService` gRPC surface

- **Contract.** `RunMcpTool(RunMcpToolRequest) returns (RunMcpToolResponse)` and
  `ListMcpTools(ListMcpToolsRequest) returns (ListMcpToolsResponse)` in
  `proto/tetral/provider_gateway/v1/provider_gateway.proto`; request/response
  bounds validators live in `packages/mcp-connector/src/bounds.ts`.
- **Lifecycle.** Each call is a complete snapshot for one tool use; there is no
  cross-call protocol state. Ops plane is a bare Bun HTTP server on a separate
  port (`http-server.ts`).
- **Invariants.** `RunMcpTool` is caller-authenticated as the Runtime pod plus a
  binding-token check; `ListMcpTools` is caller-authenticated as Bridge or Job Runner for discovery only; identity
  failures are gRPC status errors, never tool results. Exactly one terminal
  `run_mcp_tool` record per call, carrying the configured `mcp.server.name`,
  `mcp.tool.name`, `status`, `mcp.credential.refresh_triggered`,
  `mcp.result.content_count` and `mcp.result.attachment_count`; a failed call
  also carries the shared `error.class`/`error.code`/`error.message_safe`
  tuple. Omitted platform-name collisions are WARN records subject to the
  shared repeated-warning limiter. `RunMcpToolResponse.attachments[]` carry refs
  only — raw/base64 media bytes never appear.
- **Conformance.** `service.test.ts`, `bounds.test.ts`, `auth.test.ts`,
  `http-server.test.ts`.

The shared logger emits fixed owner phase records for server/credential resolution,
refresh, connect, complete readiness, manifest preparation, call, Claim and first
Commit. Execution records carry the original claim and Tool Use, phase attempt,
monotonic elapsed and remaining shared budget. A shared opening reports each
participating execution. Lost ACK convergence emits a separate `receipt_recovery`
record for the original claim. Scope-only repeated diagnostics use the shared
limiter; logs never include credentials, SDK error bodies, schemas or tool results.

#### MCP adapters and client transport

- **Contract.** The immutable `MCP_ADAPTERS` registry (`adapters/registry.ts`),
  Workspace-scoped `SQLMcpServerResolver` (`server-resolver.ts`) and `McpSDKClient`
  over the pinned SDK's `StreamableHTTPClientTransport` (`client.ts`). Installed
  Session configuration owns names; one resolved endpoint binds credential
  selection and transport construction throughout an attempt.
- **Lifecycle.** Lazy establish on first use, complete SDK connect and every
  discovery page before cache publication or calling, idle close at 1800 s,
  bounded reconnect, terminal-exhaustion settlement with cache eviction.
- **Invariants.** Only registered endpoints are allowed. GitHub transports carry
  `X-MCP-Toolsets: default,actions`; Slack transports do not. The SDK owns protocol
  and session headers; the common connector owns bearer authorization. Discovery
  follows opaque cursors into one complete list; repeated cursors, page/tool/byte
  bounds and the shared deadline fail without publishing partial definitions.
  There is no second retained tool-definition cache. The composite cache key is
  Workspace, Session, configured name, Vault, credential ID and `sha256(token)`;
  an endpoint mismatch retires an entry even if that key remains equal. Pending
  waiters share readiness and inherit spent refresh allowances. Each caller keeps
  its own deadline; shared SDK phases use the finite opening allowance and phase
  ceiling, and the initializer aborts when its last waiter leaves. Reconnect
  exhaustion is synthesized by the connector and settles every in-flight call
  exactly once; SDK message wording never establishes authentication provenance.
- **Conformance.** `catalog.test.ts`, `adapter-routing.test.ts`, `client.test.ts`,
  `client-readiness.test.ts`, `discovery.test.ts`, `execution-budget.test.ts` and
  `rpc-readiness.test.ts` exercise the owning boundaries, including real SDK HTTP.

#### Credential resolution and single-flight refresh

- **Contract.** `SQLMcpCredentialResolver` (`credential.ts`) and
  `SQLVaultMcpCredentialUpdatePath` (`credential-update-path.ts`); scope key
  `(workspace_id, vault_id, credential_id)`.
- **Lifecycle.** One read per call; the single sanctioned durable write is the
  OAuth refresh write-back under a row-level single-flight lock.
- **Invariants.** Match is by normalized `mcp_server_url`; the bounded
  `McpCredentialError` set each fails closed and leaks no internal step; a
  rotating refresh token is never burned twice concurrently; per-operation refresh
  ceiling of one per phase; on a returned call, `refreshTriggered` records refresh
  path participation or inherited refreshed material, including reuse of another
  refresher's winner. It does not prove a new issuer rotation or credential write;
  issuer attempts are counted at the locked refresh owner. Plaintext is never
  logged or persisted.
- **Conformance.** `credential.test.ts`, `credential-postgresql.test.ts`, and the
  `test/testdata/mcp-credential-vectors.json` vector set (each vector exercised by
  the credential suite).

#### Bridge idempotency and manifest RPCs

- **Contract.** `BridgeAPIMcpToolResultIdempotencyStore` (Claim/Commit) and
  `BridgeAPIManifestChangeNotifier` (`McpManifestChanged`), both in
  `bridge-client.ts`. Commit channel message size is
  `BridgeMcpCommitGrpcMessageBytes` (10 MiB + 256 KiB) to admit the inline-media
  leg.
- **Lifecycle.** Claim reserves create-only under `MCP_CLAIM_LEASE_SECONDS`;
  Commit persists refs-only and creates transient rows in one transaction;
  `McpManifestChanged` retries to a durable ACK on the 4-attempt schedule.
- **Invariants.** Owner-fenced Commit persists at most one result per
  `tool_use_event_id`; an active remote claim returns `in_flight`, stale Runtime
  custody returns `stale`, and an expired lease admits a new `claimId` without
  letting the older local attempt suppress it; notify exhaustion never flips
  readiness; the connector performs no attachment-store write.
- **Conformance.** `bridge-client.test.ts`, plus the reservation/lease paths in
  `service.test.ts`.

#### Result formatter

- **Contract.** `formatMcpToolResult` (`formatter.ts`) over the SDK
  `CallToolResult` content union, with `MCP_BLOB_MAX_BYTES`,
  `MCP_ATTACHMENT_MIME_ALLOWLIST`, and the `result_text` caps.
- **Lifecycle.** Pure mapping per call; decoded bytes held in memory only until
  the Commit leg.
- **Invariants.** At most one media attachment, aggregate ≤ 10 MiB; every closed
  row maps deterministically (unmapped content becomes an omission line);
  `structuredContent` is folded only when `content[]` produced no text.
- **Conformance.** `formatter.test.ts` (one golden per content row).

### Testing guide

| Suite | Proves |
| --- | --- |
| `service.test.ts` | End-to-end `RunMcpTool`/`ListMcpTools`: caller auth and binding rejected before side effects, claim/commit reservation flow, terminal-record uniqueness, manifest production and notify retries |
| `auth.test.ts` | TokenReview admission of the Runtime tool execution and Bridge/Job Runner discovery identities; wrong methods and every other caller rejected |
| `bounds.test.ts` | Request/response envelope validation |
| `catalog.test.ts` | Registered adapters, exact endpoint normalization and rejection of unregistered endpoints |
| `client.test.ts` | Connection cache keying, idle close, reconnect backoff, auth-retry, terminal-exhaustion settlement and cache eviction |
| `credential.test.ts`, `credential-postgresql.test.ts` | The full match/fail-closed enumeration, single-flight refresh, rotation write-back, and the `mcp-credential-vectors.json` set |
| `bridge-client.test.ts` | Claim/Commit idempotency, owner fence, commit message-size admission, and `McpManifestChanged` retry classification |
| `formatter.test.ts` | Closed content mapping goldens, attachment bounds, omission lines, `structuredContent` guard |
| `http-server.test.ts` | Ops-route responses and readiness-first shutdown |
| `schema-startup.test.ts`, `static-boundaries.test.ts` | Migration-registry verification and the no-`lowering`/no-`provider-gateway` import guard |
| `config.test.ts`, `logger.test.ts` | Env config parsing (including allowed service accounts) and the leak-free structured log envelope |

If a PR changes the adapter registry, the installed Server resolver, the credential match or refresh path, the connection
policy, the idempotency or manifest RPCs, the formatter table, or the error
taxonomy in this package, it updates the matching section here and the named
conformance suites.

## Boundaries

Neither Gateway process writes `session_events`, `session_messages`, or
`sessions.usage`; usage rides the `finish` event and Bridge commits it. The
gateway never chooses or replaces a model. `provider-gateway` contains no MCP
branch; `mcp-connector` contains no lowering; neither touches sandboxes. The
`provider-gateway` gRPC service also carries the shared
`ProviderGatewayService.RunWeb` method but rejects it with `UNIMPLEMENTED`,
because web execution is served by the independent `web-connector` Service and
the Runtime Pod dials that port directly. A call arriving on the provider port
is a misrouted client, not a deferred feature.

## Operations: platform provider keys

These procedures are for platform administrators operating the
`platform_provider_keys` pool used by platform-hosted provider access. Gateway
replicas read this table through the 30 second pool cache; the streaming data
plane never writes credential rows.

Keep the database URL and `ENGINE_VAULT_KEY` in the operator environment or a
secret manager shell session. The plaintext provider key must enter the CLI on
stdin only, never as an argv flag.

```bash
cd services/gateway
export TETRAL_DATABASE_URL='postgres://...'
export ENGINE_VAULT_KEY='<64 hex chars>'
```

Each CLI database command owns one SQL connection for its serial command and awaits
native close before exiting. The single connection avoids unused pool handshakes
that can leave Bun 1.3.14 close waiting after the write has committed. This is an
operator CLI setting; Gateway serving pools retain their own configuration.
The entrypoint flushes its buffered stdout and stderr writers before exit so
operator confirmations, help and redacted errors reach the caller.
`TestPostgreSQLPlatformKeyCLINativeClose` runs the actual entrypoint against TLS
PostgreSQL with spare handshakes held, checks the committed credential, and
requires native close and process cleanup within the existing 35-second budget.

### CLI phase diagnostics

The CLI normally emits no phase observations. An operator or test caller can
explicitly set `TETRAL_PLATFORM_KEY_DIAGNOSTIC_FD` to an inherited descriptor
number (3–1024) for a private regular file with mode `0600`. The caller owns the
file, descriptor and cleanup; stdout, stderr, stdin and SQL execution are unchanged.
The Backpressure integration fixture opts in only for its three real CLI calls.

At most nine records (4096 bytes) contain only a closed `phase` name and monotonic
`elapsed_ms`: CLI entry, stdin begin/complete, insert query begin/complete, body
error, close begin/complete and exit begin. No credentials, SQL, error text or
payloads enter this file. Invalid descriptors and observation/write failures
disable diagnostics without changing the operation or native close outcome.
Missing or invalid observations cannot establish which native phase ran. A partial
chain proves only recorded checkpoints; a missing next record can also reflect
an observation failure. The Go fixture disables the diagnostic on Windows, where
`os/exec` cannot pass `ExtraFiles`, and still runs the original CLI and assertions.

These checkpoints bracket existing awaits; they do not explain an intermittent
native failure. An inherited descriptor and synchronous writes can affect timing,
so a successful observed run is not proof that the failure has been repaired.

### Initialize

Run this before first traffic, after the platform master key exists.

1. Create platform API keys in the Anthropic, OpenAI, and DeepSeek provider
   consoles.
2. Record each provider's cache scope:
   - Anthropic: workspace id.
   - OpenAI: organization id.
   - DeepSeek: operator-chosen account label.
3. Insert one or more active keys for each platform-hosted provider:

```bash
printf '%s' "$ANTHROPIC_API_KEY" \
  | bun scripts/platform-key.ts insert \
      --provider anthropic \
      --key-id pfk_anthropic_20260703_a \
      --cache-scope "$ANTHROPIC_WORKSPACE_ID"
```

Repeat with `--provider openai` and `--provider deepseek`. All active keys for
one provider must share the same `cache_scope`.

4. Verify rows are active:

```sql
SELECT key_id, provider_id, weight, priority, cache_scope, status
FROM platform_provider_keys
ORDER BY provider_id, priority, key_id;
```

5. Start or roll Gateway. Replicas pick up active rows on startup and refresh
   changes within 30 seconds.

### Add a key

1. Create the new key in the provider console under the same cache scope as the
   provider's active pool.
2. Insert it with the ops CLI:

```bash
printf '%s' "$NEW_PROVIDER_API_KEY" \
  | bun scripts/platform-key.ts insert \
      --provider anthropic \
      --key-id pfk_anthropic_20260703_b \
      --cache-scope "$ANTHROPIC_WORKSPACE_ID" \
      --weight 1 \
      --priority 0
```

3. Confirm the row is `active`. No Gateway restart is required; all replicas
   should use the new pool within 30 seconds.

### Rotate a key

1. Insert the replacement key with the same `provider` and `cache_scope` as the
   old key.
2. Watch the old key's provider-console usage until it reaches zero, then wait
   longer than the longest in-flight turn.
3. Disable the old key in the table:

```bash
bun scripts/platform-key.ts disable \
  --key-id pfk_anthropic_20260703_a \
  --reason rotated
```

4. After Gateway replicas have had 30 seconds to refresh, revoke the old key in
   the provider console.

Rollback before provider-console revocation:

```bash
bun scripts/platform-key.ts enable --key-id pfk_anthropic_20260703_a
```

### Disable a key

Use this for leak response or provider-console compromise.

1. Disable the affected key first, before revoking it at the provider:

```bash
bun scripts/platform-key.ts disable \
  --key-id pfk_anthropic_20260703_a \
  --reason leak_response
```

2. Wait up to 30 seconds for all Gateway replicas to refresh.
3. Revoke the key in the provider console.
4. Monitor Gateway quarantine/error logs and provider-console usage to confirm
   traffic has moved away from the disabled key.

If a PR changes the credential resolution table, a lowering rule family, the
stream event set, the error taxonomy, the key pool, the model catalog, or the
attachment resolution in this folder, it updates the matching section here.

## Workload identity configuration

`TETRAL_MCP_CONNECTOR_ALLOWED_BRIDGE_SERVICE_ACCOUNTS` retains its existing name
and accepts an explicit comma-separated list of `namespace/serviceaccount`
identities, bounded to 16 entries and 4096 bytes. The deployed discovery callers are `tetral-system/bridge` and
`tetral-system/job-runner`. Empty, duplicate, malformed and wildcard entries
fail startup; namespace overrides remain supported. Discovery admission never
grants `RunMcpTool`: that method admits only the configured Runtime identity
and verifies its signed Session binding and reviewed Pod UID. Provider model
execution likewise admits only Runtime. Provider Gateway has only the Bridge
attachment methods; MCP has only the Bridge manifest and result methods.
Configuration changes require a process restart.

Provider Gateway and MCP Connector always run beside the mandatory routing
sidecar, injected as a Kubernetes native sidecar. Kubernetes starts the
application container only after that proxy has started, which supplies the
start-after-proxy ordering; neither command has its own proxy wait or reads a
transport-profile setting.

## Process diagnostics and database pools

Provider Gateway and MCP Connector use the shared
[TypeScript diagnostic contract](../../internal/ts-observability/README.md).
`TETRAL_LOG_LEVEL`, `TETRAL_LOG_MAX_RECORD_BYTES`,
`TETRAL_LOG_SUMMARY_INTERVAL_MS`, and `TETRAL_LOG_BURST` are restart-only controls.
The default level is Info. Safe builders retain fixed classifications and bounded
identities; repeated degradation emits bounded summaries. Existing metrics expose
drops and asynchronous stderr failures. Startup and shutdown finally blocks
release diagnostic timers/listeners without waiting for stream flush.

Each command attempts every acquired resource once after startup, listener,
wait or shutdown failure. Provider and MCP stop admission, join producers and listeners, close owned
clients, and close SQL last under their shared absolute deadline. A rejected earlier close does not skip
later resources. Programmatic callers retain the original run failure, or the
first cleanup failure after a successful run. Executable and signal boundaries
emit fixed safe phase/class records and exit nonzero on failure, without raw
exception messages or stacks. Diagnostic faults add no stderr-flush wait;
business shutdown retains the same durable outcome when diagnostics fail.

Provider request validation, caller denial and cancellation summaries use Info
while retaining the safe failure tuple. Provider transport, configuration and
final failure summaries use Error. Existing stream/timeout/discovery/review
classifications remain admitted by the shared scalar vocabulary.

The [Bun PostgreSQL pool owner](../../internal/ts-dbconnect/README.md) supplies
`max` 10 connections, `idleTimeout` 30 seconds, `maxLifetime` 1800 seconds, a
30-second `connectionTimeout` and `statementTimeoutMs` 30000 milliseconds. All
five controls accept canonical positive
safe integers. Missing values use defaults; Provider Gateway also treats explicit
empty values as defaults, while MCP Connector rejects explicit empties.
`TETRAL_DRAIN_TIMEOUT_MS` and `TETRAL_SERVICE_CANCEL_JOIN_TIMEOUT_MS`
follow the same per-process rule: Provider Gateway treats an empty value as
omitted, and MCP Connector rejects it. The pool values configure each owned SQL
generation. Explicit database TLS requires both
`TETRAL_DATABASE_TLS_CA_PATH` and `TETRAL_DATABASE_TLS_SERVER_NAME`, hostname
verification, and a complete verified initial generation before readiness.
Every actual awaited store operation, including transaction commit, stays inside
the generation owner's `withSQL` callback. CA rotation follows the owner's
[trust replacement policy](../../internal/ts-dbconnect/README.md#pool-generation-owner):
a candidate verifies before it receives work, and an update that removes a
still-valid CA first stops admission on the old generation. Old-generation
borrowers are joined under the 20-second drain bound. Shutdown interrupts
candidate validation and uses the remaining application deadline; it does not
start a fresh rotation budget.
Go's independently owned pool policy remains distinct.
