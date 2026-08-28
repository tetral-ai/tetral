# gateway

The Gateway Pod: the only place in the platform where provider SDKs, provider
credentials, and provider wire formats exist. It runs three containers behind
one cluster-internal headless `Service` — one gRPC port per container plus HTTP
health/metrics listeners. This folder owns two of them, both TypeScript/Bun
under `packages/`: `provider-gateway` (this document's subject) and
`mcp-connector` (the MCP tool relay, detailed in its own section below). The
third container, `web-connector`, lives in its own service folder with its own
README. The shared support packages — `protocol`, `lowering`, and `schema` —
sit beside the container packages.

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
| Authenticate | Verify the runtime workload token and the per-thread `runtime_binding_token` (scope triple, binding id/generation, pod UID, expiry) | gRPC `UNAUTHENTICATED`/`PERMISSION_DENIED` before any credential is resolved |
| Readiness | Reject if the process is not ready | gRPC `UNAVAILABLE` ("gateway service not ready") — transient, runtime retries |
| Admission | Bounded concurrent in-flight turns (`TurnAdmissionGate`, default 8, `TETRAL_GATEWAY_MAX_CONCURRENT_TURNS`) | fast retryable `provider-error` rather than event-loop queueing |
| Validate | `validateProviderRequest` (pure, in `packages/protocol`) | deterministic `INVALID_ARGUMENT` — **non-retryable** for that turn |
| Catalog lookup | Model → rules + client | unknown model → `provider_unavailable` (non-retryable) |
| Lower | `lowerProviderRequest` (hop ①, pure, once per turn) | lowering failure closes the turn |
| Credential + call | Resolve credential, attempt the provider stream inside the pool failover loop | see credential and pool tables |
| Raise + write | `ProviderStreamRaiser.map` each SDK stream part → `ProviderStreamEvent`, written with gRPC drain backpressure | terminal event only |

The stream event set is closed: `text-*`, `reasoning-*`, `tool-input-*`,
`tool-call`, `finish`, `provider-error`, and the pre-stream
`attachment-rejections` report. Ordering and terminal rules the gateway must
never violate: `finish` and `provider-error` are terminal and mutually
exclusive; `usage` rides `finish` only; every fragment id starts before its
delta/end and never restarts after end; `tool-call` arrives once per id and its
name matches the streamed tool-input; `attachment-rejections` is non-terminal,
emitted at most once, before provider streaming begins.

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
`/metrics`; `packages/provider-gateway/src/http-server.ts`). Graceful shutdown on
SIGTERM: flip readiness false → graceful `server.stop()` drain → grpc
`tryShutdown()` → exit. New turns are refused (`UNAVAILABLE`) the moment
readiness flips, while in-flight streams finish under the drain.

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
  independent 8 MiB carrier for bounded tool-call input. Provider deltas and raw
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
- **Lifecycle.** One SDK stream per turn; SDK client retries are disabled.
- **Liveness.** First-event and inter-event watchdogs bound transport stalls.
  Independently, a 60-second semantic-progress watchdog is measured from the
  last non-empty text/reasoning delta or Tool Call/input delta. Metadata,
  keepalive traffic, and start/end markers do not re-arm it; expiry uses the
  existing retryable provider-stream timeout projection.
- **Invariants.** Raw-wire access is confined to the enumerated file-and-purpose
  points — a raw-wire mutation elsewhere is a boundary violation. Provider
  fetches may target only catalog base URLs plus the OAuth issuer/subscription
  endpoints (the app-layer allowlist, fully separate from the web tool's
  SSRF classification, which lives in the `web-connector` service). AI SDK versions are pinned. Abort must deterministically error
  the stream so `streamText` cannot hang.
- **Conformance.** `packages/provider-gateway/test/unit/clients.test.ts`,
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
- **Invariants.** The separate migration owner constructs schema. Gateway and
  MCP serving roles are NOSUPERUSER and NOBYPASSRLS; both fail closed through a
  stable error that retains no role name, DSN, credential, or driver text when
  the stamp, live policies, or role posture differs from the repository-owned
  contract.
- **Conformance.** `packages/provider-gateway/test/unit/schema-startup.test.ts`,
  and `static-boundaries.test.ts` for the cross-package import guardrails.

## Testing guide

| Suite | Proves |
| --- | --- |
| `packages/lowering/test/unit/*-request.test.ts` | Each provider's request-lowering rule cells (table-driven, one case set per rule id) |
| `packages/lowering/test/unit/stream.test.ts` | SDK stream part → `ProviderStreamEvent` mapping, dropped parts, ordering/terminal negatives |
| `packages/lowering/test/unit/usage.test.ts` | Usage normalization including the anthropic-wire vs openai-wire family split and cache-hit numbers |
| `packages/lowering/test/unit/errors.test.ts` | Error classification and the retryable overrides (5xx, in-stream 5xx, context overflow, entitlement) |
| `packages/lowering/test/rules-coverage.test.ts` | Every enumerated rule id has at least one test — the matrix-to-test mapping, enforced by CI |
| `packages/provider-gateway/test/golden/*` | Byte-level outbound request (cache placement, thinking envelopes, `store:false`/encrypted include, beta headers, OAuth swap+rewrite, schema surgery, absent/omitted fields) and SSE replay → event sequence with usage |
| `credentials*.test.ts`, `openai-oauth-refresh.test.ts` | Fail-closed enumeration, positive resolution to the provider-native credential header, Go↔TS decryption round-trip, OAuth single-flight refresh and rotation CAS |
| `platform-pool.test.ts`, `platform-key-cli.test.ts` | Body-level classification, cool/quarantine transitions, pre-first-byte failover and switch cap, cooldown clamp, weighted selection, cache-scope startup refusal, operator-CLI round-trip |
| `leak-guards.test.ts` | No key/token sentinel appears in any captured log, event, or error payload |
| `service.test.ts`, `grpc-server.test.ts` | End-to-end streaming turn, drain backpressure, cancellation, header/inter-chunk and semantic-progress timeouts, admission cap, and the Bun/grpc-js tripwire (high event count + trailers + clean status) |
| `http-server.test.ts` | Ops-route responses and readiness-first graceful shutdown |
| `attachments.test.ts` | Transient and file-backed resolution, per-ref rejection reporting, and the integrity-mismatch fatal path |
| `schema-startup.test.ts`, `static-boundaries.test.ts` | Migration-registry verification and cross-package boundary guards |

Run from `services/gateway`. The whole workspace suite (both containers
plus the shared packages) is `bun run test` — the `test` script in
`package.json`, the same set the `gateway-ts` CI job runs. For a single suite,
`bun test` filters by path substring per package (e.g. `bun test
packages/provider-gateway/test/unit/service.test.ts`), matching the
`agent-runtime` idiom. Golden fixtures are checked in; regeneration is an
explicit reviewed action, never a side effect of a failing run.

## mcp-connector

### Responsibilities

`mcp-connector` is the second TypeScript/Bun container of the Gateway Pod. It
terminates tool calls of `kind = mcp` on its own gRPC port (`McpConnectorService`,
defined beside the provider service in
`proto/tetral/provider_gateway/v1/provider_gateway.proto`), holds the MCP client
sessions to a **closed catalog of curated servers**, discovers each server's
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

The catalog is one entry — GitHub — in `MCP_CATALOG`
(`packages/mcp-connector/src/catalog.ts`). `assertCatalogURL` refuses any
connection whose URL, after single-trailing-slash normalization
(`normalizeCatalogURL`), is not in the constant; catalog-only admission is
enforced upstream and this is defense in depth. Adding a server is a code change.

### States & lifecycle

#### `RunMcpTool` turn (`packages/mcp-connector/src/service.ts`)

`McpConnectorServiceShell` drives one tool call as an ordered pipeline, with a
Bridge-backed durable reservation bracketing the external side effect. The
`RunMcpTool` caller is the Runtime pod; `ListMcpTools` is called by Bridge. Both
identities are TokenReview-authenticated (`KubernetesTokenReviewClient`,
`packages/mcp-connector/src/auth.ts`); `RunMcpTool` additionally verifies the
per-thread runtime binding token.

| Stage | Action | Failure |
| --- | --- | --- |
| Authenticate | TokenReview the Runtime workload token, then verify the binding token | gRPC `Unauthenticated` / `PermissionDenied` before any side effect |
| Validate | `validateRunMcpToolRequest` (`bounds.ts`) | gRPC `INVALID_ARGUMENT` |
| Claim | Create one execution-attempt `claimId`, then call `ClaimMcpToolResult` with `(scope, tool_use_event_id, claimId)`; Bridge loads the durable server, tool, and canonical input and compares its normalized hash internally | same-claim replay renews the lease; an unexpired different claim remains in flight; an expired lease admits a new claim; a terminal result replays directly |
| Resolve credential | Match one session-vault credential (table below) | fail closed, no MCP call is made |
| Establish + execute | Lazy-connect the MCP client, call the tool within `MCP_CALL_TIMEOUT_SECONDS` (120) | reconnect/auth policy below; timeout → `mcp_timeout` |
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

`SQLGitHubMcpCredentialResolver` searches the session's immutable vault set for a
credential whose `auth_public_json.mcp_server_url` equals the catalog URL
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

The bounded error set is `GitHubMcpCredentialError` (`credential_required |
ambiguous | undecryptable | expired | refresh_failed`); an uncaught
selection-query failure is outside it. Decrypted plaintext exists only in process
memory and inside TLS to the server — never logged, never returned to Runtime,
never persisted here.

#### Single-flight OAuth refresh (`packages/mcp-connector/src/credential-update-path.ts`)

GitHub OAuth refresh tokens rotate on use, so two consumers must never burn one
rotating token concurrently. `SQLVaultGitHubMcpCredentialUpdatePath` serializes
refresh on a `SELECT … FOR UPDATE` (or advisory lock) keyed by `(workspace_id,
vault_id, credential_id)` for the refresh HTTP call plus write-back. Losers block,
re-read the row, and use the newer material without refreshing once `expires_at`
has moved forward. Proactive refresh at resolution triggers only when `expires_at
<= now + REFRESH_SKEW_SECONDS` (60 s, `credential-constants.ts`); a reactive path
(upstream 401/403) invokes one single-flight refresh regardless of expiry. A
refresh failure marks the resolution `refresh_failed` and leaves the row
unmutated. One `RunMcpTool` operation traverses at most one proactive, one
establishment-reactive, and one operation-reactive refresh — no path loops a
refresh on a repeated same-phase failure.

#### MCP client connection (`packages/mcp-connector/src/client.ts`)

`McpSDKClient` wraps the SDK's `StreamableHTTPClientTransport` (Bearer header via
`streamableHTTPTransportOptions`). Clients are cached by `(workspace_id,
session_id, mcp_server_name, sha256(token))`; a per-call resolution that yields
different material creates a new client and closes the old, so credential
switches need no coordination.

| State | Trigger | Transition |
| --- | --- | --- |
| Establish | first use (discovery or first call) | lazy `initialize` handshake, `MCP_CONNECT_TIMEOUT_MS` (10 s) |
| Idle close | `MCP_SESSION_IDLE_SECONDS` (1800) without a call | close the client |
| Reconnect | connection loss | 3 attempts, backoff 1 s / 4 s / 16 s (`MCP_RECONNECT_DELAYS_MS`, `MCP_RECONNECT_MAX_RETRIES`); in-flight call reports `retry_status: retrying` then `exhausted` |
| Auth retry | server 401/403 | one single-flight refresh + one retry; a second auth failure is `terminal` → `mcp_authentication_failed` |
| Exhaustion settlement | terminal reconnect exhaustion | the connector's own `Client.onerror` synthesizes `mcp_connection_failed` / `retry_status = exhausted`, settles every in-flight call on that client exactly once, evicts the cached entry, and clears the idle timer; late responses are ignored |

Exhaustion settlement (~21 s after loss) pre-empts the 120 s `mcp_timeout`, which
remains for non-connection stalls — one failure yields exactly one
classification. The SDK fires `onerror` but never `onclose`, so a client→entry
index performs the eviction; without it the dead client would stay cached.

#### Durable idempotency and reservation lease

The connector owns no replay store; records live in the Bridge-owned
`session_runtime_tool_results` table (`tool_kind = mcp`) reached through three
TokenReview-authenticated Bridge RPCs the gateway ServiceAccount may call
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
RESIDUAL, stated not hidden: an operation whose full authorized path — a 120 s
call, an operation-reactive refresh, and a second 120 s call — outruns the lease
can let two replicas *execute* the external side effect. Double-persist is already
excluded by the owner-fenced Commit; the short-lease-with-heartbeat fix is
deferred to a dedicated cycle. On replay a media attachment whose transient ref is
no longer resolvable renders an omission line `[MCP attachment unavailable:
<mime> (<size>)]` rather than serving stale bytes.

#### Discovery and manifest delivery

The connector alone can reach the server, so it **produces** the manifest; Bridge
**delivers** it (Bridge can reach the Gateway Pod; the connector cannot reach
Runtime). `ListMcpTools` returns each tool's `{name, description, input_schema}`
verbatim, plus a `manifest_etag` (content hash via `manifestEtag`) and
`omitted_tools` (platform-tool name collisions the connector filtered via
`filterManifestTools`, `reason = builtin_name_collision`). Bridge captures the
manifest to a durable `session_mcp_manifests` row before delivering, assigns a
monotonic `manifest_generation`, and enforces the 256 KiB per-server manifest
bound at acceptance. Supersession keys on generation monotonicity, never on etag
inequality — a flapping A→B→A etag must not clobber newer state — and the etag is
identity-only, so the family-filtered delivered subset need not re-hash. At
runtime every `tools/list_changed` notification triggers a re-list, and each
successful re-list is reported to Bridge even when its etag matches an earlier
notification. The initial upstream list precedes notification retry and is
non-mutating on failure. The connector retries a within-cap notification with 4
total attempts (`MCP_MANIFEST_NOTIFY_RETRY_DELAYS_MS`, 1 s / 4 s / 16 s). Notify
exhaustion is a structured connector log only, never a readiness flip; the next
notification re-triggers. Bridge alone decides whether the durable manifest is
current, requires a readiness restore, or advances generation. Bridge treats a
durably committed over-cap transition
as terminal and returns it without connector retry. The durable row carries a
`(readiness, diagnostic)` pair orthogonal to content: an over-cap manifest is
written `unready` and contributes no tools while its last-accepted content is
preserved; discovery failure leaves the row and Queue unchanged. Restore is
readiness-aware, so a re-notify matching the stored etag while `unready` is a
restore (not a duplicate no-op).

#### Tool-system mapping

| Concern | Rule |
| --- | --- |
| Definition | MCP `{name, description, inputSchema}` verbatim |
| Route | `gateway`, `kind = mcp` |
| Scheduler | `parallel_safe`, no conflict key (server-side effects are GitHub's own concurrency domain) |
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

`McpConnectorErrorCode` is a ten-member union mapped to the protocol enum by
`mcpErrorKind`. Every `RunMcpTool` produces exactly one terminal record;
`mcp_in_flight` is its own kind and is never logged as `mcp_connection_failed`.

| `error_kind` | Trigger | Delivery |
| --- | --- | --- |
| `mcp_tool_error` | MCP `isError: true` | `tool_error` result — model-visible |
| `mcp_invalid_input` | server rejects arguments (JSON-RPC invalid params) | `tool_error` result — model-visible |
| `mcp_connection_failed` | reconnect exhausted / terminal | `session.error` wrapping `mcp_connection_failed_error`; call settles `runtime_error` |
| `mcp_authentication_failed` | auth retry failed (an existing credential was rejected) | `session.error` wrapping `mcp_authentication_failed_error`; call settles `runtime_error` |
| `mcp_credential_required` | zero matching credential to try | `session.error` wrapping `mcp_authentication_failed_error` with `retry_status = terminal`; call settles `runtime_error` |
| `mcp_timeout` | call exceeded `MCP_CALL_TIMEOUT_SECONDS` (120) | `tool_error` result naming the timeout |
| `mcp_claim_conflict` | Claim stored-result hash mismatch | `session.error` wrapping `unknown_error` with `retry_status = terminal`; call settles `runtime_error` |
| `mcp_in_flight` | live unexpired reservation on claim | retryable `runtime_error`, no `session.error` |
| `mcp_commit_failed` | post-effect Commit/store failure after the side effect ran | retryable `runtime_error`, no `session.error` |
| `mcp_internal_error` | an unclassified connector-side exception during execution | retryable `runtime_error`, no `session.error`, no `retry_status` |

#### Event mapping

Runtime Core writes the public events; the connector supplies `mcp_server_name`,
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
  binding-token check; `ListMcpTools` is caller-authenticated as Bridge; identity
  failures are gRPC status errors, never tool results. Exactly one terminal
  `run_mcp_tool` record per call. `RunMcpToolResponse.attachments[]` carry refs
  only — raw/base64 media bytes never appear.
- **Conformance.** `service.test.ts`, `bounds.test.ts`, `auth.test.ts`,
  `http-server.test.ts`.

#### MCP catalog and client transport

- **Contract.** `MCP_CATALOG` / `assertCatalogURL` (`catalog.ts`) and
  `McpSDKClient` over `StreamableHTTPClientTransport` (`client.ts`).
- **Lifecycle.** Lazy establish on first use, idle close at 1800 s, bounded
  reconnect, terminal-exhaustion settlement with cache eviction.
- **Invariants.** The connector opens a connection only to a catalog URL (defense
  in depth on top of upstream admission). Reconnect exhaustion is synthesized in
  the handler, never mapped from SDK wording, and settles every in-flight call on
  the client exactly once. The connection cache key includes `sha256(token)`, so a
  credential switch is a new client.
- **Conformance.** `catalog.test.ts`, `client.test.ts`.

#### Credential resolution and single-flight refresh

- **Contract.** `SQLGitHubMcpCredentialResolver` (`credential.ts`) and
  `SQLVaultGitHubMcpCredentialUpdatePath` (`credential-update-path.ts`); scope key
  `(workspace_id, vault_id, credential_id)`.
- **Lifecycle.** One read per call; the single sanctioned durable write is the
  OAuth refresh write-back under a row-level single-flight lock.
- **Invariants.** Match is by normalized `mcp_server_url`; the bounded
  `GitHubMcpCredentialError` set each fails closed and leaks no internal step; a
  rotating refresh token is never burned twice concurrently; per-operation refresh
  ceiling of one per phase; `refreshTriggered` means a successful rotation
  contributed to the returned call, while issuer attempts are counted at the
  locked refresh owner; plaintext never logged or persisted.
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
| `auth.test.ts` | TokenReview admission of the Runtime pod and Bridge identities; every other caller rejected |
| `bounds.test.ts` | Request/response envelope validation |
| `catalog.test.ts` | Closed catalog, URL normalization, connection refusal to any non-catalog URL |
| `client.test.ts` | Connection cache keying, idle close, reconnect backoff, auth-retry, terminal-exhaustion settlement and cache eviction |
| `credential.test.ts`, `credential-postgresql.test.ts` | The full match/fail-closed enumeration, single-flight refresh, rotation write-back, and the `mcp-credential-vectors.json` set |
| `bridge-client.test.ts` | Claim/Commit idempotency, owner fence, commit message-size admission, and `McpManifestChanged` retry classification |
| `formatter.test.ts` | Closed content mapping goldens, attachment bounds, omission lines, `structuredContent` guard |
| `http-server.test.ts` | Ops-route responses and readiness-first shutdown |
| `schema-startup.test.ts`, `static-boundaries.test.ts` | Migration-registry verification and the no-`lowering`/no-`provider-gateway` import guard |
| `config.test.ts`, `logger.test.ts` | Env config parsing (including allowed service accounts) and the leak-free structured log envelope |

If a PR changes the catalog, the credential match or refresh path, the connection
policy, the idempotency or manifest RPCs, the formatter table, or the error
taxonomy in this package, it updates the matching section here and the named
conformance suites.

## Boundaries

No Gateway Pod container writes `session_events`, `session_messages`, or
`sessions.usage`; usage rides the `finish` event and Bridge commits it. The
gateway never chooses or replaces a model. `provider-gateway` contains no MCP
branch; `mcp-connector` contains no lowering; neither touches sandboxes. The
`provider-gateway` gRPC service also carries the shared
`ProviderGatewayService.RunWeb` method but rejects it with `UNIMPLEMENTED`,
because web execution is served on the `web-connector` container's own port and
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
