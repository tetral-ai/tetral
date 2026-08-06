# web-connector

## Responsibilities

`web-connector` is the execution service behind the platform `web` tool —
search the web, open a page, find within an opened page. It is the third
container of the Gateway Pod, alongside `provider-gateway` and
`mcp-connector`, and terminates exactly one method, `ProviderGatewayService.RunWeb`,
on its own gRPC port; every other method on that service returns
`UNIMPLEMENTED` here. It contains no provider-lowering code and no MCP code,
never talks to a model provider or an MCP server, and owns no tables and no
durable history: its only store is a dedicated S3-compatible cache bucket
reached through `internal/blob`, where it keeps search stubs, page snapshots,
and idempotency records and nothing else. The platform never fetches a target
URL from its own network — connecting, resolving DNS, and following redirects
all happen on the search backend's infrastructure (delegated fetch), and every
model-supplied URL is screened by the URL classifier first. This README is
the service-local contract.

Code lives under `engine/services/web-connector` (Go, inside the engine
module, following the `bridge` precedent); the binary is at
`cmd/web-connector`. Configuration is per-process env only:

| Variable | Meaning |
| --- | --- |
| `TETRAL_BLOB_ENDPOINT` / `REGION` / `BUCKET` / `ACCESS_KEY` / `SECRET_KEY` | Standard blob set, pointed at the cache bucket, credentials scoped to that bucket only |
| `TETRAL_WEB_SEARCH_ENDPOINT` | Search backend base URL (default `https://s.jina.ai/`) |
| `TETRAL_WEB_READER_ENDPOINT` | Reader backend base URL (default `https://r.jina.ai/`) |
| `TETRAL_WEB_API_KEYS` | Ordered JSON array of platform backend keys (pool order) |
| `TETRAL_WEB_CONNECTOR_GRPC_ADDR` | gRPC listen address (default `0.0.0.0:9092`) |
| `TETRAL_WEB_CONNECTOR_METRICS_ADDR` | Prometheus / health listen address (default `0.0.0.0:9464`) |
| `TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY` | HMAC key for runtime binding-token verification |

Constants that govern behavior are fixed source values, not free knobs, and are
Web-stage contract values. The window / snapshot / wrap / item / hit / domain /
find bounds live in `types.go`: window bound 2000 lines / 50 KiB (whichever
binds first), snapshot cap 1 MiB, stored-line wrap 4 KiB, find match cap 250,
find match-line render cap 250 chars, find pattern cap 64 KiB, max 8 input
items per call, 10 rendered hits per query, 4 domains max per query. The gRPC
request carrier is 1 MiB and the independently bounded response carrier is
512 KiB. Search results do not repeat the request query, and find results
report the ref, match count, line count, and matched lines without repeating
the request pattern. The fetch
timeout 30 s, fetch token budget 262144, and key cooldowns 60 s (rate-limited)
and 3600 s (quota-exhausted) live in `backend.go`. The 7-day cache TTL is the
bucket lifecycle rule, provisioned outside this repository.

## States & lifecycle

### Request pipeline (`RunWeb`)

A `RunWeb` call carries the scope triple `(workspace_id, session_id,
session_thread_id)`, a `tool_use_event_id`, the runtime binding fields, and a
`WebToolInput` of up to eight items across three arrays (`search_query`,
`open`, `find`). Each call passes these stages in order; the first failure
returns and stops.

| Stage | Check | Failure surface | Side effects on failure |
| --- | --- | --- | --- |
| Caller identity | gRPC peer authenticates as the `agent-runtime` service account in namespace `tetral-agent-runtime` with a pod UID; no other caller, no public bearer tokens (`MethodAuthorizer`) | gRPC `Unauthenticated` / `PermissionDenied` (out of band, not a tool result) | none |
| Semantic envelope | scope triple + `tool_use_event_id` present; input not all-empty; ≤ 8 items | in-band `tool_error` | none |
| Structural envelope | fields within size bounds; each `open` item sets exactly one of `url` / `ref_id` | gRPC `InvalidArgument` (malformed internal request) | none |
| Binding token | `rtbt_v1` HMAC token verifies against this scope triple, `binding_id`, `binding_generation`, caller pod UID; not expired (`BindingVerifier.Verify`) | gRPC status error | none |
| Idempotency | key = `tool_use_event_id` + canonical-input hash; read job record first | matching hash replays stored response verbatim; mismatched hash is `runtime_error` "tool delivery conflict", never re-executed | none |
| Execution | run `search_query`, then `open`, then `find` in field order | per-operation `tool_error` / `runtime_error` in the composed result | stub / snapshot writes as each operation dictates |
| Settlement | write the create-only job record | — | see result-class table below |

### Result classes and persistence

| Result status | Persisted as job record? | Same-key retry | Notes |
| --- | --- | --- | --- |
| `completed` | yes (create-only) | replays the stored response byte-identical | settled outcome |
| `tool_error` | yes (create-only) | replays the stored response byte-identical | a settled, model-visible outcome |
| `runtime_error` | never | re-executes | the retryable class; a transient backend failure must not stick |

Pre-execution rejections (envelope failures, idempotency conflict) are never
persisted. A concurrent duplicate that loses the create-only job race reads
and returns the winner's stored response. A non-completed result additionally
deletes its own cache objects best-effort, and its usage block still rides the
error response.

### Cache object lifecycle

Objects are keyed under the scope triple taken from the authenticated envelope
(`metaKey` / `docKey` / `jobKey` in `storage.go`):

```text
{workspace_id}/{session_id}/{session_thread_id}/{ref_id}.meta   # search stub
{workspace_id}/{session_id}/{session_thread_id}/{ref_id}.doc    # snapshot
{workspace_id}/{session_id}/{session_thread_id}/jobs/{tool_use_event_id}.job
```

| Object | Created by | Mutability | Expiry |
| --- | --- | --- | --- |
| `.meta` stub | a rendered search hit (`SnapshotStore.StoreStub`) | write-once; a lazy upgrade adds a sibling `.doc`, never rewrites the stub | bucket lifecycle (7 days) |
| `.doc` snapshot | `open(url)`, or lazy upgrade of a stub (`StorePage` / `StorePageForRef`) | immutable; normalized once at write time and never again | bucket lifecycle (7 days) |
| `.job` record | settlement of a `completed` / `tool_error` result (`PutJob`) | create-only | bucket lifecycle (7 days) — a replay after TTL simply re-executes |

A `ref_id` is `r_` followed by 26 lowercase base32 characters over 128 random
bits, minted by the connector. Model input contributes only the final
`ref_id` segment; there is no lookup table and no cross-scope query, so a
`ref_id` resolved under a foreign scope simply misses and returns an
invalid-ref `tool_error` that never reveals whether the ref existed elsewhere.
Everything in the bucket is cache, re-derivable by re-fetching; the 7-day TTL
is bucket lifecycle configuration provisioned outside this repository, not a
business sweep — no sweep code exists in the service.

### The three operations

The search / open / find production logic lives in `service.go` (`Service.execute`
and its per-operation helpers); the conformance suite is `operations_test.go`.

**search** takes each query through the domain rule and one backend search
call per resulting target: no domains means an unscoped search; exactly one
domain scopes to that site and its subdomains; two to four domains fan out
into one call each, merged and deduplicated by URL; more than four is a
`tool_error` with no backend call. Search is lazy — the backend returns SERP
metadata only (per-hit title, URL, description, date), never page bodies. Up
to ten hits are rendered per query; each gets a fresh `ref_id` and a `.meta`
stub.

**open** materializes a page. By URL, the classifier runs first, then the page
is fetched, normalized, and stored as an immutable `.doc`, and the first
window is returned; every `open(url)` is a fresh fetch with a fresh `ref_id`,
never deduplicated against an earlier snapshot. By `ref_id`, the `.doc` is
read from the caller's own scope; if only the `.meta` stub exists, a lazy
upgrade classifies the stored URL, fetches it, and writes the `.doc` beside
the stub before windowing. A window is consecutive whole stored lines from
`lineno` (default 1), bounded by 2000 lines or 50 KiB, whichever binds first;
when lines remain it carries `next_lineno` and a continuation footer. A
`lineno` outside `[1, total_lines]` is a `tool_error` naming `total_lines`.

**find** scans an opened snapshot with Go's `regexp` engine (RE2: linear time,
no backreferences or lookaround), the same lazy-upgrade path applying when only
a stub exists. It reports up to 250 matches as line number plus truncated line
text; zero matches is a completed result, not an error. `find` locates and
`open` reads — there are no context lines.

Snapshot normalization happens once, at write time, in this order (`normalizeContent`
in `storage.go`): content over 1 MiB is truncated back to a UTF-8 boundary
(setting `source_incomplete`); the content is split on `\n` with one trailing
`\r` stripped per line; any physical line over 4 KiB is wrapped into
consecutive stored lines at UTF-8 boundaries; `total_lines` is counted after
truncation and wrapping. Empty content still splits to exactly one empty stored
line, so `total_lines` is always at least 1 and an empty document renders as a
valid lines 1–1 of 1 window. Every line coordinate the service reports —
`lineno`, `line_start`, `line_end`, `total_lines`, `find` match numbers —
names a real stored line, and `open`, `find`, and `refs[]` share that one
coordinate system. Every window contains at least one whole stored line,
`next_lineno` strictly increases, and every continuation chain terminates.

### Key pool states

Platform keys load from `TETRAL_WEB_API_KEYS` into a pool selected
round-robin over keys not in cooldown. Cooldown state lives in memory only, so
a restart relearns it at the cost of at most one failed call per bad key.

| Backend response | Key transition | Call behavior |
| --- | --- | --- |
| 429 | cooldown 60 s | rotate to next live key, retry |
| 402 | cooldown 3600 s | rotate to next live key, retry |
| 401 | dead until process restart | rotate to next live key, retry |
| pool exhausted | — | `runtime_error`, no further backend call |

## Seams

Each seam below is a replaceable boundary: its interface contract, its
lifecycle, the invariants a replacement must preserve, and the conformance
tests that pin it.

### Seam 1 — search/fetch backend

**Interface contract.** All search and fetch traffic goes through one internal
Go interface, `Backend` (`types.go`):

```go
type Backend interface {
    Search(context.Context, string, []string) ([]SearchHit, BackendOutcome)
    Fetch(context.Context, string) (Page, BackendOutcome)
}
```

Only the Jina implementation ships (`JinaBackend`, `backend.go`; search at
`s.jina.ai`, reader at `r.jina.ai` by default, both endpoints
operator-overridable to any HTTPS URL). The interface exists so a second
backend lands without touching operation semantics, storage, or formatters; a
replacement implements `Backend` and nothing above it changes in this service.
Two things outside the interface do move for a differently-hosted vendor: the
backend endpoint hosts (`s.jina.ai` / `r.jina.ai`) are also listed in the
Gateway Pod NetworkPolicy egress-intent host list
(`services/gateway/k8s/networkpolicy.yaml`), so a swap to a new host
needs that manifest edit; and the vendor's fixtures must be recorded before the
suite can stay fixture-only (see the testing guide).

**Lifecycle.** A backend is constructed once (`NewJinaBackend`) with the key
pool and endpoints, and lives for the process. It holds the key-pool cooldown
state (in memory) and, optionally, the metrics sink. It performs no storage
and no formatting.

**Invariants a replacement must preserve:**

- Requests carry only enumerated headers — auth, JSON content/accept, the
  `no-content` signal and single-domain filter on search, the
  format/timeout/token-budget/generated-alt hints on reads, and a blanked
  user-agent. No cookie-injection, proxy, or page-script headers are ever
  constructible.
- Responses map to a closed error taxonomy: an outer 400/422 → `tool_error`
  ("URL could not be fetched"); any other outer 4xx → `tool_error` with the
  `name`/`status` logged for taxonomy drift; 5xx or transport timeout →
  `runtime_error`; an outer 200 whose inner target status is 400–599 →
  `tool_error` ("target returned HTTP <status>") with no snapshot written, so
  a fetched 404 page never lands as content.
- Backend anonymity: no model-visible field ever carries the vendor name, an
  endpoint, or raw error text.
- A backend `publishedTime` is an opaque timestamp string preserved verbatim —
  the connector stores and renders it exactly as received, never parsing,
  normalizing, or reformatting it (live values appear in both ISO 8601 and
  RFC 1123 forms).
- Every successful backend call is accounted in the response usage block;
  failed calls are not counted.
- The key pool rotates on auth/quota/rate failures and returns `runtime_error`
  only when exhausted.

**Conformance tests.** `backend_test.go` — closed header tables, fixture-driven
search and fetch mapping, the full failure taxonomy, key-pool rotation /
cooldown boundaries / exhaustion, and domain fan-out merge/dedup. Fixtures
live under `testdata/` (`search-*.json`, `reader-*.json`,
`backend-error-synthetic.json`); no test performs a live backend call.

### Seam 2 — cache bucket

**Interface contract.** The connector's only store is `blob.BlobStore`
(`internal/blob`), driven through the `SnapshotStore` wrapper (`storage.go`),
which uses only create-only `Put`, `Get`, and `Delete`. The bucket holds the
three object kinds above and nothing else. A replacement store need only honor
create-only `Put` (a duplicate key surfaces `blob.DuplicateKeyError`) and the
`blob.NotFoundError` miss signal.

**Lifecycle.** Stubs, snapshots, and job records are written under the scope
triple and expire with the bucket lifecycle rule. Snapshots are immutable
once written; two concurrent opens of one stub race on the create-only write,
the loser reads and serves the winner's bytes, and a lazy upgrade only adds
the `.doc`.

**Invariants a replacement must preserve:**

- Scope from the envelope: the key's scope triple comes only from the
  authenticated `RunWebRequest`; model-controlled input contributes only the
  final `ref_id` segment. Tenant isolation is by scope triple in the object
  key — there is no cross-scope query path, so a foreign-scope `ref_id`
  misses.
- Immutable snapshots: a `ref_id`'s bytes are written once and never
  overwritten; lazy upgrade writes a sibling, never rewrites the stub.
- No other state store: no SQL writes, no Event Stream writes, no Bridge
  calls, no writes to any other bucket. This is statically guarded.
- Best-effort cleanup: a failed multi-step write, and any non-completed
  result, delete their own partial objects via `Delete`.
- Cache-class GC: everything is re-derivable by re-fetching; expiry is the
  bucket's lifecycle rule, excluded from every backup scope.

**Conformance tests.** `storage_test.go` — snapshot normalization order,
create-only write semantics, UTF-8-safe truncation, window continuation, and
canonical input-hash stability. `confinement_test.go`
(`TestConnectorProductionSourceRecursivelyUsesOnlyBlobForStorage`) statically
proves the production source uses only `internal/blob` for storage.
Scope-isolation and concurrency behavior are pinned in `operations_test.go`
(`TestRefLookupIsIsolatedByAuthenticatedEnvelopeScope`,
`TestLazyUpgradeWritesOneImmutableSiblingAndConcurrentOpenServesWinner`,
`TestConcurrentIdempotentDeliveryReturnsWinnerAndCleansLoserObjects`).

### Seam 3 — URL classifier (pre-egress screen)

**Interface contract.** `ClassifyURL` (`classifier.go`) returns a
`URLClassification` accepting or denying a target before any backend call. It
is the retained, platform-side half of the fetch security model; the network
half — connect, DNS resolution, redirect following — is delegated to the
backend, which never exposes the platform's own network position.

**Lifecycle.** Applied to every target URL before it enters any outbound
request — an `open(url)` or a lazy upgrade on a stored URL. A denied URL is a
`tool_error` with no backend call.

**Invariants a replacement must preserve:** `http`/`https` only; no userinfo;
rejection of literal IPs in loopback, link-local, multicast, unspecified,
RFC1918, carrier-grade-NAT, and IPv6-local ranges (including legacy octal and
hex IPv4 forms); rejection of cloud and cluster metadata endpoints, Kubernetes
service DNS, `.cluster.local` / `.local` / `.svc` names, and Tetral internal
service names.

**Conformance tests.** `classifier_test.go`
(`TestClassifyURLAllowsPublicHTTPAndHTTPSAndRejectsNonPublicTargets`); the
alternate/malformed IP-spelling and stub-stored-URL paths are also exercised
in `operations_test.go`
(`TestAlternateAndMalformedLocalIPSpellingsAreDeniedBeforeBackendAccess`,
`TestDeniedURLStoredInStubNeverCallsBackendOrCreatesSnapshot`).

### Seam 4 — the `RunWeb` gRPC surface and caller admission

**Interface contract.** The service implements only
`ProviderGatewayService.RunWeb` (generated stubs of the `provider_gateway`
proto owned by `gateway`, imported from
`services/gateway/gen/tetral/provider_gateway/v1`); every other method
returns `UNIMPLEMENTED` on this port. Admission is `MethodAuthorizer` (workload auth: the runtime service
account only) plus `BindingVerifier.Verify` (the `rtbt_v1` binding token).

**Lifecycle.** `Run` (`run.go`) opens two listeners — the gRPC port and the
metrics/health port — and stops both cleanly on context cancellation.

**Invariants a replacement must preserve:** identity failures are gRPC status
errors, never tool results; both authorization and binding pass before any
blob or backend access; the metrics port serves `/metrics`, `/health`, and
`/ready`, and the gRPC port serves gRPC only.

**Conformance tests.** `admission_test.go` (tampered-claim rejection before
blob/backend access; the sibling provider stream stays `UNIMPLEMENTED`),
`config_test.go` (`TestMethodAuthorizerAdmitsOnlyRuntimeServiceAccountToProviderMethods`),
`service_test.go` (identity and binding rejected before dependencies),
`run_test.go` (listener open/stop).

## Testing guide

All backend-shape tests run against recorded fixtures under `testdata/`; no
test performs a live backend call or requires a real key. The provenance of
those fixtures is a one-time step: when a backend is onboarded or swapped, its
`search-*.json` / `reader-*.json` responses are recorded once against the live
vendor endpoint, then committed under `testdata/`; from then on the suite is
fixture-only. Standing up a replacement backend therefore includes recording its
fixtures before the tests can pin it.

| Suite (file) | Proves |
| --- | --- |
| `service_test.go` | `RunWeb` end-to-end: identity and binding rejected before dependencies; search+open usage summed; failed search does not count backend requests; validation errors have usage and no side effects |
| `admission_test.go` | binding admission rejects every tampered claim before any blob/backend access; matching claims proceed; the web port leaves the sibling provider stream `UNIMPLEMENTED` |
| `operations_test.go` | operation semantics: envelope validation performs no I/O; lazy upgrade from stub then stays local; scope isolation; idempotent replay and conflict; runtime failures re-execute; multi-item composition and singular-field reduction; window/lineno bounds; denied-URL and target-HTTP taxonomy; loser-cleanup on concurrent delivery |
| `storage_test.go` | snapshot normalization order (truncate → CRLF split → wrap → count); create-only writes never replace bytes; UTF-8-safe truncation; every stored line addressable; window continuation to the final window; canonical input-hash stability and array-order sensitivity |
| `backend_test.go` | Jina backend: closed header tables; fixture-driven search/fetch mapping; usage from the data block; target-redirect status treated as readable; full failure taxonomy; key-pool rotation, cooldown boundaries, dead-key persistence, exhaustion; domain fan-out dedup and too-many-domains rejection |
| `classifier_test.go` | URL classifier accepts public `http`/`https` and rejects every non-public target class |
| `format_test.go` | formatter goldens for search / open / find / error vocabulary; no model-visible string carries the vendor name, endpoints, bucket, key material, or raw binary/base64 |
| `metrics_test.go` | the metrics endpoint exports exactly the closed, low-cardinality metric families |
| `protocol_test.go` | `RunWebResponse` carries complete per-call usage |
| `confinement_test.go` | the production source recursively uses only `internal/blob` for storage (no SQL, Event Stream, or Bridge client) |
| `config_test.go` | `LoadConfig` applies pinned addresses/endpoints and rejects a malformed key pool or short binding key; the method authorizer admits only the runtime service account |
| `run_test.go` | `Run` opens separate gRPC and metrics listeners and stops cleanly on cancellation |

### Usage and metrics

Every response carries a usage block: the coarse `operation` label
(`search` / `open` / `find` / `mixed`), `backend_tokens`, a nullable
`target_http_status`, `stored_bytes` (including the job record it writes),
`duration_ms`, and the authoritative per-call counters `web_search_requests`
and `web_fetch_requests` (successful backend calls only — each search fan-out
counts, a lazy-upgrade fetch counts, failed calls do not). The block rides the
tool result: Runtime Core carries it and Bridge commits it on the durable web
tool-result event; the connector keeps no separate usage ledger. For the
singular response fields, `source_incomplete` is the OR across items, while
`next_lineno`, `window_truncated`, and `target_http_status` are populated only
when exactly one applicable item is in the request.

A separate metrics port (`TETRAL_WEB_CONNECTOR_METRICS_ADDR`) exposes
Prometheus counters and a duration histogram —
`web_requests_total`, `web_backend_calls_total`, `web_backend_tokens_total`,
`web_key_rotations_total`, `web_cache_bytes_written_total`,
`web_request_duration_seconds` — with low-cardinality labels only;
per-session accounting stays in the usage block, never in metrics. `/health`
and `/ready` are served on the metrics port beside `/metrics`; the gRPC port
serves gRPC only.

### Maintenance rule

If a PR changes the `RunWeb` pipeline, the search/open/find semantics, the
cache-bucket keys or snapshot normalization, the backend taxonomy or key pool,
the URL classifier, or the usage and metrics surface in this folder, it
updates the matching section here and the conformance tests named above.
