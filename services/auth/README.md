# auth

## Responsibilities

Auth authenticates selected public API keys and exchanged access tokens, resolves
current Engine workspace authority, and mints request-bound Ed25519 internal
principals. It owns `POST /v1/oauth/token`, `/v1/api_keys`, the Envoy v3
`Authorization/Check` gRPC adapter, and bootstrap key refresh. Public API and
Event Stream services receive signed principals rather than raw credentials.
Reusable verification, authority, policy and credential logic lives in
`internal/auth`; this service owns configuration, PostgreSQL, HTTP, gRPC and process
lifecycle. The serving binary is `services/auth/cmd/tetral-auth`.

Authentication identifies a credential and its truthful actor. Authorization is
separate: every registered public business route invokes the shared operation
gate against signed authority and trusted resource facts from its owning store.
Only `workspace_full_access` is assignable; the schema and policy import accept
no other role. It means every operation in the code-owned registry at admission
time, and an unregistered operation name is always denied. Exchanged tokens and
independent keys therefore gain a newly registered operation once the serving
code includes it, while identity-derived keys keep the operation ceiling
recorded at their issuance.

`PolicyVersion` is a code constant recorded on grants, access tokens and
identity-derived keys. When it changes, every access token and identity-derived
key issued under the old version is permanently rejected, and new exchanges fail
until grants are re-imported; the import records the new version and advances
each grant's revision. Independent keys are unaffected. Maintainers bump it when
a role's meaning changes; nothing derives it from the registry.

The production database connection requires `TETRAL_DATABASE_TLS_CA_PATH` and
`TETRAL_DATABASE_TLS_SERVER_NAME`. It verifies trust and hostname with no
plaintext fallback. New connections load the current validated trust generation.
Administrative policy provisioning uses a separate protected administrative
connection; serving Auth cannot administer policy.

For the exact SDK configuration, Engine workspace selectors and actor wire
format, see [authentication](../../docs/authentication.md).

## States & lifecycle

### Request surfaces

| Route | Identity and input | Success | Failure |
|-------|--------------------|---------|---------|
| `POST /v1/oauth/token` | JSON JWT bearer exchange; registered rule, organization, upstream assertion, optional Engine workspace/service selector | opaque `access_token`, `token_type: Bearer`, integer `expires_in` | malformed/ambiguous input `400`; oversized body `413`; invalid assertion or missing/currently invalid authority `401`; issuer/database unavailable `503`; bounded admission exhausted `429` |
| `envoy.service.auth.v3.Authorization/Check` | raw public headers, actual method/request URI, one trusted request ID and forwarded-for | OK Check response overwrites one `X-Tetral-Internal-Principal` and enumerates credential/untrusted header removals | JSON denied response: invalid credential `401`, invalid metadata `400`, internal/dependency failure `500`; edge Check transport unavailability maps to `503` |
| `POST /v1/api_keys` | verified internal principal; `{ "name": ... }`, at most one MiB of strict JSON | metadata and one-time raw `api_key` at `200` | invalid name/body `400`; oversized body `413`; denied action `403`; stale authority/issuer credential `401` |
| `GET /v1/api_keys` | verified internal principal; `limit` defaults to 20, cap 100; opaque `page` | metadata page at `200` | foreign/mismatched cursor `400`; denied action `403` |
| `DELETE /v1/api_keys/{api_key_id}` | verified internal principal and tenant-resolved durable key facts | empty `204` | absent/revoked/foreign row `404`; malformed ID `400`; denied action `403` |

API-key management verifies the signed principal against the actual method and
path before installing trusted principal/workspace context and invoking the
registered-operation wrapper. Body and path fields never supply workspace
authority. Delete resolves its key under the signed workspace before disclosure
or mutation. The exchange route is a direct Auth HTTP surface; deployment-owned
edge routing is a separate contract.

The API-key metadata DTO is `{ id, type: "api_key", workspace_id, name,
key_prefix, key_kind: "bootstrap"|"standard", created_at, last_used_at?,
revoked_at? }`. The SDK has no API-key management resource; use raw HTTP for this
surface. Create returns raw key material exactly once. List returns no raw key,
digest or authority lineage. Prefixes are non-authenticating identification
metadata. Revoked keys are retained for audit and excluded from lists.

### Credential selection and admission

A nonempty selected `Header.Get("X-Api-Key")` retains its existing precedence
and exact bytes. Its invalidity never falls back to Bearer. A selected key does
not borrow a simultaneously supplied Bearer's identity. With no selected key,
Auth accepts one unambiguous `Authorization: Bearer <token>` value; repeated or
combined Bearer values are rejected. Query-string credentials are ignored.

Check requires Envoy `encode_raw_headers: true`; its raw repeated entries preserve
header order and mixed-case duplicate semantics. The first API-key value remains
the selected value, including an empty first value admitting Bearer instead of a
later key. Auth never calls its own HTTP surface or duplicates authority SQL.

Auth validates the actual HTTP method and request URI attributes, optional raw
`:method`/`:path` consistency, and exactly one bounded request ID and forwarded-for.
Envoy's `HttpRequest.Id` is a stream identifier, independent of `x-request-id`.
The generated header supplies the signed principal's request ID, which Check
diagnostics record as `request.id`. API, Auth API-key and Event Stream handlers
keep their own `req_` request ID for the `request-id` response header, error
envelopes and `request.id`; after verifying the principal they record its signed
request ID as `edge.request.id`, so Check and backend records of one request
join. That claim is diagnostic correlation only and authorizes nothing. The
signed forwarded-for value is audit metadata that no service reads today. The
edge owns fresh request-ID generation and trusted source forwarding. `X-Original-*` and
`X-Tetral-*` values never supply authority. `url.ParseRequestURI` produces the
same decoded `URL.Path` used by Go HTTP handlers, excluding the query from the
signature. A Check uses at most five seconds and preserves a shorter caller
cancellation; it neither requests nor buffers the public request body.

An allow response contains exactly one overwrite mutation for the newly minted
principal. Auth enumerates all presented case-insensitive `X-Tetral-*` (except
that overwritten principal), `X-Original-*`, `X-Api-Key`, and `Authorization`
headers for removal. Denials contain no principal, carry the SDK JSON error
envelope and an explicit HTTP status, and do not use Envoy's default `403`.
Protected business routes perform their normal operation gate after admission.

Admission uses one bounded transaction:

1. A fixed owner-controlled function looks up only the selected SHA-256 digest,
   resolving credential/workspace/provenance before workspace scope is known.
2. Identity-derived credentials lock rule, identity and grant in that order and
   require their current enabled state and exact recorded revisions.
3. Auth installs the validated workspace and locks the selected credential row.
   A separate statement then rechecks the same digest, revocation and expiry
   against fresh PostgreSQL time **after** the lock wait.
4. Auth reads the workspace, validates the complete typed principal, updates
   usage, and commits. A concurrent individual revoke either precedes this
   decision or waits for the admitted usage transaction.

**Atomic admission invariant.** Current root authority, selected credential
material, expiry, revocation and usage are checked under one transaction. A
caller-set lookup flag cannot grant global access. No Bearer business request
contacts the identity provider.

### Token exchange and authority selection

The exchange accepts `application/json` with exactly one strict JSON object:
`grant_type` equal to `urn:ietf:params:oauth:grant-type:jwt-bearer`, `assertion`,
`federation_rule_id`, `organization_id`, and optional `workspace_id` and
`service_account_id`. Unknown, duplicate, escaped-equivalent and case-equivalent
fields are rejected. Form bodies and query parameters do not supply selectors.
Every exchange response uses `Cache-Control: no-store` and `Pragma: no-cache`.

The registered verifier establishes an immutable rule-revision proof outside
any database issuance transaction. Auth then resolves a provisioned identity by
registered organization, exact issuer and subject; it does not provision users
or infer permissions from email, group claims or upstream organization names.
Human identities cannot supply a service-account selector. A service selector
must match the bound Engine service account.

Workspace IDs select Engine grants. An omitted selector succeeds only for one
eligible grant; multiple eligible grants are ambiguous. The exact selector
`default` resolves the configured `ENGINE_BOOTSTRAP_WORKSPACE_ID`, then requires
a matching grant. There is no per-identity default-grant fallback. Ordinary
selectors are bounded opaque Engine IDs and must resolve an eligible grant.

Issuance locks and rechecks rule, identity and grant against the proof and
selected revisions, then inserts a digest-only opaque token. The advertised
integer lifetime is capped at 600 seconds and by upstream assertion expiry;
after flooring it must exceed the SDK's 120-second renewal margin. Tokens
retain exact root and policy revisions. Disable/re-enable or security changes
cannot resurrect old tokens. An explicit token revoke affects only that token.

### Registered issuer verification

A rule fixes an HTTPS issuer, audience, RS256 algorithm, discovery or JWKS
endpoint, permitted origins/CIDRs and CA trust. Assertions cannot supply a JWKS
URL. Redirects and ambient proxies are rejected; every resolved DNS destination
is checked before connecting. Private, loopback and link-local addresses need
explicit allowed CIDRs. TLS verifies hostname and configured trust. Issuer calls
have a five-second deadline and one-MiB response limit. Invalid assertions use
`401`; unavailable issuer dependencies use a safe `503` envelope.
A refresh is bounded by that deadline and by its initiating caller's
cancellation. When the initiating caller's own context ends before the keys
load, that caller receives its own context error and the refresh is abandoned:
Auth records no issuer failure, cooldown or unknown-key forced refresh, and
joined and later callers start a new bounded refresh under their own context.
Keys that loaded and validated are published even if the initiating caller has
ended. Because an abandoned refresh does not consume the forced refresh, each
cancelled exchange can start one aborted issuer request; the per-process
exchange limits below bound that rate. Issuer timeouts, transport failures and
invalid responses for a live caller are recorded, and a completed malformed
response for a live caller remains invalid.

Caches are partitioned by rule and trust revision, bounded to 128 entries with
at most 16 concurrent refreshes. Known keys refresh after 90 percent of recorded
validity, and outage fallback ends exactly at expiry. Unknown-key and failed
cold/expired refreshes that a live caller completes have a 30-second cooldown.
Concurrent callers join one refresh; obsolete revisions cannot publish into the
current cache. Shutdown cancels and joins verifier work. Verification supplies
identity proof rather than workspace permission.

### Derived and independent keys

Bootstrap and explicitly created independent keys retain full workspace access.
Missing provenance never defaults to independent authority. An identity-derived
key records immutable rule/identity/grant IDs and revisions, policy version,
workspace ceiling and explicit operations. Key issuance rechecks the admitted
principal and its still-active issuer credential under root/credential locks;
stale principals are rejected rather than restamped with new revisions. The
child ceiling intersects the current role and the issuer's immutable ceiling.
Restricted fixtures exercise this same production gate and issuance path.

A derived key has its own durable lifetime. Its parent token expiring, being
revoked or being physically pruned does not revoke the key; an individual parent
key revoke also does not recursively revoke issued keys. Current root
revocation, disabling or revision mismatch invalidates every affected derived
credential. SQL guards prevent editing a key's provenance, workspace or ceiling.

Bootstrap refresh takes its workspace advisory lock and upserts one
`key_kind = 'bootstrap'` row. Matching active configuration is a no-op; matching
revoked configuration reactivates it; changed material replaces digest/prefix in
place and resets usage. Standard keys are untouched. The locked digest recheck
prevents old bootstrap material from authenticating across an in-place rotation.

### Administrative policy changes

Prepare the canonical database and role contract first, then run
`go run ./services/auth/cmd/tetral-auth-policy < policy.json` with
`TETRAL_DATABASE_ADMIN_URL`, `TETRAL_DATABASE_TLS_CA_PATH` and
`TETRAL_DATABASE_TLS_SERVER_NAME`. Keep administrative credentials out of the
serving Auth environment. The command verifies schema readiness and never
migrates, imports default policy at startup, or overwrites omitted entries.

The at-most-one-MiB document is an explicit change set: `federation_rules`,
`identities`, `workspace_grants`, `remove_federation_rules`, `remove_identities`,
`revoke_workspace_grants`, and `revoke_access_tokens` (ID plus workspace).
Rules carry exact HTTPS trust configuration; identities explicitly distinguish
`human` from `service`; grants name an existing Engine workspace and the sole
assignable role. Callers do not provide security revisions. See the typed DTOs
in [policy.go](../../internal/auth/policy.go) for complete fields.

Whole-document validation precedes changes. Imports prelock affected roots in
rule/identity/grant order and atomically apply them; unchanged rows preserve
revisions. Omission preserves state. Removal retains disabled root tombstones;
recreation advances revision. Grant revocation is terminal: regrant uses a new
ID, and SQL cannot clear an old revocation. Disabling/replacing a grant in one
change set is atomic. Output contains only change IDs and revisions; no-op
returns an empty `changes` array. Failed commands emit a fixed safe diagnostic.

### Maintenance and shutdown

Each actual Auth process owns one cancellable minute timer. Each pass has a
two-second database deadline and deletes at most 1,000 tokens strictly more than
24 hours past expiry by PostgreSQL time, using ordered `FOR UPDATE SKIP LOCKED`.
It deletes neither keys nor policy roots. Backlog aggregation is time bounded
by that deadline; it is not a claim of a bounded aggregate scan.

Auth's separate metrics listener exports fixed status counters
`tetral_auth_token_prune_passes_total{status="success"|"failed"|"cancelled"}`,
`deleted_total`, `expired_backlog`, `oldest_expiry_age_seconds`,
`last_success_timestamp_seconds`, and `healthy`, all with the same
`tetral_auth_token_prune_` prefix. Last successful capacity values remain
available after a failed pass; initial values do not claim a completed pass.
There are no token, identity or workspace metric labels. Healthy ticks are
quiet; degradation/recovery diagnostics are bounded and contain safe tuples.
All listeners bind and native credential material validates before readiness.
A listener failure cancels its siblings and clears readiness. The separate gRPC
health service reports `envoy.service.auth.v3.Authorization`; the process probes
remain on HTTP/metrics. Listener shutdown joins requests within the ten-second
drain budget, cancelling forced gRPC work, then Application closes and joins pruning and
issuer work before closing PostgreSQL and the trust observer.

The same metrics listener exports fixed-bucket
`tetral_operation_duration_seconds{service="auth",operation="/envoy.service.auth.v3.Authorization/Check",outcome=...}`
from the actual Check unary boundary, including current-peer validation. A typed
allow is `success`; a typed 4xx denial is `rejected`; a typed 5xx or transport
failure is `error`. Actual caller/transport cancellation or deadline expiry is
`cancelled`/`timeout`. A nil gRPC error on a denied response never means success.
An internal dependency expiry returned as HTTP 500 remains `error` unless the
caller/transport context itself expired. No request, credential or workspace
labels are added, and HTTP token/key metrics retain their names and units.

`shutdown_grpc_drain` measures the actual bounded GracefulStop wait; expiry is
`timeout`. Forced `shutdown_grpc_cancel_join` records only after Stop and the
handler owner join, without a new join budget. The existing process logger
records the same phase duration even if the metrics listener is already closed,
subject to [shared collection limits](../../internal/workload/README.md#operation-durations).
The owning PostgreSQL Check test verifies typed denial populations, a real
closed Auth-role database failure with no principal or credential usage, and
the existing held native Check transaction completing during graceful drain.

### Config and ports

| Setting | Default | Supported bound |
|---------|---------|-----------------|
| `TETRAL_AUTH_HTTP_ADDR` | `:8080` | public routes plus `/health` and `/ready`; `/metrics` is `404` |
| `TETRAL_AUTH_METRICS_ADDR` | `:8081` | must differ from HTTP and gRPC; `/metrics`, `/health`, `/ready` |
| `TETRAL_AUTH_GRPC_ADDR` | `:9095` | separate Check and gRPC health listener |
| `TETRAL_AUTH_GRPC_TRANSPORT` | `plaintext` when unset | `plaintext` for the standard routed profile or `native-mtls` for the hardened Auth/edge hop |
| `TETRAL_AUTH_GRPC_TLS_CA_PATH`, `TETRAL_AUTH_GRPC_TLS_CERT_PATH`, `TETRAL_AUTH_GRPC_TLS_KEY_PATH` | none | all required for `native-mtls`; rejected for plaintext |
| `TETRAL_AUTH_GRPC_TLS_EDGE_CLIENT_URI` | none | required for `native-mtls` and must be `spiffe://<trust-domain>/ns/envoy-gateway-system/sa/tetral-public-edge`; only the trust domain is configurable, unlike `TETRAL_HTTP_TLS_EDGE_CLIENT_URI`, which accepts any SPIFFE path |
| `TETRAL_HTTP_TRANSPORT` | `plaintext` | public Auth HTTP only: `plaintext` or `native-mtls` |
| `TETRAL_HTTP_TLS_CA_PATH`, `TETRAL_HTTP_TLS_CERT_PATH`, `TETRAL_HTTP_TLS_KEY_PATH`, `TETRAL_HTTP_TLS_EDGE_CLIENT_URI` | none | complete trust/leaf/edge role for native HTTP; metrics stay internal |
| `TETRAL_AUTH_JWKS_CACHE_TTL_SECONDS` | 600 | integer 1–600 |
| `TETRAL_AUTH_EXCHANGE_BODY_BYTES` | 32768 | integer 1024–65536 |
| `TETRAL_AUTH_EXCHANGE_CONCURRENCY` | 32 | integer 1–32 |
| `TETRAL_AUTH_EXCHANGE_REQUESTS_PER_MINUTE` | 60 | integer 1–6000, per actual process |
| `TETRAL_AUTH_EXCHANGE_BODY_READ_TIMEOUT_MS` | 5000 | integer 100–5000 |
| `TETRAL_AUTH_INTERNAL_PRINCIPAL_TTL_SECONDS` | 60 | integer 1–300 |

Exchange admission includes parsing and body draining within the configured
read deadline. A rejected request terminates its body read and connection
without consuming a full drain budget outside a slot. The limiter retains no
assertions or caller identifiers. Bootstrap requires an existing configured
workspace, a strong `ENGINE_API_KEY`, and a valid Ed25519 private signing key.
Startup verifies config, canonical schema and the serving role before refreshing
the bootstrap key once. The workspace must already exist. HTTP and Check borrow
the same authority resolver and signer; constructing the adapter does not create
workspaces or repeat bootstrap writes. Native Check credentials reload complete
mounted generations through `internal/transportsecurity`, require the exact edge
client role, and never retry a failed TLS connection as plaintext. The hardened
edge verifies the exact Auth DNS name `auth.tetral-system.svc.cluster.local`.
Every new native RPC verifies the established peer against current trust, exact
edge role and certificate validity. CA overlap keeps the listener and admitted
requests alive. Operators distribute overlap trust, rotate leaves and observe
fresh handshakes, then replace/drain old Auth Pods within the ten-second listener
budget before removing old trust. Retirement does not silently stop the whole
Auth process. A missed drain cannot admit a new Check on an old connection:
retired or expired peer certificates produce transport unavailability, and the
edge returns `503` without a principal.

## Seams

- **Edge adapter:** `NewExternalAuthorization(ExternalAuthorizationConfig)` borrows
  the real request authenticator and signer; `Register(grpc.ServiceRegistrar)`
  installs the v3 Check service. `OpenExternalAuthorizationServer` owns its
  separately bound listener, native credential observer, gRPC health, and drain.
  Deployment routing and its real Envoy proof live with the public edge. The
  former nginx HTTP authorization endpoint has been removed.
- **Principal signer:** only Auth owns the private signing key. Public services
  receive the verify key. Claims bind audience, exact method/path, request audit
  metadata, issuance/expiry and a mandatory discriminated credential/identity/
  authority union. API-key actors retain their actual durable key ID; direct
  human/service Bearers carry stable Engine identity IDs and no API-key ID.
- **Persistence:** pre-workspace access is restricted to fixed owner-controlled
  lookup/lock/prune functions. Serving roots are read-only; workspace writes
  require trusted scope. Raw keys, access tokens and assertions are never stored
  or logged; SHA-256 digests alone authenticate stored credentials. Parent
  credential IDs are audit lineage, not a dependency on retained token rows.

## Testing guide

`make test` runs pure checks; `make test-affected` and `make test-full` provision
native declared dependencies. Focused PostgreSQL roots still accept an
administrative `TETRAL_TEST_DATABASE_URL` and create isolated clones/roles.

- `services/auth/exchange_bounds_test.go`: strict exchange fields, typed config,
  protected responses and real-socket slow body/admission bounds.
- `services/auth/exchange_postgresql_test.go`: actual HTTP exchange, Engine
  selectors, human/service bindings, ported Check credential precedence and error classes.
- `services/auth/ext_authz_test.go`: actual gRPC Check, real Auth-role private
  PostgreSQL, signature/typed actor/touch/path binding, foreign and revoked keys,
  frozen Bearer selection/duplicates, malformed metadata, actual lock-graph
  revoke/admission orders and interrupted database waits.
- `services/auth/grpc_test.go`: listener configuration and, within the owning
  PostgreSQL root, real native TLS peer rejection without credential admission,
  held Check across leaf renewal and trust overlap, retired-channel rejection,
  and readiness withdrawal with an admitted Check completing before listener,
  credential observer and pool shutdown join.
- `services/auth/operation_coverage_test.go`: actual Auth route registrations.
- `internal/auth/authority_resolver_test.go` and `authority_transactions_test.go`:
  durable lineage/ceilings, parent pruning, exact revisions, actual Auth-role
  row-lock barriers, both revocation orders and post-lock expiry/digest checks.
- `internal/auth/policy_test.go`, database role tests and the policy command tests:
  atomic/no-op/tombstone imports, narrow role isolation and protected operator
  entry with serving-role rejection.
- Verifier/bounds tests: controlled HTTPS trust, claims, malicious metadata,
  rotation, retirement, cancellation, cache pressure and revision isolation.
- Pruner tests: strict retention, bounded batches, replica progress, role
  isolation, failure recovery, metrics and joined cancellation.
- Actual OIDC integration roots: unchanged SDK/real HTTPS Keycloak exchange and
  reactive 401 recovery, real Keycloak/Auth rotation, two actual Auth processes
  sharing PostgreSQL with frozen Bearers and zero issuer calls, and exact test
  edge forwarding. Native inventory owns their dependency lifecycle.

## Process diagnostics

The command uses the shared [Go process diagnostic contract](../../internal/workload/README.md).
Restart-only `TETRAL_LOG_LEVEL`, `TETRAL_LOG_MAX_RECORD_BYTES`,
`TETRAL_LOG_SUMMARY_INTERVAL_MS` and `TETRAL_LOG_BURST` default to Info-level
bounded diagnostics. Failure records retain safe error tuples; repeated
degradation emits suppression summaries. Cleanup completes before bounded
process-diagnostic close. Credentials and assertion contents never enter logs.

Auth outcomes use fixed `auth.stage` and `auth.result` values. Invalid input,
credentials, and assertions differ from grant/operation denial, exchange limits,
PostgreSQL unavailability (`auth_store_unavailable`), and issuer/JWKS
unavailability (`auth_jwks_unavailable`). Successful exchange records retain
trusted human/service kind and positive rule, identity, and grant revision
numbers. Denials retain these facts only after their lookup or signed admission
establishes them. Operation denials include the registered semantic
`auth.operation`; resource selectors and identity IDs are omitted. Unknown facts
stay absent. Successful request admission and operation checks stay quiet at the
default level. Repeated failures share fixed stage/code suppression partitions;
kind, revisions, and actions never expand limiter keys. The same shared audit
context covers Auth, API (including its event-list routes), and EventStream
gates. Diagnostic sink failure cannot change the authorization result or
business effects.
