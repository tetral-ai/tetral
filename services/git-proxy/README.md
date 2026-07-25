# git-proxy

## Responsibilities

Streaming git smart-HTTP relay between sandboxes and `github.com`, and the
platform's only internet-reachable ingress. Sandboxes hold no git credentials;
this service is the single site that injects the per-repository access token on
the upstream leg of each request. It owns no durable tables, holds no
cross-request state beyond the per-ticket in-flight counter
(`MaxConnsPerTicket`), and persists, caches, and
logs none of what passes through — no repository data, no request bodies, no
raw tickets, no decrypted tokens. Its sole upstream is `github.com`, pinned as
a compile-time constant (`githubHost` in `relay.go`), so the request-time SSRF
surface is structurally zero. Go standard library only (`net/http`,
`net/http/httputil`, `crypto/sha256`, `crypto/subtle`) plus `engine/internal`
packages for store access, AES-GCM decryption, and structured logging. The core
request pipeline runs across `routes.go` (grammar + ticket and owner/repo
extraction), `ticket.go` (validation), `credential.go` (token lookup and
injection), `policy.go` (allowlist decision), `relay.go`
(`httputil.ReverseProxy` wiring), and `observability.go` (logging) — this is the
pipeline, not the full tree. `cmd/git-proxy/main.go` is a thin shim over `run.go`, which holds
the `Run()` entrypoint and wires the public and metrics listeners, the database,
and the encryptor; `config.go`, `handler.go`, `limits.go`, `metrics.go`, and
`transport.go` carry config, handler assembly, limit constants, the metrics
registry, and the upstream transport respectively.

## States & lifecycle

### Request pipeline

Sandbox git CLIs reach the proxy through a global git config written at
preparation: an `insteadOf` URL rewrite pointing `github.com` here, plus an
`extraHeader` carrying the session ticket. Each request passes four stages. A
failure in any of the first three — path gate, ticket check, authorization —
short-circuits with **zero upstream requests**; the zero-upstream guarantee is
scoped to these pre-relay stages. The relay stage necessarily contacts
`github.com`: a chunked push crossing `MaxRequestBodyBytes` mid-stream returns
`413` after the upstream request may already have been entered, and the reactive
`401` re-read (see Seams) is itself triggered by an upstream response. Only the
`Content-Length`-declared over-cap rejection is guaranteed before any upstream
dial.

| Stage | Decides | Failure outcome |
| --- | --- | --- |
| Path gate (`routes.go`) | one of the four smart-HTTP shapes; `<owner>`/`<repo>` match `[A-Za-z0-9][A-Za-z0-9._-]*` | any other path/method/host → `404`, no upstream |
| Ticket check (`ticket.go`) | `X-Tetral-Git-Ticket` decodes as a 43-char base64url value and its `SHA-256` matches a `session_git_tickets` row (constant-time compare) | missing/malformed/unknown/expired → `401`; over `MaxConnsPerTicket` → `429` |
| Authorization (`credential.go`, `policy.go`) | mounted-repository lookup and token decrypt | see credential arms below |
| Relay (`relay.go`) | bidirectional unbuffered stream to `github.com` | body over cap → `413`; idle stall → cut |

Public URL shape (the ticket never travels in the URL):

```text
https://<git-proxy-host>/github.com/<owner>/<repo>[.git]<git-path>
```

Accepted `<git-path>` values — exactly these four:

```text
GET  /info/refs?service=git-upload-pack
GET  /info/refs?service=git-receive-pack
POST /git-upload-pack
POST /git-receive-pack
```

Dumb-protocol `GET /info/refs` without `service`, any `/info/lfs/` path, any
non-`github.com` host segment, and any other method are `404`. The upstream URL
relays the `.git` suffix exactly as received. A bounded migration flag
`TETRAL_GIT_PROXY_LEGACY_PATH_CUTOVER` (default off — the target end state)
gates a legacy fifth shape carrying the ticket as the leading URL segment; with
it off, URL-borne tickets are never accepted.

### Ticket states

Tickets are minted by Sandbox Service at the session's first GitHub
preparation and rotated by each re-preparation, which rewrites the sandbox git
config wholesale. Only `SHA-256` hashes are stored, never raw tickets.

| Ticket row state | Accepted? |
| --- | --- |
| `live` | yes |
| `rotated`, within grace (`now - rotated_at <= TICKET_ROTATION_GRACE_SECONDS`) | yes |
| `rotated`, past grace | no → `401` |
| missing / unknown / malformed | no → `401` |

The rotation grace defaults equal to the drain grace
(`DefaultTicketRotationGraceSeconds = DefaultDrainGraceSeconds`, wired from
`TETRAL_GIT_PROXY_DRAIN_GRACE_SECONDS`) so a single git operation whose
sequential requests (`GET /info/refs` then `POST git-*-pack`) straddle a
re-preparation completes instead of failing mid-way. Rotation is
crash-hardened by pending-activation ordering: a re-preparation inserts the new
ticket as a `pending` row and keeps the prior ticket `live`; only after the
sandbox git-config install succeeds does one DB transaction flip the new row
`pending -> live` and the old row `live -> rotated`. The installed token stays
`live` until its replacement is proven installed.

### Relay bounds (named constants; config may override, names may not)

| Constant | Default | Meaning |
| --- | --- | --- |
| `HeaderReadTimeout` | 30s | inbound request headers |
| `UpstreamDialTimeout` | 10s | TCP+TLS to `github.com` |
| `IdleProgressTimeout` | 120s | cut a transfer after this long with zero bytes moving either way; never a slow-but-moving one |
| (total transfer) | deliberately absent | large clones legitimately run for minutes |
| `MaxRequestBodyBytes` | 2 GiB | push-pack cap; `413` on violation, checked on `Content-Length` up front and on actual bytes mid-stream |
| `MaxConnsPerTicket` | 16 | concurrent in-flight requests per ticket; excess `429` |
| `DefaultDrainGraceSeconds` | 1800 | SIGTERM drain window |

Bodies stream both ways with `FlushInterval = -1` and per-connection buffers
only (memory growth is bounded per connection, independent of payload size).
Only a fixed request-header allowlist goes upstream (`Content-Type`, `Accept`,
`Git-Protocol`, `Content-Encoding`, `Accept-Encoding`); the inbound ticket and
any client-sent `Authorization` are stripped. Upstream 3xx `Location` headers
pointing at `github.com` (renamed/transferred repositories) are rewritten back
through the proxy's public base URL so redirect-following stays inside the
relay; the follow-up re-carries the ticket because the sandbox
`extraHeader` matches the same-host redirect target. The upstream leg is HTTPS
to `github.com` (`githubScheme = "https"` in `relay.go`) verified strictly
against a public-CA chain: `transport.go` sets no `TLSClientConfig`, so the Go
standard library's default certificate verification applies unchanged and no
insecure-TLS skip exists anywhere in the transport. A replacement must not
introduce one.

### Drain

On SIGTERM: readiness flips false, the listener stops accepting, in-flight
transfers run to completion within `DefaultDrainGraceSeconds`, then the process
exits. New connections after SIGTERM are refused. Deployment sets
`terminationGracePeriodSeconds >= DRAIN_GRACE_SECONDS`, so a rolling deploy
never truncates a clone/push that fits the window. Sandboxes must be able to
reach the proxy host for git to work at all: a sandbox environment configured
with a `cidr_allow_list` must include the proxy's published CIDR, or git is
unavailable in that sandbox — documented behavior, not a defect.

Operationally, the main HTTP port also serves `/health` and `/ready`;
`/metrics` is `404` there and served on a separate metrics port. The access log
emits exactly one structured record per request with a closed field set
(`operation`, `event.kind`, `component`, `ticket_id`, `owner_repo`, `decision`,
`upstream_status`, `bytes_in`, `bytes_out`, `duration.ms`, plus the shared
envelope); the raw
ticket, the credential token, and the injected `Authorization` value appear in
logs zero times. `operation` is one of `refs-upload`, `refs-receive`,
`upload-pack`, or `receive-pack`, and is empty when a request is rejected before
it parses. Metrics series (with their label dimensions):
`gitproxy_active_connections` (gauge), `gitproxy_bytes_relayed_total{direction}`,
`gitproxy_requests_total{endpoint,decision,upstream_status}`,
`gitproxy_upstream_latency_seconds` (histogram), and
`gitproxy_ticket_rejections_total{reason}`; the metrics output also names two
alert conditions, `ticket_rejection_spike` and `upstream_5xx_ratio` (constants
`AlertTicketRejectionSpike` / `AlertUpstream5xxRatio` in `observability.go`).

## Seams

### The credential-injection boundary

This service is the one place a per-repository GitHub token becomes an upstream
`Authorization` header. The boundary is the `RepositoryTokenResolver` interface
(consumer-defined in `policy.go`), backed in production by
`PostgreSQLRepositoryTokenResolver` (`credential.go`).

**Interface contract.** A resolver answers one question per request —
"for this `(workspace_id, session_id, owner, repo)`, what credential decision?"
— by returning a `RepositoryTokenResolution{Mounted, Token}` or an error:

```go
type RepositoryTokenResolver interface {
	ResolveRepositoryToken(context.Context, RepositoryAuthRequest) (RepositoryTokenResolution, error)
}
```

`RepositoryPolicyAuthorizer` (`policy.go`) lowers a resolution into the wire
decision, and `GitHubBasicAuthorization` builds the header as
`Basic base64("x-access-token:" + token)` — GitHub's documented token-as-password
form. The closed arm table:

| Resolution | Decision | Wire outcome |
| --- | --- | --- |
| mounted, token present | `injected` | inject decrypted token upstream |
| mounted, token NULL/absent | fail closed | `424 credential_required`, never anonymous |
| mounted, token undecryptable | fail closed | `424 credential_required` |
| not mounted | `anonymous` | relay with no `Authorization` upstream |
| two mounted rows for one repository (ambiguous identity) | error | `502`, never a `LIMIT 1` pick |
| resolver DB failure / policy dependency unavailable | error | `502` |

**Lifecycle.** Read fresh per request — no preparation-time resolution, no
cache, no refresh of any kind. The resolver joins
`session_github_repository_resources` with `session_resources` and `sessions`
inside a workspace-scoped read-only transaction
(`WithWorkspaceReadOnlyTx`), so a row authorizes a token only while its
resource is active (not detached, not deletion-pending) and its session not
tombstoned. Rows are matched on a case-folded comparison key
(`githubrepo.ComparisonKey`: trim a case-sensitive `.git` suffix, then
lowercase owner/repo) while the client's casing is relayed upstream verbatim.
The stored token is decrypted with the platform master key
(`ENGINE_VAULT_KEY`, 64 hex characters, read once at startup) via
`vault.CredentialEncryptor`.

**Invariants a replacement must preserve.**

- The resource token is **write-only** and **never platform-refreshed**. It
  never enters the sandbox, a log line, or a URL to GitHub. Rotation is
  user-driven through the Resources API and takes effect on the next request
  because the row is re-read every time — no proxy or sandbox state to update.
- Fail closed, never downgrade: a mounted repository with a missing or
  undecryptable token yields `424`, never an anonymous relay.
- Ambiguous identity fails closed to `502`; per-repository identity is never
  satisfied by an arbitrary single-row pick.
- This git regime and the MCP vault regime are two independent layers with no
  shared resolution: neither reads the other's store, and this resolver never
  yields an MCP credential nor serves anything but git transport.
- Reactive re-read on an upstream `401` with a token injected: re-read the row
  exactly once, and re-inject and retry **only** on a bodyless
  `GET /info/refs` (a body-carrying request has already streamed and cannot be
  replayed). Re-read outcomes are closed: decryptable token → retry;
  NULL/undecryptable → `424`; no mounted row (resource detached/deleted between
  injection and re-read) → the original upstream `401` relayed unchanged, no
  anonymous downgrade; resolver failure → `502`. The initial arm table is not
  re-applied at re-read time.

**Conformance tests.** `testdata/git-credential-vectors.json` enumerates the
credential arms (mounted token, two distinct repos with no cross-injection,
NULL token, undecryptable token, unmounted); `TestGitCredentialVectorFileIsExercisedCompletely`
gates that every vector in the file is exercised by the suite. A replacement
resolver must pass this suite plus the reactive-re-read and ambiguous-identity
tests below.

## Testing guide

All suites live beside the code (`*_test.go`); standard `testing` only, race
detector on. What proves what:

| Guarantee | Test(s) |
| --- | --- |
| four accepted shapes; everything else `404` before upstream | `TestParseGitRequestWhitelist`, `TestParseGitRequestRejectsNonWhitelistedEndpoints`, `TestProxyRejectsInvalidRoutesBeforeUpstream`, `TestProxyRejectsMalformedQueryBeforeUpstream` |
| legacy path segment gated behind the cutover flag | `TestParseGitRequestLegacyPathRequiresCutover`, `TestProxyDedicatedHeaderAndLegacyCutoverTable` |
| ticket live/rotated-grace accept, hash-mismatch reject | `TestTicketValidatorLiveAndRotatedGrace`, `TestTicketValidatorRejectsHashMismatch`, `TestProxyRejectsBadTicketsBeforeUpstream`, `TestProxyAllowsTwoRequestOperationAcrossTicketRotationGrace` |
| credential arms (inject / anonymous / `424`) | `git-credential-vectors.json` via `TestGitCredentialVectorFileIsExercisedCompletely`, `TestProxyInjectsAuthorizationOnlyForAllowlistedRepositories` |
| per-request re-read observes rotation without cache; excludes detached/deleting rows | `TestPostgreSQLRepositoryTokenResolverObservesRotationWithoutCache`, `TestPostgreSQLRepositoryTokenResolverExcludesDetachedAndDeletingRows` |
| ambiguous / case-variant identity fails closed, casing relayed verbatim | `TestProxyFailsClosedOnCaseVariantLegacyRepositoryCollision`, `TestProxyMatchesMixedCaseRepositoryAndRelaysClientCasingVerbatim`, `TestProxyDoesNotTrimUppercaseGITSuffixFromRepositoryIdentity` |
| 401 reactive re-read: once, bodyless only, closed outcomes | `TestProxyRereadsRepositoryTokenOnceForBodylessInfoRefsUnauthorized`, `TestProxyStopsAfterOneBodylessInfoRefsReread`, `TestProxyDoesNotRereadOrReplayRequestBodiesAfterUnauthorized`, `TestProxyDoesNotRereadInjectedCredentialAfterForbidden`, `TestProxyRelaysOriginalUnauthorizedWhenReactiveRereadFindsRepositoryUnmounted`, `TestProxyClassifiesReactiveCredentialRereadFailures`, `TestProxyLeavesAnonymousUpstreamAuthFailuresUnchanged` |
| streaming, bounded memory, limits, redirect rewrite, header allowlist | `TestProxyStreamsRequestAndResponseBodies`, `TestProxyStreams100MiBWithBoundedRSS`, `TestProxyEnforcesConnectionAndRequestBodyLimits`, `TestProxyStreamsUnknownLengthRequestBodyWithCap`, `TestProxyRewritesGitHubRedirectsBackThroughProxy`, `TestProxyLeavesNonRedirectLocationHeadersUnchanged`, `TestProxyForwardsOnlyContractedGitHeaders` |
| idle-progress watchdog cuts stalls, not progress; no total-transfer timeout | `TestProxyIdleProgressTimeoutCancelsStalledUpstream`, `TestProxyUploadProgressPreventsIdleTimeout`, `TestProxyProgressingTransferHasNoTotalTimeout`, `TestProxyConfigHasNoTotalTransferTimeout`, `TestProxyDefaultIdleProgressTimeoutIsContractConstant` |
| graceful drain completes in-flight, refuses new | `TestProxyGracefullyDrainsInFlightTransferOnShutdown` |
| access-log field set exact; secret leak scan | `TestProxyAccessLogHasExactContractFields`, `TestProxyAccessLogLeakScan` |
| metrics series exposed and scrapeable | `TestProxyMetricsExposeContractSeries` |
| probes off the git relay; metrics on internal surface | `TestBuildHTTPHandlerKeepsProbesOutOfGitRelay`, `TestBuildMetricsHTTPHandlerServesMetricsOnInternalSurface`, `TestRunHTTPPairStartsPublicAndMetricsListeners` |
| config validation; fail before opening DB/listeners; startup redaction | `TestConfigFromEnvLoadsGitProxyRuntimeSurface`, `TestConfigFromEnvRequiresSafeCredentialAndDatabaseShape`, `TestGitProxyCommandRejectsInvalidConfigBeforeDatabaseOpen`, `TestGitProxySchemaBehindStopsBeforeEncryptorAndListeners`, `TestGitProxyCommandStartupFailureLogRedactsDependencyError` |
| named constants stay available; ambient proxy ignored | `TestContractConstantNamesStayAvailable`, `TestDefaultGitHubTransportIgnoresAmbientProxy` |

If a PR changes routing, ticket validation, the credential-injection boundary,
streaming, or egress behavior in this folder, it updates the matching section
here.
