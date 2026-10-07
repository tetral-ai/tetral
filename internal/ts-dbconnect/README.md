# Bun PostgreSQL connections

This package owns the common Bun PostgreSQL process-pool policy and the pool
generation owner used by Provider Gateway and MCP Connector. It reads no
ambient environment and imports no service: each command parses its own
configuration and passes validated bounds and mounted trust references. Go's
pool policy remains owned independently by `internal/dbconnect`.

## Pool policy

| Environment key | Field | Default | Unit |
| --- | --- | --- | --- |
| `TETRAL_DATABASE_POOL_MAX` | max | 10 | connections |
| `TETRAL_DATABASE_POOL_IDLE_TIMEOUT_SECONDS` | idleTimeout | 30 | seconds |
| `TETRAL_DATABASE_POOL_MAX_LIFETIME_SECONDS` | maxLifetime | 1800 | seconds |
| `TETRAL_DATABASE_POOL_CONNECTION_TIMEOUT_SECONDS` | connectionTimeout | 30 | seconds |
| `TETRAL_DATABASE_STATEMENT_TIMEOUT_MS` | statementTimeoutMs | 30000 | milliseconds |

All supplied values must be canonical positive decimal safe integers. Zero,
negative, fractions, whitespace and unsafe integers fail startup; omitted keys
use defaults. The caller explicitly supplies the established empty-value policy:
Provider Gateway treats an empty string as omitted; MCP Connector rejects it.
Pool bounds are parsed once during boot and require a process restart to
change; database trust material reloads live, as described below. Service
configuration tests assert literal defaults, override propagation and invalid
values through those caller boundaries.

## Pool generation owner

`openPostgresSQLOwner` opens and owns the process's Bun SQL pools. Its inputs
are the PostgreSQL URL, the validated pool policy, optional database TLS
references (a mounted CA bundle path and a fixed DNS server name), a drain bound
(default 20 seconds), a verification query run on every new pool, and a
bounded observer.

- **Verified TLS.** With TLS references the owner requires one explicit TCP
  host, removes TLS parameters from the URL (`ssl`, `sslmode`, `sslrootcert`,
  `sslcert`, `sslkey`, `sslpassword`) and connects with `verify-full` against
  the mounted CA and the fixed server name. An IP address is rejected as the
  server name. The initial generation must verify before the owner is returned.
- **Pinned work.** `withSQL` admits work on the active generation. The work,
  including lazy query execution and transaction commit, stays on the
  generation that admitted it, even after a later generation is activated.
- **Observation.** The mounted CA is read every 250 ms. A read resolves the
  projected symlink and is rejected if the generation changes while it is
  read. The whole file's SHA-256 fingerprint identifies an update.
- **Anchor validity.** Every certificate in the bundle must be a CA. A
  not-yet-valid anchor makes the update invalid, so the active generation stays
  in place and a later read activates the bundle once the anchor is valid.
  Expired anchors are left out, and at least one currently valid anchor is
  required. Only currently valid anchors are given to Bun, so an old CA that
  expires during a planned overlap does not disable the bundle.
- **Replacement.** Every update is built as a complete candidate pool and
  verified before it receives work. An update that adds anchors or keeps them
  leaves the active generation admitting until its candidate verifies; new work
  then routes to the candidate and the old pool drains. An update that removes
  a still-valid anchor is authoritative trust removal: the active generation
  stops admitting at once and drains, and admission resumes only through a
  verified candidate built from the narrowed bundle. On a planned CA retirement,
  where every peer already presents a chain to the remaining trust, admission
  pauses only while that candidate connects and verifies. If the server does
  not yet present such a chain, the store is unavailable until it does; the
  removed CA is not kept in use.
- **Invalid updates.** A malformed bundle, a non-CA certificate, a
  not-yet-valid anchor or a bundle without a currently valid anchor leaves the
  active generation in place and reports failure. It cannot broaden trust.
- **Retry spacing.** A candidate that fails verification for the same mounted
  bundle is retried after 250 ms, doubling up to 5 seconds. A different mounted
  bundle or a successful activation resets the spacing.
- **Two-generation ceiling.** Replacement is serialized, so a process holds at
  most two pools: the candidate and the generation that is draining. An update
  observed during a replacement is re-read after the old pool closes; a trust
  removal observed then takes effect at that point, within the drain bound.
  The aggregate connection budget (`deploy/helm/connection_budget_test.go`)
  counts both generations for each Bun process.
- **Drain.** A replaced or retired generation waits for its admitted work up to
  the drain bound, then closes.
- **Expiry.** `withSQL` refuses new work once the latest retained anchor of
  the active generation has expired.
- **Diagnostics.** Observations are `opened`, `closed`, `activated`,
  `reload_failed` and `reload_recovered`, with the current pool count.
  `reload_failed` carries the failure count and a reason, `invalid_bundle` or
  `candidate_verification_failed`, and is emitted at most once per 30 seconds
  while degraded; `reload_recovered` follows the next successful activation or
  unchanged read. Observations carry no URL or certificate material, and an
  observer failure cannot change admission, activation or cleanup. The commands
  record a failure as `transport.credential_reload_failed` with
  `transport.outcome` `invalid_generation` or `candidate_verification_failed`.
- **Close.** `close()` stops observation, interrupts a validating candidate
  and joins every pool within its deadline (default 5 seconds).
