# auth

## Responsibilities

`auth` is the platform's public API-key authenticator and the only
minter of signed internal principals. It turns a raw `X-Api-Key` into a
short-lived Ed25519-signed principal token that every other public service
trusts, and it owns the workspace-scoped `/v1/api_keys` management surface.
Raw public keys stop here — target services never receive them, only the
minted principal. The service owns four routes (`POST
/internal/auth/authorize` plus the three `/v1/api_keys` handlers), bootstrap
key seeding from `ENGINE_API_KEY`, API-key digest lookup / revoke checks /
`last_used_at` updates, and internal-principal minting. It reads and writes
exactly one table, `api_keys`, and reads `workspaces`; it holds no
cross-request state beyond the injected signing key, and it never reaches
sessions, resources, or any other domain table. Reusable auth domain logic
lives in the `internal/auth` package; the service package
(`github.com/tetral-ai/tetral/services/auth`, package `tetralauth`)
only wires config, database, signer, and router. The binary is at
`cmd/tetral-auth`.

## States & lifecycle

### Request surfaces

| Route | Caller identity | Input | Success | Failure |
|-------|-----------------|-------|---------|---------|
| `POST /internal/auth/authorize` | edge external-auth subrequest | `X-Api-Key`, `X-Original-Method`, `X-Original-Path`, `X-Request-Id`, `X-Forwarded-For` | `200 {"allow":true}` + `X-Tetral-Internal-Principal` header | missing key `401`; missing method/path or request-id/forwarded-for `400`; unknown/revoked key `401` |
| `POST /v1/api_keys` | verified internal principal | `{ "name": ... }`, body capped 1 MiB, unknown fields rejected, exactly one JSON value | `200` metadata + one-time raw `api_key` | empty/oversized name `400`; body too large `413` |
| `GET /v1/api_keys` | verified internal principal | `limit` (default 20, cap 100), opaque `page` cursor | `200 { "data": [...], "next_page": ... }` | cursor for another workspace or a different limit `400` |
| `DELETE /v1/api_keys/{api_key_id}` | verified internal principal | id must carry the `ak_` prefix | `204` empty body | absent / already-revoked / other-workspace row `404`; malformed id `400` |

The `/v1/api_keys` handlers consume `X-Tetral-Internal-Principal`, not the raw
key: the edge already authorized the caller through the authorize subrequest
and injected the principal. Each handler re-verifies that header against its
own method and path, and takes workspace authority solely from the verified
principal. A request body never supplies workspace identity.

The metadata DTO (`auth.APIKeyMetadata`) returned by create and list — this
service is the only public home for its shape, since the forked SDK carries no
api-keys resource — is pinned for raw-HTTP callers as
`{ id, type = "api_key", workspace_id, name, key_prefix, key_kind =
bootstrap|standard, created_at, last_used_at, revoked_at }`. `key_prefix` is
non-authenticating metadata; the create response additionally carries the
one-time raw `api_key`, never returned again. `last_used_at` and `revoked_at`
are omit-when-empty (not required-nullable), and a revoked row is kept for audit
but excluded from list responses.

### Authorize subrequest steps

| Step | Action | Failure |
|------|--------|---------|
| 1. Digest lookup | `SHA-256` over the raw token compared against `api_keys.key_digest` where `revoked_at IS NULL`, joined to `workspaces` so the owning workspace resolves in one query | no matching active row `401` |
| 2. Usage touch | `last_used_at` stamped through the workspace-scoped write path | row revoked or gone between steps `401` |
| 3. Mint | Ed25519-signed principal bound to the original method and path written to `X-Tetral-Internal-Principal`; body `{"allow":true}` at `200` | signing unavailable `401` |

The digest lookup runs in a read-only transaction that sets only the
transaction-local `tetral.auth_lookup` flag and no workspace scope, so it may
match a key before the owning workspace is known while any accidental write
through that transaction is still blocked by the workspace-scoped write path.

**Single active predicate (invariant).** The authenticate lookup
(`AuthenticateRawKey`) and the usage touch (`TouchLastUsedForWorkspace`) MUST
gate on the identical active predicate — both currently `revoked_at IS NULL` in
`internal/auth/store.go`. Letting the two diverge (a key that authenticates but
is no longer touched, or vice versa) is the primary way to weaken the auth model,
so the predicate is one clause repeated verbatim, not two independent filters.

### `api_keys` row states

| State | Predicate | Enters via | Authenticates? |
|-------|-----------|-----------|----------------|
| absent | no row | — | no |
| active | `revoked_at IS NULL` | `POST /v1/api_keys` (standard) or bootstrap seeding | yes |
| revoked | `revoked_at` set | `DELETE /v1/api_keys/{id}` | no (row kept for audit) |

`key_kind` is `bootstrap` (the single seeded row) or `standard`
(workspace-managed). Revoke sets `revoked_at` only where it was still `NULL`.

### Bootstrap refresh (`auth.RefreshBootstrap` → `APIKeyStore.UpsertBootstrap`)

Idempotent against the one `key_kind = 'bootstrap'` row:

| Existing bootstrap row | Action |
|------------------------|--------|
| none | insert row from digest + 16-char prefix |
| digest matches, active | no-op |
| digest matches, revoked | clear `revoked_at` on that row |
| digest differs | replace digest + prefix in place, reset `last_used_at` |

Standard rows are never touched, so re-seeding the bootstrap key never
invalidates or reactivates workspace-managed keys.

### Startup gates (`BuildApplication` → `BuildRouter`)

| Gate | Check | On failure |
|------|-------|-----------|
| config | env decoded; `ENGINE_API_KEY` non-blank and ≥ 32 bytes after trim; signing key valid base64 Ed25519; metrics addr ≠ HTTP addr | startup error, no serve |
| schema | `VerifySchema` | startup error |
| runtime role | `VerifyRuntimeRole` | startup error |
| bootstrap workspace | `ENGINE_BOOTSTRAP_WORKSPACE_ID` names an existing `workspaces` row | fail closed, refuse to seed |
| bootstrap key | `RefreshBootstrap` upserts the seeded row | startup error |

### Internal principal token

Compact `header.payload.signature` (EdDSA, `typ = tetral-internal-principal`).
Claims: `workspace_id`, `api_key_id`, `aud = tetral-public-api`, bound
`method`/`path`, `iat`/`exp`, `jti` (`itok_` token id), `request_id`,
`forwarded_for`. TTL defaults to 60 seconds
(`TETRAL_AUTH_INTERNAL_PRINCIPAL_TTL_SECONDS` overrides). Verification
requires a valid signature, the fixed audience, an exact method-and-path
match, a not-yet-expired `exp`, and non-empty workspace and key ids. The same
key signs the `/v1/api_keys` list cursors (`typ = tetral-cursor`), which bind
the workspace id, list position, and limit.

### Ports

| Port | Serves |
|------|--------|
| `TETRAL_AUTH_HTTP_ADDR` (default `:8080`) | the four routes plus `/health` and `/ready`; `/metrics` returns `404` here |
| `TETRAL_AUTH_METRICS_ADDR` (default `:8081`) | `/metrics` plus `/health` and `/ready` |

## Seams

### Edge external-auth boundary

- **Interface contract.** The edge gateway is deployment infrastructure (an
  adapter over Traefik ForwardAuth, Envoy `ext_authz`, ingress-nginx
  `auth-url`, or equivalent). It calls `POST /internal/auth/authorize` with
  the public key and original request metadata, receives allow/deny plus the
  `X-Tetral-Internal-Principal` header, then strips the raw key and any
  client-supplied `X-Tetral-*` headers, injects the minted principal, and
  forwards to the target service.
- **Reference manifest.** The edge is deployment infrastructure, but a reference
  ingress manifest lives in-repo at
  `deploy/kubernetes/edge-gateway/ingress-nginx.yaml`, routing `/v1/api_keys` to
  `auth` with `pathType: Prefix` (the catch-all `/v1` routes to
  `api`); `deploy/kubernetes/manifest_test.go` asserts that path→backend
  map exactly. Because `/v1/api_keys` is a Prefix route, a new sub-path such as
  `POST /v1/api_keys/{id}/rotate` routes to `auth` automatically and needs
  no edge or manifest change.
- **Lifecycle.** One subrequest precedes each public request, including
  `/v1/api_keys` calls, which re-enter through the edge as an ordinary target
  service.
- **Invariants a replacement must preserve.** A client-supplied principal is
  never trusted; the raw key is never forwarded past the edge; no public route
  requires method-based backend selection (a read and a write on one path
  terminate on the same service).
- **Conformance tests.** `services/auth/routes_test.go`
  (`TestAuthorizeMintsSignedInternalPrincipalAndTouchesKey`,
  `TestAuthorizePathOnlyPrincipalVerifiesQueryBearingRequest`,
  `TestAuthorizeRequiresAuditRateLimitMetadata`).

### Internal principal signer

- **Interface contract.** `auth.InternalPrincipalSigner` mints and
  `auth.InternalPrincipalVerifier` verifies. The signing key is an Ed25519
  private key handed to this service alone as deployment config; target public
  services receive only the verify key
  (`NewInternalPrincipalVerifierFromBase64`), so they can check principals but
  can never mint them.
- **Lifecycle.** Minted per authorize call, verified once per downstream
  request, expired at `exp`.
- **Invariants a replacement must preserve.** Asymmetric signing with
  verify-only distribution; per-request binding of audience, method, and path;
  a positive TTL; non-empty workspace and key ids. A different scheme must keep
  all four or downstream trust breaks.
- **Conformance tests.** `services/auth/routes_test.go` (mint side);
  consumer verification in `internal/eventstream/eventstream_test.go`
  (`TestEventStreamRoutesRequireSignedInternalPrincipal`) and the
  `api` / `event-stream` command tests.

### API-key store (persistence)

- **Interface contract.** `auth.APIKeyStore` over a `pgx` connection: create,
  list, revoke, touch, authenticate, and bootstrap upsert against `api_keys`,
  with `workspaces` read for the join and the startup check.
- **Lifecycle.** Every mutating call runs inside a workspace-scoped
  transaction (`storage.WithWorkspaceTx`) that sets `tetral.workspace_id`; the
  authenticate lookup is the one read-only transaction that sets no workspace
  scope.
- **Invariants a replacement must preserve.** Only public-safe metadata and
  the non-recoverable `SHA-256` digest are stored; the raw token is never
  written to any column and never read back; the stored `key_prefix` is
  identification metadata that cannot authenticate. Tenant isolation is signed
  principal binding with `workspace_id` in every primary-key predicate: reads
  and writes bind the principal's `workspace_id`, and a cross-workspace row is
  a `404`, never a fallback. Time columns are written as
  `time.Now().UTC().Format(time.RFC3339)` (`internal/auth/store.go`) — fixed
  width, always `Z`, no fractional seconds — so lexicographic string comparison
  equals chronological order. Any new time-window predicate depends on that
  format; switching to `RFC3339Nano` or a local offset would silently break the
  ordering.
- **Conformance tests.** `internal/auth/store_test.go`
  (`TestCreateForWorkspaceReturnsRawKeyOnceAndStoresMetadataOnly`,
  `TestListActiveForWorkspaceRejectsCrossWorkspaceCursor`,
  `TestRevokeForWorkspaceCannotCrossWorkspace`),
  `internal/auth/bootstrap_test.go`
  (`TestRefreshBootstrapPreservesStandardKeys`,
  `TestRefreshBootstrapReactivatesRevokedBootstrapRow`).

### Key distribution (admin, out of band)

- **Interface contract.** Keys are issued and revoked only through the
  admin/raw-HTTP `/v1/api_keys` surface; the client SDK deliberately carries no
  api-keys resource. `ENGINE_API_KEY` is a bootstrap credential for first
  administrative access — not a user public API key and not a provider API key.
  Flow: an admin sets `ENGINE_API_KEY` at deploy → startup
  seeds the bootstrap row → the admin calls `POST /v1/api_keys` with the
  bootstrap key to mint a `standard` key (raw value returned once) → the admin
  hands it to the user out of band → the user sets it as the SDK `apiKey`.
- **Lifecycle.** Rotation is mint-new then `DELETE /v1/api_keys/{id}` (`204`);
  the revoked row stays for audit.
- **Invariants a replacement must preserve.** The raw key is emitted exactly
  once, from the create body, and in no log line. A future SDK api-keys
  resource, if ever added, must be generated from the pinned DTO, not
  hand-authored.
- **Conformance tests.** `services/auth/routes_test.go`
  (`TestAPIKeyManagementUsesSignedPrincipalAndManagedAgentsCursorShape`,
  `TestAPIKeyManagementErrorsUseSDKEnvelopeWithRequestID`).

## Testing guide

| Suite | Proves |
|-------|--------|
| `services/auth/routes_test.go` | authorize mints and touches; path-only principals verify query-bearing requests; audit/rate-limit metadata required; `/v1/api_keys` uses the signed principal and the cursor shape; errors use the SDK error envelope with a request id, including `413` for oversized bodies |
| `services/auth/schema_startup_test.go` | schema verified before runtime role; a behind schema stops before runtime-role and bootstrap seeding |
| `services/auth/cmd/tetral-auth/main_test.go` | the public handler does not expose `/metrics`; metrics serve on a separate listener; startup-failure logs use shared fields |
| `internal/auth/store_test.go` | raw key returned once and only metadata stored; list excludes revoked and honors limit + cursor; cross-workspace cursor and cross-workspace revoke rejected; unknown/empty keys rejected |
| `internal/auth/bootstrap_test.go` | strength floor enforced; insert / rotate / no-op / reactivate branches; standard keys preserved and revoked standard keys not reactivated |
| `internal/auth/generator_test.go` | `tetral_sk_` prefix, single ASCII token, 32 decoded secret bytes, `SHA-256` digest, 16-char prefix |
| `internal/auth/middleware_test.go` | missing/invalid key rejected; query-string keys ignored; workspace and principal attached on success; the provided key is never logged |

If a PR changes the authorize flow, the internal-principal token shape, the
`/v1/api_keys` handlers, or bootstrap seeding in this folder, it updates the
matching section here.
