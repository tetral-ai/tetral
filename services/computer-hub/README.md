# computer-hub

## Responsibilities

The service between Engine and the machines it uses. It serves every path
under `/v1/client/` (the routes a CLI calls with its connection key), and the
Controller and developer routes under `/v1/instance_requests` and
`/v1/instances`; `api` keeps every other `/v1` route. The public edge sends the
three prefixes here after `auth`'s Check.

The service owns no table. It reads and writes through `internal/instance`
(instances, their connection credentials and the idempotency records of the
issuing routes). It connects to PostgreSQL as the
serving role `computer_hub` and holds the blob store open through
`internal/blob` for drive content and images.

## Routes and credential classes

Connection keys and workspace credentials (API keys and access tokens) are
separate classes, decided first at the edge and again here:

| Path | Accepted principal | Any other principal |
|---|---|---|
| under `/v1/client/` | `connection_key`, authority `instance_connection`, scope `{workspace_id, type: "instance", id}`, operation `client.connect` | `401` with the client error body, code `credential_invalid` |
| `/v1/instance_requests`, `/v1/instances` and below | a workspace credential, through the shared operation gate (`httpapi.DeclarePublicOperation`) | a `connection_key` principal: `401` `authentication_error`, also on a path with no route |

Every request first verifies the signed internal principal
(`X-Tetral-Internal-Principal`) against the actual method and path; a missing
or invalid one is answered as a credential failure in the body of its path's
surface. A client route that does not exist answers `404 not_found` with the
client error body; a management path that does not exist answers `404
not_found_error`.

Error bodies:

- Client routes answer `{code, message}` with a code of the client error table;
  `message` is displayable and never holds a credential. The protocol defines
  no body for a `5xx`, which keeps the API envelope.
- Management routes answer the API error envelope through
  `httpapi.WriteError`.

## Instance routes

`POST /v1/instances`, `GET /v1/instances`, `GET /v1/instances/stream`,
`GET /v1/instances/{instance_id}`, `DELETE /v1/instances/{instance_id}`, and
`POST|GET /v1/instances/{instance_id}/credentials` and
`DELETE /v1/instances/{instance_id}/credentials/{credential_id}` run under the
operations `instances.*` and `instance_credentials.*`.

- Bodies are read by `httpapi.ReadJSONBody` (at most 1 MiB, else `413
  request_too_large`; U+0000 refused) and must be one JSON object without
  repeated or unknown keys.
- The issuing routes require `Idempotency-Key` once, 1 to 255 bytes after
  trimming, checked before any idempotency record is read. Creation checks the
  key, the record, then the body; issuance checks that the instance exists, the
  key, the record, a deregistered instance (`403 instance_unavailable_error`),
  `delivery`, `expires_at`, then the credential limit (`409
  credential_limit_error`). A replay with an equal canonical fingerprint
  answers the stored status with the current objects and `connection_key:
  null`; a different request with the key answers `409
  idempotency_conflict_error`. A record is written with the effect, and a
  transaction that loses the record's unique key to a concurrent identical
  request runs once more and replays.
- `expires_at` is absent (issuance plus `TETRAL_COMPUTER_HUB_MAX_CREDENTIAL_LIFETIME`)
  or an RFC 3339 time ending in `Z`, later than issuance and at most the
  maximum lifetime after it, stored to the microsecond; issuance time is the
  database clock of the transaction.
- `delivery: stream`, also when `delivery` is absent, answers `409
  instance_offline_error` after these checks and writes nothing: no CLI
  connection is online yet.
- Revocation and deregistration answer `204` and change nothing when
  repeated; deregistration revokes every credential not yet revoked with cause
  `deregistration` and keeps the instance as a tombstone.
- Lists take `limit` (default 20, at most 100, a positive integer given once)
  and `page`, a token signed with `TETRAL_COMPUTER_HUB_PAGE_TOKEN_KEY` that
  binds the workspace, the route, the filters and the order; items come in
  creation order and `next_page` is `null` after the last page.
- A missing or other-workspace object answers `404 not_found_error`; any other
  failure than the ones above is a database failure, `503` with nothing
  changed. The key appears only in the first successful issuance response;
  nothing logs a request or response body.
- `GET /v1/instances/stream` answers SSE: `event: credential_reused` or
  `event: unused`, `data:` the instance object. A writer that produces an
  event calls `instance.NotifyInstanceEvent` in its transaction
  (`pg_notify` on `tetral_hub_instance_events`); every pod listens on that
  channel (`InstanceEvents.Run`, reconnecting after a failure, which writes
  `instance_events.listener_disconnected`), reads the instance object of each
  notice and queues the event for its own subscribers in that workspace.
  Delivery is best effort: a subscriber more than 64 events behind is closed,
  a reconnecting subscriber gets no backlog, and every stream ends at
  shutdown.
- Each issued or revoked credential writes one `instance_credential_audit`
  record (`log.type` `audit`) after the commit, with the actor's credential
  kind and ID, `instance.id`, `credential.id` and `operation` `issue` or
  `revoke`.

Controllers authenticate with the workspace API key; there is no
Controller-specific credential. Every management route acts only in the
principal's workspace, under that workspace's row-level security; no body field
names a workspace.

## Configuration and ports

| Variable | Default | Meaning |
|---|---|---|
| `TETRAL_COMPUTER_HUB_HTTP_ADDR` | `:8080` | public listener, behind the edge; `/health` and `/ready` also answer here, `/metrics` does not |
| `TETRAL_COMPUTER_HUB_METRICS_ADDR` | `:8081` | metrics, health and readiness; must differ from the public address |
| `TETRAL_DATABASE_URL` | required | the `computer_hub` serving DSN |
| `TETRAL_DATABASE_TLS_CA_PATH`, `TETRAL_DATABASE_TLS_SERVER_NAME` | | database TLS, as every serving workload |
| `TETRAL_AUTH_INTERNAL_PRINCIPAL_PUBLIC_KEY_B64` | required | Auth's Ed25519 public key for the signed internal principal |
| `TETRAL_COMPUTER_HUB_MAX_CREDENTIAL_LIFETIME` | `720h` | how far after issuance an issued credential's `expires_at` may lie, and the lifetime of one issued without it; a positive Go duration, Helm `computerHub.maxCredentialLifetime` |
| `TETRAL_COMPUTER_HUB_PAGE_TOKEN_KEY` | required | 32 bytes as 64 hexadecimal characters that sign list page tokens; every pod reads the same key from the Secret Helm `secrets.computerHubPageToken` names, key `page-token-key` |
| `TETRAL_BLOB_*` | | the blob store settings `api` and Bridge use (`internal/blob`) |
| `TETRAL_HTTP_TRANSPORT`, `TETRAL_HTTP_TLS_*` | | the public listener's native TLS from the edge (`internal/transportsecurity`) |

Startup reads the configuration, opens the database, verifies the schema and
the serving role, opens the blob store, and then serves; each failure is one
redacted startup record. Shutdown drains both listeners within 10 seconds.

## Seams

- `ConfigFromEnv` parses every `TETRAL_COMPUTER_HUB_*` setting and the
  principal key.
- `NewRouter(verifier, ...)` is the public route table with its class checks;
  `WithInstanceRoutes` serves the instance routes over a database client and
  an `InstanceEvents` fan-out, which `Run` starts;
  `BuildHTTPHandler` adds health and readiness. Tests mint principals with
  `auth.InternalPrincipalSigner` and send them here directly.
- `Run(ctx, cfg, Runtime)` serves the two listeners over a verified database
  client.

## Testing guide

- `config_test.go`: the required settings, overrides and each rejected setting.
- `instance_routes_test.go`, over HTTP as the `computer_hub` role with minted
  principals: a lost creation response replayed without a second instance and
  a changed request refused; the name and `Idempotency-Key` bounds, a refused
  key reading no record; ten concurrent issuances against the credential
  limit; no key outside its issuance response, in reads, lists, error bodies,
  idempotency rows, logs or audit records; the states and fields of instance
  and credential reads; repeated revocation and deregistration; expiry bounds
  and an omitted expiry resolved once; paging 101 instances.
- `instance_stream_test.go`: two streams in two workspaces each receive only
  their own workspace's noticed event.
- `integration/computer_hub_credential_class_test.go`: each credential class on
  the other class's routes, through `auth`'s Check and a Go edge with the
  production route rules, and straight at the hub with principals Check never
  mints there; every attempt answers `401` in its surface's body and changes no
  row of any table.
- `services/auth/ext_authz_test.go`: the class rules at the Check boundary,
  with the number of lookup statements per request.

## Process diagnostics

The service records through `internal/workload` with the shared HTTP request
records and metrics, the cold start of a service pod, and the `auth.*`
decision records of the signed principal and the operation gate, and the
`instance_credential_audit` record and the instance event listener's
disconnect record, declared in
[`observability.json`](observability.json).
