# computer-hub

## Responsibilities

The service between Engine and the machines it uses. It serves every path
under `/v1/client/` (the routes a CLI calls with its connection key), and the
Controller and developer routes under `/v1/instance_requests` and
`/v1/instances`; `api` keeps every other `/v1` route. The public edge sends the
three prefixes here after `auth`'s Check.

The service owns no table. It reads and writes through `internal/instance`
(instances and their connection credentials). It connects to PostgreSQL as the
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
| `TETRAL_BLOB_*` | | the blob store settings `api` and Bridge use (`internal/blob`) |
| `TETRAL_HTTP_TRANSPORT`, `TETRAL_HTTP_TLS_*` | | the public listener's native TLS from the edge (`internal/transportsecurity`) |

Startup reads the configuration, opens the database, verifies the schema and
the serving role, opens the blob store, and then serves; each failure is one
redacted startup record. Shutdown drains both listeners within 10 seconds.

## Seams

- `ConfigFromEnv` parses every `TETRAL_COMPUTER_HUB_*` setting and the
  principal key.
- `NewRouter(verifier, ...)` is the public route table with its class checks;
  `BuildHTTPHandler` adds health and readiness. Tests mint principals with
  `auth.InternalPrincipalSigner` and send them here directly.
- `Run(ctx, cfg, Runtime)` serves the two listeners over a verified database
  client.

## Testing guide

- `config_test.go`: the required settings, overrides and each rejected setting.
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
decision records of the signed principal and the operation gate declared in
[`observability.json`](observability.json).
