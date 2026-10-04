# Authentication

Engine accepts independent API keys and short-lived bearer tokens issued by
Engine after external identity federation. The external provider authenticates
the user or workload; Engine determines that identity's workspace access.

An operator must register a federation rule, its exact issuer and Engine
audience, the provider's discovery or JWKS endpoint, and its permitted TLS trust
and egress destinations. The operator also provisions an identity binding for
that rule's organization namespace, exact issuer, and stable subject, then grants
that identity a workspace role. Login does not create Engine membership. Email,
display names, groups, and request headers do not select an Engine identity.

The currently assignable role is `workspace_full_access`. Public operations use
the shared authorization policy and retain workspace isolation. Resource-level
permission filtering and organization membership management are separate
features.

## Exchange an assertion

Send a provider JWT intended for the registered Engine audience to
`POST /v1/oauth/token`. The endpoint accepts the explicit SDK's JSON grant:

```json
{
  "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
  "assertion": "<provider JWT>",
  "federation_rule_id": "<registered rule ID>",
  "organization_id": "<registered organization namespace>",
  "workspace_id": "<granted workspace ID>"
}
```

A service identity can additionally send `service_account_id` matching its
provisioned Engine binding. This selector is distinct from the provider's JWT
subject. A human identity cannot use a service selector. All selectors narrow
already provisioned authority; they do not create authority or change the actor.

`workspace_id` is optional when the identity has exactly one eligible workspace
grant. No eligible grant denies the exchange; multiple eligible grants require
an explicit selection. The selector `default` means the deployment's configured
Engine default workspace and requires a grant for that exact workspace. Engine
does not infer an organization default from external SDK or provider semantics.

A successful response contains an opaque `access_token`, `token_type: "Bearer"`,
and integer `expires_in`. It sets `Cache-Control: no-store` and `Pragma: no-cache`.
Use the token as `Authorization: Bearer <token>` on workspace API requests. Keep
assertions and tokens out of URLs and logs. The exchange endpoint authenticates
the assertion itself and requires no existing Engine API key or bearer token.

The deployment edge must route the exact token endpoint to Auth and forward
Bearer credentials on authenticated business requests. Those edge configuration
changes are owned by the deployment guide; issuer connectivity and trust must
be installed before enabling federation.

## Configure the TypeScript SDK

Use an explicit federation config with an assertion provider file. The provider
or workload owns renewing that file before a later exchange. A persistent SDK
client caches its Engine bearer in memory:

```typescript
import Tetral from '@tetral-ai/sdk';

const client = new Tetral({
  apiKey: null,
  authToken: null,
  baseURL: 'https://engine.example',
  config: {
    authentication: {
      type: 'oidc_federation',
      federation_rule_id: '<registered rule ID>',
      identity_token: {
        source: 'file',
        path: '/private/provider-assertion.jwt',
      },
      // For a provisioned service binding only:
      // service_account_id: '<Engine service selector>',
    },
    organization_id: '<registered organization namespace>',
    workspace_id: '<granted workspace ID>',
  },
  maxRetries: 1,
});

const sessions = await client.beta.sessions.list();
```

The explicit `null` credential options avoid selecting an ambient API key or
static bearer instead of the configured provider. The SDK exchanges through the
JSON endpoint above, caches the resulting token, and can refresh once after a
401 when retries are enabled. Engine rechecks current authority on every
request. A revoked grant or disabled identity can therefore invalidate a cached
token before its expiry; a refresh succeeds only if the identity still has
eligible authority and its current provider assertion remains valid.

## Credential precedence and attribution

When a request selects an API key under the existing key-header rules, Auth
uses that key. An invalid selected key returns 401 even if a valid bearer is
also present. Without a selected key, Auth requires one unambiguous Bearer
credential. Missing or invalid credentials return 401; a valid principal denied
by an operation policy returns 403. Workspace-isolated resource lookups retain
their existing not-found behavior.

Memory history records the actor that made each direct write or redaction:

| Credential | Actor JSON |
|---|---|
| Independent or identity-derived API key | `{"type":"api_actor","api_key_id":"<actual key ID>"}` |
| Direct human bearer | `{"type":"user_actor","user_id":"<stable Engine identity ID>"}` |
| Direct service bearer | `{"type":"service_actor","service_id":"<stable Engine identity ID>"}` |
| Session runtime write | Existing `session_actor` attribution |

A service identity retains its service actor. Its Engine identity ID, external
provider subject, and optional service-account selector serve different purposes.

An identity-derived API key retains the issuing identity's rule and workspace
grant lineage and cannot widen its operation or workspace ceiling. It has its
own expiry and revocation. Expiring or revoking the parent bearer token does not
revoke that separately issued key; disabling or revising the identity, rule, or
grant invalidates the shared authority. Revoking one derived key does not
recursively revoke other keys issued from the same grant.

## Verification

Run the repository's native profile from its root:

```sh
make test-full
```

The runner provisions private PostgreSQL and HTTPS Keycloak dependencies and
the exact pinned SDK source. `TestOIDCKeycloakSDK` composes real human and service
assertions, the actual Auth command, API services and the unchanged SDK. It checks
cached bearer reuse, the ordered 401/exchange/retry path, durable effects and raw
Memory actor responses. The pinned SDK's generated actor types do not yet include
`service_actor`; that response typing requires the separate SDK companion.

`TestOIDCReplicaAuthority` starts two actual Auth processes against the same
database and replays frozen bearer bytes through committed authority changes.
Its controlled HTTPS issuer counts requests before responses, so normal bearer
checks prove they do not contact the identity provider.

`TestOIDCKeycloakAuthRotation` uses real realm signing-key controls and sends
old and fresh assertions through the production Auth verifier. It checks signing
overlap, passive old keys, and retired-key rejection after the configured cache
lifetime at the same federation-rule revision. A retired upstream signer does
not itself revoke already issued, otherwise valid Engine tokens. Exact cache
expiry boundaries and request-count limits have separate deterministic verifier
tests. `TestOIDCEdgePreservesExchangeAndBearerBoundary` checks the integration
edge's exact exchange bypass and trusted credential forwarding; it does not
claim deployed edge configuration is installed.
