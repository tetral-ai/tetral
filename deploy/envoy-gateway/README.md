# Public Envoy Gateway

Install this prerequisite separately from the Tetral workload chart. The
canonical lock selects Envoy Gateway and matching `egctl` 1.9.2, Gateway API
1.6.1 and the compatible Envoy distroless 1.39.2 security patch. Chart archives,
CRD bytes, tool checksums and immutable controller/proxy image references live
in [the dependency lock](../dependencies.lock.json). Both application profiles
also require the independently installed [Istiod prerequisite](../istio/README.md).

The joint selected Kubernetes range is 1.33–1.36. Verify the actual server patch,
API availability and current dependency security before installation. Existing
compatible provider-owned Gateway API CRDs retain their ownership. Inspect their
served versions and schemas; do not overwrite them solely because this checkout
contains an archive. Missing or incompatible prerequisites block installation.

Create the independently owned controller namespace before installation:

```bash
kubectl create namespace envoy-gateway-system
kubectl label namespace envoy-gateway-system tetral.ai/network-role=public-ingress
```

The generated data plane uses this namespace. Bind any different public ingress
peer explicitly in the application network values rather than widening peers.
Render locally after verifying CRD ownership:

```bash
python3 deploy/envoy-gateway/render.py \
  --charts-dir deploy/envoy-gateway/charts \
  --output /tmp/tetral-envoy-gateway.yaml
```

Use `--include-gateway-api-crds` only when this installation owns the missing
Gateway API CRDs. The renderer verifies the selected archive/CRD bytes, uses the
actual selected Helm chart, pins both images and enables EnvoyPatchPolicy. It
performs no cluster action. Review the rendered resources, independently install
the controller and `tetral-envoy-gateway` GatewayClass, and verify readiness
before enabling `edge.enabled` in the application chart.

The application owns one Gateway with a shared HTTP redirect listener and
distinct API/Git HTTPS listeners, four HTTPRoutes, the API SecurityPolicy,
listener-specific ClientTrafficPolicies, a BackendTrafficPolicy and one reviewed
EnvoyPatchPolicy. Hardened mode additionally owns four BackendTLSPolicies.
Public TLS Secret references are operator-supplied; no private key is embedded
in values. Controller and generated data-plane ServiceAccounts are distinct.

Path normalization precedes routing and Check. Only `POST` on the resulting
exact `/v1/oauth/token` path bypasses Check. Legal character encodings, adjacent
slashes and dot segments that normalize to that path still reach Auth's
assertion-verifying exchange handler. Paths that remain near matches, including
suffixes, trailing slashes and different case, retain credential authorization;
encoded separators and malformed escapes are rejected before dispatch.

The protected API route calls
Auth's gRPC Check on 9095 with a five-second timeout, fail-open disabled, no body
buffering and no retry. A transport failure becomes 503. Invalid credentials,
invalid request metadata and internal Auth failures retain Auth's 401, 400 and
500 JSON envelopes. Check shares the same credential selector and authority
resolver as token exchange and never validates an issuer JWT as a business
credential.

Client traffic policy removes untrusted forwarding, identity and original-path
headers before authorization, normalizes paths, rejects escaped slash/backslash
and generates a fresh request ID. The patch targets only the generated API
external-auth filter and enables raw header encoding and mutation validation.
Raw encoding is required to preserve duplicate credential semantics. A missing
or renamed target is an error even if translation produces other resources.
No URL rewrite follows authorization. Credentials are removed before protected
backend forwarding. HTTP redirect has no business backend.

Git has a separate hostname and ticket boundary. It preserves only the exact
`X-Tetral-Git-Ticket` header from that prefix; all other `X-Tetral-` and all
`X-Original-` names are removed. API credentials are removed on Git routes.
Access logging is disabled across the public Gateway, including Git paths and
queries. Application records provide sanitized correlation and outcomes;
headers, tokens, bodies and query values are excluded from proxy logs.

The raw optional application resources are
[`envoy-gateway.yaml`](../kubernetes/edge-gateway/envoy-gateway.yaml); hardened
resources are projected separately. Regenerate both with
`python3 deploy/render-manifests.py`. The portable inventory names all fourteen superseded
Tetral-owned Ingress, combined Gateway fleet and RBAC objects with their exact
historical ownership labels. Cutover must retire those objects without deleting unrelated ingress
controllers, stores, applications or provider-owned CRDs.

Local owning tests render these production resources, supply every referenced
Service/EndpointSlice/Secret explicitly, run the matching `egctl` and serve its
exact xDS snapshot to the pinned Envoy process. Translation status, policy match
counts and Envoy acceptance are separate assertions. Local TLS and protocol
traffic establishes consumer behavior; controller reconciliation, Kubernetes
Secret propagation and load-balancer behavior require installation evidence.

For native TLS rotation, first project an old-plus-new CA bundle, then issue and
activate new leaves. Probe fresh connections from the edge to Auth Check and
each HTTP backend, including the intended DNS name and peer role. Existing
streams retain their admitted contexts during overlap. Before removing old
trust, withdraw public admission and drain every owning stream, connection pool
and listener; wait for the proxy and affected receiver owners to join. Remove
old trust only after that drain, restart the required owners and verify fresh
connections again. Read formal history through the SDK list API when reopening
an SSE subscription; a reopened stream begins at its current high-water mark
and does not replay old events.

The local TLS composition checks Secret-generation acknowledgment, renewed
public certificates, held SDK streams, fresh native backend and Check
handshakes, explicit connection drain, retired-peer rejection and exact SDK
history recovery. Controller reconciliation, projected Secret delivery and
load-balancer withdrawal remain installation checks. The fixture separates each
proxy process's bootstrap/control credentials from policy translation artifacts
and compares every policy generation against the original official translation.

The unmodified pinned `egctl` ConfigDump omits SecretType resources while its
listeners and clusters reference them through SDS. The local test apparatus
therefore uses the separate
[`envoy-gateway-secret-helper`](../../integration/envoy-gateway-secret-helper)
module to load the exact original fixture YAML and invoke the matching upstream
Gateway API and xDS translators. CLI IR deliberately redacts private keys; the
helper verifies the complete redacted IR against that output, plus the fixture
SHA-256 identity, before preserving the original policy resources. It
compares every original listener, route, cluster and endpoint with the official
translator, rejects conflicting duplicate or missing/unexpected Secret references,
and supplies only the omitted upstream-generated Secrets. It never constructs
replacement proxy policy or changes the checksum-verified CLI. The helper uses
Go 1.26.8 required by upstream; Engine remains on Go 1.25.13. Separate module and
symbol vulnerability scans cover this test-only dependency graph. This repairs
local serialization; it does not establish controller Secret propagation.
The architecture guard allows only its entry point's three protobuf serialization
imports; other helper files and gRPC transport imports remain prohibited.
