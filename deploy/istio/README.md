# Internal routing prerequisite

Tetral requires Istiod and Envoy routing even with one replica. The selected
Istio release, chart archive hashes and matching discovery/proxy image digests
are owned by [the dependency lock](../dependencies.lock.json). Kubernetes
1.32–1.36 is supported by this Istio release. The chart revision is `1-31-1`;
workload annotations pin the proxy image to the same release and immutable digest.

The unmodified upstream archives are retained in `charts/`, with their upstream
Apache 2.0 license. Their original URLs and SHA-256 hashes are in the
dependency lock. Verify and render them locally:

```bash
python3 deploy/istio/render.py --charts-dir deploy/istio/charts --output ./istio.yaml
```

The renderer verifies both archive hashes and the discovery image, then renders
base and Istiod without a cluster or kubeconfig. `--trust-domain` changes the
mesh trust domain; use the same `routing.trustDomain` when rendering Tetral.
It is not an installation command or evidence of installed traffic behavior.

Before installing the rendered prerequisite, the operator must supply `cacerts`
in `istio-system` with `ca-cert.pem`, `ca-key.pem`, `cert-chain.pem` and
`root-cert.pem`. These are an issuing intermediate certificate/key, its complete
chain and the public root bundle. Do not mount the offline root signing key.
The selected upstream chart makes this Secret optional. This renderer makes
that exact volume and all four keys mandatory, so absence blocks Istiod startup
instead of silently generating another root. External issuer, trust-transition
and Secret delivery procedures remain operator responsibilities.

The workload chart uses native Kubernetes sidecars, waits for proxy startup,
and keeps the proxy through application shutdown. The proxy shutdown allocation
(`terminationDrainDuration`) is five seconds for every routed role except
Runtime, whose allocation follows `lifecycle.runtimeProxyJoinMs` (default 5000
ms) and is validated within `lifecycle.runtimeGraceSeconds`. Mesh-wide proxy
access logging is disabled (`meshConfig` sets no `accessLogFile`), so routed
RPCs, including Queue polling and heartbeats, write no per-request proxy lines;
proxy failures surface through application records and proxy statistics.
Routing NetworkPolicies allow only the selected Istiod revision on port 15012,
in addition to each service's existing peers. Queue and Web gain receiver
proxies in the hardened profile; six routing roles are mandatory in both
profiles.

Local Docker fixtures execute the locked proxy and production Bun images. They
verify file-SDS replacement, full handshakes, exact peer identities, rejected
credentials, preserved in-flight RPCs, expiry and native store consumers.
These checks establish local transport boundaries; they do not establish
Kubernetes issuance, interception, installed policy or cluster distribution.
