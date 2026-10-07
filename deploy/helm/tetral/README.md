# Tetral Helm chart

This chart installs the same Tetral platform objects as the canonical
manifests under `deploy/kubernetes`. Default values render those 80 objects
without adding Helm-specific labels or annotations to the templates.

## Prerequisites

Complete prerequisites 1–8 before running the install command. Database
preparation and workspace seeding run independently before any service starts.

1. **Create the two namespaces.** The default chart does not own namespaces:

   ```bash
   kubectl create namespace tetral-system
   kubectl create namespace tetral-agent-runtime
   ```

   `namespaces.create=true` is available for disposable clusters only.

   > **Destructive uninstall warning:** When the chart creates the namespaces,
   > `helm uninstall` deletes both namespaces and everything operators placed
   > in them, including all supplied Secrets and an in-namespace PostgreSQL.
   > This is why `namespaces.create` defaults to `false`.

2. **Make PostgreSQL reachable under the policy.** The default is an
   in-cluster pod in `tetral-system` labelled
   `app.kubernetes.io/name=tetral-postgres`: eleven NetworkPolicies select that
   label without a namespace selector. A database in another namespace, or one
   reachable at a stable address range, is admitted by overriding
   `network.databasePeers` — a `namespaceSelector` with a `podSelector`, or an
   `ipBlock`. Note the limit of the mechanism: a `NetworkPolicyPeer` cannot
   name a hostname, so a managed endpoint whose address is dynamic cannot be
   expressed as a CIDR that stays correct; those deployments need a policy
   engine with FQDN support alongside this chart. A `CiliumNetworkPolicy` is
   one, and it composes with what the chart ships — Cilium unions its allows
   with the Kubernetes policies — but a `toFQDNs` rule does nothing on its
   own: the same policy must also carry an L7 DNS visibility rule for those
   pods. The chart does not provide a managed-database FQDN policy.
   Match `network.databasePort` to the DSN as well; a peer alone does not
   admit a port the policy never names. The database address itself always
   comes from the DSN in the Secrets; the peer list only decides what the
   policy admits.

3. **Install the Cilium CRDs or disable their objects.** Defaults preserve
   six canonical `CiliumNetworkPolicy` objects, one API-server entity
   allowance each for agent-runtime, bridge, job-runner, provider-gateway,
   mcp-connector and web-connector. Cilium does not
   match the Kubernetes API service through an `ipBlock` under the tested
   policy mode, so the six entity rules admit both the service port 443 and
   the node-direct port 6443. This behavior was verified on Cilium 1.19.6 with
   kube-proxy replacement disabled.

   A non-Cilium cluster must set `cilium.enabled=false`. That removes the
   six Cilium objects and leaves those workloads' API-server path to
   `network.apiServerPeers`.

   `cilium.gitProxyFQDNPolicy=true` is a separate opt-in network-layer GitHub
   restriction and requires `cilium.enabled=true`. It requires a CNI whose L7
   DNS interception works. That interception is measured broken on Cilium
   1.19.6 with k3s, VXLAN, and legacy host routing (upstream
   cilium/cilium#46284); enabling the flag there makes git-proxy unable to
   resolve any name.

4. **Install ingress-nginx before enabling the edge.** `edge.enabled=true`
   renders three Ingresses with `ingressClassName: nginx`. Their nginx
   `auth-url`, `auth-snippet`, and related annotations enforce the external
   authentication boundary; they are security controls, not portable
   decoration. Do not enable the edge with another ingress controller.

5. **Create all in-cluster Secrets.** The chart creates no Secret values.
   Create the following 15 Secrets with the keys referenced by the canonical
   manifests, using the names selected under `secrets`:

   ```text
   api-database
   api-secrets
   auth-bootstrap
   auth-database
   auth-internal-principal
   gateway-web-blob
   gateway-web-keypool
   queue-database
   runtime-binding-token
   sandbox-blob
   sandbox-daytona
   sandbox-r2-parent
   tetral-blob
   tetral-database
   tetral-event-stream-database
   ```

   When `edge.enabled=true`, also create the TLS Secret selected by
   `edge.tlsSecretName` (`git-proxy-tls` by default).

   Also create the `tetral-store-trust` ConfigMap selected by
   `transport.storeTrustConfigMap`, carrying the public `database-ca.crt` and
   `object-store-ca.crt`. Every PostgreSQL and object-store consumer mounts it;
   see [Internal routing and protected stores](#internal-routing-and-protected-stores).

6. **Label the ingress-controller namespace.** The public NetworkPolicies are
   always rendered and admit port 8080 only from namespaces carrying:

   ```bash
   kubectl label namespace <ingress-controller-namespace> \
     tetral.ai/network-role=public-ingress
   ```

   This is required even when the chart's edge objects are disabled, and it
   is required under the default `network.publicIngressPeers`: a deployment
   that replaces that value — with a load-balancer CIDR, for instance — admits
   its edge by that peer instead and needs no label. The two Tetral namespaces
   need no custom label; Kubernetes supplies their
   `kubernetes.io/metadata.name` labels.

7. **Prepare the database.** Use
   `go run ./cmd/tetral-db-prepare` with an administrative connection in
   `TETRAL_DATABASE_ADMIN_URL` and a JSON declaration on stdin containing the
   operator-chosen role names and passwords for every workload key in
   `database/roles.json`, plus `migration`. Run it before installing workloads.
   The command idempotently constructs the current schema, revokes public access,
   gives each serving workload only its declared tables and operations, and
   assigns schema objects to the separate migration owner. Put the API serving
   DSN in the `url` key of `api-database`. Keep administrative and schema-owner
   DSNs out of serving workload Secrets. All serving processes reject superuser and row-security
   bypass roles before readiness.

8. **Seed the bootstrap workspace.** Auth resolves
   `bootstrapWorkspaceID` against the `workspaces` table during startup.
   Set that value to the chosen ID, prepare the database, and run the
   one-shot `tetral-bootstrap` command before installing workloads. The default `existing-workspace-id` is
   a placeholder. Follow the complete
   [from-zero bootstrap sequence](../../../docs/bootstrap.md) for key
   generation, the Secret inventory, the seed command, and the Daytona
   sandbox-snapshot registration that must happen before the first tool
   execution.

## Install

Copy `values.yaml`, replace every placeholder and operator-specific endpoint,
then install the local chart:

```bash
helm install tetral ./deploy/helm/tetral -f values.yaml
```

Published releases install from GHCR:

```bash
helm install tetral oci://ghcr.io/tetral-ai/charts/tetral \
  --version <version-without-leading-v> \
  -f values.yaml
```

Choose the version from GitHub Releases. If no `v0.1.0-alpha.N` release is
listed, no public Alpha is available. The release's Candidate Manifest names
the exact four image digests; use those values rather than moving tags.

Every object has an explicit namespace. `helm install -n another-namespace`
does not relocate the platform.

Choose exactly one ownership path per cluster. Helm does not adopt objects
created from `deploy/kubernetes`; migration between kubectl and Helm ownership
is outside this chart.

## Values

The chart parameterizes only axes already present in the canonical manifests:

- `image.registry` and the effective image tag select development images from a
  source checkout. An empty `image.tag` uses the Chart `appVersion`; an explicit
  value overrides it. `image.digests` selects the four release image families:
  `tetral`, `gateway`, `agent-runtime`, and `sandbox`. A non-empty digest takes
  precedence for workload images. Published release values supply all four
  digests together. The API separately uses
  `image.registry/sandbox:<effective image tag>` as the stable Daytona snapshot
  lookup name. A packaged release therefore uses its numbered `appVersion`;
  operators register that name from the matching immutable sandbox image digest
  before rollout.
- `secrets.*` selects the names of operator-created Secrets; no Secret content
  is rendered.
- `daytona.*`, `blob.*`, `web.*`, `gitProxyHost`, and
  `bootstrapWorkspaceID` select environment endpoints and placeholders.
  `gitProxyHost` drives the sandbox config, git-proxy public URL, and edge
  host/TLS entry together.
- `network.*` carries the peers and the port for the dependencies whose
  location belongs to the cluster rather than to Tetral. Every peer list is a
  plain NetworkPolicy peer list accepting any peer shape that API allows —
  except `ciliumDNSEndpointSelectors`, which is a Cilium endpoint selector in
  Cilium's own label syntax. The defaults reproduce the canonical manifests
  exactly, and an empty list is refused at render time: Kubernetes reads an
  empty peer list as "match everything", so emptying one widens the policy
  instead of narrowing it.
  - `apiServerPeers` — the kubeadm-style `10.96.0.1/32`, in six ordinary
    NetworkPolicies. Runtime, Bridge, Provider Gateway, MCP and Web call
    TokenReview for inbound bearer authentication; Job Runner uses the API
    only for Runtime Pod and EndpointSlice visibility. Missing bearer
    credentials are rejected
    before TokenReview, and health endpoints do not use it. A non-Cilium
    cluster whose service CIDR differs must override this value; the failure
    surfaces as stalled work, not as a failed install. On Cilium, the six
    entity policies described in prerequisite 3 carry this path instead;
    `apiServerPeers` remains the non-Cilium fallback.
  - `databasePeers` and `databasePort` — the in-cluster database pod label and
    `5432`, in eleven policies. The port must match the DSN in the Secrets;
    managed PostgreSQL often listens elsewhere, and a pooler in front of it
    often does.
  - `publicIngressPeers` — the labelled ingress namespace, in four policies.
    An edge that is not a pod, such as a cloud load balancer, is admitted by
    replacing this with the peer that describes it.
  - `dnsPeers` — `kube-system` plus `k8s-app=kube-dns`, in ten ordinary
    NetworkPolicies, and
    `ciliumDNSEndpointSelectors` — the same dependency for the opt-in
    git-proxy FQDN policy. When `cilium.gitProxyFQDNPolicy=true`, that policy
    carries the L7 DNS rule that teaches Cilium the addresses behind its
    `toFQDNs` allowance; override both selectors together. One topology
    neither value reaches is NodeLocal DNSCache, where the resolver runs on
    the host network: an `ipBlock` for the link-local address expresses it
    under some CNIs and not under others.
  - `externalEgressPeers` and `externalEgressPorts` — outbound traffic for the
    five workloads that reach
    third-party APIs, unrestricted by default because those endpoints are
    operator-chosen and resolve dynamically, on port 443 by default. If a
    provider, sandbox, blob, or web endpoint URL carries an explicit port,
    list it in `externalEgressPorts`. NetworkPolicy is a union of allows, so a
    deployment that requires an egress gateway or a fixed provider range
    narrows the peer list here; adding a second, tighter policy cannot revoke
    what this one permits.
- `observability.deploymentEnvironment` and
  `observability.serviceVersion` drive all eleven container sites.
- `resources.*` carries the thirteen workload-container request/limit blocks.
  The other five `resources:` mappings in the canonical YAML are fixed RBAC
  resource-name lists, not container budgets, so the chart has no values for
  them.
- `cilium.enabled` controls the six API-server Cilium objects.
  `cilium.gitProxyFQDNPolicy` adds the opt-in git-proxy Cilium policy and
  replaces its ordinary DNS and external-HTTPS NetworkPolicy branches with
  the FQDN restriction. `edge.enabled`, `edge.tlsSecretName`, and
  `namespaces.create` control the other explicitly enumerated optional
  objects.

By default, git-proxy uses `externalEgressPeers`:`externalEgressPorts` like
the other outbound workloads. Its GitHub-only guarantee is enforced in the
application layer: the upstream is hardcoded to `https://github.com`, only
git endpoint route shapes are accepted, and ambient proxy environment
variables are ignored. Setting `cilium.gitProxyFQDNPolicy=true` restores the
network-layer FQDN restriction subject to the CNI limitation above.

Sandbox and Web egress-intent annotations derive provider, search and reader
hostnames from the same non-secret values as their ConfigMaps. Bridge, Job
Runner, Web and Sandbox derive the object-store hostname from `blob.endpoint`.
Bridge, Job Runner and Web read their actual endpoint from Secrets
(`tetral-blob` key `endpoint`, `gateway-web-blob` key `TETRAL_BLOB_ENDPOINT`),
so their object-store annotation is advisory and correct only while
`blob.endpoint` names the same host. The api annotation remains an advisory
canonical literal. The chart cannot verify Secret values.

Sandbox provider completions and Queue lease wait times are logged at the
default info level. Set `sandbox.debugLogging: true` only while diagnosing
Sandbox queue waits or provider commands; logged summaries remain bounded and
exclude command bodies, credentials, tokens, headers, and mount URLs. Queue
notifications are wake hints: Job Runner and Sandbox reconnect their PostgreSQL
listeners and retain timer polling as fallback, while Queue `Lease` remains the
execution authority.
The Sandbox over-limit reconciler, expired output-capture sweep, and
resource-prefix garbage collector are deliberate poll-only maintenance loops;
latency-relevant business Queue runners receive the PostgreSQL wake hint.

The following remain deliberately fixed for the initial numbered Alpha line:

- `tetral-system` and `tetral-agent-runtime`, including every service FQDN,
  NetworkPolicy namespace selector, RBAC subject, and service-account
  allow-list.
- Git Proxy replicas and its HPA definition. The separated workload replicas
  and Provider HPA toggle are configurable as documented below; their existing
  autoscaling metrics and thresholds remain fixed.
- Repository names within the four image families and all other canonical
  security and topology literals.

Sandbox Environment build timing is configurable with
`sandbox.environmentBuildWarnAfter` (default `10m`) and
`sandbox.environmentBuildTimeout` (default `30m`). Both must be positive Go
durations, and timeout must exceed the warning interval; invalid values fail
Sandbox startup. These values initialize a build's saved policy on its first
claim, so changing them does not renew deadlines for builds already in progress.

## Database preparation and compatible rollout

Database preparation is a separate deployment step. Run `tetral-db-prepare`
from the same immutable revision as the workloads, with administrative TLS
trust/name configuration and operator-selected roles. The command creates the
complete current schema only in an empty database. An exact current-schema
repeat is idempotent. A predecessor, partial or incompatible schema is rejected;
there is no incremental predecessor migration or old-binary fallback.

For a fresh installation, prepare the empty database and roles, seed bootstrap
state, then install matching workloads. All database consumers verify the exact
current schema and their scoped serving role before readiness. Runner uses the
separate `job_runner` role through `tetral-database/job-runner-url`; it cannot
inherit Bridge's process-registry writes. Provider Gateway and MCP Connector use
the separate `provider_gateway` and `mcp_connector` roles through
`tetral-database/provider-gateway-url` and `tetral-database/mcp-connector-url`;
MCP Connector cannot read provider session bindings or platform provider keys.

A future rollout requires independently demonstrated schema, protocol and
handoff compatibility. Preserve `maxUnavailable: 0`, bounded surge and the
aggregate connection ledger. Readiness before replacement retirement does not
replace application admission or direct-channel fencing. An incompatible
change requires coordinated maintenance and a separately designed data recovery
or forward change. `helm rollback` changes workloads, not database state; it
cannot make a predecessor binary compatible with the current schema. Do not
use `--atomic` as a database recovery mechanism. `--wait` observes workload
readiness and does not establish rollout or transport behavior.

## Ownership metadata

The chart does not add Helm labels. Helm writes release ownership metadata
server-side. The canonical manifests contain
`app.kubernetes.io/managed-by: kustomize` 20 times: 17 top-level labels on api,
auth, and event-stream objects are rewritten to `Helm` during installation,
while the three pod-template copies survive as `kustomize`. Selectors use only
`app.kubernetes.io/name`, so this does not change matching, but installed
objects are not byte-identical to the files.

## Registered follow-ups

Two follow-ups are intentionally outside this chart:

1. Parameterize the two fixed namespaces as one security-reviewed change.
2. Drop stale `kustomize` managed-by labels from the canonical manifests.

## Independent workload replicas and access

Bridge, Job Runner, Sandbox, Provider Gateway, MCP Connector and Web Connector each own
one Deployment, ServiceAccount, Service and metrics/probe port. The chart
removes the former combined Gateway resources and the shared `gateway`
ServiceAccount; the database role contract likewise has no shared `gateway`
role.
Their declared Deployment replica defaults are one. Set `replicas.api`, `replicas.auth`, `replicas.bridge`,
`replicas.jobRunner`, `replicas.sandbox`, `replicas.providerGateway`, `replicas.mcpConnector` or
`replicas.webConnector` independently to a positive integer.

`autoscaling.providerGateway.enabled` defaults to true and retains the existing
HPA: minimum two, maximum ten, CPU utilization target 70 percent. An enabled
`autoscaling.providerGateway.maxReplicas` must be at least two. The HPA controls
Provider Gateway replicas after reconciliation; `replicas.providerGateway` still
sets the initial Deployment count, which may exceed the HPA maximum. Set
autoscaling to false to use that replica count directly, including one replica.
It has no effect on MCP or Web replicas.
Internal Istiod/Envoy routing is mandatory, including a one-replica deployment.
Provider Gateway uses an ordinary ClusterIP Service without session affinity.
Scoped source proxies own per-RPC selection; the Runtime client retains its
channel without installing its own replica load-balancing policy. MCP and Web
also use ordinary ClusterIP Services.

Runner alone watches Runtime Pods and EndpointSlices and controls Runtime RPCs.
In both profiles its placement probes use Runtime TCP 8080, with egress scoped
to the Runtime namespace and pod selector. The same peer permits only the
profile-selected direct command port alongside that probe port.
Bridge alone serves its durable API and has no visibility watch grant. Separate
receiver identities hold TokenReview create permission; Runner has no inbound
TokenReview role. Internal RPC tokens use `tetral-internal-grpc`, while projected
Kubernetes API reviewer/watch tokens retain the API audience. Each database
workload is granted only its own serving DSN: Bridge, Job Runner, Provider
Gateway, MCP Connector, Cleanup, Git Proxy and Sandbox read their own
`tetral-database` keys, and API, Auth, Queue and Event Stream read their own
database Secrets.
Raw, service-owned and rendered Helm tests assert exact ports, selectors,
credential paths, RBAC and NetworkPolicy peers, including denied inherited access.

## Internal routing and protected stores

Install the [locked Istiod prerequisite](../../istio/README.md) before workloads.
Internal routing is not a chart option: the chart fixes the locked Istio release,
and a `routing.revision` other than the locked revision is rejected.
Runtime, Runner, Bridge, Sandbox, Provider Gateway and MCP Connector always
receive the locked proxy. Hardened mode also gives Queue and Web receiver
proxies. Source/destination route scopes are generated from
`files/internal-routing.json`: exact service-account URI identities, service
DNS, business port and permitted profile. Adding a caller requires changing
that inventory and its tests.

Each scoped HTTP/2 route selects `LEAST_REQUEST`, connects within one second,
and has no generic retry. Two consecutive local-origin failures eject an
endpoint for ten seconds; status-based ejection is disabled, max ejection is
100 percent and panic routing is disabled. All-unhealthy traffic fails within
its caller deadline. Bridge's exhaustive descriptor policy owns method
budgets. Its proxy routes have timeout zero so a valid configured application
budget is preserved; Queue routes retain their five-second envelope. A shorter
parent deadline still wins.

The default `transport.profile=standard-routed` exposes Runtime directly to
Runner on plaintext 19090, excluding exactly that port from mesh capture.
Hardened mode exposes TLS 19443 through the Runtime proxy and leaves Bun's
business listener only on 127.0.0.1:9090. It excludes exactly 19443 from capture,
requires TLS 1.3, full certificate verification and exact Runner/Runtime URI
SANs, and disables session resumption. The rendered filter adds one inbound
listener and one static single-loopback cluster. Runtime server leaf/private
key mount only in the proxy; Runner's native client owns its separate mounted
leaf. PodUID, process ID and binding fences remain enforced after TLS. There is
no endpoint/profile fallback or cross-Pod replay.

All deployed PostgreSQL and object-store consumers use native verified TLS.
Supply `transport.storeTrustConfigMap` with `database-ca.crt` and
`object-store-ca.crt`, and set the exact `databaseServerName` and
`blobServerName`. The deployed chart supplies both trust/name references;
explicit TLS construction validates them. Programmatic Bun SQL callers that
omit both explicit TLS options retain URL-selected transport. PostgreSQL URL
sslmode cannot downgrade explicit TLS, and protected Unix sockets are refused.
Protected object stores require HTTPS and use direct connections; environment
HTTP/CONNECT proxies do not own their TLS path. Ordinary store bytes,
create-only writes and database roles remain unchanged.

Go PostgreSQL consumers retain one pool and acquire a validated credential
snapshot for each new connection. A valid trust update closes idle sockets;
admitted transactions retain their connection until completion, then discard
retired connections before returning them to the pool. Object stores retain at
most two HTTP transport generations. Each admitted response body owns its
generation through EOF or Close; new operations use the active transport.
Retirement closes idle sockets immediately and bounds old response bodies to
20 seconds, then closes remaining old sockets. Further updates retain only the
latest mounted reference until the retired transport closes. Bun consumers validate a candidate SQL pool
before activation. Each actual store operation and entire transaction remains
inside one generation-owned `withSQL` callback, including lazy query execution,
issuer work and commit. Replacement owns at most two pools, including the
candidate and draining pool. While both exist, only the latest mount reference
is pending; it is re-read after old-pool closure. Malformed updates preserve
only previously valid material and emit bounded failure observations. Expired
material cannot admit a fresh connection. CA retirement requires bounded drain
of connections established with the retired generation. Shutdown first joins
application producers, then closes both SQL generations within the remaining
process deadline; it does not start a pending replacement.

For hardened direct TLS, supply `tetral-runtime-direct-trust` with `ca.crt`
and separate `tetral-runtime-direct-tls` and `tetral-runner-direct-tls` Secrets
with `tls.crt`/`tls.key`. The Runtime certificate has the configured
`runtimeServerName` DNS and exact Runtime URI SAN; Runner has its exact URI SAN.
CA and complete leaf/key generations are projected atomically by the operator.
The application cannot repair unavailable issuance by creating trust material.
Hardened startup also requires successful initial updates for both named SDS
leaf and validation resources. The chart retains those counters for the fixed
local admin readiness check; socket acceptance alone does not establish TLS
readiness.

Application drains are configured through `lifecycle.*Ms`. The chart passes the
owning parser's milliseconds environment values and rejects drains that exceed
Pod grace after cancellation, resource and proxy joins. Runtime projects the
existing typed phase defaults through `runtimeDrainMs=60000`,
`runtimeSettlementMs=15000`, `runtimeLocalJoinMs=5000` and
`runtimeProxyJoinMs=5000`; the actual Runtime proxy drain annotation follows the
configured proxy phase. `runtimeGraceSeconds=90` must cover all four phases plus
the fixed five-second scheduling/signal margin. These are shared phase deadlines,
not per-Session extensions or preStop sleeps. `settlementAttemptTimeoutMs=5000`
sets the final phase's per-Bridge-attempt cap through
`TETRAL_RUNTIME_SETTLEMENT_ATTEMPT_TIMEOUT_MS`. It accepts 1–2147483647
milliseconds and is clipped by the method, caller and remaining settlement
deadline. It can exceed the total settlement phase and adds no time to Pod grace.
Queue and Web receive the configured
`cancelJoinMs` through `TETRAL_CANCEL_JOIN_TIMEOUT_MS`. Queue and Web drain plus
join must each fit 25 seconds in both profiles, preserving a five-second
signal/proxy margin. Queue and
Web retain their typed drain maxima of 25 and 20 seconds respectively; a shorter
configured join can increase the Queue drain within those bounds.
Both retain a 30-second Pod grace. git-proxy's Pod grace is its drain grace
(`TETRAL_GIT_PROXY_DRAIN_GRACE_SECONDS`, 1800 seconds) plus a five-second
signal margin. Runner's default
30-second drain and five-second cancellation join fit its 45-second grace with
five seconds each for resource joins and proxy drain. Provider Gateway and MCP
retain separate 30-second business drains and configurable five-second forced
cancellation joins (`providerJoinMs`/`mcpJoinMs`); each drain plus join plus
five-second proxy margin must remain strictly below its 60-second Pod grace.
Their database close uses the remaining shared shutdown deadline after worker
join.

## PostgreSQL connection ledger

`tetral-database-connection-budget` records maximum owned pool slots, operator
reserve, server capacity and external consumers. Defaults leave capacity and
external consumers unknown and its status unresolved. These are required
operator bindings before deployment; an unknown shared consumer is not zero.
`databaseReserve=8` is a portable example reservation, not measured server
capacity. Bind every external/admin consumer and available server connections
after server-reserved slots using `transport.externalDatabaseConnections`,
`transport.databaseCapacity` and `transport.databaseReserve`.

The ledger counts the greater of initial Deployment replicas and an enabled
HPA maximum, then resolves absolute or percentage `rollout.maxSurge` against
that bound. Disabled autoscaling counts only initial replicas. Go processes
count one pool; Provider Gateway and MCP count
two generations throughout simultaneous rollout and trust replacement. API,
Auth, Sandbox, Queue and Event Stream each count their replica plus surge; Git Proxy
uses its existing HPA maximum of ten plus surge. Cleanup counts one nonoverlapping job. Pool-backed listeners and worker
concurrency do not add another pool. Separately configured old/new release
cohorts require a combined ledger for both settings. A fully bound capacity
below the total is rejected at render time. The default owned maximum is 780
slots, before reserve and external consumers. Rendered subset fixtures prove
128/127 and 368/367 boundaries without claiming eagerly opened connections.

Regenerate raw/service projections with `python3 deploy/render-manifests.py`.
The default raw directory is the standard profile. The complete alternate raw
set in `deploy/kubernetes/profiles/hardened` replaces the default set; applying
both sets together is unsupported. The local TLS fixtures use controlled
certificates and Docker networks; issuing/policy/rollout behavior still needs
verification in the deployment's actual environment.
