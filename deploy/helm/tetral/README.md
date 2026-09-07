# Tetral Helm chart

This chart installs the same Tetral platform objects as the canonical
manifests under `deploy/kubernetes`. Default values render those 61 objects
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
   `app.kubernetes.io/name=tetral-postgres`: nine NetworkPolicies select that
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
   three canonical `CiliumNetworkPolicy` objects, one API-server entity
   allowance each for agent-runtime, bridge, and gateway. Cilium does not
   match the Kubernetes API service through an `ipBlock` under the tested
   policy mode, so the three entity rules admit both the service port 443 and
   the node-direct port 6443. This behavior was verified on Cilium 1.19.6 with
   kube-proxy replacement disabled.

   A non-Cilium cluster must set `cilium.enabled=false`. That removes the
   three Cilium objects and leaves those workloads' API-server path to
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
  - `apiServerPeers` — the kubeadm-style `10.96.0.1/32`, in the three ordinary
    NetworkPolicies whose workloads call the API server (agent-runtime for its
    runtime contract, bridge for pod visibility, gateway for TokenReview on
    bearer-authenticated requests). Missing bearer credentials are rejected
    before TokenReview, and health endpoints do not use it. A non-Cilium
    cluster whose service CIDR differs must override this value; the failure
    surfaces as stalled work, not as a failed install. On Cilium, the three
    entity policies described in prerequisite 3 carry this path instead;
    `apiServerPeers` remains the non-Cilium fallback.
  - `databasePeers` and `databasePort` — the in-cluster database pod label and
    `5432`, in nine policies. The port must match the DSN in the Secrets;
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
- `cilium.enabled` controls the three API-server Cilium objects.
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

The gateway and sandbox egress-intent annotations derive hostnames from the
same non-secret endpoint values as their ConfigMaps. The api and Bridge blob
endpoints are Secret-sourced, so their egress-intent annotations remain
operator-advisory canonical literals; the chart cannot verify a value it
cannot read.

Sandbox provider completions and Queue lease wait times are logged at the
default info level. Set `sandbox.debugLogging: true` only while diagnosing
Sandbox queue waits or provider commands; logged summaries remain bounded and
exclude command bodies, credentials, tokens, headers, and mount URLs. Queue
notifications are wake hints: Bridge and Sandbox reconnect their PostgreSQL
listeners and retain timer polling as fallback, while Queue `Lease` remains the
execution authority.
The Sandbox over-limit reconciler, expired output-capture sweep, and
resource-prefix garbage collector are deliberate poll-only maintenance loops;
latency-relevant business Queue runners receive the PostgreSQL wake hint.

The following remain deliberately fixed for the initial numbered Alpha line:

- `tetral-system` and `tetral-agent-runtime`, including every service FQDN,
  NetworkPolicy namespace selector, RBAC subject, and service-account
  allow-list.
- Deployment replicas and the two HPA definitions. A replicas value for
  gateway or git-proxy would fight the HPA on every upgrade.
- Repository names within the four image families and all other canonical
  security and topology literals.

## Upgrade and rollback

Database preparation is a separate deployment step, not an API startup action
or an automatic Helm hook. Use the same immutable release revision for
`tetral-db-prepare` and the workloads.

For an upgrade that changes the schema:

1. Record the current release and take a restorable database backup. Register
   the candidate's numbered Daytona snapshot from its matching sandbox image
   digest before testing tool execution.
2. Stop admission and drain or stop application workloads, including runtime
   workers. Suspend cleanup scheduling and autoscalers so they do not recreate
   stopped workloads. Preserve PostgreSQL and application data. This is a
   maintenance-window upgrade; the chart does not orchestrate this pause.
3. Run `tetral-db-prepare` with administrative credentials and the existing
   operator-selected role declarations. The [bootstrap command examples](../../../docs/bootstrap.md)
   show source and one-shot Pod execution. Wait for exit zero, confirming both
   migration and role application completed. On any failure, keep the rollout
   stopped and inspect the structured logs; committed migrations remain applied.
4. Apply the matching workload revision with plain `helm upgrade`, then restore
   the intended worker, scheduler and autoscaler state and verify readiness.
5. Run the release's rehearsal checks before reopening normal traffic.

```bash
helm upgrade tetral ./deploy/helm/tetral -f values.yaml
```

All eleven DB-connected containers now only verify schema and serving-role
state. If preparation was skipped or failed, they stop startup rather than
repairing the database. `agent-runtime` has no direct database connection.
There is no API-first rollout barrier. Do not let old and new binaries overlap
across a schema change unless compatibility has been independently established:
running processes do not recheck schema, and restarted old binaries reject
newer schema history.

For a release without schema changes, run preparation before rollout if the
role contract changes. A normal rolling upgrade still requires application
compatibility; schema equality alone does not prove it.

**Rollback:** migrations are forward-only. `helm rollback` changes workloads,
not the database. After V2 commits, V1-only binaries reject it as `schema_ahead`;
returning to that release requires an explicit compatible database recovery or
a forward fix. A nonzero preparation exit does not imply the whole upgrade was
rolled back: migration versions and role application use separate transactions.

Do not use `--atomic` as a database rollback mechanism. `--wait` can observe
workload readiness but neither validates rehearsal nor restores the database.
On a failed first install, `--atomic` also uninstalls the release and, when
`namespaces.create=true`, can delete namespaces and their operator-owned contents.

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
