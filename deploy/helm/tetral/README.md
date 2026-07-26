# Tetral Helm chart

This chart installs the same Tetral platform objects as the canonical
manifests under `deploy/kubernetes`. Default values render those 59 objects
without adding Helm-specific labels or annotations to the templates.

## Prerequisites

Complete prerequisites 1–7 before running the install command. Prerequisite 8
finishes bootstrap after API has created the schema.

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

2. **Run PostgreSQL inside `tetral-system`.** Label its pods
   `app.kubernetes.io/name=tetral-postgres`. Nine NetworkPolicies select that
   label without a namespace selector, so an external or differently
   namespaced database is unreachable under the shipped policy.

3. **Install the Cilium CRDs or disable their object.** Defaults preserve the
   canonical `CiliumNetworkPolicy`. A non-Cilium cluster must set
   `cilium.enabled=false`, but that policy is git-proxy's only GitHub egress
   permission. On a policy-enforcing non-Cilium CNI, the operator must supply
   an equivalent GitHub egress allowance or git-proxy cannot operate.

4. **Install ingress-nginx before enabling the edge.** `edge.enabled=true`
   renders three Ingresses with `ingressClassName: nginx`. Their nginx
   `auth-url`, `auth-snippet`, and related annotations enforce the external
   authentication boundary; they are security controls, not portable
   decoration. Do not enable the edge with another ingress controller.

5. **Create all in-cluster Secrets.** The chart creates no Secret values.
   Create the following 17 Secrets with the keys referenced by the canonical
   manifests, using the names selected under `secrets`:

   ```text
   api-database
   api-secrets
   auth-bootstrap
   auth-database
   auth-internal-principal
   bridge-daytona
   gateway-web-blob
   gateway-web-keypool
   queue-database
   runtime-binding-token
   sandbox-blob
   sandbox-daytona
   sandbox-internal-grpc
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

   This is required even when the chart's edge objects are disabled. The two
   Tetral namespaces need no custom label; Kubernetes supplies their
   `kubernetes.io/metadata.name` labels.

7. **Use restricted database roles.** The nine Go DB-connected containers
   reject superusers and roles with row-security bypass privileges at startup.
   The TypeScript `provider-gateway` and `mcp-connector` currently perform only
   the schema check, so using a privileged DSN causes an asymmetric and unsafe
   partial startup. Use non-superuser, non-bypass roles for every container.
   The api role additionally needs DDL rights because api owns migrations.

8. **Seed the bootstrap workspace.** Auth resolves
   `bootstrapWorkspaceID` against the `workspaces` table during startup.
   Set that value to the chosen ID, install the platform so API can migrate the
   schema, allow Auth to crash-loop while the row is absent, then run the
   one-shot seed and let Auth self-heal. The default `existing-workspace-id` is
   a placeholder. Follow the complete
   [from-zero bootstrap sequence](../../../docs/bootstrap.md) for key
   generation, the Secret inventory, and the seed command.

## Install

Copy `values.yaml`, replace every placeholder and operator-specific endpoint,
then install the local chart:

```bash
helm install tetral ./deploy/helm/tetral -f values.yaml
```

Published releases install from GHCR:

```bash
helm install tetral oci://ghcr.io/tetral-ai/charts/tetral \
  --version 0.1.0-alpha \
  -f values.yaml
```

Every object has an explicit namespace. `helm install -n another-namespace`
does not relocate the platform.

Choose exactly one ownership path per cluster. Helm does not adopt objects
created from `deploy/kubernetes`; migration between kubectl and Helm ownership
is outside this chart.

## Values

The chart parameterizes only axes already present in the canonical manifests:

- `image.registry` and `image.tag` select all four image families:
  `tetral`, `gateway`, `agent-runtime`, and the `sandbox` artifact reference.
  An empty `image.tag` resolves to `.Chart.AppVersion`.
- `secrets.*` selects the names of operator-created Secrets; no Secret content
  is rendered.
- `daytona.*`, `blob.*`, `web.*`, `gitProxyHost`, and
  `bootstrapWorkspaceID` select environment endpoints and placeholders.
  `gitProxyHost` drives the sandbox config, git-proxy public URL, and edge
  host/TLS entry together.
- `observability.deploymentEnvironment` and
  `observability.serviceVersion` drive all eleven container sites.
- `resources.*` carries the thirteen workload-container request/limit blocks.
  The other five `resources:` mappings in the canonical YAML are fixed RBAC
  resource-name lists, not container budgets, so the chart has no values for
  them.
- `cilium.enabled`, `edge.enabled`, `edge.tlsSecretName`, and
  `namespaces.create` control the explicitly enumerated optional objects.

The gateway and sandbox egress-intent annotations derive hostnames from the
same non-secret endpoint values as their ConfigMaps. The api and Bridge blob
endpoints are Secret-sourced, so their egress-intent annotations remain
operator-advisory canonical literals; the chart cannot verify a value it
cannot read.

The following remain deliberately fixed for `0.1.0-alpha`:

- `tetral-system` and `tetral-agent-runtime`, including every service FQDN,
  NetworkPolicy namespace selector, RBAC subject, and service-account
  allow-list.
- Deployment replicas and the two HPA definitions. A replicas value for
  gateway or git-proxy would fight the HPA on every upgrade.
- Repository names within the four image families and all other canonical
  security and topology literals.

## Upgrade and rollback

Use plain `helm upgrade` as the supported upgrade mode:

```bash
helm upgrade tetral ./deploy/helm/tetral -f values.yaml
```

Helm applies every object at once. All eleven DB-connected containers validate
schema state at boot. Api alone runs forward migrations under a
pinned-connection advisory lock. During an upgrade, new non-api pods enter
`CrashLoopBackOff` until api completes migration; operators observe restart
counts, not merely unready pods. Existing ReplicaSet pods continue serving. At
the manifest scale of one or two replicas, the default RollingUpdate 25%
`maxUnavailable` rounds down to zero. At HPA scale, gateway and git-proxy may
make up to two of ten old replicas unavailable, so old capacity remains at
least 80% while replacements wait.

The cleanup CronJob has no old ReplicaSet: its per-minute Jobs fail with
`schema_behind` until api finishes. `agent-runtime` has no database and is not
part of schema convergence.

There is an unavoidable schema-ahead window after api reaches N+1 while old
N pods are still serving; running pods do not re-check schema. The chart does
not guarantee one-version-back migration compatibility.

> **Rollback warning:** `helm rollback` across a schema migration causes a
> total outage. Migrations are forward-only, and the rolled-back api fails on
> `schema_ahead`. Rollback is safe only between versions with no schema change.

Do not use `--atomic` or `--wait` as an upgrade safety mechanism. `--atomic`
waits for readiness, so a crash-loop window can exceed its timeout and trigger
the unsafe rollback. `--wait` alone can time out and leave the release applied
but not converged; it does not roll back. On a failed first install, `--atomic`
uninstalls the release and, when `namespaces.create=true`, deletes both
namespaces and their operator-owned contents.

`deploy/kubernetes/rollout-schema-ordered.sh` is a partial nine-file re-roll
tool, not an installation path and not a strict-order alternative to Helm.

## Ownership metadata

The chart does not add Helm labels. Helm writes release ownership metadata
server-side. The canonical manifests contain
`app.kubernetes.io/managed-by: kustomize` 20 times: 17 top-level labels on api,
auth, and event-stream objects are rewritten to `Helm` during installation,
while the three pod-template copies survive as `kustomize`. Selectors use only
`app.kubernetes.io/name`, so this does not change matching, but installed
objects are not byte-identical to the files.

## Registered follow-ups

Four follow-ups are intentionally outside this chart:

1. Parameterize the two fixed namespaces as one security-reviewed change.
2. Define migration compatibility or a documented stop-the-world upgrade mode.
3. Drop stale `kustomize` managed-by labels from the canonical manifests.
4. Port the runtime-role privilege check to the two TypeScript gateway
   containers.
