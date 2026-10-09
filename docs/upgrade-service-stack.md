# Upgrade to the separated service stack

This guide is for operators of an installation built from release
`v0.1.0-alpha.2` or from a later `main` revision up to `11a85f85`, the
revision recorded as `sourceRevision` in
[`deploy/managed/previous-resource-dispositions.json`](../deploy/managed/previous-resource-dispositions.json).
Both are called the previous release below, and every statement about it holds
for both unless the guide names one. Where a setting existed only in
pre-release builds of this stack, the guide says so. Each topic states the
previous and final behavior or configuration, what the operator does, and what
limits rollback.

The upgrade replaces the previous workloads in their fixed namespaces and
starts the new ones on a new, empty database. There is no in-place schema
migration, no data copy and no mixed-version operation. The
[bootstrap guide](bootstrap.md) and the
[managed installation contract](../deploy/managed/README.md) own the command
sequence; this guide lists what differs from the previous installation and
what cannot be carried over.

## Upgrade sequence

1. Close public admission to the previous installation, for example by
   deleting its three ingress-nginx Ingresses, which this release retires
   anyway. Then stop its workloads: scale the Deployments `api`, `auth`,
   `bridge`, `event-stream`, `gateway`, `git-proxy`, `queue` and `sandbox` in
   `tetral-system` and `agent-runtime` in `tetral-agent-runtime` to zero, and
   delete the `cleanup` CronJob, which also deletes its Jobs. Nothing then
   writes to the previous database. Step 7 starts the new revision's
   Deployments at their configured replica counts and recreates the CronJob.
2. Back up the previous database and retain it unchanged, together with its
   object namespace, the previous Secret values, Helm values or manifests and
   image digests. They are the only way back (see [Rollback](#rollback)).
3. Install the prerequisites the previous release did not need: Kubernetes
   1.33–1.36, the [locked Istiod](../deploy/istio/README.md),
   [Envoy Gateway and the Gateway API CRDs](../deploy/envoy-gateway/README.md),
   the [Core NATS broker](../deploy/nats/README.md) while previews are enabled,
   [cert-manager](../deploy/cert-manager/README.md) for the hardened profile's
   native-certificate automation, verified TLS to PostgreSQL 18 and the object
   store, and the `tetral-store-trust` ConfigMap. The
   [README](../README.md#getting-started) lists them.
4. Create a dedicated empty database and an isolated object namespace. Run
   `tetral-db-prepare` from the new revision with a role declaration for the
   new workload keys ([Database roles and Secret keys](#database-roles-and-secret-keys)).
5. Seed every workspace with `tetral-bootstrap` and import the Auth policy with
   `tetral-auth-policy`.
6. Update the `tetral-database` Secret keys, the shutdown settings and the Helm
   values as described below, and remove keys that no longer exist.
7. Replace the previous objects with the new workloads while public admission
   stays closed:
   - Helm: run `helm upgrade` on the existing release with the new chart and
     the updated values. Helm replaces the objects that keep their name in
     place, creates the new workloads and deletes the
     [retired objects](#retired-objects) the release rendered. A second
     release cannot be installed beside it: every object has a fixed
     namespace, and Helm does not adopt objects it does not own. Do not
     uninstall a release rendered with `namespaces.create=true`; that deletes
     both namespaces with their Secrets and any in-namespace PostgreSQL,
     including the retained previous database.
   - Raw manifests: apply the new revision's manifests with `kubectl apply`,
     which replaces the objects that keep their name in place, and delete the
     [retired objects](#retired-objects) by name, because `kubectl apply`
     leaves them behind. Helm does not adopt objects created by kubectl, so
     such an installation stays on the raw-manifest path.

   Then run the managed contract's inventory check over a list of the
   installation's objects collected from the cluster; it fails while any
   retired object survives. Open public admission only after the checks in the
   managed contract pass.
8. Recreate what the new database does not contain: API keys, platform
   provider keys and any workspace data clients need.

Nothing stored by the previous installation moves to the new database:
Sessions, events, messages, files, memory stores, vaults, credentials, API keys
and Auth policy all start empty. Files stay in the previous object namespace,
which the new installation does not read. Provider Sandboxes the previous
installation created are unknown to the new database, and nothing in the new
installation deletes them. They stay in the Daytona account until Daytona's
auto-delete removes them, under the interval the previous Sandbox service set
on each one at creation (`TETRAL_SANDBOX_AUTO_DELETE_INTERVAL`, 720h in the
previous manifests and chart), unless they are deleted there by hand.

## Database roles and Secret keys

| | Previous | Final |
|---|---|---|
| Role declaration keys for `tetral-db-prepare` | `api`, `auth`, `queue`, `bridge`, `cleanup`, `gateway`, `git_proxy`, `sandbox`, `event_stream`, plus `migration` | `api`, `auth`, `queue`, `bridge`, `cleanup`, `provider_gateway`, `mcp_connector`, `git_proxy`, `sandbox`, `event_stream`, `job_runner`, plus `migration` |
| `tetral-database` Secret keys | `bridge-url`, `cleanup-url`, `gateway-url`, `git-proxy-url`, `TETRAL_POSTGRES_DSN` | `bridge-url`, `cleanup-url`, `git-proxy-url`, `job-runner-url`, `mcp-connector-url`, `provider-gateway-url`, `TETRAL_POSTGRES_DSN` |
| Job Runner database identity | a container in the Bridge Pod using `bridge-url` and the `bridge` role | its own Deployment using `job-runner-url` and the `job_runner` role |
| Provider Gateway and MCP Connector | one combined `gateway` workload using `gateway-url` and the `gateway` role | separate workloads using `provider-gateway-url` with `provider_gateway`, and `mcp-connector-url` with `mcp_connector` |

The declaration must contain exactly the workload keys in
[`database/roles.json`](../database/roles.json) plus `migration`; a declaration
that still names `gateway` or omits a new key is rejected before any schema or
role change. `tetral-db-prepare` installs every table grant, sequence grant and
function grant from that file, so no grant is applied by hand. The final grants
differ from the previous ones in these operator-visible ways:

- Of the two former Gateway halves, only Provider Gateway can read provider
  session bindings and platform provider keys, and it now only reads the
  bindings, which the previous `gateway` role could also insert and update.
  MCP Connector can read neither. API keeps its read-write access to the
  bindings.
- Job Runner and Cleanup no longer read the `workspaces` table. Work these
  roles cannot do through table grants runs through new migration-owned
  `SECURITY DEFINER` functions: the Job Runner binding discovery and
  process-liveness lock, the Cleanup due-Session discovery and receipt/change
  retention, the Queue Job Runner terminal retention, and the Auth lookup, lock
  and prune functions, each executable by that one workload only; and the
  Runtime process lock `tetral_lock_runtime_process`, which Bridge and Job
  Runner share. The [database contract](../database/README.md) lists their
  exact signatures and the owner-checked policies behind them.
- Bridge alone writes the new Runtime process tables (`runtime_process_pods`,
  `runtime_processes` and `runtime_process_liveness`), which Job Runner may only
  read; Bridge and Job Runner may only insert and read the new Assistant part
  rows; Event Stream may only read the new feed retention table.

PostgreSQL role names are cluster-wide. Preparation creates each declared role
or, when the name already exists and carries the marker of the same workload
key, resets its password and attributes. An existing name without that marker,
such as the previous `gateway` role offered as `provider_gateway`, is rejected.
If the new database shares a PostgreSQL cluster with the previous one, declare
new role names for every key. Reusing a previous name changes the password the
previous installation's Secrets hold.

Rollback: previous binaries read `gateway-url` and expect the previous role
set. Keep the previous `tetral-database` values; the new keys are ignored by
previous workloads but the removed `gateway-url` is required by them.

## Shutdown and drain keys

Previously, application drain windows were fixed in code; the only shutdown
setting was Git Proxy's `TETRAL_GIT_PROXY_DRAIN_GRACE_SECONDS`, which is
unchanged (1800 seconds). Pre-release builds of this stack read
`TETRAL_QUEUE_DRAIN_TIMEOUT_MS` for Queue and `TETRAL_SERVICE_DRAIN_TIMEOUT_MS`
for Web Connector, Provider Gateway and MCP Connector.

The seven workloads in the table below now read one drain key,
`TETRAL_DRAIN_TIMEOUT_MS`. API, Auth and Event Stream keep their fixed
10-second drain windows, and Git Proxy keeps its own seconds key. The
cancellation-join keys stay distinct: the Go workloads in the table read
`TETRAL_CANCEL_JOIN_TIMEOUT_MS`, and the TypeScript Provider Gateway and MCP
Connector read `TETRAL_SERVICE_CANCEL_JOIN_TIMEOUT_MS`. These values are
milliseconds, and an invalid value is a startup error.

| Workload | Drain default and range | Join key and default | Joint limit | Helm values |
|---|---|---|---|---|
| [Bridge](../services/bridge/README.md#lifecycle-settings) | 40000, positive; must exceed the admission and release attempt timeouts (3000 and 5000 by default) | `TETRAL_CANCEL_JOIN_TIMEOUT_MS`, 5000 | drain + join ≤ 50000 | `lifecycle.bridgeDrainMs`, `lifecycle.cancelJoinMs` |
| [Job Runner](../services/job-runner/README.md#process-lifecycle) | 30000, positive | `TETRAL_CANCEL_JOIN_TIMEOUT_MS`, 5000 | drain + join ≤ 35000 | `lifecycle.runnerDrainMs`, `lifecycle.cancelJoinMs` |
| [Sandbox](../services/sandbox/README.md#process-shutdown) | 30000, positive | `TETRAL_CANCEL_JOIN_TIMEOUT_MS`, 5000 | drain + join ≤ 50000 | `lifecycle.sandboxDrainMs`, `lifecycle.cancelJoinMs` |
| [Queue](../services/queue/README.md#startup-configuration) | 10000, 1–25000 | `TETRAL_CANCEL_JOIN_TIMEOUT_MS`, 5000 | drain + join ≤ 25000 | `lifecycle.queueDrainMs`, `lifecycle.cancelJoinMs` |
| [Web Connector](../services/web-connector/README.md#process-diagnostics) | 10000, 1–20000 | `TETRAL_CANCEL_JOIN_TIMEOUT_MS`, 5000 (1–25000) | drain + join ≤ 25000 | `lifecycle.webDrainMs`, `lifecycle.cancelJoinMs` |
| [Provider Gateway](../services/gateway/README.md#process-lifecycle) | 30000, positive | `TETRAL_SERVICE_CANCEL_JOIN_TIMEOUT_MS`, 5000 | drain + join + 5000 < 60000 | `lifecycle.providerDrainMs`, `lifecycle.providerJoinMs` |
| MCP Connector (same section) | 30000, positive | `TETRAL_SERVICE_CANCEL_JOIN_TIMEOUT_MS`, 5000 | drain + join + 5000 < 60000 | `lifecycle.mcpDrainMs`, `lifecycle.mcpJoinMs` |

Provider Gateway treats an empty drain or join value as unset; MCP Connector
rejects it. The chart checks every combination against the Pod grace before
rendering
([Helm lifecycle values](../deploy/helm/tetral/README.md#internal-routing-and-protected-stores)).

The Runtime does not read the unified key. Its shutdown keys are new in this
release, and each accepts 1–2147483647:

| Runtime key | Bounds | Default | Helm value |
|---|---|---|---|
| `TETRAL_RUNTIME_DRAIN_TIMEOUT_MS` | current-step phase | 60000 | `lifecycle.runtimeDrainMs` |
| `TETRAL_RUNTIME_SETTLEMENT_TIMEOUT_MS` | settlement phase | 15000 | `lifecycle.runtimeSettlementMs` |
| `TETRAL_RUNTIME_SETTLEMENT_ATTEMPT_TIMEOUT_MS` | each settlement attempt; not a phase and adds no Pod grace | 5000 | `lifecycle.settlementAttemptTimeoutMs` |
| `TETRAL_RUNTIME_LOCAL_JOIN_TIMEOUT_MS` | local-join phase | 5000 | `lifecycle.runtimeLocalJoinMs` |
| `TETRAL_RUNTIME_PROXY_JOIN_TIMEOUT_MS` | proxy-join phase | 5000 | `lifecycle.runtimeProxyJoinMs` |

The chart requires the four phases plus a 5000 ms signal margin to fit
`lifecycle.runtimeGraceSeconds` (default 90). The Runtime's
[process registration and shutdown](../services/agent-runtime/README.md#process-registration-and-shutdown)
section owns these phases.

Operator action: set drains through the `lifecycle.*Ms` values, or set the keys
above in raw manifests. Remove `TETRAL_QUEUE_DRAIN_TIMEOUT_MS` and
`TETRAL_SERVICE_DRAIN_TIMEOUT_MS`; they are no longer read, so a value left
there is silently replaced by the default.

Rollback: previous builds ignore all of these keys and use their fixed
windows.

## Helm values, routing and network

| | Previous | Final |
|---|---|---|
| Internal routing | none | Istiod 1.31.1, installed separately, is required in every profile, even with one replica. Runtime, Job Runner, Bridge, Sandbox, Provider Gateway and MCP Connector always run the routing sidecar; the hardened profile adds Queue and Web Connector. |
| Istio images | none | the unmodified upstream `istio/proxyv2` and `istio/pilot` 1.31.1 images, pinned by digest in [`deploy/dependencies.lock.json`](../deploy/dependencies.lock.json); no custom or patched Istio image is used |
| `routing.*` values | none | `routing.revision` must be `1-31-1`, and any other value fails rendering; `routing.trustDomain` must equal the mesh trust domain used to render Istiod. Pre-release `routing.enabled` and `routing.version` are not values; routing is not optional. |
| Public edge | ingress-nginx Ingresses `tetral-public-api`, `tetral-event-stream` and `git-proxy`, with `edge.tlsSecretName` | Envoy Gateway resources rendered when `edge.enabled=true`, with `edge.gatewayClassName`, `edge.apiHost`, `edge.apiTLSSecretName`, `edge.gitTLSSecretName` and the native trust references; `edge.tlsSecretName` is not read |
| Gateway workload | one combined `gateway` Deployment, HPA, Service, ServiceAccount, ConfigMap, NetworkPolicies and TokenReview RBAC | independent Provider Gateway, MCP Connector and Web Connector Deployments, each with its own ServiceAccount, Service and `replicas.*` value; the Provider Gateway HPA is `autoscaling.providerGateway` |
| Job Runner | a `job-runner` container in the Bridge Pod, sized by `resources.bridgeJobRunner`, with the `bridge-visibility` Role and RoleBinding | its own Deployment sized by `resources.jobRunner` and `replicas.jobRunner`, with the `job-runner-visibility` Role and RoleBinding |
| `cilium.enabled` default | `true` | `false`: six ordinary NetworkPolicies carry the API-server path, so set `network.apiServerPeers` and `network.apiServerPort` to the endpoint NetworkPolicy evaluates after Service translation, typically the control-plane endpoint addresses and their port rather than the `kubernetes` ClusterIP. A mismatch surfaces as stalled work, not as a failed install. Set `cilium.enabled=true` explicitly to keep the six API-server Cilium policies. |
| `network.publicIngressPeers` | the default selector admitted the ingress-nginx controller namespace labelled `tetral.ai/network-role=public-ingress` on port 8080 | it must select the Envoy Gateway data plane, which it admits on business HTTP 8080 and on Auth's Check port 9095. Under the default selector, label `envoy-gateway-system` and remove the label from the ingress-nginx namespace; otherwise replace the value with a peer that describes the Envoy Gateway data plane. |
| New values | — | `network.apiServerPort`, `observability.log*`, `queue`, `jobRunner`, `replicas`, `autoscaling`, `routing`, `transport`, `preview`, `eventStream`, `rollout`, `lifecycle`, `nativeCertificates`, `authIssuerNetwork`; from `v0.1.0-alpha.2`, also `sandbox.environmentBuildWarnAfter` and `sandbox.environmentBuildTimeout` |

The chart has no values schema, so a key it no longer reads is ignored without
an error. Remove `edge.tlsSecretName`, `resources.bridgeJobRunner`, and the
pre-release `routing.enabled`, `routing.version` and `jobRunner.pollIntervalMs`
from override files rather than relying on a render failure. Follow
[Internal routing and protected stores](../deploy/helm/tetral/README.md#internal-routing-and-protected-stores)
and the [Istio prerequisite](../deploy/istio/README.md), including its required
`cacerts` Secret.

Rollback: the previous release uses neither Istiod nor Envoy Gateway.
Restoring it means reapplying its own manifests or chart, which recreate the
retired objects and its Ingresses. Label the ingress-nginx namespace again if
the label was moved: under the default peers, the previous public
NetworkPolicies admit only the labelled namespace. `helm rollback` changes
workloads only, not database state.

## Retired objects

This release retires fourteen objects of the previous installation. The
`retirements` list in
[`deploy/managed/previous-resource-dispositions.json`](../deploy/managed/previous-resource-dispositions.json)
is authoritative:

| Scope | Retired objects |
|---|---|
| `tetral-system` | Deployment, HorizontalPodAutoscaler, Service, ServiceAccount and NetworkPolicy `gateway`; ConfigMap `gateway-config`; CiliumNetworkPolicy `gateway-apiserver-egress`; Ingresses `tetral-public-api`, `tetral-event-stream` and `git-proxy` |
| `tetral-agent-runtime` | Role and RoleBinding `bridge-visibility` |
| cluster | ClusterRole and ClusterRoleBinding `gateway-tokenreview` |

The same file marks every other previous object as retained: the new release
has an object with the same kind, name and namespace that replaces it in
place. The two retained API-server Cilium policies exist in the new release
only with `cilium.enabled=true`.

`helm upgrade` of the existing release deletes the retired objects that the
release rendered. Delete any others by name, and all of them on the
raw-manifest path. The inventory check in the
[managed installation contract](../deploy/managed/README.md) never contacts
the cluster: collect the installation's objects into a JSON list and pass it
to `deploy/managed/validate-inventory.py --observed <list>`. A surviving
retired object fails that check even when the new workloads and public edge
are healthy.

Rollback: the previous release's chart or manifests recreate these objects.

## Git credentials

Previously, the shipped manifests set `TETRAL_GIT_PROXY_LEGACY_PATH_CUTOVER=true`.
Git Proxy then also accepted the Sandbox Git ticket as the first URL path
segment, besides the `X-Tetral-Git-Ticket` header that Sandboxes send.

Git Proxy now reads the ticket only from `X-Tetral-Git-Ticket`. The public URL
is `https://<git-proxy-host>/github.com/<owner>/<repo>[.git]<git-path>`, and only
the four smart-HTTP shapes are accepted. A ticket in the URL path is a `404`
with no ticket validation and no upstream request; `GET /info/refs` accepts
only its single `service` query parameter, and the `POST` endpoints accept no
query. On the Git host the public edge keeps `X-Tetral-Git-Ticket`, strips
every other `X-Tetral-` and `X-Original-` header, and writes no access logs.
See [the request pipeline](../services/git-proxy/README.md#request-pipeline).

Operator action: remove `TETRAL_GIT_PROXY_LEGACY_PATH_CUTOVER`; it is no longer
read. Any client or tooling that put the ticket in a URL must send the header
instead. Do not publish ticket-bearing URLs in documentation or scripts.

Rollback: previous builds accept the URL shape again while their flag is set.

## Fresh database baseline

Previously, `tetral-db-prepare` recorded two to four ordered schema versions
(two at `v0.1.0-alpha.2`, four at `11a85f85`) and upgraded a database that was
behind by applying the missing versions in place.

The final schema is one fresh-install baseline: version one with the checksum
pinned in `internal/schemaidentity`. `tetral-db-prepare` accepts only an empty
catalog or exactly this schema. A previous or pre-release database, a partial
catalog or any other history is rejected without mutation; there is no
migration, no backfill and no fallback. Every database consumer, including
`tetral-bootstrap`, `tetral-auth-policy`, the Go services and the TypeScript
Gateway workloads, verifies the schema identity before readiness. A database
whose recorded version-one checksum differs from the binary's fails with
`schema_checksum_drift` ("postgresql schema checksum does not match this
binary") and the process does not start serving.

Operator action: create a new, empty database and an isolated object
namespace, and prepare it with the new revision as described in
[database preparation](../deploy/helm/tetral/README.md#database-preparation-and-compatible-rollout).
Rebuild every test or staging database that was prepared by an earlier
revision of this stack. Do not point the new workloads at the previous
database.

## Rollback

There is no in-place rollback. Previous binaries cannot use a database
prepared by this revision; they fail the same schema checksum check. The new
binaries cannot use the previous database.

To roll back:

1. Stop the new workloads, or uninstall them unless the release was rendered
   with `namespaces.create=true`.
2. Restore the previous database, either the retained original or its backup.
3. Restore the previous images, manifests or chart and values, and the
   previous Secret contents, including `gateway-url` and, if role names were
   reused on the same PostgreSQL cluster, the passwords those roles had.
4. Recreate the retired objects and Ingresses from the previous release.

Everything written to the new database after cutover is lost on rollback: no
tool moves Sessions, events, API keys or any other data between the two
schemas. `helm rollback` alone restores workload objects, not the database,
roles or Secret contents.

## Database behavior changes

These behaviors change for every installation of this revision. Each row
links the owning README section that holds its complete contract; the
automatic deletion row links its owners in the table that follows.

| Area | Previous | Final | Operator note |
|---|---|---|---|
| [Queue direct leasing](../services/queue/README.md#direct-job-runner-leasing) and [Runner capacity](../services/job-runner/README.md#acquisition) | Job Runner listed workspaces and polled each one at a fixed interval | Job Runner asks Queue for at most its free slots; Queue chooses workspaces and jobs, at most one job per workspace and 16 jobs per call, within a 1000 ms budget, and returns a retry hint of 100–1000 ms. A freed slot refills at once; a committed Queue notification wakes an idle Runner; undispatched leases return through `ReleaseUnstartedJob` at shutdown. | `TETRAL_BRIDGE_JOB_RUNNER_MAX_JOBS` (default 8) is the slot count; there is no poll interval |
| [Pod-loss repair](../services/job-runner/README.md#repair-job-runner-on-proven-gone) | a census per workspace | one repair owner, at startup, every 30 s, and on Pod deletion or a process takeover, at most 1,024 bindings and 8 repairs per run | see the repair latency limit below |
| [Cleanup scheduling](../services/cleanup/README.md#scheduling-phase) | each Cron tick visited every workspace and claimed a batch per workspace, restarting from the beginning | one elected scheduler per tick pages due Sessions across workspaces from a durable cursor: 45 s budget, at most 1000 candidates, pages of at most 100; a failing prefix is crossed on later ticks | failed claims still make the Cron Job exit non-zero |
| [API-key usage](../services/auth/README.md#api-key-usage-sampling) | admission locked the key and wrote `last_used_at` on every request | admission takes a share lock and writes nothing; `last_used_at` is a sampled database time, written about a second later, at most once per five minutes per key, and can be missing or older under overload, failure or shutdown. Access tokens record no usage. | `last_used_at` is not security evidence; drops are counted by `tetral_auth_api_key_usage_submissions_dropped_total` and `tetral_auth_api_key_usage_samples_dropped_total` |
| [Runtime liveness](../services/bridge/README.md#runtime-process-custody) | the previous release had no Runtime process registry; in pre-release builds an unchanged Runtime report locked the Pod and process rows and waited for Session mutations | an unchanged report updates only the process liveness row and never waits for Session mutations or delays promotion. Its acknowledgment grants no mutation authority and cannot revive a retired process. | none |
| [Idempotency keys](../services/api/README.md#events-admission-internalsessionevent) | a missing `Idempotency-Key` stored a receipt under a random key; receipts never expired | a missing header stores no receipt. A supplied key replays or conflicts only while the admission time, read after the receipt lock, is before the receipt's creation plus exactly 24 hours, whether or not Cleanup has deleted the row; an expired key is a new admission that replaces the receipt. | clients retrying with the same key after 24 hours create new events |
| Automatic deletion | none of the rows below were deleted | see the table below | plan storage for the retained families |
| [Feed retention](../services/event-stream/README.md#change-retention-and-the-feed-head) | change rows were kept forever | a stream whose unread position was pruned closes through the normal reader-failure path with no error frame and logs `event_stream.feed_closed` with `reason=retained_history_gap`; a stream that keeps reading is never closed by retention | reopen the stream and read missed events from the event list endpoints, which serve permanent history |
| [Shared idle checks](../services/event-stream/README.md#shared-idle-checks) | every SSE viewer polled its own feed, once per second | one shared check per watched Session, at most 128 Sessions per statement and one round per workspace per poll interval, wakes only viewers whose Session changed; a failed check closes that chunk's viewers and logs `event_stream.idle_check_failed` | database reads scale with watched Sessions, not viewers |
| [Child-interrupt waits](../services/agent-runtime/README.md#sub-agent-host) | the Runtime polled a pending child interrupt every 300 ms for as long as the child took | the first poll is immediate, later polls wait 300, 600, then 1000 ms, and each new interrupt operation starts again at 300 ms | none |
| [Assistant content](../services/bridge/README.md#incremental-assistant-members-and-stable-reasoning) | settlement and Pod-loss repair rewrote the whole Assistant message | each part is stored once as admitted and never rewritten. Escaped U+0000 in model text or Tool output no longer fails later Tool calls or settlement; numbers (large integers, signed zero, exponents) and reasoning charges are no longer changed by settlement or Pod-loss repair. | none |
| [Tool Use storability](../services/bridge/README.md#event-writer-and-tool-settlement-boundaries) | such a Tool Use was stored, and later reads that interpret Tool Use rows as JSONB failed | Tool Use input or `mcp_server_name` that PostgreSQL JSONB cannot store (for example escaped U+0000) is rejected with `InvalidArgument` and writes nothing | a provider Tool call carrying such a value fails at declaration |
| [Tool result identity](../internal/runtimecontrol/README.md) | termination and shared terminal Tool results carried an internal event reference instead of the public field | they carry the public `tool_use_id` (or `mcp_tool_use_id`) like every other result | SDK clients can pair every terminal result with its Tool Use |
| Retention counters ([Cleanup](../services/cleanup/README.md#invocation-phases-and-retention), [Queue](../services/queue/README.md#the-maintenance-loop)) | — | `tetral_cleanup_retention_budget_exhausted_total{phase="idempotency"\|"stream_changes"}` and `queue_retention_budget_exhausted_total{phase="job_runner_terminal"}` count passes that ended on their batch budget while eligible rows remained | a counter that keeps rising means sustained creation above the retention throughput below |

Automatic deletion:

| Rows | Deleted | By |
|---|---|---|
| Event-stream change rows | 24 hours after the change | [Cleanup](../services/cleanup/README.md#invocation-phases-and-retention), each minute |
| API idempotency receipts | 24 hours after creation | Cleanup, each minute |
| Finished Runner Queue jobs (acknowledged or cancelled) | 24 hours after acknowledgment or cancellation | [Queue maintenance](../services/queue/README.md#the-maintenance-loop), each tick |
| Dead-lettered Runner Queue jobs | 7 days after dead-lettering | Queue maintenance, each tick |

The Runner kinds are `runtime_input`, `runtime_recovery`,
`runtime_config_update`, `cleanup_session` and `session_delete_cleanup`;
Sandbox and Environment Queue rows keep their existing retention. A Runner job
whose terminal timestamp is missing is kept and counted, never dated by another
column. Rows are removed after, not exactly at, their age. Session events and
messages are never deleted, Session deletion remains a tombstone, and no phase
deletes Bridge operation receipts or request usage details. Feed watermarks
outlive the change rows they summarize.

Rollback: none of these behaviors has a switch. Every row reverts only by
returning to the previous release and its database as described in
[Rollback](#rollback), which loses everything written after cutover.

## Database configuration changes

| Setting | Previous | Final | Operator action |
|---|---|---|---|
| `TETRAL_BRIDGE_JOB_RUNNER_POLL_INTERVAL_MS`, Helm `jobRunner.pollIntervalMs` | manifests set 1000 ms; pre-release charts exposed the Helm value | removed; not read | delete both; Queue's retry hint and notifications replace polling |
| `TETRAL_BRIDGE_JOB_RUNNER_LEASE_DURATION_MS`, Helm `jobRunner.leaseDurationMs` | any positive value, default 30000 | 5000–300000 ms, default 30000; the heartbeat interval (`jobRunner.heartbeatIntervalMs`, default 10000, or a third of the lease when unset) must be shorter | a value outside the range fails Job Runner startup; the chart checks only that it is a positive integer above the heartbeat |
| `TETRAL_BRIDGE_JOB_RUNNER_MAX_JOBS`, Helm `jobRunner.maxJobs` | jobs leased per workspace in each sweep, default 8 | capacity slots, default 8, bounded by the Queue lease message size | larger values fail startup |
| `TETRAL_CLEANUP_CLAIM_LIMIT` | claims per workspace per tick, default 100 | size of each global discovery page, default 100; values above 100 are used as 100 | none; it no longer scales with workspace count |
| `TETRAL_EVENT_STREAM_POLL_INTERVAL_MS`, Helm `eventStream.pollIntervalMs` | the previous release polled each viewer every second with no setting; pre-release builds used this key as the per-viewer poll interval | round interval of the shared idle checks per workspace; default 1000, at most 60000 | none |
| Cleanup operation histogram | pre-release builds also reported `tetral_operation_duration_seconds{service="cleanup",operation="claim_due_across_workspaces"}` | that series is gone; `operation="claim_due"` covers the whole scheduling phase, including `error`, `cancelled` and `timeout` outcomes | move dashboards and alerts to `operation="claim_due"` |

Retention ages and batch sizes, the Queue and repair discovery limits, the
Cleanup scheduling budget and candidate limit, and the Queue direct-lease
budgets are fixed in code. The only configurable discovery page is Cleanup's:
`TETRAL_CLEANUP_CLAIM_LIMIT` sets its size, capped at 100.

Rollback: every row reverts only with the previous release
([Rollback](#rollback)), whose own manifests or values carry its settings,
including the poll interval. The previous release reads
`TETRAL_BRIDGE_JOB_RUNNER_MAX_JOBS` and `TETRAL_CLEANUP_CLAIM_LIMIT` per
workspace again and does not read `TETRAL_EVENT_STREAM_POLL_INTERVAL_MS`, so a
value tuned for this release does not carry over.

## Residual limits

These limits are part of the final design. They are stated here so capacity
planning and alerting do not assume more.

| Area | Limit |
|---|---|
| Queue ownership | Queue discovers, rotates and leases Runner work; the Runner only supplies capacity and executes. Workspace is returned task scope, not a Runner input. Fairness across workspaces is per Queue process, not global across replicas. |
| Bounded work | Discovery pages and timeouts are fixed, but existing ordering-barrier checks may still inspect larger populations inside one Session or Thread. A finite, stable set of ready work makes progress; latency has no bound under perpetual arrivals, restarts or overload. |
| Queue discovery latency | When T workspaces hold only blocked or not-yet-due Runner work, newly ready work elsewhere is reached within about ⌈T ÷ 16⌉ lease calls: about T ÷ 160 seconds while the Queue process keeps leasing (100 ms hint), and up to about T ÷ 16 seconds once it is idle (1000 ms hint). |
| Notified work behind a retained pass | A job admitted after its workspace's current discovery pass began is outside that pass. If the notified lease call finishes that pass, it returns no job and a hint of up to 1000 ms; the next call starts a fresh pass and leases the job. Such a job can wait about one hint. |
| Repair latency | Pod-loss repair reads at most 1,024 bindings per run, so a lost Pod's bindings are reached within about ⌈bindings ÷ 1,024⌉ runs at the 30-second cadence. |
| Retention throughput | Cleanup deletes at most 2,560 idempotency receipts and 2,560 change rows per Cron minute. Queue deletes at most 256 Runner rows per terminal state per maintenance tick (every 30 s by default); a tick whose reclaim or Sandbox sweep fails ends before this phase. Sustained creation above that accumulates a backlog, which the budget-exhausted counters report. |
| Runtime context | Context reads return each part exactly as admitted: settlement and Pod-loss repair no longer round numbers through float64, drop signed zero or re-spell exponents, and U+0000 no longer fails settlement. Selection, order and validation timing are unchanged; only the surrounding message envelope's key order, whitespace and equivalent string escapes may differ from before. Declaration and replay hashes stay byte-sensitive. |
| Tool call identity | A Tool call ID's uniqueness within a Thread comes only from the unique index; reuse is `AlreadyExists`, including after compaction. Message content that PostgreSQL JSONB cannot store no longer fails later Tool Use declarations. Exact replay is still checked first. Missing or non-contiguous parts and invalid current context still fail when context is read. |
| Assistant parts | Parts are append-only; the work of an append or settlement is proportional to the new data. |
| Pod-loss Tool result projection | Repair keeps the existing projection checks: a completed result rebuilt from a projection that carries `truncated` still commits, and the next context read rejects it. This release does not change that behavior. |
| API-key usage | Sampled from successful admissions, best effort, at most once per five minutes of admission time per key. Submission never blocks admission, but a key's admission can still wait for that key's usage write (bounded at 100 ms), and the worker uses one pooled connection while writing. A usage generation fences each sample, so a delayed sample never updates a key after rotation away and back or after revoke and reactivation. |
| Runtime heartbeat | An unchanged report is acknowledged as of its own update. A concurrent promotion can supersede it before the reply; the reply grants no mutation authority or custody. |
| Idempotency | A supplied key expires exactly 24 hours after its receipt was created, measured at admission after the receipt lock, regardless of Cleanup progress. No header means no stored receipt. |
| Feed retention | Change rows can be pruned after 24 hours; connection age is unlimited. Only eligible positions a viewer had not yet read close its stream, through the existing reader-failure close; there is no new error frame and no silent cursor skip. |
| Compaction | A compaction checkpoint accepts only text parts, including empty text, within the existing size bounds; reasoning, Tool call and Tool result parts are `InvalidArgument`. The shipped Runtime already sends text only. |
| Schema | Fresh install only; previous databases are rebuilt, not migrated. Event and message history is retained for the life of the database; Session deletion is a tombstone, not a purge. |
