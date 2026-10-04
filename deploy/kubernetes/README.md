# Raw deployment examples

These manifests are source-checkout examples. Their `0.0.0-dev` image
references are intentionally not a public release identity and will not pull
unless an operator builds and tags matching local images.

Published installations use the Helm Chart and the exact image digests named
by a numbered GitHub Release (`v0.1.0-alpha.N`). GitHub Releases is the sole
availability lookup; if no numbered Alpha is listed, no public Alpha is
available. Do not replace these examples with `latest` or an unnumbered Alpha
tag.

Before applying updated workloads, complete the separate database preparation
step described in the [upgrade procedure](../helm/tetral/README.md#upgrade-and-rollback).
Every serving process verifies readiness; applying API first no longer migrates
the database. The former `rollout-schema-ordered.sh` is removed for that reason.

Bridge, Job Runner, Provider Gateway, MCP Connector and Web Connector are
independent workloads with separate ServiceAccounts and access grants. Their
source-owned fragments live under their service `k8s/` directories; the Gateway
workspace holds `k8s/provider-gateway/` and `k8s/mcp-connector/`. The aggregate
files here compose those fragments exactly. `job-runner-rbac.yaml` grants only
Runner Runtime visibility. TokenReview bindings cover receiving workloads only.
The obsolete combined `gateway.yaml` and Bridge visibility role are removed.
See the [workload replica and access contract](../helm/tetral/README.md#independent-workload-replicas-and-access)
for default replicas, Provider autoscaling and credential audience separation.

Gateway and Event Stream preview wiring requires the separately rendered
[Core NATS release](../nats/README.md) and its role credential Secrets. The
hardened app example additionally expects native trust and issued per-role
leaves; it never falls back to plaintext. Regenerate application manifests with
`python3 deploy/render-manifests.py` after changing chart values, and regenerate
the independent broker manifests with `deploy/nats/render.py` for the selected
replica count. Broker and application manifests have separate lifecycle owners.
