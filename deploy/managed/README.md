# Managed installation contract

Render portable application resources and the independently owned Istiod,
Envoy Gateway and optional native certificate prerequisites before preparing
an installation. Select one supported Kubernetes version and CNI profile; bind
actual API-server/DNS/database/issuer destinations, storage access modes and
placement, resource requests, image digests, certificate references and the
aggregate PostgreSQL connection ledger. An unresolved capacity or prerequisite
is not an installation-ready declaration.

[`resource-inventory.json`](resource-inventory.json) declares exact portable
resource sets and fourteen ownership-qualified retirement identities, including
the former combined Gateway fleet, its controller/RBAC objects, Bridge visibility
grants and public Ingresses. The previous resource dispositions bind the public
source revision and explicitly retain shared stores and matching app resources. Render installation overrides through the
same owning chart; compare their exact resource identities with the declared
profile rather than silently adding missing references. Standard and hardened
workload sets replace one another. Optional public edge resources and separately
owned controller prerequisites have distinct installation/cleanup owners.

Use a dedicated empty database and isolated object namespace. Verify those
identities before preparation. Commands come from the exact workload revision;
all administrative connections require verified native PostgreSQL TLS.
Administrative/schema-owner credentials never enter serving workload Secrets.

1. Supply the protected role declaration on stdin to `tetral-db-prepare`, with
   the operator-selected name/password for every key in `database/roles.json`
   plus `migration`. Set `TETRAL_DATABASE_ADMIN_URL`,
   `TETRAL_DATABASE_TLS_CA_PATH` and `TETRAL_DATABASE_TLS_SERVER_NAME`.
   Preparation accepts only an empty catalog or the exact current schema. It
   constructs schema/checksums, narrow roles, grants and RLS. It does not create
   a workspace or import federation policy.
2. Run `tetral-bootstrap --workspace-id <declared-id> --name <declared-name>`
   for every declared installation workspace, with the prepared protected
   connection in `TETRAL_DATABASE_URL`. The bootstrap command preserves an
   existing workspace; Auth never implicitly creates one.
3. Pipe the explicit federation policy document into `tetral-auth-policy`,
   using its protected administrative environment. Import follows bootstrap
   because grants reference existing workspaces. Include trusted issuer URLs,
   audience, CA roots and any private destination allowlist; configure Auth's
   corresponding explicit HTTPS NetworkPolicy destinations independently.
   A failed atomic import leaves prior authority unchanged.
4. Supply matching serving-role Secrets, signing keys, verified public trust,
   issued native/public leaves and separately installed controller readiness.
   Install matching workload images/configuration with public admission closed.
5. Verify schema/role and workload readiness, allowed and denied API-key/Bearer
   Check, exact token-exchange routing, tenant/operation gates, issuer HTTPS and
   TLS peer controls through the explicit test address and Host. Verify the
   aggregate resource/removal inventory before opening public admission.

Repeating preparation/bootstrap/import against an exact matching installation
verifies or preserves existing identities and authority. An unexpected
predecessor, partial/nonempty catalog, invalid role declaration or failed import
keeps admission closed. There is no predecessor copy, reverse migration,
credential fallback or data-erasing repair. Keep the isolated object namespace
and existing workspace/grant/key identities after any failed preparation.

The [bootstrap guide](../../docs/bootstrap.md) owns Secret keys and actual
command invocation; the [chart guide](../helm/tetral/README.md) owns projections,
profiles, database capacity and bounded rollout. Local disposable database tests
exercise the actual command sequence from an unseeded database; their evidence
does not establish cloud resource identity or deployed controller reconciliation.


Bind an installation's actual override render before validating its collected
object list. Select the optional flags that describe this installation; pass the
same reviewed values used for installation:

```bash
python3 deploy/managed/render-inventory.py \
  --profile hardened --public-edge --native-certificates \
  --values installation-values.yaml --output-dir installation-render
python3 deploy/managed/validate-inventory.py \
  --profile hardened --expected-dir installation-render --require-complete \
  --observed observed-resources.json
```

The expected artifact binds every parsed object identity to the rendered bytes
and digest. This covers renamed certificate resources and optional Git FQDN
policies; changing the render after binding fails the check. For repository
defaults, validate a collected object list directly:

```bash
python3 deploy/managed/validate-inventory.py \
  --profile standard-routed --public-edge --require-complete \
  --observed observed-resources.json
```

Select the optional Cilium/native-certificate/issuer-policy flags only for their
rendered installation features. The check never contacts or mutates a cluster.
Omitting `--require-complete` produces explicitly partial evidence and cannot
establish installation completeness. A surviving superseded owned Deployment, autoscaler, network policy, Service or
RBAC grant is a failed inventory check even when the new Gateway is healthy.
Unrelated resources without matching ownership are preserved; controller/CRD
prerequisites remain independently verified rather than silently adopted.
