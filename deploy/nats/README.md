# Core NATS for public previews

Gateway publishes primary-thread previews and Event Stream subscribes using
separate credentials. PostgreSQL continues to own formal SSE and history.
Previews are transient: JetStream, PVCs and replay are disabled. Losing the
broker can end an affected preview prefix; the committed final message still
precedes its durable End on SSE. Broker availability is independent of the
application's core readiness.

This separate release uses the official NATS chart and server 2.15.0. The
[deployment lock](../dependencies.lock.json) records the verified chart archive,
server/reloader/exporter image digests and Go 1.53.0 / JS transport-node 3.4.0
client versions. Render tests compare every YAML document in order, including
Services and exact ConfigMap strings, while ignoring document-separator padding.
`render.py` verifies the checked archive and projects locked
images. The default release has one broker; the hardened example has three.
All brokers require separate nodes, and the three-broker topology requires
three schedulable nodes. Routes use broker-only credentials even when TLS is
active. Client certificates do not replace publisher/subscriber subject ACLs. The publisher
may publish only `preview.v1.>` and explicitly denies all subscriptions; the
subscriber may subscribe only `preview.v1.>` and explicitly denies all publishes.
Empty permission lists are unrestricted in NATS, so the forbidden direction uses
`deny: [">"]`, as described in the [NATS authorization documentation](https://docs.nats.io/learn/security/authorization).

## Prerequisites and local rendering

Create `tetral-system` and supply these Secrets through the deployment
operator. Secret values never belong in checked manifests or command output.

| Secret | Keys and scope |
| --- | --- |
| `tetral-nats-server-credentials` | `publisher_user`, `publisher_password`, `subscriber_user`, `subscriber_password`; broker Pods only |
| `tetral-nats-publisher` | `user`, `password`; Gateway only, matching the broker's publisher pair |
| `tetral-nats-subscriber` | `user`, `password`; Event Stream only, matching the broker's subscriber pair |
| `tetral-nats-cluster-credentials` | `user`, `password`, and one `route_N` per ordinal; broker Pods only |

Each `route_N` contains a complete authenticated route URI, whose username and
password match the cluster pair. Its host is
`tetral-nats-N.tetral-nats-headless.tetral-system.svc.cluster.local:6222`.
Use `nats://` for standard routing and `tls://` for hardened routing. URI-encode
reserved credential characters. The complete URI is supplied as one Secret
reference because the NATS configuration parser expands whole environment
values. Rendered ConfigMaps carry references, never route credential values.

Render locally from the repository root, without contacting Kubernetes:

```bash
python3 deploy/nats/render.py --charts-dir deploy/nats/charts \
  --profile standard-routed --replicas 1 --output /tmp/tetral-nats.yaml \
  --values-output /tmp/tetral-nats-values.json
python3 deploy/nats/render.py --charts-dir deploy/nats/charts \
  --profile hardened --replicas 3 --output /tmp/tetral-nats-hardened.yaml \
  --values-output /tmp/tetral-nats-hardened-values.json
```

Use the verified official archive, base values, selected hardened values and
generated locked values together when installing the independent Helm release.
The rendered policy grants client 4222 only to Gateway/Event Stream, route 6222
only to broker Pods, and exporter 7777 only to the monitoring namespace role.
Monitoring 8222 is internal to the Pod and is not granted through the policy.
The app chart adds the corresponding two narrowly scoped egress grants.

## Protected native TLS

The hardened profile requires separately installed cert-manager **[1.21.2](https://github.com/cert-manager/cert-manager/releases/tag/v1.21.2)** and
an operator-bound `tetral-native` ClusterIssuer backed by a dedicated native
intermediate. Apply [certificates.yaml](certificates.yaml) after that issuer
and its public trust distribution are ready. It requests four role leaves,
24-hour validity, renewal eight hours before expiry and private-key rotation
on every issuance, following the [Certificate contract](https://cert-manager.io/docs/usage/certificate/). Restrict Certificate/CertificateRequest creation and
Secret/workload mutation to the operator and issuance controller. Applications
and brokers receive leaf keys only; no workload receives a signing key.

Supply `tetral-nats-broker-trust` and `tetral-nats-client-trust` ConfigMaps with
`ca.crt`, containing the explicitly trusted native issuer bundle. The broker
listener and route use different Secrets. Each broker role leaf is shared only
among broker Pods. Its DNS SANs cover the service and/or all advertised
headless names; this allows new ordinals without per-Pod issuance. It identifies
a broker role, not an individual ordinal. Gateway and Event Stream each mount
their own role leaf and credentials, never a broker key or cluster credential.
No `subPath` mounts or certificate-triggered restart controller are used.

The client listener requires mTLS with TLS before `INFO`; routes require mTLS
plus cluster authorization. All advertised reconnect destinations are DNS
names verified by the native clients. There is no plaintext fallback and no
`verify_and_map` substitution for username/password authorization. Missing or
invalid initial leaf/trust material cannot start a protected consumer.

The pinned official reloader watches mounted files and signals server reload.
The NATS server validates complete certificate/key material before activation;
a failed reload preserves valid last-known-good server state. The watcher does
not validate the pair itself. Inspect reload errors and certificate expiry,
and use a fresh full handshake to observe the served replacement certificate.
Gateway validates a complete mounted generation before replacing its captured
TLS buffers; Event Stream loads validated TLS snapshots for new connections
and retires live connections on trust activation. Invalid updates retain only
still-valid prior material. A valid trust change that cannot connect stops
affected previews, while formal PostgreSQL delivery remains available.

For CA replacement, distribute R1+R2 trust first, switch issuance and leaves,
observe fresh handshakes at every advertised client and route DNS destination,
and retire old connections before removing R1. Test fresh R1-only rejection
and a fresh R2 positive control after removal. Kubernetes projection and
cert-manager reconciliation delays are separate from the consumer's reload
observation. Local container tests prove native process behavior; they do not
prove Kubernetes issuance, NetworkPolicy enforcement or node placement.

## Rolling replacement and replica changes

The official chart's preStop hook enters lame-duck mode. Ten seconds of grace
and a thirty-second drain fit the sixty-second Pod termination budget. The
PodDisruptionBudget permits one voluntary disruption; abrupt broker loss can
still lose previews. In a three-broker release, change one broker at a time and
wait for readiness and the complete authenticated route topology before
continuing. Clients discover advertised broker DNS names and reconnect with
fresh verified TLS material. Reconnection never replays dropped previews.

To change 3→4→3, provision `route_3` first, regenerate the four-replica route
set and locked values, update the release, and observe all four brokers and
cross-node preview fan-out. Regenerate the three-replica set before the
scale-down release update; after the removed broker and old connections have
joined, retire the unused Secret key. Certificates use wildcard headless SANs,
but the added node must satisfy placement constraints. Readiness alone does
not demonstrate cross-broker fan-out or reconnect to every advertised name.
