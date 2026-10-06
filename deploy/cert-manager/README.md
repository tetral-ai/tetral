# Native certificate prerequisite

Native certificate automation uses the locked cert-manager 1.21.2 release,
installed independently from Tetral and Istiod. It is required only when
`nativeCertificates.enabled=true`; standard routed installations can supply
their other prerequisite credentials without this automation profile.

```bash
kubectl create namespace cert-manager
python3 deploy/cert-manager/render.py \
  --charts-dir deploy/cert-manager/charts \
  --output /tmp/tetral-cert-manager.yaml
```

The local renderer verifies the official chart archive and pins the controller,
webhook, CA injector, ACME solver and startup API check images from the canonical
dependency lock. It includes retained cert-manager CRDs and performs no cluster
action. Review and independently install those prerequisites before requesting
native leaves.

An operator binds `nativeCertificates.issuerName` to a ClusterIssuer backed by a
dedicated native intermediate. Mesh signing material remains Istiod's separate
`cacerts` purpose. Signing keys never enter Tetral workload Secrets. Distribute
the verified public native CA through the declared ConfigMaps before admission.
Public API/Git certificates use separately supplied public TLS Secrets and their
own issuer/challenge credentials and renewal owner.

The hardened chart's optional Certificates issue one leaf per consuming role,
with 24-hour duration, eight-hour renewal lead time, ECDSA keys and
`rotationPolicy: Always`. Server leaves carry actual Service DNS identities;
the Runtime leaf follows `transport.runtimeServerName`. Each leaf carries the
exact namespaced role URI and appropriate server/client EKU. The edge client
Secret is stored alongside its referencing EnvoyProxy in `tetral-system`, while
its role URI identifies the data-plane ServiceAccount in
`envoy-gateway-system`. Native NATS roles are separately owned by
[`deploy/nats/certificates.yaml`](../nats/certificates.yaml).

Only deployment operators/controller roles may create Certificates or
CertificateRequests or mutate workloads/Secrets. Application ServiceAccounts
receive no issuance or signing-key grant. A consuming Pod mounts its own leaf
directory read-only without `subPath`; public CA material is not a signing key.

Bootstrap orders issuer and trust, issued leaves, ready consumers and
allowed/denied identity probes before admission. Missing or invalid initial
credentials prevent protected readiness. The Go loader validates a complete
mounted generation; Envoy Gateway reconciliation distributes public/backend
TLS Secrets through SDS; the direct Runtime listener uses watched file-backed
SDS; native NATS retains its owning reload procedure. Malformed replacements
cannot broaden trust or enable a plaintext fallback.

CA rotation distributes old-plus-new trust first, switches issuance, proves
fresh handshakes with new leaves, drains old connections and only then removes
old trust. Local controlled certificates test consumer reload and identity;
they do not prove the production cert-manager renewal schedule or Kubernetes
projection delay. Record those observations in the actual installation.
