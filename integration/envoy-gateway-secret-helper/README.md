# Pinned SecretType translation helper

This separate test-only module imports the Envoy Gateway 1.9.2 internal
translator. Its upstream dependency requires Go 1.26.8; it does not change the
Engine module's Go 1.25.13 toolchain.

The unmodified matching `egctl` emits Gateway status, xDS ConfigDump and IR, but
omits policy SecretType resources from ConfigDump. This helper accepts that
combined output together with the exact original fixture YAML and its SHA-256
identity on stdin. IR JSON deliberately redacts private keys, so the helper
loads those original explicit resources with the official parser, runs the
official Gateway API and xDS translators with the matching CLI options, and
checks the entire redacted IR against the CLI output. It requires unchanged
protobuf equality for every original listener, route, cluster
and endpoint. It deduplicates byte-identical upstream Secrets and rejects
conflicting duplicates or missing/unexpected policy SDS references. Bootstrap
controller SDS references belong to process setup and are outside this policy
resource scope; the original bootstrap stays separate.

The pinned Gateway translator unconditionally looks up the controller's `envoy`
TLS Secret even when no global rate-limit or Wasm consumer exists. The CLI
ignores all Gateway translation errors. This helper permits only that exact
missing-Secret error, after checking the complete IR has neither consumer and
that the input has no extension-server policies. Any other or combined error
fails. It reports this finite classification separately; all IR, resource and
Secret parity checks still apply.

The output contains upstream-generated missing Secrets, the Gateway name,
common resource counts, the verified fixture identity, the finite classification
and the selected upstream process drain arguments. Those arguments come from
the pinned upstream `BuildProxyArgs` and declared Shutdown configuration; they
do not change the original translated policy or Bootstrap. TLS material is
checked for usable matching certificate/key pairs and trust bundles before
serialization. No missing Service address or resource is synthesized. The output includes disposable fixture private keys: pipe
it directly to a private test-owned file or decoder, never a terminal or log.
Safe failure diagnostics contain only the constant failing stage. The owning
local edge fixture serves the original CLI resources plus these missing Secrets;
it never reconstructs route policy or changes the pinned CLI.

Native test prerequisites verify the official `egctl` archive and version,
build this module with `-mod=readonly`, and own cleanup. The Engine dependency
lock declares the exact helper module/version/toolchain. Both module and symbol
vulnerability scans run through
[`check-envoy-secret-helper.py`](../../scripts/check-envoy-secret-helper.py),
including scheduled and affected dependency security coverage. The module scan
prints every advisory in the selected graph and labels whether it found
advisories. Its normal finding exit continues to the required symbol scan;
scanner syntax, build, infrastructure and other errors fail immediately. The
symbol scan must succeed. Module findings remain visible even when no vulnerable
symbol is called; no advisory is suppressed. The separate helper graph selects
patched `golang.org/x/crypto` 0.56.0 and `go.etcd.io/etcd/client/pkg/v3` 3.7.1
without changing the upstream translator version or Engine dependency graph.
Translation
parity is separate from actual Envoy acceptance and HTTP/TLS behavior, and none
of these local proofs establishes Kubernetes controller reconciliation.
