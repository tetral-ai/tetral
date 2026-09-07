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
