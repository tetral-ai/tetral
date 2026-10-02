# runtimecontrol

This package owns reusable durable Runtime control rules: Session arbitration,
binding and mutation fences, request and Tool closeout, context projections,
interrupt custody, input receipts and completion mail. Bridge calls these rules
from Runtime RPC transactions. Job Runner calls them from reconciliation,
repair, delivery finalization and cleanup transactions.

Every database helper receives the caller's transaction. Queue birth, exact
lease ACK/NACK, receipt and context mutations therefore retain the owning
transaction's atomicity. Explicit custody DTOs carry only the durable identities
and fences required by actual Bridge and Runner callers. RuntimeJob, Queue
consumer orchestration, process configuration and connection lifetimes remain
service-owned. The package never calls either service's business code or opens
an independent transaction to hide a partial commit.

Moved literal SQL and payload values are conserved from the accepted extraction
base; owner tests and PostgreSQL compositions supply the behavioral evidence.
Shared pure vector tests cover deterministic delivery identities.
