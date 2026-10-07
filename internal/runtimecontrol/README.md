# runtimecontrol

This package owns reusable durable Runtime control rules: Session arbitration,
binding and mutation fences, request and Tool closeout, context projections,
interrupt custody, input receipts and completion mail. Bridge calls these rules
from Runtime RPC transactions. Job Runner calls them from reconciliation,
repair, delivery finalization and cleanup transactions.

Every Session-scoped database helper receives the caller's transaction. Queue
birth, exact lease ACK/NACK, receipt and context mutations therefore retain the
owning transaction's atomicity. Explicit custody DTOs carry only the durable
identities and fences required by actual Bridge and Runner callers. RuntimeJob,
Queue consumer orchestration, process configuration and connection lifetimes
remain service-owned. The package never calls either service's business code or
opens an independent transaction to hide a partial commit.

The process registry is the one exception. `RegisterProcess` and
`ReportProcess` take a client and own one short transaction that locks only the
Pod row and its process rows, promoting candidates in registration order.
Promotion must never run under, or be composed into, a Session transaction:
Session-scoped mutation, placement, release and repair lock the Session, then
the binding, then the matching process row `FOR SHARE` through
`LockProcessTx`, so a promotion waits for those writers instead of interleaving
with them.

Moved literal SQL and payload values are conserved from the accepted extraction
base; owner tests and PostgreSQL compositions supply the behavioral evidence.
Shared pure vector tests cover deterministic delivery identities.
