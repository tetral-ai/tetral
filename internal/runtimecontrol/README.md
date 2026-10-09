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
`ReportProcess` take a client and own their short registry transactions;
promotion must never run under, or be composed into, a Session transaction.
Registration creates the process row and its `runtime_process_liveness` row
(no report yet) together. `Process` carries lifecycle facts only; report time
lives in the liveness row. `ReportProcess` first reads the process row without
locking it in a READ COMMITTED transaction. An unchanged report of the current
process then updates only the liveness row, conditioned on the process still
being current with that phase and receipt, and returns a `ReportedProcess`
with the recorded database time. Its acknowledgment reflects that UPDATE
statement's view of the process row, so a concurrent promotion or drain can
supersede it; it confers no mutation authority. Any other report rolls back
and runs in a new transaction that locks the Pod row, then the process rows in
registration order, promotes candidates by registration order and records the
report on the liveness row, committing all of it together. A missing liveness
row is an invariant error on registration retry and on that path; no row is
created to repair it.

The lock graph is:

- Session mutation, placement, release and repair: Session, binding, process
  row `FOR SHARE` through `LockProcessTx`, so a promotion waits for those
  writers instead of interleaving with them. Generic process locks never read
  liveness.
- Job Runner's final loss classification: the same fences, then the liveness
  row `FOR SHARE` through `LockProcessLivenessTx`.
- Lifecycle report: Pod row, process rows `FOR UPDATE` in registration order,
  then the liveness row; never a Session.
- Unchanged report: the liveness row only; the process row is read without a
  lock and no later Pod or process lock is taken in that transaction.

Tool closeout writes and reads the scalar Tool relation of `session_events`.
The termination and shared terminal result writers set `tool_use_event_id` to
the Tool Use they answer and emit the public `tool_use_id` (or
`mcp_tool_use_id`) for the same event. Orphan discovery, result existence and
the checkpoint fence join results on that column within the Thread, and read
a Tool Use's call ID from `model_tool_call_id`. `ToolRelationInsertError`
maps a unique violation of a Tool event INSERT by constraint name: a reused
call ID is `AlreadyExists`, a global event ID collision is a non-disclosing
`AlreadyExists`, and a second result for one Tool Use is an internal invariant
error that rolls the whole transaction back without retry.

Moved literal SQL and payload values are conserved from the accepted extraction
base; owner tests and PostgreSQL compositions supply the behavioral evidence.
Shared pure vector tests cover deterministic delivery identities.
