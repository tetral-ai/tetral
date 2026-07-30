# queue

## Responsibilities

`queue` is the platform's durable job queue: the single owner of
leasing, ordering, retries, deferral, cancellation, and dead-lettering over the
shared `queue_jobs` table. It is delivery control, not business truth — a leased
job is a turn to act on a durable work item, never the work item itself, and an
acknowledgement is a claim the consumer already reconciled that truth elsewhere.
The service does not admit jobs: producers write their business rows and the
matching `queue_jobs` row in one PostgreSQL transaction through the in-process
store (`internal/queue`, `EnqueueTx` / `EnqueueBatchTx`), so a work item can
never exist without its queue entry or the reverse. What the workload exposes
is everything *after*
admission — a gRPC transition API (`services/queue/proto/tetral/queue/v1`,
`QueueService`) that consumers drive, plus one background goroutine that rescues
leases their owners abandoned. The store reads and writes only `queue_jobs` and
`queue_partition_counters`; it never touches the business tables (`session_events`,
`sandboxes`, `session_preparations`, and the rest), never calls Runtime Pod,
Bridge, Sandbox Service, or any provider, and never infers that referenced work
completed — a consumer's `Ack` is the only signal that it did. Queue also owns
`queue_partition_counters`, whose locked rows allocate causal admission order.
Each row's own
`workspace_id` is the authoritative execution scope and appears in every primary
key; no consumer takes its serving workspace from configuration.

## States & lifecycle

### Durable tables

`queue_jobs` carries delivery state. Every row has a database-assigned
`queue_partition_sequence` in addition to its `kind`, a
`partition_key`, an optional `dedupe_key`, a `payload_json` of durable references,
a `payload_version` (positive integer; the payload-schema-version guard, rejected
at admission when negative, while an unset or zero value defaults to 1), lease bookkeeping (`leased_by`, `lease_token`,
`leased_at`, `leased_until`), `attempt_count` / `max_attempts`, the config-only
`defer_count`, a `priority`, an `available_at`, and a `status`.
Payloads are references only — the admission whitelist (below) rejects any job
that would persist user content, resource bytes, model config, or credentials.

`queue_partition_counters` has one row per `(workspace_id, partition_key)`.
Enqueue locks that row, advances `last_sequence`, and inserts the job with the
returned value in the same producer transaction. Rollback publishes neither
fact, and active-dedupe replay returns the existing job without advancing the
counter. `EnqueueBatchTx` validates every request, locks the complete distinct
partition set in sorted workspace/key order, then allocates jobs in caller
order. Caller timestamps and random job ids never establish causal order.

### Status state machine

| Status | Meaning | Writers (`internal/queue/postgresql_store.go`) | Transitions to |
|---|---|---|---|
| `pending` | admitted, awaiting a lease; `Retry`/`Defer` re-admit with a backoff-delayed `available_at`; reclaim re-admits at `available_at = now` | `Enqueue` (insert), `Retry` (budget left), `Defer`, `ReclaimExpiredLeases` | `leased`, `cancelled` |
| `leased` | one consumer holds the row under a `lease_token` for the lease window | `Lease` | `pending`, `acknowledged`, `dead_lettered` |
| `acknowledged` | terminal; the leased work committed | `Ack` | (none) |
| `cancelled` | terminal; a superseded `pending` `runtime_input` row was fenced out — reached only straight from `pending`, never from `leased` | `Cancel` (`runtime_input` rows only) | (none) |
| `dead_lettered` | terminal; attempts exhausted or an explicit dead-letter | `Retry` (exhausted), `DeadLetter` | (none) |

### Transitions

All caller-driven writes off `leased` (`Ack`, `Retry`, `Defer`, `DeadLetter`,
`Heartbeat`) are fenced by `(workspace_id, job_id, lease_token)` and act only on a
row still `leased` under that token; a stale token reports `updated = false` and
changes nothing.

| Call | Fencing | Kinds | Effect |
|---|---|---|---|
| `Lease` | mints a fresh `lease_token` | any requested kind | selects `pending` candidates whose own `available_at` has arrived, ordered by priority/availability and partition sequence under `FOR UPDATE SKIP LOCKED`; leases only where the partition holds no `leased` job and no higher-ranked or causally earlier `pending` sibling; increments `attempt_count`; projects an "unset" `max_attempts = 0` to the effective default in the response |
| `Heartbeat` | lease-token | any | pushes `leased_until` forward |
| `Ack` | lease-token | any | → `acknowledged`; legal only after the consumer reconciled durable state and delivered/resolved the command |
| `Retry` | lease-token | any | carries an error kind/message only, no delay authority. If `attempt_count` reached the effective `max_attempts`, dead-letters instead. Otherwise → `pending` with capped exponential backoff + full jitter |
| `Defer` | lease-token | `session_prepare` and the two canonical `runtime_config_update` payload classes | → `pending` with Queue-owned capped backoff while decrementing `attempt_count`, so the lease consumes no retry budget. `session_prepare` retains its existing attempt-based backoff and never reads or changes `defer_count`. A locked SDK config-generation or MCP manifest-generation row is revalidated from its stored refs-only payload, increments `defer_count`, and derives backoff from that counter without approaching `max_attempts` |
| `DeadLetter` | lease-token | any | → `dead_lettered` straight, carrying the error, for terminal invariant failures |
| `Cancel` | partition-scoped, **not** lease-fenced | `runtime_input` `input_kind = messages` only | requires `workspace_id`, `session_id`, `session_thread_id`, and a positive `interrupt_fence_sequence`; marks `cancelled` every `pending` matching row in that thread whose `sequence_to` is below the fence; touches no `leased`/terminal row and deletes no `session_events` |
| `ReclaimExpiredLeases` | exempt (matches `workspace_id`/`id`/`status = 'leased'` without the stale token) | any | background loop only; clears lease bookkeeping on rows `leased` with `leased_until <= now` and returns them to `pending` at `available_at = now` with a `lease_expired` error stamp |

Backoff is `delay = rand(0, min(cap, base * 2^(count-1)))`, where `count` is
`attempt_count` for Retry and `session_prepare` Defer, and `defer_count` for
config Defer. The full-jitter distribution is fixed, not a knob. Lease expiry alone never
dead-letters: the prior owner may have committed business success and only lost
the acknowledgement, so the next lease holder revalidates the durable row under
its own token and stale-acks when the work is already done. Attempt exhaustion
through `Retry`, and an explicit `DeadLetter`, are the only routes to
`dead_lettered`.

### Invariants

Two partial-unique constraints carry the durable invariants; the transition
writers uphold them under concurrency.

| Invariant | Scope | Enforced by |
|---|---|---|
| At most one active job per `(workspace_id, dedupe_key)` | `pending` + `leased` only | `EnqueueTx` `ON CONFLICT … DO NOTHING` + partial-unique index; a later job for the same durable item is admitted once the prior one is `acknowledged`/`cancelled`/`dead_lettered` |
| At most one `leased` job per `(workspace_id, partition_key)` — the same-session serial-execution barrier | `leased` only | `leaseCandidate`'s NOT-EXISTS leased-in-partition guard + partial-unique index; many `pending` jobs may still sit in one partition awaiting their turn |
| One causal position per partition | all jobs | the locked `(workspace_id, partition_key)` counter assigns `queue_partition_sequence`; Retry, Defer, and reclaim update availability/lease state without changing it |

### The reclaim loop

One background goroutine (`RunStalledLeaseMaintenance`) runs
`ReclaimExpiredLeases` across all workspaces on a fixed interval, taking a bounded
batch per scan. This is what unsticks a `runtime_input`, `session_prepare`,
`cleanup_session`, or `session_delete_cleanup` job stranded by a crashed consumer.

### Startup configuration

Everything is startup env, validated before the service serves traffic; any
malformed value is a startup failure (`ConfigFromEnv`).

| Env var | Default | Rule |
|---|---|---|
| `TETRAL_QUEUE_HTTP_ADDR` | `:8080` | HTTP listen (`/health`, `/ready`, `/metrics`) |
| `TETRAL_QUEUE_GRPC_ADDR` | `:9090` | gRPC transition API + gRPC health |
| `TETRAL_QUEUE_RETRY_BASE_MS` | `1000` | backoff floor; rejected at ≤ 0 |
| `TETRAL_QUEUE_RETRY_CAP_MS` | `60000` | backoff ceiling; rejected at ≤ 0 and when `< base` |
| `TETRAL_QUEUE_RETRY_MAX_ATTEMPTS` | `10` | service-default attempt budget; the "unset" per-job `0` resolves to this at the lease projection and the dead-letter comparison |
| `TETRAL_QUEUE_LEASE_RECLAIM_INTERVAL_SECONDS` | `30` | reclaim cadence; required positive |
| `TETRAL_QUEUE_LEASE_RECLAIM_LIMIT` | `100` | per-scan batch size; required positive |

The retry policy is Queue-Service-owned; consumers carry no delay authority.

### Operational surface

gRPC on `:9090` serves `QueueService` and a gRPC health service; HTTP on `:8080`
serves `/health` (liveness), `/ready` (readiness), and `/metrics`. Metrics are
per kind: `queue_pending_jobs`, `queue_leased_jobs`, `queue_retry_pending_jobs`,
`queue_dead_lettered_jobs`, and `queue_ready_lag_seconds`. The service account
mounts no Kubernetes API token; the network policy restricts egress to PostgreSQL
and DNS, and ingress on both ports to `api`, `bridge`, and
`sandbox`.

## Seams

### Seam 1 — Job kind registry

Eight kinds share the table under two partition families. The queue stores `kind`
as an opaque label and never dispatches on it, but three behaviors are
kind-specific.

| Kind | Partition family | Leased by |
|---|---|---|
| `runtime_input` | `session:<workspace_id>:<session_id>` | Bridge Job Runner |
| `runtime_config_update` | `session:…` | Bridge Job Runner |
| `cleanup_session` | `session:…` | Bridge Job Runner |
| `session_delete_cleanup` | `session:…` | Bridge Job Runner |
| `session_prepare` | `session:…` | Sandbox Service |
| `environment_build` | `environment:<workspace_id>:<environment_id>` | Sandbox Service |
| `environment_ready_fanout` | `environment:…` | Sandbox Service |
| `environment_failed_fanout` | `environment:…` | Sandbox Service |

**Interface contract.** Kinds and their canonical shapes live in
`internal/queue/queue.go`: `isKnownKind` is the closed registry;
`validateCanonicalQueueShape` holds the per-kind field whitelist; the
`Format…PartitionKey` / `Format…DedupeKey` helpers compute the only accepted
`partition_key` and `dedupe_key` forms. `EnqueueTx` rejects any job whose payload
is not a JSON object, whose `workspace_id` mismatches the row, that carries a
non-whitelisted field, or whose partition/dedupe keys differ from the computed
forms. A single kind may carry more than one canonical payload sub-shape: the
`runtime_mcp_manifest_update` shape (with its own `Format…DedupeKey` helper and
`validateCanonicalQueueShape` branch) is a variant of `runtime_config_update`,
not a ninth kind — `isKnownKind` still admits only the eight above.

The `runtime_input` kind carries an `input_kind` discriminator, checked by
`isRuntimeInputKind`, over a closed set: `messages`, `interrupt_control`,
`tool_confirmation`, `task_notification`, `agent_mail`. The kind-specific
behaviors below hinge on it (`Cancel` applies to `input_kind = messages`;
priority overtaking to `interrupt_control`). A `task_notification` marks a
background command's terminal completion; it is not a public user message and
produces no second public user event.

**Lifecycle.** A kind is admitted only through `EnqueueTx` or `EnqueueBatchTx`
inside the producer's transaction; from there it flows through the shared status machine above. Consumers
lease the kinds they serve by name in `Lease.kinds`; an unknown kind there is a
validation error.

**Kind-specific behaviors a replacement must preserve.**
- `Defer` accepts `session_prepare` plus refs-only SDK config-generation and
  MCP manifest-generation `runtime_config_update` rows. It validates the
  locked stored payload before admitting either config arm.
- `Cancel` touches **only** `runtime_input` `input_kind = messages` rows.
- Priority-based lease overtaking applies **only** among `runtime_input`
  candidates: an `interrupt_control` `runtime_input` is admitted at higher
  `priority` and carries past still-`pending` message siblings.
- An earlier `runtime_config_update` blocks later ordinary `runtime_input`
  independently of the config row's `available_at`. `interrupt_control` is the
  sole exception and may cross that config row; it does not release later
  ordinary input. Every causal comparison uses `queue_partition_sequence`.

**Invariants a replacement must preserve.** Payloads stay references-only (the
whitelist is the guard); `partition_key`/`dedupe_key` remain deterministic
functions of the payload's identity fields; adding a kind means extending
`isKnownKind`, `validateCanonicalQueueShape`, and the key formatters together —
plus the durable `queue_jobs_kind_shape` CHECK constraint, which lives in
`internal/storage/postgresql_schema.go` (the `queue_jobs` DDL and its
`alterPostgreSQLQueueJobsKindConstraint` migrations, exactly how
`session_delete_cleanup` and `environment_failed_fanout` were added). A kind is
incomplete until all four agree; a kind admitted in code but absent from the
CHECK is rejected by the database on insert.

**Conformance tests.** `TestPostgreSQLStoreRejectsNonCanonicalQueueShape`,
`TestPostgreSQLStoreAcceptsRuntimeMCPManifestUpdateCanonicalShape`,
`TestPostgreSQLStoreAcceptsTaskNotificationRuntimeInputWithoutPublicEventFence`,
`TestNormalizeEnqueueRequestRejectsRuntimeInputBeyondEventReferenceLimit`,
`TestNormalizeEnqueueRequestAcceptsOnlyBareAgentMailPokes`,
`TestNormalizeEnqueueRequestRejectsOversizedPayloadForEveryJobKind`
(`internal/queue`).

### Seam 2 — Lease semantics a consumer must preserve

A consumer is any process that leases and settles jobs (Bridge Job Runner,
Sandbox Service). The lease contract is what lets a crashed or slow consumer be
replaced without losing or double-committing work.

**Interface contract.** `Lease(workspace_id, kinds, lease_owner, max_jobs,
lease_duration_ms)` returns up to `max_jobs` leased rows, each with a fresh
`lease_token`. The consumer then drives exactly one terminal transition per job
(`Ack` / `Retry` / `DeadLetter`, or an admitted `Defer` back to `pending`),
calling `Heartbeat` to extend `leased_until` while it works.
The gRPC surface is `QueueService` in
`services/queue/proto/tetral/queue/v1`; the Go boundary is the `Store`
interface in `services/queue/server.go`.

**Lifecycle.** Lease → (heartbeat)\* → one terminal transition. A lease that
expires without settlement is reclaimed by the background loop back to `pending`;
the next holder re-leases under a new token.

**Invariants a replacement must preserve.**
- **Fence before acting.** Every settlement carries the `lease_token`; a stale
  token must no-op (`updated = false`), never mutate a row it no longer owns.
- **Reconcile before Ack.** `Ack` is a claim that durable business state was
  already committed or resolved; a consumer must reconcile the durable row before
  acknowledging. Because lease expiry never dead-letters, the recovery path
  depends on the next holder revalidating and stale-acking already-done work.
- **One in-flight job per partition.** A consumer must not assume it can run two
  jobs of one session partition concurrently; the barrier serializes them.
- **No delay authority.** Backoff timing is Queue-owned; `Retry` carries only an
  error, not a delay.

**Conformance tests.**
`TestQueueServiceGeneratedClientLeasesAndFencesTransitions`,
`TestQueueServiceValidationErrorsMapToInvalidArgument` (`services/queue`);
`TestPostgreSQLStoreLeasePriorityPartitionBarrierAndAckFence`,
`TestPostgreSQLStoreLeaseHonorsCrossKindSessionBarrier`,
`TestPostgreSQLStoreLeaseRuntimeConfigBeforeRetriedRuntimeInput`,
`TestPostgreSQLStoreLeaseCandidateWindowCannotStarveEarlierBarrierJob`,
`TestPostgreSQLStoreCancelInterruptFenceOnlyCancelsPendingOlderSameThreadMessages`,
`TestPostgreSQLStoreRetryDeadLetterAndReclaimExpiredLeases`,
`TestPostgreSQLStoreDeferPreservesSessionPrepareAttemptBudgetAndFencesKind`
(`internal/queue`).

## Testing guide

| Suite (file) | Proves |
|---|---|
| `internal/queue/queue_test.go` | admission validation: per-kind canonical shape, references-only payload bounds, event-reference limits, lease batch-capacity arithmetic |
| `internal/queue/postgresql_store_test.go` | the store's durable behavior against PostgreSQL: lease ordering + priority overtaking + partition barrier, ack/retry/defer/dead-letter fencing, cancel-by-interrupt-fence, backoff full-jitter, unset-`max_attempts` projection, cross-workspace reclaim, metrics summary |
| `services/queue/server_test.go` | the gRPC surface over the generated client: lease + fenced transitions, maximum legal batch within the message fuse, the field census matching lease arithmetic, validation → `InvalidArgument` mapping |
| `services/queue/config_test.go` | `ConfigFromEnv` pins the retry policy and rejects invalid values |
| `services/queue/maintenance_test.go` | the reclaim loop logs shared operation/error fields |
| `services/queue/cmd/tetral-queue/main_test.go` | schema-behind startup stops before the store and listener; startup-failure logs use shared fields |

Run the store suite (and any test that opens PostgreSQL) with the race detector
on. If a PR changes the `queue_jobs` invariants, admission validation, lease
selection or barrier, any transition, the backoff formula, or the reclaim loop in
this folder, it updates the matching section here.
