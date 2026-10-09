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
`QueueService`) that consumers drive, one per-process Job Runner scheduler that
chooses which workspace's Runner work to lease next, and one background
goroutine that rescues leases their owners abandoned and bounds Sandbox
notification retention. The store reads and writes only `queue_jobs` and
`queue_partition_counters`; it never touches the business tables (`session_events`,
`session_sandbox_bindings`, `sandbox_lifecycle_operations`, and the rest), never calls Runtime Pod,
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
`leased_at`, `leased_until`, and the private `lease_previous_attempt_count`
described under [Direct Job Runner leasing](#direct-job-runner-leasing)), `attempt_count` / `max_attempts`, the config/build observation
`defer_count`, a `priority`, an `available_at`, and a `status`. Runtime-facing
rows also carry structured scheduling authority: `causal_session_id` preserves
Session causal order, `delivery_scope` is `thread` or `session`,
`delivery_thread_id` identifies a Thread lane, and `control_class` distinguishes
ordinary, agent-mail, and interrupt delivery. Non-Runtime rows use the
independent `partition` delivery scope.
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
| `pending` | admitted, awaiting a lease; `Retry`/`Defer` re-admit with a backoff-delayed `available_at`; reclaim and `ReleaseUnstartedJob` re-admit at `available_at = now` | `Enqueue` (insert), `Retry` (budget left), `Defer`, `ReclaimExpiredLeases`, `ReleaseUnstartedJob` | `leased`, `cancelled`, `dead_lettered` |
| `leased` | one consumer holds the row under a `lease_token` for the lease window | `Lease`, `LeaseJobRunnerJobs` | `pending`, `acknowledged`, `dead_lettered` |
| `acknowledged` | terminal; the leased work committed | `Ack` | (none) |
| `cancelled` | terminal; pending work was fenced out — reached only straight from `pending`, never from `leased` | `Cancel` (`runtime_input` rows), `CancelTx` (one exact Sandbox notification identity) | (none) |
| `dead_lettered` | terminal; attempts exhausted or an explicit dead-letter | `Retry` (exhausted), `DeadLetter`, `DeadLetterExhaustedTx` after Sandbox business settlement | (none) |

### Transitions

All caller-driven writes off `leased` (`Ack`, `Retry`, `Defer`, `DeadLetter`,
`Heartbeat`) are fenced by `(workspace_id, job_id, lease_token)` and act only on a
row still `leased` under that token; a stale token reports `updated = false` and
changes nothing.

| Call | Fencing | Kinds | Effect |
|---|---|---|---|
| `Lease` | mints a fresh `lease_token` | any requested kind | workspace-scoped and generic (Sandbox kinds and environment build/fanout in production); discovers a bounded candidate set without row locks, locks distinct Session arbitration owners in canonical order, then locks and revalidates each exact Queue row against the shared lease eligibility; partition rows remain mutually exclusive, Thread rows conflict only with the same Thread or Session-exclusive work, and Session rows conflict with every Thread in that Session; increments `attempt_count`; projects an "unset" `max_attempts = 0` to the effective default in the response |
| `LeaseJobRunnerJobs` | mints a fresh `lease_token` per job | the five Job Runner kinds only | Queue chooses the workspaces (see [Direct Job Runner leasing](#direct-job-runner-leasing)); the request carries only `max_jobs`, `lease_owner` and `lease_duration_ms` (5000–300000); same eligibility, attempt increment and clamps as `Lease`; records the previous attempt count privately; returns `retry_after_ms` |
| `ReleaseUnstartedJob` | lease-token | the five Job Runner kinds, direct leases only | returns an observed but undispatched direct lease to `pending` at database time with its saved attempt count restored exactly; stale, duplicate or expired tokens report `updated = false`; a live token without direct-lease provenance is `FailedPrecondition` |
| `Heartbeat` | lease-token | any | pushes an unexpired `leased_until` forward and returns the database-written expiry; an expired lease cannot be revived |
| `Ack` | lease-token | any | → `acknowledged`; legal only after the consumer reconciled durable state and delivered/resolved the command |
| `Retry` | lease-token | any | carries an error kind/message only, no delay authority. If `attempt_count` reached the effective `max_attempts`, dead-letters instead. Otherwise → `pending` with capped exponential backoff + full jitter |
| `Defer` | lease-token | canonical `runtime_config_update` and `environment_build` | validates the stored refs-only payload, returns the same row to `pending`, refunds one `attempt_count`, increments `defer_count`, and clears custody without changing partition sequence. Config uses capped backoff from `defer_count`; Environment builds use a fixed 30-second delay, including at the ordinary attempt limit |
| `DeadLetter` | lease-token | any | → `dead_lettered` straight, carrying the error, for terminal invariant failures |
| `Cancel` | partition-scoped, **not** lease-fenced | `runtime_input` `input_kind = messages` only | requires `workspace_id`, `session_id`, `session_thread_id`, and a positive `interrupt_fence_sequence`; marks `cancelled` every `pending` matching row in that thread whose `sequence_to` is below the fence; touches no `leased`/terminal row and deletes no `session_events` |
| `ReclaimExpiredLeases` | exempt (matches `workspace_id`/`id`/`status = 'leased'` without the stale token) | any | background loop only; clears lease bookkeeping on rows `leased` with `leased_until <= PostgreSQL clock time` and returns them to `pending` at a database-written `available_at` with a `lease_expired` error stamp |

`Lease`, `LeaseJobRunnerJobs`, `Heartbeat`, `ReleaseUnstartedJob` and reclaim
author durable lease timestamps from fresh PostgreSQL clock time; consumer wall
clocks control only local scheduling.

### Direct Job Runner leasing

`LeaseJobRunnerJobs` moves workspace selection from the Runner into Queue. The
Runner asks for at most its free capacity; Queue discovers, rotates and leases.
Workspace is returned task scope, never a Runner scheduling input. The method
admits exactly `runtime_input`, `runtime_recovery`, `runtime_config_update`,
`cleanup_session` and `session_delete_cleanup`; every other kind, including
environment build/fanout, stays on workspace-scoped `Lease`, which keeps its
generic contract and is not restricted by kind.

**Discovery.** Two partial indexes over pending rows of those five kinds serve
it: `idx_queue_job_runner_scan` on `(workspace_id, negative_priority,
available_at, partition_key, queue_partition_sequence, id)` and
`idx_queue_job_runner_due` on `(available_at, id)`. The scan key K is that
index's suffix. `negative_priority` is the stored generated column
`-(priority::bigint)` (cast before negation); it is a plain column because
unary minus is not leakproof, so under row-level security a window bound on
the expression would be filtered after the policy instead of seeking. A turn first seeks the next
workspace after the scheduler cursor (`ORDER BY workspace_id LIMIT 1`), and
when none follows, the first workspace; there is no workspace-table listing,
`DISTINCT` or eligibility filter before that `LIMIT`. For the chosen tenant it
fetches a raw window of at most 32 rows of its current pass in K order,
before any time, lifecycle or barrier evaluation. Discovery reads only
`queue_jobs`, in read-only transactions with the `tetral.queue_maintenance`
setting, each statement under a 40 ms statement timeout.

**Exact lease.** Each examined candidate gets a fresh workspace transaction
with a 40 ms statement and 10 ms lock timeout: `pg_try_advisory_xact_lock` on
the candidate Session's arbitration key, then `FOR UPDATE SKIP LOCKED` on the
exact row, then one token-mint `UPDATE` that rechecks the shared eligibility
(the same predicate `Lease` uses, evaluated at `clock_timestamp()`). A busy
Session owner, a locked or missing row, an ineligible candidate or a local
timeout leaves no mutation and moves on. Any other database failure stops the
call: committed jobs are still returned, with a 100 ms hint; with none
committed, the call fails. Eligibility counts blockers of every kind, whether
or not this method returns that kind, and payload JSON is never authority.

**Fairness and progress.** The scheduler keeps per process a shared cursor and
generation; a turn commits its tenant proposal only if the generation is
unchanged, and a stale proposal is discarded. Each tenant has a pass with an
upper key and database time captured at pass start; only rows due and admitted
by then are considered, and later work waits for the next pass. The cursor
advances through the rows actually examined, never past an unexamined suffix
of a fetched window, and a tenant whose turn another call holds is skipped,
not awaited. One call takes at most one job per tenant, ends when the cursor
returns to a tenant it already visited, and is bounded by 16 tenant turns, 128
fetched rows (charged on fetch), 16 lease transactions and a 1000 ms budget.
Requests above 16 jobs therefore receive partial responses. Fairness is per
process: replicas keep their own cursors and exact row custody chooses one
winner. With T tenants holding only blocked or future Runner work, a newly
ready job is reached within about ⌈T ÷ 16⌉ calls. Pass state is memory only
and proportional to visited unfinished tenant passes; it has no LRU eviction.

**Retry hint.** `retry_after_ms` is 100 ms when a budget or lock contention
stopped the call and the process is not idle. The process becomes idle when
its cursor wraps without any lease committed since the previous wrap, and any
committed lease clears that. Otherwise the hint is the time until the next
future Runner job clamped to 100–1000 ms, or 1000 ms when none exists. A job
admitted after its tenant's current pass began is outside that pass. A call
woken by that job's notification which completes discovery by ending the pass
at its stored upper key returns no job and this future-clamped or 1000 ms hint;
the next call starts a fresh pass and leases the job, so it waits up to about
one hint.

**Release.** The direct lease records the prior attempt count in the private
`queue_jobs.lease_previous_attempt_count`. A CHECK allows a value only on a
leased row of the five kinds, every transition out of `leased` clears it
(Ack, Retry, Defer, DeadLetter, reclaim and the business-owner custody
transitions), and `Lease` never sets it. `ReleaseUnstartedJob` takes the
Session arbitration lock, then the exact row lock, samples database time only
after that wait, and restores the saved count. Queue does not infer
non-execution from a token: the Runner calls release only before dispatch.
Success emits the Job Runner wake class.

**Scheduler lifecycle.** The Queue process constructs one scheduler with its
store before registering the RPC service, and `Run` starts its single cleanup
worker. Each second the worker probes at most 128 pass entries with one
read-only statement and removes an unchanged, idle entry whose tenant has no
pending Runner work; it issues no SQL while no pass exists. Shutdown rejects
new direct leases with `Unavailable`, cancels admitted calls so they return
their committed jobs, joins them and the worker, and clears pass state before
the database pool closes. Restart resets preference and progress, never jobs
or leases.

Four in-process Queue boundaries support Sandbox business transactions without
moving business state into Queue. `CancelTx` cancels only the exact pending row
named by job id plus expected kind, partition, and dedupe key; a mismatch is an
integrity error and a leased row is unchanged. A row already removed by bounded
terminal retention is equivalent to an already closed transport row and is a
benign no-op. `ListPendingAtOrOverBudget`
performs a nonlocking, cross-workspace census of reclaimed Sandbox jobs whose
explicit attempt budget is spent. The Sandbox owner then settles its business
row before calling `DeadLetterExhaustedTx`, which rechecks pending status and the
observed attempt count in that same transaction. None of these are Queue RPCs.
`AssertActiveLeaseTx` is the final lock in a live Sandbox business transaction;
it verifies the source job, token, leased status, and unexpired database time so
loss of Queue authority rolls back the entire business write before settlement.

Backoff is `delay = rand(0, min(cap, base * 2^(count-1)))`, where `count` is
`attempt_count` for Retry, and `defer_count` for
config Defer. Environment build Defer instead uses a fixed 30-second delay to
bound polling frequency independently of failures. Sandbox owns the durable
build deadline and terminal settlement; Queue does not infer provider failure
from observation count. The full-jitter distribution is fixed, not a knob. Lease expiry alone never
dead-letters: the prior owner may have committed business success and only lost
the acknowledgement, so the next lease holder revalidates the durable row under
its own token and stale-acks when the work is already done. Attempt exhaustion
through `Retry` and an explicit `DeadLetter` are the leased-job routes to
`dead_lettered`; a Sandbox business transaction may also use
`DeadLetterExhaustedTx` after reclaim exposes an exhausted pending row.

### Invariants

Partial-unique constraints and lease-time compatibility checks carry the
durable invariants; the transition writers uphold them under concurrency.

| Invariant | Scope | Enforced by |
|---|---|---|
| At most one active job per `(workspace_id, dedupe_key)` | `pending` + `leased` only | `EnqueueTx` `ON CONFLICT … DO NOTHING` + partial-unique index; a later job for the same durable item is admitted once the prior one is `acknowledged`/`cancelled`/`dead_lettered` |
| At most one `partition` lease per `(workspace_id, partition_key)` | `leased` partition rows only | partial-unique partition index plus exact candidate revalidation |
| At most one Thread lease per `(workspace_id, causal_session_id, delivery_thread_id)` | `leased` Thread rows only | partial-unique Thread index plus compatibility checks against Session-exclusive leases |
| At most one Session-exclusive lease per `(workspace_id, causal_session_id)` | `leased` Session rows only | partial-unique Session index plus compatibility checks against every Thread lease in that Session |
| One causal position per partition | all jobs | the locked `(workspace_id, partition_key)` counter assigns `queue_partition_sequence`; Retry, Defer, and reclaim update availability/lease state without changing it |
| Direct-lease provenance exists only on a live direct lease | `lease_previous_attempt_count` | `queue_jobs_lease_previous_attempt_custody_shape` CHECK (non-NULL only while `leased` and of a Runner kind); only `LeaseJobRunnerJobs` sets it and every exit from `leased` clears it |

### The maintenance loop

One background goroutine owned by `Run` (`runStalledLeaseMaintenance`) runs
`ReclaimExpiredLeases` across all workspaces on a fixed interval, taking a bounded
batch per scan. This is what unsticks a `runtime_input`,
`cleanup_session`, `session_delete_cleanup`, or Sandbox job stranded by a crashed
consumer. After a successful reclaim pass, the same tick deletes at most 100
Sandbox-owned terminal notifications whose matching terminal timestamp is at
least 24 hours old, then in a separate transaction deletes at most 100 partition
counters that have no job of any status. Other job families have no retention
change. Both cross-workspace sweeps use the transaction-local
`tetral.queue_maintenance` RLS policy; a terminal row missing its required status
timestamp is reported as an integrity error and retained without preventing
eligible peers in the same bounded pass from being deleted or the subsequent
empty-partition-counter sweep from running.

The serving process owns this loop together with its RPC and HTTP listeners
and the Job Runner scheduler's cleanup worker.
Shutdown marks readiness unavailable and closes both request and maintenance
cycle admission, and quiesces the scheduler as described above. New maintenance cycles observe signal cancellation directly,
even before the main shutdown path is scheduled. A cycle already admitted may
finish during the same drain window as existing RPCs. At the deadline the service
cancels maintenance and HTTP database work and force-stops RPC transport, then joins every admitted
user before returning. Only then does the command close its database pool.
A signal or parent cancellation is a planned shutdown: a drain that joins every
user within `TETRAL_DRAIN_TIMEOUT_MS` returns success and the process exits 0,
while a forced cancellation returns a deadline error and a nonzero exit.
A cancelled reclaim transaction rolls back as a whole; another replica can
reclaim the remaining expired leases. Shutdown never implies reclamation
succeeded. Queue replicas share only PostgreSQL authority: Lease, Heartbeat,
and Ack can reach different replicas with the same Workspace/job/token.

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
| `TETRAL_CANCEL_JOIN_TIMEOUT_MS` | `5000` | positive cancellation/join allocation; drain plus join fits within 25000 ms in both profiles, reserving five seconds of the 30-second Pod grace for signal delivery and proxy shutdown |
| `TETRAL_DRAIN_TIMEOUT_MS` | `10000` | concurrent RPC/HTTP/maintenance completion window; 1–25000 ms, and together with the join it fits the 25-second application allocation |

Production database startup requires `TETRAL_DATABASE_TLS_CA_PATH` and
`TETRAL_DATABASE_TLS_SERVER_NAME` alongside `TETRAL_DATABASE_URL`. The shared
database owner verifies the server identity, refreshes trust for new
connections, and joins its credential watcher when the command closes the
pool after service shutdown. The executable uses one absolute drain-plus-join
deadline across RPC, HTTP, maintenance and cleanup. If any producer refuses
cancellation, it exits with status 1 while dependencies remain retained; reusable
service calls still wait for their actual users to join.

The retry policy is Queue-Service-owned; consumers carry no delay authority.

### Operational surface

gRPC on `:9090` serves `QueueService` and a gRPC health service; HTTP on `:8080`
serves `/health` (liveness), `/ready` (readiness), and `/metrics`. Metrics are
per kind: `queue_pending_jobs`, `queue_leased_jobs`, `queue_retry_pending_jobs`,
`queue_dead_lettered_jobs`, `queue_ready_jobs`, and `queue_ready_lag_seconds`.
`queue_ready_jobs` counts pending rows with `available_at` at or before the
observation time; `queue_pending_jobs` includes future Retry/Defer availability.
`queue_ready_lag_seconds` is the oldest available pending age. These are
availability gauges, not a claim that every available job is presently leaseable
under Session/Thread ordering. Per-kind groups absent from the database have no
series; a failed collector emits its error counter rather than invented zeros.
The shared [operation histogram](../../internal/workload/README.md#operation-durations)
records registered RPC outcomes and actual Queue drain/join boundaries. The service account
mounts no Kubernetes API token; the network policy restricts egress to PostgreSQL
and DNS, and ingress on both ports to `api`, `job-runner`, and
`sandbox`.

Each successful lease logs `duration.ms` for the database Lease call and
`queue.ready_wait.ms` for time elapsed since that job's `available_at`; retry
backoff before `available_at` is deliberately excluded. A direct Job Runner
call additionally warns, with fixed fields only, when candidates were skipped
on a local lock or statement timeout (`timeout.kind`, `failed.count`) or when a
failure stopped the call after earlier leases committed (`db.sqlstate` when
available). PostgreSQL LISTEN
disconnects log only fixed authentication, permission, endpoint/transport,
timeout, or unknown categories. Raw database errors, DSNs, queries, and
credentials are never included, and polling remains the reconnect fallback.

`internal/queue` owns the shared single-connection LISTEN machinery
(`wakeup.go`): `RunListener` reconnects one channel with backoff, fires a
readiness callback after the initial LISTEN and every reconnect, and passes
raw payloads to the caller, while `RunNotificationListener` layers the Queue
wakeup protocol (channel, consumer-class filter, `WakeSignal` broadcast) on
top. Other PostgreSQL notification protocols — currently the Sandbox
execution-result hints consumed by Bridge — reuse `RunListener` and the safe
disconnect classification with their own channel and payload handling; Queue
payload semantics are unchanged. `WakeSignal.Wait` retains its timer for Queue
consumers; `WaitForWake` shares the same snapshot/broadcast semantics but waits
only for a hint or context completion, as used by Bridge execution-result waits.

## Seams

### Seam 1 — Job kind registry

Seventeen kinds share the table. The queue stores `kind` as an opaque label and
never performs Sandbox business behavior; admission shape, partition identity,
and a few transport transitions are kind-specific.

| Kind | Partition family | Leased by |
|---|---|---|
| `runtime_input` | Session causal partition; Thread delivery lane | Job Runner |
| `runtime_recovery` | Session causal partition; Thread delivery lane | Job Runner |
| `runtime_config_update` | Session causal partition; Session-exclusive delivery | Job Runner |
| `cleanup_session` | Session causal partition; Session-exclusive delivery | Job Runner |
| `session_delete_cleanup` | Session causal partition; Session-exclusive delivery | Job Runner |
| `environment_build` | `environment:<workspace_id>:<environment_id>` | Sandbox Service |
| `environment_ready_fanout` | `environment:…` | Sandbox Service |
| `sandbox_tool_execute` | `sandbox-execution:<workspace>:<session>:<thread>:<tool-use-event>` | dedicated Sandbox execution runner |
| `sandbox_activate` | `sandbox-lifecycle:<workspace>:<logical-sandbox>` | Sandbox Service |
| `sandbox_materialize` | `sandbox-lifecycle:…` | Sandbox Service |
| `sandbox_release` | `sandbox-lifecycle:…` | Sandbox Service |
| `sandbox_tool_cancel` | `sandbox-cancel:<workspace>:<session>:<thread>:<tool-use-event>` | Sandbox Service |
| `sandbox_output_capture` | `sandbox-capture:<workspace>:<session>:<finish-idle-write>` | Sandbox Service |
| `sandbox_output_capture_cleanup` | `sandbox-capture:…` | Sandbox Service |
| `sandbox_memory_projection` | `sandbox-memory:<workspace>:<memory-store>` | Sandbox Service |
| `sandbox_background_command` | `sandbox-background:<workspace>:<session>:<task>` | Sandbox Service |
| `sandbox_background_reconcile` | `sandbox-background:…` | Sandbox Service |

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
not an additional kind — `isKnownKind` still admits only the seventeen above.
`runtime_recovery` likewise has two canonical sub-shapes, and
`DecodeRuntimeRecoveryPayload` accepts exactly one source: event-origin
`{session_id, session_thread_id, source_event_id}` deduplicated by
`FormatRuntimeRecoveryDedupeKey` (workspace, Session and source event), and
handoff-origin `{session_id, session_thread_id, handoff_id}` deduplicated by
`FormatRuntimeHandoffDedupeKey` (workspace, Session, Thread and handoff). Both
use the Session partition key.

The `runtime_input` kind carries an `input_kind` discriminator, checked by
`isRuntimeInputKind`, over a closed set: `messages`, `interrupt_control`,
`tool_confirmation`, `task_notification`, `agent_mail`. The kind-specific
behaviors below hinge on it (`Cancel` applies to `input_kind = messages`;
same-Thread precedence applies to `interrupt_control`). A `task_notification` marks a
background command's terminal completion; it is not a public user message and
produces no second public user event.

**Lifecycle.** A kind is admitted only through `EnqueueTx` or `EnqueueBatchTx`
inside the producer's transaction; from there it flows through the shared status machine above. The Job
Runner receives its five kinds through `LeaseJobRunnerJobs`; Sandbox consumers
lease the kinds they serve by name in `Lease.kinds`, where an unknown kind is a
validation error.

**Kind-specific behaviors a replacement must preserve.**
- `Defer` accepts refs-only SDK config-generation and MCP
  manifest-generation `runtime_config_update` rows, plus canonical
  `environment_build` rows. It validates the locked stored payload, retains
  causal position, and refunds only the current lease attempt. Artifact
  observation/restart policy belongs to Sandbox; unrelated kinds remain rejected.
- `Cancel` touches **only** `runtime_input` `input_kind = messages` rows.
- Same-Thread interrupt precedence applies **only** among `runtime_input`
  candidates: an `interrupt_control` may overtake earlier pending ordinary
  input in its own Thread lane. It never cancels or delays a sibling Thread.
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
clean version-one `queue_jobs` definition). A kind is
incomplete until all four agree; a kind admitted in code but absent from the
CHECK is rejected by the database on insert.

Every Sandbox payload carries durable references only and must set an explicit
positive `max_attempts`; Sandbox consumers never inherit the Queue Service's
deployment default. Lifecycle mutation kinds share one logical-Sandbox
partition, while independently runnable Tool Uses each receive their own
execution partition. Transport retry preserves the same dedupe identity and is
distinct from a new business execution-attempt generation.

**Conformance tests.** `TestPostgreSQLStoreRejectsNonCanonicalQueueShape`,
`TestPostgreSQLStoreAcceptsRuntimeMCPManifestUpdateCanonicalShape`,
`TestPostgreSQLStoreAcceptsTaskNotificationRuntimeInputWithoutPublicEventFence`,
`TestNormalizeEnqueueRequestRejectsRuntimeInputBeyondEventReferenceLimit`,
`TestNormalizeEnqueueRequestAcceptsOnlyBareAgentMailPokes`,
`TestNormalizeEnqueueRequestRejectsOversizedPayloadForEveryJobKind`
(`internal/queue`).

### Seam 2 — Lease semantics a consumer must preserve

A consumer is any process that leases and settles jobs (Job Runner,
Sandbox Service). The lease contract is what lets a crashed or slow consumer be
replaced without losing or double-committing work.

**Interface contract.** `Lease(workspace_id, kinds, lease_owner, max_jobs,
lease_duration_ms)` returns up to `max_jobs` leased rows, each with a fresh
`lease_token`. `LeaseJobRunnerJobs(max_jobs, lease_owner, lease_duration_ms)`
returns up to `max_jobs` Runner jobs across workspaces and a `retry_after_ms`
hint; `max_jobs` is bounded by `MaxJobRunnerLeaseJobs()`, which keeps a maximal
response inside the 4 MiB Queue Lease transport fuse, and invalid values are
rejected, never clamped. The consumer then drives exactly one terminal transition per job
(`Ack` / `Retry` / `DeadLetter`, or an admitted `Defer` back to `pending`),
calling `Heartbeat` to extend `leased_until` while it works.
Heartbeat returns the new database-written expiry. Consumers derive a
conservative monotonic deadline from the RPC send instant and never extend local
authority from an independent wall-clock comparison.
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
- **Compatible in-flight scopes only.** Two Thread-scoped jobs may run together
  only when their `delivery_thread_id` values differ. A Session-exclusive lease
  conflicts with every Thread lane in that Session. One narrow exception lets
  `interrupt_control` reach its target while an earlier config boundary is
  pending, deferred, or leased; ordinary successors remain fenced by that
  config boundary.
- **No delay authority.** Backoff timing is Queue-owned; `Retry` carries only an
  error, not a delay.
- **Release only what never started.** `ReleaseUnstartedJob` is for a direct
  lease the consumer observed but never dispatched; Queue checks token
  provenance, not execution.

**Conformance tests.**
`TestQueueServiceGeneratedClientLeasesAndFencesTransitions`,
`TestQueueServiceValidationErrorsMapToInvalidArgument` (`services/queue`);
`TestPostgreSQLStoreLeasePriorityPartitionBarrierAndAckFence`,
`TestPostgreSQLStoreLeaseHonorsCrossKindSessionBarrier`,
`TestPostgreSQLStoreLeaseRuntimeConfigBeforeRetryingRuntimeInput`,
`TestPostgreSQLStoreLeaseCandidateWindowKeepsInterruptException`,
`TestPostgreSQLStoreCancelInterruptFenceOnlyCancelsPendingOlderSameThreadMessages`,
`TestPostgreSQLStoreRetryDeadLetterAndReclaimExpiredLeases`,
`TestPostgreSQLStoreDeferCanonicalRuntimeConfigUsesScopedCounter`
(`internal/queue`).

## Testing guide

| Suite (file) | Proves |
|---|---|
| `internal/queue/queue_test.go` | admission validation: per-kind canonical shape, references-only payload bounds, event-reference limits, lease batch-capacity arithmetic |
| `internal/queue/postgresql_store_test.go` | the store's durable behavior against PostgreSQL: Thread/Session lease compatibility, same-Thread interrupt precedence, partition exclusion, ack/retry/defer/dead-letter fencing, both cancellation boundaries, over-budget conditional dead-lettering, Sandbox terminal retention and empty-counter cleanup, backoff full-jitter, unset-`max_attempts` projection, cross-workspace maintenance, metrics summary |
| `internal/queue/job_runner_lease_test.go` | direct Job Runner leasing under the real Queue role: future-tenant yield and window resumption, examined-prefix advancement under the transaction budget, per-call visited tenants with interleaved calls, per-process alternation and replica custody, committed tokens after a later failure, the eligibility matrix against `Lease`, discovery query plans on the Runner indexes, idle-aware retry hints, busy-tenant skipping, bounded pass cleanup, quiesce, exact release refunds, post-lock expiry, and provenance cleared by every transition |
| `services/queue/server_test.go` | the gRPC surface over the generated client: lease + fenced transitions, maximum legal batch within the message fuse for both lease methods, the field census matching lease arithmetic, validation → `InvalidArgument`, release `FailedPrecondition` and draining-scheduler `Unavailable` mapping |
| `services/queue/config_test.go` | `ConfigFromEnv` pins the retry policy and rejects invalid values |
| `services/queue/maintenance_test.go` | each maintenance tick runs reclaim, bounded Sandbox terminal retention, then bounded empty-counter cleanup, and logs shared operation/error fields |
| `services/queue/run_test.go` | admitted RPC, maintenance, and HTTP users join under graceful completion and forced cancellation; no later maintenance cycle starts after drain admission closes; the Job Runner scheduler starts once and quiesces before `Run` returns |
| `integration/replica_queue_test.go` | three independent Queue receivers share lease authority; lost committed Lease/Ack responses, real expiry/reclaim, stale-token rejection on every transition, and Workspace/Session barriers |
| `integration/replica_queue_maintenance_test.go` | a real reclaim UPDATE is held before commit; normal completion commits the whole batch, forced cancellation rolls it all back, the pool stays alive until join, and a replacement maintenance owner reclaims remaining work |
| `services/queue/cmd/tetral-queue/main_test.go` | schema-behind startup stops before the store and listener; startup-failure logs use shared fields |

Run the store suite (and any test that opens PostgreSQL) with the race detector
on. If a PR changes the `queue_jobs` invariants, admission validation, lease
selection or barrier, the Job Runner scheduler, any transition, the backoff formula, or the maintenance loop in
this folder, it updates the matching section here.

## Process diagnostics

The command follows the shared [Go process diagnostic contract](../../internal/workload/README.md#diagnostics)
for the restart-only `TETRAL_LOG_*` controls, the default Info level, bounded
suppression summaries, diagnostic drop and sink-failure metrics, and the
diagnostic close after listeners and business resources.
Successful Lease records (`queue.job.leased`) use Debug.

Queue wake notifications retain the established consumer-class payload `bridge`
for Job Runner work. The Go name `ConsumerClassJobRunner` describes its current
owner; changing the process owner does not rename the wire value.

A notification disconnect emits a safe warning and bounded repeat summaries.
The existing successful reconnect broadcasts its catch-up wake before emitting
an Info recovery, so logging does not determine Queue wake delivery.
