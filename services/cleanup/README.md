# cleanup

## Responsibilities

`cleanup` (Go package `tetralcleanup`, binary `cmd/tetral-cleanup`)
is the TTL scheduler for idle sessions and the retention pruner for expired
API idempotency receipts and event-stream change rows. It runs as a Kubernetes
CronJob: each tick runs three independent bounded phases in order — Session
scheduling, receipt retention, then change retention (below). Scheduling
discovers sessions, across all workspaces, that have sat idle past their
cleanup deadline, and enqueues one `cleanup_session` queue job per due
session. It **produces** cleanup work and never **executes** it —
releasing hot Runtime Pod state belongs to Job Runner (`services/job-runner`,
`runtime_session_cleanup.go`). TTL cleanup does not stop, archive, or delete a
Sandbox. Provider-native auto-stop, auto-archive, and auto-delete continue on
their own lifecycle; a later Sandbox tool inspects and normalizes the provider
resource before execution. Session deletion owns the durable Sandbox release
request. Every process is a fresh CronJob invocation; the only state carried
between ticks is the durable scheduling cursor row described below.
Discovery and retention are the only cross-workspace work and each goes
through a Cleanup-only database function; every claim and enqueue is a
transaction scoped by `workspace_id` (with `workspace_id` in every primary key
and workspace RLS isolating tenants).

The production database connection requires `TETRAL_DATABASE_TLS_CA_PATH` and
`TETRAL_DATABASE_TLS_SERVER_NAME`. It verifies trust and hostname with no
plaintext fallback. New connections load the current validated trust generation;
shutdown joins requests/work before closing the database and trust observer.

## States & lifecycle

### Cleanup marker columns on `session_runtime_status`

The schema lives in `internal/storage` (`postgresql_schema.go` and
`postgresql_cleanup_schema.go`). Per-session cleanup state is four columns on
`session_runtime_status` plus the two binding columns; ownership of each
transition is split between Bridge or Job Runner idle writes (arm), Job Runner
(finalize/reschedule) and this scheduler (claim/enqueue).

| Column | Set by | Cleared / advanced by |
|--------|--------|-----------------------|
| `cleanup_after` | Bridge or Job Runner idle write, a fixed 30-minute delay past idle | Job Runner finalize and terminal Session closeout set it `NULL`; a busy reschedule pushes it forward by 30 minutes |
| `cleanup_job_id` | this scheduler, when it claims a due row | Job Runner finalize and busy reschedule set it `NULL` |
| `cleanup_enqueued_at` | this scheduler, at claim | Job Runner finalize and busy reschedule set it `NULL`; new-input admission clears it when no claim is active |
| `cleanup_claimed_at` | Job Runner, at execution claim time | Job Runner finalize and busy reschedule set it `NULL`; this scheduler resets any stale value at re-enqueue |
| `binding_id` / `binding_generation` | binding creation | Job Runner finalize sets both `NULL` |

The 30-minute delay is owned by shared Runtime control
(`internal/runtimecontrol.IdleCleanupDelay`); no configuration surface wires it. The same constant
serves both the initial idle re-arm and the busy reschedule, so the two delays
cannot drift apart. Its length is the window a bound-but-idle, non-terminal
session stays hot before a due cleanup releases its Runtime Pod binding. A
terminal Session is the exception: the shared termination owner
(`runtimecontrol.SettleRuntimeTerminationTx`, used by Bridge and Job Runner)
closes its residency row to `idle`, clears every cleanup marker, and retains
the binding identity only for closeout replay. Any already-issued cleanup job
then converges through the stale-job path and cannot target the Runtime. Sandbox
auto-stop/auto-archive/auto-delete timing and the 30-day retention floor in
`services/sandbox/config.go` are independent of this TTL.

### Scheduling cursor and election

The service owns one global maintenance row, `cleanup_schedule_cursor`. It
holds no tenant data, so it has no workspace RLS; the Cleanup role can only
SELECT and UPDATE it, and no other workload can read it. Fresh installation
inserts its only row as `(true, 0, NULL, NULL, NULL)`; a missing row is an
invariant error (`ErrScheduleCursorMissing`) that the scheduler never repairs.

| Column | Meaning |
|--------|---------|
| `owner_generation` | incremented by each elected scheduler before it reads or changes the cycle; never reset and never wraps (overflow is an error) |
| `cycle_cutoff` | the database clock when the current cycle started; only rows due at or before it belong to the cycle |
| `after_cleanup_at` / `after_session_id` | the last checkpointed candidate key; both `NULL` before the first checkpoint of a cycle |

A CHECK allows exactly three progress shapes: no cycle (all three `NULL`), a
new cycle (cutoff only), or a positioned cycle (cutoff plus a complete,
nonempty key).

Only one process schedules at a time. The phase takes the session advisory
lock `pg_try_advisory_lock(pg_catalog.hashtextextended('tetral.cleanup.scheduler', 0))`
on a dedicated connection (`dbconnect.Client.TryWithSessionLock`); if another
session holds it, the phase ends successfully without work. On that same
connection it increments `owner_generation` and reads the cycle state in one
statement, and that generation fences everything the phase writes:

- every cycle start, checkpoint and end-of-cycle reset is an UPDATE with
  `owner_generation = $owned`; zero affected rows means a newer owner took
  over, and the phase stops with `ErrSchedulingOwnershipLost`;
- before each candidate claim the phase rereads the generation on the
  election connection; a different value stops it, and a failed read (the
  election connection is gone) stops it too.

The advisory lock excludes overlapping owners; the generation fences a
replacement owner whose predecessor lost its connection without noticing.
The predecessor may finish one already-started claim, which is idempotent, but
it cannot checkpoint, reset or move back the new owner's cursor. Before
returning, the phase unlocks explicitly; if unlock fails, the connection is
closed instead of returning to the pool, because pgx connections keep session
locks across pool reuse. A crashed process releases the lock with its
connection.

### Due predicate and discovery order

A cycle starts by saving the database clock as `cycle_cutoff`. Each page is
one short read-only transaction calling
`public.tetral_cleanup_due_sessions(cycle_cutoff, after_cleanup_at, after_session_id, limit)`,
which returns only `workspace_id`, `session_id` and `cleanup_after` of rows
matching

```
status = 'idle'
AND cleanup_job_id IS NULL
AND binding_id IS NOT NULL
AND cleanup_after <= cycle_cutoff
AND (cleanup_after, session_id) > (after_cleanup_at, after_session_id)
```

in `(cleanup_after, session_id)` order across all workspaces. Session IDs are
globally unique, so equal due times still have a total order. The function
rejects a cutoff later than the database clock, a partly `NULL` or empty key
and a limit outside 1..100 with SQLSTATE `22023`.

Each predicate term is load-bearing. Job Runner finalize nulls **both**
`binding_id` and `cleanup_after`: either alone unmatches the row, and
together they guarantee an already-cleaned session never re-matches and
never re-enqueues a no-op job on every tick. The `binding_id IS NOT NULL`
guard additionally keeps the scheduler from claiming a session whose
binding is already gone.

Discovery is served by the global partial index
`idx_session_runtime_status_cleanup_global_due` on
`session_runtime_status(cleanup_after, session_id) WHERE status = 'idle' AND
cleanup_job_id IS NULL AND binding_id IS NOT NULL` (`internal/storage`,
`postgresql_cleanup_schema.go`). Its predicate repeats the due predicate term
for term; any change to the predicate must move the index in lockstep or
discovery loses its ordered index range. There is no workspace-table scan.

The function is `SECURITY DEFINER`, owned by the migration role like its
table, with `search_path = pg_catalog` and schema-qualified objects; PUBLIC
execution is revoked and only Cleanup may execute it. It sets the
transaction-local flag `tetral.cleanup_discovery`, and the SELECT-only
`cleanup_discovery` policy on `session_runtime_status` requires that flag
**and** `current_user` equal to the table owner, so Cleanup setting the flag
itself gains no cross-workspace read.

### Scheduling phase

| Bound | Value |
|-------|-------|
| Phase budget | 45 s child of the process context |
| Candidate attempts per phase | 1000 |
| Page size | `min(TETRAL_CLEANUP_CLAIM_LIMIT, 100)` |
| Cursor, election and page queries | 1 s each |
| One candidate claim | 2 s |

| Step | Actor | Effect |
|------|-------|--------|
| 1 | Bridge or Job Runner idle write | stamps `cleanup_after` when a reusable run finishes |
| 2 | scheduler election | takes the advisory lock, increments `owner_generation`, resumes the persisted cycle or starts one at the database clock |
| 3 | scheduler discovery | reads one page after the persisted key and closes the snapshot before any claim |
| 4 | scheduler claim (`claim`, `markCleanupEnqueuedTx`) | in one workspace transaction: takes the Session's runtime arbitration lock, reads the database clock, mints a fresh `cleanup_job_id` and, through a guarded UPDATE that repeats the due predicate against `cycle_cutoff`, stamps `cleanup_enqueued_at`, resets stale `cleanup_claimed_at`, then writes one `queue_jobs(kind = cleanup_session)` row in the session partition, deduped by the minted id, available at that database time |
| 5 | scheduler checkpoint | in a separate short statement, records the candidate's discovery key, whether the claim succeeded, was stale or failed |
| 6 | Job Runner | leases the job, re-validates the fences, settles Runtime waits, and finalizes the Runtime binding |

The guarded UPDATE affecting no row means the candidate became stale; no job
is written. The cycle cutoff only bounds eligibility: the marker and Queue
timestamps come from the clock read after the arbitration lock, so new work is
never backdated.

A failed claim is counted, its key is still checkpointed, and the phase
continues; the failures are returned together when the phase stops. A failure
to checkpoint, discover or check the generation stops the phase without
processing later keys. The phase also stops after 1000 attempts or when its
own 45 s budget ends, leaving the cursor at the last checkpoint, so the next
tick resumes behind it and a persistently failing prefix is crossed across
ticks instead of being retried from the start. Reaching either limit alone is
success; an ownership loss, cursor failure or failed claim stays an error even
when the budget ends at the same time.

Database failures are errors, so a phase with any failed claim makes the Cron
process exit non-zero. With the CronJob's `restartPolicy: OnFailure` and
`concurrencyPolicy: Forbid`, a persistently failing Session therefore makes the
Jobs whose cycles reach it exit non-zero and restart with Kubernetes backoff,
while the cursor still advances past that Session on every attempt. A crash between a claim and its
checkpoint repeats that candidate, and the guarded UPDATE makes the repeat a
no-op. A short page ends the cycle: the cursor is reset and no new cycle
starts in the same tick. Rows that become due after the cutoff, or change
behind the cursor, are picked up by the next cycle, so a failing Session is
retried once per completed cycle rather than hot-looped. Work per tick is
bounded by these limits regardless of tenant count.

### Invocation phases and retention

One invocation reads the database clock once, minus 24 hours, as the cutoff
for both retention phases, then runs:

| Order | Phase | Budget |
|-------|-------|--------|
| 1 | Session scheduling (above) | its own 45 s child of the process context |
| 2 | Receipt retention: `session_event_idempotency_keys` rows with `created_at <= cutoff` | at most 10 transactions of at most 256 rows, each with its own 2 s deadline from the process context |
| 3 | Change retention: `session_event_stream_changes` rows with `changed_at <= cutoff`, advancing feed watermarks | the same, independently |

A phase failure, or scheduling ending on its own budget, is recorded and the
next phase still runs; only cancellation of the process context stops the
remaining phases. Every error is joined and returned after the metrics export,
so the Cron process exits non-zero. All limits are internal constants. With
the minute cadence and `concurrencyPolicy: Forbid`, a launch may be skipped
when all three phases use their full budgets (about 85 s), rather than
cancelling a later retention phase.

Each retention transaction calls its Cleanup-only `SECURITY DEFINER` function
with the cutoff, the last examined key of the previous transaction and limit
256:

- `public.tetral_prune_event_idempotency(timestamptz, timestamptz, text, text, bytea, integer)`
  pages in `(created_at, workspace_id, session_id, idempotency_key_digest)`
  order over `idx_session_event_idempotency_keys_age`;
- `public.tetral_prune_event_changes(timestamptz, timestamptz, text, text, bigint, integer)`
  pages in `(changed_at, workspace_id, session_id, stream_position)` order over
  `idx_session_event_stream_changes_age`, and from exactly the rows its DELETE
  returned raises each eligible feed's `session_event_feed_retention.pruned_through`
  with `GREATEST`, upserting in `(workspace_id, session_id, feed_key)` order
  (feed eligibility is in `services/event-stream/README.md`).

Both lock their page `FOR UPDATE SKIP LOCKED`, repeat the age condition in the
DELETE under the lock, and return deleted count, examined count, the last
examined key and `more_remaining`, never content. `more_remaining` is one
indexed `LIMIT 1` probe, with the page's own eligibility predicate, for another
eligible row after the last examined key (after the caller's continuation when
the page examined nothing). They cap the cutoff at their own database clock
minus 24 hours, clamp the limit to 0..256 and reject a `NULL` cutoff or limit
and a partly `NULL` or empty continuation with SQLSTATE `22023`. A short page
or `more_remaining = false` ends the phase (candidates ran out). A failed
transaction rolls back and ends its phase with no continuation. A row locked by
another transaction is skipped and not revisited in the same phase; the next
invocation starts again at the oldest eligible row. A phase that spent all 10
transactions while its last call returned `more_remaining = true` stops as
successful partial maintenance and increments
`tetral_cleanup_retention_budget_exhausted_total{phase}`; exactly 2,560
eligible rows end the phase without counting. Per Cron minute this
deletes at most 2,560 receipts and 2,560 change rows; sustained creation above
that accumulates a backlog. Retention ages define eligibility; rows are removed
after, not at, the boundary.

Receipt pruning and API admission serialize on the receipt row: API admission
locks a supplied key's receipt before reading its own admission clock and
replaces an expired receipt in its admission transaction. When the pruner
holds the row first, admission waits and then finds no receipt; when admission
holds it first, the pruner skips it, and the replacement's new `created_at`
keeps it out of the cutoff. The pruner takes no Session lock. Change pruning
takes no Session arbitration either; only the first watermark INSERT for a
feed takes the foreign-key `KEY SHARE` locks on its Session and Thread rows,
bounded by the 2 s transaction deadline.

The functions are owned by the migration role like their tables, run with
`search_path = pg_catalog` and schema-qualified objects, set the
transaction-local flag `tetral.retention_maintenance`, and have PUBLIC
execution revoked; only Cleanup may execute them. The `retention_maintenance_*`
policies admit rows only when that flag is set **and** `current_user` is the
table owner, so a serving role that sets the flag itself gains nothing.
Cleanup has no direct grant on receipts, changes or feed metadata.

### The tree fence (role-blind busy check)

The `cleanup_after` alarm is only a hint — it is armed by the main run and
may be stale while children still run. **The authority is the claim.**
Inside Job Runner's claim transaction (holding the `session_runtime_status`
row lock), and again inside the finalize transaction, cleanup proves that
no `session_threads` row is busy. The check is **role-blind**: it scans
every thread of the session regardless of role, so a running
approval-reviewer sub-agent thread blocks cleanup exactly as a running
main thread does.

| `session_threads.status` | Classification |
|--------------------------|----------------|
| `running` | busy — blocks cleanup |
| `rescheduling` | busy — blocks cleanup |
| `idle` | quiescent |
| `requires_action` | quiescent (an approval may wait days; the confirmation is durable and in the wake-input fence) |
| `closed_for_runtime` | quiescent |
| `terminated` | quiescent |
| `failed` | quiescent |

A busy result **reschedules, never drops** — dropping would leave
`cleanup_job_id` set and the scheduler would never re-enqueue the row (an
unsleepable session). The reschedule (`rescheduleBusyCleanupSessionTx`)
clears all three cleanup markers — `cleanup_job_id`, `cleanup_claimed_at`,
`cleanup_enqueued_at` — and pushes `cleanup_after` forward by 30 minutes.

| Enforcement point | Owner (`runtime_session_cleanup.go`) | On busy |
|-------------------|--------------------------------------|---------|
| Claim time | `prepareCleanupSessionCommandTx` → `claimCleanupSessionTx` | reschedule in the claim tx; ACK stale |
| Finalize time | `FinalizeRuntimeCleanup` → `claimCleanupSessionTx` | **must** reschedule inside the finalize tx — a bare duplicate/stale ACK would strand `cleanup_job_id` on a past-due idle row forever |
| Pod side | Runtime Pod eviction refusal (`session_busy`) | final authority when the durable thread status lags hot truth: the pod refuses while any run slot is active **or** any thread's accepted-input queue is non-empty (so cleanup never wipes a queue still holding unreceipted mail) |

New input admission also clears stale cleanup markers when no claim is
active, so a woken session sheds its pending cleanup naturally, and the
claim additionally rejects a job when unprocessed input arrived after the
idle fence (`cleanupHasNewerUnprocessedInputTx`).

## Seams

### Scheduling phase (`scheduler.go`)

`Scheduler.RunSchedulingPhase` is the claim seam. Contract: it derives its own
45 s budget from the caller's context, releases its election before returning,
reads `session_runtime_status` only through the discovery function and inside
its claim transactions, and writes exactly three things — the cleanup marker
columns, one `queue_jobs` row per claimed session via `queue.EnqueueTx`, and
the fenced `cleanup_schedule_cursor` row. Invariants a replacement must
preserve: the exact due predicate above; one claim transaction per Session,
taken under its runtime arbitration lock and re-guarded against the same
predicate before enqueue (`markCleanupEnqueuedTx` returns `false` → no job);
the queue job is deduped by the minted `cleanup_job_id`; marker and job commit
together; every cursor write carries the owned generation; every attempted key
is checkpointed. It never calls Runtime Pod, Bridge, Sandbox Service, or the
sandbox provider, and never touches durable history (`session_threads`,
`session_events`, `session_messages`).
Conformance: `TestSchedulingPhaseClaimsDueSessionsAcrossWorkspaces`,
`TestSchedulingClaimStampsDatabaseTimeAfterSessionArbitration`,
`TestSchedulingOwnershipLossFencesTheOldOwner`,
`TestSchedulingReplacedOwnerMakesNoFurtherClaim`,
`TestSchedulingReplacedOwnerCannotStartOrResetTheCycle`,
`TestSchedulingPhaseDeadlineHidesNoOtherError`,
`TestSchedulingCrossesAPersistentFailingPrefix`,
`TestSchedulingStopBetweenClaimAndCheckpointRepeatsWithoutDuplicates`,
`TestSchedulingClaimLimitAboveOneHundredYieldsPagesOfOneHundred`,
`TestSchedulingGenerationOverflowIsAnError`,
`TestSchedulingMissingCursorIsAnInvariantError`,
`TestCleanupDiscoveryFunctionBoundary`,
`TestCleanupDiscoveryUsesGlobalDueIndex`,
`TestCleanupWorkloadStaysWithinSchedulerBoundary`.

### Retention phases (`retention.go`, `cmd/tetral-cleanup/main.go`)

`Retention.Cutoff`, `Retention.PruneIdempotencyReceipts` and
`Retention.PruneStreamChanges` are the retention seam; `runPhases` orders the
three phases. Contract: retention reads and writes receipts, changes and feed
watermarks only through the two functions above, one batch per transaction,
adopting a continuation only from a committed batch; a phase stops on a short
page, an empty probe, a failed batch or its 10-transaction budget, and only the
budget with a last probe that still found a row counts as budget exhaustion.

Invariants a replacement must preserve: one database cutoff per invocation;
independent phases that run after a failure of another; deletion and watermark
changes in one transaction; no Session lock.

Conformance: `TestIdempotencyRetentionFunctionPagesAtTheExactCutoff`,
`TestChangeRetentionAdvancesOnlyEligibleFeedWatermarks`,
`TestOverlappingChangePrunersSkipLockedPagesAndKeepTheGreatestWatermark`,
`TestChangeRetentionWaitsOnTheSessionParentAndRollsBackAtItsDeadline`,
`TestRetentionPhasesCountBudgetExhaustionOnlyWithRowsLeft`,
`TestRetentionPhaseDoesNotRevisitASkippedLockedRow`,
`TestRetentionFunctionsSecurityBoundary`,
`TestRetentionPruningReadsTheAgeIndexes`,
`TestCleanupRunsEveryPhaseInOrderAndReturnsJoinedErrorsAfterExport`.

### Execution boundary (Job Runner — `job-runner/runtime_session_cleanup.go`)

Everything after enqueue belongs to Job Runner and is a replaceable executor
behind the `cleanup_session` queue job. The contract this scheduler
depends on: the executor re-validates the idle fence, binding generation,
target Runtime Pod, and the role-blind tree fence at **both** claim and
finalize; a stale job (new input, changed binding, tombstoned session) is
ACKed with no side effects; finalize nulls `binding_id` **and**
`cleanup_after` together so the due predicate stops matching. A replacement
executor must keep the finalize-time busy reschedule (never a bare ACK). This
executor does not change Sandbox provider state; Session deletion uses its
separate cleanup kind and durable Sandbox release operation.
Conformance (`integration/runtime_session_cleanup_test.go`):
`TestPostgreSQLRuntimeDeliveryStoreCleanupSessionReschedulesWhileChildRuns`,
`...ReschedulesWhenChildStartsBeforeFinalize`,
`...TreeFenceClassifiesQuiescentAndBusyThreads`,
`...FinalizesWhenRuntimePodProvenGone`,
`...KeepsResolvingConfirmationAfterClaim`,
`...IgnoresPreIdleUnprocessedInputByStreamFence`,
`...RejectsPostIdleChildInputByStreamFence`.

### Metrics export (`metrics.go`, `metrics_exporter.go`)

`SchedulerMetrics` accumulates three OpenMetrics counters, each updated once
per scheduling phase —
`tetral_cleanup_claim_due_runs_total`, `tetral_cleanup_jobs_claimed_total`,
`tetral_cleanup_claim_due_duration_ms_total` — and
`tetral_cleanup_retention_budget_exhausted_total` with the fixed label
`phase="idempotency"` or `phase="stream_changes"`, exposed through
`SchedulerMetrics.Collector()`. `MetricsExporter` /
`OpenMetricsHTTPExporter` optionally POST them to
`TETRAL_CLEANUP_METRICS_EXPORT_URL`.

Each retention phase also logs `cleanup.retention.completed` with its `phase`,
`outcome` and aggregate `page.count` (batches), `candidate.count` (examined)
and `deleted.count` only.

Invariants a replacement must preserve: counters carry no per-scope labels
(the retention counter's only label is its fixed phase); the series names are
stable; export is off by default and the shipped `k8s/networkpolicy.yaml`
(postgres + DNS egress only) blocks it unless deployment opens the path.
Conformance:
`TestSchedulerMetricsCollectorReportsSafeCounters`,
`TestOpenMetricsHTTPExporterPushesSchedulerSeriesWithoutScopeLabels`.

### Configuration (`config.go`)

`ConfigFromEnv` reads three env vars: `TETRAL_CLEANUP_CLAIM_LIMIT`
(positive integer, default 100; the size of each global discovery page, which
the scheduler caps at 100 internally, not a per-workspace batch),
`TETRAL_CLEANUP_METRICS_EXPORT_URL`
(HTTP(S), no credentials, else startup error), and
`TETRAL_CLEANUP_METRICS_EXPORT_TIMEOUT` (positive duration, default 2s).
The tick schedule lives in `k8s/cronjob.yaml` (every minute,
`concurrencyPolicy: Forbid`); the page size is config; the phase limits are
the internal constants above; the TTL delay is
`runtimecontrol.IdleCleanupDelay`. Invalid positive-integer / URL /
duration settings are startup errors (`workload.NewConfigError`).
Conformance: `TestConfigFromEnvValidatesMetricsExporter`.

## Testing guide

| Suite | Proves |
|-------|--------|
| `scheduler_test.go` | with the installed Cleanup role: global discovery order across workspaces; markers stamped and one deduped job enqueued with database time read after arbitration; generation fencing of a replaced owner's checkpoint, cycle start, end-of-cycle reset and next claim; only the phase's own deadline counts as success; a 1000-candidate failing prefix crossed across ticks with pages of one and equal due times; a stop between claim and checkpoint repeated without duplicates; pages capped at 100; generation overflow and a missing cursor row as errors; the workload stays within its read/write boundary; metrics counters stay safe |
| `retention_test.go` | with the installed Cleanup role: receipt pages in key order across workspaces, the exact cutoff included and a microsecond younger kept, the cutoff cap, limit clamp and argument validation; change pruning advances only eligible feed watermarks, never backward, and rolls back with its deletion; overlapping pruners skip each other's pages; a held Session parent times the batch out atomically; exactly 2,560 eligible rows do not count budget exhaustion while 2,561 count it once, and a skipped locked row is not revisited; the `more_remaining` probe on each page; denied direct access, other-workload execution, spoofed flags, search-path shadowing and non-owner definers; both age indexes serve the locked page under its Limit and the `LIMIT 1` probe |
| `cmd/tetral-cleanup/main_test.go` | the three phases run in order, a failed receipt batch does not skip change retention, errors return after the metrics export, and cancellation skips the remaining phase |
| `discovery_boundary_test.go` | the discovery function's real-role pages and continuation boundaries, argument validation, denied direct and spoofed reads, other workloads' denied execution, search-path shadowing, cursor privileges and CHECK, owner-checked policy and catalog posture; the generic plan of both discovery statements reads the global due index under its Limit |
| `metrics_exporter_test.go` | exported series carry no scope labels; config validation rejects a bad exporter endpoint |
| `integration/runtime_session_cleanup_test.go` | the executor contract this scheduler depends on: role-blind tree fence and reschedule-at-both-points, stale-job ACK, Runtime settlement before finalization, stream-fence input rejection |

If a PR changes the due predicate, the scheduling phase or its cursor, the
retention phases or their functions, the marker writes, the enqueue shape, or
the metrics/config surface in this folder, it updates the matching section
here. A due-predicate change must
also move the discovery function and the
`idx_session_runtime_status_cleanup_global_due` partial index in
`internal/storage` in lockstep so discovery stays an ordered index range. If it changes the tree fence,
reschedule, or finalize order, it updates the execution-boundary seam and
its conformance list.

## Process diagnostics

The command follows the shared [Go process diagnostic contract](../../internal/workload/README.md#diagnostics)
for the restart-only `TETRAL_LOG_*` controls, the default Info level, bounded
suppression summaries, diagnostic drop and sink-failure metrics, and the
diagnostic close after listeners and business resources.

## Operation measurement

The existing optional exporter also carries the additive
`tetral_operation_duration_seconds` histogram with `service="cleanup"`.
`claim_due` records each scheduling phase, including error, cancellation and
deadline failure; a phase that finished with failed claims is an `error`, even
when a claim failed on its own deadline. Workspace IDs never become labels. The three counters keep
their units and count scheduling phases, committed claims and phase duration.
The phase is observed once before the final export; a later export failure is
separate from claim failure, and a CronJob registry does not persist across
invocations. Deployment must collect each invocation's export rather
than treating its reset counts as a resident counter.

Fixed seconds buckets and replica percentile queries follow the
[shared operation duration contract](../../internal/workload/README.md#operation-durations).
