# cleanup

## Responsibilities

`cleanup` (Go package `tetralcleanup`, binary `cmd/tetral-cleanup`)
is the TTL scheduler for idle sessions. It runs as a Kubernetes CronJob:
each tick enumerates every workspace, finds sessions that have sat idle
past their cleanup deadline, and enqueues one `cleanup_session` queue job
per due session. It **produces** cleanup work and never **executes** it —
releasing hot Runtime Pod state belongs to Job Runner (`services/job-runner`,
`runtime_session_cleanup.go`). TTL cleanup does not stop, archive, or delete a
Sandbox. Provider-native auto-stop, auto-archive, and auto-delete continue on
their own lifecycle; a later Sandbox tool inspects and normalizes the provider
resource before execution. Session deletion owns the durable Sandbox release
request. Every process is a fresh
CronJob invocation holding no state between ticks, and every read and
write is scoped by `workspace_id` (a signed principal binding, with
`workspace_id` in every primary key, isolates tenants).

The production database connection requires `TETRAL_DATABASE_TLS_CA_PATH` and
`TETRAL_DATABASE_TLS_SERVER_NAME`. It verifies trust and hostname with no
plaintext fallback. New connections load the current validated trust generation;
shutdown joins requests/work before closing the database and trust observer.

## States & lifecycle

### Cleanup marker columns on `session_runtime_status`

The service owns no tables; the schema lives in `internal/storage`
(`postgresql_schema.go`). Cleanup state is four columns on
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

### Due-scan predicate (per workspace, per tick)

`dueCleanupSessionsTx` reads candidates without row locks, ordered by
`cleanup_after ASC, session_id ASC`, bounded to a batch (default 100). The
claim then takes each candidate's Session runtime arbitration lock and stamps
the marker through a guarded UPDATE that repeats the predicate:

```
status = 'idle'
AND cleanup_after <= now()
AND cleanup_job_id IS NULL
AND binding_id IS NOT NULL
```

Each predicate term is load-bearing. Job Runner finalize nulls **both**
`binding_id` and `cleanup_after`: either alone unmatches the row, and
together they guarantee an already-cleaned session never re-matches and
never re-enqueues a no-op job on every tick. The `binding_id IS NOT NULL`
guard additionally keeps the scheduler from claiming a session whose
binding is already gone.

The per-tick scan is covered by a partial index in `internal/storage`
(`postgresql_schema.go`, `idx_session_runtime_status_cleanup_due` on
`session_runtime_status(workspace_id, cleanup_after, cleanup_job_id)
WHERE status = 'idle' AND binding_id IS NOT NULL`). It mirrors this
predicate term for term; any change to the predicate must move the index
in lockstep or the scan loses coverage.

### Tick flow

| Step | Actor | Effect |
|------|-------|--------|
| 1 | Bridge or Job Runner idle write | stamps `cleanup_after` when a reusable run finishes |
| 2 | scheduler `ClaimDueAcrossWorkspaces` | enumerates the `workspaces` catalog; runs the due-scan once per workspace, each in its own transaction |
| 3 | scheduler `markCleanupEnqueuedTx` | mints a fresh `cleanup_job_id`, stamps `cleanup_enqueued_at`, resets stale `cleanup_claimed_at`; guarded re-check of the due predicate |
| 4 | scheduler `queue.EnqueueBatchTx` | writes one `queue_jobs(kind = cleanup_session)` row in the session partition, deduped by the minted `cleanup_job_id` |
| 5 | Job Runner | leases the job, re-validates the fences, settles Runtime waits, and finalizes the Runtime binding |

An error in one workspace aborts the rest of that tick; the next tick
retries. Batch bound is per workspace, so a tick's total work scales with
tenant count.

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

### Workspace fan-out (`workspace_fanout.go`)

`ClaimDueAcrossWorkspaces` is the orchestration boundary, wired to two
consumer-side interfaces:

- `WorkspaceLister.ListIDs(ctx) ([]workspace.ID, error)` — tenant discovery.
- `CleanupClaimer.ClaimDue(ctx, ClaimDueRequest) ([]ClaimedCleanupJob, error)` — the per-workspace claim.

Invariants a replacement must preserve: every discovered workspace is
visited once per tick; each `ClaimDue` runs in its own transaction; an
empty `workspace_id` is a `ValidationError`; a per-workspace error aborts
the remaining fan-out (fail-closed, retry next tick). Conformance:
`TestClaimDueAcrossWorkspacesVisitsEveryDiscoveredWorkspace`.

### Due-scan and enqueue (`scheduler.go`)

`Scheduler.ClaimDue` is the claim seam. Contract: it reads
`session_runtime_status` for one workspace, and writes exactly two things — the
cleanup marker columns and one `queue_jobs` row per claimed session via
`queue.EnqueueBatchTx`. Invariants a replacement must preserve: the exact due
predicate above; the marker stamp is re-guarded against the same predicate
before enqueue (`markCleanupEnqueuedTx` returns `false` → skip, no job);
the queue job is deduped by the minted `cleanup_job_id`; the batch is
bounded, and each Session is claimed under its runtime arbitration lock. It
never calls Runtime Pod, Bridge, Sandbox Service, or the sandbox provider, and
never touches durable history (`session_threads`, `session_events`, `session_messages`).
Conformance: `TestSchedulerClaimsOnlyDueBoundIdleRowsAndEnqueuesCleanupJobs`,
`TestCleanupWorkloadStaysWithinSchedulerBoundary`.

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

`SchedulerMetrics` accumulates three OpenMetrics counters —
`tetral_cleanup_claim_due_runs_total`, `tetral_cleanup_jobs_claimed_total`,
`tetral_cleanup_claim_due_duration_ms_total` — exposed through
`SchedulerMetrics.Collector()`. `MetricsExporter` /
`OpenMetricsHTTPExporter` optionally POST them to
`TETRAL_CLEANUP_METRICS_EXPORT_URL`. Invariants a replacement must
preserve: counters carry no per-scope labels; the series names are stable;
export is off by default and the shipped `k8s/networkpolicy.yaml` (postgres
+ DNS egress only) blocks it unless deployment opens the path. Conformance:
`TestSchedulerMetricsCollectorReportsSafeCounters`,
`TestOpenMetricsHTTPExporterPushesSchedulerSeriesWithoutScopeLabels`.

### Configuration (`config.go`)

`ConfigFromEnv` reads three env vars: `TETRAL_CLEANUP_CLAIM_LIMIT`
(positive integer, default 100), `TETRAL_CLEANUP_METRICS_EXPORT_URL`
(HTTP(S), no credentials, else startup error), and
`TETRAL_CLEANUP_METRICS_EXPORT_TIMEOUT` (positive duration, default 2s).
The tick schedule lives in `k8s/cronjob.yaml`; batch size is config; the
TTL delay is `runtimecontrol.IdleCleanupDelay`. Invalid positive-integer / URL /
duration settings are startup errors (`workload.NewConfigError`).
Conformance: `TestConfigFromEnvValidatesMetricsExporter`.

## Testing guide

| Suite | Proves |
|-------|--------|
| `scheduler_test.go` | the due predicate selects only due, bound, idle rows; markers stamped and one deduped job enqueued; the workload stays within its read/write boundary; metrics counters stay safe |
| `workspace_fanout_test.go` | every discovered workspace is visited once per tick |
| `metrics_exporter_test.go` | exported series carry no scope labels; config validation rejects a bad exporter endpoint |
| `integration/runtime_session_cleanup_test.go` | the executor contract this scheduler depends on: role-blind tree fence and reschedule-at-both-points, stale-job ACK, Runtime settlement before finalization, stream-fence input rejection |

If a PR changes the due predicate, the workspace fan-out, the marker
writes, the enqueue shape, or the metrics/config surface in this folder,
it updates the matching section here. A due-predicate change must also
move the `idx_session_runtime_status_cleanup_due` partial index in
`internal/storage` in lockstep so the scan stays covered. If it changes the tree fence,
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
`claim_due` records the existing successful per-workspace claim boundary;
`claim_due_across_workspaces` records the whole job's enumeration and claims,
including error, cancellation and deadline failure. Workspace IDs never
become labels. The three existing counters preserve their population and
units. The full job is observed once before the final export; a later export
failure is separate from claim failure, and a CronJob registry does not persist
across invocations. Deployment must collect each invocation's export rather
than treating its reset counts as a resident counter.

Fixed seconds buckets and replica percentile queries follow the
[shared operation duration contract](../../internal/workload/README.md#operation-durations).
