# job-runner

## Responsibilities

Job Runner is an independent Deployment and ServiceAccount that consumes
`runtime_input`, `runtime_config_update`, `cleanup_session` and
`session_delete_cleanup` Queue jobs. It declares and replaces Session Runtime
bindings, delivers commands directly to their bound Pod, reconciles lost Pod
custody, and finalizes hot-state cleanup. It has no inbound business gRPC
surface or TokenReview receiver. [Bridge](../bridge/README.md) owns Runtime
RPC acceptance, receipts and durable context reads.

## Process lifecycle

`cmd/job-runner` owns its database pool, Queue client, Kubernetes visibility
watches, Queue wake listener, polling workers and outbound Runtime/MCP clients.
Startup validates configuration, schema, runtime role and inbox capacities
before opening listeners or business clients. Cancellation joins polling and
Queue-listener workers, then visibility watches (including replacements),
before closing the process pool. Bridge cancellation does not stop these
resources. Health readiness depends on synchronized Pod/EndpointSlice visibility.

The process uses shared `internal/workload` diagnostics: `info` by default;
`TETRAL_LOG_LEVEL` accepts `debug`, `info`, `warn` or `error`. Metadata and bounded
record, limiter and sink controls are read once at boot; changes require a
process restart. The owned sink closes after business resources with a bounded
shutdown budget. Diagnostic backpressure never becomes Queue or receipt
custody. Health metrics expose diagnostic drop/failure counts. Secrets, token
contents and connection credentials are excluded from diagnostic records.

`TETRAL_DB_MAX_OPEN_CONNS` defaults to 20 and must be at least 2. One connection
belongs to the Queue listener; at least one remains for business transactions.
Queue insertion and wakeup share a transaction. The notification payload for
Job Runner work is the transient wire value `bridge`, which Queue producers and
this listener share through `ConsumerClassJobRunner`. Hints carry no work:
reconnect triggers catch-up and bounded polling remains the fallback for a lost
or coalesced hint.

Configuration and wire identifiers keep their established names when process
ownership moves: boot keys use `TETRAL_BRIDGE_JOB_RUNNER_*` names and the
default Queue lease owner is `bridge-job-runner`. Diagnostics name the owner:
records carry `service.name` `job-runner`, the poll and delivery-attempt events
use the `job_runner.` prefix, and transaction labels use `jobrunner.*`. Runtime
and MCP outbound calls use projected internal gRPC audience credentials.
Kubernetes visibility uses its separate Kubernetes API audience credential.
Runner receives Blob credentials for Session cleanup and no Sandbox provider
credentials.

## Binding, repair and cleanup

### The binding fence and pod visibility

The Job Runner decides which pod owns a session — claim, verify, replace
under a session-scoped lock. It classifies the bound pod through
`internal/kubernetes` visibility and splits **proven gone** from **merely
unavailable**:

| `BindingVisibility*` | Meaning | Disposition |
| --- | --- | --- |
| `Reusable` | Live, ready, same UID/IP | Deliver |
| `Absent` / `Deleted` / `UIDChanged` / `IPChanged` | Proven gone | Repair, then replace the binding |
| `SnapshotNotReady` / `NotReady` / `NotServing` / `Terminating` | Merely unavailable | Retry; never finalize |

For `runtime_input` the runner reconciles referenced events first — all
already processed → stale with no command; superseded by a processed
interrupt fence → never delivered — then upserts the delivery-inbox row and
sends the typed command addressed to the bound pod
**directly** (never through a load-balanced service, which could livelock on
identity rejection). At each interrupt claim it also cancels older same-thread
pending message jobs below the interrupt fence — inputs the user retracted by
interrupting.

### Repair (Job Runner, on proven-gone)

Each workspace pass performs pod-loss reconciliation before Queue leasing. It
freezes the active binding census
in a read-only repeatable-read transaction, takes one watcher snapshot after
the database snapshot exists, and keyset-pages binding identities in batches of
32. The read transaction closes before any candidate mutation. Running Runtime
status or a rescheduling Session admits proactive closeout; an idle retained
binding remains for the next input to replace through the same Session lock and
binding-generation fence. Errors are isolated across repair and Queue phases,
with runner cancellation as the only early stop.

Under the Session arbitration owner and binding fence, durable evidence is
reconstructed per Thread. Threads already owned by an exact interrupt keep
their open Request and control custody for the replacement interrupt owner;
unaffected Threads with unfinished work are repaired independently. For each
included repair scope, unfinished
request spans close as errors (`runtime_pod_lost`, original
`model_request_id` reused). Nonterminal `pending` or `resolving` tool routes
remain durable, including reconstructible `resolving` / `allow` routes, with
the old Runtime scope fenced. The replacement continues
the same tool identity and eventually commits exactly one terminal result;
loss repair does not synthesize a result for these retained routes. Other
orphaned public tool uses receive exactly one terminal result
(`spawn_agent` / `send_message` settle delivery-aware by their
`delivery_id`, keyed on the durable inter-agent delivery state); a delivered-
but-uncommitted input replays from the inbox; pending waits owned by the lost
binding are cancelled; every included scope the loss left unsettled
resolves to idle with its `session.error` — **except** a scope whose committed
Request End already owns provider reschedule, where repair retires only the
lost residency row and preserves the accepted retry facts, and an interrupted-then-
lost scope, which settles quietly as `end_turn` with no error because the
user's own processed `user.interrupt` (the thread's highest-sequence committed
input) is the durable proof the stop was requested; the retry budget resets;
only then is the stale Runtime binding released. Sandbox lifecycle is
independent of Runtime Pod loss. Provider text is never reconstructed
— only ledgers are repaired.

### Cleanup order (hot Runtime state only)

1. Runtime Pod accepts `CleanupSession` and clears its hot state (or is proven gone), proving no active run can still resolve a wait;
2. durable `approval` waits and their recoverable Sandbox execution records remain for a later confirmation; other `pending` external waits and unowned Sandbox executions expire by terminal projection;
3. the Runtime binding and `session_runtime_status` finalize after those settlements are durable;
4. durable `session_threads`, `session_events`, `session_messages`, Sandbox bindings, and provider resources are never deleted by TTL cleanup.

**The tree fence.** The cleanup alarm is a hint and may be stale (armed while
children still run); the **claim** carries the proof. Inside the claim
transaction — which holds the `session_runtime_status` row lock and which
finalize re-executes — the claim additionally proves no `session_threads` row
is busy (`running` or `rescheduling`; `idle`, `requires_action`,
`closed_for_runtime`, `terminated`, `failed` are quiescent, and
`requires_action` is expressly quiescent — an approval may wait days on a
durable, wake-fenced confirmation). A busy result **reschedules at both
enforcement points** (clearing `cleanup_job_id`, `cleanup_claimed_at`,
`cleanup_enqueued_at` and pushing `cleanup_after` forward); a bare stale result at
finalize would strand `cleanup_job_id` set on a past-due row forever. The
pod-side eviction refusal (`session_busy` while any run slot is active or any
thread's accepted-input queue is non-empty) remains the final authority.

**Delete exception.** The Session-delete transaction is the producer of the
durable `sandbox_release` operation. The `session_delete_cleanup` branch clears
hot Runtime custody, joins that release operation idempotently, and waits for
Sandbox Service and Sandbox Queue custody to close before deleting private
Sandbox rows. Job Runner never performs the provider call.

## Delivery and custody

### Delivery and durable wake machinery

- **Contract.** Message producers commit `session_runtime_inbox` and Queue
  custody beside their source facts. The Job Runner (`job_runner.go`,
  `runtime_delivery.go`) binds that existing custody, sends typed commands to
  the bound pod, and maps replies onto queue transitions.
  Child completion returns to the parent through one
  `agent.thread_message_sent` envelope written in the child's settling
  transaction (`completion_mail.go`), with a durable agent-mail wake enqueued
  in the same transaction.
- **Lifecycle.** Completion is decided by an event discriminator, never by stop
  reason alone: a clean `end_turn` mails a completed envelope; `retries_
  exhausted`, an `end_turn` carrying a terminal `session.error`, and a child-
  scoped termination mail an errored envelope; a processed `user.interrupt`,
  `requires_action`, reviewer settlements, and pod-loss repairs mail nothing.
- **Invariants a replacement must preserve.** There is no settled-without-mail,
  mail-without-Inbox, or Inbox-without-Queue birth state. Completion replay
  joins the same durable identities; delivery never scans the event ledger to
  reconstruct custody. Delivery targets the bound pod directly, never a
  load-balanced service. A leased interrupt owns only its target Thread lane:
  later inputs still commit their Event, Inbox, and Queue custody, sibling
  Threads continue, and no later target-Thread job is leased until the
  interrupt's atomic Request End,
  Tool settlements, and receipt are durable and its Queue job is acknowledged.
  Runtime acceptance alone never acknowledges an interrupt. Exact receipt
  replay acknowledges without another Runtime call; a 30-second interrupt send
  timeout covers command admission through Tool cancellation/join, durable
  closeout writes, and receipt return. A caller timeout remains outcome-unknown
  and retains the same barrier and attempt identity. Proven pod loss may
  transfer that identity only while attempts remain. At exhaustion, the exact
  live Queue lease owner replays a receipt or terminalizes the target Thread;
  only main-Thread exhaustion terminalizes the Session. Neither path sends the
  interrupt to a replacement Runtime. For inputs other than queued user
  messages, initial MCP manifest capture gives each list call a fixed
  180-second deadline; a queued user message instead spends its per-input
  discovery budget (see [Manifest discovery](#manifest-discovery)), where each
  list call receives only the remainder of one shared 120-second deadline.
  Either bound limits one Job Runner worker slot per stalled call. Expiry ends
  the Runner wait and reaches the connector as the gRPC deadline;
  connector-side cancellation follows the
  [Gateway discovery contract](../gateway/README.md#discovery-and-manifest-delivery).
- **Agent-mail custody.** Child creation atomically persists the child, context
  prefix, first mail, Inbox row, Queue job, and spawn receipt. First and later
  mail then share one delivery path: `CommitInputs` makes the Message durable,
  Runtime admits that input into hot state, Bridge records the agent-mail-only
  accepted transition, and JobRunner acknowledges the exact Queue lease.
  Request Start remains a Runtime declaration and is never delivery or ACK
  authority. If the accepted Runtime is lost before Request Start, the generic
  pod-loss owner returns the same durable input identity to Queue custody. At
  exhaustion, a finalization-only lease performs no Runtime call and atomically
  fails only the target subagent, settles the exact Inbox and Queue custody, and
  emits one existing completion notification to its parent.
- **Conformance.** Runner-owned:
  [job_runner_test.go](job_runner_test.go),
  [runtime_delivery_test.go](runtime_delivery_test.go),
  [runtime_delivery_store_test.go](runtime_delivery_store_test.go),
  [runtime_delivery_exhaustion_test.go](runtime_delivery_exhaustion_test.go),
  [completion_mail_test.go](completion_mail_test.go),
  [completion_mail_delivery_test.go](completion_mail_delivery_test.go).
  Cross-owner compositions:
  [completion_mail_test.go](../../integration/completion_mail_test.go),
  [completion_mail_delivery_test.go](../../integration/completion_mail_delivery_test.go),
  [runtime_pod_lost_delivery_repair_test.go](../../integration/runtime_pod_lost_delivery_repair_test.go).

## Manifest discovery

Initial and restoration discovery is Runner-owned; hot change acceptance is
Bridge-owned. Both use `internal/mcpmanifest` canonicalization and acceptance
inside their caller-owned transaction. External connector I/O occurs outside
that transaction. The connector endpoint is the independent `mcp-connector`
workload; no provider gateway business package is imported.

- **Discovery lifecycle.** For a configured server without a usable complete
  manifest, one queued user input owns at most 3 whole-discovery attempts and a shared
  120-second deadline. Job Runner reserves attempts in `session_runtime_inbox`
  before external I/O; process restart and Queue lease replay cannot replenish
  them. Once delivery may have reached Runtime, existing custody reconciliation
  applies instead of retroactively failing that input for discovery. PostgreSQL migration V3 adds these counters, deadline and safe diagnostic
  fields without changing the baseline migration. Connector authentication
  refresh consumes the same wall-clock budget. Validation and the final 256 KiB
  canonical cap are part of discovery acceptance. All discovery errors, including
  internal/protocol failures, consume this finite budget.
  After exhaustion, one transaction records unready state, marks the input
  processed/dead-lettered, and emits one safe `session.error` to the application.
  Operator logs identify the input, server, attempt and failure class. This input
  never reaches Runtime/model execution. An otherwise inactive main Session
  emits idle; other running threads and control operations remain intact. The
  error's `retry_status: exhausted` describes the input discovery budget,
  including credential failures; it is distinct from a connector operation's
  terminal status. While a configured directory remains unavailable, each new
  user message can exhaust its own budget and be rejected before model execution.
  The idle event may therefore have no preceding running event for that input;
  it reports settlement, not proof that a model request ran. See the
  [public event lifecycle](../event-stream/README.md#discovery-failure-before-model-execution).
  The Session is not terminated. A distinct later user input may retry and restore
  `unready -> ready` with a higher generation; a matching etag does not prevent
  recovery. Inputs that performed discovery apply the accepted manifest through
  Runtime config control before `AcceptInput`. A cold Pod's `no_residency` result
  defers installation to its existing `LoadContext` path. Busy/rejected config
  application does not send the input; Queue delivery retry reuses the durable
  manifest and discovery budget. The independent config carrier remains durable.
  Existing usable manifests are reused on cold restoration. Stop/interrupt does
  not wait for discovery. Discovery retry never retries a tool's external write.
## Visibility

### Kubernetes pod visibility (engine-root `internal/kubernetes`)

- **Contract.** `internal/kubernetes` and `internal/internalgrpc/auth` are
  engine-root shared packages; Job Runner consumes them but does not own them.
  `internal/kubernetes` owns Pod and EndpointSlice visibility clients
  (`VisibilityClient`: list/watch) and a `WatcherCache` that the Job Runner
  consumes via `BindingVisibilitySnapshot`. It holds no control-plane
  ownership and receives explicit inputs; `internal/internalgrpc/auth` may
  import the Kubernetes client libraries only for TokenReview authentication.
- **Lifecycle.** The runner's readiness depends on the cache being synced;
  `SyncAndWatch` primes it and keeps it current. The snapshot classifies the
  bound pod into the `BindingVisibility*` set that drives the proven-gone vs
  merely-unavailable split (see the binding fence table).
- **Invariants a replacement must preserve.** Visibility is read-only — it never
  mutates pods or bindings; proven-gone must be distinguishable from merely-
  unavailable, because only the former is allowed to replace a binding; a
  not-ready snapshot must retry, never finalize.
- **Conformance.** Cross-owner loss recovery:
  [runtime_pod_lost_store_test.go](../../integration/runtime_pod_lost_store_test.go).
  Engine-root (under
  `internal/kubernetes/`): `visibility_client_test.go`, `cache_test.go`
  (covering the `WatcherCache` type in `watcher_cache.go`),
  `static_visibility_test.go`.

## Shared durable boundaries

`internal/runtimeconfig` interprets the Session-pinned agent version's system
configuration, installed tool families/policy and attached memory resources.
`internal/runtimecontrol` owns reusable locks, fences, receipts and atomic
settlement projections. Runner projects its owned RuntimeJob into explicit
custody DTOs; Queue lease identity remains Runner-owned. Helpers receive the
caller's transaction so context mutations and exact Queue ACK/NACK remain
atomic. Shared packages never import Bridge or Runner business code.

## Testing

Owner-local unit and PostgreSQL suites are in this directory; real Bridge and
Runner compositions are in `integration/`. Runtime/Bun and Blob fixtures retain
their declared dependencies. The command tests enforce schema/capacity failure
before listeners and clients; Kubernetes lifecycle tests prove watch workers
join before Stop returns. Run repository `make test-affected` for the declared
owner closure and managed dependencies; direct database tests require an
administrative test DSN and create restricted private clones.
