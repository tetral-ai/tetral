# bridge

## Responsibilities

Agent Runtime Bridge is the runtime's durable half. Every fact the agent
loop needs persisted crosses exactly one boundary — a Bridge RPC — and
every durable-write RPC is one PostgreSQL transaction; read-only resolvers
are the exception. Sandbox execution crosses two distinct boundaries:
`AcceptSandboxExecution` atomically records the execution and its refs-only
Queue job, while `AwaitSandboxExecution` only reads that durable execution
until Sandbox Service stores a terminal result. Acceptance validates the exact
durable Tool Use event and its immutable Tool Call in the shared assistant
projection; approval input comes from the event rather than the bounded message
preview. Sandbox Service stores the terminal refs-only result and its internal
digest together. `AwaitSandboxExecution` returns only the executor result;
`SettleToolResult` selects the stored row by durable Tool Use and Bridge reads
and validates its own digest before consuming staged custody. Bridge never performs
the provider call while a Runtime RPC is open. Only the MCP `Claim`/`Commit` pair
uses a connector-side leased reservation before its refs-only result commit.
The Runtime Pod
holds hot state only and mutates it after the Bridge ACK; nothing the pod
holds is ever the source of truth, so a lost pod loses no durable fact.
Tenant isolation is structural: every row carries `workspace_id` in its
primary key and every caller presents a signed principal binding that Bridge
verifies before any read or mutation. Bridge runs as one Kubernetes pod with
two containers — `bridge-api` (`cmd/bridge-api`) serving the Runtime RPC
surface, and `job-runner` (`cmd/job-runner`) consuming runtime-facing queue
jobs and owning the binding fence — sharing one trust boundary but with
credentials split per container: `bridge-api` receives Blob credentials for
attachment reads, while `job-runner` receives them for Session cleanup. Manifests
and tests keep that split inspectable. Bridge
owns no provider lowering, no model calls, no sandbox lifecycle, and no
public HTTP; it never deletes durable history.

## States & lifecycle

### Two containers

| Container | Command | Serves / consumes | Credentials |
| --- | --- | --- | --- |
| `bridge-api` | `/usr/local/bin/bridge-api` | The Runtime→Bridge RPC surface (below), plus Gateway-facing read-only attachment resolvers | Blob credentials for the read-only attachment surface, scoped to this container only |
| `job-runner` | `/usr/local/bin/job-runner` | Queue kinds `runtime_input`, `runtime_config_update`, `cleanup_session`, `session_delete_cleanup`; owns delivery, the binding fence, repair, cleanup | Projected Gateway credential for initial MCP manifest discovery and Blob credentials for Session-owned attachment cleanup; no Sandbox-provider credentials |

Both containers run in one pod, so ServiceAccount and NetworkPolicy treat
Bridge as a single trust boundary; the per-container credential split limits
accidental exposure, not network identity.

Queue insertion and wakeup share the enqueue transaction: each newly inserted
job emits a PostgreSQL `NOTIFY` containing only its consumer class. The
`job-runner` owns one reconnecting `LISTEN` connection for the Bridge class and
turns matching notifications into local wake hints; it then leases durable jobs
from PostgreSQL normally. Notifications carry no work and are not correctness
state. A listener connection or reconnect also triggers a catch-up poll, and
the bounded polling loop remains the fallback if a notification is coalesced or
lost.

### The Bridge API RPC surface

Grouped by what each call settles. Every durable-write RPC carries a stable
idempotency identity: replaying it with the identical payload returns the
stored ACK; the same identity with a divergent payload is a fatal conflict.
The Bridge API process admits 64 MiB messages for complete `WriteEvent`
declarations and `LoadContext` responses. Attachment clients retain their
separate 32 MiB transport fuse and existing per-attachment semantic limits.

| Group | RPCs | Settles |
| --- | --- | --- |
| Context | `LoadContext` | Cold-start one thread from current durable facts: ordered Messages, Request/Tool Events, direct internal-repair Message/Event references, unresolved pending tool waits, per-server MCP manifests, and pending media. Runtime reconstructs its checkpoint from these direct identities; Bridge does not project Message mutation history. |
| Input | `CommitInputs`, `CommitTaskNotificationResult` | User / inter-agent / internal-reviewer inputs stamp and project in one transaction. Tool confirmation settles the named pending-tool state. Interrupt intent makes Bridge census every unfinished durable Tool Use, write and consume one honest terminal conversation result per target, and return only minimal hot-state projections; background-task settlement remains independently Sandbox-owned and never creates a second public Tool Result. |
| Events | `WriteEvent`, `CommitInternalToolRepair` | One non-result semantic event plus its projection in one transaction; a public Tool Use may carry the anchored prefix of completed reasoning parts. The event-less invalid-tool repair row is atomic and rehydratable. |
| Settlement | `SettleToolResult`, `WriteRequestEnd`, `FinishIdle`, `CommitRuntimeTermination` | `SettleToolResult` derives one public result Event and terminal Tool projection from the named durable Tool Use; its closed result is only committed, duplicate, or stale. Request End writes usage and cumulative projection in one transaction. An ordinary successful end may append only its final not-yet-durable Assistant members before sealing the existing model-request projection; retryable failure seals only content already durable and carries the reschedule leg. An interrupt during an open provider request joins its separately owned `CommitInputs` envelope. The reschedule leg increments the durable per-thread retry budget and writes rescheduled status only when the ceiling admits — at most one terminal end per model request, a losing close yields. `FinishIdle` ensures or joins Sandbox-owned output capture, waits without a database transaction, then atomically adopts its staged Blob references with idle status. `CommitRuntimeTermination` validates the open durable turn and stores only deterministic terminal declarations. A child failure remains local and, when the child is a sub-agent, commits its completion mail; a Main failure atomically closes every non-terminal sibling request and Tool obligation before terminating the Session, without mailing the terminal Main Thread. |
| Children | `CreateSubagentThread`, `EnsureApprovalReviewerTrunk`, `EnsureApprovalReviewerSidecar`, `AdmitApprovalReviewInput`, `ResolveChildThread`, `ListChildThreads`, `DeliverInterAgentMail`, `ReadAgentMail`, `AdmitChildInterrupt`, `AwaitChildInterrupt`, `CloseChildControl`, `CloseApprovalReviewer`, `MarkChildThreadActive` | Bridge-owned child identity and context prefix; accepted reviewer Inbox custody; durable sender-time mail delivery plus target-owned text reads; durable subtree interrupt admission and completion; operation-specific child control and reviewer lifecycle marks |
| Tools | `AcceptSandboxExecution`, `AwaitSandboxExecution`, `ReadCommandResult`, `SendCommandInput`, `CancelCommand`, `RunMemory` | Atomic Sandbox execution handoff and independent terminal-result read; background-command follow-ups whose operation kind, task, and executor input are selected from the durable Tool declaration; durable memory writes with content-match conflict checks |
| Attachment resolution (Gateway, read-only, scope-validated) | `ResolveTransientAttachment`, `ResolveFileAttachmentMetadata`, `ReadFileAttachmentChunk` | Stored attachment bytes for provider-request lowering; batch file-backed metadata preflight with zero blob reads; bounded offset-addressed file-backed chunk reads (≤ 8 MiB, idempotent by construction) |
| MCP | `McpManifestChanged`, `ClaimMcpToolResult`, `CommitMcpToolResult`, `RelinquishMcpToolResult` | Manifest capture-before-deliver and runtime redelivery; leased pre-execution reservation, refs-only durable result commit, and exact-claim deterministic relinquish |
| Binding | `RefreshRuntimeBindingToken` | Re-mints a thread's gateway token from live binding state under the locking binding fence, so a superseded pod never gets a fresh token |

Callers are checked twice before any durable mutation: workload identity
first (ServiceAccount, namespace, audience, expiry), then the binding fence —
the request's workspace, session, binding id and generation, and pod UID must
match the live binding row (bindings are session-scoped; the thread scope
rides the request and the per-thread gateway token), or the call is rejected
as a retryable stale-binding error.

### Idle reasons

| Reason | Written by | Meaning |
| --- | --- | --- |
| `end_turn` | Runtime `FinishIdle` | Clean turn end |
| `requires_action` | Runtime `FinishIdle` | Blocking approval / external wait; carries the blocking public event ids |
| `retries_exhausted` | Runtime `FinishIdle`, or pod-loss repair | The reschedule budget is spent or a reschedule was Bridge-denied; the turn is dead but the session stays resumable |

Output capture runs before the idle write through durable Sandbox-owned work.
No Sandbox means an empty capture; per-entry skips and scan/normalization
failures stage a best-effort result. Provider, Blob, lock, quota, index, or
persistence failure fails `FinishIdle`, whose retry rejoins or advances the
durable capture generation. The final transaction adopts staged Blob custody,
updates the file index, rearms completion mail, and records idle together.

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

Under the session mutation lock and binding fence, from durable evidence
alone: every Thread sharing the lost Session binding that has unfinished work
is included regardless of its current Thread status; unfinished
request spans close as errors (`runtime_pod_lost`, original
`model_request_id` reused); each orphaned public tool use gets exactly one
terminal result (`spawn_agent` / `send_message` settle delivery-aware by their
`delivery_id`, keyed on the durable inter-agent delivery state); a delivered-
but-uncommitted input replays from the inbox; pending waits owned by the lost
binding are cancelled; every included scope the loss left unsettled
resolves to idle with its `session.error` — **except** an interrupted-then-
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
Sandbox rows. Bridge never performs the provider call.

### Closeout failure dispositions

A run fiber that dies without reaching a settlement write still routes through
a durable closeout — a terminal `session.error` then the idle settlement, so
the child-completion discriminator classifies it errored, never a false
completion. When the closeout write itself fails, the governing asymmetry is:
a **lost** closeout (released when a retry would have landed it) is
unrecoverable; a **loud** retry loop is visible and curable. Release is
therefore only ever sentinel-gated (`closeout_sentinel.go`), and retry is the
default:

| Disposition | Trigger | Bridge behavior |
| --- | --- | --- |
| Retryable (default) | Any closeout-write failure with no release sentinel | The pod retries the same immutable operation (1 s doubling to 60 s cap) until it commits, replays as duplicate, reaches a typed terminal condition, or shutdown supersedes it |
| Superseded (`scope_superseded`) | Custody has demonstrably ended: binding row absent or replaced, session deleted, caller pod-UID mismatch, or a terminal target | Bridge maps the condition to that RPC's closed `stale` result; the pod releases without writing |
| Unrepairable (`closeout_unrepairable`) | A durable validation or target failure means the same operation cannot succeed | Bridge preserves the typed gRPC failure; the pod terminates retry and emits a bounded redacted record |

Each closeout RPC returns its own closed result union. A committed or duplicate
variant contains only Bridge-created facts that its caller immediately uses;
it never echoes the operation identity or submitted declaration payload.

## Seams

Each replaceable boundary states its contract, lifecycle, the invariants a
replacement must preserve, and the conformance suites that prove it.

### Runtime declaration boundary

Runtime owns Agent business content and sends operation-specific, bounded
context deltas. Bridge validates caller scope, durable target, custody,
fencing, idempotency, bounds, projection safety, and transaction ownership; it
does not validate a Runtime message state machine or accept database message,
part, status, origin, or timestamp fields. PostgreSQL assigns durable ordering
and audit metadata outside the stored provider-visible context.

`CommitInputs`, `WriteEvent`, `SettleToolResult`, `WriteRequestEnd`, repair,
compaction, idle, and termination retain separate request and result types.
There is no generic declaration result. Each successful hot-path application
uses the immutable request plus only newly assigned facts returned by that
operation. Cold recovery reconstructs sealed context, the open request draft,
and active lifecycle facts directly from durable rows.

### Event-writer and Tool-settlement boundaries

- **Contract.** `WriteEvent` persists one non-result `session_events` row plus an
  event-specific Assistant append into `session_messages` in one transaction. Usage,
  transport metadata, raw provider payloads, request ids, and raw attachment
  bytes never project. Opening or resolving an external wait updates
  `session_pending_tool_uses` in the same transaction: the trigger is the tool
  event's `evaluated_permission` — `ask` upserts the approval's
  `status='pending'` row (`applyToolEventBookkeepingTx` in `bridge_api_events.go`),
  while `allow` and `deny` write no row. A public tool event may
  carry an anchored reasoning prefix. `SettleToolResult` is the sole ordinary
  Tool-result writer: Runtime supplies the durable Tool target and final bounded
  provider-visible outcome, while Bridge resolves the immutable Tool Call from
  its direct Tool Event/execution facts and appends a separate terminal Tool
  Result paired by call id. The Tool Call is never rewritten. Bridge derives the
  public result Event, Tool family, and any accepted Sandbox result digest from
  those direct facts. Web usage is part of the bounded outcome and increments
  `sessions.usage` exactly once. Neither digest nor settlement payload is
  returned to Runtime.
- **Lifecycle.** `WriteEvent` is idempotency-keyed by `runtime_write_id`; the attached
  reasoning set folds into the request hash. `SettleToolResult` hashes its
  bounded outcome, including optional web usage, under the Tool Use identity. Runtime updates
  hot state from the immutable declaration and operation-specific result. An unknown transport
  result retries the same frozen declaration and receives the duplicate
  variant without reconstructing content. `SettleToolResult` is keyed by the
  durable Tool Use and returns only `committed`, `duplicate`, or `stale`; cold
  recovery reads the resulting durable Event and projection directly.
- **Invariants a replacement must preserve.** Event and message declaration are atomic;
  the declaration class is whitelisted by event type; a replay is byte-identical or a
  fatal conflict; no double-count on replay; per-request stable-reasoning
  byte/part budgets roll back the enclosing transaction when validation fails.
- **Conformance.** `bridge_api_events_test.go`.

### Settlement transaction

- **Contract.** `WriteRequestEnd`, `FinishIdle`, and `CommitRuntimeTermination`
  are the request/model-usage and idle/terminal writers. `WriteRequestEnd`
  inserts the request-end span, inserts request usage detail idempotently, and
  updates `sessions.usage` only when the detail insert wins. It may append the
  request's final not-yet-durable Assistant members and then seals the existing
  assistant projection without replacing its owning event. Its closed result
  distinguishes ordinary, rescheduled, and compacted commits and returns only
  Bridge-assigned facts with an immediate caller.
  A no-content end still commits the request boundary so a stale custodian
  cannot continue merely because there is no assistant projection.
  An interrupt received while the request is open carries only its admitted
  source envelope. Bridge derives and terminalizes the locked unfinished-Tool
  census in the nested input transaction. The response carries only the
  operation-specific interrupt Tool outcomes needed by the caller.
  The reschedule leg increments `session_turn_retries` and writes rescheduled
  status only when the ceiling admits. `FinishIdle` adopts a Sandbox-staged
  output-capture generation into `session_output_captures` and the file tables,
  then writes `session.status_idle` in the same transaction.
  `CommitRuntimeTermination` accepts only the live loop's current-thread
  declarations under the open durable-turn identity. A sub-agent declaration
  persists local failure and completion mail; other child roles persist local
  failure without mail. A Main declaration additionally
  closes every non-terminal sibling's open request and Tool Use from durable
  evidence, marks those children terminated, and terminates the Session in the
  same transaction. The response returns only the declaring Thread's
  database-assigned stamps.
- **Lifecycle.** At most one terminal end per `model_request_id` (pod close and
  repair close both check inside the transaction, serialized on the start-row
  lock; a divergent loser is rejected and cold-recovers the durable winner). `FinishIdle` and
  `CommitRuntimeTermination` are idempotent on the database-named durable turn:
  declaration-and-close or nothing. A live child loop supplies its errored
  completion-return envelope in that transaction. A Main terminal declaration
  derives sibling closeout only from already durable requests and Tool Uses;
  it never invents sibling model content or completion mail. Once a custodian
  is mechanically gone, the pod-loss and cleanup repair paths own the
  postmortem closeout.
- **Invariants a replacement must preserve.** The request-end transaction is
  the sole provider/model-usage writer (web `server_tool_use` counters arrive
  on `SettleToolResult`, not here); output capture scan failures are best-effort while
  staged-custody and persistence failures prevent idle; cumulative usage never double-counts; current-thread live closeout is
  atomic, and postmortem writers remain disjoint from live loop authorship.
- **Conformance.** `bridge_api_settlement_test.go`, Runtime termination tests,
  Sandbox output-capture runner/store tests, and `closeout_sentinel_test.go`.

### Incremental Assistant members and stable reasoning

- **Contract.** `WriteEvent` appends only newly completed Assistant members.
  A text or Tool Use event may carry preceding reasoning or step-boundary
  members in the same ordered append; later writes never resend them.
  `SettleToolResult` independently names one durable Tool Use and appends its
  separate terminal Tool Result without mutating the earlier Tool Call. A
  successful `WriteRequestEnd` may append an otherwise
  unanchored reasoning/step suffix before sealing the request. Runtime declares
  each part identity; Bridge assigns durable message and event identities,
  sequences, and timestamps. Completed
  conversation results contain only the final provider-visible text; truncation
  and cancellation diagnostics remain on their owning Tool Event/operation.
  The settlement
  response does not return any of those facts; Runtime applies its immutable
  request after a committed or duplicate result.
- **Budget.** `MaxStableReasoningPartsPerRequest` (16) and
  `MaxStableReasoningBytesPerRequest` (2 MiB) are one budget enforced ACROSS
  the locked durable Assistant message, not per append. Reasoning remains only
  in its provider-visible context member; Bridge does not create a second audit
  projection or synthetic Part identity.
- **Invariants a replacement must preserve.** Each append and create is atomic,
  positional, and idempotent under its owning operation key. Tool settlement
  is independent of prior reasoning, text, and sibling Tool Uses. Replay must
  return the operation-specific result; a changed declaration conflicts.
- **Failure behavior.** An error or rescheduled request end carries no new
  Assistant append. Every member already acknowledged by its owning
  `WriteEvent` remains durable and provider-visible; request-local fragments
  still buffered in pod memory are discarded and regenerated on retry. A
  committed Tool Use forces its preceding ordered prefix through the same
  durable append boundary, but Tool membership is not a separate survival
  rule. Pod loss remains stricter: an abandoned open draft is excluded unless
  reconciliation completes the exact Tool Call/Result repair pair.
- **Conformance.** `bridge_api_events_test.go` drives PostgreSQL `WriteEvent`
  and `WriteRequestEnd` to prove ordered durable members, deterministic replay,
  global Tool Call identity, target-only Tool settlement, and exact/one-over
  count and byte bounds with transactional rollback. Context-load and Pod-loss
  tests distinguish ordinary failed/rescheduled preservation from incomplete
  Pod-loss repair exclusion.

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
  load-balanced service. Initial MCP manifest listing
  similarly uses a fixed 180-second per-call deadline. This accommodates the
  connector's credential, reconnect, and list budgets while bounding the
  single-threaded Job Runner sweep to 180 seconds per stalled workspace; it
  abandons the Bridge wait but does not cancel connector work.
- **Conformance.** `job_runner_test.go`, `runtime_delivery_test.go`,
  `runtime_delivery_store_test.go`, `runtime_delivery_exhaustion_test.go`,
  `completion_mail_test.go`, `completion_mail_delivery_test.go`,
  `runtime_pod_lost_delivery_repair_test.go`.

### Sandbox handoff and output adoption

- **Contract.** Bridge records refs-only Sandbox execution, command, memory,
  and output-capture work. It never imports a provider SDK or calls a helper.
  Sandbox Service resolves the provider adapter and persists normalized
  outcomes for Bridge to consume.
- **Lifecycle.** `FinishIdle` creates or joins a capture generation and waits
  outside a transaction. Sandbox Service stages deterministic Blob children
  before the parent result. Bridge's final transaction either adopts that
  exact generation with the file index and idle event or rolls back without
  losing staged custody. Expired, unadopted generations are cleaned by
  Sandbox-owned cleanup jobs.
- **Invariants a replacement must preserve.** One open generation exists per
  FinishIdle write id; failed generations remain immutable; Blob custody moves
  only in the final adoption transaction; a stale Runtime scope cannot adopt
  or write a second idle event.
- **Conformance.** `bridge_api_settlement_test.go` and
  `services/sandbox/output_capture_runner_test.go` plus
  `services/sandbox/output_capture_store_test.go`.

### MCP durable claim/commit idempotency

- **Contract.** MCP tool calls (`bridge_api_mcp.go`, `mcp_manifest_lister.go`)
  are Bridge-backed because the mcp-connector owns no writable store. `Claim
  McpToolResult` replays a stored result on hash match, fences concurrent
  execution with a leased reservation, or admits execution; `CommitMcpTool
  Result` stores the refs-only result and creates its transient-attachment rows
  from the commit's bounded inline-media leg in one transaction. `RelinquishMcpToolResult`
  deletes only the named in-flight claim and records the operation for lost-ACK
  replay; it returns stale without changing stored results or a different active claim. Manifests are
  captured before delivery through two connector clients: `McpManifestChanged`
  handles hot changes from a running pod, while the Job Runner lists and
  captures the initial manifest before a session's first input. Both write the
  bounded, generation-ordered `session_mcp_manifests` row and enqueue
  redelivery over `runtime_config_update`.
- **Lifecycle.** Each Connector execution attempt creates a `claimId`.
  Same-claim replay renews its lease; a different unexpired claim returns
  in-flight; expiry admits a new `claimId` takeover. Commit and relinquish both
  fence the exact active claim, whose terminal lifetime ends only when its
  result is committed, it is explicitly relinquished, or it is replaced after
  lease expiry. The active claim is stored as `mcp_claim_id`; no generic request
  identity aliases this ownership. Bridge compares the normalized input hash against the durable
  Tool declaration internally; the hash is not executor ownership and is not
  returned as a claim handle. At most one result is ever persisted.
- **Invariants a replacement must preserve.** The mcp-connector never writes the
  attachment store — Bridge is the sole writer, and attachment creation cannot
  be split from commit by a crash. Persisted results carry text, metadata, and
  `attachment_ref` capabilities only, never raw or base64 media. Supersession
  keys on `manifest_generation` monotonicity, never on etag inequality.
  Attachment GC preserves uploading and staged media while the source Sandbox
  execution remains unconsumed, so expiry cannot race ahead of Tool Result
  adoption.
- **Conformance.** `bridge_api_mcp_test.go`, `mcp_manifest_continuity_test.go`,
  `mcp_collision_split_test.go`, `command_read_claim_test.go`,
  `TestJobRunnerRuntimeDeliveryStoreDiscoversInitialMCPManifestThroughProductionAssembly`.

### Resource roots snapshot and credential-expiry readiness gate

- **Contract.** Bridge records the approved Sandbox execution and its Queue job
  in one transaction. It does not inspect provider state, mint credentials,
  mount resources, build helper payloads, or reset lifecycle state.
- **Lifecycle.** Sandbox Service resolves the current binding, performs fresh
  provider inspection, and converges activation and materialization before it
  authorizes command submission. The materialization result carries the
  resource roots and credential expiry used by the provider adapter.
- **Invariants a replacement must preserve.** Bridge remains a durable clerk:
  it validates declaration identity, writes refs-only business facts, and
  publishes Queue work. Provider and helper behavior stays behind the Sandbox
  Service adapter registry.
- **Conformance.** Sandbox lifecycle/execution store suites and Runtime Pod
  tool-runner tests.

### Kubernetes pod visibility (engine-root `internal/kubernetes`)

- **Contract.** `internal/kubernetes` and `internal/internalgrpc/auth` are
  engine-root shared packages — they live at the repository root, not under
  `services/bridge/`; the bridge consumes them but does not own them.
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
- **Conformance.** Bridge-local: `bridge_visibility_test.go`,
  `runtime_pod_lost_store_test.go`. Engine-root (under
  `internal/kubernetes/`): `visibility_client_test.go`, `cache_test.go`
  (covering the `WatcherCache` type in `watcher_cache.go`),
  `static_visibility_test.go`.

## What it owns

The platform's widest writer surface, every row keyed by `workspace_id` and
idempotent: runtime-side `session_events` and the `session_messages`
projection; `session_pending_tool_uses`; `session_background_tasks`;
`session_runtime_inbox`; `session_runtime_bindings`; request usage detail rows
and the `sessions.usage` projection; `session_output_captures`;
`session_transient_attachments` and the file-attachment consumption records;
`session_mcp_manifests`; `session_turn_retries`;
`session_runtime_tool_results` (including the `tool_kind = mcp`
claim/stage/consume lifecycle); `session_runtime_status` (running/idle writes and cleanup finalization)
and the runtime-side writes on `session_threads` (child lifecycle; public
archive admission stays with api). Through its RPCs it also writes the
memory tables (`RunMemory`), the event change-log rows that ride every public
event write, and refs-only Sandbox execution requests plus their Queue rows.
It never inspects provider state or mutates Sandbox lifecycle.

Boundaries it does not cross: no hot loop state (no run slots, fibers, or
provider streams); no provider lowering, credentials, or model calls; no
Sandbox provider execution (create/start/release belong to Sandbox Service — the
Bridge carries no Sandbox-provider configuration, while `job-runner` receives
only the Blob credentials needed for Session cleanup, inspectable in the
manifests and asserted by tests); no public HTTP termination; and cleanup
never deletes durable history.

## Testing guide

| Suite | Proves |
| --- | --- |
| `authz_test.go` | Workload-identity check and the binding fence reject before any durable mutation |
| `bridge_api_context_test.go` | `LoadContext` cold-start assembly, in-band manifests, pending-media reconstruction |
| `bridge_api_inputs_test.go` | `CommitInputs` / `CommitTaskNotificationResult` stamping, projection, interrupt-snapshot control state |
| `bridge_api_events_test.go` | `WriteEvent` atomicity, whitelisted projection, anchored-reasoning and usage-attachment folding, replay conflict |
| `bridge_api_settlement_test.go` | `WriteRequestEnd` / `FinishIdle` / `CommitRuntimeTermination`, single-terminal-end serialization, reschedule ceiling, best-effort capture gate |
| `bridge_api_children_test.go` | Child create/resolve/mark lifecycle and thread-context-prefix checkpoint |
| Sandbox execution store/runner suites and Runtime Pod tool-runner tests | Sandbox acceptance/result-wait separation, exact replay identity, result custody, and durable memory behavior |
| `bridge_api_tasks_test.go`, `background_bash_real_lifecycle_test.go`, `command_read_claim_test.go` | Background-command follow-ups and stdin write-sequence dedupe |
| `bridge_api_mcp_test.go`, `mcp_manifest_continuity_test.go`, `mcp_collision_split_test.go` | MCP claim/commit idempotency, reservation fencing, capture-before-deliver, generation-ordered supersession, collision split |
| `bridge_api_attachments_test.go`, `bridge_api_file_attachments_test.go`, `attachment_transport_test.go`, `scoped_transport_capacity_test.go` | Read-only Gateway resolvers, scope validation, offset-addressed file chunk reads, helper-transport capacity scoping |
| `closeout_sentinel_test.go` | `scope_superseded` stale mapping and precise `closeout_unrepairable` status typing |
| `job_runner_test.go`, `runtime_delivery_test.go`, `runtime_delivery_store_test.go`, `runtime_delivery_exhaustion_test.go` | Queue reconcile, direct-to-pod delivery, inbox upsert, delivery-exhaustion fencing |
| `completion_mail_test.go`, `completion_mail_delivery_test.go` | Child completion-return discriminator and atomic envelope-plus-wake write |
| `runtime_pod_lost.go` suites (`runtime_pod_loss_repair_test.go`, `runtime_pod_lost_interrupt_fence_test.go`, `runtime_pod_lost_delivery_repair_test.go`, `runtime_pod_lost_store_test.go`) | Lazy proven-gone discovery, snapshot/pagination and concurrency fences, interrupted-then-lost quiet settlement, and Sandbox-lifecycle independence |
| `runtime_session_cleanup_test.go` | Cleanup order, the tree fence claim proof, reschedule-at-both-points, and the durable Sandbox release gate |
| `bridge_visibility_test.go` + `internal/kubernetes/*_test.go` | Pod/EndpointSlice visibility and the proven-gone vs merely-unavailable classification |
| `config_test.go`, `cmd/bridge-api/main_test.go`, `cmd/job-runner/main_test.go` | Startup config validation and production dependency assembly |
| `runtime_delivery_store_test.go` (`TestJobRunnerRuntimeDeliveryStoreDiscoversInitialMCPManifestThroughProductionAssembly`) | Production Job Runner assembly reaches the configured MCP connector over TCP, captures the first manifest durably, and admits the retried input |
| `deploy/kubernetes/manifest_test.go` (`TestKubernetesManifestAgentRuntimeBridgeUsesSplitContainers`) | Bridge carries no Sandbox-provider configuration; `bridge-api` and `job-runner` receive only the Blob credentials required by their attachment-read and Session-cleanup responsibilities |

If a PR changes an RPC's idempotency identity, the binding fence, the repair
rules, the queue reply mapping, the cleanup order, the closeout dispositions,
or the output-capture seam in this folder, it updates the matching section
here.
