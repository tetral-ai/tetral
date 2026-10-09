# bridge

## Responsibilities

Agent Runtime Bridge is the runtime's durable half. Every fact the agent
loop needs persisted crosses exactly one boundary — a Bridge RPC — and
each durable-write RPC commits its result in one PostgreSQL transaction;
read-only resolvers are the exception. Some write RPCs also open separate
short transactions around that commit: after commit, `WriteEvent`,
`CommitInternalToolRepair`, `FinishIdle`, `CommitTaskNotificationResult`,
`MarkChildThreadActive` and child close re-read the current binding in a
read-only transaction and report a superseded scope as stale, and an interrupt
`CommitInputs` reads its request event for its log in another;
`WriteRequestEnd` reads its request start in a read-only transaction before
writing; and `FinishIdle` creates or joins its output-capture generation in
its own transaction, then polls that capture every 100 ms in short
transactions that lock the row and extend its retention. Sandbox execution crosses two distinct boundaries:
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
Tenant isolation is structural: durable rows carry `workspace_id`, reads and
mutations enforce workspace scope, and every caller presents a signed principal binding that Bridge
verifies before any read or mutation. Bridge runs as an independent Deployment and ServiceAccount, with one
`bridge-api` process (`cmd/bridge-api`). It serves Runtime and executor-facing
RPCs and owns its database pool, execution-result listener and attachment GC.
[Job Runner](../job-runner/README.md) independently consumes runtime-facing Queue
jobs and owns binding declaration, delivery, loss repair and Session cleanup.
Each process has its own credentials and shutdown lifecycle. Bridge
owns no provider lowering, no model calls, no sandbox lifecycle, and no
public HTTP; it never deletes durable history.

## States & lifecycle

### Process lifecycle

Bridge exposes authenticated gRPC plus health and metrics HTTP. Startup validates
process diagnostic controls and database schema/runtime role before constructing
business listeners and clients. Protected PostgreSQL and Blob clients require
`TETRAL_DATABASE_TLS_CA_PATH` / `TETRAL_DATABASE_TLS_SERVER_NAME` and
`TETRAL_BLOB_TLS_CA_PATH` / `TETRAL_BLOB_TLS_SERVER_NAME`. When
`TETRAL_ROUTING_PROXY_REQUIRED=true`, startup also waits for the local routing
proxy before admitting business calls. Shutdown withdraws HTTP readiness and
gRPC health, closes admission, and gives admitted RPCs their configured drain
budget. Execution-result LISTEN and attachment GC stay available while those
RPCs settle. After graceful completion or forced cancellation, handlers,
listeners and maintenance owners join before retained MCP connections, Blob
resources or the database pool close. The process diagnostic
owner closes last with a bounded shutdown budget. The executable enforces one
absolute drain-plus-join deadline; an uncooperative producer causes exit status 1
without premature resource closure. Drain plus join may not exceed 50000 ms
inside the 60-second Pod grace. A healthy idle listener emits
no periodic successful diagnostic records.
The shared `internal/workload` diagnostic owner defaults to `info` and validates
`TETRAL_LOG_LEVEL` values `debug`, `info`, `warn` or `error` plus bounded record,
limiter and sink controls at boot;
configuration changes require restart. Health metrics expose dropped records
and sink failures. A stalled diagnostic sink cannot own a business receipt or
block transaction producers. See [workload diagnostics](../../internal/workload/README.md).


Sandbox execution results have their own channel. Every production write that
transitions an execution to `terminal_unconsumed` — Sandbox Service settlement
and Session-deletion waiter settlement — emits a refs-only
`tetral_sandbox_execution_result` notification in the same transaction, so
commit publishes the result and its hint together and rollback publishes
neither. The `bridge-api` process owns one reconnecting `LISTEN` connection
for that channel and routes each hint to the local `AwaitSandboxExecution`
waiters whose workspace-qualified durable identity it names; LISTEN readiness
and every reconnect broadcast a catch-up wake to all local waiters. A waiter
registers and takes its wake snapshot before its first verification read, so a
commit landing during the read or between the read and blocking forces an
immediate re-read instead of a missed wake. A hint is never a result: every
wake leads through the durable verification read. There is no periodic result
query within a wait. The configured result-wait deadline (30 seconds by default, clipped by an earlier
caller deadline) ends the RPC. Runtime rejoins the same accepted execution
after its existing 300 ms retry delay; the new wait begins with a durable read.
A result missed during a listener outage is therefore observed on reconnect
catch-up or rejoin. Detecting a half-open listener connection depends on TCP
keepalive and network settings; reconnect has no fixed detection bound.
If the listener remains unavailable, discovery can take the remaining RPC
deadline plus retry and database latency; there is no one-second
delivery guarantee. A healthy idle 30-second wait performs one result
verification transaction plus the separate entry scope-validation transaction.
Connection failures are logged before retry; an unexpected listener error return
is logged as `bridge.execution_result_listener.stopped`. Normal shutdown is quiet.

### Runtime process custody

`RegisterRuntimeProcess` authenticates the Pod and registers one stable boot ID
as a non-current starting candidate. Bridge allocates its registration order and
opaque receipt with the database clock; caller timestamps are not trusted. The
same transaction creates the process's liveness row with no report yet.
Exact registration retry returns the same order and receipt; a retry whose
liveness row is missing fails as an internal invariant error instead of
creating one. Only the matching receipt-bearing accepting report promotes a
candidate. Promotion compares the last promoted order, retires the previous
process atomically, and schedules old binding reconciliation after commit. An
unseen or abandoned candidate cannot write, receive placement, or displace the
current process.

`ReportRuntimeProcess` keeps lifecycle and liveness apart. Every report first
reads the process row without locking it in one READ COMMITTED transaction: a
missing or retired process is stale and a different receipt is denied. An
unchanged report of the current process (same phase) then updates only its
`runtime_process_liveness` row, conditioned on the process still being current,
unretired, in that phase and with that receipt, and commits. It never takes the
Pod or process locks, so it neither waits for Session mutations holding the
process `FOR SHARE` nor delays promotion. Its acknowledgment reflects the
process row as that UPDATE statement saw it: a promotion or drain committing
meanwhile can supersede the reply, which is only an observation acknowledgment
and grants no mutation authority, placement or custody. Everything else
(promotion, phase change, abandonment, a draining process asking to accept,
or an unchanged report whose UPDATE matched no row) rolls back and runs in a
new transaction that locks the Pod row, then the old current and candidate
process rows in registration order, applies the existing promotion rules and
records the report on the candidate's liveness row; all of it commits together.
A missing liveness row fails that transaction as an invariant error. Neither
path locks a Session.

Every Runtime scope carries `runtime_process_id` alongside binding ID,
generation and target Pod UID. New mutations hold Session arbitration, the exact
binding row, then the matching current process shared lock until commit or
rollback; they never read liveness. One receipt-scope verification takes those
locks once per transaction and returns a transaction-local proof of the locked
binding and process facts. Receipt replay may use the proof even after the
process retired; every new mutation first checks on that same proof that the
process is current, unretired and not starting, without repeating binding or
process SQL. The proof is passed down the RPC's own transaction closure only.
The Sandbox-serving entrypoints (`CommitTaskNotificationResult`, background
command acceptance and cancellation, FinishIdle output-capture adoption,
`AcceptSandboxExecution` and `RunMemory`) still lock the binding and process a
second time before new work; `CommitRuntimeTermination`, whose terminal
receipt replays without a binding, locks them once through the standalone
current-process check. The lock order is Session, binding, process `FOR SHARE`
for mutations; Pod, process rows `FOR UPDATE`, liveness for lifecycle reports;
liveness alone for unchanged reports. Ordinary receipt replay may bypass
process-current after retirement only while its authenticated workspace,
Session, Thread and exact binding remain unchanged. It returns the stored identity/result without touching timestamps,
claims or projections. Background operation and memory projection waiters bind
the exact scope proof to the sensitive receipt SELECT itself, so a binding cut
between an earlier validation and a later poll cannot disclose stored results.
Independently accepted Sandbox/Queue work retains its own custody.
Context, attachment and first-effect authorization reads
still require current process custody. Once the binding is superseded, ordinary
receipt replay rejects. Named terminal/frozen-child receipts and cooperative
release use their separately persisted old-owner proofs.

`ReleaseRuntimeBinding` requires the old current process's acknowledged draining
phase. Under Session arbitration it checks every resident Thread's reconstructible
checkpoint, hands back ordinary accepted/delivering input custody, preserves
committed inputs and accepted executor identities, then atomically removes the old
binding and writes immutable per-Thread `IDLE` or `RECOVER` dispositions. For
each `RECOVER` Thread the same transaction enqueues one new `runtime_recovery`
job carrying `handoff_id`, deduplicated by workspace, Session, Thread and
handoff, and records its Queue ID; `IDLE` Threads get no job. Accepted,
uncommitted reviewer input rejects release as not checkpointed.
Exact release response-loss retry reads the original receipt after unbind; it
cannot enqueue again or reapply custody transitions.
The process sink records `runtime.binding.released` or
`runtime.binding.release_rejected` with exact operation, binding and process
correlation. Successful release records its handoff ID, Thread `target.count`
and returned inbox `input.count`; rejection records the owning `grpc.code`.

### Lifecycle settings

Settings are positive milliseconds and resolve once at startup. Registration and
report budgets share the Runtime contract:

| Setting | Default |
|---|---:|
| `TETRAL_RUNTIME_REGISTER_TIMEOUT_MS` | 5000 |
| `TETRAL_RUNTIME_REPORT_TIMEOUT_MS` | 1000 |
| `TETRAL_RUNTIME_REPORT_INTERVAL_MS` | 2000 |
| `TETRAL_RUNTIME_PROCESS_FRESHNESS_MS` | 10000 |
| `TETRAL_DRAIN_TIMEOUT_MS` | 40000 |
| `TETRAL_CANCEL_JOIN_TIMEOUT_MS` | 5000 |
| `TETRAL_BRIDGE_ADMISSION_TIMEOUT_MS` | 3000 |
| `TETRAL_BRIDGE_RELEASE_RUNTIME_BINDING_TIMEOUT_MS` | 5000 |
| `TETRAL_BRIDGE_SANDBOX_RESULT_WAIT_TIMEOUT_MS` | 30000 |
| `TETRAL_BRIDGE_BACKGROUND_RESULT_WAIT_TIMEOUT_MS` | 30000 |
| `TETRAL_BRIDGE_MEMORY_PROJECTION_WAIT_TIMEOUT_MS` | 30000 |
| `TETRAL_BRIDGE_OUTPUT_CAPTURE_WAIT_TIMEOUT_MS` | 30000 |

Report timeout must be shorter than report interval, which must be shorter than
freshness. Admission and release attempts must fit within the drain budget. An
in-process store whose lifecycle policy has a non-positive phase or violates
these phase limits rejects the RPCs that use it with `FailedPrecondition` rather
than substituting defaults.
Admission commits and waits have separate budgets: a successful admission never
keeps a transaction open while waiting for Sandbox, memory projection, background
command or output capture. Caller deadlines clip the owning phase; cancellation
remains distinguishable from deadline expiry. Wait expiry preserves durable
accepted work for exact rejoin. No proxy or application retry guesses a new
operation identity.

### Database connection pool configuration

`TETRAL_DB_MAX_OPEN_CONNS` defaults to **20 per process**. Bridge requires at
least **2** connections: its execution-result listener holds one, leaving at
least one for business transactions. An await waiter returns its query
connection before waiting for a wake. Job Runner has an independent pool and
Queue listener; its corresponding minimum is documented by its owner.

### Session infrastructure and Thread execution

One Session binding hosts a collection of independently executing Threads.
The durable ownership chain is:

```text
SessionEvent producer
  -> Event + target-Thread Inbox + Queue job (one transaction)
  -> Queue Session arbitration + exact delivery lane lease
  -> JobRunner validates lease and Session binding
  -> Runtime target ThreadEntry / runSlot
  -> Bridge receipt + Inbox settlement + Queue ACK
```

`queue_jobs.causal_session_id` orders work against shared configuration,
cleanup, delete, binding loss, and Pod loss. `delivery_scope` and
`delivery_thread_id` separately define lease exclusion: one live lease per
Thread, concurrent sibling Thread lanes, and Session-exclusive shared work.
The short PostgreSQL arbitration owner is acquired before an exact existing
Queue row; it is released before Runtime, Provider, Tool, or approval work.

An interrupt fences only its target Thread's later delivery and mutation.
Source and sibling Threads keep their own custody. Shared configuration remains
Session-exclusive; only a structurally declared target interrupt may cross a
pending or leased config boundary, and it does not release ordinary input past
that boundary. The concrete carriers are `queue.EnqueueRequest`,
`queue.PostgreSQLQueueStore.Lease`, `AppendClientEvents`,
`runtimecontrol.ThreadInterruptBarrier`, and Job Runner's
`RuntimePodDirectDeliverer` (`services/job-runner`).

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
| Events | `WriteEvent`, `CommitInternalToolRepair` | One non-result semantic event plus its projection in one transaction; a public Tool Use may carry the anchored prefix of completed reasoning parts. An invalid-tool repair atomically appends its private reasoning prefix and Tool Call/Result pair, with one rehydratable result Event. An absent or empty prefix preserves the ordinary repair declaration identity. |
| Settlement | `SettleToolResult`, `WriteRequestEnd`, `FinishIdle`, `CommitRuntimeTermination` | `SettleToolResult` derives one public result Event and terminal Tool projection from the named durable Tool Use; its closed result is only committed, duplicate, or stale. Request End writes usage and cumulative projection in one transaction. An ordinary successful end may append only its final not-yet-durable Assistant members before sealing the existing model-request projection; retryable failure seals only content already durable and carries the reschedule leg. An interrupt during an open provider request joins its separately owned `CommitInputs` envelope. The reschedule leg increments the durable per-thread retry budget and writes rescheduled status only when the ceiling admits — at most one terminal end per model request, a losing close yields. `FinishIdle` ensures or joins Sandbox-owned output capture, polls it every 100 ms in short transactions without holding one across the wait, then atomically adopts its staged Blob references with idle status. `CommitRuntimeTermination` validates the open durable turn and stores only deterministic terminal declarations. A child failure remains local and, when the child is a sub-agent, commits its completion mail; a Main failure atomically closes every non-terminal sibling request and Tool obligation, cancels remaining Session input custody, closes the live residency row to `idle` without arming ordinary TTL cleanup, and terminates the Session while retaining the binding identity only for closeout replay, without mailing the terminal Main Thread. |
| Children | `CreateSubagentThread`, `EnsureApprovalReviewerTrunk`, `EnsureApprovalReviewerSidecar`, `AdmitApprovalReviewInput`, `ResolveChildThread`, `ListChildThreads`, `DeliverInterAgentMail`, `ReadAgentMail`, `AdmitChildInterrupt`, `AwaitChildInterrupt`, `CloseChildControl`, `CloseApprovalReviewer`, `MarkChildThreadActive` | Bridge-owned child identity and exact snapshot of Runtime-selected parent Message references; accepted reviewer Inbox custody; durable sender-time mail delivery plus target-owned text reads; durable subtree interrupt admission and completion; operation-specific child control and reviewer lifecycle marks |
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

### Closeout failure dispositions

A run fiber that dies without reaching a settlement write still routes through
a durable closeout — a terminal `session.error` then the idle settlement, so
the child-completion discriminator classifies it errored, never a false
completion. When the closeout write itself fails, the governing asymmetry is:
a **lost** closeout (released when a retry would have landed it) is
unrecoverable; a **loud** retry loop is visible and curable. Release is
therefore only ever sentinel-gated (`internal/runtimecontrol/closeout.go`), and retry is the
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

JSON content bounds count the UTF-8 bytes of JavaScript `JSON.stringify`.
The shared Go encoder correction preserves actual Unicode line and paragraph
separators while keeping literal backslash-u text escaped. Declaration identity,
stored context validation, and stable reasoning metadata accounting use this
same correction; a literal `\\u2028` is not a Unicode separator.

Tool declaration normalization owns fresh canonical Provider and public execution
inputs alongside their decoded context. Digest encoding reuses those validated
raw bytes while retaining their numeric and escape spelling. It applies separator
restoration to the entire envelope, then canonicalizes and hashes that envelope;
unsupported or unprepared envelopes retain the ordinary encoder and its errors.

`CommitInputs`, `WriteEvent`, `SettleToolResult`, `WriteRequestEnd`, repair,
compaction, idle, and termination retain separate request and result types.
There is no generic declaration result. Each successful hot-path application
uses the immutable request plus only newly assigned facts returned by that
operation. Cold recovery returns one ordered `messages` array of
`{messageSequence, contextKind, parts}` and direct `turnFacts`. The separate
`currentRequestMessage` is `{modelRequestId, assistantMessageSequence}` or null;
it references the unique acknowledged Assistant for the current durable
request, including sealed requests with unfinished retained tools. The
selection is a checked projection of Runtime's current-request relation:
ordinary closed runs and no-content requests have no reference. It does not
fall back to a historical Assistant. When no durable turn is open, the bounded
request read includes its actual latest idle or terminal closeout and the exact
paired terminal failure; the Runtime selection rule remains unchanged.
A successful reasoning-only End can assign
the first Assistant sequence; cold loading follows its exact ordinary End
receipt under the same workspace, Session, Thread, Request and End Event. It
does not invent a caller retention selection. Immutable inherited context remains in
`threadContextPrefix.entries`. Messages carry no lifecycle flags or checkpoint,
and loading alone starts no tool work.

`LoadContext`, the declared sub-agent prefix and the reviewer-sidecar prefix
keep their own message selection and differ only in how a selected message's
content is read. An embedded message returns its stored document. The
Assistant message of a model request returns its immutable part rows joined
in index order, keyed by its workspace and message ID; the rows must be
exactly `0..next_part_index-1`, otherwise the read fails with the existing
durable-context `FailedPrecondition` and never substitutes an empty or
embedded document. The assembled text goes to the same number-preserving
decoder and part validator without any database cast. Child creation stores
the entries it read in the immutable prefix envelope; later parent appends,
settlements, compaction or Pod-loss repair never change that stored prefix.

Request End retention references must belong to that request. Completed and
compacted outcomes name every declared Tool; failed, interrupted and rescheduled
outcomes may omit a Tool only after its durable result exists. Every unfinished
Tool remains required, preserving custody through final settlement. Selection
changes provider-context eligibility while retaining the original audit events.

### Event-writer and Tool-settlement boundaries

- **Contract.** `WriteEvent` persists one non-result `session_events` row plus an
  event-specific append to the request's Assistant parts in one transaction. Usage,
  transport metadata, raw provider payloads, request ids, and raw attachment
  bytes never project. Opening or resolving an external wait updates
  `session_pending_tool_uses` in the same transaction: the trigger is the tool
  event's `evaluated_permission`. `ask` upserts an approval route with
  `status='pending'` and no decision; `allow` and `deny` upsert
  `status='resolving'` routes with the corresponding decision
  (`applyToolUseBookkeepingTx` in `bridge_api_events.go`). An allowed route
  records the execution decision; it does not itself prove execution acceptance
  or a terminal result. A public tool event may
  carry an anchored reasoning prefix. `SettleToolResult` is the sole ordinary
  Tool-result writer: Runtime supplies the durable Tool target and final bounded
  provider-visible outcome, while Bridge resolves the immutable Tool Call from
  its direct Tool Event/execution facts and appends a separate terminal Tool
  Result paired by call id. The Tool Call is never rewritten. Bridge derives the
  public result Event, Tool family, and any accepted Sandbox result digest from
  those direct facts. Web usage is part of the bounded outcome and increments
  `sessions.usage` exactly once. Neither digest nor settlement payload is
  returned to Runtime.
- **Tool relation.** Every Tool event writer stores its relation in scalar
  `session_events` columns derived, together with the payload and projection,
  from the one validated fact it is writing. A Tool Use or MCP Tool Use row
  carries `model_tool_call_id`, and `WriteEvent` inserts it once with its
  complete projection. An ordinary or MCP Tool Result carries
  `tool_use_event_id`, the event ID of the Tool Use it answers; its public
  payload names the same event as `tool_use_id` or `mcp_tool_use_id`. The
  synthetic invalid-tool repair result carries its call ID and references no
  Tool Use. The Thread-scoped unique call-ID index is the only call-ID
  admission check: reusing a call ID in the Thread, including after
  compaction, returns `AlreadyExists`; another Thread may use it. No
  declaration reads message history. Every result, interrupt, settlement,
  context and child-control association joins on `tool_use_event_id` within
  its workspace, Session and Thread, and a repair result is recognized by its
  call-ID column. Tool declaration validation already rejects an unpaired
  surrogate escape or a number beyond float64 range in the provider input,
  which is the execution input unless a distinct provider input is declared.
  Values that pass it but PostgreSQL JSONB cannot store fail the Tool Use
  storability CHECK at the declaration INSERT: escaped U+0000 in either input
  or the MCP server name, an unpaired surrogate escape or a number outside the
  PostgreSQL numeric range only in an execution input that has a distinct
  provider input, and in either input a nonzero number whose magnitude is
  below that range. Both rejections return `InvalidArgument` and write
  nothing. The CHECK exists because other readers still cast Tool Use payloads
  and projections to JSONB; assistant text, reasoning and Tool Results carry
  no such requirement.
- **Event feed.** Every feed-producing writer inserts its event through
  `sessioneventwrite.InsertInitialTx` after taking its existing Session
  mutation serialization: one reserved value of the change identity, one
  complete event INSERT at revision 1 with both stream positions set to that
  value, and one matching revision-1 change at the same position. The writer
  issues no follow-up UPDATE of the row it just inserted. Because the value is
  reserved after the Session serialization, a waiting writer on another Thread
  commits at a later position and a reader cursor never skips it; a
  rolled-back writer leaves a legal gap. `CommitInputs` marks committed inputs
  processed through `sessioneventwrite.RecordProcessedRevisionTx`, which keeps
  the insert position and records revision 2 at a newly reserved position; only
  such a later revision moves `latest_stream_position`. The Sandbox task
  notification remains the one event without a feed change.
- **Lifecycle.** `WriteEvent` is idempotency-keyed by `runtime_write_id`; the attached
  reasoning set folds into the request hash. `agent.message` and `agent.thinking`
  additionally require the Gateway-supplied `preallocated_event_id` (`evt_`
  followed by 32 lowercase hexadecimal characters) and model-request identity.
  That ID participates in the declaration digest and is returned unchanged on
  commit or exact receipt replay; it is forbidden on all other declarations.
  Missing or malformed identity fails before receipt lookup. New text and
  thinking require one durable request Start and no End; an exact pre-End
  receipt may still replay after End under the existing binding fence.
  Event IDs are globally unique. A collision returns a bounded conflict and
  rolls back the event, stream change, context append and receipt together,
  without returning the existing event or choosing a replacement ID. Bridge
  assigns event IDs for other event types and all database sequences.
  `SettleToolResult` hashes its
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
- **Conformance.** `bridge_api_events_test.go`; `tool_relation_test.go` drives
  every Bridge Tool event writer, the relation constraints, Tool Use
  storability, call-ID identity across compaction, text-only compaction and
  the Sandbox entrypoints' shared result checks under the real Bridge role.
  `feed_position_test.go` pauses a writer holding the Session fence and proves
  a concurrent production write on another Thread commits at a later position,
  a rolled-back reservation leaves a gap, and other Sessions proceed.

### Settlement transaction

- **Contract.** `WriteRequestEnd`, `FinishIdle`, and `CommitRuntimeTermination`
  are the request/model-usage and idle/terminal writers. `WriteRequestEnd`
  inserts the request-end span, inserts request usage detail idempotently, and
  updates `sessions.usage` only when the detail insert wins. It may append the
  request's final not-yet-durable Assistant members and then seals the existing
  assistant projection without replacing its owning event. Its closed result
  distinguishes ordinary, rescheduled, and compacted commits and returns only
  Bridge-assigned facts with an immediate caller. A compacted commit's
  checkpoint is text only: a nonempty list of text parts (empty text allowed,
  each within the text byte bound). Any reasoning, Tool call or Tool result
  part is `InvalidArgument` before anything commits, so the previous
  compaction and inherited prefix stay intact. Compaction changes the selected
  context window and never removes historical Tool identity events.
  A no-content end still commits the request boundary so a stale custodian
  cannot continue merely because there is no assistant projection.
  An interrupt received while the request is open carries only its admitted
  source envelope. Bridge derives and terminalizes the locked unfinished-Tool
  census in the nested input transaction. The response carries only the
  operation-specific interrupt Tool outcomes needed by the caller.
  The reschedule leg increments `session_turn_retries` and writes rescheduled
  status only when the ceiling admits. Private `LoadContext` direct facts pair
  that accepted attempt with the effective deadline from the request-end receipt
  and identify the request's Assistant message sequence; Runtime consumes those
  identities before constructing provider-visible entries. `FinishIdle` adopts a Sandbox-staged
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
- **Child creation authority.** Runtime interprets `spawn_agent` and declares
  normalized child metadata plus the bounded initial prompt once, together
  with the exact ordered durable parent Message references selected from
  public `fork_turns`.
  `CreateSubagentThread` verifies the exact public executable Tool route, live
  parent scope, mutation barrier, reference custody and bounds, and source-event
  idempotency without parsing Tool business arguments. The Tool Use may belong
  to an open provider request: Bridge snapshots exactly the referenced Messages
  before its durable Assistant boundary and commits the
  child, immutable prefix, first-mail sent/received Events, target Inbox/Queue
  custody, and minimal child-identity operation receipt in one transaction. Exact replay returns that
  committed lineage without requiring the route to remain executable or
  rereading later parent Message growth; a new Tool Use identity still crosses
  task-name uniqueness rather than aliasing the earlier receipt.
- **Child-control authority.** Runtime resolves `task_name` and declares the
  exact direct public subagent plus interrupt/close action (or resume target).
  Bridge checks the route capability and parent-child ownership mechanically;
  interrupt targets only the declared child, close freezes that child and its
  descendants, and resume targets exactly that child. Committed operation
  replay validates the stored declaration digest without requiring an
  executable route or decoding the source Tool input again.
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
  the ordered parts; Bridge assigns durable message and event identities,
  sequences, and timestamps. Completed
  conversation results contain only the final provider-visible text. A durable
  cancelled result is exactly `{type:"cancelled"}`; truncation and cancellation
  diagnostics remain on their owning Tool Event/operation.
  The settlement
  response does not return any of those facts; Runtime applies its immutable
  request after a committed or duplicate result.
- **Storage.** The Assistant message of a model request is a small header plus
  immutable `session_message_parts` rows. Every append, whether a declaration,
  repair, request-end suffix or Tool settlement, goes through the shared
  `runtimecontrol` append primitive under the caller's existing fences: it
  locks the header without reading content, validates only the new delta,
  inserts the new parts at the next contiguous indexes with one statement and
  advances the header counters once. Each part is stored as the JSON its writer
  admitted and is never read back, re-encoded or rewritten by a later append,
  so escaped U+0000, large integers, signed zero and exponent spellings survive
  settlement unchanged. A rolled-back append leaves its indexes for the next
  commit; replay appends nothing; no second message or sequence is allocated.
- **Budget.** `MaxStableReasoningPartsPerRequest` (16) and
  `MaxStableReasoningBytesPerRequest` (2 MiB) are one budget charged against
  the header's reasoning counters, not per append. Each reasoning part is
  charged once when it is admitted: its UTF-8 text bytes plus its metadata in
  the transported `JSON.stringify` form (no HTML escaping, separators
  restored), absent metadata counting as `{}`. Declarations, repair prefixes
  and request-end suffixes are checked cumulatively and rejected with
  `InvalidArgument` past either cap; Tool results add nothing; the database
  repeats both caps. Reasoning remains only in its provider-visible context
  member; Bridge does not create a second audit projection.
- **Invariants a replacement must preserve.** Each append and create is atomic,
  positional, and idempotent under its owning operation key. Tool settlement
  is independent of prior reasoning, text, and sibling Tool Uses. Replay must
  return the operation-specific result; a changed declaration conflicts.
- **Failure behavior.** An error or rescheduled request end carries no new
  Assistant append. Every member already acknowledged by its owning
  `WriteEvent` remains durable audit history. Bridge returns that direct Message
  plus Request/Tool facts; Runtime alone decides provider eligibility, excluding
  failed text while retaining exact terminal Tool Call/Result pairs and holding
  nonterminal Tool ownership privately. Request-local fragments still buffered
  in pod memory are discarded. Pod loss remains stricter: an abandoned open
  draft is excluded unless reconciliation completes the exact Tool Call/Result
  repair pair.
- **File attachment boundary.** A provider Request Start privately declares
  the exact file-backed `(source Event, file)` ride. Bridge writes those
  consumption rows atomically with the Start Event only when exactly one
  committed messages-Inbox owns the source Event. The source/file pair is
  consumable by only one Request Start, so a lost ACK replays one receipt and a
  later error, reschedule, or pod loss cannot make that file ride appear pending
  again. Request End retains only transient attachment settlement.
- **Conformance.** `bridge_api_events_test.go` drives PostgreSQL `WriteEvent`
  and `WriteRequestEnd` to prove ordered durable members, deterministic replay,
  Thread-scoped Tool Call identity, target-only Tool settlement, and exact/one-over
  count and byte bounds with transactional rollback. `message_parts_test.go`
  traces appends that read no stored content, commit-ordered indexes through
  every context and prefix reader, rollback reuse and replay, cumulative
  reasoning charges, contiguity failures, and raw U+0000 and numeric tokens
  through settlement, both prefix readers and a frozen child prefix.
  Context-load and Pod-loss tests distinguish ordinary failed/rescheduled
  preservation from incomplete Pod-loss repair exclusion.

### Sandbox handoff and output adoption

- **Contract.** Bridge records refs-only Sandbox execution, command, memory,
  and output-capture work. It never imports a provider SDK or calls a helper.
  Sandbox Service resolves the provider adapter and persists normalized
  outcomes for Bridge to consume.
- **Result wait.** `AwaitSandboxExecution` blocks on wake hints from the
  `tetral_sandbox_execution_result` channel (see States & lifecycle), with
  reconnect catch-up and deadline/rejoin recovery instead of periodic queries.
  The stored row — its identity match, terminal state,
  and result JSON validity — remains the only acceptance authority. The
  background-command result wait and the memory-projection wait are separate
  poll-based paths and deliberately unchanged.
- **Lifecycle.** `FinishIdle` creates or joins a capture generation and polls
  it every 100 ms in short transactions that extend its retention; no
  transaction stays open across the wait. Sandbox Service stages deterministic Blob children
  before the parent result. Bridge's final transaction either adopts that
  exact generation with the file index and idle event or rolls back without
  losing staged custody. Expired, unadopted generations are cleaned by
  Sandbox-owned cleanup jobs.
- **Invariants a replacement must preserve.** One open generation exists per
  FinishIdle write id; failed generations remain immutable; Blob custody moves
  only in the final adoption transaction; a stale Runtime scope cannot adopt
  or write a second idle event.
- **Conformance.** `bridge_api_settlement_test.go`,
  `execution_result_notification_test.go` (the 30-second idle check is skipped
  with `go test -short` and runs in full in CI; wake-path acceptance includes
  a real Queue/runner/terminal-writer-to-Bridge notification round trip with
  a gated provider double, plus local same-execution waiter cancellation and
  cross-instance fan-out), and
  `services/sandbox/output_capture_runner_test.go` plus
  `services/sandbox/output_capture_store_test.go`.

### MCP durable claim/commit idempotency

- **Contract.** MCP tool calls (`bridge_api_mcp.go`, `internal/mcpmanifest/client.go`)
  are Bridge-backed because the mcp-connector owns no writable Tool Result store. `Claim
  McpToolResult` replays a stored result on hash match, fences concurrent
  execution with a leased reservation, or admits execution; `CommitMcpTool
  Result` stores the refs-only result and creates its transient-attachment rows
  from the commit's bounded inline-media leg in one transaction. `RelinquishMcpToolResult`
  deletes only the named in-flight claim and records the operation for lost-ACK
  replay; it returns stale without changing stored results or a different active claim. Manifests are
  captured before delivery through two connector clients: `McpManifestChanged`
  handles hot changes from a running pod, while the Job Runner lists and
  captures a missing or unready manifest before executing a user message. Both write the
  bounded, generation-ordered `session_mcp_manifests` row and enqueue
  redelivery over `runtime_config_update`.
- **Discovery ownership.** [Job Runner](../job-runner/README.md#manifest-discovery) reserves initial discovery attempts and drives delivery. Bridge handles hot manifest changes. Both call `internal/mcpmanifest` acceptance helpers under their own transaction; external connector listing runs before the acceptance transaction.
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
  `mcp_collision_split_test.go`, `mcp_connector_production_composition_test.go`,
  plus the cross-owner compositions under `integration/` and initial discovery
  suites owned by [Job Runner](../job-runner/README.md). The actual SDK HTTP compositions in
  `mcp_server_resolution_test.go`, `mcp_output_validation_test.go`,
  `mcp_oauth_refresh_test.go`, `mcp_oauth_concurrency_test.go`,
  `mcp_adapter_execution_test.go`, `mcp_client_recovery_test.go`,
  `mcp_manifest_notifications_test.go` and `mcp_execution_budget_test.go`
  run against private PostgreSQL clones with installed Bridge and Gateway
  roles. They distinguish discovery and verification requests, prove encrypted
  credential scope and rotation, and compare original Runtime outcomes with
  durable receipts and replay. Held external responses prove cancellation of
  transport work; an already accepted external effect remains counted.
  `integration/mcp_runtime_manifest_delivery_test.go` consumes the resulting
  Queue carrier through Job Runner and the real Runtime command listener.

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

## What it owns

The platform's widest writer surface, every row keyed by `workspace_id` and
idempotent: runtime-side `session_events` and the `session_messages`
projection with its Assistant `session_message_parts`; `session_pending_tool_uses`; `session_background_tasks`;
`session_runtime_inbox` (Runtime commits and interrupt receipts); request usage detail rows
and the `sessions.usage` projection; `session_output_captures`;
`session_transient_attachments` and the file-attachment consumption records;
`session_mcp_manifests`; `session_turn_retries`;
`session_runtime_tool_results` (including the `tool_kind = mcp`
claim/stage/consume lifecycle); `session_runtime_status` (Runtime running/idle writes)
and the runtime-side writes on `session_threads` (child lifecycle; public
archive admission stays with api). Through its RPCs it also writes the
memory tables (`RunMemory`), the event change-log rows that ride every public
event write, and refs-only Sandbox execution requests plus their Queue rows.
It never inspects provider state or mutates Sandbox lifecycle.

Boundaries it does not cross: no hot loop state (no run slots, fibers, or
provider streams); no provider lowering, credentials, or model calls; no
Sandbox provider execution (create/start/release belong to Sandbox Service — the
Bridge carries no Sandbox-provider configuration); no public HTTP termination; and cleanup
never deletes durable history.

## Testing guide

`TestPostgreSQLRuntimeProcessLiveness` and the registration/report response-loss
cases exercise real process arbitration. Held-mutation, overlapping-report and
missing-liveness cases in `runtime_process_test.go` pin the separate liveness
path, and `TestPostgreSQLRuntimeScopeTransactionsLockBindingAndProcessOnce`
counts one binding and process lock per verified transaction.
`TestPostgreSQLRuntimeExecutorReceiptRetirement`
uses authenticated TCP Bridge instances, a same-Pod promotion and complete tenant
table snapshots to prove stored executor replay has no durable effects, then
checks ordinary denial after unbind. Integration replica placement, Runtime
handoff, Bridge recovery and worker drain compose the actual owning services.


| Suite | Proves |
| --- | --- |
| `authz_test.go` | Workload-identity check and the binding fence reject before any durable mutation |
| `bridge_api_context_test.go` | `LoadContext` cold-start assembly, in-band manifests, pending-media reconstruction |
| `bridge_api_inputs_test.go` | `CommitInputs` / `CommitTaskNotificationResult` stamping, projection, interrupt-snapshot control state |
| `bridge_api_events_test.go` | `WriteEvent` atomicity, whitelisted projection, anchored-reasoning and usage-attachment folding, replay conflict |
| `bridge_api_settlement_test.go` | `WriteRequestEnd` / `FinishIdle` / `CommitRuntimeTermination`, single-terminal-end serialization, reschedule ceiling, best-effort capture gate |
| `bridge_api_children_test.go` | Child create/resolve/mark lifecycle and thread-context-prefix checkpoint |
| Sandbox execution store/runner suites and Runtime Pod tool-runner tests | Sandbox acceptance/result-wait separation, exact replay identity, result custody, and durable memory behavior |
| `bridge_api_tasks_test.go` | Background-task creation, follow-up projection, and idempotent result commit |
| `bridge_api_mcp_test.go`, `mcp_manifest_continuity_test.go`, `mcp_collision_split_test.go` | MCP claim/commit idempotency, reservation fencing, capture-before-deliver, generation-ordered supersession, collision split |
| `bridge_api_attachments_test.go`, `bridge_api_file_attachments_test.go`, `attachment_transport_test.go`, `scoped_transport_capacity_test.go` | Read-only Gateway resolvers, scope validation, offset-addressed file chunk reads, helper-transport capacity scoping |
| `closeout_sentinel_test.go` | `scope_superseded` stale mapping and precise `closeout_unrepairable` status typing |
| `completion_mail_test.go`, `integration/completion_mail_delivery_test.go` | Child completion-return discriminator and atomic envelope-plus-wake write |
| `config_test.go`, `cmd/bridge-api/main_test.go` | Startup config validation and production dependency assembly |

If a PR changes an RPC's idempotency identity, the binding fence, the shared durable control
rules, the closeout dispositions,
or the output-capture seam in this folder, it updates the matching section
here.

Shared durable rules live in `internal/runtimecontrol`, installed configuration
interpretation and attached-memory reads in `internal/runtimeconfig`, and
manifest canonicalization and acceptance in `internal/mcpmanifest`. These
packages receive explicit data or the caller's existing transaction; they do
not read process environment, open a database pool, own a Queue consumer or
call a service business package. The process registry behind
`RegisterRuntimeProcess` and `ReportRuntimeProcess` is the exception: it
receives the Bridge client and owns its own short registry transactions,
because promotion must never run inside a Session transaction. Bridge remains the Runtime RPC owner and
Job Runner remains the reconciliation owner. Mixed owner tests live in
`integration/` and call each owner's actual production entry points.
