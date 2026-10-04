# agent-runtime

The runtime's hot half: the TypeScript/Effect process that runs ThreadLoop.
Everything it holds lives in memory and is disposable by construction —
durable truth stays in the database behind Bridge, and the pod mutates hot
state only after the matching Bridge ACK (one named exception: the interrupt
closeout is hot-first by design and commits its snapshot last). Its wires are
Bridge RPCs for persistence, the Provider Gateway service's provider rail (the streaming
provider request) and its tool rails (`RunMcpTool` on the MCP Connector
service, `RunWeb` on the Web Connector service), its own gRPC command server
for Job Runner commands, and the Kubernetes TokenReview call that
authenticates them. It holds no SQL connection, no sandbox provider key, and no
provider API key; its only mounted credentials are the Kubernetes
service-account tokens used for internal gRPC authentication. Because nothing
here is the source of truth, anything lost with the pod is either rebuilt from
durable state on the next cold load or settled by Job Runner repair.

The package is three Bun/TypeScript workspaces:

| Workspace | Path | Owns |
| --- | --- | --- |
| core | `packages/core` | ThreadLoop, hot-state model, tool system, provider/runtime contracts — no process, no transport |
| protocol | `packages/protocol` | generated pod and Bridge gRPC types plus shared bound constants |
| runtime-pod | `packages/runtime-pod` | process entrypoint, gRPC/HTTP servers, Bridge/Gateway clients, TokenReview auth, tool runner |

The process entrypoint is `packages/runtime-pod/src/command.ts`
(`runRuntimePodCommand`), built to `dist/command.js` and shipped as
`ghcr.io/tetral-ai/agent-runtime`. Kubernetes manifests live under `k8s/`.

## States & lifecycle

### Hot-state objects

All hot state lives inside one pod and is recoverable from durable state. The
named classes below are the current in-pod shape; the binding rules are the
invariants stated with them.

| Object | Anchor | Holds | Disposability |
| --- | --- | --- | --- |
| `SessionEntry` | `session-manager.ts` | `threads: Map<session_thread_id, ThreadEntry>` — a residency map, not a durable parent-child index | rebuilt from Bridge reads over `session_threads` |
| `ThreadEntry` | `session-manager.ts` | one resident thread with role, status, `ThreadRuntime`, command channel, and `run_slot` | released whole on cleanup — no orphan fibers, timers, or maps survive |
| `ThreadRunSlot` | `session-manager.ts` | the single-owner run guard (below) | hot memory only; durable truth is `session_events` / `session_messages` / `session_pending_tool_uses` / `session_threads` |
| `ThreadRuntime` | `thread-loop/thread-runtime.ts` | one thread's binding identity, `ThreadState`, configuration and shared coordinators | recreated when the thread becomes resident; owns no durable truth |
| `ThreadState` | `thread-loop/thread-state.ts` | canonical `ThreadTurnCheckpoint`, read-only route/input/media views, pending tool work and other reconstructible hot data | rebuilt from durable state on cold start; dispatch is never stored here |
| `ContextManager` | `context-manager.ts` | one committed message store and an immutable child prefix; ThreadState supplies current-request association and historical eligibility | rebuilt from durable projection; mutates only after the owning operation's durable ACK |
| `ThreadTurnCheckpoint` | `thread-loop/turn/checkpoint.ts` | canonical durable lifecycle projection for one thread | reconstructed from durable facts; contains no provider write identity or hot dispatch |
| Turn views | `thread-loop/turn/types.ts`, `thread-loop/turn/checkpoint.ts`, and `ThreadState` | read-only accepted-input, attachment and Tool-route projections consulted with the checkpoint | derived from current hot/durable custody; not an independent lifecycle |
| Reducer / transition | `thread-loop/turn/reducer.ts` | pure rules producing checkpoint, state, stable next step and an optional one-time dispatch | immutable result of one fact/view cut; owns no I/O or mutable data |
| `ThreadLoop` | `thread-loop/thread-loop.ts` | sole fact-application and dispatch-execution authority for one thread | captures dispatch on the stack, performs external work, then applies the next durable fact |
| `RequestContentProcessor` | `accumulator.ts` | completed provider-event validation, reasoning prefix and ordered member submission | created per provider request; committed content belongs to ContextManager and execution registrations belong to ThreadState.activeTools |
| `ToolJob` / `ToolScheduler` | `tool-scheduler.ts` | per-provider-request coordination over `toolJobs[]` | belongs to the active provider request; reads no database, owns no Bridge |
| `AutoApprovalReviewerManager` | `approval-reviewer-manager.ts` | reviewer trunk + ephemeral sidecars, transcript feed cursor, last-committed snapshot, target-specific decision memo | disposable hot state on the parent thread; failure fallback requires an ACKed outcome, failed requests reach durable idle before trunk reuse, and uncertain outcomes evict only the addressed execution |

Invariants a replacement must preserve:

1. Request-turn accumulation is scoped to exactly one provider turn and never
   leaks across turns.
2. Thread-scoped hot state is fully released with thread cleanup.
3. A thread serves no inbound command until its cold load — durable context,
   pending tool waits, background task handles — has completed; no fiber ever
   observes a half-hydrated entry.
4. Hot state is not the source of truth.

### Session infrastructure and Thread execution

`SessionEntry` owns shared binding-local infrastructure and the `controlGate`.
Each `ThreadEntry` independently owns one `ThreadRuntime`, command queue and
`ThreadRunSlot`:

```text
SessionEntry
  -> shared configuration / binding / cleanup control
  -> ThreadEntry A -> ThreadRuntime A -> runSlot A -> ThreadLoop A
  -> ThreadEntry B -> ThreadRuntime B -> runSlot B -> ThreadLoop B
```

Sibling run slots may execute concurrently. An interrupt or close addresses one
ThreadEntry and cannot make another Thread's custody stale. Session-wide config
installation and cleanup still use `SessionEntry.controlGate`; config returns
`control_busy` while any resident Thread is installing or running, then applies
once every run slot is idle. Durable delivery exclusion is owned by Queue and
Bridge, while `threadRunCanStart`, `startThreadRunUnderControl`, and
`applyRuntimeConfigPatch` own the corresponding hot-state rules.

The turn data and execution flow for each thread is:

```text
durable Events / Messages / Tool facts
  -> turn/load.ts
  -> ThreadTurnCheckpoint + read-only views
  -> ThreadState
  -> pure Reducer
  -> ThreadTurnTransition
       checkpoint + state + stable nextStep
       optional one-time dispatch
  -> ThreadLoop captures dispatch on its stack
  -> Provider / Tool / Bridge operation
  -> durable ACK
  -> next committed ThreadTurnFact
```

Cold preload installs a checkpoint and views and derives a stable snapshot; it
never replays a hot dispatch. Ordinary input such as a message or task
notification updates the accepted-input view and signals the run slot. It does
not mutate the current Request/Tool lifecycle or replace a dispatch already
captured by ThreadLoop. Explicit interrupt and shutdown use their control paths
and may cancel current execution.

`ContextManager` is adjacent to, not inside, the turn state machine. It supplies
provider-visible context after durable ACKs. A model Request ID identifies the
provider attempt, an Event ID identifies a durable lifecycle fact, and a Runtime
write ID identifies one in-flight idempotent write. A write ID may live in the
stack-local operation and Bridge receipt, but never in `ContextManager`, the
checkpoint, provider history, Tool payloads or a later turn.

### `run_slot` — single-owner run guard

At most one owner run exists per thread. Many callers may join that owner.
Accepted inputs remain in `ThreadState` until their durable custody transition;
the Reducer decides at a legal boundary whether they can advance the thread.

| Field | Meaning |
| --- | --- |
| `run_id` / `owner_fiber` / `scope` / `done_deferred` | the active run, its scope, and the deferred that joined waiters await |
| `stopping` | interrupt installed; the owner is unwinding |
| `ThreadState` accepted-input view | ordered accepted custody retained until a durable commit, parked custody, or terminal rejection/stale result |

| Event | Idle thread | Active, `stopping = false` | Active, `stopping = true` |
| --- | --- | --- | --- |
| `resume` | mark the resident Thread idle and receivable; later input starts work | reject as busy | reject as busy |
| accepted input | install the input and start one run only when its derived next step is active | install the input; the current run observes it at a legal transition boundary | install the input behind the interrupt fence |
| `interrupt` | mark accepted, start no provider request | `stopping = true`, interrupt owner, close scopes | already stopping |
| owner exits clean | — | clear the old slot, derive the latest transition, and start at most one successor for an active next step | clear the old slot after finalizers, then apply the same reducer rule |

A successful `FinishIdle(end_turn)` ends the run even when input is queued; the
follow-up run for queued work is the next run. Intra-turn retries — reschedule,
overflow, compaction — stay inside the run. Cancelling a joined waiter never
cancels the owner. Different threads run concurrently while each thread stays
single-owner.

`FinishIdle(end_turn)` closes the Request/Run that authored the durable
closeout. Its validity comes from that exact owner and its committed Request
End, not from the action currently derived after later input arrives. Applying
the closeout removes only the old execution owner; queued input remains owned
by the successor run.

### The ThreadRun loop

One fixed algorithm per run freezes and commits a finite input cut, then drives
the durable Thread-turn transition. Hot context changes only after the matching
ACK. Compaction, approval, reviewer, retry, interrupt and failure are typed
next steps around the six states rather than extra top-level states.

Approval waits are selected from the current committed Tool routes, not the
stream's earlier approval observations. A confirmation committed before Request
End dispatches its named allow or deny settlement in the same run. A confirmation
committed while `FinishIdle(requires_action)` awaits its ACK remains actionable
after that frozen closeout commits; the existing route coordinator resumes it
under the newly opened execution owner. Still-undecided members remain passive
and do not start a successor provider request.

| Loop state | Owner | Durable boundary |
| --- | --- | --- |
| `idle` | ThreadRun owner fiber | `CommitInputs` installs pending context; the Reducer decides when that context authorizes Request Start |
| `ready_to_request` | ThreadRun owner fiber | pure hot decision point |
| `request_open` | provider-request scope | Tool Use ACKs and then `WriteRequestEnd` ACK |
| `request_sealed` | ThreadRun owner fiber | the committed Request End directly selects retry, Tool settlement, compaction, reviewer completion or closeout |
| `waiting_for_tool_results` | provider-request tool-fiber set or durable pending routes | every named terminal Tool Result ACK |
| `ready_to_finish` | ThreadRun owner fiber | `FinishIdle` ACK gates local idle |

The Thread-turn state is an internal typed contract, never a public status enum.
Cold reconstruction consumes the same durable facts that ACK application uses:
Message sequence defines pending user-side input, Request and Tool Events define
the active request, and internal repairs carry one direct repair identity. It
never compares Message and Event sequence coordinate systems or reconstructs a
Message mutation history. The Reducer selects the stable next step from the
checkpoint and separately owned read-only views. Only the exact fact transition
that first hands external work to ThreadLoop can also emit a dispatch; duplicate
fact replay and later ordinary input cannot reacquire it.
A normal run exit settles its scope exactly once through its owning durable
operation. Cooperative process replacement instead returns `checkpoint_yield`:
the current provider step seals its Request End and accepted work remains under
its original durable IDs. Yield does not fabricate idle, interrupt, completion,
or a successor wake. Bridge atomically releases the exact binding and creates
one recoverable handoff descriptor per resident Thread; the Queue and Runner
then install the continuation under a new process and binding.

A Session checkpoint covers every resident Thread, run, admitted tool,
reviewer dependency, and control command. An admitted reviewer may complete
only its captured parent step. Expiry cancels and joins both parent and reviewer,
then commits failed closeout before release. Accepted Sandbox execution and
approval waits can discard their local waiter while their durable owner remains
live. Provider, MCP, and Web calls with unknown external outcomes cannot be
transferred by retrying the tool. A rejected checkpoint retains custody for
binding-loss recovery; it never reports successful handoff.

### Durable-ACK gates

Durable ACKs gate every hot-state mutation; Bridge calls that gate durable
state are awaited Effects, never detached background work.

| RPC | Hot mutation it gates |
| --- | --- |
| `CommitInputs` | apply the caller-held accepted-input context drafts at the Bridge-assigned sequences after the committed result (including idempotent replay) |
| `WriteEvent` | apply the closed event result and its optional Assistant context append; open or resolve pending waits |
| `WriteRequestEnd` | validate current custody, append any trailing reasoning, and acknowledge the durable request outcome; committed Assistant content stays addressable for pending tools. An interrupt during an open provider request also applies the identity-matched input commit returned by the same transaction before acknowledging the interrupt; only then update `lastRequestUsage` and close the request turn |
| `FinishIdle` | enter local idle (after output capture / status) |
| `CommitRuntimeTermination` | under the current durable-turn identity, persist loop-authored current-thread cancellations and any abnormal child completion envelope; apply the closed termination result before removing pending tools or releasing the turn |

An active interrupt also closes any still-open recovered Request before its
control receipt and idle transition. Outstanding failed-run writes join before
this fallback starts. The same joined-End application gates the idle and active
paths; a stale result stops further writes, and a committed result that cannot
be applied evicts the invalid resident projection for cold reconstruction.
Already-closed requests do not receive a second End.

Message and Thinking writes carry frozen preallocated event IDs and framed stable
write keys. Committed and duplicate receipts must return that exact ID. A receipt
that cannot be coherently applied invalidates residency and fences subsequent
members. Reasoning prefixes are committed once with the next text/tool/invalid
repair or the successful End transaction; empty text creates no Assistant member.
Cold loads carry `messages` and an explicit nullable `currentRequestMessage`
reference. Runtime validates it against the selected durable request, stages
content and pending controls, and checks the full reducer/route projection before
publishing residency. It never restores the retired split draft envelope.

An interrupt delivery attempt reports a retryable Request-End failure through
the current `ThreadRunSlot`; retryable attempt state is never promoted into the
terminal replay memo. A joined duplicate is different: the durable operation
already committed, so it completes only the matching resident interrupt fence
and asks ordinary Reducer readiness whether one successor run must wake. Joined
without a matching fence is a successful no-op; stale custody keeps its discard
semantics. If a completed `ThreadRunSlot` is still retained as a custody fence,
the matching settlement releases that exact slot before starting the successor;
an active or replaced slot is never displaced by the wake.

`Effect` is the shape of every operation with I/O, failure, or cancellation.
`Fiber` exists only for owned lifetimes — the ThreadRun owner, the provider
stream fiber, and the per-turn tool-fiber set — each inside a `Scope` whose
finalizers close streams, tools, and hot waiters on interrupt or release.
`Deferred` is hot coordination only: durable waits (approvals, background tasks,
child state) live in database rows; hot waiters are disposable accelerators.

### Compaction

The proactive trigger runs at the request boundary, before assembly: compact
when `usage_total + estimate(delta) >= usable`, where the held usage and
route-effective limits come from the last successful finish (the pod holds no
model catalog and no tokenizer). A model change invalidates the held numbers,
so cold starts, wakes, and switches run unarmed and are covered by the reactive
backstop — a provider context-overflow is intercepted once per episode (a
one-shot flag), compaction runs, and the rebuilt request is re-issued.

The cycle serializes the committed conversation to tagged text lines, oldest
first; scans back a keep budget so the tail becomes `recent` (kept verbatim)
and everything older is summarize material; runs a fit-check against the context
window and refuses rather than trims; sends the prompt as one user message on
the compacting thread's own model and credentials, tools disabled, no system
prompt; then mints the checkpoint and applies it only after the Bridge ACK. The
loop never proceeds past a failed compaction.

| Platform policy constant | Value |
| --- | --- |
| reserve | `min(20,000, output cap)`, or the deployment override used as-is |
| keep budget | 8,000 tokens (chars/4) |
| summary output ceiling | 4,096 tokens |
| checkpoint mint cap | 60 KiB (Bridge validates the durable row at 64 KiB) |

## Seams

### Provider boundary (via Gateway)

Runtime never speaks a provider dialect. It assembles a provider-neutral
`ProviderRequest` and consumes a normalized `ProviderStreamEvent` stream;
Gateway owns all provider lowering and credential injection.

- Interface: `GatewayClient.streamProviderRequest` in `core/src/llm/llm-service.ts`,
  implemented by `RuntimePodGatewayClient` (`runtime-pod/src/gateway-client.ts`)
  over `ProviderGatewayService`. The request is built by
  `core/src/thread-loop/provider-request.ts`; the provider error codes are in
  `core/src/contracts/provider.ts`; stream validation and normalization
  (`validateProviderStreamEvent`, consumed in `llm-service.ts`) reject any
  malformed or misidentified `ProviderStreamEvent`.
- Gateway resource failures preserve `provider_stream_limit_exceeded`, status
  and message through normalization. They are fatal and nonretryable; existing
  Tool custody and settlement still follow the committed Request End.
- System-segment composition (assembly order): the platform base prompt (the
  `gpt` family also carries the `apply_patch` format instructions), the agent's
  create-time `system` text, one memory segment per attached store carrying that
  store's name, access mode, and create-time instructions, then the skill
  guidance segment. Reviewer requests carry base + reviewer policy only;
  compaction requests carry no system segments. Memory store content is never
  projected as files into the prompt — only this metadata is rendered.
- Skill guidance is a listing, not an embedding: the segment names each skill
  with its description, immutable version, and projected `/skills/<directory>/SKILL.md`
  path, and the model reads those projected files with ordinary tools; skill
  bodies are never inlined. Cold context derives the resolved skill index from
  the Session's immutable Agent version and durable skill-version rows; Sandbox
  materialization uses the same resolver, so prompt paths and mounted packages
  describe the same pinned versions.
- Lifecycle: a provider request begins at the `span.model_request_start` ACK; before
  that ACK no request end is owed. After it, every terminal success, provider
  error, cancellation, or repairable failure closes with `WriteRequestEnd`.

Invariants a replacement must preserve:

- `ProviderRequest.tools[]` carries definitions only — no route kind, RPC names,
  formatter identity, permission policy, sandbox binding, or credentials.
- Runtime removes an unresolved Tool Call from inherited child-prefix context
  before provider projection while retaining safe text and reasoning siblings.
  A local in-progress request may retain its pending Tool Call; a terminal Tool
  Result remains paired only by `modelToolCallId`.
- Runtime applies a terminal cancellation to hot conversation context as exactly
  `{type:"cancelled"}`. Provider-facing context never carries its internal
  cancellation error or diagnostic.
- Gateway output contract v2 carries RPC-scoped identity and consecutive frame
  sequences. Runtime accepts ThinkingStarted, TextComplete, ReasoningComplete,
  ToolCallComplete, and the existing terminal/attachment events. Retired fragment
  enums are rejected. Completed text/thinking event IDs are globally unique;
  provider part IDs are unique within their content kind.
- A validated terminal is held until the Gateway gRPC stream reaches normal
  EOF. Runtime adapts grpc-js through a Web reader and owns one typed completion
  latch; EOF without a terminal, transport failure after a terminal, consumer
  cancellation, or expiry of the request timeout plus the fixed 10-second
  transport-completion allowance cannot be mistaken for success. Every
  non-EOF exit cancels the reader and generated call before the latch settles.
- Each ToolCallComplete reserves one provider-ordered semantic position. Its
  durable Tool Use ACK installs a reference in activeTools. After permit/token
  waits, every fresh or recovered execution revalidates that reference and reads
  the canonical committed call before accepting or executing the tool.
- The next request cannot start until the stream is terminal, the request end is
  ACKed, and every committed Tool Use has reached its existing settlement owner.
  A failed attempt's text remains durable audit history but is excluded from all
  later provider requests. Runtime normalizes abnormal requests to exact retained
  Tool calls/results and associated reasoning, and withholds that whole message
  from history while any retained tool is pending. Successful End makes the same
  message eligible without relocating content. Tool settlement appends to its
  exact registered message and applies the terminal fact before releasing work.
- The pod is the only retry driver. The accepted Request End reschedule receipt
  carries the attempt and Bridge-effective deadline through hot or cold recovery;
  Runtime waits only the remaining deadline and re-issues exactly one request
  rebuilt from eligible committed context. The durable attempt seeds the next
  proposal, so pod loss cannot reset the budget.
- The reviewer model and provider credentials are platform-owned; Gateway injects
  credentials but never chooses or replaces the model.
- Media attachments obey `MaxProviderRequestAttachments` = 32
  (`core/src/thread-loop/thread-state.ts`). Runtime admits at most that many
  attachments into a pending request ride; it does not synthesize model-only
  advisory messages for attachments outside the ride.
- File-backed attachment consumption is declared by exact source Event/file
  pairs and becomes durable in the Request Start transaction before Provider
  dispatch. It is therefore at-most-once across error, reschedule, and pod loss.
  Transient tool-result media remains hot-owned and is consumed only by a
  successful Request End. Attachments admitted during an active ride wait for
  the next request.

Conformance tests: `core/test/unit/llm-service.test.ts`,
`core/test/unit/thread-loop/provider-request.test.ts`,
`runtime-pod/test/unit/gateway-client.test.ts`.

### Tool route table

Which builtin tools exist is decided by the session's pinned toolset family —
exactly one family (`claude` or `gpt`), delivered cold with the runtime config,
materialized family-exact, failing closed rather than falling back to any full
catalog. The required platform tool `memory` stays installed regardless of
config. Materialization, gating, scheduling, and dispatch are separate stages
with separate anchors.

| Stage | Anchor |
| --- | --- |
| materialization (`ToolEntry`: definition / route / formatter) | `core/src/tools/tool-catalog.ts` |
| availability vs approval (`evaluateToolGate` → `ToolGateDecision`) | `core/src/tools/tool-gate.ts` |
| per-turn scheduling (`ToolScheduler`, `runPolicy`) | `core/src/tools/tool-scheduler.ts` |
| route dispatch (`RuntimePodToolRunner`) | `runtime-pod/src/tool-runner.ts` |

`ToolRoute` (execution-only, never provider-visible):

| Route kind | Operation | Target |
| --- | --- | --- |
| `sandbox` | `AcceptSandboxExecution` / `AwaitSandboxExecution`; `CommandIO` | Bridge durable acceptance/result read; Sandbox Service executes provider work |
| `gateway` | `RunWeb` | independent Web Connector Service (`web-connector`) |
| `gateway` | `RunMcpTool` | independent MCP Connector Service (`mcp-connector`) |
| `bridge` | `RunMemory` | Bridge |
| `subagent` | `spawn_agent` … `list_agents` | in-process child thread |

Sandbox dispatch accepts one exact durable Tool Use before waiting for its
result. A cold Runtime rejoins that accepted execution after refreshing its
binding token; a transient refresh failure does not invent a Tool Result or
consume durable custody. Bridge reads and verifies the terminal Sandbox result
digest from its own durable execution row when Runtime settles the Tool target;
the digest never crosses the Runtime boundary.
Sandbox activation exhaustion is normalized at this shared rejoin boundary for
command, file, media, and command-I/O routes: the private lifecycle settlement
becomes one non-retryable Runtime error whose public Tool Result contains only
`The requested operation could not be completed.`, with no Sandbox concept,
route status, partial result, attempt metadata, or provider diagnosis.

`RunWeb` reaches the web-connector through `TETRAL_WEB_CONNECTOR_GRPC_ADDR`,
which boot config requires and gives no default: a Runtime Pod whose Deployment
spec lacks it fails to start rather than losing web tools alone. Roll the spec
and the image together.

`evaluateToolGate` separates availability from approval: `full_access` skips approval
for enabled tools; `ask_for_approval` defaults to asking (a per-tool
`always_allow` policy short-circuits it); `approve_for_me` routes
review-required decisions to an internal reviewer thread — the same loop under
an internal thread id, a find-or-create trunk plus ephemeral sidecars when the
trunk is busy, each decision keyed to the exact target tool call and committed
durably before the gate returns. Any reviewer failure falls back to a public ask,
never a silent allow. The reviewer toolset is fixed (`Read`, `Grep`, `Glob`,
each `always_allow`) and its model is platform runtime configuration.

Invariants a replacement must preserve:

- Route kind is execution-only and never provider-visible.
- Family materialization fails closed; there is no full-catalog fallback and no
  hot family transition; `memory` is always installed.
- The per-family builtin tool-NAME set is a closed set, pinned by the
  `ClaudeFamilyToolNames` / `GPTFamilyToolNames` `as const` tuples in
  `core/src/tools/tool-catalog.ts` (unioned into `FamilyToolNames`). A new
  capability that is not family-specific must be platform-owned and
  family-independent (as `memory`, `web`, and the subagent tools are), not
  appended to a family list. Extending a family tuple is a code change to a
  compatibility surface, guarded by the SDK-compatibility traceability gate in
  the `go-static` CI job, so a family-specific addition that skips that
  treatment does not ship.
- `runPolicy`: `exclusive` jobs with the same `conflictKey` serialize (same-path
  `Write`/`Edit`/`apply_patch`; `write_stdin` by `task_id`; `memory`
  session-wide); `parallel_safe` runs bounded by `maxConcurrentTools`
  (default 8). Results reach the next request in model order regardless of
  completion order.
- No route executes until the required `agent.tool_use` event has ACKed; a fiber
  updates hot state only after the matching `WriteEvent` ACK.
- A call to a disabled or unknown tool produces no public events: it settles
  through the event-less internal repair, committed via Bridge before the next
  request may start.
- Runtime holds no cross-invocation lock for file operations: the sandbox helper
  takes no file locks and assumes upstream exclusivity is already granted through
  `conflictKey` (the one named exception is the detached-task reservation lock
  `.task-limit.lock`, a kernel `flock` guarding the task-cap critical section, a
  detached-task-cap concern owned by sandbox, not file-operation defense);
  whole-file temp+rename bounds the symlink-aliasing gap, and cancellation reaches the
  sandbox through command teardown (foreground) or Bridge `CancelCommand`
  (background task), never a bare fiber interrupt.

Conformance tests: `core/test/unit/tool-system.test.ts`,
`runtime-pod/test/unit/tool-runner.test.ts`.

### Sub-agent host

Sub-agent tools use the same ThreadLoop under child thread ids. There is no
specialized sub-agent loop and no reviewer-only model-call path. The parent
thread sees tool use/result; child work stays child-thread-local.

- Interface: the `subagent` route operations dispatch in `tool-runner.ts`; child
  threads are created through Bridge `CreateSubagentThread`; Runtime declares
  the normalized child metadata and bounded initial prompt once, interprets
  public `fork_turns`, and sends the exact ordered durable parent Message
  references. Bridge validates and snapshots only those references while
  committing initial custody atomically; turn partitioning is
  `core/src/runtime/conversation-turns.ts`.
- Lifecycle: `spawn_agent` commits the durable child row, immutable context
  prefix, initial sent/received Events, target Inbox/Queue custody, and one
  replay receipt directly from its live durable Tool Use, before the parent
  Provider Request End. Later `send_message` calls resolve the child by
  `task_name` and deliver through their own stored envelope and durable Runtime input rail;
  `wait_agent`, `interrupt_agent`, `close_agent`, `resume_agent`, and
  `list_agents` operate over durable `session_threads`. Runtime resolves
  `task_name` before child control and declares the exact child ID plus fixed
  interrupt/close action; Bridge never reinterprets those Tool arguments.

Invariants a replacement must preserve:

- Child thread, prefix, initial mail Events, Inbox/Queue custody, and receipt exist
  together or not at all. A crash after `CreateSubagentThread` commits reuses
  that complete Bridge-owned lineage without a second mail birth; the
  replay receipt carries only the child identity needed by Runtime.
- Inter-agent delivery is exactly-once by `delivery_id`, ordered
  sent envelope → received source/inbox → Runtime command → committed input
  result → hot admission → accepted Inbox stamp → exact Queue ACK. Initial and
  later mail use this same path. Request Start remains a one-time ThreadLoop
  dispatch, never delivery or ACK authority. If the Runtime binding is proven lost,
  generic accepted-input handoff restores the same input and Queue identity for
  the replacement owner without replaying a second durable Message.
- `task_name` is unique under the parent by durable constraint, never by
  serializing spawns in the scheduler.
- Completion return rides the same durable wake rail: the child
  settlement writes one sent envelope and wake, admission creates or reuses the
  received source and inbox, and the parent's `CommitInputs` projects it once.
  `wait_agent` returns the exact stored envelope immediately while ensuring the
  same delivery remains recoverable for the parent's next legal run.
- `interrupt_agent` and `close_agent` first ask Bridge to freeze the durable
  target census. Each target acknowledges the internal control input before
  the parent can complete; Bridge owns terminal Tool projection and the
  no-new-work fence. `close_agent` then closes the complete descendant subtree,
  preserves `failed` and `terminated` outcomes, and only afterward releases
  resident hot state. `resume_agent` validates a quiescent closed checkpoint
  before reactivation; terminal rows are never installed into hot state.

Conformance tests: `core/test/unit/session-manager.test.ts`,
`core/test/unit/conversation-turns.test.ts`,
`core/test/unit/tool-system.test.ts`.

### Command boundary

The deployed control caller is `tetral-system/job-runner`; Bridge and the tool
connectors have no control authority. Every inbound method-specific request must select this exact Pod UID and boot-scoped Runtime process ID, carry a
non-empty current binding id and a non-zero binding generation, and authenticate
through TokenReview to the closed RPC set; a mismatch is a retryable rejection,
never a processed command. The Runtime Pod does not accept or echo Pod
namespace, name, or IP as command payload. Anchors:
`runtime-pod/src/command.ts` (dispatch), `runtime-pod/src/runtime-service.ts`
(scope binding), `runtime-pod/src/auth.ts` (TokenReview). The pod queries no
database, calls no sandbox provider, terminates no public HTTP, and holds no
secret material.

### Process registration and shutdown

Each boot generates one stable process ID and registers it after its owned
clients, Runtime Core, listeners, and routing proxy are ready. An authenticated
Bridge registration returns a receipt and server-assigned order; an ACCEPTING
report must commit before `/ready` becomes ready. Reports repeat every two
seconds with a one-second timeout. Ten seconds without a committed report
withdraws admission and readiness. A later validated current ACCEPTING ACK from
this boot restores both after a temporary Bridge outage. An explicit stale-process
rejection permanently fences this boot and notifies the command to shut down and
exit nonzero, without registering another identity. Startup rejection has the same
disposition. Reusable lifecycle code reports the failure to its owner; the
executable supplies the bounded exit policy. A late ACK cannot reopen shutdown admission.
A newer boot in the same Pod supersedes the old boot; old-process commands, tokens,
writes, and promotion are stale.

`TETRAL_TRANSPORT_PROFILE=standard-routed` binds `0.0.0.0:19090`.
`hardened` binds the application to `127.0.0.1:9090`, with the routing proxy's
external TLS listener on 19443. `TETRAL_RUNTIME_POD_GRPC_PORT` must equal the
selected profile's application port (19090 or 9090), and
`TETRAL_ROUTING_PROXY_REQUIRED` accepts only `true`. Startup waits at most 30
seconds for the fixed local proxy readiness endpoint
(`127.0.0.1:15021/healthz/ready`). Hardened startup also requires successful
initial updates for both named direct-listener SDS resources through the fixed
local Envoy admin endpoint (`127.0.0.1:15000`); a listening socket alone is not
sufficient. Runtime does not read the proxy's private key. A failed cross-field
check reports the keys involved, never their values.

Shutdown withdraws admission immediately and joins command ingress while each
Session independently reaches its next current-step checkpoint. A committed
DRAINING report gates binding release. A rejected release leaves only that
Session's binding in place for Job Runner's fenced loss repair and is recorded as
an incomplete handoff; it never stops another Session's release. Owned clients
close only after every Session has released, failed, or reached the settlement
deadline. Defaults allocate 60 seconds for current
steps, 15 seconds for settlement and release, five seconds for local joins, and
five seconds for proxy joins, within the 90-second Pod grace period. An idle
Session releases immediately; one slow Session does not delay another Session's
handoff. No successor provider request starts in the draining process. Owned
unary calls cancel real handles and join callbacks before channels close.
Reusable lifecycle owners retain unjoined producers and their dependencies. The
executable command arms one absolute application deadline from the configured
current-step, settlement, and local-join phases (80 seconds by default). If a
producer or cleanup still cannot join, the executable reports incomplete shutdown
and exits nonzero at that deadline, without claiming a handoff or closing required
dependencies early. Diagnostic sink failures cannot defer this exit. The proxy
allocation remains a separate deployment and listener cleanup bound.

Lifecycle controls are `TETRAL_RUNTIME_REPORT_INTERVAL_MS`,
`TETRAL_RUNTIME_PROCESS_FRESHNESS_MS`, `TETRAL_RUNTIME_DRAIN_TIMEOUT_MS`,
`TETRAL_RUNTIME_SETTLEMENT_TIMEOUT_MS`,
`TETRAL_RUNTIME_SETTLEMENT_ATTEMPT_TIMEOUT_MS`, `TETRAL_RUNTIME_LOCAL_JOIN_TIMEOUT_MS`,
and `TETRAL_RUNTIME_PROXY_JOIN_TIMEOUT_MS`. The register/report RPC controls are
`TETRAL_RUNTIME_REGISTER_TIMEOUT_MS` and `TETRAL_RUNTIME_REPORT_TIMEOUT_MS`;
report timeout must be shorter than interval, which must be shorter than
freshness. Session capacity defaults to 256 and concurrent tools to eight.

The exhaustive method policy in `runtime-pod/src/bridge-policy.ts` owns actual
unary deadlines: reads 30 seconds, durable waits 35 seconds, ordinary writes
three seconds, registration/release five seconds, and reports one second.
Each fixed method accepts its `TETRAL_BRIDGE_<METHOD>_TIMEOUT_MS` override.
Attachment reads consume the provider request's remaining absolute deadline.
Parent cancellation and shutdown deadlines can shorten every budget. Named
receipt recovery repeats only the unchanged operation; generic unknown-outcome
retries and whole-turn drain are absent. The generated-method inventory and
policy projection must remain exhaustive when methods change.

The typed shutdown policy owns a separate final-settlement attempt maximum,
five seconds by default. At quiesce, clients receive both the current-step and
settlement boundaries. Current-step calls retain their method deadlines. After
the current-step boundary, each Bridge attempt clips to the configured final
maximum and remaining settlement time. An already retained current-step call
has at most that maximum remaining at the transition; its actual handle cancels
and callback joins before retry or client close. A two-second total settlement
window with the default five-second attempt maximum is valid: the shorter
shared window wins. Attempts cannot reset either phase or extend process grace.
`RunWeb` and `RunMcpTool` are outside this Bridge method policy: their execution
budgets belong to the Web Connector and to the MCP Connector's shared execution
budget, so during quiesce they end with the Tool route abort at the current-step
deadline, and client close cancels and joins any remaining call.

Core ordinary writer and accepted-input consumers await the adapter's actual
transport result before retrying. The generated Bridge owner supplies parsed
method deadlines, cancels real RPC handles and joins callbacks; an expired shared
phase cannot dispatch another RPC. The existing retry owners retain three
attempts and 100/300 ms backoffs where allowed. Core does not race a second fixed
three-second timer against configured transport deadlines. Metadata preparation
precedes the generated RPC attempt; an earlier Core observation cannot discard a
healthy result or abandon the raw operation.

FinishIdle owns a durable capture/closeout operation rather than an ordinary
short write. A joined retryable wait expiry rejoins the identical declaration,
serially, with 100/300 ms backoffs capped at 300 ms. It does not exhaust the
ordinary three-failure budget. Other retryable failures retain their separate
three attempts and 100/300 ms backoffs; stale and deterministic rejection stop
immediately. The settlement phase deadline, set when shutdown begins, is the
only outer bound: it cancels and joins the actual unary call and stops rejoin.
Outside drain no caller deadline applies, and wait expiries rejoin the same
capture until success, stale or deterministic rejection, or three ordinary
failures. The independent three-second failed-run memo observer never supplies
these operation controls; it may expire while the same raw FinishIdle remains
owned, and later observation rejoins that memo. Ordinary Core interruption also
retains non-abandonable closeout ownership until the actual callback joins.

Failed-run closeout is distinct: its existing three-second memo observation
window may expire while separately owned settlement continues. Later observers
rejoin the same memo work, and observer cancellation does not cancel transport.
The generic writer also serves the production reviewer failure host. Its abstract
promise-only adapter must own a bounded deadline and cancellation/join behavior;
an adapter that never settles keeps the reusable consumer pending. Executable
shutdown supplies the separate absolute exit bound while retaining required
dependencies until joined work completes.

The real Runtime handoff composition exercises independent held provider requests,
fresh placement after admission closes, exact original turn/tool/result context,
all-thread checkpoints, reviewer Read continuation and expiry, and authenticated
old-owner fences. Fault controls distinguish precommit rejection, committed lost
responses, selected process death, same-Pod container restart, and simultaneous
legal input/recovery binding owners. The same-Pod restart kills and joins the old
container after its original Read has reached the independent Sandbox owner. A
fresh boot under the unchanged ready Pod causes production loss repair, Queue
recovery and rebinding; the new Core rejoins that same external execution and
consumes its exact result before dispatching the successor provider request.
The final census retains one original user input, one external dispatch and one
adopted output capture keyed to the original turn, with no further user trigger.
A child must have actual spawn lineage before
its normal completion; its original mail can remain accepted after Runner admission
and is handed back through the same durable input identity. Queue ordering retains
that input ahead of a handoff wake. The fixture drives those existing owners without
manual acknowledgement or extra input to trigger the original continuation.

## Testing guide

Run from the package root:

```sh
bun run typecheck
bun test               # core + protocol + runtime-pod unit suites, with coverage
bun run test:integration   # runtime-pod/test/integration against fakes and gRPC harnesses
```

| Suite | Proves |
| --- | --- |
| `core/test/unit/session-manager.test.ts` | `run_slot` single-owner, `wake` coalescing, idle-only resume, interrupt/stop fences, concurrent distinct threads, sub-agent delivery and lifecycle, cold load of durable context, pending waits, and background handles |
| `core/test/unit/thread-loop/thread-loop.test.ts` | ThreadLoop coordination and recoverable ThreadState behavior |
| `core/test/unit/thread-loop/thread-turn-load.test.ts`, `thread-turn-transition.test.ts` | durable turn reconstruction and the closed transition table |
| `core/test/unit/thread-loop/tool-execution.test.ts`, `closeout.test.ts` | post-ACK tool execution, continuation, interruption, and settlement |
| `core/test/unit/thread-loop/compaction.test.ts` | proactive and reactive compaction lifecycle |
| `integration/content_compaction_cycles_test.go` (repository root) | three actual SDK compaction/eviction cycles plus cold provider-context recovery; fixed external usage triggers the normal model threshold after bootstrap history exceeds the retained recent window, and fixed summaries verify exact committed checkpoints |
| `core/test/unit/thread-loop/provider-request.test.ts` | system-segment composition, tool-definition-only requests, attachment inclusion |
| `core/test/unit/llm-service.test.ts` | provider-stream ordering/identity validation and normalization |
| `core/test/unit/request-content-processor.test.ts` | completed member ordering, exact ACK application, reasoning prefixes, pressure and ownership release |
| `core/test/unit/session-event-writer.test.ts`, `runtime-context-projection.test.ts` | `WriteEvent` projection whitelist and hot-state updates after ACK |
| `core/test/unit/turn-retry-budget.test.ts` | provider and compaction reschedule budgets |
| `core/test/unit/tool-system.test.ts` | `evaluateToolGate` decisions, `runPolicy` serialization/parallelism, invalid-tool repair, approval routing |
| `core/test/unit/approval-reviewer-manager.test.ts` | reviewer trunk/sidecar selection, cursor and snapshot succession, decision memo |
| `core/test/unit/conversation-turns.test.ts` | `fork_turns` turn partitioning |
| `core/test/unit/session-run-static-boundaries.test.ts`, `static-boundaries.test.ts` | import confinement and dependency boundaries |
| `runtime-pod/test/unit/command.test.ts`, `runtime-service.test.ts` | method-specific ingress validation and scope binding |
| `runtime-pod/test/unit/auth.test.ts` | TokenReview identity and the closed command set |
| `runtime-pod/test/unit/gateway-client.test.ts` | the Gateway provider-stream client |
| `runtime-pod/test/unit/tool-runner.test.ts` | route dispatch across sandbox / gateway / bridge / subagent |
| `runtime-pod/test/unit/bridge-client.test.ts`, `core-writer-policy.test.ts` | Bridge clients/input committers, parsed method policy through actual Core assembly, bounded attempts and independent failed-run observation |
| `integration/mcp_runtime_manifest_delivery_test.go` (repository root) | actual SDK notification → Bridge manifest generation → Queue lease → Job Runner → Runtime catalog application, busy rejection and same-job retry, duplicate/stale commands, next Core provider request, and replacement Bridge cold load after both Connector owners close |
| `integration/replica_runtime_handoff_test.go` (repository root) | real-process checkpoint, reviewer and fault continuation, exact durable custody/context, binding races and joined exit |
| `runtime-pod/test/integration/app.test.ts`, `gateway-capacity.test.ts` | full process wiring plus maximum-context projection, protobuf, real gRPC, and provider-lowering capacity proof |
| `protocol/test/unit/bounds.test.ts` | shared bound constants |

Production dependency factories never return fake, mock, or fallback clients;
tests inject fakes at the smallest useful boundary (network, time, ids,
storage).

If a PR changes the `run_slot` law, the loop algorithm, the request-turn
lifecycle, the compaction trigger or cycle, the tool family/gate/route rules,
the sub-agent delivery contract, or the command-validation surface in this
folder, it updates the matching section here.

## Process diagnostics

The Runtime command uses the shared
[TypeScript diagnostic contract](../../internal/ts-observability/README.md).
Diagnostic level, record byte ceiling, summary interval and burst controls are
read once at startup. The default level is Info. Existing safe admission,
provider transport, reviewer, closeout and committed-settlement builders retain
bounded operator classifications and identities. Stream backpressure and
asynchronous stderr failures appear through the existing HTTP metrics endpoint.
Startup and shutdown cleanup release logger timers/listeners without waiting
for stderr, after the business resource owner completes its cleanup.

Completed content records expose bridge acknowledgement and local application
as separate stages. Their `output.size_bytes` is the UTF-8 size of the owning
frozen canonical JSON (Assistant append, Thinking event, repair, settlement or
End envelope), not protobuf or socket bytes. Size cohorts can be derived from
these numeric samples; transport byte metrics remain separate. Continuation
records expose the pre-End declaration/member barrier, permit admission, binding refresh,
approval wait, execution acceptance, result settlement, cold reconstruction and
cleanup join. Aggregate latency labels are closed operation, outcome and request
classes; identities appear only in bounded Debug samples. Pending member count
and bytes report submissions still owned through acknowledgement and application.
Tool-fiber gauges cover fresh and recovered fiber lifetimes, including approval
and permit waits, and release on fiber exit.
A protected post-Finish member barrier finishes when its actual custody owners
join; cancellation intent alone does not manufacture a cancelled wait sample.
Cleanup observers read correlation without running the reducer, so damaged
resident state cannot prevent the resource owner from closing its scope.

Approval timing covers one hot residency span. The existing pending control owns
a one-shot observation with source `user` or `auto_reviewer`; a decision completes
it, and residency disposal records local observation cancellation. That
cancellation does not cancel the durable approval. Started and outstanding
metrics retain the denominator for incomplete observations. Cold reconstruction
reports timing unavailable in a separate counter and a raw sample without a
duration. It contributes neither zero nor a successful completion to latency
summaries. These metrics do not measure total human wait across processes.

The command attempts app shutdown before Runtime Core close even when startup,
waiting or an earlier close rejects, and makes repeated shutdown calls share
one cleanup attempt. The app observes all started drain and listener shutdown
operations. Programmatic callers retain the original run failure, or the first
cleanup failure after a successful run. Executable and signal boundaries use
fixed safe phase/class records and nonzero failure exits without exception text
or stacks. Diagnostic faults add no stderr-flush wait; bounded business drain
settings remain owned by the lifecycle.
