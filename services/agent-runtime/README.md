# agent-runtime

The runtime's hot half: the TypeScript/Effect process that runs ThreadLoop.
Everything it holds lives in memory and is disposable by construction —
durable truth stays in the database behind Bridge, and the pod mutates hot
state only after the matching Bridge ACK (one named exception: the interrupt
closeout is hot-first by design and commits its snapshot last). Its wires are
Bridge RPCs for persistence, the Gateway Pod's provider rail (the streaming
provider request) and its tool rails (`RunMcpTool` on the mcp-connector
container, `RunWeb` on the web-connector container), its own gRPC command server
for Bridge-issued commands, and the Kubernetes TokenReview call that
authenticates them. It holds no SQL connection, no sandbox provider key, and no
provider API key; its only mounted credentials are the Kubernetes
service-account tokens used for internal gRPC authentication. Because nothing
here is the source of truth, anything lost with the pod is either rebuilt from
durable state on the next cold load or settled by Bridge's repair.

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
| `ThreadState` | `thread-loop/thread-state.ts` | accepted inputs, pending tool and approval work, attachments, configuration views, and request usage hints for one resident thread | rebuilt from durable state on cold start; held limits and usage hints are never durable |
| `ContextManager` | `context-manager.ts` | sealed provider context plus one optional open Assistant draft | mutates only after an operation-specific durable result |
| `ProviderStreamAccumulator` | `accumulator.ts` | request-local provider framing and incremental Assistant member state | created per provider turn at the ThreadLoop boundary, discarded when the turn settles; it never owns or retransmits a complete durable Assistant message |
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

### `run_slot` — single-owner run guard

At most one owner run per thread. Many callers may join that owner. Inputs
accepted while it is active are installed in the thread's `ThreadProcessor`;
the reducer, not a side-channel wake flag, decides whether work remains.

| Field | Meaning |
| --- | --- |
| `run_id` / `owner_fiber` / `scope` / `done_deferred` | the active run, its scope, and the deferred that joined waiters await |
| `stopping` | interrupt installed; the owner is unwinding |
| `ThreadProcessor.acceptedInputs` | ordered accepted facts retained until a durable commit, parked custody, or terminal rejection/stale result |

| Event | Idle thread | Active, `stopping = false` | Active, `stopping = true` |
| --- | --- | --- | --- |
| `resume` | mark the resident Thread idle and receivable; later input starts work | reject as busy | reject as busy |
| accepted input | install the fact and start one run only when its reducer action is active | install the fact; the current run observes it at an action boundary | install the fact behind the interrupt fence |
| `interrupt` | mark accepted, start no provider request | `stopping = true`, interrupt owner, close scopes | already stopping |
| owner exits clean | — | clear the old slot, reduce the latest projection, and start at most one successor for an active action | clear the old slot after finalizers, then apply the same reducer rule |

A successful `FinishIdle(end_turn)` ends the run even when input is queued; the
follow-up run for queued work is the next run. Intra-turn retries — reschedule,
overflow, compaction — stay inside the run. Cancelling a joined waiter never
cancels the owner. Different threads run concurrently while each thread stays
single-owner.

### The ThreadRun loop

One fixed algorithm per run freezes and commits a finite input cut, then drives
the durable Thread-turn reduction. Hot context changes only after the matching
ACK. Compaction, approval, reviewer, retry, interrupt, and failure are typed
actions around the six states rather than extra top-level states.

| Loop state | Owner | Durable boundary |
| --- | --- | --- |
| `idle` | ThreadRun owner fiber | `CommitInputs` ACK makes a request eligible |
| `ready_to_request` | ThreadRun owner fiber | pure hot decision point |
| `request_open` | provider-request scope | Tool Use ACKs and then `WriteRequestEnd` ACK |
| `request_sealed` | ThreadRun owner fiber | typed seal reconciliation |
| `waiting_for_tool_results` | provider-request tool-fiber set or durable pending routes | every named terminal Tool Result ACK |
| `ready_to_finish` | ThreadRun owner fiber | `FinishIdle` ACK gates local idle |

The Thread-turn state is an internal typed contract, never a public status enum.
Cold reconstruction consumes the same durable facts that ACK application uses:
Message sequence defines pending user-side input, Request and Tool Events define
the active request, and internal repairs carry one direct repair identity. It
never compares Message and Event sequence coordinate systems or reconstructs a
Message mutation history. The reducer then selects the next action from the
checkpoint and the separately validated Tool-route view.
Every run exit settles its scope exactly once, by exactly one writer with
disjoint triggers (`FinishIdle`, terminal commit, pod-loss repair, or the
cooperative cancellation closeout for internal child scopes on a healthy pod) —
with two registered, record-bearing exceptions from the closeout-failure path:
the unrepairable release and the in-place-restart shutdown release. Both release
the scope to a durable custodian rather than settling at this exit, so the
durable row may remain `running` with no live run until pod identity changes or
an operator acts. These two residuals are the only cases where a run exit does
not itself settle.

### Durable-ACK gates

Durable ACKs gate every hot-state mutation; Bridge calls that gate durable
state are awaited Effects, never detached background work.

| RPC | Hot mutation it gates |
| --- | --- |
| `CommitInputs` | apply the caller-held accepted-input context drafts at the Bridge-assigned sequences after the committed result (including idempotent replay) |
| `WriteEvent` | apply the closed event result and its optional Assistant context append; open or resolve pending waits |
| `WriteRequestEnd` | validate current custody even when no Assistant draft exists; otherwise seal the open draft. An interrupt during an open provider request also applies the identity-matched input commit returned by the same transaction before acknowledging the interrupt; only then update `lastRequestUsage` and close the request turn |
| `FinishIdle` | enter local idle (after output capture / status) |
| `CommitRuntimeTermination` | under the current durable-turn identity, persist loop-authored current-thread cancellations and any abnormal child completion envelope; apply the closed termination result before removing pending tools or releasing the turn |

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
- Stream events echo the request identity and arrive well-formed: fragments in
  order, one terminal event, each tool call at most once per id; any violation
  closes the turn as a protocol error and discards uncommitted drafts.
- A validated terminal is held until the Gateway gRPC stream reaches normal
  EOF. Runtime adapts grpc-js through a Web reader and owns one typed completion
  latch; EOF without a terminal, transport failure after a terminal, consumer
  cancellation, or expiry of the request timeout plus the fixed 10-second
  transport-completion allowance cannot be mistaken for success. Every
  non-EOF exit cancels the reader and generated call before the latch settles.
- The stable `tool-call` is the execution boundary; `tool-input` fragments start
  nothing.
- The next request cannot start until the stream is terminal, the request end is
  ACKed, its terminal assistant projection is installed in hot context, and the
  per-turn tool-fiber set has settled. A rescheduled request carries only parts
  already proven durable; successful closeout adds the request's complete stable
  reasoning set in the same settlement.
- The pod is the only retry driver: a retryable failure parks in-run until the
  Bridge-effective deadline and re-issues the request rebuilt from committed
  context, or settles as retries-exhausted.
- The reviewer model and provider credentials are platform-owned; Gateway injects
  credentials but never chooses or replaces the model.
- Media attachments obey `MaxProviderRequestAttachments` = 32
  (`core/src/thread-loop/thread-state.ts`). Runtime admits at most that many
  attachments into a pending request ride; it does not synthesize model-only
  advisory messages for attachments outside the ride.
- Attachment consumption is settled-output-only: the pending media rides the
  request and is consumed only when that request end settles. A failed,
  interrupted, or rescheduled request consumes nothing, and the same media
  re-rides the next attempt (a rescheduled request end cannot consume
  attachments).

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
| `gateway` | `RunWeb` | web-connector container |
| `gateway` | `RunMcpTool` | mcp-connector container |
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
  threads are created through Bridge `CreateSubagentThread`; Bridge derives and stores the thread context prefix;
  `fork_turns` partitioning is `core/src/runtime/conversation-turns.ts`.
- Lifecycle: `spawn_agent` prepares a durable child row and context prefix before the
  first message; `send_message` resolves the child by `task_name`, delivering
  every instruction through the stored envelope and durable Runtime input rail;
  `wait_agent`, `interrupt_agent`, `close_agent`, `resume_agent`, and
  `list_agents` operate over durable `session_threads`.

Invariants a replacement must preserve:

- Child durable thread and context prefix exist before the initial message; a crash
  after a duplicate `CreateSubagentThread` result reuses the same Bridge-owned child and prefix.
- Inter-agent delivery is exactly-once by `delivery_id`, ordered
  sent envelope → received source/inbox → Runtime command → committed input
  result. Pod-loss reconciliation hands an accepted input back to the existing
  queue job or creates one replacement only after exact Runtime custody is lost.
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

Every inbound method-specific request must select this exact pod UID, carry a
non-empty current binding id and a non-zero binding generation, and authenticate
through TokenReview to the closed RPC set; a mismatch is a retryable rejection,
never a processed command. The Runtime Pod does not accept or echo Pod
namespace, name, or IP as command payload. Anchors:
`runtime-pod/src/command.ts` (dispatch), `runtime-pod/src/runtime-service.ts`
(scope binding), `runtime-pod/src/auth.ts` (TokenReview). The pod queries no
database, calls no sandbox provider, terminates no public HTTP, and holds no
secret material.

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
| `core/test/unit/thread-loop/thread-turn-checkpoint.test.ts`, `thread-turn-reducer.test.ts` | durable turn reconstruction and the closed transition table |
| `core/test/unit/thread-loop/tool-execution.test.ts`, `closeout.test.ts` | post-ACK tool execution, continuation, interruption, and settlement |
| `core/test/unit/thread-loop/compaction.test.ts` | proactive and reactive compaction lifecycle |
| `core/test/unit/thread-loop/provider-request.test.ts` | system-segment composition, tool-definition-only requests, attachment inclusion |
| `core/test/unit/llm-service.test.ts` | provider-stream ordering/identity validation and normalization |
| `core/test/unit/runtime-accumulator.test.ts` | per-turn `ProviderStreamAccumulator` framing that never leaks across turns |
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
| `runtime-pod/test/unit/bridge-client.test.ts` | Bridge RPC clients and input committers |
| `runtime-pod/test/integration/app.test.ts`, `gateway-capacity.test.ts` | full process wiring plus maximum-context projection, protobuf, real gRPC, and provider-lowering capacity proof |
| `protocol/test/unit/bounds.test.ts` | shared bound constants |

Production dependency factories never return fake, mock, or fallback clients;
tests inject fakes at the smallest useful boundary (network, time, ids,
storage).

If a PR changes the `run_slot` law, the loop algorithm, the request-turn
lifecycle, the compaction trigger or cycle, the tool family/gate/route rules,
the sub-agent delivery contract, or the command-validation surface in this
folder, it updates the matching section here.
