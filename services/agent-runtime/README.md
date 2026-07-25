# agent-runtime

The runtime's hot half: the TypeScript/Effect process that runs the agent
loop. Everything it holds lives in memory and is disposable by construction —
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
| core | `packages/core` | Agent Loop, hot-state model, tool system, provider/runtime contracts — no process, no transport |
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
| `ThreadEntry` | `session-state.ts` | one entry shape for `role = main \| subagent \| approval_reviewer`; `status`, `session_state`, `accepted_input_queue`, `control_queue`, `run_slot` | released whole on cleanup — no orphan fibers, timers, or maps survive |
| `ThreadRunSlot` | `session-manager.ts` | the single-owner run guard (below) | hot memory only; durable truth is `session_events` / `session_messages` / `session_pending_tool_uses` / `session_threads` |
| `SessionState` | `session-state.ts` | profile keys, installed tool definitions/routes/formatters, availability + approval policy, skill guidance index, pending attachments, last-request usage hint, held route-effective limits | reloaded on cold start; held limits and usage hint are never durable |
| `ContextManager` | `context-manager.ts` | hot `RuntimeMessage` state for one thread | appends only after durable ACK |
| `SessionProcessor` | `accumulator.ts` | the request-turn accumulator (assistant shell, part/tool maps, tool-use ids, terminal state) | created per provider turn at the Agent-Loop boundary, discarded when the turn settles; state never leaks across turns |
| `ToolJob` / `ToolScheduler` | `tool-scheduler.ts` | per-request-turn coordination over `toolJobs[]` | belongs to the active `RequestTurn`; reads no database, owns no Bridge |
| `AutoApprovalReviewerManager` | `approval-reviewer-manager.ts` | reviewer trunk + ephemeral sidecars, transcript feed cursor, last-committed snapshot, target-specific decision memo | recoverable hot state on the parent thread; durable truth is the trunk ledger |

Invariants a replacement must preserve:

1. Request-turn accumulation is scoped to exactly one provider turn and never
   leaks across turns.
2. Thread-scoped hot state is fully released with thread cleanup.
3. A thread serves no inbound command until its cold load — durable context,
   pending tool waits, background task handles — has completed; no fiber ever
   observes a half-hydrated entry.
4. Hot state is not the source of truth.

### `run_slot` — single-owner run guard

At most one owner run per thread. Many callers may join that owner; wake
signals coalesce while it is active.

| Field | Meaning |
| --- | --- |
| `run_id` / `owner_fiber` / `scope` / `done_deferred` | the active run, its scope, and the deferred that joined waiters await |
| `pending_wake` | coalescing flag (not a counter): any number of inputs arriving while active yield exactly one follow-up run |
| `pending_wake_after_stop` | input accepted after an interrupt fence; owner cleanup must not drop it |
| `stopping` | interrupt installed; the owner is unwinding |
| `start_reason` | `resume \| wake \| follow_up` |

| Event | Idle thread | Active, `stopping = false` | Active, `stopping = true` |
| --- | --- | --- | --- |
| `resume` | create `run_slot(resume)` | join `done_deferred` | await done, then retry `resume` |
| `wake` | create `run_slot(wake)` | set `pending_wake` | set `pending_wake_after_stop` |
| `interrupt` | mark accepted, start no provider request | `stopping = true`, clear `pending_wake`, interrupt owner, close scopes | already stopping |
| owner exits clean | — | if `pending_wake`: one follow-up run; else clear slot and complete `done_deferred` | if `pending_wake_after_stop`: clear `stopping`, start one follow-up after cleanup finalizers |

A successful `FinishIdle(end_turn)` ends the run even when input is queued; the
follow-up run for queued work is the next run. Intra-turn retries — reschedule,
overflow, compaction — stay inside the run. Cancelling a joined waiter never
cancels the owner. Different threads run concurrently while each thread stays
single-owner.

### The ThreadRun loop

One fixed algorithm per run: write the running status if the durable status was
idle, commit any accepted input (hot context appends only after the ACK), then
loop — handle an installed interrupt, enter `requires_action` when an external
wait remains, finish idle when nothing is pending, wait out a pending
reschedule until its Bridge-effective deadline, run the compaction cycle when
the trigger fires, otherwise run one request turn and sweep.

| Loop state | Owner | Durable boundary |
| --- | --- | --- |
| `accepting_input` | command handler + `SessionManager` | none until a later `CommitInputs`; the queue ACK is safe |
| `committing_input` | ThreadRun owner fiber | `CommitInputs` ACK gates hot context/control/tool mutation |
| `ready_to_request` | ThreadRun owner fiber | pure hot decision point |
| `compacting` | ThreadRun owner fiber | request-start, request-end, and `agent.thread_context_compacted` ACK |
| `provider_request_start` | ThreadRun owner fiber | `WriteEvent(span.model_request_start)` ACK is RequestTurn birth |
| `provider_streaming` | RequestTurn scope | Gateway stream; stable events still go through `WriteEvent` |
| `request_end_write` | RequestTurn scope | `WriteRequestEnd` ACK closes the turn and gates the usage hint |
| `tool_settlement` | RequestTurn tool-fiber set | every public tool use settles by `agent.tool_result` or an external wait |
| `waiting_external` | durable pending rows + hot ToolJobs | `FinishIdle(requires_action)` ACK; no live fiber owns the wait |
| `finish_idle` | ThreadRun owner fiber | `FinishIdle` ACK gates local idle |
| `stop_error` | ThreadRun owner fiber | terminal error / repair writes when required |

The loop state is an implementation contract for tests and logs, never a public
enum. Every run exit settles its scope exactly once, by exactly one writer with
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
| `CommitInputs` | append accepted messages to `ContextManager` |
| `WriteEvent` | update hot assistant/tool state; open or resolve pending waits |
| `WriteRequestEnd` | update `lastRequestUsage`; close the request turn |
| `FinishIdle` | enter local idle (after output capture / status) |

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
  `provider-call-assembly.ts`; the provider error codes are in
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
  bodies are never inlined. The segment is derived from the resolved skill index
  persisted at preparation and is never re-derived by re-resolving `latest`, so it
  always describes exactly the packages the sandbox mounted.
- Lifecycle: a RequestTurn is born at the `span.model_request_start` ACK; before
  that ACK no request end is owed. After it, every terminal success, provider
  error, cancellation, or repairable failure closes with `WriteRequestEnd`.

Invariants a replacement must preserve:

- `ProviderRequest.tools[]` carries definitions only — no route kind, RPC names,
  formatter identity, permission policy, sandbox binding, or credentials.
- Stream events echo the request identity and arrive well-formed: fragments in
  order, one terminal event, each tool call at most once per id; any violation
  closes the turn as a protocol error and discards uncommitted drafts.
- The stable `tool-call` is the execution boundary; `tool-input` fragments start
  nothing.
- The next request cannot start until the stream is terminal, the request end is
  ACKed, and the per-turn tool-fiber set has settled.
- The pod is the only retry driver: a retryable failure parks in-run until the
  Bridge-effective deadline and re-issues the request rebuilt from committed
  context, or settles as retries-exhausted.
- The reviewer model and provider credentials are platform-owned; Gateway injects
  credentials but never chooses or replaces the model.
- Media attachments obey `MaxProviderRequestAttachments` = 32
  (`core/src/session/session-state.ts`); the cap is enforced at Runtime
  pending-accumulation, not by a silent drop. Attachments past 32 are held as an
  overflow count and surface as a model-only transient note before request
  assembly. Attachments that expire before the Gateway request likewise become a
  model-only transient note, never a user-visible event.
- Attachment consumption is settled-output-only: the pending media rides the
  request and is consumed only when that request end settles. A failed,
  interrupted, or rescheduled request consumes nothing, and the same media
  re-rides the next attempt (a rescheduled request end cannot consume
  attachments).

Conformance tests: `core/test/unit/llm-service.test.ts`,
`core/test/unit/provider-call-assembly.test.ts`,
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
| `sandbox` | `RunTool` / `CommandIO` | Bridge (helper subcommand per tool) |
| `gateway` | `RunWeb` | web-connector container |
| `gateway` | `RunMcpTool` | mcp-connector container |
| `bridge` | `RunMemory` | Bridge |
| `subagent` | `spawn_agent` … `list_agents` | in-process child thread |

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

Sub-agent tools are the same Agent Loop under child thread ids. There is no
specialized sub-agent loop and no reviewer-only model-call path. The parent
thread sees tool use/result; child work stays child-thread-local.

- Interface: the `subagent` route operations dispatch in `tool-runner.ts`; child
  threads are created through Bridge `CreateChildThread` with a fork seed;
  `fork_turns` partitioning is `core/src/runtime/conversation-turns.ts`.
- Lifecycle: `spawn_agent` prepares a durable child row and fork seed before the
  first message; `send_message` resolves the child by `task_name`, delivering
  in-process when the child is co-resident and through Bridge when cold;
  `wait_agent`, `interrupt_agent`, `close_agent`, `resume_agent`, and
  `list_agents` operate over durable `session_threads`.

Invariants a replacement must preserve:

- Child durable thread and fork seed exist before the initial message; a crash
  after `CreateChildThread` ACK reuses the same child and seed.
- Inter-agent delivery is exactly-once by a deterministic `delivery_id`, ordered
  parent-sent → enqueue → child-received, and repaired by `delivery_id` on the
  hot path and inside pod-loss repair on the cold path.
- `task_name` is unique under the parent by durable constraint, never by
  serializing spawns in the scheduler.
- Completion return rides the durable wake/receipt rail: the child settlement
  writes completion mail plus a bare-poke wake job; the parent's `CommitInputs`
  writes `agent.thread_message_received` (replay-deduped by `delivery_id`); a
  `wait_agent` pull carries a settled child's outcome and writes the receipt in
  the same step so the push copy dedups away.
- `close_agent` cascades to the closed child's descendant subtree; a closed row
  wins against any concurrent status flip; `resume_agent` reactivates only
  `closed_for_runtime` rows.

Conformance tests: `core/test/unit/session-manager.test.ts`,
`core/test/unit/conversation-turns.test.ts`,
`core/test/unit/tool-system.test.ts`.

### Command boundary

Every inbound command must name this exact pod — namespace, name, UID (and IP
when configured) — carry a non-empty binding id and a non-zero binding
generation, and authenticate through TokenReview to the closed command set; a
mismatch is a retryable rejection, never a processed command. Anchors:
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
| `core/test/unit/session-manager.test.ts` | `run_slot` single-owner, `wake` coalescing, `resume` join, interrupt/stop fences, concurrent distinct threads, sub-agent delivery and lifecycle |
| `core/test/unit/agent-loop.test.ts` | the ThreadRun loop algorithm and state transitions |
| `core/test/unit/session-state.test.ts` | thread hot-state shape and release |
| `core/test/unit/context-loader.test.ts` | cold load of durable context, pending waits, and background handles |
| `core/test/unit/provider-call-assembly.test.ts` | system-segment composition, tool-definition-only requests, attachment inclusion |
| `core/test/unit/llm-service.test.ts` | provider-stream ordering/identity validation and normalization |
| `core/test/unit/runtime-accumulator.test.ts` | per-turn `SessionProcessor` accumulation that never leaks across turns |
| `core/test/unit/session-event-writer.test.ts`, `runtime-message-projection.test.ts` | `WriteEvent` projection whitelist and hot-state updates after ACK |
| `core/test/unit/turn-retry-budget.test.ts` | provider and compaction reschedule budgets |
| `core/test/unit/tool-system.test.ts` | `evaluateToolGate` decisions, `runPolicy` serialization/parallelism, invalid-tool repair, approval routing |
| `core/test/unit/approval-reviewer-manager.test.ts` | reviewer trunk/sidecar selection, cursor and snapshot succession, decision memo |
| `core/test/unit/conversation-turns.test.ts` | `fork_turns` turn partitioning |
| `core/test/unit/session-run-static-boundaries.test.ts`, `static-boundaries.test.ts` | import confinement and dependency boundaries |
| `runtime-pod/test/unit/command.test.ts`, `runtime-service.test.ts` | command-envelope validation and scope binding |
| `runtime-pod/test/unit/auth.test.ts` | TokenReview identity and the closed command set |
| `runtime-pod/test/unit/gateway-client.test.ts` | the Gateway provider-stream client |
| `runtime-pod/test/unit/tool-runner.test.ts` | route dispatch across sandbox / gateway / bridge / subagent |
| `runtime-pod/test/unit/bridge-client.test.ts` | Bridge RPC clients and input committers |
| `runtime-pod/test/integration/app.test.ts` | full process wiring through `gateway-transport-harness` and `grpc-harness` |
| `protocol/test/unit/bounds.test.ts` | shared bound constants |

Production dependency factories never return fake, mock, or fallback clients;
tests inject fakes at the smallest useful boundary (network, time, ids,
storage).

If a PR changes the `run_slot` law, the loop algorithm, the request-turn
lifecycle, the compaction trigger or cycle, the tool family/gate/route rules,
the sub-agent delivery contract, or the command-validation surface in this
folder, it updates the matching section here.
