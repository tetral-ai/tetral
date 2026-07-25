/**
 * @packageDocumentation
 * Owns in-pod session/thread residency and the single-owner ThreadRun lifecycle:
 * SessionEntry -> ThreadEntry -> run_slot, driving each thread's Agent Loop.
 * SessionRunHost calls this service for every Runtime command; it calls AgentLoop,
 * SessionState, reviewer coordination, tool coordination, and Effect fiber primitives.
 *
 * OWNS:
 *   - SessionEntry / ThreadEntry / ThreadRunSlot hot maps;
 *   - the per-session tool concurrency coordinator;
 *   - wake/interrupt/preload/cleanup handling and wait/join observation over run_slot.
 *
 * STATE MACHINE: the ThreadRunSlot single-owner coordinator (see the table above the
 * interface below); the concrete ThreadRun states live in agent-loop.ts.
 *
 * INVARIANTS (hot state, binding):
 *   1. Request-turn accumulation state is scoped to exactly one provider turn and
 *      never leaks across turns (the SessionProcessor accumulator is per-turn).
 *   2. Thread-scoped hot state is fully released with its thread-entry cleanup — no
 *      orphaned fibers, timers, or maps survive the thread.
 *   3. A thread does not serve inbound commands until its cold load (durable context,
 *      pending tool uses, background handles) has completed.
 *   4. Hot state is not the source of truth and stays recoverable from durable state.
 *
 * UPDATE-WITH: services/agent-runtime/packages/core/src/agent-loop/agent-loop.ts,
 *              services/agent-runtime/packages/core/src/session/session-state.ts,
 *              services/bridge/bridge_api_store.go
 */
import { Context, Deferred, Effect, Exit, Fiber, Layer, Option, Scope } from "effect";
import * as AgentLoop from "../agent-loop/agent-loop.js";
import * as Session from "./session.js";
import type {
  RuntimeAcceptedThreadMetadataState,
  RuntimeAcceptedInputState,
  RuntimeInterAgentAcceptedInputState,
  RuntimeConfigPatchState,
  SessionCurrentModel,
  RuntimeThreadRoleState,
  RuntimeThreadPreloadState,
  RuntimeThreadStatusState,
  RuntimeTaskNotificationState,
  RuntimeThreadControlState,
  RuntimeThreadVisibilityState,
  RuntimeToolConfirmationState,
  RuntimeInterruptInputCommit,
} from "./session-state.js";
import type { RuntimeMessage } from "../contracts/runtime.js";
import { NoopRuntimeMetricsSink } from "../runtime/metrics.js";
import type { RuntimeHotStateMetrics, RuntimeMetricsSink } from "../runtime/metrics.js";
import { AutoApprovalReviewerManager } from "./approval-reviewer-manager.js";
import type { ReviewerExecutionToken } from "./approval-reviewer-manager.js";
import { SessionToolCoordinator } from "../tools/tool-scheduler.js";

export type { ReviewerExecutionToken } from "./approval-reviewer-manager.js";

type LocalSessionRejectionReason = "local_session_capacity_exceeded";
type CleanupRejectionReason = "session_busy";
type ControlRejectionReason = LocalSessionRejectionReason | "control_busy" | "control_conflict" | "context_load_failed";
type SubAgentRejectionReason = LocalSessionRejectionReason | "thread_not_receivable" | "thread_busy" | "reviewer_execution_unavailable";
type ThreadRole = RuntimeThreadRoleState;
type ThreadVisibility = RuntimeThreadVisibilityState;
type ThreadStatus = RuntimeThreadStatusState;

/** Fenced interrupt command; active runs close in-run, while idle ingress commits directly. */
export interface RuntimeInterruptControlCommand extends RuntimeThreadControlState {}

/** Fenced command that releases one session's disposable hot state. */
export interface RuntimeCleanupSessionCommand extends RuntimeThreadControlState {}

/** Outcome of an interrupt command after its run-slot and input-commit closeout. */
export type InterruptControlResult =
  | {
      readonly ok: true;
      readonly sessionId: string;
      readonly created: boolean;
      readonly interrupted: boolean;
      readonly idleInterrupt: boolean;
    }
  | {
      readonly ok: false;
      readonly sessionId: string;
      readonly reason: LocalSessionRejectionReason | "control_busy" | "context_load_failed";
    };

/** Common applied, duplicate, conflict, or capacity result for hot control commands. */
export type RuntimeControlResult =
  | {
      readonly ok: true;
      readonly sessionId: string;
      readonly created: boolean;
      readonly applied: boolean;
    }
  | {
      readonly ok: false;
      readonly sessionId: string;
      readonly reason: ControlRejectionReason;
    };

/** Result of admitting input and starting or waking its thread run. */
export type AcceptInputResult =
  | {
      readonly ok: true;
      readonly sessionId: string;
      readonly created: boolean;
      readonly started: boolean;
      readonly pendingWake: boolean;
      readonly reviewerExecutionToken?: ReviewerExecutionToken | undefined;
    }
  | {
      readonly ok: false;
      readonly sessionId: string;
      readonly reason: SubAgentRejectionReason;
    };

/** Result of a thread preload, interrupt, activation, or close operation. */
export type RunExitOutcome = "completed_clean" | "interrupt_applied" | "failed_closeout";

export type ThreadLifecycleResult =
  | {
      readonly ok: true;
      readonly sessionId: string;
      readonly sessionThreadId: string;
      readonly applied: boolean;
      readonly runExitOutcome?: RunExitOutcome | undefined;
    }
  | {
      readonly ok: false;
      readonly sessionId: string;
      readonly sessionThreadId: string;
      readonly reason: LocalSessionRejectionReason | "thread_busy" | "context_load_failed";
    };

/** Observation result for joining a resident thread with an optional timeout. */
export type ThreadWaitResult =
  | {
      readonly ok: true;
      readonly sessionId: string;
      readonly sessionThreadId: string;
      readonly observed: boolean;
      readonly status?: RuntimeThreadStatusState | undefined;
      readonly timedOut: boolean;
    }
  | {
      readonly ok: false;
      readonly sessionId: string;
      readonly sessionThreadId: string;
      readonly reason: LocalSessionRejectionReason | "thread_busy";
    };

/** Snapshot of one resident thread's status and committed hot message view. */
export type ThreadSnapshotResult =
  | {
      readonly ok: true;
      readonly sessionId: string;
      readonly sessionThreadId: string;
      readonly observed: boolean;
      readonly status?: RuntimeThreadStatusState | undefined;
      readonly hasPendingApprovalToolJobs?: boolean | undefined;
      readonly messages: readonly RuntimeMessage[];
    }
  | {
      readonly ok: false;
      readonly sessionId: string;
      readonly sessionThreadId: string;
      readonly reason: LocalSessionRejectionReason | "thread_busy";
    };

type ReviewerExecutionRejectionReason = "thread_busy" | "reviewer_execution_mismatch";

/** Token-fenced outcome of cancelling one concrete reviewer execution. */
export type ReviewerExecutionControlResult =
  | {
      readonly ok: true;
      readonly sessionId: string;
      readonly sessionThreadId: string;
      readonly applied: boolean;
      readonly terminal: boolean;
    }
  | {
      readonly ok: false;
      readonly sessionId: string;
      readonly sessionThreadId: string;
      readonly reason: ReviewerExecutionRejectionReason;
    };

/** Token-fenced wait result for one concrete reviewer execution. */
export type ReviewerExecutionWaitResult =
  | {
      readonly ok: true;
      readonly sessionId: string;
      readonly sessionThreadId: string;
      readonly status?: RuntimeThreadStatusState | undefined;
      readonly terminal: boolean;
      readonly timedOut: boolean;
    }
  | {
      readonly ok: false;
      readonly sessionId: string;
      readonly sessionThreadId: string;
      readonly reason: ReviewerExecutionRejectionReason;
    };

/** Token-fenced committed hot snapshot available only after reviewer completion. */
export type ReviewerExecutionSnapshotResult =
  | {
      readonly ok: true;
      readonly sessionId: string;
      readonly sessionThreadId: string;
      readonly observed: true;
      readonly status: RuntimeThreadStatusState;
      readonly messages: readonly RuntimeMessage[];
    }
  | {
      readonly ok: false;
      readonly sessionId: string;
      readonly sessionThreadId: string;
      readonly reason: ReviewerExecutionRejectionReason;
    };

/** Result of releasing an idle session's complete resident hot-state tree. */
export type CleanupSessionResult =
  | {
      readonly ok: true;
      readonly sessionId: string;
      readonly cleaned: boolean;
    }
  | {
      readonly ok: false;
      readonly sessionId: string;
      readonly reason: CleanupRejectionReason;
    };

/** Command surface that exclusively owns resident session and thread lifecycles. */
export interface Interface {
  readonly acceptInput: (command: RuntimeAcceptedInputState) => Effect.Effect<AcceptInputResult>;
  readonly interruptControl: (sessionId: string, command: RuntimeInterruptControlCommand, commitInput: RuntimeInterruptInputCommit) => Effect.Effect<InterruptControlResult>;
  readonly resolveToolConfirmation: (sessionId: string, command: RuntimeToolConfirmationState) => Effect.Effect<RuntimeControlResult>;
  readonly commitTaskNotification: (sessionId: string, command: RuntimeTaskNotificationState) => Effect.Effect<RuntimeControlResult>;
  readonly applyRuntimeConfigPatch: (sessionId: string, command: RuntimeConfigPatchState) => Effect.Effect<RuntimeControlResult>;
  readonly cleanupSession: (sessionId: string, command: RuntimeCleanupSessionCommand) => Effect.Effect<CleanupSessionResult>;
  readonly preloadThread: (command: RuntimeThreadPreloadState) => Effect.Effect<ThreadLifecycleResult>;
  readonly interruptThread: (command: RuntimeThreadControlState) => Effect.Effect<ThreadLifecycleResult>;
  readonly interruptReviewerExecution: (command: RuntimeThreadControlState, token: ReviewerExecutionToken) => Effect.Effect<ReviewerExecutionControlResult>;
  readonly markThreadClosed: (command: RuntimeThreadControlState) => Effect.Effect<ThreadLifecycleResult>;
  readonly markThreadActive: (command: RuntimeThreadControlState) => Effect.Effect<ThreadLifecycleResult>;
  readonly waitThread: (command: RuntimeThreadControlState, timeoutMs: number | undefined) => Effect.Effect<ThreadWaitResult>;
  readonly markAgentMailPulled: (command: RuntimeThreadControlState, deliveryId: string) => Effect.Effect<ThreadLifecycleResult>;
  readonly waitReviewerExecution: (command: RuntimeThreadControlState, token: ReviewerExecutionToken, timeoutMs: number | undefined) => Effect.Effect<ReviewerExecutionWaitResult>;
  readonly inspectThread: (command: RuntimeThreadControlState) => Effect.Effect<ThreadSnapshotResult>;
  readonly inspectReviewerExecution: (command: RuntimeThreadControlState, token: ReviewerExecutionToken) => Effect.Effect<ReviewerExecutionSnapshotResult>;
  readonly shutdownActiveRuns: () => Effect.Effect<void>;
}

/** Dependencies and local capacity limits used to construct SessionManager. */
export interface LayerOptions {
  readonly maxLocalSessions: number;
  readonly maxConcurrentTools?: number | undefined;
  readonly now: () => string;
  readonly metrics?: RuntimeMetricsSink | undefined;
  readonly refreshRuntimeBindingToken?: (
    identity: Session.RuntimeSessionIdentity,
    options?: { readonly force?: boolean | undefined },
  ) => Promise<string>;
  readonly loadPendingAgentMail?: (
    command: RuntimeThreadControlState,
  ) => Promise<readonly RuntimeInterAgentAcceptedInputState[]>;
  readonly loadThreadMessages?: (command: RuntimeThreadControlState) => Promise<readonly RuntimeMessage[]>;
  readonly registerAcceptedInput?: (input: RuntimeAcceptedInputState) => () => void;
  readonly closeoutMonotonicMs?: (() => number) | undefined;
  readonly closeoutSleep?: ((durationMs: number, signal: AbortSignal) => Promise<boolean>) | undefined;
  readonly recordCloseoutEvent?: ((event: RuntimeCloseoutEvent) => void) | undefined;
}

export const CloseoutRetryInitialBackoffMs = 1_000;
export const CloseoutRetryMaxBackoffMs = 60_000;
export const CloseoutStalledAlarmThresholdMs = 120_000;

/** Bounded process-local observation emitted for a failed-run closeout episode. */
export interface RuntimeCloseoutEvent {
  readonly event:
    | "runtime_closeout_stalled"
    | "runtime_closeout_recovered"
    | "runtime_closeout_unrepairable";
  readonly activeCloseouts: number;
  readonly errorCode?: "schema_mismatch" | "ack_mismatch" | "unrepairable" | undefined;
}

/** Effect service tag for resident session and thread coordination. */
export class Service extends Context.Service<Service, Interface>()("tetral-agent/SessionManager") {}

// ThreadRunSlot: single-owner run guard for one ThreadEntry (OpenCode coordinator
// pattern). At most one owner ThreadRun per thread key; many callers may join that
// owner, and wakes coalesce while it runs. run_slot is hot memory only — durable truth
// stays in session_events / session_messages / session_pending_tool_uses / session_threads.
//
//   | field                | meaning                                                       |
//   | -------------------- | ------------------------------------------------------------- |
//   | ownerFiber           | the one ThreadRun owner fiber (forked once per run)           |
//   | doneDeferred         | joiners await this; completed when the run settles            |
//   | scope                | run scope, closed on finish or direct lifecycle cancellation |
//   | pendingWake          | coalescing flag (NOT a counter): many inputs -> one follow-up |
//   | pendingWakeAfterStop | wake accepted during interrupt cleanup; must not be dropped   |
//   | stopping             | an interrupt is unwinding this run                            |
//
// wake/interrupt transitions:
//   | trigger        | run_slot empty            | active (stopping=false)          | active (stopping=true)        |
//   | -------------- | ------------------------- | -------------------------------- | ----------------------------- |
//   | wake/new input | start run_slot(wake)      | set pendingWake=true             | set pendingWakeAfterStop=true |
//   | interrupt      | fiber no-op (nothing runs)| stopping=true, clear pendingWake,| already stopping              |
//   |                |                           | signal in-run closeout, await owner                              |
//
// On a successful owner exit: pendingWake -> start exactly one follow-up ThreadRun;
// else pendingWakeAfterStop -> clear stopping and start one follow-up after cleanup;
// else clear run_slot and complete doneDeferred. Cold context load completes BEFORE
// the ThreadEntry is inserted into SessionEntry.threads. Cleanup cannot release a
// ThreadEntry while run_slot is active except on the approved interrupt/cleanup closeout.
// UPDATE-WITH: services/agent-runtime/packages/core/src/agent-loop/agent-loop.ts
interface ThreadRunSlot {
  readonly runId: number;
  ownerFiber: Fiber.Fiber<AgentLoop.AgentLoopRunResult, unknown> | undefined;
  readonly doneDeferred: Deferred.Deferred<AgentLoop.AgentLoopRunResult, unknown>;
  readonly scope: Scope.Scope;
  pendingWake: boolean;
  pendingWakeAfterStop: boolean;
  stopping: boolean;
  readonly reviewerExecutionToken: ReviewerExecutionToken | undefined;
}

interface ThreadEntry {
  readonly sessionThreadId: string;
  readonly parentThreadId: string | undefined;
  readonly role: ThreadRole;
  readonly visibility: ThreadVisibility;
  readonly taskName: string | undefined;
  readonly agentType: string | undefined;
  status: ThreadStatus;
  readonly session: Session.Session;
  readonly acceptedInputQueue: Session.Session["state"];
  readonly controlQueue: Session.Session["state"];
  readonly statusSignal: Session.Session["state"];
  runSlot: ThreadRunSlot | undefined;
  lastCompletedReviewerExecutionToken: ReviewerExecutionToken | undefined;
  refreshContextAfterRun: boolean;
  readonly approvalReviewer: AutoApprovalReviewerManager | undefined;
}

interface SessionEntry {
  workspaceId: string;
  readonly sessionId: string;
  bindingId: string;
  bindingGeneration: number;
  // Residency map only — the threads currently hot in this pod. NOT a durable
  // parent-child index: parent-child discovery, closed-child lookup, and resume
  // eligibility all come from Bridge reads over session_threads, never from this map.
  // A thread absent here is not closed, only not resident.
  // UPDATE-WITH: services/bridge/bridge_api_store.go
  readonly threads: Map<string, ThreadEntry>;
  readonly toolCoordinator: SessionToolCoordinator;
  readonly runtimeShutdown: RuntimeShutdownObservation;
}

interface RuntimeShutdownObservation {
  readonly wait: Promise<void>;
  requested: boolean;
  request: () => void;
}

/** Builds SessionManager with one residency map and one owner fiber per active thread. */
export function layer(options: LayerOptions): Layer.Layer<Service, never, AgentLoop.Service> {
  return Layer.effect(
    Service,
    Effect.gen(function* () {
      const agentLoop = yield* AgentLoop.Service;
      const scope = yield* Scope.Scope;
      const sessions = new Map<string, SessionEntry>();
      const metrics = options.metrics ?? NoopRuntimeMetricsSink;
      const closeoutMonotonicMs = options.closeoutMonotonicMs ?? (() => Date.now());
      const closeoutSleep = options.closeoutSleep ?? defaultCloseoutSleep;
      let nextRunId = 0;
      let nextCloseoutId = 0;
      const activeCloseouts = new Map<number, { readonly startedAt: number; stalled: boolean }>();
      let closeoutStallLatched = false;

      const sessionKey = (workspaceId: string, sessionId: string): string => `${workspaceId}\u0000${sessionId}`;
      const identitySessionKey = (identity: Session.RuntimeSessionIdentity): string => sessionKey(identity.workspaceId, identity.sessionId);
      const commandSessionKey = (command: { readonly workspaceId: string; readonly sessionId: string }): string =>
        sessionKey(command.workspaceId, command.sessionId);
      const entrySessionKey = (entry: SessionEntry): string => sessionKey(entry.workspaceId, entry.sessionId);
      const hotStateMetrics = (): RuntimeHotStateMetrics => {
        let activeThreads = 0;
        let activeFibers = 0;
        let pendingApprovals = 0;
        for (const sessionEntry of sessions.values()) {
          activeThreads += sessionEntry.threads.size;
          for (const threadEntry of sessionEntry.threads.values()) {
            if (threadEntry.runSlot !== undefined) {
              activeFibers += 1;
            }
            pendingApprovals += threadEntry.session.state.pendingApprovalToolJobs().length;
          }
        }
        return {
          activeSessions: sessions.size,
          activeThreads,
          activeFibers,
          pendingApprovals,
        };
      };

      const recordHotStateMetrics = (): void => {
        metrics.recordHotState(hotStateMetrics());
      };

      const allocateRunId = (): number => {
        nextRunId += 1;
        return nextRunId;
      };

      const beginFailedRunCloseout = (): number => {
        nextCloseoutId += 1;
        activeCloseouts.set(nextCloseoutId, {
          startedAt: closeoutMonotonicMs(),
          stalled: false,
        });
        return nextCloseoutId;
      };

      const stalledCloseoutCount = (): number =>
        [...activeCloseouts.values()].filter((state) => state.stalled).length;

      const containedRecordCloseoutEvent = (event: RuntimeCloseoutEvent): void => {
        try {
          options.recordCloseoutEvent?.(event);
        } catch {
          // Observability cannot become a fourth closeout disposition.
        }
      };

      const observeFailedRunCloseoutStall = (closeoutId: number): void => {
        const now = closeoutMonotonicMs();
        for (const state of activeCloseouts.values()) {
          if (now - state.startedAt >= CloseoutStalledAlarmThresholdMs) {
            state.stalled = true;
          }
        }
        if (closeoutStallLatched || stalledCloseoutCount() === 0) {
          return;
        }
        const current = activeCloseouts.get(closeoutId);
        if (current !== undefined) {
          current.stalled = true;
        }
        closeoutStallLatched = true;
        containedRecordCloseoutEvent({
          event: "runtime_closeout_stalled",
          activeCloseouts: stalledCloseoutCount(),
        });
      };

      const completeFailedRunCloseout = (closeoutId: number): void => {
        activeCloseouts.delete(closeoutId);
        if (closeoutStallLatched && stalledCloseoutCount() === 0) {
          closeoutStallLatched = false;
          containedRecordCloseoutEvent({
            event: "runtime_closeout_recovered",
            activeCloseouts: stalledCloseoutCount(),
          });
        }
      };

      const waitForFailedRunCloseoutRetry = async (
        sessionEntry: SessionEntry,
        backoffMs: number,
      ): Promise<"elapsed" | "shutdown"> => {
        if (sessionEntry.runtimeShutdown.requested) {
          return "shutdown";
        }
        const controller = new AbortController();
        const outcome = await Promise.race([
          closeoutSleep(backoffMs, controller.signal).then(() => "elapsed" as const),
          sessionEntry.runtimeShutdown.wait.then(() => "shutdown" as const),
        ]);
        controller.abort();
        return outcome;
      };

      const createSessionEntry = (identity: Session.RuntimeSessionIdentity): SessionEntry => ({
        workspaceId: identity.workspaceId,
        sessionId: identity.sessionId,
        bindingId: identity.bindingId,
        bindingGeneration: identity.bindingGeneration,
        threads: new Map<string, ThreadEntry>(),
        toolCoordinator: new SessionToolCoordinator({ maxConcurrentTools: options.maxConcurrentTools ?? 8 }),
        runtimeShutdown: createRuntimeShutdownObservation(),
      });

      const createThreadEntry = (
        identity: Session.RuntimeSessionIdentity,
        toolCoordinator: SessionToolCoordinator,
        role: ThreadRole = "main",
        visibility: ThreadVisibility = "public",
        metadata: RuntimeAcceptedThreadMetadataState = {},
      ): ThreadEntry => {
        const resolvedRole = metadata.role ?? role;
        const resolvedVisibility = metadata.visibility ?? visibility;
        const approvalReviewer = resolvedVisibility === "public" && resolvedRole !== "approval_reviewer"
          ? new AutoApprovalReviewerManager()
          : undefined;
        const session = new Session.Session(identity, approvalReviewer, toolCoordinator);
        return {
          sessionThreadId: identity.sessionThreadId,
          parentThreadId: metadata.parentThreadId ?? identity.parentThreadId,
          role: resolvedRole,
          visibility: resolvedVisibility,
          taskName: metadata.taskName,
          agentType: metadata.agentType,
          status: metadata.status ?? "idle",
          session,
          acceptedInputQueue: session.state,
          controlQueue: session.state,
          statusSignal: session.state,
          runSlot: undefined,
          lastCompletedReviewerExecutionToken: undefined,
          refreshContextAfterRun: false,
          approvalReviewer,
        };
      };

      const getOrCreateSessionEntry = (
        identity: Session.RuntimeSessionIdentity,
      ): { readonly entry: SessionEntry; readonly created: boolean } | undefined => {
        const key = identitySessionKey(identity);
        const existing = sessions.get(key);
        if (existing !== undefined) {
          existing.bindingId = identity.bindingId;
          existing.bindingGeneration = identity.bindingGeneration;
          return { entry: existing, created: false };
        }
        if (sessions.size >= options.maxLocalSessions) {
          return undefined;
        }
        const entry = createSessionEntry(identity);
        sessions.set(key, entry);
        recordHotStateMetrics();
        return { entry, created: true };
      };

      const getOrCreateThreadEntry = (
        identity: Session.RuntimeSessionIdentity,
        metadata: RuntimeAcceptedThreadMetadataState = {},
      ):
        | {
            readonly sessionEntry: SessionEntry;
            readonly threadEntry: ThreadEntry;
            readonly sessionCreated: boolean;
            readonly threadCreated: boolean;
          }
        | undefined => {
        const sessionResult = getOrCreateSessionEntry(identity);
        if (sessionResult === undefined) {
          return undefined;
        }
        const existingThread = sessionResult.entry.threads.get(identity.sessionThreadId);
        if (existingThread !== undefined) {
          existingThread.session.updateIdentity({
            ...identity,
            parentThreadId: identity.parentThreadId ?? existingThread.session.identity.parentThreadId,
            threadRole: identity.threadRole ?? existingThread.session.identity.threadRole,
            runtimeBindingToken: existingThread.session.identity.runtimeBindingToken,
          });
          return {
            sessionEntry: sessionResult.entry,
            threadEntry: existingThread,
            sessionCreated: sessionResult.created,
            threadCreated: false,
          };
        }
        const threadEntry = createThreadEntry(
          identity,
          sessionResult.entry.toolCoordinator,
          metadata.role ?? "main",
          metadata.visibility ?? "public",
          metadata,
        );
        sessionResult.entry.threads.set(identity.sessionThreadId, threadEntry);
        recordHotStateMetrics();
        return {
          sessionEntry: sessionResult.entry,
          threadEntry,
          sessionCreated: sessionResult.created,
          threadCreated: true,
        };
      };

      const commandRuntimeBindingToken = (command: RuntimeThreadControlState): string =>
        sessions.get(commandSessionKey(command))?.threads.get(command.sessionThreadId)?.session.identity.runtimeBindingToken ?? "";

      const acceptedInputIdentity = (command: RuntimeAcceptedInputState): Session.RuntimeSessionIdentity => ({
        ...acceptedInputIdentityFromMetadata(command, acceptedInputThreadMetadata(command), commandRuntimeBindingToken(command)),
      });

      const controlIdentity = (command: RuntimeThreadControlState): Session.RuntimeSessionIdentity => ({
        workspaceId: command.workspaceId,
        sessionId: command.sessionId,
        sessionThreadId: command.sessionThreadId,
        bindingId: command.bindingId,
        bindingGeneration: command.bindingGeneration,
        targetPodUid: command.targetPodUid,
        runtimeBindingToken: commandRuntimeBindingToken(command),
      });

      const removeThreadEntry = (sessionEntry: SessionEntry, threadEntry: ThreadEntry): void => {
        threadEntry.approvalReviewer?.dispose();
        threadEntry.session.state.clear();
        threadEntry.runSlot = undefined;
        sessionEntry.threads.delete(threadEntry.sessionThreadId);
        const key = entrySessionKey(sessionEntry);
        if (sessionEntry.threads.size === 0 && sessions.get(key) === sessionEntry) {
          sessions.delete(key);
        }
        recordHotStateMetrics();
      };

      const releaseThreadEntry = (sessionEntry: SessionEntry, threadEntry: ThreadEntry): Effect.Effect<void> =>
        Effect.gen(function* () {
          const reviewerChildren = [...sessionEntry.threads.values()].filter(
            (childEntry) => childEntry.role === "approval_reviewer"
              && childEntry.parentThreadId === threadEntry.sessionThreadId,
          );
          for (const childEntry of reviewerChildren) {
            childEntry.status = "closed_for_runtime";
            childEntry.session.state.beginRuntimeShutdown();
            const childRunSlot = childEntry.runSlot;
            if (childRunSlot === undefined) {
              continue;
            }
            childRunSlot.stopping = true;
            childRunSlot.pendingWake = false;
            childRunSlot.pendingWakeAfterStop = false;
          }
          yield* Effect.all(
            reviewerChildren.map((childEntry) => {
              const childRunSlot = childEntry.runSlot;
              if (childRunSlot === undefined) {
                return Effect.void;
              }
              return Effect.gen(function* () {
                if (childRunSlot.ownerFiber !== undefined) {
                  yield* Fiber.interrupt(childRunSlot.ownerFiber).pipe(Effect.exit, Effect.asVoid);
                }
                yield* awaitRunSlot(childRunSlot);
              });
            }),
            { concurrency: "unbounded" },
          );
          for (const childEntry of reviewerChildren) {
            if (
              sessionEntry.threads.get(childEntry.sessionThreadId) === childEntry
              && childEntry.runSlot === undefined
            ) {
              removeThreadEntry(sessionEntry, childEntry);
            }
          }
          if (sessionEntry.threads.get(threadEntry.sessionThreadId) === threadEntry) {
            removeThreadEntry(sessionEntry, threadEntry);
          }
        });

      const clearSessionEntry = (sessionEntry: SessionEntry): void => {
        for (const threadEntry of sessionEntry.threads.values()) {
          threadEntry.approvalReviewer?.dispose();
          if (threadEntry.session.state.hasPendingApprovalToolJobs()) {
            threadEntry.session.state.clear();
          }
          threadEntry.runSlot = undefined;
        }
        sessionEntry.threads.clear();
        const key = entrySessionKey(sessionEntry);
        if (sessions.get(key) === sessionEntry) {
          sessions.delete(key);
        }
        recordHotStateMetrics();
      };

      const startThreadRun = (
        sessionEntry: SessionEntry,
        threadEntry: ThreadEntry,
        reviewId: string | undefined = undefined,
      ): Effect.Effect<boolean> => {
        if (threadEntry.runSlot !== undefined) {
          return Effect.succeed(false);
        }
        return Effect.gen(function* () {
          const runScope = yield* Scope.fork(scope);
          const runId = allocateRunId();
          const runSlot: ThreadRunSlot = {
            runId,
            ownerFiber: undefined,
            doneDeferred: yield* Deferred.make<AgentLoop.AgentLoopRunResult, unknown>(),
            scope: runScope,
            pendingWake: false,
            pendingWakeAfterStop: false,
            stopping: false,
            reviewerExecutionToken: reviewId === undefined
              ? undefined
              : { reviewId, reviewerThreadId: threadEntry.sessionThreadId, runId },
          };
          threadEntry.runSlot = runSlot;
          threadEntry.status = "running";
          recordHotStateMetrics();
          const run = agentLoop
            .run(threadEntry.session)
            .pipe(Scope.provide(runScope), Effect.onExit((exit) => finishThreadRun(sessionEntry, threadEntry, runSlot, exit)));
          const fiber = yield* Effect.forkIn(run, runScope);
          runSlot.ownerFiber = fiber;
          return true;
        });
      };

      const completeRunSlot = (
        runSlot: ThreadRunSlot,
        exit: Exit.Exit<AgentLoop.AgentLoopRunResult, unknown>,
      ): Effect.Effect<void> => Deferred.done(runSlot.doneDeferred, exit).pipe(Effect.exit, Effect.asVoid);

      const awaitRunSlot = (
        runSlot: ThreadRunSlot,
      ): Effect.Effect<Exit.Exit<AgentLoop.AgentLoopRunResult, unknown>> =>
        Deferred.await(runSlot.doneDeferred).pipe(Effect.exit);

      const closeRunScope = (
        runSlot: ThreadRunSlot,
        exit: Exit.Exit<AgentLoop.AgentLoopRunResult, unknown>,
      ): Effect.Effect<void> => Scope.close(runSlot.scope, exit).pipe(Effect.exit, Effect.asVoid);

      const acceptRegisteredInput = (
        input: RuntimeAcceptedInputState,
      ): Effect.Effect<AcceptInputResult> =>
        Effect.gen(function* () {
          const unregister = options.registerAcceptedInput?.(input);
          const result = yield* acceptInput(input);
          if (!result.ok) {
            unregister?.();
          }
          return result;
        });

      const rescanPendingCompletionMail = (
        threadEntry: ThreadEntry,
        runSlot: ThreadRunSlot,
        targetThreadId: string,
      ): Effect.Effect<void> =>
        Effect.gen(function* () {
          if (options.loadPendingAgentMail === undefined) {
            return;
          }
          const command: RuntimeThreadControlState = {
            requestId: `agent_mail_rescan:${targetThreadId}:${runSlot.runId}`,
            workspaceId: threadEntry.session.identity.workspaceId,
            sessionId: threadEntry.session.identity.sessionId,
            sessionThreadId: targetThreadId,
            bindingId: threadEntry.session.identity.bindingId,
            bindingGeneration: threadEntry.session.identity.bindingGeneration,
            targetPodUid: threadEntry.session.identity.targetPodUid,
            runtimeInputId: `agent_mail_rescan:${targetThreadId}:${runSlot.runId}`,
            eventIds: [],
            sequenceFrom: 0,
            sequenceTo: 0,
          };
          const pendingAgentMail = yield* Effect.promise(() => options.loadPendingAgentMail!(command)).pipe(
            Effect.catchCause(() => Effect.succeed([] as readonly RuntimeInterAgentAcceptedInputState[])),
          );
          for (const mail of pendingAgentMail) {
            yield* acceptRegisteredInput(mail).pipe(Effect.asVoid);
          }
        });

      const deliverPendingCompletionMail = (
        threadEntry: ThreadEntry,
        runSlot: ThreadRunSlot,
      ): Effect.Effect<void> =>
        threadEntry.role === "subagent" && threadEntry.parentThreadId !== undefined
          ? rescanPendingCompletionMail(threadEntry, runSlot, threadEntry.parentThreadId)
          : Effect.void;

      const settleFailedRunCloseout = (
        sessionEntry: SessionEntry,
        threadEntry: ThreadEntry,
        exit: Exit.Exit<AgentLoop.AgentLoopRunResult, unknown>,
      ): Effect.Effect<void> => {
        const closeoutId = beginFailedRunCloseout();
        const attempt = agentLoop.closeFailedRun(threadEntry.session, exit);
        return Effect.gen(function* () {
          let backoffMs = CloseoutRetryInitialBackoffMs;
          for (;;) {
            const result = yield* attempt;
            if (result.type === "landed" || result.type === "superseded") {
              return;
            }
            if (result.type === "unrepairable") {
              const current = activeCloseouts.get(closeoutId);
              if (current !== undefined) {
                current.stalled = true;
              }
              const errorCode =
                result.error.code === "schema_mismatch" || result.error.code === "ack_mismatch"
                  ? result.error.code
                  : "unrepairable";
              containedRecordCloseoutEvent({
                event: "runtime_closeout_unrepairable",
                activeCloseouts: stalledCloseoutCount(),
                errorCode,
              });
              return;
            }
            observeFailedRunCloseoutStall(closeoutId);
            const waited = yield* Effect.promise(() =>
              waitForFailedRunCloseoutRetry(sessionEntry, backoffMs)
            );
            if (waited === "shutdown") {
              return;
            }
            backoffMs = Math.min(backoffMs * 2, CloseoutRetryMaxBackoffMs);
          }
        }).pipe(
          Effect.ensuring(Effect.sync(() => completeFailedRunCloseout(closeoutId))),
        );
      };

      const finishThreadRun = (
        sessionEntry: SessionEntry,
        threadEntry: ThreadEntry,
        runSlot: ThreadRunSlot,
        exit: Exit.Exit<AgentLoop.AgentLoopRunResult, unknown>,
      ): Effect.Effect<void> =>
        Effect.gen(function* () {
          if (threadEntry.runSlot !== runSlot) {
            yield* closeRunScope(runSlot, exit);
            yield* completeRunSlot(runSlot, exit);
            return;
          }
          const committedMailReceipt = threadEntry.session.state.takeInterAgentMessageReceiptCommitted();
          yield* closeRunScope(runSlot, exit);
          if (sessionEntry.runtimeShutdown.requested) {
            yield* releaseThreadEntry(sessionEntry, threadEntry);
            yield* completeRunSlot(runSlot, exit);
            return;
          }
          if (Exit.hasFails(exit) || Exit.hasDies(exit)) {
            const releaseFailedRun = releaseThreadEntry(sessionEntry, threadEntry).pipe(
              Effect.ensuring(completeRunSlot(runSlot, exit)),
            );
            yield* Effect.gen(function* () {
              yield* settleFailedRunCloseout(sessionEntry, threadEntry, exit);
              yield* deliverPendingCompletionMail(threadEntry, runSlot);
            }).pipe(Effect.ensuring(releaseFailedRun));
            return;
          }
          if (runRequiresHotStateDiscard(exit)) {
            yield* deliverPendingCompletionMail(threadEntry, runSlot);
            yield* releaseThreadEntry(sessionEntry, threadEntry);
            yield* completeRunSlot(runSlot, exit);
            return;
          }
          if (threadEntry.status === "closed_for_runtime") {
            yield* releaseThreadEntry(sessionEntry, threadEntry);
            yield* completeRunSlot(runSlot, exit);
            return;
          }
          if (threadEntry.refreshContextAfterRun && options.loadThreadMessages !== undefined) {
            const refreshCommand: RuntimeThreadControlState = {
              requestId: `agent_mail_refresh:${threadEntry.sessionThreadId}:${runSlot.runId}`,
              workspaceId: threadEntry.session.identity.workspaceId,
              sessionId: threadEntry.session.identity.sessionId,
              sessionThreadId: threadEntry.sessionThreadId,
              bindingId: threadEntry.session.identity.bindingId,
              bindingGeneration: threadEntry.session.identity.bindingGeneration,
              targetPodUid: threadEntry.session.identity.targetPodUid,
              runtimeInputId: `agent_mail_refresh:${threadEntry.sessionThreadId}:${runSlot.runId}`,
              eventIds: [],
              sequenceFrom: 0,
              sequenceTo: 0,
            };
            const messages = yield* Effect.promise(() => options.loadThreadMessages!(refreshCommand)).pipe(
              Effect.map(Option.some),
              Effect.catchCause(() => Effect.succeed(Option.none())),
            );
            if (Option.isSome(messages)) {
              threadEntry.session.state.contextManager.replaceMessages(messages.value);
              threadEntry.refreshContextAfterRun = false;
            }
          }

          yield* deliverPendingCompletionMail(threadEntry, runSlot);
          const cleanExit = Exit.isSuccess(exit) && exit.value.type === "completed";
          if (cleanExit && committedMailReceipt) {
            yield* rescanPendingCompletionMail(threadEntry, runSlot, threadEntry.sessionThreadId);
          }
          const valueLevelFailed = Exit.isSuccess(exit) && exit.value.type === "failed";
          if (valueLevelFailed && threadEntry.session.state.hasQueuedInterAgentMessage()) {
            yield* releaseThreadEntry(sessionEntry, threadEntry);
            yield* completeRunSlot(runSlot, exit);
            return;
          }

          if (runSlot.stopping && runSlot.reviewerExecutionToken !== undefined) {
            threadEntry.session.state.discardQueuedApprovalReview(runSlot.reviewerExecutionToken.reviewId);
          }
          threadEntry.lastCompletedReviewerExecutionToken = runSlot.reviewerExecutionToken;
          threadEntry.runSlot = undefined;
          threadEntry.status = "idle";
          recordHotStateMetrics();
          const queuedInput = threadEntry.session.state.peekAcceptedInput() !== undefined;
          const pendingApproval = threadEntry.session.state.hasPendingApprovalToolJobs();
          const reviewerRun = runSlot.reviewerExecutionToken !== undefined;
          const shouldWakeAfterStop =
            runSlot.stopping &&
            !valueLevelFailed &&
            (
              (queuedInput && (!reviewerRun || runSlot.pendingWakeAfterStop)) ||
              (runSlot.pendingWakeAfterStop && pendingApproval)
            ) &&
            sessionEntry.threads.get(threadEntry.sessionThreadId) === threadEntry;
          const shouldWake =
            !runSlot.stopping &&
            cleanExit &&
            (
              (queuedInput && (!reviewerRun || runSlot.pendingWake)) ||
              (runSlot.pendingWake && pendingApproval)
            ) &&
            sessionEntry.threads.get(threadEntry.sessionThreadId) === threadEntry;

          if (shouldWakeAfterStop) {
            yield* startThreadRun(sessionEntry, threadEntry).pipe(Effect.asVoid);
          } else if (shouldWake) {
            yield* startThreadRun(sessionEntry, threadEntry).pipe(Effect.asVoid);
          }
          yield* completeRunSlot(runSlot, exit);
        });

      const acceptInput = (command: RuntimeAcceptedInputState): Effect.Effect<AcceptInputResult> =>
        Effect.gen(function* () {
          const sessionId = command.sessionId;
          const threadResult = getOrCreateThreadEntry(acceptedInputIdentity(command), acceptedInputThreadMetadata(command));
          if (threadResult === undefined) {
            return { ok: false, sessionId, reason: "local_session_capacity_exceeded" };
          }
          if (command.kind === "inter_agent_message" && threadResult.threadCreated) {
            removeThreadEntry(threadResult.sessionEntry, threadResult.threadEntry);
            return { ok: false, sessionId, reason: "thread_not_receivable" };
          }
          if (command.kind === "inter_agent_message" && !threadReceivable(threadResult.threadEntry)) {
            return { ok: false, sessionId, reason: "thread_not_receivable" };
          }
          if (command.kind === "approval_review" && threadResult.threadEntry.runSlot !== undefined) {
            return { ok: false, sessionId, reason: "thread_busy" };
          }
          const accepted = threadResult.threadEntry.acceptedInputQueue.enqueueAcceptedInput(command);
          if (accepted === "conflict") {
            return { ok: false, sessionId, reason: "local_session_capacity_exceeded" };
          }
          if (accepted === "duplicate") {
            if (command.kind === "approval_review") {
              return { ok: false, sessionId, reason: "reviewer_execution_unavailable" };
            }
            return { ok: true, sessionId, created: threadResult.sessionCreated, started: false, pendingWake: false };
          }
          const runSlot = threadResult.threadEntry.runSlot;
          if (runSlot !== undefined) {
            if (runSlot.stopping) {
              runSlot.pendingWakeAfterStop = true;
            } else {
              runSlot.pendingWake = true;
            }
            return { ok: true, sessionId, created: threadResult.sessionCreated, started: false, pendingWake: true };
          }
          const started = yield* startThreadRun(
            threadResult.sessionEntry,
            threadResult.threadEntry,
            command.kind === "approval_review" ? command.reviewId : undefined,
          );
          const reviewerExecutionToken = command.kind === "approval_review"
            ? threadResult.threadEntry.runSlot?.reviewerExecutionToken
            : undefined;
          if (command.kind === "approval_review" && reviewerExecutionToken === undefined) {
            return { ok: false, sessionId, reason: "reviewer_execution_unavailable" };
          }
          return {
            ok: true,
            sessionId,
            created: threadResult.sessionCreated,
            started,
            pendingWake: false,
            ...(reviewerExecutionToken === undefined ? {} : { reviewerExecutionToken }),
          };
        });

      const interruptControl = (
        sessionId: string,
        command: RuntimeInterruptControlCommand,
        commitInput: RuntimeInterruptInputCommit,
      ): Effect.Effect<InterruptControlResult> =>
        Effect.gen(function* () {
          const sessionEntry = sessions.get(commandSessionKey(command));
          const threadEntry = sessionEntry?.threads.get(command.sessionThreadId);
          if (sessionEntry === undefined || threadEntry === undefined) {
            return { ok: true, sessionId, created: false, interrupted: false, idleInterrupt: true };
          }
          threadEntry.session.updateIdentity(controlIdentity(command));
          const runSlot = threadEntry.runSlot;
          if (runSlot?.ownerFiber !== undefined) {
            let interruptCommitResult: Awaited<ReturnType<RuntimeInterruptInputCommit>> | undefined;
            let interruptCloseoutCompleted = false;
            const admitted = threadEntry.session.state.beginUserInterrupt(command, async () => {
              interruptCommitResult = await commitInput();
              return interruptCommitResult;
            }, () => {
              interruptCloseoutCompleted = true;
            });
            if (admitted === "conflict") {
              return { ok: false, sessionId, reason: "control_busy" };
            }
            runSlot.stopping = true;
            runSlot.pendingWake = false;
            threadEntry.session.state.discardQueuedAcceptedInputsBeforeFence(command.sequenceTo);
            yield* awaitRunSlot(runSlot);
            const committed = interruptCommitResult ?? threadEntry.session.state.userInterruptCommitResult(command.runtimeInputId);
            if (committed?.ok !== true) {
              return { ok: false, sessionId, reason: "context_load_failed" };
            }
            if (!interruptCloseoutCompleted && !threadEntry.session.state.userInterruptCloseoutCompleted(command.runtimeInputId)) {
              return { ok: false, sessionId, reason: "control_busy" };
            }
            return { ok: true, sessionId, created: false, interrupted: true, idleInterrupt: false };
          }
          threadEntry.session.state.clear();
          return { ok: true, sessionId, created: false, interrupted: false, idleInterrupt: true };
        });

      const resolveToolConfirmation = (
        sessionId: string,
        command: RuntimeToolConfirmationState,
      ): Effect.Effect<RuntimeControlResult> =>
        Effect.gen(function* () {
          const threadResult = getOrCreateThreadEntry(controlIdentity(command));
          if (threadResult === undefined) {
            return { ok: false, sessionId, reason: "local_session_capacity_exceeded" };
          }
          const result = threadResult.threadEntry.controlQueue.resolveToolConfirmation(command);
          if (result === "conflict") {
            return { ok: false, sessionId, reason: "control_conflict" };
          }
          if (threadResult.threadEntry.session.state.hasPendingApprovalToolJobs()) {
            const runSlot = threadResult.threadEntry.runSlot;
            if (runSlot !== undefined) {
              if (runSlot.stopping) {
                runSlot.pendingWakeAfterStop = true;
              } else {
                runSlot.pendingWake = true;
              }
            } else {
              yield* startThreadRun(threadResult.sessionEntry, threadResult.threadEntry).pipe(Effect.asVoid);
            }
          }
          return { ok: true, sessionId, created: threadResult.sessionCreated, applied: result === "applied" };
        });

      const commitTaskNotification = (
        sessionId: string,
        command: RuntimeTaskNotificationState,
      ): Effect.Effect<RuntimeControlResult> =>
        Effect.gen(function* () {
          const threadResult = getOrCreateThreadEntry(controlIdentity(command));
          if (threadResult === undefined) {
            return { ok: false, sessionId, reason: "local_session_capacity_exceeded" };
          }
          const result = threadResult.threadEntry.controlQueue.commitTaskNotification(command);
          if (result === "conflict") {
            return { ok: false, sessionId, reason: "control_conflict" };
          }
          return { ok: true, sessionId, created: threadResult.sessionCreated, applied: result === "applied" };
        });

      const applyRuntimeConfigPatch = (
        sessionId: string,
        command: RuntimeConfigPatchState,
      ): Effect.Effect<RuntimeControlResult> =>
        Effect.gen(function* () {
          const sessionEntry = sessions.get(commandSessionKey(command));
          if (sessionEntry === undefined || !sessionEntry.threads.has(command.sessionThreadId)) {
            return { ok: true, sessionId, created: false, applied: false };
          }
          if ([...sessionEntry.threads.values()].some((threadEntry) => threadEntry.runSlot !== undefined)) {
            return { ok: false, sessionId, reason: "control_busy" };
          }
          let applied = false;
          for (const threadEntry of sessionEntry.threads.values()) {
            threadEntry.session.updateIdentity({
              ...threadEntry.session.identity,
              bindingId: command.bindingId,
              bindingGeneration: command.bindingGeneration,
              targetPodUid: command.targetPodUid,
            });
            const threadApplied = threadEntry.session.state.applyRuntimeConfigPatch(command) === "applied";
            applied = threadApplied || applied;
            if (threadApplied && command.generation !== undefined && options.refreshRuntimeBindingToken !== undefined) {
              const runtimeBindingToken = yield* Effect.promise(() =>
                options.refreshRuntimeBindingToken?.(threadEntry.session.identity, { force: true }) ?? Promise.resolve(threadEntry.session.identity.runtimeBindingToken)
              );
              threadEntry.session.updateIdentity({ ...threadEntry.session.identity, runtimeBindingToken });
            }
          }
          return { ok: true, sessionId, created: false, applied };
        });

      const cleanupSession = (sessionId: string, command: RuntimeCleanupSessionCommand): Effect.Effect<CleanupSessionResult> =>
        Effect.gen(function* () {
          const entry = sessions.get(commandSessionKey(command));
          if (entry === undefined) {
            return { ok: true, sessionId, cleaned: false };
          }
          for (const threadEntry of entry.threads.values()) {
            if (threadEntry.runSlot !== undefined || threadEntry.session.state.acceptedInputCount() > 0) {
              return { ok: false, sessionId, reason: "session_busy" };
            }
          }
          clearSessionEntry(entry);
          return { ok: true, sessionId, cleaned: true };
        });

      const preloadThread = (command: RuntimeThreadPreloadState): Effect.Effect<ThreadLifecycleResult> =>
        Effect.gen(function* () {
          const metadata = command.thread ?? {};
          const identity: Session.RuntimeSessionIdentity = {
            workspaceId: command.workspaceId,
            sessionId: command.sessionId,
            sessionThreadId: command.sessionThreadId,
            ...(metadata.parentThreadId !== undefined ? { parentThreadId: metadata.parentThreadId } : {}),
            ...(metadata.role !== undefined ? { threadRole: metadata.role } : {}),
            bindingId: command.bindingId,
            bindingGeneration: command.bindingGeneration,
            targetPodUid: command.targetPodUid,
            runtimeBindingToken: command.runtimeBindingToken,
          };
          const threadResult = getOrCreateThreadEntry(identity, metadata);
          if (threadResult === undefined) {
            return { ok: false, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, reason: "local_session_capacity_exceeded" };
          }
          if (threadResult.threadEntry.runSlot !== undefined) {
            return { ok: false, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, reason: "thread_busy" };
          }
          threadResult.threadEntry.session.updateIdentity(identity);
          threadResult.threadEntry.session.state.contextManager.replaceMessages(command.messages);
          threadResult.threadEntry.session.state.markPersistentContextLoaded();
          if (command.runtimeConfigPatch !== undefined) {
            threadResult.threadEntry.session.state.applyRuntimeConfigPatch(command.runtimeConfigPatch);
          }
          for (const manifest of command.mcpManifests ?? []) {
            threadResult.threadEntry.session.state.applyRuntimeConfigPatch(manifest);
          }
          const currentModel = lastAcceptedUserMessageModel(command.messages);
          if (currentModel !== undefined) {
            threadResult.threadEntry.session.state.updateCurrentModel(currentModel);
          }
          threadResult.threadEntry.session.state.replacePendingAttachments(command.pendingAttachments ?? []);
          const pendingToolUseInstall = yield* agentLoop.installLoadedPendingToolUses(threadResult.threadEntry.session, command.pendingToolUses, command.messages);
          if (!pendingToolUseInstall.ok) {
            yield* releaseThreadEntry(threadResult.sessionEntry, threadResult.threadEntry);
            return { ok: false, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, reason: "context_load_failed" };
          }
          for (const backgroundTool of command.backgroundTools ?? []) {
            threadResult.threadEntry.session.state.recordBackgroundTool(backgroundTool);
          }
          threadResult.threadEntry.status = metadata.status ?? "idle";
          for (const mail of command.pendingAgentMail ?? []) {
            const accepted = yield* acceptRegisteredInput(mail);
            if (!accepted.ok) {
              yield* releaseThreadEntry(threadResult.sessionEntry, threadResult.threadEntry);
              return { ok: false, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, reason: "context_load_failed" };
            }
          }
          return { ok: true, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, applied: true };
        });

      const interruptThread = (command: RuntimeThreadControlState): Effect.Effect<ThreadLifecycleResult> =>
        Effect.gen(function* () {
          const sessionEntry = sessions.get(commandSessionKey(command));
          const threadEntry = sessionEntry?.threads.get(command.sessionThreadId);
          if (sessionEntry === undefined || threadEntry === undefined) {
            return { ok: true, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, applied: false };
          }
          threadEntry.session.updateIdentity(controlIdentity(command));
          const runSlot = threadEntry.runSlot;
          if (runSlot?.ownerFiber === undefined) {
            return { ok: true, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, applied: false };
          }
          runSlot.stopping = true;
          runSlot.pendingWake = false;
          yield* Fiber.interrupt(runSlot.ownerFiber).pipe(Effect.exit, Effect.asVoid);
          const runExit = yield* awaitRunSlot(runSlot);
          const runExitOutcome = classifyRunExitOutcome(runExit);
          return {
            ok: true,
            sessionId: command.sessionId,
            sessionThreadId: command.sessionThreadId,
            applied: runExitOutcome === "interrupt_applied",
            runExitOutcome,
          };
        });

      const interruptReviewerExecution = (
        command: RuntimeThreadControlState,
        token: ReviewerExecutionToken,
      ): Effect.Effect<ReviewerExecutionControlResult> =>
        Effect.gen(function* () {
          const threadEntry = sessions.get(commandSessionKey(command))?.threads.get(command.sessionThreadId);
          if (threadEntry === undefined || !reviewerTokenAddressesCommand(token, command)) {
            return reviewerExecutionRejection(command, "reviewer_execution_mismatch");
          }
          const runSlot = threadEntry.runSlot;
          if (runSlot === undefined) {
            return reviewerTokensEqual(threadEntry.lastCompletedReviewerExecutionToken, token)
              ? {
                  ok: true,
                  sessionId: command.sessionId,
                  sessionThreadId: command.sessionThreadId,
                  applied: false,
                  terminal: true,
                }
              : reviewerExecutionRejection(command, "reviewer_execution_mismatch");
          }
          if (!reviewerTokensEqual(runSlot.reviewerExecutionToken, token)) {
            return reviewerExecutionRejection(command, "reviewer_execution_mismatch");
          }
          if (runSlot.ownerFiber === undefined) {
            return reviewerExecutionRejection(command, "thread_busy");
          }
          runSlot.stopping = true;
          runSlot.pendingWake = false;
          runSlot.pendingWakeAfterStop = false;
          threadEntry.session.state.beginCooperativeCancel();
          yield* awaitRunSlot(runSlot);
          threadEntry.session.state.finishCooperativeCancel();
          return {
            ok: true,
            sessionId: command.sessionId,
            sessionThreadId: command.sessionThreadId,
            applied: true,
            terminal: true,
          };
        });

      const markThreadClosed = (command: RuntimeThreadControlState): Effect.Effect<ThreadLifecycleResult> =>
        Effect.gen(function* () {
          const sessionEntry = sessions.get(commandSessionKey(command));
          const threadEntry = sessionEntry?.threads.get(command.sessionThreadId);
          if (sessionEntry === undefined || threadEntry === undefined) {
            return { ok: true, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, applied: false };
          }
          threadEntry.session.updateIdentity(controlIdentity(command));
          const closing = new Set<ThreadEntry>([threadEntry]);
          let changed = true;
          while (changed) {
            changed = false;
            for (const candidate of sessionEntry.threads.values()) {
              if (
                candidate.parentThreadId !== undefined &&
                [...closing].some((parent) => parent.sessionThreadId === candidate.parentThreadId) &&
                !closing.has(candidate)
              ) {
                closing.add(candidate);
                changed = true;
              }
            }
          }
          const closingRunSlots = new Map<ThreadEntry, ThreadRunSlot>();
          for (const closingEntry of closing) {
            closingEntry.status = "closed_for_runtime";
            const runSlot = closingEntry.runSlot;
            if (runSlot?.ownerFiber === undefined) {
              continue;
            }
            closingRunSlots.set(closingEntry, runSlot);
            runSlot.stopping = true;
            runSlot.pendingWake = false;
            runSlot.pendingWakeAfterStop = false;
            closingEntry.session.state.beginCooperativeCancel();
          }
          let requestedRunExitOutcome: RunExitOutcome | undefined;
          for (const closingEntry of [...closing].reverse()) {
            const runSlot = closingRunSlots.get(closingEntry);
            if (runSlot !== undefined) {
              const runExit = yield* awaitRunSlot(runSlot);
              if (closingEntry === threadEntry) {
                requestedRunExitOutcome = classifyRunExitOutcome(runExit);
              }
            }
            if (sessionEntry.threads.get(closingEntry.sessionThreadId) === closingEntry) {
              yield* releaseThreadEntry(sessionEntry, closingEntry);
            }
          }
          return {
            ok: true,
            sessionId: command.sessionId,
            sessionThreadId: command.sessionThreadId,
            applied: true,
            ...(requestedRunExitOutcome === undefined ? {} : { runExitOutcome: requestedRunExitOutcome }),
          };
        });

      const markThreadActive = (command: RuntimeThreadControlState): Effect.Effect<ThreadLifecycleResult> =>
        Effect.gen(function* () {
          const sessionEntry = sessions.get(commandSessionKey(command));
          const threadEntry = sessionEntry?.threads.get(command.sessionThreadId);
          if (sessionEntry === undefined || threadEntry === undefined) {
            return { ok: true, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, applied: false };
          }
          if (threadEntry.runSlot !== undefined) {
            return { ok: false, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, reason: "thread_busy" };
          }
          threadEntry.session.updateIdentity(controlIdentity(command));
          threadEntry.status = "idle";
          return { ok: true, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, applied: true };
        });

      const waitThread = (command: RuntimeThreadControlState, timeoutMs: number | undefined): Effect.Effect<ThreadWaitResult> =>
        Effect.gen(function* () {
          const threadEntry = sessions.get(commandSessionKey(command))?.threads.get(command.sessionThreadId);
          if (threadEntry === undefined) {
            return {
              ok: true,
              sessionId: command.sessionId,
              sessionThreadId: command.sessionThreadId,
              observed: false,
              timedOut: false,
            };
          }
          const runSlot = threadEntry.runSlot;
          if (runSlot === undefined) {
            return {
              ok: true,
              sessionId: command.sessionId,
              sessionThreadId: command.sessionThreadId,
              observed: true,
              status: threadEntry.status,
              timedOut: false,
            };
          }
          if (timeoutMs === undefined) {
            yield* awaitRunSlot(runSlot);
            return {
              ok: true,
              sessionId: command.sessionId,
              sessionThreadId: command.sessionThreadId,
              observed: true,
              status: threadEntry.status,
              timedOut: false,
            };
          }
          const completed = yield* awaitRunSlot(runSlot).pipe(Effect.timeoutOption(`${Math.max(0, timeoutMs)} millis`));
          if (Option.isNone(completed)) {
            return {
              ok: true,
              sessionId: command.sessionId,
              sessionThreadId: command.sessionThreadId,
              observed: true,
              status: threadEntry.status,
              timedOut: true,
            };
          }
          return {
            ok: true,
            sessionId: command.sessionId,
            sessionThreadId: command.sessionThreadId,
            observed: true,
            status: threadEntry.status,
            timedOut: false,
          };
        });

      const markAgentMailPulled = (command: RuntimeThreadControlState, deliveryId: string): Effect.Effect<ThreadLifecycleResult> =>
        Effect.gen(function* () {
          const threadEntry = sessions.get(commandSessionKey(command))?.threads.get(command.sessionThreadId);
          if (threadEntry === undefined) {
            return { ok: true, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, applied: false };
          }
          threadEntry.session.state.discardAcceptedAgentMail(deliveryId);
          if (threadEntry.runSlot !== undefined) {
            threadEntry.refreshContextAfterRun = true;
            return { ok: true, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, applied: true };
          }
          if (options.loadThreadMessages !== undefined) {
            const messages = yield* Effect.promise(() => options.loadThreadMessages!(command));
            threadEntry.session.state.contextManager.replaceMessages(messages);
          }
          return { ok: true, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, applied: true };
        });

      const waitReviewerExecution = (
        command: RuntimeThreadControlState,
        token: ReviewerExecutionToken,
        timeoutMs: number | undefined,
      ): Effect.Effect<ReviewerExecutionWaitResult> =>
        Effect.gen(function* () {
          const threadEntry = sessions.get(commandSessionKey(command))?.threads.get(command.sessionThreadId);
          if (threadEntry === undefined || !reviewerTokenAddressesCommand(token, command)) {
            return reviewerExecutionRejection(command, "reviewer_execution_mismatch");
          }
          const runSlot = threadEntry.runSlot;
          if (runSlot === undefined) {
            return reviewerTokensEqual(threadEntry.lastCompletedReviewerExecutionToken, token)
              ? {
                  ok: true,
                  sessionId: command.sessionId,
                  sessionThreadId: command.sessionThreadId,
                  status: threadEntry.status,
                  terminal: true,
                  timedOut: false,
                }
              : reviewerExecutionRejection(command, "reviewer_execution_mismatch");
          }
          if (!reviewerTokensEqual(runSlot.reviewerExecutionToken, token)) {
            return reviewerExecutionRejection(command, "reviewer_execution_mismatch");
          }
          if (timeoutMs === undefined) {
            yield* awaitRunSlot(runSlot);
            return {
              ok: true,
              sessionId: command.sessionId,
              sessionThreadId: command.sessionThreadId,
              status: threadEntry.status,
              terminal: true,
              timedOut: false,
            };
          }
          const completed = yield* awaitRunSlot(runSlot).pipe(Effect.timeoutOption(`${Math.max(0, timeoutMs)} millis`));
          return {
            ok: true,
            sessionId: command.sessionId,
            sessionThreadId: command.sessionThreadId,
            status: threadEntry.status,
            terminal: Option.isSome(completed),
            timedOut: Option.isNone(completed),
          };
        });

      const inspectThread = (command: RuntimeThreadControlState): Effect.Effect<ThreadSnapshotResult> =>
        Effect.sync(() => {
          const threadEntry = sessions.get(commandSessionKey(command))?.threads.get(command.sessionThreadId);
          if (threadEntry === undefined) {
            return {
              ok: true,
              sessionId: command.sessionId,
              sessionThreadId: command.sessionThreadId,
              observed: false,
              messages: [],
            };
          }
          return {
            ok: true,
            sessionId: command.sessionId,
            sessionThreadId: command.sessionThreadId,
            observed: true,
            status: threadEntry.status,
            hasPendingApprovalToolJobs: threadEntry.session.state.hasPendingApprovalToolJobs(),
            messages: threadEntry.session.state.contextManager.messages(),
          };
        });

      const inspectReviewerExecution = (
        command: RuntimeThreadControlState,
        token: ReviewerExecutionToken,
      ): Effect.Effect<ReviewerExecutionSnapshotResult> =>
        Effect.sync(() => {
          const threadEntry = sessions.get(commandSessionKey(command))?.threads.get(command.sessionThreadId);
          if (threadEntry === undefined || !reviewerTokenAddressesCommand(token, command)) {
            return reviewerExecutionRejection(command, "reviewer_execution_mismatch");
          }
          if (threadEntry.runSlot !== undefined) {
            return reviewerTokensEqual(threadEntry.runSlot.reviewerExecutionToken, token)
              ? reviewerExecutionRejection(command, "thread_busy")
              : reviewerExecutionRejection(command, "reviewer_execution_mismatch");
          }
          if (!reviewerTokensEqual(threadEntry.lastCompletedReviewerExecutionToken, token)) {
            return reviewerExecutionRejection(command, "reviewer_execution_mismatch");
          }
          return {
            ok: true,
            sessionId: command.sessionId,
            sessionThreadId: command.sessionThreadId,
            observed: true,
            status: threadEntry.status,
            messages: threadEntry.session.state.contextManager.messages(),
          };
        });

      const shutdownActiveRuns = (): Effect.Effect<void> =>
        Effect.gen(function* () {
          const entries = [...sessions.values()];
          yield* Effect.all(entries.map((entry) => shutdownSessionEntry(entry)), { concurrency: "unbounded" });
        });

      const shutdownSessionEntry = (entry: SessionEntry): Effect.Effect<void> =>
        Effect.gen(function* () {
          entry.runtimeShutdown.request();
          const threads = [...entry.threads.values()];
          for (const threadEntry of threads) {
            threadEntry.session.state.beginRuntimeShutdown();
            const runSlot = threadEntry.runSlot;
            if (runSlot !== undefined) {
              runSlot.stopping = true;
              runSlot.pendingWake = false;
              runSlot.pendingWakeAfterStop = false;
            }
          }
          yield* Effect.all(
            threads.map((threadEntry) => {
              const runSlot = threadEntry.runSlot;
              if (runSlot === undefined) {
                return Effect.void;
              }
              const fiber = runSlot.ownerFiber;
              if (fiber === undefined) {
                return Effect.void;
              }
              return Effect.gen(function* () {
                yield* Fiber.interrupt(fiber).pipe(Effect.exit, Effect.asVoid);
                yield* awaitRunSlot(runSlot);
              });
            }),
            { concurrency: "unbounded" },
          );
          clearSessionEntry(entry);
        });

      return Service.of({
        acceptInput,
        applyRuntimeConfigPatch,
        cleanupSession,
        commitTaskNotification,
        preloadThread,
        interruptThread,
        interruptReviewerExecution,
        interruptControl,
        markThreadActive,
        markThreadClosed,
        markAgentMailPulled,
        resolveToolConfirmation,
        waitThread,
        waitReviewerExecution,
        inspectThread,
        inspectReviewerExecution,
        shutdownActiveRuns,
      });
    }),
  );
}

function runRequiresHotStateDiscard(exit: Exit.Exit<AgentLoop.AgentLoopRunResult, unknown>): boolean {
  if (Exit.hasFails(exit) || Exit.hasDies(exit)) {
    return true;
  }
  if (!Exit.isSuccess(exit)) {
    return false;
  }
  const result = exit.value;
  if (result.type === "interrupted" && result.discardHotState === true) {
    return true;
  }
  return result.type === "failed" && result.releaseSession !== undefined;
}

function classifyRunExitOutcome(
  exit: Exit.Exit<AgentLoop.AgentLoopRunResult, unknown>,
): RunExitOutcome {
  if (!Exit.isSuccess(exit)) {
    return Exit.hasFails(exit) || Exit.hasDies(exit) ? "failed_closeout" : "interrupt_applied";
  }
  switch (exit.value.type) {
    case "completed":
      return "completed_clean";
    case "interrupted":
      return "interrupt_applied";
    case "failed":
      return "failed_closeout";
  }
}

function reviewerTokenAddressesCommand(
  token: ReviewerExecutionToken,
  command: RuntimeThreadControlState,
): boolean {
  return token.reviewId.length > 0
    && token.reviewerThreadId === command.sessionThreadId
    && Number.isSafeInteger(token.runId)
    && token.runId > 0;
}

function reviewerTokensEqual(
  left: ReviewerExecutionToken | undefined,
  right: ReviewerExecutionToken,
): boolean {
  return left?.reviewId === right.reviewId
    && left.reviewerThreadId === right.reviewerThreadId
    && left.runId === right.runId;
}

function reviewerExecutionRejection(
  command: RuntimeThreadControlState,
  reason: ReviewerExecutionRejectionReason,
): Extract<ReviewerExecutionControlResult, { readonly ok: false }> {
  return {
    ok: false,
    sessionId: command.sessionId,
    sessionThreadId: command.sessionThreadId,
    reason,
  };
}

function acceptedInputThreadMetadata(input: RuntimeAcceptedInputState): RuntimeAcceptedThreadMetadataState {
  if (input.kind === "messages") {
    return {};
  }
  return input.thread ?? {};
}

function acceptedInputIdentityFromMetadata(
  command: RuntimeAcceptedInputState,
  metadata: RuntimeAcceptedThreadMetadataState,
  runtimeBindingToken: string,
): Session.RuntimeSessionIdentity {
  return {
    workspaceId: command.workspaceId,
    sessionId: command.sessionId,
    sessionThreadId: command.sessionThreadId,
    ...(metadata.parentThreadId !== undefined ? { parentThreadId: metadata.parentThreadId } : {}),
    ...(metadata.role !== undefined ? { threadRole: metadata.role } : {}),
    bindingId: command.bindingId,
    bindingGeneration: command.bindingGeneration,
    targetPodUid: command.targetPodUid,
    runtimeBindingToken,
  };
}

function threadReceivable(threadEntry: ThreadEntry): boolean {
  return threadEntry.role !== "approval_reviewer" &&
    threadEntry.status !== "closed_for_runtime" &&
    threadEntry.status !== "terminated" &&
    threadEntry.status !== "failed";
}

function createRuntimeShutdownObservation(): RuntimeShutdownObservation {
  let resolveWait: (() => void) | undefined;
  const wait = new Promise<void>((resolve) => {
    resolveWait = resolve;
  });
  return {
    wait,
    requested: false,
    request() {
      if (this.requested) {
        return;
      }
      this.requested = true;
      resolveWait?.();
    },
  };
}

async function defaultCloseoutSleep(durationMs: number, signal: AbortSignal): Promise<boolean> {
  return await new Promise<boolean>((resolve) => {
    if (signal.aborted) {
      resolve(false);
      return;
    }
    const timeout = setTimeout(() => resolve(true), durationMs);
    signal.addEventListener("abort", () => {
      clearTimeout(timeout);
      resolve(false);
    }, { once: true });
  });
}

function lastAcceptedUserMessageModel(messages: readonly RuntimeMessage[]): SessionCurrentModel | undefined {
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = messages[index];
    if (message?.role === "user" && message.providerId !== undefined && message.modelId !== undefined) {
      return { providerId: message.providerId, modelId: message.modelId };
    }
  }
  return undefined;
}
