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
import { Context, Deferred, Effect, Exit, Fiber, Layer, Option, Scope, Semaphore } from "effect";
import * as AgentLoop from "../agent-loop/agent-loop.js";
import * as Session from "./session.js";
import type {
  RuntimeAcceptedThreadMetadataState,
  RuntimeAcceptedInputState,
  RuntimeConfigPatchState,
  RuntimeThreadRoleState,
  RuntimeThreadPreloadState,
  RuntimeThreadStatusState,
  RuntimeTaskNotificationCommandState,
  RuntimeTaskNotificationCommit,
  RuntimeThreadControlState,
  RuntimeThreadVisibilityState,
  RuntimeToolConfirmationState,
  RuntimeControlInputCommit,
} from "./session-state.js";
import { normalizeRuntimeFailure } from "../contracts/runtime.js";
import type { RuntimeMessage } from "../contracts/runtime.js";
import { NoopRuntimeMetricsSink } from "../runtime/metrics.js";
import type { RuntimeHotStateMetrics, RuntimeMetricsSink } from "../runtime/metrics.js";
import { AutoApprovalReviewerManager } from "./approval-reviewer-manager.js";
import type { ReviewerExecutionToken } from "./approval-reviewer-manager.js";
import { SessionToolCoordinator } from "../tools/tool-scheduler.js";
import { SessionConfiguration } from "./session-configuration.js";
import { makeThreadCommandChannel } from "./thread-command-channel.js";
import type { ThreadCommandChannel } from "./thread-command-channel.js";

export type { ReviewerExecutionToken } from "./approval-reviewer-manager.js";

type LocalSessionRejectionReason = "local_session_capacity_exceeded";
type CleanupRejectionReason = "session_busy";
type ControlRejectionReason = LocalSessionRejectionReason | "control_busy" | "control_conflict" | "context_load_failed";
type SubAgentRejectionReason =
  | LocalSessionRejectionReason
  | "context_load_failed"
  | "control_conflict"
  | "thread_not_receivable"
  | "thread_busy"
  | "reviewer_execution_unavailable";
type ThreadRole = RuntimeThreadRoleState;
type ThreadVisibility = RuntimeThreadVisibilityState;
type ThreadStatus = RuntimeThreadStatusState;

/** Fenced interrupt command; active runs close in-run, while idle ingress commits directly. */
export interface RuntimeInterruptControlCommand extends RuntimeThreadControlState {}

/** Fenced command that releases one session's disposable hot state. */
export interface RuntimeCleanupSessionCommand extends RuntimeThreadControlState {}

export type { RuntimeTaskNotificationCommit } from "./session-state.js";

/** Outcome of an interrupt command after its run-slot and input-commit closeout. */
export type InterruptControlResult =
  | {
      readonly ok: true;
      readonly sessionId: string;
      readonly created: boolean;
      readonly interrupted: boolean;
      readonly idleInterrupt: boolean;
      readonly duplicate?: true | undefined;
      readonly stale?: true | undefined;
    }
  | {
      readonly ok: false;
      readonly sessionId: string;
      readonly reason: LocalSessionRejectionReason | "control_busy" | "context_load_failed";
      readonly retryable?: boolean | undefined;
      readonly errorCode?: string | number | undefined;
    };

/** Common applied, duplicate, conflict, or capacity result for hot control commands. */
export type RuntimeControlResult =
  | {
      readonly ok: true;
      readonly sessionId: string;
      readonly created: boolean;
      readonly applied: boolean;
      readonly noResidency?: true | undefined;
      readonly stale?: true | undefined;
    }
  | {
      readonly ok: false;
      readonly sessionId: string;
      readonly reason: ControlRejectionReason;
    };

/** Result of admitting a task notification to the owning thread's serialized input queue. */
export type RuntimeTaskNotificationResult = RuntimeControlResult;

/** Result of admitting input and starting or waking its thread run. */
export type AcceptInputResult =
  | {
      readonly ok: true;
      readonly sessionId: string;
      readonly created: boolean;
      readonly started: boolean;
      readonly pendingWake: boolean;
      readonly duplicate?: true | undefined;
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
  readonly interruptControl: (sessionId: string, command: RuntimeInterruptControlCommand, commitInput: RuntimeControlInputCommit) => Effect.Effect<InterruptControlResult>;
  readonly resolveToolConfirmation: (
    sessionId: string,
    command: RuntimeToolConfirmationState,
    commit: RuntimeControlInputCommit,
  ) => Effect.Effect<RuntimeControlResult>;
  readonly commitTaskNotification: (
    sessionId: string,
    command: RuntimeTaskNotificationCommandState,
    commit: RuntimeTaskNotificationCommit,
  ) => Effect.Effect<RuntimeTaskNotificationResult>;
  readonly applyRuntimeConfigPatch: (sessionId: string, command: RuntimeConfigPatchState) => Effect.Effect<RuntimeControlResult>;
  readonly cleanupSession: (sessionId: string, command: RuntimeCleanupSessionCommand) => Effect.Effect<CleanupSessionResult>;
  readonly preloadThread: (command: RuntimeThreadPreloadState) => Effect.Effect<ThreadLifecycleResult>;
  readonly ensureThreadInstalled: (
    command: RuntimeThreadControlState,
    options?: { readonly requirePendingApprovalToolJobs?: boolean | undefined },
  ) => Effect.Effect<ThreadLifecycleResult>;
  readonly interruptThread: (command: RuntimeThreadControlState) => Effect.Effect<ThreadLifecycleResult>;
  readonly interruptReviewerExecution: (command: RuntimeThreadControlState, token: ReviewerExecutionToken) => Effect.Effect<ReviewerExecutionControlResult>;
  readonly markThreadClosed: (command: RuntimeThreadControlState) => Effect.Effect<ThreadLifecycleResult>;
  readonly markThreadActive: (command: RuntimeThreadControlState) => Effect.Effect<ThreadLifecycleResult>;
  readonly waitThread: (command: RuntimeThreadControlState, timeoutMs: number | undefined) => Effect.Effect<ThreadWaitResult>;
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
  readonly loadThreadContext?: (
    command: RuntimeThreadControlState,
  ) => Promise<RuntimeThreadPreloadState>;
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
// else clear run_slot and complete doneDeferred. A cold thread is inserted in the
// installing state before its sole LoadContext call and becomes command-ready only
// after that install succeeds. Cleanup cannot release an installing ThreadEntry or a
// ThreadEntry with an active run_slot except on the approved closeout path.
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
  durableTurnId: string | undefined;
}

interface ThreadEntry {
  readonly sessionThreadId: string;
  readonly parentThreadId: string | undefined;
  readonly role: ThreadRole;
  readonly visibility: ThreadVisibility;
  readonly taskName: string | undefined;
  readonly agentType: string | undefined;
  status: ThreadStatus;
  installationState: "installing" | "ready";
  installation: ThreadInstallation | undefined;
  readonly session: Session.Session;
  readonly acceptedInputQueue: Session.Session["state"];
  readonly controlQueue: Session.Session["state"];
  readonly statusSignal: Session.Session["state"];
  readonly commandChannel: ThreadCommandChannel;
  runSlot: ThreadRunSlot | undefined;
  bridgeScope: RuntimeThreadControlState | undefined;
  durableTurnId: string | undefined;
  lastCompletedReviewerExecutionToken: ReviewerExecutionToken | undefined;
  readonly approvalReviewer: AutoApprovalReviewerManager | undefined;
}

interface ThreadInstallation {
  readonly start: Deferred.Deferred<void>;
  readonly result: Deferred.Deferred<ThreadLifecycleResult>;
  fiber: Fiber.Fiber<void, never> | undefined;
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
  readonly controlGate: Semaphore.Semaphore;
  readonly configuration: SessionConfiguration;
  // Exactly one cold thread initializes Session-scoped config and MCP state.
  // Sibling installs wait for that result and never promote their own snapshots.
  sharedStateStatus: "initializing" | "ready" | "failed";
  readonly sharedStateInitializerThreadId: string;
  readonly sharedStateReady: Promise<boolean>;
  readonly completeSharedStateReady: (ready: boolean) => void;
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
      const sessions = new Map<string, SessionEntry>();
      const metrics = options.metrics ?? NoopRuntimeMetricsSink;
      const closeoutMonotonicMs = options.closeoutMonotonicMs ?? (() => Date.now());
      const closeoutSleep = options.closeoutSleep ?? defaultCloseoutSleep;
      const installationScope = yield* Scope.make();
      let admissionClosed = false;
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

      const createSessionEntry = (identity: Session.RuntimeSessionIdentity): SessionEntry => {
        let completed = false;
        let completeSharedStateReady: (ready: boolean) => void = () => {};
        const sharedStateReady = new Promise<boolean>((resolve) => {
          completeSharedStateReady = (ready) => {
            if (!completed) {
              completed = true;
              resolve(ready);
            }
          };
        });
        return {
          workspaceId: identity.workspaceId,
          sessionId: identity.sessionId,
          bindingId: identity.bindingId,
          bindingGeneration: identity.bindingGeneration,
          threads: new Map<string, ThreadEntry>(),
          toolCoordinator: new SessionToolCoordinator({ maxConcurrentTools: options.maxConcurrentTools ?? 8 }),
          runtimeShutdown: createRuntimeShutdownObservation(),
          controlGate: Semaphore.makeUnsafe(1),
          configuration: new SessionConfiguration(),
          sharedStateStatus: "initializing",
          sharedStateInitializerThreadId: identity.sessionThreadId,
          sharedStateReady,
          completeSharedStateReady,
        };
      };

      const createThreadEntry = (
        identity: Session.RuntimeSessionIdentity,
        toolCoordinator: SessionToolCoordinator,
        role: ThreadRole = "main",
        visibility: ThreadVisibility = "public",
        metadata: RuntimeAcceptedThreadMetadataState = {},
        installationState: ThreadEntry["installationState"] = "ready",
      ): ThreadEntry => {
        const resolvedRole = metadata.role ?? role;
        const resolvedVisibility = metadata.visibility ?? visibility;
        const approvalReviewer = resolvedVisibility === "public" && resolvedRole !== "approval_reviewer"
          ? new AutoApprovalReviewerManager()
          : undefined;
        const sessionConfiguration = sessions.get(identitySessionKey(identity))?.configuration ?? new SessionConfiguration();
        const session = new Session.Session(identity, approvalReviewer, toolCoordinator, sessionConfiguration);
        return {
          sessionThreadId: identity.sessionThreadId,
          parentThreadId: metadata.parentThreadId ?? identity.parentThreadId,
          role: resolvedRole,
          visibility: resolvedVisibility,
          taskName: metadata.taskName,
          agentType: metadata.agentType,
          status: metadata.status ?? "idle",
          installationState,
          installation: undefined,
          session,
          acceptedInputQueue: session.state,
          controlQueue: session.state,
          statusSignal: session.state,
          commandChannel: Effect.runSync(makeThreadCommandChannel()),
          runSlot: undefined,
          bridgeScope: undefined,
          durableTurnId: undefined,
          lastCompletedReviewerExecutionToken: undefined,
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
        installationState: ThreadEntry["installationState"] = "ready",
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
            parentTaskName: identity.parentTaskName ?? existingThread.session.identity.parentTaskName,
            taskName: identity.taskName ?? existingThread.session.identity.taskName,
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
          installationState,
        );
        sessionResult.entry.threads.set(identity.sessionThreadId, threadEntry);
        if (sessionResult.created && installationState === "ready") {
          sessionResult.entry.sharedStateStatus = "ready";
          sessionResult.entry.completeSharedStateReady(true);
        }
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

      const submitThreadCommand = <CommandResult, ClosedResult>(
        threadEntry: ThreadEntry,
        command: Effect.Effect<CommandResult>,
        closedResult: ClosedResult,
      ): Effect.Effect<CommandResult | ClosedResult> =>
        threadEntry.commandChannel.submit(command).pipe(
          Effect.catchCause(() => Effect.succeed(closedResult)),
        );

      const removeThreadEntry = (sessionEntry: SessionEntry, threadEntry: ThreadEntry): Effect.Effect<void> =>
        threadEntry.commandChannel.close().pipe(
          Effect.andThen(threadEntry.approvalReviewer?.dispose() ?? Effect.void),
          Effect.ensuring(Effect.sync(() => {
            threadEntry.bridgeScope = undefined;
            threadEntry.session.state.clear();
            threadEntry.runSlot = undefined;
            sessionEntry.threads.delete(threadEntry.sessionThreadId);
            const key = entrySessionKey(sessionEntry);
            if (sessionEntry.threads.size === 0 && sessions.get(key) === sessionEntry) {
              sessions.delete(key);
            }
            recordHotStateMetrics();
          })),
        );

      const releaseThreadEntry = (sessionEntry: SessionEntry, threadEntry: ThreadEntry): Effect.Effect<void> =>
        Effect.gen(function* () {
          const residentChildren = [...sessionEntry.threads.values()].filter(
            (childEntry) => childEntry.parentThreadId === threadEntry.sessionThreadId,
          );
          for (const childEntry of residentChildren) {
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
            residentChildren.map((childEntry) => {
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
          for (const childEntry of residentChildren) {
            if (
              sessionEntry.threads.get(childEntry.sessionThreadId) === childEntry
              && childEntry.runSlot === undefined
            ) {
              yield* releaseThreadEntry(sessionEntry, childEntry);
            }
          }
          if (sessionEntry.threads.get(threadEntry.sessionThreadId) === threadEntry) {
            yield* removeThreadEntry(sessionEntry, threadEntry);
          }
        });

      const discardThreadEntryForStaleCustody = (
        sessionEntry: SessionEntry,
        threadEntry: ThreadEntry,
      ): Effect.Effect<void> =>
        Effect.gen(function* () {
          if (sessionEntry.threads.get(threadEntry.sessionThreadId) !== threadEntry) {
            return;
          }
          threadEntry.status = "closed_for_runtime";
          const runSlot = threadEntry.runSlot;
          if (runSlot?.ownerFiber !== undefined) {
            runSlot.stopping = true;
            runSlot.pendingWake = false;
            runSlot.pendingWakeAfterStop = false;
            yield* Fiber.interrupt(runSlot.ownerFiber).pipe(Effect.exit, Effect.asVoid);
            yield* awaitRunSlot(runSlot);
          }
          if (sessionEntry.threads.get(threadEntry.sessionThreadId) === threadEntry) {
            yield* releaseThreadEntry(sessionEntry, threadEntry);
          }
        });

      const clearSessionEntry = (sessionEntry: SessionEntry): Effect.Effect<void> =>
        Effect.forEach(
          [...sessionEntry.threads.values()],
          (threadEntry) => removeThreadEntry(sessionEntry, threadEntry),
          { concurrency: "unbounded", discard: true },
        );

      const startThreadRun = (
        sessionEntry: SessionEntry,
        threadEntry: ThreadEntry,
        reviewId: string | undefined = undefined,
      ): Effect.Effect<boolean> =>
        sessionEntry.controlGate.withPermit(Effect.gen(function* () {
          if (
            threadEntry.runSlot !== undefined ||
            threadEntry.installationState !== "ready" ||
            sessionEntry.sharedStateStatus !== "ready"
          ) {
            return false;
          }
          const runScope = yield* Scope.make();
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
            durableTurnId: threadEntry.durableTurnId,
          };
          threadEntry.runSlot = runSlot;
          threadEntry.status = "running";
          recordHotStateMetrics();
          const custody: AgentLoop.AgentLoopRunCustody = {
            durableTurnId: () => runSlot.durableTurnId,
            recordDurableTurnId: (durableTurnId) => {
              runSlot.durableTurnId = durableTurnId;
              threadEntry.durableTurnId = durableTurnId;
            },
            closeDurableTurn: (durableTurnId) => {
              if (runSlot.durableTurnId !== durableTurnId || threadEntry.durableTurnId !== durableTurnId) {
                throw new Error("durable turn closeout does not match run custody");
              }
              runSlot.durableTurnId = undefined;
              threadEntry.durableTurnId = undefined;
            },
          };
          const run = agentLoop
            .run(threadEntry.session, custody)
            .pipe(Scope.provide(runScope), Effect.onExit((exit) => finishThreadRun(sessionEntry, threadEntry, runSlot, exit)));
          const fiber = yield* Effect.forkIn(run, runScope);
          runSlot.ownerFiber = fiber;
          return true;
        }));

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

      const settleFailedRunCloseout = (
        sessionEntry: SessionEntry,
        threadEntry: ThreadEntry,
        exit: Exit.Exit<AgentLoop.AgentLoopRunResult, unknown>,
      ): Effect.Effect<void> => {
        const closeoutId = beginFailedRunCloseout();
        const custody: AgentLoop.AgentLoopRunCustody = {
          durableTurnId: () => threadEntry.runSlot?.durableTurnId ?? threadEntry.durableTurnId,
          recordDurableTurnId: (durableTurnId) => {
            threadEntry.durableTurnId = durableTurnId;
            if (threadEntry.runSlot !== undefined) {
              threadEntry.runSlot.durableTurnId = durableTurnId;
            }
          },
          closeDurableTurn: (durableTurnId) => {
            if (threadEntry.durableTurnId === durableTurnId) {
              threadEntry.durableTurnId = undefined;
            }
            if (threadEntry.runSlot?.durableTurnId === durableTurnId) {
              threadEntry.runSlot.durableTurnId = undefined;
            }
          },
        };
        const attempt = agentLoop.closeFailedRun(threadEntry.session, exit, custody);
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
            }).pipe(Effect.ensuring(releaseFailedRun));
            return;
          }
          if (runRequiresHotStateDiscard(exit)) {
            yield* releaseThreadEntry(sessionEntry, threadEntry);
            yield* completeRunSlot(runSlot, exit);
            return;
          }
          if (threadEntry.status === "closed_for_runtime") {
            yield* releaseThreadEntry(sessionEntry, threadEntry);
            yield* completeRunSlot(runSlot, exit);
            return;
          }
          const cleanExit = Exit.isSuccess(exit) && exit.value.type === "completed";
          const valueLevelFailed = Exit.isSuccess(exit) && exit.value.type === "failed";
          if (
            valueLevelFailed &&
            (
              threadEntry.session.state.hasQueuedInterAgentMessage() ||
              threadEntry.session.state.hasQueuedTaskNotification()
            )
          ) {
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
          const reviewerRun = runSlot.reviewerExecutionToken !== undefined;
          const shouldWakeAfterStop =
            runSlot.stopping &&
            !valueLevelFailed &&
            (
              (queuedInput && !reviewerRun) ||
              runSlot.pendingWakeAfterStop
            ) &&
            sessionEntry.threads.get(threadEntry.sessionThreadId) === threadEntry;
          const shouldWake =
            !runSlot.stopping &&
            cleanExit &&
            (
              (queuedInput && !reviewerRun) ||
              runSlot.pendingWake
            ) &&
            sessionEntry.threads.get(threadEntry.sessionThreadId) === threadEntry;

          if (shouldWakeAfterStop) {
            yield* startThreadRun(sessionEntry, threadEntry).pipe(Effect.asVoid);
          } else if (shouldWake) {
            yield* startThreadRun(sessionEntry, threadEntry).pipe(Effect.asVoid);
          }
          yield* completeRunSlot(runSlot, exit);
        });

      type ResidentThreadResult = Exclude<ReturnType<typeof getOrCreateThreadEntry>, undefined>;

      const submitInstalledThreadCommand = <CommandResult>(
        command: RuntimeThreadControlState,
        metadata: RuntimeAcceptedThreadMetadataState,
        run: (threadResult: ResidentThreadResult) => Effect.Effect<CommandResult>,
        unavailable: (reason: "local_session_capacity_exceeded" | "context_load_failed" | "thread_busy") => CommandResult,
      ): Effect.Effect<CommandResult> =>
        Effect.gen(function* () {
          const prepared = yield* prepareThreadInstallation(command, metadata);
          if (prepared.threadResult === undefined) {
            return unavailable(prepared.failedResult.reason);
          }
          const queued = yield* prepared.threadResult.sessionEntry.controlGate.withPermit(
            Effect.gen(function* () {
              if (
                prepared.threadResult.sessionEntry.threads.get(command.sessionThreadId) !==
                prepared.threadResult.threadEntry
              ) {
                return Option.none();
              }
              return yield* prepared.threadResult.threadEntry.commandChannel.enqueue(
                Effect.gen(function* () {
                  if (prepared.installation !== undefined) {
                    const installed = yield* Deferred.await(prepared.installation.result);
                    if (!installed.ok) {
                      return unavailable(installed.reason === "local_session_capacity_exceeded"
                        ? "local_session_capacity_exceeded"
                        : "context_load_failed");
                    }
                  }
                  return yield* run(prepared.threadResult);
                }),
              ).pipe(Effect.option);
            }),
          );
          if (Option.isNone(queued)) {
            return unavailable("thread_busy");
          }
          if (prepared.createdInstallation && prepared.installation !== undefined) {
            yield* Deferred.succeed(prepared.installation.start, undefined);
          }
          return yield* queued.value.pipe(
            Effect.catchCause(() => Effect.succeed(unavailable("thread_busy"))),
          );
        });

      const acceptInput = (command: RuntimeAcceptedInputState): Effect.Effect<AcceptInputResult> =>
        submitInstalledThreadCommand<AcceptInputResult>(
          command,
          acceptedInputThreadMetadata(command),
          (threadResult) => Effect.gen(function* () {
          const sessionId = command.sessionId;
          if (command.kind === "inter_agent_message" && !threadReceivable(threadResult.threadEntry)) {
            return { ok: false, sessionId, reason: "thread_not_receivable" };
          }
          if (command.kind === "approval_review" && threadResult.threadEntry.runSlot !== undefined) {
            return { ok: false, sessionId, reason: "thread_busy" };
          }
          const accepted = threadResult.threadEntry.acceptedInputQueue.enqueueAcceptedInput(command);
          if (accepted === "conflict") {
            return { ok: false, sessionId, reason: "control_conflict" } as const;
          }
          if (accepted === "duplicate") {
            if (command.kind === "approval_review") {
              return { ok: false, sessionId, reason: "reviewer_execution_unavailable" } as const;
            }
            threadResult.threadEntry.bridgeScope = command;
            const runSlot = threadResult.threadEntry.runSlot;
            const started = runSlot === undefined &&
              threadResult.threadEntry.session.state.peekAcceptedInput() !== undefined
              ? yield* startThreadRun(threadResult.sessionEntry, threadResult.threadEntry)
              : false;
            return {
              ok: true,
              sessionId,
              created: threadResult.sessionCreated,
              started,
              pendingWake: false,
              duplicate: true,
            } as const;
          }
          threadResult.threadEntry.bridgeScope = command;
          const runSlot = threadResult.threadEntry.runSlot;
          if (runSlot !== undefined) {
            if (runSlot.stopping) {
              runSlot.pendingWakeAfterStop = true;
            } else {
              runSlot.pendingWake = true;
            }
            return { ok: true, sessionId, created: threadResult.sessionCreated, started: false, pendingWake: true } as const;
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
            return { ok: false, sessionId, reason: "reviewer_execution_unavailable" } as const;
          }
          return {
            ok: true,
            sessionId,
            created: threadResult.sessionCreated,
            started,
            pendingWake: false,
            ...(reviewerExecutionToken === undefined ? {} : { reviewerExecutionToken }),
          } as const;
        }),
          (reason) => ({
            ok: false,
            sessionId: command.sessionId,
            reason: reason === "thread_busy" ? "thread_busy" : reason,
          } as AcceptInputResult),
        );

      const settleIdleInterrupt = (
        sessionId: string,
        command: RuntimeInterruptControlCommand,
        commitInput: RuntimeControlInputCommit,
        threadResult: ResidentThreadResult,
      ): Effect.Effect<InterruptControlResult> =>
        Effect.gen(function* () {
          const threadEntry = threadResult.threadEntry;
          if (threadEntry.session.state.userInterruptCloseoutCompleted(command.runtimeInputId)) {
            return {
              ok: true,
              sessionId,
              created: false,
              interrupted: false,
              idleInterrupt: true,
              duplicate: true,
            } as const;
          }
          const admitted = threadEntry.session.state.beginUserInterrupt(command, commitInput);
          if (admitted === "conflict") {
            return { ok: false, sessionId, reason: "control_busy" } as const;
          }
          if (command.eventIds.length !== 1) {
            return { ok: false, sessionId, reason: "context_load_failed" } as const;
          }
          const pendingTools = threadEntry.session.state.pendingApprovalToolJobs();
          const failure = normalizeRuntimeFailure({
            type: "runtime",
            code: "runtime_invalid_sequence",
            retryable: false,
            fatal: false,
            reason: "aborted",
            retryStatus: { type: "terminal" },
            sessionId,
          });
          const declaration = AgentLoop.interruptPendingToolDeclarations({
            workspaceId: command.workspaceId,
            sessionId,
            sessionThreadId: command.sessionThreadId,
            runtimeInputId: command.runtimeInputId,
            sourceEventId: command.eventIds[0]!,
            pendingTools,
            pendingSandboxExecutions: threadEntry.session.state.pendingSandboxExecutionJobs(),
            failure,
            completedAt: options.now(),
          });
          const application = yield* Effect.promise(() =>
            threadEntry.session.state.commitUserInterruptInput(declaration)
          );
          const committed = application.result;
          if (!committed.ok) {
            return { ok: false, sessionId, reason: "context_load_failed" } as const;
          }
          if ("receipt" in committed) {
            const messages = AgentLoop.applyInterruptInputReceipt({
              sessionId,
              sessionThreadId: command.sessionThreadId,
              runtimeInputId: command.runtimeInputId,
              eventIds: command.eventIds,
              drafts: application.declaration.drafts,
              pendingToolCancellations: application.declaration.pendingToolCancellations,
              sandboxExecutionToolUseEventIds: application.declaration.sandboxExecutionToolUseEventIds,
              existingMessages: threadEntry.session.state.contextManager.messages(),
            }, committed.receipt);
            threadEntry.session.state.contextManager.replaceMessages(messages);
            for (const cancellation of application.declaration.pendingToolCancellations) {
              if (threadEntry.session.state.pendingApprovalToolJobs().some(
                (pending) => pending.toolUseEventId === cancellation.toolUseEventId,
              )) {
                threadEntry.session.state.removePendingApprovalToolJob(cancellation.toolUseEventId);
                metrics.addPendingApprovals(-1);
              }
            }
          }
          threadEntry.session.state.completeUserInterrupt(command.runtimeInputId);
          if ("stale" in committed) {
            return {
              ok: true,
              sessionId,
              created: false,
              interrupted: false,
              idleInterrupt: true,
              stale: true,
            } as const;
          }
          return { ok: true, sessionId, created: false, interrupted: false, idleInterrupt: true } as const;
        });

      const interruptControl = (
        sessionId: string,
        command: RuntimeInterruptControlCommand,
        commitInput: RuntimeControlInputCommit,
      ): Effect.Effect<InterruptControlResult> =>
        Effect.gen(function* () {
          const sessionEntry = sessions.get(commandSessionKey(command));
          const threadEntry = sessionEntry?.threads.get(command.sessionThreadId);
          if (sessionEntry === undefined || threadEntry === undefined) {
            let installedTarget: ResidentThreadResult | undefined;
            const result = yield* submitInstalledThreadCommand<
              InterruptControlResult
            >(
              command,
              {},
              (threadResult) => {
                installedTarget = threadResult;
                threadResult.threadEntry.session.updateIdentity(controlIdentity(command));
                return settleIdleInterrupt(sessionId, command, commitInput, threadResult);
              },
              (reason) => ({
                ok: false,
                sessionId,
                reason: reason === "thread_busy" ? "control_busy" : reason,
              }),
            );
            if (
              result.ok &&
              "stale" in result &&
              installedTarget !== undefined &&
              installedTarget.sessionEntry.threads.get(command.sessionThreadId) === installedTarget.threadEntry
            ) {
              yield* discardThreadEntryForStaleCustody(installedTarget.sessionEntry, installedTarget.threadEntry);
            }
            return result;
          }
          threadEntry.session.updateIdentity(controlIdentity(command));
          if (threadEntry.session.state.userInterruptCloseoutCompleted(command.runtimeInputId)) {
            return {
              ok: true,
              sessionId,
              created: false,
              interrupted: false,
              idleInterrupt: true,
              duplicate: true,
            };
          }
          const runSlot = threadEntry.runSlot;
          if (runSlot?.ownerFiber !== undefined) {
            let interruptCommitResult: Awaited<ReturnType<RuntimeControlInputCommit>> | undefined;
            let interruptCloseoutCompleted = false;
            const admitted = yield* submitThreadCommand(
              threadEntry,
              Effect.sync(() => {
                if (runSlot.stopping) {
                  return "conflict" as const;
                }
                threadEntry.bridgeScope = command;
                const result = threadEntry.session.state.beginUserInterrupt(command, async (declaration) => {
                  interruptCommitResult = await commitInput(declaration);
                  return interruptCommitResult;
                }, () => {
                  interruptCloseoutCompleted = true;
                });
                if (result === "applied") {
                  runSlot.stopping = true;
                  runSlot.pendingWake = false;
                  threadEntry.session.state.discardQueuedAcceptedInputsBeforeFence(command.sequenceTo);
                }
                return result;
              }),
              "conflict" as const,
            );
            if (admitted === "conflict") {
              if (sessions.get(commandSessionKey(command))?.threads.get(command.sessionThreadId) !== threadEntry) {
                return { ok: false, sessionId, reason: "context_load_failed" };
              }
              yield* awaitRunSlot(runSlot);
              if (sessions.get(commandSessionKey(command))?.threads.get(command.sessionThreadId) !== threadEntry) {
                return { ok: false, sessionId, reason: "control_busy" };
              }
              return yield* interruptControl(sessionId, command, commitInput);
            }
            yield* Fiber.interrupt(runSlot.ownerFiber).pipe(Effect.exit, Effect.asVoid);
            yield* awaitRunSlot(runSlot);
            const committed = interruptCommitResult ?? threadEntry.session.state.userInterruptCommitResult(command.runtimeInputId);
            if (committed?.ok !== true) {
              return {
                ok: false,
                sessionId,
                reason: "context_load_failed",
                ...(committed === undefined
                  ? {}
                  : { retryable: committed.retryable, errorCode: committed.errorCode }),
              };
            }
            if ("stale" in committed) {
              yield* discardThreadEntryForStaleCustody(sessionEntry, threadEntry);
              return {
                ok: true,
                sessionId,
                created: false,
                interrupted: true,
                idleInterrupt: false,
                stale: true,
              };
            }
            if (!interruptCloseoutCompleted && !threadEntry.session.state.userInterruptCloseoutCompleted(command.runtimeInputId)) {
              return { ok: false, sessionId, reason: "control_busy" };
            }
            return { ok: true, sessionId, created: false, interrupted: true, idleInterrupt: false };
          }
          const result = yield* submitThreadCommand(
            threadEntry,
            settleIdleInterrupt(sessionId, command, commitInput, {
              sessionEntry,
              threadEntry,
              sessionCreated: false,
              threadCreated: false,
            }),
            { ok: false, sessionId, reason: "control_busy" } as const,
          );
          if (
            result.ok &&
            "stale" in result &&
            sessionEntry.threads.get(command.sessionThreadId) === threadEntry
          ) {
            yield* discardThreadEntryForStaleCustody(sessionEntry, threadEntry);
          }
          return result;
        });

      const resolveToolConfirmation = (
        sessionId: string,
        command: RuntimeToolConfirmationState,
        commit: RuntimeControlInputCommit,
      ): Effect.Effect<RuntimeControlResult> =>
        Effect.gen(function* () {
          let staleTarget: ResidentThreadResult | undefined;
          const result = yield* submitInstalledThreadCommand<RuntimeControlResult>(
            command,
            {},
            (threadResult) => Effect.gen(function* () {
              const existingConfirmation = threadResult.threadEntry.session.state.toolConfirmation(command.toolUseEventId);
              if (existingConfirmation?.runtimeInputId === command.runtimeInputId) {
                return {
                  ok: true,
                  sessionId,
                  created: threadResult.sessionCreated,
                  applied: false,
                } as const;
              }
              const pendingTool = threadResult.threadEntry.session.state.pendingApprovalToolJobs()
                .find((pending) => pending.toolUseEventId === command.toolUseEventId);
              if (
                pendingTool === undefined ||
                command.eventIds.length !== 1 ||
                command.eventIds[0] !== command.sourceEventId
              ) {
                return { ok: false, sessionId, reason: "control_conflict" } as const;
              }
              const draft = AgentLoop.toolConfirmationDraft({
                workspaceId: command.workspaceId,
                sessionId,
                sessionThreadId: command.sessionThreadId,
                runtimeInputId: command.runtimeInputId,
                sourceEventId: command.sourceEventId,
                toolUseEventId: command.toolUseEventId,
                pendingTool,
                decision: command.decision,
                ...(command.denyMessage === undefined ? {} : { denyMessage: command.denyMessage }),
              });
              const committed = yield* Effect.promise(() => commit({
                drafts: [draft],
                pendingToolCancellations: [],
                sandboxExecutionToolUseEventIds: [],
              }));
              if (!committed.ok) {
                return { ok: false, sessionId, reason: "context_load_failed" } as const;
              }
              if ("stale" in committed) {
                staleTarget = threadResult;
                return {
                  ok: true,
                  sessionId,
                  created: threadResult.sessionCreated,
                  applied: false,
                  stale: true,
                } as const;
              }
              if (!("receipt" in committed)) {
                return { ok: false, sessionId, reason: "control_conflict" } as const;
              }
              const messages = AgentLoop.applyToolConfirmationReceipt({
                sessionId,
                sessionThreadId: command.sessionThreadId,
                runtimeInputId: command.runtimeInputId,
                sourceEventId: command.sourceEventId,
                draft,
                existingMessages: threadResult.threadEntry.session.state.contextManager.messages(),
              }, committed.receipt);
              threadResult.threadEntry.session.state.contextManager.replaceMessages(messages);
              threadResult.threadEntry.bridgeScope = command;
              const confirmation = threadResult.threadEntry.controlQueue.resolveToolConfirmation(command);
              if (confirmation === "conflict") {
                return { ok: false, sessionId, reason: "control_conflict" } as const;
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
              return {
                ok: true,
                sessionId,
                created: threadResult.sessionCreated,
                applied: confirmation === "applied",
              } as const;
            }),
            (reason) => ({
              ok: false,
              sessionId,
              reason: reason === "local_session_capacity_exceeded"
                ? "local_session_capacity_exceeded"
                : reason === "context_load_failed"
                  ? "context_load_failed"
                  : "control_busy",
            } as RuntimeControlResult),
          );
          if (
            result.ok &&
            "stale" in result &&
            staleTarget !== undefined &&
            staleTarget.sessionEntry.threads.get(command.sessionThreadId) === staleTarget.threadEntry
          ) {
            yield* discardThreadEntryForStaleCustody(staleTarget.sessionEntry, staleTarget.threadEntry);
          }
          return result;
        });

      const commitTaskNotification = (
        sessionId: string,
        command: RuntimeTaskNotificationCommandState,
        commit: RuntimeTaskNotificationCommit,
      ): Effect.Effect<RuntimeTaskNotificationResult> =>
        submitInstalledThreadCommand<RuntimeTaskNotificationResult>(
          command,
          {},
          (threadResult) => Effect.gen(function* () {
            const accepted = threadResult.threadEntry.acceptedInputQueue.enqueueAcceptedInput({
              ...command,
              kind: "task_notification",
              commit,
            });
            if (accepted === "conflict") {
              return { ok: false, sessionId, reason: "control_conflict" } as const;
            }
            threadResult.threadEntry.bridgeScope = command;
            if (accepted === "applied") {
              const runSlot = threadResult.threadEntry.runSlot;
              if (runSlot === undefined) {
                yield* startThreadRun(threadResult.sessionEntry, threadResult.threadEntry).pipe(Effect.asVoid);
              } else if (runSlot.stopping) {
                runSlot.pendingWakeAfterStop = true;
              } else {
                runSlot.pendingWake = true;
              }
            }
            return {
              ok: true,
              sessionId,
              created: threadResult.sessionCreated,
              applied: accepted === "applied",
            } as const;
          }),
          (reason) => ({
            ok: false,
            sessionId,
            reason: reason === "local_session_capacity_exceeded"
              ? "local_session_capacity_exceeded"
              : reason === "context_load_failed"
                ? "context_load_failed"
                : "control_busy",
          }),
        );

      const applyRuntimeConfigPatch = (
        sessionId: string,
        command: RuntimeConfigPatchState,
      ): Effect.Effect<RuntimeControlResult> =>
        Effect.gen(function* () {
          const sessionEntry = sessions.get(commandSessionKey(command));
          if (sessionEntry === undefined) {
            return { ok: true, sessionId, created: false, applied: false, noResidency: true };
          }
          return yield* sessionEntry.controlGate.withPermit(Effect.gen(function* () {
            if (
              sessionEntry.sharedStateStatus !== "ready" ||
              [...sessionEntry.threads.values()].some(
                (threadEntry) =>
                  threadEntry.installationState !== "ready" ||
                  threadEntry.runSlot !== undefined,
              )
            ) {
              return { ok: false, sessionId, reason: "control_busy" } as const;
            }
            const applied = sessionEntry.configuration.apply(command) === "applied";
            for (const threadEntry of sessionEntry.threads.values()) {
              threadEntry.bridgeScope = command;
              threadEntry.session.updateIdentity({
                ...threadEntry.session.identity,
                bindingId: command.bindingId,
                bindingGeneration: command.bindingGeneration,
                targetPodUid: command.targetPodUid,
              });
              if (applied && command.generation !== undefined && options.refreshRuntimeBindingToken !== undefined) {
                const runtimeBindingToken = yield* Effect.promise(() =>
                  options.refreshRuntimeBindingToken?.(threadEntry.session.identity, { force: true }) ??
                  Promise.resolve(threadEntry.session.identity.runtimeBindingToken)
                );
                threadEntry.session.updateIdentity({ ...threadEntry.session.identity, runtimeBindingToken });
              }
            }
            return { ok: true, sessionId, created: false, applied } as const;
          }));
        });

      const cleanupSession = (sessionId: string, command: RuntimeCleanupSessionCommand): Effect.Effect<CleanupSessionResult> =>
        Effect.gen(function* () {
          const entry = sessions.get(commandSessionKey(command));
          if (entry === undefined) {
            return { ok: true, sessionId, cleaned: false };
          }
          return yield* entry.controlGate.withPermit(Effect.gen(function* () {
            for (const threadEntry of entry.threads.values()) {
              if (
                threadEntry.installationState !== "ready" ||
                threadEntry.runSlot !== undefined ||
                threadEntry.session.state.acceptedInputCount() > 0 ||
                threadEntry.commandChannel.busy()
              ) {
                return { ok: false, sessionId, reason: "session_busy" } as const;
              }
            }
            yield* clearSessionEntry(entry);
            return { ok: true, sessionId, cleaned: true } as const;
          }));
        });

      const preloadThread = (
        command: RuntimeThreadPreloadState,
        initializeSharedState?: boolean,
        startPendingWork = true,
      ): Effect.Effect<ThreadLifecycleResult> =>
        Effect.gen(function* () {
          if (admissionClosed) {
            return {
              ok: false,
              sessionId: command.sessionId,
              sessionThreadId: command.sessionThreadId,
              reason: "context_load_failed",
            };
          }
          const metadata = command.thread ?? {};
          const identity: Session.RuntimeSessionIdentity = {
            workspaceId: command.workspaceId,
            sessionId: command.sessionId,
            sessionThreadId: command.sessionThreadId,
            ...(metadata.parentThreadId !== undefined ? { parentThreadId: metadata.parentThreadId } : {}),
            ...(metadata.parentTaskName !== undefined ? { parentTaskName: metadata.parentTaskName } : {}),
            ...(metadata.taskName !== undefined ? { taskName: metadata.taskName } : {}),
            ...(metadata.role !== undefined ? { threadRole: metadata.role } : {}),
            bindingId: command.bindingId,
            bindingGeneration: command.bindingGeneration,
            targetPodUid: command.targetPodUid,
            runtimeBindingToken: command.runtimeBindingToken,
          };
          const threadResult = getOrCreateThreadEntry(identity, metadata, "installing");
          if (threadResult === undefined) {
            return { ok: false, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, reason: "local_session_capacity_exceeded" };
          }
          if (threadResult.threadEntry.runSlot !== undefined) {
            return { ok: false, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, reason: "thread_busy" };
          }
          threadResult.threadEntry.durableTurnId = command.durableTurnId;
          const shouldInitializeSharedState = initializeSharedState ??
            (
              threadResult.sessionEntry.sharedStateStatus === "initializing" &&
              threadResult.sessionEntry.sharedStateInitializerThreadId === command.sessionThreadId
            );
          threadResult.threadEntry.session.updateIdentity(identity);
          threadResult.threadEntry.bridgeScope = command;
          threadResult.threadEntry.session.state.contextManager.installThreadContextPrefix(command.threadContextPrefix);
          threadResult.threadEntry.session.state.contextManager.replaceMessages(command.messages);
          threadResult.threadEntry.session.state.markPersistentContextLoaded();
          const observedSharedPatches = [
            ...(command.runtimeConfigPatch === undefined ? [] : [command.runtimeConfigPatch]),
            ...(command.mcpManifests ?? []),
          ];
          if (shouldInitializeSharedState) {
            yield* threadResult.sessionEntry.controlGate.withPermit(Effect.sync(() => {
              for (const patch of observedSharedPatches) {
                threadResult.sessionEntry.configuration.apply(patch);
              }
              threadResult.sessionEntry.sharedStateStatus = "ready";
              threadResult.sessionEntry.completeSharedStateReady(true);
            }));
          } else {
            const sharedStateReady = yield* Effect.promise(() => threadResult.sessionEntry.sharedStateReady);
            if (!sharedStateReady || threadResult.sessionEntry.sharedStateStatus !== "ready") {
              return { ok: false, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, reason: "context_load_failed" };
            }
          }
          // Model seeding must precede pending-tool restoration because restoration requires the
          // config-selected model to rebuild its processor source.
          if (!coldCoverageMatchesPreload(command)) {
            yield* releaseThreadEntry(threadResult.sessionEntry, threadResult.threadEntry);
            return { ok: false, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, reason: "context_load_failed" };
          }
          agentLoop.seedRuntimeModel(threadResult.threadEntry.session);
          threadResult.threadEntry.session.state.replacePendingAttachments(command.pendingAttachments ?? []);
          const pendingToolUseInstall = yield* agentLoop.installLoadedPendingToolUses(threadResult.threadEntry.session, command.pendingToolUses, command.messages);
          if (!pendingToolUseInstall.ok) {
            yield* releaseThreadEntry(threadResult.sessionEntry, threadResult.threadEntry);
            return { ok: false, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, reason: "context_load_failed" };
          }
          const sandboxExecutionInstall = yield* agentLoop.installLoadedSandboxExecutions(
            threadResult.threadEntry.session,
            command.pendingSandboxExecutions,
            command.messages,
          );
          if (!sandboxExecutionInstall.ok) {
            yield* releaseThreadEntry(threadResult.sessionEntry, threadResult.threadEntry);
            return { ok: false, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, reason: "context_load_failed" };
          }
          for (const backgroundTool of command.backgroundTools ?? []) {
            threadResult.threadEntry.session.state.recordBackgroundTool(backgroundTool);
          }
          threadResult.threadEntry.status = metadata.status ?? "idle";
          for (const mail of command.pendingAgentMail ?? []) {
            const accepted = threadResult.threadEntry.acceptedInputQueue.enqueueAcceptedInput(mail);
            if (accepted === "conflict") {
              yield* releaseThreadEntry(threadResult.sessionEntry, threadResult.threadEntry);
              return { ok: false, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, reason: "context_load_failed" };
            }
          }
          const published = yield* threadResult.sessionEntry.controlGate.withPermit(Effect.sync(() => {
            if (
              admissionClosed ||
              threadResult.sessionEntry.threads.get(command.sessionThreadId) !== threadResult.threadEntry
            ) {
              return false;
            }
            threadResult.threadEntry.installationState = "ready";
            return true;
          }));
          if (!published) {
            return {
              ok: false,
              sessionId: command.sessionId,
              sessionThreadId: command.sessionThreadId,
              reason: "context_load_failed",
            };
          }
          if (startPendingWork && threadResult.threadEntry.session.state.peekAcceptedInput() !== undefined) {
            yield* startThreadRun(threadResult.sessionEntry, threadResult.threadEntry).pipe(Effect.asVoid);
          }
          return { ok: true, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, applied: true };
        });

      const prepareThreadInstallation = (
        command: RuntimeThreadControlState,
        metadata: RuntimeAcceptedThreadMetadataState = {},
      ) =>
        Effect.gen(function* () {
          const failedResult = {
            ok: false,
            sessionId: command.sessionId,
            sessionThreadId: command.sessionThreadId,
            reason: "context_load_failed",
          } as const;
          if (admissionClosed) {
            return { failedResult } as const;
          }
          if (options.loadThreadContext === undefined) {
            const threadResult = getOrCreateThreadEntry(controlIdentity(command), metadata);
            if (threadResult === undefined) {
              return {
                failedResult: {
                  ok: false,
                  sessionId: command.sessionId,
                  sessionThreadId: command.sessionThreadId,
                  reason: "local_session_capacity_exceeded",
                } as const,
              } as const;
            }
            return { threadResult, failedResult, installation: undefined, createdInstallation: false } as const;
          }
          const residentSession = sessions.get(commandSessionKey(command));
          if (residentSession?.sharedStateStatus === "failed") {
            return { failedResult } as const;
          }
          const threadResult = getOrCreateThreadEntry(controlIdentity(command), metadata, "installing");
          if (threadResult === undefined) {
            return {
              failedResult: {
                ok: false,
                sessionId: command.sessionId,
                sessionThreadId: command.sessionThreadId,
                reason: "local_session_capacity_exceeded",
              } as const,
            } as const;
          }
          if (threadResult.threadEntry.installationState === "ready") {
            return { threadResult, failedResult, installation: undefined, createdInstallation: false } as const;
          }
          if (threadResult.threadEntry.installation !== undefined) {
            return {
              threadResult,
              failedResult,
              installation: threadResult.threadEntry.installation,
              createdInstallation: false,
            } as const;
          }

          const initializeSharedState =
            threadResult.sessionEntry.sharedStateStatus === "initializing" &&
            threadResult.sessionEntry.sharedStateInitializerThreadId === command.sessionThreadId;
          const start = yield* Deferred.make<void>();
          const resultDeferred = yield* Deferred.make<ThreadLifecycleResult>();
          const installation: ThreadInstallation = {
            start,
            result: resultDeferred,
            fiber: undefined,
          };
          threadResult.threadEntry.installation = installation;
          const install = Effect.uninterruptibleMask((restore) =>
            Effect.gen(function* () {
              yield* Deferred.await(start);
              const installExit = yield* restore(Effect.gen(function* () {
                const context = yield* Effect.promise(() => options.loadThreadContext!(command));
                if (
                  admissionClosed ||
                  threadResult.sessionEntry.threads.get(command.sessionThreadId) !== threadResult.threadEntry
                ) {
                  return failedResult;
                }
                return yield* preloadThread(context, initializeSharedState, false);
              })).pipe(Effect.exit);
              const result = Exit.isSuccess(installExit) ? installExit.value : failedResult;
              if (!result.ok) {
                if (
                  initializeSharedState &&
                  threadResult.sessionEntry.sharedStateStatus === "initializing"
                ) {
                  threadResult.sessionEntry.sharedStateStatus = "failed";
                  threadResult.sessionEntry.completeSharedStateReady(false);
                }
                if (
                  threadResult.sessionEntry.threads.get(command.sessionThreadId) === threadResult.threadEntry
                ) {
                  yield* removeThreadEntry(threadResult.sessionEntry, threadResult.threadEntry);
                }
              }
              yield* Deferred.succeed(resultDeferred, result);
            }),
          ).pipe(
            Effect.ensuring(Effect.sync(() => {
              if (threadResult.threadEntry.installation === installation) {
                threadResult.threadEntry.installation = undefined;
              }
            })),
          );
          const installationFiber = yield* install.pipe(Effect.forkIn(installationScope));
          installation.fiber = installationFiber;
          return { threadResult, failedResult, installation, createdInstallation: true } as const;
        });

      const ensureThreadInstalled = (
        command: RuntimeThreadControlState,
        installOptions: { readonly requirePendingApprovalToolJobs?: boolean | undefined } = {},
      ): Effect.Effect<ThreadLifecycleResult> =>
        Effect.gen(function* () {
          const prepared = yield* prepareThreadInstallation(command);
          if (prepared.threadResult === undefined) {
            return prepared.failedResult;
          }
          if (prepared.installation === undefined) {
            if (
              installOptions.requirePendingApprovalToolJobs === true &&
              !prepared.threadResult.threadEntry.session.state.hasPendingApprovalToolJobs()
            ) {
              return prepared.failedResult;
            }
            return {
              ok: true,
              sessionId: command.sessionId,
              sessionThreadId: command.sessionThreadId,
              applied: false,
            };
          }
          if (prepared.createdInstallation) {
            yield* Deferred.succeed(prepared.installation.start, undefined);
          }
          const result = yield* Deferred.await(prepared.installation.result);
          if (
            result.ok &&
            installOptions.requirePendingApprovalToolJobs === true &&
            !prepared.threadResult.threadEntry.session.state.hasPendingApprovalToolJobs()
          ) {
            return prepared.failedResult;
          }
          return result;
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
          yield* submitThreadCommand(
            threadEntry,
            Effect.sync(() => {
              threadEntry.bridgeScope = command;
              runSlot.stopping = true;
              runSlot.pendingWake = false;
            }),
            undefined,
          );
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
          yield* submitThreadCommand(
            threadEntry,
            Effect.sync(() => threadEntry.session.state.beginCooperativeCancel()),
            undefined,
          );
          yield* Fiber.interrupt(runSlot.ownerFiber).pipe(Effect.exit, Effect.asVoid);
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
          const runSlot = threadEntry.runSlot;
          yield* submitThreadCommand(
            threadEntry,
            Effect.sync(() => {
              threadEntry.bridgeScope = command;
              threadEntry.status = "closed_for_runtime";
              if (runSlot !== undefined) {
                runSlot.stopping = true;
                runSlot.pendingWake = false;
                runSlot.pendingWakeAfterStop = false;
                threadEntry.session.state.beginCooperativeCancel();
              }
            }),
            undefined,
          );
          let requestedRunExitOutcome: RunExitOutcome | undefined;
          if (runSlot?.ownerFiber !== undefined) {
            yield* Fiber.interrupt(runSlot.ownerFiber).pipe(Effect.exit, Effect.asVoid);
            const runExit = yield* awaitRunSlot(runSlot);
            requestedRunExitOutcome = classifyRunExitOutcome(runExit);
          }
          if (sessionEntry.threads.get(threadEntry.sessionThreadId) === threadEntry) {
            yield* releaseThreadEntry(sessionEntry, threadEntry);
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
          return yield* submitThreadCommand(
            threadEntry,
            Effect.sync(() => {
              threadEntry.bridgeScope = command;
              threadEntry.session.updateIdentity(controlIdentity(command));
              threadEntry.status = "idle";
              return { ok: true, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, applied: true } as const;
            }),
            { ok: false, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, reason: "thread_busy" } as const,
          );
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
          admissionClosed = true;
          const entries = [...sessions.values()];
          yield* Effect.all(entries.map((entry) => shutdownSessionEntry(entry)), { concurrency: "unbounded" });
          yield* Scope.close(installationScope, Exit.void).pipe(Effect.exit, Effect.asVoid);
        });

      const shutdownSessionEntry = (entry: SessionEntry): Effect.Effect<void> =>
        Effect.gen(function* () {
          entry.runtimeShutdown.request();
          const threads = [...entry.threads.values()];
          yield* Effect.forEach(
            threads,
            (threadEntry) => {
              const installationFiber = threadEntry.installation?.fiber;
              return installationFiber === undefined
                ? Effect.void
                : Fiber.interrupt(installationFiber).pipe(Effect.exit, Effect.asVoid);
            },
            { concurrency: "unbounded", discard: true },
          );
          for (const threadEntry of threads) {
            const runSlot = threadEntry.runSlot;
            yield* submitThreadCommand(
              threadEntry,
              Effect.sync(() => {
                threadEntry.session.state.beginRuntimeShutdown();
                if (runSlot !== undefined) {
                  runSlot.stopping = true;
                  runSlot.pendingWake = false;
                  runSlot.pendingWakeAfterStop = false;
                }
              }),
              undefined,
            );
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
          yield* clearSessionEntry(entry);
        });

      yield* Effect.addFinalizer(() => shutdownActiveRuns());

      return Service.of({
        acceptInput,
        applyRuntimeConfigPatch,
        cleanupSession,
        commitTaskNotification,
        preloadThread,
        ensureThreadInstalled,
        interruptThread,
        interruptReviewerExecution,
        interruptControl,
        markThreadActive,
        markThreadClosed,
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
  if (input.kind === "messages" || input.kind === "rejection" || input.kind === "task_notification") {
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

function coldCoverageMatchesPreload(command: RuntimeThreadPreloadState): boolean {
  const sameIdentities = (loaded: readonly string[], covered: readonly string[]): boolean =>
    loaded.length === covered.length &&
    [...loaded].sort().every((identity, index) => identity === [...covered].sort()[index]);
  const pendingToolIds = (command.pendingToolUses ?? []).map((pending) => pending.toolUseEventId);
  const pendingSandboxExecutionIds = (command.pendingSandboxExecutions ?? []).map((execution) => execution.toolUseEventId);
  if (pendingToolIds.some((identity) => pendingSandboxExecutionIds.includes(identity))) {
    return false;
  }
  const pendingAttachmentIdentities = (command.pendingAttachments ?? []).map((attachment) => {
    if (attachment.transient !== undefined) {
      return `transient:${attachment.transient.sourceToolUseEventId}:${attachment.transient.attachmentRef}`;
    }
    return `file:${attachment.fileBacked?.sourceEventId ?? ""}:${attachment.fileBacked?.fileId ?? ""}`;
  });
  const undeliveredMailDeliveryIds = (command.pendingAgentMail ?? []).map((mail) => mail.deliveryId);
  return sameIdentities(pendingToolIds, command.coldCoverage.pendingToolIds) &&
    sameIdentities(pendingSandboxExecutionIds, command.coldCoverage.pendingSandboxExecutionIds) &&
    sameIdentities(pendingAttachmentIdentities, command.coldCoverage.pendingAttachmentIdentities) &&
    sameIdentities(undeliveredMailDeliveryIds, command.coldCoverage.undeliveredMailDeliveryIds);
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
