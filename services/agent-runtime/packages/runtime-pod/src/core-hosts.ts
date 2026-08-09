/**
 * @packageDocumentation
 * Owns the scoped Effect runtime that backs Runtime Pod command, sub-agent, cleanup, and shutdown
 * operations. The process composition root builds these hosts, the gRPC runtime service and tool
 * orchestration call them, and each adapter runs the corresponding SessionRunHost effect. The
 * module keeps one layer scope for the host lifetime and delegates cold-install single-flight
 * ownership to SessionManager before control operations that require resident state. It exposes active-run
 * shutdown and scope release as separate operations that the process composition root orders.
 */
import { status } from "@grpc/grpc-js";
import type { ServiceError } from "@grpc/grpc-js";
import { Context, Effect, Exit, Layer, Scope } from "effect";
import * as ThreadLoop from "@tetral/agent-runtime-core/src/thread-loop/thread-loop.js";
import * as ContextLoader from "@tetral/agent-runtime-core/src/context/context-loader.js";
import * as SessionManager from "@tetral/agent-runtime-core/src/session/session-manager.js";
import * as SessionRunHost from "@tetral/agent-runtime-core/src/session-run-host/session-run-host.js";
import type { RuntimeAcceptedInputState, RuntimeApprovalReviewAcceptedInputState, RuntimeThreadControlState, RuntimeThreadPreloadState } from "@tetral/agent-runtime-core/src/thread-loop/thread-state.js";
import type { RuntimeResolvedAgentMail } from "@tetral/agent-runtime-core/src/context/context-loader.js";
import type { SessionEvent, SessionEventWriterAppendResult } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import { extractColdThreadToolRouteView, extractThreadTurnCheckpoint } from "@tetral/agent-runtime-core/src/thread-loop/thread-turn-checkpoint.js";
import type { ThreadToolRouteView, ThreadTurnCheckpoint } from "@tetral/agent-runtime-core/src/thread-loop/thread-turn-checkpoint.js";
import { deriveThreadTurnDecision } from "@tetral/agent-runtime-core/src/thread-loop/thread-turn-reducer.js";
import type { RuntimeMetricsSink } from "@tetral/agent-runtime-core/src/runtime/metrics.js";
import type { RuntimeCloseoutEvent } from "@tetral/agent-runtime-core/src/session/session-manager.js";
import type { RuntimeAgentMailCommand, RuntimeSessionRunHost } from "./runtime-service.js";
import type { RuntimeCoreCleanupHost } from "./cleanup-controller.js";
import { runtimeAgentMailText, runtimeMessageFromPublicAgentMail } from "./agent-mail.js";

/**
 * Groups the promise-based Runtime Core host surfaces and the two operations that end their shared
 * process lifetime.
 */
export interface RuntimeCoreHosts {
  readonly commandRunHost: RuntimeSessionRunHost;
  readonly subAgentRunHost: RuntimeSubAgentRunHost;
  readonly cleanupRunHost: RuntimeCoreCleanupHost;
  readonly shutdownActiveRuns: () => Promise<void>;
  readonly close: () => Promise<void>;
}

/**
 * Exposes child-thread and approval-reviewer operations to tool orchestration without leaking the
 * Effect service or scope across the Runtime Pod package boundary.
 */
export interface RuntimeSubAgentRunHost {
  readonly enqueueThreadInput: (input: RuntimeAcceptedInputState) => Promise<SessionManager.AcceptInputResult>;
  readonly preloadThread: (input: Omit<RuntimeThreadPreloadState, "messages" | "turnCheckpoint" | "turnToolRouteView" | "durableTurnId" | "runtimeBindingToken" | "runtimeConfigPatch" | "mcpManifests" | "pendingToolUses" | "pendingSandboxExecutions" | "backgroundTools" | "pendingAttachments" | "pendingAgentMail" | "coldCoverage">) => Promise<SessionManager.ThreadLifecycleResult>;
  readonly interruptReviewerExecution: (command: RuntimeThreadControlState, token: SessionManager.ReviewerExecutionToken) => Promise<SessionManager.ReviewerExecutionControlResult>;
  readonly markThreadClosed: (command: Parameters<SessionRunHost.Interface["handleMarkThreadClosed"]>[0]) => Promise<SessionManager.ThreadLifecycleResult>;
  readonly markThreadActive: (command: Parameters<SessionRunHost.Interface["handleMarkThreadActive"]>[0]) => Promise<SessionManager.ThreadLifecycleResult>;
  readonly waitThread: (command: Parameters<SessionRunHost.Interface["handleWaitThread"]>[0], timeoutMs: number | undefined, abortSignal?: AbortSignal | undefined) => Promise<SessionManager.ThreadWaitResult>;
  readonly pullAgentMail?: (
    command: RuntimeThreadControlState,
    sourceThreadId: string,
  ) => Promise<{ readonly deliveryId: string; readonly finalMessage: string } | undefined>;
  readonly waitReviewerExecution: (command: RuntimeThreadControlState, token: SessionManager.ReviewerExecutionToken, timeoutMs: number | undefined, abortSignal?: AbortSignal | undefined) => Promise<SessionManager.ReviewerExecutionWaitResult>;
  readonly inspectThread: (command: Parameters<SessionRunHost.Interface["handleInspectThread"]>[0]) => Promise<SessionManager.ThreadSnapshotResult>;
  readonly inspectReviewerExecution: (command: RuntimeThreadControlState, token: SessionManager.ReviewerExecutionToken) => Promise<SessionManager.ReviewerExecutionSnapshotResult>;
  readonly commitApprovalReviewDecision: (command: RuntimeApprovalReviewAcceptedInputState, event: Extract<SessionEvent, { readonly type: "approval_review.decision" }>) => Promise<SessionEventWriterAppendResult>;
  readonly commitApprovalReviewFailure: (command: RuntimeApprovalReviewAcceptedInputState, event: Extract<SessionEvent, { readonly type: "approval_review.failure" }>) => Promise<SessionEventWriterAppendResult>;
}

/** Configures capacity, time, context, ThreadLoop, and metrics for one Runtime Core host scope. */
export interface RuntimeCoreHostsOptions {
  readonly maxLocalSessions: number;
  readonly maxConcurrentTools?: number | undefined;
  readonly now: () => string;
  readonly contextLoader: ContextLoader.ContextLoader;
  readonly threadLoop: ThreadLoop.ThreadLoopRuntimeOptions;
  readonly metrics?: RuntimeMetricsSink | undefined;
  readonly recordCloseoutEvent?: ((event: RuntimeCloseoutEvent) => void) | undefined;
}

/**
 * Builds the ThreadLoop, Session Manager, and SessionRunHost layers in one explicit Effect scope and
 * returns promise adapters for network commands and in-process tools. Cold installation is owned by
 * SessionManager, accepted messages are converted to Runtime Core input only after installation, and
 * callers close the returned hosts to release the layer scope.
 */
export async function buildRuntimeCoreHosts(options: RuntimeCoreHostsOptions): Promise<RuntimeCoreHosts> {
  const threadLoopLayer = ThreadLoop.layer({
    ...options.threadLoop,
    ...(options.contextLoader.refreshRuntimeBindingToken !== undefined
      ? { refreshRuntimeBindingToken: (identity, refreshOptions) => options.contextLoader.refreshRuntimeBindingToken?.(identity, refreshOptions) ?? Promise.resolve(identity.runtimeBindingToken) }
      : {}),
    ...(options.metrics !== undefined ? { metrics: options.metrics } : {}),
  }).pipe(Layer.provide(ThreadLoop.contextLoaderLayer(options.contextLoader)));
  const managerLayer = SessionManager.layer({
    maxLocalSessions: options.maxLocalSessions,
    maxConcurrentTools: options.maxConcurrentTools ?? 8,
    now: options.now,
    ...(options.contextLoader.loadThreadContext !== undefined
      ? {
          loadThreadContext: async (command: RuntimeThreadControlState): Promise<RuntimeThreadPreloadState> => {
            const context = await options.contextLoader.loadThreadContext!(command);
            const pendingAgentMail: Array<Extract<RuntimeAcceptedInputState, { readonly kind: "inter_agent_message" }>> = [];
            if ((context.pendingAgentMail?.length ?? 0) > 0 && context.thread === undefined) {
              throw new Error("pending agent mail requires durable thread lineage");
            }
            for (const descriptor of context.pendingAgentMail ?? []) {
              if (options.contextLoader.resolveAgentMail === undefined) {
                throw new Error("agent mail resolver is unavailable");
              }
              // Resolver authority is parent-scoped even when the cold target is the child.
              const currentIsChild = context.thread?.parentThreadId === descriptor.sourceThreadId;
              const resolverCommand = currentIsChild
                ? { ...command, sessionThreadId: descriptor.sourceThreadId }
                : command;
              const childThreadId = currentIsChild
                ? command.sessionThreadId
                : descriptor.sourceThreadId;
              const resolved = await options.contextLoader.resolveAgentMail(
                resolverCommand,
                childThreadId,
                descriptor.deliveryId,
              );
              pendingAgentMail.push(acceptedResolvedAgentMail(command, resolved, context.thread));
            }
            const turnCheckpoint = extractThreadTurnCheckpoint({ messages: context.messages, facts: context.turnFacts });
            if (context.durableTurnId !== turnCheckpoint.executionRunId) {
              throw new Error("durable turn identity conflicts with the reconstructed Thread turn");
            }
            const turnToolRouteView = extractColdThreadToolRouteView({
              checkpoint: turnCheckpoint,
              pendingToolUses: context.pendingToolUses ?? [],
              pendingSandboxExecutions: context.pendingSandboxExecutions ?? [],
            });
			if (context.thread?.status === "closed_for_runtime") {
			  validateClosedThreadResumeCheckpoint(
				turnCheckpoint,
				turnToolRouteView,
				context.pendingToolUses ?? [],
				context.pendingSandboxExecutions ?? [],
			  );
			}
            return {
              ...command,
              ...(context.thread !== undefined ? { thread: context.thread } : {}),
              messages: context.messages,
              turnCheckpoint,
              turnToolRouteView,
              ...(context.threadContextPrefix !== undefined ? { threadContextPrefix: context.threadContextPrefix } : {}),
              ...(context.durableTurnId !== undefined ? { durableTurnId: context.durableTurnId } : {}),
              runtimeBindingToken: context.runtimeBindingToken,
              ...(context.runtimeConfigPatch !== undefined ? { runtimeConfigPatch: context.runtimeConfigPatch } : {}),
              ...(context.mcpManifests !== undefined ? { mcpManifests: context.mcpManifests } : {}),
              ...(context.pendingToolUses !== undefined ? { pendingToolUses: context.pendingToolUses } : {}),
              ...(context.pendingSandboxExecutions !== undefined ? { pendingSandboxExecutions: context.pendingSandboxExecutions } : {}),
              ...(context.backgroundTools !== undefined ? { backgroundTools: context.backgroundTools } : {}),
              ...(context.pendingAttachments !== undefined ? { pendingAttachments: context.pendingAttachments } : {}),
              pendingAgentMail,
              coldCoverage: context.coldCoverage,
            };
          },
        }
      : {}),
    ...(options.contextLoader.refreshRuntimeBindingToken !== undefined
      ? { refreshRuntimeBindingToken: (identity, refreshOptions) => options.contextLoader.refreshRuntimeBindingToken?.(identity, refreshOptions) ?? Promise.resolve(identity.runtimeBindingToken) }
      : {}),
    ...(options.metrics !== undefined ? { metrics: options.metrics } : {}),
    closeoutMonotonicMs: options.threadLoop.runtime.monotonicMs,
    closeoutSleep: options.threadLoop.runtime.sleep,
    ...(options.recordCloseoutEvent !== undefined ? { recordCloseoutEvent: options.recordCloseoutEvent } : {}),
  }).pipe(Layer.provide(threadLoopLayer));
  const hostLayer = SessionRunHost.layer.pipe(Layer.provide(managerLayer));
  const { host, scope } = await Effect.runPromise(
    Effect.gen(function* () {
      const layerScope = yield* Scope.make();
      const context = yield* Layer.buildWithScope(hostLayer, layerScope);
      return { host: Context.get(context, SessionRunHost.Service), scope: layerScope };
    }),
  );
  return {
    commandRunHost: {
      handleAcceptInput: async (command) => {
        const acceptedInput = runtimeAcceptedInputFromCommand(command);
        const result = await Effect.runPromise(host.handleAcceptInput(acceptedInput));
        if (result.ok) {
          return result;
        }
        if (result.reason === "context_load_failed") {
          throw new Error("cold thread preload failed");
        }
        return { ok: false, sessionId: result.sessionId, reason: "local_session_capacity_exceeded" };
      },
      handleAgentMail: async (command) => {
        try {
          const result = await Effect.runPromise(host.handleAcceptInput(acceptedAgentMailCommand(command)));
          if (!result.ok) {
            return {
              ok: false,
              sessionId: command.sessionId,
              reason: result.reason === "local_session_capacity_exceeded"
                ? "local_session_capacity_exceeded"
                : "thread_not_receivable",
            } as const;
          }
          return {
            ok: true,
            sessionId: command.sessionId,
            applied: result.started || result.pendingWake,
          };
        } catch {
          return { ok: false, sessionId: command.sessionId, reason: "context_load_failed" };
        }
      },
      handleInterruptControl: async (sessionId, command, commitInput) => await Effect.runPromise(host.handleInterruptControl(
        sessionId,
        command,
        commitInput,
      )),
      handleToolConfirmation: async (sessionId, command, commit) =>
        await Effect.runPromise(host.handleToolConfirmation(sessionId, command, commit)),
      handleTaskNotification: async (sessionId, command, commit) =>
        await Effect.runPromise(host.handleTaskNotification(sessionId, command, commit)),
      handleRuntimeConfigPatch: async (sessionId, command) => {
        return await Effect.runPromise(host.handleRuntimeConfigPatch(sessionId, command));
      },
    },
    subAgentRunHost: {
      enqueueThreadInput: async (input) => await Effect.runPromise(host.handleAcceptInput(input)),
      preloadThread: async (input) => {
        return await Effect.runPromise(host.handleEnsureThreadInstalled(input));
      },
      interruptReviewerExecution: async (command, token) => await Effect.runPromise(host.handleInterruptReviewerExecution(command, token)),
      markThreadClosed: async (command) => await Effect.runPromise(host.handleMarkThreadClosed(command)),
      markThreadActive: async (command) => await Effect.runPromise(host.handleMarkThreadActive(command)),
      waitThread: async (command, timeoutMs, abortSignal) => await Effect.runPromise(
        host.handleWaitThread(command, timeoutMs),
        abortSignal === undefined ? undefined : { signal: abortSignal },
      ),
      pullAgentMail: async (command, sourceThreadId) => {
        if (options.contextLoader.resolveAgentMail === undefined) {
          throw new Error("agent mail resolver is unavailable");
        }
        let resolved: RuntimeResolvedAgentMail;
        try {
          resolved = await options.contextLoader.resolveAgentMail(command, sourceThreadId);
        } catch (error) {
          if (isGrpcStatus(error, status.NOT_FOUND)) {
            return undefined;
          }
          throw error;
        }
        return {
          deliveryId: resolved.deliveryId,
          finalMessage: runtimeAgentMailText(runtimeMessageFromPublicAgentMail(resolved.publicMessageJson)),
        };
      },
      waitReviewerExecution: async (command, token, timeoutMs, abortSignal) => await Effect.runPromise(
        host.handleWaitReviewerExecution(command, token, timeoutMs),
        abortSignal === undefined ? undefined : { signal: abortSignal },
      ),
      inspectThread: async (command) => await Effect.runPromise(host.handleInspectThread(command)),
      inspectReviewerExecution: async (command, token) => await Effect.runPromise(host.handleInspectReviewerExecution(command, token)),
      commitApprovalReviewDecision: async (command, event) => await options.threadLoop.sessionEventWriter.append({
        requestId: command.requestId,
        workspaceId: command.workspaceId,
        sessionId: command.sessionId,
        sessionThreadId: command.sessionThreadId,
        bindingId: command.bindingId,
        bindingGeneration: command.bindingGeneration,
        targetPodUid: command.targetPodUid,
        writeId: `rwrite_${event.review_id}_decision`,
        event,
      }),
      commitApprovalReviewFailure: async (command, event) => await options.threadLoop.sessionEventWriter.append({
        requestId: command.requestId,
        workspaceId: command.workspaceId,
        sessionId: command.sessionId,
        sessionThreadId: command.sessionThreadId,
        bindingId: command.bindingId,
        bindingGeneration: command.bindingGeneration,
        targetPodUid: command.targetPodUid,
        writeId: `rwrite_${event.review_id}_failure`,
        event,
      }),
    },
    cleanupRunHost: {
      handleCleanupSession: async (scope) => await Effect.runPromise(host.handleCleanupSession(scope.sessionId, scope)),
    },
    shutdownActiveRuns: async () => {
      await Effect.runPromise(host.shutdownActiveRuns());
    },
    close: async () => {
      await Effect.runPromise(Scope.close(scope, Exit.void));
    },
  };
}

/**
 * Accepts only a quiescent cold checkpoint for resume. Deferred task
 * notifications are intentionally absent from this execution checkpoint and
 * are reactivated by Bridge after the durable lifecycle transition wins.
 */
export function validateClosedThreadResumeCheckpoint(
  checkpoint: ThreadTurnCheckpoint,
  routeView: ThreadToolRouteView,
  pendingToolUses: readonly unknown[],
  pendingSandboxExecutions: readonly unknown[],
): void {
  const decision = deriveThreadTurnDecision(checkpoint, routeView);
  const incompleteToolUse = checkpoint.request?.toolMembers.some((member) =>
    member.memberKind === "public_tool_use" && member.terminalResult === undefined
  ) ?? false;
  if (
    checkpoint.executionRunId !== undefined ||
    checkpoint.interruptEventId !== undefined ||
    checkpoint.terminalCloseout !== undefined ||
    checkpoint.request?.requestEnd === undefined && checkpoint.request !== undefined ||
    incompleteToolUse ||
    pendingToolUses.length !== 0 ||
    pendingSandboxExecutions.length !== 0 ||
    routeView.routes.length !== 0 ||
    decision.state.state !== "idle" ||
    decision.action.action !== "await_input"
  ) {
    throw new Error("closed Thread resume requires a quiescent durable checkpoint");
  }
}

function acceptedResolvedAgentMail(
  command: RuntimeThreadControlState,
  mail: RuntimeResolvedAgentMail,
  thread: RuntimeThreadPreloadState["thread"],
): Extract<RuntimeAcceptedInputState, { readonly kind: "inter_agent_message" }> {
  return {
    ...command,
    requestId: `agent_mail:${mail.deliveryId}`,
    runtimeInputId: `agent_mail:${mail.deliveryId}`,
    eventIds: [mail.receivedEventId],
    sequenceFrom: mail.receivedSequence,
    sequenceTo: mail.receivedSequence,
    kind: "inter_agent_message",
    deliveryId: mail.deliveryId,
    sourceThreadId: mail.sourceThreadId,
    sourceToolUseEventId: mail.sourceToolUseEventId,
    message: mail.message,
    thread,
  };
}

function acceptedAgentMailCommand(
  command: RuntimeAgentMailCommand,
): Extract<RuntimeAcceptedInputState, { readonly kind: "inter_agent_message" }> {
  return {
    ...command,
    kind: "inter_agent_message",
  };
}

function isGrpcStatus(error: unknown, code: status): boolean {
  return typeof error === "object" && error !== null && (error as Partial<ServiceError>).code === code;
}

function runtimeAcceptedInputFromCommand(command: Parameters<RuntimeSessionRunHost["handleAcceptInput"]>[0]): RuntimeAcceptedInputState {
  if (command.kind === "rejection") {
    return { ...command };
  }
  return {
    requestId: command.requestId,
    workspaceId: command.workspaceId,
    sessionId: command.sessionId,
    sessionThreadId: command.sessionThreadId,
    bindingId: command.bindingId,
    bindingGeneration: command.bindingGeneration,
    targetPodUid: command.targetPodUid,
    runtimeInputId: command.runtimeInputId,
    eventIds: [...command.eventIds],
    sequenceFrom: command.sequenceFrom,
    sequenceTo: command.sequenceTo,
    kind: "messages",
    payloadJson: command.payloadJson,
  };
}
