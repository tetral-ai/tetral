/**
 * @packageDocumentation
 * Owns the scoped Effect runtime that backs Runtime Pod command, sub-agent, cleanup, and shutdown
 * operations. The process composition root builds these hosts, the gRPC runtime service and tool
 * orchestration call them, and each adapter runs the corresponding SessionRunHost effect. The
 * module keeps one layer scope for the host lifetime, coalesces identical cold-thread preloads,
 * and hydrates thread state before control operations that require it. It exposes active-run
 * shutdown and scope release as separate operations that the process composition root orders.
 */
import { Context, Effect, Exit, Layer, Scope } from "effect";
import * as AgentLoop from "@tetral/agent-runtime-core/src/agent-loop/agent-loop.js";
import * as ContextLoader from "@tetral/agent-runtime-core/src/context/context-loader.js";
import * as SessionManager from "@tetral/agent-runtime-core/src/session/session-manager.js";
import * as SessionRunHost from "@tetral/agent-runtime-core/src/session-run-host/session-run-host.js";
import type { RuntimeAcceptedInputState, RuntimeApprovalReviewAcceptedInputState, RuntimeThreadControlState, RuntimeThreadPreloadState } from "@tetral/agent-runtime-core/src/session/session-state.js";
import type { RuntimeLoadedAgentMail } from "@tetral/agent-runtime-core/src/context/context-loader.js";
import type { SessionEvent, SessionEventWriterAppendResult } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import type { RuntimeMetricsSink } from "@tetral/agent-runtime-core/src/runtime/metrics.js";
import type { RuntimeCloseoutEvent } from "@tetral/agent-runtime-core/src/session/session-manager.js";
import type { RuntimeSessionRunHost } from "./runtime-service.js";
import type { RuntimeCoreCleanupHost } from "./cleanup-controller.js";

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
  readonly preloadThread: (input: Omit<RuntimeThreadPreloadState, "messages" | "runtimeBindingToken" | "runtimeConfigPatch" | "mcpManifests" | "pendingToolUses" | "backgroundTools" | "pendingAttachments" | "pendingAgentMail">) => Promise<SessionManager.ThreadLifecycleResult>;
  readonly interruptThread: (command: Parameters<SessionRunHost.Interface["handleInterruptThread"]>[0]) => Promise<SessionManager.ThreadLifecycleResult>;
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

/** Configures capacity, time, context, Agent Loop, and metrics for one Runtime Core host scope. */
export interface RuntimeCoreHostsOptions {
  readonly maxLocalSessions: number;
  readonly maxConcurrentTools?: number | undefined;
  readonly now: () => string;
  readonly contextLoader: ContextLoader.ContextLoader;
  readonly registerAcceptedInput?: (input: RuntimeAcceptedInputState) => () => void;
  readonly agentLoop: AgentLoop.AgentLoopRuntimeOptions;
  readonly metrics?: RuntimeMetricsSink | undefined;
  readonly recordCloseoutEvent?: ((event: RuntimeCloseoutEvent) => void) | undefined;
}

/**
 * Builds the Agent Loop, Session Manager, and SessionRunHost layers in one explicit Effect scope and
 * returns promise adapters for network commands and in-process tools. Cold preloads are shared per
 * binding identity, accepted messages are converted to Runtime Core input only after preload, and
 * callers close the returned hosts to release the layer scope.
 */
export async function buildRuntimeCoreHosts(options: RuntimeCoreHostsOptions): Promise<RuntimeCoreHosts> {
  const coldThreadPreloads = new Map<string, Promise<ColdThreadPreloadResult>>();
  const agentLoopLayer = AgentLoop.layer({
    ...options.agentLoop,
    ...(options.contextLoader.refreshRuntimeBindingToken !== undefined
      ? { refreshRuntimeBindingToken: (identity, refreshOptions) => options.contextLoader.refreshRuntimeBindingToken?.(identity, refreshOptions) ?? Promise.resolve(identity.runtimeBindingToken) }
      : {}),
    ...(options.metrics !== undefined ? { metrics: options.metrics } : {}),
  }).pipe(Layer.provide(AgentLoop.contextLoaderLayer(options.contextLoader)));
  const managerLayer = SessionManager.layer({
    maxLocalSessions: options.maxLocalSessions,
    maxConcurrentTools: options.maxConcurrentTools ?? 8,
    now: options.now,
    ...(options.contextLoader.loadThreadContext !== undefined
      ? {
          loadPendingAgentMail: async (command: RuntimeThreadControlState) => {
            const context = await options.contextLoader.loadThreadContext!(command);
            return (context.pendingAgentMail ?? []).map((mail) => acceptedAgentMail(command, mail, context.thread));
          },
        }
      : {}),
    ...(options.contextLoader.loadThreadContext !== undefined
      ? { loadThreadMessages: async (command: RuntimeThreadControlState) => (await options.contextLoader.loadThreadContext!(command)).messages }
      : {}),
    ...(options.contextLoader.refreshRuntimeBindingToken !== undefined
      ? { refreshRuntimeBindingToken: (identity, refreshOptions) => options.contextLoader.refreshRuntimeBindingToken?.(identity, refreshOptions) ?? Promise.resolve(identity.runtimeBindingToken) }
      : {}),
    ...(options.registerAcceptedInput !== undefined ? { registerAcceptedInput: options.registerAcceptedInput } : {}),
    ...(options.metrics !== undefined ? { metrics: options.metrics } : {}),
    closeoutMonotonicMs: options.agentLoop.runtime.monotonicMs,
    closeoutSleep: options.agentLoop.runtime.sleep,
    ...(options.recordCloseoutEvent !== undefined ? { recordCloseoutEvent: options.recordCloseoutEvent } : {}),
  }).pipe(Layer.provide(agentLoopLayer));
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
        const preload = await preloadColdThreadForCommand(host, options.contextLoader, coldThreadPreloads, command);
        if (!preload.ok) {
          if (preload.reason === "local_session_capacity_exceeded") {
            return { ok: false, sessionId: preload.sessionId, reason: "local_session_capacity_exceeded" };
          }
          throw new Error(`cold thread preload failed: ${preload.reason}`);
        }
        const acceptedInput = runtimeAcceptedInputFromCommand(command);
        const result = await Effect.runPromise(host.handleAcceptInput(acceptedInput));
        if (result.ok) {
          return result;
        }
        return { ok: false, sessionId: result.sessionId, reason: "local_session_capacity_exceeded" };
      },
      handleAgentMail: async (command) => {
        if (options.contextLoader.loadThreadContext === undefined) {
          return { ok: false, sessionId: command.sessionId, reason: "context_load_failed" };
        }
        try {
          const inspected = await Effect.runPromise(host.handleInspectThread(command));
          const context = await options.contextLoader.loadThreadContext(command);
          if (!inspected.ok || !inspected.observed) {
            const preloaded = await Effect.runPromise(host.handlePreloadThread({
              ...command,
              ...(context.thread !== undefined ? { thread: context.thread } : {}),
              messages: context.messages,
              runtimeBindingToken: context.runtimeBindingToken,
              ...(context.runtimeConfigPatch !== undefined ? { runtimeConfigPatch: context.runtimeConfigPatch } : {}),
              ...(context.mcpManifests !== undefined ? { mcpManifests: context.mcpManifests } : {}),
              ...(context.pendingToolUses !== undefined ? { pendingToolUses: context.pendingToolUses } : {}),
              ...(context.backgroundTools !== undefined ? { backgroundTools: context.backgroundTools } : {}),
              ...(context.pendingAttachments !== undefined ? { pendingAttachments: context.pendingAttachments } : {}),
              pendingAgentMail: (context.pendingAgentMail ?? []).map((mail) => acceptedAgentMail(command, mail, context.thread)),
            }));
            if (!preloaded.ok) {
              return { ok: false, sessionId: command.sessionId, reason: preloaded.reason === "local_session_capacity_exceeded" ? "local_session_capacity_exceeded" : "context_load_failed" };
            }
            return { ok: true, sessionId: command.sessionId, applied: (context.pendingAgentMail?.length ?? 0) > 0 };
          }
          let applied = false;
          for (const mail of context.pendingAgentMail ?? []) {
            const input = acceptedAgentMail(command, mail, context.thread);
            const unregister = options.registerAcceptedInput?.(input);
            const result = await Effect.runPromise(host.handleAcceptInput(input));
            if (!result.ok) {
              unregister?.();
              return { ok: false, sessionId: command.sessionId, reason: result.reason === "local_session_capacity_exceeded" ? "local_session_capacity_exceeded" : "context_load_failed" };
            }
            applied = applied || result.started || result.pendingWake;
          }
          return { ok: true, sessionId: command.sessionId, applied };
        } catch {
          return { ok: false, sessionId: command.sessionId, reason: "context_load_failed" };
        }
      },
      handleInterruptControl: async (sessionId, command, commitInput) => await Effect.runPromise(host.handleInterruptControl(
        sessionId,
        command,
        commitInput ?? (async () => ({ ok: false, retryable: true, errorCode: "interrupt_commit_unavailable" })),
      )),
      handleToolConfirmation: async (sessionId, command) => {
        const preload = await preloadColdThreadForCommand(host, options.contextLoader, coldThreadPreloads, command, { requirePendingApprovalToolJobs: true });
        if (!preload.ok) {
          return { ok: false, sessionId, reason: preload.reason };
        }
        return await Effect.runPromise(host.handleToolConfirmation(sessionId, command));
      },
      handleTaskNotification: async (sessionId, command) => {
        const preload = await preloadColdThreadForCommand(host, options.contextLoader, coldThreadPreloads, command);
        if (!preload.ok) {
          return { ok: false, sessionId, reason: preload.reason };
        }
        return await Effect.runPromise(host.handleTaskNotification(sessionId, command));
      },
      handleRuntimeConfigPatch: async (sessionId, command) => {
        const preload = await preloadColdThreadForCommand(host, options.contextLoader, coldThreadPreloads, command);
        if (!preload.ok) {
          return { ok: false, sessionId, reason: preload.reason };
        }
        return await Effect.runPromise(host.handleRuntimeConfigPatch(sessionId, command));
      },
    },
    subAgentRunHost: {
      enqueueThreadInput: async (input) => await Effect.runPromise(host.handleAcceptInput(input)),
      preloadThread: async (input) => {
        if (options.contextLoader.loadThreadContext === undefined) {
          return { ok: false, sessionId: input.sessionId, sessionThreadId: input.sessionThreadId, reason: "local_session_capacity_exceeded" };
        }
        const context = await options.contextLoader.loadThreadContext(input);
        return await Effect.runPromise(host.handlePreloadThread({
          ...input,
          ...(context.thread !== undefined || input.thread !== undefined ? { thread: context.thread ?? input.thread } : {}),
          messages: context.messages,
          runtimeBindingToken: context.runtimeBindingToken,
          ...(context.runtimeConfigPatch !== undefined ? { runtimeConfigPatch: context.runtimeConfigPatch } : {}),
          ...(context.mcpManifests !== undefined ? { mcpManifests: context.mcpManifests } : {}),
          ...(context.pendingToolUses !== undefined ? { pendingToolUses: context.pendingToolUses } : {}),
          ...(context.backgroundTools !== undefined ? { backgroundTools: context.backgroundTools } : {}),
          ...(context.pendingAttachments !== undefined ? { pendingAttachments: context.pendingAttachments } : {}),
          pendingAgentMail: (context.pendingAgentMail ?? []).map((mail) => acceptedAgentMail(input, mail, context.thread)),
        }));
      },
      interruptThread: async (command) => await Effect.runPromise(host.handleInterruptThread(command)),
      interruptReviewerExecution: async (command, token) => await Effect.runPromise(host.handleInterruptReviewerExecution(command, token)),
      markThreadClosed: async (command) => await Effect.runPromise(host.handleMarkThreadClosed(command)),
      markThreadActive: async (command) => await Effect.runPromise(host.handleMarkThreadActive(command)),
      waitThread: async (command, timeoutMs, abortSignal) => await Effect.runPromise(
        host.handleWaitThread(command, timeoutMs),
        abortSignal === undefined ? undefined : { signal: abortSignal },
      ),
      pullAgentMail: async (command, sourceThreadId) => {
        if (options.contextLoader.loadThreadContext === undefined || options.contextLoader.commitAcceptedInput === undefined) {
          return undefined;
        }
        const context = await options.contextLoader.loadThreadContext(command, { agentMailSourceThreadId: sourceThreadId });
        const mail = context.pendingAgentMail?.find((candidate) => candidate.sourceThreadId === sourceThreadId);
        if (mail === undefined) {
          return undefined;
        }
        const input = { ...acceptedAgentMail(command, mail, context.thread), presentation: "pull" as const };
        const committed = await options.contextLoader.commitAcceptedInput(input, {
          registerScope: false,
          hydrateContext: false,
        });
        await Effect.runPromise(host.handleMarkAgentMailPulled(command, mail.deliveryId));
        if (committed.type !== "receipt") {
          throw new Error("pulled agent mail receipt commit returned hydrated context");
        }
        return { deliveryId: mail.deliveryId, finalMessage: completionMessageText(mail.message) };
      },
      waitReviewerExecution: async (command, token, timeoutMs, abortSignal) => await Effect.runPromise(
        host.handleWaitReviewerExecution(command, token, timeoutMs),
        abortSignal === undefined ? undefined : { signal: abortSignal },
      ),
      inspectThread: async (command) => await Effect.runPromise(host.handleInspectThread(command)),
      inspectReviewerExecution: async (command, token) => await Effect.runPromise(host.handleInspectReviewerExecution(command, token)),
      commitApprovalReviewDecision: async (command, event) => await options.agentLoop.sessionEventWriter.append({
        sessionId: command.sessionId,
        sessionThreadId: command.sessionThreadId,
        writeId: `rwrite_${event.review_id}_decision`,
        event,
      }),
      commitApprovalReviewFailure: async (command, event) => await options.agentLoop.sessionEventWriter.append({
        sessionId: command.sessionId,
        sessionThreadId: command.sessionThreadId,
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

function completionMessageText(message: RuntimeLoadedAgentMail["message"]): string {
  return message.parts
    .map((part) => part.type === "text" ? part.text : "")
    .join("");
}

type ColdThreadPreloadResult =
  | { readonly ok: true }
  | { readonly ok: false; readonly sessionId: string; readonly reason: "local_session_capacity_exceeded" | "context_load_failed" };

function preloadColdThreadForCommand(
  host: SessionRunHost.Interface,
  contextLoader: ContextLoader.ContextLoader,
  inFlight: Map<string, Promise<ColdThreadPreloadResult>>,
  command: RuntimeThreadControlState,
  options: { readonly requirePendingApprovalToolJobs?: boolean | undefined } = {},
): Promise<ColdThreadPreloadResult> {
  const key = [
    command.workspaceId,
    command.sessionId,
    command.sessionThreadId,
    command.bindingId,
    command.bindingGeneration,
    command.targetPodUid,
    options.requirePendingApprovalToolJobs === true ? "approval" : "general",
  ].join("\u0000");
  const existing = inFlight.get(key);
  if (existing !== undefined) {
    return existing;
  }
  const pending = preloadColdThreadForCommandOnce(host, contextLoader, command, options);
  inFlight.set(key, pending);
  const clear = (): void => {
    if (inFlight.get(key) === pending) {
      inFlight.delete(key);
    }
  };
  void pending.then(clear, clear);
  return pending;
}

async function preloadColdThreadForCommandOnce(
  host: SessionRunHost.Interface,
  contextLoader: ContextLoader.ContextLoader,
  command: RuntimeThreadControlState,
  options: { readonly requirePendingApprovalToolJobs?: boolean | undefined },
): Promise<ColdThreadPreloadResult> {
  const inspected = await Effect.runPromise(host.handleInspectThread(command));
  if (!inspected.ok) {
    if (inspected.reason === "local_session_capacity_exceeded") {
      return { ok: false, sessionId: command.sessionId, reason: "local_session_capacity_exceeded" };
    }
    return { ok: true };
  }
  if (inspected.observed && (options.requirePendingApprovalToolJobs !== true || inspected.hasPendingApprovalToolJobs === true)) {
    return { ok: true };
  }
  if (contextLoader.loadThreadContext === undefined) {
    return { ok: false, sessionId: command.sessionId, reason: "local_session_capacity_exceeded" };
  }
  const context = await contextLoader.loadThreadContext(command);
  const preloaded = await Effect.runPromise(host.handlePreloadThread({
    ...command,
    ...(context.thread !== undefined ? { thread: context.thread } : {}),
    messages: context.messages,
    runtimeBindingToken: context.runtimeBindingToken,
    ...(context.runtimeConfigPatch !== undefined ? { runtimeConfigPatch: context.runtimeConfigPatch } : {}),
    ...(context.mcpManifests !== undefined ? { mcpManifests: context.mcpManifests } : {}),
    ...(context.pendingToolUses !== undefined ? { pendingToolUses: context.pendingToolUses } : {}),
    ...(context.backgroundTools !== undefined ? { backgroundTools: context.backgroundTools } : {}),
    ...(context.pendingAttachments !== undefined ? { pendingAttachments: context.pendingAttachments } : {}),
    pendingAgentMail: (context.pendingAgentMail ?? []).map((mail) => acceptedAgentMail(command, mail, context.thread)),
  }));
  if (!preloaded.ok) {
    if (preloaded.reason === "local_session_capacity_exceeded") {
      return { ok: false, sessionId: command.sessionId, reason: "local_session_capacity_exceeded" };
    }
    if (preloaded.reason === "context_load_failed") {
      return { ok: false, sessionId: command.sessionId, reason: "context_load_failed" };
    }
    return { ok: true };
  }
  return { ok: true };
}

function acceptedAgentMail(
  command: RuntimeThreadControlState,
  mail: RuntimeLoadedAgentMail,
  thread: RuntimeThreadPreloadState["thread"],
): Extract<RuntimeAcceptedInputState, { readonly kind: "inter_agent_message" }> {
  return {
    ...command,
    requestId: `agent_mail:${mail.deliveryId}`,
    runtimeInputId: `agent_mail:${mail.deliveryId}`,
    eventIds: [],
    sequenceFrom: 0,
    sequenceTo: 0,
    kind: "inter_agent_message",
    deliveryId: mail.deliveryId,
    sourceThreadId: mail.sourceThreadId,
    sourceToolUseEventId: mail.sourceToolUseEventId,
    message: mail.message,
    thread,
  };
}

function runtimeAcceptedInputFromCommand(command: Parameters<RuntimeSessionRunHost["handleAcceptInput"]>[0]): RuntimeAcceptedInputState {
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
