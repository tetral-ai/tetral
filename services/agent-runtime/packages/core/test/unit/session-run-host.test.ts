import { describe, expect, test } from "bun:test";
import { Context, Effect, Exit, Layer, Scope, Stream } from "effect";
import {
  RuntimeMessageRole,
  SystemCacheHint,
  SystemSegmentKind,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type {
  PendingInputResult,
  RuntimeDependencies,
  RuntimeMessage,
  RuntimeMessageInfo,
  RuntimeMessageStoreOperationControls,
  RuntimePart,
  SessionEvent,
  SessionEventEnvelope,
  SessionEventWriter,
  SessionEventWriterAppendResult,
  SessionEventWriterFinishIdleEnvelope,
  SessionEventWriterRequestEndEnvelope,
} from "../../src/contracts/runtime.js";
import { RuntimeMessageSchema, RuntimeMessageStore, normalizeRuntimeMessageStoreError } from "../../src/contracts/runtime.js";
import type { AcceptedInputCommitResult, ContextLoader } from "../../src/context/context-loader.js";
import type { LLMEvent } from "../../src/llm/llm-event.js";
import type { Interface as LLMServiceInterface, LLMRequest } from "../../src/llm/llm-service.js";
import type { RuntimeAcceptedInputState, RuntimeThreadControlState } from "../../src/session/session-state.js";
import * as AgentLoop from "../../src/agent-loop/agent-loop.js";
import * as SessionManager from "../../src/session/session-manager.js";
import * as SessionRunHost from "../../src/session-run-host/session-run-host.js";
import {
  buildSessionRunHostUserMessage as userMessage,
  buildSessionRunHostRuntimeNotificationMessage as runtimeNotificationMessage,
} from "./runtime-message-builders.js";

const createdAt = "2026-06-14T00:00:00.000Z";

function acceptedInput(sessionId: string, runtimeInputId = `rin_${sessionId}`): RuntimeAcceptedInputState {
  return {
    requestId: `req_${runtimeInputId}`,
    workspaceId: "wksp_test",
    sessionId,
    sessionThreadId: `thrd_${sessionId}`,
    bindingId: `bind_${sessionId}`,
    bindingGeneration: 1,
    targetPodUid: `pod_${sessionId}`,
    runtimeInputId,
    eventIds: [`sevt_${runtimeInputId}`],
    sequenceFrom: 1,
    sequenceTo: 1,
    kind: "messages",
    payloadJson: "{}",
  };
}

function threadControl(
  sessionId: string,
  runtimeInputId = `rin_control_${sessionId}`,
): RuntimeThreadControlState {
  return {
    requestId: `req_${runtimeInputId}`,
    workspaceId: "wksp_test",
    sessionId,
    sessionThreadId: `thrd_${sessionId}`,
    bindingId: `bind_${sessionId}`,
    bindingGeneration: 1,
    targetPodUid: `pod_${sessionId}`,
    runtimeInputId,
    eventIds: [`sevt_${runtimeInputId}`],
    sequenceFrom: 1,
    sequenceTo: 1,
  };
}

interface ManagerCall {
  readonly method:
    | "acceptInput"
    | "interruptControl"
    | "resolveToolConfirmation"
    | "commitTaskNotification"
    | "applyRuntimeConfigPatch"
    | "cleanupSession"
    | "preloadThread"
    | "interruptThread"
    | "interruptReviewerExecution"
    | "markThreadClosed"
    | "markThreadActive"
    | "markAgentMailPulled"
    | "waitThread"
    | "waitReviewerExecution"
    | "inspectThread"
    | "inspectReviewerExecution"
    | "shutdownActiveRuns";
  readonly args: readonly unknown[];
}

function fakeManagerLayer(calls: ManagerCall[]): Layer.Layer<SessionManager.Service> {
  return Layer.succeed(
    SessionManager.Service,
    SessionManager.Service.of({
      acceptInput: (...args: readonly [RuntimeAcceptedInputState, ...unknown[]]) =>
        Effect.sync(() => {
          calls.push({ method: "acceptInput", args });
          const sessionId = args[0].sessionId;
          return { ok: true as const, sessionId, created: false, started: true, pendingWake: false };
        }),
      interruptControl: (...args: readonly [string, SessionManager.RuntimeInterruptControlCommand, ...unknown[]]) =>
        Effect.sync(() => {
          calls.push({ method: "interruptControl", args });
          const sessionId = args[0];
          return { ok: true as const, sessionId, created: false, interrupted: true, idleInterrupt: false };
        }),
      resolveToolConfirmation: (...args: readonly [string, Parameters<SessionManager.Interface["resolveToolConfirmation"]>[1], ...unknown[]]) =>
        Effect.sync(() => {
          calls.push({ method: "resolveToolConfirmation", args });
          const sessionId = args[0];
          return { ok: true as const, sessionId, created: false, applied: true };
        }),
      commitTaskNotification: (...args: readonly [string, Parameters<SessionManager.Interface["commitTaskNotification"]>[1], ...unknown[]]) =>
        Effect.sync(() => {
          calls.push({ method: "commitTaskNotification", args });
          const sessionId = args[0];
          return { ok: true as const, sessionId, created: false, applied: true };
        }),
      applyRuntimeConfigPatch: (...args: readonly [string, Parameters<SessionManager.Interface["applyRuntimeConfigPatch"]>[1], ...unknown[]]) =>
        Effect.sync(() => {
          calls.push({ method: "applyRuntimeConfigPatch", args });
          const sessionId = args[0];
          return { ok: true as const, sessionId, created: false, applied: true };
        }),
      cleanupSession: (...args: readonly [string, ...unknown[]]) =>
        Effect.sync(() => {
          calls.push({ method: "cleanupSession", args });
          const sessionId = args[0];
          return { ok: true as const, sessionId, cleaned: true };
        }),
      preloadThread: (...args: readonly [Parameters<SessionManager.Interface["preloadThread"]>[0], ...unknown[]]) =>
        Effect.sync(() => {
          calls.push({ method: "preloadThread", args });
          const command = args[0];
          return { ok: true as const, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, applied: true };
        }),
      interruptThread: (...args: readonly [Parameters<SessionManager.Interface["interruptThread"]>[0], ...unknown[]]) =>
        Effect.sync(() => {
          calls.push({ method: "interruptThread", args });
          const command = args[0];
          return { ok: true as const, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, applied: true };
        }),
      interruptReviewerExecution: (...args: readonly [Parameters<SessionManager.Interface["interruptReviewerExecution"]>[0], SessionManager.ReviewerExecutionToken, ...unknown[]]) =>
        Effect.sync(() => {
          calls.push({ method: "interruptReviewerExecution", args });
          const command = args[0];
          return { ok: true as const, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, applied: true, terminal: true };
        }),
      markThreadClosed: (...args: readonly [Parameters<SessionManager.Interface["markThreadClosed"]>[0], ...unknown[]]) =>
        Effect.sync(() => {
          calls.push({ method: "markThreadClosed", args });
          const command = args[0];
          return { ok: true as const, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, applied: true };
        }),
      markThreadActive: (...args: readonly [Parameters<SessionManager.Interface["markThreadActive"]>[0], ...unknown[]]) =>
        Effect.sync(() => {
          calls.push({ method: "markThreadActive", args });
          const command = args[0];
          return { ok: true as const, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, applied: true };
        }),
      markAgentMailPulled: (...args: readonly [Parameters<SessionManager.Interface["markAgentMailPulled"]>[0], string, ...unknown[]]) =>
        Effect.sync(() => {
          calls.push({ method: "markAgentMailPulled", args });
          const command = args[0];
          return { ok: true as const, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, applied: true };
        }),
      waitThread: (...args: readonly [Parameters<SessionManager.Interface["waitThread"]>[0], number | undefined, ...unknown[]]) =>
        Effect.sync(() => {
          calls.push({ method: "waitThread", args });
          const command = args[0];
          return { ok: true as const, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, observed: true, status: "idle" as const, timedOut: false };
        }),
      waitReviewerExecution: (...args: readonly [Parameters<SessionManager.Interface["waitReviewerExecution"]>[0], SessionManager.ReviewerExecutionToken, number | undefined, ...unknown[]]) =>
        Effect.sync(() => {
          calls.push({ method: "waitReviewerExecution", args });
          const command = args[0];
          return { ok: true as const, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, status: "idle" as const, terminal: true, timedOut: false };
        }),
      inspectThread: (...args: readonly [Parameters<SessionManager.Interface["inspectThread"]>[0], ...unknown[]]) =>
        Effect.sync(() => {
          calls.push({ method: "inspectThread", args });
          const command = args[0];
          return { ok: true as const, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, observed: true, status: "idle" as const, messages: [] };
        }),
      inspectReviewerExecution: (...args: readonly [Parameters<SessionManager.Interface["inspectReviewerExecution"]>[0], SessionManager.ReviewerExecutionToken, ...unknown[]]) =>
        Effect.sync(() => {
          calls.push({ method: "inspectReviewerExecution", args });
          const command = args[0];
          return { ok: true as const, sessionId: command.sessionId, sessionThreadId: command.sessionThreadId, observed: true as const, status: "idle" as const, messages: [] };
        }),
      shutdownActiveRuns: () =>
        Effect.sync(() => {
          calls.push({ method: "shutdownActiveRuns", args: [] });
        }),
    }),
  );
}

class QueuedContextLoader implements ContextLoader {
  readonly buildCalls: string[] = [];
  readonly pendingCalls: string[] = [];
  readonly commitCalls: RuntimeAcceptedInputState[] = [];

  constructor(
    private readonly history: readonly RuntimeMessage[],
    private readonly pendingResults: PendingInputResult[],
    private readonly acceptedResults: Array<AcceptedInputCommitResult | ((input: RuntimeAcceptedInputState) => AcceptedInputCommitResult)> = [],
  ) {}

  async buildContext(sessionId: string): Promise<readonly RuntimeMessage[]> {
    this.buildCalls.push(sessionId);
    return this.history;
  }

  async loadPendingInput(sessionId: string): Promise<PendingInputResult> {
    this.pendingCalls.push(sessionId);
    return this.pendingResults.shift() ?? { type: "empty" };
  }

  async commitAcceptedInput(input: RuntimeAcceptedInputState): Promise<AcceptedInputCommitResult> {
    this.commitCalls.push(input);
    const result = this.acceptedResults.shift();
    if (typeof result === "function") {
      return result(input);
    }
    return result ?? { type: "empty" };
  }
}

class HostRuntimeStore extends RuntimeMessageStore {
  readonly messages = new Map<string, RuntimeMessage>();
  readonly writes: string[] = [];

  protected async writeMessageRecord(message: RuntimeMessageInfo, _controls: RuntimeMessageStoreOperationControls): Promise<unknown> {
    this.writes.push(`message:${message.status}`);
    const existing = this.messages.get(message.id);
    this.messages.set(message.id, RuntimeMessageSchema.parse({ ...message, parts: existing?.parts ?? [] }));
    return { ok: true, messageId: message.id, operation: "writeMessage" };
  }

  protected async writePartRecord(part: RuntimePart, _controls: RuntimeMessageStoreOperationControls): Promise<unknown> {
    this.writes.push(`part:${part.type}`);
    const existing = this.messages.get(part.messageId);
    if (existing === undefined) {
      return {
        ok: false,
        error: normalizeRuntimeMessageStoreError({
          code: "not_found",
          operation: "writePart",
          sessionId: part.sessionId,
          messageId: part.messageId,
          partId: part.id,
        }),
      };
    }
    this.messages.set(
      existing.id,
      RuntimeMessageSchema.parse({
        ...existing,
        parts: [...existing.parts.filter((current) => current.id !== part.id), part].sort((left, right) => left.sequence - right.sequence),
      }),
    );
    return { ok: true, messageId: part.messageId, partId: part.id, operation: "writePart" };
  }
}

class RecordingWriter implements SessionEventWriter {
  readonly events: SessionEvent[] = [];

  async append(envelope: SessionEventEnvelope): Promise<SessionEventWriterAppendResult> {
    this.events.push(envelope.event);
    return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
  }

  async writeRequestEnd(envelope: SessionEventWriterRequestEndEnvelope): Promise<SessionEventWriterAppendResult> {
    this.events.push({
      type: "span.model_request_end",
      model_request_start_id: envelope.modelRequestStartEventId,
      is_error: envelope.isError,
      ...(envelope.errorKind !== undefined ? { error_kind: envelope.errorKind } : {}),
      model_usage: {
        input_tokens: envelope.usage?.inputTokens ?? 0,
        output_tokens: envelope.usage?.outputTokens ?? 0,
        cache_creation_input_tokens: envelope.usage?.cacheWriteTokens ?? 0,
        cache_read_input_tokens: envelope.usage?.cacheReadTokens ?? 0,
        speed: null,
      },
    });
    return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
  }

  async finishIdle(envelope: SessionEventWriterFinishIdleEnvelope): Promise<SessionEventWriterAppendResult> {
    this.events.push({ type: "session.status_idle", stop_reason: envelope.stopReason });
    return { ok: true, writeId: envelope.writeId, eventId: `bridge-${envelope.writeId}`, processedAt: createdAt };
  }
}

class ControlledLLMService implements LLMServiceInterface {
  readonly requests: LLMRequest[] = [];
  private releaseCurrent: (() => void) | undefined;
  private releasePending = false;

  stream(request: LLMRequest) {
    this.requests.push(request);
    const service = this;
    return Stream.fromAsyncIterable(
      (async function* (): AsyncIterable<LLMEvent> {
        yield { type: "text-start", id: "text-1" };
        yield { type: "text-delta", id: "text-1", text_delta: "ok" };
        yield { type: "text-end", id: "text-1" };
        await new Promise<void>((resolve) => {
          if (service.releasePending) {
            service.releasePending = false;
            resolve();
            return;
          }
          service.releaseCurrent = resolve;
        });
        yield { type: "finish", finishReason: "stop" };
      })(),
      () => ({
        type: "llm-service" as const,
        error: {
          providerId: "fake",
          modelId: "fake-chat",
          code: "provider_unknown",
          message: "Provider stream failed.",
          retryable: false,
          fatal: true,
        } as never,
      }),
    );
  }

  release(): void {
    if (this.releaseCurrent === undefined) {
      this.releasePending = true;
      return;
    }
    const release = this.releaseCurrent;
    this.releaseCurrent = undefined;
    release();
  }
}

function runtime(): RuntimeDependencies {
  let counter = 0;
  return {
    now: () => createdAt,
    monotonicMs: () => 0,
    createId: (prefix: string) => `${prefix}-${++counter}`,
    sleep: async () => true,
  };
}

function fullHostLayer(options: {
  readonly loader: ContextLoader;
  readonly store: HostRuntimeStore;
  readonly writer: RecordingWriter;
  readonly llmService: LLMServiceInterface;
}): Layer.Layer<SessionRunHost.Service> {
  const agentLoopLayer = AgentLoop.layer({
    messageStore: options.store,
    sessionEventWriter: options.writer,
    runtime: runtime(),
    llmService: options.llmService,
    runtimeModel: () => ({ providerId: "fake", modelId: "fake-chat" }),
    storeOperationTimeoutMs: 1_000,
    providerCallRuntime: {
      systemInstructions: "SessionRunHost integration test.",
      timeoutMs: 1_800_000,
    },
  }).pipe(Layer.provide(AgentLoop.contextLoaderLayer(options.loader)));
  const managerLayer = SessionManager.layer({
    maxLocalSessions: 10,
    now: () => createdAt,
  }).pipe(Layer.provide(agentLoopLayer));
  return SessionRunHost.layer.pipe(Layer.provide(managerLayer));
}

async function withHost<T>(
  layer: Layer.Layer<SessionRunHost.Service>,
  useHost: (host: SessionRunHost.Interface) => Promise<T>,
): Promise<T> {
  const { host, scope } = await Effect.runPromise(
    Effect.gen(function* () {
      const layerScope = yield* Scope.make();
      const context = yield* Layer.buildWithScope(layer, layerScope);
      return { host: Context.get(context, SessionRunHost.Service), scope: layerScope };
    }),
  );
  try {
    return await useHost(host);
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
}

async function waitForCondition(predicate: () => boolean, label: string): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  throw new Error(`timed out waiting for ${label}`);
}

describe("SessionRunHost", () => {
  test("handlers route to the exact SessionManager command boundary", async () => {
    const calls: ManagerCall[] = [];
    const bridgeProjection = runtimeNotificationMessage("sesn_5");

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const host = yield* SessionRunHost.Service;
        const accepted = yield* host.handleAcceptInput(acceptedInput("sesn_2"));
        const interrupt = yield* host.handleInterruptControl("sesn_3", { ...threadControl("sesn_3"), runtimeInputId: "rin_interrupt", sequenceTo: 3 });
        const confirmation = yield* host.handleToolConfirmation("sesn_4", {
          ...threadControl("sesn_4"),
          runtimeInputId: "rin_confirm",
          sourceEventId: "sevt_confirm_1",
          toolUseEventId: "sevt_tool_1",
          decision: "allow",
        });
        const task = yield* host.handleTaskNotification("sesn_5", {
          ...threadControl("sesn_5"),
          runtimeInputId: "rin_task",
          taskId: "task_1",
          sourceToolUseEventId: "sevt_tool_1",
          status: "completed",
          payloadJson: "{\"task_id\":\"task_1\",\"source_tool_use_event_id\":\"sevt_tool_1\",\"status\":\"completed\"}",
          bridgeProjection,
        });
        const config = yield* host.handleRuntimeConfigPatch("sesn_6", {
          ...threadControl("sesn_6"),
          runtimeInputId: "rin_config",
          generation: 3,
          payloadJson: "{\"config_generation\":3}",
        });
        const cleanup = yield* host.handleCleanupSession("sesn_7", threadControl("sesn_7"));
        return { accepted, interrupt, confirmation, task, config, cleanup };
      }).pipe(Effect.provide(SessionRunHost.layer.pipe(Layer.provide(fakeManagerLayer(calls))))),
    );

    expect(result).toEqual({
      accepted: { ok: true, sessionId: "sesn_2", created: false, started: true, pendingWake: false },
      interrupt: { ok: true, sessionId: "sesn_3", created: false, interrupted: true, idleInterrupt: false },
      confirmation: { ok: true, sessionId: "sesn_4", created: false, applied: true },
      task: { ok: true, sessionId: "sesn_5", created: false, applied: true },
      config: { ok: true, sessionId: "sesn_6", created: false, applied: true },
      cleanup: { ok: true, sessionId: "sesn_7", cleaned: true },
    });
    expect(calls).toEqual([
      { method: "acceptInput", args: [expect.objectContaining({ sessionId: "sesn_2", runtimeInputId: "rin_sesn_2" })] },
      { method: "interruptControl", args: ["sesn_3", { ...threadControl("sesn_3"), runtimeInputId: "rin_interrupt", sequenceTo: 3 }, expect.any(Function)] },
      {
        method: "resolveToolConfirmation",
        args: [
          "sesn_4",
          { ...threadControl("sesn_4"), runtimeInputId: "rin_confirm", sourceEventId: "sevt_confirm_1", toolUseEventId: "sevt_tool_1", decision: "allow" },
        ],
      },
      {
        method: "commitTaskNotification",
        args: [
          "sesn_5",
          {
            ...threadControl("sesn_5"),
            runtimeInputId: "rin_task",
            taskId: "task_1",
            sourceToolUseEventId: "sevt_tool_1",
            status: "completed",
            payloadJson: "{\"task_id\":\"task_1\",\"source_tool_use_event_id\":\"sevt_tool_1\",\"status\":\"completed\"}",
            bridgeProjection,
          },
        ],
      },
      { method: "applyRuntimeConfigPatch", args: ["sesn_6", { ...threadControl("sesn_6"), runtimeInputId: "rin_config", generation: 3, payloadJson: "{\"config_generation\":3}" }] },
      { method: "cleanupSession", args: ["sesn_7", threadControl("sesn_7")] },
    ]);
  });

  test("payload-like extra input is ignored before it can reach SessionManager", async () => {
    const calls: ManagerCall[] = [];

    await Effect.runPromise(
      Effect.gen(function* () {
        const host = yield* SessionRunHost.Service;
        const unsafeHost = host as unknown as {
          readonly handleAcceptInput: (command: ReturnType<typeof acceptedInput>, payload: unknown) => ReturnType<typeof host.handleAcceptInput>;
          readonly handleCleanupSession: (sessionId: string, command: ReturnType<typeof threadControl>, payload: unknown) => ReturnType<typeof host.handleCleanupSession>;
        };
        yield* unsafeHost.handleAcceptInput(acceptedInput("sesn_2"), { modelId: "gpt-5", body: "must-not-cross" });
        yield* unsafeHost.handleCleanupSession("sesn_3", threadControl("sesn_3"), { event: { type: "user.message" } });
      }).pipe(Effect.provide(SessionRunHost.layer.pipe(Layer.provide(fakeManagerLayer(calls))))),
    );

    expect(calls).toEqual([
      { method: "acceptInput", args: [expect.objectContaining({ sessionId: "sesn_2", runtimeInputId: "rin_sesn_2" })] },
      { method: "cleanupSession", args: ["sesn_3", threadControl("sesn_3")] },
    ]);
  });

  test("host layer exposes only the command ingress and shutdown handlers", async () => {
    const calls: ManagerCall[] = [];
    const keys = await Effect.runPromise(
      Effect.gen(function* () {
        const host = yield* SessionRunHost.Service;
        return Object.keys(host).sort();
      }).pipe(Effect.provide(SessionRunHost.layer.pipe(Layer.provide(fakeManagerLayer(calls))))),
    );

    expect(keys).toEqual([
      "handleAcceptInput",
      "handleCleanupSession",
      "handleInspectReviewerExecution",
      "handleInspectThread",
      "handleInterruptControl",
      "handleInterruptReviewerExecution",
      "handleInterruptThread",
      "handleMarkAgentMailPulled",
      "handleMarkThreadActive",
      "handleMarkThreadClosed",
      "handlePreloadThread",
      "handleRuntimeConfigPatch",
      "handleTaskNotification",
      "handleToolConfirmation",
      "handleWaitReviewerExecution",
      "handleWaitThread",
      "shutdownActiveRuns",
    ]);
  });

  test("handleAcceptInput drives the real manager and agent loop through the fake runtime path", async () => {
    const loader = new QueuedContextLoader([], [], [{ type: "context", messages: [userMessage("user-1", 0, "hello")], runtimeBindingToken: "rtbt_sesn_1" }]);
    const store = new HostRuntimeStore();
    const writer = new RecordingWriter();
    const llmService = new ControlledLLMService();

    await withHost(fullHostLayer({ loader, store, writer, llmService }), async (host) => {
      expect(await Effect.runPromise(host.handleAcceptInput(acceptedInput("sesn_1")))).toEqual({
        ok: true,
        sessionId: "sesn_1",
        created: true,
        started: true,
        pendingWake: false,
      });
      await waitForCondition(() => llmService.requests.length === 1, "provider request");
      llmService.release();
      await waitForCondition(() => writer.events.some((event) => event.type === "session.status_idle"), "terminal idle");
    });

    expect(loader.buildCalls).toEqual([]);
    expect(loader.pendingCalls).toEqual([]);
    expect(loader.commitCalls).toEqual([expect.objectContaining({ sessionId: "sesn_1", runtimeInputId: "rin_sesn_1" })]);
    expect(llmService.requests).toHaveLength(1);
    expect(llmService.requests[0]?.runtimeBindingToken).toBe("rtbt_sesn_1");
    expect(store.writes).toEqual(["message:streaming", "part:text", "message:completed"]);
    expect(writer.events.map((event) => event.type)).toEqual([
      "session.status_running",
      "span.model_request_start",
      "agent.message",
      "span.model_request_end",
      "session.status_idle",
    ]);
  });

});
