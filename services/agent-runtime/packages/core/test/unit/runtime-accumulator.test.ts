import { describe, expect, test } from "bun:test";
import type {
  RuntimeMessageDraft,
  RuntimeInternalToolRepairCommit,
  RuntimeInternalToolRepairCommitResult,
  SessionEvent,
  SessionEventWriterAppendResult,
  SessionEventWriterStableReasoningPart,
} from "../../src/contracts/runtime.js";
import type { LLMEvent } from "../../src/llm/llm-event.js";
import { LLMEventSchema } from "../../src/llm/llm-event.js";
import { runtimeFailureFromProviderError } from "../../src/llm/llm-event.js";
import { normalizeProviderError } from "../../src/contracts/provider.js";
import { internalToolRepairKey, SessionProcessor } from "../../src/runtime/accumulator.js";
import {
  runtimeOutputDraft,
  runtimeWorkingAssistantDraft,
} from "../../src/runtime/runtime-declaration.js";
import type { RuntimeProcessorSource } from "../../src/runtime/accumulator.js";
import { toGatewayRuntimeMessages } from "../../src/runtime/message-projection.js";

const createdAt = "2026-01-01T00:00:00.000Z";
const source: RuntimeProcessorSource = { providerId: "fake", modelId: "fake-chat" };
const hostileText = [
  "UNIT1_DUMMY_TOKEN_CANARY",
  "select * from secrets",
  "postgres://user:pass@db.internal/app",
  "authorization: bearer raw-header-secret",
  "system prompt raw backend payload marker",
  "raw provider payload marker",
].join(" ");

function expectHostileTextRedacted(value: unknown): void {
  const serialized = JSON.stringify(value);
  for (const marker of [
    "UNIT1_DUMMY_TOKEN_CANARY",
    "select * from secrets",
    "postgres://user:pass@db.internal/app",
    "authorization: bearer raw-header-secret",
    "system prompt raw backend payload marker",
    "raw provider payload marker",
  ]) {
    expect(serialized).not.toContain(marker);
  }
}

function envelope(event: LLMEvent): RuntimeProcessorSource & { readonly event: LLMEvent } {
  return { providerId: "fake", modelId: "fake-chat", event };
}

function providerFailure(input: Parameters<typeof normalizeProviderError>[0]): ReturnType<typeof runtimeFailureFromProviderError> {
  return runtimeFailureFromProviderError(normalizeProviderError(input));
}

function assistantShell(): RuntimeMessageDraft {
  return runtimeWorkingAssistantDraft({
    workspaceId: "workspace-1",
    sessionId: "session-1",
    sessionThreadId: "thread-1",
    modelRequestId: "model-request-1",
  });
}

function createProcessor(options: {
  readonly maxBytes?: number;
  readonly commitInternalToolRepair?: (repair: RuntimeInternalToolRepairCommit) => Promise<RuntimeInternalToolRepairCommitResult>;
  readonly appendEvent?: (
    event: SessionEvent,
    output?: {
      readonly draftKind: RuntimeMessageDraft["draftKind"];
      readonly message: RuntimeMessageDraft;
    },
    stableReasoningParts?: readonly SessionEventWriterStableReasoningPart[],
    modelRequestId?: string,
    serverToolUse?: { readonly webSearchRequests: number; readonly webFetchRequests: number },
  ) => Promise<SessionEventWriterAppendResult>;
  readonly writes?: string[];
} = {}): SessionProcessor {
  let durableMessageCreated = false;
  let owningEventId = "";
  let eventSequence = 0;
  const partIds = new Map<string, string>();
  const writes = options.writes;
  return new SessionProcessor({
    modelRequestId: "model-request-1",
    requestId: "request-1",
    workspaceId: "workspace-1",
    sessionId: "session-1",
    sessionThreadId: "thread-1",
    bindingId: "binding-1",
    bindingGeneration: 1,
    targetPodUid: "pod-1",
    message: assistantShell(),
    ...(options.maxBytes !== undefined ? { maxNormalizedTextPreviewBytes: options.maxBytes } : {}),
    now: () => createdAt,
    writer: {
      appendEvent: async (event, _source, output, stableReasoningParts, modelRequestId, serverToolUse) => {
        writes?.push(`append-event:${event.type}`);
        const writeId = `write-${writes?.filter((write) => write.startsWith("append-event")).length ?? 1}`;
        const eventId = event.type === "agent.tool_use" || event.type === "agent.mcp_tool_use"
          ? "bridge-tool-use-1"
          : `bridge-event-${eventSequence + 1}`;
        const custom = await options.appendEvent?.(event, output, stableReasoningParts, modelRequestId, serverToolUse);
        const result = custom ?? {
          ok: true,
          writeId,
          eventId,
          processedAt: createdAt,
        };
        if (!result.ok || output === undefined || result.declaration !== undefined) {
          return result;
        }
        eventSequence += 1;
        const draft = runtimeOutputDraft({
          workspaceId: "workspace-1",
          sessionId: "session-1",
          sessionThreadId: "thread-1",
          runtimeWriteId: result.writeId,
          eventType: event.type,
          draftKind: output.draftKind,
          message: output.message,
        });
        if (!durableMessageCreated) {
          owningEventId = result.eventId;
        }
        const messageDisposition = durableMessageCreated ? "updated" as const : "created" as const;
        const partStamps = draft.parts.map((part) => {
          const association = part.type === "tool"
            ? `tool:${part.toolCallId}`
            : part.type === "reasoning" && part.providerPartId !== undefined
              ? `reasoning:${part.providerPartId}`
              : `${part.type}:${part.ordinal}`;
          const existingId = partIds.get(association);
          const partId = existingId ?? `durable-part-${partIds.size + 1}`;
          partIds.set(association, partId);
          return {
            runtimeLocalPartId: part.runtimeLocalPartId,
            partId,
            messageId: "durable-message-1",
            partSequence: [...partIds.keys()].indexOf(association),
            createdAt,
            updatedAt: createdAt,
            disposition: existingId === undefined ? "created" as const : "updated" as const,
          };
        });
        durableMessageCreated = true;
        return {
          ...result,
          declaration: {
            applicationDisposition: "current_custody" as const,
            observedBindingId: "binding-1",
            observedBindingGeneration: 1,
            receipt: {
              sessionThreadId: "thread-1",
              operationKind: "write_event",
              sourceKind: event.type,
              sourceId: result.writeId,
              declarationDigest: "test-digest",
              pendingAttachmentDelta: [],
              pendingToolDelta: [],
              prefixConsumptions: [],

              childLifecycle: [],
              events: [{
                sessionThreadId: "thread-1",
                sourceEventId: result.writeId,
                eventId: result.eventId,
                eventSequence,
                disposition: "created" as const,
              }],
              messages: [{
                runtimeLocalId: draft.runtimeLocalId,
                sessionThreadId: "thread-1",
                owningEventId,
                messageId: "durable-message-1",
                messageSequence: 1,
                createdAt,
                updatedAt: createdAt,
                disposition: messageDisposition,
                parts: partStamps,
              }],
            },
          },
        };
      },
      commitInternalToolRepair: async (repair) => {
        writes?.push(`commit-repair:${repair.repairKey}`);
        if (options.commitInternalToolRepair !== undefined) {
          return await options.commitInternalToolRepair(repair);
        }
        const part = repair.draft.parts[0];
        return {
          ok: true,
          eventId: "bridge-internal-repair-event",
          declaration: {
            applicationDisposition: "current_custody",
            observedBindingId: "binding-1",
            observedBindingGeneration: 1,
            receipt: {
              sessionThreadId: "thread-1",
              operationKind: "commit_internal_tool_repair",
              sourceKind: "internal_tool_repair",
              sourceId: repair.repairKey,
              declarationDigest: "repair-digest",
              pendingAttachmentDelta: [],
              pendingToolDelta: [],
              prefixConsumptions: [],

              childLifecycle: [],
              events: [{
                sessionThreadId: "thread-1",
                sourceEventId: repair.repairKey,
                eventId: "bridge-internal-repair-event",
                eventSequence: 20,
                disposition: "created",
              }],
              messages: [{
                runtimeLocalId: repair.draft.runtimeLocalId,
                sessionThreadId: "thread-1",
                owningEventId: "bridge-internal-repair-event",
                messageId: "durable-repair-message",
                messageSequence: 2,
                createdAt,
                updatedAt: createdAt,
                disposition: "created",
                parts: [{
                  runtimeLocalPartId: part?.runtimeLocalPartId ?? "",
                  partId: "durable-repair-part",
                  messageId: "durable-repair-message",
                  partSequence: 0,
                  createdAt,
                  updatedAt: createdAt,
                  disposition: "created",
                }],
              }],
            },
          },
        };
      },
    },
  });
}

describe("SessionProcessor", () => {
  test("reasoning-start durably emits one content-less thinking event without projection", async () => {
    const writes: string[] = [];
    const outputs: Array<RuntimeMessageDraft | undefined> = [];
    const processor = createProcessor({
      writes,
      appendEvent: async (_event, output) => {
        outputs.push(output?.message);
        return { ok: true, writeId: "write-thinking", eventId: "sevt_thinking", processedAt: createdAt };
      },
    });

    const first = await processor.process(envelope({ type: "reasoning-start", id: "reasoning-1" }));
    const duplicate = await processor.process(envelope({ type: "reasoning-start", id: "reasoning-1" }));

    expect(first).toEqual({ ok: true, events: [{ type: "agent.thinking" }] });
    expect(duplicate).toEqual({ ok: true, events: [] });
    expect(writes).toEqual(["append-event:agent.thinking"]);
    expect(outputs).toEqual([undefined]);
  });

  test("internal repair identities are bounded, tuple-safe, and stable across implementations", () => {
    const key = internalToolRepairKey("request", "call:a", "b");
    expect(key).toBe("internal_invalid_tool_6b53f75d29a34b47f5fdadebf740525a170464690d545d7deb4c9b859818b6fd");
    expect(key).not.toBe(internalToolRepairKey("request", "call", "a:b"));
    expect(internalToolRepairKey("请求", "调用", "工具")).toMatch(/^internal_invalid_tool_[0-9a-f]{64}$/);
  });

  test("provider text chunks produce one durable message event", async () => {
    const writes: string[] = [];
    const modelRequestIds: Array<string | undefined> = [];
    const processor = createProcessor({
      writes,
      appendEvent: async (event, _output, _stableReasoningParts, modelRequestId) => {
        modelRequestIds.push(modelRequestId);
        return { ok: true, writeId: "write-text", eventId: "sevt_text", processedAt: createdAt };
      },
    });
    const events: SessionEvent[] = [];

    events.push(...(await processor.process(envelope({ type: "text-start", id: "text-1" }))).events);
    events.push(...(await processor.process(envelope({ type: "text-delta", id: "text-1", text_delta: "Hello" }))).events);
    events.push(...(await processor.process(envelope({ type: "text-end", id: "text-1" }))).events);
    events.push(...(await processor.process(envelope({ type: "finish", finishReason: "stop" }))).events);

    expect(writes).toEqual(["append-event:agent.message"]);
    expect(events.map((event) => event.type)).toEqual([
      "agent.message",
    ]);
    expect(events[0]).toMatchObject({ type: "agent.message", content: [{ type: "text", text: "Hello" }] });
    expect(modelRequestIds).toEqual(["model-request-1"]);
  });

  test("tool-input-end emits no public event while Runtime-owned tool settlement writes public events", async () => {
    const processor = createProcessor();
    const events: SessionEvent[] = [];

    events.push(...(await processor.process(envelope({ type: "tool-input-start", id: "tool-1", toolName: "search" }))).events);
    events.push(...(await processor.process(envelope({ type: "tool-input-delta", id: "tool-1", toolName: "search", text_delta: "{\"q\":" }))).events);
    const inputEnd = await processor.process(envelope({ type: "tool-input-end", id: "tool-1", toolName: "search" }));
    events.push(...inputEnd.events);
    events.push(...(await processor.process(envelope({
      type: "tool-call",
      id: "tool-1",
      toolName: "search",
      input: { q: "x" },
      inputPreview: { value: { q: "x" }, preview: "{\"q\":\"x\"}", truncated: false },
    }))).events);
    events.push(...(await processor.commitPublicToolUse(source, "tool-1", { q: "x" }, "allow")).events);
    events.push(...(await processor.commitToolSettlement(source, "tool-1", {
      type: "completed",
      output: { text: "done", truncated: false },
    })).events);

    expect(inputEnd.events).toEqual([]);
    expect(events.map((event) => event.type)).toEqual([
      "agent.tool_use",
      "agent.tool_result",
    ]);
    expect(events[0]).toMatchObject({ type: "agent.tool_use", name: "search", input: { q: "x" }, evaluated_permission: "allow" });
    expect(events[1]).toMatchObject({ type: "agent.tool_result", tool_use_id: "bridge-tool-use-1", content: [{ type: "text", text: "done" }] });
  });

  test("anchors only the completed preceding not-yet-durable reasoning prefix on public tools", async () => {
    const anchors: Array<{ type: string; parts: readonly SessionEventWriterStableReasoningPart[] }> = [];
    const processor = createProcessor({
      appendEvent: async (event, _output, stableReasoningParts) => {
        if (event.type === "agent.tool_use" || event.type === "agent.mcp_tool_use") {
          anchors.push({ type: event.type, parts: stableReasoningParts ?? [] });
        }
        return { ok: true, writeId: `write-${anchors.length}`, eventId: `bridge-tool-use-${anchors.length}`, processedAt: createdAt };
      },
    });
    await processor.process(envelope({ type: "reasoning-start", id: "reasoning-1" }));
    await processor.process(envelope({ type: "reasoning-delta", id: "reasoning-1", text_delta: "first", providerMetadata: { anthropic: { signature: "sig" } } }));
    await processor.process(envelope({ type: "reasoning-end", id: "reasoning-1" }));
    await processor.process(envelope({
      type: "tool-call", id: "tool-1", toolName: "search", input: {},
      inputPreview: { value: {}, preview: "{}", truncated: false },
    }));
    await processor.commitPublicToolUse(source, "tool-1", {}, "allow");
    await processor.process(envelope({
      type: "tool-call", id: "tool-2", toolName: "create_issue", input: {},
      inputPreview: { value: {}, preview: "{}", truncated: false },
    }));
    await processor.commitPublicToolUse(source, "tool-2", {}, "allow", { kind: "mcp", mcpServerName: "github" });

    expect(anchors).toEqual([
      { type: "agent.tool_use", parts: [expect.objectContaining({ partSequence: 0, text: "first" })] },
      { type: "agent.mcp_tool_use", parts: [] },
    ]);
    const reasoningPartId = anchors[0]?.parts[0]?.reasoningPartId;
    expect(reasoningPartId).toMatch(/^stid_[0-9a-f]{64}$/);
    expect(processor.isReasoningPartDurable(reasoningPartId ?? "")).toBe(true);
  });

  test("tool events carry loop-authored message drafts for Bridge durable context", async () => {
    const projections: unknown[] = [];
    const modelRequestIds: Array<{ readonly type: string; readonly modelRequestId: string | undefined }> = [];
    const processor = createProcessor({
      appendEvent: async (event, output, _stableReasoningParts, modelRequestId) => {
        modelRequestIds.push({ type: event.type, modelRequestId });
        if (output !== undefined) {
          projections.push(output.message);
        }
        return {
          ok: true,
          writeId: `write-${projections.length}`,
          eventId: event.type === "agent.tool_use" ? "bridge-tool-use-1" : "bridge-event-1",
          processedAt: createdAt,
        };
      },
    });

    await processor.process(envelope({
      type: "tool-call",
      id: "tool-1",
      toolName: "search",
      input: { q: "x" },
      inputPreview: { value: { q: "x" }, preview: "{\"q\":\"x\"}", truncated: false },
    }));
    await processor.commitPublicToolUse(source, "tool-1", { q: "x" }, "allow");
    await processor.commitToolSettlement(source, "tool-1", {
      type: "completed",
      output: { text: "done", truncated: false },
    });

    expect(projections).toHaveLength(2);
    expect(projections[0]).toMatchObject({
      role: "assistant",
      origin: "agent",
      parts: [{
        type: "tool",
        toolCallId: "tool-1",
        toolName: "search",
        state: {
          status: "running",
          input: { value: { q: "x" } },
        },
      }],
    });
    expect(projections[1]).toMatchObject({
      role: "assistant",
      origin: "agent",
      parts: [{
        type: "tool",
        toolCallId: "tool-1",
        toolName: "search",
        state: {
          status: "completed",
          output: { text: "done", truncated: false },
        },
      }],
    });
    expect(modelRequestIds).toEqual([
      { type: "agent.tool_use", modelRequestId: "model-request-1" },
      { type: "agent.tool_result", modelRequestId: "model-request-1" },
    ]);
  });

  test("a settled tool stays current when a sibling Tool Use is declared later", async () => {
    const projections: Array<{ readonly type: string; readonly message: RuntimeMessageDraft }> = [];
    let toolUseCount = 0;
    const processor = createProcessor({
      appendEvent: async (event, output) => {
        if (output !== undefined) {
          projections.push({ type: event.type, message: output.message });
        }
        if (event.type === "agent.tool_use") {
          toolUseCount += 1;
        }
        return {
          ok: true,
          writeId: `write-${projections.length}`,
          eventId: event.type === "agent.tool_use" ? `bridge-tool-use-${toolUseCount}` : `bridge-event-${projections.length}`,
          processedAt: createdAt,
        };
      },
    });

    for (const [id, path] of [["tool-1", "src/a.ts"], ["tool-2", "src/b.ts"]] as const) {
      await processor.process(envelope({
        type: "tool-call",
        id,
        toolName: "Read",
        input: { file_path: path },
        inputPreview: { value: { file_path: path }, preview: JSON.stringify({ file_path: path }), truncated: false },
      }));
    }
    await processor.commitPublicToolUse(source, "tool-1", { file_path: "src/a.ts" }, "allow");
    await processor.commitToolSettlement(source, "tool-1", {
      type: "completed",
      output: { text: "first done", truncated: false },
    });
    await processor.commitPublicToolUse(source, "tool-2", { file_path: "src/b.ts" }, "allow");

    const toolStates = projections.map(({ type, message }) => ({
      type,
      tools: message.parts
        .filter((part) => part.type === "tool")
        .map((part) => ({ id: part.toolCallId, status: part.state.status })),
    }));
    expect(toolStates).toEqual([
      { type: "agent.tool_use", tools: [{ id: "tool-1", status: "running" }] },
      { type: "agent.tool_result", tools: [{ id: "tool-1", status: "completed" }] },
      {
        type: "agent.tool_use",
        tools: [
          { id: "tool-1", status: "completed" },
          { id: "tool-2", status: "running" },
        ],
      },
    ]);
  });

  test("MCP tools emit fork-SDK MCP tool use and result events", async () => {
    const modelRequestIds: Array<{ readonly type: string; readonly modelRequestId: string | undefined }> = [];
    const processor = createProcessor({
      appendEvent: async (event, _output, _stableReasoningParts, modelRequestId) => {
        modelRequestIds.push({ type: event.type, modelRequestId });
        return {
          ok: true,
          writeId: `write-${modelRequestIds.length}`,
          eventId: event.type === "agent.mcp_tool_use" ? "bridge-tool-use-1" : "bridge-event-1",
          processedAt: createdAt,
        };
      },
    });
    const events: SessionEvent[] = [];

    events.push(...(await processor.process(envelope({
      type: "tool-call",
      id: "tool-1",
      toolName: "create_issue",
      input: { title: "Bug" },
      inputPreview: { value: { title: "Bug" }, preview: "{\"title\":\"Bug\"}", truncated: false },
    }))).events);
    events.push(...(await processor.commitPublicToolUse(source, "tool-1", { title: "Bug" }, "allow", { kind: "mcp", mcpServerName: "github" })).events);
    events.push(...(await processor.commitToolSettlement(source, "tool-1", {
      type: "completed",
      output: { text: "created", truncated: false },
    })).events);

    expect(events.map((event) => event.type)).toEqual([
      "agent.mcp_tool_use",
      "agent.mcp_tool_result",
    ]);
    expect(events[0]).toMatchObject({
      type: "agent.mcp_tool_use",
      name: "create_issue",
      input: { title: "Bug" },
      mcp_server_name: "github",
      evaluated_permission: "allow",
    });
    expect(events[1]).toMatchObject({
      type: "agent.mcp_tool_result",
      mcp_tool_use_id: "bridge-tool-use-1",
      content: [{ type: "text", text: "created" }],
    });
    expect(modelRequestIds).toEqual([
      { type: "agent.mcp_tool_use", modelRequestId: "model-request-1" },
      { type: "agent.mcp_tool_result", modelRequestId: "model-request-1" },
    ]);
  });

  test("MCP terminal auth and connection failures emit fork-SDK session error payloads", async () => {
    for (const scenario of [
      {
        publicErrorEvent: {
          type: "mcp_authentication_failed_error" as const,
          mcpServerName: "github",
          message: "MCP authentication failed after refresh.",
          retryStatus: { type: "terminal" as const },
        },
        expectedError: {
          type: "mcp_authentication_failed_error",
          mcp_server_name: "github",
          message: "MCP authentication failed after refresh.",
          retry_status: { type: "terminal" },
        },
      },
      {
        publicErrorEvent: {
          type: "mcp_connection_failed_error" as const,
          mcpServerName: "github",
          message: "MCP connection failed.",
          retryStatus: { type: "exhausted" as const },
        },
        expectedError: {
          type: "mcp_connection_failed_error",
          mcp_server_name: "github",
          message: "MCP connection failed.",
          retry_status: { type: "exhausted" },
        },
      },
      {
        publicErrorEvent: {
          type: "unknown_error" as const,
          message: "MCP tool idempotency conflict.",
          retryStatus: { type: "terminal" as const },
        },
        expectedError: {
          type: "unknown_error",
          message: "MCP tool idempotency conflict.",
          retry_status: { type: "terminal" },
        },
      },
    ]) {
      const processor = createProcessor();
      const events: SessionEvent[] = [];

      events.push(...(await processor.process(envelope({
        type: "tool-call",
        id: "tool-1",
        toolName: "create_issue",
        input: { title: "Bug" },
        inputPreview: { value: { title: "Bug" }, preview: "{\"title\":\"Bug\"}", truncated: false },
      }))).events);
      events.push(...(await processor.commitPublicToolUse(source, "tool-1", { title: "Bug" }, "allow", { kind: "mcp", mcpServerName: "github" })).events);
      events.push(...(await processor.commitToolSettlement(source, "tool-1", {
        type: "error",
        error: {
          type: "runtime",
          code: "runtime_invalid_sequence",
          message: scenario.publicErrorEvent.message,
          retryable: true,
          fatal: false,
          sessionId: "session-1",
          retryStatus: scenario.publicErrorEvent.retryStatus,
        },
        publicErrorEvent: scenario.publicErrorEvent,
      })).events);

      expect(events.map((event) => event.type)).toEqual([
        "agent.mcp_tool_use",
        "agent.mcp_tool_result",
        "session.error",
      ]);
      expect(events[1]).toMatchObject({
        type: "agent.mcp_tool_result",
        mcp_tool_use_id: "bridge-tool-use-1",
        is_error: true,
      });
      expect(events[2]).toMatchObject({
        type: "session.error",
        error: scenario.expectedError,
      });
    }
  });

  test("raw tool input lifecycle is scratch-only until authoritative tool call input arrives", async () => {
    const writes: string[] = [];
    const processor = createProcessor({ writes });

    const started = await processor.process(envelope({ type: "tool-input-start", id: "tool-1", toolName: "search" }));
    const delta = await processor.process(envelope({ type: "tool-input-delta", id: "tool-1", toolName: "search", text_delta: "{\"q\"" }));
    const ended = await processor.process(envelope({ type: "tool-input-end", id: "tool-1", toolName: "search" }));

    expect(started).toEqual({ ok: true, events: [] });
    expect(delta).toEqual({ ok: true, events: [] });
    expect(ended).toEqual({ ok: true, events: [] });
    expect(writes).toEqual([]);
  });

  test("step start and finish remain request-local without public append events", async () => {
    const writes: string[] = [];
    const processor = createProcessor({ writes });

    const stepStarted = await processor.process(envelope({ type: "step-start", stepIndex: 1 }));
    const stepFinished = await processor.process(envelope({ type: "step-finish", finishReason: "stop" }));

    expect(stepStarted).toEqual({ ok: true, events: [] });
    expect(stepFinished).toEqual({ ok: true, events: [] });
    expect(writes).toEqual([]);
  });

  test("stable tool event append uses Bridge tool_use id for matching tool_result", async () => {
    const writes: string[] = [];
    const appended: SessionEvent[] = [];
    const processor = createProcessor({
      writes,
      appendEvent: async (event) => {
        appended.push(event);
        return {
          ok: true,
          writeId: `write-${appended.length}`,
          eventId: event.type === "agent.tool_use" ? "bridge-event-tool-use" : `bridge-event-${appended.length}`,
          processedAt: createdAt,
        };
      },
    });

    await processor.process(envelope({
      type: "tool-call",
      id: "internal-tool-call",
      toolName: "search",
      input: { q: "x" },
      inputPreview: { value: { q: "x" }, preview: "{\"q\":\"x\"}", truncated: false },
    }));
    const toolUse = await processor.commitPublicToolUse(source, "internal-tool-call", { q: "x" }, "allow");
    expect(toolUse).toMatchObject({ ok: true, toolUseEventId: "bridge-event-tool-use" });
    const result = await processor.commitToolSettlement(source, "internal-tool-call", {
      type: "completed",
      output: { text: "done", truncated: false },
    });

    expect(result.events).toEqual([
      {
        type: "agent.tool_result",
        tool_use_id: "bridge-event-tool-use",
        content: [{ type: "text", text: "done" }],
      },
    ]);
    expect(appended).toEqual([
      { type: "agent.tool_use", name: "search", input: { q: "x" }, evaluated_permission: "allow" },
      { type: "agent.tool_result", tool_use_id: "bridge-event-tool-use", content: [{ type: "text", text: "done" }] },
    ]);
    expect(writes).toEqual([
      "append-event:agent.tool_use",
      "append-event:agent.tool_result",
    ]);
  });

  test("web usage follows only the matching durable tool-result append", async () => {
    const attachments: Array<{ type: string; usage: unknown }> = [];
    const processor = createProcessor({
      appendEvent: async (event, _output, _stableReasoningParts, _modelRequestId, serverToolUse) => {
        attachments.push({ type: event.type, usage: serverToolUse });
        return {
          ok: true,
          writeId: `write-${attachments.length}`,
          eventId: event.type === "agent.tool_use" ? "bridge-web-tool-use" : `bridge-event-${attachments.length}`,
          processedAt: createdAt,
        };
      },
    });

    await processor.process(envelope({
      type: "tool-call",
      id: "web-tool-call",
      toolName: "web",
      input: { search_query: [{ q: "tetral" }] },
      inputPreview: { value: { search_query: [{ q: "tetral" }] }, preview: "{}", truncated: false },
    }));
    await processor.commitPublicToolUse(source, "web-tool-call", { search_query: [{ q: "tetral" }] }, "allow");
    await processor.commitToolSettlement(source, "web-tool-call", {
      type: "completed",
      output: { text: "web result", truncated: false },
      serverToolUse: { webSearchRequests: 2, webFetchRequests: 1 },
    });

    expect(attachments).toEqual([
      { type: "agent.tool_use", usage: undefined },
      { type: "agent.tool_result", usage: { webSearchRequests: 2, webFetchRequests: 1 } },
    ]);
  });

  test("provider-origin tool result is rejected at the LLM event boundary", () => {
    expect(LLMEventSchema.safeParse({
      type: "tool-result",
      id: "orphan-tool",
      output: { text: "done", truncated: false },
    }).success).toBe(false);
  });

  test("internal invalid-tool repair projects without public tool events", async () => {
    const appended: SessionEvent[] = [];
    const processor = createProcessor({
      appendEvent: async (event) => {
        appended.push(event);
        return { ok: true, writeId: `write-${appended.length}`, eventId: `bridge-event-${appended.length}`, processedAt: createdAt };
      },
    });

    await processor.process(envelope({
      type: "tool-call",
      id: "unknown-tool-call",
      toolName: "unknown_tool",
      input: { q: "x" },
      inputPreview: { value: { q: "x" }, preview: "{\"q\":\"x\"}", truncated: false },
    }));
    const repaired = await processor.commitInternalToolRepair(
      source,
      "unknown-tool-call",
      "model_request_1",
      internalToolRepairKey("model_request_1", "unknown-tool-call", "unknown_tool"),
      providerFailure({ code: "provider_tool_protocol_error", retryable: false, providerId: "fake", modelId: "fake-chat" }),
    );

    expect(repaired.ok).toBe(true);
    expect(repaired.events).toEqual([]);
    expect(appended).toEqual([]);
    const projected = toGatewayRuntimeMessages(processor.messages());
    expect(projected).toMatchObject({ ok: true });
    if (projected.ok) {
      expect(projected.messages[0]?.parts[0]?.tool).toMatchObject({
        callId: "unknown-tool-call",
        name: "unknown_tool",
        toolUseEventId: undefined,
      });
    }
  });

  test("internal invalid-tool repair hot projection waits for durable ack", async () => {
    let releaseCommit: ((result: RuntimeInternalToolRepairCommitResult) => void) | undefined;
    let startedRepair: ((repair: RuntimeInternalToolRepairCommit) => void) | undefined;
    const commitStarted = new Promise<RuntimeInternalToolRepairCommit>((resolve) => {
      releaseCommit = undefined;
      startedRepair = resolve;
    });
    const processor = createProcessor({
      commitInternalToolRepair: async (repair) => {
        startedRepair?.(repair);
        return await new Promise<RuntimeInternalToolRepairCommitResult>((commitResolve) => {
          releaseCommit = commitResolve;
        });
      },
    });
    await processor.process(envelope({
      type: "tool-call",
      id: "unknown-tool-call",
      toolName: "unknown_tool",
      input: { q: "x" },
      inputPreview: { value: { q: "x" }, preview: "{\"q\":\"x\"}", truncated: false },
    }));
    const pendingRepair = processor.commitInternalToolRepair(
      source,
      "unknown-tool-call",
      "model_request_1",
      internalToolRepairKey("model_request_1", "unknown-tool-call", "unknown_tool"),
      providerFailure({ code: "provider_tool_protocol_error", retryable: false, providerId: "fake", modelId: "fake-chat" }),
    );

    const repair = await commitStarted;
    expect(repair.repairKey).toMatch(/^internal_invalid_tool_[0-9a-f]{64}$/);
    expect(repair.draft.runtimeLocalId).toMatch(/^stid_[0-9a-f]{64}$/);
    expect(repair.draft).not.toHaveProperty("id");
    expect(repair.draft.parts[0]).not.toHaveProperty("id");
    expect(processor.messages()).toEqual([]);
    releaseCommit?.({
      ok: true,
      eventId: "bridge-internal-repair-event",
      declaration: {
        applicationDisposition: "current_custody",
        observedBindingId: "binding-1",
        observedBindingGeneration: 1,
        receipt: {
          sessionThreadId: "thread-1",
          operationKind: "commit_internal_tool_repair",
          sourceKind: "internal_tool_repair",
          sourceId: repair.repairKey,
          declarationDigest: "repair-digest",
          pendingAttachmentDelta: [],
              pendingToolDelta: [],
              prefixConsumptions: [],

              childLifecycle: [],
          events: [{
            sessionThreadId: "thread-1",
            sourceEventId: repair.repairKey,
            eventId: "bridge-internal-repair-event",
            eventSequence: 20,
            disposition: "created",
          }],
          messages: [{
            runtimeLocalId: repair.draft.runtimeLocalId,
            sessionThreadId: "thread-1",
            owningEventId: "bridge-internal-repair-event",
            messageId: "durable-repair-message",
            messageSequence: 2,
            createdAt,
            updatedAt: createdAt,
            disposition: "created",
            parts: [{
              runtimeLocalPartId: repair.draft.parts[0]?.runtimeLocalPartId ?? "",
              partId: "durable-repair-part",
              messageId: "durable-repair-message",
              partSequence: 0,
              createdAt,
              updatedAt: createdAt,
              disposition: "created",
            }],
          }],
        },
      },
    });
    await expect(pendingRepair).resolves.toMatchObject({ ok: true });
    expect(processor.messages().map((message) => message.id)).toEqual(["durable-repair-message"]);
  });

  test("orphan text and reasoning deltas do not write or emit events", async () => {
    const writes: string[] = [];
    const processor = createProcessor({ writes });

    const textDelta = await processor.process(envelope({ type: "text-delta", id: "missing-text", text_delta: "ignored" }));
    const textEnd = await processor.process(envelope({ type: "text-end", id: "missing-text" }));
    const reasoningDelta = await processor.process(envelope({ type: "reasoning-delta", id: "missing-reasoning", text_delta: "ignored" }));
    const reasoningEnd = await processor.process(envelope({ type: "reasoning-end", id: "missing-reasoning" }));

    expect(textDelta).toEqual({ ok: true, events: [] });
    expect(textEnd).toEqual({ ok: true, events: [] });
    expect(reasoningDelta).toEqual({ ok: true, events: [] });
    expect(reasoningEnd).toEqual({ ok: true, events: [] });
    expect(writes).toEqual([]);
  });

  test("finish with no stable part is invalid stream state", async () => {
    const writes: string[] = [];
    const processor = createProcessor({ writes });

    const result = await processor.process(envelope({ type: "finish", finishReason: "stop" }));

    expect(result.ok).toBe(true);
    expect(result.events).toContainEqual(expect.objectContaining({
      type: "session.error",
      error: expect.objectContaining({
        type: "runtime",
        code: "runtime_invalid_sequence",
      }),
    }));
    expect(writes).toEqual([]);
  });

  test("post-terminal provider events remain protocol failures", async () => {
    const processor = createProcessor();
    await processor.process(envelope({ type: "finish", finishReason: "stop" }));

    const result = await processor.process(envelope({ type: "finish", finishReason: "stop" }));

    expect(result).toMatchObject({
      ok: false,
      error: { type: "runtime", code: "gateway_protocol_error" },
    });
  });

  test("provider-origin tool error is rejected at the LLM event boundary", () => {
    expect(LLMEventSchema.safeParse({
      type: "tool-error",
      id: "orphan-tool",
      toolName: "search",
      error: providerFailure({
        providerId: "fake",
        modelId: "fake-chat",
        code: "provider_tool_protocol_error",
        retryable: false,
      }),
    }).success).toBe(false);
  });

  test("tool payloads stay verbatim while provider failure events are redacted", async () => {
    const successfulToolDrafts: RuntimeMessageDraft[] = [];
    const successProcessor = createProcessor({
      appendEvent: async (_event, output) => {
        if (output !== undefined) {
          successfulToolDrafts.push(output.message);
        }
        return {
          ok: true,
          writeId: `write-${successfulToolDrafts.length}`,
          eventId: successfulToolDrafts.length === 1 ? "bridge-tool-use-1" : "bridge-tool-result-1",
          processedAt: createdAt,
        };
      },
    });
    const successEvents: SessionEvent[] = [];
    successEvents.push(...(await successProcessor.process(envelope({ type: "tool-input-start", id: "tool-ok", toolName: "search" }))).events);
    successEvents.push(...(await successProcessor.process(envelope({
      type: "tool-call",
      id: "tool-ok",
      toolName: "search",
      input: { query: hostileText },
      inputPreview: { value: { query: hostileText }, preview: hostileText, truncated: false },
    }))).events);
    successEvents.push(...(await successProcessor.commitPublicToolUse(source, "tool-ok", { query: hostileText }, "allow")).events);
    successEvents.push(...(await successProcessor.commitToolSettlement(source, "tool-ok", {
      type: "completed",
      output: { text: hostileText, truncated: false },
    })).events);

    expect(successEvents.map((event) => event.type)).toEqual(["agent.tool_use", "agent.tool_result"]);
    expect(JSON.stringify(successEvents)).toContain(hostileText);
    expect(JSON.stringify(successfulToolDrafts)).toContain(hostileText);

    const processor = createProcessor();
    const events: SessionEvent[] = [];

    events.push(...(await processor.process(envelope({ type: "tool-input-start", id: "tool-1", toolName: "search" }))).events);
    events.push(...(await processor.process(envelope({
      type: "tool-call",
      id: "tool-1",
      toolName: "search",
      input: { query: hostileText },
      inputPreview: { value: { query: hostileText }, preview: hostileText, truncated: false },
    }))).events);
    events.push(...(await processor.commitPublicToolUse(source, "tool-1", { query: hostileText }, "allow")).events);
    events.push(...(await processor.commitToolSettlement(source, "tool-1", {
      type: "error",
      error: providerFailure({
        providerId: "fake",
        modelId: "fake-chat",
        code: "provider_tool_protocol_error",
        message: hostileText,
        retryable: false,
      }),
    })).events);

    expect(events.map((event) => event.type)).toEqual(["agent.tool_use", "agent.tool_result"]);
    expect(events[0]).toMatchObject({ type: "agent.tool_use", input: { query: hostileText } });
    expectHostileTextRedacted(events[1]);

    const failed = await processor.process(envelope({
      type: "provider-error",
      error: providerFailure({
        providerId: "fake",
        modelId: "fake-chat",
        code: "provider_stream_error",
        message: hostileText,
        retryable: false,
      }),
    }));
    expect(failed.events).toContainEqual(expect.objectContaining({ type: "session.error" }));
    expectHostileTextRedacted(failed.events);
  });

  test("preserves tool-route bounded output through durable settlement", async () => {
    const outputDrafts: RuntimeMessageDraft[] = [];
    const processor = createProcessor({
      appendEvent: async (event, output) => {
        if (output !== undefined) {
          outputDrafts.push(output.message);
        }
        return {
          ok: true,
          writeId: `write-${outputDrafts.length}`,
          eventId: event.type === "agent.tool_use" ? "bridge-tool-use-1" : "bridge-tool-result-1",
          processedAt: createdAt,
        };
      },
    });
    await processor.process(envelope({ type: "tool-input-start", id: "tool-large", toolName: "Read" }));
    await processor.process(envelope({
      type: "tool-call",
      id: "tool-large",
      toolName: "Read",
      input: { file_path: "notes/a.txt" },
      inputPreview: { value: { file_path: "notes/a.txt" }, preview: "{}", truncated: false },
    }));
    await processor.commitPublicToolUse(source, "tool-large", { file_path: "notes/a.txt" }, "allow");
    const output = `content: ${"x".repeat(60 * 1024)}\nnext_offset: 61440`;

    await processor.commitToolSettlement(source, "tool-large", {
      type: "completed",
      output: { text: output, truncated: false },
    });

    const completed = outputDrafts.at(-1)?.parts.findLast(
      (part) => part.type === "tool" && part.state.status === "completed",
    );
    expect(completed?.type === "tool" && completed.state.status === "completed" ? completed.state.output.text : "").toBe(output);
  });

  test("text delta updates only processor-local state before stable text write", async () => {
    const processor = createProcessor({ maxBytes: 5 });
    await processor.process(envelope({ type: "text-start", id: "text-1" }));
    const first = await processor.process(envelope({ type: "text-delta", id: "text-1", text_delta: "hello" }));
    const second = await processor.process(envelope({ type: "text-delta", id: "text-1", text_delta: "SECRET" }));
    const ended = await processor.process(envelope({ type: "text-end", id: "text-1" }));

    expect(first.events).toEqual([]);
    expect(second.events).toEqual([]);
    expect(ended.events).toEqual([expect.objectContaining({ type: "agent.message", content: [{ type: "text", text: "hello" }] })]);
    expect(JSON.stringify(ended.events)).not.toContain("SECRET");
  });

  test("stable text writes exclude completed reasoning until a tool anchors it", async () => {
    const drafts: RuntimeMessageDraft[] = [];
    const processor = createProcessor({
      appendEvent: async (_event, output) => {
        if (output !== undefined) {
          drafts.push(output.message);
        }
        return {
          ok: true,
          writeId: "write-text",
          eventId: "bridge-text-1",
          processedAt: createdAt,
        };
      },
    });

    await processor.process(envelope({ type: "reasoning-start", id: "reasoning-1" }));
    await processor.process(envelope({ type: "reasoning-delta", id: "reasoning-1", text_delta: "private thought" }));
    await processor.process(envelope({ type: "reasoning-end", id: "reasoning-1" }));
    await processor.process(envelope({ type: "text-start", id: "text-1" }));
    await processor.process(envelope({ type: "text-delta", id: "text-1", text_delta: "answer" }));
    await processor.process(envelope({ type: "text-end", id: "text-1" }));

    expect(drafts).toHaveLength(1);
    expect(drafts[0]?.parts).toEqual([
      expect.objectContaining({ type: "text", text: "answer" }),
    ]);
  });

  test("reasoning lifecycle preserves provider metadata locally before public anchoring", async () => {
    const anchoredReasoning: SessionEventWriterStableReasoningPart[] = [];
    const processor = createProcessor({
      appendEvent: async (event, _output, stableReasoningParts) => {
        if (event.type === "agent.tool_use") {
          anchoredReasoning.push(...(stableReasoningParts ?? []));
        }
        return {
          ok: true,
          writeId: "write-tool-use",
          eventId: "bridge-tool-use-1",
          processedAt: createdAt,
        };
      },
    });

    await processor.process(envelope({
      type: "reasoning-start",
      id: "reasoning-1",
      providerMetadata: { anthropic: { redactedData: "red_1" } },
    }));
    await processor.process(envelope({ type: "reasoning-delta", id: "reasoning-1", text_delta: "think" }));
    await processor.process(envelope({
      type: "reasoning-end",
      id: "reasoning-1",
      providerMetadata: { anthropic: { signature: "sig_1" } },
    }));
    await processor.process(envelope({
      type: "tool-call",
      id: "tool-1",
      toolName: "search",
      input: {},
      inputPreview: { value: {}, preview: "{}", truncated: false },
    }));
    await processor.commitPublicToolUse(source, "tool-1", {}, "allow");

    expect(anchoredReasoning).toEqual([expect.objectContaining({
      text: "think",
      providerMetadata: { anthropic: { redactedData: "red_1", signature: "sig_1" } },
    })]);
  });

  test("reasoning provider metadata is accepted at the normalized LLM event boundary", async () => {
    const result = LLMEventSchema.safeParse({
      type: "reasoning-start",
      id: "reasoning-1",
      providerMetadata: { anthropic: { signature: "sig_1" } },
    });

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data).toMatchObject({
        type: "reasoning-start",
        providerMetadata: { anthropic: { signature: "sig_1" } },
      });
    }
  });

  test("provider error discards an active text draft without projecting agent.message", async () => {
    const writes: string[] = [];
    const appended: SessionEvent[] = [];
    const processor = createProcessor({
      writes,
      appendEvent: async (event) => {
        appended.push(event);
        return { ok: true, writeId: "write-failed-draft", eventId: "sevt-failed-draft", processedAt: createdAt };
      },
    });
    await processor.process(envelope({ type: "text-start", id: "text-1" }));
    await processor.process(envelope({ type: "text-delta", id: "text-1", text_delta: "visible" }));

    const result = await processor.process(envelope({
      type: "provider-error",
      error: providerFailure({
        code: "provider_stream_error",
        message: "Provider failed https://secret.example/path raw-secret-body",
        retryable: false,
        providerId: "fake",
        modelId: "fake-chat",
      }),
    }));

    expect(result.ok).toBe(true);
    expect(writes).toEqual([]);
    expect(appended).toEqual([]);
    expect(result.events.map((event) => event.type)).toEqual(["session.error"]);
    expect(JSON.stringify(result.events)).not.toContain("raw-secret-body");
    expect(JSON.stringify(result.events)).not.toContain("https://secret.example/path");
  });

  test("cancellation discards an active text draft without projecting agent.message", async () => {
    const writes: string[] = [];
    const appended: SessionEvent[] = [];
    const processor = createProcessor({
      writes,
      appendEvent: async (event) => {
        appended.push(event);
        return { ok: true, writeId: "write-cancelled-draft", eventId: "sevt-cancelled-draft", processedAt: createdAt };
      },
    });
    await processor.process(envelope({ type: "text-start", id: "text-1" }));
    await processor.process(envelope({ type: "text-delta", id: "text-1", text_delta: "cancelled draft" }));

    const result = await processor.cancel(source, providerFailure({
      code: "provider_cancelled",
      message: "cancelled",
      retryable: false,
      providerId: "fake",
      modelId: "fake-chat",
    }));

    expect(result.ok).toBe(true);
    expect(writes).toEqual([]);
    expect(appended).toEqual([]);
    expect(result.events.map((event) => event.type)).toEqual(["session.error"]);
  });

  test("provider error discards open reasoning after its public thinking marker", async () => {
    const writes: string[] = [];
    const processor = createProcessor({ writes });

    await processor.process(envelope({ type: "reasoning-start", id: "reasoning-1" }));
    await processor.process(envelope({ type: "reasoning-delta", id: "reasoning-1", text_delta: "thinking" }));
    const result = await processor.process(envelope({
      type: "provider-error",
      error: providerFailure({ code: "provider_stream_error", retryable: false, providerId: "fake", modelId: "fake-chat" }),
    }));

    expect(result.ok).toBe(true);
    expect(writes).toEqual(["append-event:agent.thinking"]);
    expect(result.events.map((event) => event.type)).toEqual(["session.error"]);
  });

  test("provider error terminalizes pending and running tools without losing Bridge ids", async () => {
    const writes: string[] = [];
    const appended: SessionEvent[] = [];
    const processor = createProcessor({
      writes,
      appendEvent: async (event) => {
        appended.push(event);
        return {
          ok: true,
          writeId: `write-${appended.length}`,
          eventId: event.type === "agent.tool_use" ? "bridge-tool-running" : `bridge-event-${appended.length}`,
          processedAt: createdAt,
        };
      },
    });

    await processor.process(envelope({ type: "tool-input-start", id: "pending-tool", toolName: "lookup" }));
    await processor.process(envelope({
      type: "tool-call",
      id: "running-tool",
      toolName: "search",
      input: { q: "x" },
      inputPreview: { value: { q: "x" }, preview: "{\"q\":\"x\"}", truncated: false },
    }));
    await processor.commitPublicToolUse(source, "running-tool", { q: "x" }, "allow");
    const result = await processor.process(envelope({
      type: "provider-error",
      error: providerFailure({ code: "provider_stream_error", retryable: false, providerId: "fake", modelId: "fake-chat" }),
    }));

    expect(result.ok).toBe(true);
    expect(writes).toEqual([
      "append-event:agent.tool_use",
      "append-event:agent.tool_result",
    ]);
    expect(appended).toEqual([
      expect.objectContaining({ type: "agent.tool_use", name: "search", evaluated_permission: "allow" }),
      expect.objectContaining({ type: "agent.tool_result", tool_use_id: "bridge-tool-running", is_error: true }),
    ]);
    expect(result.events.map((event) => event.type)).toEqual(["agent.tool_result", "session.error"]);
  });
});
