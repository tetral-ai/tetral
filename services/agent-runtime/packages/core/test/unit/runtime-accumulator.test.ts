import { describe, expect, test } from "bun:test";
import type {
  RuntimeMessageInfo,
  RuntimeMessageStoreWriteMessageResult,
  RuntimeMessageStoreWritePartResult,
  RuntimeInternalToolRepairCommit,
  RuntimePart,
  SessionEvent,
  SessionEventWriterAppendResult,
  SessionEventWriterStableReasoningPart,
} from "../../src/contracts/runtime.js";
import type { LLMEvent } from "../../src/llm/llm-event.js";
import { LLMEventSchema } from "../../src/llm/llm-event.js";
import { normalizeRuntimeMessageStoreError } from "../../src/contracts/runtime.js";
import { runtimeFailureFromProviderError } from "../../src/llm/llm-event.js";
import { normalizeProviderError } from "../../src/contracts/provider.js";
import { internalToolRepairKey, SessionProcessor } from "../../src/runtime/accumulator.js";
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

function assistantShell(): RuntimeMessageInfo {
  return {
    id: "message-1",
    sessionId: "session-1",
    role: "assistant",
    origin: "agent",
    sequence: 1,
    status: "streaming",
    createdAt,
  };
}

function createProcessor(options: {
  readonly maxBytes?: number;
  readonly writeMessage?: (message: RuntimeMessageInfo) => Promise<RuntimeMessageStoreWriteMessageResult>;
  readonly writePart?: (part: RuntimePart) => Promise<RuntimeMessageStoreWritePartResult>;
  readonly commitInternalToolRepair?: (repair: RuntimeInternalToolRepairCommit) => Promise<RuntimeMessageStoreWritePartResult>;
  readonly appendEvent?: (
    event: SessionEvent,
    projectionJson?: string,
    stableReasoningParts?: readonly SessionEventWriterStableReasoningPart[],
    modelRequestId?: string,
    serverToolUse?: { readonly webSearchRequests: number; readonly webFetchRequests: number },
  ) => Promise<SessionEventWriterAppendResult>;
  readonly writes?: string[];
} = {}): SessionProcessor {
  let counter = 0;
  const writes = options.writes;
  return new SessionProcessor({
    messageId: "message-1",
    modelRequestId: "model-request-1",
    workspaceId: "workspace-1",
    sessionId: "session-1",
    sessionThreadId: "thread-1",
    message: assistantShell(),
    ...(options.maxBytes !== undefined ? { maxNormalizedTextPreviewBytes: options.maxBytes } : {}),
    now: () => createdAt,
    createId: (prefix) => `${prefix}-${++counter}`,
    writer: {
      writeMessage: async (message) => {
        writes?.push(`write-message:${message.status}`);
        return options.writeMessage?.(message) ?? { ok: true, messageId: message.id, operation: "writeMessage" };
      },
      writePart: async (part) => {
        writes?.push(`write-part:${part.type}`);
        return options.writePart?.(part) ?? { ok: true, messageId: part.messageId, partId: part.id, operation: "writePart" };
      },
      appendEvent: async (event, _source, projectionJson, stableReasoningParts, modelRequestId, serverToolUse) => {
        writes?.push(`append-event:${event.type}`);
        return options.appendEvent?.(event, projectionJson, stableReasoningParts, modelRequestId, serverToolUse) ?? {
          ok: true,
          writeId: `write-${writes?.filter((write) => write.startsWith("append-event")).length ?? 1}`,
          eventId: event.type === "agent.tool_use" || event.type === "agent.mcp_tool_use" ? "bridge-tool-use-1" : "bridge-event-1",
          processedAt: createdAt,
        };
      },
      commitInternalToolRepair: async (repair) => {
        writes?.push(`commit-repair:${repair.repairKey}`);
        if (options.commitInternalToolRepair !== undefined) {
          return await options.commitInternalToolRepair(repair);
        }
        const part = repair.message.parts[0];
        return {
          ok: true,
          messageId: repair.message.id,
          partId: part?.id ?? "",
          operation: "writePart",
        };
      },
    },
  });
}

describe("SessionProcessor", () => {
  test("reasoning-start durably emits one content-less thinking event without projection", async () => {
    const writes: string[] = [];
    const projections: Array<string | undefined> = [];
    const processor = createProcessor({
      writes,
      appendEvent: async (_event, projectionJson) => {
        projections.push(projectionJson);
        return { ok: true, writeId: "write-thinking", eventId: "sevt_thinking", processedAt: createdAt };
      },
    });

    const first = await processor.process(envelope({ type: "reasoning-start", id: "reasoning-1" }));
    const duplicate = await processor.process(envelope({ type: "reasoning-start", id: "reasoning-1" }));

    expect(first).toEqual({ ok: true, events: [{ type: "agent.thinking" }] });
    expect(duplicate).toEqual({ ok: true, events: [] });
    expect(writes).toEqual(["append-event:agent.thinking"]);
    expect(projections).toEqual([undefined]);
  });

  test("internal repair identities are bounded, tuple-safe, and stable across implementations", () => {
    const key = internalToolRepairKey("request", "call:a", "b");
    expect(key).toBe("internal_invalid_tool_6b53f75d29a34b47f5fdadebf740525a170464690d545d7deb4c9b859818b6fd");
    expect(key).not.toBe(internalToolRepairKey("request", "call", "a:b"));
    expect(internalToolRepairKey("请求", "调用", "工具")).toMatch(/^internal_invalid_tool_[0-9a-f]{64}$/);
  });

  test("provider text chunks produce one stable message event after durable writes", async () => {
    const writes: string[] = [];
    const modelRequestIds: Array<string | undefined> = [];
    const processor = createProcessor({
      writes,
      appendEvent: async (event, _projectionJson, _stableReasoningParts, modelRequestId) => {
        modelRequestIds.push(modelRequestId);
        return { ok: true, writeId: "write-text", eventId: "sevt_text", processedAt: createdAt };
      },
    });
    const events: SessionEvent[] = [];

    events.push(...(await processor.process(envelope({ type: "text-start", id: "text-1" }))).events);
    events.push(...(await processor.process(envelope({ type: "text-delta", id: "text-1", text_delta: "Hello" }))).events);
    events.push(...(await processor.process(envelope({ type: "text-end", id: "text-1" }))).events);
    events.push(...(await processor.process(envelope({ type: "finish", finishReason: "stop" }))).events);

    expect(writes).toEqual(["write-part:text", "append-event:agent.message", "write-message:completed"]);
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
      input: { value: { q: "x" }, preview: "{\"q\":\"x\"}", truncated: false },
    }))).events);
    events.push(...(await processor.commitPublicToolUse(source, "tool-1", "allow")).events);
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
      appendEvent: async (event, _projectionJson, stableReasoningParts) => {
        if (event.type === "agent.tool_use" || event.type === "agent.mcp_tool_use") {
          anchors.push({ type: event.type, parts: stableReasoningParts ?? [] });
        }
        return { ok: true, writeId: `write-${anchors.length}`, eventId: `bridge-tool-use-${anchors.length}`, processedAt: createdAt };
      },
    });
    await processor.process(envelope({ type: "reasoning-start", id: "reasoning-1" }));
    await processor.process(envelope({ type: "reasoning-delta", id: "reasoning-1", text_delta: "first", providerMetadata: { anthropic: { signature: "sig" } } }));
    await processor.process(envelope({ type: "reasoning-end", id: "reasoning-1" }));
    await processor.process(envelope({ type: "tool-call", id: "tool-1", toolName: "search", input: { value: {}, preview: "{}", truncated: false } }));
    await processor.commitPublicToolUse(source, "tool-1", "allow");
    await processor.process(envelope({ type: "tool-call", id: "tool-2", toolName: "create_issue", input: { value: {}, preview: "{}", truncated: false } }));
    await processor.commitPublicToolUse(source, "tool-2", "allow", { kind: "mcp", mcpServerName: "github" });

    expect(anchors).toEqual([
      { type: "agent.tool_use", parts: [expect.objectContaining({ reasoningPartId: "part-1", partSequence: 0, text: "first" })] },
      { type: "agent.mcp_tool_use", parts: [] },
    ]);
    expect(processor.isReasoningPartDurable("part-1")).toBe(true);
  });

  test("tool events carry internal projection metadata for Bridge durable context", async () => {
    const projections: unknown[] = [];
    const modelRequestIds: Array<{ readonly type: string; readonly modelRequestId: string | undefined }> = [];
    const processor = createProcessor({
      appendEvent: async (event, projectionJson, _stableReasoningParts, modelRequestId) => {
        modelRequestIds.push({ type: event.type, modelRequestId });
        if (projectionJson !== undefined) {
          projections.push(JSON.parse(projectionJson));
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
      input: { value: { q: "x" }, preview: "{\"q\":\"x\"}", truncated: false },
    }));
    await processor.commitPublicToolUse(source, "tool-1", "allow");
    await processor.commitToolSettlement(source, "tool-1", {
      type: "completed",
      output: { text: "done", truncated: false },
    });

    expect(projections).toEqual([
      {
        type: "runtime_tool_projection",
        message_id: "message-1",
        part_id: "part-1",
        part_sequence: 0,
        model_tool_call_id: "tool-1",
        tool_name: "search",
        input: { q: "x" },
        state: "running",
      },
      {
        type: "runtime_tool_projection",
        message_id: "message-1",
        part_id: "part-1",
        part_sequence: 0,
        model_tool_call_id: "tool-1",
        tool_name: "search",
        input: { q: "x" },
        state: "completed",
        output: { text: "done", truncated: false },
      },
    ]);
    expect(modelRequestIds).toEqual([
      { type: "agent.tool_use", modelRequestId: undefined },
      { type: "agent.tool_result", modelRequestId: "model-request-1" },
    ]);
  });

  test("MCP tools emit fork-SDK MCP tool use and result events", async () => {
    const modelRequestIds: Array<{ readonly type: string; readonly modelRequestId: string | undefined }> = [];
    const processor = createProcessor({
      appendEvent: async (event, _projectionJson, _stableReasoningParts, modelRequestId) => {
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
      input: { value: { title: "Bug" }, preview: "{\"title\":\"Bug\"}", truncated: false },
    }))).events);
    events.push(...(await processor.commitPublicToolUse(source, "tool-1", "allow", { kind: "mcp", mcpServerName: "github" })).events);
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
      { type: "agent.mcp_tool_use", modelRequestId: undefined },
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
        input: { value: { title: "Bug" }, preview: "{\"title\":\"Bug\"}", truncated: false },
      }))).events);
      events.push(...(await processor.commitPublicToolUse(source, "tool-1", "allow", { kind: "mcp", mcpServerName: "github" })).events);
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

  test("step start and finish are stable writes without public append events", async () => {
    const writes: string[] = [];
    const processor = createProcessor({ writes });

    const stepStarted = await processor.process(envelope({ type: "step-start", stepIndex: 1 }));
    const stepFinished = await processor.process(envelope({ type: "step-finish", finishReason: "stop" }));

    expect(stepStarted).toEqual({ ok: true, events: [] });
    expect(stepFinished).toEqual({ ok: true, events: [] });
    expect(writes).toEqual([
      "write-part:step-start",
      "write-part:step-finish",
      "write-message:streaming",
    ]);
  });

  test("step finish message update failure becomes persistence failure without public events", async () => {
    const writes: string[] = [];
    const processor = createProcessor({
      writes,
      writeMessage: async (message) => {
        if (message.status === "streaming") {
          return {
            ok: false,
            error: normalizeRuntimeMessageStoreError({
              code: "unavailable",
              operation: "writeMessage",
              sessionId: message.sessionId,
              messageId: message.id,
            }),
          };
        }
        return { ok: true, messageId: message.id, operation: "writeMessage" };
      },
    });

    await processor.process(envelope({ type: "step-start", stepIndex: 1 }));
    const result = await processor.process(envelope({ type: "step-finish", finishReason: "stop" }));

    expect(result.ok).toBe(false);
    expect(result.events).toEqual([]);
    expect(result).toMatchObject({
      error: { type: "message-store", operation: "writeMessage", code: "unavailable" },
    });
    expect(writes).toEqual([
      "write-part:step-start",
      "write-part:step-finish",
      "write-message:streaming",
      "write-message:failed",
    ]);
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
      input: { value: { q: "x" }, preview: "{\"q\":\"x\"}", truncated: false },
    }));
    const toolUse = await processor.commitPublicToolUse(source, "internal-tool-call", "allow");
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
      "write-part:tool",
      "append-event:agent.tool_use",
      "write-part:tool",
      "write-part:tool",
      "append-event:agent.tool_result",
    ]);
  });

  test("web usage follows only the matching durable tool-result append", async () => {
    const attachments: Array<{ type: string; usage: unknown }> = [];
    const processor = createProcessor({
      appendEvent: async (event, _projectionJson, _stableReasoningParts, _modelRequestId, serverToolUse) => {
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
      input: { value: { search_query: [{ q: "tetral" }] }, preview: "{}", truncated: false },
    }));
    await processor.commitPublicToolUse(source, "web-tool-call", "allow");
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
      input: { value: { q: "x" }, preview: "{\"q\":\"x\"}", truncated: false },
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
    let releaseCommit: ((result: RuntimeMessageStoreWritePartResult) => void) | undefined;
    let startedRepair: ((repair: RuntimeInternalToolRepairCommit) => void) | undefined;
    const commitStarted = new Promise<RuntimeInternalToolRepairCommit>((resolve) => {
      releaseCommit = undefined;
      startedRepair = resolve;
    });
    const processor = createProcessor({
      commitInternalToolRepair: async (repair) => {
        startedRepair?.(repair);
        return await new Promise<RuntimeMessageStoreWritePartResult>((commitResolve) => {
          releaseCommit = commitResolve;
        });
      },
    });
    await processor.process(envelope({
      type: "tool-call",
      id: "unknown-tool-call",
      toolName: "unknown_tool",
      input: { value: { q: "x" }, preview: "{\"q\":\"x\"}", truncated: false },
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
    expect(repair.message.id).toMatch(/^msg_repair_[0-9a-f]{64}$/);
    expect(repair.message.parts[0]?.id).toMatch(/^part_repair_[0-9a-f]{64}$/);
    expect(processor.messages().some((message) => message.id === repair.message.id)).toBe(false);
    releaseCommit?.({
      ok: true,
      messageId: repair.message.id,
      partId: repair.message.parts[0]?.id ?? "",
      operation: "writePart",
    });
    await expect(pendingRepair).resolves.toMatchObject({ ok: true });
    expect(processor.messages().some((message) => message.id === repair.message.id)).toBe(true);
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
    expect(writes).toEqual(["write-message:failed"]);
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
    const successfulToolParts: RuntimePart[] = [];
    const successProcessor = createProcessor({
      writePart: async (part) => {
        successfulToolParts.push(part);
        return { ok: true, messageId: part.messageId, partId: part.id, operation: "writePart" };
      },
    });
    const successEvents: SessionEvent[] = [];
    successEvents.push(...(await successProcessor.process(envelope({ type: "tool-input-start", id: "tool-ok", toolName: "search" }))).events);
    successEvents.push(...(await successProcessor.process(envelope({
      type: "tool-call",
      id: "tool-ok",
      toolName: "search",
      input: { value: { query: hostileText }, preview: hostileText, truncated: false },
    }))).events);
    successEvents.push(...(await successProcessor.commitPublicToolUse(source, "tool-ok", "allow")).events);
    successEvents.push(...(await successProcessor.commitToolSettlement(source, "tool-ok", {
      type: "completed",
      output: { text: hostileText, truncated: false },
    })).events);

    expect(successEvents.map((event) => event.type)).toEqual(["agent.tool_use", "agent.tool_result"]);
    expect(JSON.stringify(successEvents)).toContain(hostileText);
    expect(JSON.stringify(successfulToolParts)).toContain(hostileText);

    const processor = createProcessor();
    const events: SessionEvent[] = [];

    events.push(...(await processor.process(envelope({ type: "tool-input-start", id: "tool-1", toolName: "search" }))).events);
    events.push(...(await processor.process(envelope({
      type: "tool-call",
      id: "tool-1",
      toolName: "search",
      input: { value: { query: hostileText }, preview: hostileText, truncated: false },
    }))).events);
    events.push(...(await processor.commitPublicToolUse(source, "tool-1", "allow")).events);
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
    const written: RuntimePart[] = [];
    const processor = createProcessor({
      writePart: async (part) => {
        written.push(part);
        return { ok: true, messageId: part.messageId, partId: part.id, operation: "writePart" };
      },
    });
    await processor.process(envelope({ type: "tool-input-start", id: "tool-large", toolName: "Read" }));
    await processor.process(envelope({
      type: "tool-call",
      id: "tool-large",
      toolName: "Read",
      input: { value: { file_path: "notes/a.txt" }, preview: "{}", truncated: false },
    }));
    await processor.commitPublicToolUse(source, "tool-large", "allow");
    const output = `content: ${"x".repeat(60 * 1024)}\nnext_offset: 61440`;

    await processor.commitToolSettlement(source, "tool-large", {
      type: "completed",
      output: { text: output, truncated: false },
    });

    const completed = written.findLast((part) => part.type === "tool" && part.state.status === "completed");
    expect(completed?.type === "tool" && completed.state.status === "completed" ? completed.state.output.text : "").toBe(output);
  });

  test("hostile store writer failures are sanitized before processor failure output", async () => {
    const processor = createProcessor({
      writePart: async () => ({
        ok: false,
        error: {
          type: "message-store",
          code: "schema_mismatch",
          operation: "writePart",
          message: hostileText,
          retryable: false,
          fatal: true,
          constraint: hostileText,
          messageId: hostileText,
          partId: hostileText,
          sessionId: hostileText,
        },
      }),
    });

    await processor.process(envelope({ type: "text-start", id: "text-1" }));
    const result = await processor.process(envelope({ type: "text-end", id: "text-1" }));

    expect(result.ok).toBe(false);
    expectHostileTextRedacted(result);
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

  test("reasoning lifecycle preserves provider metadata before persistent part writes", async () => {
    const writtenParts: RuntimePart[] = [];
    const processor = createProcessor({
      writePart: async (part) => {
        writtenParts.push(part);
        return { ok: true, messageId: part.messageId, partId: part.id, operation: "writePart" };
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

    expect(writtenParts.map((part) => part.type)).toEqual(["reasoning"]);
    expect(writtenParts.at(-1)).toMatchObject({
      type: "reasoning",
      text: "think",
      providerMetadata: { anthropic: { redactedData: "red_1", signature: "sig_1" } },
      status: "completed",
    });
    expect(processor.messages()[0]?.parts).toEqual([
      expect.objectContaining({
        type: "reasoning",
        text: "think",
        providerMetadata: { anthropic: { redactedData: "red_1", signature: "sig_1" } },
      }),
    ]);
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

  test("failed stable writes do not emit the failed update event and become session errors", async () => {
    const processor = createProcessor({
      writePart: async () => ({
        ok: false,
        error: normalizeRuntimeMessageStoreError({
          code: "unavailable",
          operation: "writePart",
          sessionId: "session-1",
          messageId: "message-1",
          partId: "part-2",
        }),
      }),
    });

    await processor.process(envelope({ type: "text-start", id: "text-1" }));
    const result = await processor.process(envelope({ type: "text-end", id: "text-1" }));

    expect(result.ok).toBe(false);
    if (result.ok) {
      throw new Error("expected failed result");
    }
    expect(result.events.map((event) => event.type)).toEqual([]);
    expect(result.error.type).toBe("message-store");
  });

  test("provider error terminalizes active draft without projecting agent.message", async () => {
    const writes: string[] = [];
    const parts: RuntimePart[] = [];
    const appended: SessionEvent[] = [];
    const processor = createProcessor({
      writes,
      writePart: async (part) => {
        parts.push(part);
        return { ok: true, messageId: part.messageId, partId: part.id, operation: "writePart" };
      },
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
    expect(parts).toEqual([expect.objectContaining({ type: "text", text: "visible", status: "failed" })]);
    expect(writes).toEqual(["write-part:text", "write-message:failed"]);
    expect(appended).toEqual([]);
    expect(result.events.map((event) => event.type)).toEqual(["session.error"]);
    expect(JSON.stringify(result.events)).not.toContain("raw-secret-body");
    expect(JSON.stringify(result.events)).not.toContain("https://secret.example/path");
  });

  test("cancellation terminalizes active draft without projecting agent.message", async () => {
    const writes: string[] = [];
    const parts: RuntimePart[] = [];
    const appended: SessionEvent[] = [];
    const processor = createProcessor({
      writes,
      writePart: async (part) => {
        parts.push(part);
        return { ok: true, messageId: part.messageId, partId: part.id, operation: "writePart" };
      },
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
    expect(parts).toEqual([expect.objectContaining({ type: "text", text: "cancelled draft", status: "cancelled" })]);
    expect(writes).toEqual(["write-part:text", "write-message:cancelled"]);
    expect(appended).toEqual([]);
    expect(result.events.map((event) => event.type)).toEqual(["session.error"]);
  });

  test("provider error terminalizes open reasoning before failed message", async () => {
    const writes: string[] = [];
    const parts: RuntimePart[] = [];
    const processor = createProcessor({
      writes,
      writePart: async (part) => {
        parts.push(part);
        return { ok: true, messageId: part.messageId, partId: part.id, operation: "writePart" };
      },
    });

    await processor.process(envelope({ type: "reasoning-start", id: "reasoning-1" }));
    await processor.process(envelope({ type: "reasoning-delta", id: "reasoning-1", text_delta: "thinking" }));
    const result = await processor.process(envelope({
      type: "provider-error",
      error: providerFailure({ code: "provider_stream_error", retryable: false, providerId: "fake", modelId: "fake-chat" }),
    }));

    expect(result.ok).toBe(true);
    expect(parts).toEqual([
      expect.objectContaining({ type: "reasoning", text: "thinking", status: "failed" }),
    ]);
    expect(writes).toEqual([
      "append-event:agent.thinking",
      "write-part:reasoning",
      "write-message:failed",
    ]);
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
      input: { value: { q: "x" }, preview: "{\"q\":\"x\"}", truncated: false },
    }));
    await processor.commitPublicToolUse(source, "running-tool", "allow");
    const result = await processor.process(envelope({
      type: "provider-error",
      error: providerFailure({ code: "provider_stream_error", retryable: false, providerId: "fake", modelId: "fake-chat" }),
    }));

    expect(result.ok).toBe(true);
    expect(writes).toEqual([
      "write-part:tool",
      "append-event:agent.tool_use",
      "write-part:tool",
      "write-part:tool",
      "write-part:tool",
      "append-event:agent.tool_result",
      "write-message:failed",
    ]);
    expect(appended).toEqual([
      expect.objectContaining({ type: "agent.tool_use", name: "search", evaluated_permission: "allow" }),
      expect.objectContaining({ type: "agent.tool_result", tool_use_id: "bridge-tool-running", is_error: true }),
    ]);
    expect(result.events.map((event) => event.type)).toEqual(["agent.tool_result", "session.error"]);
  });
});
