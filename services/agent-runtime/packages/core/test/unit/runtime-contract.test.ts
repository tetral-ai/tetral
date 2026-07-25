import { describe, expect, test } from "bun:test";
import {
  ContextLoaderErrorSchema,
  LLMEventSchema,
  PendingInputResultSchema,
  RuntimeBoundedJsonSchema,
  RuntimeBoundedTextSchema,
  RuntimeFailureSchema,
  RuntimeInternalToolRepairCommitSchema,
  RuntimeMessageInfoSchema,
  RuntimeMessageSchema,
  RuntimeMessageStore,
  RuntimeMessageStoreErrorSchema,
  RuntimeMessageStoreOperationControlsSchema,
  RuntimeMessageStoreWriteMessageResultSchema,
  RuntimeMessageStoreWritePartResultSchema,
  RuntimePartSchema,
  RuntimeToolErrorSchema,
  MaxStableReasoningPartsPerRequest,
  SessionEventEnvelopeSchema,
  SessionEventSchema,
  SessionEventWriterAppendResultSchema,
  SessionEventWriterErrorSchema,
  SessionEventWriterRequestEndEnvelopeSchema,
  boundRuntimeJson,
  boundRuntimeText,
  boundRuntimeToolError,
  createRuntimeId,
  normalizeContextLoaderError,
  normalizeRuntimeFailure,
  normalizeRuntimeMessageStoreError,
  normalizeSessionEventWriterError,
  runtimeNow,
  runtimeSleep,
} from "../../src/contracts/runtime.js";
import { sessionEventForDurableWrite } from "../../src/runtime/session-event-writer.js";
import { normalizeProviderError } from "../../src/contracts/provider.js";
import type {
  RuntimeMessageInfo,
  RuntimeInternalToolRepairCommit,
  RuntimeMessageStoreOperationControls,
  RuntimeMessageStoreWriteMessageResult,
  RuntimeMessageStoreWritePartResult,
  RuntimePart,
  SessionEvent,
} from "../../src/contracts/runtime.js";

const signal = new AbortController().signal;
const createdAt = "2026-05-22T00:00:00.000Z";
const canary = "UNIT1_DUMMY_TOKEN_CANARY";
const rawSql = "select * from secrets";
const connectionString = "postgres://user:pass@db.internal/app";
const rawHeaders = "authorization: bearer raw-header-secret";
const rawPrompt = "system prompt raw backend payload marker";
const rawProviderPayload = "raw provider payload marker";

test("attachment_unavailable defaults to a non-fatal provider failure", () => {
  expect(normalizeProviderError({ code: "attachment_unavailable" })).toMatchObject({
    code: "attachment_unavailable",
    message: "Attachment bytes are no longer available.",
    retryable: false,
    fatal: false,
  });
});

type ForkSDKRetryStatus =
  | { readonly type: "retrying" }
  | { readonly type: "exhausted" }
  | { readonly type: "terminal" };

type ForkSDKTextBlock = {
  readonly type: "text";
  readonly text: string;
};

type ForkSDKAgentMCPToolUseEvent = {
  readonly id: string;
  readonly processed_at: string;
  readonly type: "agent.mcp_tool_use";
  readonly name: string;
  readonly input: { readonly [key: string]: unknown };
  readonly mcp_server_name: string;
  readonly evaluated_permission?: "allow" | "ask" | "deny" | undefined;
};

type ForkSDKAgentMCPToolResultEvent = {
  readonly id: string;
  readonly processed_at: string;
  readonly type: "agent.mcp_tool_result";
  readonly mcp_tool_use_id: string;
  readonly content?: readonly ForkSDKTextBlock[] | undefined;
  readonly is_error?: boolean | null | undefined;
};

type ForkSDKMCPAuthenticationFailedError = {
  readonly type: "mcp_authentication_failed_error";
  readonly mcp_server_name: string;
  readonly message: string;
  readonly retry_status: ForkSDKRetryStatus;
};

type ForkSDKMCPConnectionFailedError = {
  readonly type: "mcp_connection_failed_error";
  readonly mcp_server_name: string;
  readonly message: string;
  readonly retry_status: ForkSDKRetryStatus;
};

type ForkSDKSessionMCPErrorEvent = {
  readonly id: string;
  readonly processed_at: string;
  readonly type: "session.error";
  readonly error: ForkSDKMCPAuthenticationFailedError | ForkSDKMCPConnectionFailedError;
};

type AssertType<T extends true> = T;
type RuntimeMCPToolUseSDKCompatibility = AssertType<
  (Extract<SessionEvent, { readonly type: "agent.mcp_tool_use" }> & { readonly id: string; readonly processed_at: string }) extends ForkSDKAgentMCPToolUseEvent ? true : false
>;
type RuntimeMCPToolResultSDKCompatibility = AssertType<
  (Extract<SessionEvent, { readonly type: "agent.mcp_tool_result" }> & { readonly id: string; readonly processed_at: string }) extends ForkSDKAgentMCPToolResultEvent ? true : false
>;
type RuntimeSessionErrorEvent = Extract<SessionEvent, { readonly type: "session.error" }>;
type RuntimeMCPAuthErrorSDKCompatibility = AssertType<
  ({ readonly type: "session.error"; readonly error: Extract<RuntimeSessionErrorEvent["error"], { readonly type: "mcp_authentication_failed_error" }> } & {
    readonly id: string;
    readonly processed_at: string;
  }) extends ForkSDKSessionMCPErrorEvent ? true : false
>;
type RuntimeMCPConnectionErrorSDKCompatibility = AssertType<
  ({ readonly type: "session.error"; readonly error: Extract<RuntimeSessionErrorEvent["error"], { readonly type: "mcp_connection_failed_error" }> } & {
    readonly id: string;
    readonly processed_at: string;
  }) extends ForkSDKSessionMCPErrorEvent ? true : false
>;

void (undefined as unknown as RuntimeMCPToolUseSDKCompatibility);
void (undefined as unknown as RuntimeMCPToolResultSDKCompatibility);
void (undefined as unknown as RuntimeMCPAuthErrorSDKCompatibility);
void (undefined as unknown as RuntimeMCPConnectionErrorSDKCompatibility);

async function sleepUntilAborted(_durationMs: number, sleepSignal: AbortSignal): Promise<boolean> {
  if (sleepSignal.aborted) {
    return false;
  }
  await new Promise<void>((resolve) => sleepSignal.addEventListener("abort", () => resolve(), { once: true }));
  return false;
}

function operationControls(overrides: Partial<RuntimeMessageStoreOperationControls> = {}): RuntimeMessageStoreOperationControls {
  return {
    signal,
    timeoutMs: 100,
    sleep: sleepUntilAborted,
    ...overrides,
  };
}

function messageInfo(overrides: Partial<RuntimeMessageInfo> = {}): RuntimeMessageInfo {
  return RuntimeMessageInfoSchema.parse({
    id: "message-1",
    sessionId: "session-1",
    role: "assistant",
    origin: "agent",
    sequence: 7,
    status: "streaming",
    createdAt,
    providerId: "openai",
    modelId: "gpt-5.5",
    ...overrides,
  });
}

function parseExactSessionEvent<T extends SessionEvent>(event: T): T {
  return SessionEventSchema.parse(event) as T;
}

function withSDKEventEnvelope<T extends SessionEvent>(event: T): T & { readonly id: string; readonly processed_at: string } {
  return {
    id: "sevt_sdk_shape_1",
    processed_at: createdAt,
    ...event,
  };
}

function part(overrides: Partial<RuntimePart> = {}): RuntimePart {
  const base = {
    id: "part-1",
    sessionId: "session-1",
    messageId: "message-1",
    sequence: 1,
    createdAt,
  };
  switch (overrides.type) {
    case "reasoning":
      return RuntimePartSchema.parse({
        ...base,
        type: "reasoning",
        text: "reasoning",
        truncated: false,
        status: "completed",
        ...overrides,
      });
    case "tool":
      return RuntimePartSchema.parse({
        ...base,
        type: "tool",
        toolCallId: "tool-call-1",
        toolName: "tool",
        state: { status: "pending" },
        ...overrides,
      });
    case "step-start":
      return RuntimePartSchema.parse({ ...base, type: "step-start", ...overrides });
    case "step-finish":
      return RuntimePartSchema.parse({ ...base, type: "step-finish", finishReason: "stop", ...overrides });
    case "text":
    default:
      return RuntimePartSchema.parse({
        ...base,
        type: "text",
        text: "hello",
        truncated: false,
        status: "completed",
        ...overrides,
      });
  }
}

function internalToolRepairCommit(): RuntimeInternalToolRepairCommit {
  const repairMessageId = "message-repair-1";
  const modelToolCallId = "tool-call-1";
  const toolName = "tool";
  const repairPart = part({
    id: "part-repair-1",
    messageId: repairMessageId,
    type: "tool",
    toolCallId: modelToolCallId,
    toolName,
    state: {
      status: "error",
      error: {
        type: "provider_tool_protocol_error",
        message: "model emitted an unavailable tool",
        retryable: false,
      },
    },
  });
  return RuntimeInternalToolRepairCommitSchema.parse({
    sessionId: "session-1",
    sessionThreadId: "thread-1",
    modelRequestId: "model-request-1",
    modelToolCallId,
    toolName,
    repairKey: "repair-key-1",
    message: {
      ...messageInfo({
        id: repairMessageId,
        status: "completed",
      }),
      parts: [repairPart],
    },
  });
}

function redactionNeedles(): readonly string[] {
  return [canary, rawSql, connectionString, rawHeaders, rawPrompt, rawProviderPayload];
}

function expectSanitized(value: unknown): void {
  const serialized = JSON.stringify(value);
  for (const needle of redactionNeedles()) {
    expect(serialized).not.toContain(needle);
  }
}

class UnitRuntimeMessageStore extends RuntimeMessageStore {
  readonly messages: RuntimeMessageInfo[] = [];
  readonly parts: RuntimePart[] = [];

  protected async writeMessageRecord(message: RuntimeMessageInfo): Promise<RuntimeMessageStoreWriteMessageResult> {
    this.messages.push(message);
    return { ok: true, messageId: message.id, operation: "writeMessage" };
  }

  protected async writePartRecord(runtimePart: RuntimePart): Promise<RuntimeMessageStoreWritePartResult> {
    this.parts.push(runtimePart);
    return { ok: true, messageId: runtimePart.messageId, partId: runtimePart.id, operation: "writePart" };
  }

  protected override async commitInternalToolRepairRecord(repair: RuntimeInternalToolRepairCommit): Promise<RuntimeMessageStoreWritePartResult> {
    const [runtimePart] = repair.message.parts;
    if (runtimePart === undefined) {
      throw new Error("repair message missing part");
    }
    this.messages.push(repair.message);
    this.parts.push(runtimePart);
    return { ok: true, messageId: repair.message.id, partId: runtimePart.id, operation: "writePart" };
  }
}

describe("runtime boundary contracts", () => {
  test("assistant projection events carry model request identity under the closed association law", () => {
    const part = {
      reasoningPartId: "part_anchor_1",
      providerPartId: "provider_anchor_1",
      partSequence: 0,
      text: "thinking",
      providerMetadata: {},
      truncated: false,
    };
    const base = {
      sessionId: "sesn_1",
      sessionThreadId: "thr_1",
      writeId: "rwrite_1",
      stableReasoningParts: [part],
      modelRequestId: "mreq_anchor_1",
    };
    for (const event of [
      { type: "agent.tool_use" as const, name: "bash", input: {}, evaluated_permission: "allow" as const },
      { type: "agent.mcp_tool_use" as const, name: "search", input: {}, evaluated_permission: "allow" as const, mcp_server_name: "github" },
    ]) {
      expect(SessionEventEnvelopeSchema.safeParse({ ...base, event }).success).toBe(true);
    }
    expect(SessionEventEnvelopeSchema.safeParse({ ...base, event: { type: "session.status_running" } }).success).toBe(false);
    expect(SessionEventEnvelopeSchema.safeParse({ ...base, modelRequestId: undefined, event: {
      type: "agent.tool_use",
      name: "bash",
      input: {},
      evaluated_permission: "allow",
    } }).success).toBe(false);
    expect(SessionEventEnvelopeSchema.safeParse({
      sessionId: "sesn_1",
      sessionThreadId: "thr_1",
      writeId: "rwrite_2",
      event: { type: "agent.tool_use", name: "bash", input: {}, evaluated_permission: "allow" },
      modelRequestId: "mreq_without_anchor",
    }).success).toBe(false);

    const projectionBase = {
      sessionId: "sesn_1",
      sessionThreadId: "thr_1",
      writeId: "rwrite_projection",
      modelRequestId: "mreq_projection",
      projectionJson: "{\"type\":\"runtime_projection\"}",
    };
    for (const event of [
      { type: "agent.tool_result" as const, tool_use_id: "sevt_tool", content: [{ type: "text" as const, text: "done" }] },
      { type: "agent.mcp_tool_result" as const, mcp_tool_use_id: "sevt_mcp", content: [{ type: "text" as const, text: "done" }] },
      { type: "agent.message" as const, content: [{ type: "text" as const, text: "answer" }] },
    ]) {
      expect(SessionEventEnvelopeSchema.safeParse({ ...projectionBase, event }).success).toBe(true);
      expect(SessionEventEnvelopeSchema.safeParse({ ...projectionBase, modelRequestId: undefined, event }).success).toBe(false);
    }
    expect(SessionEventEnvelopeSchema.safeParse({
      ...projectionBase,
      event: { type: "agent.message", content: [{ type: "text", text: "answer" }] },
      projectionJson: "{}",
    }).success).toBe(true);
    expect(SessionEventEnvelopeSchema.safeParse({
      ...projectionBase,
      event: { type: "span.model_request_start", model_request_id: "mreq_projection" },
    }).success).toBe(false);
  });

  test("request-end stable reasoning contract enforces count aggregate identity order and success-only bounds", () => {
    const base = {
      sessionId: "sesn_1",
      sessionThreadId: "thr_1",
      writeId: "rwrite_1",
      modelRequestId: "mreq_1",
      modelRequestStartEventId: "sevt_start_1",
      isError: false,
      finishReason: "stop" as const,
    };
    const part = (id: string, partSequence: number, text = "x") => ({
      reasoningPartId: id,
      providerPartId: `provider_${id}`,
      partSequence,
      text,
      providerMetadata: {},
      truncated: false,
    });
    const exactCount = Array.from({ length: MaxStableReasoningPartsPerRequest }, (_, sequence) => part(`part_${sequence}`, sequence));
    expect(SessionEventWriterRequestEndEnvelopeSchema.safeParse({ ...base, stableReasoningParts: exactCount }).success).toBe(true);
    expect(SessionEventWriterRequestEndEnvelopeSchema.safeParse({ ...base, stableReasoningParts: [...exactCount, part("part_16", 16)] }).success).toBe(false);

    const exactAggregate = [
      part("aggregate_1", 0, "a".repeat(1024 * 1024 - 2)),
      part("aggregate_2", 1, "b".repeat(1024 * 1024 - 2)),
    ];
    expect(SessionEventWriterRequestEndEnvelopeSchema.safeParse({ ...base, stableReasoningParts: exactAggregate }).success).toBe(true);
    exactAggregate[1] = part("aggregate_2", 1, "b".repeat(1024 * 1024 - 1));
    expect(SessionEventWriterRequestEndEnvelopeSchema.safeParse({ ...base, stableReasoningParts: exactAggregate }).success).toBe(false);

    expect(SessionEventWriterRequestEndEnvelopeSchema.safeParse({ ...base, stableReasoningParts: [part("same", 0), part("same", 1)] }).success).toBe(false);
    expect(SessionEventWriterRequestEndEnvelopeSchema.safeParse({ ...base, stableReasoningParts: [part("one", 0), part("two", 0)] }).success).toBe(false);
    expect(SessionEventWriterRequestEndEnvelopeSchema.safeParse({ ...base, stableReasoningParts: [part("two", 2), part("one", 1)] }).success).toBe(false);
    expect(SessionEventWriterRequestEndEnvelopeSchema.safeParse({ ...base, isError: true, errorKind: "provider_error", stableReasoningParts: [part("error", 0)] }).success).toBe(false);
    expect(SessionEventWriterRequestEndEnvelopeSchema.safeParse({ ...base, isError: true, errorKind: "provider_error", reschedule: { attempt: 1, deadline: createdAt, backoffMs: 1 }, stableReasoningParts: [part("retry", 0)] }).success).toBe(false);
  });

  test("request-end attachment settlement is combined-bounded and absent on reschedule", () => {
    const base = {
      sessionId: "sesn_1",
      sessionThreadId: "thr_1",
      writeId: "rwrite_attachments",
      modelRequestId: "mreq_attachments",
      modelRequestStartEventId: "sevt_start_attachments",
      isError: false,
      finishReason: "stop" as const,
    };
    const fileAttachments = Array.from({ length: 16 }, (_, index) => ({
      sourceEventId: `sevt_file_${index}`,
      fileId: `file_${index}`,
    }));
    expect(SessionEventWriterRequestEndEnvelopeSchema.safeParse({
      ...base,
      consumedAttachmentRefs: Array.from({ length: 16 }, (_, index) => `att_${index}`),
      consumedFileAttachments: fileAttachments,
    }).success).toBe(true);
    expect(SessionEventWriterRequestEndEnvelopeSchema.safeParse({
      ...base,
      consumedAttachmentRefs: Array.from({ length: 17 }, (_, index) => `att_${index}`),
      consumedFileAttachments: fileAttachments,
    }).success).toBe(false);
    expect(SessionEventWriterRequestEndEnvelopeSchema.safeParse({
      ...base,
      isError: true,
      errorKind: "provider_error",
      reschedule: { attempt: 1, deadline: createdAt, backoffMs: 1 },
      consumedFileAttachments: [fileAttachments[0]!],
    }).success).toBe(false);
  });

  test("keeps reasoning stream admission at the narrower 8 KiB text and 4 KiB metadata bounds", () => {
    const metadataAtLimit = { x: "m".repeat(4_096 - 8) };
    expect(LLMEventSchema.safeParse({
      type: "reasoning-delta",
      id: "reasoning_1",
      text_delta: "x".repeat(8_192),
      providerMetadata: metadataAtLimit,
    }).success).toBe(true);
    expect(LLMEventSchema.safeParse({
      type: "reasoning-delta",
      id: "reasoning_1",
      text_delta: "x".repeat(8_193),
    }).success).toBe(false);
    expect(LLMEventSchema.safeParse({
      type: "reasoning-delta",
      id: "reasoning_1",
      text_delta: "x",
      providerMetadata: { x: "m".repeat(4_096 - 7) },
    }).success).toBe(false);
  });
  test("maps internal RuntimeFailure session errors to fork-SDK durable payloads", () => {
    const failures = [
      normalizeRuntimeFailure({ type: "provider", code: "provider_rate_limited", retryable: true, fatal: false, retryStatus: { type: "retrying", attempt: 1 } }),
      normalizeRuntimeFailure({ type: "provider", code: "provider_unavailable", retryable: true, fatal: false, retryStatus: { type: "exhausted" } }),
      normalizeRuntimeFailure({ type: "provider", code: "provider_invalid_request", retryable: false, fatal: true, retryStatus: { type: "terminal" } }),
    ];

    expect(failures.map((error) => sessionEventForDurableWrite(SessionEventSchema.parse({ type: "session.error", error })))).toEqual([
      { type: "session.error", error: { type: "model_rate_limited_error", message: "Runtime operation failed.", retry_status: { type: "retrying" } } },
      { type: "session.error", error: { type: "model_overloaded_error", message: "Runtime operation failed.", retry_status: { type: "exhausted" } } },
      { type: "session.error", error: { type: "model_request_failed_error", message: "Runtime operation failed.", retry_status: { type: "terminal" } } },
    ]);
  });

  test("emits MCP runtime events compatible with fork SDK event types", () => {
    const mcpToolUse = withSDKEventEnvelope(parseExactSessionEvent({
      type: "agent.mcp_tool_use",
      name: "create_issue",
      input: { title: "Bug" },
      mcp_server_name: "github",
      evaluated_permission: "ask",
    })) satisfies ForkSDKAgentMCPToolUseEvent;
    const mcpToolResult = withSDKEventEnvelope(parseExactSessionEvent({
      type: "agent.mcp_tool_result",
      mcp_tool_use_id: "sevt_mcp_tool_use_1",
      content: [{ type: "text", text: "created" }],
      is_error: false,
    })) satisfies ForkSDKAgentMCPToolResultEvent;
    const authError = withSDKEventEnvelope(parseExactSessionEvent({
      type: "session.error",
      error: {
        type: "mcp_authentication_failed_error",
        mcp_server_name: "github",
        message: "MCP authentication failed after refresh.",
        retry_status: { type: "terminal" },
      },
    })) satisfies ForkSDKSessionMCPErrorEvent;
    const connectionError = withSDKEventEnvelope(parseExactSessionEvent({
      type: "session.error",
      error: {
        type: "mcp_connection_failed_error",
        mcp_server_name: "github",
        message: "MCP connection failed.",
        retry_status: { type: "exhausted" },
      },
    })) satisfies ForkSDKSessionMCPErrorEvent;

    expect([mcpToolUse.type, mcpToolResult.type, authError.error.type, connectionError.error.type]).toEqual([
      "agent.mcp_tool_use",
      "agent.mcp_tool_result",
      "mcp_authentication_failed_error",
      "mcp_connection_failed_error",
    ]);
    expect(SessionEventSchema.safeParse({
      type: "agent.mcp_tool_use",
      name: "create_issue",
      input: "not-an-sdk-tool-input-object",
      mcp_server_name: "github",
      evaluated_permission: "ask",
    }).success).toBe(false);
  });

  test("accepts the required Anthropic-shaped outbound event subset and rejects legacy events", () => {
    const runtimeFailure = RuntimeFailureSchema.parse({
      type: "provider",
      code: "provider_timeout",
      message: "Provider timed out.",
      retryable: true,
      fatal: false,
      providerId: "openai",
      modelId: "gpt-5.5",
      retryStatus: { type: "exhausted" },
    });
    const events = [
      { type: "agent.message", content: [{ type: "text", text: "hello" }] },
      { type: "agent.tool_use", name: "search", input: { q: "runtime" }, evaluated_permission: "allow" },
      { type: "agent.tool_result", tool_use_id: "sevt_tool_use_1", content: [{ type: "text", text: "done" }] },
      { type: "agent.tool_result", tool_use_id: "sevt_tool_use_1", is_error: true },
      { type: "agent.mcp_tool_use", name: "create_issue", input: { title: "Bug" }, mcp_server_name: "github", evaluated_permission: "allow" },
      { type: "agent.mcp_tool_result", mcp_tool_use_id: "sevt_mcp_tool_use_1", content: [{ type: "text", text: "created" }] },
      { type: "agent.mcp_tool_result", mcp_tool_use_id: "sevt_mcp_tool_use_1", is_error: true },
      {
        type: "session.error",
        error: {
          type: "mcp_authentication_failed_error",
          mcp_server_name: "github",
          message: "MCP authentication failed after refresh.",
          retry_status: { type: "terminal" },
        },
      },
      {
        type: "session.error",
        error: {
          type: "mcp_connection_failed_error",
          mcp_server_name: "github",
          message: "MCP connection failed.",
          retry_status: { type: "exhausted" },
        },
      },
      { type: "session.status_running" },
      { type: "session.status_idle", stop_reason: { type: "end_turn" } },
      { type: "session.status_idle", stop_reason: { type: "requires_action", event_ids: ["sevt_blocking_1"] } },
      { type: "session.status_idle", stop_reason: { type: "retries_exhausted" } },
      { type: "session.status_terminated" },
      { type: "session.thread_status_terminated", session_thread_id: "thr_child" },
      { type: "session.error", error: runtimeFailure },
      { type: "span.model_request_start", model_request_id: "mreq_1" },
      {
        type: "span.model_request_end",
        model_request_start_id: "sevt_model_start_1",
        is_error: false,
        model_usage: {
          input_tokens: 1,
          output_tokens: 2,
          cache_creation_input_tokens: 0,
          cache_read_input_tokens: 0,
          speed: null,
        },
      },
    ];

    expect(events.map((event) => SessionEventSchema.parse(event).type)).toEqual([
      "agent.message",
      "agent.tool_use",
      "agent.tool_result",
      "agent.tool_result",
      "agent.mcp_tool_use",
      "agent.mcp_tool_result",
      "agent.mcp_tool_result",
      "session.error",
      "session.error",
      "session.status_running",
      "session.status_idle",
      "session.status_idle",
      "session.status_idle",
      "session.status_terminated",
      "session.thread_status_terminated",
      "session.error",
      "span.model_request_start",
      "span.model_request_end",
    ]);

    for (const legacyEvent of [
      { type: "session.status", status: { type: "idle" } },
      { type: "session.status_idle", stop_reason: { type: "requires_action", blocking_event_ids: ["sevt_blocking_1"] } },
      { type: "agent.message", info: messageInfo() },
      { type: "agent.message", part: part(), time: 0 },
      { type: "agent.message", messageId: "message-1", partId: "part-1", field: "text", delta: "hello" },
    ]) {
      expect(SessionEventSchema.safeParse(legacyEvent).success).toBe(false);
    }
  });

  test("defines strict pending input, loader, LLM event, event writer, and event-writer boundary schemas", () => {
    const userMessage = RuntimeMessageSchema.parse({
      ...messageInfo({ role: "user", origin: "user", status: "completed" }),
      parts: [part({ type: "text", text: "hello", status: "completed" })],
    });

    expect(PendingInputResultSchema.parse({ type: "messages", messages: [userMessage] }).type).toBe("messages");
    expect(PendingInputResultSchema.parse({ type: "empty" }).type).toBe("empty");
    expect(PendingInputResultSchema.safeParse({ type: "interrupt" }).success).toBe(false);
    expect(PendingInputResultSchema.safeParse({ type: "messages", messages: [messageInfo({ role: "assistant" })] }).success).toBe(false);
    expect(ContextLoaderErrorSchema.parse(normalizeContextLoaderError({ code: "unavailable", rawError: new Error(canary) })).message).toBe(
      "Context loader operation failed.",
    );
    expect(LLMEventSchema.parse({ type: "text-delta", id: "text-1", text_delta: "hello" }).type).toBe("text-delta");
    expect(LLMEventSchema.safeParse({ type: "raw-provider-event", raw: canary }).success).toBe(false);
    expect(LLMEventSchema.safeParse({ type: "tool-result", id: "tool-1", output: { text: "done", truncated: false } }).success).toBe(false);
    expect(LLMEventSchema.safeParse({
      type: "tool-error",
      id: "tool-1",
      error: normalizeRuntimeFailure({ type: "runtime", code: "runtime_invalid_sequence", rawError: "tool failed" }),
    }).success).toBe(false);
    expect(SessionEventEnvelopeSchema.parse({
      sessionId: "session-1",
      sessionThreadId: "thread-1",
      writeId: "write-1",
      event: { type: "session.status_running" },
    }).writeId).toBe("write-1");
    const appendResult = SessionEventWriterAppendResultSchema.parse({
      ok: true,
      writeId: "write-1",
      eventId: "sevt_1",
      processedAt: createdAt,
    });
    expect(appendResult.ok).toBe(true);
    if (!appendResult.ok) {
      throw new Error("expected session event append success");
    }
    expect(appendResult.eventId).toBe("sevt_1");
    expect(SessionEventWriterErrorSchema.parse(normalizeSessionEventWriterError({ code: "ack_mismatch", rawError: canary })).message).toBe(
      "Session event writer operation failed.",
    );
    expect(normalizeSessionEventWriterError({ code: "superseded", rawError: canary })).toMatchObject({
      code: "superseded",
      retryable: false,
      fatal: false,
    });
    expect(normalizeSessionEventWriterError({ code: "unrepairable", rawError: canary })).toMatchObject({
      code: "unrepairable",
      retryable: false,
      fatal: true,
    });
  });

  test("RuntimeMessageStore exposes only durable writes, validates payloads, and rejects acknowledgement mismatch", async () => {
    class MismatchStore extends UnitRuntimeMessageStore {
      protected override async writeMessageRecord(): Promise<RuntimeMessageStoreWriteMessageResult> {
        return { ok: true, messageId: `${canary}-${rawSql}`, operation: "writeMessage" };
      }
      protected override async writePartRecord(): Promise<RuntimeMessageStoreWritePartResult> {
        return { ok: true, messageId: `${canary}-${rawSql}`, partId: `${connectionString}-${rawHeaders}`, operation: "writePart" };
      }
    }
    class HostileFailureStore extends UnitRuntimeMessageStore {
      protected override async writePartRecord(): Promise<RuntimeMessageStoreWritePartResult> {
        const hostileText = `${canary} ${rawSql} ${connectionString} ${rawHeaders} ${rawPrompt} ${rawProviderPayload}`;
        return {
          ok: false,
          error: {
            type: "message-store",
            code: "schema_mismatch",
            operation: "writePart",
            message: hostileText,
            retryable: false,
            fatal: true,
            constraint: rawSql,
            messageId: canary,
            partId: connectionString,
            sessionId: rawHeaders,
          },
        };
      }
    }
    class RawAcknowledgementStore extends UnitRuntimeMessageStore {
      protected override async writeMessageRecord(): Promise<RuntimeMessageStoreWriteMessageResult> {
        return { ok: true, messageId: canary, operation: "writeMessage" };
      }
      protected override async writePartRecord(): Promise<RuntimeMessageStoreWritePartResult> {
        return { ok: true, messageId: canary, partId: rawSql, operation: "writePart" };
      }
    }

    const store = new UnitRuntimeMessageStore();
    const runtimeMessage = messageInfo();
    const runtimePart = part();
    const repair = internalToolRepairCommit();
    const writeMessageResult = await store.writeMessage(runtimeMessage, operationControls());
    const writePartResult = await store.writePart(runtimePart, operationControls());
    const repairResult = await store.commitInternalToolRepair(repair, operationControls());
    const mismatchStore = new MismatchStore();
    const mismatchMessage = await mismatchStore.writeMessage(runtimeMessage, operationControls());
    const mismatchPart = await mismatchStore.writePart(runtimePart, operationControls());
    const hostileFailure = await new HostileFailureStore().writePart(runtimePart, operationControls());
    const rawAcknowledgementStore = new RawAcknowledgementStore();
    const rawAcknowledgementMessage = await rawAcknowledgementStore.writeMessage(messageInfo({ id: canary }), operationControls());
    const rawAcknowledgementPart = await rawAcknowledgementStore.writePart(part({ messageId: canary, id: rawSql }), operationControls());
    const repairPart = repair.message.parts[0];
    if (repairPart === undefined) {
      throw new Error("internal repair fixture must include one part");
    }

    expect(Object.getOwnPropertyNames(RuntimeMessageStore.prototype).sort()).toEqual([
      "commitInternalToolRepair",
      "commitInternalToolRepairRecord",
      "constructor",
      "writeMessage",
      "writePart",
    ]);
    expect("listMessages" in store).toBe(false);
    expect("readMessages" in store).toBe(false);
    expect(writeMessageResult).toEqual({ ok: true, messageId: runtimeMessage.id, operation: "writeMessage" });
    expect(writePartResult).toEqual({ ok: true, messageId: runtimePart.messageId, partId: runtimePart.id, operation: "writePart" });
    expect(repairResult).toEqual({ ok: true, messageId: repair.message.id, partId: repairPart.id, operation: "writePart" });
    expect(await store.writeMessage({ ...runtimeMessage, rawDriver: "pg" }, operationControls())).toMatchObject({
      ok: false,
      error: { code: "schema_mismatch", operation: "writeMessage" },
    });
    expect(await store.writePart({ ...runtimePart, rawDriver: "pg" }, operationControls())).toMatchObject({
      ok: false,
      error: { code: "schema_mismatch", operation: "writePart" },
    });
    expect(await store.commitInternalToolRepair({ ...repair, rawDriver: "pg" }, operationControls())).toMatchObject({
      ok: false,
      error: { code: "schema_mismatch", operation: "writePart" },
    });
    expect(RuntimeMessageSchema.safeParse({
      ...messageInfo({ id: canary, sessionId: rawHeaders }),
      parts: [part({ id: "part-mismatch", messageId: rawSql, sessionId: connectionString })],
    }).success).toBe(false);
    expect(mismatchMessage).toMatchObject({
      ok: false,
      error: { code: "schema_mismatch", operation: "writeMessage", reason: "write_acknowledgement_mismatch" },
    });
    expect(mismatchPart).toMatchObject({
      ok: false,
      error: { code: "schema_mismatch", operation: "writePart", reason: "write_acknowledgement_mismatch" },
    });
    expectSanitized(mismatchMessage);
    expectSanitized(mismatchPart);
    expect(rawAcknowledgementMessage).toEqual({ ok: true, messageId: canary, operation: "writeMessage" });
    expect(rawAcknowledgementPart).toEqual({ ok: true, messageId: canary, partId: rawSql, operation: "writePart" });
    expectSanitized(hostileFailure);
    expect(RuntimeMessageStoreOperationControlsSchema.safeParse({ ...operationControls(), sessionId: "session-1" }).success).toBe(false);
    expect(RuntimeMessageStoreWriteMessageResultSchema.safeParse({ ok: true, messageId: "message-1", operation: "listMessages" }).success).toBe(false);
    expect(RuntimeMessageStoreWritePartResultSchema.safeParse({ ok: true, messageId: "message-1", partId: "part-1", operation: "listMessages" }).success).toBe(false);
  });

  test("preserves executable runtime payloads while sanitizing failure channels", () => {
    const executableText = `${canary} ${rawSql} ${connectionString} ${rawHeaders} ${rawPrompt} ${rawProviderPayload}`;
    const boundedJson = RuntimeBoundedJsonSchema.parse({
      value: { [rawHeaders]: executableText },
      preview: executableText,
      truncated: false,
    });
    const runtimeMessage = RuntimeMessageSchema.parse({
      ...messageInfo({ id: canary, sessionId: rawHeaders, modelId: rawProviderPayload }),
      parts: [part({ type: "text", id: "part-raw", messageId: canary, sessionId: rawHeaders, text: executableText })],
    });
    const toolCall = LLMEventSchema.parse({
      type: "tool-call",
      id: canary,
      toolName: "search",
      input: boundedJson,
    });
    const failure = RuntimeFailureSchema.parse({
      type: "provider",
      code: "provider_stream_error",
      message: executableText,
      retryable: false,
      fatal: true,
    });

    expect(boundedJson).toEqual({ value: { [rawHeaders]: executableText }, preview: executableText, truncated: false });
    expect(runtimeMessage.parts[0]).toMatchObject({ text: executableText });
    expect(toolCall).toMatchObject({ input: { value: { [rawHeaders]: executableText }, preview: executableText } });
    expectSanitized(failure);
  });

  test("sanitizes failure and log-safe boundary outputs", async () => {
    const hostileText = `${canary} ${rawSql} ${connectionString} ${rawHeaders} ${rawPrompt} ${rawProviderPayload}`;
    const outputs = [
      normalizeContextLoaderError({ code: "schema_mismatch", rawError: hostileText }),
      ContextLoaderErrorSchema.parse({
        type: "context-loader",
        code: "schema_mismatch",
        message: hostileText,
        retryable: false,
        fatal: true,
        sessionId: rawHeaders,
        reason: hostileText,
      }),
      normalizeRuntimeMessageStoreError({ code: "schema_mismatch", operation: "writePart", rawError: hostileText }),
      normalizeSessionEventWriterError({ code: "unavailable", rawError: hostileText }),
      normalizeRuntimeFailure({ type: "provider", code: "provider_stream_error", rawError: hostileText }),
      RuntimeFailureSchema.parse({
        type: "provider",
        code: "provider_stream_error",
        message: hostileText,
        retryable: false,
        fatal: true,
        messageId: canary,
        partId: connectionString,
        sessionId: rawHeaders,
        modelId: rawProviderPayload,
      }),
      normalizeRuntimeMessageStoreError({
        code: "schema_mismatch",
        operation: "writePart",
        constraint: rawSql,
        messageId: canary,
        partId: connectionString,
        sessionId: rawHeaders,
      }),
      RuntimeMessageStoreErrorSchema.parse({
        type: "message-store",
        code: "schema_mismatch",
        operation: "writePart",
        message: hostileText,
        retryable: false,
        fatal: true,
        constraint: rawSql,
        messageId: canary,
        partId: connectionString,
        sessionId: rawHeaders,
      }),
      SessionEventSchema.parse({ type: "session.error", error: normalizeRuntimeFailure({ type: "runtime", code: "runtime_invalid_sequence", rawError: hostileText }) }),
      SessionEventSchema.parse({
        type: "session.error",
        error: RuntimeFailureSchema.parse({
          type: "message-store",
          code: "schema_mismatch",
          message: hostileText,
          retryable: false,
          fatal: true,
          operation: "writePart",
          constraint: rawSql,
        }),
      }),
      RuntimeToolErrorSchema.parse({ type: hostileText, message: hostileText, retryable: false }),
      boundRuntimeToolError({ type: "tool_failed", message: hostileText, retryable: true }, 1_000),
    ];

    for (const output of outputs) {
      expectSanitized(output);
    }
  });

  test("runtime dependencies control ids, timestamps, and sleeps", async () => {
    const deps = {
      createId: (prefix: string) => `${prefix}-1`,
      now: () => createdAt,
      monotonicMs: () => 12,
      sleep: async (_durationMs: number, sleepSignal: AbortSignal) => !sleepSignal.aborted,
    };
    const id = createRuntimeId(deps, "message");
    expect(id).toBe("message-1");
    expect(runtimeNow(deps)).toBe(createdAt);
    expect(RuntimeMessageInfoSchema.shape.createdAt.safeParse(runtimeNow(deps)).success).toBe(true);

    const controller = new AbortController();
    controller.abort();
    expect(await runtimeSleep(deps, 10_000, controller.signal)).toBe(false);
    expect(RuntimeMessageStoreErrorSchema.parse(normalizeRuntimeMessageStoreError({ code: "timeout", operation: "writePart" })).operation).toBe(
      "writePart",
    );
  });
});
