import { describe, expect, test } from "bun:test";
import {
  RuntimeMessageRole,
  RuntimeToolPartState,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { RuntimeMessage, RuntimeMessageInfo, RuntimeMessageStoreOperationControls, RuntimePart } from "../../src/contracts/runtime.js";
import { RuntimeMessageSchema, RuntimeMessageStore } from "../../src/contracts/runtime.js";
import { RuntimeMessageProjection, toGatewayRuntimeMessages } from "../../src/runtime/message-projection.js";
import {
  buildRuntimeMessageProjectionMessage as message,
} from "./runtime-message-builders.js";

const createdAt = "2026-06-14T00:00:00.000Z";

function textPart(id: string, messageId: string, text: string): RuntimePart {
  return {
    id,
    sessionId: "session-a",
    messageId,
    sequence: 0,
    type: "text",
    text,
    truncated: false,
    status: "completed",
    createdAt,
  };
}

function toolPart(state: Extract<RuntimePart, { readonly type: "tool" }>["state"], toolUseEventId = "event-tool-1"): RuntimePart {
  return {
    id: "tool-part-1",
    sessionId: "session-a",
    messageId: "assistant-1",
    sequence: 2,
    type: "tool",
    toolCallId: "call-1",
    toolName: "lookup",
    ...(toolUseEventId !== "" ? { toolUseEventId } : {}),
    state,
    createdAt,
  };
}

describe("RuntimeMessageProjection", () => {
  test("projects Tetral RuntimeMessage context into Gateway generated RuntimeMessage shape", () => {
    const result = toGatewayRuntimeMessages([
      message("user-1", "user", [textPart("user-text-1", "user-1", "hello")]),
      message("assistant-1", "assistant", [
        textPart("assistant-text-1", "assistant-1", "answer"),
        {
          id: "reasoning-1",
          sessionId: "session-a",
          messageId: "assistant-1",
          sequence: 1,
          type: "reasoning",
          providerPartId: "reasoning-provider-1",
          providerMetadata: { anthropic: { signature: "sig_1" } },
          text: "thinking",
          truncated: false,
          status: "completed",
          createdAt,
        },
        toolPart({
          status: "completed",
          input: { value: { q: "hi" }, preview: "{\"q\":\"hi\"}", truncated: false },
          output: { text: "result", truncated: false },
        }),
      ]),
    ]);

    expect(result).toEqual({
      ok: true,
      messages: [
        {
          id: "user-1",
          role: RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_USER,
          status: "completed",
          origin: "user",
          parts: [{ id: "user-text-1", text: { text: "hello" } }],
        },
        {
          id: "assistant-1",
          role: RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_ASSISTANT,
          status: "completed",
          origin: "agent",
          parts: [
            { id: "assistant-text-1", text: { text: "answer" } },
            {
              id: "reasoning-1",
              reasoning: {
                text: "thinking",
                metadataJson: "{\"anthropic\":{\"signature\":\"sig_1\"}}",
              },
            },
            {
              id: "tool-part-1",
              tool: {
                callId: "call-1",
                name: "lookup",
                toolUseEventId: "event-tool-1",
                state: RuntimeToolPartState.RUNTIME_TOOL_PART_STATE_COMPLETED,
                inputJson: "{\"q\":\"hi\"}",
                outputOrErrorJson: "{\"text\":\"result\"}",
              },
            },
          ],
        },
      ],
    });
  });

  test("omits streaming messages from Gateway provider context", () => {
    const streaming = RuntimeMessageSchema.parse({
      id: "assistant-streaming",
      sessionId: "session-a",
      role: "assistant",
      origin: "agent",
      sequence: 1,
      status: "streaming",
      createdAt,
      parts: [],
    });

    expect(toGatewayRuntimeMessages([
      message("user-1", "user", [textPart("user-text-1", "user-1", "hello")]),
      streaming,
    ])).toEqual({
      ok: true,
      messages: [
        {
          id: "user-1",
          role: RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_USER,
          status: "completed",
          origin: "user",
          parts: [{ id: "user-text-1", text: { text: "hello" } }],
        },
      ],
    });
  });

  test("preserves signed empty reasoning parts for provider replay", () => {
    const result = toGatewayRuntimeMessages([
      message("assistant-1", "assistant", [
        {
          id: "reasoning-1",
          sessionId: "session-a",
          messageId: "assistant-1",
          sequence: 0,
          type: "reasoning",
          providerPartId: "reasoning-provider-1",
          providerMetadata: { anthropic: { signature: "sig_empty" } },
          text: "",
          truncated: false,
          status: "completed",
          createdAt,
        },
      ]),
    ]);

    expect(result).toEqual({
      ok: true,
      messages: [
        {
          id: "assistant-1",
          role: RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_ASSISTANT,
          status: "completed",
          origin: "agent",
          parts: [
            {
              id: "reasoning-1",
              reasoning: {
                text: "",
                metadataJson: "{\"anthropic\":{\"signature\":\"sig_empty\"}}",
              },
            },
          ],
        },
      ],
    });
  });

  test("rejects pending/running tool state and missing tool_use_event_id before Gateway request assembly", () => {
    expect(toGatewayRuntimeMessages([
      message("assistant-1", "assistant", [
        toolPart({
          status: "running",
          input: { value: { q: "hi" }, preview: "{\"q\":\"hi\"}", truncated: false },
        }),
      ]),
    ])).toMatchObject({ ok: false, error: { code: "provider_invalid_request" } });

    expect(toGatewayRuntimeMessages([
      message("assistant-1", "assistant", [
        toolPart({
          status: "error",
          input: { value: { q: "hi" }, preview: "{\"q\":\"hi\"}", truncated: false },
          error: { type: "tool_failed", message: "failed", retryable: false },
        }, ""),
      ]),
    ])).toMatchObject({ ok: false, error: { code: "provider_invalid_request" } });

    expect(toGatewayRuntimeMessages([
      message("assistant-1", "assistant", [
        toolPart({
          status: "completed",
          input: { value: { q: "hi" }, preview: "{\"q\":\"hi\"}", truncated: false },
          output: { text: "result", truncated: false },
        }, ""),
      ]),
    ])).toMatchObject({ ok: false, error: { code: "provider_invalid_request" } });
  });

  test("projects internal invalid-tool repair without persisting a public tool use id", () => {
    const result = toGatewayRuntimeMessages([
      message("assistant-1", "assistant", [
        toolPart({
          status: "running",
          input: { value: { q: "hi" }, preview: "{\"q\":\"hi\"}", truncated: false },
        }, ""),
      ]),
      message("assistant-repair-1", "assistant", [
        {
          id: `part_repair_${"a".repeat(64)}`,
          sessionId: "session-a",
          messageId: "assistant-repair-1",
          sequence: 0,
          type: "tool",
          toolCallId: "call-1",
          toolName: "lookup",
          state: {
            status: "error",
            input: { value: { q: "hi" }, preview: "{\"q\":\"hi\"}", truncated: false },
            error: { type: "provider_tool_protocol_error", message: "invalid tool", retryable: false },
          },
          createdAt,
          completedAt: createdAt,
        },
      ]),
    ]);

    expect(result).toMatchObject({ ok: true });
    if (result.ok) {
      expect(result.messages).toHaveLength(1);
      expect(result.messages[0]?.parts[0]?.tool).toMatchObject({
        callId: "call-1",
        name: "lookup",
        toolUseEventId: undefined,
        state: RuntimeToolPartState.RUNTIME_TOOL_PART_STATE_ERROR,
      });
    }
  });

  test("keeps hot projection updates ACK-gated by successful store writes", async () => {
    const projection = new RuntimeMessageProjection();
    const info: RuntimeMessageInfo = {
      id: "assistant-1",
      sessionId: "session-a",
      role: "assistant",
      origin: "agent",
      sequence: 0,
      status: "streaming",
      createdAt,
    };
    const writes: unknown[] = [];
    class RecordingStore extends RuntimeMessageStore {
      protected async writeMessageRecord(messageInfo: RuntimeMessageInfo, _controls: RuntimeMessageStoreOperationControls): Promise<unknown> {
        writes.push(messageInfo);
        return { ok: true, messageId: messageInfo.id, operation: "writeMessage" as const };
      }

      protected async writePartRecord(part: RuntimePart, _controls: RuntimeMessageStoreOperationControls): Promise<unknown> {
        writes.push(part);
        return { ok: true, messageId: part.messageId, partId: part.id, operation: "writePart" as const };
      }
    }
    const store = new RecordingStore();
    const controls = { signal: new AbortController().signal, timeoutMs: 1000, sleep: async () => true };

    await projection.writeMessageAndUpdate(store, info, controls);
    await projection.writePartAndUpdate(store, textPart("text-1", "assistant-1", "ok"), controls);

    expect(writes).toHaveLength(2);
    expect(projection.messages()).toEqual([
      expect.objectContaining({
        id: "assistant-1",
        parts: [expect.objectContaining({ type: "text", text: "ok" })],
      }),
    ]);
  });
});
