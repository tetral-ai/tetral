import { describe, expect, test } from "bun:test";
import {
  RuntimeMessageRole,
  RuntimeToolPartState,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { RuntimeMessage, RuntimePart } from "../../src/contracts/runtime.js";
import { RuntimeMessageSchema } from "../../src/contracts/runtime.js";
import { toGatewayRuntimeMessages } from "../../src/runtime/message-projection.js";
import { assembleProviderCallRequest } from "../../src/thread-loop/provider-request.js";
import { lowerProviderRequest } from "../../../../../gateway/packages/lowering/src/request.js";
import { OpenAIGPT55Rules } from "../../../../../gateway/packages/lowering/src/rules/openai.js";
import { validateProviderRequest } from "../../../../../gateway/packages/protocol/src/bounds.js";
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

  test("rejects pending public tool state and completed tools without a durable tool-use identity", () => {
    expect(toGatewayRuntimeMessages([
      message("assistant-1", "assistant", [
        toolPart({
          status: "running",
          input: { value: { q: "hi" }, preview: "{\"q\":\"hi\"}", truncated: false },
        }),
      ]),
    ])).toMatchObject({ ok: false, error: { code: "provider_invalid_request" } });

    expect(toGatewayRuntimeMessages([
      message("assistant-1", "assistant", [{
        ...toolPart({
          status: "completed",
          input: { value: { q: "hi" }, preview: "{\"q\":\"hi\"}", truncated: false },
          output: { text: "result", truncated: false },
        }, ""),
      }]),
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
        state: RuntimeToolPartState.RUNTIME_TOOL_PART_STATE_ERROR,
      });
    }
  });

  test("preserves ordinary completed, errored, and cancelled Tool lowering after carrier cleanup", () => {
    const input = { value: { q: "hi" }, preview: "{\"q\":\"hi\"}", truncated: false } as const;
    const cases = [
      {
        status: "completed" as const,
        state: { status: "completed" as const, input, output: { text: "result", truncated: false } },
        output: { text: "result" },
        isError: undefined,
      },
      {
        status: "error" as const,
        state: {
          status: "error" as const,
          input,
          error: { type: "provider_tool_protocol_error" as const, message: "failed", retryable: false },
        },
        output: { error: { type: "provider_tool_protocol_error", message: "failed", retryable: false } },
        isError: true as const,
      },
      {
        status: "cancelled" as const,
        state: { status: "cancelled" as const, input },
        output: { type: "text", text: "[tool execution cancelled]" },
        isError: true as const,
      },
    ];

    for (const testCase of cases) {
      const projected = toGatewayRuntimeMessages([
        message("assistant-1", "assistant", [toolPart(testCase.state)]),
      ]);
      expect(projected.ok).toBe(true);
      if (!projected.ok) {
        continue;
      }
      const projectedTool = projected.messages[0]?.parts[0]?.tool;
      expect(projectedTool).toBeDefined();
      expect(Object.hasOwn(projectedTool ?? {}, "toolUseEventId")).toBe(false);
      const assembled = assembleProviderCallRequest({
        identity: {
          workspaceId: "default",
          sessionId: "session-a",
          sessionThreadId: "thread-a",
          bindingId: "binding-a",
          bindingGeneration: 1,
          targetPodUid: "pod-a",
          runtimeBindingToken: "binding-token",
        },
        requestId: `request-${testCase.status}`,
        modelRequestId: `model-request-${testCase.status}`,
        currentModel: { providerId: OpenAIGPT55Rules.providerId, modelId: OpenAIGPT55Rules.modelId },
        runtimeMessages: projected.messages,
        runtime: { systemInstructions: "ordinary Tool regression", timeoutMs: 30_000 },
      });
      expect(assembled.ok).toBe(true);
      if (!assembled.ok) {
        continue;
      }
      expect(validateProviderRequest(assembled.request)).toEqual({ ok: true });
      const lowered = lowerProviderRequest(assembled.request, OpenAIGPT55Rules, { modelOutputTokenLimit: 32_000 });
      const toolMessages = lowered.messages.filter((loweredMessage) =>
        loweredMessage.role === "assistant" || loweredMessage.role === "tool"
      );
      expect(toolMessages).toEqual([
        {
          role: "assistant",
          content: [{ type: "tool-call", toolCallId: "call-1", toolName: "lookup", input: { q: "hi" } }],
        },
        {
          role: "tool",
          content: [{
            type: "tool-result",
            toolCallId: "call-1",
            toolName: "lookup",
            output: testCase.output,
            ...(testCase.isError === undefined ? {} : { isError: testCase.isError }),
          }],
        },
      ]);
    }
  });

});
