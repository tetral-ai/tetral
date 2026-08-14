import { describe, expect, test } from "bun:test";
import { ProviderContextRole } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { RuntimePart } from "../../src/contracts/runtime.js";
import { RuntimeMessageSchema } from "../../src/contracts/runtime.js";
import { toGatewayProviderContext } from "../../src/runtime/message-projection.js";
import { assembleProviderCallRequest } from "../../src/thread-loop/provider-request.js";
import { lowerProviderRequest } from "../../../../../gateway/packages/lowering/src/request.js";
import { OpenAIGPT55Rules } from "../../../../../gateway/packages/lowering/src/rules/openai.js";
import { validateProviderRequest } from "../../../../../gateway/packages/protocol/src/bounds.js";
import { buildRuntimeMessageProjectionMessage as message } from "./runtime-message-builders.js";

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

describe("Runtime provider-context projection", () => {
  test("projects provider-visible Runtime facts without Runtime identities or lifecycle metadata", () => {
    const result = toGatewayProviderContext([
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
      context: [
        {
          role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
          content: [{ text: { text: "hello" } }],
        },
        {
          role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
          content: [
            { text: { text: "answer" } },
            { reasoning: { text: "thinking", metadataJson: "{\"anthropic\":{\"signature\":\"sig_1\"}}" } },
            { toolCall: { modelToolCallId: "call-1", name: "lookup", inputJson: "{\"q\":\"hi\"}" } },
            {
              toolResult: {
                modelToolCallId: "call-1",
                completed: { outputJson: "{\"text\":\"result\"}" },
                error: undefined,
                cancelled: undefined,
              },
            },
          ],
        },
      ],
    });
  });

  test("omits open assistant drafts from provider context", () => {
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

    expect(toGatewayProviderContext([
      message("user-1", "user", [textPart("user-text-1", "user-1", "hello")]),
      streaming,
    ])).toEqual({
      ok: true,
      context: [{
        role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
        content: [{ text: { text: "hello" } }],
      }],
    });
  });

  test("preserves signed empty reasoning for provider replay", () => {
    const result = toGatewayProviderContext([
      message("assistant-1", "assistant", [{
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
      }]),
    ]);

    expect(result).toEqual({
      ok: true,
      context: [{
        role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
        content: [{ reasoning: { text: "", metadataJson: "{\"anthropic\":{\"signature\":\"sig_empty\"}}" } }],
      }],
    });
  });

  test("keeps a pending public Tool Call in place and adds only its later paired result", () => {
    const pending = message("assistant-1", "assistant", [toolPart({
        status: "running",
        input: { value: { q: "hi" }, preview: "{\"q\":\"hi\"}", truncated: false },
      })]);
    const terminal = message("assistant-2", "assistant", [{
      ...toolPart({
        status: "completed",
        input: { value: { q: "hi" }, preview: "{\"q\":\"hi\"}", truncated: false },
        output: { text: "result", truncated: false },
      }),
      id: "tool-part-terminal",
      messageId: "assistant-2",
    }]);
    expect(toGatewayProviderContext([pending])).toEqual({
      ok: true,
      context: [{
        role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
        content: [{ toolCall: { modelToolCallId: "call-1", name: "lookup", inputJson: "{\"q\":\"hi\"}" } }],
      }],
    });
    expect(toGatewayProviderContext([pending, terminal])).toEqual({
      ok: true,
      context: [
        {
          role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
          content: [{ toolCall: { modelToolCallId: "call-1", name: "lookup", inputJson: "{\"q\":\"hi\"}" } }],
        },
        {
          role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
          content: [{
            toolResult: {
              modelToolCallId: "call-1",
              completed: { outputJson: "{\"text\":\"result\"}" },
              error: undefined,
              cancelled: undefined,
            },
          }],
        },
      ],
    });
  });

  test("rejects a terminal public Tool without durable identity", () => {
    expect(toGatewayProviderContext([
      message("assistant-1", "assistant", [toolPart({
        status: "completed",
        input: { value: { q: "hi" }, preview: "{\"q\":\"hi\"}", truncated: false },
        output: { text: "result", truncated: false },
      }, "")]),
    ])).toMatchObject({ ok: false, error: { code: "provider_invalid_request" } });
  });

  test("projects internal invalid-tool repair only as its model Tool Call/Error pair", () => {
    const result = toGatewayProviderContext([
      message("assistant-1", "assistant", [toolPart({
        status: "running",
        input: { value: { q: "hi" }, preview: "{\"q\":\"hi\"}", truncated: false },
      }, "")]),
      message("assistant-repair-1", "assistant", [{
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
      }]),
    ]);

    expect(result).toMatchObject({ ok: true });
    if (result.ok) {
      expect(result.context).toHaveLength(2);
      expect(result.context.flatMap((entry) => entry.content)).toMatchObject([
        { toolCall: { modelToolCallId: "call-1", name: "lookup" } },
        { toolResult: { modelToolCallId: "call-1", error: { errorJson: expect.any(String) } } },
      ]);
    }
  });

  test("keeps completed, errored, and cancelled Tool wire lowering unchanged", () => {
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
        state: { status: "error" as const, input, error: { type: "provider_tool_protocol_error" as const, message: "failed", retryable: false } },
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
      const projected = toGatewayProviderContext([
        message("assistant-1", "assistant", [toolPart(testCase.state)]),
      ]);
      expect(projected.ok).toBe(true);
      if (!projected.ok) continue;

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
        providerContext: projected.context,
        runtime: { systemInstructions: "ordinary Tool regression", timeoutMs: 30_000 },
      });
      expect(assembled.ok).toBe(true);
      if (!assembled.ok) continue;

      expect(validateProviderRequest(assembled.request)).toEqual({ ok: true });
      const lowered = lowerProviderRequest(assembled.request, OpenAIGPT55Rules, { modelOutputTokenLimit: 32_000 });
      const toolMessages = lowered.messages.filter((entry) => entry.role === "assistant" || entry.role === "tool");
      expect(toolMessages).toEqual([
        { role: "assistant", content: [{ type: "tool-call", toolCallId: "call-1", toolName: "lookup", input: { q: "hi" } }] },
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
