import { describe, expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import {
  ProviderRequestKind,
  RuntimeMessageRole,
  RuntimeToolPartState,
  SystemCacheHint,
  SystemSegmentKind,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { ProviderRequest } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { lowerProviderRequest, type ResolvedProviderRequestAttachment } from "../../src/request.js";
import { classifyProviderStreamError, ProviderRequestLoweringError } from "../../src/errors.js";
import { AnthropicOpus48Rules } from "../../src/rules/anthropic.js";

const approvalReviewerOutputSchemaJson = await readFile(
  new URL("../../../../../agent-runtime/packages/runtime-pod/src/assets/approval-reviewer-output-schema.json", import.meta.url),
  "utf8",
);

describe("anthropic request lowering", () => {
  test("anthropic-content-normalization drops empty text and unsigned empty reasoning while preserving signed reasoning", () => {
    const lowered = lowerAnthropicRequest(anthropicRequest({
      messages: [
        {
          id: "msg_assistant",
          role: RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_ASSISTANT,
          status: "completed",
          origin: "agent",
          parts: [
            { id: "empty_text", text: { text: "" } },
            { id: "empty_unsigned_reasoning", reasoning: { text: "", metadataJson: "{}" } },
            { id: "empty_unsigned_anthropic_reasoning", reasoning: { text: "", metadataJson: JSON.stringify({ anthropic: {} }) } },
            { id: "signed_reasoning", reasoning: { text: "", metadataJson: JSON.stringify({ anthropic: { signature: "sig_1" } }) } },
          ],
        },
        {
          id: "msg_empty",
          role: RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_ASSISTANT,
          status: "completed",
          origin: "agent",
          parts: [{ id: "only_empty_text", text: { text: "" } }],
        },
      ],
    }));

    const assistant = lowered.messages.find((message) => message.role === "assistant");
    expect(assistant).toMatchObject({
      role: "assistant",
      content: [
        { type: "text", text: " " },
        { type: "reasoning", text: "", providerMetadata: { anthropic: { signature: "sig_1" } } },
      ],
    });
    expect(lowered.messages).toHaveLength(2);
  });

  test("L3 gives a signed reasoning-only assistant message a companion text part", () => {
    const lowered = lowerAnthropicRequest(anthropicRequest({
      messages: [{
        id: "msg_reasoning_only",
        role: RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_ASSISTANT,
        status: "completed",
        origin: "agent",
        parts: [{ id: "signed_reasoning", reasoning: { text: "thinking", metadataJson: JSON.stringify({ anthropic: { signature: "sig_1" } }) } }],
      }],
    }));

    expect(lowered.messages.find((message) => message.role === "assistant")).toMatchObject({
      role: "assistant",
      content: [
        { type: "reasoning", text: "thinking", providerMetadata: { anthropic: { signature: "sig_1" } } },
        { type: "text", text: " " },
      ],
    });
  });

  test("anthropic-tool-id-scrubbing scrubs claude tool call ids to Anthropic's accepted character set", () => {
    const lowered = lowerAnthropicRequest(anthropicRequest({
      messages: [{
        id: "msg_tool",
        role: RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_ASSISTANT,
        status: "completed",
        origin: "agent",
        parts: [{
          id: "tool_part",
          tool: {
            callId: "call:with/slashes and spaces",
            name: "Read",
            toolUseEventId: "sevt_1",
            state: RuntimeToolPartState.RUNTIME_TOOL_PART_STATE_COMPLETED,
            inputJson: JSON.stringify({ path: "/tmp/a" }),
            outputOrErrorJson: JSON.stringify({ text: "ok" }),
          },
        }],
      }],
    }));

    expect(lowered.messages[1]).toMatchObject({
      role: "assistant",
      content: [{ type: "tool-call", toolCallId: "call_with_slashes_and_spaces" }],
    });
    expect(lowered.messages[2]).toMatchObject({
      role: "tool",
      content: [{ type: "tool-result", toolCallId: "call_with_slashes_and_spaces" }],
    });
  });

  test("anthropic-media-lowering lowers resolved image, PDF, and plain-text attachments into user media parts", () => {
    expect(AnthropicOpus48Rules.supportedMedia).toEqual({
      exactMimes: ["application/pdf", "text/plain"],
      mimePrefixes: ["image/"],
    });
    const attachments: ResolvedProviderRequestAttachment[] = [
      resolvedAttachment({ transient: transientOrigin("att_image"), mime: "image/png", filename: "image.png", data: new Uint8Array([1, 2]) }),
      resolvedAttachment({ transient: transientOrigin("att_pdf"), mime: "application/pdf", filename: "report.pdf", data: new Uint8Array([3, 4]) }),
      resolvedAttachment({
        transient: undefined,
        fileBacked: { sourceEventId: "sevt_text", fileId: "file_text" },
        mime: "text/plain",
        filename: "notes.txt",
        data: new TextEncoder().encode("hello"),
      }),
    ];
    const lowered = lowerAnthropicRequest(anthropicRequest({ attachments }), { resolvedAttachments: attachments });

    expect(lowered.messages.at(-1)).toEqual({
      role: "user",
      content: [
        { type: "image", image: new Uint8Array([1, 2]), mediaType: "image/png" },
        { type: "file", data: new Uint8Array([3, 4]), filename: "report.pdf", mediaType: "application/pdf" },
        { type: "file", data: new TextEncoder().encode("hello"), filename: "notes.txt", mediaType: "text/plain" },
      ],
      providerOptions: { anthropic: { cacheControl: { type: "ephemeral" } } },
    });
    expect(JSON.stringify(lowered)).not.toContain("ERROR: cannot read attachment");
  });

  test("attachment lowering matches resolved bytes by origin identity", () => {
    const transient = resolvedAttachment({
      transient: transientOrigin("shared"),
      mime: "image/png",
      filename: "image.png",
      data: new Uint8Array([1]),
    });
    const fileBacked: ResolvedProviderRequestAttachment = {
      transient: undefined,
      fileBacked: { sourceEventId: "sevt_user_1", fileId: "shared" },
      mime: "application/pdf",
      filename: "report.pdf",
      data: new Uint8Array([2]),
    };

    const lowered = lowerAnthropicRequest(
      anthropicRequest({ attachments: [transient, fileBacked] }),
      { resolvedAttachments: [fileBacked, transient] },
    );

    expect(lowered.messages.at(-1)).toMatchObject({
      role: "user",
      content: [
        { type: "image", image: new Uint8Array([1]), mediaType: "image/png" },
        { type: "file", data: new Uint8Array([2]), filename: "report.pdf", mediaType: "application/pdf" },
      ],
    });
  });

  test("file attachment origin matching is collision-free for delimiter-bearing ids", () => {
    const first: ResolvedProviderRequestAttachment = {
      transient: undefined,
      fileBacked: { sourceEventId: "sevt:a", fileId: "file" },
      mime: "application/pdf",
      filename: "first.pdf",
      data: new Uint8Array([1]),
    };
    const second: ResolvedProviderRequestAttachment = {
      transient: undefined,
      fileBacked: { sourceEventId: "sevt", fileId: "a:file" },
      mime: "application/pdf",
      filename: "second.pdf",
      data: new Uint8Array([2]),
    };

    const lowered = lowerAnthropicRequest(
      anthropicRequest({ attachments: [first, second] }),
      { resolvedAttachments: [second, first] },
    );

    expect(lowered.messages.at(-1)).toMatchObject({
      role: "user",
      content: [
        { type: "file", data: new Uint8Array([1]), filename: "first.pdf" },
        { type: "file", data: new Uint8Array([2]), filename: "second.pdf" },
      ],
    });
  });

  test("anthropic-cache-placement places cache control at message level on first system and last non-system messages", () => {
    const lowered = lowerAnthropicRequest(anthropicRequest({
      system: [
        { kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE, text: "stable one", cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE },
        { kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_AGENT, text: "stable two", cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_SESSION },
        { kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_SKILL, text: "stable three", cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE },
      ],
      messages: [
        textMessage("msg_1", "first"),
        textMessage("msg_2", "second"),
        textMessage("msg_3", "third"),
      ],
    }));

    expect(cacheControlType(lowered.messages[0])).toBe("ephemeral");
    expect(cacheControlType(lowered.messages[1])).toBe("ephemeral");
    expect(cacheControlType(lowered.messages[2])).toBeUndefined();
    expect(cacheControlType(lowered.messages[3])).toBeUndefined();
    expect(cacheControlType(lowered.messages[4])).toBe("ephemeral");
    expect(cacheControlType(lowered.messages[5])).toBe("ephemeral");
  });

  test("anthropic-adaptive-thinking emits adaptive summarized thinking with the ModelRef effort variant", () => {
    const lowered = lowerAnthropicRequest(anthropicRequest({ model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "xhigh" } }));

    expect(lowered.options.providerOptions).toEqual({
      anthropic: {
        thinking: { type: "adaptive", display: "summarized" },
        sendReasoning: true,
        effort: "xhigh",
      },
    });
    const defaultEffort = lowerAnthropicRequest(anthropicRequest({ model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" } }));
    expect(defaultEffort.options.providerOptions).toEqual({
      anthropic: {
        thinking: { type: "adaptive", display: "summarized" },
        sendReasoning: true,
        effort: "xhigh",
      },
    });
    expect(() => lowerAnthropicRequest(anthropicRequest({ model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "standard" } }))).toThrow("unsupported model variant");
    expect(() => lowerAnthropicRequest(anthropicRequest({ model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "fast" } }))).toThrow("unsupported model variant");
    expect(() => lowerAnthropicRequest(anthropicRequest({ model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "turbo" } }))).toThrow();
  });

  test("anthropic-reasoning-metadata preserves Anthropic reasoning metadata and enables reasoning send-through", () => {
    const lowered = lowerAnthropicRequest(anthropicRequest({
      messages: [{
        id: "msg_reasoning",
        role: RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_ASSISTANT,
        status: "completed",
        origin: "agent",
        parts: [
          { id: "foreign", reasoning: { text: "foreign thought", metadataJson: JSON.stringify({ openai: { itemId: "rs_1" } }) } },
          { id: "anthropic", reasoning: { text: "native thought", metadataJson: JSON.stringify({ anthropic: { redactedData: "red_1" } }) } },
        ],
      }],
    }));

    expect(lowered.messages[1]).toMatchObject({
      role: "assistant",
      content: [
        { type: "text", text: "foreign thought" },
        { type: "reasoning", text: "native thought", providerMetadata: { anthropic: { redactedData: "red_1" } } },
      ],
    });
    expect((lowered.options.providerOptions.anthropic as Record<string, unknown>).sendReasoning).toBe(true);
  });

  test("anthropic-beta-header sends the exact Anthropic beta header only for official Anthropic rules", () => {
    const lowered = lowerAnthropicRequest(anthropicRequest());

    expect(lowered.options.headers).toEqual({
      "anthropic-beta": "interleaved-thinking-2025-05-14,fine-grained-tool-streaming-2025-05-14",
    });
  });

  test("anthropic-sampling-defaults omits temperature, top_p, and top_k sampling defaults", () => {
    const lowered = lowerAnthropicRequest(anthropicRequest());

    expect(lowered.options.temperature).toBeUndefined();
    expect(lowered.options.topP).toBeUndefined();
    expect(lowered.options.topK).toBeUndefined();
  });

  test("anthropic-schema-passthrough passes Anthropic tool schemas through without provider surgery", () => {
    const inputSchema = { type: "object", properties: { path: { type: "string" } }, required: ["path"] };
    const lowered = lowerAnthropicRequest(anthropicRequest({
      tools: [{
        name: "Read",
        description: "Read a file",
        inputSchemaJson: JSON.stringify(inputSchema),
        outputSchemaJson: JSON.stringify({ type: "object", properties: { text: { type: "string" } } }),
      }],
    }));

    expect(lowered.tools.Read).toEqual({
      description: "Read a file",
      inputSchema: { kind: "ai-sdk-json-schema", schema: inputSchema },
      outputSchema: { kind: "ai-sdk-json-schema", schema: { type: "object", properties: { text: { type: "string" } } } },
    });
  });

  test("pins the exact Claude Bash provider schema and excludes GPT aliases", () => {
    const bashSchema = {
      type: "object",
      additionalProperties: false,
      properties: {
        command: { type: "string" },
        cwd: { type: "string" },
        timeout: { type: "integer", maximum: 600_000 },
        run_in_background: { type: "boolean" },
      },
      required: ["command"],
    };
    const lowered = lowerAnthropicRequest(anthropicRequest({
      tools: [{ name: "Bash", description: "Run a command", inputSchemaJson: JSON.stringify(bashSchema) }],
    }));

    expect(lowered.tools.Bash?.inputSchema).toEqual({ kind: "ai-sdk-json-schema", schema: bashSchema });
    const wireSchema = JSON.stringify(lowered.tools.Bash?.inputSchema);
    for (const forbidden of ["timeout_ms", "cmd", "workdir", "yield-time_ms"]) {
      expect(wireSchema).not.toContain(`\"${forbidden}\"`);
    }
  });

  test("anthropic-error-mapping leaves provider-specific error overrides empty beyond generic classification", () => {
    expect(AnthropicOpus48Rules.providerSpecificErrorRules).toEqual([]);
  });

  test("pins approval reviewer structured output to the configured Anthropic model rule", () => {
    const schema = JSON.parse(approvalReviewerOutputSchemaJson) as unknown;
    const lowered = lowerAnthropicRequest(anthropicRequest({
      requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
      outputSchemaJson: approvalReviewerOutputSchemaJson,
    }));

    expect(AnthropicOpus48Rules.requestOutputSchema).toBe("approval-reviewer");
    expect(lowered.outputSchema).toEqual({ kind: "ai-sdk-json-schema", schema });
    expect(lowerAnthropicRequest(anthropicRequest()).outputSchema).toBeUndefined();
  });

  test("rejects missing, malformed, and cross-kind request output schemas during lowering", () => {
    expect(() => lowerAnthropicRequest(anthropicRequest({
      requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
      outputSchemaJson: undefined,
    }))).toThrow("approval reviewer output schema");
    expect(() => lowerAnthropicRequest(anthropicRequest({
      requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
      outputSchemaJson: "[]",
    }))).toThrow("approval reviewer output schema");
    expect(() => lowerAnthropicRequest(anthropicRequest({
      outputSchemaJson: approvalReviewerOutputSchemaJson,
    }))).toThrow("request output schema is not allowed");
  });

  test("L4 rejects unknown tool state instead of inventing interrupted repair messages", () => {
    expect(() => lowerAnthropicRequest(anthropicRequest({
      messages: [{
        id: "msg_tool",
        role: RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_ASSISTANT,
        status: "completed",
        origin: "agent",
        parts: [{
          id: "tool_part",
          tool: {
            callId: "call_1",
            name: "Read",
            toolUseEventId: "sevt_1",
            state: RuntimeToolPartState.UNRECOGNIZED,
            inputJson: "{}",
            outputOrErrorJson: "{}",
          },
        }],
      }],
    }))).toThrow("unknown tool state");
  });

  test("L5 sanitizes lone UTF-16 surrogates before provider lowering", () => {
    const lowered = lowerAnthropicRequest(anthropicRequest({
      system: [{ kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE, text: "bad \uD800 text", cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE }],
      messages: [
        textMessage("msg_1", "bad \uDC00 text"),
        {
          id: "msg_tool",
          role: RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_ASSISTANT,
          status: "completed",
          origin: "agent",
          parts: [{
            id: "tool_part",
            tool: {
              callId: "call_1",
              name: "Read",
              toolUseEventId: "sevt_1",
              state: RuntimeToolPartState.RUNTIME_TOOL_PART_STATE_COMPLETED,
              inputJson: JSON.stringify({ path: "bad \uD800 input" }),
              outputOrErrorJson: JSON.stringify({ text: "bad \uDC00 output" }),
            },
          }],
        },
      ],
      tools: [{
        name: "Read",
        description: "bad \uD800 description",
        inputSchemaJson: JSON.stringify({ type: "object", properties: { q: { const: "bad \uD800 schema" } } }),
        outputSchemaJson: undefined,
      }],
    }));

    expect(lowered.messages[0]).toMatchObject({ role: "system", content: "bad \uFFFD text" });
    expect(lowered.messages[1]).toMatchObject({ role: "user", content: [{ type: "text", text: "bad \uFFFD text" }] });
    expect(lowered.messages[2]).toMatchObject({ role: "assistant", content: [{ type: "tool-call", input: { path: "bad \uFFFD input" } }] });
    expect(lowered.messages[3]).toMatchObject({ role: "tool", content: [{ type: "tool-result", output: { text: "bad \uFFFD output" } }] });
    expect(lowered.tools.Read).toMatchObject({
      description: "bad \uFFFD description",
      inputSchema: { schema: { properties: { q: { const: "bad \uFFFD schema" } } } },
    });
  });

  test("L6 attachment requests fail loudly when the process layer does not provide bytes", () => {
    let caught: unknown;
    try {
      lowerAnthropicRequest(anthropicRequest({
        attachments: [{
          transient: {
            attachmentRef: "att_1",
            sourceToolUseEventId: "sevt_1",
            sourcePath: "/tmp/image.png",
            pageRange: "",
            detail: "auto",
          },
          fileBacked: undefined,
          mime: "image/png",
          filename: "image.png",
        }],
      }));
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(ProviderRequestLoweringError);
    expect(classifyProviderStreamError(caught)).toMatchObject({
      code: "provider_request_invalid",
      retryable: false,
      fatal: true,
    });
  });

  test("L8 clamps request max_output_tokens to the catalog model output cap", () => {
    const capped = lowerAnthropicRequest(anthropicRequest({ limits: { maxOutputTokens: 999_999, timeoutMs: 30_000 } }), 4096);
    const belowCap = lowerAnthropicRequest(anthropicRequest({ limits: { maxOutputTokens: 1024, timeoutMs: 30_000 } }), 4096);

    expect(capped.options.maxOutputTokens).toBe(4096);
    expect(belowCap.options.maxOutputTokens).toBe(1024);
  });
});

function lowerAnthropicRequest(
  request: ProviderRequest,
  options: number | { readonly modelOutputTokenLimit?: number; readonly resolvedAttachments?: readonly ResolvedProviderRequestAttachment[] } = 16_384,
) {
  const normalized = typeof options === "number" ? { modelOutputTokenLimit: options } : options;
  return lowerProviderRequest(request, AnthropicOpus48Rules, {
    modelOutputTokenLimit: normalized.modelOutputTokenLimit ?? 16_384,
    resolvedAttachments: normalized.resolvedAttachments,
  });
}

function anthropicRequest(overrides: Partial<ProviderRequest> = {}): ProviderRequest {
  return {
    requestId: "req_1",
    modelRequestId: "mreq_1",
    requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
    workspaceId: "wksp_1",
    sessionId: "sesn_1",
    sessionThreadId: "thrd_1",
    parentThreadId: undefined,
    bindingId: "bind_1",
    bindingGeneration: 1,
    runtimeBindingToken: "rtbt_v1.test",
    model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" },
    system: [{ kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE, text: "You are concise.", cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE }],
    messages: [textMessage("msg_1", "hello")],
    tools: [],
    attachments: [],
    limits: { maxOutputTokens: 2048, timeoutMs: 30_000 },
    ...overrides,
  };
}

function textMessage(id: string, text: string) {
  return {
    id,
    role: RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_USER,
    status: "completed",
    origin: "user",
    parts: [{ id: `${id}_part`, text: { text } }],
  };
}

function resolvedAttachment(
  overrides: Partial<ResolvedProviderRequestAttachment> = {},
): ResolvedProviderRequestAttachment {
  return {
    transient: transientOrigin("att_1"),
    fileBacked: undefined,
    mime: "image/png",
    filename: "image.png",
    data: new Uint8Array([1, 2, 3]),
    ...overrides,
  };
}

function transientOrigin(attachmentRef: string): NonNullable<ResolvedProviderRequestAttachment["transient"]> {
  return {
    attachmentRef,
    sourceToolUseEventId: "sevt_1",
    sourcePath: "/tmp/image.png",
    pageRange: "",
    detail: "auto",
  };
}

function cacheControlType(message: unknown): unknown {
  return (((message as { providerOptions?: { anthropic?: { cacheControl?: { type?: unknown } } } }).providerOptions?.anthropic?.cacheControl)?.type);
}
