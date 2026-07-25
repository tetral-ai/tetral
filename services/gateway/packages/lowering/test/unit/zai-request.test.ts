import { describe, expect, test } from "bun:test";
import {
  ProviderRequestKind,
  RuntimeMessageRole,
  SystemCacheHint,
  SystemSegmentKind,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { ProviderRequest, RuntimeMessage } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { classifyProviderStreamError, ProviderRequestLoweringError } from "../../src/errors.js";
import { lowerProviderRequest, remapOpenAICompatibleMessageMetadataForSDK, type ResolvedProviderRequestAttachment } from "../../src/request.js";
import { ZaiGLM52Rules } from "../../src/rules/zai.js";

describe("zai GLM request lowering", () => {
  // GLM follows the openai-compatible unsupported-media behavior.
  test("zai-unsupported-media follows the openai-compatible unsupported-media rule", () => {
    expect(ZaiGLM52Rules.supportedMedia).toEqual({
      exactMimes: [],
      mimePrefixes: [],
    });

    let caught: unknown;
    try {
      lowerZaiRequest(zaiRequest({
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

    const attachment = resolvedAttachment({ transient: transientOrigin("att_image"), mime: "image/png", filename: "image.png" });
    const lowered = lowerZaiRequest(zaiRequest({ attachments: [attachment] }), { resolvedAttachments: [attachment] });
    expect(lowered.messages.at(-1)).toEqual({
      role: "user",
      content: [{
        type: "text",
        text: "ERROR: cannot read attachment image.png (image/png) from /tmp/image.png: media is not supported by zai/glm-5.2.",
      }],
    });
  });

  test("zai-unsupported-media lowers plain text as one provenance-bearing text part", () => {
    const attachment = resolvedAttachment({
      transient: undefined,
      fileBacked: { sourceEventId: "sevt_text", fileId: "file_text" },
      mime: "text/plain",
      filename: "notes.txt",
      data: new TextEncoder().encode("hello"),
    });
    const lowered = lowerZaiRequest(zaiRequest({ attachments: [attachment] }), { resolvedAttachments: [attachment] });

    expect(lowered.messages.at(-1)).toEqual({
      role: "user",
      content: [{
        type: "text",
        text: "[attachment: notes.txt (text/plain)]\nhello",
      }],
    });
    expect(JSON.stringify(lowered)).not.toContain("ERROR: cannot read attachment");
  });

  // GLM preserves verified reasoning_content under the zai provider key.
  test("zai-reasoning-remap lifts interleaved reasoning_content under the zai provider key", () => {
    const withReasoning = lowerZaiRequest(zaiRequest({
      messages: [{
        id: "msg_assistant",
        role: RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_ASSISTANT,
        status: "completed",
        origin: "agent",
        parts: [
          { id: "reasoning", reasoning: { text: "hidden thought", metadataJson: "{}" } },
          { id: "text", text: { text: "visible answer" } },
        ],
      }],
    }));
    const withoutReasoning = lowerZaiRequest(zaiRequest({
      messages: [{
        id: "msg_assistant_plain",
        role: RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_ASSISTANT,
        status: "completed",
        origin: "agent",
        parts: [{ id: "text", text: { text: "visible answer" } }],
      }],
    }));

    expect(withReasoning.messages).toEqual([{
      role: "assistant",
      content: [{ type: "text", text: "visible answer" }],
      providerOptions: { zai: { reasoning_content: "hidden thought" } },
    }]);
    expect(withoutReasoning.messages).toEqual([{
      role: "assistant",
      content: [{ type: "text", text: "visible answer" }],
      providerOptions: { zai: { reasoning_content: "" } },
    }]);
  });

  test("zai-reasoning-remap remaps per-message reasoning metadata to openaiCompatible only at the SDK boundary", () => {
    const lowered = lowerZaiRequest(zaiRequest({
      model: { providerId: "zai", modelId: "glm-5.2", variant: "high" },
      messages: [{
        id: "msg_assistant",
        role: RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_ASSISTANT,
        status: "completed",
        origin: "agent",
        parts: [
          { id: "reasoning", reasoning: { text: "hidden thought", metadataJson: "{}" } },
          { id: "text", text: { text: "visible answer" } },
        ],
      }],
    }));
    const sdkMessages = remapOpenAICompatibleMessageMetadataForSDK(lowered.messages, ZaiGLM52Rules);

    expect(lowered.messages[0]).toEqual({
      role: "assistant",
      content: [{ type: "text", text: "visible answer" }],
      providerOptions: { zai: { reasoning_content: "hidden thought" } },
    });
    expect(sdkMessages[0]).toEqual({
      role: "assistant",
      content: [{ type: "text", text: "visible answer" }],
      providerOptions: { openaiCompatible: { reasoning_content: "hidden thought" } },
    });
    expect(lowered.options.providerOptions).toEqual({
      zai: {
        thinking: { type: "enabled", clear_thinking: false },
        reasoningEffort: "high",
      },
    });
  });

  // from opencode:packages/opencode/test/provider/transform.test.ts "ProviderTransform.options - zai/zhipuai thinking"
  test("zai-thinking-mode enables GLM thinking with clear_thinking false", () => {
    const lowered = lowerZaiRequest(zaiRequest());

    expect(lowered.options.providerOptions).toEqual({
      zai: {
        thinking: { type: "enabled", clear_thinking: false },
        reasoningEffort: "high",
      },
    });
  });

  // from opencode:packages/opencode/test/provider/transform.test.ts "glm-5.2 returns native effort variants for openai-compatible providers"
  test("zai-effort-lowering routes only high and max reasoningEffort variants through zai providerOptions", () => {
    const high = lowerZaiRequest(zaiRequest({
      model: { providerId: "zai", modelId: "glm-5.2", variant: "high" },
    }));
    const max = lowerZaiRequest(zaiRequest({
      model: { providerId: "zai", modelId: "glm-5.2", variant: "max" },
    }));
    const defaultVariant = lowerZaiRequest(zaiRequest());

    expect(high.options.providerOptions.zai).toMatchObject({ reasoningEffort: "high" });
    expect(max.options.providerOptions.zai).toMatchObject({ reasoningEffort: "max" });
    expect(defaultVariant.options.providerOptions.zai).toMatchObject({ reasoningEffort: "high" });
    expect(() => lowerZaiRequest(zaiRequest({
      model: { providerId: "zai", modelId: "glm-5.2", variant: "medium" },
    }))).toThrow("unsupported model variant");
  });

  // GLM emits no cache metadata and keeps provider options under the zai key.
  test("zai-cache-absence emits no cache-control metadata and uses the zai providerOptions key", () => {
    const lowered = lowerZaiRequest(zaiRequest({
      system: [
        { kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE, text: "stable one", cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE },
        { kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_AGENT, text: "stable two", cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_SESSION },
      ],
      messages: [
        textMessage("msg_1", "first"),
        textMessage("msg_2", "second"),
      ],
    }));

    expect(JSON.stringify(lowered.messages)).not.toContain("cacheControl");
    expect(Object.keys(lowered.options.providerOptions)).toEqual(["zai"]);
  });

  // GLM leaves temperature, top-p, and top-k unset.
  test("zai-sampling-defaults omits temperature, top_p, and top_k sampling defaults", () => {
    const lowered = lowerZaiRequest(zaiRequest());

    expect(lowered.options.temperature).toBeUndefined();
    expect(lowered.options.topP).toBeUndefined();
    expect(lowered.options.topK).toBeUndefined();
  });

  // GLM passes openai-compatible tool schemas through unchanged.
  test("zai-schema-passthrough passes GLM openai-compatible schemas through without provider surgery", () => {
    const inputSchema = { type: "object", properties: { path: { type: "string" } }, required: ["path"] };
    const lowered = lowerZaiRequest(zaiRequest({
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

  // GLM uses only the generic provider error classifier.
  test("zai-error-mapping leaves provider-specific error overrides empty beyond generic classification", () => {
    expect(ZaiGLM52Rules.providerSpecificErrorRules).toEqual([]);
  });
});

function lowerZaiRequest(
  request: ProviderRequest,
  options: number | { readonly modelOutputTokenLimit?: number; readonly resolvedAttachments?: readonly ResolvedProviderRequestAttachment[] } = 128_000,
) {
  const normalized = typeof options === "number" ? { modelOutputTokenLimit: options } : options;
  return lowerProviderRequest(request, ZaiGLM52Rules, {
    modelOutputTokenLimit: normalized.modelOutputTokenLimit ?? 128_000,
    resolvedAttachments: normalized.resolvedAttachments,
  });
}

function zaiRequest(overrides: Partial<ProviderRequest> = {}): ProviderRequest {
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
    model: { providerId: "zai", modelId: "glm-5.2", variant: "" },
    system: [],
    messages: [textMessage("msg_1", "hello")],
    tools: [],
    attachments: [],
    limits: { maxOutputTokens: 2048, timeoutMs: 30_000 },
    ...overrides,
  };
}

function textMessage(id: string, text: string): RuntimeMessage {
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
