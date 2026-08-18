import { describe, expect, test } from "bun:test";
import {
  ProviderRequestKind,
  ProviderContextRole,
  SystemCacheHint,
  SystemSegmentKind,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { ProviderRequest, ProviderContextEntry } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { lowerProviderRequest, type ResolvedProviderRequestAttachment } from "../../src/request.js";
import { MoonshotKimiK3Rules } from "../../src/rules/moonshotai.js";

describe("moonshotai Kimi request lowering", () => {
  // Kimi inherits the Anthropic-family content and media behavior without Claude tool-id scrubbing.
  test("kimi-content-media-lowering inherits Anthropic reasoning/media handling but does not scrub non-Claude tool ids", () => {
    expect(MoonshotKimiK3Rules.supportedMedia).toEqual({
      exactMimes: ["application/pdf", "text/plain"],
      mimePrefixes: ["image/"],
    });

    const lowered = lowerKimiRequest(kimiRequest({
      context: [{
        role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
        content: [
          { reasoning: { text: "", metadataJson: JSON.stringify({ anthropic: { signature: "sig_1" } }) } },
          { toolCall: {
              modelToolCallId: "call:with/slashes and spaces",
              name: "Read",
              inputJson: JSON.stringify({ path: "/tmp/a" }),
          } },
          { toolResult: {
              modelToolCallId: "call:with/slashes and spaces",
              completed: { outputJson: JSON.stringify({ text: "ok" }) },
              error: undefined,
              cancelled: undefined,
          } },
        ],
      }],
    }));

    expect(lowered.messages[1]).toMatchObject({
      role: "assistant",
      content: [
        { type: "reasoning", text: "", providerMetadata: { anthropic: { signature: "sig_1" } } },
        { type: "tool-call", toolCallId: "call:with/slashes and spaces" },
        { type: "text", text: " " },
      ],
    });
    expect(lowered.messages[2]).toMatchObject({
      role: "tool",
      content: [{ type: "tool-result", toolCallId: "call:with/slashes and spaces" }],
    });

    const attachment = resolvedAttachment({ transient: transientOrigin("att_image"), mime: "image/png", filename: "image.png" });
    const withAttachment = lowerKimiRequest(kimiRequest({ attachments: [attachment] }), { resolvedAttachments: [attachment] });
    expect(withAttachment.messages.at(-1)).toEqual({
      role: "user",
      content: [{
        type: "image",
        image: new Uint8Array([1, 2, 3]),
        mediaType: "image/png",
        providerOptions: { anthropic: { cacheControl: { type: "ephemeral" } } },
      }],
    });
  });

  test("kimi-content-media-lowering keeps plain text on the Anthropic document-text path", () => {
    const attachment = resolvedAttachment({
      transient: undefined,
      fileBacked: { sourceEventId: "sevt_text", fileId: "file_text" },
      mime: "text/plain",
      filename: "notes.txt",
      data: new TextEncoder().encode("hello"),
    });
    const lowered = lowerKimiRequest(kimiRequest({ attachments: [attachment] }), { resolvedAttachments: [attachment] });

    expect(lowered.messages.at(-1)).toMatchObject({
      role: "user",
      content: [{
        type: "file",
        data: new TextEncoder().encode("hello"),
        filename: "notes.txt",
        mediaType: "text/plain",
      }],
    });
    expect(JSON.stringify(lowered)).not.toContain("ERROR: cannot read attachment");
  });

  // Kimi cache markers are attached to selected content parts rather than whole messages.
  test("kimi-cache-placement places Kimi cache markers on selected content parts", () => {
    const lowered = lowerKimiRequest(kimiRequest({
      system: [
        { kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE, text: "stable one", cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE },
        { kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_AGENT, text: "stable two", cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_SESSION },
        { kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_SKILL, text: "stable three", cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE },
      ],
      context: [
        textMessage("first"),
        textMessage("second"),
        multipartUserMessage(["third-a", "third-b"]),
      ],
    }));

    expect(contentCacheControlType(lowered.messages[0])).toBe("ephemeral");
    expect(contentCacheControlType(lowered.messages[1])).toBe("ephemeral");
    expect(contentCacheControlType(lowered.messages[2])).toBeUndefined();
    expect(messageCacheControlType(lowered.messages[0])).toBeUndefined();
    expect(messageCacheControlType(lowered.messages[1])).toBeUndefined();
    expect(contentCacheControlType(lowered.messages[3])).toBeUndefined();
    expect(contentCacheControlType(lowered.messages[4])).toBe("ephemeral");
    expect(contentCacheControlType(lowered.messages[5])).toBe("ephemeral");
    expect(firstContentCacheControlType(lowered.messages[5])).toBeUndefined();
    expect(messageCacheControlType(lowered.messages[4])).toBeUndefined();
    expect(messageCacheControlType(lowered.messages[5])).toBeUndefined();
  });

  // The non-Claude Anthropic transport disables fine-grained Kimi tool streaming.
  test("kimi-tool-streaming disables fine-grained tool streaming on the Anthropic transport", () => {
    const lowered = lowerKimiRequest(kimiRequest());

    expect(lowered.options.providerOptions).toMatchObject({
      anthropic: { toolStreaming: false },
    });
  });

  // K3 realizes its only supported max posture through the coding endpoint's
  // native default, without sending Anthropic effort or thinking controls.
  test("kimi-beta-effort-absence sends no beta, effort, or thinking fields", () => {
    const lowered = lowerKimiRequest(kimiRequest({
      model: { providerId: "moonshotai", modelId: "kimi-k3", variant: "max" },
    }));

    expect(lowered.options.headers).toEqual({});
    expect(lowered.options.providerOptions.anthropic).not.toHaveProperty("effort");
    expect(lowered.options.providerOptions.anthropic).not.toHaveProperty("thinking");
  });

  // K3 uses provider-default sampling for its native-thinking path.
  test("kimi-sampling-defaults freezes K3 sampling to provider defaults", () => {
    const lowered = lowerKimiRequest(kimiRequest());

    expect(lowered.options.temperature).toBeUndefined();
    expect(lowered.options.topP).toBeUndefined();
    expect(lowered.options.topK).toBeUndefined();
  });

  // from opencode:packages/opencode/test/provider/transform.test.ts "ProviderTransform.schema - moonshot $ref siblings"
  test("kimi-schema-lowering applies Moonshot $ref collapse and tuple items schema surgery", () => {
    const inputSchema = {
      type: "object",
      properties: {
        refSibling: { $ref: "#/$defs/Thing", description: "drop me" },
        tuple: { type: "array", items: [{ type: "string" }, { type: "number" }] },
        emptyTuple: { type: "array", items: [] },
        nested: {
          anyOf: [
            { $ref: "#/$defs/Thing", title: "drop me too" },
            { type: "array", items: [{ type: "boolean" }] },
          ],
        },
      },
      $defs: {
        Thing: { type: "object", properties: { value: { type: "string" } } },
      },
    };
    const lowered = lowerKimiRequest(kimiRequest({
      tools: [{
        name: "Search",
        description: "Search",
        function: { inputSchemaJson: JSON.stringify(inputSchema), outputSchemaJson: undefined },
      }],
    }));

    expect(lowered.tools.Search?.inputSchema?.schema).toEqual({
      type: "object",
      properties: {
        refSibling: { $ref: "#/$defs/Thing" },
        tuple: { type: "array", items: { type: "string" } },
        emptyTuple: { type: "array", items: {} },
        nested: {
          anyOf: [
            { $ref: "#/$defs/Thing" },
            { type: "array", items: { type: "boolean" } },
          ],
        },
      },
      $defs: {
        Thing: { type: "object", properties: { value: { type: "string" } } },
      },
    });
  });

  // Kimi uses only the generic provider error classifier.
  test("kimi-error-mapping leaves provider-specific error overrides empty beyond generic classification", () => {
    expect(MoonshotKimiK3Rules.providerSpecificErrorRules).toEqual([]);
  });

  test("kimi-output-limit sends the catalog's documented model output limit when the request sets none", () => {
    const unset = lowerKimiRequest(kimiRequest({ limits: { maxOutputTokens: 0, timeoutMs: 30_000 } }), 131_072);

    expect(unset.options.maxOutputTokens).toBe(131_072);
  });
});

function lowerKimiRequest(
  request: ProviderRequest,
  options: number | { readonly modelOutputTokenLimit?: number; readonly resolvedAttachments?: readonly ResolvedProviderRequestAttachment[] } = 32_000,
) {
  const normalized = typeof options === "number" ? { modelOutputTokenLimit: options } : options;
  return lowerProviderRequest(request, MoonshotKimiK3Rules, {
    modelOutputTokenLimit: normalized.modelOutputTokenLimit ?? 32_000,
    resolvedAttachments: normalized.resolvedAttachments,
  });
}

function kimiRequest(overrides: Partial<ProviderRequest> = {}): ProviderRequest {
  return {
    requestId: "req_1",
    modelRequestId: "mreq_1",
    requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
    workspaceId: "wksp_1",
    sessionId: "sesn_1",
    sessionThreadId: "thrd_1",
    bindingId: "bind_1",
    bindingGeneration: 1,
    runtimeBindingToken: "rtbt_v1.test",
    model: { providerId: "moonshotai", modelId: "kimi-k3", variant: "" },
    system: [{ kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE, text: "You are concise.", cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE }],
    context: [textMessage("hello")],
    tools: [],
    attachments: [],
    limits: { maxOutputTokens: 2048, timeoutMs: 30_000 },
    ...overrides,
  };
}

function textMessage(text: string): ProviderContextEntry {
  return {
    role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
    content: [{ text: { text } }],
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
    sourcePath: "/tmp/image.png",
    pageRange: "",
    detail: "auto",
  };
}

function multipartUserMessage(texts: readonly string[]): ProviderContextEntry {
  return {
    role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
    content: texts.map((text) => ({ text: { text } })),
  };
}

function messageCacheControlType(message: unknown): unknown {
  return (((message as { providerOptions?: { anthropic?: { cacheControl?: { type?: unknown } } } }).providerOptions?.anthropic?.cacheControl)?.type);
}

function contentCacheControlType(message: unknown): unknown {
  const content = (message as { content?: readonly unknown[] }).content;
  const last = content?.[content.length - 1];
  return (((last as { providerOptions?: { anthropic?: { cacheControl?: { type?: unknown } } } } | undefined)?.providerOptions?.anthropic?.cacheControl)?.type);
}

function firstContentCacheControlType(message: unknown): unknown {
  const first = (message as { content?: readonly unknown[] }).content?.[0];
  return (((first as { providerOptions?: { anthropic?: { cacheControl?: { type?: unknown } } } } | undefined)?.providerOptions?.anthropic?.cacheControl)?.type);
}
