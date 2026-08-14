import { describe, expect, test } from "bun:test";
import {
  ProviderRequestKind,
  ProviderContextRole,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { ProviderRequest, ProviderContextEntry } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { ProviderRequestLoweringError, classifyProviderStreamError } from "../../src/errors.js";
import { lowerProviderRequest, type ResolvedProviderRequestAttachment } from "../../src/request.js";
import { OpenAIGPT55Rules } from "../../src/rules/openai.js";

describe("openai request lowering", () => {
  test("openai-reasoning-metadata preserves encrypted reasoning metadata and strips stateless item ids", () => {
    const lowered = lowerOpenAIRequest(openAIRequest({
      context: [{
        role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
        content: [
          {
            reasoning: {
              text: "",
              metadataJson: JSON.stringify({
                openai: {
                  encrypted_content: "enc_reasoning",
                  itemId: "rs_item",
                  id: "rs_raw",
                },
              }),
            },
          },
        ],
      }],
    }));

    expect(lowered.messages).toEqual([{
      role: "assistant",
      content: [{
        type: "reasoning",
        text: "",
        providerMetadata: {
          openai: { encrypted_content: "enc_reasoning" },
        },
      }],
    }]);

    const noMetadata = lowerOpenAIRequest(openAIRequest({
      context: [{
        role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
        content: [{
          reasoning: { text: "internal chain", metadataJson: "{}" },
        }],
      }],
    }));

    expect(noMetadata.messages).toEqual([{
      role: "assistant",
      content: [{ type: "text", text: "internal chain" }],
    }]);
  });

  test("openai-media-lowering lowers resolved image and PDF attachments into user media parts", () => {
    expect(OpenAIGPT55Rules.supportedMedia).toEqual({
      exactMimes: ["application/pdf"],
      mimePrefixes: ["image/"],
    });

    let caught: unknown;
    try {
      lowerOpenAIRequest(openAIRequest({
        attachments: [{
          transient: {
            attachmentRef: "att_1",
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

    const attachments: ResolvedProviderRequestAttachment[] = [
      resolvedAttachment({ transient: transientOrigin("att_image"), mime: "image/png", filename: "image.png", data: new Uint8Array([1, 2]) }),
      resolvedAttachment({ transient: transientOrigin("att_pdf"), mime: "application/pdf", filename: "report.pdf", data: new Uint8Array([3, 4]) }),
    ];
    const lowered = lowerOpenAIRequest(openAIRequest({ attachments }), { resolvedAttachments: attachments });
    expect(lowered.messages.at(-1)).toEqual({
      role: "user",
      content: [
        { type: "image", image: new Uint8Array([1, 2]), mediaType: "image/png" },
        { type: "file", data: new Uint8Array([3, 4]), filename: "report.pdf", mediaType: "application/pdf" },
      ],
    });
  });

  test("openai-media-lowering renders plain text with provenance and replacement decoding", () => {
    const attachment = resolvedAttachment({
      transient: undefined,
      fileBacked: { sourceEventId: "sevt_text", fileId: "file_text" },
      mime: "text/plain",
      filename: "notes.txt",
      data: new Uint8Array([0x41, 0xff, 0x42]),
    });
    const lowered = lowerOpenAIRequest(openAIRequest({ attachments: [attachment] }), { resolvedAttachments: [attachment] });

    expect(lowered.messages.at(-1)).toEqual({
      role: "user",
      content: [{
        type: "text",
        text: "[attachment: notes.txt (text/plain)]\nA\uFFFDB",
      }],
    });
    expect(JSON.stringify(lowered)).not.toContain("ERROR: cannot read attachment");
  });

  test("openai-effort-lowering routes effort variants through providerOptions.openai", () => {
    const lowered = lowerOpenAIRequest(openAIRequest({
      model: { providerId: "openai", modelId: "gpt-5.5", variant: "xhigh" },
    }));

    expect(lowered.options.providerOptions.openai).toMatchObject({
      reasoningEffort: "xhigh",
      reasoningSummary: "auto",
      include: ["reasoning.encrypted_content"],
    });
    const none = lowerOpenAIRequest(openAIRequest({
      model: { providerId: "openai", modelId: "gpt-5.5", variant: "none" },
    }));
    expect(none.options.providerOptions.openai).toMatchObject({ reasoningEffort: "none" });
    expect(() => lowerOpenAIRequest(openAIRequest({
      model: { providerId: "openai", modelId: "gpt-5.5", variant: "turbo" },
    }))).toThrow("unsupported model variant");
  });

  test("openai-request-options sets Responses API request options without enabling OpenAI execution", () => {
    const lowered = lowerOpenAIRequest(openAIRequest({ sessionId: "sesn_openai" }));

    expect(lowered.options.providerOptions.openai).toMatchObject({
      store: false,
      promptCacheKey: "sesn_openai",
      textVerbosity: "low",
    });
  });

  test("openai-tool-strictness marks every OpenAI tool strict false", () => {
    const lowered = lowerOpenAIRequest(openAIRequest({
      tools: [{
        name: "Read",
        description: "Read a file",
        function: {
          inputSchemaJson: JSON.stringify({ type: "object", properties: { path: { type: "string" } } }),
          outputSchemaJson: undefined,
        },
      }],
    }));

    expect(lowered.tools.Read?.providerOptions).toEqual({
      openai: { strict: false },
    });
  });

  test("openai-custom-tool lowering preserves the freeform grammar arm", () => {
    const grammar = "start: PATCH";
    const lowered = lowerOpenAIRequest(openAIRequest({
      tools: [{
        name: "apply_patch",
        description: "Apply a patch",
        freeform: { larkGrammar: grammar },
      }],
    }));

    expect(lowered.tools.apply_patch).toEqual({
      kind: "freeform",
      description: "Apply a patch",
      larkGrammar: grammar,
    });
  });

  test("openai-base-reasoning applies base reasoning defaults when variant is absent", () => {
    const lowered = lowerOpenAIRequest(openAIRequest());

    expect(lowered.options.providerOptions.openai).toMatchObject({
      reasoningEffort: "medium",
      reasoningSummary: "auto",
      include: ["reasoning.encrypted_content"],
      forceReasoning: true,
    });
  });

  test("openai-output-cap-omission omits maxOutputTokens for OpenAI", () => {
    const lowered = lowerOpenAIRequest(openAIRequest({
      limits: { maxOutputTokens: 2048, timeoutMs: 30_000 },
    }));

    expect("maxOutputTokens" in lowered.options).toBe(false);
  });

  test("openai-header-absence sends no extra official OpenAI identification headers", () => {
    const lowered = lowerOpenAIRequest(openAIRequest());

    expect(lowered.options.headers).toEqual({});
  });

  test("openai-sampling-defaults omits temperature, top_p, and top_k sampling defaults", () => {
    const lowered = lowerOpenAIRequest(openAIRequest());

    expect(lowered.options.temperature).toBeUndefined();
    expect(lowered.options.topP).toBeUndefined();
    expect(lowered.options.topK).toBeUndefined();
  });

  test("L5 sanitizes attachment filenames, tool ids, and tool-name map keys", () => {
    const attachment = resolvedAttachment({
      transient: transientOrigin("att_surrogate"),
      mime: "application/pdf",
      filename: "report_\uD800.pdf",
      data: new Uint8Array([1]),
    });
    const lowered = lowerOpenAIRequest(openAIRequest({
      attachments: [attachment],
      context: [{
        role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
        content: [
          { toolCall: {
            modelToolCallId: "call_\uD800",
            name: "tool_\uDC00",
            inputJson: "{}",
          } },
          { toolResult: {
            modelToolCallId: "call_\uD800",
            completed: { outputJson: "{}" },
            error: undefined,
            cancelled: undefined,
          } },
        ],
      }],
      tools: [{
        name: "tool_\uDC00",
        description: "description",
        function: { inputSchemaJson: "{}", outputSchemaJson: undefined },
      }],
    }), { resolvedAttachments: [attachment] });

    expect(lowered.messages).toEqual(expect.arrayContaining([
      expect.objectContaining({ content: [expect.objectContaining({ toolCallId: "call_\uFFFD", toolName: "tool_\uFFFD" })] }),
      expect.objectContaining({ content: [expect.objectContaining({ filename: "report_\uFFFD.pdf" })] }),
    ]));
    expect(Object.keys(lowered.tools)).toEqual(["tool_\uFFFD"]);
  });

  test("openai-schema-lowering applies Codex-style schema lowering", () => {
    const lowered = lowerOpenAIRequest(openAIRequest({
      tools: [{
        name: "Search",
        description: "Search",
        function: { inputSchemaJson: JSON.stringify({
          properties: {
            enabled: true,
            mode: { const: "fast", title: "dropped" },
            maybe: { type: ["null", "string"] },
            scalarUnion: { type: ["null", "string", "number"] },
            emptyObject: { type: "object" },
            emptyArray: { type: "array" },
            inferredArray: { items: { const: "tag" } },
            count: { minimum: 0 },
            step: { multipleOf: 2 },
            url: { format: "uri" },
            refItem: { $ref: "#/$defs/Item", title: "dropped ref title" },
            legacyRef: { $ref: "#/definitions/Legacy" },
            choice: { anyOf: [{ const: "fast" }, { type: "number", multipleOf: 1 }] },
            legacyChoice: { oneOf: [{ type: "boolean" }, { const: "auto" }] },
            merged: { allOf: [{ properties: { id: { type: "string" } } }] },
          },
          $defs: {
            Item: {
              properties: { name: { type: "string", examples: ["dropped"] } },
            },
          },
          definitions: {
            Legacy: {
              format: "uuid",
            },
          },
          required: ["mode"],
          title: "dropped root",
        }), outputSchemaJson: JSON.stringify(false) },
      }],
    }));

    expect(lowered.tools.Search?.inputSchema?.schema).toEqual({
      properties: {
        enabled: { type: "string" },
        mode: { enum: ["fast"], type: "string" },
        maybe: { type: "string" },
        scalarUnion: { type: ["string", "number"] },
        emptyObject: { type: "object", properties: {} },
        emptyArray: { type: "array", items: { type: "string" } },
        inferredArray: { items: { enum: ["tag"], type: "string" }, type: "array" },
        count: { minimum: 0, type: "number" },
        step: { multipleOf: 2, type: "number" },
        url: { format: "uri", type: "string" },
        refItem: { $ref: "#/$defs/Item", type: "string" },
        legacyRef: { $ref: "#/definitions/Legacy", type: "string" },
        choice: { anyOf: [{ enum: ["fast"], type: "string" }, { type: "number", multipleOf: 1 }], type: "string" },
        legacyChoice: { oneOf: [{ type: "boolean" }, { enum: ["auto"], type: "string" }], type: "string" },
        merged: { allOf: [{ properties: { id: { type: "string" } }, type: "object" }], type: "string" },
      },
      $defs: {
        Item: {
          properties: { name: { type: "string" } },
          type: "object",
        },
      },
      definitions: {
        Legacy: {
          format: "uuid",
          type: "string",
        },
      },
      required: ["mode"],
      type: "object",
    });
    expect(lowered.tools.Search?.outputSchema?.schema).toEqual({ type: "string" });
  });

  test("pins the exact GPT exec_command provider schema and excludes Claude aliases", () => {
    const execSchema = {
      type: "object",
      additionalProperties: false,
      properties: {
        cmd: { type: "string" },
        workdir: { type: "string" },
        tty: { type: "boolean" },
        yield_time_ms: { type: "integer" },
        max_output_tokens: { type: "integer" },
        sandbox_permissions: { type: "string" },
        justification: { type: "string" },
        prefix_rule: { type: "array", items: { type: "string" } },
      },
      required: ["cmd"],
    };
    const lowered = lowerOpenAIRequest(openAIRequest({
      tools: [{ name: "exec_command", description: "Run a command", function: { inputSchemaJson: JSON.stringify(execSchema) } }],
    }));

    expect(lowered.tools.exec_command?.inputSchema).toEqual({ kind: "ai-sdk-json-schema", schema: execSchema });
    const wireSchema = JSON.stringify(lowered.tools.exec_command?.inputSchema);
    for (const forbidden of ["command", "timeout", "run_in_background"]) {
      expect(wireSchema).not.toContain(`\"${forbidden}\"`);
    }
  });

});

function lowerOpenAIRequest(
  request: ProviderRequest,
  options: { readonly modelOutputTokenLimit?: number; readonly resolvedAttachments?: readonly ResolvedProviderRequestAttachment[] } = {},
) {
  return lowerProviderRequest(request, OpenAIGPT55Rules, {
    modelOutputTokenLimit: options.modelOutputTokenLimit ?? 128_000,
    resolvedAttachments: options.resolvedAttachments,
  });
}

function openAIRequest(overrides: Partial<ProviderRequest> = {}): ProviderRequest {
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
    model: { providerId: "openai", modelId: "gpt-5.5", variant: "" },
    system: [],
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
