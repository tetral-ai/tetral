import { describe, expect, test } from "bun:test";
import { readdir, readFile } from "node:fs/promises";
import {
  ProviderRequestKind,
  RuntimeMessageRole,
  SystemCacheHint,
  SystemSegmentKind,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { ProviderRequest, RuntimeMessage } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { ProviderRequestLoweringError, classifyProviderStreamError } from "../../src/errors.js";
import { lowerProviderRequest, remapOpenAICompatibleMessageMetadataForSDK, type ResolvedProviderRequestAttachment } from "../../src/request.js";
import { DeepSeekV4ProRules } from "../../src/rules/deepseek.js";

const deepSeekFixtureRoot = new URL("../fixtures/deepseek/", import.meta.url);
const deepSeekFixtures = await loadDeepSeekFixtures();

describe("deepseek request lowering", () => {
  test("approval reviewer selects JSON-object wire enforcement", () => {
    const schema = { type: "object", properties: { outcome: { type: "string" } } };
    const lowered = lowerDeepSeekRequest(deepSeekRequest({
      requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
      outputSchemaJson: JSON.stringify(schema),
    }));

    expect(DeepSeekV4ProRules.structuredOutputStrategy).toBe("json_object");
    expect(lowered.structuredOutput).toEqual({
      strategy: "json_object",
      schema: { kind: "ai-sdk-json-schema", schema },
    });
    expect(lowerDeepSeekRequest(deepSeekRequest()).structuredOutput).toBeUndefined();
  });

  test("DeepSeek lowering fixtures exist and load", () => {
    expect(deepSeekFixtures.length).toBeGreaterThan(0);
  });

  test("deepseek-reasoning-default fixtures cover reasoning_content append rules", () => {
    expect(deepSeekFixtures.filter((fixture) => fixture.rule === "deepseek-reasoning-default").length).toBeGreaterThan(0);
  });

  test("deepseek-effort-lowering fixtures cover effort mapping rules", () => {
    expect(deepSeekFixtures.filter((fixture) => fixture.rule === "deepseek-effort-lowering").length).toBeGreaterThan(0);
  });

  for (const fixture of deepSeekFixtures) {
    test(`${fixture.rule} fixture ${fixture.name}`, () => {
      const projected = projectLoweredDeepSeek(fixture.input);
      for (const key of Object.keys(fixture.expected) as Array<keyof LoweredProjection>) {
        expect(projected[key]).toEqual(fixture.expected[key]);
      }
    });
  }

  test("deepseek-unsupported-media renders resolved unsupported media as a model-visible error text part", () => {
    expect(DeepSeekV4ProRules.supportedMedia).toEqual({
      exactMimes: [],
      mimePrefixes: [],
    });

    let caught: unknown;
    try {
      lowerDeepSeekRequest(deepSeekRequest({
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
    const lowered = lowerDeepSeekRequest(deepSeekRequest({ attachments: [attachment] }), { resolvedAttachments: [attachment] });
    expect(lowered.messages.at(-1)).toEqual({
      role: "user",
      content: [{
        type: "text",
        text: "ERROR: cannot read attachment image.png (image/png) from /tmp/image.png: media is not supported by deepseek/deepseek-v4-pro.",
      }],
    });
  });

  test("deepseek-unsupported-media never treats an empty plain-text attachment as unsupported media", () => {
    const attachment = resolvedAttachment({
      transient: undefined,
      fileBacked: { sourceEventId: "sevt_text", fileId: "file_text" },
      mime: "text/plain",
      filename: "empty.txt",
      data: new Uint8Array(),
    });
    const lowered = lowerDeepSeekRequest(deepSeekRequest({ attachments: [attachment] }), { resolvedAttachments: [attachment] });

    expect(lowered.messages.at(-1)).toEqual({
      role: "user",
      content: [{
        type: "text",
        text: "[attachment: empty.txt (text/plain)]\n",
      }],
    });
    expect(JSON.stringify(lowered)).not.toContain("ERROR: cannot read attachment");
  });

  test("deepseek-provider-options uses the stable deepseek providerOptions key", () => {
    const lowered = lowerDeepSeekRequest(deepSeekRequest({
      messages: [{
        id: "msg_assistant",
        role: RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_ASSISTANT,
        status: "completed",
        origin: "agent",
        parts: [{ id: "t1", text: { text: "Paris." } }],
      }],
    }));

    expect(lowered.messages[0]).toMatchObject({
      role: "assistant",
      providerOptions: { deepseek: { reasoning_content: "" } },
    });
    expect(lowered.options.providerOptions).toEqual({
      deepseek: { reasoningEffort: "high" },
    });
  });

  test("deepseek-reasoning-remap remaps only per-message reasoning metadata to the SDK openaiCompatible key", () => {
    const lowered = lowerDeepSeekRequest(deepSeekRequest({
      model: { providerId: "deepseek", modelId: "deepseek-v4-pro", variant: "high" },
      messages: [{
        id: "msg_assistant",
        role: RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_ASSISTANT,
        status: "completed",
        origin: "agent",
        parts: [
          { id: "r1", reasoning: { text: "hidden", metadataJson: "{}" } },
          { id: "t1", text: { text: "visible" } },
        ],
      }],
    }));
    const sdkMessages = remapOpenAICompatibleMessageMetadataForSDK(lowered.messages, DeepSeekV4ProRules);

    expect(lowered.messages).toEqual([{
      role: "assistant",
      content: [{ type: "text", text: "visible" }],
      providerOptions: { deepseek: { reasoning_content: "hidden" } },
    }]);
    expect(sdkMessages).toEqual([{
      role: "assistant",
      content: [{ type: "text", text: "visible" }],
      providerOptions: { openaiCompatible: { reasoning_content: "hidden" } },
    }]);
    expect(lowered.options.providerOptions).toEqual({
      deepseek: { reasoningEffort: "high" },
    });
  });

  test("deepseek-cache-absence emits no DeepSeek cache-control metadata", () => {
    const lowered = lowerDeepSeekRequest(deepSeekRequest({
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
  });

  test("deepseek-sampling-defaults omits temperature, top_p, and top_k sampling defaults", () => {
    const lowered = lowerDeepSeekRequest(deepSeekRequest());

    expect(lowered.options.temperature).toBeUndefined();
    expect(lowered.options.topP).toBeUndefined();
    expect(lowered.options.topK).toBeUndefined();
  });

  test("deepseek-schema-passthrough passes DeepSeek tool schemas through without provider surgery", () => {
    const inputSchema = { type: "object", properties: { path: { type: "string" } }, required: ["path"] };
    const lowered = lowerDeepSeekRequest(deepSeekRequest({
      tools: [{
        name: "Read",
        description: "Read a file",
        function: {
          inputSchemaJson: JSON.stringify(inputSchema),
          outputSchemaJson: JSON.stringify({ type: "object", properties: { text: { type: "string" } } }),
        },
      }],
    }));

    expect(lowered.tools.Read).toEqual({
      kind: "function",
      description: "Read a file",
      inputSchema: { kind: "ai-sdk-json-schema", schema: inputSchema },
      outputSchema: { kind: "ai-sdk-json-schema", schema: { type: "object", properties: { text: { type: "string" } } } },
    });
  });

  test("deepseek-error-mapping leaves provider-specific error overrides empty beyond generic classification", () => {
    expect(DeepSeekV4ProRules.providerSpecificErrorRules).toEqual([]);
  });

  test("deepseek-output-limit sends the catalog's documented model output limit when the request sets none", () => {
    const unset = lowerDeepSeekRequest(deepSeekRequest({ limits: { maxOutputTokens: 0, timeoutMs: 30_000 } }), 384_000);

    expect(unset.options.maxOutputTokens).toBe(384_000);
  });
});

interface DeepSeekFixture {
  readonly name: string;
  readonly rule: string;
  readonly input: DeepSeekFixtureInput;
  readonly expected: Partial<LoweredProjection>;
}

interface DeepSeekFixtureInput {
  readonly model: {
    readonly provider_id: string;
    readonly model_id: string;
    readonly variant?: string;
  };
  readonly messages?: readonly DeepSeekFixtureMessage[];
}

interface DeepSeekFixtureMessage {
  readonly role: "assistant" | "user";
  readonly parts: readonly DeepSeekFixturePart[];
}

type DeepSeekFixturePart =
  | { readonly type: "text"; readonly id: string; readonly text: string }
  | { readonly type: "reasoning"; readonly id: string; readonly text: string };

interface LoweredProjection {
  readonly messages: unknown;
  readonly providerOptions: unknown;
  readonly temperature: unknown;
  readonly topP: unknown;
  readonly topK: unknown;
  readonly tools: unknown;
}

async function loadDeepSeekFixtures(): Promise<readonly DeepSeekFixture[]> {
  const entries = await readdir(deepSeekFixtureRoot, { withFileTypes: true });
  const fixtures: DeepSeekFixture[] = [];
  for (const entry of entries.sort((left, right) => left.name.localeCompare(right.name))) {
    if (!entry.isFile() || !entry.name.endsWith(".json")) {
      continue;
    }
    fixtures.push(JSON.parse(await readFile(new URL(entry.name, deepSeekFixtureRoot), "utf8")) as DeepSeekFixture);
  }
  return fixtures;
}

function projectLoweredDeepSeek(input: DeepSeekFixtureInput): LoweredProjection {
  const lowered = lowerDeepSeekRequest(deepSeekRequest({
    model: {
      providerId: input.model.provider_id,
      modelId: input.model.model_id,
      variant: input.model.variant ?? "",
    },
    messages: (input.messages ?? []).map(fixtureMessageToRuntime),
  }));
  return {
    messages: lowered.messages,
    providerOptions: lowered.options.providerOptions,
    temperature: lowered.options.temperature,
    topP: lowered.options.topP,
    topK: lowered.options.topK,
    tools: lowered.tools,
  };
}

function fixtureMessageToRuntime(message: DeepSeekFixtureMessage, index: number): RuntimeMessage {
  return {
    id: `fixture_msg_${index}`,
    role: message.role === "assistant"
      ? RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_ASSISTANT
      : RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_USER,
    status: "completed",
    origin: message.role === "assistant" ? "agent" : "user",
    parts: message.parts.map((part) => {
      switch (part.type) {
        case "text":
          return { id: part.id, text: { text: part.text } };
        case "reasoning":
          return { id: part.id, reasoning: { text: part.text, metadataJson: "{}" } };
      }
    }),
  };
}

function lowerDeepSeekRequest(
  request: ProviderRequest,
  options: number | { readonly modelOutputTokenLimit?: number; readonly resolvedAttachments?: readonly ResolvedProviderRequestAttachment[] } = 128_000,
) {
  const normalized = typeof options === "number" ? { modelOutputTokenLimit: options } : options;
  return lowerProviderRequest(request, DeepSeekV4ProRules, {
    modelOutputTokenLimit: normalized.modelOutputTokenLimit ?? 128_000,
    resolvedAttachments: normalized.resolvedAttachments,
  });
}

function deepSeekRequest(overrides: Partial<ProviderRequest> = {}): ProviderRequest {
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
    model: { providerId: "deepseek", modelId: "deepseek-v4-pro", variant: "" },
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
