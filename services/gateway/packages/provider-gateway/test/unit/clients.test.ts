import { describe, expect, jest, test } from "bun:test";
import { readFile } from "node:fs/promises";
import {
  ProviderFinishReason,
  ProviderRequestKind,
  ProviderStreamEventType,
  ProviderContextRole,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { ProviderKeyFailureError } from "../../src/providers/pool.js";
import { ProviderClientRegistry } from "../../src/providers/clients.js";
import {
  OpenAICodexOAuthClientId,
  OpenAICodexOAuthIssuer,
  OpenAICodexOAuthTokenEndpoint,
  OpenAICodexResponsesEndpoint,
  OpenAIOAuthDummyAPIKey,
} from "../../src/providers/openai-oauth.js";
import { validProviderRequest } from "./fixtures.js";
import type { TextStreamPart, ToolSet } from "ai";
import type { FetchFunction } from "@ai-sdk/provider-utils";
import type { ProviderRequest, ProviderStreamEvent } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { ResolvedProviderRequestAttachment } from "@tetral/gateway-lowering/src/request.js";
import type {
  AnthropicProviderSettings,
  GatewayStreamTextInput,
  GatewayStreamTextResult,
  OpenAIProviderSettings,
  OpenAICompatibleProviderSettings,
} from "../../src/providers/clients.js";
import type { ResolvedProviderCredential } from "../../src/providers/credentials.js";

const approvalReviewerOutputSchemaJson = await readFile(
  new URL("../../../../../agent-runtime/packages/runtime-pod/src/assets/approval-reviewer-output-schema.json", import.meta.url),
  "utf8",
);

describe("ProviderClientRegistry provider streaming", () => {
  test("forwards DeepSeek reviewer JSON-object strategy into provider construction", async () => {
    const calls: GatewayStreamTextInput[] = [];
    const providerSettings: OpenAICompatibleProviderSettings[] = [];
    const registry = new ProviderClientRegistry({
      openAICompatibleProviderFactory: (settings) => {
        providerSettings.push(settings);
        return (modelId) => ({ provider: "deepseek", modelId });
      },
      streamText: (input) => {
        calls.push(input);
        return streamTextResult([finishPart()]);
      },
    });
    const request = deepSeekRequest({
      requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
      outputSchemaJson: approvalReviewerOutputSchemaJson,
    });

    await collectEvents(registry.stream({ request, credential: sessionDeepSeekCredential() }));

    expect(providerSettings).toHaveLength(1);
    expect(providerSettings[0]?.supportsStructuredOutputs).toBe(false);
    expect(calls).toHaveLength(1);
    expect(await calls[0]?.output?.responseFormat).toEqual({
      type: "json",
      schema: JSON.parse(approvalReviewerOutputSchemaJson),
    });
  });

  test("rejects unsupported reviewer routes before provider construction", async () => {
    let providerFactoryCalls = 0;
    let streamCalls = 0;
    const registry = new ProviderClientRegistry({
      openAIProviderFactory: () => {
        providerFactoryCalls += 1;
        return { responses: (modelId) => ({ provider: "openai", modelId }) };
      },
      streamText: () => {
        streamCalls += 1;
        return streamTextResult([finishPart()]);
      },
    });
    const request = openAIRequest({
      requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
      outputSchemaJson: approvalReviewerOutputSchemaJson,
    });

    await expect(
      collectEvents(registry.stream({ request, credential: sessionOpenAICredential() })),
    ).rejects.toMatchObject({
      providerError: {
        code: "provider_configuration_invalid",
        retryable: false,
        fatal: true,
      },
    });
    expect(providerFactoryCalls).toBe(0);
    expect(streamCalls).toBe(0);
  });

  test("rejects an unpaired Tool Result at the provider client boundary", async () => {
    let providerFactoryCalls = 0;
    let streamCalls = 0;
    const registry = new ProviderClientRegistry({
      anthropicProviderFactory: () => {
        providerFactoryCalls += 1;
        return (modelId) => ({ provider: "anthropic", modelId });
      },
      streamText: () => {
        streamCalls += 1;
        return streamTextResult([finishPart()]);
      },
    });
    const request = anthropicRequest({
      context: [
        {
          role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
          content: [
            {
              toolResult: {
                modelToolCallId: "call_without_owner",
                completed: { outputJson: "{}" },
                error: undefined,
                cancelled: undefined,
              },
            },
          ],
        },
      ],
    });

    let caught: unknown;
    try {
      await collectEvents(
        registry.stream({ request, credential: platformAnthropicCredential() }),
      );
    } catch (error) {
      caught = error;
    }

    expect(caught).toMatchObject({
      providerError: {
        code: "provider_request_invalid",
        retryable: false,
        fatal: true,
        statusCode: 400,
      },
    });
    expect(providerFactoryCalls).toBe(0);
    expect(streamCalls).toBe(0);
  });

  test("lowers approval reviewer output schema and reports route-effective model limits", async () => {
    const calls: GatewayStreamTextInput[] = [];
    const request = anthropicRequest({
      requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
      outputSchemaJson: approvalReviewerOutputSchemaJson,
    });
    const registry = new ProviderClientRegistry({
      anthropicProviderFactory: () => (modelId) => ({ provider: "anthropic", modelId }),
      streamText: (input) => {
        calls.push(input);
        return streamTextResult([finishPart()]);
      },
    });

    const events = await collectEvents(registry.stream({ request, credential: platformAnthropicCredential() }));

    expect(calls).toHaveLength(1);
    expect(await calls[0]?.output?.responseFormat).toEqual({
      type: "json",
      schema: JSON.parse(approvalReviewerOutputSchemaJson),
    });
    expect(events.at(-1)).toMatchObject({
      type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
      finish: {
        contextWindowTokens: 1_000_000,
        inputLimitTokens: undefined,
        outputTokenLimit: 128_000,
      },
    });
  });

  test("streams every non-reviewer request kind as plain text", async () => {
    const calls: GatewayStreamTextInput[] = [];
    const registry = new ProviderClientRegistry({
      anthropicProviderFactory: () => (modelId) => ({ provider: "anthropic", modelId }),
      streamText: (input) => {
        calls.push(input);
        return streamTextResult([finishPart()]);
      },
    });

    for (const requestKind of [
      ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
      ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY,
      ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER_COMPACTION,
    ]) {
      await collectEvents(registry.stream({ request: anthropicRequest({ requestKind, outputSchemaJson: undefined }), credential: platformAnthropicCredential() }));
    }

    expect(calls).toHaveLength(3);
    expect(calls.every((call) => call.output === undefined)).toBe(true);
  });

  test("lowers Anthropic requests into the pinned AI SDK v6 streamText shape and raises provider events", async () => {
    const calls: GatewayStreamTextInput[] = [];
    const providerSettings: AnthropicProviderSettings[] = [];
    const request = anthropicRequest({
      model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "xhigh" },
      limits: { maxOutputTokens: 200_000, timeoutMs: 30_000 },
      attachments: [],
    });
    const registry = new ProviderClientRegistry({
      anthropicProviderFactory: (settings) => {
        providerSettings.push(settings);
        return (modelId) => ({ provider: "anthropic", modelId });
      },
      streamText: (input) => {
        calls.push(input);
        return streamTextResult([
          { type: "start" },
          { type: "text-start", id: "txt_1", providerMetadata: { anthropic: { request_id: "reqp_1" } } },
          { type: "text-delta", id: "txt_1", text: "hello" },
          { type: "text-end", id: "txt_1" },
          {
            type: "finish",
            finishReason: "stop",
            rawFinishReason: "end_turn",
            totalUsage: {
              inputTokens: 10,
              outputTokens: 4,
              inputTokenDetails: { noCacheTokens: 7, cacheReadTokens: 3, cacheWriteTokens: 2 },
              outputTokenDetails: { textTokens: 4, reasoningTokens: undefined },
              totalTokens: 19,
            },
          },
        ]);
      },
    });

    const events = await collectEvents(registry.stream({ request, credential: sessionAnthropicCredential() }));

    expect(providerSettings).toEqual([{
      apiKey: "sk-session-anthropic",
      baseURL: "https://api.anthropic.com/v1",
      headers: {
        "anthropic-beta": "interleaved-thinking-2025-05-14,fine-grained-tool-streaming-2025-05-14",
      },
      fetch: expect.any(Function),
    }]);
    expect(calls).toHaveLength(1);
    expect(calls[0]?.onError).toEqual(expect.any(Function));
    expect(calls[0]).toMatchObject({
      model: { provider: "anthropic", modelId: "claude-opus-4-8" },
      maxOutputTokens: 128_000,
      maxRetries: 0,
      headers: {
        "anthropic-beta": "interleaved-thinking-2025-05-14,fine-grained-tool-streaming-2025-05-14",
      },
      providerOptions: {
        anthropic: {
          thinking: { type: "adaptive", display: "summarized" },
          sendReasoning: true,
          effort: "xhigh",
        },
      },
    });
    expect(calls[0]?.messages).toMatchObject([
      {
        role: "system",
        content: "You are concise.",
        providerOptions: { anthropic: { cacheControl: { type: "ephemeral" } } },
      },
      {
        role: "user",
        content: [{ type: "text", text: "hello" }],
        providerOptions: { anthropic: { cacheControl: { type: "ephemeral" } } },
      },
    ]);
    expect(calls[0]?.tools?.Read?.description).toBe("Read a file.");

    expect(events.map((event) => event.type)).toEqual([
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START,
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA,
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_END,
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
    ]);
    expect(events[1]?.text?.text).toBe("hello");
    expect(events[3]?.finish).toMatchObject({
      reason: ProviderFinishReason.PROVIDER_FINISH_REASON_STOP,
      contextWindowTokens: 1_000_000,
      inputLimitTokens: undefined,
      outputTokenLimit: 128_000,
      usage: {
        inputUncachedTokens: 7,
        inputCacheReadTokens: 3,
        inputCacheWriteTokens: 2,
        inputTotalTokens: 12,
        outputTotalTokens: 4,
        totalTokens: 16,
      },
      metadataJson: JSON.stringify({
        credential_source: "session",
        raw_finish_reason: "end_turn",
      }),
    });
  });

  test("streams current-stage empty ModelRef.variant with each model's base effort behavior", async () => {
    const anthropicCalls: GatewayStreamTextInput[] = [];
    await collectEvents(new ProviderClientRegistry({
      anthropicProviderFactory: () => (modelId) => ({ provider: "anthropic", modelId }),
      streamText: (input) => {
        anthropicCalls.push(input);
        return streamTextResult([finishPart()]);
      },
    }).stream({ request: anthropicRequest(), credential: sessionAnthropicCredential() }));
    expect(anthropicCalls[0]?.providerOptions?.anthropic).toEqual({
      thinking: { type: "adaptive", display: "summarized" },
      effort: "xhigh",
      sendReasoning: true,
    });

    const openAICalls: GatewayStreamTextInput[] = [];
    await collectEvents(new ProviderClientRegistry({
      openAIProviderFactory: () => ({ responses: (modelId) => ({ provider: "openai", modelId }) }),
      streamText: (input) => {
        openAICalls.push(input);
        return streamTextResult([finishPart()]);
      },
    }).stream({ request: openAIRequest(), credential: sessionOpenAICredential() }));
    expect(openAICalls[0]?.providerOptions?.openai).toMatchObject({
      reasoningEffort: "medium",
      reasoningSummary: "auto",
      include: ["reasoning.encrypted_content"],
    });

    const deepSeekCalls: GatewayStreamTextInput[] = [];
    await collectEvents(new ProviderClientRegistry({
      openAICompatibleProviderFactory: () => (modelId) => ({ provider: "deepseek", modelId }),
      streamText: (input) => {
        deepSeekCalls.push(input);
        return streamTextResult([finishPart()]);
      },
    }).stream({ request: deepSeekRequest(), credential: sessionDeepSeekCredential() }));
    expect(deepSeekCalls[0]?.providerOptions?.deepseek).toEqual({
      reasoningEffort: "high",
    });

    const kimiCalls: GatewayStreamTextInput[] = [];
    await collectEvents(new ProviderClientRegistry({
      anthropicProviderFactory: () => (modelId) => ({ provider: "moonshotai", modelId }),
      streamText: (input) => {
        kimiCalls.push(input);
        return streamTextResult([finishPart()]);
      },
    }).stream({ request: kimiRequest(), credential: sessionKimiCredential() }));
    expect(JSON.stringify(kimiCalls[0]?.providerOptions ?? {})).not.toContain("\"effort\"");

    const zaiCalls: GatewayStreamTextInput[] = [];
    await collectEvents(new ProviderClientRegistry({
      openAICompatibleProviderFactory: () => (modelId) => ({ provider: "zai", modelId }),
      streamText: (input) => {
        zaiCalls.push(input);
        return streamTextResult([finishPart()]);
      },
    }).stream({ request: zaiRequest(), credential: sessionZaiCredential() }));
    expect(zaiCalls[0]?.providerOptions?.zai).toEqual({
      thinking: { type: "enabled", clear_thinking: false },
      reasoningEffort: "high",
    });
  });

  test("passes resolved image and PDF attachments to AI SDK user content parts", async () => {
    const calls: GatewayStreamTextInput[] = [];
    const attachments = [
      resolvedAttachment({ transient: transientOrigin("att_image"), mime: "image/png", filename: "plot.png", data: new Uint8Array([1, 2]) }),
      resolvedAttachment({ transient: transientOrigin("att_pdf"), mime: "application/pdf", filename: "report.pdf", data: new Uint8Array([3, 4]) }),
    ];
    const request = anthropicRequest({
      model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" },
      attachments,
    });
    const registry = new ProviderClientRegistry({
      anthropicProviderFactory: () => (modelId) => ({ provider: "anthropic", modelId }),
      streamText: (input) => {
        calls.push(input);
        return streamTextResult([finishPart()]);
      },
    });

    await collectEvents(registry.stream({ request, credential: sessionAnthropicCredential(), resolvedAttachments: attachments }));

    expect(calls[0]?.messages.at(-1)).toEqual({
      role: "user",
      content: [
        { type: "image", image: new Uint8Array([1, 2]), mediaType: "image/png" },
        { type: "file", data: new Uint8Array([3, 4]), filename: "report.pdf", mediaType: "application/pdf" },
      ],
      providerOptions: { anthropic: { cacheControl: { type: "ephemeral" } } },
    });
  });

  test("uses platform Anthropic keys without exposing key identity in finish metadata", async () => {
    const request = anthropicRequest({ attachments: [] });
    const registry = new ProviderClientRegistry({
      anthropicProviderFactory: () => () => ({}),
      streamText: () => streamTextResult([{
        type: "finish",
        finishReason: "stop",
        rawFinishReason: undefined,
        totalUsage: {
          inputTokens: 1,
          inputTokenDetails: { noCacheTokens: 1, cacheReadTokens: undefined, cacheWriteTokens: undefined },
          outputTokens: 1,
          outputTokenDetails: { textTokens: 1, reasoningTokens: undefined },
          totalTokens: 2,
        },
      }]),
    });

    const events = await collectEvents(registry.stream({ request, credential: platformAnthropicCredential() }));

    expect(events).toHaveLength(1);
    expect(events[0]?.finish?.metadataJson).toBe(JSON.stringify({ credential_source: "platform" }));
    expect(events[0]?.finish?.metadataJson).not.toContain("pfk_1");
  });

  test("drops AI SDK stream lifecycle markers before raising provider events", async () => {
    const request = anthropicRequest({ attachments: [] });
    const registry = new ProviderClientRegistry({
      anthropicProviderFactory: () => () => ({}),
      streamText: () => streamTextResult([
        { type: "start" } as TextStreamPart<ToolSet>,
        { type: "start-step" } as TextStreamPart<ToolSet>,
        {
          type: "finish-step",
          usage: { inputTokens: 1, outputTokens: 1, providerOnlyCounter: 9 },
        } as unknown as TextStreamPart<ToolSet>,
        finishPart(),
      ]),
    });

    const events = await collectEvents(registry.stream({ request, credential: platformAnthropicCredential() }));

    expect(events.map((event) => event.type)).toEqual([
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
    ]);
    expect(events[0]?.finish?.usage?.providerUsageJson).toContain("providerOnlyCounter");
  });

  test("preserves Anthropic beta headers assembled by the AI SDK", async () => {
    const providerSettings: AnthropicProviderSettings[] = [];
    const observedHeaders: Record<string, string | undefined>[] = [];
    const registry = new ProviderClientRegistry({
      fetch: Object.assign(async (input: Parameters<FetchFunction>[0], init?: Parameters<FetchFunction>[1]) => {
        const request = new Request(input, init);
        observedHeaders.push({ "anthropic-beta": request.headers.get("anthropic-beta") ?? undefined });
        return new Response("{}");
      }, { preconnect: () => {} }) satisfies FetchFunction,
      anthropicProviderFactory: (settings) => {
        providerSettings.push(settings);
        return () => ({});
      },
      streamText: () => streamTextResult([{
        type: "finish",
        finishReason: "stop",
        rawFinishReason: undefined,
        totalUsage: {
          inputTokens: 1,
          inputTokenDetails: { noCacheTokens: 1, cacheReadTokens: undefined, cacheWriteTokens: undefined },
          outputTokens: 1,
          outputTokenDetails: { textTokens: 1, reasoningTokens: undefined },
          totalTokens: 2,
        },
      }]),
    });

    await collectEvents(registry.stream({ request: anthropicRequest({ attachments: [] }), credential: sessionAnthropicCredential() }));
    await providerSettings[0]?.fetch?.("https://api.anthropic.com/v1/messages", {
      headers: {
        "anthropic-beta": "structured-outputs-2025-11-13,interleaved-thinking-2025-05-14,fine-grained-tool-streaming-2025-05-14",
      },
    });

    expect(observedHeaders).toEqual([{
      "anthropic-beta": "structured-outputs-2025-11-13,interleaved-thinking-2025-05-14,fine-grained-tool-streaming-2025-05-14",
    }]);
  });

  test("provider custom fetch enforces provider catalog egress before network", async () => {
    const providerSettings: AnthropicProviderSettings[] = [];
    const observedURLs: string[] = [];
    const registry = new ProviderClientRegistry({
      fetch: Object.assign(async (input: Parameters<FetchFunction>[0], init?: Parameters<FetchFunction>[1]) => {
        const request = new Request(input, init);
        observedURLs.push(request.url);
        return new Response("{}");
      }, { preconnect: () => {} }) satisfies FetchFunction,
      anthropicProviderFactory: (settings) => {
        providerSettings.push(settings);
        return () => ({});
      },
      streamText: () => streamTextResult([finishPart()]),
    });

    await collectEvents(registry.stream({ request: anthropicRequest({ attachments: [] }), credential: sessionAnthropicCredential() }));
    await providerSettings[0]?.fetch?.("https://api.anthropic.com/v1/messages");
    const deniedOAuthHost = await caughtError(providerSettings[0]?.fetch?.("https://chatgpt.com/backend-api/codex/responses"));
    const denied = await caughtError(providerSettings[0]?.fetch?.("https://example.com/v1/messages"));

    expect(deniedOAuthHost).toBeInstanceOf(TypeError);
    expect(String((deniedOAuthHost as Error).message)).toContain("not allowlisted");
    expect(denied).toBeInstanceOf(TypeError);
    expect(String((denied as Error).message)).toContain("not allowlisted");
    expect(observedURLs).toEqual(["https://api.anthropic.com/v1/messages"]);
  });

  test("provider custom fetch enforces mode-specific and path-root egress", async () => {
    const openAISettings: OpenAIProviderSettings[] = [];
    const kimiSettings: AnthropicProviderSettings[] = [];
    const observedURLs: string[] = [];
    const registry = new ProviderClientRegistry({
      fetch: Object.assign(async (input: Parameters<FetchFunction>[0], init?: Parameters<FetchFunction>[1]) => {
        const request = new Request(input, init);
        observedURLs.push(request.url);
        return new Response("{}");
      }, { preconnect: () => {} }) satisfies FetchFunction,
      openAIProviderFactory: (settings) => {
        openAISettings.push(settings);
        return { responses: (modelId) => ({ provider: "openai", modelId }) };
      },
      anthropicProviderFactory: (settings) => {
        kimiSettings.push(settings);
        return (modelId) => ({ provider: "anthropic", modelId });
      },
      streamText: () => streamTextResult([finishPart()]),
    });

    await collectEvents(registry.stream({ request: openAIRequest(), credential: sessionOpenAICredential() }));
    await collectEvents(registry.stream({ request: kimiRequest({ attachments: [] }), credential: sessionKimiCredential() }));

    await openAISettings[0]?.fetch?.("https://api.openai.com/v1/responses");
    const deniedChatGPT = await caughtError(openAISettings[0]?.fetch?.(OpenAICodexResponsesEndpoint));
    await kimiSettings[0]?.fetch?.("https://api.kimi.com/coding/v1/messages");
    const deniedSameHostWrongPath = await caughtError(kimiSettings[0]?.fetch?.("https://api.kimi.com/v1/messages"));

    expect(deniedChatGPT).toBeInstanceOf(TypeError);
    expect(String((deniedChatGPT as Error).message)).toContain("not allowlisted");
    expect(deniedSameHostWrongPath).toBeInstanceOf(TypeError);
    expect(String((deniedSameHostWrongPath as Error).message)).toContain("not allowlisted");
    expect(observedURLs).toEqual([
      "https://api.openai.com/v1/responses",
      "https://api.kimi.com/coding/v1/messages",
    ]);
  });

  test("provider custom fetch denies OAuth hosts for non-OpenAI providers before network", async () => {
    const anthropicProviderSettings: AnthropicProviderSettings[] = [];
    const compatibleProviderSettings: OpenAICompatibleProviderSettings[] = [];
    const observedURLs: string[] = [];
    const registry = new ProviderClientRegistry({
      fetch: Object.assign(async (input: Parameters<FetchFunction>[0], init?: Parameters<FetchFunction>[1]) => {
        const request = new Request(input, init);
        observedURLs.push(request.url);
        return new Response("{}");
      }, { preconnect: () => {} }) satisfies FetchFunction,
      anthropicProviderFactory: (settings) => {
        anthropicProviderSettings.push(settings);
        return () => ({});
      },
      openAICompatibleProviderFactory: (settings) => {
        compatibleProviderSettings.push(settings);
        return () => ({});
      },
      streamText: () => streamTextResult([finishPart()]),
    });

    await collectEvents(registry.stream({ request: anthropicRequest({ attachments: [] }), credential: sessionAnthropicCredential() }));
    await collectEvents(registry.stream({ request: kimiRequest({ attachments: [] }), credential: sessionKimiCredential() }));
    await collectEvents(registry.stream({ request: deepSeekRequest({ attachments: [] }), credential: sessionDeepSeekCredential() }));
    await collectEvents(registry.stream({ request: zaiRequest({ attachments: [] }), credential: sessionZaiCredential() }));

    const providerFetches = [
      anthropicProviderSettings[0]?.fetch,
      anthropicProviderSettings[1]?.fetch,
      compatibleProviderSettings[0]?.fetch,
      compatibleProviderSettings[1]?.fetch,
    ];
    expect(providerFetches).toHaveLength(4);
    for (const providerFetch of providerFetches) {
      if (providerFetch === undefined) {
        throw new Error("missing provider fetch");
      }
      for (const url of [
        "https://chatgpt.com/backend-api/codex/responses",
        "https://auth.openai.com/oauth/token",
      ]) {
        const denied = await caughtError(providerFetch(url));
        expect(denied).toBeInstanceOf(TypeError);
        expect(String((denied as Error).message)).toContain("not allowlisted");
      }
    }
    expect(observedURLs).toEqual([]);
  });

  test("provider custom fetch validates redirect targets before following", async () => {
    const providerSettings: AnthropicProviderSettings[] = [];
    const observedURLs: string[] = [];
    const registry = new ProviderClientRegistry({
      fetch: Object.assign(async (input: Parameters<FetchFunction>[0]) => {
        const request = new Request(input);
        observedURLs.push(request.url);
        if (request.url === "https://api.anthropic.com/v1/messages") {
          return new Response(null, {
            status: 302,
            headers: { location: "https://auth.openai.com/oauth/token" },
          });
        }
        return new Response("{}");
      }, { preconnect: () => {} }) satisfies FetchFunction,
      anthropicProviderFactory: (settings) => {
        providerSettings.push(settings);
        return () => ({});
      },
      streamText: () => streamTextResult([finishPart()]),
    });

    await collectEvents(registry.stream({ request: anthropicRequest({ attachments: [] }), credential: sessionAnthropicCredential() }));
    const denied = await caughtError(providerSettings[0]?.fetch?.("https://api.anthropic.com/v1/messages"));

    expect(denied).toBeInstanceOf(TypeError);
    expect(String((denied as Error).message)).toContain("not allowlisted");
    expect(observedURLs).toEqual(["https://api.anthropic.com/v1/messages"]);
  });

  test("provider redirects strip credentials when an allowlisted target changes origin", async () => {
    const requests = await openAIOAuthRedirect(307, "https://api.openai.com/v1/responses");

    expect(requests.map(({ url, authorization, cookie }) => ({ url, authorization, cookie }))).toEqual([
      { url: OpenAICodexResponsesEndpoint, authorization: "Bearer oauth-access", cookie: "session=secret" },
      { url: "https://api.openai.com/v1/responses", authorization: null, cookie: null },
    ]);
    expect(requests.map(({ method, body }) => ({ method, body }))).toEqual([
      { method: "POST", body: RedirectRequestBody },
      { method: "POST", body: RedirectRequestBody },
    ]);
  });

  test("same-origin 307 and 308 redirects preserve headers and streamed POST bodies", async () => {
    for (const status of [307, 308]) {
      const target = `${OpenAICodexResponsesEndpoint}?redirect=${status}`;
      const requests = await openAIOAuthRedirect(status, target);

      expect(requests.map(({ url, method, authorization, cookie, body }) => ({ url, method, authorization, cookie, body }))).toEqual([
        { url: OpenAICodexResponsesEndpoint, method: "POST", authorization: "Bearer oauth-access", cookie: "session=secret", body: RedirectRequestBody },
        { url: target, method: "POST", authorization: "Bearer oauth-access", cookie: "session=secret", body: RedirectRequestBody },
      ]);
    }
  });

  test("301 302 and 303 redirects rebuild POST requests as bodyless GETs", async () => {
    for (const status of [301, 302, 303]) {
      const requests = await openAIOAuthRedirect(status, `${OpenAICodexResponsesEndpoint}?redirect=${status}`);
      expect(requests[1]).toMatchObject({
        method: "GET",
        body: "",
        contentLength: null,
        contentType: null,
        contentEncoding: null,
        contentLanguage: null,
        contentLocation: null,
      });
    }
  });

  test("provider custom fetch leaves first-response liveness to the normalized service boundary", async () => {
    const providerSettings: AnthropicProviderSettings[] = [];
    jest.useFakeTimers();
    try {
      const registry = new ProviderClientRegistry({
        providerFetchTimeouts: { interChunkTimeoutMs: 100 },
        fetch: Object.assign(async (input: RequestInfo | URL, init?: RequestInit) => {
          const request = new Request(input, init);
          await new Promise<void>((resolve, reject) => {
            const timer = setTimeout(resolve, 10_001);
            request.signal.addEventListener("abort", () => {
              clearTimeout(timer);
              reject(request.signal.reason);
            }, { once: true });
          });
          return new Response(new ReadableStream<Uint8Array>({
            async start(controller) {
              controller.enqueue(new TextEncoder().encode("ok"));
              controller.close();
            },
          }));
        }, { preconnect: () => {} }) satisfies FetchFunction,
        anthropicProviderFactory: (settings) => {
          providerSettings.push(settings);
          return () => ({});
        },
        streamText: () => streamTextResult([finishPart()]),
      });

      await collectEvents(registry.stream({ request: anthropicRequest({ attachments: [] }), credential: sessionAnthropicCredential() }));
      const responsePromise = providerSettings[0]?.fetch?.("https://api.anthropic.com/v1/messages");
      await Promise.resolve();
      jest.advanceTimersByTime(10_001);
      const response = await responsePromise;

      await expect(response?.text()).resolves.toBe("ok");
    } finally {
      jest.useRealTimers();
    }
  });

  test("provider custom fetch classifies inter-chunk timeouts", async () => {
    const bodyProviderSettings: AnthropicProviderSettings[] = [];
    const bodyRegistry = new ProviderClientRegistry({
      providerFetchTimeouts: { interChunkTimeoutMs: 5 },
      fetch: Object.assign(async () =>
        new Response(new ReadableStream<Uint8Array>({
          start(controller) {
            controller.enqueue(new TextEncoder().encode("partial"));
          },
        })), { preconnect: () => {} }) satisfies FetchFunction,
      anthropicProviderFactory: (settings) => {
        bodyProviderSettings.push(settings);
        return () => ({});
      },
      streamText: () => streamTextResult([finishPart()]),
    });
    await collectEvents(bodyRegistry.stream({ request: anthropicRequest({ attachments: [] }), credential: sessionAnthropicCredential() }));
    const bodyResponse = await bodyProviderSettings[0]?.fetch?.("https://api.anthropic.com/v1/messages");
    const interChunkTimeout = await caughtError(bodyResponse?.text());

    expect(interChunkTimeout).toMatchObject({ timeout: true, kind: "inter_chunk" });
  });

  test("fails closed for non-catalog models and missing credentials before streamText", async () => {
    let streamTextCalls = 0;
    const registry = new ProviderClientRegistry({
      streamText: () => {
        streamTextCalls += 1;
        return streamTextResult([]);
      },
    });

    const unsupported = await collectEvents(registry.stream({
      request: validProviderRequest({
        model: { providerId: "openai", modelId: "gpt-4.1", variant: "" },
        attachments: [],
      }),
    }));
    const missingCredential = await collectEvents(registry.stream({
      request: anthropicRequest({ attachments: [] }),
    }));
    const missingMoonshotCredential = await collectEvents(registry.stream({
      request: kimiRequest({ attachments: [] }),
    }));

    expect(unsupported[0]?.providerError?.error).toMatchObject({
      code: "provider_unavailable",
      retryable: false,
      fatal: true,
    });
    expect(missingCredential[0]?.providerError?.error).toMatchObject({
      code: "credential_unavailable",
      retryable: false,
      fatal: true,
    });
    expect(missingMoonshotCredential[0]?.providerError?.error).toMatchObject({
      code: "credential_unavailable",
      retryable: false,
      fatal: true,
    });
    expect(streamTextCalls).toBe(0);
  });

  test("streams OpenAI GPT through the Responses client and strips stateless item ids on the raw wire", async () => {
    const calls: GatewayStreamTextInput[] = [];
    const providerSettings: OpenAIProviderSettings[] = [];
    const observedRequests: Array<{ readonly url: string; readonly body: string }> = [];
    const registry = new ProviderClientRegistry({
      fetch: Object.assign(async (input: Parameters<FetchFunction>[0], init?: Parameters<FetchFunction>[1]) => {
        const request = new Request(input, init);
        observedRequests.push({ url: request.url, body: await request.text() });
        return new Response("{}");
      }, { preconnect: () => {} }) satisfies FetchFunction,
      openAIProviderFactory: (settings) => {
        providerSettings.push(settings);
        return { responses: (modelId) => ({ provider: "openai", modelId }) };
      },
      streamText: (input) => {
        calls.push(input);
        return streamTextResult([finishPart()]);
      },
    });

    const events = await collectEvents(registry.stream({
      request: openAIRequest({ model: { providerId: "openai", modelId: "gpt-5.5", variant: "xhigh" } }),
      credential: sessionOpenAICredential(),
    }));
    await providerSettings[0]?.fetch?.("https://api.openai.com/v1/responses", {
      method: "POST",
      body: JSON.stringify({
        store: false,
        input: [{ id: "item_1", type: "message", content: [{ id: "part_1", type: "output_text", text: "hello" }] }],
      }),
    });

    expect(providerSettings).toEqual([{
      apiKey: "sk-session-openai",
      baseURL: "https://api.openai.com/v1",
      fetch: expect.any(Function),
    }]);
    expect(calls[0]).toMatchObject({
      model: { provider: "openai", modelId: "gpt-5.5" },
      maxRetries: 0,
      providerOptions: {
        openai: {
          store: false,
          promptCacheKey: "sesn_1",
          textVerbosity: "low",
          reasoningEffort: "xhigh",
          reasoningSummary: "auto",
          include: ["reasoning.encrypted_content"],
        },
      },
      headers: {},
    });
    expect("maxOutputTokens" in (calls[0] ?? {})).toBe(false);
    expect(observedRequests).toHaveLength(1);
    expect(observedRequests[0]?.url).toBe("https://api.openai.com/v1/responses");
    const observedBody = JSON.parse(observedRequests[0]?.body ?? "{}") as {
      readonly input?: ReadonlyArray<{ readonly id?: string; readonly content?: ReadonlyArray<{ readonly id?: string }> }>;
    };
    expect(observedBody.input?.[0]?.id).toBeUndefined();
    expect(observedBody.input?.[0]?.content?.[0]?.id).toBe("part_1");
    expect(events[0]?.finish).toMatchObject({
      metadataJson: JSON.stringify({ credential_source: "session" }),
      contextWindowTokens: 1_050_000,
      inputLimitTokens: undefined,
      outputTokenLimit: 128_000,
    });
  });

  test("serializes the GPT patch declaration through the frozen OpenAI Responses adapter", async () => {
    const grammar = "start: PATCH";
    const rawPatch = "*** Begin Patch\n*** Add File: note.txt\n+hello\n*** End Patch\n";
    let capturedBody: Record<string, unknown> | undefined;
    const registry = new ProviderClientRegistry({
      fetch: Object.assign(async (input: Parameters<FetchFunction>[0], init?: Parameters<FetchFunction>[1]) => {
        const request = new Request(input, init);
        capturedBody = JSON.parse(await request.text()) as Record<string, unknown>;
        const chunks = [
          { type: "response.created", response: { id: "resp_patch", created_at: 1_786_000_000, model: "gpt-5.5", service_tier: null } },
          { type: "response.output_item.added", output_index: 0, item: { type: "custom_tool_call", id: "item_patch", call_id: "call_patch", name: "apply_patch", input: "" } },
          { type: "response.custom_tool_call_input.delta", item_id: "item_patch", output_index: 0, delta: rawPatch },
          { type: "response.output_item.done", output_index: 0, item: { type: "custom_tool_call", id: "item_patch", call_id: "call_patch", name: "apply_patch", input: rawPatch, status: "completed" } },
          { type: "response.completed", response: { incomplete_details: null, usage: { input_tokens: 5, input_tokens_details: { cached_tokens: 0 }, output_tokens: 4, output_tokens_details: { reasoning_tokens: 0 } }, service_tier: "default" } },
        ];
        return new Response(chunks.map((chunk) => `data: ${JSON.stringify(chunk)}\n\n`).join(""), {
          headers: { "content-type": "text/event-stream" },
        });
      }, { preconnect: () => {} }) satisfies FetchFunction,
    });
    const objectSchema = JSON.stringify({ type: "object", properties: {}, additionalProperties: false });
    const events = await collectEvents(registry.stream({
      request: openAIRequest({
        context: [
          {
            role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
            content: [{ text: { text: "apply this patch" } }],
          },
          {
            role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
            content: [
              { toolCall: {
                modelToolCallId: "call_history_patch",
                name: "apply_patch",
                inputJson: JSON.stringify(rawPatch),
              } },
              { toolResult: {
                modelToolCallId: "call_history_patch",
                completed: { outputJson: JSON.stringify({ status: "success", result: "done" }) },
                error: undefined,
                cancelled: undefined,
              } },
            ],
          },
        ],
        tools: [
          { name: "exec_command", description: "Execute a command", function: { inputSchemaJson: objectSchema } },
          { name: "write_stdin", description: "Write command input", function: { inputSchemaJson: objectSchema } },
          { name: "view_image", description: "View an image", function: { inputSchemaJson: objectSchema } },
          { name: "apply_patch", description: "Apply a patch", freeform: { larkGrammar: grammar } },
          { name: "web", description: "Use web search", function: { inputSchemaJson: objectSchema } },
        ],
      }),
      credential: sessionOpenAICredential(),
    }));

    expect(events.map((event) => event.type)).toEqual([
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_START,
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_DELTA,
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_END,
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL,
      ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
    ]);
    expect(events[1]?.toolInput?.text).toBe(rawPatch);
    expect(JSON.parse(events[3]?.toolCall?.inputJson ?? "null")).toBe(rawPatch);
    const tools = capturedBody?.tools as ReadonlyArray<Record<string, unknown>> | undefined;
    expect(tools).toHaveLength(5);
    expect(tools?.filter((tool) => ["exec_command", "write_stdin", "view_image"].includes(String(tool.name))))
      .toEqual(expect.arrayContaining([
        expect.objectContaining({ type: "function", name: "exec_command" }),
        expect.objectContaining({ type: "function", name: "write_stdin" }),
        expect.objectContaining({ type: "function", name: "view_image" }),
      ]));
    const patchTool = tools?.find((tool) => tool.name === "apply_patch");
    expect(patchTool).toEqual({
      type: "custom",
      name: "apply_patch",
      description: "Apply a patch",
      format: { type: "grammar", syntax: "lark", definition: grammar },
    });
    expect(JSON.stringify(patchTool)).not.toContain("parameters");
    expect(patchTool?.type).not.toBe("apply_patch");
    const history = capturedBody?.input as ReadonlyArray<Record<string, unknown>> | undefined;
    expect(history).toEqual(expect.arrayContaining([
      expect.objectContaining({
        type: "custom_tool_call",
        call_id: "call_history_patch",
        name: "apply_patch",
        input: rawPatch,
      }),
      expect.objectContaining({
        type: "custom_tool_call_output",
        call_id: "call_history_patch",
      }),
    ]));
  });

  test("T6 streams OpenAI OAuth through the subscription transport with header swap and system instructions", async () => {
    const calls: GatewayStreamTextInput[] = [];
    const providerSettings: OpenAIProviderSettings[] = [];
    const observedRequests: Array<{
      readonly url: string;
      readonly authorization: string | null;
      readonly accountId: string | null;
      readonly originator: string | null;
      readonly sessionId: string | null;
      readonly userAgent: string | null;
      readonly body: string;
    }> = [];
    const registry = new ProviderClientRegistry({
      fetch: Object.assign(async (input: Parameters<FetchFunction>[0], init?: Parameters<FetchFunction>[1]) => {
        const request = new Request(input, init);
        observedRequests.push({
          url: request.url,
          authorization: request.headers.get("authorization"),
          accountId: request.headers.get("ChatGPT-Account-Id"),
          originator: request.headers.get("originator"),
          sessionId: request.headers.get("session-id"),
          userAgent: request.headers.get("user-agent"),
          body: await request.text(),
        });
        return new Response("{}");
      }, { preconnect: () => {} }) satisfies FetchFunction,
      openAIProviderFactory: (settings) => {
        providerSettings.push(settings);
        return { responses: (modelId) => ({ provider: "openai-oauth", modelId }) };
      },
      streamText: (input) => {
        calls.push(input);
        return streamTextResult([finishPart()]);
      },
    });

    const events = await collectEvents(registry.stream({
      request: openAIRequest(),
      credential: sessionOpenAIOAuthCredential(),
    }));
    await providerSettings[0]?.fetch?.("https://api.openai.com/v1/responses", {
      method: "POST",
      headers: { authorization: "Bearer sdk-dummy" },
      body: JSON.stringify({
        store: false,
        input: [{ id: "item_1", type: "message", content: [{ id: "part_1", type: "output_text", text: "hello" }] }],
      }),
    });

    expect(providerSettings).toEqual([{
      apiKey: OpenAIOAuthDummyAPIKey,
      baseURL: "https://api.openai.com/v1",
      fetch: expect.any(Function),
    }]);
    expect(calls[0]?.messages.map((message) => message.role)).toEqual(["user"]);
    expect(calls[0]?.providerOptions).toMatchObject({
      openai: {
        instructions: "You are concise.",
        store: false,
        promptCacheKey: "sesn_1",
      },
    });
    expect(observedRequests).toEqual([{
      url: "https://chatgpt.com/backend-api/codex/responses",
      authorization: "Bearer oauth-access",
      accountId: "acct_1",
      originator: null,
      sessionId: null,
      userAgent: null,
      body: expect.any(String),
    }]);
    const observedOAuthBody = JSON.parse(observedRequests[0]?.body ?? "{}") as {
      readonly input?: ReadonlyArray<{ readonly id?: string; readonly content?: ReadonlyArray<{ readonly id?: string }> }>;
    };
    expect(observedOAuthBody.input?.[0]?.id).toBeUndefined();
    expect(observedOAuthBody.input?.[0]?.content?.[0]?.id).toBe("part_1");
    expect(events[0]?.finish).toMatchObject({
      metadataJson: JSON.stringify({ credential_source: "session" }),
      contextWindowTokens: 400_000,
      inputLimitTokens: 272_000,
      outputTokenLimit: 128_000,
    });
  });

  test("GPT-5.6 Sol accepts ChatGPT OAuth supply through the subscription transport", async () => {
    const calls: GatewayStreamTextInput[] = [];
    const providerSettings: OpenAIProviderSettings[] = [];
    const registry = new ProviderClientRegistry({
      openAIProviderFactory: (settings) => {
        providerSettings.push(settings);
        return { responses: (modelId) => ({ provider: "openai", modelId }) };
      },
      streamText: (input) => {
        calls.push(input);
        return streamTextResult([finishPart()]);
      },
    });

    const events = await collectEvents(registry.stream({
      request: openAIRequest({
        model: { providerId: "openai", modelId: "gpt-5.6-sol", variant: "" },
      }),
      credential: sessionOpenAIOAuthCredential(),
    }));

    expect(events.at(-1)).toMatchObject({
      type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
      finish: {
        contextWindowTokens: 500_000,
        inputLimitTokens: 372_000,
        outputTokenLimit: 128_000,
      },
    });
    expect(providerSettings).toEqual([{
      apiKey: OpenAIOAuthDummyAPIKey,
      baseURL: "https://api.openai.com/v1",
      fetch: expect.any(Function),
    }]);
    expect(calls[0]).toMatchObject({
      model: { provider: "openai", modelId: "gpt-5.6-sol" },
      providerOptions: {
        openai: {
          instructions: "You are concise.",
          reasoningEffort: "medium",
          store: false,
        },
      },
    });
  });

  test("OpenAI OAuth pins the Codex public constants and refreshes expired tokens through Vault rotation", async () => {
    const constants = {
      clientId: OpenAICodexOAuthClientId,
      issuer: OpenAICodexOAuthIssuer,
      tokenEndpoint: OpenAICodexOAuthTokenEndpoint,
      endpoint: OpenAICodexResponsesEndpoint,
      dummyKey: OpenAIOAuthDummyAPIKey,
    };
    expect(constants).toEqual({
      clientId: "app_EMoamEEZ73f0CkXaXp7hrann",
      issuer: "https://auth.openai.com",
      tokenEndpoint: "https://auth.openai.com/oauth/token",
      endpoint: "https://chatgpt.com/backend-api/codex/responses",
      dummyKey: "tetral-openai-oauth-dummy-key",
    });

    const providerSettings: OpenAIProviderSettings[] = [];
    const observedRequests: Array<{
      readonly authorization: string | null;
      readonly accountId: string | null;
    }> = [];
    const refreshCalls: string[] = [];
    const registry = new ProviderClientRegistry({
      openAIOAuthCredentialRefreshWriter: {
        refreshOpenAIOAuthCredential: async ({ credential }) => {
          refreshCalls.push(credential.refreshToken ?? "");
          return {
            ok: true,
            credential: {
              ...credential,
              accessToken: "oauth-access-rotated",
              refreshToken: "oauth-refresh-rotated",
              expiresAt: "2999-01-01T00:00:00.000Z",
            },
          };
        },
      },
      fetch: Object.assign(async (input: RequestInfo | URL, init?: RequestInit) => {
        const request = new Request(input, init);
        observedRequests.push({
          authorization: request.headers.get("authorization"),
          accountId: request.headers.get("ChatGPT-Account-Id"),
        });
        return new Response("{}");
      }, { preconnect: () => {} }) satisfies FetchFunction,
      openAIProviderFactory: (settings) => {
        providerSettings.push(settings);
        return { responses: () => ({ provider: "openai-oauth" }) };
      },
      streamText: () => {
        return streamTextResult([finishPart()]);
      },
    });

    const events = await collectEvents(registry.stream({
      request: openAIRequest(),
      credential: sessionOpenAIOAuthCredential({ expiresAt: "2000-01-01T00:00:00.000Z" }),
    }));
    await providerSettings[0]?.fetch?.("https://api.openai.com/v1/responses", {
      method: "POST",
      headers: { authorization: "Bearer sdk-dummy" },
      body: "{}",
    });

    expect(events.at(-1)?.finish).toBeDefined();
    expect(refreshCalls).toEqual(["oauth-refresh"]);
    expect(observedRequests).toEqual([{
      authorization: "Bearer oauth-access-rotated",
      accountId: "acct_1",
    }]);
  });

  test("OpenAI OAuth fails closed when an expired token cannot be refreshed", async () => {
    let streamTextCalls = 0;
    const registry = new ProviderClientRegistry({
      openAIOAuthCredentialRefreshWriter: {
        refreshOpenAIOAuthCredential: async () => ({ ok: false, error: "credential_required" }),
      },
      openAIProviderFactory: () => ({ responses: () => ({}) }),
      streamText: () => {
        streamTextCalls += 1;
        return streamTextResult([finishPart()]);
      },
    });

    const events = await collectEvents(registry.stream({
      request: openAIRequest(),
      credential: sessionOpenAIOAuthCredential({ expiresAt: "2000-01-01T00:00:00.000Z" }),
    }));

    expect(events[0]?.providerError?.error).toMatchObject({
      code: "credential_required",
      retryable: false,
      fatal: true,
    });
    expect(streamTextCalls).toBe(0);
  });

  test("OpenAI OAuth fails closed when the Vault rotation boundary is not wired", async () => {
    let streamTextCalls = 0;
    const registry = new ProviderClientRegistry({
      openAIProviderFactory: () => ({ responses: () => ({}) }),
      streamText: () => {
        streamTextCalls += 1;
        return streamTextResult([finishPart()]);
      },
    });

    const events = await collectEvents(registry.stream({
      request: openAIRequest(),
      credential: sessionOpenAIOAuthCredential({ expiresAt: "2000-01-01T00:00:00.000Z" }),
    }));

    expect(events[0]?.providerError?.error).toMatchObject({
      code: "credential_required",
      retryable: false,
      fatal: true,
    });
    expect(streamTextCalls).toBe(0);
  });

  test("streams Moonshot Kimi through the Anthropic-family client with session credentials", async () => {
    const calls: GatewayStreamTextInput[] = [];
    const providerSettings: AnthropicProviderSettings[] = [];
    const registry = new ProviderClientRegistry({
      anthropicProviderFactory: (settings) => {
        providerSettings.push(settings);
        return (modelId) => ({ provider: "moonshotai", modelId });
      },
      streamText: (input) => {
        calls.push(input);
        return streamTextResult([finishPart()]);
      },
    });

    const events = await collectEvents(registry.stream({
      request: kimiRequest({
        limits: { maxOutputTokens: 200_000, timeoutMs: 30_000 },
      }),
      credential: sessionKimiCredential(),
    }));

    expect(providerSettings).toEqual([{
      apiKey: "sk-session-kimi",
      baseURL: "https://api.kimi.com/coding/v1",
      headers: {},
      fetch: expect.any(Function),
    }]);
    expect(calls[0]).toMatchObject({
      model: { provider: "moonshotai", modelId: "k3" },
      maxOutputTokens: 131_072,
      maxRetries: 0,
      providerOptions: {
        anthropic: {
          sendReasoning: true,
          toolStreaming: false,
        },
      },
      headers: {},
    });
    expect(calls[0]).not.toHaveProperty("temperature");
    expect(calls[0]?.providerOptions?.anthropic).not.toHaveProperty("thinking");
    expect(calls[0]?.topP).toBeUndefined();
    expect(calls[0]?.messages).toMatchObject([
      {
        role: "system",
        content: "You are concise.",
        providerOptions: { anthropic: { cacheControl: { type: "ephemeral" } } },
      },
      {
        role: "user",
        content: [{ type: "text", text: "hello", providerOptions: { anthropic: { cacheControl: { type: "ephemeral" } } } }],
      },
    ]);
    expect(events[0]?.finish?.metadataJson).toBe(JSON.stringify({ credential_source: "session" }));
  });

  test("streams DeepSeek through the openai-compatible client with hosted or session credentials", async () => {
    const calls: GatewayStreamTextInput[] = [];
    const providerSettings: OpenAICompatibleProviderSettings[] = [];
    const registry = new ProviderClientRegistry({
      openAICompatibleProviderFactory: (settings) => {
        providerSettings.push(settings);
        return (modelId) => ({ provider: "deepseek", modelId });
      },
      streamText: (input) => {
        calls.push(input);
        return streamTextResult([finishPart()]);
      },
    });

    const events = await collectEvents(registry.stream({
      request: deepSeekRequest({
        model: { providerId: "deepseek", modelId: "deepseek-v4-pro", variant: "high" },
        context: [{
          role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
          content: [
            { reasoning: { text: "hidden", metadataJson: "{}" } },
            { text: { text: "visible" } },
          ],
        }],
      }),
      credential: sessionDeepSeekCredential(),
    }));

    expect(providerSettings).toEqual([{
      apiKey: "sk-session-deepseek",
      baseURL: "https://api.deepseek.com",
      includeUsage: true,
      name: "deepseek",
      supportsStructuredOutputs: false,
      fetch: expect.any(Function),
    }]);
    expect(calls[0]).toMatchObject({
      model: { provider: "deepseek", modelId: "deepseek-v4-pro" },
      maxOutputTokens: 1024,
      maxRetries: 0,
      providerOptions: {
        deepseek: {
          reasoningEffort: "high",
        },
      },
      headers: {},
    });
    expect(calls[0]?.messages).toMatchObject([
      { role: "system", content: "You are concise." },
      { role: "assistant", providerOptions: { openaiCompatible: { reasoning_content: "hidden" } } },
    ]);
    expect(JSON.stringify(calls[0]?.messages)).not.toContain("\"deepseek\":{\"reasoning_content\"");
    expect(events[0]?.finish?.metadataJson).toBe(JSON.stringify({ credential_source: "session" }));
  });

  test("streams Z.ai GLM through the openai-compatible client with session credentials", async () => {
    const calls: GatewayStreamTextInput[] = [];
    const providerSettings: OpenAICompatibleProviderSettings[] = [];
    const registry = new ProviderClientRegistry({
      openAICompatibleProviderFactory: (settings) => {
        providerSettings.push(settings);
        return (modelId) => ({ provider: "zai", modelId });
      },
      streamText: (input) => {
        calls.push(input);
        return streamTextResult([finishPart()]);
      },
    });

    const events = await collectEvents(registry.stream({
      request: zaiRequest({
        model: { providerId: "zai", modelId: "glm-5.2", variant: "high" },
        context: [{
          role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
          content: [
            { reasoning: { text: "hidden", metadataJson: "{}" } },
            { text: { text: "visible" } },
          ],
        }],
        limits: { maxOutputTokens: 200_000, timeoutMs: 30_000 },
      }),
      credential: sessionZaiCredential(),
    }));

    expect(providerSettings).toEqual([{
      apiKey: "sk-session-zai",
      baseURL: "https://api.z.ai/api/coding/paas/v4",
      includeUsage: true,
      name: "zai",
      supportsStructuredOutputs: false,
      fetch: expect.any(Function),
    }]);
    expect(calls[0]).toMatchObject({
      model: { provider: "zai", modelId: "glm-5.2" },
      maxOutputTokens: 131_072,
      maxRetries: 0,
      providerOptions: {
        zai: {
          thinking: { type: "enabled", clear_thinking: false },
          reasoningEffort: "high",
        },
      },
      headers: {},
    });
    expect(calls[0]?.messages).toEqual([{
      role: "assistant",
      content: [{ type: "text", text: "visible" }],
      providerOptions: { openaiCompatible: { reasoning_content: "hidden" } },
    }]);
    expect(JSON.stringify(calls[0]?.messages)).not.toContain("\"zai\":{\"reasoning_content\"");
    expect(calls[0]?.temperature).toBeUndefined();
    expect(calls[0]?.topP).toBeUndefined();
    expect(calls[0]?.onError).toEqual(expect.any(Function));
    expect(events[0]?.finish?.metadataJson).toBe(JSON.stringify({ credential_source: "session" }));
  });

  test("classifies Anthropic body errors so service can switch platform keys before first byte", async () => {
    const request = anthropicRequest({ attachments: [] });
    const registry = new ProviderClientRegistry({
      anthropicProviderFactory: () => () => ({}),
      streamText: () => streamTextResult([{
        type: "error",
        error: {
          statusCode: 429,
          data: { error: { type: "rate_limit_error" } },
          responseHeaders: { "retry-after": "2" },
        },
      }]),
    });

    let caught: unknown;
    try {
      await collectEvents(registry.stream({ request, credential: platformAnthropicCredential() }));
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(ProviderKeyFailureError);
    expect((caught as ProviderKeyFailureError).classification).toMatchObject({
      action: "cooling",
      cooldownMs: 2_000,
      providerError: {
        code: "provider_rate_limited",
        retryable: true,
        fatal: false,
        statusCode: 429,
        retryAfterMs: 2_000,
      },
    });
  });

  test("classifies an untagged status-less Anthropic error as transport failure", async () => {
    const request = anthropicRequest({ attachments: [] });
    const registry = new ProviderClientRegistry({
      anthropicProviderFactory: () => () => ({}),
      streamText: () => streamTextResult([{
        type: "error",
        error: { opaque: "statusless-private-canary" },
      }]),
    });

    let caught: unknown;
    try {
      await collectEvents(registry.stream({ request, credential: platformAnthropicCredential() }));
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(ProviderKeyFailureError);
    expect(caught).toMatchObject({
      origin: "transport_failure",
      classification: {
        action: "retryable",
        providerError: {
          code: "provider_stream_error",
          retryable: true,
          fatal: false,
        },
      },
    });
  });

  test("treats an Anthropic stream that closes without finish as a retryable terminal failure", async () => {
    const request = anthropicRequest({ attachments: [] });
    const registry = new ProviderClientRegistry({
      anthropicProviderFactory: () => () => ({}),
      streamText: () => streamTextResult([
        { type: "start" },
        { type: "text-start", id: "txt_1" },
        { type: "text-delta", id: "txt_1", text: "partial" },
        { type: "text-end", id: "txt_1" },
      ]),
    });

    let caught: unknown;
    try {
      await collectEvents(registry.stream({ request, credential: platformAnthropicCredential() }));
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(ProviderKeyFailureError);
    expect((caught as ProviderKeyFailureError).classification).toMatchObject({
      action: "retryable",
      providerError: {
        code: "provider_stream_error",
        retryable: true,
        fatal: false,
      },
    });
  });
});

function anthropicRequest(overrides: Partial<ProviderRequest> = {}): ProviderRequest {
  return validProviderRequest({
    model: { providerId: "anthropic", modelId: "claude-opus-4-8", variant: "" },
    attachments: [],
    ...overrides,
  });
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

function kimiRequest(overrides: Partial<ProviderRequest> = {}): ProviderRequest {
  return validProviderRequest({
    model: { providerId: "moonshotai", modelId: "kimi-k3", variant: "" },
    attachments: [],
    ...overrides,
  });
}

function openAIRequest(overrides: Partial<ProviderRequest> = {}): ProviderRequest {
  return validProviderRequest({
    model: { providerId: "openai", modelId: "gpt-5.5", variant: "" },
    attachments: [],
    ...overrides,
  });
}

function deepSeekRequest(overrides: Partial<ProviderRequest> = {}): ProviderRequest {
  return validProviderRequest({
    model: { providerId: "deepseek", modelId: "deepseek-v4-pro", variant: "" },
    attachments: [],
    ...overrides,
  });
}

function zaiRequest(overrides: Partial<ProviderRequest> = {}): ProviderRequest {
  return validProviderRequest({
    model: { providerId: "zai", modelId: "glm-5.2", variant: "" },
    system: [],
    attachments: [],
    ...overrides,
  });
}

function streamTextResult(parts: readonly TextStreamPart<ToolSet>[]): GatewayStreamTextResult {
  return {
    fullStream: (async function* () {
      for (const part of parts) {
        yield part;
      }
    })(),
  };
}

function finishPart(): TextStreamPart<ToolSet> {
  return {
    type: "finish",
    finishReason: "stop",
    rawFinishReason: undefined,
    totalUsage: {
      inputTokens: 1,
      inputTokenDetails: { noCacheTokens: 1, cacheReadTokens: undefined, cacheWriteTokens: undefined },
      outputTokens: 1,
      outputTokenDetails: { textTokens: 1, reasoningTokens: undefined },
      totalTokens: 2,
    },
  };
}

async function collectEvents(events: AsyncIterable<ProviderStreamEvent>): Promise<readonly ProviderStreamEvent[]> {
  const output: ProviderStreamEvent[] = [];
  for await (const event of events) {
    output.push(event);
  }
  return output;
}

async function caughtError(promise: Promise<unknown> | undefined): Promise<unknown> {
  try {
    await promise;
    throw new Error("expected error");
  } catch (error) {
    return error;
  }
}

const RedirectRequestBody = JSON.stringify({ store: false, input: [] });

interface RedirectRequestSnapshot {
  readonly url: string;
  readonly method: string;
  readonly authorization: string | null;
  readonly cookie: string | null;
  readonly contentLength: string | null;
  readonly contentType: string | null;
  readonly contentEncoding: string | null;
  readonly contentLanguage: string | null;
  readonly contentLocation: string | null;
  readonly body: string;
}

async function openAIOAuthRedirect(status: number, location: string): Promise<readonly RedirectRequestSnapshot[]> {
  const providerSettings: OpenAIProviderSettings[] = [];
  const requests: RedirectRequestSnapshot[] = [];
  const registry = new ProviderClientRegistry({
    fetch: Object.assign(async (input: Parameters<FetchFunction>[0], init?: Parameters<FetchFunction>[1]) => {
      const request = new Request(input, init);
      requests.push({
        url: request.url,
        method: request.method,
        authorization: request.headers.get("authorization"),
        cookie: request.headers.get("cookie"),
        contentLength: request.headers.get("content-length"),
        contentType: request.headers.get("content-type"),
        contentEncoding: request.headers.get("content-encoding"),
        contentLanguage: request.headers.get("content-language"),
        contentLocation: request.headers.get("content-location"),
        body: await request.text(),
      });
      if (request.url === OpenAICodexResponsesEndpoint) {
        return new Response(null, { status, headers: { location } });
      }
      return new Response("ok");
    }, { preconnect: () => {} }) satisfies FetchFunction,
    openAIProviderFactory: (settings) => {
      providerSettings.push(settings);
      return { responses: () => ({ provider: "openai-oauth" }) };
    },
    streamText: () => streamTextResult([finishPart()]),
  });

  await collectEvents(registry.stream({ request: openAIRequest(), credential: sessionOpenAIOAuthCredential() }));
  const response = await providerSettings[0]?.fetch?.("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: {
      authorization: "Bearer sdk-dummy",
      cookie: "session=secret",
      "content-length": String(new TextEncoder().encode(RedirectRequestBody).byteLength),
      "content-type": "application/json",
      "content-encoding": "gzip",
      "content-language": "en",
      "content-location": "/original-body",
    },
    body: RedirectRequestBody,
  });
  await expect(response?.text()).resolves.toBe("ok");
  return requests;
}

function sessionAnthropicCredential(): ResolvedProviderCredential {
  return {
    source: "session",
    authType: "provider_api_key",
    providerId: "anthropic",
    supplyMode: "anthropic-api-key",
    vaultId: "vlt_1",
    credentialId: "cred_1",
    accessMode: "api_key",
    apiKey: "sk-session-anthropic",
  };
}

function platformAnthropicCredential(): ResolvedProviderCredential {
  return {
    source: "platform",
    authType: "provider_api_key",
    providerId: "anthropic",
    supplyMode: "anthropic-api-key",
    platformKey: {
      keyId: "pfk_1",
      providerId: "anthropic",
      key: "sk-platform-anthropic",
      weight: 1,
      priority: 0,
      cacheScope: "anthropic-scope",
    },
  };
}

function sessionOpenAICredential(): ResolvedProviderCredential {
  return {
    source: "session",
    authType: "provider_api_key",
    providerId: "openai",
    supplyMode: "openai-api-key",
    vaultId: "vlt_1",
    credentialId: "cred_openai",
    accessMode: "user_api_key",
    apiKey: "sk-session-openai",
  };
}

function sessionOpenAIOAuthCredential(overrides: Partial<Extract<ResolvedProviderCredential, { authType: "provider_oauth" }>> = {}): ResolvedProviderCredential {
  return {
    source: "session",
    authType: "provider_oauth",
    providerId: "openai",
    supplyMode: "openai-chatgpt-oauth",
    vaultId: "vlt_1",
    credentialId: "cred_openai_oauth",
    accessMode: "oauth",
    accessToken: "oauth-access",
    refreshToken: "oauth-refresh",
    expiresAt: "2999-01-01T00:00:00.000Z",
    accountId: "acct_1",
    ...overrides,
  };
}

function sessionDeepSeekCredential(): ResolvedProviderCredential {
  return {
    source: "session",
    authType: "provider_api_key",
    providerId: "deepseek",
    supplyMode: "deepseek-api-key",
    vaultId: "vlt_1",
    credentialId: "cred_deepseek",
    accessMode: "user_api_key",
    apiKey: "sk-session-deepseek",
  };
}

function sessionKimiCredential(): ResolvedProviderCredential {
  return {
    source: "session",
    authType: "provider_api_key",
    providerId: "moonshotai",
    supplyMode: "moonshotai-code-api-key",
    vaultId: "vlt_1",
    credentialId: "cred_kimi",
    accessMode: "api_key",
    apiKey: "sk-session-kimi",
  };
}

function sessionZaiCredential(): ResolvedProviderCredential {
  return {
    source: "session",
    authType: "provider_api_key",
    providerId: "zai",
    supplyMode: "zai-coding-api-key",
    vaultId: "vlt_1",
    credentialId: "cred_zai",
    accessMode: "api_key",
    apiKey: "sk-session-zai",
  };
}
