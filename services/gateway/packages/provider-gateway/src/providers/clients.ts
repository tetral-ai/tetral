/**
 * Builds provider SDK clients and translates approved Gateway requests into
 * normalized provider event streams.
 *
 * `ProviderGatewayServiceShell` calls {@link ProviderClientRegistry.stream} after
 * authenticating the Runtime caller, validating its binding, resolving
 * attachments, and selecting a credential. The registry consults the model
 * catalog and lowering rules, invokes the appropriate AI SDK provider, and
 * delegates stream normalization to `ProviderStreamRaiser` before returning
 * events to service orchestration. For due OpenAI OAuth credentials it calls
 * the injected refresh writer, whose SQL adapter owns the encrypted write-back,
 * before constructing the provider request.
 *
 * The registry accepts only cataloged provider/model pairs, disables SDK retries,
 * keeps each stream on its supplied credential, and restricts provider egress to the
 * cataloged HTTPS roots. The fetch adapters strip stateless OpenAI input-item IDs,
 * rewrite OAuth URL and authorization headers, and reconstruct allowlisted manual
 * redirects with status-specific method/body handling. Provider credentials and raw
 * chunks never enter the returned Gateway event payloads.
 *
 * @packageDocumentation
 */
import { APICallError } from "@ai-sdk/provider";
import { createAnthropic } from "@ai-sdk/anthropic";
import { createOpenAI } from "@ai-sdk/openai";
import { createOpenAICompatible } from "@ai-sdk/openai-compatible";
import { jsonSchema, Output, streamText } from "ai";
import { providerErrorEvent } from "@tetral/gateway-lowering/src/errors.js";
import { lowerProviderRequest, remapOpenAICompatibleMessageMetadataForSDK } from "@tetral/gateway-lowering/src/request.js";
import { lookupGatewayProviderRules } from "@tetral/gateway-lowering/src/rules/index.js";
import { ProviderStreamRaiser } from "@tetral/gateway-lowering/src/stream.js";
import { ProviderStreamEventType } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { lookupGatewayModel, routeEffectiveGatewayModelLimits } from "./catalog.js";
import { createOpenAIOAuthFetch, OpenAICodexResponsesEndpoint, OpenAIOAuthDummyAPIKey } from "./openai-oauth.js";
import { openAIOAuthCredentialRefreshDue } from "./openai-oauth-refresh.js";
import { classifyProviderFailure, ProviderKeyFailureError } from "./pool.js";
import type {
  ModelMessage,
  TextStreamPart,
  Tool,
  ToolSet,
} from "ai";
import type { JSONValue } from "@ai-sdk/provider";
import type { FetchFunction, ProviderOptions } from "@ai-sdk/provider-utils";
import type { OpenAIProvider as AIOpenAIProvider, OpenAIProviderSettings as AIOpenAIProviderSettings } from "@ai-sdk/openai";
import type { OpenAICompatibleProviderSettings as AIOpenAICompatibleProviderSettings } from "@ai-sdk/openai-compatible";
import type { ProviderRequest, ProviderStreamEvent } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { ProviderRules } from "@tetral/gateway-lowering/src/rules/rules.js";
import type {
  GatewayStreamPart,
} from "@tetral/gateway-lowering/src/stream.js";
import type {
  LoweredAssistantContentPart,
  LoweredProviderMessage,
  LoweredToolDefinition,
  LoweredToolResultPart,
  LoweredUserContentPart,
} from "@tetral/gateway-lowering/src/request.js";
import type { ProviderRequestStreamInput, ProviderRequestStreamer } from "../service.js";
import type { GatewayModelCatalogEntry } from "./catalog.js";
import type { GatewayCatalogProviderId } from "./pool.js";
import type { ResolvedProviderCredential, ResolvedSessionOAuthCredential } from "./credentials.js";
import type { OpenAIOAuthCredentialRefreshWriter } from "./openai-oauth-refresh.js";

/** Dependencies and transport overrides used to construct a provider client registry. */
export interface ProviderClientRegistryOptions {
  readonly streamText?: GatewayStreamTextFunction | undefined;
  readonly anthropicProviderFactory?: AnthropicProviderFactory | undefined;
  readonly openAIProviderFactory?: OpenAIProviderFactory | undefined;
  readonly openAICompatibleProviderFactory?: OpenAICompatibleProviderFactory | undefined;
  readonly fetch?: FetchFunction | undefined;
  readonly providerFetchTimeouts?: ProviderFetchTimeoutOptions | undefined;
  readonly openAIOAuthCredentialRefreshWriter?: OpenAIOAuthCredentialRefreshWriter | undefined;
}

/** Anthropic SDK settings supplied by the registry's provider factory boundary. */
export interface AnthropicProviderSettings {
  readonly apiKey: string;
  readonly baseURL?: string;
  readonly headers?: Record<string, string>;
  readonly fetch?: FetchFunction;
  readonly name?: string;
}

/** Creates an Anthropic provider whose returned selector binds a model ID. */
export type AnthropicProviderFactory = (settings: AnthropicProviderSettings) => (modelId: string) => unknown;
/** OpenAI SDK settings supplied by the registry's provider factory boundary. */
export type OpenAIProviderSettings = Pick<AIOpenAIProviderSettings, "apiKey" | "baseURL" | "fetch" | "headers" | "name">;
/** Minimal OpenAI provider surface required to select the Responses API model. */
export interface OpenAIProvider {
  readonly responses: (modelId: Parameters<AIOpenAIProvider["responses"]>[0]) => unknown;
  readonly tools?: Pick<AIOpenAIProvider["tools"], "customTool">;
}
/** Creates the OpenAI provider used for official and OAuth Responses routes. */
export type OpenAIProviderFactory = (settings: OpenAIProviderSettings) => OpenAIProvider;
/** OpenAI-compatible SDK settings supplied for DeepSeek and ZAI routes. */
export type OpenAICompatibleProviderSettings = Pick<AIOpenAICompatibleProviderSettings, "apiKey" | "baseURL" | "fetch" | "headers" | "includeUsage" | "name" | "supportsStructuredOutputs">;
/** Creates an OpenAI-compatible provider whose returned selector binds a model ID. */
export type OpenAICompatibleProviderFactory = (settings: OpenAICompatibleProviderSettings) => (modelId: string) => unknown;

/** Canonical AI SDK streaming call shape emitted after provider-specific lowering. */
export interface GatewayStreamTextInput {
  readonly model: unknown;
  readonly messages: readonly ModelMessage[];
  readonly tools?: ToolSet | undefined;
  readonly output?: ReturnType<typeof Output.object> | undefined;
  readonly providerOptions?: ProviderOptions | undefined;
  readonly maxOutputTokens?: number | undefined;
  readonly temperature?: number | undefined;
  readonly topP?: number | undefined;
  readonly topK?: number | undefined;
  readonly headers?: Record<string, string | undefined> | undefined;
  readonly abortSignal?: AbortSignal | undefined;
  readonly maxRetries: 0;
  readonly onError?: ((event: { readonly error: unknown }) => void | Promise<void>) | undefined;
}

/** Streaming portion of the AI SDK result consumed by the Gateway stream raiser. */
export interface GatewayStreamTextResult {
  readonly fullStream: AsyncIterable<TextStreamPart<ToolSet>>;
}

/** Injectable boundary around the AI SDK `streamText` operation. */
export type GatewayStreamTextFunction = (input: GatewayStreamTextInput) => GatewayStreamTextResult;

/** Raw provider-body inter-chunk watchdog duration. First normalized event liveness is service-owned. */
export interface ProviderFetchTimeoutOptions {
  readonly interChunkTimeoutMs?: number | undefined;
}

const DefaultProviderFetchInterChunkTimeoutMs = 30_000;
const MaxProviderRedirects = 5;

/**
 * Selects the cataloged provider transport, lowers one request, and yields normalized
 * Gateway stream events.
 *
 * Credentials and resolved attachment bytes are supplied by service
 * orchestration. This registry does not load session state or persist provider
 * results, but it may invoke the injected OAuth refresh writer to rotate and
 * durably replace an OpenAI credential that is due within the refresh-skew
 * window before the call.
 */
export class ProviderClientRegistry implements ProviderRequestStreamer {
  private readonly streamText: GatewayStreamTextFunction;
  private readonly anthropicProviderFactory: AnthropicProviderFactory;
  private readonly openAIProviderFactory: OpenAIProviderFactory;
  private readonly openAICompatibleProviderFactory: OpenAICompatibleProviderFactory;
  private readonly fetch: FetchFunction | undefined;
  private readonly providerFetchTimeouts: ProviderFetchTimeoutOptions;
  private readonly openAIOAuthCredentialRefreshWriter: OpenAIOAuthCredentialRefreshWriter | undefined;

  constructor(options: ProviderClientRegistryOptions = {}) {
    this.streamText = options.streamText ?? defaultStreamText;
    this.anthropicProviderFactory = options.anthropicProviderFactory ?? ((settings) => createAnthropic(settings));
    this.openAIProviderFactory = options.openAIProviderFactory ?? ((settings) => createOpenAI(settings));
    this.openAICompatibleProviderFactory = options.openAICompatibleProviderFactory ?? ((settings) => createOpenAICompatible(settings));
    this.fetch = options.fetch;
    this.providerFetchTimeouts = options.providerFetchTimeouts ?? {};
    this.openAIOAuthCredentialRefreshWriter = options.openAIOAuthCredentialRefreshWriter;
  }

  /** Streams one validated request through its catalog-selected provider client. */
  async *stream(input: ProviderRequestStreamInput): AsyncGenerator<ProviderStreamEvent> {
    input.abortSignal?.throwIfAborted();
    const model = input.request.model;
    const entry = model === undefined ? undefined : lookupGatewayModel(model.providerId, model.modelId);
    if (entry === undefined) {
      yield providerErrorEvent({
        code: "provider_unavailable",
        message: "Provider model is not approved by the Gateway catalog.",
        retryable: false,
        fatal: true,
        statusCode: 503,
      });
      return;
    }
    const rules = lookupGatewayProviderRules(entry.providerId, entry.modelId);
    if (rules === undefined) {
      yield providerErrorEvent({
        code: "provider_unavailable",
        message: "Provider model lowering rules are unavailable.",
        retryable: false,
        fatal: true,
        statusCode: 503,
      });
      return;
    }
    switch (entry.providerId) {
      case "anthropic":
        yield* this.streamAnthropicFamily(input, entry, rules);
        return;
      case "openai":
        yield* this.streamOpenAI(input, entry, rules);
        return;
      case "deepseek":
        yield* this.streamOpenAICompatible(input, entry, rules);
        return;
      case "moonshotai":
        yield* this.streamAnthropicFamily(input, entry, rules);
        return;
      case "zai":
        yield* this.streamOpenAICompatible(input, entry, rules);
        return;
    }
    yield providerErrorEvent({
      code: "provider_unavailable",
      message: "Provider model is not implemented in this Gateway tier.",
      retryable: false,
      fatal: true,
      statusCode: 503,
    });
  }

  private async *streamAnthropicFamily(
    input: ProviderRequestStreamInput,
    entry: GatewayModelCatalogEntry,
    rules: ProviderRules,
  ): AsyncGenerator<ProviderStreamEvent> {
    const credential = input.credential;
    if (!isProviderAPIKeyCredential(credential, entry.supplyProviderId)) {
      yield providerErrorEvent({
        code: "credential_unavailable",
        message: "Provider credentials are unavailable.",
        retryable: false,
        fatal: true,
        statusCode: 503,
      });
      return;
    }

    const lowered = lowerProviderRequest(input.request, rules, {
      modelOutputTokenLimit: entry.modelOutputTokenLimit,
      resolvedAttachments: input.resolvedAttachments,
    });
    if (lowered.options.maxOutputTokens === undefined) {
      yield providerErrorEvent({
        code: "provider_unavailable",
        message: "Anthropic provider output limit is unavailable.",
        retryable: false,
        fatal: true,
        statusCode: 503,
      });
      return;
    }
    const sdkBaseURL = anthropicSdkBaseURL(entry.baseURLs[0]);
    const providerSettings: AnthropicProviderSettings = {
      apiKey: credentialAPIKey(credential),
      headers: lowered.options.headers,
      ...(sdkBaseURL !== undefined ? { baseURL: sdkBaseURL } : {}),
      fetch: providerFetch({
        fetchImpl: this.fetch,
        allowedBaseURLs: entry.baseURLs,
        abortSignal: input.abortSignal,
        overallTimeoutMs: input.request.limits?.timeoutMs,
        timeouts: this.providerFetchTimeouts,
        onTransportActivity: input.onTransportActivity,
      }),
    };
    const provider = this.anthropicProviderFactory(providerSettings);
    const result = this.streamText({
      model: provider(entry.apiModelId),
      messages: lowered.messages.map(toAIModelMessage),
      tools: toAITools(lowered.tools),
      ...(lowered.structuredOutput !== undefined ? { output: toAIOutput(lowered.structuredOutput) } : {}),
      providerOptions: lowered.options.providerOptions as ProviderOptions,
      maxOutputTokens: lowered.options.maxOutputTokens,
      ...(lowered.options.temperature !== undefined ? { temperature: lowered.options.temperature } : {}),
      ...(lowered.options.topP !== undefined ? { topP: lowered.options.topP } : {}),
      ...(lowered.options.topK !== undefined ? { topK: lowered.options.topK } : {}),
      headers: lowered.options.headers,
      abortSignal: input.abortSignal,
      maxRetries: 0,
      onError: ignoreProviderStreamError,
    });
    const raiser = new ProviderStreamRaiser({
      usageWireFamily: "anthropic-wire",
      modelLimits: routeEffectiveGatewayModelLimits(entry, credential.supplyMode),
    });
    let terminal = false;
    for await (const part of result.fullStream) {
      for (const event of raiser.map(toGatewayStreamPart(part, credential, entry.supplyProviderId))) {
        terminal = isTerminalProviderStreamEvent(event);
        yield event;
        if (terminal) {
          return;
        }
      }
    }
    if (!terminal) {
      throw classifiedProviderError(entry.supplyProviderId, { networkError: true });
    }
  }

  private async *streamOpenAI(
    input: ProviderRequestStreamInput,
    entry: GatewayModelCatalogEntry,
    rules: ProviderRules,
  ): AsyncGenerator<ProviderStreamEvent> {
    const credential = input.credential;
    if (isProviderAPIKeyCredential(credential, "openai")) {
      yield* this.streamOpenAIResponses(input, entry, rules, credential, officialOpenAIProviderFetch(input, ["https://api.openai.com"], this.fetch, this.providerFetchTimeouts));
      return;
    }
    if (isOpenAIOAuthCredential(credential) && entry.supplyModes.includes("openai-chatgpt-oauth")) {
      let oauthCredential = credential;
      if (openAIOAuthCredentialRefreshDue(oauthCredential.expiresAt)) {
        const refresh = await this.openAIOAuthCredentialRefreshWriter?.refreshOpenAIOAuthCredential({
          workspaceId: input.request.workspaceId,
          credential: oauthCredential,
          abortSignal: input.abortSignal ?? AbortSignal.timeout(input.request.limits?.timeoutMs ?? 1),
        });
        if (refresh?.ok !== true || openAIOAuthCredentialRefreshDue(refresh.credential.expiresAt)) {
          yield providerErrorEvent({
            code: "credential_required",
            message: "OpenAI OAuth credential refresh failed; re-authorization is required.",
            retryable: false,
            fatal: true,
            statusCode: 403,
          });
          return;
        }
        oauthCredential = refresh.credential;
      }
      if (openAIOAuthCredentialRefreshDue(oauthCredential.expiresAt)) {
        yield providerErrorEvent({
          code: "credential_required",
          message: "OpenAI OAuth credential refresh failed; re-authorization is required.",
          retryable: false,
          fatal: true,
          statusCode: 403,
        });
        return;
      }
      yield* this.streamOpenAIResponses(
        input,
        entry,
        rules,
        oauthCredential,
        createOpenAIOAuthFetch({
          fetchImpl: officialOpenAIProviderFetch(input, ["https://api.openai.com", OpenAICodexResponsesEndpoint], this.fetch, this.providerFetchTimeouts),
          accessToken: oauthCredential.accessToken,
          accountId: oauthCredential.accountId,
        }),
        { apiKey: OpenAIOAuthDummyAPIKey },
      );
      return;
    }
    yield providerErrorEvent({
      code: "credential_unavailable",
      message: "Provider credentials are unavailable.",
      retryable: false,
      fatal: true,
      statusCode: 503,
    });
  }

  private async *streamOpenAIResponses(
    input: ProviderRequestStreamInput,
    entry: GatewayModelCatalogEntry,
    rules: ProviderRules,
    credential: ResolvedProviderAPIKeyCredential | ResolvedOpenAIOAuthCredential,
    fetchImpl: FetchFunction,
    overrides: { readonly apiKey?: string | undefined } = {},
  ): AsyncGenerator<ProviderStreamEvent> {
    const lowered = lowerProviderRequest(input.request, rules, {
      modelOutputTokenLimit: entry.modelOutputTokenLimit,
      resolvedAttachments: input.resolvedAttachments,
    });
    const baseURL = openAISdkBaseURL(entry.baseURLs[0]);
    const provider = this.openAIProviderFactory({
      apiKey: overrides.apiKey ?? credentialAPIKey(credential),
      ...(baseURL !== undefined ? { baseURL } : {}),
      fetch: fetchImpl,
    });
    const callShape = openAIResponsesCallShape(lowered, credential.authType === "provider_oauth");
    const result = this.streamText({
      model: provider.responses(entry.apiModelId),
      messages: callShape.messages,
      tools: toAITools(lowered.tools, provider.tools?.customTool),
      ...(lowered.structuredOutput !== undefined ? { output: toAIOutput(lowered.structuredOutput) } : {}),
      providerOptions: callShape.providerOptions as ProviderOptions,
      ...(lowered.options.maxOutputTokens !== undefined ? { maxOutputTokens: lowered.options.maxOutputTokens } : {}),
      ...(lowered.options.temperature !== undefined ? { temperature: lowered.options.temperature } : {}),
      ...(lowered.options.topP !== undefined ? { topP: lowered.options.topP } : {}),
      ...(lowered.options.topK !== undefined ? { topK: lowered.options.topK } : {}),
      headers: lowered.options.headers,
      abortSignal: input.abortSignal,
      maxRetries: 0,
      onError: ignoreProviderStreamError,
    });
    const raiser = new ProviderStreamRaiser({
      usageWireFamily: "openai-wire",
      modelLimits: routeEffectiveGatewayModelLimits(entry, credential.supplyMode),
    });
    let terminal = false;
    for await (const part of result.fullStream) {
      for (const event of raiser.map(toGatewayStreamPart(part, credential, entry.supplyProviderId))) {
        terminal = isTerminalProviderStreamEvent(event);
        yield event;
        if (terminal) {
          return;
        }
      }
    }
    if (!terminal) {
      throw classifiedProviderError(entry.supplyProviderId, { networkError: true });
    }
  }

  private async *streamOpenAICompatible(
    input: ProviderRequestStreamInput,
    entry: GatewayModelCatalogEntry,
    rules: ProviderRules,
  ): AsyncGenerator<ProviderStreamEvent> {
    const credential = input.credential;
    if (!isProviderAPIKeyCredential(credential, entry.supplyProviderId)) {
      yield providerErrorEvent({
        code: "credential_unavailable",
        message: "Provider credentials are unavailable.",
        retryable: false,
        fatal: true,
        statusCode: 503,
      });
      return;
    }

    const lowered = lowerProviderRequest(input.request, rules, {
      modelOutputTokenLimit: entry.modelOutputTokenLimit,
      resolvedAttachments: input.resolvedAttachments,
    });
    if (lowered.options.maxOutputTokens === undefined) {
      yield providerErrorEvent({
        code: "provider_unavailable",
        message: "Provider output limit is unavailable.",
        retryable: false,
        fatal: true,
        statusCode: 503,
      });
      return;
    }
    const providerSettings: OpenAICompatibleProviderSettings = {
      apiKey: credentialAPIKey(credential),
      baseURL: entry.baseURLs[0] ?? "",
      includeUsage: true,
      name: entry.providerId,
      supportsStructuredOutputs: rules.structuredOutputStrategy === "native_json_schema",
      fetch: providerFetch({
        fetchImpl: this.fetch,
        allowedBaseURLs: entry.baseURLs,
        abortSignal: input.abortSignal,
        overallTimeoutMs: input.request.limits?.timeoutMs,
        timeouts: this.providerFetchTimeouts,
        onTransportActivity: input.onTransportActivity,
      }),
    };
    const provider = this.openAICompatibleProviderFactory(providerSettings);
    const sdkMessages = remapOpenAICompatibleMessageMetadataForSDK(lowered.messages, rules);
    const result = this.streamText({
      model: provider(entry.apiModelId),
      messages: sdkMessages.map(toAIModelMessage),
      tools: toAITools(lowered.tools),
      ...(lowered.structuredOutput !== undefined ? { output: toAIOutput(lowered.structuredOutput) } : {}),
      providerOptions: lowered.options.providerOptions as ProviderOptions,
      maxOutputTokens: lowered.options.maxOutputTokens,
      ...(lowered.options.temperature !== undefined ? { temperature: lowered.options.temperature } : {}),
      ...(lowered.options.topP !== undefined ? { topP: lowered.options.topP } : {}),
      ...(lowered.options.topK !== undefined ? { topK: lowered.options.topK } : {}),
      headers: lowered.options.headers,
      abortSignal: input.abortSignal,
      maxRetries: 0,
      onError: ignoreProviderStreamError,
    });
    const raiser = new ProviderStreamRaiser({
      usageWireFamily: "openai-wire",
      modelLimits: routeEffectiveGatewayModelLimits(entry, credential.supplyMode),
    });
    let terminal = false;
    for await (const part of result.fullStream) {
      for (const event of raiser.map(toGatewayStreamPart(part, credential, entry.supplyProviderId))) {
        terminal = isTerminalProviderStreamEvent(event);
        yield event;
        if (terminal) {
          return;
        }
      }
    }
    if (!terminal) {
      throw classifiedProviderError(entry.supplyProviderId, { networkError: true });
    }
  }
}

/** Creates the production provider streamer with optional injectable boundaries. */
export function createProviderClientRegistry(options: ProviderClientRegistryOptions = {}): ProviderClientRegistry {
  return new ProviderClientRegistry(options);
}

function defaultStreamText(input: GatewayStreamTextInput): GatewayStreamTextResult {
  return streamText(input as unknown as Parameters<typeof streamText>[0]) as unknown as GatewayStreamTextResult;
}

function ignoreProviderStreamError(): void {}

type ResolvedProviderAPIKeyCredential = ResolvedProviderCredential & {
  readonly authType: "provider_api_key";
  readonly providerId: GatewayCatalogProviderId;
};

type ResolvedOpenAIOAuthCredential = ResolvedSessionOAuthCredential;

function isProviderAPIKeyCredential(
  credential: ResolvedProviderCredential | undefined,
  providerId: GatewayCatalogProviderId,
): credential is ResolvedProviderAPIKeyCredential {
  return credential?.authType === "provider_api_key" && credential.providerId === providerId;
}

function isOpenAIOAuthCredential(
  credential: ResolvedProviderCredential | undefined,
): credential is ResolvedOpenAIOAuthCredential {
  return credential?.authType === "provider_oauth" && credential.providerId === "openai";
}

function credentialAPIKey(credential: ResolvedProviderAPIKeyCredential | ResolvedOpenAIOAuthCredential): string {
  if (credential.authType === "provider_oauth") {
    return OpenAIOAuthDummyAPIKey;
  }
  return credential.source === "platform" ? credential.platformKey.key : credential.apiKey;
}

function anthropicSdkBaseURL(baseURL: string | undefined): string | undefined {
  if (baseURL === undefined || baseURL.length === 0) {
    return undefined;
  }
  return baseURL.endsWith("/v1") ? baseURL : `${baseURL.replace(/\/+$/, "")}/v1`;
}

function openAISdkBaseURL(baseURL: string | undefined): string | undefined {
  if (baseURL === undefined || baseURL.length === 0) {
    return undefined;
  }
  return baseURL.endsWith("/v1") ? baseURL : `${baseURL.replace(/\/+$/, "")}/v1`;
}

function officialOpenAIProviderFetch(
  input: ProviderRequestStreamInput,
  allowedBaseURLs: readonly string[],
  fetchImpl: FetchFunction | undefined,
  timeouts: ProviderFetchTimeoutOptions,
): FetchFunction {
  return openAIResponsesWireFetch(providerFetch({
    fetchImpl,
    allowedBaseURLs,
    abortSignal: input.abortSignal,
    overallTimeoutMs: input.request.limits?.timeoutMs,
    timeouts,
    onTransportActivity: input.onTransportActivity,
  }));
}

// Provider egress fetch with allowlist enforcement, raw inter-chunk liveness, and
// the request's absolute deadline. The Gateway service owns first normalized event
// liveness; this raw HTTP layer begins its watchdog only after a response body exists.
function providerFetch(options: {
  readonly fetchImpl: FetchFunction | undefined;
  readonly allowedBaseURLs: readonly string[];
  readonly abortSignal?: AbortSignal | undefined;
  readonly overallTimeoutMs?: number | undefined;
  readonly timeouts: ProviderFetchTimeoutOptions;
  readonly onTransportActivity?: (() => void) | undefined;
}): FetchFunction {
  const allowedRoots = allowedProviderURLRoots(options.allowedBaseURLs);
  const targetFetch = options.fetchImpl ?? fetch;
  return Object.assign(async (input: Parameters<FetchFunction>[0], init?: Parameters<FetchFunction>[1]) => {
    let request = new Request(input, init);
    assertProviderEgressAllowed(request.url, allowedRoots);
    const timers = providerFetchTimers({
      callerSignal: request.signal,
      registrySignal: options.abortSignal,
      overallTimeoutMs: options.overallTimeoutMs,
      interChunkTimeoutMs: options.timeouts.interChunkTimeoutMs ?? DefaultProviderFetchInterChunkTimeoutMs,
    });
    try {
      for (let redirectCount = 0; redirectCount <= MaxProviderRedirects; redirectCount += 1) {
        const response = await targetFetch(new Request(request.clone(), { signal: timers.signal, redirect: "manual" }));
        if (!providerRedirectStatus(response.status)) {
          return wrapProviderResponseBody(
            response,
            timers,
            options.onTransportActivity,
          );
        }
        const location = response.headers.get("location");
        if (location === null || location.length === 0) {
          timers.clearAll();
          return response;
        }
        if (redirectCount === MaxProviderRedirects) {
          throw new TypeError("provider egress redirect limit exceeded");
        }
        request = providerRedirectRequest(request, location, response.status, allowedRoots);
      }
      throw new TypeError("provider egress redirect limit exceeded");
    } catch (error) {
      timers.clearAll();
      throw timers.timeoutError() ?? error;
    }
  }, {
    preconnect: ((...args: Parameters<FetchFunction["preconnect"]>) => {
      assertProviderPreconnectAllowed(String(args[0]), allowedRoots);
      return targetFetch.preconnect(...args);
    }) satisfies FetchFunction["preconnect"],
  }) satisfies FetchFunction;
}

// Rewrites the outbound OpenAI Responses body to drop input-item `id` fields when
// `store` is false (stripOpenAIStatelessItemIds). This is the package's raw-body
// transform; createOpenAIOAuthFetch separately changes the subscription URL and
// authorization headers, while providerFetch reconstructs allowlisted redirects.
function openAIResponsesWireFetch(fetchImpl: FetchFunction): FetchFunction {
  return Object.assign(async (input: Parameters<FetchFunction>[0], init?: Parameters<FetchFunction>[1]) => {
    const request = new Request(input, init);
    if (request.method === "GET" || request.method === "HEAD") {
      return fetchImpl(request);
    }
    const bodyText = await request.text();
    const patchedBody = stripOpenAIStatelessItemIds(bodyText);
    const headers = new Headers(request.headers);
    headers.delete("content-length");
    return fetchImpl(new Request(request.url, {
      method: request.method,
      headers,
      body: patchedBody,
      signal: request.signal,
    }));
  }, {
    preconnect: ((...args: Parameters<FetchFunction["preconnect"]>) => fetchImpl.preconnect(...args)) satisfies FetchFunction["preconnect"],
  }) satisfies FetchFunction;
}

function stripOpenAIStatelessItemIds(bodyText: string): string {
  let body: unknown;
  try {
    body = JSON.parse(bodyText) as unknown;
  } catch {
    return bodyText;
  }
  if (!isPlainRecord(body) || body.store !== false || !Array.isArray(body.input)) {
    return bodyText;
  }
  const stripped = stripOpenAIResponseInputItemIds(body.input);
  if (!stripped.changed) {
    return bodyText;
  }
  return JSON.stringify({ ...body, input: stripped.value });
}

function stripOpenAIResponseInputItemIds(input: readonly unknown[]): { readonly value: readonly unknown[]; readonly changed: boolean } {
  let changed = false;
  const output = input.map((item) => {
    if (!isPlainRecord(item) || !("id" in item)) {
      return item;
    }
    const stripped: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(item)) {
      if (key === "id") {
        continue;
      }
      stripped[key] = value;
    }
    changed = true;
    return stripped;
  });
  return { value: output, changed };
}

interface AllowedProviderURLRoot {
  readonly origin: string;
  readonly hostname: string;
  readonly pathname: string;
}

function allowedProviderURLRoots(baseURLs: readonly string[]): readonly AllowedProviderURLRoot[] {
  return baseURLs.map((baseURL) => {
    const parsed = new URL(baseURL);
    return {
      origin: parsed.origin,
      hostname: parsed.hostname,
      pathname: parsed.pathname,
    };
  });
}

function assertProviderEgressAllowed(url: string, allowedRoots: readonly AllowedProviderURLRoot[]): void {
  const parsed = new URL(url);
  if (parsed.protocol !== "https:" || !allowedRoots.some((root) => parsed.origin === root.origin && pathWithinProviderRoot(parsed.pathname, root.pathname))) {
    throw new TypeError("provider egress URL is not allowlisted");
  }
}

function assertProviderPreconnectAllowed(url: string, allowedRoots: readonly AllowedProviderURLRoot[]): void {
  const parsed = new URL(url);
  if (parsed.protocol !== "https:" || !allowedRoots.some((root) => parsed.hostname === root.hostname)) {
    throw new TypeError("provider egress URL is not allowlisted");
  }
}

function pathWithinProviderRoot(pathname: string, rootPathname: string): boolean {
  if (rootPathname === "/" || rootPathname === "") {
    return true;
  }
  const normalizedRoot = rootPathname.endsWith("/") ? rootPathname : `${rootPathname}/`;
  return pathname === rootPathname || pathname.startsWith(normalizedRoot);
}

function providerRedirectStatus(statusCode: number): boolean {
  return statusCode === 301 || statusCode === 302 || statusCode === 303 || statusCode === 307 || statusCode === 308;
}

function providerRedirectRequest(request: Request, location: string, statusCode: number, allowedRoots: readonly AllowedProviderURLRoot[]): Request {
  const redirectURL = new URL(location, request.url);
  assertProviderEgressAllowed(redirectURL.toString(), allowedRoots);
  const headers = new Headers(request.headers);
  if (redirectURL.origin !== new URL(request.url).origin) {
    headers.delete("authorization");
    headers.delete("cookie");
  }
  const switchToGet = statusCode === 303 || ((statusCode === 301 || statusCode === 302) && request.method === "POST");
  if (switchToGet) {
    headers.delete("content-length");
    headers.delete("content-encoding");
    headers.delete("content-language");
    headers.delete("content-location");
    headers.delete("content-type");
    return new Request(redirectURL, {
      method: "GET",
      headers,
      signal: request.signal,
      redirect: "manual",
    });
  }
  const body = request.method === "GET" || request.method === "HEAD" ? undefined : request.body;
  return new Request(redirectURL, {
    method: request.method,
    headers,
    ...(body === undefined ? {} : { body }),
    signal: request.signal,
    redirect: "manual",
  });
}

class ProviderFetchTimeoutError extends Error {
  readonly timeout = true;

  constructor(readonly kind: "inter_chunk" | "overall") {
    super(`provider ${kind.replace("_", "-")} timeout`);
    this.name = "ProviderFetchTimeoutError";
  }
}

function providerFetchTimers(input: {
  readonly callerSignal: AbortSignal;
  readonly registrySignal?: AbortSignal | undefined;
  readonly overallTimeoutMs?: number | undefined;
  readonly interChunkTimeoutMs: number;
}) {
  const overallController = new AbortController();
  const chunkController = new AbortController();
  let timeoutError: ProviderFetchTimeoutError | undefined;
  let overallTimer: ReturnType<typeof setTimeout> | undefined;
  let chunkTimer: ReturnType<typeof setTimeout> | undefined;
  const abortWithTimeout = (kind: ProviderFetchTimeoutError["kind"], controller: AbortController): void => {
    timeoutError = new ProviderFetchTimeoutError(kind);
    controller.abort(timeoutError);
  };
  if (input.overallTimeoutMs !== undefined && input.overallTimeoutMs > 0) {
    overallTimer = setTimeout(() => abortWithTimeout("overall", overallController), input.overallTimeoutMs);
  }
  const signals = [
    input.callerSignal,
    ...(input.registrySignal !== undefined ? [input.registrySignal] : []),
    overallController.signal,
    chunkController.signal,
  ];
  const signal = AbortSignal.any(signals);
  const clearChunk = (): void => {
    if (chunkTimer !== undefined) {
      clearTimeout(chunkTimer);
      chunkTimer = undefined;
    }
  };
  const clearAll = (): void => {
    clearChunk();
    if (overallTimer !== undefined) {
      clearTimeout(overallTimer);
      overallTimer = undefined;
    }
  };
  const armChunk = (): void => {
    clearChunk();
    if (input.interChunkTimeoutMs > 0) {
      chunkTimer = setTimeout(() => abortWithTimeout("inter_chunk", chunkController), input.interChunkTimeoutMs);
    }
  };
  return {
    signal,
    clearAll,
    armChunk,
    clearChunk,
    timeoutError: () => timeoutError,
  };
}

function wrapProviderResponseBody(
  response: Response,
  timers: ReturnType<typeof providerFetchTimers>,
  onTransportActivity: (() => void) | undefined,
): Response {
  if (response.body === null) {
    timers.clearAll();
    return response;
  }
  const reader = response.body.getReader();
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      timers.armChunk();
      timers.signal.addEventListener("abort", () => {
        const error = timers.timeoutError() ?? timers.signal.reason ?? new DOMException("Provider fetch aborted.", "AbortError");
        controller.error(error);
        void reader.cancel(error);
        timers.clearAll();
      }, { once: true });
    },
    async pull(controller) {
      try {
        const chunk = await reader.read();
        if (chunk.done) {
          timers.clearAll();
          controller.close();
          return;
        }
        timers.armChunk();
        onTransportActivity?.();
        controller.enqueue(chunk.value);
      } catch (error) {
        timers.clearAll();
        controller.error(timers.timeoutError() ?? error);
      }
    },
    cancel(reason) {
      timers.clearAll();
      return reader.cancel(reason);
    },
  });
  return new Response(body, {
    status: response.status,
    statusText: response.statusText,
    headers: response.headers,
  });
}

function toAIModelMessage(message: LoweredProviderMessage): ModelMessage {
  switch (message.role) {
    case "system": {
      const systemContent = aiSystemContent(message);
      return {
        role: "system",
        content: systemContent.text,
        ...(systemContent.providerOptions !== undefined ? { providerOptions: systemContent.providerOptions as ProviderOptions } : {}),
      };
    }
    case "user":
      return {
        role: "user",
        content: message.content.map(toAIUserPart) as Exclude<ModelMessageForRole<"user">["content"], string>,
        ...(message.providerOptions !== undefined ? { providerOptions: message.providerOptions as ProviderOptions } : {}),
      };
    case "assistant": {
      return {
        role: "assistant",
        content: message.content.map(toAIAssistantPart) as ModelMessageForRole<"assistant">["content"],
        ...(message.providerOptions !== undefined ? { providerOptions: message.providerOptions as ProviderOptions } : {}),
      };
    }
    case "tool":
      return {
        role: "tool",
        content: message.content.map(toAIToolResultPart),
        ...(message.providerOptions !== undefined ? { providerOptions: message.providerOptions as ProviderOptions } : {}),
      };
  }
}

function aiSystemContent(message: Extract<LoweredProviderMessage, { role: "system" }>): {
  readonly text: string;
  readonly providerOptions?: Readonly<Record<string, unknown>>;
} {
  if (typeof message.content === "string") {
    return {
      text: message.content,
      ...(message.providerOptions !== undefined ? { providerOptions: message.providerOptions } : {}),
    };
  }
  const text = message.content.map((part) => part.text).join("");
  const providerOptions = message.content.reduce<Readonly<Record<string, unknown>> | undefined>((current, part) => {
    if (part.providerOptions === undefined) {
      return current;
    }
    return current === undefined ? part.providerOptions : mergePartProviderOptions(current, part.providerOptions);
  }, message.providerOptions);
  return {
    text,
    ...(providerOptions !== undefined ? { providerOptions } : {}),
  };
}

function openAIResponsesCallShape(
  lowered: ReturnType<typeof lowerProviderRequest>,
  oauth: boolean,
): { readonly messages: readonly ModelMessage[]; readonly providerOptions: Readonly<Record<string, unknown>> } {
  const messages = lowered.messages.map(toAIModelMessage);
  if (!oauth) {
    return { messages, providerOptions: lowered.options.providerOptions };
  }
  const instructions = lowered.messages
    .filter((message): message is Extract<LoweredProviderMessage, { role: "system" }> => message.role === "system")
    .map((message) => aiSystemContent(message).text)
    .filter((text) => text.length > 0)
    .join("\n");
  return {
    messages: messages.filter((message) => message.role !== "system"),
    providerOptions: instructions.length === 0
      ? lowered.options.providerOptions
      : mergePartProviderOptions(lowered.options.providerOptions, { openai: { instructions } }),
  };
}

function toAIUserPart(part: LoweredUserContentPart): ModelMessageForRole<"user">["content"][number] {
  switch (part.type) {
    case "text":
      return {
        type: "text",
        text: part.text,
        ...(part.providerOptions !== undefined ? { providerOptions: part.providerOptions as ProviderOptions } : {}),
      };
    case "image":
      return {
        type: "image",
        image: part.image,
        mediaType: part.mediaType,
        ...(part.providerOptions !== undefined ? { providerOptions: part.providerOptions as ProviderOptions } : {}),
      };
    case "file":
      return {
        type: "file",
        data: part.data,
        filename: part.filename,
        mediaType: part.mediaType,
        ...(part.providerOptions !== undefined ? { providerOptions: part.providerOptions as ProviderOptions } : {}),
      };
  }
}

function toAIAssistantPart(part: LoweredAssistantContentPart): ModelMessageForRole<"assistant">["content"][number] {
  switch (part.type) {
    case "text":
      return {
        type: "text",
        text: part.text,
        ...(part.providerOptions !== undefined ? { providerOptions: part.providerOptions as ProviderOptions } : {}),
      };
    case "reasoning":
      return {
        type: "reasoning",
        text: part.text,
        providerOptions: mergePartProviderOptions(part.providerMetadata, part.providerOptions) as ProviderOptions,
      };
    case "tool-call":
      return {
        type: "tool-call",
        toolCallId: part.toolCallId,
        toolName: part.toolName,
        input: jsonValue(part.input),
        ...(part.providerOptions !== undefined ? { providerOptions: part.providerOptions as ProviderOptions } : {}),
      };
  }
}

function toAIToolResultPart(part: LoweredToolResultPart): ModelMessageForRole<"tool">["content"][number] {
  return {
    type: "tool-result",
    toolCallId: part.toolCallId,
    toolName: part.toolName,
    output: {
      type: part.isError === true ? "error-json" : "json",
      value: jsonValue(part.output),
    },
    ...(part.providerOptions !== undefined ? { providerOptions: part.providerOptions as ProviderOptions } : {}),
  };
}

function mergePartProviderOptions(
  left: Readonly<Record<string, unknown>>,
  right: Readonly<Record<string, unknown>> | undefined,
): Readonly<Record<string, unknown>> {
  if (right === undefined) {
    return left;
  }
  const merged: Record<string, unknown> = { ...left };
  for (const [key, value] of Object.entries(right)) {
    const existing = merged[key];
    if (isPlainRecord(existing) && isPlainRecord(value)) {
      merged[key] = { ...existing, ...value };
      continue;
    }
    merged[key] = value;
  }
  return merged;
}

function isPlainRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function toAITools(
  tools: Readonly<Record<string, LoweredToolDefinition>>,
  customTool?: NonNullable<OpenAIProvider["tools"]>["customTool"],
): ToolSet | undefined {
  const entries = Object.entries(tools);
  if (entries.length === 0) {
    return undefined;
  }
  return Object.fromEntries(entries.map(([name, definition]) => {
    if (definition.kind === "freeform") {
      if (customTool === undefined) {
        throw new Error("gateway_protocol_error: freeform tool adapter is unavailable");
      }
      return [name, customTool({
        name,
        description: definition.description,
        format: { type: "grammar", syntax: "lark", definition: definition.larkGrammar },
      })];
    }
    return [name, {
      description: definition.description,
      inputSchema: jsonSchema(definition.inputSchema.schema as Parameters<typeof jsonSchema>[0]),
      ...(toolStrictOption(definition.providerOptions) !== undefined ? { strict: toolStrictOption(definition.providerOptions) } : {}),
      ...(definition.providerOptions !== undefined ? { providerOptions: definition.providerOptions as ProviderOptions } : {}),
    } as Tool<unknown, never>];
  })) as ToolSet;
}

function toAIOutput(structuredOutput: NonNullable<ReturnType<typeof lowerProviderRequest>["structuredOutput"]>): ReturnType<typeof Output.object> {
  return Output.object({
    schema: jsonSchema(structuredOutput.schema.schema as Parameters<typeof jsonSchema>[0]),
  });
}

function toolStrictOption(providerOptions: Readonly<Record<string, unknown>> | undefined): false | undefined {
  const openaiOptions = isPlainRecord(providerOptions?.openai) ? providerOptions.openai : undefined;
  return openaiOptions?.strict === false ? false : undefined;
}

type ModelMessageForRole<ROLE extends ModelMessage["role"]> = Extract<ModelMessage, { role: ROLE }>;

function toGatewayStreamPart(
  part: TextStreamPart<ToolSet>,
  credential: ResolvedProviderAPIKeyCredential | ResolvedOpenAIOAuthCredential,
  providerId: GatewayCatalogProviderId,
): GatewayStreamPart {
  switch (part.type) {
    case "text-start":
      return { type: "text-start", id: part.id, metadata: part.providerMetadata };
    case "text-delta":
      return { type: "text-delta", id: part.id, textDelta: part.text, metadata: part.providerMetadata };
    case "text-end":
      return { type: "text-end", id: part.id, metadata: part.providerMetadata };
    case "reasoning-start":
      return { type: "reasoning-start", id: part.id, metadata: part.providerMetadata };
    case "reasoning-delta":
      return { type: "reasoning-delta", id: part.id, textDelta: part.text, metadata: part.providerMetadata };
    case "reasoning-end":
      return { type: "reasoning-end", id: part.id, metadata: part.providerMetadata };
    case "tool-input-start":
      return { type: "tool-input-start", id: part.id, name: part.toolName, metadata: part.providerMetadata };
    case "tool-input-delta":
      return { type: "tool-input-delta", id: part.id, delta: part.delta, metadata: part.providerMetadata };
    case "tool-input-end":
      return { type: "tool-input-end", id: part.id, metadata: part.providerMetadata };
    case "tool-call":
      return { type: "tool-call", id: part.toolCallId, name: part.toolName, input: part.input, metadata: part.providerMetadata };
    case "finish":
      return {
        type: "finish",
        finishReason: part.finishReason,
        usage: part.totalUsage,
        metadata: {
          credential_source: credential.source,
          ...(part.rawFinishReason !== undefined ? { raw_finish_reason: part.rawFinishReason } : {}),
        },
      };
    case "finish-step":
      return { type: "finish-step", usage: part.usage };
    case "error":
      throw classifiedProviderError(providerId, part.error);
    case "abort":
      throw new DOMException(part.reason ?? "Provider stream aborted.", "AbortError");
    case "start":
    case "start-step":
    case "raw":
    case "source":
    case "file":
    case "tool-result":
    case "tool-error":
    case "tool-output-denied":
    case "tool-approval-request":
      return { type: "raw", value: part };
  }
}

function classifiedProviderError(providerId: GatewayCatalogProviderId, error: unknown): ProviderKeyFailureError {
  const input = providerFailureInput(error);
  return new ProviderKeyFailureError(
    classifyProviderFailure(providerId, input),
    input.networkError === true || input.timeout === true || input.statusCode === undefined
      ? "transport_failure"
      : "http_rejection",
  );
}

function isTerminalProviderStreamEvent(event: ProviderStreamEvent): boolean {
  return event.type === ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH ||
    event.type === ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR;
}

function providerFailureInput(error: unknown): Parameters<typeof classifyProviderFailure>[1] {
  if (APICallError.isInstance(error)) {
    return {
      statusCode: error.statusCode,
      body: error.data ?? error.responseBody,
      headers: error.responseHeaders,
    };
  }
  if (error instanceof DOMException && error.name === "AbortError") {
    return { networkError: true };
  }
  if (error instanceof TypeError) {
    return { networkError: true };
  }
  const record = typeof error === "object" && error !== null ? error as Record<string, unknown> : {};
  return {
    statusCode: typeof record.statusCode === "number" ? record.statusCode : undefined,
    body: record.data ?? record.responseBody ?? record.body,
    headers: providerHeaders(record.responseHeaders ?? record.headers),
    networkError: record.networkError === true,
    timeout: record.timeout === true,
  };
}

function providerHeaders(value: unknown): Readonly<Record<string, string | undefined>> | undefined {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return undefined;
  }
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>)
      .filter((entry): entry is [string, string] => typeof entry[1] === "string")
      .map(([key, headerValue]) => [key.toLowerCase(), headerValue]),
  );
}

function jsonValue(value: unknown): JSONValue {
  if (value === null || typeof value === "string" || typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }
  if (Array.isArray(value)) {
    return value.map(jsonValue);
  }
  if (typeof value === "object") {
    return Object.fromEntries(Object.entries(value as Record<string, unknown>).map(([key, entry]) => [key, jsonValue(entry)]));
  }
  return null;
}
