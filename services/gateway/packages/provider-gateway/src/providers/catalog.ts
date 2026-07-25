/**
 * Defines the provider gateway's closed registry of accepted provider/model
 * pairs, supply routes, endpoints, and route-effective token limits.
 *
 * Lookups require an exact provider and model pair and never substitute a
 * model. `platformHosted` controls eligibility for platform-owned credentials,
 * while supply-mode overrides can narrow context and input limits without
 * changing an entry's output limit. The registry is static and performs no
 * I/O.
 *
 * The gateway service uses exact lookup for request admission. Credential
 * resolution consumes the selected supply provider plus platform-hosting
 * eligibility, while provider clients consume endpoint, wire-model, and supply-
 * mode fields; limit selection consumes the declared overrides. The complete
 * route record also retains provider family even though current runtime branches
 * do not read that field. This module has no runtime callees outside its own pure
 * lookup helpers.
 *
 * @packageDocumentation
 */
/** A concrete credential and endpoint route supported by the gateway. */
export type GatewaySupplyMode =
  | "anthropic-api-key"
  | "openai-api-key"
  | "openai-chatgpt-oauth"
  | "deepseek-api-key"
  | "moonshotai-code-api-key"
  | "zai-coding-api-key";

/** Provider identifiers accepted by credential resolution and client construction. */
export type GatewaySupplyProviderId = "anthropic" | "openai" | "deepseek" | "moonshotai" | "zai";

/**
 * One admitted provider/model pair and its complete declared supply, transport,
 * family, and limit facts; current consumers deliberately read only their slice.
 */
export interface GatewayModelCatalogEntry {
  readonly providerId: string;
  readonly modelId: string;
  readonly apiModelId: string;
  readonly supplyProviderId: GatewaySupplyProviderId;
  readonly providerFamily: "anthropic" | "openai" | "openai-compatible";
  readonly supplyModes: readonly GatewaySupplyMode[];
  readonly baseURLs: readonly string[];
  readonly platformHosted: boolean;
  readonly modelContextWindowTokens: number;
  readonly modelOutputTokenLimit: number;
  readonly supplyModeLimitOverrides?: Readonly<Partial<Record<GatewaySupplyMode, {
    readonly contextWindowTokens: number;
    readonly inputLimitTokens?: number | undefined;
  }>>> | undefined;
}

/** Token limits effective for one catalog entry and selected supply route. */
export interface GatewayModelLimits {
  readonly contextWindowTokens: number;
  readonly inputLimitTokens?: number | undefined;
  readonly outputTokenLimit: number;
}

/** The complete seven-entry provider/model allowlist accepted by the gateway. */
export const GatewayModelCatalog: readonly GatewayModelCatalogEntry[] = [
  {
    providerId: "openai",
    modelId: "gpt-5.5",
    apiModelId: "gpt-5.5",
    supplyProviderId: "openai",
    providerFamily: "openai",
    supplyModes: ["openai-api-key", "openai-chatgpt-oauth"],
    baseURLs: ["https://api.openai.com", "https://chatgpt.com/backend-api/codex/responses"],
    platformHosted: true,
    // VERIFY-DOCS 2026-07-20:
    // https://developers.openai.com/api/docs/models/gpt-5.5 pins the
    // official API route at 1,050,000 context tokens.
    modelContextWindowTokens: 1_050_000,
    modelOutputTokenLimit: 128_000,
    // VERIFY-UPSTREAM 2026-07-20:
    // https://github.com/anomalyco/opencode/blob/4a81e8392b4c18cbcc0914527bdab8ff94b9a434/packages/opencode/src/plugin/openai/codex.ts
    // pins the ChatGPT OAuth/Codex route at a narrower 400,000-token context
    // with a separate 272,000-token input limit.
    supplyModeLimitOverrides: {
      "openai-chatgpt-oauth": {
        contextWindowTokens: 400_000,
        inputLimitTokens: 272_000,
      },
    },
  },
  {
    providerId: "openai",
    modelId: "gpt-5.6-sol",
    apiModelId: "gpt-5.6-sol",
    supplyProviderId: "openai",
    providerFamily: "openai",
    supplyModes: ["openai-api-key", "openai-chatgpt-oauth"],
    baseURLs: ["https://api.openai.com", "https://chatgpt.com/backend-api/codex/responses"],
    platformHosted: true,
    // VERIFY-DOCS 2026-07-19:
    // https://developers.openai.com/api/docs/models/gpt-5.6-sol
    // pins the official Responses model id, 1,050,000-token context, and
    // 128,000-token maximum output.
    modelContextWindowTokens: 1_050_000,
    modelOutputTokenLimit: 128_000,
    // VERIFY-UPSTREAM 2026-07-20:
    // https://github.com/anomalyco/opencode/blob/4a81e8392b4c18cbcc0914527bdab8ff94b9a434/packages/opencode/src/plugin/openai/codex.ts
    // pins the ChatGPT OAuth/Codex route at a narrower 500,000-token context
    // with a separate 372,000-token input limit.
    // Derivation: OpenCode packages/opencode/test/plugin/codex.test.ts:184
    // freezes { context: 500_000, input: 372_000, output: 128_000 }.
    supplyModeLimitOverrides: {
      "openai-chatgpt-oauth": {
        contextWindowTokens: 500_000,
        inputLimitTokens: 372_000,
      },
    },
  },
  {
    providerId: "anthropic",
    modelId: "claude-opus-4-8",
    apiModelId: "claude-opus-4-8",
    supplyProviderId: "anthropic",
    providerFamily: "anthropic",
    supplyModes: ["anthropic-api-key"],
    baseURLs: ["https://api.anthropic.com"],
    platformHosted: true,
    // VERIFY-DOCS 2026-07-20:
    // https://platform.claude.com/docs/en/about-claude/models/overview
    // pins the default 1M-token context window for Claude Opus 4.8.
    modelContextWindowTokens: 1_000_000,
    modelOutputTokenLimit: 128_000,
  },
  {
    providerId: "anthropic",
    modelId: "claude-fable-5",
    apiModelId: "claude-fable-5",
    supplyProviderId: "anthropic",
    providerFamily: "anthropic",
    supplyModes: ["anthropic-api-key"],
    baseURLs: ["https://api.anthropic.com"],
    platformHosted: true,
    // VERIFY-DOCS 2026-07-19:
    // https://platform.claude.com/docs/en/about-claude/models/overview
    // pins the official model id, 1M-token context, and 128k maximum output.
    modelContextWindowTokens: 1_000_000,
    modelOutputTokenLimit: 128_000,
  },
  {
    providerId: "deepseek",
    modelId: "deepseek-v4-pro",
    apiModelId: "deepseek-v4-pro",
    supplyProviderId: "deepseek",
    providerFamily: "openai-compatible",
    supplyModes: ["deepseek-api-key"],
    // VERIFY-DOCS 2026-07-05:
    // https://api-docs.deepseek.com/quick_start/pricing lists deepseek-v4-pro
    // with OpenAI-format base URL https://api.deepseek.com.
    // https://api-docs.deepseek.com/api/list-models confirms Bearer auth for
    // the /models surface, and https://api-docs.deepseek.com/api/deepseek-api
    // defines the platform authentication scheme as HTTP Bearer.
    // Keep the openai-compatible instance name stable as "deepseek" so
    // lowering continues to use providerOptions.deepseek.*.
    baseURLs: ["https://api.deepseek.com"],
    platformHosted: true,
    // VERIFY-DOCS 2026-07-20:
    // https://api-docs.deepseek.com/quick_start/pricing pins the
    // 1,000,000-token context window for deepseek-v4-pro; models.dev's
    // first-party DeepSeek catalog entry corroborates the same value.
    modelContextWindowTokens: 1_000_000,
    modelOutputTokenLimit: 384_000,
  },
  {
    providerId: "moonshotai",
    modelId: "kimi-k3",
    apiModelId: "k3",
    supplyProviderId: "moonshotai",
    providerFamily: "anthropic",
    supplyModes: ["moonshotai-code-api-key"],
    baseURLs: ["https://api.kimi.com/coding/v1"],
    platformHosted: false,
    // VERIFY-LIVE 2026-07-19: GET https://api.kimi.com/coding/v1/models
    // returned HTTP 200 and the sanitized K3 row
    // {"id":"k3","context_length":1048576}, with no output-cap field.
    // PLATFORM-CHOSEN: Kimi publishes no separate output cap, so Tetral uses
    // this finite generation ceiling.
    modelContextWindowTokens: 1_048_576,
    modelOutputTokenLimit: 131_072,
  },
  {
    providerId: "zai",
    modelId: "glm-5.2",
    apiModelId: "glm-5.2",
    supplyProviderId: "zai",
    providerFamily: "openai-compatible",
    supplyModes: ["zai-coding-api-key"],
    // PROVIDER-DOCS 2026-07-20:
    // https://docs.z.ai/guides/llm/glm-5.2 and the NVIDIA NIM GLM-5.2
    // model profile pin the 1,048,576-token context. The Z.AI
    // chat-completion schema pins the 131,072-token output cap.
    baseURLs: ["https://api.z.ai/api/coding/paas/v4"],
    platformHosted: false,
    modelContextWindowTokens: 1_048_576,
    modelOutputTokenLimit: 131_072,
  },
];

/** Returns the exact catalog entry for a provider/model pair, with no fallback. */
export function lookupGatewayModel(providerId: string, modelId: string): GatewayModelCatalogEntry | undefined {
  return GatewayModelCatalog.find((entry) => entry.providerId === providerId && entry.modelId === modelId);
}

/** Resolves route-specific context and input limits while preserving the model output limit. */
export function routeEffectiveGatewayModelLimits(
  entry: GatewayModelCatalogEntry,
  supplyMode: GatewaySupplyMode,
): GatewayModelLimits {
  const override = entry.supplyModeLimitOverrides?.[supplyMode];
  return {
    contextWindowTokens: override?.contextWindowTokens ?? entry.modelContextWindowTokens,
    inputLimitTokens: override?.inputLimitTokens,
    outputTokenLimit: entry.modelOutputTokenLimit,
  };
}
