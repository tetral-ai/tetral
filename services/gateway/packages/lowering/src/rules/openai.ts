/**
 * @packageDocumentation
 * Defines the fixed request-lowering policies for the approved OpenAI models. It guards encrypted
 * reasoning continuity, stateless request options, prompt-cache identity, non-strict tool schemas,
 * supported media, explicit effort defaults, omitted output-token fields, schema normalization,
 * sampling defaults, and provider-specific error metadata. The rules registry imports these
 * records, provider-gateway selects one through the registry, and the request lowerer consumes it.
 * This module depends only on the shared ProviderRules shape and performs no I/O or runtime calls.
 */
import type { ProviderRules } from "./rules.js";

export const OpenAIRuleIds = [
  "openai-reasoning-metadata",
  "openai-effort-lowering",
  "openai-request-options",
  "openai-tool-strictness",
  "openai-base-reasoning",
  "openai-output-cap-omission",
  "openai-media-lowering",
  "openai-header-absence",
  "openai-sampling-defaults",
  "openai-schema-lowering",
  "openai-error-mapping",
  "openai-output-cap",
] as const;

/** Default reasoning effort sent when the GPT 5.5 model has no explicit variant. */
export const OpenAIGPT55DefaultEffort = "medium" as const;

/** Complete lowering policy for the approved GPT 5.5 model. */
export const OpenAIGPT55Rules: ProviderRules = {
  ruleIds: OpenAIRuleIds,
  providerId: "openai",
  modelId: "gpt-5.5",
  providerFamily: "openai",
  reasoning: {
    strategy: "provider-metadata",
    metadataKey: "openai",
    preserveEmptySignedReasoning: false,
    preserveEmptyProviderMetadata: true,
    requireProviderMetadataContent: true,
    unmatchedReasoning: "text",
    stripProviderMetadataKeys: ["id", "itemId", "item_id"],
  },
  scrubToolCallIds: false,
  cacheControl: undefined,
  supportedMedia: {
    exactMimes: ["application/pdf"],
    mimePrefixes: ["image/"],
  },
  effort: {
    allowed: ["none", "low", "medium", "high", "xhigh"],
    noEffortVariants: [""],
    providerKey: "openai",
    providerOptionKey: "reasoningEffort",
  },
  requestOptions: {
    providerKey: "openai",
    promptCacheKeyField: "promptCacheKey",
    promptCacheKeySource: "session_id",
  },
  toolOptions: {
    providerKey: "openai",
    strict: false,
  },
  freeformTools: "openai-custom",
  providerOptions: {
    openai: {
      store: false,
      promptCacheKey: "",
      textVerbosity: "low",
      reasoningEffort: OpenAIGPT55DefaultEffort,
      reasoningSummary: "auto",
      include: ["reasoning.encrypted_content"],
      forceReasoning: true,
    },
  },
  dynamicProviderOptions: undefined,
  // Official API-key mode deliberately sends no OpenCode identification
  // headers. OAuth subscription transport owns its header swap in
  // provider-gateway/openai-oauth.ts.
  headers: {},
  sampling: {
    temperature: undefined,
    topP: undefined,
    topK: undefined,
  },
  maxOutputTokens: "omit",
  schemaStrategy: "openai-codex",
  requestOutputSchema: undefined,
  providerSpecificErrorRules: [
    "404_retryable",
    "usage_not_included_non_retryable",
    "insufficient_quota_non_retryable",
    "invalid_prompt_non_retryable",
    "server_is_overloaded_retryable",
    "server_error_retryable",
  ],
};

/** Default reasoning effort sent when the GPT 5.6 solution model has no explicit variant. */
export const OpenAIGPT56SolDefaultEffort = "medium" as const;
/** Accepted explicit reasoning-effort variants for the GPT 5.6 solution model. */
export const OpenAIGPT56SolEffortTiers = ["none", "low", "medium", "high", "xhigh"] as const;

/** Complete lowering policy for the approved GPT 5.6 solution model. */
export const OpenAIGPT56SolRules: ProviderRules = {
  ...OpenAIGPT55Rules,
  modelId: "gpt-5.6-sol",
  effort: {
    ...OpenAIGPT55Rules.effort!,
    allowed: OpenAIGPT56SolEffortTiers,
  },
  providerOptions: {
    openai: {
      store: false,
      promptCacheKey: "",
      textVerbosity: "low",
      reasoningEffort: OpenAIGPT56SolDefaultEffort,
      reasoningSummary: "auto",
      include: ["reasoning.encrypted_content"],
      forceReasoning: true,
    },
  },
};
