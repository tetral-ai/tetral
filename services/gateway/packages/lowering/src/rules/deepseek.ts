/**
 * @packageDocumentation
 * Defines the fixed request-lowering policy for the approved DeepSeek model. It guards the
 * openai-compatible transport choice, required assistant reasoning continuity, typed
 * reasoning-content remapping, unsupported-media behavior, explicit effort defaults, schema
 * passthrough, and output-token clamping. The rules registry imports this record,
 * provider-gateway selects it through the registry, and the request lowerer consumes it. This
 * module depends only on the shared ProviderRules shape and performs no I/O or runtime calls.
 */
import type { ProviderRules } from "./rules.js";

export const DeepSeekRuleIds = [
  "deepseek-reasoning-default",
  "deepseek-reasoning-remap",
  "deepseek-unsupported-media",
  "deepseek-provider-options",
  "deepseek-effort-lowering",
  "deepseek-sampling-defaults",
  "deepseek-cache-absence",
  "deepseek-schema-passthrough",
  "deepseek-error-mapping",
  "deepseek-output-cap",
] as const;

// https://api-docs.deepseek.com/guides/thinking_mode pins high as the
// default reasoning effort for regular deepseek-v4-pro requests.
/** Default reasoning effort sent when the DeepSeek model has no explicit variant. */
export const DeepSeekV4ProDefaultEffort = "high" as const;

/** Complete lowering policy for the approved DeepSeek model. */
export const DeepSeekV4ProRules: ProviderRules = {
  ruleIds: DeepSeekRuleIds,
  providerId: "deepseek",
  modelId: "deepseek-v4-pro",
  providerFamily: "openai-compatible",
  reasoning: {
    strategy: "provider-options",
    providerKey: "deepseek",
    fieldName: "reasoning_content",
    ensureAssistantReasoning: true,
  },
  scrubToolCallIds: false,
  cacheControl: undefined,
  supportedMedia: {
    exactMimes: [],
    mimePrefixes: [],
  },
  effort: {
    allowed: ["low", "medium", "high", "max"],
    noEffortVariants: [""],
    providerKey: "deepseek",
    providerOptionKey: "reasoningEffort",
  },
  requestOptions: undefined,
  toolOptions: undefined,
  providerOptions: {
    deepseek: {
      reasoningEffort: DeepSeekV4ProDefaultEffort,
    },
  },
  dynamicProviderOptions: undefined,
  headers: {},
  sampling: {
    temperature: undefined,
    topP: undefined,
    topK: undefined,
  },
  maxOutputTokens: "clamp",
  schemaStrategy: "passthrough",
  requestOutputSchema: undefined,
  providerSpecificErrorRules: [],
};
