/**
 * @packageDocumentation
 * Defines the fixed request-lowering policy for the approved Z.AI model. It guards the
 * openai-compatible transport choice, typed reasoning-content remapping, enabled thinking mode,
 * explicit effort tiers, unsupported-media behavior, absent cache and sampling overrides, schema
 * passthrough, and output-token clamping. The rules registry imports this record,
 * provider-gateway selects it through the registry, and the request lowerer consumes it. This
 * module depends only on the shared ProviderRules shape and performs no I/O or runtime calls.
 */
import type { ProviderRules } from "./rules.js";

export const ZaiGLM52RuleIds = [
  "zai-unsupported-media",
  "zai-reasoning-remap",
  "zai-thinking-mode",
  "zai-effort-lowering",
  "zai-cache-absence",
  "zai-sampling-defaults",
  "zai-schema-passthrough",
  "zai-error-mapping",
  "zai-output-cap",
] as const;

// https://docs.z.ai/api-reference/llm/chat-completion documents high/max reasoning tiers;
// this platform sends the lower tier when no explicit variant is present.
/** Default reasoning effort sent when the Z.AI model has no explicit variant. */
export const ZaiGLM52DefaultEffort = "high" as const;

/** Complete lowering policy for the approved Z.AI model. */
export const ZaiGLM52Rules: ProviderRules = {
  ruleIds: ZaiGLM52RuleIds,
  providerId: "zai",
  modelId: "glm-5.2",
  providerFamily: "openai-compatible",
  reasoning: {
    // VERIFY-DOCS: official Z.AI thinking/streaming docs say GLM-5.2 preserves reasoning_content
    // and streaming deltas include delta.reasoning_content; use the same typed
    // metadata remap as DeepSeek under the zai key.
    // Sources: https://docs.z.ai/guides/capabilities/thinking-mode and https://docs.z.ai/guides/capabilities/streaming.
    strategy: "provider-options",
    providerKey: "zai",
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
    allowed: ["high", "max"],
    noEffortVariants: [""],
    providerKey: "zai",
    providerOptionKey: "reasoningEffort",
  },
  requestOptions: undefined,
  toolOptions: undefined,
  providerOptions: {
    zai: {
      thinking: { type: "enabled", clear_thinking: false },
      reasoningEffort: ZaiGLM52DefaultEffort,
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
