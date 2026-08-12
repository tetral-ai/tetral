/**
 * @packageDocumentation
 * Defines the fixed request-lowering policy for the approved Moonshot model on its
 * Anthropic-shaped transport. It guards signed-reasoning replay, content-level cache placement,
 * accepted media, transport-native reasoning controls, disabled tool streaming, absent sampling
 * overrides, schema normalization, and output-token clamping. The rules registry imports this
 * record, provider-gateway selects it through the registry, and the request lowerer consumes it.
 * This module depends only on the shared ProviderRules shape and performs no I/O or runtime calls.
 */
import type { ProviderRules } from "./rules.js";

export const MoonshotKimiK3RuleIds = [
  "kimi-content-media-lowering",
  "kimi-cache-placement",
  "kimi-tool-streaming",
  "kimi-beta-effort-absence",
  "kimi-sampling-defaults",
  "kimi-schema-lowering",
  "kimi-error-mapping",
  "kimi-terminal-cache-usage",
  "kimi-output-cap",
] as const;

// K3's only supported "max" reasoning posture is the coding endpoint's
// transport-native default. The Anthropic-shaped route sends no effort or
// thinking field; this frozen absence prevents accidental wire invention.
/** Marks that the model relies on its transport-native reasoning posture without an effort field. */
export const MoonshotKimiK3DefaultEffort = undefined;

/** Complete lowering policy for the approved Moonshot model. */
export const MoonshotKimiK3Rules: ProviderRules = {
  ruleIds: MoonshotKimiK3RuleIds,
  providerId: "moonshotai",
  modelId: "kimi-k3",
  providerFamily: "anthropic",
  reasoning: {
    strategy: "provider-metadata",
    metadataKey: "anthropic",
    preserveEmptySignedReasoning: true,
    preserveEmptyProviderMetadata: false,
    requireProviderMetadataContent: false,
    unmatchedReasoning: "text",
  },
  scrubToolCallIds: false,
  cacheControl: {
    placement: "content",
    providerKey: "anthropic",
    firstSystemMessages: 2,
    lastNonSystemMessages: 2,
  },
  supportedMedia: {
    exactMimes: ["application/pdf", "text/plain"],
    mimePrefixes: ["image/"],
  },
  effort: undefined,
  requestOptions: undefined,
  toolOptions: undefined,
  freeformTools: undefined,
  providerOptions: {
    anthropic: {
      sendReasoning: true,
      toolStreaming: false,
    },
  },
  dynamicProviderOptions: undefined,
  headers: {},
  sampling: {
    // Evaluated from OpenCode transform.ts for the exact id "kimi-k3":
    // K3 matches neither the K2 sampling branch nor any explicit top-p/top-k branch.
    temperature: undefined,
    topP: undefined,
    topK: undefined,
  },
  maxOutputTokens: "clamp",
  schemaStrategy: "moonshot",
  structuredOutputStrategy: "unsupported",
  providerSpecificErrorRules: [],
};
