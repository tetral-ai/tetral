/**
 * @packageDocumentation
 * Defines the fixed request-lowering policies for the approved Anthropic models. It guards the
 * Anthropic transport family, signed-reasoning preservation, tool-call identifier scrubbing,
 * message-level cache placement, supported media, adaptive thinking, sampling defaults, and
 * output-token handling for each exact model. The rules registry imports these records,
 * provider-gateway selects one through the registry, and the request lowerer consumes it. This
 * module depends only on the shared ProviderRules shape and performs no I/O or runtime calls.
 */
import type { ProviderRules } from "./rules.js";

export const AnthropicRuleIds = [
  "anthropic-content-normalization",
  "anthropic-tool-id-scrubbing",
  "anthropic-media-lowering",
  "anthropic-cache-placement",
  "anthropic-adaptive-thinking",
  "anthropic-reasoning-metadata",
  "anthropic-beta-header",
  "anthropic-sampling-defaults",
  "anthropic-schema-passthrough",
  "anthropic-error-mapping",
  "anthropic-output-cap",
] as const;

// https://platform.claude.com/docs/en/build-with-claude/effort recommends
// xhigh for Opus 4.8 coding and agentic workloads.
/** Default reasoning effort sent when the Opus model has no explicit variant. */
export const AnthropicOpus48DefaultEffort = "xhigh" as const;

// The same official guide pins high as the Claude Fable 5 default.
/** Default reasoning effort sent when the Fable model has no explicit variant. */
export const AnthropicFable5DefaultEffort = "high" as const;

/** Complete lowering policy for the approved Opus model on the Anthropic transport. */
export const AnthropicOpus48Rules: ProviderRules = {
  ruleIds: AnthropicRuleIds,
  providerId: "anthropic",
  modelId: "claude-opus-4-8",
  providerFamily: "anthropic",
  reasoning: {
    strategy: "provider-metadata",
    metadataKey: "anthropic",
    preserveEmptySignedReasoning: true,
    preserveEmptyProviderMetadata: false,
    requireProviderMetadataContent: false,
    unmatchedReasoning: "text",
  },
  scrubToolCallIds: true,
  cacheControl: {
    placement: "message",
    providerKey: "anthropic",
    firstSystemMessages: 2,
    lastNonSystemMessages: 2,
  },
  supportedMedia: {
    exactMimes: ["application/pdf", "text/plain"],
    mimePrefixes: ["image/"],
  },
  effort: {
    allowed: ["low", "medium", "high", "xhigh", "max"],
    noEffortVariants: [""],
    providerKey: "anthropic",
    providerOptionKey: "effort",
  },
  requestOptions: undefined,
  toolOptions: undefined,
  providerOptions: {
    anthropic: {
      thinking: { type: "adaptive", display: "summarized" },
      effort: AnthropicOpus48DefaultEffort,
      sendReasoning: true,
    },
  },
  dynamicProviderOptions: undefined,
  headers: {
    "anthropic-beta": "interleaved-thinking-2025-05-14,fine-grained-tool-streaming-2025-05-14",
  },
  sampling: {
    temperature: undefined,
    topP: undefined,
    topK: undefined,
  },
  maxOutputTokens: "clamp",
  schemaStrategy: "passthrough",
  requestOutputSchema: "approval-reviewer",
  providerSpecificErrorRules: [],
};

/** Complete lowering policy for the approved Fable model with its model-specific effort default. */
export const AnthropicFable5Rules: ProviderRules = {
  ...AnthropicOpus48Rules,
  modelId: "claude-fable-5",
  providerOptions: {
    anthropic: {
      thinking: { type: "adaptive", display: "summarized" },
      effort: AnthropicFable5DefaultEffort,
      sendReasoning: true,
    },
  },
};
