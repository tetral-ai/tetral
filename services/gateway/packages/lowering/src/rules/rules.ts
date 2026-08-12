/**
 * @packageDocumentation
 * Defines the readonly policy vocabulary used to lower one exact provider/model pair. It guards
 * closed transport-family and strategy choices, keeps optional behaviors explicit, and separates
 * message normalization, caching, media, effort, provider options, schema, sampling, output, and
 * error metadata into typed fields. Provider policy modules implement these shapes, the rules
 * registry exposes their records, and the request lowerer consumes them without this module
 * calling any service or performing I/O.
 */
/**
 * Readonly lowering policy for one exact approved provider/model pair.
 *
 * The request lowerer uses the discriminated fields to produce provider messages and call options;
 * coverage checks use `ruleIds` and `providerSpecificErrorRules` as descriptive metadata.
 */
export interface ProviderRules {
  readonly ruleIds: readonly string[];
  readonly providerId: string;
  readonly modelId: string;
  readonly providerFamily: "anthropic" | "openai" | "openai-compatible";
  readonly reasoning: ProviderReasoningRules;
  readonly scrubToolCallIds: boolean;
  readonly cacheControl: ProviderCacheControlRules | undefined;
  readonly supportedMedia: ProviderMediaSupport;
  readonly effort: ProviderEffortRules | undefined;
  readonly requestOptions: ProviderRequestOptionRules | undefined;
  readonly toolOptions: ProviderToolOptionRules | undefined;
  /** Provider-visible freeform declaration supported by this exact route. */
  readonly freeformTools: "openai-custom" | undefined;
  readonly providerOptions: Readonly<Record<string, unknown>>;
  readonly dynamicProviderOptions: ProviderDynamicOptionRules | undefined;
  readonly headers: Readonly<Record<string, string>>;
  readonly sampling: ProviderSamplingRules;
  readonly maxOutputTokens: "clamp" | "omit";
  readonly schemaStrategy: "passthrough" | "openai-codex" | "moonshot";
  /** Provider wire enforcement available for request-level structured output. */
  readonly structuredOutputStrategy: "native_json_schema" | "json_object" | "unsupported";
  readonly providerSpecificErrorRules: readonly string[];
}

/** Selects whether reasoning continuity uses provider metadata or provider option fields. */
export type ProviderReasoningRules =
  | ProviderMetadataReasoningRules
  | ProviderOptionsReasoningRules;

/** Controls preservation, filtering, and sanitization of provider-native reasoning metadata. */
export interface ProviderMetadataReasoningRules {
  readonly strategy: "provider-metadata";
  readonly metadataKey: string;
  readonly preserveEmptySignedReasoning: boolean;
  readonly preserveEmptyProviderMetadata: boolean;
  readonly requireProviderMetadataContent: boolean;
  readonly unmatchedReasoning: "text" | "drop";
  readonly stripProviderMetadataKeys?: readonly string[];
}

/** Controls typed remapping of assistant reasoning into an openai-compatible provider field. */
export interface ProviderOptionsReasoningRules {
  readonly strategy: "provider-options";
  readonly providerKey: string;
  readonly fieldName: "reasoning_content";
  readonly ensureAssistantReasoning: boolean;
}

/** Defines where ephemeral cache markers are placed in the normalized message list. */
export interface ProviderCacheControlRules {
  readonly placement: "message" | "content";
  readonly providerKey: string;
  readonly firstSystemMessages: number;
  readonly lastNonSystemMessages: number;
}

/** Defines the exact MIME values and MIME prefixes accepted by a provider route. */
export interface ProviderMediaSupport {
  readonly exactMimes: readonly string[];
  readonly mimePrefixes: readonly string[];
}

/** Defines explicit effort variants and their provider option destination. */
export interface ProviderEffortRules {
  readonly allowed: readonly string[];
  readonly noEffortVariants: readonly string[];
  readonly providerKey: string;
  readonly providerOptionKey: string;
}

/** Defines request-derived provider options such as the session-scoped prompt cache key. */
export interface ProviderRequestOptionRules {
  readonly providerKey: string;
  readonly promptCacheKeyField?: "promptCacheKey";
  readonly promptCacheKeySource?: "session_id";
}

/** Defines provider options attached to every lowered tool definition. */
export interface ProviderToolOptionRules {
  readonly providerKey: string;
  readonly strict?: false;
}

/** Defines provider options computed from effective request limits. */
export interface ProviderDynamicOptionRules {
  readonly providerKey: string;
  readonly thinkingBudget?: {
    readonly type: "enabled";
    readonly maxBudgetTokens: number;
  };
}

/** Defines fixed sampling values; `undefined` preserves the provider default. */
export interface ProviderSamplingRules {
  readonly temperature: number | undefined;
  readonly topP: number | undefined;
  readonly topK: number | undefined;
}
