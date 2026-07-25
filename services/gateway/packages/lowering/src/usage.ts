/**
 * @packageDocumentation
 *
 * Normalizes AI SDK usage into the generated Runtime accounting shape carried
 * by a provider finish event. It guards cache-inclusive input arithmetic with a
 * wire-family split, clamps malformed counters to non-negative integers, and
 * redacts and bounds opaque provider usage JSON. Stream raising calls this
 * module with final SDK usage and the preferred terminal provider frame; the
 * module calls the shared redaction serializer and performs no persistence.
 */
import type { RequestUsage } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { boundedRedactedJson } from "./redaction.js";

/** Provider wire families whose cache counters require different input arithmetic. */
export type ProviderUsageWireFamily = "anthropic-wire" | "openai-wire";

/** AI SDK usage fields consumed by Gateway accounting normalization. */
export interface ProviderUsageInput {
  readonly inputTokens?: number | undefined;
  readonly outputTokens?: number | undefined;
  readonly totalTokens?: number | undefined;
  readonly reasoningTokens?: number | undefined;
  readonly cachedInputTokens?: number | undefined;
  readonly inputTokenDetails?: {
    readonly noCacheTokens?: number | undefined;
    readonly cacheReadTokens?: number | undefined;
    readonly cacheWriteTokens?: number | undefined;
  } | undefined;
  readonly outputTokenDetails?: {
    readonly reasoningTokens?: number | undefined;
  } | undefined;
}

/** Selects input-token arithmetic and the opaque provider frame retained for diagnostics. */
export interface ProviderUsageNormalizationOptions {
  readonly wireFamily: ProviderUsageWireFamily;
  readonly providerUsage?: unknown;
}

const ProviderUsageJsonMaxBytes = 16 * 1024;

/**
 * Converts one SDK usage summary into Runtime accounting fields.
 * Anthropic-wire input subtracts cache reads and writes unless a no-cache count
 * is present; OpenAI-wire input subtracts cache reads only. The input total is
 * rebuilt from normalized components, including the total carried on the wire.
 * The opaque diagnostic JSON retains any SDK-reported total for drift checks.
 */
export function normalizeProviderUsage(
  input: ProviderUsageInput | undefined,
  options: ProviderUsageNormalizationOptions,
): RequestUsage | undefined {
  if (input === undefined) {
    return undefined;
  }
  // Per-family uncached-input derivation. The SDK reports inputTokens as the
  // cache-INCLUSIVE input total for both wire families, so uncached input is
  // never read directly -- it is derived from the terminal usage frame's cache
  // fields:
  //   anthropic-wire (anthropic, moonshotai): noCacheTokens when present, else
  //     inputTokens - cacheRead - cacheWrite.
  //   openai-wire (openai, deepseek, zai): inputTokens - cacheRead only.
  // Applying the wrong family silently corrupts usage (an anthropic-wire total
  // still has cache-write folded into the uncached figure). The family
  // assignment for each model lives at the call sites in
  // services/gateway/packages/provider-gateway/src/providers/clients.ts.
  // UPDATE-WITH: provider-gateway/src/providers/clients.ts (usageWireFamily per model).
  const cacheRead = optionalNonNegativeInteger(input.inputTokenDetails?.cacheReadTokens ?? input.cachedInputTokens);
  const cacheWrite = optionalNonNegativeInteger(input.inputTokenDetails?.cacheWriteTokens);
  const sdkInputTokens = nonNegativeInteger(input.inputTokens);
  const inputUncachedTokens = options.wireFamily === "anthropic-wire"
    ? (optionalNonNegativeInteger(input.inputTokenDetails?.noCacheTokens) ?? Math.max(0, sdkInputTokens - (cacheRead ?? 0) - (cacheWrite ?? 0)))
    : Math.max(0, sdkInputTokens - (cacheRead ?? 0));
  const inputTotalTokens = inputUncachedTokens + (cacheRead ?? 0) + (cacheWrite ?? 0);
  const outputTotalTokens = nonNegativeInteger(input.outputTokens);
  const outputReasoningTokens = optionalNonNegativeInteger(input.outputTokenDetails?.reasoningTokens ?? input.reasoningTokens);
  const totalTokens = inputTotalTokens + outputTotalTokens;
  return {
    inputTotalTokens,
    inputUncachedTokens,
    ...(cacheRead !== undefined ? { inputCacheReadTokens: cacheRead } : {}),
    ...(cacheWrite !== undefined ? { inputCacheWriteTokens: cacheWrite } : {}),
    outputTotalTokens,
    ...(outputReasoningTokens !== undefined ? { outputReasoningTokens } : {}),
    totalTokens,
    providerUsageJson: boundedProviderUsageJson(options.providerUsage ?? input),
  };
}

function nonNegativeInteger(value: number | undefined): number {
  return value !== undefined && Number.isInteger(value) && value >= 0 ? value : 0;
}

function optionalNonNegativeInteger(value: number | undefined): number | undefined {
  return value !== undefined && Number.isInteger(value) && value >= 0 ? value : undefined;
}

function boundedProviderUsageJson(value: unknown): string {
  return boundedRedactedJson(value, ProviderUsageJsonMaxBytes);
}
