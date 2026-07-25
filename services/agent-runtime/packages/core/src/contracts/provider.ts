/**
 * This module defines Runtime Core's normalized provider metadata and failure
 * boundary. It guards the currently recognized error-code vocabulary, safe
 * identifier and message bounds, retryability defaults, and JSON-only metadata.
 * The LLM service and message projection call it to normalize failures, and it
 * calls Zod for the final structural check.
 *
 * @packageDocumentation
 */

import { z } from "zod/v4";

/** Provider identifier carried through Runtime without selecting a provider implementation. */
export type ProviderId = string;

/** Failure codes currently recognized by Runtime error normalization. */
export const ProviderErrorCodes = [
  "credential_required",
  "platform_keys_exhausted",
  "context_overflow",
  "provider_request_invalid",
  "provider_plan_required",
  "provider_key_unavailable",
  "provider_quota_exhausted",
  "provider_auth_failed",
  "provider_rate_limited",
  "provider_quota_exceeded",
  "provider_context_overflow",
  "provider_model_not_found",
  "provider_invalid_request",
  "provider_tool_protocol_error",
  "provider_timeout",
  "provider_stream_error",
  "provider_unavailable",
  "provider_cancelled",
  "attachment_unavailable",
  "provider_unknown",
] as const;
/** Provider failure-code vocabulary currently accepted by the local schema. */
export type ProviderErrorCode = (typeof ProviderErrorCodes)[number];

/** Recursive JSON value permitted inside provider metadata. */
export type ProviderMetadataJson =
  | null
  | boolean
  | number
  | string
  | readonly ProviderMetadataJson[]
  | { readonly [key: string]: ProviderMetadataJson };

/** JSON object metadata that can cross the provider-to-Runtime boundary. */
export type ProviderMetadata = Readonly<Record<string, ProviderMetadataJson>>;

const ProviderIdentifierSchema = z.string().min(1).max(128);
const ProviderErrorCodeSchema = z.enum(ProviderErrorCodes);
const NonNegativeIntegerSchema = z.number().int().nonnegative();

function isProviderMetadataJson(value: unknown): value is ProviderMetadataJson {
  if (value === null || typeof value === "boolean" || typeof value === "string") {
    return true;
  }
  if (typeof value === "number") {
    return Number.isFinite(value);
  }
  if (Array.isArray(value)) {
    return value.every(isProviderMetadataJson);
  }
  if (typeof value !== "object" || value === null) {
    return false;
  }
  if (Object.getPrototypeOf(value) !== Object.prototype) {
    return false;
  }
  return Object.values(value).every(isProviderMetadataJson);
}

function isProviderMetadata(value: unknown): value is ProviderMetadata {
  return typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    Object.getPrototypeOf(value) === Object.prototype &&
    Object.values(value).every(isProviderMetadataJson);
}

/** Validates a plain JSON object used as provider metadata. */
export const ProviderMetadataSchema = z.custom<ProviderMetadata>(isProviderMetadata, "ProviderMetadata");

/**
 * Validates normalized provider failures with bounded message and identifier
 * fields; metadata is JSON-shaped but is not locally size-bounded.
 */
export const ProviderErrorSchema = z.strictObject({
  code: ProviderErrorCodeSchema,
  message: z.string().min(1).max(8_192),
  retryable: z.boolean(),
  fatal: z.boolean(),
  providerId: ProviderIdentifierSchema.optional(),
  modelId: ProviderIdentifierSchema.optional(),
  statusCode: NonNegativeIntegerSchema.optional(),
  retryAfterMs: NonNegativeIntegerSchema.optional(),
  providerMetadata: ProviderMetadataSchema.optional(),
});
/** Canonical normalized provider failure consumed by Runtime Core. */
export type ProviderError = z.infer<typeof ProviderErrorSchema>;

/** Raw adapter input accepted by the provider error normalizer. */
export interface ProviderErrorInput {
  readonly code?: string | undefined;
  readonly message?: string | undefined;
  readonly retryable?: boolean | undefined;
  readonly fatal?: boolean | undefined;
  readonly providerId?: string | undefined;
  readonly modelId?: string | undefined;
  readonly statusCode?: number | undefined;
  readonly retryAfterMs?: number | undefined;
  readonly providerMetadata?: ProviderMetadata | undefined;
}

const RetryableCodes = new Set<ProviderErrorCode>([
  "platform_keys_exhausted",
  "provider_key_unavailable",
  "provider_rate_limited",
  "provider_timeout",
  "provider_stream_error",
  "provider_unavailable",
]);

/** Normalizes adapter failures with the recognized code set and bounded identifiers and message. */
export function normalizeProviderError(input: ProviderErrorInput): ProviderError {
  const code = providerErrorCode(input.code);
  return ProviderErrorSchema.parse({
    code,
    message: boundedProviderErrorMessage(input.message, code),
    retryable: input.retryable ?? RetryableCodes.has(code),
    fatal: input.fatal ?? isFatalProviderError(code),
    ...(safeOptionalIdentifier(input.providerId) !== undefined ? { providerId: safeOptionalIdentifier(input.providerId) } : {}),
    ...(safeOptionalIdentifier(input.modelId) !== undefined ? { modelId: safeOptionalIdentifier(input.modelId) } : {}),
    ...(safeOptionalNonNegativeInteger(input.statusCode) !== undefined ? { statusCode: safeOptionalNonNegativeInteger(input.statusCode) } : {}),
    ...(safeOptionalNonNegativeInteger(input.retryAfterMs) !== undefined ? { retryAfterMs: safeOptionalNonNegativeInteger(input.retryAfterMs) } : {}),
    ...(input.providerMetadata !== undefined ? { providerMetadata: input.providerMetadata } : {}),
  });
}

function providerErrorCode(value: string | undefined): ProviderErrorCode {
  return ProviderErrorCodes.includes(value as ProviderErrorCode) ? (value as ProviderErrorCode) : "provider_unknown";
}

function boundedProviderErrorMessage(message: string | undefined, code: ProviderErrorCode): string {
  const text = message ?? defaultProviderErrorMessage(code);
  return text.length > 8_192 ? text.slice(0, 8_192) : text;
}

function defaultProviderErrorMessage(code: ProviderErrorCode): string {
  switch (code) {
    case "credential_required":
      return "Provider credential is required.";
    case "platform_keys_exhausted":
      return "Platform provider keys are exhausted.";
    case "context_overflow":
      return "Provider context window exceeded.";
    case "provider_request_invalid":
      return "Provider request is invalid.";
    case "provider_plan_required":
      return "Provider plan does not allow this request.";
    case "provider_key_unavailable":
      return "Provider key is not usable.";
    case "provider_quota_exhausted":
      return "Provider quota is exhausted.";
    case "provider_auth_failed":
      return "Provider authentication failed.";
    case "provider_rate_limited":
      return "Provider rate limit exceeded.";
    case "provider_quota_exceeded":
      return "Provider quota exceeded.";
    case "provider_context_overflow":
      return "Provider context window exceeded.";
    case "provider_model_not_found":
      return "Provider model was not found.";
    case "provider_invalid_request":
      return "Provider request is invalid.";
    case "provider_tool_protocol_error":
      return "Provider tool protocol failed.";
    case "provider_timeout":
      return "Provider request timed out.";
    case "provider_stream_error":
      return "Provider stream failed.";
    case "provider_unavailable":
      return "Provider is unavailable.";
    case "provider_cancelled":
      return "Provider request was cancelled.";
    case "attachment_unavailable":
      return "Attachment bytes are no longer available.";
    case "provider_unknown":
      return "Provider request failed.";
  }
}

function isFatalProviderError(code: ProviderErrorCode): boolean {
  return code === "credential_required" ||
    code === "context_overflow" ||
    code === "provider_request_invalid" ||
    code === "provider_plan_required" ||
    code === "provider_quota_exhausted" ||
    code === "provider_auth_failed" ||
    code === "provider_context_overflow" ||
    code === "provider_model_not_found" ||
    code === "provider_invalid_request" ||
    code === "provider_tool_protocol_error";
}

function safeOptionalIdentifier(value: string | undefined): string | undefined {
  if (value === undefined || value.length === 0) {
    return undefined;
  }
  return value.length > 128 ? value.slice(0, 128) : value;
}

function safeOptionalNonNegativeInteger(value: number | undefined): number | undefined {
  return value !== undefined && Number.isInteger(value) && value >= 0 ? value : undefined;
}
