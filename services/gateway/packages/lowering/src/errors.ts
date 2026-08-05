/**
 * @packageDocumentation
 *
 * Defines the lowering package's public provider-error vocabulary and converts
 * failures into bounded `ProviderStreamEvent` payloads. It keeps timeout,
 * cancellation, lowering, provider-specific, and unknown stream failures on
 * explicit classification paths, with unknown stream failures collapsing to a
 * retryable transport error. Request lowering and provider-gateway stream,
 * credential, and pool code construct these errors; the module emits generated
 * Gateway protocol values and performs no provider calls or retries itself.
 */
import {
  ProviderStreamEventType,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type {
  ProviderError,
  ProviderRequest,
  ProviderStreamEvent,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { MaxProviderErrorMessageBytes, truncateUtf8Bytes } from "@tetral/gateway-protocol/src/bounds.js";

/** Public provider-error fields accepted before defaults and numeric bounds are applied. */
export interface ProviderErrorInput {
  readonly code?: string | undefined;
  readonly message?: string | undefined;
  readonly retryable?: boolean | undefined;
  readonly fatal?: boolean | undefined;
  readonly statusCode?: number | undefined;
  readonly retryAfterMs?: number | undefined;
}

/** Provider-native status and code fields used by provider-specific classifiers. */
export interface ProviderSpecificErrorInput {
  readonly statusCode?: number | undefined;
  readonly providerCode?: string | undefined;
}

/** Timeout boundaries surfaced across pre-stream setup and provider stream control. */
export type ProviderStreamTimeoutKind = "first_byte_timeout" | "inter_chunk_timeout" | "overall_timeout";

/** Carries a deliberate, caller-safe provider error out of pure request lowering. */
export class ProviderRequestLoweringError extends Error {
  constructor(readonly providerError: ProviderErrorInput) {
    super(providerError.message ?? "Provider request could not be lowered.");
    this.name = "ProviderRequestLoweringError";
  }
}

/** Identifies which overall request or stream-activity deadline expired without exposing details. */
export class ProviderStreamTimeoutError extends Error {
  constructor(readonly kind: ProviderStreamTimeoutKind) {
    super(timeoutMessage(kind));
    this.name = "ProviderStreamTimeoutError";
  }
}

/**
 * Builds a terminal provider-error event tied to the Runtime request identities.
 * The input message is bounded but is otherwise trusted to already be safe for
 * Runtime; callers do not pass provider bodies, headers, credentials, or stack
 * traces through this function.
 */
export function providerErrorEvent(request: Pick<ProviderRequest, "requestId" | "modelRequestId">, input: ProviderErrorInput): ProviderStreamEvent {
  return {
    requestId: request.requestId,
    modelRequestId: request.modelRequestId,
    type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR,
    providerError: {
      metadataJson: "{}",
      error: normalizeProviderError(input),
    },
  };
}

/** Applies stable defaults and non-negative integer bounds to a provider error. */
export function normalizeProviderError(input: ProviderErrorInput): ProviderError {
  const code = boundedIdentifier(input.code) ?? "provider_unknown";
  return {
    code,
    message: boundedMessage(input.message) ?? defaultProviderErrorMessage(code),
    retryable: input.retryable ?? false,
    fatal: input.fatal ?? !input.retryable,
    statusCode: nonNegativeInteger(input.statusCode),
    retryAfterMs: nonNegativeInteger(input.retryAfterMs),
  };
}

/**
 * Classifies failures escaping request lowering, pre-stream setup, or stream consumption.
 * Recognized cancellation and timeout errors retain their distinct semantics;
 * every unmatched failure becomes a retryable stream error without forwarding
 * the original error text.
 */
export function classifyProviderStreamError(error: unknown): ProviderErrorInput {
  if (error instanceof ProviderRequestLoweringError) {
    return error.providerError;
  }
  if (error instanceof DOMException && error.name === "AbortError") {
    return {
      code: "provider_cancelled",
      message: "Provider request was cancelled.",
      retryable: false,
      fatal: false,
      statusCode: 499,
    };
  }
  if (error instanceof ProviderStreamTimeoutError) {
    return {
      code: "provider_timeout",
      message: error.message,
      retryable: true,
      fatal: false,
      statusCode: 504,
    };
  }
  // INVARIANT: unmatched provider stream failures (server 5xx, dropped
  // connections, and other unclassified in-stream errors) classify
  // retryable=true / fatal=false, regardless of the SDK error's isRetryable
  // flag. Anchored to the retry gate (mirrors OpenCode session/retry.ts#
  // retryable), not the SDK classifier alone.
  return {
    code: "provider_stream_error",
    message: "Provider stream failed.",
    retryable: true,
    fatal: false,
    statusCode: 502,
  };
}

/**
 * Maps the OpenAI-family provider codes that alter retry or operator action.
 * Unrecognized combinations return `undefined` so the provider-gateway pool can
 * continue through its generic status and transport classification.
 */
export function classifyOpenAIProviderError(input: ProviderSpecificErrorInput): ProviderErrorInput | undefined {
  // INVARIANT: OpenAI-family HTTP 404 classifies retryable=true / fatal=false.
  // The provider emits 404 for transient conditions, not only a genuine missing
  // resource.
  if (input.statusCode === 404) {
    return {
      code: "provider_stream_error",
      message: "Provider returned a retryable not-found error.",
      retryable: true,
      fatal: false,
      statusCode: 404,
    };
  }
  switch (input.providerCode) {
    case "usage_not_included":
      return {
        code: "provider_plan_required",
        message: "Provider plan does not include access for this model or feature.",
        retryable: false,
        fatal: true,
        statusCode: input.statusCode,
      };
    case "insufficient_quota":
      return {
        code: "provider_quota_exhausted",
        message: "Provider quota is exhausted or the plan needs attention.",
        retryable: false,
        fatal: true,
        statusCode: input.statusCode,
      };
    case "invalid_prompt":
      return {
        code: "provider_request_invalid",
        message: "Provider rejected the prompt; revise the request before retrying.",
        retryable: false,
        fatal: true,
        statusCode: input.statusCode,
      };
    case "server_is_overloaded":
    case "server_error":
      return {
        code: "provider_stream_error",
        message: "Provider returned a retryable server error.",
        retryable: true,
        fatal: false,
        statusCode: input.statusCode,
      };
    default:
      return undefined;
  }
}

function timeoutMessage(kind: ProviderStreamTimeoutKind): string {
  switch (kind) {
    case "first_byte_timeout":
      return "Provider did not start streaming before the timeout.";
    case "inter_chunk_timeout":
      return "Provider stream stalled before the next chunk.";
    case "overall_timeout":
      return "Provider request timed out.";
  }
}

function boundedIdentifier(value: string | undefined): string | undefined {
  if (value === undefined || value.length === 0) {
    return undefined;
  }
  return value.length > 128 ? value.slice(0, 128) : value;
}

function boundedMessage(value: string | undefined): string | undefined {
  if (value === undefined || value.length === 0) {
    return undefined;
  }
  return truncateUtf8Bytes(value, MaxProviderErrorMessageBytes);
}

function defaultProviderErrorMessage(code: string): string {
  switch (code) {
    case "provider_unavailable":
      return "Provider is unavailable.";
    case "provider_cancelled":
      return "Provider request was cancelled.";
    case "provider_timeout":
      return "Provider request timed out.";
    case "context_overflow":
      return "Provider context window exceeded.";
    default:
      return "Provider request failed.";
  }
}

function nonNegativeInteger(value: number | undefined): number {
  return value !== undefined && Number.isInteger(value) && value >= 0 ? value : 0;
}
