/**
 * @packageDocumentation
 *
 * Provides the lowering package's bounded JSON serializer for provider metadata
 * and usage diagnostics. It recursively replaces sensitive keys and text
 * patterns, handles repeated object references without recursion, and returns an
 * empty object for optional diagnostics that fail serialization or exceed their cap.
 * Required stream metadata instead fails explicitly and preserves opaque signatures.
 * Stream raising and usage normalization call this module before placing opaque
 * provider telemetry on generated Gateway protocol messages; the module calls
 * only JSON serialization and UTF-8 byte measurement primitives.
 */
import { ProviderRequestLoweringError } from "./errors.js";

const SensitiveTextPatterns = [
  /\b(?:sk|dummy)[-_][A-Za-z0-9._-]+\b/g,
  /\bauthorization\s*:\s*bearer\s+[^\n\r]+/gi,
  /\b(?:bearer|token)\s+[A-Za-z0-9._-]+\b/gi,
  /\bhttps?:\/\/[^\s"'<>]+/gi,
] as const;

/**
 * Serializes provider telemetry after recursive redaction and byte-bound
 * enforcement. `credential_source` remains visible for accounting attribution;
 * sensitive transport fields and matching text become `[redacted]`, while an
 * undefined, unserializable, or oversized value becomes `{}`.
 */
export function boundedRedactedJson(value: unknown, maxBytes: number): string {
  if (value === undefined) {
    return "{}";
  }
  try {
    const text = JSON.stringify(redactProviderTelemetry(value, new WeakSet()));
    return text !== undefined && utf8ByteLength(text) <= maxBytes ? text : "{}";
  } catch {
    return "{}";
  }
}

/** Required provider metadata fails closed on overflow; signatures cannot silently disappear. */
export function redactedProviderMetadataJson(value: unknown, maxBytes: number): string {
  if (value === undefined) return "{}";
  const text = JSON.stringify(redactProviderTelemetry(value, new WeakSet(), [], true));
  if (text === undefined || utf8ByteLength(text) > maxBytes) {
    throw new ProviderRequestLoweringError({
      code: "provider_metadata_limit_exceeded",
      message: "Provider metadata exceeded its content bound.",
      retryable: false,
      fatal: true,
      statusCode: 413,
    });
  }
  return text;
}

function redactProviderTelemetry(value: unknown, seen: WeakSet<object>, path: readonly string[] = [], preserveRequired = false): unknown {
  if (typeof value === "string") {
    // These opaque provider-owned fields are resubmission material, never log telemetry.
    if (preserveRequired && path.length === 2 && (
      path[0] === "anthropic" && path[1] === "signature" ||
      path[0] === "openai" && path[1] === "reasoningEncryptedContent"
    )) return value;
    return redactSensitiveText(value);
  }
  if (typeof value !== "object" || value === null) {
    return value;
  }
  if (seen.has(value)) {
    return "[redacted]";
  }
  seen.add(value);
  if (Array.isArray(value)) {
    return value.map((item, index) => redactProviderTelemetry(item, seen, [...path, String(index)], preserveRequired));
  }
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>).map(([key, entry]) => [
      redactSensitiveText(key),
      isSensitiveProviderTelemetryKey(key) ? "[redacted]" : redactProviderTelemetry(entry, seen, [...path, key], preserveRequired),
    ]),
  );
}

function redactSensitiveText(value: string): string {
  return SensitiveTextPatterns.reduce((output, pattern) => output.replace(pattern, "[redacted]"), value);
}

function isSensitiveProviderTelemetryKey(key: string): boolean {
  const normalized = key.replace(/[^A-Za-z0-9]/g, "").toLowerCase();
  if (normalized === "credentialsource") {
    return false;
  }
  return normalized.includes("authorization")
    || normalized === "key"
    || normalized.endsWith("key")
    || normalized.includes("apikey")
    || normalized.includes("bearer")
    || normalized.includes("cookie")
    || normalized.includes("credential")
    || normalized.includes("password")
    || normalized.includes("secret")
    || normalized.endsWith("token")
    || normalized === "headers"
    || normalized.endsWith("headers")
    || normalized === "header"
    || normalized === "body"
    || normalized.endsWith("body")
    || normalized === "raw"
    || normalized.includes("rawrequest")
    || normalized.includes("rawresponse")
    || normalized.includes("stacktrace")
    || normalized === "stack"
    || normalized === "trace"
    || normalized.endsWith("url");
}

function utf8ByteLength(value: string): number {
  return new TextEncoder().encode(value).byteLength;
}
