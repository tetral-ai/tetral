/**
 * @packageDocumentation
 * Validates context-loader values before they enter Runtime hot state.
 * It guards session ownership, provider/model completeness, and per-part byte limits, and rejects
 * configured sensitive keys and known unsafe payload patterns at the load boundary.
 * ContextLoaderBoundary calls these helpers; they inspect Runtime message schemas and return
 * normalized loader errors to the caller.
 */
import type { ContextLoaderError, RuntimeMessage, RuntimePart } from "../contracts/runtime.js";
import { normalizeContextLoaderError } from "../contracts/runtime.js";

/** Patterns rejected when they appear in externally loaded Runtime context. */
export const UnsafeContextPayloadPatterns = [
  /\bUNIT\d+_[A-Z0-9_]*TOKEN[A-Z0-9_]*\b/i,
  /\b(?:sk|dummy)[-_][A-Za-z0-9._-]+\b/i,
  /\bbearer\s+[^\s"'<>]+/i,
  /\b(?:postgres|postgresql|mysql|redis):\/\/[^\s"'<>]+/i,
  /\bselect\s+.+?\s+from\s+\S+/i,
  /\bauthorization\s*:\s*bearer\s+[^\s"'<>]+/i,
  /\b(?:x-api-key|api-key|cookie|set-cookie)\s*:\s*[^\n\r]+/i,
  /\bsystem prompt raw backend payload marker\b/i,
  /\braw backend payload marker\b/i,
  /\braw provider payload marker\b/i,
] as const;

const SensitiveContextKeys = [
  "authorization",
  "xapikey",
  "apikey",
  "cookie",
  "setcookie",
  "token",
  "secret",
  "credential",
  "credentials",
  "password",
] as const;

/** Bounds needed to validate one loaded Runtime message and its parts. */
export interface ContextLoaderValidationBounds {
  readonly maxLoadedMessages: number;
  readonly maxPendingMessages: number;
  readonly maxMessageParts: number;
  readonly maxPartBytes: number;
}

/** Creates a normalized context-loader failure tied to the requested session. */
export function contextLoaderFailure(
  sessionId: string,
  code: ContextLoaderError["code"],
  reason: string,
): ContextLoaderError {
  return normalizeContextLoaderError({ code, sessionId, reason });
}

/** Returns whether an unknown value contains a forbidden key or sensitive payload pattern. */
export function containsUnsafeContextPayload(value: unknown): boolean {
  return containsUnsafeContextValue(value);
}

function containsUnsafeContextValue(value: unknown, keyPath: readonly string[] = []): boolean {
  if (typeof value === "string") {
    const parsedJsonString = parseJsonLikeString(value);
    if (parsedJsonString !== undefined && containsUnsafeContextValue(parsedJsonString, keyPath)) {
      return true;
    }
    if (containsSensitiveContextKeyString(value)) {
      return true;
    }
    return keyPath.some(isSensitiveContextKey) && value.trim().length > 0
      ? true
      : UnsafeContextPayloadPatterns.some((pattern) => pattern.test(value));
  }
  if (Array.isArray(value)) {
    return value.some((item) => containsUnsafeContextValue(item, keyPath));
  }
  if (typeof value === "object" && value !== null) {
    return Object.entries(value).some(([key, entry]) => {
      const nextPath = [...keyPath, key];
      return isSensitiveContextKey(key) || containsUnsafeContextValue(entry, nextPath);
    });
  }
  return false;
}

function isSensitiveContextKey(key: string): boolean {
  const normalized = canonicalContextKey(key);
  return SensitiveContextKeys.some((sensitiveKey) => normalized === sensitiveKey);
}

function containsSensitiveContextKeyString(value: string): boolean {
  if (!value.includes(":") && !value.includes("=")) {
    return false;
  }
  for (const match of value.matchAll(/["']?([A-Za-z][A-Za-z0-9_-]*)["']?\s*[:=]\s*["']?([^"',}\]\s]+)/g)) {
    const key = match[1];
    const matchedValue = match[2];
    if (key !== undefined && matchedValue !== undefined && isSensitiveContextKey(key) && matchedValue.trim().length > 0) {
      return true;
    }
  }
  return false;
}

function canonicalContextKey(key: string): string {
  return key
    .replace(/([a-z0-9])([A-Z])/g, "$1$2")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

function parseJsonLikeString(value: string): unknown | undefined {
  const trimmed = value.trim();
  if (
    !(
      (trimmed.startsWith("{") && trimmed.endsWith("}")) ||
      (trimmed.startsWith("[") && trimmed.endsWith("]"))
    )
  ) {
    return undefined;
  }
  try {
    return JSON.parse(trimmed);
  } catch {
    return undefined;
  }
}

/** Validates ownership, model identity, and byte bounds for one loaded message. */
export function validateRuntimeMessageForContext(
  message: RuntimeMessage,
  sessionId: string,
  bounds: ContextLoaderValidationBounds,
): ContextLoaderError | undefined {
  if (message.sessionId !== sessionId) {
    return contextLoaderFailure(sessionId, "wrong_session", "runtime message sessionId does not match requested session");
  }
  if (message.parts.some((part) => part.sessionId !== sessionId || part.messageId !== message.id)) {
    return contextLoaderFailure(sessionId, "wrong_session", "runtime part owner does not match message/session");
  }
  if (message.role === "user" || message.providerId !== undefined || message.modelId !== undefined) {
    if (message.providerId === undefined || message.modelId === undefined) {
      return contextLoaderFailure(sessionId, "unknown_provider_model", "runtime message model identity is incomplete");
    }
  }
  for (const part of message.parts) {
    if (runtimePartBytes(part) > bounds.maxPartBytes) {
      return contextLoaderFailure(sessionId, "bounds_exceeded", "runtime part exceeds loader byte bound");
    }
  }
  return undefined;
}

function runtimePartBytes(part: RuntimePart): number {
  switch (part.type) {
    case "text":
    case "reasoning":
      return new TextEncoder().encode(part.text).length;
    case "tool":
      return new TextEncoder().encode(JSON.stringify(part.state)).length;
    case "step-start":
    case "step-finish":
      return 0;
    default: {
      const unsupportedPart: never = part;
      return unsupportedPart;
    }
  }
}
