import approvedFieldNames from "./fields.json";

/** Scalar field values accepted by the shared structured log record. */
export type TetralLogFieldValue = string | number | boolean | undefined;

const approvedDiagnosticFields = new Set<string>(approvedFieldNames);
export const invalidEvent = "diagnostic.invalid_event";
const redacted = "[REDACTED]";
const sensitiveKeyPattern = /(?:^|[._-])(?:authorization|api[_-]?key|access[_-]?token|refresh[_-]?token|session[_-]?token|secret|password|engine[_-]?vault[_-]?key|body|content|prompt|reasoning|arguments|payload|stack|headers|sql|exception|tool_result|provider_response)(?:[._-]|$)|^(?:authorizationToken|apiKey|accessToken|refreshToken|sessionToken|clientSecret|secretAccessKey|engineVaultKey)$/i;
const sensitiveValuePattern = /Bearer\s+\S+|(?:sk|ant|ghp|github)[-_][A-Za-z0-9._-]{8,}|(?:access_token|refresh_token|client_secret|token)=|https?:\/\//i;
const maxStringBytes = 1024;

/**
 * Applies the shared field rules to caller fields: at most 64 fields, scalar
 * values only, sensitive keys and token-shaped values redacted, fields outside
 * the allowlist dropped, strings over 1,024 UTF-8 bytes truncated and control
 * characters replaced by spaces.
 */
export function redactLogRecord(record: Readonly<Record<string, unknown>>): Record<string, TetralLogFieldValue> {
  const next: Record<string, TetralLogFieldValue> = {};
  let fields = 0;
  for (const key in record) {
    if (!Object.hasOwn(record, key)) continue;
    const value = record[key];
    if (++fields > 64) break;
    if (key.length > 128) continue;
    if (typeof value !== "string" && typeof value !== "number" && typeof value !== "boolean") continue;
    if (typeof value === "number" && !Number.isFinite(value)) continue;
    const approved = approvedDiagnosticFields.has(key);
    if (sensitiveKeyPattern.test(key) && !(approved && key.endsWith("_count") && typeof value === "number")) { next[key] = redacted; continue; }
    const oversized = typeof value === "string" && Buffer.byteLength(value, "utf8") > maxStringBytes;
    if (typeof value === "string" && !oversized && sensitiveValuePattern.test(value)) { next[key] = redacted; continue; }
    if (!approved) continue;
    next[key] = typeof value !== "string" ? value : oversized ? "[TRUNCATED]" : value.replace(/[\x00-\x1f\x7f]/g, " ");
  }
  return next;
}

export function safeDiagnosticEvent(event: unknown): string {
  return typeof event === "string" && /^[a-z][a-z0-9_.-]{0,127}$/.test(event) && !sensitiveValuePattern.test(event) ? event : invalidEvent;
}

export function safeResourceValue(value: string): string {
  if (Buffer.byteLength(value, "utf8") > 253) return "[TRUNCATED]";
  if (sensitiveValuePattern.test(value)) return redacted;
  return value.replace(/[\x00-\x1f\x7f]/g, " ");
}
