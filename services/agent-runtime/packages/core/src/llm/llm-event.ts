/**
 * @packageDocumentation
 * Defines Runtime's local LLM event vocabulary, schemas, and failure constructors.
 * It guards explicit local-schema parsing and sanitizes normalized failure fields;
 * ordinary stream text and metadata retain the bounds already enforced at the
 * shared Gateway protocol boundary. LLMService maps those validated Gateway frames
 * into these shapes, while ProviderStreamAccumulator and ThreadLoop consume them.
 */
import { z } from "zod/v4";
import {
  MaxIdBytes,
  MaxMetadataBytes,
  MaxProviderErrorMessageBytes,
  MaxProviderToolCallInputJsonBytes,
  MaxProviderUsageJsonBytes,
  MaxTextBytes,
} from "@tetral/gateway-protocol/src/bounds.js";
import type {
  ProviderError,
} from "../contracts/provider.js";
import { ProviderErrorCodes, ProviderMetadataSchema as RawProviderMetadataSchema } from "../contracts/provider.js";

/** Maximum UTF-8 bytes retained in a local diagnostic or tool-input preview. */
export const RuntimePreviewTextMaxBytes = 8_192;

const IdentifierSchema = z.string().min(1);
const NonNegativeIntegerSchema = z.number().int().nonnegative();
const SafeOperationNameSchema = z.enum(["commitInternalToolRepair"]);
const SafeReasonCodeSchema = z.enum([
  "aborted",
  "bounded",
  "gateway_transport_completion_deadline",
  "runtime_contract_validation",
  "runtime_shutdown",
  "timeout",
  "write_acknowledgement_mismatch",
]);
const SafeTerminalStatusSchema = z.enum(["completed", "failed", "cancelled"]);
const RuntimeMessageStoreErrorCodes = [
  "unavailable",
  "timeout",
  "conflict",
  "constraint_violation",
  "not_found",
  "serialization_failure",
  "schema_mismatch",
  "unknown",
] as const;
const RuntimeErrorCodeSchema = z.enum([
  ...ProviderErrorCodes,
  ...RuntimeMessageStoreErrorCodes,
  "ack_mismatch",
  "gateway_protocol_error",
  "gateway_stream_error",
  "gateway_unavailable",
  "runtime_invalid_sequence",
]);

const RedactedText = "[redacted]";
const SensitiveTextPatterns = [
  /\b[A-Z0-9_]*(?:TOKEN|CREDENTIAL|SECRET)[A-Z0-9_]*CANARY\b/g,
  /\b(?:sk|dummy)[-_][A-Za-z0-9._-]+\b/g,
  /\b(?:postgres|postgresql|mysql|redis):\/\/[^\s"'<>]+/gi,
  /\bselect\s+.+?\s+from\s+\S+/gi,
  /\bauthorization\s*:\s*bearer\s+[^\s"'<>]+/gi,
  /\b(?:x-api-key|api-key|cookie|set-cookie)\s*:\s*[^\n\r]+/gi,
  /\bsystem prompt raw backend payload marker\b/gi,
  /\braw backend payload marker\b/gi,
  /\braw provider payload marker\b/gi,
  /\braw-secret-body\b/gi,
  /\/tmp\/[^\s"'<>]+/gi,
  /\bhttps?:\/\/[^\s"'<>]+/gi,
  /\bprompt text\b/gi,
  /\btool input\b/gi,
  /\btool output\b/gi,
  /\bstack trace\b/gi,
  /\bcause value\b/gi,
] as const;

/** Closed event-type vocabulary emitted by the Runtime LLM stream adapter. */
export const LLMEventTypes = [
  "step-start",
  "text-start",
  "text-delta",
  "text-end",
  "reasoning-start",
  "reasoning-delta",
  "reasoning-end",
  "tool-input-start",
  "tool-input-delta",
  "tool-input-end",
  "tool-call",
  "step-finish",
  "finish",
  "provider-error",
  "attachment-rejections",
] as const;

/** JSON value shape allowed in bounded Runtime stream payloads. */
export type RuntimeJsonValue =
  | null
  | boolean
  | number
  | string
  | readonly RuntimeJsonValue[]
  | { readonly [key: string]: RuntimeJsonValue };

function redactedRuntimeToken(input: string): string {
  let hash = 0x811c9dc5;
  for (let index = 0; index < input.length; index += 1) {
    hash ^= input.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193);
  }
  return `${RedactedText.slice(0, -1)}:${(hash >>> 0).toString(36)}]`;
}

function sanitizeRuntimeText(input: string): string {
  return SensitiveTextPatterns.reduce((output, pattern) => output.replace(pattern, (match) => redactedRuntimeToken(match)), input);
}

function utf8ByteLength(input: string): number {
  return new TextEncoder().encode(input).byteLength;
}

function isWithinUtf8ByteBudget(input: string, maxBytes: number): boolean {
  return utf8ByteLength(input) <= maxBytes;
}

function serializedRuntimeJsonByteLength(value: RuntimeJsonValue): number {
  return utf8ByteLength(JSON.stringify(value));
}

function isRuntimeJsonValue(value: unknown): value is RuntimeJsonValue {
  if (value === null || typeof value === "boolean" || typeof value === "string") {
    return true;
  }
  if (typeof value === "number") {
    return Number.isFinite(value);
  }
  if (Array.isArray(value)) {
    return value.every(isRuntimeJsonValue);
  }
  if (typeof value !== "object" || value === null) {
    return false;
  }
  if (Object.getPrototypeOf(value) !== Object.prototype) {
    return false;
  }
  return Object.values(value).every(isRuntimeJsonValue);
}

const SanitizedTextSchema = z.string()
  .refine((value) => isWithinUtf8ByteBudget(value, MaxProviderErrorMessageBytes), `text must be at most ${MaxProviderErrorMessageBytes} UTF-8 bytes`)
  .transform(sanitizeRuntimeText);
const SanitizedIdentifierSchema = IdentifierSchema
  .refine((value) => isWithinUtf8ByteBudget(value, MaxIdBytes), `identifier must be at most ${MaxIdBytes} UTF-8 bytes`)
  .transform(sanitizeRuntimeText);
const RuntimeTextSchema = z.string()
  .refine((value) => isWithinUtf8ByteBudget(value, MaxTextBytes), `text must be at most ${MaxTextBytes} UTF-8 bytes`);
const RuntimePreviewTextSchema = z.string()
  .refine((value) => isWithinUtf8ByteBudget(value, RuntimePreviewTextMaxBytes), `preview must be at most ${RuntimePreviewTextMaxBytes} UTF-8 bytes`);
const RuntimeIdentifierSchema = IdentifierSchema
  .refine((value) => isWithinUtf8ByteBudget(value, MaxIdBytes), `identifier must be at most ${MaxIdBytes} UTF-8 bytes`);
const ProviderMetadataSchema = RawProviderMetadataSchema.refine(
  (metadata) => serializedRuntimeJsonByteLength(metadata as RuntimeJsonValue) <= MaxMetadataBytes,
  `provider metadata must be at most ${MaxMetadataBytes} UTF-8 bytes`,
);

/** Normalized finish reasons accepted from provider streams. */
export const RuntimeFinishReasonSchema = z.enum([
  "stop",
  "length",
  "content-filter",
  "tool-calls",
  "error",
  "cancelled",
  "other",
  "unknown",
]);
/** Normalized finish reason reported at a provider step or request boundary. */
export type RuntimeFinishReason = z.infer<typeof RuntimeFinishReasonSchema>;

/** Token usage reported at a provider step or request boundary. */
export const RuntimeUsageSchema = z.strictObject({
  inputTokens: NonNegativeIntegerSchema,
  outputTokens: NonNegativeIntegerSchema,
  reasoningTokens: NonNegativeIntegerSchema,
  cacheReadTokens: NonNegativeIntegerSchema,
  cacheWriteTokens: NonNegativeIntegerSchema,
  totalTokens: NonNegativeIntegerSchema.optional(),
  unknownTokens: NonNegativeIntegerSchema.optional(),
  providerUsageJson: z.string()
    .refine((value) => isWithinUtf8ByteBudget(value, MaxProviderUsageJsonBytes), `provider usage must be at most ${MaxProviderUsageJsonBytes} UTF-8 bytes`)
    .optional(),
});
/** Normalized token accounting carried by a step-finish or finish event. */
export type RuntimeUsage = z.infer<typeof RuntimeUsageSchema>;

/** Route-effective model limits optionally reported on a finish event. */
export const RuntimeModelLimitsSchema = z.strictObject({
  contextWindowTokens: z.number().int().positive(),
  inputLimitTokens: z.number().int().positive().optional(),
  outputTokenLimit: z.number().int().positive(),
});
/** Route-effective context and output limits reported independently of usage. */
export type RuntimeModelLimits = z.infer<typeof RuntimeModelLimitsSchema>;

/** Recursive JSON value validator used by bounded provider-stream payloads. */
export const RuntimeJsonValueSchema = z.custom<RuntimeJsonValue>(isRuntimeJsonValue, "RuntimeJsonValue");
const RuntimeToolInputSchema = RuntimeJsonValueSchema.refine(
  (value) => serializedRuntimeJsonByteLength(value) <= MaxProviderToolCallInputJsonBytes,
  `tool input must be at most ${MaxProviderToolCallInputJsonBytes} UTF-8 bytes`,
);

/** Bounded display preview paired with the separately retained tool input. */
export const RuntimeJsonPreviewSchema = z.strictObject({
  preview: RuntimePreviewTextSchema,
  truncated: z.boolean(),
});
/** Display-only preview consumed alongside a complete tool input. */
export type RuntimeJsonPreview = z.infer<typeof RuntimeJsonPreviewSchema>;

/** Per-origin attachment rejection that does not terminate the provider request. */
export const RuntimeAttachmentRejectionSchema = z.strictObject({
  origin: z.discriminatedUnion("type", [
    z.strictObject({
      type: z.literal("transient"),
      attachmentRef: RuntimeIdentifierSchema,
    }),
    z.strictObject({
      type: z.literal("file-backed"),
      sourceEventId: RuntimeIdentifierSchema,
      fileId: RuntimeIdentifierSchema,
    }),
  ]),
  reason: z.enum(["deleted", "over_envelope"]),
});
/** Normalized attachment rejection delivered to ThreadLoop. */
export type RuntimeAttachmentRejection = z.infer<typeof RuntimeAttachmentRejectionSchema>;

/** Sanitized Runtime failure carried by terminal stream and persistence paths. */
export const RuntimeFailureSchema = z.strictObject({
  type: z.enum(["provider", "message-store", "session-event-writer", "session-binding", "runtime"]),
  code: RuntimeErrorCodeSchema,
  message: SanitizedTextSchema,
  retryable: z.boolean(),
  fatal: z.boolean(),
  retryStatus: z.discriminatedUnion("type", [
    z.strictObject({ type: z.literal("retrying"), attempt: NonNegativeIntegerSchema }),
    z.strictObject({ type: z.literal("exhausted") }),
    z.strictObject({ type: z.literal("terminal") }),
  ]).optional(),
  operation: SafeOperationNameSchema.optional(),
  reason: SafeReasonCodeSchema.optional(),
  constraint: SanitizedIdentifierSchema.optional(),
  status: SafeTerminalStatusSchema.optional(),
  attemptedStatus: SafeTerminalStatusSchema.optional(),
  messageId: SanitizedIdentifierSchema.optional(),
  partId: SanitizedIdentifierSchema.optional(),
  sessionId: SanitizedIdentifierSchema.optional(),
  providerId: SanitizedIdentifierSchema.optional(),
  modelId: SanitizedIdentifierSchema.optional(),
  statusCode: NonNegativeIntegerSchema.optional(),
  retryAfterMs: NonNegativeIntegerSchema.optional(),
});
/** Sanitized failure shared by stream, persistence, binding, and Runtime paths. */
export type RuntimeFailure = z.infer<typeof RuntimeFailureSchema>;

// Normalized provider-to-processor contract; raw SDK/provider events cannot cross this boundary.
/** Bounded event union emitted to request-turn orchestration. */
export const LLMEventSchema = z.discriminatedUnion("type", [
  z.strictObject({ type: z.literal("step-start"), stepIndex: NonNegativeIntegerSchema.optional() }),
  z.strictObject({ type: z.literal("text-start"), id: RuntimeIdentifierSchema }),
  z.strictObject({ type: z.literal("text-delta"), id: RuntimeIdentifierSchema, text_delta: RuntimeTextSchema }),
  z.strictObject({ type: z.literal("text-end"), id: RuntimeIdentifierSchema }),
  z.strictObject({ type: z.literal("reasoning-start"), id: RuntimeIdentifierSchema, providerMetadata: ProviderMetadataSchema.optional() }),
  z.strictObject({ type: z.literal("reasoning-delta"), id: RuntimeIdentifierSchema, text_delta: RuntimeTextSchema, providerMetadata: ProviderMetadataSchema.optional() }),
  z.strictObject({ type: z.literal("reasoning-end"), id: RuntimeIdentifierSchema, providerMetadata: ProviderMetadataSchema.optional() }),
  z.strictObject({ type: z.literal("tool-input-start"), id: RuntimeIdentifierSchema, toolName: RuntimeIdentifierSchema }),
  z.strictObject({ type: z.literal("tool-input-delta"), id: RuntimeIdentifierSchema, text_delta: RuntimeTextSchema, toolName: RuntimeIdentifierSchema.optional() }),
  z.strictObject({ type: z.literal("tool-input-end"), id: RuntimeIdentifierSchema, toolName: RuntimeIdentifierSchema.optional() }),
  z.strictObject({
    type: z.literal("tool-call"),
    id: RuntimeIdentifierSchema,
    toolName: RuntimeIdentifierSchema,
    input: RuntimeToolInputSchema,
    inputPreview: RuntimeJsonPreviewSchema,
  }),
  z.strictObject({ type: z.literal("step-finish"), finishReason: RuntimeFinishReasonSchema.optional(), usage: RuntimeUsageSchema.optional() }),
  z.strictObject({
    type: z.literal("finish"),
    finishReason: RuntimeFinishReasonSchema.optional(),
    usage: RuntimeUsageSchema.optional(),
    providerMetadata: ProviderMetadataSchema.optional(),
    modelLimits: RuntimeModelLimitsSchema.optional(),
  }),
  z.strictObject({ type: z.literal("provider-error"), error: RuntimeFailureSchema }),
  z.strictObject({
    type: z.literal("attachment-rejections"),
    rejections: z.array(RuntimeAttachmentRejectionSchema).min(1).max(32),
  }),
]);
/** Discriminated provider-stream event consumed by request-turn orchestration. */
export type LLMEvent = z.infer<typeof LLMEventSchema>;

/** Converts a normalized provider failure into the Runtime failure vocabulary. */
export function runtimeFailureFromProviderError(error: ProviderError, retryStatus?: RuntimeFailure["retryStatus"]): RuntimeFailure {
  return RuntimeFailureSchema.parse({
    type: "provider",
    code: error.code,
    message: error.message,
    retryable: error.retryable,
    fatal: error.fatal,
    ...(retryStatus !== undefined ? { retryStatus } : {}),
    ...(error.providerId !== undefined ? { providerId: error.providerId } : {}),
    ...(error.modelId !== undefined ? { modelId: error.modelId } : {}),
    ...(error.statusCode !== undefined ? { statusCode: error.statusCode } : {}),
    ...(error.retryAfterMs !== undefined ? { retryAfterMs: error.retryAfterMs } : {}),
  });
}
