/**
 * This module provides Runtime Core's shared domain and integration-port boundary definitions.
 * It guards validated message, part, event, failure, settlement, and store shapes
 * plus the byte bounds imposed at stable-write and transport edges. The session
 * processor, agent loop, tool runner, context loader, and Bridge adapters consume
 * it; it composes provider schemas and invokes injected store operations through
 * validating wrappers.
 *
 * @packageDocumentation
 */

import { z } from "zod/v4";
import type { ProviderRequestAttachment } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { ProviderErrorCodes, ProviderMetadataSchema as RawProviderMetadataSchema } from "./provider.js";
import type { ProviderId } from "./provider.js";
export {
  LLMEventSchema,
  LLMEventTypes,
  RuntimeBoundedJsonSchema as LLMRuntimeBoundedJsonSchema,
  RuntimeBoundedTextSchema as LLMRuntimeBoundedTextSchema,
  RuntimeFailureSchema as LLMRuntimeFailureSchema,
  RuntimeUsageSchema as LLMRuntimeUsageSchema,
} from "../llm/llm-event.js";
export type {
  LLMEvent,
  RuntimeBoundedJson as LLMRuntimeBoundedJson,
  RuntimeBoundedText as LLMRuntimeBoundedText,
  RuntimeFailure as LLMRuntimeFailure,
  RuntimeUsage as LLMRuntimeUsage,
} from "../llm/llm-event.js";

/** Provider and model identity selected for one Runtime processor invocation. */
export interface RuntimeProcessorSource {
  readonly providerId: ProviderId;
  readonly modelId: string;
}

/** Public MCP failure projection that may accompany a terminal tool settlement. */
export type PublicMcpErrorEvent =
  | {
      readonly type: "mcp_authentication_failed_error" | "mcp_connection_failed_error";
      readonly mcpServerName: string;
      readonly message: string;
      readonly retryStatus: { readonly type: "retrying" | "exhausted" | "terminal" };
    }
  | {
      readonly type: "unknown_error";
      readonly message: string;
      readonly retryStatus: { readonly type: "terminal" };
    };

export interface SessionEventWriterServerToolUse {
  readonly webSearchRequests: number;
  readonly webFetchRequests: number;
}

/** Final disposition returned by a Runtime tool route to its request turn. */
export type RuntimeToolSettlement =
  | { readonly type: "completed"; readonly output: RuntimeBoundedText; readonly serverToolUse?: SessionEventWriterServerToolUse; readonly mcpMaterializationHandle?: string }
  | { readonly type: "error"; readonly error: RuntimeFailure; readonly publicErrorEvent?: PublicMcpErrorEvent | undefined; readonly serverToolUse?: SessionEventWriterServerToolUse; readonly mcpMaterializationHandle?: string }
  | { readonly type: "cancelled"; readonly error?: RuntimeFailure };

const IdentifierSchema = z.string().min(1);
const TimestampSchema = z.string().datetime({ offset: true });
const NonNegativeIntegerSchema = z.number().int().nonnegative();
const PositiveIntegerSchema = z.number().int().positive();
const RuntimeProviderMetadataMaxBytes = 16 * 1024;
// Stable-reasoning durable bounds for one model request, enforced as a SINGLE
// budget across BOTH durability vectors: the per-tool anchored-prefix attach
// (WriteEvent) and the request-end settlement set (WriteRequestEnd).
//   - MaxStableReasoningPartsPerRequest bounds the part COUNT.
//   - MaxStableReasoningBytesPerRequest bounds the aggregate text+metadata BYTES.
// These pod-side values must stay byte-for-byte equal to the Bridge enforcement
// constants of the same name: Bridge fatally rejects any attached or settled set
// that exceeds them, so a pod value larger than Bridge's would emit sets Bridge
// refuses, and a smaller one would drop parts Bridge would have accepted.
// UPDATE-WITH: services/bridge/bridge_api_store.go,
//              services/bridge/bridge_api_settlement.go
export const MaxStableReasoningPartsPerRequest = 16;
export const MaxStableReasoningBytesPerRequest = 2 * 1024 * 1024;
const SafeOperationNameSchema = z.enum(["commitInternalToolRepair"]);
const SafeReasonCodeSchema = z.enum([
  "aborted",
  "bounded",
  "runtime_contract_validation",
  "runtime_shutdown",
  "timeout",
  "write_acknowledgement_mismatch",
]);
const SafeTerminalStatusSchema = z.enum(["completed", "failed", "cancelled"]);
const Utf8Encoder = new TextEncoder();
const DefaultMaxNormalizedTextPreviewBytes = 8_192;

const RedactedText = "[redacted]";
const SensitiveTextPatterns = [
  /\bUNIT\d+_[A-Z0-9_]*TOKEN[A-Z0-9_]*\b/g,
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

function sanitizeRuntimeText(input: string): string {
  return SensitiveTextPatterns.reduce((output, pattern) => output.replace(pattern, (match) => redactedRuntimeToken(match)), input);
}

function redactedRuntimeToken(input: string): string {
  let hash = 0x811c9dc5;
  for (let index = 0; index < input.length; index += 1) {
    hash ^= input.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193);
  }
  return `${RedactedText.slice(0, -1)}:${(hash >>> 0).toString(36)}]`;
}

const SanitizedTextSchema = z.string().transform(sanitizeRuntimeText);
const SanitizedIdentifierSchema = IdentifierSchema.transform(sanitizeRuntimeText);
const RuntimeTextSchema = z.string();
const RuntimeIdentifierSchema = IdentifierSchema;

const ProviderMetadataSchema = RawProviderMetadataSchema.refine(
  (metadata) => byteLength(JSON.stringify(metadata)) <= RuntimeProviderMetadataMaxBytes,
  `provider metadata must be at most ${RuntimeProviderMetadataMaxBytes} UTF-8 bytes`,
);

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
export type RuntimeFinishReason = z.infer<typeof RuntimeFinishReasonSchema>;

// Request-turn error classification stamped on span.model_request_end when
// is_error = true. Internal telemetry/audit only: never projected to any public
// event, message, or SDK surface (session-event-writer.ts maps public failures on
// a separate axis). Each value has a disjoint trigger and none is a catch-all:
//   provider_error            Gateway emitted provider-error before finish.
//   gateway_stream_error      the stream reset/timed out/closed with no finish and
//                             no provider-error after the span started.
//   gateway_protocol_error    NEVER a fallback: stamped only for a malformed,
//                             out-of-order, duplicate-terminal, or wrong
//                             request_id/model_request_id stream event, or a
//                             deterministic Gateway admission request-shape rejection.
//   runtime_interrupted       a runtime interrupt won before a terminal stream event.
//   runtime_persistence_error a Runtime/Bridge durable write failed after span start.
//   runtime_semantic_error    a well-formed terminal result failed a Runtime semantic
//                             invariant after span start (unusable/empty result or
//                             invalid/empty compaction summary); the failure site
//                             carries its own discriminator so it is never conflated
//                             with a stream-event rejection.
// The Bridge runtime-repair path stamps one further value the pod never stamps —
// runtime_pod_lost — when it closes a request left open by a dead pod, reusing the
// original model_request_id; that value is intentionally absent from this pod enum.
// UPDATE-WITH: services/agent-runtime/packages/core/src/runtime/session-event-writer.ts,
//              services/bridge/runtime_pod_lost.go
export const RuntimeRequestErrorKindSchema = z.enum([
  "provider_error",
  "gateway_stream_error",
  "gateway_protocol_error",
  "runtime_interrupted",
  "runtime_persistence_error",
  "runtime_semantic_error",
]);
export type RuntimeRequestErrorKind = z.infer<typeof RuntimeRequestErrorKindSchema>;

export const RuntimeUsageSchema = z.strictObject({
  inputTokens: NonNegativeIntegerSchema,
  outputTokens: NonNegativeIntegerSchema,
  reasoningTokens: NonNegativeIntegerSchema,
  cacheReadTokens: NonNegativeIntegerSchema,
  cacheWriteTokens: NonNegativeIntegerSchema,
  totalTokens: NonNegativeIntegerSchema.optional(),
  unknownTokens: NonNegativeIntegerSchema.optional(),
  providerUsageJson: z.string().optional(),
});
export type RuntimeUsage = z.infer<typeof RuntimeUsageSchema>;

export const RuntimeMessageStatusSchema = z.enum(["streaming", "completed", "failed", "cancelled"]);

export const RuntimePartStatusSchema = z.enum(["streaming", "completed", "failed", "cancelled"]);

export const RuntimeBoundedTextSchema = z.strictObject({
  text: RuntimeTextSchema,
  truncated: z.boolean(),
});
export type RuntimeBoundedText = z.infer<typeof RuntimeBoundedTextSchema>;

export type RuntimeJsonValue =
  | null
  | boolean
  | number
  | string
  | readonly RuntimeJsonValue[]
  | { readonly [key: string]: RuntimeJsonValue };
export type RuntimeJsonObject = { readonly [key: string]: RuntimeJsonValue };

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

function isRuntimeJsonObject(value: unknown): value is RuntimeJsonObject {
  return typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    Object.getPrototypeOf(value) === Object.prototype &&
    Object.values(value).every(isRuntimeJsonValue);
}

export const RuntimeJsonValueSchema = z.custom<RuntimeJsonValue>(isRuntimeJsonValue, "RuntimeJsonValue");
export const RuntimeJsonObjectSchema = z.custom<RuntimeJsonObject>(isRuntimeJsonObject, "RuntimeJsonObject");

export const RuntimeBoundedJsonSchema = z.strictObject({
  value: RuntimeJsonValueSchema.optional(),
  preview: RuntimeTextSchema,
  truncated: z.boolean(),
});
export type RuntimeBoundedJson = z.infer<typeof RuntimeBoundedJsonSchema>;

export const RuntimeToolErrorSchema = z.strictObject({
  type: SanitizedIdentifierSchema,
  message: SanitizedTextSchema,
  retryable: z.boolean().optional(),
});
export type RuntimeToolError = z.infer<typeof RuntimeToolErrorSchema>;

export const ToolStateSchema = z.discriminatedUnion("status", [
  z.strictObject({
    status: z.literal("pending"),
  }),
  z.strictObject({
    status: z.literal("running"),
    input: RuntimeBoundedJsonSchema,
  }),
  z.strictObject({
    status: z.literal("completed"),
    input: RuntimeBoundedJsonSchema,
    output: RuntimeBoundedTextSchema,
  }),
  z.strictObject({
    status: z.literal("error"),
    input: RuntimeBoundedJsonSchema.optional(),
    error: RuntimeToolErrorSchema,
  }),
  z.strictObject({
    status: z.literal("cancelled"),
    input: RuntimeBoundedJsonSchema.optional(),
    error: RuntimeToolErrorSchema.optional(),
  }),
]);
export type ToolState = z.infer<typeof ToolStateSchema>;

export const RuntimeMessageInfoSchema = z.strictObject({
  id: RuntimeIdentifierSchema,
  sessionId: RuntimeIdentifierSchema,
  role: z.enum(["user", "assistant"]),
  origin: z.enum(["user", "agent", "runtime"]),
  sequence: NonNegativeIntegerSchema,
  status: RuntimeMessageStatusSchema,
  createdAt: TimestampSchema,
  updatedAt: TimestampSchema.optional(),
  error: z.lazy(() => RuntimeFailureSchema).optional(),
  finishReason: RuntimeFinishReasonSchema.optional(),
  usage: RuntimeUsageSchema.optional(),
  responseId: RuntimeIdentifierSchema.optional(),
});
export type RuntimeMessageInfo = z.infer<typeof RuntimeMessageInfoSchema>;

const RuntimePartBaseSchema = {
  id: RuntimeIdentifierSchema,
  sessionId: RuntimeIdentifierSchema,
  messageId: RuntimeIdentifierSchema,
  sequence: NonNegativeIntegerSchema,
  createdAt: TimestampSchema,
  updatedAt: TimestampSchema.optional(),
} as const;

const TextRuntimePartSchema = z.strictObject({
  ...RuntimePartBaseSchema,
  type: z.literal("text"),
  text: RuntimeTextSchema,
  truncated: z.boolean(),
  status: RuntimePartStatusSchema,
  startedAt: TimestampSchema.optional(),
  completedAt: TimestampSchema.optional(),
});

const ReasoningRuntimePartSchema = z.strictObject({
  ...RuntimePartBaseSchema,
  type: z.literal("reasoning"),
  providerPartId: RuntimeIdentifierSchema.optional(),
  providerMetadata: ProviderMetadataSchema.optional(),
  text: RuntimeTextSchema,
  truncated: z.boolean(),
  status: RuntimePartStatusSchema,
  startedAt: TimestampSchema.optional(),
  completedAt: TimestampSchema.optional(),
});

const ToolRuntimePartSchema = z.strictObject({
  ...RuntimePartBaseSchema,
  type: z.literal("tool"),
  toolCallId: RuntimeIdentifierSchema,
  toolName: RuntimeIdentifierSchema,
  toolUseEventId: RuntimeIdentifierSchema.optional(),
  toolEvent: z.discriminatedUnion("kind", [
    z.strictObject({ kind: z.literal("tool") }),
    z.strictObject({ kind: z.literal("mcp"), mcpServerName: RuntimeIdentifierSchema }),
  ]).optional(),
  state: ToolStateSchema,
  startedAt: TimestampSchema.optional(),
  completedAt: TimestampSchema.optional(),
});

const StepStartRuntimePartSchema = z.strictObject({
  ...RuntimePartBaseSchema,
  type: z.literal("step-start"),
  stepIndex: NonNegativeIntegerSchema.optional(),
});

const StepFinishRuntimePartSchema = z.strictObject({
  ...RuntimePartBaseSchema,
  type: z.literal("step-finish"),
  stepIndex: NonNegativeIntegerSchema.optional(),
  finishReason: RuntimeFinishReasonSchema,
  usage: RuntimeUsageSchema.optional(),
});

// Boundary contract for persisted runtime parts, not a backend table definition.
// Message stores and hot projection code share this union to preserve part identity.
export const RuntimePartSchema = z.discriminatedUnion("type", [
  TextRuntimePartSchema,
  ReasoningRuntimePartSchema,
  ToolRuntimePartSchema,
  StepStartRuntimePartSchema,
  StepFinishRuntimePartSchema,
]);
/** Persisted or hot-projected message part with stable ownership and sequence identity. */
export type RuntimePart = z.infer<typeof RuntimePartSchema>;

const RuntimePartDraftBaseSchema = {
  runtimeLocalPartId: RuntimeIdentifierSchema,
  ordinal: NonNegativeIntegerSchema,
} as const;

const TextRuntimePartDraftSchema = z.strictObject({
  ...RuntimePartDraftBaseSchema,
  type: z.literal("text"),
  text: RuntimeTextSchema,
  truncated: z.boolean(),
  status: RuntimePartStatusSchema,
  startedAt: TimestampSchema.optional(),
  completedAt: TimestampSchema.optional(),
});

const ReasoningRuntimePartDraftSchema = z.strictObject({
  ...RuntimePartDraftBaseSchema,
  type: z.literal("reasoning"),
  providerPartId: RuntimeIdentifierSchema.optional(),
  providerMetadata: ProviderMetadataSchema.optional(),
  text: RuntimeTextSchema,
  truncated: z.boolean(),
  status: RuntimePartStatusSchema,
  startedAt: TimestampSchema.optional(),
  completedAt: TimestampSchema.optional(),
});

const ToolRuntimePartDraftSchema = z.strictObject({
  ...RuntimePartDraftBaseSchema,
  type: z.literal("tool"),
  toolCallId: RuntimeIdentifierSchema,
  toolName: RuntimeIdentifierSchema,
  toolUseEventId: RuntimeIdentifierSchema.optional(),
  toolEvent: z.discriminatedUnion("kind", [
    z.strictObject({ kind: z.literal("tool") }),
    z.strictObject({ kind: z.literal("mcp"), mcpServerName: RuntimeIdentifierSchema }),
  ]).optional(),
  state: ToolStateSchema,
  startedAt: TimestampSchema.optional(),
  completedAt: TimestampSchema.optional(),
});

const StepStartRuntimePartDraftSchema = z.strictObject({
  ...RuntimePartDraftBaseSchema,
  type: z.literal("step-start"),
  stepIndex: NonNegativeIntegerSchema.optional(),
});

const StepFinishRuntimePartDraftSchema = z.strictObject({
  ...RuntimePartDraftBaseSchema,
  type: z.literal("step-finish"),
  stepIndex: NonNegativeIntegerSchema.optional(),
  finishReason: RuntimeFinishReasonSchema,
  usage: RuntimeUsageSchema.optional(),
});

/** Closed semantic part set authored before Bridge assigns durable part stamps. */
export const RuntimePartDraftSchema = z.discriminatedUnion("type", [
  TextRuntimePartDraftSchema,
  ReasoningRuntimePartDraftSchema,
  ToolRuntimePartDraftSchema,
  StepStartRuntimePartDraftSchema,
  StepFinishRuntimePartDraftSchema,
]);
export type RuntimePartDraft = z.infer<typeof RuntimePartDraftSchema>;

export const RuntimeDraftKindSchema = z.enum([
  "user_input",
  "approval_input",
  "reviewer_input",
  "agent_mail_input",
  "assistant_text",
  "tool_use",
  "tool_result",
  "task_notification",
  "rejection",
  "cancellation",
  "completion_mail",
  "compaction_checkpoint",
  "internal_tool_repair",
  "termination",
]);
export type RuntimeDraftKind = z.infer<typeof RuntimeDraftKindSchema>;

/** Loop-authored message declaration before any durable identity is assigned. */
export const RuntimeMessageDraftSchema = z.strictObject({
  runtimeLocalId: RuntimeIdentifierSchema,
  sourceKind: RuntimeIdentifierSchema,
  sourceId: RuntimeIdentifierSchema,
  sourceEventId: RuntimeIdentifierSchema.optional(),
  draftKind: RuntimeDraftKindSchema,
  ordinal: NonNegativeIntegerSchema,
  role: z.enum(["user", "assistant"]),
  origin: z.enum(["user", "agent", "runtime"]),
  status: RuntimeMessageStatusSchema,
  error: z.lazy(() => RuntimeFailureSchema).optional(),
  finishReason: RuntimeFinishReasonSchema.optional(),
  usage: RuntimeUsageSchema.optional(),
  responseId: RuntimeIdentifierSchema.optional(),
  parts: z.array(RuntimePartDraftSchema),
});
export type RuntimeMessageDraft = z.infer<typeof RuntimeMessageDraftSchema>;

export interface RuntimeDurableEventStamp {
  readonly sessionThreadId: string;
  readonly sourceEventId: string;
  readonly eventId: string;
  readonly eventSequence: number;
  readonly disposition: "existing" | "created";
}

export interface RuntimeDurablePartStamp {
  readonly runtimeLocalPartId: string;
  readonly partId: string;
  readonly messageId: string;
  readonly partSequence: number;
  readonly createdAt: string;
  readonly updatedAt: string;
  readonly disposition: "created" | "updated";
}

export interface RuntimeDurableMessageStamp {
  readonly runtimeLocalId: string;
  readonly sessionThreadId: string;
  readonly owningEventId: string;
  readonly messageId: string;
  readonly messageSequence: number;
  readonly createdAt: string;
  readonly updatedAt: string;
  readonly disposition: "created" | "updated";
  readonly parts: readonly RuntimeDurablePartStamp[];
}

export interface RuntimePrefixConsumptionStamp {
  readonly childThreadId: string;
  readonly parentBoundaryEventId: string;
  readonly checkpointMessageId: string;
  readonly disposition: "consumed";
}

export interface RuntimeRequestRescheduleStamp {
  readonly disposition: "accepted" | "denied_attempt_mismatch" | "denied_budget_exhausted";
  readonly requestKind: "agent_provider_request" | "compaction_summary" | "approval_reviewer";
  readonly attempt: number;
  readonly effectiveDeadline: string;
}

export interface RuntimeIdleCloseoutStamp {
  readonly durableTurnId: string;
  readonly idleEventId: string;
  readonly idleEventSequence: number;
  readonly committedIdleAt: string;
}

export interface RuntimeChildLifecycleStamp {
  readonly childThreadId: string;
  readonly disposition:
    | "closed"
    | "already_closed"
    | "resumed"
    | "already_active"
    | "preserved_failed"
    | "preserved_terminated";
  readonly effectiveAt: string;
}

export interface RuntimeSealedAgentMailStamp {
  readonly runtimeInputId: string;
  readonly birthPreparationAttemptId: string;
  readonly failedPreparationAttemptId: string;
  readonly disposition: "sealed_for_failed_birth";
}

export interface RuntimeDeclarationReceipt {
  readonly sessionThreadId: string;
  readonly operationKind: string;
  readonly sourceKind: string;
  readonly sourceId: string;
  readonly declarationDigest: string;
  readonly events: readonly RuntimeDurableEventStamp[];
  readonly messages: readonly RuntimeDurableMessageStamp[];
  readonly pendingAttachmentDelta: readonly ProviderRequestAttachment[];
  readonly pendingToolDelta: readonly string[];
  readonly prefixConsumptions: readonly RuntimePrefixConsumptionStamp[];
  readonly requestReschedule?: RuntimeRequestRescheduleStamp | undefined;
  readonly idleCloseout?: RuntimeIdleCloseoutStamp | undefined;
  readonly compactedThroughMessageSequence?: number | undefined;
  readonly childLifecycle: readonly RuntimeChildLifecycleStamp[];
  readonly sealedAgentMail?: RuntimeSealedAgentMailStamp | undefined;
}

// Boundary contract for persisted runtime parts, not a backend table definition.
// It also enforces that every part remains attached to the owning message and session.
export const RuntimeMessageSchema = RuntimeMessageInfoSchema.extend({
  parts: z.array(RuntimePartSchema),
})
  .refine(
    (message) => message.parts.every((runtimePart) => runtimePart.messageId === message.id),
    "runtime part messageId must match the owning message id",
  )
  .refine(
    (message) => message.parts.every((runtimePart) => runtimePart.sessionId === message.sessionId),
    "runtime part sessionId must match the owning message session id",
  );
/** Runtime conversation message carrying message and part identity for hot projection. */
export type RuntimeMessage = z.infer<typeof RuntimeMessageSchema>;

/**
 * Database-stamped conversation message installed by cold recovery or receipt
 * application. Provider and model routing remain request configuration.
 */
export const DurableRuntimeMessageSchema = RuntimeMessageInfoSchema
  .extend({
    owningEventId: RuntimeIdentifierSchema,
    eventSequence: PositiveIntegerSchema,
    parts: z.array(RuntimePartSchema),
  })
  .refine(
    (message) => message.parts.every((runtimePart) => runtimePart.messageId === message.id),
    "runtime part messageId must match the owning message id",
  )
  .refine(
    (message) => message.parts.every((runtimePart) => runtimePart.sessionId === message.sessionId),
    "runtime part sessionId must match the owning message session id",
  );
export type DurableRuntimeMessage = z.infer<typeof DurableRuntimeMessageSchema>;

/** One self-contained invalid-tool repair committed before it enters hot history. */
export const RuntimeInternalToolRepairCommitSchema = z.strictObject({
  requestId: SanitizedIdentifierSchema,
  workspaceId: SanitizedIdentifierSchema,
  sessionId: SanitizedIdentifierSchema,
  sessionThreadId: SanitizedIdentifierSchema,
  bindingId: SanitizedIdentifierSchema,
  bindingGeneration: NonNegativeIntegerSchema,
  targetPodUid: SanitizedIdentifierSchema,
  modelRequestId: SanitizedIdentifierSchema,
  modelToolCallId: SanitizedIdentifierSchema,
  toolName: SanitizedIdentifierSchema,
  repairKey: SanitizedIdentifierSchema,
  draft: RuntimeMessageDraftSchema,
})
  .refine((repair) =>
    repair.draft.sourceKind === "internal_tool_repair" &&
    repair.draft.sourceId === repair.repairKey &&
    repair.draft.sourceEventId === undefined &&
    repair.draft.draftKind === "internal_tool_repair" &&
    repair.draft.ordinal === 0,
  "repair draft identity must match the repair operation")
  .refine((repair) =>
    repair.draft.role === "assistant" &&
    repair.draft.origin === "agent" &&
    repair.draft.status === "completed",
  "repair draft must be a completed assistant agent message")
  .refine((repair) => repair.draft.parts.length === 1, "repair draft must carry exactly one part")
  .refine((repair) => {
    const [part] = repair.draft.parts;
    return part?.type === "tool" &&
      part.toolCallId === repair.modelToolCallId &&
      part.toolName === repair.toolName &&
      part.toolUseEventId === undefined &&
      part.state.status === "error";
  }, "repair draft part must be an internal terminal tool error without a public tool use id");
export type RuntimeInternalToolRepairCommit = z.infer<typeof RuntimeInternalToolRepairCommitSchema>;

const UserRuntimeMessageSchema = RuntimeMessageSchema.refine((message) => message.role === "user", "pending input messages must be user messages");

// Bridge has already classified the pending event result; Agent Pod validates and executes it.
// The loader exposes either an ordered non-empty message batch or an explicit empty result.
export const PendingInputResultSchema = z.discriminatedUnion("type", [
  z.strictObject({
    type: z.literal("messages"),
    messages: z.array(UserRuntimeMessageSchema).min(1),
  }),
  z.strictObject({
    type: z.literal("empty"),
  }),
]);
export type PendingInputResult = z.infer<typeof PendingInputResultSchema>;

export const ContextLoaderErrorCodes = [
  "unavailable",
  "timeout",
  "schema_mismatch",
  "wrong_session",
  "bounds_exceeded",
  "unsafe_payload",
  "unknown_provider_model",
  "unknown",
] as const;

export const ContextLoaderErrorSchema = z.strictObject({
  type: z.literal("context-loader"),
  code: z.enum(ContextLoaderErrorCodes),
  message: SanitizedTextSchema,
  retryable: z.boolean(),
  fatal: z.boolean(),
  sessionId: SanitizedIdentifierSchema.optional(),
  reason: SanitizedTextSchema.optional(),
});
export type ContextLoaderError = z.infer<typeof ContextLoaderErrorSchema>;

export const RuntimeMessageStoreErrorCodes = [
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

export const RuntimeMessageStoreErrorSchema = z.strictObject({
  type: z.literal("message-store"),
  code: z.enum(RuntimeMessageStoreErrorCodes),
  operation: SafeOperationNameSchema,
  message: SanitizedTextSchema,
  retryable: z.boolean(),
  fatal: z.boolean(),
  reason: SafeReasonCodeSchema.optional(),
  constraint: SanitizedIdentifierSchema.optional(),
  status: SafeTerminalStatusSchema.optional(),
  attemptedStatus: SafeTerminalStatusSchema.optional(),
  messageId: SanitizedIdentifierSchema.optional(),
  partId: SanitizedIdentifierSchema.optional(),
  sessionId: SanitizedIdentifierSchema.optional(),
});
export type RuntimeMessageStoreError = z.infer<typeof RuntimeMessageStoreErrorSchema>;

const RuntimeFailureRetryStatusSchema = z.discriminatedUnion("type", [
  z.strictObject({ type: z.literal("retrying"), attempt: NonNegativeIntegerSchema }),
  z.strictObject({ type: z.literal("exhausted") }),
  z.strictObject({ type: z.literal("terminal") }),
]);

const SessionErrorRetryStatusSchema = z.discriminatedUnion("type", [
  z.strictObject({ type: z.literal("retrying") }),
  z.strictObject({ type: z.literal("exhausted") }),
  z.strictObject({ type: z.literal("terminal") }),
]);

const SessionMcpErrorSchema = z.discriminatedUnion("type", [
  z.strictObject({
    type: z.literal("mcp_authentication_failed_error"),
    mcp_server_name: SanitizedIdentifierSchema,
    message: SanitizedTextSchema,
    retry_status: SessionErrorRetryStatusSchema,
  }),
  z.strictObject({
    type: z.literal("mcp_connection_failed_error"),
    mcp_server_name: SanitizedIdentifierSchema,
    message: SanitizedTextSchema,
    retry_status: SessionErrorRetryStatusSchema,
  }),
  z.strictObject({
    type: z.literal("unknown_error"),
    message: SanitizedTextSchema,
    retry_status: SessionErrorRetryStatusSchema,
  }),
]);

const SessionModelErrorSchema = z.discriminatedUnion("type", [
  z.strictObject({
    type: z.literal("model_overloaded_error"),
    message: SanitizedTextSchema,
    retry_status: SessionErrorRetryStatusSchema,
  }),
  z.strictObject({
    type: z.literal("model_rate_limited_error"),
    message: SanitizedTextSchema,
    retry_status: SessionErrorRetryStatusSchema,
  }),
  z.strictObject({
    type: z.literal("model_request_failed_error"),
    message: SanitizedTextSchema,
    retry_status: SessionErrorRetryStatusSchema,
  }),
]);

/** Canonical internal failure used by retry, termination, and durable error projection. */
export const RuntimeFailureSchema = z.strictObject({
  type: z.enum(["provider", "message-store", "session-event-writer", "session-binding", "runtime"]),
  code: RuntimeErrorCodeSchema,
  message: SanitizedTextSchema,
  retryable: z.boolean(),
  fatal: z.boolean(),
  retryStatus: RuntimeFailureRetryStatusSchema.optional(),
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
export type RuntimeFailure = z.infer<typeof RuntimeFailureSchema>;

const NonTerminalProviderFailureCodes = new Set([
  "credential_required",
  "platform_keys_exhausted",
  "provider_key_unavailable",
  "provider_rate_limited",
  "provider_timeout",
  "provider_stream_error",
  "provider_unavailable",
  "provider_cancelled",
  "attachment_unavailable",
]);

/** Returns whether a non-retryable failure must terminate the Runtime scope. */
export function isRuntimeTerminationFailure(failure: RuntimeFailure): boolean {
  if (failure.retryable || failure.retryStatus?.type !== "terminal") {
    return false;
  }
  if (failure.type === "runtime") {
    return failure.code === "runtime_invalid_sequence" && failure.reason === "runtime_contract_validation";
  }
  return failure.type === "provider" && !NonTerminalProviderFailureCodes.has(failure.code);
}

const SessionErrorPayloadSchema = z.union([RuntimeFailureSchema, SessionMcpErrorSchema, SessionModelErrorSchema]);

const SessionEventTextBlockSchema = z.strictObject({
  type: z.literal("text"),
  text: RuntimeTextSchema,
});

const SessionIdleStopReasonSchema = z.discriminatedUnion("type", [
  z.strictObject({ type: z.literal("end_turn") }),
  z.strictObject({ type: z.literal("requires_action"), event_ids: z.array(SanitizedIdentifierSchema).min(1) }),
  z.strictObject({ type: z.literal("retries_exhausted") }),
]);

const ApprovalReviewRiskLevelSchema = z.enum(["low", "medium", "high", "critical"]);
const ApprovalReviewUserAuthorizationSchema = z.enum(["unknown", "low", "medium", "high"]);

const ModelUsageSchema = z.strictObject({
  input_tokens: NonNegativeIntegerSchema,
  output_tokens: NonNegativeIntegerSchema,
  cache_creation_input_tokens: NonNegativeIntegerSchema,
  cache_read_input_tokens: NonNegativeIntegerSchema,
  speed: z.null(),
});

/** Closed durable session event payloads emitted by Runtime Core. */
export const SessionEventSchema = z.discriminatedUnion("type", [
  z.strictObject({
    type: z.literal("agent.message"),
    content: z.array(SessionEventTextBlockSchema).min(1),
  }),
  z.strictObject({
    type: z.literal("agent.thinking"),
  }),
  z.strictObject({
    type: z.literal("agent.tool_use"),
    name: RuntimeIdentifierSchema,
    input: RuntimeJsonObjectSchema,
    evaluated_permission: z.enum(["allow", "ask", "deny"]),
  }),
  z.strictObject({
    type: z.literal("agent.mcp_tool_use"),
    name: RuntimeIdentifierSchema,
    input: RuntimeJsonObjectSchema,
    mcp_server_name: RuntimeIdentifierSchema,
    evaluated_permission: z.enum(["allow", "ask", "deny"]),
  }),
  z.strictObject({
    type: z.literal("agent.tool_result"),
    tool_use_id: RuntimeIdentifierSchema,
    content: z.array(SessionEventTextBlockSchema).optional(),
    is_error: z.boolean().optional(),
  }),
  z.strictObject({
    type: z.literal("agent.thread_context_compacted"),
    summary: RuntimeTextSchema,
    recent_context: RuntimeJsonValueSchema,
  }),
  z.strictObject({
    type: z.literal("approval_review.decision"),
    review_id: SanitizedIdentifierSchema,
    parent_thread_id: SanitizedIdentifierSchema,
    target_model_tool_call_id: SanitizedIdentifierSchema,
    target_tool_name: SanitizedIdentifierSchema,
    risk_level: ApprovalReviewRiskLevelSchema,
    user_authorization: ApprovalReviewUserAuthorizationSchema,
    outcome: z.enum(["allow", "deny"]),
    rationale: SanitizedTextSchema,
  }),
  z.strictObject({
    type: z.literal("approval_review.failure"),
    review_id: SanitizedIdentifierSchema,
    parent_thread_id: SanitizedIdentifierSchema,
    target_model_tool_call_id: SanitizedIdentifierSchema,
    target_tool_name: SanitizedIdentifierSchema,
    failure_kind: z.enum(["timeout", "parse_failure", "runtime_failure"]),
    message: SanitizedTextSchema,
  }),
  z.strictObject({
    type: z.literal("agent.mcp_tool_result"),
    mcp_tool_use_id: RuntimeIdentifierSchema,
    content: z.array(SessionEventTextBlockSchema).optional(),
    is_error: z.boolean().optional(),
  }),
  z.strictObject({
    type: z.literal("session.status_running"),
  }),
  z.strictObject({
    type: z.literal("session.status_rescheduled"),
  }),
  z.strictObject({
    type: z.literal("session.status_idle"),
    stop_reason: SessionIdleStopReasonSchema,
  }),
  z.strictObject({
    type: z.literal("session.status_terminated"),
  }),
  z.strictObject({
    type: z.literal("session.thread_status_terminated"),
    session_thread_id: SanitizedIdentifierSchema,
  }),
  z.strictObject({
    type: z.literal("session.error"),
    error: SessionErrorPayloadSchema,
  }),
  z.strictObject({
    type: z.literal("span.model_request_start"),
    model_request_id: SanitizedIdentifierSchema,
  }),
  z.strictObject({
    type: z.literal("span.model_request_end"),
    model_request_start_id: SanitizedIdentifierSchema,
    is_error: z.boolean(),
    error_kind: RuntimeRequestErrorKindSchema.optional(),
    model_usage: ModelUsageSchema,
  }),
]);
export type SessionEvent = z.infer<typeof SessionEventSchema>;

export const SessionEventWriterStableReasoningPartSchema = z.strictObject({
  reasoningPartId: SanitizedIdentifierSchema,
  providerPartId: RuntimeIdentifierSchema.optional(),
  partSequence: NonNegativeIntegerSchema,
  text: RuntimeTextSchema,
  providerMetadata: ProviderMetadataSchema.optional(),
  truncated: z.boolean(),
});
export type SessionEventWriterStableReasoningPart = z.infer<typeof SessionEventWriterStableReasoningPartSchema>;

export const SessionEventWriterServerToolUseSchema = z.strictObject({
  webSearchRequests: NonNegativeIntegerSchema,
  webFetchRequests: NonNegativeIntegerSchema,
});

function validateStableReasoningSet(
  parts: readonly SessionEventWriterStableReasoningPart[],
  context: z.RefinementCtx,
): void {
  const ids = new Set<string>();
  const sequences = new Set<number>();
  let aggregateBytes = 0;
  let previousSequence = -1;
  for (const part of parts) {
    if (ids.has(part.reasoningPartId) || sequences.has(part.partSequence) || part.partSequence <= previousSequence) {
      context.addIssue({ code: "custom", message: "stable reasoning identities and order must be unique" });
      break;
    }
    ids.add(part.reasoningPartId);
    sequences.add(part.partSequence);
    previousSequence = part.partSequence;
    aggregateBytes += byteLength(part.text) + byteLength(JSON.stringify(part.providerMetadata ?? {}));
  }
  if (aggregateBytes > MaxStableReasoningBytesPerRequest) {
    context.addIssue({ code: "custom", message: "stable reasoning exceeds aggregate byte bound" });
  }
}

/** WriteEvent payload with identity, projection, reasoning, and server-tool bounds. */
export const SessionEventEnvelopeSchema = z.strictObject({
  requestId: SanitizedIdentifierSchema,
  workspaceId: SanitizedIdentifierSchema,
  sessionId: SanitizedIdentifierSchema,
  sessionThreadId: SanitizedIdentifierSchema,
  bindingId: SanitizedIdentifierSchema,
  bindingGeneration: NonNegativeIntegerSchema,
  targetPodUid: SanitizedIdentifierSchema,
  writeId: SanitizedIdentifierSchema,
  event: SessionEventSchema,
  drafts: z.array(RuntimeMessageDraftSchema),
  modelRequestId: SanitizedIdentifierSchema.optional(),
  stableReasoningParts: z.array(SessionEventWriterStableReasoningPartSchema).max(MaxStableReasoningPartsPerRequest).optional(),
  serverToolUse: SessionEventWriterServerToolUseSchema.optional(),
  mcpMaterializationHandle: SanitizedIdentifierSchema.optional(),
}).superRefine((envelope, context) => {
  const parts = envelope.stableReasoningParts ?? [];
  if (parts.length > 0 && envelope.event.type !== "agent.tool_use" && envelope.event.type !== "agent.mcp_tool_use") {
    context.addIssue({ code: "custom", message: "stable reasoning requires a public tool-use event" });
  }
  if (parts.length > 0 && envelope.modelRequestId === undefined) {
    context.addIssue({ code: "custom", message: "stable reasoning requires a model request id" });
  }
  if (envelope.drafts.length > 0 && envelope.modelRequestId === undefined) {
    context.addIssue({ code: "custom", message: "runtime output projection requires a model request id" });
  }
  if ((envelope.event.type.startsWith("session.") || envelope.event.type.startsWith("span.")) && envelope.modelRequestId !== undefined) {
    context.addIssue({ code: "custom", message: "non-assistant event forbids a model request id" });
  }
  if (envelope.serverToolUse !== undefined && envelope.event.type !== "agent.tool_result") {
    context.addIssue({ code: "custom", message: "server tool usage requires a tool-result event" });
  }
  if (envelope.mcpMaterializationHandle !== undefined && envelope.event.type !== "agent.mcp_tool_result") {
    context.addIssue({ code: "custom", message: "MCP materialization requires an MCP tool-result event" });
  }
  if (envelope.event.type === "agent.mcp_tool_result" && envelope.mcpMaterializationHandle === undefined) {
    context.addIssue({ code: "custom", message: "MCP tool-result event requires materialization" });
  }
  validateStableReasoningSet(parts, context);
});
export type SessionEventEnvelope = z.infer<typeof SessionEventEnvelopeSchema>;

/** Request-end settlement payload, including retry and attachment consumption facts. */
export const SessionEventWriterRequestEndEnvelopeSchema = z.strictObject({
  requestId: SanitizedIdentifierSchema,
  workspaceId: SanitizedIdentifierSchema,
  sessionId: SanitizedIdentifierSchema,
  sessionThreadId: SanitizedIdentifierSchema,
  bindingId: SanitizedIdentifierSchema,
  bindingGeneration: NonNegativeIntegerSchema,
  targetPodUid: SanitizedIdentifierSchema,
  writeId: SanitizedIdentifierSchema,
  modelRequestId: SanitizedIdentifierSchema,
  modelRequestStartEventId: SanitizedIdentifierSchema,
  requestKind: z.enum(["agent_provider_request", "compaction_summary", "approval_reviewer"]).optional(),
  isError: z.boolean(),
  errorKind: RuntimeRequestErrorKindSchema.optional(),
  finishReason: RuntimeFinishReasonSchema,
  usage: RuntimeUsageSchema.optional(),
  consumedAttachmentRefs: z.array(SanitizedIdentifierSchema).optional(),
  consumedFileAttachments: z.array(z.strictObject({
    sourceEventId: SanitizedIdentifierSchema,
    fileId: SanitizedIdentifierSchema,
  })).max(32).optional(),
  reschedule: z.strictObject({
    attempt: z.number().int().positive(),
    deadline: TimestampSchema,
    backoffMs: NonNegativeIntegerSchema,
  }).optional(),
  stableReasoningParts: z.array(SessionEventWriterStableReasoningPartSchema).max(MaxStableReasoningPartsPerRequest).optional(),
  drafts: z.array(RuntimeMessageDraftSchema).optional(),
  prefixConsumption: z.strictObject({
    childThreadId: SanitizedIdentifierSchema,
    parentBoundaryEventId: SanitizedIdentifierSchema,
    checkpointRuntimeLocalId: SanitizedIdentifierSchema,
  }).optional(),
  compactedThroughMessageSequence: NonNegativeIntegerSchema.optional(),
  compactionEventPayloadJson: z.string().superRefine((value, context) => {
    try {
      const parsed = JSON.parse(value) as unknown;
      if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
        context.addIssue({ code: "custom", message: "compaction event payload must be a JSON object" });
      }
    } catch {
      context.addIssue({ code: "custom", message: "compaction event payload must be valid JSON" });
    }
  }).optional(),
  interruptSettlement: z.strictObject({
    runtimeInputId: SanitizedIdentifierSchema,
    eventIds: z.array(SanitizedIdentifierSchema).length(1),
    sequenceFrom: NonNegativeIntegerSchema,
    sequenceTo: NonNegativeIntegerSchema,
    pendingToolCancellations: z.array(z.strictObject({
      toolUseEventId: SanitizedIdentifierSchema,
      runtimeLocalId: SanitizedIdentifierSchema,
    })),
  }).optional(),
}).superRefine((envelope, context) => {
  const parts = envelope.stableReasoningParts ?? [];
  if (parts.length > 0 && (envelope.isError || envelope.reschedule !== undefined)) {
    context.addIssue({ code: "custom", message: "stable reasoning requires a successful request end" });
  }
  const consumedAttachmentCount =
    (envelope.consumedAttachmentRefs?.length ?? 0) +
    (envelope.consumedFileAttachments?.length ?? 0);
  if (consumedAttachmentCount > 32) {
    context.addIssue({ code: "custom", message: "consumed attachments exceed the request bound" });
  }
  if (envelope.reschedule !== undefined && consumedAttachmentCount > 0) {
    context.addIssue({ code: "custom", message: "rescheduled request ends cannot consume attachments" });
  }
  const isCompaction = envelope.requestKind === "compaction_summary";
  const drafts = envelope.drafts ?? [];
  const cancellationDrafts = drafts.filter((draft) => draft.draftKind === "cancellation");
  const primaryDrafts = drafts.filter((draft) => draft.draftKind !== "cancellation");
  if (envelope.interruptSettlement === undefined) {
    if (cancellationDrafts.length > 0) {
      context.addIssue({ code: "custom", message: "request-end cancellation drafts require interrupt settlement" });
    }
  } else {
    if (
      !envelope.isError ||
      envelope.errorKind !== "runtime_interrupted" ||
      envelope.reschedule !== undefined ||
      envelope.interruptSettlement.sequenceTo < envelope.interruptSettlement.sequenceFrom
    ) {
      context.addIssue({ code: "custom", message: "request-end interrupt settlement requires an interrupted terminal end" });
    }
    if (
      cancellationDrafts.some((draft) =>
        draft.sourceKind !== "interrupt_control" ||
        draft.sourceId !== envelope.interruptSettlement?.runtimeInputId ||
        draft.sourceEventId !== envelope.interruptSettlement?.eventIds[0]
      )
    ) {
      context.addIssue({ code: "custom", message: "request-end interrupt cancellation drafts are incomplete" });
    }
    const cancellationIDs = new Set(cancellationDrafts.map((draft) => draft.runtimeLocalId));
    const pendingToolIDs = new Set(
      envelope.interruptSettlement.pendingToolCancellations.map((pending) => pending.toolUseEventId),
    );
    if (
      cancellationIDs.size !== cancellationDrafts.length ||
      pendingToolIDs.size !== envelope.interruptSettlement.pendingToolCancellations.length ||
      envelope.interruptSettlement.pendingToolCancellations.some(
        (pending) => !cancellationIDs.has(pending.runtimeLocalId),
      )
    ) {
      context.addIssue({ code: "custom", message: "request-end interrupt cancellation mapping is invalid" });
    }
  }
  if (isCompaction) {
    if (!envelope.isError && envelope.reschedule === undefined) {
      if (
        primaryDrafts.length !== 1 ||
        primaryDrafts[0]?.draftKind !== "compaction_checkpoint" ||
        envelope.compactedThroughMessageSequence === undefined ||
        envelope.compactionEventPayloadJson === undefined
      ) {
        context.addIssue({ code: "custom", message: "successful compaction requires its event, checkpoint, and sequence boundary" });
      }
      if (
        envelope.prefixConsumption !== undefined &&
        envelope.prefixConsumption.checkpointRuntimeLocalId !== primaryDrafts[0]?.runtimeLocalId
      ) {
        context.addIssue({ code: "custom", message: "prefix consumption must name the compaction checkpoint draft" });
      }
    } else {
      if (primaryDrafts.length !== 0) {
        context.addIssue({ code: "custom", message: "failed compaction request ends permit cancellation drafts only" });
      }
      if (
        envelope.prefixConsumption !== undefined ||
        envelope.compactedThroughMessageSequence !== undefined ||
        envelope.compactionEventPayloadJson !== undefined
      ) {
        context.addIssue({ code: "custom", message: "failed compaction request ends forbid checkpoint declaration fields" });
      }
    }
  } else {
    if (
      envelope.prefixConsumption !== undefined ||
      envelope.compactedThroughMessageSequence !== undefined ||
      envelope.compactionEventPayloadJson !== undefined
    ) {
      context.addIssue({ code: "custom", message: "non-compaction request ends forbid checkpoint declaration fields" });
    }
    if (
      primaryDrafts.length > 1 ||
      primaryDrafts.some((draft) =>
        draft.draftKind !== "assistant_text" ||
        draft.sourceKind !== "model_request" ||
        draft.sourceId !== envelope.modelRequestId ||
        draft.status === "streaming"
      )
    ) {
      context.addIssue({ code: "custom", message: "ordinary request ends permit one terminal assistant seal only" });
    }
  }
  validateStableReasoningSet(parts, context);
});
export type SessionEventWriterRequestEndEnvelope = z.infer<typeof SessionEventWriterRequestEndEnvelopeSchema>;

export const SessionEventWriterFinishIdleEnvelopeSchema = z.strictObject({
  workspaceId: SanitizedIdentifierSchema,
  sessionId: SanitizedIdentifierSchema,
  sessionThreadId: SanitizedIdentifierSchema,
  bindingId: SanitizedIdentifierSchema,
  bindingGeneration: NonNegativeIntegerSchema,
  targetPodUid: SanitizedIdentifierSchema,
  durableTurnId: SanitizedIdentifierSchema,
  stopReason: SessionIdleStopReasonSchema,
  drafts: z.array(RuntimeMessageDraftSchema).max(1).optional(),
}).superRefine((envelope, context) => {
  if ((envelope.drafts ?? []).some((draft) => draft.draftKind !== "completion_mail")) {
    context.addIssue({ code: "custom", message: "idle closeout permits a completion-mail draft only" });
  }
});
export type SessionEventWriterFinishIdleEnvelope = z.infer<typeof SessionEventWriterFinishIdleEnvelopeSchema>;

export const SessionEventWriterRuntimeTerminationEnvelopeSchema = z.strictObject({
  requestId: SanitizedIdentifierSchema,
  workspaceId: SanitizedIdentifierSchema,
  sessionId: SanitizedIdentifierSchema,
  sessionThreadId: SanitizedIdentifierSchema,
  bindingId: SanitizedIdentifierSchema,
  bindingGeneration: NonNegativeIntegerSchema,
  targetPodUid: SanitizedIdentifierSchema,
  writeId: SanitizedIdentifierSchema,
  failure: RuntimeFailureSchema,
  drafts: z.array(RuntimeMessageDraftSchema),
  pendingToolCancellations: z.array(z.strictObject({
    toolUseEventId: SanitizedIdentifierSchema,
    runtimeLocalId: SanitizedIdentifierSchema,
  })),
}).superRefine((envelope, context) => {
  const cancellationDrafts = envelope.drafts.filter((draft) => draft.draftKind === "termination");
  const completionDrafts = envelope.drafts.filter((draft) => draft.draftKind === "completion_mail");
  if (
    cancellationDrafts.length + completionDrafts.length !== envelope.drafts.length ||
    completionDrafts.length > 1 ||
    envelope.drafts.some((draft, ordinal) =>
      draft.sourceKind !== "runtime_termination" ||
      draft.sourceId !== envelope.writeId ||
      draft.sourceEventId !== undefined ||
      draft.ordinal !== ordinal
    )
  ) {
    context.addIssue({ code: "custom", message: "runtime termination draft set is invalid" });
  }
  const cancellationIDs = new Set(cancellationDrafts.map((draft) => draft.runtimeLocalId));
  const toolUseIDs = new Set(envelope.pendingToolCancellations.map((pending) => pending.toolUseEventId));
  if (
    cancellationIDs.size !== cancellationDrafts.length ||
    toolUseIDs.size !== envelope.pendingToolCancellations.length ||
    cancellationDrafts.length !== envelope.pendingToolCancellations.length ||
    envelope.pendingToolCancellations.some((pending) => !cancellationIDs.has(pending.runtimeLocalId)) ||
    cancellationDrafts.some((draft) =>
      draft.role !== "assistant" ||
      draft.origin !== "agent" ||
      draft.status !== "completed" ||
      draft.parts.length !== 1 ||
      draft.parts[0]?.type !== "tool" ||
      draft.parts[0].state.status !== "cancelled"
    )
  ) {
    context.addIssue({ code: "custom", message: "runtime termination pending-tool declaration is incomplete" });
  }
  if (completionDrafts.some((draft) =>
    draft.role !== "user" ||
    draft.origin !== "runtime" ||
    draft.status !== "completed" ||
    draft.parts.length !== 1 ||
    draft.parts[0]?.type !== "text"
  )) {
    context.addIssue({ code: "custom", message: "runtime termination completion mail is invalid" });
  }
});
export type SessionEventWriterRuntimeTerminationEnvelope = z.infer<typeof SessionEventWriterRuntimeTerminationEnvelopeSchema>;

// This timeout bounds both each ordinary transport attempt and each failed-run
// closeout observation window; the latter never cancels its in-flight transport.
export const SessionEventWriterRetryPolicy = {
  attempts: 3,
  timeoutPerAttemptMs: 3000,
  backoffMs: [100, 300],
} as const;

export const SessionEventWriterErrorSchema = z.strictObject({
  type: z.literal("session-event-writer"),
  code: z.enum(["unavailable", "timeout", "schema_mismatch", "ack_mismatch", "superseded", "unrepairable", "unknown"]),
  message: SanitizedTextSchema,
  retryable: z.boolean(),
  fatal: z.boolean(),
  sessionId: SanitizedIdentifierSchema.optional(),
  writeId: SanitizedIdentifierSchema.optional(),
});
export type SessionEventWriterError = z.infer<typeof SessionEventWriterErrorSchema>;

export const SessionEventWriterAppendResultSchema = z.discriminatedUnion("ok", [
  z.strictObject({
    ok: z.literal(true),
      writeId: SanitizedIdentifierSchema,
      eventId: SanitizedIdentifierSchema,
      processedAt: TimestampSchema,
      declaration: z.strictObject({
        receipt: z.custom<RuntimeDeclarationReceipt>(),
        relatedReceipts: z.array(z.custom<RuntimeDeclarationReceipt>()).optional(),
        applicationDisposition: z.enum(["current_custody", "stale_custody"]),
        observedBindingId: SanitizedIdentifierSchema,
        observedBindingGeneration: NonNegativeIntegerSchema,
      }).optional(),
      rescheduleDisposition: z.discriminatedUnion("status", [
      z.strictObject({
        status: z.literal("accepted"),
        attempt: z.number().int().positive(),
        effectiveDeadline: TimestampSchema,
      }),
      z.strictObject({
        status: z.literal("denied"),
        reason: z.enum(["attempt_mismatch", "budget_exhausted"]),
        attempt: z.number().int().nonnegative(),
      }),
    ]).optional(),
  }),
  z.strictObject({
    ok: z.literal(false),
    error: SessionEventWriterErrorSchema,
  }),
]);
export type SessionEventWriterAppendResult = z.infer<typeof SessionEventWriterAppendResultSchema>;

/** Durable event port whose acknowledgements gate the corresponding hot projection. */
export interface SessionEventWriter {
  /**
   * Appends one Bridge-bound event before its matching event projection mutates hot
   * context. The envelope excludes final identifiers and timestamps. Any retry
   * must reuse the same writeId; a successful result returns eventId and
   * processedAt only after acknowledgement.
   */
  readonly append: (envelope: SessionEventEnvelope) => Promise<SessionEventWriterAppendResult>;
  readonly writeRequestEnd: (envelope: SessionEventWriterRequestEndEnvelope) => Promise<SessionEventWriterAppendResult>;
  readonly finishIdle?: (envelope: SessionEventWriterFinishIdleEnvelope) => Promise<SessionEventWriterAppendResult>;
  readonly commitRuntimeTermination?: (envelope: SessionEventWriterRuntimeTerminationEnvelope) => Promise<SessionEventWriterAppendResult>;
}

const AbortSignalLikeSchema = z.custom<AbortSignal>(
  (value) =>
    typeof value === "object" &&
    value !== null &&
    "aborted" in value &&
    "addEventListener" in value &&
    "removeEventListener" in value,
  "AbortSignal",
);
const OperationClockSchema = z.custom<() => number>((value) => typeof value === "function", "operation clock");
const OperationSleepSchema = z.custom<(durationMs: number, signal: AbortSignal) => Promise<boolean>>(
  (value) => typeof value === "function",
  "operation sleep",
);

/** Cancellation and deadline controls for the Bridge-backed internal repair declaration. */
export const RuntimeDeclarationOperationControlsSchema = z
  .strictObject({
    signal: AbortSignalLikeSchema,
    timeoutMs: PositiveIntegerSchema.optional(),
    deadlineEpochMs: PositiveIntegerSchema.optional(),
    nowEpochMs: OperationClockSchema.optional(),
    sleep: OperationSleepSchema,
  })
  .refine(
    (controls) => controls.timeoutMs !== undefined || controls.deadlineEpochMs !== undefined,
    "runtime message store operation controls require timeoutMs or deadlineEpochMs",
  )
  .refine(
    (controls) => controls.deadlineEpochMs === undefined || controls.nowEpochMs !== undefined,
    "runtime message store deadline controls require nowEpochMs",
  );
export type RuntimeDeclarationOperationControls = z.infer<typeof RuntimeDeclarationOperationControlsSchema>;

type RuntimeDeclarationRawOperationStart = () => () => void;
const RuntimeDeclarationRawOperationOwners = new WeakMap<AbortSignal, RuntimeDeclarationRawOperationStart>();

/** Internal hot-execution lifecycle hook. It is keyed by the local operation
 * signal and never crosses the Runtime/Bridge contract boundary. */
export function ownRuntimeDeclarationRawOperations(
  signal: AbortSignal,
  start: RuntimeDeclarationRawOperationStart,
): () => void {
  RuntimeDeclarationRawOperationOwners.set(signal, start);
  return () => {
    if (RuntimeDeclarationRawOperationOwners.get(signal) === start) {
      RuntimeDeclarationRawOperationOwners.delete(signal);
    }
  };
}

const RuntimeDeclarationApplicationSchema = z.strictObject({
  receipt: z.custom<RuntimeDeclarationReceipt>(),
  applicationDisposition: z.enum(["current_custody", "stale_custody"]),
  observedBindingId: SanitizedIdentifierSchema,
  observedBindingGeneration: NonNegativeIntegerSchema,
});

export const RuntimeInternalToolRepairCommitResultSchema = z.discriminatedUnion("ok", [
  z.strictObject({
    ok: z.literal(true),
    eventId: RuntimeIdentifierSchema,
    declaration: RuntimeDeclarationApplicationSchema,
  }),
  z.strictObject({
    ok: z.literal(false),
    error: RuntimeMessageStoreErrorSchema,
  }),
]);
export type RuntimeInternalToolRepairCommitResult = z.infer<typeof RuntimeInternalToolRepairCommitResultSchema>;

const RawRuntimeInternalToolRepairCommitResultSchema = RuntimeInternalToolRepairCommitResultSchema;

/** Injectable clock, identity, and cancellation-aware sleep dependencies for Runtime Core. */
export interface RuntimeDependencies {
  readonly now: () => string;
  readonly monotonicMs: () => number;
  readonly createId: (prefix: string) => string;
  readonly sleep: (durationMs: number, signal: AbortSignal) => Promise<boolean>;
}

const StoreErrorRetryableCodes = new Set<(typeof RuntimeMessageStoreErrorCodes)[number]>([
  "unavailable",
  "timeout",
  "serialization_failure",
]);

export interface RuntimeMessageStoreErrorInput {
  readonly code?: RuntimeMessageStoreError["code"];
  readonly operation: "commitInternalToolRepair";
  readonly rawError?: unknown;
  readonly reason?: z.infer<typeof SafeReasonCodeSchema> | undefined;
  readonly constraint?: string | undefined;
  readonly status?: z.infer<typeof SafeTerminalStatusSchema> | undefined;
  readonly attemptedStatus?: z.infer<typeof SafeTerminalStatusSchema> | undefined;
  readonly messageId?: string | undefined;
  readonly partId?: string | undefined;
  readonly sessionId?: string | undefined;
}

/** Converts store-specific failures into the bounded Runtime message-store error shape. */
export function normalizeRuntimeMessageStoreError(input: RuntimeMessageStoreErrorInput): RuntimeMessageStoreError {
  const code = input.code ?? "unknown";
  return RuntimeMessageStoreErrorSchema.parse({
    type: "message-store",
    code,
    operation: input.operation,
    message: "Runtime message store operation failed.",
    retryable: StoreErrorRetryableCodes.has(code),
    fatal: code === "schema_mismatch" || code === "constraint_violation",
    ...(input.reason !== undefined ? { reason: input.reason } : {}),
    ...(input.constraint !== undefined ? { constraint: sanitizeRuntimeText(input.constraint) } : {}),
    ...(input.status !== undefined ? { status: input.status } : {}),
    ...(input.attemptedStatus !== undefined ? { attemptedStatus: input.attemptedStatus } : {}),
    ...(input.messageId !== undefined ? { messageId: sanitizeRuntimeText(input.messageId) } : {}),
    ...(input.partId !== undefined ? { partId: sanitizeRuntimeText(input.partId) } : {}),
    ...(input.sessionId !== undefined ? { sessionId: sanitizeRuntimeText(input.sessionId) } : {}),
  });
}

export interface ContextLoaderErrorInput {
  readonly code?: ContextLoaderError["code"];
  readonly rawError?: unknown;
  readonly sessionId?: string | undefined;
  readonly reason?: string | undefined;
}

/** Converts context-loading failures into the bounded Runtime loader error shape. */
export function normalizeContextLoaderError(input: ContextLoaderErrorInput): ContextLoaderError {
  const code = input.code ?? "unknown";
  return {
    type: "context-loader",
    code,
    message: "Context loader operation failed.",
    retryable: code === "unavailable" || code === "timeout",
    fatal: code === "schema_mismatch" || code === "wrong_session" || code === "bounds_exceeded" || code === "unsafe_payload",
    ...(input.sessionId !== undefined ? { sessionId: sanitizeRuntimeText(input.sessionId) } : {}),
    ...(input.reason !== undefined ? { reason: sanitizeRuntimeText(input.reason) } : {}),
  };
}

export interface SessionEventWriterErrorInput {
  readonly code?: SessionEventWriterError["code"];
  readonly rawError?: unknown;
  readonly sessionId?: string | undefined;
  readonly writeId?: string | undefined;
}

/** Converts Bridge event-write failures into the bounded Runtime writer error shape. */
export function normalizeSessionEventWriterError(input: SessionEventWriterErrorInput): SessionEventWriterError {
  const code = input.code ?? "unknown";
  return {
    type: "session-event-writer",
    code,
    message: "Session event writer operation failed.",
    retryable: code === "unavailable" || code === "timeout",
    fatal: code === "schema_mismatch" || code === "ack_mismatch" || code === "unrepairable",
    ...(input.sessionId !== undefined ? { sessionId: sanitizeRuntimeText(input.sessionId) } : {}),
    ...(input.writeId !== undefined ? { writeId: sanitizeRuntimeText(input.writeId) } : {}),
  };
}

export interface RuntimeFailureInput {
  readonly type: RuntimeFailure["type"];
  readonly code?: RuntimeFailure["code"];
  readonly rawError?: unknown;
  readonly retryable?: boolean | undefined;
  readonly fatal?: boolean | undefined;
  readonly operation?: RuntimeFailure["operation"] | undefined;
  readonly reason?: RuntimeFailure["reason"] | undefined;
  readonly retryStatus?: RuntimeFailure["retryStatus"] | undefined;
  readonly sessionId?: string | undefined;
  readonly messageId?: string | undefined;
  readonly partId?: string | undefined;
  readonly providerId?: RuntimeFailure["providerId"] | undefined;
  readonly modelId?: string | undefined;
}

/** Builds the canonical internal failure without retaining raw dependency errors. */
export function normalizeRuntimeFailure(input: RuntimeFailureInput): RuntimeFailure {
  const code = input.code ?? (input.type === "runtime" ? "runtime_invalid_sequence" : "unknown");
  return RuntimeFailureSchema.parse({
    type: input.type,
    code,
    message: "Runtime operation failed.",
    retryable: input.retryable ?? false,
    fatal: input.fatal ?? true,
    ...(input.retryStatus !== undefined ? { retryStatus: input.retryStatus } : {}),
    ...(input.operation !== undefined ? { operation: input.operation } : {}),
    ...(input.reason !== undefined ? { reason: input.reason } : {}),
    ...(input.sessionId !== undefined ? { sessionId: sanitizeRuntimeText(input.sessionId) } : {}),
    ...(input.messageId !== undefined ? { messageId: sanitizeRuntimeText(input.messageId) } : {}),
    ...(input.partId !== undefined ? { partId: sanitizeRuntimeText(input.partId) } : {}),
    ...(input.providerId !== undefined ? { providerId: input.providerId } : {}),
    ...(input.modelId !== undefined ? { modelId: sanitizeRuntimeText(input.modelId) } : {}),
  });
}

function sanitizeRuntimeFailure(failure: RuntimeFailure): RuntimeFailure {
  return RuntimeFailureSchema.parse({
    ...failure,
    message: sanitizeRuntimeText(failure.message),
    ...(failure.sessionId !== undefined ? { sessionId: sanitizeRuntimeText(failure.sessionId) } : {}),
    ...(failure.messageId !== undefined ? { messageId: sanitizeRuntimeText(failure.messageId) } : {}),
    ...(failure.partId !== undefined ? { partId: sanitizeRuntimeText(failure.partId) } : {}),
    ...(failure.modelId !== undefined ? { modelId: sanitizeRuntimeText(failure.modelId) } : {}),
  });
}

function declarationSchemaMismatch(): RuntimeMessageStoreError {
  return normalizeRuntimeMessageStoreError({
    code: "schema_mismatch",
    operation: "commitInternalToolRepair",
    reason: "runtime_contract_validation",
  });
}

function caughtDeclarationStoreError(error: unknown): RuntimeMessageStoreError {
  if (error instanceof RuntimeDeclarationOperationError) {
    return normalizeRuntimeMessageStoreError({
      code: error.storeError.code,
      operation: "commitInternalToolRepair",
      reason: error.storeError.reason,
      constraint: error.storeError.constraint,
      status: error.storeError.status,
      attemptedStatus: error.storeError.attemptedStatus,
      messageId: error.storeError.messageId,
      partId: error.storeError.partId,
      sessionId: error.storeError.sessionId,
    });
  }
  const errorRecord = typeof error === "object" && error !== null ? (error as Readonly<Record<string, unknown>>) : {};
  const code = RuntimeMessageStoreErrorSchema.shape.code.safeParse(errorRecord.code);
  return normalizeRuntimeMessageStoreError({
    code: code.success ? code.data : "unknown",
    operation: "commitInternalToolRepair",
    rawError: error,
  });
}

/** Error wrapper that preserves a normalized declaration failure across an async implementation. */
export class RuntimeDeclarationOperationError extends Error {
  readonly storeError: RuntimeMessageStoreError;

  constructor(storeError: RuntimeMessageStoreError) {
    super(storeError.message);
    this.name = "RuntimeDeclarationOperationError";
    this.storeError = storeError;
  }
}

/**
 * Validating Bridge-backed port for the one internal repair declaration that
 * is not authored as a public WriteEvent.
 */
export abstract class RuntimeInternalToolRepairStore {
  async commitInternalToolRepair(repair: unknown, controls: unknown): Promise<RuntimeInternalToolRepairCommitResult> {
    const parsedRepair = RuntimeInternalToolRepairCommitSchema.safeParse(repair);
    const parsedControls = RuntimeDeclarationOperationControlsSchema.safeParse(controls);
    if (!parsedRepair.success || !parsedControls.success) {
      return { ok: false, error: declarationSchemaMismatch() };
    }
    const preflightFailure = operationPreflightFailure(parsedControls.data);
    if (preflightFailure !== undefined) {
      return { ok: false, error: preflightFailure };
    }
    try {
      const result = await withBoundedStoreOperation(
        (operationControls) => this.commitInternalToolRepairRecord(parsedRepair.data, operationControls),
        parsedControls.data,
      );
      const parsedResult = RawRuntimeInternalToolRepairCommitResultSchema.safeParse(result);
      if (!parsedResult.success) {
        return { ok: false, error: declarationSchemaMismatch() };
      }
      if (parsedResult.data.ok === false) {
        return {
          ok: false,
          error: normalizeRuntimeMessageStoreError({
            code: parsedResult.data.error.code,
            operation: "commitInternalToolRepair",
            reason: parsedResult.data.error.reason,
            constraint: parsedResult.data.error.constraint,
            status: parsedResult.data.error.status,
            attemptedStatus: parsedResult.data.error.attemptedStatus,
            messageId: parsedResult.data.error.messageId,
            partId: parsedResult.data.error.partId,
            sessionId: parsedResult.data.error.sessionId,
          }),
        };
      }
      return parsedResult.data;
    } catch (error) {
      return { ok: false, error: caughtDeclarationStoreError(error) };
    }
  }

  protected abstract commitInternalToolRepairRecord(
    repair: RuntimeInternalToolRepairCommit,
    controls: RuntimeDeclarationOperationControls,
  ): Promise<unknown>;
}

function operationPreflightFailure(
  controls: RuntimeDeclarationOperationControls,
): RuntimeMessageStoreError | undefined {
  if (controls.signal.aborted) {
    return normalizeRuntimeMessageStoreError({
      code: "timeout",
      operation: "commitInternalToolRepair",
      reason: "aborted",
    });
  }
  if (remainingOperationBudgetMs(controls) <= 0) {
    return normalizeRuntimeMessageStoreError({
      code: "timeout",
      operation: "commitInternalToolRepair",
      reason: "timeout",
    });
  }
  return undefined;
}

function remainingOperationBudgetMs(controls: RuntimeDeclarationOperationControls): number {
  const budgets: number[] = [];
  if (controls.timeoutMs !== undefined) {
    budgets.push(controls.timeoutMs);
  }
  if (controls.deadlineEpochMs !== undefined && controls.nowEpochMs !== undefined) {
    budgets.push(controls.deadlineEpochMs - controls.nowEpochMs());
  }
  return budgets.length === 0 ? 0 : Math.min(...budgets);
}

async function pendingForever(): Promise<never> {
  return await new Promise<never>(() => undefined);
}

async function withBoundedStoreOperation<T, TControls extends RuntimeDeclarationOperationControls>(
  operation: (controls: TControls) => Promise<T>,
  controls: TControls,
): Promise<T> {
  const operationController = new AbortController();
  const abortOperation = (): void => operationController.abort();
  const operationControls = {
    ...controls,
    signal: operationController.signal,
  } as TControls;
  let abortOperationPromise: ((result: { readonly kind: "aborted" }) => void) | undefined;
  const resolveAbortOperation = (): void => abortOperationPromise?.({ kind: "aborted" });
  controls.signal.addEventListener("abort", abortOperation, { once: true });
  const abortPromise = new Promise<{ readonly kind: "aborted" }>((resolve) => {
    abortOperationPromise = resolve;
    controls.signal.addEventListener("abort", resolveAbortOperation, { once: true });
  });
  const timeoutPromise = controls.sleep(remainingOperationBudgetMs(controls), operationController.signal)
    .then(async (elapsed) => (elapsed ? { kind: "timeout" as const } : await pendingForever()));
  const finishOwnedRawOperation = RuntimeDeclarationRawOperationOwners.get(controls.signal)?.();
  let rawOperation: Promise<T>;
  try {
    rawOperation = operation(operationControls);
  } catch (error) {
    finishOwnedRawOperation?.();
    throw error;
  }
  const operationPromise = rawOperation.then(
    (value) => ({ kind: "value" as const, value }),
    (error: unknown) => ({ kind: "error" as const, error }),
  ).finally(() => finishOwnedRawOperation?.());

  try {
    const result = await Promise.race([operationPromise, timeoutPromise, abortPromise]);
    if (result.kind === "value") {
      return result.value;
    }
    if (result.kind === "error") {
      throw result.error;
    }
    operationController.abort();
    if (finishOwnedRawOperation !== undefined) {
      const ownedResult = await operationPromise;
      if (ownedResult.kind === "value") {
        return ownedResult.value;
      }
      throw ownedResult.error;
    }
    throw new RuntimeDeclarationOperationError(
      normalizeRuntimeMessageStoreError({
        code: "timeout",
        operation: "commitInternalToolRepair",
        reason: result.kind,
      }),
    );
  } finally {
    controls.signal.removeEventListener("abort", abortOperation);
    controls.signal.removeEventListener("abort", resolveAbortOperation);
    operationController.abort();
  }
}

function byteLength(value: string): number {
  return Utf8Encoder.encode(value).length;
}

function utf8Prefix(input: string, maxBytes: number): string {
  if (maxBytes <= 0) {
    return "";
  }
  let usedBytes = 0;
  let output = "";
  for (const character of input) {
    const characterBytes = byteLength(character);
    if (usedBytes + characterBytes > maxBytes) {
      break;
    }
    output += character;
    usedBytes += characterBytes;
  }
  return output;
}

/** Truncates text on a UTF-8 boundary and reports whether bytes were removed. */
export function boundRuntimeText(input: string, maxBytes: number): RuntimeBoundedText {
  const safeMaxBytes = Math.max(0, Math.floor(maxBytes));
  const originalBytes = byteLength(input);
  if (originalBytes <= safeMaxBytes) {
    return { text: input, truncated: false };
  }
  const text = utf8Prefix(input, safeMaxBytes);
  return {
    text,
    truncated: true,
  };
}

function jsonPreview(input: unknown): string {
  if (isRuntimeJsonValue(input)) {
    return JSON.stringify(input);
  }
  if (typeof input === "string") {
    return input;
  }
  if (typeof input === "number" || typeof input === "boolean" || input === null) {
    return JSON.stringify(input);
  }
  return "[non-json-runtime-value]";
}

/** Produces a bounded JSON preview while retaining the value only when it fits intact. */
export function boundRuntimeJson(input: unknown, maxBytes: number): RuntimeBoundedJson {
  const preview = jsonPreview(input);
  const boundedPreview = boundRuntimeText(preview, maxBytes);
  return {
    ...(isRuntimeJsonValue(input) && !boundedPreview.truncated ? { value: input } : {}),
    preview: boundedPreview.text,
    truncated: boundedPreview.truncated,
  };
}

export function boundRuntimeToolError(input: RuntimeToolError, maxBytes: number): RuntimeToolError {
  const boundedMessage = boundRuntimeText(input.message, maxBytes);
  return RuntimeToolErrorSchema.parse({
    type: input.type,
    message: boundedMessage.text,
    ...(input.retryable !== undefined ? { retryable: input.retryable } : {}),
  });
}

function boundExistingRuntimeBoundedText(input: RuntimeBoundedText, maxBytes: number): RuntimeBoundedText {
  const bounded = boundRuntimeText(input.text, maxBytes);
  return {
    text: bounded.text,
    truncated: input.truncated || bounded.truncated,
  };
}

function boundExistingRuntimeBoundedJson(input: RuntimeBoundedJson, maxBytes: number): RuntimeBoundedJson {
  const runtimeValue = input.value;
  const boundedPreview = boundRuntimeText(input.preview, maxBytes);
  const serializedValue = runtimeValue === undefined ? undefined : JSON.stringify(runtimeValue);
  const valueFits =
    serializedValue !== undefined && byteLength(serializedValue) <= maxBytes && !boundedPreview.truncated;
  return {
    ...(valueFits ? { value: runtimeValue } : {}),
    preview: boundedPreview.text,
    truncated: input.truncated || boundedPreview.truncated || !valueFits,
  };
}

function boundToolState(state: ToolState, maxBytes: number): ToolState {
  switch (state.status) {
    case "pending":
      return {
        status: "pending",
      };
    case "running":
      return {
        status: "running",
        input: boundExistingRuntimeBoundedJson(state.input, maxBytes),
      };
    case "completed":
      return {
        status: "completed",
        input: boundExistingRuntimeBoundedJson(state.input, maxBytes),
        // Tool routes apply their source-specific capture bound before the
        // result reaches Runtime Core. Re-bounding here would destroy
        // continuation metadata and narrower route-specific windows.
        output: RuntimeBoundedTextSchema.parse(state.output),
      };
    case "error":
      return {
        status: "error",
        ...(state.input !== undefined ? { input: boundExistingRuntimeBoundedJson(state.input, maxBytes) } : {}),
        error: boundRuntimeToolError(state.error, maxBytes),
      };
    case "cancelled":
      return {
        status: "cancelled",
        ...(state.input !== undefined ? { input: boundExistingRuntimeBoundedJson(state.input, maxBytes) } : {}),
        ...(state.error !== undefined ? { error: boundRuntimeToolError(state.error, maxBytes) } : {}),
      };
  }
}

/** Re-bounds one stable part without weakening route-specific tool output bounds. */
export function boundRuntimePartForStableWrite(part: RuntimePart, maxBytes: number = DefaultMaxNormalizedTextPreviewBytes): RuntimePart {
  switch (part.type) {
    case "text": {
      const bounded = boundRuntimeText(part.text, maxBytes);
      return RuntimePartSchema.parse({
        ...part,
        text: bounded.text,
        truncated: part.truncated || bounded.truncated,
      });
    }
    case "reasoning": {
      const bounded = boundRuntimeText(part.text, maxBytes);
      return RuntimePartSchema.parse({
        ...part,
        text: bounded.text,
        truncated: part.truncated || bounded.truncated,
      });
    }
    case "tool":
      return RuntimePartSchema.parse({
        ...part,
        state: boundToolState(part.state, maxBytes),
      });
    case "step-start":
    case "step-finish":
      return part;
  }
}

export function createRuntimeId(deps: RuntimeDependencies, prefix: string): string {
  return deps.createId(prefix);
}

export function runtimeNow(deps: RuntimeDependencies): string {
  return deps.now();
}

export async function runtimeSleep(deps: RuntimeDependencies, durationMs: number, signal: AbortSignal): Promise<boolean> {
  return await deps.sleep(durationMs, signal);
}
