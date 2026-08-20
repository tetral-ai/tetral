/**
 * This module provides Runtime Core's shared domain and integration-port boundary definitions.
 * It guards validated message, part, event, failure, settlement, and store shapes
 * plus the byte bounds imposed at stable-write and transport edges. The session
 * processor, ThreadLoop, tool runner, context loader, and Bridge adapters consume
 * it; it composes provider schemas and invokes injected store operations through
 * validating wrappers.
 *
 * @packageDocumentation
 */

import {
	MaxIdBytes,
	MaxProviderContextTextJsonBytes,
	MaxProviderRequestToolOutputJsonBytes,
	MaxProviderToolCallInputJsonBytes,
} from "@tetral/gateway-protocol/src/bounds.js";
import { z } from "zod/v4";
import { RuntimePreviewTextMaxBytes } from "../llm/llm-event.js";
import type { ProviderId } from "./provider.js";
import {
	ProviderErrorCodes,
	ProviderMetadataSchema as RawProviderMetadataSchema,
} from "./provider.js";

export type {
	LLMEvent,
	RuntimeFailure as LLMRuntimeFailure,
	RuntimeJsonPreview as LLMRuntimeJsonPreview,
	RuntimeUsage as LLMRuntimeUsage,
} from "../llm/llm-event.js";
export {
	LLMEventSchema,
	LLMEventTypes,
	RuntimeFailureSchema as LLMRuntimeFailureSchema,
	RuntimeJsonPreviewSchema as LLMRuntimeJsonPreviewSchema,
	RuntimeUsageSchema as LLMRuntimeUsageSchema,
} from "../llm/llm-event.js";

/** Provider and model identity selected for one Runtime processor invocation. */
export interface RuntimeProcessorSource {
	readonly providerId: ProviderId;
	readonly modelId: string;
}

export interface SessionEventWriterServerToolUse {
	readonly webSearchRequests: number;
	readonly webFetchRequests: number;
}

/** Runtime-owned attachment custody retained until a settled provider request consumes it. */
export type RuntimeProviderAttachment =
	| {
			readonly transient: {
				readonly attachmentRef: string;
				readonly sourcePath: string;
				readonly pageRange: string;
				readonly detail: string;
			};
			readonly fileBacked?: undefined;
			readonly mime: string;
			readonly filename: string;
	  }
	| {
			readonly transient?: undefined;
			readonly fileBacked: {
				readonly sourceEventId: string;
				readonly fileId: string;
			};
			readonly mime: string;
			readonly filename: string;
	  };

/** Final disposition returned by a Runtime tool route to its request turn. */
export type RuntimeToolSettlement =
	| {
			readonly type: "completed";
			readonly output: RuntimeBoundedText;
			readonly serverToolUse?: SessionEventWriterServerToolUse;
	  }
	| {
			readonly type: "error";
			readonly error: RuntimeFailure;
			readonly serverToolUse?: SessionEventWriterServerToolUse;
	  }
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

/** Encodes the exact reasoning metadata bytes sent to and counted by Bridge. */
// UPDATE-WITH: services/bridge/bridge_api_settlement.go
// (normalizeStableReasoningParts metadataJSON aggregate accounting).
export function stableReasoningMetadataJSON(
	metadata: RuntimeJsonValue | undefined,
): string {
	return JSON.stringify(metadata ?? {});
}
const SafeOperationNameSchema = z.enum(["commitInternalToolRepair"]);
const SafeReasonCodeSchema = z.enum([
	"aborted",
	"bounded",
	"gateway_transport_completion_deadline",
	"runtime_contract_validation",
	"runtime_input_commit_exhausted",
	"runtime_shutdown",
	"timeout",
	"write_acknowledgement_mismatch",
]);
const SafeTerminalStatusSchema = z.enum(["completed", "failed", "cancelled"]);
const Utf8Encoder = new TextEncoder();

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

function sanitizeRuntimeText(input: string): string {
	return SensitiveTextPatterns.reduce(
		(output, pattern) =>
			output.replace(pattern, (match) => redactedRuntimeToken(match)),
		input,
	);
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
const SanitizedIdentifierSchema =
	IdentifierSchema.transform(sanitizeRuntimeText);
const RuntimeTextSchema = z
	.string()
	.refine(
		hasOnlyUnicodeScalars,
		"text must contain only Unicode scalar values",
	);
const RuntimeIdentifierSchema = IdentifierSchema;
const RuntimeContextIdentifierSchema = RuntimeIdentifierSchema.refine(
	hasOnlyUnicodeScalars,
	"identifier must contain only Unicode scalar values",
).refine(
	(value) => byteLength(value) <= MaxIdBytes,
	`identifier must be at most ${MaxIdBytes} UTF-8 bytes`,
);
const RuntimeContextTextSchema = RuntimeTextSchema.refine(
	(value) =>
		byteLength(JSON.stringify(value)) <= MaxProviderContextTextJsonBytes,
	`context text JSON must be at most ${MaxProviderContextTextJsonBytes} UTF-8 bytes`,
);

const ProviderMetadataSchema = RawProviderMetadataSchema.refine(
	(metadata) =>
		isRuntimeJsonValue(metadata) &&
		byteLength(JSON.stringify(metadata)) <= RuntimeProviderMetadataMaxBytes,
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
export type RuntimeRequestErrorKind = z.infer<
	typeof RuntimeRequestErrorKindSchema
>;

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

// Measures the exact one-time {text} JSON shape projected to Gateway.
// UPDATE-WITH: services/gateway/packages/protocol/src/bounds.ts
// (MaxProviderRequestToolOutputJsonBytes); services/web-connector/types.go
// (maxModelVisibleToolOutputJSONBytes, maxVisibleResultBytes).
export const RuntimeBoundedTextSchema = z
	.strictObject({
		text: RuntimeTextSchema,
		truncated: z.boolean(),
	})
	.refine(
		(value) =>
			byteLength(JSON.stringify({ text: value.text })) <=
			MaxProviderRequestToolOutputJsonBytes,
		`tool output JSON must be at most ${MaxProviderRequestToolOutputJsonBytes} UTF-8 bytes`,
	);
export type RuntimeBoundedText = z.infer<typeof RuntimeBoundedTextSchema>;

export const RuntimeToolOutputTruncationNotice =
	"\n\n[Tool output was truncated to fit the provider context limit.]";

const RuntimeContextToolOutputSchema = z
	.strictObject({ text: RuntimeTextSchema })
	.refine(
		(value) =>
			byteLength(JSON.stringify(value)) <=
			MaxProviderRequestToolOutputJsonBytes,
		`tool output JSON must be at most ${MaxProviderRequestToolOutputJsonBytes} UTF-8 bytes`,
	);

/** Produces the final provider-visible Tool text while retaining internal truncation evidence. */
export function finalizeRuntimeToolOutput(
	output: RuntimeBoundedText,
): RuntimeBoundedText {
	if (!output.truncated) return output;
	const characters = [...output.text];
	let low = 0;
	let high = characters.length;
	while (low < high) {
		const candidate = Math.ceil((low + high) / 2);
		const text =
			characters.slice(0, candidate).join("") +
			RuntimeToolOutputTruncationNotice;
		if (
			byteLength(JSON.stringify({ text, truncated: true })) <=
			MaxProviderRequestToolOutputJsonBytes
		) {
			low = candidate;
		} else {
			high = candidate - 1;
		}
	}
	return RuntimeBoundedTextSchema.parse({
		text:
			characters.slice(0, low).join("") + RuntimeToolOutputTruncationNotice,
		truncated: true,
	});
}

export type RuntimeJsonValue =
	| null
	| boolean
	| number
	| string
	| readonly RuntimeJsonValue[]
	| { readonly [key: string]: RuntimeJsonValue };
export type RuntimeJsonObject = { readonly [key: string]: RuntimeJsonValue };

function isRuntimeJsonValue(value: unknown): value is RuntimeJsonValue {
	if (value === null || typeof value === "boolean") {
		return true;
	}
	if (typeof value === "string") return hasOnlyUnicodeScalars(value);
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
	return Object.entries(value).every(
		([key, item]) => hasOnlyUnicodeScalars(key) && isRuntimeJsonValue(item),
	);
}

function hasOnlyUnicodeScalars(value: string): boolean {
	for (let index = 0; index < value.length; index += 1) {
		const code = value.charCodeAt(index);
		if (code >= 0xd800 && code <= 0xdbff) {
			const next = value.charCodeAt(index + 1);
			if (index + 1 >= value.length || next < 0xdc00 || next > 0xdfff)
				return false;
			index += 1;
		} else if (code >= 0xdc00 && code <= 0xdfff) {
			return false;
		}
	}
	return true;
}

function isRuntimeJsonObject(value: unknown): value is RuntimeJsonObject {
	return (
		typeof value === "object" &&
		value !== null &&
		!Array.isArray(value) &&
		Object.getPrototypeOf(value) === Object.prototype &&
		Object.entries(value).every(
			([key, item]) => hasOnlyUnicodeScalars(key) && isRuntimeJsonValue(item),
		)
	);
}

export const RuntimeJsonValueSchema = z.custom<RuntimeJsonValue>(
	isRuntimeJsonValue,
	"RuntimeJsonValue",
);
export const RuntimeJsonObjectSchema = z.custom<RuntimeJsonObject>(
	isRuntimeJsonObject,
	"RuntimeJsonObject",
);

export const RuntimeBoundedJsonSchema = z.strictObject({
	value: RuntimeJsonValueSchema.refine(
		(value) =>
			byteLength(JSON.stringify(value)) <= MaxProviderToolCallInputJsonBytes,
		`tool input must be at most ${MaxProviderToolCallInputJsonBytes} UTF-8 bytes`,
	),
	preview: RuntimeTextSchema.refine(
		(value) => byteLength(value) <= RuntimePreviewTextMaxBytes,
		`preview must be at most ${RuntimePreviewTextMaxBytes} UTF-8 bytes`,
	),
	truncated: z.boolean(),
});
export type RuntimeBoundedJson = z.infer<typeof RuntimeBoundedJsonSchema>;

export const RuntimeToolErrorSchema = z
	.strictObject({
		type: SanitizedIdentifierSchema,
		message: SanitizedTextSchema,
		retryable: z.boolean().optional(),
	})
	.refine(
		(value) =>
			hasOnlyUnicodeScalars(value.type) &&
			hasOnlyUnicodeScalars(value.message) &&
			byteLength(JSON.stringify({ error: value })) <=
				MaxProviderRequestToolOutputJsonBytes,
		`tool error JSON must be at most ${MaxProviderRequestToolOutputJsonBytes} UTF-8 bytes`,
	);
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

/** Provider-visible content retained after a durable context entry is sealed. */
export const RuntimeContextPartSchema = z.discriminatedUnion("type", [
	z.strictObject({
		type: z.literal("text"),
		text: RuntimeContextTextSchema,
	}),
	z.strictObject({
		type: z.literal("reasoning"),
		text: RuntimeContextTextSchema,
		providerMetadata: ProviderMetadataSchema.optional(),
	}),
	z.strictObject({
		type: z.literal("tool_call"),
		modelToolCallId: RuntimeContextIdentifierSchema,
		toolName: RuntimeContextIdentifierSchema,
		canonicalInput: RuntimeJsonValueSchema.refine(
			(value) =>
				byteLength(JSON.stringify(value)) <= MaxProviderToolCallInputJsonBytes,
			`tool input must be at most ${MaxProviderToolCallInputJsonBytes} UTF-8 bytes`,
		),
	}),
	z.strictObject({
		type: z.literal("tool_result"),
		modelToolCallId: RuntimeContextIdentifierSchema,
		result: z.discriminatedUnion("type", [
			z.strictObject({
				type: z.literal("completed"),
				output: RuntimeContextToolOutputSchema,
			}),
			z.strictObject({
				type: z.literal("error"),
				error: RuntimeToolErrorSchema,
			}),
			z.strictObject({
				type: z.literal("cancelled"),
			}),
		]),
	}),
]);
export type RuntimeContextPart = z.infer<typeof RuntimeContextPartSchema>;

export const RuntimeContextKindSchema = z.enum([
	"user",
	"assistant",
	"runtime_notification",
	"compaction",
]);
export type RuntimeContextKind = z.infer<typeof RuntimeContextKindSchema>;

/** Ordered sealed provider history. Database identities and audit time do not enter this model. */
export const RuntimeContextEntrySchema = z.strictObject({
	messageSequence: PositiveIntegerSchema,
	contextKind: RuntimeContextKindSchema,
	parts: z.array(RuntimeContextPartSchema),
});
export type RuntimeContextEntry = z.infer<typeof RuntimeContextEntrySchema>;

/** Assistant content persisted for an open Request but excluded from provider history. */
export const RuntimeOpenRequestDraftSchema = z.strictObject({
	modelRequestId: RuntimeContextIdentifierSchema,
	messageSequence: PositiveIntegerSchema,
	parts: z.array(RuntimeContextPartSchema),
});
export type RuntimeOpenRequestDraft = z.infer<
	typeof RuntimeOpenRequestDraftSchema
>;

/** Request-local Assistant member before its durable operation settles. */
export const RuntimeAssistantDraftPartSchema = z.discriminatedUnion("type", [
	z.strictObject({
		type: z.literal("text"),
		text: RuntimeContextTextSchema,
		truncated: z.boolean(),
	}),
	z.strictObject({
		type: z.literal("reasoning"),
		providerPartId: RuntimeIdentifierSchema.optional(),
		providerMetadata: ProviderMetadataSchema.optional(),
		text: RuntimeContextTextSchema,
		truncated: z.boolean(),
	}),
	z.strictObject({
		type: z.literal("tool"),
		modelToolCallId: RuntimeContextIdentifierSchema,
		toolName: RuntimeContextIdentifierSchema,
		toolUseEventId: RuntimeIdentifierSchema.optional(),
		toolEvent: z
			.discriminatedUnion("kind", [
				z.strictObject({ kind: z.literal("tool") }),
				z.strictObject({
					kind: z.literal("mcp"),
					mcpServerName: RuntimeIdentifierSchema,
				}),
			])
			.optional(),
		state: ToolStateSchema,
	}),
]);
export type RuntimeAssistantDraftPart = z.infer<
	typeof RuntimeAssistantDraftPartSchema
>;

/** One narrow Assistant context append; array order is durable content order. */
export const RuntimeAssistantContextAppendSchema = z.strictObject({
	parts: z.array(RuntimeAssistantDraftPartSchema).min(1),
});
export type RuntimeAssistantContextAppend = z.infer<
	typeof RuntimeAssistantContextAppendSchema
>;

export const RuntimeToolSettlementDeclarationSchema = z.strictObject({
	toolUseEventId: RuntimeIdentifierSchema,
	outcome: z.lazy(() => RuntimeToolSettlementSchema),
});
export type RuntimeToolSettlementDeclaration = z.infer<
	typeof RuntimeToolSettlementDeclarationSchema
>;

export interface RuntimeInterruptToolResult {
	readonly toolUseEventId: string;
	readonly result:
		| { readonly type: "error"; readonly error: RuntimeToolError }
		| { readonly type: "cancelled" };
}

/** One self-contained invalid-tool repair committed before it enters hot history. */
export const RuntimeInternalToolRepairCommitSchema = z.strictObject({
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
	canonicalInput: RuntimeJsonValueSchema,
	error: RuntimeToolErrorSchema,
});
export type RuntimeInternalToolRepairCommit = z.infer<
	typeof RuntimeInternalToolRepairCommitSchema
>;

const RequestInputContextEntrySchema = RuntimeContextEntrySchema.refine(
	(entry) =>
		entry.contextKind !== "assistant" && entry.contextKind !== "compaction",
	"request input context must originate outside the assistant request",
);

// Runtime carries only the newly committed context delta into request assembly.
export const PendingInputResultSchema = z.discriminatedUnion("type", [
	z.strictObject({
		type: z.literal("context"),
		entries: z.array(RequestInputContextEntrySchema).min(1),
	}),
	z.strictObject({
		type: z.literal("empty"),
	}),
]);
export type PendingInputResult = z.infer<typeof PendingInputResultSchema>;

export const ContextLoaderErrorCodes = [
	"unavailable",
	"timeout",
	"superseded",
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

export const RuntimeInternalToolRepairStoreErrorCodes = [
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
	...RuntimeInternalToolRepairStoreErrorCodes,
	"ack_mismatch",
	"gateway_protocol_error",
	"gateway_stream_error",
	"gateway_unavailable",
	"runtime_invalid_sequence",
	"runtime_persistence_exhausted",
]);

export const RuntimeInternalToolRepairStoreErrorSchema = z.strictObject({
	type: z.literal("message-store"),
	code: z.enum(RuntimeInternalToolRepairStoreErrorCodes),
	operation: SafeOperationNameSchema,
	message: SanitizedTextSchema,
	retryable: z.boolean(),
	fatal: z.boolean(),
	reason: SafeReasonCodeSchema.optional(),
	constraint: SanitizedIdentifierSchema.optional(),
	status: SafeTerminalStatusSchema.optional(),
	attemptedStatus: SafeTerminalStatusSchema.optional(),
	sessionId: SanitizedIdentifierSchema.optional(),
});
export type RuntimeInternalToolRepairStoreError = z.infer<
	typeof RuntimeInternalToolRepairStoreErrorSchema
>;

const RuntimeFailureRetryStatusSchema = z.discriminatedUnion("type", [
	z.strictObject({
		type: z.literal("retrying"),
		attempt: NonNegativeIntegerSchema,
	}),
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
	type: z.enum([
		"provider",
		"message-store",
		"session-event-writer",
		"session-binding",
		"runtime",
	]),
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
	sessionId: SanitizedIdentifierSchema.optional(),
	providerId: SanitizedIdentifierSchema.optional(),
	modelId: SanitizedIdentifierSchema.optional(),
	statusCode: NonNegativeIntegerSchema.optional(),
	retryAfterMs: NonNegativeIntegerSchema.optional(),
});
export type RuntimeFailure = z.infer<typeof RuntimeFailureSchema>;

/** Projects Runtime-owned lifecycle failure state into the stable durable Tool error. */
export function runtimeToolErrorFromFailure(
	failure: RuntimeFailure,
): RuntimeToolError {
	return RuntimeToolErrorSchema.parse({
		type: failure.code,
		message: failure.message,
		retryable: failure.retryable,
	});
}

const RuntimeToolSettlementSchema = z.discriminatedUnion("type", [
	z.strictObject({
		type: z.literal("completed"),
		output: RuntimeBoundedTextSchema,
		serverToolUse: z
			.strictObject({
				webSearchRequests: NonNegativeIntegerSchema,
				webFetchRequests: NonNegativeIntegerSchema,
			})
			.optional(),
	}),
	z.strictObject({
		type: z.literal("error"),
		error: RuntimeFailureSchema,
		serverToolUse: z
			.strictObject({
				webSearchRequests: NonNegativeIntegerSchema,
				webFetchRequests: NonNegativeIntegerSchema,
			})
			.optional(),
	}),
	z.strictObject({
		type: z.literal("cancelled"),
		error: RuntimeFailureSchema.optional(),
	}),
]);

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
		return (
			(failure.code === "runtime_invalid_sequence" &&
				failure.reason === "runtime_contract_validation") ||
			(failure.code === "runtime_persistence_exhausted" &&
				failure.reason === "runtime_input_commit_exhausted")
		);
	}
	return (
		failure.type === "provider" &&
		!NonTerminalProviderFailureCodes.has(failure.code)
	);
}

const SessionErrorPayloadSchema = z.union([
	RuntimeFailureSchema,
	SessionMcpErrorSchema,
	SessionModelErrorSchema,
]);

const SessionEventTextBlockSchema = z.strictObject({
	type: z.literal("text"),
	text: RuntimeTextSchema,
});

const SessionIdleStopReasonSchema = z.discriminatedUnion("type", [
	z.strictObject({ type: z.literal("end_turn") }),
	z.strictObject({
		type: z.literal("requires_action"),
		event_ids: z.array(SanitizedIdentifierSchema).min(1),
	}),
	z.strictObject({ type: z.literal("retries_exhausted") }),
]);

const ApprovalReviewRiskLevelSchema = z.enum([
	"low",
	"medium",
	"high",
	"critical",
]);
const ApprovalReviewUserAuthorizationSchema = z.enum([
	"unknown",
	"low",
	"medium",
	"high",
]);

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
		input: RuntimeJsonValueSchema,
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

export type SessionEventWriterAppendEvent = Exclude<
	SessionEvent,
	{ readonly type: "agent.tool_result" | "agent.mcp_tool_result" }
>;

export const SessionEventWriterAppendEventSchema = SessionEventSchema.refine(
	(event) =>
		event.type !== "agent.tool_result" &&
		event.type !== "agent.mcp_tool_result",
	"Tool Result events are created only by SettleToolResult",
).transform((event) => event as SessionEventWriterAppendEvent);

/** WriteEvent payload for public events and optional Assistant-member append. */
export const SessionEventEnvelopeSchema = z
	.strictObject({
		workspaceId: SanitizedIdentifierSchema,
		sessionId: SanitizedIdentifierSchema,
		sessionThreadId: SanitizedIdentifierSchema,
		bindingId: SanitizedIdentifierSchema,
		bindingGeneration: NonNegativeIntegerSchema,
		targetPodUid: SanitizedIdentifierSchema,
		writeId: SanitizedIdentifierSchema,
		event: SessionEventWriterAppendEventSchema,
		assistantContextAppend: RuntimeAssistantContextAppendSchema.optional(),
		modelRequestId: SanitizedIdentifierSchema.optional(),
		contextThroughMessageSequence: NonNegativeIntegerSchema.optional(),
		requestKind: z
			.enum([
				"agent_provider_request",
				"compaction_summary",
				"approval_reviewer",
			])
			.optional(),
		consumedFileAttachments: z
			.array(
				z.strictObject({
					sourceEventId: SanitizedIdentifierSchema,
					fileId: SanitizedIdentifierSchema,
				}),
			)
			.max(32)
			.optional(),
		canonicalExecutionInput: RuntimeJsonValueSchema.optional(),
	})
	.superRefine((envelope, context) => {
		const memberEvent =
			envelope.event.type === "agent.message" ||
			envelope.event.type === "agent.tool_use" ||
			envelope.event.type === "agent.mcp_tool_use";
		if (memberEvent && envelope.assistantContextAppend === undefined) {
			context.addIssue({
				code: "custom",
				message: "Assistant member event requires one part append",
			});
		}
		if (!memberEvent && envelope.assistantContextAppend !== undefined) {
			context.addIssue({
				code: "custom",
				message: "event forbids a Runtime declaration shape",
			});
		}
		const toolUseEvent =
			envelope.event.type === "agent.tool_use" ||
			envelope.event.type === "agent.mcp_tool_use";
		if (toolUseEvent && envelope.canonicalExecutionInput === undefined) {
			context.addIssue({
				code: "custom",
				message: "Tool Use event requires canonical execution input",
			});
		}
		if (!toolUseEvent && envelope.canonicalExecutionInput !== undefined) {
			context.addIssue({
				code: "custom",
				message: "non-Tool event forbids canonical execution input",
			});
		}
		if (memberEvent && envelope.modelRequestId === undefined) {
			context.addIssue({
				code: "custom",
				message: "Assistant declaration requires a model request id",
			});
		}
		if (
			(envelope.event.type.startsWith("session.") ||
				envelope.event.type.startsWith("span.")) &&
			envelope.modelRequestId !== undefined
		) {
			context.addIssue({
				code: "custom",
				message: "non-assistant event forbids a model request id",
			});
		}
		if (envelope.event.type === "span.model_request_start") {
			if (
				envelope.contextThroughMessageSequence === undefined ||
				envelope.requestKind === undefined
			) {
				context.addIssue({
					code: "custom",
					message: "model request start requires its private request stamp",
				});
			}
		} else if (
			envelope.contextThroughMessageSequence !== undefined ||
			envelope.requestKind !== undefined ||
			envelope.consumedFileAttachments !== undefined
		) {
			context.addIssue({
				code: "custom",
				message: "private request stamp requires a model request start",
			});
		}
	});
export type SessionEventEnvelope = z.infer<typeof SessionEventEnvelopeSchema>;

/** Dedicated Tool settlement request; its response never echoes payload or durable stamps. */
export const SessionEventWriterToolSettlementEnvelopeSchema = z.strictObject({
	workspaceId: SanitizedIdentifierSchema,
	sessionId: SanitizedIdentifierSchema,
	sessionThreadId: SanitizedIdentifierSchema,
	bindingId: SanitizedIdentifierSchema,
	bindingGeneration: NonNegativeIntegerSchema,
	targetPodUid: SanitizedIdentifierSchema,
	settlement: RuntimeToolSettlementDeclarationSchema,
});
export type SessionEventWriterToolSettlementEnvelope = z.infer<
	typeof SessionEventWriterToolSettlementEnvelopeSchema
>;

/** Request-end settlement payload and its disjoint append/checkpoint declarations. */
export const SessionEventWriterRequestEndEnvelopeSchema = z
	.strictObject({
		workspaceId: SanitizedIdentifierSchema,
		sessionId: SanitizedIdentifierSchema,
		sessionThreadId: SanitizedIdentifierSchema,
		bindingId: SanitizedIdentifierSchema,
		bindingGeneration: NonNegativeIntegerSchema,
		targetPodUid: SanitizedIdentifierSchema,
		writeId: SanitizedIdentifierSchema,
		modelRequestId: SanitizedIdentifierSchema,
		isError: z.boolean(),
		errorKind: RuntimeRequestErrorKindSchema.optional(),
		finishReason: RuntimeFinishReasonSchema,
		usage: RuntimeUsageSchema.optional(),
		consumedAttachmentRefs: z.array(SanitizedIdentifierSchema).optional(),
		reschedule: z
			.strictObject({
				attempt: z.number().int().positive(),
				deadline: TimestampSchema,
				backoffMs: NonNegativeIntegerSchema,
			})
			.optional(),
		trailingContextAppend: RuntimeAssistantContextAppendSchema.optional(),
		compactionContext: z
			.strictObject({ parts: z.array(RuntimeContextPartSchema).min(1) })
			.optional(),
		prefixConsumption: z
			.strictObject({
				childThreadId: SanitizedIdentifierSchema,
				parentBoundaryEventId: SanitizedIdentifierSchema,
			})
			.optional(),
		compactedThroughMessageSequence: NonNegativeIntegerSchema.optional(),
		compactionEventPayloadJson: z
			.string()
			.superRefine((value, context) => {
				try {
					const parsed = JSON.parse(value) as unknown;
					if (
						typeof parsed !== "object" ||
						parsed === null ||
						Array.isArray(parsed)
					) {
						context.addIssue({
							code: "custom",
							message: "compaction event payload must be a JSON object",
						});
					}
				} catch {
					context.addIssue({
						code: "custom",
						message: "compaction event payload must be valid JSON",
					});
				}
			})
			.optional(),
		interruptSettlement: z
			.strictObject({
				runtimeInputId: SanitizedIdentifierSchema,
				interruptLeaseRef: z.strictObject({
					jobId: SanitizedIdentifierSchema,
					leaseToken: SanitizedIdentifierSchema,
					partitionKey: SanitizedIdentifierSchema,
					dedupeKey: SanitizedIdentifierSchema,
				}),
			})
			.optional(),
	})
	.superRefine((envelope, context) => {
		const consumedAttachmentCount =
			envelope.consumedAttachmentRefs?.length ?? 0;
		if (consumedAttachmentCount > 32) {
			context.addIssue({
				code: "custom",
				message: "consumed attachments exceed the request bound",
			});
		}
		if (envelope.reschedule !== undefined && consumedAttachmentCount > 0) {
			context.addIssue({
				code: "custom",
				message: "rescheduled request ends cannot consume attachments",
			});
		}
		if (
			envelope.interruptSettlement !== undefined &&
			(!envelope.isError ||
				envelope.errorKind !== "runtime_interrupted" ||
				envelope.reschedule !== undefined)
		) {
			context.addIssue({
				code: "custom",
				message:
					"request-end interrupt settlement requires an interrupted terminal end",
			});
		}
		const successful = !envelope.isError && envelope.reschedule === undefined;
		const isCompaction =
			envelope.compactionContext !== undefined ||
			envelope.prefixConsumption !== undefined ||
			envelope.compactedThroughMessageSequence !== undefined ||
			envelope.compactionEventPayloadJson !== undefined;
		if (
			!successful &&
			(envelope.trailingContextAppend !== undefined ||
				envelope.compactionContext !== undefined)
		) {
			context.addIssue({
				code: "custom",
				message:
					"unsuccessful request end forbids append and checkpoint declarations",
			});
		}
		if (isCompaction && successful) {
			if (
				envelope.trailingContextAppend !== undefined ||
				envelope.compactionContext === undefined ||
				envelope.compactedThroughMessageSequence === undefined ||
				envelope.compactionEventPayloadJson === undefined
			) {
				context.addIssue({
					code: "custom",
					message:
						"successful compaction requires its event, checkpoint, and sequence boundary",
				});
			}
		} else if (
			envelope.compactionContext !== undefined ||
			envelope.prefixConsumption !== undefined ||
			envelope.compactedThroughMessageSequence !== undefined ||
			envelope.compactionEventPayloadJson !== undefined
		) {
			context.addIssue({
				code: "custom",
				message:
					"non-successful-compaction request end forbids checkpoint fields",
			});
		}
	});
export type SessionEventWriterRequestEndEnvelope = z.infer<
	typeof SessionEventWriterRequestEndEnvelopeSchema
>;

export const SessionEventWriterFinishIdleEnvelopeSchema = z.strictObject({
	workspaceId: SanitizedIdentifierSchema,
	sessionId: SanitizedIdentifierSchema,
	sessionThreadId: SanitizedIdentifierSchema,
	bindingId: SanitizedIdentifierSchema,
	bindingGeneration: NonNegativeIntegerSchema,
	targetPodUid: SanitizedIdentifierSchema,
	durableTurnId: SanitizedIdentifierSchema,
	stopReason: SessionIdleStopReasonSchema,
	completionMailText: RuntimeTextSchema.optional(),
});
export type SessionEventWriterFinishIdleEnvelope = z.infer<
	typeof SessionEventWriterFinishIdleEnvelopeSchema
>;

export const SessionEventWriterRuntimeTerminationEnvelopeSchema =
	z.strictObject({
		workspaceId: SanitizedIdentifierSchema,
		sessionId: SanitizedIdentifierSchema,
		sessionThreadId: SanitizedIdentifierSchema,
		bindingId: SanitizedIdentifierSchema,
		bindingGeneration: NonNegativeIntegerSchema,
		targetPodUid: SanitizedIdentifierSchema,
		writeId: SanitizedIdentifierSchema,
		failure: RuntimeFailureSchema,
	});
export type SessionEventWriterRuntimeTerminationEnvelope = z.infer<
	typeof SessionEventWriterRuntimeTerminationEnvelopeSchema
>;

// This timeout bounds both each ordinary transport attempt and each failed-run
// closeout observation window; the latter never cancels its in-flight transport.
export const SessionEventWriterRetryPolicy = {
	attempts: 3,
	timeoutPerAttemptMs: 3000,
	backoffMs: [100, 300],
} as const;

export const SessionEventWriterErrorSchema = z.strictObject({
	type: z.literal("session-event-writer"),
	code: z.enum([
		"unavailable",
		"timeout",
		"schema_mismatch",
		"ack_mismatch",
		"superseded",
		"unrepairable",
		"unknown",
	]),
	message: SanitizedTextSchema,
	retryable: z.boolean(),
	fatal: z.boolean(),
	sessionId: SanitizedIdentifierSchema.optional(),
	writeId: SanitizedIdentifierSchema.optional(),
});
export type SessionEventWriterError = z.infer<
	typeof SessionEventWriterErrorSchema
>;

export const SessionEventWriterAppendResultSchema = z.union([
	z.strictObject({
		ok: z.literal(true),
		type: z.enum(["committed", "duplicate"]),
		eventId: SanitizedIdentifierSchema,
	}),
	z.strictObject({
		ok: z.literal(true),
		type: z.enum(["committed", "duplicate"]),
		eventId: SanitizedIdentifierSchema,
		assistant: z.strictObject({
			messageSequence: PositiveIntegerSchema,
			createdToolUseEventIds: z.array(SanitizedIdentifierSchema),
		}),
	}),
	z.strictObject({ ok: z.literal(true), type: z.literal("stale") }),
	z.strictObject({
		ok: z.literal(false),
		error: SessionEventWriterErrorSchema,
	}),
]);
export type SessionEventWriterAppendResult = z.infer<
	typeof SessionEventWriterAppendResultSchema
>;

export type SessionEventWriterRequestEndResult =
	| {
			readonly ok: true;
			readonly type: "committed" | "duplicate";
			readonly requestEndEventId: string;
			readonly outcome:
				| {
						readonly type: "ordinary";
						readonly sealedMessageSequence?: number | undefined;
				  }
				| { readonly type: "rescheduled"; readonly effectiveDeadline: string }
				| {
						readonly type: "compacted";
						readonly compactionEventId: string;
						readonly checkpointMessageSequence: number;
				  };
			readonly interruptToolResults: readonly RuntimeInterruptToolResult[];
	  }
	| { readonly ok: true; readonly type: "stale" }
	| { readonly ok: false; readonly error: SessionEventWriterError };

export type SessionEventWriterFinishIdleResult =
	| {
			readonly ok: true;
			readonly type: "committed" | "duplicate";
			readonly idleEventId: string;
	  }
	| { readonly ok: true; readonly type: "stale" }
	| { readonly ok: false; readonly error: SessionEventWriterError };

export type SessionEventWriterRuntimeTerminationResult =
	| {
			readonly ok: true;
			readonly type: "committed" | "duplicate";
			readonly failureEventId: string;
			readonly closeoutEventId: string;
	  }
	| { readonly ok: true; readonly type: "stale" }
	| { readonly ok: false; readonly error: SessionEventWriterError };

export type SessionEventWriterToolSettlementResult =
	| { readonly type: "committed" }
	| { readonly type: "duplicate" }
	| { readonly type: "stale" };

export type SessionEventWriterToolSettlementAttempt =
	| {
			readonly ok: true;
			readonly result: SessionEventWriterToolSettlementResult;
	  }
	| { readonly ok: false; readonly error: SessionEventWriterError };

/** Durable event port whose closed operation results gate the corresponding hot projection. */
export interface SessionEventWriter {
	/**
	 * Appends one Bridge-bound event before its matching event projection mutates hot
	 * context. The envelope excludes Bridge-assigned facts and timestamps. Any retry
	 * must reuse the same writeId; the closed result returns only assigned facts with
	 * immediate Runtime consumers.
	 */
	readonly append: (
		envelope: SessionEventEnvelope,
	) => Promise<SessionEventWriterAppendResult>;
	readonly settleToolResult: (
		envelope: SessionEventWriterToolSettlementEnvelope,
	) => Promise<SessionEventWriterToolSettlementAttempt>;
	readonly writeRequestEnd: (
		envelope: SessionEventWriterRequestEndEnvelope,
	) => Promise<SessionEventWriterRequestEndResult>;
	readonly finishIdle?: (
		envelope: SessionEventWriterFinishIdleEnvelope,
	) => Promise<SessionEventWriterFinishIdleResult>;
	readonly commitRuntimeTermination?: (
		envelope: SessionEventWriterRuntimeTerminationEnvelope,
	) => Promise<SessionEventWriterRuntimeTerminationResult>;
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
const OperationClockSchema = z.custom<() => number>(
	(value) => typeof value === "function",
	"operation clock",
);
const OperationSleepSchema = z.custom<
	(durationMs: number, signal: AbortSignal) => Promise<boolean>
>((value) => typeof value === "function", "operation sleep");

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
		(controls) =>
			controls.timeoutMs !== undefined ||
			controls.deadlineEpochMs !== undefined,
		"internal Tool repair operation controls require timeoutMs or deadlineEpochMs",
	)
	.refine(
		(controls) =>
			controls.deadlineEpochMs === undefined ||
			controls.nowEpochMs !== undefined,
		"internal Tool repair deadline controls require nowEpochMs",
	);
export type RuntimeDeclarationOperationControls = z.infer<
	typeof RuntimeDeclarationOperationControlsSchema
>;

type RuntimeDeclarationRawOperationStart = () => () => void;
const RuntimeDeclarationRawOperationOwners = new WeakMap<
	AbortSignal,
	RuntimeDeclarationRawOperationStart
>();

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

export const RuntimeInternalToolRepairCommitResultSchema = z.union([
	z.strictObject({
		ok: z.literal(true),
		type: z.enum(["committed", "duplicate"]),
		repairEventId: RuntimeIdentifierSchema,
		assignedMessageSequence: PositiveIntegerSchema,
	}),
	z.strictObject({ ok: z.literal(true), type: z.literal("stale") }),
	z.strictObject({
		ok: z.literal(false),
		error: RuntimeInternalToolRepairStoreErrorSchema,
	}),
]);
export type RuntimeInternalToolRepairCommitResult = z.infer<
	typeof RuntimeInternalToolRepairCommitResultSchema
>;

const RawRuntimeInternalToolRepairCommitResultSchema =
	RuntimeInternalToolRepairCommitResultSchema;

/** Injectable clock, identity, and cancellation-aware sleep dependencies for Runtime Core. */
export interface RuntimeDependencies {
	readonly now: () => string;
	readonly monotonicMs: () => number;
	readonly createId: (prefix: string) => string;
	readonly sleep: (durationMs: number, signal: AbortSignal) => Promise<boolean>;
}

const StoreErrorRetryableCodes = new Set<
	(typeof RuntimeInternalToolRepairStoreErrorCodes)[number]
>(["unavailable", "timeout", "serialization_failure"]);

export interface RuntimeInternalToolRepairStoreErrorInput {
	readonly code?: RuntimeInternalToolRepairStoreError["code"];
	readonly operation: "commitInternalToolRepair";
	readonly rawError?: unknown;
	readonly reason?: z.infer<typeof SafeReasonCodeSchema> | undefined;
	readonly constraint?: string | undefined;
	readonly status?: z.infer<typeof SafeTerminalStatusSchema> | undefined;
	readonly attemptedStatus?:
		| z.infer<typeof SafeTerminalStatusSchema>
		| undefined;
	readonly sessionId?: string | undefined;
}

/** Converts store-specific failures into the bounded internal Tool-repair store error shape. */
export function normalizeRuntimeInternalToolRepairStoreError(
	input: RuntimeInternalToolRepairStoreErrorInput,
): RuntimeInternalToolRepairStoreError {
	const code = input.code ?? "unknown";
	return RuntimeInternalToolRepairStoreErrorSchema.parse({
		type: "message-store",
		code,
		operation: input.operation,
		message: "Runtime internal Tool repair store operation failed.",
		retryable: StoreErrorRetryableCodes.has(code),
		fatal: code === "schema_mismatch" || code === "constraint_violation",
		...(input.reason !== undefined ? { reason: input.reason } : {}),
		...(input.constraint !== undefined
			? { constraint: sanitizeRuntimeText(input.constraint) }
			: {}),
		...(input.status !== undefined ? { status: input.status } : {}),
		...(input.attemptedStatus !== undefined
			? { attemptedStatus: input.attemptedStatus }
			: {}),
		...(input.sessionId !== undefined
			? { sessionId: sanitizeRuntimeText(input.sessionId) }
			: {}),
	});
}

export interface ContextLoaderErrorInput {
	readonly code?: ContextLoaderError["code"];
	readonly rawError?: unknown;
	readonly sessionId?: string | undefined;
	readonly reason?: string | undefined;
}

/** Converts context-loading failures into the bounded Runtime loader error shape. */
export function normalizeContextLoaderError(
	input: ContextLoaderErrorInput,
): ContextLoaderError {
	const code = input.code ?? "unknown";
	return {
		type: "context-loader",
		code,
		message: "Context loader operation failed.",
		retryable: code === "unavailable" || code === "timeout",
		fatal:
			code === "superseded" ||
			code === "schema_mismatch" ||
			code === "wrong_session" ||
			code === "bounds_exceeded" ||
			code === "unsafe_payload",
		...(input.sessionId !== undefined
			? { sessionId: sanitizeRuntimeText(input.sessionId) }
			: {}),
		...(input.reason !== undefined
			? { reason: sanitizeRuntimeText(input.reason) }
			: {}),
	};
}

export interface SessionEventWriterErrorInput {
	readonly code?: SessionEventWriterError["code"];
	readonly rawError?: unknown;
	readonly sessionId?: string | undefined;
	readonly writeId?: string | undefined;
}

/** Converts Bridge event-write failures into the bounded Runtime writer error shape. */
export function normalizeSessionEventWriterError(
	input: SessionEventWriterErrorInput,
): SessionEventWriterError {
	const code = input.code ?? "unknown";
	return {
		type: "session-event-writer",
		code,
		message: "Session event writer operation failed.",
		retryable: code === "unavailable" || code === "timeout",
		fatal:
			code === "schema_mismatch" ||
			code === "ack_mismatch" ||
			code === "unrepairable",
		...(input.sessionId !== undefined
			? { sessionId: sanitizeRuntimeText(input.sessionId) }
			: {}),
		...(input.writeId !== undefined
			? { writeId: sanitizeRuntimeText(input.writeId) }
			: {}),
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
	readonly providerId?: RuntimeFailure["providerId"] | undefined;
	readonly modelId?: string | undefined;
}

/** Builds the canonical internal failure without retaining raw dependency errors. */
export function normalizeRuntimeFailure(
	input: RuntimeFailureInput,
): RuntimeFailure {
	const code =
		input.code ??
		(input.type === "runtime" ? "runtime_invalid_sequence" : "unknown");
	return RuntimeFailureSchema.parse({
		type: input.type,
		code,
		message:
			code === "runtime_persistence_exhausted"
				? "Runtime could not durably commit accepted input."
				: "Runtime operation failed.",
		retryable: input.retryable ?? false,
		fatal: input.fatal ?? true,
		...(input.retryStatus !== undefined
			? { retryStatus: input.retryStatus }
			: {}),
		...(input.operation !== undefined ? { operation: input.operation } : {}),
		...(input.reason !== undefined ? { reason: input.reason } : {}),
		...(input.sessionId !== undefined
			? { sessionId: sanitizeRuntimeText(input.sessionId) }
			: {}),
		...(input.providerId !== undefined ? { providerId: input.providerId } : {}),
		...(input.modelId !== undefined
			? { modelId: sanitizeRuntimeText(input.modelId) }
			: {}),
	});
}

function sanitizeRuntimeFailure(failure: RuntimeFailure): RuntimeFailure {
	return RuntimeFailureSchema.parse({
		...failure,
		message: sanitizeRuntimeText(failure.message),
		...(failure.sessionId !== undefined
			? { sessionId: sanitizeRuntimeText(failure.sessionId) }
			: {}),
		...(failure.modelId !== undefined
			? { modelId: sanitizeRuntimeText(failure.modelId) }
			: {}),
	});
}

function declarationSchemaMismatch(): RuntimeInternalToolRepairStoreError {
	return normalizeRuntimeInternalToolRepairStoreError({
		code: "schema_mismatch",
		operation: "commitInternalToolRepair",
		reason: "runtime_contract_validation",
	});
}

function caughtDeclarationStoreError(
	error: unknown,
): RuntimeInternalToolRepairStoreError {
	if (error instanceof RuntimeDeclarationOperationError) {
		return normalizeRuntimeInternalToolRepairStoreError({
			code: error.storeError.code,
			operation: "commitInternalToolRepair",
			reason: error.storeError.reason,
			constraint: error.storeError.constraint,
			status: error.storeError.status,
			attemptedStatus: error.storeError.attemptedStatus,
			sessionId: error.storeError.sessionId,
		});
	}
	const errorRecord =
		typeof error === "object" && error !== null
			? (error as Readonly<Record<string, unknown>>)
			: {};
	const code = RuntimeInternalToolRepairStoreErrorSchema.shape.code.safeParse(
		errorRecord.code,
	);
	return normalizeRuntimeInternalToolRepairStoreError({
		code: code.success ? code.data : "unknown",
		operation: "commitInternalToolRepair",
		rawError: error,
	});
}

/** Error wrapper that preserves a normalized declaration failure across an async implementation. */
export class RuntimeDeclarationOperationError extends Error {
	readonly storeError: RuntimeInternalToolRepairStoreError;

	constructor(storeError: RuntimeInternalToolRepairStoreError) {
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
	async commitInternalToolRepair(
		repair: unknown,
		controls: unknown,
	): Promise<RuntimeInternalToolRepairCommitResult> {
		const parsedRepair =
			RuntimeInternalToolRepairCommitSchema.safeParse(repair);
		const parsedControls =
			RuntimeDeclarationOperationControlsSchema.safeParse(controls);
		if (!parsedRepair.success || !parsedControls.success) {
			return { ok: false, error: declarationSchemaMismatch() };
		}
		const preflightFailure = operationPreflightFailure(parsedControls.data);
		if (preflightFailure !== undefined) {
			return { ok: false, error: preflightFailure };
		}
		try {
			const result = await withBoundedStoreOperation(
				(operationControls) =>
					this.commitInternalToolRepairRecord(
						parsedRepair.data,
						operationControls,
					),
				parsedControls.data,
			);
			const parsedResult =
				RawRuntimeInternalToolRepairCommitResultSchema.safeParse(result);
			if (!parsedResult.success) {
				return { ok: false, error: declarationSchemaMismatch() };
			}
			if (parsedResult.data.ok === false) {
				return {
					ok: false,
					error: normalizeRuntimeInternalToolRepairStoreError({
						code: parsedResult.data.error.code,
						operation: "commitInternalToolRepair",
						reason: parsedResult.data.error.reason,
						constraint: parsedResult.data.error.constraint,
						status: parsedResult.data.error.status,
						attemptedStatus: parsedResult.data.error.attemptedStatus,
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
): RuntimeInternalToolRepairStoreError | undefined {
	if (controls.signal.aborted) {
		return normalizeRuntimeInternalToolRepairStoreError({
			code: "timeout",
			operation: "commitInternalToolRepair",
			reason: "aborted",
		});
	}
	if (remainingOperationBudgetMs(controls) <= 0) {
		return normalizeRuntimeInternalToolRepairStoreError({
			code: "timeout",
			operation: "commitInternalToolRepair",
			reason: "timeout",
		});
	}
	return undefined;
}

function remainingOperationBudgetMs(
	controls: RuntimeDeclarationOperationControls,
): number {
	const budgets: number[] = [];
	if (controls.timeoutMs !== undefined) {
		budgets.push(controls.timeoutMs);
	}
	if (
		controls.deadlineEpochMs !== undefined &&
		controls.nowEpochMs !== undefined
	) {
		budgets.push(controls.deadlineEpochMs - controls.nowEpochMs());
	}
	return budgets.length === 0 ? 0 : Math.min(...budgets);
}

async function pendingForever(): Promise<never> {
	return await new Promise<never>(() => undefined);
}

async function withBoundedStoreOperation<
	T,
	TControls extends RuntimeDeclarationOperationControls,
>(
	operation: (controls: TControls) => Promise<T>,
	controls: TControls,
): Promise<T> {
	const operationController = new AbortController();
	const abortOperation = (): void => operationController.abort();
	const operationControls = {
		...controls,
		signal: operationController.signal,
	} as TControls;
	let abortOperationPromise:
		| ((result: { readonly kind: "aborted" }) => void)
		| undefined;
	const resolveAbortOperation = (): void =>
		abortOperationPromise?.({ kind: "aborted" });
	controls.signal.addEventListener("abort", abortOperation, { once: true });
	const abortPromise = new Promise<{ readonly kind: "aborted" }>((resolve) => {
		abortOperationPromise = resolve;
		controls.signal.addEventListener("abort", resolveAbortOperation, {
			once: true,
		});
	});
	const timeoutPromise = controls
		.sleep(remainingOperationBudgetMs(controls), operationController.signal)
		.then(async (elapsed) =>
			elapsed ? { kind: "timeout" as const } : await pendingForever(),
		);
	const finishOwnedRawOperation = RuntimeDeclarationRawOperationOwners.get(
		controls.signal,
	)?.();
	let rawOperation: Promise<T>;
	try {
		rawOperation = operation(operationControls);
	} catch (error) {
		finishOwnedRawOperation?.();
		throw error;
	}
	const operationPromise = rawOperation
		.then(
			(value) => ({ kind: "value" as const, value }),
			(error: unknown) => ({ kind: "error" as const, error }),
		)
		.finally(() => finishOwnedRawOperation?.());

	try {
		const result = await Promise.race([
			operationPromise,
			timeoutPromise,
			abortPromise,
		]);
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
			normalizeRuntimeInternalToolRepairStoreError({
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
export function boundRuntimeText(
	input: string,
	maxBytes: number,
): RuntimeBoundedText {
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
	if (
		typeof input === "number" ||
		typeof input === "boolean" ||
		input === null
	) {
		return JSON.stringify(input);
	}
	return "[non-json-runtime-value]";
}

/** Retains a complete admitted JSON value and derives an independently bounded preview. */
export function boundRuntimeJson(
	input: RuntimeJsonValue,
	maxBytes: number,
): RuntimeBoundedJson {
	const preview = jsonPreview(input);
	const boundedPreview = boundRuntimeText(preview, maxBytes);
	return RuntimeBoundedJsonSchema.parse({
		value: input,
		preview: boundedPreview.text,
		truncated: boundedPreview.truncated,
	});
}

export function boundRuntimeToolError(
	input: RuntimeToolError,
	maxBytes: number,
): RuntimeToolError {
	const boundedMessage = boundRuntimeText(input.message, maxBytes);
	return RuntimeToolErrorSchema.parse({
		type: input.type,
		message: boundedMessage.text,
		...(input.retryable !== undefined ? { retryable: input.retryable } : {}),
	});
}

export function createRuntimeId(
	deps: RuntimeDependencies,
	prefix: string,
): string {
	return deps.createId(prefix);
}

export function runtimeNow(deps: RuntimeDependencies): string {
	return deps.now();
}

export async function runtimeSleep(
	deps: RuntimeDependencies,
	durationMs: number,
	signal: AbortSignal,
): Promise<boolean> {
	return await deps.sleep(durationMs, signal);
}
