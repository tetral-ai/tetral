/**
 * @packageDocumentation
 *
 * Defines pure wire-shape validation for Runtime-to-Gateway provider requests and Gateway-to-
 * Runtime provider stream events, plus byte ceilings shared by provider, MCP, Web, and Runtime
 * callers. The provider-gateway service validates
 * requests after caller authorization and readiness checks but before binding-token verification,
 * admission, attachment and credential resolution, or provider lowering. It also validates provider
 * adapter stream output before forwarding it. Runtime Core applies the stream validator before its
 * stateful ordering checks and hot-state updates. Provider-gateway and MCP/Web validators consume
 * the shared limits and validation result; Runtime Core consumes `MaxTextBytes` while assembling
 * provider requests and bounding normalized tool-input previews.
 *
 * Validation operates on generated protocol types and enums, measures bounded strings as UTF-8,
 * parses serialized JSON where payload shape matters, and returns one redacted failure message.
 * Each call validates one request or event in isolation; authentication, tenant authorization,
 * cross-event ordering, terminal-state tracking, and provider behavior remain caller responsibilities.
 */
import {
  ProviderAttachmentRejectionReason,
  ProviderFinishReason,
  ProviderRequestKind,
  ProviderStreamEventType,
  RuntimeMessageRole,
  RuntimeToolPartState,
  SystemCacheHint,
  SystemSegmentKind,
} from "./gen/tetral/provider_gateway/v1/provider_gateway.js";
import type {
  ProviderAttachmentRejection,
  ProviderRequest,
  ProviderStreamEvent,
  RequestUsage,
  RuntimePart,
} from "./gen/tetral/provider_gateway/v1/provider_gateway.js";

/**
 * Maximum UTF-8 size of required identifier-like fields and short labels,
 * including filenames and MCP attachment MIME strings.
 */
export const MaxIdBytes = 128;
/** Maximum UTF-8 size of a required Runtime binding token accepted at Gateway boundaries. */
export const MaxTokenBytes = 1024;
// MaxTextBytes: field-sanity ceiling for the shorter free-text fields carried in a
// provider request or stream event — system-segment text (validateProviderRequest),
// tool descriptions, transient-attachment source paths, provider-error messages, and
// streamed text/reasoning/tool-input fragment deltas (validFragment). It does NOT bound
// message text or reasoning parts; those ride the request fuse under
// MaxProviderRequestMessagePartJsonBytes. The 64 KiB value is a fixed sanity ceiling, not
// sized to any payload.
// UPDATE-WITH: validateProviderRequest, validateProviderStreamEvent,
//   validAttachmentRejections, validFragment (all in this file).
/** Maximum UTF-8 size of short free-text fields and individual streamed fragment deltas. */
export const MaxTextBytes = 64 * 1024;
// MaxProviderRequestMessagePartJsonBytes: per-part semantic bound for message text and
// reasoning in a ProviderRequest (validRuntimePart). Runtime accumulation and Gateway
// validation both measure JSON.stringify(text), so escape-dense content has one contract.
// UPDATE-WITH: validRuntimePart (part.text.text, part.reasoning.text) in this file.
/** Maximum canonical JSON-string size of one Runtime message text or reasoning part. */
export const MaxProviderRequestMessagePartJsonBytes = 16 * 1024 * 1024;
// A fitted Read result is already a complete JSON-escaped helper envelope of at
// most 200,000 bytes. Runtime decodes it, adds at most 2,000 line prefixes, and
// serializes the visible text once. 512 KiB also carries the MCP formatter's
// 50 KiB raw result under worst-case JSON escaping without truncation.
// UPDATE-WITH: internal/sandbox/helper/internal/filetool/read.go
//   (maxReadEnvelopeBytes); validRuntimePart below;
//   services/agent-runtime/packages/core/src/contracts/runtime.ts
//   (RuntimeBoundedTextSchema); services/web-connector/types.go
//   (maxModelVisibleToolOutputJSONBytes, maxVisibleResultBytes).
/** Maximum UTF-8 size of provider-request tool output/error JSON. */
export const MaxProviderRequestToolOutputJsonBytes = 512 * 1024;
/** Maximum UTF-8 size of one provider-produced tool-call input JSON value. */
export const MaxProviderToolCallInputJsonBytes = 4 * 1024 * 1024;
/** Maximum UTF-8 size of provider-native usage JSON. */
export const MaxProviderUsageJsonBytes = 16 * 1024;
/** Maximum UTF-8 size of MCP tool input JSON. */
export const MaxMcpToolInputJsonBytes = 64 * 1024;
/** Maximum UTF-8 size of a safe provider error message. */
export const MaxProviderErrorMessageBytes = 8 * 1024;
/**
 * Maximum UTF-8 size of a serialized JSON schema accepted on provider requests
 * and MCP tool-list responses.
 */
export const MaxSchemaBytes = 32 * 1024;
/** Maximum UTF-8 size of serialized JSON metadata on Runtime parts and stream events. */
export const MaxMetadataBytes = 16 * 1024;
/** Maximum UTF-8 size of descriptive attachment page-range and detail hints. */
export const MaxAttachmentHintBytes = 128;
// MaxProviderRequestAttachments: bound on the number of attachments in one provider
// request (validateProviderRequest); transient-origin and file-backed-origin refs share
// this single budget, since both occupy the one request.attachments array. The same cap
// also bounds the attachment-rejection list carried on a stream event
// (validAttachmentRejections). This is a count bound only. Transient and file-backed
// byte envelopes are enforced separately by their resolver paths, so resident-byte
// bounds must not be inferred from this constant alone.
// UPDATE-WITH: validateProviderRequest (request.attachments), validAttachmentRejections
//   (both in this file).
/** Maximum attachment count on one provider request or attachment-rejections event. */
export const MaxProviderRequestAttachments = 32;
/** Largest positive Runtime binding generation representable by the protocol validators. */
export const MaxBindingGeneration = 0xffffffff;

const AllowedProviderRequestAttachmentMimes = new Set([
  "application/pdf",
  "image/png",
  "image/jpeg",
  "image/gif",
  "image/webp",
]);
const AllowedFileBackedProviderRequestAttachmentMimes = new Set([
  ...AllowedProviderRequestAttachmentMimes,
  "text/plain",
]);
const AllowedRuntimeMessageStatuses = new Set(["completed", "failed", "cancelled"]);
const AllowedRuntimeMessageOrigins = new Set(["user", "agent", "runtime"]);

/**
 * Result returned by Gateway wire validators.
 *
 * The `ok` discriminant lets callers continue with the typed message or reject it with the fixed,
 * non-diagnostic failure text without exposing malformed internal data.
 */
export type ValidationResult = { readonly ok: true } | {
  readonly ok: false;
  readonly message: "invalid internal request";
  /** Fixed, non-payload diagnostic category safe for structured logs. */
  readonly code?: string;
  /** Fixed protocol member path safe for structured logs. */
  readonly member?: string;
};

/**
 * Validates the bounded shape of one complete provider request before Gateway performs provider work.
 *
 * The check covers request and tenancy identities, the Runtime binding token and generation,
 * request-kind and model fields, segmented system input, settled Runtime messages, tool definitions,
 * attachment origin shape and file-origin uniqueness, request limits, and reviewer output-schema use.
 * It does not authenticate the caller or verify that the binding token belongs to the request.
 */
export function validateProviderRequest(request: ProviderRequest): ValidationResult {
  for (const [member, value] of [
    ["request_id", request.requestId],
    ["model_request_id", request.modelRequestId],
    ["workspace_id", request.workspaceId],
    ["session_id", request.sessionId],
    ["session_thread_id", request.sessionThreadId],
    ["binding_id", request.bindingId],
  ] as const) {
    if (invalidBytes(value, MaxIdBytes)) {
      return invalidRequest("invalid_identifier", member);
    }
  }
  if (request.parentThreadId !== undefined && invalidBytes(request.parentThreadId, MaxIdBytes)) {
    return invalidRequest("invalid_identifier", "parent_thread_id");
  }
  if (invalidBytes(request.runtimeBindingToken, MaxTokenBytes) || invalidBindingGeneration(request.bindingGeneration)) {
    return invalidRequest("invalid_binding", "runtime_binding");
  }
  if (!validEnumValue(
    request.requestKind,
    ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
    ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY,
    ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
    ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER_COMPACTION,
  )) {
    return invalidRequest("invalid_request_kind", "request_kind");
  }
  const approvalReviewerRequest = request.requestKind === ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER;
  if (
    (approvalReviewerRequest && (request.outputSchemaJson === undefined || !validJsonObject(request.outputSchemaJson, MaxSchemaBytes))) ||
    (!approvalReviewerRequest && request.outputSchemaJson !== undefined)
  ) {
    return invalidRequest("invalid_output_schema", "output_schema_json");
  }
  if (request.model === undefined || invalidBytes(request.model.providerId, MaxIdBytes) || invalidBytes(request.model.modelId, MaxIdBytes)) {
    return invalidRequest("invalid_model", "model");
  }
  if (request.model.variant !== "") {
    return invalidRequest("unsupported_model_variant", "model.variant");
  }
  for (const segment of request.system) {
    if (
      !validEnumValue(
        segment.kind,
        SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
        SystemSegmentKind.SYSTEM_SEGMENT_KIND_TOOL_GUIDANCE,
        SystemSegmentKind.SYSTEM_SEGMENT_KIND_AGENT,
        SystemSegmentKind.SYSTEM_SEGMENT_KIND_MEMORY,
        SystemSegmentKind.SYSTEM_SEGMENT_KIND_SKILL,
        SystemSegmentKind.SYSTEM_SEGMENT_KIND_APPROVAL_REVIEWER_POLICY,
      ) ||
      !validEnumValue(segment.cacheHint, SystemCacheHint.SYSTEM_CACHE_HINT_STABLE, SystemCacheHint.SYSTEM_CACHE_HINT_SESSION, SystemCacheHint.SYSTEM_CACHE_HINT_NONE) ||
      invalidBytes(segment.text, MaxTextBytes)
    ) {
      return invalidRequest("invalid_system_segment", "system");
    }
  }
  for (const message of request.messages) {
    const messageIds = validateIdFields(message.id);
    if (
      !messageIds.ok ||
      !validEnum(message.role, RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_USER, RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_ASSISTANT) ||
      !AllowedRuntimeMessageStatuses.has(message.status) ||
      !AllowedRuntimeMessageOrigins.has(message.origin)
    ) {
      return invalidRequest("invalid_message", "messages");
    }
    for (const part of message.parts) {
      if (!validRuntimePart(part)) {
        return invalidRequest("invalid_message_part", "messages.parts");
      }
    }
  }
  for (const tool of request.tools) {
    if (
      invalidBytes(tool.name, MaxIdBytes) ||
      invalidBytes(tool.description, MaxTextBytes) ||
      !validJsonObject(tool.inputSchemaJson, MaxSchemaBytes) ||
      (tool.outputSchemaJson !== undefined && !validJsonObject(tool.outputSchemaJson, MaxSchemaBytes))
    ) {
      return invalidRequest("invalid_tool_definition", "tools");
    }
  }
  if (request.attachments.length > MaxProviderRequestAttachments) {
    return invalidRequest("too_many_attachments", "attachments");
  }
  const fileAttachmentOrigins = new Set<string>();
  for (const attachment of request.attachments) {
    if (invalidBytes(attachment.filename, MaxIdBytes)) {
      return invalidRequest("invalid_attachment", "attachments.filename");
    }
    if (attachment.transient !== undefined) {
      if (
        attachment.fileBacked !== undefined ||
        !AllowedProviderRequestAttachmentMimes.has(attachment.mime) ||
        invalidBytes(attachment.transient.attachmentRef, MaxIdBytes) ||
        invalidBytes(attachment.transient.sourceToolUseEventId, MaxIdBytes) ||
        invalidBytes(attachment.transient.sourcePath, MaxTextBytes) ||
        !validAttachmentPageRange(attachment.transient.pageRange) ||
        invalidBytes(attachment.transient.detail, MaxAttachmentHintBytes)
      ) {
        return invalidRequest("invalid_attachment", "attachments.transient");
      }
      continue;
    }
    if (
      attachment.fileBacked === undefined ||
      !AllowedFileBackedProviderRequestAttachmentMimes.has(attachment.mime) ||
      invalidBytes(attachment.fileBacked.sourceEventId, MaxIdBytes) ||
      invalidBytes(attachment.fileBacked.fileId, MaxIdBytes)
    ) {
      return invalidRequest("invalid_attachment", "attachments.file_backed");
    }
    const originKey = JSON.stringify([
      attachment.fileBacked.sourceEventId,
      attachment.fileBacked.fileId,
    ]);
    if (fileAttachmentOrigins.has(originKey)) {
      return invalidRequest("duplicate_attachment", "attachments.file_backed");
    }
    fileAttachmentOrigins.add(originKey);
  }
  if (request.limits === undefined || request.limits.maxOutputTokens <= 0 || request.limits.timeoutMs <= 0) {
    return invalidRequest("invalid_limits", "limits");
  }
  return { ok: true };
}

/**
 * Validates one normalized provider stream event against its originating request identifiers.
 *
 * The event must carry exactly one payload matching a supported event type. Payload-specific checks
 * enforce bounded identifiers and text, JSON and metadata shape, non-negative usage and error values,
 * finish limits, and unique attachment-rejection origins. Fragment ordering and terminal-event
 * sequencing require stream state and are intentionally left to the Runtime consumer.
 */
export function validateProviderStreamEvent(event: ProviderStreamEvent, request: Pick<ProviderRequest, "requestId" | "modelRequestId">): ValidationResult {
  if (event.requestId !== request.requestId || event.modelRequestId !== request.modelRequestId) {
    return invalidRequest();
  }
  if (!validEnum(event.type, ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START, ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_ATTACHMENT_REJECTIONS)) {
    return invalidRequest();
  }
  const payloadCount = [
    event.text,
    event.reasoning,
    event.toolInput,
    event.toolCall,
    event.finish,
    event.providerError,
    event.attachmentRejections,
  ].filter((payload) => payload !== undefined).length;
  if (payloadCount !== 1) {
    return invalidRequest();
  }
  switch (event.type) {
    case ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START:
      return event.text !== undefined && validFragment(event.text.id, event.text.text, event.text.metadataJson, { textAllowed: false }) ? { ok: true } : invalidRequest();
    case ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA:
      return event.text !== undefined && validFragment(event.text.id, event.text.text, event.text.metadataJson, { textAllowed: true }) ? { ok: true } : invalidRequest();
    case ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_END:
      return event.text !== undefined && validFragment(event.text.id, event.text.text, event.text.metadataJson, { textAllowed: false }) ? { ok: true } : invalidRequest();
    case ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_START:
      return event.reasoning !== undefined && validFragment(event.reasoning.id, event.reasoning.text, event.reasoning.metadataJson, { textAllowed: false }) ? { ok: true } : invalidRequest();
    case ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_DELTA:
      return event.reasoning !== undefined && validFragment(event.reasoning.id, event.reasoning.text, event.reasoning.metadataJson, { textAllowed: true }) ? { ok: true } : invalidRequest();
    case ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_END:
      return event.reasoning !== undefined && validFragment(event.reasoning.id, event.reasoning.text, event.reasoning.metadataJson, { textAllowed: false }) ? { ok: true } : invalidRequest();
    case ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_START:
      return event.toolInput !== undefined &&
        !invalidBytes(event.toolInput.name, MaxIdBytes) &&
        validFragment(event.toolInput.id, event.toolInput.text, event.toolInput.metadataJson, { textAllowed: false })
        ? { ok: true }
        : invalidRequest();
    case ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_DELTA:
      return event.toolInput !== undefined &&
        !invalidBytes(event.toolInput.name, MaxIdBytes) &&
        validFragment(event.toolInput.id, event.toolInput.text, event.toolInput.metadataJson, { textAllowed: true })
        ? { ok: true }
        : invalidRequest();
    case ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_END:
      return event.toolInput !== undefined &&
        !invalidBytes(event.toolInput.name, MaxIdBytes) &&
        validFragment(event.toolInput.id, event.toolInput.text, event.toolInput.metadataJson, { textAllowed: false })
        ? { ok: true }
        : invalidRequest();
    case ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL:
      return event.toolCall !== undefined &&
        !invalidBytes(event.toolCall.id, MaxIdBytes) &&
        !invalidBytes(event.toolCall.name, MaxIdBytes) &&
        validJson(event.toolCall.inputJson, MaxProviderToolCallInputJsonBytes) &&
        validMetadata(event.toolCall.metadataJson)
        ? { ok: true }
        : invalidRequest();
    case ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH:
      return event.finish !== undefined &&
        validEnumValue(
          event.finish.reason,
          ProviderFinishReason.PROVIDER_FINISH_REASON_STOP,
          ProviderFinishReason.PROVIDER_FINISH_REASON_LENGTH,
          ProviderFinishReason.PROVIDER_FINISH_REASON_TOOL_CALLS,
          ProviderFinishReason.PROVIDER_FINISH_REASON_CONTENT_FILTER,
          ProviderFinishReason.PROVIDER_FINISH_REASON_ERROR,
          ProviderFinishReason.PROVIDER_FINISH_REASON_OTHER,
          ProviderFinishReason.PROVIDER_FINISH_REASON_UNKNOWN,
        ) &&
        (event.finish.usage === undefined || validRequestUsage(event.finish.usage)) &&
        optionalPositiveInteger(event.finish.contextWindowTokens) &&
        optionalPositiveInteger(event.finish.inputLimitTokens) &&
        optionalPositiveInteger(event.finish.outputTokenLimit) &&
        validMetadata(event.finish.metadataJson)
        ? { ok: true }
        : invalidRequest();
    case ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR:
      return event.providerError?.error !== undefined &&
        !invalidBytes(event.providerError.error.code, MaxIdBytes) &&
        !invalidBytes(event.providerError.error.message, MaxProviderErrorMessageBytes) &&
        nonNegativeInteger(event.providerError.error.statusCode) &&
        nonNegativeInteger(event.providerError.error.retryAfterMs) &&
        validMetadata(event.providerError.metadataJson)
        ? { ok: true }
        : invalidRequest();
    case ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_ATTACHMENT_REJECTIONS:
      return validAttachmentRejections(event.attachmentRejections?.rejections)
        ? { ok: true }
        : invalidRequest();
    default:
      return invalidRequest();
  }
}

function validAttachmentRejections(
  rejections: readonly ProviderAttachmentRejection[] | undefined,
): boolean {
  if (!Array.isArray(rejections) || rejections.length === 0 || rejections.length > MaxProviderRequestAttachments) {
    return false;
  }
  const origins = new Set<string>();
  for (const rejection of rejections) {
    if (
      !validEnumValue(
        rejection.reason,
        ProviderAttachmentRejectionReason.PROVIDER_ATTACHMENT_REJECTION_REASON_DELETED,
        ProviderAttachmentRejectionReason.PROVIDER_ATTACHMENT_REJECTION_REASON_OVER_ENVELOPE,
      )
    ) {
      return false;
    }
    let originKey: string;
    if (rejection.transient !== undefined) {
      if (
        rejection.fileBacked !== undefined ||
        invalidBytes(rejection.transient.attachmentRef, MaxIdBytes) ||
        invalidBytes(rejection.transient.sourceToolUseEventId, MaxIdBytes) ||
        invalidBytes(rejection.transient.sourcePath, MaxTextBytes) ||
        !validAttachmentPageRange(rejection.transient.pageRange) ||
        invalidBytes(rejection.transient.detail, MaxAttachmentHintBytes)
      ) {
        return false;
      }
      originKey = JSON.stringify(["transient", rejection.transient.attachmentRef]);
    } else if (
      rejection.fileBacked !== undefined &&
      !invalidBytes(rejection.fileBacked.sourceEventId, MaxIdBytes) &&
      !invalidBytes(rejection.fileBacked.fileId, MaxIdBytes)
    ) {
      originKey = JSON.stringify([
        "file",
        rejection.fileBacked.sourceEventId,
        rejection.fileBacked.fileId,
      ]);
    } else {
      return false;
    }
    if (origins.has(originKey)) {
      return false;
    }
    origins.add(originKey);
  }
  return true;
}

function validRuntimePart(part: RuntimePart): boolean {
  if (invalidBytes(part.id, MaxIdBytes)) {
    return false;
  }
  const payloadCount = [part.text, part.reasoning, part.tool].filter((payload) => payload !== undefined).length;
  if (payloadCount !== 1) {
    return false;
  }
  if (part.text !== undefined) {
    return part.text.text.length > 0 && validJsonString(part.text.text, MaxProviderRequestMessagePartJsonBytes);
  }
  if (part.reasoning !== undefined) {
    return validJsonString(part.reasoning.text, MaxProviderRequestMessagePartJsonBytes) && validMetadata(part.reasoning.metadataJson);
  }
  return part.tool !== undefined &&
    !invalidBytes(part.tool.callId, MaxIdBytes) &&
    !invalidBytes(part.tool.name, MaxIdBytes) &&
    (part.tool.toolUseEventId !== undefined
      ? !invalidBytes(part.tool.toolUseEventId, MaxIdBytes)
      : part.tool.state === RuntimeToolPartState.RUNTIME_TOOL_PART_STATE_ERROR &&
        part.id.startsWith("internal_invalid_tool:") && part.id.endsWith(":part")) &&
    validEnumValue(
      part.tool.state,
      RuntimeToolPartState.RUNTIME_TOOL_PART_STATE_COMPLETED,
      RuntimeToolPartState.RUNTIME_TOOL_PART_STATE_ERROR,
      RuntimeToolPartState.RUNTIME_TOOL_PART_STATE_CANCELLED,
    ) &&
    validJson(part.tool.inputJson, MaxProviderToolCallInputJsonBytes) &&
    validJson(part.tool.outputOrErrorJson, MaxProviderRequestToolOutputJsonBytes);
}

function validFragment(id: string, text: string, metadataJson: string, options: { readonly textAllowed: boolean }): boolean {
  if (invalidBytes(id, MaxIdBytes) || !validMetadata(metadataJson)) {
    return false;
  }
  return options.textAllowed ? text === "" || !invalidBytes(text, MaxTextBytes) : text === "";
}

function validRequestUsage(usage: RequestUsage): boolean {
  return nonNegativeInteger(usage.inputTotalTokens) &&
    nonNegativeInteger(usage.inputUncachedTokens) &&
    optionalNonNegativeInteger(usage.inputCacheReadTokens) &&
    optionalNonNegativeInteger(usage.inputCacheWriteTokens) &&
    nonNegativeInteger(usage.outputTotalTokens) &&
    optionalNonNegativeInteger(usage.outputReasoningTokens) &&
    optionalNonNegativeInteger(usage.totalTokens) &&
    (usage.providerUsageJson === "" || validJson(usage.providerUsageJson, MaxProviderUsageJsonBytes));
}

/** Returns the longest valid-Unicode prefix whose UTF-8 encoding fits the byte budget. */
export function truncateUtf8Bytes(value: string, maxBytes: number): string {
  const encoder = new TextEncoder();
  if (encoder.encode(value).byteLength <= maxBytes) {
    return value;
  }
  const characters: string[] = [];
  let usedBytes = 0;
  for (const character of value) {
    const characterBytes = encoder.encode(character).byteLength;
    if (usedBytes + characterBytes > maxBytes) {
      break;
    }
    characters.push(character);
    usedBytes += characterBytes;
  }
  return characters.join("");
}

function validAttachmentPageRange(value: string): boolean {
  if (value === "") {
    return true;
  }
  if (invalidBytes(value, MaxAttachmentHintBytes)) {
    return false;
  }
  const segments = value.split(",");
  for (const segment of segments) {
    const match = /^([1-9][0-9]{0,3})(?:-([1-9][0-9]{0,3}))?$/.exec(segment);
    if (match === null) {
      return false;
    }
    const start = Number(match[1]);
    const end = match[2] === undefined ? start : Number(match[2]);
    if (end < start) {
      return false;
    }
  }
  return true;
}

function nonNegativeInteger(value: number): boolean {
  return Number.isInteger(value) && value >= 0;
}

function optionalNonNegativeInteger(value: number | undefined): boolean {
  return value === undefined || nonNegativeInteger(value);
}

function optionalPositiveInteger(value: number | undefined): boolean {
  return value === undefined || (Number.isInteger(value) && value > 0);
}

function validateIdFields(...values: readonly string[]): ValidationResult {
  for (const value of values) {
    if (invalidBytes(value, MaxIdBytes)) {
      return invalidRequest();
    }
  }
  return { ok: true };
}

function invalidBytes(value: string, limit: number): boolean {
  return value.length === 0 || exceedsBytes(value, limit);
}

function exceedsBytes(value: string, limit: number): boolean {
  return new TextEncoder().encode(value).byteLength > limit;
}

function validJsonString(value: string, limit: number): boolean {
  return !exceedsBytes(JSON.stringify(value), limit);
}

function invalidBindingGeneration(value: number): boolean {
  return !Number.isInteger(value) || value <= 0 || value > MaxBindingGeneration;
}

function validEnum(value: number, min: number, max: number): boolean {
  return Number.isInteger(value) && value >= min && value <= max;
}

function validEnumValue(value: number, ...allowed: readonly number[]): boolean {
  return Number.isInteger(value) && allowed.includes(value);
}

function validMetadata(value: string): boolean {
  return value === "" || validJsonObject(value, MaxMetadataBytes);
}

function validJsonObject(value: string, limit: number): boolean {
  const parsed = parseJson(value, limit);
  return typeof parsed === "object" && parsed !== null && !Array.isArray(parsed);
}

function validJson(value: string, limit: number): boolean {
  return parseJson(value, limit) !== undefined;
}

function parseJson(value: string, limit: number): unknown {
  if (invalidBytes(value, limit)) {
    return undefined;
  }
  try {
    return JSON.parse(value) as unknown;
  } catch {
    return undefined;
  }
}

function invalidRequest(code?: string, member?: string): ValidationResult {
  return {
    ok: false,
    message: "invalid internal request",
    ...(code === undefined ? {} : { code }),
    ...(member === undefined ? {} : { member }),
  };
}
