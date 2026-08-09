/**
 * @packageDocumentation
 *
 * Pure request lowering from a validated Runtime-to-Gateway provider request
 * into the intermediate message, tool, structured-output, and call-option plan
 * consumed by the provider client registry. Provider-gateway resolves attachment
 * bytes before calling the registry, which supplies those bytes with the model's
 * fixed ProviderRules; the registry then converts this module's output to AI SDK
 * values and starts the provider stream. This module calls only protocol-shape,
 * rules, and lowering-error code and performs no credential lookup, attachment
 * read, network call, or durable write.
 *
 * Lowering first verifies that the request model and required limits agree with
 * the selected rules. It renders sanitized system segments, runtime messages,
 * completed tool call/result pairs, and a trailing user attachment message;
 * attachment bytes must match request entries one-for-one by origin and
 * metadata. Provider media and reasoning rules run during that render, so empty
 * or unsupported parts are settled before cache control marks the first eligible
 * system messages and last surviving non-system messages. Tool schemas and the
 * approval-reviewer structured-output schema are parsed, sanitized, transformed,
 * and tagged for the provider adapter. Call options merge frozen defaults with a
 * validated effort variant, dynamic thinking budget, request-derived cache key,
 * headers, sampling values, and the provider's output-token omission or clamp.
 * OpenAI-compatible reasoning metadata receives its final SDK key remap only at
 * the explicit adapter boundary exported by this module.
 */
import {
  ProviderRequestKind,
  RuntimeMessageRole,
  RuntimeToolPartState,
  SystemCacheHint,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type {
  ProviderRequest,
  RuntimeMessage,
  RuntimePart,
  ProviderRequestAttachment,
  RuntimeToolDefinition,
  RuntimeToolPart,
  SystemSegment,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { ProviderRequestLoweringError } from "./errors.js";
import type { ProviderRules } from "./rules/rules.js";

/** Complete intermediate request plan consumed by the provider client registry. */
export interface LoweredProviderRequest {
  readonly messages: readonly LoweredProviderMessage[];
  readonly tools: Readonly<Record<string, LoweredToolDefinition>>;
  readonly outputSchema?: LoweredJsonSchema;
  readonly options: LoweredProviderOptions;
}

/** Ordered provider-message union produced after normalization and cache marking. */
export type LoweredProviderMessage =
  | LoweredSystemMessage
  | LoweredUserMessage
  | LoweredAssistantMessage
  | LoweredToolMessage;

/** Sanitized system content, optionally carrying provider cache options. */
export interface LoweredSystemMessage {
  readonly role: "system";
  readonly content: string | readonly LoweredTextPart[];
  readonly providerOptions?: Readonly<Record<string, unknown>>;
}

/** Sanitized user content, including a separately appended attachment message. */
export interface LoweredUserMessage {
  readonly role: "user";
  readonly content: readonly LoweredUserContentPart[];
  readonly providerOptions?: Readonly<Record<string, unknown>>;
}

/** Assistant text, reasoning, or tool-call content ready for SDK conversion. */
export interface LoweredAssistantMessage {
  readonly role: "assistant";
  readonly content: readonly LoweredAssistantContentPart[];
  readonly providerOptions?: Readonly<Record<string, unknown>>;
}

/** Tool results paired with the assistant tool calls emitted immediately before them. */
export interface LoweredToolMessage {
  readonly role: "tool";
  readonly content: readonly LoweredToolResultPart[];
  readonly providerOptions?: Readonly<Record<string, unknown>>;
}

/** Content variants accepted by the provider adapter for a user message. */
export type LoweredUserContentPart = LoweredTextPart | LoweredImagePart | LoweredFilePart;
/** Content variants accepted by the provider adapter for an assistant message. */
export type LoweredAssistantContentPart = LoweredTextPart | LoweredReasoningPart | LoweredToolCallPart;

/** Sanitized provider-visible text with optional part-level provider options. */
export interface LoweredTextPart {
  readonly type: "text";
  readonly text: string;
  readonly providerOptions?: Readonly<Record<string, unknown>>;
}

/** Resolved image bytes retained in memory until the provider adapter consumes them. */
export interface LoweredImagePart {
  readonly type: "image";
  readonly image: Uint8Array;
  readonly mediaType: string;
  readonly providerOptions?: Readonly<Record<string, unknown>>;
}

/** Resolved file bytes with the sanitized filename and declared media type. */
export interface LoweredFilePart {
  readonly type: "file";
  readonly data: Uint8Array;
  readonly filename: string;
  readonly mediaType: string;
  readonly providerOptions?: Readonly<Record<string, unknown>>;
}

/** Reasoning text whose accepted provider metadata is preserved for round-trip replay. */
export interface LoweredReasoningPart {
  readonly type: "reasoning";
  readonly text: string;
  readonly providerMetadata: Readonly<Record<string, unknown>>;
  readonly providerOptions?: Readonly<Record<string, unknown>>;
}

/** Parsed assistant tool call with identifiers sanitized for the selected model. */
export interface LoweredToolCallPart {
  readonly type: "tool-call";
  readonly toolCallId: string;
  readonly toolName: string;
  readonly input: unknown;
  readonly providerOptions?: Readonly<Record<string, unknown>>;
}

/** Parsed tool output paired to a call, with errors and cancellations marked explicitly. */
export interface LoweredToolResultPart {
  readonly type: "tool-result";
  readonly toolCallId: string;
  readonly toolName: string;
  readonly output: unknown;
  readonly isError?: true;
  readonly providerOptions?: Readonly<Record<string, unknown>>;
}

/** Intermediate tool definition whose input schema is consumed by the provider adapter. */
export interface LoweredFunctionToolDefinition {
  readonly kind: "function";
  readonly description: string;
  readonly inputSchema: LoweredJsonSchema;
  readonly outputSchema?: LoweredJsonSchema;
  readonly providerOptions?: Readonly<Record<string, unknown>>;
  readonly larkGrammar?: never;
}

export interface LoweredFreeformToolDefinition {
  readonly kind: "freeform";
  readonly description: string;
  readonly larkGrammar: string;
  readonly inputSchema?: never;
  readonly outputSchema?: never;
  readonly providerOptions?: never;
}

export type LoweredToolDefinition = LoweredFunctionToolDefinition | LoweredFreeformToolDefinition;

/**
 * SDK-free schema plan consumed for tool input and request-level structured
 * output. A tool definition's output schema is retained but is not currently
 * sent by the provider adapter.
 */
export interface LoweredJsonSchema {
  readonly kind: "ai-sdk-json-schema";
  readonly schema: unknown;
}

/** Call-level provider options, headers, sampling values, and optional output limit. */
export interface LoweredProviderOptions {
  readonly providerOptions: Readonly<Record<string, unknown>>;
  readonly headers: Readonly<Record<string, string>>;
  readonly maxOutputTokens?: number;
  readonly temperature?: number;
  readonly topP?: number;
  readonly topK?: number;
}

interface LoweredMessageEnvelope {
  readonly message: LoweredProviderMessage;
  readonly systemCacheCandidate: boolean;
}

/** External catalog limits and already-resolved attachment bytes required for lowering. */
export interface LowerProviderRequestOptions {
  readonly modelOutputTokenLimit: number;
  readonly resolvedAttachments?: readonly ResolvedProviderRequestAttachment[] | undefined;
}

/** Request attachment metadata paired with bytes resolved outside this pure module. */
export interface ResolvedProviderRequestAttachment extends ProviderRequestAttachment {
  readonly data: Uint8Array;
}

/**
 * Lowers one complete provider request according to its exact model rules.
 *
 * Messages are rendered and filtered before cache points are selected. System
 * messages stay first, runtime messages retain their order, each tool part
 * expands to an adjacent assistant-call/tool-result pair, and resolved media is
 * appended as one user message. Tool schemas and the request-level structured
 * output schema use the selected schema strategy; the latter is accepted only
 * for an approval-reviewer request on a model that enables it. Provider options
 * retain the model's frozen defaults for an empty variant, validate any nonempty
 * effort variant, and either omit or clamp the output-token field as directed by
 * the rules.
 *
 * @param request - Validated protocol snapshot for one provider invocation.
 * @param rules - Closed provider/model behavior selected for `request.model`.
 * @param options - Catalog output limit and attachment bytes resolved by the caller.
 * @returns An intermediate request plan for the provider client registry.
 * @throws When model/rule identity, attachments, JSON, tool state, schema use,
 * or effort selection violates the lowering invariants.
 */
export function lowerProviderRequest(request: ProviderRequest, rules: ProviderRules, options: LowerProviderRequestOptions): LoweredProviderRequest {
  if (request.model?.providerId !== rules.providerId || request.model.modelId !== rules.modelId) {
    throw new Error("gateway_protocol_error: provider rules do not match request model");
  }
  if (request.limits === undefined) {
    throw new Error("gateway_protocol_error: request limits are required");
  }
  if (!Number.isInteger(options.modelOutputTokenLimit) || options.modelOutputTokenLimit <= 0) {
    throw new Error("gateway_protocol_error: model output token limit is required");
  }
  // Fixed lowering order -- reordering these steps changes the outbound wire bytes:
  //   1. Attachment + message normalization builds the envelope list
  //      (lowerRequestAttachments, lowerSystemSegment, lowerRuntimeMessage).
  //      Normalization is what drops empty/omitted parts, so a message removed
  //      here is absent from the list the next step observes.
  //   2. applyCacheControl marks cache points over that already-normalized list,
  //      so a message dropped in step 1 can never receive a cache marker.
  //   3. Output-schema and provider-option assembly (lowerRequestOutputSchema,
  //      lowerProviderOptions) run last and do not touch message content.
  // UPDATE-WITH: lowerRequestAttachments, lowerSystemSegment, lowerRuntimeMessage,
  // applyCacheControl, lowerRequestOutputSchema, lowerProviderOptions (this file).
  const attachmentMessage = lowerRequestAttachments(request.attachments, options.resolvedAttachments ?? [], rules);

  const envelopes = [
    ...request.system.map((segment) => lowerSystemSegment(segment)),
    ...request.messages.flatMap((message) => lowerRuntimeMessage(message, rules)),
    ...(attachmentMessage === undefined ? [] : [attachmentMessage]),
  ];
  const messages = applyCacheControl(envelopes, rules);
  const outputSchema = lowerRequestOutputSchema(request, rules);

  return {
    messages,
    tools: lowerTools(request.tools, rules),
    ...(outputSchema !== undefined ? { outputSchema } : {}),
    options: lowerProviderOptions(request, rules, options),
  };
}

/**
 * Moves assistant `reasoning_content` from the model's canonical provider key
 * to the `openaiCompatible` key expected by the OpenAI-compatible SDK adapter.
 * Other provider families and reasoning strategies are returned unchanged, and
 * unrelated provider options remain on the message.
 */
export function remapOpenAICompatibleMessageMetadataForSDK(
  messages: readonly LoweredProviderMessage[],
  rules: ProviderRules,
): readonly LoweredProviderMessage[] {
  const reasoning = rules.reasoning;
  if (rules.providerFamily !== "openai-compatible" || reasoning.strategy !== "provider-options") {
    return messages;
  }
  return messages.map((message) => {
    if (message.role !== "assistant") {
      return message;
    }
    const reasoningContent = providerOptionString(message.providerOptions, reasoning.providerKey, reasoning.fieldName);
    if (reasoningContent === undefined) {
      return message;
    }
    return {
      ...message,
      providerOptions: moveProviderOptionField(
        message.providerOptions ?? {},
        reasoning.providerKey,
        reasoning.fieldName,
        "openaiCompatible",
        { [reasoning.fieldName]: reasoningContent },
      ),
    };
  });
}

function lowerSystemSegment(segment: SystemSegment): LoweredMessageEnvelope {
  return {
    message: {
      role: "system",
      content: sanitizeText(segment.text),
    },
    systemCacheCandidate: segment.cacheHint === SystemCacheHint.SYSTEM_CACHE_HINT_STABLE ||
      segment.cacheHint === SystemCacheHint.SYSTEM_CACHE_HINT_SESSION,
  };
}

function lowerRuntimeMessage(message: RuntimeMessage, rules: ProviderRules): readonly LoweredMessageEnvelope[] {
  if (rules.reasoning.strategy === "provider-options" && message.role === RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_ASSISTANT) {
    return lowerProviderOptionsReasoningMessage(message, rules);
  }

  const envelopes: LoweredMessageEnvelope[] = [];
  const normalParts: Array<LoweredTextPart | LoweredReasoningPart> = [];
  const hasSignedReasoning = message.parts.some((part) => part.reasoning !== undefined && hasSignedProviderReasoning(part, rules));

  for (const part of message.parts) {
    if (part.text !== undefined) {
      const text = sanitizeText(part.text.text);
      if (text.length > 0) {
        normalParts.push({ type: "text", text });
      } else if (hasSignedReasoning && message.role === RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_ASSISTANT) {
        normalParts.push({ type: "text", text: " " });
      }
      continue;
    }
    if (part.reasoning !== undefined) {
      const lowered = lowerReasoningPart(part, rules);
      if (lowered !== undefined) {
        normalParts.push(lowered);
      }
      continue;
    }
    if (part.tool !== undefined) {
      envelopes.push(...lowerToolPart(part.tool, rules));
    }
  }

  if (
    hasSignedReasoning &&
    message.role === RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_ASSISTANT &&
    !normalParts.some((part) => part.type === "text")
  ) {
    normalParts.push({ type: "text", text: " " });
  }

  if (normalParts.length > 0) {
    envelopes.unshift({ message: messageForRole(message.role, normalParts), systemCacheCandidate: false });
  }
  return envelopes;
}

function lowerRequestAttachments(
  attachments: readonly ProviderRequestAttachment[],
  resolvedAttachments: readonly ResolvedProviderRequestAttachment[],
  rules: ProviderRules,
): LoweredMessageEnvelope | undefined {
  if (attachments.length === 0) {
    return undefined;
  }
  if (resolvedAttachments.length !== attachments.length) {
    throw invalidAttachmentResolution();
  }
  const resolvedByOrigin = new Map(resolvedAttachments.map((attachment) => [attachmentOriginKey(attachment), attachment]));
  const content = attachments.map((attachment) => {
    const resolved = resolvedByOrigin.get(attachmentOriginKey(attachment));
    if (resolved === undefined || !sameAttachmentMetadata(attachment, resolved)) {
      throw invalidAttachmentResolution();
    }
    if (attachment.mime === "text/plain" && !mediaSupported(attachment.mime, rules.supportedMedia)) {
      return lowerPlainTextAttachment(attachment, resolved);
    }
    if (!mediaSupported(attachment.mime, rules.supportedMedia)) {
      return unsupportedMediaPlaceholder(attachment, rules);
    }
    if (attachment.mime.startsWith("image/")) {
      return {
        type: "image" as const,
        image: new Uint8Array(resolved.data),
        mediaType: attachment.mime,
      };
    }
    return {
      type: "file" as const,
      data: new Uint8Array(resolved.data),
      filename: sanitizeText(attachment.filename),
      mediaType: attachment.mime,
    };
  });
  return {
    message: {
      role: "user",
      content,
    },
    systemCacheCandidate: false,
  };
}

function lowerPlainTextAttachment(
  attachment: ProviderRequestAttachment,
  resolved: ResolvedProviderRequestAttachment,
): LoweredTextPart {
  const filename = sanitizeText(attachment.filename);
  const text = new TextDecoder("utf-8", { fatal: false }).decode(resolved.data);
  return {
    type: "text",
    text: `[attachment: ${filename} (text/plain)]\n${text}`,
  };
}

function sameAttachmentMetadata(left: ProviderRequestAttachment, right: ProviderRequestAttachment): boolean {
  return left.mime === right.mime &&
    left.filename === right.filename &&
    sameAttachmentOrigin(left, right);
}

function sameAttachmentOrigin(left: ProviderRequestAttachment, right: ProviderRequestAttachment): boolean {
  if (left.transient !== undefined || right.transient !== undefined) {
    return left.transient !== undefined &&
      right.transient !== undefined &&
      left.fileBacked === undefined &&
      right.fileBacked === undefined &&
      left.transient.attachmentRef === right.transient.attachmentRef &&
      left.transient.sourceToolUseEventId === right.transient.sourceToolUseEventId &&
      left.transient.sourcePath === right.transient.sourcePath &&
      left.transient.pageRange === right.transient.pageRange &&
      left.transient.detail === right.transient.detail;
  }
  return left.fileBacked !== undefined &&
    right.fileBacked !== undefined &&
    left.fileBacked.sourceEventId === right.fileBacked.sourceEventId &&
    left.fileBacked.fileId === right.fileBacked.fileId;
}

function attachmentOriginKey(attachment: ProviderRequestAttachment): string {
  if (attachment.transient !== undefined && attachment.fileBacked === undefined) {
    return JSON.stringify(["transient", attachment.transient.attachmentRef]);
  }
  if (attachment.fileBacked !== undefined && attachment.transient === undefined) {
    return JSON.stringify(["file", attachment.fileBacked.sourceEventId, attachment.fileBacked.fileId]);
  }
  throw invalidAttachmentResolution();
}

function mediaSupported(mime: string, support: ProviderRules["supportedMedia"]): boolean {
  return support.exactMimes.includes(mime) || support.mimePrefixes.some((prefix) => mime.startsWith(prefix));
}

function unsupportedMediaPlaceholder(attachment: ProviderRequestAttachment, rules: ProviderRules): LoweredTextPart {
  const source = attachment.transient === undefined
    ? attachment.fileBacked?.fileId ?? "unknown"
    : attachment.transient.sourcePath.length === 0
      ? attachment.transient.attachmentRef
      : attachment.transient.sourcePath;
  return {
    type: "text",
    text: sanitizeText(`ERROR: cannot read attachment ${attachment.filename} (${attachment.mime}) from ${source}: media is not supported by ${rules.providerId}/${rules.modelId}.`),
  };
}

function invalidAttachmentResolution(): ProviderRequestLoweringError {
  return new ProviderRequestLoweringError({
    code: "provider_request_invalid",
    message: "Resolved attachments do not match the provider request.",
    retryable: false,
    fatal: true,
    statusCode: 400,
  });
}

function lowerProviderOptionsReasoningMessage(message: RuntimeMessage, rules: ProviderRules): readonly LoweredMessageEnvelope[] {
  if (rules.reasoning.strategy !== "provider-options") {
    throw new Error("gateway_protocol_error: provider-options reasoning strategy is not configured");
  }
  const reasoningRules = rules.reasoning;

  const envelopes: LoweredMessageEnvelope[] = [];
  const normalParts: LoweredTextPart[] = [];
  const reasoningParts: string[] = [];

  for (const part of message.parts) {
    if (part.text !== undefined) {
      const text = sanitizeText(part.text.text);
      if (text.length > 0) {
        normalParts.push({ type: "text", text });
      }
      continue;
    }
    if (part.reasoning !== undefined) {
      reasoningParts.push(sanitizeText(part.reasoning.text));
      continue;
    }
    if (part.tool !== undefined) {
      envelopes.push(...lowerToolPart(part.tool, rules));
    }
  }

  if (normalParts.length > 0 || reasoningParts.length > 0) {
    envelopes.unshift({ message: { role: "assistant", content: normalParts }, systemCacheCandidate: false });
  }

  const reasoningContent = reasoningParts.length > 0 ? reasoningParts.join("") : "";
  let reasoningApplied = false;
  return envelopes.map((envelope) => {
    if (envelope.message.role !== "assistant") {
      return envelope;
    }
    const content = reasoningApplied ? "" : reasoningContent;
    reasoningApplied = true;
    return {
      ...envelope,
      message: withProviderOption(envelope.message, reasoningRules.providerKey, {
        [reasoningRules.fieldName]: reasoningRules.ensureAssistantReasoning ? content : reasoningContent,
      }),
    };
  });
}

function lowerReasoningPart(part: RuntimePart, rules: ProviderRules): LoweredReasoningPart | LoweredTextPart | undefined {
  const reasoning = part.reasoning;
  if (reasoning === undefined) {
    return undefined;
  }
  const text = sanitizeText(reasoning.text);
  if (rules.reasoning.strategy !== "provider-metadata") {
    return text.length > 0 ? { type: "text", text } : undefined;
  }
  const metadata = transformProviderReasoningMetadata(parseMetadataJson(reasoning.metadataJson), rules.reasoning);
  const providerMetadata = providerMetadataFor(metadata, rules.reasoning.metadataKey);
  if (providerMetadata !== undefined && (!rules.reasoning.requireProviderMetadataContent || hasAnyMetadata(providerMetadata))) {
    if (
      text.length === 0 &&
      (!rules.reasoning.preserveEmptySignedReasoning || !hasSignedMetadata(providerMetadata)) &&
      (!rules.reasoning.preserveEmptyProviderMetadata || !hasAnyMetadata(providerMetadata))
    ) {
      return undefined;
    }
    return { type: "reasoning", text, providerMetadata: metadata };
  }
  return rules.reasoning.unmatchedReasoning === "text" && text.length > 0 ? { type: "text", text } : undefined;
}

function lowerToolPart(tool: RuntimeToolPart, rules: ProviderRules): readonly LoweredMessageEnvelope[] {
  const sanitizedToolCallId = sanitizeText(tool.callId);
  const toolCallId = rules.scrubToolCallIds ? scrubClaudeToolCallId(sanitizedToolCallId) : sanitizedToolCallId;
  const toolName = sanitizeText(tool.name);
  const input = parseProviderJson(tool.inputJson, "tool input");
  const toolCall: LoweredMessageEnvelope = {
    message: {
      role: "assistant",
      content: [{ type: "tool-call", toolCallId, toolName, input }],
    },
    systemCacheCandidate: false,
  };
  const toolResult: LoweredMessageEnvelope = {
    message: {
      role: "tool",
      content: [lowerToolResult(tool, toolCallId, toolName)],
    },
    systemCacheCandidate: false,
  };
  return [toolCall, toolResult];
}

function lowerToolResult(tool: RuntimeToolPart, toolCallId: string, toolName: string): LoweredToolResultPart {
  switch (tool.state) {
    case RuntimeToolPartState.RUNTIME_TOOL_PART_STATE_COMPLETED:
      return {
        type: "tool-result",
        toolCallId,
        toolName,
        output: parseProviderJson(tool.outputOrErrorJson, "tool output"),
      };
    case RuntimeToolPartState.RUNTIME_TOOL_PART_STATE_ERROR:
      return {
        type: "tool-result",
        toolCallId,
        toolName,
        output: parseProviderJson(tool.outputOrErrorJson, "tool error"),
        isError: true,
      };
    case RuntimeToolPartState.RUNTIME_TOOL_PART_STATE_CANCELLED:
      return {
        type: "tool-result",
        toolCallId,
        toolName,
        output: { type: "text", text: "[tool execution cancelled]" },
        isError: true,
      };
    default:
      throw new Error("gateway_protocol_error: unknown tool state cannot be lowered");
  }
}

function messageForRole(
  role: RuntimeMessageRole,
  parts: readonly (LoweredTextPart | LoweredReasoningPart)[],
): LoweredUserMessage | LoweredAssistantMessage {
  switch (role) {
    case RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_USER:
      return { role: "user", content: parts.filter((part): part is LoweredTextPart => part.type === "text") };
    case RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_ASSISTANT:
      return { role: "assistant", content: parts };
    default:
      throw new Error("gateway_protocol_error: only user and assistant messages carry text or reasoning parts");
  }
}

function lowerTools(tools: readonly RuntimeToolDefinition[], rules: ProviderRules): Readonly<Record<string, LoweredToolDefinition>> {
  const loweredTools: Record<string, LoweredToolDefinition> = {};
  for (const tool of tools) {
    const toolName = sanitizeText(tool.name);
    if (Object.hasOwn(loweredTools, toolName)) {
      throw new Error("gateway_protocol_error: tool names collide after surrogate sanitization");
    }
    const declarationCount = Number(tool.function !== undefined) + Number(tool.freeform !== undefined);
    if (declarationCount !== 1) {
      throw new Error("gateway_protocol_error: tool declaration must select exactly one arm");
    }
    let lowered: LoweredToolDefinition;
    if (tool.function !== undefined) {
      lowered = {
        kind: "function",
        description: sanitizeText(tool.description),
        inputSchema: wrapJsonSchema(lowerJsonSchema(parseProviderJson(tool.function.inputSchemaJson, `${tool.name} input schema`), rules)),
        ...(tool.function.outputSchemaJson !== undefined ? { outputSchema: wrapJsonSchema(lowerJsonSchema(parseProviderJson(tool.function.outputSchemaJson, `${tool.name} output schema`), rules)) } : {}),
        ...(rules.toolOptions !== undefined ? { providerOptions: providerToolOptions(rules.toolOptions) } : {}),
      };
    } else {
      if (rules.freeformTools !== "openai-custom" || tool.freeform === undefined || tool.freeform.larkGrammar.length === 0) {
        throw new Error("gateway_protocol_error: provider does not support freeform tool declarations");
      }
      lowered = {
        kind: "freeform",
        description: sanitizeText(tool.description),
        larkGrammar: tool.freeform.larkGrammar,
      };
    }
    loweredTools[toolName] = lowered;
  }
  return loweredTools;
}

function lowerRequestOutputSchema(request: ProviderRequest, rules: ProviderRules): LoweredJsonSchema | undefined {
  const approvalReviewerRequest = request.requestKind === ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER;
  if (!approvalReviewerRequest) {
    if (request.outputSchemaJson !== undefined) {
      throw new Error("gateway_protocol_error: request output schema is not allowed for this request kind");
    }
    return undefined;
  }
  if (request.outputSchemaJson === undefined) {
    throw new Error("gateway_protocol_error: approval reviewer output schema is required");
  }
  if (rules.requestOutputSchema !== "approval-reviewer") {
    throw new Error("gateway_protocol_error: approval reviewer output schema is not enabled for model");
  }
  const schema = parseProviderJson(request.outputSchemaJson, "approval reviewer output schema");
  if (typeof schema !== "object" || schema === null || Array.isArray(schema)) {
    throw new Error("gateway_protocol_error: approval reviewer output schema must be a JSON object");
  }
  return wrapJsonSchema(lowerJsonSchema(schema, rules));
}

function lowerProviderOptions(request: ProviderRequest, rules: ProviderRules, options: LowerProviderRequestOptions): LoweredProviderOptions {
  const providerOptions = applyRequestProviderOptions(
    applyDynamicProviderOptions(
      mergeProviderOptionEffort(rules.providerOptions, request.model?.variant ?? "", rules),
      request,
      options,
      rules,
    ),
    request,
    rules,
  );
  return {
    providerOptions,
    headers: rules.headers,
    ...(rules.maxOutputTokens === "clamp" ? { maxOutputTokens: clampedOutputTokens(request, rules, options) } : {}),
    ...(rules.sampling.temperature !== undefined ? { temperature: rules.sampling.temperature } : {}),
    ...(rules.sampling.topP !== undefined ? { topP: rules.sampling.topP } : {}),
    ...(rules.sampling.topK !== undefined ? { topK: rules.sampling.topK } : {}),
  };
}

function clampedOutputTokens(request: ProviderRequest, rules: ProviderRules, options: LowerProviderRequestOptions): number {
  const outputLimit = effectiveOutputTokenLimit(request, options);
  const thinkingBudget = dynamicThinkingBudgetTokens(request, rules, options);
  return outputLimit - thinkingBudget;
}

function applyDynamicProviderOptions(
  providerOptions: Readonly<Record<string, unknown>>,
  request: ProviderRequest,
  options: LowerProviderRequestOptions,
  rules: ProviderRules,
): Readonly<Record<string, unknown>> {
  if (rules.dynamicProviderOptions === undefined) {
    return providerOptions;
  }
  const dynamic: Record<string, unknown> = {};
  if (rules.dynamicProviderOptions.thinkingBudget !== undefined) {
    dynamic.thinking = {
      type: rules.dynamicProviderOptions.thinkingBudget.type,
      budgetTokens: dynamicThinkingBudgetTokens(request, rules, options),
    };
  }
  return withProviderOptionObject(providerOptions, rules.dynamicProviderOptions.providerKey, dynamic);
}

function dynamicThinkingBudgetTokens(
  request: ProviderRequest,
  rules: ProviderRules,
  options: LowerProviderRequestOptions,
): number {
  if (rules.dynamicProviderOptions?.thinkingBudget === undefined) {
    return 0;
  }
  return Math.min(
    rules.dynamicProviderOptions.thinkingBudget.maxBudgetTokens,
    Math.max(0, Math.floor(effectiveOutputTokenLimit(request, options) / 2 - 1)),
  );
}

function effectiveOutputTokenLimit(request: ProviderRequest, options: LowerProviderRequestOptions): number {
  // An unset request limit (proto3 zero) means the provider's own documented
  // model output limit governs; an explicit request value only ever narrows it.
  const requested = request.limits?.maxOutputTokens ?? 0;
  return requested > 0 ? Math.min(requested, options.modelOutputTokenLimit) : options.modelOutputTokenLimit;
}

function applyRequestProviderOptions(
  providerOptions: Readonly<Record<string, unknown>>,
  request: ProviderRequest,
  rules: ProviderRules,
): Readonly<Record<string, unknown>> {
  if (rules.requestOptions === undefined) {
    return providerOptions;
  }
  const dynamic: Record<string, unknown> = {};
  if (rules.requestOptions.promptCacheKeyField === "promptCacheKey" && rules.requestOptions.promptCacheKeySource === "session_id") {
    dynamic.promptCacheKey = request.sessionId;
  }
  return withProviderOptionObject(providerOptions, rules.requestOptions.providerKey, dynamic);
}

function providerToolOptions(toolOptions: NonNullable<ProviderRules["toolOptions"]>): Readonly<Record<string, unknown>> {
  return withProviderOptionObject({}, toolOptions.providerKey, {
    ...(toolOptions.strict === false ? { strict: false } : {}),
  });
}

function mergeProviderOptionEffort(
  providerOptions: Readonly<Record<string, unknown>>,
  variant: string,
  rules: ProviderRules,
): Readonly<Record<string, unknown>> {
  if (rules.effort === undefined) {
    return providerOptions;
  }
  if (rules.effort.noEffortVariants.includes(variant)) {
    return providerOptions;
  }
  const effort = variant;
  if (!rules.effort.allowed.includes(effort)) {
    throw new Error("gateway_protocol_error: unsupported model variant for provider");
  }
  const providerValue = providerOptions[rules.effort.providerKey];
  const providerObject = typeof providerValue === "object" && providerValue !== null && !Array.isArray(providerValue)
    ? providerValue as Record<string, unknown>
    : {};
  return {
    ...providerOptions,
    [rules.effort.providerKey]: {
      ...providerObject,
      [rules.effort.providerOptionKey]: effort,
    },
  };
}

function applyCacheControl(
  envelopes: readonly LoweredMessageEnvelope[],
  rules: ProviderRules,
): readonly LoweredProviderMessage[] {
  if (rules.cacheControl === undefined) {
    return envelopes.map((envelope) => envelope.message);
  }
  const cacheControl = rules.cacheControl;
  const systemIndexes = envelopes
    .map((envelope, index) => ({ envelope, index }))
    .filter(({ envelope }) => envelope.message.role === "system" && envelope.systemCacheCandidate)
    .slice(0, cacheControl.firstSystemMessages)
    .map(({ index }) => index);
  const nonSystemIndexes = envelopes
    .map((envelope, index) => ({ envelope, index }))
    .filter(({ envelope }) => envelope.message.role !== "system")
    .slice(-cacheControl.lastNonSystemMessages)
    .map(({ index }) => index);
  const cached = new Set([...systemIndexes, ...nonSystemIndexes]);
  return envelopes.map((envelope, index) => cached.has(index) ? withCacheControl(envelope.message, cacheControl) : envelope.message);
}

function withCacheControl(message: LoweredProviderMessage, cacheControl: NonNullable<ProviderRules["cacheControl"]>): LoweredProviderMessage {
  switch (cacheControl.placement) {
    case "message":
      return withMessageCacheControl(message, cacheControl);
    case "content":
      return withContentCacheControl(message, cacheControl);
  }
}

function withContentCacheControl(message: LoweredProviderMessage, cacheControl: NonNullable<ProviderRules["cacheControl"]>): LoweredProviderMessage {
  if (message.role === "system") {
    const content = typeof message.content === "string"
      ? [{ type: "text" as const, text: message.content }]
      : message.content;
    if (content.length === 0) {
      return message;
    }
    const lastIndex = content.length - 1;
    return { ...message, content: content.map((part, index) => index === lastIndex ? withPartProviderOption(part, cacheControl) : part) };
  }
  if (message.content.length === 0) {
    return message;
  }
  const lastIndex = message.content.length - 1;
  switch (message.role) {
    case "user":
      return { ...message, content: message.content.map((part, index) => index === lastIndex ? withPartProviderOption(part, cacheControl) : part) };
    case "assistant":
      return { ...message, content: message.content.map((part, index) => index === lastIndex ? withPartProviderOption(part, cacheControl) : part) };
    case "tool":
      return { ...message, content: message.content.map((part, index) => index === lastIndex ? withPartProviderOption(part, cacheControl) : part) };
  }
}

function withMessageCacheControl(message: LoweredProviderMessage, cacheControl: NonNullable<ProviderRules["cacheControl"]>): LoweredProviderMessage {
  const existingProviderOptions = "providerOptions" in message ? message.providerOptions ?? {} : {};
  const existingProviderValue = existingProviderOptions[cacheControl.providerKey];
  const existingProviderObject = typeof existingProviderValue === "object" && existingProviderValue !== null && !Array.isArray(existingProviderValue)
    ? existingProviderValue as Record<string, unknown>
    : {};
  return {
    ...message,
    providerOptions: {
      ...existingProviderOptions,
      [cacheControl.providerKey]: {
        ...existingProviderObject,
        cacheControl: { type: "ephemeral" },
      },
    },
  };
}

function withPartProviderOption<T extends LoweredUserContentPart | LoweredAssistantContentPart | LoweredToolResultPart>(
  part: T,
  cacheControl: NonNullable<ProviderRules["cacheControl"]>,
): T {
  return {
    ...part,
    providerOptions: withProviderOptionObject(part.providerOptions ?? {}, cacheControl.providerKey, {
      cacheControl: { type: "ephemeral" },
    }),
  };
}

function hasSignedProviderReasoning(part: RuntimePart, rules: ProviderRules): boolean {
  if (rules.reasoning.strategy !== "provider-metadata") {
    return false;
  }
  const metadata = parseMetadataJson(part.reasoning?.metadataJson ?? "{}");
  const providerMetadata = providerMetadataFor(metadata, rules.reasoning.metadataKey);
  return providerMetadata !== undefined && hasSignedMetadata(providerMetadata);
}

function withProviderOption<T extends LoweredProviderMessage>(
  message: T,
  providerKey: string,
  providerValue: Readonly<Record<string, unknown>>,
): T {
  return {
    ...message,
    providerOptions: withProviderOptionObject(
      "providerOptions" in message ? message.providerOptions ?? {} : {},
      providerKey,
      providerValue,
    ),
  };
}

function withProviderOptionObject(
  providerOptions: Readonly<Record<string, unknown>>,
  providerKey: string,
  providerValue: Readonly<Record<string, unknown>>,
): Readonly<Record<string, unknown>> {
  const existingProviderValue = providerOptions[providerKey];
  const existingProviderObject = typeof existingProviderValue === "object" && existingProviderValue !== null && !Array.isArray(existingProviderValue)
    ? existingProviderValue as Record<string, unknown>
    : {};
  return {
    ...providerOptions,
    [providerKey]: {
      ...existingProviderObject,
      ...providerValue,
    },
  };
}

function providerOptionString(
  providerOptions: Readonly<Record<string, unknown>> | undefined,
  providerKey: string,
  fieldName: string,
): string | undefined {
  const value = providerOptions?.[providerKey];
  if (!isPlainRecord(value)) {
    return undefined;
  }
  const field = value[fieldName];
  return typeof field === "string" ? field : undefined;
}

function moveProviderOptionField(
  providerOptions: Readonly<Record<string, unknown>>,
  sourceProviderKey: string,
  sourceFieldName: string,
  targetProviderKey: string,
  targetProviderValue: Readonly<Record<string, unknown>>,
): Readonly<Record<string, unknown>> {
  const output: Record<string, unknown> = { ...providerOptions };
  const sourceValue = output[sourceProviderKey];
  if (isPlainRecord(sourceValue)) {
    const sourceCopy = { ...sourceValue };
    delete sourceCopy[sourceFieldName];
    if (Object.keys(sourceCopy).length === 0) {
      delete output[sourceProviderKey];
    } else {
      output[sourceProviderKey] = sourceCopy;
    }
  }
  return withProviderOptionObject(output, targetProviderKey, targetProviderValue);
}

function isPlainRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function providerMetadataFor(metadata: Readonly<Record<string, unknown>>, key: string): Readonly<Record<string, unknown>> | undefined {
  const value = metadata[key];
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? value as Readonly<Record<string, unknown>>
    : undefined;
}

function hasSignedMetadata(metadata: Readonly<Record<string, unknown>>): boolean {
  return typeof metadata.signature === "string" || typeof metadata.redactedData === "string";
}

function hasAnyMetadata(metadata: Readonly<Record<string, unknown>>): boolean {
  return Object.keys(metadata).length > 0;
}

function transformProviderReasoningMetadata(
  metadata: Readonly<Record<string, unknown>>,
  rules: Extract<ProviderRules["reasoning"], { strategy: "provider-metadata" }>,
): Readonly<Record<string, unknown>> {
  if (rules.stripProviderMetadataKeys === undefined || rules.stripProviderMetadataKeys.length === 0) {
    return metadata;
  }
  const providerValue = providerMetadataFor(metadata, rules.metadataKey);
  if (providerValue === undefined) {
    return metadata;
  }
  const stripped = Object.fromEntries(Object.entries(providerValue).filter(([key]) => !rules.stripProviderMetadataKeys?.includes(key)));
  return {
    ...metadata,
    [rules.metadataKey]: stripped,
  };
}

function parseMetadataJson(value: string): Readonly<Record<string, unknown>> {
  const parsed = parseJson(value === "" ? "{}" : value, "reasoning metadata");
  return typeof parsed === "object" && parsed !== null && !Array.isArray(parsed) ? parsed as Readonly<Record<string, unknown>> : {};
}

function parseProviderJson(value: string, label: string): unknown {
  return sanitizeProviderJsonValue(parseJson(value, label));
}

function parseJson(value: string, label: string): unknown {
  try {
    return JSON.parse(value) as unknown;
  } catch {
    throw new Error(`gateway_protocol_error: invalid ${label} JSON`);
  }
}

function wrapJsonSchema(schema: unknown): LoweredJsonSchema {
  return { kind: "ai-sdk-json-schema", schema };
}

function lowerJsonSchema(schema: unknown, rules: ProviderRules): unknown {
  switch (rules.schemaStrategy) {
    case "passthrough":
      return schema;
    case "openai-codex":
      return lowerOpenAIJsonSchema(schema);
    case "moonshot":
      return lowerMoonshotJsonSchema(schema);
  }
}

const openAIAllowedSchemaKeywords = new Set([
  "type",
  "properties",
  "required",
  "additionalProperties",
  "items",
  "$ref",
  "$defs",
  "definitions",
  "anyOf",
  "oneOf",
  "allOf",
  "enum",
  "description",
  "format",
  "multipleOf",
  "minimum",
  "maximum",
  "exclusiveMinimum",
  "exclusiveMaximum",
  "minLength",
  "maxLength",
  "pattern",
  "minItems",
  "maxItems",
]);

function lowerOpenAIJsonSchema(schema: unknown): unknown {
  if (typeof schema === "boolean") {
    return { type: "string" };
  }
  if (Array.isArray(schema) || typeof schema !== "object" || schema === null) {
    return { type: "string" };
  }
  const input = schema as Record<string, unknown>;
  const output: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(input)) {
    if (key === "const") {
      output.enum ??= [value];
      continue;
    }
    if (!openAIAllowedSchemaKeywords.has(key)) {
      continue;
    }
    switch (key) {
      case "type": {
        const loweredType = lowerOpenAISchemaType(value);
        if (loweredType !== undefined) {
          output.type = loweredType;
        }
        break;
      }
      case "properties":
        if (typeof value === "object" && value !== null && !Array.isArray(value)) {
          output.properties = Object.fromEntries(Object.entries(value as Record<string, unknown>).map(([propertyName, propertySchema]) => [
            propertyName,
            lowerOpenAIJsonSchema(propertySchema),
          ]));
        }
        break;
      case "items":
        output.items = Array.isArray(value)
          ? lowerOpenAIJsonSchema(value[0] ?? {})
          : lowerOpenAIJsonSchema(value);
        break;
      case "$defs":
      case "definitions":
        if (typeof value === "object" && value !== null && !Array.isArray(value)) {
          output[key] = Object.fromEntries(Object.entries(value as Record<string, unknown>).map(([definitionName, definitionSchema]) => [
            definitionName,
            lowerOpenAIJsonSchema(definitionSchema),
          ]));
        }
        break;
      case "anyOf":
      case "oneOf":
      case "allOf":
        if (Array.isArray(value)) {
          output[key] = value.map((schema) => lowerOpenAIJsonSchema(schema));
        }
        break;
      case "additionalProperties":
        output.additionalProperties = typeof value === "object" && value !== null && !Array.isArray(value)
          ? lowerOpenAIJsonSchema(value)
          : value;
        break;
      default:
        output[key] = value;
        break;
    }
  }
  output.type ??= inferOpenAISchemaType(output);
  if (output.type === "object" && output.properties === undefined) {
    output.properties = {};
  }
  if (output.type === "array" && output.items === undefined) {
    output.items = { type: "string" };
  }
  return output;
}

function lowerOpenAISchemaType(value: unknown): string | readonly string[] | undefined {
  if (typeof value === "string") {
    return value === "null" ? undefined : value;
  }
  if (!Array.isArray(value)) {
    return undefined;
  }
  const candidates = value.filter((entry): entry is string => typeof entry === "string" && entry !== "null");
  return candidates.length === 0 ? undefined : candidates.length === 1 ? candidates[0] : candidates;
}

function inferOpenAISchemaType(schema: Readonly<Record<string, unknown>>): string {
  if (schema.properties !== undefined) {
    return "object";
  }
  if (schema.items !== undefined) {
    return "array";
  }
  if (schema.enum !== undefined || schema.format !== undefined) {
    return "string";
  }
  if (
    schema.minimum !== undefined ||
    schema.maximum !== undefined ||
    schema.exclusiveMinimum !== undefined ||
    schema.exclusiveMaximum !== undefined ||
    schema.multipleOf !== undefined
  ) {
    return "number";
  }
  return "string";
}

function lowerMoonshotJsonSchema(schema: unknown): unknown {
  if (Array.isArray(schema)) {
    return schema.map((item) => lowerMoonshotJsonSchema(item));
  }
  if (typeof schema !== "object" || schema === null) {
    return schema;
  }
  const input = schema as Record<string, unknown>;
  if (typeof input.$ref === "string") {
    return { $ref: input.$ref };
  }
  return Object.fromEntries(Object.entries(input).map(([key, value]) => [
    key,
    key === "items" && Array.isArray(value)
      ? lowerMoonshotJsonSchema(value[0] ?? {})
      : lowerMoonshotJsonSchema(value),
  ]));
}

function sanitizeProviderJsonValue(value: unknown): unknown {
  if (typeof value === "string") {
    return sanitizeText(value);
  }
  if (Array.isArray(value)) {
    return value.map((item) => sanitizeProviderJsonValue(item));
  }
  if (typeof value === "object" && value !== null) {
    return Object.fromEntries(
      Object.entries(value).map(([key, entry]) => [
        sanitizeText(key),
        sanitizeProviderJsonValue(entry),
      ]),
    );
  }
  return value;
}

function scrubClaudeToolCallId(value: string): string {
  return value.replace(/[^A-Za-z0-9_-]/g, "_");
}

function sanitizeText(value: string): string {
  let output = "";
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (next >= 0xdc00 && next <= 0xdfff) {
        output += value[index] ?? "";
        output += value[index + 1] ?? "";
        index += 1;
      } else {
        output += "\uFFFD";
      }
      continue;
    }
    if (code >= 0xdc00 && code <= 0xdfff) {
      output += "\uFFFD";
      continue;
    }
    output += value[index] ?? "";
  }
  return output;
}
