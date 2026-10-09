/**
 * @packageDocumentation
 * Adapts the validated Gateway provider stream into Runtime LLM event shapes.
 * It guards request identity, completed member identity and ordering, attachment-rejection placement, terminal
 * uniqueness, and protocol completeness before any event reaches request-turn orchestration.
 * The Runtime host constructs a concrete adapter backed by GatewayClient and injects it through
 * the Interface boundary into ThreadLoop, which calls stream. The adapter validates
 * shared protocol bounds and ordering, maps ordinary frames directly, and calls
 * Runtime's bounded constructors when it normalizes failures.
 */
import { Context, Layer, Stream } from "effect";
import {
  ProviderAttachmentRejectionReason,
  ProviderFinishReason,
  ProviderStreamEventType,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { validateProviderStreamEvent } from "@tetral/gateway-protocol/src/bounds.js";
import type {
  ProviderError as GatewayProviderError,
  ProviderRequest,
  ProviderStreamEvent,
  RequestUsage,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { RuntimeFailure } from "./llm-event.js";
import type { LLMEvent, RuntimeJsonValue } from "./llm-event.js";
import { RuntimeJsonPreviewSchema, RuntimeFailureSchema, RuntimePreviewTextMaxBytes, runtimeFailureFromProviderError } from "./llm-event.js";
import { normalizeProviderError } from "../contracts/provider.js";
import { boundRuntimeJson } from "../contracts/runtime.js";

/** Provider request shape accepted by the Runtime LLM boundary. */
export type LLMRequest = ProviderRequest;

/** Transport or protocol failure raised by the Gateway client implementation. */
export interface GatewayClientError {
  readonly type: "gateway-client";
  readonly code: "gateway_unavailable" | "gateway_stream_error" | "gateway_protocol_error" | "gateway_cancelled";
  readonly message: string;
  readonly retryable: boolean;
  readonly fatal: boolean;
  readonly statusCode?: number | undefined;
  readonly retryAfterMs?: number | undefined;
}

/** Streaming Gateway dependency used by the Runtime LLM service. */
export type RuntimeGatewayStreamCancelKind = "caller" | "consumer_validation" | "consumer_early_exit";

/** Transport completion settles only after the adapter has released reader, listener, and call custody. */
export type RuntimeGatewayStreamCompletion =
  | { readonly outcome: "eof" }
  | { readonly outcome: "cancelled"; readonly cancelKind: RuntimeGatewayStreamCancelKind }
  | { readonly outcome: "completion_deadline" }
  | { readonly outcome: "transport_failure"; readonly error: GatewayClientError };

/** One generated Gateway call and its single-consumer event and completion carriers. */
export interface RuntimeGatewayStreamHandle {
  readonly events: Stream.Stream<ProviderStreamEvent>;
  readonly completion: Promise<RuntimeGatewayStreamCompletion>;
  readonly cancel: (reason: RuntimeGatewayStreamCancelKind) => void;
}

export interface GatewayClient {
  readonly streamProviderRequest: (
    request: ProviderRequest,
    options?: { readonly abortSignal?: AbortSignal },
  ) => Promise<RuntimeGatewayStreamHandle>;
}

/** Identity-only observations emitted by the semantic stream owner. */
export interface LLMServiceStreamObserver {
  readonly terminalObserved?: (request: ProviderRequest, terminalKind: "finish" | "provider_error") => void;
  readonly streamClosed?: (
    request: ProviderRequest,
    completion: RuntimeGatewayStreamCompletion,
    terminalCandidateExisted: boolean,
    durationMs: number,
  ) => void;
  readonly nowEpochMs?: () => number;
}

/** Error channel exposed by the Runtime LLM service. */
export interface LLMServiceError {
  readonly type: "llm-service";
  readonly error: RuntimeFailure;
}

/** Effect service interface for one validated provider stream. */
export interface Interface {
  readonly stream: (
    request: LLMRequest,
    options?: { readonly abortSignal?: AbortSignal },
  ) => Stream.Stream<LLMEvent, LLMServiceError>;
}

/** Effect service tag used by request-turn orchestration. */
export class Service extends Context.Service<Service, Interface>()("tetral-agent/LLMService") {}

type RuntimeFinishReason = Exclude<Extract<LLMEvent, { readonly type: "finish" }>["finishReason"], undefined>;

class ProviderStreamValidator {
 private readonly providerParts = new Set<string>();
 private readonly eventIds = new Set<string>();
 private readonly thinking = new Map<string, string>();
 private readonly toolCalls = new Set<string>();
 private attachmentRejectionsSeen = false;
 private providerStreamingStarted = false;
 private terminal = false;
 private nextFrameSequence = 1;
 constructor(private readonly request: ProviderRequest) {}
 map(event: ProviderStreamEvent): LLMEvent | RuntimeFailure {
  if (this.terminal || event.frameSequence !== this.nextFrameSequence || !validateProviderStreamEvent(event).ok) return gatewayProtocolFailure(this.request);
  this.nextFrameSequence++;
  if (event.type === ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_ATTACHMENT_REJECTIONS) return this.attachmentRejections(event);
  this.providerStreamingStarted = true;
  switch(event.type) {
   case ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_THINKING_STARTED: {
    const part=event.thinkingStarted!;
    if(this.providerParts.has(`reasoning:${part.providerPartId}`)||this.eventIds.has(part.eventId)) return gatewayProtocolFailure(this.request);
    this.providerParts.add(`reasoning:${part.providerPartId}`);this.eventIds.add(part.eventId);this.thinking.set(part.providerPartId,part.eventId);
    return {type:"thinking-started",providerPartId:part.providerPartId,eventId:part.eventId};
   }
   case ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_COMPLETE: {
    const part=event.textComplete!;
    if(this.providerParts.has(`text:${part.providerPartId}`)||this.eventIds.has(part.eventId)) return gatewayProtocolFailure(this.request);
    this.providerParts.add(`text:${part.providerPartId}`);this.eventIds.add(part.eventId);
    return {type:"text-complete",providerPartId:part.providerPartId,eventId:part.eventId,text:part.text};
   }
   case ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_COMPLETE: {
    const part=event.reasoningComplete!;
    if(this.thinking.get(part.providerPartId)!==part.thinkingEventId) return gatewayProtocolFailure(this.request);
    this.thinking.delete(part.providerPartId);
    const metadata=metadataFromJson(part.providerMetadataJson);
    return {type:"reasoning-complete",providerPartId:part.providerPartId,thinkingEventId:part.thinkingEventId,text:part.text,...(metadata===undefined?{}:{providerMetadata:metadata})};
   }
   case ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL_COMPLETE: {
    const part=event.toolCallComplete!;
    if(this.toolCalls.has(part.modelToolCallId)) return gatewayProtocolFailure(this.request);
    const input=runtimeJsonFromString(part.inputJson);
    if(input===undefined) return gatewayProtocolFailure(this.request);
    this.toolCalls.add(part.modelToolCallId);
    const metadata=metadataFromJson(part.providerMetadataJson);
    return {type:"tool-call-complete",id:part.modelToolCallId,toolName:part.name,input:input.input,inputPreview:input.inputPreview,...(metadata===undefined?{}:{providerMetadata:metadata})};
   }
   case ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH: {
    if(this.thinking.size>0 || event.finish?.usage===undefined || event.finish.contextWindowTokens===undefined || event.finish.outputTokenLimit===undefined) return gatewayProtocolFailure(this.request);
    this.terminal=true;
    const metadata=metadataFromJson(event.finish.metadataJson);
    return {type:"finish",finishReason:runtimeFinishReason(event.finish.reason),usage:runtimeUsage(event.finish.usage),modelLimits:{contextWindowTokens:event.finish.contextWindowTokens,outputTokenLimit:event.finish.outputTokenLimit,...(event.finish.inputLimitTokens===undefined?{}:{inputLimitTokens:event.finish.inputLimitTokens})},...(metadata===undefined?{}:{providerMetadata:metadata})};
   }
   case ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR:
    this.terminal=true;this.thinking.clear();
    return {type:"provider-error",error:runtimeFailureFromGatewayProviderError(event.providerError?.error)};
   default: return gatewayProtocolFailure(this.request);
  }
 }

  private attachmentRejections(event: ProviderStreamEvent): LLMEvent | RuntimeFailure {
    if (this.providerStreamingStarted || this.attachmentRejectionsSeen) {
      return gatewayProtocolFailure(this.request);
    }
    const requestOrigins = new Set(this.request.attachments.map(providerAttachmentOriginIdentity));
    const rejections = event.attachmentRejections?.rejections.map((rejection) => {
      const origin = rejection.transientAttachmentRef !== undefined
        ? {
            type: "transient" as const,
            attachmentRef: rejection.transientAttachmentRef,
          }
        : rejection.fileBacked !== undefined
          ? {
              type: "file-backed" as const,
              sourceEventId: rejection.fileBacked.sourceEventId,
              fileId: rejection.fileBacked.fileId,
            }
          : undefined;
      const reason = rejection.reason === ProviderAttachmentRejectionReason.PROVIDER_ATTACHMENT_REJECTION_REASON_DELETED
        ? "deleted" as const
        : rejection.reason === ProviderAttachmentRejectionReason.PROVIDER_ATTACHMENT_REJECTION_REASON_OVER_ENVELOPE
          ? "over_envelope" as const
          : undefined;
      return origin === undefined || reason === undefined ? undefined : { origin, reason };
    });
    if (
      rejections === undefined
      || rejections.length === 0
      || rejections.some((rejection) =>
        rejection === undefined || !requestOrigins.has(runtimeAttachmentRejectionOriginIdentity(rejection.origin))
      )
    ) {
      return gatewayProtocolFailure(this.request);
    }
    this.attachmentRejectionsSeen = true;
    return {
      type: "attachment-rejections",
      rejections: rejections as NonNullable<(typeof rejections)[number]>[],
    };
  }

}

function providerAttachmentOriginIdentity(attachment: ProviderRequest["attachments"][number]): string {
  if (attachment.transient !== undefined) {
    return JSON.stringify(["transient", attachment.transient.attachmentRef]);
  }
  if (attachment.fileBacked !== undefined) {
    return JSON.stringify(["file-backed", attachment.fileBacked.sourceEventId, attachment.fileBacked.fileId]);
  }
  return JSON.stringify(["invalid"]);
}

function runtimeAttachmentRejectionOriginIdentity(
  origin: Extract<LLMEvent, { readonly type: "attachment-rejections" }>["rejections"][number]["origin"],
): string {
  return origin.type === "transient"
    ? JSON.stringify(["transient", origin.attachmentRef])
    : JSON.stringify(["file-backed", origin.sourceEventId, origin.fileId]);
}

/** Builds the LLM service around a concrete Gateway streaming client. */
export function createLLMService(gatewayClient: GatewayClient, observer: LLMServiceStreamObserver = {}): Interface {
  return {
    stream(request, options) {
      return streamLLMEvents(request, gatewayClient, options, observer);
    },
  };
}

/** Maps a Gateway provider request into a bounded Runtime event stream. */
export function streamLLMEvents(
  request: LLMRequest,
  gatewayClient: GatewayClient,
  options: { readonly abortSignal?: AbortSignal } = {},
  observer: LLMServiceStreamObserver = {},
): Stream.Stream<LLMEvent, LLMServiceError> {
  return Stream.fromAsyncIterable(
    streamLLMEventsAsync(request, gatewayClient, options, observer),
    (error): LLMServiceError => isLLMServiceError(error) ? error : {
      type: "llm-service",
      error: gatewayStreamFailure(request),
    },
  );
}

async function* streamLLMEventsAsync(
  request: LLMRequest,
  gatewayClient: GatewayClient,
  options: { readonly abortSignal?: AbortSignal },
  observer: LLMServiceStreamObserver,
): AsyncGenerator<LLMEvent> {
  const validator = new ProviderStreamValidator(request);
  const startedAt = (observer.nowEpochMs ?? Date.now)();
  let handle: RuntimeGatewayStreamHandle;
  try {
    handle = await gatewayClient.streamProviderRequest(request, options);
  } catch (error) {
    throw {
      type: "llm-service",
      error: gatewayClientFailure(request, error, options.abortSignal),
    } satisfies LLMServiceError;
  }
  let terminalEvent: LLMEvent | undefined;
  let completion: RuntimeGatewayStreamCompletion | undefined;
  let semanticFailure: RuntimeFailure | undefined;
  try {
    for await (const event of Stream.toAsyncIterable(handle.events)) {
      const mapped = validator.map(event);
      if (isRuntimeFailure(mapped)) {
        semanticFailure = mapped;
        handle.cancel("consumer_validation");
        break;
      }
      if (mapped.type === "provider-error" || mapped.type === "finish") {
        terminalEvent = mapped;
        try {
          observer.terminalObserved?.(request, mapped.type === "finish" ? "finish" : "provider_error");
        } catch {
          // Observability cannot replace stream settlement.
        }
        continue;
      }
      yield mapped;
    }
    completion = await handle.completion;
    recordStreamClosed(observer, request, completion, terminalEvent !== undefined, startedAt);
    if (semanticFailure !== undefined) {
      yield { type: "provider-error", error: semanticFailure };
      return;
    }
    if (completion.outcome === "eof" && terminalEvent !== undefined) {
      yield terminalEvent;
      return;
    }
    if (completion.outcome === "completion_deadline") {
      throw {
        type: "llm-service",
        error: gatewayTransportCompletionDeadlineFailure(request),
      } satisfies LLMServiceError;
    }
    if (completion.outcome === "transport_failure") {
      throw {
        type: "llm-service",
        error: gatewayClientFailure(request, completion.error, options.abortSignal),
      } satisfies LLMServiceError;
    }
    if (completion.outcome === "cancelled") {
      throw {
        type: "llm-service",
        error: gatewayClientFailure(request, gatewayCancelledError(), options.abortSignal),
      } satisfies LLMServiceError;
    }
    yield { type: "provider-error", error: gatewayStreamFailure(request) };
  } finally {
    if (completion === undefined) {
      handle.cancel("consumer_early_exit");
      completion = await handle.completion;
      recordStreamClosed(observer, request, completion, terminalEvent !== undefined, startedAt);
    }
  }
}

/** Provides the configured LLM service as an Effect layer. */
export function layer(gatewayClient: GatewayClient, observer: LLMServiceStreamObserver = {}): Layer.Layer<Service> {
  return Layer.succeed(Service, Service.of(createLLMService(gatewayClient, observer)));
}

function recordStreamClosed(
  observer: LLMServiceStreamObserver,
  request: ProviderRequest,
  completion: RuntimeGatewayStreamCompletion,
  terminalCandidateExisted: boolean,
  startedAt: number,
): void {
  try {
    observer.streamClosed?.(
      request,
      completion,
      terminalCandidateExisted,
      Math.max(0, (observer.nowEpochMs ?? Date.now)() - startedAt),
    );
  } catch {
    // Observability cannot replace stream settlement.
  }
}

function gatewayCancelledError(): GatewayClientError {
  return {
    type: "gateway-client",
    code: "gateway_cancelled",
    message: "Gateway provider stream was cancelled.",
    retryable: false,
    fatal: false,
  };
}

function gatewayTransportCompletionDeadlineFailure(request: ProviderRequest): RuntimeFailure {
  return RuntimeFailureSchema.parse({
    type: "runtime",
    code: "gateway_stream_error",
    reason: "gateway_transport_completion_deadline",
    message: "Gateway provider stream did not complete before the Runtime transport deadline.",
    retryable: true,
    fatal: false,
    providerId: request.model?.providerId,
    modelId: request.model?.modelId,
  });
}

function runtimeFailureFromGatewayProviderError(error: GatewayProviderError | undefined): RuntimeFailure {
  const normalized = normalizeProviderError({
    code: error?.code,
    message: error?.message,
    retryable: error?.retryable,
    fatal: error?.fatal,
    statusCode: error?.statusCode,
    retryAfterMs: (error?.retryAfterMs ?? 0) > 0 ? error?.retryAfterMs : undefined,
  });
  return runtimeFailureFromProviderError(
    normalized,
    normalized.code === "provider_key_unavailable" ||
      normalized.code === "provider_unavailable" ||
      normalized.code === "credential_required" ||
      normalized.code === "credential_unavailable"
      ? { type: "exhausted" }
      : { type: "terminal" },
  );
}

function gatewayClientFailure(request: ProviderRequest, error: unknown, abortSignal: AbortSignal | undefined): RuntimeFailure {
  if (isGatewayClientError(error)) {
    const locallyInterrupted = error.code === "gateway_cancelled" && abortSignal?.aborted === true;
    return RuntimeFailureSchema.parse({
      type: "runtime",
      code: error.code === "gateway_protocol_error" ? "gateway_protocol_error" : "gateway_stream_error",
      message: error.message,
      retryable: error.retryable,
      fatal: error.fatal,
      ...(locallyInterrupted ? { reason: "runtime_shutdown" } : {}),
      providerId: request.model?.providerId,
      modelId: request.model?.modelId,
      ...(error.statusCode !== undefined ? { statusCode: error.statusCode } : {}),
      ...(error.retryAfterMs !== undefined ? { retryAfterMs: error.retryAfterMs } : {}),
    });
  }
  return gatewayStreamFailure(request);
}

function gatewayStreamFailure(request: ProviderRequest): RuntimeFailure {
  return RuntimeFailureSchema.parse({
    type: "runtime",
    code: "gateway_stream_error",
    message: "Gateway stream ended before a terminal ProviderStreamEvent.",
    retryable: true,
    fatal: false,
    providerId: request.model?.providerId,
    modelId: request.model?.modelId,
  });
}

function gatewayProtocolFailure(request: ProviderRequest): RuntimeFailure {
  return RuntimeFailureSchema.parse({
    type: "runtime",
    code: "gateway_protocol_error",
    message: "Gateway stream violated the Runtime ProviderStreamEvent protocol.",
    retryable: false,
    fatal: true,
    providerId: request.model?.providerId,
    modelId: request.model?.modelId,
  });
}

function runtimeJsonFromString(inputJson: string): {
  readonly input: RuntimeJsonValue;
  readonly inputPreview: ReturnType<typeof RuntimeJsonPreviewSchema.parse>;
} | undefined {
  try {
    const value = JSON.parse(inputJson) as RuntimeJsonValue;
    const bounded = boundRuntimeJson(value, RuntimePreviewTextMaxBytes);
    return {
      input: value,
      inputPreview: RuntimeJsonPreviewSchema.parse({
        preview: bounded.preview,
        truncated: bounded.truncated,
      }),
    };
  } catch {
    return undefined;
  }
}

function runtimeUsage(usage: RequestUsage) {
  return {
    inputTokens: usage.inputUncachedTokens,
    outputTokens: usage.outputTotalTokens,
    reasoningTokens: usage.outputReasoningTokens ?? 0,
    cacheReadTokens: usage.inputCacheReadTokens ?? 0,
    cacheWriteTokens: usage.inputCacheWriteTokens ?? 0,
    ...(usage.totalTokens !== undefined ? { totalTokens: usage.totalTokens } : {}),
    providerUsageJson: usage.providerUsageJson,
  };
}

function runtimeFinishReason(reason: ProviderFinishReason | undefined): RuntimeFinishReason {
  switch (reason) {
    case ProviderFinishReason.PROVIDER_FINISH_REASON_STOP:
      return "stop";
    case ProviderFinishReason.PROVIDER_FINISH_REASON_LENGTH:
      return "length";
    case ProviderFinishReason.PROVIDER_FINISH_REASON_TOOL_CALLS:
      return "tool-calls";
    case ProviderFinishReason.PROVIDER_FINISH_REASON_CONTENT_FILTER:
      return "content-filter";
    case ProviderFinishReason.PROVIDER_FINISH_REASON_ERROR:
      return "error";
    case ProviderFinishReason.PROVIDER_FINISH_REASON_OTHER:
      return "other";
    case ProviderFinishReason.PROVIDER_FINISH_REASON_UNKNOWN:
    case ProviderFinishReason.PROVIDER_FINISH_REASON_UNSPECIFIED:
    case ProviderFinishReason.UNRECOGNIZED:
    case undefined:
      return "unknown";
  }
}

function metadataFromJson(metadataJson: string | undefined) {
  if (metadataJson === undefined || metadataJson.length === 0) {
    return undefined;
  }
  try {
    const parsed = JSON.parse(metadataJson) as unknown;
    return typeof parsed === "object" && parsed !== null && !Array.isArray(parsed) && Object.keys(parsed).length > 0
      ? parsed as Record<string, RuntimeJsonValue>
      : undefined;
  } catch {
    return undefined;
  }
}

function isGatewayClientError(error: unknown): error is GatewayClientError {
  return typeof error === "object" &&
    error !== null &&
    "type" in error &&
    error.type === "gateway-client" &&
    "code" in error;
}

function isLLMServiceError(error: unknown): error is LLMServiceError {
  return typeof error === "object" &&
    error !== null &&
    "type" in error &&
    error.type === "llm-service" &&
    "error" in error;
}

function isRuntimeFailure(value: LLMEvent | RuntimeFailure): value is RuntimeFailure {
  return value.type === "runtime" ||
    value.type === "provider" ||
    value.type === "message-store" ||
    value.type === "session-event-writer" ||
    value.type === "session-binding";
}
