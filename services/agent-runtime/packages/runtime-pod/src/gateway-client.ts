/**
 * Implements Runtime Core's provider-stream client over the internal Gateway
 * gRPC protocol. Runtime Pod command assembly constructs this adapter and the
 * LLM service calls it with a complete ProviderRequest snapshot for each model
 * turn.
 *
 * The adapter rejects oversized encoded requests before transmission, obtains
 * workload bearer metadata for every stream, propagates caller aborts to gRPC
 * cancellation, removes abort listeners during cleanup, and maps transport
 * statuses into the closed retryable/fatal classification consumed by Runtime
 * Core. It calls the generated ProviderGatewayService client, protobuf encoder,
 * and outbound service-account metadata factory; provider lowering,
 * credentials, attachment resolution, and stream event production remain in
 * Gateway.
 */
import { credentials, status } from "@grpc/grpc-js";
import type { CallOptions, ChannelOptions, ClientReadableStream, Metadata, ServiceError } from "@grpc/grpc-js";
import { Readable } from "node:stream";
import {
  ProviderRequest as ProviderRequestMessage,
  ProviderGatewayServiceClient,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type {
  ProviderRequest,
  ProviderStreamEvent,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { Stream } from "effect";
import type {
  GatewayClient,
  GatewayClientError,
  LLMServiceStreamObserver,
  RuntimeGatewayStreamCancelKind,
  RuntimeGatewayStreamCompletion,
  RuntimeGatewayStreamHandle,
} from "@tetral/agent-runtime-core/src/llm/llm-service.js";
import { buildOutboundBearerMetadata } from "./auth.js";
import type { ServiceAccountTokenConfig } from "./auth.js";
import { MaxGatewayRequestGrpcMessageBytes, MaxGatewayStreamEventGrpcMessageBytes } from "./bounds.js";
import { semanticErrorFields } from "@tetral/ts-observability";
import type { RuntimePodLogger } from "./logger.js";

/** Configures Gateway addressing, workload authentication, transport, and injectable test boundaries. */
export interface RuntimePodGatewayClientOptions {
  /** Gateway gRPC target used when no client instance is injected. */
  readonly address: string;
  /** Mounted service-account token read for each outbound stream. */
  readonly tokenPath: string;
  /** Overrides workload metadata construction, primarily for controlled hosts and tests. */
  readonly metadataFactory?: (config: ServiceAccountTokenConfig) => Promise<Metadata>;
  /** Reuses an existing generated client instead of constructing a channel. */
  readonly client?: ProviderGatewayServiceClient;
  /** Extends or overrides the default round-robin channel configuration. */
  readonly channelOptions?: ChannelOptions;
  /** Receives bounded identity-only transport diagnostics. */
  readonly logger?: RuntimePodLogger;
  /** Supplies the call-start clock for deterministic transport-deadline tests. */
  readonly nowEpochMs?: () => number;
  /** Overrides the completion timer only for deterministic hosts and tests. */
  readonly scheduleTimeout?: (callback: () => void, durationMs: number) => ReturnType<typeof setTimeout>;
  readonly cancelTimeout?: (timer: ReturnType<typeof setTimeout>) => void;
}

/** Runtime-owned grace for Gateway transport completion after the request budget. */
export const RuntimeGatewayTransportCompletionAllowanceMs = 10_000;
export const RuntimeGatewayTransportDeadlineGuardMs = 1;

/** Builds the semantic stream observer consumed by Runtime Core's LLM service. */
export function runtimeProviderStreamObserver(
  logger: RuntimePodLogger | undefined,
  nowEpochMs: () => number = Date.now,
): LLMServiceStreamObserver {
  return {
    nowEpochMs,
    terminalObserved(request, terminalKind) {
      try {
        logger?.info({
          event: "runtime_provider_terminal_observed",
          "event.kind": "runtime_provider_terminal_observed",
          operation: "StreamProviderRequest",
          component: "agent-runtime",
          message: "Gateway provider terminal observed",
          "workspace.id": request.workspaceId,
          "session.id": request.sessionId,
          "thread.id": request.sessionThreadId,
          "request.id": request.requestId,
          "model_request.id": request.modelRequestId,
          "provider.id": request.model?.providerId,
          "model.id": request.model?.modelId,
          "terminal.kind": terminalKind,
        });
      } catch {
        // Observability cannot replace provider-stream settlement.
      }
    },
    streamClosed(request, completion, terminalCandidateExisted, durationMs) {
      try {
        const transportError = completion.outcome === "transport_failure" ? completion.error : undefined;
        const statusCode = transportError?.statusCode;
        logger?.info({
          event: "runtime_provider_stream_closed",
          "event.kind": "runtime_provider_stream_closed",
          operation: "StreamProviderRequest",
          component: "agent-runtime",
          message: "Gateway provider stream closed",
          "workspace.id": request.workspaceId,
          "session.id": request.sessionId,
          "thread.id": request.sessionThreadId,
          "request.id": request.requestId,
          "model_request.id": request.modelRequestId,
          "provider.id": request.model?.providerId,
          "model.id": request.model?.modelId,
          "transport.outcome": completion.outcome,
          ...(completion.outcome === "cancelled" ? { "cancel.kind": completion.cancelKind } : {}),
          "terminal.candidate_observed": terminalCandidateExisted,
          "duration.ms": durationMs,
          ...(statusCode === undefined ? {} : {
            "rpc.grpc.status_code": statusCode,
            "rpc.grpc.status_name": grpcStatusName(statusCode),
          }),
          ...(transportError === undefined ? {} : {
            retryable: transportError.retryable,
            terminal: transportError.fatal,
            ...semanticErrorFields({
              errorClass: "gateway_client",
              errorCode: transportError.code,
              messageSafe: "Gateway provider stream failed",
            }),
          }),
        });
      } catch {
        // Observability cannot replace provider-stream settlement.
      }
    },
  };
}

const GatewayRoundRobinChannelOptions: ChannelOptions = {
  "grpc.service_config": JSON.stringify({
    loadBalancingConfig: [{ round_robin: {} }],
  }),
};

/**
 * Streams provider requests from Runtime Core to Gateway while enforcing the
 * Runtime-side transport fuse, workload authentication, cancellation cleanup,
 * and non-leaky transport error normalization.
 */
export class RuntimePodGatewayClient implements GatewayClient {
  private readonly client: ProviderGatewayServiceClient;
  private readonly metadataFactory: (config: ServiceAccountTokenConfig) => Promise<Metadata>;

  /** Creates a Gateway adapter, constructing an insecure in-cluster gRPC channel unless one is injected. */
  constructor(private readonly options: RuntimePodGatewayClientOptions) {
    this.client = options.client ?? new ProviderGatewayServiceClient(
      options.address,
      credentials.createInsecure(),
      { ...GatewayRoundRobinChannelOptions, ...options.channelOptions },
    );
    this.metadataFactory = options.metadataFactory ?? buildOutboundBearerMetadata;
  }

  /**
   * Opens one authenticated Gateway stream for a complete provider request.
   * Requests beyond the outbound message fuse fail locally and deterministically;
   * otherwise the returned handle carries ordered events, an idempotent cancel
   * operation, and one completion result that settles after transport cleanup.
   */
  async streamProviderRequest(
    request: ProviderRequest,
    options: { readonly abortSignal?: AbortSignal } = {},
  ): Promise<RuntimeGatewayStreamHandle> {
    if (ProviderRequestMessage.encode(request).finish().byteLength > MaxGatewayRequestGrpcMessageBytes) {
      const error: GatewayClientError = {
        type: "gateway-client",
        code: "gateway_protocol_error",
        message: "Gateway provider request exceeds the transport fuse.",
        retryable: false,
        fatal: true,
        statusCode: status.RESOURCE_EXHAUSTED,
      };
    logProviderStreamFailed(this.options.logger, request, error, "request_fuse");
      throw error;
    }
    let metadata: Metadata;
    try {
      metadata = await this.metadataFactory({ tokenPath: this.options.tokenPath });
    } catch (error) {
    const normalized = gatewayClientError(error);
    logProviderStreamFailed(this.options.logger, request, normalized, "metadata");
    throw normalized;
    }
    const callStartedAt = (this.options.nowEpochMs ?? Date.now)();
    const deadlineEpochMs = callStartedAt + Math.max(0, request.limits?.timeoutMs ?? 0) + RuntimeGatewayTransportCompletionAllowanceMs;
    let call: ClientReadableStream<ProviderStreamEvent>;
    try {
      call = this.client.streamProviderRequest(request, metadata, {
        // The Runtime timer owns the exact completion boundary. Keeping the
        // transport deadline strictly later prevents scheduler order from
        // relabeling that boundary as an infrastructure failure.
        deadline: new Date(deadlineEpochMs + RuntimeGatewayTransportDeadlineGuardMs),
      } satisfies CallOptions);
    } catch (error) {
      const normalized = gatewayClientError(error);
      logProviderStreamFailed(this.options.logger, request, normalized, "stream_call");
      throw normalized;
    }
    logProviderStreamOpened(this.options.logger, request);
    return runtimeGatewayStreamHandle(
      call,
      request,
      deadlineEpochMs,
      options.abortSignal,
      this.options.nowEpochMs ?? Date.now,
      this.options.scheduleTimeout ?? setTimeout,
      this.options.cancelTimeout ?? clearTimeout,
    );
  }
}

function logProviderStreamFailed(
  logger: RuntimePodLogger | undefined,
  request: ProviderRequest,
  error: GatewayClientError,
  stage: "request_fuse" | "metadata" | "stream_call",
): void {
  try {
    logger?.error({
      event: "runtime_provider_stream_failed",
      "event.kind": "runtime_provider_stream_failed",
      operation: "StreamProviderRequest",
      component: "agent-runtime",
      message: "Gateway provider stream failed before opening",
      "workspace.id": request.workspaceId,
      "session.id": request.sessionId,
      "thread.id": request.sessionThreadId,
      "request.id": request.requestId,
      "model_request.id": request.modelRequestId,
      "provider.id": request.model?.providerId,
      "model.id": request.model?.modelId,
      "transport.stage": stage,
      ...(error.statusCode === undefined ? {} : {
        "rpc.grpc.status_code": error.statusCode,
        "rpc.grpc.status_name": grpcStatusName(error.statusCode),
      }),
      retryable: error.retryable,
      terminal: error.fatal,
      ...semanticErrorFields({
        errorClass: "gateway_client",
        errorCode: error.code,
        messageSafe: "Gateway provider stream failed before opening",
      }),
    });
  } catch {
    // Observability cannot replace provider-stream settlement.
  }
}

function runtimeGatewayStreamHandle(
  call: ClientReadableStream<ProviderStreamEvent>,
  request: ProviderRequest,
  deadlineEpochMs: number,
  abortSignal: AbortSignal | undefined,
  nowEpochMs: () => number,
  scheduleTimeout: (callback: () => void, durationMs: number) => ReturnType<typeof setTimeout>,
  cancelTimeout: (timer: ReturnType<typeof setTimeout>) => void,
): RuntimeGatewayStreamHandle {
  const webStream = Readable.toWeb(call as unknown as Readable) as unknown as ReadableStream<ProviderStreamEvent>;
  const reader = webStream.getReader();
  let winner: RuntimeGatewayStreamCompletion | undefined;
  let cancelRequested = false;
  let callCancelled = false;
  let resolveCompletion!: (completion: RuntimeGatewayStreamCompletion) => void;
  const completion = new Promise<RuntimeGatewayStreamCompletion>((resolve) => {
    resolveCompletion = resolve;
  });
  const cancelCall = (): void => {
    if (!callCancelled) {
      callCancelled = true;
      call.cancel();
    }
  };
  const onAbort = (): void => requestExit({ outcome: "cancelled", cancelKind: "caller" });
  let deadlineTimer: ReturnType<typeof setTimeout> | undefined;

  const cleanup = async (outcome: RuntimeGatewayStreamCompletion): Promise<void> => {
    if (deadlineTimer !== undefined) {
      cancelTimeout(deadlineTimer);
      deadlineTimer = undefined;
    }
    abortSignal?.removeEventListener("abort", onAbort);
    if (outcome.outcome !== "eof") {
      cancelCall();
      try {
        await reader.cancel();
      } catch {
        // The first typed exit remains authoritative.
      }
    }
    try {
      reader.releaseLock();
    } catch {
      // A released or errored reader still has no remaining Runtime custody.
    }
    resolveCompletion(outcome);
  };
  function requestExit(outcome: RuntimeGatewayStreamCompletion): void {
    if (winner !== undefined) {
      return;
    }
    winner = outcome;
    void cleanup(outcome);
  }

  const delayMs = Math.max(0, deadlineEpochMs - nowEpochMs());
  deadlineTimer = scheduleTimeout(() => requestExit({ outcome: "completion_deadline" }), delayMs);
  if (abortSignal?.aborted === true) {
    requestExit({ outcome: "cancelled", cancelKind: "caller" });
  } else {
    abortSignal?.addEventListener("abort", onAbort, { once: true });
  }

  const events = Stream.fromAsyncIterable((async function* (): AsyncGenerator<ProviderStreamEvent> {
    try {
      while (winner === undefined) {
        const result = await reader.read();
        if (result.done) {
          requestExit({ outcome: "eof" });
          break;
        }
        yield result.value;
      }
    } catch (error) {
      if (winner === undefined) {
        requestExit({ outcome: "transport_failure", error: gatewayClientError(error) });
      }
    } finally {
      if (winner === undefined) {
        requestExit({ outcome: "cancelled", cancelKind: cancelRequested ? "consumer_validation" : "consumer_early_exit" });
      }
    }
  })(), (error): never => {
    throw error;
  });

  return {
    events,
    completion,
    cancel(reason) {
      cancelRequested = reason === "consumer_validation";
      requestExit({ outcome: "cancelled", cancelKind: reason });
    },
  };
}

function logProviderStreamOpened(
  logger: RuntimePodLogger | undefined,
  request: ProviderRequest,
): void {
  try {
    logger?.info({
      event: "runtime_provider_stream_opened",
      "event.kind": "runtime_provider_stream_opened",
      operation: "StreamProviderRequest",
      component: "agent-runtime",
      message: "Gateway provider stream opened",
      "workspace.id": request.workspaceId,
      "session.id": request.sessionId,
      "thread.id": request.sessionThreadId,
      "request.id": request.requestId,
      "model_request.id": request.modelRequestId,
      "provider.id": request.model?.providerId,
      "model.id": request.model?.modelId,
    });
  } catch {
    // Observability cannot replace provider-stream settlement.
  }
}

function grpcStatusName(code: number): string {
  switch (code) {
    case status.OK: return "OK";
    case status.CANCELLED: return "CANCELLED";
    case status.UNKNOWN: return "UNKNOWN";
    case status.INVALID_ARGUMENT: return "INVALID_ARGUMENT";
    case status.DEADLINE_EXCEEDED: return "DEADLINE_EXCEEDED";
    case status.NOT_FOUND: return "NOT_FOUND";
    case status.PERMISSION_DENIED: return "PERMISSION_DENIED";
    case status.RESOURCE_EXHAUSTED: return "RESOURCE_EXHAUSTED";
    case status.FAILED_PRECONDITION: return "FAILED_PRECONDITION";
    case status.ABORTED: return "ABORTED";
    case status.OUT_OF_RANGE: return "OUT_OF_RANGE";
    case status.UNIMPLEMENTED: return "UNIMPLEMENTED";
    case status.INTERNAL: return "INTERNAL";
    case status.UNAVAILABLE: return "UNAVAILABLE";
    case status.DATA_LOSS: return "DATA_LOSS";
    case status.UNAUTHENTICATED: return "UNAUTHENTICATED";
    default: return "UNRECOGNIZED";
  }
}

// GATEWAY STATUS CLASSIFICATION (closed enumeration). Maps each gRPC status a
// provider stream can fail with to exactly one GatewayClientError. `retryable`/
// `fatal` are read downstream by the reschedule/backoff consumer in
// @tetral/agent-runtime-core/src/llm/llm-service.ts (gatewayClientFailure), so
// the row picked here decides whether a failed turn burns reschedule budget.
//
//  gRPC status         | classification                  | emitted code           | retryable | fatal
// ---------------------+---------------------------------+------------------------+-----------+------
//  CANCELLED           | local abort / stream cancel     | gateway_cancelled      | no        | no
//  UNAVAILABLE         | transient infra outage          | gateway_unavailable    | yes       | no
//  DEADLINE_EXCEEDED   | transient infra outage          | gateway_unavailable    | yes       | no
//  RESOURCE_EXHAUSTED  | local request send fuse         | gateway_protocol_error | no        | yes
//  RESOURCE_EXHAUSTED  | Gateway request receive fuse    | gateway_protocol_error | no        | yes
//  RESOURCE_EXHAUSTED  | local response receive fuse     | gateway_protocol_error | no        | yes
//  RESOURCE_EXHAUSTED  | remote resource exhaustion      | gateway_unavailable    | yes       | no
//  INVALID_ARGUMENT    | deterministic request-shape rej | gateway_protocol_error | no        | yes
//  INTERNAL            | deterministic fatal             | gateway_stream_error   | no        | yes
//  UNAUTHENTICATED     | deterministic                   | gateway_stream_error   | no        | no
//  PERMISSION_DENIED   | deterministic                   | gateway_stream_error   | no        | no
//  UNIMPLEMENTED       | deterministic                   | gateway_stream_error   | no        | no
//  NOT_FOUND           | deterministic                   | gateway_stream_error   | no        | no
//  (any other status)  | unenumerated -> fail fast       | gateway_stream_error   | no        | no
//
// INVARIANTS:
// - Retryable is a closed set: only UNAVAILABLE, DEADLINE_EXCEEDED, and generic
//   RESOURCE_EXHAUSTED classify retryable. grpc-js's send/receive size details
//   classify as deterministic protocol failures. Every other status, INCLUDING any
//   unenumerated one, falls through the final arm as non-retryable and fails
//   fast. There is deliberately no default-retryable catch-all arm: retrying a
//   doomed request would burn the whole reschedule budget plus minutes of
//   backoff before the caller sees the honest error.
// - INVALID_ARGUMENT is a deterministic request-shape rejection: it closes the
//   turn as gateway_protocol_error and consumes no reschedule budget, because
//   retrying a deterministic rejection reproduces the same rejection.
// - The pre-stream transport-fuse check in streamProviderRequest emits the same
//   non-retryable, fatal gateway_protocol_error for an oversized request before
//   the stream opens.
// UPDATE-WITH: ./bounds.ts (MaxGatewayRequestGrpcMessageBytes transport fuse);
//   @tetral/agent-runtime-core/src/llm/llm-service.ts (GatewayClientError shape
//   and gatewayClientFailure, which read retryable/fatal).
function gatewayClientError(error: unknown): GatewayClientError {
  if (isGatewayClientError(error)) {
    return error;
  }
  const serviceError = error as Partial<ServiceError>;
  const code = serviceError.code;
  if (code === status.CANCELLED) {
    return {
      type: "gateway-client",
      code: "gateway_cancelled",
      message: "Gateway provider stream was cancelled.",
      retryable: false,
      fatal: false,
      statusCode: code,
    };
  }
  if (code === status.RESOURCE_EXHAUSTED) {
    const sizeFailure = gatewayTransportSizeFailure(serviceError);
    if (sizeFailure !== undefined) {
      return {
        type: "gateway-client",
        code: "gateway_protocol_error",
        message: sizeFailure,
        retryable: false,
        fatal: true,
        statusCode: code,
      };
    }
  }
  if (code === status.UNAVAILABLE || code === status.DEADLINE_EXCEEDED || code === status.RESOURCE_EXHAUSTED) {
    return {
      type: "gateway-client",
      code: "gateway_unavailable",
      message: "Gateway provider stream is unavailable.",
      retryable: true,
      fatal: false,
      ...(code !== undefined ? { statusCode: code } : {}),
    };
  }
  if (code === status.INVALID_ARGUMENT) {
    return {
      type: "gateway-client",
      code: "gateway_protocol_error",
      message: "Gateway rejected the provider request.",
      retryable: false,
      fatal: true,
      statusCode: code,
    };
  }
  const fatal = code === status.INTERNAL;
  return {
    type: "gateway-client",
    code: "gateway_stream_error",
    message: "Gateway provider stream failed.",
    retryable: false,
    fatal,
    ...(code !== undefined ? { statusCode: code } : {}),
  };
}

function isGatewayClientError(error: unknown): error is GatewayClientError {
  if (typeof error !== "object" || error === null) {
    return false;
  }
  const candidate = error as Partial<GatewayClientError>;
  return candidate.type === "gateway-client" &&
    (
      candidate.code === "gateway_unavailable" ||
      candidate.code === "gateway_stream_error" ||
      candidate.code === "gateway_protocol_error" ||
      candidate.code === "gateway_cancelled"
    ) &&
    typeof candidate.message === "string" &&
    typeof candidate.retryable === "boolean" &&
    typeof candidate.fatal === "boolean";
}

function gatewayTransportSizeFailure(error: Partial<ServiceError>): string | undefined {
  const details = typeof error.details === "string"
    ? error.details
    : error instanceof Error
      ? error.message
      : "";
  if (/Attempted to send message with a size larger than \d+/.test(details)) {
    return "Gateway request exceeded the local transport fuse.";
  }
  const framedLimit = /Received message larger than max \(\d+ vs (\d+)\)/.exec(details)?.[1];
  const decompressedLimit = /Received message that decompresses to a size larger than (\d+)/.exec(details)?.[1];
  const limit = Number(framedLimit ?? decompressedLimit);
  if (!Number.isFinite(limit)) {
    return undefined;
  }
  return limit === MaxGatewayStreamEventGrpcMessageBytes
    ? "Gateway response exceeded the transport fuse."
    : "Gateway rejected the request above its transport fuse.";
}
