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
import type { ChannelOptions, ClientReadableStream, Metadata, ServiceError } from "@grpc/grpc-js";
import {
  ProviderRequest as ProviderRequestMessage,
  ProviderGatewayServiceClient,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type {
  ProviderRequest,
  ProviderStreamEvent,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { Stream } from "effect";
import type { GatewayClient, GatewayClientError } from "@tetral/agent-runtime-core/src/llm/llm-service.js";
import { buildOutboundBearerMetadata } from "./auth.js";
import type { ServiceAccountTokenConfig } from "./auth.js";
import { MaxGatewayRequestGrpcMessageBytes, MaxGatewayStreamEventGrpcMessageBytes } from "./bounds.js";

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
   * otherwise the returned Effect stream yields Gateway-normalized events and
   * translates aborts or gRPC failures into `GatewayClientError` values.
   */
  streamProviderRequest(
    request: ProviderRequest,
    options: { readonly abortSignal?: AbortSignal } = {},
  ): Stream.Stream<ProviderStreamEvent, GatewayClientError> {
    if (ProviderRequestMessage.encode(request).finish().byteLength > MaxGatewayRequestGrpcMessageBytes) {
      return Stream.fail({
        type: "gateway-client",
        code: "gateway_protocol_error",
        message: "Gateway provider request exceeds the transport fuse.",
        retryable: false,
        fatal: true,
      });
    }
    return Stream.fromAsyncIterable(
      streamProviderRequest(this.client, this.metadataFactory, this.options.tokenPath, request, options.abortSignal),
      (error): GatewayClientError => gatewayClientError(error),
    );
  }
}

async function* streamProviderRequest(
  client: ProviderGatewayServiceClient,
  metadataFactory: (config: ServiceAccountTokenConfig) => Promise<Metadata>,
  tokenPath: string,
  request: ProviderRequest,
  abortSignal: AbortSignal | undefined,
): AsyncGenerator<ProviderStreamEvent> {
  const metadata = await metadataFactory({ tokenPath });
  const call = client.streamProviderRequest(request, metadata);
  const cancel = (): void => {
    call.cancel();
  };
  if (abortSignal !== undefined) {
    if (abortSignal.aborted) {
      cancel();
      throw gatewayClientError({ code: status.CANCELLED });
    }
    abortSignal.addEventListener("abort", cancel, { once: true });
  }
  try {
    for await (const event of readableEvents(call)) {
      yield event;
    }
  } finally {
    abortSignal?.removeEventListener("abort", cancel);
  }
}

function readableEvents(call: ClientReadableStream<ProviderStreamEvent>): AsyncIterable<ProviderStreamEvent> {
  return call as unknown as AsyncIterable<ProviderStreamEvent>;
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
//  RESOURCE_EXHAUSTED  | local receive fuse              | gateway_protocol_error | no        | yes
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
// - Retryable is a closed set: only UNAVAILABLE, DEADLINE_EXCEEDED, and remote
//   RESOURCE_EXHAUSTED classify retryable. grpc-js's two local receive-fuse
//   detail prefixes classify as deterministic protocol failures. Every other status, INCLUDING any
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
  if (code === status.RESOURCE_EXHAUSTED && isLocalReceiveFuseError(serviceError)) {
    return {
      type: "gateway-client",
      code: "gateway_protocol_error",
      message: "Gateway response exceeded the transport fuse.",
      retryable: false,
      fatal: true,
      statusCode: code,
    };
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

function isLocalReceiveFuseError(error: Partial<ServiceError>): boolean {
  const details = typeof error.details === "string"
    ? error.details
    : error instanceof Error
      ? error.message
      : "";
  const framedLimit = /Received message larger than max \(\d+ vs (\d+)\)/.exec(details)?.[1];
  const decompressedLimit = /Received message that decompresses to a size larger than (\d+)/.exec(details)?.[1];
  return Number(framedLimit ?? decompressedLimit) === MaxGatewayStreamEventGrpcMessageBytes;
}
