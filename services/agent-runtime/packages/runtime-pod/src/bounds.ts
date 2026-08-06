// Runtime Pod process-local gRPC transport options live here; protocol bounds
// remain limited to validating the wire shapes shared with callers.
// The command server, Bridge clients, Gateway client composition, and compatibility
// checks call these factories. Each factory pins both send and receive ceilings for
// its own traffic class so widening one internal carrier does not widen another.

import type { ChannelOptions, ServerOptions } from "@grpc/grpc-js";

/** Maximum inbound Runtime Pod command message size, including its protobuf envelope. */
export const MaxGrpcInboundMessageBytes = 4 * 1024 * 1024;
/** Maximum outbound Runtime Pod command response size, including its protobuf envelope. */
export const MaxGrpcOutboundMessageBytes = 4 * 1024 * 1024;
/** Symmetric message ceiling for Bridge attachment transfers. */
export const MaxAttachmentGrpcMessageBytes = 32 * 1024 * 1024;
/** Symmetric message ceiling for durable declarations and complete cold context. */
// UPDATE-WITH: internal/sessionrpc/bounds.go (MaxBridgeAPIGRPCMessageBytes);
// services/bridge/cmd/bridge-api/main.go.
export const MaxBridgeDurableContextGrpcMessageBytes = 64 * 1024 * 1024;
/** Maximum serialized provider request the Runtime Pod sends to Gateway. */
// UPDATE-WITH: services/gateway/packages/provider-gateway/src/bounds.ts
// (MaxGrpcInboundMessageBytes).
export const MaxGatewayRequestGrpcMessageBytes = 64 * 1024 * 1024;
/** Maximum provider stream event the Runtime Pod accepts from Gateway. */
// UPDATE-WITH: services/gateway/packages/provider-gateway/src/bounds.ts
// (MaxGrpcOutboundMessageBytes).
export const MaxGatewayStreamEventGrpcMessageBytes = 8 * 1024 * 1024;
/** Maximum encoded Web request sent from Runtime to web-connector. */
export const MaxWebRequestGrpcMessageBytes = 1024 * 1024;
/** Maximum encoded Web response received from web-connector. */
export const MaxWebResponseGrpcMessageBytes = 512 * 1024;
/** Active-call keepalive interval paired with the internal Go server minimum. */
export const GrpcKeepaliveTimeMs = 30 * 1000;
/** Time allowed for an internal keepalive acknowledgement before closing the channel. */
export const GrpcKeepaliveTimeoutMs = 10 * 1000;

function grpcClientKeepaliveOptions(): ChannelOptions {
  return {
    "grpc.keepalive_time_ms": GrpcKeepaliveTimeMs,
    "grpc.keepalive_timeout_ms": GrpcKeepaliveTimeoutMs,
    "grpc.keepalive_permit_without_calls": 0,
  };
}

function grpcServerKeepaliveOptions(): ServerOptions {
  return {
    "grpc.keepalive_time_ms": GrpcKeepaliveTimeMs,
    "grpc.keepalive_timeout_ms": GrpcKeepaliveTimeoutMs,
  };
}

/** Returns the scoped send and receive ceilings used by the Runtime Pod command server. */
export function grpcServerOptions(): ServerOptions {
  return {
    "grpc.max_receive_message_length": MaxGrpcInboundMessageBytes,
    "grpc.max_send_message_length": MaxGrpcOutboundMessageBytes,
    ...grpcServerKeepaliveOptions(),
  };
}

/** Returns symmetric command-channel ceilings for Runtime Pod gRPC client compatibility. */
export function grpcClientChannelOptions(): ChannelOptions {
  return {
    "grpc.max_receive_message_length": MaxGrpcInboundMessageBytes,
    "grpc.max_send_message_length": MaxGrpcOutboundMessageBytes,
    ...grpcClientKeepaliveOptions(),
  };
}

/** Returns the symmetric Bridge API channel ceilings used by Runtime-to-Bridge clients. */
export function bridgeAttachmentGrpcChannelOptions(): ChannelOptions {
  return {
    "grpc.max_receive_message_length": MaxAttachmentGrpcMessageBytes,
    "grpc.max_send_message_length": MaxAttachmentGrpcMessageBytes,
    ...grpcClientKeepaliveOptions(),
  };
}

/** Returns the Bridge channel ceilings for durable declarations and complete cold context. */
export function bridgeDurableContextGrpcChannelOptions(): ChannelOptions {
  return {
    "grpc.max_receive_message_length": MaxBridgeDurableContextGrpcMessageBytes,
    "grpc.max_send_message_length": MaxBridgeDurableContextGrpcMessageBytes,
    ...grpcClientKeepaliveOptions(),
  };
}

/** Returns the asymmetric Gateway channel ceilings for large requests and bounded stream events. */
export function gatewayGrpcChannelOptions(): ChannelOptions {
  return {
    "grpc.max_receive_message_length": MaxGatewayStreamEventGrpcMessageBytes,
    "grpc.max_send_message_length": MaxGatewayRequestGrpcMessageBytes,
    ...grpcClientKeepaliveOptions(),
  };
}

/** Returns the paired Web request and response ceilings used by Runtime's Web client. */
export function webGrpcChannelOptions(): ChannelOptions {
  return {
    "grpc.max_receive_message_length": MaxWebResponseGrpcMessageBytes,
    "grpc.max_send_message_length": MaxWebRequestGrpcMessageBytes,
    ...grpcClientKeepaliveOptions(),
  };
}
