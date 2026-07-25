/**
 * Defines MCP Connector process-local gRPC keepalive options shared by its
 * listener and Bridge clients. Active calls ping at the same interval accepted
 * by the internal Go server policy; idle clients do not send pings.
 */
import type { ChannelOptions, ServerOptions } from "@grpc/grpc-js";

/** Active-call keepalive interval paired with internal Go server enforcement. */
export const McpGrpcKeepaliveTimeMs = 30 * 1000;
/** Time allowed for a keepalive acknowledgement before closing the transport. */
export const McpGrpcKeepaliveTimeoutMs = 10 * 1000;

/** Returns keepalive options for MCP Connector outbound clients. */
export function mcpGrpcClientKeepaliveOptions(): ChannelOptions {
  return {
    "grpc.keepalive_time_ms": McpGrpcKeepaliveTimeMs,
    "grpc.keepalive_timeout_ms": McpGrpcKeepaliveTimeoutMs,
    "grpc.keepalive_permit_without_calls": 0,
  };
}

/** Returns active-connection keepalive options for the MCP Connector listener. */
export function mcpGrpcServerKeepaliveOptions(): ServerOptions {
  return {
    "grpc.keepalive_time_ms": McpGrpcKeepaliveTimeMs,
    "grpc.keepalive_timeout_ms": McpGrpcKeepaliveTimeoutMs,
  };
}
