// These tests pin Runtime Pod process-local gRPC transport options; shared
// protocol validation is covered by the protocol package's bounds tests.

import { describe, expect, test } from "bun:test";
import {
  MaxAttachmentGrpcMessageBytes,
  MaxGatewayRequestGrpcMessageBytes,
  MaxGatewayStreamEventGrpcMessageBytes,
  MaxGrpcInboundMessageBytes,
  MaxGrpcOutboundMessageBytes,
  GrpcKeepaliveTimeMs,
  GrpcKeepaliveTimeoutMs,
  bridgeAttachmentGrpcChannelOptions,
  gatewayGrpcChannelOptions,
  grpcClientChannelOptions,
  grpcServerOptions,
} from "../../src/bounds.js";

describe("Runtime Pod transport bounds", () => {
  test("configures command and Bridge channel fuses", () => {
    expect(MaxGrpcInboundMessageBytes).toBe(4 * 1024 * 1024);
    expect(MaxGrpcOutboundMessageBytes).toBe(4 * 1024 * 1024);
    expect(MaxAttachmentGrpcMessageBytes).toBe(32 * 1024 * 1024);
    expect(GrpcKeepaliveTimeMs).toBe(30 * 1000);
    expect(GrpcKeepaliveTimeoutMs).toBe(10 * 1000);
    expect(grpcServerOptions()).toEqual({
      "grpc.max_receive_message_length": MaxGrpcInboundMessageBytes,
      "grpc.max_send_message_length": MaxGrpcOutboundMessageBytes,
      "grpc.keepalive_time_ms": GrpcKeepaliveTimeMs,
      "grpc.keepalive_timeout_ms": GrpcKeepaliveTimeoutMs,
    });
    expect(grpcClientChannelOptions()).toEqual({
      "grpc.max_receive_message_length": MaxGrpcInboundMessageBytes,
      "grpc.max_send_message_length": MaxGrpcOutboundMessageBytes,
      "grpc.keepalive_time_ms": GrpcKeepaliveTimeMs,
      "grpc.keepalive_timeout_ms": GrpcKeepaliveTimeoutMs,
      "grpc.keepalive_permit_without_calls": 0,
    });
    expect(bridgeAttachmentGrpcChannelOptions()).toEqual({
      "grpc.max_receive_message_length": MaxAttachmentGrpcMessageBytes,
      "grpc.max_send_message_length": MaxAttachmentGrpcMessageBytes,
      "grpc.keepalive_time_ms": GrpcKeepaliveTimeMs,
      "grpc.keepalive_timeout_ms": GrpcKeepaliveTimeoutMs,
      "grpc.keepalive_permit_without_calls": 0,
    });
    expect(gatewayGrpcChannelOptions()).toEqual({
      "grpc.max_receive_message_length": MaxGatewayStreamEventGrpcMessageBytes,
      "grpc.max_send_message_length": MaxGatewayRequestGrpcMessageBytes,
      "grpc.keepalive_time_ms": GrpcKeepaliveTimeMs,
      "grpc.keepalive_timeout_ms": GrpcKeepaliveTimeoutMs,
      "grpc.keepalive_permit_without_calls": 0,
    });
    expect(MaxGatewayRequestGrpcMessageBytes).toBe(32 * 1024 * 1024);
    expect(MaxGatewayStreamEventGrpcMessageBytes).toBe(512 * 1024);
  });

  test("assigns each Bridge adapter the smallest sufficient channel class", async () => {
    const source = await Bun.file(new URL("../../src/bridge-client.ts", import.meta.url)).text();
    const channelFactoryFor = (className: string): string => {
      const classBody = source.split(`export class ${className}`, 2)[1]?.split("\n}", 1)[0];
      const match = classBody?.match(/credentials\.createInsecure\(\), ([A-Za-z]+ChannelOptions)\(\)/);
      if (match?.[1] === undefined) {
        throw new Error(`missing Bridge channel factory for ${className}`);
      }
      return match[1];
    };

    expect(channelFactoryFor("BridgeAPIControlInputCommitter")).toBe("grpcClientChannelOptions");
    expect(channelFactoryFor("BridgeAPITaskNotificationCommitter")).toBe("grpcClientChannelOptions");
    expect(channelFactoryFor("BridgeAPIApprovalReviewerThreadCreator")).toBe("grpcClientChannelOptions");
    expect(channelFactoryFor("BridgeAPIContextLoader")).toBe("bridgeAttachmentGrpcChannelOptions");
    expect(channelFactoryFor("BridgeAPIEventWriter")).toBe("grpcClientChannelOptions");
    expect(channelFactoryFor("BridgeAPIInternalToolRepairCommitter")).toBe("grpcClientChannelOptions");
  });
});
