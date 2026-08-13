/**
 * @packageDocumentation
 *
 * Adapts the generated MCP connector gRPC service to the service shell on the
 * process's dedicated internal listener. Runtime-facing RunMcpTool carries
 * bounded JSON input and refs-only results; Bridge-facing ListMcpTools carries
 * scoped catalog requests and tool definitions. Raw decoded media leaves the
 * connector only through its separate Bridge commit client. This adapter applies
 * symmetric message limits and preserves only intentional service status errors,
 * mapping every unexpected failure to a bounded internal error. The process
 * command creates and controls the listener; generated handlers call the service
 * shell, which authenticates callers and verifies Runtime binding identity before
 * tool execution.
 */

import {
  Metadata,
  Server,
  ServerCredentials,
  status,
} from "@grpc/grpc-js";
import type {
  sendUnaryData,
  ServerUnaryCall,
  ServiceError,
} from "@grpc/grpc-js";
import {
  McpConnectorServiceService,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type {
  ListMcpToolsRequest,
  ListMcpToolsResponse,
  McpConnectorServiceServer,
  RunMcpToolRequest,
  RunMcpToolResponse,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { MCP_BLOB_MAX_BYTES } from "./formatter.js";
import { mcpGrpcServerKeepaliveOptions } from "./transport.js";
import { GrpcStatusError, type McpConnectorServiceShell } from "./service.js";

/** Compatibility reserve used to derive the connector listener's message ceiling. */
export const MCP_CONNECTOR_GRPC_OVERHEAD_BYTES = 1024 * 1024;
/** Bounds both inbound and outbound messages on the connector's gRPC listener. */
export const MCP_CONNECTOR_GRPC_MAX_MESSAGE_BYTES = MCP_BLOB_MAX_BYTES + MCP_CONNECTOR_GRPC_OVERHEAD_BYTES;

/** Exposes the underlying listener together with explicit bind and drain operations. */
export interface McpConnectorGrpcServer {
  readonly server: Server;
  readonly bind: (address: string) => Promise<number>;
  readonly shutdown: () => Promise<void>;
}

/**
 * Creates the bounded gRPC listener adapter for tool execution and manifest
 * discovery without binding a network address until the caller requests it.
 */
export function createMcpConnectorGrpcServer(service: McpConnectorServiceShell): McpConnectorGrpcServer {
  const server = new Server({
    "grpc.max_receive_message_length": MCP_CONNECTOR_GRPC_MAX_MESSAGE_BYTES,
    "grpc.max_send_message_length": MCP_CONNECTOR_GRPC_MAX_MESSAGE_BYTES,
    ...mcpGrpcServerKeepaliveOptions(),
  });
  const implementation: McpConnectorServiceServer = {
    runMcpTool: unaryHandler((request, metadata) => service.runMcpTool(request, metadata)),
    listMcpTools: unaryHandler((request, metadata) => service.listMcpTools(request, metadata)),
  };
  server.addService(McpConnectorServiceService, implementation);
  return {
    server,
    bind: async (address) =>
      await new Promise<number>((resolve, reject) => {
        server.bindAsync(address, ServerCredentials.createInsecure(), (error, port) => {
          if (error !== null) {
            reject(new Error("mcp connector grpc listener unavailable"));
            return;
          }
          resolve(port);
        });
      }),
    shutdown: async () =>
      await new Promise<void>((resolve) => {
        server.tryShutdown(() => resolve());
      }),
  };
}

function unaryHandler<Request extends RunMcpToolRequest | ListMcpToolsRequest, Response extends RunMcpToolResponse | ListMcpToolsResponse>(
  handler: (request: Request, metadata: Metadata) => Promise<Response>,
): (call: ServerUnaryCall<Request, Response>, callback: sendUnaryData<Response>) => void {
  return (call, callback) => {
    void unary(() => handler(call.request, call.metadata), callback);
  };
}

async function unary<Response>(
  handler: () => Promise<Response>,
  callback: sendUnaryData<Response>,
): Promise<void> {
  try {
    callback(null, await handler());
  } catch (error) {
    callback(toServiceError(error), null);
  }
}

function toServiceError(error: unknown): ServiceError {
  if (error instanceof GrpcStatusError) {
    return serviceError(error.code, error.message, error.metadata);
  }
  return serviceError(status.INTERNAL, "mcp connector failed");
}

function serviceError(code: status, message: string, metadata = new Metadata()): ServiceError {
  const error = new Error(message) as ServiceError;
  error.code = code;
  error.details = message;
  error.metadata = metadata;
  return error;
}
