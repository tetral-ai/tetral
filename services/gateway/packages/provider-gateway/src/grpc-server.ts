/**
 * @packageDocumentation
 *
 * Adapts the generated Provider Gateway service definition to the in-process
 * service shell. Application composition creates and binds this server; the
 * adapter forwards Runtime metadata and requests to the shell, writes provider
 * events with gRPC backpressure, and propagates stream cancellation through an
 * abort signal. The same server registers the unary Web compatibility method.
 * Status-bearing service failures retain their bounded gRPC code and message,
 * while unknown failures and listener errors surface generic diagnostics.
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
  ServerWritableStream,
  ServiceError,
} from "@grpc/grpc-js";
import {
  ProviderGatewayServiceService,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type {
  ProviderGatewayServiceServer,
  ProviderRequest,
  ProviderStreamEvent,
  RunWebRequest,
  RunWebResponse,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { grpcServerOptions } from "./bounds.js";
import { GrpcStatusError } from "./errors.js";
import type { ProviderGatewayServiceShell } from "./service.js";

/** Owns the gRPC server together with asynchronous bind and graceful shutdown controls. */
export interface GatewayGrpcServer {
  readonly server: Server;
  readonly bind: (address: string) => Promise<number>;
  readonly shutdown: () => Promise<void>;
}

/**
 * Creates an unbound internal Provider Gateway server with process-scoped
 * message and connection bounds.
 *
 * The listener uses insecure transport. Independently, the service shell
 * authenticates workload bearer metadata and verifies Runtime binding tokens
 * before admitting work; those checks do not provide transport security.
 */
export function createGatewayGrpcServer(service: ProviderGatewayServiceShell): GatewayGrpcServer {
  const server = new Server(grpcServerOptions());
  const implementation: ProviderGatewayServiceServer = {
    streamProviderRequest: (call) => {
      void streamProviderRequest(service, call);
    },
    runWeb: unaryHandler((request, metadata) => service.runWeb(request, metadata)),
  };
  server.addService(ProviderGatewayServiceService, implementation);
  return {
    server,
    bind: async (address) =>
      await new Promise<number>((resolve, reject) => {
        server.bindAsync(address, ServerCredentials.createInsecure(), (error, port) => {
          if (error !== null) {
            reject(new Error("grpc listener unavailable"));
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

async function streamProviderRequest(
  service: ProviderGatewayServiceShell,
  call: ServerWritableStream<ProviderRequest, ProviderStreamEvent>,
): Promise<void> {
  const abortController = new AbortController();
  const abort = (): void => abortController.abort();
  call.once("cancelled", abort);
  try {
    await writeProviderStreamEvents(call, service.streamProviderRequest(call.request, call.metadata, { abortSignal: abortController.signal }));
    if (!call.cancelled) {
      call.end();
    }
  } catch (error) {
    if (!call.cancelled) {
      call.destroy(toServiceError(error));
    }
  } finally {
    call.off("cancelled", abort);
  }
}

/**
 * Writes an asynchronous provider-event sequence until exhaustion or caller
 * cancellation, waiting for either drain or cancellation after backpressure.
 */
export async function writeProviderStreamEvents(
  call: ProviderWritableStream,
  events: AsyncIterable<ProviderStreamEvent>,
): Promise<void> {
  for await (const event of events) {
    if (call.cancelled) {
      return;
    }
    if (!call.write(event)) {
      await waitForDrainOrCancellation(call);
      if (call.cancelled) {
        return;
      }
    }
  }
}

/** Defines the writable gRPC stream capabilities required by provider-event forwarding. */
export interface ProviderWritableStream {
  readonly cancelled: boolean;
  readonly write: (event: ProviderStreamEvent) => boolean;
  readonly on: (event: "cancelled" | "drain", listener: () => void) => unknown;
  readonly off: (event: "cancelled" | "drain", listener: () => void) => unknown;
}

function waitForDrainOrCancellation(call: Pick<ProviderWritableStream, "on" | "off">): Promise<void> {
  return new Promise((resolve) => {
    const done = (): void => {
      call.off("drain", done);
      call.off("cancelled", done);
      resolve();
    };
    call.on("drain", done);
    call.on("cancelled", done);
  });
}

function unaryHandler<Request, Response>(
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
    return serviceError(error.code, error.message);
  }
  return serviceError(status.INTERNAL, "gateway service failed");
}

function serviceError(code: status, message: string): ServiceError {
  const error = new Error(message) as ServiceError;
  error.code = code;
  error.details = message;
  error.metadata = new Metadata();
  return error;
}
