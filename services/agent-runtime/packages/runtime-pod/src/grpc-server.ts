/**
 * Adapts the generated Runtime Pod unary command service to `RuntimeControlService`. Application
 * composition creates this server, which applies the scoped transport bounds, forwards each request
 * and its metadata to the matching control method, and maps status-bearing failures to gRPC errors.
 * All seven command methods share one adapter, unknown failures become a generic internal status, and
 * listener errors do not expose bind details.
 */
import {
  Metadata,
  Server,
  ServerCredentials,
  status,
} from "@grpc/grpc-js";
import type { sendUnaryData, ServerUnaryCall, ServiceError } from "@grpc/grpc-js";
import {
  AgentRuntimePodServiceService,
} from "@tetral/agent-runtime-protocol/src/gen/tetral/agent_runtime/v1/agent_runtime.js";
import { grpcServerOptions } from "./bounds.js";
import { GrpcStatusError } from "./runtime-service.js";
import type { RuntimeControlService } from "./runtime-service.js";

/** Owns the underlying gRPC server together with asynchronous bind and graceful shutdown controls. */
export interface RuntimeGrpcServer {
  readonly server: Server;
  readonly bind: (address: string) => Promise<number>;
  readonly shutdown: () => Promise<void>;
}

/**
 * Creates the internal Runtime Pod command server without binding it to an address.
 * The returned server registers every supported Bridge command and uses insecure transport because
 * caller authentication is enforced from service-account metadata by the control service.
 */
export function createRuntimeGrpcServer(service: RuntimeControlService): RuntimeGrpcServer {
  const server = new Server(grpcServerOptions());
  server.addService(AgentRuntimePodServiceService, {
    acceptInput: commandHandler((request, metadata) => service.acceptInput(request, metadata)),
    acceptAgentMail: commandHandler((request, metadata) => service.acceptAgentMail(request, metadata)),
    acceptTaskNotification: commandHandler((request, metadata) => service.acceptTaskNotification(request, metadata)),
    interrupt: commandHandler((request, metadata) => service.interrupt(request, metadata)),
    resolveToolConfirmation: commandHandler((request, metadata) => service.resolveToolConfirmation(request, metadata)),
    applyRuntimeConfig: commandHandler((request, metadata) => service.applyRuntimeConfig(request, metadata)),
    cleanupSession: commandHandler((request, metadata) => service.cleanupSession(request, metadata)),
  });
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

function commandHandler<Request, Response>(
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
  return serviceError(status.INTERNAL, "runtime pod command failed");
}

function serviceError(code: status, message: string): ServiceError {
  const error = new Error(message) as ServiceError;
  error.code = code;
  error.details = message;
  error.metadata = new Metadata();
  return error;
}
