/**
 * @packageDocumentation
 *
 * Defines the provider gateway's intentional gRPC status error. Service methods
 * throw this error after admission, authentication, readiness, or stream event
 * checks so the gRPC server can preserve the selected status and bounded message.
 * The server maps all other thrown values to a generic internal failure. This
 * transport error does not represent a provider-stream terminal event and carries
 * no provider response, credential, or request body.
 */
import { status } from "@grpc/grpc-js";

/** Error carrying a deliberate gRPC status code from service logic to the transport adapter. */
export class GrpcStatusError extends Error {
  constructor(
    readonly code: status,
    message: string,
  ) {
    super(message);
    this.name = "GrpcStatusError";
  }
}
