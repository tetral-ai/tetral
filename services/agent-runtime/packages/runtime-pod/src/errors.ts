/**
 * Defines the status-bearing error shared by Runtime Pod lifecycle, command handling, and the unary
 * gRPC adapter. Lifecycle and service code throw this error for deliberate transport outcomes, and
 * the gRPC server preserves its status and safe message while mapping every unknown error to a
 * generic internal failure. The class carries no raw dependency response or diagnostic payload.
 */
import type { status } from "@grpc/grpc-js";

/** Represents an intentional gRPC status and caller-safe message from Runtime Pod command handling. */
export class GrpcStatusError extends Error {
  constructor(readonly code: status, message: string) {
    super(message);
    this.name = "GrpcStatusError";
  }
}
