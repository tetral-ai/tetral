/**
 * @packageDocumentation
 *
 * Holds provider-gateway process bounds that depend on gRPC server options or
 * the RunWeb compatibility surface. `ProviderGatewayServiceShell` uses the
 * request validator before binding-token verification and returns the bounded
 * fallback response after successful admission; `createGatewayGrpcServer`
 * applies the channel and connection-lifetime options when it constructs the
 * server.
 *
 * RunWeb admission validates identity fields, token and generation bounds,
 * operation shape, field byte sizes, and HTTP(S) URL syntax. It does not perform
 * DNS or SSRF validation. The gRPC receive limit carries a full provider request,
 * while the smaller send limit bounds internal stream events; connection age and
 * grace settings allow client re-resolution without severing expected in-flight
 * turns. Shared scalar limits come from the protocol package, and no protocol
 * package code imports this process-specific module.
 */
import {
  RunWebStatus,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type {
  RunWebRequest,
  RunWebResponse,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import {
  MaxBindingGeneration,
  MaxIdBytes,
  MaxTextBytes,
  MaxTokenBytes,
} from "@tetral/gateway-protocol/src/bounds.js";
import type { ValidationResult } from "@tetral/gateway-protocol/src/bounds.js";

// UPDATE-WITH: services/web-connector/types.go; services/agent-runtime/packages/
// runtime-pod/src/tool-runner.ts.
const MaxWebOperations = 8;
const MaxSearchDomains = 4;
const MaxDomainBytes = 253;
// The Runtime->Gateway request-channel fuse. 32 MiB is sized to the catalog's
// largest full-context request (largest context window serialized plus envelope
// overhead), the same order as the upstream provider's own public request cap.
// It is pinned at BOTH ends: this server-receive length AND the runtime-pod
// client's send length (grpc.max_send_message_length, plus its pre-send encoded-
// size check) must carry the same value — setting one without the other leaves
// the smaller wall in force on the model path.
// UPDATE-WITH: services/agent-runtime/packages/runtime-pod/src/bounds.ts
const MaxGrpcInboundMessageBytes = 32 * 1024 * 1024;
const MaxGrpcOutboundMessageBytes = 8 * 1024 * 1024;
// Connection-lifecycle bounds that drive per-call load balancing across replicas.
// max_connection_age (5 min) forces clients to periodically drop and re-resolve
// DNS so newly scaled-out replicas start receiving traffic; the grace (30 min)
// exceeds the longest expected turn, so a stream in flight when GOAWAY is sent
// finishes under grace rather than being severed. Both values are load-bearing
// only together with the headless Gateway Service (per-pod DNS A records) and the
// runtime-pod client's round_robin channel config; changing any one alone breaks
// even distribution.
// UPDATE-WITH: services/gateway/k8s (headless Service manifest),
//              services/agent-runtime/packages/runtime-pod/src/gateway-client.ts
const GatewayGrpcMaxConnectionAgeMs = 5 * 60 * 1000;
const GatewayGrpcMaxConnectionAgeGraceMs = 30 * 60 * 1000;
/** Active-call keepalive interval paired with Runtime Pod clients. */
export const GatewayGrpcKeepaliveTimeMs = 30 * 1000;
/** Time allowed for a keepalive acknowledgement before closing the connection. */
export const GatewayGrpcKeepaliveTimeoutMs = 10 * 1000;

// validateRunWebRequest and grpcServerOptions live
// in this process package because they depend on web-URL validation
// (validWebURLSyntax) and gRPC option shapes; the dependency direction is
// provider-gateway -> protocol, and the protocol package holds only the pure
// request/stream validators.
/** Validates the process-owned RunWeb request fields before service execution. */
export function validateRunWebRequest(request: RunWebRequest): ValidationResult {
  const ids = validateIdFields(request.workspaceId, request.sessionId, request.sessionThreadId, request.toolUseEventId, request.bindingId);
  if (
    !ids.ok ||
    request.input === undefined ||
    invalidBytes(request.runtimeBindingToken, MaxTokenBytes) ||
    invalidBindingGeneration(request.bindingGeneration)
  ) {
    return invalidRequest();
  }
  const operations =
    request.input.searchQuery.length +
    request.input.open.length +
    request.input.find.length;
  if (operations <= 0 || operations > MaxWebOperations) {
    return invalidRequest();
  }
  for (const search of request.input.searchQuery) {
    if (
      invalidBytes(search.q, MaxTextBytes) ||
      search.domains.length > MaxSearchDomains ||
      search.domains.some((domain) => invalidBytes(domain, MaxDomainBytes))
    ) {
      return invalidRequest();
    }
  }
  for (const open of request.input.open) {
    const hasUrl = open.url !== undefined && open.url !== "";
    const hasRef = open.refId !== undefined && open.refId !== "";
    if (hasUrl === hasRef) {
      return invalidRequest();
    }
    if (open.url !== undefined && (invalidBytes(open.url, MaxTextBytes) || !validWebURLSyntax(open.url))) {
      return invalidRequest();
    }
    if (open.refId !== undefined && invalidBytes(open.refId, MaxIdBytes)) {
      return invalidRequest();
    }
    if (open.lineno !== undefined && (!Number.isInteger(open.lineno) || open.lineno < 1)) {
      return invalidRequest();
    }
  }
  for (const find of request.input.find) {
    if (invalidBytes(find.refId, MaxIdBytes) || invalidBytes(find.pattern, MaxTextBytes)) {
      return invalidRequest();
    }
  }
  return { ok: true };
}

/** Returns gRPC message and connection-lifetime options for the provider gateway server. */
export function grpcServerOptions() {
  return {
    "grpc.max_receive_message_length": MaxGrpcInboundMessageBytes,
    "grpc.max_send_message_length": MaxGrpcOutboundMessageBytes,
    "grpc.max_connection_age_ms": GatewayGrpcMaxConnectionAgeMs,
    "grpc.max_connection_age_grace_ms": GatewayGrpcMaxConnectionAgeGraceMs,
    "grpc.keepalive_time_ms": GatewayGrpcKeepaliveTimeMs,
    "grpc.keepalive_timeout_ms": GatewayGrpcKeepaliveTimeoutMs,
  };
}

function validateIdFields(...values: readonly string[]): ValidationResult {
  for (const value of values) {
    if (invalidBytes(value, MaxIdBytes)) {
      return invalidRequest();
    }
  }
  return { ok: true };
}

function invalidBytes(value: string, limit: number): boolean {
  return value.length === 0 || new TextEncoder().encode(value).byteLength > limit;
}

function invalidBindingGeneration(value: number): boolean {
  return !Number.isInteger(value) || value <= 0 || value > MaxBindingGeneration;
}

function validWebURLSyntax(value: string): boolean {
  try {
    const parsed = new URL(value);
    return parsed.protocol === "http:" || parsed.protocol === "https:";
  } catch {
    return false;
  }
}

function invalidRequest(): ValidationResult {
  return { ok: false, message: "invalid internal request" };
}
