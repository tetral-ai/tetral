import { InterceptingCall, status } from "@grpc/grpc-js";
import type { Interceptor, Metadata, MethodDefinition, ServiceDefinition } from "@grpc/grpc-js";
import { ROOT_CONTEXT, SpanKind, trace } from "@opentelemetry/api";
import type { Context } from "@opentelemetry/api";
import { contextWithFields, runInContext } from "./context.js";
import { currentTelemetry } from "./telemetry.js";
import { activeTraceparent, contextWithTraceparent } from "./tracing.js";

/** Writes the active span's traceparent into outgoing metadata; without an active span it writes nothing. */
export function injectTraceparent(metadata: Pick<Metadata, "set">): void {
  const value = activeTraceparent();
  if (value !== undefined) metadata.set("traceparent", value);
}

/**
 * Client interceptor for calls to Engine services. grpc-js starts the call in
 * the caller's context, so the traceparent is the caller's active span.
 */
export const traceparentClientInterceptor: Interceptor = (options, nextCall) =>
  new InterceptingCall(nextCall(options), {
    start: (metadata, listener, next) => {
      injectTraceparent(metadata);
      next(metadata, listener);
    },
  });

type Handler = (call: { readonly metadata: Metadata } & NodeJS.EventEmitter, callback?: (error: { code?: number } | null, ...rest: unknown[]) => void) => void;

/** gRPC status names as Go's `codes.Code` spells them, indexed by code, so both languages record one vocabulary. */
const statusNames = ["OK", "Canceled", "Unknown", "InvalidArgument", "DeadlineExceeded", "NotFound", "AlreadyExists", "PermissionDenied", "ResourceExhausted", "FailedPrecondition", "Aborted", "OutOfRange", "Unimplemented", "Internal", "Unavailable", "DataLoss", "Unauthenticated"] as const;

/** The request fields that name a workspace, Session or thread, as ts-proto spells them, with their correlation field names. */
const requestCorrelationFields = [["workspaceId", "workspace.id"], ["sessionId", "session.id"], ["threadId", "thread.id"]] as const;

/** Codes a caller may retry, as Go's interceptor classifies them. */
const retryableCodes: ReadonlySet<number> = new Set([status.DEADLINE_EXCEEDED, status.RESOURCE_EXHAUSTED, status.ABORTED, status.UNAVAILABLE]);

/**
 * Wraps each method of a gRPC service implementation as Go's server
 * interceptors record a call: it runs in a server span named by the full
 * method path, a child of the caller's `traceparent` or a new trace without
 * one, under the request's workspace, Session and thread as correlation
 * fields; it ends with one `grpc_server_request_duration_seconds` sample and
 * one `DEBUG` `internal.grpc.request` record, a failed call included, since
 * the component that handles a failure records it. The span carries the
 * status in `rpc.grpc.status_name`; a failed call adds `error.class`
 * `grpc_error` and the lowercase status name as `error.code`.
 */
export function continueGrpcTraces<Implementation extends object>(definition: ServiceDefinition, implementation: Implementation): Implementation {
  const traced: Record<string, unknown> = { ...(implementation as Record<string, unknown>) };
  for (const [name, method] of Object.entries(definition) as [string, MethodDefinition<unknown, unknown>][]) {
    const handler = (implementation as Record<string, unknown>)[name];
    if (typeof handler !== "function") continue;
    traced[name] = tracedHandler(method.path, method.responseStream, handler.bind(implementation) as Handler);
  }
  return traced as Implementation;
}

function tracedHandler(spanName: string, responseStream: boolean, handler: Handler): Handler {
  return (call, callback) => {
    const { context, finish } = startServerSpan(spanName, call.metadata, requestFields((call as { readonly request?: unknown }).request));
    if (responseStream) {
      call.once("finish", () => finish(status.OK));
      call.once("error", (error: { code?: number }) => finish(error?.code ?? status.UNKNOWN));
      call.once("cancelled", () => finish(status.CANCELLED));
      call.once("close", () => finish(status.OK));
      runInContext(context, () => handler(call, callback));
      return;
    }
    runInContext(context, () => handler(call, (error, ...rest) => {
      finish(error === null || error === undefined ? status.OK : typeof error.code === "number" ? error.code : status.UNKNOWN);
      callback?.(error, ...rest);
    }));
  };
}

function requestFields(request: unknown): Record<string, string> {
  const fields: Record<string, string> = {};
  if (typeof request !== "object" || request === null) return fields;
  for (const [property, field] of requestCorrelationFields) {
    const value = (request as Record<string, unknown>)[property];
    if (typeof value === "string" && value !== "") fields[field] = value;
  }
  return fields;
}

function startServerSpan(method: string, metadata: Metadata, fields: Record<string, string>): { readonly context: Context; readonly finish: (code: number) => void } {
  const telemetry = currentTelemetry();
  const started = telemetry.monotonicClock();
  const parent = contextWithFields(contextWithTraceparent(ROOT_CONTEXT, metadata.get("traceparent")[0]), fields);
  const span = telemetry.tracer.startSpan(method, { kind: SpanKind.SERVER, attributes: fields }, parent);
  const context = trace.setSpan(parent, span);
  let ended = false;
  return {
    context,
    finish: (code) => {
      if (ended) return;
      ended = true;
      const elapsedMs = Math.max(0, telemetry.monotonicClock() - started);
      const name = statusNames[code] ?? "Unknown";
      span.setAttribute("rpc.grpc.status_name", name);
      if (code !== status.OK) span.setAttributes({ "error.class": "grpc_error", "error.code": name.toLowerCase() });
      span.end();
      telemetry.grpcRequests.observe(elapsedMs / 1_000, telemetry.service, method, name);
      const failure = code === status.OK ? {} : {
        retryable: retryableCodes.has(code), terminal: !retryableCodes.has(code),
        "error.class": "grpc_error", "error.code": name.toLowerCase(), "error.message_safe": "internal gRPC request failed",
      };
      runInContext(context, () => telemetry.record("debug", {
        event: "internal.grpc.request", operation: "internal.grpc.request", "event.kind": "grpc_request", component: "internal-grpc",
        "grpc.method": method, "grpc.code": name, "duration.ms": Math.trunc(elapsedMs), ...failure,
      }));
    },
  };
}
