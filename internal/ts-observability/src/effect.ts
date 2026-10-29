/**
 * @packageDocumentation
 *
 * The layer that makes an Effect program record through this package: its
 * `Effect.log*` calls become records and its `Effect.withSpan` spans become
 * spans of the process telemetry. It needs `effect` and
 * `@effect/opentelemetry`, which the program's workspace provides.
 */
import * as OtelTracer from "@effect/opentelemetry/Tracer";
import { ROOT_CONTEXT, trace } from "@opentelemetry/api";
import type * as Otel from "@opentelemetry/api";
import { Cause, Effect, Layer, Logger, References, Tracer } from "effect";
import type { LogLevel as EffectLogLevel } from "effect/LogLevel";
import type { LogLevel } from "./config.js";
import { runInContext } from "./context.js";
import type { FieldValue } from "./context.js";
import { lookupEvent } from "./declarations.js";
import { spanAttributes } from "./observe.js";
import type { Telemetry } from "./telemetry.js";
import { currentTelemetry } from "./telemetry.js";

const levels: Readonly<Record<EffectLogLevel, LogLevel | undefined>> = {
  All: undefined, None: undefined, Fatal: "error", Error: "error", Warn: "warn", Info: "info", Debug: "debug", Trace: "debug",
};

/**
 * A layer that replaces Effect's default loggers, the tracer logger included,
 * with one logger writing this package's records and installs a tracer backed
 * by `@effect/opentelemetry` on the telemetry's tracer.
 */
export function observabilityLayer(telemetry: Telemetry): Layer.Layer<never> {
  return Layer.mergeAll(
    Logger.layer([recordLogger(telemetry)]),
    Layer.succeed(References.MinimumLogLevel, "Trace"),
    Layer.effect(Tracer.Tracer, Effect.map(OtelTracer.make, failureAttributingTracer)).pipe(
      Layer.provide(Layer.succeed(OtelTracer.OtelTracer, sanitizedTracer(telemetry.tracer))),
    ),
  );
}

/** {@link observabilityLayer} on the telemetry installed when the layer is built. */
export const ObservabilityLive: Layer.Layer<never> = Layer.unwrap(Effect.sync(() => observabilityLayer(currentTelemetry())));

/**
 * The message is the event name, which the record's logger checks against
 * its table; annotations are the record's fields. The level filter is the
 * record logger's own, so Effect's minimum level passes everything.
 */
function recordLogger(telemetry: Telemetry): Logger.Logger<unknown, void> {
  return Logger.make(({ message, logLevel, cause, fiber }) => {
    const level = levels[logLevel];
    if (level === undefined) return;
    const parts = Array.isArray(message) ? message : [message];
    const fields: Record<string, FieldValue> = {};
    for (const [key, value] of Object.entries(fiber.getRef(References.CurrentLogAnnotations))) {
      if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") fields[key] = value;
    }
    const event = typeof parts[0] === "string" ? parts[0] : undefined;
    fields.event = event;
    if (cause.reasons.length > 0 && !Cause.hasInterruptsOnly(cause)) Object.assign(fields, failureFields(telemetry, event, cause));
    const span = fiber.currentSpan;
    const ctx = span === undefined ? ROOT_CONTEXT : trace.setSpanContext(ROOT_CONTEXT, { traceId: span.traceId, spanId: span.spanId, traceFlags: span.sampled ? 1 : 0, isRemote: span._tag === "ExternalSpan" });
    runInContext(ctx, () => telemetry.record(level, fields));
  });
}

/** The declared tuple of the failure's errorCode for event, otherwise the generic one; never the failure's own text. */
function failureFields(telemetry: Telemetry, event: string | undefined, cause: Cause.Cause<unknown>): Record<string, string> {
  const code = errorCode(cause);
  const fixed = event === undefined || code === undefined ? undefined : lookupEvent(telemetry.declarations, event)?.errors?.[code];
  return fixed === undefined || code === undefined
    ? { "error.class": "operation_failed", "error.code": "operation_failed", "error.message_safe": "operation failed" }
    : { "error.class": fixed.class, "error.code": code, "error.message_safe": fixed.message_safe };
}

function errorCode(cause: Cause.Cause<unknown>): string | undefined {
  for (const reason of cause.reasons) {
    if (!Cause.isFailReason(reason)) continue;
    const error = reason.error;
    if (typeof error === "object" && error !== null && typeof (error as { errorCode?: unknown }).errorCode === "string") return (error as { errorCode: string }).errorCode;
  }
  return undefined;
}

/** A failed span carries error.class and error.code; @effect/opentelemetry's exception events and status text are dropped below. */
function failureAttributingTracer(base: Tracer.Tracer): Tracer.Tracer {
  return Tracer.make({
    span(options) {
      const span = base.span(options);
      const end = span.end.bind(span);
      Object.defineProperty(span, "end", {
        value: (endTime: bigint, exit: Parameters<Tracer.Span["end"]>[1]) => {
          if (exit._tag === "Failure" && !Cause.hasInterruptsOnly(exit.cause)) {
            span.attribute("error.class", "operation_failed");
            span.attribute("error.code", errorCode(exit.cause) ?? "operation_failed");
          }
          end(endTime, exit);
        },
      });
      return span;
    },
    ...(base.context === undefined ? {} : { context: base.context }),
  });
}

/**
 * An OpenTelemetry tracer whose spans keep only attributes the record field
 * rules admit and drop every event, recorded exception and status message,
 * so no log text or error text reaches a span.
 */
function sanitizedTracer(tracer: Otel.Tracer): Otel.Tracer {
  const startActiveSpan = (name: string, ...args: unknown[]): unknown => {
    const fn = args.pop() as (span: Otel.Span) => unknown;
    return (tracer.startActiveSpan as (name: string, ...rest: unknown[]) => unknown)(name, ...args, (span: Otel.Span) => fn(sanitizedSpan(span)));
  };
  return {
    startSpan: (name, options, ctx) => sanitizedSpan(tracer.startSpan(name, { ...options, attributes: spanAttributes(options?.attributes as Record<string, FieldValue> | undefined ?? {}) }, ctx)),
    startActiveSpan: startActiveSpan as Otel.Tracer["startActiveSpan"],
  };
}

function sanitizedSpan(span: Otel.Span): Otel.Span {
  const self: Otel.Span = {
    spanContext: () => span.spanContext(),
    setAttribute: (key, value) => {
      const safe = spanAttributes({ [key]: value as FieldValue });
      if (safe[key] !== undefined) span.setAttribute(key, safe[key]);
      return self;
    },
    setAttributes: (attributes) => { span.setAttributes(spanAttributes(attributes as Record<string, FieldValue>)); return self; },
    addEvent: () => self,
    addLink: (link) => { span.addLink(link); return self; },
    addLinks: (links) => { span.addLinks(links); return self; },
    setStatus: (status) => { span.setStatus({ code: status.code }); return self; },
    updateName: (name) => { span.updateName(name); return self; },
    end: (endTime) => { span.end(endTime); },
    isRecording: () => span.isRecording(),
    recordException: () => {},
  };
  return self;
}
