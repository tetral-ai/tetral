import { context, trace } from "@opentelemetry/api";
import type { Context, Span } from "@opentelemetry/api";
import { contextFields, contextWithFields, runInContext } from "./context.js";
import type { Fields } from "./context.js";
import { lookupEvent, lookupOperation, UndeclaredNameError } from "./declarations.js";
import type { OperationDeclaration } from "./declarations.js";
import type { LogLevel } from "./config.js";
import { redactLogRecord } from "./fields.js";
import { Rejection } from "./pressure.js";
import type { Telemetry } from "./telemetry.js";
import { currentTelemetry } from "./telemetry.js";

/** An error that carries an error.code an operation may declare. */
export interface CodedError {
  readonly errorCode: string;
}

/** Starts operation on the installed telemetry; see {@link Telemetry.observe}. */
export function observe(operation: string, fields: Fields = {}): Observation {
  return currentTelemetry().observe(operation, fields);
}

export function startObservation(telemetry: Telemetry, operation: string, fields: Fields): Observation {
  let declared = lookupOperation(telemetry.declarations, operation);
  if (declared === undefined) {
    if (telemetry.undeclared === "throw") throw new UndeclaredNameError("operation", operation);
    telemetry.record("warn", { event: "diagnostic.invalid_event", component: "workload", reason: "undeclared_operation" });
    operation = "unknown_operation";
    declared = { long: false };
  }
  const parent = contextWithFields(context.active(), fields);
  const span = telemetry.tracer.startSpan(operation, { attributes: spanAttributes(contextFields(parent)) }, parent);
  return new Observation(telemetry, operation, declared, span, trace.setSpan(parent, span));
}

/** Correlation fields as span attributes, under the same field rules as records. */
export function spanAttributes(fields: Fields): Record<string, string | number | boolean> {
  const attributes: Record<string, string | number | boolean> = {};
  for (const [key, value] of Object.entries(redactLogRecord(fields))) if (value !== undefined) attributes[key] = value;
  return attributes;
}

/**
 * One running operation: its span, its start and the telemetry that records
 * its end. The component that handles a failure ends it with
 * {@link Observation.endHandled}; one that returns the failure to its caller
 * ends it with {@link Observation.end}.
 */
export class Observation {
  readonly #telemetry: Telemetry;
  readonly #operation: string;
  readonly #declared: OperationDeclaration;
  readonly #span: Span;
  readonly #started: number;
  #ended = false;

  constructor(telemetry: Telemetry, operation: string, declared: OperationDeclaration, span: Span, readonly context: Context) {
    this.#telemetry = telemetry;
    this.#operation = operation;
    this.#declared = declared;
    this.#span = span;
    this.#started = telemetry.monotonicClock();
  }

  /** Runs fn inside the operation's span: its records and child spans carry its trace. */
  run<T>(fn: () => T): T {
    return runInContext(this.context, fn);
  }

  /** Records the duration and outcome; writes no record. */
  end(error?: unknown): void { this.#end(error, false); }

  /** {@link Observation.end} that also writes the failure record `<operation>.failed`. */
  endHandled(error?: unknown): void { this.#end(error, true); }

  #end(error: unknown, handled: boolean): void {
    if (this.#ended) return;
    this.#ended = true;
    const elapsedMs = Math.max(0, this.#telemetry.monotonicClock() - this.#started);
    const { outcome, code } = this.#outcome(error);
    if (outcome !== "ok") this.#span.setAttributes({ "error.class": outcome === "error" ? "operation_failed" : outcome, "error.code": code });
    this.#span.end();
    const histogram = this.#declared.long ? this.#telemetry.longOperations : this.#telemetry.operations;
    histogram.observe(elapsedMs / 1000, this.#telemetry.service, this.#operation, outcome);
    if (handled && error !== undefined && error !== null && outcome !== "rejected") this.#writeFailure(code, elapsedMs);
  }

  #outcome(error: unknown): { readonly outcome: string; readonly code: string } {
    if (error === undefined || error === null) return { outcome: "ok", code: "" };
    if (error instanceof Rejection) return { outcome: "rejected", code: "rejected" };
    const code = typeof error === "object" && typeof (error as Partial<CodedError>).errorCode === "string" ? (error as CodedError).errorCode : undefined;
    if (code !== undefined && this.#declared.errors?.includes(code) === true) return { outcome: code, code };
    return { outcome: "error", code: "operation_failed" };
  }

  /** The failure record carries the error tuple its declaration fixes for code, never the error's own text. */
  #writeFailure(code: string, elapsedMs: number): void {
    const event = `${this.#operation}.failed`;
    const declared = lookupEvent(this.#telemetry.declarations, event);
    const fixed = declared?.errors?.[code];
    const level = (declared?.level.toLowerCase() ?? "error") as LogLevel;
    runInContext(this.context, () => this.#telemetry.record(level, {
      event,
      "error.class": fixed?.class ?? "operation_failed",
      "error.code": fixed === undefined ? "operation_failed" : code,
      "error.message_safe": fixed?.message_safe ?? "operation failed",
      duration_ms: Math.trunc(elapsedMs),
    }));
  }
}
