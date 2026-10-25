import { context } from "@opentelemetry/api";
import { diagnosticDefaults } from "./config.js";
import type { DiagnosticConfig, LogLevel } from "./config.js";
import { activeSpanContext, contextFields } from "./context.js";
import { defaultUndeclaredNamePolicy, lookupEvent, UndeclaredNameError } from "./declarations.js";
import type { Declarations, UndeclaredNamePolicy } from "./declarations.js";
import { processInstanceId } from "./instance.js";
import { invalidEvent, redactLogRecord, safeDiagnosticEvent, safeResourceValue } from "./fields.js";
import type { TetralLogFieldValue } from "./fields.js";
/**
 * @packageDocumentation
 *
 * Provides the shared structured JSON logger used by the Runtime Pod, Provider
 * Gateway, and MCP Connector service logger modules, and the process
 * telemetry (tracing, operation observations, pressure points, cold starts)
 * those services record through. Callers specialize the record type and
 * supply a prompt-return sink. The logger bounds, filters, redacts and limits
 * records before serialization; sink failures are contained. Production
 * callers select standard error, while tests may inject an in-memory sink.
 * This package provides in-memory metrics primitives and opens no listener.
 *
 * Accepted records emit one JSON object followed by a newline. Debug is off by
 * default; repeated event/reason pairs emit an initial record and bounded
 * summaries. The logger-supplied `time`, `level`, `event`, `msg`, `log.type`,
 * `trace_id`, `span_id` and the configured or defaulted `service.name`,
 * `service.version`, `deployment.environment`, `service.instance.id`, and
 * `process.pid` fields overwrite fields with the same names in the caller's
 * record. A missing `instanceId` option defaults to one random identifier per
 * process, and missing version and environment options use `"unknown"`;
 * record fields whose value is `undefined` are omitted.
 *
 * Redaction is a syntactic, field-level safeguard: matching sensitive key names
 * or token-shaped string values replace the complete scalar value with
 * `"[REDACTED]"`. It is not a complete semantic-safety boundary and does not
 * prove that arbitrary messages or unmatched values are safe. Callers remain
 * responsible for supplying only approved, bounded operational fields and for
 * excluding raw secrets, payloads, content, dependency bodies, and stack
 * traces.
 */

export type { TetralLogFieldValue } from "./fields.js";
export { redactLogRecord } from "./fields.js";

/**
 * Defines the common structured fields and permits service-specific scalar
 * fields. An `undefined` value requests omission from the emitted record.
 */
type TetralLogRecordFields = {
  readonly event?: string;
  readonly kind?: string;
  readonly message?: string;
  readonly operation?: string;
  readonly "event.kind"?: string;
  readonly "recovery.event"?: string;
  readonly component?: string;
  readonly "duration.ms"?: number;
  readonly retryable?: boolean;
  readonly terminal?: boolean;
  readonly "request.id"?: string;
  readonly "workspace.id"?: string;
  readonly "session.id"?: string;
  readonly "thread.id"?: string;
  readonly "operation.id"?: string;
  readonly time?: string;
  readonly "job.id"?: string;
  readonly "queue.kind"?: string;
  readonly "binding.id"?: string;
  readonly "sandbox.id"?: string;
  readonly "cleanup.id"?: string;
  readonly "provider.request.id"?: string;
  readonly "service.name"?: string;
  readonly "service.version"?: string;
  readonly "deployment.environment"?: string;
  readonly [key: string]: TetralLogFieldValue;
};

type TetralSemanticErrorFields = {
  readonly [semanticErrorOutcome]: true;
  readonly "error.class": string;
  readonly "error.code": string;
  readonly "error.message_safe": string;
};

type TetralNonErrorFields = {
  readonly [semanticErrorOutcome]?: false | undefined;
  readonly "error.class"?: never;
  readonly "error.code"?: never;
  readonly "error.message_safe"?: never;
};

/** Marks a record as a semantic failure without adding a serialized log field. */
export const semanticErrorOutcome: unique symbol = Symbol("tetral.semantic-error-outcome");

/**
 * Requires semantic failure records to carry the complete shared error tuple,
 * independently of the severity method used to emit them.
 */
export type TetralLogRecord = TetralLogRecordFields & (TetralSemanticErrorFields | TetralNonErrorFields);

/** Builds the complete shared tuple and internal discriminator for a semantic failure record. */
export function semanticErrorFields(input: {
  readonly errorClass: string;
  readonly errorCode: string;
  readonly messageSafe: string;
}): TetralSemanticErrorFields {
  return {
    [semanticErrorOutcome]: true,
    "error.class": input.errorClass,
    "error.code": input.errorCode,
    "error.message_safe": input.messageSafe,
  };
}

/** The minimal methods consumers need; full process loggers also expose debug and warn. */
export interface TetralJsonLogger<TRecord extends TetralLogRecord = TetralLogRecord> {
  readonly info: (record: TRecord) => void;
  readonly error: (record: TRecord) => void;
}
export interface TetralDiagnosticLogger<TRecord extends TetralLogRecord = TetralLogRecord> extends TetralJsonLogger<TRecord> {
  readonly debug: (record: TRecord) => void;
  readonly warn: (record: TRecord) => void;
  /** Flush bounded suppression summaries; sink failures remain best effort. */
  readonly flush: () => void;
  readonly close: () => void;
  readonly stats: () => Readonly<DiagnosticStats>;
}
export interface DiagnosticStats {
  readonly emitted: number;
  readonly filtered: number;
  readonly suppressed: number;
  readonly dropped: number;
  readonly sinkFailures: number;
  readonly limiterEntries: number;
}
export interface TetralJsonLoggerOptions {
  /** Must return promptly. False indicates backpressure/drop; exceptions are contained. */
  readonly write: (line: string) => unknown;
  readonly serviceName: string;
  /** Optional nonrecursive production sink failure counter, including asynchronous errors. */
  readonly sinkFailures?: (() => number) | undefined;
  readonly deploymentEnvironment?: string | undefined;
  readonly serviceVersion?: string | undefined;
  readonly instanceId?: string | undefined;
  readonly clock?: (() => Date) | undefined;
  /** Monotonic duration clock, independent of UTC timestamps. */
  readonly monotonicClock?: (() => number) | undefined;
  readonly diagnostics?: DiagnosticConfig | undefined;
  /** The service's table; with one, an event that neither it nor the library table declares is refused. */
  readonly declarations?: Declarations | undefined;
  readonly undeclared?: UndeclaredNamePolicy | undefined;
}
export { diagnosticDefaults, diagnosticEnvKeys, parseDiagnosticConfig, parseDiagnosticConfigResult } from "./config.js";
export type { DiagnosticConfig, DiagnosticConfigResult, LogLevel } from "./config.js";
export { createDiagnosticStreamSink } from "./sink.js";
export type { DiagnosticStream } from "./sink.js";

const levels: Readonly<Record<LogLevel, number>> = { debug: 10, info: 20, warn: 30, error: 40 };
const recordLevels: Readonly<Record<LogLevel, string>> = { debug: "DEBUG", info: "INFO", warn: "WARN", error: "ERROR" };
const maxLimiterEntries = 256;
interface Window {
  started: number; first: string; last: string; emitted: number; suppressed: number;
  level: LogLevel; event: string; reason: string;
  sample: Record<string, TetralLogFieldValue>;
}
/** Safe bounded records and repeated-event state; diagnostic loss never changes business outcomes. */
export function createTetralJsonLogger<TRecord extends TetralLogRecord = TetralLogRecord>(
  options: TetralJsonLoggerOptions,
): TetralDiagnosticLogger<TRecord> {
  const cfg = options.diagnostics ?? diagnosticDefaults;
  const policy = options.undeclared ?? defaultUndeclaredNamePolicy();
  const base = {
    "service.name": safeResourceValue(options.serviceName),
    "deployment.environment": safeResourceValue(options.deploymentEnvironment ?? "unknown"),
    "service.version": safeResourceValue(options.serviceVersion ?? "unknown"),
    "service.instance.id": safeResourceValue(options.instanceId ?? processInstanceId),
    "process.pid": process.pid,
  };
  const now = (): string => recordTime(options.clock);
  const windows = new Map<string, Window>();
  const counts = { emitted: 0, filtered: 0, suppressed: 0, dropped: 0, sinkFailures: 0 };
  const summarySample = (record: Record<string, TetralLogFieldValue>, semanticFailure: boolean): Record<string, TetralLogFieldValue> => {
    const sample: Record<string, TetralLogFieldValue> = {};
    for (const field of ["workspace.id", "session.id", "thread.id", "request.id", "operation.id", "phase", "component"]) {
      if (record[field] !== undefined) sample[field] = record[field];
    }
    if (semanticFailure) {
      for (const field of ["error.class", "error.code", "error.message_safe"]) {
        if (record[field] !== undefined) sample[field] = record[field];
      }
    }
    return sample;
  };
  const write = (level: LogLevel, event: string, fields: Record<string, TetralLogFieldValue>, time: string): void => {
    try {
      const line = `${JSON.stringify({
        time, level: recordLevels[level], event, msg: event, ...fields, "log.type": lookupEvent(options.declarations, event)?.log_type ?? "app", ...base,
      })}\n`;
      if (Buffer.byteLength(line, "utf8") > cfg.maxRecordBytes) { counts.dropped++; return; }
      if (options.write(line) === false) { counts.dropped++; return; }
      counts.emitted++;
    } catch { counts.sinkFailures++; counts.dropped++; }
  };
  const summarize = (window: Window, time: string): void => {
    if (window.suppressed === 0) return;
    write(window.level, "diagnostic.suppressed", {
      operation: "diagnostic.limiter",
      "event.original": window.event, reason: window.reason, ...window.sample,
      "suppressed.count": window.suppressed, "first_seen": window.first, "last_seen": window.last,
    }, time);
  };
  const emit = (level: LogLevel, record: TRecord): void => {
    if (levels[level] < levels[cfg.level]) { counts.filtered++; return; }
    try {
      const time = now();
      const active = context.active();
      const fields = redactLogRecord({ ...contextFields(active), ...record });
      const spanContext = activeSpanContext(active);
      if (spanContext !== undefined) { fields.trace_id = spanContext.traceId; fields.span_id = spanContext.spanId; }
      let event = recordEvent(fields);
      if (event !== invalidEvent && options.declarations !== undefined && lookupEvent(options.declarations, event) === undefined) {
        if (policy === "throw") {
          const error = new UndeclaredNameError("event", event);
          // A caller that contains logging failures cannot hide the missing declaration from the test run.
          queueMicrotask(() => { throw error; });
          throw error;
        }
        fields["event.original"] = event;
        event = invalidEvent;
      }
      delete fields.event;
      fields.operation ??= event.split(".")[0];
      if (level === "error" || fields["error.class"] !== undefined || fields["error.code"] !== undefined || fields["error.message_safe"] !== undefined) {
        fields["error.class"] ??= "operation_failed";
        fields["error.code"] ??= "operation_failed";
        fields["error.message_safe"] ??= "operation failed";
      }
      const semanticFailure = record[semanticErrorOutcome] === true || fields["error.class"] !== undefined;
      const monotonicNow = options.monotonicClock?.() ?? performance.now();
      if (typeof fields["recovery.event"] === "string") {
        for (const [key, window] of windows) { if (window.event === fields["recovery.event"]) { summarize(window, time); windows.delete(key); } }
      }
      const reason = event === "pressure.rejecting_started" || event === "pressure.rejecting_ended"
        ? `${String(fields["pressure.point"])}\0${String(fields["pressure.cause"])}`
        : String(fields["error.code"] ?? fields.reason ?? fields.outcome ?? "none").slice(0, 128);
      if (level === "info" && !semanticFailure && record["diagnostic.repeat"] !== true) { write(level, event, fields, time); return; }
      if (level === "debug") { write(level, event, fields, time); return; }
      const key = `${event}\0${reason}`;
      let window = windows.get(key);
      if (window !== undefined && monotonicNow - window.started >= cfg.summaryIntervalMs) {
        summarize(window, time); windows.delete(key); window = undefined;
      }
      if (window === undefined) {
        for (const [oldKey, old] of windows) {
          if (monotonicNow - old.started >= cfg.summaryIntervalMs) { summarize(old, time); windows.delete(oldKey); }
        }
        if (windows.size >= maxLimiterEntries) { counts.dropped++; return; }
        const sample = summarySample(fields, semanticFailure);
        window = { started: monotonicNow, first: time, last: time, emitted: 0, suppressed: 0, level, event, reason, sample };
        windows.set(key, window);
      }
      if (window.sample["error.class"] === undefined && semanticFailure) {
        window.sample = summarySample(fields, true);
      }
      window.last = time;
      if (levels[level] > levels[window.level]) { window.level = level; window.emitted = 0; }
      if (window.emitted >= cfg.burst) { window.suppressed++; counts.suppressed++; return; }
      window.emitted++; write(level, event, fields, time);
    } catch (error) {
      if (error instanceof UndeclaredNameError) throw error;
      counts.dropped++;
    }
  };
  const timer = setInterval(() => {
    try { const monotonicNow = options.monotonicClock?.() ?? performance.now(); const time = now();
      for (const [key, window] of windows) { if (monotonicNow - window.started >= cfg.summaryIntervalMs) { summarize(window, time); windows.delete(key); } }
    } catch { counts.dropped++; }
  }, cfg.summaryIntervalMs);
  timer.unref();
  return {
    close: () => { clearInterval(timer); },
    debug: (record) => emit("debug", record), info: (record) => emit("info", record),
    warn: (record) => emit("warn", record), error: (record) => emit("error", record),
    flush: () => {
      clearInterval(timer);
      try { const time = now(); for (const window of windows.values()) summarize(window, time); }
      catch { counts.dropped++; }
      windows.clear();
    },
    stats: () => {
      let sinkFailures = counts.sinkFailures;
      try { const failures = options.sinkFailures?.() ?? 0; if (Number.isSafeInteger(failures) && failures >= 0) sinkFailures += failures; } catch { /* diagnostics must remain best effort */ }
      return { ...counts, sinkFailures, limiterEntries: windows.size };
    },
  };
}

/** RFC 3339 UTC with exactly six fractional digits, from a monotonic-anchored clock unless one is injected. */
function recordTime(clock: (() => Date) | undefined): string {
  const micros = clock === undefined ? Math.floor((performance.timeOrigin + performance.now()) * 1000) : clock().getTime() * 1000;
  const iso = new Date(Math.floor(micros / 1000)).toISOString();
  return `${iso.slice(0, -1)}${String(micros % 1000).padStart(3, "0")}Z`;
}

function recordEvent(fields: Readonly<Record<string, TetralLogFieldValue>>): string {
  for (const candidate of [fields.event, fields["event.kind"]]) {
    const event = safeDiagnosticEvent(candidate);
    if (event !== invalidEvent) return event;
  }
  return invalidEvent;
}

/** Fixed series, no request/workspace/credential labels; independent of the log sink. */
export function diagnosticMetricsText<TRecord extends TetralLogRecord>(logger: TetralJsonLogger<TRecord>): string {
  if (!("stats" in logger) || typeof logger.stats !== "function") return "";
  try {
    const stats = logger.stats() as DiagnosticStats;
    return ([
      ["emitted", stats.emitted], ["dropped", stats.dropped], ["sink_failures", stats.sinkFailures],
      ["suppressed", stats.suppressed], ["filtered", stats.filtered],
    ] as const).map(([name, value]) => `# TYPE tetral_diagnostic_${name}_total counter\ntetral_diagnostic_${name}_total ${value}\n`).join("") +
      `# TYPE tetral_diagnostic_limiter_entries gauge\ntetral_diagnostic_limiter_entries ${stats.limiterEntries}\n`;
  } catch { return ""; }
}

export { workloadResourceEnvKeys, parseWorkloadResourceConfig } from "./resource-config.js";
export type { WorkloadResourceConfig } from "./resource-config.js";
export { processFailureLogRecord, registerProcessSignalHandlers, runProcessEntry } from "./process-boundary.js";
export type { ProcessFailurePhase } from "./process-boundary.js";
export { processShutdownFailureLogRecord } from "./process-boundary.js";
export type { ExecutableProcessBoundary } from "./process-boundary.js";
export { OperationMetricsRegistry } from "./operation-metrics.js";
export type { OperationService, OperationOutcome, ShutdownPhaseLogRecord, ShutdownPhaseLogger } from "./operation-metrics.js";
export { libraryDeclarations, parseDeclarations, serviceDeclarations, UndeclaredNameError } from "./declarations.js";
export type { Declarations, EventDeclaration, MetricDeclaration, OperationDeclaration, SpanDeclaration, UndeclaredNamePolicy } from "./declarations.js";
export { longOperationDurationBuckets, MetricRegistry, renderMetrics, requestDurationBuckets } from "./metric-registry.js";
export type { Counter, Gauge, Histogram, MetricSample } from "./metric-registry.js";
export { contextFields, withFields } from "./context.js";
export type { Fields } from "./context.js";
export { activeTraceparent, formatTraceparent, parseTraceparent, BoundedSpanProcessor } from "./tracing.js";
export { createTelemetry, currentTelemetry, installTelemetry, operationMetricsText, Telemetry } from "./telemetry.js";
export type { TelemetryOptions } from "./telemetry.js";
export { observe, Observation } from "./observe.js";
export type { CodedError } from "./observe.js";
export { PressurePoint, Rejection } from "./pressure.js";
export { beginColdStart, beginServiceColdStart, ColdStart, completeServiceColdStart, endServiceColdStartPhase, failServiceColdStart, failServiceColdStartAtConfig } from "./cold-start.js";
export { continueGrpcTraces, injectTraceparent, traceparentClientInterceptor } from "./grpc.js";
