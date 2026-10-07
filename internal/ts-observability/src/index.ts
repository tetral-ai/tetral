import approvedFieldNames from "./fields.json";
const approvedDiagnosticFields = new Set<string>(approvedFieldNames);
/**
 * @packageDocumentation
 *
 * Provides the shared structured JSON logger used by the Runtime Pod, Provider
 * Gateway, and MCP Connector service logger modules. Those callers specialize
 * the record type and supply a prompt-return sink. The logger bounds, filters,
 * redacts and limits records before serialization; sink failures are contained. Production callers currently select standard error, while tests may
 * inject an in-memory sink. This package does not collect metrics or open a
 * listener.
 *
 * Accepted records emit one JSON object followed by a newline. Debug is off by
 * default; repeated event/reason pairs emit an initial record and bounded summaries. The
 * logger-supplied `level`, UTC `time`, and the configured or defaulted
 * `service.name`, `service.version`, `deployment.environment`,
 * `service.instance.id`, and `process.pid` fields overwrite fields with the
 * same names in the caller's record. A missing `instanceId` option defaults to
 * one random UUID per process, and missing version and environment options use
 * `"unknown"`; record fields whose value is `undefined` are omitted.
 *
 * Redaction is a syntactic, field-level safeguard: matching sensitive key names
 * or token-shaped string values replace the complete scalar value with
 * `"[REDACTED]"`. It is not a complete semantic-safety boundary and does not
 * prove that arbitrary messages or unmatched values are safe. Callers remain
 * responsible for supplying only approved, bounded operational fields and for
 * excluding raw secrets, payloads, content, dependency bodies, and stack
 * traces.
 */

/** Scalar field values accepted by the shared structured log record. */
export type TetralLogFieldValue = string | number | boolean | undefined;

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
  readonly "trace.id"?: string;
  readonly "span.id"?: string;
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
}
import { diagnosticDefaults } from "./config.js";
import type { DiagnosticConfig, LogLevel } from "./config.js";
export { diagnosticDefaults, diagnosticEnvKeys, parseDiagnosticConfig } from "./config.js";
export type { DiagnosticConfig, LogLevel } from "./config.js";
export { createDiagnosticStreamSink } from "./sink.js";
export type { DiagnosticStream } from "./sink.js";

const processInstanceId = crypto.randomUUID();
const levels: Readonly<Record<LogLevel, number>> = { debug: 10, info: 20, warn: 30, error: 40 };
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
  const base = {
    "service.name": safeResourceValue(options.serviceName),
    "deployment.environment": safeResourceValue(options.deploymentEnvironment ?? "unknown"),
    "service.version": safeResourceValue(options.serviceVersion ?? "unknown"),
    "service.instance.id": safeResourceValue(options.instanceId ?? processInstanceId),
    "process.pid": process.pid,
  };
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
  const write = (level: LogLevel, record: Record<string, TetralLogFieldValue>, time: string): void => {
    try {
      const bounded = redactLogRecord(record as TRecord);
      if (typeof bounded.event === "string") bounded.event = safeDiagnosticEvent(bounded.event);
      const line = `${JSON.stringify({ ...bounded, time, level, ...base })}\n`;
      if (new TextEncoder().encode(line).byteLength > cfg.maxRecordBytes) { counts.dropped++; return; }
      if (options.write(line) === false) { counts.dropped++; return; }
      counts.emitted++;
    } catch { counts.sinkFailures++; counts.dropped++; }
  };
  const summarize = (window: Window, time: string): void => {
    if (window.suppressed === 0) return;
    write(window.level, {
      event: "diagnostic.suppressed", operation: "diagnostic.limiter",
      "event.original": window.event, reason: window.reason, ...window.sample,
      "suppressed.count": window.suppressed, "first_seen": window.first, "last_seen": window.last,
    }, time);
  };
  const emit = (level: LogLevel, record: TRecord): void => {
    if (levels[level] < levels[cfg.level]) { counts.filtered++; return; }
    try {
      const time = (options.clock?.() ?? new Date()).toISOString();
      const boundedRecord = redactLogRecord(record);
      const now = options.monotonicClock?.() ?? performance.now();
      if (typeof boundedRecord["recovery.event"] === "string") {
        for (const [key, window] of windows) { if (window.event === boundedRecord["recovery.event"]) { summarize(window, time); windows.delete(key); } }
      }
      const event = safeDiagnosticEvent(boundedRecord.event ?? boundedRecord["event.kind"] ?? "diagnostic.unknown");
      const reason = String(boundedRecord["error.code"] ?? boundedRecord.reason ?? boundedRecord.outcome ?? "none").slice(0, 128);
      if (level === "info" && record[semanticErrorOutcome] !== true && record["diagnostic.repeat"] !== true) { write(level, record, time); return; }
      if (level === "debug") { write(level, record, time); return; }
      const key = `${event}\0${reason}`;
      let window = windows.get(key);
      if (window !== undefined && now - window.started >= cfg.summaryIntervalMs) {
        summarize(window, time); windows.delete(key); window = undefined;
      }
      if (window === undefined) {
        for (const [oldKey, old] of windows) {
          if (now - old.started >= cfg.summaryIntervalMs) { summarize(old, time); windows.delete(oldKey); }
        }
        if (windows.size >= maxLimiterEntries) { counts.dropped++; return; }
        const sample = summarySample(boundedRecord, record[semanticErrorOutcome] === true);
        window = { started: now, first: time, last: time, emitted: 0, suppressed: 0, level, event, reason, sample };
        windows.set(key, window);
      }
      if (window.sample["error.class"] === undefined && record[semanticErrorOutcome] === true) {
        window.sample = summarySample(boundedRecord, true);
      }
      window.last = time;
      if (levels[level] > levels[window.level]) { window.level = level; window.emitted = 0; }
      if (window.emitted >= cfg.burst) { window.suppressed++; counts.suppressed++; return; }
      window.emitted++; write(level, record, time);
    } catch { counts.dropped++; }
  };
  const timer = setInterval(() => {
    try { const now = options.monotonicClock?.() ?? performance.now(); const time = (options.clock?.() ?? new Date()).toISOString();
      for (const [key, window] of windows) { if (now - window.started >= cfg.summaryIntervalMs) { summarize(window, time); windows.delete(key); } }
    } catch { counts.dropped++; }
  }, cfg.summaryIntervalMs);
  timer.unref();
  return {
    close: () => { clearInterval(timer); },
    debug: (record) => emit("debug", record), info: (record) => emit("info", record),
    warn: (record) => emit("warn", record), error: (record) => emit("error", record),
    flush: () => {
      clearInterval(timer);
      try { const time = (options.clock?.() ?? new Date()).toISOString(); for (const window of windows.values()) summarize(window, time); }
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

const redacted = "[REDACTED]";
const sensitiveKeyPattern = /(?:^|[._-])(?:authorization|api[_-]?key|access[_-]?token|refresh[_-]?token|session[_-]?token|secret|password|engine[_-]?vault[_-]?key)(?:[._-]|$)|^(?:authorizationToken|apiKey|accessToken|refreshToken|sessionToken|clientSecret|secretAccessKey|engineVaultKey)$/i;
const contentKeyPattern = /(?:^|[._-])(?:body|content|prompt|reasoning|arguments|payload|stack|headers|sql|exception|provider_response|tool_result)(?:[._-]|$)/i;
const sensitiveValuePattern = /\bBearer\s+\S+|(?:sk|ant|ghp|github)[-_][A-Za-z0-9._-]{8,}|(?:access_token|refresh_token|client_secret|accessToken|refreshToken|clientSecret|token)=/i;

function redactLogRecord<TRecord extends TetralLogRecord>(record: TRecord): Record<string, TetralLogFieldValue> {
  const next: Record<string, TetralLogFieldValue> = {};
  let fields = 0;
  for (const key in record) {
    if (!Object.hasOwn(record, key)) continue;
    const value = record[key];
    if (++fields > 64) break;
    if (key.length > 128) continue;
    if (typeof value !== "string" && typeof value !== "number" && typeof value !== "boolean") continue;
    if (typeof value === "number" && !Number.isFinite(value)) continue;
    if (value === undefined) {
      continue;
    }
    if (sensitiveKeyPattern.test(key) || (contentKeyPattern.test(key) && !(approvedDiagnosticFields.has(key) && typeof value === "number" && key.endsWith("_count")))) {
      next[key] = redacted;
      continue;
    }
    if (typeof value === "string" && value.length > 1024) { if (approvedDiagnosticFields.has(key)) next[key] = "[TRUNCATED]"; continue; }
    if (typeof value === "string" && (sensitiveValuePattern.test(value) || /https?:\/\//i.test(value))) {
      next[key] = redacted;
      continue;
    }
    if (!approvedDiagnosticFields.has(key)) continue;
    next[key] = typeof value === "string" ? value.replace(/[\x00-\x1f\x7f]/g, " ").slice(0, 1024) : value;
  }
  return next;
}

function safeDiagnosticEvent(event: unknown): string {
  return typeof event === "string" && /^[a-z][a-z0-9_.-]{0,127}$/.test(event) && !sensitiveValuePattern.test(event) ? event : "diagnostic.invalid_event";
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

function safeResourceValue(value: string): string {
  if (value.length > 253) return "[TRUNCATED]";
  if (sensitiveValuePattern.test(value) || /https?:\/\//i.test(value)) return redacted;
  return value.replace(/[\x00-\x1f\x7f]/g, " ");
}

export { processFailureLogRecord, registerProcessSignalHandlers, runProcessEntry } from "./process-boundary.js";
export type { ProcessFailurePhase } from "./process-boundary.js";
