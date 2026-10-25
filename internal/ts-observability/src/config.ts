/** Validated, restart-required process diagnostic controls. */
export type LogLevel = "debug" | "info" | "warn" | "error";
export interface DiagnosticConfig {
  readonly level: LogLevel;
  readonly maxRecordBytes: number;
  readonly summaryIntervalMs: number;
  readonly burst: number;
  /** Root sampling ratio of the parent-based trace sampler, honoured exactly. */
  readonly traceSampleRatio: number;
  /** OTLP/gRPC collector; undefined exports no span. */
  readonly traceEndpoint?: string | undefined;
  readonly dbSlowQueryMs: number;
}
export const diagnosticDefaults: Readonly<DiagnosticConfig> = Object.freeze({
  level: "info", maxRecordBytes: 16_384, summaryIntervalMs: 30_000, burst: 1, traceSampleRatio: 1, dbSlowQueryMs: 250,
});
export const diagnosticEnvKeys = [
  "TETRAL_LOG_LEVEL", "TETRAL_LOG_MAX_RECORD_BYTES", "TETRAL_LOG_SUMMARY_INTERVAL_MS", "TETRAL_LOG_BURST",
  "TETRAL_TRACE_SAMPLE_RATIO", "TETRAL_DB_SLOW_QUERY_MS", "OTEL_EXPORTER_OTLP_ENDPOINT",
] as const;
const traceSampleRatioPattern = /^(0|1)(\.[0-9]+)?$/;

export type DiagnosticConfigResult =
  | { readonly ok: true; readonly config: DiagnosticConfig }
  /** problem names the variable and its accepted values, never the supplied value. */
  | { readonly ok: false; readonly problem: string };

/** Undefined/empty means default; a malformed recognized input fails startup without exposing its value. */
export function parseDiagnosticConfigResult(env: Readonly<Record<string, string | undefined>>): DiagnosticConfigResult {
  const fail = (problem: string): DiagnosticConfigResult => ({ ok: false, problem });
  const level = env.TETRAL_LOG_LEVEL || diagnosticDefaults.level;
  if (level !== "debug" && level !== "info" && level !== "warn" && level !== "error") return fail("TETRAL_LOG_LEVEL must be debug, info, warn, or error");
  const integer = (key: string, fallback: number, minimum: number, maximum: number): number | undefined => {
    const value = env[key];
    if (value === undefined || value === "") return fallback;
    if (!/^[1-9][0-9]*$/.test(value)) return undefined;
    const result = Number(value);
    return Number.isSafeInteger(result) && result >= minimum && result <= maximum ? result : undefined;
  };
  const maxRecordBytes = integer("TETRAL_LOG_MAX_RECORD_BYTES", diagnosticDefaults.maxRecordBytes, 1_024, 65_536);
  if (maxRecordBytes === undefined) return fail("TETRAL_LOG_MAX_RECORD_BYTES outside supported positive integer range");
  const burst = integer("TETRAL_LOG_BURST", diagnosticDefaults.burst, 1, 1_000);
  if (burst === undefined) return fail("TETRAL_LOG_BURST outside supported positive integer range");
  const summaryIntervalMs = integer("TETRAL_LOG_SUMMARY_INTERVAL_MS", diagnosticDefaults.summaryIntervalMs, 100, 3_600_000);
  if (summaryIntervalMs === undefined) return fail("TETRAL_LOG_SUMMARY_INTERVAL_MS must be 100 through 3600000 milliseconds");
  let traceSampleRatio = diagnosticDefaults.traceSampleRatio;
  const ratio = env.TETRAL_TRACE_SAMPLE_RATIO;
  if (ratio !== undefined && ratio !== "") {
    traceSampleRatio = Number(ratio);
    if (!traceSampleRatioPattern.test(ratio) || traceSampleRatio > 1) return fail("TETRAL_TRACE_SAMPLE_RATIO must be a decimal from 0 through 1");
  }
  const dbSlowQueryMs = integer("TETRAL_DB_SLOW_QUERY_MS", diagnosticDefaults.dbSlowQueryMs, 1, 60_000);
  if (dbSlowQueryMs === undefined) return fail("TETRAL_DB_SLOW_QUERY_MS must be 1 through 60000 milliseconds");
  const traceEndpoint = env.OTEL_EXPORTER_OTLP_ENDPOINT || undefined;
  if (traceEndpoint !== undefined && !httpURL(traceEndpoint)) return fail("OTEL_EXPORTER_OTLP_ENDPOINT must be an http or https URL");
  return { ok: true, config: Object.freeze({ level, maxRecordBytes, summaryIntervalMs, burst, traceSampleRatio, dbSlowQueryMs, ...(traceEndpoint === undefined ? {} : { traceEndpoint }) }) };
}

/** {@link parseDiagnosticConfigResult} for callers that report one generic configuration error. */
export function parseDiagnosticConfig(env: Readonly<Record<string, string | undefined>>): DiagnosticConfig | undefined {
  const result = parseDiagnosticConfigResult(env);
  return result.ok ? result.config : undefined;
}

function httpURL(value: string): boolean {
  try {
    const url = new URL(value);
    return (url.protocol === "http:" || url.protocol === "https:") && url.hostname !== "";
  } catch {
    return false;
  }
}
