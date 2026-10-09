/** Validated, restart-required process diagnostic controls. */
export type LogLevel = "debug" | "info" | "warn" | "error";
export interface DiagnosticConfig {
  readonly level: LogLevel;
  readonly maxRecordBytes: number;
  readonly summaryIntervalMs: number;
  readonly burst: number;
}
export const diagnosticDefaults: Readonly<DiagnosticConfig> = Object.freeze({
  level: "info", maxRecordBytes: 16_384, summaryIntervalMs: 30_000, burst: 1,
});
export const diagnosticEnvKeys = ["TETRAL_LOG_LEVEL", "TETRAL_LOG_MAX_RECORD_BYTES", "TETRAL_LOG_SUMMARY_INTERVAL_MS", "TETRAL_LOG_BURST"] as const;
/** Undefined/empty means default; malformed recognized inputs fail startup without exposing values. */
export function parseDiagnosticConfig(env: Readonly<Record<string, string | undefined>>): DiagnosticConfig | undefined {
  const level = env.TETRAL_LOG_LEVEL || diagnosticDefaults.level;
  if (level !== "debug" && level !== "info" && level !== "warn" && level !== "error") return undefined;
  const integer = (key: string, fallback: number, minimum: number, maximum: number): number | undefined => {
    const value = env[key];
    if (value === undefined || value === "") return fallback;
    if (!/^[1-9][0-9]*$/.test(value)) return undefined;
    const result = Number(value);
    return Number.isSafeInteger(result) && result >= minimum && result <= maximum ? result : undefined;
  };
  const maxRecordBytes = integer("TETRAL_LOG_MAX_RECORD_BYTES", diagnosticDefaults.maxRecordBytes, 1_024, 65_536);
  const summaryIntervalMs = integer("TETRAL_LOG_SUMMARY_INTERVAL_MS", diagnosticDefaults.summaryIntervalMs, 100, 3_600_000);
  const burst = integer("TETRAL_LOG_BURST", diagnosticDefaults.burst, 1, 1_000);
  if (maxRecordBytes === undefined || summaryIntervalMs === undefined || burst === undefined) return undefined;
  return Object.freeze({ level, maxRecordBytes, summaryIntervalMs, burst });
}
