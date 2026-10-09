import bucketBounds from "./operation-duration-buckets.json";

/** Minimal scalar record emitted at the completed owning shutdown boundary. */
export type ShutdownPhaseLogRecord = {
  readonly event: "workload.shutdown.phase_completed";
  readonly component: "workload";
  readonly operation: string;
  readonly outcome: string;
  readonly "duration.seconds": number;
  readonly "metric.observation.count": number;
};
export type ShutdownPhaseLogger = { readonly info: (record: ShutdownPhaseLogRecord) => void };

/** Fixed seconds buckets shared by public owning operations. Never label with IDs or tool names. */
export const operationDurationBuckets: readonly number[] = Object.freeze(bucketBounds);
export type OperationService = "provider-gateway" | "mcp-connector" | "agent-runtime";
export type OperationOutcome = "success" | "error" | "cancelled" | "rejected" | "timeout" | "committed" | "duplicate" | "stale" | "failed";
const outcomes = new Set<string>(["success", "error", "cancelled", "rejected", "timeout", "committed", "duplicate", "stale", "failed"]);
interface Observation { count: number; sum: number; buckets: number[] }

/** Each owning constructor supplies its static operation domain; unknown inputs share one fallback. */
export class OperationMetricsRegistry {
  readonly #observations = new Map<string, Observation>();
  readonly #operations: ReadonlySet<string>;
  constructor(readonly service: OperationService, operations: readonly string[]) {
    this.#operations = new Set(operations);
  }
  observe(operation: string, outcome: OperationOutcome, durationSeconds: number): void {
    this.#record(operation, outcome, durationSeconds);
  }
  /** The owning phase has completed; stderr can outlive the metrics listener. */
  observeShutdown(operation: string, outcome: OperationOutcome, durationSeconds: number, logger?: ShutdownPhaseLogger): void {
    const recorded = this.#record(operation, outcome, durationSeconds);
    if (recorded === undefined) return;
    try {
      logger?.info({ event: "workload.shutdown.phase_completed", component: "workload",
        operation: recorded.operation, outcome: recorded.outcome,
        "duration.seconds": durationSeconds, "metric.observation.count": recorded.count });
    } catch { /* Diagnostic delivery cannot change the owning shutdown result. */ }
  }
  #record(operation: string, outcome: OperationOutcome, durationSeconds: number): { operation: string; outcome: string; count: number } | undefined {
    if (!Number.isFinite(durationSeconds) || durationSeconds < 0) return;
    const boundedOperation = this.#operations.has(operation) ? operation : "unknown_method";
    const boundedOutcome = outcomes.has(outcome) ? outcome : "error";
    const key = `${boundedOperation}\0${boundedOutcome}`;
    const value = this.#observations.get(key) ?? { count: 0, sum: 0, buckets: operationDurationBuckets.map(() => 0) };
    value.count++;
    value.sum += durationSeconds;
    for (let i = 0; i < operationDurationBuckets.length; i++) {
      if (durationSeconds <= operationDurationBuckets[i]!) value.buckets[i] = value.buckets[i]! + 1;
    }
    this.#observations.set(key, value);
    return { operation: boundedOperation, outcome: boundedOutcome, count: value.count };
  }
  render(): string {
    const name = "tetral_operation_duration_seconds";
    const lines = [`# HELP ${name} Completed owning operation duration in seconds.`, `# TYPE ${name} histogram`];
    for (const [key, value] of [...this.#observations].sort(([a], [b]) => a.localeCompare(b))) {
      const [operation, outcome] = key.split("\0");
      const labels = `service="${this.service}",operation="${operation}",outcome="${outcome}"`;
      for (let i = 0; i < operationDurationBuckets.length; i++) lines.push(`${name}_bucket{${labels},le="${operationDurationBuckets[i]}"} ${value.buckets[i]}`);
      lines.push(`${name}_bucket{${labels},le="+Inf"} ${value.count}`, `${name}_count{${labels}} ${value.count}`, `${name}_sum{${labels}} ${value.sum}`);
    }
    return `${lines.join("\n")}\n`;
  }
}
