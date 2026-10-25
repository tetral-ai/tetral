import { lookupOperation } from "./declarations.js";
import type { Declarations } from "./declarations.js";
import { MetricRegistry, renderMetrics } from "./metric-registry.js";
import type { Histogram, MetricSample } from "./metric-registry.js";

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

export type OperationService = "provider-gateway" | "mcp-connector" | "agent-runtime";
/**
 * `success` is accepted from existing callers and recorded as `ok`. The
 * recorded domain is `observe`'s: `ok`, `rejected`, `error` or a code the
 * operation declares, so any other outcome counts as `error`.
 */
export type OperationOutcome = "ok" | "success" | "error" | "cancelled" | "rejected" | "timeout" | "duplicate" | "stale" | "failed";

/**
 * Duration histograms of one service's owned operations: an operation its
 * table marks long goes to `tetral_long_operation_duration_seconds` with the
 * long-operation buckets, every other one to
 * `tetral_operation_duration_seconds` with the request buckets. Each owning
 * constructor supplies its static operation domain; other inputs share
 * `unknown_method`.
 */
export class OperationMetricsRegistry {
  readonly #registry: MetricRegistry;
  readonly #request: Histogram;
  readonly #long: Histogram;
  readonly #operations: ReadonlySet<string>;
  readonly #declarations: Declarations | undefined;
  readonly #counts = new Map<string, number>();

  constructor(readonly service: OperationService, operations: readonly string[], declarations?: Declarations) {
    this.#operations = new Set(operations);
    this.#declarations = declarations;
    this.#registry = new MetricRegistry(declarations);
    this.#request = this.#registry.histogram("tetral_operation_duration_seconds", "Duration of a completed operation.", ["service", "operation", "outcome"]);
    this.#long = this.#registry.histogram("tetral_long_operation_duration_seconds", "Duration of a completed long operation.", ["service", "operation", "outcome"]);
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

  collect(): MetricSample[] { return this.#registry.collect(); }

  render(): string { return renderMetrics(this.collect()); }

  #record(operation: string, outcome: OperationOutcome, durationSeconds: number): { operation: string; outcome: string; count: number } | undefined {
    if (!Number.isFinite(durationSeconds) || durationSeconds < 0) return;
    const boundedOperation = this.#operations.has(operation) ? operation : "unknown_method";
    const declared = lookupOperation(this.#declarations, boundedOperation);
    const boundedOutcome = outcome === "success" ? "ok"
      : outcome === "ok" || outcome === "error" || outcome === "rejected" || declared?.errors?.includes(outcome) === true ? outcome : "error";
    const histogram = declared?.long === true ? this.#long : this.#request;
    histogram.observe(durationSeconds, this.service, boundedOperation, boundedOutcome);
    const key = `${boundedOperation}\0${boundedOutcome}`;
    const count = (this.#counts.get(key) ?? 0) + 1;
    this.#counts.set(key, count);
    return { operation: boundedOperation, outcome: boundedOutcome, count };
  }
}
