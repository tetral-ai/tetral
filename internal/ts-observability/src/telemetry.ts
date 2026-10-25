import type { Tracer } from "@opentelemetry/api";
import type { SpanExporter, SpanProcessor } from "@opentelemetry/sdk-trace-base";
import type { DiagnosticConfig, LogLevel } from "./config.js";
import { diagnosticDefaults } from "./config.js";
import { ensureContextManager } from "./context.js";
import type { Fields } from "./context.js";
import { defaultUndeclaredNamePolicy, UndeclaredNameError } from "./declarations.js";
import type { Declarations, UndeclaredNamePolicy } from "./declarations.js";
import type { TetralDiagnosticLogger, TetralJsonLogger, TetralLogRecord } from "./index.js";
import { processInstanceId } from "./instance.js";
import { longOperationDurationBuckets, MetricRegistry, renderMetrics, requestDurationBuckets } from "./metric-registry.js";
import type { Histogram, MetricSample } from "./metric-registry.js";
import { startObservation } from "./observe.js";
import type { Observation } from "./observe.js";
import type { OperationMetricsRegistry } from "./operation-metrics.js";
import { createTracing, otlpTraceExporter } from "./tracing.js";
import type { Tracing } from "./tracing.js";

/** The logger methods the library writes its own records through. */
type TelemetryLogger = TetralJsonLogger<TetralLogRecord> & Partial<Pick<TetralDiagnosticLogger<TetralLogRecord>, "debug" | "warn">>;

export interface TelemetryOptions<TRecord extends TetralLogRecord = TetralLogRecord> {
  readonly serviceName: string;
  readonly serviceVersion?: string | undefined;
  readonly deploymentEnvironment?: string | undefined;
  readonly instanceId?: string | undefined;
  /** Writes the library's records; without one they are discarded. */
  readonly logger?: TetralJsonLogger<TRecord> & Partial<Pick<TetralDiagnosticLogger<TRecord>, "debug" | "warn">> | undefined;
  /** The service's table, which declares its operations. */
  readonly declarations?: Declarations | undefined;
  /** Sample ratio and OTLP endpoint; the endpoint builds the exporter only when set. */
  readonly diagnostics?: Pick<DiagnosticConfig, "traceSampleRatio" | "traceEndpoint"> | undefined;
  /** Replaces the OTLP exporter; it receives sampled spans through the bounded queue. */
  readonly exporter?: SpanExporter | undefined;
  readonly spanProcessors?: readonly SpanProcessor[] | undefined;
  /** Milliseconds on the process's monotonic clock, whose zero is process start. */
  readonly monotonicClock?: (() => number) | undefined;
  readonly undeclared?: UndeclaredNamePolicy | undefined;
}

/**
 * One process's recording owner: its service, logger, tracer and the metric
 * families the library records for every service. A process creates one at
 * startup and installs it with {@link installTelemetry}.
 */
export class Telemetry {
  readonly service: string;
  readonly declarations: Declarations | undefined;
  readonly undeclared: UndeclaredNamePolicy;
  readonly registry = new MetricRegistry();
  readonly tracer: Tracer;
  readonly monotonicClock: () => number;
  readonly operations: Histogram;
  readonly longOperations: Histogram;
  readonly coldStartPhases: Histogram;
  readonly coldStarts: Histogram;
  readonly grpcRequests: Histogram;
  readonly #tracing: Tracing;
  readonly #logger: TelemetryLogger | undefined;

  constructor(options: TelemetryOptions) {
    ensureContextManager();
    this.service = options.serviceName;
    this.declarations = options.declarations;
    this.undeclared = options.undeclared ?? defaultUndeclaredNamePolicy();
    this.monotonicClock = options.monotonicClock ?? (() => performance.now());
    this.#logger = options.logger;
    const endpoint = options.diagnostics?.traceEndpoint;
    this.#tracing = createTracing({
      serviceName: options.serviceName,
      serviceVersion: options.serviceVersion ?? "unknown",
      deploymentEnvironment: options.deploymentEnvironment ?? "unknown",
      instanceId: options.instanceId ?? processInstanceId,
      sampleRatio: options.diagnostics?.traceSampleRatio ?? diagnosticDefaults.traceSampleRatio,
      exporter: options.exporter ?? (endpoint === undefined ? undefined : otlpTraceExporter(endpoint)),
      spanProcessors: options.spanProcessors,
    });
    this.tracer = this.#tracing.tracer;
    const labels = ["service", "operation", "outcome"];
    this.operations = this.registry.histogram("tetral_operation_duration_seconds", "Duration of a completed operation.", requestDurationBuckets, labels);
    this.longOperations = this.registry.histogram("tetral_long_operation_duration_seconds", "Duration of a completed long operation.", longOperationDurationBuckets, labels);
    this.coldStartPhases = this.registry.histogram("cold_start_phase_seconds", "Duration of one completed cold start phase.", longOperationDurationBuckets, ["service", "kind", "phase"]);
    this.coldStarts = this.registry.histogram("cold_start_seconds", "Duration of a completed cold start, in total and Engine's part.", longOperationDurationBuckets, ["service", "kind", "scope"]);
    this.grpcRequests = this.registry.histogram("grpc_server_request_duration_seconds", "Duration of one internal gRPC call.", requestDurationBuckets, ["service", "method", "code"]);
  }

  /** Starts operation, which the service's table must declare. */
  observe(operation: string, fields: Fields = {}): Observation {
    return startObservation(this, operation, fields);
  }

  /** Writes one of the library's records; only a missing declaration under test escapes. */
  record(level: LogLevel, record: Readonly<Record<string, string | number | boolean | undefined>>): void {
    try {
      const logger = this.#logger;
      if (logger === undefined) return;
      const typed = record as TetralLogRecord;
      if (level === "debug") logger.debug?.(typed);
      else if (level === "warn") (logger.warn ?? logger.error)(typed);
      else logger[level](typed);
    } catch (error) {
      if (error instanceof UndeclaredNameError) throw error;
    }
  }

  get droppedSpans(): number { return this.#tracing.processor?.dropped ?? 0; }

  /** The library's families and the span drop counter. */
  collect(): MetricSample[] {
    return [...this.registry.collect(), {
      name: "tetral_diagnostic_spans_dropped_total", family: "tetral_diagnostic_spans_dropped_total",
      help: "Spans refused because the export queue was full.", type: "counter", labels: [], value: this.droppedSpans,
    }];
  }

  /** Exports the spans still queued, within the budget; recording never waits past it. */
  async shutdown(budgetMs = 1_000): Promise<void> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    await Promise.race([
      this.#tracing.shutdown(),
      new Promise<void>((resolve) => { timer = setTimeout(resolve, budgetMs); }),
    ]);
    clearTimeout(timer);
  }
}

export function createTelemetry<TRecord extends TetralLogRecord>(options: TelemetryOptions<TRecord>): Telemetry {
  return new Telemetry(options as TelemetryOptions);
}

let installed: Telemetry | undefined;
let fallback: Telemetry | undefined;

/** Makes telemetry the process's recording owner and returns the restoration of the previous one. */
export function installTelemetry(telemetry: Telemetry): () => void {
  const previous = installed;
  installed = telemetry;
  return () => { installed = previous; };
}

/** The installed owner, or a quiet one that writes no record and exports no span. */
export function currentTelemetry(): Telemetry {
  return installed ?? (fallback ??= new Telemetry({ serviceName: "unknown" }));
}

/** A service's operation histograms with the process telemetry's families, one header per family. */
export function operationMetricsText(operations: OperationMetricsRegistry, telemetry: Telemetry = currentTelemetry()): string {
  return renderMetrics([...operations.collect(), ...telemetry.collect()]);
}
