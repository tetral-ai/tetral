import { InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";
import type { ReadableSpan } from "@opentelemetry/sdk-trace-base";
import { createTelemetry, createTetralJsonLogger, diagnosticDefaults, installTelemetry } from "../src/index.js";
import type { Declarations, TetralDiagnosticLogger, Telemetry, UndeclaredNamePolicy } from "../src/index.js";

/**
 * @packageDocumentation
 *
 * Test-only telemetry whose records and ended spans stay in memory. The
 * package entry does not export it.
 */

export interface RecordingTelemetry {
  readonly telemetry: Telemetry;
  readonly logger: TetralDiagnosticLogger;
  /** Every record written so far, parsed. */
  readonly records: () => Record<string, unknown>[];
  /** Every sampled span ended so far. */
  readonly spans: () => readonly ReadableSpan[];
  /** Restores the telemetry installed before this one. */
  readonly restore: () => void;
}

export function recordingTelemetry(options: {
  readonly serviceName: string;
  readonly declarations?: Declarations;
  readonly undeclared?: UndeclaredNamePolicy;
  readonly monotonicClock?: () => number;
  readonly traceSampleRatio?: number;
}): RecordingTelemetry {
  const lines: string[] = [];
  const exporter = new InMemorySpanExporter();
  const logger = createTetralJsonLogger({
    write: (line) => { lines.push(line); return true; },
    serviceName: options.serviceName,
    serviceVersion: "test-version",
    deploymentEnvironment: "test",
    diagnostics: { ...diagnosticDefaults, level: "debug", burst: 1_000 },
    declarations: options.declarations,
    undeclared: options.undeclared,
  });
  const telemetry = createTelemetry({
    serviceName: options.serviceName,
    serviceVersion: "test-version",
    deploymentEnvironment: "test",
    logger,
    declarations: options.declarations,
    undeclared: options.undeclared,
    monotonicClock: options.monotonicClock,
    diagnostics: { traceSampleRatio: options.traceSampleRatio ?? 1 },
    spanProcessors: [new SimpleSpanProcessor(exporter)],
  });
  const restore = installTelemetry(telemetry);
  return {
    telemetry,
    logger,
    records: () => lines.map((line) => JSON.parse(line) as Record<string, unknown>),
    spans: () => exporter.getFinishedSpans(),
    restore: () => { restore(); logger.close(); },
  };
}
