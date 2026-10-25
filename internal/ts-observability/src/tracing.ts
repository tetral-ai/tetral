import { context, trace, TraceFlags } from "@opentelemetry/api";
import type { Context, SpanContext, Tracer } from "@opentelemetry/api";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-grpc";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { BasicTracerProvider, ParentBasedSampler, TraceIdRatioBasedSampler } from "@opentelemetry/sdk-trace-base";
import type { ReadableSpan, SpanExporter, SpanProcessor } from "@opentelemetry/sdk-trace-base";
import { activeSpanContext } from "./context.js";

/** Bounds the spans waiting for export; a full queue refuses the span instead of holding the request that ended it. */
const spanQueueCapacity = 2048;
const spanExportBatch = 256;
const spanExportIntervalMs = 1_000;
const spanExportTimeoutMs = 10_000;

const traceparentPattern = /^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$/;
const zeroTraceId = "0".repeat(32);
const zeroSpanId = "0".repeat(16);

/** Formats a span context as a version 00 traceparent value, the only propagated header. */
export function formatTraceparent(spanContext: SpanContext): string {
  const sampled = (spanContext.traceFlags & TraceFlags.SAMPLED) === TraceFlags.SAMPLED;
  return `00-${spanContext.traceId}-${spanContext.spanId}-${sampled ? "01" : "00"}`;
}

/** Reads a version 00 traceparent; anything else, tracestate included, is ignored. */
export function parseTraceparent(value: unknown): SpanContext | undefined {
  if (typeof value !== "string") return undefined;
  const match = traceparentPattern.exec(value);
  if (match === null || match[1] === zeroTraceId || match[2] === zeroSpanId) return undefined;
  return { traceId: match[1]!, spanId: match[2]!, traceFlags: Number.parseInt(match[3]!, 16) & TraceFlags.SAMPLED, isRemote: true };
}

/** The traceparent of ctx's span, or undefined without one. */
export function activeTraceparent(ctx: Context = context.active()): string | undefined {
  const spanContext = activeSpanContext(ctx);
  return spanContext === undefined ? undefined : formatTraceparent(spanContext);
}

/** ctx continuing the caller's traceparent; an absent or invalid value leaves ctx unchanged. */
export function contextWithTraceparent(ctx: Context, value: unknown): Context {
  const caller = parseTraceparent(value);
  return caller === undefined ? ctx : trace.setSpanContext(ctx, caller);
}

/**
 * Hands ended sampled spans to one export loop through a fixed queue. The
 * loop runs on a timer, never in the request that ended the span.
 */
export class BoundedSpanProcessor implements SpanProcessor {
  readonly #exporter: SpanExporter;
  readonly #queue: ReadableSpan[] = [];
  readonly #timer: ReturnType<typeof setInterval> | undefined;
  #dropped = 0;
  #exporting: Promise<void> | undefined;
  #scheduled = false;
  #stopped = false;

  constructor(exporter: SpanExporter, options: { readonly start?: boolean } = {}) {
    this.#exporter = exporter;
    if (options.start !== false) {
      this.#timer = setInterval(() => void this.#drain(), spanExportIntervalMs);
      this.#timer.unref?.();
    }
  }

  get dropped(): number { return this.#dropped; }
  get queued(): number { return this.#queue.length; }

  onStart(): void {}

  onEnd(span: ReadableSpan): void {
    if ((span.spanContext().traceFlags & TraceFlags.SAMPLED) !== TraceFlags.SAMPLED) return;
    if (this.#stopped || this.#queue.length >= spanQueueCapacity) { this.#dropped++; return; }
    this.#queue.push(span);
    if (this.#timer !== undefined && this.#queue.length >= spanExportBatch && !this.#scheduled) {
      this.#scheduled = true;
      setImmediate(() => { this.#scheduled = false; void this.#drain(); });
    }
  }

  forceFlush(): Promise<void> { return this.#drain(); }

  async shutdown(): Promise<void> {
    if (this.#timer !== undefined) clearInterval(this.#timer);
    await this.#drain();
    this.#stopped = true;
    try { await this.#exporter.shutdown(); } catch { /* export is best effort */ }
  }

  #drain(): Promise<void> {
    this.#exporting ??= (async () => {
      try {
        while (this.#queue.length > 0) await this.#export(this.#queue.splice(0, spanExportBatch));
      } finally {
        this.#exporting = undefined;
      }
    })();
    return this.#exporting;
  }

  #export(batch: ReadableSpan[]): Promise<void> {
    return new Promise<void>((resolve) => {
      const timeout = setTimeout(resolve, spanExportTimeoutMs);
      timeout.unref?.();
      try {
        this.#exporter.export(batch, () => { clearTimeout(timeout); resolve(); });
      } catch {
        clearTimeout(timeout);
        resolve();
      }
    });
  }
}

export interface TracingOptions {
  readonly serviceName: string;
  readonly serviceVersion: string;
  readonly deploymentEnvironment: string;
  readonly instanceId: string;
  readonly sampleRatio: number;
  /** Receives sampled spans through the bounded queue; undefined keeps spans and trace IDs local. */
  readonly exporter?: SpanExporter | undefined;
  /** Further processors, which see every span synchronously; tests attach in-memory exporters here. */
  readonly spanProcessors?: readonly SpanProcessor[] | undefined;
}

export interface Tracing {
  readonly tracer: Tracer;
  readonly processor: BoundedSpanProcessor | undefined;
  readonly shutdown: () => Promise<void>;
}

export function createTracing(options: TracingOptions): Tracing {
  const processor = options.exporter === undefined ? undefined : new BoundedSpanProcessor(options.exporter);
  const provider = new BasicTracerProvider({
    resource: resourceFromAttributes({
      "service.name": options.serviceName,
      "service.version": options.serviceVersion,
      "service.instance.id": options.instanceId,
      "deployment.environment": options.deploymentEnvironment,
    }),
    sampler: new ParentBasedSampler({ root: new TraceIdRatioBasedSampler(options.sampleRatio) }),
    spanProcessors: [...(processor === undefined ? [] : [processor]), ...(options.spanProcessors ?? [])],
  });
  return {
    tracer: provider.getTracer("@tetral/ts-observability"),
    processor,
    shutdown: async () => { try { await provider.shutdown(); } catch { /* export is best effort */ } },
  };
}

/**
 * The OTLP/gRPC exporter for endpoint. It connects lazily: an unreachable
 * collector drops spans in the export loop and never delays startup or a
 * request.
 */
export function otlpTraceExporter(endpoint: string): SpanExporter | undefined {
  try {
    return new OTLPTraceExporter({ url: endpoint });
  } catch {
    return undefined;
  }
}
