import { longOperationDurationBuckets, MetricRegistry, OperationMetricsRegistry, operationMetricsText, PressurePoint, renderMetrics } from "@tetral/ts-observability";
import type { Counter, Gauge, Histogram, OperationOutcome } from "@tetral/ts-observability";
import type { ProviderAssemblyResources } from "./providers/block-assembler.js";
import { providerGatewayDeclarations } from "./logger.js";
/**
 * @packageDocumentation
 *
 * Owns provider-gateway's process-local metrics and their Prometheus text
 * rendering: the turn admission series, the per-stream model speed and
 * token families, provider-stream counters, content-assembly gauges and the
 * operation histograms. The service shell reports each admitted, released
 * and refused turn before catalog, attachment, credential, or provider-stream
 * setup and observes each provider stream once when it ends. The operations
 * listener renders the registry with live readiness and process-memory gauges.
 * Completion callbacks are idempotent, values are clamped to non-negative
 * output, and the registry retains no request, provider, or credential content.
 */

/** Tokens per second; a rate is a speed, so neither duration bucket set applies. */
const outputTokenRateBuckets: readonly number[] = Object.freeze([1, 5, 10, 20, 40, 80, 160, 320]);

/** Aggregates process-local admitted-turn observations for operations exposition. */
export class ProviderGatewayMetricsRegistry {
  readonly operations = new OperationMetricsRegistry("provider-gateway", ["StreamProviderRequest", "provider_first_fragment", "provider_first_complete", "complete_frame_write", "shutdown_drain", "shutdown_cancel_join"], providerGatewayDeclarations);
  /** The Gateway's own families; label values are catalogue ids and fixed sets. */
  readonly registry = new MetricRegistry();
  readonly #turnsInProgress: Gauge = this.registry.gauge("providergateway_turns_in_progress", "Provider turns admitted and not yet finished.");
  readonly #turnsLimit: Gauge = this.registry.gauge("providergateway_turns_limit", "Configured concurrent provider turn limit.");
  /** Refusals count in providergateway_turns_rejected_total and record their episodes. */
  readonly #turns = new PressurePoint(this.registry, "providergateway_turns", ["capacity"]);
  readonly #timeToFirstToken: Histogram = this.registry.histogram("providergateway_time_to_first_token_seconds", "Provider dispatch to the stream's first content fragment.", longOperationDurationBuckets, ["provider", "model", "first_kind"]);
  readonly #outputTokensPerSecond: Histogram = this.registry.histogram("providergateway_output_tokens_per_second", "Provider-reported output tokens over the first to last content fragment.", outputTokenRateBuckets, ["provider", "model"]);
  readonly #streamDuration: Histogram = this.registry.histogram("providergateway_stream_duration_seconds", "Provider dispatch to the end of its stream.", longOperationDurationBuckets, ["provider", "model", "outcome"]);
  readonly #tokens: Counter = this.registry.counter("providergateway_tokens_total", "Provider-reported tokens; cached is the part of input read from the provider's cache.", ["provider", "model", "type"]);
  readonly #providerRejected: Counter = this.registry.counter("providergateway_provider_rejected_total", "Provider attempts the provider refused, by cause.", ["provider", "cause"]);

  constructor() {
    this.#turnsInProgress.set(0);
    // The point's counter is the same family; a zero series lets rate() see the first refusal.
    this.registry.counter("providergateway_turns_rejected_total", "Work refused at this point, by cause.", ["cause"]).add(0, "capacity");
  }

  setTurnLimit(limit: number): void { this.#turnsLimit.set(Math.max(0, limit)); }
  /** One admitted turn, which ends the rejection episodes quiet for 60 seconds, and the turns now in flight. */
  turnAdmitted(inFlight: number): void { this.#turns.admit(); this.#turnsInProgress.set(inFlight); }
  turnReleased(inFlight: number): void { this.#turnsInProgress.set(inFlight); }
  turnRejected(): void { this.#turns.reject("capacity"); }

  observeRequest(outcome: OperationOutcome, durationSeconds: number): void { this.operations.observe("StreamProviderRequest", outcome, durationSeconds); }
  #assemblyResources: ProviderAssemblyResources = {retainedBytes:0,cumulativeContentBytes:0,segments:0,openBlocks:0,identities:0};
  #pendingFrameBytes = 0;
  /** Stage durations are exported only through the shared operation histogram. */
  observeProviderStage(sample: ProviderStageSample): void {
    this.operations.observe(sample.stage, sample.outcome, sample.durationMs / 1000);
  }

  /**
   * Observes one ended provider stream. Each sample is measured on its own,
   * so a value that cannot be measured drops only that sample.
   */
  observeProviderStream(stream: ProviderStreamSample): void {
    const { provider, model } = stream;
    const first = stream.firstFragment;
    if (first !== undefined) {
      try { this.#timeToFirstToken.observe((first.at - stream.dispatchedAt) / 1000, provider, model, first.kind); } catch { /* Drop this sample only. */ }
    }
    try {
      const outputTokens = stream.usage?.outputTokens ?? 0;
      const intervalSeconds = first === undefined || stream.lastFragmentAt === undefined ? 0 : (stream.lastFragmentAt - first.at) / 1000;
      if (outputTokens > 0 && intervalSeconds > 0) this.#outputTokensPerSecond.observe(outputTokens / intervalSeconds, provider, model);
    } catch { /* Drop this sample only. */ }
    try { this.#streamDuration.observe((stream.endedAt - stream.dispatchedAt) / 1000, provider, model, stream.outcome); } catch { /* Drop this sample only. */ }
    const usage = stream.usage;
    if (usage === undefined) return;
    for (const [type, tokens] of [["input", usage.inputTokens], ["output", usage.outputTokens], ["cached", usage.cachedTokens ?? 0]] as const) {
      try { this.#tokens.add(tokens, provider, model, type); } catch { /* Drop this sample only. */ }
    }
  }

  /** Counts a provider HTTP refusal: 429 as rate_limited, 503 and 529 as overloaded; other statuses count nothing. */
  recordProviderRejection(provider: string, statusCode: number): void {
    const cause = statusCode === 429 ? "rate_limited" : statusCode === 503 || statusCode === 529 ? "overloaded" : undefined;
    if (cause !== undefined) this.#providerRejected.add(1, provider, cause);
  }

  holdCompleteFrame(bytes: number): () => void {
    this.#pendingFrameBytes += bytes;
    let released = false;
    return () => { if (!released) { released = true; this.#pendingFrameBytes -= bytes; } };
  }
  startContentAssembly(): { readonly observe: (resources:ProviderAssemblyResources)=>void; readonly close:()=>void } {
    let previous:ProviderAssemblyResources={retainedBytes:0,cumulativeContentBytes:0,segments:0,openBlocks:0,identities:0};
    const observe=(resources:ProviderAssemblyResources):void=>{
      for(const key of Object.keys(previous) as (keyof ProviderAssemblyResources)[]) this.#assemblyResources = {...this.#assemblyResources,[key]:this.#assemblyResources[key]+resources[key]-previous[key]};
      previous=resources;
    };
    return {observe,close:()=>observe({retainedBytes:0,cumulativeContentBytes:0,segments:0,openBlocks:0,identities:0})};
  }
  #providerStreamsTotal = 0;
  #providerStreamFailuresTotal = 0;
  #providerStreamDurationMsSum = 0;

  /**
   * Records an admitted provider request turn and returns an idempotent
   * completion callback that updates failure and cumulative duration values.
   */
  startProviderStream(): (failed: boolean) => void {
    const started = performance.now();
    let finished = false;
    this.#providerStreamsTotal += 1;
    return (failed: boolean): void => {
      if (finished) {
        return;
      }
      finished = true;
      if (failed) {
        this.#providerStreamFailuresTotal += 1;
      }
      this.#providerStreamDurationMsSum += Math.max(0, performance.now() - started);
    };
  }

  /** Renders readiness, admission, model speed, provider-stream, and process-memory metrics in Prometheus text format. */
  render(input: { readonly ready: boolean }): string {
    const memory = process.memoryUsage();
    return [
      operationMetricsText(this.operations),
      renderMetrics(this.registry.collect()),
      metric("providergateway_ready", "Provider Gateway readiness state.", "gauge", input.ready ? 1 : 0),
      metric("providergateway_provider_streams_total", "Provider streams admitted by Provider Gateway.", "counter", this.#providerStreamsTotal),
      metric("providergateway_provider_stream_failures_total", "Provider streams that ended with a classified failure.", "counter", this.#providerStreamFailuresTotal),
      metric("providergateway_provider_stream_duration_ms_sum", "Cumulative provider stream duration in milliseconds.", "counter", this.#providerStreamDurationMsSum),
      metric("providergateway_complete_frame_pending_bytes", "Encoded complete frames held through write callback and required drain.", "gauge", this.#pendingFrameBytes),
      ...Object.entries(this.#assemblyResources).map(([key,value])=>metric(`providergateway_content_${key.replace(/[A-Z]/g,letter=>`_${letter.toLowerCase()}`)}`,"Live request-local provider content resources.","gauge",value)),
      metric("process_heap_used_bytes", "JavaScript heap bytes currently used by the process.", "gauge", memory.heapUsed),
      metric("process_rss_bytes", "Resident set size bytes for the process.", "gauge", memory.rss),
    ].join("");
  }
}

function metric(name: string, help: string, type: "counter" | "gauge", value: number): string {
  return `# HELP ${name} ${help}\n# TYPE ${name} ${type}\n${name} ${formatMetricValue(value)}\n`;
}

function formatMetricValue(value: number): string {
  if (!Number.isFinite(value)) {
    return "0";
  }
  return String(Math.max(0, value));
}

/** Closed per-operation observations; raw samples are emitted by the owning service. */
export type ProviderStageOutcome = "success" | "error" | "cancelled";
export type ProviderContentKind = "none" | "text" | "reasoning" | "tool";
export interface ProviderStageSample {
  readonly stage: "provider_first_fragment" | "provider_first_complete" | "complete_frame_write";
  readonly outcome: ProviderStageOutcome;
  readonly durationMs: number;
}

/**
 * One provider stream, from its dispatch to its end, labelled by the
 * catalogue's provider and model ids. Times are milliseconds on one clock;
 * usage is what the provider reported, absent when it reported none.
 */
export interface ProviderStreamSample {
  readonly provider: string;
  readonly model: string;
  readonly outcome: "ok" | "error" | "cancelled";
  readonly dispatchedAt: number;
  readonly endedAt: number;
  readonly firstFragment?: { readonly at: number; readonly kind: "text" | "reasoning" | "tool_call" } | undefined;
  readonly lastFragmentAt?: number | undefined;
  readonly usage?: { readonly inputTokens: number; readonly outputTokens: number; readonly cachedTokens?: number | undefined } | undefined;
}
