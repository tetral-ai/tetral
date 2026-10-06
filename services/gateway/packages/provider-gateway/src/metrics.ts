import { OperationMetricsRegistry } from "@tetral/ts-observability";
import type { OperationOutcome } from "@tetral/ts-observability";
import type { ProviderAssemblyResources } from "./providers/block-assembler.js";
/**
 * @packageDocumentation
 *
 * Owns provider-gateway's process-local provider-stream-named counters, active
 * gauge, duration sum, and Prometheus text rendering. The service shell starts
 * one observation when a provider request turn clears admission, before
 * catalog, attachment, credential, or provider-stream setup, and finishes it at
 * turn exit. The operations listener renders the registry with live readiness
 * and process-memory gauges.
 * Completion callbacks are idempotent, values are clamped to non-negative
 * output, and the registry retains no request, provider, or credential content.
 */

/** Aggregates process-local admitted-turn observations for operations exposition. */
export class ProviderGatewayMetricsRegistry {
  readonly operations = new OperationMetricsRegistry("provider-gateway", ["StreamProviderRequest", "provider_first_fragment", "provider_first_complete", "complete_frame_write", "shutdown_drain", "shutdown_cancel_join"]);
  #capacity = 0;
  #admissionRejections = 0;
  setCapacity(capacity: number): void { this.#capacity = Math.max(0, capacity); }
  recordAdmissionRejection(): void { this.#admissionRejections++; }
  observeRequest(outcome: OperationOutcome, durationSeconds: number): void { this.operations.observe("StreamProviderRequest", outcome, durationSeconds); }
  #activeProviderStreams = 0;
  #assemblyResources: ProviderAssemblyResources = {retainedBytes:0,cumulativeContentBytes:0,segments:0,openBlocks:0,identities:0};
  #stages = {provider_first_fragment:{count:0,sum:0},provider_first_complete:{count:0,sum:0},complete_frame_write:{count:0,sum:0}};
  #pendingFrameBytes = 0;
  #stageOutcomes: Record<ProviderStageOutcome, number> = { success: 0, error: 0, cancelled: 0 };
  observeProviderStage(sample: ProviderStageSample): void {
    this.operations.observe(sample.stage, sample.outcome, sample.durationMs / 1000);
    const value = this.#stages[sample.stage];
    value.count++;
    value.sum += Math.max(0, sample.durationMs);
    this.#stageOutcomes[sample.outcome]++;
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
   * completion callback that updates active, failure, and cumulative duration
   * values.
   */
  startProviderStream(): (failed: boolean) => void {
    const started = performance.now();
    let finished = false;
    this.#activeProviderStreams += 1;
    this.#providerStreamsTotal += 1;
    return (failed: boolean): void => {
      if (finished) {
        return;
      }
      finished = true;
      this.#activeProviderStreams = Math.max(0, this.#activeProviderStreams - 1);
      if (failed) {
        this.#providerStreamFailuresTotal += 1;
      }
      this.#providerStreamDurationMsSum += Math.max(0, performance.now() - started);
    };
  }

  /** Renders readiness, provider-stream, and process-memory metrics in Prometheus text format. */
  render(input: { readonly ready: boolean }): string {
    const memory = process.memoryUsage();
    return [
      this.operations.render(),
      metric("providergateway_provider_stream_capacity", "Configured concurrent provider stream admission capacity.", "gauge", this.#capacity),
      metric("providergateway_admission_rejections_total", "Provider streams rejected by concurrent admission capacity.", "counter", this.#admissionRejections),
      metric("providergateway_ready", "Provider Gateway readiness state.", "gauge", input.ready ? 1 : 0),
      metric("providergateway_provider_streams_active", "Active provider streams admitted by Provider Gateway.", "gauge", this.#activeProviderStreams),
      metric("providergateway_provider_streams_total", "Provider streams admitted by Provider Gateway.", "counter", this.#providerStreamsTotal),
      metric("providergateway_provider_stream_failures_total", "Provider streams that ended with a classified failure.", "counter", this.#providerStreamFailuresTotal),
      metric("providergateway_provider_stream_duration_ms_sum", "Cumulative provider stream duration in milliseconds.", "counter", this.#providerStreamDurationMsSum),
      ...Object.entries(this.#stages).flatMap(([stage,value])=>[
        metric(`providergateway_${stage}_ms_sum`,"Cumulative provider stage duration in milliseconds.","counter",value.sum),
        metric(`providergateway_${stage}_total`,"Observed provider stages.","counter",value.count),
      ]),
      ...Object.entries(this.#stageOutcomes).map(([outcome,count])=>metric(`providergateway_provider_stage_${outcome}_total`,"Provider stage samples by closed outcome.","counter",count)),
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
  readonly kind: ProviderContentKind;
  readonly durationMs: number;
  readonly canonicalBytes: number;
  readonly encodedBytes: number;
}
