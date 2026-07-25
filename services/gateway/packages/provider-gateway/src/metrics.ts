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
  #activeProviderStreams = 0;
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
      metric("providergateway_ready", "Provider Gateway readiness state.", "gauge", input.ready ? 1 : 0),
      metric("providergateway_provider_streams_active", "Active provider streams admitted by Provider Gateway.", "gauge", this.#activeProviderStreams),
      metric("providergateway_provider_streams_total", "Provider streams admitted by Provider Gateway.", "counter", this.#providerStreamsTotal),
      metric("providergateway_provider_stream_failures_total", "Provider streams that ended with a classified failure.", "counter", this.#providerStreamFailuresTotal),
      metric("providergateway_provider_stream_duration_ms_sum", "Cumulative provider stream duration in milliseconds.", "counter", this.#providerStreamDurationMsSum),
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
