export type PreviewStopReason = "unavailable" | "connection_lost" | "flush_failed" | "queue_bytes" | "queue_frames" | "frame_bytes" | "encoding" | "event_identities" | "credential_reload" | "shutdown";
/** Local accounting only: server flush does not establish subscriber delivery. */
export class PreviewPublisherMetrics {
  attempted = 0; accepted = 0; flushed = 0; dropped = 0; failures = 0;
  encodedFrames = 0; encodedBytes = 0; pendingBytes = 0; clientPendingBytes = 0; pendingFrames = 0;
  disabledRequests = 0; disabledEvents = 0; connected = false;
  private readonly stops = new Map<PreviewStopReason, number>();
  stop(reason: PreviewStopReason, request: boolean): void {
    if (request) this.disabledRequests++; else this.disabledEvents++;
    this.stops.set(reason, (this.stops.get(reason) ?? 0) + 1);
  }
  render(): string {
    const values = { attempted_total: this.attempted, accepted_total: this.accepted, flushed_total: this.flushed,
      dropped_total: this.dropped, failures_total: this.failures, encoded_frames_total: this.encodedFrames, encoded_bytes_total: this.encodedBytes,
      pending_bytes: this.pendingBytes, client_pending_bytes: this.clientPendingBytes, pending_frames: this.pendingFrames,
      disabled_requests_total: this.disabledRequests, disabled_events_total: this.disabledEvents, connected: Number(this.connected) };
    return Object.entries(values).map(([name, value]) => `# TYPE providergateway_preview_${name} ${name.endsWith("_total") ? "counter" : "gauge"}\nprovidergateway_preview_${name} ${value}\n`).join("") +
      [...this.stops].map(([reason, value]) => `providergateway_preview_stops_total{reason="${reason}"} ${value}\n`).join("");
  }
}
