import { describe, expect, test } from "bun:test";
import { ProviderGatewayMetricsRegistry } from "../../src/metrics.js";

describe("ProviderGatewayMetricsRegistry", () => {
  test("exports readiness and provider stream counters", () => {
    const registry = new ProviderGatewayMetricsRegistry();

    const finish = registry.startProviderStream();
    let body = registry.render({ ready: true });
    expect(body).toContain("providergateway_ready 1");
    expect(body).toContain("providergateway_provider_streams_active 1");
    expect(body).toContain("providergateway_provider_streams_total 1");
    expect(body).toContain("providergateway_provider_stream_failures_total 0");

    finish(true);
    finish(true);
    body = registry.render({ ready: false });
    expect(body).toContain("providergateway_ready 0");
    expect(body).toContain("providergateway_provider_streams_active 0");
    expect(body).toContain("providergateway_provider_stream_failures_total 1");
    expect(body).toContain("process_heap_used_bytes");
  });
});
