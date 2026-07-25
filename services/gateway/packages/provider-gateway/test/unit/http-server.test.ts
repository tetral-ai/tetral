import { describe, expect, test } from "bun:test";
import { createGatewayHttpServer } from "../../src/http-server.js";

describe("Gateway HTTP ops plane", () => {
  test("serves healthz, readyz, and metrics through bare Bun and stops gracefully", async () => {
    let ready = true;
    const server = createGatewayHttpServer("127.0.0.1:0", {
      health: () => ({ ok: true }),
      ready: () => ({ ready }),
      metricsText: () => "# HELP providergateway_ready Provider Gateway readiness state.\n# TYPE providergateway_ready gauge\nprovidergateway_ready 1\n",
    });
    try {
      expect(await jsonProbe(server.url, "/healthz")).toEqual({ status: 200, body: { ok: true } });
      expect(await jsonProbe(server.url, "/readyz")).toEqual({ status: 200, body: { ready: true } });
      ready = false;
      expect(await jsonProbe(server.url, "/readyz")).toEqual({ status: 503, body: { ready: false } });
      const metrics = await fetch(new URL("/metrics", server.url));
      expect(metrics.status).toBe(200);
      expect(metrics.headers.get("content-type")).toContain("text/plain");
      expect(await metrics.text()).toContain("providergateway_ready 1");
      const missing = await fetch(new URL("/missing", server.url));
      expect(missing.status).toBe(404);
    } finally {
      await server.stop();
    }
  });
});

async function jsonProbe(base: URL, path: string): Promise<{ readonly status: number; readonly body: unknown }> {
  const response = await fetch(new URL(path, base));
  return { status: response.status, body: await response.json() };
}
