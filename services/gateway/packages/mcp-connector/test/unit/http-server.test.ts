import { describe, expect, test } from "bun:test";
import { createMcpConnectorHttpServer } from "../../src/http-server.js";
import { McpConnectorMetricsRegistry } from "../../src/metrics.js";

describe("MCP connector HTTP ops plane", () => {
  test("serves healthz, readyz, and contract metrics on the internal listener", async () => {
    let ready = true;
    const metrics = new McpConnectorMetricsRegistry();
    metrics.recordRunTool({
      tool: "create_issue",
      status: "completed",
      errorKind: "",
      durationSeconds: 0.25,
    });
    const server = createMcpConnectorHttpServer("127.0.0.1:0", {
      health: () => ({ ok: true }),
      ready: () => ({ ready }),
      metricsText: () => metrics.render(),
    });
    try {
      expect(await jsonProbe(server.url, "/healthz")).toEqual({ status: 200, body: { ok: true } });
      expect(await jsonProbe(server.url, "/readyz")).toEqual({ status: 200, body: { ready: true } });
      ready = false;
      expect(await jsonProbe(server.url, "/readyz")).toEqual({ status: 503, body: { ready: false } });
      const metricsResponse = await fetch(new URL("/metrics", server.url));
      expect(metricsResponse.status).toBe(200);
      expect(metricsResponse.headers.get("content-type")).toContain("text/plain");
      expect(await metricsResponse.text()).toContain('mcpconnector_calls_total{tool="create_issue",status="completed",error_kind=""} 1');
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
