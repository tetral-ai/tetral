/**
 * Hosts the Runtime Pod's process-operational HTTP surface. Application composition calls this
 * module after gRPC bind; the Bun server reads health, readiness, and drain state from lifecycle and
 * delegates Prometheus rendering to the metrics module. It exposes no runtime command or public API,
 * returns readiness failure as HTTP 503, and returns 404 for every unrecognized path.
 */
import type { RuntimePodLifecycle } from "./lifecycle.js";
import { runtimePodMetricsText } from "./metrics.js";
import type { RuntimePodMetricsSource } from "./metrics.js";

/** Identifies the bound operational endpoint and provides a forceful listener stop. */
export interface RuntimeHttpServer {
  readonly url: URL;
  readonly stop: () => void;
}

/** Starts the operational HTTP listener and returns its bound URL and stop handle. */
export function createRuntimeHttpServer(
  address: string,
  lifecycle: RuntimePodLifecycle,
  runtimeMetrics?: RuntimePodMetricsSource,
): RuntimeHttpServer {
  const bind = parseBindAddress(address);
  const server = Bun.serve({
    hostname: bind.hostname,
    port: bind.port,
    fetch: (request) => {
      const path = new URL(request.url).pathname;
      if (path === "/health") {
        return Response.json(lifecycle.health(), { status: 200 });
      }
      if (path === "/ready") {
        const ready = lifecycle.ready().ready;
        return Response.json({ ready }, { status: ready ? 200 : 503 });
      }
      if (path === "/metrics") {
        return new Response(runtimePodMetricsText(lifecycle, runtimeMetrics), {
          status: 200,
          headers: { "Content-Type": "text/plain; version=0.0.4; charset=utf-8" },
        });
      }
      return new Response("not found", { status: 404 });
    },
  });
  return {
    url: server.url,
    stop: () => server.stop(true),
  };
}

function parseBindAddress(address: string): { readonly hostname: string; readonly port: number } {
  const index = address.lastIndexOf(":");
  if (index <= 0 || index === address.length - 1) {
    throw new Error("invalid http bind address");
  }
  const hostname = address.slice(0, index);
  const port = Number.parseInt(address.slice(index + 1), 10);
  if (!Number.isInteger(port) || port < 0 || port > 65535) {
    throw new Error("invalid http bind address");
  }
  return { hostname, port };
}
