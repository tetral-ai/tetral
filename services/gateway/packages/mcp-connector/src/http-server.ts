/**
 * @packageDocumentation
 *
 * Implements the MCP connector's operations-only HTTP listener for liveness,
 * readiness, and Prometheus text exposition. It keeps liveness independent of
 * traffic readiness, returns an unavailable status while startup or shutdown
 * holds readiness false, and exposes no connector RPC or MCP result data. The
 * process command supplies lifecycle callbacks, and the listener calls the
 * service shell only through the injected metrics renderer.
 */

/** Supplies live process state to the operations listener without owning it. */
export interface McpConnectorHealthState {
  readonly health: () => { readonly ok: true };
  readonly ready: () => { readonly ready: boolean };
  readonly metricsText: () => string;
}

/** Exposes the bound operations URL and its asynchronous stop operation. */
export interface McpConnectorHttpServer {
  readonly url: URL;
  readonly stop: () => Promise<void>;
}

/**
 * Binds the operations listener and maps its three fixed paths to injected
 * health, readiness, and metrics state.
 */
export function createMcpConnectorHttpServer(address: string, state: McpConnectorHealthState): McpConnectorHttpServer {
  const bind = parseBindAddress(address);
  const server = Bun.serve({
    hostname: bind.hostname,
    port: bind.port,
    development: false,
    fetch: (request) => {
      const path = new URL(request.url).pathname;
      if (path === "/healthz") {
        return Response.json(state.health(), { status: 200 });
      }
      if (path === "/readyz") {
        const ready = state.ready().ready;
        return Response.json({ ready }, { status: ready ? 200 : 503 });
      }
      if (path === "/metrics") {
        return new Response(state.metricsText(), {
          status: 200,
          headers: {
            "content-type": "text/plain; version=0.0.4; charset=utf-8",
          },
        });
      }
      return new Response("not found", { status: 404 });
    },
  });
  return {
    url: server.url,
    stop: async () => {
      await server.stop();
    },
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
