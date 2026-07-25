/**
 * @packageDocumentation
 *
 * Hosts the provider-gateway operations-only HTTP listener. Application
 * composition supplies live health, readiness, and metrics callbacks; this
 * module maps them to fixed probe and Prometheus paths without exposing
 * provider requests, credentials, or stream data. Liveness remains independent
 * of traffic readiness, unready probes return HTTP 503, and every unknown path
 * returns HTTP 404.
 */

/** Supplies live process state to the operations listener without transferring ownership. */
export interface GatewayHealthState {
  readonly health: () => { readonly ok: true };
  readonly ready: () => { readonly ready: boolean };
  readonly metricsText: () => string;
}

/** Exposes the bound operations URL and its asynchronous stop operation. */
export interface GatewayHttpServer {
  readonly url: URL;
  readonly stop: () => Promise<void>;
}

/** Binds the operations listener and serves its three fixed paths from injected state. */
export function createGatewayHttpServer(address: string, state: GatewayHealthState): GatewayHttpServer {
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
          headers: { "Content-Type": "text/plain; version=0.0.4; charset=utf-8" },
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
