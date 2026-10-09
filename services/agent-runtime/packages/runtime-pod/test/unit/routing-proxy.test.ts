import { afterEach, describe, expect, test } from "bun:test";
import { hardenedRoutingSecretsReady, waitForRoutingProxy } from "../../src/routing-proxy.js";

const leaf = { name: "sds.tetral.runtime.direct_leaf.update_success", value: 1 };
const validation = { name: "sds.tetral.runtime.direct_validation.update_success", value: 1 };
describe("hardened routing readiness", () => {
  test("requires independent successful initial leaf and trust SDS updates", () => {
    expect(hardenedRoutingSecretsReady({ stats: [leaf, validation] })).toBe(true);
    expect(hardenedRoutingSecretsReady({ stats: [leaf] })).toBe(false);
    expect(hardenedRoutingSecretsReady({ stats: [validation] })).toBe(false);
    expect(hardenedRoutingSecretsReady({ stats: [{ ...leaf, value: 0 }, validation] })).toBe(false);
    expect(hardenedRoutingSecretsReady({ stats: [leaf, { ...validation, value: 0 }] })).toBe(false);
    expect(hardenedRoutingSecretsReady({ stats: [leaf, leaf, validation] })).toBe(false);
    expect(hardenedRoutingSecretsReady({ stats: [{ ...leaf, value: Infinity }, validation] })).toBe(
      false,
    );
    expect(hardenedRoutingSecretsReady({ stats: [{ ...leaf, value: "1" }, validation] })).toBe(
      false,
    );
    expect(hardenedRoutingSecretsReady({ stats: [] })).toBe(false);
  });
});

const proxies: { stop: (closeActiveConnections?: boolean) => void }[] = [];
afterEach(() => {
  for (const proxy of proxies.splice(0)) proxy.stop(true);
});

/** A local stand-in for the proxy readiness and admin endpoints. */
function stubProxy(options: { ready: boolean; stats?: unknown; statsStatus?: number }) {
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch(request) {
      const path = new URL(request.url).pathname;
      if (path === "/ready") return new Response(null, { status: options.ready ? 200 : 503 });
      if (path === "/stats")
        return Response.json(options.stats ?? { stats: [] }, { status: options.statsStatus ?? 200 });
      return new Response(null, { status: 404 });
    },
  });
  proxies.push(server);
  return {
    readiness: `http://127.0.0.1:${server.port}/ready`,
    stats: `http://127.0.0.1:${server.port}`,
  };
}

describe("routing proxy startup gate", () => {
  test("standard profile requires only proxy readiness", async () => {
    await waitForRoutingProxy("standard-routed", stubProxy({ ready: true, statsStatus: 503 }), 2000);
  });

  test("hardened profile also requires both direct-listener SDS resources", async () => {
    await waitForRoutingProxy(
      "hardened",
      stubProxy({ ready: true, stats: { stats: [leaf, validation] } }),
      2000,
    );
    for (const stats of [[leaf], [validation]]) {
      await expect(
        waitForRoutingProxy("hardened", stubProxy({ ready: true, stats: { stats } }), 300),
      ).rejects.toThrow("Runtime routing proxy readiness deadline exceeded");
    }
  });

  test("hardened profile treats a non-OK statistics response as not ready", async () => {
    await expect(
      waitForRoutingProxy(
        "hardened",
        stubProxy({ ready: true, stats: { stats: [leaf, validation] }, statsStatus: 503 }),
        300,
      ),
    ).rejects.toThrow("Runtime routing proxy readiness deadline exceeded");
  });

  test("hardened profile treats a statistics body over 16 KiB as not ready", async () => {
    // Apart from its size this body reports both SDS resources ready, so only the cap rejects it.
    await expect(
      waitForRoutingProxy(
        "hardened",
        stubProxy({
          ready: true,
          stats: { stats: [leaf, validation], padding: "x".repeat(17_000) },
        }),
        300,
      ),
    ).rejects.toThrow("Runtime routing proxy readiness deadline exceeded");
  });

  test("an unready proxy blocks startup until the deadline", async () => {
    await expect(
      waitForRoutingProxy("standard-routed", stubProxy({ ready: false }), 300),
    ).rejects.toThrow("Runtime routing proxy readiness deadline exceeded");
  });
});
