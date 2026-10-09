/** Local endpoints of the routing proxy attached to this Pod. */
export interface RoutingProxyEndpoints {
  /** Proxy readiness URL. */
  readonly readiness: string;
  /** Envoy admin base URL that serves `/stats`. */
  readonly stats: string;
}

/** The fixed production endpoints: the Istio agent readiness port and the Envoy admin port. */
export const routingProxyEndpoints: RoutingProxyEndpoints = Object.freeze({
  readiness: "http://127.0.0.1:15021/healthz/ready",
  stats: "http://127.0.0.1:15000",
});

/** Both named SDS resources must have accepted initial resources before admission. */
export async function hardenedRoutingListenerReady(
  timeoutMs: number,
  statsBase: string = routingProxyEndpoints.stats,
): Promise<boolean> {
  try {
    const response = await fetch(
      `${statsBase}/stats?format=json&filter=^sds\\.tetral\\.runtime\\.direct_(leaf|validation)\\.update_success$`,
      {
        signal: AbortSignal.timeout(timeoutMs),
      },
    );
    if (!response.ok) {
      await response.body?.cancel();
      return false;
    }
    if (response.body === null) return false;
    const reader = response.body.getReader();
    const chunks: Uint8Array[] = [];
    let length = 0;
    try {
      for (;;) {
        const chunk = await reader.read();
        if (chunk.done) break;
        length += chunk.value.length;
        if (length > 16_384) {
          await reader.cancel();
          return false;
        }
        chunks.push(chunk.value);
      }
    } finally {
      reader.releaseLock();
    }
    const bytes = new Uint8Array(length);
    let offset = 0;
    for (const chunk of chunks) {
      bytes.set(chunk, offset);
      offset += chunk.length;
    }
    const body = new TextDecoder().decode(bytes);
    return hardenedRoutingSecretsReady(JSON.parse(body));
  } catch {
    return false;
  }
}

export function hardenedRoutingSecretsReady(parsed: unknown): boolean {
  if (
    typeof parsed !== "object" ||
    parsed === null ||
    !("stats" in parsed) ||
    !Array.isArray(parsed.stats)
  )
    return false;
  const required = new Set([
    "sds.tetral.runtime.direct_leaf.update_success",
    "sds.tetral.runtime.direct_validation.update_success",
  ]);
  for (const stat of parsed.stats as unknown[]) {
    if (typeof stat !== "object" || stat === null || !("name" in stat) || !("value" in stat))
      return false;
    if (typeof stat.name !== "string" || !required.has(stat.name)) return false;
    if (typeof stat.value !== "number" || !Number.isSafeInteger(stat.value) || stat.value <= 0)
      return false;
    required.delete(stat.name);
  }
  return required.size === 0;
}

/**
 * Startup depends on the routed proxy attached to this Pod, with a fixed local readiness target:
 * it waits at most 30 seconds for `127.0.0.1:15021/healthz/ready` and, in the hardened profile,
 * for both direct-listener SDS resources reported by the local admin endpoint. Production always
 * uses these endpoints and deadline; they are parameters only so local fixtures can run this
 * same gate against their own proxy.
 */
export async function waitForRoutingProxy(
  profile: "standard-routed" | "hardened" = "standard-routed",
  endpoints: RoutingProxyEndpoints = routingProxyEndpoints,
  deadlineMs = 30_000,
): Promise<void> {
  const deadline = Date.now() + deadlineMs;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(endpoints.readiness, {
        signal: AbortSignal.timeout(Math.min(1000, deadline - Date.now())),
      });
      await response.body?.cancel();
      if (
        response.ok &&
        (profile !== "hardened" ||
          (await hardenedRoutingListenerReady(
            Math.min(1000, Math.max(1, deadline - Date.now())),
            endpoints.stats,
          )))
      )
        return;
    } catch {
      /* Only the attached proxy can satisfy this dependency. */
    }
    await new Promise<void>((resolve) =>
      setTimeout(resolve, Math.min(100, Math.max(0, deadline - Date.now()))),
    );
  }
  throw new Error("Runtime routing proxy readiness deadline exceeded");
}
