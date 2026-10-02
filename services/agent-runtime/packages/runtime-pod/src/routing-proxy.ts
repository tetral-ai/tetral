/** Startup depends on the routed proxy attached to this Pod, with a fixed local readiness target. */
/** Both named SDS resources must have accepted initial resources before admission. */
export async function hardenedRoutingListenerReady(timeoutMs: number): Promise<boolean> {
  try {
    const response = await fetch(
      "http://127.0.0.1:15000/stats?format=json&filter=^sds\\.tetral\\.runtime\\.direct_(leaf|validation)\\.update_success$",
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

export async function waitForRoutingProxy(
  profile: "standard-routed" | "hardened" = "standard-routed",
): Promise<void> {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    try {
      const response = await fetch("http://127.0.0.1:15021/healthz/ready", {
        signal: AbortSignal.timeout(Math.min(1000, deadline - Date.now())),
      });
      await response.body?.cancel();
      if (
        response.ok &&
        (profile !== "hardened" ||
          (await hardenedRoutingListenerReady(Math.min(1000, Math.max(1, deadline - Date.now())))))
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
