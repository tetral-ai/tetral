import type { PreviewConnection } from "../../src/providers/preview-publisher.js";
import { ProviderRequest, ProviderRequestKind, ProviderThreadRole, ProviderThreadVisibility } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
export function deferred<T>() {
  let resolve!: (value: T) => void, reject!: (error: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
export class ObservedPreviewConnection implements PreviewConnection {
  readonly frames: { subject: string; data: Uint8Array }[] = [];
  readonly flushing = deferred<void>();
  readonly ended = deferred<void | Error>();
  flushCalls = 0; closeCalls = 0;
  autoFlush = true;
  constructor() { void this.flushing.promise.catch(() => undefined); }
  publish(subject: string, data: Uint8Array): void { this.frames.push({ subject, data: data.slice() }); }
  async flush(): Promise<void> { this.flushCalls++; if (!this.autoFlush) await this.flushing.promise; }
  async close(): Promise<void> { this.closeCalls++; this.ended.resolve(); this.flushing.reject(new Error("closed")); }
  closed(): Promise<void | Error> { return this.ended.promise; }
  decoded(): Record<string, unknown>[] { return this.frames.map(frame => JSON.parse(new TextDecoder().decode(frame.data))); }
}
export function previewRequest(): ProviderRequest {
  return ProviderRequest.fromPartial({ workspaceId: "workspace", sessionId: "session", sessionThreadId: "thread", modelRequestId: "model-request", modelRequestStartEventId: "start-event", requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST, threadRole: ProviderThreadRole.PROVIDER_THREAD_ROLE_MAIN, threadVisibility: ProviderThreadVisibility.PROVIDER_THREAD_VISIBILITY_PUBLIC });
}
export const textId = "evt_11111111111111111111111111111111";
export const secondId = "evt_22222222222222222222222222222222";
export async function observed(predicate: () => boolean): Promise<void> {
  const deadline = performance.now() + 5000;
  while (!predicate()) {
    if (performance.now() >= deadline) throw new Error("preview test observation timed out");
    await new Promise(resolve => setTimeout(resolve, 0));
  }
}
