import type { ProviderRequest } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { ProviderRequestKind, ProviderThreadRole, ProviderThreadVisibility } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { semanticErrorFields } from "@tetral/ts-observability";
import type { GatewayLogger } from "../logger.js";
import type { ProviderPreviewOffer } from "./block-assembler.js";
import { PreviewLimits, encodePreviewFrame, isScalarText, previewSubject, previewFrameEncodedSize } from "./preview-protocol.js";
import type { PreviewFrame, PreviewIdentity } from "./preview-protocol.js";
import { PreviewPublisherDefaults, validPreviewPublisherPolicy } from "./preview-config.js";
import type { PreviewPublisherPolicy } from "./preview-config.js";
import { PreviewPublisherMetrics } from "./preview-metrics.js";
import type { PreviewStopReason } from "./preview-metrics.js";

export interface PreviewConnection {
  publish(subject: string, bytes: Uint8Array): void;
  flush(): Promise<void>;
  close(): Promise<void>;
  closed(): Promise<void | Error>;
}
export interface PreviewRequestProducer { readonly offer: ProviderPreviewOffer; close(): void }
export interface PreviewPublisherOptions {
  readonly connect: () => Promise<PreviewConnection>;
  readonly policy?: PreviewPublisherPolicy;
  readonly logger?: GatewayLogger;
  readonly metrics?: PreviewPublisherMetrics;
  readonly random?: () => number;
  readonly encodeFrame?: typeof encodePreviewFrame;
}
interface PendingFrame { readonly producer: RequestProducer; readonly eventId?: string; readonly subject: string; readonly data: Uint8Array; readonly charge: number }
interface EventState { readonly type: "agent.message" | "agent.thinking"; sequence: number; trailingHigh: string; stopped: boolean }
const noopProducer: PreviewRequestProducer = { offer: () => undefined, close: () => undefined };

/** One process client, one worker/flush and one reconnect supervisor; offers never await I/O. */
export class PreviewPublisher {
  readonly metrics: PreviewPublisherMetrics;
  private readonly policy: PreviewPublisherPolicy;
  private connection: PreviewConnection | undefined;
  private queue: PendingFrame[] = [];
  private inFlight: PendingFrame[] = [];
  private readonly producers = new Set<RequestProducer>();
  private worker: Promise<void> | undefined;
  private supervisor: Promise<void> | undefined;
  private stopping = false;
  private wakeSupervisor: (() => void) | undefined;
  private retryTimer: ReturnType<typeof setTimeout> | undefined;
  private degradation: PreviewStopReason | undefined;
  private readonly connectionCloses = new WeakMap<PreviewConnection, Promise<void>>();
  private readonly closingConnections = new Set<Promise<void>>();
  private closeFailure: unknown;
  constructor(private readonly options: PreviewPublisherOptions) {
    this.policy = options.policy ?? PreviewPublisherDefaults;
    if (!validPreviewPublisherPolicy(this.policy)) throw new Error("invalid preview policy");
    this.metrics = options.metrics ?? new PreviewPublisherMetrics();
  }
  start(): void {
    if (this.supervisor !== undefined || this.stopping) return;
    this.supervisor = this.supervise();
  }
  createProducer(request: ProviderRequest): PreviewRequestProducer {
    if (request.requestKind !== ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST ||
      request.threadRole !== ProviderThreadRole.PROVIDER_THREAD_ROLE_MAIN || request.threadVisibility !== ProviderThreadVisibility.PROVIDER_THREAD_VISIBILITY_PUBLIC) return noopProducer;
    const identity: PreviewIdentity = { workspace_id: request.workspaceId, session_id: request.sessionId, thread_id: request.sessionThreadId,
      model_request_id: request.modelRequestId, model_request_start_event_id: request.modelRequestStartEventId, request_kind: "agent_provider_request" };
    const producer = new RequestProducer(this, identity);
    if (this.connection === undefined || this.stopping) { producer.stop("unavailable"); return producer; }
    this.producers.add(producer);
    if (!this.offer(producer, { version: 1, ...identity, kind: "request_open" })) producer.stop("unavailable");
    return producer;
  }
  /** A validated TLS generation replaces the client before retiring its predecessor. */
  async replaceConnection(candidate: PreviewConnection): Promise<void> {
    if (this.stopping) { await this.closeConnection(candidate); return; }
    const old = this.connection;
    this.invalidate("credential_reload", old);
    this.connection = candidate; this.metrics.connected = true;
    this.watch(candidate);
    this.wakeSupervisor?.();
    if (old !== undefined) await this.closeConnection(old);
  }
  async retireConnection(reason: PreviewStopReason): Promise<void> {
    const connection = this.connection;
    this.invalidate(reason, connection);
    if (connection !== undefined) await this.closeConnection(connection);
  }
  offer(producer: RequestProducer, frame: PreviewFrame): boolean {
    this.metrics.attempted++;
    if (this.connection === undefined || this.stopping || producer.stopped) { this.metrics.dropped++; return false; }
    const eventId = "event_id" in frame ? frame.event_id : undefined;
    if (this.metrics.pendingFrames >= this.policy.queueFrames) { this.metrics.dropped++; producer.stopEvent(eventId, "queue_frames"); return false; }
    let size: number, subject: string;
    try { size = previewFrameEncodedSize(frame); subject = previewSubject(frame.workspace_id, frame.session_id); }
    catch { this.metrics.dropped++; producer.stopEvent("event_id" in frame ? frame.event_id : undefined, "encoding"); return false; }
    if (size > this.policy.batchBytes) { this.metrics.dropped++; producer.stopEvent(eventId, "frame_bytes"); return false; }
    // Reserve the bounded client copy and PUB framing at admission, so a full
    // queue cannot force an uncharged batch into the NATS client's buffer.
    const wireBytes = Buffer.byteLength(`PUB ${subject} ${size}\r\n`) + size + 2 + Buffer.byteLength("PING\r\n");
    const charge = size + wireBytes;
    const reason = this.metrics.pendingBytes + charge > this.policy.queueBytes ? "queue_bytes" : undefined;
    if (reason !== undefined) { this.metrics.dropped++; producer.stopEvent("event_id" in frame ? frame.event_id : undefined, reason); return false; }
    // Reserve the encoded buffer before allocation. Direct encoding creates no
    // transient JSON string outside the same payload/client reservation.
    this.metrics.pendingBytes += charge; this.metrics.pendingFrames++;
    let data: Uint8Array;
    try { data = (this.options.encodeFrame ?? encodePreviewFrame)(frame); if (data.length !== size) throw new Error("preview encoder size mismatch"); }
    catch { this.metrics.pendingBytes -= charge; this.metrics.pendingFrames--; this.metrics.dropped++; producer.stopEvent(eventId, "encoding"); return false; }
    this.metrics.encodedFrames++; this.metrics.encodedBytes += data.length; this.metrics.accepted++;
    this.queue.push({ producer, ...(frame.kind === "request_open" ? {} : { eventId: frame.event_id }), subject, data, charge });
    this.kickWorker(); return true;
  }
  remove(producer: RequestProducer, eventId?: string): void {
    this.queue = this.queue.filter(frame => {
      if (frame.producer !== producer || (eventId !== undefined && frame.eventId !== eventId)) return true;
      this.release(frame); this.metrics.dropped++; return false;
    });
  }
  releaseProducer(producer: RequestProducer): void { this.producers.delete(producer); }
  stopped(reason: PreviewStopReason, request: boolean, identity: PreviewIdentity): void {
    this.metrics.stop(reason, request);
    this.log("preview.stopped", reason, identity);
  }
  private release(frame: PendingFrame): void { this.metrics.pendingBytes -= frame.charge; this.metrics.pendingFrames--; }
  private kickWorker(): void {
    if (this.worker !== undefined || this.queue.length === 0 || this.connection === undefined || this.stopping) return;
    this.worker = Promise.resolve().then(() => this.publish()).catch(error => { this.closeFailure ??= error; }).finally(() => { this.worker = undefined; this.kickWorker(); });
  }
  private async publish(): Promise<void> {
    const connection = this.connection;
    if (connection === undefined) return;
    let bytes = 0;
    while (this.queue.length > 0 && this.inFlight.length < this.policy.batchFrames) {
      const frame = this.queue[0]!;
      if (bytes + frame.data.length > this.policy.batchBytes) break;
      this.queue.shift(); this.inFlight.push(frame); bytes += frame.data.length;
    }
    const batch = this.inFlight;
    let flushed = false;
    this.metrics.clientPendingBytes = batch.reduce((sum, frame) => sum + frame.charge - frame.data.length, 0);
    try {
      for (const frame of batch) connection.publish(frame.subject, frame.data);
      await withTimeout(connection.flush(), this.policy.flushTimeoutMs);
      flushed = this.connection === connection;
    } catch {
      this.metrics.failures++;
      this.invalidate("flush_failed", connection);
    } finally {
      try {
        // A retired client and this worker still own their batch until both
        // settle. Replacement admission must account that retained storage.
        if (!flushed) await this.closeConnection(connection);
      } finally {
        for (const frame of batch) this.release(frame);
        if (flushed) this.metrics.flushed += batch.length;
        else this.metrics.dropped += batch.length;
        this.inFlight = []; this.metrics.clientPendingBytes = 0;
      }
    }
  }
  private invalidate(reason: PreviewStopReason, connection: PreviewConnection | undefined): void {
    if (connection !== this.connection) return;
    this.connection = undefined; this.metrics.connected = false;
    if (connection !== undefined) void this.closeConnection(connection).catch(() => undefined);
    for (const producer of [...this.producers]) producer.stop(reason);
    // The single worker retains its batch/client reservation through flush and
    // old-client cleanup. Only queued frames have no remaining transport owner.
    // Producer stop removed all queued frames; account any producer already closed.
    for (const frame of this.queue.splice(0)) { this.release(frame); this.metrics.dropped++; }
    if (this.degradation === undefined) this.log("preview.disconnected", reason);
    this.degradation = reason; this.wakeSupervisor?.();
  }
  private watch(connection: PreviewConnection): void {
    void connection.closed().then(() => { this.invalidate("connection_lost", connection); }, () => { this.invalidate("connection_lost", connection); });
  }
  private async supervise(): Promise<void> {
    while (!this.stopping) {
      if (this.connection === undefined) {
        try {
          await Promise.allSettled([...this.closingConnections]);
          if (this.stopping) break;
          const pending = this.options.connect();
          let expired = false;
          const joined = pending.then(async connection => { if (expired || this.stopping) await this.closeConnection(connection); }, () => undefined);
          void joined.catch(() => undefined);
          let connection: PreviewConnection;
          try { connection = await withTimeout(pending, this.policy.connectTimeoutMs); }
          catch (error) { expired = true; await joined; throw error; }
          if (this.stopping) { await this.closeConnection(connection); break; }
          // A validated credential replacement can win while this dial is in
          // flight. Never overwrite its client or leave that client unowned.
          if (this.connection !== undefined) { await this.closeConnection(connection); continue; }
          this.connection = connection; this.metrics.connected = true; this.watch(connection);
          if (this.degradation !== undefined) this.log("preview.recovered", this.degradation);
          this.degradation = undefined;
        } catch {
          this.metrics.failures++;
          if (this.degradation === undefined) this.log("preview.disconnected", "unavailable");
          this.degradation = "unavailable";
        }
      }
      if (this.stopping) break;
      await new Promise<void>(resolve => {
        this.wakeSupervisor = resolve;
        if (this.connection === undefined) this.retryTimer = setTimeout(resolve, Math.max(1, Math.round(this.policy.retryMaxMs * (0.5 + 0.5 * (this.options.random?.() ?? Math.random())))));
      });
      if (this.retryTimer !== undefined) clearTimeout(this.retryTimer);
      this.retryTimer = undefined; this.wakeSupervisor = undefined;
    }
  }
  private log(event: string, reason: PreviewStopReason, identity?: PreviewIdentity): void {
    try {
      const record = { event, "event.kind": event, operation: "preview.publish", component: "gateway", reason,
        "transport.stage": "preview", "transport.outcome": event === "preview.recovered" ? "recovered" : "stopped",
        ...(identity === undefined ? {} : { "workspace.id": identity.workspace_id, "session.id": identity.session_id, "thread.id": identity.thread_id, "model_request.id": identity.model_request_id }) };
      if (event === "preview.recovered") this.options.logger?.info(record);
      else {
        const logger = this.options.logger as (GatewayLogger & { warn?: GatewayLogger["error"] }) | undefined;
        (logger?.warn ?? logger?.error)?.call(logger, { ...record, ...semanticErrorFields({ errorClass: "preview_unavailable", errorCode: reason, messageSafe: "best-effort preview stopped" }) });
      }
    } catch { /* Diagnostics never change complete provider output. */ }
  }
  private closeConnection(connection: PreviewConnection): Promise<void> {
    const previous = this.connectionCloses.get(connection);
    if (previous !== undefined) return previous;
    const closing = Promise.resolve().then(() => connection.close()).catch(error => { this.closeFailure ??= error; throw error; });
    this.connectionCloses.set(connection, closing); this.closingConnections.add(closing);
    void closing.then(() => this.closingConnections.delete(closing), () => this.closingConnections.delete(closing));
    return closing;
  }
  async close(): Promise<void> {
    this.stopping = true;
    const connection = this.connection;
    this.invalidate("shutdown", connection);
    this.wakeSupervisor?.();
    if (this.retryTimer !== undefined) clearTimeout(this.retryTimer);
    const outcomes = await Promise.allSettled([...(connection === undefined ? [] : [this.closeConnection(connection)]), this.worker, this.supervisor]);
    await Promise.allSettled([...this.closingConnections]);
    if (this.closeFailure !== undefined) throw this.closeFailure;
    const failed = outcomes.find((outcome): outcome is PromiseRejectedResult => outcome.status === "rejected");
    if (failed !== undefined) throw failed.reason;
  }
}

class RequestProducer implements PreviewRequestProducer {
  stopped = false;
  private readonly events = new Map<string, EventState>();
  constructor(private readonly publisher: PreviewPublisher, readonly identity: PreviewIdentity) {}
  readonly offer: ProviderPreviewOffer = preview => {
    if (this.stopped) return;
    let event = this.events.get(preview.eventId);
    if (event === undefined) {
      if (this.events.size >= PreviewLimits.maxEventIdentities) { this.stop("event_identities"); return; }
      event = { type: preview.kind === "thinking_started" ? "agent.thinking" : "agent.message", sequence: 0, trailingHigh: "", stopped: false };
      this.events.set(preview.eventId, event);
      if (!this.publisher.offer(this, { version: 1, ...this.identity, kind: "event_start", event_type: event.type, event_id: preview.eventId, preview_sequence: 0 })) return;
    }
    if (event.stopped || preview.kind !== "text_delta") return;
    if (preview.delta.length > PreviewLimits.maxFrameBytes) { this.stopEvent(preview.eventId, "frame_bytes"); return; }
    let text = event.trailingHigh + preview.delta;
    event.trailingHigh = "";
    const last = text.charCodeAt(text.length - 1);
    if (last >= 0xd800 && last <= 0xdbff) { event.trailingHigh = text.slice(-1); text = text.slice(0, -1); }
    if (!isScalarText(text)) { this.stopEvent(preview.eventId, "encoding"); return; }
    if (text.length === 0) return;
    this.publisher.offer(this, { version: 1, ...this.identity, kind: "event_delta", event_type: "agent.message", event_id: preview.eventId, preview_sequence: ++event.sequence, text });
  };
  stopEvent(eventId: string | undefined, reason: PreviewStopReason): void {
    if (eventId === undefined) { this.stop(reason); return; }
    const event = this.events.get(eventId);
    if (event?.stopped) return;
    if (event !== undefined) { event.stopped = true; event.trailingHigh = ""; }
    this.publisher.remove(this, eventId); this.publisher.stopped(reason, false, this.identity);
  }
  stop(reason: PreviewStopReason): void {
    if (this.stopped) return;
    this.stopped = true; this.events.clear(); this.publisher.remove(this);
    this.publisher.releaseProducer(this); this.publisher.stopped(reason, true, this.identity);
  }
  close(): void { this.stopped = true; this.events.clear(); this.publisher.releaseProducer(this); }
}
async function withTimeout<T>(pending: Promise<T>, milliseconds: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try { return await Promise.race([pending, new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error("preview operation timed out")), milliseconds); })]); }
  finally { if (timer !== undefined) clearTimeout(timer); }
}
