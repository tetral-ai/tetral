/** Request-local block assembly. Fragments, provider bodies and signatures stay private. */
import {
  MaxIdBytes, MaxMetadataBytes, MaxProviderContextTextJsonBytes,
  MaxStableReasoningBytesPerRequest, MaxStableReasoningPartsPerRequest,
  validProviderCompleteText, validProviderEventId, validProviderMetadataJson,
  validProviderToolCallInputJson, validateProviderStreamEvent,
} from "@tetral/gateway-protocol/src/bounds.js";
import { ProviderRequestKind, ProviderStreamEventType, ProviderThreadRole, ProviderThreadVisibility } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { ProviderRequest, ProviderStreamEvent } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { NormalizedProviderEventType as Kind, validateNormalizedProviderEvent } from "@tetral/gateway-lowering/src/normalized-stream.js";
import type { NormalizedProviderEvent } from "@tetral/gateway-lowering/src/normalized-stream.js";

/** Operating resource caps are injectable and calibrated separately from legal content limits. */
export interface ProviderAssemblyBounds {
  readonly maxRetainedBytes: number;
  readonly maxOpenBlocks: number;
  readonly maxIdentities: number;
  readonly maxSegments: number;
  readonly coalesceCodeUnits: number;
}
export interface ProviderAssemblyResources {
  readonly retainedBytes: number;
  /** Diagnostic work counter; delivered content does not consume a live budget. */
  readonly cumulativeContentBytes: number;
  readonly segments: number;
  readonly openBlocks: number;
  readonly identities: number;
}
/** PR4 offers progress through an injected nonblocking seam; transport belongs to its owner. */
export type ProviderPreviewOffer = (preview:
  | { readonly kind: "thinking_started"; readonly eventId: string; readonly providerPartId: string }
  | { readonly kind: "text_delta"; readonly eventId: string; readonly providerPartId: string; readonly delta: string }
) => void;
export interface ProviderBlockAssemblerOptions {
  readonly bounds: ProviderAssemblyBounds;
  readonly request: Pick<ProviderRequest, "requestKind" | "threadRole" | "threadVisibility">;
  readonly allocateEventId?: () => string;
  readonly offerPreview?: ProviderPreviewOffer;
  readonly observeResources?: (resources: ProviderAssemblyResources) => void;
}
export type ProviderIncompleteStreamCategory = "finish" | "eof" | "text" | "reasoning" | "tool_input" | "tool_call";
export class ProviderIncompleteStreamError extends Error {
  constructor(readonly category: ProviderIncompleteStreamCategory, readonly counts: { readonly text: number; readonly reasoning: number; readonly toolInput: number }) {
    super("provider stream ended with an incomplete fragment lifecycle");
    this.name = "ProviderIncompleteStreamError";
  }
}
export class ProviderAssemblyLimitError extends Error {
  constructor(readonly reason: "retained_bytes" | "cumulative_bytes" | "open_blocks" | "identities" | "segments" | "text_bytes" | "reasoning_budget" | "metadata_bytes") {
    super("provider stream resource limit exceeded");
    this.name = "ProviderAssemblyLimitError";
  }
}
interface TextBlock {
  readonly kind: "text" | "reasoning";
  readonly id: string;
  eventId: string | undefined;
  readonly chunks: string[];
  pendingChunk: string;
  readonly accounting: ScalarTextAccounting;
  metadata: Record<string, unknown>;
  metadataBytes: number;
}
interface ToolBlock { readonly accounting: ScalarTextAccounting; readonly name: string; ended: boolean; metadata: Record<string, unknown>; metadataBytes: number }
const encoder = new TextEncoder();

/** Owns every normalized lifecycle, lifetime identity and request-wide reasoning charge. */
export class ProviderBlockAssembler {
  private readonly blocks = new Map<string, TextBlock>();
  private readonly tools = new Map<string, ToolBlock>();
  private readonly seen = new Set<string>();
  private readonly eventIds = new Set<string>();
  private sequence = 0;
  private terminal = false;
  private contentStarted = false;
  private attachmentsSeen = false;
  private retainedBytes = 0;
  private cumulativeContentBytes = 0;
  private segments = 0;
  private reasoningParts = 0;
  private reasoningBytes = 0;
  private readonly previewAdmitted: boolean;
  constructor(private readonly options: ProviderBlockAssemblerOptions) {
    for (const value of Object.values(options.bounds)) {
      if (!Number.isSafeInteger(value) || value <= 0) throw new Error("invalid provider assembly bounds");
    }
    this.previewAdmitted = options.request.requestKind === ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST &&
      options.request.threadRole === ProviderThreadRole.PROVIDER_THREAD_ROLE_MAIN &&
      options.request.threadVisibility === ProviderThreadVisibility.PROVIDER_THREAD_VISIBILITY_PUBLIC;
  }

  accept(event: NormalizedProviderEvent): readonly ProviderStreamEvent[] {
    if (this.terminal) throw new Error("provider stream emitted after terminal");
    if (!validateNormalizedProviderEvent(event)) throw new Error("invalid normalized provider event");
    switch (event.type) {
      case Kind.TextStart:
        this.start("text", event.text.id, event.text.metadataJson); return [];
      case Kind.ReasoningStart: {
        const block = this.start("reasoning", event.reasoning.id, event.reasoning.metadataJson);
        const frame = this.frame({ type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_THINKING_STARTED,
          thinkingStarted: { providerPartId: block.id, eventId: block.eventId! } });
        this.offer({ kind: "thinking_started", providerPartId: block.id, eventId: block.eventId! });
        return [frame];
      }
      case Kind.TextDelta:
        this.delta("text", event.text.id, event.text.text, event.text.metadataJson); return [];
      case Kind.ReasoningDelta:
        this.delta("reasoning", event.reasoning.id, event.reasoning.text, event.reasoning.metadataJson); return [];
      case Kind.TextEnd:
        return this.end("text", event.text.id, event.text.metadataJson);
      case Kind.ReasoningEnd:
        return this.end("reasoning", event.reasoning.id, event.reasoning.metadataJson);
      case Kind.ToolInputStart:
        this.startTool(event.toolInput); return [];
      case Kind.ToolInputDelta:
        this.toolInput(event.toolInput, false); return [];
      case Kind.ToolInputEnd:
        this.toolInput(event.toolInput, true); return [];
      case Kind.ToolCall:
        return [this.toolCall(event.toolCall)];
      case Kind.AttachmentRejections:
        if (this.contentStarted || this.attachmentsSeen) throw new Error("misplaced attachment rejections");
        this.attachmentsSeen = true;
        return [this.frame({ type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_ATTACHMENT_REJECTIONS, attachmentRejections: event.attachmentRejections })];
      case Kind.Finish:
        this.assertComplete("finish");
        this.terminal = true;
        return [this.frame({ type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH, finish: event.finish })];
      case Kind.ProviderError:
        this.terminal = true;
        this.release();
        return [this.frame({ type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR, providerError: event.providerError })];
    }
  }

  assertComplete(category: ProviderIncompleteStreamCategory = "eof"): void {
    if (this.blocks.size > 0 || this.tools.size > 0 || !this.terminal && category === "eof") throw this.incomplete(category);
  }
  get resources(): ProviderAssemblyResources {
    return { retainedBytes: this.retainedBytes, cumulativeContentBytes: this.cumulativeContentBytes,
      segments: this.segments, openBlocks: this.blocks.size + this.tools.size, identities: this.seen.size };
  }
  get isTerminal(): boolean { return this.terminal; }
  /** Called on transport completion/cancellation; no state survives this request. */
  release(): void {
    this.blocks.clear(); this.tools.clear(); this.seen.clear(); this.eventIds.clear();
    this.retainedBytes = 0; this.cumulativeContentBytes = 0; this.reasoningBytes = 0; this.reasoningParts = 0; this.segments = 0; this.observe();
  }
  private start(kind: "text" | "reasoning", id: string, metadataJson: string): TextBlock {
    this.identity(kind, id);
    const metadata = kind === "reasoning" ? parseMetadata(metadataJson) : {};
    const metadataBytes = kind === "reasoning" ? jsonBytes(metadata) : 0;
    if (kind === "reasoning") {
      this.reasoningParts += 1; this.reasoningBytes += metadataBytes; this.cumulativeContentBytes += metadataBytes;
      this.checkReasoning();
    }
    const block: TextBlock = { kind, id, eventId: kind === "reasoning" ? this.allocate() : undefined,
      chunks: [], pendingChunk: "", accounting: new ScalarTextAccounting(), metadata, metadataBytes };
    this.blocks.set(`${kind}:${id}`, block); this.retainedBytes += metadataBytes;
    this.contentStarted = true; this.check(); return block;
  }
  private delta(kind: "text" | "reasoning", id: string, delta: string, metadataJson: string): void {
    const block = this.block(kind, id);
    this.mergeMetadata(block, metadataJson);
    const previous = block.accounting.rawBytes;
    block.accounting.append(delta);
    if (block.accounting.jsonBytes > MaxProviderContextTextJsonBytes) throw new ProviderAssemblyLimitError("text_bytes");
    const added = block.accounting.rawBytes - previous;
    this.retainedBytes += added; this.cumulativeContentBytes += added;
    if (kind === "reasoning") { this.reasoningBytes += added; this.checkReasoning(); }
    if (delta.length > 0) {
      if (block.eventId === undefined) block.eventId = this.allocate();
      let offset = 0;
      while (offset < delta.length) {
        if (block.pendingChunk.length === 0) this.segments += 1;
        const count = Math.min(delta.length - offset, this.options.bounds.coalesceCodeUnits - block.pendingChunk.length);
        block.pendingChunk += delta.slice(offset, offset + count); offset += count;
        if (block.pendingChunk.length === this.options.bounds.coalesceCodeUnits) {
          block.chunks.push(block.pendingChunk); block.pendingChunk = "";
        }
        this.check();
      }
      if (kind === "text") this.offer({ kind: "text_delta", providerPartId: id, eventId: block.eventId, delta });
    }
    this.check();
  }
  private end(kind: "text" | "reasoning", id: string, metadataJson: string): readonly ProviderStreamEvent[] {
    const block = this.block(kind, id); this.mergeMetadata(block, metadataJson); block.accounting.finish();
    if (block.pendingChunk.length > 0) block.chunks.push(block.pendingChunk);
    const text = block.chunks.join("");
    if (!validProviderCompleteText(text) || jsonBytes(text) !== block.accounting.jsonBytes) throw new Error("invalid completed provider text");
    let frame: ProviderStreamEvent | undefined;
    if (kind === "reasoning") frame = this.frame({ type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_COMPLETE,
      reasoningComplete: { providerPartId: id, thinkingEventId: block.eventId!, text, providerMetadataJson: JSON.stringify(block.metadata) } });
    else if (text.length > 0) frame = this.frame({ type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_COMPLETE,
      textComplete: { providerPartId: id, eventId: block.eventId!, text } });
    this.blocks.delete(`${kind}:${id}`);
    this.retainedBytes -= block.accounting.rawBytes + block.metadataBytes;
    this.segments -= block.chunks.length; this.observe();
    return frame === undefined ? [] : [frame];
  }
  private startTool(fragment: { readonly id: string; readonly name: string; readonly metadataJson: string }): void {
    this.identity("tool", fragment.id); requireId(fragment.name);
    const metadata = parseMetadata(fragment.metadataJson), metadataBytes = jsonBytes(metadata);
    this.tools.set(fragment.id, { accounting: new ScalarTextAccounting(), name: fragment.name, ended: false, metadata, metadataBytes });
    this.retainedBytes += metadataBytes; this.contentStarted = true; this.check();
  }
  private toolInput(fragment: { readonly id: string; readonly name: string; readonly text: string; readonly metadataJson: string }, ended: boolean): void {
    const block = this.tools.get(fragment.id);
    if (block === undefined || block.ended || block.name !== fragment.name) throw this.incomplete("tool_input");
    // The SDK retains the streamed arguments and also supplies complete JSON.
    // Charge both copies without keeping another argument string here.
    const previous = block.accounting.rawBytes;
    block.accounting.append(fragment.text);
    if (ended) block.accounting.finish();
    this.cumulativeContentBytes += block.accounting.rawBytes - previous;
    const metadata = mergeMetadata(block.metadata, parseMetadata(fragment.metadataJson));
    const size = jsonBytes(metadata); this.retainedBytes += size - block.metadataBytes;
    block.metadata = metadata; block.metadataBytes = size; block.ended = ended; this.check();
  }
  private toolCall(call: { readonly id: string; readonly name: string; readonly inputJson: string; readonly metadataJson: string }): ProviderStreamEvent {
    requireId(call.id); requireId(call.name);
    const prior = this.tools.get(call.id);
    if (prior !== undefined && (!prior.ended || prior.name !== call.name)) throw this.incomplete("tool_call");
    if (prior === undefined) this.identity("tool", call.id);
    if (!validProviderToolCallInputJson(call.inputJson)) throw new Error("invalid complete tool input");
    const metadata = mergeMetadata(prior?.metadata ?? {}, parseMetadata(call.metadataJson));
    const metadataJson = JSON.stringify(metadata);
    if (!validProviderMetadataJson(metadataJson)) throw new ProviderAssemblyLimitError("metadata_bytes");
    if (prior !== undefined) { this.tools.delete(call.id); this.retainedBytes -= prior.metadataBytes; }
    this.cumulativeContentBytes += encoder.encode(call.inputJson).byteLength + encoder.encode(metadataJson).byteLength;
    this.contentStarted = true; this.check();
    return this.frame({ type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL_COMPLETE,
      toolCallComplete: { modelToolCallId: call.id, name: call.name, inputJson: call.inputJson, providerMetadataJson: metadataJson } });
  }
  private mergeMetadata(block: TextBlock, incoming: string): void {
    if (block.kind === "text") return;
    const metadata = mergeMetadata(block.metadata, parseMetadata(incoming));
    const bytes = jsonBytes(metadata);
    if (bytes > MaxMetadataBytes) throw new ProviderAssemblyLimitError("metadata_bytes");
    if (block.kind === "reasoning") {
      const added = bytes - block.metadataBytes;
      this.retainedBytes += added; this.reasoningBytes += added; this.cumulativeContentBytes += Math.max(0, added);
      block.metadataBytes = bytes; this.checkReasoning();
    }
    block.metadata = metadata; this.check();
  }
  private identity(kind: string, id: string): void {
    requireId(id); const key = `${kind}:${id}`;
    if (this.seen.has(key)) throw this.incomplete(kind === "tool" ? "tool_call" : kind as "text" | "reasoning");
    this.seen.add(key); this.check();
  }
  private allocate(): string {
    const id = this.options.allocateEventId?.() ?? `evt_${Buffer.from(crypto.getRandomValues(new Uint8Array(16))).toString("hex")}`;
    if (!validProviderEventId(id) || this.eventIds.has(id)) throw new Error("invalid provider event identity");
    this.eventIds.add(id); return id;
  }
  private block(kind: "text" | "reasoning", id: string): TextBlock {
    const block = this.blocks.get(`${kind}:${id}`); if (block === undefined) throw this.incomplete(kind); return block;
  }
  private frame(value: Omit<ProviderStreamEvent, "frameSequence">): ProviderStreamEvent {
    const frame = { ...value, frameSequence: ++this.sequence };
    if (!validateProviderStreamEvent(frame).ok) throw new Error("invalid assembled provider frame");
    return frame;
  }
  private checkReasoning(): void {
    if (this.reasoningParts > MaxStableReasoningPartsPerRequest || this.reasoningBytes > MaxStableReasoningBytesPerRequest) throw new ProviderAssemblyLimitError("reasoning_budget");
  }
  private check(): void {
    const limits = this.options.bounds;
    if (this.retainedBytes > limits.maxRetainedBytes) throw new ProviderAssemblyLimitError("retained_bytes");
    if (this.blocks.size + this.tools.size > limits.maxOpenBlocks) throw new ProviderAssemblyLimitError("open_blocks");
    if (this.seen.size > limits.maxIdentities) throw new ProviderAssemblyLimitError("identities");
    if (this.segments > limits.maxSegments) throw new ProviderAssemblyLimitError("segments");
    this.observe();
  }
  private incomplete(category: ProviderIncompleteStreamCategory): ProviderIncompleteStreamError {
    let text = 0, reasoning = 0; for (const block of this.blocks.values()) block.kind === "text" ? text++ : reasoning++;
    return new ProviderIncompleteStreamError(category, { text, reasoning, toolInput: this.tools.size });
  }
  private offer(preview: Parameters<ProviderPreviewOffer>[0]): void {
    if (!this.previewAdmitted) return;
    try { this.options.offerPreview?.(preview); } catch { /* Lossy progress never changes complete delivery. */ }
  }
  private observe(): void { try { this.options.observeResources?.(this.resources); } catch { /* Diagnostics fail open. */ } }
}

/** Counts UTF-8 and canonical JSON bytes across split UTF-16 surrogate pairs. */
class ScalarTextAccounting {
  private bytes = 0;
  private json = 2;
  private pendingHigh = false;
  get rawBytes(): number { return this.bytes + (this.pendingHigh ? 3 : 0); }
  get jsonBytes(): number { return this.json + (this.pendingHigh ? 4 : 0); }
  append(value: string): void {
    for (let index = 0; index < value.length; index++) {
      const unit = value.charCodeAt(index);
      if (this.pendingHigh) {
        if (unit < 0xdc00 || unit > 0xdfff) throw new Error("invalid provider Unicode scalar");
        this.pendingHigh = false; this.bytes += 4; this.json += 4; continue;
      }
      if (unit >= 0xd800 && unit <= 0xdbff) { this.pendingHigh = true; continue; }
      if (unit >= 0xdc00 && unit <= 0xdfff) throw new Error("invalid provider Unicode scalar");
      this.bytes += unit <= 0x7f ? 1 : unit <= 0x7ff ? 2 : 3;
      this.json += unit === 0x22 || unit === 0x5c || unit === 8 || unit === 9 || unit === 10 || unit === 12 || unit === 13 ? 2 : unit < 0x20 ? 6 : unit <= 0x7f ? 1 : unit <= 0x7ff ? 2 : 3;
    }
  }
  finish(): void { if (this.pendingHigh) throw new Error("invalid completed provider Unicode scalar"); }
}
function requireId(value: string): void { if (value.length === 0 || encoder.encode(value).byteLength > MaxIdBytes) throw new Error("invalid normalized provider identifier"); }
function parseMetadata(value: string): Record<string, unknown> {
  if (!validProviderMetadataJson(value)) throw new ProviderAssemblyLimitError("metadata_bytes");
  return value === "" ? {} : JSON.parse(value) as Record<string, unknown>;
}
function jsonBytes(value: unknown): number { return encoder.encode(JSON.stringify(value)).byteLength; }
function mergeMetadata(existing: Record<string, unknown>, incoming: Record<string, unknown>): Record<string, unknown> {
  const result = { ...existing };
  for (const [key, value] of Object.entries(incoming)) {
    const prior = existing[key];
    Object.defineProperty(result, key, { value: isObject(prior) && isObject(value) ? { ...prior, ...value } : value, enumerable: true, configurable: true, writable: true });
  }
  if (jsonBytes(result) > MaxMetadataBytes) throw new ProviderAssemblyLimitError("metadata_bytes");
  return result;
}
function isObject(value: unknown): value is Record<string, unknown> { return typeof value === "object" && value !== null && !Array.isArray(value); }
