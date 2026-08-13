/**
 * Accumulates one provider request's uncommitted stream members. Durable
 * current-Turn state belongs to ThreadState's ThreadProcessor; this request-
 * local accumulator only freezes provider order and applies Bridge receipts.
 */
import { createHash } from "node:crypto";
import { MaxProviderRequestMessagePartJsonBytes } from "@tetral/gateway-protocol/src/bounds.js";
import type {
  DurableRuntimeMessage,
  RuntimeAssistantPartAppend,
  RuntimeBoundedJson,
  RuntimeDeclarationReceipt,
  RuntimeFailure,
  RuntimeInternalToolRepairCommit,
  RuntimeInternalToolRepairCommitResult,
  RuntimeJsonValue,
  RuntimeMessage,
  RuntimeMessageStoreError,
  RuntimePart,
  RuntimePartCreate,
  RuntimeProcessorSource,
  RuntimeToolSettlement,
  RuntimeToolSettlementDeclaration,
  RuntimeUsage,
  SessionEvent,
  SessionEventWriterAppendResult,
  SessionEventWriterError,
} from "../contracts/runtime.js";
import {
  DurableRuntimeMessageSchema,
  MaxStableReasoningBytesPerRequest,
  MaxStableReasoningPartsPerRequest,
  RuntimeAssistantPartAppendSchema,
  RuntimeBoundedJsonSchema,
  RuntimeBoundedTextSchema,
  RuntimeFailureSchema,
  RuntimePartCreateSchema,
  RuntimeToolSettlementDeclarationSchema,
  SessionEventSchema,
  boundRuntimeText,
  normalizeRuntimeMessageStoreError,
  normalizeSessionEventWriterError,
  runtimeToolErrorFromFailure,
  stableReasoningMetadataJSON,
} from "../contracts/runtime.js";
import type { LLMEvent } from "../llm/llm-event.js";
import {
  applyAssistantPartAppendReceipt,
  applyInternalToolRepairReceipt,
  applyInterruptInputReceipt,
  applyToolSettlementReceipt,
  runtimeInternalToolRepairCreate,
} from "./runtime-declaration.js";

export type ProviderStreamAccumulatorResult =
  | { readonly ok: true; readonly events: readonly SessionEvent[]; readonly durableEventIds?: readonly string[] }
  | { readonly ok: false; readonly events: readonly SessionEvent[]; readonly error: RuntimeFailure; readonly messageId?: string };

export type ToolUseCommitResult =
  | { readonly ok: true; readonly events: readonly SessionEvent[]; readonly toolUseEventId: string }
  | { readonly ok: false; readonly events: readonly SessionEvent[]; readonly error: RuntimeFailure; readonly messageId?: string };

export type PublicToolEvent =
  | { readonly kind: "tool" }
  | { readonly kind: "mcp"; readonly mcpServerName: string };

export type { RuntimeProcessorSource, RuntimeToolSettlement } from "../contracts/runtime.js";

/** Frozen provider-order member owned by one request-local sequencer. */
export interface FrozenAssistantPartAppend {
  readonly source: RuntimeProcessorSource;
  readonly append: RuntimeAssistantPartAppend;
  readonly event: Promise<SessionEvent>;
  readonly toolCallId?: string | undefined;
}

export interface MemberCommitHandle {
  readonly committed: Promise<ProviderStreamAccumulatorResult>;
}

export interface AssistantMemberSequencer {
  enqueue(append: FrozenAssistantPartAppend): MemberCommitHandle;
  awaitDrained(): Promise<void>;
}

/** Serializes immutable Assistant members by provider observation order. */
export class RequestAssistantMemberSequencer implements AssistantMemberSequencer {
  private tail: Promise<void> = Promise.resolve();

  constructor(
    private readonly commit: (append: FrozenAssistantPartAppend) => Promise<ProviderStreamAccumulatorResult>,
  ) {}

  enqueue(append: FrozenAssistantPartAppend): MemberCommitHandle {
    const committed = this.tail.then(() => this.commit(append));
    this.tail = committed.then(() => undefined, () => undefined);
    return { committed };
  }

  async awaitDrained(): Promise<void> {
    await this.tail;
  }
}

export interface ProviderStreamAccumulatorWriter {
  readonly appendEvent: (
    event: SessionEvent,
    source: RuntimeProcessorSource,
    declaration?:
      | { readonly assistantPartAppend: RuntimeAssistantPartAppend }
      | { readonly toolSettlement: RuntimeToolSettlementDeclaration },
    modelRequestId?: string,
    serverToolUse?: { readonly webSearchRequests: number; readonly webFetchRequests: number },
    mcpMaterializationHandle?: string,
    sandboxResultDigest?: string,
  ) => Promise<SessionEventWriterAppendResult>;
  readonly commitInternalToolRepair: (
    repair: RuntimeInternalToolRepairCommit,
    source: RuntimeProcessorSource,
  ) => Promise<RuntimeInternalToolRepairCommitResult>;
}

export interface ProviderStreamAccumulatorOptions {
  readonly modelRequestId: string;
  readonly requestId: string;
  readonly workspaceId: string;
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly bindingId: string;
  readonly bindingGeneration: number;
  readonly targetPodUid: string;
  readonly durableMessages?: readonly DurableRuntimeMessage[] | undefined;
  /** Existing Assistant Message to mutate only while resuming its named durable request. */
  readonly resumedAssistantMessageId?: string | undefined;
  readonly maxNormalizedTextPreviewBytes?: number;
  readonly now: () => string;
  readonly writer: ProviderStreamAccumulatorWriter;
  readonly onInternalToolRepairCommitted?: (fact: {
    readonly eventId: string;
    readonly modelRequestId: string;
    readonly modelToolCallId: string;
    readonly toolName: string;
  }) => void;
  readonly onToolResultCommitted?: (fact: {
    readonly eventId: string;
    readonly toolUseEventId: string;
    readonly outcome: "success" | "error" | "cancelled";
  }) => void;
}

type TextPartCreate = Extract<RuntimePartCreate, { readonly type: "text" }>;
type ReasoningPartCreate = Extract<RuntimePartCreate, { readonly type: "reasoning" }>;
type ToolPartCreate = Extract<RuntimePartCreate, { readonly type: "tool" }>;
type ReasoningProviderMetadata = NonNullable<ReasoningPartCreate["providerMetadata"]>;

interface LLMEventEnvelope extends RuntimeProcessorSource { readonly event: LLMEvent }

type EnsureToolPartResult =
  | { readonly ok: true; readonly events: readonly SessionEvent[]; readonly part: ToolPartCreate }
  | { readonly ok: false; readonly events: readonly SessionEvent[]; readonly error: RuntimeFailure; readonly messageId?: string };

/**
 * Request-local provider stream accumulator. It never owns a complete
 * Assistant Message: only unreceipted prefix members and current Tool shells
 * live here, and every successful write is installed from its positional ACK.
 */
export class ProviderStreamAccumulator {
  private readonly options: ProviderStreamAccumulatorOptions;
  private readonly reasoningParts = new Map<string, ReasoningPartCreate>();
  private readonly toolParts = new Map<string, ToolPartCreate>();
  private readonly toolUseEventIds = new Map<string, string>();
  private readonly toolEvents = new Map<string, PublicToolEvent>();
  private pendingPrefix: RuntimePartCreate[] = [];
  private durableProjection: DurableRuntimeMessage[];
  private assistantMessageId: string | undefined;
  private activeTextPart: TextPartCreate | undefined;
  private activeStepIndex = 0;
  private semanticMemberCount = 0;
  private terminal = false;
  private readonly memberSequencer: RequestAssistantMemberSequencer;
  private readonly reservedToolMembers = new Map<string, {
    readonly authorize: (event: SessionEvent) => void;
    readonly committed: Promise<ProviderStreamAccumulatorResult>;
  }>();

  constructor(options: ProviderStreamAccumulatorOptions) {
    this.options = options;
    this.memberSequencer = new RequestAssistantMemberSequencer(async (frozen) => {
      const event = await frozen.event;
      const result = await this.options.writer.appendEvent(event, frozen.source, { assistantPartAppend: frozen.append }, this.options.modelRequestId);
      if (!result.ok) return { ok: false, events: [], error: eventWriterFailure(result.error) };
      if (!this.applyMemberAppend(result, event, frozen.append)) return declarationApplicationFailure(result.writeId);
      return { ok: true, events: [event], durableEventIds: [result.eventId] };
    });
    this.durableProjection = [...(options.durableMessages ?? [])];
    const assistant = options.resumedAssistantMessageId === undefined
      ? undefined
      : this.durableProjection.find((message) => message.id === options.resumedAssistantMessageId);
    if (assistant !== undefined) this.installDurableAssistant(assistant);
  }

  messages(): readonly RuntimeMessage[] { return [...this.durableProjection]; }

  async awaitAssistantMembersDrained(): Promise<void> {
    await this.memberSequencer.awaitDrained();
  }

  /** Returns and clears only the trailing receiptable reasoning/boundary prefix. */
  requestEndAppend(): RuntimeAssistantPartAppend | undefined {
    if (!this.terminal) throw new Error("request end append requires a terminal provider stream");
    if (this.pendingPrefix.length === 0) return undefined;
    return RuntimeAssistantPartAppendSchema.parse({ parts: [...this.pendingPrefix] });
  }

  discardUnreceiptedMembers(): void {
    this.pendingPrefix = [];
    this.activeTextPart = undefined;
    this.reasoningParts.clear();
  }

  /** Applies the optional trailing append after the outer Request End ACK. */
  applyRequestEndAppend(
    eventId: string,
    append: RuntimeAssistantPartAppend | undefined,
    declaration: NonNullable<Extract<SessionEventWriterAppendResult, { readonly ok: true }>["declaration"]>,
    seal: {
      readonly status: "completed" | "failed";
      readonly finishReason: RuntimeMessage["finishReason"];
      readonly usage?: RuntimeUsage | undefined;
    },
  ): boolean {
    if (declaration.applicationDisposition !== "current_custody") return false;
    if (append === undefined) {
      if (declaration.receipt.messages.length !== 0) return false;
      this.pendingPrefix = [];
      this.applyAssistantSeal(seal);
      return true;
    }
    try {
      const durable = applyAssistantPartAppendReceipt({
        sessionId: this.options.sessionId,
        sessionThreadId: this.options.sessionThreadId,
        operationKind: "write_request_end",
        sourceKind: "model_request",
        operationId: this.options.modelRequestId,
        eventId,
        append,
        existingMessage: this.currentAssistantMessage(),
      }, declaration.receipt);
      this.installDurableAssistant(durable);
      this.pendingPrefix = [];
      this.applyAssistantSeal(seal);
      return true;
    } catch {
      return false;
    }
  }

  private applyAssistantSeal(seal: {
    readonly status: "completed" | "failed";
    readonly finishReason: RuntimeMessage["finishReason"];
    readonly usage?: RuntimeUsage | undefined;
  }): void {
    if (this.assistantMessageId === undefined) return;
    this.durableProjection = this.durableProjection.map((message) =>
      message.id === this.assistantMessageId
        ? DurableRuntimeMessageSchema.parse({
            ...message,
            status: seal.status,
            finishReason: seal.finishReason,
            ...(seal.usage === undefined ? {} : { usage: seal.usage }),
          })
        : message
    );
  }

  async process(envelope: LLMEventEnvelope): Promise<ProviderStreamAccumulatorResult> {
    if (this.terminal) return this.failWithoutWrites(protocolSequenceFailure());
    switch (envelope.event.type) {
      case "step-start": return this.startStep(envelope as LLMEventEnvelope & { readonly event: Extract<LLMEvent, { type: "step-start" }> });
      case "step-finish": return this.finishStep(envelope as LLMEventEnvelope & { readonly event: Extract<LLMEvent, { type: "step-finish" }> });
      case "text-start": return this.startText(envelope as LLMEventEnvelope & { readonly event: Extract<LLMEvent, { type: "text-start" }> });
      case "text-delta": return this.appendText(envelope as LLMEventEnvelope & { readonly event: Extract<LLMEvent, { type: "text-delta" }> });
      case "text-end": return await this.endText(envelope as LLMEventEnvelope & { readonly event: Extract<LLMEvent, { type: "text-end" }> });
      case "reasoning-start": return await this.startReasoning(envelope as LLMEventEnvelope & { readonly event: Extract<LLMEvent, { type: "reasoning-start" }> });
      case "reasoning-delta": return this.appendReasoning(envelope as LLMEventEnvelope & { readonly event: Extract<LLMEvent, { type: "reasoning-delta" }> });
      case "reasoning-end": return this.endReasoning(envelope as LLMEventEnvelope & { readonly event: Extract<LLMEvent, { type: "reasoning-end" }> });
      case "tool-input-start": return this.startToolInput(envelope as LLMEventEnvelope & { readonly event: Extract<LLMEvent, { type: "tool-input-start" }> });
      case "tool-input-delta": case "tool-input-end": case "attachment-rejections": return { ok: true, events: [] };
      case "tool-call": return this.startToolCall(envelope as LLMEventEnvelope & { readonly event: Extract<LLMEvent, { type: "tool-call" }> });
      case "finish": return this.finish(envelope as LLMEventEnvelope & { readonly event: Extract<LLMEvent, { type: "finish" }> });
      case "provider-error": return await this.terminalFailure(envelope, envelope.event.error, false);
    }
  }

  async cancel(source: RuntimeProcessorSource, failure: RuntimeFailure): Promise<ProviderStreamAccumulatorResult> {
    if (this.terminal) return this.failWithoutWrites(protocolSequenceFailure());
    return await this.terminalFailure(source, failure, false);
  }

  async cancelOpenTools(
    source: RuntimeProcessorSource,
    failure: RuntimeFailure,
    externallyOwnedToolUseEventIds: ReadonlySet<string> = new Set(),
  ): Promise<ProviderStreamAccumulatorResult> {
    const events: SessionEvent[] = [];
    const durableEventIds: string[] = [];
    for (const [toolCallId, part] of this.toolParts) {
      if (
        part.state.status !== "running" || part.toolUseEventId === undefined ||
        externallyOwnedToolUseEventIds.has(part.toolUseEventId)
      ) continue;
      const result = await this.commitToolSettlement(source, toolCallId, { type: "cancelled", error: failure });
      if (!result.ok) return result;
      events.push(...result.events);
      durableEventIds.push(...(result.durableEventIds ?? []));
    }
    return { ok: true, events, ...(durableEventIds.length === 0 ? {} : { durableEventIds }) };
  }

  /** Interrupt intent carries no Tool census; Bridge owns every outcome. */
  prepareInterruptSettlement(interrupt: { readonly eventIds: readonly string[] }, _failure: RuntimeFailure): void {
    if (this.terminal || interrupt.eventIds.length !== 1) {
      throw new Error("interrupt settlement requires one open request and one admitted event");
    }
    this.terminal = true;
    this.discardUnreceiptedMembers();
  }

  applyInterruptSettlement(
    interrupt: { readonly runtimeInputId: string; readonly eventIds: readonly string[] },
    receipt: RuntimeDeclarationReceipt,
  ): void {
    const expected = this.unfinishedDurableTools()
      .sort((left, right) => left.sequence - right.sequence)
      .map((part) => part.toolUseEventId!);
    const projections = applyInterruptInputReceipt({
      sessionThreadId: this.options.sessionThreadId,
      runtimeInputId: interrupt.runtimeInputId,
      eventIds: interrupt.eventIds,
      expectedToolUseEventIds: expected,
    }, receipt);
    const updated = new Map<string, RuntimePart>();
    for (const projection of projections) {
      const existing = this.findDurableTool(projection.toolUseEventId);
      if (existing === undefined || isTerminalTool(existing)) throw new Error("interrupt projection has no unfinished hot Tool");
      const state = projection.terminalState.type === "error"
        ? { status: "error" as const, ...(existing.state.status === "running" ? { input: existing.state.input } : {}), error: projection.terminalState.error }
        : { status: "cancelled" as const, ...(existing.state.status === "running" ? { input: existing.state.input } : {}), ...(projection.terminalState.error === undefined ? {} : { error: projection.terminalState.error }) };
      updated.set(existing.id, { ...existing, state } as RuntimePart);
      this.options.onToolResultCommitted?.({
        eventId: projection.resultEvent.eventId,
        toolUseEventId: projection.toolUseEventId,
        outcome: projection.terminalState.type === "error" ? "error" : "cancelled",
      });
    }
    this.replaceDurableParts(updated);
  }

  async commitPublicToolUse(
    source: RuntimeProcessorSource,
    toolCallId: string,
    input: RuntimeJsonValue,
    evaluatedPermission: "allow" | "ask" | "deny",
    toolEvent: PublicToolEvent = { kind: "tool" },
  ): Promise<ToolUseCommitResult> {
    const existing = this.toolParts.get(toolCallId);
    if (existing === undefined || existing.state.status !== "running") return { ok: false, events: [], error: protocolSequenceFailure() };
    const prior = this.toolUseEventIds.get(toolCallId);
    if (prior !== undefined) return { ok: true, events: [], toolUseEventId: prior };
    const reserved = this.reservedToolMembers.get(toolCallId);
    if (reserved === undefined) return { ok: false, events: [], error: protocolSequenceFailure() };
    const event = toolUseSessionEvent(existing, input, evaluatedPermission, toolEvent);
    reserved.authorize(event);
    const committed = await reserved.committed;
    if (!committed.ok) return committed;
    const eventId = committed.durableEventIds?.[0];
    if (eventId === undefined) return { ok: false, events: [], error: protocolSequenceFailure() };
    this.reservedToolMembers.delete(toolCallId);
    const tool = parseToolPart({ ...existing, toolEvent });
    const stamped = parseToolPart({ ...tool, toolUseEventId: eventId, toolEvent });
    this.toolParts.set(toolCallId, stamped);
    this.toolUseEventIds.set(toolCallId, eventId);
    this.toolEvents.set(toolCallId, toolEvent);
    this.replaceDurableToolEventIdentity(toolCallId, eventId, toolEvent);
    return { ok: true, events: committed.events, toolUseEventId: eventId };
  }

  /** Reserves a Tool member at its provider position before permission work begins. */
  reservePublicToolUse(source: RuntimeProcessorSource, toolCallId: string, toolEvent: PublicToolEvent): boolean {
    if (this.reservedToolMembers.has(toolCallId)) return true;
    const existing = this.toolParts.get(toolCallId);
    if (existing === undefined || existing.state.status !== "running") return false;
    let authorize!: (event: SessionEvent) => void;
    const event = new Promise<SessionEvent>((resolve) => { authorize = resolve; });
    const tool = parseToolPart({ ...existing, toolEvent });
    const append = RuntimeAssistantPartAppendSchema.parse({ parts: [...this.takePendingPrefix(), tool] });
    const handle = this.memberSequencer.enqueue({ source, append, event, toolCallId });
    this.reservedToolMembers.set(toolCallId, { authorize, committed: handle.committed });
    this.semanticMemberCount++;
    return true;
  }

  async commitToolSettlement(
    source: RuntimeProcessorSource,
    toolCallId: string,
    settlement: RuntimeToolSettlement,
  ): Promise<ProviderStreamAccumulatorResult> {
    const existing = this.toolParts.get(toolCallId);
    const toolUseEventId = existing?.toolUseEventId ?? this.toolUseEventIds.get(toolCallId);
    if (existing === undefined || existing.state.status !== "running" || toolUseEventId === undefined) {
      return { ok: false, events: [], error: semanticSequenceFailure() };
    }
    const event = runtimeToolResultEvent(toolUseEventId, this.toolEvents.get(toolCallId) ?? existing.toolEvent ?? { kind: "tool" }, settlement);
    const declaration = RuntimeToolSettlementDeclarationSchema.parse({ toolUseEventId, outcome: settlement });
    const result = await this.options.writer.appendEvent(
      event,
      source,
      { toolSettlement: declaration },
      this.options.modelRequestId,
      settlement.type === "completed" || settlement.type === "error" ? settlement.serverToolUse : undefined,
      settlement.type === "completed" || settlement.type === "error" ? settlement.mcpMaterializationHandle : undefined,
      settlement.sandboxResultDigest,
    );
    if (!result.ok) return { ok: false, events: [], error: eventWriterFailure(result.error) };
    if (result.declaration?.applicationDisposition !== "current_custody") return declarationApplicationFailure(result.writeId);
    try {
      applyToolSettlementReceipt({
        sessionThreadId: this.options.sessionThreadId,
        operationKind: "write_event",
        sourceKind: event.type,
        operationId: result.writeId,
        eventId: result.eventId,
        settlement: declaration,
      }, result.declaration.receipt);
    } catch {
      return declarationApplicationFailure(result.writeId);
    }
    const terminalPart = terminalToolPart(existing, settlement, result.processedAt);
    this.toolParts.set(toolCallId, terminalPart);
    this.replaceDurableTool(toolUseEventId, terminalPart);
    const outcome = settlement.type === "completed" ? "success" : settlement.type;
    this.options.onToolResultCommitted?.({ eventId: result.eventId, toolUseEventId, outcome });
    return { ok: true, events: [event], durableEventIds: [result.eventId] };
  }

  async commitInternalToolRepair(
    source: RuntimeProcessorSource,
    toolCallId: string,
    modelRequestId: string,
    repairKey: string,
    failure: RuntimeFailure,
  ): Promise<ProviderStreamAccumulatorResult> {
    const existing = this.toolParts.get(toolCallId);
    if (existing === undefined || existing.state.status !== "running") return { ok: false, events: [], error: semanticSequenceFailure() };
    const repaired = parseToolPart({
      type: "tool", toolCallId: existing.toolCallId, toolName: existing.toolName,
      state: { status: "error", input: existing.state.input, error: runtimeToolErrorFromFailure(failure) },
      ...(existing.startedAt === undefined ? {} : { startedAt: existing.startedAt }), completedAt: this.options.now(),
    });
    const messageCreate = runtimeInternalToolRepairCreate({ part: repaired });
    const commit = await this.options.writer.commitInternalToolRepair({
      requestId: this.options.requestId,
      workspaceId: this.options.workspaceId,
      sessionId: this.options.sessionId,
      sessionThreadId: this.options.sessionThreadId,
      bindingId: this.options.bindingId,
      bindingGeneration: this.options.bindingGeneration,
      targetPodUid: this.options.targetPodUid,
      modelRequestId,
      modelToolCallId: toolCallId,
      toolName: existing.toolName,
      repairKey,
      messageCreate,
    }, source);
    if (!commit.ok) return { ok: false, events: [], error: storeFailure(commit.error) };
    if (commit.declaration.applicationDisposition !== "current_custody") return { ok: false, events: [], error: storeFailure(normalizeRuntimeMessageStoreError({ code: "unavailable", operation: "commitInternalToolRepair", reason: "runtime_contract_validation", sessionId: this.options.sessionId })) };
    try {
      const message = applyInternalToolRepairReceipt({ sessionId: this.options.sessionId, sessionThreadId: this.options.sessionThreadId, repairKey, eventId: commit.eventId, create: messageCreate }, commit.declaration.receipt);
      this.durableProjection = upsertMessage(this.durableProjection, message);
    } catch {
      return { ok: false, events: [], error: storeFailure(normalizeRuntimeMessageStoreError({ code: "schema_mismatch", operation: "commitInternalToolRepair", reason: "runtime_contract_validation", sessionId: this.options.sessionId })) };
    }
    this.options.onInternalToolRepairCommitted?.({ eventId: commit.eventId, modelRequestId, modelToolCallId: toolCallId, toolName: existing.toolName });
    this.semanticMemberCount++;
    return { ok: true, events: [], durableEventIds: [commit.eventId] };
  }

  sessionStatus(status: { readonly type: "idle"; readonly stopReason?: { readonly type: "end_turn" } | { readonly type: "requires_action"; readonly event_ids: string[] } | { readonly type: "retries_exhausted" } } | { readonly type: "busy" } | { readonly type: "retry" }): SessionEvent {
    return status.type === "idle"
      ? SessionEventSchema.parse({ type: "session.status_idle", stop_reason: status.stopReason ?? { type: "end_turn" } })
      : SessionEventSchema.parse({ type: "session.status_running" });
  }

  private startStep(envelope: LLMEventEnvelope & { readonly event: Extract<LLMEvent, { type: "step-start" }> }): ProviderStreamAccumulatorResult {
    this.activeStepIndex = envelope.event.stepIndex ?? this.activeStepIndex + 1;
    this.pendingPrefix.push(RuntimePartCreateSchema.parse({ type: "step-start", stepIndex: this.activeStepIndex }));
    return { ok: true, events: [] };
  }

  private finishStep(envelope: LLMEventEnvelope & { readonly event: Extract<LLMEvent, { type: "step-finish" }> }): ProviderStreamAccumulatorResult {
    this.pendingPrefix.push(RuntimePartCreateSchema.parse({
      type: "step-finish", stepIndex: this.activeStepIndex === 0 ? undefined : this.activeStepIndex,
      finishReason: envelope.event.finishReason ?? "unknown",
      ...(envelope.event.usage === undefined ? {} : { usage: envelope.event.usage }),
    }));
    return { ok: true, events: [] };
  }

  private startText(_envelope: LLMEventEnvelope): ProviderStreamAccumulatorResult {
    if (this.activeTextPart === undefined) this.activeTextPart = parseTextPart({ type: "text", text: "", truncated: false, status: "streaming", startedAt: this.options.now() });
    return { ok: true, events: [] };
  }

  private appendText(envelope: LLMEventEnvelope & { readonly event: Extract<LLMEvent, { type: "text-delta" }> }): ProviderStreamAccumulatorResult {
    if (this.activeTextPart === undefined) return { ok: true, events: [] };
    const text = `${this.activeTextPart.text}${envelope.event.text_delta}`;
    if (!withinJsonStringBudget(text, MaxProviderRequestMessagePartJsonBytes)) return this.failWithoutWrites(boundedSemanticFailure());
    this.activeTextPart = parseTextPart({ ...this.activeTextPart, text });
    return { ok: true, events: [] };
  }

  private async endText(envelope: LLMEventEnvelope): Promise<ProviderStreamAccumulatorResult> {
    if (this.activeTextPart === undefined) return { ok: true, events: [] };
    const completed = parseTextPart({ ...this.activeTextPart, status: "completed", completedAt: this.options.now() });
    this.activeTextPart = undefined;
    if (completed.text.length === 0) return { ok: true, events: [] };
    const event = SessionEventSchema.parse({ type: "agent.message", content: [{ type: "text", text: completed.text }] });
    const append = RuntimeAssistantPartAppendSchema.parse({ parts: [...this.takePendingPrefix(), completed] });
    this.semanticMemberCount++;
    return await this.memberSequencer.enqueue({ event: Promise.resolve(event), source: envelope, append }).committed;
  }

  private async startReasoning(envelope: LLMEventEnvelope & { readonly event: Extract<LLMEvent, { type: "reasoning-start" }> }): Promise<ProviderStreamAccumulatorResult> {
    if (this.reasoningParts.has(envelope.event.id)) return { ok: true, events: [] };
    const part = parseReasoningPart({
      type: "reasoning", providerPartId: envelope.event.id,
      ...(envelope.event.providerMetadata === undefined ? {} : { providerMetadata: envelope.event.providerMetadata }),
      text: "", truncated: false, status: "streaming", startedAt: this.options.now(),
    });
    if (!this.reasoningSetFits(part)) return this.failWithoutWrites(boundedSemanticFailure());
    const event = SessionEventSchema.parse({ type: "agent.thinking" });
    const result = await this.options.writer.appendEvent(event, envelope);
    if (!result.ok) return { ok: false, events: [], error: eventWriterFailure(result.error) };
    this.reasoningParts.set(envelope.event.id, part);
    return { ok: true, events: [event], durableEventIds: [result.eventId] };
  }

  private appendReasoning(envelope: LLMEventEnvelope & { readonly event: Extract<LLMEvent, { type: "reasoning-delta" }> }): ProviderStreamAccumulatorResult {
    const part = this.reasoningParts.get(envelope.event.id);
    if (part === undefined) return { ok: true, events: [] };
    const metadata = mergeProviderMetadata(part.providerMetadata, envelope.event.providerMetadata);
    const updated = parseReasoningPart({ ...part, ...(metadata === undefined ? {} : { providerMetadata: metadata }), text: `${part.text}${envelope.event.text_delta}` });
    if (!withinJsonStringBudget(updated.text, MaxProviderRequestMessagePartJsonBytes) || !this.reasoningSetFits(updated)) return this.failWithoutWrites(boundedSemanticFailure());
    this.reasoningParts.set(envelope.event.id, updated);
    return { ok: true, events: [] };
  }

  private endReasoning(envelope: LLMEventEnvelope & { readonly event: Extract<LLMEvent, { type: "reasoning-end" }> }): ProviderStreamAccumulatorResult {
    const part = this.reasoningParts.get(envelope.event.id);
    if (part === undefined) return { ok: true, events: [] };
    const metadata = mergeProviderMetadata(part.providerMetadata, envelope.event.providerMetadata);
    const completed = parseReasoningPart({ ...part, ...(metadata === undefined ? {} : { providerMetadata: metadata }), status: "completed", completedAt: this.options.now() });
    if (!this.reasoningSetFits(completed)) return this.failWithoutWrites(boundedSemanticFailure());
    this.reasoningParts.delete(envelope.event.id);
    this.pendingPrefix.push(completed);
    return { ok: true, events: [] };
  }

  private startToolInput(envelope: LLMEventEnvelope & { readonly event: Extract<LLMEvent, { type: "tool-input-start" }> }): ProviderStreamAccumulatorResult {
    if (!this.toolParts.has(envelope.event.id)) this.toolParts.set(envelope.event.id, parseToolPart({ type: "tool", toolCallId: envelope.event.id, toolName: envelope.event.toolName, state: { status: "pending" }, startedAt: this.options.now() }));
    return { ok: true, events: [] };
  }

  private startToolCall(envelope: LLMEventEnvelope & { readonly event: Extract<LLMEvent, { type: "tool-call" }> }): ProviderStreamAccumulatorResult {
    const ensured = this.ensureToolPart(envelope.event.id, envelope.event.toolName);
    if (!ensured.ok) return ensured;
    const updated = parseToolPart({ ...ensured.part, toolName: envelope.event.toolName, state: { status: "running", input: runtimeJsonFromProvider(envelope.event.input, envelope.event.inputPreview, this.maxBytes()) } });
    this.toolParts.set(envelope.event.id, updated);
    return { ok: true, events: [] };
  }

  private finish(envelope: LLMEventEnvelope & { readonly event: Extract<LLMEvent, { type: "finish" }> }): ProviderStreamAccumulatorResult {
    if (
      this.activeTextPart !== undefined ||
      this.reasoningParts.size > 0 ||
      (this.semanticMemberCount === 0 && this.pendingPrefix.length === 0)
    ) return this.failWithoutWrites(semanticSequenceFailure());
    this.terminal = true;
    return { ok: true, events: [] };
  }

  private async terminalFailure(source: RuntimeProcessorSource, failure: RuntimeFailure, settleTools: boolean): Promise<ProviderStreamAccumulatorResult> {
    this.discardUnreceiptedMembers();
    if (settleTools) {
      for (const [toolCallId, part] of this.toolParts) {
        if (part.state.status === "running" && part.toolUseEventId !== undefined) {
          const result = await this.commitToolSettlement(source, toolCallId, { type: "cancelled", error: failure });
          if (!result.ok) return result;
        }
      }
    }
    this.terminal = true;
    return { ok: true, events: [SessionEventSchema.parse({ type: "session.error", error: failure })] };
  }

  private ensureToolPart(toolCallId: string, toolName: string): EnsureToolPartResult {
    const existing = this.toolParts.get(toolCallId);
    if (existing !== undefined) return { ok: true, events: [], part: existing };
    const part = parseToolPart({ type: "tool", toolCallId, toolName, state: { status: "pending" }, startedAt: this.options.now() });
    this.toolParts.set(toolCallId, part);
    return { ok: true, events: [], part };
  }

  private applyMemberAppend(result: Extract<SessionEventWriterAppendResult, { ok: true }>, event: SessionEvent, append: RuntimeAssistantPartAppend): boolean {
    if (result.declaration?.applicationDisposition !== "current_custody") return false;
    try {
      const durable = applyAssistantPartAppendReceipt({
        sessionId: this.options.sessionId,
        sessionThreadId: this.options.sessionThreadId,
        operationKind: "write_event",
        sourceKind: event.type,
        operationId: result.writeId,
        eventId: result.eventId,
        append,
        existingMessage: this.currentAssistantMessage(),
      }, result.declaration.receipt);
      this.installDurableAssistant(durable);
      return true;
    } catch {
      return false;
    }
  }

  private installDurableAssistant(message: DurableRuntimeMessage): void {
    this.assistantMessageId = message.id;
    this.durableProjection = upsertMessage(this.durableProjection, DurableRuntimeMessageSchema.parse(message));
    for (const part of message.parts) {
      if (part.type !== "tool") continue;
      this.toolParts.set(part.toolCallId, parseToolPart(partCreateFromDurable(part)));
      if (part.toolUseEventId !== undefined) this.toolUseEventIds.set(part.toolCallId, part.toolUseEventId);
      if (part.toolEvent !== undefined) this.toolEvents.set(part.toolCallId, part.toolEvent);
    }
  }

  private currentAssistantMessage(): DurableRuntimeMessage | undefined {
    return this.assistantMessageId === undefined ? undefined : this.durableProjection.find((message) => message.id === this.assistantMessageId);
  }

  private replaceDurableToolEventIdentity(toolCallId: string, eventId: string, toolEvent: PublicToolEvent): void {
    const assistant = this.currentAssistantMessage();
    if (assistant === undefined) return;
    this.installDurableAssistant(DurableRuntimeMessageSchema.parse({
      ...assistant,
      parts: assistant.parts.map((part) => part.type === "tool" && part.toolCallId === toolCallId ? { ...part, toolUseEventId: eventId, toolEvent } : part),
    }));
  }

  private replaceDurableTool(toolUseEventId: string, next: ToolPartCreate): void {
    const assistant = this.currentAssistantMessage();
    if (assistant === undefined) return;
    this.installDurableAssistant(DurableRuntimeMessageSchema.parse({
      ...assistant,
      parts: assistant.parts.map((part) => part.type === "tool" && part.toolUseEventId === toolUseEventId ? { ...part, ...next, id: part.id, sessionId: part.sessionId, messageId: part.messageId, sequence: part.sequence, createdAt: part.createdAt } : part),
    }));
  }

  private replaceDurableParts(updates: ReadonlyMap<string, RuntimePart>): void {
    this.durableProjection = this.durableProjection.map((message) => DurableRuntimeMessageSchema.parse({
      ...message, parts: message.parts.map((part) => updates.get(part.id) ?? part),
    }));
  }

  private findDurableTool(toolUseEventId: string): Extract<RuntimePart, { type: "tool" }> | undefined {
    for (const message of this.durableProjection) for (const part of message.parts) if (part.type === "tool" && part.toolUseEventId === toolUseEventId) return part;
    return undefined;
  }

  private unfinishedDurableTools(): Extract<RuntimePart, { type: "tool" }>[] {
    return this.durableProjection.flatMap((message) => message.parts.filter((part): part is Extract<RuntimePart, { type: "tool" }> => part.type === "tool" && part.toolUseEventId !== undefined && !isTerminalTool(part)));
  }

  private takePendingPrefix(): RuntimePartCreate[] {
    const prefix = this.pendingPrefix;
    this.pendingPrefix = [];
    return prefix;
  }

  private reasoningSetFits(candidate: ReasoningPartCreate): boolean {
    const parts = [
      ...this.pendingPrefix.filter((part): part is ReasoningPartCreate => part.type === "reasoning"),
      ...[...this.reasoningParts.values()].filter((part) => part.providerPartId !== candidate.providerPartId),
      candidate,
    ];
    if (parts.length > MaxStableReasoningPartsPerRequest) return false;
    return parts.reduce((total, part) => total + byteLength(part.text) + byteLength(stableReasoningMetadataJSON(part.providerMetadata)), 0) <= MaxStableReasoningBytesPerRequest;
  }

  private failWithoutWrites(error: RuntimeFailure): ProviderStreamAccumulatorResult {
    this.discardUnreceiptedMembers();
    return { ok: false, events: [], error };
  }

  private maxBytes(): number { return this.options.maxNormalizedTextPreviewBytes ?? 8_192; }
}

/** Deterministic idempotency key for one invalid internal Tool-call repair. */
export function internalToolRepairKey(modelRequestId: string, modelToolCallId: string, toolName: string): string {
  const hash = createHash("sha256");
  for (const value of [modelRequestId, modelToolCallId, toolName]) {
    hash.update(String(Buffer.byteLength(value, "utf8")), "ascii"); hash.update(":", "ascii"); hash.update(value, "utf8");
  }
  return `internal_invalid_tool_${hash.digest("hex")}`;
}

function parseTextPart(value: unknown): TextPartCreate {
  const part = RuntimePartCreateSchema.parse(value); if (part.type !== "text") throw new Error("expected text part"); return part;
}
function parseReasoningPart(value: unknown): ReasoningPartCreate {
  const part = RuntimePartCreateSchema.parse(value); if (part.type !== "reasoning") throw new Error("expected reasoning part"); return part;
}
function parseToolPart(value: unknown): ToolPartCreate {
  const part = RuntimePartCreateSchema.parse(value); if (part.type !== "tool") throw new Error("expected Tool part"); return part;
}

function partCreateFromDurable(part: RuntimePart): RuntimePartCreate {
  const { id: _id, sessionId: _sessionId, messageId: _messageId, sequence: _sequence, createdAt: _createdAt, updatedAt: _updatedAt, ...value } = part;
  return RuntimePartCreateSchema.parse(value);
}

function runtimeJsonFromProvider(value: RuntimeJsonValue, preview: Extract<LLMEvent, { type: "tool-call" }>["inputPreview"], maxBytes: number): RuntimeBoundedJson {
  const bounded = boundRuntimeText(preview.preview, maxBytes);
  return RuntimeBoundedJsonSchema.parse({ value, preview: bounded.text, truncated: preview.truncated || bounded.truncated });
}

function toolUseSessionEvent(part: ToolPartCreate, input: RuntimeJsonValue, permission: "allow" | "ask" | "deny", toolEvent: PublicToolEvent): SessionEvent {
  if (!isRuntimeJsonObject(input)) throw new Error("public Tool input must be an object");
  return toolEvent.kind === "mcp"
    ? SessionEventSchema.parse({ type: "agent.mcp_tool_use", name: part.toolName, input, mcp_server_name: toolEvent.mcpServerName, evaluated_permission: permission })
    : SessionEventSchema.parse({ type: "agent.tool_use", name: part.toolName, input, evaluated_permission: permission });
}

export function runtimeToolResultEvent(toolUseEventId: string, toolEvent: PublicToolEvent, settlement: RuntimeToolSettlement): SessionEvent {
  const content = settlement.type === "completed"
    ? [{ type: "text" as const, text: settlement.output.text }]
    : settlement.type === "error" ? [{ type: "text" as const, text: settlement.error.message }] : undefined;
  const isError = settlement.type === "completed" ? undefined : true;
  return toolEvent.kind === "mcp"
    ? SessionEventSchema.parse({ type: "agent.mcp_tool_result", mcp_tool_use_id: toolUseEventId, ...(content === undefined ? {} : { content }), ...(isError === undefined ? {} : { is_error: isError }) })
    : SessionEventSchema.parse({ type: "agent.tool_result", tool_use_id: toolUseEventId, ...(content === undefined ? {} : { content }), ...(isError === undefined ? {} : { is_error: isError }) });
}

function terminalToolPart(part: ToolPartCreate, settlement: RuntimeToolSettlement, completedAt: string): ToolPartCreate {
  if (part.state.status !== "running") throw new Error("terminal settlement requires a running Tool");
  if (settlement.type === "completed") return parseToolPart({ ...part, state: { status: "completed", input: part.state.input, output: RuntimeBoundedTextSchema.parse(settlement.output) }, completedAt });
  if (settlement.type === "error") return parseToolPart({ ...part, state: { status: "error", input: part.state.input, error: runtimeToolErrorFromFailure(settlement.error) }, completedAt });
  return parseToolPart({ ...part, state: { status: "cancelled", input: part.state.input, ...(settlement.error === undefined ? {} : { error: runtimeToolErrorFromFailure(settlement.error) }) }, completedAt });
}

function isTerminalTool(part: Extract<RuntimePart, { type: "tool" }>): boolean { return part.state.status === "completed" || part.state.status === "error" || part.state.status === "cancelled"; }
function isRuntimeJsonObject(value: RuntimeJsonValue): value is { readonly [key: string]: RuntimeJsonValue } { return typeof value === "object" && value !== null && !Array.isArray(value) && Object.getPrototypeOf(value) === Object.prototype; }
function withinJsonStringBudget(value: string, maxBytes: number): boolean { return byteLength(JSON.stringify(value)) <= maxBytes; }
function byteLength(value: string): number { return new TextEncoder().encode(value).byteLength; }

function mergeProviderMetadata(existing: ReasoningProviderMetadata | undefined, incoming: ReasoningProviderMetadata | undefined): ReasoningProviderMetadata | undefined {
  if (incoming === undefined) return existing; if (existing === undefined) return incoming;
  const merged: Record<string, ReasoningProviderMetadata[keyof ReasoningProviderMetadata]> = { ...existing };
  for (const [key, value] of Object.entries(incoming)) {
    const prior = existing[key];
    merged[key] = isMetadataObject(prior) && isMetadataObject(value) ? { ...prior, ...value } : value;
  }
  return merged;
}
function isMetadataObject(value: unknown): value is Readonly<Record<string, ReasoningProviderMetadata[keyof ReasoningProviderMetadata]>> { return typeof value === "object" && value !== null && !Array.isArray(value); }

function declarationApplicationFailure(writeId: string): { readonly ok: false; readonly events: readonly SessionEvent[]; readonly error: RuntimeFailure } {
  return { ok: false, events: [], error: eventWriterFailure(normalizeSessionEventWriterError({ code: "schema_mismatch", writeId })) };
}
function storeFailure(error: RuntimeMessageStoreError): RuntimeFailure {
  return RuntimeFailureSchema.parse({ type: "message-store", code: error.code, message: error.message, retryable: error.retryable, fatal: error.fatal, operation: error.operation, ...(error.reason === undefined ? {} : { reason: error.reason }), ...(error.sessionId === undefined ? {} : { sessionId: error.sessionId }) });
}
function eventWriterFailure(error: SessionEventWriterError): RuntimeFailure {
  return RuntimeFailureSchema.parse({ type: "session-event-writer", code: error.code, message: error.message, retryable: error.retryable, fatal: error.fatal, ...(error.sessionId === undefined ? {} : { sessionId: error.sessionId }) });
}
function protocolSequenceFailure(): RuntimeFailure { return { type: "runtime", code: "gateway_protocol_error", message: "Runtime stream sequence is invalid.", retryable: false, fatal: true, reason: "runtime_contract_validation" }; }
function semanticSequenceFailure(): RuntimeFailure { return { type: "runtime", code: "runtime_invalid_sequence", message: "Runtime terminal result violates a semantic invariant.", retryable: false, fatal: true, reason: "runtime_contract_validation" }; }
function boundedSemanticFailure(): RuntimeFailure { return { type: "runtime", code: "runtime_invalid_sequence", message: "Runtime provider output exceeds its semantic size bound.", retryable: false, fatal: true, reason: "bounded" }; }

function upsertMessage<T extends RuntimeMessage>(messages: readonly T[], next: T): T[] {
  return messages.some((message) => message.id === next.id)
    ? messages.map((message) => message.id === next.id ? next : message)
    : [...messages, next];
}
