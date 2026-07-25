/**
 * @packageDocumentation
 * Implements SessionProcessor, the stable-part and event accumulator for one assistant shell.
 * It guards provider event ordering, stable message and tool projections, reasoning durability,
 * public event identity, and exactly-once terminalization without owning thread lifecycle state.
 * AgentLoop feeds it validated LLM events and tool settlements during a provider turn and may
 * rehydrate it later to settle a pending approval. It calls the injected message, event, and
 * repair writers and returns the hot projection and durable-ACK outcomes to AgentLoop.
 */
import { createHash } from "node:crypto";
import type {
  RuntimeMessageStoreWriteMessageResult,
  RuntimeMessageStoreWritePartResult,
  RuntimeMessageStoreError,
  RuntimeBoundedJson,
  RuntimeBoundedText,
  SessionEvent,
  RuntimeFailure,
  RuntimeProcessorSource,
  RuntimeToolSettlement,
  RuntimeInternalToolRepairCommit,
  RuntimeMessage,
  RuntimeMessageInfo,
  RuntimePart,
  RuntimeJsonValue,
  RuntimeToolError,
  PublicMcpErrorEvent,
  SessionEventWriterStableReasoningPart,
  SessionEventWriterAppendResult,
  SessionEventWriterError,
} from "../contracts/runtime.js";
import type { LLMEvent } from "../llm/llm-event.js";
import {
  SessionEventSchema,
  RuntimeMessageInfoSchema,
  RuntimeMessageSchema,
  RuntimeFailureSchema,
  RuntimePartSchema,
  RuntimeBoundedTextSchema,
  boundRuntimePartForStableWrite,
  boundRuntimeText,
} from "../contracts/runtime.js";

/** Result of applying one provider event to the current request-turn projection. */
export type SessionProcessorResult =
  | { readonly ok: true; readonly events: readonly SessionEvent[] }
  | { readonly ok: false; readonly events: readonly SessionEvent[]; readonly error: RuntimeFailure; readonly messageId?: string };

/** Result of durably anchoring a public tool-use event. */
export type ToolUseCommitResult =
  | { readonly ok: true; readonly events: readonly SessionEvent[]; readonly toolUseEventId: string }
  | { readonly ok: false; readonly events: readonly SessionEvent[]; readonly error: RuntimeFailure; readonly messageId?: string };

/** Public event family associated with a Runtime tool part. */
export type PublicToolEvent =
  | { readonly kind: "tool" }
  | { readonly kind: "mcp"; readonly mcpServerName: string };

export type { PublicMcpErrorEvent, RuntimeProcessorSource, RuntimeToolSettlement } from "../contracts/runtime.js";

/** Hot projection, durable event, and internal-repair capabilities used by one processor. */
export interface SessionProcessorWriter {
  readonly writeMessage: (
    message: RuntimeMessageInfo,
    source: RuntimeProcessorSource,
  ) => Promise<RuntimeMessageStoreWriteMessageResult>;
  readonly writePart: (
    part: RuntimePart,
    source: RuntimeProcessorSource,
  ) => Promise<RuntimeMessageStoreWritePartResult>;
  readonly appendEvent: (
    event: SessionEvent,
    source: RuntimeProcessorSource,
    projectionJson?: string,
    stableReasoningParts?: readonly SessionEventWriterStableReasoningPart[],
    modelRequestId?: string,
    serverToolUse?: { readonly webSearchRequests: number; readonly webFetchRequests: number },
  ) => Promise<SessionEventWriterAppendResult>;
  readonly commitInternalToolRepair: (
    repair: RuntimeInternalToolRepairCommit,
    source: RuntimeProcessorSource,
  ) => Promise<RuntimeMessageStoreWritePartResult>;
  readonly commitPart?: (part: RuntimePart) => void;
}

interface LLMEventEnvelope extends RuntimeProcessorSource {
  readonly event: LLMEvent;
}

/** Identity, clock, shell, bounds, and writer dependencies for one request turn. */
export interface SessionProcessorOptions {
  readonly messageId: string;
  readonly modelRequestId: string;
  readonly sessionId: string;
  readonly sessionThreadId: string;
  /**
   * AgentLoop owns assistant shell creation and its hot-store acknowledgement
   * before the provider call. Durable transcript projection is gated separately
   * by event and request-end writes; SessionProcessor owns stable part updates and
   * terminal message finalization over the supplied shell.
   */
  readonly message: RuntimeMessageInfo;
  readonly maxNormalizedTextPreviewBytes?: number;
  readonly now: () => string;
  readonly createId: (prefix: string) => string;
  readonly writer: SessionProcessorWriter;
}

interface TextAppendResult {
  readonly text: string;
  readonly truncated: boolean;
  readonly acceptedDelta: string;
}

type TextRuntimePart = Extract<RuntimePart, { readonly type: "text" }>;
type ReasoningRuntimePart = Extract<RuntimePart, { readonly type: "reasoning" }>;
type ToolRuntimePart = Extract<RuntimePart, { readonly type: "tool" }>;
type ReasoningProviderMetadata = NonNullable<ReasoningRuntimePart["providerMetadata"]>;

function stableReasoningPartForWrite(part: ReasoningRuntimePart): SessionEventWriterStableReasoningPart {
  return {
    reasoningPartId: part.id,
    ...(part.providerPartId !== undefined ? { providerPartId: part.providerPartId } : {}),
    partSequence: part.sequence,
    text: part.text,
    ...(part.providerMetadata !== undefined ? { providerMetadata: part.providerMetadata } : {}),
    truncated: part.truncated,
  };
}

interface RuntimePartBase {
  readonly id: string;
  readonly sessionId: string;
  readonly messageId: string;
  readonly sequence: number;
  readonly createdAt: string;
}

type EnsureToolPartResult =
  | { readonly ok: true; readonly events: readonly SessionEvent[]; readonly part: ToolRuntimePart }
  | { readonly ok: false; readonly events: readonly SessionEvent[]; readonly error: RuntimeFailure; readonly messageId?: string };

/**
 * Accumulates stable Runtime parts and public events for one assistant shell.
 * AgentLoop creates it for a live model request or rehydrates it around an outstanding approved
 * tool use, and discards the instance after that bounded operation settles.
 */
export class SessionProcessor {
  private readonly options: SessionProcessorOptions;
  private readonly reasoningParts = new Map<string, ReasoningRuntimePart>();
  private readonly toolParts = new Map<string, ToolRuntimePart>();
  private readonly toolUseEventIds = new Map<string, string>();
  private readonly toolEvents = new Map<string, PublicToolEvent>();
  private readonly durableReasoningPartIds = new Set<string>();
  private projection: RuntimeMessage[];
  private messageInfo: RuntimeMessageInfo;
  private activeTextPart: TextRuntimePart | undefined;
  private activeStepIndex = 0;
  private terminal = false;

  constructor(options: SessionProcessorOptions) {
    this.options = options;
    this.messageInfo = RuntimeMessageInfoSchema.parse(options.message);
    this.projection = upsertMessageInfo([], this.messageInfo);
  }

  messages(): readonly RuntimeMessage[] {
    return [...this.projection];
  }

  stableReasoningParts(): readonly SessionEventWriterStableReasoningPart[] {
    return this.completedReasoningParts().map(stableReasoningPartForWrite);
  }

  markStableReasoningDurable(parts: readonly SessionEventWriterStableReasoningPart[]): void {
    for (const part of parts) {
      this.durableReasoningPartIds.add(part.reasoningPartId);
    }
  }

  isReasoningPartDurable(partId: string): boolean {
    return this.durableReasoningPartIds.has(partId);
  }

  hydratePendingToolUse(part: ToolRuntimePart): void {
    const parsed = parseToolPart(part);
    if (parsed.state.status !== "running" || parsed.toolUseEventId === undefined) {
      throw new Error("pending tool use hydration requires a running tool part with toolUseEventId");
    }
    this.toolParts.set(parsed.toolCallId, parsed);
    this.toolUseEventIds.set(parsed.toolCallId, parsed.toolUseEventId);
    if (parsed.toolEvent !== undefined) {
      this.toolEvents.set(parsed.toolCallId, parsed.toolEvent);
    }
    this.upsertPart(parsed);
  }

  // Terminal / rejection stream-event handling. Two failure-handling rules the switch
  // arms below encode:
  //
  // attachment-rejections (per-ref): NON-terminal here — the arm returns ok with no
  //   events, so the provider request proceeds with the valid subset. The agent loop
  //   adds one model-only note per rejected origin and EXCLUDES those origins from any
  //   same-turn re-assembly, which makes deterministic rejection loop-free.
  //
  // provider-error / stream error after span start: the accumulator terminalizes the
  //   in-flight assistant draft and discards only UNCOMMITTED draft parts; COMPLETED
  //   committed tools and their anchored reasoning are already durable and carry
  //   forward. This provider-error arm calls terminalFailure and terminalizes the
  //   accumulator's active tool parts immediately. AgentLoop separately joins the
  //   request ToolFiberSet before choosing backoff or terminal closeout; a re-issue
  //   rebases on the durably-committed view, while a denied stale_terminal loser
  //   performs no additional local repair because the winning close owns it.
  // UPDATE-WITH: services/agent-runtime/packages/core/src/agent-loop/agent-loop.ts,
  //              services/agent-runtime/packages/core/src/runtime/message-projection.ts
  async process(envelope: LLMEventEnvelope): Promise<SessionProcessorResult> {
    if (this.terminal) {
      return this.failWithoutWrites(protocolSequenceFailure());
    }
    const event = envelope.event;
    switch (event.type) {
      case "step-start":
        return await this.startStep({ ...envelope, event });
      case "step-finish":
        return await this.finishStep({ ...envelope, event });
      case "text-start":
        return await this.startText({ ...envelope, event });
      case "text-delta":
        return this.appendText({ ...envelope, event });
      case "text-end":
        return await this.endText({ ...envelope, event });
      case "reasoning-start":
        return await this.startReasoning({ ...envelope, event });
      case "reasoning-delta":
        return this.appendReasoning({ ...envelope, event });
      case "reasoning-end":
        return await this.endReasoning({ ...envelope, event });
      case "tool-input-start":
        return await this.startToolInput({ ...envelope, event });
      case "tool-input-delta":
        return { ok: true, events: [] };
      case "tool-input-end":
        return { ok: true, events: [] };
      case "attachment-rejections":
        return { ok: true, events: [] };
      case "tool-call":
        return await this.startToolCall({ ...envelope, event });
      case "finish":
        return await this.finish({ ...envelope, event });
      case "provider-error":
        return await this.terminalFailure(envelope, "failed", event.error);
    }
  }

  async cancel(source: RuntimeProcessorSource, failure: RuntimeFailure): Promise<SessionProcessorResult> {
    if (this.terminal) {
      return this.failWithoutWrites(protocolSequenceFailure());
    }
    return await this.terminalFailure(source, "cancelled", failure);
  }

  async cancelOpenTools(source: RuntimeProcessorSource, failure: RuntimeFailure): Promise<SessionProcessorResult> {
    return await this.terminalizeActiveTools(source, "cancelled", failure);
  }

  async commitPublicToolUse(
    source: RuntimeProcessorSource,
    toolCallId: string,
    evaluatedPermission: "allow" | "ask" | "deny",
    toolEvent: PublicToolEvent = { kind: "tool" },
  ): Promise<ToolUseCommitResult> {
    const existing = this.toolParts.get(toolCallId);
    if (existing === undefined || existing.state.status !== "running") {
      return {
        ok: false,
        events: [],
        error: protocolSequenceFailure(),
        messageId: this.options.messageId,
      };
    }
    const existingToolUseEventId = this.toolUseEventIds.get(toolCallId);
    if (existingToolUseEventId !== undefined) {
      return { ok: true, events: [], toolUseEventId: existingToolUseEventId };
    }
    const event = toolUseSessionEvent(existing, evaluatedPermission, toolEvent);
    const anchoredReasoning = this.stableReasoningPrefixForTool(existing);
    const appendResult = await this.options.writer.appendEvent(
      event,
      source,
      toolProjectionJson(existing, toolEvent),
      anchoredReasoning,
      anchoredReasoning.length > 0 ? this.options.modelRequestId : undefined,
    );
    if (!appendResult.ok) {
      return {
        ok: false,
        events: [],
        error: eventWriterFailure(appendResult.error),
        messageId: this.options.messageId,
      };
    }
    this.markStableReasoningDurable(anchoredReasoning);
    this.toolUseEventIds.set(toolCallId, appendResult.eventId);
    this.toolEvents.set(toolCallId, toolEvent);
    const toolUseStampedPart = parseToolPart({
      ...existing,
      toolUseEventId: appendResult.eventId,
      toolEvent,
      updatedAt: this.options.now(),
    });
    const stampedWrite = await this.options.writer.writePart(toolUseStampedPart, source);
    if (!stampedWrite.ok) {
      return {
        ok: false,
        events: [event],
        error: storeFailure(stampedWrite.error),
        messageId: this.options.messageId,
      };
    }
    this.toolParts.set(toolCallId, toolUseStampedPart);
    this.upsertPart(toolUseStampedPart);
    this.options.writer.commitPart?.(toolUseStampedPart);
    return {
      ok: true,
      events: [event],
      toolUseEventId: appendResult.eventId,
    };
  }

  async commitToolSettlement(
    source: RuntimeProcessorSource,
    toolCallId: string,
    settlement: RuntimeToolSettlement,
  ): Promise<SessionProcessorResult> {
    const existing = this.toolParts.get(toolCallId);
    if (existing === undefined || existing.state.status !== "running") {
      return await this.terminalFailure(source, "failed", semanticSequenceFailure());
    }
    if (settlement.type === "completed") {
      const completed = parseToolPart({
        ...existing,
        state: {
          status: "completed",
          input: existing.state.input,
          output: RuntimeBoundedTextSchema.parse(settlement.output),
        },
        updatedAt: this.options.now(),
        completedAt: this.options.now(),
      });
      this.toolParts.set(toolCallId, completed);
      return await this.writePart(completed, source, settlement.serverToolUse);
    }
    if (settlement.type === "error") {
      const errored = parseToolPart({
        ...existing,
        state: {
          status: "error",
          input: existing.state.input,
          error: runtimeToolError(settlement.error),
        },
        updatedAt: this.options.now(),
        completedAt: this.options.now(),
      });
      this.toolParts.set(toolCallId, errored);
      const result = await this.writePart(errored, source, settlement.serverToolUse);
      return await this.appendPublicMcpErrorEvent(result, settlement.publicErrorEvent, source);
    }
    const cancelled = parseToolPart({
      ...existing,
      state: {
        status: "cancelled",
        input: existing.state.input,
        ...(settlement.error !== undefined ? { error: runtimeToolError(settlement.error) } : {}),
      },
      updatedAt: this.options.now(),
      completedAt: this.options.now(),
    });
    this.toolParts.set(toolCallId, cancelled);
    return await this.writePart(cancelled, source);
  }

  async commitInternalToolRepair(
    source: RuntimeProcessorSource,
    toolCallId: string,
    modelRequestId: string,
    repairKey: string,
    failure: RuntimeFailure,
  ): Promise<SessionProcessorResult> {
    const existing = this.toolParts.get(toolCallId);
    if (existing === undefined || existing.state.status !== "running") {
      return await this.terminalFailure(source, "failed", semanticSequenceFailure());
    }
    const now = this.options.now();
    const repairMessageId = internalToolRepairProjectionId("msg_repair", repairKey);
    const repaired = parseToolPart({
      id: internalToolRepairProjectionId("part_repair", repairKey),
      sessionId: this.options.sessionId,
      messageId: repairMessageId,
      sequence: 0,
      createdAt: now,
      type: "tool",
      toolCallId: existing.toolCallId,
      toolName: existing.toolName,
      state: {
        status: "error",
        input: existing.state.input,
        error: runtimeToolError(failure),
      },
      startedAt: existing.startedAt,
      updatedAt: now,
      completedAt: now,
    });
    const repairMessage = RuntimeMessageSchema.parse({
      id: repairMessageId,
      sessionId: this.options.sessionId,
      role: "assistant",
      origin: "agent",
      sequence: this.messageInfo.sequence + 1,
      status: "completed",
      createdAt: now,
      updatedAt: now,
      ...(this.messageInfo.providerId !== undefined ? { providerId: this.messageInfo.providerId } : {}),
      ...(this.messageInfo.modelId !== undefined ? { modelId: this.messageInfo.modelId } : {}),
      parts: [boundRuntimePartForStableWrite(repaired, this.maxBytes())],
    });
    const commit = await this.options.writer.commitInternalToolRepair({
      sessionId: this.options.sessionId,
      sessionThreadId: this.options.sessionThreadId,
      modelRequestId,
      modelToolCallId: toolCallId,
      toolName: existing.toolName,
      repairKey,
      message: repairMessage,
    }, source);
    if (!commit.ok) {
      const failureResult = storeFailure(commit.error);
      this.discardProjection();
      return this.failWithOptionalMessageId(failureResult);
    }
    const repairPart = repairMessage.parts[0] as ToolRuntimePart;
    this.toolParts.set(toolCallId, repairPart);
    this.projection = upsertMessage(this.projection, repairMessage);
    return { ok: true, events: [] };
  }

  private async startStep(
    envelope: LLMEventEnvelope & { readonly event: Extract<LLMEvent, { readonly type: "step-start" }> },
  ): Promise<SessionProcessorResult> {
    this.activeStepIndex = envelope.event.stepIndex ?? this.activeStepIndex + 1;
    return await this.writePart(
      RuntimePartSchema.parse({
        ...this.partBase(),
        type: "step-start",
        stepIndex: this.activeStepIndex,
      }),
      envelope,
    );
  }

  private async finishStep(
    envelope: LLMEventEnvelope & { readonly event: Extract<LLMEvent, { readonly type: "step-finish" }> },
  ): Promise<SessionProcessorResult> {
    const partWritten = await this.writePart(
      RuntimePartSchema.parse({
        ...this.partBase(),
        type: "step-finish",
        stepIndex: this.activeStepIndex === 0 ? undefined : this.activeStepIndex,
        finishReason: envelope.event.finishReason ?? "unknown",
        ...(envelope.event.usage !== undefined ? { usage: envelope.event.usage } : {}),
      }),
      envelope,
    );
    if (!partWritten.ok) {
      return partWritten;
    }
    return this.withPriorEvents(partWritten.events, await this.writeMessage(
      RuntimeMessageInfoSchema.parse({
        ...this.currentMessageInfoFields(),
        status: "streaming",
        finishReason: envelope.event.finishReason ?? "unknown",
        ...(envelope.event.usage !== undefined ? { usage: envelope.event.usage } : {}),
        updatedAt: this.options.now(),
      }),
      envelope,
    ));
  }

  private async startText(
    envelope: LLMEventEnvelope & { readonly event: Extract<LLMEvent, { readonly type: "text-start" }> },
  ): Promise<SessionProcessorResult> {
    if (this.activeTextPart !== undefined) {
      return { ok: true, events: [] };
    }
    const now = this.options.now();
    const part = parseTextPart({
      ...this.partBase(now),
      type: "text",
      text: "",
      truncated: false,
      status: "streaming",
      startedAt: now,
    });
    this.activeTextPart = part;
    return { ok: true, events: [] };
  }

  private appendText(
    envelope: LLMEventEnvelope & { readonly event: Extract<LLMEvent, { readonly type: "text-delta" }> },
  ): SessionProcessorResult {
    if (this.activeTextPart === undefined) {
      return { ok: true, events: [] };
    }
    const appended = appendBoundedText(this.activeTextPart, envelope.event.text_delta, this.maxBytes());
    const updatedText = parseTextPart({
      ...this.activeTextPart,
      text: appended.text,
      truncated: appended.truncated,
      updatedAt: this.options.now(),
    });
    this.activeTextPart = updatedText;
    this.upsertPart(this.activeTextPart);
    return { ok: true, events: [] };
  }

  private async endText(
    envelope: LLMEventEnvelope & { readonly event: Extract<LLMEvent, { readonly type: "text-end" }> },
  ): Promise<SessionProcessorResult> {
    if (this.activeTextPart === undefined) {
      return { ok: true, events: [] };
    }
    const completed = parseTextPart({
      ...this.activeTextPart,
      status: "completed",
      updatedAt: this.options.now(),
      completedAt: this.options.now(),
    });
    this.activeTextPart = undefined;
    return await this.writePart(completed, envelope);
  }

  private async startReasoning(
    envelope: LLMEventEnvelope & { readonly event: Extract<LLMEvent, { readonly type: "reasoning-start" }> },
  ): Promise<SessionProcessorResult> {
    if (this.reasoningParts.has(envelope.event.id)) {
      return { ok: true, events: [] };
    }
    const now = this.options.now();
    const part = parseReasoningPart({
      ...this.partBase(now),
      type: "reasoning",
      providerPartId: envelope.event.id,
      ...(envelope.event.providerMetadata !== undefined ? { providerMetadata: envelope.event.providerMetadata } : {}),
      text: "",
      truncated: false,
      status: "streaming",
      startedAt: now,
    });
    const event = this.sessionEvent({ type: "agent.thinking" });
    const appendResult = await this.options.writer.appendEvent(event, envelope);
    if (!appendResult.ok) {
      return {
        ok: false,
        events: [],
        error: eventWriterFailure(appendResult.error),
        messageId: this.options.messageId,
      };
    }
    this.reasoningParts.set(envelope.event.id, part);
    return { ok: true, events: [event] };
  }

  private appendReasoning(
    envelope: LLMEventEnvelope & { readonly event: Extract<LLMEvent, { readonly type: "reasoning-delta" }> },
  ): SessionProcessorResult {
    const part = this.reasoningParts.get(envelope.event.id);
    if (part === undefined) {
      return { ok: true, events: [] };
    }
    const appended = appendBoundedText(part, envelope.event.text_delta, this.maxBytes());
    const providerMetadata = mergeProviderMetadata(part.providerMetadata, envelope.event.providerMetadata);
    const updated = parseReasoningPart({
      ...part,
      ...(providerMetadata !== undefined ? { providerMetadata } : {}),
      text: appended.text,
      truncated: appended.truncated,
      updatedAt: this.options.now(),
    });
    this.reasoningParts.set(envelope.event.id, updated);
    this.upsertPart(updated);
    return { ok: true, events: [] };
  }

  private async endReasoning(
    envelope: LLMEventEnvelope & { readonly event: Extract<LLMEvent, { readonly type: "reasoning-end" }> },
  ): Promise<SessionProcessorResult> {
    const part = this.reasoningParts.get(envelope.event.id);
    if (part === undefined) {
      return { ok: true, events: [] };
    }
    const providerMetadata = mergeProviderMetadata(part.providerMetadata, envelope.event.providerMetadata);
    const completed = parseReasoningPart({
      ...part,
      ...(providerMetadata !== undefined ? { providerMetadata } : {}),
      status: "completed",
      updatedAt: this.options.now(),
      completedAt: this.options.now(),
    });
    this.reasoningParts.delete(envelope.event.id);
    return await this.writePart(completed, envelope);
  }

  private async startToolInput(
    envelope: LLMEventEnvelope & { readonly event: Extract<LLMEvent, { readonly type: "tool-input-start" }> },
  ): Promise<SessionProcessorResult> {
    if (this.toolParts.has(envelope.event.id)) {
      return { ok: true, events: [] };
    }
    const now = this.options.now();
    const part = parseToolPart({
      ...this.partBase(now),
      type: "tool",
      toolCallId: envelope.event.id,
      toolName: envelope.event.toolName,
      state: { status: "pending" },
      startedAt: now,
    });
    this.toolParts.set(envelope.event.id, part);
    return { ok: true, events: [] };
  }

  private async startToolCall(
    envelope: LLMEventEnvelope & { readonly event: Extract<LLMEvent, { readonly type: "tool-call" }> },
  ): Promise<SessionProcessorResult> {
    const part = await this.ensureToolPart(envelope, envelope.event.id, envelope.event.toolName);
    if (!part.ok) {
      return part;
    }
    const updated = parseToolPart({
      ...part.part,
      toolName: envelope.event.toolName,
      state: {
        status: "running",
        input: runtimeJsonFromProvider(envelope.event.input, this.maxBytes()),
      },
      updatedAt: this.options.now(),
    });
    this.toolParts.set(envelope.event.id, updated);
    return this.withPriorEvents(part.events, await this.writePart(updated, envelope));
  }

  private async finish(
    envelope: LLMEventEnvelope & { readonly event: Extract<LLMEvent, { readonly type: "finish" }> },
  ): Promise<SessionProcessorResult> {
    const invalidFinish = this.messageInfo === undefined || (this.hasIncompleteParts() && envelope.event.finishReason !== "tool-calls") || this.currentParts().length === 0;
    if (invalidFinish) {
      return await this.terminalFailure(envelope, "failed", semanticSequenceFailure());
    }
    const completed = await this.writeMessage(
      RuntimeMessageInfoSchema.parse({
        ...this.currentMessageInfoFields(),
        status: "completed",
        finishReason: envelope.event.finishReason ?? "unknown",
        ...(envelope.event.usage !== undefined ? { usage: envelope.event.usage } : {}),
        updatedAt: this.options.now(),
      }),
      envelope,
    );
    if (!completed.ok) {
      return completed;
    }
    this.terminal = true;
    return completed;
  }

  private async terminalFailure(
    source: RuntimeProcessorSource,
    status: "failed" | "cancelled",
    failure: RuntimeFailure,
  ): Promise<SessionProcessorResult> {
    const terminalized = await this.terminalizeActiveParts(source, status, failure);
    if (!terminalized.ok) {
      return terminalized;
    }
    const messageWritten = await this.writeMessage(
      RuntimeMessageInfoSchema.parse({
        ...this.currentMessageInfoFields(),
        status,
        error: failure,
        updatedAt: this.options.now(),
      }),
      source,
    );
    if (!messageWritten.ok) {
      return this.withPriorEvents(terminalized.events, messageWritten);
    }
    this.terminal = true;
    return {
      ok: true,
      events: [
        ...terminalized.events,
        ...messageWritten.events,
        this.sessionEvent({
          type: "session.error",
          error: failure,
        }),
      ],
    };
  }

  private async terminalizeActiveParts(
    source: RuntimeProcessorSource,
    status: "failed" | "cancelled",
    failure: RuntimeFailure,
  ): Promise<SessionProcessorResult> {
    const events: SessionEvent[] = [];
    if (this.activeTextPart !== undefined) {
      const terminalText = parseTextPart({
        ...this.activeTextPart,
        status,
        updatedAt: this.options.now(),
        completedAt: this.options.now(),
      });
      this.activeTextPart = undefined;
      const written = await this.writePart(terminalText, source);
      if (!written.ok) {
        return this.withPriorEvents(events, written);
      }
      events.push(...written.events);
    }
    for (const [providerPartId, part] of [...this.reasoningParts]) {
      const terminalReasoning = parseReasoningPart({
        ...part,
        status,
        updatedAt: this.options.now(),
        completedAt: this.options.now(),
      });
      this.reasoningParts.delete(providerPartId);
      const written = await this.writePart(terminalReasoning, source);
      if (!written.ok) {
        return this.withPriorEvents(events, written);
      }
      events.push(...written.events);
    }
    const tools = await this.terminalizeActiveTools(source, status, failure);
    return this.withPriorEvents(events, tools);
  }

  private async terminalizeActiveTools(
    source: RuntimeProcessorSource,
    status: "failed" | "cancelled",
    failure: RuntimeFailure,
  ): Promise<SessionProcessorResult> {
    const events: SessionEvent[] = [];
    for (const [toolCallId, part] of [...this.toolParts]) {
      if (part.state.status === "completed" || part.state.status === "error" || part.state.status === "cancelled") {
        continue;
      }
      const terminalError = runtimeToolErrorFromFailure(failure);
      const terminalTool = parseToolPart({
        ...part,
        state: part.state.status === "running"
          ? status === "cancelled"
            ? { status: "cancelled", input: part.state.input, error: terminalError }
            : { status: "error", input: part.state.input, error: terminalError }
          : status === "cancelled"
            ? { status: "cancelled", error: terminalError }
            : { status: "error", error: terminalError },
        updatedAt: this.options.now(),
        completedAt: this.options.now(),
      });
      this.toolParts.set(toolCallId, terminalTool);
      const written = await this.writePart(terminalTool, source);
      if (!written.ok) {
        return this.withPriorEvents(events, written);
      }
      events.push(...written.events);
    }
    return { ok: true, events };
  }

  private async ensureToolPart(
    envelope: LLMEventEnvelope,
    toolCallId: string,
    toolName: string,
  ): Promise<EnsureToolPartResult> {
    const existing = this.toolParts.get(toolCallId);
    if (existing !== undefined) {
      return { ok: true, events: [], part: existing };
    }
    const now = this.options.now();
    const part = parseToolPart({
      ...this.partBase(now),
      type: "tool",
      toolCallId,
      toolName,
      state: { status: "pending" },
      startedAt: now,
    });
    this.toolParts.set(toolCallId, part);
    return { ok: true, events: [], part };
  }

  private async writeMessage(
    message: RuntimeMessageInfo,
    source: RuntimeProcessorSource,
  ): Promise<SessionProcessorResult> {
    const result = await this.options.writer.writeMessage(message, source);
    if (!result.ok) {
      const failure = storeFailure(result.error);
      if (this.messageInfo !== undefined && message.status !== "failed") {
        await this.writeFailedMessageAfterStableWriteFailure(source, failure);
      }
      this.discardProjection();
      return this.failWithOptionalMessageId(failure);
    }
    this.messageInfo = message;
    this.projection = upsertMessageInfo(this.projection, message);
    return {
      ok: true,
      events: [],
    };
  }

  private async writePart(
    part: RuntimePart,
    source: RuntimeProcessorSource,
    serverToolUse?: { readonly webSearchRequests: number; readonly webFetchRequests: number },
  ): Promise<SessionProcessorResult> {
    const stablePart = boundRuntimePartForStableWrite(part, this.maxBytes());
    const result = await this.options.writer.writePart(stablePart, source);
    if (!result.ok) {
      const failure = storeFailure(result.error);
      await this.writeFailedPartAfterStableWriteFailure(stablePart, source, failure);
      await this.writeFailedMessageAfterStableWriteFailure(source, failure);
      this.discardProjection();
      return this.failWithOptionalMessageId(failure);
    }
    this.upsertPart(stablePart);
    const appended = await this.appendStablePartEvents(stablePart, source, serverToolUse);
    if (!appended.ok) {
      return appended;
    }
    if (stablePart.type !== "reasoning" && appended.events.length > 0) {
      this.options.writer.commitPart?.(appended.committedPart ?? stablePart);
    }
    return appended;
  }

  private async appendStablePartEvents(
    part: RuntimePart,
    source: RuntimeProcessorSource,
    serverToolUse?: { readonly webSearchRequests: number; readonly webFetchRequests: number },
  ): Promise<SessionProcessorResult & { readonly committedPart?: RuntimePart }> {
    const events = sessionEventsForStablePart(part, this.toolUseEventIds, this.toolEvents);
    const appendedEvents: SessionEvent[] = [];
    for (const event of events) {
      const anchoredReasoning = part.type === "tool" && (event.type === "agent.tool_use" || event.type === "agent.mcp_tool_use")
        ? this.stableReasoningPrefixForTool(part)
        : [];
      const projectionJson = projectionJsonForStablePartEvent(part, event);
      const modelRequestId = anchoredReasoning.length > 0
        || event.type === "agent.message"
        || event.type === "agent.tool_result"
        || event.type === "agent.mcp_tool_result"
        ? this.options.modelRequestId
        : undefined;
      const appendResult = await this.options.writer.appendEvent(
        event,
        source,
        projectionJson,
        anchoredReasoning,
        modelRequestId,
        event.type === "agent.tool_result" ? serverToolUse : undefined,
      );
      if (!appendResult.ok) {
        return {
          ok: false,
          events: appendedEvents,
          error: eventWriterFailure(appendResult.error),
          messageId: this.options.messageId,
        };
      }
      this.markStableReasoningDurable(anchoredReasoning);
      if (part.type === "tool" && (event.type === "agent.tool_use" || event.type === "agent.mcp_tool_use")) {
        this.toolUseEventIds.set(part.toolCallId, appendResult.eventId);
        const toolEvent = event.type === "agent.mcp_tool_use"
          ? { kind: "mcp" as const, mcpServerName: event.mcp_server_name }
          : { kind: "tool" as const };
        this.toolEvents.set(part.toolCallId, toolEvent);
        const toolUseStampedPart = parseToolPart({
          ...part,
          toolUseEventId: appendResult.eventId,
          toolEvent,
          updatedAt: this.options.now(),
        });
        const stampedWrite = await this.options.writer.writePart(toolUseStampedPart, source);
        if (!stampedWrite.ok) {
          return {
            ok: false,
            events: appendedEvents,
            error: storeFailure(stampedWrite.error),
            messageId: this.options.messageId,
          };
        }
        this.upsertPart(toolUseStampedPart);
        return {
          ok: true,
          events: [...appendedEvents, event],
          committedPart: toolUseStampedPart,
        };
      }
      appendedEvents.push(event);
    }
    return {
      ok: true,
      events: appendedEvents,
    };
  }

  private async writeFailedMessageAfterStableWriteFailure(
    source: RuntimeProcessorSource,
    failure: RuntimeFailure,
  ): Promise<void> {
    const failedMessage = RuntimeMessageInfoSchema.parse({
      ...this.currentMessageInfoFields(),
      status: "failed",
      error: failure,
      updatedAt: this.options.now(),
    });
    const result = await this.options.writer.writeMessage(failedMessage, source);
    if (result.ok) {
      this.messageInfo = failedMessage;
      this.projection = upsertMessageInfo(this.projection, failedMessage);
      this.terminal = true;
    }
  }

  private async writeFailedPartAfterStableWriteFailure(
    failedWritePart: RuntimePart,
    source: RuntimeProcessorSource,
    failure: RuntimeFailure,
  ): Promise<void> {
    const currentPart = this.currentParts().find((part) => part.id === failedWritePart.id);
    if (currentPart === undefined) {
      return;
    }
    const terminalPart = terminalRuntimePartFromFailure(currentPart, failure, this.options.now());
    if (terminalPart === undefined) {
      return;
    }
    const stableTerminalPart = boundRuntimePartForStableWrite(terminalPart, this.maxBytes());
    const result = await this.options.writer.writePart(stableTerminalPart, source);
    if (result.ok) {
      this.upsertPart(stableTerminalPart);
      if (this.activeTextPart?.id === stableTerminalPart.id) {
        this.activeTextPart = undefined;
      }
      for (const [providerPartId, part] of [...this.reasoningParts]) {
        if (part.id === stableTerminalPart.id) {
          this.reasoningParts.delete(providerPartId);
        }
      }
      for (const [toolCallId, part] of [...this.toolParts]) {
        if (part.id === stableTerminalPart.id && stableTerminalPart.type === "tool") {
          this.toolParts.set(toolCallId, stableTerminalPart);
        }
      }
    }
  }

  private async appendPublicMcpErrorEvent(
    result: SessionProcessorResult,
    publicErrorEvent: PublicMcpErrorEvent | undefined,
    source: RuntimeProcessorSource,
  ): Promise<SessionProcessorResult> {
    if (!result.ok || publicErrorEvent === undefined) {
      return result;
    }
    const event = mcpErrorSessionEvent(publicErrorEvent);
    const appendResult = await this.options.writer.appendEvent(event, source);
    if (!appendResult.ok) {
      return {
        ok: false,
        events: result.events,
        error: eventWriterFailure(appendResult.error),
        messageId: this.options.messageId,
      };
    }
    return {
      ok: true,
      events: [...result.events, event],
    };
  }

  private failWithoutWrites(failure: RuntimeFailure): SessionProcessorResult {
    this.discardProjection();
    return { ok: false, events: [], error: failure };
  }

  private failWithOptionalMessageId(failure: RuntimeFailure): SessionProcessorResult {
    return {
      ok: false,
      events: [],
      error: failure,
      messageId: this.options.messageId,
    };
  }

  private withPriorEvents(priorEvents: readonly SessionEvent[], result: SessionProcessorResult): SessionProcessorResult {
    return {
      ...result,
      events: [...priorEvents, ...result.events],
    };
  }

  private upsertPart(part: RuntimePart): void {
    this.projection = upsertMessagePart(this.projection, part);
  }

  private completedReasoningParts(): ReasoningRuntimePart[] {
    return this.projection
      .flatMap((message) => message.parts)
      .filter((part): part is ReasoningRuntimePart => part.type === "reasoning" && part.status === "completed")
      .sort((left, right) => left.sequence - right.sequence);
  }

  private stableReasoningPrefixForTool(tool: ToolRuntimePart): SessionEventWriterStableReasoningPart[] {
    return this.completedReasoningParts()
      .filter((part) => part.sequence < tool.sequence && !this.durableReasoningPartIds.has(part.id))
      .map(stableReasoningPartForWrite);
  }

  private discardProjection(): void {
    this.projection = [];
  }

  private currentMessageInfoFields(): RuntimeMessageInfo {
    return this.messageInfo;
  }

  private currentParts(): readonly RuntimePart[] {
    return this.projection.find((message) => message.id === this.options.messageId)?.parts ?? [];
  }

  private hasIncompleteParts(): boolean {
    if (this.activeTextPart !== undefined || this.reasoningParts.size > 0) {
      return true;
    }
    return this.currentParts().some((part) => part.type === "tool" && (part.state.status === "pending" || part.state.status === "running"));
  }

  private partBase(createdAt: string = this.options.now()): RuntimePartBase {
    return {
      id: this.options.createId("part"),
      sessionId: this.options.sessionId,
      messageId: this.options.messageId,
      sequence: this.currentParts().length,
      createdAt,
    };
  }

  sessionStatus(status: { readonly type: "idle"; readonly stopReason?: { readonly type: "end_turn" } | { readonly type: "requires_action"; readonly event_ids: string[] } | { readonly type: "retries_exhausted" } } | { readonly type: "busy" } | { readonly type: "retry"; readonly attempt: number; readonly message: string; readonly next: number; readonly action?: { readonly reason: string; readonly provider: string; readonly title: string; readonly message: string; readonly label: string; readonly link?: string } }): SessionEvent {
    if (status.type === "idle") {
      return this.sessionEvent({
        type: "session.status_idle",
        stop_reason: status.stopReason ?? { type: "end_turn" },
      });
    }
    return this.sessionEvent({
      type: "session.status_running",
    });
  }

  private sessionEvent(fields: Readonly<Record<string, unknown>>): SessionEvent {
    return SessionEventSchema.parse(fields);
  }

  private maxBytes(): number {
    return this.options.maxNormalizedTextPreviewBytes ?? 8_192;
  }
}

/** Derives the deterministic idempotency key for one invalid internal tool-call repair. */
export function internalToolRepairKey(modelRequestId: string, modelToolCallId: string, toolName: string): string {
  const hash = createHash("sha256");
  for (const value of [modelRequestId, modelToolCallId, toolName]) {
    hash.update(String(Buffer.byteLength(value, "utf8")), "ascii");
    hash.update(":", "ascii");
    hash.update(value, "utf8");
  }
  return `internal_invalid_tool_${hash.digest("hex")}`;
}

function internalToolRepairProjectionId(prefix: "msg_repair" | "part_repair", repairKey: string): string {
  return `${prefix}_${createHash("sha256").update(prefix, "utf8").update("\0", "utf8").update(repairKey, "utf8").digest("hex")}`;
}

function mergeProviderMetadata(
  existing: ReasoningProviderMetadata | undefined,
  incoming: ReasoningProviderMetadata | undefined,
): ReasoningProviderMetadata | undefined {
  if (incoming === undefined) {
    return existing;
  }
  if (existing === undefined) {
    return incoming;
  }
  const merged: Record<string, ReasoningProviderMetadata[keyof ReasoningProviderMetadata]> = { ...existing };
  for (const [key, value] of Object.entries(incoming)) {
    const previous = existing[key];
    merged[key] = isMetadataObject(previous) && isMetadataObject(value)
      ? { ...previous, ...value }
      : value;
  }
  return merged;
}

function isMetadataObject(value: unknown): value is Readonly<Record<string, ReasoningProviderMetadata[keyof ReasoningProviderMetadata]>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseTextPart(input: unknown): TextRuntimePart {
  const part = RuntimePartSchema.parse(input);
  if (part.type !== "text") {
    throw new Error("expected text runtime part");
  }
  return part;
}

function parseReasoningPart(input: unknown): ReasoningRuntimePart {
  const part = RuntimePartSchema.parse(input);
  if (part.type !== "reasoning") {
    throw new Error("expected reasoning runtime part");
  }
  return part;
}

function parseToolPart(input: unknown): ToolRuntimePart {
  const part = RuntimePartSchema.parse(input);
  if (part.type !== "tool") {
    throw new Error("expected tool runtime part");
  }
  return part;
}

function terminalRuntimePartFromFailure(part: RuntimePart, failure: RuntimeFailure, now: string): RuntimePart | undefined {
  if (part.type === "text") {
    if (part.status === "completed" || part.status === "failed" || part.status === "cancelled") {
      return undefined;
    }
    return parseTextPart({
      ...part,
      status: "failed",
      updatedAt: now,
      completedAt: now,
    });
  }
  if (part.type === "reasoning") {
    if (part.status === "completed" || part.status === "failed" || part.status === "cancelled") {
      return undefined;
    }
    return parseReasoningPart({
      ...part,
      status: "failed",
      updatedAt: now,
      completedAt: now,
    });
  }
  if (part.type === "tool") {
    if (part.state.status === "completed" || part.state.status === "error" || part.state.status === "cancelled") {
      return undefined;
    }
    return parseToolPart({
      ...part,
      state: {
        status: "error",
        ...(part.state.status === "running" ? { input: part.state.input } : {}),
        error: runtimeToolErrorFromFailure(failure),
      },
      updatedAt: now,
      completedAt: now,
    });
  }
  return undefined;
}

function appendBoundedText(
  part: TextRuntimePart | ReasoningRuntimePart,
  text_delta: string,
  maxBytes: number,
): TextAppendResult {
  const bounded = boundRuntimeText(`${part.text}${text_delta}`, maxBytes);
  const acceptedDelta = bounded.text.startsWith(part.text) ? bounded.text.slice(part.text.length) : "";
  return {
    text: bounded.text,
    truncated: part.truncated || bounded.truncated,
    acceptedDelta,
  };
}

function runtimeJsonFromProvider(payload: RuntimeBoundedJson, maxBytes: number): RuntimeBoundedJson {
  const boundedPreview = boundRuntimeText(payload.preview, maxBytes);
  const serializedValue = payload.value === undefined ? undefined : JSON.stringify(payload.value);
  const valueFits =
    serializedValue !== undefined && byteLength(serializedValue) <= maxBytes && !boundedPreview.truncated;
  return {
    ...(valueFits ? { value: payload.value } : {}),
    preview: boundedPreview.text,
    truncated: payload.truncated || boundedPreview.truncated || (payload.value !== undefined && !valueFits),
  };
}

function byteLength(value: string): number {
  return new TextEncoder().encode(value).byteLength;
}

function toolUseSessionEvent(
  part: ToolRuntimePart,
  evaluatedPermission: "allow" | "ask" | "deny",
  toolEvent: PublicToolEvent,
): SessionEvent {
  const input = toolInputForEvent(part);
  if (toolEvent.kind === "mcp") {
    return SessionEventSchema.parse({
      type: "agent.mcp_tool_use",
      name: part.toolName,
      input,
      mcp_server_name: toolEvent.mcpServerName,
      evaluated_permission: evaluatedPermission,
    });
  }
  return SessionEventSchema.parse({
    type: "agent.tool_use",
    name: part.toolName,
    input,
    evaluated_permission: evaluatedPermission,
  });
}

function toolInputForEvent(part: ToolRuntimePart): RuntimeJsonValue {
  if ("input" in part.state) {
    const input = part.state.input;
    if (input !== undefined && input.value !== undefined && isRuntimeJsonObjectForEvent(input.value)) {
      return input.value;
    }
  }
  return {};
}

function isRuntimeJsonObjectForEvent(value: RuntimeJsonValue): value is { readonly [key: string]: RuntimeJsonValue } {
  return typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    Object.getPrototypeOf(value) === Object.prototype;
}

function toolResultSessionEvent(
  toolUseEventId: string,
  toolEvent: PublicToolEvent,
  content?: readonly { readonly type: "text"; readonly text: string }[],
  isError?: boolean,
): SessionEvent {
  if (toolEvent.kind === "mcp") {
    return SessionEventSchema.parse({
      type: "agent.mcp_tool_result",
      mcp_tool_use_id: toolUseEventId,
      ...(content !== undefined ? { content } : {}),
      ...(isError !== undefined ? { is_error: isError } : {}),
    });
  }
  return SessionEventSchema.parse({
    type: "agent.tool_result",
    tool_use_id: toolUseEventId,
    ...(content !== undefined ? { content } : {}),
    ...(isError !== undefined ? { is_error: isError } : {}),
  });
}

function projectionJsonForStablePartEvent(part: RuntimePart, event: SessionEvent): string | undefined {
	if (part.type === "text" && event.type === "agent.message") {
		return JSON.stringify({
			type: "runtime_text_projection",
			message_id: part.messageId,
			part_id: part.id,
			part_sequence: part.sequence,
			truncated: part.truncated,
		});
	}
	if (part.type !== "tool") {
		return undefined;
  }
  switch (event.type) {
    case "agent.tool_use":
    case "agent.tool_result":
    case "agent.mcp_tool_use":
    case "agent.mcp_tool_result":
      return toolProjectionJson(part);
    default:
      return undefined;
  }
}

function toolProjectionJson(part: ToolRuntimePart, publicEvent: PublicToolEvent | undefined = part.toolEvent): string {
  const input = toolInputForEvent(part);
	const projection: Record<string, unknown> = {
		type: "runtime_tool_projection",
		message_id: part.messageId,
		part_id: part.id,
		part_sequence: part.sequence,
		model_tool_call_id: part.toolCallId,
    tool_name: part.toolName,
    input,
    state: part.state.status,
  };
  if (publicEvent?.kind === "mcp") {
    projection.mcp_server_name = publicEvent.mcpServerName;
  }
  switch (part.state.status) {
    case "completed":
      projection.output = part.state.output;
      break;
    case "error":
      projection.error = part.state.error;
      break;
    case "cancelled":
      projection.error = part.state.error ?? { type: "cancelled", message: "tool call was cancelled" };
      break;
    case "pending":
    case "running":
      break;
  }
  return JSON.stringify(projection);
}

function mcpErrorSessionEvent(error: PublicMcpErrorEvent): SessionEvent {
  if (error.type === "unknown_error") {
    return SessionEventSchema.parse({
      type: "session.error",
      error: {
        type: error.type,
        message: error.message,
        retry_status: error.retryStatus,
      },
    });
  }
  return SessionEventSchema.parse({
    type: "session.error",
    error: {
      type: error.type,
      mcp_server_name: error.mcpServerName,
      message: error.message,
      retry_status: error.retryStatus,
    },
  });
}

function toolEventForPart(part: ToolRuntimePart, toolEvents: ReadonlyMap<string, PublicToolEvent>): PublicToolEvent {
  if (part.toolEvent !== undefined) {
    return part.toolEvent;
  }
  return toolEvents.get(part.toolCallId) ?? { kind: "tool" };
}

function sessionEventsForStablePart(
  part: RuntimePart,
  toolUseEventIds: ReadonlyMap<string, string>,
  toolEvents: ReadonlyMap<string, PublicToolEvent>,
): readonly SessionEvent[] {
  if (part.type === "text" && part.status === "completed" && part.text.length > 0) {
    return [SessionEventSchema.parse({ type: "agent.message", content: [{ type: "text", text: part.text }] })];
  }
  if (part.type !== "tool") {
    return [];
  }
  if (part.state.status === "completed") {
    const toolUseEventId = toolUseEventIds.get(part.toolCallId);
    if (toolUseEventId === undefined) {
      return [];
    }
    return [toolResultSessionEvent(toolUseEventId, toolEventForPart(part, toolEvents), [{ type: "text", text: part.state.output.text }])];
  }
  if (part.state.status === "error") {
    const toolUseEventId = toolUseEventIds.get(part.toolCallId);
    if (toolUseEventId === undefined) {
      return [];
    }
    return [toolResultSessionEvent(toolUseEventId, toolEventForPart(part, toolEvents), [{ type: "text", text: part.state.error.message }], true)];
  }
  if (part.state.status === "cancelled") {
    const toolUseEventId = toolUseEventIds.get(part.toolCallId);
    if (toolUseEventId === undefined) {
      return [];
    }
    return [toolResultSessionEvent(toolUseEventId, toolEventForPart(part, toolEvents), undefined, true)];
  }
  return [];
}

function runtimeToolError(error: RuntimeFailure): RuntimeToolError {
  return {
    type: error.code,
    message: error.message,
    retryable: error.retryable,
  };
}

function runtimeToolErrorFromFailure(error: RuntimeFailure): RuntimeToolError {
  return {
    type: error.code,
    message: error.message,
    retryable: error.retryable,
  };
}

function storeFailure(error: RuntimeMessageStoreError): RuntimeFailure {
  return RuntimeFailureSchema.parse({
    type: "message-store",
    code: error.code,
    message: error.message,
    retryable: error.retryable,
    fatal: error.fatal,
    operation: error.operation,
    ...(error.reason !== undefined ? { reason: error.reason } : {}),
    ...(error.constraint !== undefined ? { constraint: error.constraint } : {}),
    ...(error.status !== undefined ? { status: error.status } : {}),
    ...(error.attemptedStatus !== undefined ? { attemptedStatus: error.attemptedStatus } : {}),
    ...(error.messageId !== undefined ? { messageId: error.messageId } : {}),
    ...(error.partId !== undefined ? { partId: error.partId } : {}),
    ...(error.sessionId !== undefined ? { sessionId: error.sessionId } : {}),
  });
}

function eventWriterFailure(error: SessionEventWriterError): RuntimeFailure {
  return RuntimeFailureSchema.parse({
    type: "session-event-writer",
    code: error.code,
    message: error.message,
    retryable: error.retryable,
    fatal: error.fatal,
    ...(error.sessionId !== undefined ? { sessionId: error.sessionId } : {}),
  });
}

function protocolSequenceFailure(): RuntimeFailure {
  return {
    type: "runtime",
    code: "gateway_protocol_error",
    message: "Runtime stream sequence is invalid.",
    retryable: false,
    fatal: true,
    reason: "runtime_contract_validation",
  };
}

function semanticSequenceFailure(): RuntimeFailure {
  return {
    type: "runtime",
    code: "runtime_invalid_sequence",
    message: "Runtime terminal result violates a semantic invariant.",
    retryable: false,
    fatal: true,
    reason: "runtime_contract_validation",
  };
}

function upsertMessageInfo(messages: readonly RuntimeMessage[], messageInfo: RuntimeMessageInfo): RuntimeMessage[] {
  const existing = messages.find((message) => message.id === messageInfo.id);
  if (existing === undefined) {
    return [...messages, RuntimeMessageSchema.parse({ ...messageInfo, parts: [] })];
  }
  return messages.map((message) =>
    message.id === messageInfo.id ? RuntimeMessageSchema.parse({ ...messageInfo, parts: message.parts }) : message,
  );
}

function upsertMessagePart(messages: readonly RuntimeMessage[], part: RuntimePart): RuntimeMessage[] {
  return messages.map((message) => {
    if (message.id !== part.messageId) {
      return message;
    }
    const parts = [...message.parts.filter((currentPart) => currentPart.id !== part.id), part]
      .sort((left, right) => left.sequence - right.sequence);
    return RuntimeMessageSchema.parse({ ...message, parts });
  });
}

function upsertMessage(messages: readonly RuntimeMessage[], next: RuntimeMessage): RuntimeMessage[] {
  const exists = messages.some((message) => message.id === next.id);
  if (!exists) {
    return [...messages, next];
  }
  return messages.map((message) => message.id === next.id ? next : message);
}
