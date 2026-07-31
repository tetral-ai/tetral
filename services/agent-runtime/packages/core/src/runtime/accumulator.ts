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
  RuntimeMessageStoreError,
  RuntimeBoundedJson,
  RuntimeBoundedText,
  SessionEvent,
  RuntimeFailure,
  RuntimeProcessorSource,
  RuntimeToolSettlement,
  RuntimeInternalToolRepairCommit,
  RuntimeInternalToolRepairCommitResult,
  DurableRuntimeMessage,
  RuntimeMessage,
  RuntimeMessageDraft,
  RuntimeDeclarationReceipt,
  RuntimePart,
  RuntimePartDraft,
  RuntimeJsonValue,
  RuntimeToolError,
  PublicMcpErrorEvent,
  SessionEventWriterStableReasoningPart,
  SessionEventWriterAppendResult,
  SessionEventWriterError,
} from "../contracts/runtime.js";
import type { LLMEvent } from "../llm/llm-event.js";
import {
  DurableRuntimeMessageSchema,
  SessionEventSchema,
  RuntimeMessageDraftSchema,
  RuntimeFailureSchema,
  RuntimePartDraftSchema,
  RuntimeBoundedTextSchema,
  normalizeSessionEventWriterError,
  normalizeRuntimeMessageStoreError,
  boundRuntimeText,
} from "../contracts/runtime.js";
import {
  applyInternalToolRepairReceipt,
  applyInterruptInputReceipt,
  applyRuntimeOutputReceipt,
  applyRuntimeRequestEndReceipt,
  runtimeInternalToolRepairDraft,
  runtimeOutputDraft,
} from "./runtime-declaration.js";
import { stableRuntimeID } from "./runtime-identity.js";

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

/** Loop-authored cancellation declaration joined to one open request closeout. */
export interface RuntimeInterruptSettlementDraft {
  readonly terminalAssistantSeal?: RuntimeMessageDraft;
  readonly drafts: readonly RuntimeMessageDraft[];
  readonly pendingToolCancellations: readonly {
    readonly toolUseEventId: string;
    readonly runtimeLocalId: string;
  }[];
  readonly sandboxExecutionToolUseEventIds: readonly string[];
}

/** Hot projection, durable event, and internal-repair capabilities used by one processor. */
export interface SessionProcessorWriter {
  readonly appendEvent: (
    event: SessionEvent,
    source: RuntimeProcessorSource,
    output?: {
      readonly draftKind: RuntimeMessageDraft["draftKind"];
      readonly message: RuntimeMessageDraft;
    },
    stableReasoningParts?: readonly SessionEventWriterStableReasoningPart[],
    modelRequestId?: string,
    serverToolUse?: { readonly webSearchRequests: number; readonly webFetchRequests: number },
    mcpMaterializationHandle?: string,
  ) => Promise<SessionEventWriterAppendResult>;
  readonly commitInternalToolRepair: (
    repair: RuntimeInternalToolRepairCommit,
    source: RuntimeProcessorSource,
  ) => Promise<RuntimeInternalToolRepairCommitResult>;
}

interface LLMEventEnvelope extends RuntimeProcessorSource {
  readonly event: LLMEvent;
}

/** Identity, clock, shell, bounds, and writer dependencies for one request turn. */
export interface SessionProcessorOptions {
  readonly modelRequestId: string;
  readonly requestId: string;
  readonly workspaceId: string;
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly bindingId: string;
  readonly bindingGeneration: number;
  readonly targetPodUid: string;
  /** Unstamped assistant draft owned by this provider request. */
  readonly message: RuntimeMessageDraft;
  readonly maxNormalizedTextPreviewBytes?: number;
  readonly now: () => string;
  readonly writer: SessionProcessorWriter;
}

interface TextAppendResult {
  readonly text: string;
  readonly truncated: boolean;
  readonly acceptedDelta: string;
}

type TextRuntimePartDraft = Extract<RuntimePartDraft, { readonly type: "text" }>;
type ReasoningRuntimePartDraft = Extract<RuntimePartDraft, { readonly type: "reasoning" }>;
type ToolRuntimePartDraft = Extract<RuntimePartDraft, { readonly type: "tool" }>;
type DurableToolRuntimePart = Extract<RuntimePart, { readonly type: "tool" }>;
type ReasoningProviderMetadata = NonNullable<ReasoningRuntimePartDraft["providerMetadata"]>;

function stableReasoningPartForWrite(part: ReasoningRuntimePartDraft): SessionEventWriterStableReasoningPart {
  return {
    reasoningPartId: part.runtimeLocalPartId,
    ...(part.providerPartId !== undefined ? { providerPartId: part.providerPartId } : {}),
    partSequence: part.ordinal,
    text: part.text,
    ...(part.providerMetadata !== undefined ? { providerMetadata: part.providerMetadata } : {}),
    truncated: part.truncated,
  };
}

interface RuntimePartDraftBase {
  readonly runtimeLocalPartId: string;
  readonly ordinal: number;
}

type EnsureToolPartResult =
  | { readonly ok: true; readonly events: readonly SessionEvent[]; readonly part: ToolRuntimePartDraft }
  | { readonly ok: false; readonly events: readonly SessionEvent[]; readonly error: RuntimeFailure; readonly messageId?: string };

/**
 * Accumulates stable Runtime parts and public events for one assistant shell.
 * AgentLoop creates it for a live model request or rehydrates it around an outstanding approved
 * tool use, and discards the instance after that bounded operation settles.
 */
export class SessionProcessor {
  private readonly options: SessionProcessorOptions;
  private readonly reasoningParts = new Map<string, ReasoningRuntimePartDraft>();
  private readonly toolParts = new Map<string, ToolRuntimePartDraft>();
  private readonly toolUseEventIds = new Map<string, string>();
  private readonly toolEvents = new Map<string, PublicToolEvent>();
  private readonly durableReasoningPartIds = new Set<string>();
  private durableProjection: DurableRuntimeMessage[] = [];
  private primaryDurableMessageId: string | undefined;
  private failedCloseoutParts: RuntimePartDraft[] = [];
  private workingMessage: RuntimeMessageDraft;
  private activeTextPart: TextRuntimePartDraft | undefined;
  private activeStepIndex = 0;
  private terminal = false;

  constructor(options: SessionProcessorOptions) {
    this.options = options;
    this.workingMessage = RuntimeMessageDraftSchema.parse(options.message);
  }

  messages(): readonly RuntimeMessage[] {
    return [...this.durableProjection];
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
    return this.durableReasoningPartIds.has(partId) ||
      this.durableProjection.some((message) =>
        message.parts.some((part) => part.type === "reasoning" && part.id === partId)
      );
  }

  /** Builds the terminal assistant seal carried by an ordinary request-end declaration. */
  requestEndSeal(includeSettlementReasoning: boolean): RuntimeMessageDraft | undefined {
    if (this.workingMessage.status === "streaming") {
      throw new Error("request end assistant seal requires a terminal processor");
    }
    const primaryMessage = this.primaryDurableMessageId === undefined
      ? undefined
      : this.durableProjection.find((message) => message.id === this.primaryDurableMessageId);
    const durableAssociations = new Set(runtimePartAssociations(primaryMessage?.parts ?? []));
    const currentAssociations = runtimePartAssociations(this.currentParts());
    const parts = this.currentParts().filter((part, index) =>
      durableAssociations.has(currentAssociations[index] ?? "") ||
      (includeSettlementReasoning && part.type === "reasoning" && part.status === "completed")
    );
    if (parts.length === 0) {
      return undefined;
    }
    return runtimeOutputDraft({
      workspaceId: this.options.workspaceId,
      sessionId: this.options.sessionId,
      sessionThreadId: this.options.sessionThreadId,
      runtimeWriteId: this.options.modelRequestId,
      eventType: "model_request",
      draftKind: "assistant_text",
      message: RuntimeMessageDraftSchema.parse({
        ...this.currentMessage(),
        parts,
      }),
    });
  }

  /**
   * Builds a failed request-end seal from content that already has durable
   * stamps when processor settlement cannot reach its normal terminal state.
   */
  requestEndFailureSeal(failure: RuntimeFailure): RuntimeMessageDraft | undefined {
    const primaryMessage = this.primaryDurableMessageId === undefined
      ? undefined
      : this.durableProjection.find((message) => message.id === this.primaryDurableMessageId);
    const durableAssociations = new Set(runtimePartAssociations(primaryMessage?.parts ?? []));
    const currentAssociations = runtimePartAssociations(this.currentParts());
    const currentDurableParts = this.currentParts().filter((_part, index) =>
      durableAssociations.has(currentAssociations[index] ?? "")
    );
    const parts = currentDurableParts.length > 0 ? currentDurableParts : this.failedCloseoutParts;
    if (parts.length === 0) {
      return undefined;
    }
    return runtimeOutputDraft({
      workspaceId: this.options.workspaceId,
      sessionId: this.options.sessionId,
      sessionThreadId: this.options.sessionThreadId,
      runtimeWriteId: this.options.modelRequestId,
      eventType: "model_request",
      draftKind: "assistant_text",
      message: RuntimeMessageDraftSchema.parse({
        ...this.currentMessage(),
        status: "failed",
        error: failure,
        parts,
      }),
    });
  }

  /** Applies the database stamps returned for one ordinary request-end assistant seal. */
  applyRequestEndSeal(
    eventId: string,
    draft: RuntimeMessageDraft | undefined,
    declaration: NonNullable<Extract<SessionEventWriterAppendResult, { readonly ok: true }>["declaration"]>,
  ): boolean {
    if (declaration.applicationDisposition !== "current_custody") {
      return false;
    }
    const expectedMessage = this.primaryDurableMessageId === undefined
      ? undefined
      : this.durableProjection.find((message) => message.id === this.primaryDurableMessageId);
    this.durableProjection = [
      ...applyRuntimeRequestEndReceipt({
        sessionId: this.options.sessionId,
        sessionThreadId: this.options.sessionThreadId,
        modelRequestId: this.options.modelRequestId,
        eventId,
        drafts: draft === undefined ? [] : [draft],
        existingMessages: this.durableProjection,
        ...(expectedMessage === undefined ? {} : { expectedMessage }),
      }, declaration.receipt),
    ];
    const sealStamp = draft === undefined
      ? undefined
      : declaration.receipt.messages.find((message) => message.runtimeLocalId === draft.runtimeLocalId);
    if (sealStamp !== undefined) {
      this.primaryDurableMessageId = sealStamp.messageId;
    }
    return true;
  }

  hydratePendingToolUse(message: DurableRuntimeMessage, part: DurableToolRuntimePart): void {
    const parsed = this.currentParts().find(
      (candidate): candidate is ToolRuntimePartDraft =>
        candidate.type === "tool" && candidate.toolCallId === part.toolCallId,
    );
    if (parsed === undefined) {
      throw new Error("pending tool use hydration is missing its working draft");
    }
    if (parsed.state.status !== "running" || parsed.toolUseEventId === undefined) {
      throw new Error("pending tool use hydration requires a running tool part with toolUseEventId");
    }
    if (
      part.messageId !== message.id ||
      !message.parts.some((candidate) => candidate.id === part.id)
    ) {
      throw new Error("pending tool use hydration requires its durable owning message");
    }
    this.durableProjection = [message];
    this.primaryDurableMessageId = message.id;
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
  //   rebases on the durably-committed view. A divergent request close is rejected
  //   by Bridge and the Runtime discards hot state before cold recovery.
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

  /**
   * Freezes the terminal request seal and loop-authored tool cancellations
   * without performing a separate event write.
   */
  prepareInterruptSettlement(
    interrupt: {
      readonly runtimeInputId: string;
      readonly eventIds: readonly string[];
    },
    failure: RuntimeFailure,
    pendingToolUseEventIds: ReadonlySet<string>,
    sandboxExecutionToolUseEventIds: ReadonlySet<string>,
  ): RuntimeInterruptSettlementDraft {
    if (this.terminal || interrupt.eventIds.length !== 1) {
      throw new Error("interrupt settlement requires one open request and one admitted source event");
    }
    this.updateMessage({ status: "cancelled", error: failure });
    this.terminal = true;
    const terminalAssistantSeal = this.requestEndSeal(false);
    const declaration = this.prepareInterruptToolDeclarations(
      interrupt,
      failure,
      pendingToolUseEventIds,
      sandboxExecutionToolUseEventIds,
    );
    return {
      ...(terminalAssistantSeal === undefined ? {} : { terminalAssistantSeal }),
      ...declaration,
    };
  }

  /** Freezes active-tool cancellations for a separate CommitInputs closeout. */
  prepareInterruptToolDeclarations(
    interrupt: {
      readonly runtimeInputId: string;
      readonly eventIds: readonly string[];
    },
    failure: RuntimeFailure,
    pendingToolUseEventIds: ReadonlySet<string>,
    sandboxExecutionToolUseEventIds: ReadonlySet<string>,
  ): Omit<RuntimeInterruptSettlementDraft, "terminalAssistantSeal"> {
    if (interrupt.eventIds.length !== 1) {
      throw new Error("interrupt tool declaration requires one admitted source event");
    }
    const runtimeLocalId = stableRuntimeID(
      "runtime_message_draft",
      this.options.workspaceId,
      this.options.sessionId,
      this.options.sessionThreadId,
      "interrupt_control",
      interrupt.runtimeInputId,
      "cancellation",
      "0",
    );
    const parts: RuntimeMessageDraft["parts"][number][] = [];
    const pendingToolCancellations: {
      readonly toolUseEventId: string;
      readonly runtimeLocalId: string;
    }[] = [];
    const sandboxExecutions: string[] = [];
    for (const [toolCallId, part] of this.toolParts) {
      const toolUseEventId = this.toolUseEventIds.get(toolCallId) ?? part.toolUseEventId;
      if (
        toolUseEventId === undefined ||
        part.state.status === "completed" ||
        part.state.status === "error" ||
        part.state.status === "cancelled"
      ) {
        continue;
      }
      if (sandboxExecutionToolUseEventIds.has(toolUseEventId)) {
        sandboxExecutions.push(toolUseEventId);
        continue;
      }
      const ordinal = parts.length;
      const terminalError = runtimeToolErrorFromFailure(failure);
      const cancelledPart = parseToolPart({
        ...part,
        runtimeLocalPartId: stableRuntimeID(
          "runtime_message_part_draft",
          runtimeLocalId,
          "tool",
          String(ordinal),
        ),
        ordinal,
        toolUseEventId,
        state: part.state.status === "running"
          ? { status: "cancelled", input: part.state.input, error: terminalError }
          : { status: "cancelled", error: terminalError },
        completedAt: this.options.now(),
      });
      parts.push(cancelledPart);
      if (pendingToolUseEventIds.has(toolUseEventId)) {
        pendingToolCancellations.push({ toolUseEventId, runtimeLocalId });
      }
    }
    const drafts = parts.length === 0
      ? []
      : [RuntimeMessageDraftSchema.parse({
          runtimeLocalId,
          sourceKind: "interrupt_control",
          sourceId: interrupt.runtimeInputId,
          sourceEventId: interrupt.eventIds[0],
          draftKind: "cancellation",
          ordinal: 0,
          role: "assistant",
          origin: "agent",
          status: "completed",
          parts,
        })];
    return {
      drafts,
      pendingToolCancellations,
      sandboxExecutionToolUseEventIds: sandboxExecutions,
    };
  }

  /** Applies the interrupt receipt after its joined request-end receipt. */
  applyInterruptSettlement(
    interrupt: {
      readonly runtimeInputId: string;
      readonly eventIds: readonly string[];
    },
    settlement: RuntimeInterruptSettlementDraft,
    receipt: RuntimeDeclarationReceipt,
  ): void {
    this.durableProjection = [
      ...applyInterruptInputReceipt({
        sessionId: this.options.sessionId,
        sessionThreadId: this.options.sessionThreadId,
        runtimeInputId: interrupt.runtimeInputId,
        eventIds: interrupt.eventIds,
        drafts: settlement.drafts,
        pendingToolCancellations: settlement.pendingToolCancellations,
        sandboxExecutionToolUseEventIds: settlement.sandboxExecutionToolUseEventIds,
        existingMessages: this.durableProjection,
      }, receipt).map((message) => DurableRuntimeMessageSchema.parse(message)),
    ];
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
      };
    }
    const existingToolUseEventId = this.toolUseEventIds.get(toolCallId);
    if (existingToolUseEventId !== undefined) {
      return { ok: true, events: [], toolUseEventId: existingToolUseEventId };
    }
    const event = toolUseSessionEvent(existing, evaluatedPermission, toolEvent);
    const anchoredReasoning = this.stableReasoningPrefixForTool(existing);
    const draftPart = parseToolPart({
      ...existing,
      toolEvent,
    });
    this.toolParts.set(toolCallId, draftPart);
    this.upsertPart(draftPart);
    const declaredMessage = this.declarationMessage(anchoredReasoning);
    // Transport and receipt application share this snapshot because another
    // tool fiber may mutate the accumulator while the durable write is pending.
    const appendResult = await this.options.writer.appendEvent(
      event,
      source,
      { draftKind: "tool_use", message: declaredMessage },
      anchoredReasoning,
      this.options.modelRequestId,
    );
    if (!appendResult.ok) {
      return {
        ok: false,
        events: [],
        error: eventWriterFailure(appendResult.error),
      };
    }
    const draft = this.outputDraftFromMessage(appendResult.writeId, event.type, "tool_use", declaredMessage);
    if (!this.applyDeclaration(event, draft, appendResult)) {
      return {
        ok: false,
        events: [],
        error: eventWriterFailure(normalizeSessionEventWriterError({
          code: "schema_mismatch",
          sessionId: this.options.sessionId,
          writeId: appendResult.writeId,
        })),
      };
    }
    this.markStableReasoningDurable(anchoredReasoning);
    this.toolUseEventIds.set(toolCallId, appendResult.eventId);
    this.toolEvents.set(toolCallId, toolEvent);
    const toolUseStampedPart = parseToolPart({
      ...existing,
      toolUseEventId: appendResult.eventId,
      toolEvent,
    });
    this.toolParts.set(toolCallId, toolUseStampedPart);
    this.upsertPart(toolUseStampedPart);
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
        completedAt: this.options.now(),
      });
      this.toolParts.set(toolCallId, completed);
      return await this.writePart(completed, source, settlement.serverToolUse, settlement.mcpMaterializationHandle);
    }
    if (settlement.type === "error") {
      const errored = parseToolPart({
        ...existing,
        state: {
          status: "error",
          input: existing.state.input,
          error: runtimeToolError(settlement.error),
        },
        completedAt: this.options.now(),
      });
      this.toolParts.set(toolCallId, errored);
      const result = await this.writePart(errored, source, settlement.serverToolUse, settlement.mcpMaterializationHandle);
      return await this.appendPublicMcpErrorEvent(result, settlement.publicErrorEvent, source);
    }
    const cancelled = parseToolPart({
      ...existing,
      state: {
        status: "cancelled",
        input: existing.state.input,
        ...(settlement.error !== undefined ? { error: runtimeToolError(settlement.error) } : {}),
      },
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
    const repaired = parseToolPart({
      runtimeLocalPartId: existing.runtimeLocalPartId,
      ordinal: existing.ordinal,
      type: "tool",
      toolCallId: existing.toolCallId,
      toolName: existing.toolName,
      state: {
        status: "error",
        input: existing.state.input,
        error: runtimeToolError(failure),
      },
      startedAt: existing.startedAt,
      completedAt: now,
    });
    const draft = runtimeInternalToolRepairDraft({
      workspaceId: this.options.workspaceId,
      sessionId: this.options.sessionId,
      sessionThreadId: this.options.sessionThreadId,
      repairKey,
      part: repaired,
    });
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
      draft,
    }, source);
    if (!commit.ok) {
      const failureResult = storeFailure(commit.error);
      this.discardProjection();
      return this.failWithOptionalMessageId(failureResult);
    }
    if (commit.declaration.applicationDisposition !== "current_custody") {
      this.discardProjection();
      return this.failWithOptionalMessageId(storeFailure(normalizeRuntimeMessageStoreError({
        code: "unavailable",
        operation: "commitInternalToolRepair",
        reason: "runtime_contract_validation",
        sessionId: this.options.sessionId,
      })));
    }
    let repairMessage: DurableRuntimeMessage;
    try {
      repairMessage = applyInternalToolRepairReceipt({
        sessionId: this.options.sessionId,
        sessionThreadId: this.options.sessionThreadId,
        repairKey,
        eventId: commit.eventId,
        draft,
      }, commit.declaration.receipt);
    } catch {
      this.discardProjection();
      return this.failWithOptionalMessageId(storeFailure(normalizeRuntimeMessageStoreError({
        code: "schema_mismatch",
        operation: "commitInternalToolRepair",
        reason: "runtime_contract_validation",
        sessionId: this.options.sessionId,
      })));
    }
    this.durableProjection = upsertMessage(this.durableProjection, repairMessage);
    return { ok: true, events: [] };
  }

  private async startStep(
    envelope: LLMEventEnvelope & { readonly event: Extract<LLMEvent, { readonly type: "step-start" }> },
  ): Promise<SessionProcessorResult> {
    this.activeStepIndex = envelope.event.stepIndex ?? this.activeStepIndex + 1;
    return await this.writePart(
      RuntimePartDraftSchema.parse({
        ...this.partBase("step-start"),
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
      RuntimePartDraftSchema.parse({
        ...this.partBase("step-finish"),
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
    this.updateMessage({
      status: "streaming",
      finishReason: envelope.event.finishReason ?? "unknown",
      ...(envelope.event.usage !== undefined ? { usage: envelope.event.usage } : {}),
    });
    return partWritten;
  }

  private async startText(
    envelope: LLMEventEnvelope & { readonly event: Extract<LLMEvent, { readonly type: "text-start" }> },
  ): Promise<SessionProcessorResult> {
    if (this.activeTextPart !== undefined) {
      return { ok: true, events: [] };
    }
    const now = this.options.now();
    const part = parseTextPart({
      ...this.partBase("text"),
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
      ...this.partBase("reasoning"),
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
      ...this.partBase("tool"),
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
    });
    this.toolParts.set(envelope.event.id, updated);
    return this.withPriorEvents(part.events, await this.writePart(updated, envelope));
  }

  private async finish(
    envelope: LLMEventEnvelope & { readonly event: Extract<LLMEvent, { readonly type: "finish" }> },
  ): Promise<SessionProcessorResult> {
    const invalidFinish = (this.hasIncompleteParts() && envelope.event.finishReason !== "tool-calls") || this.currentParts().length === 0;
    if (invalidFinish) {
      return await this.terminalFailure(envelope, "failed", semanticSequenceFailure());
    }
    this.updateMessage({
      status: "completed",
      finishReason: envelope.event.finishReason ?? "unknown",
      ...(envelope.event.usage !== undefined ? { usage: envelope.event.usage } : {}),
    });
    this.terminal = true;
    return { ok: true, events: [] };
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
    this.updateMessage({ status, error: failure });
    this.terminal = true;
    return {
      ok: true,
      events: [
        ...terminalized.events,
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
      ...this.partBase("tool"),
      type: "tool",
      toolCallId,
      toolName,
      state: { status: "pending" },
      startedAt: now,
    });
    this.toolParts.set(toolCallId, part);
    return { ok: true, events: [], part };
  }

  private async writePart(
    part: RuntimePartDraft,
    source: RuntimeProcessorSource,
    serverToolUse?: { readonly webSearchRequests: number; readonly webFetchRequests: number },
    mcpMaterializationHandle?: string,
  ): Promise<SessionProcessorResult> {
    this.upsertPart(part);
    return await this.appendStablePartEvents(part, source, serverToolUse, mcpMaterializationHandle);
  }

  private async appendStablePartEvents(
    part: RuntimePartDraft,
    source: RuntimeProcessorSource,
    serverToolUse?: { readonly webSearchRequests: number; readonly webFetchRequests: number },
    mcpMaterializationHandle?: string,
  ): Promise<SessionProcessorResult> {
    const events = sessionEventsForStablePart(part, this.toolUseEventIds, this.toolEvents);
    const appendedEvents: SessionEvent[] = [];
    for (const event of events) {
      const anchoredReasoning = part.type === "tool" && (event.type === "agent.tool_use" || event.type === "agent.mcp_tool_use")
        ? this.stableReasoningPrefixForTool(part)
        : [];
      const draftKind = event.type === "agent.message"
        ? "assistant_text"
        : event.type === "agent.tool_use" || event.type === "agent.mcp_tool_use"
          ? "tool_use"
          : "tool_result";
      const modelRequestId = this.options.modelRequestId;
      const declaredMessage = this.declarationMessage(anchoredReasoning);
      // Transport and receipt application share this snapshot because another
      // tool fiber may mutate the accumulator while the durable write is pending.
      const appendResult = await this.options.writer.appendEvent(
        event,
        source,
        { draftKind, message: declaredMessage },
        anchoredReasoning,
        modelRequestId,
        event.type === "agent.tool_result" ? serverToolUse : undefined,
        event.type === "agent.mcp_tool_result" ? mcpMaterializationHandle : undefined,
      );
      if (!appendResult.ok) {
        return {
          ok: false,
          events: appendedEvents,
          error: eventWriterFailure(appendResult.error),
        };
      }
      const draft = this.outputDraftFromMessage(appendResult.writeId, event.type, draftKind, declaredMessage);
      if (!this.applyDeclaration(event, draft, appendResult)) {
        return {
          ok: false,
          events: appendedEvents,
          error: eventWriterFailure(normalizeSessionEventWriterError({
            code: "schema_mismatch",
            sessionId: this.options.sessionId,
            writeId: appendResult.writeId,
          })),
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
        });
        this.upsertPart(toolUseStampedPart);
        return {
          ok: true,
          events: [...appendedEvents, event],
        };
      }
      appendedEvents.push(event);
    }
    return {
      ok: true,
      events: appendedEvents,
    };
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
    };
  }

  private withPriorEvents(priorEvents: readonly SessionEvent[], result: SessionProcessorResult): SessionProcessorResult {
    return {
      ...result,
      events: [...priorEvents, ...result.events],
    };
  }

  private upsertPart(part: RuntimePartDraft): void {
    this.workingMessage = upsertDraftPart(this.workingMessage, part);
  }

  private completedReasoningParts(): ReasoningRuntimePartDraft[] {
    return this.currentParts()
      .filter((part): part is ReasoningRuntimePartDraft => part.type === "reasoning" && part.status === "completed")
      .sort((left, right) => left.ordinal - right.ordinal);
  }

  private stableReasoningPrefixForTool(tool: ToolRuntimePartDraft): SessionEventWriterStableReasoningPart[] {
    return this.completedReasoningParts()
      .filter((part) =>
        part.ordinal < tool.ordinal &&
        !this.durableReasoningPartIds.has(part.runtimeLocalPartId)
      )
      .map(stableReasoningPartForWrite);
  }

  private discardProjection(): void {
    const primaryMessage = this.primaryDurableMessageId === undefined
      ? undefined
      : this.durableProjection.find((message) => message.id === this.primaryDurableMessageId);
    const durableAssociations = new Set(runtimePartAssociations(primaryMessage?.parts ?? []));
    const currentAssociations = runtimePartAssociations(this.currentParts());
    const durableParts = this.currentParts().filter((_part, index) =>
      durableAssociations.has(currentAssociations[index] ?? "")
    );
    if (durableParts.length > 0) {
      this.failedCloseoutParts = durableParts;
    }
    this.workingMessage = RuntimeMessageDraftSchema.parse({
      ...this.workingMessage,
      parts: [],
    });
    this.durableProjection = [];
    this.primaryDurableMessageId = undefined;
  }

  private updateMessage(
    update: Pick<RuntimeMessageDraft, "status"> &
      Partial<Pick<RuntimeMessageDraft, "error" | "finishReason" | "usage" | "responseId">>,
  ): void {
    this.workingMessage = RuntimeMessageDraftSchema.parse({
      ...this.workingMessage,
      ...update,
    });
  }

  private currentParts(): readonly RuntimePartDraft[] {
    return this.workingMessage.parts;
  }

  private outputDraftFromMessage(
    runtimeWriteId: string,
    eventType: string,
    draftKind: RuntimeMessageDraft["draftKind"],
    message: RuntimeMessageDraft,
  ): RuntimeMessageDraft {
    return runtimeOutputDraft({
      workspaceId: this.options.workspaceId,
      sessionId: this.options.sessionId,
      sessionThreadId: this.options.sessionThreadId,
      runtimeWriteId,
      eventType,
      draftKind,
      message,
    });
  }

  private currentMessage(): RuntimeMessageDraft {
    return this.workingMessage;
  }

  private declarationMessage(
    anchoredReasoning: readonly SessionEventWriterStableReasoningPart[],
  ): RuntimeMessageDraft {
    const admittedReasoning = new Set([
      ...this.durableReasoningPartIds,
      ...anchoredReasoning.map((part) => part.reasoningPartId),
    ]);
    return RuntimeMessageDraftSchema.parse({
      ...this.workingMessage,
      parts: this.workingMessage.parts.filter(
        (part) => part.type !== "reasoning" || admittedReasoning.has(part.runtimeLocalPartId),
      ),
    });
  }

  private applyDeclaration(
    event: SessionEvent,
    draft: RuntimeMessageDraft,
    result: Extract<SessionEventWriterAppendResult, { readonly ok: true }>,
  ): boolean {
    if (
      result.declaration === undefined ||
      result.declaration.applicationDisposition !== "current_custody"
    ) {
      return false;
    }
    this.durableProjection = [
      ...applyRuntimeOutputReceipt({
        sessionId: this.options.sessionId,
        sessionThreadId: this.options.sessionThreadId,
        runtimeWriteId: result.writeId,
        eventType: event.type,
        eventId: result.eventId,
        drafts: [draft],
        existingMessages: this.durableProjection,
      }, result.declaration.receipt),
    ];
    const messageStamp = result.declaration.receipt.messages.find(
      (message) => message.runtimeLocalId === draft.runtimeLocalId,
    );
    if (messageStamp !== undefined) {
      this.primaryDurableMessageId = messageStamp.messageId;
    }
    const durableMessage = this.primaryDurableMessageId === undefined
      ? undefined
      : this.durableProjection.find((message) => message.id === this.primaryDurableMessageId);
    const durableAssociations = new Set(runtimePartAssociations(durableMessage?.parts ?? []));
    const currentAssociations = runtimePartAssociations(this.currentParts());
    const durableParts = this.currentParts().filter((_part, index) =>
      durableAssociations.has(currentAssociations[index] ?? "")
    );
    if (durableParts.length > 0) {
      this.failedCloseoutParts = durableParts;
    }
    return true;
  }

  private hasIncompleteParts(): boolean {
    if (this.activeTextPart !== undefined || this.reasoningParts.size > 0) {
      return true;
    }
    return this.currentParts().some((part) => part.type === "tool" && (part.state.status === "pending" || part.state.status === "running"));
  }

  private partBase(partKind: RuntimePartDraft["type"]): RuntimePartDraftBase {
    const ordinal = this.currentParts().length;
    return {
      runtimeLocalPartId: stableRuntimeID(
        "runtime_message_part_draft",
        this.workingMessage.runtimeLocalId,
        partKind,
        String(ordinal),
      ),
      ordinal,
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

function parseTextPart(input: unknown): TextRuntimePartDraft {
  const part = RuntimePartDraftSchema.parse(input);
  if (part.type !== "text") {
    throw new Error("expected text runtime part");
  }
  return part;
}

function parseReasoningPart(input: unknown): ReasoningRuntimePartDraft {
  const part = RuntimePartDraftSchema.parse(input);
  if (part.type !== "reasoning") {
    throw new Error("expected reasoning runtime part");
  }
  return part;
}

function parseToolPart(input: unknown): ToolRuntimePartDraft {
  const part = RuntimePartDraftSchema.parse(input);
  if (part.type !== "tool") {
    throw new Error("expected tool runtime part");
  }
  return part;
}

function appendBoundedText(
  part: TextRuntimePartDraft | ReasoningRuntimePartDraft,
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
  part: ToolRuntimePartDraft,
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

function toolInputForEvent(part: ToolRuntimePartDraft): RuntimeJsonValue {
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

function toolEventForPart(part: ToolRuntimePartDraft, toolEvents: ReadonlyMap<string, PublicToolEvent>): PublicToolEvent {
  if (part.toolEvent !== undefined) {
    return part.toolEvent;
  }
  return toolEvents.get(part.toolCallId) ?? { kind: "tool" };
}

function sessionEventsForStablePart(
  part: RuntimePartDraft,
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

function runtimePartAssociation(
  part: RuntimePart | RuntimePartDraft,
  fallbackOrdinal: number,
): string {
  if (part.type === "tool") {
    return `tool:${part.toolCallId}`;
  }
  if (part.type === "reasoning" && part.providerPartId !== undefined) {
    return `reasoning:${part.providerPartId}`;
  }
  return `${part.type}:${fallbackOrdinal}`;
}

function runtimePartAssociations(
  parts: readonly (RuntimePart | RuntimePartDraft)[],
): readonly string[] {
  const kindOrdinals = new Map<RuntimePart["type"], number>();
  return parts.map((part) => {
    const ordinal = kindOrdinals.get(part.type) ?? 0;
    kindOrdinals.set(part.type, ordinal + 1);
    return runtimePartAssociation(part, ordinal);
  });
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

function upsertDraftPart(message: RuntimeMessageDraft, part: RuntimePartDraft): RuntimeMessageDraft {
  const parts = [
    ...message.parts.filter((candidate) => candidate.runtimeLocalPartId !== part.runtimeLocalPartId),
    part,
  ].sort((left, right) => left.ordinal - right.ordinal);
  return RuntimeMessageDraftSchema.parse({ ...message, parts });
}

function upsertMessage<T extends RuntimeMessage>(messages: readonly T[], next: T): T[] {
  const exists = messages.some((message) => message.id === next.id);
  if (!exists) {
    return [...messages, next];
  }
  return messages.map((message) => message.id === next.id ? next : message);
}
