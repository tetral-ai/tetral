/**
 * Builds identity-free Runtime declarations and applies Bridge-owned durable
 * stamps by request position. Message creation, Assistant member append, and
 * Tool settlement intentionally have separate applicators so no caller can
 * reconstruct an old Assistant snapshot at this boundary.
 */

import {
  DurableRuntimeMessageSchema,
  RuntimeAssistantPartAppendSchema,
  RuntimeMessageCreateSchema,
  RuntimeMessageSchema,
  RuntimePartCreateSchema,
  RuntimeToolSettlementDeclarationSchema,
} from "../contracts/runtime.js";
import type {
  DurableRuntimeMessage,
  RuntimeAssistantPartAppend,
  RuntimeDeclarationReceipt,
  RuntimeFailure,
  RuntimeMessage,
  RuntimeMessageCreate,
  RuntimePart,
  RuntimePartCreate,
  RuntimeToolSettlementDeclaration,
} from "../contracts/runtime.js";
import type {
  RuntimeAcceptedInputState,
  RuntimePendingApprovalToolJobState,
} from "../thread-loop/thread-state.js";
import { stableRuntimeID } from "./runtime-identity.js";

export type { RuntimeDeclarationReceipt } from "../contracts/runtime.js";

type ToolRuntimePart = Extract<RuntimePart, { readonly type: "tool" }>;

function assertOrdinaryDeclarationReceipt(receipt: RuntimeDeclarationReceipt): void {
  if (receipt.compactedThroughMessageSequence !== undefined) {
    throw new Error("ordinary declaration receipt cannot carry a compaction boundary");
  }
}

function assertReceiptIdentity(
  receipt: RuntimeDeclarationReceipt,
  expected: {
    readonly sessionThreadId: string;
    readonly operationKind: string;
    readonly sourceKind: string;
    readonly operationId: string;
  },
): void {
  if (
    receipt.sessionThreadId !== expected.sessionThreadId ||
    receipt.operationKind !== expected.operationKind ||
    receipt.sourceKind !== expected.sourceKind ||
    receipt.operationId !== expected.operationId ||
    receipt.declarationDigest.length === 0
  ) {
    throw new Error("declaration receipt identity does not match");
  }
}

/** Derives the stable declaration identity for one durable running interval. */
export function runtimeTurnOpenWriteId(input: {
  readonly workspaceId: string;
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly openingSourceKind: string;
  readonly openingSourceId: string;
}): string {
  return stableRuntimeID(
    "turn_open",
    input.workspaceId,
    input.sessionId,
    input.sessionThreadId,
    input.openingSourceKind,
    input.openingSourceId,
  );
}

/** Maps an internal accepted-input kind to the declaration source kind. */
export function acceptedInputDeclarationKind(input: RuntimeAcceptedInputState): string {
  return input.kind === "inter_agent_message" ? "agent_mail" : input.kind;
}

/** Converts one accepted command's semantic messages into ordered creates. */
export function acceptedInputCreates(input: RuntimeAcceptedInputState): readonly RuntimeMessageCreate[] {
  if (input.kind === "task_notification") {
    return [taskNotificationCreate({ payloadJson: input.payloadJson })];
  }
  if (input.kind === "rejection") {
    if (input.eventIds.length === 0) {
      throw new Error("rejection input must name at least one source event");
    }
    return input.eventIds.map(() => RuntimeMessageCreateSchema.parse({
      messageKind: "rejection",
      role: "assistant",
      origin: "agent",
      status: "completed",
      parts: [{
        type: "text",
        text: "The session runtime could not accept this input.",
        truncated: false,
        status: "completed",
      }],
    }));
  }
  const messages = acceptedInputMessages(input);
  const messageKind = acceptedInputMessageKind(input);
  if (messages.length !== input.eventIds.length) {
    throw new Error("accepted input message and source event counts differ");
  }
  return messages.map((message) => RuntimeMessageCreateSchema.parse({
    ...messageCreateInfo(message),
    messageKind,
    parts: message.parts.map(partCreateFromDurable),
  }));
}

/** Applies an accepted-input receipt by message and part position. */
export function applyAcceptedInputReceipt(
  input: RuntimeAcceptedInputState,
  creates: readonly RuntimeMessageCreate[],
  receipt: RuntimeDeclarationReceipt,
): readonly DurableRuntimeMessage[] {
  if (input.kind === "task_notification") {
    const create = creates[0];
    if (creates.length !== 1 || create === undefined) {
      throw new Error("task notification declaration must contain one message create");
    }
    return [applyTaskNotificationReceipt({
      sessionId: input.sessionId,
      sessionThreadId: input.sessionThreadId,
      operationId: taskNotificationOperationId(input.runtimeInputId, input.taskId),
      create,
    }, receipt)];
  }
  assertOrdinaryDeclarationReceipt(receipt);
  assertReceiptIdentity(receipt, {
    sessionThreadId: input.sessionThreadId,
    operationKind: "commit_inputs",
    sourceKind: acceptedInputDeclarationKind(input),
    operationId: input.runtimeInputId,
  });
  if (receipt.events.length !== input.eventIds.length) {
    throw new Error("accepted input event stamp count does not match");
  }
  const expectedDisposition = input.kind === "approval_review" ? "created" : "existing";
  receipt.events.forEach((stamp, index) => {
    if (
      stamp.sessionThreadId !== input.sessionThreadId ||
      stamp.eventId !== input.eventIds[index] ||
      stamp.disposition !== expectedDisposition
    ) {
      throw new Error("accepted input event stamp is invalid");
    }
  });
  return applyMessageCreateStamps({
    sessionId: input.sessionId,
    sessionThreadId: input.sessionThreadId,
    creates,
    eventStamps: receipt.events,
    messageStamps: receipt.messages,
  });
}

/** Validates one admitted interrupt and returns its Bridge-authored projections. */
export function applyInterruptInputReceipt(
  input: {
    readonly sessionThreadId: string;
    readonly runtimeInputId: string;
    readonly eventIds: readonly string[];
    readonly expectedToolUseEventIds: readonly string[];
  },
  receipt: RuntimeDeclarationReceipt,
): readonly RuntimeDeclarationReceipt["interruptToolProjections"][number][] {
  assertOrdinaryDeclarationReceipt(receipt);
  assertReceiptIdentity(receipt, {
    sessionThreadId: input.sessionThreadId,
    operationKind: "commit_inputs",
    sourceKind: "interrupt_control",
    operationId: input.runtimeInputId,
  });
  if (input.eventIds.length !== 1 || receipt.events.length !== 1 || receipt.messages.length !== 0) {
    throw new Error("interrupt receipt carrier set is invalid");
  }
  const interruptEvent = receipt.events[0]!;
  if (
    interruptEvent.sessionThreadId !== input.sessionThreadId ||
    interruptEvent.eventId !== input.eventIds[0] ||
    interruptEvent.disposition !== "existing"
  ) {
    throw new Error("interrupt event stamp is invalid");
  }
  if (receipt.interruptToolProjections.length !== input.expectedToolUseEventIds.length) {
    throw new Error("interrupt Tool projection census is incomplete");
  }
  let previousSequence = -1;
  receipt.interruptToolProjections.forEach((projection, index) => {
    if (
      projection.toolUseEventId !== input.expectedToolUseEventIds[index] ||
      projection.resultEvent.sessionThreadId !== input.sessionThreadId ||
      projection.resultEvent.disposition !== "created" ||
      projection.resultEvent.eventSequence <= previousSequence
    ) {
      throw new Error("interrupt Tool projection is invalid");
    }
    previousSequence = projection.resultEvent.eventSequence;
  });
  return [...receipt.interruptToolProjections];
}

/** Applies a fully validated Bridge-authored interrupt census atomically to hot messages. */
export function applyInterruptToolProjections(
  messages: readonly RuntimeMessage[],
  projections: RuntimeDeclarationReceipt["interruptToolProjections"],
): readonly RuntimeMessage[] {
  const unfinished = messages.flatMap((message) => message.parts
    .filter((part): part is ToolRuntimePart =>
      part.type === "tool" && part.toolUseEventId !== undefined &&
      part.state.status !== "completed" && part.state.status !== "error" && part.state.status !== "cancelled",
    )
    .map((part) => ({ messageId: message.id, part })))
    .sort((left, right) => left.part.sequence - right.part.sequence);
  if (
    unfinished.length !== projections.length ||
    unfinished.some((entry, index) => entry.part.toolUseEventId !== projections[index]?.toolUseEventId)
  ) {
    throw new Error("interrupt projection census does not match hot unfinished Tools");
  }
  const stateByPartId = new Map<string, ToolRuntimePart["state"]>();
  unfinished.forEach(({ part }, index) => {
    const projection = projections[index]!;
    const input = part.state.status === "running" ? { input: part.state.input } : {};
    stateByPartId.set(part.id, projection.terminalState.type === "error"
      ? { status: "error", ...input, error: failureToolError(projection.terminalState.error) }
      : { status: "cancelled", ...input, ...(projection.terminalState.error === undefined ? {} : { error: failureToolError(projection.terminalState.error) }) });
  });
  return messages.map((message) => {
    const projected = {
      ...message,
      parts: message.parts.map((part) => {
      const state = stateByPartId.get(part.id);
      return state === undefined || part.type !== "tool" ? part : { ...part, state };
      }),
    };
    return DurableRuntimeMessageSchema.parse(projected);
  });
}

/** Applies one Bridge-acknowledged target Tool settlement without rebuilding its Assistant Message. */
export function applyToolSettlementProjection(
  messages: readonly RuntimeMessage[],
  toolUseEventId: string,
  settlement: RuntimeToolSettlementDeclaration["outcome"],
  completedAt: string,
): readonly RuntimeMessage[] {
  let matches = 0;
  const projected = messages.map((message) => {
    let changed = false;
    const parts = message.parts.map((part) => {
      if (part.type !== "tool" || part.toolUseEventId !== toolUseEventId) return part;
      matches += 1;
      if (part.state.status !== "running") {
        throw new Error("Tool settlement target is already terminal");
      }
      changed = true;
      const state = settlement.type === "completed"
        ? { status: "completed" as const, input: part.state.input, output: settlement.output }
        : settlement.type === "error"
          ? { status: "error" as const, input: part.state.input, error: failureToolError(settlement.error) }
          : {
              status: "cancelled" as const,
              input: part.state.input,
              ...(settlement.error === undefined ? {} : { error: failureToolError(settlement.error) }),
            };
      return { ...part, state, completedAt };
    });
    if (!changed) return message;
    const next = { ...message, parts };
    return DurableRuntimeMessageSchema.parse(next);
  });
  if (matches !== 1) {
    throw new Error("Tool settlement must name exactly one unfinished hot Tool");
  }
  return projected;
}

/** Builds the sole user message created for an approval decision. */
export function toolConfirmationCreate(input: {
  readonly sourceEventId: string;
  readonly toolUseEventId: string;
  readonly pendingTool: RuntimePendingApprovalToolJobState;
  readonly decision: "allow" | "deny";
  readonly denyMessage?: string | undefined;
}): RuntimeMessageCreate {
  if (
    input.pendingTool.toolUseEventId !== input.toolUseEventId ||
    input.pendingTool.toolPart.toolUseEventId !== input.toolUseEventId
  ) {
    throw new Error("tool confirmation does not identify the pending tool");
  }
  const text = input.decision === "allow"
    ? "Approval allowed"
    : input.denyMessage === undefined || input.denyMessage.length === 0
      ? "Approval denied"
      : `Approval denied: ${input.denyMessage}`;
  return RuntimeMessageCreateSchema.parse({
    messageKind: "approval_input",
    role: "user",
    origin: "user",
    status: "completed",
    parts: [{ type: "text", text, truncated: false, status: "completed" }],
  });
}

/** Applies the database stamp for one admitted approval decision. */
export function applyToolConfirmationReceipt(input: {
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly runtimeInputId: string;
  readonly sourceEventId: string;
  readonly create: RuntimeMessageCreate;
}, receipt: RuntimeDeclarationReceipt): DurableRuntimeMessage {
  assertOrdinaryDeclarationReceipt(receipt);
  assertReceiptIdentity(receipt, {
    sessionThreadId: input.sessionThreadId,
    operationKind: "commit_inputs",
    sourceKind: "tool_confirmation",
    operationId: input.runtimeInputId,
  });
  if (receipt.events.length !== 1 || receipt.events[0]?.eventId !== input.sourceEventId || receipt.events[0].disposition !== "existing") {
    throw new Error("tool confirmation event stamp is invalid");
  }
  return applyMessageCreateStamps({
    sessionId: input.sessionId,
    sessionThreadId: input.sessionThreadId,
    creates: [input.create],
    eventStamps: receipt.events,
    messageStamps: receipt.messages,
  })[0]!;
}

/** Builds the task-notification message created by its dedicated RPC. */
export function taskNotificationCreate(input: { readonly payloadJson: string }): RuntimeMessageCreate {
  return RuntimeMessageCreateSchema.parse({
    messageKind: "task_notification",
    role: "user",
    origin: "runtime",
    status: "completed",
    parts: [{ type: "text", text: input.payloadJson, truncated: false, status: "completed" }],
  });
}

/** Derives the replay identity shared by task notification commit and receipt. */
export function taskNotificationOperationId(runtimeInputId: string, taskId: string): string {
  return stableRuntimeID("task_notification", runtimeInputId, taskId);
}

/** Applies the database stamp for a task notification. */
export function applyTaskNotificationReceipt(input: {
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly operationId: string;
  readonly create: RuntimeMessageCreate;
}, receipt: RuntimeDeclarationReceipt): DurableRuntimeMessage {
  assertOrdinaryDeclarationReceipt(receipt);
  assertReceiptIdentity(receipt, {
    sessionThreadId: input.sessionThreadId,
    operationKind: "commit_task_notification_result",
    sourceKind: "task_notification",
    operationId: input.operationId,
  });
  if (receipt.events.length !== 1 || receipt.events[0]?.disposition !== "created") {
    throw new Error("task notification event stamp is invalid");
  }
  return applyMessageCreateStamps({
    sessionId: input.sessionId,
    sessionThreadId: input.sessionThreadId,
    creates: [input.create],
    eventStamps: receipt.events,
    messageStamps: receipt.messages,
  })[0]!;
}

/** Builds the child completion envelope created by FinishIdle. */
export function completionMailCreate(input: { readonly envelope: string }): RuntimeMessageCreate {
  return RuntimeMessageCreateSchema.parse({
    messageKind: "completion_mail",
    role: "user",
    origin: "runtime",
    status: "completed",
    parts: [{ type: "text", text: input.envelope, truncated: false, status: "completed" }],
  });
}

/** Builds the same completion envelope for abnormal runtime closeout. */
export function runtimeTerminationCompletionMailCreate(input: { readonly envelope: string }): RuntimeMessageCreate {
  return completionMailCreate(input);
}

/** Builds exhaustive deterministic terminal settlements for runtime termination. */
export function runtimeTerminationToolSettlements(input: {
  readonly pendingTools: readonly { readonly toolUseEventId: string }[];
  readonly failure: RuntimeFailure;
}): readonly RuntimeToolSettlementDeclaration[] {
  const seen = new Set<string>();
  return input.pendingTools.map((pending) => {
    if (seen.has(pending.toolUseEventId)) throw new Error("runtime termination Tool census is duplicated");
    seen.add(pending.toolUseEventId);
    return RuntimeToolSettlementDeclarationSchema.parse({
      toolUseEventId: pending.toolUseEventId,
      outcome: { type: "cancelled", error: input.failure },
    });
  });
}

/** Validates the positional Tool-result and terminal event stamps for termination. */
export function validateRuntimeTerminationReceipt(input: {
  readonly sessionThreadId: string;
  readonly operationId: string;
  readonly toolSettlements: readonly RuntimeToolSettlementDeclaration[];
  readonly completionMailCreate?: RuntimeMessageCreate | undefined;
}, receipt: RuntimeDeclarationReceipt): { readonly failureEventId: string; readonly closeoutEventId: string } {
  assertReceiptIdentity(receipt, {
    sessionThreadId: input.sessionThreadId,
    operationKind: "commit_runtime_termination",
    sourceKind: "runtime_termination",
    operationId: input.operationId,
  });
  const completionCount = input.completionMailCreate === undefined ? 0 : 1;
  if (
    receipt.events.length !== input.toolSettlements.length + completionCount + 2 ||
    receipt.messages.length !== completionCount ||
    receipt.interruptToolProjections.length !== 0 ||
    receipt.pendingAttachmentDelta.length !== 0 ||
    receipt.prefixConsumptions.length !== 0 ||
    receipt.requestReschedule !== undefined ||
    receipt.idleCloseout !== undefined ||
    receipt.compactedThroughMessageSequence !== undefined
  ) {
    throw new Error("runtime termination receipt carrier set is incomplete");
  }
  assertCreatedEvents(receipt.events, input.sessionThreadId);
  if (input.completionMailCreate !== undefined) {
    const completionEvent = receipt.events[input.toolSettlements.length];
    applyMessageCreateStamps({
      sessionId: "runtime-termination-validation",
      sessionThreadId: input.sessionThreadId,
      creates: [input.completionMailCreate],
      eventStamps: completionEvent === undefined ? [] : [completionEvent],
      messageStamps: receipt.messages,
      skipSessionValidation: true,
    });
  }
  const terminalStart = input.toolSettlements.length + completionCount;
  return {
    failureEventId: receipt.events[terminalStart]!.eventId,
    closeoutEventId: receipt.events[terminalStart + 1]!.eventId,
  };
}

/** Validates an idle closeout and its optional child completion create. */
export function validateFinishIdleReceipt(input: {
  readonly sessionThreadId: string;
  readonly durableTurnId: string;
  readonly completionMailCreate?: RuntimeMessageCreate | undefined;
}, receipt: RuntimeDeclarationReceipt): void {
  assertReceiptIdentity(receipt, {
    sessionThreadId: input.sessionThreadId,
    operationKind: "finish_idle",
    sourceKind: "turn_closeout",
    operationId: input.durableTurnId,
  });
  const completionCount = input.completionMailCreate === undefined ? 0 : 1;
  if (
    receipt.events.length !== completionCount + 1 ||
    receipt.messages.length !== completionCount ||
    receipt.interruptToolProjections.length !== 0 ||
    receipt.pendingAttachmentDelta.length !== 0 ||
    receipt.prefixConsumptions.length !== 0 ||
    receipt.requestReschedule !== undefined ||
    receipt.compactedThroughMessageSequence !== undefined
  ) {
    throw new Error("finish idle receipt carrier set is invalid");
  }
  const idleEvent = receipt.events[0];
  const closeout = receipt.idleCloseout;
  if (
    idleEvent === undefined || closeout === undefined ||
    idleEvent.sessionThreadId !== input.sessionThreadId ||
    idleEvent.eventId !== closeout.idleEventId ||
    idleEvent.eventSequence !== closeout.idleEventSequence ||
    idleEvent.disposition !== "created" ||
    closeout.durableTurnId !== input.durableTurnId ||
    closeout.committedIdleAt.length === 0
  ) {
    throw new Error("finish idle closeout stamp is invalid");
  }
  if (input.completionMailCreate !== undefined) {
    applyMessageCreateStamps({
      sessionId: "finish-idle-validation",
      sessionThreadId: input.sessionThreadId,
      creates: [input.completionMailCreate],
      eventStamps: [receipt.events[1]!],
      messageStamps: receipt.messages,
      skipSessionValidation: true,
    });
  }
}

/** Builds the sole checkpoint message for a successful compaction. */
export function compactionCheckpointCreate(input: { readonly text: string }): RuntimeMessageCreate {
  return RuntimeMessageCreateSchema.parse({
    messageKind: "compaction_checkpoint",
    role: "user",
    origin: "runtime",
    status: "completed",
    parts: [{ type: "text", text: input.text, truncated: false, status: "completed" }],
  });
}

/** Applies the successful compaction create and prefix-consumption stamp. */
export function applyCompactionReceipt(input: {
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly modelRequestId: string;
  readonly requestEndEventId: string;
  readonly compactedThroughMessageSequence: number;
  readonly create: RuntimeMessageCreate;
  readonly prefixConsumption?: { readonly childThreadId: string; readonly parentBoundaryEventId: string } | undefined;
}, receipt: RuntimeDeclarationReceipt): DurableRuntimeMessage {
  assertReceiptIdentity(receipt, {
    sessionThreadId: input.sessionThreadId,
    operationKind: "write_request_end",
    sourceKind: "model_request",
    operationId: input.modelRequestId,
  });
  if (
    receipt.events.length !== 2 || receipt.messages.length !== 1 ||
    receipt.events[0]?.eventId !== input.requestEndEventId ||
    receipt.compactedThroughMessageSequence !== input.compactedThroughMessageSequence
  ) {
    throw new Error("compaction receipt identity is invalid");
  }
  const checkpoint = applyMessageCreateStamps({
    sessionId: input.sessionId,
    sessionThreadId: input.sessionThreadId,
    creates: [input.create],
    eventStamps: [receipt.events[1]!],
    messageStamps: receipt.messages,
  })[0]!;
  if (checkpoint.sequence !== input.compactedThroughMessageSequence + 1) {
    throw new Error("compaction checkpoint message sequence is not contiguous");
  }
  const prefixStamp = receipt.prefixConsumptions[0];
  if (input.prefixConsumption === undefined) {
    if (receipt.prefixConsumptions.length !== 0) {
      throw new Error("compaction receipt contains an unsolicited prefix consumption");
    }
  } else if (
    receipt.prefixConsumptions.length !== 1 || prefixStamp === undefined ||
    prefixStamp.childThreadId !== input.prefixConsumption.childThreadId ||
    prefixStamp.parentBoundaryEventId !== input.prefixConsumption.parentBoundaryEventId ||
    prefixStamp.checkpointMessageId !== checkpoint.id ||
    prefixStamp.disposition !== "consumed"
  ) {
    throw new Error("compaction prefix consumption stamp is invalid");
  }
  return checkpoint;
}

/** Creates the sole message for one invalid provider Tool call repair. */
export function runtimeInternalToolRepairCreate(input: { readonly part: RuntimePartCreate }): RuntimeMessageCreate {
  if (input.part.type !== "tool" || input.part.state.status !== "error" || input.part.toolUseEventId !== undefined) {
    throw new Error("internal Tool repair requires one unexposed terminal Tool part");
  }
  return RuntimeMessageCreateSchema.parse({
    messageKind: "internal_tool_repair",
    role: "assistant",
    origin: "agent",
    status: "completed",
    parts: [input.part],
  });
}

/** Applies the database identities returned for one internal repair create. */
export function applyInternalToolRepairReceipt(input: {
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly repairKey: string;
  readonly eventId: string;
  readonly create: RuntimeMessageCreate;
}, receipt: RuntimeDeclarationReceipt): DurableRuntimeMessage {
  assertOrdinaryDeclarationReceipt(receipt);
  assertReceiptIdentity(receipt, {
    sessionThreadId: input.sessionThreadId,
    operationKind: "commit_internal_tool_repair",
    sourceKind: "internal_tool_repair",
    operationId: input.repairKey,
  });
  if (receipt.events.length !== 1 || receipt.events[0]?.eventId !== input.eventId) {
    throw new Error("internal Tool repair event stamp is invalid");
  }
  return applyMessageCreateStamps({
    sessionId: input.sessionId,
    sessionThreadId: input.sessionThreadId,
    creates: [input.create],
    eventStamps: receipt.events,
    messageStamps: receipt.messages,
  })[0]!;
}

/** Applies one identity-free Assistant append to the current durable message. */
export function applyAssistantPartAppendReceipt(input: {
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly operationKind: "write_event" | "write_request_end";
  readonly sourceKind: string;
  readonly operationId: string;
  readonly eventId: string;
  readonly append: RuntimeAssistantPartAppend;
  readonly existingMessage?: DurableRuntimeMessage | undefined;
}, receipt: RuntimeDeclarationReceipt): DurableRuntimeMessage {
  assertOrdinaryDeclarationReceipt(receipt);
  assertReceiptIdentity(receipt, input);
  if (receipt.events.length !== 1 || receipt.events[0]?.eventId !== input.eventId || receipt.messages.length !== 1) {
    throw new Error("Assistant append receipt carrier set is invalid");
  }
  const eventStamp = receipt.events[0]!;
  const messageStamp = receipt.messages[0]!;
  const append = RuntimeAssistantPartAppendSchema.parse(input.append);
  if (
    eventStamp.sessionThreadId !== input.sessionThreadId ||
    eventStamp.disposition !== "created" ||
    messageStamp.sessionThreadId !== input.sessionThreadId ||
    messageStamp.parts.length !== append.parts.length
  ) {
    throw new Error("Assistant append receipt stamp is invalid");
  }
  const existing = input.existingMessage;
  if (existing !== undefined && (
    existing.id !== messageStamp.messageId ||
    existing.sequence !== messageStamp.messageSequence ||
    messageStamp.disposition !== "updated"
  )) {
    throw new Error("Assistant append changed its durable message identity");
  }
  if (existing === undefined && messageStamp.disposition !== "created") {
    throw new Error("first Assistant append did not create its message");
  }
  const expectedFirstPartSequence = existing === undefined
    ? 0
    : Math.max(-1, ...existing.parts.map((part) => part.sequence)) + 1;
  const parts = append.parts.map((part, index) => durablePartFromStamp(
    input.sessionId,
    messageStamp.messageId,
    part,
    messageStamp.parts[index]!,
    expectedFirstPartSequence === undefined ? undefined : expectedFirstPartSequence + index,
  ));
  return DurableRuntimeMessageSchema.parse({
    id: messageStamp.messageId,
    sessionId: input.sessionId,
    role: "assistant",
    origin: "agent",
    sequence: messageStamp.messageSequence,
    status: existing?.status ?? "streaming",
    createdAt: messageStamp.createdAt,
    ...(messageStamp.updatedAt.length > 0 ? { updatedAt: messageStamp.updatedAt } : {}),
    ...(existing?.error === undefined ? {} : { error: existing.error }),
    ...(existing?.finishReason === undefined ? {} : { finishReason: existing.finishReason }),
    ...(existing?.usage === undefined ? {} : { usage: existing.usage }),
    ...(existing?.responseId === undefined ? {} : { responseId: existing.responseId }),
    parts: [...(existing?.parts ?? []), ...parts],
  });
}

/** Validates an ordinary target settlement ACK and returns its result event. */
export function applyToolSettlementReceipt(input: {
  readonly sessionThreadId: string;
  readonly operationKind: "write_event" | "commit_runtime_termination";
  readonly sourceKind: string;
  readonly operationId: string;
  readonly eventId: string;
  readonly settlement: RuntimeToolSettlementDeclaration;
}, receipt: RuntimeDeclarationReceipt): RuntimeDeclarationReceipt["events"][number] {
  assertOrdinaryDeclarationReceipt(receipt);
  RuntimeToolSettlementDeclarationSchema.parse(input.settlement);
  assertReceiptIdentity(receipt, input);
  if (receipt.messages.length !== 0 || receipt.events.length !== 1 || receipt.interruptToolProjections.length !== 0) {
    throw new Error("Tool settlement receipt carrier set is invalid");
  }
  const stamp = receipt.events[0]!;
  if (stamp.sessionThreadId !== input.sessionThreadId || stamp.eventId !== input.eventId || stamp.disposition !== "created") {
    throw new Error("Tool settlement event stamp is invalid");
  }
  return stamp;
}

function applyMessageCreateStamps(input: {
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly creates: readonly RuntimeMessageCreate[];
  readonly eventStamps: RuntimeDeclarationReceipt["events"];
  readonly messageStamps: RuntimeDeclarationReceipt["messages"];
  readonly skipSessionValidation?: boolean | undefined;
}): DurableRuntimeMessage[] {
  if (input.creates.length !== input.messageStamps.length) {
    throw new Error("message create stamp count does not match");
  }
  let previousMessageSequence = -1;
  return input.creates.map((rawCreate, index) => {
    const create = RuntimeMessageCreateSchema.parse(rawCreate);
    const stamp = input.messageStamps[index]!;
    const owningEvent = input.eventStamps[index];
    if (
      stamp.sessionThreadId !== input.sessionThreadId ||
      stamp.disposition !== "created" ||
      owningEvent === undefined || owningEvent.sessionThreadId !== input.sessionThreadId ||
      (index > 0 && stamp.messageSequence !== previousMessageSequence + 1) ||
      stamp.parts.length !== create.parts.length
    ) {
      throw new Error("message create stamp is invalid");
    }
    previousMessageSequence = stamp.messageSequence;
    const parts = create.parts.map((part, partIndex) => durablePartFromStamp(
      input.sessionId,
      stamp.messageId,
      part,
      stamp.parts[partIndex]!,
      partIndex === 0 ? 0 : stamp.parts[partIndex - 1]!.partSequence + 1,
    ));
    return DurableRuntimeMessageSchema.parse({
      id: stamp.messageId,
      sessionId: input.skipSessionValidation ? input.sessionId : input.sessionId,
      role: create.role,
      origin: create.origin,
      sequence: stamp.messageSequence,
      status: create.status,
      createdAt: stamp.createdAt,
      ...(stamp.updatedAt.length > 0 ? { updatedAt: stamp.updatedAt } : {}),
      ...(create.error === undefined ? {} : { error: create.error }),
      ...(create.finishReason === undefined ? {} : { finishReason: create.finishReason }),
      ...(create.usage === undefined ? {} : { usage: create.usage }),
      ...(create.responseId === undefined ? {} : { responseId: create.responseId }),
      parts,
    });
  });
}

function durablePartFromStamp(
  sessionId: string,
  messageId: string,
  rawPart: RuntimePartCreate,
  stamp: RuntimeDeclarationReceipt["messages"][number]["parts"][number],
  expectedSequence: number | undefined,
): RuntimePart {
  const part = RuntimePartCreateSchema.parse(rawPart);
  if (
    stamp === undefined || stamp.messageId !== messageId ||
    stamp.disposition !== "created" ||
    (expectedSequence !== undefined && stamp.partSequence !== expectedSequence)
  ) {
    throw new Error("message part stamp is invalid or non-contiguous");
  }
  return {
    ...part,
    id: stamp.partId,
    sessionId,
    messageId,
    sequence: stamp.partSequence,
    createdAt: stamp.createdAt,
    ...(stamp.updatedAt.length > 0 ? { updatedAt: stamp.updatedAt } : {}),
  } as RuntimePart;
}

function messageCreateInfo(message: RuntimeMessage): Omit<RuntimeMessageCreate, "messageKind" | "parts"> {
  return {
    role: message.role,
    origin: message.origin,
    status: message.status,
    ...(message.error === undefined ? {} : { error: message.error }),
    ...(message.finishReason === undefined ? {} : { finishReason: message.finishReason }),
    ...(message.usage === undefined ? {} : { usage: message.usage }),
    ...(message.responseId === undefined ? {} : { responseId: message.responseId }),
  };
}

function partCreateFromDurable(part: RuntimePart): RuntimePartCreate {
  const { id: _id, sessionId: _sessionId, messageId: _messageId, sequence: _sequence, createdAt: _createdAt, updatedAt: _updatedAt, ...create } = part;
  return RuntimePartCreateSchema.parse(create);
}

function acceptedInputMessages(input: RuntimeAcceptedInputState): readonly RuntimeMessage[] {
  if (input.kind === "task_notification") return [];
  if (input.kind === "inter_agent_message") return [RuntimeMessageSchema.parse(input.message)];
  if (input.kind === "approval_review") return input.promptItems.map((message) => RuntimeMessageSchema.parse(message));
  if (input.kind !== "messages") return [];
  const parsed = JSON.parse(input.payloadJson) as { readonly messages?: unknown };
  if (!Array.isArray(parsed.messages)) throw new Error("accepted input payload has no messages");
  return parsed.messages.map((message) => RuntimeMessageSchema.parse(message));
}

function acceptedInputMessageKind(input: RuntimeAcceptedInputState): RuntimeMessageCreate["messageKind"] {
  switch (input.kind) {
    case "messages": return "user_input";
    case "inter_agent_message": return "agent_mail_input";
    case "approval_review": return "reviewer_input";
    case "rejection": return "rejection";
    case "task_notification": throw new Error("task notification uses its dedicated message kind");
  }
}

function assertCreatedEvents(events: RuntimeDeclarationReceipt["events"], sessionThreadId: string): void {
  const ids = new Set<string>();
  let previousSequence = -1;
  for (const event of events) {
    if (
      event.sessionThreadId !== sessionThreadId || event.disposition !== "created" ||
      ids.has(event.eventId) || event.eventSequence <= previousSequence
    ) {
      throw new Error("created event stamp order is invalid");
    }
    ids.add(event.eventId);
    previousSequence = event.eventSequence;
  }
}

function failureToolError(failure: RuntimeFailure): { readonly type: string; readonly message: string; readonly retryable: boolean } {
  return { type: failure.code, message: failure.message, retryable: failure.retryable };
}
