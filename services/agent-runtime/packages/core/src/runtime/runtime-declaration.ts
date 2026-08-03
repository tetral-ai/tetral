/**
 * Builds deterministic Runtime declarations and applies database receipts to
 * produce durable hot messages. It guards the boundary between unstamped loop
 * authorship and database-owned identities; the agent loop calls it around
 * accepted-input commits and the Bridge adapter only transports its values.
 */

import {
  DurableRuntimeMessageSchema,
  RuntimeMessageDraftSchema,
  RuntimeMessageSchema,
  RuntimePartDraftSchema,
} from "../contracts/runtime.js";
import type {
  DurableRuntimeMessage,
  RuntimeDeclarationReceipt,
  RuntimeMessage,
  RuntimeMessageDraft,
  RuntimeMessageInfo,
  RuntimeFailure,
  RuntimePart,
  RuntimePartDraft,
} from "../contracts/runtime.js";
import type {
  RuntimeAcceptedInputState,
  RuntimePendingApprovalToolJobState,
} from "../session/session-state.js";
import { stableRuntimeID } from "./runtime-identity.js";

export type { RuntimeDeclarationReceipt } from "../contracts/runtime.js";

type ToolRuntimePart = Extract<RuntimePart, { readonly type: "tool" }>;
type ToolRuntimePartDraft = Extract<RuntimePartDraft, { readonly type: "tool" }>;

function assertOrdinaryDeclarationReceipt(receipt: RuntimeDeclarationReceipt): void {
  if (receipt.compactedThroughMessageSequence !== undefined) {
    throw new Error("ordinary declaration receipt cannot carry a compaction boundary");
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

/** Converts one accepted command's semantic messages into deterministic drafts. */
export function acceptedInputDrafts(input: RuntimeAcceptedInputState): readonly RuntimeMessageDraft[] {
  if (input.kind === "task_notification") {
    throw new Error("task notification uses its dedicated declaration boundary");
  }
  const sourceKind = acceptedInputDeclarationKind(input);
  if (input.kind === "rejection") {
    if (input.eventIds.length === 0) {
      throw new Error("rejection input must name at least one source event");
    }
    return input.eventIds.map((sourceEventId, ordinal) => {
      const runtimeLocalId = stableRuntimeID(
        "runtime_message_draft",
        input.workspaceId,
        input.sessionId,
        input.sessionThreadId,
        sourceKind,
        input.runtimeInputId,
        "rejection",
        String(ordinal),
      );
      return RuntimeMessageDraftSchema.parse({
        runtimeLocalId,
        sourceKind: "rejection",
        sourceId: input.runtimeInputId,
        sourceEventId,
        draftKind: "rejection",
        ordinal,
        role: "assistant",
        origin: "agent",
        status: "completed",
        parts: [{
          runtimeLocalPartId: stableRuntimeID(
            "runtime_message_part_draft",
            runtimeLocalId,
            "text",
            "0",
          ),
          ordinal: 0,
          type: "text",
          text: "The session runtime could not accept this input.",
          truncated: false,
          status: "completed",
        }],
      });
    });
  }
  const messages = acceptedInputMessages(input);
  const draftKind = acceptedInputDraftKind(input);
  return messages.map((message, ordinal) => {
    const sourceEventId = input.eventIds[ordinal];
    if (sourceEventId === undefined) {
      throw new Error("accepted input message is missing its source event");
    }
    const runtimeLocalId = stableRuntimeID(
      "runtime_message_draft",
      input.workspaceId,
      input.sessionId,
      input.sessionThreadId,
      sourceKind,
      input.runtimeInputId,
      draftKind,
      String(ordinal),
    );
    const partKindOrdinals = new Map<string, number>();
    const parts = message.parts.map((part): RuntimePartDraft => {
      const partOrdinal = partKindOrdinals.get(part.type) ?? 0;
      partKindOrdinals.set(part.type, partOrdinal + 1);
      const {
        id: _id,
        sessionId: _sessionId,
        messageId: _messageId,
        sequence: _sequence,
        createdAt: _createdAt,
        updatedAt: _updatedAt,
        ...semanticPart
      } = part;
      return {
        ...semanticPart,
        runtimeLocalPartId: stableRuntimeID(
          "runtime_message_part_draft",
          runtimeLocalId,
          part.type,
          String(partOrdinal),
        ),
        ordinal: partOrdinal,
      };
    });
    const {
      id: _id,
      sessionId: _sessionId,
      sequence: _sequence,
      createdAt: _createdAt,
      updatedAt: _updatedAt,
      parts: _parts,
      ...messageInfo
    } = message;
    return RuntimeMessageDraftSchema.parse({
      ...messageInfo,
      runtimeLocalId,
      sourceKind,
      sourceId: input.runtimeInputId,
      sourceEventId,
      draftKind,
      ordinal,
      parts,
    });
  });
}

function assertPendingToolCancellationDeltas(
  expected: readonly {
    readonly toolUseEventId: string;
    readonly runtimeLocalId: string;
  }[],
  received: readonly string[],
): void {
  const pairKey = (runtimeLocalId: string, toolUseEventId: string): string =>
    `${runtimeLocalId}\u0000${toolUseEventId}`;
  const expectedPairs = new Set(
    expected.map((cancellation) => pairKey(cancellation.runtimeLocalId, cancellation.toolUseEventId)),
  );
  const receivedPairs = new Set<string>();
  for (const rawDelta of received) {
    const delta = JSON.parse(rawDelta) as Record<string, unknown>;
    const runtimeLocalId = typeof delta.runtime_local_id === "string" ? delta.runtime_local_id : "";
    const toolUseEventId = typeof delta.tool_use_event_id === "string" ? delta.tool_use_event_id : "";
    if (
      runtimeLocalId.length === 0 ||
      toolUseEventId.length === 0 ||
      delta.status !== "cancelled" ||
      typeof delta.result_event_id !== "string" ||
      receivedPairs.has(pairKey(runtimeLocalId, toolUseEventId))
    ) {
      throw new Error("declaration receipt has an invalid pending-tool delta");
    }
    receivedPairs.add(pairKey(runtimeLocalId, toolUseEventId));
  }
  if (
    receivedPairs.size !== expectedPairs.size ||
    [...expectedPairs].some((pair) => !receivedPairs.has(pair))
  ) {
    throw new Error("declaration receipt pending-tool identity does not match");
  }
}

function assertDisjointToolUseIds(
  pendingToolCancellations: readonly { readonly toolUseEventId: string }[],
  sandboxExecutionToolUseEventIds: readonly string[],
): void {
  const approvalIds = new Set(pendingToolCancellations.map((cancellation) => cancellation.toolUseEventId));
  const sandboxIds = new Set(sandboxExecutionToolUseEventIds);
  if (
    approvalIds.size !== pendingToolCancellations.length ||
    sandboxIds.size !== sandboxExecutionToolUseEventIds.length ||
    [...sandboxIds].some((toolUseEventId) => approvalIds.has(toolUseEventId))
  ) {
    throw new Error("approval and sandbox execution cancellation identities overlap");
  }
}

/** Applies a complete current-custody receipt to the exact submitted drafts. */
export function applyAcceptedInputReceipt(
  input: RuntimeAcceptedInputState,
  drafts: readonly RuntimeMessageDraft[],
  receipt: RuntimeDeclarationReceipt,
): readonly DurableRuntimeMessage[] {
  if (input.kind === "task_notification") {
    throw new Error("task notification uses its dedicated declaration receipt");
  }
  assertOrdinaryDeclarationReceipt(receipt);
  const sourceKind = acceptedInputDeclarationKind(input);
  if (
    receipt.sessionThreadId !== input.sessionThreadId ||
    receipt.operationKind !== "commit_inputs" ||
    receipt.sourceKind !== sourceKind ||
    receipt.sourceId !== input.runtimeInputId
  ) {
    throw new Error("declaration receipt identity does not match the accepted input");
  }
  const expectedEventIDs = new Set(input.eventIds);
  const eventByID = uniqueMap(receipt.events, (event) => event.eventId, "event");
  if (eventByID.size !== expectedEventIDs.size) {
    throw new Error("declaration receipt event stamp set is incomplete");
  }
  for (const event of receipt.events) {
    if (
      event.sessionThreadId !== input.sessionThreadId ||
      event.sourceEventId !== event.eventId ||
      event.disposition !== "existing" ||
      !expectedEventIDs.has(event.eventId)
    ) {
      throw new Error("declaration receipt contains an invalid event stamp");
    }
  }
  const messageByLocalID = uniqueMap(receipt.messages, (message) => message.runtimeLocalId, "message");
  if (messageByLocalID.size !== drafts.length) {
    throw new Error("declaration receipt message stamp set is incomplete");
  }
  return drafts.map((draft) => {
    const messageStamp = messageByLocalID.get(draft.runtimeLocalId);
    const eventStamp = messageStamp === undefined ? undefined : eventByID.get(messageStamp.owningEventId);
    if (
      messageStamp === undefined ||
      eventStamp === undefined ||
      messageStamp.sessionThreadId !== input.sessionThreadId ||
      messageStamp.owningEventId !== draft.sourceEventId ||
      messageStamp.disposition !== "created"
    ) {
      throw new Error("declaration receipt is missing a message or event stamp");
    }
    const partByLocalID = uniqueMap(messageStamp.parts, (part) => part.runtimeLocalPartId, "part");
    if (partByLocalID.size !== draft.parts.length) {
      throw new Error("declaration receipt part stamp set is incomplete");
    }
    const parts = draft.parts.map((part) => {
      const stamp = partByLocalID.get(part.runtimeLocalPartId);
      if (
        stamp === undefined ||
        stamp.messageId !== messageStamp.messageId ||
        stamp.disposition !== "created"
      ) {
        throw new Error("declaration receipt is missing a part stamp");
      }
      const {
        runtimeLocalPartId: _runtimeLocalPartId,
        ordinal: _ordinal,
        ...semanticPart
      } = part;
      return {
        ...semanticPart,
        id: stamp.partId,
        sessionId: input.sessionId,
        messageId: stamp.messageId,
        sequence: stamp.partSequence,
        createdAt: stamp.createdAt,
        ...(stamp.updatedAt.length > 0 ? { updatedAt: stamp.updatedAt } : {}),
      };
    });
    const {
      runtimeLocalId: _runtimeLocalId,
      sourceKind: _sourceKind,
      sourceId: _sourceId,
      sourceEventId: _sourceEventId,
      draftKind: _draftKind,
      ordinal: _ordinal,
      parts: _parts,
      ...messageInfo
    } = draft;
    return DurableRuntimeMessageSchema.parse({
      ...messageInfo,
      id: messageStamp.messageId,
      sessionId: input.sessionId,
      owningEventId: messageStamp.owningEventId,
      eventSequence: eventStamp.eventSequence,
      sequence: messageStamp.messageSequence,
      createdAt: messageStamp.createdAt,
      ...(messageStamp.updatedAt.length > 0 ? { updatedAt: messageStamp.updatedAt } : {}),
      parts,
    });
  });
}

/** Maps an internal accepted-input kind to the closed declaration kind carried on the Bridge wire. */
export function acceptedInputDeclarationKind(input: RuntimeAcceptedInputState): string {
  return input.kind === "inter_agent_message" ? "agent_mail" : input.kind;
}

/** Applies cancellation projections settled under one admitted interrupt event. */
export function applyInterruptInputReceipt(
  input: {
    readonly sessionId: string;
    readonly sessionThreadId: string;
    readonly runtimeInputId: string;
    readonly eventIds: readonly string[];
    readonly drafts: readonly RuntimeMessageDraft[];
    readonly pendingToolCancellations: readonly {
      readonly toolUseEventId: string;
      readonly runtimeLocalId: string;
    }[];
    readonly sandboxExecutionToolUseEventIds: readonly string[];
    readonly existingMessages: readonly RuntimeMessage[];
  },
  receipt: RuntimeDeclarationReceipt,
): readonly RuntimeMessage[] {
  assertOrdinaryDeclarationReceipt(receipt);
  if (
    input.eventIds.length !== 1 ||
    receipt.sessionThreadId !== input.sessionThreadId ||
    receipt.operationKind !== "commit_inputs" ||
    receipt.sourceKind !== "interrupt_control" ||
    receipt.sourceId !== input.runtimeInputId ||
    receipt.events.length !== 1
  ) {
    throw new Error("interrupt declaration receipt identity does not match");
  }
  const eventStamp = receipt.events[0];
  if (
    eventStamp === undefined ||
    eventStamp.sessionThreadId !== input.sessionThreadId ||
    eventStamp.sourceEventId !== input.eventIds[0] ||
    eventStamp.eventId !== input.eventIds[0] ||
    eventStamp.disposition !== "existing"
  ) {
    throw new Error("interrupt declaration receipt event stamp is invalid");
  }
  assertDisjointToolUseIds(
    input.pendingToolCancellations,
    input.sandboxExecutionToolUseEventIds,
  );
  assertPendingToolCancellationDeltas(input.pendingToolCancellations, receipt.pendingToolDelta);
  return applyCreatedControlDraftStamps({
    sessionId: input.sessionId,
    sessionThreadId: input.sessionThreadId,
    eventType: "user.interrupt",
    eventStamp,
    drafts: input.drafts,
    existingMessages: input.existingMessages,
  }, receipt.messages);
}

/** Builds the sole user-role declaration for one admitted approval decision. */
export function toolConfirmationDraft(input: {
  readonly workspaceId: string;
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly runtimeInputId: string;
  readonly sourceEventId: string;
  readonly toolUseEventId: string;
  readonly pendingTool: RuntimePendingApprovalToolJobState;
  readonly decision: "allow" | "deny";
  readonly denyMessage?: string | undefined;
}): RuntimeMessageDraft {
  if (
    input.pendingTool.toolUseEventId !== input.toolUseEventId ||
    input.pendingTool.toolPart.toolUseEventId !== input.toolUseEventId
  ) {
    throw new Error("tool confirmation does not identify the current pending tool");
  }
  const runtimeLocalId = stableRuntimeID(
    "runtime_message_draft",
    input.workspaceId,
    input.sessionId,
    input.sessionThreadId,
    "tool_confirmation",
    input.runtimeInputId,
    "approval_input",
    "0",
  );
  const text = input.decision === "allow"
    ? "Approval allowed"
    : input.denyMessage === undefined || input.denyMessage.length === 0
      ? "Approval denied"
      : `Approval denied: ${input.denyMessage}`;
  return RuntimeMessageDraftSchema.parse({
    runtimeLocalId,
    sourceKind: "tool_confirmation",
    sourceId: input.runtimeInputId,
    sourceEventId: input.sourceEventId,
    draftKind: "approval_input",
    ordinal: 0,
    role: "user",
    origin: "user",
    status: "completed",
    parts: [{
      runtimeLocalPartId: stableRuntimeID(
        "runtime_message_part_draft",
        runtimeLocalId,
        "text",
        "0",
      ),
      ordinal: 0,
      type: "text",
      text,
      truncated: false,
      status: "completed",
    }],
  });
}

/** Applies the database stamp for one admitted approval decision. */
export function applyToolConfirmationReceipt(input: {
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly runtimeInputId: string;
  readonly sourceEventId: string;
  readonly draft: RuntimeMessageDraft;
  readonly existingMessages: readonly RuntimeMessage[];
}, receipt: RuntimeDeclarationReceipt): readonly RuntimeMessage[] {
  assertOrdinaryDeclarationReceipt(receipt);
  if (
    receipt.sessionThreadId !== input.sessionThreadId ||
    receipt.operationKind !== "commit_inputs" ||
    receipt.sourceKind !== "tool_confirmation" ||
    receipt.sourceId !== input.runtimeInputId ||
    receipt.events.length !== 1 ||
    receipt.messages.length !== 1
  ) {
    throw new Error("tool confirmation declaration receipt identity does not match");
  }
  const eventStamp = receipt.events[0];
  if (
    eventStamp === undefined ||
    eventStamp.sessionThreadId !== input.sessionThreadId ||
    eventStamp.sourceEventId !== input.sourceEventId ||
    eventStamp.eventId !== input.sourceEventId ||
    eventStamp.disposition !== "existing"
  ) {
    throw new Error("tool confirmation declaration receipt event stamp is invalid");
  }
  return applyCreatedControlDraftStamps({
    sessionId: input.sessionId,
    sessionThreadId: input.sessionThreadId,
    eventType: "user.tool_confirmation",
    eventStamp,
    drafts: [input.draft],
    existingMessages: input.existingMessages,
  }, receipt.messages);
}

/** Builds cancellation declarations for pending tools settled by one admitted interrupt. */
export function interruptPendingToolDeclarations(input: {
  readonly workspaceId: string;
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly runtimeInputId: string;
  readonly sourceEventId: string;
  readonly pendingTools: readonly RuntimePendingApprovalToolJobState[];
  readonly pendingSandboxExecutions: readonly { readonly toolUseEventId: string }[];
  readonly failure: RuntimeFailure;
  readonly completedAt: string;
}): {
  readonly drafts: readonly RuntimeMessageDraft[];
  readonly pendingToolCancellations: readonly {
    readonly toolUseEventId: string;
    readonly runtimeLocalId: string;
  }[];
  readonly sandboxExecutionToolUseEventIds: readonly string[];
} {
  const runtimeLocalId = stableRuntimeID(
    "runtime_message_draft",
    input.workspaceId,
    input.sessionId,
    input.sessionThreadId,
    "interrupt_control",
    input.runtimeInputId,
    "cancellation",
    "0",
  );
  const parts = input.pendingTools.map((pending, ordinal) => {
    if (
      pending.toolPart.toolUseEventId !== pending.toolUseEventId ||
      (pending.toolPart.state.status !== "pending" && pending.toolPart.state.status !== "running")
    ) {
      throw new Error("interrupt pending tool is not cancellable");
    }
    const state = pending.toolPart.state.status === "running"
      ? {
          status: "cancelled" as const,
          input: pending.toolPart.state.input,
          error: {
            type: input.failure.type,
            message: input.failure.message,
            retryable: input.failure.retryable,
          },
        }
      : {
          status: "cancelled" as const,
          error: {
            type: input.failure.type,
            message: input.failure.message,
            retryable: input.failure.retryable,
          },
        };
    return {
      runtimeLocalPartId: stableRuntimeID(
        "runtime_message_part_draft",
        runtimeLocalId,
        "tool",
        String(ordinal),
      ),
      ordinal,
      type: "tool" as const,
      toolCallId: pending.toolPart.toolCallId,
      toolName: pending.toolPart.toolName,
      toolUseEventId: pending.toolUseEventId,
      ...(pending.toolPart.toolEvent === undefined ? {} : { toolEvent: pending.toolPart.toolEvent }),
      state,
      ...(pending.toolPart.startedAt === undefined ? {} : { startedAt: pending.toolPart.startedAt }),
      completedAt: input.completedAt,
    };
  });
  const drafts = parts.length === 0
    ? []
    : [RuntimeMessageDraftSchema.parse({
        runtimeLocalId,
        sourceKind: "interrupt_control",
        sourceId: input.runtimeInputId,
        sourceEventId: input.sourceEventId,
        draftKind: "cancellation",
        ordinal: 0,
        role: "assistant",
        origin: "agent",
        status: "completed",
        parts,
      })];
  return {
    drafts,
    pendingToolCancellations: input.pendingTools.map((pending) => ({
      toolUseEventId: pending.toolUseEventId,
      runtimeLocalId,
    })),
    sandboxExecutionToolUseEventIds: input.pendingSandboxExecutions.map((pending) => pending.toolUseEventId),
  };
}

/** Converts one live loop projection into the unstamped draft carried by WriteEvent. */
export function runtimeOutputDraft(
  input: {
    readonly workspaceId: string;
    readonly sessionId: string;
    readonly sessionThreadId: string;
    readonly runtimeWriteId: string;
    readonly eventType: string;
    readonly draftKind: RuntimeMessageDraft["draftKind"];
    readonly message: RuntimeMessageDraft;
  },
): RuntimeMessageDraft {
  const runtimeLocalId = stableRuntimeID(
    "runtime_message_draft",
    input.workspaceId,
    input.sessionId,
    input.sessionThreadId,
    input.eventType,
    input.runtimeWriteId,
    input.draftKind,
    "0",
  );
  const partKindOrdinals = new Map<string, number>();
  const parts = input.message.parts.map((part): RuntimePartDraft => {
    const ordinal = partKindOrdinals.get(part.type) ?? 0;
    partKindOrdinals.set(part.type, ordinal + 1);
    const {
      runtimeLocalPartId: _runtimeLocalPartId,
      ordinal: _ordinal,
      ...semanticPart
    } = part;
    return {
      ...semanticPart,
      runtimeLocalPartId: stableRuntimeID(
        "runtime_message_part_draft",
        runtimeLocalId,
        part.type,
        String(ordinal),
      ),
      ordinal,
    };
  });
  const {
    runtimeLocalId: _runtimeLocalId,
    sourceKind: _sourceKind,
    sourceId: _sourceId,
    sourceEventId: _sourceEventId,
    draftKind: _draftKind,
    ordinal: _ordinal,
    parts: _parts,
    ...messageInfo
  } = input.message;
  return RuntimeMessageDraftSchema.parse({
    ...messageInfo,
    runtimeLocalId,
    sourceKind: input.eventType,
    sourceId: input.runtimeWriteId,
    draftKind: input.draftKind,
    ordinal: 0,
    parts,
  });
}

/** Builds the user-role notification draft authored from one bounded terminal task fact. */
export function taskNotificationDraft(input: {
  readonly workspaceId: string;
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly runtimeInputId: string;
  readonly taskId: string;
  readonly payloadJson: string;
}): RuntimeMessageDraft {
  const sourceId = taskNotificationSourceId(input.runtimeInputId, input.taskId);
  const runtimeLocalId = stableRuntimeID(
    "runtime_message_draft",
    input.workspaceId,
    input.sessionId,
    input.sessionThreadId,
    "task_notification",
    sourceId,
    "task_notification",
    "0",
  );
  return RuntimeMessageDraftSchema.parse({
    runtimeLocalId,
    sourceKind: "task_notification",
    sourceId,
    draftKind: "task_notification",
    ordinal: 0,
    role: "user",
    origin: "runtime",
    status: "completed",
    parts: [{
      runtimeLocalPartId: stableRuntimeID(
        "runtime_message_part_draft",
        runtimeLocalId,
        "text",
        "0",
      ),
      ordinal: 0,
      type: "text",
      text: input.payloadJson,
      truncated: false,
      status: "completed",
    }],
  });
}

/** Derives the stable source identity shared by task-notification commit and replay. */
export function taskNotificationSourceId(runtimeInputId: string, taskId: string): string {
  return stableRuntimeID("task_notification", runtimeInputId, taskId);
}

/** Applies the database stamps for one loop-authored task notification. */
export function applyTaskNotificationReceipt(input: {
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly sourceId: string;
  readonly draft: RuntimeMessageDraft;
  readonly existingMessages: readonly DurableRuntimeMessage[];
}, receipt: RuntimeDeclarationReceipt): readonly DurableRuntimeMessage[] {
  assertOrdinaryDeclarationReceipt(receipt);
  if (
    receipt.sessionThreadId !== input.sessionThreadId ||
    receipt.operationKind !== "commit_task_notification_result" ||
    receipt.sourceKind !== "task_notification" ||
    receipt.sourceId !== input.sourceId ||
    receipt.events.length !== 1
  ) {
    throw new Error("task notification declaration receipt identity does not match");
  }
  const eventStamp = receipt.events[0];
  if (
    eventStamp === undefined ||
    eventStamp.sessionThreadId !== input.sessionThreadId ||
    eventStamp.sourceEventId !== input.sourceId ||
    eventStamp.disposition !== "created"
  ) {
    throw new Error("task notification declaration receipt event stamp is invalid");
  }
  return applyRuntimeDraftStamps({
    sessionId: input.sessionId,
    sessionThreadId: input.sessionThreadId,
    eventType: "runtime_notification",
    eventStamp,
    drafts: [input.draft],
    existingMessages: input.existingMessages,
  }, receipt.messages);
}

/** Builds the child-authored envelope declaration carried by FinishIdle. */
export function completionMailDraft(input: {
  readonly workspaceId: string;
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly durableTurnId: string;
  readonly envelope: string;
}): RuntimeMessageDraft {
  const runtimeLocalId = stableRuntimeID(
    "runtime_message_draft",
    input.workspaceId,
    input.sessionId,
    input.sessionThreadId,
    "finish_idle",
    input.durableTurnId,
    "completion_mail",
    "0",
  );
  return RuntimeMessageDraftSchema.parse({
    runtimeLocalId,
    sourceKind: "finish_idle",
    sourceId: input.durableTurnId,
    draftKind: "completion_mail",
    ordinal: 0,
    role: "user",
    origin: "runtime",
    status: "completed",
    parts: [{
      runtimeLocalPartId: stableRuntimeID(
        "runtime_message_part_draft",
        runtimeLocalId,
        "text",
        "0",
      ),
      ordinal: 0,
      type: "text",
      text: input.envelope,
      truncated: false,
      status: "completed",
    }],
  });
}

/** Builds one abnormal child completion envelope carried by runtime termination. */
export function runtimeTerminationCompletionMailDraft(input: {
  readonly workspaceId: string;
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly durableTurnId: string;
  readonly ordinal: number;
  readonly envelope: string;
}): RuntimeMessageDraft {
  const runtimeLocalId = stableRuntimeID(
    "runtime_message_draft",
    input.workspaceId,
    input.sessionId,
    input.sessionThreadId,
    "runtime_termination",
    input.durableTurnId,
    "completion_mail",
    String(input.ordinal),
  );
  return RuntimeMessageDraftSchema.parse({
    runtimeLocalId,
    sourceKind: "runtime_termination",
    sourceId: input.durableTurnId,
    draftKind: "completion_mail",
    ordinal: input.ordinal,
    role: "user",
    origin: "runtime",
    status: "completed",
    parts: [{
      runtimeLocalPartId: stableRuntimeID(
        "runtime_message_part_draft",
        runtimeLocalId,
        "text",
        "0",
      ),
      ordinal: 0,
      type: "text",
      text: input.envelope,
      truncated: false,
      status: "completed",
    }],
  });
}

/** Builds current-thread pending-tool cancellations for one live termination. */
export function runtimeTerminationToolDeclarations(input: {
  readonly workspaceId: string;
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly durableTurnId: string;
  readonly pendingTools: readonly RuntimePendingApprovalToolJobState[];
  readonly failure: RuntimeFailure;
  readonly completedAt: string;
}): {
  readonly drafts: readonly RuntimeMessageDraft[];
  readonly pendingToolCancellations: readonly {
    readonly toolUseEventId: string;
    readonly runtimeLocalId: string;
  }[];
} {
  const drafts = input.pendingTools.map((pending, ordinal) => {
    if (
      pending.toolPart.toolUseEventId !== pending.toolUseEventId ||
      (pending.toolPart.state.status !== "pending" && pending.toolPart.state.status !== "running")
    ) {
      throw new Error("runtime termination pending tool is not cancellable");
    }
    const runtimeLocalId = stableRuntimeID(
      "runtime_message_draft",
      input.workspaceId,
      input.sessionId,
      input.sessionThreadId,
      "runtime_termination",
      input.durableTurnId,
      "termination",
      String(ordinal),
    );
    const state = pending.toolPart.state.status === "running"
      ? {
          status: "cancelled" as const,
          input: pending.toolPart.state.input,
          error: {
            type: input.failure.type,
            message: input.failure.message,
            retryable: input.failure.retryable,
          },
        }
      : {
          status: "cancelled" as const,
          error: {
            type: input.failure.type,
            message: input.failure.message,
            retryable: input.failure.retryable,
          },
        };
    return RuntimeMessageDraftSchema.parse({
      runtimeLocalId,
      sourceKind: "runtime_termination",
      sourceId: input.durableTurnId,
      draftKind: "termination",
      ordinal,
      role: "assistant",
      origin: "agent",
      status: "completed",
      parts: [{
        runtimeLocalPartId: stableRuntimeID(
          "runtime_message_part_draft",
          runtimeLocalId,
          "tool",
          "0",
        ),
        ordinal: 0,
        type: "tool",
        toolCallId: pending.toolPart.toolCallId,
        toolName: pending.toolPart.toolName,
        toolUseEventId: pending.toolUseEventId,
        ...(pending.toolPart.toolEvent === undefined ? {} : { toolEvent: pending.toolPart.toolEvent }),
        state,
        ...(pending.toolPart.startedAt === undefined ? {} : { startedAt: pending.toolPart.startedAt }),
        completedAt: input.completedAt,
      }],
    });
  });
  return {
    drafts,
    pendingToolCancellations: drafts.map((draft, index) => ({
      toolUseEventId: input.pendingTools[index]!.toolUseEventId,
      runtimeLocalId: draft.runtimeLocalId,
    })),
  };
}

/** Validates the complete receipt returned for one live runtime termination. */
export function validateRuntimeTerminationReceipt(input: {
  readonly sessionThreadId: string;
  readonly durableTurnId: string;
  readonly drafts: readonly RuntimeMessageDraft[];
  readonly pendingToolCancellations: readonly {
    readonly toolUseEventId: string;
    readonly runtimeLocalId: string;
  }[];
  readonly sandboxExecutionToolUseEventIds: readonly string[];
}, receipt: RuntimeDeclarationReceipt): void {
  if (
    receipt.sessionThreadId !== input.sessionThreadId ||
    receipt.operationKind !== "commit_runtime_termination" ||
    receipt.sourceKind !== "runtime_termination" ||
    receipt.sourceId !== input.durableTurnId ||
    receipt.messages.length !== input.drafts.length ||
    receipt.events.length !== input.drafts.length + 2 ||
    receipt.pendingToolDelta.length !== input.pendingToolCancellations.length ||
    receipt.pendingAttachmentDelta.length !== 0 ||
    receipt.prefixConsumptions.length !== 0 ||
    receipt.requestReschedule !== undefined ||
    receipt.idleCloseout !== undefined ||
    receipt.compactedThroughMessageSequence !== undefined
  ) {
    throw new Error("runtime termination declaration receipt is incomplete");
  }
  const events = new Map(receipt.events.map((event) => [event.eventId, event]));
  const messages = new Map(receipt.messages.map((message) => [message.runtimeLocalId, message]));
  if (events.size !== receipt.events.length || messages.size !== receipt.messages.length) {
    throw new Error("runtime termination declaration receipt is duplicated");
  }
  for (const draft of input.drafts) {
    const message = messages.get(draft.runtimeLocalId);
    const event = message === undefined ? undefined : events.get(message.owningEventId);
    if (
      message === undefined ||
      event === undefined ||
      message.sessionThreadId !== input.sessionThreadId ||
      message.disposition !== "created" ||
      event.sessionThreadId !== input.sessionThreadId ||
      event.disposition !== "created" ||
      message.parts.length !== draft.parts.length
    ) {
      throw new Error("runtime termination declaration receipt has an invalid message stamp");
    }
    const parts = new Map(message.parts.map((part) => [part.runtimeLocalPartId, part]));
    if (
      parts.size !== message.parts.length ||
      draft.parts.some((part) => parts.get(part.runtimeLocalPartId)?.messageId !== message.messageId)
    ) {
      throw new Error("runtime termination declaration receipt has an invalid part stamp");
    }
  }
  assertDisjointToolUseIds(
    input.pendingToolCancellations,
    input.sandboxExecutionToolUseEventIds,
  );
  const messageEventIds = new Set(receipt.messages.map((message) => message.owningEventId));
  const terminalEvents = receipt.events.filter((event) => !messageEventIds.has(event.eventId));
  if (
    terminalEvents.length !== 2 ||
    terminalEvents.some((event) =>
      event.sessionThreadId !== input.sessionThreadId ||
      event.sourceEventId !== input.durableTurnId ||
      event.disposition !== "created"
    )
  ) {
    throw new Error("runtime termination declaration receipt has invalid terminal event stamps");
  }
  assertPendingToolCancellationDeltas(input.pendingToolCancellations, receipt.pendingToolDelta);
}

/** Validates the durable closeout stamp and any child completion-mail stamps. */
export function validateFinishIdleReceipt(
  input: {
    readonly sessionThreadId: string;
    readonly durableTurnId: string;
    readonly drafts: readonly RuntimeMessageDraft[];
  },
  receipt: RuntimeDeclarationReceipt,
): void {
  if (
    receipt.sessionThreadId !== input.sessionThreadId ||
    receipt.operationKind !== "finish_idle" ||
    receipt.sourceKind !== "turn_closeout" ||
    receipt.sourceId !== input.durableTurnId ||
    receipt.declarationDigest.length === 0 ||
    receipt.events.length !== input.drafts.length + 1 ||
    receipt.messages.length !== input.drafts.length ||
    receipt.pendingAttachmentDelta.length !== 0 ||
    receipt.pendingToolDelta.length !== 0 ||
    receipt.prefixConsumptions.length !== 0 ||
    receipt.requestReschedule !== undefined ||
    receipt.compactedThroughMessageSequence !== undefined
  ) {
    throw new Error("finish idle declaration receipt identity does not match");
  }
  const idleEvent = receipt.events[0];
  const idleCloseout = receipt.idleCloseout;
  if (
    idleEvent === undefined ||
    idleCloseout === undefined ||
    idleEvent.sessionThreadId !== input.sessionThreadId ||
    idleEvent.sourceEventId !== input.durableTurnId ||
    idleEvent.eventId !== idleCloseout.idleEventId ||
    idleEvent.eventSequence !== idleCloseout.idleEventSequence ||
    idleEvent.disposition !== "created" ||
    idleCloseout.durableTurnId !== input.durableTurnId ||
    idleCloseout.committedIdleAt.length === 0
  ) {
    throw new Error("finish idle declaration receipt closeout stamp is invalid");
  }
  const messagesByLocalID = uniqueMap(receipt.messages, (message) => message.runtimeLocalId, "message");
  for (const draft of input.drafts) {
    const message = messagesByLocalID.get(draft.runtimeLocalId);
    if (
      message === undefined ||
      message.sessionThreadId !== input.sessionThreadId ||
      message.disposition !== "created"
    ) {
      throw new Error("finish idle declaration receipt message stamp is invalid");
    }
    const owningEvent = receipt.events.find((event) => event.eventId === message.owningEventId);
    if (
      owningEvent === undefined ||
      owningEvent.sourceEventId === input.durableTurnId ||
      owningEvent.sessionThreadId !== input.sessionThreadId ||
      owningEvent.disposition !== "created"
    ) {
      throw new Error("finish idle declaration receipt completion event is invalid");
    }
    const partsByLocalID = uniqueMap(message.parts, (part) => part.runtimeLocalPartId, "part");
    if (
      partsByLocalID.size !== draft.parts.length ||
      draft.parts.some((part) => {
        const stamp = partsByLocalID.get(part.runtimeLocalPartId);
        return stamp === undefined ||
          stamp.messageId !== message.messageId ||
          stamp.disposition !== "created";
      })
    ) {
      throw new Error("finish idle declaration receipt part set is invalid");
    }
  }
}

/** Builds the unstamped checkpoint projected by a successful compaction request end. */
export function compactionCheckpointDraft(input: {
  readonly workspaceId: string;
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly modelRequestId: string;
  readonly text: string;
}): RuntimeMessageDraft {
  const runtimeLocalId = stableRuntimeID(
    "runtime_message_draft",
    input.workspaceId,
    input.sessionId,
    input.sessionThreadId,
    "agent.thread_context_compacted",
    input.modelRequestId,
    "compaction_checkpoint",
    "0",
  );
  return RuntimeMessageDraftSchema.parse({
    runtimeLocalId,
    sourceKind: "agent.thread_context_compacted",
    sourceId: input.modelRequestId,
    draftKind: "compaction_checkpoint",
    ordinal: 0,
    role: "user",
    origin: "runtime",
    status: "completed",
    parts: [{
      runtimeLocalPartId: stableRuntimeID(
        "runtime_message_part_draft",
        runtimeLocalId,
        "text",
        "0",
      ),
      ordinal: 0,
      type: "text",
      text: input.text,
      truncated: false,
      status: "completed",
    }],
  });
}

/** Applies a successful compaction request-end receipt to its checkpoint draft. */
export function applyCompactionReceipt(
  input: {
    readonly sessionId: string;
    readonly sessionThreadId: string;
    readonly modelRequestId: string;
    readonly requestEndEventId: string;
    readonly compactedThroughMessageSequence: number;
    readonly draft: RuntimeMessageDraft;
    readonly prefixConsumption?: {
      readonly childThreadId: string;
      readonly parentBoundaryEventId: string;
    } | undefined;
  },
  receipt: RuntimeDeclarationReceipt,
): DurableRuntimeMessage {
  if (
    receipt.sessionThreadId !== input.sessionThreadId ||
    receipt.operationKind !== "write_request_end" ||
    receipt.sourceKind !== "model_request" ||
    receipt.sourceId !== input.modelRequestId ||
    receipt.events.length !== 2 ||
    receipt.messages.length !== 1 ||
    receipt.compactedThroughMessageSequence !== input.compactedThroughMessageSequence
  ) {
    throw new Error("compaction declaration receipt identity does not match");
  }
  const requestEndEvent = receipt.events[0];
  const compactionEvent = receipt.events[1];
  const messageStamp = receipt.messages[0];
  if (
    requestEndEvent === undefined ||
    compactionEvent === undefined ||
    messageStamp === undefined ||
    requestEndEvent.sessionThreadId !== input.sessionThreadId ||
    compactionEvent.sessionThreadId !== input.sessionThreadId ||
    requestEndEvent.sourceEventId !== input.modelRequestId ||
    compactionEvent.sourceEventId !== input.modelRequestId ||
    requestEndEvent.eventId !== input.requestEndEventId ||
    requestEndEvent.disposition !== "created" ||
    compactionEvent.disposition !== "created" ||
    messageStamp.runtimeLocalId !== input.draft.runtimeLocalId ||
    messageStamp.sessionThreadId !== input.sessionThreadId ||
    messageStamp.owningEventId !== compactionEvent.eventId ||
    messageStamp.messageSequence !== input.compactedThroughMessageSequence + 1 ||
    messageStamp.disposition !== "created"
  ) {
    throw new Error("compaction declaration receipt stamp is invalid");
  }
  const prefixStamp = receipt.prefixConsumptions[0];
  if (input.prefixConsumption === undefined) {
    if (receipt.prefixConsumptions.length !== 0) {
      throw new Error("compaction declaration receipt contains an unsolicited prefix consumption");
    }
  } else if (
    receipt.prefixConsumptions.length !== 1 ||
    prefixStamp === undefined ||
    prefixStamp.childThreadId !== input.prefixConsumption.childThreadId ||
    prefixStamp.parentBoundaryEventId !== input.prefixConsumption.parentBoundaryEventId ||
    prefixStamp.checkpointMessageId !== messageStamp.messageId ||
    prefixStamp.disposition !== "consumed"
  ) {
    throw new Error("compaction declaration receipt prefix consumption is invalid");
  }
  const partByLocalID = uniqueMap(messageStamp.parts, (part) => part.runtimeLocalPartId, "part");
  if (partByLocalID.size !== input.draft.parts.length) {
    throw new Error("compaction declaration receipt part set is incomplete");
  }
  const parts = input.draft.parts.map((part) => {
    const stamp = partByLocalID.get(part.runtimeLocalPartId);
    if (
      stamp === undefined ||
      stamp.messageId !== messageStamp.messageId ||
      stamp.disposition !== "created"
    ) {
      throw new Error("compaction declaration receipt part stamp is invalid");
    }
    const {
      runtimeLocalPartId: _runtimeLocalPartId,
      ordinal: _ordinal,
      ...semanticPart
    } = part;
    return {
      ...semanticPart,
      id: stamp.partId,
      sessionId: input.sessionId,
      messageId: messageStamp.messageId,
      sequence: stamp.partSequence,
      createdAt: stamp.createdAt,
      ...(stamp.updatedAt.length > 0 ? { updatedAt: stamp.updatedAt } : {}),
    };
  });
  const {
    runtimeLocalId: _runtimeLocalId,
    sourceKind: _sourceKind,
    sourceId: _sourceId,
    sourceEventId: _sourceEventId,
    draftKind: _draftKind,
    ordinal: _ordinal,
    parts: _parts,
    ...messageInfo
  } = input.draft;
  return DurableRuntimeMessageSchema.parse({
    ...messageInfo,
    id: messageStamp.messageId,
    sessionId: input.sessionId,
    owningEventId: compactionEvent.eventId,
    eventSequence: compactionEvent.eventSequence,
    sequence: messageStamp.messageSequence,
    createdAt: messageStamp.createdAt,
    ...(messageStamp.updatedAt.length > 0 ? { updatedAt: messageStamp.updatedAt } : {}),
    parts,
  });
}

/** Creates the unstamped working assistant buffer for one model request. */
export function runtimeWorkingAssistantDraft(
  input: {
    readonly workspaceId: string;
    readonly sessionId: string;
    readonly sessionThreadId: string;
    readonly modelRequestId: string;
  },
): RuntimeMessageDraft {
  return RuntimeMessageDraftSchema.parse({
    runtimeLocalId: stableRuntimeID(
      "runtime_working_message",
      input.workspaceId,
      input.sessionId,
      input.sessionThreadId,
      input.modelRequestId,
    ),
    sourceKind: "model_request",
    sourceId: input.modelRequestId,
    draftKind: "assistant_text",
    ordinal: 0,
    role: "assistant",
    origin: "agent",
    status: "streaming",
    parts: [],
  });
}

/** Rehydrates one durable assistant message into an unstamped working buffer. */
export function runtimeWorkingDraftFromDurable(
  input: {
    readonly workspaceId: string;
    readonly sessionThreadId: string;
    readonly modelRequestId: string;
    readonly message: DurableRuntimeMessage;
  },
): RuntimeMessageDraft {
  const working = runtimeWorkingAssistantDraft({
    workspaceId: input.workspaceId,
    sessionId: input.message.sessionId,
    sessionThreadId: input.sessionThreadId,
    modelRequestId: input.modelRequestId,
  });
  const partKindOrdinals = new Map<string, number>();
  const parts = input.message.parts.map((part): RuntimePartDraft => {
    const ordinal = partKindOrdinals.get(part.type) ?? 0;
    partKindOrdinals.set(part.type, ordinal + 1);
    const {
      id: _id,
      sessionId: _sessionId,
      messageId: _messageId,
      sequence: _sequence,
      createdAt: _createdAt,
      updatedAt: _updatedAt,
      ...semanticPart
    } = part;
    return RuntimePartDraftSchema.parse({
      ...semanticPart,
      runtimeLocalPartId: stableRuntimeID(
        "runtime_message_part_draft",
        working.runtimeLocalId,
        part.type,
        String(ordinal),
      ),
      ordinal,
    });
  });
  const {
    id: _id,
    sessionId: _sessionId,
    owningEventId: _owningEventId,
    eventSequence: _eventSequence,
    sequence: _sequence,
    createdAt: _createdAt,
    updatedAt: _updatedAt,
    parts: _parts,
    ...messageInfo
  } = input.message;
  return RuntimeMessageDraftSchema.parse({
    ...working,
    ...messageInfo,
    parts,
  });
}

/** Rehydrates the one durable pending tool needed by an approval settlement. */
export function runtimeWorkingDraftForPendingTool(
  input: {
    readonly workspaceId: string;
    readonly sessionId: string;
    readonly sessionThreadId: string;
    readonly modelRequestId: string;
    readonly message: DurableRuntimeMessage;
    readonly part: ToolRuntimePart;
  },
): RuntimeMessageDraft {
  if (
    input.part.messageId !== input.message.id ||
    !input.message.parts.some((part) => part.id === input.part.id)
  ) {
    throw new Error("pending tool use is not attached to its durable message");
  }
  return runtimeWorkingDraftFromDurable({
    workspaceId: input.workspaceId,
    sessionThreadId: input.sessionThreadId,
    modelRequestId: input.modelRequestId,
    message: input.message,
  });
}

/** Creates the single unstamped invalid-tool repair declaration. */
export function runtimeInternalToolRepairDraft(
  input: {
    readonly workspaceId: string;
    readonly sessionId: string;
    readonly sessionThreadId: string;
    readonly repairKey: string;
    readonly part: ToolRuntimePartDraft;
  },
): RuntimeMessageDraft {
  const runtimeLocalId = stableRuntimeID(
    "runtime_message_draft",
    input.workspaceId,
    input.sessionId,
    input.sessionThreadId,
    "internal_tool_repair",
    input.repairKey,
    "internal_tool_repair",
    "0",
  );
  const {
    runtimeLocalPartId: _runtimeLocalPartId,
    ordinal: _ordinal,
    ...semanticPart
  } = input.part;
  return RuntimeMessageDraftSchema.parse({
    runtimeLocalId,
    sourceKind: "internal_tool_repair",
    sourceId: input.repairKey,
    draftKind: "internal_tool_repair",
    ordinal: 0,
    role: "assistant",
    origin: "agent",
    status: "completed",
    parts: [{
      ...semanticPart,
      runtimeLocalPartId: stableRuntimeID(
        "runtime_message_part_draft",
        runtimeLocalId,
        "tool",
        "0",
      ),
      ordinal: 0,
    }],
  });
}

/** Applies the database identities returned for one internal repair draft. */
export function applyInternalToolRepairReceipt(
  input: {
    readonly sessionId: string;
    readonly sessionThreadId: string;
    readonly repairKey: string;
    readonly eventId: string;
    readonly draft: RuntimeMessageDraft;
  },
  receipt: RuntimeDeclarationReceipt,
): DurableRuntimeMessage {
  assertOrdinaryDeclarationReceipt(receipt);
  if (
    receipt.sessionThreadId !== input.sessionThreadId ||
    receipt.operationKind !== "commit_internal_tool_repair" ||
    receipt.sourceKind !== "internal_tool_repair" ||
    receipt.sourceId !== input.repairKey ||
    receipt.events.length !== 1 ||
    receipt.messages.length !== 1
  ) {
    throw new Error("internal tool repair declaration receipt identity does not match");
  }
  const eventStamp = receipt.events[0];
  const messageStamp = receipt.messages[0];
  if (
    eventStamp === undefined ||
    messageStamp === undefined ||
    eventStamp.sessionThreadId !== input.sessionThreadId ||
    eventStamp.sourceEventId !== input.repairKey ||
    eventStamp.eventId !== input.eventId ||
    eventStamp.disposition !== "created" ||
    messageStamp.runtimeLocalId !== input.draft.runtimeLocalId ||
    messageStamp.sessionThreadId !== input.sessionThreadId ||
    messageStamp.owningEventId !== input.eventId ||
    messageStamp.disposition !== "created"
  ) {
    throw new Error("internal tool repair declaration receipt stamp is invalid");
  }
  const partByLocalID = uniqueMap(messageStamp.parts, (part) => part.runtimeLocalPartId, "part");
  if (partByLocalID.size !== input.draft.parts.length) {
    throw new Error("internal tool repair declaration receipt part set is incomplete");
  }
  const parts = input.draft.parts.map((part) => {
    const stamp = partByLocalID.get(part.runtimeLocalPartId);
    if (
      stamp === undefined ||
      stamp.messageId !== messageStamp.messageId ||
      stamp.disposition !== "created"
    ) {
      throw new Error("internal tool repair declaration receipt part stamp is invalid");
    }
    const {
      runtimeLocalPartId: _runtimeLocalPartId,
      ordinal: _ordinal,
      ...semanticPart
    } = part;
    return {
      ...semanticPart,
      id: stamp.partId,
      sessionId: input.sessionId,
      messageId: messageStamp.messageId,
      sequence: stamp.partSequence,
      createdAt: stamp.createdAt,
      ...(stamp.updatedAt.length > 0 ? { updatedAt: stamp.updatedAt } : {}),
    };
  });
  const {
    runtimeLocalId: _runtimeLocalId,
    sourceKind: _sourceKind,
    sourceId: _sourceId,
    sourceEventId: _sourceEventId,
    draftKind: _draftKind,
    ordinal: _ordinal,
    parts: _parts,
    ...messageInfo
  } = input.draft;
  return DurableRuntimeMessageSchema.parse({
    ...messageInfo,
    id: messageStamp.messageId,
    sessionId: input.sessionId,
    owningEventId: input.eventId,
    eventSequence: eventStamp.eventSequence,
    sequence: messageStamp.messageSequence,
    createdAt: messageStamp.createdAt,
    ...(messageStamp.updatedAt.length > 0 ? { updatedAt: messageStamp.updatedAt } : {}),
    parts,
  });
}

/** Applies one current-custody WriteEvent receipt to its exact live draft. */
export function applyRuntimeOutputReceipt(
  input: {
    readonly sessionId: string;
    readonly sessionThreadId: string;
    readonly runtimeWriteId: string;
    readonly eventType: string;
    readonly eventId: string;
    readonly drafts: readonly RuntimeMessageDraft[];
    readonly existingMessages: readonly DurableRuntimeMessage[];
  },
  receipt: RuntimeDeclarationReceipt,
): readonly DurableRuntimeMessage[] {
  assertOrdinaryDeclarationReceipt(receipt);
  if (
    receipt.sessionThreadId !== input.sessionThreadId ||
    receipt.operationKind !== "write_event" ||
    receipt.sourceKind !== input.eventType ||
    receipt.sourceId !== input.runtimeWriteId ||
    receipt.events.length !== 1
  ) {
    throw new Error("write event declaration receipt identity does not match");
  }
  const eventStamp = receipt.events[0];
  if (
    eventStamp === undefined ||
    eventStamp.sessionThreadId !== input.sessionThreadId ||
    eventStamp.sourceEventId !== input.runtimeWriteId ||
    eventStamp.eventId !== input.eventId ||
    eventStamp.disposition !== "created"
  ) {
    throw new Error("write event declaration receipt event stamp is invalid");
  }
  return applyRuntimeDraftStamps({
    sessionId: input.sessionId,
    sessionThreadId: input.sessionThreadId,
    eventType: input.eventType,
    eventStamp,
    drafts: input.drafts,
    existingMessages: input.existingMessages,
  }, receipt.messages);
}

/** Applies one ordinary request-end assistant seal to the current hot projection. */
export function applyRuntimeRequestEndReceipt(
  input: {
    readonly sessionId: string;
    readonly sessionThreadId: string;
    readonly modelRequestId: string;
    readonly eventId: string;
    readonly drafts: readonly RuntimeMessageDraft[];
    readonly existingMessages: readonly DurableRuntimeMessage[];
    readonly expectedMessage?: DurableRuntimeMessage;
  },
  receipt: RuntimeDeclarationReceipt,
): readonly DurableRuntimeMessage[] {
  assertOrdinaryDeclarationReceipt(receipt);
  if (
    receipt.sessionThreadId !== input.sessionThreadId ||
    receipt.operationKind !== "write_request_end" ||
    receipt.sourceKind !== "model_request" ||
    receipt.sourceId !== input.modelRequestId ||
    receipt.events.length !== 1
  ) {
    throw new Error("request end declaration receipt identity does not match");
  }
  const eventStamp = receipt.events[0];
  if (
    eventStamp === undefined ||
    eventStamp.sessionThreadId !== input.sessionThreadId ||
    eventStamp.sourceEventId !== input.modelRequestId ||
    eventStamp.eventId !== input.eventId ||
    eventStamp.disposition !== "created"
  ) {
    throw new Error("request end declaration receipt event stamp is invalid");
  }
  return applyRuntimeDraftStamps({
    sessionId: input.sessionId,
    sessionThreadId: input.sessionThreadId,
    eventType: "model_request",
    eventStamp,
    drafts: input.drafts,
    existingMessages: input.existingMessages,
    ...(input.expectedMessage === undefined ? {} : { expectedMessage: input.expectedMessage }),
  }, receipt.messages);
}

function applyRuntimeDraftStamps(
  input: {
    readonly sessionId: string;
    readonly sessionThreadId: string;
    readonly eventType: string;
    readonly eventStamp: RuntimeDeclarationReceipt["events"][number];
    readonly drafts: readonly RuntimeMessageDraft[];
    readonly existingMessages: readonly DurableRuntimeMessage[];
    readonly expectedMessage?: DurableRuntimeMessage;
  },
  messageStamps: RuntimeDeclarationReceipt["messages"],
): readonly DurableRuntimeMessage[] {
  const eventStamp = input.eventStamp;
  const messageByLocalID = uniqueMap(messageStamps, (message) => message.runtimeLocalId, "message");
  if (messageByLocalID.size !== input.drafts.length) {
    throw new Error("write event declaration receipt message stamp set is incomplete");
  }
  const stampedMessages = input.drafts.map((draft) => {
    const messageStamp = messageByLocalID.get(draft.runtimeLocalId);
    if (
      messageStamp === undefined ||
      messageStamp.sessionThreadId !== input.sessionThreadId
    ) {
      throw new Error("write event declaration receipt message stamp is invalid");
    }
    const existing = input.existingMessages.find((message) => message.id === messageStamp.messageId);
    if (
      input.expectedMessage !== undefined &&
      (
        messageStamp.messageId !== input.expectedMessage.id ||
        messageStamp.owningEventId !== input.expectedMessage.owningEventId ||
        messageStamp.messageSequence !== input.expectedMessage.sequence ||
        messageStamp.createdAt !== input.expectedMessage.createdAt ||
        messageStamp.disposition !== "updated"
      )
    ) {
      throw new Error("request end declaration receipt replaced its durable assistant message");
    }
    if (
      input.expectedMessage === undefined &&
      input.eventType === "model_request" &&
      (
        messageStamp.disposition !== "created" ||
        messageStamp.owningEventId !== eventStamp.eventId ||
        draft.parts.some((part) => part.type !== "reasoning")
      )
    ) {
      throw new Error("request end declaration receipt created an invalid assistant message");
    }
    const eventSequence = messageStamp.owningEventId === eventStamp.eventId
      ? eventStamp.eventSequence
      : existing?.eventSequence;
    if (
      eventSequence === undefined ||
      (messageStamp.disposition === "created" && existing !== undefined) ||
      (messageStamp.disposition === "updated" &&
        (existing === undefined || existing.owningEventId !== messageStamp.owningEventId))
    ) {
      throw new Error("write event declaration receipt cannot resolve the owning event");
    }
    const partByLocalID = uniqueMap(messageStamp.parts, (part) => part.runtimeLocalPartId, "part");
    if (partByLocalID.size !== draft.parts.length) {
      throw new Error("write event declaration receipt part stamp set is incomplete");
    }
    const existingPartByAssociation = runtimeDeclarationPartMap(existing?.parts ?? []);
    const draftAssociations = runtimeDeclarationPartAssociations(draft.parts);
    const existingPartIDs = new Set((existing?.parts ?? []).map((part) => part.id));
    const parts = draft.parts.map((part, partIndex) => {
      const stamp = partByLocalID.get(part.runtimeLocalPartId);
      if (stamp === undefined || stamp.messageId !== messageStamp.messageId) {
        throw new Error("write event declaration receipt part stamp is invalid");
      }
      const priorPart = existingPartByAssociation.get(draftAssociations[partIndex] ?? "");
      if (
        (priorPart !== undefined &&
          (stamp.partId !== priorPart.id || stamp.disposition !== "updated")) ||
        (priorPart === undefined &&
          (existingPartIDs.has(stamp.partId) || stamp.disposition !== "created"))
      ) {
        throw new Error("write event declaration receipt changed a durable part association");
      }
      const {
        runtimeLocalPartId: _runtimeLocalPartId,
        ordinal: _ordinal,
        ...semanticPart
      } = part;
      const existingPart = existing?.parts.find((candidate) => candidate.id === stamp.partId);
      const toolAssociation =
        semanticPart.type === "tool" &&
        semanticPart.toolUseEventId === undefined &&
        (
          (input.eventType === "agent.tool_use" && semanticPart.toolEvent?.kind === "tool") ||
          (input.eventType === "agent.mcp_tool_use" && semanticPart.toolEvent?.kind === "mcp")
        )
          ? {
              toolUseEventId: eventStamp.eventId,
              toolEvent: input.eventType === "agent.mcp_tool_use"
                ? semanticPart.toolEvent
                : { kind: "tool" as const },
            }
          : semanticPart.type === "tool" && semanticPart.toolUseEventId === undefined &&
              existingPart?.type === "tool" && existingPart.toolUseEventId !== undefined
            ? {
                toolUseEventId: existingPart.toolUseEventId,
                ...(existingPart.toolEvent !== undefined ? { toolEvent: existingPart.toolEvent } : {}),
              }
            : {};
      return {
        ...semanticPart,
        ...toolAssociation,
        id: stamp.partId,
        sessionId: input.sessionId,
        messageId: stamp.messageId,
        sequence: stamp.partSequence,
        createdAt: stamp.createdAt,
        ...(stamp.updatedAt.length > 0 ? { updatedAt: stamp.updatedAt } : {}),
      };
    });
    const {
      runtimeLocalId: _runtimeLocalId,
      sourceKind: _sourceKind,
      sourceId: _sourceId,
      sourceEventId: _sourceEventId,
      draftKind: _draftKind,
      ordinal: _ordinal,
      parts: _parts,
      ...messageInfo
    } = draft;
    return DurableRuntimeMessageSchema.parse({
      ...messageInfo,
      id: messageStamp.messageId,
      sessionId: input.sessionId,
      owningEventId: messageStamp.owningEventId,
      eventSequence,
      sequence: messageStamp.messageSequence,
      createdAt: messageStamp.createdAt,
      ...(messageStamp.updatedAt.length > 0 ? { updatedAt: messageStamp.updatedAt } : {}),
      parts,
    });
  });
  const stampedByMessageID = uniqueMap(stampedMessages, (message) => message.id, "stamped message");
  return [
    ...input.existingMessages.map((message) => stampedByMessageID.get(message.id) ?? message),
    ...stampedMessages.filter(
      (message) => !input.existingMessages.some((existing) => existing.id === message.id),
    ),
  ];
}

function applyCreatedControlDraftStamps(
  input: {
    readonly sessionId: string;
    readonly sessionThreadId: string;
    readonly eventType: string;
    readonly eventStamp: RuntimeDeclarationReceipt["events"][number];
    readonly drafts: readonly RuntimeMessageDraft[];
    readonly existingMessages: readonly RuntimeMessage[];
  },
  messageStamps: RuntimeDeclarationReceipt["messages"],
): readonly RuntimeMessage[] {
  const stamped = applyRuntimeDraftStamps({
    sessionId: input.sessionId,
    sessionThreadId: input.sessionThreadId,
    eventType: input.eventType,
    eventStamp: input.eventStamp,
    drafts: input.drafts,
    existingMessages: [],
  }, messageStamps);
  if (stamped.length === 0) {
    return input.existingMessages;
  }
  const existingByID = uniqueMap(input.existingMessages, (message) => message.id, "existing message");
  const existingStamped = stamped.filter((message) => existingByID.has(message.id));
  if (existingStamped.length === 0) {
    return [...input.existingMessages, ...stamped];
  }
  if (
    existingStamped.length !== stamped.length ||
    stamped.some((message) => !sameRuntimeValue(existingByID.get(message.id), message))
  ) {
    throw new Error("control declaration receipt conflicts with the installed durable projection");
  }
  return input.existingMessages;
}

function sameRuntimeValue(left: unknown, right: unknown): boolean {
  if (Object.is(left, right)) {
    return true;
  }
  if (Array.isArray(left) || Array.isArray(right)) {
    return Array.isArray(left) &&
      Array.isArray(right) &&
      left.length === right.length &&
      left.every((value, index) => sameRuntimeValue(value, right[index]));
  }
  if (
    left === null ||
    right === null ||
    typeof left !== "object" ||
    typeof right !== "object"
  ) {
    return false;
  }
  const leftRecord = left as Record<string, unknown>;
  const rightRecord = right as Record<string, unknown>;
  const leftKeys = Object.keys(leftRecord).filter((key) => leftRecord[key] !== undefined).sort();
  const rightKeys = Object.keys(rightRecord).filter((key) => rightRecord[key] !== undefined).sort();
  return leftKeys.length === rightKeys.length &&
    leftKeys.every((key, index) =>
      key === rightKeys[index] && sameRuntimeValue(leftRecord[key], rightRecord[key])
    );
}

function runtimeDeclarationPartMap(
  parts: readonly RuntimePart[],
): ReadonlyMap<string, RuntimePart> {
  const associations = runtimeDeclarationPartAssociations(parts);
  const result = new Map<string, RuntimePart>();
  parts.forEach((part, index) => {
    const association = associations[index] ?? "";
    if (result.has(association)) {
      throw new Error("durable runtime message has duplicate part associations");
    }
    result.set(association, part);
  });
  return result;
}

function runtimeDeclarationPartAssociations(
  parts: readonly (RuntimePart | RuntimePartDraft)[],
): readonly string[] {
  const kindOrdinals = new Map<RuntimePart["type"], number>();
  return parts.map((part) => {
    const ordinal = kindOrdinals.get(part.type) ?? 0;
    kindOrdinals.set(part.type, ordinal + 1);
    if (part.type === "tool") {
      return `tool:${part.toolCallId}`;
    }
    if (part.type === "reasoning" && part.providerPartId !== undefined) {
      return `reasoning:${part.providerPartId}`;
    }
    return `${part.type}:${ordinal}`;
  });
}

function uniqueMap<T>(
  values: readonly T[],
  keyOf: (value: T) => string,
  label: string,
): ReadonlyMap<string, T> {
  const result = new Map<string, T>();
  for (const value of values) {
    const key = keyOf(value);
    if (result.has(key)) {
      throw new Error(`declaration receipt contains a duplicate ${label} stamp`);
    }
    result.set(key, value);
  }
  return result;
}

function acceptedInputMessages(input: RuntimeAcceptedInputState): readonly RuntimeMessage[] {
  if (input.kind === "task_notification") {
    return [];
  }
  if (input.kind === "inter_agent_message") {
    return [RuntimeMessageSchema.parse(input.message)];
  }
  if (input.kind === "approval_review") {
    return input.promptItems.map((message) => RuntimeMessageSchema.parse(message));
  }
  if (input.kind !== "messages") {
    return [];
  }
  const parsed = JSON.parse(input.payloadJson) as { readonly messages?: unknown };
  if (!Array.isArray(parsed.messages)) {
    throw new Error("accepted input payload has no messages");
  }
  return parsed.messages.map((message) => RuntimeMessageSchema.parse(message));
}

function acceptedInputDraftKind(input: RuntimeAcceptedInputState): RuntimeMessageDraft["draftKind"] {
  switch (input.kind) {
    case "messages":
      return "user_input";
    case "inter_agent_message":
      return "agent_mail_input";
    case "approval_review":
      return "reviewer_input";
    case "rejection":
      return "rejection";
    case "task_notification":
      throw new Error("task notification uses its dedicated draft kind");
  }
}
