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
} from "../contracts/runtime.js";
import type {
  DurableRuntimeMessage,
  RuntimeMessage,
  RuntimeMessageDraft,
  RuntimePartDraft,
} from "../contracts/runtime.js";
import type { RuntimeAcceptedInputState } from "../session/session-state.js";
import type { ProviderRequestAttachment } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { stableRuntimeID } from "./runtime-identity.js";

export interface RuntimeDurableEventStamp {
  readonly sessionThreadId: string;
  readonly sourceEventId: string;
  readonly eventId: string;
  readonly eventSequence: number;
  readonly disposition: "existing" | "created";
}

export interface RuntimeDurablePartStamp {
  readonly runtimeLocalPartId: string;
  readonly partId: string;
  readonly messageId: string;
  readonly partSequence: number;
  readonly createdAt: string;
  readonly updatedAt: string;
  readonly disposition: "created" | "updated";
}

export interface RuntimeDurableMessageStamp {
  readonly runtimeLocalId: string;
  readonly sessionThreadId: string;
  readonly owningEventId: string;
  readonly messageId: string;
  readonly messageSequence: number;
  readonly createdAt: string;
  readonly updatedAt: string;
  readonly disposition: "created" | "updated";
  readonly parts: readonly RuntimeDurablePartStamp[];
}

export interface RuntimeDeclarationReceipt {
  readonly sessionThreadId: string;
  readonly operationKind: string;
  readonly sourceKind: string;
  readonly sourceId: string;
  readonly declarationDigest: string;
  readonly events: readonly RuntimeDurableEventStamp[];
  readonly messages: readonly RuntimeDurableMessageStamp[];
  readonly pendingAttachmentDelta: readonly ProviderRequestAttachment[];
}

/** Converts one accepted command's semantic messages into deterministic drafts. */
export function acceptedInputDrafts(input: RuntimeAcceptedInputState): readonly RuntimeMessageDraft[] {
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
      input.kind,
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
      sourceKind: input.kind,
      sourceId: input.runtimeInputId,
      sourceEventId,
      draftKind,
      ordinal,
      parts,
    });
  });
}

/** Applies a complete current-custody receipt to the exact submitted drafts. */
export function applyAcceptedInputReceipt(
  input: RuntimeAcceptedInputState,
  drafts: readonly RuntimeMessageDraft[],
  receipt: RuntimeDeclarationReceipt,
): readonly DurableRuntimeMessage[] {
  if (
    receipt.sessionThreadId !== input.sessionThreadId ||
    receipt.operationKind !== "commit_inputs" ||
    receipt.sourceKind !== input.kind ||
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
  }
}
