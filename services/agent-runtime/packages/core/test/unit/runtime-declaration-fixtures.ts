import type { AcceptedInputCommitResult } from "../../src/context/context-loader.js";
import { acceptedInputDrafts } from "../../src/runtime/runtime-declaration.js";
import type { RuntimeDeclarationReceipt } from "../../src/runtime/runtime-declaration.js";
import type { RuntimeAcceptedInputState } from "../../src/session/session-state.js";

const committedAt = "2026-07-28T00:00:00.000Z";

/** Builds a complete database-shaped receipt for accepted-input unit tests. */
export function acceptedInputReceipt(
  input: RuntimeAcceptedInputState,
  inputDisposition: AcceptedInputCommitResult["inputDisposition"] = "committed",
  messageSequenceStart = 1,
): AcceptedInputCommitResult {
  const drafts = input.kind === "inter_agent_message" && input.presentation === "pull"
    ? []
    : acceptedInputDrafts(input);
  const receipt: RuntimeDeclarationReceipt = {
    sessionThreadId: input.sessionThreadId,
    operationKind: "commit_inputs",
    sourceKind: input.kind,
    sourceId: input.runtimeInputId,
    declarationDigest: `digest_${input.runtimeInputId}`,
    pendingAttachmentDelta: [],
    pendingToolDelta: [],
    prefixConsumptions: [],

    childLifecycle: [],
    events: input.eventIds.map((eventId, index) => ({
      sessionThreadId: input.sessionThreadId,
      sourceEventId: eventId,
      eventId,
      eventSequence: input.sequenceFrom + index,
      disposition: "existing",
    })),
    messages: drafts.map((draft, messageIndex) => {
      if (draft.sourceEventId === undefined) {
        throw new Error("accepted input test draft is missing its source event");
      }
      return {
        runtimeLocalId: draft.runtimeLocalId,
        sessionThreadId: input.sessionThreadId,
        owningEventId: draft.sourceEventId,
        messageId: `msg_${draft.runtimeLocalId}`,
        messageSequence: messageSequenceStart + messageIndex,
        createdAt: committedAt,
        updatedAt: committedAt,
        disposition: "created",
        parts: draft.parts.map((part, partIndex) => ({
          runtimeLocalPartId: part.runtimeLocalPartId,
          partId: `part_${part.runtimeLocalPartId}`,
          messageId: `msg_${draft.runtimeLocalId}`,
          partSequence: partIndex,
          createdAt: committedAt,
          updatedAt: committedAt,
          disposition: "created",
        })),
      };
    }),
  };
  return {
    type: "receipt",
    inputDisposition,
    applicationDisposition: "current_custody",
    receipt,
  };
}
