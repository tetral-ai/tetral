import type { AcceptedInputCommitResult } from "../../src/context/context-loader.js";
import { acceptedInputCreates, acceptedInputDeclarationKind } from "../../src/runtime/runtime-declaration.js";
import type { RuntimeDeclarationReceipt } from "../../src/runtime/runtime-declaration.js";
import type { RuntimeAcceptedInputState } from "../../src/thread-loop/thread-state.js";

const committedAt = "2026-07-28T00:00:00.000Z";

/** Builds a complete database-shaped receipt for accepted-input unit tests. */
export function acceptedInputReceipt(
  input: RuntimeAcceptedInputState,
  inputDisposition: AcceptedInputCommitResult["inputDisposition"] = "committed",
  messageSequenceStart = 1,
): AcceptedInputCommitResult {
	const creates = acceptedInputCreates(input);
  const receipt: RuntimeDeclarationReceipt = {
    sessionThreadId: input.sessionThreadId,
    operationKind: "commit_inputs",
    sourceKind: acceptedInputDeclarationKind(input),
		operationId: input.runtimeInputId,
    declarationDigest: `digest_${input.runtimeInputId}`,
    pendingAttachmentDelta: [],
		interruptToolProjections: [],
    prefixConsumptions: [],

    childLifecycle: [],
    events: input.eventIds.map((eventId, index) => ({
      sessionThreadId: input.sessionThreadId,
      eventId,
      eventSequence: input.kind === "approval_review"
        ? messageSequenceStart + index
        : input.sequenceFrom + index,
      disposition: input.kind === "approval_review" ? "created" : "existing",
    })),
		messages: creates.map((create, messageIndex) => {
			if (create.sourceEventId === undefined) {
				throw new Error("accepted input test create is missing its source event");
			}
			const messageId = `msg_${input.runtimeInputId}_${messageIndex}`;
			return {
				sessionThreadId: input.sessionThreadId,
				owningEventId: create.sourceEventId,
				messageId,
        messageSequence: messageSequenceStart + messageIndex,
        createdAt: committedAt,
        updatedAt: committedAt,
        disposition: "created",
				parts: create.parts.map((_part, partIndex) => ({
					partId: `part_${input.runtimeInputId}_${messageIndex}_${partIndex}`,
					messageId,
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
