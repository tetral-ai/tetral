import type { AcceptedInputCommitResult } from "../../src/context/context-loader.js";
import { acceptedInputCreates, acceptedInputDeclarationKind } from "../../src/runtime/runtime-declaration.js";
import { taskNotificationOperationId } from "../../src/runtime/runtime-declaration.js";
import type { RuntimeDeclarationReceipt } from "../../src/runtime/runtime-declaration.js";
import type { RuntimeAcceptedInputState } from "../../src/thread-loop/thread-state.js";

const committedAt = "2026-07-28T00:00:00.000Z";
type AcceptedInputReceiptResult = Extract<AcceptedInputCommitResult, { readonly type: "receipt" }>;

/** Builds a complete database-shaped receipt for accepted-input unit tests. */
export function acceptedInputReceipt(
  input: RuntimeAcceptedInputState,
  inputDisposition: AcceptedInputReceiptResult["inputDisposition"] = "committed",
  messageSequenceStart = 1,
): AcceptedInputReceiptResult {
	const creates = acceptedInputCreates(input);
  const receipt: RuntimeDeclarationReceipt = {
    sessionThreadId: input.sessionThreadId,
    operationKind: input.kind === "task_notification" ? "commit_task_notification_result" : "commit_inputs",
    sourceKind: acceptedInputDeclarationKind(input),
    operationId: input.kind === "task_notification"
      ? taskNotificationOperationId(input.runtimeInputId, input.taskId)
      : input.runtimeInputId,
    declarationDigest: `digest_${input.runtimeInputId}`,
    pendingAttachmentDelta: [],
		interruptToolProjections: [],
    prefixConsumptions: [],

    childLifecycle: [],
    events: creates.map((_create, index) => ({
      sessionThreadId: input.sessionThreadId,
      eventId: `sevt_commit_${input.runtimeInputId}_${index}`,
      eventSequence: ("inputOrder" in input ? input.inputOrder : messageSequenceStart) + index,
      disposition: input.kind === "approval_review" || input.kind === "task_notification"
        ? "created"
        : "existing",
    })),
		messages: creates.map((create, messageIndex) => {
			const messageId = `msg_${input.runtimeInputId}_${messageIndex}`;
			return {
				sessionThreadId: input.sessionThreadId,
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
