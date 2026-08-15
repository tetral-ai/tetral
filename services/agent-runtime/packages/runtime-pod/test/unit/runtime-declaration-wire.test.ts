import { describe, expect, test } from "bun:test";
import { RuntimeInputDisposition, RuntimeMessageCreateKind } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import {
  commitInputsDeclarationDigest,
  writeEventDeclarationDigest,
} from "../../src/runtime-declaration-wire.js";

const scope = {
	requestId: "request", workspaceId: "workspace", sessionId: "session", sessionThreadId: "thread",
	binding: { bindingId: "binding", bindingGeneration: 1, targetPodUid: "pod" },
};
const messageCreate = {
  messageKind: RuntimeMessageCreateKind.RUNTIME_MESSAGE_CREATE_KIND_USER_INPUT,
  messageInfoJson: '{"role":"user","origin":"user","status":"completed"}',
  parts: [{ partKind: "text", partJson: '{"type":"text","text":"hello","truncated":false,"status":"completed"}' }],
};

describe("Runtime declaration wire digests", () => {
  test("narrow approval content participates in CommitInputs identity", () => {
		const request: Parameters<typeof commitInputsDeclarationDigest>[0] = {
      scope,
      runtimeInputId: "input",
      disposition: RuntimeInputDisposition.RUNTIME_INPUT_DISPOSITION_COMMIT,
      approvalReviewText: ["review one"],
    };
    const first = commitInputsDeclarationDigest(request, "approval_review");
    expect(commitInputsDeclarationDigest({ ...request, approvalReviewText: ["review two"] }, "approval_review")).not.toBe(first);
  });

  test("Assistant append content participates in WriteEvent identity", () => {
		const request: Parameters<typeof writeEventDeclarationDigest>[0] = { scope, runtimeWriteId: "write", modelRequestId: "model_request", payloadJson: "{}", contextThroughMessageSequence: undefined, requestKind: "", eventType: "agent.message", assistantPartAppend: { parts: messageCreate.parts } };
		const first = writeEventDeclarationDigest(request);
    expect(writeEventDeclarationDigest({ ...request, payloadJson: '{"type":"agent.message","content":[{"type":"text","text":"other"}]}' })).not.toBe(first);
  });
});
