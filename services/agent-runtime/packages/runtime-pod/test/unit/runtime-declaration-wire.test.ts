import { describe, expect, test } from "bun:test";
import { RuntimeMessageCreateKind } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import {
  childLifecycleDeclarationDigest,
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
  test("message create position and content participate in CommitInputs identity", () => {
		const request: Parameters<typeof commitInputsDeclarationDigest>[0] = { scope, runtimeInputId: "input", eventIds: ["event"], sequenceFrom: 1, sequenceTo: 1, inputKind: "messages", messageCreates: [messageCreate] };
    const first = commitInputsDeclarationDigest(request);
    expect(commitInputsDeclarationDigest({ ...request, messageCreates: [{ ...messageCreate, parts: [{ ...messageCreate.parts[0]!, partJson: '{"type":"text","text":"other","truncated":false,"status":"completed"}' }] }] })).not.toBe(first);
  });

  test("Assistant append content participates in WriteEvent identity", () => {
		const request: Parameters<typeof writeEventDeclarationDigest>[0] = { scope, runtimeWriteId: "write", modelRequestId: "model_request", payloadJson: "{}", contextThroughMessageSequence: undefined, requestKind: "", eventType: "agent.message", assistantPartAppend: { parts: messageCreate.parts } };
    const first = writeEventDeclarationDigest(request);
    expect(writeEventDeclarationDigest({ ...request, payloadJson: '{"type":"agent.message","content":[{"type":"text","text":"other"}]}' })).not.toBe(first);
  });

  test("child lifecycle digest excludes caller timestamps", () => {
    const digest = childLifecycleDeclarationDigest({ operationKind: "mark_child_thread_closed", action: "close", sessionThreadId: "parent", childThreadId: "child", sourceKind: "tool_use", sourceCommandId: "tool_event" });
    expect(digest).toHaveLength(64);
  });
});
