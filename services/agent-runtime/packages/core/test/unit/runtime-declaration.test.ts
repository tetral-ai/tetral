import { describe, expect, test } from "bun:test";
import type {
  RuntimeAssistantPartAppend,
  RuntimeDeclarationReceipt,
} from "../../src/contracts/runtime.js";
import { RuntimeMessageSchema } from "../../src/contracts/runtime.js";
import {
  acceptedInputCreates,
  applyAcceptedInputReceipt,
  applyAssistantPartAppendReceipt,
  applyInterruptInputReceipt,
  applyInterruptToolProjections,
  applyToolSettlementReceipt,
} from "../../src/runtime/runtime-declaration.js";
import type { RuntimeAcceptedInputState } from "../../src/thread-loop/thread-state.js";

const timestamp = "2026-01-01T00:00:00.000Z";

function baseReceipt(overrides: Partial<RuntimeDeclarationReceipt>): RuntimeDeclarationReceipt {
  return {
    sessionThreadId: "thread",
    operationKind: "write_event",
    sourceKind: "agent.message",
    operationId: "write",
    declarationDigest: "digest",
    events: [],
    messages: [],
    pendingAttachmentDelta: [],
    interruptToolProjections: [],
    prefixConsumptions: [],
    childLifecycle: [],
    ...overrides,
  };
}

describe("incremental Runtime declarations", () => {
  test("accepted input creates receive identity only from positional stamps", () => {
    const input: RuntimeAcceptedInputState = {
      requestId: "request",
      workspaceId: "workspace",
      sessionId: "session",
      sessionThreadId: "thread",
      bindingId: "binding",
      bindingGeneration: 1,
      targetPodUid: "pod",
      runtimeInputId: "input",
      eventIds: ["event"],
      sequenceFrom: 4,
      sequenceTo: 4,
      kind: "messages",
      payloadJson: JSON.stringify({ messages: [{
        id: "provider_message",
        sessionId: "session",
        role: "user",
        origin: "user",
        sequence: 1,
        status: "completed",
        createdAt: timestamp,
        parts: [{ id: "provider_part", sessionId: "session", messageId: "provider_message", sequence: 0, type: "text", text: "hello", truncated: false, status: "completed", createdAt: timestamp }],
      }] }),
    };
    const creates = acceptedInputCreates(input);
    expect(JSON.stringify(creates)).not.toContain("provider_message");
    const messages = applyAcceptedInputReceipt(input, creates, baseReceipt({
      operationKind: "commit_inputs",
      sourceKind: "messages",
      operationId: "input",
      events: [{ sessionThreadId: "thread", eventId: "event", eventSequence: 4, disposition: "existing" }],
      messages: [{
        sessionThreadId: "thread", messageId: "message", messageSequence: 1,
        createdAt: timestamp, updatedAt: timestamp, disposition: "created",
        parts: [{ partId: "part", messageId: "message", partSequence: 0, createdAt: timestamp, updatedAt: timestamp, disposition: "created" }],
      }],
    }));
    expect(messages[0]?.id).toBe("message");
    expect(messages[0]?.parts[0]?.id).toBe("part");
  });

  test("Assistant append rejects a non-contiguous Bridge part stamp", () => {
    const append: RuntimeAssistantPartAppend = { parts: [{ type: "text", text: "hello", truncated: false, status: "completed" }] };
    expect(() => applyAssistantPartAppendReceipt({
      sessionId: "session", sessionThreadId: "thread", operationKind: "write_event",
      sourceKind: "agent.message", operationId: "write", eventId: "event", append,
    }, baseReceipt({
      events: [{ sessionThreadId: "thread", eventId: "event", eventSequence: 1, disposition: "created" }],
      messages: [{
        sessionThreadId: "thread", messageId: "message", messageSequence: 1,
        createdAt: timestamp, updatedAt: timestamp, disposition: "created",
        parts: [{ partId: "part", messageId: "message", partSequence: 1, createdAt: timestamp, updatedAt: timestamp, disposition: "created" }],
      }],
    }))).toThrow();
  });

  test("Tool settlement ACK cannot smuggle a message projection", () => {
    const settlement = { toolUseEventId: "tool_event", outcome: { type: "completed" as const, output: { text: "done", truncated: false } } };
    const receipt = baseReceipt({
      sourceKind: "agent.tool_result",
      events: [{ sessionThreadId: "thread", eventId: "result_event", eventSequence: 2, disposition: "created" }],
      messages: [{ sessionThreadId: "thread", messageId: "message", messageSequence: 1, createdAt: timestamp, updatedAt: timestamp, disposition: "updated", parts: [] }],
    });
    expect(() => applyToolSettlementReceipt({
      sessionThreadId: "thread", operationKind: "write_event", sourceKind: "agent.tool_result",
      operationId: "write", eventId: "result_event", settlement,
    }, receipt)).toThrow();
  });

  test("interrupt projections reject the closed malformed census before hot mutation", () => {
    const input = {
      sessionThreadId: "thread",
      runtimeInputId: "interrupt",
      eventIds: ["interrupt_event"],
      expectedToolUseEventIds: ["tool_a", "tool_b"],
    };
    const projection = (toolUseEventId: string, eventSequence: number, sessionThreadId = "thread") => ({
      toolUseEventId,
      resultEvent: {
        sessionThreadId,
        eventId: `result_${toolUseEventId}`,
        eventSequence,
        disposition: "created" as const,
      },
      terminalState: { type: "cancelled" as const },
    });
    const valid = baseReceipt({
      operationKind: "commit_inputs",
      sourceKind: "interrupt_control",
      operationId: "interrupt",
      events: [{ sessionThreadId: "thread", eventId: "interrupt_event", eventSequence: 4, disposition: "existing" }],
      interruptToolProjections: [projection("tool_a", 5), projection("tool_b", 6)],
    });
    expect(applyInterruptInputReceipt(input, valid)).toHaveLength(2);

    const malformed = [
      { name: "missing", receipt: { ...valid, interruptToolProjections: valid.interruptToolProjections.slice(0, 1) } },
      { name: "duplicate", receipt: { ...valid, interruptToolProjections: [projection("tool_a", 5), projection("tool_a", 6)] } },
      { name: "cross-thread", receipt: { ...valid, interruptToolProjections: [projection("tool_a", 5, "other_thread"), projection("tool_b", 6)] } },
      { name: "out-of-order", receipt: { ...valid, interruptToolProjections: [projection("tool_a", 6), projection("tool_b", 5)] } },
      { name: "wrong-disposition", receipt: { ...valid, interruptToolProjections: [
        { ...projection("tool_a", 5), resultEvent: { ...projection("tool_a", 5).resultEvent, disposition: "existing" as const } },
        projection("tool_b", 6),
      ] } },
    ];
    for (const candidate of malformed) {
      expect(() => applyInterruptInputReceipt(input, candidate.receipt), candidate.name).toThrow();
    }

    const messages = [RuntimeMessageSchema.parse({
      id: "message",
      sessionId: "session",
      sequence: 1,
      role: "assistant",
      origin: "agent",
      status: "completed",
      createdAt: timestamp,
      parts: [
        { id: "part_a", sessionId: "session", messageId: "message", sequence: 0, type: "tool", toolCallId: "call_a", toolName: "Read", toolUseEventId: "tool_a", state: { status: "pending" }, createdAt: timestamp },
        { id: "part_b", sessionId: "session", messageId: "message", sequence: 1, type: "tool", toolCallId: "call_b", toolName: "Read", toolUseEventId: "tool_b", state: { status: "running", input: { value: {}, preview: "{}", truncated: false } }, createdAt: timestamp },
      ],
    })];
    expect(() => applyInterruptToolProjections(messages, [projection("tool_b", 5), projection("tool_a", 6)]))
      .toThrow("interrupt projection census does not match hot unfinished Tools");
    expect(() => applyInterruptToolProjections(messages, [{
      ...projection("tool_a", 5),
      terminalState: { type: "error", error: { unrecognized: true } } as never,
    }, projection("tool_b", 6)])).toThrow();
    const contradictory = [{
      ...messages[0]!,
      parts: messages[0]!.parts.map((part) => part.type === "tool" && part.toolUseEventId === "tool_a"
        ? { ...part, state: { status: "completed" as const, input: { value: {}, preview: "{}", truncated: false }, output: { text: "already done", truncated: false } } }
        : part),
    }];
    expect(() => applyInterruptToolProjections(contradictory, [projection("tool_a", 5), projection("tool_b", 6)]))
      .toThrow("interrupt projection census does not match hot unfinished Tools");
    expect(messages[0]?.parts.map((part) => part.type === "tool" ? part.state.status : part.type))
      .toEqual(["pending", "running"]);
  });
});
