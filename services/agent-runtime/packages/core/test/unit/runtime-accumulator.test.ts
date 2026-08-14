import { describe, expect, test } from "bun:test";
import type { RuntimeProcessorSource, SessionEventWriterAppendEvent } from "../../src/contracts/runtime.js";
import { RequestAssistantMemberSequencer } from "../../src/runtime/accumulator.js";
import type { FrozenAssistantPartAppend } from "../../src/runtime/accumulator.js";

const source = { providerId: "openai" as RuntimeProcessorSource["providerId"], modelId: "model" };

function deferredEvent(): {
  readonly promise: Promise<SessionEventWriterAppendEvent>;
  readonly resolve: (event: SessionEventWriterAppendEvent) => void;
} {
  let resolve!: (event: SessionEventWriterAppendEvent) => void;
  const promise = new Promise<SessionEventWriterAppendEvent>((settle) => { resolve = settle; });
  return { promise, resolve };
}

function member(toolCallId: string, event: Promise<SessionEventWriterAppendEvent>): FrozenAssistantPartAppend {
  return {
    source,
    toolCallId,
    event,
    append: {
      parts: [{
        type: "tool",
        toolCallId,
        toolName: "exec_command",
        state: { status: "running", input: { value: {}, preview: "{}", truncated: false } },
      }],
    },
  };
}

describe("request Assistant member sequencing", () => {
  test("a later authorized Tool cannot overtake an earlier permission wait", async () => {
    const first = deferredEvent();
    const second = deferredEvent();
    const committed: string[] = [];
    const sequencer = new RequestAssistantMemberSequencer(async (frozen) => {
      await frozen.event;
      committed.push(frozen.toolCallId ?? "text");
      return { ok: true, events: [] };
    });

    const firstCommit = sequencer.enqueue(member("call_first", first.promise)).committed;
    const secondCommit = sequencer.enqueue(member("call_second", second.promise)).committed;
    second.resolve({ type: "agent.tool_use", name: "exec_command", input: {}, evaluated_permission: "allow" });
    await Promise.resolve();
    expect(committed).toEqual([]);

    first.resolve({ type: "agent.tool_use", name: "exec_command", input: {}, evaluated_permission: "ask" });
    await Promise.all([firstCommit, secondCommit, sequencer.awaitDrained()]);
    expect(committed).toEqual(["call_first", "call_second"]);
  });
});
