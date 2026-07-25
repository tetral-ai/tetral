import { describe, expect, test } from "bun:test";
import { partitionRuntimeConversationTurns, selectRecentUserLedTurns } from "../../src/runtime/conversation-turns.js";
import {
  buildConversationTurnsTextMessage as textMessage,
  buildConversationTurnsToolMessage as toolMessage,
} from "./runtime-message-builders.js";

describe("conversation turn boundaries", () => {
  test("fork selection keeps the complete latest user-led turn", () => {
    const messages = [
      textMessage("user-1", "user", "user", 0, "first"),
      toolMessage("assistant-1", 1, "call-1", "tool-1"),
      textMessage("user-2", "user", "runtime", 2, "inter-agent input"),
      textMessage("assistant-2", "assistant", "agent", 3, "answer"),
    ];

    expect(selectRecentUserLedTurns(messages, 1).map((message) => message.id)).toEqual([
      "user-2",
      "assistant-2",
    ]);
    expect(selectRecentUserLedTurns(messages, 2).map((message) => message.id)).toEqual([
      "user-1",
      "assistant-1",
      "user-2",
      "assistant-2",
    ]);
  });

  test("compaction checkpoints stay inside the preceding user-led turn", () => {
    const messages = [
      textMessage("user-1", "user", "user", 0, "first"),
      textMessage("assistant-1", "assistant", "agent", 1, "answer"),
      textMessage("checkpoint", "user", "runtime", 2, "<conversation-checkpoint>summary</conversation-checkpoint>"),
      textMessage("assistant-after-checkpoint", "assistant", "agent", 3, "continued"),
    ];

    const turns = partitionRuntimeConversationTurns(messages);
    expect(turns).toHaveLength(1);
    expect(turns[0]?.userLed).toBe(true);
    expect(turns[0]?.compactable).toBe(false);
    expect(selectRecentUserLedTurns(messages, 1).map((message) => message.id)).toEqual([
      "user-1",
      "assistant-1",
      "checkpoint",
      "assistant-after-checkpoint",
    ]);
  });
});
