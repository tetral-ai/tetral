import { describe, expect, test } from "bun:test";
import type {
	RuntimeContextEntry,
	RuntimeContextKind,
} from "../../src/contracts/runtime.js";
import {
	partitionRuntimeConversationTurns,
	selectRecentUserLedTurns,
} from "../../src/runtime/conversation-turns.js";

function textEntry(
	messageSequence: number,
	contextKind: RuntimeContextKind,
	text: string,
): RuntimeContextEntry {
	return { messageSequence, contextKind, parts: [{ type: "text", text }] };
}

function toolEntry(
	messageSequence: number,
	modelToolCallId: string,
	toolName: string,
): RuntimeContextEntry {
	return {
		messageSequence,
		contextKind: "assistant",
		parts: [
			{ type: "tool_call", modelToolCallId, toolName, canonicalInput: {} },
		],
	};
}

describe("conversation turn boundaries", () => {
	test("fork selection keeps the complete latest user-led turn", () => {
		const entries = [
			textEntry(1, "user", "first"),
			toolEntry(2, "call-1", "tool-1"),
			textEntry(3, "runtime_notification", "inter-agent input"),
			textEntry(4, "assistant", "answer"),
		];

		expect(
			selectRecentUserLedTurns(entries, 1).map(
				(entry) => entry.messageSequence,
			),
		).toEqual([3, 4]);
		expect(
			selectRecentUserLedTurns(entries, 2).map(
				(entry) => entry.messageSequence,
			),
		).toEqual([1, 2, 3, 4]);
	});

	test("compaction checkpoints stay inside the preceding user-led turn", () => {
		const entries = [
			textEntry(1, "user", "first"),
			textEntry(2, "assistant", "answer"),
			textEntry(
				3,
				"compaction",
				"<conversation-checkpoint>summary</conversation-checkpoint>",
			),
			textEntry(4, "assistant", "continued"),
		];

		const turns = partitionRuntimeConversationTurns(entries);
		expect(turns).toHaveLength(1);
		expect(turns[0]?.userLed).toBe(true);
		expect(turns[0]?.compactable).toBe(false);
		expect(
			selectRecentUserLedTurns(entries, 1).map(
				(entry) => entry.messageSequence,
			),
		).toEqual([1, 2, 3, 4]);
	});
});
