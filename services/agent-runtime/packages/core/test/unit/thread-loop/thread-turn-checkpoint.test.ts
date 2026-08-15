import { describe, expect, test } from "bun:test";
import type { RuntimeContextEntry } from "../../../src/contracts/runtime.js";
import type { ThreadTurnLoadFacts } from "../../../src/thread-loop/thread-turn-checkpoint.js";
import {
	extractColdThreadToolRouteView,
	extractThreadTurnCheckpoint,
	parseThreadTurnCheckpoint,
} from "../../../src/thread-loop/thread-turn-checkpoint.js";

function entry(
	messageSequence: number,
	contextKind: RuntimeContextEntry["contextKind"],
	text: string,
): RuntimeContextEntry {
	return { messageSequence, contextKind, parts: [{ type: "text", text }] };
}

const noFacts: ThreadTurnLoadFacts = { events: [], internalRepairs: [] };

describe("cold Thread-turn reconstruction", () => {
	test("checkpoint retains pending input by context sequence, never DB Message identity", () => {
		const checkpoint = extractThreadTurnCheckpoint({
			contextEntries: [
				entry(1, "user", "first"),
				entry(2, "assistant", "reply"),
				entry(3, "runtime_notification", "task completed"),
				entry(4, "compaction", "summary"),
			],
			facts: noFacts,
		});

		expect(checkpoint).toEqual({ pendingInputContextSequences: [1, 3] });
		expect(JSON.stringify(checkpoint)).not.toContain("context:");
		expect(JSON.stringify(checkpoint)).not.toContain("messageId");
	});

	test("Request Start boundary removes already-consumed input context", () => {
		const facts: ThreadTurnLoadFacts = {
			events: [
				{
					eventId: "evt_run",
					eventSequence: 1,
					type: "session.status_running",
				},
				{
					eventId: "evt_start",
					eventSequence: 2,
					type: "span.model_request_start",
					modelRequestId: "req_1",
					requestStart: {
						requestKind: "agent_provider_request",
						contextThroughMessageSequence: 1,
					},
				},
			],
			internalRepairs: [],
		};
		expect(
			extractThreadTurnCheckpoint({
				contextEntries: [
					entry(1, "user", "served"),
					entry(2, "user", "queued"),
				],
				facts,
			}),
		).toEqual({
			executionRunId: "evt_run",
			pendingInputContextSequences: [2],
			request: {
				modelRequestId: "req_1",
				requestStartEventId: "evt_start",
				requestKind: "agent_provider_request",
				contextThroughMessageSequence: 1,
				toolMembers: [],
			},
		});
	});

	test("ordinary Tool facts reconstruct exact model call/result pairing", () => {
		const facts: ThreadTurnLoadFacts = {
			events: [
				{
					eventId: "evt_run",
					eventSequence: 1,
					type: "session.status_running",
				},
				{
					eventId: "evt_start",
					eventSequence: 2,
					type: "span.model_request_start",
					modelRequestId: "req_1",
					requestStart: {
						requestKind: "agent_provider_request",
						contextThroughMessageSequence: 1,
					},
				},
				{
					eventId: "evt_tool",
					eventSequence: 3,
					type: "agent.tool_use",
					modelRequestId: "req_1",
					toolUse: { modelToolCallId: "call_1", toolName: "read_file" },
				},
				{
					eventId: "evt_result",
					eventSequence: 4,
					type: "agent.tool_result",
					modelRequestId: "req_1",
					toolResult: {
						modelToolCallId: "call_1",
						toolName: "read_file",
						outcome: "completed",
					},
				},
				{
					eventId: "evt_end",
					eventSequence: 5,
					type: "span.model_request_end",
					modelRequestId: "req_1",
					requestEnd: {
						requestStartEventId: "evt_start",
						isError: false,
						rescheduled: false,
					},
				},
			],
			internalRepairs: [],
		};
		const checkpoint = extractThreadTurnCheckpoint({
			contextEntries: [entry(1, "user", "go")],
			facts,
		});
		expect(checkpoint.request?.toolMembers).toEqual([
			{
				memberKind: "public_tool_use",
				modelToolCallId: "call_1",
				toolUseEventId: "evt_tool",
				toolName: "read_file",
				terminalResult: { outcome: "success" },
			},
		]);
	});

	test("cold pending Tool route is derived from the dedicated active fact", () => {
		const checkpoint = parseThreadTurnCheckpoint({
			executionRunId: "evt_run",
			pendingInputContextSequences: [],
			request: {
				modelRequestId: "req_1",
				requestStartEventId: "evt_start",
				requestKind: "agent_provider_request",
				contextThroughMessageSequence: 1,
				requestEnd: { eventId: "evt_end", isError: false, rescheduled: false },
				toolMembers: [
					{
						memberKind: "public_tool_use",
						modelToolCallId: "call_1",
						toolUseEventId: "evt_tool",
						toolName: "read_file",
					},
				],
			},
		});
		expect(
			extractColdThreadToolRouteView({
				checkpoint,
				pendingToolUses: [
					{
						toolUseEventId: "evt_tool",
						modelRequestId: "req_1",
						modelToolCallId: "call_1",
						toolName: "read_file",
						status: "pending",
					},
				],
				pendingSandboxExecutions: [],
			}),
		).toEqual({
			routes: [
				{ toolUseEventId: "evt_tool", disposition: "requires_user_action" },
			],
		});
	});

	test("malformed direct facts fail closed", () => {
		expect(() =>
			extractThreadTurnCheckpoint({
				contextEntries: [entry(1, "user", "go")],
				facts: {
					events: [
						{
							eventId: "evt_2",
							eventSequence: 2,
							type: "session.status_running",
						},
						{ eventId: "evt_1", eventSequence: 1, type: "user.interrupt" },
					],
					internalRepairs: [],
				},
			}),
		).toThrow();
	});
});
