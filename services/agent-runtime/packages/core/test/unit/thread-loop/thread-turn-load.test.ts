import { describe, expect, test } from "bun:test";
import type { RuntimeContextEntry } from "../../../src/contracts/runtime.js";
import type { ThreadTurnLoadFacts } from "../../../src/thread-loop/turn/load.js";
import { parseThreadTurnCheckpoint } from "../../../src/thread-loop/turn/checkpoint.js";
import {
	extractColdThreadToolRouteView,
	extractThreadTurnCheckpoint,
} from "../../../src/thread-loop/turn/load.js";
import { deriveThreadTurnSnapshot } from "../../../src/thread-loop/turn/reducer.js";
import {
	projectFailedRequestProviderContext,
	projectFailedRequestsProviderContext,
} from "../../../src/thread-loop/turn/provider-context.js";

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
						providerContextRetention: { disposition: "completed", toolUseEventIds: ["evt_tool"], repairEventIds: [] },
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

	test("durable extraction covers the six stable Thread-turn states", () => {
		const run = {
			eventId: "evt_run",
			eventSequence: 1,
			type: "session.status_running",
		} as const;
		const start = {
			eventId: "evt_start",
			eventSequence: 2,
			type: "span.model_request_start",
			modelRequestId: "req_1",
			requestStart: {
				requestKind: "agent_provider_request",
				contextThroughMessageSequence: 1,
			},
		} as const;
		const toolUse = {
			eventId: "evt_tool",
			eventSequence: 3,
			type: "agent.tool_use",
			modelRequestId: "req_1",
			toolUse: { modelToolCallId: "call_1", toolName: "Read" },
		} as const;
		const completedResult = {
			eventId: "evt_result",
			eventSequence: 4,
			type: "agent.tool_result",
			modelRequestId: "req_1",
			toolResult: {
				modelToolCallId: "call_1",
				toolName: "Read",
				outcome: "completed",
			},
		} as const;
		const requestEnd = (
			eventSequence: number,
			options: {
				readonly isError?: boolean;
				readonly reschedule?: {
					readonly attempt: number;
					readonly effectiveDeadline: string;
					readonly providerAttempts: number;
					readonly compactionAttempts: number;
				};
				readonly toolUseEventIds?: string[];
			} = {},
		): ThreadTurnLoadFacts["events"][number] =>
			({
				eventId: `evt_end_${eventSequence}`,
				eventSequence,
				type: "span.model_request_end",
				modelRequestId: "req_1",
				requestEnd: {
					requestStartEventId: "evt_start",
					isError: options.isError ?? false,
					providerContextRetention: {
						disposition: options.reschedule === undefined ? "completed" : "rescheduled",
						toolUseEventIds: options.toolUseEventIds ?? [],
						repairEventIds: [] as string[],
					},
					...(options.reschedule === undefined
						? {}
						: { reschedule: options.reschedule }),
				},
			});
		const cases: readonly {
			readonly name: string;
			readonly facts: ThreadTurnLoadFacts;
			readonly pendingSandboxExecutions?: readonly {
				readonly toolUseEventId: string;
				readonly modelRequestId: string;
				readonly modelToolCallId: string;
				readonly toolName: string;
			}[];
			readonly state: ReturnType<
				typeof deriveThreadTurnSnapshot
			>["state"]["state"];
			readonly nextStep: ReturnType<
				typeof deriveThreadTurnSnapshot
			>["nextStep"]["action"];
		}[] = [
			{
				name: "open request",
				facts: { events: [run, start], internalRepairs: [] },
				state: "request_open",
				nextStep: "await_request_end",
			},
			{
				name: "unresolved Tool",
				facts: {
					events: [
						run,
						start,
						toolUse,
						requestEnd(4, { toolUseEventIds: ["evt_tool"] }),
					],
					internalRepairs: [],
				},
				pendingSandboxExecutions: [
					{
						toolUseEventId: "evt_tool",
						modelRequestId: "req_1",
						modelToolCallId: "call_1",
						toolName: "Read",
					},
				],
				state: "waiting_for_tool_results",
				nextStep: "resume_tool_routes",
			},
			{
				name: "completed Tool pair",
				facts: {
					events: [
						run,
						start,
						toolUse,
						completedResult,
						requestEnd(5, { toolUseEventIds: ["evt_tool"] }),
					],
					internalRepairs: [],
				},
				state: "ready_to_request",
				nextStep: "prepare_next_request",
			},
			{
				name: "reschedule",
				facts: {
					events: [
						run,
						start,
						requestEnd(3, {
							isError: true,
							reschedule: {
								attempt: 1,
								effectiveDeadline: "2026-08-26T00:00:00.000Z",
								providerAttempts: 1,
								compactionAttempts: 0,
							},
						}),
					],
					internalRepairs: [],
				},
				state: "request_sealed",
				nextStep: "apply_request_retry_or_reschedule",
			},
			{
				name: "interrupt",
				facts: {
					events: [
						run,
						start,
						{ eventId: "evt_interrupt", eventSequence: 3, type: "user.interrupt" },
					],
					internalRepairs: [],
				},
				state: "request_open",
				nextStep: "close_interrupted",
			},
			{
				name: "idle",
				facts: {
					events: [
						run,
						start,
						requestEnd(3),
						{
							eventId: "evt_idle",
							eventSequence: 4,
							type: "session.status_idle",
							idle: { stopReason: "end_turn" },
						},
					],
					internalRepairs: [],
				},
				state: "idle",
				nextStep: "await_input",
			},
		];

		for (const testCase of cases) {
			const checkpoint = extractThreadTurnCheckpoint({
				contextEntries: [entry(1, "user", "go")],
				facts: testCase.facts,
			});
			const routes = extractColdThreadToolRouteView({
				checkpoint,
				pendingToolUses: [],
				pendingSandboxExecutions:
					testCase.pendingSandboxExecutions ?? [],
			});
			const snapshot = deriveThreadTurnSnapshot(
				checkpoint,
				routes,
				[],
				{ hasPendingAttachments: false },
			);
			expect(snapshot.state.state, testCase.name).toBe(testCase.state);
			expect(snapshot.nextStep.action, testCase.name).toBe(testCase.nextStep);
			expect(snapshot).not.toHaveProperty("dispatch");
		}
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
				requestEnd: {
					eventId: "evt_end",
					isError: false,
					providerContextRetention: { disposition: "completed", toolUseEventIds: ["evt_tool"], repairEventIds: [] },
				},
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

	test("failed Request projection drops partial text and retains exact Tool ownership", () => {
		const checkpoint = parseThreadTurnCheckpoint({
			executionRunId: "evt_run",
			pendingInputContextSequences: [],
			request: {
				modelRequestId: "req_failed",
				requestStartEventId: "evt_start",
				requestKind: "agent_provider_request",
				contextThroughMessageSequence: 1,
				requestEnd: {
					eventId: "evt_end",
					isError: true,
					providerContextRetention: {
						disposition: "rescheduled",
						assistantMessageSequence: 2,
						toolUseEventIds: ["evt_tool_done", "evt_tool_pending"],
						repairEventIds: [],
					},
					errorKind: "provider_stream_error",
					reschedule: {
						attempt: 1,
						effectiveDeadline: "2026-08-15T00:00:00.000Z",
						providerAttempts: 1,
						compactionAttempts: 0,
					},
				},
				toolMembers: [
					{
						memberKind: "public_tool_use",
						modelToolCallId: "call_done",
						toolUseEventId: "evt_tool_done",
						toolName: "Read",
						terminalResult: { outcome: "success" },
					},
					{
						memberKind: "public_tool_use",
						modelToolCallId: "call_pending",
						toolUseEventId: "evt_tool_pending",
						toolName: "Write",
					},
				],
			},
		});
		const projected = projectFailedRequestProviderContext({
			contextEntries: [
				entry(1, "user", "go"),
				{
					messageSequence: 2,
					contextKind: "assistant",
					parts: [
						{ type: "text", text: "failed partial" },
						{ type: "reasoning", text: "signed thought", providerMetadata: { anthropic: { signature: "sig" } } },
						{ type: "tool_call", modelToolCallId: "call_done", toolName: "Read", canonicalInput: {} },
						{ type: "tool_call", modelToolCallId: "call_pending", toolName: "Write", canonicalInput: {} },
						{ type: "tool_result", modelToolCallId: "call_done", result: { type: "completed", output: { text: "ok" } } },
						{ type: "reasoning", text: "failed trailing thought", providerMetadata: { anthropic: { signature: "trailing" } } },
						{ type: "text", text: "failed trailing text" },
					],
				},
			],
			checkpoint,
		});

		expect(projected.contextEntries).toEqual([entry(1, "user", "go")]);
		expect(projected.openRequestDraft).toMatchObject({
			modelRequestId: "req_failed",
			messageSequence: 2,
		});
		expect(projected.openRequestDraft?.parts.map((part) => part.type)).toEqual([
			"reasoning",
			"tool_call",
			"tool_call",
			"tool_result",
		]);
		expect(JSON.stringify(projected)).not.toContain("failed partial");
		expect(JSON.stringify(projected)).not.toContain("failed trailing thought");
		expect(JSON.stringify(projected)).not.toContain("failed trailing text");
	});

	test("cold eligibility removes older failed partials after a later request closes", () => {
		const facts: ThreadTurnLoadFacts = {
			events: [
				{ eventId: "run", eventSequence: 1, type: "session.status_running" },
				{
					eventId: "failed-start", eventSequence: 2,
					type: "span.model_request_start", modelRequestId: "failed",
					requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 1 },
				},
				{
					eventId: "failed-end", eventSequence: 3,
					type: "span.model_request_end", modelRequestId: "failed",
					requestEnd: {
						requestStartEventId: "failed-start",
						isError: true,
						providerContextRetention: { disposition: "failed", assistantMessageSequence: 2, toolUseEventIds: [], repairEventIds: [] },
					},
				},
				{
					eventId: "success-start", eventSequence: 4,
					type: "span.model_request_start", modelRequestId: "success",
					requestStart: { requestKind: "agent_provider_request", contextThroughMessageSequence: 2 },
				},
				{
					eventId: "success-end", eventSequence: 5,
					type: "span.model_request_end", modelRequestId: "success",
					requestEnd: {
						requestStartEventId: "success-start",
						isError: false,
						providerContextRetention: { disposition: "completed", assistantMessageSequence: 3, toolUseEventIds: [], repairEventIds: [] },
					},
				},
				{
					eventId: "idle", eventSequence: 6, type: "session.status_idle",
					idle: { stopReason: "end_turn" },
				},
			],
			internalRepairs: [],
		};
		const projected = projectFailedRequestsProviderContext({
			contextEntries: [
				entry(1, "user", "go"),
				entry(2, "assistant", "failed partial must remain audit-only"),
				entry(3, "assistant", "successful answer"),
			],
			openRequestDraft: {
				modelRequestId: "current-open",
				messageSequence: 4,
				parts: [{ type: "text", text: "current retry draft" }],
			},
			facts,
		});

			expect(projected.contextEntries).toEqual([
			entry(1, "user", "go"),
			entry(3, "assistant", "successful answer"),
		]);
		expect(projected.openRequestDraft?.modelRequestId).toBe("current-open");

		const alreadyExcluded = projectFailedRequestsProviderContext({
			contextEntries: [
				entry(1, "user", "go"),
				entry(3, "assistant", "successful answer"),
			],
			openRequestDraft: {
				modelRequestId: "current-open",
				messageSequence: 4,
				parts: [{ type: "text", text: "current retry draft" }],
			},
			facts,
		});
		expect(alreadyExcluded.contextEntries).toEqual([
			entry(1, "user", "go"),
			entry(3, "assistant", "successful answer"),
		]);
		expect(alreadyExcluded.openRequestDraft?.modelRequestId).toBe(
			"current-open",
		);
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
