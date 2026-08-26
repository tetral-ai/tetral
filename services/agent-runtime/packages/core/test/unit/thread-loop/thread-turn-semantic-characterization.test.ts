import { describe, expect, test } from "bun:test";
import type {
	ThreadToolRouteView,
	ThreadTurnCheckpoint,
} from "../../../src/thread-loop/turn/checkpoint.js";
import {
	extractColdThreadToolRouteView,
	extractThreadTurnCheckpoint,
	ThreadTurnLoadFactsSchema,
} from "../../../src/thread-loop/turn/load.js";
import {
	deriveThreadTurnSnapshot as deriveThreadTurnSnapshotWithActiveInput,
	initializeThreadTurnTransition as initializeThreadTurnTransitionWithActiveInput,
	reduceThreadTurn as reduceThreadTurnWithActiveInput,
} from "../../../src/thread-loop/turn/reducer.js";
import { toGatewayProviderContext } from "../../../src/runtime/context-projection.js";

const noRoutes: ThreadToolRouteView = { routes: [] };
type ActiveInputView = Parameters<
	typeof deriveThreadTurnSnapshotWithActiveInput
>[3];
const noPendingAttachments: ActiveInputView = { hasPendingAttachments: false };

const deriveThreadTurnSnapshot = (
	activeInputView: ActiveInputView,
	checkpoint: Parameters<typeof deriveThreadTurnSnapshotWithActiveInput>[0],
	routes: Parameters<typeof deriveThreadTurnSnapshotWithActiveInput>[1],
	acceptedInputIds: readonly string[] = [],
) =>
	deriveThreadTurnSnapshotWithActiveInput(
		checkpoint,
		routes,
		acceptedInputIds,
		activeInputView,
	);
const initializeThreadTurnTransition = (
	activeInputView: ActiveInputView,
	checkpoint: Parameters<
		typeof initializeThreadTurnTransitionWithActiveInput
	>[0],
	routes: Parameters<typeof initializeThreadTurnTransitionWithActiveInput>[1],
	acceptedInputIds: readonly string[] = [],
) =>
	initializeThreadTurnTransitionWithActiveInput(
		checkpoint,
		routes,
		acceptedInputIds,
		activeInputView,
	);
const reduceThreadTurn = (
	activeInputView: ActiveInputView,
	current: Parameters<typeof reduceThreadTurnWithActiveInput>[0],
	fact: Parameters<typeof reduceThreadTurnWithActiveInput>[1],
	routes: Parameters<typeof reduceThreadTurnWithActiveInput>[2],
	acceptedInputIds: readonly string[] = [],
) =>
	reduceThreadTurnWithActiveInput(
		current,
		fact,
		routes,
		acceptedInputIds,
		activeInputView,
	);

describe("Thread-turn semantic characterization", () => {
	test("production durable facts reconstruct legal turn families with exact retention", () => {
		const run = turnEvent("event_run", 1, "session.status_running");
		const start = requestStartEvent(2);
		const toolA = toolUseEvent("event_tool_a", 3, "call_a", "Read");
		const toolB = toolUseEvent("event_tool_b", 4, "call_b", "Grep");
		const resultA = toolResultEvent(
			"event_result_a",
			5,
			"call_a",
			"Read",
		);
		const resultB = toolResultEvent(
			"event_result_b",
			6,
			"call_b",
			"Grep",
		);
		const repair = {
			eventId: "event_repair",
			eventSequence: 6,
			type: "agent.tool_result" as const,
			modelRequestId: "request",
			toolResult: { repairKey: "repair_key" },
		};
		const repairReference = {
			repairKey: "repair_key",
			repairEventId: "event_repair",
			eventSequence: 6,
			modelRequestId: "request",
			modelToolCallId: "call_repair",
			toolName: "UnavailableTool",
		};
		const cases = [
			{
				name: "ordinary",
				events: [run, start, requestEndEvent(3, "completed", [], [])],
				internalRepairs: [],
				expectedAction: "finish_idle",
				expectedMembers: [],
			},
			{
				name: "multi Tool",
				events: [
					run,
					start,
					toolA,
					toolB,
					resultA,
					resultB,
					requestEndEvent(
						7,
						"completed",
						["event_tool_a", "event_tool_b"],
						[],
					),
				],
				internalRepairs: [],
				expectedAction: "prepare_next_request",
				expectedMembers: ["event_tool_a", "event_tool_b"],
			},
			{
				name: "approval",
				events: [
					run,
					start,
					toolA,
					requestEndEvent(4, "completed", ["event_tool_a"], []),
					{
						eventId: "event_approval_idle",
						eventSequence: 5,
						type: "session.status_idle" as const,
						idle: { stopReason: "requires_action" },
					},
				],
				internalRepairs: [],
				pendingToolUses: [
					{
						toolUseEventId: "event_tool_a",
						modelRequestId: "request",
						modelToolCallId: "call_a",
						toolName: "Read",
						status: "pending" as const,
					},
				],
				expectedAction: "await_tool_results",
				expectedMembers: ["event_tool_a"],
			},
			{
				name: "reschedule with Tool and repair",
				events: [
					run,
					start,
					toolA,
					resultA,
					repair,
					requestEndEvent(
						7,
						"rescheduled",
						["event_tool_a"],
						["event_repair"],
						true,
					),
				],
				internalRepairs: [repairReference],
				expectedAction: "apply_request_retry_or_reschedule",
				expectedMembers: ["event_tool_a", "event_repair"],
			},
			{
				name: "interrupt",
				events: [
					run,
					start,
					toolA,
					turnEvent("event_interrupt", 4, "user.interrupt"),
				],
				internalRepairs: [],
				expectedAction: "close_interrupted",
				expectedMembers: ["event_tool_a"],
			},
			{
				name: "terminal",
				events: [
					run,
					{
						eventId: "event_failure",
						eventSequence: 2,
						type: "session.error" as const,
						failure: { errorType: "runtime", retryStatus: "terminal" as const },
					},
					turnEvent("event_terminal", 3, "session.status_terminated"),
				],
				internalRepairs: [],
				expectedAction: "await_input",
				expectedMembers: [],
			},
		] as const;

		for (const scenario of cases) {
			const facts = ThreadTurnLoadFactsSchema.parse({
				events: scenario.events,
				internalRepairs: scenario.internalRepairs,
			});
			const checkpoint = extractThreadTurnCheckpoint({
				contextEntries: [
					{
						messageSequence: 1,
						contextKind: "user",
						parts: [{ type: "text", text: "request" }],
					},
				],
				facts,
			});
			const routes = extractColdThreadToolRouteView({
				checkpoint,
				pendingToolUses:
					"pendingToolUses" in scenario ? scenario.pendingToolUses : [],
				pendingSandboxExecutions: [],
			});
			const snapshot = deriveThreadTurnSnapshot(
				noPendingAttachments,
				checkpoint,
				routes,
			);
			expect(snapshot.nextStep.action, scenario.name).toBe(
				scenario.expectedAction,
			);
			expect(
				checkpoint.request?.toolMembers.map((member) =>
					member.memberKind === "public_tool_use"
						? member.toolUseEventId
						: member.repairEventId,
				) ?? [],
				scenario.name,
			).toEqual([...scenario.expectedMembers]);
		}
	});

	test("a pending durable Tool Call is valid context but cannot start the next provider request", () => {
		expect(toGatewayProviderContext([{
			messageSequence: 1,
			contextKind: "assistant",
			parts: [{
				type: "tool_call",
				modelToolCallId: "call_pending",
				toolName: "Read",
				canonicalInput: { path: "README.md" },
			}],
		}])).toMatchObject({ ok: true });

		const pending = initializeThreadTurnTransition(noPendingAttachments,
			sealedRequest([pendingTool("tool_pending", "call_pending", "Read")]),
			{ routes: [{ toolUseEventId: "tool_pending", disposition: "hot_execution" }] },
		);
		expect(pending).toMatchObject({
			state: { state: "waiting_for_tool_results" },
			nextStep: { action: "await_tool_results", toolUseEventIds: ["tool_pending"] },
		});

		const terminal = reduceThreadTurn(noPendingAttachments,
			pending,
			{ fact: "tool_result_committed", toolUseEventId: "tool_pending", outcome: "success" },
			noRoutes,
		);
		expect(terminal).toMatchObject({
			state: { state: "ready_to_request" },
			nextStep: { action: "prepare_next_request" },
		});
	});

	test("durable facts select the established state and action truth table", () => {
		const awaitingApproval = sealedRequest([
			pendingTool("tool_approval", "call_approval", "Write"),
		]);
		const reviewerDecision = sealedRequest([], "approval_reviewer");
		const reviewerFailure = sealedRequest([], "approval_reviewer", {
			isError: true,
			providerContextRetention: { disposition: "failed", toolUseEventIds: [], repairEventIds: [] },
			errorKind: "provider_stream_error",
		});
		const reviewerCancelled = {
			...reviewerDecision,
			interruptEventId: "event_reviewer_interrupt",
		};
		const cases: readonly {
			readonly name: string;
			readonly checkpoint: ThreadTurnCheckpoint;
			readonly routes?: ThreadToolRouteView;
			readonly acceptedInputIds?: readonly string[];
			readonly expected: ReturnType<typeof deriveThreadTurnSnapshot>;
		}[] = [
			{
				name: "empty thread",
				checkpoint: { pendingInputContextSequences: [] },
				expected: {
					state: { state: "idle" },
					nextStep: { action: "await_input" },
				},
			},
			{
				name: "committed provider input",
				checkpoint: { pendingInputContextSequences: [1] },
				expected: {
					state: { state: "ready_to_request" },
					nextStep: { action: "prepare_next_request" },
				},
			},
			{
				name: "accepted child completion or mail wake",
				checkpoint: { pendingInputContextSequences: [] },
				acceptedInputIds: ["runtime_input_wake"],
				expected: {
					state: { state: "idle" },
					nextStep: {
						action: "commit_accepted_input",
						runtimeInputId: "runtime_input_wake",
					},
				},
			},
			{
				name: "open request",
				checkpoint: openRequest(),
				expected: {
					state: { state: "request_open", modelRequestId: "request" },
					nextStep: { action: "await_request_end", modelRequestId: "request" },
				},
			},
			{
				name: "sealed request without a Tool",
				checkpoint: sealedRequest([]),
				expected: {
					state: { state: "ready_to_finish" },
					nextStep: { action: "finish_idle", stopReason: { type: "end_turn" } },
				},
			},
			{
				name: "unresolved Tool",
				checkpoint: sealedRequest([
					pendingTool("tool_pending", "call_pending", "Read"),
				]),
				routes: {
					routes: [
						{ toolUseEventId: "tool_pending", disposition: "hot_execution" },
					],
				},
				expected: {
					state: {
						state: "waiting_for_tool_results",
						modelRequestId: "request",
					},
					nextStep: {
						action: "await_tool_results",
						modelRequestId: "request",
						toolUseEventIds: ["tool_pending"],
					},
				},
			},
			{
				name: "approval wait",
				checkpoint: awaitingApproval,
				routes: {
					routes: [
						{
							toolUseEventId: "tool_approval",
							disposition: "requires_user_action",
						},
					],
				},
				expected: {
					state: {
						state: "waiting_for_tool_results",
						modelRequestId: "request",
					},
					nextStep: {
						action: "finish_idle",
						stopReason: {
							type: "requires_action",
							eventIds: ["tool_approval"],
						},
					},
				},
			},
			{
				name: "approval resume",
				checkpoint: awaitingApproval,
				routes: {
					routes: [
						{
							toolUseEventId: "tool_approval",
							disposition: "resume_approval_settlement",
						},
					],
				},
				expected: {
					state: {
						state: "waiting_for_tool_results",
						modelRequestId: "request",
					},
					nextStep: {
						action: "resume_tool_routes",
						modelRequestId: "request",
						toolUseEventIds: ["tool_approval"],
					},
				},
			},
			{
				name: "reviewer decision",
				checkpoint: reviewerDecision,
				expected: {
					state: { state: "request_sealed", modelRequestId: "request" },
					nextStep: { action: "complete_reviewer", modelRequestId: "request" },
				},
			},
			{
				name: "reviewer failure",
				checkpoint: reviewerFailure,
				expected: {
					state: { state: "request_sealed", modelRequestId: "request" },
					nextStep: {
						action: "apply_request_retry_or_reschedule",
						modelRequestId: "request",
					},
				},
			},
			{
				name: "reviewer cancellation",
				checkpoint: reviewerCancelled,
				expected: {
					state: { state: "request_sealed", modelRequestId: "request" },
					nextStep: { action: "close_interrupted", modelRequestId: "request" },
				},
			},
			{
				name: "compaction continuation",
				checkpoint: sealedRequest([], "compaction_summary"),
				expected: {
					state: { state: "ready_to_request" },
					nextStep: {
						action: "continue_after_compaction",
						modelRequestId: "request",
					},
				},
			},
			{
				name: "terminal closeout",
				checkpoint: {
					pendingInputContextSequences: [],
					terminalCloseout: {
						failureEventId: "event_failure",
						closeoutEventId: "event_closeout",
						disposition: "terminated",
					},
				},
				expected: {
					state: { state: "idle" },
					nextStep: { action: "await_input" },
				},
			},
		];

		for (const scenario of cases) {
			expect(
				deriveThreadTurnSnapshot(noPendingAttachments,
					scenario.checkpoint,
					scenario.routes ?? noRoutes,
					scenario.acceptedInputIds ?? [],
				),
				scenario.name,
			).toEqual(scenario.expected);
		}
	});

	test("out-of-order multi-Tool settlement and interrupt ordering remain stable", () => {
		const routes: ThreadToolRouteView = {
			routes: [
				{ toolUseEventId: "tool_a", disposition: "hot_execution" },
				{ toolUseEventId: "tool_b", disposition: "hot_execution" },
			],
		};
		const initial = initializeThreadTurnTransition(noPendingAttachments,
			sealedRequest([
				pendingTool("tool_a", "call_a", "Read"),
				pendingTool("tool_b", "call_b", "Grep"),
			]),
			routes,
		);

		const settledSecond = reduceThreadTurn(noPendingAttachments,
			initial,
			{
				fact: "tool_result_committed",
				toolUseEventId: "tool_b",
				outcome: "error",
			},
			{ routes: [routes.routes[0]!] },
		);
		expect(settledSecond).toMatchObject({
			state: { state: "waiting_for_tool_results", modelRequestId: "request" },
			nextStep: {
				action: "await_tool_results",
				modelRequestId: "request",
				toolUseEventIds: ["tool_a"],
			},
		});

		const interruptedBeforeLastSettlement = reduceThreadTurn(noPendingAttachments,
			settledSecond,
			{
				fact: "interrupt_committed",
				eventId: "event_interrupt",
			},
			{ routes: [routes.routes[0]!] },
		);
		expect(interruptedBeforeLastSettlement).toMatchObject({
			nextStep: { action: "close_interrupted", modelRequestId: "request" },
		});

		const lastSettlementAfterInterrupt = reduceThreadTurn(noPendingAttachments,
			interruptedBeforeLastSettlement,
			{
				fact: "tool_result_committed",
				toolUseEventId: "tool_a",
				outcome: "success",
			},
			noRoutes,
		);
		expect(lastSettlementAfterInterrupt).toMatchObject({
			state: { state: "request_sealed", modelRequestId: "request" },
			nextStep: { action: "close_interrupted", modelRequestId: "request" },
		});

		const allSettledBeforeInterrupt = reduceThreadTurn(noPendingAttachments,
			settledSecond,
			{
				fact: "tool_result_committed",
				toolUseEventId: "tool_a",
				outcome: "success",
			},
			noRoutes,
		);
		expect(allSettledBeforeInterrupt).toMatchObject({
			state: { state: "ready_to_request" },
			nextStep: { action: "prepare_next_request" },
		});
		expect(
			reduceThreadTurn(noPendingAttachments,
				allSettledBeforeInterrupt,
				{
					fact: "interrupt_committed",
					eventId: "event_interrupt_after_tools",
				},
				noRoutes,
			),
		).toMatchObject({
			nextStep: { action: "close_interrupted", modelRequestId: "request" },
		});
	});
});

function openRequest(): ThreadTurnCheckpoint {
	return {
		executionRunId: "run",
		pendingInputContextSequences: [],
		request: {
			modelRequestId: "request",
			requestStartEventId: "event_start",
			requestKind: "agent_provider_request",
			contextThroughMessageSequence: 1,
			toolMembers: [],
		},
	};
}

function sealedRequest(
	toolMembers: NonNullable<ThreadTurnCheckpoint["request"]>["toolMembers"],
	requestKind: NonNullable<
		ThreadTurnCheckpoint["request"]
	>["requestKind"] = "agent_provider_request",
	end: {
		readonly isError: boolean;
		readonly providerContextRetention: NonNullable<
			NonNullable<ThreadTurnCheckpoint["request"]>["requestEnd"]
		>["providerContextRetention"];
		readonly reschedule?: {
			readonly attempt: number;
			readonly effectiveDeadline: string;
			readonly providerAttempts: number;
			readonly compactionAttempts: number;
		};
		readonly errorKind?: string;
	} = productionRetentionFor(toolMembers),
): ThreadTurnCheckpoint {
	return {
		executionRunId: "run",
		pendingInputContextSequences: [],
		request: {
			modelRequestId: "request",
			requestStartEventId: "event_start",
			requestKind,
			contextThroughMessageSequence: 1,
			requestEnd: { eventId: "event_end", ...end },
			toolMembers,
		},
	};
}

function productionRetentionFor(
	toolMembers: NonNullable<ThreadTurnCheckpoint["request"]>["toolMembers"],
) {
	return {
		isError: false,
		providerContextRetention: {
			disposition: "completed" as const,
			toolUseEventIds: toolMembers.flatMap((member) =>
				member.memberKind === "public_tool_use"
					? [member.toolUseEventId]
					: [],
			),
			repairEventIds: toolMembers.flatMap((member) =>
				member.memberKind === "internal_tool_repair"
					? [member.repairEventId]
					: [],
			),
		},
	};
}

function pendingTool(
	toolUseEventId: string,
	modelToolCallId: string,
	toolName: string,
): NonNullable<ThreadTurnCheckpoint["request"]>["toolMembers"][number] {
	return {
		memberKind: "public_tool_use",
		modelToolCallId,
		toolUseEventId,
		toolName,
	};
}

function turnEvent(
	eventId: string,
	eventSequence: number,
	type:
		| "session.status_running"
		| "user.interrupt"
		| "session.status_terminated",
) {
	return { eventId, eventSequence, type } as const;
}

function requestStartEvent(eventSequence: number) {
	return {
		eventId: "event_start",
		eventSequence,
		type: "span.model_request_start" as const,
		modelRequestId: "request",
		requestStart: {
			requestKind: "agent_provider_request" as const,
			contextThroughMessageSequence: 1,
		},
	};
}

function toolUseEvent(
	eventId: string,
	eventSequence: number,
	modelToolCallId: string,
	toolName: string,
) {
	return {
		eventId,
		eventSequence,
		type: "agent.tool_use" as const,
		modelRequestId: "request",
		toolUse: { modelToolCallId, toolName },
	};
}

function toolResultEvent(
	eventId: string,
	eventSequence: number,
	modelToolCallId: string,
	toolName: string,
) {
	return {
		eventId,
		eventSequence,
		type: "agent.tool_result" as const,
		modelRequestId: "request",
		toolResult: { modelToolCallId, toolName, outcome: "completed" as const },
	};
}

function requestEndEvent(
	eventSequence: number,
	disposition: "completed" | "rescheduled",
	toolUseEventIds: readonly string[],
	repairEventIds: readonly string[],
	isError = false,
) {
	return {
		eventId: `event_end_${eventSequence}`,
		eventSequence,
		type: "span.model_request_end" as const,
		modelRequestId: "request",
		requestEnd: {
			requestStartEventId: "event_start",
			isError,
			providerContextRetention: {
				disposition,
				toolUseEventIds: [...toolUseEventIds],
				repairEventIds: [...repairEventIds],
			},
			...(disposition === "rescheduled"
				? {
						reschedule: {
							attempt: 1,
							effectiveDeadline: "2026-08-26T00:00:00.000Z",
							providerAttempts: 1,
							compactionAttempts: 0,
						},
					}
				: {}),
		},
	};
}
