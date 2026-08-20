import { describe, expect, test } from "bun:test";
import type {
	ThreadToolRouteView,
	ThreadTurnCheckpoint,
} from "../../../src/thread-loop/thread-turn-checkpoint.js";
import {
	deriveThreadTurnDecision as deriveThreadTurnDecisionWithActiveInput,
	initializeThreadTurnReduction as initializeThreadTurnReductionWithActiveInput,
	reduceThreadTurn as reduceThreadTurnWithActiveInput,
} from "../../../src/thread-loop/thread-turn-reducer.js";
import { toGatewayProviderContext } from "../../../src/runtime/context-projection.js";

const noRoutes: ThreadToolRouteView = { routes: [] };
type ActiveInputView = Parameters<
	typeof deriveThreadTurnDecisionWithActiveInput
>[3];
const noPendingAttachments: ActiveInputView = { hasPendingAttachments: false };

const deriveThreadTurnDecision = (
	activeInputView: ActiveInputView,
	checkpoint: Parameters<typeof deriveThreadTurnDecisionWithActiveInput>[0],
	routes: Parameters<typeof deriveThreadTurnDecisionWithActiveInput>[1],
	acceptedInputIds: readonly string[] = [],
) =>
	deriveThreadTurnDecisionWithActiveInput(
		checkpoint,
		routes,
		acceptedInputIds,
		activeInputView,
	);
const initializeThreadTurnReduction = (
	activeInputView: ActiveInputView,
	checkpoint: Parameters<
		typeof initializeThreadTurnReductionWithActiveInput
	>[0],
	routes: Parameters<typeof initializeThreadTurnReductionWithActiveInput>[1],
	acceptedInputIds: readonly string[] = [],
) =>
	initializeThreadTurnReductionWithActiveInput(
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

		const pending = initializeThreadTurnReduction(noPendingAttachments,
			sealedRequest([pendingTool("tool_pending", "call_pending", "Read")]),
			{ routes: [{ toolUseEventId: "tool_pending", disposition: "hot_execution" }] },
		);
		expect(pending).toMatchObject({
			state: { state: "waiting_for_tool_results" },
			action: { action: "await_tool_results", toolUseEventIds: ["tool_pending"] },
		});

		const terminal = reduceThreadTurn(noPendingAttachments,
			pending,
			{ fact: "tool_result_committed", toolUseEventId: "tool_pending", outcome: "success" },
			noRoutes,
		);
		expect(terminal).toMatchObject({
			state: { state: "ready_to_request" },
			action: { action: "prepare_next_request" },
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
			readonly expected: ReturnType<typeof deriveThreadTurnDecision>;
		}[] = [
			{
				name: "empty thread",
				checkpoint: { pendingInputContextSequences: [] },
				expected: {
					state: { state: "idle" },
					action: { action: "await_input" },
				},
			},
			{
				name: "committed provider input",
				checkpoint: { pendingInputContextSequences: [1] },
				expected: {
					state: { state: "ready_to_request" },
					action: { action: "prepare_next_request" },
				},
			},
			{
				name: "accepted child completion or mail wake",
				checkpoint: { pendingInputContextSequences: [] },
				acceptedInputIds: ["runtime_input_wake"],
				expected: {
					state: { state: "idle" },
					action: {
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
					action: { action: "await_request_end", modelRequestId: "request" },
				},
			},
			{
				name: "sealed request without a Tool",
				checkpoint: sealedRequest([]),
				expected: {
					state: { state: "ready_to_finish" },
					action: { action: "finish_idle", stopReason: { type: "end_turn" } },
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
					action: {
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
					action: {
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
					action: {
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
					action: { action: "complete_reviewer", modelRequestId: "request" },
				},
			},
			{
				name: "reviewer failure",
				checkpoint: reviewerFailure,
				expected: {
					state: { state: "request_sealed", modelRequestId: "request" },
					action: {
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
					action: { action: "close_interrupted", modelRequestId: "request" },
				},
			},
			{
				name: "compaction continuation",
				checkpoint: sealedRequest([], "compaction_summary"),
				expected: {
					state: { state: "ready_to_request" },
					action: {
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
					action: { action: "await_input" },
				},
			},
		];

		for (const scenario of cases) {
			expect(
				deriveThreadTurnDecision(noPendingAttachments,
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
		const initial = initializeThreadTurnReduction(noPendingAttachments,
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
			action: {
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
			action: { action: "close_interrupted", modelRequestId: "request" },
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
			action: { action: "close_interrupted", modelRequestId: "request" },
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
			action: { action: "prepare_next_request" },
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
			action: { action: "close_interrupted", modelRequestId: "request" },
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
	} = {
		isError: false,
			providerContextRetention: { disposition: "completed", toolUseEventIds: [], repairEventIds: [] },
	},
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
