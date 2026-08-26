import { describe, expect, test } from "bun:test";
import type {
	ThreadToolRouteView,
	ThreadTurnCheckpoint,
} from "../../../src/thread-loop/turn/checkpoint.js";
import {
	deriveThreadTurnSnapshot,
	initializeThreadTurnTransition,
	reduceThreadTurn,
} from "../../../src/thread-loop/turn/reducer.js";
import type { ThreadTurnFact } from "../../../src/thread-loop/turn/facts.js";
import type {
	ThreadActiveInputView,
	ThreadTurnTransition,
} from "../../../src/thread-loop/turn/types.js";
import { ThreadTurnContractError } from "../../../src/thread-loop/turn/types.js";

const noRoutes: ThreadToolRouteView = { routes: [] };
const noAttachments: ThreadActiveInputView = {
	hasPendingAttachments: false,
};

describe("Thread-turn reducer", () => {
	test("folds an ordinary run directly through its stable closeout", () => {
		let transition = emptyTransition();
		transition = apply(transition, {
			fact: "run_opened",
			eventId: "event_run",
		});
		transition = apply(transition, {
			fact: "inputs_committed",
			eventId: "event_inputs",
			contextSequences: [1],
		});
		const started = apply(transition, {
			fact: "request_started",
			eventId: "event_start",
			modelRequestId: "request_1",
			requestKind: "agent_provider_request",
			contextThroughMessageSequence: 1,
			consumedInputContextSequences: [1],
		});
		expect(started).toMatchObject({
			state: { state: "request_open", modelRequestId: "request_1" },
			nextStep: { action: "await_request_end", modelRequestId: "request_1" },
			dispatch: {
				dispatch: "start_provider_request",
				modelRequestId: "request_1",
			},
		});

		transition = apply(started, requestEnded("event_end", "request_1"));
		expect(transition).toMatchObject({
			state: { state: "ready_to_finish" },
			nextStep: { action: "finish_idle", stopReason: { type: "end_turn" } },
		});
		expect(transition.dispatch).toBeUndefined();

		transition = apply(transition, {
			fact: "finish_idle_committed",
			eventId: "event_idle",
			stopReason: { type: "end_turn" },
		});
		expect(transition).toMatchObject({
			checkpoint: {
				pendingInputContextSequences: [],
				idleCloseout: { eventId: "event_idle", stopReason: "end_turn" },
			},
			state: { state: "idle" },
			nextStep: { action: "await_input" },
		});
	});

	test("routes two Tools once and accepts reverse-order terminal results", () => {
		let transition = startedRequest();
		const firstTool = apply(transition, toolUse("event_tool_1", "call_1", "Read"));
		expect(firstTool.dispatch).toEqual({
			dispatch: "route_tool_use",
			toolUseEventId: "event_tool_1",
		});
		expect(firstTool.nextStep).toEqual({
			action: "await_request_end",
			modelRequestId: "request_1",
		});

		const secondTool = apply(
			firstTool,
			toolUse("event_tool_2", "call_2", "Grep"),
		);
		expect(secondTool.dispatch).toEqual({
			dispatch: "route_tool_use",
			toolUseEventId: "event_tool_2",
		});

		const routes: ThreadToolRouteView = {
			routes: [
				{ toolUseEventId: "event_tool_1", disposition: "hot_execution" },
				{ toolUseEventId: "event_tool_2", disposition: "hot_execution" },
			],
		};
		transition = apply(
			secondTool,
			requestEnded("event_end", "request_1"),
			routes,
		);
		expect(transition.nextStep).toEqual({
			action: "await_tool_results",
			modelRequestId: "request_1",
			toolUseEventIds: ["event_tool_1", "event_tool_2"],
		});

		transition = apply(
			transition,
			{
				fact: "tool_result_committed",
				toolUseEventId: "event_tool_2",
				outcome: "error",
			},
			{ routes: [routes.routes[0]!] },
		);
		expect(transition.nextStep).toMatchObject({
			action: "await_tool_results",
			toolUseEventIds: ["event_tool_1"],
		});

		transition = apply(transition, {
			fact: "tool_result_committed",
			toolUseEventId: "event_tool_1",
			outcome: "success",
		});
		expect(transition).toMatchObject({
			state: { state: "ready_to_request" },
			nextStep: { action: "prepare_next_request" },
		});
		expect(
			transition.checkpoint.request?.toolMembers.map((member) =>
				member.memberKind === "public_tool_use"
					? [member.toolUseEventId, member.terminalResult?.outcome]
					: [],
			),
		).toEqual([
			["event_tool_1", "success"],
			["event_tool_2", "error"],
		]);
	});

	test("keeps approval and reschedule ownership behind unresolved Tools", () => {
		const approvalTool = apply(
			startedRequest(),
			toolUse("event_approval", "call_approval", "Write"),
		);
		const approvalRoutes: ThreadToolRouteView = {
			routes: [
				{
					toolUseEventId: "event_approval",
					disposition: "requires_user_action",
				},
			],
		};
		let approval = apply(
			approvalTool,
			requestEnded("event_approval_end", "request_1"),
			approvalRoutes,
		);
		expect(approval.nextStep).toEqual({
			action: "finish_idle",
			stopReason: {
				type: "requires_action",
				eventIds: ["event_approval"],
			},
		});
		approval = apply(
			approval,
			{
				fact: "finish_idle_committed",
				eventId: "event_approval_idle",
				stopReason: {
					type: "requires_action",
					eventIds: ["event_approval"],
				},
			},
			approvalRoutes,
		);
		expect(approval.nextStep).toMatchObject({
			action: "await_tool_results",
			toolUseEventIds: ["event_approval"],
		});

		const rescheduleTool = apply(
			startedRequest(),
			toolUse("event_retry_tool", "call_retry_tool", "Read"),
		);
		const retryRoutes: ThreadToolRouteView = {
			routes: [
				{ toolUseEventId: "event_retry_tool", disposition: "hot_execution" },
			],
		};
		let retry = apply(
			rescheduleTool,
			{
				...requestEnded("event_retry_end", "request_1", true),
				reschedule: {
					attempt: 1,
					effectiveDeadline: "2026-08-26T00:00:00.000Z",
					providerAttempts: 1,
					compactionAttempts: 0,
				},
			},
			retryRoutes,
		);
		expect(retry.nextStep).toMatchObject({
			action: "await_tool_results",
			toolUseEventIds: ["event_retry_tool"],
		});
		retry = apply(retry, {
			fact: "tool_result_committed",
			toolUseEventId: "event_retry_tool",
			outcome: "success",
		});
		expect(retry.nextStep).toEqual({
			action: "apply_request_retry_or_reschedule",
			modelRequestId: "request_1",
		});
	});

	test("folds repair, interrupt, Request End, and terminal closeout facts", () => {
		let transition = apply(startedRequest(), {
			fact: "internal_tool_repair_committed",
			eventId: "event_repair",
			modelRequestId: "request_1",
			modelToolCallId: "call_repair",
			toolName: "UnavailableTool",
		});
		expect(transition.checkpoint.request?.toolMembers).toHaveLength(1);
		expect(transition.dispatch).toBeUndefined();

		transition = apply(transition, {
			fact: "interrupt_committed",
			eventId: "event_interrupt",
		});
		expect(transition.nextStep).toEqual({
			action: "close_interrupted",
			modelRequestId: "request_1",
		});

		transition = apply(
			transition,
			requestEnded("event_interrupted_end", "request_1"),
		);
		expect(transition.nextStep).toEqual({
			action: "close_interrupted",
			modelRequestId: "request_1",
		});
		expect(transition.dispatch).toBeUndefined();

		transition = apply(transition, {
			fact: "terminal_closeout_committed",
			eventId: "event_terminal",
			failureEventId: "event_failure",
			disposition: "terminated",
		});
		expect(transition).toMatchObject({
			checkpoint: {
				terminalCloseout: {
					failureEventId: "event_failure",
					closeoutEventId: "event_terminal",
					disposition: "terminated",
				},
			},
			state: { state: "idle" },
			nextStep: { action: "await_input" },
		});
	});

	test("keeps dispatch stack-local across view refresh and duplicate replay", () => {
		const ready = apply(
			apply(emptyTransition(), {
				fact: "run_opened",
				eventId: "event_run",
			}),
			{
				fact: "inputs_committed",
				eventId: "event_inputs",
				contextSequences: [1],
			},
		);
		const started = apply(ready, {
			fact: "request_started",
			eventId: "event_start",
			modelRequestId: "request_1",
			requestKind: "agent_provider_request",
			contextThroughMessageSequence: 1,
			consumedInputContextSequences: [1],
		});
		expect(started.dispatch).toEqual({
			dispatch: "start_provider_request",
			modelRequestId: "request_1",
		});

		const refreshed = deriveThreadTurnSnapshot(
			started.checkpoint,
			noRoutes,
			["runtime_input_later"],
			noAttachments,
		);
		expect(refreshed).toEqual({
			state: { state: "request_open", modelRequestId: "request_1" },
			nextStep: { action: "await_request_end", modelRequestId: "request_1" },
		});
		expect(started.dispatch).toEqual({
			dispatch: "start_provider_request",
			modelRequestId: "request_1",
		});

		const duplicate = apply(started, {
			fact: "request_started",
			eventId: "event_start",
			modelRequestId: "request_1",
			requestKind: "agent_provider_request",
			contextThroughMessageSequence: 1,
			consumedInputContextSequences: [1],
		});
		expect(duplicate.dispatch).toBeUndefined();
		expect(duplicate.nextStep).toEqual(refreshed.nextStep);
	});

	test("Request End directly owns its stable progression and duplicate replay", () => {
		const ended = apply(
			startedRequest(),
			requestEnded("event_end_direct", "request_1"),
		);
		expect(ended.nextStep).toEqual({
			action: "finish_idle",
			stopReason: { type: "end_turn" },
		});
		expect(ended.dispatch).toBeUndefined();

		const duplicate = apply(
			ended,
			requestEnded("event_end_direct", "request_1"),
		);
		expect(duplicate.nextStep).toEqual(ended.nextStep);
		expect(duplicate.dispatch).toBeUndefined();
	});

	test("hot transitions and cold snapshots share stable state", () => {
		const hotCases = [
			startedRequest(),
			apply(
				apply(
					startedRequest(),
					toolUse("event_tool", "call_tool", "Read"),
				),
				requestEnded("event_end", "request_1"),
				{
					routes: [
						{ toolUseEventId: "event_tool", disposition: "hot_execution" },
					],
				},
			),
			apply(startedRequest(), {
				fact: "interrupt_committed",
				eventId: "event_interrupt",
			}),
		] as const;

		for (const hot of hotCases) {
			const routes = coldRoutesFor(hot.checkpoint);
			const cold = deriveThreadTurnSnapshot(
				hot.checkpoint,
				routes,
				[],
				noAttachments,
			);
			expect(cold).toEqual({ state: hot.state, nextStep: hot.nextStep });
		}
	});

	test("rejects conflicting durable identities", () => {
		const started = startedRequest();
		expect(() =>
			apply(started, {
				fact: "request_ended",
				eventId: "event_end",
				modelRequestId: "different_request",
				isError: false,
				providerContextRetention: retention("completed"),
			}),
		).toThrow(ThreadTurnContractError);
	});
});

function emptyTransition(): ThreadTurnTransition {
	return initializeThreadTurnTransition(
		{ pendingInputContextSequences: [] },
		noRoutes,
		[],
		noAttachments,
	);
}

function startedRequest(): ThreadTurnTransition {
	let transition = emptyTransition();
	transition = apply(transition, {
		fact: "run_opened",
		eventId: "event_run",
	});
	transition = apply(transition, {
		fact: "inputs_committed",
		eventId: "event_inputs",
		contextSequences: [1],
	});
	return apply(transition, {
		fact: "request_started",
		eventId: "event_start",
		modelRequestId: "request_1",
		requestKind: "agent_provider_request",
		contextThroughMessageSequence: 1,
		consumedInputContextSequences: [1],
	});
}

function apply(
	current: ThreadTurnTransition,
	fact: ThreadTurnFact,
	routes: ThreadToolRouteView = noRoutes,
	acceptedInputIds: readonly string[] = [],
): ThreadTurnTransition {
	return reduceThreadTurn(
		current,
		fact,
		routes,
		acceptedInputIds,
		noAttachments,
	);
}

function toolUse(
	eventId: string,
	modelToolCallId: string,
	toolName: string,
): ThreadTurnFact {
	return {
		fact: "tool_use_committed",
		eventId,
		modelRequestId: "request_1",
		modelToolCallId,
		toolName,
	};
}

function requestEnded(
	eventId: string,
	modelRequestId: string,
	isError = false,
): Extract<ThreadTurnFact, { readonly fact: "request_ended" }> {
	return {
		fact: "request_ended",
		eventId,
		modelRequestId,
		isError,
		providerContextRetention: retention(isError ? "failed" : "completed"),
	};
}

function retention(
	disposition: "completed" | "failed",
): Extract<
	ThreadTurnFact,
	{ readonly fact: "request_ended" }
>["providerContextRetention"] {
	return { disposition, toolUseEventIds: [], repairEventIds: [] };
}

function coldRoutesFor(checkpoint: ThreadTurnCheckpoint): ThreadToolRouteView {
	return {
		routes:
			checkpoint.request?.toolMembers.flatMap((member) =>
				member.memberKind === "public_tool_use" &&
				member.terminalResult === undefined
					? [
							{
								toolUseEventId: member.toolUseEventId,
								disposition: "hot_execution" as const,
							},
						]
					: [],
			) ?? [],
	};
}
