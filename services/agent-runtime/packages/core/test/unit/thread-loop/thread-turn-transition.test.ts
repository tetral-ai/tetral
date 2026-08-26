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
import { ThreadState } from "../../../src/thread-loop/thread-state.js";

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
		const firstTool = apply(
			transition,
			toolUse("event_tool_1", "call_1", "Read"),
		);
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
			requestEnded("event_end", "request_1", false, {
				disposition: "completed",
				toolUseEventIds: ["event_tool_1", "event_tool_2"],
				repairEventIds: [],
			}),
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

	test("hands one Tool route off once across duplicate replay and terminal closeout", () => {
		const committed = apply(
			startedRequest(),
			toolUse("event_tool_once", "call_tool_once", "Read"),
		);
		expect(committed.dispatch).toEqual({
			dispatch: "route_tool_use",
			toolUseEventId: "event_tool_once",
		});

		const duplicate = apply(
			committed,
			toolUse("event_tool_once", "call_tool_once", "Read"),
		);
		expect(duplicate.checkpoint).toEqual(committed.checkpoint);
		expect({ state: duplicate.state, nextStep: duplicate.nextStep }).toEqual({
			state: committed.state,
			nextStep: committed.nextStep,
		});
		expect(duplicate.dispatch).toBeUndefined();

		const interrupted = apply(duplicate, {
			fact: "interrupt_committed",
			eventId: "event_interrupt_after_handoff",
		});
		expect(interrupted.dispatch).toBeUndefined();
		const terminal = apply(interrupted, {
			fact: "terminal_closeout_committed",
			eventId: "event_terminal_after_handoff",
			failureEventId: "event_failure_after_handoff",
			disposition: "terminated",
		});
		expect(terminal.dispatch).toBeUndefined();
		expect(terminal.nextStep).toEqual({ action: "await_input" });
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
			requestEnded("event_approval_end", "request_1", false, {
				disposition: "completed",
				toolUseEventIds: ["event_approval"],
				repairEventIds: [],
			}),
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
				...requestEnded("event_retry_end", "request_1", true, {
					disposition: "rescheduled",
					toolUseEventIds: ["event_retry_tool"],
					repairEventIds: [],
				}),
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
			requestEnded("event_interrupted_end", "request_1", false, {
				disposition: "interrupted",
				toolUseEventIds: [],
				repairEventIds: ["event_repair"],
			}),
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
		const unresolvedTool = apply(
			apply(startedRequest(), toolUse("event_tool", "call_tool", "Read")),
			requestEnded("event_end", "request_1", false, {
				disposition: "completed",
				toolUseEventIds: ["event_tool"],
				repairEventIds: [],
			}),
			{
				routes: [
					{ toolUseEventId: "event_tool", disposition: "hot_execution" },
				],
			},
		);
		const completedTool = apply(unresolvedTool, {
			fact: "tool_result_committed",
			toolUseEventId: "event_tool",
			outcome: "success",
		});
		const rescheduled = apply(startedRequest(), {
			...requestEnded("event_rescheduled", "request_1", true),
			reschedule: {
				attempt: 1,
				effectiveDeadline: "2026-08-26T00:00:00.000Z",
				providerAttempts: 1,
				compactionAttempts: 0,
			},
		});
		const idle = apply(
			apply(startedRequest(), requestEnded("event_idle_end", "request_1")),
			{
				fact: "finish_idle_committed",
				eventId: "event_idle",
				stopReason: { type: "end_turn" },
			},
		);
		const hotCases = [
			startedRequest(),
			unresolvedTool,
			completedTool,
			rescheduled,
			apply(startedRequest(), {
				fact: "interrupt_committed",
				eventId: "event_interrupt",
			}),
			idle,
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

	test("keeps Request Start dispatch stack-local across ordinary admission", () => {
		const state = new ThreadState("session_dispatch_owner");
		state.installThreadTurn(
			{
				executionRunId: "event_run_dispatch_owner",
				pendingInputContextSequences: [1],
			},
			noRoutes,
		);
		const owner = state.threadTurnTransition();
		const started = state.applyRequestStartFact(owner, {
			fact: "request_started",
			eventId: "event_start_dispatch_owner",
			modelRequestId: "request_dispatch_owner",
			requestKind: "agent_provider_request",
			contextThroughMessageSequence: 1,
			consumedInputContextSequences: [1],
		});
		expect(
			state.enqueueAcceptedInput({
				workspaceId: "workspace_dispatch_owner",
				sessionId: "session_dispatch_owner",
				sessionThreadId: "thread_dispatch_owner",
				bindingId: "binding_dispatch_owner",
				bindingGeneration: 1,
				targetPodUid: "pod_dispatch_owner",
				runtimeInputId: "input_after_request_start",
				inputOrder: 2,
				kind: "messages",
				contentJson: JSON.stringify({ messages: [{ parts: [] }] }),
			}),
		).toBe("applied");

		const delayedDispatch = started.dispatch;
		expect(delayedDispatch).toEqual({
			dispatch: "start_provider_request",
			modelRequestId: "request_dispatch_owner",
		});
		const resident = state.threadTurnTransition();
		expect(resident).toEqual({
			checkpoint: started.checkpoint,
			state: {
				state: "request_open",
				modelRequestId: "request_dispatch_owner",
			},
			nextStep: {
				action: "await_request_end",
				modelRequestId: "request_dispatch_owner",
			},
		});
		expect("dispatch" in resident).toBe(false);
		if (delayedDispatch?.dispatch !== "start_provider_request") {
			throw new ThreadTurnContractError(
				"delayed Request Start handoff lost its stack-local dispatch",
			);
		}
		expect(delayedDispatch.modelRequestId).toBe("request_dispatch_owner");
	});

	test("fails closed for representative invariant families without partial transition", () => {
		const open = startedRequest();
		const ended = apply(open, requestEnded("event_end", "request_1"));
		const settled = apply(
			apply(open, toolUse("event_tool", "call_tool", "Read")),
			{
				fact: "tool_result_committed",
				toolUseEventId: "event_tool",
				outcome: "success",
			},
		);
		const cases: readonly {
			readonly name: string;
			readonly current: ThreadTurnTransition;
			readonly fact: ThreadTurnFact;
		}[] = [
			{
				name: "conflicting durable identity",
				current: open,
				fact: requestEnded("event_end_other", "different_request"),
			},
			{
				name: "illegal lifecycle order",
				current: ended,
				fact: toolUse("event_tool_late", "call_tool_late", "Read"),
			},
			{
				name: "missing owning request member",
				current: open,
				fact: {
					fact: "tool_result_committed",
					toolUseEventId: "event_tool_missing",
					outcome: "success",
				},
			},
			{
				name: "conflicting duplicate",
				current: settled,
				fact: {
					fact: "tool_result_committed",
					toolUseEventId: "event_tool",
					outcome: "error",
				},
			},
		];

		for (const scenario of cases) {
			const before = structuredClone(scenario.current);
			let partialTransition: ThreadTurnTransition | undefined;
			let thrown: unknown;
			try {
				partialTransition = apply(before, scenario.fact);
			} catch (error) {
				thrown = error;
			}
			expect(thrown, scenario.name).toBeInstanceOf(ThreadTurnContractError);
			expect(partialTransition, scenario.name).toBeUndefined();
			expect(before, scenario.name).toEqual(scenario.current);
		}
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
	providerContextRetention = retention(isError ? "failed" : "completed"),
): Extract<ThreadTurnFact, { readonly fact: "request_ended" }> {
	return {
		fact: "request_ended",
		eventId,
		modelRequestId,
		isError,
		providerContextRetention,
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
