import { describe, expect, test } from "bun:test";
import type {
	ThreadToolRouteView,
	ThreadTurnCheckpoint,
} from "../../../src/thread-loop/thread-turn-checkpoint.js";
import type { ThreadTurnAction } from "../../../src/thread-loop/thread-turn-reducer.js";
import {
	deriveThreadTurnDecision as deriveThreadTurnDecisionWithActiveInput,
	initializeThreadTurnReduction as initializeThreadTurnReductionWithActiveInput,
	reconcileThreadTurnSeal as reconcileThreadTurnSealWithActiveInput,
	reduceThreadTurn as reduceThreadTurnWithActiveInput,
	ThreadTurnContractError,
} from "../../../src/thread-loop/thread-turn-reducer.js";

const noRoutes: ThreadToolRouteView = { routes: [] };
const noActiveInput = { hasPendingAttachments: false } as const;

const deriveThreadTurnDecision = (
	checkpoint: Parameters<typeof deriveThreadTurnDecisionWithActiveInput>[0],
	routes: Parameters<typeof deriveThreadTurnDecisionWithActiveInput>[1],
	acceptedInputIds: readonly string[] = [],
) =>
	deriveThreadTurnDecisionWithActiveInput(
		checkpoint,
		routes,
		acceptedInputIds,
		noActiveInput,
	);

const initializeThreadTurnReduction = (
	checkpoint: Parameters<typeof initializeThreadTurnReductionWithActiveInput>[0],
	routes: Parameters<typeof initializeThreadTurnReductionWithActiveInput>[1],
	acceptedInputIds: readonly string[] = [],
) =>
	initializeThreadTurnReductionWithActiveInput(
		checkpoint,
		routes,
		acceptedInputIds,
		noActiveInput,
	);

const reduceThreadTurn = (
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
		noActiveInput,
	);

const reconcileThreadTurnSeal = (
	current: Parameters<typeof reconcileThreadTurnSealWithActiveInput>[0],
	routes: Parameters<typeof reconcileThreadTurnSealWithActiveInput>[1],
	acceptedInputIds: readonly string[] = [],
) =>
	reconcileThreadTurnSealWithActiveInput(
		current,
		routes,
		acceptedInputIds,
		noActiveInput,
	);

describe("Thread-turn reducer", () => {
	test("derives idle and plural committed-input readiness", () => {
		expect(
			deriveThreadTurnDecision({ pendingInputContextSequences: [] }, noRoutes),
		).toEqual({
			state: { state: "idle" },
			action: { action: "await_input" },
		});
		expect(
			deriveThreadTurnDecision(
				{
					pendingInputContextSequences: [1, 2],
				},
				noRoutes,
			),
		).toEqual({
			state: { state: "ready_to_request" },
			action: { action: "prepare_next_request" },
		});
	});

	test("retains accepted input behind active work and selects one exact commit at a safe boundary", () => {
		const oneOutstandingTool = sealedCheckpoint([
			{
				memberKind: "public_tool_use",
				modelToolCallId: "call_waiting",
				toolUseEventId: "event_tool_waiting",
				toolName: "Read",
			},
		]);
		const multipleOutstandingTools = sealedCheckpoint([
			{
				memberKind: "public_tool_use",
				modelToolCallId: "call_waiting_1",
				toolUseEventId: "event_tool_waiting_1",
				toolName: "Read",
			},
			{
				memberKind: "public_tool_use",
				modelToolCallId: "call_waiting_2",
				toolUseEventId: "event_tool_waiting_2",
				toolName: "Read",
			},
		]);
		const cases: readonly {
			readonly name: string;
			readonly checkpoint: ThreadTurnCheckpoint;
			readonly routes: ThreadToolRouteView;
			readonly expectedAction: ThreadTurnAction["action"];
		}[] = [
			{
				name: "idle",
				checkpoint: { pendingInputContextSequences: [] },
				routes: noRoutes,
				expectedAction: "commit_accepted_input",
			},
			{
				name: "open request",
				checkpoint: openRequest().checkpoint,
				routes: noRoutes,
				expectedAction: "await_request_end",
			},
			{
				name: "one outstanding Tool Result",
				checkpoint: oneOutstandingTool,
				routes: {
					routes: [
						{
							toolUseEventId: "event_tool_waiting",
							disposition: "hot_execution",
						},
					],
				},
				expectedAction: "await_tool_results",
			},
			{
				name: "multiple outstanding Tool Results",
				checkpoint: multipleOutstandingTools,
				routes: {
					routes: [
						{
							toolUseEventId: "event_tool_waiting_1",
							disposition: "hot_execution",
						},
						{
							toolUseEventId: "event_tool_waiting_2",
							disposition: "hot_execution",
						},
					],
				},
				expectedAction: "await_tool_results",
			},
			{
				name: "interrupt",
				checkpoint: {
					...sealedCheckpoint([]),
					interruptEventId: "event_interrupt",
				},
				routes: noRoutes,
				expectedAction: "close_interrupted",
			},
			{
				name: "terminal closeout",
				checkpoint: {
					...sealedCheckpoint([]),
					terminalCloseout: {
						failureEventId: "event_failure",
						closeoutEventId: "event_closeout",
						disposition: "terminated",
					},
				},
				routes: noRoutes,
				expectedAction: "await_input",
			},
			{
				name: "sealed safe boundary",
				checkpoint: sealedCheckpoint([]),
				routes: noRoutes,
				expectedAction: "commit_accepted_input",
			},
		];

		for (const testCase of cases) {
			const decision = deriveThreadTurnDecision(
				testCase.checkpoint,
				testCase.routes,
				["rin_first", "rin_second"],
			);
			expect(decision.action.action, testCase.name).toBe(
				testCase.expectedAction,
			);
			if (decision.action.action === "commit_accepted_input") {
				expect(decision.action.runtimeInputId, testCase.name).toBe("rin_first");
			}
		}
	});

	test("a committed-input result advances the same reducer to request preparation", () => {
		const selected = initializeThreadTurnReduction(
			{ executionRunId: "run_input", pendingInputContextSequences: [] },
			noRoutes,
			["rin_input"],
		);
		expect(selected.action).toEqual({
			action: "commit_accepted_input",
			runtimeInputId: "rin_input",
		});

		const applied = reduceThreadTurn(
			selected,
			{
				fact: "inputs_committed",
				eventId: "event_input_committed",
				contextSequences: [1],
			},
			noRoutes,
		);
		expect(applied).toMatchObject({
			checkpoint: { pendingInputContextSequences: [1] },
			state: { state: "ready_to_request" },
			action: { action: "prepare_next_request" },
		});
	});

	test("an attachment-only input advances without inventing a context sequence", () => {
		const selected = initializeThreadTurnReduction(
			{ executionRunId: "run_attachment", pendingInputContextSequences: [] },
			noRoutes,
			["rin_attachment"],
		);

		const applied = reduceThreadTurnWithActiveInput(
			selected,
			{
				fact: "inputs_committed",
				eventId: "event_attachment_committed",
				contextSequences: [],
			},
			noRoutes,
			[],
			{ hasPendingAttachments: true },
		);

		expect(applied).toMatchObject({
			checkpoint: { pendingInputContextSequences: [] },
			state: { state: "ready_to_request" },
			action: { action: "prepare_next_request" },
		});
	});

	test("a sibling accepted input is committed before an attachment-only request advances", () => {
		const selected = initializeThreadTurnReduction(
			{ executionRunId: "run_siblings", pendingInputContextSequences: [] },
			noRoutes,
			["rin_attachment", "rin_text"],
		);

		const applied = reduceThreadTurnWithActiveInput(
			selected,
			{
				fact: "inputs_committed",
				eventId: "event_attachment_committed",
				contextSequences: [],
			},
			noRoutes,
			["rin_text"],
			{ hasPendingAttachments: true },
		);

		expect(applied).toMatchObject({
			checkpoint: { pendingInputContextSequences: [] },
			action: {
				action: "commit_accepted_input",
				runtimeInputId: "rin_text",
			},
		});
	});

	test("a cold attachment-only run prepares a request from its active-input view", () => {
		const recovered = initializeThreadTurnReductionWithActiveInput(
			{ executionRunId: "run_recovered", pendingInputContextSequences: [] },
			noRoutes,
			[],
			{ hasPendingAttachments: true },
		);

		expect(recovered).toMatchObject({
			checkpoint: { pendingInputContextSequences: [] },
			state: { state: "ready_to_request" },
			action: { action: "prepare_next_request" },
		});
	});

	test("a replayed committed-input result preserves one pending durable context sequence", () => {
		const cold = initializeThreadTurnReduction(
			{
				executionRunId: "run_reloaded",
				pendingInputContextSequences: [1],
			},
			noRoutes,
		);

		const replayed = reduceThreadTurn(
			cold,
			{
				fact: "inputs_committed",
				eventId: "event_commit_replayed",
				contextSequences: [1],
			},
			noRoutes,
		);

		expect(replayed).toMatchObject({
			checkpoint: { pendingInputContextSequences: [1] },
			state: { state: "ready_to_request" },
			action: { action: "prepare_next_request" },
		});
	});

	test("a durable run-open fact clears a prior final closeout before new input is committed", () => {
		const closed = initializeThreadTurnReduction(
			{
				pendingInputContextSequences: [],
				request: {
					modelRequestId: "request_closed",
					requestStartEventId: "event_start_closed",
					requestKind: "agent_provider_request",
					contextThroughMessageSequence: 1,
					requestEnd: {
						eventId: "event_end_closed",
						isError: true,
						errorKind: "provider_stream_error",
						rescheduled: false,
					},
					toolMembers: [],
				},
				terminalCloseout: {
					failureEventId: "event_failure_closed",
					closeoutEventId: "event_idle_closed",
					disposition: "retries_exhausted",
				},
				idleCloseout: {
					eventId: "event_idle_closed",
					stopReason: "retries_exhausted",
				},
			},
			noRoutes,
		);
		const opened = reduceThreadTurn(
			closed,
			{
				fact: "run_opened",
				eventId: "event_running_new",
			},
			noRoutes,
		);
		expect(opened.checkpoint).toEqual({
			executionRunId: "event_running_new",
			pendingInputContextSequences: [],
		});
		expect(
			reduceThreadTurn(
				opened,
				{
					fact: "inputs_committed",
					eventId: "event_input_new",
					contextSequences: [1],
				},
				noRoutes,
			),
		).toMatchObject({
			state: { state: "ready_to_request" },
			action: { action: "prepare_next_request" },
		});
	});

	test("a duplicate run-open fact preserves the action already derived for that run", () => {
		const initial = initializeThreadTurnReduction(
			{
				pendingInputContextSequences: [],
			},
			noRoutes,
		);
		const opened = reduceThreadTurn(
			initial,
			{
				fact: "run_opened",
				eventId: "event_running_replayed",
			},
			noRoutes,
		);
		expect(opened).toMatchObject({
			state: { state: "ready_to_finish" },
			action: { action: "finish_idle", stopReason: { type: "end_turn" } },
		});

		const replayed = reduceThreadTurn(
			opened,
			{
				fact: "run_opened",
				eventId: "event_running_replayed",
			},
			noRoutes,
		);
		expect(replayed).toMatchObject({
			state: { state: "ready_to_finish" },
			action: { action: "finish_idle", stopReason: { type: "end_turn" } },
		});

		expect(
			reduceThreadTurn(
				replayed,
				{
					fact: "finish_idle_committed",
					eventId: "event_idle_replayed",
					stopReason: { type: "end_turn" },
				},
				noRoutes,
			),
		).toMatchObject({
			state: { state: "idle" },
			action: { action: "await_input" },
		});
	});

	test("a confirmation run-open preserves the sealed requires-action request", () => {
		const waiting = initializeThreadTurnReduction(
			{
				...sealedCheckpoint([
					{
						memberKind: "public_tool_use",
						modelToolCallId: "call_approval",
						toolUseEventId: "event_tool_approval",
						toolName: "Write",
					},
				]),
				idleCloseout: {
					eventId: "event_idle_approval",
					stopReason: "requires_action",
				},
			},
			{
				routes: [
					{
						toolUseEventId: "event_tool_approval",
						disposition: "requires_user_action",
					},
				],
			},
		);
		const opened = reduceThreadTurn(
			waiting,
			{
				fact: "run_opened",
				eventId: "event_running_confirmation",
			},
			{
				routes: [
					{
						toolUseEventId: "event_tool_approval",
						disposition: "requires_user_action",
					},
				],
			},
		);
		expect(opened.checkpoint).toMatchObject({
			executionRunId: "event_running_confirmation",
			request: { modelRequestId: "request_1" },
		});
		expect(opened.checkpoint.idleCloseout).toBeUndefined();
	});

	test("a recovery run-open preserves any sealed request with an outstanding durable member", () => {
		const routes = {
			routes: [
				{
					toolUseEventId: "event_tool_recovery",
					disposition: "resume_sandbox_execution" as const,
				},
			],
		};
		const waiting = initializeThreadTurnReduction(
			sealedCheckpoint([
				{
					memberKind: "public_tool_use",
					modelToolCallId: "call_recovery",
					toolUseEventId: "event_tool_recovery",
					toolName: "Bash",
				},
			]),
			routes,
		);
		const opened = reduceThreadTurn(
			waiting,
			{
				fact: "run_opened",
				eventId: "event_running_recovery",
			},
			routes,
		);
		expect(opened.checkpoint).toMatchObject({
			executionRunId: "event_running_recovery",
			request: { modelRequestId: "request_1" },
		});
		expect(opened).toMatchObject({
			state: { state: "waiting_for_tool_results" },
			action: {
				action: "resume_tool_routes",
				toolUseEventIds: ["event_tool_recovery"],
			},
		});
	});

	test("a recovery run-open preserves a sealed request whose tool results are ready to continue", () => {
		const { executionRunId: _closedRun, ...recoveredCheckpoint } =
			sealedCheckpoint([
				{
					memberKind: "public_tool_use",
					modelToolCallId: "call_completed",
					toolUseEventId: "event_tool_completed",
					toolName: "Read",
					terminalResult: { outcome: "success" },
				},
			]);
		const ready = initializeThreadTurnReduction(recoveredCheckpoint, noRoutes);
		expect(ready).toMatchObject({
			state: { state: "ready_to_request" },
			action: { action: "prepare_next_request" },
		});

		const opened = reduceThreadTurn(
			ready,
			{
				fact: "run_opened",
				eventId: "event_running_continuation",
			},
			noRoutes,
		);
		expect(opened).toMatchObject({
			checkpoint: {
				executionRunId: "event_running_continuation",
				request: { modelRequestId: "request_1" },
			},
			state: { state: "ready_to_request" },
			action: { action: "prepare_next_request" },
		});
	});

	test("starts one prepared provider request only on first local Request Start ACK application", () => {
		const initial = initializeThreadTurnReduction(
			{
				executionRunId: "run_1",
				pendingInputContextSequences: [1, 2],
			},
			noRoutes,
		);
		const started = reduceThreadTurn(
			initial,
			{
				fact: "request_started",
				eventId: "event_start",
				modelRequestId: "request_1",
				requestKind: "agent_provider_request",
				contextThroughMessageSequence: 2,
				consumedInputContextSequences: [1, 2],
			},
			noRoutes,
		);

		expect(started).toMatchObject({
			checkpoint: {
				pendingInputContextSequences: [],
				request: { modelRequestId: "request_1" },
			},
			state: { state: "request_open", modelRequestId: "request_1" },
			action: { action: "start_provider_request", modelRequestId: "request_1" },
		});

		expect(
			reduceThreadTurn(
				started,
				{
					fact: "request_started",
					eventId: "event_start",
					modelRequestId: "request_1",
					requestKind: "agent_provider_request",
					contextThroughMessageSequence: 2,
					consumedInputContextSequences: [1, 2],
				},
				noRoutes,
			),
		).toMatchObject({
			state: { state: "request_open", modelRequestId: "request_1" },
			action: { action: "none" },
		});
	});

	test("keeps streaming Tool Use and early Tool Result inside an open request", () => {
		const opened = openRequest();
		const toolUse = reduceThreadTurn(
			opened,
			{
				fact: "tool_use_committed",
				eventId: "event_tool_1",
				modelRequestId: "request_1",
				modelToolCallId: "call_1",
				toolName: "Read",
			},
			noRoutes,
		);
		expect(toolUse).toMatchObject({
			state: { state: "request_open", modelRequestId: "request_1" },
			action: { action: "dispatch_tool_use", toolUseEventId: "event_tool_1" },
		});

		const result = reduceThreadTurn(
			toolUse,
			{
				fact: "tool_result_committed",
				toolUseEventId: "event_tool_1",
				outcome: "success",
			},
			noRoutes,
		);
		expect(result).toMatchObject({
			checkpoint: {
				request: {
					toolMembers: [{ terminalResult: { outcome: "success" } }],
				},
			},
			state: { state: "request_open", modelRequestId: "request_1" },
			action: { action: "await_request_end", modelRequestId: "request_1" },
		});

		const replayedResult = reduceThreadTurn(
			result,
			{
				fact: "tool_result_committed",
				toolUseEventId: "event_tool_1",
				outcome: "success",
			},
			noRoutes,
		);
		expect(replayedResult).toEqual({ ...result, action: { action: "none" } });
		expect(() =>
			reduceThreadTurn(
				result,
				{
					fact: "tool_result_committed",
					toolUseEventId: "event_tool_1",
					outcome: "error",
				},
				noRoutes,
			),
		).toThrow("conflicting terminal Tool Result");

		expect(
			reduceThreadTurn(
				toolUse,
				{
					fact: "tool_use_committed",
					eventId: "event_tool_1",
					modelRequestId: "request_1",
					modelToolCallId: "call_1",
					toolName: "Read",
				},
				noRoutes,
			),
		).toMatchObject({
			state: { state: "request_open", modelRequestId: "request_1" },
			action: { action: "none" },
		});
	});

	test("makes Request End an explicit seal before continuing", () => {
		const withTerminalTool = terminalToolRequest();
		const sealed = reduceThreadTurn(
			withTerminalTool,
			{
				fact: "request_ended",
				eventId: "event_end",
				modelRequestId: "request_1",
				isError: false,
				rescheduled: false,
			},
			noRoutes,
		);

		expect(sealed).toMatchObject({
			state: { state: "request_sealed", modelRequestId: "request_1" },
			action: { action: "reconcile_request_seal", modelRequestId: "request_1" },
		});
		expect(reconcileThreadTurnSeal(sealed, noRoutes)).toMatchObject({
			state: { state: "ready_to_request" },
			action: { action: "prepare_next_request" },
		});
	});

	test("waits for the whole sealed tool set and continues exactly after the last terminal result", () => {
		let reduction = sealRequestWithTools([
			["event_tool_1", "call_1"],
			["event_tool_2", "call_2"],
			["event_tool_3", "call_3"],
			["event_tool_4", "call_4"],
		]);
		const routes: ThreadToolRouteView = {
			routes: [
				{ toolUseEventId: "event_tool_1", disposition: "hot_execution" },
				{ toolUseEventId: "event_tool_2", disposition: "hot_execution" },
				{ toolUseEventId: "event_tool_3", disposition: "hot_execution" },
				{ toolUseEventId: "event_tool_4", disposition: "hot_execution" },
			],
		};
		reduction = reconcileThreadTurnSeal(reduction, routes);
		expect(reduction).toMatchObject({
			state: { state: "waiting_for_tool_results", modelRequestId: "request_1" },
			action: {
				action: "await_tool_results",
				toolUseEventIds: [
					"event_tool_1",
					"event_tool_2",
					"event_tool_3",
					"event_tool_4",
				],
			},
		});

		for (const [index, toolUseEventId] of [
			"event_tool_1",
			"event_tool_2",
			"event_tool_3",
		].entries()) {
			reduction = reduceThreadTurn(
				reduction,
				{
					fact: "tool_result_committed",
					toolUseEventId,
					outcome: "success",
				},
				routesForOutstanding(routes, toolUseEventId),
			);
			expect(reduction.state.state).toBe("waiting_for_tool_results");
			expect(reduction.action.action).toBe("await_tool_results");
		}

		reduction = reduceThreadTurn(
			reduction,
			{
				fact: "tool_result_committed",
				toolUseEventId: "event_tool_4",
				outcome: "error",
			},
			noRoutes,
		);
		expect(reduction).toMatchObject({
			state: { state: "ready_to_request" },
			action: { action: "prepare_next_request" },
		});
	});

	test("distinguishes empty-seal completion from later committed input", () => {
		const emptySeal = sealedCheckpoint([]);
		expect(deriveThreadTurnDecision(emptySeal, noRoutes)).toEqual({
			state: { state: "ready_to_finish" },
			action: { action: "finish_idle", stopReason: { type: "end_turn" } },
		});
		expect(
			deriveThreadTurnDecision(
				{
					...emptySeal,
					pendingInputContextSequences: [3],
				},
				noRoutes,
			),
		).toEqual({
			state: { state: "ready_to_request" },
			action: { action: "prepare_next_request" },
		});

		const readyToFinish = initializeThreadTurnReduction(emptySeal, noRoutes);
		const finished = reduceThreadTurn(
			readyToFinish,
			{
				fact: "finish_idle_committed",
				eventId: "event_idle",
				stopReason: { type: "end_turn" },
			},
			noRoutes,
		);
		expect(finished).toMatchObject({
			checkpoint: {
				idleCloseout: { eventId: "event_idle", stopReason: "end_turn" },
			},
			state: { state: "idle" },
			action: { action: "await_input" },
		});
		expect(finished.checkpoint.request).toBeUndefined();
	});

	test("a final idle closeout keeps pre-request committed input dormant until a later run opens", () => {
		const closed = initializeThreadTurnReduction(
			{
				pendingInputContextSequences: [1],
				idleCloseout: {
					eventId: "event_idle_interrupted",
					stopReason: "end_turn",
				},
			},
			noRoutes,
		);
		expect(closed).toMatchObject({
			state: { state: "idle" },
			action: { action: "await_input" },
		});

		const reopened = reduceThreadTurn(
			closed,
			{
				fact: "run_opened",
				eventId: "event_running_after_explicit_input",
			},
			noRoutes,
		);
		expect(reopened).toMatchObject({
			checkpoint: {
				executionRunId: "event_running_after_explicit_input",
				pendingInputContextSequences: [1],
			},
			state: { state: "ready_to_request" },
			action: { action: "prepare_next_request" },
		});
	});

	test("a later run discards the request closed by an idle interruption", () => {
		const interrupted = initializeThreadTurnReduction(
			{
				pendingInputContextSequences: [1],
				interruptEventId: "event_interrupt_idle",
				request: {
					modelRequestId: "model_request_interrupted_idle",
					requestStartEventId: "event_start_interrupted_idle",
					requestKind: "agent_provider_request",
					contextThroughMessageSequence: 0,
					requestEnd: {
						eventId: "event_end_interrupted_idle",
						isError: false,
						rescheduled: false,
					},
					toolMembers: [
						{
							memberKind: "public_tool_use",
							modelToolCallId: "tool_call_interrupted_idle",
							toolUseEventId: "event_tool_interrupted_idle",
							toolName: "Write",
						},
					],
				},
			},
			noRoutes,
		);
		expect(interrupted).toMatchObject({
			state: { state: "idle" },
			action: { action: "await_input" },
		});

		const reopened = reduceThreadTurn(
			interrupted,
			{
				fact: "run_opened",
				eventId: "event_running_after_idle_interrupt",
			},
			noRoutes,
		);
		expect(reopened).toMatchObject({
			checkpoint: {
				executionRunId: "event_running_after_idle_interrupt",
				pendingInputContextSequences: [1],
			},
			state: { state: "ready_to_request" },
			action: { action: "prepare_next_request" },
		});
		expect(reopened.checkpoint.request).toBeUndefined();
		expect(reopened.checkpoint.interruptEventId).toBeUndefined();
	});

	test("rejects end-turn closeout while a sealed tool continuation is ready", () => {
		const ready = initializeThreadTurnReduction(
			sealedCheckpoint([
				{
					memberKind: "public_tool_use",
					modelToolCallId: "call_completed",
					toolUseEventId: "event_tool_completed",
					toolName: "Read",
					terminalResult: { outcome: "success" },
				},
			]),
			noRoutes,
		);
		expect(ready).toMatchObject({
			state: { state: "ready_to_request" },
			action: { action: "prepare_next_request" },
		});
		expect(() =>
			reduceThreadTurn(
				ready,
				{
					fact: "finish_idle_committed",
					eventId: "event_idle_illegal",
					stopReason: { type: "end_turn" },
				},
				noRoutes,
			),
		).toThrow("cannot finish end_turn from ready_to_request");
	});

	test("treats internal invalid-tool repair as a terminal synthetic member", () => {
		const repaired = reduceThreadTurn(
			openRequest(),
			{
				fact: "internal_tool_repair_committed",
				eventId: "event_repair",
				modelRequestId: "request_1",
				modelToolCallId: "call_invalid",
				toolName: "missing_tool",
			},
			noRoutes,
		);
		expect(repaired).toMatchObject({
			state: { state: "request_open", modelRequestId: "request_1" },
			action: { action: "await_request_end", modelRequestId: "request_1" },
		});

		const sealed = reduceThreadTurn(
			repaired,
			{
				fact: "request_ended",
				eventId: "event_end",
				modelRequestId: "request_1",
				isError: false,
				rescheduled: false,
			},
			noRoutes,
		);
		expect(reconcileThreadTurnSeal(sealed, noRoutes)).toMatchObject({
			state: { state: "ready_to_request" },
			action: { action: "prepare_next_request" },
		});
	});

	test("emits a bounded cold resume action before converging to the stable wait", () => {
		const checkpoint = sealedCheckpoint([
			{
				memberKind: "public_tool_use",
				modelToolCallId: "call_1",
				toolUseEventId: "event_tool_1",
				toolName: "Bash",
			},
			{
				memberKind: "public_tool_use",
				modelToolCallId: "call_2",
				toolUseEventId: "event_tool_2",
				toolName: "Write",
			},
		]);
		const coldRoutes: ThreadToolRouteView = {
			routes: [
				{
					toolUseEventId: "event_tool_1",
					disposition: "resume_sandbox_execution",
				},
				{
					toolUseEventId: "event_tool_2",
					disposition: "resume_approval_settlement",
				},
			],
		};
		expect(deriveThreadTurnDecision(checkpoint, coldRoutes)).toEqual({
			state: { state: "waiting_for_tool_results", modelRequestId: "request_1" },
			action: {
				action: "resume_tool_routes",
				modelRequestId: "request_1",
				toolUseEventIds: ["event_tool_1", "event_tool_2"],
			},
		});
		expect(
			deriveThreadTurnDecision(checkpoint, {
				routes: [
					{ toolUseEventId: "event_tool_1", disposition: "hot_execution" },
					{ toolUseEventId: "event_tool_2", disposition: "hot_execution" },
				],
			}),
		).toEqual({
			state: { state: "waiting_for_tool_results", modelRequestId: "request_1" },
			action: {
				action: "await_tool_results",
				modelRequestId: "request_1",
				toolUseEventIds: ["event_tool_1", "event_tool_2"],
			},
		});
	});

	test("preserves sealed approval wait across FinishIdle requires_action", () => {
		const checkpoint = sealedCheckpoint([
			{
				memberKind: "public_tool_use",
				modelToolCallId: "call_1",
				toolUseEventId: "event_tool_1",
				toolName: "Write",
			},
		]);
		const routes: ThreadToolRouteView = {
			routes: [
				{ toolUseEventId: "event_tool_1", disposition: "requires_user_action" },
			],
		};
		const waiting = initializeThreadTurnReduction(checkpoint, routes);
		expect(waiting).toMatchObject({
			state: { state: "waiting_for_tool_results", modelRequestId: "request_1" },
			action: {
				action: "finish_idle",
				stopReason: { type: "requires_action", eventIds: ["event_tool_1"] },
			},
		});

		const idleReceipt = reduceThreadTurn(
			waiting,
			{
				fact: "finish_idle_committed",
				eventId: "event_idle",
				stopReason: { type: "requires_action", eventIds: ["event_tool_1"] },
			},
			routes,
		);
		expect(idleReceipt).toMatchObject({
			state: { state: "waiting_for_tool_results", modelRequestId: "request_1" },
			action: {
				action: "await_tool_results",
				toolUseEventIds: ["event_tool_1"],
			},
		});
		expect(idleReceipt.checkpoint.executionRunId).toBeUndefined();
	});

	test("does not finish requires_action while another sealed tool is still executing", () => {
		const checkpoint = sealedCheckpoint([
			{
				memberKind: "public_tool_use",
				modelToolCallId: "call_running",
				toolUseEventId: "event_tool_running",
				toolName: "Read",
			},
			{
				memberKind: "public_tool_use",
				modelToolCallId: "call_approval",
				toolUseEventId: "event_tool_approval",
				toolName: "Write",
			},
		]);
		const routes: ThreadToolRouteView = {
			routes: [
				{ toolUseEventId: "event_tool_running", disposition: "hot_execution" },
				{
					toolUseEventId: "event_tool_approval",
					disposition: "requires_user_action",
				},
			],
		};
		expect(deriveThreadTurnDecision(checkpoint, routes)).toEqual({
			state: { state: "waiting_for_tool_results", modelRequestId: "request_1" },
			action: {
				action: "await_tool_results",
				modelRequestId: "request_1",
				toolUseEventIds: ["event_tool_running", "event_tool_approval"],
			},
		});
		const waiting = initializeThreadTurnReduction(checkpoint, routes);
		expect(() =>
			reduceThreadTurn(
				waiting,
				{
					fact: "finish_idle_committed",
					eventId: "event_idle",
					stopReason: {
						type: "requires_action",
						eventIds: ["event_tool_approval"],
					},
				},
				routes,
			),
		).toThrow(
			"requires_action ACK does not match the current FinishIdle action",
		);
	});

	test("rejects a requires-action ACK whose event set differs from the declared closeout", () => {
		const checkpoint = sealedCheckpoint([
			{
				memberKind: "public_tool_use",
				modelToolCallId: "call_1",
				toolUseEventId: "event_tool_1",
				toolName: "Write",
			},
		]);
		const routes: ThreadToolRouteView = {
			routes: [
				{ toolUseEventId: "event_tool_1", disposition: "requires_user_action" },
			],
		};
		const waiting = initializeThreadTurnReduction(checkpoint, routes);
		expect(() =>
			reduceThreadTurn(
				waiting,
				{
					fact: "finish_idle_committed",
					eventId: "event_idle",
					stopReason: { type: "requires_action", eventIds: ["event_other"] },
				},
				routes,
			),
		).toThrow(
			"requires_action ACK event IDs do not match the declared closeout",
		);
	});

	test("records an interrupt-prioritized Tool Use ACK without dispatching its side effect", () => {
		const interrupted = reduceThreadTurn(
			openRequest(),
			{
				fact: "interrupt_committed",
				eventId: "event_interrupt",
			},
			noRoutes,
		);
		const lateToolUse = reduceThreadTurn(
			interrupted,
			{
				fact: "tool_use_committed",
				eventId: "event_tool_1",
				modelRequestId: "request_1",
				modelToolCallId: "call_1",
				toolName: "Bash",
			},
			noRoutes,
		);

		expect(lateToolUse.checkpoint.request?.toolMembers).toHaveLength(1);
		expect(lateToolUse).toMatchObject({
			state: { state: "request_open", modelRequestId: "request_1" },
			action: { action: "close_interrupted", modelRequestId: "request_1" },
		});
	});

	test("makes retries-exhausted FinishIdle hot reduction reconstructible", () => {
		const sealed = reduceThreadTurn(
			openRequest(),
			{
				fact: "request_ended",
				eventId: "event_end",
				modelRequestId: "request_1",
				isError: true,
				errorKind: "provider_stream_error",
				rescheduled: false,
			},
			noRoutes,
		);
		const finished = reduceThreadTurn(
			sealed,
			{
				fact: "finish_idle_committed",
				eventId: "event_idle",
				stopReason: {
					type: "retries_exhausted",
					failureEventId: "event_error",
				},
			},
			noRoutes,
		);

		expect(finished.checkpoint).toMatchObject({
			terminalCloseout: {
				failureEventId: "event_error",
				closeoutEventId: "event_idle",
				disposition: "retries_exhausted",
			},
		});
		expect(finished.checkpoint.executionRunId).toBeUndefined();
		expect(finished.checkpoint.request).toBeUndefined();
		expect(deriveThreadTurnDecision(finished.checkpoint, noRoutes)).toEqual({
			state: finished.state,
			action: finished.action,
		});
	});

	test("routes compaction, reviewer, retry, interrupt, and terminal closeout before ordinary continuation", () => {
		expect(
			deriveThreadTurnDecision(
				sealedCheckpoint([], "compaction_summary"),
				noRoutes,
			),
		).toMatchObject({
			state: { state: "ready_to_request" },
			action: {
				action: "continue_after_compaction",
				modelRequestId: "request_1",
			},
		});
		expect(
			deriveThreadTurnDecision(
				sealedCheckpoint([], "approval_reviewer"),
				noRoutes,
			),
		).toMatchObject({
			state: { state: "request_sealed", modelRequestId: "request_1" },
			action: { action: "complete_reviewer", modelRequestId: "request_1" },
		});
		for (const requestKind of [
			"compaction_summary",
			"approval_reviewer",
		] as const) {
			expect(
				deriveThreadTurnDecision(
					sealedCheckpoint([], requestKind, {
						isError: true,
						rescheduled: false,
						errorKind: "provider_stream_error",
					}),
					noRoutes,
				),
			).toMatchObject({
				state: { state: "request_sealed", modelRequestId: "request_1" },
				action: {
					action: "apply_request_retry_or_reschedule",
					modelRequestId: "request_1",
				},
			});
		}
		expect(
			deriveThreadTurnDecision(
				sealedCheckpoint([], "agent_provider_request", {
					isError: true,
					rescheduled: true,
					errorKind: "provider_unavailable",
				}),
				noRoutes,
			),
		).toMatchObject({
			action: {
				action: "apply_request_retry_or_reschedule",
				modelRequestId: "request_1",
			},
		});
		expect(
			deriveThreadTurnDecision(
				{
					...sealedCheckpoint([]),
					interruptEventId: "event_interrupt",
				},
				noRoutes,
			),
		).toEqual({
			state: { state: "request_sealed", modelRequestId: "request_1" },
			action: { action: "close_interrupted", modelRequestId: "request_1" },
		});
		expect(
			deriveThreadTurnDecision(
				{
					...sealedCheckpoint([]),
					terminalCloseout: {
						failureEventId: "event_error",
						closeoutEventId: "event_terminated",
						disposition: "terminated",
					},
				},
				noRoutes,
			),
		).toEqual({
			state: { state: "idle" },
			action: { action: "await_input" },
		});
	});

	test("approval reviewer settles terminal members and later input before completing the review", () => {
		const terminalPublicMember: NonNullable<
			ThreadTurnCheckpoint["request"]
		>["toolMembers"][number] = {
			memberKind: "public_tool_use",
			modelToolCallId: "call_public",
			toolUseEventId: "event_tool_public",
			toolName: "Read",
			terminalResult: { outcome: "success" },
		};
		const terminalInternalRepair: NonNullable<
			ThreadTurnCheckpoint["request"]
		>["toolMembers"][number] = {
			memberKind: "internal_tool_repair",
			modelToolCallId: "call_repair",
			toolName: "MissingTool",
			repairEventId: "event_repair",
			outcome: "error",
		};
		for (const checkpoint of [
			sealedCheckpoint([terminalPublicMember], "approval_reviewer"),
			sealedCheckpoint([terminalInternalRepair], "approval_reviewer"),
			{
				...sealedCheckpoint([], "approval_reviewer"),
				pendingInputContextSequences: [1],
			},
		]) {
			expect(deriveThreadTurnDecision(checkpoint, noRoutes)).toEqual({
				state: { state: "ready_to_request" },
				action: { action: "prepare_next_request" },
			});
		}
		expect(
			deriveThreadTurnDecision(
				sealedCheckpoint([], "approval_reviewer"),
				noRoutes,
			),
		).toEqual({
			state: { state: "request_sealed", modelRequestId: "request_1" },
			action: { action: "complete_reviewer", modelRequestId: "request_1" },
		});
	});

	test("fails closed for orphan results, post-seal Tool Uses, and missing sealed routes", () => {
		expect(() =>
			reduceThreadTurn(
				openRequest(),
				{
					fact: "tool_result_committed",
					toolUseEventId: "event_missing",
					outcome: "success",
				},
				noRoutes,
			),
		).toThrow(ThreadTurnContractError);

		const sealed = initializeThreadTurnReduction(
			sealedCheckpoint([]),
			noRoutes,
		);
		expect(() =>
			reduceThreadTurn(
				sealed,
				{
					fact: "tool_use_committed",
					eventId: "event_tool",
					modelRequestId: "request_1",
					modelToolCallId: "call_1",
					toolName: "Read",
				},
				noRoutes,
			),
		).toThrow("cannot append Tool Use after Request End");

		expect(() =>
			deriveThreadTurnDecision(
				sealedCheckpoint([
					{
						memberKind: "public_tool_use",
						modelToolCallId: "call_1",
						toolUseEventId: "event_tool_1",
						toolName: "Read",
					},
				]),
				noRoutes,
			),
		).toThrow("sealed non-terminal Tool Use has no route");
	});
});

function openRequest() {
	return initializeThreadTurnReduction(
		{
			executionRunId: "run_1",
			pendingInputContextSequences: [],
			request: {
				modelRequestId: "request_1",
				requestStartEventId: "event_start",
				requestKind: "agent_provider_request",
				contextThroughMessageSequence: 1,
				toolMembers: [],
			},
		},
		noRoutes,
	);
}

function terminalToolRequest() {
	const withTool = reduceThreadTurn(
		openRequest(),
		{
			fact: "tool_use_committed",
			eventId: "event_tool_1",
			modelRequestId: "request_1",
			modelToolCallId: "call_1",
			toolName: "Read",
		},
		noRoutes,
	);
	return reduceThreadTurn(
		withTool,
		{
			fact: "tool_result_committed",
			toolUseEventId: "event_tool_1",
			outcome: "success",
		},
		noRoutes,
	);
}

function sealRequestWithTools(tools: readonly (readonly [string, string])[]) {
	let reduction = openRequest();
	for (const [toolUseEventId, modelToolCallId] of tools) {
		reduction = reduceThreadTurn(
			reduction,
			{
				fact: "tool_use_committed",
				eventId: toolUseEventId,
				modelRequestId: "request_1",
				modelToolCallId,
				toolName: "Read",
			},
			noRoutes,
		);
	}
	return reduceThreadTurn(
		reduction,
		{
			fact: "request_ended",
			eventId: "event_end",
			modelRequestId: "request_1",
			isError: false,
			rescheduled: false,
		},
		noRoutes,
	);
}

function sealedCheckpoint(
	toolMembers: NonNullable<ThreadTurnCheckpoint["request"]>["toolMembers"],
	requestKind: NonNullable<
		ThreadTurnCheckpoint["request"]
	>["requestKind"] = "agent_provider_request",
	requestEnd: {
		readonly isError: boolean;
		readonly rescheduled: boolean;
		readonly errorKind?: string;
	} = {
		isError: false,
		rescheduled: false,
	},
): ThreadTurnCheckpoint {
	return {
		executionRunId: "run_1",
		pendingInputContextSequences: [],
		request: {
			modelRequestId: "request_1",
			requestStartEventId: "event_start",
			requestKind,
			contextThroughMessageSequence: 1,
			requestEnd: {
				eventId: "event_end",
				...requestEnd,
			},
			toolMembers,
		},
	};
}

function routesForOutstanding(
	routes: ThreadToolRouteView,
	settledToolUseEventId: string,
): ThreadToolRouteView {
	return {
		routes: routes.routes.filter(
			(route) => route.toolUseEventId !== settledToolUseEventId,
		),
	};
}
