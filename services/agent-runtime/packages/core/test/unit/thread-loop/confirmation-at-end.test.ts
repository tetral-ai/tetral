import { expect, test } from "bun:test";
import { Effect } from "effect";
import type { SessionEvent, SessionEventWriter, SessionEventWriterToolSettlementEnvelope } from "../../../src/contracts/runtime.js";
import type { LLMRequest } from "../../../src/llm/llm-service.js";
import * as ThreadLoop from "../../../src/thread-loop/thread-loop.js";
import { ThreadRuntime } from "../../../src/thread-loop/thread-runtime.js";
import {
	QueuedContextLoader,
	catalogForTest,
	runtimeThreadLoopLayer,
	testRunCustody,
	userMessage,
	waitForCondition,
	writerFrom,
} from "./thread-loop-test-support.js";

type MutableWriter = { -readonly [K in keyof SessionEventWriter]: SessionEventWriter[K] };

function harness(toolCount = 1) {
	const session = new ThreadRuntime("sesn_confirmation_at_end");
	const events: SessionEvent[] = [];
	const requests: LLMRequest[] = [];
	const executions: string[] = [];
	const settlements: SessionEventWriterToolSettlementEnvelope[] = [];
	const idleTurns: Parameters<NonNullable<SessionEventWriter["finishIdle"]>>[0][] = [];
	const confirmedTools = new Map<string, string>();
	const baseWriter: MutableWriter = writerFrom(envelope => {
		events.push(envelope.event);
		return { ok: true, type: "committed", eventId: `bridge-${envelope.writeId}` };
	}, undefined, [], undefined, async envelope => {
		settlements.push(envelope);
		return { ok: true, result: { type: "committed" } };
	});
	const finishIdle = baseWriter.finishIdle!;
	baseWriter.finishIdle = async envelope => {
		idleTurns.push(envelope);
		return finishIdle(envelope);
	};
	const writer: MutableWriter = { ...baseWriter };
	const layer = runtimeThreadLoopLayer(new QueuedContextLoader([], [
		{ type: "context", entries: [userMessage("user", 0, "write files")] },
	]), {
		writer,
		events: [
			...Array.from({ length: toolCount }, (_, index) => ({
				type: "tool-call-complete" as const,
				id: `write-${index}`,
				toolName: "Write",
				input: { file_path: `file-${index}.txt`, content: "content" },
				inputPreview: { preview: "{}", truncated: false },
			})),
			{ type: "finish", finishReason: "tool-calls" },
		],
		onStream: request => requests.push(request),
		providerCallRuntime: {
			systemInstructions: "confirmation at End",
			toolCatalog: catalogForTest({ name: "Write", description: "Write file", inputSchema: { type: "object" }, permissionPolicy: "always_ask" }),
		},
		runTool: request => {
			executions.push(request.modelToolCallId);
			return { type: "completed", output: { text: `result-${request.modelToolCallId}`, truncated: false } };
		},
	});
	const run = () => Effect.runPromise(Effect.gen(function* () {
		return yield* (yield* ThreadLoop.Service).run(session, testRunCustody());
	}).pipe(Effect.provide(layer)));
	const confirm = async (modelToolCallId: string, decision: "allow" | "deny") => {
		await waitForCondition(() => session.state.pendingApprovalToolJobs().some(job => job.job.modelToolCallId === modelToolCallId), "pending approval control");
		const pending = session.state.pendingApprovalToolJobs().find(job => job.job.modelToolCallId === modelToolCallId)!;
		confirmedTools.set(modelToolCallId, pending.toolUseEventId);
		const result = await Effect.runPromise(ThreadLoop.settleToolConfirmation(session, {
			...session.identity,
			runtimeInputId: `rin_${modelToolCallId}_${decision}`,
			toolUseEventId: pending.toolUseEventId,
			decision,
		}, async declaration => {
			expect(declaration).toEqual({ inputKind: "tool_confirmation" });
			return {
				ok: true,
				type: "committed",
				assignedContextSequences: [Math.max(0, ...session.state.contextManager.messages().map(message => message.messageSequence)) + 1],
				pendingAttachments: [],
				interruptToolResults: [],
			};
		}));
		expect(result).toMatchObject({ type: "applied", wakeThread: true });
	};
	return { session, events, requests, executions, settlements, idleTurns, confirmedTools, baseWriter, writer, run, confirm };
}

for (const decision of ["allow", "deny"] as const) {
	for (const boundary of ["before End", "during FinishIdle ACK"] as const) {
		test(`${decision} confirmation ${boundary} resumes the named Tool in the current run`, async () => {
			const h = harness();
			let confirmationApplied = false;
			if (boundary === "before End") {
				h.writer.writeRequestEnd = async envelope => {
					if (!confirmationApplied) {
						await h.confirm("write-0", decision);
						confirmationApplied = true;
					}
					return h.baseWriter.writeRequestEnd(envelope);
				};
			} else {
				h.writer.finishIdle = async envelope => {
					if (envelope.stopReason.type === "requires_action") {
						expect(confirmationApplied).toBe(false);
						await h.confirm("write-0", decision);
						confirmationApplied = true;
					}
					return h.baseWriter.finishIdle!(envelope);
				};
			}
			expect(await h.run()).toMatchObject({ type: "completed" });
			expect(confirmationApplied).toBe(true);
			expect(h.executions).toEqual(decision === "allow" ? ["write-0"] : []);
			expect(h.settlements).toHaveLength(1);
			expect(h.settlements[0]!.settlement.toolUseEventId).toBe(h.confirmedTools.get("write-0")!);
			expect(h.session.state.contextManager.historyMessages().flatMap(message => message.parts).filter(part => part.type === "tool_result")).toEqual([expect.objectContaining({
				type: "tool_result", modelToolCallId: "write-0", result: expect.objectContaining({ type: decision === "allow" ? "completed" : "error" }),
			})]);
			expect(h.requests).toHaveLength(2);
			expect(h.events.filter(event => event.type === "span.model_request_end")).toHaveLength(2);
			expect(h.events.filter(event => event.type === "session.status_idle" && event.stop_reason.type === "requires_action")).toHaveLength(boundary === "before End" ? 0 : 1);
			if (boundary === "during FinishIdle ACK") {
				expect(h.idleTurns).toHaveLength(2);
				expect(h.idleTurns[1]!.durableTurnId).not.toBe(h.idleTurns[0]!.durableTurnId);
			}
			expect(h.session.state.activeTools()).toEqual([]);
			expect(h.session.state.threadTurnTransition().nextStep).toEqual({ action: "await_input" });
		});
	}
}

test("an undecided approval remains passive and starts no successor request", async () => {
	const h = harness();
	expect(await h.run()).toMatchObject({ type: "completed" });
	expect(h.requests).toHaveLength(1);
	expect(h.executions).toEqual([]);
	expect(h.settlements).toEqual([]);
	expect(h.session.state.threadTurnTransition().nextStep.action).toBe("await_tool_results");
	expect(h.events.at(-1)).toMatchObject({ type: "session.status_idle", stop_reason: { type: "requires_action" } });
});

test("a sibling confirmation during recovered-route FinishIdle resumes without another wake", async () => {
	const h = harness(2);
	expect(await h.run()).toMatchObject({ type: "completed" });
	await h.confirm("write-0", "allow");
	h.writer.finishIdle = async envelope => {
		if (envelope.stopReason.type === "requires_action") {
			expect(h.executions).toEqual(["write-0"]);
			await h.confirm("write-1", "deny");
		}
		return h.baseWriter.finishIdle!(envelope);
	};
	expect(await h.run()).toMatchObject({ type: "completed" });
	expect(h.requests).toHaveLength(2);
	expect(h.executions).toEqual(["write-0"]);
	expect(h.settlements).toHaveLength(2);
	expect(h.session.state.activeTools()).toEqual([]);
	expect(h.session.state.threadTurnTransition().nextStep).toEqual({ action: "await_input" });
});


test("a resolved route without a hot or cold decision fails closed", async () => {
	const h = harness();
	expect(await h.run()).toMatchObject({ type: "completed" });
	const pending = h.session.state.pendingApprovalToolJobs()[0]!;
	h.session.state.recordThreadToolRoute(pending.toolUseEventId, "resume_approval_settlement");
	expect(() => h.session.state.resolvedToolRouteJobs()).toThrow("resolved Tool route has no approval decision");
	expect(h.executions).toEqual([]);
	expect(h.settlements).toEqual([]);
});

test("checkpoint admission fence wins over confirmation received during FinishIdle ACK", async () => {
	const h = harness();
	h.writer.finishIdle = async envelope => {
		await h.confirm("write-0", "allow");
		h.session.state.beginRuntimeQuiesce();
		return h.baseWriter.finishIdle!(envelope);
	};
	expect(await h.run()).toMatchObject({ type: "checkpoint_yield" });
	expect(h.requests).toHaveLength(1);
	expect(h.executions).toEqual([]);
	expect(h.settlements).toEqual([]);
	expect(h.session.state.threadTurnTransition().nextStep.action).toBe("resume_tool_routes");
});
