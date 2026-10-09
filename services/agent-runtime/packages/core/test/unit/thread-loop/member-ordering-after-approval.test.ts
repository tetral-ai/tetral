import { expect, test } from "bun:test";
import { Effect } from "effect";
import type { RuntimeContextPart, SessionEventEnvelope, SessionEventWriter } from "../../../src/contracts/runtime.js";
import { RequestContentProcessor } from "../../../src/runtime/accumulator.js";
import { AutoApprovalReviewerManager } from "../../../src/session/approval-reviewer-manager.js";
import * as ThreadLoop from "../../../src/thread-loop/thread-loop.js";
import { RuntimePodMetricsRegistry } from "../../../../runtime-pod/src/metrics.js";
import { ThreadRuntime } from "../../../src/thread-loop/thread-runtime.js";
import { deferred, RecordingContextLoader, runtimeThreadLoopLayer, testRunCustody, userMessage, writerFrom } from "./thread-loop-test-support.js";

for (const decision of ["allow", "deny"] as const) {
	test(`held reviewer ${decision} preserves reasoning/Tool/text/Tool member order`, async () => {
		const reviewerEntered = deferred<void>();
		const releaseReviewer = deferred<void>();
		const laterTextEntered = deferred<void>();
		const appends: SessionEventEnvelope[] = [];
		const metrics = new RuntimePodMetricsRegistry();
		let requestParts: readonly RuntimeContextPart[] | undefined;
		const executions: string[] = [];
		const session = new ThreadRuntime(`sesn_member_${decision}`, new AutoApprovalReviewerManager());
		const baseWriter = writerFrom(envelope => {
			appends.push(envelope);
			return { ok: true, type: "committed", eventId: `bridge-${envelope.writeId}`, eventSequence: 1 };
		});
		const writer: SessionEventWriter = {
			...baseWriter,
			writeRequestEnd: async envelope => {
				requestParts ??= structuredClone(session.state.contextManager.currentAssistantMessage()?.parts ?? []);
				return baseWriter.writeRequestEnd(envelope);
			},
		};
		const run = Effect.runPromise(Effect.gen(function* () {
			return yield* (yield* ThreadLoop.Service).run(session, testRunCustody());
		}).pipe(Effect.provide(runtimeThreadLoopLayer(new RecordingContextLoader([], {
			type: "context", entries: [userMessage("user", 0, "write then read")],
		}), {
			writer, metrics, approvalMode: "approve_for_me",
			createProcessor: options => {
				const processor = new RequestContentProcessor(options);
				const process = processor.process.bind(processor);
				processor.process = async (envelope, signal) => {
					if (envelope.event.type === "text-complete" && envelope.event.text === "beta") laterTextEntered.resolve();
					return process(envelope, signal);
				};
				return processor;
			},
			events: [
				{ type: "thinking-started", providerPartId: "reason", eventId: "evt_11111111111111111111111111111111" },
				{ type: "reasoning-complete", providerPartId: "reason", thinkingEventId: "evt_11111111111111111111111111111111", text: "reason-before-tool", providerMetadata: { anthropic: { signature: "fixture-signature" } } },
				{ type: "tool-call-complete", id: "write", toolName: "Write", input: { file_path: "/workspace/a.txt", content: "first\n" }, inputPreview: { preview: "{}", truncated: false } },
				{ type: "text-complete", providerPartId: "beta", eventId: "evt_22222222222222222222222222222222", text: "beta" },
				{ type: "tool-call-complete", id: "read", toolName: "Read", input: { file_path: "/workspace/b.txt" }, inputPreview: { preview: "{}", truncated: false } },
				{ type: "finish", finishReason: "tool-calls" },
			],
			reviewApproval: () => Effect.promise(async () => {
				reviewerEntered.resolve();
				await releaseReviewer.promise;
				return { type: "decision" as const, riskLevel: "low" as const, userAuthorization: "high" as const, outcome: decision };
			}),
			runTool: request => {
				executions.push(request.modelToolCallId);
				return { type: "completed", output: { text: "fixture-note", truncated: false } };
			},
		}))));
		const beforeCompletion = (barrier: Promise<void>, name: string) => Promise.race([
			barrier, run.then(() => { throw new Error(`run completed before ${name}`); }),
		]);
		try {
			await beforeCompletion(reviewerEntered.promise, "reviewer hold");
			await beforeCompletion(laterTextEntered.promise, "later text consumer");
			expect(appends.filter(envelope => envelope.event.type === "agent.tool_use" || envelope.event.type === "agent.message")).toEqual([]);
			expect(session.state.contextManager.currentAssistantMessage()).toBeUndefined();
			expect(metrics.snapshot().activeToolFibers).toBe(1);
			releaseReviewer.resolve();
			expect((await run).type).toBe("completed");
			expect(appends.filter(envelope => envelope.event.type === "agent.tool_use" || envelope.event.type === "agent.message").slice(0, 3).map(envelope => envelope.event.type)).toEqual(["agent.tool_use", "agent.message", "agent.tool_use"]);
			expect(requestParts?.filter(part => part.type !== "tool_result")).toEqual([
				{ type: "reasoning", text: "reason-before-tool", providerMetadata: { anthropic: { signature: "fixture-signature" } } },
				{ type: "tool_call", modelToolCallId: "write", toolName: "Write", canonicalInput: { file_path: "/workspace/a.txt", content: "first\n" } },
				{ type: "text", text: "beta" },
				{ type: "tool_call", modelToolCallId: "read", toolName: "Read", canonicalInput: { file_path: "/workspace/b.txt" } },
			]);
			expect(executions.sort()).toEqual(decision === "allow" ? ["read", "write"] : ["read"]);
			expect(session.state.activeTools()).toEqual([]);
			expect(metrics.snapshot().activeToolFibers).toBe(0);
		} finally {
			releaseReviewer.resolve();
			await run;
		}
	});
}
