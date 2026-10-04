import { expect, test } from "bun:test";
import { Effect } from "effect";
import type { SessionEventWriter } from "../../../src/contracts/runtime.js";
import * as ThreadLoop from "../../../src/thread-loop/thread-loop.js";
import { ThreadRuntime } from "../../../src/thread-loop/thread-runtime.js";
import { RuntimePodMetricsRegistry } from "../../../../runtime-pod/src/metrics.js";
import { RecordingContextLoader, runtimeThreadLoopLayer, testRunCustody, userMessage, writerFrom } from "./thread-loop-test-support.js";

// Change only the successful response after the normal fixture has constructed
// its receipt. Its supplied-ID preservation must not repair our malformed ACK.
for (const receipt of ["committed", "duplicate"] as const) {
	for (const wrong of [false, true]) {
		test(`actual Thinking ${receipt} ACK ${wrong ? "rejects the wrong identity" : "permits dependent members"}`, async () => {
			const session = new ThreadRuntime(`sesn_thinking_ack_${receipt}_${wrong}`);
			const registry = new RuntimePodMetricsRegistry();
			const appended: string[] = [];
			const samples: Parameters<NonNullable<ThreadLoop.ThreadLoopRuntimeOptions["recordContentCommit"]>>[0][] = [];
			let requests = 0;
			let executions = 0;
			const base = writerFrom(envelope => ({ ok: true, type: "committed", eventId: envelope.preallocatedEventId ?? `bridge-${envelope.writeId}` }));
			const writer: SessionEventWriter = {
				...base,
				append: async envelope => {
					appended.push(envelope.event.type);
					const result = await base.append(envelope);
					if (envelope.event.type !== "agent.thinking" || !result.ok || result.type === "stale") return result;
					return { ...result, type: receipt, eventId: wrong ? "evt_99999999999999999999999999999999" : result.eventId };
				},
			};
			const result = await Effect.runPromise(Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(session, testRunCustody());
			}).pipe(Effect.provide(runtimeThreadLoopLayer(new RecordingContextLoader([], {
				type: "context", entries: [userMessage("user", 0, "think then read")],
			}), {
				writer, metrics: registry, recordContentCommit: sample => samples.push(sample),
				onStream: () => { requests++; },
				runTool: () => { executions++; return { type: "completed", output: { text: "fixture bytes", truncated: false } }; },
				events: [
					{ type: "thinking-started", providerPartId: "reason", eventId: "evt_11111111111111111111111111111111" },
					{ type: "reasoning-complete", providerPartId: "reason", thinkingEventId: "evt_11111111111111111111111111111111", text: "signed reason", providerMetadata: { anthropic: { signature: "sig" } } },
					{ type: "text-complete", providerPartId: "text", eventId: "evt_22222222222222222222222222222222", text: "answer" },
					{ type: "tool-call-complete", id: "read", toolName: "Read", input: { file_path: "/workspace/note.txt" }, inputPreview: { preview: "{}", truncated: false } },
					{ type: "finish", finishReason: "tool-calls" },
				],
			}))));
			expect(appended.filter(kind => kind === "agent.thinking")).toHaveLength(1);
			expect(samples.filter(sample => sample.kind === "thinking").map(({ phase, outcome }) => ({ phase, outcome }))).toEqual([
				{ phase: "content_commit", outcome: receipt },
				{ phase: "content_apply", outcome: wrong ? "failed" : receipt },
			]);
			if (wrong) {
				expect(result.type).toBe("failed");
				expect(requests).toBe(1);
				expect(executions).toBe(0);
				expect(appended.filter(kind => kind === "agent.message" || kind === "agent.tool_use")).toEqual([]);
				expect(session.state.persistentContextLoaded()).toBe(false);
			} else {
				expect(result.type).toBe("completed");
				expect(requests).toBe(2);
				expect(executions).toBe(1);
				expect(appended.filter(kind => kind === "agent.tool_use")).toHaveLength(1);
			}
			expect(session.state.activeTools()).toEqual([]);
			expect(registry.snapshot().pendingContentEntries).toBe(0);
			expect(registry.snapshot().pendingContentBytes).toBe(0);
			expect(registry.snapshot().activeToolFibers).toBe(0);
		});
	}
}
