import { expect, test } from "bun:test";
import { ProviderRequestKind } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { Effect } from "effect";
import type { SessionEventEnvelope, SessionEventWriter, SessionEventWriterRequestEndEnvelope } from "../../../src/contracts/runtime.js";
import type { LLMEvent } from "../../../src/llm/llm-event.js";
import type { LLMRequest } from "../../../src/llm/llm-service.js";
import * as ThreadLoop from "../../../src/thread-loop/thread-loop.js";
import { ThreadRuntime } from "../../../src/thread-loop/thread-runtime.js";
import { approvalReviewAcceptedInput, compactionHistory, queuedLLMService, QueuedContextLoader, RecordingContextLoader, recordCompactionHint, runtimeThreadLoopLayer, testRunCustody, userMessage, writerFrom } from "./thread-loop-test-support.js";

for (const consumer of ["normal", "reviewer", "compaction"] as const) {
	for (const reasoning of [false, true]) {
		test(`${consumer} ${reasoning ? "reasoning-only" : "Finish-only"} preserves its End/content contract`, async () => {
			const review = approvalReviewAcceptedInput();
			const session = consumer === "reviewer" ? new ThreadRuntime({
				...review, threadRole: "approval_reviewer", threadVisibility: "internal", runtimeBindingToken: "binding-token",
			}) : new ThreadRuntime(`sesn_empty_${consumer}`);
			if (consumer === "reviewer") session.state.enqueueAcceptedInput(review);
			if (consumer === "compaction") recordCompactionHint(session, { inputTokens: 1, outputTokens: 1, reasoningTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0 });
			const requests: LLMRequest[] = [];
			const appended: SessionEventEnvelope[] = [];
			const ends: SessionEventWriterRequestEndEnvelope[] = [];
			const baseWriter = writerFrom(envelope => {
				appended.push(envelope);
				return { ok: true, type: "committed", eventId: `bridge-${envelope.writeId}`, eventSequence: 1 };
			});
			const writer: SessionEventWriter = { ...baseWriter, writeRequestEnd: async envelope => {
				ends.push(envelope);
				return baseWriter.writeRequestEnd(envelope);
			} };
			const events: LLMEvent[] = reasoning ? [
				{ type: "thinking-started", providerPartId: "reason", eventId: "evt_11111111111111111111111111111111" },
				{ type: "reasoning-complete", providerPartId: "reason", thinkingEventId: "evt_11111111111111111111111111111111", text: "reasoning-only", providerMetadata: { anthropic: { signature: "fixture-signature" } } },
				{ type: "finish", finishReason: "stop" },
			] : [{ type: "finish", finishReason: "stop" }];
			const loader = consumer === "reviewer" ? new QueuedContextLoader([], []) : new RecordingContextLoader(
				consumer === "compaction" ? [userMessage("old", 1, compactionHistory("summarize old context"))] : [],
				{ type: "context", entries: [userMessage("user", 2, "continue")] },
			);
			const result = await Effect.runPromise(Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(session, testRunCustody());
			}).pipe(Effect.provide(runtimeThreadLoopLayer(loader, {
				writer, llmService: queuedLLMService([events], requests), ...(consumer === "compaction" ? { compaction: {} } : {}),
			}))));
			expect(requests).toHaveLength(1);
			expect(requests[0]!.requestKind).toBe(consumer === "normal" ? ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST : consumer === "reviewer" ? ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER : ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY);
			expect(appended.filter(envelope => envelope.event.type === "agent.message")).toEqual([]);
			expect(ends).toHaveLength(1);
			if (reasoning && consumer !== "compaction") {
				expect(result.type).toBe("completed");
				expect(ends[0]).toMatchObject({ isError: false, trailingContextAppend: { parts: [
					{ type: "reasoning", text: "reasoning-only", truncated: false, providerMetadata: { anthropic: { signature: "fixture-signature" } } },
				] } });
			} else {
				expect(result.type).toBe("failed");
				expect(ends[0]!.isError).toBe(true);
				expect(ends[0]!.trailingContextAppend).toBeUndefined();
				expect(ends[0]!.compactionContext).toBeUndefined();
			}
			expect(session.state.activeTools()).toEqual([]);
		});
	}
}
