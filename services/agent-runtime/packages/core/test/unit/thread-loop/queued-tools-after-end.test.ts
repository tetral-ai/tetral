import { expect, test } from "bun:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { Effect, Stream } from "effect";
import { normalizeRuntimeFailure } from "../../../src/contracts/runtime.js";
import type { LLMEvent } from "../../../src/llm/llm-event.js";
import { RuntimeFailureSchema } from "../../../src/llm/llm-event.js";
import * as ThreadLoop from "../../../src/thread-loop/thread-loop.js";
import { RuntimePodMetricsRegistry } from "../../../../runtime-pod/src/metrics.js";
import { ThreadRuntime } from "../../../src/thread-loop/thread-runtime.js";
import { createToolCatalog } from "../../../src/tools/tool-catalog.js";
import {
	deferred,
	RecordingContextLoader,
	runtimeThreadLoopLayer,
	testRunCustody,
	userMessage,
	writerFrom,
} from "./thread-loop-test-support.js";

// Real ThreadLoop, declaration sequencer and session permit coordinator. Only
// Bridge receipts and the external Sandbox adapter are supplied by this fixture.
// The adapter performs actual local file writes through the captured call input.
for (const delayed of [true, false]) {
	test(`same-path Write waits ${delayed ? "across End" : "before End control"}`, async () => {
		const directory = await mkdtemp(path.join(tmpdir(), "tetral-queued-write-"));
		const filename = path.join(directory, "note.txt");
		const session = new ThreadRuntime("sesn_queued_write");
		const metrics = new RuntimePodMetricsRegistry();
		const firstAccepted = deferred<void>();
		const releaseFirst = deferred<void>();
		const bothSettled = deferred<void>();
		const firstEndApplied = deferred<void>();
		const order: string[] = [];
		const accepted: string[] = [];
		const executed: string[] = [];
		let active = 0;
		let maxActive = 0;
		let requests = 0;
		let settlementCount = 0;
		let initialAssistantSequence: number | undefined;
		const applyFact = session.state.applyThreadTurnFact.bind(session.state);
		session.state.applyThreadTurnFact = (fact) => {
			const result = applyFact(fact);
			if (fact.fact === "request_ended" && requests === 1) {
				order.push("end-applied");
				firstEndApplied.resolve();
			}
			return result;
		};
		const writer = writerFrom(
			(envelope) => {
				order.push(`ack:${envelope.event.type}`);
				return { ok: true, type: "committed", eventId: `bridge-${envelope.writeId}`, eventSequence: 1 };
			},
			undefined,
			[],
			undefined,
			async () => {
				settlementCount += 1;
				order.push(`settlement:${settlementCount}`);
				if (settlementCount === 2) bothSettled.resolve();
				return { ok: true, result: { type: "committed" } };
			},
		);
		const catalog = createToolCatalog({ family: "claude", configs: [{ name: "Write", enabled: true, permissionPolicy: "always_allow" }] });
		const loader = new RecordingContextLoader([], { type: "context", entries: [userMessage("user", 0, "write twice")] });
		const run = Effect.runPromise(Effect.gen(function* () {
			const loop = yield* ThreadLoop.Service;
			return yield* loop.run(session, testRunCustody());
		}).pipe(Effect.provide(runtimeThreadLoopLayer(loader, {
			writer, metrics,
			providerCallRuntime: { systemInstructions: "queued writes", toolCatalog: catalog },
			llmService: {
				stream() {
					requests += 1;
					return Stream.fromAsyncIterable((async function* (): AsyncGenerator<LLMEvent> {
						if (requests > 1) {
							yield { type: "text-complete", providerPartId: "done", eventId: "evt_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", text: "done" };
							yield { type: "finish", finishReason: "stop" };
							return;
						}
						for (const [id, file_path, content] of [
							["write-first", "note.txt", "first\n"],
							["write-second", "/workspace/note.txt", "second\n"],
						] as const) {
							const input = { file_path, content };
							yield { type: "tool-call-complete", id, toolName: "Write", input, inputPreview: { preview: JSON.stringify(input), truncated: false } };
						}
						await firstAccepted.promise;
						if (!delayed) await bothSettled.promise;
						yield { type: "finish", finishReason: "tool-calls" };
					})(), (error) => ({ type: "llm-service", error: RuntimeFailureSchema.parse(normalizeRuntimeFailure({ type: "runtime", code: "runtime_invalid_sequence", retryable: false, fatal: true, rawError: error })) }));
				},
			},
			acceptSandboxExecution: async (request) => {
				accepted.push(request.modelToolCallId);
				order.push(`accepted:${request.modelToolCallId}`);
				active += 1;
				maxActive = Math.max(maxActive, active);
				expect(request.entry.route).toMatchObject({ kind: "sandbox", operation: "RunTool", helperSubcommand: "write" });
				const input = request.input as { file_path: string; content: string };
				expect(path.posix.resolve("/workspace", input.file_path)).toBe("/workspace/note.txt");
				await writeFile(filename, input.content);
				if (request.modelToolCallId === "write-first") {
					initialAssistantSequence = session.state.currentRequestMessage()?.assistantMessageSequence;
					firstAccepted.resolve();
				}
				return { type: "accepted" };
			},
			awaitSandboxExecution: async (request) => {
				executed.push(request.modelToolCallId);
				if (request.modelToolCallId === "write-first") await releaseFirst.promise;
				order.push(`completed:${request.modelToolCallId}`);
				active -= 1;
				return { type: "completed", output: { text: `wrote ${request.modelToolCallId}`, truncated: false } };
			},
		}))));
		const beforeCompletion = (marker: Promise<void>, boundary: string) => Promise.race([
			marker,
			run.then(() => { throw new Error(`run completed before ${boundary}`); }),
		]);
		try {
			await beforeCompletion(firstAccepted.promise, "first Sandbox acceptance");
			if (delayed) {
				await beforeCompletion(firstEndApplied.promise, "first End application");
				expect(session.state.currentRequestMessage()?.assistantMessageSequence).toBe(initialAssistantSequence);
				expect(session.state.activeTools()).toHaveLength(2);
				expect(metrics.snapshot().activeToolFibers).toBe(2);
				expect(session.state.contextManager.entry(initialAssistantSequence!)?.parts.filter(part => part.type === "tool_call").map(part => part.modelToolCallId)).toEqual(["write-first", "write-second"]);
			}
			expect(await readFile(filename, "utf8")).toBe("first\n");
			expect(accepted).toEqual(["write-first"]);
			expect(requests).toBe(1);
			releaseFirst.resolve();
			expect(await run).toMatchObject({ type: "completed" });
			expect(accepted).toEqual(["write-first", "write-second"]);
			expect(executed).toEqual(["write-first", "write-second"]);
			expect(maxActive).toBe(1);
			expect(active).toBe(0);
			expect(settlementCount).toBe(2);
			expect(requests).toBe(2);
			expect(await readFile(filename, "utf8")).toBe("second\n");
			const firstAssistant = session.state.contextManager.entry(initialAssistantSequence!);
			expect(firstAssistant?.parts.filter(part => part.type === "tool_result").map(part => [part.modelToolCallId, part.result.type])).toEqual([["write-first", "completed"], ["write-second", "completed"]]);
			expect(session.state.activeTools()).toHaveLength(0);
			expect(metrics.snapshot().activeToolFibers).toBe(0);
			if (delayed) expect(order.indexOf("end-applied")).toBeLessThan(order.indexOf("accepted:write-second"));
			else expect(order.indexOf("settlement:2")).toBeLessThan(order.indexOf("end-applied"));
		} finally {
			firstAccepted.resolve();
			releaseFirst.resolve();
			bothSettled.resolve();
			await run;
			await rm(directory, { recursive: true, force: true });
		}
	}, 15000);
}
