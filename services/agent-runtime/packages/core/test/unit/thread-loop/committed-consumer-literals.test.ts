import { expect, test } from "bun:test";
import { ProviderRequestKind } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { Effect, Stream } from "effect";
import type { RuntimeContextEntry, SessionEventWriter } from "../../../src/contracts/runtime.js";
import type { LLMEvent } from "../../../src/llm/llm-event.js";
import type { LLMRequest } from "../../../src/llm/llm-service.js";
import { toGatewayProviderContext } from "../../../src/runtime/context-projection.js";
import { AutoApprovalReviewerManager } from "../../../src/session/approval-reviewer-manager.js";
import * as ThreadLoop from "../../../src/thread-loop/thread-loop.js";
import { finishIdleCompletionCreate } from "../../../src/thread-loop/closeout.js";
import { ThreadRuntime } from "../../../src/thread-loop/thread-runtime.js";
import { createToolCatalog } from "../../../src/tools/tool-catalog.js";
import { compactionHistory, queuedLLMService, recordCompactionHint, deferred, RecordingContextLoader, runtimeThreadLoopLayer, testRunCustody, userMessage, writerFrom } from "./thread-loop-test-support.js";

// The literals are independent of provider projection helpers. The reviewer,
// End boundary, and successor request all consume the actual committed owner.
test("reviewer and normal consumers share one committed alpha/Read owner across End", async () => {
	const history: readonly RuntimeContextEntry[] = [
		userMessage("prior-user", 1, "prior-user"),
		{ messageSequence: 2, contextKind: "assistant", parts: [{ type: "text", text: "prior-answer" }] },
	];
	const currentUser = userMessage("current-user", 3, "continue");
	const beforeEndHistory = [...history, currentUser];
	const session = new ThreadRuntime("sesn_consumer_literals", new AutoApprovalReviewerManager());
	const reviewerEntered = deferred<void>();
	const releaseReviewer = deferred<void>();
	const executionEntered = deferred<void>();
	const releaseExecution = deferred<void>();
	const ended = deferred<void>();
	const requests: LLMRequest[] = [];
	let reviewInput: Parameters<NonNullable<ThreadLoop.ThreadLoopRuntimeOptions["reviewApproval"]>>[0] | undefined;
	let openHistory: readonly RuntimeContextEntry[] | undefined;
	const originalFact = session.state.applyThreadTurnFact.bind(session.state);
	session.state.applyThreadTurnFact = fact => {
		const result = originalFact(fact);
		if (fact.fact === "request_ended") ended.resolve();
		return result;
	};
	const baseWriter = writerFrom(envelope => ({ ok: true, type: "committed", eventId: `bridge-${envelope.writeId}` }), undefined, [], { eventSequence: 0, messageSequence: 2 });
	const writer: SessionEventWriter = {
		...baseWriter,
		writeRequestEnd: async envelope => {
			openHistory ??= structuredClone(session.state.contextManager.historyMessages());
			return baseWriter.writeRequestEnd(envelope);
		},
	};
	const run = Effect.runPromise(Effect.gen(function* () {
		return yield* (yield* ThreadLoop.Service).run(session, testRunCustody());
	}).pipe(Effect.provide(runtimeThreadLoopLayer(new RecordingContextLoader(history, { type: "context", entries: [currentUser] }), {
		writer,
		approvalMode: "approve_for_me",
		providerCallRuntime: { systemInstructions: "consumer literals", toolCatalog: createToolCatalog({ family: "claude", configs: [{ name: "Read", enabled: true, permissionPolicy: "always_ask" }] }) },
		llmService: { stream: request => {
			requests.push(request);
			return Stream.fromIterable<LLMEvent>(requests.length === 1 ? [
				{ type: "text-complete" as const, providerPartId: "alpha", eventId: "evt_11111111111111111111111111111111", text: "alpha" },
				{ type: "thinking-started" as const, providerPartId: "reason", eventId: "evt_22222222222222222222222222222222" },
				{ type: "reasoning-complete" as const, providerPartId: "reason", thinkingEventId: "evt_22222222222222222222222222222222", text: "reason-before-tool", providerMetadata: { anthropic: { signature: "fixture-signature" } } },
				{ type: "tool-call-complete" as const, id: "read-note", toolName: "Read", input: { file_path: "note.txt" }, inputPreview: { preview: '{"file_path":"note.txt"}', truncated: false } },
				{ type: "finish" as const, finishReason: "tool-calls" as const },
			] : [
				{ type: "text-complete" as const, providerPartId: "done", eventId: "evt_33333333333333333333333333333333", text: "done" },
				{ type: "finish" as const, finishReason: "stop" as const },
			]);
		} },
		reviewApproval: input => Effect.promise(async () => {
			reviewInput = input;
			reviewerEntered.resolve();
			await releaseReviewer.promise;
			return { type: "decision", riskLevel: "low", userAuthorization: "high", outcome: "allow" };
		}),
		runTool: async () => {
			executionEntered.resolve();
			await releaseExecution.promise;
			return { type: "completed", output: { text: "fixture-note", truncated: false } };
		},
	}))));
	const beforeCompletion = (barrier: Promise<void>) => Promise.race([barrier, run.then(() => { throw new Error("run completed before consumer barrier"); })]);
	const timeout = setTimeout(() => { releaseReviewer.resolve(); releaseExecution.resolve(); }, 2000);
	try {
		await beforeCompletion(reviewerEntered.promise);
		expect(reviewInput?.parentTranscript.entries).toEqual(beforeEndHistory);
		expect(reviewInput?.currentAssistantDraft).toEqual([{ type: "text", text: "alpha" }]);
		expect(session.state.contextManager.historyMessages()).toEqual(beforeEndHistory);
		releaseReviewer.resolve();
		await beforeCompletion(executionEntered.promise);
		await beforeCompletion(ended.promise);
		const pendingOwner = session.state.contextManager.currentAssistantMessage();
		expect(openHistory).toEqual(beforeEndHistory);
		expect(pendingOwner).toEqual({ messageSequence: 4, contextKind: "assistant", parts: [
			{ type: "text", text: "alpha" },
			{ type: "reasoning", text: "reason-before-tool", providerMetadata: { anthropic: { signature: "fixture-signature" } } },
			{ type: "tool_call", modelToolCallId: "read-note", toolName: "Read", canonicalInput: { file_path: "note.txt" } },
		] });
		expect(session.state.contextManager.historyMessages()).toEqual([...beforeEndHistory, pendingOwner!]);
		expect(session.state.activeTools()).toHaveLength(1);
		expect(requests).toHaveLength(1);
		releaseExecution.resolve();
		expect((await run).type).toBe("completed");
		expect(requests).toHaveLength(2);
		expect(requests[1]!.context).toEqual([
			{ role: 1, content: [{ text: { text: "prior-user" } }] },
			{ role: 2, content: [{ text: { text: "prior-answer" } }] },
			{ role: 1, content: [{ text: { text: "continue" } }] },
			{ role: 2, content: [
				{ text: { text: "alpha" } },
				{ reasoning: { text: "reason-before-tool", metadataJson: '{"anthropic":{"signature":"fixture-signature"}}' } },
				{ toolCall: { modelToolCallId: "read-note", name: "Read", inputJson: '{"file_path":"note.txt"}' } },
				{ toolResult: { modelToolCallId: "read-note", completed: { outputJson: '{"text":"fixture-note"}' } } },
			] },
		]);
		expect(session.state.activeTools()).toEqual([]);
		session.updateIdentity({ ...session.identity, threadRole: "subagent", taskName: "child", parentTaskName: "main" });
		expect(finishIdleCompletionCreate(session, { type: "end_turn" }, undefined)).toBe(
			"Message Type: FINAL_ANSWER\nTask name: main\nSender: child\nPayload:\ndone",
		);
	} finally {
		clearTimeout(timeout);
		releaseReviewer.resolve();
		releaseExecution.resolve();
		await run;
	}
}, 5000);

test("actual compaction consumes prior literals and installs compact-prior once", async () => {
	const session = new ThreadRuntime("sesn_compaction_literals");
	recordCompactionHint(session, { inputTokens: 1, outputTokens: 1, reasoningTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0 });
	const requests: LLMRequest[] = [];
	const history: readonly RuntimeContextEntry[] = [
		userMessage("prior-user", 1, "prior-user"),
		{ messageSequence: 2, contextKind: "assistant", parts: [{ type: "text", text: "prior-answer" }] },
		userMessage("padding", 3, compactionHistory("fixed historical padding")),
	];
	const result = await Effect.runPromise(Effect.gen(function* () {
		return yield* (yield* ThreadLoop.Service).run(session, testRunCustody());
	}).pipe(Effect.provide(runtimeThreadLoopLayer(new RecordingContextLoader(history, {
		type: "context", entries: [userMessage("current", 4, "continue")],
	}), {
		compaction: {},
		writer: writerFrom(envelope => ({ ok: true, type: "committed", eventId: `bridge-${envelope.writeId}` }), undefined, [], { eventSequence: 0, messageSequence: 4 }),
		llmService: queuedLLMService([
			[{ type: "text-complete", providerPartId: "summary", eventId: "evt_44444444444444444444444444444444", text: "compact-prior" }, { type: "finish", finishReason: "stop" }],
			[{ type: "text-complete", providerPartId: "done", eventId: "evt_55555555555555555555555555555555", text: "done" }, { type: "finish", finishReason: "stop" }],
		], requests),
	}))));
	expect(result.type).toBe("completed");
	expect(requests.map(request => request.requestKind)).toEqual([
		ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY,
		ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
	]);
	const compactionInput = JSON.stringify(requests[0]!.context);
	expect(compactionInput.split("prior-user")).toHaveLength(2);
	expect(compactionInput.split("prior-answer")).toHaveLength(2);
	expect(compactionInput).not.toContain("compact-prior");
	const normalInput = JSON.stringify(requests[1]!.context);
	expect(normalInput.split("compact-prior")).toHaveLength(2);
	expect(normalInput).toContain("continue");
	expect(session.state.contextManager.messages().filter(message => message.contextKind === "compaction")).toHaveLength(1);
});

test("abnormal reducer retention removes completed siblings and publishes only the selected signed pair", () => {
	const session = new ThreadRuntime("sesn_abnormal_literals");
	const history: readonly RuntimeContextEntry[] = [
		userMessage("prior-user", 1, "prior-user"),
		{ messageSequence: 2, contextKind: "assistant", parts: [{ type: "text", text: "prior-answer" }] },
	];
	const call = { type: "tool_call" as const, modelToolCallId: "read-note", toolName: "Read", canonicalInput: { file_path: "note.txt" } };
	const reason = { type: "reasoning" as const, text: "reason-before-tool", providerMetadata: { anthropic: { signature: "fixture-signature" } } };
	const siblingReason = { type: "reasoning" as const, text: "unrelated-sibling-reason", providerMetadata: { anthropic: { signature: "sibling-signature" } } };
	const siblingCall = { type: "tool_call" as const, modelToolCallId: "read-sibling", toolName: "Read", canonicalInput: { file_path: "sibling.txt" } };
	const siblingResult = { type: "tool_result" as const, modelToolCallId: "read-sibling", result: { type: "completed" as const, output: { text: "sibling-result" } } };
	const originalParts = [
		{ type: "text" as const, text: "alpha" },
		siblingReason,
		siblingCall,
		siblingResult,
		{ type: "text" as const, text: "unrelated-between" },
		reason,
		call,
		{ type: "text" as const, text: "unrelated-after" },
		{ type: "reasoning" as const, text: "unrelated-trailing", providerMetadata: { anthropic: { signature: "trailing-signature" } } },
	];
	const checkpoint = {
		executionRunId: "running",
		pendingInputContextSequences: [],
		request: {
			modelRequestId: "request", requestStartEventId: "start", requestKind: "agent_provider_request" as const,
			contextThroughMessageSequence: 2,
			toolMembers: [
				{ memberKind: "public_tool_use" as const, modelToolCallId: "read-sibling", toolUseEventId: "sibling-use", toolName: "Read", terminalResult: { outcome: "success" as const } },
				{ memberKind: "public_tool_use" as const, modelToolCallId: "read-note", toolUseEventId: "use", toolName: "Read" },
			],
		},
	};
	session.state.installThreadCheckpoint(checkpoint);
	session.state.contextManager.replaceMessages([...history, { messageSequence: 3, contextKind: "assistant", parts: originalParts }]);
	session.state.installCurrentRequestMessage({ modelRequestId: "request", assistantMessageSequence: 3 });
	session.state.markPersistentContextLoaded();
	session.state.registerActiveTool({ toolUseEventId: "use", modelRequestId: "request", modelToolCallId: "read-note", assistantMessageSequence: 3, disposition: "requires_user_action" });
	session.state.installThreadTurn(checkpoint, { routes: [{ toolUseEventId: "use", disposition: "requires_user_action" }] });
	expect(session.state.contextManager.currentAssistantMessage()?.parts).toEqual(originalParts);
	expect(session.state.contextManager.historyMessages()).toEqual(history);
	session.state.applyThreadTurnFact({ fact: "request_ended", eventId: "end", modelRequestId: "request", isError: true, errorKind: "provider_stream_error", providerContextRetention: {
		disposition: "failed", assistantMessageSequence: 3, toolUseEventIds: ["use"], repairEventIds: [],
	} });
	expect(session.state.contextManager.currentAssistantMessage()?.parts).toEqual([reason, call]);
	expect(session.state.contextManager.historyMessages()).toEqual(history);
	const result = { type: "completed" as const, output: { text: "fixture-note" } };
	session.state.contextManager.appendToolResult(3, "read-note", result);
	session.state.applyThreadTurnFact({ fact: "tool_result_committed", toolUseEventId: "use", outcome: "success" });
	session.state.clearThreadToolRoute("use");
	expect(session.state.contextManager.historyMessages()).toEqual([...history, { messageSequence: 3, contextKind: "assistant", parts: [reason, call, { type: "tool_result", modelToolCallId: "read-note", result }] }]);
	expect(session.state.contextManager.messages().some(message => message.parts.some(part => part.type === "text" && part.text === "alpha"))).toBe(false);
	expect(session.state.activeTools()).toEqual([]);
	expect(toGatewayProviderContext(session.state.contextManager.historyMessages())).toEqual({
		ok: true,
		context: [
			{ role: 1, content: [{ text: { text: "prior-user" } }] },
			{ role: 2, content: [{ text: { text: "prior-answer" } }] },
			{ role: 2, content: [
				{ reasoning: { text: "reason-before-tool", metadataJson: '{"anthropic":{"signature":"fixture-signature"}}' } },
				{ toolCall: { modelToolCallId: "read-note", name: "Read", inputJson: '{"file_path":"note.txt"}' } },
				{ toolResult: { modelToolCallId: "read-note", completed: { outputJson: '{"text":"fixture-note"}' } } },
			] },
		],
	});
});
