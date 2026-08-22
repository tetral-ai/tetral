import { describe, expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import {
	ProviderContextRole,
	ProviderRequestKind,
	SystemCacheHint,
	SystemSegmentKind,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { Effect, Layer, Stream } from "effect";
import { normalizeProviderError } from "../../../src/contracts/provider.js";
import type {
	PendingInputResult,
	RuntimeContextEntry,
	SessionEvent,
	SessionEventWriter,
} from "../../../src/contracts/runtime.js";
import {
	normalizeContextLoaderError,
	normalizeSessionEventWriterError,
	RuntimeContextEntrySchema,
} from "../../../src/contracts/runtime.js";
import type { LLMEvent } from "../../../src/llm/llm-event.js";
import { runtimeFailureFromProviderError } from "../../../src/llm/llm-event.js";
import type {
	LLMRequest,
	Interface as LLMServiceInterface,
} from "../../../src/llm/llm-service.js";
import type { ProviderCallRuntimeConfig } from "../../../src/thread-loop/provider-request.js";
import { DefaultProviderCallRuntimeConfig } from "../../../src/thread-loop/provider-request.js";
import * as ThreadLoop from "../../../src/thread-loop/thread-loop.js";
import { ThreadRuntime } from "../../../src/thread-loop/thread-runtime.js";
import type {
	RuntimeAcceptedInputState,
	RuntimeTaskNotificationState,
} from "../../../src/thread-loop/thread-state.js";
import {
	MaxProviderAttachments,
	ThreadState,
} from "../../../src/thread-loop/thread-state.js";
import type { ThreadTurnAction } from "../../../src/thread-loop/thread-turn-reducer.js";
import type {
	PackageJson,
	TestContextLoader,
} from "./thread-loop-test-support.js";
import {
	acceptedInput,
	acceptedInputCommitResult,
	catalogForTest,
	createdAt,
	failingEventWriter,
	installLoaderStateForTest,
	llmService,
	providerAttachmentsForTest,
	QueuedContextLoader,
	RecordingContextLoader,
	runtimeNotificationMessage,
	runtimeThreadLoopLayer,
	ThreadLoopRuntimeStore,
	taskNotificationInput,
	testRunCustody,
	threadLoopRuntime,
	userMessage,
	writerFrom,
} from "./thread-loop-test-support.js";

describe("ThreadLoop", () => {
	test("the ThreadLoop action interpreter exhaustively classifies the closed action union", () => {
		const activeActions: ThreadTurnAction[] = [
			{ action: "prepare_next_request" },
			{ action: "start_provider_request", modelRequestId: "mrq_start" },
			{ action: "dispatch_tool_use", toolUseEventId: "sevt_tool" },
			{ action: "reconcile_request_seal", modelRequestId: "mrq_seal" },
			{
				action: "resume_tool_routes",
				modelRequestId: "mrq_resume",
				toolUseEventIds: ["sevt_tool"],
			},
			{ action: "finish_idle", stopReason: { type: "end_turn" } },
			{ action: "continue_after_compaction", modelRequestId: "mrq_compaction" },
			{ action: "complete_reviewer", modelRequestId: "mrq_reviewer" },
			{
				action: "apply_request_retry_or_reschedule",
				modelRequestId: "mrq_retry",
			},
			{ action: "commit_accepted_input", runtimeInputId: "rin_commit" },
			{ action: "close_interrupted" },
			{ action: "close_failed" },
		];
		const passiveActions: ThreadTurnAction[] = [
			{ action: "none" },
			{ action: "await_input" },
			{ action: "await_request_end", modelRequestId: "mrq_open" },
			{
				action: "await_tool_results",
				modelRequestId: "mrq_wait",
				toolUseEventIds: ["sevt_tool"],
			},
		];
		expect(
			activeActions.map(
				(action) => ThreadLoop.interpretThreadTurnAction(action).runDisposition,
			),
		).toEqual(activeActions.map(() => "active"));
		expect(
			passiveActions.map(
				(action) => ThreadLoop.interpretThreadTurnAction(action).runDisposition,
			),
		).toEqual(passiveActions.map(() => "passive"));
	});
	test("pins effect to the approved beta version in package metadata", async () => {
		const packageJson = JSON.parse(
			await readFile(new URL("../../../package.json", import.meta.url), "utf8"),
		) as PackageJson;
		const lockfileText = await readFile(
			new URL("../../../../../bun.lock", import.meta.url),
			"utf8",
		);
		expect(packageJson.dependencies?.effect).toBe("4.0.0-beta.66");
		expect(lockfileText).toContain('"effect": "4.0.0-beta.66"');
		expect(lockfileText).toContain('"effect": ["effect@4.0.0-beta.66"');
		expect(lockfileText).not.toContain('"effect": "^4.0.0-beta.66"');
		expect(lockfileText).not.toContain('"effect": "4.0.0-beta.74"');
		expect(lockfileText).not.toContain('"effect": "3.');
	});
	test("default layer resolves a void session run frame", async () => {
		const loader = new RecordingContextLoader([], { type: "empty" });
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(
					new ThreadRuntime("sesn_1"),
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, { runtimeModel: () => undefined }),
				),
			),
		);
		expect(result).toEqual({ type: "completed", modelMessageCount: 0 });
	});
	test("refreshes the binding token without advancing the request anchor past its committed snapshot", async () => {
		const session = new ThreadRuntime("sesn_1");
		const requests: LLMRequest[] = [];
		const identities: string[] = [];
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-refresh", 0, "refresh token")],
		});
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						events: [
							{ type: "text-start", id: "text-1" },
							{ type: "text-delta", id: "text-1", text_delta: "ok" },
							{ type: "text-end", id: "text-1" },
							{
								type: "finish",
								finishReason: "stop",
								usage: {
									inputTokens: 10,
									outputTokens: 2,
									reasoningTokens: 0,
									cacheReadTokens: 0,
									cacheWriteTokens: 0,
								},
								modelLimits: {
									contextWindowTokens: 320,
									outputTokenLimit: 120,
								},
							},
						],
						onStream: (request) => requests.push(request),
						refreshRuntimeBindingToken: async (identity) => {
							identities.push(identity.runtimeBindingToken);
							session.state.contextManager.appendEntry({
								...runtimeNotificationMessage(
									"msg_task_during_refresh",
									"committed after request snapshot",
								),
								messageSequence: 1,
							});
							return "runtime-binding-token-refreshed";
						},
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed" });
		expect(identities).toEqual(["runtime-binding-token-test"]);
		expect(requests[0]?.runtimeBindingToken).toBe(
			"runtime-binding-token-refreshed",
		);
		expect(JSON.stringify(requests[0]?.context)).not.toContain(
			"committed after request snapshot",
		);
		expect(session.identity.runtimeBindingToken).toBe(
			"runtime-binding-token-refreshed",
		);
		expect(session.state.lastRequestContextAnchorSequence()).toBe(
			session.state.contextManager
				.entries()
				.find((message) => message.contextKind === "user")?.messageSequence,
		);
	});
	test("loads cold context and pending messages without deriving the configured model from either message list", async () => {
		const history = [userMessage("user-1", 1, "first")];
		const pending = {
			type: "context",
			entries: [
				userMessage("user-2", 2, "second"),
				userMessage("user-3", 3, "third"),
			],
		} as const satisfies PendingInputResult;
		const loader = new RecordingContextLoader(history, pending);
		const session = new ThreadRuntime("sesn_1");
		// The supplier returns a model no message in either fixture list carries,
		// so a reintroduced derivation from ANY message (first-wins or last-wins)
		// produces a mismatch here instead of passing by coincidence.
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						runtimeModel: () => ({
							providerId: "resolved",
							modelId: "from-config",
						}),
					}),
				),
			),
		);
		expect(result).toEqual({
			type: "completed",
			currentModel: { providerId: "resolved", modelId: "from-config" },
			modelMessageCount: 3,
		});
		expect(loader.buildCalls).toEqual(["sesn_1"]);
		expect(loader.pendingCalls).toEqual(["sesn_1"]);
		expect(
			session.state.contextManager
				.entries()
				.map((message) => message.contextKind),
		).toEqual(["user", "user", "user", "assistant"]);
		expect(
			JSON.stringify(session.state.contextManager.entries()),
		).not.toContain("system prompt");
		expect(
			JSON.stringify(session.state.contextManager.entries()),
		).not.toContain("toolDefinitions");
	});
	test("assembles non-persistent runtime inputs into LLMRequest without storing them in hot or durable messages", async () => {
		const session = new ThreadRuntime("sesn_1");
		const store = new ThreadLoopRuntimeStore([]);
		const capturedRequests: LLMRequest[] = [];
		const systemCanary = "third group system instruction canary";
		const toolDescriptionCanary = "third group tool description canary";
		const toolSchemaCanary = "third group schema canary";
		const providerConfigCanary = "third group provider config canary";
		const dummyTokenCanary = "dummy-thirdgroup-token";
		const tools = [
			{
				name: "third_group_lookup",
				description: toolDescriptionCanary,
				inputSchema: {
					type: "object",
					properties: {
						query: { type: "string", description: toolSchemaCanary },
						providerConfigMarker: { const: providerConfigCanary },
						dummyTokenMarker: { const: dummyTokenCanary },
					},
				},
			},
		];
		const runtimeBoundary: ProviderCallRuntimeConfig = {
			systemInstructions: systemCanary,
			toolCatalog: catalogForTest(tools[0]!),
			maxOutputTokens: 321,
			timeoutMs: 777,
		};
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						store,
						providerCallRuntime: runtimeBoundary,
						onStream: (request) => {
							capturedRequests.push(request);
						},
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed", modelMessageCount: 1 });
		expect(capturedRequests).toHaveLength(1);
		expect(capturedRequests[0]).toMatchObject({
			requestKind:
				ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
			workspaceId: "workspace-test",
			sessionId: "sesn_1",
			sessionThreadId: "thread-test",
			bindingId: "binding-test",
			bindingGeneration: 1,
			runtimeBindingToken: "runtime-binding-token-test",
			model: { providerId: "fake", modelId: "fake-chat", variant: "" },
			system: [
				{
					kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
					text: systemCanary,
					cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE,
				},
			],
			context: [
				{
					role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
					content: [{ text: { text: "hello" } }],
				},
			],
			tools: [
				{
					name: "third_group_lookup",
					description: toolDescriptionCanary,
					function: { inputSchemaJson: JSON.stringify(tools[0]?.inputSchema) },
				},
			],
			attachments: [],
			limits: { maxOutputTokens: 321, timeoutMs: 777 },
		});
		const hotContext = JSON.stringify(session.state.contextManager.entries());
		for (const canary of [
			systemCanary,
			toolDescriptionCanary,
			toolSchemaCanary,
			providerConfigCanary,
			dummyTokenCanary,
			"maxOutputTokens",
			"timeoutMs",
		]) {
			expect(hotContext).not.toContain(canary);
		}
	});
	test("empty cold history still processes pending input", async () => {
		const pendingLoader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const pendingResult = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(
					new ThreadRuntime("sesn_1"),
					testRunCustody(),
				);
			}).pipe(Effect.provide(runtimeThreadLoopLayer(pendingLoader))),
		);
		expect(pendingResult).toMatchObject({
			type: "completed",
			modelMessageCount: 1,
		});
	});
	test("lost Request Start acknowledgement retries one write identity and starts the provider once", async () => {
		const session = new ThreadRuntime("sesn_request_start_ack_loss");
		session.state.enqueueAcceptedInput(
			acceptedInput("rin_request_start_ack_loss", session.sessionId),
		);
		const requestStartWriteIds: string[] = [];
		let providerCalls = 0;
		const writer = writerFrom((envelope) => {
			if (envelope.event.type === "span.model_request_start") {
				requestStartWriteIds.push(envelope.writeId);
				if (requestStartWriteIds.length === 1) {
					return {
						ok: false,
						error: normalizeSessionEventWriterError({
							code: "timeout",
							sessionId: envelope.sessionId,
						}),
					};
				}
			}
			return {
				ok: true,
				eventId:
					envelope.event.type === "span.model_request_start"
						? "sevt_request_start_ack_loss"
						: `bridge-${envelope.writeId}`,
				type: "committed",
				eventSequence: 1,
			};
		});
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(new QueuedContextLoader([], []), {
						writer,
						onStream: () => {
							providerCalls += 1;
						},
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed" });
		expect(requestStartWriteIds).toHaveLength(2);
		expect(new Set(requestStartWriteIds).size).toBe(1);
		expect(providerCalls).toBe(1);
	});
	test("input accepted during an empty provider response prevents premature idle closeout", async () => {
		const session = new ThreadRuntime("sesn_late_input_before_idle");
		session.state.enqueueAcceptedInput(
			acceptedInput("rin_initial_before_idle", session.sessionId),
		);
		const requests: LLMRequest[] = [];
		const appended: SessionEvent[] = [];
		const llm: LLMServiceInterface = {
			stream(request) {
				requests.push(request);
				if (requests.length === 1) {
					expect(
						session.state.enqueueAcceptedInput(
							acceptedInput("rin_late_before_idle", session.sessionId),
						),
					).toBe("applied");
				}
				return Stream.fromIterable([
					{ type: "text-start", id: `text-${requests.length}` },
					{
						type: "text-delta",
						id: `text-${requests.length}`,
						text_delta: `answer ${requests.length}`,
					},
					{ type: "text-end", id: `text-${requests.length}` },
					{ type: "finish", finishReason: "stop" },
				]);
			},
		};
		const writer = writerFrom((envelope) => {
			appended.push(envelope.event);
			return {
				ok: true,
				eventId: `bridge-${envelope.writeId}`,
				type: "committed",
				eventSequence: 1,
			};
		});
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(new QueuedContextLoader([], []), {
						llmService: llm,
						writer,
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed" });
		expect(requests).toHaveLength(2);
		expect(requests[0]?.context.map((message) => message.role)).toEqual([
			ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
		]);
		expect(requests[1]?.context.map((message) => message.role)).toEqual([
			ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
			ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
			ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
		]);
		expect(
			appended.filter((event) => event.type === "session.status_idle"),
		).toHaveLength(1);
	});
	test("lost CommitInputs response retries the frozen declaration without duplicating hot input", async () => {
		const session = new ThreadRuntime("sesn_lost_commit_response");
		const input = acceptedInput("rin_lost_commit_response", session.sessionId);
		session.state.enqueueAcceptedInput(input);
		let attempts = 0;
		let observeAssignedSequence = (_messageSequence: number): void => undefined;
		const loader: TestContextLoader = {
			buildContext: async () => [],
			loadPendingInput: async () => ({ type: "empty" }),
			bindDurableSequence: (sequence) => {
				observeAssignedSequence = (messageSequence) => {
					sequence.messageSequence = Math.max(
						sequence.messageSequence,
						messageSequence,
					);
				};
			},
			commitAcceptedInput: async (accepted) => {
				attempts += 1;
				if (attempts === 1) {
					throw normalizeContextLoaderError({
						code: "unavailable",
						sessionId: accepted.sessionId,
						reason: "commit response was lost",
					});
				}
				const result = acceptedInputCommitResult(accepted, "duplicate");
				for (const messageSequence of result.assignedContextSequences) {
					observeAssignedSequence(messageSequence);
				}
				return result;
			},
		};
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(Effect.provide(runtimeThreadLoopLayer(loader))),
		);
		expect(result).toMatchObject({ type: "completed" });
		expect(attempts).toBe(2);
		expect(
			session.state.contextManager
				.entries()
				.filter((message) => message.contextKind === "user"),
		).toHaveLength(1);
	});
	test("hung CommitInputs attempts exhaust the bounded writer budget without opening provider work", async () => {
		const session = new ThreadRuntime("sesn_hung_commit_inputs");
		const input = acceptedInput("rin_hung_commit_inputs", session.sessionId);
		session.state.enqueueAcceptedInput(input);
		const commitOutcomes: string[] = [];
		const terminationEnvelopes: Array<
			Parameters<NonNullable<SessionEventWriter["commitRuntimeTermination"]>>[0]
		> = [];
		let providerCalls = 0;
		const loader: TestContextLoader = {
			buildContext: async () => [],
			loadPendingInput: async () => ({ type: "empty" }),
			commitAcceptedInput: async () => {
				return await new Promise<never>(() => undefined);
			},
		};
		const baseWriter = writerFrom((envelope) => ({
			ok: true,
			eventId: `bridge-${envelope.writeId}`,
			type: "committed",
			eventSequence: 1,
		}));
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer: {
							...baseWriter,
							commitRuntimeTermination: (envelope) => {
								terminationEnvelopes.push(envelope);
								return baseWriter.commitRuntimeTermination!(envelope);
							},
						},
						onStream: () => {
							providerCalls += 1;
						},
						recordAcceptedInputCommit: (event) =>
							commitOutcomes.push(`${event.attempt}:${event.outcome}`),
					}),
				),
			),
		);
		expect(result).toMatchObject({
			type: "failed",
			error: {
				code: "runtime_persistence_exhausted",
				reason: "runtime_input_commit_exhausted",
				retryable: false,
				fatal: true,
				retryStatus: { type: "terminal" },
			},
		});
		expect(commitOutcomes).toEqual([
			"1:started",
			"1:retry",
			"2:started",
			"2:retry",
			"3:started",
			"3:exhausted",
		]);
		expect(providerCalls).toBe(0);
		expect(terminationEnvelopes).toHaveLength(1);
		expect(terminationEnvelopes[0]?.failure).toMatchObject({
			type: "runtime",
			code: "runtime_persistence_exhausted",
			reason: "runtime_input_commit_exhausted",
			retryable: false,
			fatal: true,
			retryStatus: { type: "terminal" },
		});
		expect(
			session.state
				.acceptedInputSnapshot()
				.map((accepted) => accepted.runtimeInputId),
		).toEqual([input.runtimeInputId]);
	});
	test("non-retryable CommitInputs rejection stops after the first frozen attempt", async () => {
		const session = new ThreadRuntime("sesn_nonretryable_commit_inputs");
		const input = acceptedInput(
			"rin_nonretryable_commit_inputs",
			session.sessionId,
		);
		session.state.enqueueAcceptedInput(input);
		let attempts = 0;
		const loader: TestContextLoader = {
			buildContext: async () => [],
			loadPendingInput: async () => ({ type: "empty" }),
			commitAcceptedInput: async (accepted) => {
				attempts += 1;
				throw normalizeContextLoaderError({
					code: "schema_mismatch",
					sessionId: accepted.sessionId,
					reason: "CommitInputs receipt is invalid",
				});
			},
		};
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(Effect.provide(runtimeThreadLoopLayer(loader))),
		);
		expect(result).toMatchObject({
			type: "failed",
			error: {
				code: "runtime_invalid_sequence",
				reason: "runtime_contract_validation",
				retryable: false,
			},
		});
		expect(attempts).toBe(1);
		expect(session.state.acceptedInputSnapshot()).toHaveLength(1);
	});
	test("one request cut commits every input accepted before the boundary", async () => {
		const session = new ThreadRuntime("sesn_plural_input_cut");
		session.state.enqueueAcceptedInput(
			acceptedInput("rin_plural_first", session.sessionId),
		);
		session.state.enqueueAcceptedInput(
			acceptedInput("rin_plural_second", session.sessionId),
		);
		const loader = new QueuedContextLoader([], []);
		const requests: LLMRequest[] = [];
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						installLoaderState: false,
						onStream: (request) => requests.push(request),
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed" });
		expect(loader.commitCalls.map((input) => input.runtimeInputId)).toEqual([
			"rin_plural_first",
			"rin_plural_second",
		]);
		expect(requests).toHaveLength(1);
		expect(requests[0]?.context.map((message) => message.role)).toEqual([
			ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
			ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
		]);
	});
	test("inputs admitted during a commit are reducer-selected before the finite request cut", async () => {
		const session = new ThreadRuntime("sesn_dynamic_input_cut");
		const first = acceptedInput("rin_dynamic_first", session.sessionId);
		const second = acceptedInput("rin_dynamic_second", session.sessionId);
		const third = acceptedInput("rin_dynamic_third", session.sessionId);
		session.state.enqueueAcceptedInput(first);
		const loader = new QueuedContextLoader(
			[],
			[],
			[
				(input: RuntimeAcceptedInputState) => {
					expect(session.state.enqueueAcceptedInput(second)).toBe("applied");
					return acceptedInputCommitResult(input, "committed", 1);
				},
				(input: RuntimeAcceptedInputState) => {
					expect(session.state.enqueueAcceptedInput(third)).toBe("applied");
					return acceptedInputCommitResult(input, "committed", 2);
				},
				(input: RuntimeAcceptedInputState) =>
					acceptedInputCommitResult(input, "committed", 3),
			],
		);
		const requests: LLMRequest[] = [];
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						installLoaderState: false,
						onStream: (request) => requests.push(request),
					}),
				),
			),
		);

		expect(result).toMatchObject({ type: "completed" });
		expect(loader.commitCalls.map((input) => input.runtimeInputId)).toEqual([
			first.runtimeInputId,
			second.runtimeInputId,
			third.runtimeInputId,
		]);
		expect(requests).toHaveLength(1);
		expect(
			requests[0]?.context.filter(
				(message) =>
					message.role === ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
			),
		).toHaveLength(3);
	});
	test("lost CommitInputs acknowledgement cold-loads the database-stamped input exactly once", async () => {
		const session = new ThreadRuntime("sesn_cold_committed_input");
		const replayedInput = acceptedInput(
			"rin_cold_committed_input",
			session.sessionId,
		);
		const committed = userMessage(
			"msg_rin_cold_committed_input_0",
			1,
			"resume this durable input",
		);
		session.state.contextManager.replaceEntries([committed]);
		session.state.markPersistentContextLoaded();
		session.state.installThreadTurn(
			{
				pendingInputContextSequences: [committed.messageSequence],
			},
			{ routes: [] },
		);
		expect(session.state.enqueueAcceptedInput(replayedInput)).toBe("applied");
		const requests: LLMRequest[] = [];
		const loader = new QueuedContextLoader(
			[],
			[],
			[
				(input: RuntimeAcceptedInputState) =>
					acceptedInputCommitResult(input, "duplicate", 1),
			],
		);
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						installLoaderState: false,
						onStream: (request) => requests.push(request),
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed" });
		expect(loader.commitCalls.map((input) => input.runtimeInputId)).toEqual([
			replayedInput.runtimeInputId,
		]);
		expect(
			session.state.threadTurnReduction().checkpoint
				.pendingInputContextSequences,
		).toEqual([]);
		expect(requests).toHaveLength(1);
		expect(
			requests[0]?.context.filter(
				(message) =>
					message.role === ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
			),
		).toHaveLength(1);
		expect(requests[0]?.context.map((message) => message.role)).toEqual([
			ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
		]);
	});
	test("cold recovery opens a run before continuing a sealed request with terminal tool results", async () => {
		const session = new ThreadRuntime("sesn_cold_tool_continuation");
		const committed = userMessage(
			"msg_cold_tool_continuation",
			1,
			"continue after the durable tool result",
		);
		session.state.contextManager.replaceEntries([committed]);
		session.state.markPersistentContextLoaded();
		session.state.installThreadTurn(
			{
				pendingInputContextSequences: [],
				request: {
					modelRequestId: "request_cold_tool_continuation",
					requestStartEventId: "event_start_cold_tool_continuation",
					requestKind: "agent_provider_request",
					contextThroughMessageSequence: 1,
					requestEnd: {
						eventId: "event_end_cold_tool_continuation",
						isError: false,
			providerContextRetention: { disposition: "completed", toolUseEventIds: [], repairEventIds: [] },
					},
					toolMembers: [
						{
							memberKind: "public_tool_use",
							modelToolCallId: "call_cold_tool_continuation",
							toolUseEventId: "event_tool_cold_tool_continuation",
							toolName: "Read",
							terminalResult: { outcome: "success" },
						},
					],
				},
			},
			{ routes: [] },
		);
		const requests: LLMRequest[] = [];
		const appended: SessionEvent[] = [];
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(new QueuedContextLoader([], []), {
						installLoaderState: false,
						onStream: (request) => requests.push(request),
						writer: writerFrom((envelope) => {
							appended.push(envelope.event);
							return {
								ok: true,
								eventId: `bridge-${envelope.writeId}`,
								type: "committed",
								eventSequence: 1,
							};
						}),
					}),
				),
			),
		);

		expect(result).toMatchObject({ type: "completed" });
		expect(requests).toHaveLength(1);
		expect(appended[0]).toEqual({ type: "session.status_running" });
		expect(
			appended.filter((event) => event.type === "span.model_request_start"),
		).toHaveLength(1);
	});
	test("cold interrupt closeout consumes the typed action before releasing the run", async () => {
		const session = new ThreadRuntime("sesn_cold_interrupt_closeout");
		session.state.markPersistentContextLoaded();
		session.state.installThreadTurn(
			{
				executionRunId: "sevt_cold_interrupt_running",
				pendingInputContextSequences: [],
				interruptEventId: "sevt_cold_interrupt",
			},
			{ routes: [] },
		);
		const appended: SessionEvent[] = [];
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(new QueuedContextLoader([], []), {
						installLoaderState: false,
						writer: writerFrom((envelope) => {
							appended.push(envelope.event);
							return {
								ok: true,
								eventId: `bridge-${envelope.writeId}`,
								type: "committed",
								eventSequence: 1,
							};
						}),
					}),
				),
			),
		);

		expect(result).toEqual({ type: "interrupted" });
		expect(appended).toEqual([
			{ type: "session.status_idle", stop_reason: { type: "end_turn" } },
		]);
		expect(session.state.threadTurnReduction()).toMatchObject({
			checkpoint: {
				idleCloseout: {
					eventId: expect.any(String),
					stopReason: "end_turn",
				},
			},
			state: { state: "idle" },
			action: { action: "await_input" },
		});
	});
	test("a cold open request fails closed before accepting a later input", async () => {
		const session = new ThreadRuntime("sesn_cold_open_request");
		session.state.markPersistentContextLoaded();
		session.state.installThreadTurn(
			{
				executionRunId: "event_cold_open_running",
				pendingInputContextSequences: [],
				request: {
					modelRequestId: "request_cold_open",
					requestStartEventId: "event_cold_open_start",
					requestKind: "agent_provider_request",
					contextThroughMessageSequence: 0,
					toolMembers: [],
				},
			},
			{ routes: [] },
		);
		expect(
			session.state.enqueueAcceptedInput(acceptedInput("rin_after_cold_open")),
		).toBe("applied");
		const loader = new QueuedContextLoader([], [{ type: "empty" }]);
		let providerCalls = 0;
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						installLoaderState: false,
						onStream: () => {
							providerCalls += 1;
						},
					}),
				),
			),
		);

		expect(result).toMatchObject({
			type: "failed",
			error: {
				code: "runtime_invalid_sequence",
				retryStatus: { type: "terminal" },
			},
		});
		expect(loader.commitCalls ?? []).toEqual([]);
		expect(providerCalls).toBe(0);
		expect(session.state.threadTurnReduction()).toMatchObject({
			checkpoint: { terminalCloseout: { disposition: "terminated" } },
			state: { state: "idle" },
			action: { action: "await_input" },
		});
	});
	test("cold committed reschedule reopens exactly one provider request", async () => {
		const session = new ThreadRuntime("sesn_cold_reschedule_fence");
		const message = userMessage(
			"msg_cold_reschedule_fence",
			1,
			"retry this request",
		);
		session.state.contextManager.replaceEntries([message]);
		session.state.markPersistentContextLoaded();
		session.state.installThreadTurn(
			{
				executionRunId: "sevt_cold_reschedule_running",
				pendingInputContextSequences: [],
				request: {
					modelRequestId: "mreq_cold_reschedule",
					requestStartEventId: "sevt_cold_reschedule_start",
					requestKind: "agent_provider_request",
					contextThroughMessageSequence: 1,
					requestEnd: {
						eventId: "sevt_cold_reschedule_end",
						isError: true,
			providerContextRetention: { disposition: "failed", toolUseEventIds: [], repairEventIds: [] },
						errorKind: "provider_error",
						reschedule: {
							attempt: 1,
							effectiveDeadline: "2026-06-14T00:00:05.000Z",
							providerAttempts: 1,
							compactionAttempts: 1,
						},
					},
					toolMembers: [],
				},
			},
			{ routes: [] },
		);
		const appended: SessionEvent[] = [];
		let providerCalls = 0;
		const waitedMs: number[] = [];
		const requestEndAttempts: Array<number | undefined> = [];
		let runtimeId = 0;
		const baseWriter = writerFrom((envelope) => {
			appended.push(envelope.event);
			return {
				ok: true,
				eventId: `bridge-${envelope.writeId}`,
				type: "committed",
				eventSequence: 1,
			};
		});
		const coldRetryFailure = runtimeFailureFromProviderError(
			normalizeProviderError({
				code: "provider_stream_error",
				message: "retry after cold recovery",
				retryable: true,
				fatal: false,
			}),
		);
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
						runtimeThreadLoopLayer(new QueuedContextLoader([], []), {
							installLoaderState: false,
							runtime: {
								now: () => createdAt,
								monotonicMs: () => 0,
								createId: (prefix) => `${prefix}-${++runtimeId}`,
								sleep: async (delayMs) => {
									waitedMs.push(delayMs);
									return true;
								},
							},
							llmService: {
								stream: () => {
									providerCalls += 1;
									return Stream.fromIterable<LLMEvent>(
										providerCalls === 1
											? [{ type: "provider-error" as const, error: coldRetryFailure }]
											: [
													{ type: "text-start" as const, id: "cold-retry-text" },
													{ type: "text-delta" as const, id: "cold-retry-text", text_delta: "done" },
													{ type: "text-end" as const, id: "cold-retry-text" },
													{ type: "finish" as const, finishReason: "stop" as const },
												],
									);
								},
							},
							writer: {
								...baseWriter,
								writeRequestEnd: async (envelope) => {
									requestEndAttempts.push(envelope.reschedule?.attempt);
									return await baseWriter.writeRequestEnd(envelope);
								},
							},
					}),
				),
			),
		);

		expect(result).toMatchObject({ type: "completed" });
		expect(waitedMs.slice(0, 2)).toEqual([5_000, 2_000]);
		expect(providerCalls).toBe(2);
		expect(requestEndAttempts).toEqual([2, undefined]);
		expect(appended.filter((event) => event.type === "session.error")).toHaveLength(1);
		expect(
			appended.filter((event) => event.type === "span.model_request_start"),
		).toHaveLength(2);
		expect(appended.at(-1)).toEqual({
			type: "session.status_idle",
			stop_reason: { type: "end_turn" },
		});
		expect(session.state.threadTurnReduction()).toMatchObject({
			state: { state: "idle" },
			action: { action: "await_input" },
		});
	});
	test("accepted input without a resolvable runtime model settles an explicit exhausted error", async () => {
		const session = new ThreadRuntime("sesn_missing_config_model");
		session.state.enqueueAcceptedInput(
			acceptedInput("rin_missing_config_model", session.sessionId),
		);
		const loader = new QueuedContextLoader([], []);
		const appended: SessionEvent[] = [];
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						runtimeModel: () => undefined,
						writer: writerFrom((envelope) => {
							appended.push(envelope.event);
							return {
								ok: true,
								eventId: `bridge-${envelope.writeId}`,
								type: "committed",
								eventSequence: 1,
							};
						}),
					}),
				),
			),
		);
		expect(result).toMatchObject({
			type: "failed",
			error: {
				code: "runtime_invalid_sequence",
				fatal: true,
				reason: "runtime_contract_validation",
			},
		});
		expect(appended).toEqual([
			{ type: "session.status_running" },
			expect.objectContaining({
				type: "session.error",
				error: expect.objectContaining({
					code: "runtime_invalid_sequence",
					retryStatus: { type: "exhausted" },
				}),
			}),
			{
				type: "session.status_idle",
				stop_reason: { type: "retries_exhausted" },
			},
		]);
	});
	test("no accepted pending input performs no durable turn transition", async () => {
		const loader = new RecordingContextLoader([], { type: "empty" });
		const session = new ThreadRuntime("sesn_1");
		const appendedTypes: string[] = [];
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						runtimeModel: () => undefined,
						writer: writerFrom((envelope) => {
							appendedTypes.push(envelope.event.type);
							return {
								ok: true,
								eventId: `bridge-${envelope.writeId}`,
								type: "committed",
								eventSequence: 1,
							};
						}),
					}),
				),
			),
		);
		expect(result).toEqual({ type: "completed", modelMessageCount: 0 });
		expect(appendedTypes).toEqual([]);
	});
	test("a second warm turn preserves model and ContextManager state without context reads", async () => {
		const loader = new QueuedContextLoader([], []);
		const session = new ThreadRuntime("sesn_1");
		const layer = runtimeThreadLoopLayer(loader, { installLoaderState: false });
		session.state.enqueueAcceptedInput(acceptedInput("rin_warm_first"));
		const first = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(Effect.provide(layer)),
		);
		session.state.enqueueAcceptedInput(acceptedInput("rin_warm_second"));
		const second = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(Effect.provide(layer)),
		);
		const third = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(Effect.provide(layer)),
		);
		expect(first).toMatchObject({
			type: "completed",
			currentModel: { providerId: "fake", modelId: "fake-chat" },
		});
		expect(second).toMatchObject({
			type: "completed",
			currentModel: { providerId: "fake", modelId: "fake-chat" },
		});
		expect(third).toMatchObject({
			type: "completed",
			currentModel: { providerId: "fake", modelId: "fake-chat" },
		});
		expect(loader.buildCalls).toEqual([]);
		expect(loader.pendingCalls).toEqual([]);
		expect(
			session.state.contextManager
				.entries()
				.map((message) => message.contextKind),
		).toEqual(["user", "assistant", "user", "assistant"]);
	});
	test("projection failure returns a failed run instead of masking invalid context as completed", async () => {
		const loader = new RecordingContextLoader([], { type: "empty" });
		const session = new ThreadRuntime("sesn_1");
		session.state.contextManager.appendEntry({
			id: "system-1",
			sessionId: "sesn_1",
			role: "system",
			sequence: 0,
			status: "completed",
			createdAt,
			parts: [
				{
					id: "system-1-text",
					sessionId: "sesn_1",
					messageId: "system-1",
					sequence: 0,
					type: "text",
					text: "system prompt",
					truncated: false,
					status: "completed",
					createdAt,
				},
			],
		} as unknown as RuntimeContextEntry);
		session.state.markPersistentContextLoaded();
		const appendedTypes: string[] = [];
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer: writerFrom((envelope) => {
							appendedTypes.push(envelope.event.type);
							return {
								ok: true,
								eventId: `bridge-${envelope.writeId}`,
								type: "committed",
								eventSequence: 1,
							};
						}),
					}),
				),
			),
		);
		expect(result).toMatchObject({
			type: "failed",
			releaseSession: { reason: "crashed" },
			error: {
				code: "provider_invalid_request",
				message:
					"Runtime context is not valid for Gateway ProviderRequest: schema.",
			},
		});
		expect(appendedTypes).toEqual([]);
		expect(session.state.contextManager.entries()).toEqual([]);
	});
	test("runtime layer gates assistant progress hot context on durable event ACKs", async () => {
		const order: string[] = [];
		const session = new ThreadRuntime("sesn_1");
		const store = new ThreadLoopRuntimeStore(order);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		let providerSawShell = false;
		const writer = writerFrom((envelope) => {
			const openAssistant = session.state.contextManager.openRequestDraft();
			const sealedAssistant = session.state.contextManager
				.entries()
				.find((entry) => entry.contextKind === "assistant");
			order.push(
				`event:${envelope.event.type}:open_parts_${openAssistant?.parts.length ?? 0}:sealed_parts_${sealedAssistant?.parts.length ?? 0}`,
			);
			return {
				ok: true,
				eventId: `bridge-${envelope.writeId}`,
				type: "committed",
				eventSequence: 1,
			};
		});
		await installLoaderStateForTest(loader, session);
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						store,
						writer,
						llmService: llmService(
							[
								{ type: "text-start", id: "text-1" },
								{ type: "text-delta", id: "text-1", text_delta: "hello" },
								{ type: "text-end", id: "text-1" },
								{ type: "finish", finishReason: "tool-calls" },
							],
							() => {
								const assistant =
									session.state.contextManager.openRequestDraft();
								providerSawShell =
									assistant !== undefined && assistant.parts.length === 0;
								order.push("provider:stream");
							},
						),
						providerCallRuntime: {
							...DefaultProviderCallRuntimeConfig,
							timeoutMs: 1800000,
						},
						runtimeModel: () => ({ providerId: "fake", modelId: "fake-chat" }),
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed", modelMessageCount: 1 });
		expect(providerSawShell).toBe(false);
		expect(order).toEqual([
			"event:session.status_running:open_parts_0:sealed_parts_0",
			"event:span.model_request_start:open_parts_0:sealed_parts_0",
			"provider:stream",
			"event:agent.message:open_parts_0:sealed_parts_0",
			"event:span.model_request_end:open_parts_1:sealed_parts_0",
			"event:session.status_idle:open_parts_0:sealed_parts_1",
		]);
		expect(
			session.state.contextManager
				.entries()
				.map((message) => message.contextKind),
		).toEqual(["user", "assistant"]);
		expect(session.state.contextManager.entries().at(-1)?.parts).toEqual([
			expect.objectContaining({ type: "text", text: "hello" }),
		]);
	});
	test("hot input reconciliation preserves a full retry ride and advances new media at the next safe request", async () => {
		const session = new ThreadRuntime("sesn_hot_attachment_reconcile");
		const activeRide = Array.from({ length: 32 }, (_, index) => ({
			transient: {
				attachmentRef: `att_hot_active_${index}`,
				sourcePath: `mcp:github/hot-active-${index}.png`,
				pageRange: "",
				detail: "auto" as const,
			},
			fileBacked: undefined,
			mime: "image/png",
			filename: `hot-active-${index}.png`,
		}));
		const nextRide = {
			transient: {
				attachmentRef: "att_hot_next",
				sourcePath: "mcp:github/hot-next.png",
				pageRange: "",
				detail: "auto" as const,
			},
			fileBacked: undefined,
			mime: "image/png",
			filename: "hot-next.png",
		};
		session.state.addPendingAttachments(activeRide);
		const followUp = acceptedInput("rin_hot_attachment_follow_up");
		const firstMessage = userMessage("user-hot-1", 0, "first");
		const loader = new QueuedContextLoader(
			[],
			[{ type: "context", entries: [firstMessage] }],
			[],
		);
		const requests: LLMRequest[] = [];
		const llm: LLMServiceInterface = {
			stream(request) {
				requests.push(request);
				if (requests.length === 1) {
					session.state.addPendingAttachments([nextRide]);
					session.state.enqueueAcceptedInput(followUp);
					return Stream.fromIterable([
						{
							type: "provider-error",
							error: runtimeFailureFromProviderError(
								normalizeProviderError({
									code: "provider_unavailable",
									message: "retry after hot input",
									retryable: true,
									fatal: false,
								}),
							),
						},
					]);
				}
				return Stream.fromIterable([
					{ type: "text-start", id: "text-hot-retry" },
					{ type: "text-delta", id: "text-hot-retry", text_delta: "done" },
					{ type: "text-end", id: "text-hot-retry" },
					{ type: "finish", finishReason: "stop" },
				]);
			},
		};
		const baseWriter = writerFrom((envelope) => ({
			ok: true,
			eventId: `bridge-${envelope.writeId}`,
			type: "committed",
			eventSequence: 1,
		}));
		const writer: SessionEventWriter = {
			...baseWriter,
			writeRequestEnd: async (envelope) =>
				await baseWriter.writeRequestEnd(envelope),
		};
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						llmService: llm,
						runtimePolicy: () => ({
							providerRescheduleBudget: 3,
							compactionRescheduleBudget: 2,
						}),
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed" });
		expect(requests.map((request) => request.attachments)).toEqual([
			providerAttachmentsForTest(activeRide),
			providerAttachmentsForTest(activeRide),
			providerAttachmentsForTest([nextRide]),
		]);
		expect(loader.commitCalls.map((input) => input.runtimeInputId)).toEqual([
			expect.stringMatching(/^rin_test_harness_/),
			followUp.runtimeInputId,
		]);
		expect(session.state.peekAcceptedInput()).toBeUndefined();
		expect(session.state.pendingAttachments()).toEqual([]);
	});
	test("running status append failure stops before accepted input commit", async () => {
		const session = new ThreadRuntime("sesn_1");
		const followUp = acceptedInput();
		session.state.enqueueAcceptedInput(followUp);
		const loader = new QueuedContextLoader(
			[],
			[],
			[
				{
					type: "context",
					entries: [userMessage("user-accepted", 2, "accepted")],
				},
			],
		);
		const appendedTypes: string[] = [];
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer: failingEventWriter(
							appendedTypes,
							(event) => event.type === "session.status_running",
						),
					}),
				),
			),
		);
		expect(result).toMatchObject({
			type: "failed",
			error: { type: "session-event-writer", code: "unavailable" },
			releaseSession: { reason: "event_write_failed" },
		});
		expect(appendedTypes).toEqual(["session.status_running"]);
		expect(loader.commitCalls).toEqual([]);
		expect(session.state.peekAcceptedInput()).toEqual(followUp);
	});
	test("runtime layer admits an input accepted during an empty request before idle closeout", async () => {
		const session = new ThreadRuntime("sesn_1");
		const followUp = acceptedInput();
		const loader = new QueuedContextLoader(
			[],
			[{ type: "context", entries: [userMessage("user-1", 0, "first")] }],
			[],
		);
		const capturedRequests: LLMRequest[] = [];
		const runtimeBoundary: ProviderCallRuntimeConfig = {
			systemInstructions: "third group hot follow-up system",
			toolCatalog: catalogForTest({
				name: "third_group_follow_up",
				description: "follow-up tool",
				inputSchema: { type: "object", properties: {} },
			}),
			maxOutputTokens: 222,
		};
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						providerCallRuntime: runtimeBoundary,
						onStream: (request) => {
							capturedRequests.push(request);
							if (capturedRequests.length === 1) {
								session.state.enqueueAcceptedInput(followUp);
							}
						},
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed" });
		expect(capturedRequests).toHaveLength(2);
		for (const request of capturedRequests) {
			expect(request.system).toEqual([
				{
					kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
					text: runtimeBoundary.systemInstructions,
					cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE,
				},
			]);
			expect(request.tools).toEqual([
				{
					name: "third_group_follow_up",
					description: "follow-up tool",
					function: { inputSchemaJson: '{"type":"object","properties":{}}' },
				},
			]);
			expect(request.limits?.maxOutputTokens).toBe(222);
		}
		expect(capturedRequests[0]?.context.map((message) => message.role)).toEqual(
			[ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER],
		);
		expect(session.state.peekAcceptedInput()).toBeUndefined();
		expect(loader.pendingCalls).toEqual(["sesn_1"]);
		expect(loader.commitCalls.map((input) => input.runtimeInputId)).toEqual([
			expect.stringMatching(/^rin_test_harness_/),
			followUp.runtimeInputId,
		]);
	});
	test("runtime layer updates non-text hot context only after the matching ACK boundary", async () => {
		const order: string[] = [];
		const session = new ThreadRuntime("sesn_1");
		const store = new ThreadLoopRuntimeStore(order);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const toolStatus = (): string => {
			const parts = [
				...session.state.contextManager
					.entries()
					.flatMap((entry) => entry.parts),
				...(session.state.contextManager.openRequestDraft()?.parts ?? []),
			];
			const result = parts.find((part) => part.type === "tool_result");
			if (result?.type === "tool_result") return result.result.type;
			return parts.some((part) => part.type === "tool_call")
				? "running"
				: "missing";
		};
		const writer = writerFrom(
			(envelope) => {
				order.push(`event:${envelope.event.type}:tool_${toolStatus()}`);
				return {
					ok: true,
					eventId:
						envelope.event.type === "agent.tool_use"
							? "bridge-tool"
							: `bridge-${envelope.writeId}`,
					type: "committed",
					eventSequence: 1,
				};
			},
			undefined,
			[],
			undefined,
			async (envelope) => {
				order.push(
					`settlement:${envelope.settlement.outcome.type}:tool_${toolStatus()}`,
				);
				return { ok: true, result: { type: "committed" } };
			},
		);
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						store,
						writer,
						events: [
							{ type: "step-start", stepIndex: 1 },
							{ type: "step-finish", finishReason: "tool-calls" },
							{ type: "reasoning-start", id: "reasoning-1" },
							{
								type: "reasoning-delta",
								id: "reasoning-1",
								text_delta: "thinking",
							},
							{ type: "reasoning-end", id: "reasoning-1" },
							{
								type: "tool-call",
								id: "tool-1",
								toolName: "search",
								input: { q: "x" },
								inputPreview: { preview: '{"q":"x"}', truncated: false },
							},
							{ type: "finish", finishReason: "tool-calls" },
						],
						providerCallRuntime: {
							systemInstructions: "tool test system",
							toolCatalog: catalogForTest({
								name: "search",
								description: "Search test tool",
								inputSchema: {
									type: "object",
									properties: { q: { type: "string" } },
								},
							}),
						},
						runTool: () => ({
							type: "completed",
							output: { text: "done", truncated: false },
						}),
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed" });
		expect(order).toEqual([
			"event:session.status_running:tool_missing",
			"event:span.model_request_start:tool_missing",
			"event:agent.thinking:tool_missing",
			"event:agent.tool_use:tool_missing",
			"event:span.model_request_end:tool_running",
			"settlement:completed:tool_running",
			"event:span.model_request_start:tool_completed",
			"event:agent.message:tool_completed",
			"event:span.model_request_end:tool_completed",
			"event:session.status_idle:tool_completed",
		]);
		expect(
			session.state.contextManager
				.entries()
				.find((message) =>
					message.parts.some((part) => part.type === "tool_call"),
				),
		).toMatchObject({
			contextKind: "assistant",
			parts: expect.arrayContaining([
				expect.objectContaining({ type: "reasoning", text: "thinking" }),
				expect.objectContaining({
					type: "tool_call",
					modelToolCallId: "tool-1",
				}),
				expect.objectContaining({
					type: "tool_result",
					modelToolCallId: "tool-1",
					result: expect.objectContaining({ type: "completed" }),
				}),
			]),
		});
	});
	test("runtime layer keeps unacked progress out of hot context and requests hot-state discard", async () => {
		const order: string[] = [];
		const session = new ThreadRuntime("sesn_1");
		const store = new ThreadLoopRuntimeStore(order);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const appendedTypes: string[] = [];
		const writer = failingEventWriter(
			appendedTypes,
			(event) => event.type === "agent.message",
		);
		await installLoaderStateForTest(loader, session);
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					ThreadLoop.runtimeLayer({
						internalToolRepairStore: store,
						sessionEventWriter: writer,
						runtime: threadLoopRuntime(),
						llmService: llmService([
							{ type: "text-start", id: "text-1" },
							{ type: "text-delta", id: "text-1", text_delta: "hello" },
							{ type: "text-end", id: "text-1" },
						]),
						storeOperationTimeoutMs: 1000,
						providerCallRuntime: {
							...DefaultProviderCallRuntimeConfig,
							timeoutMs: 1800000,
						},
						runtimeModel: () => ({ providerId: "fake", modelId: "fake-chat" }),
					}).pipe(Layer.provide(ThreadLoop.contextLoaderLayer(loader))),
				),
			),
		);
		expect(result).toMatchObject({
			type: "failed",
			error: { type: "session-event-writer", code: "unavailable" },
			releaseSession: { reason: "event_write_failed" },
		});
		expect(appendedTypes).toEqual([
			"session.status_running",
			"span.model_request_start",
			"agent.message",
			"span.model_request_end",
			"session.error",
		]);
		expect(
			session.state.contextManager
				.entries()
				.some((message) => message.contextKind === "assistant"),
		).toBe(false);
	});
});

const timestamp = "2026-01-01T00:00:00.000Z";

describe("ThreadState", () => {
	test("successful request hints stay paired and an actual model change invalidates both", () => {
		const state = new ThreadState("sesn_compaction_hints");
		state.updateCurrentModel({ providerId: "openai", modelId: "gpt-5.5" });
		state.recordLastRequestCompletion(
			{
				inputTokens: 300_000,
				outputTokens: 2_000,
				reasoningTokens: 50_000,
				cacheReadTokens: 1_000,
				cacheWriteTokens: 500,
			},
			{
				contextWindowTokens: 400_000,
				inputLimitTokens: 272_000,
				outputTokenLimit: 128_000,
			},
			41,
		);

		state.updateCurrentModel({ providerId: "openai", modelId: "gpt-5.5" });
		expect(state.lastRequestUsage()).toMatchObject({ inputTokens: 300_000 });
		expect(state.lastRequestModelLimits()).toEqual({
			contextWindowTokens: 400_000,
			inputLimitTokens: 272_000,
			outputTokenLimit: 128_000,
		});
		expect(state.lastRequestContextAnchorSequence()).toBe(41);

		state.updateCurrentModel({
			providerId: "anthropic",
			modelId: "claude-fable-5",
		});
		expect(state.lastRequestUsage()).toBeUndefined();
		expect(state.lastRequestModelLimits()).toBeUndefined();
		expect(state.lastRequestContextAnchorSequence()).toBeUndefined();
	});

	test("caps pending provider attachments", () => {
		const state = new ThreadState("sesn_attachments");
		state.addPendingAttachments(
			Array.from({ length: MaxProviderAttachments + 3 }, (_, index) => ({
				transient: {
					attachmentRef: `att_${index}`,
					sourcePath: `/tmp/image-${index}.png`,
					pageRange: "",
					detail: "auto",
				},
				fileBacked: undefined,
				mime: "image/png",
				filename: `image-${index}.png`,
			})),
		);

		expect(state.pendingAttachments()).toHaveLength(MaxProviderAttachments);
		expect(state.pendingAttachments().at(-1)?.transient?.attachmentRef).toBe(
			"att_31",
		);
	});

	test("owns pending attachment origins independently of caller snapshots", () => {
		const state = new ThreadState("sesn_attachment_ownership");
		const attachment = {
			transient: {
				attachmentRef: "att_original",
				sourcePath: "/tmp/image.png",
				pageRange: "",
				detail: "auto",
			},
			fileBacked: undefined,
			mime: "image/png",
			filename: "image.png",
		};

		state.addPendingAttachments([attachment]);
		attachment.transient.attachmentRef = "att_mutated_input";
		const snapshot = state.pendingAttachments();
		(snapshot[0]!.transient as { attachmentRef: string }).attachmentRef =
			"att_mutated_output";

		expect(state.pendingAttachments()[0]?.transient?.attachmentRef).toBe(
			"att_original",
		);
	});

	test("does not duplicate a file attachment installed by cold load before input commit", () => {
		const state = new ThreadState("sesn_attachment_cold_commit_overlap");
		const attachment = {
			transient: undefined,
			fileBacked: {
				sourceEventId: "sevt_attachment_cold_commit_overlap",
				fileId: "file_attachment_cold_commit_overlap",
			},
			mime: "image/png",
			filename: "overlap.png",
		};

		state.replacePendingAttachments([attachment]);
		state.addPendingAttachments([attachment]);

		expect(state.pendingAttachments()).toEqual([attachment]);
	});

	test("derives attachment readiness from ThreadState without adding Message context", () => {
		const attachment = {
			transient: undefined,
			fileBacked: {
				sourceEventId: "sevt_attachment_state",
				fileId: "file_attachment_state",
			},
			mime: "image/png",
			filename: "attachment.png",
		};
		const hot = new ThreadState("sesn_attachment_hot");
		hot.installThreadTurn(
			{
				executionRunId: "run_attachment_hot",
				pendingInputContextSequences: [],
			},
			{ routes: [] },
		);
		hot.enqueueAcceptedInput(
			acceptedInput("rin_attachment_hot", "sesn_attachment_hot"),
		);
		hot.enqueueAcceptedInput(
			acceptedInput("rin_sibling_hot", "sesn_attachment_hot"),
		);
		hot.addPendingAttachments([attachment]);
		hot.acknowledgeAcceptedInput("rin_attachment_hot");

		expect(
			hot.applyThreadTurnFact({
				fact: "inputs_committed",
				eventId: "sevt_attachment_committed",
				contextSequences: [],
			}).action,
		).toEqual({
			action: "commit_accepted_input",
			runtimeInputId: "rin_sibling_hot",
		});
		expect(hot.threadTurnReduction().checkpoint).toEqual({
			executionRunId: "run_attachment_hot",
			pendingInputContextSequences: [],
		});

		const cold = new ThreadState("sesn_attachment_cold");
		cold.installThreadTurn(
			{
				executionRunId: "run_attachment_cold",
				pendingInputContextSequences: [],
			},
			{ routes: [] },
		);
		cold.replacePendingAttachments([attachment]);
		expect(cold.threadTurnReduction()).toMatchObject({
			checkpoint: {
				executionRunId: "run_attachment_cold",
				pendingInputContextSequences: [],
			},
			state: { state: "ready_to_request" },
			action: { action: "prepare_next_request" },
		});
	});

	test("commits a text sibling before sending one request with its pending attachment", async () => {
		const session = new ThreadRuntime("sesn_attachment_sibling_request");
		const attachmentInput = {
			...acceptedInput(
				"rin_attachment_sibling_first",
				"sesn_attachment_sibling_request",
			),
			contentJson: JSON.stringify({ messages: [{ parts: [] }] }),
		};
		const textInput = acceptedInput(
			"rin_attachment_sibling_text",
			"sesn_attachment_sibling_request",
		);
		const attachment = {
			transient: undefined,
			fileBacked: {
				sourceEventId: "sevt_attachment_sibling",
				fileId: "file_attachment_sibling",
			},
			mime: "image/png",
			filename: "sibling.png",
		};
		expect(session.state.enqueueAcceptedInput(attachmentInput)).toBe("applied");
		expect(session.state.enqueueAcceptedInput(textInput)).toBe("applied");
		const loader = new QueuedContextLoader(
			[],
			[],
			[
				{
					type: "committed",
					assignedContextSequences: [],
					pendingAttachments: [attachment],
					interruptToolResults: [],
				},
			],
		);
		const requests: LLMRequest[] = [];
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						onStream: (request) => requests.push(request),
					}),
				),
			),
		);

		expect(result).toMatchObject({ type: "completed" });
		expect(loader.commitCalls.map((input) => input.runtimeInputId)).toEqual([
			attachmentInput.runtimeInputId,
			textInput.runtimeInputId,
		]);
		expect(requests).toHaveLength(1);
		expect(requests[0]?.context).toEqual([
			{
				role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
				content: [{ text: { text: "test input" } }],
			},
		]);
		expect(requests[0]?.attachments).toEqual(
			providerAttachmentsForTest([attachment]),
		);
	});

	test("agent-mail delivery identity remains reserved until Request Start", async () => {
		const state = new ThreadState("sesn_agent_mail_dedup");
		const mail = {
			workspaceId: "wksp_agent_mail",
			sessionId: "sesn_agent_mail_dedup",
			sessionThreadId: "thrd_agent_mail_main",
			bindingId: "bind_agent_mail",
			bindingGeneration: 1,
			targetPodUid: "pod_agent_mail",
			runtimeInputId: "agent_mail:delivery_agent_mail",
			kind: "inter_agent_message",
			deliveryId: "delivery_agent_mail",
			content:
				"Message Type: FINAL_ANSWER\nTask name: main\nSender: worker\nPayload:\ndone",
		} satisfies RuntimeAcceptedInputState;

		expect(state.enqueueAcceptedInput(mail)).toBe("applied");
		expect(state.enqueueAcceptedInput(mail)).toBe("duplicate");
		expect(state.enqueueAcceptedInput({ ...mail, bindingGeneration: 2 })).toBe(
			"conflict",
		);
		state.acknowledgeAcceptedInput(mail.runtimeInputId, true);
		let settled = false;
		const requestOpeningSettled = state
			.requestOpeningSettlement(mail.runtimeInputId)
			.then(() => {
				settled = true;
			});
		expect(state.peekAcceptedInput()).toBeUndefined();
		expect(state.hasAcceptedInputCustody()).toBe(true);
		expect(state.enqueueAcceptedInput(mail)).toBe("duplicate");
		await Promise.resolve();
		expect(settled).toBe(false);
		state.clear();
		await Promise.resolve();
		expect(settled).toBe(false);
		expect(state.enqueueAcceptedInput(mail)).toBe("duplicate");
		state.completeRequestOpening();
		await requestOpeningSettled;
		expect(settled).toBe(true);
		expect(state.hasAcceptedInputCustody()).toBe(false);
		expect(state.enqueueAcceptedInput(mail)).toBe("applied");
		expect(
			state.enqueueAcceptedInput({ ...mail, deliveryId: "delivery_new" }),
		).toBe("conflict");
	});

	test("ordinary accepted input does not acquire agent-mail opening custody", () => {
		const state = new ThreadState("sesn_ordinary_opening_custody");
		const input = acceptedInput(
			"rin_ordinary_opening_custody",
			"sesn_ordinary_opening_custody",
		);

		expect(state.enqueueAcceptedInput(input)).toBe("applied");
		state.acknowledgeAcceptedInput(input.runtimeInputId, true);
		expect(state.acceptedInputCount()).toBe(0);
		expect(state.hasAcceptedInputCustody()).toBe(false);
	});

	test("ordinary subagent mail does not reserve opening custody after durable commit", async () => {
		const session = new ThreadRuntime({
			workspaceId: "wksp_ordinary_subagent_mail",
			sessionId: "sesn_ordinary_subagent_mail",
			sessionThreadId: "thrd_ordinary_subagent_mail",
			threadRole: "subagent",
			bindingId: "bind_ordinary_subagent_mail",
			bindingGeneration: 1,
			targetPodUid: "pod_ordinary_subagent_mail",
			runtimeBindingToken: "token_ordinary_subagent_mail",
		});
		session.state.contextManager.appendEntry(
			userMessage("msg_opening_subagent_mail", 1, "opening input"),
		);
		session.state.markPersistentContextLoaded();
		session.state.installThreadTurn(
			{
				pendingInputContextSequences: [],
				idleCloseout: {
					eventId: "sevt_ordinary_subagent_mail_idle",
					stopReason: "end_turn",
				},
			},
			{ routes: [] },
		);
		const mail = {
			workspaceId: session.identity.workspaceId,
			sessionId: session.sessionId,
			sessionThreadId: session.identity.sessionThreadId,
			bindingId: session.identity.bindingId,
			bindingGeneration: session.identity.bindingGeneration,
			targetPodUid: session.identity.targetPodUid,
			runtimeInputId: "agent_mail:delivery_ordinary_subagent_mail",
			kind: "inter_agent_message",
			deliveryId: "delivery_ordinary_subagent_mail",
			content: "ordinary follow-up",
		} satisfies RuntimeAcceptedInputState;
		expect(session.state.enqueueAcceptedInput(mail)).toBe("applied");
		const appended: string[] = [];
		const loader = new QueuedContextLoader([], [], [
			{
				type: "committed",
				assignedContextSequences: [2],
				pendingAttachments: [],
				interruptToolResults: [],
			},
		]);

		const result = await Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						installLoaderState: false,
						writer: failingEventWriter(
							appended,
							(event) => event.type === "span.model_request_start",
						),
					}),
				),
			),
		);

		expect(result).toMatchObject({ type: "failed" });
		expect(loader.commitCalls).toHaveLength(1);
		expect(session.state.acceptedInputCount()).toBe(0);
		expect(session.state.hasAcceptedInputCustody()).toBe(false);
	});

	test("does not reopen a request for an already resident committed mail", async () => {
		const session = new ThreadRuntime("sesn_agent_mail_resident_replay");
		session.state.contextManager.appendEntry(
			userMessage("msg_agent_mail_resident", 1, "already resident"),
		);
		session.state.markPersistentContextLoaded();
		session.state.installThreadTurn(
			{
				pendingInputContextSequences: [],
				idleCloseout: {
					eventId: "sevt_agent_mail_resident_idle",
					stopReason: "end_turn",
				},
			},
			{ routes: [] },
		);
		const mail = {
			workspaceId: "wksp_agent_mail_resident_replay",
			sessionId: "sesn_agent_mail_resident_replay",
			sessionThreadId: "thrd_agent_mail_resident_replay",
			bindingId: "bind_agent_mail_resident_replay",
			bindingGeneration: 1,
			targetPodUid: "pod_agent_mail_resident_replay",
			runtimeInputId: "agent_mail:delivery_resident_replay",
			kind: "inter_agent_message",
			deliveryId: "delivery_resident_replay",
			content: "already resident",
		} satisfies RuntimeAcceptedInputState;
		expect(session.state.enqueueAcceptedInput(mail)).toBe("applied");
		const loader = new QueuedContextLoader(
			[],
			[],
			[
				{
					type: "committed",
					assignedContextSequences: [1],
					pendingAttachments: [],
					interruptToolResults: [],
				},
			],
		);
		const requests: LLMRequest[] = [];

		const result = await Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						installLoaderState: false,
						onStream: (request) => requests.push(request),
					}),
				),
			),
		);

		expect(result).toMatchObject({ type: "completed" });
		expect(requests).toEqual([]);
		expect(session.state.hasAcceptedInputCustody()).toBe(false);
	});

	test("interrupt fence preserves queued stamped completion mail", () => {
		const state = new ThreadState("sesn_agent_mail_interrupt_fence");
		const mail = {
			workspaceId: "wksp_agent_mail_interrupt_fence",
			sessionId: "sesn_agent_mail_interrupt_fence",
			sessionThreadId: "thrd_agent_mail_main",
			bindingId: "bind_agent_mail_interrupt_fence",
			bindingGeneration: 1,
			targetPodUid: "pod_agent_mail_interrupt_fence",
			runtimeInputId: "agent_mail:delivery_interrupt_fence",
			kind: "inter_agent_message",
			deliveryId: "delivery_interrupt_fence",
			content: "completion",
		} satisfies RuntimeAcceptedInputState;

		expect(state.enqueueAcceptedInput(mail)).toBe("applied");
		state.discardQueuedAcceptedInputsForInterrupt(true);

		expect(state.peekAcceptedInput()).toEqual(mail);
		expect(state.enqueueAcceptedInput(mail)).toBe("duplicate");
	});

	test("task-notification identity deduplicates while the accepted fact remains queued", () => {
		const state = new ThreadState("sesn_task_notification_queued");
		const notification = taskNotificationInput(
			"rin_task_notification_queued",
			"task_notification_queued",
			"sevt_task_notification_source",
			"completed",
			'{"status":"completed"}',
			"sesn_task_notification_queued",
		);

		expect(state.enqueueAcceptedInput(notification)).toBe("applied");
		expect(state.enqueueAcceptedInput(notification)).toBe("duplicate");
		expect(state.peekAcceptedInput()).toBe(notification);
	});

	test("task-notification identity deduplicates after its durable message is committed", () => {
		const state = new ThreadState("sesn_task_notification_committed");
		const committedEntry = RuntimeContextEntrySchema.parse({
			messageSequence: 4,
			contextKind: "runtime_notification",
			parts: [{ type: "text", text: "Task completed." }],
		});
		const { kind: _kind, ...notificationCommand } = taskNotificationInput(
			"rin_task_notification_committed",
			"task_notification_committed",
			"sevt_task_notification_source",
			"completed",
			'{"status":"completed"}',
			"sesn_task_notification_committed",
		);
		const notification = {
			...notificationCommand,
			committedEntry,
		} satisfies RuntimeTaskNotificationState;

		expect(state.commitTaskNotification(notification)).toBe("applied");
		expect(state.commitTaskNotification(notification)).toBe("duplicate");
		expect(state.contextManager.entries()).toEqual([committedEntry]);
	});

	test("clear removes pending attachments", () => {
		const state = new ThreadState("sesn_clear");
		state.installThreadTurn(
			{
				executionRunId: "run_clear",
				pendingInputContextSequences: [],
			},
			{ routes: [] },
		);
		state.addPendingAttachments([
			{
				transient: {
					attachmentRef: "att_1",
					sourcePath: "/tmp/image.png",
					pageRange: "",
					detail: "auto",
				},
				fileBacked: undefined,
				mime: "image/png",
				filename: "image.png",
			},
		]);
		expect(state.threadTurnReduction().action).toEqual({
			action: "prepare_next_request",
		});
		state.clear();

		expect(state.pendingAttachments()).toEqual([]);
		expect(state.threadTurnReduction()).toMatchObject({
			state: { state: "ready_to_finish" },
			action: { action: "finish_idle" },
		});
	});

	test("generic failure cleanup preserves accepted custody until a durable handoff", () => {
		const state = new ThreadState("sesn_clear_accepted");
		const input = acceptedInput("rin_clear_accepted", "sesn_clear_accepted");
		state.enqueueAcceptedInput(input);

		state.clear();
		expect(state.acceptedInputSnapshot()).toEqual([input]);

		state.clearAfterCustodyHandoff();
		expect(state.acceptedInputSnapshot()).toEqual([]);
	});
});
