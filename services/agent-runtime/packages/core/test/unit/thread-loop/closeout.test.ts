import { describe, expect, test } from "bun:test";
import {
	Cause,
	Context,
	Effect,
	Exit,
	Fiber,
	Layer,
	Scope,
	Stream,
} from "effect";
import type { AcceptedInputCommitResult } from "../../../src/context/context-loader.js";
import { normalizeProviderError } from "../../../src/contracts/provider.js";
import type {
	RuntimeDependencies,
	SessionEvent,
	SessionEventWriter,
	SessionEventWriterAppendResult,
	SessionEventWriterRequestEndEnvelope,
	SessionEventWriterRuntimeTerminationEnvelope,
} from "../../../src/contracts/runtime.js";
import {
	normalizeSessionEventWriterError,
	SessionEventWriterRetryPolicy,
} from "../../../src/contracts/runtime.js";
import type { LLMEvent } from "../../../src/llm/llm-event.js";
import { runtimeFailureFromProviderError } from "../../../src/llm/llm-event.js";
import type {
	LLMRequest,
	LLMServiceError,
	Interface as LLMServiceInterface,
} from "../../../src/llm/llm-service.js";
import { ProviderStreamAccumulator } from "../../../src/runtime/accumulator.js";
import * as SessionManager from "../../../src/session/session-manager.js";
import { assembleProviderCallRequest } from "../../../src/thread-loop/provider-request.js";
import * as ThreadLoop from "../../../src/thread-loop/thread-loop.js";
import { ThreadRuntime } from "../../../src/thread-loop/thread-runtime.js";
import type { RuntimeAcceptedInputState } from "../../../src/thread-loop/thread-state.js";
import type { TestContextLoader } from "./thread-loop-test-support.js";
import {
	acceptedInput,
	acceptedInputCommitResult,
	approvalReviewAcceptedInput,
	beginTestUserInterrupt,
	buildRuntimeControlCommitResult,
	catalogForTest,
	createdAt,
	deferred,
	expectNoProviderDiagnosticCanaries,
	failingEventWriter,
	flushMicrotasks,
	interruptInput,
	QueuedContextLoader,
	queuedLLMService,
	RecordingContextLoader,
	requestEndResultForTest,
	runtimeTerminationResultForTest,
	runtimeThreadLoopLayer,
	sleepUntilAborted,
	ThreadLoopRuntimeStore,
	testControlCommit,
	testRunCustody,
	threadLoopRuntime,
	userMessage,
	waitForCondition,
	waitForReleaseOrAbort,
	withFinishIdleResultForTest,
	writerFrom,
} from "./thread-loop-test-support.js";

describe("ThreadLoop", () => {
	function threadWithActiveTurn(
		sessionId: string,
		executionRunId: string,
	): ThreadRuntime {
		const session = new ThreadRuntime(sessionId);
		session.state.installThreadTurn(
			{ executionRunId, pendingInputContextSequences: [] },
			{ routes: [] },
		);
		return session;
	}

	test("failed-run closeout writes a sanitized terminal error before idle settlement", async () => {
		const appended: SessionEvent[] = [];
		const writeIds: string[] = [];
		const loader = new RecordingContextLoader([], { type: "empty" });
		const session = new ThreadRuntime("sesn_failed_closeout");
		session.state.installThreadTurn(
			{
				executionRunId: "evt_failed_closeout_running",
				pendingInputContextSequences: [],
			},
			{ routes: [] },
		);
		const defectCanary = "FAILED_RUN_DEFECT_SECRET_CANARY";
		await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				const closeout = threadLoop.closeFailedRun(
					session,
					new Error(defectCanary),
					testRunCustody(),
				);
				expect(yield* closeout).toEqual({ type: "landed" });
				expect(yield* closeout).toEqual({ type: "landed" });
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer: writerFrom((envelope) => {
							appended.push(envelope.event);
							writeIds.push(envelope.writeId);
							return {
								ok: true,
								eventId: `bridge-${envelope.writeId}`,
								type: "committed",
							};
						}),
						runtime: { ...threadLoopRuntime(), sleep: sleepUntilAborted },
					}),
				),
			),
		);
		expect(appended.map((event) => event.type)).toEqual([
			"session.error",
			"session.status_idle",
		]);
		expect(appended[0]).toMatchObject({
			type: "session.error",
			error: {
				code: "runtime_invalid_sequence",
				retryStatus: { type: "terminal" },
			},
		});
		expect(appended[1]).toEqual({
			type: "session.status_idle",
			stop_reason: { type: "end_turn" },
		});
		expect(writeIds[0]).not.toBe(writeIds[1]);
		expect(JSON.stringify(appended)).not.toContain(defectCanary);
		expect(session.state.threadTurnReduction()).toMatchObject({
			checkpoint: { idleCloseout: { stopReason: "end_turn" } },
			state: { state: "idle" },
			action: { action: "await_input" },
		});
	});
	test("failed-run closeout with accepted custody uses atomic Runtime termination", async () => {
		const session = new ThreadRuntime("sesn_failed_closeout_accepted");
		const unresolved = acceptedInput(
			"rin_failed_closeout_accepted",
			session.sessionId,
		);
		session.state.installThreadTurn(
			{
				executionRunId: "evt_failed_closeout_accepted_running",
				pendingInputContextSequences: [],
			},
			{ routes: [] },
		);
		session.state.enqueueAcceptedInput(unresolved);
		const appended: SessionEvent[] = [];
		const terminations: SessionEventWriterRuntimeTerminationEnvelope[] = [];
		const baseWriter = writerFrom((envelope) => {
			appended.push(envelope.event);
			return {
				ok: true,
				eventId: `bridge-${envelope.writeId}`,
				type: "committed",
			};
		});
		const writer: SessionEventWriter = {
			...baseWriter,
			commitRuntimeTermination: async (envelope) => {
				terminations.push(envelope);
				return await baseWriter.commitRuntimeTermination!(envelope);
			},
		};
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).closeFailedRun(
					session,
					new Error("failed run"),
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(
						new RecordingContextLoader([], { type: "empty" }),
						{
							writer,
							runtime: { ...threadLoopRuntime(), sleep: sleepUntilAborted },
						},
					),
				),
			),
		);

		expect(result).toEqual({ type: "landed" });
		expect(appended).toEqual([]);
		expect(terminations).toHaveLength(1);
		expect(terminations[0]).toMatchObject({
			writeId: "evt_failed_closeout_accepted_running",
			failure: {
				type: "runtime",
				code: "runtime_invalid_sequence",
				reason: "runtime_contract_validation",
				retryStatus: { type: "terminal" },
			},
		});
		expect(session.state.threadTurnReduction()).toMatchObject({
			checkpoint: { terminalCloseout: { disposition: "terminated" } },
			state: { state: "idle" },
			action: { action: "await_input" },
		});
		expect(session.state.acceptedInputSnapshot()).toEqual([unresolved]);
	});
	test("failed-run closeout observes one in-flight step across timeout windows and memoizes success", async () => {
		const errorResult = deferred<SessionEventWriterAppendResult>();
		let errorCalls = 0;
		let idleCalls = 0;
		let errorWriteId = "";
		const writer: SessionEventWriter = {
			...writerFrom((envelope) => ({
				ok: true,
				eventId: `bridge-${envelope.writeId}`,
				type: "committed",
			})),
			append: async (envelope) => {
				errorCalls += 1;
				errorWriteId = envelope.writeId;
				return await errorResult.promise;
			},
			finishIdle: async (envelope) => {
				idleCalls += 1;
				return withFinishIdleResultForTest(envelope, {
					ok: true,
					eventId: `bridge-${envelope.durableTurnId}`,
					type: "committed",
				});
			},
		};
		const loader = new RecordingContextLoader([], { type: "empty" });
		const session = threadWithActiveTurn(
			"sesn_failed_closeout_single_flight",
			"evt_failed_closeout_single_flight_running",
		);
		let observationWindows = 0;
		const closeout = await Effect.runPromise(
			Effect.gen(function* () {
				return (yield* ThreadLoop.Service).closeFailedRun(
					session,
					new Error("defect"),
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						runtime: {
							...threadLoopRuntime(),
							sleep: async (milliseconds, signal) => {
								if (
									milliseconds ===
										SessionEventWriterRetryPolicy.timeoutPerAttemptMs &&
									observationWindows < 2
								) {
									observationWindows += 1;
									return true;
								}
								return await sleepUntilAborted(milliseconds, signal);
							},
						},
					}),
				),
			),
		);
		expect(await Effect.runPromise(closeout)).toMatchObject({
			type: "retry",
			error: { code: "timeout" },
		});
		expect(await Effect.runPromise(closeout)).toMatchObject({
			type: "retry",
			error: { code: "timeout" },
		});
		expect(errorCalls).toBe(1);
		expect(idleCalls).toBe(0);
		errorResult.resolve({
			ok: true,
			eventId: `bridge-${errorWriteId}`,
			type: "committed",
		});
		await flushMicrotasks(10);
		expect(await Effect.runPromise(closeout)).toEqual({ type: "landed" });
		expect(await Effect.runPromise(closeout)).toEqual({ type: "landed" });
		expect(errorCalls).toBe(1);
		expect(idleCalls).toBe(1);
	});
	test("failed-run closeout shares one observation window across error and idle steps", async () => {
		const errorResult = deferred<SessionEventWriterAppendResult>();
		const idleResult = deferred<SessionEventWriterAppendResult>();
		const observationElapsed = deferred<void>();
		let errorWriteId = "";
		let idleWriteId = "";
		let errorCalls = 0;
		let idleCalls = 0;
		let timeoutSleeps = 0;
		const writer: SessionEventWriter = {
			...writerFrom((envelope) => ({
				ok: true,
				eventId: `bridge-${envelope.writeId}`,
				type: "committed",
			})),
			append: async (envelope) => {
				errorCalls += 1;
				errorWriteId = envelope.writeId;
				return await errorResult.promise;
			},
			finishIdle: async (envelope) => {
				idleCalls += 1;
				idleWriteId = envelope.durableTurnId;
				return withFinishIdleResultForTest(envelope, await idleResult.promise);
			},
		};
		const closeout = await Effect.runPromise(
			Effect.gen(function* () {
				return (yield* ThreadLoop.Service).closeFailedRun(
					threadWithActiveTurn(
						"sesn_failed_closeout_shared_window",
						"evt_failed_closeout_shared_window_running",
					),
					new Error("defect"),
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(
						new RecordingContextLoader([], { type: "empty" }),
						{
							writer,
							runtime: {
								...threadLoopRuntime(),
								sleep: async (milliseconds, signal) => {
									if (
										milliseconds ===
										SessionEventWriterRetryPolicy.timeoutPerAttemptMs
									) {
										timeoutSleeps += 1;
										if (timeoutSleeps > 1) {
											return await sleepUntilAborted(milliseconds, signal);
										}
										await Promise.race([
											observationElapsed.promise,
											new Promise<void>((resolve) =>
												signal.addEventListener("abort", () => resolve(), {
													once: true,
												}),
											),
										]);
										return true;
									}
									return await sleepUntilAborted(milliseconds, signal);
								},
							},
						},
					),
				),
			),
		);
		const first = Effect.runPromise(closeout);
		await waitForCondition(
			() => errorCalls === 1,
			"failed-run error closeout start",
		);
		errorResult.resolve({
			ok: true,
			eventId: `bridge-${errorWriteId}`,
			type: "committed",
		});
		await waitForCondition(
			() => idleCalls === 1,
			"failed-run idle closeout start",
		);
		observationElapsed.resolve();
		expect(await first).toMatchObject({
			type: "retry",
			error: { code: "timeout" },
		});
		expect(timeoutSleeps).toBe(2);
		idleResult.resolve({
			ok: true,
			eventId: `bridge-${idleWriteId}`,
			type: "committed",
		});
		await flushMicrotasks(10);
		expect(await Effect.runPromise(closeout)).toEqual({ type: "landed" });
		expect(errorCalls).toBe(1);
		expect(idleCalls).toBe(1);
	});
	test("failed-run closeout retries a rejected step with the same write identity on the next cycle", async () => {
		const appendWriteIds: string[] = [];
		let appendCalls = 0;
		const writer: SessionEventWriter = {
			...writerFrom((envelope) => ({
				ok: true,
				eventId: `bridge-${envelope.writeId}`,
				type: "committed",
			})),
			append: async (envelope) => {
				appendCalls += 1;
				appendWriteIds.push(envelope.writeId);
				if (appendCalls <= SessionEventWriterRetryPolicy.attempts) {
					return {
						ok: false,
						error: normalizeSessionEventWriterError({
							code: "unavailable",
							sessionId: envelope.sessionId,
						}),
					};
				}
				return {
					ok: true,
					eventId: `bridge-${envelope.writeId}`,
					type: "committed",
				};
			},
		};
		const closeout = await Effect.runPromise(
			Effect.gen(function* () {
				return (yield* ThreadLoop.Service).closeFailedRun(
					threadWithActiveTurn(
						"sesn_failed_closeout_reissue",
						"evt_failed_closeout_reissue_running",
					),
					new Error("defect"),
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(
						new RecordingContextLoader([], { type: "empty" }),
						{
							writer,
							runtime: {
								...threadLoopRuntime(),
								sleep: async (milliseconds, signal) =>
									milliseconds ===
									SessionEventWriterRetryPolicy.timeoutPerAttemptMs
										? await sleepUntilAborted(milliseconds, signal)
										: true,
							},
						},
					),
				),
			),
		);
		expect(await Effect.runPromise(closeout)).toMatchObject({
			type: "retry",
			error: { code: "unavailable" },
		});
		expect(await Effect.runPromise(closeout)).toEqual({ type: "landed" });
		expect(appendWriteIds).toHaveLength(
			SessionEventWriterRetryPolicy.attempts + 1,
		);
		expect(new Set(appendWriteIds).size).toBe(1);
	});
	test("failed-run closeout classifies an acknowledgement mismatch as unrepairable", async () => {
		const loader = new RecordingContextLoader([], { type: "empty" });
		const session = new ThreadRuntime("sesn_failed_closeout_ack_mismatch");
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.closeFailedRun(
					session,
					new Error("defect"),
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer: writerFrom((envelope) => ({
							ok: false,
							error: normalizeSessionEventWriterError({
								code: "ack_mismatch",
								sessionId: envelope.sessionId,
								writeId: envelope.writeId,
							}),
						})),
						runtime: { ...threadLoopRuntime(), sleep: sleepUntilAborted },
					}),
				),
			),
		);
		expect(result).toMatchObject({
			type: "unrepairable",
			error: { code: "ack_mismatch" },
		});
	});
	test("a stale CommitInputs receipt discards hot state without terminal declaration writes", async () => {
		const session = new ThreadRuntime("sesn_stale_commit_receipt");
		const input = acceptedInput("rin_stale_commit_receipt", session.sessionId);
		session.state.enqueueAcceptedInput(input);
		const terminalEvents: string[] = [];
		const loader: TestContextLoader = {
			buildContext: async () => [],
			loadPendingInput: async () => ({ type: "empty" }),
			commitAcceptedInput: async () => ({ type: "stale_custody" }),
		};
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer: writerFrom((envelope) => {
							if (
								envelope.event.type === "session.error" ||
								envelope.event.type === "span.model_request_end"
							) {
								terminalEvents.push(envelope.event.type);
							}
							return {
								ok: true,
								eventId: `bridge-${envelope.writeId}`,
								type: "committed",
							};
						}),
					}),
				),
			),
		);
		expect(result).toEqual({ type: "interrupted", discardHotState: true });
		expect(session.state.contextManager.entries()).toEqual([]);
		expect(session.state.acceptedInputCount()).toBe(0);
		expect(terminalEvents).toEqual([]);
	});
	test("idle finalization fails closed when FinishIdle boundary is unavailable", async () => {
		const loader = new RecordingContextLoader([], { type: "empty" });
		const appendedTypes: string[] = [];
		const writer: SessionEventWriter = {
			settleToolResult: async () => ({
				ok: true,
				result: { type: "committed" },
			}),
			append: async (envelope) => {
				appendedTypes.push(envelope.event.type);
				return {
					ok: true,
					eventId: `bridge-${envelope.writeId}`,
					type: "committed",
				};
			},
			writeRequestEnd: async () => {
				throw new Error("idle-only test must not close a provider request");
			},
		};
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(
					threadWithActiveTurn("sesn_1", "evt_open_idle_failure"),
					testRunCustody(),
				);
			}).pipe(Effect.provide(runtimeThreadLoopLayer(loader, { writer }))),
		);
		expect(result).toMatchObject({
			type: "failed",
			error: { code: "unavailable", sessionId: "sesn_1" },
			releaseSession: { reason: "event_write_failed" },
		});
		expect(appendedTypes).toEqual([]);
	});
	test("idle finalization retries lost ACKs with the same runtime write id", async () => {
		const loader = new RecordingContextLoader([], { type: "empty" });
		const session = new ThreadRuntime("sesn_1");
		session.state.markPersistentContextLoaded();
		session.state.installThreadTurn(
			{
				executionRunId: "evt_open_idle_retry",
				pendingInputContextSequences: [],
			},
			{ routes: [] },
		);
		const finishIdleWriteIds: string[] = [];
		const statesBeforeFinishIdleReceipts: string[] = [];
		const writer: SessionEventWriter = {
			settleToolResult: async () => ({
				ok: true,
				result: { type: "committed" },
			}),
			append: async (envelope) => ({
				ok: true,
				eventId: `bridge-${envelope.writeId}`,
				type: "committed",
			}),
			writeRequestEnd: async () => {
				throw new Error("idle-only test must not close a provider request");
			},
			finishIdle: async (envelope) => {
				finishIdleWriteIds.push(envelope.durableTurnId);
				statesBeforeFinishIdleReceipts.push(
					session.state.threadTurnReduction().state.state,
				);
				if (finishIdleWriteIds.length === 1) {
					return {
						ok: false,
						error: {
							type: "session-event-writer",
							code: "timeout",
							message: "lost ack",
							retryable: true,
							fatal: false,
							sessionId: envelope.sessionId,
							writeId: envelope.durableTurnId,
						},
					};
				}
				return withFinishIdleResultForTest(envelope, {
					ok: true,
					eventId: `bridge-${envelope.durableTurnId}`,
					type: "committed",
				});
			},
		};
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						installLoaderState: false,
						runtimeModel: () => undefined,
						writer,
					}),
				),
			),
		);
		expect(result).toEqual({ type: "completed", modelMessageCount: 0 });
		expect(finishIdleWriteIds).toEqual([
			"evt_open_idle_retry",
			"evt_open_idle_retry",
		]);
		expect(statesBeforeFinishIdleReceipts).toEqual([
			"ready_to_finish",
			"ready_to_finish",
		]);
		expect(session.state.threadTurnReduction().state).toEqual({
			state: "idle",
		});
	});
	test("idle finalization drains the raw FinishIdle call after its local timeout", async () => {
		const rawFinish = deferred<SessionEventWriterAppendResult>();
		let finishCalls = 0;
		const writer: SessionEventWriter = {
			settleToolResult: async () => ({
				ok: true,
				result: { type: "committed" },
			}),
			append: async (envelope) => ({
				ok: true,
				eventId: `bridge-${envelope.writeId}`,
				type: "committed",
			}),
			writeRequestEnd: async () => {
				throw new Error("idle-only test must not close a provider request");
			},
			finishIdle: async (envelope) => {
				finishCalls += 1;
				return withFinishIdleResultForTest(envelope, await rawFinish.promise);
			},
		};
		const run = Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					threadWithActiveTurn("sesn_finish_idle_drain", "evt_open_idle_drain"),
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(
						new RecordingContextLoader([], { type: "empty" }),
						{
							runtimeModel: () => undefined,
							writer,
							runtime: { ...threadLoopRuntime(), sleep: async () => true },
						},
					),
				),
			),
		);
		let settled = false;
		void run.finally(() => {
			settled = true;
		});
		await flushMicrotasks(10);
		expect(finishCalls).toBe(1);
		expect(settled).toBe(false);
		rawFinish.resolve({
			ok: true,
			eventId: "bridge-evt_open_idle_drain",
			type: "committed",
		});
		expect(await run).toEqual({ type: "completed", modelMessageCount: 0 });
		expect(finishCalls).toBe(1);
	});
	test("user interruption during an accepted reschedule wait settles end_turn before unwind", async () => {
		const session = new ThreadRuntime("sesn_1");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "retry then interrupt")],
		});
		const waitStarted = deferred<void>();
		const appended: SessionEvent[] = [];
		const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
		const writer = writerFrom(
			(envelope) => {
				appended.push(envelope.event);
				return {
					ok: true,
					eventId: `bridge-${envelope.writeId}`,
					type: "committed",
				};
			},
			async (envelope) => {
				requestEnds.push(envelope);
				return requestEndResultForTest(envelope);
			},
		);
		const runtime = {
			...threadLoopRuntime(),
			sleep: async (milliseconds: number, signal: AbortSignal) => {
				if (milliseconds !== 1_000) {
					return true;
				}
				waitStarted.resolve();
				await new Promise<void>((resolve, reject) => {
					signal.addEventListener(
						"abort",
						() => reject(new DOMException("aborted", "AbortError")),
						{ once: true },
					);
				});
				return true;
			},
		} satisfies RuntimeDependencies;
		const runFiber = Effect.runFork(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						llmService: queuedLLMService([
							[
								{
									type: "provider-error",
									error: runtimeFailureFromProviderError(
										normalizeProviderError({
											code: "provider_unavailable",
											message: "temporary provider failure",
											retryable: true,
											fatal: false,
										}),
									),
								},
							],
						]),
						writer,
						runtime,
					}),
				),
			),
		);
		await waitStarted.promise;
		expect(requestEnds[0]?.reschedule).toMatchObject({ attempt: 1 });
		await Effect.runPromise(Fiber.interrupt(runFiber));
		expect(appended.at(-1)).toEqual({
			type: "session.status_idle",
			stop_reason: { type: "end_turn" },
		});
		expect(appended.filter((event) => event.type === "session.error")).toEqual([
			expect.objectContaining({
				type: "session.error",
				error: expect.objectContaining({
					code: "provider_unavailable",
					retryStatus: { type: "retrying", attempt: 1 },
				}),
			}),
		]);
	});
	test("runtime shutdown abandons active provider state without Runtime-owned idle or error", async () => {
		const session = new ThreadRuntime("sesn_1");
		const loader = new QueuedContextLoader(
			[],
			[
				{ type: "context", entries: [userMessage("user-1", 0, "hello")] },
				{ type: "context", entries: [userMessage("user-2", 2, "second")] },
			],
		);
		const store = new ThreadLoopRuntimeStore([]);
		const releaseProvider = deferred<void>();
		const streamStarted = deferred<void>();
		const appended: SessionEvent[] = [];
		let providerCalls = 0;
		let observedAbortSignal: AbortSignal | undefined;
		const service: LLMServiceInterface = {
			stream(_request, options) {
				providerCalls += 1;
				observedAbortSignal = options?.abortSignal;
				return Stream.fromAsyncIterable(
					(async function* () {
						yield { type: "text-start" as const, id: "text-1" };
						yield {
							type: "text-delta" as const,
							id: "text-1",
							text_delta: "partial answer",
						};
						streamStarted.resolve();
						if (options?.abortSignal === undefined) {
							throw new Error("provider stream requires an abort signal");
						}
						await waitForReleaseOrAbort(
							releaseProvider.promise,
							options.abortSignal,
						);
						yield { type: "finish" as const, finishReason: "stop" as const };
					})(),
					(error): LLMServiceError => ({
						type: "llm-service",
						error: runtimeFailureFromProviderError(
							normalizeProviderError({
								code: "provider_unknown",
								retryable: false,
								message: error instanceof Error ? error.message : undefined,
							}),
						),
					}),
				);
			},
		};
		const runFiber = Effect.runFork(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						store,
						llmService: service,
						writer: writerFrom((envelope) => {
							appended.push(envelope.event);
							return {
								ok: true,
								eventId: `bridge-${envelope.writeId}`,
								type: "committed",
							};
						}),
					}),
				),
			),
		);
		await streamStarted.promise;
		session.state.beginRuntimeShutdown();
		const shutdown = Effect.runPromise(Fiber.interrupt(runFiber));
		await waitForCondition(
			() => observedAbortSignal?.aborted === true,
			"provider abort signal",
		);
		await shutdown;
		const runExit = await Effect.runPromise(Fiber.await(runFiber));
		releaseProvider.resolve();
		expect(
			Exit.isFailure(runExit) && Cause.hasInterruptsOnly(runExit.cause),
		).toBe(true);
		expect(providerCalls).toBe(1);
		expect(loader.pendingCalls).toEqual(["sesn_1"]);
		expect(
			[...store.messages.values()].find(
				(message) => message.role === "assistant",
			),
		).toBeUndefined();
		expect(appended).toEqual([
			{ type: "session.status_running" },
			{
				type: "span.model_request_start",
				model_request_id: expect.any(String),
			},
		]);
		expect(JSON.stringify(appended)).not.toContain("authorization");
		expect(JSON.stringify(appended)).not.toContain("bearer");
	});
	test("owner interruption waits for an in-flight running declaration", async () => {
		const session = new ThreadRuntime("sesn_running_declaration_owner");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-running-owner", 0, "hello")],
		});
		const appendStarted = deferred<void>();
		const releaseAppend = deferred<void>();
		const appended: SessionEvent[] = [];
		const baseWriter = writerFrom((envelope) => ({
			ok: true,
			eventId: `bridge-${envelope.writeId}`,
			type: "committed",
		}));
		const writer: SessionEventWriter = {
			...baseWriter,
			append: async (envelope) => {
				if (envelope.event.type === "session.status_running") {
					appendStarted.resolve();
					await releaseAppend.promise;
				}
				appended.push(envelope.event);
				return {
					ok: true,
					eventId: `bridge-${envelope.writeId}`,
					type: "committed",
				};
			},
		};
		const runFiber = Effect.runFork(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(Effect.provide(runtimeThreadLoopLayer(loader, { writer }))),
		);
		await appendStarted.promise;
		const interruptionFiber = Effect.runFork(Fiber.interrupt(runFiber));
		expect(interruptionFiber.pollUnsafe()).toBeUndefined();
		releaseAppend.resolve();
		await Effect.runPromise(Fiber.join(interruptionFiber));
		expect(appended).toEqual([{ type: "session.status_running" }]);
		const runExit = await Effect.runPromise(Fiber.await(runFiber));
		expect(
			Exit.isFailure(runExit) && Cause.hasInterruptsOnly(runExit.cause),
		).toBe(true);
	});
	test("runtime layer terminalizes processor creation failure before publishing an assistant message", async () => {
		const order: string[] = [];
		const session = new ThreadRuntime("sesn_1");
		const store = new ThreadLoopRuntimeStore(order);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const appended: SessionEvent[] = [];
		let providerCalled = false;
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						store,
						onStream: () => {
							providerCalled = true;
						},
						writer: writerFrom((envelope) => {
							appended.push(envelope.event);
							return {
								ok: true,
								eventId: `bridge-${envelope.writeId}`,
								type: "committed",
							};
						}),
						createProcessor: () => {
							const assistant = session.state.contextManager
								.entries()
								.find((message) => message.contextKind === "assistant");
							expect(assistant).toBeUndefined();
							throw new Error("processor construction failed");
						},
					}),
				),
			),
		);
		expect(providerCalled).toBe(false);
		expect(result).toMatchObject({
			type: "failed",
			error: {
				type: "runtime",
				code: "runtime_invalid_sequence",
				reason: "runtime_contract_validation",
			},
			releaseSession: { reason: "crashed" },
		});
		expect(order).toEqual([]);
		expect(appended).toEqual([
			{ type: "session.status_running" },
			expect.objectContaining({
				type: "session.error",
				error: expect.objectContaining({
					type: "runtime",
					code: "runtime_invalid_sequence",
					retryStatus: { type: "exhausted" },
				}),
			}),
			{
				type: "session.status_idle",
				stop_reason: { type: "retries_exhausted" },
			},
		]);
		expect(session.state.contextManager.entries()).toEqual([]);
	});
	test("discards completed reasoning when an in-flight request is interrupted", async () => {
		const session = new ThreadRuntime("sesn_1");
		const attachment = {
			transient: {
				attachmentRef: "att_interrupted_reasoning",
				sourceToolUseEventId: "sevt_interrupted_reasoning",
				sourcePath: "mcp:test/interrupted-reasoning.png",
				pageRange: "",
				detail: "auto",
			},
			fileBacked: undefined,
			mime: "image/png",
			filename: "interrupted-reasoning.png",
		} as const;
		session.state.addPendingAttachments([attachment]);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const reasoningProcessed = deferred<void>();
		const releaseStream = deferred<void>();
		const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
		const service: LLMServiceInterface = {
			stream(_request, options) {
				return Stream.fromAsyncIterable(
					(async function* () {
						yield {
							type: "reasoning-start" as const,
							id: "interrupt-reasoning",
						};
						yield {
							type: "reasoning-delta" as const,
							id: "interrupt-reasoning",
							text_delta: "discard on interrupt",
						};
						yield { type: "reasoning-end" as const, id: "interrupt-reasoning" };
						reasoningProcessed.resolve();
						if (options?.abortSignal === undefined) {
							throw new Error("provider stream requires an abort signal");
						}
						await waitForReleaseOrAbort(
							releaseStream.promise,
							options.abortSignal,
						);
					})(),
					(error): LLMServiceError => ({
						type: "llm-service",
						error: runtimeFailureFromProviderError(
							normalizeProviderError({
								code: "provider_unknown",
								retryable: false,
								message: error instanceof Error ? error.message : undefined,
							}),
						),
					}),
				);
			},
		};
		const baseWriter = writerFrom((envelope) => ({
			ok: true,
			eventId: `bridge-${envelope.writeId}`,
			type: "committed",
		}));
		const writer: SessionEventWriter = {
			...baseWriter,
			writeRequestEnd: async (envelope) => {
				requestEnds.push(envelope);
				return requestEndResultForTest(envelope);
			},
		};
		const runFiber = Effect.runFork(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, { llmService: service, writer }),
				),
			),
		);
		await reasoningProcessed.promise;
		const interruptCommand = {
			...acceptedInput("rin_reasoning_interrupt"),
			origin: "user" as const,
		};
		session.state.beginUserInterrupt(
			interruptCommand,
			testControlCommit(interruptCommand),
		);
		await Effect.runPromise(Fiber.interrupt(runFiber));
		releaseStream.resolve();
		expect(requestEnds).toHaveLength(1);
		expect(requestEnds[0]).toMatchObject({
			isError: true,
			errorKind: "runtime_interrupted",
			finishReason: "cancelled",
		});
		expect(
			session.state.contextManager
				.entries()
				.flatMap((message) => message.parts)
				.some((part) => part.type === "reasoning"),
		).toBe(false);
		expect(session.state.pendingAttachments()).toEqual([attachment]);
	});
	test("user interrupt after normal request assembly starts no assistant shell, span, or provider", async () => {
		const session = new ThreadRuntime("sesn_1");
		const storeOrder: string[] = [];
		const store = new ThreadLoopRuntimeStore(storeOrder);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "do not start")],
		});
		const appended: SessionEvent[] = [];
		let providerCalls = 0;
		let interruptCommits = 0;
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						store,
						writer: writerFrom((envelope) => {
							appended.push(envelope.event);
							return {
								ok: true,
								eventId: `bridge-${envelope.writeId}`,
								type: "committed",
							};
						}),
						providerCallAssembler: async (input) => {
							beginTestUserInterrupt(
								session,
								"rin_before_shell",
								() => interruptCommits++,
							);
							return await assembleProviderCallRequest(input);
						},
						llmService: {
							stream() {
								providerCalls++;
								return Stream.empty;
							},
						},
					}),
				),
			),
		);
		expect(result).toEqual({ type: "interrupted" });
		expect(interruptCommits).toBe(1);
		expect(providerCalls).toBe(0);
		expect(storeOrder).toEqual([]);
		expect(
			appended.filter((event) => event.type === "span.model_request_start"),
		).toEqual([]);
		expect(
			appended.filter((event) => event.type === "span.model_request_end"),
		).toEqual([]);
		expect(appended.at(-1)).toEqual({
			type: "session.status_idle",
			stop_reason: { type: "end_turn" },
		});
	});
	test("stale pre-provider interrupt commit discards hot state without FinishIdle", async () => {
		const session = new ThreadRuntime("sesn_stale_pre_provider_interrupt");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-stale-pre-provider", 0, "do not start")],
		});
		let finishIdleCalls = 0;
		let providerCalls = 0;
		const baseWriter = writerFrom((envelope) => ({
			ok: true,
			eventId: `bridge-${envelope.writeId}`,
			type: "committed",
		}));
		const writer: SessionEventWriter = {
			...baseWriter,
			finishIdle: async (envelope) => {
				finishIdleCalls += 1;
				return await baseWriter.finishIdle!(envelope);
			},
		};
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						providerCallAssembler: async (input) => {
							const command = acceptedInput("rin_stale_pre_provider_interrupt");
							session.state.beginUserInterrupt(command, async () => ({
								ok: true,
								stale: true,
							}));
							return await assembleProviderCallRequest(input);
						},
						llmService: {
							stream() {
								providerCalls += 1;
								return Stream.empty;
							},
						},
					}),
				),
			),
		);
		expect(result).toEqual({ type: "interrupted", discardHotState: true });
		expect(providerCalls).toBe(0);
		expect(finishIdleCalls).toBe(0);
	});
	test("user interrupt after normal span ACK closes it before any provider call", async () => {
		const session = new ThreadRuntime("sesn_1");
		const attachment = {
			transient: {
				attachmentRef: "att_interrupt_before_provider",
				sourceToolUseEventId: "sevt_tool_interrupt_before_provider",
				sourcePath: "mcp:github/interrupt.png",
				pageRange: "",
				detail: "auto",
			},
			fileBacked: undefined,
			mime: "image/png",
			filename: "interrupt.png",
		} as const;
		session.state.addPendingAttachments([attachment]);
		const store = new ThreadLoopRuntimeStore([]);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "stop before provider")],
		});
		const appended: SessionEvent[] = [];
		const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
		let providerCalls = 0;
		let interrupted = false;
		const baseWriter = writerFrom((envelope) => {
			appended.push(envelope.event);
			if (!interrupted && envelope.event.type === "span.model_request_start") {
				interrupted = true;
				beginTestUserInterrupt(session, "rin_after_span");
			}
			return {
				ok: true,
				eventId: `bridge-${envelope.writeId}`,
				type: "committed",
			};
		});
		const writer: SessionEventWriter = {
			...baseWriter,
			writeRequestEnd: async (envelope) => {
				requestEnds.push(envelope);
				return await baseWriter.writeRequestEnd(envelope);
			},
		};
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						store,
						writer,
						llmService: {
							stream() {
								providerCalls++;
								return Stream.empty;
							},
						},
					}),
				),
			),
		);
		expect(result).toEqual({ type: "interrupted" });
		expect(providerCalls).toBe(0);
		expect(
			appended.filter((event) => event.type === "span.model_request_start"),
		).toHaveLength(1);
		expect(requestEnds).toEqual([
			expect.objectContaining({
				isError: true,
				errorKind: "runtime_interrupted",
				finishReason: "cancelled",
			}),
		]);
		expect(requestEnds[0]?.consumedAttachmentRefs ?? []).toEqual([]);
		expect(session.state.pendingAttachments()).toEqual([attachment]);
	});
	test("provider interruption exposes request-end rejection through the Effect Cause", async () => {
		const session = new ThreadRuntime("sesn_provider_closeout_rejected");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [
				userMessage("user-provider-closeout", 0, "stop before provider"),
			],
		});
		let interrupted = false;
		const baseWriter = writerFrom((envelope) => {
			if (!interrupted && envelope.event.type === "span.model_request_start") {
				interrupted = true;
				beginTestUserInterrupt(session, "rin_provider_closeout_rejected");
			}
			return {
				ok: true,
				eventId: `bridge-${envelope.writeId}`,
				type: "committed",
			};
		});
		const writer: SessionEventWriter = {
			...baseWriter,
			writeRequestEnd: async (envelope) => ({
				ok: false,
				error: {
					type: "session-event-writer",
					code: "unavailable",
					message: "request end not ACKed",
					retryable: false,
					fatal: false,
					sessionId: envelope.sessionId,
				},
			}),
		};
		const runExit = await Effect.runPromiseExit(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						llmService: {
							stream() {
								throw new Error("provider must not start");
							},
						},
					}),
				),
			),
		);
		expect(Exit.isFailure(runExit)).toBe(true);
		if (Exit.isFailure(runExit)) {
			expect(
				runExit.cause.reasons.find(Cause.isDieReason)?.defect,
			).toMatchObject({
				type: "session-event-writer",
				code: "unavailable",
			});
		}
		expect(
			session.state.userInterruptCommitResult("rin_provider_closeout_rejected"),
		).toEqual({
			ok: false,
			retryable: false,
			errorCode: "unavailable",
		});
	});
	test("a stale interrupt request-end receipt performs no fallback idle closeout", async () => {
		const session = new ThreadRuntime("sesn_stale_interrupt_end");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-stale-interrupt", 0, "stop before provider")],
		});
		let interrupted = false;
		let requestEndCalls = 0;
		let finishIdleCalls = 0;
		const baseWriter = writerFrom((envelope) => {
			if (!interrupted && envelope.event.type === "span.model_request_start") {
				interrupted = true;
				beginTestUserInterrupt(session, "rin_stale_interrupt_end");
			}
			return {
				ok: true,
				eventId: `bridge-${envelope.writeId}`,
				type: "committed",
			};
		});
		const writer: SessionEventWriter = {
			...baseWriter,
			writeRequestEnd: async (envelope) => {
				requestEndCalls += 1;
				const result = await baseWriter.writeRequestEnd(envelope);
				if (!result.ok) {
					return result;
				}
				return { ok: true, type: "stale" };
			},
			finishIdle: async (envelope) => {
				finishIdleCalls += 1;
				return await baseWriter.finishIdle!(envelope);
			},
		};
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						llmService: {
							stream() {
								throw new Error("provider must not start");
							},
						},
					}),
				),
			),
		);
		expect(result).toEqual({ type: "interrupted", discardHotState: true });
		expect(requestEndCalls).toBe(1);
		expect(finishIdleCalls).toBe(0);
		expect(
			session.state.userInterruptCommitResult("rin_stale_interrupt_end"),
		).toEqual({
			ok: true,
			stale: true,
		});
	});
	test("cooperative child cancellation closes an ACKed request before run release", async () => {
		const session = new ThreadRuntime("sesn_1");
		const attachment = {
			transient: {
				attachmentRef: "att_cooperative_before_provider",
				sourceToolUseEventId: "sevt_tool_cooperative_before_provider",
				sourcePath: "mcp:github/cooperative.png",
				pageRange: "",
				detail: "auto",
			},
			fileBacked: undefined,
			mime: "image/png",
			filename: "cooperative.png",
		} as const;
		session.state.addPendingAttachments([attachment]);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "review this")],
		});
		const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
		let providerCalls = 0;
		const writer = writerFrom(
			(envelope) => {
				if (envelope.event.type === "span.model_request_start") {
					session.state.beginCooperativeCancel();
				}
				return {
					ok: true,
					eventId: `bridge-${envelope.writeId}`,
					type: "committed",
				};
			},
			async (envelope) => {
				requestEnds.push(envelope);
				return requestEndResultForTest(envelope);
			},
		);
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						llmService: {
							stream() {
								providerCalls++;
								return Stream.empty;
							},
						},
					}),
				),
			),
		);
		expect(result).toEqual({ type: "interrupted" });
		expect(providerCalls).toBe(0);
		expect(requestEnds).toEqual([
			expect.objectContaining({
				isError: true,
				errorKind: "runtime_interrupted",
				finishReason: "cancelled",
			}),
		]);
		expect(requestEnds[0]?.consumedAttachmentRefs ?? []).toEqual([]);
		expect(session.state.pendingAttachments()).toEqual([attachment]);
	});
	test("cooperative cancellation before request start is write-free", async () => {
		const session = new ThreadRuntime("sesn_1");
		session.state.beginCooperativeCancel();
		const appended: SessionEvent[] = [];
		const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
		const baseWriter = writerFrom((envelope) => {
			appended.push(envelope.event);
			return {
				ok: true,
				eventId: `bridge-${envelope.writeId}`,
				type: "committed",
			};
		});
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(
						new RecordingContextLoader([], {
							type: "context",
							entries: [userMessage("user-1", 0, "review")],
						}),
						{
							writer: {
								...baseWriter,
								writeRequestEnd: async (envelope) => {
									requestEnds.push(envelope);
									return requestEndResultForTest(envelope);
								},
							},
						},
					),
				),
			),
		);
		expect(result).toEqual({ type: "interrupted" });
		expect(appended).toEqual([]);
		expect(requestEnds).toEqual([]);
	});
	test("failed interrupt request-end leaves the snapshot and FinishIdle unwritten for Bridge repair", async () => {
		const session = new ThreadRuntime("sesn_1");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [
				userMessage(
					"user-request-end-failure",
					0,
					"interrupt after span start",
				),
			],
		});
		const appended: SessionEvent[] = [];
		let interruptCommits = 0;
		let finishIdleCalls = 0;
		const baseWriter = writerFrom((envelope) => {
			appended.push(envelope.event);
			if (envelope.event.type === "span.model_request_start") {
				beginTestUserInterrupt(session, "rin_request_end_failure", () => {
					interruptCommits++;
				});
			}
			return {
				ok: true,
				eventId: `bridge-${envelope.writeId}`,
				type: "committed",
			};
		});
		const writer: SessionEventWriter = {
			...baseWriter,
			writeRequestEnd: async (envelope) => ({
				ok: false,
				error: {
					type: "session-event-writer",
					code: "unavailable",
					message: "request end unavailable",
					retryable: false,
					fatal: true,
					sessionId: envelope.sessionId,
				},
			}),
			finishIdle: async (envelope) => {
				finishIdleCalls++;
				return withFinishIdleResultForTest(envelope, {
					ok: true,
					eventId: `bridge-${envelope.durableTurnId}`,
					type: "committed",
				});
			},
		};
		const runExit = await Effect.runPromiseExit(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						llmService: {
							stream() {
								throw new Error(
									"provider must not start after the interrupt wins at span ACK",
								);
							},
						},
					}),
				),
			),
		);
		expect(Exit.isFailure(runExit)).toBe(true);
		if (Exit.isFailure(runExit)) {
			expect(
				runExit.cause.reasons.find(Cause.isDieReason)?.defect,
			).toMatchObject({
				type: "session-event-writer",
				code: "unavailable",
			});
		}
		expect(interruptCommits).toBe(0);
		expect(finishIdleCalls).toBe(0);
		expect(
			appended.filter((event) => event.type === "session.status_idle"),
		).toEqual([]);
	});
	test("failed interrupt receipt application leaves FinishIdle unwritten and surfaces through the Effect Cause", async () => {
		const session = new ThreadRuntime("sesn_1");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-repair-failure", 0, "run then interrupt")],
		});
		const appended: SessionEvent[] = [];
		const toolUseWritten = deferred<void>();
		let interruptCommits = 0;
		let finishIdleCalls = 0;
		const baseWriter = writerFrom((envelope) => {
			appended.push(envelope.event);
			if (envelope.event.type === "agent.tool_use") {
				beginTestUserInterrupt(session, "rin_repair_failure", () => {
					interruptCommits++;
				});
				toolUseWritten.resolve();
			}
			return {
				ok: true,
				eventId:
					envelope.event.type === "agent.tool_use"
						? "sevt_repair_failure"
						: `bridge-${envelope.writeId}`,
				type: "committed",
			};
		});
		const writer: SessionEventWriter = {
			...baseWriter,
			writeRequestEnd: async (envelope) => {
				const result = await baseWriter.writeRequestEnd(envelope);
				return result.ok &&
					result.type !== "stale" &&
					envelope.interruptSettlement !== undefined
					? { ...result, interruptToolResults: [] }
					: result;
			},
			finishIdle: async (envelope) => {
				finishIdleCalls++;
				return withFinishIdleResultForTest(envelope, {
					ok: true,
					eventId: `bridge-${envelope.durableTurnId}`,
					type: "committed",
				});
			},
		};
		const runFiber = Effect.runFork(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						providerCallRuntime: {
							systemInstructions: "interrupt repair failure test",
							toolCatalog: catalogForTest({
								name: "Write",
								description: "Write file",
								inputSchema: { type: "object" },
							}),
						},
						llmService: {
							stream(_request, streamOptions) {
								return Stream.fromAsyncIterable(
									(async function* () {
										yield {
											type: "tool-call" as const,
											id: "tool-repair-failure",
											toolName: "Write",
											input: { file_path: "src/failure.ts", content: "one" },
											inputPreview: {
												preview: "{}",
												truncated: false,
											},
										};
										if (streamOptions?.abortSignal === undefined) {
											throw new Error(
												"provider stream requires an abort signal",
											);
										}
										await waitForReleaseOrAbort(
											new Promise<void>(() => undefined),
											streamOptions.abortSignal,
										);
									})(),
									(error): LLMServiceError => ({
										type: "llm-service",
										error: runtimeFailureFromProviderError(
											normalizeProviderError({
												code: "provider_stream_error",
												message: String(error),
												retryable: true,
											}),
										),
									}),
								);
							},
						},
					}),
				),
			),
		);
		await toolUseWritten.promise;
		await Effect.runPromise(Fiber.interrupt(runFiber));
		const runExit = await Effect.runPromise(Fiber.await(runFiber));
		expect(Exit.isFailure(runExit)).toBe(true);
		if (Exit.isFailure(runExit)) {
			expect(
				runExit.cause.reasons.find(Cause.isDieReason)?.defect,
			).toMatchObject({
				type: "runtime",
				code: "runtime_invalid_sequence",
				reason: "runtime_contract_validation",
			});
		}
		expect(interruptCommits).toBe(0);
		expect(finishIdleCalls).toBe(0);
		expect(
			appended.filter((event) => event.type === "session.status_idle"),
		).toEqual([]);
	});
	test("SessionManager joins the original interrupt FinishIdle ACK before releasing the run slot", async () => {
		const firstProviderStarted = deferred<void>();
		const releaseFinishIdle = deferred<void>();
		const finishIdleStarted = deferred<void>();
		const requests: LLMRequest[] = [];
		const finishIdleWriteIds: string[] = [];
		const loader = new QueuedContextLoader([], []);
		const baseWriter = writerFrom((envelope) => ({
			ok: true,
			eventId: `bridge-${envelope.writeId}`,
			type: "committed",
		}));
		const writer: SessionEventWriter = {
			...baseWriter,
			finishIdle: async (envelope) => {
				finishIdleWriteIds.push(envelope.durableTurnId);
				if (finishIdleWriteIds.length === 1) {
					finishIdleStarted.resolve();
					await releaseFinishIdle.promise;
				}
				return withFinishIdleResultForTest(envelope, {
					ok: true,
					eventId: `bridge-${envelope.durableTurnId}`,
					type: "committed",
				});
			},
		};
		let providerCalls = 0;
		const agentLayer = runtimeThreadLoopLayer(loader, {
			writer,
			llmService: {
				stream(request, streamOptions) {
					requests.push(request);
					providerCalls++;
					if (providerCalls === 1) {
						firstProviderStarted.resolve();
						return Stream.fromAsyncIterable(
							(async function* () {
								await new Promise<void>((resolve) => {
									if (streamOptions?.abortSignal?.aborted === true) {
										resolve();
										return;
									}
									streamOptions?.abortSignal?.addEventListener(
										"abort",
										() => resolve(),
										{ once: true },
									);
								});
							})(),
							(error): LLMServiceError => ({
								type: "llm-service",
								error: runtimeFailureFromProviderError(
									normalizeProviderError({
										code: "provider_stream_error",
										message: String(error),
										retryable: true,
									}),
								),
							}),
						);
					}
					return Stream.fromIterable([
						{ type: "text-start" as const, id: "follow-up" },
						{
							type: "text-delta" as const,
							id: "follow-up",
							text_delta: "after ACK",
						},
						{ type: "text-end" as const, id: "follow-up" },
						{ type: "finish" as const, finishReason: "stop" as const },
					]);
				},
			},
		});
		const managerLayer = SessionManager.layer({
			maxLocalSessions: 4,
			now: () => createdAt,
		}).pipe(Layer.provide(agentLayer));
		const { manager, scope } = await Effect.runPromise(
			Effect.gen(function* () {
				const layerScope = yield* Scope.make();
				const context = yield* Layer.buildWithScope(managerLayer, layerScope);
				return {
					manager: Context.get(context, SessionManager.Service),
					scope: layerScope,
				};
			}),
		);
		try {
			const firstInput = {
				...acceptedInput("rin_finish_idle_owner"),
				inputOrder: 1,
				contentJson: JSON.stringify({
					messages: [
						userMessage("user-finish-idle-owner", 1, "hold the first run"),
					],
				}),
			};
			await Effect.runPromise(
				manager.preloadThread({
					...firstInput,
					runtimeBindingToken: "runtime-binding-token",
					contextEntries: [
						userMessage("user-finish-idle-owner", 1, "hold the first run"),
					],
					thread: {
						role: "main",
						visibility: "public",
						agentType: "general",
						status: "idle",
					},
				}),
			);
			await Effect.runPromise(manager.acceptInput(firstInput));
			await firstProviderStarted.promise;
			const interruptCommand = interruptInput("rin_finish_idle_interrupt", 9);
			let interruptSettled = false;
			const interrupt = Effect.runPromise(
				manager.interruptControl(
					"sesn_1",
					interruptCommand,
					testControlCommit(interruptCommand),
				),
			).then((result) => {
				interruptSettled = true;
				return result;
			});
			await finishIdleStarted.promise;
			const postFenceInput = {
				...acceptedInput("rin_after_finish_idle"),
				inputOrder: 10,
			};
			expect(
				await Effect.runPromise(manager.acceptInput(postFenceInput)),
			).toMatchObject({
				ok: true,
			});
			await flushMicrotasks(50);
			expect(interruptSettled).toBe(false);
			expect(
				await Effect.runPromise(manager.waitThread(interruptCommand, 0)),
			).toMatchObject({ ok: true, timedOut: true });
			expect(finishIdleWriteIds).toHaveLength(1);
			expect(new Set(finishIdleWriteIds).size).toBe(1);
			expect(providerCalls).toBe(1);
			releaseFinishIdle.resolve();
			await expect(interrupt).resolves.toMatchObject({
				ok: true,
				interrupted: true,
			});
			expect(
				await Effect.runPromise(manager.waitThread(postFenceInput, 1000)),
			).toMatchObject({
				ok: true,
				observed: true,
				timedOut: false,
			});
			expect(loader.commitCalls.map((input) => input.runtimeInputId)).toEqual([
				"rin_finish_idle_owner",
				"rin_after_finish_idle",
			]);
			expect(providerCalls).toBe(2);
			expect(requests).toHaveLength(2);
			expect(finishIdleWriteIds[0]).not.toBe(finishIdleWriteIds.at(-1));
		} finally {
			releaseFinishIdle.resolve();
			await Effect.runPromise(Scope.close(scope, Exit.void));
		}
	});
	test("interrupt accepted during ordinary FinishIdle completes after that idle ACK", async () => {
		const session = new ThreadRuntime({
			workspaceId: "wksp_test",
			sessionId: "sesn_1",
			sessionThreadId: "thrd_1",
			threadRole: "main",
			bindingId: "bind_1",
			bindingGeneration: 1,
			targetPodUid: "pod_1",
			runtimeBindingToken: "runtime-binding-token",
		});
		const releaseFinishIdle = deferred<void>();
		const finishIdleStarted = deferred<void>();
		const appended: SessionEvent[] = [];
		const finishIdleWriteIds: string[] = [];
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [
				userMessage("user-ordinary-finish-idle", 0, "finish this turn"),
			],
		});
		const baseWriter = writerFrom((envelope) => {
			appended.push(envelope.event);
			return {
				ok: true,
				eventId: `bridge-${envelope.writeId}`,
				type: "committed",
			};
		});
		const writer: SessionEventWriter = {
			...baseWriter,
			finishIdle: async (envelope) => {
				finishIdleWriteIds.push(envelope.durableTurnId);
				finishIdleStarted.resolve();
				await releaseFinishIdle.promise;
				return withFinishIdleResultForTest(envelope, {
					ok: true,
					eventId: `bridge-${envelope.durableTurnId}`,
					type: "committed",
				});
			},
		};
		const agentLayer = runtimeThreadLoopLayer(loader, {
			writer,
			llmService: {
				stream() {
					return Stream.fromIterable([
						{ type: "text-start" as const, id: "answer" },
						{ type: "text-delta" as const, id: "answer", text_delta: "done" },
						{ type: "text-end" as const, id: "answer" },
						{ type: "finish" as const, finishReason: "stop" as const },
					]);
				},
			},
		});
		const custody = testRunCustody();
		const runFiber = Effect.runFork(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, custody);
			}).pipe(Effect.provide(agentLayer)),
		);
		try {
			await finishIdleStarted.promise;
			const command = interruptInput("rin_interrupt_ordinary_finish_idle", 9);
			let commits = 0;
			expect(
				session.state.beginUserInterrupt(command, async (declaration) => {
					commits += 1;
					return buildRuntimeControlCommitResult(
						command,
						"interrupt_control",
						declaration,
					);
				}),
			).toBe("applied");
			const interrupted = Effect.runPromise(Fiber.interrupt(runFiber));
			expect(commits).toBe(0);
			releaseFinishIdle.resolve();
			await interrupted;
			const runExit = await Effect.runPromise(Fiber.await(runFiber));
			expect(
				Exit.isFailure(runExit) && Cause.hasInterruptsOnly(runExit.cause),
			).toBe(true);
			expect(commits).toBe(1);
			expect(
				session.state.userInterruptCloseoutCompleted(command.runtimeInputId),
			).toBe(true);
			expect(finishIdleWriteIds).toHaveLength(1);
			expect(
				appended.filter((event) => event.type === "session.error"),
			).toEqual([]);
		} finally {
			releaseFinishIdle.resolve();
			await Effect.runPromise(Fiber.interrupt(runFiber));
		}
	});
	test("interrupt snapshot joins an in-flight pre-fence CommitInputs and remains the last closeout input", async () => {
		const preCommitStarted = deferred<void>();
		const releasePreCommit = deferred<void>();
		const order: string[] = [];
		const commitCalls: string[] = [];
		let nextMessageSequence = 1;
		const commitReceipt = (
			input: RuntimeAcceptedInputState,
		): ReturnType<typeof acceptedInputCommitResult> => {
			const result = acceptedInputCommitResult(
				input,
				"committed",
				nextMessageSequence,
			);
			nextMessageSequence += result.assignedContextSequences.length;
			return result;
		};
		const loader: TestContextLoader = {
			buildContext: async () => [],
			loadPendingInput: async () => ({ type: "empty" }),
			commitAcceptedInput: async (input) => {
				commitCalls.push(input.runtimeInputId);
				if (input.runtimeInputId === "rin_pre_fence") {
					order.push("commit:pre:start");
					preCommitStarted.resolve();
					await releasePreCommit.promise;
					order.push("commit:pre:end");
					return commitReceipt(input);
				}
				order.push(`commit:${input.runtimeInputId}`);
				return commitReceipt(input);
			},
		};
		const appended: SessionEvent[] = [];
		const managerLayer = SessionManager.layer({
			maxLocalSessions: 4,
			now: () => createdAt,
		}).pipe(
			Layer.provide(
				runtimeThreadLoopLayer(loader, {
					runtime: { ...threadLoopRuntime(), sleep: sleepUntilAborted },
					writer: writerFrom((envelope) => {
						appended.push(envelope.event);
						order.push(`event:${envelope.event.type}`);
						return {
							ok: true,
							eventId: `bridge-${envelope.writeId}`,
							type: "committed",
						};
					}),
				}),
			),
		);
		const { manager, scope } = await Effect.runPromise(
			Effect.gen(function* () {
				const layerScope = yield* Scope.make();
				const context = yield* Layer.buildWithScope(managerLayer, layerScope);
				return {
					manager: Context.get(context, SessionManager.Service),
					scope: layerScope,
				};
			}),
		);
		try {
			const preFenceInput = {
				...acceptedInput("rin_pre_fence"),
				inputOrder: 5,
			};
			await Effect.runPromise(
				manager.preloadThread({
					...preFenceInput,
					runtimeBindingToken: "runtime-binding-token",
					contextEntries: [],
					thread: {
						role: "main",
						visibility: "public",
						agentType: "general",
						status: "idle",
					},
				}),
			);
			await Effect.runPromise(manager.acceptInput(preFenceInput));
			await preCommitStarted.promise;
			const interruptCommand = interruptInput("rin_commit_fence_interrupt", 9);
			const interrupt = Effect.runPromise(
				manager.interruptControl(
					"sesn_1",
					interruptCommand,
					async (declaration) => {
						order.push("commit:interrupt");
						return buildRuntimeControlCommitResult(
							interruptCommand,
							"interrupt_control",
							declaration,
						);
					},
				),
			);
			await flushMicrotasks(50);
			await Effect.runPromise(
				manager.acceptInput({
					...acceptedInput("rin_post_fence"),
					inputOrder: 10,
				}),
			);
			expect(order).toEqual([
				"event:session.status_running",
				"commit:pre:start",
			]);
			releasePreCommit.resolve();
			await expect(interrupt).resolves.toMatchObject({
				ok: true,
				interrupted: true,
			});
			await Effect.runPromise(manager.waitThread(interruptCommand, 1000));
			expect(commitCalls).toEqual(["rin_pre_fence", "rin_post_fence"]);
			expect(order.indexOf("commit:pre:end")).toBeLessThan(
				order.indexOf("commit:interrupt"),
			);
			expect(order.indexOf("commit:interrupt")).toBeLessThan(
				order.indexOf("event:session.status_idle"),
			);
			expect(order.indexOf("event:session.status_idle")).toBeLessThan(
				order.indexOf("commit:rin_post_fence"),
			);
			expect(
				appended.filter((event) => event.type === "session.error"),
			).toEqual([]);
		} finally {
			releasePreCommit.resolve();
			await Effect.runPromise(Scope.close(scope, Exit.void));
		}
	});
	test("runtime layer treats progress append exhaustion as fatal and terminal append failures as non-recursive best effort", async () => {
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
			(event) =>
				event.type === "agent.message" ||
				event.type === "session.error" ||
				event.type === "session.status_idle",
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
							{ type: "text-start", id: "text-1" },
							{ type: "text-delta", id: "text-1", text_delta: "hello" },
							{ type: "text-end", id: "text-1" },
						],
					}),
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
	test("runtime layer requests hot-state discard when invalid finish terminal append fails", async () => {
		const order: string[] = [];
		const session = new ThreadRuntime("sesn_1");
		const store = new ThreadLoopRuntimeStore(order);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const appended: SessionEvent[] = [];
		const writer = writerFrom((envelope) => {
			appended.push(envelope.event);
			if (
				envelope.event.type === "session.error" ||
				envelope.event.type === "session.status_idle"
			) {
				return {
					ok: false,
					error: {
						type: "session-event-writer",
						code: "unavailable",
						message: "append failed",
						retryable: false,
						fatal: false,
						sessionId: envelope.sessionId,
					},
				};
			}
			return {
				ok: true,
				eventId: `bridge-${envelope.writeId}`,
				type: "committed",
			};
		});
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						store,
						writer,
						events: [{ type: "finish", finishReason: "stop" }],
					}),
				),
			),
		);
		expect(result).toMatchObject({
			type: "failed",
			error: { type: "session-event-writer", code: "unavailable" },
			releaseSession: { reason: "event_write_failed" },
		});
		expect(appended.map((event) => event.type)).toEqual([
			"session.status_running",
			"span.model_request_start",
			"span.model_request_end",
			"session.error",
			"session.status_idle",
		]);
		expect(appended).toContainEqual(
			expect.objectContaining({
				type: "span.model_request_end",
				is_error: true,
				error_kind: "runtime_semantic_error",
			}),
		);
		expect(order).toEqual([]);
		expect(
			session.state.contextManager
				.entries()
				.some((message) => message.contextKind === "assistant"),
		).toBe(false);
	});
	test("runtime layer appends clean terminal events for provider diagnostics before durable progress", async () => {
		const order: string[] = [];
		const session = new ThreadRuntime("sesn_1");
		const store = new ThreadLoopRuntimeStore(order);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const appended: SessionEvent[] = [];
		const service: LLMServiceInterface = {
			stream() {
				return Stream.fail({
					type: "llm-service",
					error: runtimeFailureFromProviderError(
						normalizeProviderError({
							code: "provider_unavailable",
							retryable: true,
							providerId: "fake",
							modelId: "fake-chat",
							statusCode: 503,
							retryAfterMs: 7,
							message: "Provider unavailable.",
						}),
						{ type: "terminal" },
					),
				} satisfies LLMServiceError);
			},
		};
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						store,
						llmService: service,
						writer: writerFrom((envelope) => {
							appended.push(envelope.event);
							return {
								ok: true,
								eventId: `bridge-${envelope.writeId}`,
								type: "committed",
							};
						}),
					}),
				),
			),
		);
		expect(result).toMatchObject({
			type: "failed",
			error: {
				type: "provider",
				code: "provider_unavailable",
				message: "Provider unavailable.",
				retryable: true,
				providerId: "fake",
				modelId: "fake-chat",
				statusCode: 503,
				retryAfterMs: 7,
			},
		});
		expect(appended.at(3)).toMatchObject({
			type: "session.error",
			error: {
				code: "provider_unavailable",
				message: "Provider unavailable.",
				statusCode: 503,
				retryAfterMs: 7,
			},
		});
		expect(
			session.state.contextManager
				.entries()
				.some((message) => message.contextKind === "assistant"),
		).toBe(false);
		expectNoProviderDiagnosticCanaries({
			result,
			appended,
			storedMessages: [...store.messages.values()],
		});
	});
	test("runtime layer fails default fake cancelled provider turn without leaving streaming assistant state", async () => {
		const order: string[] = [];
		const session = new ThreadRuntime("sesn_1");
		const store = new ThreadLoopRuntimeStore(order);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const appended: SessionEvent[] = [];
		const service: LLMServiceInterface = {
			stream() {
				return Stream.fail({
					type: "llm-service",
					error: runtimeFailureFromProviderError(
						normalizeProviderError({
							code: "provider_cancelled",
							retryable: false,
							providerId: "fake",
							modelId: "fake-chat",
							message: "Provider request was cancelled.",
						}),
						{ type: "terminal" },
					),
				} satisfies LLMServiceError);
			},
		};
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						store,
						llmService: service,
						writer: writerFrom((envelope) => {
							appended.push(envelope.event);
							return {
								ok: true,
								eventId: `bridge-${envelope.writeId}`,
								type: "committed",
							};
						}),
					}),
				),
			),
		);
		expect(result).toMatchObject({
			type: "failed",
			error: { type: "provider", code: "provider_cancelled" },
		});
		expect(result).not.toHaveProperty("releaseSession");
		expect(appended.map((event) => event.type)).toEqual([
			"session.status_running",
			"span.model_request_start",
			"span.model_request_end",
			"session.error",
			"session.status_idle",
		]);
		expect(
			appended.filter((event) => event.type === "span.model_request_end"),
		).toEqual([
			expect.objectContaining({
				type: "span.model_request_end",
				is_error: true,
				error_kind: "provider_error",
			}),
		]);
		expect(appended).not.toContainEqual(
			expect.objectContaining({
				type: "span.model_request_end",
				is_error: false,
			}),
		);
		expect(appended.at(3)).toMatchObject({
			type: "session.error",
			error: { code: "provider_cancelled" },
		});
		expect(appended.at(4)).toEqual({
			type: "session.status_idle",
			stop_reason: { type: "end_turn" },
		});
		expect(
			session.state.contextManager
				.entries()
				.some((message) => message.contextKind === "assistant"),
		).toBe(false);
		expect(order).toEqual([]);
	});
	test("runtime layer terminalizes injected no-terminal provider progress without publishing its draft", async () => {
		const order: string[] = [];
		const session = new ThreadRuntime("sesn_1");
		const store = new ThreadLoopRuntimeStore(order);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const appended: SessionEvent[] = [];
		const service: LLMServiceInterface = {
			stream() {
				return Stream.fromIterable<LLMEvent>([
					{ type: "text-start", id: "text-1" },
					{ type: "text-delta", id: "text-1", text_delta: "visible" },
				]);
			},
		};
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						store,
						llmService: service,
						writer: writerFrom((envelope) => {
							appended.push(envelope.event);
							return {
								ok: true,
								eventId: `bridge-${envelope.writeId}`,
								type: "committed",
							};
						}),
					}),
				),
			),
		);
		expect(result).toMatchObject({
			type: "failed",
			error: { type: "runtime", code: "gateway_stream_error" },
		});
		expect(result).not.toHaveProperty("releaseSession");
		expect(appended.map((event) => event.type)).toEqual([
			"session.status_running",
			"span.model_request_start",
			"span.model_request_end",
			"session.error",
			"session.status_idle",
		]);
		expect(
			appended.filter((event) => event.type === "span.model_request_end"),
		).toEqual([
			expect.objectContaining({
				type: "span.model_request_end",
				is_error: true,
				error_kind: "gateway_stream_error",
			}),
		]);
		expect(appended).not.toContainEqual(
			expect.objectContaining({
				type: "span.model_request_end",
				is_error: false,
			}),
		);
		expect(JSON.stringify(appended)).not.toContain("visible");
		expect(appended.at(3)).toMatchObject({
			type: "session.error",
			error: { code: "gateway_stream_error" },
		});
		expect(
			session.state.contextManager
				.entries()
				.some((message) => message.contextKind === "assistant"),
		).toBe(false);
	});
	test("runtime layer requests hot-state discard when provider-error terminal append fails before progress", async () => {
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
			(event) =>
				event.type === "session.error" || event.type === "session.status_idle",
		);
		const providerError = {
			type: "provider",
			code: "provider_unavailable",
			message: "Provider unavailable.",
			retryable: true,
			fatal: false,
			retryStatus: { type: "exhausted" },
			providerId: "fake",
			modelId: "fake-chat",
		} as const;
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						store,
						writer,
						events: [{ type: "provider-error", error: providerError }],
					}),
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
			"span.model_request_end",
			"session.error",
		]);
		expect(
			session.state.contextManager
				.entries()
				.some((message) => message.contextKind === "assistant"),
		).toBe(false);
	});
	test("runtime layer routes a proven terminal provider failure through atomic termination", async () => {
		const session = new ThreadRuntime("sesn_1");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const appended: SessionEvent[] = [];
		const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
		const terminations: SessionEventWriterRuntimeTerminationEnvelope[] = [];
		const closeoutOrder: string[] = [];
		const baseWriter = writerFrom((envelope) => {
			appended.push(envelope.event);
			return {
				ok: true,
				eventId: `bridge-${envelope.writeId}`,
				type: "committed",
			};
		});
		const writer: SessionEventWriter = {
			...baseWriter,
			writeRequestEnd: async (envelope) => {
				closeoutOrder.push("write_request_end");
				requestEnds.push(envelope);
				return await baseWriter.writeRequestEnd(envelope);
			},
			commitRuntimeTermination: async (envelope) => {
				closeoutOrder.push("commit_runtime_termination");
				terminations.push(envelope);
				return runtimeTerminationResultForTest(envelope);
			},
		};
		const failure = {
			type: "provider",
			code: "provider_invalid_request",
			message: "Provider request is terminal.",
			retryable: false,
			fatal: true,
			retryStatus: { type: "terminal" },
			providerId: "fake",
			modelId: "fake-chat",
		} as const;
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						events: [
							{ type: "text-start", id: "terminal-text" },
							{
								type: "text-delta",
								id: "terminal-text",
								text_delta: "partial answer",
							},
							{ type: "text-end", id: "terminal-text" },
							{ type: "provider-error", error: failure },
						],
					}),
				),
			),
		);
		expect(result).toMatchObject({
			type: "failed",
			error: failure,
			releaseSession: { reason: "terminated" },
		});
		expect(terminations).toHaveLength(1);
		expect(terminations[0]?.requestId).toBe(terminations[0]?.writeId);
		expect(terminations[0]?.writeId).toMatch(/^bridge-stid_/);
		expect(terminations[0]).toMatchObject({
			sessionId: "sesn_1",
			sessionThreadId: "thread-test",
			failure,
		});
		expect(closeoutOrder).toEqual([
			"write_request_end",
			"commit_runtime_termination",
		]);
		expect(requestEnds).toEqual([
			expect.objectContaining({
				modelRequestId: expect.any(String),
				isError: true,
				errorKind: "provider_error",
				finishReason: "error",
			}),
		]);
		expect(appended.map((event) => event.type)).toEqual([
			"session.status_running",
			"span.model_request_start",
			"agent.message",
			"span.model_request_end",
		]);
		expect(session.state.threadTurnReduction()).toMatchObject({
			checkpoint: {
				terminalCloseout: {
					failureEventId: expect.any(String),
					closeoutEventId: expect.any(String),
					disposition: "terminated",
				},
			},
			state: { state: "idle" },
			action: { action: "await_input" },
		});
	});
	test("a fatal reviewer request failure closes durably idle without terminating its thread", async () => {
		const session = new ThreadRuntime({
			workspaceId: "wksp_reviewer",
			sessionId: "sesn_1",
			sessionThreadId: "thrd_reviewer",
			parentThreadId: "thrd_main",
			threadRole: "approval_reviewer",
			bindingId: "bind_reviewer",
			bindingGeneration: 1,
			targetPodUid: "pod_reviewer",
			runtimeBindingToken: "binding-token-reviewer",
		});
		session.state.enqueueAcceptedInput(
			approvalReviewAcceptedInput("rin_reviewer_fatal"),
		);
		const appended: SessionEvent[] = [];
		const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
		const terminations: SessionEventWriterRuntimeTerminationEnvelope[] = [];
		const baseWriter = writerFrom((envelope) => {
			appended.push(envelope.event);
			return {
				ok: true,
				eventId: `bridge-${envelope.writeId}`,
				type: "committed",
			};
		});
		const writer: SessionEventWriter = {
			...baseWriter,
			writeRequestEnd: async (envelope) => {
				requestEnds.push(envelope);
				return await baseWriter.writeRequestEnd(envelope);
			},
			commitRuntimeTermination: async (envelope) => {
				terminations.push(envelope);
				return await baseWriter.commitRuntimeTermination!(envelope);
			},
		};
		const failure = {
			type: "provider",
			code: "provider_invalid_request",
			message: "Reviewer request is terminal.",
			retryable: false,
			fatal: true,
			retryStatus: { type: "terminal" },
			providerId: "anthropic",
			modelId: "claude-opus-4-8",
		} as const;

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
						events: [{ type: "provider-error", error: failure }],
						runtimeModel: () => ({
							providerId: "anthropic",
							modelId: "claude-opus-4-8",
						}),
					}),
				),
			),
		);

		expect(result).toMatchObject({ type: "failed", error: failure });
		expect(result).not.toHaveProperty("releaseSession");
		expect(requestEnds).toEqual([expect.objectContaining({ isError: true })]);
		expect(requestEnds[0]).not.toHaveProperty("requestKind");
		expect(terminations).toEqual([]);
		expect(appended.map((event) => event.type)).toContain(
			"session.status_idle",
		);
		expect(session.state.threadTurnReduction()).toMatchObject({
			checkpoint: { idleCloseout: { stopReason: "end_turn" } },
			state: { state: "idle" },
			action: { action: "await_input" },
		});
	});
	test("runtime layer seals a terminal stream failure before atomic termination", async () => {
		const session = new ThreadRuntime("sesn_1");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const closeoutOrder: string[] = [];
		const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
		const failure = {
			type: "provider",
			code: "provider_invalid_request",
			message: "Terminal stream failure.",
			retryable: false,
			fatal: true,
			retryStatus: { type: "terminal" },
			providerId: "fake",
			modelId: "fake-chat",
		} as const;
		const baseWriter = writerFrom((envelope) => ({
			ok: true,
			eventId: `bridge-${envelope.writeId}`,
			type: "committed",
		}));
		const writer: SessionEventWriter = {
			...baseWriter,
			writeRequestEnd: async (envelope) => {
				closeoutOrder.push("write_request_end");
				requestEnds.push(envelope);
				return await baseWriter.writeRequestEnd(envelope);
			},
			commitRuntimeTermination: async (envelope) => {
				closeoutOrder.push("commit_runtime_termination");
				return runtimeTerminationResultForTest(envelope);
			},
		};
		const service: LLMServiceInterface = {
			stream() {
				return Stream.concat(
					Stream.fromIterable<LLMEvent>([
						{ type: "text-start", id: "stream-terminal-text" },
						{
							type: "text-delta",
							id: "stream-terminal-text",
							text_delta: "partial stream answer",
						},
						{ type: "text-end", id: "stream-terminal-text" },
					]),
					Stream.fail({
						type: "llm-service",
						error: failure,
					} satisfies LLMServiceError),
				);
			},
		};
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, { writer, llmService: service }),
				),
			),
		);
		expect(result).toMatchObject({
			type: "failed",
			error: failure,
			releaseSession: { reason: "terminated" },
		});
		expect(closeoutOrder).toEqual([
			"write_request_end",
			"commit_runtime_termination",
		]);
		expect(requestEnds).toEqual([
			expect.objectContaining({
				isError: true,
				finishReason: "error",
			}),
		]);
		expect(session.state.contextManager.entries()).toEqual([
			{
				messageSequence: 1,
				contextKind: "user",
				parts: [{ type: "text", text: "hello" }],
			},
			{
				messageSequence: 2,
				contextKind: "assistant",
				parts: [{ type: "text", text: "partial stream answer" }],
			},
		]);
	});
	test("processor settlement failure seals durable assistant content before terminal closeout", async () => {
		const session = new ThreadRuntime("sesn_1");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
		const baseWriter = writerFrom((envelope) => ({
			ok: true,
			eventId: `bridge-${envelope.writeId}`,
			type: "committed",
		}));
		const writer: SessionEventWriter = {
			...baseWriter,
			writeRequestEnd: async (envelope) => {
				requestEnds.push(envelope);
				return await baseWriter.writeRequestEnd(envelope);
			},
		};
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						events: [
							{ type: "text-start", id: "processor-failure-text" },
							{
								type: "text-delta",
								id: "processor-failure-text",
								text_delta: "durable before repair",
							},
							{ type: "text-end", id: "processor-failure-text" },
							{ type: "step-start", stepIndex: 1 },
						],
						createProcessor: (options) => {
							const processor = new ProviderStreamAccumulator(options);
							const process = processor.process.bind(processor);
							processor.process = async (source) => {
								if (source.event.type === "step-start") {
									return {
										ok: false,
										events: [],
										error: {
											type: "runtime",
											code: "runtime_invalid_sequence",
											message: "Processor settlement failed.",
											retryable: false,
											fatal: true,
											reason: "runtime_contract_validation",
										},
									};
								}
								return await process(source);
							};
							return processor;
						},
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "failed" });
		expect(requestEnds).toEqual([
			expect.objectContaining({
				isError: true,
				finishReason: "error",
			}),
		]);
		expect(session.state.contextManager.entries()).toEqual([]);
	});
	test("runtime layer discards active draft before terminal provider-error events", async () => {
		const order: string[] = [];
		const session = new ThreadRuntime("sesn_1");
		const store = new ThreadLoopRuntimeStore(order);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const appended: SessionEvent[] = [];
		let failureEventId: string | undefined;
		const writer = writerFrom((envelope) => {
			appended.push(envelope.event);
			const eventId = `bridge-${envelope.writeId}`;
			if (envelope.event.type === "session.error") {
				failureEventId = eventId;
			}
			return { ok: true, eventId, type: "committed" };
		});
		const providerError = {
			type: "provider",
			code: "provider_stream_error",
			message: "Stream failed.",
			retryable: false,
			fatal: true,
			providerId: "fake",
			modelId: "fake-chat",
		} as const;
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
							{ type: "text-start", id: "text-1" },
							{ type: "text-delta", id: "text-1", text_delta: "visible" },
							{ type: "provider-error", error: providerError },
							{ type: "text-start", id: "text-after-error" },
						],
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "failed", error: providerError });
		expect(appended.map((event) => event.type)).toEqual([
			"session.status_running",
			"span.model_request_start",
			"span.model_request_end",
			"session.error",
			"session.status_idle",
		]);
		expect(JSON.stringify(appended)).not.toContain("visible");
		expect(appended.at(3)).toEqual({
			type: "session.error",
			error: { ...providerError, retryStatus: { type: "exhausted" } },
		});
		expect(appended.at(4)).toEqual({
			type: "session.status_idle",
			stop_reason: { type: "retries_exhausted" },
		});
		expect(failureEventId).toBeDefined();
		expect(session.state.threadTurnReduction()).toMatchObject({
			state: { state: "idle" },
			action: { action: "await_input" },
			checkpoint: {
				terminalCloseout: {
					failureEventId,
					disposition: "retries_exhausted",
				},
				idleCloseout: { stopReason: "retries_exhausted" },
			},
		});
		expect(
			session.state.contextManager
				.entries()
				.some((message) => message.contextKind === "assistant"),
		).toBe(false);
		expect(order).toEqual([]);
	});
	test("terminal provider failure preserves completed text with a durable message ACK", async () => {
		const session = new ThreadRuntime("sesn_1");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const appended: SessionEvent[] = [];
		const providerError = {
			type: "provider",
			code: "provider_stream_error",
			message: "Stream failed after completed text.",
			retryable: false,
			fatal: true,
			providerId: "fake",
			modelId: "fake-chat",
		} as const;
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer: writerFrom((envelope) => {
							appended.push(envelope.event);
							return {
								ok: true,
								eventId: `bridge-${envelope.writeId}`,
								type: "committed",
							};
						}),
						events: [
							{ type: "text-start", id: "text-completed" },
							{
								type: "text-delta",
								id: "text-completed",
								text_delta: "durably completed",
							},
							{ type: "text-end", id: "text-completed" },
							{ type: "provider-error", error: providerError },
						],
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "failed", error: providerError });
		expect(appended.filter((event) => event.type === "agent.message")).toEqual([
			{
				type: "agent.message",
				content: [{ type: "text", text: "durably completed" }],
			},
		]);
		expect(session.state.contextManager.entries().at(-1)).toMatchObject({
			contextKind: "assistant",
			parts: [{ type: "text", text: "durably completed" }],
		});
	});
	test("runtime layer does not publish active draft when provider-error terminal append fails", async () => {
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
			(event) =>
				event.type === "session.error" || event.type === "session.status_idle",
		);
		const providerError = {
			type: "provider",
			code: "provider_stream_error",
			message: "Stream failed.",
			retryable: false,
			fatal: true,
			providerId: "fake",
			modelId: "fake-chat",
		} as const;
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
							{ type: "text-start", id: "text-1" },
							{ type: "text-delta", id: "text-1", text_delta: "visible" },
							{ type: "provider-error", error: providerError },
						],
					}),
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
