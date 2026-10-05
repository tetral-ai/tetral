import { expect, test } from "bun:test";
import { Cause, Effect, Exit, Fiber } from "effect";
import { normalizeSessionEventWriterError } from "../../../src/contracts/runtime.js";
import type {
	SessionEventWriter,
	SessionEventWriterRequestEndEnvelope,
} from "../../../src/contracts/runtime.js";
import * as ThreadLoop from "../../../src/thread-loop/thread-loop.js";
import { ThreadRuntime } from "../../../src/thread-loop/thread-runtime.js";
import {
	deferred,
	interruptInput,
	RecordingContextLoader,
	requestEndResultForTest,
	runtimeThreadLoopLayer,
	testRunCustody,
	writerFrom,
} from "./thread-loop-test-support.js";
for (const boundary of [
	"admitted_open_request",
	"failed_reviewer_closeout",
] as const) {
	for (const receipt of ["committed", "duplicate"] as const) {
		test(`${boundary} interrupt joins ${receipt} original Request End before Idle`, async () => {
			const session = new ThreadRuntime("sesn_1");
			session.updateIdentity({
				...session.identity,
				threadRole:
					boundary === "failed_reviewer_closeout"
						? "approval_reviewer"
						: "main",
			});
			session.state.markPersistentContextLoaded();
			session.state.installThreadTurn(
				{
					executionRunId: "original_turn",
					pendingInputContextSequences: [],
					request: {
						modelRequestId: "original_request",
						requestStartEventId: "original_start",
						requestKind:
							boundary === "failed_reviewer_closeout"
								? "approval_reviewer"
								: "agent_provider_request",
						contextThroughMessageSequence: 0,
						toolMembers: [],
					},
				},
				{ routes: [] },
			);
			const entered = deferred<void>();
			const release = deferred<void>();
			const order: string[] = [];
			const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
			let providerCalls = 0;
			let standaloneCommits = 0;
			let idleCalls = 0;
			const baseWriter = writerFrom(
				(envelope) => ({
					ok: true,
					type: "committed",
					eventId: envelope.writeId,
				}),
				async (envelope) => {
					order.push("request_end");
					requestEnds.push(envelope);
					const result = requestEndResultForTest(envelope);
					return result.ok && result.type !== "stale"
						? { ...result, type: receipt }
						: result;
				},
			);
			const writer: SessionEventWriter = {
				...baseWriter,
				finishIdle: async () => {
					idleCalls++;
					if (boundary === "failed_reviewer_closeout" && idleCalls === 1) {
						order.push("failed_closeout_enter");
						entered.resolve();
						await release.promise;
						order.push("failed_closeout_joined");
						return { ok: true, type: "stale" };
					}
					order.push("interrupt_idle");
					return { ok: true, type: "committed", idleEventId: "interrupt_idle" };
				},
			};
			const command = interruptInput("original_interrupt");
			const admit = () =>
				session.state.beginUserInterrupt(command, async () => {
					standaloneCommits++;
					return { ok: true, joined: true };
				});
			if (boundary === "admitted_open_request") admit();
			const fiber = Effect.runFork(
				Effect.gen(function* () {
					const loop = yield* ThreadLoop.Service;
					return yield* loop.run(session, testRunCustody());
				}).pipe(
					Effect.provide(
						runtimeThreadLoopLayer(
							new RecordingContextLoader([], { type: "empty" }),
							{
								writer,
								installLoaderState: false,
								onStream: () => {
									providerCalls++;
								},
							},
						),
					),
				),
			);
			try {
				if (boundary === "failed_reviewer_closeout") {
					await Promise.race([
						entered.promise,
						Effect.runPromise(Fiber.await(fiber)).then((exit) => {
							throw new Error(
								`run exited before closeout: ${JSON.stringify(exit)}`,
							);
						}),
					]);
					admit();
				}
				const interrupt = Effect.runPromise(Fiber.interrupt(fiber));
				// The adapter remains held while cancellation is already requested.
				await Promise.resolve();
				if (boundary === "failed_reviewer_closeout") {
					expect(requestEnds).toHaveLength(0);
					expect(standaloneCommits).toBe(0);
				}
				release.resolve();
				await interrupt;
				const exit = await Effect.runPromise(Fiber.await(fiber));
				if (boundary === "failed_reviewer_closeout") {
					expect(
						Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause),
					).toBe(true);
				}
				expect(requestEnds).toHaveLength(1);
				expect(requestEnds[0]).toMatchObject({
					modelRequestId: "original_request",
					isError: true,
					errorKind: "runtime_interrupted",
					finishReason: "cancelled",
					interruptSettlement: {
						runtimeInputId: command.runtimeInputId,
						interruptLeaseRef: command.interruptLeaseRef,
					},
				});
				expect(order).toEqual(
					boundary === "failed_reviewer_closeout"
						? [
								"failed_closeout_enter",
								"failed_closeout_joined",
								"request_end",
								"interrupt_idle",
							]
						: ["request_end", "interrupt_idle"],
				);
				expect(providerCalls).toBe(0);
				expect(standaloneCommits).toBe(0);
				expect(session.state.userInterruptRequested()).toBe(false);
			} finally {
				release.resolve();
				await Effect.runPromise(Fiber.interrupt(fiber));
			}
		}, 5000);
	}
}
for (const outcome of ["stale", "failed", "malformed", "closed"] as const) {
	test(`open-request interrupt ${outcome} preserves the terminal write fence`, async () => {
		const session = new ThreadRuntime("sesn_1");
		session.state.markPersistentContextLoaded();
		session.state.installThreadTurn(
			{
				executionRunId: "original_turn",
				pendingInputContextSequences: [],
				request: {
					modelRequestId: "original_request",
					requestStartEventId: "original_start",
					requestKind: "agent_provider_request",
					contextThroughMessageSequence: 0,
					toolMembers: [],
					...(outcome === "closed"
						? {
								requestEnd: {
									eventId: "already_committed_end",
									isError: false,
									providerContextRetention: {
										disposition: "completed" as const,
										toolUseEventIds: [],
										repairEventIds: [],
									},
								},
							}
						: {}),
				},
			},
			{ routes: [] },
		);
		if (outcome === "malformed") {
			session.state.contextManager.installOpenRequestDraft({
				modelRequestId: "original_request",
				messageSequence: 1,
				parts: [],
			});
		}
		let ends = 0;
		let idle = 0;
		let standalone = 0;
		let completed = false;
		let reloadRequired = false;
		const baseWriter = writerFrom(
			(envelope) => ({
				ok: true,
				type: "committed",
				eventId: envelope.writeId,
			}),
			async (envelope) => {
				ends++;
				if (outcome === "stale") return { ok: true, type: "stale" };
				if (outcome === "failed")
					return {
						ok: false,
						error: normalizeSessionEventWriterError({
							code: "unavailable",
							sessionId: session.sessionId,
							writeId: envelope.writeId,
						}),
					};
				return requestEndResultForTest(
					envelope,
					outcome === "malformed"
						? { type: "ordinary", sealedMessageSequence: 99 }
						: undefined,
				);
			},
		);
		const writer: SessionEventWriter = {
			...baseWriter,
			finishIdle: async () => {
				idle++;
				return { ok: true, type: "committed", idleEventId: "idle" };
			},
		};
		const command = interruptInput("original_interrupt");
		session.state.beginUserInterrupt(
			command,
			async () => {
				standalone++;
				return { ok: true, joined: true };
			},
			() => {
				completed = true;
			},
		);
		const attempts: unknown[] = [];
		const exit = await Effect.runPromise(
			Effect.gen(function* () {
				const loop = yield* ThreadLoop.Service;
				return yield* loop.run(session, {
					...testRunCustody(),
					recordInterruptAttemptResult: (
						runtimeInputId,
						result,
						disposition,
					) => {
						attempts.push({ runtimeInputId, result });
						reloadRequired ||= disposition?.reloadHotState === true;
					},
				});
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(
						new RecordingContextLoader([], { type: "empty" }),
						{ writer, installLoaderState: false },
					),
				),
				Effect.exit,
			),
		);
		expect(ends).toBe(outcome === "closed" ? 0 : outcome === "failed" ? 3 : 1);
		expect(standalone).toBe(outcome === "closed" ? 1 : 0);
		expect(idle).toBe(outcome === "closed" ? 1 : 0);
		expect(completed).toBe(outcome === "closed");
		expect(reloadRequired).toBe(outcome === "malformed");
		if (outcome === "stale")
			expect(attempts).toEqual([
				{
					runtimeInputId: command.runtimeInputId,
					result: { ok: true, stale: true },
				},
			]);
		if (outcome === "failed" || outcome === "malformed")
			expect(Exit.isFailure(exit)).toBe(true);
	});
}
