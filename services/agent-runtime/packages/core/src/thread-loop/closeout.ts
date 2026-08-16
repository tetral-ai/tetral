/**
 * Builds sub-agent completion declarations and validates durable FinishIdle
 * acknowledgements. ThreadLoop selects the typed closeout action; this module owns
 * failed-run, FinishIdle, the narrow runtime-termination request, and typed-result execution.
 *
 * @packageDocumentation
 */

import type {
	RuntimeContextEntry,
	RuntimeFailure,
	SessionEventWriterAppendResult,
	SessionEventWriterError,
	SessionEventWriterFinishIdleEnvelope,
	SessionEventWriterFinishIdleResult,
	SessionEventWriterRuntimeTerminationEnvelope,
	SessionEventWriterRuntimeTerminationResult,
} from "../contracts/runtime.js";
import {
	isRuntimeTerminationFailure,
	normalizeRuntimeFailure,
	normalizeSessionEventWriterError,
	SessionEventWriterRetryPolicy,
} from "../contracts/runtime.js";
import { runtimeFailureFromProviderError } from "../llm/llm-event.js";
import { completionMailText } from "../runtime/runtime-declaration.js";
import type {
	ThreadLoopRunCustody,
	ThreadLoopRunResult,
	ThreadLoopRuntimeOptions,
} from "./thread-loop.js";
import type { ThreadRuntime } from "./thread-runtime.js";

/** Reports whether failed-run closeout landed or which durable retry disposition applies. */
export type FailedRunCloseoutResult =
	| { readonly type: "landed" }
	| { readonly type: "retry"; readonly error: SessionEventWriterError }
	| { readonly type: "superseded"; readonly error: SessionEventWriterError }
	| { readonly type: "unrepairable"; readonly error: SessionEventWriterError };

type FailedRunCloseoutStepState<Result> =
	| { readonly type: "empty" }
	| { readonly type: "in_flight"; readonly promise: Promise<Result> }
	| { readonly type: "done"; readonly result: Result };

interface FailedRunCloseoutStepMemo<Result> {
	state: FailedRunCloseoutStepState<Result>;
}

function failedRunCloseoutStepState<Result>(
	memo: FailedRunCloseoutStepMemo<Result>,
): FailedRunCloseoutStepState<Result> {
	return memo.state;
}

export interface FailedRunCloseoutMemo {
	readonly errorWriteId: string;
	readonly durableTurnId: string | undefined;
	readonly errorStep: FailedRunCloseoutStepMemo<SessionEventWriterAppendResult>;
	readonly idleStep: FailedRunCloseoutStepMemo<SessionEventWriterFinishIdleResult>;
}

export function createFailedRunCloseoutMemo(
	errorWriteId: string,
	durableTurnId: string | undefined,
): FailedRunCloseoutMemo {
	return {
		errorWriteId,
		durableTurnId,
		errorStep: { state: { type: "empty" } },
		idleStep: { state: { type: "empty" } },
	};
}

type AppendIdleCloseout = (
	options: ThreadLoopRuntimeOptions,
	session: ThreadRuntime,
	custody: ThreadLoopRunCustody,
	stopReason:
		| { readonly type: "end_turn" }
		| { readonly type: "retries_exhausted" },
	failure: RuntimeFailure,
	suppressCompletionMail: boolean,
	failureEventId: string | undefined,
	failedRun: boolean,
) => Promise<
	| { readonly ok: true; readonly eventId: string }
	| { readonly ok: false; readonly error: RuntimeFailure }
>;

/** Applies the terminal failure or failed-idle route for one owned Thread run. */
export async function closeFailedThreadRun(
	options: ThreadLoopRuntimeOptions,
	session: ThreadRuntime,
	custody: ThreadLoopRunCustody,
	result: ThreadLoopRunResult,
	appendIdle: AppendIdleCloseout,
): Promise<ThreadLoopRunResult> {
	if (
		result.type !== "failed" ||
		result.releaseSession?.reason === "event_write_failed"
	) {
		return result;
	}
	const durableTurnId = custody.activeTurnId(session);
	if (durableTurnId === undefined) {
		return result;
	}
	const failure =
		"type" in result.error
			? result.error
			: runtimeFailureFromProviderError(result.error);
	const reviewerRequest =
		session.state.threadTurnReduction().checkpoint.request?.requestKind ===
		"approval_reviewer";
	if (reviewerRequest || !isRuntimeTerminationFailure(failure)) {
		// An internal reviewer request must leave its reusable trunk durably idle;
		// its separately acknowledged outcome owns whether the parent falls back
		// to user approval. Even a fatal request-local failure therefore closes
		// this request turn without terminalizing the reviewer Thread.
		const idle = await appendIdle(
			options,
			session,
			custody,
			failure.retryStatus?.type === "exhausted" && !reviewerRequest
				? { type: "retries_exhausted" }
				: { type: "end_turn" },
			failure,
			false,
			result.failureEventId,
			true,
		);
		if (!idle.ok) {
			return {
				type: "failed",
				error: idle.error,
				releaseSession: { reason: "event_write_failed" },
			};
		}
		if (reviewerRequest) {
			const { releaseSession: _releaseSession, ...reviewerResult } = result;
			return reviewerResult;
		}
		return result;
	}

	const pendingTools = unfinishedToolUseEventIds(session).map(
		(toolUseEventId) => ({ toolUseEventId }),
	);
	const termination = await commitRuntimeTerminationWithRetry(options, {
		workspaceId: session.identity.workspaceId,
		sessionId: session.identity.sessionId,
		sessionThreadId: session.identity.sessionThreadId,
		bindingId: session.identity.bindingId,
		bindingGeneration: session.identity.bindingGeneration,
		targetPodUid: session.identity.targetPodUid,
		writeId: durableTurnId,
		failure,
	});
	if (!termination.ok) {
		return {
			type: "failed",
			error: runtimeFailureFromEventWriter(termination.error),
			releaseSession: { reason: "event_write_failed" },
		};
	}
	if (termination.type === "stale") {
		session.state.clearAfterCustodyHandoff();
		return { type: "interrupted", discardHotState: true };
	}
	session.state.applyThreadTurnFact({
		fact: "terminal_closeout_committed",
		eventId: termination.closeoutEventId,
		failureEventId: termination.failureEventId,
		disposition: "terminated",
	});
	for (const pending of pendingTools) {
		session.state.removePendingApprovalToolJob(pending.toolUseEventId);
	}
	return {
		...result,
		releaseSession: result.releaseSession ?? { reason: "terminated" },
	};
}

export function runtimeFailureFromEventWriter(
	error: Exclude<
		SessionEventWriterAppendResult,
		{ readonly ok: true }
	>["error"],
): RuntimeFailure {
	const runtimeCode =
		error.code === "superseded" || error.code === "unrepairable"
			? "runtime_invalid_sequence"
			: error.code;
	return normalizeRuntimeFailure({
		type: "session-event-writer",
		code: runtimeCode,
		retryable: error.retryable,
		fatal: error.fatal,
		sessionId: error.sessionId,
	});
}

/** Declares one idle transition, applies its closed result, and closes the owned durable turn. */
export async function appendIdleEvent(
	options: ThreadLoopRuntimeOptions,
	session: ThreadRuntime,
	custody: ThreadLoopRunCustody,
	stopReason:
		| { readonly type: "end_turn" }
		| { readonly type: "requires_action"; readonly event_ids: string[] }
		| { readonly type: "retries_exhausted" },
	failure?: RuntimeFailure,
	suppressCompletionMail = false,
	failureEventId?: string,
	failedRun = false,
): Promise<
	| { readonly ok: true; readonly eventId: string }
	| { readonly ok: false; readonly error: RuntimeFailure }
> {
	const durableTurnId = custody.activeTurnId(session);
	if (durableTurnId === undefined) {
		return {
			ok: false,
			error: normalizeRuntimeFailure({
				type: "runtime",
				code: "runtime_invalid_sequence",
				retryable: true,
				fatal: false,
				reason: "runtime_contract_validation",
				sessionId: session.sessionId,
			}),
		};
	}
	let result: SessionEventWriterFinishIdleResult;
	let declaredCompletionMail: string | undefined;
	try {
		declaredCompletionMail = suppressCompletionMail
			? undefined
			: finishIdleCompletionCreate(session, stopReason, failure);
		result =
			options.sessionEventWriter.finishIdle === undefined
				? {
						ok: false,
						error: normalizeSessionEventWriterError({
							code: "unavailable",
							rawError: new Error("finish idle writer is unavailable"),
							sessionId: session.sessionId,
							writeId: durableTurnId,
						}),
					}
				: await finishIdleWithRetry(options, {
						workspaceId: session.identity.workspaceId,
						sessionId: session.sessionId,
						sessionThreadId: session.identity.sessionThreadId,
						bindingId: session.identity.bindingId,
						bindingGeneration: session.identity.bindingGeneration,
						targetPodUid: session.identity.targetPodUid,
						durableTurnId,
						stopReason,
						...(declaredCompletionMail === undefined
							? {}
							: { completionMailText: declaredCompletionMail }),
					});
	} catch (error) {
		result = {
			ok: false,
			error: normalizeSessionEventWriterError({
				code: "unknown",
				rawError: error,
				sessionId: session.sessionId,
				writeId: durableTurnId,
			}),
		};
	}
	if (!result.ok) {
		return { ok: false, error: runtimeFailureFromEventWriter(result.error) };
	}
	const validated = validateFinishIdleResponse(
		session,
		durableTurnId,
		declaredCompletionMail,
		result,
	);
	if (!validated.ok) {
		return { ok: false, error: runtimeFailureFromEventWriter(validated.error) };
	}
	if (
		stopReason.type === "requires_action" &&
		requiresActionTargetsAreAlreadySettled(session, stopReason.event_ids)
	) {
		session.state.closeSettledRequiresActionRun(durableTurnId);
		return { ok: true, eventId: validated.eventId };
	}
	session.state.applyThreadTurnFact({
		fact: "finish_idle_committed",
		eventId: validated.eventId,
		stopReason:
			stopReason.type === "requires_action"
				? { type: "requires_action", eventIds: stopReason.event_ids }
				: stopReason.type === "retries_exhausted"
					? {
							type: "retries_exhausted",
							failureEventId: failureEventId ?? validated.eventId,
							...(failedRun ? { failedRun: true } : {}),
						}
					: { type: "end_turn", ...(failedRun ? { failedRun: true } : {}) },
	});
	return { ok: true, eventId: validated.eventId };
}

function requiresActionTargetsAreAlreadySettled(
	session: ThreadRuntime,
	toolUseEventIds: readonly string[],
): boolean {
	const members = session.state.threadTurnReduction().checkpoint.request?.toolMembers;
	if (members === undefined || toolUseEventIds.length === 0) {
		return false;
	}
	return toolUseEventIds.every((toolUseEventId) =>
		members.some(
			(member) =>
				member.memberKind === "public_tool_use" &&
				member.toolUseEventId === toolUseEventId &&
				member.terminalResult !== undefined,
		),
	);
}

export async function closeFailedRunDurably(
	options: ThreadLoopRuntimeOptions,
	session: ThreadRuntime,
	closeout: FailedRunCloseoutMemo,
	custody: ThreadLoopRunCustody,
	appendFailure: (
		writeId: string,
		failure: RuntimeFailure,
	) => Promise<SessionEventWriterAppendResult>,
): Promise<FailedRunCloseoutResult> {
	let observationController: AbortController | undefined;
	let observationWindow: Promise<void> | undefined;
	const currentObservationWindow = (): Promise<void> => {
		if (observationWindow === undefined) {
			observationController = new AbortController();
			observationWindow = options.runtime
				.sleep(
					SessionEventWriterRetryPolicy.timeoutPerAttemptMs,
					observationController.signal,
				)
				.then(() => undefined);
		}
		return observationWindow;
	};
	const failure = normalizeRuntimeFailure({
		type: "runtime",
		code: "runtime_invalid_sequence",
		retryable: false,
		fatal: true,
		reason: "runtime_contract_validation",
		retryStatus: { type: "terminal" },
		sessionId: session.sessionId,
	});
	try {
		// An unresolved accepted input makes atomic Runtime termination the only
		// legal same-Pod release boundary. FinishIdle cannot disposition Inbox or
		// Queue custody, so failed-run recovery reuses the open durable turn and
		// the existing termination transaction until its typed result lands.
		if (session.state.acceptedInputCount() > 0) {
			const durableTurnId = closeout.durableTurnId;
			if (durableTurnId === undefined) {
				return {
					type: "unrepairable",
					error: normalizeSessionEventWriterError({
						code: "unrepairable",
						sessionId: session.sessionId,
					}),
				};
			}
			const pendingTools = unfinishedToolUseEventIds(session).map(
				(toolUseEventId) => ({ toolUseEventId }),
			);
			const termination = await commitRuntimeTerminationWithRetry(options, {
				workspaceId: session.identity.workspaceId,
				sessionId: session.identity.sessionId,
				sessionThreadId: session.identity.sessionThreadId,
				bindingId: session.identity.bindingId,
				bindingGeneration: session.identity.bindingGeneration,
				targetPodUid: session.identity.targetPodUid,
				writeId: durableTurnId,
				failure,
			});
			if (!termination.ok) {
				return failedRunCloseoutFailure(termination.error);
			}
			if (termination.type === "stale") {
				return {
					type: "superseded",
					error: normalizeSessionEventWriterError({
						code: "superseded",
						sessionId: session.sessionId,
						writeId: durableTurnId,
					}),
				};
			}
			session.state.applyThreadTurnFact({
				fact: "terminal_closeout_committed",
				eventId: termination.closeoutEventId,
				failureEventId: termination.failureEventId,
				disposition: "terminated",
			});
			for (const pending of pendingTools) {
				session.state.removePendingApprovalToolJob(pending.toolUseEventId);
			}
			return { type: "landed" };
		}
		const errorAppend = await observeFailedRunCloseoutStep(
			closeout.errorStep,
			closeout.errorWriteId,
			session.sessionId,
			currentObservationWindow,
			() => appendFailure(closeout.errorWriteId, failure),
			(error) => ({
				ok: false as const,
				error: normalizeSessionEventWriterError({
					code: "unknown",
					rawError: error,
					sessionId: session.sessionId,
					writeId: closeout.errorWriteId,
				}),
			}),
			() => ({
				ok: false as const,
				error: normalizeSessionEventWriterError({
					code: "timeout",
					sessionId: session.sessionId,
					writeId: closeout.errorWriteId,
				}),
			}),
		);
		if (!errorAppend.ok) {
			return failedRunCloseoutFailure(errorAppend.error);
		}
		const durableTurnId = closeout.durableTurnId;
		if (durableTurnId === undefined) {
			return {
				type: "unrepairable",
				error: normalizeSessionEventWriterError({
					code: "unrepairable",
					sessionId: session.sessionId,
				}),
			};
		}
		const idleAppend = await observeFailedRunCloseoutStep(
			closeout.idleStep,
			durableTurnId,
			session.sessionId,
			currentObservationWindow,
			() =>
				finishIdleWithRetry(options, {
					workspaceId: session.identity.workspaceId,
					sessionId: session.sessionId,
					sessionThreadId: session.identity.sessionThreadId,
					bindingId: session.identity.bindingId,
					bindingGeneration: session.identity.bindingGeneration,
					targetPodUid: session.identity.targetPodUid,
					durableTurnId,
					stopReason: { type: "end_turn" },
				}),
			(error) => ({
				ok: false as const,
				error: normalizeSessionEventWriterError({
					code: "unknown",
					rawError: error,
					sessionId: session.sessionId,
					writeId: durableTurnId,
				}),
			}),
			() => ({
				ok: false as const,
				error: normalizeSessionEventWriterError({
					code: "timeout",
					sessionId: session.sessionId,
					writeId: durableTurnId,
				}),
			}),
		);
		if (!idleAppend.ok) {
			return failedRunCloseoutFailure(idleAppend.error);
		}
		const validatedIdle = validateFinishIdleResponse(
			session,
			durableTurnId,
			undefined,
			idleAppend,
		);
		if (!validatedIdle.ok) {
			return failedRunCloseoutFailure(validatedIdle.error);
		}
		if (
			session.state.threadTurnReduction().checkpoint.idleCloseout?.eventId !==
			validatedIdle.eventId
		) {
			session.state.applyThreadTurnFact({
				fact: "finish_idle_committed",
				eventId: validatedIdle.eventId,
				stopReason: { type: "end_turn", failedRun: true },
			});
		}
		return { type: "landed" };
	} finally {
		observationController?.abort();
	}
}

function failedRunCloseoutFailure(
	error: SessionEventWriterError,
): FailedRunCloseoutResult {
	if (error.code === "superseded") {
		return { type: "superseded", error };
	}
	if (
		error.code === "unrepairable" ||
		error.code === "schema_mismatch" ||
		error.code === "ack_mismatch"
	) {
		return { type: "unrepairable", error };
	}
	return { type: "retry", error };
}

// A timed-out observation retains its in-flight write; later callers observe
// the same operation instead of abandoning or duplicating it.
async function observeFailedRunCloseoutStep<
	Result extends { readonly ok: boolean },
>(
	memo: FailedRunCloseoutStepMemo<Result>,
	writeId: string,
	sessionId: string,
	observationWindow: () => Promise<void>,
	start: () => Promise<Result>,
	rejectionResult: (error: unknown) => Result,
	timeoutResult: () => Result,
): Promise<Result> {
	if (memo.state.type === "done") {
		return memo.state.result;
	}
	let inFlight: Promise<Result>;
	if (memo.state.type === "empty") {
		const promise = start().then(
			(result) => {
				memo.state = result.ok ? { type: "done", result } : { type: "empty" };
				return result;
			},
			(error) => {
				memo.state = { type: "empty" };
				return rejectionResult(error);
			},
		);
		memo.state = { type: "in_flight", promise };
		inFlight = promise;
	} else {
		inFlight = memo.state.promise;
	}
	await Promise.resolve();
	const stateAfterMicrotask = failedRunCloseoutStepState(memo);
	if (stateAfterMicrotask.type === "done") {
		return stateAfterMicrotask.result;
	}
	const observed = await Promise.race([
		inFlight.then((result) => ({ type: "result" as const, result })),
		observationWindow().then(() => ({
			type: "timeout" as const,
			result: timeoutResult(),
		})),
	]);
	if (observed.type === "result") {
		return observed.result;
	}
	return observed.result;
}

export async function finishIdleWithRetry(
	options: ThreadLoopRuntimeOptions,
	envelope: SessionEventWriterFinishIdleEnvelope,
): Promise<SessionEventWriterFinishIdleResult> {
	if (options.sessionEventWriter.finishIdle === undefined) {
		return writerUnavailable(envelope.sessionId, envelope.durableTurnId);
	}
	let lastFailure: SessionEventWriterFinishIdleResult | undefined;
	for (
		let attempt = 1;
		attempt <= SessionEventWriterRetryPolicy.attempts;
		attempt += 1
	) {
		const result = await finishIdleWithTimeout(options, envelope);
		if (result.ok) return result;
		lastFailure = result;
		if (
			!result.error.retryable ||
			attempt === SessionEventWriterRetryPolicy.attempts
		) {
			return result;
		}
		await retryBackoff(options, attempt);
	}
	return (
		lastFailure ?? writerUnknown(envelope.sessionId, envelope.durableTurnId)
	);
}

export async function commitRuntimeTerminationWithRetry(
	options: ThreadLoopRuntimeOptions,
	envelope: SessionEventWriterRuntimeTerminationEnvelope,
): Promise<SessionEventWriterRuntimeTerminationResult> {
	if (options.sessionEventWriter.commitRuntimeTermination === undefined) {
		return {
			ok: false,
			error: normalizeSessionEventWriterError({
				code: "unavailable",
				sessionId: envelope.sessionId,
				writeId: envelope.writeId,
			}),
		};
	}
	let lastFailure: SessionEventWriterRuntimeTerminationResult | undefined;
	for (
		let attempt = 1;
		attempt <= SessionEventWriterRetryPolicy.attempts;
		attempt += 1
	) {
		const result = await Promise.race([
			commitRuntimeTerminationOnce(options, envelope),
			options.runtime
				.sleep(
					SessionEventWriterRetryPolicy.timeoutPerAttemptMs,
					new AbortController().signal,
				)
				.then(
					(): SessionEventWriterRuntimeTerminationResult => ({
						ok: false,
						error: normalizeSessionEventWriterError({
							code: "timeout",
							sessionId: envelope.sessionId,
							writeId: envelope.writeId,
						}),
					}),
				),
		]);
		if (result.ok) {
			return result;
		}
		lastFailure = result;
		if (
			!result.error.retryable ||
			attempt === SessionEventWriterRetryPolicy.attempts
		) {
			return result;
		}
		await retryBackoff(options, attempt);
	}
	return (
		lastFailure ?? {
			ok: false,
			error: normalizeSessionEventWriterError({
				code: "unknown",
				sessionId: envelope.sessionId,
				writeId: envelope.writeId,
			}),
		}
	);
}

async function commitRuntimeTerminationOnce(
	options: ThreadLoopRuntimeOptions,
	envelope: SessionEventWriterRuntimeTerminationEnvelope,
): Promise<SessionEventWriterRuntimeTerminationResult> {
	const startedAt = options.runtime.monotonicMs();
	try {
		const result =
			await options.sessionEventWriter.commitRuntimeTermination!(envelope);
		observeEventWrite(
			options,
			"commit_runtime_termination",
			startedAt,
			result.ok ? "success" : "error",
		);
		return result;
	} catch (error) {
		observeEventWrite(
			options,
			"commit_runtime_termination",
			startedAt,
			"error",
		);
		return {
			ok: false,
			error: normalizeSessionEventWriterError({
				code: "unknown",
				rawError: error,
				sessionId: envelope.sessionId,
				writeId: envelope.writeId,
			}),
		};
	}
}

async function finishIdleWithTimeout(
	options: ThreadLoopRuntimeOptions,
	envelope: SessionEventWriterFinishIdleEnvelope,
): Promise<SessionEventWriterFinishIdleResult> {
	const rawOperation = finishIdleOnce(options, envelope);
	const timeoutController = new AbortController();
	const first = await Promise.race([
		rawOperation.then((result) => ({ type: "raw" as const, result })),
		options.runtime
			.sleep(
				SessionEventWriterRetryPolicy.timeoutPerAttemptMs,
				timeoutController.signal,
			)
			.then(() => ({ type: "local_timeout" as const })),
	]);
	if (first.type === "raw") {
		timeoutController.abort();
		return first.result;
	}
	// The transport has no cancellation contract, so ownership stays with the raw write.
	return await rawOperation;
}

async function finishIdleOnce(
	options: ThreadLoopRuntimeOptions,
	envelope: SessionEventWriterFinishIdleEnvelope,
): Promise<SessionEventWriterFinishIdleResult> {
	const startedAt = options.runtime.monotonicMs();
	try {
		const result = await options.sessionEventWriter.finishIdle!(envelope);
		observeEventWrite(
			options,
			"finish_idle",
			startedAt,
			result.ok ? "success" : "error",
		);
		return result;
	} catch (error) {
		observeEventWrite(options, "finish_idle", startedAt, "error");
		return {
			ok: false,
			error: normalizeSessionEventWriterError({
				code: "unknown",
				rawError: error,
				sessionId: envelope.sessionId,
				writeId: envelope.durableTurnId,
			}),
		};
	}
}

async function retryBackoff(
	options: ThreadLoopRuntimeOptions,
	attempt: number,
): Promise<void> {
	const backoffMs = SessionEventWriterRetryPolicy.backoffMs[attempt - 1] ?? 0;
	if (backoffMs > 0) {
		await options.runtime.sleep(backoffMs, new AbortController().signal);
	}
}

function observeEventWrite(
	options: ThreadLoopRuntimeOptions,
	operation: "finish_idle" | "commit_runtime_termination",
	startedAt: number,
	outcome: "success" | "error",
): void {
	options.metrics?.observeEventWriteLatency(
		operation,
		options.runtime.monotonicMs() - startedAt,
		outcome,
	);
}

function writerUnavailable(
	sessionId: string,
	writeId: string,
): SessionEventWriterFinishIdleResult {
	return {
		ok: false,
		error: normalizeSessionEventWriterError({
			code: "unavailable",
			sessionId,
			writeId,
		}),
	};
}

function writerUnknown(
	sessionId: string,
	writeId: string,
): SessionEventWriterFinishIdleResult {
	return {
		ok: false,
		error: normalizeSessionEventWriterError({
			code: "unknown",
			sessionId,
			writeId,
		}),
	};
}

export function finishIdleCompletionCreate(
	runtimeThread: ThreadRuntime,
	stopReason: SessionEventWriterFinishIdleEnvelope["stopReason"],
	failure: RuntimeFailure | undefined,
): string | undefined {
	if (
		runtimeThread.identity.threadRole !== "subagent" ||
		stopReason.type === "requires_action"
	) {
		return undefined;
	}
	const sender = runtimeThread.identity.taskName;
	if (sender === undefined || sender.length === 0) {
		throw new Error("sub-agent completion sender has no task name");
	}
	const payload =
		failure === undefined
			? finalAssistantText(runtimeThread.state.contextManager.entries())
			: completionMailErrorPayload(failure.message);
	return completionMailText(
		[
			"Message Type: FINAL_ANSWER",
			`Task name: ${runtimeThread.identity.parentTaskName ?? "main"}`,
			`Sender: ${sender}`,
			"Payload:",
			payload,
		].join("\n"),
	);
}

export function validateFinishIdleResponse(
	runtimeThread: ThreadRuntime,
	durableTurnId: string,
	_declaredCompletionMail: string | undefined,
	result: Extract<SessionEventWriterFinishIdleResult, { readonly ok: true }>,
):
	| { readonly ok: true; readonly eventId: string }
	| { readonly ok: false; readonly error: SessionEventWriterError } {
	if (result.type === "stale") {
		return {
			ok: false,
			error: normalizeSessionEventWriterError({
				code: "superseded",
				sessionId: runtimeThread.sessionId,
				writeId: durableTurnId,
			}),
		};
	}
	return { ok: true, eventId: result.idleEventId };
}

function finalAssistantText(entries: readonly RuntimeContextEntry[]): string {
	for (let index = entries.length - 1; index >= 0; index -= 1) {
		const entry = entries[index]!;
		if (entry.contextKind === "assistant") {
			return entry.parts
				.flatMap((part) => (part.type === "text" ? [part.text] : []))
				.join("");
		}
	}
	return "";
}

function unfinishedToolUseEventIds(runtimeThread: ThreadRuntime): string[] {
	return (
		runtimeThread.state
			.threadTurnReduction()
			.checkpoint.request?.toolMembers.flatMap((member) =>
				member.memberKind === "public_tool_use" &&
				member.terminalResult === undefined
					? [member.toolUseEventId]
					: [],
			) ?? []
	);
}

const CompletionMailErrorReasonMaxBytes = 3600;
const CompletionMailErrorGuidance =
	"This agent's turn failed. If you still need this agent, use the available collaboration tools to give it another task.";

function completionMailErrorPayload(reason: string): string {
	return `Agent errored: ${middleTruncateCompletionReason(reason)}\n\n${CompletionMailErrorGuidance}`;
}

function middleTruncateCompletionReason(reason: string): string {
	const bytes = new TextEncoder().encode(reason);
	if (bytes.length <= CompletionMailErrorReasonMaxBytes) {
		return reason;
	}
	const halfBudget = CompletionMailErrorReasonMaxBytes / 2;
	let headEnd = halfBudget;
	while (headEnd > 0 && (bytes[headEnd]! & 0xc0) === 0x80) {
		headEnd -= 1;
	}
	let tailStart = bytes.length - halfBudget;
	while (tailStart < bytes.length && (bytes[tailStart]! & 0xc0) === 0x80) {
		tailStart += 1;
	}
	const removedBytes = bytes.length - CompletionMailErrorReasonMaxBytes;
	const removedTokens = Math.ceil(removedBytes / 4);
	const decoder = new TextDecoder();
	return `${decoder.decode(bytes.slice(0, headEnd))}…${removedTokens} tokens truncated…${decoder.decode(bytes.slice(tailStart))}`;
}
