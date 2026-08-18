/**
 * Adapts Runtime Core approval requests to isolated internal reviewer-thread
 * execution. ThreadLoop calls the reviewer returned by this module when
 * ToolGate requires a model decision; Runtime Pod command assembly supplies
 * the platform reviewer model, packaged policy assets, and Bridge-backed thread
 * lifecycle adapter.
 *
 * The orchestration keeps review and sidecar identities deterministic, reuses
 * one freshly minted trunk id for the parent hot lifetime, serializes trunk work
 * while using sidecars for overlap, labels parent evidence as a full re-anchor or
 * cursor delta, and lets cancellation race with exactly one outcome owner. It
 * validates the reviewer's exact JSON result before asking the hot
 * Runtime host to commit a decision or failure event through the shared durable
 * writer policy. In turn it calls
 * RuntimeSubAgentRunHost for preload, enqueue, wait, inspection, interruption,
 * hot close, and durable review writes, and calls the injected thread creator
 * for Bridge-owned child-thread creation and closure.
 */
import { createHash, randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import type {
	RuntimeContextEntry,
	RuntimeContextPart,
	RuntimeJsonValue,
	SessionEvent,
	SessionEventWriterAppendResult,
	SessionEventWriterError,
} from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import { normalizeSessionEventWriterError } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import type { ReviewerExecutionToken } from "@tetral/agent-runtime-core/src/session/session-manager.js";
import type { RuntimeModelRef } from "@tetral/agent-runtime-core/src/thread-loop/thread-loop.js";
import type {
	RuntimeAcceptedInputState,
	RuntimeThreadAddressState,
} from "@tetral/agent-runtime-core/src/thread-loop/thread-state.js";
import type {
	RuntimeApprovalReviewer,
	RuntimeApprovalReviewRequest,
	RuntimeApprovalReviewResult,
} from "@tetral/agent-runtime-core/src/thread-loop/tool-execution.js";
import type {
	ApprovalReviewerOutcome,
	ApprovalReviewRiskLevel,
	ApprovalReviewUserAuthorization,
} from "@tetral/agent-runtime-core/src/tools/tool-gate.js";
import { semanticErrorFields } from "@tetral/ts-observability";
import { Effect, Fiber } from "effect";
import type { RuntimeSubAgentRunHost } from "./core-hosts.js";
import type { RuntimePodLogger } from "./logger.js";

const DefaultReviewerWaitTimeoutMs = 120_000;
const FailureMessageMaxLength = 512;
const ParentTranscriptDeltaNote =
	"History added since your last assessment — continue the same review conversation.";
const ParentTranscriptReanchorNote =
	"Full parent transcript — treat this as your first assessment of this conversation; it may repeat evidence you have seen before.";

type ApprovalReviewFailureEvent = Extract<
	SessionEvent,
	{ readonly type: "approval_review.failure" }
>;
type ApprovalReviewFailureKind = ApprovalReviewFailureEvent["failure_kind"];
type ParsedApprovalReviewerOutcome =
	| Extract<ApprovalReviewerOutcome, { readonly type: "decision" }>
	| {
			readonly type: "failed";
			readonly message: string;
			readonly failureKind: ApprovalReviewFailureKind;
	  };

/** Supplies the policy instructions and exact JSON output schema for reviewer runs. */
export interface ApprovalReviewerAssets {
	/** Platform-owned instructions added to approval-reviewer provider requests. */
	readonly policyPrompt: string;
	/** Serialized schema included in each review prompt and parsed before use. */
	readonly outputSchemaJson: string;
}

/**
 * Provides the durable child-thread lifecycle used around hot reviewer
 * execution. The Runtime Pod implementation delegates these operations to the
 * Agent Runtime Bridge and reports failures as bounded, model-independent
 * messages.
 */
export interface RuntimeApprovalReviewerThreadCreator {
	/** Ensures the review trunk or sidecar exists before Runtime Core preloads it. */
	readonly createApprovalReviewerThread: (
		input: ApprovalReviewerThreadCreation,
	) => Promise<
		| {
				readonly ok: true;
				readonly reviewerThreadId?: string;
				readonly runtimeInputId?: string;
		  }
		| {
				readonly ok: false;
				readonly message: string;
				readonly staleCustody?: true;
		  }
	>;
	/** Durably closes a sidecar before the corresponding hot thread is released. */
	readonly closeApprovalReviewerThread: (
		input: ApprovalReviewerThreadCreation,
	) => Promise<
		| { readonly ok: true }
		| {
				readonly ok: false;
				readonly message: string;
				readonly discardHotState?: boolean;
		  }
	>;
}

/**
 * Describes one durable reviewer thread operation with the parent request scope
 * and reviewer identity needed by the Bridge adapter. A trunk uses the manager's
 * fresh hot-lifetime id and starts without a context prefix; a sidecar uses its
 * deterministic review id while Bridge selects the durable parent context.
 */
export interface ApprovalReviewerThreadCreation {
	readonly request: RuntimeApprovalReviewRequest;
	readonly reviewId: string;
	readonly isTrunk: boolean;
	readonly ensureOperationId?: string;
	readonly reviewerThreadId?: string;
}

export interface ApprovalReviewerThreadClose
	extends ApprovalReviewerThreadCreation {
	readonly reviewerThreadId: string;
}

/**
 * Creates the approval callback consumed by Runtime Core ThreadLoop.
 *
 * The returned effect coordinates review leases through the request's
 * approval-reviewer manager, runs the selected trunk or sidecar through the
 * supplied hot Runtime host, accepts only an exact allow-or-deny JSON shape,
 * and commits the resulting decision before caching and returning it. Runtime,
 * timeout, parse, and caller-cancellation defects are normalized as uncertain
 * settlement after the review lease has initiated its owned cleanup.
 */
export function createRuntimeApprovalReviewer(
	hostRef: () => RuntimeSubAgentRunHost | undefined,
	options: {
		readonly model: RuntimeModelRef;
		readonly threadCreator: RuntimeApprovalReviewerThreadCreator;
		readonly createId?: (prefix: string) => string;
		readonly waitTimeoutMs?: number;
		readonly logger?: RuntimePodLogger;
		readonly assets?: ApprovalReviewerAssets;
	},
): RuntimeApprovalReviewer {
	const waitTimeoutMs = options.waitTimeoutMs ?? DefaultReviewerWaitTimeoutMs;
	const createId =
		options.createId ?? ((prefix: string) => `${prefix}_${randomUUID()}`);
	const reviewerModel = options.model;
	const assets = options.assets ?? loadApprovalReviewerAssets();
	return (request) =>
		Effect.gen(function* () {
			const manager = request.approvalReviewerManager;
			const cacheKey = reviewCacheKey(request);
			const cached = manager.decisionFor(cacheKey);
			if (cached !== undefined) {
				return cached;
			}
			const reviewId = stableId("arvw", cacheKey);
			const persistenceUncertainFailure = (
				message: string,
			): RuntimeApprovalReviewResult => {
				logApprovalReviewFailure(
					options.logger,
					request,
					reviewId,
					"runtime_failure",
					message,
				);
				return {
					type: "settlement_failed",
					error: normalizeSessionEventWriterError({
						code: "unknown",
						sessionId: request.sessionId,
						writeId: `rwrite_${reviewId}_failure`,
					}),
				};
			};
			const host = hostRef();
			if (host === undefined) {
				return persistenceUncertainFailure("approval reviewer is unavailable");
			}
			const lease = manager.beginReview(reviewId);
			const currentParentBoundaryEventId = request.parentBoundaryEventId;
			if (currentParentBoundaryEventId.length === 0) {
				lease.release();
				return persistenceUncertainFailure(
					"approval reviewer parent transcript has no durable boundary",
				);
			}
			let executionThreadId: string;
			let runtimeInputId: string;
			let executionCreation: ApprovalReviewerThreadCreation;
			let sidecarCreated = false;
			if (lease.kind === "trunk") {
				const trunkCreation: ApprovalReviewerThreadCreation = {
					request,
					reviewId,
					isTrunk: true,
					ensureOperationId: manager.trunkEnsureOperationId(createId),
				};
				const ensured = yield* Effect.promise(() =>
					options.threadCreator.createApprovalReviewerThread(trunkCreation),
				).pipe(
					Effect.catchCause(() =>
						Effect.succeed({
							ok: false as const,
							message: "approval reviewer trunk creation failed",
						}),
					),
				);
				if (!ensured.ok) {
					lease.release();
					if ("staleCustody" in ensured && ensured.staleCustody === true) {
						return { type: "stale_custody" as const };
					}
					return persistenceUncertainFailure(ensured.message);
				}
				if (
					ensured.reviewerThreadId === undefined ||
					ensured.runtimeInputId === undefined ||
					!manager.installTrunkThreadId(ensured.reviewerThreadId)
				) {
					lease.release();
					return persistenceUncertainFailure(
						"approval reviewer trunk identity conflicts",
					);
				}
				executionThreadId = ensured.reviewerThreadId;
				runtimeInputId = ensured.runtimeInputId;
				executionCreation = {
					...trunkCreation,
					reviewerThreadId: executionThreadId,
				};
			} else {
				const sidecarRequest: ApprovalReviewerThreadCreation = {
					request,
					reviewId,
					isTrunk: false,
				};
				const created = yield* Effect.promise(() =>
					options.threadCreator.createApprovalReviewerThread(sidecarRequest),
				).pipe(Effect.catchCause(() => Effect.succeed(undefined)));
				if (
					created === undefined ||
					!created.ok ||
					created.reviewerThreadId === undefined ||
					created.runtimeInputId === undefined
				) {
					lease.release();
					if (created !== undefined && !created.ok && created.staleCustody === true) {
						return { type: "stale_custody" as const };
					}
					return persistenceUncertainFailure(
						created !== undefined && !created.ok
							? created.message
							: "approval reviewer sidecar creation failed",
					);
				}
				executionThreadId = created.reviewerThreadId;
				runtimeInputId = created.runtimeInputId;
				executionCreation = {
					...sidecarRequest,
					reviewerThreadId: executionThreadId,
				};
				sidecarCreated = true;
			}
			const feed = manager.parentTranscriptFeed(request.parentTranscript);
			const control = reviewThreadControl(request, executionThreadId);
			const input = approvalReviewInput(
				request,
				control,
				0,
				reviewId,
				runtimeInputId,
				reviewerModel,
				assets,
				feed.entries,
				feed.reanchored,
			);
			let requestQuiescent = false;
			let releaseInFinally = true;
			let resourcesReleased = false;
			const releaseResources = () => {
				if (resourcesReleased) {
					return;
				}
				resourcesReleased = true;
				lease.release();
			};
			const quarantineUnconfirmedExecution = (): Effect.Effect<void> =>
				Effect.gen(function* () {
					if (lease.kind === "trunk") {
						manager.discardTrunk(executionThreadId);
					}
					yield* manager.fork(
						Effect.never.pipe(Effect.ensuring(Effect.sync(releaseResources))),
					);
					releaseInFinally = false;
					yield* Effect.yieldNow;
				});
			const settleCancellation = (): Effect.Effect<void> =>
				Effect.gen(function* () {
					const token = lease.executionToken();
					if (token === undefined) {
						if (lease.kind === "trunk") {
							manager.discardTrunk(executionThreadId);
						}
						return;
					}
					requestQuiescent = yield* quiesceReviewerRequestEffect(
						host,
						control,
						token,
					);
					if (!requestQuiescent && lease.kind === "trunk") {
						manager.discardTrunk(executionThreadId);
					}
					if (!requestQuiescent) {
						logApprovalReviewLifecycleFailure(
							options.logger,
							request,
							reviewId,
							lease.kind,
							"quiescence_unconfirmed",
						);
					}
					if (requestQuiescent && lease.kind === "trunk") {
						const released = yield* releaseReviewerExecutionEffect(
							host,
							control,
							token,
						);
						if (!released) {
							manager.discardTrunk(executionThreadId);
							logApprovalReviewLifecycleFailure(
								options.logger,
								request,
								reviewId,
								lease.kind,
								"hot_release_failed",
							);
						}
					}
					if (requestQuiescent && lease.kind === "sidecar" && sidecarCreated) {
						yield* closeApprovalReviewerSidecarEffect(
							host,
							options.threadCreator,
							executionCreation,
							control,
							token,
							options.logger,
							request,
							reviewId,
						);
					}
				});
			const failWithRecord = (
				failureKind: ApprovalReviewFailureKind,
				message: string,
				ownerSettlement: Effect.Effect<boolean> | undefined = undefined,
			): Effect.Effect<RuntimeApprovalReviewResult> =>
				Effect.gen(function* () {
					let quiescenceAttempted = false;
					if (ownerSettlement !== undefined) {
						requestQuiescent = yield* ownerSettlement;
						quiescenceAttempted = true;
					}
					const event = approvalReviewFailureEvent(input, failureKind, message);
					const settlement = yield* commitReviewerOutcomeEffect(
						() => host.commitApprovalReviewFailure(input, event),
						input.sessionId,
						`rwrite_${reviewId}_failure`,
					);
					const token = lease.executionToken();
					if (settlement.type === "acknowledged" && !lease.claimOutcome()) {
						yield* settleCancellation();
						return {
							type: "failed",
							message: "approval reviewer was cancelled",
						};
					}
					if (
						(settlement.type !== "acknowledged" || !requestQuiescent) &&
						!quiescenceAttempted
					) {
						if (token !== undefined) {
							requestQuiescent = yield* quiesceReviewerRequestEffect(
								host,
								control,
								token,
							);
							quiescenceAttempted = true;
						}
					}
					if (settlement.type !== "acknowledged") {
						if (requestQuiescent && token !== undefined) {
							const evicted = yield* evictReviewerExecutionEffect(
								host,
								control,
								token,
							);
							if (!evicted) {
								logApprovalReviewLifecycleFailure(
									options.logger,
									request,
									reviewId,
									lease.kind,
									"hot_eviction_failed",
								);
							}
						}
						if (lease.kind === "trunk") {
							manager.discardTrunk(executionThreadId);
						}
						if (!requestQuiescent) {
							yield* quarantineUnconfirmedExecution();
						}
						if (settlement.type === "stale_custody") {
							return { type: "stale_custody" };
						}
						logApprovalReviewSettlementFailure(
							options.logger,
							request,
							reviewId,
							settlement,
						);
						return { type: "settlement_failed", error: settlement.error };
					}
					if (lease.kind === "sidecar" && sidecarCreated && requestQuiescent) {
						yield* closeApprovalReviewerSidecarEffect(
							host,
							options.threadCreator,
							executionCreation,
							control,
							token,
							options.logger,
							request,
							reviewId,
						);
					}
					if (lease.kind === "trunk" && requestQuiescent) {
						const released =
							token !== undefined &&
							(yield* releaseReviewerExecutionEffect(host, control, token));
						if (!released) {
							manager.discardTrunk(executionThreadId);
							logApprovalReviewLifecycleFailure(
								options.logger,
								request,
								reviewId,
								lease.kind,
								"hot_release_failed",
							);
						}
					}
					if (lease.kind === "trunk" && !requestQuiescent) {
						manager.discardTrunk(executionThreadId);
					}
					if (!requestQuiescent) {
						logApprovalReviewLifecycleFailure(
							options.logger,
							request,
							reviewId,
							lease.kind,
							"quiescence_unconfirmed",
						);
						yield* quarantineUnconfirmedExecution();
						return {
							type: "settlement_failed",
							error: normalizeSessionEventWriterError({
								code: "unknown",
								sessionId: input.sessionId,
								writeId: `rwrite_${reviewId}_failure`,
							}),
						};
					}
					return { type: "failed", message };
				});

			const owner = Effect.gen(function* () {
				if (lease.raceState() === "cancellation_won") {
					yield* settleCancellation();
					return {
						type: "failed" as const,
						message: "approval reviewer was cancelled",
					};
				}
				const preloaded = yield* Effect.promise(() =>
					host.preloadThread({
						...control,
						thread: input.thread,
					}),
				).pipe(Effect.catchCause(() => Effect.succeed(undefined)));
				if (
					preloaded === undefined ||
					(!preloaded.ok && preloaded.reason !== "thread_busy")
				) {
					requestQuiescent = true;
					return yield* failWithRecord(
						"runtime_failure",
						"approval reviewer preload failed",
					);
				}
				if (lease.raceState() === "cancellation_won") {
					yield* settleCancellation();
					return {
						type: "failed" as const,
						message: "approval reviewer was cancelled",
					};
				}
				const accepted = yield* Effect.promise(() =>
					host.enqueueThreadInput(input),
				).pipe(Effect.catchCause(() => Effect.succeed(undefined)));
				const executionToken =
					accepted?.ok === true ? accepted.reviewerExecutionToken : undefined;
				const tokenInstalled =
					executionToken !== undefined &&
					executionToken.reviewId === reviewId &&
					executionToken.reviewerThreadId === executionThreadId &&
					lease.installExecutionToken(executionToken);
				if (lease.raceState() === "cancellation_won" && tokenInstalled) {
					yield* settleCancellation();
					return {
						type: "failed" as const,
						message: "approval reviewer was cancelled",
					};
				}
				if (accepted?.ok === true) {
					if (!tokenInstalled || executionToken === undefined) {
						return yield* failWithRecord(
							"runtime_failure",
							"approval reviewer input was rejected",
						);
					}
				} else {
					if (accepted === undefined) {
						return yield* failWithRecord(
							"runtime_failure",
							"approval reviewer input was rejected",
						);
					}
					requestQuiescent = true;
					return yield* failWithRecord(
						"runtime_failure",
						"approval reviewer input was rejected",
					);
				}
				const waitAbortController = new AbortController();
				const unregisterCancellation = lease.onCancellation(() =>
					waitAbortController.abort(),
				);
				const waited = yield* Effect.promise(() =>
					host.waitReviewerExecution(
						control,
						executionToken,
						waitTimeoutMs,
						waitAbortController.signal,
					),
				).pipe(
					Effect.catchCause(() => Effect.succeed(undefined)),
					Effect.ensuring(Effect.sync(unregisterCancellation)),
				);
				if (lease.raceState() === "cancellation_won") {
					yield* settleCancellation();
					return {
						type: "failed" as const,
						message: "approval reviewer was cancelled",
					};
				}
				if (
					waited === undefined ||
					!waited.ok ||
					(!waited.timedOut && !waited.terminal)
				) {
					return yield* failWithRecord(
						"runtime_failure",
						"approval reviewer wait failed",
					);
				}
				if (waited.timedOut) {
					return yield* failWithRecord(
						"timeout",
						"approval reviewer timed out",
						quiesceReviewerRequestEffect(host, control, executionToken),
					);
				}
				requestQuiescent = true;
				const snapshot = yield* Effect.promise(() =>
					host.inspectReviewerExecution(control, executionToken),
				).pipe(Effect.catchCause(() => Effect.succeed(undefined)));
				if (snapshot === undefined || !snapshot.ok || !snapshot.observed) {
					return yield* failWithRecord(
						"runtime_failure",
						"approval reviewer decision is unavailable",
					);
				}
				const decision = parseApprovalDecision(snapshot.entries);
				if (decision.type !== "decision") {
					return yield* failWithRecord(decision.failureKind, decision.message);
				}
				const committed = yield* commitReviewerOutcomeEffect(
					() =>
						host.commitApprovalReviewDecision(
							input,
							approvalReviewDecisionEvent(input, decision),
						),
					input.sessionId,
					`rwrite_${reviewId}_decision`,
					waitTimeoutMs,
				);
				if (committed.type === "acknowledged" && !lease.claimOutcome()) {
					yield* settleCancellation();
					return {
						type: "failed" as const,
						message: "approval reviewer was cancelled",
					};
				}
				if (committed.type !== "acknowledged") {
					requestQuiescent = yield* quiesceReviewerRequestEffect(
						host,
						control,
						executionToken,
					);
					if (requestQuiescent) {
						const evicted = yield* evictReviewerExecutionEffect(
							host,
							control,
							executionToken,
						);
						if (!evicted) {
							logApprovalReviewLifecycleFailure(
								options.logger,
								request,
								reviewId,
								lease.kind,
								"hot_eviction_failed",
							);
						}
					} else {
						logApprovalReviewLifecycleFailure(
							options.logger,
							request,
							reviewId,
							lease.kind,
							"quiescence_unconfirmed",
						);
						yield* quarantineUnconfirmedExecution();
					}
					if (lease.kind === "trunk") {
						manager.discardTrunk(executionThreadId);
					}
					if (committed.type === "stale_custody") {
						return { type: "stale_custody" as const };
					}
					logApprovalReviewSettlementFailure(
						options.logger,
						request,
						reviewId,
						committed,
					);
					return { type: "settlement_failed" as const, error: committed.error };
				}
				if (lease.kind === "trunk") {
					manager.completeTrunkReview(request.parentTranscript);
					const released = yield* releaseReviewerExecutionEffect(
						host,
						control,
						executionToken,
					);
					if (!released) {
						manager.discardTrunk(executionThreadId);
						logApprovalReviewLifecycleFailure(
							options.logger,
							request,
							reviewId,
							lease.kind,
							"hot_release_failed",
						);
					}
				} else {
					const closeOutcome = yield* closeApprovalReviewerSidecarEffect(
						host,
						options.threadCreator,
						executionCreation,
						control,
						executionToken,
						options.logger,
						request,
						reviewId,
					);
					if (closeOutcome === "stale_custody") {
						return { type: "stale_custody" as const };
					}
				}
				manager.rememberDecision(cacheKey, decision);
				return decision;
			}).pipe(
				Effect.ensuring(
					Effect.sync(() => {
						if (releaseInFinally) {
							releaseResources();
						}
					}),
				),
			);

			const ownerFiber = yield* manager.fork(owner);
			return yield* Fiber.join(ownerFiber).pipe(
				Effect.onInterrupt(() =>
					Effect.sync(() => {
						lease.cancel();
					}),
				),
			);
		}).pipe(
			Effect.catchCause(() =>
				Effect.succeed({
					type: "settlement_failed" as const,
					error: normalizeSessionEventWriterError({
						code: "unknown",
						sessionId: request.sessionId,
						writeId: `rwrite_${stableId("arvw", reviewCacheKey(request))}_failure`,
					}),
				}),
			),
		);
}

function reviewCacheKey(request: RuntimeApprovalReviewRequest): string {
	return stableId(
		"arvwc",
		request.workspaceId,
		request.sessionId,
		request.sessionThreadId,
		request.modelRequestId,
		request.targetModelToolCallId,
		request.targetToolName,
		canonicalJSON(request.actionJson),
		canonicalJSON(request.policyContext),
	);
}

function reviewThreadControl(
	request: RuntimeApprovalReviewRequest,
	reviewerThreadId: string,
): RuntimeThreadAddressState {
	return {
		workspaceId: request.workspaceId,
		sessionId: request.sessionId,
		sessionThreadId: reviewerThreadId,
		bindingId: request.bindingId,
		bindingGeneration: request.bindingGeneration,
		targetPodUid: request.targetPodUid,
	};
}

function approvalReviewInput(
	request: RuntimeApprovalReviewRequest,
	control: RuntimeThreadAddressState,
	inputOrder: number,
	reviewId: string,
	runtimeInputId: string,
	reviewerModel: RuntimeModelRef,
	assets: ApprovalReviewerAssets,
	parentTranscriptFeed: readonly RuntimeContextEntry[],
	parentTranscriptReanchored: boolean,
): Extract<RuntimeAcceptedInputState, { readonly kind: "approval_review" }> {
	return {
		workspaceId: control.workspaceId,
		sessionId: control.sessionId,
		sessionThreadId: control.sessionThreadId,
		bindingId: control.bindingId,
		bindingGeneration: control.bindingGeneration,
		targetPodUid: control.targetPodUid,
		runtimeInputId,
		inputOrder,
		kind: "approval_review",
		reviewId,
		parentThreadId: request.sessionThreadId,
		targetModelToolCallId: request.targetModelToolCallId,
		targetToolName: request.targetToolName,
		promptText: [
			approvalReviewPromptText(
				request,
				reviewerModel,
				assets,
				parentTranscriptFeed,
				parentTranscriptReanchored,
			),
		],
		outputSchemaJson: assets.outputSchemaJson,
		thread: {
			parentThreadId: request.sessionThreadId,
			role: "approval_reviewer",
			visibility: "internal",
			agentType: "approval_reviewer",
			status: "idle",
		},
	};
}

function approvalReviewPromptText(
	request: RuntimeApprovalReviewRequest,
	reviewerModel: RuntimeModelRef,
	assets: ApprovalReviewerAssets,
	parentTranscriptFeed: readonly RuntimeContextEntry[],
	parentTranscriptReanchored: boolean,
): string {
	const text = JSON.stringify(
		{
			output_schema: JSON.parse(assets.outputSchemaJson) as RuntimeJsonValue,
			target_model_tool_call_id: request.targetModelToolCallId,
			target_tool_name: request.targetToolName,
			action_json: request.actionJson,
			parent_transcript_feed_note: parentTranscriptReanchored
				? ParentTranscriptReanchorNote
				: ParentTranscriptDeltaNote,
			parent_transcript_feed: renderTranscriptEvidence(parentTranscriptFeed),
			current_assistant_draft:
				request.currentAssistantDraft.length === 0
					? []
					: [renderContextParts("assistant", request.currentAssistantDraft)],
			sibling_tool_calls: request.siblingToolCalls,
			policy_context: request.policyContext,
		},
		null,
		2,
	);
	return text;
}

/**
 * Loads the packaged reviewer policy and output schema used by Runtime Pod
 * startup and validates that the schema asset is JSON before returning it.
 */
export function loadApprovalReviewerAssets(): ApprovalReviewerAssets {
	const policyPrompt = readFileSync(
		new URL("./assets/approval-reviewer-policy.md", import.meta.url),
		"utf8",
	).trim();
	const outputSchemaJson = readFileSync(
		new URL("./assets/approval-reviewer-output-schema.json", import.meta.url),
		"utf8",
	).trim();
	JSON.parse(outputSchemaJson);
	return { policyPrompt, outputSchemaJson };
}

function renderTranscriptEvidence(
	entries: readonly RuntimeContextEntry[],
): readonly RuntimeJsonValue[] {
	return entries.map((entry) =>
		renderContextParts(
			entry.contextKind === "assistant" ? "assistant" : "user",
			entry.parts,
		),
	);
}

function renderContextParts(
	role: "assistant" | "user",
	parts: readonly RuntimeContextPart[],
): RuntimeJsonValue {
	return {
		role,
		text: parts
			.filter((part) => part.type === "text")
			.map((part) => part.text)
			.join("\n"),
		reasoning: parts
			.filter((part) => part.type === "reasoning")
			.map((part) => part.text)
			.join("\n"),
		tool_calls: parts
			.filter((part) => part.type === "tool_call")
			.map((part) => ({
				tool_call_id: part.modelToolCallId,
				tool_name: part.toolName,
				input: part.canonicalInput,
			})),
	};
}

function parseApprovalDecision(
	entries: readonly RuntimeContextEntry[],
): ParsedApprovalReviewerOutcome {
	const text = latestAssistantText(entries);
	if (text === undefined) {
		return {
			type: "failed",
			failureKind: "runtime_failure",
			message: "approval reviewer returned no decision",
		};
	}
	const parsed = parseJSONDecision(text);
	if (!isRecord(parsed)) {
		return {
			type: "failed",
			failureKind: "parse_failure",
			message: "approval reviewer decision is not JSON",
		};
	}
	if (!hasExactApprovalDecisionFields(parsed)) {
		return {
			type: "failed",
			failureKind: "parse_failure",
			message: "approval reviewer decision fields are invalid",
		};
	}
	const outcome = parsed.outcome;
	if (outcome !== "allow" && outcome !== "deny") {
		return {
			type: "failed",
			failureKind: "parse_failure",
			message: "approval reviewer decision outcome is invalid",
		};
	}
	const riskLevel = parsed.risk_level;
	if (!isApprovalReviewRiskLevel(riskLevel)) {
		return {
			type: "failed",
			failureKind: "parse_failure",
			message: "approval reviewer decision risk_level is invalid",
		};
	}
	const userAuthorization = parsed.user_authorization;
	if (!isApprovalReviewUserAuthorization(userAuthorization)) {
		return {
			type: "failed",
			failureKind: "parse_failure",
			message: "approval reviewer decision user_authorization is invalid",
		};
	}
	const rationale = parsed.rationale;
	if (typeof rationale !== "string") {
		return {
			type: "failed",
			failureKind: "parse_failure",
			message: "approval reviewer decision rationale is invalid",
		};
	}
	return {
		type: "decision",
		riskLevel,
		userAuthorization,
		outcome,
		...(rationale.length > 0 ? { message: rationale } : {}),
	};
}

function hasExactApprovalDecisionFields(
	value: Record<string, unknown>,
): boolean {
	const fields = Object.keys(value).sort();
	return (
		fields.length === 4 &&
		fields.every(
			(field, index) =>
				field ===
				["outcome", "rationale", "risk_level", "user_authorization"][index],
		)
	);
}

function approvalReviewDecisionEvent(
	input: Extract<
		RuntimeAcceptedInputState,
		{ readonly kind: "approval_review" }
	>,
	decision: Extract<ApprovalReviewerOutcome, { readonly type: "decision" }>,
): Extract<SessionEvent, { readonly type: "approval_review.decision" }> {
	return {
		type: "approval_review.decision" as const,
		review_id: input.reviewId,
		parent_thread_id: input.parentThreadId,
		target_model_tool_call_id: input.targetModelToolCallId,
		target_tool_name: input.targetToolName,
		risk_level: decision.riskLevel,
		user_authorization: decision.userAuthorization,
		outcome: decision.outcome,
		rationale: decision.message ?? "",
	};
}

function approvalReviewFailureEvent(
	input: Extract<
		RuntimeAcceptedInputState,
		{ readonly kind: "approval_review" }
	>,
	failureKind: ApprovalReviewFailureKind,
	message: string,
): ApprovalReviewFailureEvent {
	return {
		type: "approval_review.failure" as const,
		review_id: input.reviewId,
		parent_thread_id: input.parentThreadId,
		target_model_tool_call_id: input.targetModelToolCallId,
		target_tool_name: input.targetToolName,
		failure_kind: failureKind,
		message: safeReviewFailureMessage(message),
	};
}

type ReviewerOutcomeSettlement =
	| { readonly type: "acknowledged" }
	| { readonly type: "stale_custody" }
	| {
			readonly type: "rejected" | "unknown";
			readonly error: SessionEventWriterError;
	  };

function commitReviewerOutcomeEffect(
	write: () => Promise<SessionEventWriterAppendResult>,
	sessionId: string,
	writeId: string,
	timeoutMs?: number,
): Effect.Effect<ReviewerOutcomeSettlement> {
	const settlement = Effect.promise(write).pipe(
		Effect.map(
			(result): ReviewerOutcomeSettlement =>
				result.ok
					? result.type === "stale"
						? { type: "stale_custody" }
						: { type: "acknowledged" }
					: {
							type: result.error.retryable ? "unknown" : "rejected",
							error: result.error,
						},
		),
		Effect.catchCause(() =>
			Effect.succeed({
				type: "unknown" as const,
				error: normalizeSessionEventWriterError({
					code: "unknown",
					sessionId,
					writeId,
				}),
			}),
		),
	);
	if (timeoutMs === undefined) {
		return settlement;
	}
	return settlement.pipe(
		Effect.timeoutOption(`${Math.max(0, timeoutMs)} millis`),
		Effect.map(
			(result): ReviewerOutcomeSettlement =>
				result._tag === "Some"
					? result.value
					: {
							type: "unknown",
							error: normalizeSessionEventWriterError({
								code: "timeout",
								sessionId,
								writeId,
							}),
						},
		),
	);
}

function quiesceReviewerRequestEffect(
	host: RuntimeSubAgentRunHost,
	control: RuntimeThreadAddressState,
	token: ReviewerExecutionToken,
): Effect.Effect<boolean> {
	return Effect.promise(async () => {
		const interrupted = await host.interruptReviewerExecution(control, token);
		if (!interrupted.ok || !interrupted.terminal) {
			return false;
		}
		const waited = await host.waitReviewerExecution(control, token, undefined);
		return waited.ok && waited.terminal && !waited.timedOut;
	}).pipe(Effect.catchCause(() => Effect.succeed(false)));
}

function evictReviewerExecutionEffect(
	host: RuntimeSubAgentRunHost,
	control: RuntimeThreadAddressState,
	token: ReviewerExecutionToken,
): Effect.Effect<boolean> {
	return Effect.promise(async () => {
		const evicted = await host.evictReviewerExecution(control, token);
		return evicted.ok && evicted.terminal;
	}).pipe(Effect.catchCause(() => Effect.succeed(false)));
}

function releaseReviewerExecutionEffect(
	host: RuntimeSubAgentRunHost,
	control: RuntimeThreadAddressState,
	token: ReviewerExecutionToken,
): Effect.Effect<boolean> {
	return Effect.promise(async () => {
		const released = await host.releaseReviewerExecution(control, token);
		return released.ok && released.terminal;
	}).pipe(Effect.catchCause(() => Effect.succeed(false)));
}

function closeApprovalReviewerSidecarEffect(
	host: RuntimeSubAgentRunHost,
	threadCreator: RuntimeApprovalReviewerThreadCreator,
	creation: ApprovalReviewerThreadCreation,
	control: RuntimeThreadAddressState,
	token: ReviewerExecutionToken | undefined,
	logger: RuntimePodLogger | undefined,
	request: RuntimeApprovalReviewRequest,
	reviewId: string,
): Effect.Effect<"closed" | "failed" | "stale_custody"> {
	return Effect.gen(function* () {
		const durable = yield* Effect.promise(() =>
			threadCreator.closeApprovalReviewerThread(creation),
		);
		if (!durable.ok) {
			logApprovalReviewLifecycleFailure(
				logger,
				request,
				reviewId,
				"sidecar",
				"durable_close_rejected",
			);
			if (durable.discardHotState === true) {
				return "stale_custody" as const;
			}
			if (token !== undefined) {
				const evicted = yield* evictReviewerExecutionEffect(
					host,
					control,
					token,
				);
				if (!evicted) {
					logApprovalReviewLifecycleFailure(
						logger,
						request,
						reviewId,
						"sidecar",
						"hot_eviction_failed",
					);
				}
			}
			return "failed" as const;
		}
		const hot = yield* Effect.promise(() => host.markThreadClosed(control));
		if (!hot.ok) {
			logApprovalReviewLifecycleFailure(
				logger,
				request,
				reviewId,
				"sidecar",
				"hot_close_failed",
			);
			if (token !== undefined) {
				const evicted = yield* evictReviewerExecutionEffect(
					host,
					control,
					token,
				);
				if (!evicted) {
					logApprovalReviewLifecycleFailure(
						logger,
						request,
						reviewId,
						"sidecar",
						"hot_eviction_failed",
					);
				}
			}
			return "failed" as const;
		}
		return "closed" as const;
	}).pipe(
		Effect.catchCause(() =>
			Effect.gen(function* () {
				logApprovalReviewLifecycleFailure(
					logger,
					request,
					reviewId,
					"sidecar",
					"durable_close_failed",
				);
				if (token !== undefined) {
					const evicted = yield* evictReviewerExecutionEffect(
						host,
						control,
						token,
					);
					if (!evicted) {
						logApprovalReviewLifecycleFailure(
							logger,
							request,
							reviewId,
							"sidecar",
							"hot_eviction_failed",
						);
					}
				}
				return "failed" as const;
			}),
		),
	);
}

function logApprovalReviewFailure(
	logger: RuntimePodLogger | undefined,
	request: RuntimeApprovalReviewRequest,
	reviewId: string,
	failureKind: ApprovalReviewFailureKind,
	message: string,
): void {
	const safeMessage = safeReviewFailureMessage(message);
	try {
		logger?.error({
			event: "approval_review_failure",
			"event.kind": "approval_review.failure",
			operation: "approval_review",
			component: "agent-runtime",
			"request.id": request.modelRequestId,
			"workspace.id": request.workspaceId,
			"session.id": request.sessionId,
			"thread.id": request.sessionThreadId,
			"review.id": reviewId,
			"target.tool_call.id": request.targetModelToolCallId,
			"target.tool.name": request.targetToolName,
			"approval.failure_kind": failureKind,
			message: safeMessage,
			...semanticErrorFields({
				errorClass: "approval_review_failure",
				errorCode: failureKind,
				messageSafe: safeMessage,
			}),
		});
	} catch {
		// Reviewer settlement is authoritative; observability is not part of its custody boundary.
	}
}

function logApprovalReviewSettlementFailure(
	logger: RuntimePodLogger | undefined,
	request: RuntimeApprovalReviewRequest,
	reviewId: string,
	settlement: Exclude<
		ReviewerOutcomeSettlement,
		{ readonly type: "acknowledged" | "stale_custody" }
	>,
): void {
	try {
		logger?.error({
			event: "approval_review_settlement_failure",
			"event.kind": "approval_review.settlement_failure",
			operation: "approval_review_settlement",
			component: "agent-runtime",
			"request.id": request.modelRequestId,
			"workspace.id": request.workspaceId,
			"session.id": request.sessionId,
			"thread.id": request.sessionThreadId,
			"review.id": reviewId,
			"target.tool_call.id": request.targetModelToolCallId,
			"target.tool.name": request.targetToolName,
			"settlement.outcome": settlement.type,
			...semanticErrorFields({
				errorClass: "durable_settlement_failure",
				errorCode: settlement.error.code,
				messageSafe: "approval reviewer outcome was not durably acknowledged",
			}),
		});
	} catch {
		// Outcome settlement and parent closeout cannot depend on telemetry.
	}
}

function logApprovalReviewLifecycleFailure(
	logger: RuntimePodLogger | undefined,
	request: RuntimeApprovalReviewRequest,
	reviewId: string,
	reviewerKind: "trunk" | "sidecar",
	reason:
		| "quiescence_unconfirmed"
		| "durable_close_rejected"
		| "durable_close_failed"
		| "hot_close_failed"
		| "hot_eviction_failed"
		| "hot_release_failed",
): void {
	try {
		logger?.error({
			event: "approval_review_lifecycle_failure",
			"event.kind": "approval_review.lifecycle_failure",
			operation: "approval_review_lifecycle",
			component: "agent-runtime",
			"request.id": request.modelRequestId,
			"workspace.id": request.workspaceId,
			"session.id": request.sessionId,
			"thread.id": request.sessionThreadId,
			"review.id": reviewId,
			"reviewer.kind": reviewerKind,
			"lifecycle.reason": reason,
			...semanticErrorFields({
				errorClass: "reviewer_lifecycle_failure",
				errorCode: reason,
				messageSafe:
					"approval reviewer lifecycle did not reach its expected durable boundary",
			}),
		});
	} catch {
		// Reviewer lifecycle ownership cannot depend on telemetry.
	}
}

function safeReviewFailureMessage(message: string): string {
	const normalized = message.replace(/\s+/g, " ").trim();
	return normalized.length <= FailureMessageMaxLength
		? normalized
		: `${normalized.slice(0, FailureMessageMaxLength - 3)}...`;
}

function latestAssistantText(
	entries: readonly RuntimeContextEntry[],
): string | undefined {
	const entry = entries.at(-1);
	if (entry?.contextKind !== "assistant") {
		return undefined;
	}
	const text = entry.parts
		.filter((part) => part.type === "text")
		.map((part) => part.text)
		.join("\n")
		.trim();
	return text.length > 0 ? text : undefined;
}

function parseJSONDecision(text: string): unknown {
	try {
		return JSON.parse(text.trim());
	} catch {
		return undefined;
	}
}

function isApprovalReviewRiskLevel(
	value: unknown,
): value is ApprovalReviewRiskLevel {
	return (
		value === "low" ||
		value === "medium" ||
		value === "high" ||
		value === "critical"
	);
}

function isApprovalReviewUserAuthorization(
	value: unknown,
): value is ApprovalReviewUserAuthorization {
	return (
		value === "unknown" ||
		value === "low" ||
		value === "medium" ||
		value === "high"
	);
}

function canonicalJSON(value: RuntimeJsonValue): string {
	return JSON.stringify(sortJSON(value));
}

function sortJSON(value: RuntimeJsonValue): RuntimeJsonValue {
	if (Array.isArray(value)) {
		return value.map(sortJSON);
	}
	if (!isRecord(value)) {
		return value;
	}
	return Object.fromEntries(
		Object.keys(value)
			.sort()
			.map((key) => [key, sortJSON(value[key] as RuntimeJsonValue)]),
	);
}

function stableId(prefix: string, ...parts: readonly string[]): string {
	const hash = createHash("sha256");
	for (const part of parts) {
		hash.update(part);
		hash.update("\0");
	}
	return `${prefix}_${hash.digest("hex").slice(0, 24)}`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}
