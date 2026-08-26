/*
 * This module is the sole transition and dispatch owner for one loaded Thread.
 * SessionManager calls Service.run from the thread's single run-slot owner Fiber;
 * committed facts are applied here and any returned dispatch is captured on the
 * current stack before external work begins. Ordinary input may update ThreadState
 * and signal the run slot, but cannot replace an in-flight dispatch.
 *
 * The loop guards these invariants:
 * - Newly accepted input enters hot context only after its durable write acknowledges it;
 *   pending-tool state is synchronously rehydrated from the already loaded durable view.
 * - Running status and model-request-start acknowledgment precede provider work,
 *   and every started request receives a terminal request-end closeout before the
 *   loop continues.
 * - Each provider-request scope owns its provider stream and ToolFibers. A tool route
 *   starts only after public tool-use acknowledgment, tool settlement updates hot
 *   context only after acknowledgment, and the next provider request waits for
 *   stream termination, request-end acknowledgment, and ToolFiber settlement.
 * - External approvals leave durable pending rows and hot ToolJobs as the wait
 *   owners; reply handling starts new ToolFibers instead of resuming exited ones.
 * - Shutdown, user interrupt, and cooperative cancellation fence writes, stop new
 *   tools, cancel and bounded-join active work, and terminalize committed public
 *   tool uses where this process owns closeout.
 * - FinishIdle acknowledgment gates a locally completed or requires-action return,
 *   while failure results tell SessionManager when to release hot state.
 *
 * Runtime Pod composition supplies context loading, persistence, event writing,
 * provider streaming, Tool routing, review, policy, metrics, clocks, IDs and
 * binding refresh. This module does not own run-slot coalescing, Bridge storage,
 * Gateway transport or the provider-visible message list in ContextManager.
 */

import type { ProviderContextEntry as GatewayProviderContextEntry } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { ProviderRequestKind } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import {
	Cause,
	Context,
	Deferred,
	Effect,
	Exit,
	Fiber,
	Layer,
	Option,
	Scope,
	Semaphore,
	Stream,
} from "effect";
import type {
	AcceptedInputCommitResult,
	ContextLoader as ContextLoaderInterface,
} from "../context/context-loader.js";
import * as ContextLoader from "../context/context-loader.js";
import type { ProviderError } from "../contracts/provider.js";
import type {
	PendingInputResult,
	RuntimeAssistantContextAppend,
	RuntimeContextEntry,
	RuntimeContextPart,
	RuntimeDeclarationOperationControls,
	RuntimeDependencies,
	RuntimeFailure,
	RuntimeFinishReason,
	RuntimeInternalToolRepairCommit,
	RuntimeInternalToolRepairCommitResult,
	RuntimeInternalToolRepairStore,
	RuntimeInternalToolRepairStoreError,
	RuntimeInterruptToolResult,
	RuntimeJsonValue,
	RuntimeOpenRequestDraft,
	RuntimeProviderAttachment,
	RuntimeRequestErrorKind,
	RuntimeToolSettlementDeclaration,
	SessionEvent,
	SessionEventEnvelope,
	SessionEventWriter,
	SessionEventWriterAppendEvent,
	SessionEventWriterAppendResult,
	SessionEventWriterError,
	SessionEventWriterFinishIdleEnvelope,
	SessionEventWriterRequestEndEnvelope,
	SessionEventWriterRequestEndResult,
} from "../contracts/runtime.js";
import {
	ContextLoaderErrorSchema,
	isRuntimeTerminationFailure,
	normalizeContextLoaderError,
	normalizeRuntimeFailure,
	normalizeSessionEventWriterError,
	ownRuntimeDeclarationRawOperations,
	RuntimeJsonValueSchema,
	RuntimeToolSettlementDeclarationSchema,
	SessionEventWriterRetryPolicy,
} from "../contracts/runtime.js";
import type {
	LLMEvent,
	RuntimeFailure as LLMRuntimeFailure,
	RuntimeAttachmentRejection,
	RuntimeModelLimits,
	RuntimeUsage,
} from "../llm/llm-event.js";
import { RuntimeFailureSchema as LLMRuntimeFailureSchema } from "../llm/llm-event.js";
import type {
	LLMRequest,
	LLMServiceError,
	Interface as LLMServiceInterface,
} from "../llm/llm-service.js";
import type {
	ProviderStreamAccumulatorOptions,
	ProviderStreamAccumulatorResult,
	PublicToolEvent,
	RuntimeProcessorSource,
	ToolSettlementApplicationResult,
} from "../runtime/accumulator.js";
import {
	internalToolRepairKey,
	ProviderStreamAccumulator,
} from "../runtime/accumulator.js";
import {
	toGatewayProviderContext,
	toGatewayProviderContextSegments,
} from "../runtime/context-projection.js";
import type {
	RuntimeContextLoadOperation,
	RuntimeEventWriteOperation,
	RuntimeMetricOutcome,
	RuntimeMetricsSink,
	RuntimeProviderStreamKind,
} from "../runtime/metrics.js";
import { NoopRuntimeMetricsSink } from "../runtime/metrics.js";
import {
	acceptedInputContextDrafts,
	applyAcceptedInputResult,
	applyInterruptToolResults,
	compactionContext,
	contextToolResultFromSettlement,
	runtimeTurnOpenWriteId,
	toolConfirmationContext,
} from "../runtime/runtime-declaration.js";
import type {
	RuntimeTurnRetryCounters,
	RuntimeTurnRetryKind,
} from "../runtime/turn-retry-budget.js";
import { evaluateTurnRetryBudget } from "../runtime/turn-retry-budget.js";
import type { ThreadContextPrefix } from "../session/context-manager.js";
import type { RequestExecutionSnapshot } from "../session/session-configuration.js";
import type { ToolCatalog, ToolEntry } from "../tools/tool-catalog.js";
import {
	effectivePermissionPolicy,
	lookupToolEntry,
} from "../tools/tool-catalog.js";
import type { ToolApprovalMode } from "../tools/tool-gate.js";
import { evaluateToolGate } from "../tools/tool-gate.js";
import type { ToolJob } from "../tools/tool-scheduler.js";
import { inferToolRunPolicy, ToolScheduler } from "../tools/tool-scheduler.js";
import type { FailedRunCloseoutResult } from "./closeout.js";
import {
	appendIdleEvent,
	closeFailedRunDurably,
	closeFailedThreadRun,
	createFailedRunCloseoutMemo,
	runtimeFailureFromEventWriter,
} from "./closeout.js";
import type {
	CompactionStreamState,
	ThreadLoopCompactionOptions,
} from "./compaction.js";
import {
	assembleCompactionLLMRequest,
	buildCompactionPrompt,
	CompactionContextLimitMessage,
	CompactionSummaryOutputTokens,
	compactionBoundaryMessageSequence,
	compactionContextLimitFailure,
	compactionFailure,
	compactionPromptMessage,
	consumeCompactionStreamEvent,
	estimatedRuntimeContextTokens,
	highestMessageSequence,
	isContextOverflowFailure,
	mintCompactionCheckpoint,
	runCompactionStreamLifecycle,
	runtimeUsageTokenTotal,
	selectCompactionContext,
	usableModelInputTokens,
} from "./compaction.js";
import type {
	MemoryStorePromptEntry,
	ProviderCallAssembler,
	ProviderCallRuntimeConfig,
	RejectedProviderAttachment,
	SkillGuidanceIndexEntry,
} from "./provider-request.js";
import {
	applyRequestEndSeal,
	assembleLLMRequest,
	assembleProviderCallRequest,
	attachmentConsumptionUnion,
	DefaultProviderCallRuntimeConfig,
	providerRequestAttachmentIdentity,
	providerRequestWithoutRejectedAttachments,
	providerStreamExhaustedFailure,
	providerStreamMetricOutcome,
	recordAttachmentRejections,
	recordProviderStreamDuration,
	requestEndKindForSession,
	requestEndKindFromRequest,
	requestErrorKindFromFailure,
	runProviderStreamLifecycle,
	runtimeAttachmentsWithoutRejectedAttachments,
	runtimeProviderStreamKindFromRequest,
} from "./provider-request.js";
import type { ThreadRuntime } from "./thread-runtime.js";
import type {
	RuntimePendingApprovalToolJobState,
	RuntimePendingSandboxExecutionJobState,
	SessionCurrentModel,
} from "./thread-state.js";
import type {
	RuntimeAcceptedInputState,
} from "./input/accepted-input.js";
import type {
	RuntimeControlInputCommit,
	RuntimeInterruptCommandState,
	RuntimeToolConfirmationState,
} from "./input/control-input.js";
import type {
	RuntimePreloadedPendingToolUseState,
	RuntimePreloadedSandboxExecutionState,
} from "./input/preload.js";
import type {
	ThreadTurnNextStep,
	ThreadTurnTransition,
} from "./turn/types.js";
import { projectFailedRequestProviderContext } from "./turn/provider-context.js";
import type {
	RuntimeApprovalReviewer,
	RuntimeRecoveredToolJobState,
	RuntimeSandboxExecutionAcceptanceResult,
	RuntimeSandboxExecutionAccepter,
	RuntimeSandboxExecutionRequest,
	RuntimeToolExecutionRequest,
	RuntimeToolExecutionResult,
	RuntimeToolRunner,
} from "./tool-execution.js";
import {
	acceptRuntimeSandboxExecution,
	commitRuntimeToolSettlement,
	defaultRuntimeSandboxExecutionAccepter,
	defaultRuntimeSandboxExecutionWaiter,
	defaultRuntimeToolRunner,
	deniedToolCallFailure,
	distinctProviderInputForEntry,
	effectiveToolPermissionPolicy,
	installLoadedPendingToolUses,
	installLoadedSandboxExecutions,
	invalidToolCallFailure,
	isPendingSandboxExecution,
	isResolvedToolRoute,
	pendingApprovalResumeFailure,
	pendingSandboxExecutionToolUseEventIds,
	publicInputForRegisteredTool,
	publicToolEventForEntry,
	registerRuntimeToolCall,
	routeCapabilityForEntry,
	runRuntimeToolEffect,
	runtimeToolSettlement,
} from "./tool-execution.js";

export {
	applyAcceptedInputResult,
	applyInterruptToolResults,
	toolConfirmationContext,
};

export interface InterpretedThreadTurnNextStep {
	readonly nextStep: ThreadTurnNextStep;
	readonly runDisposition: "passive" | "active";
}

/** Exhaustively interprets stable next steps before ThreadLoop or SessionManager acts on them. */
export function interpretThreadTurnNextStep(
	nextStep: ThreadTurnNextStep,
): InterpretedThreadTurnNextStep {
	switch (nextStep.action) {
		case "await_input":
		case "await_request_end":
		case "await_tool_results":
			return { nextStep, runDisposition: "passive" };
		case "prepare_next_request":
		case "resume_tool_routes":
		case "finish_idle":
		case "continue_after_compaction":
		case "complete_reviewer":
		case "apply_request_retry_or_reschedule":
		case "commit_accepted_input":
		case "close_interrupted":
		case "close_failed":
			return { nextStep, runDisposition: "active" };
	}
}

/** Returns whether a reconstructed stable next step needs an owned ThreadRun. */
function threadTurnNextStepNeedsRun(
	nextStep: ThreadTurnNextStep,
): boolean {
	return interpretThreadTurnNextStep(nextStep).runDisposition === "active";
}

function threadNeedsRun(session: ThreadRuntime): boolean {
	return (
		session.state.peekAcceptedInput() !== undefined ||
		threadTurnNextStepNeedsRun(session.state.threadTurnTransition().nextStep)
	);
}

interface ThreadRunOpeningSource {
	readonly kind:
		| "committed_input"
		| "prepare_next_request"
		| "continue_after_compaction";
	readonly id: string;
}

function recoveredThreadRunOpeningSource(
	transition: ThreadTurnTransition,
): ThreadRunOpeningSource | undefined {
	if (
		transition.nextStep.action !== "prepare_next_request" &&
		transition.nextStep.action !== "continue_after_compaction"
	) {
		return undefined;
	}
	const pendingInputContextSequence =
		transition.checkpoint.pendingInputContextSequences[0];
	if (pendingInputContextSequence !== undefined) {
		return { kind: "committed_input", id: String(pendingInputContextSequence) };
	}
	const requestEndEventId = transition.checkpoint.request?.requestEnd?.eventId;
	return requestEndEventId === undefined
		? undefined
		: { kind: transition.nextStep.action, id: requestEndEventId };
}

/** Tells SessionManager whether one run retains, discards, or releases the thread's hot state. */
export type ThreadLoopRunResult =
	| {
			readonly type: "completed";
			readonly modelMessageCount: number;
			readonly currentModel?: {
				readonly providerId: string;
				readonly modelId: string;
			};
	  }
	| {
			readonly type: "interrupted";
			readonly discardHotState?: true;
	  }
	| {
			readonly type: "failed";
			readonly error: ProviderError | RuntimeFailure;
			readonly failureEventId?: string;
			readonly closeoutDisposition?: "continuation" | "terminal";
			readonly releaseSession?: {
				readonly reason: ThreadLoopSessionReleaseReason;
			};
	  };

/** Classifies terminal outcomes that require resident Thread state to be released. */
export type ThreadLoopSessionReleaseReason =
	| "terminated"
	| "crashed"
	| "persistence_failed"
	| "event_write_failed";

/** Result of applying one committed idle-interrupt receipt inside the ThreadLoop owner. */
export type IdleInterruptSettlementResult =
	| { readonly type: "applied"; readonly wakeThread: boolean }
	| { readonly type: "duplicate" }
	| { readonly type: "stale" }
	| { readonly type: "conflict" }
	| {
			readonly type: "commit_failed";
			readonly retryable: boolean;
			readonly errorCode: string | number;
	  }
	| { readonly type: "projection_failed" };

/** Result of applying one Tool-confirmation receipt before SessionManager consumes its wake intent. */
export type ToolConfirmationSettlementResult =
	| {
			readonly type: "applied";
			readonly applied: boolean;
			readonly wakeThread: boolean;
	  }
	| { readonly type: "duplicate" }
	| { readonly type: "stale" }
	| {
			readonly type: "failed";
			readonly reason: "context_load_failed" | "control_conflict";
	  };

/** Identifies the provider and model selected from immutable Runtime configuration. */
export interface RuntimeModelRef {
	readonly providerId: string;
	readonly modelId: string;
}

/** Exposes the per-thread run effect and cold pending-tool restoration used by SessionManager. */
export interface Interface {
	readonly run: (
		session: ThreadRuntime,
		custody: ThreadLoopRunCustody,
	) => Effect.Effect<ThreadLoopRunResult, unknown>;
	readonly closeFailedRun: (
		session: ThreadRuntime,
		defect: unknown,
		custody: ThreadLoopRunCustody,
	) => Effect.Effect<FailedRunCloseoutResult>;
	/** Re-enters target interrupt closeout for an open Request restored before a run Fiber exists. */
	readonly closeRecoveredOpenRequestForInterrupt: (
		session: ThreadRuntime,
		custody: ThreadLoopRunCustody,
	) => Effect.Effect<ThreadLoopRunResult>;
	/** Applies an idle interrupt's complete durable receipt before releasing its local fence. */
	readonly settleIdleInterrupt: (
		session: ThreadRuntime,
		command: RuntimeInterruptCommandState,
		commitInput: RuntimeControlInputCommit,
	) => Effect.Effect<IdleInterruptSettlementResult>;
	/** Applies a committed Tool confirmation and returns one run wake intent. */
	readonly settleToolConfirmation: (
		session: ThreadRuntime,
		command: RuntimeToolConfirmationState,
		commitInput: RuntimeControlInputCommit,
	) => Effect.Effect<ToolConfirmationSettlementResult>;
	/** Keeps Reducer interpretation inside ThreadLoop while SessionManager owns the run slot. */
	readonly threadNeedsRun: (session: ThreadRuntime) => boolean;
	/**
	 * Seeds config-sourced model state synchronously. Both preload and accepted-input config
	 * application call this before pending-tool restoration; an unresolved model stays undefined
	 * for the run gate to settle.
	 */
	readonly seedRuntimeModel: (session: ThreadRuntime) => void;
	readonly installLoadedPendingToolUses: (
		session: ThreadRuntime,
		pendingToolUses: readonly RuntimePreloadedPendingToolUseState[] | undefined,
		entries: readonly RuntimeContextEntry[],
		openRequestDraft: RuntimeOpenRequestDraft | undefined,
	) => Effect.Effect<PendingToolUseInstallResult>;
	readonly installLoadedSandboxExecutions: (
		session: ThreadRuntime,
		executions: readonly RuntimePreloadedSandboxExecutionState[] | undefined,
		entries: readonly RuntimeContextEntry[],
		openRequestDraft: RuntimeOpenRequestDraft | undefined,
	) => Effect.Effect<PendingToolUseInstallResult>;
}

/** Read access to the reducer-owned active durable turn identity. */
export interface ThreadLoopRunCustody {
	readonly activeTurnId: (session: ThreadRuntime) => string | undefined;
	readonly interruptLeaseRef: (
		runtimeInputId: string,
	) =>
		| {
				readonly jobId: string;
				readonly leaseToken: string;
				readonly partitionKey: string;
				readonly dedupeKey: string;
		  }
		| undefined;
}

/** Provides the thread-loop Effect service consumed by SessionManager. */
export class Service extends Context.Service<Service, Interface>()(
	"tetral-agent/ThreadLoop",
) {}

/** Reports whether cold-loaded pending tool uses enter hot state successfully. */
export type PendingToolUseInstallResult =
	| { readonly ok: true }
	| { readonly ok: false; readonly error: unknown };

/** Adapts a ContextLoader implementation into the Effect layer required by ThreadLoop. */
export const contextLoaderLayer = ContextLoader.layer;

const ToolRouteCancelJoinTimeoutMs = 250;
const ProviderRequestScopeCloseTimeoutMs = ToolRouteCancelJoinTimeoutMs + 50;
const RecoveredSandboxBindingRefreshBackoffMs = 100;

class HotDurableOperationFenced extends Error {}

class HotDurableOperationOwner {
	#fenced = false;
	#active = 0;
	#waiters: Array<() => void> = [];

	fence(): void {
		this.#fenced = true;
	}

	begin(allowAfterFence = false): () => void {
		if (this.#fenced && !allowAfterFence) {
			throw new HotDurableOperationFenced();
		}
		this.#active += 1;
		let finished = false;
		return () => {
			if (finished) {
				return;
			}
			finished = true;
			this.#active = Math.max(0, this.#active - 1);
			if (this.#active !== 0) {
				return;
			}
			const waiters = this.#waiters;
			this.#waiters = [];
			for (const resolve of waiters) {
				resolve();
			}
		};
	}

	async run<T>(
		operation: () => Promise<T>,
		allowAfterFence = false,
	): Promise<T> {
		const finish = this.begin(allowAfterFence);
		try {
			return await operation();
		} finally {
			finish();
		}
	}

	active(): boolean {
		return this.#active > 0;
	}

	async awaitIdle(): Promise<void> {
		if (this.#active === 0) {
			return;
		}
		await new Promise<void>((resolve) => this.#waiters.push(resolve));
	}
}

function ownHotDurableEffect<A, E, R>(
	owner: HotDurableOperationOwner,
	effect: Effect.Effect<A, E, R>,
	allowAfterFence = false,
): Effect.Effect<A, E, R> {
	return Effect.acquireUseRelease(
		Effect.sync(() => owner.begin(allowAfterFence)),
		() => effect,
		(finish) => Effect.sync(finish),
	);
}

/** Supplies the persistence, provider, policy, tool, reviewer, and runtime adapters for the loop. */
export interface ThreadLoopRuntimeOptions {
	readonly internalToolRepairStore: RuntimeInternalToolRepairStore;
	readonly sessionEventWriter: SessionEventWriter;
	readonly runtime: RuntimeDependencies;
	readonly llmService: LLMServiceInterface;
	readonly storeOperationTimeoutMs: number;
	readonly maxNormalizedTextPreviewBytes?: number;
	readonly createProcessor?: (
		options: ProviderStreamAccumulatorOptions,
	) => ProviderStreamAccumulator;
	readonly providerCallRuntime?: ProviderCallRuntimeConfig;
	readonly providerCallAssembler?: ProviderCallAssembler;
	readonly compaction?: ThreadLoopCompactionOptions;
	readonly approvalMode?: ToolApprovalMode;
	/** Totally resolves the immutable config-selected model; the run gate settles undefined. */
	readonly runtimeModel?: (
		session: ThreadRuntime,
	) => SessionCurrentModel | undefined;
	readonly runtimePolicy?: (session: ThreadRuntime) => ThreadLoopRuntimePolicy;
	readonly runTool?: RuntimeToolRunner;
	readonly acceptSandboxExecution?: RuntimeSandboxExecutionAccepter;
	readonly awaitSandboxExecution?: RuntimeToolRunner;
	readonly reviewApproval?: RuntimeApprovalReviewer;
	readonly metrics?: RuntimeMetricsSink | undefined;
	/** Side-channel observation emitted only after Bridge accepts a provider reschedule. */
	readonly recordProviderReschedule?:
		| ((event: RuntimeProviderRescheduleObservation) => void)
		| undefined;
	/** Side-channel observation for a deterministic tool declaration rejection before request open. */
	readonly recordProviderToolDeclarationRejection?:
		| ((event: RuntimeProviderToolDeclarationRejectionObservation) => void)
		| undefined;
	/** Payload-free observation of the reducer-owned accepted-input commit boundary. */
	readonly recordAcceptedInputCommit?:
		| ((event: RuntimeAcceptedInputCommitObservation) => void)
		| undefined;
	/** Bounded observation emitted only after a terminal Runtime settlement lands durably. */
	readonly recordRuntimeTerminalSettlement?:
		| ((event: RuntimeTerminalSettlementObservation) => void)
		| undefined;
	readonly refreshRuntimeBindingToken?: (
		identity: ThreadRuntime["identity"],
		options?: { readonly force?: boolean | undefined },
	) => Promise<string>;
}

/** Bounded facts selected by the Runtime-owned provider retry policy. */
export interface RuntimeProviderRescheduleObservation {
	readonly workspaceId: string;
	readonly sessionId: string;
	readonly sessionThreadId: string;
	readonly requestId: string;
	readonly modelRequestId: string;
	readonly attempt: number;
	readonly delayMs: number;
	readonly delaySource: "provider" | "runtime_fallback";
	readonly failureCode: string;
}

/** Bounded request and declaration facts for a pre-provider structural rejection. */
export interface RuntimeProviderToolDeclarationRejectionObservation {
	readonly workspaceId: string;
	readonly sessionId: string;
	readonly sessionThreadId: string;
	readonly requestId: string;
	readonly modelRequestId: string;
	readonly declarationKind: "function" | "freeform" | "unknown";
	readonly family: "claude" | "gpt" | "unspecified";
	readonly validationMember:
		| "declaration_kind"
		| "tool_family"
		| "function_schema"
		| "freeform_grammar";
}

/** Stable identities and bounded outcome for one accepted-input commit attempt. */
export interface RuntimeAcceptedInputCommitObservation {
	readonly workspaceId: string;
	readonly sessionId: string;
	readonly sessionThreadId: string;
	readonly runtimeInputId: string;
	readonly inputKind: RuntimeAcceptedInputState["kind"];
	readonly attempt: number;
	readonly durationMs: number;
	readonly outcome:
		| "started"
		| "committed"
		| "deferred"
		| "rejected"
		| "retry"
		| "exhausted";
	readonly failureClass?:
		| "task_notification_result_invalid"
		| "task_notification_message_invalid"
		| "task_notification_payload_mismatch"
		| undefined;
}

/** Stable identity and durable outcome for one terminal Runtime settlement. */
export interface RuntimeTerminalSettlementObservation {
	readonly workspaceId: string;
	readonly sessionId: string;
	readonly sessionThreadId: string;
	readonly modelRequestId?: string | undefined;
	readonly settlementOutcome: "committed" | "duplicate";
}

/** Provides request-time policy and provider context for one resident thread. */
export interface ThreadLoopRuntimePolicy {
	readonly approvalMode?: ToolApprovalMode;
	readonly system?: string;
	readonly toolCatalog?: ToolCatalog;
	readonly skillsIndex?: readonly SkillGuidanceIndexEntry[];
	readonly memoryStores?: readonly MemoryStorePromptEntry[];
	readonly providerRescheduleBudget?: number;
	readonly compactionRescheduleBudget?: number;
}

type ProviderTurnResult = (
	| { readonly type: "completed" }
	| {
			readonly type: "waiting_external";
			readonly blockingEventIds: readonly string[];
	  }
	| {
			readonly type: "rescheduled";
			readonly failure: RuntimeFailure;
			readonly effectiveDeadline: string;
	  }
	| { readonly type: "context_overflow"; readonly failure: RuntimeFailure }
	| { readonly type: "interrupted" }
	| {
			readonly type: "failed";
			readonly error: RuntimeFailure;
			readonly failureEventId?: string;
			readonly releaseSession?: {
				readonly reason: ThreadLoopSessionReleaseReason;
			};
	  }
) & {
	readonly requestEndCommitted?: true;
	readonly attachmentRideDisposition?:
		| "settled"
		| "retained"
		| "discard_hot_state";
};

interface ProviderTurnStreamState {
	executionSnapshot: RequestExecutionSnapshot;
	executionPolicy: Readonly<ThreadLoopRuntimePolicy>;
	durableOperations: HotDurableOperationOwner;
	modelUsage: RuntimeUsage | undefined;
	modelLimits: RuntimeModelLimits | undefined;
	modelFinishReason: RuntimeFinishReason | undefined;
	terminalProviderEventSeen: boolean;
	waitingToolUseEventIds: string[];
	toolFibers: Fiber.Fiber<ProviderTurnResult, unknown>[];
	// One assistant projection is mutated through its durable ACKs in order;
	// tool route execution remains independently concurrent outside this gate.
	assistantProjectionGate: Semaphore.Semaphore;
	providerRequestScope: Scope.Scope;
	toolScheduler: ToolScheduler;
	toolEntries: Record<string, ToolEntry | undefined>;
	toolProviderInputs: Record<string, RuntimeJsonValue | undefined>;
	toolDeclarationBarriers: Record<
		string,
		Deferred.Deferred<boolean> | undefined
	>;
	allToolDeclarationBarriers: Deferred.Deferred<boolean>[];
	nextToolModelOrder: number;
	rejectedAttachments: RejectedProviderAttachment[];
	carriedAttachments: readonly RuntimeProviderAttachment[];
}

class ProviderTurnShortCircuit {
	constructor(readonly result: ProviderTurnResult) {}
}

function isProviderTurnShortCircuit(
	error: unknown,
): error is ProviderTurnShortCircuit {
	return error instanceof ProviderTurnShortCircuit;
}

function providerTurnCompleted(): ProviderTurnResult {
	return { type: "completed" };
}

function providerTurnWaitingExternal(
	blockingEventIds: readonly string[],
): ProviderTurnResult {
	return { type: "waiting_external", blockingEventIds };
}

function providerTurnRescheduled(
	failure: RuntimeFailure,
	effectiveDeadline: string,
): ProviderTurnResult {
	return { type: "rescheduled", failure, effectiveDeadline };
}

function providerTurnContextOverflow(
	failure: RuntimeFailure,
): ProviderTurnResult {
	return { type: "context_overflow", failure };
}

function providerTurnInterrupted(): ProviderTurnResult {
	return { type: "interrupted" };
}

function providerTurnInterruptedWithDiscard(): Extract<
	ProviderTurnResult,
	{ readonly type: "interrupted" }
> {
	return {
		type: "interrupted",
		attachmentRideDisposition: "discard_hot_state",
	};
}

function providerTurnFailed(
	error: RuntimeFailure,
	releaseReason?: ThreadLoopSessionReleaseReason,
	failureEventId?: string,
): ProviderTurnResult {
	const durableFailure = failureEventId === undefined ? {} : { failureEventId };
	if (releaseReason === undefined) {
		return { type: "failed", error, ...durableFailure };
	}
	return {
		type: "failed",
		error,
		...durableFailure,
		releaseSession: { reason: releaseReason },
	};
}

function failRequestCloseout(
	error: RuntimeFailure,
): Effect.Effect<never, never> {
	return Effect.die(error);
}

function nonAbandonablePromise<A>(
	operation: () => Promise<A>,
): Effect.Effect<A, never> {
	return Effect.promise(operation).pipe(Effect.uninterruptible);
}

function requestEndCommitted(
	result: ProviderTurnResult,
	attachmentRideDisposition: NonNullable<
		ProviderTurnResult["attachmentRideDisposition"]
	> = "retained",
): ProviderTurnResult {
	return { ...result, requestEndCommitted: true, attachmentRideDisposition };
}

/** Builds the thread-loop service from host adapters and a provided ContextLoader service. */
export function layer(
	options: ThreadLoopRuntimeOptions,
): Layer.Layer<Service, never, ContextLoader.ContextLoaderService> {
	return threadLoopLayer(options);
}

export const runtimeLayer = layer;

function threadLoopLayer(
	options: ThreadLoopRuntimeOptions,
): Layer.Layer<Service, never, ContextLoader.ContextLoaderService> {
	return Layer.effect(
		Service,
		Effect.gen(function* () {
			const contextLoader = yield* ContextLoader.ContextLoaderService;

			return Service.of({
				run: (session, custody) =>
					runThreadLoopEffect(contextLoader, session, options, custody).pipe(
						Effect.ensuring(
							Effect.sync(() => session.state.finishThreadRunProjection()),
						),
					),
				closeFailedRun: (session, defect, custody) => {
					const request =
						session.state.threadTurnTransition().checkpoint.request;
					const nextStep: Extract<
						ThreadTurnNextStep,
						{ readonly action: "close_failed" }
					> =
						request === undefined
							? { action: "close_failed" }
							: {
									action: "close_failed",
									modelRequestId: request.modelRequestId,
								};
					const closeout = createFailedRunCloseoutMemo(
						options.runtime.createId("event_write"),
						custody.activeTurnId(session),
					);
					return executeCloseFailedThreadTurnNextStep(nextStep, () =>
						closeFailedRunDurably(
							options,
							session,
							closeout,
							custody,
							(writeId, failure) =>
								appendEventWithRetry(options, session, writeId, {
									type: "session.error",
									error: failure,
								}),
						),
					);
				},
				closeRecoveredOpenRequestForInterrupt: (session, custody) =>
					nonAbandonablePromise(() =>
						closeRecoveredOpenRequestForUserInterrupt(session, options, custody),
					),
				settleIdleInterrupt: (session, command, commitInput) =>
					settleIdleInterrupt(
						session,
						command,
						commitInput,
						runtimeMetrics(options),
					),
				settleToolConfirmation: (session, command, commitInput) =>
					settleToolConfirmation(session, command, commitInput),
				threadNeedsRun,
				seedRuntimeModel: (session) => seedRuntimeModel(session, options),
				installLoadedPendingToolUses: (
					session,
					pendingToolUses,
					entries,
					openRequestDraft,
				) =>
					Effect.sync(() =>
						installLoadedPendingToolUses(
							session,
							() => toolCatalogForSession(session, options),
							pendingToolUses,
							entries,
							openRequestDraft,
						),
					),
				installLoadedSandboxExecutions: (
					session,
					executions,
					entries,
					openRequestDraft,
				) =>
					Effect.sync(() =>
						installLoadedSandboxExecutions(
							session,
							() => toolCatalogForSession(session, options),
							executions,
							entries,
							openRequestDraft,
						),
					),
			});
		}),
	);
}

function executeCloseFailedThreadTurnNextStep(
	nextStep: Extract<ThreadTurnNextStep, { readonly action: "close_failed" }>,
	close: () => Promise<FailedRunCloseoutResult>,
): Effect.Effect<FailedRunCloseoutResult, never> {
	return interpretThreadTurnNextStep(nextStep).runDisposition === "active"
		? Effect.promise(close)
		: Effect.die(new Error("close_failed next step must remain active"));
}

async function executeRecoveredCloseInterruptedNextStep(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	custody: ThreadLoopRunCustody,
): Promise<ThreadLoopRunResult> {
	const idle = await appendIdleEvent(
		options,
		session,
		custody,
		{ type: "end_turn" },
		undefined,
		true,
	);
	if (!idle.ok) {
		return {
			type: "failed",
			error: idle.error,
			releaseSession: { reason: "event_write_failed" },
		};
	}
	return { type: "interrupted" };
}

function failRecoveredOpenRequest(session: ThreadRuntime): ThreadLoopRunResult {
	return {
		type: "failed",
		error: normalizeRuntimeFailure({
			type: "runtime",
			code: "runtime_invalid_sequence",
			retryable: false,
			fatal: true,
			reason: "runtime_contract_validation",
			retryStatus: { type: "terminal" },
			sessionId: session.sessionId,
		}),
	};
}

async function executeRecoveredRequestRetryOrRescheduleNextStep(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
): Promise<ThreadLoopRunResult> {
	const failure = normalizeRuntimeFailure({
		type: "runtime",
		code: "runtime_invalid_sequence",
		retryable: false,
		fatal: false,
		reason: "runtime_contract_validation",
		retryStatus: { type: "exhausted" },
		sessionId: session.sessionId,
	});
	const terminalAppend = await appendTerminalEventsBestEffort(
		options,
		session,
		failure,
	);
	if (!terminalAppend.ok) {
		return {
			type: "failed",
			error: terminalAppend.error,
			releaseSession: { reason: "event_write_failed" },
		};
	}
	return {
		type: "failed",
		error: terminalAppend.settledFailure,
		...(terminalAppend.failureEventId === undefined
			? {}
			: { failureEventId: terminalAppend.failureEventId }),
	};
}

async function closeRecoveredOpenRequestForUserInterrupt(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	custody: ThreadLoopRunCustody,
): Promise<ThreadLoopRunResult> {
	const request = session.state.threadTurnTransition().checkpoint.request;
	if (request === undefined || request.requestEnd !== undefined) {
		session.state.markUserInterruptCloseoutEligible();
		return { type: "interrupted" };
	}
	const command = session.state.userInterruptCommand();
	const interruptLeaseRef =
		command === undefined
			? undefined
			: custody.interruptLeaseRef(command.runtimeInputId);
	if (command === undefined || interruptLeaseRef === undefined) {
		return failRecoveredOpenRequest(session);
	}
	const end = await appendModelRequestEndEvent(
		options,
		session,
		request.modelRequestId,
		request.requestStartEventId,
		true,
		"runtime_interrupted",
		"cancelled",
		undefined,
		[],
		request.requestKind,
		undefined,
		undefined,
		undefined,
		{ command, interruptLeaseRef },
	);
	if (!end.ok) {
		return {
			type: "failed",
			error: end.error,
			releaseSession: { reason: "event_write_failed" },
		};
	}
	if (end.type === "stale") {
		session.state.recordJoinedUserInterruptResult(
			command.runtimeInputId,
			{ ok: true, stale: true },
			{ inputKind: "interrupt" },
		);
		return { type: "interrupted", discardHotState: true };
	}
	if (!applyJoinedInterruptRequestEnd(session, end)) {
		return failRecoveredOpenRequest(session);
	}
	const projectionFailure = applyFailedRequestProviderProjection(session);
	if (projectionFailure !== undefined) {
		return { type: "failed", error: projectionFailure };
	}
	releaseInterruptedPendingTools(
		session,
		options,
		end.interruptToolResults.map((result) => result.toolUseEventId),
	);
	session.state.markUserInterruptCloseoutEligible();
	const idle = await appendIdleEvent(
		options,
		session,
		custody,
		{ type: "end_turn" },
		undefined,
		true,
	);
	if (!idle.ok) {
		return {
			type: "failed",
			error: idle.error,
			releaseSession: { reason: "event_write_failed" },
		};
	}
	session.state.completeUserInterrupt(command.runtimeInputId);
	return { type: "interrupted" };
}

export function settleIdleInterrupt(
	session: ThreadRuntime,
	command: RuntimeInterruptCommandState,
	commitInput: RuntimeControlInputCommit,
	metrics: RuntimeMetricsSink = NoopRuntimeMetricsSink,
): Effect.Effect<IdleInterruptSettlementResult> {
	return Effect.gen(function* () {
		const committed = yield* Effect.promise(() =>
			commitInput({ inputKind: "interrupt" }),
		);
		if (!committed.ok) {
			return {
				type: "commit_failed" as const,
				retryable: committed.retryable,
				errorCode: committed.errorCode,
			};
		}
		if ("stale" in committed) {
			return { type: "stale" as const };
		}
		if ("joined" in committed) {
			if (
				session.state.userInterruptCommand()?.runtimeInputId ===
				command.runtimeInputId
			) {
				session.state.completeUserInterrupt(command.runtimeInputId);
				session.state.finishThreadRunProjection();
			}
			return { type: "duplicate" as const };
		}

		const admitted = session.state.beginUserInterrupt(
			command,
			async () => committed,
		);
		if (admitted === "conflict") {
			return { type: "conflict" as const };
		}
		if (admitted === "applied") {
			session.state.discardQueuedAcceptedInputsForInterrupt(
				command.origin === "user",
			);
		}

		const application = yield* Effect.promise(() =>
			session.state.commitUserInterruptInput({ inputKind: "interrupt" }),
		);
		if (!application.result.ok) {
			return {
				type: "commit_failed" as const,
				retryable: application.result.retryable,
				errorCode: application.result.errorCode,
			};
		}
		if ("stale" in application.result) {
			return { type: "stale" as const };
		}
		if ("joined" in application.result) {
			session.state.completeUserInterrupt(command.runtimeInputId);
			session.state.finishThreadRunProjection();
			return { type: "duplicate" as const };
		}

		try {
			const pendingTools = [
				...session.state.pendingApprovalToolJobs(),
				...session.state.resolvedToolRouteJobs(),
				...session.state.pendingSandboxExecutionJobs(),
			];
			session.state.contextManager.appendInterruptToolResults(
				pendingTools.map((pending) => ({
					toolUseEventId: pending.toolUseEventId,
					assistantMessageSequence: pending.assistantMessageSequence,
					modelToolCallId: pending.toolPart.modelToolCallId,
				})),
				application.result.interruptToolResults,
			);
			session.state.addPendingAttachments(application.result.pendingAttachments);
			session.state.applyThreadTurnFact({
				fact: "interrupt_committed",
				eventId: command.runtimeInputId,
			});
			for (const cancellation of application.result.interruptToolResults) {
				if (
					session.state
						.pendingApprovalToolJobs()
						.some(
							(pending) =>
								pending.toolUseEventId === cancellation.toolUseEventId,
						)
				) {
					session.state.removePendingApprovalToolJob(
						cancellation.toolUseEventId,
					);
					metrics.addPendingApprovals(-1);
				}
				session.state.clearThreadToolRoute(cancellation.toolUseEventId);
				session.state.removeResolvedToolRouteJob(cancellation.toolUseEventId);
				session.state.removePendingSandboxExecutionJob(
					cancellation.toolUseEventId,
				);
			}
		} catch {
			return { type: "projection_failed" as const };
		}

		session.state.completeUserInterrupt(command.runtimeInputId);
		session.state.finishThreadRunProjection();
		return {
			type: "applied" as const,
			wakeThread: threadNeedsRun(session),
		};
	});
}

export function settleToolConfirmation(
	session: ThreadRuntime,
	command: RuntimeToolConfirmationState,
	commitInput: RuntimeControlInputCommit,
): Effect.Effect<ToolConfirmationSettlementResult> {
	return Effect.gen(function* () {
		const existingConfirmation = session.state.toolConfirmation(
			command.toolUseEventId,
		);
		if (existingConfirmation?.runtimeInputId === command.runtimeInputId) {
			return { type: "duplicate" as const };
		}

		const pendingTool = session.state
			.pendingApprovalToolJobs()
			.find((pending) => pending.toolUseEventId === command.toolUseEventId);
		if (pendingTool === undefined) {
			const replay = yield* Effect.promise(() =>
				commitInput({ inputKind: "tool_confirmation" }),
			);
			if (!replay.ok) {
				return {
					type: "failed" as const,
					reason: "context_load_failed" as const,
				};
			}
			if ("stale" in replay) {
				return { type: "stale" as const };
			}
			if (
				"type" in replay &&
				replay.assignedContextSequences.length > 0 &&
				replay.assignedContextSequences.every(
					(sequence) => session.state.contextManager.entry(sequence) !== undefined,
				)
			) {
				return { type: "duplicate" as const };
			}
			return {
				type: "failed" as const,
				reason: "context_load_failed" as const,
			};
		}

		const draft = toolConfirmationContext({
			toolUseEventId: command.toolUseEventId,
			pendingTool,
			decision: command.decision,
			...(command.denyMessage === undefined
				? {}
				: { denyMessage: command.denyMessage }),
		});
		const committed = yield* Effect.promise(() =>
			commitInput({ inputKind: "tool_confirmation" }),
		);
		if (!committed.ok) {
			return {
				type: "failed" as const,
				reason: "context_load_failed" as const,
			};
		}
		if ("stale" in committed) {
			return { type: "stale" as const };
		}
		if (!("type" in committed)) {
			return {
				type: "failed" as const,
				reason: "control_conflict" as const,
			};
		}

		const message = applyAcceptedInputResult(
			[draft],
			committed.assignedContextSequences,
		)[0];
		if (message === undefined) {
			return {
				type: "failed" as const,
				reason: "control_conflict" as const,
			};
		}
		const existingEntry = session.state.contextManager.entry(
			message.messageSequence,
		);
		if (existingEntry === undefined) {
			session.state.contextManager.appendEntry(message);
		} else if (JSON.stringify(existingEntry) !== JSON.stringify(message)) {
			return {
				type: "failed" as const,
				reason: "control_conflict" as const,
			};
		}
		session.state.addPendingAttachments(committed.pendingAttachments);
		session.state.applyThreadTurnFact({
			fact: "inputs_committed",
			eventId: command.runtimeInputId,
			contextSequences: [message.messageSequence],
		});
		const confirmation = session.state.resolveToolConfirmation(command);
		if (confirmation === "conflict") {
			return {
				type: "failed" as const,
				reason: "control_conflict" as const,
			};
		}
		const wakeThread = session.state.hasPendingApprovalToolJobs();
		if (wakeThread) {
			session.state.recordThreadToolRoute(
				command.toolUseEventId,
				"resume_approval_settlement",
			);
		}
		return {
			type: "applied" as const,
			applied: confirmation === "applied",
			wakeThread,
		};
	});
}

function runThreadLoopEffect(
	contextLoader: ContextLoaderInterface,
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	custody: ThreadLoopRunCustody,
): Effect.Effect<ThreadLoopRunResult, unknown> {
	let pendingProviderRequestReschedule = false;
	let recoveredRescheduleDeadline: string | undefined;
	let recoveredRescheduleKind:
		| "agent_provider_request"
		| "compaction_summary"
		| "approval_reviewer"
		| undefined;
	let recoveredFailureWithoutReschedule = false;
	const run: Effect.Effect<ThreadLoopRunResult, unknown> = Effect.gen(
		function* () {
			let pendingInput: PendingInputResult = { type: "empty" };
			const turnRetryCounters = {
				providerAttempts: 0,
				compactionAttempts: 0,
			};
			const rejectedAttachments: RejectedProviderAttachment[] = [];
			// Reactive compaction backstop: one-shot flag driving one compaction+rebuild+re-issue
			// per context-overflow episode. SET when Gateway's context_overflow is intercepted
			// before the terminal path; CLEARED by the next successfully completed provider request
			// (a completed sweep or a waiting_external exit). While set, proactive compaction is
			// suppressed for the rebuilt request, and an overflow arriving while it is still set
			// falls through to the terminal path. Budgets are separate: an overflow is never
			// provider-rescheduled, the compaction sub-request keeps its own reschedule budget, and
			// the rebuilt request runs under the normal provider budget.
			let reactiveContextOverflowPending = false;
			let runStatusRunningAppended =
				custody.activeTurnId(session) !== undefined;
			if (
				session.state.runtimeShutdownRequested() ||
				session.state.userInterruptRequested() ||
				session.state.cooperativeCancelRequested()
			) {
				if (session.state.userInterruptRequested()) {
					session.state.markUserInterruptCloseoutEligible();
				}
				return { type: "interrupted" };
			}
			seedRuntimeModel(session, options);
			const recoveredNextStep = interpretThreadTurnNextStep(
				session.state.threadTurnTransition().nextStep,
			).nextStep;
			if (recoveredNextStep.action === "close_interrupted") {
				return yield* nonAbandonablePromise(() =>
					executeRecoveredCloseInterruptedNextStep(session, options, custody),
				);
			}
			const recoveredRequest =
				session.state.threadTurnTransition().checkpoint.request;
			const reschedule = recoveredRequest?.requestEnd?.reschedule;
			const hasIncompleteTool = recoveredRequest?.toolMembers.some(
					(member) =>
						member.memberKind === "public_tool_use" &&
						member.terminalResult === undefined,
				) ?? false;
			if (
				recoveredNextStep.action === "apply_request_retry_or_reschedule" ||
				reschedule !== undefined ||
				(recoveredRequest?.requestEnd?.isError === true &&
					hasIncompleteTool)
			) {
				if (
					recoveredRequest === undefined ||
					(reschedule === undefined && !hasIncompleteTool)
				) {
					return yield* nonAbandonablePromise(() =>
						executeRecoveredRequestRetryOrRescheduleNextStep(session, options),
					);
				}
				if (reschedule === undefined) {
					recoveredFailureWithoutReschedule = true;
					pendingProviderRequestReschedule = true;
				} else {
					turnRetryCounters.providerAttempts =
						reschedule.providerAttempts;
					turnRetryCounters.compactionAttempts =
						reschedule.compactionAttempts;
					pendingProviderRequestReschedule = true;
					recoveredRescheduleDeadline = reschedule.effectiveDeadline;
					recoveredRescheduleKind = recoveredRequest.requestKind;
				}
			}
			if (recoveredNextStep.action === "await_request_end") {
				return failRecoveredOpenRequest(session);
			}
			while (true) {
				let acceptedContextCommitted = false;
				let statusRunningAlreadyAppended = runStatusRunningAppended;
				if (
					session.state.runtimeShutdownRequested() ||
					session.state.userInterruptRequested() ||
					session.state.cooperativeCancelRequested()
				) {
					if (session.state.userInterruptRequested()) {
						session.state.markUserInterruptCloseoutEligible();
					}
					return { type: "interrupted" };
				}
				// The reducer selects one exact accepted-input identity at a safe Turn
				// boundary. Inputs admitted while Provider or Tool work is open remain in
				// ThreadState and cannot be projected into the in-flight request.
				const selectedNextStep = interpretThreadTurnNextStep(
					session.state.threadTurnTransition().nextStep,
				).nextStep;
				const selectedAcceptedInput =
					selectedNextStep.action === "commit_accepted_input"
						? session.state
								.acceptedInputSnapshot()
								.find(
									(input) =>
										input.runtimeInputId === selectedNextStep.runtimeInputId,
								)
						: undefined;
				if (
					selectedNextStep.action === "commit_accepted_input" &&
					selectedAcceptedInput === undefined
				) {
					throw new Error(
						"reducer selected an accepted input that is not owned by ThreadState",
					);
				}
				const acceptedInputCut =
					selectedAcceptedInput === undefined ? [] : [selectedAcceptedInput];
				const committedContextEntries: RuntimeContextEntry[] = [];
				const committedRequestInputEntries: RuntimeContextEntry[] = [];
				for (const acceptedInput of acceptedInputCut) {
					const runningAppend = yield* nonAbandonablePromise(() =>
						appendRunningEvent(
							options,
							session,
							custody,
							acceptedInput.kind,
							acceptedInput.runtimeInputId,
						),
					);
					if (!runningAppend.ok) {
						return {
							type: "failed",
							error: runtimeFailureFromEventWriter(runningAppend.error),
							releaseSession: { reason: "event_write_failed" },
						};
					}
					statusRunningAlreadyAppended = true;
					runStatusRunningAppended = true;
					session.state.beginAcceptedInputCommit(acceptedInput.runtimeInputId);
					if (acceptedInput.kind === "task_notification") {
						const declaration = yield* effectFromAbortablePromise((signal) =>
							commitAcceptedInputWithRetry(
								contextLoader,
								acceptedInput,
								options,
								signal,
							),
						).pipe(
							Effect.ensuring(
								Effect.sync(() =>
									session.state.finishAcceptedInputCommit(
										acceptedInput.runtimeInputId,
									),
								),
							),
							Effect.uninterruptible,
						);
						if (!declaration.ok) {
							if (declaration.exhausted) {
								return {
									type: "failed",
									error: normalizeRuntimeFailure({
										type: "runtime",
										code: "runtime_persistence_exhausted",
										reason: "runtime_input_commit_exhausted",
										retryable: false,
										fatal: true,
										retryStatus: { type: "terminal" },
										sessionId: session.sessionId,
									}),
								};
							}
							return yield* nonAbandonablePromise(() =>
								handleContextLoaderFailure(options, session, declaration.error),
							);
						}
						if (declaration.result.type === "task_notification_deferred") {
							session.state.acknowledgeAcceptedInput(
								acceptedInput.runtimeInputId,
							);
							continue;
						}
						if (declaration.result.type === "task_notification_rejected") {
							session.state.acknowledgeAcceptedInput(
								acceptedInput.runtimeInputId,
							);
							continue;
						}
						if (
							declaration.result.type === "stale_custody" ||
							declaration.result.type === "barrier_stale_custody"
						) {
							session.state.clearAfterCustodyHandoff();
							return { type: "interrupted", discardHotState: true };
						}
						const { kind: _kind, ...command } = acceptedInput;
						const committedEntries = applyAcceptedInputResult(
							declaration.contextDrafts,
							declaration.result.assignedContextSequences,
						);
						const committedEntry = committedEntries[0];
						if (committedEntries.length !== 1 || committedEntry === undefined) {
							throw new Error(
								"task notification commit did not assign one context entry",
							);
						}
						const notification = session.state.commitTaskNotification({
							...command,
							committedEntry,
						});
						if (notification === "conflict") {
							return yield* nonAbandonablePromise(() =>
								handleContextLoaderFailure(
									options,
									session,
									normalizeContextLoaderError({
										code: "schema_mismatch",
										sessionId: session.sessionId,
										reason:
											"task notification context conflicts with hot state",
									}),
								),
							);
						}
						session.state.acknowledgeAcceptedInput(
							acceptedInput.runtimeInputId,
						);
						if (notification === "applied") {
							session.state.applyThreadTurnFact({
								fact: "inputs_committed",
								eventId: acceptedInput.runtimeInputId,
								contextSequences: [committedEntry.messageSequence],
							});
							session.state.setProviderOutputSchemaJson(undefined);
							acceptedContextCommitted = true;
						}
					} else {
						// CommitInputs and its typed result application form one local ownership boundary:
						// interruption may observe either the queued input or the applied result,
						// never a durable write whose hot acknowledgement is still missing.
						const committed = yield* Effect.gen(function* () {
							const declaration = yield* effectFromAbortablePromise((signal) =>
								commitAcceptedInputWithRetry(
									contextLoader,
									acceptedInput,
									options,
									signal,
								),
							);
							if (!declaration.ok) {
								if (declaration.exhausted) {
									return {
										type: "exhausted" as const,
										error: normalizeRuntimeFailure({
											type: "runtime",
											code: "runtime_persistence_exhausted",
											reason: "runtime_input_commit_exhausted",
											retryable: false,
											fatal: true,
											retryStatus: { type: "terminal" },
											sessionId: session.sessionId,
										}),
									};
								}
								return { type: "failed" as const, error: declaration.error };
							}
							if (
								declaration.result.type === "stale_custody" ||
								declaration.result.type === "barrier_stale_custody"
							) {
								return { type: "stale_custody" as const };
							}
							if (
								declaration.result.type !== "committed" &&
								declaration.result.type !== "duplicate"
							) {
								return {
									type: "failed" as const,
									error: normalizeContextLoaderError({
										code: "schema_mismatch",
										sessionId: session.sessionId,
										reason:
											"non-task accepted input returned a task notification outcome",
									}),
								};
							}
							const residentSequences = new Set(
								session.state.contextManager
									.entries()
									.map((entry) => entry.messageSequence),
							);
							const contextEntries = applyAcceptedInputResult(
								declaration.contextDrafts,
								declaration.result.assignedContextSequences,
							);
							const newlyResidentContextEntries = contextEntries.filter(
								(entry) => !residentSequences.has(entry.messageSequence),
							);
							const pendingContextSequences = new Set(
								session.state.threadTurnTransition().checkpoint
									.pendingInputContextSequences,
							);
							const resumesPendingInput =
								declaration.result.assignedContextSequences.length > 0 &&
								declaration.result.assignedContextSequences.every((sequence) =>
									pendingContextSequences.has(sequence),
								);
							session.state.addPendingAttachments(
								declaration.result.pendingAttachments,
							);
							session.state.setProviderOutputSchemaJson(
								acceptedInput.kind === "approval_review"
									? acceptedInput.outputSchemaJson
									: undefined,
							);
							const makesRequestReady =
								(contextEntries.length > 0 ||
									declaration.result.pendingAttachments.length > 0) &&
								(newlyResidentContextEntries.length > 0 ||
									resumesPendingInput) &&
								(acceptedInput.kind === "messages" ||
									acceptedInput.kind === "approval_review" ||
									acceptedInput.kind === "inter_agent_message");
							session.state.acknowledgeAcceptedInput(
								acceptedInput.runtimeInputId,
							);
							if (makesRequestReady) {
								session.state.applyThreadTurnFact({
									fact: "inputs_committed",
									eventId: acceptedInput.runtimeInputId,
									contextSequences: contextEntries.map(
										(entry) => entry.messageSequence,
									),
								});
							}
							return {
								type: "committed" as const,
								contextEntries: newlyResidentContextEntries,
								makesRequestReady,
							};
						}).pipe(
							Effect.ensuring(
								Effect.sync(() =>
									session.state.finishAcceptedInputCommit(
										acceptedInput.runtimeInputId,
									),
								),
							),
							Effect.uninterruptible,
						);
						if (committed.type === "failed") {
							return yield* nonAbandonablePromise(() =>
								handleContextLoaderFailure(options, session, committed.error),
							);
						}
						if (committed.type === "exhausted") {
							return { type: "failed", error: committed.error };
						}
						if (committed.type === "stale_custody") {
							session.state.clearAfterCustodyHandoff();
							return { type: "interrupted", discardHotState: true };
						}
						committedContextEntries.push(...committed.contextEntries);
						if (committed.makesRequestReady) {
							committedRequestInputEntries.push(...committed.contextEntries);
							acceptedContextCommitted = true;
						}
					}
				}
				for (const entry of committedContextEntries) {
					session.state.contextManager.appendEntry(entry);
				}
				if (
					acceptedInputCut.length > 0 &&
					session.state.acceptedInputSnapshot().length > 0
				) {
					// One stable next step owns one commit. Derive again before opening
					// provider work so the next admitted identity is selected explicitly.
					pendingInput = { type: "empty" };
					continue;
				}
				pendingInput =
					committedRequestInputEntries.length === 0
						? { type: "empty" }
						: { type: "context", entries: committedRequestInputEntries };
				const pendingApprovalResume = yield* resumeRecoveredToolJobsEffect(
					session,
					options,
					custody,
				);
				if (pendingApprovalResume.type === "failed") {
					return pendingApprovalResume;
				}
				if (pendingApprovalResume.type === "interrupted") {
					return pendingApprovalResume.attachmentRideDisposition ===
						"discard_hot_state"
						? { type: "interrupted", discardHotState: true }
						: { type: "interrupted" };
				}
				if (session.state.userInterruptRequested()) {
					session.state.markUserInterruptCloseoutEligible();
					return { type: "interrupted" };
				}
				if (pendingApprovalResume.type === "waiting_external") {
					if (pendingProviderRequestReschedule) {
						// The failed Request already owns retry/reschedule reduction.
						// Keep the exact approval route active without authoring a
						// requires-action Idle transition for a sealed error Request.
						return { type: "interrupted" };
					}
					if (custody.activeTurnId(session) === undefined) {
						return completedHotStateRunResult(session);
					}
					const idleAppend = yield* nonAbandonablePromise(() =>
						appendIdleEvent(options, session, custody, {
							type: "requires_action",
							event_ids: [...pendingApprovalResume.blockingEventIds],
						}),
					);
					if (!idleAppend.ok) {
						return {
							type: "failed",
							error: idleAppend.error,
							releaseSession: { reason: "event_write_failed" },
						};
					}
					return completedHotStateRunResult(session);
				}
				if (pendingApprovalResume.type === "resumed") {
					statusRunningAlreadyAppended = true;
					pendingInput = { type: "empty" };
				}
				const postToolNextStep = interpretThreadTurnNextStep(
					session.state.threadTurnTransition().nextStep,
				).nextStep;
				if (postToolNextStep.action === "commit_accepted_input") {
					pendingInput = { type: "empty" };
					continue;
				}
				if (
					pendingProviderRequestReschedule &&
					recoveredRescheduleDeadline !== undefined
				) {
					const projectionFailure = applyFailedRequestProviderProjection(session);
					if (projectionFailure !== undefined) {
						return { type: "failed", error: projectionFailure };
					}
					const waited = yield* waitForProviderRequestRescheduleEffect(
						session,
						options,
						recoveredRescheduleDeadline,
					);
					if (waited.type !== "deadline") {
						pendingProviderRequestReschedule = false;
						if (waited.type === "user_interrupt") {
							session.state.markUserInterruptCloseoutEligible();
						}
						return { type: "interrupted" };
					}
					recoveredRescheduleDeadline = undefined;
				}
				if (recoveredFailureWithoutReschedule) {
					const projectionFailure = applyFailedRequestProviderProjection(session);
					if (projectionFailure !== undefined) {
						return { type: "failed", error: projectionFailure };
					}
					return yield* nonAbandonablePromise(() =>
						executeRecoveredRequestRetryOrRescheduleNextStep(session, options),
					);
				}
				if (
					(pendingInput.type !== "context" ||
						pendingInput.entries.length === 0) &&
					!acceptedContextCommitted &&
					!pendingProviderRequestReschedule
				) {
					const resumedNextStep = interpretThreadTurnNextStep(
						session.state.threadTurnTransition().nextStep,
					).nextStep;
					if (resumedNextStep.action === "commit_accepted_input") {
						pendingInput = { type: "empty" };
						continue;
					}
					if (
						resumedNextStep.action !== "prepare_next_request" &&
						resumedNextStep.action !== "continue_after_compaction"
					) {
						return yield* nonAbandonablePromise(() =>
							completeRun(session, options, custody),
						);
					}
				}
				const committedContext = session.state.contextManager.entries();
				const providerContextMessages =
					session.state.contextManager.providerEntrySegments();
				let currentModel = session.state.currentModel();
				const hasPendingAttachments =
					session.state.pendingAttachments().length > 0;
				const projected = providerContextMessages.every(
					(segment) => segment.length === 0,
				)
					? hasPendingAttachments
						? { ok: true as const, context: [] }
						: undefined
					: toGatewayProviderContextSegments(providerContextMessages);
				if (projected !== undefined && !projected.ok) {
					session.state.clear();
					return {
						type: "failed",
						error: projected.error,
						releaseSession: { reason: "crashed" },
					};
				}
				if (currentModel === undefined) {
					const failure = normalizeRuntimeFailure({
						type: "runtime",
						code: "runtime_invalid_sequence",
						retryable: false,
						fatal: true,
						reason: "runtime_contract_validation",
						sessionId: session.sessionId,
					});
					const terminalAppend = yield* nonAbandonablePromise(() =>
						appendTerminalEventsBestEffort(options, session, failure),
					);
					if (!terminalAppend.ok) {
						return {
							type: "failed",
							error: terminalAppend.error,
							releaseSession: { reason: "event_write_failed" },
						};
					}
					return {
						type: "failed",
						error: terminalAppend.settledFailure,
						...(terminalAppend.failureEventId === undefined
							? {}
							: { failureEventId: terminalAppend.failureEventId }),
					};
				}
				if (
					projected === undefined ||
					(projected.context.length === 0 && !hasPendingAttachments)
				) {
					return yield* nonAbandonablePromise(() =>
						completeRun(session, options, custody),
					);
				}
				const bindingTokenRefresh = yield* Effect.promise(() =>
					refreshSessionRuntimeBindingToken(session, options),
				);
				if (bindingTokenRefresh.type === "stale_custody") {
					return { type: "interrupted", discardHotState: true };
				}
				if (bindingTokenRefresh.type === "failed") {
					const terminalAppend = yield* nonAbandonablePromise(() =>
						appendTerminalEventsBestEffort(
							options,
							session,
							bindingTokenRefresh.error,
						),
					);
					if (!terminalAppend.ok) {
						return {
							type: "failed",
							error: terminalAppend.error,
							releaseSession: { reason: "event_write_failed" },
						};
					}
					return {
						type: "failed",
						error: terminalAppend.settledFailure,
						...(terminalAppend.failureEventId === undefined
							? {}
							: { failureEventId: terminalAppend.failureEventId }),
					};
				}
				if (!statusRunningAlreadyAppended) {
					const pendingTool = session.state.pendingApprovalToolJobs()[0];
					const turnReduction = session.state.threadTurnTransition();
					const recoveredOpeningSource =
						recoveredThreadRunOpeningSource(turnReduction);
					const openingSource =
						pendingTool === undefined
							? recoveredOpeningSource
							: { kind: "pending_tool", id: pendingTool.toolUseEventId };
					if (openingSource === undefined) {
						return {
							type: "failed",
							error: normalizeRuntimeFailure({
								type: "runtime",
								code: "runtime_invalid_sequence",
								retryable: false,
								fatal: true,
								reason: "runtime_contract_validation",
								sessionId: session.sessionId,
							}),
						};
					}
					const runningAppend = yield* nonAbandonablePromise(() =>
						appendRunningEvent(
							options,
							session,
							custody,
							openingSource.kind,
							openingSource.id,
						),
					);
					if (!runningAppend.ok) {
						return {
							type: "failed",
							error: runtimeFailureFromEventWriter(runningAppend.error),
							releaseSession: { reason: "event_write_failed" },
						};
					}
					statusRunningAlreadyAppended = true;
					runStatusRunningAppended = true;
				}
				let providerContext = projected.context;
				let requestContextAnchorSequence = Math.max(
					0,
					highestMessageSequence(committedContext),
				);
				const compactionResult =
					recoveredRescheduleKind === "compaction_summary"
						? options.compaction === undefined
							? {
									type: "failed" as const,
									result: {
										type: "failed" as const,
										error: normalizeRuntimeFailure({
											type: "runtime",
											code: "runtime_invalid_sequence",
											retryable: false,
											fatal: true,
											reason: "runtime_contract_validation",
											sessionId: session.sessionId,
										}),
									},
								}
							: yield* runCompactionSummaryEffect(
									session,
									options,
									custody,
									committedContext,
									options.compaction,
									turnRetryCounters,
									(pending) => {
										pendingProviderRequestReschedule = pending;
									},
								)
						: yield* coordinateCompactionBeforeProviderRequestEffect(
								session,
								options,
								custody,
								committedContext,
								turnRetryCounters,
								(pending) => {
									pendingProviderRequestReschedule = pending;
								},
							);
				recoveredRescheduleKind = undefined;
				if (compactionResult.type === "failed") {
					return compactionResult.result;
				}
				if (compactionResult.type === "interrupted") {
					return compactionResult.discardHotState === true
						? { type: "interrupted", discardHotState: true }
						: { type: "interrupted" };
				}
				if (compactionResult.type === "applied") {
					if (session.state.acceptedInputSnapshot().length > 0) {
						pendingInput = { type: "empty" };
						continue;
					}
					currentModel = compactionResult.currentModel;
					providerContext = compactionResult.projectedContext;
					requestContextAnchorSequence = compactionResult.contextAnchorSequence;
				}
				const baseResult = {
					type: "completed" as const,
					modelMessageCount: providerContext.length,
					currentModel,
				};
				if (
					session.state.runtimeShutdownRequested() ||
					session.state.userInterruptRequested() ||
					session.state.cooperativeCancelRequested()
				) {
					if (session.state.userInterruptRequested()) {
						session.state.markUserInterruptCloseoutEligible();
					}
					return { type: "interrupted" };
				}
				const executionPolicy = requestExecutionPolicy(session, options);
				const executionSnapshot = session.configuration.snapshot({
					currentModel,
					approvalMode:
						executionPolicy.approvalMode ??
						options.approvalMode ??
						"ask_for_approval",
					toolPolicyJson: JSON.stringify({
						approvalMode:
							executionPolicy.approvalMode ??
							options.approvalMode ??
							"ask_for_approval",
					}),
					toolCatalogJson: JSON.stringify(executionPolicy.toolCatalog ?? null),
				});
				const assembledRequest = yield* Effect.promise(() =>
					assembleLLMRequest(
						session,
						options,
						currentModel,
						providerContext,
						executionPolicy,
					),
				);
				if (!assembledRequest.ok) {
					const terminalAppend = yield* nonAbandonablePromise(() =>
						appendTerminalEventsBestEffort(
							options,
							session,
							assembledRequest.error,
						),
					);
					if (!terminalAppend.ok) {
						return {
							type: "failed",
							error: terminalAppend.error,
							releaseSession: { reason: "event_write_failed" },
						};
					}
					return {
						type: "failed",
						error: terminalAppend.settledFailure,
						...(terminalAppend.failureEventId === undefined
							? {}
							: { failureEventId: terminalAppend.failureEventId }),
					};
				}
				const requestForAttempt = providerRequestWithoutRejectedAttachments(
					assembledRequest.request,
					rejectedAttachments,
				);
				const runtimeAttachmentsForAttempt =
					runtimeAttachmentsWithoutRejectedAttachments(
						assembledRequest.runtimeAttachments,
						rejectedAttachments,
					);
				pendingProviderRequestReschedule = false;
				const runtimeResult = yield* coordinateProviderTurnEffect(
					session,
					options,
					custody,
					requestForAttempt,
					runtimeAttachmentsForAttempt,
					turnRetryCounters,
					rejectedAttachments,
					options.compaction !== undefined && !reactiveContextOverflowPending,
					requestContextAnchorSequence,
					executionSnapshot,
					executionPolicy,
				);
				if (runtimeResult.attachmentRideDisposition === "settled") {
					session.state.settlePendingAttachmentRide();
				}
				if (runtimeResult.type === "rescheduled") {
					pendingProviderRequestReschedule = true;
					const waited = yield* waitForProviderRequestRescheduleEffect(
						session,
						options,
						runtimeResult.effectiveDeadline,
					);
					if (waited.type !== "deadline") {
						pendingProviderRequestReschedule = false;
						if (waited.type === "user_interrupt") {
							session.state.markUserInterruptCloseoutEligible();
						}
						return { type: "interrupted" };
					}
					pendingInput = { type: "empty" };
					continue;
				}
				if (runtimeResult.type === "context_overflow") {
					reactiveContextOverflowPending = true;
					const compaction = options.compaction;
					if (compaction === undefined) {
						return { type: "failed", error: runtimeResult.failure };
					}
					const reactiveMessages = session.state.contextManager.entries();
					const reactiveCompaction = yield* runCompactionSummaryEffect(
						session,
						options,
						custody,
						reactiveMessages,
						compaction,
						turnRetryCounters,
						(pending) => {
							pendingProviderRequestReschedule = pending;
						},
					);
					if (reactiveCompaction.type === "failed") {
						return reactiveCompaction.result;
					}
					if (reactiveCompaction.type === "interrupted") {
						return reactiveCompaction.discardHotState === true
							? { type: "interrupted", discardHotState: true }
							: { type: "interrupted" };
					}
					if (reactiveCompaction.type !== "applied") {
						return { type: "failed", error: runtimeResult.failure };
					}
					pendingInput = { type: "empty" };
					continue;
				}
				if (runtimeResult.type === "failed") {
					return runtimeResult;
				}
				if (
					runtimeResult.type === "interrupted" ||
					session.state.runtimeShutdownRequested()
				) {
					return runtimeResult.attachmentRideDisposition === "discard_hot_state"
						? { type: "interrupted", discardHotState: true }
						: { type: "interrupted" };
				}
				if (runtimeResult.type === "waiting_external") {
					reactiveContextOverflowPending = false;
					const idleAppend = yield* nonAbandonablePromise(() =>
						appendIdleEvent(options, session, custody, {
							type: "requires_action",
							event_ids: [...runtimeResult.blockingEventIds],
						}),
					);
					if (!idleAppend.ok) {
						return {
							type: "failed",
							error: idleAppend.error,
							releaseSession: { reason: "event_write_failed" },
						};
					}
					return baseResult;
				}
				reactiveContextOverflowPending = false;
				const turnTransition = session.state.threadTurnTransition();
				const turnNextStep = interpretThreadTurnNextStep(
					turnTransition.nextStep,
				).nextStep;
				if (
					turnNextStep.action === "commit_accepted_input" &&
					session.state
						.acceptedInputSnapshot()
						.some(
							(input) => input.runtimeInputId === turnNextStep.runtimeInputId,
						)
				) {
					pendingInput = { type: "empty" };
					continue;
				}
				if (turnNextStep.action === "prepare_next_request") {
					pendingInput = { type: "empty" };
					continue;
				}
				if (turnNextStep.action === "complete_reviewer") {
					const idleAppend = yield* nonAbandonablePromise(() =>
						appendIdleEvent(options, session, custody, { type: "end_turn" }),
					);
					if (!idleAppend.ok) {
						return {
							type: "failed",
							error: idleAppend.error,
							releaseSession: { reason: "event_write_failed" },
						};
					}
					return baseResult;
				}
				if (
					turnNextStep.action !== "finish_idle" ||
					turnNextStep.stopReason.type !== "end_turn"
				) {
					return {
						type: "failed",
						error: normalizeRuntimeFailure({
							type: "runtime",
							code: "runtime_invalid_sequence",
							retryable: false,
							fatal: true,
							reason: "runtime_contract_validation",
							sessionId: session.sessionId,
						}),
					};
				}
				const idleAppend = yield* nonAbandonablePromise(() =>
					appendIdleEvent(options, session, custody, { type: "end_turn" }),
				);
				if (!idleAppend.ok) {
					return {
						type: "failed",
						error: idleAppend.error,
						releaseSession: { reason: "event_write_failed" },
					};
				}
				return baseResult;
			}
		},
	);
	return run.pipe(
		Effect.flatMap((result) =>
			Effect.promise(() =>
				closeFailedThreadRun(
					options,
					session,
					custody,
					result,
					appendIdleEvent,
				),
			),
		),
		Effect.onExit((exit) =>
			Effect.suspend(() => {
				// A stale operation result has already revoked execution custody;
				// no interrupt fallback may issue another durable write afterward.
				if (
					Exit.isSuccess(exit) &&
					exit.value.type === "interrupted" &&
					exit.value.discardHotState === true
				) {
					return Effect.void;
				}
				if (session.state.userInterruptRequested()) {
					// Failed request closeout remains failed; only a clean or pure-interrupt
					// exit may perform the fallback interrupt commit and idle settlement.
					if (Exit.isFailure(exit) && !Cause.hasInterruptsOnly(exit.cause)) {
						return Effect.void;
					}
					return settleUserInterruptAtRunExitEffect(session, options, custody);
				}
				const hasUnsettledToolOwner =
					session.state.threadTurnTransition().checkpoint.request?.toolMembers.some(
						(member) =>
							member.memberKind === "public_tool_use" &&
							member.terminalResult === undefined,
					) ?? false;
				return pendingProviderRequestReschedule &&
					!hasUnsettledToolOwner &&
					!session.state.runtimeShutdownRequested()
					? nonAbandonablePromise(() =>
							appendIdleEvent(options, session, custody, { type: "end_turn" }),
						).pipe(Effect.asVoid)
					: Effect.void;
			}),
		),
	);
}

function settleUserInterruptAtRunExitEffect(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	custody: ThreadLoopRunCustody,
): Effect.Effect<void, unknown> {
	return Effect.gen(function* () {
		const pendingApprovalSettlement = yield* resumeRecoveredToolJobsEffect(
			session,
			options,
			custody,
		);
		if (pendingApprovalSettlement.type === "failed") {
			return yield* failRequestCloseout(pendingApprovalSettlement.error);
		}
		session.state.markUserInterruptCloseoutEligible();
		const interruptFence = yield* settleUserInterruptFenceEffect(
			session,
			options,
		);
		if (!interruptFence.ok) {
			return yield* failRequestCloseout(interruptFence.error);
		}
		if ("stale" in interruptFence) {
			return;
		}
		const durableTurnId = custody.activeTurnId(session);
		if (durableTurnId === undefined) {
			// A non-abandonable ordinary FinishIdle may have closed the turn while
			// the interrupt waited; its ACK is the required idle settlement.
			const command = session.state.userInterruptCommand();
			if (command !== undefined) {
				session.state.completeUserInterrupt(command.runtimeInputId);
			}
			return;
		}
		const idle = yield* nonAbandonablePromise(() =>
			appendIdleEvent(
				options,
				session,
				custody,
				{ type: "end_turn" },
				undefined,
				true,
			),
		);
		if (!idle.ok) {
			return yield* failRequestCloseout(idle.error);
		}
		const command = session.state.userInterruptCommand();
		if (command !== undefined) {
			session.state.completeUserInterrupt(command.runtimeInputId);
		}
	});
}

function waitForProviderRequestRescheduleEffect(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	effectiveDeadline: string,
): Effect.Effect<
	| { readonly type: "deadline" }
	| { readonly type: "runtime_shutdown" }
	| { readonly type: "user_interrupt" },
	unknown
> {
	const delayMs = Math.max(
		0,
		Date.parse(effectiveDeadline) - Date.parse(options.runtime.now()),
	);
	const wait = Effect.callback<{ readonly type: "deadline" }, unknown>(
		(resume, signal) => {
			options.runtime.sleep(delayMs, signal).then(
				() => resume(Effect.succeed({ type: "deadline" as const })),
				(error) => {
					if (!signal.aborted) {
						resume(Effect.fail(error));
					}
				},
			);
		},
	);
	return wait;
}

export async function commitAcceptedInputWithRetry(
	contextLoader: ContextLoaderInterface,
	input: ReturnType<ThreadRuntime["state"]["peekAcceptedInput"]> extends infer T
		? Exclude<T, undefined>
		: never,
	options: ThreadLoopRuntimeOptions,
	signal: AbortSignal,
): Promise<
	| {
			readonly ok: true;
			readonly result: AcceptedInputCommitResult;
			readonly contextDrafts: ReturnType<typeof acceptedInputContextDrafts>;
	  }
	| { readonly ok: false; readonly error: unknown; readonly exhausted: boolean }
> {
	const contextDrafts = acceptedInputContextDrafts(input);
	if (contextLoader.commitAcceptedInput === undefined) {
		return {
			ok: false,
			error: normalizeContextLoaderError({
				code: "unavailable",
				sessionId: input.sessionId,
				reason: "accepted input commit boundary is unavailable",
			}),
			exhausted: false,
		};
	}
	let lastError: unknown = normalizeContextLoaderError({
		code: "unavailable",
		sessionId: input.sessionId,
		reason: "accepted input commit failed",
	});
	for (
		let attempt = 1;
		attempt <= SessionEventWriterRetryPolicy.attempts;
		attempt += 1
	) {
		const attemptStartedAt = options.runtime.monotonicMs();
		recordAcceptedInputCommit(
			options,
			input,
			attempt,
			attemptStartedAt,
			"started",
		);
		try {
			const timeoutController = new AbortController();
			let settled:
				| {
						readonly type: "result";
						readonly result: AcceptedInputCommitResult;
				  }
				| { readonly type: "error"; readonly error: unknown }
				| undefined;
			const rawAttempt = contextLoader
				.commitAcceptedInput(
					input,
					input.kind === "approval_review"
						? { approvalReviewText: input.promptText }
						: undefined,
				)
				.then(
					(result) => {
						runtimeMetrics(options).observeContextLoadLatency(
							"commit_accepted_input",
							options.runtime.monotonicMs() - attemptStartedAt,
							"success",
						);
						return (settled = { type: "result" as const, result });
					},
					(error) => {
						runtimeMetrics(options).observeContextLoadLatency(
							"commit_accepted_input",
							options.runtime.monotonicMs() - attemptStartedAt,
							"error",
						);
						return (settled = { type: "error" as const, error });
					},
				);
			// A test clock and some embedders may resolve sleep immediately. Give an
			// already-settled transport result one microtask to win before arming
			// the local timeout; an actually pending write remains bounded below.
			await Promise.resolve();
			const attemptResult =
				settled ??
				(await Promise.race([
					rawAttempt,
					options.runtime
						.sleep(
							SessionEventWriterRetryPolicy.timeoutPerAttemptMs,
							timeoutController.signal,
						)
						.then((elapsed) => ({
							type: elapsed ? ("timeout" as const) : ("cancelled" as const),
						})),
				]));
			timeoutController.abort();
			if (attemptResult.type === "result") {
				recordAcceptedInputCommit(
					options,
					input,
					attempt,
					attemptStartedAt,
					attemptResult.result.type === "task_notification_deferred"
						? "deferred"
						: attemptResult.result.type === "task_notification_rejected"
							? "rejected"
							: "committed",
					attemptResult.result.type === "task_notification_rejected"
						? attemptResult.result.errorCode
						: undefined,
				);
				return { ok: true, result: attemptResult.result, contextDrafts };
			}
			if (attemptResult.type === "error") {
				throw attemptResult.error;
			}
			lastError = normalizeContextLoaderError({
				code: attemptResult.type === "timeout" ? "timeout" : "unavailable",
				sessionId: input.sessionId,
				reason:
					attemptResult.type === "timeout"
						? "accepted input commit attempt timed out"
						: "accepted input commit was cancelled",
			});
		} catch (error) {
			lastError = error;
			const parsed = ContextLoaderErrorSchema.safeParse(error);
			if (!parsed.success || !parsed.data.retryable) {
				recordAcceptedInputCommit(
					options,
					input,
					attempt,
					attemptStartedAt,
					"exhausted",
				);
				return { ok: false, error, exhausted: false };
			}
		}
		if (attempt === SessionEventWriterRetryPolicy.attempts) {
			recordAcceptedInputCommit(
				options,
				input,
				attempt,
				attemptStartedAt,
				"exhausted",
			);
			break;
		}
		recordAcceptedInputCommit(
			options,
			input,
			attempt,
			attemptStartedAt,
			"retry",
		);
		const backoffMs = SessionEventWriterRetryPolicy.backoffMs[attempt - 1] ?? 0;
		if (backoffMs > 0 && !(await options.runtime.sleep(backoffMs, signal))) {
			recordAcceptedInputCommit(
				options,
				input,
				attempt,
				attemptStartedAt,
				"exhausted",
			);
			return { ok: false, error: lastError, exhausted: false };
		}
	}
	return { ok: false, error: lastError, exhausted: true };
}

function recordAcceptedInputCommit(
	options: ThreadLoopRuntimeOptions,
	input: RuntimeAcceptedInputState,
	attempt: number,
	startedAt: number,
	outcome: RuntimeAcceptedInputCommitObservation["outcome"],
	failureClass?: RuntimeAcceptedInputCommitObservation["failureClass"],
): void {
	try {
		options.recordAcceptedInputCommit?.({
			workspaceId: input.workspaceId,
			sessionId: input.sessionId,
			sessionThreadId: input.sessionThreadId,
			runtimeInputId: input.runtimeInputId,
			inputKind: input.kind,
			attempt,
			durationMs: Math.max(0, options.runtime.monotonicMs() - startedAt),
			outcome,
			...(failureClass === undefined ? {} : { failureClass }),
		});
	} catch {
		// Durable operation results are authoritative; observability is fail-open.
	}
}

async function observeContextLoad<T>(
	options: ThreadLoopRuntimeOptions,
	operation: RuntimeContextLoadOperation,
	load: () => Promise<T>,
): Promise<T> {
	const startedAt = options.runtime.monotonicMs();
	try {
		const result = await load();
		runtimeMetrics(options).observeContextLoadLatency(
			operation,
			options.runtime.monotonicMs() - startedAt,
			"success",
		);
		return result;
	} catch (error) {
		runtimeMetrics(options).observeContextLoadLatency(
			operation,
			options.runtime.monotonicMs() - startedAt,
			"error",
		);
		throw error;
	}
}

async function observeEventWriter<Result extends { readonly ok: boolean }>(
	options: ThreadLoopRuntimeOptions,
	operation: RuntimeEventWriteOperation,
	write: () => Promise<Result>,
): Promise<Result> {
	const startedAt = options.runtime.monotonicMs();
	try {
		const result = await write();
		runtimeMetrics(options).observeEventWriteLatency(
			operation,
			options.runtime.monotonicMs() - startedAt,
			result.ok ? "success" : "error",
		);
		return result;
	} catch (error) {
		runtimeMetrics(options).observeEventWriteLatency(
			operation,
			options.runtime.monotonicMs() - startedAt,
			"error",
		);
		throw error;
	}
}

function runtimeMetrics(options: ThreadLoopRuntimeOptions): RuntimeMetricsSink {
	return options.metrics ?? NoopRuntimeMetricsSink;
}

type CompactionDecisionResult =
	| { readonly type: "skipped" }
	| {
			readonly type: "applied";
			readonly currentModel: RuntimeModelRef;
			readonly projectedContext: readonly GatewayProviderContextEntry[];
			readonly contextAnchorSequence: number;
	  }
	| { readonly type: "interrupted"; readonly discardHotState?: true }
	| {
			readonly type: "failed";
			readonly result: Extract<
				ThreadLoopRunResult,
				{ readonly type: "failed" }
			>;
	  };

type CompactionAttemptResult =
	| CompactionDecisionResult
	| { readonly type: "post_start_failed"; readonly failure: RuntimeFailure }
	| { readonly type: "rescheduled"; readonly effectiveDeadline: string };

function coordinateCompactionBeforeProviderRequestEffect(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	custody: ThreadLoopRunCustody,
	committedContext: readonly RuntimeContextEntry[],
	turnRetryCounters: { providerAttempts: number; compactionAttempts: number },
	setRetryWaitPending: (pending: boolean) => void,
): Effect.Effect<CompactionDecisionResult, unknown> {
	const compaction = options.compaction;
	if (compaction === undefined) {
		return Effect.succeed({ type: "skipped" });
	}
	const lastUsage = session.state.lastRequestUsage();
	const limits = session.state.lastRequestModelLimits();
	const anchorSequence = session.state.lastRequestContextAnchorSequence();
	if (
		lastUsage === undefined ||
		limits === undefined ||
		anchorSequence === undefined
	) {
		return Effect.succeed({ type: "skipped" });
	}
	const committedDelta = committedContext.filter(
		(entry) => entry.messageSequence > anchorSequence,
	);
	if (
		runtimeUsageTokenTotal(lastUsage) +
			estimatedRuntimeContextTokens(committedDelta) <
		usableModelInputTokens(limits, compaction.reservedInputTokens)
	) {
		return Effect.succeed({ type: "skipped" });
	}
	return runCompactionSummaryEffect(
		session,
		options,
		custody,
		committedContext,
		compaction,
		turnRetryCounters,
		setRetryWaitPending,
	);
}

function runCompactionSummaryEffect(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	custody: ThreadLoopRunCustody,
	messages: readonly RuntimeContextEntry[],
	compaction: ThreadLoopCompactionOptions,
	turnRetryCounters: { providerAttempts: number; compactionAttempts: number },
	setRetryWaitPending: (pending: boolean) => void,
): Effect.Effect<CompactionDecisionResult, unknown> {
	return Effect.gen(function* () {
		let retryWaitPending = false;
		while (true) {
			if (
				session.state.runtimeShutdownRequested() ||
				session.state.userInterruptRequested()
			) {
				if (session.state.userInterruptRequested()) {
					session.state.markUserInterruptCloseoutEligible();
				}
				return { type: "interrupted" };
			}
			if (retryWaitPending) {
				setRetryWaitPending(false);
				retryWaitPending = false;
			}
			const currentModel = session.state.currentModel();
			if (currentModel === undefined) {
				return yield* Effect.promise(() =>
					failedCompactionResult(
						session,
						options,
						normalizeRuntimeFailure({
							type: "runtime",
							code: "runtime_invalid_sequence",
							retryable: false,
							fatal: true,
							reason: "runtime_contract_validation",
							sessionId: session.sessionId,
						}),
					),
				);
			}
			const result = yield* runCompactionSummaryAttemptEffect(
				session,
				options,
				custody,
				currentModel,
				messages,
				compaction,
				turnRetryCounters,
			);
			if (result.type === "rescheduled") {
				setRetryWaitPending(true);
				retryWaitPending = true;
				const waited = yield* waitForProviderRequestRescheduleEffect(
					session,
					options,
					result.effectiveDeadline,
				);
				if (waited.type !== "deadline") {
					setRetryWaitPending(false);
					if (waited.type === "user_interrupt") {
						session.state.markUserInterruptCloseoutEligible();
					}
					return { type: "interrupted" };
				}
				continue;
			}
			if (result.type !== "post_start_failed") {
				return result;
			}
			return yield* Effect.promise(() =>
				failedCompactionResult(session, options, result.failure),
			);
		}
	});
}

function runCompactionSummaryAttemptEffect(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	custody: ThreadLoopRunCustody,
	currentModel: RuntimeModelRef,
	messages: readonly RuntimeContextEntry[],
	compaction: ThreadLoopCompactionOptions,
	turnRetryCounters: { providerAttempts: number; compactionAttempts: number },
): Effect.Effect<CompactionAttemptResult, unknown> {
	return Effect.gen(function* () {
		if (options.sessionEventWriter.writeRequestEnd === undefined) {
			return yield* Effect.promise(() =>
				failedCompactionResult(
					session,
					options,
					compactionFailure(
						session,
						currentModel,
						"runtime_invalid_sequence",
						"runtime_contract_validation",
						"compaction requires WriteRequestEnd transport",
					),
				),
			);
		}
		const prefix = session.state.contextManager.threadContextPrefix();
		const compactableMessages = [...(prefix?.entries ?? []), ...messages];
		const selected = selectCompactionContext(compactableMessages);
		if (
			selected === undefined ||
			(selected.head.length === 0 && selected.previousSummary === undefined)
		) {
			return yield* Effect.promise(() =>
				failedCompactionResult(
					session,
					options,
					compactionFailure(
						session,
						currentModel,
						"runtime_invalid_sequence",
						"runtime_contract_validation",
						"session context has no compactable history",
					),
				),
			);
		}
		const prompt = buildCompactionPrompt({
			...(selected.previousSummary === undefined
				? {}
				: { previousSummary: selected.previousSummary }),
			context: [
				...(selected.previousRecent === undefined
					? []
					: [selected.previousRecent]),
				...(selected.head.length === 0 ? [] : [selected.head]),
			],
		});
		const originatingOutputTokens =
			options.providerCallRuntime?.maxOutputTokens;
		const summaryOutputTokens =
			originatingOutputTokens === undefined
				? CompactionSummaryOutputTokens
				: Math.min(originatingOutputTokens, CompactionSummaryOutputTokens);
		const limits = session.state.lastRequestModelLimits();
		if (
			limits !== undefined &&
			Math.round(prompt.length / 4) >
				limits.contextWindowTokens - summaryOutputTokens
		) {
			return yield* Effect.promise(() =>
				failedCompactionResult(
					session,
					options,
					compactionContextLimitFailure(session, currentModel),
				),
			);
		}
		let promptMessage: RuntimeContextEntry;
		try {
			promptMessage = compactionPromptMessage(
				session,
				options,
				messages,
				prompt,
			);
		} catch {
			return yield* Effect.promise(() =>
				failedCompactionResult(
					session,
					options,
					compactionFailure(
						session,
						currentModel,
						"runtime_invalid_sequence",
						"runtime_contract_validation",
						"compaction prompt projection failed",
					),
				),
			);
		}
		const projectedPrompt = toGatewayProviderContext([promptMessage]);
		if (!projectedPrompt.ok) {
			return yield* Effect.promise(() =>
				failedCompactionResult(
					session,
					options,
					compactionFailure(
						session,
						currentModel,
						"runtime_invalid_sequence",
						"runtime_contract_validation",
						"compaction prompt projection failed",
					),
				),
			);
		}
		const assembled = yield* Effect.promise(() =>
			assembleCompactionLLMRequest(
				session,
				options,
				currentModel,
				projectedPrompt.context,
				compaction,
				summaryOutputTokens,
			),
		);
		if (!assembled.ok) {
			return yield* Effect.promise(() =>
				failedCompactionResult(session, options, assembled.error),
			);
		}
		if (session.state.cooperativeCancelRequested()) {
			return { type: "interrupted" };
		}
		if (
			session.state.runtimeShutdownRequested() ||
			session.state.userInterruptRequested()
		) {
			if (session.state.userInterruptRequested()) {
				session.state.markUserInterruptCloseoutEligible();
			}
			return { type: "interrupted" };
		}
		return yield* runOwnedCompactionSummaryAttemptEffect(
			session,
			options,
			custody,
			currentModel,
			messages,
			prefix,
			assembled.request,
			selected.recent,
			turnRetryCounters,
		);
	});
}

function runOwnedCompactionSummaryAttemptEffect(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	custody: ThreadLoopRunCustody,
	currentModel: RuntimeModelRef,
	messages: readonly RuntimeContextEntry[],
	prefix: ThreadContextPrefix | undefined,
	request: LLMRequest,
	recentContext: string,
	turnRetryCounters: { providerAttempts: number; compactionAttempts: number },
): Effect.Effect<CompactionAttemptResult, unknown> {
	return Effect.gen(function* () {
		const compactionScope = yield* Scope.make();
		// Keep the start ACK and its terminal closeout under one owner; only provider work is restored as interruptible.
		return yield* Effect.uninterruptibleMask((restore) =>
			Effect.gen(function* () {
				if (
					session.state.runtimeShutdownRequested() ||
					session.state.userInterruptRequested()
				) {
					if (session.state.userInterruptRequested()) {
						session.state.markUserInterruptCloseoutEligible();
					}
					return { type: "interrupted" } as const;
				}
				const contextThroughMessageSequence =
					compactionBoundaryMessageSequence(messages);
				const compactedContextSequences = new Set(
					messages
						.filter(
							(entry) => entry.messageSequence <= contextThroughMessageSequence,
						)
						.map((entry) => entry.messageSequence),
				);
				const consumedInputContextSequences = session.state
					.threadTurnTransition()
					.checkpoint.pendingInputContextSequences.filter((sequence) =>
						compactedContextSequences.has(sequence),
					);
				const requestStartOwner = session.state.threadTurnTransition();
				const startAppend = yield* Effect.promise(() =>
					appendRetriedEvent(
						options,
						session,
						{
							type: "span.model_request_start",
							model_request_id: request.modelRequestId,
						},
						undefined,
						undefined,
						{
							contextThroughMessageSequence,
							requestKind: "compaction_summary",
						},
					),
				);
				if (!startAppend.ok) {
					return {
						type: "failed",
						result: {
							type: "failed",
							error: runtimeFailureFromEventWriter(startAppend.error),
							releaseSession: { reason: "event_write_failed" },
						},
					} as const;
				}
				if (startAppend.type === "stale") {
					session.state.clearAfterCustodyHandoff();
					return { type: "interrupted", discardHotState: true } as const;
				}
				const requestStartReduction = session.state.applyRequestStartFact(
					requestStartOwner,
					{
					fact: "request_started",
					eventId: startAppend.eventId,
					modelRequestId: request.modelRequestId,
					requestKind: "compaction_summary",
					contextThroughMessageSequence,
					consumedInputContextSequences,
					},
				);
				const requestStartDispatch = requestStartReduction.dispatch;
				if (
					requestStartDispatch?.dispatch !== "start_provider_request" ||
					requestStartDispatch.modelRequestId !== request.modelRequestId
				) {
					return {
						type: "failed",
						result: {
							type: "failed",
							error: compactionFailure(
								session,
								currentModel,
								"runtime_invalid_sequence",
								"runtime_contract_validation",
								"Request Start result did not authorize compaction dispatch",
							),
							releaseSession: { reason: "event_write_failed" },
						},
					} as const;
				}
				const providerAbortController = new AbortController();
				const abortProvider = (): void => providerAbortController.abort();
				const streamState: CompactionStreamState = {
					summaryText: [],
					usage: undefined,
					finishReason: undefined,
					terminalProviderEventSeen: false,
					failure: undefined,
				};
				if (session.state.runtimeShutdownRequested()) {
					return { type: "interrupted" } as const;
				}
				if (session.state.userInterruptRequested()) {
					return yield* closeStartedCompactionForUserInterruptEffect(
						session,
						options,
						custody,
						request,
						startAppend.eventId,
						undefined,
					);
				}
				const streamStartedAt = options.runtime.monotonicMs();
				if (session.state.runtimeShutdownRequested()) {
					return { type: "interrupted" } as const;
				}
				if (session.state.userInterruptRequested()) {
					return yield* closeStartedCompactionForUserInterruptEffect(
						session,
						options,
						custody,
						request,
						startAppend.eventId,
						undefined,
					);
				}
				const providerStream = Effect.suspend(() =>
							options.llmService
								.stream(request, {
									abortSignal: providerAbortController.signal,
								})
								.pipe(
									Stream.runForEach((event) =>
										Effect.sync(() =>
											consumeCompactionStreamEvent(
												session,
												currentModel,
												streamState,
												event,
											),
										),
									),
									Effect.as({ type: "completed" as const }),
								),
						);
				const providerLifecycle = yield* runCompactionStreamLifecycle(
					restore,
					providerStream,
					compactionScope,
					abortProvider,
				);
				const { interruptProvider, streamExit } = providerLifecycle;
				recordProviderStreamDuration(
					options,
					"compaction_summary",
					streamStartedAt,
					providerStreamMetricOutcome(
						streamExit,
						session.state.runtimeShutdownRequested(),
					),
				);
				if (
					session.state.runtimeShutdownRequested() ||
					(Exit.isFailure(streamExit) &&
						Cause.hasInterruptsOnly(streamExit.cause))
				) {
					yield* interruptProvider;
					if (
						session.state.runtimeShutdownRequested() ||
						!session.state.userInterruptRequested()
					) {
						return { type: "interrupted" } as const;
					}
					const interruptCommand = session.state.userInterruptCommand();
					if (interruptCommand === undefined) {
						return yield* failRequestCloseout(
							normalizeRuntimeFailure({
								type: "runtime",
								code: "runtime_invalid_sequence",
								retryable: false,
								fatal: true,
								reason: "runtime_contract_validation",
								sessionId: session.sessionId,
							}),
						);
					}
					const interruptLeaseRef = custody.interruptLeaseRef(
						interruptCommand.runtimeInputId,
					);
					if (interruptLeaseRef === undefined) {
						return yield* failRequestCloseout(
							normalizeRuntimeFailure({
								type: "runtime",
								code: "runtime_invalid_sequence",
								retryable: false,
								fatal: true,
								reason: "runtime_contract_validation",
								sessionId: session.sessionId,
							}),
						);
					}
						const endResult = yield* Effect.promise(() =>
							appendModelRequestEndEvent(
								options,
							session,
							request.modelRequestId,
							startAppend.eventId,
							true,
							"runtime_interrupted",
							"cancelled",
							streamState.usage,
							[],
							"compaction_summary",
							undefined,
							undefined,
							undefined,
							{
								command: interruptCommand,
								interruptLeaseRef,
							},
							),
						);
						const end =
							endResult.ok && !applyJoinedInterruptRequestEnd(session, endResult)
								? {
										ok: false as const,
										error: joinedInterruptRequestEndFailure(session),
									}
								: endResult;
					if (!end.ok) {
						return yield* failRequestCloseout(end.error);
					}
					session.state.markUserInterruptCloseoutEligible();
					return { type: "interrupted" } as const;
				}
				const streamFailure = Exit.isFailure(streamExit)
					? runtimeFailureFromLlmService(streamExit.cause)
					: undefined;
				const rawFailure =
					streamFailure ??
					streamState.failure ??
					(!streamState.terminalProviderEventSeen
						? compactionFailure(
								session,
								currentModel,
								"gateway_stream_error",
								"runtime_contract_validation",
								"compaction provider stream ended without terminal event",
							)
						: undefined);
				const failure =
					rawFailure !== undefined && isContextOverflowFailure(rawFailure)
						? compactionContextLimitFailure(session, currentModel)
						: rawFailure;
				if (failure !== undefined) {
					return yield* Effect.promise(() =>
						closeCompactionFailure(
							session,
							options,
							request,
							startAppend.eventId,
							streamState.usage,
							failure,
							turnRetryCounters,
						),
					);
				}
				const summary = streamState.summaryText.join("");
				if (summary.trim().length === 0) {
					const failure = compactionFailure(
						session,
						currentModel,
						"runtime_invalid_sequence",
						"runtime_contract_validation",
						"compaction provider returned an empty summary",
					);
					return yield* Effect.promise(() =>
						closeCompactionFailure(
							session,
							options,
							request,
							startAppend.eventId,
							streamState.usage,
							failure,
							turnRetryCounters,
						),
					);
				}
				const checkpointDraft = compactionContext(
					mintCompactionCheckpoint(summary, recentContext),
				);
				const compactionBoundarySequence =
					compactionBoundaryMessageSequence(messages);
				const prefixConsumption =
					prefix === undefined
						? undefined
						: {
								childThreadId: prefix.childThreadId,
								parentBoundaryEventId: prefix.parentBoundaryEventId,
							};
				const end = yield* Effect.promise(() =>
					appendModelRequestEndEvent(
						options,
						session,
						request.modelRequestId,
						startAppend.eventId,
						false,
						undefined,
						streamState.finishReason ?? "unknown",
						streamState.usage,
						[],
						"compaction_summary",
						undefined,
						undefined,
						{
							context: checkpointDraft,
							compactedThroughMessageSequence: compactionBoundarySequence,
							compactionEventPayloadJson: JSON.stringify({
								type: "agent.thread_context_compacted",
								summary,
								recent_context: recentContext,
							}),
							...(prefixConsumption === undefined ? {} : { prefixConsumption }),
						},
					),
				);
				if (!end.ok) {
					return {
						type: "failed",
						result: {
							type: "failed",
							error: end.error,
							releaseSession: { reason: "event_write_failed" },
						},
					} as const;
				}
				if (end.type === "stale") {
					session.state.clearAfterCustodyHandoff();
					return { type: "interrupted", discardHotState: true } as const;
				}
				if (end.outcome.type !== "compacted") {
					session.state.clear();
					return {
						type: "failed",
						result: {
							type: "failed",
							error: normalizeRuntimeFailure({
								type: "runtime",
								code: "runtime_invalid_sequence",
								retryable: true,
								fatal: false,
								reason: "runtime_contract_validation",
								sessionId: session.sessionId,
							}),
							releaseSession: { reason: "event_write_failed" },
						},
					} as const;
				}
				const checkpoint = applyAcceptedInputResult(
					[checkpointDraft],
					[end.outcome.checkpointMessageSequence],
				)[0]!;
				const compactedMessages =
					session.state.contextManager.replaceEntriesThroughSequence(
						compactionBoundarySequence,
						[checkpoint],
					);
				session.state.applyThreadTurnFact({
					fact: "inputs_committed",
					eventId: end.outcome.compactionEventId,
					contextSequences: [checkpoint.messageSequence],
				});
				if (prefixConsumption !== undefined) {
					session.state.contextManager.installThreadContextPrefix(undefined);
				}
				session.state.clearLastRequestUsage();
				const projectedCheckpoint = toGatewayProviderContext(compactedMessages);
				if (
					!projectedCheckpoint.ok ||
					projectedCheckpoint.context.length === 0
				) {
					return yield* Effect.promise(() =>
						failedCompactionResult(
							session,
							options,
							compactionFailure(
								session,
								currentModel,
								"runtime_invalid_sequence",
								"runtime_contract_validation",
								"compaction checkpoint projection failed",
							),
						),
					);
				}
				return {
					type: "applied",
					currentModel,
					projectedContext: projectedCheckpoint.context,
					contextAnchorSequence: highestMessageSequence(compactedMessages),
				} as const;
			}),
		).pipe(
			Effect.ensuring(
				Scope.close(compactionScope, Exit.void).pipe(
					Effect.timeoutOption(`${ProviderRequestScopeCloseTimeoutMs} millis`),
					Effect.asVoid,
				),
			),
		);
	});
}

function closeStartedCompactionForUserInterruptEffect(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	custody: ThreadLoopRunCustody,
	request: LLMRequest,
	modelRequestStartId: string,
	usage: RuntimeUsage | undefined,
): Effect.Effect<CompactionAttemptResult, never> {
	return Effect.gen(function* () {
		const command = session.state.userInterruptCommand();
		if (command === undefined) {
			return yield* failRequestCloseout(
				normalizeRuntimeFailure({
					type: "runtime",
					code: "runtime_invalid_sequence",
					retryable: false,
					fatal: true,
					reason: "runtime_contract_validation",
					sessionId: session.sessionId,
				}),
			);
		}
		const interruptLeaseRef = custody.interruptLeaseRef(command.runtimeInputId);
		if (interruptLeaseRef === undefined) {
			return yield* failRequestCloseout(
				normalizeRuntimeFailure({
					type: "runtime",
					code: "runtime_invalid_sequence",
					retryable: false,
					fatal: true,
					reason: "runtime_contract_validation",
					sessionId: session.sessionId,
				}),
			);
		}
		const endResult = yield* Effect.promise(() =>
			appendModelRequestEndEvent(
				options,
				session,
				request.modelRequestId,
				modelRequestStartId,
				true,
				"runtime_interrupted",
				"cancelled",
				usage,
				[],
				"compaction_summary",
				undefined,
				undefined,
				undefined,
				{
					command,
					interruptLeaseRef,
				},
			),
		);
		const end =
			endResult.ok && !applyJoinedInterruptRequestEnd(session, endResult)
				? {
						ok: false as const,
						error: joinedInterruptRequestEndFailure(session),
					}
				: endResult;
		if (!end.ok) {
			return yield* failRequestCloseout(end.error);
		}
		session.state.markUserInterruptCloseoutEligible();
		return { type: "interrupted" };
	});
}

async function failedCompactionResult(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	failure: RuntimeFailure,
): Promise<CompactionDecisionResult> {
	const terminalAppend = await appendTerminalEventsBestEffort(
		options,
		session,
		failure,
	);
	if (!terminalAppend.ok) {
		return {
			type: "failed",
			result: {
				type: "failed",
				error: terminalAppend.error,
				releaseSession: { reason: "event_write_failed" },
			},
		};
	}
	return {
		type: "failed",
		result: {
			type: "failed",
			error: terminalAppend.settledFailure,
			...(terminalAppend.failureEventId === undefined
				? {}
				: { failureEventId: terminalAppend.failureEventId }),
		},
	};
}

async function closeCompactionFailure(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	request: LLMRequest,
	modelRequestStartId: string,
	usage: RuntimeUsage | undefined,
	failure: RuntimeFailure,
	counters: { providerAttempts: number; compactionAttempts: number },
): Promise<CompactionAttemptResult> {
	const plan = providerRequestReschedulePlan(
		session,
		options,
		counters,
		"compaction",
		failure,
	);
	const end = await appendModelRequestEndEvent(
		options,
		session,
		request.modelRequestId,
		modelRequestStartId,
		true,
		requestErrorKindFromFailure(failure),
		"error",
		usage,
		[],
		"compaction_summary",
		plan.type === "proposed"
			? {
					...plan.reschedule,
					providerAttempts: counters.providerAttempts,
					compactionAttempts: plan.reschedule.attempt,
				}
			: undefined,
	);
	if (!end.ok) {
		return {
			type: "failed",
			result: {
				type: "failed",
				error: end.error,
				releaseSession: { reason: "event_write_failed" },
			},
		};
	}
	if (plan.type === "proposed") {
		if (end.type !== "stale" && end.outcome.type === "rescheduled") {
			counters.compactionAttempts = plan.reschedule.attempt;
			return {
				type: "rescheduled",
				effectiveDeadline: end.outcome.effectiveDeadline,
			};
		}
		return {
			type: "post_start_failed",
			failure: compactionFailureWithRetryStatus(failure, { type: "exhausted" }),
		};
	}
	return {
		type: "post_start_failed",
		failure: plan.type === "exhausted" ? plan.failure : failure,
	};
}

function compactionFailureWithRetryStatus(
	failure: RuntimeFailure,
	retryStatus: NonNullable<RuntimeFailure["retryStatus"]>,
): RuntimeFailure {
	return {
		...failure,
		retryStatus,
	};
}

function coordinateProviderTurnEffect(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	custody: ThreadLoopRunCustody,
	request: LLMRequest,
	carriedAttachments: readonly RuntimeProviderAttachment[],
	turnRetryCounters: { providerAttempts: number; compactionAttempts: number },
	rejectedAttachments: RejectedProviderAttachment[],
	allowContextOverflowRecovery: boolean,
	requestContextAnchorSequence: number,
	executionSnapshot: RequestExecutionSnapshot,
	executionPolicy: Readonly<ThreadLoopRuntimePolicy>,
): Effect.Effect<ProviderTurnResult, unknown> {
	const durableOperations = new HotDurableOperationOwner();
	const providerRequestWriteController = new AbortController();
	const processorOperationWriteController = new AbortController();
	let settlementWriteController: AbortController | undefined;
	let releaseSettlementRawOperationOwner: (() => void) | undefined;
	const releaseProcessorRawOperationOwner = ownRuntimeDeclarationRawOperations(
		processorOperationWriteController.signal,
		() => durableOperations.begin(true),
	);
	const abortProviderRequestWrites = (): void => {
		durableOperations.fence();
		providerRequestWriteController.abort();
	};
	return Effect.gen(function* () {
		if (session.state.runtimeShutdownRequested()) {
			return providerTurnInterrupted();
		}
		if (session.state.userInterruptRequested()) {
			return yield* settleUnstartedUserInterruptEffect(session, options);
		}
		if (session.state.cooperativeCancelRequested()) {
			return providerTurnInterrupted();
		}
		const source = {
			providerId: request.model?.providerId ?? "",
			modelId: request.model?.modelId ?? "",
		};
		if (session.state.runtimeShutdownRequested()) {
			return providerTurnInterrupted();
		}
		if (session.state.userInterruptRequested()) {
			return yield* settleUnstartedUserInterruptEffect(session, options);
		}
		if (session.state.cooperativeCancelRequested()) {
			return providerTurnInterrupted();
		}
		const processorWriteSignal = (): AbortSignal =>
			settlementWriteController?.signal ??
			processorOperationWriteController.signal;
		const beginSettlementWrites = (): void => {
			releaseSettlementRawOperationOwner?.();
			settlementWriteController?.abort();
			settlementWriteController = new AbortController();
			releaseSettlementRawOperationOwner = ownRuntimeDeclarationRawOperations(
				settlementWriteController.signal,
				() => durableOperations.begin(true),
			);
		};
		const endSettlementWrites = (): void => {
			releaseSettlementRawOperationOwner?.();
			releaseSettlementRawOperationOwner = undefined;
			settlementWriteController?.abort();
			settlementWriteController = undefined;
		};
		let processor: ProviderStreamAccumulator;
		try {
			processor = (
				options.createProcessor ??
				((processorOptions: ProviderStreamAccumulatorOptions) =>
					new ProviderStreamAccumulator(processorOptions))
			)({
				modelRequestId: request.modelRequestId,
				requestId: request.requestId,
				workspaceId: session.identity.workspaceId,
				sessionId: session.sessionId,
				sessionThreadId: session.identity.sessionThreadId,
				bindingId: session.identity.bindingId,
				bindingGeneration: session.identity.bindingGeneration,
				targetPodUid: session.identity.targetPodUid,
				contextOwner: session.state.contextManager,
				...(options.maxNormalizedTextPreviewBytes !== undefined
					? {
							maxNormalizedTextPreviewBytes:
								options.maxNormalizedTextPreviewBytes,
						}
					: {}),
				writer: {
					appendEvent: async (event, _source, declaration, modelRequestId) =>
						await appendProcessorEvent(
							options,
							session,
							event,
							declaration,
							modelRequestId,
						),
					settleToolResult: async (envelope) =>
						await options.sessionEventWriter.settleToolResult(envelope),
					commitInternalToolRepair: async (repair, envelope) =>
						await commitInternalToolRepairStable(
							session,
							options,
							repair,
							envelope,
							processorWriteSignal(),
						),
				},
				onInternalToolRepairCommitted: (fact) => {
					commitProcessorProjection(session, processor);
					session.state.applyThreadTurnFact({
						fact: "internal_tool_repair_committed",
						...fact,
					});
				},
				onToolResultCommitted: (fact) => {
					commitProcessorProjection(session, processor);
					session.state.applyThreadTurnFact({
						fact: "tool_result_committed",
						...fact,
					});
					session.state.clearThreadToolRoute(fact.toolUseEventId);
				},
			});
		} catch (error) {
			const failure = normalizeRuntimeFailure({
				type: "runtime",
				code: "runtime_invalid_sequence",
				rawError: error,
				retryable: false,
				fatal: true,
				reason: "runtime_contract_validation",
				sessionId: session.sessionId,
			});
			session.state.clear();
			const terminalAppend = yield* Effect.promise(() =>
				appendTerminalEventsBestEffort(options, session, failure),
			);
			if (!terminalAppend.ok) {
				return providerTurnFailed(terminalAppend.error, "event_write_failed");
			}
			return providerTurnFailed(
				terminalAppend.settledFailure,
				"crashed",
				terminalAppend.failureEventId,
			);
		}

		if (session.state.cooperativeCancelRequested()) {
			return providerTurnInterrupted();
		}
		if (
			session.state.runtimeShutdownRequested() ||
			session.state.userInterruptRequested()
		) {
			return yield* settleRuntimeShutdownEffect(
				session,
				options,
				custody,
				processor,
				source,
				undefined,
				undefined,
				[],
				undefined,
				durableOperations,
			);
		}
		if (options.sessionEventWriter.writeRequestEnd === undefined) {
			return providerTurnFailed(
				runtimeFailureFromEventWriter(
					normalizeSessionEventWriterError({
						code: "unavailable",
						sessionId: session.sessionId,
						writeId: options.runtime.createId("event_write"),
					}),
				),
				"event_write_failed",
			);
		}
		if (
			session.state.runtimeShutdownRequested() ||
			session.state.userInterruptRequested()
		) {
			return yield* settleRuntimeShutdownEffect(
				session,
				options,
				custody,
				processor,
				source,
				undefined,
				undefined,
				[],
				undefined,
				durableOperations,
			);
		}
		const requestStartOwner = session.state.threadTurnTransition();
		const spanStartAppend = yield* nonAbandonablePromise(() =>
			appendRetriedEvent(
				options,
				session,
				{
					type: "span.model_request_start",
					model_request_id: request.modelRequestId,
				},
				undefined,
				undefined,
				{
					contextThroughMessageSequence: requestContextAnchorSequence,
					requestKind: runtimeProviderStreamKindFromRequest(request),
					consumedFileAttachments: request.attachments.flatMap((attachment) =>
						attachment.fileBacked === undefined
							? []
							: [
									{
										sourceEventId: attachment.fileBacked.sourceEventId,
										fileId: attachment.fileBacked.fileId,
									},
								],
					),
				},
			),
		);
		if (!spanStartAppend.ok) {
			return providerTurnFailed(
				runtimeFailureFromEventWriter(spanStartAppend.error),
				"event_write_failed",
			);
		}
		if (spanStartAppend.type === "stale") {
			session.state.clearAfterCustodyHandoff();
			return providerTurnInterruptedWithDiscard();
		}
		const requestStartReduction = session.state.applyRequestStartFact(
			requestStartOwner,
			{
				fact: "request_started",
				eventId: spanStartAppend.eventId,
				modelRequestId: request.modelRequestId,
				requestKind: runtimeProviderStreamKindFromRequest(request),
				contextThroughMessageSequence: requestContextAnchorSequence,
				consumedInputContextSequences:
					session.state.threadTurnTransition().checkpoint
						.pendingInputContextSequences,
			},
		);
		const requestStartDispatch = requestStartReduction.dispatch;
		if (
			requestStartDispatch?.dispatch !== "start_provider_request" ||
			requestStartDispatch.modelRequestId !== request.modelRequestId
		) {
			return providerTurnFailed(
				pendingApprovalResumeFailure(
					session.sessionId,
					source,
					"Request Start result did not authorize provider dispatch",
				),
				"event_write_failed",
			);
		}
		session.state.consumeFileBackedAttachmentRide();
		if (
			session.state.runtimeShutdownRequested() ||
			session.state.userInterruptRequested() ||
			session.state.cooperativeCancelRequested()
		) {
			return session.state.cooperativeCancelRequested()
				? yield* settleCooperativeCancellationEffect(
						session,
						options,
						processor,
						source,
						spanStartAppend.eventId,
						request.modelRequestId,
						[],
						undefined,
						beginSettlementWrites,
						endSettlementWrites,
						durableOperations,
					)
				: yield* settleRuntimeShutdownEffect(
						session,
						options,
						custody,
						processor,
						source,
						spanStartAppend.eventId,
						request.modelRequestId,
						[],
						undefined,
						durableOperations,
					);
		}

		const providerAbortController = new AbortController();
		const providerRequestScope = yield* Scope.make();
		const streamState: ProviderTurnStreamState = {
			executionSnapshot,
			executionPolicy,
			durableOperations,
			modelUsage: undefined,
			modelLimits: undefined,
			modelFinishReason: undefined,
			terminalProviderEventSeen: false,
			waitingToolUseEventIds: [],
			toolFibers: [],
			assistantProjectionGate: Semaphore.makeUnsafe(1),
			providerRequestScope,
			toolScheduler: new ToolScheduler(),
			toolEntries: Object.create(null) as Record<string, ToolEntry | undefined>,
			toolProviderInputs: Object.create(null) as Record<
				string,
				RuntimeJsonValue | undefined
			>,
			toolDeclarationBarriers: Object.create(null) as Record<
				string,
				Deferred.Deferred<boolean> | undefined
			>,
			allToolDeclarationBarriers: [],
			nextToolModelOrder: 0,
			rejectedAttachments,
			carriedAttachments,
		};

		return yield* Effect.uninterruptibleMask((restore) =>
			Effect.gen(function* () {
				if (
					session.state.runtimeShutdownRequested() ||
					session.state.userInterruptRequested() ||
					session.state.cooperativeCancelRequested()
				) {
					return session.state.cooperativeCancelRequested()
						? yield* settleCooperativeCancellationEffect(
								session,
								options,
								processor,
								source,
								spanStartAppend.eventId,
								request.modelRequestId,
								[],
								undefined,
								beginSettlementWrites,
								endSettlementWrites,
								durableOperations,
								streamState,
							)
						: yield* settleRuntimeShutdownEffect(
								session,
								options,
								custody,
								processor,
								source,
								spanStartAppend.eventId,
								request.modelRequestId,
								[],
								undefined,
								durableOperations,
								streamState,
							);
				}
				const providerStream = Effect.suspend(() =>
							options.llmService
								.stream(request, {
									abortSignal: providerAbortController.signal,
								})
								.pipe(
									Stream.runForEach((event) => {
										const processing = processProviderEventEffect(
											session,
											options,
											processor,
											source,
											request,
											spanStartAppend.eventId,
											streamState,
											event,
											turnRetryCounters,
											allowContextOverflowRecovery,
										);
										return (
											event.type === "provider-error"
												? processing
												: ownHotDurableEffect(durableOperations, processing)
										).pipe(Effect.uninterruptible);
									}),
									Effect.as({ type: "completed" as const }),
								),
						);
				const streamStartedAt = options.runtime.monotonicMs();
				const providerLifecycle = yield* runProviderStreamLifecycle(
					restore,
					providerStream,
					providerRequestScope,
					() => providerAbortController.abort(),
				);
				const { interruptProvider, streamExit } = providerLifecycle;
				const requestKind = runtimeProviderStreamKindFromRequest(request);
				const requestEndKind = requestEndKindFromRequest(request);
				recordProviderStreamDuration(
					options,
					requestKind,
					streamStartedAt,
					providerStreamMetricOutcome(
						streamExit,
						session.state.runtimeShutdownRequested(),
					),
				);
				if (Exit.isSuccess(streamExit)) {
					if (
						streamExit.value.type !== "completed" ||
						session.state.runtimeShutdownRequested() ||
						session.state.userInterruptRequested() ||
						session.state.cooperativeCancelRequested()
					) {
						yield* interruptProvider;
						return session.state.cooperativeCancelRequested()
							? yield* settleCooperativeCancellationEffect(
									session,
									options,
									processor,
									source,
									spanStartAppend.eventId,
									request.modelRequestId,
									streamState.carriedAttachments,
									streamState.modelUsage,
									beginSettlementWrites,
									endSettlementWrites,
									durableOperations,
									streamState,
								)
							: yield* settleRuntimeShutdownEffect(
									session,
									options,
									custody,
									processor,
									source,
									spanStartAppend.eventId,
									request.modelRequestId,
									streamState.carriedAttachments,
									streamState.modelUsage,
									durableOperations,
									streamState,
								);
					}
					if (!streamState.terminalProviderEventSeen) {
						return yield* handleProviderStreamExhaustedEffect(
							session,
							options,
							processor,
							source,
							request,
							spanStartAppend.eventId,
							streamState.modelUsage,
							turnRetryCounters,
							streamState,
						);
					}
					const declarationOutcomes = yield* Effect.forEach(
						streamState.allToolDeclarationBarriers,
						(barrier) => Deferred.await(barrier),
						{ concurrency: "unbounded" },
					);
					if (declarationOutcomes.some((declared) => !declared)) {
						const toolResult = yield* joinToolFibersEffect(
							session,
							options,
							processor,
							source,
							request.modelRequestId,
							streamState,
						);
						return (
							toolResult ??
							providerTurnFailed(
								pendingApprovalResumeFailure(
									session.sessionId,
									source,
									"provider Tool Use declaration did not reach a durable boundary",
								),
								"event_write_failed",
							)
						);
					}
					yield* Effect.promise(() => processor.awaitAssistantMembersDrained());
					const trailingAppend = processor.requestEndAppend();
					const spanEndAppend = yield* Effect.promise(() =>
						appendModelRequestEndEvent(
							options,
							session,
							request.modelRequestId,
							spanStartAppend.eventId,
							false,
							undefined,
							streamState.modelFinishReason ?? "unknown",
							streamState.modelUsage,
							attachmentConsumptionUnion(
								streamState.carriedAttachments,
								streamState.rejectedAttachments,
							),
							requestEndKind,
							undefined,
							trailingAppend,
						),
					);
					if (!spanEndAppend.ok) {
						return yield* settleToolsAfterRequestEndFailureEffect(
							session,
							processor,
							source,
							streamState,
							spanEndAppend.error,
						);
					}
					const sealApplication = applyRequestEndSeal(
						processor,
						trailingAppend,
						requestEndOutcome(spanEndAppend),
					);
					if (sealApplication.type === "stale_custody") {
						yield* interruptAndJoinToolFibersEffect(streamState);
						return requestEndCommitted(
							providerTurnInterrupted(),
							"discard_hot_state",
						);
					}
					if (sealApplication.type === "failed") {
						return yield* settleToolsAfterRequestEndFailureEffect(
							session,
							processor,
							source,
							streamState,
							sealApplication.error,
						);
					}
					// Agent and reviewer finishes arm their own thread's next proactive compaction check;
					// compaction sub-requests update this state only through checkpoint application.
					const recordsCompactionHint =
						request.requestKind ===
							ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST ||
						request.requestKind ===
							ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER;
					if (
						recordsCompactionHint &&
						streamState.modelUsage !== undefined &&
						streamState.modelLimits !== undefined
					) {
						session.state.recordLastRequestCompletion(
							streamState.modelUsage,
							streamState.modelLimits,
							requestContextAnchorSequence,
						);
					} else if (recordsCompactionHint) {
						session.state.clearLastRequestCompletion();
					}
					commitProcessorProjection(session, processor);
					if (session.state.userInterruptRequested()) {
						return yield* settleUserInterruptAfterRequestEndEffect(
							session,
							options,
							processor,
							streamState,
						);
					}
					if (session.state.cooperativeCancelRequested()) {
						return yield* settleCooperativeCancellationAfterRequestEndEffect(
							session,
							processor,
							source,
							streamState,
						);
					}
					const toolJoin = yield* restore(
						joinToolFibersEffect(
							session,
							options,
							processor,
							source,
							request.modelRequestId,
							streamState,
						).pipe(
							Effect.map((result) => ({ type: "joined" as const, result })),
							Effect.onInterrupt(() => {
								if (session.state.cooperativeCancelRequested()) {
									return settleCooperativeCancellationAfterRequestEndEffect(
										session,
										processor,
										source,
										streamState,
									).pipe(Effect.asVoid);
								}
								if (session.state.userInterruptRequested()) {
									return settleUserInterruptAfterRequestEndEffect(
										session,
										options,
										processor,
										streamState,
									).pipe(Effect.asVoid);
								}
								return interruptAndJoinToolFibersEffect(streamState);
							}),
						),
					);
					const toolFiberResult = toolJoin.result;
					if (toolFiberResult !== undefined) {
						return requestEndCommitted(
							toolFiberResult,
							toolFiberResult.type === "interrupted" &&
								toolFiberResult.attachmentRideDisposition ===
									"discard_hot_state"
								? "discard_hot_state"
								: "settled",
						);
					}
					commitProcessorProjection(session, processor);
					if (streamState.waitingToolUseEventIds.length > 0) {
						return requestEndCommitted(
							providerTurnWaitingExternal(streamState.waitingToolUseEventIds),
							"settled",
						);
					}
					return requestEndCommitted(providerTurnCompleted(), "settled");
				}

				const failure = exitFailure(streamExit);
				if (isProviderTurnShortCircuit(failure)) {
					return failure.result;
				}
				if (
					session.state.runtimeShutdownRequested() ||
					session.state.cooperativeCancelRequested() ||
					Cause.hasInterruptsOnly(streamExit.cause)
				) {
					yield* interruptProvider;
					return session.state.cooperativeCancelRequested()
						? yield* settleCooperativeCancellationEffect(
								session,
								options,
								processor,
								source,
								spanStartAppend.eventId,
								request.modelRequestId,
								streamState.carriedAttachments,
								streamState.modelUsage,
								beginSettlementWrites,
								endSettlementWrites,
								durableOperations,
								streamState,
							)
						: yield* settleRuntimeShutdownEffect(
								session,
								options,
								custody,
								processor,
								source,
								spanStartAppend.eventId,
								request.modelRequestId,
								streamState.carriedAttachments,
								streamState.modelUsage,
								durableOperations,
								streamState,
							);
				}

				const providerFailure = runtimeFailureFromLlmService(failure);
				const processed = yield* streamState.assistantProjectionGate.withPermit(
					Effect.promise(async () => {
						const result = await durableOperations.run(() =>
							processor.process({
								...source,
								event: { type: "provider-error", error: providerFailure },
							}),
						);
						if (result.ok) {
							commitProcessorProjectionWithoutStableReasoning(
								session,
								processor,
							);
						}
						return result;
					}),
				);
				if (!processed.ok) {
					return yield* Effect.promise(() =>
						closeStartedRequestAfterProcessorFailure(
							session,
							options,
							processor,
							request.modelRequestId,
							spanStartAppend.eventId,
							processed.error,
							streamState.modelUsage,
							streamState.carriedAttachments,
							requestEndKind,
						),
					);
				}
				return yield* closeProviderFailureEffect(
					session,
					options,
					processor,
					source,
					request,
					spanStartAppend.eventId,
					streamState.modelUsage,
					providerFailure,
					turnRetryCounters,
					streamState,
					allowContextOverflowRecovery,
				);
			}),
		).pipe(
			Effect.ensuring(
				Scope.close(providerRequestScope, Exit.void).pipe(
					Effect.timeoutOption(`${ProviderRequestScopeCloseTimeoutMs} millis`),
					Effect.asVoid,
				),
			),
		);
	}).pipe(
		Effect.ensuring(
			Effect.sync(abortProviderRequestWrites).pipe(
				Effect.andThen(Effect.promise(() => durableOperations.awaitIdle())),
				Effect.andThen(
					Effect.sync(() => {
						processorOperationWriteController.abort();
						releaseProcessorRawOperationOwner();
						releaseSettlementRawOperationOwner?.();
						settlementWriteController?.abort();
					}),
				),
			),
		),
	);
}

function processProviderEventEffect(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	processor: ProviderStreamAccumulator,
	source: RuntimeProcessorSource,
	request: LLMRequest,
	modelRequestStartId: string,
	state: ProviderTurnStreamState,
	event: LLMEvent,
	turnRetryCounters: { providerAttempts: number; compactionAttempts: number },
	allowContextOverflowRecovery: boolean,
): Effect.Effect<void, unknown> {
	return Effect.gen(function* () {
		if (
			(event.type === "step-finish" || event.type === "finish") &&
			event.usage !== undefined
		) {
			state.modelUsage = event.usage;
		}
		if (event.type === "finish") {
			state.modelFinishReason = event.finishReason;
			state.modelLimits = event.modelLimits;
		}
		if (event.type === "finish" || event.type === "provider-error") {
			state.terminalProviderEventSeen = true;
		}
		if (event.type === "attachment-rejections") {
			recordAttachmentRejections(
				state.carriedAttachments,
				state.rejectedAttachments,
				event.rejections,
			);
		}
		const processEvent = async (): Promise<ProviderStreamAccumulatorResult> => {
			const result = await processor.process({ ...source, event });
			if (result.ok) {
				commitProcessorProjectionWithoutStableReasoning(session, processor);
			}
			return result;
		};
		const processed = yield* Effect.promise(() =>
			event.type === "provider-error"
				? state.durableOperations.run(processEvent)
				: processEvent(),
		);
		if (!processed.ok) {
			const result = yield* Effect.promise(() =>
				closeStartedRequestAfterProcessorFailure(
					session,
					options,
					processor,
					request.modelRequestId,
					modelRequestStartId,
					processed.error,
					state.modelUsage,
					state.carriedAttachments,
					requestEndKindFromRequest(request),
				),
			);
			return yield* Effect.fail(new ProviderTurnShortCircuit(result));
		}
		if (event.type === "provider-error") {
			const result = yield* closeProviderFailureEffect(
				session,
				options,
				processor,
				source,
				request,
				modelRequestStartId,
				state.modelUsage,
				event.error,
				turnRetryCounters,
				state,
				allowContextOverflowRecovery,
			);
			return yield* Effect.fail(new ProviderTurnShortCircuit(result));
		}
		const terminalFailure = terminalFailureFromProcessorResult(processed);
		if (terminalFailure !== undefined) {
			processor.discardUncommittedMembers();
			const spanEndAppend = yield* Effect.promise(() =>
				appendModelRequestEndEvent(
					options,
					session,
					request.modelRequestId,
					modelRequestStartId,
					true,
					requestErrorKindFromFailure(terminalFailure),
					"error",
					state.modelUsage,
					state.carriedAttachments,
					requestEndKindFromRequest(request),
					undefined,
					undefined,
					undefined,
				),
			);
			if (!spanEndAppend.ok) {
				return yield* Effect.fail(
					new ProviderTurnShortCircuit(
						providerTurnFailed(spanEndAppend.error, "event_write_failed"),
					),
				);
			}
			if (spanEndAppend.type === "stale") {
				yield* interruptAndJoinToolFibersEffect(state);
				return yield* Effect.fail(
					new ProviderTurnShortCircuit(
						requestEndCommitted(providerTurnInterrupted(), "discard_hot_state"),
					),
				);
			}
			const projectionFailure = applyFailedRequestProviderProjection(session);
			if (projectionFailure !== undefined) {
				return yield* Effect.fail(
					new ProviderTurnShortCircuit(
						providerTurnFailed(projectionFailure, "event_write_failed"),
					),
				);
			}
			const terminalAppend = yield* Effect.promise(() =>
				appendTerminalEventsBestEffort(options, session, terminalFailure),
			);
			if (!terminalAppend.ok) {
				return yield* Effect.fail(
					new ProviderTurnShortCircuit(
						requestEndCommitted(
							providerTurnFailed(terminalAppend.error, "event_write_failed"),
						),
					),
				);
			}
			commitProcessorProjectionWithoutStableReasoning(session, processor);
			if (terminalFailure.type === "runtime") {
				session.state.clear();
				return yield* Effect.fail(
					new ProviderTurnShortCircuit(
						requestEndCommitted(
							providerTurnFailed(
								terminalAppend.settledFailure,
								"crashed",
								terminalAppend.failureEventId,
							),
						),
					),
				);
			}
			return yield* Effect.fail(
				new ProviderTurnShortCircuit(
					requestEndCommitted(
						providerTurnFailed(
							terminalAppend.settledFailure,
							undefined,
							terminalAppend.failureEventId,
						),
					),
				),
			);
		}
		if (event.type === "tool-call") {
			if (
				session.state.runtimeShutdownRequested() ||
				session.state.userInterruptRequested()
			) {
				return;
			}
			const declarationBarrier = yield* Deferred.make<boolean>();
			state.allToolDeclarationBarriers.push(declarationBarrier);
			const registered = registerRuntimeToolCall(
				request.modelRequestId,
				state,
				event,
			);
			if (registered.type === "invalid") {
				const settled = yield* state.assistantProjectionGate.withPermit(
					Effect.promise(async () => {
						const result = await processor.commitInternalToolRepair(
							source,
							event.id,
							request.modelRequestId,
							internalToolRepairKey(
								request.modelRequestId,
								event.id,
								event.toolName,
							),
							invalidToolCallFailure(session.sessionId, source, event.toolName),
						);
						if (result.ok) {
							commitProcessorProjectionWithoutStableReasoning(
								session,
								processor,
							);
						}
						return result;
					}),
				);
				if (!settled.ok) {
					yield* Deferred.succeed(declarationBarrier, false);
					const result = yield* Effect.promise(() =>
						closeStartedRequestAfterProcessorFailure(
							session,
							options,
							processor,
							request.modelRequestId,
							modelRequestStartId,
							settled.error,
							state.modelUsage,
							state.carriedAttachments,
							requestEndKindFromRequest(request),
						),
					);
					return yield* Effect.fail(new ProviderTurnShortCircuit(result));
				}
				yield* Deferred.succeed(declarationBarrier, true);
				return;
			}
			state.toolDeclarationBarriers[registered.jobId] = declarationBarrier;
			state.toolProviderInputs[registered.jobId] = event.input;
			const job = state.toolScheduler
				.jobs()
				.find((candidate) => candidate.id === registered.jobId);
			if (job === undefined) {
				yield* Deferred.succeed(declarationBarrier, false);
				return yield* Effect.fail(
					new ProviderTurnShortCircuit(
						providerTurnFailed(
							pendingApprovalResumeFailure(
								session.sessionId,
								source,
								"registered Tool Job is unavailable",
							),
							"event_write_failed",
						),
					),
				);
			}
			const entry = state.toolEntries[job.id];
			if (
				entry === undefined ||
				!processor.reservePublicToolUse(
					source,
					job.modelToolCallId,
					publicToolEventForEntry(entry),
					distinctProviderInputForEntry(entry, event.input),
					routeCapabilityForEntry(entry),
				)
			) {
				yield* Deferred.succeed(declarationBarrier, false);
				return yield* Effect.fail(
					new ProviderTurnShortCircuit(
						providerTurnFailed(
							pendingApprovalResumeFailure(
								session.sessionId,
								source,
								"registered Tool member could not reserve provider order",
							),
							"event_write_failed",
						),
					),
				);
			}
			const toolFiber = yield* coordinateRuntimeToolJobEffect(
				session,
				options,
				processor,
				source,
				request.modelRequestId,
				state,
				job,
			).pipe(
				Effect.ensuring(
					Deferred.succeed(declarationBarrier, false).pipe(Effect.asVoid),
				),
				Effect.forkIn(state.providerRequestScope),
			);
			state.toolFibers.push(toolFiber);
		}
	});
}

type PendingApprovalResumeResult =
	| { readonly type: "none" }
	| { readonly type: "resumed" }
	| Extract<
			ProviderTurnResult,
			{ readonly type: "waiting_external" | "interrupted" | "failed" }
	  >;

type PendingApprovalToolSettlementResult =
	| { readonly type: "settled" }
	| Extract<ProviderTurnResult, { readonly type: "interrupted" | "failed" }>;

function removeRecoveredToolRouteJob(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	pending: RuntimeRecoveredToolJobState,
): void {
	if (isResolvedToolRoute(pending)) {
		session.state.removeResolvedToolRouteJob(pending.toolUseEventId);
		return;
	}
	session.state.removePendingApprovalToolJob(pending.toolUseEventId);
	runtimeMetrics(options).addPendingApprovals(-1);
}

function resumeRecoveredToolJobsEffect(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	custody: ThreadLoopRunCustody,
): Effect.Effect<PendingApprovalResumeResult, unknown> {
	const durableOperations = new HotDurableOperationOwner();
	const settlementWriteController = new AbortController();
	const releaseSettlementRawOperationOwner = ownRuntimeDeclarationRawOperations(
		settlementWriteController.signal,
		() => durableOperations.begin(true),
	);
	let activeSettlementFibers: readonly Fiber.Fiber<
		PendingApprovalToolSettlementResult,
		never
	>[] = [];
	const fenceWrites = (): void => {
		durableOperations.fence();
		settlementWriteController.abort();
	};

	const closeForRuntimeControl = (
		activeFibers: readonly Fiber.Fiber<
			PendingApprovalToolSettlementResult,
			never
		>[],
	): Effect.Effect<PendingApprovalResumeResult, never> =>
		Effect.gen(function* () {
			yield* interruptAndAwaitFibersWithinBound(activeFibers);
			// The route bound only releases route ownership. Raw Bridge operations that
			// started before the fence remain owned until their actual ACK/rejection.
			yield* Effect.promise(() => durableOperations.awaitIdle());
			if (!session.state.userInterruptRequested()) {
				return { type: "interrupted" as const };
			}

			session.state.markUserInterruptCloseoutEligible();
			return { type: "interrupted" as const };
		});

	const run = Effect.gen(function* () {
		const pendingJobs: readonly RuntimeRecoveredToolJobState[] = [
			...session.state.pendingApprovalToolJobs(),
			...session.state.resolvedToolRouteJobs(),
			...session.state.pendingSandboxExecutionJobs(),
		];
		if (pendingJobs.length === 0) {
			return { type: "none" as const };
		}

		for (const pending of pendingJobs) {
			const sealedAssistant = session.state.contextManager.entry(
				pending.assistantMessageSequence,
			);
			const openAssistant =
				session.state.contextManager.openRequestDraft()?.messageSequence ===
				pending.assistantMessageSequence
					? session.state.contextManager.openRequestDraft()
					: undefined;
			const currentAssistant = sealedAssistant ?? openAssistant;
			if (
				currentAssistant === undefined ||
				currentAssistant.messageSequence !== pending.assistantMessageSequence ||
				!currentAssistant.parts.some(
					(part) =>
						part.type === "tool_call" &&
						part.modelToolCallId === pending.job.modelToolCallId,
				) ||
				currentAssistant.parts.some(
					(part) =>
						part.type === "tool_result" &&
						part.modelToolCallId === pending.job.modelToolCallId,
				)
			) {
				return pendingApprovalResumeFailed(
					pendingApprovalResumeFailure(
						session.sessionId,
						pending.source,
						"pending Tool settlement target is absent from the resident durable turn",
					),
				);
			}
		}
		if (
			session.state.runtimeShutdownRequested() ||
			session.state.userInterruptRequested()
		) {
			return yield* closeForRuntimeControl([]);
		}
		const unresolved = pendingJobs.filter(
			(pending) =>
				!isPendingSandboxExecution(pending) &&
				!isResolvedToolRoute(pending) &&
				session.state.toolConfirmation(pending.toolUseEventId) === undefined,
		);
		const actionableJobs = pendingJobs.filter(
			(pending) =>
				isPendingSandboxExecution(pending) ||
				isResolvedToolRoute(pending) ||
				session.state.toolConfirmation(pending.toolUseEventId) !== undefined,
		);
		if (actionableJobs.length === 0) {
			return {
				type: "waiting_external" as const,
				blockingEventIds: unresolved.map((pending) => pending.toolUseEventId),
			};
		}
		const runningAppend = yield* Effect.promise(() =>
			durableOperations.run(() =>
				appendRunningEvent(
					options,
					session,
					custody,
					"pending_tool",
					actionableJobs[0]!.toolUseEventId,
				),
			),
		);
		if (
			session.state.runtimeShutdownRequested() ||
			session.state.userInterruptRequested()
		) {
			return yield* closeForRuntimeControl([]);
		}
		if (!runningAppend.ok) {
			return pendingApprovalResumeFailed(
				runtimeFailureFromEventWriter(runningAppend.error),
				"event_write_failed",
			);
		}

		const allowedJobs: RuntimeRecoveredToolJobState[] = [];
		for (const pending of actionableJobs) {
			if (isPendingSandboxExecution(pending)) {
				allowedJobs.push(pending);
				continue;
			}
			const confirmation = isResolvedToolRoute(pending)
				? pending
				: session.state.toolConfirmation(pending.toolUseEventId);
			if (confirmation === undefined) {
				continue;
			}
			if (confirmation.decision === "deny") {
				if (
					session.state.runtimeShutdownRequested() ||
					session.state.userInterruptRequested()
				) {
					return yield* closeForRuntimeControl([]);
				}
				const denied = yield* Effect.promise(() =>
					durableOperations.run(() =>
						commitRecoveredToolSettlement(session, options, pending, {
							type: "error",
							error: deniedToolCallFailure(
								session.sessionId,
								pending.source,
								confirmation.denyMessage ?? "The user denied this tool call.",
							),
						}),
					),
				);
				if (
					session.state.runtimeShutdownRequested() ||
					session.state.userInterruptRequested()
				) {
					return yield* closeForRuntimeControl([]);
				}
				if (denied.type === "failed") {
					return yield* Effect.promise(() =>
						handleProcessorFailure(session, options, denied.error),
					);
				}
				if (denied.type === "stale_custody") {
					return providerTurnInterruptedWithDiscard();
				}
				removeRecoveredToolRouteJob(session, options, pending);
				continue;
			}
			allowedJobs.push(pending);
		}

		if (allowedJobs.length === 0) {
			if (unresolved.length > 0) {
				return {
					type: "waiting_external" as const,
					blockingEventIds: unresolved.map((pending) => pending.toolUseEventId),
				};
			}
			return { type: "resumed" as const };
		}

		const scheduler = new ToolScheduler();
		const statesByJobId = Object.create(null) as Record<
			string,
			RuntimeRecoveredToolJobState | undefined
		>;
		for (const pending of allowedJobs) {
			const job = {
				...pending.job,
				toolUseEventId: pending.toolUseEventId,
				gateState: "runnable" as const,
				decision: "allow" as const,
				...(isResolvedToolRoute(pending)
					? {}
					: { approvalSource: "user" as const }),
			};
			scheduler.addJob(job);
			statesByJobId[job.id] = { ...pending, job };
		}

		const batchScope = yield* Scope.make();
		return yield* Effect.gen(function* () {
			while (Object.keys(statesByJobId).length > 0) {
				if (
					session.state.runtimeShutdownRequested() ||
					session.state.userInterruptRequested()
				) {
					return yield* closeForRuntimeControl([]);
				}
				const readyJobs = scheduler.startReady();
				if (readyJobs.length === 0) {
					const first = Object.values(statesByJobId).find(
						(state) => state !== undefined,
					);
					const source = first?.source ?? {
						providerId: "unknown",
						modelId: "unknown",
					};
					return pendingApprovalResumeFailed(
						pendingApprovalResumeFailure(
							session.sessionId,
							source,
							"approved tool scheduler made no progress",
						),
					);
				}
				const active: Array<{
					readonly job: ToolJob;
					readonly fiber: Fiber.Fiber<
						PendingApprovalToolSettlementResult,
						never
					>;
				}> = [];
				for (const job of readyJobs) {
					if (
						session.state.runtimeShutdownRequested() ||
						session.state.userInterruptRequested()
					) {
						return yield* closeForRuntimeControl(
							active.map(({ fiber }) => fiber),
						);
					}
					const pending = statesByJobId[job.id];
					if (pending === undefined) {
						return pendingApprovalResumeFailed(
							pendingApprovalResumeFailure(
								session.sessionId,
								{ providerId: "unknown", modelId: "unknown" },
								"approved tool job state missing",
							),
						);
					}
					const fiber = yield* resumeRecoveredToolJobEffect(
						session,
						options,
						pending,
						durableOperations,
					).pipe(Effect.forkIn(batchScope));
					active.push({ job, fiber });
				}
				activeSettlementFibers = active.map(({ fiber }) => fiber);
				const joined = Effect.forEach(
					active,
					({ job, fiber }) =>
						Fiber.join(fiber).pipe(Effect.map((result) => ({ job, result }))),
					{ concurrency: "unbounded" },
				).pipe(Effect.map((results) => ({ type: "joined" as const, results })));
				const completed = yield* joined;
				activeSettlementFibers = [];
				const interrupted = completed.results.find(
					({ result }) => result.type === "interrupted",
				);
				if (
					interrupted ||
					session.state.runtimeShutdownRequested() ||
					session.state.userInterruptRequested()
				) {
					const closed = yield* closeForRuntimeControl(
						active.map(({ fiber }) => fiber),
					);
					return interrupted?.result.type === "interrupted" &&
						interrupted.result.attachmentRideDisposition === "discard_hot_state"
						? interrupted.result
						: closed;
				}
				const failed = completed.results.find(
					({ result }) => result.type === "failed",
				);
				if (failed?.result.type === "failed") {
					return failed.result;
				}
				for (const { job } of completed.results) {
					scheduler.finishJob(job.id, "approved");
					delete statesByJobId[job.id];
				}
			}

			if (unresolved.length > 0) {
				return {
					type: "waiting_external" as const,
					blockingEventIds: unresolved.map((pending) => pending.toolUseEventId),
				};
			}
			return { type: "resumed" as const };
		}).pipe(
			Effect.ensuring(
				Scope.close(batchScope, Exit.void).pipe(
					Effect.timeoutOption(`${ProviderRequestScopeCloseTimeoutMs} millis`),
					Effect.asVoid,
				),
			),
		);
	});

	return run.pipe(
		Effect.onInterrupt(() =>
			closeForRuntimeControl(activeSettlementFibers).pipe(Effect.asVoid),
		),
		Effect.ensuring(
			Effect.sync(fenceWrites).pipe(
				Effect.andThen(Effect.promise(() => durableOperations.awaitIdle())),
				Effect.andThen(
					Effect.sync(() => {
						releaseSettlementRawOperationOwner();
					}),
				),
			),
		),
	);
}

function pendingApprovalResumeFailed(
	error: RuntimeFailure,
	releaseReason?: ThreadLoopSessionReleaseReason,
): Extract<ProviderTurnResult, { readonly type: "failed" }> {
	if (releaseReason === undefined) {
		return { type: "failed", error };
	}
	return { type: "failed", error, releaseSession: { reason: releaseReason } };
}

function resumeRecoveredToolJobEffect(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	pending: RuntimeRecoveredToolJobState,
	durableOperations: HotDurableOperationOwner,
): Effect.Effect<PendingApprovalToolSettlementResult, never> {
	return session.toolCoordinator.withPermit(
		pending.job.runPolicy,
		Effect.gen(function* () {
			if (
				session.state.runtimeShutdownRequested() ||
				session.state.userInterruptRequested()
			) {
				return { type: "interrupted" as const };
			}
			const currentModel = pending.currentModel ?? session.state.currentModel();
			let ownsSandboxExecution = isPendingSandboxExecution(pending);
			const tokenRefresh = ownsSandboxExecution
				? yield* refreshAcceptedSandboxBindingTokenEffect(session, options)
				: yield* Effect.promise(() =>
						refreshSessionRuntimeBindingToken(session, options),
					);
			if (
				session.state.runtimeShutdownRequested() ||
				session.state.userInterruptRequested()
			) {
				return { type: "interrupted" as const };
			}
			const executionRequest: RuntimeSandboxExecutionRequest = {
				workspaceId: session.identity.workspaceId,
				sessionId: session.identity.sessionId,
				sessionThreadId: session.identity.sessionThreadId,
				...(session.identity.parentThreadId !== undefined
					? { parentThreadId: session.identity.parentThreadId }
					: {}),
				bindingId: session.identity.bindingId,
				bindingGeneration: session.identity.bindingGeneration,
				targetPodUid: session.identity.targetPodUid,
				runtimeBindingToken: session.identity.runtimeBindingToken,
				modelRequestId: pending.modelRequestId,
				modelToolCallId: pending.job.modelToolCallId,
				modelOrder: pending.job.modelOrder,
				toolUseEventId: pending.toolUseEventId,
				entry: pending.entry,
				input: pending.job.input,
				retainedContextEntries: session.state.contextManager.entries(),
				backgroundCancellationIntent: () =>
					session.state.userInterruptRequested()
						? "user_interrupt"
						: "custody_handoff",
				...(currentModel !== undefined ? { currentModel } : {}),
			};
			let executionResult: RuntimeToolExecutionResult;
			if (tokenRefresh.type === "stale_custody") {
				executionResult = { type: "stale_custody" };
			} else if (tokenRefresh.type === "failed") {
				executionResult = { type: "error", error: tokenRefresh.error };
			} else if (ownsSandboxExecution) {
				executionResult = yield* runRuntimeToolEffect(
					executionRequest,
					pending.source,
					options.awaitSandboxExecution ?? defaultRuntimeSandboxExecutionWaiter,
					ToolRouteCancelJoinTimeoutMs,
				);
			} else if (
				pending.entry.route.kind === "sandbox" &&
				pending.entry.route.operation === "RunTool"
			) {
				const acceptance = yield* Effect.promise(() =>
					durableOperations.run(async () => {
						const accepted = await acceptRuntimeSandboxExecution(
							executionRequest,
							pending.source,
							options.acceptSandboxExecution ??
								defaultRuntimeSandboxExecutionAccepter,
						);
						if (accepted.type === "accepted") {
							session.state.recordPendingSandboxExecutionJob({
								...pending,
								recoveryKind: "sandbox_execution",
								job: {
									...pending.job,
									gateState: "runnable",
									decision: "allow",
									...(isResolvedToolRoute(pending)
										? {}
										: { approvalSource: "user" as const }),
								},
							});
							removeRecoveredToolRouteJob(session, options, pending);
						}
						return accepted;
					}),
				);
				if (acceptance.type === "accepted") {
					ownsSandboxExecution = true;
					executionResult = yield* runRuntimeToolEffect(
						executionRequest,
						pending.source,
						options.awaitSandboxExecution ??
							defaultRuntimeSandboxExecutionWaiter,
						ToolRouteCancelJoinTimeoutMs,
					);
				} else {
					executionResult = acceptance;
				}
			} else {
				executionResult = yield* runRuntimeToolEffect(
					executionRequest,
					pending.source,
					options.runTool ?? defaultRuntimeToolRunner,
					ToolRouteCancelJoinTimeoutMs,
				);
			}
			if (executionResult.type === "stale_custody") {
				return providerTurnInterruptedWithDiscard();
			}
			if (
				session.state.runtimeShutdownRequested() ||
				session.state.userInterruptRequested()
			) {
				return { type: "interrupted" as const };
			}
			const settlement = yield* Effect.gen(function* () {
				const settled = yield* Effect.promise(() =>
					durableOperations.run(() =>
						commitRecoveredToolSettlement(
							session,
							options,
							pending,
							executionResult,
						),
					),
				);
				if (settled.type === "failed") {
					return { type: "failed" as const, error: settled.error };
				}
				if (settled.type === "stale_custody") {
					return { type: "stale_custody" as const };
				}
				if (ownsSandboxExecution) {
					session.state.removePendingSandboxExecutionJob(
						pending.toolUseEventId,
					);
				} else {
					removeRecoveredToolRouteJob(session, options, pending);
				}
				return { type: "settled" as const };
			});
			if (settlement.type === "failed") {
				return yield* Effect.promise(() =>
					handleProcessorFailure(session, options, settlement.error),
				);
			}
			if (settlement.type === "stale_custody") {
				return providerTurnInterruptedWithDiscard();
			}
			if (
				session.state.runtimeShutdownRequested() ||
				session.state.userInterruptRequested()
			) {
				return { type: "interrupted" as const };
			}
			if (
				executionResult.type !== "cancelled" &&
				executionResult.attachments !== undefined &&
				executionResult.attachments.length > 0
			) {
				session.state.addPendingAttachments(executionResult.attachments);
			}
			return { type: "settled" as const };
		}),
	);
}

/** Settles one recovered Tool by durable target identity through the resident turn owner. */
async function commitRecoveredToolSettlement(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	pending: RuntimeRecoveredToolJobState,
	executionResult: Exclude<
		RuntimeToolExecutionResult,
		{ readonly type: "stale_custody" }
	>,
): Promise<ToolSettlementApplicationResult> {
	const settlement = runtimeToolSettlement(executionResult);
	const declaration = RuntimeToolSettlementDeclarationSchema.parse({
		toolUseEventId: pending.toolUseEventId,
		outcome: settlement,
	});
	const result = await options.sessionEventWriter.settleToolResult({
		workspaceId: session.identity.workspaceId,
		sessionId: session.sessionId,
		sessionThreadId: session.identity.sessionThreadId,
		bindingId: session.identity.bindingId,
		bindingGeneration: session.identity.bindingGeneration,
		targetPodUid: session.identity.targetPodUid,
		settlement: declaration,
	});
	if (!result.ok) {
		return {
			type: "failed",
			error: runtimeFailureFromEventWriter(result.error),
		};
	}
	if (result.result.type === "stale") {
		return { type: "stale_custody" };
	}
	return applyCommittedRecoveredToolSettlement(
		session,
		{
			toolUseEventId: pending.toolUseEventId,
			assistantMessageSequence: pending.assistantMessageSequence,
			modelToolCallId: pending.toolPart.modelToolCallId,
		},
		settlement,
	);
}

/** Applies one committed recovered-Tool receipt to the resident Thread projection. */
export function applyCommittedRecoveredToolSettlement(
	session: ThreadRuntime,
	target: {
		readonly toolUseEventId: string;
		readonly assistantMessageSequence: number;
		readonly modelToolCallId: string;
	},
	settlement: RuntimeToolSettlementDeclaration["outcome"],
): ToolSettlementApplicationResult {
	try {
		const resultPart = contextToolResultFromSettlement(
			target.modelToolCallId,
			settlement,
		);
		session.state.contextManager.appendToolResult(
			target.assistantMessageSequence,
			target.modelToolCallId,
			resultPart.result,
		);
		session.state.applyThreadTurnFact({
			fact: "tool_result_committed",
			toolUseEventId: target.toolUseEventId,
			outcome: settlement.type === "completed" ? "success" : settlement.type,
		});
		session.state.clearThreadToolRoute(target.toolUseEventId);
	} catch (error) {
		return {
			type: "failed",
			error: normalizeRuntimeFailure({
				type: "runtime",
				code: "runtime_invalid_sequence",
				rawError: error,
				retryable: false,
				fatal: true,
				reason: "runtime_contract_validation",
				sessionId: session.sessionId,
			}),
		};
	}
	return { type: "settled" };
}

function joinToolFibersEffect(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	processor: ProviderStreamAccumulator,
	source: RuntimeProcessorSource,
	modelRequestId: string,
	state: ProviderTurnStreamState,
): Effect.Effect<ProviderTurnResult | undefined, unknown> {
	return Effect.gen(function* () {
		let index = 0;
		while (index < state.toolFibers.length) {
			const fiber = state.toolFibers[index];
			index += 1;
			if (fiber === undefined) {
				continue;
			}
			const result = yield* Fiber.join(fiber);
			if (result.type === "failed" || result.type === "interrupted") {
				return result;
			}
		}
		return undefined;
	});
}

function settleToolsAfterRequestEndFailureEffect(
	session: ThreadRuntime,
	processor: ProviderStreamAccumulator,
	source: RuntimeProcessorSource,
	state: ProviderTurnStreamState,
	failure: RuntimeFailure,
): Effect.Effect<ProviderTurnResult, never> {
	return Effect.gen(function* () {
		yield* Effect.forEach(state.toolFibers, (fiber) => Fiber.interrupt(fiber), {
			concurrency: "unbounded",
			discard: true,
		}).pipe(
			Effect.timeoutOption(`${ProviderRequestScopeCloseTimeoutMs} millis`),
			Effect.asVoid,
		);
		yield* Effect.promise(() => state.durableOperations.awaitIdle());
		const terminalized = yield* Effect.promise(() =>
			state.durableOperations.run(
				() =>
					processor.cancelOpenTools(
						source,
						failure,
						pendingSandboxExecutionToolUseEventIds(session),
					),
				true,
			),
		);
		yield* Effect.promise(() => state.durableOperations.awaitIdle());
		if (terminalized.type === "failed") {
			return providerTurnFailed(
				terminalized.error,
				terminalized.error.type === "message-store"
					? "persistence_failed"
					: "event_write_failed",
			);
		}
		if (terminalized.type === "stale_custody") {
			return providerTurnInterruptedWithDiscard();
		}
		return providerTurnFailed(failure, "event_write_failed");
	});
}

function coordinateRuntimeToolJobEffect(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	processor: ProviderStreamAccumulator,
	source: RuntimeProcessorSource,
	modelRequestId: string,
	state: ProviderTurnStreamState,
	job: ToolJob,
): Effect.Effect<ProviderTurnResult, unknown> {
	return Effect.gen(function* () {
		if (
			session.state.runtimeShutdownRequested() ||
			session.state.userInterruptRequested()
		) {
			return providerTurnInterrupted();
		}
		const toolCatalog = state.executionPolicy.toolCatalog;
		const entry = state.toolEntries[job.id];
		const providerInput = state.toolProviderInputs[job.id];
		if (entry === undefined || toolCatalog === undefined) {
			state.toolScheduler.finishJob(job.id);
			return providerTurnCompleted();
		}
		if (providerInput === undefined) {
			state.toolScheduler.finishJob(job.id);
			return yield* Effect.promise(() =>
				handleProcessorFailure(
					session,
					options,
					normalizeRuntimeFailure({
						type: "runtime",
						code: "runtime_invalid_sequence",
						retryable: false,
						fatal: true,
						reason: "runtime_contract_validation",
						sessionId: session.sessionId,
					}),
				),
			);
		}

		let gateDecision = evaluateToolGate({
			catalog: toolCatalog,
			toolName: job.name,
			approvalMode: state.executionSnapshot.approvalMode,
		});
		if (gateDecision.type === "invalid") {
			const settled = yield* state.assistantProjectionGate.withPermit(
				Effect.promise(async () => {
					const result = await state.durableOperations.run(() =>
						processor.commitInternalToolRepair(
							source,
							job.modelToolCallId,
							modelRequestId,
							internalToolRepairKey(
								modelRequestId,
								job.modelToolCallId,
								job.name,
							),
							invalidToolCallFailure(session.sessionId, source, job.name),
						),
					);
					if (result.ok) {
						commitProcessorProjectionWithoutStableReasoning(session, processor);
					}
					return result;
				}),
			);
			if (!settled.ok) {
				return yield* Effect.promise(() =>
					handleProcessorFailure(session, options, settled.error),
				);
			}
			const declarationBarrier = state.toolDeclarationBarriers[job.id];
			if (declarationBarrier !== undefined) {
				yield* Deferred.succeed(declarationBarrier, true);
			}
			state.toolScheduler.finishJob(job.id);
			return providerTurnCompleted();
		}

		if (gateDecision.type === "review_required") {
			const parentBoundaryEventId =
				session.state.threadTurnTransition().checkpoint.request
					?.requestStartEventId;
			if (parentBoundaryEventId === undefined) {
				return yield* Effect.promise(() =>
					handleProcessorFailure(
						session,
						options,
						normalizeRuntimeFailure({
							type: "runtime",
							code: "runtime_invalid_sequence",
							retryable: false,
							fatal: true,
							reason: "runtime_contract_validation",
							sessionId: session.sessionId,
						}),
					),
				);
			}
			const reviewerOutcome = yield* Effect.gen(function* () {
				if (
					options.reviewApproval === undefined ||
					session.approvalReviewerManager === undefined
				) {
					return {
						type: "settlement_failed" as const,
						error: normalizeSessionEventWriterError({
							code: "unknown",
							sessionId: session.sessionId,
							writeId: `rwrite_${modelRequestId}_${job.modelToolCallId}_reviewer_unavailable`,
						}),
					};
				}
				return yield* options.reviewApproval({
					workspaceId: session.identity.workspaceId,
					sessionId: session.identity.sessionId,
					sessionThreadId: session.identity.sessionThreadId,
					...(session.identity.parentThreadId !== undefined
						? { parentThreadId: session.identity.parentThreadId }
						: {}),
					bindingId: session.identity.bindingId,
					bindingGeneration: session.identity.bindingGeneration,
					targetPodUid: session.identity.targetPodUid,
					runtimeBindingToken: session.identity.runtimeBindingToken,
					modelRequestId,
					parentBoundaryEventId,
					targetModelToolCallId: job.modelToolCallId,
					targetToolName: job.name,
					actionJson: job.input,
					approvalReviewerManager: session.approvalReviewerManager,
					parentTranscript: session.state.contextManager.entryListSnapshot(),
					currentAssistantDraft: processor.openRequestDraft()?.parts ?? [],
					siblingToolCalls: state.toolScheduler.jobs().map((candidate) => ({
						modelToolCallId: candidate.modelToolCallId,
						toolName: candidate.name,
						actionJson: candidate.input,
					})),
					policyContext: {
						approvalMode: state.executionSnapshot.approvalMode,
						permissionPolicy: effectiveToolPermissionPolicy(
							toolCatalog,
							job.name,
						),
						routeKind: entry.route.kind,
					},
					currentModel: session.state.currentModel(),
				});
			}).pipe(
				Effect.catchCause(() =>
					Effect.succeed({
						type: "settlement_failed" as const,
						error: normalizeSessionEventWriterError({
							code: "unknown",
							sessionId: session.sessionId,
							writeId: `rwrite_${modelRequestId}_${job.modelToolCallId}_reviewer_settlement`,
						}),
					}),
				),
			);
			if (reviewerOutcome.type === "stale_custody") {
				return providerTurnInterruptedWithDiscard();
			}
			if (reviewerOutcome.type === "settlement_failed") {
				return providerTurnFailed(
					runtimeFailureFromEventWriter(reviewerOutcome.error),
					"event_write_failed",
				);
			}
			gateDecision = evaluateToolGate({
				catalog: toolCatalog,
				toolName: job.name,
				approvalMode: state.executionSnapshot.approvalMode,
				reviewerOutcome,
			});
		}

		if (gateDecision.type === "invalid") {
			const declarationBarrier = state.toolDeclarationBarriers[job.id];
			if (declarationBarrier !== undefined) {
				yield* Deferred.succeed(declarationBarrier, true);
			}
			state.toolScheduler.finishJob(job.id);
			return providerTurnCompleted();
		}

		const toolUse = yield* Effect.promise(() =>
			state.durableOperations.run(() =>
				processor
					.commitPublicToolUse(
						source,
						job.modelToolCallId,
						publicInputForRegisteredTool(job.input),
						gateDecision.evaluatedPermission,
						publicToolEventForEntry(entry),
					)
					.then((result) => {
						if (!result.ok) return result;
						commitProcessorProjectionWithoutStableReasoning(session, processor);
						const toolUseReduction = session.state.applyThreadTurnFact({
							fact: "tool_use_committed",
							eventId: result.toolUseEventId,
							modelRequestId,
							modelToolCallId: job.modelToolCallId,
							toolName: job.name,
						});
						const toolUseDispatch = toolUseReduction.dispatch;
						const dispatchAuthorized =
							toolUseDispatch?.dispatch === "route_tool_use" &&
							toolUseDispatch.toolUseEventId === result.toolUseEventId;
						if (dispatchAuthorized) {
							session.state.recordThreadToolRoute(
								result.toolUseEventId,
								gateDecision.type === "ask" ||
									gateDecision.type === "review_required"
									? "requires_user_action"
									: "hot_execution",
							);
						}
						return { ...result, dispatchAuthorized };
					}),
			),
		);
		if (!toolUse.ok) {
			if (!("error" in toolUse)) {
				return providerTurnInterruptedWithDiscard();
			}
			return yield* Effect.promise(() =>
				handleProcessorFailure(session, options, toolUse.error),
			);
		}
		if (!toolUse.dispatchAuthorized) {
			return yield* Effect.promise(() =>
				handleProcessorFailure(
					session,
					options,
					pendingApprovalResumeFailure(
						session.sessionId,
						source,
						"Tool Use result did not authorize dispatch",
					),
				),
			);
		}
		const declarationBarrier = state.toolDeclarationBarriers[job.id];
		if (declarationBarrier !== undefined) {
			yield* Deferred.succeed(declarationBarrier, true);
		}
		if (
			session.state.runtimeShutdownRequested() ||
			session.state.userInterruptRequested()
		) {
			return providerTurnInterrupted();
		}

		if (
			gateDecision.type === "ask" ||
			gateDecision.type === "review_required"
		) {
			state.waitingToolUseEventIds.push(toolUse.toolUseEventId);
			state.toolScheduler.waitForApproval(job.id, gateDecision.approvalSource);
			const toolPart = processor.activeToolPart(job.modelToolCallId);
			const openRequestDraft = processor.openRequestDraft();
			if (toolPart === undefined || openRequestDraft === undefined) {
				return yield* Effect.promise(() =>
					handleProcessorFailure(
						session,
						options,
						pendingApprovalResumeFailure(
							session.sessionId,
							source,
							"committed approval tool part is unavailable",
						),
					),
				);
			}
			session.state.recordPendingApprovalToolJob({
				toolUseEventId: toolUse.toolUseEventId,
				modelRequestId,
				source,
				assistantMessageSequence: openRequestDraft.messageSequence,
				toolPart,
				job: {
					...job,
					toolUseEventId: toolUse.toolUseEventId,
					gateState: "waiting_approval",
					approvalSource: gateDecision.approvalSource,
				},
				entry,
				...(session.state.currentModel() !== undefined
					? { currentModel: session.state.currentModel() }
					: {}),
			});
			runtimeMetrics(options).addPendingApprovals(1);
			return providerTurnCompleted();
		}

		if (gateDecision.type === "deny") {
			const denied = yield* state.assistantProjectionGate.withPermit(
				Effect.promise(async () => {
					return await state.durableOperations.run(() =>
						commitRuntimeToolSettlement(
							processor,
							source,
							job.modelToolCallId,
							{
								type: "error",
								error: deniedToolCallFailure(
									session.sessionId,
									source,
									gateDecision.message,
								),
							},
							() =>
								commitProcessorProjectionWithoutStableReasoning(
									session,
									processor,
								),
						),
					);
				}),
			);
			if (denied.type === "failed") {
				return yield* Effect.promise(() =>
					handleProcessorFailure(session, options, denied.error),
				);
			}
			if (denied.type === "stale_custody") {
				return providerTurnInterruptedWithDiscard();
			}
			state.toolScheduler.finishJob(job.id, gateDecision.message);
			return providerTurnCompleted();
		}

		const execution = Effect.gen(function* () {
			const tracksSandboxExecution =
				entry.route.kind === "sandbox" && entry.route.operation === "RunTool";
			const tokenRefresh = yield* Effect.promise(() =>
				refreshSessionRuntimeBindingToken(session, options),
			);
			const executionRequest: RuntimeSandboxExecutionRequest = {
				workspaceId: session.identity.workspaceId,
				sessionId: session.identity.sessionId,
				sessionThreadId: session.identity.sessionThreadId,
				...(session.identity.parentThreadId !== undefined
					? { parentThreadId: session.identity.parentThreadId }
					: {}),
				bindingId: session.identity.bindingId,
				bindingGeneration: session.identity.bindingGeneration,
				targetPodUid: session.identity.targetPodUid,
				runtimeBindingToken: session.identity.runtimeBindingToken,
				modelRequestId,
				modelToolCallId: job.modelToolCallId,
				modelOrder: job.modelOrder,
				toolUseEventId: toolUse.toolUseEventId,
				entry,
				input: job.input,
				retainedContextEntries: session.state.contextManager.entries(),
				backgroundCancellationIntent: () =>
					session.state.userInterruptRequested()
						? "user_interrupt"
						: "custody_handoff",
				...(session.state.currentModel() !== undefined
					? { currentModel: session.state.currentModel() }
					: {}),
			};
			let executionResult: RuntimeToolExecutionResult;
			if (tokenRefresh.type === "stale_custody") {
				executionResult = { type: "stale_custody" };
			} else if (tokenRefresh.type === "failed") {
				executionResult = { type: "error", error: tokenRefresh.error };
			} else if (!tracksSandboxExecution) {
				executionResult = yield* runRuntimeToolEffect(
					executionRequest,
					source,
					options.runTool ?? defaultRuntimeToolRunner,
					ToolRouteCancelJoinTimeoutMs,
				);
			} else {
				const toolPart = processor.activeToolPart(job.modelToolCallId);
				const openRequestDraft = processor.openRequestDraft();
				if (toolPart === undefined || openRequestDraft === undefined) {
					return yield* Effect.promise(() =>
						handleProcessorFailure(
							session,
							options,
							pendingApprovalResumeFailure(
								session.sessionId,
								source,
								"committed sandbox tool part is unavailable",
							),
						),
					);
				}
				const acceptance = yield* Effect.promise(() =>
					state.durableOperations.run(async () => {
						const accepted = await acceptRuntimeSandboxExecution(
							executionRequest,
							source,
							options.acceptSandboxExecution ??
								defaultRuntimeSandboxExecutionAccepter,
						);
						if (accepted.type === "accepted") {
							session.state.recordPendingSandboxExecutionJob({
								recoveryKind: "sandbox_execution",
								toolUseEventId: toolUse.toolUseEventId,
								modelRequestId,
								source,
								assistantMessageSequence: openRequestDraft.messageSequence,
								toolPart,
								job: {
									...job,
									toolUseEventId: toolUse.toolUseEventId,
									gateState: "runnable",
								},
								entry,
								...(session.state.currentModel() !== undefined
									? { currentModel: session.state.currentModel() }
									: {}),
							});
						}
						return accepted;
					}),
				);
				executionResult =
					acceptance.type === "accepted"
						? yield* runRuntimeToolEffect(
								executionRequest,
								source,
								options.awaitSandboxExecution ??
									defaultRuntimeSandboxExecutionWaiter,
								ToolRouteCancelJoinTimeoutMs,
							)
						: acceptance;
			}
			if (executionResult.type === "stale_custody") {
				return providerTurnInterruptedWithDiscard();
			}
			const settled = yield* state.assistantProjectionGate.withPermit(
				Effect.promise(async () => {
					return await state.durableOperations.run(() =>
						commitRuntimeToolSettlement(
							processor,
							source,
							job.modelToolCallId,
							executionResult,
							() =>
								commitProcessorProjectionWithoutStableReasoning(
									session,
									processor,
								),
						),
					);
				}),
			);
			if (settled.type === "failed") {
				return yield* Effect.promise(() =>
					handleProcessorFailure(session, options, settled.error),
				);
			}
			if (settled.type === "stale_custody") {
				return providerTurnInterruptedWithDiscard();
			}
			if (tracksSandboxExecution) {
				session.state.removePendingSandboxExecutionJob(toolUse.toolUseEventId);
			}
			if (
				executionResult.type !== "cancelled" &&
				executionResult.attachments !== undefined &&
				executionResult.attachments.length > 0
			) {
				session.state.addPendingAttachments(executionResult.attachments);
			}
			state.toolScheduler.finishJob(
				job.id,
				executionResult.type === "completed"
					? executionResult.output.text
					: executionResult.type,
			);
			return providerTurnCompleted();
		});
		return yield* session.toolCoordinator.withPermit(
			job.runPolicy,
			Effect.sync(() => runtimeMetrics(options).addActiveToolFibers(1)).pipe(
				Effect.andThen(execution),
				Effect.ensuring(
					Effect.sync(() => runtimeMetrics(options).addActiveToolFibers(-1)),
				),
			),
		);
	});
}

function refreshAcceptedSandboxBindingTokenEffect(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
): Effect.Effect<RuntimeBindingTokenRefreshResult, never> {
	return Effect.gen(function* () {
		for (;;) {
			if (
				session.state.runtimeShutdownRequested() ||
				session.state.userInterruptRequested()
			) {
				return {
					type: "failed" as const,
					error: normalizeRuntimeFailure({
						type: "session-binding",
						code: "gateway_unavailable",
						sessionId: session.sessionId,
						retryable: true,
						fatal: false,
						reason: "runtime_shutdown",
					}),
				};
			}
			const refreshed = yield* Effect.promise(() =>
				refreshSessionRuntimeBindingToken(session, options),
			);
			if (refreshed.type !== "failed") {
				return refreshed;
			}
			yield* Effect.sleep(RecoveredSandboxBindingRefreshBackoffMs);
		}
	});
}

async function refreshSessionRuntimeBindingToken(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
): Promise<RuntimeBindingTokenRefreshResult> {
	if (options.refreshRuntimeBindingToken === undefined) {
		return { type: "refreshed" };
	}
	try {
		const runtimeBindingToken = await options.refreshRuntimeBindingToken(
			session.identity,
		);
		if (runtimeBindingToken.length === 0) {
			throw new Error("runtime binding token refresh returned an empty token");
		}
		session.updateIdentity({ ...session.identity, runtimeBindingToken });
		return { type: "refreshed" };
	} catch (error) {
		const loaderError = ContextLoaderErrorSchema.safeParse(error);
		if (loaderError.success && loaderError.data.code === "superseded") {
			return { type: "stale_custody" };
		}
		return {
			type: "failed",
			error: normalizeRuntimeFailure({
				type: "session-binding",
				code: "gateway_unavailable",
				sessionId: session.sessionId,
				retryable: true,
				fatal: false,
				retryStatus: { type: "exhausted" },
			}),
		};
	}
}

type RuntimeBindingTokenRefreshResult =
	| { readonly type: "refreshed" }
	| { readonly type: "stale_custody" }
	| { readonly type: "failed"; readonly error: RuntimeFailure };

function handleProviderStreamExhaustedEffect(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	processor: ProviderStreamAccumulator,
	source: RuntimeProcessorSource,
	request: LLMRequest,
	modelRequestStartId: string,
	modelUsage: RuntimeUsage | undefined,
	turnRetryCounters: { providerAttempts: number; compactionAttempts: number },
	state: ProviderTurnStreamState,
): Effect.Effect<ProviderTurnResult, unknown> {
	return Effect.gen(function* () {
		const failure = providerStreamExhaustedFailure(request);
		const processed = yield* state.assistantProjectionGate.withPermit(
			Effect.promise(async () => {
				const result = await state.durableOperations.run(() =>
					processor.process({
						...source,
						event: { type: "provider-error", error: failure },
					}),
				);
				if (result.ok) {
					commitProcessorProjectionWithoutStableReasoning(session, processor);
				}
				return result;
			}),
		);
		if (!processed.ok) {
			return yield* Effect.promise(() =>
				closeStartedRequestAfterProcessorFailure(
					session,
					options,
					processor,
					request.modelRequestId,
					modelRequestStartId,
					processed.error,
					modelUsage,
					state.carriedAttachments,
					requestEndKindFromRequest(request),
				),
			);
		}
		return yield* closeProviderFailureEffect(
			session,
			options,
			processor,
			source,
			request,
			modelRequestStartId,
			modelUsage,
			failure,
			turnRetryCounters,
			state,
		);
	});
}

function closeProviderFailureEffect(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	processor: ProviderStreamAccumulator,
	source: RuntimeProcessorSource,
	request: LLMRequest,
	modelRequestStartId: string,
	usage: RuntimeUsage | undefined,
	failure: RuntimeFailure,
	counters: { providerAttempts: number; compactionAttempts: number },
	state: ProviderTurnStreamState,
	allowContextOverflowRecovery = false,
): Effect.Effect<ProviderTurnResult, unknown> {
	return Effect.gen(function* () {
		const plan = providerRequestReschedulePlan(
			session,
			options,
			counters,
			"provider",
			failure,
			state.executionPolicy,
		);
		processor.discardUncommittedMembers();
		yield* Effect.promise(() => processor.awaitAssistantMembersDrained());
		const declarationOutcomes = yield* Effect.forEach(
			state.allToolDeclarationBarriers,
			(barrier) => Deferred.await(barrier),
			{ concurrency: "unbounded" },
		);
		for (let index = 0; index < declarationOutcomes.length; index += 1) {
			if (declarationOutcomes[index] === true) continue;
			const fiber = state.toolFibers[index];
			if (fiber !== undefined) {
				yield* Fiber.interrupt(fiber);
			}
		}
		const requestEnd = yield* Effect.promise(() =>
			appendModelRequestEndEvent(
				options,
				session,
				request.modelRequestId,
				modelRequestStartId,
				true,
				requestErrorKindFromFailure(failure),
				"error",
				usage,
				state.carriedAttachments,
				requestEndKindFromRequest(request),
				plan.type === "proposed"
					? {
							...plan.reschedule,
							providerAttempts: plan.reschedule.attempt,
							compactionAttempts: counters.compactionAttempts,
						}
					: undefined,
				undefined,
			),
		);
		if (!requestEnd.ok) {
			return providerTurnFailed(requestEnd.error, "event_write_failed");
		}
		if (requestEnd.type === "stale") {
			yield* interruptAndJoinToolFibersEffect(state);
			return requestEndCommitted(
				providerTurnInterrupted(),
				"discard_hot_state",
			);
		}
		const toolSettlement = yield* settleProviderErrorToolsEffect(
			session,
			processor,
			state,
		);
		if (toolSettlement !== undefined) {
			return requestEndCommitted(toolSettlement, "retained");
		}
		if (allowContextOverflowRecovery && isContextOverflowFailure(failure)) {
			commitProcessorProjectionWithoutStableReasoning(session, processor);
			return requestEndCommitted(
				providerTurnContextOverflow(failure),
				"retained",
			);
		}
		if (plan.type === "proposed") {
			if (requestEnd.outcome.type === "rescheduled") {
				counters.providerAttempts = plan.reschedule.attempt;
				recordProviderReschedule(
					options,
					session,
					request,
					plan.reschedule.attempt,
					plan.reschedule.backoffMs,
					failure,
				);
				const retryingFailure = compactionFailureWithRetryStatus(failure, {
					type: "retrying",
					attempt: plan.reschedule.attempt,
				});
				const retryAppend = yield* Effect.promise(() =>
					appendEvent(options, session, {
						type: "session.error",
						error: retryingFailure,
					}),
				);
				if (!retryAppend.ok) {
					return requestEndCommitted(
						providerTurnFailed(
							runtimeFailureFromEventWriter(retryAppend.error),
							"event_write_failed",
						),
						"retained",
					);
				}
				commitProcessorProjectionWithoutStableReasoning(session, processor);
				return requestEndCommitted(
					providerTurnRescheduled(
						failure,
						requestEnd.outcome.effectiveDeadline,
					),
					"retained",
				);
			}
			const exhausted = compactionFailureWithRetryStatus(failure, {
				type: "exhausted",
			});
			const terminalAppend = yield* Effect.promise(() =>
				appendTerminalEventsBestEffort(options, session, exhausted),
			);
			if (!terminalAppend.ok) {
				return requestEndCommitted(
					providerTurnFailed(terminalAppend.error, "event_write_failed"),
					"retained",
				);
			}
			commitProcessorProjectionWithoutStableReasoning(session, processor);
			return requestEndCommitted(
				providerTurnFailed(
					terminalAppend.settledFailure,
					undefined,
					terminalAppend.failureEventId,
				),
				"retained",
			);
		}
		const terminalFailure = plan.type === "exhausted" ? plan.failure : failure;
		const terminalAppend = yield* Effect.promise(() =>
			appendTerminalEventsBestEffort(options, session, terminalFailure),
		);
		if (!terminalAppend.ok) {
			return requestEndCommitted(
				providerTurnFailed(terminalAppend.error, "event_write_failed"),
			);
		}
		commitProcessorProjectionWithoutStableReasoning(session, processor);
		return requestEndCommitted(
			providerTurnFailed(
				terminalAppend.settledFailure,
				undefined,
				terminalAppend.failureEventId,
			),
		);
	});
}

function recordProviderReschedule(
	options: ThreadLoopRuntimeOptions,
	session: ThreadRuntime,
	request: LLMRequest,
	attempt: number,
	delayMs: number,
	failure: RuntimeFailure,
): void {
	try {
		options.recordProviderReschedule?.({
			workspaceId: session.identity.workspaceId,
			sessionId: session.sessionId,
			sessionThreadId: session.identity.sessionThreadId,
			requestId: request.requestId,
			modelRequestId: request.modelRequestId,
			attempt,
			delayMs,
			delaySource:
				(failure.retryAfterMs ?? 0) > 0 ? "provider" : "runtime_fallback",
			failureCode: failure.code,
		});
	} catch {
		// Retry settlement is authoritative; observability remains a side channel.
	}
}

function settleProviderErrorToolsEffect(
	session: ThreadRuntime,
	processor: ProviderStreamAccumulator,
	state: ProviderTurnStreamState,
): Effect.Effect<ProviderTurnResult | undefined, unknown> {
	return Effect.gen(function* () {
		yield* Effect.promise(() => processor.awaitAssistantMembersDrained());
		const declarationOutcomes = yield* Effect.forEach(
			state.allToolDeclarationBarriers,
			(barrier) => Deferred.await(barrier),
			{ concurrency: "unbounded" },
		);
		for (let index = 0; index < state.toolFibers.length; index += 1) {
			const fiber = state.toolFibers[index];
			if (fiber === undefined) continue;
			if (declarationOutcomes[index] !== true) {
				yield* Fiber.interrupt(fiber);
				continue;
			}
			const result = yield* Fiber.join(fiber);
			if (result.type === "failed" || result.type === "interrupted") {
				return result;
			}
		}
		yield* Effect.promise(() => state.durableOperations.awaitIdle());
		const projection = applyFailedRequestProviderProjection(session);
		if (projection !== undefined) {
			return providerTurnFailed(projection, "event_write_failed");
		}
		const blockingEventIds = [
			...new Set([
				...state.waitingToolUseEventIds,
				...(session.state.threadTurnTransition().checkpoint.request?.toolMembers.flatMap(
					(member) =>
						member.memberKind === "public_tool_use" &&
						member.terminalResult === undefined
							? [member.toolUseEventId]
							: [],
				) ?? []),
			]),
		];
		if (blockingEventIds.length > 0) {
			// An error Request End intentionally keeps the reducer's existing
			// retry/reschedule action. Yield the run without authoring an Idle
			// transition; the exact pending Tool route wakes the next run and
			// remains the sole settlement owner.
			return providerTurnInterrupted();
		}
		return undefined;
	});
}

function applyFailedRequestProviderProjection(
	session: ThreadRuntime,
): RuntimeFailure | undefined {
	try {
		const projected = projectFailedRequestProviderContext({
			contextEntries: session.state.contextManager.entries(),
			...(session.state.contextManager.openRequestDraft() === undefined
				? {}
				: {
						openRequestDraft:
							session.state.contextManager.openRequestDraft(),
					}),
			checkpoint: session.state.threadTurnTransition().checkpoint,
		});
		session.state.contextManager.replaceEntries(projected.contextEntries);
		session.state.contextManager.installOpenRequestDraft(
			projected.openRequestDraft,
		);
		return undefined;
	} catch (error) {
		return normalizeRuntimeFailure({
			type: "runtime",
			code: "runtime_invalid_sequence",
			rawError: error,
			retryable: false,
			fatal: true,
			reason: "runtime_contract_validation",
			sessionId: session.sessionId,
		});
	}
}

type ProviderRequestReschedulePlan =
	| { readonly type: "none" }
	| { readonly type: "exhausted"; readonly failure: RuntimeFailure }
	| {
			readonly type: "proposed";
			readonly reschedule: NonNullable<
				SessionEventWriterRequestEndEnvelope["reschedule"]
			>;
	  };

function providerRequestReschedulePlan(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	counters: RuntimeTurnRetryCounters,
	kind: RuntimeTurnRetryKind,
	failure: RuntimeFailure,
	executionPolicy: Readonly<ThreadLoopRuntimePolicy> = requestExecutionPolicy(
		session,
		options,
	),
): ProviderRequestReschedulePlan {
	if (
		!failure.retryable ||
		failure.fatal ||
		isRuntimeTerminationFailure(failure)
	) {
		return { type: "none" };
	}
	const policy = executionPolicy;
	const maxAttempts =
		kind === "provider"
			? (policy.providerRescheduleBudget ?? 3)
			: (policy.compactionRescheduleBudget ?? 2);
	const decision = evaluateTurnRetryBudget(counters, kind, maxAttempts);
	if (decision.type === "exhausted") {
		return {
			type: "exhausted",
			failure: compactionFailureWithRetryStatus(failure, { type: "exhausted" }),
		};
	}
	const fallbackBackoffMs = Math.min(
		120_000,
		1_000 * 2 ** Math.min(decision.attempt - 1, 6),
	);
	const backoffMs = Math.min(
		120_000,
		Math.max(0, failure.retryAfterMs ?? fallbackBackoffMs),
	);
	const nowMs = Date.parse(options.runtime.now());
	return {
		type: "proposed",
		reschedule: {
			attempt: decision.attempt,
			backoffMs,
			deadline: new Date(nowMs + backoffMs).toISOString(),
		},
	};
}

function runtimePolicyForSession(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
): ThreadLoopRuntimePolicy {
	return options.runtimePolicy?.(session) ?? {};
}

function requestExecutionPolicy(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
): Readonly<ThreadLoopRuntimePolicy> {
	const policy = runtimePolicyForSession(session, options);
	return Object.freeze({
		...policy,
		...(policy.skillsIndex !== undefined
			? { skillsIndex: Object.freeze([...policy.skillsIndex]) }
			: {}),
		...(policy.memoryStores !== undefined
			? { memoryStores: Object.freeze([...policy.memoryStores]) }
			: {}),
	});
}

function toolCatalogForSession(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
): ToolCatalog | undefined {
	return runtimePolicyForSession(session, options).toolCatalog;
}

async function completeRun(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	custody: ThreadLoopRunCustody,
): Promise<ThreadLoopRunResult> {
	const result = completedRunResult(session);
	if (result.type !== "completed") {
		return result;
	}
	if (custody.activeTurnId(session) === undefined) {
		return result;
	}
	const idleAppend = await appendIdleEvent(options, session, custody, {
		type: "end_turn",
	});
	if (!idleAppend.ok) {
		return {
			type: "failed",
			error: idleAppend.error,
			releaseSession: { reason: "event_write_failed" },
		};
	}
	return result;
}

function settleUnstartedUserInterruptEffect(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
): Effect.Effect<ProviderTurnResult, never> {
	session.state.markUserInterruptCloseoutEligible();
	return settleUserInterruptFenceEffect(session, options).pipe(
		Effect.map((fence) => {
			if (!fence.ok) {
				return providerTurnFailed(fence.error, "event_write_failed");
			}
			return "stale" in fence
				? providerTurnInterruptedWithDiscard()
				: providerTurnInterrupted();
		}),
	);
}

function settleRuntimeShutdownEffect(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	custody: ThreadLoopRunCustody,
	processor: ProviderStreamAccumulator,
	source: RuntimeProcessorSource,
	modelRequestStartId: string | undefined,
	modelRequestId: string | undefined,
	consumedAttachments: readonly RuntimeProviderAttachment[],
	modelUsage: RuntimeUsage | undefined,
	durableOperations: HotDurableOperationOwner,
	streamState?: ProviderTurnStreamState,
): Effect.Effect<ProviderTurnResult, unknown> {
	return Effect.gen(function* () {
		if (!session.state.userInterruptRequested()) {
			if (streamState !== undefined) {
				yield* interruptAndJoinToolFibersEffect(streamState);
			}
			yield* Effect.promise(() => durableOperations.awaitIdle());
			return providerTurnInterrupted();
		}
		const failure = userInterruptFailure(session.sessionId, source);
		let committedRequestEnd = false;
		if (streamState !== undefined) {
			yield* interruptAndJoinToolFibersEffect(streamState);
		}
		yield* Effect.promise(() => durableOperations.awaitIdle());
		if (modelRequestStartId !== undefined && modelRequestId !== undefined) {
			const command = session.state.userInterruptCommand();
			if (command === undefined) {
				return yield* failRequestCloseout(
					normalizeRuntimeFailure({
						type: "runtime",
						code: "runtime_invalid_sequence",
						retryable: true,
						fatal: false,
						reason: "runtime_contract_validation",
						sessionId: session.sessionId,
					}),
				);
			}
			const interruptLeaseRef = custody.interruptLeaseRef(
				command.runtimeInputId,
			);
			if (interruptLeaseRef === undefined) {
				return yield* failRequestCloseout(
					normalizeRuntimeFailure({
						type: "runtime",
						code: "runtime_invalid_sequence",
						retryable: false,
						fatal: true,
						reason: "runtime_contract_validation",
						sessionId: session.sessionId,
					}),
				);
			}
			try {
				processor.prepareInterruptSettlement(command, failure);
			} catch (error) {
				return yield* failRequestCloseout(
					normalizeRuntimeFailure({
						type: "runtime",
						code: "runtime_invalid_sequence",
						rawError: error,
						retryable: false,
						fatal: true,
						reason: "runtime_contract_validation",
						sessionId: session.sessionId,
					}),
				);
			}
			const spanEndAppend = yield* Effect.promise(() =>
				appendModelRequestEndEvent(
					options,
					session,
					modelRequestId,
					modelRequestStartId,
					true,
					"runtime_interrupted",
					"cancelled",
					modelUsage,
					consumedAttachments,
					requestEndKindForSession(session),
					undefined,
					undefined,
					undefined,
					{
						command,
						interruptLeaseRef,
					},
				),
			);
			if (!spanEndAppend.ok) {
				session.state.recordJoinedUserInterruptResult(
					command.runtimeInputId,
					{
						ok: false,
						retryable: spanEndAppend.error.retryable,
						errorCode: spanEndAppend.error.code,
					},
					{ inputKind: "interrupt" },
				);
				return yield* failRequestCloseout(spanEndAppend.error);
			}
			if (spanEndAppend.type === "stale") {
				session.state.recordJoinedUserInterruptResult(
					command.runtimeInputId,
					{ ok: true, stale: true },
					{ inputKind: "interrupt" },
				);
				return requestEndCommitted(
					providerTurnInterrupted(),
					"discard_hot_state",
				);
			}
			const sealApplication = applyRequestEndSeal(
				processor,
				undefined,
				requestEndOutcome(spanEndAppend),
			);
			if (sealApplication.type === "stale_custody") {
				if (
					!session.state.recordJoinedUserInterruptResult(
						command.runtimeInputId,
						{ ok: true, stale: true },
						{ inputKind: "interrupt" },
					)
				) {
					return yield* failRequestCloseout(
						normalizeRuntimeFailure({
							type: "runtime",
							code: "runtime_invalid_sequence",
							retryable: false,
							fatal: true,
							reason: "runtime_contract_validation",
							sessionId: session.sessionId,
						}),
					);
				}
				return requestEndCommitted(
					providerTurnInterrupted(),
					"discard_hot_state",
				);
			}
			if (sealApplication.type === "failed") {
				return yield* failRequestCloseout(sealApplication.error);
			}
			try {
				processor.applyInterruptSettlement(
					command,
					spanEndAppend.interruptToolResults,
				);
				session.state.applyThreadTurnFact({
					fact: "interrupt_committed",
					eventId: command.runtimeInputId,
				});
			} catch (error) {
				return yield* failRequestCloseout(
					normalizeRuntimeFailure({
						type: "runtime",
						code: "runtime_invalid_sequence",
						rawError: error,
						retryable: false,
						fatal: true,
						reason: "runtime_contract_validation",
						sessionId: session.sessionId,
					}),
				);
			}
			const requestSealFailure = applyCommittedRequestEnd(
				session,
				spanEndAppend.requestEndEventId,
				modelRequestId,
				true,
				"runtime_interrupted",
				spanEndAppend.providerContextRetention,
				undefined,
			);
			if (requestSealFailure !== undefined) {
				return yield* failRequestCloseout(requestSealFailure);
			}
			const projectionFailure = applyFailedRequestProviderProjection(session);
			if (projectionFailure !== undefined) {
				return yield* failRequestCloseout(projectionFailure);
			}
			if (
				!session.state.recordJoinedUserInterruptResult(
					command.runtimeInputId,
					{ ok: true, joined: true },
					{ inputKind: "interrupt" },
				)
			) {
				return yield* failRequestCloseout(
					normalizeRuntimeFailure({
						type: "runtime",
						code: "runtime_invalid_sequence",
						retryable: false,
						fatal: true,
						reason: "runtime_contract_validation",
						sessionId: session.sessionId,
					}),
				);
			}
			releaseInterruptedPendingTools(
				session,
				options,
				spanEndAppend.interruptToolResults.map(
					(result) => result.toolUseEventId,
				),
			);
			committedRequestEnd = true;
		}
		commitProcessorProjectionWithoutStableReasoning(session, processor);
		session.state.markUserInterruptCloseoutEligible();
		const interruptFence = yield* settleUserInterruptFenceEffect(
			session,
			options,
		);
		if (!interruptFence.ok) {
			return yield* failRequestCloseout(interruptFence.error);
		}
		if ("stale" in interruptFence) {
			return providerTurnInterruptedWithDiscard();
		}
		const result = providerTurnInterrupted();
		return committedRequestEnd
			? requestEndCommitted(result, "retained")
			: result;
	});
}

function settleCooperativeCancellationEffect(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	processor: ProviderStreamAccumulator,
	source: RuntimeProcessorSource,
	modelRequestStartId: string,
	modelRequestId: string,
	consumedAttachments: readonly RuntimeProviderAttachment[],
	modelUsage: RuntimeUsage | undefined,
	beginSettlementWrites: () => void,
	endSettlementWrites: () => void,
	durableOperations: HotDurableOperationOwner,
	streamState?: ProviderTurnStreamState,
): Effect.Effect<ProviderTurnResult, unknown> {
	return Effect.gen(function* () {
		if (streamState !== undefined) {
			yield* interruptAndJoinToolFibersEffect(streamState);
		}
		yield* Effect.promise(() => durableOperations.awaitIdle());
		const cancelled = yield* Effect.sync(beginSettlementWrites).pipe(
			Effect.andThen(
				Effect.promise(() =>
					durableOperations.run(
						() =>
							processor.cancel(
								source,
								cooperativeCancellationFailure(session.sessionId, source),
							),
						true,
					),
				),
			),
			Effect.ensuring(Effect.sync(endSettlementWrites)),
		);
		yield* Effect.promise(() => durableOperations.awaitIdle());
		if (!cancelled.ok) {
			return yield* failRequestCloseout(cancelled.error);
		}
		processor.discardUncommittedMembers();
		const requestEnd = yield* Effect.promise(() =>
			appendModelRequestEndEvent(
				options,
				session,
				modelRequestId,
				modelRequestStartId,
				true,
				"runtime_interrupted",
				"cancelled",
				modelUsage,
				consumedAttachments,
				requestEndKindForSession(session),
				undefined,
				undefined,
			),
		);
		if (!requestEnd.ok) {
			return yield* failRequestCloseout(requestEnd.error);
		}
		if (requestEnd.type === "stale") {
			return requestEndCommitted(
				providerTurnInterrupted(),
				"discard_hot_state",
			);
		}
		const projectionFailure = applyFailedRequestProviderProjection(session);
		if (projectionFailure !== undefined) {
			return yield* failRequestCloseout(projectionFailure);
		}
		commitProcessorProjectionWithoutStableReasoning(session, processor);
		return requestEndCommitted(providerTurnInterrupted(), "retained");
	});
}

function settleCooperativeCancellationAfterRequestEndEffect(
	session: ThreadRuntime,
	processor: ProviderStreamAccumulator,
	source: RuntimeProcessorSource,
	streamState: ProviderTurnStreamState,
): Effect.Effect<ProviderTurnResult, unknown> {
	return Effect.gen(function* () {
		yield* interruptAndJoinToolFibersEffect(streamState);
		yield* Effect.promise(() => streamState.durableOperations.awaitIdle());
		const cancelled = yield* Effect.promise(() =>
			processor.cancelOpenTools(
				source,
				cooperativeCancellationFailure(session.sessionId, source),
				pendingSandboxExecutionToolUseEventIds(session),
			),
		);
		if (cancelled.type === "failed") {
			return requestEndCommitted(
				providerTurnFailed(
					cancelled.error,
					cancelled.error.type === "message-store"
						? "persistence_failed"
						: "event_write_failed",
				),
				"settled",
			);
		}
		if (cancelled.type === "stale_custody") {
			return requestEndCommitted(
				providerTurnInterruptedWithDiscard(),
				"discard_hot_state",
			);
		}
		commitProcessorProjection(session, processor);
		return requestEndCommitted(providerTurnInterrupted(), "settled");
	});
}

function settleUserInterruptAfterRequestEndEffect(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	processor: ProviderStreamAccumulator,
	streamState: ProviderTurnStreamState,
): Effect.Effect<ProviderTurnResult, unknown> {
	return Effect.gen(function* () {
		yield* interruptAndJoinToolFibersEffect(streamState);
		yield* Effect.promise(() => streamState.durableOperations.awaitIdle());
		session.state.markUserInterruptCloseoutEligible();
		const interruptFence = yield* settleUserInterruptFenceEffect(
			session,
			options,
			processor,
		);
		if (!interruptFence.ok) {
			// Request End already consumed this ride at Bridge; a later control-fence
			// failure cannot put those attachment origins back into the hot queue.
			session.state.settlePendingAttachmentRide();
			return yield* failRequestCloseout(interruptFence.error);
		}
		if ("stale" in interruptFence) {
			return requestEndCommitted(
				providerTurnInterruptedWithDiscard(),
				"discard_hot_state",
			);
		}
		commitProcessorProjection(session, processor);
		return requestEndCommitted(providerTurnInterrupted(), "settled");
	});
}

function settleUserInterruptFenceEffect(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	processor?: ProviderStreamAccumulator,
): Effect.Effect<
	| { readonly ok: true }
	| { readonly ok: true; readonly stale: true }
	| { readonly ok: false; readonly error: RuntimeFailure },
	never
> {
	return Effect.promise(async () => {
		const command = session.state.userInterruptCommand();
		if (command === undefined) {
			return { ok: true };
		}
		if (!session.state.userInterruptCloseoutEligible()) {
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
		if (session.state.userInterruptInputCommitApplied()) {
			return { ok: true };
		}
		const declaration = { inputKind: "interrupt" as const };
		let committed;
		try {
			const application =
				await session.state.commitUserInterruptInput(declaration);
			committed = application.result;
		} catch (error) {
			return {
				ok: false,
				error: normalizeRuntimeFailure({
					type: "runtime",
					code: "runtime_invalid_sequence",
					rawError: error,
					retryable: true,
					fatal: false,
					reason: "runtime_contract_validation",
					sessionId: session.sessionId,
				}),
			};
		}
		if (!committed.ok) {
			return {
				ok: false,
				error: normalizeRuntimeFailure({
					type: "runtime",
					code: "runtime_invalid_sequence",
					retryable: committed.retryable,
					fatal: !committed.retryable,
					reason: "runtime_contract_validation",
					sessionId: session.sessionId,
				}),
			};
		}
		if ("stale" in committed) {
			return { ok: true, stale: true };
		}
		if ("type" in committed) {
			try {
				const pendingTools = [
					...session.state.pendingApprovalToolJobs(),
					...session.state.resolvedToolRouteJobs(),
					...session.state.pendingSandboxExecutionJobs(),
				];
				const hotToolUseEventIds =
					processor?.unfinishedToolUseEventIds() ?? new Set<string>();
				const pendingToolUseEventIds = new Set(
					pendingTools.map((pending) => pending.toolUseEventId),
				);
				const hotResults = committed.interruptToolResults.filter((result) =>
					hotToolUseEventIds.has(result.toolUseEventId),
				);
				const pendingResults = committed.interruptToolResults.filter(
					(result) =>
						!hotToolUseEventIds.has(result.toolUseEventId) &&
						pendingToolUseEventIds.has(result.toolUseEventId),
				);
				if (
					hotResults.length + pendingResults.length !==
					committed.interruptToolResults.length
				) {
					throw new Error("interrupt result has no active Runtime Tool owner");
				}
				processor?.applyInterruptSettlement(command, hotResults);
				session.state.contextManager.appendInterruptToolResults(
					pendingTools.map((pending) => ({
						toolUseEventId: pending.toolUseEventId,
						assistantMessageSequence: pending.assistantMessageSequence,
						modelToolCallId: pending.toolPart.modelToolCallId,
					})),
					pendingResults,
				);
				session.state.addPendingAttachments(committed.pendingAttachments);
				session.state.applyThreadTurnFact({
					fact: "interrupt_committed",
					eventId: command.runtimeInputId,
				});
			} catch (error) {
				return {
					ok: false,
					error: normalizeRuntimeFailure({
						type: "runtime",
						code: "runtime_invalid_sequence",
						rawError: error,
						retryable: false,
						fatal: true,
						reason: "runtime_contract_validation",
						sessionId: session.sessionId,
					}),
				};
			}
			releaseInterruptedPendingTools(
				session,
				options,
				committed.interruptToolResults.map((result) => result.toolUseEventId),
			);
			session.state.markUserInterruptInputCommitApplied();
		}
		return { ok: true };
	});
}

function releaseInterruptedPendingTools(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	toolUseEventIds: readonly string[],
): void {
	for (const toolUseEventId of toolUseEventIds) {
		if (
			session.state
				.pendingApprovalToolJobs()
				.some((pending) => pending.toolUseEventId === toolUseEventId)
		) {
			session.state.removePendingApprovalToolJob(toolUseEventId);
			runtimeMetrics(options).addPendingApprovals(-1);
		}
		session.state.removeResolvedToolRouteJob(toolUseEventId);
		session.state.removePendingSandboxExecutionJob(toolUseEventId);
		session.state.clearThreadToolRoute(toolUseEventId);
	}
}

function interruptAndJoinToolFibersEffect(
	state: ProviderTurnStreamState,
): Effect.Effect<void, never> {
	return interruptAndAwaitFibersWithinBound(state.toolFibers);
}

function interruptAndAwaitFibersWithinBound<A, E>(
	fibers: readonly Fiber.Fiber<A, E>[],
): Effect.Effect<void, never> {
	return Effect.gen(function* () {
		// onInterrupt finalizers are uninterruptible, so each bounded wait runs in
		// its own detached fiber; the finalizer only joins those bounded closers.
		const closers = yield* Effect.forEach(
			fibers,
			(fiber) =>
				Fiber.interrupt(fiber).pipe(
					Effect.timeoutOption(`${ProviderRequestScopeCloseTimeoutMs} millis`),
					Effect.asVoid,
					Effect.forkDetach({ startImmediately: true }),
				),
			{ concurrency: "unbounded" },
		);
		yield* Effect.forEach(closers, (fiber) => Fiber.await(fiber), {
			concurrency: "unbounded",
			discard: true,
		});
	});
}

function completedRunResult(session: ThreadRuntime): ThreadLoopRunResult {
	const messages = session.state.contextManager.entries();
	const projected =
		messages.length === 0 ? undefined : toGatewayProviderContext(messages);
	if (projected !== undefined && !projected.ok) {
		session.state.clear();
		return {
			type: "failed",
			error: projected.error,
			releaseSession: { reason: "crashed" },
		};
	}
	const currentModel = session.state.currentModel();
	return {
		type: "completed",
		modelMessageCount:
			projected !== undefined && projected.ok ? projected.context.length : 0,
		...(currentModel !== undefined ? { currentModel } : {}),
	};
}

function completedHotStateRunResult(
	session: ThreadRuntime,
): ThreadLoopRunResult {
	const currentModel = session.state.currentModel();
	return {
		type: "completed",
		modelMessageCount: session.state.contextManager.entries().length,
		...(currentModel !== undefined ? { currentModel } : {}),
	};
}

async function handleProcessorFailure(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	failure: RuntimeFailure,
): Promise<{
	readonly type: "failed";
	readonly error: RuntimeFailure;
	readonly failureEventId?: string;
	readonly releaseSession?: { readonly reason: ThreadLoopSessionReleaseReason };
}> {
	const terminalAppend = await appendTerminalEventsBestEffort(
		options,
		session,
		failure,
	);
	const settledFailure = terminalAppend.ok
		? terminalAppend.settledFailure
		: failure;
	const durableFailure =
		terminalAppend.ok && terminalAppend.failureEventId !== undefined
			? { failureEventId: terminalAppend.failureEventId }
			: {};
	if (failure.type === "message-store") {
		session.state.clear();
		return {
			type: "failed",
			error: settledFailure,
			...durableFailure,
			releaseSession: { reason: "persistence_failed" },
		};
	}
	if (failure.type === "session-event-writer") {
		return {
			type: "failed",
			error: settledFailure,
			...durableFailure,
			releaseSession: { reason: "event_write_failed" },
		};
	}
	if (failure.type === "runtime") {
		session.state.clear();
		return {
			type: "failed",
			error: settledFailure,
			...durableFailure,
			releaseSession: { reason: "crashed" },
		};
	}
	return { type: "failed", error: settledFailure, ...durableFailure };
}

async function closeStartedRequestAfterProcessorFailure(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	processor: ProviderStreamAccumulator,
	modelRequestId: string,
	modelRequestStartId: string,
	failure: RuntimeFailure,
	usage: RuntimeUsage | undefined,
	consumedAttachments: readonly RuntimeProviderAttachment[],
	requestKind: NonNullable<SessionEventEnvelope["requestKind"]> | undefined,
): Promise<ProviderTurnResult> {
	processor.discardUncommittedMembers();
	const requestEnd = await appendModelRequestEndEvent(
		options,
		session,
		modelRequestId,
		modelRequestStartId,
		true,
		requestErrorKindFromFailure(failure),
		"error",
		usage,
		consumedAttachments,
		requestKind,
		undefined,
		undefined,
	);
	if (!requestEnd.ok) {
		return providerTurnFailed(requestEnd.error, "event_write_failed");
	}
	if (requestEnd.type === "stale") {
		return requestEndCommitted(providerTurnInterrupted(), "discard_hot_state");
	}
	const projectionFailure = applyFailedRequestProviderProjection(session);
	if (projectionFailure !== undefined) {
		return requestEndCommitted(
			providerTurnFailed(projectionFailure, "event_write_failed"),
		);
	}
	commitProcessorProjectionWithoutStableReasoning(session, processor);
	return requestEndCommitted(
		await handleProcessorFailure(session, options, failure),
	);
}

async function handleContextLoaderFailure(
	options: ThreadLoopRuntimeOptions,
	session: ThreadRuntime,
	error: unknown,
): Promise<ThreadLoopRunResult> {
	const sessionId = session.sessionId;
	const parsed = ContextLoaderErrorSchema.safeParse(error);
	const loaderError = parsed.success
		? parsed.data
		: normalizeContextLoaderError({
				code: "unknown",
				rawError: error,
				sessionId,
			});
	const failure = normalizeRuntimeFailure({
		type: "runtime",
		code: "runtime_invalid_sequence",
		retryable: loaderError.retryable,
		fatal: loaderError.fatal,
		reason: "runtime_contract_validation",
		retryStatus: { type: "exhausted" },
		sessionId,
	});
	const terminalAppend = await appendTerminalEventsBestEffort(
		options,
		session,
		failure,
	);
	if (!terminalAppend.ok) {
		return {
			type: "failed",
			error: terminalAppend.error,
			releaseSession: { reason: "event_write_failed" },
		};
	}
	return {
		type: "failed",
		error: terminalAppend.settledFailure,
		...(terminalAppend.failureEventId === undefined
			? {}
			: { failureEventId: terminalAppend.failureEventId }),
	};
}

async function appendTerminalEventsBestEffort(
	options: ThreadLoopRuntimeOptions,
	session: ThreadRuntime,
	failure: RuntimeFailure,
): Promise<
	| {
			readonly ok: true;
			readonly settledFailure: RuntimeFailure;
			readonly failureEventId?: string;
	  }
	| { readonly ok: false; readonly error: RuntimeFailure }
> {
	if (isRuntimeTerminationFailure(failure)) {
		return { ok: true, settledFailure: failure };
	}
	const settledFailure =
		failure.retryStatus?.type === "terminal"
			? failure
			: compactionFailureWithRetryStatus(failure, { type: "exhausted" });
	const errorAppend = await appendEvent(options, session, {
		type: "session.error",
		error: settledFailure,
	});
	if (!errorAppend.ok) {
		return {
			ok: false,
			error: runtimeFailureFromEventWriter(errorAppend.error),
		};
	}
	if (errorAppend.type === "stale") {
		return {
			ok: false,
			error: runtimeFailureFromEventWriter(
				normalizeSessionEventWriterError({
					code: "superseded",
					sessionId: session.sessionId,
				}),
			),
		};
	}
	return { ok: true, settledFailure, failureEventId: errorAppend.eventId };
}

async function appendModelRequestEndEvent(
	options: ThreadLoopRuntimeOptions,
	session: ThreadRuntime,
	modelRequestId: string,
	modelRequestStartId: string,
	isError: boolean,
	errorKind: RuntimeRequestErrorKind | undefined,
	finishReason: RuntimeFinishReason,
	usage: RuntimeUsage | undefined,
	consumedAttachments: readonly RuntimeProviderAttachment[] = [],
	requestKind?:
		| "agent_provider_request"
		| "compaction_summary"
		| "approval_reviewer",
	reschedule?: NonNullable<SessionEventWriterRequestEndEnvelope["reschedule"]> & {
		readonly providerAttempts: number;
		readonly compactionAttempts: number;
	},
	trailingContextAppend?: RuntimeAssistantContextAppend,
	compaction?: {
		readonly context: { readonly parts: readonly RuntimeContextPart[] };
		readonly compactedThroughMessageSequence: number;
		readonly compactionEventPayloadJson: string;
		readonly prefixConsumption?: NonNullable<
			SessionEventWriterRequestEndEnvelope["prefixConsumption"]
		>;
	},
	interrupt?: {
		readonly command: NonNullable<
			ReturnType<ThreadRuntime["state"]["userInterruptCommand"]>
		>;
		readonly interruptLeaseRef: NonNullable<
			SessionEventWriterRequestEndEnvelope["interruptSettlement"]
		>["interruptLeaseRef"];
	},
): Promise<
	| {
			readonly ok: true;
			readonly type: "committed" | "duplicate";
			readonly requestEndEventId: string;
			readonly outcome: Extract<
				SessionEventWriterRequestEndResult,
				{ readonly ok: true; readonly type: "committed" | "duplicate" }
			>["outcome"];
			readonly providerContextRetention: SessionEventWriterRequestEndEnvelope["providerContextRetention"];
			readonly interruptToolResults: readonly RuntimeInterruptToolResult[];
			readonly assistantSeal: {
				readonly status: "completed" | "failed";
				readonly finishReason: RuntimeFinishReason;
				readonly usage?: RuntimeUsage | undefined;
			};
	  }
	| {
			readonly ok: true;
			readonly type: "stale";
			readonly interruptToolResults: readonly [];
			readonly assistantSeal: {
				readonly status: "completed" | "failed";
				readonly finishReason: RuntimeFinishReason;
				readonly usage?: RuntimeUsage | undefined;
			};
	  }
	| { readonly ok: false; readonly error: RuntimeFailure }
> {
	const writeId = options.runtime.createId("event_write");
	const activeRequest = session.state.threadTurnTransition().checkpoint.request;
	if (
		activeRequest === undefined ||
		activeRequest.modelRequestId !== modelRequestId ||
		activeRequest.requestStartEventId !== modelRequestStartId ||
		(requestKind !== undefined && activeRequest.requestKind !== requestKind)
	) {
		return {
			ok: false,
			error: normalizeRuntimeFailure({
				type: "runtime",
				code: "runtime_invalid_sequence",
				retryable: false,
				fatal: true,
				reason: "runtime_contract_validation",
				sessionId: session.sessionId,
			}),
		};
	}
	const servedAttachments = !isError ? consumedAttachments : [];
	const consumedAttachmentRefs = servedAttachments.flatMap((attachment) =>
		attachment.transient === undefined
			? []
			: [attachment.transient.attachmentRef],
	);
	const providerContextRetention = providerContextRetentionForRequest(
		session,
			compaction !== undefined
				? ("compacted" as const)
				: interrupt !== undefined
					? ("interrupted" as const)
					: reschedule !== undefined
						? ("rescheduled" as const)
						: isError
							? ("failed" as const)
							: ("completed" as const),
	);
	const envelope: SessionEventWriterRequestEndEnvelope = {
		workspaceId: session.identity.workspaceId,
		sessionId: session.sessionId,
		sessionThreadId: session.identity.sessionThreadId,
		bindingId: session.identity.bindingId,
		bindingGeneration: session.identity.bindingGeneration,
		targetPodUid: session.identity.targetPodUid,
		writeId,
		modelRequestId,
		providerContextRetention,
		isError,
		...(errorKind !== undefined ? { errorKind } : {}),
		finishReason,
		...(usage !== undefined ? { usage } : {}),
		...(consumedAttachmentRefs.length > 0
			? { consumedAttachmentRefs: [...consumedAttachmentRefs] }
			: {}),
		...(reschedule !== undefined
			? {
					reschedule: {
						attempt: reschedule.attempt,
						backoffMs: reschedule.backoffMs,
						deadline: reschedule.deadline,
					},
				}
			: {}),
		...(trailingContextAppend === undefined ? {} : { trailingContextAppend }),
		...(compaction === undefined
			? {}
			: {
					compactedThroughMessageSequence:
						compaction.compactedThroughMessageSequence,
					compactionEventPayloadJson: compaction.compactionEventPayloadJson,
					compactionContext: { parts: [...compaction.context.parts] },
					...(compaction.prefixConsumption === undefined
						? {}
						: { prefixConsumption: compaction.prefixConsumption }),
				}),
		...(interrupt === undefined
			? {}
			: {
					interruptSettlement: {
						runtimeInputId: interrupt.command.runtimeInputId,
						interruptLeaseRef: interrupt.interruptLeaseRef,
					},
				}),
	};
	const result = await writeRequestEndWithRetry(options, envelope);
	if (!result.ok) {
		return { ok: false, error: runtimeFailureFromEventWriter(result.error) };
	}
	if (result.type !== "stale" && interrupt === undefined) {
		const acceptedReschedule =
			result.outcome.type === "rescheduled" && reschedule !== undefined
				? {
						attempt: reschedule.attempt,
						effectiveDeadline: result.outcome.effectiveDeadline,
						providerAttempts: reschedule.providerAttempts,
						compactionAttempts: reschedule.compactionAttempts,
					}
				: undefined;
		const requestSealFailure = applyCommittedRequestEnd(
			session,
			result.requestEndEventId,
			modelRequestId,
			isError,
			errorKind,
			providerContextRetention,
			acceptedReschedule,
		);
		if (requestSealFailure !== undefined) {
			return { ok: false, error: requestSealFailure };
		}
	}
	const assistantSeal = {
		status:
			isError || reschedule !== undefined
				? ("failed" as const)
				: ("completed" as const),
		finishReason,
		...(usage === undefined ? {} : { usage }),
	};
	if (result.type === "stale") {
		return {
			ok: true,
			type: "stale",
			interruptToolResults: [],
			assistantSeal,
		};
	}
	return {
		ok: true,
		type: result.type,
		requestEndEventId: result.requestEndEventId,
		outcome: result.outcome,
		providerContextRetention,
		interruptToolResults: result.interruptToolResults,
		assistantSeal,
	};
}

function requestEndOutcome(
	result: Extract<
		Awaited<ReturnType<typeof appendModelRequestEndEvent>>,
		{ readonly ok: true }
	>,
):
	| { readonly type: "stale" }
	| Extract<
			SessionEventWriterRequestEndResult,
			{ readonly ok: true; readonly type: "committed" | "duplicate" }
	  >["outcome"] {
	return result.type === "stale" ? { type: "stale" } : result.outcome;
}

/** Applies the reducer half of a durable Request End after all co-committed Tool facts are projected. */
function applyCommittedRequestEnd(
	session: ThreadRuntime,
	eventId: string,
	modelRequestId: string,
	isError: boolean,
	errorKind: RuntimeRequestErrorKind | undefined,
	providerContextRetention: SessionEventWriterRequestEndEnvelope["providerContextRetention"],
	reschedule:
		| {
				readonly attempt: number;
				readonly effectiveDeadline: string;
				readonly providerAttempts: number;
				readonly compactionAttempts: number;
		  }
		| undefined,
): RuntimeFailure | undefined {
	try {
		const requestEndTransition = session.state.applyThreadTurnFact({
			fact: "request_ended",
			eventId,
			modelRequestId,
			isError,
			...(errorKind !== undefined ? { errorKind } : {}),
			providerContextRetention,
			...(reschedule !== undefined ? { reschedule } : {}),
		});
		if (requestEndTransition.dispatch !== undefined) {
			throw new Error("Request End must not produce a dispatch");
		}
		return undefined;
	} catch (error) {
		return normalizeRuntimeFailure({
			type: "runtime",
			code: "runtime_invalid_sequence",
			rawError: error,
			retryable: false,
			fatal: true,
			reason: "runtime_contract_validation",
			sessionId: session.sessionId,
		});
	}
}

function providerContextRetentionForRequest(
	session: ThreadRuntime,
	disposition: SessionEventWriterRequestEndEnvelope["providerContextRetention"]["disposition"],
): SessionEventWriterRequestEndEnvelope["providerContextRetention"] {
	const request = session.state.threadTurnTransition().checkpoint.request;
	if (request === undefined) {
		throw new Error("provider-context retention requires an active request");
	}
	const draft = session.state.contextManager.openRequestDraft();
	const toolUseEventIds = request.toolMembers.flatMap((member) =>
		member.memberKind === "public_tool_use" ? [member.toolUseEventId] : [],
	);
	const repairEventIds = request.toolMembers.flatMap((member) =>
		member.memberKind === "internal_tool_repair" ? [member.repairEventId] : [],
	);
	const retainsAssistantOwner =
		disposition === "completed" ||
		disposition === "compacted" ||
		toolUseEventIds.length !== 0 ||
		repairEventIds.length !== 0;
	return {
		disposition,
		...(retainsAssistantOwner && draft?.modelRequestId === request.modelRequestId
			? { assistantMessageSequence: draft.messageSequence }
			: {}),
		toolUseEventIds,
		repairEventIds,
	};
}

async function writeRequestEndWithRetry(
	options: ThreadLoopRuntimeOptions,
	envelope: SessionEventWriterRequestEndEnvelope,
): Promise<SessionEventWriterRequestEndResult> {
	if (options.sessionEventWriter.writeRequestEnd === undefined) {
		return {
			ok: false,
			error: normalizeSessionEventWriterError({
				code: "unavailable",
				sessionId: envelope.sessionId,
				writeId: envelope.writeId,
			}),
		};
	}
	let lastFailure: SessionEventWriterRequestEndResult | undefined;
	for (
		let attempt = 1;
		attempt <= SessionEventWriterRetryPolicy.attempts;
		attempt += 1
	) {
		let result: SessionEventWriterRequestEndResult;
		try {
			result = await observeEventWriter(options, "write_request_end", () =>
				options.sessionEventWriter.writeRequestEnd(envelope),
			);
		} catch (error) {
			result = {
				ok: false,
				error: normalizeSessionEventWriterError({
					code: "unknown",
					rawError: error,
					sessionId: envelope.sessionId,
					writeId: envelope.writeId,
				}),
			};
		}
		if (result.ok) return result;
		lastFailure = result;
		if (
			!result.error.retryable ||
			attempt === SessionEventWriterRetryPolicy.attempts
		) {
			return result;
		}
		const backoffMs = SessionEventWriterRetryPolicy.backoffMs[attempt - 1] ?? 0;
		if (backoffMs > 0) {
			await options.runtime.sleep(backoffMs, new AbortController().signal);
		}
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

async function commitInternalToolRepairStable(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	repair: RuntimeInternalToolRepairCommit,
	_source: RuntimeProcessorSource,
	signal: AbortSignal = new AbortController().signal,
): Promise<RuntimeInternalToolRepairCommitResult> {
	return await options.internalToolRepairStore.commitInternalToolRepair(
		repair,
		storeControls(options, signal),
	);
}

async function appendEvent(
	options: ThreadLoopRuntimeOptions,
	session: ThreadRuntime,
	event: SessionEventWriterAppendEvent,
	declaration?: {
		readonly assistantContextAppend: RuntimeAssistantContextAppend;
		readonly distinctProviderInput?: RuntimeJsonValue | undefined;
		readonly toolRouteCapability?: SessionEventEnvelope["toolRouteCapability"];
	},
	modelRequestId?: string,
	requestStart?: {
		readonly contextThroughMessageSequence: number;
		readonly requestKind:
			| "agent_provider_request"
			| "compaction_summary"
			| "approval_reviewer";
		readonly consumedFileAttachments?:
			| SessionEventEnvelope["consumedFileAttachments"]
			| undefined;
	},
): Promise<SessionEventWriterAppendResult> {
	const writeId = options.runtime.createId("event_write");
	return await appendEventWithWriteId(
		options,
		session,
		writeId,
		event,
		declaration,
		modelRequestId,
		requestStart,
	);
}

async function appendProcessorEvent(
	options: ThreadLoopRuntimeOptions,
	session: ThreadRuntime,
	event: SessionEventWriterAppendEvent,
	declaration?: {
		readonly assistantContextAppend: RuntimeAssistantContextAppend;
		readonly distinctProviderInput?: RuntimeJsonValue | undefined;
		readonly toolRouteCapability?: SessionEventEnvelope["toolRouteCapability"];
	},
	modelRequestId?: string,
): Promise<SessionEventWriterAppendResult> {
	if (event.type === "agent.tool_use" || event.type === "agent.mcp_tool_use") {
		return await appendRetriedEvent(
			options,
			session,
			event,
			declaration,
			modelRequestId,
		);
	}
	return await appendEvent(
		options,
		session,
		event,
		declaration,
		modelRequestId,
	);
}

async function appendRetriedEvent(
	options: ThreadLoopRuntimeOptions,
	session: ThreadRuntime,
	event: SessionEventWriterAppendEvent,
	declaration?: {
		readonly assistantContextAppend: RuntimeAssistantContextAppend;
		readonly distinctProviderInput?: RuntimeJsonValue | undefined;
		readonly toolRouteCapability?: SessionEventEnvelope["toolRouteCapability"];
	},
	modelRequestId?: string,
	requestStart?: {
		readonly contextThroughMessageSequence: number;
		readonly requestKind:
			| "agent_provider_request"
			| "compaction_summary"
			| "approval_reviewer";
		readonly consumedFileAttachments?:
			| SessionEventEnvelope["consumedFileAttachments"]
			| undefined;
	},
): Promise<SessionEventWriterAppendResult> {
	const writeId = options.runtime.createId("event_write");
	return await appendEventWithRetry(
		options,
		session,
		writeId,
		event,
		declaration,
		modelRequestId,
		requestStart,
	);
}

async function appendRunningEvent(
	options: ThreadLoopRuntimeOptions,
	session: ThreadRuntime,
	custody: ThreadLoopRunCustody,
	openingSourceKind: string,
	openingSourceId: string,
): Promise<
	| {
			readonly ok: true;
			readonly type: "committed" | "duplicate";
			readonly eventId: string;
	  }
	| { readonly ok: false; readonly error: SessionEventWriterError }
> {
	const existingDurableTurnId = custody.activeTurnId(session);
	if (existingDurableTurnId !== undefined) {
		if (
			session.state.threadTurnTransition().checkpoint.executionRunId !==
			existingDurableTurnId
		) {
			session.state.applyThreadTurnFact({
				fact: "run_opened",
				eventId: existingDurableTurnId,
			});
		}
		return { ok: true, type: "duplicate", eventId: existingDurableTurnId };
	}
	const writeId = runtimeTurnOpenWriteId({
		workspaceId: session.identity.workspaceId,
		sessionId: session.sessionId,
		sessionThreadId: session.identity.sessionThreadId,
		openingSourceKind,
		openingSourceId,
	});
	const result = await appendEventWithRetry(options, session, writeId, {
		type: "session.status_running",
	});
	if (!result.ok) {
		return result;
	}
	if (result.type === "stale") {
		return {
			ok: false,
			error: normalizeSessionEventWriterError({
				code: "superseded",
				sessionId: session.sessionId,
				writeId,
			}),
		};
	}
	if (
		session.state.threadTurnTransition().checkpoint.executionRunId !==
		result.eventId
	) {
		session.state.applyThreadTurnFact({
			fact: "run_opened",
			eventId: result.eventId,
		});
	}
	return result;
}

async function appendEventWithRetry(
	options: ThreadLoopRuntimeOptions,
	session: ThreadRuntime,
	writeId: string,
	event: SessionEventWriterAppendEvent,
	declaration?: {
		readonly assistantContextAppend: RuntimeAssistantContextAppend;
		readonly distinctProviderInput?: RuntimeJsonValue | undefined;
		readonly toolRouteCapability?: SessionEventEnvelope["toolRouteCapability"];
	},
	modelRequestId?: string,
	requestStart?: {
		readonly contextThroughMessageSequence: number;
		readonly requestKind:
			| "agent_provider_request"
			| "compaction_summary"
			| "approval_reviewer";
		readonly consumedFileAttachments?:
			| SessionEventEnvelope["consumedFileAttachments"]
			| undefined;
	},
): Promise<SessionEventWriterAppendResult> {
	let lastFailure: SessionEventWriterAppendResult | undefined;
	for (
		let attempt = 1;
		attempt <= SessionEventWriterRetryPolicy.attempts;
		attempt += 1
	) {
		const result = await appendEventWithWriteId(
			options,
			session,
			writeId,
			event,
			declaration,
			modelRequestId,
			requestStart,
		);
		if (result.ok) return result;
		lastFailure = result;
		if (
			!result.error.retryable ||
			attempt === SessionEventWriterRetryPolicy.attempts
		) {
			return result;
		}
		const backoffMs = SessionEventWriterRetryPolicy.backoffMs[attempt - 1] ?? 0;
		if (backoffMs > 0) {
			await options.runtime.sleep(backoffMs, new AbortController().signal);
		}
	}
	return (
		lastFailure ?? {
			ok: false,
			error: normalizeSessionEventWriterError({
				code: "unknown",
				sessionId: session.sessionId,
				writeId,
			}),
		}
	);
}

async function appendEventWithWriteId(
	options: ThreadLoopRuntimeOptions,
	session: ThreadRuntime,
	writeId: string,
	event: SessionEventWriterAppendEvent,
	declaration?: {
		readonly assistantContextAppend: RuntimeAssistantContextAppend;
		readonly distinctProviderInput?: RuntimeJsonValue | undefined;
		readonly toolRouteCapability?: SessionEventEnvelope["toolRouteCapability"];
	},
	modelRequestId?: string,
	requestStart?: {
		readonly contextThroughMessageSequence: number;
		readonly requestKind:
			| "agent_provider_request"
			| "compaction_summary"
			| "approval_reviewer";
		readonly consumedFileAttachments?:
			| SessionEventEnvelope["consumedFileAttachments"]
			| undefined;
	},
): Promise<SessionEventWriterAppendResult> {
	const startedAt = options.runtime.monotonicMs();
	try {
		const result = await options.sessionEventWriter.append({
			workspaceId: session.identity.workspaceId,
			sessionId: session.sessionId,
			sessionThreadId: session.identity.sessionThreadId,
			bindingId: session.identity.bindingId,
			bindingGeneration: session.identity.bindingGeneration,
			targetPodUid: session.identity.targetPodUid,
			writeId,
			event,
			...(modelRequestId !== undefined ? { modelRequestId } : {}),
			...(declaration ?? {}),
			...(requestStart !== undefined ? requestStart : {}),
		});
		runtimeMetrics(options).observeEventWriteLatency(
			"append",
			options.runtime.monotonicMs() - startedAt,
			result.ok ? "success" : "error",
		);
		return result;
	} catch (error) {
		runtimeMetrics(options).observeEventWriteLatency(
			"append",
			options.runtime.monotonicMs() - startedAt,
			"error",
		);
		return {
			ok: false,
			error: normalizeSessionEventWriterError({
				code: "unknown",
				rawError: error,
				sessionId: session.sessionId,
				writeId,
			}),
		};
	}
}

function storeControls(
	options: ThreadLoopRuntimeOptions,
	signal: AbortSignal,
): RuntimeDeclarationOperationControls {
	return {
		signal,
		timeoutMs: options.storeOperationTimeoutMs,
		sleep: options.runtime.sleep,
	};
}

function commitProcessorProjection(
	session: ThreadRuntime,
	processor: ProviderStreamAccumulator,
): void {
	if (!processor.ownsContext(session.state.contextManager)) {
		throw new Error(
			"provider stream accumulator lost its durable context owner",
		);
	}
}

function commitProcessorProjectionWithoutStableReasoning(
	session: ThreadRuntime,
	processor: ProviderStreamAccumulator,
): void {
	commitProcessorProjection(session, processor);
}

function runtimeFailureFromStore(
	error: RuntimeInternalToolRepairStoreError,
): RuntimeFailure {
	return normalizeRuntimeFailure({
		type: "message-store",
		code: error.code,
		retryable: error.retryable,
		fatal: error.fatal,
		operation: error.operation,
		reason: error.reason,
		sessionId: error.sessionId,
	});
}

function terminalFailureFromProcessorResult(
	result: ProviderStreamAccumulatorResult,
): RuntimeFailure | undefined {
	if (!result.ok) {
		return undefined;
	}
	if ("type" in result && result.type === "stale_custody") {
		return undefined;
	}
	const sessionErrorEvent = result.events.find(
		(event) => event.type === "session.error",
	);
	if (sessionErrorEvent?.type !== "session.error") {
		return undefined;
	}
	if (!("code" in sessionErrorEvent.error)) {
		return undefined;
	}
	return sessionErrorEvent.error;
}

function runtimeFailureFromLlmService(error: unknown): LLMRuntimeFailure {
	if (
		typeof error === "object" &&
		error !== null &&
		"type" in error &&
		error.type === "llm-service" &&
		"error" in error
	) {
		return (error as LLMServiceError).error;
	}
	return LLMRuntimeFailureSchema.parse(
		normalizeRuntimeFailure({
			type: "provider",
			code: "provider_unknown",
			retryable: false,
			fatal: true,
		}),
	);
}

function runtimeShutdownFailure(
	sessionId: string,
	source: RuntimeProcessorSource,
): RuntimeFailure {
	return normalizeRuntimeFailure({
		type: "runtime",
		code: "runtime_invalid_sequence",
		retryable: false,
		fatal: false,
		reason: "runtime_shutdown",
		retryStatus: { type: "terminal" },
		sessionId,
		providerId: source.providerId,
		modelId: source.modelId,
	});
}

function applyJoinedInterruptRequestEnd(
	session: ThreadRuntime,
	result: Extract<
		Awaited<ReturnType<typeof appendModelRequestEndEvent>>,
		{ readonly ok: true }
	>,
): boolean {
	const command = session.state.userInterruptCommand();
	if (command === undefined) {
		return false;
	}
	if (result.type === "stale") return false;
	const request = session.state.threadTurnTransition().checkpoint.request;
	if (request === undefined) {
		return false;
	}
	try {
		const pendingTools = [
			...session.state.pendingApprovalToolJobs(),
			...session.state.resolvedToolRouteJobs(),
			...session.state.pendingSandboxExecutionJobs(),
		];
		session.state.contextManager.appendInterruptToolResults(
			pendingTools.map((pending) => ({
				toolUseEventId: pending.toolUseEventId,
				assistantMessageSequence: pending.assistantMessageSequence,
				modelToolCallId: pending.toolPart.modelToolCallId,
			})),
			result.interruptToolResults,
		);
		for (const settlement of result.interruptToolResults) {
			session.state.applyThreadTurnFact({
				fact: "tool_result_committed",
				toolUseEventId: settlement.toolUseEventId,
				outcome: settlement.result.type,
			});
			session.state.clearThreadToolRoute(settlement.toolUseEventId);
		}
		const openDraft = session.state.contextManager.openRequestDraft();
		if (openDraft !== undefined) {
			if (
				result.outcome.type !== "ordinary" ||
				result.outcome.sealedMessageSequence !== openDraft.messageSequence
			) {
				return false;
			}
			session.state.contextManager.sealOpenRequestDraft();
		}
	} catch {
		return false;
	}
	session.state.applyThreadTurnFact({
		fact: "interrupt_committed",
		eventId: command.runtimeInputId,
	});
	const requestSealFailure = applyCommittedRequestEnd(
		session,
		result.requestEndEventId,
		request.modelRequestId,
		true,
		"runtime_interrupted",
		result.providerContextRetention,
		undefined,
	);
	if (requestSealFailure !== undefined) {
		return false;
	}
	const projectionFailure = applyFailedRequestProviderProjection(session);
	if (projectionFailure !== undefined) {
		return false;
	}
	return session.state.recordJoinedUserInterruptResult(
		command.runtimeInputId,
		{ ok: true, joined: true },
		{ inputKind: "interrupt" },
	);
}

function joinedInterruptRequestEndFailure(session: ThreadRuntime): RuntimeFailure {
	return normalizeRuntimeFailure({
		type: "runtime",
		code: "runtime_invalid_sequence",
		retryable: false,
		fatal: true,
		reason: "runtime_contract_validation",
		sessionId: session.sessionId,
	});
}

function userInterruptFailure(
	sessionId: string,
	source: RuntimeProcessorSource,
): RuntimeFailure {
	return normalizeRuntimeFailure({
		type: "runtime",
		code: "runtime_invalid_sequence",
		retryable: false,
		fatal: false,
		reason: "aborted",
		retryStatus: { type: "terminal" },
		sessionId,
		providerId: source.providerId,
		modelId: source.modelId,
	});
}

function providerRescheduleInterruptFailure(
	sessionId: string,
	source: RuntimeProcessorSource,
): RuntimeFailure {
	return normalizeRuntimeFailure({
		type: "runtime",
		code: "runtime_invalid_sequence",
		retryable: false,
		fatal: false,
		reason: "aborted",
		retryStatus: { type: "terminal" },
		sessionId,
		providerId: source.providerId,
		modelId: source.modelId,
	});
}

function cooperativeCancellationFailure(
	sessionId: string,
	source: RuntimeProcessorSource,
): RuntimeFailure {
	return normalizeRuntimeFailure({
		type: "runtime",
		code: "runtime_invalid_sequence",
		retryable: false,
		fatal: false,
		reason: "aborted",
		retryStatus: { type: "terminal" },
		sessionId,
		providerId: source.providerId,
		modelId: source.modelId,
	});
}

function effectFromAbortablePromise<A>(
	run: (signal: AbortSignal) => Promise<A>,
): Effect.Effect<A, unknown> {
	return Effect.callback<A, unknown>((resume, signal) => {
		run(signal).then(
			(value) => resume(Effect.succeed(value)),
			(error) => {
				if (!signal.aborted) {
					resume(Effect.fail(error));
				}
			},
		);
	});
}

function exitFailure<A, E>(exit: Exit.Exit<A, E>): E | undefined {
	const failure = Exit.findErrorOption(exit);
	return Option.isSome(failure) ? failure.value : undefined;
}

function seedRuntimeModel(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
): void {
	const model = options.runtimeModel?.(session);
	if (model !== undefined) {
		session.state.updateCurrentModel(model);
	}
}
