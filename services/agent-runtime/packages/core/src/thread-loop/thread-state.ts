/**
 * @packageDocumentation
 * Thread-local hot ThreadState for one ThreadEntry: the in-pod working state a
 * ThreadRun mutates. Not the source of truth; recoverable from durable state.
 * SessionManager, ThreadLoop, and tool execution call it; it delegates hot message
 * list changes to ContextManager, performs ordinary in-memory transitions, and owns
 * the injected interrupt-input commit state and hot agent-mail delivery dedupe.
 *
 * OWNS (hot, per-thread):
 *   - accepted-input queue and the in-flight commit marker;
 *   - tool confirmations, pending-approval ToolJobs, task notifications, background
 *     tool handles;
 *   - runtime config patch and installed MCP manifest patches;
 *   - pending file-backed user media and tool-result transient attachments plus the
 *     single active ride;
 *   - the last-request usage hint and held route-effective model limits;
 *   - interrupt / cooperative-cancel / runtime-shutdown controllers.
 *
 * STATE MACHINE (provider attachment ride):
 *   | state   | meaning                              | writer                      | legal transitions         |
 *   | ------- | ------------------------------------ | --------------------------- | ------------------------- |
 *   | pending | media queued for the next request    | addPendingAttachments       | pending -> riding         |
 *   | riding  | media attached to the active request | beginPendingAttachmentRide  | riding -> settled;        |
 *   |         |                                      |                             | riding retained on retry  |
 *   | riding  | file media commits at Request Start  | consumeFileBackedAttachmentRide | transient remains |
 *   | settled | transient consumed by request-end    | settlePendingAttachmentRide | terminal for that media   |
 *   A durable Request Start consumes only the exact file-backed pairs before Provider
 *   dispatch. Error request-ends retain transient media for retry. Attachments admitted
 *   during a ride remain queued separately for the next request.
 *
 * INVARIANTS:
 *   - One-ride media: file-backed user media becomes at-most-once at durable Request
 *     Start, while tool-result transient media remains on the ride until successful
 *     Request End. Rejected origins stay excluded from same-turn reassembly. Later
 *     turns keep only durable context projection (context-projection.ts).
 *   - The approval-reviewer model is platform runtime config: clients never choose it,
 *     Runtime Core sets it on the provider invocation, and Gateway injects credentials but
 *     never chooses or replaces it.
 *   - Hot state is not the source of truth and stays recoverable from durable state.
 *
 * UPDATE-WITH: services/agent-runtime/packages/core/src/runtime/context-projection.ts,
 *              services/agent-runtime/packages/core/src/thread-loop/thread-loop.ts,
 *              services/agent-runtime/packages/core/src/session/session-manager.ts
 */

import type {
	RuntimeAssistantDraftPart,
	RuntimeContextEntry,
	RuntimeFailure,
	RuntimeInterruptToolResult,
	RuntimeJsonValue,
	RuntimeOpenRequestDraft,
	RuntimeProcessorSource,
	RuntimeProviderAttachment,
	RuntimeUsage,
} from "../contracts/runtime.js";
import type { RuntimeModelLimits } from "../llm/llm-event.js";
import type { ThreadContextPrefix } from "../session/context-manager.js";
import { ContextManager } from "../session/context-manager.js";
import type { RuntimeConfigurationPatch } from "../session/session-configuration.js";
import type { ToolEntry } from "../tools/tool-catalog.js";
import type { ToolJob } from "../tools/tool-scheduler.js";
import type {
	ThreadToolRouteView,
	ThreadTurnCheckpoint,
} from "./thread-turn-checkpoint.js";
import type {
	ThreadActiveInputView,
	ThreadTurnFact,
	ThreadTurnTransition,
} from "./thread-turn-reducer.js";
import {
	deriveThreadTurnSnapshot,
	initializeThreadTurnTransition,
	reduceThreadTurn,
	ThreadTurnContractError,
} from "./thread-turn-reducer.js";

/** Combined cap for file-backed and transient attachments on one provider request. */
export const MaxProviderAttachments = 32;

/** Current provider/model selection held by a hot session thread. */
export interface SessionCurrentModel {
	readonly providerId: string;
	readonly modelId: string;
}

/** Identity and binding fence shared by every command that addresses hot thread state. */
export interface RuntimeThreadAddressState {
	readonly workspaceId: string;
	readonly sessionId: string;
	readonly sessionThreadId: string;
	readonly bindingId: string;
	readonly bindingGeneration: number;
	readonly targetPodUid: string;
}

/** Method-specific Runtime Pod input identity with its sole interrupt-order fact. */
export interface RuntimeAcceptedInputScopeState
	extends RuntimeThreadAddressState {
	readonly runtimeInputId: string;
	readonly inputOrder: number;
}

/** Method-specific control input identity; control commands are not queued accepted inputs. */
export interface RuntimeControlInputState extends RuntimeThreadAddressState {
	readonly runtimeInputId: string;
}

/** Interrupt identity retained by hot control state without Queue transport authority. */
export interface RuntimeInterruptCommandState extends RuntimeControlInputState {
	readonly origin: "user" | "agent";
}

export interface RuntimeControlInputDeclaration {
	readonly inputKind: "interrupt" | "tool_confirmation";
}

export type RuntimeControlInputCommitResult =
	| {
			readonly ok: true;
			readonly stale: true;
			readonly barrierStale?: true | undefined;
	  }
	| { readonly ok: true; readonly joined: true }
	| {
			readonly ok: true;
			readonly type: "committed";
			readonly assignedContextSequences: readonly number[];
			readonly pendingAttachments: readonly RuntimeProviderAttachment[];
			readonly interruptToolResults: readonly RuntimeInterruptToolResult[];
	  }
	| {
			readonly ok: false;
			readonly retryable: boolean;
			readonly errorCode: string | number;
	  };

export type RuntimeControlInputCommit = (
	declaration: RuntimeControlInputDeclaration,
) => Promise<RuntimeControlInputCommitResult>;

export interface RuntimeControlInputCommitApplication {
	readonly declaration: RuntimeControlInputDeclaration;
	readonly result: RuntimeControlInputCommitResult;
}

interface RuntimeUserInterruptState {
	readonly command: RuntimeInterruptCommandState;
	readonly commitInput: RuntimeControlInputCommit;
	readonly completeCloseout: () => void;
	closeoutEligible: boolean;
	inputCommitApplied: boolean;
	declaration?: RuntimeControlInputDeclaration | undefined;
	commitPromise?: Promise<RuntimeControlInputCommitApplication> | undefined;
	commitResult?: RuntimeControlInputCommitResult | undefined;
}

export type RuntimeThreadRoleState = "main" | "subagent" | "approval_reviewer";
export type RuntimeThreadVisibilityState = "public" | "internal";
// Runtime-side thread lifecycle. These transitions are the source Bridge commits as
// the public session.status_* and session.thread_status_* events; the runtime enum is
// wider than the public one and is projected at the durable boundary:
//   | RuntimeThreadStatusState | public projection                        |
//   | ------------------------ | ---------------------------------------- |
//   | idle                     | idle                                     |
//   | running                  | running                                  |
//   | rescheduling             | rescheduled                              |
//   | requires_action          | idle (projected at the durable boundary) |
//   | closed_for_runtime       | idle                                     |
//   | terminated               | terminated                               |
//   | failed                   | terminated                               |
// UPDATE-WITH: services/agent-runtime/packages/core/src/session/session-manager.ts,
//              services/agent-runtime/packages/core/src/runtime/session-event-writer.ts
export type RuntimeThreadStatusState =
	| "idle"
	| "running"
	| "requires_action"
	| "closed_for_runtime"
	| "rescheduling"
	| "terminated"
	| "failed";
export type RuntimeSubAgentTypeState = "general" | "research" | "worker";

/** Durable thread metadata accepted when a command first makes a thread resident. */
export interface RuntimeAcceptedThreadMetadataState {
	readonly parentThreadId?: string | undefined;
	readonly parentTaskName?: string | undefined;
	readonly role?: RuntimeThreadRoleState | undefined;
	readonly visibility?: RuntimeThreadVisibilityState | undefined;
	readonly taskName?: string | undefined;
	readonly agentType?:
		| RuntimeSubAgentTypeState
		| "approval_reviewer"
		| undefined;
	readonly status?: RuntimeThreadStatusState | undefined;
}

/** User-message input delivered as an opaque Bridge-classified payload. */
export interface RuntimeCommittedContextAcceptedInputState
	extends RuntimeAcceptedInputScopeState {
	readonly kind: "messages";
	readonly contentJson: string;
}

/** Fenced inter-agent delivery carrying its durably sourced user-message draft. */
export interface RuntimeInterAgentAcceptedInputState
	extends RuntimeControlInputState {
	readonly kind: "inter_agent_message";
	readonly deliveryId: string;
	readonly content: string;
	readonly thread?: RuntimeAcceptedThreadMetadataState | undefined;
}

/** Internal reviewer input carrying the target case, transcript feed, and output schema. */
export interface RuntimeApprovalReviewAcceptedInputState
	extends RuntimeAcceptedInputScopeState {
	readonly kind: "approval_review";
	readonly reviewId: string;
	readonly parentThreadId: string;
	readonly targetModelToolCallId: string;
	readonly targetToolName: string;
	readonly promptText: readonly string[];
	readonly outputSchemaJson: string;
	readonly thread?: RuntimeAcceptedThreadMetadataState | undefined;
}

/** Bounded fact authorizing the loop to replace an undeliverable input with one rejection projection. */
export interface RuntimeRejectionAcceptedInputState
	extends RuntimeAcceptedInputScopeState {
	readonly kind: "rejection";
	readonly reasonCode:
		| "runtime_command_payload_too_large"
		| "runtime_command_rejected";
}

/** Accepted input variants queued for one thread without merging their durable identities. */
export type RuntimeAcceptedInputState =
	| RuntimeCommittedContextAcceptedInputState
	| RuntimeInterAgentAcceptedInputState
	| RuntimeApprovalReviewAcceptedInputState
	| RuntimeRejectionAcceptedInputState
	| RuntimeTaskNotificationAcceptedInputState;

/** Exact durable identities represented by one cold baseline. */
/** Cold thread state loaded before a resident thread is allowed to serve commands. */
export interface RuntimeThreadPreloadState extends RuntimeThreadAddressState {
	readonly thread?: RuntimeAcceptedThreadMetadataState | undefined;
	readonly contextEntries: readonly RuntimeContextEntry[];
	readonly openRequestDraft?: RuntimeOpenRequestDraft | undefined;
	readonly turnCheckpoint?: ThreadTurnCheckpoint | undefined;
	readonly turnToolRouteView?: ThreadToolRouteView | undefined;
	readonly threadContextPrefix?: ThreadContextPrefix | undefined;
	readonly runtimeBindingToken: string;
	readonly runtimeConfigPatch?: RuntimeConfigPatchState | undefined;
	readonly mcpManifests?: readonly RuntimeConfigPatchState[] | undefined;
	readonly pendingToolUses?:
		| readonly RuntimePreloadedPendingToolUseState[]
		| undefined;
	readonly pendingSandboxExecutions?:
		| readonly RuntimePreloadedSandboxExecutionState[]
		| undefined;
	readonly pendingAttachments?:
		| readonly RuntimeProviderAttachment[]
		| undefined;
	readonly pendingAgentMail?:
		| readonly RuntimeInterAgentAcceptedInputState[]
		| undefined;
}

/** Durable pending tool state restored before a cold thread may resume execution. */
export interface RuntimePreloadedPendingToolUseState {
	readonly toolUseEventId: string;
	readonly modelRequestId: string;
	readonly modelToolCallId: string;
	readonly toolName: string;
	readonly input: RuntimeJsonValue;
	readonly decision?: "allow" | "deny" | undefined;
	readonly denyMessage?: string | undefined;
	readonly status: "pending" | "resolving";
}

/** Durable accepted Sandbox execution restored before its Tool Result exists. */
export interface RuntimePreloadedSandboxExecutionState {
	readonly toolUseEventId: string;
	readonly modelRequestId: string;
	readonly modelToolCallId: string;
	readonly toolName: string;
	readonly input: RuntimeJsonValue;
	readonly executionState:
		| "pending"
		| "preparing"
		| "running"
		| "waiting_activation"
		| "waiting_materialization"
		| "terminal_unconsumed";
}

/** Recorded user decision applied to one durable pending tool use. */
export interface RuntimeToolConfirmationState extends RuntimeControlInputState {
	readonly toolUseEventId: string;
	readonly decision: "allow" | "deny";
	readonly denyMessage?: string | undefined;
}

export interface RuntimePendingApprovalToolJobState {
	readonly toolUseEventId: string;
	readonly modelRequestId: string;
	readonly source: RuntimeProcessorSource;
	readonly assistantMessageSequence: number;
	readonly toolPart: Extract<
		RuntimeAssistantDraftPart,
		{ readonly type: "tool" }
	>;
	readonly job: ToolJob;
	readonly entry: ToolEntry;
	readonly currentModel?: SessionCurrentModel | undefined;
}

/** Cold Tool route whose durable allow/deny decision is already authoritative. */
export interface RuntimeResolvedToolRouteJobState
	extends RuntimePendingApprovalToolJobState {
	readonly recoveryKind: "resolved_route";
	readonly decision: "allow" | "deny";
	readonly denyMessage?: string | undefined;
}

/** Hot reconstruction of an accepted Sandbox execution that still needs conversation settlement. */
export interface RuntimePendingSandboxExecutionJobState {
	readonly recoveryKind: "sandbox_execution";
	readonly toolUseEventId: string;
	readonly modelRequestId: string;
	readonly source: RuntimeProcessorSource;
	readonly assistantMessageSequence: number;
	readonly toolPart: Extract<
		RuntimeAssistantDraftPart,
		{ readonly type: "tool" }
	>;
	readonly job: ToolJob;
	readonly entry: ToolEntry;
	readonly currentModel?: SessionCurrentModel | undefined;
}

/** Terminal background-task command before its durable declaration has committed context. */
export interface RuntimeTaskNotificationCommandState
	extends RuntimeAcceptedInputScopeState {
	readonly taskId: string;
	readonly sourceToolUseEventId: string;
	readonly status: "completed" | "failed" | "cancelled" | "expired";
	readonly notificationJson: string;
}

/** Terminal task fact waiting for the next serialized semantic turn. */
export interface RuntimeTaskNotificationAcceptedInputState
	extends RuntimeTaskNotificationCommandState {
	readonly kind: "task_notification";
}

/** Terminal background-task fact and its committed context entry installed together in hot state. */
export interface RuntimeTaskNotificationState
	extends RuntimeTaskNotificationCommandState {
	readonly committedEntry: RuntimeContextEntry;
}

/** Generation-fenced runtime or per-server MCP configuration patch. */
export interface RuntimeConfigPatchState extends RuntimeConfigurationPatch {
	readonly workspaceId: string;
	readonly sessionId: string;
	readonly bindingId: string;
	readonly bindingGeneration: number;
	readonly targetPodUid: string;
	readonly configIdentity: string;
}

/** Sole resident owner of one Thread's derived current-Turn checkpoint and Tool routes. */
export class ThreadProcessor {
	#checkpoint: ThreadTurnCheckpoint;
	#toolRoutes: ThreadToolRouteView;
	#activeInputView: ThreadActiveInputView;
	#acceptedInputs: RuntimeAcceptedInputState[] = [];
	#committingAcceptedInputId: string | undefined;
	#acceptedInputBlockedUntilRunExit = false;

	constructor(
		checkpoint: ThreadTurnCheckpoint,
		toolRoutes: ThreadToolRouteView,
		activeInputView: ThreadActiveInputView,
	) {
		this.#toolRoutes = toolRoutes;
		this.#activeInputView = activeInputView;
		this.#checkpoint = initializeThreadTurnTransition(
			checkpoint,
			toolRoutes,
			[],
			activeInputView,
		).checkpoint;
	}

	get checkpoint(): ThreadTurnCheckpoint {
		return this.#checkpoint;
	}
	get toolRoutes(): ThreadToolRouteView {
		return this.#toolRoutes;
	}
	transition(): ThreadTurnTransition {
		return {
			checkpoint: this.#checkpoint,
			...deriveThreadTurnSnapshot(
				this.#checkpoint,
				this.#toolRoutes,
				this.acceptedInputIds(),
				this.#activeInputView,
			),
		};
	}

	apply(fact: ThreadTurnFact): ThreadTurnTransition {
		return this.applyFrom(this.transition(), fact);
	}

	applyFrom(
		owner: ThreadTurnTransition,
		fact: ThreadTurnFact,
	): ThreadTurnTransition {
		if (owner.checkpoint !== this.#checkpoint) {
			throw new ThreadTurnContractError(
				"Thread turn changed while a stack-local transition owner was active",
			);
		}
		const transition = reduceThreadTurn(
			owner,
			fact,
			this.#toolRoutes,
			this.acceptedInputIds(),
			this.#activeInputView,
		);
		this.#checkpoint = transition.checkpoint;
		const currentToolUseEventIds = new Set(
			this.#checkpoint.request?.toolMembers.flatMap((member) =>
				member.memberKind === "public_tool_use" ? [member.toolUseEventId] : [],
			) ?? [],
		);
		this.#toolRoutes = {
			routes: this.#toolRoutes.routes.filter((route) =>
				currentToolUseEventIds.has(route.toolUseEventId),
			),
		};
		return transition;
	}

	setActiveInputView(activeInputView: ThreadActiveInputView): void {
		this.#activeInputView = activeInputView;
		this.refreshDecision();
	}

	recordRoute(
		toolUseEventId: string,
		disposition: ThreadToolRouteView["routes"][number]["disposition"],
	): void {
		this.#toolRoutes = {
			routes: [
				...this.#toolRoutes.routes.filter(
					(route) => route.toolUseEventId !== toolUseEventId,
				),
				{ toolUseEventId, disposition },
			],
		};
	}

	clearRoute(toolUseEventId: string): void {
		this.#toolRoutes = {
			routes: this.#toolRoutes.routes.filter(
				(route) => route.toolUseEventId !== toolUseEventId,
			),
		};
	}

	/**
	 * Installs one accepted command into the same projection the reducer consumes.
	 * Admission is the Runtime-command success boundary; the typed durable commit result
	 * is the only operation that removes the fact or advances request readiness.
	 */
	admitAcceptedInput(
		state: RuntimeAcceptedInputState,
	): "applied" | "duplicate" | "conflict" {
		const existing = this.#acceptedInputs.find(
			(input) => input.runtimeInputId === state.runtimeInputId,
		);
		if (existing !== undefined) {
			return sameAcceptedInput(existing, state) ? "duplicate" : "conflict";
		}
		this.#acceptedInputs.push(state);
		this.refreshDecision();
		return "applied";
	}

	peekAcceptedInput(): RuntimeAcceptedInputState | undefined {
		return this.#acceptedInputs[0];
	}

	acceptedInputSnapshot(): readonly RuntimeAcceptedInputState[] {
		return [...this.#acceptedInputs];
	}

	acknowledgeAcceptedInput(runtimeInputId: string): void {
		if (this.#acceptedInputs[0]?.runtimeInputId === runtimeInputId) {
			this.#acceptedInputs.shift();
		} else {
			this.#acceptedInputs = this.#acceptedInputs.filter(
				(input) => input.runtimeInputId !== runtimeInputId,
			);
		}
		this.refreshDecision();
	}

	discardApprovalReview(reviewId: string): void {
		this.#acceptedInputs = this.#acceptedInputs.filter(
			(input) =>
				input.kind !== "approval_review" || input.reviewId !== reviewId,
		);
		this.refreshDecision();
	}

	beginAcceptedInputCommit(runtimeInputId: string): void {
		this.#committingAcceptedInputId = runtimeInputId;
	}

	finishAcceptedInputCommit(runtimeInputId: string): void {
		if (this.#committingAcceptedInputId === runtimeInputId) {
			this.#committingAcceptedInputId = undefined;
		}
	}

	blockAcceptedInputUntilRunExit(): void {
		this.#acceptedInputBlockedUntilRunExit = true;
		this.refreshDecision();
	}

	finishRunBoundary(): void {
		if (this.#acceptedInputBlockedUntilRunExit) {
			this.#acceptedInputBlockedUntilRunExit = false;
			this.refreshDecision();
		}
	}

	discardAcceptedInputsForInterrupt(
		preserveTaskNotifications: boolean,
	): void {
		this.#acceptedInputs = this.#acceptedInputs.filter(
			(input) =>
				input.kind === "inter_agent_message" ||
				(preserveTaskNotifications && input.kind === "task_notification") ||
				input.runtimeInputId === this.#committingAcceptedInputId,
		);
		this.refreshDecision();
	}

	acceptedInputCount(): number {
		return this.#acceptedInputs.length;
	}

	clearAcceptedInputs(): void {
		this.#acceptedInputs = [];
		this.#committingAcceptedInputId = undefined;
		this.refreshDecision();
	}

	private acceptedInputIds(): readonly string[] {
		return this.#acceptedInputBlockedUntilRunExit
			? []
			: this.#acceptedInputs.map((input) => input.runtimeInputId);
	}

	private refreshDecision(): void {
		deriveThreadTurnSnapshot(
			this.#checkpoint,
			this.#toolRoutes,
			this.acceptedInputIds(),
			this.#activeInputView,
		);
	}
}

/** Recoverable thread-local working state for input, tools, config, media, and cancellation. */
export class ThreadState {
	readonly contextManager: ContextManager;
	#persistentContextLoaded = false;
	#currentModel: SessionCurrentModel | undefined;
	#toolConfirmations: Record<string, RuntimeToolConfirmationState | undefined> =
		Object.create(null) as Record<
			string,
			RuntimeToolConfirmationState | undefined
		>;
	#pendingApprovalToolJobs: Record<
		string,
		RuntimePendingApprovalToolJobState | undefined
	> = Object.create(null) as Record<
		string,
		RuntimePendingApprovalToolJobState | undefined
	>;
	#resolvedToolRouteJobs: Record<
		string,
		RuntimeResolvedToolRouteJobState | undefined
	> = Object.create(null) as Record<
		string,
		RuntimeResolvedToolRouteJobState | undefined
	>;
	#pendingSandboxExecutionJobs: Record<
		string,
		RuntimePendingSandboxExecutionJobState | undefined
	> = Object.create(null) as Record<
		string,
		RuntimePendingSandboxExecutionJobState | undefined
	>;
	#activeAttachmentRide: RuntimeProviderAttachment[] | undefined;
	#fileBackedRideConsumed = false;
	#pendingAttachments: RuntimeProviderAttachment[] = [];
	#threadProcessor: ThreadProcessor | undefined;
	#lastRequestUsage: RuntimeUsage | undefined;
	#lastRequestModelLimits: RuntimeModelLimits | undefined;
	#lastRequestContextAnchorSequence: number | undefined;
	#providerRequestOutputSchemaJson: string | undefined;
	#runtimeShutdownRequested = false;
	#cooperativeCancelRequested = false;
	#userInterrupt: RuntimeUserInterruptState | undefined;
	#lastUserInterruptCommit:
		| {
				readonly runtimeInputId: string;
				readonly result: RuntimeControlInputCommitResult;
		  }
		| undefined;

	constructor(sessionId: string) {
		this.contextManager = new ContextManager(sessionId);
	}

	persistentContextLoaded(): boolean {
		return this.#persistentContextLoaded;
	}

	markPersistentContextLoaded(): void {
		this.#persistentContextLoaded = true;
	}

	installThreadTurn(
		checkpoint: ThreadTurnCheckpoint | undefined,
		routeView: ThreadToolRouteView | undefined,
	): void {
		this.#threadProcessor = new ThreadProcessor(
			checkpoint ?? { pendingInputContextSequences: [] },
			routeView ?? { routes: [] },
			this.activeInputView(),
		);
	}

	threadTurnReduction(): ThreadTurnTransition {
		if (this.#threadProcessor === undefined) {
			this.installThreadTurn(undefined, undefined);
		}
		return this.#threadProcessor!.transition();
	}

	applyThreadTurnFact(fact: ThreadTurnFact): ThreadTurnTransition {
		this.threadTurnReduction();
		return this.#threadProcessor!.apply(fact);
	}

	applyRequestStartFact(
		owner: ThreadTurnTransition,
		fact: Extract<ThreadTurnFact, { readonly fact: "request_started" }>,
	): ThreadTurnTransition {
		this.threadTurnReduction();
		return this.#threadProcessor!.applyFrom(owner, fact);
	}

	recordThreadToolRoute(
		toolUseEventId: string,
		disposition: ThreadToolRouteView["routes"][number]["disposition"],
	): void {
		this.threadTurnReduction();
		this.#threadProcessor!.recordRoute(toolUseEventId, disposition);
	}

	clearThreadToolRoute(toolUseEventId: string): void {
		this.threadTurnReduction();
		this.#threadProcessor!.clearRoute(toolUseEventId);
	}

	currentModel(): SessionCurrentModel | undefined {
		return this.#currentModel;
	}

	updateCurrentModel(model: SessionCurrentModel): void {
		if (
			this.#currentModel !== undefined &&
			(this.#currentModel.providerId !== model.providerId ||
				this.#currentModel.modelId !== model.modelId)
		) {
			this.clearLastRequestCompletion();
		}
		this.#currentModel = model;
	}

	enqueueAcceptedInput(
		state: RuntimeAcceptedInputState,
	): "applied" | "duplicate" | "conflict" {
		this.threadTurnReduction();
		return this.#threadProcessor!.admitAcceptedInput(state);
	}

	peekAcceptedInput(): RuntimeAcceptedInputState | undefined {
		this.threadTurnReduction();
		return this.#threadProcessor!.peekAcceptedInput();
	}

	acceptedInputSnapshot(): readonly RuntimeAcceptedInputState[] {
		this.threadTurnReduction();
		return this.#threadProcessor!.acceptedInputSnapshot();
	}

	acknowledgeAcceptedInput(runtimeInputId: string): void {
		this.threadTurnReduction();
		this.#threadProcessor!.acknowledgeAcceptedInput(runtimeInputId);
	}

	discardQueuedApprovalReview(reviewId: string): void {
		this.threadTurnReduction();
		this.#threadProcessor!.discardApprovalReview(reviewId);
	}

	beginAcceptedInputCommit(runtimeInputId: string): void {
		this.threadTurnReduction();
		this.#threadProcessor!.beginAcceptedInputCommit(runtimeInputId);
	}

	finishAcceptedInputCommit(runtimeInputId: string): void {
		this.threadTurnReduction();
		this.#threadProcessor!.finishAcceptedInputCommit(runtimeInputId);
	}

	discardQueuedAcceptedInputsForInterrupt(
		preserveTaskNotifications: boolean,
	): void {
		this.threadTurnReduction();
		this.#threadProcessor!.discardAcceptedInputsForInterrupt(
			preserveTaskNotifications,
		);
	}

	acceptedInputCount(): number {
		this.threadTurnReduction();
		return this.#threadProcessor!.acceptedInputCount();
	}

	resolveToolConfirmation(
		state: RuntimeToolConfirmationState,
	): "applied" | "duplicate" | "conflict" {
		const existing = this.#toolConfirmations[state.toolUseEventId];
		if (existing === undefined) {
			this.#toolConfirmations[state.toolUseEventId] = state;
			return "applied";
		}
		if (
			existing.runtimeInputId === state.runtimeInputId ||
			(existing.decision === state.decision &&
				existing.denyMessage === state.denyMessage)
		) {
			return "duplicate";
		}
		return "conflict";
	}

	toolConfirmation(
		toolUseEventId: string,
	): RuntimeToolConfirmationState | undefined {
		return this.#toolConfirmations[toolUseEventId];
	}

	recordPendingApprovalToolJob(
		state: RuntimePendingApprovalToolJobState,
	): void {
		this.#pendingApprovalToolJobs[state.toolUseEventId] = state;
	}

	pendingApprovalToolJobs(): readonly RuntimePendingApprovalToolJobState[] {
		return Object.values(this.#pendingApprovalToolJobs)
			.filter(
				(state): state is RuntimePendingApprovalToolJobState =>
					state !== undefined,
			)
			.sort((left, right) => {
				if (left.modelRequestId !== right.modelRequestId) {
					return left.modelRequestId.localeCompare(right.modelRequestId);
				}
				return left.job.modelOrder - right.job.modelOrder;
			});
	}

	removePendingApprovalToolJob(toolUseEventId: string): void {
		delete this.#pendingApprovalToolJobs[toolUseEventId];
		delete this.#toolConfirmations[toolUseEventId];
	}

	recordResolvedToolRouteJob(state: RuntimeResolvedToolRouteJobState): void {
		this.#resolvedToolRouteJobs[state.toolUseEventId] = state;
	}

	resolvedToolRouteJobs(): readonly RuntimeResolvedToolRouteJobState[] {
		return Object.values(this.#resolvedToolRouteJobs)
			.filter(
				(state): state is RuntimeResolvedToolRouteJobState =>
					state !== undefined,
			)
			.sort((left, right) => {
				if (left.modelRequestId !== right.modelRequestId) {
					return left.modelRequestId.localeCompare(right.modelRequestId);
				}
				return left.job.modelOrder - right.job.modelOrder;
			});
	}

	removeResolvedToolRouteJob(toolUseEventId: string): void {
		delete this.#resolvedToolRouteJobs[toolUseEventId];
	}

	hasPendingApprovalToolJobs(): boolean {
		return Object.values(this.#pendingApprovalToolJobs).some(
			(state) => state !== undefined,
		);
	}

	recordPendingSandboxExecutionJob(
		state: RuntimePendingSandboxExecutionJobState,
	): void {
		this.#pendingSandboxExecutionJobs[state.toolUseEventId] = state;
	}

	pendingSandboxExecutionJobs(): readonly RuntimePendingSandboxExecutionJobState[] {
		return Object.values(this.#pendingSandboxExecutionJobs)
			.filter(
				(state): state is RuntimePendingSandboxExecutionJobState =>
					state !== undefined,
			)
			.sort((left, right) => {
				if (left.modelRequestId !== right.modelRequestId) {
					return left.modelRequestId.localeCompare(right.modelRequestId);
				}
				return left.job.modelOrder - right.job.modelOrder;
			});
	}

	removePendingSandboxExecutionJob(toolUseEventId: string): void {
		delete this.#pendingSandboxExecutionJobs[toolUseEventId];
	}

	commitTaskNotification(
		state: RuntimeTaskNotificationState,
	): "applied" | "duplicate" | "conflict" {
		const durableEntry = this.contextManager.entry(
			state.committedEntry.messageSequence,
		);
		if (durableEntry === undefined) {
			this.contextManager.appendEntry(state.committedEntry);
			return "applied";
		}
		return JSON.stringify(durableEntry) === JSON.stringify(state.committedEntry)
			? "duplicate"
			: "conflict";
	}

	addPendingAttachments(
		attachments: readonly RuntimeProviderAttachment[],
	): void {
		const hadPendingAttachments = this.hasPendingAttachments();
		const existing = new Set(
			[
				...(this.#activeAttachmentRide ?? []),
				...this.#pendingAttachments,
			].map(runtimeProviderAttachmentIdentityKey),
		);
		const additions = attachments.filter((attachment) => {
			const identity = runtimeProviderAttachmentIdentityKey(attachment);
			if (existing.has(identity)) return false;
			existing.add(identity);
			return true;
		});
		const available = Math.max(
			0,
			MaxProviderAttachments - this.#pendingAttachments.length,
		);
		this.#pendingAttachments.push(
			...additions.slice(0, available).map(cloneRuntimeProviderAttachment),
		);
		this.refreshActiveInputViewIfChanged(hadPendingAttachments);
	}

	pendingAttachments(): readonly RuntimeProviderAttachment[] {
		return [
			...(this.#activeAttachmentRide ?? []),
			...this.#pendingAttachments,
		].map(cloneRuntimeProviderAttachment);
	}

	beginPendingAttachmentRide(): readonly RuntimeProviderAttachment[] {
		if (
			this.#activeAttachmentRide === undefined &&
			this.#pendingAttachments.length > 0
		) {
			this.#activeAttachmentRide = this.#pendingAttachments;
			this.#pendingAttachments = [];
			this.#fileBackedRideConsumed = false;
		}
		return (this.#activeAttachmentRide ?? []).map(
			cloneRuntimeProviderAttachment,
		);
	}

	consumeFileBackedAttachmentRide(): void {
		if (this.#activeAttachmentRide !== undefined) {
			const before = this.#activeAttachmentRide.length;
			this.#activeAttachmentRide = this.#activeAttachmentRide.filter(
				(attachment) => attachment.fileBacked === undefined,
			);
			this.#fileBackedRideConsumed ||=
				this.#activeAttachmentRide.length !== before;
		}
	}

	settlePendingAttachmentRide(): void {
		const hadPendingAttachments =
			this.hasPendingAttachments() || this.#fileBackedRideConsumed;
		this.#activeAttachmentRide = undefined;
		this.#fileBackedRideConsumed = false;
		this.refreshActiveInputViewIfChanged(hadPendingAttachments);
	}

	replacePendingAttachments(
		attachments: readonly RuntimeProviderAttachment[],
	): void {
		const hadPendingAttachments = this.hasPendingAttachments();
		this.#activeAttachmentRide = undefined;
		this.#fileBackedRideConsumed = false;
		this.#pendingAttachments = [];
		const available = Math.min(attachments.length, MaxProviderAttachments);
		this.#pendingAttachments.push(
			...attachments
				.slice(0, available)
				.map(cloneRuntimeProviderAttachment),
		);
		this.refreshActiveInputViewIfChanged(hadPendingAttachments);
	}

	private hasPendingAttachments(): boolean {
		return (
			(this.#activeAttachmentRide?.length ?? 0) > 0 ||
			this.#pendingAttachments.length > 0
		);
	}

	private activeInputView(): ThreadActiveInputView {
		return { hasPendingAttachments: this.hasPendingAttachments() };
	}

	private refreshActiveInputViewIfChanged(previous: boolean): void {
		if (previous !== this.hasPendingAttachments()) {
			this.refreshActiveInputView();
		}
	}

	private refreshActiveInputView(): void {
		this.#threadProcessor?.setActiveInputView(this.activeInputView());
	}

	recordLastRequestCompletion(
		usage: RuntimeUsage,
		limits: RuntimeModelLimits,
		contextAnchorSequence: number,
	): void {
		this.#lastRequestUsage = { ...usage };
		this.#lastRequestModelLimits = { ...limits };
		this.#lastRequestContextAnchorSequence = contextAnchorSequence;
	}

	lastRequestUsage(): RuntimeUsage | undefined {
		return this.#lastRequestUsage === undefined
			? undefined
			: { ...this.#lastRequestUsage };
	}

	lastRequestModelLimits(): RuntimeModelLimits | undefined {
		return this.#lastRequestModelLimits === undefined
			? undefined
			: { ...this.#lastRequestModelLimits };
	}

	lastRequestContextAnchorSequence(): number | undefined {
		return this.#lastRequestContextAnchorSequence;
	}

	clearLastRequestUsage(): void {
		this.#lastRequestUsage = undefined;
		this.#lastRequestContextAnchorSequence = undefined;
	}

	clearLastRequestCompletion(): void {
		this.#lastRequestUsage = undefined;
		this.#lastRequestModelLimits = undefined;
		this.#lastRequestContextAnchorSequence = undefined;
	}

	setProviderOutputSchemaJson(outputSchemaJson: string | undefined): void {
		this.#providerRequestOutputSchemaJson = outputSchemaJson;
	}

	providerRequestOutputSchemaJson(): string | undefined {
		return this.#providerRequestOutputSchemaJson;
	}

	beginRuntimeShutdown(): void {
		this.#runtimeShutdownRequested = true;
	}

	runtimeShutdownRequested(): boolean {
		return this.#runtimeShutdownRequested;
	}

	beginCooperativeCancel(): void {
		this.#cooperativeCancelRequested = true;
	}

	cooperativeCancelRequested(): boolean {
		return this.#cooperativeCancelRequested;
	}

	finishCooperativeCancel(): void {
		this.#cooperativeCancelRequested = false;
	}

	beginUserInterrupt(
		command: RuntimeInterruptCommandState,
		commitInput: RuntimeControlInputCommit,
		completeCloseout: () => void = () => {},
	): "applied" | "duplicate" | "conflict" {
		if (this.#userInterrupt !== undefined) {
			return this.#userInterrupt.command.runtimeInputId ===
				command.runtimeInputId
				? "duplicate"
				: "conflict";
		}
		this.#userInterrupt = {
			command,
			commitInput,
			completeCloseout,
			closeoutEligible: false,
			inputCommitApplied: false,
		};
		this.#threadProcessor?.blockAcceptedInputUntilRunExit();
		return "applied";
	}

	finishThreadRunProjection(): void {
		this.#threadProcessor?.finishRunBoundary();
	}

	blockAcceptedInputUntilRunExit(): void {
		this.#threadProcessor?.blockAcceptedInputUntilRunExit();
	}

	userInterruptRequested(): boolean {
		return this.#userInterrupt !== undefined;
	}

	userInterruptCommand(): RuntimeInterruptCommandState | undefined {
		return this.#userInterrupt?.command;
	}

	markUserInterruptCloseoutEligible(): void {
		if (this.#userInterrupt !== undefined) {
			this.#userInterrupt.closeoutEligible = true;
		}
	}

	userInterruptCloseoutEligible(): boolean {
		return this.#userInterrupt?.closeoutEligible === true;
	}

	userInterruptInputCommitApplied(): boolean {
		return this.#userInterrupt?.inputCommitApplied === true;
	}

	markUserInterruptInputCommitApplied(): void {
		if (this.#userInterrupt !== undefined) {
			this.#userInterrupt.inputCommitApplied = true;
		}
	}

	async commitUserInterruptInput(
		declaration: RuntimeControlInputDeclaration,
	): Promise<RuntimeControlInputCommitApplication> {
		const interrupt = this.#userInterrupt;
		if (interrupt === undefined) {
			return {
				declaration,
				result: {
					ok: false,
					retryable: true,
					errorCode: "interrupt_closeout_missing",
				},
			};
		}
		interrupt.declaration ??= declaration;
		if (interrupt.commitResult !== undefined) {
			return {
				declaration: interrupt.declaration,
				result: interrupt.commitResult,
			};
		}
		if (interrupt.commitPromise === undefined) {
			const commitPromise = interrupt
				.commitInput(interrupt.declaration)
				.then((result) => {
					if (result.ok || !result.retryable) {
						interrupt.commitResult = result;
						this.#lastUserInterruptCommit = {
							runtimeInputId: interrupt.command.runtimeInputId,
							result,
						};
					}
					return { declaration: interrupt.declaration!, result };
				})
				.finally(() => {
					if (
						interrupt.commitPromise === commitPromise &&
						interrupt.commitResult === undefined
					) {
						interrupt.commitPromise = undefined;
					}
				});
			interrupt.commitPromise = commitPromise;
		}
		return await interrupt.commitPromise;
	}

	recordJoinedUserInterruptResult(
		runtimeInputId: string,
		result: RuntimeControlInputCommitResult = { ok: true, joined: true },
		declaration: RuntimeControlInputDeclaration = {
			inputKind: "interrupt",
		},
	): boolean {
		const interrupt = this.#userInterrupt;
		if (interrupt?.command.runtimeInputId !== runtimeInputId) {
			return false;
		}
		interrupt.declaration = declaration;
		interrupt.commitResult = result;
		interrupt.commitPromise = Promise.resolve({ declaration, result });
		interrupt.inputCommitApplied = result.ok && "joined" in result;
		this.#lastUserInterruptCommit = { runtimeInputId, result };
		return true;
	}

	userInterruptCommitResult(
		runtimeInputId: string,
	): RuntimeControlInputCommitResult | undefined {
		return this.#lastUserInterruptCommit?.runtimeInputId === runtimeInputId
			? this.#lastUserInterruptCommit.result
			: undefined;
	}

	completeUserInterrupt(runtimeInputId: string): void {
		if (this.#userInterrupt?.command.runtimeInputId !== runtimeInputId) {
			return;
		}
		this.#userInterrupt.completeCloseout();
		this.#userInterrupt = undefined;
	}

	clear(): void {
		// Generic failure cleanup cannot erase an accepted Runtime input. The
		// Session owner must first land an exact stale/close/termination result;
		// only that durable custody handoff may use clearAfterCustodyHandoff.
		if (this.acceptedInputCount() > 0) {
			return;
		}
		const activeLifecycle = this.#threadProcessor;
		this.clearAfterCustodyHandoff();
		// Generic hot-state cleanup precedes failed-run closeout. Preserve the
		// reducer checkpoint as the sole active-turn owner until that durable
		// closeout lands; the Session owner performs the final custody clear.
		this.#threadProcessor = activeLifecycle;
	}

	clearAfterCustodyHandoff(): void {
		this.contextManager.clear();
		this.#persistentContextLoaded = false;
		this.#currentModel = undefined;
		this.#threadProcessor?.clearAcceptedInputs();
		this.#toolConfirmations = Object.create(null) as Record<
			string,
			RuntimeToolConfirmationState | undefined
		>;
		this.#pendingApprovalToolJobs = Object.create(null) as Record<
			string,
			RuntimePendingApprovalToolJobState | undefined
		>;
		this.#resolvedToolRouteJobs = Object.create(null) as Record<
			string,
			RuntimeResolvedToolRouteJobState | undefined
		>;
		this.#pendingSandboxExecutionJobs = Object.create(null) as Record<
			string,
			RuntimePendingSandboxExecutionJobState | undefined
		>;
		this.#activeAttachmentRide = undefined;
		this.#fileBackedRideConsumed = false;
		this.#pendingAttachments = [];
		this.refreshActiveInputView();
		this.#threadProcessor = undefined;
		this.#lastRequestUsage = undefined;
		this.#lastRequestModelLimits = undefined;
		this.#lastRequestContextAnchorSequence = undefined;
		this.#providerRequestOutputSchemaJson = undefined;
		this.#runtimeShutdownRequested = false;
		this.#cooperativeCancelRequested = false;
		this.#userInterrupt = undefined;
		this.#lastUserInterruptCommit = undefined;
	}
}

function cloneRuntimeProviderAttachment(
	attachment: RuntimeProviderAttachment,
): RuntimeProviderAttachment {
	return attachment.transient !== undefined
		? {
				...attachment,
				transient: { ...attachment.transient },
				fileBacked: undefined,
			}
		: {
				...attachment,
				transient: undefined,
				fileBacked: { ...attachment.fileBacked },
			};
}

function runtimeProviderAttachmentIdentityKey(
	attachment: RuntimeProviderAttachment,
): string {
	return attachment.transient !== undefined
		? JSON.stringify(["transient", attachment.transient.attachmentRef])
		: JSON.stringify([
				"file-backed",
				attachment.fileBacked.sourceEventId,
				attachment.fileBacked.fileId,
			]);
}

function sameAcceptedInput(
	left: RuntimeAcceptedInputState,
	right: RuntimeAcceptedInputState,
): boolean {
	if (
		left.kind === "inter_agent_message" &&
		right.kind === "inter_agent_message"
	) {
		// Cold preload carries durable Thread lineage needed to construct the
		// resident aggregate; a later delivery retry does not carry that local
		// installation projection. Every mail custody and payload field must still
		// match before the delivery is considered duplicate.
		const { thread: _leftThread, ...leftIdentity } = left;
		const { thread: _rightThread, ...rightIdentity } = right;
		return JSON.stringify(leftIdentity) === JSON.stringify(rightIdentity);
	}
	return JSON.stringify(left) === JSON.stringify(right);
}
