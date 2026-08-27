/**
 * @packageDocumentation
 * Reconstructible hot data for one resident Thread. ThreadState owns the
 * canonical turn checkpoint, read-only input/route/media views, pending Tool
 * work and cancellation controllers. ContextManager separately owns the
 * provider-visible message view. Cold preload installs durable projections;
 * ThreadLoop applies committed facts and captures any resulting dispatch.
 * ThreadState never persists or replays a dispatch and is not durable truth.
 */

import type {
	RuntimeAssistantDraftPart,
	RuntimeContextEntry,
	RuntimeFailure,
	RuntimeProcessorSource,
	RuntimeProviderAttachment,
	RuntimeUsage,
} from "../contracts/runtime.js";
import type { RuntimeModelLimits } from "../llm/llm-event.js";
import { ContextManager } from "../session/context-manager.js";
import type { ToolEntry } from "../tools/tool-catalog.js";
import type { ToolJob } from "../tools/tool-scheduler.js";
import type {
	RuntimeAcceptedInputState,
	RuntimeTaskNotificationState,
} from "./input/accepted-input.js";
import type {
	RuntimeControlInputCommit,
	RuntimeControlInputCommitApplication,
	RuntimeControlInputCommitResult,
	RuntimeControlInputDeclaration,
	RuntimeInterruptCommandState,
	RuntimeToolConfirmationState,
} from "./input/control-input.js";
import type {
	ThreadToolRouteView,
	ThreadTurnCheckpoint,
} from "./turn/checkpoint.js";
import type {
	ThreadActiveInputView,
	ThreadTurnTransition,
} from "./turn/types.js";
import type { ThreadTurnFact } from "./turn/facts.js";
import {
	deriveThreadTurnSnapshot,
	initializeThreadTurnTransition,
	reduceThreadTurn,
} from "./turn/reducer.js";
import { ThreadTurnContractError } from "./turn/types.js";

/** Combined cap for file-backed and transient attachments on one provider request. */
export const MaxProviderAttachments = 32;

/** Current provider/model selection held by a hot session thread. */
export interface SessionCurrentModel {
	readonly providerId: string;
	readonly modelId: string;
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
	#threadTurnCheckpoint: ThreadTurnCheckpoint = {
		pendingInputContextSequences: [],
	};
	#threadToolRoutes: ThreadToolRouteView = { routes: [] };
	#acceptedInputs: RuntimeAcceptedInputState[] = [];
	#committingAcceptedInputId: string | undefined;
	#acceptedInputBlockedUntilRunExit = false;
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
		const initialized = initializeThreadTurnTransition(
			checkpoint ?? { pendingInputContextSequences: [] },
			routeView ?? { routes: [] },
			this.acceptedInputIds(),
			this.activeInputView(),
		);
		this.#threadTurnCheckpoint = initialized.checkpoint;
		this.#threadToolRoutes = routeView ?? { routes: [] };
	}

	threadTurnTransition(): ThreadTurnTransition {
		return {
			checkpoint: this.#threadTurnCheckpoint,
			...deriveThreadTurnSnapshot(
				this.#threadTurnCheckpoint,
				this.#threadToolRoutes,
				this.acceptedInputIds(),
				this.activeInputView(),
			),
		};
	}

	applyThreadTurnFact(fact: ThreadTurnFact): ThreadTurnTransition {
		return this.applyThreadTurnFactFrom(this.threadTurnTransition(), fact);
	}

	applyRequestStartFact(
		owner: ThreadTurnTransition,
		fact: Extract<ThreadTurnFact, { readonly fact: "request_started" }>,
	): ThreadTurnTransition {
		return this.applyThreadTurnFactFrom(owner, fact);
	}

	applyFinishIdleFact(
		owner: ThreadTurnTransition,
		fact: Extract<ThreadTurnFact, { readonly fact: "finish_idle_committed" }>,
	): ThreadTurnTransition {
		const current = this.threadTurnTransition();
		if (
			owner.checkpoint.executionRunId !== current.checkpoint.executionRunId ||
			owner.checkpoint.request?.modelRequestId !==
				current.checkpoint.request?.modelRequestId
		) {
			throw new ThreadTurnContractError(
				"Thread turn changed while FinishIdle was in flight",
			);
		}
		const settlementOwner = {
			checkpoint: current.checkpoint,
			state: owner.state,
			nextStep: owner.nextStep,
		};
		return this.applyThreadTurnFactFrom(settlementOwner, fact);
	}

	recordThreadToolRoute(
		toolUseEventId: string,
		disposition: ThreadToolRouteView["routes"][number]["disposition"],
	): void {
		this.#threadToolRoutes = {
			routes: [
				...this.#threadToolRoutes.routes.filter(
					(route) => route.toolUseEventId !== toolUseEventId,
				),
				{ toolUseEventId, disposition },
			],
		};
	}

	clearThreadToolRoute(toolUseEventId: string): void {
		this.#threadToolRoutes = {
			routes: this.#threadToolRoutes.routes.filter(
				(route) => route.toolUseEventId !== toolUseEventId,
			),
		};
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
		const existing = this.#acceptedInputs.find(
			(input) => input.runtimeInputId === state.runtimeInputId,
		);
		if (existing !== undefined) {
			return sameAcceptedInput(existing, state) ? "duplicate" : "conflict";
		}
		this.#acceptedInputs.push(state);
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
			return;
		}
		this.#acceptedInputs = this.#acceptedInputs.filter(
			(input) => input.runtimeInputId !== runtimeInputId,
		);
	}

	discardQueuedApprovalReview(reviewId: string): void {
		this.#acceptedInputs = this.#acceptedInputs.filter(
			(input) =>
				input.kind !== "approval_review" || input.reviewId !== reviewId,
		);
	}

	beginAcceptedInputCommit(runtimeInputId: string): void {
		this.#committingAcceptedInputId = runtimeInputId;
	}

	finishAcceptedInputCommit(runtimeInputId: string): void {
		if (this.#committingAcceptedInputId === runtimeInputId) {
			this.#committingAcceptedInputId = undefined;
		}
	}

	discardQueuedAcceptedInputsForInterrupt(
		preserveTaskNotifications: boolean,
	): void {
		this.#acceptedInputs = this.#acceptedInputs.filter(
			(input) =>
				input.kind === "inter_agent_message" ||
				(preserveTaskNotifications && input.kind === "task_notification") ||
				input.runtimeInputId === this.#committingAcceptedInputId,
		);
	}

	acceptedInputCount(): number {
		return this.#acceptedInputs.length;
	}

	private applyThreadTurnFactFrom(
		owner: ThreadTurnTransition,
		fact: ThreadTurnFact,
	): ThreadTurnTransition {
		if (owner.checkpoint !== this.#threadTurnCheckpoint) {
			throw new ThreadTurnContractError(
				"Thread turn changed while a stack-local transition owner was active",
			);
		}
		const transition = reduceThreadTurn(
			owner,
			fact,
			this.#threadToolRoutes,
			this.acceptedInputIds(),
			this.activeInputView(),
		);
		this.#threadTurnCheckpoint = transition.checkpoint;
		const currentToolUseEventIds = new Set(
			this.#threadTurnCheckpoint.request?.toolMembers.flatMap((member) =>
				member.memberKind === "public_tool_use"
					? [member.toolUseEventId]
					: [],
			) ?? [],
		);
		this.#threadToolRoutes = {
			routes: this.#threadToolRoutes.routes.filter((route) =>
				currentToolUseEventIds.has(route.toolUseEventId),
			),
		};
		return transition;
	}

	private acceptedInputIds(): readonly string[] {
		return this.#acceptedInputBlockedUntilRunExit
			? []
			: this.#acceptedInputs.map((input) => input.runtimeInputId);
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
		this.#activeAttachmentRide = undefined;
		this.#fileBackedRideConsumed = false;
	}

	replacePendingAttachments(
		attachments: readonly RuntimeProviderAttachment[],
	): void {
		this.#activeAttachmentRide = undefined;
		this.#fileBackedRideConsumed = false;
		this.#pendingAttachments = [];
		const available = Math.min(attachments.length, MaxProviderAttachments);
		this.#pendingAttachments.push(
			...attachments
				.slice(0, available)
				.map(cloneRuntimeProviderAttachment),
		);
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
		this.#acceptedInputBlockedUntilRunExit = true;
		return "applied";
	}

	finishThreadRunProjection(): void {
		this.#acceptedInputBlockedUntilRunExit = false;
	}

	blockAcceptedInputUntilRunExit(): void {
		this.#acceptedInputBlockedUntilRunExit = true;
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
		if (!result.ok && result.retryable) {
			interrupt.commitResult = undefined;
			interrupt.commitPromise = undefined;
			interrupt.inputCommitApplied = false;
			return true;
		}
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
		const checkpoint = this.#threadTurnCheckpoint;
		const routes = this.#threadToolRoutes;
		this.clearAfterCustodyHandoff();
		// Generic hot-state cleanup precedes failed-run closeout. Preserve the
		// reducer checkpoint as the sole active-turn owner until that durable
		// closeout lands; the Session owner performs the final custody clear.
		this.#threadTurnCheckpoint = checkpoint;
		this.#threadToolRoutes = routes;
	}

	clearAfterCustodyHandoff(): void {
		this.contextManager.clear();
		this.#persistentContextLoaded = false;
		this.#currentModel = undefined;
		this.#acceptedInputs = [];
		this.#committingAcceptedInputId = undefined;
		this.#acceptedInputBlockedUntilRunExit = false;
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
		this.#threadTurnCheckpoint = { pendingInputContextSequences: [] };
		this.#threadToolRoutes = { routes: [] };
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
