import { MaxProviderRequestAttachments } from "@tetral/gateway-protocol/src/bounds.js";
import { RuntimePreviewTextMaxBytes } from "../llm/llm-event.js";
/**
 * @packageDocumentation
 * Reconstructible hot data for one resident Thread. ThreadState owns the
 * canonical turn checkpoint, read-only input/route/media views, pending Tool
 * work and cancellation controllers. ContextManager separately owns the
 * provider-visible message view. Cold preload installs durable projections;
 * ThreadLoop applies committed facts and captures any resulting dispatch.
 * ThreadState never persists or replays a dispatch and is not durable truth.
 */
import { boundRuntimeJson, RuntimeAssistantDraftPartSchema } from "../contracts/runtime.js";
import type { RuntimeCurrentRequestMessage } from "../contracts/runtime.js";
import { normalizeRequestMessages } from "./turn/provider-context.js";
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
import { executionInputForToolCall } from "../tools/tool-catalog.js";
import type { ToolEntry } from "../tools/tool-catalog.js";
import type { ToolJob } from "../tools/tool-scheduler.js";
import type { RuntimeAcceptedInputState, RuntimeTaskNotificationState, } from "./input/accepted-input.js";
import type {
	RuntimeControlInputCommit,
	RuntimeControlInputCommitApplication,
	RuntimeControlInputCommitResult,
	RuntimeControlInputDeclaration,
	RuntimeInterruptCommandState,
	RuntimeToolConfirmationState,
} from "./input/control-input.js";
import { parseThreadTurnCheckpoint } from "./turn/checkpoint.js";
import type { ThreadToolRouteView, ThreadTurnCheckpoint, } from "./turn/checkpoint.js";
import type { ThreadActiveInputView, ThreadTurnTransition, } from "./turn/types.js";
import type { ThreadTurnFact } from "./turn/facts.js";
import { deriveThreadTurnSnapshot, initializeThreadTurnTransition, reduceThreadTurn, } from "./turn/reducer.js";
import { ThreadTurnContractError } from "./turn/types.js";
/** Combined cap for file-backed and transient attachments on one provider request. */
export const MaxProviderAttachments = MaxProviderRequestAttachments;
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
	readonly toolPart: Extract<RuntimeAssistantDraftPart, {
		readonly type: "tool";
	}>;
	readonly job: ToolJob;
	readonly entry: ToolEntry;
	readonly currentModel?: SessionCurrentModel | undefined;
}

/** Cold Tool route whose durable allow/deny decision is already authoritative. */
export interface RuntimeResolvedToolRouteJobState extends RuntimePendingApprovalToolJobState {
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
	readonly toolPart: Extract<RuntimeAssistantDraftPart, {
		readonly type: "tool";
	}>;
	readonly job: ToolJob;
	readonly entry: ToolEntry;
	readonly currentModel?: SessionCurrentModel | undefined;
}

/** Reference and execution controls only; canonical calls/results belong to ContextManager. */
export interface RuntimeActiveTool {
	readonly toolUseEventId: string;
	readonly modelRequestId: string;
	readonly modelToolCallId: string;
	readonly assistantMessageSequence: number;
	readonly disposition: ThreadToolRouteView["routes"][number]["disposition"];
	readonly source?: RuntimeProcessorSource;
	readonly job?: Omit<ToolJob, "input" | "result">;
	readonly entry?: ToolEntry;
	readonly currentModel?: SessionCurrentModel;
	readonly decision?: "allow" | "deny";
	readonly denyMessage?: string | undefined;
	/** One hot residency span; its closure retains no canonical call or result. */
	readonly finishApprovalObservation?: (outcome: import("../runtime/metrics.js").RuntimeMetricOutcome) => void;
}

/** Recoverable thread-local working state for input, tools, config, media, and cancellation. */
export class ThreadState {
	readonly contextManager: ContextManager;
	#persistentContextLoaded = false;
	#residencyValid = true;
	#currentModel: SessionCurrentModel | undefined;
	#toolConfirmations: Record<string, RuntimeToolConfirmationState | undefined> = Object.create(null) as Record<string, RuntimeToolConfirmationState | undefined>;
	#currentRequestMessage: RuntimeCurrentRequestMessage | undefined;
	readonly #activeTools = new Map<string, RuntimeActiveTool>();
	#activeAttachmentRide: RuntimeProviderAttachment[] | undefined;
	#fileBackedRideConsumed = false;
	#pendingAttachments: RuntimeProviderAttachment[] = [];
	#threadTurnCheckpoint: ThreadTurnCheckpoint = {
		pendingInputContextSequences: [],
	};
	#acceptedInputs: RuntimeAcceptedInputState[] = [];
	#committingAcceptedInputId: string | undefined;
	#acceptedInputBlockedUntilRunExit = false;
	#lastRequestUsage: RuntimeUsage | undefined;
	#lastRequestModelLimits: RuntimeModelLimits | undefined;
	#lastRequestContextAnchorSequence: number | undefined;
	#providerRequestOutputSchemaJson: string | undefined;
	#runtimeShutdownRequested = false;
	#runtimeQuiesceRequested = false;
	#runtimeCheckpointExpired = false;
	#quiesceController = new AbortController();
	#drainDependencyAllowed: (() => boolean) | undefined;
	#quiesceModelRequestId: string | undefined;
	#reviewDependencies = new Map<string, string>();
	#cooperativeCancelRequested = false;
	#userInterrupt: RuntimeUserInterruptState | undefined;
	#lastUserInterruptCommit: {
		readonly runtimeInputId: string;
		readonly result: RuntimeControlInputCommitResult;
	} | undefined;
	constructor(sessionId: string) {
		this.contextManager = new ContextManager(sessionId, [], () => this.#currentRequestMessage, message => this.messageHistoricallyEligible(message));
	}

	persistentContextLoaded(): boolean {
		return this.#persistentContextLoaded;
	}

	markPersistentContextLoaded(): void {
		this.#persistentContextLoaded = true;
		this.#residencyValid = true;
	}

	invalidateResidentState(): void {
		this.#residencyValid = false;
		this.#persistentContextLoaded = false;
		this.cancelApprovalObservations();
	}
	/** Ends local timing only; the durable approval remains governed by its receipt. */
	cancelApprovalObservations(): void {
		for (const tool of this.#activeTools.values())
			tool.finishApprovalObservation?.("cancelled");
	}
	/** Unpublished cold staging only; full route/reducer validation gates publication. */
	installThreadCheckpoint(checkpoint: ThreadTurnCheckpoint | undefined): void {
		this.#threadTurnCheckpoint = parseThreadTurnCheckpoint(checkpoint ?? { pendingInputContextSequences: [] });
	}

	installThreadTurn(checkpoint: ThreadTurnCheckpoint | undefined, routeView: ThreadToolRouteView | undefined): void {
		const initialized = initializeThreadTurnTransition(checkpoint ?? { pendingInputContextSequences: [] }, routeView ?? { routes: [] }, this.acceptedInputIds(), this.activeInputView());
		// Cold controls must exactly match the validated route projection before publication.
		if (routeView !== undefined) {
			const routes = new Map(routeView.routes.map(route => [route.toolUseEventId, route.disposition]));
			if (routes.size !== routeView.routes.length || routes.size !== this.#activeTools.size || [...this.#activeTools.values()].some(tool => routes.get(tool.toolUseEventId) !== tool.disposition)) {
				this.invalidateResidentState();
				throw new Error("cold routes do not match registered Tool controls");
			}
		}
		this.#threadTurnCheckpoint = initialized.checkpoint;
	}

	hasUnsettledToolOwner(): boolean {
		return this.#threadTurnCheckpoint.request?.toolMembers.some(member => member.memberKind === "public_tool_use" && member.terminalResult === undefined) ?? false;
	}
	/** Observers read correlation only; they must not validate or dispatch a turn. */
	requestObservationScope(): Pick<NonNullable<ThreadTurnCheckpoint["request"]>, "modelRequestId" | "requestKind"> | undefined {
		const request = this.#threadTurnCheckpoint.request;
		return request === undefined ? undefined : {
			modelRequestId: request.modelRequestId,
			requestKind: request.requestKind,
		};
	}

	threadTurnTransition(): ThreadTurnTransition {
		return {
			checkpoint: this.#threadTurnCheckpoint,
			...deriveThreadTurnSnapshot(this.#threadTurnCheckpoint, this.threadToolRouteView(), this.acceptedInputIds(), this.activeInputView()),
		};
	}

	applyThreadTurnFact(fact: ThreadTurnFact): ThreadTurnTransition {
		return this.applyThreadTurnFactFrom(this.threadTurnTransition(), fact);
	}

	applyRequestStartFact(owner: ThreadTurnTransition, fact: Extract<ThreadTurnFact, {
		readonly fact: "request_started";
	}>): ThreadTurnTransition {
		return this.applyThreadTurnFactFrom(owner, fact);
	}

	applyFinishIdleFact(owner: ThreadTurnTransition, fact: Extract<ThreadTurnFact, {
		readonly fact: "finish_idle_committed";
	}>): ThreadTurnTransition {
		const current = this.threadTurnTransition();
		if (owner.checkpoint.executionRunId !== current.checkpoint.executionRunId ||
			owner.checkpoint.request?.modelRequestId !==
				current.checkpoint.request?.modelRequestId) {
			throw new ThreadTurnContractError("Thread turn changed while FinishIdle was in flight");
		}
		const settlementOwner = {
			checkpoint: current.checkpoint,
			state: owner.state,
			nextStep: owner.nextStep,
		};
		return this.applyThreadTurnFactFrom(settlementOwner, fact);
	}

	currentRequestMessage(): RuntimeCurrentRequestMessage | undefined {
		return this.#currentRequestMessage;
	}

	associateCurrentRequestMessage(reference: RuntimeCurrentRequestMessage): void {
		const request = this.#threadTurnCheckpoint.request;
		const current = this.#currentRequestMessage;
		if (request?.modelRequestId !== reference.modelRequestId || (current !== undefined && (current.modelRequestId !== reference.modelRequestId || current.assistantMessageSequence !== reference.assistantMessageSequence)))
			throw new Error("Assistant ACK changed current request identity");
		if (this.contextManager.entry(reference.assistantMessageSequence)?.contextKind !== "assistant")
			throw new Error("Assistant reference has no committed message");
		this.#currentRequestMessage = reference;
	}

	installCurrentRequestMessage(reference: RuntimeCurrentRequestMessage | undefined): void {
		this.#currentRequestMessage = undefined;
		if (reference !== undefined)
			this.associateCurrentRequestMessage(reference);
	}

	private messageHistoricallyEligible(message: RuntimeContextEntry): boolean {
		const reference = this.#currentRequestMessage, request = this.#threadTurnCheckpoint.request;
		if (reference === undefined || reference.assistantMessageSequence !== message.messageSequence)
			return true;
		if (request === undefined || request.modelRequestId !== reference.modelRequestId)
			throw new Error("current Assistant reference lost checkpoint owner");
		const end = request.requestEnd;
		if (end === undefined)
			return false;
		if (end.providerContextRetention.disposition === "completed" || end.providerContextRetention.disposition === "compacted")
			return true;
		const retained = new Set(end.providerContextRetention.toolUseEventIds);
		return !request.toolMembers.some(member => member.memberKind === "public_tool_use" && retained.has(member.toolUseEventId) && member.terminalResult === undefined);
	}

	private threadToolRouteView(): ThreadToolRouteView {
		return { routes: [...this.#activeTools.values()].map(tool => ({ toolUseEventId: tool.toolUseEventId, disposition: tool.disposition })) };
	}

	registerActiveTool(input: {
		readonly toolUseEventId: string;
		readonly modelRequestId: string;
		readonly modelToolCallId: string;
		readonly assistantMessageSequence: number;
		readonly disposition: ThreadToolRouteView["routes"][number]["disposition"];
	}): void {
		const message = this.contextManager.entry(input.assistantMessageSequence);
		const calls = message?.parts.filter(part => part.type === "tool_call" && part.modelToolCallId === input.modelToolCallId) ?? [];
		if (!this.#residencyValid || message?.contextKind !== "assistant" || calls.length !== 1 || message.parts.some(part => part.type === "tool_result" && part.modelToolCallId === input.modelToolCallId))
			throw new Error("active Tool must reference one committed nonterminal Assistant call");
		if (this.#threadTurnCheckpoint.request?.modelRequestId !== input.modelRequestId || this.#currentRequestMessage?.modelRequestId !== input.modelRequestId || this.#currentRequestMessage.assistantMessageSequence !== input.assistantMessageSequence)
			throw new Error("active Tool must belong to current request association");
		const existing = this.#activeTools.get(input.toolUseEventId);
		if (existing !== undefined && (existing.modelRequestId !== input.modelRequestId || existing.modelToolCallId !== input.modelToolCallId || existing.assistantMessageSequence !== input.assistantMessageSequence))
			throw new Error("active Tool identity conflict");
		this.#activeTools.set(input.toolUseEventId, { ...existing, ...input });
	}

	recordThreadToolRoute(toolUseEventId: string, disposition: ThreadToolRouteView["routes"][number]["disposition"]): void {
		const tool = this.#activeTools.get(toolUseEventId);
		if (tool === undefined)
			throw new Error("route must identify registered committed Tool");
		this.#activeTools.set(toolUseEventId, { ...tool, disposition });
	}

	clearThreadToolRoute(toolUseEventId: string): void {
		this.#activeTools.get(toolUseEventId)?.finishApprovalObservation?.("cancelled");
		this.#activeTools.delete(toolUseEventId);
	}

	installApprovalObservation(toolUseEventId: string, finish: NonNullable<RuntimeActiveTool["finishApprovalObservation"]>): void {
		const tool = this.#activeTools.get(toolUseEventId);
		if (tool?.disposition !== "requires_user_action")
			throw new Error("approval observation requires a pending control");
		tool.finishApprovalObservation?.("cancelled");
		this.#activeTools.set(toolUseEventId, { ...tool, finishApprovalObservation: finish });
	}

	activeTools(): readonly RuntimeActiveTool[] {
		return [...this.#activeTools.values()];
	}

	resolveActiveTool(toolUseEventId: string, expected: {
		readonly modelRequestId: string;
		readonly modelToolCallId: string;
		readonly assistantMessageSequence: number;
	}, entry: ToolEntry) {
		if (!this.#residencyValid || this.#runtimeShutdownRequested || this.#runtimeCheckpointExpired)
			return undefined;
		const reference = this.#activeTools.get(toolUseEventId);
		if (reference === undefined || reference.modelRequestId !== expected.modelRequestId || reference.modelToolCallId !== expected.modelToolCallId || reference.assistantMessageSequence !== expected.assistantMessageSequence)
			return undefined;
		const message = this.contextManager.entry(reference.assistantMessageSequence);
		const calls = message?.parts.filter(part => part.type === "tool_call" && part.modelToolCallId === reference.modelToolCallId) ?? [];
		if (message?.contextKind !== "assistant" || calls.length !== 1 || message.parts.some(part => part.type === "tool_result" && part.modelToolCallId === reference.modelToolCallId))
			return undefined;
		const call = calls[0]!;
		if (call.type !== "tool_call" || call.toolName !== entry.definition.name)
			return undefined;
		const input = executionInputForToolCall(entry, call.canonicalInput);
		if (input === undefined)
			return undefined;
		const toolPart = RuntimeAssistantDraftPartSchema.parse({ type: "tool", modelToolCallId: call.modelToolCallId, toolName: call.toolName, toolUseEventId, state: { status: "running", input: boundRuntimeJson(call.canonicalInput, RuntimePreviewTextMaxBytes) } });
		if (toolPart.type !== "tool")
			return undefined;
		return { reference, message, input, toolPart };
	}

	currentModel(): SessionCurrentModel | undefined {
		return this.#currentModel;
	}

	updateCurrentModel(model: SessionCurrentModel): void {
		if (this.#currentModel !== undefined &&
			(this.#currentModel.providerId !== model.providerId ||
				this.#currentModel.modelId !== model.modelId)) {
			this.clearLastRequestCompletion();
		}
		this.#currentModel = model;
	}

	enqueueAcceptedInput(state: RuntimeAcceptedInputState): "applied" | "duplicate" | "conflict" {
		const existing = this.#acceptedInputs.find((input) => input.runtimeInputId === state.runtimeInputId);
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
		this.#acceptedInputs = this.#acceptedInputs.filter((input) => input.runtimeInputId !== runtimeInputId);
	}

	discardQueuedApprovalReview(reviewId: string): void {
		this.#acceptedInputs = this.#acceptedInputs.filter((input) => input.kind !== "approval_review" || input.reviewId !== reviewId);
	}

	beginAcceptedInputCommit(runtimeInputId: string): void {
		this.#committingAcceptedInputId = runtimeInputId;
	}

	finishAcceptedInputCommit(runtimeInputId: string): void {
		if (this.#committingAcceptedInputId === runtimeInputId) {
			this.#committingAcceptedInputId = undefined;
		}
	}

	discardQueuedAcceptedInputsForInterrupt(preserveTaskNotifications: boolean): void {
		this.#acceptedInputs = this.#acceptedInputs.filter((input) => input.kind === "inter_agent_message" ||
			(preserveTaskNotifications && input.kind === "task_notification") ||
			input.runtimeInputId === this.#committingAcceptedInputId);
	}

	acceptedInputCount(): number {
		return this.#acceptedInputs.length;
	}

	private applyThreadTurnFactFrom(owner: ThreadTurnTransition, fact: ThreadTurnFact): ThreadTurnTransition {
		if (owner.checkpoint !== this.#threadTurnCheckpoint) {
			throw new ThreadTurnContractError("Thread turn changed while a stack-local transition owner was active");
		}
		const transition = reduceThreadTurn(owner, fact, this.threadToolRouteView(), this.acceptedInputIds(), this.activeInputView());
		let normalized: readonly RuntimeContextEntry[];
		try {
			normalized = normalizeRequestMessages({ messages: this.contextManager.messages(), checkpoint: transition.checkpoint, ...(this.#currentRequestMessage === undefined ? {} : { currentRequestMessage: this.#currentRequestMessage }) });
		}
		catch (error) {
			this.invalidateResidentState();
			throw error;
		}
		this.#threadTurnCheckpoint = transition.checkpoint;
		const currentToolUseEventIds = new Set(this.#threadTurnCheckpoint.request?.toolMembers.flatMap((member) => member.memberKind === "public_tool_use"
			? [member.toolUseEventId]
			: []) ?? []);
		for (const tool of this.#activeTools.values())
			if (!currentToolUseEventIds.has(tool.toolUseEventId))
				this.clearThreadToolRoute(tool.toolUseEventId);
		if (this.#currentRequestMessage !== undefined && this.#currentRequestMessage.modelRequestId !== this.#threadTurnCheckpoint.request?.modelRequestId)
			this.#currentRequestMessage = undefined;
		this.contextManager.replaceMessages(normalized);
		if (this.#currentRequestMessage !== undefined && this.contextManager.entry(this.#currentRequestMessage.assistantMessageSequence) === undefined)
			this.#currentRequestMessage = undefined;
		this.contextManager.invalidateHistory();
		return transition;
	}

	private acceptedInputIds(): readonly string[] {
		return this.#acceptedInputBlockedUntilRunExit
			? []
			: this.#acceptedInputs.map((input) => input.runtimeInputId);
	}

	resolveToolConfirmation(state: RuntimeToolConfirmationState): "applied" | "duplicate" | "conflict" {
		const existing = this.#toolConfirmations[state.toolUseEventId];
		if (existing === undefined) {
			this.#toolConfirmations[state.toolUseEventId] = state;
			this.#activeTools.get(state.toolUseEventId)?.finishApprovalObservation?.(state.decision === "allow" ? "success" : "rejected");
			return "applied";
		}
		if (existing.runtimeInputId === state.runtimeInputId ||
			(existing.decision === state.decision &&
				existing.denyMessage === state.denyMessage)) {
			return "duplicate";
		}
		return "conflict";
	}

	toolConfirmation(toolUseEventId: string): RuntimeToolConfirmationState | undefined {
		return this.#toolConfirmations[toolUseEventId];
	}

	private recordToolControl(state: RuntimePendingApprovalToolJobState | RuntimeResolvedToolRouteJobState | RuntimePendingSandboxExecutionJobState, disposition: RuntimeActiveTool["disposition"]): void {
		this.registerActiveTool({ toolUseEventId: state.toolUseEventId, modelRequestId: state.modelRequestId, modelToolCallId: state.job.modelToolCallId, assistantMessageSequence: state.assistantMessageSequence, disposition });
		const { input: _input, result: _result, ...job } = state.job;
		this.#activeTools.set(state.toolUseEventId, { ...this.#activeTools.get(state.toolUseEventId)!, source: state.source, job, entry: state.entry, ...(state.currentModel === undefined ? {} : { currentModel: state.currentModel }), ...("decision" in state ? { decision: state.decision } : {}), ...("denyMessage" in state ? { denyMessage: state.denyMessage } : {}) });
	}

	private toolControlSnapshots(disposition: RuntimeActiveTool["disposition"]): RuntimePendingApprovalToolJobState[] {
		return this.activeTools().filter(tool => tool.disposition === disposition && tool.job !== undefined && tool.entry !== undefined && tool.source !== undefined).map(tool => {
			const message = this.contextManager.entry(tool.assistantMessageSequence)!;
			const call = message.parts.find(part => part.type === "tool_call" && part.modelToolCallId === tool.modelToolCallId);
			if (call?.type !== "tool_call")
				throw new Error("active Tool lost committed call");
			const toolPart = RuntimeAssistantDraftPartSchema.parse({ type: "tool", modelToolCallId: call.modelToolCallId, toolName: call.toolName, toolUseEventId: tool.toolUseEventId, state: { status: "running", input: boundRuntimeJson(call.canonicalInput, RuntimePreviewTextMaxBytes) } });
			if (toolPart.type !== "tool")
				throw new Error("invalid active Tool snapshot");
			return { toolUseEventId: tool.toolUseEventId, modelRequestId: tool.modelRequestId, source: tool.source!, assistantMessageSequence: tool.assistantMessageSequence, toolPart, job: { ...tool.job!, input: executionInputForToolCall(tool.entry!, call.canonicalInput)! }, entry: tool.entry!, ...(tool.currentModel === undefined ? {} : { currentModel: tool.currentModel }) };
		}).sort((a, b) => a.modelRequestId.localeCompare(b.modelRequestId) || a.job.modelOrder - b.job.modelOrder);
	}

	recordPendingApprovalToolJob(state: RuntimePendingApprovalToolJobState): void {
		this.recordToolControl(state, "requires_user_action");
	}

	pendingApprovalToolJobs(): readonly RuntimePendingApprovalToolJobState[] {
		return this.toolControlSnapshots("requires_user_action");
	}

	removePendingApprovalToolJob(id: string): void {
		if (this.#activeTools.get(id)?.disposition === "requires_user_action")
			this.clearThreadToolRoute(id);
		delete this.#toolConfirmations[id];
	}

	recordResolvedToolRouteJob(state: RuntimeResolvedToolRouteJobState): void {
		this.recordToolControl(state, "resume_approval_settlement");
	}

	resolvedToolRouteJobs(): readonly RuntimeResolvedToolRouteJobState[] {
		return this.toolControlSnapshots("resume_approval_settlement").map(state => {
			const tool = this.#activeTools.get(state.toolUseEventId)!;
			return { ...state, recoveryKind: "resolved_route", decision: tool.decision!, ...(tool.denyMessage === undefined ? {} : { denyMessage: tool.denyMessage }) };
		});
	}

	removeResolvedToolRouteJob(id: string): void {
		if (this.#activeTools.get(id)?.disposition === "resume_approval_settlement")
			this.clearThreadToolRoute(id);
	}

	hasPendingApprovalToolJobs(): boolean {
		return this.activeTools().some(tool => tool.disposition === "requires_user_action");
	}

	recordPendingSandboxExecutionJob(state: RuntimePendingSandboxExecutionJobState): void {
		this.recordToolControl(state, "resume_sandbox_execution");
	}

	pendingSandboxExecutionJobs(): readonly RuntimePendingSandboxExecutionJobState[] {
		return this.toolControlSnapshots("resume_sandbox_execution").map(state => ({ ...state, recoveryKind: "sandbox_execution" }));
	}

	removePendingSandboxExecutionJob(id: string): void {
		if (this.#activeTools.get(id)?.disposition === "resume_sandbox_execution")
			this.clearThreadToolRoute(id);
	}

	commitTaskNotification(state: RuntimeTaskNotificationState): "applied" | "duplicate" | "conflict" {
		const durableEntry = this.contextManager.entry(state.committedEntry.messageSequence);
		if (durableEntry === undefined) {
			this.contextManager.appendEntry(state.committedEntry);
			return "applied";
		}
		return JSON.stringify(durableEntry) === JSON.stringify(state.committedEntry)
			? "duplicate"
			: "conflict";
	}

	addPendingAttachments(attachments: readonly RuntimeProviderAttachment[]): void {
		const existing = new Set([
			...(this.#activeAttachmentRide ?? []),
			...this.#pendingAttachments,
		].map(runtimeProviderAttachmentIdentityKey));
		const additions = attachments.filter((attachment) => {
			const identity = runtimeProviderAttachmentIdentityKey(attachment);
			if (existing.has(identity))
				return false;
			existing.add(identity);
			return true;
		});
		const available = Math.max(0, MaxProviderAttachments - this.#pendingAttachments.length);
		this.#pendingAttachments.push(...additions.slice(0, available).map(cloneRuntimeProviderAttachment));
	}

	pendingAttachments(): readonly RuntimeProviderAttachment[] {
		return [
			...(this.#activeAttachmentRide ?? []),
			...this.#pendingAttachments,
		].map(cloneRuntimeProviderAttachment);
	}

	beginPendingAttachmentRide(): readonly RuntimeProviderAttachment[] {
		if (this.#activeAttachmentRide === undefined &&
			this.#pendingAttachments.length > 0) {
			this.#activeAttachmentRide = this.#pendingAttachments;
			this.#pendingAttachments = [];
			this.#fileBackedRideConsumed = false;
		}
		return (this.#activeAttachmentRide ?? []).map(cloneRuntimeProviderAttachment);
	}

	consumeFileBackedAttachmentRide(): void {
		if (this.#activeAttachmentRide !== undefined) {
			const before = this.#activeAttachmentRide.length;
			this.#activeAttachmentRide = this.#activeAttachmentRide.filter((attachment) => attachment.fileBacked === undefined);
			this.#fileBackedRideConsumed ||=
				this.#activeAttachmentRide.length !== before;
		}
	}

	settlePendingAttachmentRide(): void {
		this.#activeAttachmentRide = undefined;
		this.#fileBackedRideConsumed = false;
	}

	replacePendingAttachments(attachments: readonly RuntimeProviderAttachment[]): void {
		this.#activeAttachmentRide = undefined;
		this.#fileBackedRideConsumed = false;
		this.#pendingAttachments = [];
		const available = Math.min(attachments.length, MaxProviderAttachments);
		this.#pendingAttachments.push(...attachments
			.slice(0, available)
			.map(cloneRuntimeProviderAttachment));
	}

	private hasPendingAttachments(): boolean {
		return ((this.#activeAttachmentRide?.length ?? 0) > 0 ||
			this.#pendingAttachments.length > 0);
	}

	private activeInputView(): ThreadActiveInputView {
		return { hasPendingAttachments: this.hasPendingAttachments() };
	}

	recordLastRequestCompletion(usage: RuntimeUsage, limits: RuntimeModelLimits, contextAnchorSequence: number): void {
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
	/** Closes ordinary step admission while permitting an explicitly owned reviewer dependency. */
	beginRuntimeQuiesce(dependencyAllowed?: () => boolean): void {
		if (!this.#runtimeQuiesceRequested)
			this.#quiesceModelRequestId =
				this.#threadTurnCheckpoint.request?.modelRequestId;
		this.#runtimeQuiesceRequested = true;
		this.#drainDependencyAllowed = dependencyAllowed;
		// A reviewer may already be awaiting Read with this signal. Preserve that
		// admitted dependency across the scheduling fence; expiry still cancels
		// its owning run and joins the real transport before resource close.
		if (!dependencyAllowed?.())
			this.#quiesceController.abort();
	}

	beginRuntimeCheckpointExpiry(): void {
		this.#runtimeCheckpointExpired = true;
	}

	runtimeCheckpointExpired(): boolean {
		return this.#runtimeCheckpointExpired;
	}
	/** Quiesce is a scheduling fence; it does not cancel the already admitted step. */
	runtimeCheckpointYieldRequested(): boolean {
		return this.#runtimeQuiesceRequested && !this.#drainDependencyAllowed?.();
	}
	/** Only the exact tool in the already admitted parent request can extend a drain. */
	beginReviewDependency(modelRequestId: string, modelToolCallId: string): () => void {
		this.#reviewDependencies.set(modelToolCallId, modelRequestId);
		return () => {
			this.#reviewDependencies.delete(modelToolCallId);
		};
	}

	allowsReviewDependency(modelToolCallId: string): boolean {
		const requestId = this.#reviewDependencies.get(modelToolCallId);
		return (requestId !== undefined &&
			(!this.#runtimeQuiesceRequested ||
				requestId === this.#quiesceModelRequestId));
	}

	checkpointSignal(): AbortSignal | undefined {
		return this.#drainDependencyAllowed?.()
			? undefined
			: this.#quiesceController.signal;
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

	beginUserInterrupt(command: RuntimeInterruptCommandState, commitInput: RuntimeControlInputCommit, completeCloseout: () => void = () => {
	}): "applied" | "duplicate" | "conflict" {
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

	async commitUserInterruptInput(declaration: RuntimeControlInputDeclaration): Promise<RuntimeControlInputCommitApplication> {
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
				if (interrupt.commitPromise === commitPromise &&
					interrupt.commitResult === undefined) {
					interrupt.commitPromise = undefined;
				}
			});
			interrupt.commitPromise = commitPromise;
		}
		return await interrupt.commitPromise;
	}

	recordJoinedUserInterruptResult(runtimeInputId: string, result: RuntimeControlInputCommitResult = { ok: true, joined: true }, declaration: RuntimeControlInputDeclaration = {
		inputKind: "interrupt",
	}): boolean {
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

	userInterruptCommitResult(runtimeInputId: string): RuntimeControlInputCommitResult | undefined {
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
		this.invalidateResidentState();
		// Generic failure cleanup cannot erase an accepted Runtime input. The
		// Session owner must first land an exact stale/close/termination result;
		// only that durable custody handoff may use clearAfterCustodyHandoff.
		if (this.acceptedInputCount() > 0) {
			return;
		}
		const checkpoint = this.#threadTurnCheckpoint;
		const activeTools = this.activeTools();
		const messages = this.contextManager.messages(), reference = this.#currentRequestMessage;
		this.clearAfterCustodyHandoff();
		// Generic hot-state cleanup precedes failed-run closeout. Preserve the
		// reducer checkpoint as the sole active-turn owner until that durable
		// closeout lands; the Session owner performs the final custody clear.
		this.#threadTurnCheckpoint = checkpoint;
		if (activeTools.length > 0) {
			this.contextManager.replaceMessages(messages);
			this.#currentRequestMessage = reference;
		}
		for (const tool of activeTools)
			this.#activeTools.set(tool.toolUseEventId, tool);
	}

	clearAfterCustodyHandoff(): void {
		this.contextManager.clear();
		this.#persistentContextLoaded = false;
		this.#currentModel = undefined;
		this.#acceptedInputs = [];
		this.#committingAcceptedInputId = undefined;
		this.#acceptedInputBlockedUntilRunExit = false;
		this.#toolConfirmations = Object.create(null) as Record<string, RuntimeToolConfirmationState | undefined>;
		for (const tool of this.#activeTools.values())
			this.clearThreadToolRoute(tool.toolUseEventId);
		this.#currentRequestMessage = undefined;
		this.#activeAttachmentRide = undefined;
		this.#fileBackedRideConsumed = false;
		this.#pendingAttachments = [];
		this.#threadTurnCheckpoint = { pendingInputContextSequences: [] };
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
function cloneRuntimeProviderAttachment(attachment: RuntimeProviderAttachment): RuntimeProviderAttachment {
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
function runtimeProviderAttachmentIdentityKey(attachment: RuntimeProviderAttachment): string {
	return attachment.transient !== undefined
		? JSON.stringify(["transient", attachment.transient.attachmentRef])
		: JSON.stringify([
			"file-backed",
			attachment.fileBacked.sourceEventId,
			attachment.fileBacked.fileId,
		]);
}
function sameAcceptedInput(left: RuntimeAcceptedInputState, right: RuntimeAcceptedInputState): boolean {
	if (left.kind === "inter_agent_message" &&
		right.kind === "inter_agent_message") {
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
