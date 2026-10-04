import type {
	RuntimeContentKind,
	RuntimeContentCommitPhase,
	RuntimeContentCommitOutcome,
	RuntimeProviderStreamKind,
} from "./metrics.js";
import { RuntimePreviewTextMaxBytes } from "../llm/llm-event.js";
/**
 * Accumulates one provider request's uncommitted stream members. Durable
 * current-Turn state belongs to ThreadState; this request-
 * local accumulator only freezes provider order and applies operation-specific Bridge results.
 */
import { createHash } from "node:crypto";
import type {
	RuntimeAssistantContextAppend,
	RuntimeAssistantDraftPart,
	RuntimeBoundedJson,
	RuntimeContextEntry,
	RuntimeContextPart,
	RuntimeFailure,
	RuntimeInternalToolRepairCommit,
	RuntimeInternalToolRepairCommitResult,
	RuntimeInternalToolRepairStoreError,
	RuntimeJsonValue,
	RuntimeProcessorSource,
	RuntimeToolSettlement,
	RuntimeToolSettlementDeclaration,
	RuntimeToolRouteCapability,
	SessionEvent,
	SessionEventWriterAppendEvent,
	SessionEventWriterAppendResult,
	SessionEventWriterError,
	SessionEventWriterToolSettlementAttempt,
	SessionEventWriterToolSettlementEnvelope,
} from "../contracts/runtime.js";
import {
	boundRuntimeText,
	MaxStableReasoningBytesPerRequest,
	MaxStableReasoningPartsPerRequest,
	normalizeRuntimeInternalToolRepairStoreError,
	normalizeSessionEventWriterError,
	RuntimeAssistantContextAppendSchema,
	RuntimeAssistantDraftPartSchema,
	RuntimeBoundedJsonSchema,
	RuntimeFailureSchema,
	RuntimeInternalToolRepairCommitSchema,
	RuntimeToolSettlementDeclarationSchema,
	runtimeToolErrorFromFailure,
	SessionEventSchema,
	SessionEventWriterAppendEventSchema,
	stableReasoningMetadataJSON,
} from "../contracts/runtime.js";
import type { LLMEvent } from "../llm/llm-event.js";
import {
	applyAssistantAppendResult,
	applyInternalToolRepairResult,
	contextToolResultFromSettlement,
	internalToolRepairContext,
} from "./runtime-declaration.js";
export type RequestContentProcessorResult = {
	readonly ok: true;
	readonly events: readonly SessionEvent[];
	readonly durableEventIds?: readonly string[];
	readonly createdToolUseEventIds?: readonly string[];
} | {
	readonly ok: true;
	readonly type: "stale_custody";
	readonly events: readonly [
	];
} | {
	readonly ok: false;
	readonly events: readonly SessionEvent[];
	readonly error: RuntimeFailure;
};
export type ToolSettlementApplicationResult = {
	readonly type: "settled";
} | {
	readonly type: "stale_custody";
} | {
	readonly type: "failed";
	readonly error: RuntimeFailure;
};
export type ToolUseCommitResult = {
	readonly ok: true;
	readonly events: readonly SessionEvent[];
	readonly toolUseEventId: string;
} | {
	readonly ok: false;
	readonly type: "stale_custody";
	readonly events: readonly [
	];
} | {
	readonly ok: false;
	readonly events: readonly SessionEvent[];
	readonly error: RuntimeFailure;
};
export type PublicToolEvent = {
	readonly kind: "tool";
} | {
	readonly kind: "mcp";
	readonly mcpServerName: string;
};
export type { RuntimeProcessorSource, RuntimeToolSettlement, } from "../contracts/runtime.js";
/** Frozen provider-order member owned by one request-local sequencer. */
export interface FrozenAssistantPartAppend {
	readonly source: RuntimeProcessorSource;
	readonly append: RuntimeAssistantContextAppend;
	readonly event: Promise<SessionEventWriterAppendEvent | undefined>;
	readonly distinctProviderInput?: RuntimeJsonValue | undefined;
	readonly toolRouteCapability?: RuntimeToolRouteCapability | undefined;
	readonly toolCallId?: string | undefined;
	readonly preallocatedEventId?: string;
}

export interface MemberCommitHandle {
	readonly committed: Promise<RequestContentProcessorResult>;
}

export interface AssistantMemberSequencer {
	enqueue(append: FrozenAssistantPartAppend): MemberCommitHandle;
	awaitDrained(): Promise<void>;
}

/** Serializes immutable Assistant members by provider observation order. */
export class RequestAssistantMemberSequencer implements AssistantMemberSequencer {
	private tail: Promise<void> = Promise.resolve();
	private failure: RequestContentProcessorResult | undefined;
	private pendingEntries = 0;
	private pendingBytes = 0;
	private readonly capacityWaiters = new Set<() => void>();
	constructor(
		private readonly commit: (append: FrozenAssistantPartAppend) => Promise<RequestContentProcessorResult>,
		private readonly limits = { maxPendingEntries: 64, maxPendingBytes: 32 * 1024 * 1024 },
		private readonly onCommitException: () => void = () => {},
		private readonly onSubmissionDelta: (entries: number, bytes: number) => void = () => {},
	) {
		if (!Number.isSafeInteger(limits.maxPendingEntries) || limits.maxPendingEntries <= 0 || !Number.isSafeInteger(limits.maxPendingBytes) || limits.maxPendingBytes <= 0)
			throw new Error("submission limits must be positive safe integers");
	}

	private observeDelta(entries: number, bytes: number): void {
		try {
			this.onSubmissionDelta(entries, bytes);
		}
		catch {
		}
	}

	enqueue(append: FrozenAssistantPartAppend): MemberCommitHandle {
		return this.enqueueOperation(() => this.commit(append), Buffer.byteLength(JSON.stringify(append.append), "utf8"));
	}

	enqueueOperation(commit: () => Promise<RequestContentProcessorResult>, bytes: number): MemberCommitHandle {
		if (this.failure !== undefined) return { committed: Promise.resolve(this.failure) };
		// Admission is separate from provider-order enqueue, but every caller must
		// account the exact frozen submission before transferring custody here.
		if (!Number.isSafeInteger(bytes) || bytes < 0 || this.pendingEntries >= this.limits.maxPendingEntries || this.pendingBytes + bytes > this.limits.maxPendingBytes)
			throw new Error("semantic submission was not admitted within its budget");
		this.pendingEntries++;
		this.pendingBytes += bytes;
		this.observeDelta(1, bytes);
		const committed = this.tail.then(async () => {
			if (this.failure !== undefined)
				return this.failure;
			let result: RequestContentProcessorResult;
			try {
				result = await commit();
			}
			catch {
				this.onCommitException();
				result = declarationApplicationFailure();
			}
			if (!result.ok || ("type" in result && result.type === "stale_custody"))
				this.failure = result;
			return result;
		}).finally(() => {
			this.pendingEntries--;
			this.pendingBytes -= bytes;
			this.observeDelta(-1, -bytes);
			for (const wake of this.capacityWaiters)
				wake();
		});
		this.tail = committed.then(() => undefined, () => undefined);
		return { committed };
	}

	async awaitCapacity(bytes: number, signal?: AbortSignal): Promise<void> {
		if (signal?.aborted) throw signal.reason;
		if (bytes > this.limits.maxPendingBytes)
			throw new Error("semantic member exceeds submission budget");
		while (this.failure === undefined && (this.pendingEntries >= this.limits.maxPendingEntries || this.pendingBytes + bytes > this.limits.maxPendingBytes)) {
			if (signal?.aborted)
				throw signal.reason;
			await new Promise<void>((resolve, reject) => {
				const wake = () => {
					cleanup();
					resolve();
				}, abort = () => {
					cleanup();
					reject(signal?.reason);
				}, cleanup = () => {
					this.capacityWaiters.delete(wake);
					signal?.removeEventListener("abort", abort);
				};
				this.capacityWaiters.add(wake);
				signal?.addEventListener("abort", abort, { once: true });
			});
		}
	}

	async awaitDrained(): Promise<void> {
		await this.tail;
	}
}

export interface RequestContentProcessorWriter {
	readonly appendEvent: (event: SessionEventWriterAppendEvent, _source: RuntimeProcessorSource, declaration?: {
		readonly assistantContextAppend: RuntimeAssistantContextAppend;
		readonly distinctProviderInput?: RuntimeJsonValue | undefined;
		readonly toolRouteCapability?: RuntimeToolRouteCapability | undefined;
	}, modelRequestId?: string, preallocatedEventId?: string) => Promise<SessionEventWriterAppendResult>;
	readonly settleToolResult: (envelope: SessionEventWriterToolSettlementEnvelope) => Promise<SessionEventWriterToolSettlementAttempt>;
	readonly commitInternalToolRepair: (repair: RuntimeInternalToolRepairCommit, source: RuntimeProcessorSource) => Promise<RuntimeInternalToolRepairCommitResult>;
}

export interface RequestContentProcessorOptions {
	readonly modelRequestId: string;
	readonly requestId: string;
	readonly workspaceId: string;
	readonly sessionId: string;
	readonly sessionThreadId: string;
	readonly bindingId: string;
	readonly bindingGeneration: number;
	readonly targetPodUid: string;
	readonly runtimeProcessId: string;
	readonly contextOwner: {
		messages(): readonly RuntimeContextEntry[];
		currentAssistantMessage(): RuntimeContextEntry | undefined;
		installAssistantMessage(draft: RuntimeContextEntry): void;
		appendToolResult(messageSequence: number, modelToolCallId: string, result: Extract<RuntimeContextPart, {
			readonly type: "tool_result";
		}>["result"]): void;
	};
	readonly onCommittedApplicationFailure: () => void;
	readonly monotonicMs?: () => number;
	readonly requestKind?: RuntimeProviderStreamKind;
	readonly onSubmissionDelta?: (entries: number, bytes: number) => void;
	readonly onContentCommit?: (observation: {
		readonly kind: RuntimeContentKind;
		readonly phase: RuntimeContentCommitPhase;
		readonly durationMs: number;
		readonly outcome: RuntimeContentCommitOutcome;
		readonly requestKind: RuntimeProviderStreamKind;
		/** UTF-8 bytes of the frozen canonical JSON submitted by this owner. */
		readonly canonicalJsonBytes?: number;
	}) => void;
	readonly activeToolReferences: () => readonly {
		readonly toolUseEventId: string;
		readonly modelRequestId: string;
		readonly modelToolCallId: string;
		readonly assistantMessageSequence: number;
	}[];
	readonly onAssistantMessageCommitted: (reference: {
		readonly modelRequestId: string;
		readonly assistantMessageSequence: number;
	}) => void;
	readonly maxNormalizedTextPreviewBytes?: number;
	readonly pendingSubmissionLimits?: {
		readonly maxPendingEntries: number;
		readonly maxPendingBytes: number;
	};
	readonly writer: RequestContentProcessorWriter;
	readonly onInternalToolRepairCommitted?: (fact: {
		readonly eventId: string;
		readonly modelRequestId: string;
		readonly modelToolCallId: string;
		readonly toolName: string;
	}) => void;
	readonly onToolResultCommitted?: (fact: {
		readonly toolUseEventId: string;
		readonly outcome: "success" | "error" | "cancelled";
	}) => void;
}
type TextPartCreate = Extract<RuntimeAssistantDraftPart, {
	readonly type: "text";
}>;
type ReasoningPartCreate = Extract<RuntimeAssistantDraftPart, {
	readonly type: "reasoning";
}>;
type ToolPartCreate = Extract<RuntimeAssistantDraftPart, {
	readonly type: "tool";
}>;
interface LLMEventEnvelope extends RuntimeProcessorSource {
	readonly event: LLMEvent;
}
type EnsureToolPartResult = {
	readonly ok: true;
	readonly events: readonly SessionEvent[];
	readonly part: ToolPartCreate;
} | {
	readonly ok: false;
	readonly events: readonly SessionEvent[];
	readonly error: RuntimeFailure;
};
/**
 * Request-local provider stream accumulator. It never owns a complete
 * Assistant draft: only uncommitted prefix members and current Tool shells
 * live here, and every successful write is installed from its positional ACK.
 */
export class RequestContentProcessor {
	private readonly options: RequestContentProcessorOptions;
	private readonly thinkingStarts = new Map<string, string>();
	private reasoningPartCount = 0;
	private reasoningBytes = 0;
	/** Provider shells exist only until declaration/repair ACK; never committed copies. */
	private readonly undeclaredTools = new Map<string, ToolPartCreate>();
	private pendingPrefix: RuntimeAssistantDraftPart[] = [];
	private semanticMemberCount = 0;
	private terminal = false;
	private readonly memberSequencer: RequestAssistantMemberSequencer;
	private readonly reservedToolMembers = new Map<string, {
		readonly authorize: (event: SessionEventWriterAppendEvent | undefined) => void;
		readonly committed: Promise<RequestContentProcessorResult>;
	}>();
	private now(): number {
		return this.options.monotonicMs?.() ?? performance.now();
	}

	private observeCommit(
		kind: RuntimeContentKind,
		phase: RuntimeContentCommitPhase,
		startedAt: number,
		outcome: RuntimeContentCommitOutcome,
		canonicalJsonBytes?: number,
	): void {
		try {
			this.options.onContentCommit?.({
				kind, phase, outcome,
				requestKind: this.options.requestKind ?? "agent_provider_request",
				durationMs: Math.max(0, this.now() - startedAt),
				...(canonicalJsonBytes === undefined ? {} : { canonicalJsonBytes }),
			});
		}
		catch {
		}
	}
	private async observedCommit<A>(
		kind: RuntimeContentKind,
		run: () => Promise<A>,
		outcome: (result: A) => RuntimeContentCommitOutcome,
		canonicalJsonBytes?: number,
	): Promise<A> {
		const startedAt = this.now();
		try {
			const result = await run();
			this.observeCommit(kind, "content_commit", startedAt, outcome(result), canonicalJsonBytes);
			return result;
		}
		catch (error) {
			this.observeCommit(kind, "content_commit", startedAt, "failed", canonicalJsonBytes);
			throw error;
		}
	}

	constructor(options: RequestContentProcessorOptions) {
		this.options = options;
		this.memberSequencer = new RequestAssistantMemberSequencer(async (frozen) => {
			const event = await frozen.event;
			if (event === undefined)
				return { ok: true, events: [] };
			const kind: RuntimeContentKind = frozen.toolCallId === undefined ? "text" : "tool";
			const canonicalJsonBytes = Buffer.byteLength(JSON.stringify(frozen.append), "utf8");
			const result = await this.observedCommit(kind, () => this.options.writer.appendEvent(event, frozen.source, {
				assistantContextAppend: frozen.append,
				...(frozen.distinctProviderInput === undefined
					? {}
					: {
						distinctProviderInput: frozen.distinctProviderInput,
					}),
				...(frozen.toolRouteCapability === undefined
					? {}
					: { toolRouteCapability: frozen.toolRouteCapability }),
			}, this.options.modelRequestId, frozen.preallocatedEventId), result => result.ok ? result.type : "failed", canonicalJsonBytes);
			if (!result.ok)
				return {
					ok: false,
					events: [],
					error: eventWriterFailure(result.error),
				};
			if (result.type === "stale") {
				return { ok: true, type: "stale_custody", events: [] };
			}
			const applicationStartedAt = this.now();
			if (frozen.preallocatedEventId !== undefined && result.eventId !== frozen.preallocatedEventId) {
				this.observeCommit(kind, "content_apply", applicationStartedAt, "failed", canonicalJsonBytes);
				return this.committedApplicationFailure();
			}
			if (!this.applyMemberAppend(result, frozen.append)) {
				this.observeCommit(kind, "content_apply", applicationStartedAt, "failed", canonicalJsonBytes);
				return this.committedApplicationFailure();
			}
			this.observeCommit(kind, "content_apply", applicationStartedAt, result.type, canonicalJsonBytes);
			return { ok: true, events: [event], durableEventIds: [result.eventId], createdToolUseEventIds: "assistant" in result ? result.assistant.createdToolUseEventIds : [] };
		}, this.options.pendingSubmissionLimits, () => this.options.onCommittedApplicationFailure(), (entries, bytes) => {
			try {
				this.options.onSubmissionDelta?.(entries, bytes);
			}
			catch {
			}
		});
	}

	messages(): readonly RuntimeContextEntry[] {
		return this.options.contextOwner.messages();
	}

	currentAssistantMessage(): RuntimeContextEntry | undefined {
		return this.options.contextOwner.currentAssistantMessage();
	}

	ownsContext(owner: RequestContentProcessorOptions["contextOwner"]): boolean {
		return this.options.contextOwner === owner;
	}

	activeToolPart(modelToolCallId: string): ToolPartCreate | undefined {
		const reference = this.toolReference(modelToolCallId);
		if (reference === undefined)
			return this.undeclaredTools.get(modelToolCallId);
		const message = this.options.contextOwner.messages().find(message => message.messageSequence === reference.assistantMessageSequence);
		const call = message?.parts.find(part => part.type === "tool_call" && part.modelToolCallId === modelToolCallId);
		if (call?.type !== "tool_call") {
			this.options.onCommittedApplicationFailure();
			return undefined;
		}
		const result = message!.parts.find(part => part.type === "tool_result" && part.modelToolCallId === modelToolCallId);
		if (result !== undefined)
			return undefined;
		return parseToolPart({ type: "tool", modelToolCallId, toolName: call.toolName, toolUseEventId: reference.toolUseEventId, state: { status: "running", input: { value: call.canonicalInput, preview: "", truncated: false } } });
	}

	async awaitAssistantMembersDrained(): Promise<void> {
		await this.memberSequencer.awaitDrained();
	}
	/** Freezes the trailing reasoning prefix for the owning End submission. */
	requestEndAppend(): RuntimeAssistantContextAppend | undefined {
		if (!this.terminal)
			throw new Error("request end append requires a terminal provider stream");
		if (this.pendingPrefix.length === 0)
			return undefined;
		return RuntimeAssistantContextAppendSchema.parse({
			parts: [...this.pendingPrefix],
		});
	}
	/** Joins reserved provider positions without declaring a tool after its gate expires. */
	cancelUndeclaredToolUses(): void {
		for (const reserved of this.reservedToolMembers.values())
			reserved.authorize(undefined);
		this.reservedToolMembers.clear();
	}

	discardUncommittedMembers(): void {
		this.pendingPrefix = [];
		this.cancelUndeclaredToolUses();
		this.thinkingStarts.clear();
		this.undeclaredTools.clear();
	}
	/** Producer closeout releases unreserved work while declared positions join their ACKs. */
	discardProducerWorkingState(): void {
		this.pendingPrefix = [];
		this.thinkingStarts.clear();
		// Reserved semantic positions keep declaration inputs until ACK or explicit fence.
		for (const callId of this.undeclaredTools.keys())
			if (!this.reservedToolMembers.has(callId))
				this.undeclaredTools.delete(callId);
	}

	async process(envelope: LLMEventEnvelope, signal?: AbortSignal): Promise<RequestContentProcessorResult> {
		if (this.terminal)
			return this.failWithoutWrites(protocolSequenceFailure());
		switch (envelope.event.type) {
			case "thinking-started": return this.startThinking({ ...envelope, event: envelope.event });
			case "text-complete": return this.completeText({ ...envelope, event: envelope.event }, signal);
			case "reasoning-complete": return this.completeReasoning({ ...envelope, event: envelope.event });
			case "tool-call-complete": return this.startToolCall({ ...envelope, event: envelope.event });
			case "attachment-rejections": return { ok: true, events: [] };
			case "finish": return this.finish({ ...envelope, event: envelope.event });
			case "provider-error": return this.terminalFailure(envelope, envelope.event.error);
		}
	}

	async cancel(source: RuntimeProcessorSource, failure: RuntimeFailure): Promise<RequestContentProcessorResult> {
		if (this.terminal)
			return this.failWithoutWrites(protocolSequenceFailure());
		return await this.terminalFailure(source, failure);
	}

	async cancelOpenTools(source: RuntimeProcessorSource, failure: RuntimeFailure, externallyOwnedToolUseEventIds: ReadonlySet<string> = new Set()): Promise<ToolSettlementApplicationResult> {
		for (const reference of this.requestToolReferences()) {
			const toolCallId = reference.modelToolCallId, part = this.activeToolPart(toolCallId);
			if (part === undefined)
				continue;
			if (part.state.status !== "running" ||
				part.toolUseEventId === undefined ||
				externallyOwnedToolUseEventIds.has(part.toolUseEventId))
				continue;
			const result = await this.commitToolSettlement(source, toolCallId, {
				type: "cancelled",
				error: failure,
			});
			if (result.type !== "settled")
				return result;
		}
		return { type: "settled" };
	}
	/** Interrupt intent carries no Tool census; Bridge owns every outcome. */
	prepareInterruptSettlement(_interrupt: {
		readonly runtimeInputId: string;
	}, _failure: RuntimeFailure): void {
		if (this.terminal) {
			throw new Error("interrupt settlement requires one open request");
		}
		this.terminal = true;
		this.discardUncommittedMembers();
	}

	applyInterruptSettlement(_interrupt: {
		readonly runtimeInputId: string;
	}, results: readonly import("../contracts/runtime.js").RuntimeInterruptToolResult[]): void {
		const expected = this.unfinishedToolUseEventIds();
		if (results.length !== expected.size ||
			results.some((result) => !expected.has(result.toolUseEventId))) {
			throw new Error("interrupt result set does not match unfinished Tools");
		}
		for (const result of results) {
			const reference = this.requestToolReferences().find(tool => tool.toolUseEventId === result.toolUseEventId);
			const existing = reference === undefined ? undefined : this.activeToolPart(reference.modelToolCallId);
			if (existing === undefined || isTerminalTool(existing))
				throw new Error("interrupt result has no unfinished hot Tool");
			this.appendToolResultPart(existing.modelToolCallId, {
				type: "tool_result",
				modelToolCallId: existing.modelToolCallId,
				result: result.result,
			});
			this.options.onToolResultCommitted?.({
				toolUseEventId: result.toolUseEventId,
				outcome: result.result.type === "error" ? "error" : "cancelled",
			});
		}
	}

	unfinishedToolUseEventIds(): ReadonlySet<string> {
		return new Set(this.requestToolReferences().filter(reference => this.activeToolPart(reference.modelToolCallId) !== undefined).map(reference => reference.toolUseEventId));
	}

	private requestToolReferences() {
		return this.options.activeToolReferences().filter(reference => reference.modelRequestId === this.options.modelRequestId);
	}

	private toolReference(id: string) {
		return this.requestToolReferences().find(reference => reference.modelToolCallId === id);
	}

	async commitPublicToolUse(source: RuntimeProcessorSource, toolCallId: string, input: RuntimeJsonValue, evaluatedPermission: "allow" | "ask" | "deny", toolEvent: PublicToolEvent = { kind: "tool" }): Promise<ToolUseCommitResult> {
		const existing = this.activeToolPart(toolCallId);
		if (existing === undefined || existing.state.status !== "running")
			return { ok: false, events: [], error: protocolSequenceFailure() };
		const prior = this.toolReference(toolCallId)?.toolUseEventId;
		if (prior !== undefined)
			return { ok: true, events: [], toolUseEventId: prior };
		const reserved = this.reservedToolMembers.get(toolCallId);
		if (reserved === undefined)
			return { ok: false, events: [], error: protocolSequenceFailure() };
		const event = toolUseSessionEvent(existing, input, evaluatedPermission, toolEvent);
		reserved.authorize(event);
		const committed = await reserved.committed;
		if (!committed.ok)
			return committed;
		if ("type" in committed && committed.type === "stale_custody") {
			return { ok: false, type: "stale_custody", events: [] };
		}
		const eventId = "createdToolUseEventIds" in committed ? committed.createdToolUseEventIds?.[0] : undefined;
		if (eventId === undefined)
			return { ok: false, events: [], error: protocolSequenceFailure() };
		this.reservedToolMembers.delete(toolCallId);
		this.undeclaredTools.delete(toolCallId);
		return { ok: true, events: committed.events, toolUseEventId: eventId };
	}
	/** Reserves a Tool member at its provider position before permission work begins. */
	async reservePublicToolUse(source: RuntimeProcessorSource, toolCallId: string, toolEvent: PublicToolEvent, distinctProviderInput: RuntimeJsonValue | undefined, toolRouteCapability: RuntimeToolRouteCapability, signal?: AbortSignal): Promise<boolean> {
		if (this.reservedToolMembers.has(toolCallId))
			return true;
		const existing = this.activeToolPart(toolCallId);
		if (existing === undefined || existing.state.status !== "running")
			return false;
		let authorize!: (event: SessionEventWriterAppendEvent | undefined) => void;
		const event = new Promise<SessionEventWriterAppendEvent | undefined>((resolve) => {
			authorize = resolve;
		});
		const tool = parseToolPart({ ...existing, toolEvent });
		const append = RuntimeAssistantContextAppendSchema.parse({
			parts: [...this.pendingPrefix, tool],
		});
		try {
			await this.memberSequencer.awaitCapacity(Buffer.byteLength(JSON.stringify(append), "utf8"), signal);
			signal?.throwIfAborted();
		} catch (error) {
			if (signal?.aborted) throw error;
			return false;
		}
		this.takePendingPrefix();
		const handle = this.memberSequencer.enqueue({
			source,
			append,
			event,
			...(distinctProviderInput === undefined ? {} : { distinctProviderInput }),
			toolRouteCapability,
			toolCallId,
		});
		this.reservedToolMembers.set(toolCallId, {
			authorize,
			committed: handle.committed,
		});
		this.semanticMemberCount++;
		return true;
	}

	async commitToolSettlement(_source: RuntimeProcessorSource, toolCallId: string, settlement: RuntimeToolSettlement): Promise<ToolSettlementApplicationResult> {
		const existing = this.activeToolPart(toolCallId);
		const toolUseEventId = existing?.toolUseEventId;
		if (existing === undefined ||
			existing.state.status !== "running" ||
			toolUseEventId === undefined) {
			return { type: "failed", error: semanticSequenceFailure() };
		}
		const declaration = RuntimeToolSettlementDeclarationSchema.parse({
			toolUseEventId,
			outcome: settlement,
		});
		const envelope = {
			workspaceId: this.options.workspaceId,
			sessionId: this.options.sessionId,
			sessionThreadId: this.options.sessionThreadId,
			bindingId: this.options.bindingId,
			bindingGeneration: this.options.bindingGeneration,
			targetPodUid: this.options.targetPodUid,
			runtimeProcessId: this.options.runtimeProcessId,
			settlement: declaration,
		};
		const canonicalJsonBytes = Buffer.byteLength(JSON.stringify(envelope), "utf8");
		const settlementResult = await this.observedCommit("tool_result", () => this.options.writer.settleToolResult(envelope), result => result.ok ? result.result.type : "failed", canonicalJsonBytes);
		if (!settlementResult.ok) {
			return {
				type: "failed",
				error: eventWriterFailure(settlementResult.error),
			};
		}
		if (settlementResult.result.type === "stale") {
			return { type: "stale_custody" };
		}
		const applicationStartedAt = this.now();
		try {
			this.appendToolResultPart(toolCallId, contextToolResultFromSettlement(toolCallId, settlement));
			const outcome = settlement.type === "completed" ? "success" : settlement.type;
			this.options.onToolResultCommitted?.({ toolUseEventId, outcome });
			this.observeCommit("tool_result", "content_apply", applicationStartedAt, settlementResult.result.type, canonicalJsonBytes);
		}
		catch {
			this.observeCommit("tool_result", "content_apply", applicationStartedAt, "failed", canonicalJsonBytes);
			this.options.onCommittedApplicationFailure();
			return { type: "failed", error: semanticSequenceFailure() };
		}
		return { type: "settled" };
	}

	async commitInternalToolRepair(source: RuntimeProcessorSource, toolCallId: string, modelRequestId: string, repairKey: string, failure: RuntimeFailure, signal?: AbortSignal): Promise<RequestContentProcessorResult> {
		const existing = this.activeToolPart(toolCallId);
		if (existing === undefined || existing.state.status !== "running")
			return { ok: false, events: [], error: semanticSequenceFailure() };
		const parts = [...this.pendingPrefix];
		const reasoningPrefixContextDelta = parts.length === 0 ? undefined : RuntimeAssistantContextAppendSchema.parse({ parts });
		const payload = RuntimeInternalToolRepairCommitSchema.parse({
			workspaceId: this.options.workspaceId, sessionId: this.options.sessionId, sessionThreadId: this.options.sessionThreadId,
			bindingId: this.options.bindingId, bindingGeneration: this.options.bindingGeneration, targetPodUid: this.options.targetPodUid, runtimeProcessId: this.options.runtimeProcessId,
			modelRequestId, modelToolCallId: toolCallId, toolName: existing.toolName, repairKey, canonicalInput: existing.state.input.value,
			...(reasoningPrefixContextDelta === undefined ? {} : { reasoningPrefixContextDelta }), error: runtimeToolErrorFromFailure(failure),
		});
		const bytes = Buffer.byteLength(JSON.stringify(payload), "utf8");
		try {
			await this.memberSequencer.awaitCapacity(bytes, signal);
			signal?.throwIfAborted();
		} catch (error) {
			if (signal?.aborted) throw error;
			return this.failWithoutWrites(boundedSemanticFailure());
		}
		this.takePendingPrefix();
		return this.memberSequencer.enqueueOperation(() => this.commitReservedInternalToolRepair(source, toolCallId, modelRequestId, repairKey, failure, reasoningPrefixContextDelta, payload), bytes).committed;
	}

	private async commitReservedInternalToolRepair(source: RuntimeProcessorSource, toolCallId: string, modelRequestId: string, repairKey: string, failure: RuntimeFailure, reasoningPrefixContextDelta: RuntimeAssistantContextAppend | undefined, payload: RuntimeInternalToolRepairCommit): Promise<RequestContentProcessorResult> {
		const existing = this.activeToolPart(toolCallId);
		if (existing === undefined || existing.state.status !== "running")
			return { ok: false, events: [], error: semanticSequenceFailure() };
		const canonicalJsonBytes = Buffer.byteLength(JSON.stringify(payload), "utf8");
		const commit = await this.observedCommit("internal_tool_repair", () => this.options.writer.commitInternalToolRepair(payload, source), result => result.ok ? result.type : "failed", canonicalJsonBytes);
		if (!commit.ok)
			return { ok: false, events: [], error: storeFailure(commit.error) };
		if (commit.type === "stale")
			return {
				ok: false,
				events: [],
				error: storeFailure(normalizeRuntimeInternalToolRepairStoreError({
					code: "unavailable",
					operation: "commitInternalToolRepair",
					reason: "runtime_contract_validation",
					sessionId: this.options.sessionId,
				})),
			};
		const applicationStartedAt = this.now();
		try {
			const context = internalToolRepairContext({
				modelToolCallId: toolCallId,
				toolName: existing.toolName,
				canonicalInput: existing.state.input.value,
				error: failure,
			});
			const draft = applyInternalToolRepairResult({
				modelRequestId,
				existingDraft: this.options.contextOwner.currentAssistantMessage(),
				assignedMessageSequence: commit.assignedMessageSequence,
				context,
				reasoningPrefixContextDelta,
			});
			this.options.contextOwner.installAssistantMessage(draft);
			this.options.onAssistantMessageCommitted({ modelRequestId: this.options.modelRequestId, assistantMessageSequence: draft.messageSequence });
			this.options.onInternalToolRepairCommitted?.({
				eventId: commit.repairEventId, modelRequestId, modelToolCallId: toolCallId,
				toolName: existing.toolName,
			});
			this.observeCommit("internal_tool_repair", "content_apply", applicationStartedAt, commit.type, canonicalJsonBytes);
		}
		catch {
			this.observeCommit("internal_tool_repair", "content_apply", applicationStartedAt, "failed", canonicalJsonBytes);
			this.options.onCommittedApplicationFailure();
			return {
				ok: false,
				events: [],
				error: storeFailure(normalizeRuntimeInternalToolRepairStoreError({
					code: "schema_mismatch",
					operation: "commitInternalToolRepair",
					reason: "runtime_contract_validation",
					sessionId: this.options.sessionId,
				})),
			};
		}
		this.undeclaredTools.delete(toolCallId);
		this.semanticMemberCount++;
		return { ok: true, events: [], durableEventIds: [commit.repairEventId] };
	}

	sessionStatus(status: {
		readonly type: "idle";
		readonly stopReason?: {
			readonly type: "end_turn";
		} | {
			readonly type: "requires_action";
			readonly event_ids: string[];
		} | {
			readonly type: "retries_exhausted";
		};
	} | {
		readonly type: "busy";
	} | {
		readonly type: "retry";
	}): SessionEvent {
		return status.type === "idle"
			? SessionEventWriterAppendEventSchema.parse({
				type: "session.status_idle",
				stop_reason: status.stopReason ?? { type: "end_turn" },
			})
			: SessionEventWriterAppendEventSchema.parse({
				type: "session.status_running",
			});
	}

	private async completeText(envelope: LLMEventEnvelope & {
		readonly event: Extract<LLMEvent, {
			type: "text-complete";
		}>;
	}, signal?: AbortSignal): Promise<RequestContentProcessorResult> {
		if (envelope.event.text.length === 0)
			return { ok: true, events: [] };
		const completed = parseTextPart({ type: "text", text: envelope.event.text, truncated: false });
		const event = SessionEventWriterAppendEventSchema.parse({ type: "agent.message", content: [{ type: "text", text: completed.text }] });
		const append = RuntimeAssistantContextAppendSchema.parse({ parts: [...this.pendingPrefix, completed] });
		try {
			await this.memberSequencer.awaitCapacity(Buffer.byteLength(JSON.stringify(append), "utf8"), signal);
			signal?.throwIfAborted();
		} catch (error) {
			if (signal?.aborted) throw error;
			return this.failWithoutWrites(boundedSemanticFailure());
		}
		this.takePendingPrefix();
		this.semanticMemberCount++;
		return this.memberSequencer.enqueue({ event: Promise.resolve(event), source: envelope, append, preallocatedEventId: envelope.event.eventId }).committed;
	}

	private async startThinking(envelope: LLMEventEnvelope & {
		readonly event: Extract<LLMEvent, {
			type: "thinking-started";
		}>;
	}): Promise<RequestContentProcessorResult> {
		const event = SessionEventWriterAppendEventSchema.parse({ type: "agent.thinking" });
		if (this.thinkingStarts.has(envelope.event.providerPartId))
			return this.failWithoutWrites(protocolSequenceFailure());
		const canonicalJsonBytes = Buffer.byteLength(JSON.stringify(event), "utf8");
		const result = await this.observedCommit("thinking", () => this.options.writer.appendEvent(event, envelope, undefined, this.options.modelRequestId, envelope.event.eventId), result => result.ok ? result.type : "failed", canonicalJsonBytes);
		if (!result.ok)
			return { ok: false, events: [], error: eventWriterFailure(result.error) };
		if (result.type === "stale")
			return { ok: true, type: "stale_custody", events: [] };
		const applicationStartedAt = this.now();
		if (result.eventId !== envelope.event.eventId) {
			this.observeCommit("thinking", "content_apply", applicationStartedAt, "failed", canonicalJsonBytes);
			return this.committedApplicationFailure();
		}
		this.thinkingStarts.set(envelope.event.providerPartId, envelope.event.eventId);
		this.observeCommit("thinking", "content_apply", applicationStartedAt, result.type, canonicalJsonBytes);
		return { ok: true, events: [event], durableEventIds: [result.eventId] };
	}

	private completeReasoning(envelope: LLMEventEnvelope & {
		readonly event: Extract<LLMEvent, {
			type: "reasoning-complete";
		}>;
	}): RequestContentProcessorResult {
		if (this.thinkingStarts.get(envelope.event.providerPartId) !== envelope.event.thinkingEventId)
			return this.failWithoutWrites(protocolSequenceFailure());
		const part = parseReasoningPart({ type: "reasoning", providerPartId: envelope.event.providerPartId, text: envelope.event.text, truncated: false, ...(envelope.event.providerMetadata === undefined ? {} : { providerMetadata: envelope.event.providerMetadata }) });
		const bytes = byteLength(part.text) + byteLength(stableReasoningMetadataJSON(part.providerMetadata));
		if (this.reasoningPartCount + 1 > MaxStableReasoningPartsPerRequest || this.reasoningBytes + bytes > MaxStableReasoningBytesPerRequest)
			return this.failWithoutWrites(boundedSemanticFailure());
		this.thinkingStarts.delete(envelope.event.providerPartId);
		this.reasoningPartCount++;
		this.reasoningBytes += bytes;
		this.pendingPrefix.push(part);
		return { ok: true, events: [] };
	}

	private startToolCall(envelope: LLMEventEnvelope & {
		readonly event: Extract<LLMEvent, {
			type: "tool-call-complete";
		}>;
	}): RequestContentProcessorResult {
		const ensured = this.ensureToolPart(envelope.event.id, envelope.event.toolName);
		if (!ensured.ok)
			return ensured;
		const updated = parseToolPart({
			...ensured.part,
			toolName: envelope.event.toolName,
			state: {
				status: "running",
				input: runtimeJsonFromProvider(envelope.event.input, envelope.event.inputPreview, this.maxBytes()),
			},
		});
		this.undeclaredTools.set(envelope.event.id, updated);
		return { ok: true, events: [] };
	}

	private finish(envelope: LLMEventEnvelope & {
		readonly event: Extract<LLMEvent, {
			type: "finish";
		}>;
	}): RequestContentProcessorResult {
		if (this.thinkingStarts.size > 0 ||
			(this.semanticMemberCount === 0 && this.pendingPrefix.length === 0))
			return this.failWithoutWrites(semanticSequenceFailure());
		this.terminal = true;
		return { ok: true, events: [] };
	}

	private async terminalFailure(_source: RuntimeProcessorSource, failure: RuntimeFailure): Promise<RequestContentProcessorResult> {
		this.discardProducerWorkingState();
		this.terminal = true;
		return {
			ok: true,
			events: [
				SessionEventSchema.parse({ type: "session.error", error: failure }),
			],
		};
	}

	private ensureToolPart(toolCallId: string, toolName: string): EnsureToolPartResult {
		const existing = this.activeToolPart(toolCallId);
		if (existing !== undefined)
			return { ok: false, events: [], error: protocolSequenceFailure() };
		const part = parseToolPart({
			type: "tool",
			modelToolCallId: toolCallId,
			toolName,
			state: { status: "pending" },
		});
		this.undeclaredTools.set(toolCallId, part);
		return { ok: true, events: [], part };
	}

	private applyMemberAppend(result: Extract<SessionEventWriterAppendResult, {
		readonly ok: true;
		readonly type: "committed" | "duplicate";
	}>, append: RuntimeAssistantContextAppend): boolean {
		if (!("assistant" in result))
			return false;
		const assistant = result.assistant;
		try {
			const application = applyAssistantAppendResult({
				modelRequestId: this.options.modelRequestId,
				append,
				existingDraft: this.options.contextOwner.currentAssistantMessage(),
				result: assistant,
			});
			this.options.contextOwner.installAssistantMessage(application.draft);
			this.options.onAssistantMessageCommitted({ modelRequestId: this.options.modelRequestId, assistantMessageSequence: application.draft.messageSequence });
			return true;
		}
		catch {
			return false;
		}
	}

	private committedApplicationFailure(): RequestContentProcessorResult {
		this.options.onCommittedApplicationFailure();
		return declarationApplicationFailure();
	}

	private appendToolResultPart(toolCallId: string, resultPart: Extract<RuntimeContextPart, {
		readonly type: "tool_result";
	}>): void {
		const messageSequence = this.toolReference(toolCallId)?.assistantMessageSequence;
		if (messageSequence === undefined) {
			throw new Error("Tool result lacks its request-local context target");
		}
		this.options.contextOwner.appendToolResult(messageSequence, toolCallId, resultPart.result);
	}

	private takePendingPrefix(): RuntimeAssistantDraftPart[] {
		const prefix = this.pendingPrefix;
		this.pendingPrefix = [];
		return prefix;
	}

	private failWithoutWrites(error: RuntimeFailure): RequestContentProcessorResult {
		this.discardUncommittedMembers();
		return { ok: false, events: [], error };
	}

	private maxBytes(): number {
		return this.options.maxNormalizedTextPreviewBytes ?? RuntimePreviewTextMaxBytes;
	}
}

/** Deterministic idempotency key for one invalid internal Tool-call repair. */
export function internalToolRepairKey(modelRequestId: string, modelToolCallId: string, toolName: string): string {
	const hash = createHash("sha256");
	for (const value of [modelRequestId, modelToolCallId, toolName]) {
		hash.update(String(Buffer.byteLength(value, "utf8")), "ascii");
		hash.update(":", "ascii");
		hash.update(value, "utf8");
	}
	return `internal_invalid_tool_${hash.digest("hex")}`;
}
function parseTextPart(value: unknown): TextPartCreate {
	const part = RuntimeAssistantDraftPartSchema.parse(value);
	if (part.type !== "text")
		throw new Error("expected text part");
	return part;
}
function parseReasoningPart(value: unknown): ReasoningPartCreate {
	const part = RuntimeAssistantDraftPartSchema.parse(value);
	if (part.type !== "reasoning")
		throw new Error("expected reasoning part");
	return part;
}
function parseToolPart(value: unknown): ToolPartCreate {
	const part = RuntimeAssistantDraftPartSchema.parse(value);
	if (part.type !== "tool")
		throw new Error("expected Tool part");
	return part;
}
function runtimeJsonFromProvider(value: RuntimeJsonValue, preview: Extract<LLMEvent, {
	type: "tool-call-complete";
}>["inputPreview"], maxBytes: number): RuntimeBoundedJson {
	const bounded = boundRuntimeText(preview.preview, maxBytes);
	return RuntimeBoundedJsonSchema.parse({
		value,
		preview: bounded.text,
		truncated: preview.truncated || bounded.truncated,
	});
}
function toolUseSessionEvent(part: ToolPartCreate, input: RuntimeJsonValue, permission: "allow" | "ask" | "deny", toolEvent: PublicToolEvent): SessionEventWriterAppendEvent {
	return toolEvent.kind === "mcp"
		? SessionEventWriterAppendEventSchema.parse({
			type: "agent.mcp_tool_use",
			name: part.toolName,
			input,
			mcp_server_name: toolEvent.mcpServerName,
			evaluated_permission: permission,
		})
		: SessionEventWriterAppendEventSchema.parse({
			type: "agent.tool_use",
			name: part.toolName,
			input,
			evaluated_permission: permission,
		});
}
function isTerminalTool(part: ToolPartCreate): boolean {
	return (part.state.status === "completed" ||
		part.state.status === "error" ||
		part.state.status === "cancelled");
}
function byteLength(value: string): number {
	return Buffer.byteLength(value, "utf8");
}
function declarationApplicationFailure(): {
	readonly ok: false;
	readonly events: readonly SessionEvent[];
	readonly error: RuntimeFailure;
} {
	return {
		ok: false,
		events: [],
		error: eventWriterFailure(normalizeSessionEventWriterError({ code: "schema_mismatch" })),
	};
}
function storeFailure(error: RuntimeInternalToolRepairStoreError): RuntimeFailure {
	return RuntimeFailureSchema.parse({
		type: "message-store",
		code: error.code,
		message: error.message,
		retryable: error.retryable,
		fatal: error.fatal,
		operation: error.operation,
		...(error.reason === undefined ? {} : { reason: error.reason }),
		...(error.sessionId === undefined ? {} : { sessionId: error.sessionId }),
	});
}
function eventWriterFailure(error: SessionEventWriterError): RuntimeFailure {
	return RuntimeFailureSchema.parse({
		type: "session-event-writer",
		code: error.code,
		message: error.message,
		retryable: error.retryable,
		fatal: error.fatal,
		...(error.sessionId === undefined ? {} : { sessionId: error.sessionId }),
	});
}
function protocolSequenceFailure(): RuntimeFailure {
	return {
		type: "runtime",
		code: "gateway_protocol_error",
		message: "Runtime stream sequence is invalid.",
		retryable: false,
		fatal: true,
		reason: "runtime_contract_validation",
	};
}
function semanticSequenceFailure(): RuntimeFailure {
	return {
		type: "runtime",
		code: "runtime_invalid_sequence",
		message: "Runtime terminal result violates a semantic invariant.",
		retryable: false,
		fatal: true,
		reason: "runtime_contract_validation",
	};
}
function boundedSemanticFailure(): RuntimeFailure {
	return {
		type: "runtime",
		code: "runtime_invalid_sequence",
		message: "Runtime provider output exceeds its semantic size bound.",
		retryable: false,
		fatal: true,
		reason: "bounded",
	};
}
