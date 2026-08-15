/**
 * Accumulates one provider request's uncommitted stream members. Durable
 * current-Turn state belongs to ThreadState's ThreadProcessor; this request-
 * local accumulator only freezes provider order and applies operation-specific Bridge results.
 */
import { createHash } from "node:crypto";
import { MaxProviderContextTextJsonBytes } from "@tetral/gateway-protocol/src/bounds.js";
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
	RuntimeOpenRequestDraft,
	RuntimeProcessorSource,
	RuntimeToolSettlement,
	RuntimeToolSettlementDeclaration,
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
	RuntimeBoundedTextSchema,
	RuntimeFailureSchema,
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

export type ProviderStreamAccumulatorResult =
	| {
			readonly ok: true;
			readonly events: readonly SessionEvent[];
			readonly durableEventIds?: readonly string[];
	  }
	| {
			readonly ok: false;
			readonly events: readonly SessionEvent[];
			readonly error: RuntimeFailure;
	  };

export type ToolSettlementApplicationResult =
	| { readonly type: "settled" }
	| { readonly type: "stale_custody" }
	| { readonly type: "failed"; readonly error: RuntimeFailure };

export type ToolUseCommitResult =
	| {
			readonly ok: true;
			readonly events: readonly SessionEvent[];
			readonly toolUseEventId: string;
	  }
	| {
			readonly ok: false;
			readonly events: readonly SessionEvent[];
			readonly error: RuntimeFailure;
	  };

export type PublicToolEvent =
	| { readonly kind: "tool" }
	| { readonly kind: "mcp"; readonly mcpServerName: string };

export type {
	RuntimeProcessorSource,
	RuntimeToolSettlement,
} from "../contracts/runtime.js";

/** Frozen provider-order member owned by one request-local sequencer. */
export interface FrozenAssistantPartAppend {
	readonly source: RuntimeProcessorSource;
	readonly append: RuntimeAssistantContextAppend;
	readonly event: Promise<SessionEventWriterAppendEvent>;
	readonly toolCallId?: string | undefined;
}

export interface MemberCommitHandle {
	readonly committed: Promise<ProviderStreamAccumulatorResult>;
}

export interface AssistantMemberSequencer {
	enqueue(append: FrozenAssistantPartAppend): MemberCommitHandle;
	awaitDrained(): Promise<void>;
}

/** Serializes immutable Assistant members by provider observation order. */
export class RequestAssistantMemberSequencer
	implements AssistantMemberSequencer
{
	private tail: Promise<void> = Promise.resolve();

	constructor(
		private readonly commit: (
			append: FrozenAssistantPartAppend,
		) => Promise<ProviderStreamAccumulatorResult>,
	) {}

	enqueue(append: FrozenAssistantPartAppend): MemberCommitHandle {
		const committed = this.tail.then(() => this.commit(append));
		this.tail = committed.then(
			() => undefined,
			() => undefined,
		);
		return { committed };
	}

	async awaitDrained(): Promise<void> {
		await this.tail;
	}
}

export interface ProviderStreamAccumulatorWriter {
	readonly appendEvent: (
		event: SessionEventWriterAppendEvent,
		_source: RuntimeProcessorSource,
		declaration?: {
			readonly assistantContextAppend: RuntimeAssistantContextAppend;
		},
		modelRequestId?: string,
	) => Promise<SessionEventWriterAppendResult>;
	readonly settleToolResult: (
		envelope: SessionEventWriterToolSettlementEnvelope,
	) => Promise<SessionEventWriterToolSettlementAttempt>;
	readonly commitInternalToolRepair: (
		repair: RuntimeInternalToolRepairCommit,
		source: RuntimeProcessorSource,
	) => Promise<RuntimeInternalToolRepairCommitResult>;
}

export interface ProviderStreamAccumulatorOptions {
	readonly modelRequestId: string;
	readonly requestId: string;
	readonly workspaceId: string;
	readonly sessionId: string;
	readonly sessionThreadId: string;
	readonly bindingId: string;
	readonly bindingGeneration: number;
	readonly targetPodUid: string;
	readonly contextOwner: {
		entries(): readonly RuntimeContextEntry[];
		openRequestDraft(): RuntimeOpenRequestDraft | undefined;
		installOpenRequestDraft(draft: RuntimeOpenRequestDraft | undefined): void;
		sealOpenRequestDraft(): RuntimeContextEntry;
		appendToolResult(
			messageSequence: number,
			modelToolCallId: string,
			result: Extract<
				RuntimeContextPart,
				{ readonly type: "tool_result" }
			>["result"],
		): "sealed" | "open";
	};
	readonly maxNormalizedTextPreviewBytes?: number;
	readonly writer: ProviderStreamAccumulatorWriter;
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

type TextPartCreate = Extract<
	RuntimeAssistantDraftPart,
	{ readonly type: "text" }
>;
type ReasoningPartCreate = Extract<
	RuntimeAssistantDraftPart,
	{ readonly type: "reasoning" }
>;
type ToolPartCreate = Extract<
	RuntimeAssistantDraftPart,
	{ readonly type: "tool" }
>;
type ReasoningProviderMetadata = NonNullable<
	ReasoningPartCreate["providerMetadata"]
>;

interface LLMEventEnvelope extends RuntimeProcessorSource {
	readonly event: LLMEvent;
}

type EnsureToolPartResult =
	| {
			readonly ok: true;
			readonly events: readonly SessionEvent[];
			readonly part: ToolPartCreate;
	  }
	| {
			readonly ok: false;
			readonly events: readonly SessionEvent[];
			readonly error: RuntimeFailure;
	  };

/**
 * Request-local provider stream accumulator. It never owns a complete
 * Assistant draft: only uncommitted prefix members and current Tool shells
 * live here, and every successful write is installed from its positional ACK.
 */
export class ProviderStreamAccumulator {
	private readonly options: ProviderStreamAccumulatorOptions;
	private readonly reasoningParts = new Map<string, ReasoningPartCreate>();
	private readonly toolParts = new Map<string, ToolPartCreate>();
	private readonly toolUseEventIds = new Map<string, string>();
	private readonly toolContextSequences = new Map<string, number>();
	private pendingPrefix: RuntimeAssistantDraftPart[] = [];
	private activeTextPart: TextPartCreate | undefined;
	private activeStepIndex = 0;
	private semanticMemberCount = 0;
	private terminal = false;
	private readonly memberSequencer: RequestAssistantMemberSequencer;
	private readonly reservedToolMembers = new Map<
		string,
		{
			readonly authorize: (event: SessionEventWriterAppendEvent) => void;
			readonly committed: Promise<ProviderStreamAccumulatorResult>;
		}
	>();

	constructor(options: ProviderStreamAccumulatorOptions) {
		this.options = options;
		this.memberSequencer = new RequestAssistantMemberSequencer(
			async (frozen) => {
				const event = await frozen.event;
				const result = await this.options.writer.appendEvent(
					event,
					frozen.source,
					{ assistantContextAppend: frozen.append },
					this.options.modelRequestId,
				);
				if (!result.ok)
					return {
						ok: false,
						events: [],
						error: eventWriterFailure(result.error),
					};
				if (
					result.type === "stale" ||
					!this.applyMemberAppend(result, frozen.append)
				) {
					return declarationApplicationFailure();
				}
				return { ok: true, events: [event], durableEventIds: [result.eventId] };
			},
		);
	}

	contextEntries(): readonly RuntimeContextEntry[] {
		return this.options.contextOwner.entries();
	}

	openRequestDraft(): RuntimeOpenRequestDraft | undefined {
		return this.options.contextOwner.openRequestDraft();
	}

	ownsContext(
		owner: ProviderStreamAccumulatorOptions["contextOwner"],
	): boolean {
		return this.options.contextOwner === owner;
	}

	activeToolPart(modelToolCallId: string): ToolPartCreate | undefined {
		return this.toolParts.get(modelToolCallId);
	}

	async awaitAssistantMembersDrained(): Promise<void> {
		await this.memberSequencer.awaitDrained();
	}

	/** Returns and clears only the trailing committable reasoning/boundary prefix. */
	requestEndAppend(): RuntimeAssistantContextAppend | undefined {
		if (!this.terminal)
			throw new Error("request end append requires a terminal provider stream");
		if (this.pendingPrefix.length === 0) return undefined;
		return RuntimeAssistantContextAppendSchema.parse({
			parts: [...this.pendingPrefix],
		});
	}

	discardUncommittedMembers(): void {
		this.pendingPrefix = [];
		this.activeTextPart = undefined;
		this.reasoningParts.clear();
	}

	/** Applies the optional trailing append after the outer Request End ACK. */
	applyRequestEndAppend(
		append: RuntimeAssistantContextAppend | undefined,
		result: { readonly sealedMessageSequence?: number | undefined },
	): boolean {
		try {
			if (append !== undefined) {
				const currentDraft = this.options.contextOwner.openRequestDraft();
				const sequence =
					result.sealedMessageSequence ?? currentDraft?.messageSequence;
				if (sequence === undefined) return false;
				const application = applyAssistantAppendResult({
					modelRequestId: this.options.modelRequestId,
					append,
					existingDraft: currentDraft,
					result: { messageSequence: sequence, createdToolUseEventIds: [] },
				});
				this.options.contextOwner.installOpenRequestDraft(application.draft);
			}
			this.pendingPrefix = [];
			const openDraft = this.options.contextOwner.openRequestDraft();
			if (openDraft !== undefined) {
				if (
					result.sealedMessageSequence !== undefined &&
					result.sealedMessageSequence !== openDraft.messageSequence
				)
					return false;
				this.options.contextOwner.sealOpenRequestDraft();
			} else if (result.sealedMessageSequence !== undefined) {
				return false;
			}
			return true;
		} catch {
			return false;
		}
	}

	async process(
		envelope: LLMEventEnvelope,
	): Promise<ProviderStreamAccumulatorResult> {
		if (this.terminal) return this.failWithoutWrites(protocolSequenceFailure());
		switch (envelope.event.type) {
			case "step-start":
				return this.startStep(
					envelope as LLMEventEnvelope & {
						readonly event: Extract<LLMEvent, { type: "step-start" }>;
					},
				);
			case "step-finish":
				return this.finishStep(
					envelope as LLMEventEnvelope & {
						readonly event: Extract<LLMEvent, { type: "step-finish" }>;
					},
				);
			case "text-start":
				return this.startText(
					envelope as LLMEventEnvelope & {
						readonly event: Extract<LLMEvent, { type: "text-start" }>;
					},
				);
			case "text-delta":
				return this.appendText(
					envelope as LLMEventEnvelope & {
						readonly event: Extract<LLMEvent, { type: "text-delta" }>;
					},
				);
			case "text-end":
				return await this.endText(
					envelope as LLMEventEnvelope & {
						readonly event: Extract<LLMEvent, { type: "text-end" }>;
					},
				);
			case "reasoning-start":
				return await this.startReasoning(
					envelope as LLMEventEnvelope & {
						readonly event: Extract<LLMEvent, { type: "reasoning-start" }>;
					},
				);
			case "reasoning-delta":
				return this.appendReasoning(
					envelope as LLMEventEnvelope & {
						readonly event: Extract<LLMEvent, { type: "reasoning-delta" }>;
					},
				);
			case "reasoning-end":
				return this.endReasoning(
					envelope as LLMEventEnvelope & {
						readonly event: Extract<LLMEvent, { type: "reasoning-end" }>;
					},
				);
			case "tool-input-start":
				return this.startToolInput(
					envelope as LLMEventEnvelope & {
						readonly event: Extract<LLMEvent, { type: "tool-input-start" }>;
					},
				);
			case "tool-input-delta":
			case "tool-input-end":
			case "attachment-rejections":
				return { ok: true, events: [] };
			case "tool-call":
				return this.startToolCall(
					envelope as LLMEventEnvelope & {
						readonly event: Extract<LLMEvent, { type: "tool-call" }>;
					},
				);
			case "finish":
				return this.finish(
					envelope as LLMEventEnvelope & {
						readonly event: Extract<LLMEvent, { type: "finish" }>;
					},
				);
			case "provider-error":
				return await this.terminalFailure(envelope, envelope.event.error);
		}
	}

	async cancel(
		source: RuntimeProcessorSource,
		failure: RuntimeFailure,
	): Promise<ProviderStreamAccumulatorResult> {
		if (this.terminal) return this.failWithoutWrites(protocolSequenceFailure());
		return await this.terminalFailure(source, failure);
	}

	async cancelOpenTools(
		source: RuntimeProcessorSource,
		failure: RuntimeFailure,
		externallyOwnedToolUseEventIds: ReadonlySet<string> = new Set(),
	): Promise<ToolSettlementApplicationResult> {
		for (const [toolCallId, part] of this.toolParts) {
			if (
				part.state.status !== "running" ||
				part.toolUseEventId === undefined ||
				externallyOwnedToolUseEventIds.has(part.toolUseEventId)
			)
				continue;
			const result = await this.commitToolSettlement(source, toolCallId, {
				type: "cancelled",
				error: failure,
			});
			if (result.type !== "settled") return result;
		}
		return { type: "settled" };
	}

	/** Interrupt intent carries no Tool census; Bridge owns every outcome. */
	prepareInterruptSettlement(
		_interrupt: { readonly runtimeInputId: string },
		_failure: RuntimeFailure,
	): void {
		if (this.terminal) {
			throw new Error("interrupt settlement requires one open request");
		}
		this.terminal = true;
		this.discardUncommittedMembers();
	}

	applyInterruptSettlement(
		_interrupt: { readonly runtimeInputId: string },
		results: readonly import("../contracts/runtime.js").RuntimeInterruptToolResult[],
	): void {
		const expected = this.unfinishedToolUseEventIds();
		if (
			results.length !== expected.size ||
			results.some((result) => !expected.has(result.toolUseEventId))
		) {
			throw new Error("interrupt result set does not match unfinished Tools");
		}
		for (const result of results) {
			const existing = [...this.toolParts.values()].find(
				(part) => part.toolUseEventId === result.toolUseEventId,
			);
			if (existing === undefined || isTerminalTool(existing))
				throw new Error("interrupt result has no unfinished hot Tool");
			const state =
				result.result.type === "error"
					? {
							status: "error" as const,
							...(existing.state.status === "running"
								? { input: existing.state.input }
								: {}),
							error: result.result.error,
						}
					: {
							status: "cancelled" as const,
							...(existing.state.status === "running"
								? { input: existing.state.input }
								: {}),
							...(result.result.error === undefined
								? {}
								: { error: result.result.error }),
						};
			this.appendToolResultPart(existing.modelToolCallId, {
				type: "tool_result",
				modelToolCallId: existing.modelToolCallId,
				result: result.result,
			});
			this.toolParts.set(existing.modelToolCallId, { ...existing, state });
			this.options.onToolResultCommitted?.({
				toolUseEventId: result.toolUseEventId,
				outcome: result.result.type === "error" ? "error" : "cancelled",
			});
		}
	}

	unfinishedToolUseEventIds(): ReadonlySet<string> {
		return new Set(
			[...this.toolParts.values()]
				.filter(
					(part) => part.toolUseEventId !== undefined && !isTerminalTool(part),
				)
				.map((part) => part.toolUseEventId!),
		);
	}

	async commitPublicToolUse(
		source: RuntimeProcessorSource,
		toolCallId: string,
		input: RuntimeJsonValue,
		evaluatedPermission: "allow" | "ask" | "deny",
		toolEvent: PublicToolEvent = { kind: "tool" },
	): Promise<ToolUseCommitResult> {
		const existing = this.toolParts.get(toolCallId);
		if (existing === undefined || existing.state.status !== "running")
			return { ok: false, events: [], error: protocolSequenceFailure() };
		const prior = this.toolUseEventIds.get(toolCallId);
		if (prior !== undefined)
			return { ok: true, events: [], toolUseEventId: prior };
		const reserved = this.reservedToolMembers.get(toolCallId);
		if (reserved === undefined)
			return { ok: false, events: [], error: protocolSequenceFailure() };
		const event = toolUseSessionEvent(
			existing,
			input,
			evaluatedPermission,
			toolEvent,
		);
		reserved.authorize(event);
		const committed = await reserved.committed;
		if (!committed.ok) return committed;
		const eventId = this.toolUseEventIds.get(toolCallId);
		if (eventId === undefined)
			return { ok: false, events: [], error: protocolSequenceFailure() };
		this.reservedToolMembers.delete(toolCallId);
		const tool = parseToolPart({ ...existing, toolEvent });
		const committedPart = parseToolPart({
			...tool,
			toolUseEventId: eventId,
			toolEvent,
		});
		this.toolParts.set(toolCallId, committedPart);
		this.toolUseEventIds.set(toolCallId, eventId);
		return { ok: true, events: committed.events, toolUseEventId: eventId };
	}

	/** Reserves a Tool member at its provider position before permission work begins. */
	reservePublicToolUse(
		source: RuntimeProcessorSource,
		toolCallId: string,
		toolEvent: PublicToolEvent,
	): boolean {
		if (this.reservedToolMembers.has(toolCallId)) return true;
		const existing = this.toolParts.get(toolCallId);
		if (existing === undefined || existing.state.status !== "running")
			return false;
		let authorize!: (event: SessionEventWriterAppendEvent) => void;
		const event = new Promise<SessionEventWriterAppendEvent>((resolve) => {
			authorize = resolve;
		});
		const tool = parseToolPart({ ...existing, toolEvent });
		const append = RuntimeAssistantContextAppendSchema.parse({
			parts: [...this.takePendingPrefix(), tool],
		});
		const handle = this.memberSequencer.enqueue({
			source,
			append,
			event,
			toolCallId,
		});
		this.reservedToolMembers.set(toolCallId, {
			authorize,
			committed: handle.committed,
		});
		this.semanticMemberCount++;
		return true;
	}

	async commitToolSettlement(
		_source: RuntimeProcessorSource,
		toolCallId: string,
		settlement: RuntimeToolSettlement,
	): Promise<ToolSettlementApplicationResult> {
		const existing = this.toolParts.get(toolCallId);
		const toolUseEventId =
			existing?.toolUseEventId ?? this.toolUseEventIds.get(toolCallId);
		if (
			existing === undefined ||
			existing.state.status !== "running" ||
			toolUseEventId === undefined
		) {
			return { type: "failed", error: semanticSequenceFailure() };
		}
		const declaration = RuntimeToolSettlementDeclarationSchema.parse({
			toolUseEventId,
			outcome: settlement,
		});
		const settlementResult = await this.options.writer.settleToolResult({
			workspaceId: this.options.workspaceId,
			sessionId: this.options.sessionId,
			sessionThreadId: this.options.sessionThreadId,
			bindingId: this.options.bindingId,
			bindingGeneration: this.options.bindingGeneration,
			targetPodUid: this.options.targetPodUid,
			settlement: declaration,
		});
		if (!settlementResult.ok) {
			return {
				type: "failed",
				error: eventWriterFailure(settlementResult.error),
			};
		}
		if (settlementResult.result.type === "stale") {
			return { type: "stale_custody" };
		}
		const terminalPart = terminalToolPart(existing, settlement);
		try {
			this.appendToolResultPart(
				toolCallId,
				contextToolResultFromSettlement(toolCallId, settlement),
			);
		} catch {
			return { type: "failed", error: semanticSequenceFailure() };
		}
		this.toolParts.set(toolCallId, terminalPart);
		const outcome =
			settlement.type === "completed" ? "success" : settlement.type;
		this.options.onToolResultCommitted?.({ toolUseEventId, outcome });
		return { type: "settled" };
	}

	async commitInternalToolRepair(
		source: RuntimeProcessorSource,
		toolCallId: string,
		modelRequestId: string,
		repairKey: string,
		failure: RuntimeFailure,
	): Promise<ProviderStreamAccumulatorResult> {
		const existing = this.toolParts.get(toolCallId);
		if (existing === undefined || existing.state.status !== "running")
			return { ok: false, events: [], error: semanticSequenceFailure() };
		const commit = await this.options.writer.commitInternalToolRepair(
			{
				workspaceId: this.options.workspaceId,
				sessionId: this.options.sessionId,
				sessionThreadId: this.options.sessionThreadId,
				bindingId: this.options.bindingId,
				bindingGeneration: this.options.bindingGeneration,
				targetPodUid: this.options.targetPodUid,
				modelRequestId,
				modelToolCallId: toolCallId,
				toolName: existing.toolName,
				repairKey,
				canonicalInput: existing.state.input.value,
				error: runtimeToolErrorFromFailure(failure),
			},
			source,
		);
		if (!commit.ok)
			return { ok: false, events: [], error: storeFailure(commit.error) };
		if (commit.type === "stale")
			return {
				ok: false,
				events: [],
				error: storeFailure(
					normalizeRuntimeInternalToolRepairStoreError({
						code: "unavailable",
						operation: "commitInternalToolRepair",
						reason: "runtime_contract_validation",
						sessionId: this.options.sessionId,
					}),
				),
			};
		try {
			const context = internalToolRepairContext({
				modelToolCallId: toolCallId,
				toolName: existing.toolName,
				canonicalInput: existing.state.input.value,
				error: failure,
			});
			const draft = applyInternalToolRepairResult({
				modelRequestId,
				existingDraft: this.options.contextOwner.openRequestDraft(),
				assignedMessageSequence: commit.assignedMessageSequence,
				context,
			});
			this.options.contextOwner.installOpenRequestDraft(draft);
		} catch {
			return {
				ok: false,
				events: [],
				error: storeFailure(
					normalizeRuntimeInternalToolRepairStoreError({
						code: "schema_mismatch",
						operation: "commitInternalToolRepair",
						reason: "runtime_contract_validation",
						sessionId: this.options.sessionId,
					}),
				),
			};
		}
		this.options.onInternalToolRepairCommitted?.({
			eventId: commit.repairEventId,
			modelRequestId,
			modelToolCallId: toolCallId,
			toolName: existing.toolName,
		});
		this.semanticMemberCount++;
		return { ok: true, events: [], durableEventIds: [commit.repairEventId] };
	}

	sessionStatus(
		status:
			| {
					readonly type: "idle";
					readonly stopReason?:
						| { readonly type: "end_turn" }
						| { readonly type: "requires_action"; readonly event_ids: string[] }
						| { readonly type: "retries_exhausted" };
			  }
			| { readonly type: "busy" }
			| { readonly type: "retry" },
	): SessionEvent {
		return status.type === "idle"
			? SessionEventWriterAppendEventSchema.parse({
					type: "session.status_idle",
					stop_reason: status.stopReason ?? { type: "end_turn" },
				})
			: SessionEventWriterAppendEventSchema.parse({
					type: "session.status_running",
				});
	}

	private startStep(
		envelope: LLMEventEnvelope & {
			readonly event: Extract<LLMEvent, { type: "step-start" }>;
		},
	): ProviderStreamAccumulatorResult {
		this.activeStepIndex = envelope.event.stepIndex ?? this.activeStepIndex + 1;
		return { ok: true, events: [] };
	}

	private finishStep(
		envelope: LLMEventEnvelope & {
			readonly event: Extract<LLMEvent, { type: "step-finish" }>;
		},
	): ProviderStreamAccumulatorResult {
		void envelope;
		return { ok: true, events: [] };
	}

	private startText(
		_envelope: LLMEventEnvelope,
	): ProviderStreamAccumulatorResult {
		if (this.activeTextPart === undefined)
			this.activeTextPart = parseTextPart({
				type: "text",
				text: "",
				truncated: false,
			});
		return { ok: true, events: [] };
	}

	private appendText(
		envelope: LLMEventEnvelope & {
			readonly event: Extract<LLMEvent, { type: "text-delta" }>;
		},
	): ProviderStreamAccumulatorResult {
		if (this.activeTextPart === undefined) return { ok: true, events: [] };
		const text = `${this.activeTextPart.text}${envelope.event.text_delta}`;
		if (!withinJsonStringBudget(text, MaxProviderContextTextJsonBytes))
			return this.failWithoutWrites(boundedSemanticFailure());
		this.activeTextPart = parseTextPart({ ...this.activeTextPart, text });
		return { ok: true, events: [] };
	}

	private async endText(
		envelope: LLMEventEnvelope,
	): Promise<ProviderStreamAccumulatorResult> {
		if (this.activeTextPart === undefined) return { ok: true, events: [] };
		const completed = this.activeTextPart;
		this.activeTextPart = undefined;
		if (completed.text.length === 0) return { ok: true, events: [] };
		const event = SessionEventWriterAppendEventSchema.parse({
			type: "agent.message",
			content: [{ type: "text", text: completed.text }],
		});
		const append = RuntimeAssistantContextAppendSchema.parse({
			parts: [...this.takePendingPrefix(), completed],
		});
		this.semanticMemberCount++;
		return await this.memberSequencer.enqueue({
			event: Promise.resolve(event),
			source: envelope,
			append,
		}).committed;
	}

	private async startReasoning(
		envelope: LLMEventEnvelope & {
			readonly event: Extract<LLMEvent, { type: "reasoning-start" }>;
		},
	): Promise<ProviderStreamAccumulatorResult> {
		if (this.reasoningParts.has(envelope.event.id))
			return { ok: true, events: [] };
		const part = parseReasoningPart({
			type: "reasoning",
			providerPartId: envelope.event.id,
			...(envelope.event.providerMetadata === undefined
				? {}
				: { providerMetadata: envelope.event.providerMetadata }),
			text: "",
			truncated: false,
		});
		if (!this.reasoningSetFits(part))
			return this.failWithoutWrites(boundedSemanticFailure());
		const event = SessionEventWriterAppendEventSchema.parse({
			type: "agent.thinking",
		});
		const result = await this.options.writer.appendEvent(event, envelope);
		if (!result.ok)
			return { ok: false, events: [], error: eventWriterFailure(result.error) };
		if (result.type === "stale") return declarationApplicationFailure();
		this.reasoningParts.set(envelope.event.id, part);
		return { ok: true, events: [event], durableEventIds: [result.eventId] };
	}

	private appendReasoning(
		envelope: LLMEventEnvelope & {
			readonly event: Extract<LLMEvent, { type: "reasoning-delta" }>;
		},
	): ProviderStreamAccumulatorResult {
		const part = this.reasoningParts.get(envelope.event.id);
		if (part === undefined) return { ok: true, events: [] };
		const metadata = mergeProviderMetadata(
			part.providerMetadata,
			envelope.event.providerMetadata,
		);
		const updated = parseReasoningPart({
			...part,
			...(metadata === undefined ? {} : { providerMetadata: metadata }),
			text: `${part.text}${envelope.event.text_delta}`,
		});
		if (
			!withinJsonStringBudget(updated.text, MaxProviderContextTextJsonBytes) ||
			!this.reasoningSetFits(updated)
		)
			return this.failWithoutWrites(boundedSemanticFailure());
		this.reasoningParts.set(envelope.event.id, updated);
		return { ok: true, events: [] };
	}

	private endReasoning(
		envelope: LLMEventEnvelope & {
			readonly event: Extract<LLMEvent, { type: "reasoning-end" }>;
		},
	): ProviderStreamAccumulatorResult {
		const part = this.reasoningParts.get(envelope.event.id);
		if (part === undefined) return { ok: true, events: [] };
		const metadata = mergeProviderMetadata(
			part.providerMetadata,
			envelope.event.providerMetadata,
		);
		const completed = parseReasoningPart({
			...part,
			...(metadata === undefined ? {} : { providerMetadata: metadata }),
		});
		if (!this.reasoningSetFits(completed))
			return this.failWithoutWrites(boundedSemanticFailure());
		this.reasoningParts.delete(envelope.event.id);
		this.pendingPrefix.push(completed);
		return { ok: true, events: [] };
	}

	private startToolInput(
		envelope: LLMEventEnvelope & {
			readonly event: Extract<LLMEvent, { type: "tool-input-start" }>;
		},
	): ProviderStreamAccumulatorResult {
		if (!this.toolParts.has(envelope.event.id))
			this.toolParts.set(
				envelope.event.id,
				parseToolPart({
					type: "tool",
					modelToolCallId: envelope.event.id,
					toolName: envelope.event.toolName,
					state: { status: "pending" },
				}),
			);
		return { ok: true, events: [] };
	}

	private startToolCall(
		envelope: LLMEventEnvelope & {
			readonly event: Extract<LLMEvent, { type: "tool-call" }>;
		},
	): ProviderStreamAccumulatorResult {
		const ensured = this.ensureToolPart(
			envelope.event.id,
			envelope.event.toolName,
		);
		if (!ensured.ok) return ensured;
		const updated = parseToolPart({
			...ensured.part,
			toolName: envelope.event.toolName,
			state: {
				status: "running",
				input: runtimeJsonFromProvider(
					envelope.event.input,
					envelope.event.inputPreview,
					this.maxBytes(),
				),
			},
		});
		this.toolParts.set(envelope.event.id, updated);
		return { ok: true, events: [] };
	}

	private finish(
		envelope: LLMEventEnvelope & {
			readonly event: Extract<LLMEvent, { type: "finish" }>;
		},
	): ProviderStreamAccumulatorResult {
		if (
			this.activeTextPart !== undefined ||
			this.reasoningParts.size > 0 ||
			(this.semanticMemberCount === 0 && this.pendingPrefix.length === 0)
		)
			return this.failWithoutWrites(semanticSequenceFailure());
		this.terminal = true;
		return { ok: true, events: [] };
	}

	private async terminalFailure(
		_source: RuntimeProcessorSource,
		failure: RuntimeFailure,
	): Promise<ProviderStreamAccumulatorResult> {
		this.discardUncommittedMembers();
		this.terminal = true;
		return {
			ok: true,
			events: [
				SessionEventSchema.parse({ type: "session.error", error: failure }),
			],
		};
	}

	private ensureToolPart(
		toolCallId: string,
		toolName: string,
	): EnsureToolPartResult {
		const existing = this.toolParts.get(toolCallId);
		if (existing !== undefined) return { ok: true, events: [], part: existing };
		const part = parseToolPart({
			type: "tool",
			modelToolCallId: toolCallId,
			toolName,
			state: { status: "pending" },
		});
		this.toolParts.set(toolCallId, part);
		return { ok: true, events: [], part };
	}

	private applyMemberAppend(
		result: Extract<
			SessionEventWriterAppendResult,
			{ readonly ok: true; readonly type: "committed" | "duplicate" }
		>,
		append: RuntimeAssistantContextAppend,
	): boolean {
		if (!("assistant" in result)) return false;
		const assistant = result.assistant;
		try {
			const application = applyAssistantAppendResult({
				modelRequestId: this.options.modelRequestId,
				append,
				existingDraft: this.options.contextOwner.openRequestDraft(),
				result: assistant,
			});
			this.options.contextOwner.installOpenRequestDraft(application.draft);
			for (const part of application.activeToolParts) {
				this.toolParts.set(part.modelToolCallId, part);
				this.toolContextSequences.set(
					part.modelToolCallId,
					assistant.messageSequence,
				);
				if (part.toolUseEventId !== undefined)
					this.toolUseEventIds.set(part.modelToolCallId, part.toolUseEventId);
			}
			return true;
		} catch {
			return false;
		}
	}

	private appendToolResultPart(
		toolCallId: string,
		resultPart: Extract<RuntimeContextPart, { readonly type: "tool_result" }>,
	): void {
		const messageSequence = this.toolContextSequences.get(toolCallId);
		if (messageSequence === undefined) {
			throw new Error("Tool result lacks its request-local context target");
		}
		this.options.contextOwner.appendToolResult(
			messageSequence,
			toolCallId,
			resultPart.result,
		);
		this.toolContextSequences.delete(toolCallId);
	}

	private takePendingPrefix(): RuntimeAssistantDraftPart[] {
		const prefix = this.pendingPrefix;
		this.pendingPrefix = [];
		return prefix;
	}

	private reasoningSetFits(candidate: ReasoningPartCreate): boolean {
		const parts = [
			...this.pendingPrefix.filter(
				(part): part is ReasoningPartCreate => part.type === "reasoning",
			),
			...[...this.reasoningParts.values()].filter(
				(part) => part.providerPartId !== candidate.providerPartId,
			),
			candidate,
		];
		if (parts.length > MaxStableReasoningPartsPerRequest) return false;
		return (
			parts.reduce(
				(total, part) =>
					total +
					byteLength(part.text) +
					byteLength(stableReasoningMetadataJSON(part.providerMetadata)),
				0,
			) <= MaxStableReasoningBytesPerRequest
		);
	}

	private failWithoutWrites(
		error: RuntimeFailure,
	): ProviderStreamAccumulatorResult {
		this.discardUncommittedMembers();
		return { ok: false, events: [], error };
	}

	private maxBytes(): number {
		return this.options.maxNormalizedTextPreviewBytes ?? 8_192;
	}
}

/** Deterministic idempotency key for one invalid internal Tool-call repair. */
export function internalToolRepairKey(
	modelRequestId: string,
	modelToolCallId: string,
	toolName: string,
): string {
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
	if (part.type !== "text") throw new Error("expected text part");
	return part;
}
function parseReasoningPart(value: unknown): ReasoningPartCreate {
	const part = RuntimeAssistantDraftPartSchema.parse(value);
	if (part.type !== "reasoning") throw new Error("expected reasoning part");
	return part;
}
function parseToolPart(value: unknown): ToolPartCreate {
	const part = RuntimeAssistantDraftPartSchema.parse(value);
	if (part.type !== "tool") throw new Error("expected Tool part");
	return part;
}

function runtimeJsonFromProvider(
	value: RuntimeJsonValue,
	preview: Extract<LLMEvent, { type: "tool-call" }>["inputPreview"],
	maxBytes: number,
): RuntimeBoundedJson {
	const bounded = boundRuntimeText(preview.preview, maxBytes);
	return RuntimeBoundedJsonSchema.parse({
		value,
		preview: bounded.text,
		truncated: preview.truncated || bounded.truncated,
	});
}

function toolUseSessionEvent(
	part: ToolPartCreate,
	input: RuntimeJsonValue,
	permission: "allow" | "ask" | "deny",
	toolEvent: PublicToolEvent,
): SessionEventWriterAppendEvent {
	if (!isRuntimeJsonObject(input))
		throw new Error("public Tool input must be an object");
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

function terminalToolPart(
	part: ToolPartCreate,
	settlement: RuntimeToolSettlement,
): ToolPartCreate {
	if (part.state.status !== "running")
		throw new Error("terminal settlement requires a running Tool");
	if (settlement.type === "completed")
		return parseToolPart({
			...part,
			state: {
				status: "completed",
				input: part.state.input,
				output: RuntimeBoundedTextSchema.parse(settlement.output),
			},
		});
	if (settlement.type === "error")
		return parseToolPart({
			...part,
			state: {
				status: "error",
				input: part.state.input,
				error: runtimeToolErrorFromFailure(settlement.error),
			},
		});
	return parseToolPart({
		...part,
		state: {
			status: "cancelled",
			input: part.state.input,
			...(settlement.error === undefined
				? {}
				: { error: runtimeToolErrorFromFailure(settlement.error) }),
		},
	});
}

function isTerminalTool(part: ToolPartCreate): boolean {
	return (
		part.state.status === "completed" ||
		part.state.status === "error" ||
		part.state.status === "cancelled"
	);
}
function isRuntimeJsonObject(
	value: RuntimeJsonValue,
): value is { readonly [key: string]: RuntimeJsonValue } {
	return (
		typeof value === "object" &&
		value !== null &&
		!Array.isArray(value) &&
		Object.getPrototypeOf(value) === Object.prototype
	);
}
function withinJsonStringBudget(value: string, maxBytes: number): boolean {
	return byteLength(JSON.stringify(value)) <= maxBytes;
}
function byteLength(value: string): number {
	return new TextEncoder().encode(value).byteLength;
}

function mergeProviderMetadata(
	existing: ReasoningProviderMetadata | undefined,
	incoming: ReasoningProviderMetadata | undefined,
): ReasoningProviderMetadata | undefined {
	if (incoming === undefined) return existing;
	if (existing === undefined) return incoming;
	const merged: Record<
		string,
		ReasoningProviderMetadata[keyof ReasoningProviderMetadata]
	> = { ...existing };
	for (const [key, value] of Object.entries(incoming)) {
		const prior = existing[key];
		merged[key] =
			isMetadataObject(prior) && isMetadataObject(value)
				? { ...prior, ...value }
				: value;
	}
	return merged;
}
function isMetadataObject(
	value: unknown,
): value is Readonly<
	Record<string, ReasoningProviderMetadata[keyof ReasoningProviderMetadata]>
> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function declarationApplicationFailure(): {
	readonly ok: false;
	readonly events: readonly SessionEvent[];
	readonly error: RuntimeFailure;
} {
	return {
		ok: false,
		events: [],
		error: eventWriterFailure(
			normalizeSessionEventWriterError({ code: "schema_mismatch" }),
		),
	};
}
function storeFailure(
	error: RuntimeInternalToolRepairStoreError,
): RuntimeFailure {
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
