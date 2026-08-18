import { expect } from "bun:test";
import { readFile } from "node:fs/promises";
import {
	ProviderContextRole,
	ProviderRequestKind,
	SystemCacheHint,
	SystemSegmentKind,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
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
import {
	runtimeModelForThread,
	runtimeToolPolicyFromPatchPayloads,
} from "../../../../runtime-pod/src/command.js";
import type {
	AcceptedInputCommitResult,
	ContextLoader,
} from "../../../src/context/context-loader.js";
import { normalizeProviderError } from "../../../src/contracts/provider.js";
import type {
	PendingInputResult,
	RuntimeContextEntry,
	RuntimeDeclarationOperationControls,
	RuntimeDependencies,
	RuntimeInternalToolRepairCommit,
	RuntimeProviderAttachment,
	SessionEvent,
	SessionEventEnvelope,
	SessionEventWriter,
	SessionEventWriterAppendResult,
	SessionEventWriterFinishIdleEnvelope,
	SessionEventWriterFinishIdleResult,
	SessionEventWriterRequestEndEnvelope,
	SessionEventWriterRequestEndResult,
	SessionEventWriterRuntimeTerminationEnvelope,
	SessionEventWriterRuntimeTerminationResult,
	SessionEventWriterToolSettlementAttempt,
	SessionEventWriterToolSettlementEnvelope,
} from "../../../src/contracts/runtime.js";
import {
	normalizeContextLoaderError,
	normalizeRuntimeInternalToolRepairStoreError,
	normalizeSessionEventWriterError,
	RuntimeInternalToolRepairStore,
	SessionEventWriterRetryPolicy,
} from "../../../src/contracts/runtime.js";
import type { LLMEvent, RuntimeUsage } from "../../../src/llm/llm-event.js";
import {
	LLMEventSchema,
	runtimeFailureFromProviderError,
} from "../../../src/llm/llm-event.js";
import type {
	LLMRequest,
	LLMServiceError,
	Interface as LLMServiceInterface,
} from "../../../src/llm/llm-service.js";
import type {
	RuntimeContextLoadOperation,
	RuntimeEventWriteOperation,
	RuntimeHotStateMetrics,
	RuntimeMetricOutcome,
	RuntimeMetricsSink,
	RuntimeProviderStreamKind,
} from "../../../src/runtime/metrics.js";
import { acceptedInputContextDrafts } from "../../../src/runtime/runtime-declaration.js";
import { AutoApprovalReviewerManager } from "../../../src/session/approval-reviewer-manager.js";
import type * as SessionManager from "../../../src/session/session-manager.js";
import { compactionBoundaryMessageSequence } from "../../../src/thread-loop/compaction.js";
import type {
	ProviderCallAssembler,
	ProviderCallRuntimeConfig,
} from "../../../src/thread-loop/provider-request.js";
import {
	assembleProviderCallRequest,
	DefaultProviderCallRuntimeConfig,
} from "../../../src/thread-loop/provider-request.js";
import * as ThreadLoop from "../../../src/thread-loop/thread-loop.js";
import { ThreadRuntime } from "../../../src/thread-loop/thread-runtime.js";
import type {
	RuntimeAcceptedInputState,
	RuntimeConfigPatchState,
	RuntimeControlInputCommitResult,
	RuntimeControlInputDeclaration,
	RuntimeControlInputState,
} from "../../../src/thread-loop/thread-state.js";
import type { RuntimeToolExecutionResult } from "../../../src/thread-loop/tool-execution.js";
import type { ToolCatalog } from "../../../src/tools/tool-catalog.js";
import { createToolCatalog } from "../../../src/tools/tool-catalog.js";
import { SessionToolCoordinator } from "../../../src/tools/tool-scheduler.js";

interface PackageJson {
	readonly dependencies?: Readonly<Record<string, string>>;
}

const createdAt = "2026-06-14T00:00:00.000Z";

function userMessage(
	_id: string,
	messageSequence: number,
	text: string,
): RuntimeContextEntry {
	return {
		messageSequence,
		contextKind: "user",
		parts: [{ type: "text", text }],
	};
}

function runtimeNotificationMessage(
	_id: string,
	text: string,
	messageSequence = 1,
): RuntimeContextEntry {
	return {
		messageSequence,
		contextKind: "runtime_notification",
		parts: [{ type: "text", text }],
	};
}

const durableRuntimeNotificationMessage = runtimeNotificationMessage;

function buildRuntimeControlCommitResult(
	scope: RuntimeControlInputState & { readonly inputOrder?: number },
	_inputKind: "interrupt_control" | "tool_confirmation",
	_declaration: RuntimeControlInputDeclaration,
): RuntimeControlInputCommitResult {
	return {
		ok: true,
		type: "committed",
		assignedContextSequences: [scope.inputOrder ?? 1],
		pendingAttachments: [],
		interruptToolResults: [],
	};
}

function acceptedInputCommitResult(
	input: RuntimeAcceptedInputState,
	type: "committed" | "duplicate" = "committed",
	firstContextSequence = 1,
): Extract<
	AcceptedInputCommitResult,
	{ readonly type: "committed" | "duplicate" }
> {
	const assignedContextSequences = acceptedInputContextDrafts(input).map(
		(_draft, index) => firstContextSequence + index,
	);
	return {
		type,
		assignedContextSequences,
		pendingAttachments: [],
		interruptToolResults: [],
	};
}

function providerAttachmentsForTest(
	attachments: readonly RuntimeProviderAttachment[],
) {
	return attachments.map((attachment) => ({
		transient:
			attachment.transient === undefined
				? undefined
				: {
						attachmentRef: attachment.transient.attachmentRef,
						sourcePath: attachment.transient.sourcePath,
						pageRange: attachment.transient.pageRange,
						detail: attachment.transient.detail,
					},
		fileBacked: attachment.fileBacked,
		mime: attachment.mime,
		filename: attachment.filename,
	}));
}

const approvalReviewerOutputSchemaJson = JSON.stringify({
	type: "object",
	additionalProperties: false,
	required: ["risk_level", "user_authorization", "outcome", "rationale"],
	properties: {
		risk_level: { enum: ["low", "medium", "high", "critical"] },
		user_authorization: { enum: ["unknown", "low", "medium", "high"] },
		outcome: { enum: ["allow", "deny"] },
		rationale: { type: "string" },
	},
});

const approvalReviewerPolicy = "Fixed approval reviewer policy for tests.";

const ProviderDiagnosticCanaries = [
	"opaqueDummySecret123",
	"raw-provider-json-with-user-prompt",
	"safeLookingProviderPayload",
] as const;

function expectNoProviderDiagnosticCanaries(value: unknown): void {
	const serialized = JSON.stringify(value);
	for (const canary of ProviderDiagnosticCanaries) {
		expect(serialized).not.toContain(canary);
	}
	expect(serialized).not.toContain("responseHeaders");
	expect(serialized).not.toContain("responseBody");
	expect(serialized).not.toContain("rawBody");
	expect(serialized).not.toContain("stack");
}

function acceptedInput(
	runtimeInputId = "rin_follow_up",
	sessionId = "sesn_1",
): Extract<RuntimeAcceptedInputState, { readonly kind: "messages" }> {
	return {
		workspaceId: "wksp_test",
		sessionId,
		sessionThreadId: "thrd_1",
		bindingId: "bind_1",
		bindingGeneration: 1,
		targetPodUid: "pod_1",
		runtimeInputId,
		inputOrder: 1,
		kind: "messages",
		contentJson: JSON.stringify({
			messages: [userMessage(`msg_${runtimeInputId}`, 1, "test input")],
		}),
	};
}

function taskNotificationInput(
	runtimeInputId: string,
	taskId: string,
	sourceToolUseEventId: string,
	status: "completed" | "failed" | "cancelled" | "expired",
	notificationJson: string,
	sessionId = "sesn_1",
): Extract<RuntimeAcceptedInputState, { readonly kind: "task_notification" }> {
	const {
		kind: _kind,
		contentJson: _contentJson,
		...scope
	} = acceptedInput(runtimeInputId, sessionId);
	return {
		...scope,
		kind: "task_notification",
		taskId,
		sourceToolUseEventId,
		status,
		notificationJson,
	};
}

function rejectionInput(
	runtimeInputId: string,
	reasonCode: "runtime_command_payload_too_large" | "runtime_command_rejected",
	sessionId = "sesn_1",
): Extract<RuntimeAcceptedInputState, { readonly kind: "rejection" }> {
	const {
		kind: _kind,
		contentJson: _contentJson,
		...scope
	} = acceptedInput(runtimeInputId, sessionId);
	return { ...scope, kind: "rejection", reasonCode };
}

function interruptInput(
	runtimeInputId: string,
	_inputOrder = 9,
	sessionId = "sesn_1",
	origin: "user" | "agent" = "user",
): SessionManager.RuntimeInterruptControlCommand {
	const {
		kind: _kind,
		contentJson: _contentJson,
		...scope
	} = acceptedInput(runtimeInputId, sessionId);
	return {
		...scope,
		origin,
		interruptLeaseRef: {
			jobId: `qjob_${runtimeInputId}`,
			leaseToken: `lease_${runtimeInputId}`,
			partitionKey: `session:wksp_test:${sessionId}`,
			dedupeKey: `runtime_input:wksp_test:${sessionId}:${runtimeInputId}`,
		},
	};
}

function installRecoveredToolTurn(
	session: ThreadRuntime,
	modelRequestId: string,
	members: ReadonlyArray<{
		readonly modelToolCallId: string;
		readonly toolUseEventId: string;
		readonly toolName: string;
		readonly disposition?:
			| "requires_user_action"
			| "resume_approval_settlement"
			| "resume_sandbox_execution";
	}>,
): void {
	session.state.installThreadTurn(
		{
			pendingInputContextSequences: [],
			request: {
				modelRequestId,
				requestStartEventId: `${modelRequestId}_start`,
				requestKind: "agent_provider_request",
				contextThroughMessageSequence: 0,
				toolMembers: members.map(
					({ disposition: _disposition, ...member }) => ({
						memberKind: "public_tool_use" as const,
						...member,
					}),
				),
				requestEnd: {
					eventId: `${modelRequestId}_end`,
					isError: false,
					rescheduled: false,
				},
			},
		},
		{
			routes: members.map((member) => ({
				toolUseEventId: member.toolUseEventId,
				disposition: member.disposition ?? "requires_user_action",
			})),
		},
	);
}

function testControlCommit(
	scope: Parameters<typeof buildRuntimeControlCommitResult>[0],
	inputKind: "interrupt_control" | "tool_confirmation" = "interrupt_control",
) {
	return async (declaration: RuntimeControlInputDeclaration) =>
		buildRuntimeControlCommitResult(scope, inputKind, declaration);
}

function beginTestUserInterrupt(
	session: ThreadRuntime,
	runtimeInputId: string,
	onCommit: () => void = () => {},
): void {
	const {
		kind: _kind,
		contentJson: _contentJson,
		inputOrder,
		...address
	} = acceptedInput(runtimeInputId);
	const scope = { ...address, origin: "user" as const };
	session.state.beginUserInterrupt(scope, async (declaration) => {
		onCommit();
		return buildRuntimeControlCommitResult(
			{ ...scope, inputOrder },
			"interrupt_control",
			declaration,
		);
	});
}

function approvalReviewAcceptedInput(
	runtimeInputId = "rin_approval_review",
): Extract<
	RuntimeAcceptedInputState,
	{
		readonly kind: "approval_review";
	}
> {
	return {
		workspaceId: "wksp_reviewer",
		sessionId: "sesn_1",
		sessionThreadId: "thrd_reviewer",
		bindingId: "bind_reviewer",
		bindingGeneration: 1,
		targetPodUid: "pod_reviewer",
		runtimeInputId,
		inputOrder: 1,
		kind: "approval_review",
		reviewId: `arvw_${runtimeInputId}`,
		parentThreadId: "thrd_main",
		targetModelToolCallId: `tool_call_${runtimeInputId}`,
		targetToolName: "Write",
		promptText: ["review pending tool approval"],
		outputSchemaJson: approvalReviewerOutputSchemaJson,
		thread: {
			parentThreadId: "thrd_main",
			role: "approval_reviewer",
			visibility: "internal",
			agentType: "approval_reviewer",
			status: "idle",
		},
	};
}

interface TestContextLoader extends ContextLoader {
	readonly buildContext: (
		sessionId: string,
	) => Promise<readonly RuntimeContextEntry[]>;
	readonly loadPendingInput: (sessionId: string) => Promise<PendingInputResult>;
	readonly bindDurableSequence?: (sequence: TestDurableSequence) => void;
}

class RecordingContextLoader implements TestContextLoader {
	readonly buildCalls: string[] = [];
	readonly pendingCalls: string[] = [];
	readonly commitCalls: RuntimeAcceptedInputState[] = [];
	private nextMessageSequence = 1;
	private durableSequence: TestDurableSequence | undefined;
	constructor(
		private readonly history: readonly RuntimeContextEntry[],
		private readonly pending: PendingInputResult,
	) {}
	async buildContext(
		sessionId: string,
	): Promise<readonly RuntimeContextEntry[]> {
		this.buildCalls.push(sessionId);
		this.observeSequences(this.history.map((entry) => entry.messageSequence));
		return this.history;
	}
	async loadPendingInput(sessionId: string): Promise<PendingInputResult> {
		this.pendingCalls.push(sessionId);
		return this.pending;
	}
	async commitAcceptedInput(
		input: RuntimeAcceptedInputState,
	): Promise<AcceptedInputCommitResult> {
		this.commitCalls.push(input);
		const count = acceptedInputContextDrafts(input).length;
		const firstSequence = Math.max(
			this.nextMessageSequence,
			(this.durableSequence?.messageSequence ?? 0) + 1,
		);
		const assignedContextSequences = Array.from(
			{ length: count },
			(_, index) => firstSequence + index,
		);
		this.nextMessageSequence = firstSequence + count;
		this.observeSequences(assignedContextSequences);
		return {
			type: "committed",
			assignedContextSequences,
			pendingAttachments: [],
			interruptToolResults: [],
		};
	}
	bindDurableSequence(sequence: TestDurableSequence): void {
		this.durableSequence = sequence;
		this.observeSequences(this.history.map((entry) => entry.messageSequence));
	}
	private observeSequences(sequences: readonly number[]): void {
		const highest = Math.max(0, ...sequences);
		this.nextMessageSequence = Math.max(this.nextMessageSequence, highest + 1);
		if (this.durableSequence !== undefined) {
			this.durableSequence.messageSequence = Math.max(
				this.durableSequence.messageSequence,
				highest,
			);
		}
	}
}

class QueuedContextLoader implements TestContextLoader {
	readonly buildCalls: string[] = [];
	readonly pendingCalls: string[] = [];
	readonly commitCalls: RuntimeAcceptedInputState[] = [];
	private nextMessageSequence = 1;
	private durableSequence: TestDurableSequence | undefined;
	constructor(
		private readonly history: readonly RuntimeContextEntry[],
		private readonly pendingResults: PendingInputResult[],
		private readonly acceptedResults: Array<
			unknown | ((input: RuntimeAcceptedInputState) => unknown)
		> = [],
	) {}
	async buildContext(
		sessionId: string,
	): Promise<readonly RuntimeContextEntry[]> {
		this.buildCalls.push(sessionId);
		this.observeSequences(this.history.map((entry) => entry.messageSequence));
		return this.history;
	}
	async loadPendingInput(sessionId: string): Promise<PendingInputResult> {
		this.pendingCalls.push(sessionId);
		return this.pendingResults.shift() ?? { type: "empty" };
	}
	async commitAcceptedInput(
		input: RuntimeAcceptedInputState,
	): Promise<AcceptedInputCommitResult> {
		this.commitCalls.push(input);
		const result = this.acceptedResults.shift();
		if (typeof result === "function") {
			const resolved = result(input) as AcceptedInputCommitResult;
			this.observeResult(resolved);
			return resolved;
		}
		if (result !== undefined) {
			const resolved = result as AcceptedInputCommitResult;
			this.observeResult(resolved);
			return resolved;
		}
		const count = acceptedInputContextDrafts(input).length;
		const firstSequence = Math.max(
			this.nextMessageSequence,
			(this.durableSequence?.messageSequence ?? 0) + 1,
		);
		const assignedContextSequences = Array.from(
			{ length: count },
			(_, index) => firstSequence + index,
		);
		this.nextMessageSequence = firstSequence + count;
		this.observeSequences(assignedContextSequences);
		return {
			type: "committed",
			assignedContextSequences,
			pendingAttachments: [],
			interruptToolResults: [],
		};
	}
	bindDurableSequence(sequence: TestDurableSequence): void {
		this.durableSequence = sequence;
		this.observeSequences(this.history.map((entry) => entry.messageSequence));
	}
	private observeResult(result: AcceptedInputCommitResult): void {
		if (result.type === "committed" || result.type === "duplicate") {
			this.observeSequences(result.assignedContextSequences);
		}
	}
	private observeSequences(sequences: readonly number[]): void {
		const highest = Math.max(0, ...sequences);
		this.nextMessageSequence = Math.max(this.nextMessageSequence, highest + 1);
		if (this.durableSequence !== undefined) {
			this.durableSequence.messageSequence = Math.max(
				this.durableSequence.messageSequence,
				highest,
			);
		}
	}
}

let testAcceptedInputSequence = 0;

function installPendingInputForTest(
	session: ThreadRuntime,
	pending: PendingInputResult,
): void {
	if (
		session.state.peekAcceptedInput() !== undefined ||
		pending.type !== "context" ||
		pending.entries.length === 0
	) {
		return;
	}
	testAcceptedInputSequence += 1;
	const runtimeInputId = `rin_test_harness_${testAcceptedInputSequence}`;
	session.state.enqueueAcceptedInput({
		...session.identity,
		runtimeInputId,
		inputOrder: testAcceptedInputSequence,
		kind: "messages",
		contentJson: JSON.stringify({
			messages: pending.entries.map((entry) => ({ parts: entry.parts })),
		}),
	});
}

async function installLoaderStateForTest(
	loader: TestContextLoader,
	session: ThreadRuntime,
): Promise<void> {
	if (!session.state.persistentContextLoaded()) {
		session.state.contextManager.replaceEntries(
			await loader.buildContext(session.sessionId),
		);
		session.state.markPersistentContextLoaded();
	}
	if (session.state.peekAcceptedInput() === undefined) {
		installPendingInputForTest(
			session,
			await loader.loadPendingInput(session.sessionId),
		);
	}
}

class ThreadLoopRuntimeStore extends RuntimeInternalToolRepairStore {
	readonly messages = new Map<string, { readonly role: string }>();
	readonly repairs: RuntimeInternalToolRepairCommit[] = [];
	private durableSequence: TestDurableSequence | undefined;
	constructor(
		private readonly order: string[],
		_retiredPartFailure: unknown = false,
		_retiredMessageFailure = false,
		_retiredBeforeWrite?: unknown,
		private readonly beforeInternalToolRepair?: (
			repair: RuntimeInternalToolRepairCommit,
		) => void | Promise<void>,
		durableSequence?: TestDurableSequence,
	) {
		super();
		this.durableSequence = durableSequence;
	}
	bindDurableSequence(sequence: TestDurableSequence): void {
		this.durableSequence ??= sequence;
	}
	protected async commitInternalToolRepairRecord(
		repair: RuntimeInternalToolRepairCommit,
		_controls: RuntimeDeclarationOperationControls,
	): Promise<unknown> {
		await this.beforeInternalToolRepair?.(repair);
		this.repairs.push(repair);
		this.order.push("store:internal-tool-repair");
		const messageSequence =
			this.durableSequence === undefined
				? 2
				: assistantMessageSequence(this.durableSequence, repair.modelRequestId);
		return {
			ok: true,
			type: "committed",
			repairEventId: `event-internal-repair-${this.repairs.length}`,
			assignedMessageSequence: messageSequence,
		};
	}
}

function threadLoopRuntime() {
	let counter = 0;
	return {
		now: () => createdAt,
		monotonicMs: () => 0,
		createId: (prefix: string) => `${prefix}-${++counter}`,
		sleep: async () => true,
	} satisfies RuntimeDependencies;
}

function testRunCustody(): ThreadLoop.ThreadLoopRunCustody {
	return {
		activeTurnId: (session) =>
			session.state.threadTurnReduction().checkpoint.executionRunId,
		interruptLeaseRef: (runtimeInputId) => ({
			jobId: `qjob_${runtimeInputId}`,
			leaseToken: `lease_${runtimeInputId}`,
			partitionKey: `session:wksp_test:sesn_1`,
			dedupeKey: `runtime_input:wksp_test:sesn_1:${runtimeInputId}`,
		}),
	};
}

async function sleepUntilAborted(
	_milliseconds: number,
	signal: AbortSignal,
): Promise<boolean> {
	return await new Promise<boolean>((resolve) => {
		if (signal.aborted) {
			resolve(false);
			return;
		}
		signal.addEventListener("abort", () => resolve(false), { once: true });
	});
}

function llmService(
	events: readonly LLMEvent[],
	onStream?: (request: LLMRequest) => void,
): LLMServiceInterface {
	return {
		stream(request) {
			onStream?.(request);
			return Stream.fromIterable(events);
		},
	};
}

function firstRequestThenFinalResponse(
	events: readonly LLMEvent[],
	onStream?: (request: LLMRequest) => void,
): LLMServiceInterface {
	let requestCount = 0;
	return {
		stream(request) {
			onStream?.(request);
			requestCount += 1;
			return Stream.fromIterable(
				requestCount === 1
					? events
					: [
							{ type: "text-start" as const, id: "continuation-final" },
							{
								type: "text-delta" as const,
								id: "continuation-final",
								text_delta: "done",
							},
							{ type: "text-end" as const, id: "continuation-final" },
							{ type: "finish" as const, finishReason: "stop" as const },
						],
			);
		},
	};
}

function queuedLLMService(
	eventBatches: readonly (readonly LLMEvent[])[],
	requests: LLMRequest[] = [],
): LLMServiceInterface {
	let index = 0;
	return {
		stream(request) {
			requests.push(request);
			const events = eventBatches[index] ?? [];
			index += 1;
			return Stream.fromIterable(events);
		},
	};
}

interface TestDurableSequence {
	eventSequence: number;
	messageSequence: number;
	assistantSequenceByRequest?: Map<string, number> | undefined;
}

function assistantMessageSequence(
	sequence: TestDurableSequence,
	modelRequestId: string,
): number {
	sequence.assistantSequenceByRequest ??= new Map<string, number>();
	const existing = sequence.assistantSequenceByRequest.get(modelRequestId);
	if (existing !== undefined) return existing;
	sequence.messageSequence += 1;
	sequence.assistantSequenceByRequest.set(
		modelRequestId,
		sequence.messageSequence,
	);
	return sequence.messageSequence;
}

const testDurableSequence = Symbol("testDurableSequence");

function appendSuccessForTest(
	envelope: SessionEventEnvelope,
	sequence: TestDurableSequence,
): Extract<
	SessionEventWriterAppendResult,
	{ readonly ok: true; readonly type: "committed" | "duplicate" }
> {
	sequence.eventSequence += 1;
	const result = {
		ok: true as const,
		type: "committed" as const,
		eventId: `bridge-${envelope.writeId}`,
		eventSequence: sequence.eventSequence,
	};
	return result;
}

function withFinishIdleResultForTest(
	_envelope: SessionEventWriterFinishIdleEnvelope,
	result: SessionEventWriterAppendResult,
): SessionEventWriterFinishIdleResult {
	if (!result.ok || result.type === "stale") return result;
	return { ok: true, type: result.type, idleEventId: result.eventId };
}

function runtimeTerminationResultForTest(
	envelope: SessionEventWriterRuntimeTerminationEnvelope,
): SessionEventWriterRuntimeTerminationResult {
	return {
		ok: true,
		type: "committed",
		failureEventId: `sevt_failure_${envelope.writeId}`,
		closeoutEventId: `sevt_termination_${envelope.writeId}`,
	};
}

function requestEndResultFromAppend(
	envelope: SessionEventWriterRequestEndEnvelope,
	result: SessionEventWriterAppendResult,
	sequence: TestDurableSequence,
): SessionEventWriterRequestEndResult {
	if (!result.ok || result.type === "stale") return result;
	if (envelope.reschedule !== undefined) {
		return {
			ok: true,
			type: result.type,
			requestEndEventId: result.eventId,
			outcome: {
				type: "rescheduled",
				effectiveDeadline: envelope.reschedule.deadline,
			},
			interruptToolResults: [],
		};
	}
	if (envelope.compactionContext !== undefined) {
		sequence.eventSequence += 1;
		sequence.messageSequence = Math.max(
			sequence.messageSequence + 1,
			(envelope.compactedThroughMessageSequence ?? 0) + 1,
		);
		return {
			ok: true,
			type: result.type,
			requestEndEventId: result.eventId,
			outcome: {
				type: "compacted",
				compactionEventId: `sevt_compaction_${sequence.eventSequence}`,
				checkpointMessageSequence: sequence.messageSequence,
			},
			interruptToolResults: [],
		};
	}
	let sealedMessageSequence: number | undefined;
	if (envelope.trailingContextAppend !== undefined) {
		if (sequence.messageSequence === 0) sequence.messageSequence = 1;
		sealedMessageSequence = sequence.messageSequence;
	}
	return {
		ok: true,
		type: result.type,
		requestEndEventId: result.eventId,
		outcome: {
			type: "ordinary",
			...(sealedMessageSequence === undefined ? {} : { sealedMessageSequence }),
		},
		interruptToolResults: [],
	};
}

function requestEndResultForTest(
	envelope: SessionEventWriterRequestEndEnvelope,
	outcome?: Extract<
		SessionEventWriterRequestEndResult,
		{ readonly ok: true; readonly type: "committed" | "duplicate" }
	>["outcome"],
): SessionEventWriterRequestEndResult {
	if (outcome !== undefined) {
		return {
			ok: true,
			type: "committed",
			requestEndEventId: `bridge-${envelope.writeId}`,
			outcome,
			interruptToolResults: [],
		};
	}
	return requestEndResultFromAppend(
		envelope,
		{
			ok: true,
			type: "committed",
			eventId: `bridge-${envelope.writeId}`,
		},
		{ eventSequence: 1, messageSequence: 0 },
	);
}

function writerFrom(
	append: (envelope: SessionEventEnvelope) => SessionEventWriterAppendResult,
	writeRequestEnd?: SessionEventWriter["writeRequestEnd"],
	_existingContext: readonly unknown[] = [],
	durableSequence: TestDurableSequence = {
		eventSequence: 0,
		messageSequence: 0,
	},
	settleToolResult?: SessionEventWriter["settleToolResult"],
): SessionEventWriter {
	const activeToolUseEventIds = new Set<string>();
	const appendWithFacts = async (
		envelope: SessionEventEnvelope,
	): Promise<SessionEventWriterAppendResult> => {
		const supplied = append(envelope);
		if (!supplied.ok || supplied.type === "stale") return supplied;
		durableSequence.eventSequence += 1;
		let result = supplied;
		if (
			envelope.assistantContextAppend !== undefined &&
			!("assistant" in supplied)
		) {
			const requestKey = envelope.modelRequestId ?? envelope.writeId;
			const assignedMessageSequence = assistantMessageSequence(
				durableSequence,
				requestKey,
			);
			result = {
				...supplied,
				assistant: {
					messageSequence: assignedMessageSequence,
					createdToolUseEventIds: envelope.assistantContextAppend.parts
						.filter((part) => part.type === "tool")
						.map(() => supplied.eventId),
				},
			};
		}
		if (
			(envelope.event.type === "agent.tool_use" ||
				envelope.event.type === "agent.mcp_tool_use") &&
			"assistant" in result
		) {
			for (const toolUseEventId of result.assistant.createdToolUseEventIds) {
				activeToolUseEventIds.add(toolUseEventId);
			}
		}
		return result;
	};
	const settleToolResultWithFacts: SessionEventWriter["settleToolResult"] =
		async (envelope) => {
			const result = await (
				settleToolResult ??
				(async () => ({
					ok: true as const,
					result: { type: "committed" as const },
				}))
			)(envelope);
			if (
				result.ok &&
				(result.result.type === "committed" ||
					result.result.type === "duplicate")
			) {
				activeToolUseEventIds.delete(envelope.settlement.toolUseEventId);
			}
			return result;
		};
	const baseWriteRequestEnd: SessionEventWriter["writeRequestEnd"] =
		writeRequestEnd ??
		(async (envelope) => {
			const appendResult = await appendWithFacts({
				workspaceId: envelope.workspaceId,
				sessionId: envelope.sessionId,
				sessionThreadId: envelope.sessionThreadId,
				bindingId: envelope.bindingId,
				bindingGeneration: envelope.bindingGeneration,
				targetPodUid: envelope.targetPodUid,
				writeId: envelope.writeId,
				event: {
					type: "span.model_request_end",
					model_request_start_id: envelope.modelRequestId,
					is_error: envelope.isError,
					...(envelope.errorKind === undefined
						? {}
						: { error_kind: envelope.errorKind }),
					model_usage: {
						input_tokens: envelope.usage?.inputTokens ?? 0,
						output_tokens: envelope.usage?.outputTokens ?? 0,
						cache_creation_input_tokens: envelope.usage?.cacheWriteTokens ?? 0,
						cache_read_input_tokens: envelope.usage?.cacheReadTokens ?? 0,
						speed: null,
					},
				},
			});
			return requestEndResultFromAppend(
				envelope,
				appendResult,
				durableSequence,
			);
		});
	const writeRequestEndWithFacts: SessionEventWriter["writeRequestEnd"] =
		async (envelope) => {
			const result = await baseWriteRequestEnd(envelope);
			durableSequence.assistantSequenceByRequest?.delete(
				envelope.modelRequestId,
			);
			if (
				!result.ok ||
				result.type === "stale" ||
				envelope.interruptSettlement === undefined
			) {
				return result;
			}
			const interruptToolResults = [...activeToolUseEventIds].map(
				(toolUseEventId) => ({
					toolUseEventId,
					result: { type: "cancelled" as const },
				}),
			);
			activeToolUseEventIds.clear();
			return { ...result, interruptToolResults };
		};
	return {
		[testDurableSequence]: durableSequence,
		append: appendWithFacts,
		settleToolResult: settleToolResultWithFacts,
		writeRequestEnd: writeRequestEndWithFacts,
		finishIdle: async (envelope) =>
			withFinishIdleResultForTest(
				envelope,
				await appendWithFacts({
					workspaceId: envelope.workspaceId,
					sessionId: envelope.sessionId,
					sessionThreadId: envelope.sessionThreadId,
					bindingId: envelope.bindingId,
					bindingGeneration: envelope.bindingGeneration,
					targetPodUid: envelope.targetPodUid,
					writeId: envelope.durableTurnId,
					event: {
						type: "session.status_idle",
						stop_reason: envelope.stopReason,
					},
				}),
			),
		commitRuntimeTermination: async (envelope) =>
			runtimeTerminationResultForTest(envelope),
	} as SessionEventWriter & {
		readonly [testDurableSequence]: TestDurableSequence;
	};
}

function catalogForTest(tool: {
	readonly name: string;
	readonly description: string;
	readonly inputSchema: unknown;
	readonly permissionPolicy?: "always_allow" | "always_ask";
}): ToolCatalog {
	return {
		entries: [
			{
				name: tool.name,
				definition: { kind: "function", ...tool },
				inputContract: { kind: "json_object" },
				route: { kind: "gateway", operation: "RunWeb" },
				formatter: {
					successShape: "test success text",
					errorShape: "test error text",
					forbiddenFields: ["route", "credentials", "bindingId"],
				},
				defaultPermissionPolicy: "always_allow",
				required: false,
			},
		],
		configs: [
			{
				name: tool.name,
				enabled: true,
				permissionPolicy: tool.permissionPolicy ?? "always_allow",
			},
		],
	};
}

function memoryCatalogForTest(): ToolCatalog {
	return {
		entries: [
			{
				name: "memory",
				definition: {
					kind: "function",
					name: "memory",
					description: "Memory",
					inputSchema: { type: "object" },
				},
				inputContract: { kind: "json_object" },
				route: { kind: "bridge", operation: "RunMemory" },
				formatter: {
					successShape: "memory success text",
					errorShape: "memory error text",
					forbiddenFields: ["route", "credentials", "bindingId"],
				},
				defaultPermissionPolicy: "always_allow",
				required: true,
			},
		],
		configs: [
			{ name: "memory", enabled: true, permissionPolicy: "always_allow" },
		],
	};
}

function deferred<T>(): {
	readonly promise: Promise<T>;
	readonly resolve: (value: T) => void;
} {
	let resolve: (value: T) => void = () => undefined;
	const promise = new Promise<T>((done) => {
		resolve = done;
	});
	return { promise, resolve };
}

function recordCompactionHint(
	session: ThreadRuntime,
	usage: RuntimeUsage,
): void {
	const { totalTokens: _ignoredTotal, ...usageWithoutTotal } = usage;
	session.state.recordLastRequestCompletion(
		{
			...usageWithoutTotal,
			inputTokens: 96000,
		},
		{
			contextWindowTokens: 100000,
			outputTokenLimit: 4096,
		},
		-1,
	);
}

function compactionHistory(label: string): string {
	return `${label}\n${"historical context ".repeat(2200)}`;
}

function compactionTransportHistory(label: string): string {
	const marker = "\nRECENT_SENTINEL";
	const trailing = `${"R".repeat(31999 - marker.length)}${marker}`;
	return `${label}\n${"H".repeat(48000)}😀${trailing}`;
}

function utf8RoundTrip(value: string): string {
	return new TextDecoder("utf-8", { fatal: true }).decode(
		new TextEncoder().encode(value),
	);
}

async function waitForReleaseOrAbort(
	release: Promise<void>,
	signal: AbortSignal,
): Promise<void> {
	if (signal.aborted) {
		throw new DOMException("aborted", "AbortError");
	}
	await new Promise<void>((resolve, reject) => {
		const onAbort = (): void => {
			reject(new DOMException("aborted", "AbortError"));
		};
		signal.addEventListener("abort", onAbort, { once: true });
		release.then(
			() => {
				signal.removeEventListener("abort", onAbort);
				resolve();
			},
			(error) => {
				signal.removeEventListener("abort", onAbort);
				reject(error);
			},
		);
	});
}

async function activeCompactionRun(
	session: ThreadRuntime = new ThreadRuntime("sesn_1"),
	metrics?: RuntimeMetricsSink,
) {
	recordCompactionHint(session, {
		inputTokens: 200,
		outputTokens: 75,
		reasoningTokens: 0,
		cacheReadTokens: 0,
		cacheWriteTokens: 0,
	});
	const loader = new RecordingContextLoader([], {
		type: "context",
		entries: [userMessage("user-1", 1, compactionHistory("please continue"))],
	});
	const providerRelease = deferred<void>();
	const streamStarted = deferred<void>();
	const requestEndStarted = deferred<void>();
	const requestEndAck = deferred<SessionEventWriterRequestEndResult>();
	const requests: LLMRequest[] = [];
	const appended: SessionEvent[] = [];
	const requestEndEnvelopes: SessionEventWriterRequestEndEnvelope[] = [];
	let compactionStartEventId: string | undefined;
	let observedAbortSignal: AbortSignal | undefined;
	const llm: LLMServiceInterface = {
		stream(request, options) {
			requests.push(request);
			observedAbortSignal = options?.abortSignal;
			streamStarted.resolve();
			return Stream.fromAsyncIterable(
				(async function* () {
					if (options?.abortSignal === undefined) {
						throw new Error("compaction stream requires an abort signal");
					}
					await waitForReleaseOrAbort(
						providerRelease.promise,
						options.abortSignal,
					);
					yield { type: "finish" as const, finishReason: "stop" as const };
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
	};
	const sequence: TestDurableSequence = {
		eventSequence: 0,
		messageSequence: 0,
	};
	const writer = writerFrom(
		(envelope) => {
			appended.push(envelope.event);
			const eventId = `bridge-${envelope.writeId}`;
			if (envelope.event.type === "span.model_request_start") {
				compactionStartEventId = eventId;
			}
			return { ...appendSuccessForTest(envelope, sequence), eventId };
		},
		async (envelope) => {
			requestEndEnvelopes.push(envelope);
			requestEndStarted.resolve();
			return requestEndAck.promise;
		},
	);
	const runFiber = Effect.runFork(
		Effect.gen(function* () {
			const threadLoop = yield* ThreadLoop.Service;
			return yield* threadLoop.run(session, testRunCustody());
		}).pipe(
			Effect.provide(
				runtimeThreadLoopLayer(loader, {
					llmService: llm,
					writer,
					compaction: {},
					...(metrics === undefined ? {} : { metrics }),
				}),
			),
		),
	);
	await streamStarted.promise;
	return {
		session,
		loader,
		providerRelease,
		requestEndStarted,
		requestEndAck,
		requests,
		appended,
		requestEndEnvelopes,
		compactionStartEventId,
		observedAbortSignal,
		runFiber,
	};
}

async function waitForCondition(
	predicate: () => boolean,
	label: string,
): Promise<void> {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		if (predicate()) {
			return;
		}
		await new Promise<void>((resolve) => setImmediate(resolve));
	}
	throw new Error(`timed out waiting for ${label}`);
}

async function flushMicrotasks(count = 20): Promise<void> {
	for (let index = 0; index < count; index += 1) {
		await Promise.resolve();
	}
}

function failingEventWriter(
	appendedTypes: string[],
	shouldFail: (event: SessionEvent) => boolean,
): SessionEventWriter {
	const sequence: TestDurableSequence = {
		eventSequence: 0,
		messageSequence: 0,
	};
	return writerFrom((envelope) => {
		appendedTypes.push(envelope.event.type);
		if (!shouldFail(envelope.event)) {
			return appendSuccessForTest(envelope, sequence);
		}
		return {
			ok: false,
			error: {
				type: "session-event-writer",
				code: "unavailable",
				message: "append failed",
				retryable: false,
				fatal: false,
				sessionId: envelope.sessionId,
				writeId: envelope.writeId,
			},
		};
	});
}

function runtimeThreadLoopLayer(
	loader: TestContextLoader,
	options: {
		readonly events?: readonly LLMEvent[];
		readonly store?: RuntimeInternalToolRepairStore;
		readonly writer?: SessionEventWriter;
		readonly llmService?: LLMServiceInterface;
		readonly onStream?: (request: LLMRequest) => void;
		readonly createProcessor?: Parameters<
			typeof ThreadLoop.layer
		>[0]["createProcessor"];
		readonly providerCallRuntime?: ProviderCallRuntimeConfig;
		readonly providerCallAssembler?: ProviderCallAssembler;
		readonly compaction?: Parameters<typeof ThreadLoop.layer>[0]["compaction"];
		readonly approvalMode?: Parameters<
			typeof ThreadLoop.layer
		>[0]["approvalMode"];
		readonly runTool?: Parameters<typeof ThreadLoop.layer>[0]["runTool"];
		readonly acceptSandboxExecution?: Parameters<
			typeof ThreadLoop.layer
		>[0]["acceptSandboxExecution"];
		readonly awaitSandboxExecution?: Parameters<
			typeof ThreadLoop.layer
		>[0]["awaitSandboxExecution"];
		readonly reviewApproval?: Parameters<
			typeof ThreadLoop.layer
		>[0]["reviewApproval"];
		readonly runtimeModel?: Parameters<
			typeof ThreadLoop.layer
		>[0]["runtimeModel"];
		readonly runtimePolicy?: Parameters<
			typeof ThreadLoop.layer
		>[0]["runtimePolicy"];
		readonly runtime?: RuntimeDependencies;
		readonly metrics?: RuntimeMetricsSink;
		readonly recordProviderReschedule?: Parameters<
			typeof ThreadLoop.layer
		>[0]["recordProviderReschedule"];
		readonly recordProviderToolDeclarationRejection?: Parameters<
			typeof ThreadLoop.layer
		>[0]["recordProviderToolDeclarationRejection"];
		readonly recordAcceptedInputCommit?: Parameters<
			typeof ThreadLoop.layer
		>[0]["recordAcceptedInputCommit"];
		readonly refreshRuntimeBindingToken?: Parameters<
			typeof ThreadLoop.layer
		>[0]["refreshRuntimeBindingToken"];
		readonly installLoaderState?: boolean;
	} = {},
): Layer.Layer<ThreadLoop.Service> {
	const order: string[] = [];
	const store = options.store ?? new ThreadLoopRuntimeStore(order);
	const defaultWriterSequence: TestDurableSequence = {
		eventSequence: 0,
		messageSequence: 0,
	};
	const writer =
		options.writer ??
		writerFrom(
			(envelope) => appendSuccessForTest(envelope, defaultWriterSequence),
			undefined,
			[],
			defaultWriterSequence,
		);
	const writerSequence =
		(
			writer as SessionEventWriter & {
				readonly [testDurableSequence]?: TestDurableSequence;
			}
		)[testDurableSequence] ?? defaultWriterSequence;
	loader.bindDurableSequence?.(writerSequence);
	if (store instanceof ThreadLoopRuntimeStore) {
		store.bindDurableSequence(writerSequence);
	}
	const productionLayer = ThreadLoop.layer({
		internalToolRepairStore: store,
		sessionEventWriter: writer,
		runtime: options.runtime ?? threadLoopRuntime(),
		llmService:
			options.llmService ??
			(options.events === undefined
				? llmService(
						[
							{ type: "text-start", id: "text-1" },
							{ type: "text-delta", id: "text-1", text_delta: "ok" },
							{ type: "text-end", id: "text-1" },
							{ type: "finish", finishReason: "stop" },
						],
						options.onStream,
					)
				: firstRequestThenFinalResponse(options.events, options.onStream)),
		storeOperationTimeoutMs: 1000,
		...(options.createProcessor !== undefined
			? { createProcessor: options.createProcessor }
			: {}),
		providerCallRuntime: {
			...DefaultProviderCallRuntimeConfig,
			timeoutMs: 1800000,
			...options.providerCallRuntime,
			approvalReviewerPolicy:
				options.providerCallRuntime?.approvalReviewerPolicy ??
				approvalReviewerPolicy,
		},
		...(options.providerCallAssembler !== undefined
			? { providerCallAssembler: options.providerCallAssembler }
			: {}),
		...(options.compaction !== undefined
			? { compaction: { timeoutMs: 1800000, ...options.compaction } }
			: {}),
		...(options.approvalMode !== undefined
			? { approvalMode: options.approvalMode }
			: {}),
		...(options.runTool !== undefined ? { runTool: options.runTool } : {}),
		acceptSandboxExecution:
			options.acceptSandboxExecution ?? (() => ({ type: "accepted" as const })),
		...(options.awaitSandboxExecution !== undefined
			? { awaitSandboxExecution: options.awaitSandboxExecution }
			: options.runTool !== undefined
				? { awaitSandboxExecution: options.runTool }
				: {}),
		...(options.reviewApproval !== undefined
			? { reviewApproval: options.reviewApproval }
			: {}),
		runtimeModel:
			options.runtimeModel ??
			(() => ({ providerId: "fake", modelId: "fake-chat" })),
		runtimePolicy:
			options.runtimePolicy ??
			(() => ({
				toolCatalog:
					options.providerCallRuntime?.toolCatalog ??
					createToolCatalog({ family: "claude" }),
			})),
		...(options.metrics !== undefined ? { metrics: options.metrics } : {}),
		...(options.recordProviderReschedule !== undefined
			? { recordProviderReschedule: options.recordProviderReschedule }
			: {}),
		...(options.recordProviderToolDeclarationRejection !== undefined
			? {
					recordProviderToolDeclarationRejection:
						options.recordProviderToolDeclarationRejection,
				}
			: {}),
		...(options.recordAcceptedInputCommit !== undefined
			? { recordAcceptedInputCommit: options.recordAcceptedInputCommit }
			: {}),
		...(options.refreshRuntimeBindingToken !== undefined
			? { refreshRuntimeBindingToken: options.refreshRuntimeBindingToken }
			: {}),
	}).pipe(Layer.provide(ThreadLoop.contextLoaderLayer(loader)));
	if (options.installLoaderState === false) {
		return productionLayer;
	}
	return Layer.effect(
		ThreadLoop.Service,
		Effect.gen(function* () {
			const production = yield* ThreadLoop.Service;
			return ThreadLoop.Service.of({
				...production,
				run: (session, custody) =>
					Effect.promise(() => installLoaderStateForTest(loader, session)).pipe(
						Effect.flatMap(() => production.run(session, custody)),
					),
			});
		}).pipe(Effect.provide(productionLayer)),
	);
}

class RecordingRuntimeMetrics implements RuntimeMetricsSink {
	readonly providerStreamDurations: Array<{
		readonly kind: RuntimeProviderStreamKind;
		readonly durationMs: number;
		readonly outcome: RuntimeMetricOutcome;
	}> = [];
	readonly eventWriteLatencies: Array<{
		readonly operation: RuntimeEventWriteOperation;
		readonly durationMs: number;
		readonly outcome: RuntimeMetricOutcome;
	}> = [];
	readonly contextLoadLatencies: Array<{
		readonly operation: RuntimeContextLoadOperation;
		readonly durationMs: number;
		readonly outcome: RuntimeMetricOutcome;
	}> = [];
	recordHotState(_snapshot: RuntimeHotStateMetrics): void {}
	addActiveToolFibers(): void {}
	addPendingApprovals(): void {}
	observeProviderStreamDuration(
		kind: RuntimeProviderStreamKind,
		durationMs: number,
		outcome: RuntimeMetricOutcome,
	): void {
		this.providerStreamDurations.push({ kind, durationMs, outcome });
	}
	observeEventWriteLatency(
		operation: RuntimeEventWriteOperation,
		durationMs: number,
		outcome: RuntimeMetricOutcome,
	): void {
		this.eventWriteLatencies.push({ operation, durationMs, outcome });
	}
	observeContextLoadLatency(
		operation: RuntimeContextLoadOperation,
		durationMs: number,
		outcome: RuntimeMetricOutcome,
	): void {
		this.contextLoadLatencies.push({ operation, durationMs, outcome });
	}
	recordCleanupCommandOutcome(): void {}
}

export type { PackageJson, TestContextLoader, TestDurableSequence };
export {
	acceptedInput,
	acceptedInputCommitResult,
	activeCompactionRun,
	approvalReviewAcceptedInput,
	approvalReviewerOutputSchemaJson,
	approvalReviewerPolicy,
	beginTestUserInterrupt,
	buildRuntimeControlCommitResult,
	catalogForTest,
	compactionHistory,
	compactionTransportHistory,
	createdAt,
	deferred,
	durableRuntimeNotificationMessage,
	expectNoProviderDiagnosticCanaries,
	failingEventWriter,
	firstRequestThenFinalResponse,
	flushMicrotasks,
	installLoaderStateForTest,
	installPendingInputForTest,
	installRecoveredToolTurn,
	interruptInput,
	llmService,
	memoryCatalogForTest,
	ProviderDiagnosticCanaries,
	providerAttachmentsForTest,
	QueuedContextLoader,
	queuedLLMService,
	RecordingContextLoader,
	RecordingRuntimeMetrics,
	recordCompactionHint,
	rejectionInput,
	requestEndResultForTest,
	runtimeNotificationMessage,
	runtimeTerminationResultForTest,
	runtimeThreadLoopLayer,
	sleepUntilAborted,
	ThreadLoopRuntimeStore,
	taskNotificationInput,
	testAcceptedInputSequence,
	testControlCommit,
	testRunCustody,
	threadLoopRuntime,
	userMessage,
	utf8RoundTrip,
	waitForCondition,
	waitForReleaseOrAbort,
	withFinishIdleResultForTest,
	writerFrom,
};
