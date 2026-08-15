/**
 * Tool execution contracts and the cancellable route adapter used after a
 * public Tool Use acknowledgement, including durable result settlement.
 * ThreadLoop coordinates request membership; concrete Runtime Pod adapters own
 * provider, Sandbox, and reviewer I/O.
 *
 * @packageDocumentation
 */

import { Effect } from "effect";
import type * as ContextLoader from "../context/context-loader.js";
import type {
	RuntimeAssistantDraftPart,
	RuntimeBoundedText,
	RuntimeContextEntry,
	RuntimeContextPart,
	RuntimeFailure,
	RuntimeJsonValue,
	RuntimeOpenRequestDraft,
	RuntimeProviderAttachment,
	RuntimeToolSettlement,
	SessionEventWriterError,
} from "../contracts/runtime.js";
import {
	boundRuntimeText,
	normalizeContextLoaderError,
	normalizeRuntimeFailure,
	RuntimeFailureSchema,
	RuntimeJsonValueSchema,
} from "../contracts/runtime.js";
import type { LLMEvent } from "../llm/llm-event.js";
import { RuntimePreviewTextMaxBytes } from "../llm/llm-event.js";
import type {
	ProviderStreamAccumulator,
	PublicToolEvent,
	RuntimeProcessorSource,
	ToolSettlementApplicationResult,
} from "../runtime/accumulator.js";
import type {
	AutoApprovalReviewerManager,
	ParentTranscriptView,
} from "../session/approval-reviewer-manager.js";
import type { ToolCatalog, ToolEntry } from "../tools/tool-catalog.js";
import {
	effectivePermissionPolicy,
	lookupToolEntry,
} from "../tools/tool-catalog.js";
import type { ApprovalReviewerOutcome } from "../tools/tool-gate.js";
import type { ToolJob } from "../tools/tool-scheduler.js";
import {
	inferToolRunPolicy,
	type ToolScheduler,
} from "../tools/tool-scheduler.js";
import type { ThreadRuntime } from "./thread-runtime.js";
import type {
	RuntimePendingApprovalToolJobState,
	RuntimePendingSandboxExecutionJobState,
	RuntimePreloadedPendingToolUseState,
	RuntimePreloadedSandboxExecutionState,
} from "./thread-state.js";

/** Normalizes a concrete tool route outcome before ProviderStreamAccumulator persists it. */
export type RuntimeToolExecutionResult =
	| {
			readonly type: "completed";
			readonly output: RuntimeBoundedText;
			readonly attachments?: readonly RuntimeProviderAttachment[];
			readonly backgroundTask?: RuntimeToolExecutionBackgroundTask | undefined;
			readonly serverToolUse?: {
				readonly webSearchRequests: number;
				readonly webFetchRequests: number;
			};
	  }
	| {
			readonly type: "error";
			readonly error: RuntimeFailure;
			readonly attachments?: readonly RuntimeProviderAttachment[];
			readonly serverToolUse?: {
				readonly webSearchRequests: number;
				readonly webFetchRequests: number;
			};
	  }
	| { readonly type: "cancelled"; readonly error?: RuntimeFailure }
	| { readonly type: "stale_custody" };

/** Identifies durable background work whose later notification updates the originating tool. */
export interface RuntimeToolExecutionBackgroundTask {
	readonly taskId: string;
}

/** Carries fenced thread, request, tool, and cancellation context to a concrete route adapter. */
export interface RuntimeToolExecutionRequest {
	readonly workspaceId: string;
	readonly sessionId: string;
	readonly sessionThreadId: string;
	readonly parentThreadId?: string | undefined;
	readonly bindingId: string;
	readonly bindingGeneration: number;
	readonly targetPodUid: string;
	readonly runtimeBindingToken: string;
	readonly modelRequestId: string;
	readonly modelToolCallId: string;
	readonly modelOrder: number;
	readonly toolUseEventId: string;
	readonly entry: ToolEntry;
	readonly input: RuntimeJsonValue;
	readonly currentModel?:
		| {
				readonly providerId: string;
				readonly modelId: string;
		  }
		| undefined;
	readonly committedContext: readonly RuntimeContextEntry[];
	readonly abortSignal: AbortSignal;
}

/** Dispatches a concrete tool route and returns a bounded, normalized outcome. */
export type RuntimeToolRunner = (
	request: RuntimeToolExecutionRequest,
) => RuntimeToolExecutionResult | Promise<RuntimeToolExecutionResult>;

/** Persists one post-ACK tool outcome and publishes the resulting hot projection. */
export async function commitRuntimeToolSettlement(
	processor: ProviderStreamAccumulator,
	source: RuntimeProcessorSource,
	modelToolCallId: string,
	result: Exclude<
		RuntimeToolExecutionResult,
		{ readonly type: "stale_custody" }
	>,
	applyProjection: () => void,
): Promise<ToolSettlementApplicationResult> {
	const settlement = await processor.commitToolSettlement(
		source,
		modelToolCallId,
		runtimeToolSettlement(result),
	);
	if (settlement.type === "settled") {
		applyProjection();
	}
	return settlement;
}

export function runtimeToolSettlement(
	result: Exclude<
		RuntimeToolExecutionResult,
		{ readonly type: "stale_custody" }
	>,
): RuntimeToolSettlement {
	switch (result.type) {
		case "completed":
			return {
				type: result.type,
				output: result.output,
				...(result.serverToolUse === undefined
					? {}
					: { serverToolUse: result.serverToolUse }),
			};
		case "error":
			return {
				type: result.type,
				error: result.error,
				...(result.serverToolUse === undefined
					? {}
					: { serverToolUse: result.serverToolUse }),
			};
		case "cancelled":
			return {
				type: result.type,
				...(result.error === undefined ? {} : { error: result.error }),
			};
	}
}

/** Carries one Sandbox declaration before its independently cancellable result wait. */
export type RuntimeSandboxExecutionRequest = Omit<
	RuntimeToolExecutionRequest,
	"abortSignal"
>;

/** Reports whether Bridge durably accepted ownership of one Sandbox execution. */
export type RuntimeSandboxExecutionAcceptanceResult =
	| { readonly type: "accepted" }
	| Extract<
			RuntimeToolExecutionResult,
			{ readonly type: "error" | "stale_custody" }
	  >;

/** Transfers a Sandbox Tool Use to durable execution custody. */
export type RuntimeSandboxExecutionAccepter = (
	request: RuntimeSandboxExecutionRequest,
) =>
	| RuntimeSandboxExecutionAcceptanceResult
	| Promise<RuntimeSandboxExecutionAcceptanceResult>;

/** Carries target-specific approval evidence and the reviewer-thread owner to the reviewer adapter. */
export interface RuntimeApprovalReviewRequest {
	readonly workspaceId: string;
	readonly sessionId: string;
	readonly sessionThreadId: string;
	readonly parentThreadId?: string | undefined;
	readonly bindingId: string;
	readonly bindingGeneration: number;
	readonly targetPodUid: string;
	readonly runtimeBindingToken: string;
	readonly modelRequestId: string;
	readonly parentBoundaryEventId: string;
	readonly targetModelToolCallId: string;
	readonly targetToolName: string;
	readonly actionJson: RuntimeJsonValue;
	readonly approvalReviewerManager: AutoApprovalReviewerManager;
	readonly parentTranscript: ParentTranscriptView;
	readonly currentAssistantDraft: readonly RuntimeContextPart[];
	readonly siblingToolCalls: readonly RuntimeApprovalReviewSiblingToolCall[];
	readonly policyContext: RuntimeJsonValue;
	readonly currentModel?:
		| {
				readonly providerId: string;
				readonly modelId: string;
		  }
		| undefined;
}

/** Captures one tool call present when a target-specific approval review is assembled. */
export interface RuntimeApprovalReviewSiblingToolCall {
	readonly modelToolCallId: string;
	readonly toolName: string;
	readonly actionJson: RuntimeJsonValue;
}

/** Internal reviewer result; settlement failures return to ThreadLoop's existing persistence closeout. */
export type RuntimeApprovalReviewResult =
	| ApprovalReviewerOutcome
	| {
			readonly type: "settlement_failed";
			readonly error: SessionEventWriterError;
	  };

/** Runs an internal approval review without exposing reviewer failures through the Effect error channel. */
export type RuntimeApprovalReviewer = (
	request: RuntimeApprovalReviewRequest,
) => Effect.Effect<RuntimeApprovalReviewResult, never>;

export interface RuntimeToolRegistrationState {
	readonly executionPolicy: { readonly toolCatalog?: ToolCatalog };
	readonly toolScheduler: ToolScheduler;
	readonly toolEntries: Record<string, ToolEntry | undefined>;
	nextToolModelOrder: number;
}

export function registerRuntimeToolCall(
	modelRequestId: string,
	state: RuntimeToolRegistrationState,
	event: Extract<LLMEvent, { readonly type: "tool-call" }>,
):
	| { readonly type: "registered"; readonly jobId: string }
	| { readonly type: "invalid" } {
	const toolCatalog = state.executionPolicy.toolCatalog;
	const entry =
		toolCatalog === undefined
			? undefined
			: lookupToolEntry(toolCatalog, event.toolName);
	if (entry === undefined || toolCatalog === undefined) {
		return { type: "invalid" };
	}
	const input = executionInputForToolCall(entry, event.input);
	if (input === undefined) {
		return { type: "invalid" };
	}
	const jobId = `${modelRequestId}:${event.id}`;
	const job: ToolJob = {
		id: jobId,
		modelOrder: state.nextToolModelOrder,
		modelToolCallId: event.id,
		kind:
			entry.route.kind === "gateway" && entry.route.operation === "RunMcpTool"
				? "mcp"
				: "builtin",
		name: event.toolName,
		route: entry.route,
		input,
		runPolicy: inferToolRunPolicy(entry, input),
		gateState: "runnable",
	};
	state.nextToolModelOrder += 1;
	state.toolEntries[job.id] = entry;
	state.toolScheduler.addJob(job);
	return { type: "registered", jobId };
}

function executionInputForToolCall(
	entry: ToolEntry,
	input: RuntimeJsonValue,
): RuntimeJsonValue | undefined {
	if (entry.inputContract.kind === "freeform_string") {
		return typeof input === "string"
			? { [entry.inputContract.executionField]: input }
			: undefined;
	}
	return typeof input === "object" && input !== null && !Array.isArray(input)
		? input
		: undefined;
}

function recoveredExecutionInput(
	entry: ToolEntry,
	input: RuntimeJsonValue,
): RuntimeJsonValue {
	const object = RuntimeJsonValueSchema.parse(input);
	if (typeof object !== "object" || object === null || Array.isArray(object)) {
		throw new Error("recovered tool input must be an object");
	}
	const record = object as Readonly<Record<string, RuntimeJsonValue>>;
	if (entry.inputContract.kind === "freeform_string") {
		const keys = Object.keys(record);
		if (
			keys.length !== 1 ||
			keys[0] !== entry.inputContract.executionField ||
			typeof record[entry.inputContract.executionField] !== "string"
		) {
			throw new Error("recovered freeform tool input is not canonical");
		}
	}
	return record;
}

export function installLoadedPendingToolUses(
	session: ThreadRuntime,
	resolveToolCatalog: () => ToolCatalog | undefined,
	pendingToolUses: readonly RuntimePreloadedPendingToolUseState[] | undefined,
	entries: readonly RuntimeContextEntry[],
	openRequestDraft: RuntimeOpenRequestDraft | undefined,
): { readonly ok: true } | { readonly ok: false; readonly error: unknown } {
	if (pendingToolUses === undefined || pendingToolUses.length === 0) {
		return { ok: true };
	}
	try {
		const toolCatalog = resolveToolCatalog();
		const currentModel = session.state.currentModel();
		if (currentModel === undefined) {
			throw new Error("pending tool use context has no current model");
		}
		if (toolCatalog === undefined) {
			throw new Error("pending tool use context has no tool catalog");
		}
		const seenToolUseEventIds = new Set<string>();
		const source: RuntimeProcessorSource = currentModel;
		for (const [pendingOrder, pending] of pendingToolUses.entries()) {
			if (seenToolUseEventIds.has(pending.toolUseEventId)) {
				throw new Error(
					"pending tool use context contains duplicate tool use id",
				);
			}
			seenToolUseEventIds.add(pending.toolUseEventId);
			const entry = lookupToolEntry(toolCatalog, pending.toolName);
			if (entry === undefined) {
				throw new Error(
					"pending tool use context references an unavailable tool",
				);
			}
			const input = recoveredExecutionInput(entry, pending.input);
			const loadedPart = findLoadedPendingToolUsePart(
				entries,
				openRequestDraft,
				pending,
			);
			if (loadedPart === undefined) {
				throw new Error(
					"pending tool use context is missing its sole unresolved Tool Call",
				);
			}
			const job: ToolJob = {
				id: `${pending.modelRequestId}:${pending.modelToolCallId}`,
				modelOrder: pendingOrder,
				toolUseEventId: pending.toolUseEventId,
				modelToolCallId: pending.modelToolCallId,
				kind:
					entry.route.kind === "gateway" &&
					entry.route.operation === "RunMcpTool"
						? "mcp"
						: "builtin",
				name: pending.toolName,
				route: entry.route,
				input,
				runPolicy: inferToolRunPolicy(entry, input),
				gateState: "waiting_approval",
				approvalSource: "user",
			};
			session.state.recordPendingApprovalToolJob({
				toolUseEventId: pending.toolUseEventId,
				modelRequestId: pending.modelRequestId,
				source,
				assistantMessageSequence: loadedPart.assistantMessageSequence,
				toolPart: loadedPart.part,
				job,
				entry,
				committedContext: entries,
				currentModel,
			});
			if (pending.decision !== undefined) {
				if (pending.decision === "allow" && pending.denyMessage !== undefined) {
					throw new Error(
						"pending tool use context contains allow decision with deny message",
					);
				}
				const confirmationResult = session.state.resolveToolConfirmation({
					workspaceId: session.identity.workspaceId,
					sessionId: session.identity.sessionId,
					sessionThreadId: session.identity.sessionThreadId,
					bindingId: session.identity.bindingId,
					bindingGeneration: session.identity.bindingGeneration,
					targetPodUid: session.identity.targetPodUid,
					runtimeInputId: `load_context:${pending.toolUseEventId}`,
					toolUseEventId: pending.toolUseEventId,
					decision: pending.decision,
					...(pending.denyMessage === undefined
						? {}
						: { denyMessage: pending.denyMessage }),
				});
				if (confirmationResult === "conflict") {
					throw new Error(
						"pending tool use context contains conflicting decision",
					);
				}
			}
		}
		return { ok: true };
	} catch (error) {
		return {
			ok: false,
			error: normalizeContextLoaderError({
				code: "schema_mismatch",
				rawError: error,
				sessionId: session.sessionId,
				reason:
					error instanceof Error
						? error.message
						: "pending tool use context is malformed",
			}),
		};
	}
}

export function installLoadedSandboxExecutions(
	session: ThreadRuntime,
	resolveToolCatalog: () => ToolCatalog | undefined,
	executions: readonly RuntimePreloadedSandboxExecutionState[] | undefined,
	entries: readonly RuntimeContextEntry[],
	openRequestDraft: RuntimeOpenRequestDraft | undefined,
): { readonly ok: true } | { readonly ok: false; readonly error: unknown } {
	if (executions === undefined || executions.length === 0) {
		return { ok: true };
	}
	try {
		const toolCatalog = resolveToolCatalog();
		const currentModel = session.state.currentModel();
		if (currentModel === undefined) {
			throw new Error("sandbox execution context has no current model");
		}
		if (toolCatalog === undefined) {
			throw new Error("sandbox execution context has no tool catalog");
		}
		const source: RuntimeProcessorSource = currentModel;
		const seenToolUseEventIds = new Set<string>();
		for (const [modelOrder, execution] of executions.entries()) {
			if (seenToolUseEventIds.has(execution.toolUseEventId)) {
				throw new Error(
					"sandbox execution context contains duplicate tool use id",
				);
			}
			seenToolUseEventIds.add(execution.toolUseEventId);
			const entry = lookupToolEntry(toolCatalog, execution.toolName);
			if (
				entry === undefined ||
				entry.route.kind !== "sandbox" ||
				entry.route.operation !== "RunTool"
			) {
				throw new Error(
					"sandbox execution context references an unavailable sandbox tool",
				);
			}
			const input = recoveredExecutionInput(entry, execution.input);
			const loadedPart = findLoadedPendingToolUsePart(
				entries,
				openRequestDraft,
				execution,
			);
			if (loadedPart === undefined) {
				throw new Error(
					"sandbox execution context is missing its sole unresolved Tool Call",
				);
			}
			session.state.recordPendingSandboxExecutionJob({
				recoveryKind: "sandbox_execution",
				toolUseEventId: execution.toolUseEventId,
				modelRequestId: execution.modelRequestId,
				source,
				assistantMessageSequence: loadedPart.assistantMessageSequence,
				toolPart: loadedPart.part,
				job: {
					id: `${execution.modelRequestId}:${execution.modelToolCallId}`,
					modelOrder,
					toolUseEventId: execution.toolUseEventId,
					modelToolCallId: execution.modelToolCallId,
					kind: "builtin",
					name: execution.toolName,
					route: entry.route,
					input,
					runPolicy: inferToolRunPolicy(entry, input),
					gateState: "runnable",
				},
				entry,
				committedContext: entries,
				currentModel,
			});
		}
		return { ok: true };
	} catch (error) {
		return {
			ok: false,
			error: normalizeContextLoaderError({
				code: "schema_mismatch",
				rawError: error,
				sessionId: session.sessionId,
				reason:
					error instanceof Error
						? error.message
						: "sandbox execution context is malformed",
			}),
		};
	}
}

export function findLoadedPendingToolUsePart(
	entries: readonly RuntimeContextEntry[],
	openRequestDraft: RuntimeOpenRequestDraft | undefined,
	pending: Pick<
		ContextLoader.RuntimeLoadedPendingToolUse,
		"toolUseEventId" | "modelToolCallId" | "toolName"
	>,
):
	| {
			readonly assistantMessageSequence: number;
			readonly part: Extract<
				RuntimeAssistantDraftPart,
				{ readonly type: "tool" }
			>;
	  }
	| undefined {
	const owners = [
		...entries
			.filter((entry) => entry.contextKind === "assistant")
			.map((entry) => ({
				messageSequence: entry.messageSequence,
				parts: entry.parts,
			})),
		...(openRequestDraft === undefined
			? []
			: [
					{
						messageSequence: openRequestDraft.messageSequence,
						parts: openRequestDraft.parts,
					},
				]),
	];
	const matches = owners.flatMap((owner) =>
		owner.parts
			.filter(
				(part) =>
					part.type === "tool_call" &&
					part.modelToolCallId === pending.modelToolCallId &&
					part.toolName === pending.toolName,
			)
			.map((part) => ({ owner, part })),
	);
	const resultCount = owners.reduce(
		(count, owner) =>
			count +
			owner.parts.filter(
				(part) =>
					part.type === "tool_result" &&
					part.modelToolCallId === pending.modelToolCallId,
			).length,
		0,
	);
	if (matches.length !== 1 || resultCount !== 0) return undefined;
	const { owner, part: call } = matches[0]!;
	if (call.type !== "tool_call") return undefined;
	const preview = boundRuntimeText(
		JSON.stringify(call.canonicalInput),
		RuntimePreviewTextMaxBytes,
	);
	return {
		assistantMessageSequence: owner.messageSequence,
		part: {
			type: "tool",
			modelToolCallId: pending.modelToolCallId,
			toolName: pending.toolName,
			toolUseEventId: pending.toolUseEventId,
			state: {
				status: "running",
				input: {
					value: call.canonicalInput,
					preview: preview.text,
					truncated: preview.truncated,
				},
			},
		},
	};
}

export function findPendingApprovalSettlementDescriptor(
	entries: readonly RuntimeContextEntry[],
	modelToolCallId: string,
	toolUseEventId: string,
):
	| {
			readonly entry: RuntimeContextEntry;
			readonly modelToolCallId: string;
			readonly toolUseEventId: string;
	  }
	| undefined {
	for (const entry of entries) {
		if (
			entry.parts.some(
				(part) =>
					part.type === "tool_call" && part.modelToolCallId === modelToolCallId,
			) &&
			!entry.parts.some(
				(part) =>
					part.type === "tool_result" &&
					part.modelToolCallId === modelToolCallId,
			)
		) {
			return { entry, modelToolCallId, toolUseEventId };
		}
	}
	return undefined;
}

export type RuntimeRecoveredToolJobState =
	| RuntimePendingApprovalToolJobState
	| RuntimePendingSandboxExecutionJobState;

export function isPendingSandboxExecution(
	state: RuntimeRecoveredToolJobState,
): state is RuntimePendingSandboxExecutionJobState {
	return "recoveryKind" in state && state.recoveryKind === "sandbox_execution";
}

export function pendingSandboxExecutionToolUseEventIds(
	session: ThreadRuntime,
): ReadonlySet<string> {
	return new Set(
		session.state
			.pendingSandboxExecutionJobs()
			.map((pending) => pending.toolUseEventId),
	);
}

export function publicToolEventForEntry(entry: ToolEntry): PublicToolEvent {
	if (
		entry.route.kind === "gateway" &&
		entry.route.operation === "RunMcpTool"
	) {
		return { kind: "mcp", mcpServerName: entry.route.mcpServerName };
	}
	return { kind: "tool" };
}

export function effectiveToolPermissionPolicy(
	toolCatalog: ToolCatalog,
	toolName: string,
): RuntimeJsonValue {
	const entry = lookupToolEntry(toolCatalog, toolName);
	return entry === undefined
		? "disabled"
		: effectivePermissionPolicy(entry, toolCatalog.configs);
}

export function invalidToolCallFailure(
	sessionId: string,
	source: RuntimeProcessorSource,
	toolName: string,
): RuntimeFailure {
	const failure = toolContractFailure(
		sessionId,
		source,
		`disabled or unknown tool call: ${toolName}`,
	);
	return RuntimeFailureSchema.parse({
		...failure,
		message: "Tool is unavailable.",
	});
}

export function deniedToolCallFailure(
	sessionId: string,
	source: RuntimeProcessorSource,
	message: string,
): RuntimeFailure {
	return toolContractFailure(sessionId, source, message);
}

export function pendingApprovalResumeFailure(
	sessionId: string,
	source: RuntimeProcessorSource,
	message: string,
): RuntimeFailure {
	return toolContractFailure(sessionId, source, message);
}

function toolContractFailure(
	sessionId: string,
	source: RuntimeProcessorSource,
	message: string,
): RuntimeFailure {
	return normalizeRuntimeFailure({
		type: "runtime",
		code: "runtime_invalid_sequence",
		retryable: false,
		fatal: false,
		reason: "runtime_contract_validation",
		sessionId,
		providerId: source.providerId,
		modelId: source.modelId,
		rawError: new Error(message),
	});
}

export function runRuntimeToolEffect(
	request: RuntimeSandboxExecutionRequest,
	source: RuntimeProcessorSource,
	runTool: RuntimeToolRunner,
	cancelJoinTimeoutMs: number,
): Effect.Effect<RuntimeToolExecutionResult, never> {
	return Effect.callback<RuntimeToolExecutionResult>((resume) => {
		const abortController = new AbortController();
		let resultClosed = false;
		let routeSettled = false;
		let routeSettledResolve: () => void = () => undefined;
		const routeSettledPromise = new Promise<void>((resolve) => {
			routeSettledResolve = resolve;
		});
		const settle = (result: RuntimeToolExecutionResult): void => {
			routeSettled = true;
			routeSettledResolve();
			if (resultClosed) {
				return;
			}
			resultClosed = true;
			resume(Effect.succeed(result));
		};
		Promise.resolve()
			.then(() => runTool({ ...request, abortSignal: abortController.signal }))
			.then(
				(result) => settle(result),
				(error) =>
					settle({
						type: "error",
						error: runtimeToolRunnerFailure(request.sessionId, source, error),
					}),
			);
		return Effect.gen(function* () {
			if (!routeSettled) {
				abortController.abort();
				yield* Effect.promise(() =>
					waitForToolRouteSettlement(routeSettledPromise, cancelJoinTimeoutMs),
				);
			}
			resultClosed = true;
		});
	});
}

export async function acceptRuntimeSandboxExecution(
	request: RuntimeSandboxExecutionRequest,
	source: RuntimeProcessorSource,
	accept: RuntimeSandboxExecutionAccepter,
): Promise<RuntimeSandboxExecutionAcceptanceResult> {
	try {
		return await accept(request);
	} catch (error) {
		return {
			type: "error",
			error: runtimeToolRunnerFailure(request.sessionId, source, error),
		};
	}
}

export function defaultRuntimeToolRunner(
	request: RuntimeToolExecutionRequest,
): RuntimeToolExecutionResult {
	return {
		type: "error",
		error: normalizeRuntimeFailure({
			type: "runtime",
			code: "runtime_invalid_sequence",
			retryable: false,
			fatal: false,
			reason: "runtime_contract_validation",
			sessionId: request.sessionId,
			rawError: new Error(
				`tool route ${request.entry.name} is not installed in this Runtime Pod harness`,
			),
		}),
	};
}

export function defaultRuntimeSandboxExecutionAccepter(
	request: RuntimeSandboxExecutionRequest,
): RuntimeSandboxExecutionAcceptanceResult {
	return {
		type: "error",
		error: normalizeRuntimeFailure({
			type: "runtime",
			code: "runtime_invalid_sequence",
			retryable: false,
			fatal: false,
			reason: "runtime_contract_validation",
			sessionId: request.sessionId,
			rawError: new Error(
				`sandbox execution acceptance for ${request.entry.name} is not installed in this Runtime Pod harness`,
			),
		}),
	};
}

export function defaultRuntimeSandboxExecutionWaiter(
	request: RuntimeToolExecutionRequest,
): RuntimeToolExecutionResult {
	return defaultRuntimeToolRunner(request);
}

function waitForToolRouteSettlement(
	routeSettled: Promise<void>,
	timeoutMs: number,
): Promise<void> {
	return new Promise((resolve) => {
		let resolved = false;
		const finish = (): void => {
			if (resolved) {
				return;
			}
			resolved = true;
			clearTimeout(timeout);
			resolve();
		};
		const timeout = setTimeout(finish, timeoutMs);
		routeSettled.then(finish, finish);
	});
}

function runtimeToolRunnerFailure(
	sessionId: string,
	source: RuntimeProcessorSource,
	error: unknown,
): RuntimeFailure {
	return normalizeRuntimeFailure({
		type: "runtime",
		code: "runtime_invalid_sequence",
		retryable: true,
		fatal: false,
		reason: "runtime_contract_validation",
		sessionId,
		providerId: source.providerId,
		modelId: source.modelId,
		rawError: error,
	});
}
