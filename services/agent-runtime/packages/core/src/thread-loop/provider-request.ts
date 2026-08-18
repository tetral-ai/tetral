/**
 * @packageDocumentation
 * Assembles validated runtime state into the single provider-request shape sent by an agent run.
 * It guards request-kind-specific system composition and schema coupling, system-segment ordering
 * and cache hints, provider-visible tool projection, and the byte and identity bounds that must hold
 * before a Gateway call is attempted.
 * ThreadLoop calls this module with cold runtime configuration; request assembly stays pure,
 * while the scoped provider Fiber lifecycle is owned here and supplied an already-built Effect.
 */

import { MaxTextBytes } from "@tetral/gateway-protocol/src/bounds.js";
import type {
	ProviderContextEntry as GatewayProviderContextEntry,
	ProviderRequestAttachment as GatewayProviderRequestAttachment,
	RuntimeToolDefinition as GatewayRuntimeToolDefinition,
	ProviderRequest,
	SystemSegment,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import {
	ProviderRequestKind,
	SystemCacheHint,
	SystemSegmentKind,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { Scope } from "effect";
import { Cause, Effect, Exit, Fiber } from "effect";
import type {
	RuntimeAssistantContextAppend,
	RuntimeFailure,
	RuntimeJsonValue,
	RuntimeProviderAttachment,
	RuntimeRequestErrorKind,
	SessionEventEnvelope,
	SessionEventWriterRequestEndEnvelope,
	SessionEventWriterRequestEndResult,
} from "../contracts/runtime.js";
import {
	normalizeRuntimeFailure,
	normalizeSessionEventWriterError,
} from "../contracts/runtime.js";
import type {
	RuntimeFailure as LLMRuntimeFailure,
	RuntimeAttachmentRejection,
} from "../llm/llm-event.js";
import { RuntimeFailureSchema as LLMRuntimeFailureSchema } from "../llm/llm-event.js";
import type { LLMRequest } from "../llm/llm-service.js";
import type {
	RuntimeMetricOutcome,
	RuntimeProviderStreamKind,
} from "../runtime/metrics.js";
import { NoopRuntimeMetricsSink } from "../runtime/metrics.js";
import type { ToolCatalog } from "../tools/tool-catalog.js";
import { providerToolDefinitions } from "../tools/tool-catalog.js";
import type {
	ThreadLoopRuntimeOptions,
	ThreadLoopRuntimePolicy,
} from "./thread-loop.js";
import type { RuntimeThreadIdentity, ThreadRuntime } from "./thread-runtime.js";

/** Describes one resolved skill version rendered into deterministic provider guidance. */
export interface SkillGuidanceIndexEntry {
	readonly skillId: string;
	readonly skillVersionId: string;
	readonly version: string;
	readonly name: string;
	readonly description: string;
	readonly directory: string;
}

/** Describes one memory store whose immutable metadata becomes an agent-only system segment. */
export interface MemoryStorePromptEntry {
	readonly memoryStoreId: string;
	readonly name: string;
	readonly access: "read_write" | "read_only";
	readonly instructions?: string;
}

/**
 * Carries the cold and request-local inputs that control provider-request assembly.
 * Callers select the fields appropriate to their request kind; this module additionally restricts
 * system-segment composition and couples reviewer requests to reviewer policy and output schema.
 */
export interface ProviderCallRuntimeConfig {
	readonly systemInstructions: string;
	readonly agentSystem?: string;
	readonly toolsetFamily?: "claude" | "gpt";
	readonly approvalReviewerPolicy?: string;
	readonly toolCatalog?: ToolCatalog;
	readonly maxOutputTokens?: number;
	readonly timeoutMs?: number;
	readonly modelVariant?: string;
	readonly requestKind?: ProviderRequestKind;
	readonly outputSchemaJson?: string;
	readonly attachments?: readonly RuntimeProviderAttachment[];
	readonly skillsIndex?: readonly SkillGuidanceIndexEntry[];
	readonly memoryStores?: readonly MemoryStorePromptEntry[];
	readonly skillGuidanceDescriptionBudgetBytes?: number;
}

/** Supplies authenticated identity, model selection, provider context, and runtime config for one call. */
export interface ProviderCallAssemblyInput {
	readonly identity: RuntimeThreadIdentity;
	readonly requestId: string;
	readonly modelRequestId: string;
	readonly currentModel: {
		readonly providerId: string;
		readonly modelId: string;
	};
	readonly providerContext: readonly GatewayProviderContextEntry[];
	readonly runtime: ProviderCallRuntimeConfig;
}

/** Returns both the complete wire request and the bounded values used to build it. */
export interface ProviderCallAssemblySuccess {
	readonly ok: true;
	readonly system: readonly SystemSegment[];
	readonly tools: readonly GatewayRuntimeToolDefinition[];
	readonly maxOutputTokens?: number;
	readonly timeoutMs: number;
	readonly request: ProviderRequest;
	readonly runtimeAttachments: readonly RuntimeProviderAttachment[];
}

/** Reports a normalized, non-retryable failure when local request invariants do not hold. */
export interface ProviderCallAssemblyFailure {
	readonly ok: false;
	readonly error: RuntimeFailure;
	readonly toolDeclarationRejection?: ProviderToolDeclarationRejection;
}

/** Bounded structural fact for a model-visible declaration rejected before request open. */
export interface ProviderToolDeclarationRejection {
	readonly declarationKind: "function" | "freeform" | "unknown";
	readonly family: "claude" | "gpt" | "unspecified";
	readonly validationMember:
		| "declaration_kind"
		| "tool_family"
		| "function_schema"
		| "freeform_grammar";
}

export type ProviderCallAssemblyResult =
	| ProviderCallAssemblySuccess
	| ProviderCallAssemblyFailure;

/** Allows ThreadLoop to inject an equivalent synchronous or asynchronous request assembler. */
export type ProviderCallAssembler = (
	input: ProviderCallAssemblyInput,
) => ProviderCallAssemblyResult | Promise<ProviderCallAssemblyResult>;

/** One provider attachment excluded after a normalized Gateway rejection. */
export interface RejectedProviderAttachment {
	readonly attachment: RuntimeProviderAttachment;
	readonly reason: RuntimeAttachmentRejection["reason"];
}

interface RequestEndProjection {
	applyRequestEndAppend(
		append: RuntimeAssistantContextAppend | undefined,
		result: { readonly sealedMessageSequence?: number | undefined },
	): boolean;
}

export type EffectRestore = <A, E, R>(
	effect: Effect.Effect<A, E, R>,
) => Effect.Effect<A, E, R>;

/** Runs one provider stream Fiber inside its Thread-turn scope and joins it interruptibly. */
export function runProviderStreamLifecycle<A, E, R>(
	restore: EffectRestore,
	providerStream: Effect.Effect<A, E, R>,
	requestScope: Scope.Closeable,
	abortProvider: () => void,
): Effect.Effect<
	{
		readonly streamExit: Exit.Exit<A, E>;
		readonly interruptProvider: Effect.Effect<void, never>;
	},
	never,
	R
> {
	return Effect.gen(function* () {
		const providerFiber = yield* restore(providerStream).pipe(
			Effect.forkIn(requestScope),
		);
		const interruptProvider = Effect.sync(abortProvider).pipe(
			Effect.andThen(Fiber.interrupt(providerFiber)),
			Effect.exit,
			Effect.asVoid,
		);
		const streamExit = yield* restore(
			Fiber.join(providerFiber).pipe(
				Effect.onInterrupt(() => interruptProvider),
			),
		).pipe(Effect.exit);
		return { streamExit, interruptProvider };
	});
}

export function recordProviderStreamDuration(
	options: ThreadLoopRuntimeOptions,
	kind: RuntimeProviderStreamKind,
	startedAt: number,
	outcome: RuntimeMetricOutcome,
): void {
	(options.metrics ?? NoopRuntimeMetricsSink).observeProviderStreamDuration(
		kind,
		options.runtime.monotonicMs() - startedAt,
		outcome,
	);
}

export function providerStreamMetricOutcome(
	exit: Exit.Exit<unknown, unknown>,
	runtimeShutdownRequested: boolean,
): RuntimeMetricOutcome {
	if (runtimeShutdownRequested) {
		return "cancelled";
	}
	if (Exit.isSuccess(exit)) {
		return "success";
	}
	return Cause.hasInterruptsOnly(exit.cause) ? "cancelled" : "error";
}

export async function assembleLLMRequest(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	currentModel: { readonly providerId: string; readonly modelId: string },
	providerContext: readonly GatewayProviderContextEntry[],
	executionPolicy: Readonly<ThreadLoopRuntimePolicy>,
): Promise<
	| {
			readonly ok: true;
			readonly request: LLMRequest;
			readonly runtimeAttachments: readonly RuntimeProviderAttachment[];
	  }
	| { readonly ok: false; readonly error: RuntimeFailure }
> {
	const assembler =
		options.providerCallAssembler ?? assembleProviderCallRequest;
	const requestId = options.runtime.createId("provider_request");
	const modelRequestId = options.runtime.createId("model_request");
	try {
		const result = await assembler({
			identity: session.identity,
			requestId,
			modelRequestId,
			currentModel,
			providerContext,
			runtime: providerCallRuntimeForSession(session, options, executionPolicy),
		});
		if (!result.ok && result.toolDeclarationRejection !== undefined) {
			recordProviderToolDeclarationRejection(
				options,
				session,
				requestId,
				modelRequestId,
				result.toolDeclarationRejection,
			);
		}
		return result.ok
			? {
					ok: true,
					request: result.request,
					runtimeAttachments: result.runtimeAttachments,
				}
			: { ok: false, error: result.error };
	} catch (error) {
		return {
			ok: false,
			error: normalizeRuntimeFailure({
				type: "runtime",
				code: "runtime_invalid_sequence",
				retryable: false,
				fatal: true,
				reason: "runtime_contract_validation",
				rawError: error,
				sessionId: session.sessionId,
				providerId: currentModel.providerId,
				modelId: currentModel.modelId,
			}),
		};
	}
}

function recordProviderToolDeclarationRejection(
	options: ThreadLoopRuntimeOptions,
	session: ThreadRuntime,
	requestId: string,
	modelRequestId: string,
	rejection: ProviderToolDeclarationRejection,
): void {
	try {
		options.recordProviderToolDeclarationRejection?.({
			workspaceId: session.identity.workspaceId,
			sessionId: session.identity.sessionId,
			sessionThreadId: session.identity.sessionThreadId,
			requestId,
			modelRequestId,
			...rejection,
		});
	} catch {
		// Declaration settlement is authoritative; observability stays a side channel.
	}
}

export function recordAttachmentRejections(
	requestAttachments: readonly RuntimeProviderAttachment[],
	rejectedAttachments: RejectedProviderAttachment[],
	rejections: readonly RuntimeAttachmentRejection[],
): void {
	for (const rejection of rejections) {
		const identity = attachmentRejectionOriginIdentity(rejection.origin);
		const attachment = requestAttachments.find(
			(candidate) => providerRequestAttachmentIdentity(candidate) === identity,
		);
		if (
			attachment === undefined ||
			rejectedAttachments.some(
				(existing) =>
					providerRequestAttachmentIdentity(existing.attachment) === identity,
			)
		) {
			continue;
		}
		rejectedAttachments.push({ attachment, reason: rejection.reason });
	}
}

export function attachmentConsumptionUnion(
	carriedAttachments: readonly RuntimeProviderAttachment[],
	rejectedAttachments: Iterable<RejectedProviderAttachment>,
): readonly RuntimeProviderAttachment[] {
	const union: RuntimeProviderAttachment[] = [];
	const identities = new Set<string>();
	for (const attachment of carriedAttachments) {
		const identity = providerRequestAttachmentIdentity(attachment);
		if (!identities.has(identity)) {
			identities.add(identity);
			union.push(attachment);
		}
	}
	for (const rejection of rejectedAttachments) {
		const identity = providerRequestAttachmentIdentity(rejection.attachment);
		if (!identities.has(identity)) {
			identities.add(identity);
			union.push(rejection.attachment);
		}
	}
	return union;
}

function attachmentRejectionOriginIdentity(
	origin: RuntimeAttachmentRejection["origin"],
): string {
	return origin.type === "transient"
		? JSON.stringify(["transient", origin.attachmentRef])
		: JSON.stringify(["file-backed", origin.sourceEventId, origin.fileId]);
}

export function providerCallRuntimeForSession(
	session: ThreadRuntime,
	options: ThreadLoopRuntimeOptions,
	policy: Readonly<ThreadLoopRuntimePolicy>,
): ProviderCallRuntimeConfig {
	const runtime =
		options.providerCallRuntime ?? DefaultProviderCallRuntimeConfig;
	const {
		outputSchemaJson: _discardedGlobalOutputSchema,
		...runtimeWithoutOutputSchema
	} = runtime;
	const outputSchemaJson = session.state.providerRequestOutputSchemaJson();
	const toolsetFamily = session.configuration.installedBuiltinFamily();
	const attachments = [
		...(runtime.attachments ?? []),
		...session.state.beginPendingAttachmentRide(),
	];
	return {
		...runtimeWithoutOutputSchema,
		...(toolsetFamily === undefined ? {} : { toolsetFamily }),
		...(session.identity.threadRole === "approval_reviewer"
			? {
					requestKind:
						ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
					...(outputSchemaJson === undefined ? {} : { outputSchemaJson }),
				}
			: {}),
		...(policy.toolCatalog === undefined
			? {}
			: { toolCatalog: policy.toolCatalog }),
		...(policy.system === undefined ? {} : { agentSystem: policy.system }),
		...(policy.skillsIndex === undefined
			? {}
			: { skillsIndex: policy.skillsIndex }),
		...(policy.memoryStores === undefined
			? {}
			: { memoryStores: policy.memoryStores }),
		...(attachments.length === 0 ? {} : { attachments }),
	};
}

export function requestEndKindForSession(
	session: ThreadRuntime,
): NonNullable<SessionEventEnvelope["requestKind"]> | undefined {
	return session.identity.threadRole === "approval_reviewer"
		? "approval_reviewer"
		: undefined;
}

export function requestErrorKindFromFailure(
	failure: RuntimeFailure,
): RuntimeRequestErrorKind {
	if (failure.type === "provider") {
		return "provider_error";
	}
	if (failure.reason === "runtime_shutdown") {
		return "runtime_interrupted";
	}
	if (
		failure.code === "gateway_stream_error" ||
		failure.code === "gateway_unavailable"
	) {
		return "gateway_stream_error";
	}
	if (failure.code === "gateway_protocol_error") {
		return "gateway_protocol_error";
	}
	if (
		failure.type === "runtime" &&
		failure.code === "runtime_invalid_sequence" &&
		(failure.reason === "runtime_contract_validation" ||
			failure.reason === "bounded")
	) {
		return "runtime_semantic_error";
	}
	return "runtime_persistence_error";
}

export type RequestEndSealApplication =
	| { readonly type: "applied" }
	| { readonly type: "stale_custody" }
	| { readonly type: "failed"; readonly error: RuntimeFailure };

export function applyRequestEndSeal(
	processor: RequestEndProjection,
	append: RuntimeAssistantContextAppend | undefined,
	result:
		| { readonly type: "stale" }
		| Extract<
				SessionEventWriterRequestEndResult,
				{ readonly ok: true; readonly type: "committed" | "duplicate" }
		  >["outcome"],
): RequestEndSealApplication {
	if (result.type === "stale") {
		return { type: "stale_custody" };
	}
	try {
		if (
			processor.applyRequestEndAppend(
				append,
				result.type === "ordinary" ? result : {},
			)
		) {
			return { type: "applied" };
		}
	} catch {
		// The normalized failure below owns malformed acknowledgement details.
	}
	const error = normalizeSessionEventWriterError({ code: "schema_mismatch" });
	const runtimeCode =
		error.code === "superseded" || error.code === "unrepairable"
			? "runtime_invalid_sequence"
			: error.code;
	return {
		type: "failed",
		error: normalizeRuntimeFailure({
			type: "session-event-writer",
			code: runtimeCode,
			retryable: error.retryable,
			fatal: error.fatal,
			sessionId: error.sessionId,
		}),
	};
}

export function providerRequestWithoutRejectedAttachments(
	request: LLMRequest,
	rejectedAttachments: readonly RejectedProviderAttachment[],
): LLMRequest {
	if (rejectedAttachments.length === 0) {
		return request;
	}
	return {
		...request,
		attachments: request.attachments.filter(
			(attachment) =>
				!rejectedAttachments.some(
					(rejection) =>
						providerRequestAttachmentIdentity(rejection.attachment) ===
						providerRequestAttachmentIdentity(attachment),
				),
		),
	};
}

export function runtimeAttachmentsWithoutRejectedAttachments(
	attachments: readonly RuntimeProviderAttachment[],
	rejectedAttachments: readonly RejectedProviderAttachment[],
): readonly RuntimeProviderAttachment[] {
	return attachments.filter(
		(attachment) =>
			!rejectedAttachments.some(
				(rejection) =>
					providerRequestAttachmentIdentity(rejection.attachment) ===
					providerRequestAttachmentIdentity(attachment),
			),
	);
}

type ProviderAttachmentIdentity = Pick<
	GatewayProviderRequestAttachment,
	"transient" | "fileBacked"
>;

export function providerRequestAttachmentIdentity(
	attachment: ProviderAttachmentIdentity,
): string {
	if (attachment.transient !== undefined) {
		return JSON.stringify(["transient", attachment.transient.attachmentRef]);
	}
	if (attachment.fileBacked !== undefined) {
		return JSON.stringify([
			"file-backed",
			attachment.fileBacked.sourceEventId,
			attachment.fileBacked.fileId,
		]);
	}
	return JSON.stringify(["invalid"]);
}

export function runtimeProviderStreamKindFromRequest(
	request: LLMRequest,
): RuntimeProviderStreamKind {
	switch (request.requestKind) {
		case ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST:
			return "agent_provider_request";
		case ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER:
			return "approval_reviewer";
		case ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER_COMPACTION:
		case ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY:
			return "compaction_summary";
		default:
			throw new Error("provider request kind is not supported");
	}
}

export function requestEndKindFromRequest(
	request: LLMRequest,
): NonNullable<SessionEventEnvelope["requestKind"]> | undefined {
	switch (request.requestKind) {
		case ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER:
			return "approval_reviewer";
		case ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER_COMPACTION:
		case ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY:
			return "compaction_summary";
		default:
			return undefined;
	}
}

export function providerStreamExhaustedFailure(
	request: LLMRequest,
): LLMRuntimeFailure {
	return LLMRuntimeFailureSchema.parse(
		normalizeRuntimeFailure({
			type: "runtime",
			code: "gateway_stream_error",
			retryable: false,
			fatal: true,
			retryStatus: { type: "terminal" },
			providerId: request.model?.providerId,
			modelId: request.model?.modelId,
		}),
	);
}

// The platform base prompt is a stable environment-facts slot. Its former
// cross-tool-discipline slot is deliberately empty; tool-specific guidance
// belongs to each tool description.
// UPDATE-WITH: services/gateway/packages/provider-gateway/test/golden/anthropic-wire.test.ts
/** Stable platform-owned environment context placed first on non-compaction provider requests. */
export const PlatformBaseSystemPrompt = `You are Tetral Agent, working in a sandboxed Linux environment.

Files in your sandbox persist for the life of this session,
including across session sleep and wake, and are gone when the
session ends. To keep something across sessions, use the memory
tool. To deliver a file to the user, save it under
/mnt/session/outputs — files there are collected and delivered
automatically.`;

/** Provider-visible instructions appended to the base segment for GPT-family agent requests. */
export const ApplyPatchInstructionsText = `## \`apply_patch\`

Use the \`apply_patch\` tool directly to edit files. Pass the patch as a raw string; do not JSON-wrap it.
Your patch language is a stripped-down, file-oriented diff format designed to be easy to parse and safe to apply. You can think of it as a high-level envelope:

*** Begin Patch
[ one or more file sections ]
*** End Patch

Within that envelope, you get a sequence of file operations.
You MUST include a header to specify the action you are taking.
Each operation starts with one of three headers:

*** Add File: <path> - create a new file. Every following line is a + line (the initial contents).
*** Delete File: <path> - remove an existing file. Nothing follows.
*** Update File: <path> - patch an existing file in place (optionally with a rename).

May be immediately followed by *** Move to: <new path> if you want to rename the file.
Then one or more "hunks", each introduced by @@ (optionally followed by a hunk header).
Within a hunk each line starts with:

For instructions on [context_before] and [context_after]:
- By default, show 3 lines of code immediately above and 3 lines immediately below each change. If a change is within 3 lines of a previous change, do NOT duplicate the first change's [context_after] lines in the second change's [context_before] lines.
- If 3 lines of context is insufficient to uniquely identify the snippet of code within the file, use the @@ operator to indicate the class or function to which the snippet belongs. For instance, we might have:
@@ class BaseClass
[3 lines of pre-context]
- [old_code]
+ [new_code]
[3 lines of post-context]

- If a code block is repeated so many times in a class or function such that even a single \`@@\` statement and 3 lines of context cannot uniquely identify the snippet of code, you can use multiple \`@@\` statements to jump to the right context. For instance:

@@ class BaseClass
@@ \t def method():
[3 lines of pre-context]
- [old_code]
+ [new_code]
[3 lines of post-context]

The full grammar definition is below:
Patch := Begin { FileOp } End
Begin := "*** Begin Patch" NEWLINE
End := "*** End Patch" NEWLINE
FileOp := AddFile | DeleteFile | UpdateFile
AddFile := "*** Add File: " path NEWLINE { "+" line NEWLINE }
DeleteFile := "*** Delete File: " path NEWLINE
UpdateFile := "*** Update File: " path NEWLINE [ MoveTo ] Change { Change }
MoveTo := "*** Move to: " newPath NEWLINE
Change := [ "@@" [ header ] NEWLINE ] HunkLine { HunkLine } [ "*** End of File" NEWLINE ]
HunkLine := (" " | "-" | "+") text NEWLINE

A full patch can combine several operations:

*** Begin Patch
*** Add File: hello.txt
+Hello world
*** Update File: src/app.py
*** Move to: src/main.py
@@ def greet():
-print("Hi")
+print("Hello, world!")
*** Delete File: obsolete.txt
*** End Patch

It is important to remember:

- You must include a header with your intended action (Add/Delete/Update)
- You must prefix new lines with + even when creating a new file
- Relative paths resolve against the workspace root. Absolute paths are accepted under the declared writable roots — the workspace, /mnt/session/uploads, and /mnt/session/outputs — and rejected outside them.

Call the tool directly with the full raw patch envelope; do not JSON-wrap it.`;

/** Default platform prompt and skill-description budget used when a host supplies no overrides. */
export const DefaultProviderCallRuntimeConfig: ProviderCallRuntimeConfig = {
	systemInstructions: PlatformBaseSystemPrompt,
	skillGuidanceDescriptionBudgetBytes: 32 * 1_024,
};

const SkillDescriptionPerEntryMaxBytes = 4 * 1_024;

function assemblyFailure(
	input: ProviderCallAssemblyInput,
	reason: "bounded" | "runtime_contract_validation",
	toolDeclarationRejection?: ProviderToolDeclarationRejection,
): ProviderCallAssemblyFailure {
	return {
		ok: false,
		error: normalizeRuntimeFailure({
			type: "runtime",
			code: "runtime_invalid_sequence",
			retryable: false,
			fatal: true,
			reason,
			sessionId: input.identity.sessionId,
			providerId: input.currentModel.providerId,
			modelId: input.currentModel.modelId,
		}),
		...(toolDeclarationRejection === undefined
			? {}
			: { toolDeclarationRejection }),
	};
}

/**
 * Validates and assembles one provider request without performing network or durable-state work.
 * Agent requests order base, optional agent text, memory stores, and optional skill guidance;
 * reviewer requests carry base then reviewer policy, and both compaction kinds carry no system segments.
 */
export function assembleProviderCallRequest(
	input: ProviderCallAssemblyInput,
): ProviderCallAssemblyResult {
	const requestKind =
		input.runtime.requestKind ??
		ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST;
	if (
		![
			ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
			ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
			ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY,
			ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER_COMPACTION,
		].includes(requestKind)
	) {
		return assemblyFailure(input, "runtime_contract_validation");
	}
	const compactionRequest =
		requestKind ===
			ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY ||
		requestKind ===
			ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER_COMPACTION;
	const systemInstructions = input.runtime.systemInstructions.trim();
	if (
		(!compactionRequest && systemInstructions.length === 0) ||
		(input.providerContext.length === 0 &&
			(input.runtime.attachments?.length ?? 0) === 0) ||
		input.identity.runtimeBindingToken.length === 0 ||
		input.identity.bindingGeneration <= 0
	) {
		return assemblyFailure(input, "runtime_contract_validation");
	}
	// No output budget is invented here: an absent value means the provider's
	// documented model output limit governs (Gateway lowering owns that value).
	const maxOutputTokens = input.runtime.maxOutputTokens;
	const timeoutMs = input.runtime.timeoutMs;
	if (
		(maxOutputTokens !== undefined && !positiveInteger(maxOutputTokens)) ||
		timeoutMs === undefined ||
		!positiveInteger(timeoutMs)
	) {
		return assemblyFailure(input, "bounded");
	}
	const approvalReviewerRequest =
		requestKind === ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER;
	const approvalReviewerPolicy = input.runtime.approvalReviewerPolicy?.trim();
	if (
		approvalReviewerRequest !==
			(input.runtime.outputSchemaJson !== undefined) ||
		(approvalReviewerRequest &&
			(approvalReviewerPolicy === undefined ||
				approvalReviewerPolicy.length === 0))
	) {
		return assemblyFailure(input, "runtime_contract_validation");
	}

	const baseSystemInstructions =
		requestKind ===
			ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST &&
		input.runtime.toolsetFamily === "gpt"
			? `${systemInstructions}\n\n${ApplyPatchInstructionsText}`
			: systemInstructions;
	if (!compactionRequest && utf8Bytes(baseSystemInstructions) > MaxTextBytes) {
		return assemblyFailure(input, "bounded");
	}
	const system: SystemSegment[] = compactionRequest
		? []
		: [
				{
					kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
					text: baseSystemInstructions,
					cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE,
				},
			];
	if (approvalReviewerRequest) {
		system.push({
			kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_APPROVAL_REVIEWER_POLICY,
			text: approvalReviewerPolicy ?? "",
			cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE,
		});
	}
	if (
		requestKind ===
		ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST
	) {
		const agentSystem = input.runtime.agentSystem;
		if (agentSystem !== undefined) {
			if (agentSystem.length === 0 || utf8Bytes(agentSystem) > MaxTextBytes) {
				return assemblyFailure(input, "bounded");
			}
			system.push({
				kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_AGENT,
				text: agentSystem,
				cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_SESSION,
			});
		}
		for (const memoryStore of input.runtime.memoryStores ?? []) {
			const text = renderMemoryStoreSegment(memoryStore);
			if (text === undefined || utf8Bytes(text) > MaxTextBytes) {
				return assemblyFailure(input, "bounded");
			}
			system.push({
				kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_MEMORY,
				text,
				cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_SESSION,
			});
		}
	}
	if (
		requestKind ===
			ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST &&
		(input.runtime.skillsIndex?.length ?? 0) > 0
	) {
		const descriptionBudgetBytes =
			input.runtime.skillGuidanceDescriptionBudgetBytes;
		if (
			!positiveInteger(descriptionBudgetBytes ?? 0) ||
			(descriptionBudgetBytes ?? 0) >= MaxTextBytes
		) {
			return assemblyFailure(input, "bounded");
		}
		system.push({
			kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_SKILL,
			text: renderSkillGuidanceSegment(
				input.runtime.skillsIndex ?? [],
				descriptionBudgetBytes ?? 0,
			),
			cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE,
		});
	}
	const projectedTools = projectProviderTools(input);
	if (!projectedTools.ok) {
		return assemblyFailure(
			input,
			"runtime_contract_validation",
			projectedTools.rejection,
		);
	}
	const tools = projectedTools.tools;
	const request: ProviderRequest = {
		requestId: input.requestId,
		modelRequestId: input.modelRequestId,
		requestKind,
		workspaceId: input.identity.workspaceId,
		sessionId: input.identity.sessionId,
		sessionThreadId: input.identity.sessionThreadId,
		bindingId: input.identity.bindingId,
		bindingGeneration: input.identity.bindingGeneration,
		runtimeBindingToken: input.identity.runtimeBindingToken,
		model: {
			providerId: input.currentModel.providerId,
			modelId: input.currentModel.modelId,
			variant: input.runtime.modelVariant ?? "",
		},
		system,
		context: [...input.providerContext],
		tools,
		attachments: (input.runtime.attachments ?? []).map(
			toGatewayProviderAttachment,
		),
		limits: {
			// Zero is the wire encoding of "unset": the provider's documented model
			// output limit governs at Gateway lowering.
			maxOutputTokens: maxOutputTokens ?? 0,
			timeoutMs,
		},
		...(input.runtime.outputSchemaJson !== undefined
			? { outputSchemaJson: input.runtime.outputSchemaJson }
			: {}),
	};

	return {
		ok: true,
		system,
		tools,
		runtimeAttachments: [...(input.runtime.attachments ?? [])],
		...(maxOutputTokens !== undefined ? { maxOutputTokens } : {}),
		timeoutMs,
		request,
	};
}

function toGatewayProviderAttachment(
	attachment: RuntimeProviderAttachment,
): GatewayProviderRequestAttachment {
	return attachment.transient !== undefined
		? {
				transient: {
					attachmentRef: attachment.transient.attachmentRef,
					sourcePath: attachment.transient.sourcePath,
					pageRange: attachment.transient.pageRange,
					detail: attachment.transient.detail,
				},
				fileBacked: undefined,
				mime: attachment.mime,
				filename: attachment.filename,
			}
		: {
				transient: undefined,
				fileBacked: {
					sourceEventId: attachment.fileBacked.sourceEventId,
					fileId: attachment.fileBacked.fileId,
				},
				mime: attachment.mime,
				filename: attachment.filename,
			};
}

function projectProviderTools(
	input: ProviderCallAssemblyInput,
):
	| { readonly ok: true; readonly tools: GatewayRuntimeToolDefinition[] }
	| {
			readonly ok: false;
			readonly rejection: ProviderToolDeclarationRejection;
	  } {
	const family = input.runtime.toolsetFamily ?? "unspecified";
	const definitions =
		input.runtime.toolCatalog === undefined
			? []
			: providerToolDefinitions(input.runtime.toolCatalog);
	const tools: GatewayRuntimeToolDefinition[] = [];
	for (const definition of definitions) {
		if (definition.kind === "freeform") {
			if (
				(family !== "gpt" && family !== "unspecified") ||
				definition.name !== "apply_patch"
			) {
				return {
					ok: false,
					rejection: {
						declarationKind: "freeform",
						family,
						validationMember: "tool_family",
					},
				};
			}
			if (
				typeof definition.larkGrammar !== "string" ||
				definition.larkGrammar.length === 0
			) {
				return {
					ok: false,
					rejection: {
						declarationKind: "freeform",
						family,
						validationMember: "freeform_grammar",
					},
				};
			}
			tools.push({
				name: definition.name,
				description: definition.description,
				freeform: { larkGrammar: definition.larkGrammar },
			});
			continue;
		}
		if (definition.kind !== "function") {
			return {
				ok: false,
				rejection: {
					declarationKind: "unknown",
					family,
					validationMember: "declaration_kind",
				},
			};
		}
		try {
			const inputSchemaJson = JSON.stringify(definition.inputSchema);
			const outputSchemaJson =
				definition.outputSchema === undefined
					? undefined
					: JSON.stringify(definition.outputSchema);
			if (
				inputSchemaJson === undefined ||
				(definition.outputSchema !== undefined &&
					outputSchemaJson === undefined)
			) {
				throw new Error("tool schema is not JSON serializable");
			}
			tools.push({
				name: definition.name,
				description: definition.description,
				function: {
					inputSchemaJson,
					...(outputSchemaJson !== undefined ? { outputSchemaJson } : {}),
				},
			});
		} catch {
			return {
				ok: false,
				rejection: {
					declarationKind: "function",
					family,
					validationMember: "function_schema",
				},
			};
		}
	}
	return { ok: true, tools };
}

function renderMemoryStoreSegment(
	memoryStore: MemoryStorePromptEntry,
): string | undefined {
	if (
		memoryStore.memoryStoreId.length === 0 ||
		memoryStore.name.length === 0 ||
		(memoryStore.access !== "read_write" &&
			memoryStore.access !== "read_only") ||
		memoryStore.instructions === ""
	) {
		return undefined;
	}
	const header = `Memory store: ${memoryStore.name}\nAccess: ${memoryStore.access}`;
	return memoryStore.instructions === undefined
		? header
		: `${header}\nInstructions:\n${memoryStore.instructions}`;
}

/** Renders the deterministic, bounded metadata segment for the session's resolved skills. */
export function renderSkillGuidanceSegment(
	entries: readonly SkillGuidanceIndexEntry[],
	descriptionBudgetBytes: number,
): string {
	if (
		!positiveInteger(descriptionBudgetBytes) ||
		descriptionBudgetBytes >= MaxTextBytes
	) {
		throw new Error("skill guidance description budget is invalid");
	}
	const ordered = [...entries].sort(
		(left, right) =>
			compareStrings(left.skillId, right.skillId) ||
			compareStrings(left.skillVersionId, right.skillVersionId),
	);
	let perEntryTruncated = false;
	let descriptions = ordered.map((entry) => {
		const bounded = truncateUTF8(
			entry.description,
			SkillDescriptionPerEntryMaxBytes,
		);
		perEntryTruncated ||= bounded.truncated;
		return bounded.text;
	});

	let uniformlyShortened = false;
	if (sumUTF8Bytes(descriptions) > descriptionBudgetBytes) {
		uniformlyShortened = true;
		const uniformCap = largestUniformDescriptionCap(
			descriptions,
			descriptionBudgetBytes,
		);
		descriptions = descriptions.map(
			(description) => truncateUTF8(description, uniformCap).text,
		);
	}

	const renderedEntries = ordered.map((entry, index) => ({
		version: entry.version,
		name: entry.name,
		description: descriptions[index] ?? "",
		skill_md_path: `/skills/${entry.directory}/SKILL.md`,
	}));
	let omitted = 0;
	let text = renderSkillGuidanceText(
		renderedEntries,
		perEntryTruncated,
		uniformlyShortened,
		omitted,
	);
	while (utf8Bytes(text) >= MaxTextBytes && renderedEntries.length > 0) {
		renderedEntries.pop();
		omitted += 1;
		text = renderSkillGuidanceText(
			renderedEntries,
			perEntryTruncated,
			uniformlyShortened,
			omitted,
		);
	}
	return text;
}

function renderSkillGuidanceText(
	entries: readonly Record<string, string>[],
	perEntryTruncated: boolean,
	uniformlyShortened: boolean,
	omitted: number,
): string {
	const lines = [
		"Installed skills (metadata only). Read the referenced SKILL.md files with ordinary tools when a skill applies.",
		...entries.map((entry) => JSON.stringify(entry)),
	];
	if (perEntryTruncated) {
		lines.push("[skill guidance note: per-entry description cap applied]");
	}
	if (uniformlyShortened) {
		lines.push("[skill guidance note: uniform description shortening applied]");
	}
	if (omitted > 0) {
		lines.push(
			`[skill guidance note: end-of-order skill omission applied; omitted=${omitted}]`,
		);
	}
	return lines.join("\n");
}

function largestUniformDescriptionCap(
	descriptions: readonly string[],
	budgetBytes: number,
): number {
	let low = 0;
	let high = descriptions.reduce(
		(maximum, description) => Math.max(maximum, utf8Bytes(description)),
		0,
	);
	while (low < high) {
		const candidate = Math.ceil((low + high) / 2);
		const total = descriptions.reduce(
			(sum, description) =>
				sum + utf8Bytes(truncateUTF8(description, candidate).text),
			0,
		);
		if (total <= budgetBytes) {
			low = candidate;
		} else {
			high = candidate - 1;
		}
	}
	return low;
}

function truncateUTF8(
	value: string,
	maxBytes: number,
): { readonly text: string; readonly truncated: boolean } {
	if (utf8Bytes(value) <= maxBytes) {
		return { text: value, truncated: false };
	}
	if (maxBytes <= 0) {
		return { text: "", truncated: true };
	}
	const marker = maxBytes >= 3 ? "..." : "";
	const contentBudget = maxBytes - utf8Bytes(marker);
	let text = "";
	let usedBytes = 0;
	for (const character of value) {
		const characterBytes = utf8Bytes(character);
		if (usedBytes + characterBytes > contentBudget) {
			break;
		}
		text += character;
		usedBytes += characterBytes;
	}
	return { text: text + marker, truncated: true };
}

function sumUTF8Bytes(values: readonly string[]): number {
	return values.reduce((sum, value) => sum + utf8Bytes(value), 0);
}

function utf8Bytes(value: string): number {
	return new TextEncoder().encode(value).byteLength;
}

function compareStrings(left: string, right: string): number {
	return left < right ? -1 : left > right ? 1 : 0;
}

function positiveInteger(value: number): boolean {
	return Number.isInteger(value) && value > 0;
}
