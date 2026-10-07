/**
 * Applies operation-specific Bridge results to Runtime-owned context. Every
 * applicator combines an immutable request/draft with only Bridge-generated
 * facts; no generic result base, database stamp, or caller-payload echo is used.
 */
import type {
	RuntimeAssistantContextAppend,
	RuntimeAssistantDraftPart,
	RuntimeContextEntry,
	RuntimeContextKind,
	RuntimeContextPart,
	RuntimeInterruptToolResult,
	RuntimeJsonValue,
	RuntimeToolSettlementDeclaration,
} from "../contracts/runtime.js";
import {
	finalizeRuntimeToolOutput,
	RuntimeAssistantContextAppendSchema,
	RuntimeContextEntrySchema,
	runtimeToolErrorFromFailure,
} from "../contracts/runtime.js";
import type {
	RuntimePendingApprovalToolJobState,
} from "../thread-loop/thread-state.js";
import type {
	RuntimeAcceptedInputState,
} from "../thread-loop/input/accepted-input.js";
import { stableRuntimeID } from "./runtime-identity.js";

export interface RuntimeContextDraft {
	readonly contextKind: RuntimeContextKind;
	readonly parts: readonly RuntimeContextPart[];
}

export interface AssistantAppendResult {
	readonly messageSequence: number;
	readonly createdToolUseEventIds: readonly string[];
}

export function runtimeTurnOpenWriteId(input: {
	readonly workspaceId: string;
	readonly sessionId: string;
	readonly sessionThreadId: string;
	readonly openingSourceKind: string;
	readonly openingSourceId: string;
}): string {
	return stableRuntimeID(
		"turn_open",
		input.workspaceId,
		input.sessionId,
		input.sessionThreadId,
		input.openingSourceKind,
		input.openingSourceId,
	);
}

export function acceptedInputDeclarationKind(
	input: RuntimeAcceptedInputState,
): string {
	return input.kind === "inter_agent_message" ? "agent_mail" : input.kind;
}

export function acceptedInputContextDrafts(
	input: RuntimeAcceptedInputState,
): readonly RuntimeContextDraft[] {
	switch (input.kind) {
		case "task_notification":
			return [textDraft("runtime_notification", input.notificationJson)];
		case "rejection":
			return [
				textDraft(
					"assistant",
					"The session runtime could not accept this input.",
				),
			];
		case "inter_agent_message":
			return [textDraft("user", input.content)];
		case "approval_review":
			return input.promptText.map((text) => textDraft("user", text));
		case "messages":
			return parseAcceptedMessageContent(input.contentJson);
	}
}

export function applyAcceptedInputResult(
	drafts: readonly RuntimeContextDraft[],
	assignedContextSequences: readonly number[],
): readonly RuntimeContextEntry[] {
	if (drafts.length !== assignedContextSequences.length) {
		throw new Error(
			"accepted input assigned sequence count does not match its context drafts",
		);
	}
	return drafts.map((draft, index) =>
		RuntimeContextEntrySchema.parse({
			messageSequence: assignedContextSequences[index],
			contextKind: draft.contextKind,
			parts: draft.parts,
		}),
	);
}

export function toolConfirmationContext(input: {
	readonly toolUseEventId: string;
	readonly pendingTool: RuntimePendingApprovalToolJobState;
	readonly decision: "allow" | "deny";
	readonly denyMessage?: string | undefined;
}): RuntimeContextDraft {
	if (
		input.pendingTool.toolUseEventId !== input.toolUseEventId ||
		input.pendingTool.toolPart.toolUseEventId !== input.toolUseEventId
	) {
		throw new Error("tool confirmation does not identify the pending tool");
	}
	const text =
		input.decision === "allow"
			? "Approval allowed"
			: input.denyMessage === undefined || input.denyMessage.length === 0
				? "Approval denied"
				: `Approval denied: ${input.denyMessage}`;
	return textDraft("user", text);
}

export function taskNotificationContext(
	payloadJson: string,
): RuntimeContextDraft {
	return textDraft("runtime_notification", payloadJson);
}

export function completionMailText(envelope: string): string {
	return envelope;
}

export function compactionContext(text: string): RuntimeContextDraft {
	return textDraft("compaction", text);
}

export function taskNotificationOperationId(
	runtimeInputId: string,
	taskId: string,
): string {
	return stableRuntimeID("task_notification", runtimeInputId, taskId);
}

export function assistantAppendFromDraftParts(
	parts: readonly RuntimeAssistantDraftPart[],
): RuntimeAssistantContextAppend {
	return RuntimeAssistantContextAppendSchema.parse({ parts });
}

/**
 * Applies an acknowledged Assistant append to the committed Assistant message.
 * It checks only the committed message sequence; ThreadState's
 * associateCurrentRequestMessage owns the model-request identity of that message.
 */
export function applyAssistantAppendResult(input: {
	readonly append: RuntimeAssistantContextAppend;
	readonly existingMessage?: RuntimeContextEntry | undefined;
	readonly result: AssistantAppendResult;
}): {
	readonly message: RuntimeContextEntry;
	readonly activeToolParts: readonly Extract<
		RuntimeAssistantDraftPart,
		{ readonly type: "tool" }
	>[];
} {
	if (
		input.existingMessage !== undefined &&
		(input.existingMessage.messageSequence !== input.result.messageSequence)
	) {
		throw new Error("Assistant append changed the committed message identity");
	}
	const callParts = input.append.parts.filter(
		(
			part,
		): part is Extract<RuntimeAssistantDraftPart, { readonly type: "tool" }> =>
			part.type === "tool",
	);
	if (callParts.length !== input.result.createdToolUseEventIds.length) {
		throw new Error("Assistant append Tool identity count does not match");
	}
	let toolIndex = 0;
	const activeToolParts: Extract<
		RuntimeAssistantDraftPart,
		{ readonly type: "tool" }
	>[] = [];
	const contextParts = input.append.parts.map((part): RuntimeContextPart => {
		if (part.type === "text") return { type: "text", text: part.text };
		if (part.type === "reasoning") {
			return {
				type: "reasoning",
				text: part.text,
				...(part.providerMetadata === undefined
					? {}
					: { providerMetadata: part.providerMetadata }),
			};
		}
		const toolUseEventId = input.result.createdToolUseEventIds[toolIndex++];
		if (
			toolUseEventId === undefined ||
			!("input" in part.state) ||
			part.state.input === undefined
		) {
			throw new Error(
				"Assistant Tool call lacks its durable identity or canonical input",
			);
		}
		activeToolParts.push({ ...part, toolUseEventId });
		return {
			type: "tool_call",
			modelToolCallId: part.modelToolCallId,
			toolName: part.toolName,
			canonicalInput: part.state.input.value,
		};
	});
	const message = RuntimeContextEntrySchema.parse({
		contextKind: "assistant",
		messageSequence: input.result.messageSequence,
		parts: [...(input.existingMessage?.parts ?? []), ...contextParts],
	});
	return { message, activeToolParts };
}


export function applyToolSettlementToContext(input: {
	readonly entries: readonly RuntimeContextEntry[];
	readonly assistantMessageSequence: number;
	readonly modelToolCallId: string;
	readonly settlement: RuntimeToolSettlementDeclaration["outcome"];
}): readonly RuntimeContextEntry[] {
	let matched = false;
	const entries = input.entries.map((entry) => {
		if (entry.messageSequence !== input.assistantMessageSequence) return entry;
		if (entry.contextKind !== "assistant")
			throw new Error("Tool settlement target is not Assistant context");
		const callCount = entry.parts.filter(
			(part) =>
				part.type === "tool_call" &&
				part.modelToolCallId === input.modelToolCallId,
		).length;
		const resultCount = entry.parts.filter(
			(part) =>
				part.type === "tool_result" &&
				part.modelToolCallId === input.modelToolCallId,
		).length;
		if (callCount !== 1 || resultCount !== 0)
			throw new Error("Tool settlement target is missing or already terminal");
		matched = true;
		return RuntimeContextEntrySchema.parse({
			...entry,
			parts: [
				...entry.parts,
				contextToolResultFromSettlement(
					input.modelToolCallId,
					input.settlement,
				),
			],
		});
	});
	if (!matched)
		throw new Error("Tool settlement must name one Assistant context entry");
	return entries;
}


export function applyInterruptToolResults(input: {
	readonly entries: readonly RuntimeContextEntry[];
	readonly routes: readonly {
		readonly toolUseEventId: string;
		readonly assistantMessageSequence: number;
		readonly modelToolCallId: string;
	}[];
	readonly results: readonly RuntimeInterruptToolResult[];
}): readonly RuntimeContextEntry[] {
	let entries = input.entries;
	for (const result of input.results) {
		const route = input.routes.find(
			(candidate) => candidate.toolUseEventId === result.toolUseEventId,
		);
		if (route === undefined)
			throw new Error("interrupt Tool result has no active route");
		entries = appendToolResultPart(entries, route.assistantMessageSequence, {
			type: "tool_result",
			modelToolCallId: route.modelToolCallId,
			result: result.result,
		});
	}
	return entries;
}

function appendToolResultPart(
	entries: readonly RuntimeContextEntry[],
	assistantMessageSequence: number,
	resultPart: Extract<RuntimeContextPart, { readonly type: "tool_result" }>,
): readonly RuntimeContextEntry[] {
	let matched = false;
	const updated = entries.map((entry) => {
		if (entry.messageSequence !== assistantMessageSequence) return entry;
		if (entry.contextKind !== "assistant")
			throw new Error("Tool result target is not Assistant context");
		const callCount = entry.parts.filter(
			(part) =>
				part.type === "tool_call" &&
				part.modelToolCallId === resultPart.modelToolCallId,
		).length;
		const resultCount = entry.parts.filter(
			(part) =>
				part.type === "tool_result" &&
				part.modelToolCallId === resultPart.modelToolCallId,
		).length;
		if (callCount !== 1 || resultCount !== 0)
			throw new Error("Tool result target is missing or already terminal");
		matched = true;
		return RuntimeContextEntrySchema.parse({
			...entry,
			parts: [...entry.parts, resultPart],
		});
	});
	if (!matched)
		throw new Error("Tool result must name one Assistant context entry");
	return updated;
}

export function appendToolResultToContext(
	entries: readonly RuntimeContextEntry[],
	assistantMessageSequence: number,
	resultPart: Extract<RuntimeContextPart, { readonly type: "tool_result" }>,
): readonly RuntimeContextEntry[] {
	return appendToolResultPart(entries, assistantMessageSequence, resultPart);
}

export function internalToolRepairContext(input: {
	readonly modelToolCallId: string;
	readonly toolName: string;
	readonly canonicalInput: RuntimeJsonValue;
	readonly error: Parameters<typeof runtimeToolErrorFromFailure>[0];
}): RuntimeContextDraft {
	return {
		contextKind: "assistant",
		parts: [
			{
				type: "tool_call",
				modelToolCallId: input.modelToolCallId,
				toolName: input.toolName,
				canonicalInput: input.canonicalInput,
			},
			{
				type: "tool_result",
				modelToolCallId: input.modelToolCallId,
				result: {
					type: "error",
					error: runtimeToolErrorFromFailure(input.error),
				},
			},
		],
	};
}

/**
 * Applies an acknowledged internal Tool repair to the committed Assistant
 * message. It checks only the committed message sequence; ThreadState's
 * associateCurrentRequestMessage owns the model-request identity of that message.
 */
export function applyInternalToolRepairResult(input: {
	readonly existingMessage?: RuntimeContextEntry | undefined;
	readonly assignedMessageSequence: number;
	readonly context: RuntimeContextDraft;
	readonly reasoningPrefixContextDelta?: RuntimeAssistantContextAppend | undefined;
}): RuntimeContextEntry {
	if (input.context.contextKind !== "assistant") {
		throw new Error("internal Tool repair context must be Assistant context");
	}
	if (
		input.existingMessage !== undefined &&
		(input.existingMessage.messageSequence !== input.assignedMessageSequence)
	) {
		throw new Error(
			"internal Tool repair changed the committed message identity",
		);
	}
	return RuntimeContextEntrySchema.parse({
		contextKind: "assistant",
		messageSequence: input.assignedMessageSequence,
		parts: [...(input.existingMessage?.parts ?? []), ...(input.reasoningPrefixContextDelta?.parts.map((part): RuntimeContextPart => {
      if (part.type !== "reasoning") throw new Error("internal repair prefix must contain reasoning only");
      return {type:"reasoning",text:part.text,...(part.providerMetadata === undefined ? {} : {providerMetadata:part.providerMetadata})};
    }) ?? []), ...input.context.parts],
	});
}

function parseAcceptedMessageContent(
	contentJson: string,
): readonly RuntimeContextDraft[] {
	const parsed = JSON.parse(contentJson) as unknown;
	if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
		throw new Error("accepted input payload must be a JSON object");
	}
	const content = parsed as { readonly messages?: unknown };
	if (!Array.isArray(content.messages))
		throw new Error("accepted input payload has no content");
	return content.messages.flatMap((message): RuntimeContextDraft[] => {
		if (
			typeof message !== "object" ||
			message === null ||
			Array.isArray(message)
		) {
			throw new Error("accepted input message is invalid");
		}
		const value = message as { readonly parts?: unknown };
		if (!Array.isArray(value.parts))
			throw new Error("accepted input message has no parts");
		const parts: RuntimeContextPart[] = value.parts.flatMap(
			(part): RuntimeContextPart[] => {
				if (typeof part !== "object" || part === null || Array.isArray(part))
					return [];
				const value = part as {
					readonly type?: unknown;
					readonly text?: unknown;
				};
				return value.type === "text" && typeof value.text === "string"
					? [{ type: "text", text: value.text }]
					: [];
			},
		);
		return parts.length === 0
			? []
			: [{ contextKind: "user" as const, parts }];
	});
}

function textDraft(
	contextKind: RuntimeContextKind,
	text: string,
): RuntimeContextDraft {
	return { contextKind, parts: [{ type: "text", text }] };
}

export function contextToolResultFromSettlement(
	modelToolCallId: string,
	settlement: RuntimeToolSettlementDeclaration["outcome"],
): Extract<RuntimeContextPart, { readonly type: "tool_result" }> {
	if (settlement.type === "completed") {
		const output = finalizeRuntimeToolOutput(settlement.output);
		return {
			type: "tool_result",
			modelToolCallId,
			result: { type: "completed", output: { text: output.text } },
		};
	}
	if (settlement.type === "error") {
		return {
			type: "tool_result",
			modelToolCallId,
			result: {
				type: "error",
				error: runtimeToolErrorFromFailure(settlement.error),
			},
		};
	}
	return {
		type: "tool_result",
		modelToolCallId,
		result: { type: "cancelled" },
	};
}
