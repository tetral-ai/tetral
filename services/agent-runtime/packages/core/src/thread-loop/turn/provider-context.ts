/**
 * @packageDocumentation
 * Provider-history projection for durable turn outcomes. This module selects
 * the exact retained Assistant and terminal Tool members; lifecycle derivation
 * remains in load/reducer and hot message ownership remains in ContextManager.
 */

import type {
	RuntimeContextEntry,
	RuntimeOpenRequestDraft,
} from "../../contracts/runtime.js";
import {
	RuntimeContextEntrySchema,
	RuntimeOpenRequestDraftSchema,
} from "../../contracts/runtime.js";
import type { ThreadTurnCheckpoint } from "./checkpoint.js";
import { parseThreadTurnCheckpoint } from "./checkpoint.js";
import {
	extractNewestRequest,
	ThreadTurnLoadFactsSchema,
} from "./load.js";
import type { ThreadTurnLoadFacts } from "./load.js";

/**
 * Converts the durable Assistant projection of one failed Request into the
 * Runtime-owned provider view. Text from the failed attempt is never replayed;
 * exact terminal Tool Call/Result pairs and their signed reasoning remain
 * provider-visible, while a nonterminal Tool Call stays in the private open
 * draft until its existing settlement route completes.
 */
export function projectFailedRequestProviderContext(input: {
	readonly contextEntries: readonly RuntimeContextEntry[];
	readonly openRequestDraft?: RuntimeOpenRequestDraft | undefined;
	readonly checkpoint: ThreadTurnCheckpoint;
}): {
	readonly contextEntries: readonly RuntimeContextEntry[];
	readonly openRequestDraft?: RuntimeOpenRequestDraft | undefined;
} {
	const checkpoint = parseThreadTurnCheckpoint(input.checkpoint);
	const request = checkpoint.request;
	if (
		request?.requestEnd === undefined ||
		request.requestEnd.providerContextRetention.disposition === "completed" ||
		request.requestEnd.providerContextRetention.disposition === "compacted"
	) {
		return {
			contextEntries: input.contextEntries,
			...(input.openRequestDraft === undefined
				? {}
				: { openRequestDraft: input.openRequestDraft }),
		};
	}
	const sequence =
		request.requestEnd.providerContextRetention.assistantMessageSequence ??
		(input.openRequestDraft?.modelRequestId === request.modelRequestId
			? input.openRequestDraft.messageSequence
			: undefined);
	if (sequence === undefined) {
		return {
			contextEntries: input.contextEntries,
			...(input.openRequestDraft === undefined
				? {}
				: { openRequestDraft: input.openRequestDraft }),
		};
	}
	const sealed = input.contextEntries.find(
		(entry) =>
			entry.contextKind === "assistant" && entry.messageSequence === sequence,
	);
	const open =
		input.openRequestDraft?.messageSequence === sequence
			? RuntimeOpenRequestDraftSchema.parse(input.openRequestDraft)
			: undefined;
	const unrelatedOpenRequestDraft =
		open === undefined ? input.openRequestDraft : undefined;
	if (sealed === undefined && open === undefined) {
		if (request.toolMembers.length !== 0) {
			throw new Error(
				"failed Request Tool projection has no resident Assistant owner",
			);
		}
		return {
			contextEntries: input.contextEntries,
			...(input.openRequestDraft === undefined
				? {}
				: { openRequestDraft: input.openRequestDraft }),
		};
	}
	if (sealed !== undefined && open !== undefined) {
		throw new Error(
			"failed Request Assistant projection must have exactly one resident owner",
		);
	}
	if (open !== undefined && open.modelRequestId !== request.modelRequestId) {
		throw new Error("failed Request draft identity does not match its checkpoint");
	}
	const retainedToolUseIds = new Set(
		request.requestEnd.providerContextRetention.toolUseEventIds,
	);
	const retainedRepairIds = new Set(
		request.requestEnd.providerContextRetention.repairEventIds,
	);
	const retainedMembers = request.toolMembers.filter((member) =>
		member.memberKind === "public_tool_use"
			? retainedToolUseIds.has(member.toolUseEventId)
			: retainedRepairIds.has(member.repairEventId),
	);
	const memberIds = new Set(
		retainedMembers.map((member) => member.modelToolCallId),
	);
	const terminalIds = new Set(
		retainedMembers.flatMap((member) =>
			member.memberKind === "internal_tool_repair" ||
			member.terminalResult !== undefined
				? [member.modelToolCallId]
				: [],
		),
	);
	const incomplete = retainedMembers.some(
		(member) =>
			member.memberKind === "public_tool_use" &&
			member.terminalResult === undefined,
	);
	const sourceParts = sealed?.parts ?? open!.parts;
	const retainedReasoningIndexes = new Set<number>();
	for (let index = 0; index < sourceParts.length; index += 1) {
		const part = sourceParts[index];
		if (
			part?.type !== "tool_call" ||
			!memberIds.has(part.modelToolCallId)
		) {
			continue;
		}
		for (let ownerIndex = index - 1; ownerIndex >= 0; ownerIndex -= 1) {
			const owner = sourceParts[ownerIndex];
			if (owner?.type === "text" || owner?.type === "tool_result") {
				break;
			}
			if (owner?.type === "reasoning") {
				retainedReasoningIndexes.add(ownerIndex);
			}
		}
	}
	const retainedParts = sourceParts.filter((part, index) => {
		switch (part.type) {
		case "reasoning":
			return retainedReasoningIndexes.has(index);
			case "tool_call":
				return memberIds.has(part.modelToolCallId);
			case "tool_result":
				return terminalIds.has(part.modelToolCallId);
			case "text":
				return false;
		}
	});
	const contextEntries = input.contextEntries.filter(
		(entry) => entry.messageSequence !== sequence,
	);
	if (incomplete) {
		if (unrelatedOpenRequestDraft !== undefined) {
			throw new Error("failed Request cannot replace an unrelated open draft");
		}
		return {
			contextEntries,
			openRequestDraft: RuntimeOpenRequestDraftSchema.parse({
				modelRequestId: request.modelRequestId,
				messageSequence: sequence,
				parts: retainedParts,
			}),
		};
	}
	if (retainedParts.length === 0) {
		return {
			contextEntries,
			...(unrelatedOpenRequestDraft === undefined
				? {}
				: { openRequestDraft: unrelatedOpenRequestDraft }),
		};
	}
	return {
		contextEntries: [
			...contextEntries,
			RuntimeContextEntrySchema.parse({
				messageSequence: sequence,
				contextKind: "assistant",
				parts: retainedParts,
			}),
		].sort((left, right) => left.messageSequence - right.messageSequence),
		...(unrelatedOpenRequestDraft === undefined
			? {}
			: { openRequestDraft: unrelatedOpenRequestDraft }),
	};
}

/** Applies failed-request eligibility to every durable Request represented by LoadContext facts. */
export function projectFailedRequestsProviderContext(input: {
	readonly contextEntries: readonly RuntimeContextEntry[];
	readonly openRequestDraft?: RuntimeOpenRequestDraft | undefined;
	readonly facts: ThreadTurnLoadFacts;
}): {
	readonly contextEntries: readonly RuntimeContextEntry[];
	readonly openRequestDraft?: RuntimeOpenRequestDraft | undefined;
} {
	const facts = ThreadTurnLoadFactsSchema.parse(input.facts);
	const starts = new Map(
		facts.events
			.filter((event) => event.type === "span.model_request_start")
			.map((event) => [event.modelRequestId!, event] as const),
	);
	let projected: {
		readonly contextEntries: readonly RuntimeContextEntry[];
		readonly openRequestDraft?: RuntimeOpenRequestDraft | undefined;
	} = {
		contextEntries: input.contextEntries,
		...(input.openRequestDraft === undefined
			? {}
			: { openRequestDraft: input.openRequestDraft }),
	};
	for (const end of facts.events) {
		if (
			end.type !== "span.model_request_end" ||
			end.requestEnd!.providerContextRetention.disposition === "completed" ||
			end.requestEnd!.providerContextRetention.disposition === "compacted"
		) {
			continue;
		}
		const start = starts.get(end.modelRequestId!);
		if (start === undefined) {
			throw new Error("failed Request End has no matching Request Start");
		}
		projected = projectFailedRequestProviderContext({
			contextEntries: projected.contextEntries,
			...(projected.openRequestDraft === undefined
				? {}
				: { openRequestDraft: projected.openRequestDraft }),
			checkpoint: {
				pendingInputContextSequences: [],
				request: extractNewestRequest(
					start,
					end,
					facts.events,
					facts.internalRepairs,
				),
			},
		});
	}
	return projected;
}
