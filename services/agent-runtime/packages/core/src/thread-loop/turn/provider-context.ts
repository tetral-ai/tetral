/** Committed message normalization from the reducer's durable retention decision. */
import type { RuntimeContextEntry } from "../../contracts/runtime.js";
import type { ThreadTurnCheckpoint } from "./checkpoint.js";
import { extractNewestRequest, ThreadTurnLoadFactsSchema } from "./load.js";
import type { ThreadTurnLoadFacts } from "./load.js";
export function normalizeRequestMessages(input: {
	readonly messages: readonly RuntimeContextEntry[];
	readonly checkpoint: ThreadTurnCheckpoint;
	readonly currentRequestMessage?: {
		readonly modelRequestId: string;
		readonly assistantMessageSequence: number;
	};
}): readonly RuntimeContextEntry[] {
	const request = input.checkpoint.request;
	const retention = request?.requestEnd?.providerContextRetention;
	if (request === undefined || retention === undefined || retention.disposition === "completed" || retention.disposition === "compacted")
		return input.messages;
	const sequence = retention.assistantMessageSequence ?? (input.currentRequestMessage?.modelRequestId === request.modelRequestId ? input.currentRequestMessage.assistantMessageSequence : undefined);
	if (sequence === undefined) {
		if (request.toolMembers.length > 0)
			throw new Error("retained Tool members have no committed Assistant");
		return input.messages;
	}
	const message = input.messages.find(candidate => candidate.messageSequence === sequence);
	if (message === undefined && retention.toolUseEventIds.length === 0 && retention.repairEventIds.length === 0)
		return input.messages;
	if (message?.contextKind !== "assistant")
		throw new Error("retained request has no committed Assistant owner");
	const tools = new Set(retention.toolUseEventIds), repairs = new Set(retention.repairEventIds);
	const members = request.toolMembers.filter(member => member.memberKind === "public_tool_use" ? tools.has(member.toolUseEventId) : repairs.has(member.repairEventId));
	const calls = new Set(members.map(member => member.modelToolCallId));
	const reasoning = new Set<number>();
	for (let index = 0; index < message.parts.length; index++) {
		const part = message.parts[index];
		if (part?.type !== "tool_call" || !calls.has(part.modelToolCallId))
			continue;
		for (let prefix = index - 1; prefix >= 0; prefix--) {
			const preceding = message.parts[prefix];
			if (preceding?.type !== "reasoning")
				break;
			reasoning.add(prefix);
		}
	}
	const parts = message.parts.filter((part, index) => part.type === "reasoning" ? reasoning.has(index) : (part.type === "tool_call" || part.type === "tool_result") && calls.has(part.modelToolCallId));
	return input.messages.flatMap(candidate => candidate.messageSequence !== sequence ? [
		candidate
	] : parts.length === 0 ? [] : [
		{
			...candidate, parts
		}
	]);
}
export function normalizeLoadedMessages(input: {
	readonly messages: readonly RuntimeContextEntry[];
	readonly facts: ThreadTurnLoadFacts;
}): readonly RuntimeContextEntry[] {
	const facts = ThreadTurnLoadFactsSchema.parse(input.facts);
	const starts = new Map(facts.events.filter(event => event.type === "span.model_request_start").map(event => [
		event.modelRequestId!, event
	]));
	let messages = input.messages;
	for (const end of facts.events) {
		if (end.type !== "span.model_request_end")
			continue;
		const start = starts.get(end.modelRequestId!);
		if (start === undefined)
			throw new Error("Request End has no Request Start");
		messages = normalizeRequestMessages({
			messages, checkpoint: {
				pendingInputContextSequences: [], request: extractNewestRequest(start, end, facts.events, facts.internalRepairs)
			}
		});
	}
	return messages;
}
