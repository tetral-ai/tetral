import { z } from "zod/v4";
import type {
	RuntimeContextEntry,
	RuntimeOpenRequestDraft,
} from "../contracts/runtime.js";
import {
	RuntimeContextEntrySchema,
	RuntimeOpenRequestDraftSchema,
} from "../contracts/runtime.js";

const DurableIdentitySchema = z.string().min(1);

const TerminalResultSchema = z.strictObject({
	outcome: z.enum(["success", "error", "cancelled", "unknown"]),
});

const PublicToolUseMemberSchema = z.strictObject({
	memberKind: z.literal("public_tool_use"),
	modelToolCallId: DurableIdentitySchema,
	toolUseEventId: DurableIdentitySchema,
	toolName: DurableIdentitySchema,
	terminalResult: TerminalResultSchema.optional(),
});

const InternalToolRepairMemberSchema = z.strictObject({
	memberKind: z.literal("internal_tool_repair"),
	modelToolCallId: DurableIdentitySchema,
	toolName: DurableIdentitySchema,
	repairEventId: DurableIdentitySchema,
	outcome: z.literal("error"),
});

const ProviderContextRetentionSchema = z.strictObject({
	disposition: z.enum([
		"completed",
		"interrupted",
		"rescheduled",
		"failed",
		"compacted",
	]),
	assistantMessageSequence: z
		.number()
		.int()
		.positive()
		.max(Number.MAX_SAFE_INTEGER)
		.optional(),
	toolUseEventIds: z.array(DurableIdentitySchema).max(128),
	repairEventIds: z.array(DurableIdentitySchema).max(128),
});

const RequestSchema = z
	.strictObject({
		modelRequestId: DurableIdentitySchema,
		requestStartEventId: DurableIdentitySchema,
		requestKind: z.enum([
			"agent_provider_request",
			"compaction_summary",
			"approval_reviewer",
		]),
		contextThroughMessageSequence: z
			.number()
			.int()
			.nonnegative()
			.max(Number.MAX_SAFE_INTEGER),
		requestEnd: z
			.strictObject({
				eventId: DurableIdentitySchema,
				isError: z.boolean(),
				errorKind: DurableIdentitySchema.optional(),
				providerContextRetention: ProviderContextRetentionSchema,
				reschedule: z
					.strictObject({
						attempt: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),
						effectiveDeadline: z.string().datetime({ offset: true }),
						providerAttempts: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
						compactionAttempts: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
					})
					.optional(),
			})
			.optional(),
		toolMembers: z.array(
			z.discriminatedUnion("memberKind", [
				PublicToolUseMemberSchema,
				InternalToolRepairMemberSchema,
			]),
		),
	})
	.superRefine((request, context) => {
		const modelToolCallIds = new Set<string>();
		const toolUseEventIds = new Set<string>();

		for (const member of request.toolMembers) {
			if (modelToolCallIds.has(member.modelToolCallId)) {
				context.addIssue({
					code: "custom",
					message: "modelToolCallId must be unique within a request",
					path: ["toolMembers"],
				});
			}
			modelToolCallIds.add(member.modelToolCallId);

			if (member.memberKind === "internal_tool_repair") {
				continue;
			}
			if (toolUseEventIds.has(member.toolUseEventId)) {
				context.addIssue({
					code: "custom",
					message: "toolUseEventId must be unique within a request",
					path: ["toolMembers"],
				});
			}
			toolUseEventIds.add(member.toolUseEventId);
		}
	});

export const ThreadTurnCheckpointSchema = z
	.strictObject({
		executionRunId: DurableIdentitySchema.optional(),
		pendingInputContextSequences: z.array(
			z.number().int().positive().max(Number.MAX_SAFE_INTEGER),
		),
		request: RequestSchema.optional(),
		interruptEventId: DurableIdentitySchema.optional(),
		terminalCloseout: z
			.strictObject({
				failureEventId: DurableIdentitySchema,
				closeoutEventId: DurableIdentitySchema,
				disposition: z.enum(["retries_exhausted", "terminated"]),
			})
			.optional(),
		idleCloseout: z
			.strictObject({
				eventId: DurableIdentitySchema,
				stopReason: DurableIdentitySchema,
			})
			.optional(),
	})
	.superRefine((checkpoint, context) => {
		if (
			new Set(checkpoint.pendingInputContextSequences).size !==
			checkpoint.pendingInputContextSequences.length
		) {
			context.addIssue({
				code: "custom",
				message: "pendingInputContextSequences must be unique",
				path: ["pendingInputContextSequences"],
			});
		}
		if (checkpoint.idleCloseout?.stopReason === "requires_action") {
			const request = checkpoint.request;
			const hasIncompletePublicTool =
				request?.requestEnd !== undefined &&
				request.toolMembers.some(
					(member) =>
						member.memberKind === "public_tool_use" &&
						member.terminalResult === undefined,
				);
			if (!hasIncompletePublicTool) {
				context.addIssue({
					code: "custom",
					message:
						"requires_action closeout must retain a sealed incomplete Tool Use",
					path: ["idleCloseout"],
				});
			}
		}
	});

const ThreadToolRouteSchema = z.strictObject({
	toolUseEventId: DurableIdentitySchema,
	disposition: z.enum([
		"hot_execution",
		"requires_user_action",
		"resume_approval_settlement",
		"resume_sandbox_execution",
	]),
});

export const ThreadToolRouteViewSchema = z
	.strictObject({
		routes: z.array(ThreadToolRouteSchema),
	})
	.superRefine((view, context) => {
		const toolUseEventIds = new Set<string>();
		for (const route of view.routes) {
			if (toolUseEventIds.has(route.toolUseEventId)) {
				context.addIssue({
					code: "custom",
					message: "tool route ownership must be unique",
					path: ["routes"],
				});
			}
			toolUseEventIds.add(route.toolUseEventId);
		}
	});

export interface ThreadTurnCheckpoint {
	readonly executionRunId?: string;
	readonly pendingInputContextSequences: readonly number[];
	readonly request?: {
		readonly modelRequestId: string;
		readonly requestStartEventId: string;
		readonly requestKind:
			| "agent_provider_request"
			| "compaction_summary"
			| "approval_reviewer";
		readonly contextThroughMessageSequence: number;
		readonly requestEnd?: {
			readonly eventId: string;
			readonly isError: boolean;
			readonly errorKind?: string;
			readonly providerContextRetention: z.infer<
				typeof ProviderContextRetentionSchema
			>;
			readonly reschedule?: {
				readonly attempt: number;
				readonly effectiveDeadline: string;
				readonly providerAttempts: number;
				readonly compactionAttempts: number;
			};
		};
		readonly toolMembers: readonly (
			| {
					readonly memberKind: "public_tool_use";
					readonly modelToolCallId: string;
					readonly toolUseEventId: string;
					readonly toolName: string;
					readonly terminalResult?: {
						readonly outcome: "success" | "error" | "cancelled" | "unknown";
					};
			  }
			| {
					readonly memberKind: "internal_tool_repair";
					readonly modelToolCallId: string;
					readonly toolName: string;
					readonly repairEventId: string;
					readonly outcome: "error";
			  }
		)[];
	};
	readonly interruptEventId?: string;
	readonly terminalCloseout?: {
		readonly failureEventId: string;
		readonly closeoutEventId: string;
		readonly disposition: "retries_exhausted" | "terminated";
	};
	readonly idleCloseout?: {
		readonly eventId: string;
		readonly stopReason: string;
	};
}

export interface ThreadToolRouteView {
	readonly routes: readonly {
		readonly toolUseEventId: string;
		readonly disposition:
			| "hot_execution"
			| "requires_user_action"
			| "resume_approval_settlement"
			| "resume_sandbox_execution";
	}[];
}

export function parseThreadTurnCheckpoint(
	input: unknown,
): ThreadTurnCheckpoint {
	const parsed = ThreadTurnCheckpointSchema.parse(input);
	return {
		...(parsed.executionRunId !== undefined
			? { executionRunId: parsed.executionRunId }
			: {}),
		pendingInputContextSequences: parsed.pendingInputContextSequences,
		...(parsed.request !== undefined
			? {
					request: {
						modelRequestId: parsed.request.modelRequestId,
						requestStartEventId: parsed.request.requestStartEventId,
						requestKind: parsed.request.requestKind,
						contextThroughMessageSequence:
							parsed.request.contextThroughMessageSequence,
						...(parsed.request.requestEnd !== undefined
							? {
									requestEnd: {
										eventId: parsed.request.requestEnd.eventId,
										isError: parsed.request.requestEnd.isError,
										...(parsed.request.requestEnd.errorKind !== undefined
											? { errorKind: parsed.request.requestEnd.errorKind }
											: {}),
										providerContextRetention:
											parsed.request.requestEnd.providerContextRetention,
										...(parsed.request.requestEnd.reschedule !== undefined
											? { reschedule: parsed.request.requestEnd.reschedule }
											: {}),
									},
								}
							: {}),
						toolMembers: parsed.request.toolMembers.map((member) => {
							if (member.memberKind === "internal_tool_repair") {
								return member;
							}
							return {
								memberKind: member.memberKind,
								modelToolCallId: member.modelToolCallId,
								toolUseEventId: member.toolUseEventId,
								toolName: member.toolName,
								...(member.terminalResult !== undefined
									? { terminalResult: member.terminalResult }
									: {}),
							};
						}),
					},
				}
			: {}),
		...(parsed.interruptEventId !== undefined
			? { interruptEventId: parsed.interruptEventId }
			: {}),
		...(parsed.terminalCloseout !== undefined
			? { terminalCloseout: parsed.terminalCloseout }
			: {}),
		...(parsed.idleCloseout !== undefined
			? { idleCloseout: parsed.idleCloseout }
			: {}),
	};
}

export function parseThreadToolRouteView(input: unknown): ThreadToolRouteView {
	return ThreadToolRouteViewSchema.parse(input);
}
const IdentitySchema = z.string().min(1);
const SequenceSchema = z.number().int().positive().max(Number.MAX_SAFE_INTEGER);
const MessageSequenceSchema = z
	.number()
	.int()
	.positive()
	.max(Number.MAX_SAFE_INTEGER);
const RequestKindSchema = z.enum([
	"agent_provider_request",
	"compaction_summary",
	"approval_reviewer",
]);

const RequestStartFactSchema = z.strictObject({
	requestKind: RequestKindSchema,
	contextThroughMessageSequence: z
		.number()
		.int()
		.nonnegative()
		.max(Number.MAX_SAFE_INTEGER),
});

const RequestEndFactSchema = z.strictObject({
	requestStartEventId: IdentitySchema,
	isError: z.boolean(),
	errorKind: IdentitySchema.optional(),
	providerContextRetention: ProviderContextRetentionSchema,
	reschedule: z
		.strictObject({
			attempt: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),
			effectiveDeadline: z.string().datetime({ offset: true }),
			providerAttempts: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
			compactionAttempts: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
		})
		.optional(),
});

const ToolUseFactSchema = z.strictObject({
	modelToolCallId: IdentitySchema,
	toolName: IdentitySchema,
});

const OrdinaryToolResultFactSchema = z.strictObject({
	modelToolCallId: IdentitySchema,
	toolName: IdentitySchema,
	outcome: z.enum(["completed", "error", "cancelled"]),
});

const InternalToolRepairFactSchema = z.strictObject({
	repairKey: IdentitySchema,
});

const InternalRepairReferenceSchema = z.strictObject({
	repairKey: IdentitySchema,
	repairEventId: IdentitySchema,
	eventSequence: SequenceSchema,
	modelRequestId: IdentitySchema,
	modelToolCallId: IdentitySchema,
	toolName: IdentitySchema,
});

const TurnEventTypeSchema = z.enum([
	"session.status_running",
	"session.thread_status_running",
	"span.model_request_start",
	"span.model_request_end",
	"agent.tool_use",
	"agent.mcp_tool_use",
	"agent.tool_result",
	"agent.mcp_tool_result",
	"user.interrupt",
	"agent.thread_interrupt_requested",
	"session.error",
	"session.status_rescheduled",
	"session.thread_status_rescheduled",
	"session.status_idle",
	"session.thread_status_idle",
	"session.status_terminated",
	"session.thread_status_terminated",
]);

const ThreadTurnEventFactSchema = z
	.strictObject({
		eventId: IdentitySchema,
		eventSequence: SequenceSchema,
		type: TurnEventTypeSchema,
		modelRequestId: IdentitySchema.optional(),
		requestStart: RequestStartFactSchema.optional(),
		requestEnd: RequestEndFactSchema.optional(),
		toolUse: ToolUseFactSchema.optional(),
		toolResult: z
			.union([OrdinaryToolResultFactSchema, InternalToolRepairFactSchema])
			.optional(),
		idle: z.strictObject({ stopReason: IdentitySchema }).optional(),
		failure: z
			.strictObject({
				errorType: IdentitySchema,
				retryStatus: z.enum(["retrying", "exhausted", "terminal"]),
			})
			.optional(),
	})
	.superRefine((event, context) => {
		const specialized = [
			event.requestStart,
			event.requestEnd,
			event.toolUse,
			event.toolResult,
			event.idle,
			event.failure,
		].filter((value) => value !== undefined).length;
		const requireModelRequest = (): void => {
			if (event.modelRequestId === undefined) {
				context.addIssue({
					code: "custom",
					message: `${event.type} requires modelRequestId`,
				});
			}
		};
		switch (event.type) {
			case "span.model_request_start":
				requireModelRequest();
				if (event.requestStart === undefined || specialized !== 1) {
					context.addIssue({
						code: "custom",
						message: "Request Start fact is malformed",
					});
				}
				break;
			case "span.model_request_end":
				requireModelRequest();
				if (event.requestEnd === undefined || specialized !== 1) {
					context.addIssue({
						code: "custom",
						message: "Request End fact is malformed",
					});
				}
				break;
			case "agent.tool_use":
			case "agent.mcp_tool_use":
				requireModelRequest();
				if (event.toolUse === undefined || specialized !== 1) {
					context.addIssue({
						code: "custom",
						message: "Tool Use fact is malformed",
					});
				}
				break;
			case "agent.tool_result":
			case "agent.mcp_tool_result":
				requireModelRequest();
				if (event.toolResult === undefined || specialized !== 1) {
					context.addIssue({
						code: "custom",
						message: "Tool Result fact is malformed",
					});
				}
				if (
					event.type === "agent.mcp_tool_result" &&
					event.toolResult !== undefined &&
					"repairKey" in event.toolResult
				) {
					context.addIssue({
						code: "custom",
						message: "MCP Tool Result cannot be an internal repair",
					});
				}
				break;
			case "session.status_idle":
			case "session.thread_status_idle":
				if (
					event.modelRequestId !== undefined ||
					event.idle === undefined ||
					specialized !== 1
				) {
					context.addIssue({
						code: "custom",
						message: "idle fact is malformed",
					});
				}
				break;
			case "session.error":
				if (
					event.modelRequestId !== undefined ||
					event.failure === undefined ||
					specialized !== 1
				) {
					context.addIssue({
						code: "custom",
						message: "failure fact is malformed",
					});
				}
				break;
			default:
				if (event.modelRequestId !== undefined || specialized !== 0) {
					context.addIssue({
						code: "custom",
						message: `${event.type} fact carries unsupported members`,
					});
				}
		}
	});

export const ThreadTurnLoadFactsSchema = z
	.strictObject({
		events: z.array(ThreadTurnEventFactSchema),
		internalRepairs: z.array(InternalRepairReferenceSchema),
	})
	.superRefine((facts, context) => {
		const eventIds = new Set<string>();
		let previousSequence = 0;
		for (const event of facts.events) {
			if (
				eventIds.has(event.eventId) ||
				event.eventSequence <= previousSequence
			) {
				context.addIssue({
					code: "custom",
					message: "turn events must have unique database order",
				});
				break;
			}
			eventIds.add(event.eventId);
			previousSequence = event.eventSequence;
		}
		if (
			new Set(facts.internalRepairs.map((repair) => repair.repairKey)).size !==
				facts.internalRepairs.length ||
			new Set(facts.internalRepairs.map((repair) => repair.repairEventId))
				.size !== facts.internalRepairs.length
		) {
			context.addIssue({
				code: "custom",
				message: "internal repair direct references must be unique",
			});
		}
	});

export type ThreadTurnLoadFacts = z.infer<typeof ThreadTurnLoadFactsSchema>;

interface ColdPendingToolUse {
	readonly toolUseEventId: string;
	readonly modelRequestId: string;
	readonly modelToolCallId: string;
	readonly toolName: string;
	readonly status: "pending" | "resolving";
	readonly decision?: "allow" | "deny" | undefined;
}

interface ColdPendingSandboxExecution {
	readonly toolUseEventId: string;
	readonly modelRequestId: string;
	readonly modelToolCallId: string;
	readonly toolName: string;
}

export function extractThreadTurnCheckpoint(input: {
	readonly contextEntries: readonly RuntimeContextEntry[];
	readonly facts: ThreadTurnLoadFacts;
}): ThreadTurnCheckpoint {
	// PostgreSQL order is authoritative within Events and context separately.
	// Direct identities relate Requests and Tools; context sequence alone marks
	// user-side input after the latest provider boundary.
	const facts = ThreadTurnLoadFactsSchema.parse(input.facts);
	const starts = new Map<string, ThreadTurnLoadFacts["events"][number]>();
	const ends = new Map<string, ThreadTurnLoadFacts["events"][number]>();

	for (const event of facts.events) {
		if (event.type === "span.model_request_start") {
			const modelRequestId = event.modelRequestId!;
			if (starts.has(modelRequestId)) {
				throw new Error("model request has duplicate Request Start facts");
			}
			starts.set(modelRequestId, event);
		} else if (event.type === "span.model_request_end") {
			const modelRequestId = event.modelRequestId!;
			const start = starts.get(modelRequestId);
			if (
				start === undefined ||
				event.requestEnd!.requestStartEventId !== start.eventId
			) {
				throw new Error("Request End has no matching Request Start");
			}
			if (ends.has(modelRequestId)) {
				throw new Error("model request has duplicate Request End facts");
			}
			ends.set(modelRequestId, event);
		}
	}

	validateRequestOrdering(facts.events, starts, ends);
	validateRequestMembers(facts.events, starts, ends, facts.internalRepairs);
	const latestRunning = facts.events
		.filter(
			(event) =>
				event.type === "session.status_running" ||
				event.type === "session.thread_status_running",
		)
		.at(-1);
	const closeout = extractRunCloseout(facts.events);
	const latestIdleBeforeRunning =
		latestRunning === undefined
			? undefined
			: facts.events
					.filter(
						(event) =>
							(event.type === "session.status_idle" ||
								event.type === "session.thread_status_idle") &&
							event.eventSequence < latestRunning.eventSequence,
					)
					.at(-1);
	const preservePriorRequest =
		latestIdleBeforeRunning?.idle?.stopReason === "requires_action";
	const latestStart = [...starts.values()].at(-1);
	const candidateStart = [...starts.values()]
		.filter(
			(event) =>
				latestRunning === undefined ||
				preservePriorRequest ||
				event.eventSequence >= latestRunning.eventSequence,
		)
		.at(-1);
	const runIsClosed =
		closeout.terminalCloseout !== undefined ||
		(closeout.idleCloseout !== undefined &&
			closeout.idleCloseout.stopReason !== "requires_action");
	const newestStart = runIsClosed ? undefined : candidateStart;
	const request =
		newestStart === undefined
			? undefined
			: extractNewestRequest(
					newestStart,
					ends.get(newestStart.modelRequestId!),
					facts.events,
					facts.internalRepairs,
				);
	const idleCloseout =
		closeout.idleCloseout?.stopReason === "requires_action" &&
		request?.toolMembers.every(
			(member) =>
				member.memberKind !== "public_tool_use" ||
				member.terminalResult !== undefined,
		)
			? undefined
			: closeout.idleCloseout;
	const boundary =
		latestStart?.requestStart?.contextThroughMessageSequence ?? 0;
	const contextEntries = input.contextEntries.map((entry) =>
		RuntimeContextEntrySchema.parse(entry),
	);
	if (
		new Set(contextEntries.map((entry) => entry.messageSequence)).size !==
		contextEntries.length
	) {
		throw new Error(
			"loaded context entries must have unique durable sequences",
		);
	}
	const highestLoadedMessageSequence = contextEntries.reduce(
		(highest, entry) => Math.max(highest, entry.messageSequence),
		0,
	);
	if (boundary > highestLoadedMessageSequence) {
		throw new Error(
			"Request Start context boundary exceeds the loaded durable context range",
		);
	}
	const pendingInputContextSequences = contextEntries
		.filter(
			(entry) =>
				entry.contextKind !== "assistant" &&
				entry.contextKind !== "compaction" &&
				entry.messageSequence > boundary,
		)
		.sort((left, right) => left.messageSequence - right.messageSequence)
		.map((entry) => entry.messageSequence);

	return parseThreadTurnCheckpoint({
		...(closeout.executionRunId !== undefined
			? { executionRunId: closeout.executionRunId }
			: {}),
		pendingInputContextSequences,
		...(request !== undefined ? { request } : {}),
		...(closeout.interruptEventId !== undefined
			? { interruptEventId: closeout.interruptEventId }
			: {}),
		...(closeout.terminalCloseout !== undefined
			? { terminalCloseout: closeout.terminalCloseout }
			: {}),
		...(idleCloseout !== undefined ? { idleCloseout } : {}),
	});
}

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

export function extractColdThreadToolRouteView(input: {
	readonly checkpoint: ThreadTurnCheckpoint;
	readonly pendingToolUses: readonly ColdPendingToolUse[];
	readonly pendingSandboxExecutions: readonly ColdPendingSandboxExecution[];
}): ThreadToolRouteView {
	const checkpoint = parseThreadTurnCheckpoint(input.checkpoint);
	if (checkpoint.terminalCloseout !== undefined) {
		if (
			input.pendingToolUses.length !== 0 ||
			input.pendingSandboxExecutions.length !== 0
		) {
			throw new Error(
				"terminal Thread closeout cannot retain a pending Tool route",
			);
		}
		return { routes: [] };
	}
	const incompleteMembers = new Map(
		(checkpoint.request?.toolMembers ?? [])
			.filter(
				(
					member,
				): member is Extract<
					typeof member,
					{ readonly memberKind: "public_tool_use" }
				> =>
					member.memberKind === "public_tool_use" &&
					member.terminalResult === undefined,
			)
			.map((member) => [member.toolUseEventId, member]),
	);
	const routeByToolUse = new Map<
		string,
		ThreadToolRouteView["routes"][number]
	>();

	const addRoute = (
		source: ColdPendingToolUse | ColdPendingSandboxExecution,
		disposition: ThreadToolRouteView["routes"][number]["disposition"],
	): void => {
		const member = incompleteMembers.get(source.toolUseEventId);
		if (
			member === undefined ||
			checkpoint.request === undefined ||
			source.modelRequestId !== checkpoint.request.modelRequestId ||
			source.modelToolCallId !== member.modelToolCallId ||
			source.toolName !== member.toolName
		) {
			throw new Error(
				"cold Tool route identity does not match its request member",
			);
		}
		if (routeByToolUse.has(source.toolUseEventId)) {
			throw new Error(
				"cold non-terminal Tool Use requires exactly one durable route",
			);
		}
		routeByToolUse.set(source.toolUseEventId, {
			toolUseEventId: source.toolUseEventId,
			disposition,
		});
	};

	for (const pending of input.pendingToolUses) {
		if (pending.status === "pending" && pending.decision === undefined) {
			addRoute(pending, "requires_user_action");
			continue;
		}
		if (pending.status === "resolving" && pending.decision !== undefined) {
			addRoute(pending, "resume_approval_settlement");
			continue;
		}
		throw new Error("cold pending Tool Use disposition is malformed");
	}
	for (const pending of input.pendingSandboxExecutions) {
		addRoute(pending, "resume_sandbox_execution");
	}

	const interrupted = checkpoint.interruptEventId !== undefined;
	if (
		routeByToolUse.size !== incompleteMembers.size &&
		(!interrupted || routeByToolUse.size !== 0)
	) {
		throw new Error(
			"cold non-terminal Tool Use requires exactly one durable route",
		);
	}
	return parseThreadToolRouteView({
		routes: [...incompleteMembers.keys()].flatMap((toolUseEventId) => {
			const route = routeByToolUse.get(toolUseEventId);
			return route === undefined ? [] : [route];
		}),
	});
}

function validateRequestOrdering(
	events: ThreadTurnLoadFacts["events"],
	starts: ReadonlyMap<string, ThreadTurnLoadFacts["events"][number]>,
	ends: ReadonlyMap<string, ThreadTurnLoadFacts["events"][number]>,
): void {
	const orderedStarts = [...starts.values()];
	const resultSequenceByToolUse = new Map<string, number>();
	const toolUseByRequestAndCall = new Map<string, string>();
	for (const event of events) {
		if (
			(event.type === "agent.tool_use" ||
				event.type === "agent.mcp_tool_use") &&
			event.toolUse !== undefined
		) {
			toolUseByRequestAndCall.set(
				`${event.modelRequestId}:${event.toolUse.modelToolCallId}`,
				event.eventId,
			);
		}
	}
	for (const event of events) {
		if (
			(event.type === "agent.tool_result" ||
				event.type === "agent.mcp_tool_result") &&
			event.toolResult !== undefined &&
			!("repairKey" in event.toolResult)
		) {
			const toolUseEventId = toolUseByRequestAndCall.get(
				`${event.modelRequestId}:${event.toolResult.modelToolCallId}`,
			);
			if (toolUseEventId !== undefined)
				resultSequenceByToolUse.set(toolUseEventId, event.eventSequence);
		}
	}
	for (let index = 1; index < orderedStarts.length; index += 1) {
		const predecessor = orderedStarts[index - 1]!;
		const following = orderedStarts[index]!;
		const predecessorEnd = ends.get(predecessor.modelRequestId!);
		if (
			predecessorEnd === undefined ||
			predecessorEnd.eventSequence >= following.eventSequence
		) {
			throw new Error(
				"following Request Start requires its predecessor to be sealed",
			);
		}
		const unsettledMember = events.some(
			(event) =>
				(event.type === "agent.tool_use" ||
					event.type === "agent.mcp_tool_use") &&
				event.modelRequestId === predecessor.modelRequestId &&
				(resultSequenceByToolUse.get(event.eventId) ??
					Number.POSITIVE_INFINITY) >= following.eventSequence,
		);
		if (unsettledMember) {
			throw new Error(
				"following Request Start requires every predecessor Tool Use to be terminal",
			);
		}
	}
}

function validateRequestMembers(
	events: ThreadTurnLoadFacts["events"],
	starts: ReadonlyMap<string, ThreadTurnLoadFacts["events"][number]>,
	ends: ReadonlyMap<string, ThreadTurnLoadFacts["events"][number]>,
	internalRepairs: ThreadTurnLoadFacts["internalRepairs"],
): void {
	const toolUses = new Map<string, ThreadTurnLoadFacts["events"][number]>();
	const terminalResults = new Set<string>();
	const consumedRepairs = new Set<string>();
	const repairsByKey = new Map(
		internalRepairs.map((repair) => [repair.repairKey, repair]),
	);

	for (const event of events) {
		if (
			event.type === "agent.tool_use" ||
			event.type === "agent.mcp_tool_use"
		) {
			const modelRequestId = event.modelRequestId!;
			const start = starts.get(modelRequestId);
			const end = ends.get(modelRequestId);
			if (start === undefined) {
				throw new Error("Tool Use has no matching Request Start");
			}
			if (event.eventSequence <= start.eventSequence) {
				throw new Error("Tool Use cannot precede Request Start");
			}
			if (end !== undefined && event.eventSequence > end.eventSequence) {
				throw new Error("Tool Use cannot follow Request End");
			}
			if (event.toolUse === undefined)
				throw new Error("Tool Use direct identity is absent");
			toolUses.set(event.eventId, event);
			continue;
		}
		if (
			event.type !== "agent.tool_result" &&
			event.type !== "agent.mcp_tool_result"
		) {
			continue;
		}
		const result = event.toolResult!;
		if ("repairKey" in result) {
			const repair = repairsByKey.get(result.repairKey);
			const start = starts.get(event.modelRequestId!);
			const end = ends.get(event.modelRequestId!);
			if (
				repair === undefined ||
				repair.repairEventId !== event.eventId ||
				repair.eventSequence !== event.eventSequence ||
				repair.modelRequestId !== event.modelRequestId
			) {
				throw new Error(
					"internal repair direct reference does not match its Event",
				);
			}
			if (start === undefined || event.eventSequence <= start.eventSequence) {
				throw new Error("internal repair has no preceding Request Start");
			}
			if (end !== undefined && event.eventSequence > end.eventSequence) {
				throw new Error("internal repair cannot follow Request End");
			}
			consumedRepairs.add(repair.repairKey);
			continue;
		}
		const toolUse = [...toolUses.values()].find(
			(candidate) =>
				candidate.modelRequestId === event.modelRequestId &&
				candidate.toolUse?.modelToolCallId === result.modelToolCallId &&
				candidate.toolUse.toolName === result.toolName,
		);
		if (toolUse === undefined) {
			throw new Error("Tool Result has no matching Tool Use");
		}
		if (
			event.type === "agent.mcp_tool_result" &&
			toolUse.type !== "agent.mcp_tool_use"
		) {
			throw new Error("MCP Tool Result has the wrong Tool Use family");
		}
		if (
			event.type === "agent.tool_result" &&
			toolUse.type !== "agent.tool_use"
		) {
			throw new Error("Tool Result has the wrong Tool Use family");
		}
		if (terminalResults.has(toolUse.eventId)) {
			throw new Error("Tool Use has duplicate terminal results");
		}
		terminalResults.add(toolUse.eventId);
	}
	if (consumedRepairs.size !== internalRepairs.length) {
		throw new Error("internal repair direct reference has no matching Event");
	}
}

function extractNewestRequest(
	start: ThreadTurnLoadFacts["events"][number],
	end: ThreadTurnLoadFacts["events"][number] | undefined,
	events: ThreadTurnLoadFacts["events"],
	internalRepairs: ThreadTurnLoadFacts["internalRepairs"],
): NonNullable<ThreadTurnCheckpoint["request"]> {
	const modelRequestId = start.modelRequestId!;
	const retainedToolUseEventIds = new Set(
		end?.requestEnd?.providerContextRetention.toolUseEventIds ?? [],
	);
	const retainedRepairEventIds = new Set(
		end?.requestEnd?.providerContextRetention.repairEventIds ?? [],
	);
	const terminalByToolUse = new Map<
		string,
		{ readonly outcome: "success" | "error" | "cancelled" | "unknown" }
	>();
	const members: NonNullable<
		ThreadTurnCheckpoint["request"]
	>["toolMembers"][number][] = [];
	const modelToolCallIds = new Set<string>();
	const repairsByKey = new Map(
		internalRepairs.map((repair) => [repair.repairKey, repair]),
	);
	for (const event of events) {
		if (event.modelRequestId !== modelRequestId) {
			continue;
		}
		if (
			(event.type === "agent.tool_result" ||
				event.type === "agent.mcp_tool_result") &&
			event.toolResult !== undefined &&
			!("repairKey" in event.toolResult)
		) {
			const result = event.toolResult;
			const toolUse = events.find(
				(candidate) =>
					(candidate.type === "agent.tool_use" ||
						candidate.type === "agent.mcp_tool_use") &&
					candidate.modelRequestId === modelRequestId &&
					candidate.toolUse?.modelToolCallId === result.modelToolCallId &&
					candidate.toolUse?.toolName === result.toolName,
			);
			if (toolUse === undefined)
				throw new Error("Tool Result direct identity has no Tool Use");
			terminalByToolUse.set(toolUse.eventId, {
				outcome: result.outcome === "completed" ? "success" : result.outcome,
			});
		}
	}
	for (const event of events) {
		if (event.modelRequestId !== modelRequestId) {
			continue;
		}
		if (
			event.type === "agent.tool_use" ||
			event.type === "agent.mcp_tool_use"
		) {
			if (end !== undefined && !retainedToolUseEventIds.has(event.eventId)) {
				continue;
			}
			const toolUse = event.toolUse!;
			if (modelToolCallIds.has(toolUse.modelToolCallId)) {
				throw new Error("modelToolCallId is duplicated within the request");
			}
			modelToolCallIds.add(toolUse.modelToolCallId);
			const terminalResult = terminalByToolUse.get(event.eventId);
			members.push({
				memberKind: "public_tool_use",
				modelToolCallId: toolUse.modelToolCallId,
				toolUseEventId: event.eventId,
				toolName: toolUse.toolName,
				...(terminalResult !== undefined ? { terminalResult } : {}),
			});
		} else if (
			event.type === "agent.tool_result" &&
			event.toolResult !== undefined &&
			"repairKey" in event.toolResult
		) {
			if (end !== undefined && !retainedRepairEventIds.has(event.eventId)) {
				continue;
			}
			const repair = repairsByKey.get(event.toolResult.repairKey)!;
			if (modelToolCallIds.has(repair.modelToolCallId)) {
				throw new Error("modelToolCallId is duplicated within the request");
			}
			modelToolCallIds.add(repair.modelToolCallId);
			members.push({
				memberKind: "internal_tool_repair",
				modelToolCallId: repair.modelToolCallId,
				toolName: repair.toolName,
				repairEventId: event.eventId,
				outcome: "error",
			});
		}
	}
	return {
		modelRequestId,
		requestStartEventId: start.eventId,
		requestKind: start.requestStart!.requestKind,
		contextThroughMessageSequence:
			start.requestStart!.contextThroughMessageSequence,
		...(end?.requestEnd !== undefined
			? {
					requestEnd: {
						eventId: end.eventId,
						isError: end.requestEnd.isError,
						...(end.requestEnd.errorKind !== undefined
							? { errorKind: end.requestEnd.errorKind }
							: {}),
						providerContextRetention:
							end.requestEnd.providerContextRetention,
						...(end.requestEnd.reschedule !== undefined
							? { reschedule: end.requestEnd.reschedule }
							: {}),
					},
				}
			: {}),
		toolMembers: members,
	};
}

function extractRunCloseout(
	events: ThreadTurnLoadFacts["events"],
): Pick<
	ThreadTurnCheckpoint,
	"executionRunId" | "interruptEventId" | "terminalCloseout" | "idleCloseout"
> {
	const running = events
		.filter(
			(event) =>
				event.type === "session.status_running" ||
				event.type === "session.thread_status_running",
		)
		.at(-1);
	const relevant =
		running === undefined
			? events
			: events.filter((event) => event.eventSequence >= running.eventSequence);
	const interrupt = relevant
		.filter(
			(event) =>
				event.type === "user.interrupt" ||
				event.type === "agent.thread_interrupt_requested",
		)
		.at(-1);
	const idle = relevant
		.filter(
			(event) =>
				event.type === "session.status_idle" ||
				event.type === "session.thread_status_idle",
		)
		.at(-1);
	const terminated = relevant
		.filter(
			(event) =>
				event.type === "session.status_terminated" ||
				event.type === "session.thread_status_terminated",
		)
		.at(-1);
	const runClosed = idle !== undefined || terminated !== undefined;
	const closeoutSequence = Math.max(
		idle?.eventSequence ?? 0,
		terminated?.eventSequence ?? 0,
	);
	const activeInterrupt =
		interrupt !== undefined && interrupt.eventSequence > closeoutSequence
			? interrupt
			: undefined;
	const closeout =
		terminated ??
		(idle?.idle?.stopReason === "retries_exhausted" ? idle : undefined);
	const expectedRetryStatus =
		terminated !== undefined ? "terminal" : "exhausted";
	const pairedFailure =
		closeout === undefined
			? undefined
			: relevant
					.filter(
						(event) =>
							event.type === "session.error" &&
							event.eventSequence < closeout.eventSequence &&
							event.failure?.retryStatus === expectedRetryStatus,
					)
					.at(-1);
	if (closeout !== undefined && pairedFailure === undefined) {
		throw new Error("terminal closeout has no preceding failure fact");
	}
	const terminalCloseout =
		closeout === undefined || pairedFailure === undefined
			? undefined
			: {
					failureEventId: pairedFailure.eventId,
					closeoutEventId: closeout.eventId,
					disposition:
						terminated !== undefined
							? ("terminated" as const)
							: ("retries_exhausted" as const),
				};
	return {
		...(!runClosed && running !== undefined
			? { executionRunId: running.eventId }
			: {}),
		...(activeInterrupt !== undefined
			? { interruptEventId: activeInterrupt.eventId }
			: {}),
		...(terminalCloseout !== undefined ? { terminalCloseout } : {}),
		...(idle !== undefined
			? {
					idleCloseout: {
						eventId: idle.eventId,
						stopReason: idle.idle!.stopReason,
					},
				}
			: {}),
	};
}
