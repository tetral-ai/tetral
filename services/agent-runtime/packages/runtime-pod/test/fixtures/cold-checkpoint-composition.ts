import { readFile } from "node:fs/promises";
import { Metadata } from "@grpc/grpc-js";
import type { RuntimeContextEntry } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import {
	RuntimeAssistantContextAppendSchema,
	RuntimeContextEntrySchema,
	RuntimeToolSettlementDeclarationSchema,
} from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import { internalToolRepairKey } from "@tetral/agent-runtime-core/src/runtime/accumulator.js";
import { toGatewayProviderContextSegments } from "@tetral/agent-runtime-core/src/runtime/context-projection.js";
import {
	applyAcceptedInputResult,
	applyAssistantAppendResult,
	applyInternalToolRepairResult,
	contextToolResultFromSettlement,
	internalToolRepairContext,
	sealAssistantDraft,
} from "@tetral/agent-runtime-core/src/runtime/runtime-declaration.js";
import { ContextManager } from "@tetral/agent-runtime-core/src/session/context-manager.js";
import { assembleProviderCallRequest } from "@tetral/agent-runtime-core/src/thread-loop/provider-request.js";
import {
	extractColdThreadToolRouteView,
	extractThreadTurnCheckpoint,
} from "@tetral/agent-runtime-core/src/thread-loop/thread-turn-checkpoint.js";
import { deriveThreadTurnDecision } from "@tetral/agent-runtime-core/src/thread-loop/thread-turn-reducer.js";
import type { AgentRuntimeBridgeServiceClient } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { lowerProviderRequest } from "../../../../../gateway/packages/lowering/src/request.js";
import { GatewayProviderRules } from "../../../../../gateway/packages/lowering/src/rules/index.js";
import { validateProviderRequest } from "../../../../../gateway/packages/protocol/src/bounds.js";
import { BridgeAPIContextLoader } from "../../src/bridge-client.js";

const inputPath = process.argv[2];
if (inputPath === undefined)
	throw new Error("cold checkpoint composition input path is required");

const input = JSON.parse(await readFile(inputPath, "utf8")) as {
	readonly contextJson: string;
	readonly providerComposition?: boolean;
	readonly pendingToolUses?: readonly unknown[];
	readonly pendingSandboxExecutions?: readonly unknown[];
	readonly hotScenario?: {
		readonly baseContextJson: string;
		readonly kind:
			| "accepted_input"
			| "assistant_append"
			| "tool_settlement"
			| "internal_repair"
			| "compaction";
		readonly declaredDrafts?: readonly unknown[];
		readonly append?: unknown;
		readonly result?: {
			readonly assignedContextSequences?: readonly number[];
			readonly messageSequence?: number;
			readonly createdToolUseEventIds?: readonly string[];
			readonly assignedMessageSequence?: number;
			readonly checkpointMessageSequence?: number;
		};
		readonly modelRequestId?: string;
		readonly assistantMessageSequence?: number;
		readonly modelToolCallId?: string;
		readonly toolName?: string;
		readonly canonicalInput?: unknown;
		readonly error?: unknown;
		readonly settlement?: unknown;
		readonly compactedThroughMessageSequence?: number;
		readonly turnFacts?: unknown;
		readonly pendingToolUses?: readonly unknown[];
		readonly pendingSandboxExecutions?: readonly unknown[];
	};
};

const command = {
	workspaceId: "default",
	sessionId: "composition_session",
	sessionThreadId: "composition_thread",
	bindingId: "composition_binding",
	bindingGeneration: 1,
	targetPodUid: "composition_pod",
};

const loadContext = async (contextJson: string) => {
	const loader = new BridgeAPIContextLoader({
		address: "unused.test",
		tokenPath: "/unused/token",
		metadataFactory: async () => new Metadata(),
		client: {
			loadContext: (
				_request: unknown,
				_metadata: Metadata,
				callback: (error: Error | null, response: unknown) => void,
			) => {
				callback(null, {
					contextJson,
					runtimeBindingToken: "composition-binding-token",
				});
				return { cancel: () => undefined };
			},
		} as unknown as AgentRuntimeBridgeServiceClient,
	});
	return await loader.loadThreadContext(command);
};

const cold = await loadContext(input.contextJson);
const checkpoint = extractThreadTurnCheckpoint({
	contextEntries: cold.contextEntries,
	facts: cold.turnFacts,
});
const toolRouteView = extractColdThreadToolRouteView({
	checkpoint,
	pendingToolUses: (input.pendingToolUses ??
		cold.pendingToolUses ??
		[]) as never,
	pendingSandboxExecutions: (input.pendingSandboxExecutions ??
		cold.pendingSandboxExecutions ??
		[]) as never,
});

let hot:
	| {
			readonly checkpoint: unknown;
			readonly toolRouteView: unknown;
			readonly reducerAction: unknown;
			readonly toolPart?: unknown;
			readonly providerComposition?: unknown;
	  }
	| undefined;

if (input.hotScenario !== undefined) {
	const scenario = input.hotScenario;
	const base = await loadContext(scenario.baseContextJson);
	const hotEntries = applyHotOperation(
		base.contextEntries,
		base.openRequestDraft,
		scenario,
	);
	const hotFacts =
		scenario.turnFacts === undefined
			? cold.turnFacts
			: (scenario.turnFacts as typeof cold.turnFacts);
	const hotCheckpoint = extractThreadTurnCheckpoint({
		contextEntries: hotEntries,
		facts: hotFacts,
	});
	const hotRoutes = extractColdThreadToolRouteView({
		checkpoint: hotCheckpoint,
		pendingToolUses: (scenario.pendingToolUses ??
			input.pendingToolUses ??
			cold.pendingToolUses ??
			[]) as never,
		pendingSandboxExecutions: (scenario.pendingSandboxExecutions ??
			input.pendingSandboxExecutions ??
			cold.pendingSandboxExecutions ??
			[]) as never,
	});
	const toolPart =
		scenario.modelToolCallId === undefined
			? undefined
			: hotEntries
					.flatMap((entry) => entry.parts)
					.find(
						(part) =>
							part.type === "tool_result" &&
							part.modelToolCallId === scenario.modelToolCallId,
					);
	hot = {
		checkpoint: hotCheckpoint,
		toolRouteView: hotRoutes,
		reducerAction: deriveThreadTurnDecision(hotCheckpoint, hotRoutes).action,
		...(toolPart === undefined ? {} : { toolPart }),
		...(input.providerComposition === true
			? {
					providerComposition: composeProviderRequests(
						hotEntries,
						base.threadContextPrefix?.entries,
					),
				}
			: {}),
	};
}

process.stdout.write(
	JSON.stringify({
		checkpoint,
		toolRouteView,
		reducerAction: deriveThreadTurnDecision(checkpoint, toolRouteView).action,
		derivedRepairKeys: (checkpoint.request?.toolMembers ?? []).flatMap(
			(member) =>
				member.memberKind === "internal_tool_repair"
					? [
							internalToolRepairKey(
								checkpoint.request!.modelRequestId,
								member.modelToolCallId,
								member.toolName,
							),
						]
					: [],
		),
		...(input.providerComposition === true
			? {
					providerComposition: composeProviderRequests(
						cold.contextEntries,
						cold.threadContextPrefix?.entries,
					),
				}
			: {}),
		...(hot === undefined ? {} : { hot }),
	}),
);

function applyHotOperation(
	base: readonly RuntimeContextEntry[],
	baseOpenRequestDraft: Awaited<
		ReturnType<typeof loadContext>
	>["openRequestDraft"],
	scenario: NonNullable<typeof input.hotScenario>,
): readonly RuntimeContextEntry[] {
	switch (scenario.kind) {
		case "accepted_input": {
			if (
				scenario.declaredDrafts === undefined ||
				scenario.result?.assignedContextSequences === undefined
			) {
				throw new Error("accepted-input hot scenario is incomplete");
			}
			const drafts = scenario.declaredDrafts.map((draft) => {
				if (typeof draft !== "object" || draft === null || Array.isArray(draft))
					throw new Error("accepted-input draft is malformed");
				const value = draft as {
					readonly contextKind?: unknown;
					readonly parts?: unknown;
				};
				const entry = RuntimeContextEntrySchema.parse({
					messageSequence: 1,
					contextKind: value.contextKind,
					parts: value.parts,
				});
				return { contextKind: entry.contextKind, parts: entry.parts };
			});
			return [
				...base,
				...applyAcceptedInputResult(
					drafts,
					scenario.result.assignedContextSequences,
				),
			];
		}
		case "assistant_append": {
			if (
				scenario.modelRequestId === undefined ||
				scenario.append === undefined ||
				scenario.result?.messageSequence === undefined ||
				scenario.result.createdToolUseEventIds === undefined
			) {
				throw new Error("Assistant append hot scenario is incomplete");
			}
			const append = RuntimeAssistantContextAppendSchema.parse(scenario.append);
			const applied = applyAssistantAppendResult({
				modelRequestId: scenario.modelRequestId,
				append,
				result: {
					messageSequence: scenario.result.messageSequence,
					createdToolUseEventIds: scenario.result.createdToolUseEventIds,
				},
			});
			return [...base, sealAssistantDraft(applied.draft)];
		}
		case "tool_settlement": {
			if (
				scenario.assistantMessageSequence === undefined ||
				scenario.modelToolCallId === undefined ||
				scenario.settlement === undefined
			) {
				throw new Error("Tool settlement hot scenario is incomplete");
			}
			const declaration = RuntimeToolSettlementDeclarationSchema.parse({
				toolUseEventId: "composition_tool",
				outcome: scenario.settlement,
			});
			const contextOwner = new ContextManager("composition_session", base);
			contextOwner.installOpenRequestDraft(baseOpenRequestDraft);
			const resultPart = contextToolResultFromSettlement(
				scenario.modelToolCallId,
				declaration.outcome,
			);
			contextOwner.appendToolResult(
				scenario.assistantMessageSequence,
				scenario.modelToolCallId,
				resultPart.result,
			);
			return contextOwner.entries();
		}
		case "internal_repair": {
			if (
				scenario.modelToolCallId === undefined ||
				scenario.toolName === undefined ||
				scenario.canonicalInput === undefined ||
				scenario.error === undefined ||
				scenario.result?.assignedMessageSequence === undefined
			) {
				throw new Error("internal-repair hot scenario is incomplete");
			}
			const draft = internalToolRepairContext({
				modelToolCallId: scenario.modelToolCallId,
				toolName: scenario.toolName,
				canonicalInput: scenario.canonicalInput as never,
				error: scenario.error as never,
			});
			return [
				...base,
				sealAssistantDraft(
					applyInternalToolRepairResult({
						modelRequestId:
							scenario.modelRequestId ??
							baseOpenRequestDraft?.modelRequestId ??
							"composition_internal_repair",
						existingDraft: baseOpenRequestDraft,
						assignedMessageSequence: scenario.result.assignedMessageSequence,
						context: draft,
					}),
				),
			];
		}
		case "compaction": {
			if (
				scenario.declaredDrafts?.length !== 1 ||
				scenario.result?.checkpointMessageSequence === undefined ||
				scenario.compactedThroughMessageSequence === undefined
			) {
				throw new Error("compaction hot scenario is incomplete");
			}
			const draft = scenario.declaredDrafts[0] as {
				readonly contextKind?: unknown;
				readonly parts?: unknown;
			};
			const checkpointEntry = RuntimeContextEntrySchema.parse({
				messageSequence: scenario.result.checkpointMessageSequence,
				contextKind: draft.contextKind,
				parts: draft.parts,
			});
			return [
				checkpointEntry,
				...base.filter(
					(entry) =>
						entry.messageSequence > scenario.compactedThroughMessageSequence!,
				),
			];
		}
	}
}

function composeProviderRequests(
	entries: readonly RuntimeContextEntry[],
	prefixEntries?: readonly RuntimeContextEntry[],
) {
	const projected = toGatewayProviderContextSegments(
		prefixEntries === undefined ? [entries] : [prefixEntries, entries],
	);
	if (!projected.ok)
		throw new Error(
			`Runtime provider projection failed: ${projected.error.code}`,
		);
	const strategies = GatewayProviderRules.map((rules) => {
		const assembled = assembleProviderCallRequest({
			identity: {
				...command,
				runtimeBindingToken: "composition-binding-token",
			},
			requestId: "req_provider_composition",
			modelRequestId: "mreq_provider_composition",
			currentModel: { providerId: rules.providerId, modelId: rules.modelId },
			providerContext: projected.context,
			runtime: {
				systemInstructions: "provider composition",
				timeoutMs: 30_000,
			},
		});
		if (!assembled.ok)
			throw new Error(
				`Runtime ProviderRequest assembly failed for ${rules.providerId}/${rules.modelId}`,
			);
		const validation = validateProviderRequest(assembled.request);
		return {
			providerId: rules.providerId,
			modelId: rules.modelId,
			providerFamily: rules.providerFamily,
			validation,
			...(validation.ok
				? {
						providerRequest: assembled.request,
						loweredMessages: lowerProviderRequest(assembled.request, rules, {
							modelOutputTokenLimit: 32_000,
						}).messages,
					}
				: {}),
		};
	});
	const toolItems = projected.context.flatMap((entry) =>
		entry.content.filter(
			(item) => item.toolCall !== undefined || item.toolResult !== undefined,
		),
	);
	return {
		carrierMessages: projected.context,
		carrierHasToolUseEventIdProperty: toolItems.some((item) =>
			Object.hasOwn(item, "toolUseEventId"),
		),
		strategies,
	};
}
