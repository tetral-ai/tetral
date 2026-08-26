import { readFile } from "node:fs/promises";
import { Metadata } from "@grpc/grpc-js";
import type { RuntimeContextEntry } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import { RuntimeToolSettlementDeclarationSchema } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import { internalToolRepairKey } from "@tetral/agent-runtime-core/src/runtime/accumulator.js";
import { toGatewayProviderContextSegments } from "@tetral/agent-runtime-core/src/runtime/context-projection.js";
import { applyCommittedRecoveredToolSettlement } from "@tetral/agent-runtime-core/src/thread-loop/thread-loop.js";
import { ThreadRuntime } from "@tetral/agent-runtime-core/src/thread-loop/thread-runtime.js";
import { assembleProviderCallRequest } from "@tetral/agent-runtime-core/src/thread-loop/provider-request.js";
import {
	extractColdThreadToolRouteView,
	extractThreadTurnCheckpoint,
} from "@tetral/agent-runtime-core/src/thread-loop/turn/load.js";
import {
	deriveThreadTurnSnapshot,
} from "@tetral/agent-runtime-core/src/thread-loop/turn/reducer.js";
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
		readonly assistantMessageSequence?: number;
		readonly toolUseEventId?: string;
		readonly modelToolCallId?: string;
		readonly settlement?: unknown;
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
const coldActiveInputView = {
	hasPendingAttachments: (cold.pendingAttachments?.length ?? 0) > 0,
};
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
	const hotActiveInputView = {
		hasPendingAttachments: (base.pendingAttachments?.length ?? 0) > 0,
	};
	const baseCheckpoint = extractThreadTurnCheckpoint({
		contextEntries: base.contextEntries,
		facts: base.turnFacts,
	});
	const baseRoutes = extractColdThreadToolRouteView({
		checkpoint: baseCheckpoint,
		pendingToolUses: (base.pendingToolUses ?? []) as never,
		pendingSandboxExecutions: (base.pendingSandboxExecutions ?? []) as never,
	});
	const hotSettlement = RuntimeToolSettlementDeclarationSchema.parse({
		toolUseEventId: scenario.toolUseEventId,
		outcome: scenario.settlement,
	});
	if (
		scenario.assistantMessageSequence === undefined ||
		scenario.modelToolCallId === undefined
	) {
		throw new Error("Tool settlement hot receipt is incomplete");
	}
	const runtime = new ThreadRuntime("composition_session");
	runtime.state.contextManager.replaceEntries(base.contextEntries);
	runtime.state.contextManager.installOpenRequestDraft(base.openRequestDraft);
	runtime.state.installThreadTurn(baseCheckpoint, baseRoutes);
	const applied = applyCommittedRecoveredToolSettlement(
		runtime,
		{
			toolUseEventId: hotSettlement.toolUseEventId,
			assistantMessageSequence: scenario.assistantMessageSequence,
			modelToolCallId: scenario.modelToolCallId,
		},
		hotSettlement.outcome,
	);
	if (applied.type !== "settled") {
		throw new Error(`Tool settlement hot receipt failed: ${applied.type}`);
	}
	const hotEntries = runtime.state.contextManager.entries();
	const hotCheckpoint = runtime.state.threadTurnTransition().checkpoint;
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
		reducerAction: deriveThreadTurnSnapshot(
			hotCheckpoint,
			hotRoutes,
			[],
			hotActiveInputView,
		).nextStep,
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
		reducerAction: deriveThreadTurnSnapshot(
			checkpoint,
			toolRouteView,
			[],
			coldActiveInputView,
		).nextStep,
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
