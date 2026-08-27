import { readFile } from "node:fs/promises";
import { Metadata } from "@grpc/grpc-js";
import type { RuntimeContextEntry } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import { RuntimeToolSettlementDeclarationSchema } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import type {
	LLMRequest,
	Interface as LLMServiceInterface,
} from "@tetral/agent-runtime-core/src/llm/llm-service.js";
import { internalToolRepairKey } from "@tetral/agent-runtime-core/src/runtime/accumulator.js";
import { toGatewayProviderContextSegments } from "@tetral/agent-runtime-core/src/runtime/context-projection.js";
import * as ThreadLoop from "@tetral/agent-runtime-core/src/thread-loop/thread-loop.js";
import { ThreadRuntime } from "@tetral/agent-runtime-core/src/thread-loop/thread-runtime.js";
import {
	assembleProviderCallRequest,
	DefaultProviderCallRuntimeConfig,
} from "@tetral/agent-runtime-core/src/thread-loop/provider-request.js";
import { createToolCatalog } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import {
	extractColdThreadToolRouteView,
	extractThreadTurnCheckpoint,
} from "@tetral/agent-runtime-core/src/thread-loop/turn/load.js";
import { deriveThreadTurnSnapshot } from "@tetral/agent-runtime-core/src/thread-loop/turn/reducer.js";
import type { AgentRuntimeBridgeServiceClient } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { Effect, Stream } from "effect";
import { lowerProviderRequest } from "../../../../../gateway/packages/lowering/src/request.js";
import { GatewayProviderRules } from "../../../../../gateway/packages/lowering/src/rules/index.js";
import { validateProviderRequest } from "../../../../../gateway/packages/protocol/src/bounds.js";
import {
	QueuedContextLoader,
	runtimeThreadLoopLayer,
	testRunCustody,
	writerFrom,
} from "../../../core/test/unit/thread-loop/thread-loop-test-support.js";
import { BridgeAPIContextLoader } from "../../src/bridge-client.js";
import { buildRuntimeCoreHosts } from "../../src/core-hosts.js";

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
		readonly toolUseEventId?: string;
		readonly modelToolCallId?: string;
		readonly settlement?: unknown;
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
const coldHosts = await buildRuntimeCoreHosts({
	maxLocalSessions: 1,
	now: () => "2026-08-27T00:00:00.000Z",
	contextLoader: {
		loadThreadContext: async () => cold,
		commitAcceptedInput: async () => {
			throw new Error("cold preload must not commit accepted input");
		},
	},
	threadLoop: productionRuntimeOptions({
		runTool: async () => {
			throw new Error("settled cold context must not execute a Tool");
		},
		stream: () => Stream.empty,
	}),
});
let coldProductionEntries: readonly RuntimeContextEntry[];
let coldProductionPreloaded = false;
try {
	const preload = await coldHosts.subAgentRunHost.preloadThread(command);
	if (!preload.ok) {
		throw new Error(`cold production preload failed: ${preload.reason}`);
	}
	const snapshot = await coldHosts.subAgentRunHost.inspectThread(command);
	if (!snapshot.ok || !snapshot.observed) {
		throw new Error("cold production preload did not publish the resident Thread");
	}
	coldProductionPreloaded = true;
	coldProductionEntries = snapshot.entries;
} finally {
	await coldHosts.close();
}
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
			readonly toolPart?: unknown;
			readonly providerComposition?: unknown;
			readonly providerContext?: unknown;
			readonly providerInvocations: number;
	  }
	| undefined;

if (input.hotScenario !== undefined) {
	const scenario = input.hotScenario;
	const base = await loadContext(scenario.baseContextJson);
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
	if (scenario.modelToolCallId === undefined) {
		throw new Error("Tool settlement hot receipt is incomplete");
	}
	const runtime = new ThreadRuntime("composition_session");
	runtime.state.contextManager.replaceEntries(base.contextEntries);
	runtime.state.contextManager.installOpenRequestDraft(base.openRequestDraft);
	runtime.state.installThreadTurn(baseCheckpoint, baseRoutes);
	const providerRequests: LLMRequest[] = [];
	const layer = runtimeThreadLoopLayer(
		new QueuedContextLoader([], []),
		hotRuntimeOptions({
			runTool: async () => toolExecutionResult(hotSettlement.outcome),
			stream: (request) => {
				providerRequests.push(request);
				return Stream.fromIterable([
					{ type: "text-start" as const, id: "composition-text" },
					{ type: "text-delta" as const, id: "composition-text", text_delta: "done" },
					{ type: "text-end" as const, id: "composition-text" },
					{ type: "finish" as const, finishReason: "stop" as const },
				]);
			},
		}),
	);
	const runResult = await Effect.runPromise(
		Effect.gen(function* () {
			const threadLoop = yield* ThreadLoop.Service;
			runtime.state.markPersistentContextLoaded();
			threadLoop.seedRuntimeModel(runtime);
			if ((base.pendingToolUses?.length ?? 0) > 0) {
				const installed = yield* threadLoop.installLoadedPendingToolUses(
					runtime,
					base.pendingToolUses ?? [],
					base.contextEntries,
					base.openRequestDraft,
				);
				if (!installed.ok) throw new Error("hot pending Tool installation failed");
			}
			if ((base.pendingSandboxExecutions?.length ?? 0) > 0) {
				const installed = yield* threadLoop.installLoadedSandboxExecutions(
					runtime,
					base.pendingSandboxExecutions ?? [],
					base.contextEntries,
					base.openRequestDraft,
				);
				if (!installed.ok) throw new Error("hot Sandbox installation failed");
			}
			return yield* threadLoop.run(runtime, testRunCustody());
		}).pipe(Effect.provide(layer)),
	);
	if (runResult.type !== "completed" || providerRequests.length !== 1) {
		throw new Error(
			`hot production ThreadLoop did not settle once before Provider dispatch: ${runResult.type}/${providerRequests.length}`,
		);
	}
	const toolPart =
		scenario.modelToolCallId === undefined
			? undefined
			: (providerRequests[0]?.context ?? [])
					.flatMap((entry) => entry.content)
					.find(
						(part) => part.toolResult?.modelToolCallId === scenario.modelToolCallId,
					);
	hot = {
		...(toolPart === undefined ? {} : { toolPart }),
		providerContext: providerRequests[0]?.context,
		providerInvocations: providerRequests.length,
		...(input.providerComposition === true
			? {
					providerComposition: composeProviderContext(
						providerRequests[0]?.context ?? [],
					),
				}
			: {}),
	};
}

process.stdout.write(
	JSON.stringify({
		coldProductionPreloaded,
		checkpoint,
		toolRouteView,
		nextStep: deriveThreadTurnSnapshot(
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
						coldProductionEntries,
						cold.threadContextPrefix?.entries,
					),
				}
			: {}),
		...(hot === undefined ? {} : { hot }),
	}),
);

function productionRuntimeOptions(options: {
	readonly runTool: NonNullable<ThreadLoop.ThreadLoopRuntimeOptions["runTool"]>;
	readonly stream: LLMServiceInterface["stream"];
}): ThreadLoop.ThreadLoopRuntimeOptions {
	let nextId = 0;
	return {
		internalToolRepairStore: {} as never,
		sessionEventWriter: compositionWriter(),
		runtime: {
			now: () => "2026-08-27T00:00:00.000Z",
			monotonicMs: () => 0,
			createId: (prefix) => `${prefix}_composition_${++nextId}`,
			sleep: async (_delayMs, signal) => !signal.aborted,
		},
		llmService: { stream: options.stream },
		storeOperationTimeoutMs: 5_000,
		approvalMode: "full_access",
		providerCallRuntime: {
			...DefaultProviderCallRuntimeConfig,
			systemInstructions: "hot/cold Tool production composition",
			timeoutMs: 5_000,
		},
		runtimeModel: () => ({ providerId: "fake", modelId: "fake-chat" }),
		runtimePolicy: () => ({
			toolCatalog: createToolCatalog({ family: "claude" }),
			providerRescheduleBudget: 0,
		}),
		runTool: options.runTool,
	};
}

function hotRuntimeOptions(options: {
	readonly runTool: NonNullable<ThreadLoop.ThreadLoopRuntimeOptions["runTool"]>;
	readonly stream: LLMServiceInterface["stream"];
}): NonNullable<Parameters<typeof runtimeThreadLoopLayer>[1]> {
	let nextId = 0;
	return {
		writer: compositionWriter(),
		runtime: {
			now: () => "2026-08-27T00:00:00.000Z",
			monotonicMs: () => 0,
			createId: (prefix) => `${prefix}_composition_${++nextId}`,
			sleep: async (_delayMs, signal) => !signal.aborted,
		},
		llmService: { stream: options.stream },
		providerCallRuntime: {
			...DefaultProviderCallRuntimeConfig,
			systemInstructions: "hot/cold Tool production composition",
			timeoutMs: 5_000,
			toolCatalog: createToolCatalog({ family: "claude" }),
		},
		runTool: options.runTool,
	};
}

function compositionWriter() {
	return writerFrom(
			(envelope) => ({
				ok: true,
				type: "committed",
				eventId: `bridge-${envelope.writeId}`,
			}),
			undefined,
			[],
			{ eventSequence: 100, messageSequence: 100 },
			async () => ({ ok: true, result: { type: "committed" } }),
		);
}

function toolExecutionResult(
	outcome: ReturnType<typeof RuntimeToolSettlementDeclarationSchema.parse>["outcome"],
) {
	switch (outcome.type) {
		case "completed":
			return { type: "completed" as const, output: outcome.output };
		case "error":
			return { type: "error" as const, error: outcome.error };
		case "cancelled":
			return { type: "cancelled" as const };
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
	return composeProviderContext(projected.context);
}

function composeProviderContext(
	providerContext: readonly LLMRequest["context"][number][],
) {
	const requestContext = [...providerContext];
	const strategies = GatewayProviderRules.map((rules) => {
		const assembled = assembleProviderCallRequest({
			identity: {
				...command,
				runtimeBindingToken: "composition-binding-token",
			},
			requestId: "req_provider_composition",
			modelRequestId: "mreq_provider_composition",
			currentModel: { providerId: rules.providerId, modelId: rules.modelId },
			providerContext: requestContext,
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
	const toolItems = providerContext.flatMap((entry) =>
		entry.content.filter(
			(item) => item.toolCall !== undefined || item.toolResult !== undefined,
		),
	);
	return {
		carrierMessages: providerContext,
		carrierHasToolUseEventIdProperty: toolItems.some((item) =>
			Object.hasOwn(item, "toolUseEventId"),
		),
		strategies,
	};
}
