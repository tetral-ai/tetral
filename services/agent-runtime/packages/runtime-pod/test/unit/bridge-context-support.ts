import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { Metadata } from "@grpc/grpc-js";
import type { RuntimeContextEntry } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import { RuntimeToolSettlementDeclarationSchema } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import type {
	LLMRequest,
	Interface as LLMServiceInterface,
} from "@tetral/agent-runtime-core/src/llm/llm-service.js";
import { toGatewayProviderContextSegments } from "@tetral/agent-runtime-core/src/runtime/context-projection.js";
import * as ThreadLoop from "@tetral/agent-runtime-core/src/thread-loop/thread-loop.js";
import { ThreadRuntime } from "@tetral/agent-runtime-core/src/thread-loop/thread-runtime.js";
import {
	assembleProviderCallRequest,
	DefaultProviderCallRuntimeConfig,
} from "@tetral/agent-runtime-core/src/thread-loop/provider-request.js";
import {
	extractColdThreadToolRouteView,
	extractThreadTurnCheckpoint,
} from "@tetral/agent-runtime-core/src/thread-loop/turn/load.js";
import { deriveThreadTurnSnapshot } from "@tetral/agent-runtime-core/src/thread-loop/turn/reducer.js";
import { createToolCatalog } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import type { AgentRuntimeBridgeServiceClient } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { ProviderRequest } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { Effect, Stream } from "effect";
import {
	QueuedContextLoader,
	runtimeThreadLoopLayer,
	testRunCustody,
	writerFrom,
} from "../../../core/test/unit/thread-loop/thread-loop-test-support.js";
import { BridgeAPIContextLoader } from "../../src/bridge-client.js";
import { buildRuntimeCoreHosts } from "../../src/core-hosts.js";

// Bridge's LoadContext envelopes, written by Bridge's tests (see the folder's
// README), and the ProviderRequests the Runtime builds from them, read by the
// Gateway's tests.
export const bridgeContextVectors = new URL(
	"../../../protocol/testdata/bridge-context/",
	import.meta.url,
);
export const liveHotContexts = new URL("../../../protocol/testdata/runtime-hot-context/", import.meta.url);
export const providerRequestVectors = new URL(
	"../../../../../../testdata/provider-requests/",
	import.meta.url,
);

export function vectorNames(folder: URL): string[] {
	return readdirSync(folder)
		.filter((name) => name.endsWith(".json"))
		.map((name) => name.slice(0, -".json".length))
		.sort();
}

// The envelope as Bridge sends it in LoadContextResponse.context_json.
export function bridgeContext(name: string): string {
	return JSON.stringify(
		JSON.parse(readFileSync(new URL(`${name}.json`, bridgeContextVectors), "utf8")),
	);
}

// pinProviderRequest compares the request, in its protobuf JSON form, with its
// vector, or rewrites the vector when TETRAL_UPDATE_VECTORS=1; CI never sets it.
export const pinnedProviderRequests = new Set<string>();

export function pinProviderRequest(name: string, request: ProviderRequest): void {
	pinnedProviderRequests.add(name);
	const encoded = `${JSON.stringify(ProviderRequest.toJSON(request), null, 2)}\n`;
	const path = new URL(`${name}.json`, providerRequestVectors);
	if (process.env.TETRAL_UPDATE_VECTORS === "1") {
		if (process.env.CI) throw new Error("TETRAL_UPDATE_VECTORS is set in CI");
		writeFileSync(path, encoded);
		return;
	}
	const pinned = readFileSync(path, "utf8");
	if (pinned !== encoded) {
		throw new Error(
			`ProviderRequest differs from vector ${name}; the Gateway's tests hold the old shape. Rewrite it with TETRAL_UPDATE_VECTORS=1 if the change is intended.\n${encoded}`,
		);
	}
}

export const command = {
	workspaceId: "default",
	sessionId: "composition_session",
	sessionThreadId: "composition_thread",
	bindingId: "composition_binding",
	bindingGeneration: 1,
	targetPodUid: "composition_pod",
	runtimeProcessId: "process-test",
};

const now = "2026-08-27T00:00:00.000Z";

// loadContext parses the envelope through the production Bridge client.
export async function loadContext(contextJson: string) {
	const loader = new BridgeAPIContextLoader({
		address: "unused.test",
		tokenPath: "/unused/token",
		metadataFactory: async () => new Metadata(),
		client: {
			loadContext: (
				_request: unknown,
				_metadata: Metadata,
				_options: unknown,
				callback: (error: Error | null, response: unknown) => void,
			) => {
				callback(null, { contextJson, runtimeBindingToken: "composition-binding-token" });
				return { cancel: () => undefined };
			},
		} as unknown as AgentRuntimeBridgeServiceClient,
	});
	return await loader.loadThreadContext(command);
}

// restoreCold installs the envelope through the production preload, as a
// delivery to a fresh pod does, and derives the Thread's next step from it.
export async function restoreCold(contextJson: string) {
	const cold = await loadContext(contextJson);
	const hosts = await buildRuntimeCoreHosts({
		maxLocalSessions: 1,
		now: () => now,
		contextLoader: {
			loadThreadContext: async () => cold,
			commitAcceptedInput: async () => {
				throw new Error("cold preload must not commit accepted input");
			},
		},
		threadLoop: productionRuntimeOptions({
			runTool: async () => {
				throw new Error("cold preload must not execute a Tool");
			},
			stream: () => Stream.empty,
		}),
	});
	try {
		const preload = await hosts.subAgentRunHost.preloadThread(command);
		if (!preload.ok) throw new Error(`cold preload failed: ${preload.reason}`);
		const snapshot = await hosts.subAgentRunHost.inspectThread(command);
		if (!snapshot.ok || !snapshot.observed) {
			throw new Error("cold preload did not publish the resident Thread");
		}
		const checkpoint = extractThreadTurnCheckpoint({
			messages: cold.messages,
			facts: cold.turnFacts,
		});
		const toolRouteView = extractColdThreadToolRouteView({
			checkpoint,
			pendingToolUses: (cold.pendingToolUses ?? []) as never,
			pendingSandboxExecutions: (cold.pendingSandboxExecutions ?? []) as never,
		});
		return {
			cold,
			entries: snapshot.entries,
			currentRequestMessage: snapshot.currentRequestMessage ?? null,
			activeToolReferences: snapshot.activeToolReferences,
			toolRouteView,
			nextStep: deriveThreadTurnSnapshot(checkpoint, toolRouteView, [], {
				hasPendingAttachments: (cold.pendingAttachments?.length ?? 0) > 0,
			}).nextStep,
		};
	} finally {
		await hosts.close();
	}
}

// preloadOwnership runs the production preload on an envelope that may be
// malformed and reports what it did, never what the envelope held.
export async function preloadOwnership(contextJson: string) {
	const calls = { inputs: 0, tools: 0, provider: 0 };
	const hosts = await buildRuntimeCoreHosts({
		maxLocalSessions: 1,
		now: () => now,
		contextLoader: {
			loadThreadContext: async () => await loadContext(contextJson),
			commitAcceptedInput: async () => {
				calls.inputs += 1;
				throw new Error("ownership preload must not commit accepted input");
			},
		},
		threadLoop: productionRuntimeOptions({
			runTool: async () => {
				calls.tools += 1;
				throw new Error("ownership preload must not execute a Tool");
			},
			stream: () => {
				calls.provider += 1;
				return Stream.empty;
			},
		}),
	});
	try {
		const preload = await hosts.subAgentRunHost.preloadThread(command);
		const snapshot = await hosts.subAgentRunHost.inspectThread(command);
		if (!snapshot.ok) throw new Error("ownership inspection unavailable");
		return { preload, observed: snapshot.observed, calls };
	} finally {
		await hosts.close();
	}
}

// settleHot installs the envelope Bridge sent before the settlement into a
// live Thread, settles its Tool with the given outcome through the real
// ThreadLoop, and returns the context of the one provider request that follows.
export async function settleHot(
	baseContextJson: string,
	toolUseEventId: string,
	outcome: unknown,
): Promise<LLMRequest["context"]> {
	const base = await loadContext(baseContextJson);
	const checkpoint = extractThreadTurnCheckpoint({
		messages: base.messages,
		facts: base.turnFacts,
	});
	const routes = extractColdThreadToolRouteView({
		checkpoint,
		pendingToolUses: (base.pendingToolUses ?? []) as never,
		pendingSandboxExecutions: (base.pendingSandboxExecutions ?? []) as never,
	});
	const settlement = RuntimeToolSettlementDeclarationSchema.parse({
		toolUseEventId,
		outcome,
	});
	const runtime = new ThreadRuntime("composition_session");
	runtime.state.contextManager.replaceMessages(base.messages);
	runtime.state.installThreadCheckpoint(checkpoint);
	runtime.state.installCurrentRequestMessage(base.currentRequestMessage ?? undefined);
	const providerRequests: LLMRequest[] = [];
	const layer = runtimeThreadLoopLayer(
		new QueuedContextLoader([], []),
		hotRuntimeOptions({
			runTool: async () => toolExecutionResult(settlement.outcome),
			stream: (request) => {
				providerRequests.push(request);
				return Stream.fromIterable([
					{
						type: "text-complete" as const,
						providerPartId: "composition-text",
						eventId: "evt_d3fe1c00a9116fea3e6865ae1f59aaf9",
						text: "done",
					},
					{ type: "finish" as const, finishReason: "stop" as const },
				]);
			},
		}),
	);
	const result = await Effect.runPromise(
		Effect.gen(function* () {
			const threadLoop = yield* ThreadLoop.Service;
			runtime.state.markPersistentContextLoaded();
			threadLoop.seedRuntimeModel(runtime);
			if ((base.pendingToolUses?.length ?? 0) > 0) {
				const installed = yield* threadLoop.installLoadedPendingToolUses(
					runtime,
					base.pendingToolUses ?? [],
					base.messages,
				);
				if (!installed.ok) throw new Error("hot pending Tool installation failed");
			}
			if ((base.pendingSandboxExecutions?.length ?? 0) > 0) {
				const installed = yield* threadLoop.installLoadedSandboxExecutions(
					runtime,
					base.pendingSandboxExecutions ?? [],
					base.messages,
				);
				if (!installed.ok) throw new Error("hot Sandbox installation failed");
			}
			runtime.state.installThreadTurn(checkpoint, routes);
			return yield* threadLoop.run(runtime, testRunCustody());
		}).pipe(Effect.provide(layer)),
	);
	if (result.type !== "completed" || providerRequests.length !== 1) {
		throw new Error(
			`hot ThreadLoop did not settle once before the provider request: ${result.type}/${providerRequests.length}`,
		);
	}
	return providerRequests[0]?.context ?? [];
}

// providerContext projects restored entries, after the Thread's prefix when
// it has one, to the context the Runtime sends the Gateway.
export function providerContext(
	entries: readonly RuntimeContextEntry[],
	prefixEntries?: readonly RuntimeContextEntry[],
): LLMRequest["context"] {
	const projected = toGatewayProviderContextSegments(
		prefixEntries === undefined ? [entries] : [prefixEntries, entries],
	);
	if (!projected.ok) {
		throw new Error(`Runtime provider projection failed: ${projected.error.code}`);
	}
	return [...projected.context];
}

// providerRequest assembles the request the Runtime would dispatch for the
// context. The Gateway's tests set the model of each provider they hold.
export function providerRequest(
	context: LLMRequest["context"],
	thread: Awaited<ReturnType<typeof loadContext>>["thread"],
): ProviderRequest {
	const assembled = assembleProviderCallRequest({
		identity: {
			...command,
			threadRole: thread?.role,
			threadVisibility: thread?.visibility,
			runtimeBindingToken: "composition-binding-token",
		},
		requestId: "req_provider_composition",
		modelRequestId: "mreq_provider_composition",
		currentModel: { providerId: "anthropic", modelId: "claude-opus-4-8" },
		providerContext: context,
		runtime: { systemInstructions: "provider composition", timeoutMs: 30_000 },
	});
	if (!assembled.ok) throw new Error("Runtime ProviderRequest assembly failed");
	// Real dispatch fills this from its committed RequestStart acknowledgement.
	return { ...assembled.request, modelRequestStartEventId: "evt_1000000000000000" };
}

function productionRuntimeOptions(options: {
	readonly runTool: NonNullable<ThreadLoop.ThreadLoopRuntimeOptions["runTool"]>;
	readonly stream: LLMServiceInterface["stream"];
}): ThreadLoop.ThreadLoopRuntimeOptions {
	let nextId = 0;
	return {
		internalToolRepairStore: {} as never,
		sessionEventWriter: compositionWriter(),
		runtime: {
			now: () => now,
			monotonicMs: () => 0,
			createId: (prefix) => `${prefix}_composition_${++nextId}`,
			sleep: async (_delayMs, signal) => !signal.aborted,
		},
		llmService: { stream: options.stream },
		storeOperationTimeoutMs: 5_000,
		approvalMode: "full_access",
		providerCallRuntime: {
			...DefaultProviderCallRuntimeConfig,
			systemInstructions: "hot/cold Tool composition",
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
			now: () => now,
			monotonicMs: () => 0,
			createId: (prefix) => `${prefix}_composition_${++nextId}`,
			sleep: async (_delayMs, signal) => !signal.aborted,
		},
		llmService: { stream: options.stream },
		providerCallRuntime: {
			...DefaultProviderCallRuntimeConfig,
			systemInstructions: "hot/cold Tool composition",
			timeoutMs: 5_000,
			toolCatalog: createToolCatalog({ family: "claude" }),
		},
		runTool: options.runTool,
	};
}

function compositionWriter() {
	return writerFrom(
		(envelope) => ({ ok: true, type: "committed", eventId: `bridge-${envelope.writeId}` }),
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
