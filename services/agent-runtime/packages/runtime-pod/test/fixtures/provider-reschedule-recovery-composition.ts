import { readFile } from "node:fs/promises";
import { Metadata } from "@grpc/grpc-js";
import type { LLMRequest } from "@tetral/agent-runtime-core/src/llm/llm-service.js";
import * as ThreadLoop from "@tetral/agent-runtime-core/src/thread-loop/thread-loop.js";
import { createToolCatalog } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import { DefaultProviderCallRuntimeConfig } from "@tetral/agent-runtime-core/src/thread-loop/provider-request.js";
import {
	extractColdThreadToolRouteView,
	extractThreadTurnCheckpoint,
} from "@tetral/agent-runtime-core/src/thread-loop/thread-turn-checkpoint.js";
import { ThreadRuntime } from "@tetral/agent-runtime-core/src/thread-loop/thread-runtime.js";
import { Effect, Stream } from "effect";
import {
	QueuedContextLoader,
	runtimeThreadLoopLayer,
	testRunCustody,
	ThreadLoopRuntimeStore,
} from "../../../core/test/unit/thread-loop/thread-loop-test-support.js";
import {
	BridgeAPIContextLoader,
	BridgeAPIEventWriter,
} from "../../src/bridge-client.js";
import { buildRuntimeCoreHosts } from "../../src/core-hosts.js";

const inputPath = process.argv[2];
if (inputPath === undefined) {
	throw new Error("provider reschedule recovery input is required");
}
const input = JSON.parse(await readFile(inputPath, "utf8")) as {
	readonly bridgeAddress: string;
	readonly workspaceId: string;
	readonly sessionId: string;
	readonly sessionThreadId: string;
	readonly bindingId: string;
	readonly bindingGeneration: number;
	readonly targetPodUid: string;
	readonly now: string;
	readonly preloadOnly?: boolean;
	readonly terminationWriteId?: string;
	readonly terminationReplayCount?: number;
};
const command = {
	workspaceId: input.workspaceId,
	sessionId: input.sessionId,
	sessionThreadId: input.sessionThreadId,
	bindingId: input.bindingId,
	bindingGeneration: input.bindingGeneration,
	targetPodUid: input.targetPodUid,
};
const bridgeOptions = {
	address: input.bridgeAddress,
	tokenPath: "/unused/test-token",
	metadataFactory: async () => new Metadata(),
};
const loader = new BridgeAPIContextLoader(bridgeOptions);
const providerRequests: LLMRequest[] = [];
const waitedMs: number[] = [];
let providerInvocations = 0;
let executorInvocations = 0;
let nextID = 0;
const writer = new BridgeAPIEventWriter(bridgeOptions);
const hosts = await buildRuntimeCoreHosts({
	maxLocalSessions: 1,
	now: () => input.now,
	contextLoader: {
		loadThreadContext: loader.loadThreadContext.bind(loader),
		refreshRuntimeBindingToken: async (identity) => identity.runtimeBindingToken,
	},
	threadLoop: {
		internalToolRepairStore: {} as never,
		sessionEventWriter: writer,
		runtime: {
					now: () => input.now,
					monotonicMs: () => 0,
					createId: (prefix) => `${prefix}_reschedule_recovery_${++nextID}`,
					sleep: async (delayMs, signal) => {
						if (signal.aborted) return false;
						waitedMs.push(delayMs);
						return true;
					},
				},
		llmService: {
					stream: (request) => {
						providerInvocations += 1;
						providerRequests.push(request);
						return Stream.fromIterable([
							{ type: "text-start" as const, id: "recovered-text" },
							{
								type: "text-delta" as const,
								id: "recovered-text",
								text_delta: "recovered",
							},
							{ type: "text-end" as const, id: "recovered-text" },
							{ type: "finish" as const, finishReason: "stop" as const },
						]);
					},
				},
		storeOperationTimeoutMs: 5_000,
		approvalMode: "full_access",
		providerCallRuntime: {
			...DefaultProviderCallRuntimeConfig,
			systemInstructions: "provider reschedule recovery composition",
		},
		runtimeModel: () => ({ providerId: "fake", modelId: "fake-chat" }),
		runtimePolicy: () => ({ toolCatalog: createToolCatalog({ family: "claude" }) }),
		runTool: () => {
					executorInvocations += 1;
					return {
						type: "completed",
						output: { text: "unexpected replay", truncated: false },
					};
		},
	},
});
let resultType = "failed";
let preloadResult: unknown;
let lastSnapshot: unknown;
let productionEntries: readonly unknown[] = [];
const terminationResults: unknown[] = [];
try {
	const preload = await hosts.subAgentRunHost.preloadThread(command);
	preloadResult = preload;
	const snapshot = await hosts.subAgentRunHost.inspectThread(command);
	lastSnapshot = snapshot;
	if (preload.ok && snapshot.ok) productionEntries = snapshot.entries;
} finally {
	await hosts.shutdownActiveRuns();
	await hosts.close();
}
if (input.terminationWriteId !== undefined) {
	for (let attempt = 0; attempt < (input.terminationReplayCount ?? 1); attempt += 1) {
		terminationResults.push(
			await writer.commitRuntimeTermination({
				...command,
				writeId: input.terminationWriteId,
				failure: {
					type: "runtime",
					code: "runtime_invalid_sequence",
					message: "Runtime operation failed.",
					retryable: false,
					fatal: true,
					retryStatus: { type: "terminal" },
					reason: "runtime_contract_validation",
				},
			}),
		);
	}
}
if (input.preloadOnly === true) {
	process.stdout.write(JSON.stringify({
		resultType: "preloaded",
		providerInvocations,
		executorInvocations,
		waitedMs,
		providerContext: [],
		preloadResult,
		lastSnapshot,
		terminationResults,
	}));
	process.exit(0);
}
const loaded = await loader.loadThreadContext(command);
const checkpoint = extractThreadTurnCheckpoint({
	contextEntries: loaded.contextEntries,
	facts: loaded.turnFacts,
});
const routes = extractColdThreadToolRouteView({
	checkpoint,
	pendingToolUses: loaded.pendingToolUses ?? [],
	pendingSandboxExecutions: loaded.pendingSandboxExecutions ?? [],
});
const session = new ThreadRuntime({
	...command,
	runtimeBindingToken: loaded.runtimeBindingToken,
});
session.state.contextManager.replaceEntries(productionEntries as never);
session.state.markPersistentContextLoaded();
session.state.installThreadTurn(checkpoint, routes);
const result = await Effect.runPromise(
	Effect.gen(function* () {
		return yield* (yield* ThreadLoop.Service).run(session, testRunCustody());
	}).pipe(
		Effect.provide(
			runtimeThreadLoopLayer(new QueuedContextLoader([], []), {
				installLoaderState: false,
				store: new ThreadLoopRuntimeStore([]),
				writer,
				runtime: {
					now: () => input.now,
					monotonicMs: () => 0,
					createId: (prefix) => `${prefix}_reschedule_recovery_run_${++nextID}`,
					sleep: async (delayMs) => {
						waitedMs.push(delayMs);
						return true;
					},
				},
				llmService: {
					stream: (request) => {
						providerInvocations += 1;
						providerRequests.push(request);
						return Stream.fromIterable([
							{ type: "text-start" as const, id: "recovered-text" },
							{ type: "text-delta" as const, id: "recovered-text", text_delta: "recovered" },
							{ type: "text-end" as const, id: "recovered-text" },
							{ type: "finish" as const, finishReason: "stop" as const },
						]);
					},
				},
				providerCallRuntime: { systemInstructions: "provider reschedule recovery composition" },
				runTool: () => {
					executorInvocations += 1;
					return { type: "completed", output: { text: "unexpected replay", truncated: false } };
				},
			}),
		),
	),
);
resultType = result.type;

process.stdout.write(
	JSON.stringify({
		resultType,
		providerInvocations,
		executorInvocations,
		waitedMs,
		providerContext: providerRequests[0]?.context,
		preloadResult,
		lastSnapshot,
		terminationResults,
	}),
);
