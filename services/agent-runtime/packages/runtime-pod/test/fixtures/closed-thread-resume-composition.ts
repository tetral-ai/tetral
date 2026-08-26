import { access, readFile, writeFile } from "node:fs/promises";
import { Metadata } from "@grpc/grpc-js";
import type { RuntimeToolExecutionRequest } from "@tetral/agent-runtime-core/src/thread-loop/tool-execution.js";
import { DefaultProviderCallRuntimeConfig } from "@tetral/agent-runtime-core/src/thread-loop/provider-request.js";
import {
	createToolCatalog,
	lookupToolEntry,
} from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import { Stream } from "effect";
import {
	extractColdThreadToolRouteView,
	extractThreadTurnCheckpoint,
} from "@tetral/agent-runtime-core/src/thread-loop/thread-turn-checkpoint.js";
import { deriveThreadTurnSnapshot } from "@tetral/agent-runtime-core/src/thread-loop/thread-turn-reducer.js";
import {
	BridgeAPIContextLoader,
	BridgeAPIControlInputCommitter,
	BridgeAPIEventWriter,
} from "../../src/bridge-client.js";
import { buildRuntimeCoreHosts } from "../../src/core-hosts.js";
import { createRuntimeGrpcServer } from "../../src/grpc-server.js";
import type { RuntimeCleanupController } from "../../src/runtime-service.js";
import { RuntimeControlService } from "../../src/runtime-service.js";
import { RuntimePodToolRunner } from "../../src/tool-runner.js";

const inputPath = process.argv[2];
if (inputPath === undefined) {
	throw new Error("closed Thread resume composition input path is required");
}
const input = JSON.parse(await readFile(inputPath, "utf8")) as {
	readonly address: string;
	readonly workspaceId: string;
	readonly sessionId: string;
	readonly parentThreadId: string;
	readonly childThreadId: string;
	readonly childTaskName: string;
	readonly bindingId: string;
	readonly bindingGeneration: number;
	readonly targetPodUid: string;
	readonly sourceToolUseEventId: string;
	readonly readyPath?: string;
	readonly resumeResultPath?: string;
	readonly acceptResultPath?: string;
	readonly providerStartedPath?: string;
	readonly closePath?: string;
	readonly providerScenario?: "terminal-tool";
};

const metadataFactory = async () => new Metadata();
const loader = new BridgeAPIContextLoader({
	address: input.address,
	tokenPath: "/unused/service-account-token",
	metadataFactory,
});
let providerRequests = 0;
let runtimeEvents = 0;
let resumeComplete = false;
let nextRuntimeID = 0;
const eventWriter = new BridgeAPIEventWriter({
	address: input.address,
	tokenPath: "/unused/service-account-token",
	metadataFactory,
});
const unexpectedRuntimeWrite = async (): Promise<never> => {
	runtimeEvents += 1;
	throw new Error("closed Thread resume unexpectedly started Runtime work");
};
const guardedRuntimeWrite =
	<TArgs extends readonly unknown[], TResult>(
		write: (...args: TArgs) => Promise<TResult>,
	) =>
	async (...args: TArgs): Promise<TResult> => {
		runtimeEvents += 1;
		if (!resumeComplete) return await unexpectedRuntimeWrite();
		return await write(...args);
	};
let hosts: Awaited<ReturnType<typeof buildRuntimeCoreHosts>> | undefined;
const productionToolRunner = new RuntimePodToolRunner({
	bridgeAddress: input.address,
	webAddress: "127.0.0.1:1",
	mcpConnectorAddress: "127.0.0.1:1",
	tokenPath: "/unused/service-account-token",
	metadataFactory,
	subAgentRunHost: () => {
		if (hosts === undefined) throw new Error("Runtime hosts are unavailable");
		return hosts.subAgentRunHost;
	},
});
hosts = await buildRuntimeCoreHosts({
	maxLocalSessions: 2,
	now: () => "2026-08-16T00:00:00.000Z",
	contextLoader: {
		loadThreadContext: loader.loadThreadContext.bind(loader),
		commitAcceptedInput: async (...args) => {
			if (!resumeComplete)
				throw new Error("closed Thread resume unexpectedly committed input");
			return await loader.commitAcceptedInput(...args);
		},
		readAgentMail: loader.readAgentMail.bind(loader),
		refreshRuntimeBindingToken:
			loader.refreshRuntimeBindingToken.bind(loader),
	},
	threadLoop: {
		internalToolRepairStore: {} as never,
		sessionEventWriter: {
			append: guardedRuntimeWrite(eventWriter.append.bind(eventWriter)),
			writeRequestEnd: guardedRuntimeWrite(
				eventWriter.writeRequestEnd.bind(eventWriter),
			),
			finishIdle: guardedRuntimeWrite(eventWriter.finishIdle.bind(eventWriter)),
			settleToolResult: guardedRuntimeWrite(
				eventWriter.settleToolResult.bind(eventWriter),
			),
		},
		runtime: {
			now: () => "2026-08-16T00:00:00.000Z",
			monotonicMs: () => 0,
			createId: (prefix) =>
				input.providerScenario === "terminal-tool" && prefix === "event_write"
					? `${prefix}_closed_resume_${++nextRuntimeID}`
					: `${prefix}_closed_resume`,
			sleep: async (durationMs, signal) => {
				if (signal.aborted) return false;
				await new Promise((resolve) => setTimeout(resolve, durationMs));
				return !signal.aborted;
			},
		},
		llmService: {
			stream: (request) => {
				providerRequests += 1;
				if (input.providerStartedPath !== undefined) {
					void writeFile(
						input.providerStartedPath,
						JSON.stringify({ providerRequests, request }),
						{ mode: 0o600 },
					);
				}
				if (input.providerScenario === "terminal-tool" && providerRequests === 1) {
					return Stream.fromIterable([
						{ type: "text-start" as const, id: "terminal-tool-text" },
						{
							type: "text-delta" as const,
							id: "terminal-tool-text",
							text_delta: "checking the retained tool result",
						},
						{ type: "text-end" as const, id: "terminal-tool-text" },
						{
							type: "tool-call" as const,
							id: "call_closed_resume_terminal_tool",
							toolName: "memory",
							input: {
								action: "create",
								path: "resume/terminal-tool.md",
								content: "retained terminal tool result",
							},
							inputPreview: {
								preview: '{"action":"create","path":"resume/terminal-tool.md"}',
								truncated: false,
							},
						},
						{ type: "finish" as const, finishReason: "tool-calls" as const },
					]);
				}
				return Stream.fromIterable([
					{ type: "text-start" as const, id: "later-resume-text" },
					{
						type: "text-delta" as const,
						id: "later-resume-text",
						text_delta: "later input completed",
					},
					{ type: "text-end" as const, id: "later-resume-text" },
					{ type: "finish" as const, finishReason: "stop" as const },
				]);
			},
		},
		acceptSandboxExecution: async () => ({ type: "accepted" as const }),
		awaitSandboxExecution: async () => ({ type: "cancelled" as const }),
		...(input.providerScenario === "terminal-tool"
			? { runTool: productionToolRunner.runTool.bind(productionToolRunner) }
			: {}),
		storeOperationTimeoutMs: 5_000,
		approvalMode: "full_access",
		providerCallRuntime: {
			...DefaultProviderCallRuntimeConfig,
			systemInstructions: "Closed resume production composition.",
			timeoutMs: 5_000,
		},
		runtimeModel: () => ({
			providerId: "anthropic",
			modelId: "claude-opus-4-8",
		}),
		runtimePolicy: () => ({
			toolCatalog: createToolCatalog({ family: "claude" }),
		}),
	},
});

try {
	const loaded = await loader.loadThreadContext({
		workspaceId: input.workspaceId,
		sessionId: input.sessionId,
		sessionThreadId: input.childThreadId,
		bindingId: input.bindingId,
		bindingGeneration: input.bindingGeneration,
		targetPodUid: input.targetPodUid,
	});
	const checkpoint = extractThreadTurnCheckpoint({
		contextEntries: loaded.contextEntries,
		facts: loaded.turnFacts,
	});
	const routeView = extractColdThreadToolRouteView({
		checkpoint,
		pendingToolUses: loaded.pendingToolUses ?? [],
		pendingSandboxExecutions: loaded.pendingSandboxExecutions ?? [],
	});
	const decision = deriveThreadTurnSnapshot(checkpoint, routeView, [], {
		hasPendingAttachments: (loaded.pendingAttachments?.length ?? 0) > 0,
	});
	const entry = lookupToolEntry(
		createToolCatalog({ family: "claude" }),
		"resume_agent",
	);
	if (entry === undefined) throw new Error("resume_agent is unavailable");
	const request: RuntimeToolExecutionRequest = {
		workspaceId: input.workspaceId,
		sessionId: input.sessionId,
		sessionThreadId: input.parentThreadId,
		bindingId: input.bindingId,
		bindingGeneration: input.bindingGeneration,
		runtimeBindingToken: loaded.runtimeBindingToken,
		targetPodUid: input.targetPodUid,
		modelRequestId: "mreq_closed_resume_composition",
		modelToolCallId: "call_closed_resume_composition",
		modelOrder: 0,
		toolUseEventId: input.sourceToolUseEventId,
		entry,
		input: { task_name: input.childTaskName },
		retainedContextEntries: [],
		currentModel: { providerId: "anthropic", modelId: "claude-sonnet-4-6" },
		abortSignal: new AbortController().signal,
	};
	const result = await productionToolRunner.runTool(request);
	resumeComplete = true;
	const inspected = await hosts.subAgentRunHost.inspectThread({
		workspaceId: input.workspaceId,
		sessionId: input.sessionId,
		sessionThreadId: input.childThreadId,
		bindingId: input.bindingId,
		bindingGeneration: input.bindingGeneration,
		targetPodUid: input.targetPodUid,
	});
	const resumeResult = {
			result,
			inspected,
			checkpoint,
			decision,
			contextEntries: loaded.contextEntries,
			turnFacts: loaded.turnFacts,
			providerRequests,
			runtimeEvents,
	};
	if (
		input.readyPath === undefined ||
		input.resumeResultPath === undefined ||
		input.acceptResultPath === undefined ||
		input.closePath === undefined
	) {
		process.stdout.write(JSON.stringify(resumeResult));
	} else {
		await writeFile(input.resumeResultPath, JSON.stringify(resumeResult), {
			mode: 0o600,
		});
		const controlInputCommitter = new BridgeAPIControlInputCommitter({
			address: input.address,
			tokenPath: "/unused/service-account-token",
			metadataFactory,
		});
		const cleanupController = {
			startCleanup: async () => {
				throw new Error("unexpected cleanup command");
			},
		} satisfies RuntimeCleanupController;
		const service = new RuntimeControlService({
			ownPod: {
				namespace: "tetral-agent-runtime",
				name: "runtime-pod-closed-resume",
				uid: input.targetPodUid,
				ip: "127.0.0.1",
			},
			allowedBridge: { namespace: "tetral-system", name: "bridge" },
			authenticator: {
				authenticate: async () => ({
					ok: true as const,
					serviceAccount: { namespace: "tetral-system", name: "bridge" },
				}),
			},
			runHost: {
				...hosts.commandRunHost,
				handleAgentMail: async (...args) => {
					const accepted = await hosts.commandRunHost.handleAgentMail(...args);
					await writeFile(
						input.acceptResultPath!,
						JSON.stringify({ result: accepted, providerRequests, runtimeEvents }),
						{ mode: 0o600 },
					);
					setTimeout(() => {
						void hosts.subAgentRunHost
							.inspectThread({
								workspaceId: input.workspaceId,
								sessionId: input.sessionId,
								sessionThreadId: input.childThreadId,
								bindingId: input.bindingId,
								bindingGeneration: input.bindingGeneration,
								targetPodUid: input.targetPodUid,
							})
							.then((afterAccept) =>
								writeFile(
									input.acceptResultPath!,
									JSON.stringify({
										result: accepted,
										providerRequests,
										runtimeEvents,
										afterAccept,
									}),
									{ mode: 0o600 },
								),
							);
					}, 200);
					return accepted;
				},
			},
			controlInputCommitter,
			cleanupController,
			logger: {
				info: () => undefined,
				warn: () => undefined,
				error: () => undefined,
			} as never,
			ready: () => true,
		});
		const server = createRuntimeGrpcServer(service);
		const port = await server.bind("127.0.0.1:0");
		await writeFile(input.readyPath, JSON.stringify({ port }), { mode: 0o600 });
		try {
			for (;;) {
				try {
					await access(input.closePath);
					break;
				} catch {
					await new Promise((resolve) => setTimeout(resolve, 10));
				}
			}
		} finally {
			await server.shutdown();
		}
	}
} finally {
	await hosts.shutdownActiveRuns();
	await hosts.close();
}
