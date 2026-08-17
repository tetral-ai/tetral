import { access, readFile, writeFile } from "node:fs/promises";
import { Metadata } from "@grpc/grpc-js";
import { createToolCatalog } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import { DefaultProviderCallRuntimeConfig } from "@tetral/agent-runtime-core/src/thread-loop/provider-request.js";
import { extractThreadTurnCheckpoint } from "@tetral/agent-runtime-core/src/thread-loop/thread-turn-checkpoint.js";
import { Effect, Stream } from "effect";
import {
	BridgeAPIContextLoader,
	BridgeAPIControlInputCommitter,
	BridgeAPIEventWriter,
} from "../../src/bridge-client.js";
import { buildRuntimeCoreHosts } from "../../src/core-hosts.js";
import { createRuntimeGrpcServer } from "../../src/grpc-server.js";
import type { RuntimeCleanupController } from "../../src/runtime-service.js";
import { RuntimeControlService } from "../../src/runtime-service.js";

const inputPath = process.argv[2];
if (inputPath === undefined) {
	throw new Error("attachment recovery composition input path is required");
}

const input = JSON.parse(await readFile(inputPath, "utf8")) as {
	readonly mode: "complete" | "hang" | "cold";
	readonly bridgeAddress: string;
	readonly workspaceId: string;
	readonly sessionId: string;
	readonly sessionThreadId: string;
	readonly bindingId: string;
	readonly bindingGeneration: number;
	readonly targetPodUid: string;
	readonly fileId?: string;
	readonly readyPath?: string;
	readonly acceptResultPath?: string;
	readonly inspectResultPath?: string;
	readonly providerStartedPath?: string;
	readonly closePath?: string;
};

const metadataFactory = async () => new Metadata();
const bridgeLoader = new BridgeAPIContextLoader({
	address: input.bridgeAddress,
	tokenPath: "/unused/service-account-token",
	metadataFactory,
});
const writer = new BridgeAPIEventWriter({
	address: input.bridgeAddress,
	tokenPath: "/unused/service-account-token",
	metadataFactory,
});
let nextId = 0;
let providerInvocations = 0;
const providerAttachmentCounts: number[] = [];
const signalProviderStart = async (attachmentCount: number): Promise<void> => {
	if (input.providerStartedPath === undefined) return;
	await writeFile(
		input.providerStartedPath,
		JSON.stringify({ providerInvocations, attachmentCount }),
		{ mode: 0o600 },
	);
};
const hosts = await buildRuntimeCoreHosts({
	maxLocalSessions: 2,
	now: () => "2026-08-17T00:00:00.000Z",
	contextLoader: {
		loadThreadContext: bridgeLoader.loadThreadContext.bind(bridgeLoader),
		commitAcceptedInput: bridgeLoader.commitAcceptedInput.bind(bridgeLoader),
		readAgentMail: bridgeLoader.readAgentMail.bind(bridgeLoader),
		refreshRuntimeBindingToken: async (identity) => identity.runtimeBindingToken,
	},
	threadLoop: {
		internalToolRepairStore: {} as never,
		sessionEventWriter: writer,
		runtime: {
			now: () => "2026-08-17T00:00:00.000Z",
			monotonicMs: () => 0,
			createId: (prefix) => `${prefix}_attachment_recovery_${++nextId}`,
			sleep: async (durationMs, signal) => {
				if (signal.aborted) return false;
				await new Promise((resolve) => setTimeout(resolve, durationMs));
				return !signal.aborted;
			},
		},
		llmService: {
			stream: (request) => {
				providerInvocations += 1;
				providerAttachmentCounts.push(request.attachments.length);
				const started = Stream.fromEffect(
					Effect.promise(() => signalProviderStart(request.attachments.length)),
				).pipe(Stream.drain);
				if (input.mode === "hang") {
					return Stream.concat(started, Stream.never);
				}
				return Stream.concat(
					started,
					Stream.fromIterable([
						{ type: "text-start" as const, id: "attachment-answer" },
						{
							type: "text-delta" as const,
							id: "attachment-answer",
							text_delta: "attachment received",
						},
						{ type: "text-end" as const, id: "attachment-answer" },
						{ type: "finish" as const, finishReason: "stop" as const },
					]),
				);
			},
		},
		acceptSandboxExecution: async () => ({ type: "accepted" as const }),
		awaitSandboxExecution: async () => ({ type: "cancelled" as const }),
		storeOperationTimeoutMs: 5_000,
		approvalMode: "full_access",
		providerCallRuntime: {
			...DefaultProviderCallRuntimeConfig,
			systemInstructions: "Attachment recovery composition.",
			timeoutMs: 5_000,
		},
		runtimeModel: () => ({ providerId: "fake", modelId: "fake-chat" }),
		runtimePolicy: () => ({ toolCatalog: createToolCatalog({ family: "claude" }) }),
	},
});

const address = {
	workspaceId: input.workspaceId,
	sessionId: input.sessionId,
	sessionThreadId: input.sessionThreadId,
	bindingId: input.bindingId,
	bindingGeneration: input.bindingGeneration,
	targetPodUid: input.targetPodUid,
};

if (input.mode === "cold") {
	try {
		const loaded = await bridgeLoader.loadThreadContext(address);
		const checkpoint = extractThreadTurnCheckpoint({
			contextEntries: loaded.contextEntries,
			facts: loaded.turnFacts,
		});
		const preloadResult = await hosts.subAgentRunHost.preloadThread(address);
		process.stdout.write(
			JSON.stringify({
				pendingAttachments: loaded.pendingAttachments?.length ?? 0,
				attachmentInMessageOrCheckpoint:
					input.fileId !== undefined &&
					JSON.stringify({ contextEntries: loaded.contextEntries, checkpoint }).includes(input.fileId),
				providerInvocations,
				preloadResult,
			}),
		);
	} finally {
		await hosts.shutdownActiveRuns();
		await hosts.close();
	}
} else {
	if (
		input.readyPath === undefined ||
		input.acceptResultPath === undefined ||
		input.inspectResultPath === undefined ||
		input.providerStartedPath === undefined ||
		input.closePath === undefined
	) {
		throw new Error("serving attachment composition paths are required");
	}
	const cleanupController = {
		startCleanup: async () => {
			throw new Error("unexpected cleanup command");
		},
	} satisfies RuntimeCleanupController;
	const controlInputCommitter = new BridgeAPIControlInputCommitter({
		address: input.bridgeAddress,
		tokenPath: "/unused/service-account-token",
		metadataFactory,
	});
	const service = new RuntimeControlService({
		ownPod: {
			namespace: "tetral-agent-runtime",
			name: "runtime-pod-attachment-recovery",
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
			handleAcceptInput: async (...args) => {
				const result = await hosts.commandRunHost.handleAcceptInput(...args);
				await writeFile(input.acceptResultPath!, JSON.stringify(result), { mode: 0o600 });
				setTimeout(() => {
					void hosts.subAgentRunHost.inspectThread(address).then(
						(inspected) => writeFile(input.inspectResultPath!, JSON.stringify(inspected), { mode: 0o600 }),
						(error) => writeFile(input.inspectResultPath!, JSON.stringify({ error: String(error) }), { mode: 0o600 }),
					);
				}, 100);
				return result;
			},
		},
		controlInputCommitter,
		cleanupController,
		logger: { info: () => undefined, warn: () => undefined, error: () => undefined } as never,
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
		process.stdout.write(
			JSON.stringify({ providerInvocations, providerAttachmentCounts }),
		);
	} finally {
		await hosts.shutdownActiveRuns();
		await server.shutdown();
		await hosts.close();
	}
}
