import { access, readFile, writeFile } from "node:fs/promises";
import { Metadata } from "@grpc/grpc-js";
import { createLLMService } from "@tetral/agent-runtime-core/src/llm/llm-service.js";
import { createToolCatalog } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import { DefaultProviderCallRuntimeConfig } from "@tetral/agent-runtime-core/src/thread-loop/provider-request.js";
import { extractThreadTurnCheckpoint } from "@tetral/agent-runtime-core/src/thread-loop/thread-turn-checkpoint.js";
import {
	BridgeAPIContextLoader,
	BridgeAPIControlInputCommitter,
	BridgeAPIEventWriter,
} from "../../src/bridge-client.js";
import { buildRuntimeCoreHosts } from "../../src/core-hosts.js";
import { RuntimePodGatewayClient } from "../../src/gateway-client.js";
import { createRuntimeGrpcServer } from "../../src/grpc-server.js";
import type { RuntimeCleanupController } from "../../src/runtime-service.js";
import { RuntimeControlService } from "../../src/runtime-service.js";
import { BridgeAPIAttachmentResolver } from "../../../../../gateway/packages/provider-gateway/src/attachments.js";
import { createGatewayGrpcServer } from "../../../../../gateway/packages/provider-gateway/src/grpc-server.js";
import { ProviderClientRegistry } from "../../../../../gateway/packages/provider-gateway/src/providers/clients.js";
import { ProviderCredentialResolver } from "../../../../../gateway/packages/provider-gateway/src/providers/credentials.js";
import { ProviderGatewayServiceShell } from "../../../../../gateway/packages/provider-gateway/src/service.js";
import type { GatewayStreamTextInput } from "../../../../../gateway/packages/provider-gateway/src/providers/clients.js";

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
let gatewayRequests = 0;
const providerAttachmentCounts: number[] = [];
const signalProviderStart = async (
	attachmentCount: number,
	attachmentBytes: number,
	providerRequest: GatewayStreamTextInput,
): Promise<void> => {
	if (input.providerStartedPath === undefined) return;
	await writeFile(
		input.providerStartedPath,
		JSON.stringify({
			providerInvocations,
			gatewayRequests,
			attachmentCount,
			loweredAttachmentBytes: attachmentBytes,
			providerRequest,
		}),
		{ mode: 0o600 },
	);
};

const providerStreamer = new ProviderClientRegistry({
	anthropicProviderFactory: () => (modelId) => ({ provider: "anthropic", modelId }),
	streamText: (providerRequest) => {
		gatewayRequests += 1;
		providerInvocations += 1;
		const attachmentFacts = loweredProviderAttachmentFacts(providerRequest);
		providerAttachmentCounts.push(attachmentFacts.count);
		return {
			fullStream: (async function* () {
				await signalProviderStart(
					attachmentFacts.count,
					attachmentFacts.bytes,
					providerRequest,
				);
				if (input.mode === "hang") {
					await new Promise<void>(() => undefined);
					return;
				}
				yield { type: "start" as const };
				yield { type: "text-start" as const, id: "attachment-answer" };
				yield {
					type: "text-delta" as const,
					id: "attachment-answer",
					text: "attachment received",
				};
				yield { type: "text-end" as const, id: "attachment-answer" };
				yield {
					type: "finish" as const,
					finishReason: "stop" as const,
					rawFinishReason: "end_turn",
					totalUsage: {
						inputTokens: 1,
						inputTokenDetails: {
							noCacheTokens: 1,
							cacheReadTokens: undefined,
							cacheWriteTokens: undefined,
						},
						outputTokens: 1,
						outputTokenDetails: { textTokens: 1, reasoningTokens: undefined },
						totalTokens: 2,
					},
				};
			})(),
		};
	},
});
const credentialResolver = new ProviderCredentialResolver({
	store: {
		loadActiveSessionProviderAuth: async () => [],
		loadPlatformProviderKeyRows: async () => [],
	},
	platformPool: {
		select: async () => ({
			ok: true as const,
			key: {
				keyId: "pfk_attachment_recovery",
				providerId: "anthropic" as const,
				key: "attachment-recovery-provider-key",
				weight: 1,
				priority: 0,
				cacheScope: "attachment-recovery",
			},
		}),
	},
	masterKeyHex: "0".repeat(64),
});
const gatewayService = new ProviderGatewayServiceShell({
	authenticator: {
		authenticate: async () => ({
			ok: true as const,
			serviceAccount: {
				namespace: "tetral-agent-runtime",
				name: "agent-runtime",
				podUid: input.targetPodUid,
			},
		}),
	},
	runtimeBindingTokenVerifier: { verify: () => true },
	logger: { info: () => undefined, error: () => undefined },
	ready: () => true,
	providerStreamer,
	credentialResolver,
	attachmentResolver: new BridgeAPIAttachmentResolver({
		address: input.bridgeAddress,
		tokenPath: "/unused/service-account-token",
		metadataFactory,
	}),
});
const gatewayServer = createGatewayGrpcServer(gatewayService);
const gatewayPort = await gatewayServer.bind("127.0.0.1:0");
const gatewayClient = new RuntimePodGatewayClient({
	address: `127.0.0.1:${gatewayPort}`,
	tokenPath: "/unused/service-account-token",
	metadataFactory,
});
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
		llmService: createLLMService(gatewayClient),
		acceptSandboxExecution: async () => ({ type: "accepted" as const }),
		awaitSandboxExecution: async () => ({ type: "cancelled" as const }),
		storeOperationTimeoutMs: 5_000,
		approvalMode: "full_access",
		providerCallRuntime: {
			...DefaultProviderCallRuntimeConfig,
			systemInstructions: "Attachment recovery composition.",
			timeoutMs: 5_000,
		},
		runtimeModel: () => ({ providerId: "anthropic", modelId: "claude-opus-4-8" }),
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
		await gatewayServer.shutdown();
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
		await gatewayServer.shutdown();
	}
}

function loweredProviderAttachmentFacts(input: GatewayStreamTextInput): {
	readonly count: number;
	readonly bytes: number;
} {
	let count = 0;
	let bytes = 0;
	for (const message of input.messages) {
		if (!Array.isArray(message.content)) continue;
		for (const part of message.content) {
			if (part.type === "image" && part.image instanceof Uint8Array) {
				count += 1;
				bytes += part.image.byteLength;
			}
			if (part.type === "file" && part.data instanceof Uint8Array) {
				count += 1;
				bytes += part.data.byteLength;
			}
		}
	}
	return { count, bytes };
}
