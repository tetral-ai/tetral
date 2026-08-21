import { access, readFile, writeFile } from "node:fs/promises";
import { Metadata } from "@grpc/grpc-js";
import { createLLMService } from "@tetral/agent-runtime-core/src/llm/llm-service.js";
import type { SessionEventWriter } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import { createToolCatalog } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import { DefaultProviderCallRuntimeConfig } from "@tetral/agent-runtime-core/src/thread-loop/provider-request.js";
import {
	ProviderFinishReason,
	ProviderStreamEventType,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { createGatewayGrpcServer } from "../../../../../gateway/packages/provider-gateway/src/grpc-server.js";
import { ProviderClientRegistry } from "../../../../../gateway/packages/provider-gateway/src/providers/clients.js";
import type {
	GatewayStreamTextInput,
	GatewayStreamTextResult,
} from "../../../../../gateway/packages/provider-gateway/src/providers/clients.js";
import { ProviderCredentialResolver } from "../../../../../gateway/packages/provider-gateway/src/providers/credentials.js";
import { encryptAES256GCM } from "../../../../../gateway/packages/provider-gateway/src/providers/crypto.js";
import { PlatformKeyPool } from "../../../../../gateway/packages/provider-gateway/src/providers/pool.js";
import { ProviderGatewayServiceShell } from "../../../../../gateway/packages/provider-gateway/src/service.js";
import type { ProviderRequestStreamInput } from "../../../../../gateway/packages/provider-gateway/src/service.js";
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

const inputPath = process.argv[2];
if (inputPath === undefined) {
	throw new Error("provider failure production composition input is required");
}
const input = JSON.parse(await readFile(inputPath, "utf8")) as {
	readonly bridgeAddress: string;
	readonly workspaceId: string;
	readonly sessionId: string;
	readonly sessionThreadId: string;
	readonly bindingId: string;
	readonly bindingGeneration: number;
	readonly targetPodUid: string;
	readonly readyPath: string;
	readonly statePath: string;
	readonly closePath: string;
	readonly scenario?:
		| "semantic_timeout"
		| "platform_billing_pre_progress"
		| "platform_billing_post_progress"
		| "platform_billing_exhausted"
		| "statusless_transport"
		| "invalid_byok";
};
const scenario = input.scenario ?? "semantic_timeout";

const metadataFactory = async () => new Metadata();
const bridgeOptions = {
	address: input.bridgeAddress,
	tokenPath: "/unused/service-account-token",
	metadataFactory,
};
const bridgeLoader = new BridgeAPIContextLoader(bridgeOptions);
const bridgeWriter = new BridgeAPIEventWriter(bridgeOptions);
const credentialMasterKeyHex = "0".repeat(64);
const badPlatformKey = {
	keyId: "pfk_provider_failure_bad",
	providerId: "anthropic" as const,
	key: "sk-provider-failure-bad",
	weight: 1,
	priority: 0,
	cacheScope: "provider-failure-composition",
};
const healthyPlatformKey = {
	...badPlatformKey,
	keyId: "pfk_provider_failure_healthy",
	key: "sk-provider-failure-healthy",
};
const platformKeySelections: string[] = [];
const platformKeyQuarantines: string[] = [];
const platformPool = new PlatformKeyPool(
	scenario === "platform_billing_pre_progress" || scenario === "platform_billing_post_progress"
		? [badPlatformKey, healthyPlatformKey]
		: [badPlatformKey],
	{
		random: () => 0,
		onQuarantine: (event) => platformKeyQuarantines.push(event.keyId),
	},
);
let sessionCredentialHealthy = false;
const encryptSessionAuth = async (token: string): Promise<Uint8Array> =>
	await encryptAES256GCM(
		new TextEncoder().encode(
			JSON.stringify({
				type: "provider_api_key",
				provider_id: "anthropic",
				access_mode: "user_api_key",
				token,
			}),
		),
		credentialMasterKeyHex,
		() => new Uint8Array(12).fill(token === "sk-byok-invalid" ? 3 : 7),
	);
const invalidSessionAuth = await encryptSessionAuth("sk-byok-invalid");
const healthySessionAuth = await encryptSessionAuth("sk-byok-healthy");
let providerInvocations = 0;
let finishIdleInvocations = 0;
let finishIdleResult = "none";
let nextId = 0;
const gatewayLogs: unknown[] = [];
const writeRuntimeState = async (): Promise<void> => {
	await writeFile(
		input.statePath,
		JSON.stringify({
			providerInvocations,
			finishIdleInvocations,
			finishIdleResult,
			platformKeySelections,
			platformKeyQuarantines,
			sensitiveLogLeak:
				/private-billing-canary|statusless-private-canary|private-byok-canary|sk-provider-failure|sk-byok/i.test(
					JSON.stringify(gatewayLogs),
				),
		}),
		{ mode: 0o600 },
	);
};
const writer = {
	append: bridgeWriter.append.bind(bridgeWriter),
	settleToolResult: bridgeWriter.settleToolResult.bind(bridgeWriter),
	writeRequestEnd: bridgeWriter.writeRequestEnd.bind(bridgeWriter),
	finishIdle: async (envelope) => {
		finishIdleInvocations += 1;
		await writeRuntimeState();
		const result = await bridgeWriter.finishIdle(envelope);
		finishIdleResult = result.ok ? result.type : result.error.code;
		if (result.ok) {
			if (scenario === "invalid_byok" && finishIdleInvocations === 1) {
				sessionCredentialHealthy = true;
			}
		}
		await writeRuntimeState();
		return result;
	},
	commitRuntimeTermination:
		bridgeWriter.commitRuntimeTermination.bind(bridgeWriter),
} satisfies SessionEventWriter;

const credentialResolver = new ProviderCredentialResolver({
	store: {
		loadActiveSessionProviderAuth: async () =>
			scenario === "invalid_byok"
				? [
						{
							providerId: "anthropic" as const,
							vaultId: "vlt_provider_failure",
							credentialId: "cred_provider_failure",
							accessMode: "user_api_key",
							credentialAuthType: "provider_api_key" as const,
							credentialProviderId: "anthropic",
							credentialAccessMode: "user_api_key",
							encryptedAuth: sessionCredentialHealthy
								? healthySessionAuth
								: invalidSessionAuth,
							archived: false,
							revoked: false,
						},
					]
				: [],
		loadPlatformProviderKeyRows: async () => [],
	},
	platformPool: {
		select: async (providerId, options) =>
			platformPool.select(providerId, options),
		recordFailure: (keyId, classification) =>
			platformPool.recordFailure(keyId, classification),
	},
	masterKeyHex: credentialMasterKeyHex,
});
const streamTextResult = (
	parts: readonly GatewaySDKStreamPart[],
): GatewayStreamTextResult => ({
	fullStream: (async function* () {
		for (const part of parts) yield part;
	})(),
});
type GatewaySDKStreamPart = GatewayStreamTextResult["fullStream"] extends AsyncIterable<
	infer Part
>
	? Part
	: never;
const successProviderParts = (): readonly GatewaySDKStreamPart[] => [
	{ type: "text-start", id: "recovered" },
	{ type: "text-delta", id: "recovered", text: "recovered input" },
	{ type: "text-end", id: "recovered" },
	{
		type: "finish",
		finishReason: "stop",
		rawFinishReason: undefined,
		totalUsage: {
			inputTokens: 1,
			inputTokenDetails: {
				noCacheTokens: 1,
				cacheReadTokens: undefined,
				cacheWriteTokens: undefined,
			},
			outputTokens: 1,
			outputTokenDetails: {
				textTokens: 1,
				reasoningTokens: undefined,
			},
			totalTokens: 2,
		},
	},
];
const providerClientRegistry = new ProviderClientRegistry({
	anthropicProviderFactory: (settings) => (modelId) => ({
		provider: "anthropic",
		modelId,
		apiKey: settings.apiKey,
	}),
	streamText: (request: GatewayStreamTextInput) => {
		providerInvocations += 1;
		void writeRuntimeState();
		const apiKey = (request.model as { readonly apiKey?: string }).apiKey;
		if (apiKey === badPlatformKey.key && scenario.startsWith("platform_billing_")) {
			platformKeySelections.push(badPlatformKey.keyId);
			const error = {
				type: "error" as const,
				error: {
					statusCode: 400,
					data: {
						error: {
							type: "invalid_request_error",
							message:
								"Your credit balance is too low. private-billing-canary",
						},
					},
				},
			};
			if (scenario === "platform_billing_post_progress") {
				return streamTextResult([
					{ type: "text-start", id: "partial" },
					{ type: "text-delta", id: "partial", text: "committed partial" },
					error,
				]);
			}
			return streamTextResult([
				error,
			]);
		}
		if (apiKey === healthyPlatformKey.key && scenario.startsWith("platform_billing_")) {
			platformKeySelections.push(healthyPlatformKey.keyId);
		}
		if (scenario === "statusless_transport" && providerInvocations <= 2) {
			return streamTextResult([
				{
					type: "error",
					error: { opaque: "statusless-private-canary" },
				},
			]);
		}
		if (scenario === "invalid_byok" && apiKey === "sk-byok-invalid") {
			return streamTextResult([
				{
					type: "error",
					error: {
						statusCode: 401,
						data: {
							error: {
								type: "unknown_private_auth_code",
								message: "invalid credential private-byok-canary",
							},
						},
					},
				},
			]);
		}
		return streamTextResult(successProviderParts());
	},
});
const semanticTimeoutStreamer = {
	stream: async function* (request: ProviderRequestStreamInput) {
		providerInvocations += 1;
		await writeRuntimeState();
		if (providerInvocations <= 2) {
			for (let index = 0; index < 40; index += 1) {
				request.onTransportActivity?.();
				await new Promise((resolve) => setTimeout(resolve, 5));
			}
			return;
		}
		yield {
			type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START,
			text: { id: "recovered", text: "", metadataJson: "{}" },
		};
		yield {
			type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA,
			text: { id: "recovered", text: "recovered input", metadataJson: "{}" },
		};
		yield {
			type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_END,
			text: { id: "recovered", text: "", metadataJson: "{}" },
		};
		yield {
			type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
			finish: {
				reason: ProviderFinishReason.PROVIDER_FINISH_REASON_STOP,
				contextWindowTokens: 200_000,
				outputTokenLimit: 32_000,
				usage: {
					inputTotalTokens: 1,
					inputUncachedTokens: 1,
					outputTotalTokens: 1,
					totalTokens: 2,
					providerUsageJson: "{}",
				},
				metadataJson: "{}",
			},
		};
	},
};
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
	ready: () => true,
	logger: {
		info: (record) => gatewayLogs.push(record),
		error: (record) => gatewayLogs.push(record),
	},
	credentialResolver,
	providerStreamTimeouts: {
		firstByteTimeoutMs: 500,
		interChunkTimeoutMs: 100,
		semanticProgressTimeoutMs: 40,
	},
	providerStreamer:
		scenario === "semantic_timeout"
			? semanticTimeoutStreamer
			: providerClientRegistry,
});
const gatewayServer = createGatewayGrpcServer(gatewayService);
const gatewayPort = await gatewayServer.bind("127.0.0.1:0");
const gatewayClient = new RuntimePodGatewayClient({
	address: `127.0.0.1:${gatewayPort}`,
	tokenPath: "/unused/service-account-token",
	metadataFactory,
});
const hosts = await buildRuntimeCoreHosts({
	maxLocalSessions: 1,
	now: () => new Date().toISOString(),
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
			now: () => new Date().toISOString(),
			monotonicMs: () => performance.now(),
			createId: (prefix) => `${prefix}_provider_timeout_${++nextId}`,
			sleep: async (durationMs, signal) =>
				await new Promise<boolean>((resolve) => {
					const timer = setTimeout(() => resolve(true), durationMs);
					const abort = () => {
						clearTimeout(timer);
						resolve(false);
					};
					if (signal.aborted) {
						abort();
						return;
					}
					signal.addEventListener("abort", abort, { once: true });
				}),
		},
		llmService: createLLMService(gatewayClient),
		storeOperationTimeoutMs: 5_000,
		approvalMode: "full_access",
		providerCallRuntime: {
			...DefaultProviderCallRuntimeConfig,
			systemInstructions: "Provider timeout production composition.",
			timeoutMs: 5_000,
		},
		runtimeModel: () => ({ providerId: "anthropic", modelId: "claude-opus-4-8" }),
		runtimePolicy: () => ({
			toolCatalog: createToolCatalog({ family: "claude" }),
			providerRescheduleBudget: 1,
		}),
	},
});
const cleanupController = {
	startCleanup: async () => {
		throw new Error("unexpected cleanup command");
	},
} satisfies RuntimeCleanupController;
const controlInputCommitter = new BridgeAPIControlInputCommitter(bridgeOptions);
const runtimeService = new RuntimeControlService({
	ownPod: {
		namespace: "tetral-agent-runtime",
		name: "runtime-pod-provider-timeout",
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
	runHost: hosts.commandRunHost,
	controlInputCommitter,
	cleanupController,
	logger: { info: () => undefined, warn: () => undefined, error: () => undefined } as never,
	ready: () => true,
});
const runtimeServer = createRuntimeGrpcServer(runtimeService);
const runtimePort = await runtimeServer.bind("127.0.0.1:0");
await writeFile(input.readyPath, JSON.stringify({ port: runtimePort }), {
	mode: 0o600,
});

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
		JSON.stringify({
			providerInvocations,
			finishIdleInvocations,
			finishIdleResult,
			platformKeySelections,
			platformKeyQuarantines,
			sensitiveLogLeak:
				/private-billing-canary|statusless-private-canary|private-byok-canary|sk-provider-failure|sk-byok/i.test(
					JSON.stringify(gatewayLogs),
				),
		}),
	);
} finally {
	await hosts.shutdownActiveRuns();
	await runtimeServer.shutdown();
	await hosts.close();
	await gatewayServer.shutdown();
}
