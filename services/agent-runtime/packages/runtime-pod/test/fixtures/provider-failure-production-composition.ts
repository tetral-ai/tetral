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
import {
	ProviderCredentialResolver,
	SQLGatewayCredentialStore,
} from "../../../../../gateway/packages/provider-gateway/src/providers/credentials.js";
import { SQLOpenAIOAuthCredentialRefreshWriter } from "../../../../../gateway/packages/provider-gateway/src/providers/openai-oauth-refresh.js";
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
	readonly toolReleasePath?: string;
	readonly scenario?:
		| "semantic_timeout"
		| "semantic_tool_route"
		| "platform_billing_pre_progress"
		| "platform_billing_post_progress"
		| "platform_billing_exhausted"
		| "statusless_transport"
		| "provider_rate_limited"
		| "invalid_kimi_byok"
		| "invalid_openai_oauth"
		| "missing_kimi_credential"
		| "unavailable_openai_credential";
};
const scenario = input.scenario ?? "semantic_timeout";
const customerCredentialScenario = scenario === "invalid_kimi_byok";
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
let providerInvocations = 0;
let toolInvocations = 0;
let finishIdleInvocations = 0;
let finishIdleResult = "none";
let oauthAccessTokenConsumed = false;
const providerRequestContexts: string[] = [];
let nextId = input.bindingGeneration * 1_000;
const gatewayLogs: unknown[] = [];
const runtimeLogs: unknown[] = [];
const writeRuntimeState = async (): Promise<void> => {
	await writeFile(
		input.statePath,
		JSON.stringify({
			providerInvocations,
			toolInvocations,
			finishIdleInvocations,
			finishIdleResult,
			platformKeySelections,
			platformKeyQuarantines,
			oauthAccessTokenConsumed,
			providerRequestContexts,
			sensitiveLogLeak:
				/private-billing-canary|statusless-private-canary|private-byok-canary|provider-failure-canary|credential-unavailable-canary|session-key|oauth-access|oauth-refresh|sk-provider-failure/i.test(
					JSON.stringify({ gatewayLogs, runtimeLogs, providerRequestContexts }),
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
		await writeRuntimeState();
		return result;
	},
	commitRuntimeTermination:
		bridgeWriter.commitRuntimeTermination.bind(bridgeWriter),
} satisfies SessionEventWriter;

const databaseURL = process.env.TETRAL_TEST_DATABASE_URL;
const databaseSchema = process.env.TETRAL_TEST_DATABASE_SCHEMA;
if (databaseURL === undefined || databaseSchema === undefined) {
	throw new Error("provider failure composition requires its PostgreSQL schema");
}
const credentialSQL = new Bun.SQL({ url: databaseURL, max: 1 });
await credentialSQL.unsafe(`SET search_path TO ${databaseSchema}`);
const credentialResolver = new ProviderCredentialResolver({
	store: new SQLGatewayCredentialStore(credentialSQL),
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
const providerFetch = Object.assign(
	async (input: RequestInfo | URL, init?: RequestInit) => {
		const request = new Request(input, init);
		if (scenario === "invalid_openai_oauth") {
			oauthAccessTokenConsumed =
				request.headers.get("authorization") === "Bearer oauth-access-healthy";
			await writeRuntimeState();
			if (!oauthAccessTokenConsumed) {
				return new Response('{"error":"invalid_token"}', { status: 401 });
			}
		}
		return new Response("{}", { status: 200 });
	},
	{ preconnect: () => {} },
);
const providerClientRegistry = new ProviderClientRegistry({
	fetch: providerFetch,
	openAIOAuthCredentialRefreshWriter: new SQLOpenAIOAuthCredentialRefreshWriter({
		sql: credentialSQL,
		masterKeyHex: credentialMasterKeyHex,
		fetch: Object.assign(
			async () =>
				new Response('{"error":"invalid_grant"}', {
					status: 400,
					headers: { "content-type": "application/json" },
				}),
			{ preconnect: () => {} },
		),
	}),
	anthropicProviderFactory: (settings) => (modelId) => ({
		provider: "anthropic",
		modelId,
		apiKey: settings.apiKey,
	}),
	openAIProviderFactory: (settings) => ({
		responses: (modelId) => ({
			provider: "openai",
			modelId,
			apiKey: settings.apiKey,
			fetch: settings.fetch,
		}),
	}),
	streamText: (request: GatewayStreamTextInput) => {
		providerInvocations += 1;
		providerRequestContexts.push(JSON.stringify(request.messages));
		void writeRuntimeState();
		const apiKey = (request.model as { readonly apiKey?: string }).apiKey;
		if (scenario === "invalid_openai_oauth") {
			const oauthFetch = (request.model as { readonly fetch?: typeof providerFetch }).fetch;
			return {
				fullStream: (async function* () {
					if (oauthFetch === undefined) {
						throw new Error("OpenAI OAuth fetch wrapper is missing");
					}
					const response = await oauthFetch("https://api.openai.com/v1/responses", {
						method: "POST",
						headers: { authorization: "Bearer sdk-placeholder" },
						body: "{}",
					});
					if (!response.ok) {
						throw new Error("repaired OpenAI OAuth credential was not consumed");
					}
					for (const part of successProviderParts()) yield part;
				})(),
			};
		}
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
		if (scenario === "provider_rate_limited" && providerInvocations === 1) {
			return streamTextResult([
				{ type: "text-start", id: "rate-limited-partial" },
				{ type: "text-delta", id: "rate-limited-partial", text: "committed before rate limit" },
				{ type: "text-end", id: "rate-limited-partial" },
				{
					type: "error",
					error: {
						statusCode: 429,
						data: {
							error: {
								code: "rate_limit_exceeded",
								type: "rate_limit_error",
								message: "rate limited provider-failure-canary",
							},
						},
					},
				},
			]);
		}
		if (customerCredentialScenario && providerInvocations === 1) {
			return streamTextResult([
				{
					type: "error",
					error: {
						statusCode: 401,
						data: {
							error: {
								code: "invalid_authentication_error",
								type: "invalid_authentication_error",
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
		providerRequestContexts.push(JSON.stringify(request.request.context));
		await writeRuntimeState();
		if (scenario === "semantic_tool_route" && providerInvocations === 1) {
			yield {
				type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL,
				toolCall: {
					id: "call_semantic_tool_route",
					name: "Read",
					inputJson: '{"file_path":"/workspace/input.txt"}',
					metadataJson: "{}",
				},
			};
			yield {
				type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
				finish: {
					reason: ProviderFinishReason.PROVIDER_FINISH_REASON_TOOL_CALLS,
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
			return;
		}
		if (
			(scenario === "semantic_tool_route" && providerInvocations === 2) ||
			(scenario !== "semantic_tool_route" && providerInvocations <= 2)
		) {
			yield {
				type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START,
				text: { id: `failed-partial-${providerInvocations}`, text: "", metadataJson: "{}" },
			};
			yield {
				type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA,
				text: {
					id: `failed-partial-${providerInvocations}`,
					text: `failed partial ${providerInvocations}`,
					metadataJson: "{}",
				},
			};
			yield {
				type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_END,
				text: { id: `failed-partial-${providerInvocations}`, text: "", metadataJson: "{}" },
			};
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
		scenario === "semantic_timeout" || scenario === "semantic_tool_route"
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
	logger: {
		info: (record: unknown) => runtimeLogs.push(record),
		warn: (record: unknown) => runtimeLogs.push(record),
		error: (record: unknown) => runtimeLogs.push(record),
	} as never,
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
		runtimeModel: () =>
			scenario === "invalid_kimi_byok" || scenario === "missing_kimi_credential"
				? { providerId: "moonshotai", modelId: "kimi-k3" }
				: scenario === "invalid_openai_oauth" ||
						scenario === "unavailable_openai_credential"
					? { providerId: "openai", modelId: "gpt-5.5" }
					: { providerId: "anthropic", modelId: "claude-opus-4-8" },
		runtimePolicy: () => ({
			toolCatalog: createToolCatalog({ family: "claude" }),
			providerRescheduleBudget: 1,
		}),
		...(scenario === "semantic_tool_route"
			? {
					acceptSandboxExecution: async () => ({ type: "accepted" as const }),
					awaitSandboxExecution: async () => {
						toolInvocations += 1;
						await writeRuntimeState();
						if (input.toolReleasePath === undefined) {
							throw new Error("semantic Tool route release path is required");
						}
						for (;;) {
							try {
								await access(input.toolReleasePath);
								break;
							} catch {
								await new Promise((resolve) => setTimeout(resolve, 10));
							}
						}
						return {
							type: "completed" as const,
							output: { text: "semantic tool result", truncated: false },
						};
					},
				}
			: {}),
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
	logger: {
		info: (record: unknown) => runtimeLogs.push(record),
		warn: (record: unknown) => runtimeLogs.push(record),
		error: (record: unknown) => runtimeLogs.push(record),
	} as never,
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
			toolInvocations,
			finishIdleInvocations,
			finishIdleResult,
			platformKeySelections,
			platformKeyQuarantines,
			oauthAccessTokenConsumed,
			providerRequestContexts,
			sensitiveLogLeak:
				/private-billing-canary|statusless-private-canary|private-byok-canary|provider-failure-canary|credential-unavailable-canary|session-key|oauth-access|oauth-refresh|sk-provider-failure/i.test(
					JSON.stringify({ gatewayLogs, runtimeLogs, providerRequestContexts }),
				),
		}),
	);
} finally {
	await hosts.shutdownActiveRuns();
	await runtimeServer.shutdown();
	await hosts.close();
	await gatewayServer.shutdown();
	await credentialSQL.close();
}
