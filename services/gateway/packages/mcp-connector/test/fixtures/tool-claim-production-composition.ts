import { createHmac } from "node:crypto";
import { Metadata } from "@grpc/grpc-js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import { createRuntimeBindingTokenVerifier } from "@tetral/gateway-protocol/src/binding-token.js";
import type {
	McpErrorKind,
	RunMcpToolRequest,
	RunMcpToolStatus,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { RuntimeToolExecutionRequest } from "../../../../../agent-runtime/packages/core/src/thread-loop/tool-execution.js";
import { runtimeToolSettlement } from "../../../../../agent-runtime/packages/core/src/thread-loop/tool-execution.js";
import type { ToolEntry } from "../../../../../agent-runtime/packages/core/src/tools/tool-catalog.js";
import { BridgeAPIEventWriter } from "../../../../../agent-runtime/packages/runtime-pod/src/bridge-client.js";
import { RuntimePodToolRunner } from "../../../../../agent-runtime/packages/runtime-pod/src/tool-runner.js";
import { BridgeAPIMcpToolResultIdempotencyStore } from "../../src/bridge-client.js";
import { McpSDKClient } from "../../src/client.js";
import type { GitHubMcpCredentialResolver } from "../../src/credential.js";
import { createMcpConnectorGrpcServer } from "../../src/server.js";
import type {
	McpAuthenticator,
	McpClient,
	McpConnectorLogger,
} from "../../src/service.js";
import { McpConnectorServiceShell } from "../../src/service.js";

const bridgeAddress = process.argv[2];
const gatewayTokenPath = process.argv[3];
const runtimeTokenPath = process.argv[4];
const toolUseEventIdArgument = process.argv[5];
const cleanupToolUseEventIdArgument = process.argv[6];
const cancelledToolUseEventIdArgument = process.argv[7];
const oauthSuccessToolUseEventIdArgument = process.argv[8];
const oauthFailureToolUseEventIdArgument = process.argv[9];
if (
	bridgeAddress === undefined ||
	gatewayTokenPath === undefined ||
	runtimeTokenPath === undefined ||
	toolUseEventIdArgument === undefined ||
	cleanupToolUseEventIdArgument === undefined ||
	cancelledToolUseEventIdArgument === undefined
	|| oauthSuccessToolUseEventIdArgument === undefined
	|| oauthFailureToolUseEventIdArgument === undefined
) {
	throw new Error(
		"usage: tool-claim-production-composition <bridge-address> <gateway-token-path> <runtime-token-path> <tool-use-event-id> <cleanup-tool-use-event-id> <cancelled-tool-use-event-id> <oauth-success-tool-use-event-id> <oauth-failure-tool-use-event-id>",
	);
}
const toolUseEventId: string = toolUseEventIdArgument;
const cleanupToolUseEventId: string = cleanupToolUseEventIdArgument;
const cancelledToolUseEventId: string = cancelledToolUseEventIdArgument;
const oauthSuccessToolUseEventId: string = oauthSuccessToolUseEventIdArgument;
const oauthFailureToolUseEventId: string = oauthFailureToolUseEventIdArgument;
const compositionBridgeAddress: string = bridgeAddress;
const compositionRuntimeTokenPath: string = runtimeTokenPath;

class TakeoverMcpClient implements McpClient {
	readonly calls: string[] = [];
	#cleanupCalls = 0;
	#cancelCalls = 0;
	#firstStartedResolve!: () => void;
	readonly #firstStarted = new Promise<void>((resolve) => {
		this.#firstStartedResolve = resolve;
	});
	#firstReleaseResolve!: () => void;
	readonly #firstRelease = new Promise<void>((resolve) => {
		this.#firstReleaseResolve = resolve;
	});
	#cancelStartedResolve!: () => void;
	readonly #cancelStarted = new Promise<void>((resolve) => {
		this.#cancelStartedResolve = resolve;
	});
	#cancelReleaseResolve!: () => void;
	readonly #cancelRelease = new Promise<void>((resolve) => {
		this.#cancelReleaseResolve = resolve;
	});

	async listTools() {
		return [];
	}

	async callTool(input: Parameters<McpClient["callTool"]>[0]) {
		if (input.input["mode"] === "cancel-between") {
			this.#cancelCalls += 1;
			this.calls.push(`cancel:${this.#cancelCalls}:${input.toolName}`);
			if (this.#cancelCalls === 1) {
				this.#cancelStartedResolve();
				await this.#cancelRelease;
			}
			return {
				content: [{ type: "text" as const, text: "cancelled claimant" }],
			};
		}
		if (input.input["mode"] === "cleanup") {
			this.#cleanupCalls += 1;
			this.calls.push(`cleanup:${this.#cleanupCalls}:${input.toolName}`);
			if (this.#cleanupCalls === 1) {
				return {
					content: [
						{ type: "image" as const, data: "", mimeType: "image/png" },
					],
				};
			}
			return {
				content: [{ type: "text" as const, text: "reacquired claimant" }],
			};
		}
		const callNumber = this.calls.length + 1;
		this.calls.push(
			`${callNumber}:${input.toolName}:${JSON.stringify(input.input)}`,
		);
		if (callNumber === 1) {
			this.#firstStartedResolve();
			await this.#firstRelease;
			return { content: [{ type: "text" as const, text: "old claimant" }] };
		}
		return { content: [{ type: "text" as const, text: "takeover claimant" }] };
	}

	async waitForFirstExecution(): Promise<void> {
		await this.#firstStarted;
	}

	releaseFirstExecution(): void {
		this.#firstReleaseResolve();
	}

	async waitForCancelledExecution(): Promise<void> {
		await this.#cancelStarted;
	}

	releaseCancelledExecution(): void {
		this.#cancelReleaseResolve();
	}
}

const runtimePodUid = "pod_mcp_production_composition";
const bindingKey = "mcp-production-composition-binding-key";
const client = new TakeoverMcpClient();
const idempotencyStore = new BridgeAPIMcpToolResultIdempotencyStore({
	address: bridgeAddress,
	tokenPath: gatewayTokenPath,
	sleep: async () => undefined,
});
const firstService = serviceForClaim("claim_mcp_production_old");
const takeoverService = serviceForClaim("claim_mcp_production_takeover");
const request = runRequest(toolUseEventId);
const metadata = new Metadata();
metadata.set("authorization", "Bearer mcp-production-runtime-token");

const first = firstService.runMcpTool(request, metadata);
await client.waitForFirstExecution();
const takeover = await takeoverService.runMcpTool(request, metadata);
client.releaseFirstExecution();
const staleFirst = await first;

const runtimeService = serviceForClaim("claim_mcp_production_runtime_replay");
const connectorServer = createMcpConnectorGrpcServer(runtimeService);
const connectorPort = await connectorServer.bind("127.0.0.1:0");
const runtimeRequest = mcpRuntimeRequest(
	toolUseEventId,
	"call_mcp_durable_claim",
	{ title: "Bug", body: "Details" },
);
const runner = new RuntimePodToolRunner({
	bridgeAddress,
	webAddress: "127.0.0.1:1",
	mcpConnectorAddress: `127.0.0.1:${connectorPort}`,
	tokenPath: runtimeTokenPath,
	sleep: async () => undefined,
});
const runtimeResult = await runner.runTool(runtimeRequest);
if (runtimeResult.type !== "completed")
	throw new Error(
		`MCP Runtime mapping returned ${JSON.stringify(runtimeResult)}`,
	);
const writer = new BridgeAPIEventWriter({
	address: bridgeAddress,
	tokenPath: runtimeTokenPath,
	sleep: async () => undefined,
});
const settlement = await writer.settleToolResult({
	workspaceId: runtimeRequest.workspaceId,
	sessionId: runtimeRequest.sessionId,
	sessionThreadId: runtimeRequest.sessionThreadId,
	bindingId: runtimeRequest.bindingId,
	bindingGeneration: runtimeRequest.bindingGeneration,
	targetPodUid: runtimeRequest.targetPodUid,
	settlement: { toolUseEventId, outcome: runtimeToolSettlement(runtimeResult) },
});
await connectorServer.shutdown();
if (!settlement.ok) throw settlement.error;

const cancelledFirstPromise = serviceForClaim(
	"claim_mcp_production_cancel_old",
).runMcpTool(runRequest(cancelledToolUseEventId), metadata);
await client.waitForCancelledExecution();
const cancelledTakeover = await serviceForClaim(
	"claim_mcp_production_cancel_takeover",
).runMcpTool(runRequest(cancelledToolUseEventId), metadata);
client.releaseCancelledExecution();
const cancelledFirst = await cancelledFirstPromise;

let cleanupFailureCode = -1;
try {
	await serviceForClaim("claim_mcp_production_cleanup_failed").runMcpTool(
		runRequest(cleanupToolUseEventId),
		metadata,
	);
} catch (error) {
	cleanupFailureCode =
		typeof error === "object" && error !== null && "code" in error
			? Number((error as { readonly code: unknown }).code)
			: -1;
}
const cleanupReacquired = await serviceForClaim(
	"claim_mcp_production_cleanup_reacquired",
).runMcpTool(runRequest(cleanupToolUseEventId), metadata);
const oauthSuccess = await runOAuthRuntime(
	oauthSuccessToolUseEventId,
	"call_mcp_oauth_success",
	"claim_mcp_oauth_success",
	oauthMcpClient("success"),
);
const oauthFailure = await runOAuthRuntime(
	oauthFailureToolUseEventId,
	"call_mcp_oauth_failure",
	"claim_mcp_oauth_failure",
	oauthMcpClient("unavailable"),
);

process.stdout.write(
	JSON.stringify({
		executionCalls: client.calls,
		takeover: responseSummary(takeover),
		staleFirst: responseSummary(staleFirst),
		cancelledTakeover: responseSummary(cancelledTakeover),
		cancelledFirst: responseSummary(cancelledFirst),
		cleanupFailureCode,
		cleanupReacquired: responseSummary(cleanupReacquired),
	runtimeResult,
	settlement: settlement.result,
	oauthSuccess,
	oauthFailure,
	}),
);

function serviceForClaim(
	claimId: string,
	serviceClient: McpClient = client,
): McpConnectorServiceShell {
	const authenticator: McpAuthenticator = {
		authenticate: async ({ metadata: requestMetadata }) =>
			requestMetadata
				.get("authorization")
				.some(
					(value) =>
						String(value).toLowerCase() ===
						"bearer mcp-production-runtime-token",
				)
				? {
						ok: true,
						serviceAccount: {
							namespace: "tetral",
							name: "runtime",
							podUid: runtimePodUid,
						},
					}
				: {
						ok: false,
						code: "Unauthenticated",
						message: "composition caller token rejected",
					},
	};
	const logger: McpConnectorLogger = {
		info: () => undefined,
		error: () => undefined,
	};
	return new McpConnectorServiceShell({
		authenticator,
		runtimeBindingTokenVerifier: createRuntimeBindingTokenVerifier({
			hmacKey: bindingKey,
			now: () => new Date("2026-01-01T00:00:00Z"),
		}),
		logger,
		ready: () => true,
		client: serviceClient,
		idempotencyStore,
		claimIdFactory: () => claimId,
	});
}

function runRequest(eventId: string): RunMcpToolRequest {
	return {
		workspaceId: "default",
		sessionId: "sesn_mcp_production_composition",
		sessionThreadId: "thr_mcp_production_composition",
		toolUseEventId: eventId,
		bindingId: "bind_mcp_production_composition",
		bindingGeneration: 1,
		runtimeBindingToken: signedBindingToken(),
	};
}

function mcpRuntimeRequest(
	eventId: string,
	modelToolCallId: string,
	input: RuntimeToolExecutionRequest["input"],
): RuntimeToolExecutionRequest {
	return {
		workspaceId: "default",
		sessionId: "sesn_mcp_production_composition",
		sessionThreadId: "thr_mcp_production_composition",
		bindingId: "bind_mcp_production_composition",
		bindingGeneration: 1,
		runtimeBindingToken: signedBindingToken(),
		targetPodUid: runtimePodUid,
		modelRequestId: "mreq_mcp_durable_claim",
		modelToolCallId,
		modelOrder: 0,
		toolUseEventId: eventId,
		entry: mcpToolEntry(),
		input,
		retainedContextEntries: [],
		abortSignal: new AbortController().signal,
	};
}

async function runOAuthRuntime(
	eventId: string,
	modelToolCallId: string,
	claimId: string,
	oauthClient: McpClient,
) {
	const service = serviceForClaim(claimId, oauthClient);
	const server = createMcpConnectorGrpcServer(service);
	const port = await server.bind("127.0.0.1:0");
	try {
		const request = mcpRuntimeRequest(eventId, modelToolCallId, {
			mode: claimId.endsWith("success") ? "oauth-success" : "oauth-failure",
		});
		const oauthRunner = new RuntimePodToolRunner({
			bridgeAddress: compositionBridgeAddress,
			webAddress: "127.0.0.1:1",
			mcpConnectorAddress: `127.0.0.1:${port}`,
			tokenPath: compositionRuntimeTokenPath,
			sleep: async () => undefined,
		});
		const result = await oauthRunner.runTool(request);
		if (result.type === "stale_custody") {
			throw new Error("OAuth MCP composition lost Runtime custody");
		}
		const oauthSettlement = await writer.settleToolResult({
			workspaceId: request.workspaceId,
			sessionId: request.sessionId,
			sessionThreadId: request.sessionThreadId,
			bindingId: request.bindingId,
			bindingGeneration: request.bindingGeneration,
			targetPodUid: request.targetPodUid,
			settlement: { toolUseEventId: eventId, outcome: runtimeToolSettlement(result) },
		});
		if (!oauthSettlement.ok) throw oauthSettlement.error;
		return result;
	} finally {
		await server.shutdown();
	}
}

function oauthMcpClient(mode: "success" | "unavailable"): McpSDKClient {
	const success = {
		ok: true as const,
		mode: "bearer" as const,
		token: "bounded-oauth-token",
		tokenHash: "bounded-oauth-token-hash",
		vaultId: "vlt_mcp_oauth",
		credentialId: "cred_mcp_oauth",
		refreshTriggered: true,
	};
	const unavailable = { ok: false as const, error: "expired" as const };
	const resolver: GitHubMcpCredentialResolver = {
		resolve: async () => (mode === "success" ? success : unavailable),
		refresh: async () => (mode === "success" ? success : unavailable),
	};
	return new McpSDKClient({
		credentialResolver: resolver,
		onToolsListChanged: async () => undefined,
		createTransport: () => new OAuthCompositionTransport(),
	});
}

class OAuthCompositionTransport implements Transport {
	onclose?: () => void;
	onerror?: (error: Error) => void;
	onmessage: NonNullable<Transport["onmessage"]> = () => undefined;

	async start(): Promise<void> {}

	async send(message: Parameters<Transport["send"]>[0]): Promise<void> {
		if (!("id" in message) || !("method" in message)) return;
		const result = message.method === "initialize"
			? { protocolVersion: "2025-06-18", capabilities: { tools: {} }, serverInfo: { name: "oauth-composition", version: "1.0.0" } }
			: message.method === "tools/list"
				? { tools: [] }
				: message.method === "tools/call"
					? { content: [{ type: "text", text: "oauth refreshed" }] }
					: undefined;
		if (result === undefined) throw new Error(`unexpected MCP request ${message.method}`);
		queueMicrotask(() => this.onmessage?.({ jsonrpc: "2.0", id: message.id, result } as Parameters<NonNullable<Transport["onmessage"]>>[0]));
	}

	async close(): Promise<void> {
		this.onclose?.();
	}
}

function mcpToolEntry(): ToolEntry {
	return {
		name: "create_issue",
		definition: {
			kind: "function",
			name: "create_issue",
			description: "Create an issue through MCP.",
			inputSchema: { type: "object" },
		},
		inputContract: { kind: "json_object" },
		route: {
			kind: "gateway",
			operation: "RunMcpTool",
			mcpServerName: "github",
		},
		formatter: {
			successShape: "MCP formatted text.",
			errorShape: "MCP formatted error.",
			forbiddenFields: ["authorization", "token", "credential"],
		},
		defaultPermissionPolicy: "always_allow",
		required: false,
	};
}

function signedBindingToken(): string {
	const payloadPart = Buffer.from(
		JSON.stringify({
			v: 1,
			workspace_id: "default",
			session_id: "sesn_mcp_production_composition",
			session_thread_id: "thr_mcp_production_composition",
			binding_id: "bind_mcp_production_composition",
			binding_generation: 1,
			runtime_pod_uid: runtimePodUid,
			exp: Math.floor(new Date("2026-01-01T00:05:00Z").getTime() / 1_000),
		}),
	).toString("base64url");
	const signature = createHmac("sha256", bindingKey)
		.update(payloadPart)
		.digest("base64url");
	return `rtbt_v1.${payloadPart}.${signature}`;
}

function responseSummary(response: {
	readonly status: RunMcpToolStatus;
	readonly resultText: string;
	readonly errorKind?: McpErrorKind | undefined;
}) {
	return {
		status: response.status,
		resultText: response.resultText,
		errorKind: response.errorKind,
	};
}
