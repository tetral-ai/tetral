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
import { SQLGitHubMcpCredentialResolver } from "../../src/credential.js";
import type { McpCredentialSQL } from "../../src/credential.js";
import type { McpOAuthRefreshCompletedEvent } from "../../src/credential-update-path.js";
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
const databaseURL = process.env.TETRAL_TEST_DATABASE_URL;
const databaseSchema = process.env.TETRAL_TEST_DATABASE_SCHEMA;
if (
	bridgeAddress === undefined ||
	gatewayTokenPath === undefined ||
	runtimeTokenPath === undefined ||
	toolUseEventIdArgument === undefined ||
	cleanupToolUseEventIdArgument === undefined ||
	cancelledToolUseEventIdArgument === undefined ||
	oauthSuccessToolUseEventIdArgument === undefined ||
	oauthFailureToolUseEventIdArgument === undefined ||
	databaseURL === undefined ||
	databaseSchema === undefined
) {
	throw new Error(
		"usage: tool-claim-production-composition <bridge-address> <gateway-token-path> <runtime-token-path> <tool-use-event-id> <cleanup-tool-use-event-id> <cancelled-tool-use-event-id> <oauth-success-tool-use-event-id> <oauth-failure-tool-use-event-id> with TETRAL_TEST_DATABASE_URL and TETRAL_TEST_DATABASE_SCHEMA",
	);
}
const toolUseEventId: string = toolUseEventIdArgument;
const cleanupToolUseEventId: string = cleanupToolUseEventIdArgument;
const cancelledToolUseEventId: string = cancelledToolUseEventIdArgument;
const oauthSuccessToolUseEventId: string = oauthSuccessToolUseEventIdArgument;
const oauthFailureToolUseEventId: string = oauthFailureToolUseEventIdArgument;
const compositionBridgeAddress: string = bridgeAddress;
const compositionRuntimeTokenPath: string = runtimeTokenPath;
const connectorLogRecords: unknown[] = [];

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
const oauthComposition = await createOAuthCredentialComposition(databaseURL, databaseSchema);
let oauthSuccess: Awaited<ReturnType<typeof runOAuthRuntime>>;
let oauthFailure: Awaited<ReturnType<typeof runOAuthRuntime>>;
let oauthProof: OAuthCredentialProof;
try {
	oauthSuccess = await runOAuthRuntime(
		oauthSuccessToolUseEventId,
		"call_mcp_oauth_success",
		"claim_mcp_oauth_success",
		oauthComposition.successClient,
	);
	await oauthComposition.prepareFailure();
	oauthFailure = await runOAuthRuntime(
		oauthFailureToolUseEventId,
		"call_mcp_oauth_failure",
		"claim_mcp_oauth_failure",
		oauthComposition.failureClient,
	);
	oauthProof = await oauthComposition.proof();
} finally {
	await oauthComposition.close();
}

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
	oauthProof,
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
		info: (...args: unknown[]) => { connectorLogRecords.push(args); },
		error: (...args: unknown[]) => { connectorLogRecords.push(args); },
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

interface OAuthCredentialProof {
	readonly issuerRequests: number;
	readonly successTransportCount: number;
	readonly failureTransportCount: number;
	readonly durableRotation: boolean;
	readonly failedRefreshPreservedCredential: boolean;
	readonly refreshOutcomes: readonly string[];
	readonly leakSurfacesClean: boolean;
}

async function createOAuthCredentialComposition(databaseURL: string, schema: string) {
	if (!/^[a-z0-9_]+$/.test(schema)) throw new Error("invalid PostgreSQL composition schema");
	const keyHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
	const now = new Date("2026-01-01T00:00:00.000Z");
	const initialAuth = {
		type: "mcp_oauth",
		mcp_server_url: "https://api.githubcopilot.com/mcp/",
		access_token: "ACCESS_TOKEN_CANARY_OLD",
		expires_at: "2025-12-31T23:59:00.000Z",
		refresh: {
			refresh_token: "REFRESH_TOKEN_CANARY_OLD",
			client_id: "github-client",
			token_endpoint: "https://github.example.invalid/oauth/token",
			token_endpoint_auth: { type: "none" },
		},
	};
	const publicAuth = JSON.stringify({
		type: initialAuth.type,
		mcp_server_url: initialAuth.mcp_server_url,
		expires_at: initialAuth.expires_at,
		refresh: {
			client_id: initialAuth.refresh.client_id,
			token_endpoint: initialAuth.refresh.token_endpoint,
			token_endpoint_auth: initialAuth.refresh.token_endpoint_auth,
		},
	});
	const initialEncrypted = await encryptAES256GCM(
		new TextEncoder().encode(JSON.stringify(initialAuth)),
		keyHex,
	);
	const admin = new Bun.SQL({ url: databaseURL, max: 1 });
	const appURL = new URL(databaseURL);
	appURL.username = "tetral_runtime_test";
	appURL.password = "tetral_runtime_test_pw";
	const app = new Bun.SQL({ url: appURL.toString(), max: 1 });
	await admin.unsafe(`SET search_path TO ${schema}, pg_catalog`);
	await app.unsafe(`SET search_path TO ${schema}, pg_catalog`);
	await admin`
		INSERT INTO vaults (workspace_id, id, display_name, metadata_json, created_at, updated_at)
		VALUES ('default', 'vlt_mcp_oauth', 'MCP OAuth composition', '{}', ${now}, ${now})`;
	await admin`UPDATE sessions SET vault_ids_json='["vlt_mcp_oauth"]' WHERE workspace_id='default' AND id='sesn_mcp_production_composition'`;
	await admin`
		INSERT INTO credentials (
			workspace_id, id, vault_id, display_name, metadata_json, auth_type,
			auth_public_json, mcp_server_url, expires_at, encrypted_auth, created_at, updated_at
		) VALUES (
			'default', 'cred_mcp_oauth', 'vlt_mcp_oauth', 'GitHub MCP OAuth', '{}', 'mcp_oauth',
			${publicAuth}, ${initialAuth.mcp_server_url}, ${initialAuth.expires_at}, ${initialEncrypted}, ${now}, ${now}
		)`;

	let issuerFailure = false;
	let issuerRequests = 0;
	const refreshEvents: McpOAuthRefreshCompletedEvent[] = [];
	const successTransportTokens: string[] = [];
	const failureTransportTokens: string[] = [];
	const fetchFn = async (_input: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]): ReturnType<typeof fetch> => {
		issuerRequests += 1;
		if (new Headers(init?.headers).get("accept") !== "application/json") {
			throw new Error("OAuth refresh omitted the JSON accept header");
		}
		if (issuerFailure) {
			return new Response(JSON.stringify({ error: "RAW_ISSUER_ERROR_CANARY" }), { status: 503 });
		}
		return Response.json({
			access_token: "ACCESS_TOKEN_CANARY_ROTATED",
			refresh_token: "REFRESH_TOKEN_CANARY_ROTATED",
			expires_in: 3600,
		});
	};
	const resolver = new SQLGitHubMcpCredentialResolver(
		app as unknown as McpCredentialSQL,
		keyHex,
		() => now,
		fetchFn,
		undefined,
		undefined,
		(event) => { refreshEvents.push(event); },
	);
	const successClient = oauthMcpClient(resolver, (token) => { successTransportTokens.push(token); });
	const failureClient = oauthMcpClient(resolver, (token) => { failureTransportTokens.push(token); });
	let rotatedCredential = false;
	let failedRefreshPreservedCredential = false;

	return {
		successClient,
		failureClient,
		prepareFailure: async () => {
			const rows = await admin<{ encrypted_auth: Uint8Array; expires_at: string }[]>`
				SELECT encrypted_auth, expires_at FROM credentials
				WHERE workspace_id='default' AND vault_id='vlt_mcp_oauth' AND id='cred_mcp_oauth'`;
			const rotated = JSON.parse(new TextDecoder().decode(
				await decryptAES256GCM(rows[0]!.encrypted_auth, keyHex),
			)) as { access_token?: string; expires_at?: string; refresh?: { refresh_token?: string } };
			rotatedCredential = rotated.access_token === "ACCESS_TOKEN_CANARY_ROTATED"
				&& rotated.refresh?.refresh_token === "REFRESH_TOKEN_CANARY_ROTATED"
				&& rows[0]!.expires_at === "2026-01-01T01:00:00.000Z";
			await successClient.closeAll();
			await admin`
				UPDATE credentials SET auth_public_json=${publicAuth}, encrypted_auth=${initialEncrypted},
					expires_at=${initialAuth.expires_at}, updated_at=${now}
				WHERE workspace_id='default' AND vault_id='vlt_mcp_oauth' AND id='cred_mcp_oauth'`;
			issuerFailure = true;
		},
		proof: async (): Promise<OAuthCredentialProof> => {
			const rows = await admin<{ encrypted_auth: Uint8Array }[]>`
				SELECT encrypted_auth FROM credentials
				WHERE workspace_id='default' AND vault_id='vlt_mcp_oauth' AND id='cred_mcp_oauth'`;
			failedRefreshPreservedCredential = Buffer.from(rows[0]!.encrypted_auth)
				.equals(Buffer.from(initialEncrypted));
			const leakSurface = JSON.stringify({ refreshEvents, connectorLogRecords });
			return {
				issuerRequests,
				successTransportCount: successTransportTokens.length,
				failureTransportCount: failureTransportTokens.length,
				durableRotation: rotatedCredential,
				failedRefreshPreservedCredential,
				refreshOutcomes: refreshEvents.map((event) => event.outcome),
				leakSurfacesClean: !["ACCESS_TOKEN_CANARY", "REFRESH_TOKEN_CANARY", "RAW_ISSUER_ERROR_CANARY"]
					.some((canary) => leakSurface.includes(canary)),
			};
		},
		close: async () => {
			await successClient.closeAll();
			await failureClient.closeAll();
			await app.close();
			await admin.close();
		},
	};
}

function oauthMcpClient(
	resolver: SQLGitHubMcpCredentialResolver,
	observeToken: (token: string) => void,
): McpSDKClient {
	return new McpSDKClient({
		credentialResolver: resolver,
		onToolsListChanged: async () => undefined,
		createTransport: ({ token }) => {
			observeToken(token ?? "");
			return new OAuthCompositionTransport();
		},
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

async function encryptAES256GCM(plaintext: Uint8Array, keyHex: string): Promise<Uint8Array> {
	const key = await crypto.subtle.importKey("raw", arrayBuffer(Uint8Array.fromHex(keyHex)), { name: "AES-GCM" }, false, ["encrypt"]);
	const nonce = crypto.getRandomValues(new Uint8Array(12));
	const ciphertext = new Uint8Array(await crypto.subtle.encrypt(
		{ name: "AES-GCM", iv: arrayBuffer(nonce), tagLength: 128 }, key, arrayBuffer(plaintext),
	));
	const encoded = new Uint8Array(nonce.length + ciphertext.length);
	encoded.set(nonce, 0);
	encoded.set(ciphertext, nonce.length);
	return encoded;
}

async function decryptAES256GCM(ciphertext: Uint8Array, keyHex: string): Promise<Uint8Array> {
	const key = await crypto.subtle.importKey("raw", arrayBuffer(Uint8Array.fromHex(keyHex)), { name: "AES-GCM" }, false, ["decrypt"]);
	const plaintext = await crypto.subtle.decrypt(
		{ name: "AES-GCM", iv: arrayBuffer(ciphertext.slice(0, 12)), tagLength: 128 },
		key,
		arrayBuffer(ciphertext.slice(12)),
	);
	return new Uint8Array(plaintext);
}

function arrayBuffer(bytes: Uint8Array): ArrayBuffer {
	return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;
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
