import { runMcpConnectorCommand } from "../../../mcp-connector/src/command.js";
import { buildProviderGatewayCommandDependencies } from "../../../provider-gateway/src/command.js";
import type { ProviderGatewayConfig } from "../../../provider-gateway/src/config.js";
import type { SchemaSQL } from "../../src/verify.js";

const databaseURL = process.env.TETRAL_TEST_RUNTIME_DATABASE_URL;
const expectation = process.env.TETRAL_TEST_ROLE_EXPECTATION;
if (!databaseURL || (expectation !== "accepted" && expectation !== "rejected")) {
	throw new Error("role readiness fixture configuration is invalid");
}

Object.assign(process.env, {
	TETRAL_MCP_CONNECTOR_GRPC_ADDR: "127.0.0.1:9091",
	TETRAL_MCP_CONNECTOR_HTTP_ADDR: "127.0.0.1:8081",
	TETRAL_DEPLOYMENT_ENVIRONMENT: "test",
	TETRAL_SERVICE_VERSION: "fixture",
	TETRAL_INTERNAL_GRPC_AUDIENCE: "tetral-internal-grpc",
	TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS: "tetral/runtime",
	TETRAL_MCP_CONNECTOR_ALLOWED_BRIDGE_SERVICE_ACCOUNTS: "tetral/bridge",
	TETRAL_BRIDGE_API_GRPC_ADDR: "bridge:9090",
	TETRAL_MCP_CONNECTOR_BRIDGE_TOKEN_PATH: "/bridge-token",
	TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY: "x".repeat(32),
	TETRAL_DATABASE_URL: databaseURL,
	ENGINE_VAULT_KEY: "01".repeat(32),
	KUBERNETES_API_SERVER_URL: "https://kubernetes.default.svc",
	KUBERNETES_API_CA_CERT_PATH: "/ca",
	KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH: "/token",
});

const pools: Array<Bun.SQL & SchemaSQL> = [];
const sqlFactory = (): Bun.SQL & SchemaSQL => {
	const sql = new Bun.SQL({ url: databaseURL, max: 2 });
	pools.push(sql);
	return sql as Bun.SQL & SchemaSQL;
};

const results: string[] = [];
try {
	await runMcpConnectorCommand({
		sqlFactory,
		client: { connectionCount: () => 0, listTools: async () => [], callTool: async () => ({ content: [] }) },
		manifestChangeNotifier: { notify: async () => ({ ok: true, duplicate: false }) },
		reviewerMaterialValidator: async () => undefined,
		serverFactory: () => ({ server: undefined as never, bind: async () => 9091, shutdown: async () => undefined }),
		httpServerFactory: () => ({ url: new URL("http://127.0.0.1:8081"), stop: async () => undefined }),
		registerSignalHandlers: () => undefined,
		waitForever: async () => undefined as never,
	});
	results.push("mcp:accepted");
} catch (error) {
	results.push(`mcp:${(error as { kind?: string }).kind === "runtime_role_invalid" ? "rejected" : "unexpected"}`);
}

try {
	const dependencies = await buildProviderGatewayCommandDependencies({
		config: providerConfig(databaseURL),
		logger: { info: () => undefined, error: () => undefined },
		builderOptions: {
			sqlFactory: sqlFactory as never,
			tokenReviewClientFactory: () => ({
				createTokenReview: async () => ({ authenticated: false, username: "", audiences: [], podUid: "" }),
			}),
		},
	});
	await dependencies.close?.();
	results.push("provider:accepted");
} catch (error) {
	results.push(`provider:${(error as { kind?: string }).kind === "runtime_role_invalid" ? "rejected" : "unexpected"}`);
}

await Promise.allSettled(pools.map(async (pool) => await pool.close({ timeout: 1 })));
const wanted = [`mcp:${expectation}`, `provider:${expectation}`];
if (results.join(",") !== wanted.join(",")) {
	throw new Error("serving command role readiness result is invalid");
}

function providerConfig(url: string): ProviderGatewayConfig {
	return {
		deploymentEnvironment: "test",
		serviceVersion: "fixture",
		grpcBindAddress: "127.0.0.1:9090",
		httpBindAddress: "127.0.0.1:8080",
		allowedRuntimePod: { namespace: "tetral", serviceAccount: "runtime" },
		runtimeBindingTokenHMACKey: "x".repeat(32),
		databaseUrl: url,
		databasePool: { max: 2, idleTimeout: 1, maxLifetime: 10, connectionTimeout: 2, statementTimeoutMs: 5_000 },
		vaultKeyHex: "01".repeat(32),
		kubernetesApiServerUrl: "https://kubernetes.default.svc",
		kubernetesApiCaCertPath: "/ca",
		tokenReviewReviewerTokenPath: "/token",
		bridgeApiGrpcAddress: "bridge:9090",
		bridgeTokenPath: "/bridge-token",
		maxConcurrentTurns: 1,
	};
}
