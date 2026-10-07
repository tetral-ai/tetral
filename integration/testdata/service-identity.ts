// Drives the production Bun receivers through TCP. Only external business adapters and
// Kubernetes TokenReview's HTTP authority are controlled by the composition fixture.
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { createHmac } from "node:crypto";
import { DefaultBridgeMethodPolicies } from "../../services/agent-runtime/packages/runtime-pod/src/bridge-policy.js";
import { createRuntimePodApp } from "../../services/agent-runtime/packages/runtime-pod/src/app.js";
import { KubernetesTokenReviewClient as RuntimeReview } from "../../services/agent-runtime/packages/runtime-pod/src/auth.js";
import { AgentRuntimePodServiceClient, CleanupSessionReason } from "../../services/agent-runtime/packages/protocol/src/gen/tetral/agent_runtime/v1/agent_runtime.js";
import { ProviderCredentialResolver } from "../../services/gateway/packages/provider-gateway/src/providers/credentials.js";
import { createProviderGatewayApp } from "../../services/gateway/packages/provider-gateway/src/app.js";
import { KubernetesTokenReviewClient as GatewayReview } from "../../services/gateway/packages/provider-gateway/src/auth.js";
import { createMcpConnectorGrpcServer } from "../../services/gateway/packages/mcp-connector/src/server.js";
import { McpConnectorServiceShell } from "../../services/gateway/packages/mcp-connector/src/service.js";
import { authenticateMcpCaller, KubernetesTokenReviewClient as McpReview } from "../../services/gateway/packages/mcp-connector/src/auth.js";
import { loadMcpConnectorConfigFromEnv } from "../../services/gateway/packages/mcp-connector/src/config.js";
import { InMemoryMcpIdempotencyStore } from "../../services/gateway/packages/mcp-connector/src/idempotency.js";
import { createRuntimeBindingTokenVerifier } from "../../services/gateway/packages/protocol/src/binding-token.js";
import { ProviderGatewayServiceClient, McpConnectorServiceClient, ProviderStreamEventType } from "../../services/gateway/packages/protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { validProviderRequest } from "../../services/gateway/packages/provider-gateway/test/unit/fixtures.js";
const runtimeGrpc = createRequire(new URL("../../services/agent-runtime/package.json", import.meta.url))("@grpc/grpc-js");
const grpc = createRequire(new URL("../../services/gateway/package.json", import.meta.url))("@grpc/grpc-js");
const { Metadata, credentials, status } = grpc;
const reviewOptions = { apiServerUrl: process.env.TETRAL_TEST_REVIEW_URL!, reviewerTokenPath: process.env.TETRAL_TEST_REVIEW_TOKEN_PATH!, apiServerCaCertPath: process.env.TETRAL_TEST_REVIEW_CA_PATH! };
const key = "service-identity-binding-key-at-least-32-bytes";
const uid = "pod_uid_service_identity";
const diagnostics = { level: "info" as const, maxRecordBytes: 16384, summaryIntervalMs: 30000, burst: 1 };
const logger = { info: () => undefined, error: () => undefined };
const runtimeProcessId = process.env.TETRAL_TEST_RUNTIME_PROCESS_ID!;
const base = { runtimeProcessId, workspaceId: "default", sessionId: "sesn_identity", sessionThreadId: "thrd_identity", bindingId: process.env.TETRAL_TEST_RUNTIME_BINDING_ID!, bindingGeneration: Number(process.env.TETRAL_TEST_RUNTIME_BINDING_GENERATION!) };
const rows: unknown[] = [];
function binding(identity = base, pod = uid, expiry = Math.floor(Date.now() / 1000) + 600) { const payload = Buffer.from(JSON.stringify({ v: 1, workspace_id: identity.workspaceId, session_id: identity.sessionId, session_thread_id: identity.sessionThreadId, binding_id: identity.bindingId, binding_generation: identity.bindingGeneration, runtime_pod_uid: pod, runtime_process_id: identity.runtimeProcessId, exp: expiry })).toString("base64url"); return `rtbt_v1.${payload}.${createHmac("sha256", key).update(payload).digest("base64url")}`; }
function metadata(token: string, runtime = false) { const m = new (runtime ? runtimeGrpc.Metadata : Metadata)(); m.set("authorization", `bearer ${token}`); return m; }
async function unary(client: any, method: string, request: any, token: string) { return await new Promise<any>((resolve, reject) => client[method](request, metadata(token, client === runtimeClient), { deadline: Date.now() + 5000 }, (err: any, result: any) => err ? reject(err) : resolve(result))); }
async function expectCode(receiver: string, method: string, token: string, invoke: () => Promise<any>, code: number, readEffect: () => number) { const before = readEffect(); let observed = -1; try {
    await invoke();
}
catch (error) {
    observed = (error as {
        code: number;
    }).code;
} assert.equal(observed, code, `${receiver}.${method} ${token}`); assert.equal(readEffect(), before, `${receiver}.${method} denied caller changed business effects`); rows.push({ receiver, method, token, code: observed, effectsUnchanged: true }); }
const denied = ["bridge", "provider", "mcp", "oldgateway", "wrongns", "expired", "wrongaud"];
let runtimeEffects = 0, providerCredentialReads = 0, providerPoolSelections = 0;
const runtime = createRuntimePodApp({ runtimeProcess: { runtimeProcessId, register: async () => undefined, report: async () => undefined, release: async () => { throw new Error("identity fixture owns no Session binding"); }, close: async () => undefined }, quiesce: async () => undefined, config: { ownPod: { namespace: "tetral-agent-runtime", name: "runtime-identity", uid, ip: "127.0.0.1" }, deploymentEnvironment: "test", diagnostics, serviceVersion: "test", jobRunner: { namespace: "tetral-system", serviceAccount: "job-runner" }, grpcBindAddress: "127.0.0.1:0", httpBindAddress: "127.0.0.1:0", kubernetesApiServerUrl: reviewOptions.apiServerUrl, kubernetesApiCaCertPath: reviewOptions.apiServerCaCertPath, tokenReviewReviewerTokenPath: reviewOptions.reviewerTokenPath, outboundInternalGrpcTokenPath: reviewOptions.reviewerTokenPath, bridgeApiGrpcAddress: "127.0.0.1:1", gatewayGrpcAddress: "127.0.0.1:1", mcpConnectorGrpcAddress: "127.0.0.1:1", webConnectorGrpcAddress: "127.0.0.1:1", platformModels: { approvalReviewer: { providerId: "deepseek", modelId: "deepseek-v4-pro" } }, skillGuidance: { descriptionBudgetBytes: 32768 }, providerStreamTimeoutMs: 1800000, bridgeMethodPolicies: DefaultBridgeMethodPolicies, maxLocalSessions: 8, maxConcurrentTools: 8, lifecycle: { reportIntervalMs: 2000, processFreshnessMs: 10000, currentStepTimeoutMs: 60000, settlementTimeoutMs: 15000, localJoinTimeoutMs: 5000, proxyJoinTimeoutMs: 5000 } }, logger, tokenReviewClient: new RuntimeReview(reviewOptions), commandRunHost: {} as never, cleanupRunHost: { handleCleanupSession: async (scope: any) => { runtimeEffects++; return { ok: true, sessionId: scope.sessionId, cleaned: true }; } } });
let runtimeClient: any, providerClient: any, mcpClient: any;
const provider = createProviderGatewayApp({ config: { deploymentEnvironment: "test", diagnostics, serviceVersion: "test", grpcBindAddress: "127.0.0.1:0", httpBindAddress: "127.0.0.1:0", allowedRuntimePod: { namespace: "tetral-agent-runtime", serviceAccount: "agent-runtime" }, runtimeBindingTokenHMACKey: key, databaseUrl: process.env.TETRAL_TEST_DATABASE_URL!, databasePool: { max: 10, idleTimeout: 30, maxLifetime: 1800, connectionTimeout: 30, statementTimeoutMs: 30000 }, vaultKeyHex: "0".repeat(64), bridgeApiGrpcAddress: "127.0.0.1:1", bridgeTokenPath: reviewOptions.reviewerTokenPath, kubernetesApiServerUrl: reviewOptions.apiServerUrl, kubernetesApiCaCertPath: reviewOptions.apiServerCaCertPath, tokenReviewReviewerTokenPath: reviewOptions.reviewerTokenPath, maxConcurrentTurns: 100, drainTimeoutMs: 30000,cancelJoinTimeoutMs:1000 }, logger, tokenReviewClient: new GatewayReview(reviewOptions), credentialResolver: new ProviderCredentialResolver({ masterKeyHex: "0".repeat(64), store: { loadActiveSessionProviderAuth: async () => { providerCredentialReads++; return []; } }, platformPool: { select: async () => { providerPoolSelections++; return { ok: false, error: { code: "platform_keys_exhausted", message: "Controlled provider pool is empty.", retryable: true, fatal: false, statusCode: 503, retryAfterMs: 100 } }; } } }) });
let discoveryEffects = 0, toolEffects = 0;
const mcpConfig = loadMcpConnectorConfigFromEnv({ TETRAL_MCP_CONNECTOR_GRPC_ADDR: "127.0.0.1:0", TETRAL_MCP_CONNECTOR_HTTP_ADDR: "127.0.0.1:0", TETRAL_DEPLOYMENT_ENVIRONMENT: "test", TETRAL_SERVICE_VERSION: "test", TETRAL_INTERNAL_GRPC_AUDIENCE: "tetral-internal-grpc", TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS: "tetral-agent-runtime/agent-runtime", TETRAL_MCP_CONNECTOR_ALLOWED_BRIDGE_SERVICE_ACCOUNTS: "tetral-system/bridge,tetral-system/job-runner", TETRAL_BRIDGE_API_GRPC_ADDR: "127.0.0.1:1", TETRAL_MCP_CONNECTOR_BRIDGE_TOKEN_PATH: reviewOptions.reviewerTokenPath, TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY: key, TETRAL_DATABASE_URL: process.env.TETRAL_TEST_DATABASE_URL!, ENGINE_VAULT_KEY: "0".repeat(64), KUBERNETES_API_SERVER_URL: reviewOptions.apiServerUrl, KUBERNETES_API_CA_CERT_PATH: reviewOptions.apiServerCaCertPath, KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH: reviewOptions.reviewerTokenPath });
assert.equal(mcpConfig.ok, true);
if (!mcpConfig.ok)
    throw new Error("MCP fixture configuration failed");
const mcpReview = new McpReview(reviewOptions);
const mcpService = new McpConnectorServiceShell({ logger, ready: () => true, runtimeBindingTokenVerifier: createRuntimeBindingTokenVerifier({ hmacKey: key }), authenticator: { authenticate: async ({ metadata, method }) => authenticateMcpCaller({ metadata, method, tokenReviewClient: mcpReview, allowedRuntimePod: { namespace: mcpConfig.config.allowedRuntimePod.namespace, name: mcpConfig.config.allowedRuntimePod.serviceAccount }, allowedDiscoveryCallers: mcpConfig.config.allowedDiscoveryCallers.map(c => ({ namespace: c.namespace, name: c.serviceAccount })) }) }, client: { listTools: async () => { discoveryEffects++; return [{ name: "identity_probe", description: "identity probe", inputSchema: { type: "object" } }]; }, callTool: async () => { toolEffects++; return { content: [{ type: "text", text: "identity tool completed" }], isError: false }; } }, idempotencyStore: new InMemoryMcpIdempotencyStore({ mcpServerName: "github", toolName: "identity_probe", inputJson: "{}" }) });
const mcp = createMcpConnectorGrpcServer(mcpService);
try {
    const rs = await runtime.start();
    runtimeClient = new AgentRuntimePodServiceClient(`127.0.0.1:${rs.grpcPort}`, runtimeGrpc.credentials.createInsecure());
    const ps = await provider.start();
    providerClient = new ProviderGatewayServiceClient(`127.0.0.1:${ps.grpcPort}`, credentials.createInsecure());
    const mp = await mcp.bind("127.0.0.1:0");
    mcpClient = new McpConnectorServiceClient(`127.0.0.1:${mp}`, credentials.createInsecure());
    const cleanup = { workspaceId: base.workspaceId, sessionId: base.sessionId, bindingId: base.bindingId, bindingGeneration: base.bindingGeneration, targetPodUid: uid, runtimeProcessId, cleanupOperationId: "cleanup_identity", reason: CleanupSessionReason.CLEANUP_SESSION_REASON_EXPIRED };
    const completedCleanup = await unary(runtimeClient, "cleanupSession", cleanup, "runner");
    assert.deepEqual(completedCleanup.completed, {});
    assert.equal(completedCleanup.duplicate, undefined);
    assert.equal(completedCleanup.rejected, undefined);
    assert.equal(runtimeEffects, 1);
    rows.push({ receiver: "runtime", method: "cleanupSession", token: "runner", business: "completed", effects: 1 });
    for (const token of [...denied, "runtime"]) {
        await expectCode("runtime", "cleanupSession", token, () => unary(runtimeClient, "cleanupSession", cleanup, token), token === "expired" || token === "wrongaud" ? status.UNAUTHENTICATED : status.PERMISSION_DENIED, () => runtimeEffects);
    }
    const mismatch = await unary(runtimeClient, "cleanupSession", { ...cleanup, targetPodUid: "other-pod" }, "runner");
    assert.ok(mismatch.rejected);
    assert.equal(runtimeEffects, 1);
    rows.push({ receiver: "runtime", method: "cleanupSession", token: "runner", business: "selected pod rejected", effectsUnchanged: true });
    await expectCode("runtime", "cleanupSession", "runner", () => unary(runtimeClient, "cleanupSession", { ...cleanup, runtimeProcessId: "retired-process" }, "runner"), status.FAILED_PRECONDITION, () => runtimeEffects);
    const providerRequest = validProviderRequest({ ...base, model: { providerId: "openai", modelId: "gpt-5.6-sol", variant: "" }, runtimeBindingToken: process.env.TETRAL_TEST_RUNTIME_BINDING_TOKEN! });
    const stream = async (request: any, token: string) => await new Promise<any[]>((resolve, reject) => { const events: any[] = []; const call = providerClient.streamProviderRequest(request, metadata(token), { deadline: Date.now() + 5000 }); call.on("data", (event: any) => events.push(event)); call.on("error", reject); call.on("end", () => resolve(events)); });
    const events = await stream(providerRequest, "runtime");
    assert.equal(events.length, 1);
    assert.equal(events[0].type, ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR);
    assert.equal(events[0].providerError?.error?.code, "provider_unavailable");
    assert.equal(providerCredentialReads, 1);
    assert.equal(providerPoolSelections, 1);
    rows.push({ receiver: "provider", method: "streamProviderRequest", token: "runtime", business: "provider pool unavailable", bindingVerified: true, credentialReads: 1, poolSelections: 1 });
    for (const token of [...denied, "runner"]) {
        await expectCode("provider", "streamProviderRequest", token, () => stream(providerRequest, token), token === "expired" || token === "wrongaud" ? status.UNAUTHENTICATED : status.PERMISSION_DENIED, () => providerCredentialReads + providerPoolSelections);
    }
    await expectCode("provider", "streamProviderRequest", "runtime", () => stream({ ...providerRequest, runtimeBindingToken: binding(base, "other-pod") }, "runtime"), status.PERMISSION_DENIED, () => providerCredentialReads + providerPoolSelections);
    await expectCode("provider", "streamProviderRequest", "runtime", () => stream({ ...providerRequest, runtimeProcessId: "retired-process" }, "runtime"), status.PERMISSION_DENIED, () => providerCredentialReads + providerPoolSelections);
    await expectCode("provider", "streamProviderRequest", "runtime", () => stream({ ...providerRequest, requestId: "" }, "runtime"), status.INVALID_ARGUMENT, () => providerCredentialReads + providerPoolSelections);
    const list = { workspaceId: base.workspaceId, sessionId: base.sessionId, mcpServerName: "github" };
    for (const token of ["bridge", "runner"]) {
        const before = discoveryEffects;
        const result = await unary(mcpClient, "listMcpTools", list, token);
        assert.equal(result.tools[0]?.name, "identity_probe");
        assert.equal(discoveryEffects, before + 1);
        rows.push({ receiver: "mcp", method: "listMcpTools", token, business: "manifest returned", effects: discoveryEffects });
    }
    for (const token of ["runtime", "provider", "mcp", "oldgateway", "wrongns", "expired", "wrongaud"]) {
        await expectCode("mcp", "listMcpTools", token, () => unary(mcpClient, "listMcpTools", list, token), token === "expired" || token === "wrongaud" ? status.UNAUTHENTICATED : status.PERMISSION_DENIED, () => discoveryEffects);
    }
    const tool = { ...base, toolUseEventId: "sevt_identity", runtimeBindingToken: process.env.TETRAL_TEST_RUNTIME_BINDING_TOKEN! };
    const result = await unary(mcpClient, "runMcpTool", tool, "runtime");
    assert.equal(result.resultText, "identity tool completed");
    assert.equal(toolEffects, 1);
    rows.push({ receiver: "mcp", method: "runMcpTool", token: "runtime", business: "tool completed", effects: 1 });
    for (const token of [...denied, "runner"]) {
        await expectCode("mcp", "runMcpTool", token, () => unary(mcpClient, "runMcpTool", tool, token), token === "expired" || token === "wrongaud" ? status.UNAUTHENTICATED : status.PERMISSION_DENIED, () => toolEffects);
    }
    await expectCode("mcp", "runMcpTool", "runtime", () => unary(mcpClient, "runMcpTool", { ...tool, runtimeBindingToken: binding(base, uid, 1) }, "runtime"), status.PERMISSION_DENIED, () => toolEffects);
    await expectCode("mcp", "runMcpTool", "runtime", () => unary(mcpClient, "runMcpTool", { ...tool, runtimeProcessId: "retired-process" }, "runtime"), status.PERMISSION_DENIED, () => toolEffects);
    await expectCode("mcp", "listMcpTools", "runner", () => unary(mcpClient, "listMcpTools", { ...list, sessionId: "" }, "runner"), status.INVALID_ARGUMENT, () => discoveryEffects);
}
finally {
    runtimeClient?.close();
    providerClient?.close();
    mcpClient?.close();
    let shutdownDeadline: ReturnType<typeof setTimeout> | undefined;
    try {
        const cleanup = await Promise.race([
            Promise.allSettled([runtime.shutdown(), provider.shutdown(), mcp.shutdown()]),
            new Promise<never>((_, reject) => {
                shutdownDeadline = setTimeout(() => reject(new Error("receiver cleanup exceeded 10 seconds")), 10000);
            }),
        ]);
        assert.deepEqual(cleanup.map(result => result.status), ["fulfilled", "fulfilled", "fulfilled"]);
    } finally {
        clearTimeout(shutdownDeadline);
    }
}
// The proof includes only fixed role labels/outcomes and counters, never credentials or binding tokens.
console.log(JSON.stringify({ rows, runtimeEffects, discoveryEffects, toolEffects, providerCredentialReads, providerPoolSelections, cleanup: "all receivers stopped" }));
