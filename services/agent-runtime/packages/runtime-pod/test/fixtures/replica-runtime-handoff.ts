import { access, readFile, writeFile } from "node:fs/promises";
import { writeJsonSnapshot } from "./json-snapshot.js";
import { appendFileSync } from "node:fs";
import { createJsonLogger } from "../../src/logger.js";
import { Metadata } from "@grpc/grpc-js";
import { createLLMService } from "@tetral/agent-runtime-core/src/llm/llm-service.js";
import { createApprovalReviewerToolCatalog, createToolCatalog, } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import { DefaultProviderCallRuntimeConfig } from "@tetral/agent-runtime-core/src/thread-loop/provider-request.js";
import { ProviderFinishReason, ProviderStreamEventType, ProviderRequestKind, } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { createRuntimeBindingTokenVerifier } from "../../../../../gateway/packages/protocol/src/binding-token.js";
import { ProviderGatewayServiceShell } from "../../../../../gateway/packages/provider-gateway/src/service.js";
import { createGatewayGrpcServer } from "../../../../../gateway/packages/provider-gateway/src/grpc-server.js";
import { BridgeAPIApprovalReviewerThreadCreator, BridgeAPIContextLoader, BridgeAPIControlInputCommitter, BridgeAPIEventWriter, } from "../../src/bridge-client.js";
import { RuntimePodGatewayClient } from "../../src/gateway-client.js";
import { RuntimePodToolRunner } from "../../src/tool-runner.js";
import { createRuntimeApprovalReviewer } from "../../src/approval-reviewer.js";
import type { RuntimeSubAgentRunHost } from "../../src/core-hosts.js";
import { buildRuntimeCoreHosts } from "../../src/core-hosts.js";
import { BridgeRuntimeProcess } from "../../src/runtime-process.js";
import { createRuntimePodApp } from "../../src/app.js";
import { loadRuntimePodConfig } from "../../src/config.js";
import { runtimeToolPolicyForThread } from "../../src/command.js";
const input = JSON.parse(await readFile(process.argv[2]!, "utf8")) as {
    bridgeAddress: string;
    podUID: string;
    processID: string;
    token: string;
    directory: string;
    replacement: boolean;
    loggerMode?: "normal" | "silent" | "throw";
    mcpAddress?: string;
    reviewerScenario?: "hold" | "allow" | "deny";
    unrelatedReviewer?: {
        sessionId: string;
        sessionThreadId: string;
        parentThreadId: string;
        bindingId: string;
        bindingGeneration: number;
    };
    idle: Array<{
        sessionId: string;
        sessionThreadId: string;
        bindingId: string;
        bindingGeneration: number;
    }>;
};
const exists = async (path: string) => {
    try {
        await access(path);
        return true;
    }
    catch {
        return false;
    }
};
const wait = async (path: string, signal?: AbortSignal) => {
    while (!(await exists(path))) {
        signal?.throwIfAborted();
        await new Promise((r) => setTimeout(r, 10));
    }
};
const logger = createJsonLogger({
    write: (line) => {
        if (input.loggerMode === "throw")
            throw new Error("diagnostic sink unavailable");
        if (input.loggerMode !== "silent")
            appendFileSync(`${input.directory}/diagnostics.jsonl`, line);
    },
});
const metadataFactory = async () => {
    const metadata = new Metadata();
    metadata.set("authorization", `Bearer ${input.token}`);
    return metadata;
};
const bridgeOptions = {
    address: input.bridgeAddress,
    tokenPath: "/unused",
    metadataFactory,
};
const loader = new BridgeAPIContextLoader(bridgeOptions), writer = new BridgeAPIEventWriter(bridgeOptions), committer = new BridgeAPIControlInputCommitter(bridgeOptions);
const calls: Record<string, number> = {}, reviewerCalls: Record<string, number> = {};
const ledger: Array<{
    sessionId: string;
    sessionThreadId: string;
    requestId: string;
    modelRequestId: string;
    ordinal: number;
    messagesJson: string;
}> = [];
const gatewayService = new ProviderGatewayServiceShell({
    ready: () => true,
    logger: {
        info: () => undefined, error: () => undefined
    },
    authenticator: {
        authenticate: async () => ({
            ok: true as const,
            serviceAccount: {
                namespace: "tetral-agent-runtime",
                name: "agent-runtime",
                podUid: input.podUID,
            },
        }),
    },
    runtimeBindingTokenVerifier: createRuntimeBindingTokenVerifier({
        hmacKey: "replica-handoff-shared-token-signing-key",
    }),
    providerStreamer: {
        stream: async function* (request) {
            const session = request.request.sessionId;
            const reviewer = request.request.requestKind ===
                ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER;
            const reviewOrdinal = reviewer
                ? (reviewerCalls[session] = (reviewerCalls[session] ?? 0) + 1)
                : 0;
            const ordinal = (calls[session] = (calls[session] ?? 0) + 1);
            ledger.push({
                sessionId: session,
                sessionThreadId: request.request.sessionThreadId,
                requestId: request.request.requestId,
                modelRequestId: request.request.modelRequestId,
                ordinal,
                messagesJson: JSON.stringify(request.request.context),
            });
            await writeJsonSnapshot(`${input.directory}/ledger.json`, ledger);
            yield {
                type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START,
                text: {
                    id: "frame", text: "", metadataJson: "{}"
                },
            };
            yield {
                type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA,
                text: {
                    id: "frame",
                    text: reviewer
                        ? reviewOrdinal > 1 || input.reviewerScenario === "hold"
                            ? JSON.stringify({
                                outcome: input.reviewerScenario === "deny" ? "deny" : "allow",
                                risk_level: "low",
                                user_authorization: "high",
                                rationale: "inspect admitted action",
                            })
                            : "Inspect current action"
                        : `${session} current step`,
                    metadataJson: "{}",
                },
            };
            // The consumer has received the text frame before this barrier is visible.
            await writeFile(`${input.directory}/${session}-${ordinal}.frame`, "delivered");
            if ((!reviewer &&
                (input.replacement ||
                    input.reviewerScenario !== undefined ||
                    session.endsWith("a1") ||
                    session.endsWith("a2"))) ||
                (reviewer && input.reviewerScenario === "hold"))
                await wait(`${input.directory}/${session}-${ordinal}.release`, request.abortSignal);
            yield {
                type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_END,
                text: {
                    id: "frame", text: "", metadataJson: "{}"
                },
            };
            const reviewRead = reviewer && reviewOrdinal === 1 && input.reviewerScenario !== "hold";
            const tool = reviewRead ||
                (!reviewer &&
                    !input.replacement &&
                    ordinal === 1 &&
                    (input.reviewerScenario !== undefined ||
                        session.endsWith("a1") ||
                        session.endsWith("b")));
            if (tool)
                yield {
                    type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL,
                    toolCall: {
                        id: "tool-current",
                        name: reviewRead || session.endsWith("b")
                            ? "Read"
                            : input.reviewerScenario !== undefined
                                ? "Write"
                                : "list_agents",
                        inputJson: reviewRead || session.endsWith("b")
                            ? '{"file_path":"/workspace/input.txt"}'
                            : input.reviewerScenario !== undefined
                                ? '{"file_path":"/workspace/output.txt","content":"reviewed output"}'
                                : "{}",
                        metadataJson: "{}",
                    },
                };
            yield {
                type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
                finish: {
                    reason: tool
                        ? ProviderFinishReason.PROVIDER_FINISH_REASON_TOOL_CALLS
                        : ProviderFinishReason.PROVIDER_FINISH_REASON_STOP,
                    contextWindowTokens: 200000,
                    outputTokenLimit: 32000,
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
    },
});
const gatewayServer = createGatewayGrpcServer(gatewayService), gatewayPort = await gatewayServer.bind("127.0.0.1:0");
const gatewayClient = new RuntimePodGatewayClient({
    logger,
    address: `127.0.0.1:${gatewayPort}`,
    tokenPath: "/unused",
    metadataFactory,
});
const toolRunner = new RuntimePodToolRunner({
    bridgeAddress: input.bridgeAddress,
    webAddress: "127.0.0.1:1",
    mcpConnectorAddress: input.mcpAddress ?? "127.0.0.1:1",
    tokenPath: "/unused",
    metadataFactory,
});
let sequence = 0, phaseDeadline: number | undefined, subAgentRunHost: RuntimeSubAgentRunHost | undefined;
const reviewerCreator = new BridgeAPIApprovalReviewerThreadCreator(bridgeOptions);
const hosts = await buildRuntimeCoreHosts({
    logger,
    maxLocalSessions: 16,
    contextLoader: loader,
    now: () => new Date().toISOString(),
    threadLoop: {
        internalToolRepairStore: {} as never,
        sessionEventWriter: writer,
        runtime: {
            now: () => new Date().toISOString(),
            monotonicMs: () => performance.now(),
            createId: (prefix) => `${prefix}_${input.processID}_${++sequence}`,
            sleep: (duration, signal) => new Promise<boolean>((resolve) => {
                if (signal.aborted) {
                    resolve(false);
                    return;
                }
                let timer: ReturnType<typeof setTimeout>;
                const finish = (completed: boolean) => {
                    clearTimeout(timer);
                    signal.removeEventListener("abort", abort);
                    resolve(completed);
                };
                const abort = () => finish(false);
                timer = setTimeout(() => finish(true), duration);
                signal.addEventListener("abort", abort, {
                    once: true
                });
                if (signal.aborted)
                    abort();
            }),
        },
        llmService: createLLMService(gatewayClient),
        storeOperationTimeoutMs: 35000,
        phaseDeadline: () => phaseDeadline,
        approvalMode: input.reviewerScenario === undefined ? "full_access" : "approve_for_me",
        reviewApproval: createRuntimeApprovalReviewer(() => subAgentRunHost, {
            model: {
                providerId: "anthropic", modelId: "claude-opus-4-8"
            },
            threadCreator: reviewerCreator,
            createId: (prefix) => `${prefix}_${input.processID}_${++sequence}`,
            logger,
        }),
        providerCallRuntime: {
            ...DefaultProviderCallRuntimeConfig,
            approvalReviewerPolicy: "Return the required approval decision JSON.",
            systemInstructions: "Replica handoff fixture",
            timeoutMs: 12000,
        },
        runtimeModel: () => ({
            providerId: "anthropic",
            modelId: "claude-opus-4-8",
        }),
        runtimePolicy: (session) => ({
            toolCatalog: input.mcpAddress !== undefined
                ? runtimeToolPolicyForThread(session.identity.threadRole, session.configuration.patches().map((patch) => patch.contentJson), session.configuration.installedBuiltinFamily(), createApprovalReviewerToolCatalog()).toolCatalog
                : session.identity.threadRole === "approval_reviewer"
                    ? createApprovalReviewerToolCatalog()
                    : createToolCatalog({
                        family: "claude"
                    }),
            approvalMode: session.identity.threadRole === "approval_reviewer"
                ? "full_access"
                : input.reviewerScenario === undefined
                    ? "full_access"
                    : "approve_for_me",
            providerRescheduleBudget: 0,
        }),
        runTool: (request) => toolRunner.runTool(request),
        acceptSandboxExecution: (request) => toolRunner.acceptSandboxExecution(request),
        awaitSandboxExecution: (request) => toolRunner.awaitSandboxExecution(request),
    },
});
subAgentRunHost = hosts.subAgentRunHost;
const parsed = loadRuntimePodConfig({
    TETRAL_RUNTIME_POD_NAMESPACE: "tetral-agent-runtime",
    TETRAL_RUNTIME_POD_NAME: "runtime-pod-0",
    TETRAL_RUNTIME_POD_UID: input.podUID,
    TETRAL_RUNTIME_POD_IP: "127.0.0.1",
    TETRAL_RUNTIME_POD_GRPC_PORT: "19090",
    TETRAL_RUNTIME_POD_HTTP_ADDR: "127.0.0.1:0",
    TETRAL_DEPLOYMENT_ENVIRONMENT: "test",
    TETRAL_SERVICE_VERSION: "test",
    TETRAL_RUNTIME_POD_GRPC_AUDIENCE: "tetral-internal-grpc",
    TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS: "tetral-system/job-runner",
    KUBERNETES_API_SERVER_URL: "https://unused",
    KUBERNETES_API_CA_CERT_PATH: "/unused",
    KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH: "/unused",
    TETRAL_RUNTIME_POD_OUTBOUND_GRPC_TOKEN_PATH: "/unused",
    TETRAL_BRIDGE_API_GRPC_ADDR: input.bridgeAddress,
    TETRAL_GATEWAY_GRPC_ADDR: `127.0.0.1:${gatewayPort}`,
    TETRAL_MCP_CONNECTOR_GRPC_ADDR: "127.0.0.1:1",
    TETRAL_WEB_CONNECTOR_GRPC_ADDR: "127.0.0.1:1",
    TETRAL_RUNTIME_APPROVAL_REVIEWER_MODEL: "anthropic/claude-opus-4-8",
    TETRAL_RUNTIME_SKILL_GUIDANCE_DESCRIPTION_BUDGET_BYTES: "32768",
    TETRAL_RUNTIME_DRAIN_TIMEOUT_MS: "2000",
    TETRAL_RUNTIME_SETTLEMENT_TIMEOUT_MS: "2000",
    TETRAL_RUNTIME_LOCAL_JOIN_TIMEOUT_MS: "1000",
    TETRAL_RUNTIME_PROXY_JOIN_TIMEOUT_MS: "1000",
});
if (!parsed.ok)
    throw new Error(parsed.error.message);
const config = {
    ...parsed.config, grpcBindAddress: "127.0.0.1:0"
};
const runtimeProcess = new BridgeRuntimeProcess(input.processID, {
    ...bridgeOptions,
    policies: config.bridgeMethodPolicies,
});
const app = createRuntimePodApp({
    readContainerMemory: () => ({
        usageBytes: 100, limitBytes: 1000
    }),
    config,
    logger,
    runtimeProcess,
    commandRunHost: hosts.commandRunHost,
    cleanupRunHost: hosts.cleanupRunHost,
    controlInputCommitter: committer,
    tokenReviewClient: {
        createTokenReview: async () => ({
            authenticated: true,
            username: "system:serviceaccount:tetral-system:job-runner",
            audiences: ["tetral-internal-grpc"],
        }),
    },
    quiesce: async (options) => {
        phaseDeadline = options.settlementDeadline;
        const bridgePhase = { currentStepDeadline: options.currentStepDeadline, settlementDeadline: options.settlementDeadline, settlementAttemptTimeoutMs: config.lifecycle.settlementAttemptTimeoutMs };
        writer.beginDrain(bridgePhase);
        loader.beginDrain(bridgePhase);
        committer.beginDrain(bridgePhase);
        toolRunner.beginDrain(bridgePhase);
        reviewerCreator.beginDrain(bridgePhase);
        await hosts.quiesce(options);
    },
    closeClients: async () => {
        await Promise.all([
            gatewayClient.close(),
            toolRunner.close(),
            loader.close(),
            writer.close(),
            committer.close(),
            reviewerCreator.close(),
        ]);
    },
});
try {
    const ready = await app.start();
    for (const idle of input.idle ?? [])
        await hosts.subAgentRunHost.preloadThread({
            ...idle,
            workspaceId: "default",
            targetPodUid: input.podUID,
            runtimeProcessId: input.processID,
        });
    await writeJsonSnapshot(`${input.directory}/ready.json`, {
        port: ready.grpcPort, httpUrl: ready.httpUrl.href
    });
    await wait(`${input.directory}/quiesce`);
    const shutdown = app.shutdown();
    await writeJsonSnapshot(`${input.directory}/quiescing.json`, app.lifecycle.metricsSnapshot());
    if (input.unrelatedReviewer !== undefined) {
        await wait(`${input.directory}/reject-unrelated-review`);
        const result = await hosts.subAgentRunHost.enqueueThreadInput({
            ...input.unrelatedReviewer,
            workspaceId: "default",
            targetPodUid: input.podUID,
            runtimeProcessId: input.processID,
            kind: "approval_review",
            runtimeInputId: "rin_unrelated_review",
            inputOrder: 999,
            reviewId: "arvw_unrelated_review",
            targetModelToolCallId: "tool-unrelated-to-admitted-parent",
            targetToolName: "Write",
            promptText: ["Inspect an unrelated action"],
            outputSchemaJson: '{"type":"object"}',
            thread: {
                parentThreadId: input.unrelatedReviewer.parentThreadId,
                role: "approval_reviewer",
                visibility: "internal",
                agentType: "approval_reviewer",
                status: "idle",
            },
        });
        await writeJsonSnapshot(`${input.directory}/unrelated-review.json`, result);
    }
    await shutdown;
    await hosts.close();
    await gatewayService.shutdown(new Date(Date.now() + 1000));
    await gatewayServer.shutdown(new Date(Date.now() + 1000));
    await writeJsonSnapshot(`${input.directory}/closed.json`, {
        ledger
    });
}
finally {
    await app.shutdown();
    await hosts.close();
    await gatewayClient.close();
    await gatewayServer.shutdown(new Date(Date.now() + 1000));
}
