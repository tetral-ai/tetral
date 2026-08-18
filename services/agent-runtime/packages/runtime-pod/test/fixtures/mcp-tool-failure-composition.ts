import { readFile } from "node:fs/promises";
import type { CallOptions } from "@grpc/grpc-js";
import { Metadata } from "@grpc/grpc-js";
import type { RuntimeToolExecutionRequest } from "@tetral/agent-runtime-core/src/thread-loop/tool-execution.js";
import { runtimeToolSettlement } from "@tetral/agent-runtime-core/src/thread-loop/tool-execution.js";
import {
	createToolCatalog,
	lookupToolEntry,
} from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import type {
	AgentRuntimeBridgeServiceClient,
	SettleToolResultRequest,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type {
	McpConnectorServiceClient,
	ProviderGatewayServiceClient,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import {
	McpErrorKind,
	McpRetryStatus,
	RunMcpToolStatus,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { BridgeAPIEventWriter } from "../../src/bridge-client.js";
import type { RuntimePodToolRunnerOptions } from "../../src/tool-runner.js";
import { RuntimePodToolRunner } from "../../src/tool-runner.js";

const inputPath = process.argv[2];
if (inputPath === undefined) throw new Error("fixture input path is required");
const input = JSON.parse(await readFile(inputPath, "utf8")) as {
	readonly workspaceId: string;
	readonly sessionId: string;
	readonly sessionThreadId: string;
	readonly bindingId: string;
	readonly bindingGeneration: number;
	readonly targetPodUid: string;
	readonly modelRequestId: string;
	readonly modelToolCallId: string;
	readonly toolUseEventId: string;
};
let connectorCalls = 0;
const mcpConnectorClient = {
	runMcpTool: (
		_request: unknown,
		_metadata: Metadata,
		callback: (error: Error | null, response: unknown) => void,
	) => {
		connectorCalls++;
		callback(null, {
			status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_RUNTIME_ERROR,
			resultText: "credential and provider response must not escape",
			attachments: [],
			errorKind: McpErrorKind.MCP_ERROR_KIND_AUTHENTICATION_FAILED,
			retryStatus: McpRetryStatus.MCP_RETRY_STATUS_TERMINAL,
		});
		return { cancel() {} };
	},
} as unknown as Pick<McpConnectorServiceClient, "runMcpTool">;
const runner = new RuntimePodToolRunner({
	bridgeAddress: "bridge.test:9090",
	webAddress: "gateway.test:9090",
	mcpConnectorAddress: "gateway.test:9091",
	tokenPath: "/var/run/token",
	bridgeClient: {} as NonNullable<RuntimePodToolRunnerOptions["bridgeClient"]>,
	webClient: {} as Pick<ProviderGatewayServiceClient, "runWeb">,
	mcpConnectorClient,
	metadataFactory: async () => new Metadata(),
});
const catalog = createToolCatalog({
	family: "claude",
	mcpManifests: [
		{
			mcpServerName: "github",
			manifestETag: "etag_failure",
			manifestGeneration: 1,
			tools: [
				{
					name: "github_search",
					description: "Search GitHub",
					inputSchema: { type: "object" },
				},
			],
		},
	],
});
const entry = lookupToolEntry(catalog, "github_search");
if (entry === undefined)
	throw new Error("MCP tool is unavailable in fixture catalog");
const request: RuntimeToolExecutionRequest = {
	workspaceId: input.workspaceId,
	sessionId: input.sessionId,
	sessionThreadId: input.sessionThreadId,
	bindingId: input.bindingId,
	bindingGeneration: input.bindingGeneration,
	runtimeBindingToken: "fixture-binding-token",
	targetPodUid: input.targetPodUid,
	modelRequestId: input.modelRequestId,
	modelToolCallId: input.modelToolCallId,
	modelOrder: 0,
	toolUseEventId: input.toolUseEventId,
	entry,
	input: { query: "tetral" },
	abortSignal: new AbortController().signal,
};
const result = await runner.runTool(request);
if (result.type === "stale_custody")
	throw new Error("MCP failure lost result custody");
const settlement = runtimeToolSettlement(result);
let captured: SettleToolResultRequest | undefined;
const bridgeClient = {
	settleToolResult: (
		request: SettleToolResultRequest,
		_metadata: Metadata,
		_options: CallOptions,
		callback: (error: Error | null, response: unknown) => void,
	) => {
		captured = request;
		callback(null, { committed: {} });
		return { cancel() {} };
	},
} as unknown as AgentRuntimeBridgeServiceClient;
const writer = new BridgeAPIEventWriter({
	address: "bridge.test:9090",
	tokenPath: "/var/run/token",
	client: bridgeClient,
	metadataFactory: async () => new Metadata(),
});
const attempt = await writer.settleToolResult({
	workspaceId: input.workspaceId,
	sessionId: input.sessionId,
	sessionThreadId: input.sessionThreadId,
	bindingId: input.bindingId,
	bindingGeneration: input.bindingGeneration,
	targetPodUid: input.targetPodUid,
	settlement: { toolUseEventId: input.toolUseEventId, outcome: settlement },
});
if (
	!attempt.ok ||
	attempt.result.type !== "committed" ||
	captured?.settlement?.error === undefined
) {
	throw new Error("Runtime Bridge adapter did not declare the MCP Tool error");
}
process.stdout.write(
	JSON.stringify({
		result,
		settlement,
		connectorCalls,
		declaredError: JSON.parse(captured.settlement.error.errorJson) as unknown,
	}),
);
