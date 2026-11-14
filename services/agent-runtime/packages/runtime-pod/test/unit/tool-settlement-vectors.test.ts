// Layer 1: the Runtime's tool settlements against the pinned vectors, in
// process.
import { afterAll, describe, expect, test } from "bun:test";
import { readdirSync } from "node:fs";
import type { CallOptions } from "@grpc/grpc-js";
import { Metadata } from "@grpc/grpc-js";
import { runtimeToolSettlement } from "@tetral/agent-runtime-core/src/thread-loop/tool-execution.js";
import { createToolCatalog, lookupToolEntry } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import type {
	AgentRuntimeBridgeServiceClient,
	SettleToolResultRequest,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { RuntimeToolSettlement } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type {
	McpConnectorServiceClient,
	ProviderGatewayServiceClient,
	RunMcpToolResponse,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import {
	McpErrorKind,
	McpRetryStatus,
	RunMcpToolStatus,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { BridgeAPIEventWriter } from "../../src/bridge-client.js";
import { RuntimePodToolRunner } from "../../src/tool-runner.js";
import type { RuntimePodToolRunnerOptions } from "../../src/tool-runner.js";
import { pinnedVectors, pinVector } from "./vector-pin.js";

// The settlements the Runtime sends Bridge for one Tool outcome, read by
// Bridge's tests (see protocol/testdata/README.md).
const toolSettlementVectors = new URL("../../../protocol/testdata/tool-settlements/", import.meta.url);

// Each row is the MCP Connector's answer to one Tool call.
const rows: Record<string, RunMcpToolResponse> = {
	"mcp-authorization-failure": {
		status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_RUNTIME_ERROR,
		resultText: "credential and provider response must not escape",
		attachments: [],
		errorKind: McpErrorKind.MCP_ERROR_KIND_AUTHENTICATION_FAILED,
		retryStatus: McpRetryStatus.MCP_RETRY_STATUS_TERMINAL,
	},
};

const ran = new Set<string>();

describe("Tool settlement vectors", () => {
	test("every vector has a row and every row a vector", () => {
		expect(
			readdirSync(toolSettlementVectors)
				.filter((name) => name.endsWith(".json"))
				.map((name) => name.slice(0, -".json".length))
				.sort(),
		).toEqual(Object.keys(rows).sort());
	});

	afterAll(() => {
		const unpinned = [...ran].filter(
			(name) => !pinnedVectors.has(new URL(`${name}.json`, toolSettlementVectors).href),
		);
		if (unpinned.length > 0) throw new Error(`Tool settlements not pinned: ${unpinned.join(", ")}`);
	});

	for (const [name, response] of Object.entries(rows)) {
		test(`${name} is the settlement the Runtime sends`, async () => {
			ran.add(name);
			pinVector(
				new URL(`${name}.json`, toolSettlementVectors),
				`${JSON.stringify(RuntimeToolSettlement.toJSON(await settlementFor(response)), null, 2)}\n`,
				"Bridge's tests",
			);
		});
	}
});

// settlementFor runs one MCP Tool call through the production runner and
// returns the settlement the production Bridge writer puts on the wire.
async function settlementFor(response: RunMcpToolResponse): Promise<RuntimeToolSettlement> {
	const runner = new RuntimePodToolRunner({
		bridgeAddress: "bridge.test:9090",
		webAddress: "gateway.test:9090",
		mcpConnectorAddress: "gateway.test:9091",
		tokenPath: "/var/run/token",
		bridgeClient: {} as NonNullable<RuntimePodToolRunnerOptions["bridgeClient"]>,
		webClient: {} as Pick<ProviderGatewayServiceClient, "runWeb">,
		mcpConnectorClient: {
			runMcpTool: (
				_request: unknown,
				_metadata: Metadata,
				callback: (error: Error | null, answer: RunMcpToolResponse) => void,
			) => {
				callback(null, response);
				return { cancel() {} };
			},
		} as unknown as Pick<McpConnectorServiceClient, "runMcpTool">,
		metadataFactory: async () => new Metadata(),
	});
	const entry = lookupToolEntry(
		createToolCatalog({
			family: "claude",
			mcpManifests: [
				{
					mcpServerName: "github",
					manifestETag: "etag_settlement",
					manifestGeneration: 1,
					tools: [{ name: "github_search", description: "Search GitHub", inputSchema: { type: "object" } }],
				},
			],
		}),
		"github_search",
	);
	if (entry === undefined) throw new Error("MCP Tool missing from the catalog");
	const scope = {
		workspaceId: "default",
		sessionId: "sesn_settlement",
		sessionThreadId: "thr_settlement",
		bindingId: "bind_settlement",
		bindingGeneration: 1,
		targetPodUid: "pod_settlement",
		runtimeProcessId: "process_settlement",
	};
	const result = await runner.runTool({
		...scope,
		runtimeBindingToken: "settlement-binding-token",
		modelRequestId: "mreq_settlement",
		modelToolCallId: "call_settlement",
		modelOrder: 0,
		toolUseEventId: "sevt_tool_shape",
		entry,
		input: { query: "tetral" },
		retainedContextEntries: [],
		abortSignal: new AbortController().signal,
	});
	if (result.type === "stale_custody") throw new Error("MCP Tool lost result custody");
	let captured: SettleToolResultRequest | undefined;
	const writer = new BridgeAPIEventWriter({
		address: "bridge.test:9090",
		tokenPath: "/var/run/token",
		client: {
			settleToolResult: (
				request: SettleToolResultRequest,
				_metadata: Metadata,
				_options: CallOptions,
				callback: (error: Error | null, answer: unknown) => void,
			) => {
				captured = request;
				callback(null, { committed: {} });
				return { cancel() {} };
			},
		} as unknown as AgentRuntimeBridgeServiceClient,
		metadataFactory: async () => new Metadata(),
	});
	const settled = await writer.settleToolResult({
		...scope,
		settlement: { toolUseEventId: "sevt_tool_shape", outcome: runtimeToolSettlement(result) },
	});
	if (!settled.ok || captured?.settlement === undefined) {
		throw new Error("the Bridge writer sent no settlement");
	}
	return captured.settlement;
}
