import { readFile } from "node:fs/promises";
import { Metadata } from "@grpc/grpc-js";
import { runtimeToolResultEvent } from "@tetral/agent-runtime-core/src/runtime/accumulator.js";
import { runtimeToolSettlement } from "@tetral/agent-runtime-core/src/thread-loop/tool-execution.js";
import { createToolCatalog, lookupToolEntry } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import { McpErrorKind, McpRetryStatus, RunMcpToolStatus } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { RuntimeToolExecutionRequest } from "@tetral/agent-runtime-core/src/thread-loop/tool-execution.js";
import type { McpConnectorServiceClient, ProviderGatewayServiceClient } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { RuntimePodToolRunner } from "../../src/tool-runner.js";
import type { RuntimePodToolRunnerOptions } from "../../src/tool-runner.js";

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
  readonly materializationHandle: string;
};
let connectorCalls = 0;
const mcpConnectorClient = {
  runMcpTool: (_request: unknown, _metadata: Metadata, callback: (error: Error | null, response: unknown) => void) => {
    connectorCalls++;
    callback(null, {
      status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_RUNTIME_ERROR,
      resultText: "credential and provider response must not escape",
      attachments: [],
      errorKind: McpErrorKind.MCP_ERROR_KIND_AUTHENTICATION_FAILED,
      retryStatus: McpRetryStatus.MCP_RETRY_STATUS_TERMINAL,
      materializationHandle: input.materializationHandle,
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
  mcpManifests: [{
    mcpServerName: "github",
    manifestETag: "etag_failure",
    manifestGeneration: 1,
    tools: [{ name: "github_search", description: "Search GitHub", inputSchema: { type: "object" } }],
  }],
});
const entry = lookupToolEntry(catalog, "github_search");
if (entry === undefined) throw new Error("MCP tool is unavailable in fixture catalog");
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
  committedMessages: [],
  abortSignal: new AbortController().signal,
};
const result = await runner.runTool(request);
if (result.type === "stale_custody") throw new Error("MCP failure lost materialization custody");
const settlement = runtimeToolSettlement(result);
const event = runtimeToolResultEvent(input.toolUseEventId, { kind: "mcp", mcpServerName: "github" }, settlement);
process.stdout.write(JSON.stringify({ result, settlement, event, connectorCalls }));
