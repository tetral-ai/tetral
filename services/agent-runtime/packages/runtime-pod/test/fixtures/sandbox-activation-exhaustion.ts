import { readFile } from "node:fs/promises";
import { Metadata } from "@grpc/grpc-js";
import { createToolCatalog, lookupToolEntry } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import { runtimeToolResultEvent } from "@tetral/agent-runtime-core/src/runtime/accumulator.js";
import { runtimeToolSettlement } from "@tetral/agent-runtime-core/src/thread-loop/tool-execution.js";
import type { RuntimeJsonValue } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import type { RuntimeToolExecutionRequest } from "@tetral/agent-runtime-core/src/thread-loop/tool-execution.js";
import type { ProviderGatewayServiceClient } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { McpConnectorServiceClient } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { RuntimePodToolRunner } from "../../src/tool-runner.js";
import type { RuntimePodToolRunnerOptions } from "../../src/tool-runner.js";

interface FixtureInput {
  readonly workspaceId: string;
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly bindingId: string;
  readonly bindingGeneration: number;
  readonly targetPodUid: string;
  readonly modelRequestId: string;
  readonly modelToolCallId: string;
  readonly toolUseEventId: string;
  readonly resultJson: string;
  readonly resultDigest: string;
}

const inputPath = process.argv[2];
if (inputPath === undefined) {
  throw new Error("fixture input path is required");
}
const input = JSON.parse(await readFile(inputPath, "utf8")) as FixtureInput;
const bridgeClient = {
  awaitSandboxExecution: (
    _request: unknown,
    _metadata: Metadata,
    callback: (error: Error | null, response: unknown) => void,
  ) => {
    callback(null, {
      resultJson: input.resultJson,
      resultDigest: input.resultDigest,
      backgroundTaskStarted: false,
      taskId: "",
    });
    return { cancel() {} };
  },
} as unknown as NonNullable<RuntimePodToolRunnerOptions["bridgeClient"]>;
const runner = new RuntimePodToolRunner({
  bridgeAddress: "bridge.test:9090",
  webAddress: "gateway.test:9090",
  mcpConnectorAddress: "gateway.test:9091",
  tokenPath: "/var/run/token",
  bridgeClient,
  webClient: {} as Pick<ProviderGatewayServiceClient, "runWeb">,
  mcpConnectorClient: {} as Pick<McpConnectorServiceClient, "runMcpTool">,
  metadataFactory: async () => new Metadata(),
});

function request(toolName: string, value: RuntimeJsonValue): RuntimeToolExecutionRequest {
  const catalog = createToolCatalog({ family: toolName === "exec_command" ? "gpt" : "claude" });
  const entry = lookupToolEntry(catalog, toolName);
  if (entry === undefined) {
    throw new Error(`fixture tool ${toolName} is unavailable`);
  }
  return {
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
    input: value,
    committedMessages: [],
    abortSignal: new AbortController().signal,
  };
}

const commandResult = await runner.awaitSandboxExecution(request("exec_command", { cmd: "true" }));
const fileResult = await runner.awaitSandboxExecution(request("Write", { content: "ok", file_path: "result.txt" }));
if (commandResult.type !== "error" || fileResult.type !== "error") {
  throw new Error("activation exhaustion did not produce Runtime errors");
}
const settlement = runtimeToolSettlement(commandResult);
const event = runtimeToolResultEvent(input.toolUseEventId, { kind: "tool" }, settlement);
console.log(JSON.stringify({ commandResult, fileResult, settlement, event }));
process.exit(0);
