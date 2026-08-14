import { readFile } from "node:fs/promises";
import { createToolCatalog, lookupToolEntry } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import { runtimeToolSettlement } from "@tetral/agent-runtime-core/src/thread-loop/tool-execution.js";
import type { RuntimeToolExecutionRequest } from "@tetral/agent-runtime-core/src/thread-loop/tool-execution.js";
import { BridgeAPIEventWriter } from "../../src/bridge-client.js";
import { RuntimePodToolRunner } from "../../src/tool-runner.js";

const inputPath = process.argv[2];
if (inputPath === undefined) throw new Error("Sandbox production composition input path is required");
const input = JSON.parse(await readFile(inputPath, "utf8")) as {
  readonly address: string;
  readonly tokenPath: string;
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

const entry = lookupToolEntry(createToolCatalog({ family: "gpt" }), "exec_command");
if (entry === undefined) throw new Error("exec_command is unavailable in the production catalog");
const request: RuntimeToolExecutionRequest = {
  workspaceId: input.workspaceId,
  sessionId: input.sessionId,
  sessionThreadId: input.sessionThreadId,
  bindingId: input.bindingId,
  bindingGeneration: input.bindingGeneration,
  runtimeBindingToken: "composition-binding-token",
  targetPodUid: input.targetPodUid,
  modelRequestId: input.modelRequestId,
  modelToolCallId: input.modelToolCallId,
  modelOrder: 0,
  toolUseEventId: input.toolUseEventId,
  entry,
  input: { cmd: "printf production-boundary" },
  committedMessages: [],
  abortSignal: new AbortController().signal,
};
const runner = new RuntimePodToolRunner({
  bridgeAddress: input.address,
  webAddress: "127.0.0.1:1",
  mcpConnectorAddress: "127.0.0.1:1",
  tokenPath: input.tokenPath,
  sleep: async () => {},
});
const result = await runner.runTool(request);
if (result.type !== "completed") throw new Error(`Sandbox production result was ${result.type}`);

const writer = new BridgeAPIEventWriter({
  address: input.address,
  tokenPath: input.tokenPath,
  sleep: async () => {},
});
const settlement = await writer.settleToolResult({
  workspaceId: input.workspaceId,
  sessionId: input.sessionId,
  sessionThreadId: input.sessionThreadId,
  bindingId: input.bindingId,
  bindingGeneration: input.bindingGeneration,
  targetPodUid: input.targetPodUid,
  settlement: { toolUseEventId: input.toolUseEventId, outcome: runtimeToolSettlement(result) },
});
if (!settlement.ok) throw settlement.error;
process.stdout.write(JSON.stringify({ result, settlement: settlement.result }));
