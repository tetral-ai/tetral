import { readFile } from "node:fs/promises";
import { Metadata } from "@grpc/grpc-js";
import type { CallOptions } from "@grpc/grpc-js";
import type {
  AgentRuntimeBridgeServiceClient,
  SettleToolResultRequest,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { RuntimeFailureSchema, runtimeToolErrorFromFailure } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import { BridgeAPIEventWriter } from "../../src/bridge-client.js";

const inputPath = process.argv[2];
if (inputPath === undefined) throw new Error("durable Tool error declaration input path is required");
const input = JSON.parse(await readFile(inputPath, "utf8")) as {
  readonly workspaceId: string;
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly bindingId: string;
  readonly bindingGeneration: number;
  readonly targetPodUid: string;
  readonly modelRequestId: string;
  readonly toolUseEventId: string;
};

const failure = RuntimeFailureSchema.parse({
  type: "runtime",
  code: "provider_tool_protocol_error",
  message: "Read failed because the requested file does not exist.",
  retryable: false,
  fatal: true,
  retryStatus: { type: "terminal" },
});
const outcome = { type: "error" as const, error: failure };
let captured: SettleToolResultRequest | undefined;
const client = {
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
  client,
  metadataFactory: async () => new Metadata(),
});
const attempt = await writer.settleToolResult({
  workspaceId: input.workspaceId,
  sessionId: input.sessionId,
  sessionThreadId: input.sessionThreadId,
  bindingId: input.bindingId,
  bindingGeneration: input.bindingGeneration,
  targetPodUid: input.targetPodUid,
  settlement: { toolUseEventId: input.toolUseEventId, outcome },
});
if (!attempt.ok || attempt.result.type !== "committed" || captured?.settlement?.error === undefined) {
  throw new Error("Runtime Bridge adapter did not declare a Tool error settlement");
}

process.stdout.write(JSON.stringify({
  toolUseEventId: captured.settlement.toolUseEventId,
  errorJson: captured.settlement.error.errorJson,
  runtimeSettlement: outcome,
  expectedDurableError: runtimeToolErrorFromFailure(failure),
}));
