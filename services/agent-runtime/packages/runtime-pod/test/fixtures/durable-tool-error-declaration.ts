import { readFile } from "node:fs/promises";
import { Metadata } from "@grpc/grpc-js";
import {
  BridgeWriteStatus,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type {
  AgentRuntimeBridgeServiceClient,
  WriteEventRequest,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { RuntimeFailureSchema, runtimeToolErrorFromFailure } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import { runtimeToolResultEvent } from "@tetral/agent-runtime-core/src/runtime/accumulator.js";
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
const event = runtimeToolResultEvent(input.toolUseEventId, { kind: "tool" }, outcome);
let captured: WriteEventRequest | undefined;
const client = {
  writeEvent: (
    request: WriteEventRequest,
    _metadata: Metadata,
    callback: (error: Error | null, response: unknown) => void,
  ) => {
    captured = request;
    callback(null, {
      ack: {
        status: BridgeWriteStatus.BRIDGE_WRITE_STATUS_REJECTED,
        runtimeWriteId: request.runtimeWriteId,
        errorCode: "fixture_capture_complete",
      },
    });
    return { cancel() {} };
  },
} as unknown as AgentRuntimeBridgeServiceClient;
const writer = new BridgeAPIEventWriter({
  address: "bridge.test:9090",
  tokenPath: "/var/run/token",
  client,
  metadataFactory: async () => new Metadata(),
});
await writer.append({
  requestId: "req_durable_tool_error",
  workspaceId: input.workspaceId,
  sessionId: input.sessionId,
  sessionThreadId: input.sessionThreadId,
  bindingId: input.bindingId,
  bindingGeneration: input.bindingGeneration,
  targetPodUid: input.targetPodUid,
  writeId: "rwrite_durable_tool_error_result",
  modelRequestId: input.modelRequestId,
  event,
  toolSettlement: { toolUseEventId: input.toolUseEventId, outcome },
});
if (captured?.toolSettlement?.error === undefined) {
  throw new Error("Runtime Bridge adapter did not declare a Tool error settlement");
}

process.stdout.write(JSON.stringify({
  eventType: captured.eventType,
  payloadJson: captured.payloadJson,
  toolUseEventId: captured.toolSettlement.toolUseEventId,
  errorJson: captured.toolSettlement.error.errorJson,
  runtimeSettlement: outcome,
  expectedDurableError: runtimeToolErrorFromFailure(failure),
}));
