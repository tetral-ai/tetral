import { readFile } from "node:fs/promises";
import { Metadata } from "@grpc/grpc-js";
import {
  RuntimeCommandKind,
  RuntimeCommandStatus,
} from "@tetral/agent-runtime-protocol/src/gen/tetral/agent_runtime/v1/agent_runtime.js";
import {
  BridgeWriteStatus,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type {
  AgentRuntimeBridgeServiceClient,
  CommitTaskNotificationResultRequest,
  CommitTaskNotificationResultResponse,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { acceptedInputCreates, applyAcceptedInputReceipt } from "@tetral/agent-runtime-core/src/runtime/runtime-declaration.js";
import { extractThreadTurnCheckpoint } from "@tetral/agent-runtime-core/src/thread-loop/thread-turn-checkpoint.js";
import { deriveThreadTurnDecision } from "@tetral/agent-runtime-core/src/thread-loop/thread-turn-reducer.js";
import type { RuntimeAcceptedInputState } from "@tetral/agent-runtime-core/src/thread-loop/thread-state.js";
import { BridgeAPIContextLoader } from "../../src/bridge-client.js";
import { taskNotificationDeclarationDigest } from "../../src/runtime-declaration-wire.js";
import { RuntimeControlService } from "../../src/runtime-service.js";
import type {
  RuntimeCleanupController,
  RuntimeSessionRunHost,
} from "../../src/runtime-service.js";

const inputPath = process.argv[2];
if (inputPath === undefined) {
  throw new Error("task notification composition input path is required");
}
const input = JSON.parse(await readFile(inputPath, "utf8")) as {
  readonly payloadJson: string;
  readonly workspaceId: string;
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly bindingId: string;
  readonly bindingGeneration: number;
  readonly targetPodUid: string;
  readonly runtimeInputId: string;
  readonly commitResponse?: CommitTaskNotificationResultResponse;
};

let acceptedInput: RuntimeAcceptedInputState | undefined;
const runHost = {
  handleTaskNotification: async (_sessionId, command) => {
    acceptedInput = { kind: "task_notification", ...command };
    return { ok: true as const, sessionId: command.sessionId, created: false, applied: true };
  },
  handleAcceptInput: async () => { throw new Error("unexpected message command"); },
  handleAgentMail: async () => { throw new Error("unexpected agent mail command"); },
  handleInterruptControl: async () => { throw new Error("unexpected interrupt command"); },
  handleToolConfirmation: async () => { throw new Error("unexpected tool confirmation command"); },
  handleRuntimeConfigPatch: async () => { throw new Error("unexpected runtime config command"); },
} satisfies RuntimeSessionRunHost;
const cleanupController = {
  startCleanup: async () => { throw new Error("unexpected cleanup command"); },
} satisfies RuntimeCleanupController;
const service = new RuntimeControlService({
  ownPod: { namespace: "engine", name: "runtime-pod-composition", uid: input.targetPodUid, ip: "127.0.0.1" },
  allowedBridge: { namespace: "engine", name: "bridge" },
  authenticator: {
    authenticate: async () => ({ ok: true as const, serviceAccount: { namespace: "engine", name: "bridge" } }),
  },
  runHost,
  cleanupController,
  logger: { info: () => undefined, warn: () => undefined, error: () => undefined } as never,
  ready: () => true,
});
const response = await service.acceptTaskNotification({
  requestId: "req_task_notification_composition",
  workspaceId: input.workspaceId,
  sessionId: input.sessionId,
  sessionThreadId: input.sessionThreadId,
  bindingId: input.bindingId,
  bindingGeneration: input.bindingGeneration,
  targetPodNamespace: "engine",
  targetPodName: "runtime-pod-composition",
  targetPodUid: input.targetPodUid,
  targetPodIp: "127.0.0.1",
  runtimeInputId: input.runtimeInputId,
  eventIds: [],
  sequenceFrom: 0,
  sequenceTo: 0,
  commandKind: RuntimeCommandKind.RUNTIME_COMMAND_KIND_TASK_NOTIFICATION,
  payloadJson: input.payloadJson,
}, new Metadata());
if (response.status !== RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_ACCEPTED || acceptedInput === undefined) {
  throw new Error("Runtime did not accept the task notification");
}

let declaration: CommitTaskNotificationResultRequest | undefined;
const loader = new BridgeAPIContextLoader({
  address: "unused.test",
  tokenPath: "/unused/token",
  metadataFactory: async () => new Metadata(),
  client: {
    commitTaskNotificationResult: (
      request: CommitTaskNotificationResultRequest,
      _metadata: Metadata,
      callback: (error: Error | null, value: unknown) => void,
    ) => {
      declaration = request;
      callback(null, input.commitResponse ?? {
        ack: {
          status: BridgeWriteStatus.BRIDGE_WRITE_STATUS_REJECTED,
          runtimeInputId: request.runtimeInputId,
          errorCode: "task_notification_result_invalid",
        },
      });
      return { cancel: () => undefined };
    },
  } as unknown as AgentRuntimeBridgeServiceClient,
});
const creates = acceptedInputCreates(acceptedInput);
const commitResult = await loader.commitAcceptedInput(acceptedInput, { messageCreates: creates });
if (declaration === undefined) {
  throw new Error("Runtime declaration did not cross the Bridge adapter");
}
let durableMessages: readonly unknown[] = [];
let reducerAction: unknown;
if (commitResult.type === "receipt") {
  durableMessages = applyAcceptedInputReceipt(acceptedInput, creates, commitResult.receipt);
  const checkpoint = extractThreadTurnCheckpoint({
    messages: durableMessages as never,
    facts: {
      events: [],
      internalRepairs: [],
    },
  });
  reducerAction = deriveThreadTurnDecision(checkpoint, { routes: [] }).action;
}
process.stdout.write(JSON.stringify({
  declaration,
  declarationDigest: taskNotificationDeclarationDigest(declaration),
  durableMessages,
  reducerAction,
}));
