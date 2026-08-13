import { Effect } from "effect";
import { Metadata } from "@grpc/grpc-js";
import { createToolCatalog } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import { RuntimeInternalToolRepairStore } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import type {
  RuntimeDeclarationOperationControls,
  RuntimeInternalToolRepairCommit,
  SessionEventWriter,
} from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import type { LLMRequest } from "@tetral/agent-runtime-core/src/llm/llm-service.js";
import { ThreadRuntime } from "@tetral/agent-runtime-core/src/thread-loop/thread-runtime.js";
import * as ThreadLoop from "@tetral/agent-runtime-core/src/thread-loop/thread-loop.js";
import { BridgeAPIEventWriter, BridgeAPIInternalToolRepairCommitter } from "../../src/bridge-client.js";
import { buildThreadLoopUserMessage as userMessage } from "../../../core/test/unit/runtime-message-builders.js";
import {
  RecordingContextLoader,
  queuedLLMService,
  runtimeThreadLoopLayer,
  testRunCustody,
} from "../../../core/test/unit/thread-loop/thread-loop-test-support.js";

const address = process.argv[2];
const rawIdentity = process.argv[3];
if (address === undefined || rawIdentity === undefined) {
  throw new Error("Bridge address and Runtime thread identity are required");
}
const identity = JSON.parse(rawIdentity) as {
  readonly workspaceId: string;
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly bindingId: string;
  readonly bindingGeneration: number;
  readonly targetPodUid: string;
  readonly runtimeBindingToken: string;
};
const session = new ThreadRuntime(identity);
const storeOrder: string[] = [];
const publicToolEvents: string[] = [];
const providerRequests: LLMRequest[] = [];
let runToolCalls = 0;
let acceptSandboxExecutionCalls = 0;
let awaitSandboxExecutionCalls = 0;

const bridgeOptions = {
  address,
  tokenPath: "/unused/test-token",
  metadataFactory: async () => new Metadata(),
};
const productionWriter = new BridgeAPIEventWriter(bridgeOptions);
const writer: SessionEventWriter = {
  append: async (envelope) => {
    if (
      envelope.event.type === "agent.tool_use" ||
      envelope.event.type === "agent.tool_result" ||
      envelope.event.type === "agent.mcp_tool_use" ||
      envelope.event.type === "agent.mcp_tool_result"
    ) {
      publicToolEvents.push(envelope.event.type);
    }
    return await productionWriter.append(envelope);
  },
  writeRequestEnd: async (envelope) => await productionWriter.writeRequestEnd(envelope),
  finishIdle: async (envelope) => await productionWriter.finishIdle(envelope),
  commitRuntimeTermination: async (envelope) => await productionWriter.commitRuntimeTermination(envelope),
};

class RecordingBridgeRepairStore extends RuntimeInternalToolRepairStore {
  readonly repairs: RuntimeInternalToolRepairCommit[] = [];
  private readonly committer = new BridgeAPIInternalToolRepairCommitter(bridgeOptions);

  protected async commitInternalToolRepairRecord(
    repair: RuntimeInternalToolRepairCommit,
    _controls: RuntimeDeclarationOperationControls,
  ): Promise<unknown> {
    this.repairs.push(repair);
    storeOrder.push("store:internal-tool-repair");
    return await this.committer.commitInternalToolRepair(repair);
  }
}
const store = new RecordingBridgeRepairStore();

const result = await Effect.runPromise(Effect.gen(function* () {
  const threadLoop = yield* ThreadLoop.Service;
  return yield* threadLoop.run(session, testRunCustody());
}).pipe(Effect.provide(runtimeThreadLoopLayer(
  new RecordingContextLoader([], {
    type: "messages",
    messages: [userMessage("user-invalid-tool-repair-fixture", 0, "continue")],
  }),
  {
    store,
    writer,
    llmService: queuedLLMService([
      [
        { type: "text-start", id: "text-before-invalid-tool" },
        { type: "text-delta", id: "text-before-invalid-tool", text_delta: "continuing" },
        { type: "text-end", id: "text-before-invalid-tool" },
        {
          type: "tool-call",
          id: "call_invalid_tool_repair_fixture",
          toolName: "exec_command",
          input: { command: "must not execute" },
          inputPreview: { preview: "{\"command\":\"must not execute\"}", truncated: false },
        },
        { type: "finish", finishReason: "tool-calls" },
      ],
      [
        { type: "text-start", id: "text-invalid-tool-repaired" },
        { type: "text-delta", id: "text-invalid-tool-repaired", text_delta: "continued" },
        { type: "text-end", id: "text-invalid-tool-repaired" },
        { type: "finish", finishReason: "stop" },
      ],
    ], providerRequests),
    providerCallRuntime: { systemInstructions: "invalid-tool repair fixture" },
    runtimePolicy: () => ({ toolCatalog: createToolCatalog({ family: "claude" }) }),
    runTool: () => {
      runToolCalls += 1;
      return { type: "completed", output: { text: "must not run", truncated: false } };
    },
    acceptSandboxExecution: () => {
      acceptSandboxExecutionCalls += 1;
      return { type: "accepted" };
    },
    awaitSandboxExecution: () => {
      awaitSandboxExecutionCalls += 1;
      return { type: "completed", output: { text: "must not await", truncated: false } };
    },
  },
))));

const repair = store.repairs[0];
if (result.type !== "completed" || repair === undefined || store.repairs.length !== 1) {
  throw new Error(`Runtime invalid-tool fixture did not produce one completed repair: ${JSON.stringify({ result, repairs: store.repairs })}`);
}

process.stdout.write(JSON.stringify({
  resultType: result.type,
  repair,
  storeOrder,
  publicToolEvents,
  runToolCalls,
  acceptSandboxExecutionCalls,
  awaitSandboxExecutionCalls,
  providerRequests,
}));
