import { Effect } from "effect";
import { createToolCatalog } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import { ThreadRuntime } from "@tetral/agent-runtime-core/src/thread-loop/thread-runtime.js";
import * as ThreadLoop from "@tetral/agent-runtime-core/src/thread-loop/thread-loop.js";
import { buildThreadLoopUserMessage as userMessage } from "../../../core/test/unit/runtime-message-builders.js";
import {
  RecordingContextLoader,
  ThreadLoopRuntimeStore,
  createdAt,
  queuedLLMService,
  runtimeThreadLoopLayer,
  testRunCustody,
  writerFrom,
} from "../../../core/test/unit/thread-loop/thread-loop-test-support.js";

const session = new ThreadRuntime("sesn_invalid_tool_repair_fixture");
const storeOrder: string[] = [];
const store = new ThreadLoopRuntimeStore(storeOrder);
const publicToolEvents: string[] = [];
let runToolCalls = 0;
let acceptSandboxExecutionCalls = 0;
let awaitSandboxExecutionCalls = 0;

const writer = writerFrom((envelope) => {
  if (
    envelope.event.type === "agent.tool_use" ||
    envelope.event.type === "agent.tool_result" ||
    envelope.event.type === "agent.mcp_tool_use" ||
    envelope.event.type === "agent.mcp_tool_result"
  ) {
    publicToolEvents.push(envelope.event.type);
  }
  return {
    ok: true,
    writeId: envelope.writeId,
    eventId: `bridge-${envelope.writeId}`,
    processedAt: createdAt,
  };
});

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
    ]),
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
}));
