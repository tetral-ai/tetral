import { readFile } from "node:fs/promises";
import { Effect } from "effect";
import * as ThreadLoop from "@tetral/agent-runtime-core/src/thread-loop/thread-loop.js";
import { ThreadRuntime } from "@tetral/agent-runtime-core/src/thread-loop/thread-runtime.js";
import { lookupToolEntry } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import { runtimeToolPolicyForThread } from "../../src/command.js";
import { RecordingContextLoader, runtimeThreadLoopLayer, testRunCustody } from "../../../core/test/unit/thread-loop/thread-loop-test-support.js";
import { buildThreadLoopUserMessage } from "../../../core/test/unit/runtime-message-builders.js";

const inputPath = process.argv[2];
if (inputPath === undefined) throw new Error("fixture input path is required");
const input = JSON.parse(await readFile(inputPath, "utf8")) as {
  readonly workspaceId: string;
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly runtimeConfigPayloadJson: string;
  readonly readyManifestPayloadJson: string;
  readonly toolName: string;
};
const catalog = runtimeToolPolicyForThread(
  undefined,
  [input.runtimeConfigPayloadJson, input.readyManifestPayloadJson],
  "claude",
).toolCatalog;
if (lookupToolEntry(catalog, input.toolName) === undefined) {
  throw new Error("ready MCP tool is absent from Runtime Tool Catalog");
}

const providerToolNames: string[][] = [];
const loader = new RecordingContextLoader([], {
  type: "messages",
  messages: [buildThreadLoopUserMessage("msg_oauth_manifest", 1, "search the repository")],
});
const thread = new ThreadRuntime({
  workspaceId: input.workspaceId,
  sessionId: input.sessionId,
  sessionThreadId: input.sessionThreadId,
  bindingId: "bind_oauth_manifest",
  bindingGeneration: 1,
  targetPodUid: "pod_oauth_manifest",
  runtimeBindingToken: "runtime-binding-token",
});
const result = await Effect.runPromise(Effect.gen(function* () {
  const threadLoop = yield* ThreadLoop.Service;
  return yield* threadLoop.run(thread, testRunCustody());
}).pipe(Effect.provide(runtimeThreadLoopLayer(loader, {
  events: [
    { type: "text-start", id: "text-oauth-manifest" },
    { type: "text-delta", id: "text-oauth-manifest", text_delta: "ready" },
    { type: "text-end", id: "text-oauth-manifest" },
    { type: "finish", finishReason: "stop" },
  ],
  providerCallRuntime: {
    systemInstructions: "OAuth manifest composition provider request",
    toolCatalog: catalog,
    toolsetFamily: "claude",
  },
  approvalMode: "full_access",
  onStream: (request) => providerToolNames.push(request.tools.map((tool) => tool.name)),
}))));
if (result.type !== "completed" || providerToolNames.length !== 1) {
  throw new Error(`next provider request did not complete: ${JSON.stringify(result)}/${providerToolNames.length}`);
}
if (!providerToolNames[0]!.includes(input.toolName)) {
  throw new Error("refreshed MCP tool is absent from next provider request");
}
process.stdout.write(JSON.stringify({
  toolPresent: true,
  nextProviderRequests: providerToolNames.length,
}));
