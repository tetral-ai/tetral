import { Metadata } from "@grpc/grpc-js";
import {
	createToolCatalog,
	lookupToolEntry,
} from "../../core/src/tools/tool-catalog.js";
import { RuntimePodToolRunner } from "../src/tool-runner.js";

const address = process.env.TETRAL_E2E_BRIDGE_ADDRESS;
if (address === undefined || address === "") {
	throw new Error("TETRAL_E2E_BRIDGE_ADDRESS is required");
}
const entry = lookupToolEntry(createToolCatalog({ family: "claude" }), "Bash");
if (entry === undefined) {
	throw new Error("Claude Bash is unavailable");
}
const runner = new RuntimePodToolRunner({
	bridgeAddress: address,
	webAddress: "127.0.0.1:1",
	mcpConnectorAddress: "127.0.0.1:1",
	tokenPath: "/dev/null",
	metadataFactory: async () => new Metadata(),
});

const result = await runner.runTool({
	workspaceId: "default",
	sessionId: "sesn_real_bash",
	sessionThreadId: "thr_real_bash",
	bindingId: "bind_real_bash",
	bindingGeneration: 1,
	runtimeBindingToken: "unused-by-bridge-test",
	targetPodUid: "pod_real_bash",
	modelRequestId: "mreq_real_bash",
	modelToolCallId: "call_real_bash",
	modelOrder: 0,
	toolUseEventId: "sevt_real_bash",
	entry,
	input: { command: "sleep 60", timeout: 120_000, run_in_background: true },
	currentModel: { providerId: "anthropic", modelId: "claude-opus-4-8" },
	committedContext: [],
	abortSignal: new AbortController().signal,
});
process.stdout.write(JSON.stringify(result));
