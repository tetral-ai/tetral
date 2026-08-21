import { access, readFile } from "node:fs/promises";
import type { ProviderStreamAccumulatorWriter } from "@tetral/agent-runtime-core/src/runtime/accumulator.js";
import { ProviderStreamAccumulator } from "@tetral/agent-runtime-core/src/runtime/accumulator.js";
import { ContextManager } from "@tetral/agent-runtime-core/src/session/context-manager.js";
import type {
	RuntimeToolExecutionRequest,
	RuntimeToolRegistrationState,
} from "@tetral/agent-runtime-core/src/thread-loop/tool-execution.js";
import {
	distinctProviderInputForEntry,
	publicInputForRegisteredTool,
	publicToolEventForEntry,
	registerRuntimeToolCall,
	routeCapabilityForEntry,
	runtimeToolSettlement,
} from "@tetral/agent-runtime-core/src/thread-loop/tool-execution.js";
import { createToolCatalog } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import { evaluateToolGate } from "@tetral/agent-runtime-core/src/tools/tool-gate.js";
import { ToolScheduler } from "@tetral/agent-runtime-core/src/tools/tool-scheduler.js";
import { BridgeAPIEventWriter } from "../../src/bridge-client.js";
import { RuntimePodToolRunner } from "../../src/tool-runner.js";

const inputPath = process.argv[2];
if (inputPath === undefined)
	throw new Error("Background command abort composition input path is required");
const input = JSON.parse(await readFile(inputPath, "utf8")) as {
	readonly address: string;
	readonly tokenPath: string;
	readonly abortPath: string;
	readonly workspaceId: string;
	readonly sessionId: string;
	readonly sessionThreadId: string;
	readonly bindingId: string;
	readonly bindingGeneration: number;
	readonly targetPodUid: string;
	readonly runtimeBindingToken: string;
	readonly modelRequestId: string;
	readonly modelToolCallId: string;
	readonly taskId: string;
};

const writer = new BridgeAPIEventWriter({
	address: input.address,
	tokenPath: input.tokenPath,
	sleep: async () => undefined,
});
const processorWriter: ProviderStreamAccumulatorWriter = {
	appendEvent: async (event, _source, declaration, modelRequestId) =>
		writer.append({
			workspaceId: input.workspaceId,
			sessionId: input.sessionId,
			sessionThreadId: input.sessionThreadId,
			bindingId: input.bindingId,
			bindingGeneration: input.bindingGeneration,
			targetPodUid: input.targetPodUid,
			writeId: `rwrite_${input.modelRequestId}_${input.modelToolCallId}`,
			event,
			...(modelRequestId === undefined ? {} : { modelRequestId }),
			...(declaration ?? {}),
		}),
	settleToolResult: writer.settleToolResult.bind(writer),
	commitInternalToolRepair: async () => {
		throw new Error("internal repair is outside this composition");
	},
};
const source = { providerId: "openai", modelId: "gpt-5" } as const;
const processor = new ProviderStreamAccumulator({
	modelRequestId: input.modelRequestId,
	requestId: `req_${input.modelRequestId}`,
	workspaceId: input.workspaceId,
	sessionId: input.sessionId,
	sessionThreadId: input.sessionThreadId,
	bindingId: input.bindingId,
	bindingGeneration: input.bindingGeneration,
	targetPodUid: input.targetPodUid,
	contextOwner: new ContextManager(input.sessionId),
	writer: processorWriter,
});
const providerEvent = {
	type: "tool-call" as const,
	id: input.modelToolCallId,
	toolName: "write_stdin",
	input: { session_id: input.taskId, chars: "" },
	inputPreview: { preview: input.taskId, truncated: false },
};
const processed = await processor.process({ ...source, event: providerEvent });
if (!processed.ok) throw new Error("Runtime rejected write_stdin");

const catalog = createToolCatalog({ family: "gpt" });
const scheduler = new ToolScheduler();
const state: RuntimeToolRegistrationState = {
	executionPolicy: { toolCatalog: catalog },
	toolScheduler: scheduler,
	toolEntries: {},
	nextToolModelOrder: 0,
};
const registration = registerRuntimeToolCall(input.modelRequestId, state, providerEvent);
if (registration.type !== "registered")
	throw new Error("Runtime catalog rejected write_stdin");
const job = scheduler.jobs().find((candidate) => candidate.id === registration.jobId);
const entry = state.toolEntries[registration.jobId];
if (job === undefined || entry === undefined)
	throw new Error("Runtime omitted the registered write_stdin job");
const gate = evaluateToolGate({ catalog, toolName: job.name, approvalMode: "full_access" });
if (gate.type !== "run") throw new Error("Runtime did not authorize write_stdin");
if (
	!processor.reservePublicToolUse(
		source,
		job.modelToolCallId,
		publicToolEventForEntry(entry),
		distinctProviderInputForEntry(entry, providerEvent.input),
		routeCapabilityForEntry(entry),
	)
)
	throw new Error("Runtime could not reserve write_stdin");
const committed = await processor.commitPublicToolUse(
	source,
	job.modelToolCallId,
	publicInputForRegisteredTool(job.input),
	gate.evaluatedPermission,
);
if (!committed.ok) throw new Error("Runtime could not commit write_stdin");

const abort = new AbortController();
const request: RuntimeToolExecutionRequest = {
	workspaceId: input.workspaceId,
	sessionId: input.sessionId,
	sessionThreadId: input.sessionThreadId,
	bindingId: input.bindingId,
	bindingGeneration: input.bindingGeneration,
	runtimeBindingToken: input.runtimeBindingToken,
	targetPodUid: input.targetPodUid,
	modelRequestId: input.modelRequestId,
	modelToolCallId: input.modelToolCallId,
	modelOrder: job.modelOrder,
	toolUseEventId: committed.toolUseEventId,
	entry,
	input: job.input,
	retainedContextEntries: [],
	abortSignal: abort.signal,
};
const runner = new RuntimePodToolRunner({
	bridgeAddress: input.address,
	webAddress: "127.0.0.1:1",
	mcpConnectorAddress: "127.0.0.1:1",
	tokenPath: input.tokenPath,
	sleep: async () => undefined,
});
const resultPromise = runner.runTool(request);
for (;;) {
	try {
		await access(input.abortPath);
		break;
	} catch {
		await Bun.sleep(10);
	}
}
abort.abort();
const result = await resultPromise;
if (result.type === "stale_custody") {
	throw new Error("background cancellation lost durable custody");
}
const settlement = await writer.settleToolResult({
	workspaceId: input.workspaceId,
	sessionId: input.sessionId,
	sessionThreadId: input.sessionThreadId,
	bindingId: input.bindingId,
	bindingGeneration: input.bindingGeneration,
	targetPodUid: input.targetPodUid,
	settlement: {
		toolUseEventId: committed.toolUseEventId,
		outcome: runtimeToolSettlement(result),
	},
});
if (!settlement.ok) throw settlement.error;
process.stdout.write(
	JSON.stringify({ toolUseEventId: committed.toolUseEventId, result, settlement: settlement.result }),
);
