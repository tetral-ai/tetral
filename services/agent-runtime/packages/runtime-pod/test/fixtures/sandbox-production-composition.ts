import { readFile } from "node:fs/promises";
import type { RuntimeJsonValue } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import { SessionEventWriterRetryPolicy } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import type { ProviderStreamAccumulatorWriter } from "@tetral/agent-runtime-core/src/runtime/accumulator.js";
import { ProviderStreamAccumulator } from "@tetral/agent-runtime-core/src/runtime/accumulator.js";
import { ContextManager } from "@tetral/agent-runtime-core/src/session/context-manager.js";
import type { RuntimeToolExecutionRequest } from "@tetral/agent-runtime-core/src/thread-loop/tool-execution.js";
import type { RuntimeToolRegistrationState } from "@tetral/agent-runtime-core/src/thread-loop/tool-execution.js";
import {
	registerRuntimeToolCall,
	runtimeToolSettlement,
} from "@tetral/agent-runtime-core/src/thread-loop/tool-execution.js";
import { createToolCatalog } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import { ToolScheduler } from "@tetral/agent-runtime-core/src/tools/tool-scheduler.js";
import { BridgeAPIEventWriter } from "../../src/bridge-client.js";
import { RuntimePodToolRunner } from "../../src/tool-runner.js";

const inputPath = process.argv[2];
if (inputPath === undefined)
	throw new Error("Sandbox production composition input path is required");
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
	readonly toolName: string;
	readonly providerInput: RuntimeJsonValue;
};

const source = { providerId: "openai", modelId: "gpt-5" } as const;
const writer = new BridgeAPIEventWriter({
	address: input.address,
	tokenPath: input.tokenPath,
	sleep: async () => {},
});
const processorWriter: ProviderStreamAccumulatorWriter = {
	appendEvent: async (
		event,
		_source,
		declaration,
		modelRequestId,
	) => {
		const envelope = {
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
		};
		let result = await writer.append(envelope);
		for (
			let attempt = 1;
			!result.ok && attempt < SessionEventWriterRetryPolicy.attempts;
			attempt += 1
		) {
			result = await writer.append(envelope);
		}
		return result;
	},
	settleToolResult: writer.settleToolResult.bind(writer),
	commitInternalToolRepair: async () => {
		throw new Error("internal repair is outside the Sandbox composition");
	},
};
const contextManager = new ContextManager(input.sessionId);
const processor = new ProviderStreamAccumulator({
	modelRequestId: input.modelRequestId,
	requestId: `req_${input.modelRequestId}`,
	workspaceId: input.workspaceId,
	sessionId: input.sessionId,
	sessionThreadId: input.sessionThreadId,
	bindingId: input.bindingId,
	bindingGeneration: input.bindingGeneration,
	targetPodUid: input.targetPodUid,
	contextOwner: contextManager,
	writer: processorWriter,
});
const providerEvent = {
	type: "tool-call" as const,
	id: input.modelToolCallId,
	toolName: input.toolName,
	input: input.providerInput,
	inputPreview: {
		preview: JSON.stringify(input.providerInput),
		truncated: false,
	},
};
const processed = await processor.process({ ...source, event: providerEvent });
if (!processed.ok)
	throw new Error("Runtime rejected the production provider Tool event");

const toolScheduler = new ToolScheduler();
const registrationState: RuntimeToolRegistrationState = {
	executionPolicy: { toolCatalog: createToolCatalog({ family: "gpt" }) },
	toolScheduler,
	toolEntries: {},
	nextToolModelOrder: 0,
};
const registered = registerRuntimeToolCall(
	input.modelRequestId,
	registrationState,
	providerEvent,
);
if (registered.type !== "registered")
	throw new Error("Runtime catalog rejected the production provider Tool event");
const job = toolScheduler
	.jobs()
	.find((candidate) => candidate.id === registered.jobId);
const entry = registrationState.toolEntries[registered.jobId];
if (job === undefined || entry === undefined)
	throw new Error("Runtime catalog omitted the registered production Tool job");
if (
	!processor.reservePublicToolUse(
		source,
		job.modelToolCallId,
		{ kind: "tool" },
		job.input,
	)
)
	throw new Error("Runtime could not reserve the production Tool declaration");
const committed = await processor.commitPublicToolUse(
	source,
	job.modelToolCallId,
	input.providerInput,
	"allow",
);
if (!committed.ok)
	throw new Error("Runtime could not durably commit the production Tool declaration");

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
	modelOrder: job.modelOrder,
	toolUseEventId: committed.toolUseEventId,
	entry,
	input: job.input,
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
if (result.type !== "completed")
	throw new Error(`Sandbox production result was ${result.type}`);

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
	JSON.stringify({
		toolUseEventId: committed.toolUseEventId,
		canonicalExecutionInput: job.input,
		result,
		settlement: settlement.result,
	}),
);
