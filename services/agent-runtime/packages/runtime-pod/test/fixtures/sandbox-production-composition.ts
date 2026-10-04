import { readFile } from "node:fs/promises";
import type { RuntimeJsonValue } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import { SessionEventWriterRetryPolicy } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import type { RequestContentProcessorWriter } from "@tetral/agent-runtime-core/src/runtime/accumulator.js";
import { RequestContentProcessor } from "@tetral/agent-runtime-core/src/runtime/accumulator.js";
import {ThreadState} from "@tetral/agent-runtime-core/src/thread-loop/thread-state.js";
import {extractThreadTurnCheckpoint} from "@tetral/agent-runtime-core/src/thread-loop/turn/load.js";
import type { RuntimeToolExecutionRequest } from "@tetral/agent-runtime-core/src/thread-loop/tool-execution.js";
import type { RuntimeToolRegistrationState } from "@tetral/agent-runtime-core/src/thread-loop/tool-execution.js";
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
import { BridgeAPIEventWriter, BridgeAPIContextLoader } from "../../src/bridge-client.js";
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
	readonly runtimeProcessId: string;
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
const contextLoader = new BridgeAPIContextLoader({address:input.address,tokenPath:input.tokenPath});
const loaded = await contextLoader.loadThreadContext(input);
const threadState = new ThreadState(input.sessionId);
threadState.installThreadCheckpoint(extractThreadTurnCheckpoint({messages:loaded.messages,facts:loaded.turnFacts}));
threadState.contextManager.replaceMessages(loaded.messages);
threadState.installCurrentRequestMessage(loaded.currentRequestMessage ?? undefined);
threadState.markPersistentContextLoaded();
const processorWriter: RequestContentProcessorWriter = {
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
			runtimeProcessId: input.runtimeProcessId,
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
const processor = new RequestContentProcessor({
	modelRequestId: input.modelRequestId,
	requestId: `req_${input.modelRequestId}`,
	workspaceId: input.workspaceId,
	sessionId: input.sessionId,
	sessionThreadId: input.sessionThreadId,
	bindingId: input.bindingId,
	bindingGeneration: input.bindingGeneration,
	targetPodUid: input.targetPodUid,
	runtimeProcessId: input.runtimeProcessId,
	contextOwner: threadState.contextManager,
  activeToolReferences:()=>threadState.activeTools(),
  onAssistantMessageCommitted:reference=>threadState.associateCurrentRequestMessage(reference),
  onCommittedApplicationFailure:()=>threadState.invalidateResidentState(),
	writer: processorWriter,
});
const providerEvent = {
	type: "tool-call-complete" as const,
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
const toolCatalog = createToolCatalog({ family: "gpt" });
const registrationState: RuntimeToolRegistrationState = {
	executionPolicy: { toolCatalog },
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
const gateDecision = evaluateToolGate({
	catalog: toolCatalog,
	toolName: job.name,
	approvalMode: "full_access",
});
if (gateDecision.type !== "run")
	throw new Error("Runtime gate did not authorize the production Tool job");
if (
	!await processor.reservePublicToolUse(
		source,
		job.modelToolCallId,
		publicToolEventForEntry(entry),
		distinctProviderInputForEntry(entry, input.providerInput),
		routeCapabilityForEntry(entry),
	)
)
	throw new Error("Runtime could not reserve the production Tool declaration");
const committed = await processor.commitPublicToolUse(
	source,
	job.modelToolCallId,
	publicInputForRegisteredTool(job.input),
	gateDecision.evaluatedPermission,
);
if (!committed.ok)
	throw new Error("Runtime could not durably commit the production Tool declaration");

threadState.registerActiveTool({toolUseEventId:committed.toolUseEventId,modelRequestId:input.modelRequestId,modelToolCallId:input.modelToolCallId,assistantMessageSequence:threadState.currentRequestMessage()!.assistantMessageSequence,disposition:"hot_execution"});
threadState.applyThreadTurnFact({fact:"tool_use_committed",eventId:committed.toolUseEventId,modelRequestId:input.modelRequestId,modelToolCallId:input.modelToolCallId,toolName:entry.definition.name});
const registeredOwner = threadState.activeTools()[0]!;
const resolved = threadState.resolveActiveTool(committed.toolUseEventId,registeredOwner,entry);
if(resolved === undefined) throw new Error("registered committed Tool owner was lost before execution");
const request: RuntimeToolExecutionRequest = {
	workspaceId: input.workspaceId,
	sessionId: input.sessionId,
	sessionThreadId: input.sessionThreadId,
	bindingId: input.bindingId,
	bindingGeneration: input.bindingGeneration,
	runtimeBindingToken: "composition-binding-token",
	targetPodUid: input.targetPodUid,
	runtimeProcessId: input.runtimeProcessId,
	modelRequestId: input.modelRequestId,
	modelToolCallId: input.modelToolCallId,
	modelOrder: job.modelOrder,
	toolUseEventId: committed.toolUseEventId,
	entry,
	input: resolved.input,
	retainedContextEntries: [],
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
	runtimeProcessId: input.runtimeProcessId,
	settlement: {
		toolUseEventId: committed.toolUseEventId,
		outcome: runtimeToolSettlement(result),
	},
});
if (!settlement.ok) throw settlement.error;
process.stdout.write(
	JSON.stringify({
		toolUseEventId: committed.toolUseEventId,
		canonicalExecutionInput: resolved.input,
		result,
		settlement: settlement.result,
	}),
);

await contextLoader.close();
await writer.close();
await runner.close();
