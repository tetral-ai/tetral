import { access, readFile, rename, writeFile } from "node:fs/promises";
import type { RequestContentProcessorWriter } from "@tetral/agent-runtime-core/src/runtime/accumulator.js";
import { RequestContentProcessor } from "@tetral/agent-runtime-core/src/runtime/accumulator.js";
import {ThreadState} from "@tetral/agent-runtime-core/src/thread-loop/thread-state.js";
import {extractThreadTurnCheckpoint} from "@tetral/agent-runtime-core/src/thread-loop/turn/load.js";
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
import { BridgeAPIEventWriter, BridgeAPIContextLoader } from "../../src/bridge-client.js";
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
	readonly runtimeProcessId: string;
	readonly runtimeBindingToken: string;
	readonly modelRequestId: string;
	readonly modelToolCallId: string;
	readonly taskId: string;
	readonly mode?: "user_interrupt" | "custody_handoff" | "rejoin";
	readonly rejoinReadyPath?: string;
};

const writer = new BridgeAPIEventWriter({
	address: input.address,
	tokenPath: input.tokenPath,
	sleep: async () => undefined,
});
const contextLoader = new BridgeAPIContextLoader({address:input.address,tokenPath:input.tokenPath});
const loaded = await contextLoader.loadThreadContext(input);
const threadState = new ThreadState(input.sessionId);
const loadedCheckpoint = extractThreadTurnCheckpoint({messages:loaded.messages,facts:loaded.turnFacts});
threadState.installThreadCheckpoint(loadedCheckpoint);
threadState.contextManager.replaceMessages(loaded.messages);
threadState.installCurrentRequestMessage(loaded.currentRequestMessage ?? undefined);
threadState.markPersistentContextLoaded();
const processorWriter: RequestContentProcessorWriter = {
	appendEvent: async (event, _source, declaration, modelRequestId) =>
		writer.append({
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
		}),
	settleToolResult: writer.settleToolResult.bind(writer),
	commitInternalToolRepair: async () => {
		throw new Error("internal repair is outside this composition");
	},
};
const source = { providerId: "openai", modelId: "gpt-5" } as const;
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
	toolName: "write_stdin",
	input: { session_id: input.taskId, chars: "" },
	inputPreview: { preview: input.taskId, truncated: false },
};
if (input.mode !== "rejoin") {
	const processed = await processor.process({ ...source, event: providerEvent });
	if (!processed.ok) throw new Error("Runtime rejected write_stdin");
}

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
let toolUseEventId: string;
if (input.mode === "rejoin") {
	// Cold recovery restores the acknowledged member. It does not submit that
	// provider member again and apply its append receipt to already loaded parts.
	const owner = loadedCheckpoint?.request;
	const member = owner?.toolMembers.find(candidate => candidate.modelToolCallId === input.modelToolCallId);
	if (owner?.modelRequestId !== input.modelRequestId || member?.memberKind !== "public_tool_use" ||
		member.toolName !== entry.definition.name || member.terminalResult !== undefined) {
		throw new Error("replacement did not load the original nonterminal background owner");
	}
	toolUseEventId = member.toolUseEventId;
} else {
	if (
		!await processor.reservePublicToolUse(
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
	toolUseEventId = committed.toolUseEventId;
}

const abort = new AbortController();
threadState.registerActiveTool({toolUseEventId,modelRequestId:input.modelRequestId,modelToolCallId:input.modelToolCallId,assistantMessageSequence:threadState.currentRequestMessage()!.assistantMessageSequence,disposition:"hot_execution"});
if (input.mode !== "rejoin") {
	threadState.applyThreadTurnFact({fact:"tool_use_committed",eventId:toolUseEventId,modelRequestId:input.modelRequestId,modelToolCallId:input.modelToolCallId,toolName:entry.definition.name});
}
const registeredOwner = threadState.activeTools()[0]!;
const resolved = threadState.resolveActiveTool(toolUseEventId,registeredOwner,entry);
if(resolved === undefined) throw new Error("registered committed Tool owner was lost before execution");
const request: RuntimeToolExecutionRequest = {
	workspaceId: input.workspaceId,
	sessionId: input.sessionId,
	sessionThreadId: input.sessionThreadId,
	bindingId: input.bindingId,
	bindingGeneration: input.bindingGeneration,
	runtimeBindingToken: input.runtimeBindingToken,
	targetPodUid: input.targetPodUid,
	runtimeProcessId: input.runtimeProcessId,
	modelRequestId: input.modelRequestId,
	modelToolCallId: input.modelToolCallId,
	modelOrder: job.modelOrder,
	toolUseEventId,
	entry,
	input: resolved.input,
	retainedContextEntries: [],
	backgroundCancellationIntent: () =>
		input.mode === "user_interrupt" || input.mode === undefined
			? "user_interrupt"
			: "custody_handoff",
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
if (input.mode === "rejoin" && input.rejoinReadyPath !== undefined) {
	// The Go owner may complete the durable poll only after the replacement
	// has installed and resolved this exact nonterminal committed call.
	const pendingReadyPath = `${input.rejoinReadyPath}.pending`;
	await writeFile(pendingReadyPath, JSON.stringify({
		currentRequestMessage: threadState.currentRequestMessage(),
		reference: registeredOwner,
		message: resolved.message,
		input: resolved.input,
	}), { mode: 0o600 });
	await rename(pendingReadyPath, input.rejoinReadyPath);
}
if (input.mode !== "rejoin") {
	for (;;) {
		try {
			await access(input.abortPath);
			break;
		} catch {
			await Bun.sleep(10);
		}
	}
	abort.abort();
}
const result = await resultPromise;
if (result.type === "stale_custody") {
	process.stdout.write(
		JSON.stringify({ toolUseEventId, result }),
	);
} else {
	const settlement = await writer.settleToolResult({
		workspaceId: input.workspaceId,
		sessionId: input.sessionId,
		sessionThreadId: input.sessionThreadId,
		bindingId: input.bindingId,
		bindingGeneration: input.bindingGeneration,
		targetPodUid: input.targetPodUid,
		runtimeProcessId: input.runtimeProcessId,
		settlement: {
			toolUseEventId,
			outcome: runtimeToolSettlement(result),
		},
	});
	if (!settlement.ok) throw settlement.error;
	process.stdout.write(
		JSON.stringify({
			toolUseEventId,
			result,
			settlement: settlement.result,
		}),
	);
}

await contextLoader.close();
await writer.close();
await runner.close();
