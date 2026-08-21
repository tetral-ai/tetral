import { readFile } from "node:fs/promises";
import { Metadata } from "@grpc/grpc-js";
import type { RuntimeToolExecutionRequest } from "@tetral/agent-runtime-core/src/thread-loop/tool-execution.js";
import {
	createToolCatalog,
	lookupToolEntry,
} from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import { Stream } from "effect";
import { BridgeAPIContextLoader } from "../../src/bridge-client.js";
import { buildRuntimeCoreHosts } from "../../src/core-hosts.js";
import { RuntimePodToolRunner } from "../../src/tool-runner.js";

const inputPath = process.argv[2];
if (inputPath === undefined) {
	throw new Error("closed Thread resume composition input path is required");
}
const input = JSON.parse(await readFile(inputPath, "utf8")) as {
	readonly address: string;
	readonly workspaceId: string;
	readonly sessionId: string;
	readonly parentThreadId: string;
	readonly childThreadId: string;
	readonly childTaskName: string;
	readonly bindingId: string;
	readonly bindingGeneration: number;
	readonly targetPodUid: string;
	readonly sourceToolUseEventId: string;
};

const metadataFactory = async () => new Metadata();
const loader = new BridgeAPIContextLoader({
	address: input.address,
	tokenPath: "/unused/service-account-token",
	metadataFactory,
});
let providerRequests = 0;
let runtimeEvents = 0;
const unexpectedRuntimeWrite = async (): Promise<never> => {
	runtimeEvents += 1;
	throw new Error("closed Thread resume unexpectedly started Runtime work");
};
const hosts = await buildRuntimeCoreHosts({
	maxLocalSessions: 2,
	now: () => "2026-08-16T00:00:00.000Z",
	contextLoader: {
		loadThreadContext: loader.loadThreadContext.bind(loader),
		commitAcceptedInput: async () => {
			throw new Error("closed Thread resume unexpectedly committed input");
		},
		refreshRuntimeBindingToken:
			loader.refreshRuntimeBindingToken.bind(loader),
	},
	threadLoop: {
		internalToolRepairStore: {} as never,
		sessionEventWriter: {
			append: unexpectedRuntimeWrite,
			writeRequestEnd: unexpectedRuntimeWrite,
			finishIdle: unexpectedRuntimeWrite,
			settleToolResult: unexpectedRuntimeWrite,
		},
		runtime: {
			now: () => "2026-08-16T00:00:00.000Z",
			monotonicMs: () => 0,
			createId: (prefix) => `${prefix}_closed_resume`,
			sleep: async () => true,
		},
		llmService: {
			stream: () => {
				providerRequests += 1;
				return Stream.empty;
			},
		},
		storeOperationTimeoutMs: 100,
		runtimeModel: () => ({ providerId: "fake", modelId: "fake-chat" }),
		runtimePolicy: () => ({
			toolCatalog: createToolCatalog({ family: "claude" }),
		}),
	},
});

try {
	const entry = lookupToolEntry(
		createToolCatalog({ family: "claude" }),
		"resume_agent",
	);
	if (entry === undefined) throw new Error("resume_agent is unavailable");
	const request: RuntimeToolExecutionRequest = {
		workspaceId: input.workspaceId,
		sessionId: input.sessionId,
		sessionThreadId: input.parentThreadId,
		bindingId: input.bindingId,
		bindingGeneration: input.bindingGeneration,
		runtimeBindingToken: "closed-resume-composition-token",
		targetPodUid: input.targetPodUid,
		modelRequestId: "mreq_closed_resume_composition",
		modelToolCallId: "call_closed_resume_composition",
		modelOrder: 0,
		toolUseEventId: input.sourceToolUseEventId,
		entry,
		input: { task_name: input.childTaskName },
		retainedContextEntries: [],
		currentModel: { providerId: "anthropic", modelId: "claude-sonnet-4-6" },
		abortSignal: new AbortController().signal,
	};
	const runner = new RuntimePodToolRunner({
		bridgeAddress: input.address,
		webAddress: "127.0.0.1:1",
		mcpConnectorAddress: "127.0.0.1:1",
		tokenPath: "/unused/service-account-token",
		metadataFactory,
		subAgentRunHost: () => hosts.subAgentRunHost,
	});
	const result = await runner.runTool(request);
	const inspected = await hosts.subAgentRunHost.inspectThread({
		workspaceId: input.workspaceId,
		sessionId: input.sessionId,
		sessionThreadId: input.childThreadId,
		bindingId: input.bindingId,
		bindingGeneration: input.bindingGeneration,
		targetPodUid: input.targetPodUid,
	});
	process.stdout.write(
		JSON.stringify({
			result,
			inspected,
			providerRequests,
			runtimeEvents,
		}),
	);
} finally {
	await hosts.shutdownActiveRuns();
	await hosts.close();
}
