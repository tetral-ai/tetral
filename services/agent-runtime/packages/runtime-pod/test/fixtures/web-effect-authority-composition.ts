import { readFile } from "node:fs/promises";
import type { RuntimeToolExecutionRequest } from "@tetral/agent-runtime-core/src/thread-loop/tool-execution.js";
import {
	createToolCatalog,
	lookupToolEntry,
} from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import { RuntimePodToolRunner } from "../../src/tool-runner.js";

const inputPath = process.argv[2];
if (inputPath === undefined)
	throw new Error("Web effect authority composition input path is required");

const input = JSON.parse(await readFile(inputPath, "utf8")) as {
	readonly mode: "single" | "concurrent";
	readonly bridgeAddress: string;
	readonly webAddress: string;
	readonly tokenPath: string;
	readonly workspaceId: string;
	readonly sessionId: string;
	readonly sessionThreadId: string;
	readonly bindingId: string;
	readonly bindingGeneration: number;
	readonly targetPodUid: string;
	readonly runtimeBindingToken: string;
	readonly modelRequestId: string;
	readonly modelToolCallId: string;
	readonly toolUseEventId: string;
	readonly query: string;
};

const entry = lookupToolEntry(createToolCatalog({ family: "claude" }), "web");
if (entry === undefined) throw new Error("Web Tool is missing from the catalog");

const runner = new RuntimePodToolRunner({
	bridgeAddress: input.bridgeAddress,
	webAddress: input.webAddress,
	mcpConnectorAddress: "127.0.0.1:1",
	tokenPath: input.tokenPath,
	sleep: async () => undefined,
});

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
	modelOrder: 0,
	toolUseEventId: input.toolUseEventId,
	entry,
	input: { search_query: [{ q: input.query }] },
	retainedContextEntries: [],
	currentModel: { providerId: "openai", modelId: "gpt-5.5" },
	abortSignal: new AbortController().signal,
};

const results =
	input.mode === "concurrent"
		? await Promise.all([runner.runTool(request), runner.runTool(request)])
		: [await runner.runTool(request)];

process.stdout.write(JSON.stringify({ results }));
