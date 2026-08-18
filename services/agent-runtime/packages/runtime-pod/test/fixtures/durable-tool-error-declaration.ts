import { readFile } from "node:fs/promises";
import { Metadata } from "@grpc/grpc-js";
import type { CallOptions } from "@grpc/grpc-js";
import type {
	AcceptSandboxExecutionRequest,
	AgentRuntimeBridgeServiceClient,
	AwaitSandboxExecutionRequest,
	SettleToolResultRequest,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { runtimeToolErrorFromFailure } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import type { RuntimeToolExecutionRequest } from "@tetral/agent-runtime-core/src/thread-loop/tool-execution.js";
import { runtimeToolSettlement } from "@tetral/agent-runtime-core/src/thread-loop/tool-execution.js";
import { createToolCatalog, lookupToolEntry } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import { BridgeAPIEventWriter } from "../../src/bridge-client.js";
import { RuntimePodToolRunner } from "../../src/tool-runner.js";

const inputPath = process.argv[2];
if (inputPath === undefined) throw new Error("durable Tool error declaration input path is required");
const input = JSON.parse(await readFile(inputPath, "utf8")) as {
	readonly workspaceId: string;
	readonly sessionId: string;
	readonly sessionThreadId: string;
	readonly bindingId: string;
	readonly bindingGeneration: number;
	readonly targetPodUid: string;
	readonly modelRequestId: string;
	readonly modelToolCallId: string;
	readonly toolUseEventId: string;
};

let captured: SettleToolResultRequest | undefined;
const client = {
	acceptSandboxExecution: (
		_request: AcceptSandboxExecutionRequest,
		_metadata: Metadata,
		_options: CallOptions,
		callback: (error: Error | null, response: unknown) => void,
	) => {
		callback(null, { committed: {} });
		return { cancel() {} };
	},
	awaitSandboxExecution: (
		_request: AwaitSandboxExecutionRequest,
		_metadata: Metadata,
		callback: (error: Error | null, response: unknown) => void,
	) => {
		callback(null, {
			completed: {
				resultJson: JSON.stringify({
					status: "tool_error",
					error: { kind: "not_found", message: "path does not exist" },
					result: {},
				}),
				taskId: "",
			},
		});
		return { cancel() {} };
	},
	settleToolResult: (
		request: SettleToolResultRequest,
		_metadata: Metadata,
		_options: CallOptions,
		callback: (error: Error | null, response: unknown) => void,
	) => {
		captured = request;
		callback(null, { committed: {} });
		return { cancel() {} };
	},
} as unknown as AgentRuntimeBridgeServiceClient;
const entry = lookupToolEntry(createToolCatalog({ family: "claude" }), "Read");
if (entry === undefined) throw new Error("Read is unavailable in the production catalog");
const request: RuntimeToolExecutionRequest = {
	workspaceId: input.workspaceId,
	sessionId: input.sessionId,
	sessionThreadId: input.sessionThreadId,
	bindingId: input.bindingId,
	bindingGeneration: input.bindingGeneration,
	runtimeBindingToken: "fixture-binding-token",
	targetPodUid: input.targetPodUid,
	modelRequestId: input.modelRequestId,
	modelToolCallId: input.modelToolCallId,
	modelOrder: 0,
	toolUseEventId: input.toolUseEventId,
	entry,
	input: { file_path: "/missing.txt" },
	abortSignal: new AbortController().signal,
};
const runner = new RuntimePodToolRunner({
	bridgeAddress: "bridge.test:9090",
	webAddress: "web.test:9090",
	mcpConnectorAddress: "mcp.test:9090",
	tokenPath: "/var/run/token",
	bridgeClient: client,
	webClient: {} as never,
	mcpConnectorClient: {} as never,
	metadataFactory: async () => new Metadata(),
});
const result = await runner.runTool(request);
if (result.type !== "error") {
	throw new Error(`ordinary missing-file Read returned ${result.type}`);
}
const outcome = runtimeToolSettlement(result);
const writer = new BridgeAPIEventWriter({
	address: "bridge.test:9090",
	tokenPath: "/var/run/token",
	client,
	metadataFactory: async () => new Metadata(),
});
const attempt = await writer.settleToolResult({
	workspaceId: input.workspaceId,
	sessionId: input.sessionId,
	sessionThreadId: input.sessionThreadId,
	bindingId: input.bindingId,
	bindingGeneration: input.bindingGeneration,
	targetPodUid: input.targetPodUid,
	settlement: { toolUseEventId: input.toolUseEventId, outcome },
});
if (!attempt.ok || attempt.result.type !== "committed" || captured?.settlement?.error === undefined) {
	throw new Error("Runtime Bridge adapter did not declare a Tool error settlement");
}

process.stdout.write(JSON.stringify({
	toolUseEventId: captured.settlement.toolUseEventId,
	errorJson: captured.settlement.error.errorJson,
	adapterResult: result,
	runtimeSettlement: outcome,
	expectedDurableError: runtimeToolErrorFromFailure(result.error),
}));
