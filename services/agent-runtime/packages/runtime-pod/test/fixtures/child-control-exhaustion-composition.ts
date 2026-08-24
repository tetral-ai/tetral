import { readFile } from "node:fs/promises";
import { Metadata } from "@grpc/grpc-js";
import type { LLMRequest } from "@tetral/agent-runtime-core/src/llm/llm-service.js";
import { DefaultProviderCallRuntimeConfig } from "@tetral/agent-runtime-core/src/thread-loop/provider-request.js";
import * as ThreadLoop from "@tetral/agent-runtime-core/src/thread-loop/thread-loop.js";
import { ThreadRuntime } from "@tetral/agent-runtime-core/src/thread-loop/thread-runtime.js";
import { createToolCatalog } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import { Effect, Fiber, Stream } from "effect";
import {
	QueuedContextLoader,
	runtimeThreadLoopLayer,
	testRunCustody,
	userMessage,
} from "../../../core/test/unit/thread-loop/thread-loop-test-support.js";
import { BridgeAPIEventWriter } from "../../src/bridge-client.js";
import type { RuntimeSubAgentRunHost } from "../../src/core-hosts.js";
import { RuntimePodToolRunner } from "../../src/tool-runner.js";

const inputPath = process.argv[2];
if (inputPath === undefined) throw new Error("child control input is required");
const input = JSON.parse(await readFile(inputPath, "utf8")) as {
	readonly bridgeAddress: string;
	readonly workspaceId: string;
	readonly sessionId: string;
	readonly sessionThreadId: string;
	readonly bindingId: string;
	readonly bindingGeneration: number;
	readonly targetPodUid: string;
	readonly taskName: string;
};

const bridgeOptions = {
	address: input.bridgeAddress,
	tokenPath: "/unused/test-token",
	metadataFactory: async () => new Metadata(),
};
const writer = new BridgeAPIEventWriter(bridgeOptions);
let requestEnds = 0;
let resolveCompleted!: () => void;
const completed = new Promise<void>((resolve) => {
	resolveCompleted = resolve;
});
const tracedWriter = {
	append: writer.append.bind(writer),
	settleToolResult: writer.settleToolResult.bind(writer),
	writeRequestEnd: async (...args: Parameters<typeof writer.writeRequestEnd>) => {
		const result = await writer.writeRequestEnd(...args);
		if (result.ok && result.type !== "stale") {
			requestEnds += 1;
			if (requestEnds === 2) resolveCompleted();
		}
		return result;
	},
	finishIdle: writer.finishIdle.bind(writer),
	commitRuntimeTermination: writer.commitRuntimeTermination.bind(writer),
};
const runner = new RuntimePodToolRunner({
	bridgeAddress: input.bridgeAddress,
	webAddress: "127.0.0.1:1",
	mcpConnectorAddress: "127.0.0.1:1",
	tokenPath: bridgeOptions.tokenPath,
	metadataFactory: bridgeOptions.metadataFactory,
	sleep: async (durationMs) => {
		await new Promise((resolve) => setTimeout(resolve, Math.min(durationMs, 5)));
	},
	subAgentRunHost: () =>
		({ preloadThread: async () => ({ ok: true as const }) }) as unknown as RuntimeSubAgentRunHost,
});
const session = new ThreadRuntime({
	workspaceId: input.workspaceId,
	sessionId: input.sessionId,
	sessionThreadId: input.sessionThreadId,
	bindingId: input.bindingId,
	bindingGeneration: input.bindingGeneration,
	targetPodUid: input.targetPodUid,
	runtimeBindingToken: "child-control-exhaustion-token",
});
let providerInvocations = 0;
const providerRequests: LLMRequest[] = [];
let nextId = 0;
const llmService = {
	stream: (request: LLMRequest) => {
		providerInvocations += 1;
		providerRequests.push(request);
		if (providerInvocations === 1) {
			return Stream.fromIterable([
				{
					type: "tool-call" as const,
					id: "call_child_control_exhaustion",
					toolName: "interrupt_agent",
					input: { task_name: input.taskName },
					inputPreview: { preview: input.taskName, truncated: false },
				},
				{ type: "finish" as const, finishReason: "tool-calls" as const },
			]);
		}
		return Stream.fromIterable([
			{ type: "text-start" as const, id: "parent-continues" },
			{ type: "text-delta" as const, id: "parent-continues", text_delta: "parent continued" },
			{ type: "text-end" as const, id: "parent-continues" },
			{ type: "finish" as const, finishReason: "stop" as const },
		]);
	},
};
const runFiber = Effect.runFork(
	Effect.gen(function* () {
		return yield* (yield* ThreadLoop.Service).run(session, testRunCustody());
	}).pipe(
		Effect.provide(
			runtimeThreadLoopLayer(
				new QueuedContextLoader([], [{ type: "context", entries: [userMessage("msg_child_control", 0, "interrupt the child")] }]),
				{
					writer: tracedWriter,
					llmService,
					approvalMode: "full_access",
					providerCallRuntime: { ...DefaultProviderCallRuntimeConfig, systemInstructions: "Child control exhaustion composition." },
					runtimeModel: () => ({ providerId: "anthropic", modelId: "claude-opus-4-8" }),
					runtimePolicy: () => ({ toolCatalog: createToolCatalog({ family: "claude" }) }),
					runTool: runner.runTool.bind(runner),
					runtime: {
						now: () => "2026-08-24T00:00:00.000Z",
						monotonicMs: () => Date.now(),
						createId: (prefix) => `${prefix}_child_control_${++nextId}`,
						sleep: async (durationMs, signal) => {
							if (signal.aborted) return false;
							await new Promise((resolve) => setTimeout(resolve, Math.min(durationMs, 5)));
							return !signal.aborted;
						},
					},
				},
			),
		),
	),
);
await completed;
await Effect.runPromise(Fiber.interrupt(runFiber));
process.stdout.write(JSON.stringify({ providerInvocations, providerContexts: providerRequests.map((request) => request.context) }));
