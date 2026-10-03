import { readFile } from "node:fs/promises";
import { credentials, Metadata } from "@grpc/grpc-js";
import type { CallOptions, ServiceError } from "@grpc/grpc-js";
import { AgentRuntimeBridgeServiceClient } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type { WriteEventRequest, WriteEventResponse } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
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
import { bridgeDurableContextGrpcChannelOptions } from "../../src/bounds.js";
import type { RuntimeSubAgentRunHost } from "../../src/core-hosts.js";
import { RuntimePodToolRunner } from "../../src/tool-runner.js";

const inputPath = process.argv[2];
if (inputPath === undefined) {
	throw new Error("subagent production composition input is required");
}
const input = JSON.parse(await readFile(inputPath, "utf8")) as {
	readonly bridgeAddress: string;
	readonly workspaceId: string;
	readonly sessionId: string;
	readonly sessionThreadId: string;
	readonly bindingId: string;
	readonly bindingGeneration: number;
	readonly targetPodUid: string;
	readonly runtimeProcessId: string;
	readonly taskName: string;
	readonly prompt: string;
	readonly forkTurns: string;
};

const bridgeOptions = {
	address: input.bridgeAddress,
	tokenPath: "/unused/test-token",
	metadataFactory: async () => new Metadata(),
};
const declarationAttempts: {
	writeId: string;
	inputBytes: number;
	startedAtUnixMs: number;
	invokeElapsedMs?: number;
	callbackElapsedMs?: number;
	callbackCode?: number;
}[] = [];
const bridgeClient = new AgentRuntimeBridgeServiceClient(
	bridgeOptions.address,
	credentials.createInsecure(),
	bridgeDurableContextGrpcChannelOptions(),
);
const writeEvent = bridgeClient.writeEvent.bind(bridgeClient);
// Observe the real unary handle without changing its options, ACK, or retry owner.
bridgeClient.writeEvent = ((
	request: WriteEventRequest,
	metadata: Metadata,
	options: Partial<CallOptions>,
	callback: (error: ServiceError | null, response: WriteEventResponse) => void,
) => {
	if (request.toolDeclaration === undefined) return writeEvent(request, metadata, options, callback);
	const observation: (typeof declarationAttempts)[number] = {
		writeId: request.runtimeWriteId.slice(0, 128),
		inputBytes: Buffer.byteLength(request.toolDeclaration.publicExecutionInputJson),
		startedAtUnixMs: Date.now(),
	};
	declarationAttempts.push(observation);
	if (declarationAttempts.length > 3) declarationAttempts.shift();
	const started = performance.now();
	const call = writeEvent(request, metadata, options, (error, response) => {
		observation.callbackElapsedMs = performance.now() - started;
		observation.callbackCode = error?.code ?? 0;
		callback(error, response);
	});
	observation.invokeElapsedMs = performance.now() - started;
	return call;
}) as typeof bridgeClient.writeEvent;
const writer = new BridgeAPIEventWriter({ ...bridgeOptions, client: bridgeClient });
let requestEndCount = 0;
let resolveThirdRequestEnd!: () => void;
const thirdRequestEnd = new Promise<void>((resolve) => {
	resolveThirdRequestEnd = resolve;
});
let releaseThirdRequestEnd!: () => void;
const thirdRequestEndRelease = new Promise<void>((resolve) => {
	releaseThirdRequestEnd = resolve;
});
let lastFailedWriter:
	| { readonly method: "append"; readonly eventType: string; readonly errorCode: string; readonly elapsedMs: number }
	| { readonly method: "settleToolResult" | "writeRequestEnd"; readonly errorCode: string }
	| undefined;
const tracedWriter = {
	append: async (...args: Parameters<typeof writer.append>) => {
		const started = performance.now();
		const result = await writer.append(...args);
		if (!result.ok) {
			lastFailedWriter = {
				method: "append",
				eventType: args[0].event.type,
				errorCode: result.error.code,
				elapsedMs: performance.now() - started,
			};
		}
		return result;
	},
	settleToolResult: async (...args: Parameters<typeof writer.settleToolResult>) => {
		const result = await writer.settleToolResult(...args);
		if (!result.ok) {
			lastFailedWriter = { method: "settleToolResult", errorCode: result.error.code };
		}
		return result;
	},
	writeRequestEnd: async (...args: Parameters<typeof writer.writeRequestEnd>) => {
		const result = await writer.writeRequestEnd(...args);
		if (!result.ok) {
			lastFailedWriter = { method: "writeRequestEnd", errorCode: result.error.code };
		}
		if (result.ok && result.type !== "stale") {
			requestEndCount += 1;
			if (requestEndCount === 3) {
				resolveThirdRequestEnd();
				// Stop after the real third RequestEnd ACK. Returning it before interruption
				// lets Core enter FinishIdle, whose output-capture worker is outside this fixture.
				await thirdRequestEndRelease;
			}
		}
		return result;
	},
	finishIdle: writer.finishIdle.bind(writer),
	commitRuntimeTermination: writer.commitRuntimeTermination.bind(writer),
};
const childHost = {
	preloadThread: async () => ({ ok: true as const }),
} as unknown as RuntimeSubAgentRunHost;
const runner = new RuntimePodToolRunner({
	bridgeAddress: input.bridgeAddress,
	webAddress: "127.0.0.1:1",
	mcpConnectorAddress: "127.0.0.1:1",
	tokenPath: bridgeOptions.tokenPath,
	metadataFactory: bridgeOptions.metadataFactory,
	sleep: async () => undefined,
	subAgentRunHost: () => childHost,
});
const session = new ThreadRuntime({
	workspaceId: input.workspaceId,
	sessionId: input.sessionId,
	sessionThreadId: input.sessionThreadId,
	bindingId: input.bindingId,
	bindingGeneration: input.bindingGeneration,
	targetPodUid: input.targetPodUid,
	runtimeProcessId: input.runtimeProcessId,
	runtimeBindingToken: "subagent-production-binding-token",
});
let providerInvocations = 0;
const providerRequests: LLMRequest[] = [];
let nextId = 0;
let elapsedMs = 0;
const llmService = {
	stream: (request: LLMRequest) => {
		providerInvocations += 1;
		providerRequests.push(request);
		if (providerInvocations === 1) {
			return Stream.fromIterable([
				{
					type: "tool-call" as const,
					id: "call_subagent_production",
					toolName: "spawn_agent",
					input: {
						task_name: input.taskName,
						prompt: input.prompt,
						agent_type: "worker",
						fork_turns: input.forkTurns,
					},
					inputPreview: {
						preview: `{"task_name":"${input.taskName}"}`,
						truncated: false,
					},
				},
				{ type: "finish" as const, finishReason: "tool-calls" as const },
			]);
		}
		if (providerInvocations === 2) {
			return Stream.fail({
				type: "llm-service" as const,
				error: {
					type: "runtime" as const,
					code: "gateway_stream_error" as const,
					message: "Gateway stream failed after atomic child creation.",
					retryable: true,
					fatal: false,
				},
			});
		}
		return Stream.fromIterable([
			{ type: "text-start" as const, id: "subagent-complete" },
			{
				type: "text-delta" as const,
				id: "subagent-complete",
				text_delta: "child started",
			},
			{ type: "text-end" as const, id: "subagent-complete" },
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
				new QueuedContextLoader(
					[],
					[
						{
							type: "context",
							entries: [
								userMessage(
									"msg_subagent_production_request",
									0,
									"delegate the task now",
								),
							],
						},
					],
				),
				{
					writer: tracedWriter,
					llmService,
					approvalMode: "full_access",
					providerCallRuntime: {
						...DefaultProviderCallRuntimeConfig,
						systemInstructions: "Subagent production composition.",
					},
					runtimeModel: () => ({
						providerId: "anthropic",
						modelId: "claude-opus-4-8",
					}),
					runtimePolicy: () => ({
						toolCatalog: createToolCatalog({ family: "claude" }),
					}),
					runTool: runner.runTool.bind(runner),
					runtime: {
						now: () =>
							new Date(Date.parse("2026-08-20T00:00:00.000Z") + elapsedMs).toISOString(),
						monotonicMs: () => elapsedMs,
						createId: (prefix) => `${prefix}_subagent_production_${++nextId}`,
						sleep: async (durationMs, signal) => {
							if (signal.aborted) return false;
							elapsedMs += durationMs;
							return true;
						},
					},
				},
			),
		),
	),
);
await Promise.race([
	thirdRequestEnd,
	Effect.runPromise(Fiber.await(runFiber)).then((exit) => {
		const observed = {
			exitTag: exit._tag,
			...(exit._tag === "Success" ? { resultType: exit.value.type } : {}),
			...(exit._tag === "Success" && exit.value.type === "failed"
				? {
						errorCode: exit.value.error.code,
						releaseReason: exit.value.releaseSession?.reason,
					}
				: {}),
			requestEndCount,
			providerInvocations,
			lastFailedWriter,
			declarationAttempts,
		};
		throw new Error(`ThreadLoop exited before three durable RequestEnd ACKs: ${JSON.stringify(observed)}`);
	}),
]);
// Request interruption before releasing the ACK; joining first would hold both owners.
const interrupted = Effect.runPromise(Fiber.interrupt(runFiber));
releaseThirdRequestEnd();
await interrupted;

process.stdout.write(
	JSON.stringify({
		resultType: "observed",
		providerInvocations,
		providerContexts: providerRequests.map((request) => request.context),
	}),
);
