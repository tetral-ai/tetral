import { readFile, writeFile } from "node:fs/promises";
import { credentials, Metadata } from "@grpc/grpc-js";
import type {
	SessionEventEnvelope,
	SessionEventWriter,
	SessionEventWriterAppendResult,
} from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import type { LLMRequest } from "@tetral/agent-runtime-core/src/llm/llm-service.js";
import { createToolCatalog } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import { DefaultProviderCallRuntimeConfig } from "@tetral/agent-runtime-core/src/thread-loop/provider-request.js";
import { writerFrom } from "@tetral/agent-runtime-core/test/unit/thread-loop/thread-loop-test-support.js";
import { AgentRuntimePodServiceClient } from "@tetral/agent-runtime-protocol/src/gen/tetral/agent_runtime/v1/agent_runtime.js";
import type {
	CommitTaskNotificationResultRequest,
	CommitTaskNotificationResultResponse,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import {
	AgentRuntimeBridgeServiceClient,
	TaskNotificationRejectionReason,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { Stream } from "effect";
import {
	BridgeAPIContextLoader,
	BridgeAPIEventWriter,
} from "../../src/bridge-client.js";
import { buildRuntimeCoreHosts } from "../../src/core-hosts.js";
import { createRuntimeGrpcServer } from "../../src/grpc-server.js";
import type { RuntimeCleanupController } from "../../src/runtime-service.js";
import { RuntimeControlService } from "../../src/runtime-service.js";

const inputPath = process.argv[2];
if (inputPath === undefined) {
	throw new Error("task notification composition input path is required");
}
const input = JSON.parse(await readFile(inputPath, "utf8")) as {
	readonly notificationJson: string;
	readonly workspaceId: string;
	readonly sessionId: string;
	readonly sessionThreadId: string;
	readonly bindingId: string;
	readonly bindingGeneration: number;
	readonly targetPodUid: string;
	readonly runtimeInputId: string;
	readonly inputOrder: number;
	readonly commitResponse?: CommitTaskNotificationResultResponse;
	readonly bridgeAddress?: string;
	readonly readyPath?: string;
	readonly requestStartRace?: boolean;
};

const cleanupController = {
	startCleanup: async () => {
		throw new Error("unexpected cleanup command");
	},
} satisfies RuntimeCleanupController;

let declaration: CommitTaskNotificationResultRequest | undefined;
let acceptResult: unknown;
let resolveAcceptResult: (() => void) | undefined;
const acceptedRequest = new Promise<void>((resolve) => {
	resolveAcceptResult = resolve;
});
let resolveCommitResult: ((value: unknown) => void) | undefined;
const committedInput = new Promise<unknown>((resolve) => {
	resolveCommitResult = resolve;
});
let resolveLifecycleCompleted: (() => void) | undefined;
const lifecycleCompleted = new Promise<void>((resolve) => {
	resolveLifecycleCompleted = resolve;
});
const generatedBridgeClient =
	input.bridgeAddress === undefined
		? undefined
		: new AgentRuntimeBridgeServiceClient(
				input.bridgeAddress,
				credentials.createInsecure(),
			);
const bridgeLoader = new BridgeAPIContextLoader({
	address: input.bridgeAddress ?? "unused.test",
	tokenPath: "/unused/token",
	metadataFactory: async () => new Metadata(),
	client:
		generatedBridgeClient ??
		({
			loadContext: (
				_request: unknown,
				_metadata: Metadata,
				callback: (error: Error | null, value: unknown) => void,
			) => {
				callback(null, {
					contextJson: JSON.stringify({
						contextEntries: [],
						openRequestDraft: null,
						turnFacts: { events: [], internalRepairs: [] },
						thread: {
							parentThreadId: null,
							role: "main",
							visibility: "public",
							taskName: null,
							agentType: "general",
							status: "idle",
						},
					}),
					runtimeBindingToken: "runtime-binding-token-composition",
				});
				return { cancel: () => undefined };
			},
			commitTaskNotificationResult: (
				request: CommitTaskNotificationResultRequest,
				_metadata: Metadata,
				callback: (error: Error | null, value: unknown) => void,
			) => {
				declaration = request;
				callback(
					null,
					input.commitResponse ?? {
						rejected: {
							reason:
								TaskNotificationRejectionReason.TASK_NOTIFICATION_REJECTION_REASON_DURABLE_RESULT_INVALID,
						},
					},
				);
				return { cancel: () => undefined };
			},
		} as unknown as AgentRuntimeBridgeServiceClient),
});
const productionWriter =
	input.bridgeAddress !== undefined
		? new BridgeAPIEventWriter({
				address: input.bridgeAddress,
				tokenPath: "/unused/token",
				metadataFactory: async () => new Metadata(),
			})
		: undefined;
let requestEndCount = 0;
const sessionEventWriter: SessionEventWriter =
	productionWriter === undefined
		? writerFrom(successfulEventAppend)
		: {
				append: productionWriter.append.bind(productionWriter),
				settleToolResult:
					productionWriter.settleToolResult.bind(productionWriter),
			writeRequestEnd: async (envelope) => {
				const result = await productionWriter.writeRequestEnd(envelope);
				if (result.ok && result.type !== "stale") {
					requestEndCount += 1;
					const expectedEnds = input.requestStartRace === true ? 2 : 1;
					if (requestEndCount >= expectedEnds) resolveLifecycleCompleted?.();
				}
					return result;
				},
				finishIdle: productionWriter.finishIdle.bind(productionWriter),
				commitRuntimeTermination:
					productionWriter.commitRuntimeTermination.bind(productionWriter),
			};
const contextLoader = {
	loadThreadContext:
		input.bridgeAddress === undefined
			? async () => ({
					contextEntries: [],
					turnFacts: { events: [], internalRepairs: [] },
					thread: {
						role: "main" as const,
						visibility: "public" as const,
						agentType: "general" as const,
						status: "idle" as const,
					},
					runtimeBindingToken: "runtime-binding-token-composition",
				})
			: bridgeLoader.loadThreadContext.bind(bridgeLoader),
	commitAcceptedInput: async (
		...args: Parameters<typeof bridgeLoader.commitAcceptedInput>
	) => {
		const result = await bridgeLoader.commitAcceptedInput(...args);
		resolveCommitResult?.(result);
		return result;
	},
	refreshRuntimeBindingToken:
		bridgeLoader.refreshRuntimeBindingToken.bind(bridgeLoader),
};
let nextID = 0;
let providerInvocations = 0;
const providerRequests: LLMRequest[] = [];
const runtimeSleep = async (durationMs: number, signal: AbortSignal): Promise<boolean> => {
	if (signal.aborted) return false;
	return await new Promise<boolean>((resolve) => {
		const timer = setTimeout(() => {
			signal.removeEventListener("abort", abort);
			resolve(true);
		}, durationMs);
		const abort = () => {
			clearTimeout(timer);
			resolve(false);
		};
		signal.addEventListener("abort", abort, { once: true });
	});
};
const hosts = await buildRuntimeCoreHosts({
	maxLocalSessions: 2,
	now: () => "2026-01-01T00:00:00.000Z",
	contextLoader,
	threadLoop: {
		internalToolRepairStore: {} as never,
		sessionEventWriter,
		runtime: {
			now: () => "2026-01-01T00:00:00.000Z",
			monotonicMs: () => 0,
			createId: (prefix) => `${prefix}_${++nextID}`,
			sleep: runtimeSleep,
		},
		llmService: {
			stream: (request) => {
				providerInvocations += 1;
				providerRequests.push(request);
				const id = `task-notification-race-${providerInvocations}`;
				return Stream.fromIterable([
					{ type: "text-start" as const, id },
					{
						type: "text-delta" as const,
						id,
						text_delta:
							providerInvocations === 1
								? "current request completed"
								: "task notification consumed",
					},
					{ type: "text-end" as const, id },
					{ type: "finish" as const, finishReason: "stop" as const },
				]);
			},
		},
		storeOperationTimeoutMs: input.requestStartRace === true ? 5_000 : 100,
		approvalMode: "full_access",
		providerCallRuntime: {
			...DefaultProviderCallRuntimeConfig,
			systemInstructions: "task notification Request Start composition",
			timeoutMs: 5_000,
		},
		runtimeModel: () => ({ providerId: "anthropic", modelId: "claude-opus-4-8" }),
		runtimePolicy: () => ({
			toolCatalog: createToolCatalog({ family: "claude" }),
		}),
	},
});
const service = new RuntimeControlService({
	ownPod: {
		namespace: "engine",
		name: "runtime-pod-composition",
		uid: input.targetPodUid,
		ip: "127.0.0.1",
	},
	allowedBridge: { namespace: "engine", name: "bridge" },
	authenticator: {
		authenticate: async () => ({
			ok: true as const,
			serviceAccount: { namespace: "engine", name: "bridge" },
		}),
	},
	runHost: {
		...hosts.commandRunHost,
		handleTaskNotification: async (...args) => {
			const result = await hosts.commandRunHost.handleTaskNotification(...args);
			acceptResult = result;
			resolveAcceptResult?.();
			return result;
		},
	},
	cleanupController,
	logger: {
		info: () => undefined,
		warn: () => undefined,
		error: () => undefined,
	} as never,
	ready: () => true,
});
const grpcServer = createRuntimeGrpcServer(service);
const port = await grpcServer.bind("127.0.0.1:0");
if (input.readyPath !== undefined) {
	await writeFile(input.readyPath, JSON.stringify({ port }), { mode: 0o600 });
}
const client = new AgentRuntimePodServiceClient(
	`127.0.0.1:${port}`,
	credentials.createInsecure(),
);
try {
	if (input.bridgeAddress === undefined) {
		const response = await new Promise<{
			readonly accepted?: unknown;
			readonly duplicate?: unknown;
		}>((resolve, reject) => {
			client.acceptTaskNotification(
				{
					workspaceId: input.workspaceId,
					sessionId: input.sessionId,
					sessionThreadId: input.sessionThreadId,
					bindingId: input.bindingId,
					bindingGeneration: input.bindingGeneration,
					targetPodUid: input.targetPodUid,
					runtimeInputId: input.runtimeInputId,
					inputOrder: input.inputOrder,
					notificationJson: input.notificationJson,
				},
				new Metadata(),
				(error, value) => (error === null ? resolve(value) : reject(error)),
			);
		});
		if (response.accepted === undefined && response.duplicate === undefined) {
			throw new Error(
				`Runtime did not accept the task notification through generated gRPC: ${JSON.stringify(response)}`,
			);
		}
	}
	const [commitResult] = await Promise.all([
		committedInput,
		acceptedRequest,
		...(input.bridgeAddress === undefined ? [] : [lifecycleCompleted]),
	]);
	if (declaration === undefined && input.bridgeAddress === undefined) {
		throw new Error(
			"resident ThreadLoop did not cross the Bridge declaration adapter",
		);
	}
	const output = JSON.stringify({
		...(declaration !== undefined ? { declaration } : {}),
		acceptResult,
		commitResult,
		providerInvocations,
		requestEndCount,
		providerContexts: providerRequests.map((request) => request.context),
	});
	if (input.requestStartRace === true) {
		await new Promise<void>((resolve, reject) => {
			process.stdout.write(output, (error) =>
				error === null ? resolve() : reject(error),
			);
		});
		process.exit(0);
	}
	process.stdout.write(output);
} finally {
	client.close();
	generatedBridgeClient?.close();
	await hosts.shutdownActiveRuns();
	await grpcServer.shutdown();
	await hosts.close();
}

function successfulEventAppend(
	envelope: SessionEventEnvelope,
): SessionEventWriterAppendResult {
	const eventId = `evt_${envelope.writeId}`;
	return {
		ok: true,
		type: "committed",
		eventId,
	};
}
