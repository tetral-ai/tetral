import { readFile, writeFile } from "node:fs/promises";
import { credentials, Metadata } from "@grpc/grpc-js";
import type {
	SessionEventEnvelope,
	SessionEventWriterAppendResult,
} from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import { createToolCatalog } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
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
import { BridgeAPIContextLoader } from "../../src/bridge-client.js";
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
const contextLoader = {
	loadThreadContext:
		input.bridgeAddress === undefined
			? bridgeLoader.loadThreadContext.bind(bridgeLoader)
			: async () => ({
					contextEntries: [],
					turnFacts: { events: [], internalRepairs: [] },
					thread: {
						role: "main" as const,
						visibility: "public" as const,
						agentType: "general" as const,
						status: "idle" as const,
					},
					runtimeBindingToken: "runtime-binding-token-composition",
				}),
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
		sessionEventWriter: writerFrom(successfulEventAppend),
		runtime: {
			now: () => "2026-01-01T00:00:00.000Z",
			monotonicMs: () => 0,
			createId: (prefix) => `${prefix}_${++nextID}`,
			sleep: runtimeSleep,
		},
		llmService: { stream: () => Stream.never },
		storeOperationTimeoutMs: 100,
		runtimeModel: () => ({ providerId: "fake", modelId: "fake-chat" }),
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
	const [commitResult] = await Promise.all([committedInput, acceptedRequest]);
	if (declaration === undefined && input.bridgeAddress === undefined) {
		throw new Error(
			"resident ThreadLoop did not cross the Bridge declaration adapter",
		);
	}
	process.stdout.write(
		JSON.stringify({
			...(declaration !== undefined ? { declaration } : {}),
			acceptResult,
			commitResult,
		}),
	);
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
