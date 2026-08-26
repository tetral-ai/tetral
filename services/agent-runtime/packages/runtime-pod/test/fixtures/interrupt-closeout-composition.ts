import { access, readFile, writeFile } from "node:fs/promises";
import { credentials, Metadata } from "@grpc/grpc-js";
import { normalizeSessionEventWriterError } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import { createToolCatalog } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import { DefaultProviderCallRuntimeConfig } from "@tetral/agent-runtime-core/src/thread-loop/provider-request.js";
import { Effect, Stream } from "effect";
import {
	BridgeAPIContextLoader,
	BridgeAPIControlInputCommitter,
	BridgeAPIEventWriter,
} from "../../src/bridge-client.js";
import { buildRuntimeCoreHosts } from "../../src/core-hosts.js";
import { createRuntimeGrpcServer } from "../../src/grpc-server.js";
import type { RuntimeCleanupController } from "../../src/runtime-service.js";
import { RuntimeControlService } from "../../src/runtime-service.js";

const inputPath = process.argv[2];
if (inputPath === undefined) throw new Error("interrupt closeout composition input is required");
const input = JSON.parse(await readFile(inputPath, "utf8")) as {
	readonly bridgeAddress: string;
	readonly workspaceId: string;
	readonly sessionId: string;
	readonly sessionThreadId: string;
	readonly bindingId: string;
	readonly bindingGeneration: number;
	readonly targetPodUid: string;
	readonly readyPath: string;
	readonly acceptResultPath: string;
	readonly toolStartedPath: string;
	readonly durableOperationCompletedPath: string;
	readonly closePath: string;
	readonly fastThreadText?: string;
	readonly fastAfterFirstProviderCall?: boolean;
	readonly failFirstFinishIdle?: boolean;
};

const metadataFactory = async () => new Metadata();
const bridgeLoader = new BridgeAPIContextLoader({
	address: input.bridgeAddress,
	tokenPath: "/unused/service-account-token",
	metadataFactory,
});
const writer = new BridgeAPIEventWriter({
	address: input.bridgeAddress,
	tokenPath: "/unused/service-account-token",
	metadataFactory,
});
let finishIdleInvocations = 0;
if (input.failFirstFinishIdle === true) {
	const finishIdle = writer.finishIdle.bind(writer);
	writer.finishIdle = async (envelope) => {
		finishIdleInvocations += 1;
		if (finishIdleInvocations === 1) {
			return {
				ok: false,
				error: normalizeSessionEventWriterError({
					code: "schema_mismatch",
					sessionId: envelope.sessionId,
					writeId: envelope.durableTurnId,
				}),
			};
		}
		return await finishIdle(envelope);
	};
}
const controlInputCommitter = new BridgeAPIControlInputCommitter({
	address: input.bridgeAddress,
	tokenPath: "/unused/service-account-token",
	metadataFactory,
});
let nextId = 0;
let providerInvocations = 0;
let durableOperationCompletions = 0;
let interruptResult: unknown;
const interruptResults: unknown[] = [];
let threadSnapshot: unknown;
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
	now: () => "2026-08-17T00:00:00.000Z",
	contextLoader: {
		loadThreadContext: bridgeLoader.loadThreadContext.bind(bridgeLoader),
		commitAcceptedInput: bridgeLoader.commitAcceptedInput.bind(bridgeLoader),
		refreshRuntimeBindingToken: async (identity) => identity.runtimeBindingToken,
	},
	threadLoop: {
		internalToolRepairStore: {} as never,
		sessionEventWriter: writer,
		runtime: {
			now: () => "2026-08-17T00:00:00.000Z",
			monotonicMs: () => 0,
			createId: (prefix) => `${prefix}_interrupt_composition_${++nextId}`,
			sleep: runtimeSleep,
		},
		llmService: {
			stream: (request) => {
				providerInvocations += 1;
				if (
					(input.fastAfterFirstProviderCall === true && providerInvocations > 1) ||
					(input.fastThreadText !== undefined &&
						JSON.stringify(request.context).includes(input.fastThreadText))
				) {
					return Stream.fromIterable([
						{ type: "text-start" as const, id: "thread-isolation-fast" },
						{
							type: "text-delta" as const,
							id: "thread-isolation-fast",
							text_delta: "sibling complete",
						},
						{ type: "text-end" as const, id: "thread-isolation-fast" },
						{ type: "finish" as const, finishReason: "stop" as const },
					]);
				}
				return Stream.concat(
					Stream.fromEffect(Effect.sync(() => {
						return {
							type: "tool-call" as const,
						id: "call_interrupt_composition",
						toolName: "Bash",
						input: { command: "durable-operation" },
						inputPreview: { preview: "{}", truncated: false },
						};
					})),
					Stream.never,
				);
			},
		},
		acceptSandboxExecution: async () => ({ type: "accepted" as const }),
		awaitSandboxExecution: async (request) =>
			await new Promise((resolve, reject) => {
				const complete = () => {
					durableOperationCompletions += 1;
					writeFile(input.durableOperationCompletedPath, "completed", { mode: 0o600 }).then(
						() => resolve({ type: "cancelled" as const }),
						reject,
					);
				};
				void writeFile(input.toolStartedPath, "started", { mode: 0o600 }).then(() => {
					if (request.abortSignal.aborted) complete();
					else request.abortSignal.addEventListener("abort", complete, { once: true });
				}, reject);
			}),
		storeOperationTimeoutMs: 5_000,
		approvalMode: "full_access",
		providerCallRuntime: {
			...DefaultProviderCallRuntimeConfig,
			systemInstructions: "Interrupt production closeout composition.",
			timeoutMs: 5_000,
		},
		runtimeModel: () => ({ providerId: "fake", modelId: "fake-chat" }),
		runtimePolicy: () => ({ toolCatalog: createToolCatalog({ family: "claude" }) }),
	},
});

const cleanupController = {
	startCleanup: hosts.cleanupRunHost.handleCleanupSession,
} satisfies RuntimeCleanupController;
const service = new RuntimeControlService({
	ownPod: {
		namespace: "tetral-agent-runtime",
		name: "runtime-pod-interrupt-composition",
		uid: input.targetPodUid,
		ip: "127.0.0.1",
	},
	allowedBridge: { namespace: "tetral-system", name: "bridge" },
	authenticator: {
		authenticate: async () => ({
			ok: true as const,
			serviceAccount: { namespace: "tetral-system", name: "bridge" },
		}),
	},
	runHost: {
		...hosts.commandRunHost,
		handleAcceptInput: async (...args) => {
			const result = await hosts.commandRunHost.handleAcceptInput(...args);
			await writeFile(input.acceptResultPath, JSON.stringify(result), { mode: 0o600 });
			return result;
		},
		handleInterruptControl: async (...args) => {
			const result = await hosts.commandRunHost.handleInterruptControl(...args);
			interruptResult = result;
			interruptResults.push(result);
			threadSnapshot = await hosts.subAgentRunHost.inspectThread(args[1]);
			return result;
		},
	},
	controlInputCommitter,
	cleanupController,
	logger: { info: () => undefined, warn: () => undefined, error: () => undefined } as never,
	ready: () => true,
});
const server = createRuntimeGrpcServer(service);
const port = await server.bind("127.0.0.1:0");
await writeFile(input.readyPath, JSON.stringify({ port }), { mode: 0o600 });

try {
	for (;;) {
		try {
			await access(input.closePath);
			break;
		} catch {
			await new Promise((resolve) => setTimeout(resolve, 10));
		}
	}
	process.stdout.write(JSON.stringify({
		interruptResult,
		interruptResults,
		threadSnapshot,
		finishIdleInvocations,
		providerInvocations,
		durableOperationCompletions,
	}));
} finally {
	await hosts.shutdownActiveRuns();
	await server.shutdown();
	await hosts.close();
}
