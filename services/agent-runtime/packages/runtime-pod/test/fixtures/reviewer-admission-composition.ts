import { readFile } from "node:fs/promises";
import { Metadata } from "@grpc/grpc-js";
import { AutoApprovalReviewerManager } from "@tetral/agent-runtime-core/src/session/approval-reviewer-manager.js";
import { DefaultProviderCallRuntimeConfig } from "@tetral/agent-runtime-core/src/thread-loop/provider-request.js";
import type { RuntimeApprovalReviewRequest } from "@tetral/agent-runtime-core/src/thread-loop/tool-execution.js";
import { createToolCatalog } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import { Effect, Fiber, Scope, Stream } from "effect";
import { createRuntimeApprovalReviewer } from "../../src/approval-reviewer.js";
import {
	BridgeAPIApprovalReviewerThreadCreator,
	BridgeAPIContextLoader,
	BridgeAPIEventWriter,
} from "../../src/bridge-client.js";
import { buildRuntimeCoreHosts } from "../../src/core-hosts.js";

const inputPath = process.argv[2];
if (inputPath === undefined) {
	throw new Error("reviewer production composition input is required");
}
const input = JSON.parse(await readFile(inputPath, "utf8")) as {
	readonly bridgeAddress: string;
	readonly workspaceId: string;
	readonly sessionId: string;
	readonly sessionThreadId: string;
	readonly bindingId: string;
	readonly bindingGeneration: number;
	readonly targetPodUid: string;
};
let phase = "initialize";
const watchdog = setTimeout(() => {
	process.stderr.write(
		`reviewer production composition timed out during ${phase}; providers=${providerRequests}; creations=${creations.length}\n`,
	);
	process.exit(2);
}, 30_000);

const metadataFactory = async () => new Metadata();
const threadCreator = new BridgeAPIApprovalReviewerThreadCreator({
	address: input.bridgeAddress,
	tokenPath: "/unused/service-account-token",
	metadataFactory,
});
const contextLoader = new BridgeAPIContextLoader({
	address: input.bridgeAddress,
	tokenPath: "/unused/service-account-token",
	metadataFactory,
});
const eventWriter = new BridgeAPIEventWriter({
	address: input.bridgeAddress,
	tokenPath: "/unused/service-account-token",
	metadataFactory,
});
const creations: Array<{ readonly reviewId: string; readonly isTrunk: boolean }> = [];
const recordingThreadCreator = {
	createApprovalReviewerThread: async (
		creation: Parameters<
			typeof threadCreator.createApprovalReviewerThread
		>[0],
	) => {
		creations.push({ reviewId: creation.reviewId, isTrunk: creation.isTrunk });
		return await threadCreator.createApprovalReviewerThread(creation);
	},
	closeApprovalReviewerThread:
		threadCreator.closeApprovalReviewerThread.bind(threadCreator),
};
const firstProviderRelease = deferred<void>();
let nextId = 0;
let providerRequests = 0;
const runtimeSleep = async (
	durationMs: number,
	signal: AbortSignal,
): Promise<boolean> => {
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
const decisionStream = (id: string) =>
	Stream.fromIterable([
		{ type: "text-start" as const, id },
		{
			type: "text-delta" as const,
			id,
			text_delta: JSON.stringify({
				outcome: "allow",
				risk_level: "low",
				user_authorization: "high",
				rationale: "composed allow",
			}),
		},
		{ type: "text-end" as const, id },
		{ type: "finish" as const, finishReason: "stop" as const },
	]);
const hosts = await buildRuntimeCoreHosts({
	maxLocalSessions: 4,
	now: () => "2026-08-21T00:00:00.000Z",
	contextLoader: {
		loadThreadContext: contextLoader.loadThreadContext.bind(contextLoader),
		commitAcceptedInput: contextLoader.commitAcceptedInput.bind(contextLoader),
		refreshRuntimeBindingToken: async (identity) => identity.runtimeBindingToken,
	},
	threadLoop: {
		internalToolRepairStore: {} as never,
		sessionEventWriter: eventWriter,
		runtime: {
			now: () => "2026-08-21T00:00:00.000Z",
			monotonicMs: () => Date.now(),
			createId: (prefix) => `${prefix}_reviewer_composition_${++nextId}`,
			sleep: runtimeSleep,
		},
		llmService: {
			stream: () => {
				providerRequests += 1;
				switch (providerRequests) {
					case 1:
						return Stream.unwrap(
							Effect.promise(async () => {
								await firstProviderRelease.promise;
								return decisionStream("review-trunk");
							}),
						);
					case 2:
						return decisionStream("review-decision");
					case 3:
						return Stream.fromIterable([
							{ type: "finish" as const, finishReason: "stop" as const },
						]);
					case 4:
						return Stream.never;
					default:
						throw new Error("unexpected reviewer provider request");
				}
			},
		},
		acceptSandboxExecution: async () => ({ type: "accepted" as const }),
		awaitSandboxExecution: async () => ({ type: "cancelled" as const }),
		storeOperationTimeoutMs: 5_000,
		approvalMode: "full_access",
		providerCallRuntime: {
			...DefaultProviderCallRuntimeConfig,
			approvalReviewerPolicy: "Return the required approval decision JSON.",
			timeoutMs: 60_000,
		},
		runtimeModel: () => ({ providerId: "fake", modelId: "fake-chat" }),
		runtimePolicy: () => ({
			toolCatalog: createToolCatalog({ family: "claude" }),
		}),
	},
});

const manager = new AutoApprovalReviewerManager();
const reviewer = createRuntimeApprovalReviewer(() => hosts.subAgentRunHost, {
	model: { providerId: "anthropic", modelId: "claude-opus-4-8" },
	threadCreator: recordingThreadCreator,
	waitTimeoutMs: 60_000,
});
let earlyTrunkResult: unknown;
phase = "start trunk";
const trunk = Effect.runPromise(
	reviewer(reviewRequest(manager, "tool_call_reviewer_trunk")),
).then((result) => {
	earlyTrunkResult = result;
	return result;
});
await waitUntil(() => providerRequests === 1, () => earlyTrunkResult);
phase = "settle decision sidecar";
const decision = await Effect.runPromise(
	reviewer(reviewRequest(manager, "tool_call_reviewer_decision")),
);
phase = "settle failure sidecar";
const failure = await Effect.runPromise(
	reviewer(reviewRequest(manager, "tool_call_reviewer_failure")),
);

let cancellationSettled = false;
phase = "settle interrupted sidecar";
await Effect.runPromise(
	Effect.scoped(
		Effect.gen(function* () {
			const scope = yield* Scope.Scope;
			const fiber = yield* Effect.forkIn(
				reviewer(reviewRequest(manager, "tool_call_reviewer_interrupt")),
				scope,
			);
			yield* Effect.promise(() =>
				waitUntil(() => providerRequests === 4, () => undefined),
			);
			yield* Fiber.interrupt(fiber);
			cancellationSettled = true;
		}),
	),
);
phase = "wait for interrupted sidecar release";
await waitUntil(
	() => manager.ephemeralReviewIds().length === 0,
	() => undefined,
);

phase = "release trunk";
firstProviderRelease.resolve(undefined);
const trunkResult = await trunk;
const reviewIds = [...new Set(creations.map((creation) => creation.reviewId))];
const hotStateBeforeDispose = {
	executions: reviewIds.map((reviewId) => manager.executionState(reviewId)),
	ephemeralReviewIds: manager.ephemeralReviewIds(),
};
await Effect.runPromise(manager.dispose());
phase = "shutdown Runtime";
await hosts.shutdownActiveRuns();
await hosts.close();
clearTimeout(watchdog);

process.stdout.write(
	JSON.stringify({
		trunkResult,
		decision,
		failure,
		cancellationSettled,
		providerRequests,
		creations,
		hotStateBeforeDispose,
		managerDisposed: manager.isDisposed(),
	}),
);

function reviewRequest(
	approvalReviewerManager: AutoApprovalReviewerManager,
	targetModelToolCallId: string,
): RuntimeApprovalReviewRequest {
	return {
		workspaceId: input.workspaceId,
		sessionId: input.sessionId,
		sessionThreadId: input.sessionThreadId,
		bindingId: input.bindingId,
		bindingGeneration: input.bindingGeneration,
		targetPodUid: input.targetPodUid,
		runtimeBindingToken: "reviewer-composition-token",
		modelRequestId: "mreq_reviewer_composition_parent",
		parentBoundaryEventId: "evt_reviewer_composition_parent",
		targetModelToolCallId,
		targetToolName: "Write",
		actionJson: { path: "src/a.ts", content: "ok" },
		approvalReviewerManager,
		parentTranscript: {
			generation: 1,
			entries: [
				{
					messageSequence: 1,
					contextKind: "user",
					parts: [{ type: "text", text: "review this action" }],
				},
			],
		},
		currentAssistantDraft: [],
		siblingToolCalls: [
			{
				modelToolCallId: targetModelToolCallId,
				toolName: "Write",
				actionJson: { path: "src/a.ts", content: "ok" },
			},
		],
		policyContext: {
			approvalMode: "approve_for_me",
			permissionPolicy: "always_ask",
		},
		currentModel: { providerId: "anthropic", modelId: "claude-opus-4-8" },
	};
}

function deferred<T>(): {
	readonly promise: Promise<T>;
	readonly resolve: (value: T) => void;
} {
	let resolve: (value: T) => void = () => undefined;
	const promise = new Promise<T>((done) => {
		resolve = done;
	});
	return { promise, resolve };
}

async function waitUntil(
	predicate: () => boolean,
	earlyResult: () => unknown,
): Promise<void> {
	const deadline = Date.now() + 5_000;
	while (!predicate()) {
		if (earlyResult() !== undefined) {
			throw new Error(
				`reviewer settled before expected boundary: ${JSON.stringify(earlyResult())}`,
			);
		}
		if (Date.now() >= deadline) {
			throw new Error("reviewer production boundary did not advance");
		}
		await new Promise((resolve) => setTimeout(resolve, 1));
	}
}
