import { readFile } from "node:fs/promises";
import { Metadata } from "@grpc/grpc-js";
import type { SessionEventWriter } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import { AutoApprovalReviewerManager } from "@tetral/agent-runtime-core/src/session/approval-reviewer-manager.js";
import { DefaultProviderCallRuntimeConfig } from "@tetral/agent-runtime-core/src/thread-loop/provider-request.js";
import type { RuntimeApprovalReviewRequest } from "@tetral/agent-runtime-core/src/thread-loop/tool-execution.js";
import { createToolCatalog } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import { Effect, Stream } from "effect";
import { createRuntimeApprovalReviewer } from "../../src/approval-reviewer.js";
import { BridgeAPIApprovalReviewerThreadCreator } from "../../src/bridge-client.js";
import { buildRuntimeCoreHosts } from "../../src/core-hosts.js";

const inputPath = process.argv[2];
if (inputPath === undefined) {
	throw new Error("reviewer admission composition input is required");
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

const metadataFactory = async () => new Metadata();
const threadCreator = new BridgeAPIApprovalReviewerThreadCreator({
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
let durableDecisionReceipts = 0;
let nextMessageSequence = 1;
const writer: SessionEventWriter = {
	append: async (envelope) => {
		if (envelope.event.type === "approval_review.decision") {
			durableDecisionReceipts += 1;
		}
		return {
			ok: true,
			type: "committed",
			eventId: `evt_${envelope.writeId}`,
			...(envelope.assistantContextAppend === undefined
				? {}
				: {
						assistant: {
							messageSequence: nextMessageSequence++,
							createdToolUseEventIds: envelope.assistantContextAppend.parts
								.filter((part) => part.type === "tool")
								.map((_part, index) => `evt_tool_${envelope.writeId}_${index}`),
						},
					}),
		};
	},
	settleToolResult: async () => ({ ok: true, result: { type: "committed" } }),
	writeRequestEnd: async (envelope) => ({
		ok: true,
		type: "committed",
		requestEndEventId: `evt_${envelope.writeId}`,
		outcome: { type: "ordinary" },
		interruptToolResults: [],
		pendingAttachments: [],
	}),
	finishIdle: async (envelope) => ({
		ok: true,
		type: "committed",
		idleEventId: `evt_${envelope.durableTurnId}`,
	}),
	commitRuntimeTermination: async (envelope) => ({
		ok: true,
		type: "committed",
		failureEventId: `evt_failure_${envelope.writeId}`,
		closeoutEventId: `evt_closeout_${envelope.writeId}`,
	}),
};
const hosts = await buildRuntimeCoreHosts({
	maxLocalSessions: 4,
	now: () => "2026-08-18T00:00:00.000Z",
	contextLoader: {
		loadThreadContext: async () => ({
			contextEntries: [],
			turnFacts: { events: [], internalRepairs: [] },
			runtimeBindingToken: "reviewer-admission-composition-token",
		}),
		commitAcceptedInput: async (acceptedInput) => ({
			type: "committed",
			assignedContextSequences:
				acceptedInput.kind === "approval_review" ? [1] : [],
			pendingAttachments: [],
			interruptToolResults: [],
		}),
		refreshRuntimeBindingToken: async (identity) => identity.runtimeBindingToken,
	},
	threadLoop: {
		internalToolRepairStore: {} as never,
		sessionEventWriter: writer,
		runtime: {
			now: () => "2026-08-18T00:00:00.000Z",
			monotonicMs: () => 0,
			createId: (prefix) => `${prefix}_reviewer_composition_${++nextId}`,
			sleep: async () => true,
		},
		llmService: {
			stream: () => {
				providerRequests += 1;
				const response = Stream.fromIterable([
					{ type: "text-start" as const, id: `review-${providerRequests}` },
					{
						type: "text-delta" as const,
						id: `review-${providerRequests}`,
						text_delta: JSON.stringify({
							outcome: "allow",
							risk_level: "low",
							user_authorization: "high",
							rationale: "composed allow",
						}),
					},
					{ type: "text-end" as const, id: `review-${providerRequests}` },
					{ type: "finish" as const, finishReason: "stop" as const },
				]);
				return providerRequests === 1
					? Stream.unwrap(
							Effect.promise(async () => {
								await firstProviderRelease.promise;
								return response;
							}),
						)
					: response;
			},
		},
		acceptSandboxExecution: async () => ({ type: "accepted" as const }),
		awaitSandboxExecution: async () => ({ type: "cancelled" as const }),
		storeOperationTimeoutMs: 5_000,
		approvalMode: "full_access",
		providerCallRuntime: {
			...DefaultProviderCallRuntimeConfig,
			approvalReviewerPolicy: "Return the required approval decision JSON.",
			timeoutMs: 5_000,
		},
		runtimeModel: () => ({ providerId: "fake", modelId: "fake-chat" }),
		runtimePolicy: () => ({ toolCatalog: createToolCatalog({ family: "claude" }) }),
	},
});

const manager = new AutoApprovalReviewerManager();
const reviewer = createRuntimeApprovalReviewer(() => hosts.subAgentRunHost, {
	model: { providerId: "anthropic", modelId: "claude-opus-4-8" },
	threadCreator: recordingThreadCreator,
	waitTimeoutMs: 5_000,
});
const trunkRequest = reviewRequest(manager, "tool_call_reviewer_trunk");
const lostAck = await Effect.runPromise(reviewer(trunkRequest));
if (lostAck.type !== "settlement_failed") {
	throw new Error(
		`lost admission ACK did not preserve uncertainty: ${JSON.stringify(lostAck)}`,
	);
}
let earlyTrunkResult: unknown;
const trunk = Effect.runPromise(reviewer(trunkRequest)).then((result) => {
	earlyTrunkResult = result;
	return result;
});
await waitUntil(() => providerRequests === 1, () => earlyTrunkResult);
const sidecar = await Effect.runPromise(
	reviewer(reviewRequest(manager, "tool_call_reviewer_sidecar")),
);
firstProviderRelease.resolve(undefined);
const trunkResult = await trunk;
const reviewIds = [...new Set(creations.map((creation) => creation.reviewId))];
const hotStateBeforeDispose = {
	executions: reviewIds.map((reviewId) => manager.executionState(reviewId)),
	ephemeralReviewIds: manager.ephemeralReviewIds(),
};
await Effect.runPromise(manager.dispose());
await hosts.close();

process.stdout.write(
	JSON.stringify({
		lostAck,
		trunkResult,
		sidecar,
		providerRequests,
		durableDecisionReceipts,
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
		runtimeBindingToken: "reviewer-admission-composition-token",
		modelRequestId: "mreq_reviewer_admission_composition_parent",
		parentBoundaryEventId: "evt_reviewer_admission_composition_parent",
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
				`reviewer settled before provider request: ${JSON.stringify(earlyResult())}`,
			);
		}
		if (Date.now() >= deadline) {
			throw new Error("reviewer provider request did not start");
		}
		await new Promise((resolve) => setTimeout(resolve, 1));
	}
}
