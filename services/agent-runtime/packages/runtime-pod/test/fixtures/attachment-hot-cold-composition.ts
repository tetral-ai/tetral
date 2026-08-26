import { readFile } from "node:fs/promises";
import { Metadata } from "@grpc/grpc-js";
import type { LLMRequest } from "@tetral/agent-runtime-core/src/llm/llm-service.js";
import * as ThreadLoop from "@tetral/agent-runtime-core/src/thread-loop/thread-loop.js";
import { ThreadRuntime } from "@tetral/agent-runtime-core/src/thread-loop/thread-runtime.js";
import type {
	RuntimeAcceptedInputState,
} from "@tetral/agent-runtime-core/src/thread-loop/input/accepted-input.js";
import {
	extractColdThreadToolRouteView,
	extractThreadTurnCheckpoint,
} from "@tetral/agent-runtime-core/src/thread-loop/turn/load.js";
import type { AgentRuntimeBridgeServiceClient } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { Effect } from "effect";
import type { TestContextLoader } from "../../../core/test/unit/thread-loop/thread-loop-test-support.js";
import {
	QueuedContextLoader,
	runtimeThreadLoopLayer,
	testRunCustody,
} from "../../../core/test/unit/thread-loop/thread-loop-test-support.js";
import { BridgeAPIContextLoader } from "../../src/bridge-client.js";

const inputPath = process.argv[2];
if (inputPath === undefined) {
	throw new Error("attachment hot/cold composition input path is required");
}

const input = JSON.parse(await readFile(inputPath, "utf8")) as {
	readonly acceptedInput: RuntimeAcceptedInputState;
	readonly commitInputsResponse?: unknown;
	readonly coldContextJson: string;
	readonly coldOnly?: boolean;
};

const bridgeClient = {
	commitInputs: (...args: unknown[]) => {
		const callback = args.at(-1) as (
			error: Error | null,
			response?: unknown,
		) => void;
		callback(null, input.commitInputsResponse);
		return { cancel() {} };
	},
	loadContext: (
		_request: unknown,
		_metadata: Metadata,
		callback: (error: Error | null, response?: unknown) => void,
	) => {
		callback(null, {
			contextJson: input.coldContextJson,
			runtimeBindingToken: "attachment-composition-binding-token",
		});
		return { cancel() {} };
	},
} as unknown as AgentRuntimeBridgeServiceClient;

const loader = new BridgeAPIContextLoader({
	address: "bridge.test:9090",
	tokenPath: "/var/run/token",
	client: bridgeClient,
	metadataFactory: async () => new Metadata(),
});
let hotRequests: readonly LLMRequest[] | undefined;
if (!input.coldOnly) {
	if (input.commitInputsResponse === undefined) {
		throw new Error("hot attachment composition input is incomplete");
	}
	const hotCommitResult = await loader.commitAcceptedInput(input.acceptedInput);
	const hotContextLoader: TestContextLoader = {
		buildContext: async () => [],
		loadPendingInput: async () => ({ type: "empty" }),
		commitAcceptedInput: async () => hotCommitResult,
	};
	const hot = new ThreadRuntime({
		workspaceId: input.acceptedInput.workspaceId,
		sessionId: input.acceptedInput.sessionId,
		sessionThreadId: input.acceptedInput.sessionThreadId,
		bindingId: input.acceptedInput.bindingId,
		bindingGeneration: input.acceptedInput.bindingGeneration,
		targetPodUid: input.acceptedInput.targetPodUid,
		runtimeBindingToken: "attachment-composition-binding-token",
	});
	hot.state.markPersistentContextLoaded();
	if (hot.state.enqueueAcceptedInput(input.acceptedInput) !== "applied") {
		throw new Error("hot attachment input was not admitted");
	}
	hotRequests = await runAndCapture("hot", hot, hotContextLoader);
}

const loaded = await loader.loadThreadContext({
	workspaceId: input.acceptedInput.workspaceId,
	sessionId: input.acceptedInput.sessionId,
	sessionThreadId: input.acceptedInput.sessionThreadId,
	bindingId: input.acceptedInput.bindingId,
	bindingGeneration: input.acceptedInput.bindingGeneration,
	targetPodUid: input.acceptedInput.targetPodUid,
});
const checkpoint = extractThreadTurnCheckpoint({
	contextEntries: loaded.contextEntries,
	facts: loaded.turnFacts,
});
const routes = extractColdThreadToolRouteView({
	checkpoint,
	pendingToolUses: loaded.pendingToolUses ?? [],
	pendingSandboxExecutions: loaded.pendingSandboxExecutions ?? [],
});
const cold = new ThreadRuntime({
	workspaceId: input.acceptedInput.workspaceId,
	sessionId: input.acceptedInput.sessionId,
	sessionThreadId: input.acceptedInput.sessionThreadId,
	bindingId: input.acceptedInput.bindingId,
	bindingGeneration: input.acceptedInput.bindingGeneration,
	targetPodUid: input.acceptedInput.targetPodUid,
	runtimeBindingToken: loaded.runtimeBindingToken,
});
cold.state.contextManager.replaceEntries(loaded.contextEntries);
cold.state.contextManager.installOpenRequestDraft(loaded.openRequestDraft);
cold.state.markPersistentContextLoaded();
cold.state.installThreadTurn(checkpoint, routes);
cold.state.replacePendingAttachments(loaded.pendingAttachments ?? []);
const coldRequests = await runAndCapture(
	"cold",
	cold,
	new QueuedContextLoader([], []),
);

process.stdout.write(
	JSON.stringify({
		...(hotRequests === undefined ? {} : { hot: requestProjection(hotRequests) }),
		cold: requestProjection(coldRequests),
	}),
);

async function runAndCapture(
	label: string,
	session: ThreadRuntime,
	contextLoader: TestContextLoader,
): Promise<readonly LLMRequest[]> {
	const requests: LLMRequest[] = [];
	const result = await Effect.runPromise(
		Effect.gen(function* () {
			return yield* (yield* ThreadLoop.Service).run(session, testRunCustody());
		}).pipe(
			Effect.provide(
				runtimeThreadLoopLayer(contextLoader, {
					installLoaderState: false,
					onStream: (request) => requests.push(request),
				}),
			),
		),
	);
	if (result.type !== "completed") {
		throw new Error(
			`${label} attachment composition run returned ${JSON.stringify(result)}`,
		);
	}
	return requests;
}

function requestProjection(requests: readonly LLMRequest[]) {
	return {
		requestCount: requests.length,
		context: requests[0]?.context ?? [],
		attachments: requests[0]?.attachments ?? [],
	};
}
