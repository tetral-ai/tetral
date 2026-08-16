import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { Metadata } from "@grpc/grpc-js";
import type { AcceptedInputCommitResult } from "@tetral/agent-runtime-core/src/context/context-loader.js";
import type { LLMRequest } from "@tetral/agent-runtime-core/src/llm/llm-service.js";
import * as ThreadLoop from "@tetral/agent-runtime-core/src/thread-loop/thread-loop.js";
import { ThreadRuntime } from "@tetral/agent-runtime-core/src/thread-loop/thread-runtime.js";
import type { RuntimeAcceptedInputState } from "@tetral/agent-runtime-core/src/thread-loop/thread-state.js";
import type { AcceptInputRequest } from "@tetral/agent-runtime-protocol/src/gen/tetral/agent_runtime/v1/agent_runtime.js";
import type { AgentRuntimeBridgeServiceClient } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { Effect } from "effect";
import type { TestContextLoader } from "../../../core/test/unit/thread-loop/thread-loop-test-support.js";
import {
	providerAttachmentsForTest,
	runtimeThreadLoopLayer,
	testRunCustody,
	threadLoopRuntime,
} from "../../../core/test/unit/thread-loop/thread-loop-test-support.js";
import { BridgeAPIContextLoader } from "../../src/bridge-client.js";
import type { RuntimePodLogger } from "../../src/logger.js";
import type {
	RuntimeAuthenticator,
	RuntimeCleanupController,
	RuntimeSessionRunHost,
} from "../../src/runtime-service.js";
import { RuntimeControlService } from "../../src/runtime-service.js";

interface AttachmentInputFixture {
	readonly acceptInput: AcceptInputRequest;
	readonly commitInputs: unknown;
}

test("producer attachment receipt crosses Runtime ingress and starts one provider request", async () => {
	const fixture = JSON.parse(
		readFileSync(
			resolve(
				import.meta.dir,
				"../../../../../../testdata/attachment-only-runtime-input.json",
			),
			"utf8",
		),
	) as AttachmentInputFixture;
	const commands: RuntimeAcceptedInputState[] = [];
	const service = new RuntimeControlService({
		ownPod: {
			namespace: "engine",
			name: "runtime-pod",
			uid: fixture.acceptInput.targetPodUid,
			ip: "127.0.0.1",
		},
		allowedBridge: { namespace: "engine", name: "bridge" },
		authenticator: {
			authenticate: async () => ({
				ok: true as const,
				serviceAccount: { namespace: "engine", name: "bridge" },
			}),
		} satisfies RuntimeAuthenticator,
		runHost: {
			handleAcceptInput: async (command: RuntimeAcceptedInputState) => {
				commands.push(command);
				return {
					ok: true as const,
					sessionId: command.sessionId,
					created: true,
					started: true,
				};
			},
		} as RuntimeSessionRunHost,
		cleanupController: {} as RuntimeCleanupController,
		logger: {
			debug: () => undefined,
			info: () => undefined,
			warn: () => undefined,
			error: () => undefined,
		} as RuntimePodLogger,
		ready: () => true,
	});
	const metadata = new Metadata();
	metadata.set("authorization", "bearer test-token");
	expect(await service.acceptInput(fixture.acceptInput, metadata)).toEqual({
		accepted: {},
	});
	expect(commands).toHaveLength(1);
	expect(commands[0]).toMatchObject({
		kind: "messages",
		contentJson: '{"messages":[{"parts":[]}]}',
	});

	const bridgeClient = {
		commitInputs: (...args: unknown[]) => {
			const callback = args.at(-1) as (
				error: Error | null,
				response?: unknown,
			) => void;
			callback(null, fixture.commitInputs);
			return {};
		},
	} as unknown as AgentRuntimeBridgeServiceClient;
	const bridgeLoader = new BridgeAPIContextLoader({
		address: "bridge.test:9090",
		tokenPath: "/var/run/secrets/token",
		client: bridgeClient,
		metadataFactory: async () => new Metadata(),
	});
	const loader: TestContextLoader = {
		buildContext: async () => [],
		loadPendingInput: async () => ({ type: "empty" }),
		commitAcceptedInput: async (
			input,
		): Promise<AcceptedInputCommitResult> =>
			await bridgeLoader.commitAcceptedInput(input),
	};
	const command = commands[0];
	if (command === undefined) throw new Error("Runtime did not accept input");
	const session = new ThreadRuntime({
		workspaceId: command.workspaceId,
		sessionId: command.sessionId,
		sessionThreadId: command.sessionThreadId,
		bindingId: command.bindingId,
		bindingGeneration: command.bindingGeneration,
		targetPodUid: command.targetPodUid,
		runtimeBindingToken: "binding-token",
	});
	session.state.enqueueAcceptedInput(command);
	const requests: LLMRequest[] = [];
	const result = await Effect.runPromise(
		Effect.gen(function* () {
			return yield* (yield* ThreadLoop.Service).run(session, testRunCustody());
		}).pipe(
			Effect.provide(
				runtimeThreadLoopLayer(loader, {
					onStream: (request) => requests.push(request),
					runtime: {
						...threadLoopRuntime(),
						sleep: async (_milliseconds, signal) =>
							await new Promise<boolean>((resolve) => {
								const timer = setTimeout(() => resolve(true), 0);
								signal.addEventListener(
									"abort",
									() => {
										clearTimeout(timer);
										resolve(false);
									},
									{ once: true },
								);
							}),
					},
				}),
			),
		),
	);

	expect(result).toMatchObject({ type: "completed" });
	expect(requests).toHaveLength(1);
	expect(requests[0]?.context).toEqual([]);
	expect(requests[0]?.attachments).toEqual(
		providerAttachmentsForTest([
			{
				transient: undefined,
				fileBacked: {
					sourceEventId: "sevt_attachment_input",
					fileId: "file_first_queued_image",
				},
				mime: "image/png",
				filename: "image.png",
			},
			{
				transient: undefined,
				fileBacked: {
					sourceEventId: "sevt_attachment_input",
					fileId: "file_first_queued_document",
				},
				mime: "text/plain",
				filename: "notes.txt",
			},
		]),
	);
});
