import { describe, expect, jest, test } from "bun:test";
import {
	ProviderRequestKind,
	SystemCacheHint,
	SystemSegmentKind,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import {
	Cause,
	Context,
	Effect,
	Exit,
	Fiber,
	Layer,
	Scope,
	Stream,
} from "effect";
import type { AcceptedInputCommitResult } from "../../../src/context/context-loader.js";
import { normalizeProviderError } from "../../../src/contracts/provider.js";
import type {
	RuntimeContextEntry,
	SessionEvent,
	SessionEventEnvelope,
	SessionEventWriter,
	SessionEventWriterRequestEndEnvelope,
	SessionEventWriterToolSettlementEnvelope,
} from "../../../src/contracts/runtime.js";
import {
	normalizeSessionEventWriterError,
	SessionEventWriterRetryPolicy,
} from "../../../src/contracts/runtime.js";
import type { LLMEvent } from "../../../src/llm/llm-event.js";
import {
	LLMEventSchema,
	runtimeFailureFromProviderError,
} from "../../../src/llm/llm-event.js";
import type {
	LLMRequest,
	LLMServiceError,
	Interface as LLMServiceInterface,
} from "../../../src/llm/llm-service.js";
import { ProviderStreamAccumulator } from "../../../src/runtime/accumulator.js";
import { AutoApprovalReviewerManager } from "../../../src/session/approval-reviewer-manager.js";
import * as SessionManager from "../../../src/session/session-manager.js";
import * as ThreadLoop from "../../../src/thread-loop/thread-loop.js";
import { ThreadRuntime } from "../../../src/thread-loop/thread-runtime.js";
import type {
	RuntimeAcceptedInputState,
	RuntimeControlInputDeclaration,
} from "../../../src/thread-loop/thread-state.js";
import type {
	RuntimeApprovalReviewRequest,
	RuntimeToolExecutionResult,
} from "../../../src/thread-loop/tool-execution.js";
import type { ToolCatalog } from "../../../src/tools/tool-catalog.js";
import { createToolCatalog } from "../../../src/tools/tool-catalog.js";
import { SessionToolCoordinator } from "../../../src/tools/tool-scheduler.js";
import type {
	TestContextLoader,
	TestDurableSequence,
} from "./thread-loop-test-support.js";
import {
	acceptedInput,
	acceptedInputCommitResult,
	approvalReviewAcceptedInput,
	approvalReviewerOutputSchemaJson,
	approvalReviewerPolicy,
	buildRuntimeControlCommitResult,
	catalogForTest,
	createdAt,
	deferred,
	durableRuntimeNotificationMessage,
	flushMicrotasks,
	installRecoveredToolTurn,
	interruptInput,
	memoryCatalogForTest,
	providerAttachmentsForTest,
	QueuedContextLoader,
	queuedLLMService,
	RecordingContextLoader,
	RecordingRuntimeMetrics,
	requestEndResultForTest,
	runtimeNotificationMessage,
	runtimeThreadLoopLayer,
	sleepUntilAborted,
	ThreadLoopRuntimeStore,
	taskNotificationInput,
	testRunCustody,
	threadLoopRuntime,
	userMessage,
	waitForCondition,
	waitForReleaseOrAbort,
	writerFrom,
} from "./thread-loop-test-support.js";

describe("ThreadLoop", () => {
	function interruptCommitResult(
		command: SessionManager.RuntimeInterruptControlCommand,
		declaration: RuntimeControlInputDeclaration,
		unfinishedToolUseEventIds: readonly string[],
	) {
		const result = buildRuntimeControlCommitResult(
			command,
			"interrupt_control",
			declaration,
		);
		if (!result.ok || !("interruptToolResults" in result)) {
			return result;
		}
		return {
			...result,
			interruptToolResults: unfinishedToolUseEventIds.map((toolUseEventId) => ({
				toolUseEventId,
				result: { type: "cancelled" as const },
			})),
		};
	}

	function contextToolStatus(
		session: ThreadRuntime,
		modelToolCallId?: string,
	): string | undefined {
		const parts = [
			...session.state.contextManager.entries().flatMap((entry) => entry.parts),
			...(session.state.contextManager.openRequestDraft()?.parts ?? []),
		];
		const matches = (part: { readonly modelToolCallId?: string }): boolean =>
			modelToolCallId === undefined || part.modelToolCallId === modelToolCallId;
		const result = parts.findLast(
			(part) => part.type === "tool_result" && matches(part),
		);
		if (result?.type === "tool_result") return result.result.type;
		return parts.some((part) => part.type === "tool_call" && matches(part))
			? "running"
			: undefined;
	}

	function sealedToolContextEntry(
		_modelRequestId: string,
		messageSequence: number,
		calls: ReadonlyArray<{
			readonly modelToolCallId: string;
			readonly toolName: string;
			readonly canonicalInput: Extract<
				RuntimeContextEntry["parts"][number],
				{ readonly type: "tool_call" }
			>["canonicalInput"];
		}>,
	): RuntimeContextEntry {
		return {
			messageSequence,
			contextKind: "assistant",
			parts: calls.map((call) => ({ type: "tool_call" as const, ...call })),
		};
	}

	test("cold apply_patch recovery accepts only the canonical execution object", async () => {
		const patch =
			"*** Begin Patch\n*** Add File: note.txt\n+ok\n*** End Patch\n";
		const cases = [
			{ name: "canonical", input: { patch }, ok: true },
			{ name: "scalar", input: patch, ok: false },
			{ name: "wrong field", input: { content: patch }, ok: false },
			{ name: "null patch", input: { patch: null }, ok: false },
			{ name: "numeric patch", input: { patch: 7 }, ok: false },
			{ name: "extra field", input: { patch, content: patch }, ok: false },
		] as const;
		for (const recoveryKind of ["approval", "sandbox"] as const) {
			for (const candidate of cases) {
				const suffix = `${recoveryKind}_${candidate.name.replaceAll(" ", "_")}`;
				const session = new ThreadRuntime(`sesn_patch_${suffix}`);
				const toolUseEventId = `sevt_patch_${suffix}`;
				const modelRequestId = `mreq_patch_${suffix}`;
				const message = sealedToolContextEntry(modelRequestId, 1, [
					{
						modelToolCallId: `call_patch_${suffix}`,
						toolName: "apply_patch",
						canonicalInput: patch,
					},
				]);
				const catalog = createToolCatalog({ family: "gpt" });
				const result = await Effect.runPromise(
					Effect.gen(function* () {
						const threadLoop = yield* ThreadLoop.Service;
						session.state.markPersistentContextLoaded();
						threadLoop.seedRuntimeModel(session);
						installRecoveredToolTurn(session, modelRequestId, [
							{
								modelToolCallId: `call_patch_${suffix}`,
								toolUseEventId,
								toolName: "apply_patch",
								...(recoveryKind === "sandbox"
									? { disposition: "resume_sandbox_execution" as const }
									: {}),
							},
						]);
						return recoveryKind === "approval"
							? yield* threadLoop.installLoadedPendingToolUses(
									session,
									[
										{
											toolUseEventId,
											modelRequestId,
											modelToolCallId: `call_patch_${suffix}`,
											toolName: "apply_patch",
											input: candidate.input,
											status: "pending" as const,
										},
									],
									[message],
									undefined,
								)
							: yield* threadLoop.installLoadedSandboxExecutions(
									session,
									[
										{
											toolUseEventId,
											modelRequestId,
											modelToolCallId: `call_patch_${suffix}`,
											toolName: "apply_patch",
											input: candidate.input,
											executionState: "running" as const,
										},
									],
									[message],
									undefined,
								);
					}).pipe(
						Effect.provide(
							runtimeThreadLoopLayer(new QueuedContextLoader([], []), {
								providerCallRuntime: {
									systemInstructions: "cold apply patch contract",
									toolCatalog: catalog,
								},
							}),
						),
					),
				);
				if (candidate.ok && !result.ok) throw result.error;
				expect(result.ok, `${recoveryKind}: ${candidate.name}`).toBe(
					candidate.ok,
				);
			}
		}
	});

	test("apply_patch preserves the provider scalar while declaring one canonical execution object", async () => {
		const patch =
			"*** Begin Patch\n*** Add File: note.txt\n+hello\n*** End Patch\n";
		const session = new ThreadRuntime("sesn_patch_producer");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-patch", 0, "apply it")],
		});
		let toolUseEnvelope: SessionEventEnvelope | undefined;
		let executionInput: unknown;
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						approvalMode: "full_access",
						writer: writerFrom((envelope) => {
							if (envelope.event.type === "agent.tool_use")
								toolUseEnvelope = envelope;
							return {
								ok: true,
								eventId:
									envelope.event.type === "agent.tool_use"
										? "sevt_patch_producer"
										: `bridge-${envelope.writeId}`,
								type: "committed",
								eventSequence: 1,
							};
						}),
						events: [
							{
								type: "tool-call",
								id: "call-patch-producer",
								toolName: "apply_patch",
								input: patch,
								inputPreview: { preview: patch, truncated: false },
							},
							{ type: "finish", finishReason: "tool-calls" },
						],
						providerCallRuntime: {
							systemInstructions: "apply patch producer projection test",
							toolCatalog: createToolCatalog({ family: "gpt" }),
							toolsetFamily: "gpt",
						},
						runTool: (request) => {
							executionInput = request.input;
							return {
								type: "completed",
								output: { text: "done", truncated: false },
							};
						},
					}),
				),
			),
		);

		expect(result).toMatchObject({ type: "completed" });
		const toolPart = toolUseEnvelope?.assistantContextAppend?.parts.find(
			(part) => part.type === "tool",
		);
		expect(
			toolPart?.type === "tool" && toolPart.state.status === "running"
				? toolPart.state.input.value
				: undefined,
		).toBe(patch);
		expect(toolUseEnvelope?.event.type).toBe("agent.tool_use");
		if (toolUseEnvelope?.event.type !== "agent.tool_use")
			throw new Error("expected apply_patch Tool Use event");
		expect(toolUseEnvelope.event.name).toBe("apply_patch");
		expect(toolUseEnvelope.event.input).toEqual({ patch });
		expect(toolUseEnvelope.distinctProviderInput).toBe(patch);
		expect(toolUseEnvelope.toolRouteCapability).toBe("sandbox_execute");
		expect(executionInput).toEqual({ patch });
		expect(executionInput).not.toEqual({ patch: { patch } });
	});

	test("first accepted turn rides the file attachments returned by CommitInputs", async () => {
		const session = new ThreadRuntime("sesn_first_turn_media");
		const input = {
			...acceptedInput("rin_first_turn_media", session.sessionId),
			contentJson: JSON.stringify({
				messages: [{ parts: [] }],
			}),
		};
		session.state.enqueueAcceptedInput(input);
		const attachment = {
			transient: undefined,
			fileBacked: {
				sourceEventId: "sevt_first_turn_media",
				fileId: "file_first_turn_media",
			},
			mime: "image/png",
			filename: "first-turn.png",
		} as const;
		const loader: TestContextLoader = {
			buildContext: async () => [],
			loadPendingInput: async () => ({ type: "empty" }),
			commitAcceptedInput: async (accepted) => {
				const committed = acceptedInputCommitResult(accepted);
				return { ...committed, pendingAttachments: [attachment] };
			},
		};
		const requests: LLMRequest[] = [];
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						onStream: (request) => requests.push(request),
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed" });
		expect(requests).toHaveLength(1);
		expect(requests[0]?.context).toEqual([]);
		expect(requests[0]?.attachments).toEqual(
			providerAttachmentsForTest([attachment]),
		);
	});
	for (const finishReason of ["stop", "unknown"] as const) {
		test(`lost Tool Use acknowledgement retries one write identity and continues after ${finishReason}`, async () => {
			const session = new ThreadRuntime("sesn_tool_use_ack_loss");
			session.state.enqueueAcceptedInput(
				acceptedInput("rin_tool_use_ack_loss", session.sessionId),
			);
			const toolUseWriteIds: string[] = [];
			const requests: LLMRequest[] = [];
			let toolExecutions = 0;
			const writer = writerFrom((envelope) => {
				if (envelope.event.type === "agent.tool_use") {
					toolUseWriteIds.push(envelope.writeId);
					if (toolUseWriteIds.length === 1) {
						return {
							ok: false,
							error: normalizeSessionEventWriterError({
								code: "timeout",
								sessionId: envelope.sessionId,
							}),
						};
					}
				}
				return {
					ok: true,
					eventId:
						envelope.event.type === "agent.tool_use"
							? "sevt_tool_use_ack_loss"
							: `bridge-${envelope.writeId}`,
					type: "committed",
					eventSequence: 1,
				};
			});
			const result = await Effect.runPromise(
				Effect.gen(function* () {
					return yield* (yield* ThreadLoop.Service).run(
						session,
						testRunCustody(),
					);
				}).pipe(
					Effect.provide(
						runtimeThreadLoopLayer(new QueuedContextLoader([], []), {
							llmService: queuedLLMService(
								[
									[
										{
											type: "tool-call",
											id: "call-tool-use-ack-loss",
											toolName: "lookup_ack_loss",
											input: { query: "durable" },
											inputPreview: {
												preview: '{"query":"durable"}',
												truncated: false,
											},
										},
										{ type: "finish", finishReason },
									],
									[
										{ type: "text-start", id: "text-after-tool" },
										{
											type: "text-delta",
											id: "text-after-tool",
											text_delta: "continued",
										},
										{ type: "text-end", id: "text-after-tool" },
										{ type: "finish", finishReason: "stop" },
									],
								],
								requests,
							),
							writer,
							providerCallRuntime: {
								systemInstructions: "Tool Use acknowledgement recovery test",
								toolCatalog: catalogForTest({
									name: "lookup_ack_loss",
									description: "Look up a durable value",
									inputSchema: { type: "object" },
								}),
							},
							runTool: () => {
								toolExecutions += 1;
								return {
									type: "completed",
									output: { text: "found", truncated: false },
								};
							},
						}),
					),
				),
			);
			expect(result).toMatchObject({ type: "completed" });
			expect(toolUseWriteIds).toHaveLength(2);
			expect(new Set(toolUseWriteIds).size).toBe(1);
			expect(toolExecutions).toBe(1);
			expect(requests).toHaveLength(2);
			expect(JSON.stringify(requests[1]?.context)).toContain("found");
		});
	}
	test("approval reviewer sessions mark provider requests, request-end events, and metrics as reviewer work", async () => {
		const session = new ThreadRuntime({
			workspaceId: "wksp_reviewer",
			sessionId: "sesn_1",
			sessionThreadId: "thrd_reviewer",
			parentThreadId: "thrd_main",
			threadRole: "approval_reviewer",
			bindingId: "bind_reviewer",
			bindingGeneration: 1,
			targetPodUid: "pod_reviewer",
			runtimeBindingToken: "binding-token-reviewer",
		});
		session.state.enqueueAcceptedInput(approvalReviewAcceptedInput());
		const loader = new QueuedContextLoader([], []);
		const requests: LLMRequest[] = [];
		const requestEndEnvelopes: SessionEventWriterRequestEndEnvelope[] = [];
		const baseWriter = writerFrom((envelope) => ({
			ok: true,
			eventId: `bridge-${envelope.writeId}`,
			type: "committed",
			eventSequence: 1,
		}));
		const writer: SessionEventWriter = {
			...baseWriter,
			writeRequestEnd: async (envelope) => {
				requestEndEnvelopes.push(envelope);
				return await baseWriter.writeRequestEnd(envelope);
			},
		};
		const metrics = new RecordingRuntimeMetrics();
		const llm = queuedLLMService(
			[
				[
					{ type: "text-start", id: "text-1" },
					{ type: "text-delta", id: "text-1", text_delta: "allow" },
					{ type: "text-end", id: "text-1" },
					{
						type: "finish",
						finishReason: "stop",
						usage: {
							inputTokens: 3,
							outputTokens: 1,
							reasoningTokens: 0,
							cacheReadTokens: 0,
							cacheWriteTokens: 0,
						},
						modelLimits: {
							contextWindowTokens: 100000,
							outputTokenLimit: 4096,
						},
					},
				],
			],
			requests,
		);
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						llmService: llm,
						metrics,
						writer,
						runtimeModel: () => ({
							providerId: "anthropic",
							modelId: "claude-opus-4-8",
						}),
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed", modelMessageCount: 1 });
		expect(requests).toHaveLength(1);
		expect(requests[0]?.requestKind).toBe(
			ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
		);
		expect(requests[0]?.outputSchemaJson).toBe(
			approvalReviewerOutputSchemaJson,
		);
		expect(requests[0]?.system).toContainEqual({
			kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_APPROVAL_REVIEWER_POLICY,
			text: approvalReviewerPolicy,
			cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE,
		});
		expect(requests[0]?.model).toEqual({
			providerId: "anthropic",
			modelId: "claude-opus-4-8",
			variant: "",
		});
		expect(requestEndEnvelopes).toHaveLength(1);
		expect(metrics.providerStreamDurations).toContainEqual(
			expect.objectContaining({
				kind: "approval_reviewer",
				outcome: "success",
			}),
		);
		expect(session.state.lastRequestUsage()).toEqual({
			inputTokens: 3,
			outputTokens: 1,
			reasoningTokens: 0,
			cacheReadTokens: 0,
			cacheWriteTokens: 0,
		});
		expect(session.state.lastRequestModelLimits()).toEqual({
			contextWindowTokens: 100000,
			outputTokenLimit: 4096,
		});
	});
	test("approval reviewer waits for its created input receipt before starting the provider request", async () => {
		const session = new ThreadRuntime({
			workspaceId: "wksp_reviewer_receipt",
			sessionId: "sesn_reviewer_receipt",
			sessionThreadId: "thrd_reviewer_receipt",
			parentThreadId: "thrd_main",
			threadRole: "approval_reviewer",
			bindingId: "bind_reviewer_receipt",
			bindingGeneration: 1,
			targetPodUid: "pod_reviewer_receipt",
			runtimeBindingToken: "binding-token-reviewer-receipt",
		});
		const reviewerInput = {
			...approvalReviewAcceptedInput("rin_reviewer_receipt"),
			workspaceId: session.identity.workspaceId,
			sessionId: session.identity.sessionId,
			sessionThreadId: session.identity.sessionThreadId,
			bindingId: session.identity.bindingId,
			bindingGeneration: session.identity.bindingGeneration,
			targetPodUid: session.identity.targetPodUid,
		};
		expect(session.state.enqueueAcceptedInput(reviewerInput)).toBe("applied");
		expect(session.state.threadTurnReduction().action).toEqual({
			action: "commit_accepted_input",
			runtimeInputId: reviewerInput.runtimeInputId,
		});
		session.state.markPersistentContextLoaded();
		const releaseCommit = deferred<void>();
		const loader = new QueuedContextLoader(
			[],
			[],
			[
				async (input: RuntimeAcceptedInputState) => {
					await releaseCommit.promise;
					return acceptedInputCommitResult(input);
				},
			],
		);
		const requests: LLMRequest[] = [];
		const appended: SessionEvent[] = [];
		const commitOutcomes: string[] = [];
		let earlyResult: ThreadLoop.ThreadLoopRunResult | undefined;
		const run = Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						onStream: (request) => requests.push(request),
						installLoaderState: false,
						writer: writerFrom((envelope) => {
							appended.push(envelope.event);
							return {
								ok: true,
								eventId: `bridge-${envelope.writeId}`,
								type: "committed",
								eventSequence: 1,
							};
						}),
						recordAcceptedInputCommit: (event) =>
							commitOutcomes.push(`${event.attempt}:${event.outcome}`),
						runtime: { ...threadLoopRuntime(), sleep: sleepUntilAborted },
					}),
				),
			),
		).then((result) => {
			earlyResult = result;
			return result;
		});

		await waitForCondition(
			() => loader.commitCalls.length === 1 || earlyResult !== undefined,
			"reviewer CommitInputs call",
		);
		expect({
			earlyResult,
			appended,
			commitOutcomes,
			accepted: session.state.acceptedInputSnapshot(),
		}).toEqual({
			earlyResult: undefined,
			appended: [expect.objectContaining({ type: "session.status_running" })],
			commitOutcomes: ["1:started"],
			accepted: [reviewerInput],
		});
		expect(requests).toHaveLength(0);
		releaseCommit.resolve(undefined);
		await run;

		expect(requests).toHaveLength(1);
		expect(requests[0]?.requestKind).toBe(
			ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
		);
	});
	test("approval reviewer tools settle before the reviewer produces its final decision", async () => {
		const session = new ThreadRuntime({
			workspaceId: "wksp_reviewer_tools",
			sessionId: "sesn_reviewer_tools",
			sessionThreadId: "thrd_reviewer_tools",
			parentThreadId: "thrd_main",
			threadRole: "approval_reviewer",
			bindingId: "bind_reviewer_tools",
			bindingGeneration: 1,
			targetPodUid: "pod_reviewer_tools",
			runtimeBindingToken: "binding-token-reviewer-tools",
		});
		session.state.enqueueAcceptedInput(
			approvalReviewAcceptedInput("rin_reviewer_tools"),
		);
		const requests: LLMRequest[] = [];
		const llm = queuedLLMService(
			[
				[
					{
						type: "tool-call",
						id: "call_reviewer_read",
						toolName: "Read",
						input: { file_path: "README.md" },
						inputPreview: { preview: "{}", truncated: false },
					},
					{ type: "finish", finishReason: "tool-calls" },
				],
				[
					{ type: "text-start", id: "decision" },
					{
						type: "text-delta",
						id: "decision",
						text_delta: '{"decision":"allow"}',
					},
					{ type: "text-end", id: "decision" },
					{ type: "finish", finishReason: "stop" },
				],
			],
			requests,
		);
		let toolCalls = 0;
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(new QueuedContextLoader([], []), {
						llmService: llm,
						providerCallRuntime: {
							systemInstructions: "reviewer tool continuation",
							toolCatalog: catalogForTest({
								name: "Read",
								description: "Read",
								inputSchema: { type: "object" },
							}),
						},
						runTool: () => {
							toolCalls += 1;
							return {
								type: "completed",
								output: { text: "file contents", truncated: false },
							};
						},
					}),
				),
			),
		);

		expect(result).toMatchObject({ type: "completed" });
		expect(toolCalls).toBe(1);
		expect(requests).toHaveLength(2);
		expect(JSON.stringify(requests[1]?.context)).toContain("file contents");
	});
	test("accepted declarations preserve approval-reviewer thread metadata without reloading context", async () => {
		const session = new ThreadRuntime({
			workspaceId: "wksp_reviewer",
			sessionId: "sesn_1",
			sessionThreadId: "thrd_reviewer",
			parentThreadId: "thrd_main",
			threadRole: "approval_reviewer",
			bindingId: "bind_reviewer",
			bindingGeneration: 1,
			targetPodUid: "pod_reviewer",
			runtimeBindingToken: "binding-token-before-commit",
		});
		session.state.recordLastRequestCompletion(
			{
				inputTokens: 50,
				outputTokens: 5,
				reasoningTokens: 0,
				cacheReadTokens: 0,
				cacheWriteTokens: 0,
			},
			{
				contextWindowTokens: 100000,
				outputTokenLimit: 4096,
			},
			0,
		);
		session.state.enqueueAcceptedInput(
			approvalReviewAcceptedInput("rin_reviewer_context"),
		);
		const loader = new QueuedContextLoader([], []);
		const requests: LLMRequest[] = [];
		const requestEndEnvelopes: SessionEventWriterRequestEndEnvelope[] = [];
		const baseWriter = writerFrom((envelope) => ({
			ok: true,
			eventId: `bridge-${envelope.writeId}`,
			type: "committed",
			eventSequence: 1,
		}));
		const writer: SessionEventWriter = {
			...baseWriter,
			writeRequestEnd: async (envelope) => {
				requestEndEnvelopes.push(envelope);
				return await baseWriter.writeRequestEnd(envelope);
			},
		};
		const llm = queuedLLMService(
			[
				[
					{ type: "text-start", id: "text-1" },
					{ type: "text-delta", id: "text-1", text_delta: "allow" },
					{ type: "text-end", id: "text-1" },
					{ type: "finish", finishReason: "stop" },
				],
			],
			requests,
		);
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, { llmService: llm, writer }),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed", modelMessageCount: 1 });
		expect(session.identity).toMatchObject({
			parentThreadId: "thrd_main",
			threadRole: "approval_reviewer",
			runtimeBindingToken: "binding-token-before-commit",
		});
		expect(requests[0]?.requestKind).toBe(
			ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
		);
		expect(requestEndEnvelopes).toHaveLength(1);
		expect(session.state.lastRequestUsage()).toBeUndefined();
		expect(session.state.lastRequestModelLimits()).toBeUndefined();
	});
	test("provider reschedule does not repeat committed RunTool effect", async () => {
		const session = new ThreadRuntime("sesn_1");
		const loader = new QueuedContextLoader(
			[],
			[
				{
					type: "context",
					entries: [userMessage("user-1", 0, "apply the mutation")],
				},
			],
		);
		const requests: LLMRequest[] = [];
		const committedToolOutput = "reschedule mutation committed";
		const failedReasoning = "failed attempt reasoning";
		const failedDraft = "failed attempt draft";
		const successfulReasoning = "successful retry reasoning";
		const llm: LLMServiceInterface = {
			stream(request) {
				requests.push(request);
				if (requests.length === 1) {
					return Stream.fromIterable([
						{
							type: "tool-call" as const,
							id: "tool-reschedule",
							toolName: "mutate_record",
							input: { record_id: "reschedule", value: "committed" },
							inputPreview: {
								preview: '{"record_id":"reschedule","value":"committed"}',
								truncated: false,
							},
						},
						{ type: "finish", finishReason: "tool-calls" },
					]);
				}
				if (requests.length === 2) {
					return Stream.fromIterable([
						{ type: "reasoning-start", id: "reasoning-reschedule-failed" },
						{
							type: "reasoning-delta",
							id: "reasoning-reschedule-failed",
							text_delta: failedReasoning,
						},
						{ type: "reasoning-end", id: "reasoning-reschedule-failed" },
						{ type: "text-start", id: "text-reschedule-failed" },
						{
							type: "text-delta",
							id: "text-reschedule-failed",
							text_delta: failedDraft,
						},
						{
							type: "provider-error",
							error: runtimeFailureFromProviderError(
								normalizeProviderError({
									code: "provider_unavailable",
									message:
										"temporary provider failure after committed mutation",
									retryable: true,
									fatal: false,
								}),
							),
						},
					]);
				}
				return Stream.fromIterable([
					{ type: "reasoning-start", id: "reasoning-reschedule-success" },
					{
						type: "reasoning-delta",
						id: "reasoning-reschedule-success",
						text_delta: successfulReasoning,
					},
					{ type: "reasoning-end", id: "reasoning-reschedule-success" },
					{ type: "text-start", id: "text-reschedule-success" },
					{
						type: "text-delta",
						id: "text-reschedule-success",
						text_delta: "mutation confirmed",
					},
					{ type: "text-end", id: "text-reschedule-success" },
					{ type: "finish", finishReason: "stop" },
				]);
			},
		};
		const appended: SessionEvent[] = [];
		const settlements: SessionEventWriterToolSettlementEnvelope[] = [];
		const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
		const baseWriter = writerFrom((envelope) => {
			appended.push(envelope.event);
			const eventId =
				envelope.event.type === "agent.tool_use"
					? "sevt_reschedule_mutation"
					: `bridge-${envelope.writeId}`;
			return { ok: true, eventId, type: "committed", eventSequence: 1 };
		});
		const writer: SessionEventWriter = {
			...baseWriter,
			settleToolResult: async (envelope) => {
				settlements.push(envelope);
				return await baseWriter.settleToolResult(envelope);
			},
			writeRequestEnd: async (envelope) => {
				requestEnds.push(envelope);
				if (envelope.reschedule !== undefined) {
					const parkedHotContext = JSON.stringify(
						session.state.contextManager.entries(),
					);
					expect(parkedHotContext).not.toContain(failedDraft);
					expect(parkedHotContext).not.toContain(failedReasoning);
				}
				return await baseWriter.writeRequestEnd(envelope);
			},
		};
		let mutatingHelperExecutions = 0;
		const layer = runtimeThreadLoopLayer(loader, {
			llmService: llm,
			writer,
			providerCallRuntime: {
				systemInstructions: "provider reschedule isolation test",
				toolCatalog: catalogForTest({
					name: "mutate_record",
					description: "Mutate a test record",
					inputSchema: { type: "object" },
				}),
			},
			runTool: () => {
				mutatingHelperExecutions += 1;
				return {
					type: "completed",
					output: { text: committedToolOutput, truncated: false },
				};
			},
		});
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(Effect.provide(layer)),
		);
		const requestTwoContext = JSON.stringify(requests[1]?.context);
		const requestThreeContext = JSON.stringify(requests[2]?.context);
		const hotContext = JSON.stringify(session.state.contextManager.entries());
		const durableAppendEvents = JSON.stringify(appended);
		const occurrenceCount = (value: string, needle: string) =>
			value.split(needle).length - 1;
		expect(result).toMatchObject({ type: "completed" });
		expect(requests).toHaveLength(3);
		expect(mutatingHelperExecutions).toBe(1);
		expect(settlements).toEqual([
			expect.objectContaining({
				settlement: {
					toolUseEventId: "sevt_reschedule_mutation",
					outcome: {
						type: "completed",
						output: { text: committedToolOutput, truncated: false },
					},
				},
			}),
		]);
		expect(occurrenceCount(requestTwoContext, committedToolOutput)).toBe(1);
		expect(occurrenceCount(requestThreeContext, committedToolOutput)).toBe(1);
		expect(requestThreeContext).not.toContain(failedReasoning);
		expect(requestThreeContext).not.toContain(failedDraft);
		expect(occurrenceCount(requestThreeContext, successfulReasoning)).toBe(0);
		expect(requestEnds).toHaveLength(3);
		expect(requestEnds[1]?.reschedule).toMatchObject({ attempt: 1 });
		expect(requestEnds[1]?.trailingContextAppend).toBeUndefined();
		expect(requestEnds[2]?.trailingContextAppend).toBeUndefined();
		expect(
			new Set(requestEnds.map((envelope) => envelope.modelRequestId)).size,
		).toBe(3);
		expect(durableAppendEvents).not.toContain(failedReasoning);
		expect(durableAppendEvents).not.toContain(failedDraft);
		expect(hotContext).not.toContain(failedReasoning);
		expect(hotContext).not.toContain(failedDraft);
		expect(occurrenceCount(hotContext, committedToolOutput)).toBe(1);
		expect(occurrenceCount(hotContext, successfulReasoning)).toBe(1);
		expect(appended.filter((event) => event.type === "session.error")).toEqual([
			expect.objectContaining({
				error: expect.objectContaining({
					retryStatus: { type: "retrying", attempt: 1 },
				}),
			}),
		]);
		expect(
			appended.filter((event) => event.type === "session.status_idle"),
		).toHaveLength(1);
	});
	test("provider failure waits for live subagent creation result and retries its pair once", async () => {
		const session = new ThreadRuntime("sesn_subagent_provider_failure");
		const loader = new QueuedContextLoader(
			[],
			[
				{
					type: "context",
					entries: [userMessage("user-spawn", 0, "spawn the worker")],
				},
			],
		);
		const requests: LLMRequest[] = [];
		const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
		const settlements: SessionEventWriterToolSettlementEnvelope[] = [];
		const childCreated = deferred<void>();
		const releaseToolResult = deferred<void>();
		const providerFailureEmitted = deferred<void>();
		let childCreations = 0;
		const providerFailure = runtimeFailureFromProviderError(
			normalizeProviderError({
				code: "provider_stream_error",
				message: "provider failed after child creation",
				retryable: true,
				fatal: false,
			}),
		);
		const baseWriter = writerFrom((envelope) => ({
			ok: true,
			eventId:
				envelope.event.type === "agent.tool_use"
					? "sevt_subagent_creation"
					: `bridge-${envelope.writeId}`,
			type: "committed",
			eventSequence: 1,
		}));
		const writer: SessionEventWriter = {
			...baseWriter,
			settleToolResult: async (envelope) => {
				settlements.push(envelope);
				return await baseWriter.settleToolResult(envelope);
			},
			writeRequestEnd: async (envelope) => {
				requestEnds.push(envelope);
				return await baseWriter.writeRequestEnd(envelope);
			},
		};
		const layer = runtimeThreadLoopLayer(loader, {
			writer,
			llmService: {
				stream: (request) => {
					requests.push(request);
					if (requests.length !== 1) {
						return Stream.fromIterable([
							{ type: "text-start" as const, id: "after-spawn" },
							{
								type: "text-delta" as const,
								id: "after-spawn",
								text_delta: "child confirmed",
							},
							{ type: "text-end" as const, id: "after-spawn" },
							{ type: "finish" as const, finishReason: "stop" as const },
						]);
					}
					return Stream.fromAsyncIterable(
						(async function* () {
							yield {
								type: "tool-call" as const,
								id: "call_spawn_live",
								toolName: "spawn_agent",
								input: {
									task_name: "worker",
									prompt: "do the work",
									agent_type: "worker",
									fork_turns: "all",
								},
								inputPreview: {
									preview: '{"task_name":"worker","prompt":"do the work","agent_type":"worker","fork_turns":"all"}',
									truncated: false,
								},
							};
							await childCreated.promise;
							providerFailureEmitted.resolve(undefined);
							yield { type: "provider-error" as const, error: providerFailure };
						})(),
						(error): LLMServiceError => ({
							type: "llm-service",
							error: runtimeFailureFromProviderError(
								normalizeProviderError({
									code: "provider_stream_error",
									message: String(error),
									retryable: true,
								}),
							),
						}),
					);
				},
			},
			providerCallRuntime: {
				systemInstructions: "live subagent provider failure test",
				toolCatalog: catalogForTest({
					name: "spawn_agent",
					description: "Spawn a worker",
					inputSchema: { type: "object" },
				}),
			},
			runTool: async () => {
				childCreations += 1;
				childCreated.resolve(undefined);
				await releaseToolResult.promise;
				return {
					type: "completed",
					output: {
						text: "task_name: worker\nstatus: delivered",
						truncated: false,
					},
				};
			},
		});
		const run = Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(Effect.provide(layer)),
		);
		await providerFailureEmitted.promise;
		await flushMicrotasks(30);
		expect(childCreations).toBe(1);
		expect(requests).toHaveLength(1);
		expect(settlements).toHaveLength(0);
		expect(requestEnds).toHaveLength(1);
		expect(requestEnds[0]?.reschedule).toMatchObject({ attempt: 1 });

		releaseToolResult.resolve(undefined);
		expect(await run).toMatchObject({ type: "completed" });
		expect(childCreations).toBe(1);
		expect(requests).toHaveLength(2);
		expect(settlements).toHaveLength(1);
		expect(requestEnds).toHaveLength(2);
		const retryContext = JSON.stringify(requests[1]?.context);
		expect(retryContext).toContain("call_spawn_live");
		expect(retryContext).toContain("status: delivered");
		expect(retryContext.split("call_spawn_live")).toHaveLength(3);
	});
	test("same-request committed tool is repaired and rebased before provider reschedule", async () => {
		const session = new ThreadRuntime("sesn_1");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "mutate then continue")],
		});
		const requests: LLMRequest[] = [];
		const toolResultCommitted = deferred<void>();
		const failure = runtimeFailureFromProviderError(
			normalizeProviderError({
				code: "provider_unavailable",
				message: "provider failed after tool commit",
				retryable: true,
				fatal: false,
			}),
		);
		const llm: LLMServiceInterface = {
			stream(request) {
				requests.push(request);
				if (requests.length === 1) {
					return Stream.fromAsyncIterable(
						(async function* () {
							yield {
								type: "reasoning-start" as const,
								id: "reasoning-same-request",
							};
							yield {
								type: "reasoning-delta" as const,
								id: "reasoning-same-request",
								text_delta: "reason before mutation",
							};
							yield {
								type: "reasoning-end" as const,
								id: "reasoning-same-request",
							};
							yield {
								type: "tool-call" as const,
								id: "tool-same-request",
								toolName: "mutate_record",
								input: { id: "one" },
								inputPreview: { preview: '{"id":"one"}', truncated: false },
							};
							await toolResultCommitted.promise;
							yield { type: "provider-error" as const, error: failure };
						})(),
						(error): LLMServiceError => ({
							type: "llm-service",
							error: runtimeFailureFromProviderError(
								normalizeProviderError({
									code: "provider_stream_error",
									message: String(error),
									retryable: true,
								}),
							),
						}),
					);
				}
				return Stream.fromIterable([
					{ type: "text-start" as const, id: "text-retry" },
					{ type: "text-delta" as const, id: "text-retry", text_delta: "done" },
					{ type: "text-end" as const, id: "text-retry" },
					{ type: "finish" as const, finishReason: "stop" as const },
				]);
			},
		};
		const appended: SessionEventEnvelope[] = [];
		const settlements: SessionEventWriterToolSettlementEnvelope[] = [];
		const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
		const writer = writerFrom(
			(envelope) => {
				appended.push(envelope);
				return {
					ok: true,
					eventId:
						envelope.event.type === "agent.tool_use"
							? "sevt_same_request_tool"
							: `bridge-${envelope.writeId}`,
					type: "committed",
					eventSequence: 1,
				};
			},
			async (envelope) => {
				requestEnds.push(envelope);
				return requestEndResultForTest(envelope);
			},
			[],
			undefined,
			async (envelope) => {
				settlements.push(envelope);
				toolResultCommitted.resolve();
				return { ok: true, result: { type: "committed" } };
			},
		);
		let helperMutations = 0;
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						llmService: llm,
						writer,
						providerCallRuntime: {
							systemInstructions: "same request provider failure",
							toolCatalog: catalogForTest({
								name: "mutate_record",
								description: "mutate",
								inputSchema: { type: "object" },
							}),
						},
						runTool: () => {
							helperMutations += 1;
							return {
								type: "completed",
								output: { text: "mutation committed", truncated: false },
							};
						},
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed" });
		expect(helperMutations).toBe(1);
		expect(requests).toHaveLength(2);
		const retryContext = JSON.stringify(requests[1]?.context);
		for (const durableValue of [
			"mutate_record",
			"mutation committed",
			"reason before mutation",
		]) {
			expect(retryContext.split(durableValue)).toHaveLength(2);
		}
		const toolAnchor = appended.find(
			(envelope) => envelope.event.type === "agent.tool_use",
		);
		expect(
			toolAnchor?.assistantContextAppend?.parts.flatMap((part) =>
				part.type === "reasoning" ? [part.text] : [],
			),
		).toEqual(["reason before mutation"]);
		expect(toolAnchor?.modelRequestId).toBe(requestEnds[0]?.modelRequestId);
		expect(
			appended.filter(
				(envelope) =>
					envelope.assistantContextAppend?.parts.some(
						(part) => part.type === "reasoning",
					) ?? false,
			),
		).toHaveLength(1);
		expect(settlements).toHaveLength(1);
		expect(requestEnds[0]?.reschedule).toMatchObject({ attempt: 1 });
		expect(requestEnds[0]?.trailingContextAppend).toBeUndefined();
	});
	test("runtime layer discards hot state when a tool route observes stale custody", async () => {
		const session = new ThreadRuntime("sesn_1");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const appended: SessionEvent[] = [];
		const settlements: SessionEventWriterToolSettlementEnvelope[] = [];
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer: writerFrom(
							(envelope) => {
								appended.push(envelope.event);
								return {
									ok: true,
									eventId:
										envelope.event.type === "agent.tool_use"
											? "bridge-tool"
											: `bridge-${envelope.writeId}`,
									type: "committed",
									eventSequence: 1,
								};
							},
							undefined,
							[],
							undefined,
							async (envelope) => {
								settlements.push(envelope);
								return { ok: true, result: { type: "committed" } };
							},
						),
						events: [
							{
								type: "tool-call",
								id: "tool-1",
								toolName: "search",
								input: { q: "x" },
								inputPreview: { preview: '{"q":"x"}', truncated: false },
							},
							{ type: "finish", finishReason: "tool-calls" },
						],
						providerCallRuntime: {
							systemInstructions: "stale custody tool test",
							toolCatalog: catalogForTest({
								name: "search",
								description: "Search test tool",
								inputSchema: { type: "object" },
							}),
						},
						runTool: () => ({ type: "stale_custody" }),
					}),
				),
			),
		);
		expect(result).toEqual({ type: "interrupted", discardHotState: true });
		expect(settlements).toEqual([]);
	});
	test("runtime layer tracks background tool state until task notification settlement", async () => {
		const session = new ThreadRuntime("sesn_1");
		const store = new ThreadLoopRuntimeStore([]);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const writer = writerFrom((envelope) => ({
			ok: true,
			eventId:
				envelope.event.type === "agent.tool_use"
					? "bridge-tool"
					: `bridge-${envelope.writeId}`,
			type: "committed",
			eventSequence: 1,
		}));
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						store,
						writer,
						events: [
							{
								type: "tool-call",
								id: "tool-1",
								toolName: "search",
								input: { q: "x" },
								inputPreview: { preview: '{"q":"x"}', truncated: false },
							},
							{ type: "finish", finishReason: "tool-calls" },
						],
						providerCallRuntime: {
							systemInstructions: "background tool state test system",
							toolCatalog: catalogForTest({
								name: "search",
								description: "Search test tool",
								inputSchema: {
									type: "object",
									properties: { q: { type: "string" } },
								},
							}),
						},
						runTool: () => ({
							type: "completed",
							output: {
								text: "status: running\nsession_id: task_1",
								truncated: false,
							},
							backgroundTask: { taskId: "task_1" },
						}),
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed" });
		const projection = durableRuntimeNotificationMessage(
			"msg_task_1",
			"task done",
			Math.max(
				...session.state.contextManager
					.entries()
					.map((entry) => entry.messageSequence),
			) + 1,
		);
		const { kind: _taskKind, ...taskCommand } = taskNotificationInput(
			"rin_task_1",
			"task_1",
			"bridge-tool",
			"completed",
			'{"task_id":"task_1","source_tool_use_event_id":"bridge-tool","status":"completed"}',
			session.sessionId,
		);
		expect(
			session.state.commitTaskNotification({
				...taskCommand,
				committedEntry: projection,
			}),
		).toBe("applied");
		expect(session.state.contextManager.entries().at(-1)).toEqual(projection);
	});
	test("served request consumes its exact mixed-origin ride and preserves attachments appended in flight", async () => {
		const session = new ThreadRuntime("sesn_1");
		const requestEndEnvelopes: SessionEventWriterRequestEndEnvelope[] = [];
		const requestStartEnvelopes: SessionEventEnvelope[] = [];
		const transientAttachment = {
			transient: {
				attachmentRef: "att_1",
				sourcePath: "mcp:github/plot.png",
				pageRange: "",
				detail: "auto",
			},
			fileBacked: undefined,
			mime: "image/png",
			filename: "plot.png",
		} as const;
		const fileAttachment = {
			transient: undefined,
			fileBacked: {
				sourceEventId: "sevt_user_file",
				fileId: "file_1",
			},
			mime: "application/pdf",
			filename: "brief.pdf",
		} as const;
		const lateAttachment = {
			transient: {
				attachmentRef: "att_late",
				sourcePath: "mcp:github/late.png",
				pageRange: "",
				detail: "auto",
			},
			fileBacked: undefined,
			mime: "image/png",
			filename: "late.png",
		} as const;
		const initialRide = [
			transientAttachment,
			fileAttachment,
			...Array.from({ length: 30 }, (_, index) => ({
				transient: {
					attachmentRef: `att_fill_${index}`,
					sourcePath: `mcp:github/fill-${index}.png`,
					pageRange: "",
					detail: "auto" as const,
				},
				fileBacked: undefined,
				mime: "image/png",
				filename: `fill-${index}.png`,
			})),
		];
		session.state.addPendingAttachments(initialRide);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const capturedRequests: LLMRequest[] = [];
		const llm: LLMServiceInterface = {
			stream(request) {
				capturedRequests.push(request);
				if (capturedRequests.length === 1) {
					session.state.addPendingAttachments([lateAttachment]);
				}
				return Stream.fromIterable([
					{ type: "text-start", id: "text-1" },
					{ type: "text-delta", id: "text-1", text_delta: "done" },
					{ type: "text-end", id: "text-1" },
					{ type: "finish", finishReason: "stop" },
				]);
			},
		};
		const baseWriter = writerFrom((envelope) => {
			if (envelope.event.type === "span.model_request_start") {
				requestStartEnvelopes.push(envelope);
			}
			return {
				ok: true,
				eventId: `bridge-${envelope.writeId}`,
				type: "committed",
				eventSequence: 1,
			};
		});
		const writer: SessionEventWriter = {
			...baseWriter,
			writeRequestEnd: async (envelope) => {
				requestEndEnvelopes.push(envelope);
				return await baseWriter.writeRequestEnd(envelope);
			},
		};
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						llmService: llm,
						writer,
						providerCallRuntime: {
							systemInstructions: "tool test system",
							toolCatalog: catalogForTest({
								name: "search",
								description: "Search test tool",
								inputSchema: {
									type: "object",
									properties: { q: { type: "string" } },
								},
							}),
						},
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed" });
		expect(capturedRequests).toHaveLength(2);
		expect(capturedRequests[0]?.attachments).toEqual(
			providerAttachmentsForTest(initialRide),
		);
		expect(capturedRequests[1]?.attachments).toEqual(
			providerAttachmentsForTest([lateAttachment]),
		);
		expect(requestEndEnvelopes).toHaveLength(2);
		expect(requestEndEnvelopes[0]?.consumedAttachmentRefs).toEqual([
			"att_1",
			...Array.from({ length: 30 }, (_, index) => `att_fill_${index}`),
		]);
		expect(requestStartEnvelopes[0]?.consumedFileAttachments).toEqual([
			{
				sourceEventId: "sevt_user_file",
				fileId: "file_1",
			},
		]);
		expect(requestEndEnvelopes[1]?.consumedAttachmentRefs).toEqual(["att_late"]);
		expect(requestStartEnvelopes[1]?.consumedFileAttachments ?? []).toEqual([]);
		expect(session.state.pendingAttachments()).toEqual([]);
	});
	test("runtime layer caps pending attachments", async () => {
		const session = new ThreadRuntime("sesn_1");
		const attachments = Array.from({ length: 35 }, (_, index) => ({
			transient: {
				attachmentRef: `att_${index + 1}`,
				sourcePath: `mcp:test/plot-${index + 1}.png`,
				pageRange: "",
				detail: "auto" as const,
			},
			fileBacked: undefined,
			mime: "image/png",
			filename: `plot-${index + 1}.png`,
		}));
		session.state.addPendingAttachments(attachments);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "summarize")],
		});
		const capturedRequests: LLMRequest[] = [];
		const llm: LLMServiceInterface = {
			stream(request) {
				capturedRequests.push(request);
				return Stream.fromIterable([
					{ type: "text-start", id: "text-1" },
					{ type: "text-delta", id: "text-1", text_delta: "done" },
					{ type: "text-end", id: "text-1" },
					{ type: "finish", finishReason: "stop" },
				]);
			},
		};
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(runtimeThreadLoopLayer(loader, { llmService: llm })),
			),
		);
		expect(result).toMatchObject({ type: "completed" });
		expect(capturedRequests[0]?.attachments).toEqual(
			providerAttachmentsForTest(attachments.slice(0, 32)),
		);
		expect(JSON.stringify(capturedRequests[0]?.context)).not.toContain(
			"plot-35.png",
		);
	});
	test("runtime layer keeps pending attachments when request-end ACK fails before consumption", async () => {
		const session = new ThreadRuntime("sesn_1");
		const requestEndEnvelopes: SessionEventWriterRequestEndEnvelope[] = [];
		const attachment = {
			transient: {
				attachmentRef: "att_1",
				sourcePath: "mcp:github/plot.png",
				pageRange: "",
				detail: "auto",
			},
			fileBacked: undefined,
			mime: "image/png",
			filename: "plot.png",
		} as const;
		session.state.addPendingAttachments([attachment]);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const llm: LLMServiceInterface = {
			stream() {
				return Stream.fromIterable([
					{ type: "text-start", id: "text-1" },
					{ type: "text-delta", id: "text-1", text_delta: "done" },
					{ type: "text-end", id: "text-1" },
					{ type: "finish", finishReason: "stop" },
				]);
			},
		};
		const baseWriter = writerFrom((envelope) => ({
			ok: true,
			eventId: `bridge-${envelope.writeId}`,
			type: "committed",
			eventSequence: 1,
		}));
		const writer: SessionEventWriter = {
			...baseWriter,
			writeRequestEnd: async (envelope) => {
				requestEndEnvelopes.push(envelope);
				return {
					ok: false,
					error: {
						type: "session-event-writer",
						code: "unavailable",
						message: "request end unavailable",
						retryable: true,
						fatal: false,
						sessionId: envelope.sessionId,
					},
				};
			},
		};
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						llmService: llm,
						writer,
						providerCallRuntime: {
							systemInstructions: "tool test system",
							toolCatalog: catalogForTest({
								name: "search",
								description: "Search test tool",
								inputSchema: {
									type: "object",
									properties: { q: { type: "string" } },
								},
							}),
						},
					}),
				),
			),
		);
		expect(result).toMatchObject({
			type: "failed",
			error: { code: "unavailable", sessionId: "sesn_1" },
			releaseSession: { reason: "event_write_failed" },
		});
		expect(requestEndEnvelopes).toHaveLength(
			SessionEventWriterRetryPolicy.attempts,
		);
		expect(
			requestEndEnvelopes.every(
				(envelope) => envelope === requestEndEnvelopes[0],
			),
		).toBe(true);
		expect(
			requestEndEnvelopes.every(
				(envelope) => envelope.consumedAttachmentRefs?.[0] === "att_1",
			),
		).toBe(true);
		expect(session.state.pendingAttachments()).toEqual([attachment]);
	});
	test("request-end failure cancels and durably settles an acknowledged live tool", async () => {
		const session = new ThreadRuntime("sesn_1");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "write it")],
		});
		const appended: SessionEvent[] = [];
		const settlements: SessionEventWriterToolSettlementEnvelope[] = [];
		const toolStarted = deferred<void>();
		let toolSignal: AbortSignal | undefined;
		const baseWriter = writerFrom((envelope) => {
			appended.push(envelope.event);
			return {
				ok: true,
				eventId:
					envelope.event.type === "agent.tool_use"
						? "sevt_live_tool"
						: `bridge-${envelope.writeId}`,
				type: "committed",
				eventSequence: 1,
			};
		});
		const writer: SessionEventWriter = {
			...baseWriter,
			settleToolResult: async (envelope) => {
				settlements.push(envelope);
				return await baseWriter.settleToolResult(envelope);
			},
			writeRequestEnd: async (envelope) => ({
				ok: false,
				error: {
					type: "session-event-writer",
					code: "unavailable",
					message: "request end unavailable",
					retryable: true,
					fatal: false,
					sessionId: envelope.sessionId,
				},
			}),
		};
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						llmService: {
							stream() {
								return Stream.fromAsyncIterable(
									(async function* () {
										yield {
											type: "tool-call" as const,
											id: "tool-live",
											toolName: "Write",
											input: { file_path: "src/a.ts", content: "ok" },
											inputPreview: {
												preview: '{"file_path":"src/a.ts"}',
												truncated: false,
											},
										};
										await toolStarted.promise;
										yield {
											type: "finish" as const,
											finishReason: "tool-calls" as const,
										};
									})(),
									(error): LLMServiceError => ({
										type: "llm-service",
										error: runtimeFailureFromProviderError(
											normalizeProviderError({
												code: "provider_stream_error",
												message: String(error),
												retryable: true,
											}),
										),
									}),
								);
							},
						},
						providerCallRuntime: {
							systemInstructions: "request end failure tool closeout test",
							toolCatalog: catalogForTest({
								name: "Write",
								description: "Write file",
								inputSchema: { type: "object" },
							}),
						},
						runTool: (request) => {
							toolSignal = request.abortSignal;
							toolStarted.resolve(undefined);
							return new Promise((resolve) => {
								request.abortSignal.addEventListener(
									"abort",
									() => resolve({ type: "cancelled" }),
									{ once: true },
								);
							});
						},
					}),
				),
			),
		);
		expect(result).toMatchObject({
			type: "failed",
			error: { type: "session-event-writer", code: "unavailable" },
			releaseSession: { reason: "event_write_failed" },
		});
		expect(toolSignal?.aborted).toBe(true);
		expect(
			appended.filter((event) => event.type === "agent.tool_use"),
		).toHaveLength(1);
		expect(settlements).toEqual([
			expect.objectContaining({
				settlement: expect.objectContaining({
					toolUseEventId: "sevt_live_tool",
					outcome: expect.objectContaining({ type: "cancelled" }),
				}),
			}),
		]);
	});
	test("request end waits for an in-flight Tool Result declaration ACK", async () => {
		const session = new ThreadRuntime("sesn_request_end_tool_result_order");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "read it")],
		});
		const toolStarted = deferred<void>();
		const releaseTool = deferred<void>();
		const resultAppendArrived = deferred<void>();
		const releaseResultAppend = deferred<void>();
		const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
		let providerRequests = 0;
		const baseWriter = writerFrom((envelope) => ({
			ok: true,
			eventId:
				envelope.event.type === "agent.tool_use"
					? "sevt_ordered_tool"
					: `bridge-${envelope.writeId}`,
			type: "committed",
			eventSequence: 1,
		}));
		const writer: SessionEventWriter = {
			...baseWriter,
			settleToolResult: async (envelope) => {
				resultAppendArrived.resolve(undefined);
				await releaseResultAppend.promise;
				return await baseWriter.settleToolResult(envelope);
			},
			writeRequestEnd: async (envelope) => {
				requestEnds.push(envelope);
				return await baseWriter.writeRequestEnd(envelope);
			},
		};
		const runPromise = Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						llmService: {
							stream() {
								providerRequests += 1;
								if (providerRequests > 1) {
									return Stream.fromIterable([
										{ type: "text-start" as const, id: "text-final" },
										{
											type: "text-delta" as const,
											id: "text-final",
											text_delta: "done",
										},
										{ type: "text-end" as const, id: "text-final" },
										{ type: "finish" as const, finishReason: "stop" as const },
									]);
								}
								return Stream.fromAsyncIterable(
									(async function* () {
										yield {
											type: "tool-call" as const,
											id: "tool-live",
											toolName: "Read",
											input: { file_path: "src/a.ts" },
											inputPreview: {
												preview: '{"file_path":"src/a.ts"}',
												truncated: false,
											},
										};
										await toolStarted.promise;
										releaseTool.resolve(undefined);
										await resultAppendArrived.promise;
										yield {
											type: "finish" as const,
											finishReason: "tool-calls" as const,
										};
									})(),
									(error): LLMServiceError => ({
										type: "llm-service",
										error: runtimeFailureFromProviderError(
											normalizeProviderError({
												code: "provider_stream_error",
												message: String(error),
												retryable: true,
											}),
										),
									}),
								);
							},
						},
						providerCallRuntime: {
							systemInstructions: "request end projection ordering test",
							toolCatalog: catalogForTest({
								name: "Read",
								description: "Read file",
								inputSchema: { type: "object" },
							}),
						},
						runTool: async () => {
							toolStarted.resolve(undefined);
							await releaseTool.promise;
							return {
								type: "completed",
								output: { text: "done", truncated: false },
							};
						},
					}),
				),
			),
		);
		await resultAppendArrived.promise;
		await flushMicrotasks(50);
		expect(requestEnds).toHaveLength(1);
		expect(providerRequests).toBe(1);
		releaseResultAppend.resolve(undefined);
		expect(await runPromise).toMatchObject({ type: "completed" });
		expect(requestEnds).toHaveLength(2);
	});
	test("attachment rejections survive reschedule and settle in the cumulative origin union", async () => {
		const session = new ThreadRuntime("sesn_1");
		const transientAttachment = {
			transient: {
				attachmentRef: "att_1",
				sourcePath: "mcp:github/plot.png",
				pageRange: "",
				detail: "auto",
			},
			fileBacked: undefined,
			mime: "image/png",
			filename: "plot.png",
		} as const;
		const fileAttachment = {
			transient: undefined,
			fileBacked: {
				sourceEventId: "sevt_file_message_1",
				fileId: "file_1",
			},
			mime: "image/png",
			filename: "deleted-plot.png",
		} as const;
		session.state.addPendingAttachments([transientAttachment, fileAttachment]);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "summarize this plot")],
		});
		const capturedRequests: LLMRequest[] = [];
		const llm: LLMServiceInterface = {
			stream(request) {
				capturedRequests.push(request);
				if (capturedRequests.length === 1) {
					return Stream.fromIterable([
						{
							type: "attachment-rejections" as const,
							rejections: [
								{
									origin: {
										type: "file-backed" as const,
										sourceEventId: "sevt_file_message_1",
										fileId: "file_1",
									},
									reason: "deleted" as const,
								},
							],
						},
						{
							type: "provider-error" as const,
							error: runtimeFailureFromProviderError(
								normalizeProviderError({
									code: "provider_unavailable",
									message: "Provider is temporarily unavailable.",
									retryable: true,
									fatal: false,
									statusCode: 503,
								}),
							),
						},
					]);
				}
				return Stream.fromIterable([
					{ type: "text-start", id: "text-1" },
					{
						type: "text-delta",
						id: "text-1",
						text_delta: "I will continue without the plot.",
					},
					{ type: "text-end", id: "text-1" },
					{ type: "finish", finishReason: "stop" },
				]);
			},
		};
		const appendedEvents: SessionEvent[] = [];
		const requestStartEnvelopes: SessionEventEnvelope[] = [];
		const requestEndEnvelopes: SessionEventWriterRequestEndEnvelope[] = [];
		const baseWriter = writerFrom((envelope) => {
			appendedEvents.push(envelope.event);
			if (envelope.event.type === "span.model_request_start") {
				requestStartEnvelopes.push(envelope);
			}
			return {
				ok: true,
				eventId: `bridge-${envelope.writeId}`,
				type: "committed",
				eventSequence: 1,
			};
		});
		const writer: SessionEventWriter = {
			...baseWriter,
			writeRequestEnd: async (envelope) => {
				requestEndEnvelopes.push(envelope);
				return await baseWriter.writeRequestEnd(envelope);
			},
		};
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						llmService: llm,
						writer,
						providerCallRuntime: {
							systemInstructions: "tool test system",
							toolCatalog: catalogForTest({
								name: "search",
								description: "Search test tool",
								inputSchema: {
									type: "object",
									properties: { q: { type: "string" } },
								},
							}),
						},
						runtimePolicy: () => ({
							providerRescheduleBudget: 3,
							compactionRescheduleBudget: 2,
						}),
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed" });
		expect(capturedRequests).toHaveLength(2);
		expect(capturedRequests[0]?.attachments).toEqual(
			providerAttachmentsForTest([transientAttachment, fileAttachment]),
		);
		expect(capturedRequests[1]?.attachments).toEqual(
			providerAttachmentsForTest([transientAttachment]),
		);
		expect(requestEndEnvelopes).toHaveLength(2);
		expect(requestEndEnvelopes[0]).toMatchObject({
			isError: true,
			errorKind: "provider_error",
			finishReason: "error",
			reschedule: { attempt: 1 },
		});
		expect(requestEndEnvelopes[0]?.consumedAttachmentRefs ?? []).toEqual([]);
		expect(requestEndEnvelopes[1]?.consumedAttachmentRefs).toEqual(["att_1"]);
		expect(requestStartEnvelopes[0]?.consumedFileAttachments).toEqual([
			{
				sourceEventId: "sevt_file_message_1",
				fileId: "file_1",
			},
		]);
		expect(requestStartEnvelopes[1]?.consumedFileAttachments ?? []).toEqual([]);
		expect(
			appendedEvents.filter((event) => event.type === "session.error"),
		).toEqual([
			expect.objectContaining({
				error: expect.objectContaining({
					retryStatus: { type: "retrying", attempt: 1 },
				}),
			}),
		]);
		const retryErrorIndex = appendedEvents.findIndex(
			(event) => event.type === "session.error",
		);
		const secondRequestStartIndex = appendedEvents.findLastIndex(
			(event) => event.type === "span.model_request_start",
		);
		expect(retryErrorIndex).toBeGreaterThanOrEqual(0);
		expect(retryErrorIndex).toBeLessThan(secondRequestStartIndex);
		expect(
			appendedEvents
				.slice(0, secondRequestStartIndex)
				.map((event) => event.type),
		).not.toContain("session.status_idle");
		expect(session.state.pendingAttachments()).toEqual([]);
	});
	test("runtime layer commits valid tool errors to hot context after error result ACK", async () => {
		const order: string[] = [];
		const session = new ThreadRuntime("sesn_1");
		const store = new ThreadLoopRuntimeStore(order);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const writer = writerFrom(
			(envelope) => {
				order.push(
					`event:${envelope.event.type}:tool_${contextToolStatus(session) ?? "missing"}:progress`,
				);
				return {
					ok: true,
					eventId:
						envelope.event.type === "agent.tool_use"
							? "bridge-tool"
							: `bridge-${envelope.writeId}`,
					type: "committed",
					eventSequence: 1,
				};
			},
			undefined,
			[],
			undefined,
			async (envelope) => {
				order.push(
					`settlement:${envelope.settlement.outcome.type}:tool_${contextToolStatus(session) ?? "missing"}`,
				);
				return { ok: true, result: { type: "committed" } };
			},
		);
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						store,
						writer,
						events: [
							{
								type: "tool-call",
								id: "tool-1",
								toolName: "search",
								input: { q: "x" },
								inputPreview: { preview: '{"q":"x"}', truncated: false },
							},
							{ type: "finish", finishReason: "tool-calls" },
						],
						providerCallRuntime: {
							systemInstructions: "tool test system",
							toolCatalog: catalogForTest({
								name: "search",
								description: "Search test tool",
								inputSchema: {
									type: "object",
									properties: { q: { type: "string" } },
								},
							}),
						},
						runTool: () => ({
							type: "error",
							error: {
								type: "provider",
								code: "provider_tool_protocol_error",
								message: "tool failed",
								retryable: false,
								fatal: true,
								providerId: "fake",
								modelId: "fake-chat",
							},
						}),
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed" });
		expect(order).toEqual([
			"event:session.status_running:tool_missing:progress",
			"event:span.model_request_start:tool_missing:progress",
			"event:agent.tool_use:tool_missing:progress",
			"event:span.model_request_end:tool_running:progress",
			"settlement:error:tool_running",
			"event:span.model_request_start:tool_error:progress",
			"event:agent.message:tool_error:progress",
			"event:span.model_request_end:tool_error:progress",
			"event:session.status_idle:tool_error:progress",
		]);
	});
	test("a lost continuation Request Start acknowledgement retries one identity without duplicating its predecessor", async () => {
		const session = new ThreadRuntime("sesn_tool_continuation");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "inspect it")],
		});
		const requests: LLMRequest[] = [];
		const appended: SessionEvent[] = [];
		const continuationStartWriteIds: string[] = [];
		let requestStartAttempts = 0;
		const writer = writerFrom((envelope) => {
			appended.push(envelope.event);
			if (envelope.event.type === "span.model_request_start") {
				requestStartAttempts += 1;
				if (requestStartAttempts >= 2) {
					continuationStartWriteIds.push(envelope.writeId);
				}
				if (requestStartAttempts === 2) {
					return {
						ok: false,
						error: normalizeSessionEventWriterError({
							code: "timeout",
							sessionId: envelope.sessionId,
						}),
					};
				}
			}
			return {
				ok: true,
				eventId: `bridge-${envelope.writeId}`,
				type: "committed",
				eventSequence: 1,
			};
		});
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						llmService: queuedLLMService(
							[
								[
									{
										type: "tool-call",
										id: "tool-read",
										toolName: "Read",
										input: { file_path: "src/a.ts" },
										inputPreview: {
											preview: '{"file_path":"src/a.ts"}',
											truncated: false,
										},
									},
									{ type: "finish", finishReason: "tool-calls" },
								],
								[
									{
										type: "tool-call",
										id: "tool-read-second",
										toolName: "Read",
										input: { file_path: "src/b.ts" },
										inputPreview: {
											preview: '{"file_path":"src/b.ts"}',
											truncated: false,
										},
									},
									{ type: "finish", finishReason: "tool-calls" },
								],
								[
									{ type: "text-start", id: "text-final" },
									{ type: "text-delta", id: "text-final", text_delta: "done" },
									{ type: "text-end", id: "text-final" },
									{ type: "finish", finishReason: "stop" },
								],
							],
							requests,
						),
						providerCallRuntime: {
							systemInstructions: "tool continuation test",
							toolCatalog: catalogForTest({
								name: "Read",
								description: "Read file",
								inputSchema: { type: "object" },
							}),
						},
						runTool: (request) => ({
							type: "completed",
							output: {
								text: `file contents ${request.modelToolCallId}`,
								truncated: false,
							},
						}),
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed" });
		expect(requests).toHaveLength(3);
		expect(continuationStartWriteIds).toHaveLength(3);
		expect(continuationStartWriteIds[0]).toBe(continuationStartWriteIds[1]);
		expect(
			requests.filter((request) =>
				JSON.stringify(request.context).includes("file contents tool-read"),
			),
		).toHaveLength(2);
		expect(JSON.stringify(requests[1]?.context)).toContain(
			"file contents tool-read",
		);
		expect(JSON.stringify(requests[2]?.context)).toContain(
			"file contents tool-read-second",
		);
		expect(session.state.contextManager.entries().at(-1)?.parts).toEqual([
			{ type: "text", text: "done" },
		]);
		expect(
			appended.filter((event) => event.type === "session.status_idle"),
		).toHaveLength(1);
	});
	test("the continuation request combines terminal Tool Results with user input and Agent Mail from one finite cut", async () => {
		const session = new ThreadRuntime("sesn_1");
		session.state.enqueueAcceptedInput(
			acceptedInput("rin_mixed_initial", session.sessionId),
		);
		const userFollowUp = acceptedInput("rin_mixed_user", session.sessionId);
		const mailMessage = runtimeNotificationMessage(
			"msg_mixed_agent_mail",
			"Message Type: FINAL_ANSWER\nTask name: main\nSender: worker\nPayload:\nmail result",
		);
		const agentMail = {
			workspaceId: session.identity.workspaceId,
			sessionId: session.sessionId,
			sessionThreadId: session.identity.sessionThreadId,
			bindingId: session.identity.bindingId,
			bindingGeneration: session.identity.bindingGeneration,
			targetPodUid: session.identity.targetPodUid,
			runtimeInputId: "agent_mail:delivery_mixed_agent_mail",
			kind: "inter_agent_message",
			deliveryId: "delivery_mixed_agent_mail",
			content: mailMessage.parts
				.flatMap((part) => (part.type === "text" ? [part.text] : []))
				.join(""),
		} satisfies RuntimeAcceptedInputState;
		const loader = new QueuedContextLoader([], []);
		const requests: LLMRequest[] = [];
		let enqueuedFollowUp = false;
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						installLoaderState: false,
						llmService: queuedLLMService(
							[
								[
									{
										type: "tool-call",
										id: "tool-mixed",
										toolName: "Read",
										input: { file_path: "src/mixed.ts" },
										inputPreview: {
											preview: '{"file_path":"src/mixed.ts"}',
											truncated: false,
										},
									},
									{ type: "finish", finishReason: "tool-calls" },
								],
								[
									{ type: "text-start", id: "text-mixed-final" },
									{
										type: "text-delta",
										id: "text-mixed-final",
										text_delta: "combined",
									},
									{ type: "text-end", id: "text-mixed-final" },
									{ type: "finish", finishReason: "stop" },
								],
							],
							requests,
						),
						providerCallRuntime: {
							systemInstructions: "mixed continuation test",
							toolCatalog: catalogForTest({
								name: "Read",
								description: "Read file",
								inputSchema: { type: "object" },
							}),
						},
						runTool: () => {
							if (!enqueuedFollowUp) {
								enqueuedFollowUp = true;
								session.state.enqueueAcceptedInput(userFollowUp);
								session.state.enqueueAcceptedInput(agentMail);
							}
							return {
								type: "completed",
								output: { text: "mixed tool result", truncated: false },
							};
						},
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed" });
		expect(loader.commitCalls.map((input) => input.runtimeInputId)).toEqual([
			"rin_mixed_initial",
			"rin_mixed_user",
			"agent_mail:delivery_mixed_agent_mail",
		]);
		expect(requests).toHaveLength(2);
		const continuation = JSON.stringify(requests[1]?.context);
		expect(continuation).toContain("mixed tool result");
		expect(continuation).toContain("test input");
		expect(continuation).toContain("mail result");
	});
	test("absent cross-family builtins take the durable internal invalid-tool repair path in both directions", async () => {
		for (const tc of [
			{ family: "claude" as const, absentTool: "exec_command" },
			{ family: "gpt" as const, absentTool: "Bash" },
		]) {
			const session = new ThreadRuntime("sesn_" + tc.family);
			const order: string[] = [];
			const store = new ThreadLoopRuntimeStore(order);
			const publicToolEvents: string[] = [];
			let runToolCalls = 0;
			const loader = new RecordingContextLoader([], {
				type: "context",
				entries: [userMessage("user-" + tc.family, 0, "hello")],
			});
			const writer = writerFrom((envelope) => {
				if (envelope.event.type === "agent.tool_use") {
					publicToolEvents.push(envelope.event.type);
				}
				return {
					ok: true,
					eventId: `bridge-${envelope.writeId}`,
					type: "committed",
					eventSequence: 1,
				};
			});
			const result = await Effect.runPromise(
				Effect.gen(function* () {
					const threadLoop = yield* ThreadLoop.Service;
					return yield* threadLoop.run(session, testRunCustody());
				}).pipe(
					Effect.provide(
						runtimeThreadLoopLayer(loader, {
							store,
							writer,
							llmService: queuedLLMService([
								[
									{
										type: "tool-call",
										id: "tool-other-family",
										toolName: tc.absentTool,
										input: {},
										inputPreview: { preview: "{}", truncated: false },
									},
									{ type: "finish", finishReason: "tool-calls" },
								],
								[
									{ type: "text-start", id: "text-repaired" },
									{
										type: "text-delta",
										id: "text-repaired",
										text_delta: "repaired",
									},
									{ type: "text-end", id: "text-repaired" },
									{ type: "finish", finishReason: "stop" },
								],
							]),
							providerCallRuntime: {
								systemInstructions: "cross-family repair test",
							},
							runtimePolicy: () => ({
								toolCatalog: createToolCatalog({ family: tc.family }),
							}),
							runTool: () => {
								runToolCalls += 1;
								return {
									type: "completed",
									output: { text: "must not run", truncated: false },
								};
							},
						}),
					),
				),
			);
			expect(result).toMatchObject({ type: "completed" });
			expect(runToolCalls).toBe(0);
			expect(publicToolEvents).toEqual([]);
			expect(store.repairs).toHaveLength(1);
			expect(store.repairs[0]?.toolName).toBe(tc.absentTool);
			expect(store.repairs[0]).toMatchObject({
				canonicalInput: {},
				error: {
					type: "runtime_invalid_sequence",
					message: `disabled or unknown tool call: ${tc.absentTool}`,
					retryable: false,
				},
			});
			expect(order).toContain("store:internal-tool-repair");
		}
	});
	test("mixed internal repair and public Tool Use waits for the public terminal receipt before continuing", async () => {
		const session = new ThreadRuntime("sesn_mixed_repair_public");
		const store = new ThreadLoopRuntimeStore([]);
		const requests: LLMRequest[] = [];
		const publicToolStarted = deferred<void>();
		const releasePublicTool = deferred<void>();
		let publicToolCalls = 0;
		const run = Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(
						new RecordingContextLoader([], {
							type: "context",
							entries: [userMessage("user-mixed-repair", 0, "run both tools")],
						}),
						{
							store,
							llmService: queuedLLMService(
								[
									[
										{
											type: "tool-call",
											id: "call-invalid-cross-family",
											toolName: "exec_command",
											input: {},
											inputPreview: { preview: "{}", truncated: false },
										},
										{
											type: "tool-call",
											id: "call-public-read",
											toolName: "Read",
											input: { file_path: "README.md" },
											inputPreview: {
												preview: '{"file_path":"README.md"}',
												truncated: false,
											},
										},
										{ type: "finish", finishReason: "tool-calls" },
									],
									[
										{ type: "text-start", id: "text-after-mixed" },
										{
											type: "text-delta",
											id: "text-after-mixed",
											text_delta: "continued after both",
										},
										{ type: "text-end", id: "text-after-mixed" },
										{ type: "finish", finishReason: "stop" },
									],
								],
								requests,
							),
							providerCallRuntime: {
								systemInstructions:
									"mixed internal repair and public tool test",
							},
							runtimePolicy: () => ({
								toolCatalog: createToolCatalog({ family: "claude" }),
							}),
							runTool: async () => {
								publicToolCalls += 1;
								publicToolStarted.resolve();
								await releasePublicTool.promise;
								return {
									type: "completed",
									output: { text: "public tool result", truncated: false },
								};
							},
						},
					),
				),
			),
		);

		await publicToolStarted.promise;
		await flushMicrotasks();
		expect(store.repairs).toHaveLength(1);
		expect(requests).toHaveLength(1);
		releasePublicTool.resolve();

		expect(await run).toMatchObject({ type: "completed" });
		expect(publicToolCalls).toBe(1);
		expect(requests).toHaveLength(2);
		const continuation = JSON.stringify(requests[1]?.context);
		expect(continuation).toContain("exec_command");
		expect(continuation).toContain("public tool result");
	});
	test("runtime layer schedules same-target tool calls through ToolScheduler", async () => {
		const session = new ThreadRuntime("sesn_1");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const firstRelease = deferred<void>();
		const calls: string[] = [];
		let active = 0;
		let maxActive = 0;
		const runPromise = Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						events: [
							{
								type: "tool-call",
								id: "tool-1",
								toolName: "Write",
								input: { file_path: "src/a.ts", content: "one" },
								inputPreview: {
									preview: '{"file_path":"src/a.ts"}',
									truncated: false,
								},
							},
							{
								type: "tool-call",
								id: "tool-2",
								toolName: "Write",
								input: { file_path: "/workspace/src/a.ts", content: "two" },
								inputPreview: {
									preview: '{"file_path":"/workspace/src/a.ts"}',
									truncated: false,
								},
							},
							{ type: "finish", finishReason: "tool-calls" },
						],
						providerCallRuntime: {
							systemInstructions: "tool scheduler test system",
							toolCatalog: catalogForTest({
								name: "Write",
								description: "Write file",
								inputSchema: { type: "object" },
							}),
						},
						runTool: async (request) => {
							calls.push(request.modelToolCallId);
							active += 1;
							maxActive = Math.max(maxActive, active);
							if (request.modelToolCallId === "tool-1") {
								await firstRelease.promise;
							}
							active -= 1;
							return {
								type: "completed",
								output: {
									text: `done ${request.modelToolCallId}`,
									truncated: false,
								},
							};
						},
					}),
				),
			),
		);
		await waitForCondition(
			() => calls.length === 1,
			"first same-target tool start",
		);
		expect(calls).toEqual(["tool-1"]);
		firstRelease.resolve(undefined);
		const result = await runPromise;
		expect(result).toMatchObject({ type: "completed" });
		expect(calls).toEqual(["tool-1", "tool-2"]);
		expect(maxActive).toBe(1);
	});
	test("four mixed-policy tools continue once only after every terminal receipt", async () => {
		const session = new ThreadRuntime("sesn_mixed_tool_continuation");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const releaseFirstWave = deferred<void>();
		const releaseSecondWrite = deferred<void>();
		const starts: string[] = [];
		let providerCalls = 0;
		const readCatalog = catalogForTest({
			name: "Read",
			description: "Read file",
			inputSchema: { type: "object" },
		});
		const writeCatalog = catalogForTest({
			name: "Write",
			description: "Write file",
			inputSchema: { type: "object" },
		});
		const toolCatalog: ToolCatalog = {
			entries: [...readCatalog.entries, ...writeCatalog.entries],
			configs: [...readCatalog.configs, ...writeCatalog.configs],
		};
		const runPromise = Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						llmService: {
							stream() {
								providerCalls += 1;
								if (providerCalls === 2) {
									return Stream.fromIterable([
										{ type: "text-start" as const, id: "final" },
										{
											type: "text-delta" as const,
											id: "final",
											text_delta: "done",
										},
										{ type: "text-end" as const, id: "final" },
										{ type: "finish" as const, finishReason: "stop" as const },
									]);
								}
								return Stream.fromIterable([
									{
										type: "tool-call" as const,
										id: "write-1",
										toolName: "Write",
										input: { file_path: "same.txt", content: "one" },
										inputPreview: { preview: "{}", truncated: false },
									},
									{
										type: "tool-call" as const,
										id: "read-1",
										toolName: "Read",
										input: { file_path: "one.txt" },
										inputPreview: { preview: "{}", truncated: false },
									},
									{
										type: "tool-call" as const,
										id: "write-2",
										toolName: "Write",
										input: { file_path: "same.txt", content: "two" },
										inputPreview: { preview: "{}", truncated: false },
									},
									{
										type: "tool-call" as const,
										id: "read-2",
										toolName: "Read",
										input: { file_path: "two.txt" },
										inputPreview: { preview: "{}", truncated: false },
									},
									{
										type: "finish" as const,
										finishReason: "tool-calls" as const,
									},
								]);
							},
						},
						providerCallRuntime: {
							systemInstructions: "mixed scheduler continuation",
							toolCatalog,
						},
						runTool: async (request) => {
							starts.push(request.modelToolCallId);
							if (request.modelToolCallId === "write-2") {
								await releaseSecondWrite.promise;
							} else {
								await releaseFirstWave.promise;
							}
							return {
								type: "completed",
								output: {
									text: `done ${request.modelToolCallId}`,
									truncated: false,
								},
							};
						},
					}),
				),
			),
		);

		await waitForCondition(
			() => starts.length === 3,
			"first mixed-policy wave",
		);
		expect(starts).toEqual(["write-1", "read-1", "read-2"]);
		expect(providerCalls).toBe(1);
		releaseFirstWave.resolve(undefined);
		await waitForCondition(
			() => starts.includes("write-2"),
			"conflicting write after first write",
		);
		expect(providerCalls).toBe(1);
		releaseSecondWrite.resolve(undefined);
		expect(await runPromise).toMatchObject({ type: "completed" });
		expect(providerCalls).toBe(2);
	});
	test("serializes one shared-message declaration stream while four safe tools execute independently", async () => {
		const session = new ThreadRuntime("sesn_tool_declaration_order");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const firstDeclarationArrived = deferred<void>();
		const releaseFirstDeclaration = deferred<void>();
		const releaseExecutions = deferred<void>();
		const allExecutionsStarted = deferred<void>();
		const declarations: SessionEventEnvelope[] = [];
		const settlements: SessionEventWriterToolSettlementEnvelope[] = [];
		const executions: string[] = [];
		let activeExecutions = 0;
		let maxActiveExecutions = 0;
		const baseWriter = writerFrom((envelope) => ({
			ok: true,
			eventId: `bridge-${envelope.writeId}`,
			type: "committed",
			eventSequence: 1,
		}));
		const writer: SessionEventWriter = {
			...baseWriter,
			append: async (envelope) => {
				if (envelope.event.type === "agent.tool_use") {
					declarations.push(envelope);
					if (declarations.length === 1) {
						firstDeclarationArrived.resolve(undefined);
						await releaseFirstDeclaration.promise;
					}
				}
				return await baseWriter.append(envelope);
			},
			settleToolResult: async (envelope) => {
				settlements.push(envelope);
				return await baseWriter.settleToolResult(envelope);
			},
		};
		const runPromise = Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						events: [
							{ type: "reasoning-start", id: "reasoning-1" },
							{
								type: "reasoning-delta",
								id: "reasoning-1",
								text_delta: "first completed reasoning part",
							},
							{ type: "reasoning-end", id: "reasoning-1" },
							{
								type: "tool-call",
								id: "tool-1",
								toolName: "Read",
								input: { file_path: "src/a.ts" },
								inputPreview: {
									preview: '{"file_path":"src/a.ts"}',
									truncated: false,
								},
							},
							{
								type: "tool-call",
								id: "tool-2",
								toolName: "Read",
								input: { file_path: "src/b.ts" },
								inputPreview: {
									preview: '{"file_path":"src/b.ts"}',
									truncated: false,
								},
							},
							{ type: "reasoning-start", id: "reasoning-2" },
							{
								type: "reasoning-delta",
								id: "reasoning-2",
								text_delta: "second completed reasoning part",
							},
							{ type: "reasoning-end", id: "reasoning-2" },
							{
								type: "tool-call",
								id: "tool-3",
								toolName: "Read",
								input: { file_path: "src/c.ts", query: "x".repeat(9000) },
								inputPreview: { preview: "x".repeat(8192), truncated: true },
							},
							{
								type: "tool-call",
								id: "tool-4",
								toolName: "Read",
								input: { file_path: "src/d.ts" },
								inputPreview: {
									preview: '{"file_path":"src/d.ts"}',
									truncated: false,
								},
							},
							{ type: "finish", finishReason: "tool-calls" },
						],
						providerCallRuntime: {
							systemInstructions: "tool declaration ordering test system",
							toolCatalog: catalogForTest({
								name: "Read",
								description: "Read file",
								inputSchema: { type: "object" },
							}),
						},
						runTool: async (request) => {
							executions.push(request.modelToolCallId);
							activeExecutions += 1;
							maxActiveExecutions = Math.max(
								maxActiveExecutions,
								activeExecutions,
							);
							if (activeExecutions === 4) {
								allExecutionsStarted.resolve(undefined);
							}
							await releaseExecutions.promise;
							activeExecutions -= 1;
							return {
								type: "completed",
								output: {
									text: `done ${request.modelToolCallId}`,
									truncated: false,
								},
							};
						},
					}),
				),
			),
		);
		await firstDeclarationArrived.promise;
		await flushMicrotasks(50);
		expect(declarations).toHaveLength(1);
		releaseFirstDeclaration.resolve(undefined);
		await allExecutionsStarted.promise;
		expect(executions).toEqual(["tool-1", "tool-2", "tool-3", "tool-4"]);
		expect(maxActiveExecutions).toBe(4);
		releaseExecutions.resolve(undefined);
		expect(await runPromise).toMatchObject({ type: "completed" });
		expect(settlements).toHaveLength(4);
		const appendMembers = declarations.map((envelope) =>
			envelope.assistantContextAppend?.parts.map((part) => {
				if (part.type === "reasoning") return `reasoning:${part.text}`;
				if (part.type === "tool") return `tool:${part.modelToolCallId}`;
				return part.type;
			}),
		);
		expect(appendMembers).toEqual([
			["reasoning:first completed reasoning part", "tool:tool-1"],
			["tool:tool-2"],
			["reasoning:second completed reasoning part", "tool:tool-3"],
			["tool:tool-4"],
		]);
	});
	test("settles a Tool Result while a sibling Assistant declaration ACK is still in flight", async () => {
		const session = new ThreadRuntime(
			"sesn_tool_settlement_during_sibling_declaration",
		);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "read both")],
		});
		const secondDeclarationArrived = deferred<void>();
		const releaseSecondDeclaration = deferred<void>();
		const declarations: SessionEventEnvelope[] = [];
		const settlements: SessionEventWriterToolSettlementEnvelope[] = [];
		let secondDeclarationReleased = false;
		const baseWriter = writerFrom((envelope) => ({
			ok: true,
			eventId: `bridge-${envelope.writeId}`,
			type: "committed",
			eventSequence: 1,
		}));
		const writer: SessionEventWriter = {
			...baseWriter,
			append: async (envelope) => {
				if (envelope.event.type === "agent.tool_use") {
					declarations.push(envelope);
					if (declarations.length === 2) {
						secondDeclarationArrived.resolve(undefined);
						await releaseSecondDeclaration.promise;
						secondDeclarationReleased = true;
					}
				}
				return await baseWriter.append(envelope);
			},
			settleToolResult: async (envelope) => {
				settlements.push(envelope);
				return await baseWriter.settleToolResult(envelope);
			},
		};
		const runPromise = Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						events: [
							{
								type: "tool-call",
								id: "tool-1",
								toolName: "Read",
								input: { file_path: "src/a.ts" },
								inputPreview: {
									preview: '{"file_path":"src/a.ts"}',
									truncated: false,
								},
							},
							{
								type: "tool-call",
								id: "tool-2",
								toolName: "Read",
								input: { file_path: "src/b.ts" },
								inputPreview: {
									preview: '{"file_path":"src/b.ts"}',
									truncated: false,
								},
							},
							{ type: "finish", finishReason: "tool-calls" },
						],
						providerCallRuntime: {
							systemInstructions:
								"tool settlement and declaration independence test",
							toolCatalog: catalogForTest({
								name: "Read",
								description: "Read file",
								inputSchema: { type: "object" },
							}),
						},
						runTool: async (request) => {
							if (request.modelToolCallId === "tool-1") {
								await secondDeclarationArrived.promise;
							}
							return {
								type: "completed",
								output: {
									text: `done ${request.modelToolCallId}`,
									truncated: false,
								},
							};
						},
					}),
				),
			),
		);
		await secondDeclarationArrived.promise;
		await waitForCondition(
			() => settlements.length === 1,
			"Tool Result settlement during sibling declaration ACK",
		);
		expect(secondDeclarationReleased).toBe(false);
		expect(settlements[0]?.settlement.outcome).toMatchObject({
			type: "completed",
		});
		releaseSecondDeclaration.resolve(undefined);
		expect(await runPromise).toMatchObject({ type: "completed" });
		expect(declarations).toHaveLength(2);
		expect(settlements).toHaveLength(2);
	});
	test("settles parallel Tool Results in completion order with target-only envelopes", async () => {
		const session = new ThreadRuntime("sesn_tool_projection_order");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const bothExecutionsStarted = deferred<void>();
		const releaseFirstExecution = deferred<void>();
		const releaseSecondExecution = deferred<void>();
		const declarations: SessionEventEnvelope[] = [];
		const settlements: SessionEventWriterToolSettlementEnvelope[] = [];
		const baseWriter = writerFrom((envelope) => ({
			ok: true,
			eventId: `bridge-${envelope.writeId}`,
			type: "committed",
			eventSequence: 1,
		}));
		const writer: SessionEventWriter = {
			...baseWriter,
			append: async (envelope) => {
				if (envelope.event.type === "agent.tool_use") {
					declarations.push(envelope);
				}
				return await baseWriter.append(envelope);
			},
			settleToolResult: async (envelope) => {
				settlements.push(envelope);
				return await baseWriter.settleToolResult(envelope);
			},
		};
		const runPromise = Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						events: [
							{
								type: "tool-call",
								id: "tool-1",
								toolName: "Read",
								input: { file_path: "src/a.ts" },
								inputPreview: {
									preview: '{"file_path":"src/a.ts"}',
									truncated: false,
								},
							},
							{
								type: "tool-call",
								id: "tool-2",
								toolName: "Read",
								input: { file_path: "src/b.ts" },
								inputPreview: {
									preview: '{"file_path":"src/b.ts"}',
									truncated: false,
								},
							},
							{ type: "finish", finishReason: "tool-calls" },
						],
						providerCallRuntime: {
							systemInstructions: "tool projection ordering test system",
							toolCatalog: catalogForTest({
								name: "Read",
								description: "Read file",
								inputSchema: { type: "object" },
							}),
						},
						runTool: async (request) => {
							if (declarations.length === 2)
								bothExecutionsStarted.resolve(undefined);
							if (request.modelToolCallId === "tool-1") {
								await releaseFirstExecution.promise;
							} else {
								await releaseSecondExecution.promise;
							}
							return {
								type: "completed",
								output: {
									text: `done ${request.modelToolCallId}`,
									truncated: false,
								},
							};
						},
					}),
				),
			),
		);
		await bothExecutionsStarted.promise;
		releaseSecondExecution.resolve(undefined);
		await flushMicrotasks(50);
		expect(settlements).toHaveLength(1);
		const firstToolUseEventId = `bridge-${declarations[0]?.writeId}`;
		const secondToolUseEventId = `bridge-${declarations[1]?.writeId}`;
		expect(settlements[0]?.settlement).toEqual(
			expect.objectContaining({
				toolUseEventId: secondToolUseEventId,
				outcome: expect.objectContaining({ type: "completed" }),
			}),
		);
		releaseFirstExecution.resolve(undefined);
		expect(await runPromise).toMatchObject({ type: "completed" });
		expect(settlements).toHaveLength(2);
		expect(
			settlements.map((envelope) => envelope.settlement.toolUseEventId),
		).toEqual([secondToolUseEventId, firstToolUseEventId]);
		expect(
			settlements.every(
				(envelope) => envelope.settlement.outcome.type === "completed",
			),
		).toBe(true);
	});
	test("separate thread provider requests share session-wide tool admission", async () => {
		const coordinator = new SessionToolCoordinator({ maxConcurrentTools: 8 });
		const identity = (sessionThreadId: string) => ({
			workspaceId: "wksp_1",
			sessionId: "sesn_1",
			sessionThreadId,
			bindingId: "bind_1",
			bindingGeneration: 1,
			targetPodUid: "pod_1",
			runtimeBindingToken: "runtime-token",
		});
		const firstSession = new ThreadRuntime(
			identity("thrd_a"),
			undefined,
			coordinator,
		);
		const secondSession = new ThreadRuntime(
			identity("thrd_b"),
			undefined,
			coordinator,
		);
		const releaseFirst = deferred<void>();
		const secondToolUseAcked = deferred<void>();
		const starts: string[] = [];
		const events: readonly LLMEvent[] = [
			{
				type: "tool-call",
				id: "tool-memory",
				toolName: "memory",
				input: { action: "view", path: "notes" },
				inputPreview: {
					preview: '{"action":"view","path":"notes"}',
					truncated: false,
				},
			},
			{ type: "finish", finishReason: "tool-calls" },
		];
		const makeLayer = (threadId: string) =>
			runtimeThreadLoopLayer(
				new RecordingContextLoader([], {
					type: "context",
					entries: [userMessage(`user-${threadId}`, 0, "inspect memory")],
				}),
				{
					events,
					approvalMode: "full_access",
					writer: writerFrom((envelope) => {
						if (
							threadId === "thrd_b" &&
							envelope.event.type === "agent.tool_use"
						) {
							secondToolUseAcked.resolve();
						}
						return {
							ok: true,
							eventId: `bridge-${envelope.writeId}`,
							type: "committed",
							eventSequence: 1,
						};
					}),
					providerCallRuntime: {
						systemInstructions: "session tool admission test system",
						toolCatalog: catalogForTest({
							name: "memory",
							description: "Memory",
							inputSchema: { type: "object" },
						}),
					},
					runTool: async (request) => {
						starts.push(request.sessionThreadId);
						if (request.sessionThreadId === "thrd_a") {
							await releaseFirst.promise;
						}
						return {
							type: "completed",
							output: { text: "done", truncated: false },
						};
					},
				},
			);
		const run = (
			session: ThreadRuntime,
			layer: Layer.Layer<ThreadLoop.Service>,
		) =>
			Effect.runPromise(
				Effect.gen(function* () {
					const threadLoop = yield* ThreadLoop.Service;
					return yield* threadLoop.run(session, testRunCustody());
				}).pipe(Effect.provide(layer)),
			);
		const first = run(firstSession, makeLayer("thrd_a"));
		await waitForCondition(
			() => starts.length === 1,
			"first session-wide tool start",
		);
		const second = run(secondSession, makeLayer("thrd_b"));
		await secondToolUseAcked.promise;
		await flushMicrotasks(50);
		expect(starts).toEqual(["thrd_a"]);
		releaseFirst.resolve(undefined);
		await Promise.all([first, second]);
		expect(starts).toEqual(["thrd_a", "thrd_b"]);
	});
	test("Memory projection replay stays in one ToolFiber until one final settlement", async () => {
		const session = new ThreadRuntime("sesn_1");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "remember this")],
		});
		const firstProjectionFailure = deferred<void>();
		const releaseProjectionSuccess = deferred<void>();
		const appended: SessionEvent[] = [];
		const settlements: SessionEventWriterToolSettlementEnvelope[] = [];
		let toolRunnerCalls = 0;
		let bridgeAttempts = 0;
		let runSettlements = 0;
		const writer = writerFrom(
			(envelope) => {
				appended.push(envelope.event);
				return {
					ok: true,
					eventId:
						envelope.event.type === "agent.tool_use"
							? "sevt_memory_projection"
							: `bridge-${envelope.writeId}`,
					type: "committed",
					eventSequence: 1,
				};
			},
			undefined,
			[],
			undefined,
			async (envelope) => {
				settlements.push(envelope);
				return { ok: true, result: { type: "committed" } };
			},
		);
		const runPromise = Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						events: [
							{
								type: "tool-call",
								id: "tool-memory",
								toolName: "memory",
								input: {
									action: "create",
									path: "notes/todo.md",
									content: "one",
								},
								inputPreview: {
									preview: '{"action":"create"}',
									truncated: false,
								},
							},
							{ type: "finish", finishReason: "tool-calls" },
						],
						providerCallRuntime: {
							systemInstructions: "memory projection replay ToolFiber test",
							toolCatalog: memoryCatalogForTest(),
						},
						runTool: async () => {
							toolRunnerCalls++;
							bridgeAttempts++;
							const projectionFailure = {
								status: "runtime_error",
								error_code: "projection_refresh_failed",
								retryable: true,
							};
							expect(projectionFailure).toEqual({
								status: "runtime_error",
								error_code: "projection_refresh_failed",
								retryable: true,
							});
							firstProjectionFailure.resolve(undefined);
							await releaseProjectionSuccess.promise;
							bridgeAttempts++;
							return {
								type: "completed",
								output: { text: "memory stored", truncated: false },
							};
						},
					}),
				),
			),
		).then((result) => {
			runSettlements++;
			return result;
		});
		await firstProjectionFailure.promise;
		expect(runSettlements).toBe(0);
		expect(settlements).toHaveLength(0);
		expect(
			appended.filter((event) => event.type === "session.status_idle"),
		).toHaveLength(0);
		expect(JSON.stringify(appended)).not.toContain("projection_refresh_failed");
		expect(contextToolStatus(session)).toBe("running");
		releaseProjectionSuccess.resolve(undefined);
		const result = await runPromise;
		expect(result).toMatchObject({ type: "completed" });
		expect(toolRunnerCalls).toBe(1);
		expect(bridgeAttempts).toBe(2);
		expect(runSettlements).toBe(1);
		expect(settlements).toEqual([
			expect.objectContaining({
				settlement: expect.objectContaining({
					toolUseEventId: "sevt_memory_projection",
				}),
			}),
		]);
		expect(
			appended.filter((event) => event.type === "session.status_idle"),
		).toHaveLength(1);
		expect(contextToolStatus(session)).toBe("completed");
	});
	test("interrupt joins a pre-fence agent.tool_use Bridge ACK before atomic Request End", async () => {
		const loader = new QueuedContextLoader([], []);
		const toolUseAppendStarted = deferred<void>();
		const releaseToolUseAppend = deferred<void>();
		const providerRelease = deferred<void>();
		const order: string[] = [];
		const appended: SessionEvent[] = [];
		const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
		let toolRunnerCalls = 0;
		let interruptCommitStarted = false;
		const recordWriter = writerFrom((envelope) => {
			appended.push(envelope.event);
			order.push(`event:${envelope.event.type}`);
			return {
				ok: true,
				eventId:
					envelope.event.type === "agent.tool_use"
						? "sevt_gated_tool_use"
						: `bridge-${envelope.writeId}`,
				type: "committed",
				eventSequence: 1,
			};
		});
		const writer: SessionEventWriter = {
			...recordWriter,
			append: async (envelope) => {
				if (envelope.event.type === "agent.tool_use") {
					order.push("tool-use-append:start");
					toolUseAppendStarted.resolve();
					await releaseToolUseAppend.promise;
					order.push("tool-use-append:ack");
				}
				return await recordWriter.append(envelope);
			},
			writeRequestEnd: async (envelope) => {
				requestEnds.push(envelope);
				return await recordWriter.writeRequestEnd(envelope);
			},
		};
		const agentLayer = runtimeThreadLoopLayer(loader, {
			writer,
			llmService: {
				stream(_request, options) {
					return Stream.fromAsyncIterable(
						(async function* () {
							yield {
								type: "tool-call" as const,
								id: "tool-gated",
								toolName: "Write",
								input: { file_path: "src/gated.ts", content: "one" },
								inputPreview: { preview: "{}", truncated: false },
							};
							if (options?.abortSignal === undefined) {
								throw new Error("provider stream requires an abort signal");
							}
							await waitForReleaseOrAbort(
								providerRelease.promise,
								options.abortSignal,
							);
							yield {
								type: "finish" as const,
								finishReason: "tool-calls" as const,
							};
						})(),
						(error): LLMServiceError => ({
							type: "llm-service",
							error: runtimeFailureFromProviderError(
								normalizeProviderError({
									code: "provider_stream_error",
									message: String(error),
									retryable: true,
								}),
							),
						}),
					);
				},
			},
			providerCallRuntime: {
				systemInstructions: "gated tool-use append test",
				toolCatalog: catalogForTest({
					name: "Write",
					description: "Write file",
					inputSchema: { type: "object" },
				}),
			},
			runTool: () => {
				toolRunnerCalls++;
				return {
					type: "completed",
					output: { text: "must not run", truncated: false },
				};
			},
		});
		const managerLayer = SessionManager.layer({
			maxLocalSessions: 4,
			now: () => createdAt,
		}).pipe(Layer.provide(agentLayer));
		const { manager, scope } = await Effect.runPromise(
			Effect.gen(function* () {
				const layerScope = yield* Scope.make();
				const context = yield* Layer.buildWithScope(managerLayer, layerScope);
				return {
					manager: Context.get(context, SessionManager.Service),
					scope: layerScope,
				};
			}),
		);
		try {
			const input = {
				...acceptedInput("rin_gated_tool_use"),
				contentJson: JSON.stringify({
					messages: [userMessage("user-1", 1, "run the gated tool")],
				}),
			};
			await Effect.runPromise(
				manager.preloadThread({
					...input,
					runtimeBindingToken: "runtime-binding-token",
					contextEntries: [userMessage("user-1", 1, "run the gated tool")],
					thread: {
						role: "main",
						visibility: "public",
						agentType: "general",
						status: "idle",
					},
				}),
			);
			await Effect.runPromise(manager.acceptInput(input));
			await toolUseAppendStarted.promise;
			const command = interruptInput("rin_gated_tool_use_interrupt", 9);
			const interrupt = Effect.runPromise(
				manager.interruptControl("sesn_1", command, async (declaration) => {
					interruptCommitStarted = true;
					order.push("commit:interrupt");
					return interruptCommitResult(command, declaration, [
						"sevt_gated_tool_use",
					]);
				}),
			);
			await new Promise<void>((resolve) => setImmediate(resolve));
			jest.useFakeTimers();
			await flushMicrotasks();
			jest.advanceTimersByTime(350);
			await flushMicrotasks();
			expect(interruptCommitStarted).toBe(false);
			expect(toolRunnerCalls).toBe(0);
			releaseToolUseAppend.resolve();
			for (let attempt = 0; attempt < 5; attempt++) {
				jest.advanceTimersByTime(1000);
				await flushMicrotasks();
			}
			expect(await interrupt).toMatchObject({ ok: true, interrupted: true });
			expect(toolRunnerCalls).toBe(0);
			expect(
				appended.filter((event) => event.type === "agent.tool_use"),
			).toHaveLength(1);
			expect(requestEnds).toHaveLength(1);
			expect(requestEnds[0]?.interruptSettlement).toEqual({
				runtimeInputId: command.runtimeInputId,
				interruptLeaseRef: command.interruptLeaseRef,
			});
			expect(
				await Effect.runPromise(manager.inspectThread(command)),
			).toMatchObject({
				ok: true,
				observed: true,
				hasPendingApprovalToolJobs: false,
			});
			expect(interruptCommitStarted).toBe(false);
			expect(order).not.toContain("commit:interrupt");
			expect(order.indexOf("tool-use-append:ack")).toBeLessThan(
				order.indexOf("event:span.model_request_end"),
			);
			expect(order.indexOf("event:span.model_request_end")).toBeLessThan(
				order.indexOf("event:session.status_idle"),
			);
		} finally {
			jest.useRealTimers();
			releaseToolUseAppend.resolve();
			providerRelease.resolve();
			await Effect.runPromise(Scope.close(scope, Exit.void));
		}
	});
	test("interrupt joins a raw CommitInternalToolRepair ACK before atomic Request End", async () => {
		const loader = new QueuedContextLoader([], []);
		const repairStarted = deferred<void>();
		const releaseRepair = deferred<void>();
		const releaseRepairWrapperTimeout = deferred<boolean>();
		const providerRelease = deferred<void>();
		const order: string[] = [];
		let interruptCommitStarted = false;
		const store = new ThreadLoopRuntimeStore(
			order,
			false,
			false,
			undefined,
			async () => {
				order.push("repair:start");
				repairStarted.resolve();
				await releaseRepair.promise;
				order.push("repair:ack");
			},
		);
		const appended: SessionEvent[] = [];
		let storeSleepCalls = 0;
		const agentLayer = runtimeThreadLoopLayer(loader, {
			store,
			runtime: {
				...threadLoopRuntime(),
				sleep: async (_durationMs, signal) => {
					storeSleepCalls++;
					if (storeSleepCalls === 3) {
						return await releaseRepairWrapperTimeout.promise;
					}
					return await new Promise<boolean>((resolve) => {
						if (signal.aborted) {
							resolve(false);
							return;
						}
						signal.addEventListener("abort", () => resolve(false), {
							once: true,
						});
					});
				},
			},
			writer: writerFrom((envelope) => {
				appended.push(envelope.event);
				order.push(`event:${envelope.event.type}`);
				return {
					ok: true,
					eventId: `bridge-${envelope.writeId}`,
					type: "committed",
					eventSequence: 1,
				};
			}),
			llmService: {
				stream(_request, options) {
					return Stream.fromAsyncIterable(
						(async function* () {
							yield {
								type: "tool-call" as const,
								id: "tool-invalid",
								toolName: "MissingTool",
								input: {},
								inputPreview: { preview: "{}", truncated: false },
							};
							if (options?.abortSignal === undefined) {
								throw new Error("provider stream requires an abort signal");
							}
							await waitForReleaseOrAbort(
								providerRelease.promise,
								options.abortSignal,
							);
							yield {
								type: "finish" as const,
								finishReason: "tool-calls" as const,
							};
						})(),
						(error): LLMServiceError => ({
							type: "llm-service",
							error: runtimeFailureFromProviderError(
								normalizeProviderError({
									code: "provider_stream_error",
									message: String(error),
									retryable: true,
								}),
							),
						}),
					);
				},
			},
			providerCallRuntime: {
				systemInstructions: "gated internal repair test",
				toolCatalog: catalogForTest({
					name: "Write",
					description: "Write file",
					inputSchema: { type: "object" },
				}),
			},
		});
		const managerLayer = SessionManager.layer({
			maxLocalSessions: 4,
			now: () => createdAt,
		}).pipe(Layer.provide(agentLayer));
		const { manager, scope } = await Effect.runPromise(
			Effect.gen(function* () {
				const layerScope = yield* Scope.make();
				const context = yield* Layer.buildWithScope(managerLayer, layerScope);
				return {
					manager: Context.get(context, SessionManager.Service),
					scope: layerScope,
				};
			}),
		);
		try {
			const input = {
				...acceptedInput("rin_gated_internal_repair"),
				contentJson: JSON.stringify({
					messages: [userMessage("user-1", 1, "trigger an internal repair")],
				}),
			};
			await Effect.runPromise(
				manager.preloadThread({
					...input,
					runtimeBindingToken: "runtime-binding-token",
					contextEntries: [
						userMessage("user-1", 1, "trigger an internal repair"),
					],
					thread: {
						role: "main",
						visibility: "public",
						agentType: "general",
						status: "idle",
					},
				}),
			);
			await Effect.runPromise(manager.acceptInput(input));
			await repairStarted.promise;
			const command = interruptInput("rin_gated_internal_repair_interrupt", 9);
			const interrupt = Effect.runPromise(
				manager.interruptControl("sesn_1", command, async (declaration) => {
					interruptCommitStarted = true;
					order.push("commit:interrupt");
					return interruptCommitResult(command, declaration, []);
				}),
			);
			await flushMicrotasks();
			expect(interruptCommitStarted).toBe(false);
			releaseRepairWrapperTimeout.resolve(true);
			await flushMicrotasks();
			expect(interruptCommitStarted).toBe(false);
			releaseRepair.resolve();
			expect({ interruptResult: await interrupt, order }).toMatchObject({
				interruptResult: { ok: true, interrupted: true },
			});
			const operationCountAtCloseout = order.filter(
				(entry) => entry === "store:internal-tool-repair",
			).length;
			await flushMicrotasks();
			expect(operationCountAtCloseout).toBe(1);
			expect(
				order.filter((entry) => entry === "store:internal-tool-repair"),
			).toHaveLength(1);
			expect(interruptCommitStarted).toBe(false);
			expect(order).not.toContain("commit:interrupt");
			expect(order.indexOf("repair:ack")).toBeLessThan(
				order.indexOf("event:span.model_request_end"),
			);
			expect(order.indexOf("event:span.model_request_end")).toBeLessThan(
				order.indexOf("event:session.status_idle"),
			);
			expect(
				appended.filter((event) => event.type === "session.error"),
			).toEqual([]);
		} finally {
			releaseRepair.resolve();
			releaseRepairWrapperTimeout.resolve(false);
			providerRelease.resolve();
			await Effect.runPromise(Scope.close(scope, Exit.void));
		}
	});
	test("post-success cooperative repair failure settles the attachment ride already consumed by Bridge", async () => {
		const session = new ThreadRuntime("sesn_1");
		const attachment = {
			transient: {
				attachmentRef: "att_post_success_cooperative_failure",
				sourcePath: "mcp:test/post-success-cooperative-failure.png",
				pageRange: "",
				detail: "auto",
			},
			fileBacked: undefined,
			mime: "image/png",
			filename: "post-success-cooperative-failure.png",
		} as const;
		session.state.addPendingAttachments([attachment]);
		let failRepairWrite = false;
		let repairAttempted = false;
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [
				userMessage("user-post-success-cooperative-failure", 0, "run the tool"),
			],
		});
		const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
		let interruptOwner: (() => void) | undefined;
		const baseWriter = writerFrom((envelope) => {
			return {
				ok: true,
				eventId:
					envelope.event.type === "agent.tool_use"
						? "sevt_post_success_cooperative_failure"
						: `bridge-${envelope.writeId}`,
				type: "committed",
				eventSequence: 1,
			};
		});
		const writer: SessionEventWriter = {
			...baseWriter,
			settleToolResult: async (envelope) => {
				if (failRepairWrite) {
					repairAttempted = true;
					return {
						ok: false,
						error: normalizeSessionEventWriterError({
							code: "unavailable",
							sessionId: envelope.sessionId,
							writeId: envelope.settlement.toolUseEventId,
						}),
					};
				}
				return await baseWriter.settleToolResult(envelope);
			},
			writeRequestEnd: async (envelope) => {
				requestEnds.push(envelope);
				return await baseWriter.writeRequestEnd(envelope);
			},
		};
		const runFiber = Effect.runFork(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						providerCallRuntime: {
							systemInstructions:
								"post-success cooperative repair failure test",
							toolCatalog: catalogForTest({
								name: "Write",
								description: "Write file",
								inputSchema: { type: "object" },
							}),
						},
						events: [
							{
								type: "tool-call",
								id: "tool-post-success-cooperative-failure",
								toolName: "Write",
								input: { file_path: "src/failure.ts", content: "one" },
								inputPreview: { preview: "{}", truncated: false },
							},
							{ type: "finish", finishReason: "tool-calls" },
						],
						runTool: async (request) => {
							await new Promise<void>((resolve) => {
								if (request.abortSignal.aborted) {
									resolve();
									return;
								}
								request.abortSignal.addEventListener("abort", () => resolve(), {
									once: true,
								});
							});
							return { type: "cancelled" };
						},
					}),
				),
			),
		);
		interruptOwner = () => {
			void Effect.runPromise(Fiber.interrupt(runFiber));
		};
		await waitForCondition(
			() => requestEnds.length === 1,
			"successful request-end before cooperative cancellation",
		);
		await new Promise<void>((resolve) => setImmediate(resolve));
		failRepairWrite = true;
		session.state.beginCooperativeCancel();
		interruptOwner();
		const runExit = await Effect.runPromise(Fiber.await(runFiber));
		expect(
			Exit.isFailure(runExit) && Cause.hasInterruptsOnly(runExit.cause),
		).toBe(true);
		expect(repairAttempted).toBe(true);
		expect(requestEnds).toHaveLength(1);
		expect(requestEnds[0]).toMatchObject({
			isError: false,
			consumedAttachmentRefs: ["att_post_success_cooperative_failure"],
		});
	});
	test("post-success interrupt-fence failure settles the attachment ride already consumed by Bridge", async () => {
		const session = new ThreadRuntime("sesn_1");
		const attachment = {
			transient: {
				attachmentRef: "att_post_success_interrupt_failure",
				sourcePath: "mcp:test/post-success-interrupt-failure.png",
				pageRange: "",
				detail: "auto",
			},
			fileBacked: undefined,
			mime: "image/png",
			filename: "post-success-interrupt-failure.png",
		} as const;
		session.state.addPendingAttachments([attachment]);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [
				userMessage("user-post-success-interrupt-failure", 0, "run the tool"),
			],
		});
		const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
		const baseWriter = writerFrom((envelope) => ({
			ok: true,
			eventId:
				envelope.event.type === "agent.tool_use"
					? "sevt_post_success_interrupt_failure"
					: `bridge-${envelope.writeId}`,
			type: "committed",
			eventSequence: 1,
		}));
		const writer: SessionEventWriter = {
			...baseWriter,
			writeRequestEnd: async (envelope) => {
				requestEnds.push(envelope);
				session.state.beginUserInterrupt(
					interruptInput("rin_post_success_interrupt_failure", 9),
					async () => ({
						ok: false,
						retryable: false,
						errorCode: "interrupt_conflict",
					}),
				);
				return await baseWriter.writeRequestEnd(envelope);
			},
		};
		const result = await Effect.runPromiseExit(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						providerCallRuntime: {
							systemInstructions: "post-success interrupt-fence failure test",
							toolCatalog: catalogForTest({
								name: "Write",
								description: "Write file",
								inputSchema: { type: "object" },
							}),
						},
						events: [
							{
								type: "tool-call",
								id: "tool-post-success-interrupt-failure",
								toolName: "Write",
								input: { file_path: "src/failure.ts", content: "one" },
								inputPreview: { preview: "{}", truncated: false },
							},
							{ type: "finish", finishReason: "tool-calls" },
						],
						runTool: async (request) => {
							await new Promise<void>((resolve) => {
								if (request.abortSignal.aborted) {
									resolve();
									return;
								}
								request.abortSignal.addEventListener("abort", () => resolve(), {
									once: true,
								});
							});
							return { type: "cancelled" };
						},
					}),
				),
			),
		);
		expect(Exit.isFailure(result)).toBe(true);
		expect(requestEnds).toHaveLength(1);
		expect(requestEnds[0]).toMatchObject({
			isError: false,
			consumedAttachmentRefs: ["att_post_success_interrupt_failure"],
		});
		expect(session.state.pendingAttachments()).toEqual([]);
	});
	test("user interrupt repairs a committed ToolFiber before CommitInputs and FinishIdle", async () => {
		const session = new ThreadRuntime("sesn_1");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "remember this")],
		});
		const releaseProvider = deferred<void>();
		const projectionFailureSeen = deferred<void>();
		const toolAbortSettled = deferred<void>();
		const appended: SessionEvent[] = [];
		const settlements: SessionEventWriterToolSettlementEnvelope[] = [];
		const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
		const closeoutOrder: string[] = [];
		let observedToolSignal: AbortSignal | undefined;
		let toolRunnerCalls = 0;
		let bridgeAttempts = 0;
		const service: LLMServiceInterface = {
			stream(_request, options) {
				return Stream.fromAsyncIterable(
					(async function* () {
						yield {
							type: "tool-call" as const,
							id: "tool-memory",
							toolName: "memory",
							input: {
								action: "create",
								path: "notes/todo.md",
								content: "one",
							},
							inputPreview: {
								preview: '{"action":"create"}',
								truncated: false,
							},
						};
						if (options?.abortSignal === undefined) {
							throw new Error("provider stream requires an abort signal");
						}
						await waitForReleaseOrAbort(
							releaseProvider.promise,
							options.abortSignal,
						);
						yield {
							type: "finish" as const,
							finishReason: "tool-calls" as const,
						};
					})(),
					(error): LLMServiceError => ({
						type: "llm-service",
						error: runtimeFailureFromProviderError(
							normalizeProviderError({
								code: "provider_stream_error",
								message: String(error),
								retryable: true,
							}),
						),
					}),
				);
			},
		};
		const writer = writerFrom(
			(envelope) => {
				appended.push(envelope.event);
				closeoutOrder.push(`event:${envelope.event.type}`);
				return {
					ok: true,
					eventId:
						envelope.event.type === "agent.tool_use"
							? "sevt_memory_projection_cancel"
							: `bridge-${envelope.writeId}`,
					type: "committed",
					eventSequence: 1,
				};
			},
			async (envelope) => {
				requestEnds.push(envelope);
				closeoutOrder.push("event:span.model_request_end");
				return requestEndResultForTest(envelope);
			},
			[],
			undefined,
			async (envelope) => {
				settlements.push(envelope);
				return { ok: true, result: { type: "committed" } };
			},
		);
		const runFiber = Effect.runFork(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						llmService: service,
						providerCallRuntime: {
							systemInstructions:
								"memory projection cancellation ToolFiber test",
							toolCatalog: memoryCatalogForTest(),
						},
						runTool: (request) => {
							toolRunnerCalls++;
							bridgeAttempts++;
							const projectionFailure = {
								status: "runtime_error",
								error_code: "projection_refresh_failed",
								retryable: true,
							};
							expect(projectionFailure).toEqual({
								status: "runtime_error",
								error_code: "projection_refresh_failed",
								retryable: true,
							});
							observedToolSignal = request.abortSignal;
							return new Promise((resolve) => {
								request.abortSignal.addEventListener(
									"abort",
									() => {
										toolAbortSettled.resolve();
										resolve({ type: "cancelled" });
									},
									{ once: true },
								);
								projectionFailureSeen.resolve(undefined);
							});
						},
					}),
				),
			),
		);
		await projectionFailureSeen.promise;
		expect(settlements).toHaveLength(0);
		expect(JSON.stringify(appended)).not.toContain("projection_refresh_failed");
		expect(contextToolStatus(session)).toBe("running");
		const interruptCommand = interruptInput("rin_interrupt", 9);
		session.state.beginUserInterrupt(interruptCommand, async (declaration) => {
			closeoutOrder.push("commit:interrupt");
			return interruptCommitResult(interruptCommand, declaration, [
				"sevt_memory_projection_cancel",
			]);
		});
		const interrupt = Effect.runPromise(Fiber.interrupt(runFiber));
		await waitForCondition(
			() => observedToolSignal?.aborted === true,
			"Memory projection ToolFiber abort",
		);
		releaseProvider.resolve(undefined);
		await interrupt;
		const runExit = await Effect.runPromise(Fiber.await(runFiber));
		await toolAbortSettled.promise;
		expect(
			Exit.isFailure(runExit) && Cause.hasInterruptsOnly(runExit.cause),
		).toBe(true);
		expect(toolRunnerCalls).toBe(1);
		expect(bridgeAttempts).toBe(1);
		expect(settlements).toEqual([]);
		expect(appended.filter((event) => event.type === "session.error")).toEqual(
			[],
		);
		expect(requestEnds).toHaveLength(1);
		expect(requestEnds[0]?.interruptSettlement).toEqual({
			runtimeInputId: "rin_interrupt",
			interruptLeaseRef: interruptCommand.interruptLeaseRef,
		});
		expect(closeoutOrder).not.toContain("commit:interrupt");
		expect(closeoutOrder.indexOf("event:span.model_request_end")).toBeLessThan(
			closeoutOrder.indexOf("event:session.status_idle"),
		);
		expect(contextToolStatus(session)).toBe("cancelled");
	});
	test("SessionManager enforces the five-state interrupt fence across tools and CommitInputs", async () => {
		const releasePreCommit = deferred<void>();
		const terminalResultAcked = deferred<void>();
		const releaseNextProviderTool = deferred<void>();
		const pendingToolUseAppendStarted = deferred<void>();
		const releasePendingToolUseAppend = deferred<void>();
		const uncommittedRepairStarted = deferred<void>();
		const releaseUncommittedRepair = deferred<void>();
		let preCommitObserved = false;
		let requestEndObserved = false;
		const commitCalls: string[] = [];
		const order: string[] = [];
		let nextMessageSequence = 1;
		const commitReceipt = (
			input: RuntimeAcceptedInputState,
		): ReturnType<typeof acceptedInputCommitResult> => {
			const result = acceptedInputCommitResult(
				input,
				"committed",
				nextMessageSequence,
			);
			nextMessageSequence += result.assignedContextSequences.length;
			return result;
		};
		const loader: TestContextLoader = {
			buildContext: async () => [],
			loadPendingInput: async () => ({ type: "empty" }),
			commitAcceptedInput: async (input) => {
				commitCalls.push(input.runtimeInputId);
				if (input.runtimeInputId === "rin_initial_mixed") {
					order.push("commit:initial");
					return commitReceipt(input);
				}
				if (input.runtimeInputId === "rin_pre_fence_mixed") {
					order.push("commit:pre:start");
					preCommitObserved = true;
					await releasePreCommit.promise;
					order.push("commit:pre:end");
					return commitReceipt(input);
				}
				order.push(`commit:${input.runtimeInputId}`);
				return commitReceipt(input);
			},
		};
		const appended: SessionEvent[] = [];
		const storeOrder: string[] = [];
		const durableSequence: TestDurableSequence = {
			eventSequence: 100000,
			messageSequence: 100000,
		};
		const store = new ThreadLoopRuntimeStore(
			storeOrder,
			false,
			false,
			undefined,
			async (repair) => {
				if (repair.modelToolCallId === "tool-uncommitted") {
					uncommittedRepairStarted.resolve();
					await releaseUncommittedRepair.promise;
				}
			},
			durableSequence,
		);
		let providerCalls = 0;
		let toolUseWrites = 0;
		let toolRunnerCalls = 0;
		let interruptDeclaration: RuntimeControlInputDeclaration | undefined;
		const terminalCatalog = catalogForTest({
			name: "Read",
			description: "Terminal read",
			inputSchema: { type: "object" },
		});
		const pendingCatalog = catalogForTest({
			name: "Write",
			description: "Pending write",
			inputSchema: { type: "object" },
			permissionPolicy: "always_ask",
		});
		const mixedCatalog: ToolCatalog = {
			entries: [...terminalCatalog.entries, ...pendingCatalog.entries],
			configs: [...terminalCatalog.configs, ...pendingCatalog.configs],
		};
		const settlements: SessionEventWriterToolSettlementEnvelope[] = [];
		const mixedWriterBase = writerFrom(
			(envelope) => {
				appended.push(envelope.event);
				if (envelope.event.type === "session.status_idle") {
					order.push(
						`event:session.status_idle:${envelope.event.stop_reason.type}`,
					);
				} else {
					order.push(`event:${envelope.event.type}`);
				}
				if (envelope.event.type === "agent.tool_use") {
					toolUseWrites++;
				}
				if (envelope.event.type === "span.model_request_end") {
					requestEndObserved = true;
				}
				return {
					ok: true,
					eventId:
						envelope.event.type === "agent.tool_use"
							? `sevt_mixed_tool_${toolUseWrites}`
							: `bridge-${envelope.writeId}`,
					type: "committed",
					eventSequence: 1,
				};
			},
			undefined,
			[],
			durableSequence,
		);
		const mixedWriter: SessionEventWriter = {
			...mixedWriterBase,
			settleToolResult: async (envelope) => {
				settlements.push(envelope);
				order.push(`settlement:${envelope.settlement.toolUseEventId}`);
				if (envelope.settlement.toolUseEventId === "sevt_mixed_tool_1") {
					terminalResultAcked.resolve();
				}
				return await mixedWriterBase.settleToolResult(envelope);
			},
			append: async (envelope) => {
				const result = await mixedWriterBase.append(envelope);
				if (envelope.event.type === "agent.tool_use" && toolUseWrites === 2) {
					pendingToolUseAppendStarted.resolve();
					await releasePendingToolUseAppend.promise;
				}
				return result;
			},
		};
		const agentLayer = runtimeThreadLoopLayer(loader, {
			store,
			writer: mixedWriter,
			llmService: {
				stream() {
					providerCalls++;
					if (providerCalls > 1) {
						return Stream.fromIterable([
							{ type: "text-start" as const, id: "follow-up" },
							{
								type: "text-delta" as const,
								id: "follow-up",
								text_delta: "continued",
							},
							{ type: "text-end" as const, id: "follow-up" },
							{ type: "finish" as const, finishReason: "stop" as const },
						]);
					}
					return Stream.fromAsyncIterable(
						(async function* () {
							yield {
								type: "tool-call" as const,
								id: "tool-terminal",
								toolName: "Read",
								input: { file_path: "src/shared.ts", content: "terminal" },
								inputPreview: { preview: "{}", truncated: false },
							};
							await releaseNextProviderTool.promise;
							yield {
								type: "tool-call" as const,
								id: "tool-running",
								toolName: "Write",
								input: { file_path: "src/shared.ts", content: "running" },
								inputPreview: { preview: "{}", truncated: false },
							};
							await pendingToolUseAppendStarted.promise;
							yield {
								type: "tool-call" as const,
								id: "tool-uncommitted",
								toolName: "UncommittedWrite",
								input: {
									file_path: "src/shared.ts",
									content: "must-not-commit",
								},
								inputPreview: { preview: "{}", truncated: false },
							};
							yield {
								type: "finish" as const,
								finishReason: "tool-calls" as const,
							};
						})(),
						(error): LLMServiceError => ({
							type: "llm-service",
							error: runtimeFailureFromProviderError(
								normalizeProviderError({
									code: "provider_stream_error",
									message: String(error),
									retryable: true,
								}),
							),
						}),
					);
				},
			},
			providerCallRuntime: {
				systemInstructions: "mixed interrupt fence test",
				toolCatalog: mixedCatalog,
			},
			approvalMode: "ask_for_approval",
			runTool: (request) => {
				toolRunnerCalls++;
				expect(request.modelToolCallId).toBe("tool-terminal");
				return {
					type: "completed",
					output: { text: "terminal output", truncated: false },
				};
			},
		});
		const managerLayer = SessionManager.layer({
			maxLocalSessions: 4,
			now: () => createdAt,
		}).pipe(Layer.provide(agentLayer));
		const { manager, scope } = await Effect.runPromise(
			Effect.gen(function* () {
				const layerScope = yield* Scope.make();
				const context = yield* Layer.buildWithScope(managerLayer, layerScope);
				return {
					manager: Context.get(context, SessionManager.Service),
					scope: layerScope,
				};
			}),
		);
		try {
			const initialInput = acceptedInput("rin_initial_mixed");
			await Effect.runPromise(
				manager.preloadThread({
					...initialInput,
					runtimeBindingToken: "runtime-binding-token",
					contextEntries: [],
					thread: {
						role: "main",
						visibility: "public",
						agentType: "general",
						status: "idle",
					},
				}),
			);
			await Effect.runPromise(manager.acceptInput(initialInput));
			await terminalResultAcked.promise;
			expect(
				settlements.filter(
					(envelope) =>
						envelope.settlement.toolUseEventId === "sevt_mixed_tool_1" &&
						envelope.settlement.outcome.type === "completed",
				),
			).toHaveLength(1);
			releaseNextProviderTool.resolve();
			await pendingToolUseAppendStarted.promise;
			expect(
				appended.filter((event) => event.type === "agent.tool_use"),
			).toHaveLength(2);
			releasePendingToolUseAppend.resolve();
			await uncommittedRepairStarted.promise;
			expect(
				settlements.filter(
					(envelope) =>
						envelope.settlement.toolUseEventId === "sevt_mixed_tool_2",
				),
			).toHaveLength(0);
			expect(
				store.repairs.filter(
					(repair) => repair.modelToolCallId === "tool-uncommitted",
				),
			).toHaveLength(0);
			const preFenceInput = {
				...acceptedInput("rin_pre_fence_mixed"),
				inputOrder: 8,
			};
			await Effect.runPromise(manager.acceptInput(preFenceInput));
			expect(commitCalls).toEqual(["rin_initial_mixed"]);
			releaseUncommittedRepair.resolve();
			await waitForCondition(
				() =>
					requestEndObserved &&
					order.includes("event:session.status_idle:requires_action"),
				"request-end and requires-action settlement",
			);
			expect(preCommitObserved).toBe(false);
			expect(providerCalls).toBe(1);
			expect(toolRunnerCalls).toBe(1);
			expect(
				appended.filter((event) => event.type === "agent.tool_use"),
			).toHaveLength(2);
			expect(
				settlements.filter(
					(envelope) =>
						envelope.settlement.toolUseEventId === "sevt_mixed_tool_1",
				),
			).toHaveLength(1);
			expect(
				settlements.filter(
					(envelope) =>
						envelope.settlement.toolUseEventId === "sevt_mixed_tool_2",
				),
			).toHaveLength(0);
			expect(JSON.stringify(appended)).not.toContain("tool-uncommitted");
			expect(JSON.stringify(appended)).not.toContain("must-not-commit");
			expect(
				store.repairs.filter(
					(repair) => repair.modelToolCallId === "tool-uncommitted",
				),
			).toHaveLength(1);
			expect(order).toContain("event:session.status_idle:requires_action");
			const interruptCommand = interruptInput("rin_mixed_interrupt", 9);
			const interrupt = Effect.runPromise(
				manager.interruptControl(
					"sesn_1",
					interruptCommand,
					async (declaration) => {
						interruptDeclaration = declaration;
						order.push("commit:interrupt");
						return interruptCommitResult(interruptCommand, declaration, [
							"sevt_mixed_tool_2",
						]);
					},
				),
			);
			await new Promise<void>((resolve) => setImmediate(resolve));
			await flushMicrotasks();
			const postFenceInput = {
				...acceptedInput("rin_post_fence_mixed"),
				inputOrder: 10,
			};
			const postAccept = await Effect.runPromise(
				manager.acceptInput(postFenceInput),
			);
			expect(postAccept).toMatchObject({ ok: true, started: true });
			await flushMicrotasks();
			expect(order).not.toContain("commit:rin_post_fence_mixed");
			expect(
				order.filter((entry) => entry === "settlement:sevt_mixed_tool_2"),
			).toHaveLength(0);
			releaseNextProviderTool.resolve();
			const interruptResult = await interrupt;
			expect({ interruptResult, order }).toMatchObject({
				interruptResult: { ok: true, interrupted: false, idleInterrupt: true },
			});
			const postRun = await Effect.runPromise(
				manager.waitThread(postFenceInput, 1000),
			);
			expect(postRun).toMatchObject({ ok: true });
			await waitForCondition(
				() => commitCalls.includes("rin_post_fence_mixed"),
				"post-fence accepted input follow-up",
			);
			expect(providerCalls).toBe(2);
			expect(toolRunnerCalls).toBe(1);
			expect(commitCalls).toEqual([
				"rin_initial_mixed",
				"rin_post_fence_mixed",
			]);
			expect(
				appended.filter((event) => event.type === "agent.tool_use"),
			).toHaveLength(2);
			expect(
				settlements.filter(
					(envelope) =>
						envelope.settlement.toolUseEventId === "sevt_mixed_tool_1",
				),
			).toHaveLength(1);
			expect(
				settlements.filter(
					(envelope) =>
						envelope.settlement.toolUseEventId === "sevt_mixed_tool_2",
				),
			).toEqual([]);
			expect(interruptDeclaration).toEqual({ inputKind: "interrupt" });
			expect(JSON.stringify(appended)).not.toContain("tool-uncommitted");
			expect(JSON.stringify(appended)).not.toContain("must-not-commit");
			expect(
				appended.filter((event) => event.type === "session.error"),
			).toEqual([]);
			expect(order).not.toContain("commit:pre:start");
			expect(order).not.toContain("commit:pre:end");
			expect(order.indexOf("commit:interrupt")).toBeLessThan(
				order.indexOf("commit:rin_post_fence_mixed"),
			);
			expect(order.indexOf("commit:rin_post_fence_mixed")).toBeLessThan(
				order.indexOf("event:session.status_idle:end_turn"),
			);
			const closeoutIdleIndex = order.indexOf(
				"event:session.status_idle:end_turn",
			);
			expect(
				order
					.slice(0, closeoutIdleIndex)
					.filter((entry) => entry.startsWith("commit:")),
			).toEqual([
				"commit:initial",
				"commit:interrupt",
				"commit:rin_post_fence_mixed",
			]);
		} finally {
			releasePreCommit.resolve();
			releasePendingToolUseAppend.resolve();
			releaseUncommittedRepair.resolve();
			await Effect.runPromise(Scope.close(scope, Exit.void));
		}
	});
	test("SessionManager bounds a non-cooperative post-stream ToolFiber and fences its late completion", async () => {
		const routeStarted = deferred<void>();
		const abortObserved = deferred<void>();
		const requestEndAcked = deferred<void>();
		const lateRoute = deferred<RuntimeToolExecutionResult>();
		const order: string[] = [];
		const commitCalls: string[] = [];
		let nextMessageSequence = 1;
		const commitReceipt = (
			input: RuntimeAcceptedInputState,
		): ReturnType<typeof acceptedInputCommitResult> => {
			const result = acceptedInputCommitResult(
				input,
				"committed",
				nextMessageSequence,
			);
			nextMessageSequence += result.assignedContextSequences.length;
			return result;
		};
		const loader: TestContextLoader = {
			buildContext: async () => [],
			loadPendingInput: async () => ({ type: "empty" }),
			commitAcceptedInput: async (input) => {
				commitCalls.push(input.runtimeInputId);
				order.push(`commit:${input.runtimeInputId}`);
				if (input.runtimeInputId === "rin_non_cooperative_route") {
					return commitReceipt(input);
				}
				return commitReceipt(input);
			},
		};
		const appended: SessionEvent[] = [];
		const settlements: SessionEventWriterToolSettlementEnvelope[] = [];
		const storeOrder: string[] = [];
		const store = new ThreadLoopRuntimeStore(storeOrder);
		let providerCalls = 0;
		let toolRunnerCalls = 0;
		let toolUseWrites = 0;
		let observedRouteSignal: AbortSignal | undefined;
		let interruptDeclaration: RuntimeControlInputDeclaration | undefined;
		const agentLayer = runtimeThreadLoopLayer(loader, {
			store,
			writer: writerFrom(
				(envelope) => {
					appended.push(envelope.event);
					if (envelope.event.type === "session.status_idle") {
						order.push(
							`event:session.status_idle:${envelope.event.stop_reason.type}`,
						);
					} else {
						order.push(`event:${envelope.event.type}`);
					}
					if (envelope.event.type === "agent.tool_use") {
						toolUseWrites++;
					}
					if (envelope.event.type === "span.model_request_end") {
						requestEndAcked.resolve();
					}
					return {
						ok: true,
						eventId:
							envelope.event.type === "agent.tool_use"
								? `sevt_non_cooperative_route_${toolUseWrites}`
								: `bridge-${envelope.writeId}`,
						type: "committed",
						eventSequence: 1,
					};
				},
				undefined,
				[],
				{ eventSequence: 0, messageSequence: 1 },
				async (envelope) => {
					settlements.push(envelope);
					return { ok: true, result: { type: "committed" } };
				},
			),
			llmService: {
				stream() {
					providerCalls++;
					order.push(`provider:${providerCalls}`);
					if (providerCalls > 1) {
						return Stream.fromIterable([
							{ type: "text-start" as const, id: "follow-up" },
							{
								type: "text-delta" as const,
								id: "follow-up",
								text_delta: "continued",
							},
							{ type: "text-end" as const, id: "follow-up" },
							{ type: "finish" as const, finishReason: "stop" as const },
						]);
					}
					return Stream.fromIterable([
						{
							type: "tool-call" as const,
							id: "tool-non-cooperative-route",
							toolName: "Write",
							input: { file_path: "src/non-cooperative.ts", content: "late" },
							inputPreview: { preview: "{}", truncated: false },
						},
						{ type: "finish" as const, finishReason: "tool-calls" as const },
					]);
				},
			},
			providerCallRuntime: {
				systemInstructions: "non-cooperative post-stream ToolFiber test",
				toolCatalog: catalogForTest({
					name: "Write",
					description: "Write file",
					inputSchema: { type: "object" },
				}),
			},
			runTool: (request) => {
				toolRunnerCalls++;
				observedRouteSignal = request.abortSignal;
				request.abortSignal.addEventListener(
					"abort",
					() => {
						order.push("route:abort");
						abortObserved.resolve();
					},
					{ once: true },
				);
				routeStarted.resolve();
				return lateRoute.promise;
			},
		});
		const managerLayer = SessionManager.layer({
			maxLocalSessions: 4,
			now: () => createdAt,
		}).pipe(Layer.provide(agentLayer));
		const { manager, scope } = await Effect.runPromise(
			Effect.gen(function* () {
				const layerScope = yield* Scope.make();
				const context = yield* Layer.buildWithScope(managerLayer, layerScope);
				return {
					manager: Context.get(context, SessionManager.Service),
					scope: layerScope,
				};
			}),
		);
		try {
			const initialInput = acceptedInput("rin_non_cooperative_route");
			await Effect.runPromise(
				manager.preloadThread({
					...initialInput,
					runtimeBindingToken: "runtime-binding-token",
					contextEntries: [],
					thread: {
						role: "main",
						visibility: "public",
						agentType: "general",
						status: "idle",
					},
				}),
			);
			await Effect.runPromise(manager.acceptInput(initialInput));
			await Promise.all([routeStarted.promise, requestEndAcked.promise]);
			expect(toolRunnerCalls).toBe(1);
			expect(observedRouteSignal?.aborted).toBe(false);
			expect(
				appended.filter((event) => event.type === "agent.tool_use"),
			).toHaveLength(1);
			expect(settlements).toHaveLength(0);
			const interruptCommand = interruptInput(
				"rin_non_cooperative_route_interrupt",
				9,
			);
			let interruptSettled = false;
			const interrupt = Effect.runPromise(
				manager.interruptControl(
					"sesn_1",
					interruptCommand,
					async (declaration) => {
						interruptDeclaration = declaration;
						order.push("commit:interrupt");
						return interruptCommitResult(interruptCommand, declaration, [
							"sevt_non_cooperative_route_1",
						]);
					},
				),
			).then((result) => {
				interruptSettled = true;
				return result;
			});
			await new Promise<void>((resolve) => setImmediate(resolve));
			const postFenceInput = {
				...acceptedInput("rin_after_non_cooperative_route"),
				inputOrder: 10,
			};
			await Effect.runPromise(manager.acceptInput(postFenceInput));
			await flushMicrotasks(50);
			expect(observedRouteSignal?.aborted).toBe(true);
			await abortObserved.promise;
			expect(interruptSettled).toBe(false);
			expect(providerCalls).toBe(1);
			expect(commitCalls).toEqual(["rin_non_cooperative_route"]);
			expect(order.indexOf("commit:interrupt")).toBeLessThan(
				order.indexOf("route:abort"),
			);
			expect(
				await Effect.runPromise(manager.waitThread(interruptCommand, 0)),
			).toMatchObject({ ok: true, timedOut: true });
			await expect(interrupt).resolves.toMatchObject({
				ok: true,
				interrupted: true,
			});
			expect(interruptSettled).toBe(true);
			await Effect.runPromise(manager.waitThread(postFenceInput, 1000));
			expect(
				settlements.filter(
					(envelope) =>
						envelope.settlement.toolUseEventId ===
						"sevt_non_cooperative_route_1",
				),
			).toEqual([]);
			expect(interruptDeclaration).toEqual({ inputKind: "interrupt" });
			expect(
				appended.filter((event) => event.type === "session.error"),
			).toEqual([]);
			expect(order.indexOf("commit:interrupt")).toBeLessThan(
				order.indexOf("event:session.status_idle:end_turn"),
			);
			expect(order.indexOf("event:session.status_idle:end_turn")).toBeLessThan(
				order.indexOf("commit:rin_after_non_cooperative_route"),
			);
			expect(
				order.indexOf("commit:rin_after_non_cooperative_route"),
			).toBeLessThan(order.indexOf("provider:2"));
			expect(providerCalls).toBe(2);
			expect(toolRunnerCalls).toBe(1);
			const eventCountAtCloseout = appended.length;
			const storeOperationCountAtCloseout = storeOrder.length;
			const snapshotAtCloseout = await Effect.runPromise(
				manager.inspectThread(postFenceInput),
			);
			lateRoute.resolve({
				type: "completed",
				output: { text: "late non-cooperative output", truncated: false },
				attachments: [
					{
						transient: {
							attachmentRef: "att_late_non_cooperative",
							sourcePath: "tool:late-non-cooperative.png",
							pageRange: "",
							detail: "auto",
						},
						fileBacked: undefined,
						mime: "image/png",
						filename: "late-non-cooperative.png",
					},
				],
				backgroundTask: { taskId: "task_late_non_cooperative" },
			});
			await flushMicrotasks(50);
			expect(appended).toHaveLength(eventCountAtCloseout);
			expect(storeOrder).toHaveLength(storeOperationCountAtCloseout);
			expect(providerCalls).toBe(2);
			expect(toolRunnerCalls).toBe(1);
			expect(
				await Effect.runPromise(manager.inspectThread(postFenceInput)),
			).toEqual(snapshotAtCloseout);
			expect(JSON.stringify(snapshotAtCloseout)).not.toContain(
				"late non-cooperative output",
			);
			expect(JSON.stringify(snapshotAtCloseout)).not.toContain(
				"att_late_non_cooperative",
			);
			expect(JSON.stringify(snapshotAtCloseout)).not.toContain(
				"task_late_non_cooperative",
			);
		} finally {
			jest.useRealTimers();
			lateRoute.resolve({
				type: "completed",
				output: { text: "cleanup", truncated: false },
				attachments: [
					{
						transient: {
							attachmentRef: "att_cleanup",
							sourcePath: "tool:cleanup.png",
							pageRange: "",
							detail: "auto",
						},
						fileBacked: undefined,
						mime: "image/png",
						filename: "cleanup.png",
					},
				],
				backgroundTask: { taskId: "task_cleanup" },
			});
			await Effect.runPromise(Scope.close(scope, Exit.void));
		}
	});
	test("SessionManager interrupts rehydrated approved tools, repairs every open sibling, and preserves a pre-fence settlement", async () => {
		const loadedMessage = sealedToolContextEntry("mrq_rehydrated_approved", 2, [
			{
				modelToolCallId: "tool-approved-settled",
				toolName: "Write",
				canonicalInput: { file_path: "src/settled.ts", content: "settled" },
			},
			{
				modelToolCallId: "tool-approved-late",
				toolName: "Write",
				canonicalInput: { file_path: "src/late.ts", content: "late" },
			},
		]);
		const pendingToolUses = [
			{
				toolUseEventId: "sevt_approved_settled",
				modelRequestId: "mrq_rehydrated_approved",
				modelToolCallId: "tool-approved-settled",
				toolName: "Write",
				input: { file_path: "src/settled.ts", content: "settled" },
				status: "resolving" as const,
				decision: "allow" as const,
			},
			{
				toolUseEventId: "sevt_approved_late",
				modelRequestId: "mrq_rehydrated_approved",
				modelToolCallId: "tool-approved-late",
				toolName: "Write",
				input: { file_path: "src/late.ts", content: "late" },
				status: "resolving" as const,
				decision: "allow" as const,
			},
		];
		const loader = new QueuedContextLoader([], []);
		const lateRoute = deferred<RuntimeToolExecutionResult>();
		const lateRouteStarted = deferred<void>();
		const settledResultAcked = deferred<void>();
		const abortObserved = deferred<void>();
		const appended: SessionEvent[] = [];
		const settlements: SessionEventWriterToolSettlementEnvelope[] = [];
		const order: string[] = [];
		const storeOrder: string[] = [];
		const durableSequence: TestDurableSequence = {
			eventSequence: 100000,
			messageSequence: 100000,
		};
		const store = new ThreadLoopRuntimeStore(
			storeOrder,
			false,
			false,
			undefined,
			undefined,
			durableSequence,
		);
		let observedLateSignal: AbortSignal | undefined;
		let providerCalls = 0;
		const agentLayer = runtimeThreadLoopLayer(loader, {
			store,
			writer: writerFrom(
				(envelope) => {
					appended.push(envelope.event);
					order.push(`event:${envelope.event.type}`);
					return {
						ok: true,
						eventId: `bridge-${envelope.writeId}`,
						type: "committed",
						eventSequence: 1,
					};
				},
				undefined,
				[
					{
						sessionThreadId: "thrd_1",
						message: loadedMessage,
					},
				],
				durableSequence,
				async (envelope) => {
					settlements.push(envelope);
					order.push(`settlement:${envelope.settlement.toolUseEventId}`);
					if (envelope.settlement.toolUseEventId === "sevt_approved_settled") {
						settledResultAcked.resolve();
					}
					return { ok: true, result: { type: "committed" } };
				},
			),
			llmService: {
				stream() {
					providerCalls++;
					return Stream.empty;
				},
			},
			providerCallRuntime: {
				systemInstructions: "rehydrated approved tool interrupt test",
				toolCatalog: catalogForTest({
					name: "Write",
					description: "Write file",
					inputSchema: { type: "object" },
					permissionPolicy: "always_ask",
				}),
			},
			runTool: (request) => {
				if (request.modelToolCallId === "tool-approved-settled") {
					return {
						type: "completed",
						output: { text: "settled before interrupt", truncated: false },
					};
				}
				observedLateSignal = request.abortSignal;
				request.abortSignal.addEventListener(
					"abort",
					() => abortObserved.resolve(),
					{ once: true },
				);
				lateRouteStarted.resolve();
				return lateRoute.promise;
			},
		});
		const managerLayer = SessionManager.layer({
			maxLocalSessions: 4,
			now: () => createdAt,
		}).pipe(Layer.provide(agentLayer));
		const { manager, scope } = await Effect.runPromise(
			Effect.gen(function* () {
				const layerScope = yield* Scope.make();
				const context = yield* Layer.buildWithScope(managerLayer, layerScope);
				return {
					manager: Context.get(context, SessionManager.Service),
					scope: layerScope,
				};
			}),
		);
		let interrupt: Promise<unknown> | undefined;
		let interruptDeclaration: RuntimeControlInputDeclaration | undefined;
		try {
			const input = {
				...acceptedInput("rin_rehydrated_approved"),
				contentJson: JSON.stringify({
					messages: [
						userMessage("user-rehydrated-input", 3, "continue after tools"),
					],
				}),
			};
			await Effect.runPromise(
				manager.preloadThread({
					...input,
					runtimeBindingToken: "runtime-binding-token",
					contextEntries: [
						userMessage("user-rehydrated-approved", 1, "resume approved tools"),
						loadedMessage,
					],
					pendingToolUses,
					turnCheckpoint: {
						pendingInputContextSequences: [],
						request: {
							modelRequestId: "mrq_rehydrated_approved",
							requestStartEventId: "sevt_rehydrated_request_start",
							requestKind: "agent_provider_request",
							contextThroughMessageSequence: 1,
							toolMembers: pendingToolUses.map((pending) => ({
								memberKind: "public_tool_use" as const,
								modelToolCallId: pending.modelToolCallId,
								toolUseEventId: pending.toolUseEventId,
								toolName: pending.toolName,
							})),
							requestEnd: {
								eventId: "sevt_rehydrated_request_end",
								isError: false,
							},
						},
					},
					turnToolRouteView: {
						routes: pendingToolUses.map((pending) => ({
							toolUseEventId: pending.toolUseEventId,
							disposition: "resume_approval_settlement" as const,
						})),
					},
					thread: {
						role: "main",
						visibility: "public",
						agentType: "general",
						status: "idle",
					},
				}),
			);
			await Effect.runPromise(manager.acceptInput(input));
			await Promise.all([lateRouteStarted.promise, settledResultAcked.promise]);
			const interruptCommand = interruptInput(
				"rin_rehydrated_approved_interrupt",
				9,
			);
			let interruptSettled = false;
			interrupt = Effect.runPromise(
				manager.interruptControl(
					"sesn_1",
					interruptCommand,
					async (declaration) => {
						interruptDeclaration = declaration;
						order.push("commit:interrupt");
						return interruptCommitResult(interruptCommand, declaration, [
							"sevt_approved_late",
						]);
					},
				),
			);
			void interrupt.then(() => {
				interruptSettled = true;
			});
			await new Promise<void>((resolve) => setImmediate(resolve));
			await flushMicrotasks();
			expect(observedLateSignal?.aborted).toBe(true);
			await abortObserved.promise;
			expect(await interrupt).toMatchObject({ ok: true, interrupted: true });
			expect(interruptSettled).toBe(true);
			const settledResults = settlements.filter(
				(envelope) =>
					envelope.settlement.toolUseEventId === "sevt_approved_settled",
			);
			const repairedResults = settlements.filter(
				(envelope) =>
					envelope.settlement.toolUseEventId === "sevt_approved_late",
			);
			expect(settledResults).toHaveLength(1);
			expect(settledResults[0]?.settlement.outcome).toMatchObject({
				type: "completed",
			});
			expect(repairedResults).toEqual([]);
			expect(interruptDeclaration).toEqual({ inputKind: "interrupt" });
			expect(
				appended.filter((event) => event.type === "session.error"),
			).toEqual([]);
			expect(order.indexOf("commit:interrupt")).toBeLessThan(
				order.indexOf("event:session.status_idle"),
			);
			expect(providerCalls).toBe(0);
			const eventCountAtCloseout = appended.length;
			const storeOperationCountAtCloseout = storeOrder.length;
			const snapshotAtCloseout = await Effect.runPromise(
				manager.inspectThread(interruptCommand),
			);
			lateRoute.resolve({
				type: "completed",
				output: { text: "late approved output", truncated: false },
				attachments: [
					{
						transient: {
							attachmentRef: "att_late_approved",
							sourcePath: "tool:late-approved.png",
							pageRange: "",
							detail: "auto",
						},
						fileBacked: undefined,
						mime: "image/png",
						filename: "late-approved.png",
					},
				],
				backgroundTask: { taskId: "task_late_approved" },
			});
			await flushMicrotasks(50);
			expect(appended).toHaveLength(eventCountAtCloseout);
			expect(storeOrder).toHaveLength(storeOperationCountAtCloseout);
			expect(providerCalls).toBe(0);
			expect(
				await Effect.runPromise(manager.inspectThread(interruptCommand)),
			).toEqual(snapshotAtCloseout);
			expect(JSON.stringify(snapshotAtCloseout)).not.toContain(
				"late approved output",
			);
			expect(JSON.stringify(snapshotAtCloseout)).not.toContain(
				"att_late_approved",
			);
			expect(JSON.stringify(snapshotAtCloseout)).not.toContain(
				"task_late_approved",
			);
		} finally {
			jest.useRealTimers();
			lateRoute.resolve({
				type: "completed",
				output: { text: "cleanup", truncated: false },
				attachments: [
					{
						transient: {
							attachmentRef: "att_cleanup",
							sourcePath: "tool:cleanup.png",
							pageRange: "",
							detail: "auto",
						},
						fileBacked: undefined,
						mime: "image/png",
						filename: "cleanup.png",
					},
				],
				backgroundTask: { taskId: "task_cleanup" },
			});
			if (interrupt !== undefined) {
				await interrupt.catch(() => undefined);
			}
			await Effect.runPromise(Scope.close(scope, Exit.void));
		}
	});
	test("runtime shutdown joins durable Sandbox acceptance before freezing execution ownership", async () => {
		const session = new ThreadRuntime("sesn_1");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const acceptanceStarted = deferred<void>();
		const releaseAcceptance = deferred<void>();
		let awaitCalls = 0;
		const catalog = catalogForTest({
			name: "Write",
			description: "Write file",
			inputSchema: { type: "object" },
		});
		const runFiber = Effect.runFork(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						events: [
							{
								type: "tool-call",
								id: "tool-accept-fence",
								toolName: "Write",
								input: { file_path: "src/a.ts", content: "one" },
								inputPreview: { preview: "{}", truncated: false },
							},
							{ type: "finish", finishReason: "tool-calls" },
						],
						providerCallRuntime: {
							systemInstructions: "sandbox acceptance fence test",
							toolCatalog: {
								...catalog,
								entries: catalog.entries.map((entry) => ({
									...entry,
									route: {
										kind: "sandbox" as const,
										operation: "RunTool" as const,
										helperSubcommand: "write" as const,
									},
								})),
							},
						},
						acceptSandboxExecution: async () => {
							acceptanceStarted.resolve();
							await releaseAcceptance.promise;
							return { type: "accepted" };
						},
						awaitSandboxExecution: () => {
							awaitCalls += 1;
							return {
								type: "completed",
								output: {
									text: "must not wait after shutdown",
									truncated: false,
								},
							};
						},
					}),
				),
			),
		);
		await acceptanceStarted.promise;
		session.state.beginRuntimeShutdown();
		let interruptSettled = false;
		const interrupted = Effect.runPromise(Fiber.interrupt(runFiber)).then(
			(exit) => {
				interruptSettled = true;
				return exit;
			},
		);
		await flushMicrotasks(20);
		expect(interruptSettled).toBe(false);
		releaseAcceptance.resolve();
		await interrupted;
		expect(awaitCalls).toBe(0);
		expect(session.state.pendingSandboxExecutionJobs()).toHaveLength(1);
		expect(session.state.pendingApprovalToolJobs()).toHaveLength(0);
	});
	test("user interrupt joins an unknown Sandbox acceptance ACK before taking its closeout snapshot", async () => {
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const acceptanceStarted = deferred<void>();
		const releaseAcceptance = deferred<void>();
		const appended: SessionEvent[] = [];
		const settlements: SessionEventWriterToolSettlementEnvelope[] = [];
		let awaitCalls = 0;
		let interruptCommitStarted = false;
		let interruptDeclaration: RuntimeControlInputDeclaration | undefined;
		const catalog = catalogForTest({
			name: "Write",
			description: "Write file",
			inputSchema: { type: "object" },
		});
		const agentLayer = runtimeThreadLoopLayer(loader, {
			writer: writerFrom(
				(envelope) => {
					appended.push(envelope.event);
					return {
						ok: true,
						eventId:
							envelope.event.type === "agent.tool_use"
								? "sevt_interrupt_acceptance"
								: `bridge-${envelope.writeId}`,
						type: "committed",
						eventSequence: 1,
					};
				},
				undefined,
				[],
				undefined,
				async (envelope) => {
					settlements.push(envelope);
					return { ok: true, result: { type: "committed" } };
				},
			),
			events: [
				{
					type: "tool-call",
					id: "tool-interrupt-acceptance",
					toolName: "Write",
					input: { file_path: "src/a.ts", content: "one" },
					inputPreview: { preview: "{}", truncated: false },
				},
				{ type: "finish", finishReason: "tool-calls" },
			],
			providerCallRuntime: {
				systemInstructions: "sandbox acceptance interrupt test",
				toolCatalog: {
					...catalog,
					entries: catalog.entries.map((entry) => ({
						...entry,
						route: {
							kind: "sandbox" as const,
							operation: "RunTool" as const,
							helperSubcommand: "write" as const,
						},
					})),
				},
			},
			acceptSandboxExecution: async () => {
				acceptanceStarted.resolve();
				await releaseAcceptance.promise;
				return { type: "accepted" };
			},
			awaitSandboxExecution: () => {
				awaitCalls += 1;
				return {
					type: "completed",
					output: { text: "must not wait after interrupt", truncated: false },
				};
			},
		});
		const managerLayer = SessionManager.layer({
			maxLocalSessions: 4,
			now: () => createdAt,
		}).pipe(Layer.provide(agentLayer));
		const { manager, scope } = await Effect.runPromise(
			Effect.gen(function* () {
				const layerScope = yield* Scope.make();
				const context = yield* Layer.buildWithScope(managerLayer, layerScope);
				return {
					manager: Context.get(context, SessionManager.Service),
					scope: layerScope,
				};
			}),
		);
		try {
			const input = {
				...acceptedInput("rin_interrupt_acceptance"),
				contentJson: JSON.stringify({
					messages: [userMessage("user-1", 1, "hello")],
				}),
			};
			await Effect.runPromise(
				manager.preloadThread({
					...input,
					runtimeBindingToken: "runtime-binding-token",
					contextEntries: [userMessage("user-1", 1, "hello")],
					thread: {
						role: "main",
						visibility: "public",
						agentType: "general",
						status: "idle",
					},
				}),
			);
			await Effect.runPromise(manager.acceptInput(input));
			await acceptanceStarted.promise;
			const command = interruptInput("rin_interrupt_acceptance_control", 9);
			const interrupt = Effect.runPromise(
				manager.interruptControl("sesn_1", command, async (declaration) => {
					interruptCommitStarted = true;
					interruptDeclaration = declaration;
					return interruptCommitResult(command, declaration, [
						"sevt_interrupt_acceptance",
					]);
				}),
			);
			await new Promise<void>((resolve) => setImmediate(resolve));
			await flushMicrotasks(20);
			expect(interruptCommitStarted).toBe(false);
			releaseAcceptance.resolve();
			await expect(interrupt).resolves.toMatchObject({
				ok: true,
				interrupted: true,
			});
			expect(interruptDeclaration).toEqual({ inputKind: "interrupt" });
			expect(awaitCalls).toBe(0);
			expect(settlements).toEqual([]);
			expect(
				appended.filter((event) => event.type === "session.error"),
			).toEqual([]);
		} finally {
			releaseAcceptance.resolve();
			await Effect.runPromise(Scope.close(scope, Exit.void));
		}
	});
	test("provider closeout joins durable Sandbox acceptance before freezing execution ownership", async () => {
		const session = new ThreadRuntime("sesn_1");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const acceptanceStarted = deferred<void>();
		const releaseAcceptance = deferred<void>();
		const requestEndStarted = deferred<void>();
		const appended: SessionEvent[] = [];
		const settlements: SessionEventWriterToolSettlementEnvelope[] = [];
		let awaitCalls = 0;
		const catalog = catalogForTest({
			name: "Write",
			description: "Write file",
			inputSchema: { type: "object" },
		});
		const baseWriter = writerFrom((envelope) => {
			appended.push(envelope.event);
			return {
				ok: true,
				eventId:
					envelope.event.type === "agent.tool_use"
						? "sevt_provider_closeout"
						: `bridge-${envelope.writeId}`,
				type: "committed",
				eventSequence: 1,
			};
		});
		const writer: SessionEventWriter = {
			...baseWriter,
			settleToolResult: async (envelope) => {
				settlements.push(envelope);
				return await baseWriter.settleToolResult(envelope);
			},
			writeRequestEnd: async (envelope) => {
				requestEndStarted.resolve();
				return await baseWriter.writeRequestEnd(envelope);
			},
		};
		const runPromise = Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						llmService: {
							stream() {
								return Stream.fromAsyncIterable(
									(async function* () {
										yield {
											type: "tool-call" as const,
											id: "tool-provider-closeout",
											toolName: "Write",
											input: { file_path: "src/a.ts", content: "one" },
											inputPreview: { preview: "{}", truncated: false },
										};
										await acceptanceStarted.promise;
										yield {
											type: "provider-error" as const,
											error: runtimeFailureFromProviderError(
												normalizeProviderError({
													code: "provider_stream_error",
													message: "provider failed during sandbox acceptance",
													retryable: false,
													providerId: "fake",
													modelId: "fake-chat",
												}),
											),
										};
									})(),
									(error): LLMServiceError => ({
										type: "llm-service",
										error: runtimeFailureFromProviderError(
											normalizeProviderError({
												code: "provider_stream_error",
												message: String(error),
												retryable: false,
											}),
										),
									}),
								);
							},
						},
						providerCallRuntime: {
							systemInstructions: "sandbox acceptance provider closeout test",
							toolCatalog: {
								...catalog,
								entries: catalog.entries.map((entry) => ({
									...entry,
									route: {
										kind: "sandbox" as const,
										operation: "RunTool" as const,
										helperSubcommand: "write" as const,
									},
								})),
							},
						},
						acceptSandboxExecution: async () => {
							acceptanceStarted.resolve();
							await releaseAcceptance.promise;
							return { type: "accepted" };
						},
						awaitSandboxExecution: () => {
							awaitCalls += 1;
							return {
								type: "completed",
								output: {
									text: "must not wait after closeout",
									truncated: false,
								},
							};
						},
					}),
				),
			),
		);
		await requestEndStarted.promise;
		let settled = false;
		void runPromise.finally(() => {
			settled = true;
		});
		await flushMicrotasks(20);
		expect(settled).toBe(false);
		releaseAcceptance.resolve();
		await expect(runPromise).resolves.toMatchObject({ type: "failed" });
		expect(awaitCalls).toBe(1);
		expect(session.state.pendingSandboxExecutionJobs()).toHaveLength(0);
		expect(settlements).toHaveLength(1);
	});
	test("runtime shutdown aborts active ToolFiber route execution", async () => {
		const session = new ThreadRuntime("sesn_1");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const releaseProvider = deferred<void>();
		const releaseTool = deferred<void>();
		let observedToolSignal: AbortSignal | undefined;
		const order: string[] = [];
		const service: LLMServiceInterface = {
			stream(_request, options) {
				return Stream.fromAsyncIterable(
					(async function* () {
						yield {
							type: "tool-call" as const,
							id: "tool-1",
							toolName: "Write",
							input: { file_path: "src/a.ts", content: "one" },
							inputPreview: {
								preview: '{"file_path":"src/a.ts"}',
								truncated: false,
							},
						};
						if (options?.abortSignal === undefined) {
							throw new Error("provider stream requires an abort signal");
						}
						await waitForReleaseOrAbort(
							releaseProvider.promise,
							options.abortSignal,
						);
						yield {
							type: "finish" as const,
							finishReason: "tool-calls" as const,
						};
					})(),
					(error): LLMServiceError => ({
						type: "llm-service",
						error: runtimeFailureFromProviderError(
							normalizeProviderError({
								code: "provider_stream_error",
								message: String(error),
								retryable: true,
							}),
						),
					}),
				);
			},
		};
		const runFiber = Effect.runFork(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						llmService: service,
						writer: writerFrom(
							(envelope) => {
								order.push(`event:${envelope.event.type}`);
								return {
									ok: true,
									eventId: `bridge-${envelope.writeId}`,
									type: "committed",
									eventSequence: 1,
								};
							},
							undefined,
							[],
							undefined,
							async (envelope) => {
								order.push(`settlement:${envelope.settlement.toolUseEventId}`);
								return { ok: true, result: { type: "committed" } };
							},
						),
						providerCallRuntime: {
							systemInstructions: "tool cancellation test system",
							toolCatalog: catalogForTest({
								name: "Write",
								description: "Write file",
								inputSchema: { type: "object" },
							}),
						},
						runTool: (request) => {
							observedToolSignal = request.abortSignal;
							return new Promise((resolve) => {
								request.abortSignal.addEventListener(
									"abort",
									() => {
										order.push("tool-abort-signalled");
										void releaseTool.promise.then(() => {
											order.push("tool-route-settled");
											resolve({
												type: "completed" as const,
												output: { text: "late", truncated: false },
											});
										});
									},
									{ once: true },
								);
							});
						},
					}),
				),
			),
		);
		await waitForCondition(
			() => observedToolSignal !== undefined,
			"tool route signal",
		);
		session.state.beginRuntimeShutdown();
		let shutdownCompleted = false;
		const shutdown = Effect.runPromise(Fiber.interrupt(runFiber)).then(
			(result) => {
				shutdownCompleted = true;
				return result;
			},
		);
		await waitForCondition(
			() => observedToolSignal?.aborted === true,
			"tool route abort",
		);
		releaseProvider.resolve(undefined);
		await flushMicrotasks(50);
		expect(shutdownCompleted).toBe(false);
		releaseTool.resolve(undefined);
		await shutdown;
		const runExit = await Effect.runPromise(Fiber.await(runFiber));
		expect(
			Exit.isFailure(runExit) && Cause.hasInterruptsOnly(runExit.cause),
		).toBe(true);
		expect(order.indexOf("tool-abort-signalled")).toBeGreaterThan(-1);
		expect(order).not.toContain("event:span.model_request_end");
		expect(order.some((entry) => entry.startsWith("settlement:"))).toBe(false);
	});
	test("approve_for_me runs reviewer before public tool_use is written", async () => {
		const session = new ThreadRuntime(
			"sesn_1",
			new AutoApprovalReviewerManager(),
		);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const order: string[] = [];
		const appended: SessionEvent[] = [];
		const writer = writerFrom((envelope) => {
			order.push(`event:${envelope.event.type}`);
			appended.push(envelope.event);
			return {
				ok: true,
				eventId:
					envelope.event.type === "agent.tool_use"
						? "bridge-tool"
						: `bridge-${envelope.writeId}`,
				type: "committed",
				eventSequence: 1,
			};
		});
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						approvalMode: "approve_for_me",
						events: [
							{
								type: "tool-call",
								id: "tool-1",
								toolName: "Write",
								input: { file_path: "src/a.ts", content: "ok" },
								inputPreview: {
									preview: '{"file_path":"src/a.ts"}',
									truncated: false,
								},
							},
							{ type: "finish", finishReason: "tool-calls" },
						],
						providerCallRuntime: {
							systemInstructions: "approval reviewer test system",
							toolCatalog: catalogForTest({
								name: "Write",
								description: "Write file",
								inputSchema: { type: "object" },
								permissionPolicy: "always_ask",
							}),
						},
						reviewApproval: (request) =>
							Effect.sync(() => {
								order.push(`review:${request.targetModelToolCallId}`);
								expect(
									appended.some((event) => event.type === "agent.tool_use"),
								).toBe(false);
								return {
									type: "decision" as const,
									riskLevel: "low" as const,
									userAuthorization: "high" as const,
									outcome: "allow" as const,
								};
							}),
						runTool: () => {
							order.push("run-tool");
							return {
								type: "completed",
								output: { text: "done", truncated: false },
							};
						},
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed" });
		expect(order.indexOf("review:tool-1")).toBeLessThan(
			order.indexOf("event:agent.tool_use"),
		);
		expect(order.indexOf("event:agent.tool_use")).toBeLessThan(
			order.indexOf("run-tool"),
		);
		expect(appended).toContainEqual(
			expect.objectContaining({
				type: "agent.tool_use",
				evaluated_permission: "allow",
			}),
		);
	});
	test("approve_for_me reviewer failure falls back to public ask approval", async () => {
		const session = new ThreadRuntime(
			"sesn_1",
			new AutoApprovalReviewerManager(),
		);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const appended: SessionEvent[] = [];
		let runToolCalls = 0;
		let toolUseIndex = 0;
		const writer = writerFrom((envelope) => {
			appended.push(envelope.event);
			return {
				ok: true,
				eventId:
					envelope.event.type === "agent.tool_use"
						? `sevt_tool_${++toolUseIndex}`
						: `bridge-${envelope.writeId}`,
				type: "committed",
				eventSequence: 1,
			};
		});
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						approvalMode: "approve_for_me",
						events: [
							{
								type: "tool-call",
								id: "tool-1",
								toolName: "Write",
								input: { file_path: "src/a.ts", content: "ok" },
								inputPreview: {
									preview: '{"file_path":"src/a.ts"}',
									truncated: false,
								},
							},
							{ type: "finish", finishReason: "tool-calls" },
						],
						providerCallRuntime: {
							systemInstructions: "approval reviewer fallback test system",
							toolCatalog: catalogForTest({
								name: "Write",
								description: "Write file",
								inputSchema: { type: "object" },
								permissionPolicy: "always_ask",
							}),
						},
						reviewApproval: () => Effect.succeed({ type: "failed" }),
						runTool: () => {
							runToolCalls += 1;
							return {
								type: "completed",
								output: { text: "should not run", truncated: false },
							};
						},
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed" });
		expect(runToolCalls).toBe(0);
		expect(appended.filter((event) => event.type === "agent.tool_use")).toEqual(
			[
				expect.objectContaining({
					type: "agent.tool_use",
					evaluated_permission: "ask",
				}),
			],
		);
		expect(appended.at(-1)).toEqual({
			type: "session.status_idle",
			stop_reason: { type: "requires_action", event_ids: ["sevt_tool_1"] },
		});
	});
	test("missing reviewer infrastructure fails progression without publishing a fallback Tool Use", async () => {
		const session = new ThreadRuntime("sesn_1");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const appended: SessionEvent[] = [];
		let runToolCalls = 0;
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer: writerFrom((envelope) => {
							appended.push(envelope.event);
							return {
								ok: true,
								eventId: `bridge-${envelope.writeId}`,
								type: "committed",
								eventSequence: 1,
							};
						}),
						approvalMode: "approve_for_me",
						events: [
							{
								type: "tool-call",
								id: "tool-1",
								toolName: "Write",
								input: { file_path: "src/a.ts", content: "ok" },
								inputPreview: { preview: "{}", truncated: false },
							},
							{ type: "finish", finishReason: "tool-calls" },
						],
						providerCallRuntime: {
							systemInstructions: "approval reviewer unavailable test system",
							toolCatalog: catalogForTest({
								name: "Write",
								description: "Write file",
								inputSchema: { type: "object" },
								permissionPolicy: "always_ask",
							}),
						},
						reviewApproval: () => Effect.succeed({ type: "failed" as const }),
						runTool: () => {
							runToolCalls += 1;
							return {
								type: "completed",
								output: { text: "must not run", truncated: false },
							};
						},
					}),
				),
			),
		);

		expect(result).toMatchObject({
			type: "failed",
			error: {
				type: "session-event-writer",
				code: "unknown",
				retryable: false,
			},
			releaseSession: { reason: "event_write_failed" },
		});
		expect(runToolCalls).toBe(0);
		expect(
			appended.some(
				(event) =>
					event.type === "agent.tool_use" ||
					event.type === "agent.mcp_tool_use",
			),
		).toBe(false);
	});
	test("an unexpected reviewer defect fails parent progression without publishing or caching approval", async () => {
		const reviewerManager = new AutoApprovalReviewerManager();
		const rememberDecision = jest.spyOn(reviewerManager, "rememberDecision");
		const session = new ThreadRuntime("sesn_1", reviewerManager);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const appended: SessionEvent[] = [];
		let runToolCalls = 0;
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer: writerFrom((envelope) => {
							appended.push(envelope.event);
							return {
								ok: true,
								eventId: `bridge-${envelope.writeId}`,
								type: "committed",
								eventSequence: 1,
							};
						}),
						approvalMode: "approve_for_me",
						events: [
							{
								type: "tool-call",
								id: "tool-1",
								toolName: "Write",
								input: { file_path: "src/a.ts", content: "ok" },
								inputPreview: { preview: "{}", truncated: false },
							},
							{ type: "finish", finishReason: "tool-calls" },
						],
						providerCallRuntime: {
							systemInstructions: "approval reviewer defect safety test system",
							toolCatalog: catalogForTest({
								name: "Write",
								description: "Write file",
								inputSchema: { type: "object" },
								permissionPolicy: "always_ask",
							}),
						},
						reviewApproval: () =>
							Effect.die(new Error("unexpected reviewer defect")),
						runTool: () => {
							runToolCalls += 1;
							return {
								type: "completed",
								output: { text: "must not run", truncated: false },
							};
						},
					}),
				),
			),
		);

		expect(result).toMatchObject({
			type: "failed",
			error: {
				type: "session-event-writer",
				code: "unknown",
				retryable: false,
			},
			releaseSession: { reason: "event_write_failed" },
		});
		expect(runToolCalls).toBe(0);
		expect(rememberDecision).toHaveBeenCalledTimes(0);
		expect(
			appended.some(
				(event) =>
					event.type === "agent.tool_use" ||
					event.type === "agent.mcp_tool_use",
			),
		).toBe(false);
		rememberDecision.mockRestore();
	});
	test("reviewer settlement failure closes the parent without publishing or executing its tool", async () => {
		const session = new ThreadRuntime(
			"sesn_1",
			new AutoApprovalReviewerManager(),
		);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const appended: SessionEvent[] = [];
		let runToolCalls = 0;
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer: writerFrom((envelope) => {
							appended.push(envelope.event);
							return {
								ok: true,
								eventId: `bridge-${envelope.writeId}`,
								type: "committed",
								eventSequence: 1,
							};
						}),
						approvalMode: "approve_for_me",
						events: [
							{
								type: "tool-call",
								id: "tool-1",
								toolName: "Write",
								input: { file_path: "src/a.ts", content: "ok" },
								inputPreview: { preview: "{}", truncated: false },
							},
							{ type: "finish", finishReason: "tool-calls" },
						],
						providerCallRuntime: {
							systemInstructions: "approval reviewer settlement test system",
							toolCatalog: catalogForTest({
								name: "Write",
								description: "Write file",
								inputSchema: { type: "object" },
								permissionPolicy: "always_ask",
							}),
						},
						reviewApproval: () =>
							Effect.succeed({
								type: "settlement_failed" as const,
								error: normalizeSessionEventWriterError({
									code: "schema_mismatch",
									sessionId: "sesn_1",
									writeId: "rwrite_reviewer_failure",
								}),
							}),
						runTool: () => {
							runToolCalls += 1;
							return {
								type: "completed",
								output: { text: "must not run", truncated: false },
							};
						},
					}),
				),
			),
		);

		expect(result).toMatchObject({
			type: "failed",
			error: {
				type: "session-event-writer",
				code: "schema_mismatch",
				retryable: false,
			},
			releaseSession: { reason: "event_write_failed" },
		});
		expect(runToolCalls).toBe(0);
		expect(
			appended.some(
				(event) =>
					event.type === "agent.tool_use" ||
					event.type === "agent.mcp_tool_use",
			),
		).toBe(false);
	});
	test("approval reviewer stale custody stops the turn and discards HotState", async () => {
		const session = new ThreadRuntime(
			"sesn_1",
			new AutoApprovalReviewerManager(),
		);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const appended: SessionEvent[] = [];
		let runToolCalls = 0;
		const writer = writerFrom((envelope) => {
			appended.push(envelope.event);
			return {
				ok: true,
				eventId: `bridge-${envelope.writeId}`,
				type: "committed",
				eventSequence: 1,
			};
		});
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						approvalMode: "approve_for_me",
						events: [
							{
								type: "tool-call",
								id: "tool-1",
								toolName: "Write",
								input: { file_path: "src/a.ts", content: "ok" },
								inputPreview: {
									preview: '{"file_path":"src/a.ts"}',
									truncated: false,
								},
							},
							{ type: "finish", finishReason: "tool-calls" },
						],
						providerCallRuntime: {
							systemInstructions: "approval reviewer stale custody test system",
							toolCatalog: catalogForTest({
								name: "Write",
								description: "Write file",
								inputSchema: { type: "object" },
								permissionPolicy: "always_ask",
							}),
						},
						reviewApproval: () =>
							Effect.succeed({ type: "stale_custody" as const }),
						runTool: () => {
							runToolCalls++;
							return {
								type: "completed",
								output: { text: "must not run", truncated: false },
							};
						},
					}),
				),
			),
		);
		expect(runToolCalls).toBe(0);
		expect(appended.some((event) => event.type === "session.error")).toBe(false);
		expect(result).toEqual({ type: "interrupted", discardHotState: true });
		expect(appended.some((event) => event.type === "agent.tool_use")).toBe(
			false,
		);
	});
	test("approve_for_me reviewer receives current request-turn draft state", async () => {
		const session = new ThreadRuntime(
			"sesn_1",
			new AutoApprovalReviewerManager(),
		);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const order: string[] = [];
		const appended: SessionEvent[] = [];
		let reviewerRequest: RuntimeApprovalReviewRequest | undefined;
		const writer = writerFrom((envelope) => {
			order.push(`event:${envelope.event.type}`);
			appended.push(envelope.event);
			return {
				ok: true,
				eventId:
					envelope.event.type === "agent.tool_use"
						? "bridge-tool"
						: `bridge-${envelope.writeId}`,
				type: "committed",
				eventSequence: 1,
			};
		});
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						approvalMode: "approve_for_me",
						events: [
							{ type: "text-start", id: "text-1" },
							{
								type: "text-delta",
								id: "text-1",
								text_delta: "I will update the file before calling the tool.",
							},
							{ type: "text-end", id: "text-1" },
							{ type: "reasoning-start", id: "reasoning-1" },
							{
								type: "reasoning-delta",
								id: "reasoning-1",
								text_delta: "Need to patch one file.",
							},
							{ type: "reasoning-end", id: "reasoning-1" },
							{
								type: "tool-call",
								id: "tool-1",
								toolName: "Write",
								input: { file_path: "src/a.ts", content: "ok" },
								inputPreview: {
									preview: '{"file_path":"src/a.ts"}',
									truncated: false,
								},
							},
							{ type: "finish", finishReason: "tool-calls" },
						],
						providerCallRuntime: {
							systemInstructions: "approval reviewer test system",
							toolCatalog: catalogForTest({
								name: "Write",
								description: "Write file",
								inputSchema: { type: "object" },
								permissionPolicy: "always_ask",
							}),
						},
						reviewApproval: (request) =>
							Effect.sync(() => {
								order.push(`review:${request.targetModelToolCallId}`);
								reviewerRequest = request;
								return {
									type: "decision" as const,
									riskLevel: "low" as const,
									userAuthorization: "high" as const,
									outcome: "allow" as const,
								};
							}),
						runTool: () => ({
							type: "completed",
							output: { text: "done", truncated: false },
						}),
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed" });
		expect(appended.some((event) => event.type === "agent.tool_use")).toBe(
			true,
		);
		expect(reviewerRequest?.currentAssistantDraft).toEqual(
			expect.arrayContaining([
				expect.objectContaining({
					type: "text",
					text: "I will update the file before calling the tool.",
				}),
			]),
		);
		expect(reviewerRequest?.siblingToolCalls).toContainEqual(
			expect.objectContaining({ modelToolCallId: "tool-1", toolName: "Write" }),
		);
		expect(order.indexOf("review:tool-1")).toBeLessThan(
			order.indexOf("event:agent.tool_use"),
		);
	});
	test("ask approval resumes the pending ToolJob instead of rerunning the old ToolFiber", async () => {
		const session = new ThreadRuntime("sesn_1");
		const loader = new QueuedContextLoader(
			[],
			[
				{ type: "context", entries: [userMessage("user-1", 0, "hello")] },
				{ type: "empty" },
			],
		);
		const appended: SessionEvent[] = [];
		const settlements: SessionEventWriterToolSettlementEnvelope[] = [];
		const toolUseEventIds: string[] = [];
		let toolUseIndex = 0;
		const writer = writerFrom(
			(envelope) => {
				appended.push(envelope.event);
				const eventId =
					envelope.event.type === "agent.tool_use"
						? `sevt_tool_${++toolUseIndex}`
						: `bridge-${envelope.writeId}`;
				if (envelope.event.type === "agent.tool_use") {
					toolUseEventIds.push(eventId);
				}
				return { ok: true, eventId, type: "committed", eventSequence: 1 };
			},
			undefined,
			[],
			undefined,
			async (envelope) => {
				settlements.push(envelope);
				return { ok: true, result: { type: "committed" } };
			},
		);
		const requests: LLMRequest[] = [];
		const runToolCalls: string[] = [];
		const sandboxAcceptanceCalls: string[] = [];
		const processors: ProviderStreamAccumulator[] = [];
		const store = new ThreadLoopRuntimeStore([]);
		const layer = runtimeThreadLoopLayer(loader, {
			store,
			writer,
			llmService: queuedLLMService(
				[
					[
						{
							type: "tool-call",
							id: "tool-1",
							toolName: "Write",
							input: { file_path: "src/a.ts", content: "ok" },
							inputPreview: {
								preview: '{"file_path":"src/a.ts"}',
								truncated: false,
							},
						},
						{ type: "finish", finishReason: "tool-calls" },
					],
					[
						{ type: "text-start", id: "text-1" },
						{ type: "text-delta", id: "text-1", text_delta: "after approval" },
						{ type: "text-end", id: "text-1" },
						{ type: "finish", finishReason: "stop" },
					],
				],
				requests,
			),
			providerCallRuntime: {
				systemInstructions: "approval resume test system",
				toolCatalog: {
					...catalogForTest({
						name: "Write",
						description: "Write file",
						inputSchema: { type: "object" },
						permissionPolicy: "always_ask",
					}),
					entries: [
						{
							...catalogForTest({
								name: "Write",
								description: "Write file",
								inputSchema: { type: "object" },
								permissionPolicy: "always_ask",
							}).entries[0]!,
							route: {
								kind: "sandbox" as const,
								operation: "RunTool" as const,
								helperSubcommand: "write" as const,
							},
						},
					],
				},
			},
			runTool: (request) => {
				runToolCalls.push(
					`${request.modelToolCallId}:${request.toolUseEventId}`,
				);
				return {
					type: "completed",
					output: { text: "approved write", truncated: false },
				};
			},
			acceptSandboxExecution: (request) => {
				sandboxAcceptanceCalls.push(
					`${request.modelToolCallId}:${request.toolUseEventId}`,
				);
				expect(session.state.pendingApprovalToolJobs()).toHaveLength(1);
				expect(session.state.pendingSandboxExecutionJobs()).toHaveLength(0);
				return { type: "accepted" };
			},
			createProcessor: (options) => {
				const processor = new ProviderStreamAccumulator(options);
				processors.push(processor);
				return processor;
			},
		});
		const first = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(Effect.provide(layer)),
		);
		expect(first).toMatchObject({ type: "completed" });
		expect(toolUseEventIds).toEqual(["sevt_tool_1"]);
		expect(runToolCalls).toEqual([]);
		expect(sandboxAcceptanceCalls).toEqual([]);
		expect(processors).toHaveLength(1);
		const pendingApproval = session.state.pendingApprovalToolJobs()[0];
		const pendingAssistant = session.state.contextManager
			.entries()
			.find((message) =>
				message.parts.some(
					(part) =>
						part.type === "tool_call" && part.modelToolCallId === "tool-1",
				),
			);
		expect(pendingAssistant?.contextKind).toBe("assistant");
		expect(pendingApproval).not.toHaveProperty("processor");
		expect(appended.at(-1)).toEqual({
			type: "session.status_idle",
			stop_reason: { type: "requires_action", event_ids: ["sevt_tool_1"] },
		});
		expect(
			session.state.resolveToolConfirmation({
				workspaceId: session.identity.workspaceId,
				sessionId: session.identity.sessionId,
				sessionThreadId: session.identity.sessionThreadId,
				bindingId: session.identity.bindingId,
				bindingGeneration: session.identity.bindingGeneration,
				targetPodUid: session.identity.targetPodUid,
				runtimeInputId: "rin_confirm",
				toolUseEventId: "sevt_tool_1",
				decision: "allow",
			}),
		).toBe("applied");
		const second = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(Effect.provide(layer)),
		);
		expect(second).toMatchObject({ type: "completed" });
		expect(requests).toHaveLength(2);
		expect(sandboxAcceptanceCalls).toEqual(["tool-1:sevt_tool_1"]);
		expect(runToolCalls).toEqual(["tool-1:sevt_tool_1"]);
		expect(session.state.pendingApprovalToolJobs()).toHaveLength(0);
		expect(session.state.pendingSandboxExecutionJobs()).toHaveLength(0);
		expect(processors).toHaveLength(2);
		expect(processors[1]).not.toBe(processors[0]);
		expect(settlements).toEqual([
			expect.objectContaining({
				settlement: expect.objectContaining({ toolUseEventId: "sevt_tool_1" }),
			}),
		]);
		expect(appended.map((event) => event.type)).toEqual([
			"session.status_running",
			"span.model_request_start",
			"agent.tool_use",
			"span.model_request_end",
			"session.status_idle",
			"session.status_running",
			"span.model_request_start",
			"agent.message",
			"span.model_request_end",
			"session.status_idle",
		]);
	});
	test("one approval decision settles its named member while sibling approvals remain pending", async () => {
		for (const decision of ["allow", "deny"] as const) {
			const session = new ThreadRuntime(
				`sesn_partial_confirmation_${decision}`,
			);
			const loader = new QueuedContextLoader(
				[],
				[
					{
						type: "context",
						entries: [
							userMessage(`user_partial_${decision}`, 0, "run both tools"),
						],
					},
					{ type: "empty" },
				],
			);
			const appended: SessionEvent[] = [];
			const toolUseEventIds: string[] = [];
			let toolUseIndex = 0;
			const writer = writerFrom((envelope) => {
				appended.push(envelope.event);
				const eventId =
					envelope.event.type === "agent.tool_use"
						? `sevt_partial_${decision}_${++toolUseIndex}`
						: `bridge-${envelope.writeId}`;
				if (envelope.event.type === "agent.tool_use") {
					toolUseEventIds.push(eventId);
				}
				return { ok: true, eventId, type: "committed", eventSequence: 1 };
			});
			const runToolCalls: string[] = [];
			const requests: LLMRequest[] = [];
			const layer = runtimeThreadLoopLayer(loader, {
				writer,
				llmService: queuedLLMService(
					[
						[
							{
								type: "tool-call",
								id: "tool-partial-1",
								toolName: "Write",
								input: { file_path: "src/a.ts", content: "a" },
								inputPreview: {
									preview: '{"file_path":"src/a.ts"}',
									truncated: false,
								},
							},
							{
								type: "tool-call",
								id: "tool-partial-2",
								toolName: "Write",
								input: { file_path: "src/b.ts", content: "b" },
								inputPreview: {
									preview: '{"file_path":"src/b.ts"}',
									truncated: false,
								},
							},
							{ type: "finish", finishReason: "tool-calls" },
						],
					],
					requests,
				),
				providerCallRuntime: {
					systemInstructions: "partial approval settlement test system",
					toolCatalog: catalogForTest({
						name: "Write",
						description: "Write file",
						inputSchema: { type: "object" },
						permissionPolicy: "always_ask",
					}),
				},
				runTool: (request) => {
					runToolCalls.push(request.modelToolCallId);
					return {
						type: "completed",
						output: { text: "approved write", truncated: false },
					};
				},
			});
			const run = async () =>
				await Effect.runPromise(
					Effect.gen(function* () {
						return yield* (yield* ThreadLoop.Service).run(
							session,
							testRunCustody(),
						);
					}).pipe(Effect.provide(layer)),
				);

			expect(await run()).toMatchObject({ type: "completed" });
			expect(toolUseEventIds).toHaveLength(2);
			expect(
				session.state.resolveToolConfirmation({
					workspaceId: session.identity.workspaceId,
					sessionId: session.identity.sessionId,
					sessionThreadId: session.identity.sessionThreadId,
					bindingId: session.identity.bindingId,
					bindingGeneration: session.identity.bindingGeneration,
					targetPodUid: session.identity.targetPodUid,
					runtimeInputId: `rin_partial_${decision}`,
					toolUseEventId: toolUseEventIds[0]!,
					decision,
				}),
			).toBe("applied");

			expect(await run()).toMatchObject({ type: "completed" });
			expect(requests).toHaveLength(1);
			expect(runToolCalls).toEqual(
				decision === "allow" ? ["tool-partial-1"] : [],
			);
			expect(
				session.state
					.pendingApprovalToolJobs()
					.map((pending) => pending.toolUseEventId),
			).toEqual([toolUseEventIds[1]!]);
			expect(appended.at(-1)).toEqual({
				type: "session.status_idle",
				stop_reason: {
					type: "requires_action",
					event_ids: [toolUseEventIds[1]!],
				},
			});
		}
	});
	test("LoadContext pendingToolUses hydrates cold approval waits and settles the original tool use", async () => {
		const session = new ThreadRuntime("sesn_1");
		const pendingInput = { file_path: "src/a.ts", content: "ok" };
		const pendingMessage = sealedToolContextEntry("mrq_cold_restore", 2, [
			{
				modelToolCallId: "tool-1",
				toolName: "Write",
				canonicalInput: pendingInput,
			},
		]);
		const loadedMessages = [
			userMessage("user-cold", 1, "hello"),
			pendingMessage,
		];
		const pendingToolUses = [
			{
				toolUseEventId: "sevt_tool_1",
				modelRequestId: "mrq_cold_restore",
				modelToolCallId: "tool-1",
				toolName: "Write",
				input: pendingInput,
				status: "pending" as const,
			},
		];
		const loader = new QueuedContextLoader([], []);
		const appended: SessionEvent[] = [];
		const settlements: SessionEventWriterToolSettlementEnvelope[] = [];
		const writer = writerFrom(
			(envelope) => {
				appended.push(envelope.event);
				return {
					ok: true,
					eventId: `bridge-${envelope.writeId}`,
					type: "committed",
					eventSequence: 1,
				};
			},
			undefined,
			[
				{
					sessionThreadId: session.identity.sessionThreadId,
					message: pendingMessage,
				},
			],
			undefined,
			async (envelope) => {
				settlements.push(envelope);
				return { ok: true, result: { type: "committed" } };
			},
		);
		const requests: LLMRequest[] = [];
		const runToolCalls: string[] = [];
		const store = new ThreadLoopRuntimeStore([]);
		const coldCatalog = catalogForTest({
			name: "Write",
			description: "Write file",
			inputSchema: { type: "object" },
			permissionPolicy: "always_ask",
		});
		const layer = runtimeThreadLoopLayer(loader, {
			store,
			writer,
			llmService: queuedLLMService(
				[
					[
						{ type: "text-start", id: "text-1" },
						{
							type: "text-delta",
							id: "text-1",
							text_delta: "after cold approval",
						},
						{ type: "text-end", id: "text-1" },
						{ type: "finish", finishReason: "stop" },
					],
				],
				requests,
			),
			providerCallRuntime: {
				systemInstructions: "cold approval resume test system",
				toolCatalog: {
					...coldCatalog,
					entries: coldCatalog.entries.map((entry) => ({
						...entry,
						route: {
							kind: "gateway" as const,
							operation: "RunMcpTool" as const,
							mcpServerName: "github",
						},
					})),
				},
			},
			runTool: (request) => {
				runToolCalls.push(
					`${request.modelRequestId}:${request.modelToolCallId}:${request.toolUseEventId}`,
				);
				expect(request.input).toEqual(pendingInput);
				return {
					type: "completed",
					output: { text: "cold approved write", truncated: false },
				};
			},
		});
		const first = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				session.state.contextManager.replaceEntries(loadedMessages);
				session.state.markPersistentContextLoaded();
				threadLoop.seedRuntimeModel(session);
				installRecoveredToolTurn(session, "mrq_cold_restore", [
					{
						modelToolCallId: "tool-1",
						toolUseEventId: "sevt_tool_1",
						toolName: "Write",
					},
				]);
				expect(
					yield* threadLoop.installLoadedPendingToolUses(
						session,
						pendingToolUses,
						loadedMessages,
						undefined,
					),
				).toEqual({ ok: true });
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(Effect.provide(layer)),
		);
		expect(first).toMatchObject({ type: "completed" });
		expect(loader.commitCalls).toEqual([]);
		expect(requests).toHaveLength(0);
		expect(runToolCalls).toEqual([]);
		expect(appended).toEqual([]);
		expect(
			session.state.contextManager
				.entries()
				.find(
					(entry) => entry.messageSequence === pendingMessage.messageSequence,
				)?.parts[0],
		).toMatchObject({
			type: "tool_call",
			modelToolCallId: "tool-1",
		});
		expect(
			session.state.resolveToolConfirmation({
				workspaceId: session.identity.workspaceId,
				sessionId: session.identity.sessionId,
				sessionThreadId: session.identity.sessionThreadId,
				bindingId: session.identity.bindingId,
				bindingGeneration: session.identity.bindingGeneration,
				targetPodUid: session.identity.targetPodUid,
				runtimeInputId: "rin_confirm_cold",
				toolUseEventId: "sevt_tool_1",
				decision: "allow",
			}),
		).toBe("applied");
		const second = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(Effect.provide(layer)),
		);
		expect(second).toMatchObject({ type: "completed" });
		expect(requests).toHaveLength(1);
		expect(runToolCalls).toEqual(["mrq_cold_restore:tool-1:sevt_tool_1"]);
		expect(settlements).toEqual([
			expect.objectContaining({
				settlement: expect.objectContaining({ toolUseEventId: "sevt_tool_1" }),
			}),
		]);
		expect(appended.map((event) => event.type)).toEqual([
			"session.status_running",
			"span.model_request_start",
			"agent.message",
			"span.model_request_end",
			"session.status_idle",
		]);
	});
	test("LoadContext pendingSandboxExecutions rejoins the original durable Tool Use", async () => {
		const session = new ThreadRuntime("sesn_1");
		session.state.enqueueAcceptedInput(
			acceptedInput("rin_cold_sandbox_recovery"),
		);
		const input = { file_path: "src/a.ts", content: "ok" };
		const durableToolMessage = sealedToolContextEntry("mrq_cold_sandbox", 2, [
			{
				modelToolCallId: "tool-sandbox-1",
				toolName: "Write",
				canonicalInput: input,
			},
		]);
		const loadedMessages = [
			userMessage("user-cold-sandbox", 1, "hello"),
			durableToolMessage,
		];
		const pendingSandboxExecutions = [
			{
				toolUseEventId: "sevt_sandbox_tool_1",
				modelRequestId: "mrq_cold_sandbox",
				modelToolCallId: "tool-sandbox-1",
				toolName: "Write",
				input,
				executionState: "running" as const,
			},
		];
		const loader = new QueuedContextLoader([], []);
		const appended: SessionEvent[] = [];
		const settlements: SessionEventWriterToolSettlementEnvelope[] = [];
		const writer = writerFrom(
			(envelope) => {
				appended.push(envelope.event);
				return {
					ok: true,
					eventId: `bridge-${envelope.writeId}`,
					type: "committed",
					eventSequence: 1,
				};
			},
			undefined,
			[
				{
					sessionThreadId: session.identity.sessionThreadId,
					message: durableToolMessage,
				},
			],
			undefined,
			async (envelope) => {
				settlements.push(envelope);
				return { ok: true, result: { type: "committed" } };
			},
		);
		const requests: LLMRequest[] = [];
		const runToolCalls: string[] = [];
		let refreshAttempts = 0;
		const store = new ThreadLoopRuntimeStore([]);
		const sandboxCatalog = catalogForTest({
			name: "Write",
			description: "Write file",
			inputSchema: { type: "object" },
		});
		const layer = runtimeThreadLoopLayer(loader, {
			store,
			writer,
			llmService: queuedLLMService(
				[
					[
						{ type: "text-start", id: "text-1" },
						{
							type: "text-delta",
							id: "text-1",
							text_delta: "after sandbox recovery",
						},
						{ type: "text-end", id: "text-1" },
						{ type: "finish", finishReason: "stop" },
					],
				],
				requests,
			),
			providerCallRuntime: {
				systemInstructions: "cold sandbox recovery test system",
				toolCatalog: {
					...sandboxCatalog,
					entries: sandboxCatalog.entries.map((entry) => ({
						...entry,
						route: {
							kind: "sandbox" as const,
							operation: "RunTool" as const,
							helperSubcommand: "write" as const,
						},
					})),
				},
			},
			runTool: (request) => {
				runToolCalls.push(
					`${request.modelRequestId}:${request.modelToolCallId}:${request.toolUseEventId}`,
				);
				expect(refreshAttempts).toBe(2);
				expect(request.input).toEqual(input);
				return {
					type: "completed",
					output: { text: "cold sandbox write", truncated: false },
				};
			},
			refreshRuntimeBindingToken: async () => {
				refreshAttempts += 1;
				if (refreshAttempts === 1) {
					throw new Error("transient token refresh failure");
				}
				return "refreshed-runtime-binding-token";
			},
			acceptSandboxExecution: () => {
				throw new Error(
					"cold accepted Sandbox execution must not be accepted again",
				);
			},
		});
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				session.state.contextManager.replaceEntries(loadedMessages);
				session.state.markPersistentContextLoaded();
				threadLoop.seedRuntimeModel(session);
				installRecoveredToolTurn(session, "mrq_cold_sandbox", [
					{
						modelToolCallId: "tool-sandbox-1",
						toolUseEventId: "sevt_sandbox_tool_1",
						toolName: "Write",
						disposition: "resume_sandbox_execution",
					},
				]);
				expect(
					yield* threadLoop.installLoadedSandboxExecutions(
						session,
						pendingSandboxExecutions,
						loadedMessages,
						undefined,
					),
				).toEqual({ ok: true });
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(Effect.provide(layer)),
		);
		expect(result).toMatchObject({ type: "completed" });
		expect(refreshAttempts).toBe(3);
		expect(runToolCalls).toEqual([
			"mrq_cold_sandbox:tool-sandbox-1:sevt_sandbox_tool_1",
		]);
		expect(
			appended.filter((event) => event.type === "agent.tool_use"),
		).toHaveLength(0);
		expect(settlements).toEqual([
			expect.objectContaining({
				settlement: expect.objectContaining({
					toolUseEventId: "sevt_sandbox_tool_1",
				}),
			}),
		]);
		expect(requests).toHaveLength(1);
	});
	test("cold accepted Sandbox execution releases stale Runtime custody without authoring a result", async () => {
		const session = new ThreadRuntime("sesn_1");
		session.state.enqueueAcceptedInput(
			acceptedInput("rin_cold_sandbox_stale_custody"),
		);
		const input = { file_path: "src/a.ts", content: "ok" };
		const durableToolMessage = sealedToolContextEntry(
			"mrq_cold_sandbox_stale",
			1,
			[
				{
					modelToolCallId: "tool-sandbox-stale",
					toolName: "Write",
					canonicalInput: input,
				},
			],
		);
		const loadedMessages = [
			userMessage("user-cold-sandbox-stale", 0, "hello"),
			durableToolMessage,
		];
		const pendingSandboxExecutions = [
			{
				toolUseEventId: "sevt_sandbox_tool_stale",
				modelRequestId: "mrq_cold_sandbox_stale",
				modelToolCallId: "tool-sandbox-stale",
				toolName: "Write",
				input,
				executionState: "running" as const,
			},
		];
		const appended: SessionEvent[] = [];
		const settlements: SessionEventWriterToolSettlementEnvelope[] = [];
		const requests: LLMRequest[] = [];
		let refreshAttempts = 0;
		const sandboxCatalog = catalogForTest({
			name: "Write",
			description: "Write file",
			inputSchema: { type: "object" },
		});
		const layer = runtimeThreadLoopLayer(new QueuedContextLoader([], []), {
			writer: writerFrom(
				(envelope) => {
					appended.push(envelope.event);
					return {
						ok: true,
						eventId: `bridge-${envelope.writeId}`,
						type: "committed",
						eventSequence: 1,
					};
				},
				undefined,
				[
					{
						sessionThreadId: session.identity.sessionThreadId,
						message: durableToolMessage,
					},
				],
				undefined,
				async (envelope) => {
					settlements.push(envelope);
					return { ok: true, result: { type: "committed" } };
				},
			),
			llmService: queuedLLMService(
				[[{ type: "finish", finishReason: "stop" }]],
				requests,
			),
			providerCallRuntime: {
				systemInstructions: "cold sandbox stale custody test system",
				toolCatalog: {
					...sandboxCatalog,
					entries: sandboxCatalog.entries.map((entry) => ({
						...entry,
						route: {
							kind: "sandbox" as const,
							operation: "RunTool" as const,
							helperSubcommand: "write" as const,
						},
					})),
				},
			},
			runTool: () => {
				throw new Error("stale Sandbox custody must not await or execute");
			},
			refreshRuntimeBindingToken: async () => {
				refreshAttempts += 1;
				if (refreshAttempts > 1) {
					session.state.beginRuntimeShutdown();
				}
				throw {
					type: "context-loader",
					code: "superseded",
					message: "Context loader operation failed.",
					retryable: false,
					fatal: true,
					sessionId: session.sessionId,
				};
			},
			acceptSandboxExecution: () => {
				throw new Error(
					"cold accepted Sandbox execution must not be accepted again",
				);
			},
		});
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				session.state.contextManager.replaceEntries(loadedMessages);
				session.state.markPersistentContextLoaded();
				threadLoop.seedRuntimeModel(session);
				expect(
					yield* threadLoop.installLoadedSandboxExecutions(
						session,
						pendingSandboxExecutions,
						loadedMessages,
						undefined,
					),
				).toEqual({ ok: true });
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(Effect.provide(layer)),
		);
		expect(result).toEqual({ type: "interrupted", discardHotState: true });
		expect(refreshAttempts).toBe(1);
		expect(settlements).toEqual([]);
		expect(requests).toEqual([]);
	});
	test("cold unresolved approval does not strand an accepted Sandbox execution", async () => {
		const session = new ThreadRuntime("sesn_1");
		session.state.enqueueAcceptedInput(
			acceptedInput("rin_cold_mixed_recovery"),
		);
		const approvalInput = { file_path: "src/approval.ts", content: "wait" };
		const sandboxInput = { file_path: "src/accepted.ts", content: "done" };
		const pendingMessage = sealedToolContextEntry("mrq_cold_mixed", 1, [
			{
				modelToolCallId: "tool-approval",
				toolName: "Write",
				canonicalInput: approvalInput,
			},
			{
				modelToolCallId: "tool-sandbox",
				toolName: "Write",
				canonicalInput: sandboxInput,
			},
		]);
		const loadedMessages = [
			userMessage("user-cold-mixed", 0, "hello"),
			pendingMessage,
		];
		const pendingToolUses = [
			{
				toolUseEventId: "sevt_approval",
				modelRequestId: "mrq_cold_mixed",
				modelToolCallId: "tool-approval",
				toolName: "Write",
				input: approvalInput,
				status: "pending" as const,
			},
		];
		const pendingSandboxExecutions = [
			{
				toolUseEventId: "sevt_sandbox",
				modelRequestId: "mrq_cold_mixed",
				modelToolCallId: "tool-sandbox",
				toolName: "Write",
				input: sandboxInput,
				executionState: "running" as const,
			},
		];
		const appended: SessionEvent[] = [];
		const settlements: SessionEventWriterToolSettlementEnvelope[] = [];
		const writer = writerFrom(
			(envelope) => {
				appended.push(envelope.event);
				return {
					ok: true,
					eventId: `bridge-${envelope.writeId}`,
					type: "committed",
					eventSequence: 1,
				};
			},
			undefined,
			[
				{
					sessionThreadId: session.identity.sessionThreadId,
					message: pendingMessage,
				},
			],
			undefined,
			async (envelope) => {
				settlements.push(envelope);
				return { ok: true, result: { type: "committed" } };
			},
		);
		const coldCatalog = catalogForTest({
			name: "Write",
			description: "Write file",
			inputSchema: { type: "object" },
			permissionPolicy: "always_ask",
		});
		const sandboxCatalog = {
			...coldCatalog,
			entries: coldCatalog.entries.map((entry) => ({
				...entry,
				route: {
					kind: "sandbox" as const,
					operation: "RunTool" as const,
					helperSubcommand: "write" as const,
				},
			})),
		};
		const waits: string[] = [];
		const layer = runtimeThreadLoopLayer(new QueuedContextLoader([], []), {
			writer,
			providerCallRuntime: {
				systemInstructions: "cold mixed recovery",
				toolCatalog: sandboxCatalog,
			},
			acceptSandboxExecution: () => {
				throw new Error(
					"accepted Sandbox execution must not be accepted again",
				);
			},
			awaitSandboxExecution: (request) => {
				waits.push(request.toolUseEventId);
				return {
					type: "completed",
					output: { text: "recovered", truncated: false },
				};
			},
		});
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				session.state.contextManager.replaceEntries(loadedMessages);
				session.state.markPersistentContextLoaded();
				threadLoop.seedRuntimeModel(session);
				installRecoveredToolTurn(session, "mrq_cold_mixed", [
					{
						modelToolCallId: "tool-approval",
						toolUseEventId: "sevt_approval",
						toolName: "Write",
					},
					{
						modelToolCallId: "tool-sandbox",
						toolUseEventId: "sevt_sandbox",
						toolName: "Write",
						disposition: "resume_sandbox_execution",
					},
				]);
				expect(
					yield* threadLoop.installLoadedPendingToolUses(
						session,
						pendingToolUses,
						loadedMessages,
						undefined,
					),
				).toEqual({ ok: true });
				expect(
					yield* threadLoop.installLoadedSandboxExecutions(
						session,
						pendingSandboxExecutions,
						loadedMessages,
						undefined,
					),
				).toEqual({ ok: true });
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(Effect.provide(layer)),
		);
		expect(result).toMatchObject({ type: "completed" });
		expect(waits).toEqual(["sevt_sandbox"]);
		expect(session.state.pendingSandboxExecutionJobs()).toHaveLength(0);
		expect(session.state.pendingApprovalToolJobs()).toHaveLength(1);
		expect(settlements).toEqual([
			expect.objectContaining({
				settlement: expect.objectContaining({ toolUseEventId: "sevt_sandbox" }),
			}),
		]);
		expect(appended.at(-1)).toEqual({
			type: "session.status_idle",
			stop_reason: { type: "requires_action", event_ids: ["sevt_approval"] },
		});
	});
	test("LoadContext pendingToolUses applies recorded deny decisions without re-waiting or executing the tool", async () => {
		const session = new ThreadRuntime("sesn_1");
		session.state.enqueueAcceptedInput(acceptedInput("rin_cold_deny_restore"));
		const pendingInput = { file_path: "src/a.ts", content: "ok" };
		const pendingMessage = sealedToolContextEntry("mrq_cold_deny_restore", 2, [
			{
				modelToolCallId: "tool-1",
				toolName: "Write",
				canonicalInput: pendingInput,
			},
		]);
		const loadedMessages = [
			userMessage("user-cold-deny", 1, "hello"),
			pendingMessage,
		];
		const pendingToolUses = [
			{
				toolUseEventId: "sevt_tool_1",
				modelRequestId: "mrq_cold_deny_restore",
				modelToolCallId: "tool-1",
				toolName: "Write",
				input: pendingInput,
				status: "resolving" as const,
				decision: "deny" as const,
				denyMessage: "not now",
			},
		];
		const loader = new QueuedContextLoader([], []);
		const appended: SessionEvent[] = [];
		const settlements: SessionEventWriterToolSettlementEnvelope[] = [];
		const writer = writerFrom(
			(envelope) => {
				appended.push(envelope.event);
				return {
					ok: true,
					eventId: `bridge-${envelope.writeId}`,
					type: "committed",
					eventSequence: 1,
				};
			},
			undefined,
			[
				{
					sessionThreadId: session.identity.sessionThreadId,
					message: pendingMessage,
				},
			],
			undefined,
			async (envelope) => {
				settlements.push(envelope);
				return { ok: true, result: { type: "committed" } };
			},
		);
		const requests: LLMRequest[] = [];
		const store = new ThreadLoopRuntimeStore([]);
		const layer = runtimeThreadLoopLayer(loader, {
			store,
			writer,
			llmService: queuedLLMService(
				[
					[
						{ type: "text-start", id: "text-1" },
						{ type: "text-delta", id: "text-1", text_delta: "denied" },
						{ type: "text-end", id: "text-1" },
						{ type: "finish", finishReason: "stop" },
					],
				],
				requests,
			),
			providerCallRuntime: {
				systemInstructions: "cold approval deny resume test system",
				toolCatalog: catalogForTest({
					name: "Write",
					description: "Write file",
					inputSchema: { type: "object" },
					permissionPolicy: "always_ask",
				}),
			},
			runTool: () => {
				throw new Error("denied pending approval must not execute the tool");
			},
		});
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				session.state.contextManager.replaceEntries(loadedMessages);
				session.state.markPersistentContextLoaded();
				threadLoop.seedRuntimeModel(session);
				installRecoveredToolTurn(session, "mrq_cold_deny_restore", [
					{
						modelToolCallId: "tool-1",
						toolUseEventId: "sevt_tool_1",
						toolName: "Write",
						disposition: "resume_approval_settlement",
					},
				]);
				expect(
					yield* threadLoop.installLoadedPendingToolUses(
						session,
						pendingToolUses,
						loadedMessages,
						undefined,
					),
				).toEqual({ ok: true });
				expect(session.state.pendingApprovalToolJobs()).toEqual([]);
				expect(session.state.resolvedToolRouteJobs()).toEqual([
					expect.objectContaining({
						toolUseEventId: "sevt_tool_1",
						decision: "deny",
						denyMessage: "not now",
					}),
				]);
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(Effect.provide(layer)),
		);
		expect(result).toMatchObject({ type: "completed" });
		expect(requests).toHaveLength(1);
		expect(settlements).toEqual([
			expect.objectContaining({
				settlement: expect.objectContaining({ toolUseEventId: "sevt_tool_1" }),
			}),
		]);
		expect(appended).not.toContainEqual({
			type: "session.status_idle",
			stop_reason: { type: "requires_action", event_ids: ["sevt_tool_1"] },
		});
	});
	test("partial approval settles confirmed members and keeps the provider request idle until all ToolJobs resolve", async () => {
		const session = new ThreadRuntime("sesn_1");
		const loader = new QueuedContextLoader(
			[],
			[
				{ type: "context", entries: [userMessage("user-1", 0, "hello")] },
				{ type: "empty" },
				{ type: "empty" },
			],
		);
		const appended: SessionEvent[] = [];
		const toolUseEventIds: string[] = [];
		let toolUseIndex = 0;
		const writer = writerFrom((envelope) => {
			appended.push(envelope.event);
			const eventId =
				envelope.event.type === "agent.tool_use"
					? `sevt_tool_${++toolUseIndex}`
					: `bridge-${envelope.writeId}`;
			if (envelope.event.type === "agent.tool_use") {
				toolUseEventIds.push(eventId);
			}
			return { ok: true, eventId, type: "committed", eventSequence: 1 };
		});
		const requests: LLMRequest[] = [];
		const runToolCalls: string[] = [];
		const layer = runtimeThreadLoopLayer(loader, {
			writer,
			llmService: queuedLLMService(
				[
					[
						{
							type: "tool-call",
							id: "tool-1",
							toolName: "Write",
							input: { file_path: "src/shared.ts", content: "one" },
							inputPreview: {
								preview: '{"file_path":"src/shared.ts"}',
								truncated: false,
							},
						},
						{
							type: "tool-call",
							id: "tool-2",
							toolName: "Write",
							input: { file_path: "/workspace/src/shared.ts", content: "two" },
							inputPreview: {
								preview: '{"file_path":"/workspace/src/shared.ts"}',
								truncated: false,
							},
						},
						{ type: "finish", finishReason: "tool-calls" },
					],
					[
						{ type: "text-start", id: "text-1" },
						{ type: "text-delta", id: "text-1", text_delta: "all approved" },
						{ type: "text-end", id: "text-1" },
						{ type: "finish", finishReason: "stop" },
					],
				],
				requests,
			),
			providerCallRuntime: {
				systemInstructions: "partial approval test system",
				toolCatalog: catalogForTest({
					name: "Write",
					description: "Write file",
					inputSchema: { type: "object" },
					permissionPolicy: "always_ask",
				}),
			},
			runTool: (request) => {
				runToolCalls.push(request.modelToolCallId);
				return {
					type: "completed",
					output: {
						text: `approved ${request.modelToolCallId}`,
						truncated: false,
					},
				};
			},
		});
		const run = async () =>
			await Effect.runPromise(
				Effect.gen(function* () {
					const threadLoop = yield* ThreadLoop.Service;
					return yield* threadLoop.run(session, testRunCustody());
				}).pipe(Effect.provide(layer)),
			);
		expect(await run()).toMatchObject({ type: "completed" });
		expect(toolUseEventIds).toEqual(["sevt_tool_1", "sevt_tool_2"]);
		expect(
			session.state.resolveToolConfirmation({
				workspaceId: session.identity.workspaceId,
				sessionId: session.identity.sessionId,
				sessionThreadId: session.identity.sessionThreadId,
				bindingId: session.identity.bindingId,
				bindingGeneration: session.identity.bindingGeneration,
				targetPodUid: session.identity.targetPodUid,
				runtimeInputId: "rin_confirm_1",
				toolUseEventId: "sevt_tool_1",
				decision: "allow",
			}),
		).toBe("applied");
		expect(await run()).toMatchObject({ type: "completed" });
		expect(requests).toHaveLength(1);
		expect(runToolCalls).toEqual(["tool-1"]);
		expect(
			session.state
				.pendingApprovalToolJobs()
				.map((pending) => pending.job.modelToolCallId),
		).toEqual(["tool-2"]);
		expect(
			session.state.resolveToolConfirmation({
				workspaceId: session.identity.workspaceId,
				sessionId: session.identity.sessionId,
				sessionThreadId: session.identity.sessionThreadId,
				bindingId: session.identity.bindingId,
				bindingGeneration: session.identity.bindingGeneration,
				targetPodUid: session.identity.targetPodUid,
				runtimeInputId: "rin_confirm_2",
				toolUseEventId: "sevt_tool_2",
				decision: "allow",
			}),
		).toBe("applied");
		expect(await run()).toMatchObject({ type: "completed" });
		expect(requests).toHaveLength(2);
		expect(runToolCalls).toEqual(["tool-1", "tool-2"]);
	});
	test("provider-origin tool-result and tool-error events are rejected before ThreadLoop", () => {
		expect(
			LLMEventSchema.safeParse({
				type: "tool-result",
				id: "orphan-tool",
				output: { text: "done", truncated: false },
			}).success,
		).toBe(false);
		expect(
			LLMEventSchema.safeParse({
				type: "tool-error",
				id: "orphan-tool",
				toolName: "search",
				error: {
					type: "provider",
					code: "provider_tool_protocol_error",
					message: "tool failed",
					retryable: false,
					fatal: true,
					providerId: "fake",
					modelId: "fake-chat",
				},
			}).success,
		).toBe(false);
	});
	test("provider failure waits for the committed Tool owner before closing", async () => {
		const session = new ThreadRuntime("sesn_1");
		const loader = new QueuedContextLoader(
			[],
			[
				{
					type: "context",
					entries: [userMessage("user-1", 0, "hello")],
				},
			],
		);
		const appended: SessionEvent[] = [];
		const requests: LLMRequest[] = [];
		const settlements: SessionEventWriterToolSettlementEnvelope[] = [];
		let runToolCalls = 0;
		let providerCalls = 0;
		const providerError = {
			type: "provider",
			code: "provider_stream_error",
			message: "Stream failed before tool commitment.",
			retryable: true,
			fatal: false,
			providerId: "fake",
			modelId: "fake-chat",
		} as const;
		const layer = runtimeThreadLoopLayer(loader, {
			writer: writerFrom(
				(envelope) => {
					appended.push(envelope.event);
					return {
						ok: true,
						eventId: `bridge-${envelope.writeId}`,
						type: "committed",
						eventSequence: 1,
					};
				},
				undefined,
				[],
				undefined,
				async (envelope) => {
					settlements.push(envelope);
					return { ok: true, result: { type: "committed" } };
				},
			),
			llmService: {
				stream: (request) => {
					requests.push(request);
					providerCalls += 1;
					return Stream.fromIterable<LLMEvent>(
						providerCalls === 1
							? [
									{
										type: "tool-call",
										id: "tool-uncommitted",
										toolName: "search",
										input: { q: "x" },
										inputPreview: {
											preview: '{"q":"x"}',
											truncated: false,
										},
									},
									{ type: "provider-error", error: providerError },
								]
							: [
									{ type: "text-start", id: "retry-text" },
									{
										type: "text-delta",
										id: "retry-text",
										text_delta: "done",
									},
									{ type: "text-end", id: "retry-text" },
									{ type: "finish", finishReason: "stop" },
								],
					);
				},
			},
			providerCallRuntime: {
				systemInstructions: "terminal failure tool discard test",
				toolCatalog: catalogForTest({
					name: "search",
					description: "Search test tool",
					inputSchema: { type: "object" },
					permissionPolicy: "always_ask",
				}),
			},
			runTool: () => {
				runToolCalls += 1;
				return {
					type: "completed",
					output: { text: "must not run", truncated: false },
				};
			},
		});
		const run = async () =>
			await Effect.runPromise(
				Effect.gen(function* () {
					const threadLoop = yield* ThreadLoop.Service;
					return yield* threadLoop.run(session, testRunCustody());
				}).pipe(Effect.provide(layer)),
			);
		expect(await run()).toMatchObject({ type: "interrupted" });
		expect(session.state.pendingApprovalToolJobs()).toHaveLength(1);
		expect(await run()).toMatchObject({ type: "interrupted" });
		expect(providerCalls).toBe(1);
		const pendingToolUseEventId = session.state.pendingApprovalToolJobs()[0]?.toolUseEventId;
		expect(pendingToolUseEventId).toBeDefined();
		expect(
			session.state.resolveToolConfirmation({
				workspaceId: session.identity.workspaceId,
				sessionId: session.identity.sessionId,
				sessionThreadId: session.identity.sessionThreadId,
				bindingId: session.identity.bindingId,
				bindingGeneration: session.identity.bindingGeneration,
				targetPodUid: session.identity.targetPodUid,
				runtimeInputId: "rin_provider_failure_confirm",
				toolUseEventId: pendingToolUseEventId!,
				decision: "allow",
			}),
		).toBe("applied");
		expect(await run()).toMatchObject({ type: "completed" });
		expect(providerCalls).toBe(2);
		expect(runToolCalls).toBe(1);
		expect(
			appended.filter(
				(event) =>
					event.type === "agent.tool_use" ||
					event.type === "agent.mcp_tool_use",
			),
		).toHaveLength(1);
		expect(settlements).toHaveLength(1);
		const retryContext = JSON.stringify(requests[1]?.context);
		expect(retryContext).toContain("tool-uncommitted");
		expect(retryContext).toContain("must not run");
	});
	test("terminal provider failure retains an atomically committed internal tool repair", async () => {
		const session = new ThreadRuntime("sesn_1");
		const store = new ThreadLoopRuntimeStore([]);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const publicToolEvents: SessionEvent[] = [];
		const providerError = {
			type: "provider",
			code: "provider_stream_error",
			message: "Stream failed after internal repair.",
			retryable: false,
			fatal: true,
			providerId: "fake",
			modelId: "fake-chat",
		} as const;
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						store,
						writer: writerFrom((envelope) => {
							if (envelope.event.type === "agent.tool_use") {
								publicToolEvents.push(envelope.event);
							}
							return {
								ok: true,
								eventId: `bridge-${envelope.writeId}`,
								type: "committed",
								eventSequence: 1,
							};
						}),
						events: [
							{
								type: "tool-call",
								id: "tool-internal-repair",
								toolName: "Bash",
								input: {},
								inputPreview: { preview: "{}", truncated: false },
							},
							{ type: "provider-error", error: providerError },
						],
						providerCallRuntime: {
							systemInstructions: "terminal failure internal repair test",
						},
						runtimePolicy: () => ({
							toolCatalog: createToolCatalog({ family: "gpt" }),
						}),
					}),
				),
			),
		);
		const repairMessage = session.state.contextManager
			.entries()
			.find((message) =>
				message.parts.some(
					(part) => part.type === "tool_result" && part.result.type === "error",
				),
			);
		expect(result).toMatchObject({ type: "failed", error: providerError });
		expect(store.repairs).toHaveLength(1);
		expect(publicToolEvents).toEqual([]);
		expect(repairMessage).toMatchObject({
			contextKind: "assistant",
			parts: expect.arrayContaining([
				expect.objectContaining({
					type: "tool_call",
					modelToolCallId: "tool-internal-repair",
				}),
				expect.objectContaining({
					type: "tool_result",
					modelToolCallId: "tool-internal-repair",
					result: expect.objectContaining({ type: "error" }),
				}),
			]),
		});
		expect(repairMessage).not.toHaveProperty("toolUseEventId");
	});
});
