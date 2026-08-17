import { describe, expect, test } from "bun:test";
import { MaxTextBytes } from "@tetral/gateway-protocol/src/bounds.js";
import {
	ProviderContextRole,
	ProviderRequestKind,
	SystemCacheHint,
	SystemSegmentKind,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { Effect, Stream } from "effect";
import {
	runtimeModelForThread,
	runtimeToolPolicyFromPatchPayloads,
} from "../../../../runtime-pod/src/command.js";
import { normalizeProviderError } from "../../../src/contracts/provider.js";
import type {
	SessionEvent,
	SessionEventEnvelope,
	SessionEventWriter,
	SessionEventWriterRequestEndEnvelope,
} from "../../../src/contracts/runtime.js";
import {
	normalizeContextLoaderError,
	RuntimeContextEntrySchema,
} from "../../../src/contracts/runtime.js";
import type { LLMEvent } from "../../../src/llm/llm-event.js";
import { runtimeFailureFromProviderError } from "../../../src/llm/llm-event.js";
import type {
	LLMRequest,
	Interface as LLMServiceInterface,
} from "../../../src/llm/llm-service.js";
import type {
	MemoryStorePromptEntry,
	ProviderCallAssemblyInput,
	SkillGuidanceIndexEntry,
} from "../../../src/thread-loop/provider-request.js";
import {
	ApplyPatchInstructionsText,
	assembleProviderCallRequest,
	DefaultProviderCallRuntimeConfig,
	PlatformBaseSystemPrompt,
	renderSkillGuidanceSegment,
	requestErrorKindFromFailure,
} from "../../../src/thread-loop/provider-request.js";
import type { RuntimeAcceptedInputCommitObservation } from "../../../src/thread-loop/thread-loop.js";
import * as ThreadLoop from "../../../src/thread-loop/thread-loop.js";
import { ThreadRuntime } from "../../../src/thread-loop/thread-runtime.js";
import type {
	RuntimeAcceptedInputState,
	RuntimeConfigPatchState,
} from "../../../src/thread-loop/thread-state.js";
import type { ToolCatalog } from "../../../src/tools/tool-catalog.js";
import {
	createToolCatalog,
	lookupToolEntry,
} from "../../../src/tools/tool-catalog.js";
import type { TestContextLoader } from "./thread-loop-test-support.js";
import {
	acceptedInput,
	acceptedInputCommitResult,
	approvalReviewerPolicy,
	catalogForTest,
	compactionTransportHistory,
	createdAt,
	failingEventWriter,
	llmService,
	QueuedContextLoader,
	queuedLLMService,
	RecordingContextLoader,
	RecordingRuntimeMetrics,
	rejectionInput,
	requestEndResultForTest,
	runtimeNotificationMessage,
	runtimeThreadLoopLayer,
	ThreadLoopRuntimeStore,
	taskNotificationInput,
	testRunCustody,
	userMessage,
	utf8RoundTrip,
	writerFrom,
} from "./thread-loop-test-support.js";

describe("ThreadLoop", () => {
	test("classifies bounded Runtime output as a semantic request error", () => {
		expect(
			requestErrorKindFromFailure({
				type: "runtime",
				code: "runtime_invalid_sequence",
				message: "Runtime provider output exceeds its semantic size bound.",
				retryable: false,
				fatal: true,
				reason: "bounded",
			}),
		).toBe("runtime_semantic_error");
	});

	test("reports declaration, event-write, and provider stream metrics through injected sink", async () => {
		const metrics = new RecordingRuntimeMetrics();
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("msg_user_1", 1, "hello")],
		});
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(
					new ThreadRuntime("sesn_1"),
					testRunCustody(),
				);
			}).pipe(Effect.provide(runtimeThreadLoopLayer(loader, { metrics }))),
		);
		expect(result.type).toBe("completed");
		expect(metrics.contextLoadLatencies).toContainEqual(
			expect.objectContaining({
				operation: "commit_accepted_input",
				outcome: "success",
			}),
		);
		expect(metrics.eventWriteLatencies).toContainEqual(
			expect.objectContaining({
				operation: "append",
				outcome: "success",
			}),
		);
		expect(metrics.eventWriteLatencies).toContainEqual(
			expect.objectContaining({
				operation: "finish_idle",
				outcome: "success",
			}),
		);
		expect(metrics.providerStreamDurations).toContainEqual(
			expect.objectContaining({
				kind: "agent_provider_request",
				outcome: "success",
			}),
		);
	});
	test("provider-call assembler builds the complete non-persistent LLM request shape", () => {
		const input: Parameters<typeof assembleProviderCallRequest>[0] = {
			identity: {
				workspaceId: "workspace_1",
				sessionId: "sesn_1",
				sessionThreadId: "thread_1",
				parentThreadId: "parent_thread_1",
				bindingId: "binding_1",
				bindingGeneration: 7,
				targetPodUid: "pod_1",
				runtimeBindingToken: "runtime-binding-token",
			},
			requestId: "provider_request_1",
			modelRequestId: "model_request_1",
			currentModel: { providerId: "fake", modelId: "fake-chat" },
			providerContext: [
				{
					role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
					content: [{ text: { text: "hello" } }],
				},
			],
			runtime: {
				systemInstructions: "third group runtime system instructions",
				agentSystem: "Operate as the session specialist.",
				toolCatalog: catalogForTest({
					name: "third_group_lookup",
					description: "third group tool description",
					inputSchema: {
						type: "object",
						properties: { q: { type: "string" } },
					},
				}),
				maxOutputTokens: 321,
				timeoutMs: 456,
			},
		};
		const result = assembleProviderCallRequest(input);
		expect(result).toEqual({
			ok: true,
			system: [
				{
					kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
					text: "third group runtime system instructions",
					cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE,
				},
				{
					kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_AGENT,
					text: "Operate as the session specialist.",
					cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_SESSION,
				},
			],
			tools: [
				{
					name: "third_group_lookup",
					description: "third group tool description",
					function: {
						inputSchemaJson:
							'{"type":"object","properties":{"q":{"type":"string"}}}',
					},
				},
			],
			maxOutputTokens: 321,
			timeoutMs: 456,
			runtimeAttachments: [],
			request: {
				requestId: "provider_request_1",
				modelRequestId: "model_request_1",
				requestKind:
					ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
				workspaceId: "workspace_1",
				sessionId: "sesn_1",
				sessionThreadId: "thread_1",
				bindingId: "binding_1",
				bindingGeneration: 7,
				runtimeBindingToken: "runtime-binding-token",
				model: { providerId: "fake", modelId: "fake-chat", variant: "" },
				system: [
					{
						kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
						text: "third group runtime system instructions",
						cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE,
					},
					{
						kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_AGENT,
						text: "Operate as the session specialist.",
						cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_SESSION,
					},
				],
				context: [
					{
						role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
						content: [{ text: { text: "hello" } }],
					},
				],
				tools: [
					{
						name: "third_group_lookup",
						description: "third group tool description",
						function: {
							inputSchemaJson:
								'{"type":"object","properties":{"q":{"type":"string"}}}',
						},
					},
				],
				attachments: [],
				limits: { maxOutputTokens: 321, timeoutMs: 456 },
			},
		});
		const { maxOutputTokens: _configured, ...runtimeWithoutOutputLimit } =
			input.runtime;
		const unset = assembleProviderCallRequest({
			...input,
			runtime: runtimeWithoutOutputLimit,
		});
		if (!unset.ok)
			throw new Error(
				"expected assembly without a configured output limit to succeed",
			);
		expect(unset.maxOutputTokens).toBeUndefined();
		expect(unset.request.limits).toEqual({
			maxOutputTokens: 0,
			timeoutMs: 456,
		});
		const outputSchemaJson = JSON.stringify({
			type: "object",
			additionalProperties: false,
			required: ["outcome"],
			properties: { outcome: { enum: ["allow", "deny"] } },
		});
		const reviewer = assembleProviderCallRequest({
			...input,
			runtime: {
				...input.runtime,
				requestKind:
					ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
				approvalReviewerPolicy,
				outputSchemaJson,
			},
		});
		expect(reviewer.ok ? reviewer.request.outputSchemaJson : undefined).toBe(
			outputSchemaJson,
		);
		expect(
			assembleProviderCallRequest({
				...input,
				runtime: {
					...input.runtime,
					requestKind:
						ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
				},
			}).ok,
		).toBe(false);
		expect(
			assembleProviderCallRequest({
				...input,
				runtime: { ...input.runtime, outputSchemaJson },
			}).ok,
		).toBe(false);
	});
	test("Bridge-shaped create-time config installs agent and memory system segments on provider snapshots", async () => {
		const coldPayload = JSON.stringify({
			config_generation: 7,
			runtime_config: {
				installedTools: [{ type: "tetral_agent_toolset", family: "claude" }],
				system: "Operate as the session specialist.",
				memoryStores: [
					{
						memoryStoreId: "memstore_notes",
						name: "Project notes",
						access: "read_write",
						instructions: "Preserve this guidance.",
					},
				],
			},
		});
		const cases = [
			{
				name: "cold bootstrap",
				patches: [coldPayload],
				expectedAgentSegments: [
					{
						kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_AGENT,
						text: "Operate as the session specialist.",
						cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_SESSION,
					},
				],
				expectedMemorySegments: [
					{
						kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_MEMORY,
						text: "Memory store: Project notes\nAccess: read_write\nInstructions:\nPreserve this guidance.",
						cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_SESSION,
					},
				],
			},
			{
				name: "create-time nullable fields",
				patches: [
					JSON.stringify({
						config_generation: 7,
						runtime_config: {
							installedTools: [
								{ type: "tetral_agent_toolset", family: "claude" },
							],
							system: null,
							memoryStores: [
								{
									memoryStoreId: "memstore_reference",
									name: "Reference",
									access: "read_only",
									instructions: null,
								},
							],
						},
					}),
				],
				expectedAgentSegments: [],
				expectedMemorySegments: [
					{
						kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_MEMORY,
						text: "Memory store: Reference\nAccess: read_only",
						cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_SESSION,
					},
				],
			},
		] as const;
		for (const scenario of cases) {
			const session = new ThreadRuntime(
				`sesn_agent_system_${scenario.name.replaceAll(" ", "_")}`,
			);
			const requests: LLMRequest[] = [];
			const loader = new RecordingContextLoader([], {
				type: "context",
				entries: [
					userMessage(`user-agent-system-${scenario.name}`, 0, "hello"),
				],
			});
			const result = await Effect.runPromise(
				Effect.gen(function* () {
					const threadLoop = yield* ThreadLoop.Service;
					return yield* threadLoop.run(session, testRunCustody());
				}).pipe(
					Effect.provide(
						runtimeThreadLoopLayer(loader, {
							onStream: (request) => requests.push(request),
							runtimePolicy: () =>
								runtimeToolPolicyFromPatchPayloads(scenario.patches),
						}),
					),
				),
			);
			expect(result).toMatchObject({ type: "completed" });
			expect(requests).toHaveLength(1);
			expect(
				requests[0]?.system.filter(
					(segment) =>
						segment.kind === SystemSegmentKind.SYSTEM_SEGMENT_KIND_AGENT,
				),
			).toEqual([...scenario.expectedAgentSegments]);
			expect(
				requests[0]?.system.filter(
					(segment) =>
						segment.kind === SystemSegmentKind.SYSTEM_SEGMENT_KIND_MEMORY,
				),
			).toEqual([...scenario.expectedMemorySegments]);
		}
	});
	test("provider snapshot injects apply-patch instructions from the cold pinned GPT family", async () => {
		const session = new ThreadRuntime("sesn_gpt_patch_prompt");
		const contentJson = JSON.stringify({
			config_generation: 1,
			runtime_config: {
				installedTools: [{ type: "tetral_agent_toolset", family: "gpt" }],
				system: null,
				memoryStores: [],
			},
		});
		expect(
			session.configuration.apply({
				generation: 1,
				contentJson,
				coldLoad: true,
				installedBuiltinFamily: "gpt",
			}),
		).toBe("applied");
		const requests: LLMRequest[] = [];
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-gpt-patch-prompt", 0, "edit a file")],
		});
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						onStream: (request) => requests.push(request),
						runtimePolicy: () =>
							runtimeToolPolicyFromPatchPayloads([contentJson]),
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed" });
		expect(requests[0]?.system[0]?.text).toContain("## `apply_patch`");
		expect(requests[0]?.system[0]?.text).toContain("do not JSON-wrap it");
	});
	test("first accepted turn resolves its provider model from the cold runtime config", async () => {
		const session = new ThreadRuntime("sesn_first_config_model");
		session.state.enqueueAcceptedInput(
			acceptedInput("rin_first_config_model", session.sessionId),
		);
		const runtimeConfigPatch: RuntimeConfigPatchState = {
			workspaceId: "wksp_test",
			sessionId: session.sessionId,
			bindingId: "bind_1",
			bindingGeneration: 1,
			targetPodUid: "pod_1",
			configIdentity: "runtime_config",
			generation: 1,
			coldLoad: true,
			installedBuiltinFamily: "claude" as const,
			contentJson: JSON.stringify({
				runtime_config: {
					agent: { config: { model: "openai/gpt-5.5" } },
					installedTools: [{ type: "tetral_agent_toolset", family: "claude" }],
				},
			}),
		};
		session.configuration.apply(runtimeConfigPatch);
		const loader = new QueuedContextLoader([], []);
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
						runtimeModel: (activeSession) =>
							runtimeModelForThread(
								activeSession.identity.threadRole,
								activeSession.configuration
									.patches()
									.map((patch) => patch.contentJson),
								{ providerId: "anthropic", modelId: "claude-opus-4-8" },
							),
						onStream: (request) => requests.push(request),
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed" });
		expect(requests).toHaveLength(1);
		expect(requests[0]?.model).toEqual({
			providerId: "openai",
			modelId: "gpt-5.5",
			variant: "",
		});
	});
	test("a bounded live rejection is authored by the loop and committed before provider work", async () => {
		const session = new ThreadRuntime("sesn_live_rejection");
		const firstInput = rejectionInput(
			"rin_live_rejection",
			"runtime_command_payload_too_large",
			session.sessionId,
		);
		const secondInput = rejectionInput(
			"rin_live_rejection_second",
			"runtime_command_payload_too_large",
			session.sessionId,
		);
		session.state.enqueueAcceptedInput(firstInput);
		session.state.enqueueAcceptedInput(secondInput);
		const loader = new RecordingContextLoader([], { type: "empty" });
		let providerCalls = 0;
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						onStream: () => {
							providerCalls += 1;
						},
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed" });
		expect(session.state.contextManager.entries()).toContainEqual(
			expect.objectContaining({
				contextKind: "assistant",
				parts: [
					expect.objectContaining({
						type: "text",
						text: "The session runtime could not accept this input.",
					}),
				],
			}),
		);
		expect(
			session.state.contextManager
				.entries()
				.filter((message) =>
					message.parts.some(
						(part) =>
							part.type === "text" &&
							part.text === "The session runtime could not accept this input.",
					),
				),
		).toHaveLength(2);
		expect(providerCalls).toBe(0);
		expect(session.state.threadTurnReduction()).toMatchObject({
			checkpoint: {
				idleCloseout: { stopReason: "end_turn" },
			},
			state: { state: "idle" },
			action: { action: "await_input" },
		});
	});

	test("a stale Request Start receipt discards hot state before provider dispatch", async () => {
		const session = new ThreadRuntime("sesn_stale_request_start");
		session.state.enqueueAcceptedInput(
			acceptedInput("rin_stale_request_start", session.sessionId),
		);
		const baseWriter = writerFrom((envelope) => ({
			ok: true,
			eventId: `bridge-${envelope.writeId}`,
			type: "committed",
			eventSequence: 1,
		}));
		let providerCalls = 0;
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(new QueuedContextLoader([], []), {
						writer: {
							...baseWriter,
							append: async (envelope) => {
								const written = await baseWriter.append(envelope);
								if (
									envelope.event.type !== "span.model_request_start" ||
									!written.ok
								) {
									return written;
								}
								return { ok: true, type: "stale" as const };
							},
						},
						onStream: () => {
							providerCalls += 1;
						},
					}),
				),
			),
		);
		expect(result).toEqual({ type: "interrupted", discardHotState: true });
		expect(providerCalls).toBe(0);
		expect(session.state.contextManager.entries()).toEqual([]);
	});
	test("runtime layer emits running, span, progress, span end, and idle around a normal provider call", async () => {
		const session = new ThreadRuntime("sesn_1");
		const store = new ThreadLoopRuntimeStore([]);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const timeline: string[] = [];
		const appended: SessionEvent[] = [];
		const writer = writerFrom((envelope) => {
			timeline.push(`event:${envelope.event.type}`);
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
						store,
						writer,
						onStream: () => {
							timeline.push("provider:stream");
						},
						events: [
							{ type: "text-start", id: "text-1" },
							{ type: "text-delta", id: "text-1", text_delta: "hello" },
							{ type: "text-end", id: "text-1" },
							{
								type: "finish",
								finishReason: "stop",
								usage: {
									inputTokens: 11,
									outputTokens: 7,
									reasoningTokens: 0,
									cacheReadTokens: 3,
									cacheWriteTokens: 2,
								},
								modelLimits: {
									contextWindowTokens: 400000,
									inputLimitTokens: 272000,
									outputTokenLimit: 128000,
								},
							},
						],
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed", modelMessageCount: 1 });
		expect(timeline).toEqual([
			"event:session.status_running",
			"event:span.model_request_start",
			"provider:stream",
			"event:agent.message",
			"event:span.model_request_end",
			"event:session.status_idle",
		]);
		expect(appended.at(3)).toEqual({
			type: "span.model_request_end",
			model_request_start_id: expect.stringMatching(/^model_request-/),
			is_error: false,
			model_usage: {
				input_tokens: 11,
				output_tokens: 7,
				cache_creation_input_tokens: 2,
				cache_read_input_tokens: 3,
				speed: null,
			},
		});
		expect(session.state.lastRequestUsage()).toEqual({
			inputTokens: 11,
			outputTokens: 7,
			reasoningTokens: 0,
			cacheReadTokens: 3,
			cacheWriteTokens: 2,
		});
		expect(session.state.lastRequestModelLimits()).toEqual({
			contextWindowTokens: 400000,
			inputLimitTokens: 272000,
			outputTokenLimit: 128000,
		});
	});
	test("a provider request does not start when WriteRequestEnd transport is unavailable", async () => {
		const session = new ThreadRuntime("sesn_1");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const appendedTypes: string[] = [];
		const completeWriter = writerFrom((envelope) => {
			appendedTypes.push(envelope.event.type);
			return {
				ok: true,
				eventId: `bridge-${envelope.writeId}`,
				type: "committed",
				eventSequence: 1,
			};
		});
		const malformedWriter = {
			append: completeWriter.append,
			finishIdle: completeWriter.finishIdle,
		} as unknown as SessionEventWriter;
		const providerRequests: LLMRequest[] = [];
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer: malformedWriter,
						onStream: (request) => providerRequests.push(request),
						events: [
							{ type: "text-start", id: "text-1" },
							{ type: "text-delta", id: "text-1", text_delta: "hello" },
							{ type: "text-end", id: "text-1" },
							{ type: "finish", finishReason: "stop" },
						],
					}),
				),
			),
		);
		expect(result).toMatchObject({
			type: "failed",
			error: { code: "unavailable", sessionId: "sesn_1" },
			releaseSession: { reason: "event_write_failed" },
		});
		expect(appendedTypes).not.toContain("span.model_request_start");
		expect(appendedTypes).not.toContain("span.model_request_end");
		expect(providerRequests).toHaveLength(0);
	});
	test("runtime layer compacts context before the next provider request", async () => {
		const session = new ThreadRuntime("sesn_1");
		session.state.updateCurrentModel({
			providerId: "fake",
			modelId: "fake-chat",
		});
		session.state.contextManager.installThreadContextPrefix({
			childThreadId: "thrd_child",
			parentThreadId: "thrd_parent",
			parentBoundaryEventId: "sevt_parent_boundary",
			entries: [userMessage("parent-prefix", 41, "PARENT_PREFIX_SENTINEL")],
		});
		session.state.recordLastRequestCompletion(
			{
				inputTokens: 96000,
				outputTokens: 75,
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
		const mediaOnlyProjection = RuntimeContextEntrySchema.parse({
			...userMessage("user-media-only", 1, "discarded media placeholder"),
			parts: [],
		});
		const loader = new RecordingContextLoader(
			[
				mediaOnlyProjection,
				userMessage(
					"user-old",
					2,
					compactionTransportHistory("old context that should be summarized"),
				),
			],
			{ type: "context", entries: [userMessage("user-new", 3, "new request")] },
		);
		const compactionBoundaryOrder: string[] = [];
		const requests: LLMRequest[] = [];
		const oversizedSummary = `Summary carried forward.${"S".repeat(40000)}`;
		const queuedLlm = queuedLLMService(
			[
				[
					{ type: "text-start", id: "summary-text" },
					{
						type: "text-delta",
						id: "summary-text",
						text_delta: oversizedSummary,
					},
					{ type: "text-end", id: "summary-text" },
					{
						type: "finish",
						finishReason: "stop",
						usage: {
							inputTokens: 20,
							outputTokens: 5,
							reasoningTokens: 0,
							cacheReadTokens: 0,
							cacheWriteTokens: 0,
						},
					},
				],
				[
					{ type: "text-start", id: "answer-text" },
					{
						type: "text-delta",
						id: "answer-text",
						text_delta: "answer after compaction",
					},
					{ type: "text-end", id: "answer-text" },
					{
						type: "finish",
						finishReason: "stop",
						usage: {
							inputTokens: 9,
							outputTokens: 4,
							reasoningTokens: 0,
							cacheReadTokens: 0,
							cacheWriteTokens: 0,
						},
						modelLimits: { contextWindowTokens: 320, outputTokenLimit: 120 },
					},
				],
			],
			requests,
		);
		const llm: LLMServiceInterface = {
			stream(request) {
				if (
					request.requestKind ===
					ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST
				) {
					compactionBoundaryOrder.push("normal-provider-stream-start");
				}
				return queuedLlm.stream(request);
			},
		};
		const appended: SessionEvent[] = [];
		const requestEndEnvelopes: SessionEventWriterRequestEndEnvelope[] = [];
		const baseWriter = writerFrom((envelope) => {
			appended.push(envelope.event);
			if (
				envelope.event.type === "span.model_request_start" &&
				!compactionBoundaryOrder.includes("compaction-start-ack")
			) {
				compactionBoundaryOrder.push("compaction-start-ack");
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
				if (envelope.compactionContext !== undefined && !envelope.isError) {
					compactionBoundaryOrder.push("compaction-request-end-and-event-ack");
				}
				return await baseWriter.writeRequestEnd!(envelope);
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
						compaction: { timeoutMs: 765432 },
						providerCallRuntime: {
							systemInstructions: "normal provider system",
							maxOutputTokens: 2048,
							timeoutMs: 654321,
							toolCatalog: catalogForTest({
								name: "search",
								description: "Search test tool",
								inputSchema: { type: "object", properties: {} },
							}),
						},
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed", modelMessageCount: 1 });
		expect(requests).toHaveLength(2);
		expect(requests[0]?.requestKind).toBe(
			ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY,
		);
		expect(requests[0]?.model).toEqual({
			providerId: "fake",
			modelId: "fake-chat",
			variant: "",
		});
		expect(requests[0]?.system).toEqual([]);
		expect(requests[0]?.limits?.maxOutputTokens).toBe(2048);
		expect(requests[0]?.limits?.timeoutMs).toBe(765432);
		expect(requests[0]?.model?.variant).toBe("");
		expect(requests[0]?.tools).toEqual([]);
		const compactionPromptParts =
			requests[0]?.context[0]?.content.flatMap(
				(part) => part.text?.text ?? [],
			) ?? [];
		expect(compactionPromptParts).toHaveLength(1);
		const compactionPrompt = compactionPromptParts[0] ?? "";
		expect(
			new TextEncoder().encode(compactionPrompt).byteLength,
		).toBeLessThanOrEqual(64 * 1024);
		expect(compactionPrompt).toStartWith(
			"Create a new anchored summary from the conversation history.",
		);
		expect(compactionPrompt).toContain("## Objective");
		expect(compactionPrompt).toContain(
			"[User]:\n\n[User]: old context that should be summarized",
		);
		expect(compactionPrompt).toContain(
			"[User]: old context that should be summarized",
		);
		expect(compactionPrompt).toContain("PARENT_PREFIX_SENTINEL");
		expect(compactionPrompt).toContain("😀");
		expect(utf8RoundTrip(compactionPrompt)).toBe(compactionPrompt);
		expect(compactionPrompt).not.toContain("<previous-summary>");
		expect(compactionPrompt).not.toContain("RECENT_SENTINEL");
		expect(requests[1]?.requestKind).toBe(
			ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
		);
		expect(requests[1]?.limits?.timeoutMs).toBe(654321);
		expect(requests[1]?.model).toEqual({
			providerId: "fake",
			modelId: "fake-chat",
			variant: "",
		});
		expect(requests[1]?.model?.variant).toBe("");
		expect(requests[1]?.tools.map((tool) => tool.name)).toEqual(["search"]);
		expect(requests[1]?.context).toHaveLength(1);
		expect(JSON.stringify(requests[1]?.context[0])).toContain(
			"<conversation-checkpoint>",
		);
		expect(JSON.stringify(requests[1]?.context[0])).toContain(
			"Summary carried forward.",
		);
		expect(appended.map((event) => event.type)).toEqual([
			"session.status_running",
			"span.model_request_start",
			"span.model_request_end",
			"span.model_request_start",
			"agent.message",
			"span.model_request_end",
			"session.status_idle",
		]);
		expect(
			requestEndEnvelopes.map(
				(envelope) => envelope.compactionContext !== undefined,
			),
		).toEqual([true, false]);
		expect(requestEndEnvelopes[0]?.prefixConsumption).toEqual({
			childThreadId: "thrd_child",
			parentBoundaryEventId: "sevt_parent_boundary",
		});
		expect(compactionBoundaryOrder).toEqual([
			"compaction-start-ack",
			"compaction-request-end-and-event-ack",
			"normal-provider-stream-start",
		]);
		const hotCheckpoint = session.state.contextManager
			.entries()
			.find((message) => message.contextKind === "compaction");
		expect(hotCheckpoint).toBeDefined();
		expect(hotCheckpoint?.parts[0]).toMatchObject({ type: "text" });
		const checkpointText =
			hotCheckpoint?.parts
				.flatMap((part) => (part.type === "text" ? [part.text] : []))
				.join("") ?? "";
		expect(
			new TextEncoder().encode(checkpointText).byteLength,
		).toBeLessThanOrEqual(60 * 1024);
		expect(checkpointText).toContain("<summary>\nSummary carried forward.");
		expect(checkpointText).toContain("RECENT_SENTINEL");
		expect(checkpointText).toContain("[User]: new request\n</recent-context>");
		expect(utf8RoundTrip(checkpointText)).toBe(checkpointText);
		expect(checkpointText.indexOf("RECENT_SENTINEL")).toBeLessThan(
			checkpointText.indexOf("[User]: new request"),
		);
		expect(session.state.lastRequestUsage()).toEqual({
			inputTokens: 9,
			outputTokens: 4,
			reasoningTokens: 0,
			cacheReadTokens: 0,
			cacheWriteTokens: 0,
		});
		expect(session.state.lastRequestModelLimits()).toEqual({
			contextWindowTokens: 320,
			outputTokenLimit: 120,
		});
		expect(session.state.contextManager.threadContextPrefix()).toBeUndefined();
	});
	test("failed-attempt reasoning is absent from reschedule and successful retry commits once", async () => {
		const session = new ThreadRuntime("sesn_1");
		const pendingFileAttachment = {
			transient: undefined,
			fileBacked: {
				sourceEventId: "sevt_retry_file",
				fileId: "file_retry",
			},
			mime: "image/png",
			filename: "retry.png",
		} as const;
		session.state.addPendingAttachments([pendingFileAttachment]);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "retry this request")],
		});
		const requests: LLMRequest[] = [];
		const failedReasoning = "failed attempt private reasoning";
		const failedDraft = "failed attempt draft";
		const successfulReasoningFirst = "successful first reasoning part";
		const successfulReasoningSecond = "successful second reasoning part";
		const successfulReasoning = [
			successfulReasoningFirst,
			successfulReasoningSecond,
		];
		const llm = queuedLLMService(
			[
				[
					{ type: "reasoning-start", id: "retry-discarded-reasoning" },
					{
						type: "reasoning-delta",
						id: "retry-discarded-reasoning",
						text_delta: failedReasoning,
					},
					{ type: "reasoning-end", id: "retry-discarded-reasoning" },
					{ type: "text-start", id: "retry-discarded-text" },
					{
						type: "text-delta",
						id: "retry-discarded-text",
						text_delta: failedDraft,
					},
					{
						type: "provider-error",
						error: runtimeFailureFromProviderError(
							normalizeProviderError({
								code: "provider_unavailable",
								message: "temporary provider failure",
								retryable: true,
								fatal: false,
							}),
						),
					},
				],
				[
					{ type: "reasoning-start", id: "retry-success-reasoning-1" },
					{
						type: "reasoning-delta",
						id: "retry-success-reasoning-1",
						text_delta: successfulReasoningFirst,
					},
					{ type: "reasoning-end", id: "retry-success-reasoning-1" },
					{ type: "reasoning-start", id: "retry-success-reasoning-2" },
					{
						type: "reasoning-delta",
						id: "retry-success-reasoning-2",
						text_delta: successfulReasoningSecond,
					},
					{ type: "reasoning-end", id: "retry-success-reasoning-2" },
					{ type: "text-start", id: "answer-text" },
					{ type: "text-delta", id: "answer-text", text_delta: "recovered" },
					{ type: "text-end", id: "answer-text" },
					{ type: "finish", finishReason: "stop" },
				],
			],
			requests,
		);
		const appended: SessionEvent[] = [];
		const requestStarts: SessionEventEnvelope[] = [];
		const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
		const baseWriter = writerFrom((envelope) => {
			appended.push(envelope.event);
			if (envelope.event.type === "span.model_request_start") {
				requestStarts.push(envelope);
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
				requestEnds.push(envelope);
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
						runtimePolicy: () => ({
							providerRescheduleBudget: 3,
							compactionRescheduleBudget: 2,
						}),
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed", modelMessageCount: 1 });
		expect(requests).toHaveLength(2);
		expect(requests.map((request) => request.attachments)).toEqual([
			[pendingFileAttachment],
			[],
		]);
		expect(requestStarts.map((start) => start.consumedFileAttachments ?? [])).toEqual([
			[{ sourceEventId: "sevt_retry_file", fileId: "file_retry" }],
			[],
		]);
		expect(JSON.stringify(requests[1]?.context)).toContain(
			"retry this request",
		);
		expect(JSON.stringify(requests[1]?.context)).not.toContain(
			"temporary provider failure",
		);
		expect(JSON.stringify(requests[1]?.context)).not.toContain(failedReasoning);
		expect(JSON.stringify(requests[1]?.context)).not.toContain(failedDraft);
		expect(requestEnds).toHaveLength(2);
		expect(requestEnds[0]?.reschedule).toMatchObject({ attempt: 1 });
		expect(requestEnds[0]?.trailingContextAppend).toBeUndefined();
		expect(requestEnds[1]?.reschedule).toBeUndefined();
		expect(requestEnds[1]?.trailingContextAppend).toBeUndefined();
		expect(
			requestEnds.filter(
				(envelope) => (envelope.trailingContextAppend?.parts.length ?? 0) > 0,
			),
		).toHaveLength(0);
		expect(
			new Set(requestEnds.map((envelope) => envelope.modelRequestId)).size,
		).toBe(2);
		const durableEvents = JSON.stringify(appended);
		const hotContext = JSON.stringify(session.state.contextManager.entries());
		expect(durableEvents).not.toContain(failedReasoning);
		expect(durableEvents).not.toContain(failedDraft);
		expect(hotContext).not.toContain(failedReasoning);
		expect(hotContext).not.toContain(failedDraft);
		for (const part of successfulReasoning) {
			expect(hotContext.split(part)).toHaveLength(2);
		}
		expect(session.state.pendingAttachments()).toEqual([]);
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
	test("deterministic Gateway rejection closes on the first attempt without rescheduling", async () => {
		const session = new ThreadRuntime("sesn_gateway_protocol_rejection");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-gateway-protocol", 0, "send this request")],
		});
		const requests: LLMRequest[] = [];
		const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
		const baseWriter = writerFrom((envelope) => ({
			ok: true,
			eventId: `bridge-${envelope.writeId}`,
			type: "committed",
			eventSequence: 1,
		}));
		const writer: SessionEventWriter = {
			...baseWriter,
			writeRequestEnd: async (envelope) => {
				requestEnds.push(envelope);
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
						writer,
						llmService: {
							stream(request) {
								requests.push(request);
								return Stream.fail({
									type: "llm-service" as const,
									error: {
										type: "runtime" as const,
										code: "gateway_protocol_error" as const,
										message: "Gateway rejected the provider request.",
										retryable: false,
										fatal: true,
									},
								});
							},
						},
						runtimePolicy: () => ({
							providerRescheduleBudget: 3,
							compactionRescheduleBudget: 2,
						}),
					}),
				),
			),
		);
		expect(result).toMatchObject({
			type: "failed",
			error: { code: "gateway_protocol_error", retryable: false },
		});
		expect(requests).toHaveLength(1);
		expect(requestEnds).toHaveLength(1);
		expect(requestEnds[0]).toMatchObject({
			isError: true,
			errorKind: "gateway_protocol_error",
		});
		expect(requestEnds[0]?.reschedule).toBeUndefined();
	});
	test("Gateway transport completion deadline uses one existing provider reschedule attempt", async () => {
		const session = new ThreadRuntime("sesn_gateway_completion_deadline");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [
				userMessage("user-gateway-completion-deadline", 0, "send this request"),
			],
		});
		const requests: LLMRequest[] = [];
		const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
		const reschedules: ThreadLoop.RuntimeProviderRescheduleObservation[] = [];
		const baseWriter = writerFrom((envelope) => ({
			ok: true,
			eventId: `bridge-${envelope.writeId}`,
			type: "committed",
			eventSequence: 1,
		}));
		const writer: SessionEventWriter = {
			...baseWriter,
			writeRequestEnd: async (envelope) => {
				requestEnds.push(envelope);
				return await baseWriter.writeRequestEnd(envelope);
			},
		};
		const success = llmService([
			{ type: "text-start", id: "text_after_retry" },
			{ type: "text-delta", id: "text_after_retry", text_delta: "done" },
			{ type: "text-end", id: "text_after_retry" },
			{ type: "finish", finishReason: "stop" },
		]);
		let attempt = 0;
		const retryingLLM: LLMServiceInterface = {
			stream(request, options) {
				requests.push(request);
				attempt += 1;
				if (attempt === 1) {
					return Stream.fail({
						type: "llm-service" as const,
						error: {
							type: "runtime" as const,
							code: "gateway_stream_error" as const,
							message:
								"Gateway provider stream did not complete within its bounded allowance.",
							retryable: true,
							fatal: false,
							reason: "gateway_transport_completion_deadline" as const,
						},
					});
				}
				return success.stream(request, options);
			},
		};
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						llmService: retryingLLM,
						runtimePolicy: () => ({
							providerRescheduleBudget: 3,
							compactionRescheduleBudget: 2,
						}),
						recordProviderReschedule: (event) => reschedules.push(event),
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed", modelMessageCount: 1 });
		expect(requests).toHaveLength(2);
		expect(requestEnds).toHaveLength(2);
		expect(requestEnds[0]).toMatchObject({
			isError: true,
			errorKind: "gateway_stream_error",
			reschedule: { attempt: 1, backoffMs: 1_000 },
		});
		expect(requestEnds[1]).toMatchObject({ isError: false });
		expect(reschedules).toEqual([
			expect.objectContaining({
				attempt: 1,
				delayMs: 1_000,
				delaySource: "runtime_fallback",
				failureCode: "gateway_stream_error",
			}),
		]);
	});
	test("Gateway provider timeout seals an error and uses one existing provider reschedule attempt", async () => {
		const session = new ThreadRuntime("sesn_gateway_provider_timeout");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [
				userMessage("user-gateway-provider-timeout", 0, "send this request"),
			],
		});
		const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
		const reschedules: ThreadLoop.RuntimeProviderRescheduleObservation[] = [];
		const baseWriter = writerFrom((envelope) => ({
			ok: true,
			eventId: `bridge-${envelope.writeId}`,
			type: "committed",
			eventSequence: 1,
		}));
		const writer: SessionEventWriter = {
			...baseWriter,
			writeRequestEnd: async (envelope) => {
				requestEnds.push(envelope);
				return await baseWriter.writeRequestEnd(envelope);
			},
		};
		const success = llmService([
			{ type: "text-start", id: "text_after_provider_timeout" },
			{
				type: "text-delta",
				id: "text_after_provider_timeout",
				text_delta: "done",
			},
			{ type: "text-end", id: "text_after_provider_timeout" },
			{ type: "finish", finishReason: "stop" },
		]);
		let attempt = 0;
		const service: LLMServiceInterface = {
			stream(request, options) {
				attempt += 1;
				if (attempt === 1) {
					return Stream.fromIterable<LLMEvent>([
						{
							type: "provider-error",
							error: runtimeFailureFromProviderError(
								normalizeProviderError({
									code: "provider_timeout",
									message: "Provider request timed out.",
									retryable: true,
									fatal: false,
								}),
							),
						},
					]);
				}
				return success.stream(request, options);
			},
		};
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						llmService: service,
						runtimePolicy: () => ({
							providerRescheduleBudget: 3,
							compactionRescheduleBudget: 2,
						}),
						recordProviderReschedule: (event) => reschedules.push(event),
					}),
				),
			),
		);

		expect(result).toMatchObject({ type: "completed", modelMessageCount: 1 });
		expect(attempt).toBe(2);
		expect(requestEnds).toHaveLength(2);
		expect(requestEnds[0]).toMatchObject({
			isError: true,
			errorKind: "provider_error",
			reschedule: { attempt: 1, backoffMs: 1_000 },
		});
		expect(requestEnds[1]).toMatchObject({ isError: false });
		expect(reschedules).toEqual([
			expect.objectContaining({
				attempt: 1,
				failureCode: "provider_timeout",
			}),
		]);
	});
	test("provider reschedule fallback remains 1s 2s 4s before the fourth failure exhausts", async () => {
		const session = new ThreadRuntime("sesn_provider_reschedule_fallback");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [
				userMessage(
					"user-provider-reschedule-fallback",
					0,
					"send this request",
				),
			],
		});
		const requests: LLMRequest[] = [];
		const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
		const reschedules: ThreadLoop.RuntimeProviderRescheduleObservation[] = [];
		const baseWriter = writerFrom((envelope) => ({
			ok: true,
			eventId: `bridge-${envelope.writeId}`,
			type: "committed",
			eventSequence: 1,
		}));
		const writer: SessionEventWriter = {
			...baseWriter,
			writeRequestEnd: async (envelope) => {
				requestEnds.push(envelope);
				return await baseWriter.writeRequestEnd(envelope);
			},
		};
		const failure = {
			type: "llm-service" as const,
			error: {
				type: "runtime" as const,
				code: "gateway_unavailable" as const,
				message: "Gateway provider stream is unavailable.",
				retryable: true,
				fatal: false,
			},
		};
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						llmService: {
							stream(request) {
								requests.push(request);
								return Stream.fail(failure);
							},
						},
						runtimePolicy: () => ({
							providerRescheduleBudget: 3,
							compactionRescheduleBudget: 2,
						}),
						recordProviderReschedule: (event) => reschedules.push(event),
					}),
				),
			),
		);

		expect(result).toMatchObject({
			type: "failed",
			error: {
				code: "gateway_unavailable",
				retryStatus: { type: "exhausted" },
			},
		});
		expect(requests).toHaveLength(4);
		expect(requestEnds.map((end) => end.reschedule?.backoffMs)).toEqual([
			1_000,
			2_000,
			4_000,
			undefined,
		]);
		expect(
			requestEnds.slice(0, 3).map((end) => end.reschedule?.attempt),
		).toEqual([1, 2, 3]);
		expect(
			reschedules.map((event) => ({
				attempt: event.attempt,
				delayMs: event.delayMs,
				source: event.delaySource,
			})),
		).toEqual([
			{ attempt: 1, delayMs: 1_000, source: "runtime_fallback" },
			{ attempt: 2, delayMs: 2_000, source: "runtime_fallback" },
			{ attempt: 3, delayMs: 4_000, source: "runtime_fallback" },
		]);
	});
	test("positive provider retry delay overrides fallback without changing the attempt count", async () => {
		const session = new ThreadRuntime("sesn_provider_reschedule_override");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [
				userMessage(
					"user-provider-reschedule-override",
					0,
					"send this request",
				),
			],
		});
		const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
		const reschedules: ThreadLoop.RuntimeProviderRescheduleObservation[] = [];
		const baseWriter = writerFrom((envelope) => ({
			ok: true,
			eventId: `bridge-${envelope.writeId}`,
			type: "committed",
			eventSequence: 1,
		}));
		const writer: SessionEventWriter = {
			...baseWriter,
			writeRequestEnd: async (envelope) => {
				requestEnds.push(envelope);
				return await baseWriter.writeRequestEnd(envelope);
			},
		};
		let attempt = 0;
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						llmService: {
							stream() {
								attempt += 1;
								return attempt === 1
									? Stream.fail({
											type: "llm-service" as const,
											error: {
												type: "provider" as const,
												code: "provider_rate_limited" as const,
												message: "Provider is rate limited.",
												retryable: true,
												fatal: false,
												retryAfterMs: 2_500,
											},
										})
									: Stream.fromIterable([
											{
												type: "text-start" as const,
												id: "text_retry_override",
											},
											{
												type: "text-delta" as const,
												id: "text_retry_override",
												text_delta: "done",
											},
											{ type: "text-end" as const, id: "text_retry_override" },
											{
												type: "finish" as const,
												finishReason: "stop" as const,
											},
										]);
							},
						},
						runtimePolicy: () => ({
							providerRescheduleBudget: 3,
							compactionRescheduleBudget: 2,
						}),
						recordProviderReschedule: (event) => reschedules.push(event),
					}),
				),
			),
		);

		expect(result).toMatchObject({ type: "completed" });
		expect(requestEnds[0]?.reschedule).toMatchObject({
			attempt: 1,
			backoffMs: 2_500,
		});
		expect(reschedules).toEqual([
			expect.objectContaining({
				attempt: 1,
				delayMs: 2_500,
				delaySource: "provider",
				failureCode: "provider_rate_limited",
			}),
		]);
	});
	test("a stale no-content request-end receipt discards hot state before another provider request", async () => {
		const session = new ThreadRuntime("sesn_stale_empty_request_end");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-stale-empty-end", 0, "send this request")],
		});
		const requests: LLMRequest[] = [];
		const baseWriter = writerFrom((envelope) => ({
			ok: true,
			eventId: `bridge-${envelope.writeId}`,
			type: "committed",
			eventSequence: 1,
		}));
		const writer: SessionEventWriter = {
			...baseWriter,
			writeRequestEnd: async (envelope) => {
				const result = await baseWriter.writeRequestEnd(envelope);
				if (!result.ok) {
					return result;
				}
				return { ok: true, type: "stale" };
			},
		};
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
										type: "provider-error",
										error: runtimeFailureFromProviderError(
											normalizeProviderError({
												code: "provider_unavailable",
												message: "retryable provider failure",
												retryable: true,
												fatal: false,
											}),
										),
									},
								],
							],
							requests,
						),
						runtimePolicy: () => ({
							providerRescheduleBudget: 3,
							compactionRescheduleBudget: 2,
						}),
					}),
				),
			),
		);
		expect(result).toEqual({ type: "interrupted", discardHotState: true });
		expect(requests).toHaveLength(1);
	});
	test("runtime layer requests hot-state discard when running status append fails before provider work", async () => {
		const order: string[] = [];
		const session = new ThreadRuntime("sesn_1");
		const store = new ThreadLoopRuntimeStore(order);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const appendedTypes: string[] = [];
		let providerCalled = false;
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						store,
						writer: failingEventWriter(
							appendedTypes,
							(event) => event.type === "session.status_running",
						),
						onStream: () => {
							providerCalled = true;
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
		expect(providerCalled).toBe(false);
		expect(appendedTypes).toEqual(["session.status_running"]);
		expect(order).toEqual([]);
		expect(session.state.contextManager.entries()).toEqual([]);
	});
	test("provider-call assembly failure fails closed after running status but before assistant shell, span, and provider stream", async () => {
		const order: string[] = [];
		const session = new ThreadRuntime("sesn_1");
		const store = new ThreadLoopRuntimeStore(order);
		const appended: SessionEvent[] = [];
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const hostileMarker =
			"prompt text raw provider payload marker authorization: bearer dummy-thirdgroup-token";
		let providerCalled = false;
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						store,
						providerCallAssembler: () => {
							throw new Error(hostileMarker);
						},
						onStream: () => {
							providerCalled = true;
						},
						writer: writerFrom((envelope) => {
							appended.push(envelope.event);
							return {
								ok: true,
								eventId: `bridge-${envelope.writeId}`,
								type: "committed",
								eventSequence: 1,
							};
						}),
					}),
				),
			),
		);
		expect(result).toMatchObject({
			type: "failed",
			error: {
				type: "runtime",
				code: "runtime_invalid_sequence",
				reason: "runtime_contract_validation",
			},
		});
		expect("releaseSession" in result).toBe(false);
		expect(JSON.stringify(result)).not.toContain("raw provider payload marker");
		expect(JSON.stringify(result)).not.toContain("dummy-thirdgroup-token");
		expect(providerCalled).toBe(false);
		expect(order).toEqual([]);
		expect(appended).toEqual([
			{ type: "session.status_running" },
			expect.objectContaining({
				type: "session.error",
				error: expect.objectContaining({
					type: "runtime",
					code: "runtime_invalid_sequence",
					retryStatus: { type: "exhausted" },
				}),
			}),
			{
				type: "session.status_idle",
				stop_reason: { type: "retries_exhausted" },
			},
		]);
		expect(session.state.contextManager.entries()).toEqual([
			{
				messageSequence: 1,
				contextKind: "user",
				parts: [{ type: "text", text: "hello" }],
			},
		]);
	});
	test("runtime layer requests hot-state discard when span start append fails after shell persistence", async () => {
		const order: string[] = [];
		const session = new ThreadRuntime("sesn_1");
		const store = new ThreadLoopRuntimeStore(order);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const appendedTypes: string[] = [];
		let providerCalled = false;
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						store,
						writer: failingEventWriter(
							appendedTypes,
							(event) => event.type === "span.model_request_start",
						),
						onStream: () => {
							providerCalled = true;
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
		expect(providerCalled).toBe(false);
		expect(appendedTypes).toEqual([
			"session.status_running",
			"span.model_request_start",
		]);
		expect(order).toEqual([]);
		expect(
			session.state.contextManager
				.entries()
				.map((message) => message.contextKind),
		).toEqual(["user"]);
	});
	test("runtime layer requests hot-state discard when span end append fails after durable progress", async () => {
		const order: string[] = [];
		const session = new ThreadRuntime("sesn_1");
		const store = new ThreadLoopRuntimeStore(order);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const appendedTypes: string[] = [];
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						store,
						writer: failingEventWriter(
							appendedTypes,
							(event) => event.type === "span.model_request_end",
						),
						events: [
							{ type: "text-start", id: "text-1" },
							{ type: "text-delta", id: "text-1", text_delta: "ok" },
							{ type: "text-end", id: "text-1" },
							{
								type: "finish",
								finishReason: "stop",
								usage: {
									inputTokens: 5,
									outputTokens: 3,
									reasoningTokens: 1,
									cacheReadTokens: 0,
									cacheWriteTokens: 0,
								},
							},
						],
					}),
				),
			),
		);
		expect(result).toMatchObject({
			type: "failed",
			error: { type: "session-event-writer", code: "unavailable" },
			releaseSession: { reason: "event_write_failed" },
		});
		expect(appendedTypes).toEqual([
			"session.status_running",
			"span.model_request_start",
			"agent.message",
			"span.model_request_end",
		]);
		expect(order).toEqual([]);
		expect(session.state.contextManager.entries()).toEqual([
			{
				messageSequence: 1,
				contextKind: "user",
				parts: [{ type: "text", text: "hello" }],
			},
		]);
		expect(session.state.contextManager.openRequestDraft()).toEqual({
			modelRequestId: expect.stringMatching(/^model_request-/),
			messageSequence: 2,
			parts: [{ type: "text", text: "ok" }],
		});
		expect(session.state.lastRequestUsage()).toBeUndefined();
	});
	test("commits completed reasoning with provider metadata only after durable request settlement", async () => {
		const session = new ThreadRuntime("sesn_1");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const order: string[] = [];
		const requestEnds: Parameters<SessionEventWriter["writeRequestEnd"]>[0][] =
			[];
		const writer = writerFrom(
			(envelope) => {
				order.push(`event:${envelope.event.type}`);
				return {
					ok: true,
					eventId: `bridge-${envelope.writeId}`,
					type: "committed",
					eventSequence: 1,
				};
			},
			async (envelope) => {
				order.push("event:span.model_request_end");
				requestEnds.push(envelope);
				return requestEndResultForTest(envelope);
			},
		);
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						events: [
							{
								type: "reasoning-start",
								id: "reasoning-1",
								providerMetadata: {
									anthropic: { signature: "sig_round_trip" },
								},
							},
							{
								type: "reasoning-delta",
								id: "reasoning-1",
								text_delta: "thinking",
							},
							{ type: "reasoning-end", id: "reasoning-1" },
							{
								type: "reasoning-start",
								id: "reasoning-2",
								providerMetadata: {
									openai: { encrypted_content: "ciphertext" },
								},
							},
							{
								type: "reasoning-delta",
								id: "reasoning-2",
								text_delta: "again",
							},
							{ type: "reasoning-end", id: "reasoning-2" },
							{ type: "finish", finishReason: "stop" },
						],
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed" });
		expect(requestEnds).toHaveLength(1);
		expect(requestEnds[0]?.trailingContextAppend?.parts).toEqual([
			expect.objectContaining({
				text: "thinking",
				providerPartId: "reasoning-1",
				providerMetadata: { anthropic: { signature: "sig_round_trip" } },
			}),
			expect.objectContaining({
				text: "again",
				providerPartId: "reasoning-2",
				providerMetadata: { openai: { encrypted_content: "ciphertext" } },
			}),
		]);
		expect(
			order.filter((entry) => entry === "event:span.model_request_end"),
		).toHaveLength(1);
	});
	test("keeps failed reasoning settlement out of stable hot context", async () => {
		const session = new ThreadRuntime("sesn_1");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const writer = writerFrom(
			(envelope) => ({
				ok: true,
				eventId: `bridge-${envelope.writeId}`,
				type: "committed",
				eventSequence: 1,
			}),
			async (envelope) => ({
				ok: false,
				error: {
					type: "session-event-writer",
					code: "unavailable",
					message: "request end settlement failed",
					retryable: false,
					fatal: false,
					sessionId: envelope.sessionId,
				},
			}),
		);
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						events: [
							{
								type: "reasoning-start",
								id: "reasoning-1",
								providerMetadata: {
									anthropic: { signature: "sig_uncommitted" },
								},
							},
							{
								type: "reasoning-delta",
								id: "reasoning-1",
								text_delta: "must not stabilize",
							},
							{ type: "reasoning-end", id: "reasoning-1" },
							{ type: "finish", finishReason: "stop" },
						],
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "failed" });
		expect(
			session.state.contextManager
				.entries()
				.flatMap((message) => message.parts)
				.some((part) => part.type === "reasoning"),
		).toBe(false);
	});
	test("retries a transient request-end failure with the identical ordered reasoning batch", async () => {
		const session = new ThreadRuntime("sesn_1");
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const attempts: SessionEventWriterRequestEndEnvelope[] = [];
		const writer = writerFrom(
			(envelope) => ({
				ok: true,
				eventId: `bridge-${envelope.writeId}`,
				type: "committed",
				eventSequence: 1,
			}),
			async (envelope) => {
				attempts.push(structuredClone(envelope));
				expect(
					session.state.contextManager
						.entries()
						.flatMap((message) => message.parts)
						.some((part) => part.type === "reasoning"),
				).toBe(false);
				if (attempts.length === 1) {
					return {
						ok: false,
						error: {
							type: "session-event-writer",
							code: "unavailable",
							message: "transient request end failure",
							retryable: true,
							fatal: false,
							sessionId: envelope.sessionId,
						},
					};
				}
				return requestEndResultForTest(envelope);
			},
		);
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						events: [
							{ type: "reasoning-start", id: "retry-reasoning-1" },
							{
								type: "reasoning-delta",
								id: "retry-reasoning-1",
								text_delta: "first",
							},
							{ type: "reasoning-end", id: "retry-reasoning-1" },
							{ type: "reasoning-start", id: "retry-reasoning-2" },
							{
								type: "reasoning-delta",
								id: "retry-reasoning-2",
								text_delta: "second",
							},
							{ type: "reasoning-end", id: "retry-reasoning-2" },
							{ type: "finish", finishReason: "stop" },
						],
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed" });
		expect(attempts).toHaveLength(2);
		expect(attempts[1]).toEqual(attempts[0]);
		expect(
			attempts[0]?.trailingContextAppend?.parts.flatMap((part) =>
				part.type === "reasoning" ? [part.text] : [],
			),
		).toEqual(["first", "second"]);
		expect(
			session.state.contextManager
				.entries()
				.flatMap((message) => message.parts)
				.filter((part) => part.type === "reasoning"),
		).toHaveLength(2);
	});
	test("discards completed reasoning when the provider attempt ends with a non-retryable error", async () => {
		const session = new ThreadRuntime("sesn_1");
		const transientAttachment = {
			transient: {
				attachmentRef: "att_failed_reasoning",
				sourcePath: "mcp:test/failed-reasoning.png",
				pageRange: "",
				detail: "auto",
			},
			fileBacked: undefined,
			mime: "image/png",
			filename: "failed-reasoning.png",
		} as const;
		const fileAttachment = {
			transient: undefined,
			fileBacked: {
				sourceEventId: "sevt_failed_reasoning_file",
				fileId: "file_failed_reasoning",
			},
			mime: "image/png",
			filename: "failed-reasoning-file.png",
		} as const;
		const attachments = [transientAttachment, fileAttachment];
		session.state.addPendingAttachments(attachments);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const requestEnds: SessionEventWriterRequestEndEnvelope[] = [];
		const writer = writerFrom(
			(envelope) => ({
				ok: true,
				eventId: `bridge-${envelope.writeId}`,
				type: "committed",
				eventSequence: 1,
			}),
			async (envelope) => {
				requestEnds.push(envelope);
				return requestEndResultForTest(envelope);
			},
		);
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						events: [
							{ type: "reasoning-start", id: "failed-reasoning" },
							{
								type: "reasoning-delta",
								id: "failed-reasoning",
								text_delta: "discard me",
							},
							{ type: "reasoning-end", id: "failed-reasoning" },
							{
								type: "provider-error",
								error: runtimeFailureFromProviderError(
									normalizeProviderError({
										code: "provider_invalid_request",
										message: "terminal provider failure",
										retryable: false,
										fatal: false,
									}),
								),
							},
						],
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "failed" });
		expect(requestEnds).toHaveLength(1);
		expect(requestEnds[0]).toMatchObject({
			isError: true,
			errorKind: "provider_error",
		});
		expect(requestEnds[0]?.trailingContextAppend).toBeUndefined();
		expect(requestEnds[0]?.consumedAttachmentRefs ?? []).toEqual([]);
		expect(
			session.state.contextManager
				.entries()
				.flatMap((message) => message.parts)
				.some((part) => part.type === "reasoning"),
		).toBe(false);
		expect(session.state.pendingAttachments()).toEqual([
			attachments[0]!,
		]);
	});
	test("task notification commits after the running receipt and reaches the provider once", async () => {
		const session = new ThreadRuntime("sesn_task_notification_turn");
		const order: string[] = [];
		const requests: LLMRequest[] = [];
		expect(
			session.state.enqueueAcceptedInput(
				taskNotificationInput(
					"rin_task_notification_turn",
					"task_notification_turn",
					"sevt_task_notification_tool",
					"completed",
					'{"status":"completed","text":"task result for next turn"}',
					session.sessionId,
				),
			),
		).toBe("applied");
		const writer = writerFrom((envelope) => {
			if (envelope.event.type === "session.status_running") {
				order.push("running-receipt");
			}
			return {
				ok: true,
				eventId: `bridge-${envelope.writeId}`,
				type: "committed",
				eventSequence: 1,
			};
		});
		const loader = new QueuedContextLoader(
			[],
			[],
			[
				(input: RuntimeAcceptedInputState) => {
					order.push("task-commit");
					return acceptedInputCommitResult(input);
				},
			],
		);
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						llmService: llmService(
							[
								{ type: "text-start", id: "answer-text" },
								{
									type: "text-delta",
									id: "answer-text",
									text_delta: "acknowledged",
								},
								{ type: "text-end", id: "answer-text" },
								{ type: "finish", finishReason: "stop" },
							],
							(request) => {
								order.push("provider");
								requests.push(request);
							},
						),
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed" });
		expect(order.slice(0, 3)).toEqual([
			"running-receipt",
			"task-commit",
			"provider",
		]);
		expect(requests).toHaveLength(1);
		expect(
			JSON.stringify(requests[0]?.context).match(/task result for next turn/g),
		).toHaveLength(1);
		expect(
			JSON.stringify(session.state.contextManager.entries()).match(
				/task result for next turn/g,
			),
		).toHaveLength(1);
		expect(session.state.peekAcceptedInput()).toBeUndefined();
		const committedNotification = session.state.contextManager
			.entries()
			.find((message) =>
				JSON.stringify(message).includes("task result for next turn"),
			);
		expect(committedNotification).toBeDefined();
	});

	test("a consumed task notification redelivery does not reopen provider work", async () => {
		const session = new ThreadRuntime("sesn_task_notification_redelivery");
		const notification = taskNotificationInput(
			"rin_task_notification_redelivery",
			"task_notification_redelivery",
			"sevt_task_notification_redelivery",
			"completed",
			JSON.stringify({
				status: "completed",
				text: "already consumed task result",
			}),
			session.sessionId,
		);
		expect(session.state.enqueueAcceptedInput(notification)).toBe("applied");
		const loader = new QueuedContextLoader(
			[
				runtimeNotificationMessage(
					"msg_task_notification",
					notification.notificationJson,
					1,
				),
			],
			[],
			[(input: RuntimeAcceptedInputState) =>
				acceptedInputCommitResult(input, "duplicate", 1)],
		);
		let providerCalls = 0;
		const appended: SessionEvent[] = [];
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer: writerFrom((envelope) => {
							appended.push(envelope.event);
							return {
								ok: true,
								eventId: "sevt_consumed_task_running",
								type: "duplicate",
							};
						}),
						llmService: {
							stream() {
								providerCalls++;
								return Stream.empty;
							},
						},
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed" });
		expect(providerCalls).toBe(0);
		expect(
			appended.filter((event) => event.type === "span.model_request_start"),
		).toEqual([]);
		expect(
			session.state.threadTurnReduction().checkpoint.pendingInputContextSequences,
		).toEqual([]);
		expect(session.state.contextManager.entries()).toHaveLength(1);
		expect(session.state.peekAcceptedInput()).toBeUndefined();
	});

	test("an existing durable run-open receipt preserves the accepted-input action", async () => {
		const session = new ThreadRuntime("sesn_duplicate_run_open");
		const durableTurnId = "sevt_duplicate_run_open";
		session.state.installThreadTurn(
			{ executionRunId: durableTurnId, pendingInputContextSequences: [] },
			{ routes: [] },
		);
		expect(
			session.state.enqueueAcceptedInput(
				acceptedInput("rin_duplicate_run_open", session.sessionId),
			),
		).toBe("applied");
		const requests: LLMRequest[] = [];
		const reducerFacts: string[] = [];
		let runOpenWrites = 0;
		const applyThreadTurnFact = session.state.applyThreadTurnFact.bind(session.state);
		session.state.applyThreadTurnFact = (fact) => {
			reducerFacts.push(fact.fact);
			return applyThreadTurnFact(fact);
		};
		const writer = writerFrom((envelope) => {
			if (envelope.event.type === "session.status_running") {
				runOpenWrites += 1;
			}
			return {
				ok: true,
				eventId: `bridge-${envelope.writeId}`,
				type: "committed",
			};
		});
		const custody = {
			activeTurnId: () => durableTurnId,
		};
		const loader = new QueuedContextLoader([], []);
		const result = await Effect.runPromise(
			Effect.gen(function* () {
					return yield* (yield* ThreadLoop.Service).run(session, custody);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						writer,
						llmService: llmService(
							[
								{ type: "text-start", id: "answer" },
								{ type: "text-delta", id: "answer", text_delta: "done" },
								{ type: "text-end", id: "answer" },
								{ type: "finish", finishReason: "stop" },
							],
							(request) => requests.push(request),
						),
					}),
				),
			),
		);
		expect(result).toMatchObject({ type: "completed" });
		expect(requests).toHaveLength(1);
		expect(runOpenWrites).toBe(0);
		expect(loader.commitCalls).toHaveLength(1);
		expect(reducerFacts.filter((fact) => fact === "run_opened")).toHaveLength(0);
		expect(session.state.peekAcceptedInput()).toBeUndefined();
	});

	test("an unresolved Tool Call blocks the next provider request at the Runtime boundary", async () => {
		const session = new ThreadRuntime("sesn_unresolved_tool_call");
		const toolUseEventId = "event_tool_unresolved";
		session.state.contextManager.replaceEntries([
			RuntimeContextEntrySchema.parse({
				messageSequence: 1,
				contextKind: "assistant",
				parts: [
					{
						type: "tool_call",
						modelToolCallId: "call_unresolved",
						toolName: "Read",
						canonicalInput: { path: "README.md" },
					},
				],
			}),
		]);
		session.state.markPersistentContextLoaded();
		session.state.installThreadTurn(
			{
				pendingInputContextSequences: [],
				request: {
					modelRequestId: "request_unresolved_tool_call",
					requestStartEventId: "event_start_unresolved_tool_call",
					requestKind: "agent_provider_request",
					contextThroughMessageSequence: 0,
					requestEnd: {
						eventId: "event_end_unresolved_tool_call",
						isError: false,
						rescheduled: false,
					},
					toolMembers: [
						{
							memberKind: "public_tool_use",
							modelToolCallId: "call_unresolved",
							toolUseEventId,
							toolName: "Read",
						},
					],
				},
			},
			{
				routes: [{ toolUseEventId, disposition: "hot_execution" }],
			},
		);
		let providerCalls = 0;
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(new QueuedContextLoader([], []), {
						installLoaderState: false,
						onStream: () => {
							providerCalls += 1;
						},
					}),
				),
			),
		);

		expect(result).toMatchObject({ type: "completed" });
		expect(providerCalls).toBe(0);
		expect(session.state.threadTurnReduction()).toMatchObject({
			state: {
				state: "waiting_for_tool_results",
				modelRequestId: "request_unresolved_tool_call",
			},
			action: {
				action: "await_tool_results",
				modelRequestId: "request_unresolved_tool_call",
				toolUseEventIds: [toolUseEventId],
			},
		});
	});

	test("prefix-only child Request Start stores durable message boundary zero", async () => {
		const session = new ThreadRuntime("sesn_1");
		const prefixMessage = userMessage(
			"msg_parent_prefix",
			41,
			"prefix-only child task",
		);
		session.state.contextManager.installThreadContextPrefix({
			childThreadId: session.identity.sessionThreadId,
			parentThreadId: "thrd_parent",
			parentBoundaryEventId: "sevt_parent_boundary",
			entries: [prefixMessage],
		});
		session.state.markPersistentContextLoaded();
		session.state.installThreadTurn(
			{
				pendingInputContextSequences: [prefixMessage.messageSequence],
			},
			{ routes: [] },
		);
		const requestStarts: number[] = [];
		const writer = writerFrom((envelope) => {
			if (envelope.event.type === "span.model_request_start") {
				requestStarts.push(
					envelope.contextThroughMessageSequence ?? Number.NaN,
				);
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
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(new QueuedContextLoader([], []), {
						installLoaderState: false,
						writer,
					}),
				),
			),
		);

		expect(result).toMatchObject({ type: "completed" });
		expect(requestStarts).toEqual([0]);
	});
	test("task notification retries an unknown commit outcome before provider work", async () => {
		const session = new ThreadRuntime(
			"sesn_task_notification_retryable_commit",
		);
		let providerCalls = 0;
		expect(
			session.state.enqueueAcceptedInput(
				taskNotificationInput(
					"rin_task_notification_retryable_commit",
					"task_notification_retryable_commit",
					"sevt_task_notification_retryable_commit",
					"completed",
					'{"status":"completed","text":"task result recovered from the replayed receipt"}',
					session.sessionId,
				),
			),
		).toBe("applied");
		const custody = testRunCustody();
		const loader = new QueuedContextLoader(
			[],
			[],
			[
				(input: RuntimeAcceptedInputState) => {
					throw normalizeContextLoaderError({
						code: "unavailable",
						sessionId: input.sessionId,
						reason: "commit acknowledgement was lost",
					});
				},
				(input: RuntimeAcceptedInputState) => acceptedInputCommitResult(input),
			],
		);
		const layer = runtimeThreadLoopLayer(loader, {
			onStream: () => {
				providerCalls += 1;
			},
		});
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, custody);
			}).pipe(Effect.provide(layer)),
		);
		expect(result).toMatchObject({ type: "completed" });
		expect(loader.commitCalls).toHaveLength(2);
		expect(providerCalls).toBe(1);
		expect(session.state.peekAcceptedInput()).toBeUndefined();
		expect(JSON.stringify(session.state.contextManager.entries())).toContain(
			"task result recovered from the replayed receipt",
		);
	});
	test("task notification validation failure retains accepted custody without provider work", async () => {
		const session = new ThreadRuntime("sesn_task_notification_invalid_commit");
		let providerCalls = 0;
		const runtimeInputId = "rin_task_notification_invalid_commit";
		expect(
			session.state.enqueueAcceptedInput(
				taskNotificationInput(
					runtimeInputId,
					"task_notification_invalid_commit",
					"sevt_task_notification_invalid_commit",
					"completed",
					'{"status":"completed"}',
					session.sessionId,
				),
			),
		).toBe("applied");
		const loader = new QueuedContextLoader(
			[],
			[],
			[
				(input: RuntimeAcceptedInputState) => {
					throw normalizeContextLoaderError({
						code: "schema_mismatch",
						sessionId: input.sessionId,
						reason: "task_notification_result_invalid",
					});
				},
			],
		);
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						onStream: () => {
							providerCalls += 1;
						},
					}),
				),
			),
		);

		expect(result).toMatchObject({ type: "failed" });
		expect(loader.commitCalls).toHaveLength(1);
		expect(providerCalls).toBe(0);
		expect(session.state.peekAcceptedInput()?.runtimeInputId).toBe(
			runtimeInputId,
		);
	});
	test("terminal task notification rejection drops only that input and continues the thread", async () => {
		const session = new ThreadRuntime("sesn_task_notification_rejected");
		const observations: RuntimeAcceptedInputCommitObservation[] = [];
		const appended: SessionEvent[] = [];
		let providerCalls = 0;
		expect(
			session.state.enqueueAcceptedInput(
				taskNotificationInput(
					"rin_task_notification_rejected",
					"task_notification_rejected",
					"sevt_task_notification_rejected",
					"completed",
					'{"status":"completed"}',
					session.sessionId,
				),
			),
		).toBe("applied");
		const followUp = userMessage(
			"msg_after_task_rejection",
			1,
			"continue after rejected notification",
		);
		expect(
			session.state.enqueueAcceptedInput({
				...acceptedInput("rin_after_task_rejection", session.sessionId),
				contentJson: JSON.stringify({ messages: [followUp] }),
			}),
		).toBe("applied");
		const loader = new QueuedContextLoader(
			[],
			[],
			[
				() => ({
					type: "task_notification_rejected" as const,
					errorCode: "task_notification_payload_mismatch" as const,
				}),
				(input: RuntimeAcceptedInputState) => acceptedInputCommitResult(input),
			],
		);
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				return yield* (yield* ThreadLoop.Service).run(
					session,
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						recordAcceptedInputCommit: (event) => observations.push(event),
						onStream: () => {
							providerCalls += 1;
						},
						writer: writerFrom((envelope) => {
							appended.push(envelope.event);
							return {
								ok: true,
								eventId: `bridge-${envelope.writeId}`,
								type: "committed",
								eventSequence: 1,
							};
						}),
					}),
				),
			),
		);

		expect(result).toMatchObject({ type: "completed" });
		expect(loader.commitCalls).toHaveLength(2);
		expect(providerCalls).toBe(1);
		expect(session.state.peekAcceptedInput()).toBeUndefined();
		expect(JSON.stringify(session.state.contextManager.entries())).toContain(
			"continue after rejected notification",
		);
		expect(appended.filter((event) => event.type === "session.error")).toEqual(
			[],
		);
		expect(observations).toContainEqual(
			expect.objectContaining({
				runtimeInputId: "rin_task_notification_rejected",
				outcome: "rejected",
				failureClass: "task_notification_payload_mismatch",
			}),
		);
	});
	test("task notification arriving during provider reschedule joins the next safe request", async () => {
		const session = new ThreadRuntime("sesn_task_notification_reschedule");
		let commitCalls = 0;
		const requests: LLMRequest[] = [];
		const streams: readonly (readonly LLMEvent[])[] = [
			[
				{
					type: "provider-error",
					error: runtimeFailureFromProviderError(
						normalizeProviderError({
							code: "provider_unavailable",
							message: "retry the current request",
							retryable: true,
							fatal: false,
						}),
					),
				},
			],
			[
				{ type: "text-start", id: "current-answer" },
				{
					type: "text-delta",
					id: "current-answer",
					text_delta: "current turn recovered",
				},
				{ type: "text-end", id: "current-answer" },
				{ type: "finish", finishReason: "stop" },
			],
			[
				{ type: "text-start", id: "task-answer" },
				{
					type: "text-delta",
					id: "task-answer",
					text_delta: "task acknowledged",
				},
				{ type: "text-end", id: "task-answer" },
				{ type: "finish", finishReason: "stop" },
			],
		];
		let streamIndex = 0;
		const llm: LLMServiceInterface = {
			stream(request) {
				requests.push(request);
				if (streamIndex === 0) {
					expect(
						session.state.enqueueAcceptedInput(
							taskNotificationInput(
								"rin_task_notification_reschedule",
								"task_notification_reschedule",
								"sevt_task_notification_reschedule",
								"completed",
								'{"status":"completed","text":"task result after the retried request"}',
								session.sessionId,
							),
						),
					).toBe("applied");
				}
				return Stream.fromIterable(streams[streamIndex++] ?? []);
			},
		};
		const loader = new QueuedContextLoader(
			[],
			[
				{
					type: "context",
					entries: [
						userMessage(
							"user-task-reschedule",
							0,
							"finish the current request",
						),
					],
				},
			],
			[
				(input: RuntimeAcceptedInputState) => {
					commitCalls += 1;
					return acceptedInputCommitResult(input);
				},
			],
		);
		const layer = runtimeThreadLoopLayer(loader, {
			llmService: llm,
			runtimePolicy: () => ({
				providerRescheduleBudget: 3,
				compactionRescheduleBudget: 2,
			}),
		});
		const custody = testRunCustody();
		const currentTurn = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, custody);
			}).pipe(Effect.provide(layer)),
		);
		expect(currentTurn).toMatchObject({ type: "completed" });
		expect(commitCalls).toBe(1);
		expect(requests).toHaveLength(3);
		expect(JSON.stringify(requests[1]?.context)).not.toContain(
			"task result after the retried request",
		);
		expect(
			JSON.stringify(requests[2]?.context).match(
				/task result after the retried request/g,
			),
		).toHaveLength(1);
		expect(session.state.peekAcceptedInput()).toBeUndefined();
	});
	test("stale task notification custody discards the resident thread before provider work", async () => {
		const session = new ThreadRuntime("sesn_stale_task_notification");
		let providerCalls = 0;
		expect(
			session.state.enqueueAcceptedInput(
				taskNotificationInput(
					"rin_stale_task_notification",
					"task_stale_task_notification",
					"sevt_stale_task_notification",
					"completed",
					'{"status":"completed"}',
					session.sessionId,
				),
			),
		).toBe("applied");
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(session, testRunCustody());
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(
						new QueuedContextLoader([], [], [{ type: "stale_custody" }]),
						{
							onStream: () => {
								providerCalls += 1;
							},
						},
					),
				),
			),
		);
		expect(result).toEqual({ type: "interrupted", discardHotState: true });
		expect(providerCalls).toBe(0);
		expect(session.state.peekAcceptedInput()).toBeUndefined();
		expect(session.state.contextManager.entries()).toEqual([]);
	});
	test("denied provider reschedule appends one exhausted error before idle", async () => {
		const order: string[] = [];
		const session = new ThreadRuntime("sesn_1");
		const store = new ThreadLoopRuntimeStore(order);
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-1", 0, "hello")],
		});
		const appended: SessionEvent[] = [];
		const baseWriter = writerFrom((envelope) => {
			appended.push(envelope.event);
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
				const result = await baseWriter.writeRequestEnd(envelope);
				return result.ok && result.type !== "stale"
					? { ...result, outcome: { type: "ordinary" } }
					: result;
			},
		};
		const providerError = {
			type: "provider",
			code: "provider_unavailable",
			message: "Provider unavailable.",
			retryable: true,
			fatal: false,
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
						writer,
						events: [{ type: "provider-error", error: providerError }],
						runtimePolicy: () => ({
							providerRescheduleBudget: 3,
							compactionRescheduleBudget: 2,
						}),
					}),
				),
			),
		);
		expect(result).toMatchObject({
			type: "failed",
			error: { ...providerError, retryStatus: { type: "exhausted" } },
		});
		expect(result).not.toHaveProperty("releaseSession");
		expect(appended.map((event) => event.type)).toEqual([
			"session.status_running",
			"span.model_request_start",
			"span.model_request_end",
			"session.error",
			"session.status_idle",
		]);
		expect(appended.at(3)).toEqual({
			type: "session.error",
			error: { ...providerError, retryStatus: { type: "exhausted" } },
		});
		expect(appended.at(4)).toEqual({
			type: "session.status_idle",
			stop_reason: { type: "retries_exhausted" },
		});
		expect(JSON.stringify(appended)).not.toContain('"type":"retrying"');
		expect(
			session.state.contextManager
				.entries()
				.some((message) => message.contextKind === "assistant"),
		).toBe(false);
	});
});

describe("provider call skill guidance", () => {
	test("rejects a freeform declaration outside the GPT family before request open", async () => {
		const hostileGrammar = "start: /private provider payload marker/";
		const catalog: ToolCatalog = {
			entries: [
				{
					name: "apply_patch",
					definition: {
						kind: "freeform",
						name: "apply_patch",
						description: "Apply a patch",
						larkGrammar: hostileGrammar,
					},
					inputContract: { kind: "freeform_string", executionField: "patch" },
					route: {
						kind: "sandbox",
						operation: "RunTool",
						helperSubcommand: "apply_patch",
					},
					formatter: {
						successShape: "changed files",
						errorShape: "patch error",
						forbiddenFields: [],
					},
					defaultPermissionPolicy: "always_ask",
					required: true,
				},
			],
			configs: [{ name: "apply_patch", enabled: true }],
		};
		const input = providerInput([]);
		expect(
			assembleProviderCallRequest({
				...input,
				runtime: {
					...input.runtime,
					toolCatalog: catalog,
					toolsetFamily: "claude",
				},
			}),
		).toMatchObject({
			ok: false,
			error: { reason: "runtime_contract_validation" },
			toolDeclarationRejection: {
				declarationKind: "freeform",
				family: "claude",
				validationMember: "tool_family",
			},
		});

		const observations: unknown[] = [];
		let providerCalled = false;
		const loader = new RecordingContextLoader([], {
			type: "context",
			entries: [userMessage("user-declaration", 0, "hello")],
		});
		const result = await Effect.runPromise(
			Effect.gen(function* () {
				const threadLoop = yield* ThreadLoop.Service;
				return yield* threadLoop.run(
					new ThreadRuntime("sesn_1"),
					testRunCustody(),
				);
			}).pipe(
				Effect.provide(
					runtimeThreadLoopLayer(loader, {
						providerCallRuntime: {
							...DefaultProviderCallRuntimeConfig,
							toolCatalog: catalog,
							toolsetFamily: "claude",
						},
						recordProviderToolDeclarationRejection: (event) =>
							observations.push(event),
						onStream: () => {
							providerCalled = true;
						},
					}),
				),
			),
		);

		expect(result).toMatchObject({
			type: "failed",
			error: {
				code: "runtime_invalid_sequence",
				reason: "runtime_contract_validation",
			},
		});
		expect(providerCalled).toBe(false);
		expect(observations).toEqual([
			expect.objectContaining({
				workspaceId: "workspace-test",
				sessionId: "sesn_1",
				sessionThreadId: "thread-test",
				declarationKind: "freeform",
				family: "claude",
				validationMember: "tool_family",
			}),
		]);
		expect(JSON.stringify(observations)).not.toContain(hostileGrammar);
	});

	test("preserves third-party provider content through request assembly by top-level projection", () => {
		const input = providerInput([]);
		const catalog = createToolCatalog({
			family: "gpt",
			includeSubAgentTools: false,
			mcpManifests: [
				{
					mcpServerName: "projection-server",
					manifestETag: "etag-projection",
					manifestGeneration: 1,
					tools: [
						{
							name: "content_canary",
							description: "Use operation credentials exactly as described.",
							inputSchema: {
								type: "object",
								properties: {
									route: { const: "binding" },
									binding: { const: "credentials" },
								},
							},
						},
					],
				},
			],
		});
		const result = assembleProviderCallRequest({
			...input,
			runtime: { ...input.runtime, toolCatalog: catalog, toolsetFamily: "gpt" },
		});
		expect(result.ok).toBe(true);
		if (!result.ok) {
			return;
		}
		const projected = result.request.tools.find(
			(tool) => tool.name === "content_canary",
		);
		expect(projected).toEqual({
			name: "content_canary",
			description: "Use operation credentials exactly as described.",
			function: {
				inputSchemaJson: JSON.stringify({
					type: "object",
					properties: {
						route: { const: "binding" },
						binding: { const: "credentials" },
					},
				}),
			},
		});
		expect(
			result.request.tools.find((tool) => tool.name === "exec_command")
				?.function,
		).toBeDefined();
		expect(lookupToolEntry(catalog, "content_canary")).toMatchObject({
			route: {
				kind: "gateway",
				operation: "RunMcpTool",
				mcpServerName: "projection-server",
			},
			defaultPermissionPolicy: "always_ask",
			required: false,
		});
	});

	test("rejects an unspecified provider request kind", () => {
		expect(
			assembleProviderCallRequest(
				providerInput(
					[],
					ProviderRequestKind.PROVIDER_REQUEST_KIND_UNSPECIFIED,
				),
			),
		).toMatchObject({
			ok: false,
			error: { reason: "runtime_contract_validation" },
		});
	});
	test("adds a stable deterministic SKILL segment only to agent requests", () => {
		const skillsIndex = [
			skillEntry("sk_zeta", "2.0.0", "Zeta", "zeta", "Zeta guidance."),
			skillEntry("sk_alpha", "1.0.0", "Alpha", "alpha", "Alpha guidance."),
		];
		const agent = assembleProviderCallRequest(providerInput(skillsIndex));
		expect(agent.ok).toBe(true);
		if (!agent.ok) {
			return;
		}
		expect(agent.system[1]).toMatchObject({
			kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_SKILL,
			cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE,
		});
		expect(agent.system[1]?.text.indexOf('"name":"Alpha"')).toBeLessThan(
			agent.system[1]?.text.indexOf('"name":"Zeta"') ?? -1,
		);
		expect(agent.system[1]?.text).toContain(
			'"skill_md_path":"/skills/alpha/SKILL.md"',
		);
		expect(agent.system[1]?.text).not.toContain("skill_version_id");
		expect(agent.system[1]?.text).not.toContain("skill_id");
		expect(agent.system[1]?.text).not.toContain("skill body contents");
		for (const requestKind of [
			ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
			ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY,
			ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER_COMPACTION,
		]) {
			const nonAgent = assembleProviderCallRequest(
				providerInput(skillsIndex, requestKind),
			);
			expect(nonAgent.ok).toBe(true);
			if (nonAgent.ok) {
				expect(nonAgent.system.map((segment) => segment.kind)).not.toContain(
					SystemSegmentKind.SYSTEM_SEGMENT_KIND_SKILL,
				);
			}
		}
	});
	test("orders agent and per-store memory segments between the platform base and skill guidance", () => {
		const agentSystem = "Operate as the session specialist.";
		const memoryStores: readonly MemoryStorePromptEntry[] = [
			{
				memoryStoreId: "memstore_notes",
				name: "Project notes",
				access: "read_write",
				instructions:
					"Keep decisions and durable context here.\nPreserve this line verbatim.",
			},
			{
				memoryStoreId: "memstore_reference",
				name: "Reference",
				access: "read_only",
			},
		];
		const input = providerInput([
			skillEntry("sk_alpha", "1.0.0", "Alpha", "alpha", "Alpha guidance."),
		]);
		const agent = assembleProviderCallRequest({
			...input,
			runtime: { ...input.runtime, agentSystem, memoryStores },
		});
		expect(agent.ok).toBe(true);
		if (!agent.ok) {
			return;
		}
		expect(agent.system.map((segment) => segment.kind)).toEqual([
			SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
			SystemSegmentKind.SYSTEM_SEGMENT_KIND_AGENT,
			SystemSegmentKind.SYSTEM_SEGMENT_KIND_MEMORY,
			SystemSegmentKind.SYSTEM_SEGMENT_KIND_MEMORY,
			SystemSegmentKind.SYSTEM_SEGMENT_KIND_SKILL,
		]);
		expect(agent.system[1]).toEqual({
			kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_AGENT,
			text: agentSystem,
			cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_SESSION,
		});
		expect(agent.system[2]).toEqual({
			kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_MEMORY,
			text: "Memory store: Project notes\nAccess: read_write\nInstructions:\nKeep decisions and durable context here.\nPreserve this line verbatim.",
			cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_SESSION,
		});
		expect(agent.system[3]).toEqual({
			kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_MEMORY,
			text: "Memory store: Reference\nAccess: read_only",
			cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_SESSION,
		});
		for (const requestKind of [
			ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
			ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY,
			ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER_COMPACTION,
		]) {
			const nonAgentInput = providerInput([], requestKind);
			const nonAgent = assembleProviderCallRequest({
				...nonAgentInput,
				runtime: { ...nonAgentInput.runtime, agentSystem, memoryStores },
			});
			expect(nonAgent.ok).toBe(true);
			if (nonAgent.ok) {
				expect(nonAgent.system.map((segment) => segment.kind)).not.toContain(
					SystemSegmentKind.SYSTEM_SEGMENT_KIND_AGENT,
				);
				expect(nonAgent.system.map((segment) => segment.text)).not.toContain(
					agentSystem,
				);
				expect(nonAgent.system.map((segment) => segment.kind)).not.toContain(
					SystemSegmentKind.SYSTEM_SEGMENT_KIND_MEMORY,
				);
			}
		}
	});
	test("keeps the platform base system prompt byte-for-byte stable", () => {
		expect(PlatformBaseSystemPrompt).toBe(
			"You are Tetral Agent, working in a sandboxed Linux environment.\n\nFiles in your sandbox persist for the life of this session,\nincluding across session sleep and wake, and are gone when the\nsession ends. To keep something across sessions, use the memory\ntool. To deliver a file to the user, save it under\n/mnt/session/outputs — files there are collected and delivered\nautomatically.",
		);
	});
	test("injects bounded apply-patch instructions only for GPT-family agent requests", () => {
		expect(
			new TextEncoder().encode(ApplyPatchInstructionsText).byteLength,
		).toBeLessThan(MaxTextBytes);
		expect(ApplyPatchInstructionsText).toContain(
			"Absolute paths are accepted under the declared writable roots — the workspace, /mnt/session/uploads, and /mnt/session/outputs — and rejected outside them.",
		);
		for (const [family, requestKind, shouldInject] of [
			[
				"gpt",
				ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
				true,
			],
			[
				"claude",
				ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
				false,
			],
			[
				undefined,
				ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
				false,
			],
			[
				"gpt",
				ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
				false,
			],
			[
				"gpt",
				ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY,
				false,
			],
		] as const) {
			const input = providerInput([], requestKind);
			const result = assembleProviderCallRequest({
				...input,
				runtime: {
					...input.runtime,
					...(family === undefined ? {} : { toolsetFamily: family }),
				},
			});
			expect(result.ok).toBe(true);
			if (!result.ok) {
				continue;
			}
			const base =
				result.system.find(
					(segment) =>
						segment.kind === SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
				)?.text ?? "";
			expect(base.includes(ApplyPatchInstructionsText)).toBe(shouldInject);
		}
	});
	test("rejects a GPT base prompt whose apply-patch injection exceeds the segment cap", () => {
		const input = providerInput([]);
		expect(
			assembleProviderCallRequest({
				...input,
				runtime: {
					...input.runtime,
					toolsetFamily: "gpt",
					systemInstructions: "x".repeat(MaxTextBytes),
				},
			}),
		).toMatchObject({ ok: false, error: { reason: "bounded" } });
	});
	test("adds the approval policy as a dedicated stable segment only to reviewer requests", () => {
		const policy = "Review solely under this fixed approval policy.";
		const reviewer = assembleProviderCallRequest(
			providerInput(
				[],
				ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
				policy,
			),
		);
		expect(reviewer.ok).toBe(true);
		if (!reviewer.ok) {
			return;
		}
		expect(
			reviewer.system.filter(
				(segment) =>
					segment.kind ===
					SystemSegmentKind.SYSTEM_SEGMENT_KIND_APPROVAL_REVIEWER_POLICY,
			),
		).toEqual([
			{
				kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_APPROVAL_REVIEWER_POLICY,
				text: policy,
				cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE,
			},
		]);
		expect(
			reviewer.system.find(
				(segment) =>
					segment.kind === SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
			)?.text,
		).not.toContain(policy);
		for (const requestKind of [
			ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
			ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY,
			ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER_COMPACTION,
		]) {
			const nonReviewer = assembleProviderCallRequest(
				providerInput([], requestKind, policy),
			);
			expect(nonReviewer.ok).toBe(true);
			if (nonReviewer.ok) {
				expect(nonReviewer.system.map((segment) => segment.kind)).not.toContain(
					SystemSegmentKind.SYSTEM_SEGMENT_KIND_APPROVAL_REVIEWER_POLICY,
				);
				expect(nonReviewer.system.map((segment) => segment.text)).not.toContain(
					policy,
				);
			}
		}
	});
	test("rejects reviewer requests without a non-empty dedicated approval policy", () => {
		const withPolicy = providerInput(
			[],
			ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER,
		);
		const { approvalReviewerPolicy: _removedPolicy, ...runtimeWithoutPolicy } =
			withPolicy.runtime;
		for (const input of [
			{ ...withPolicy, runtime: runtimeWithoutPolicy },
			{
				...withPolicy,
				runtime: { ...withPolicy.runtime, approvalReviewerPolicy: "   " },
			},
		]) {
			const result = assembleProviderCallRequest(input);
			expect(result).toMatchObject({
				ok: false,
				error: { reason: "runtime_contract_validation" },
			});
		}
	});
	test("assembles reviewer compaction without system segments, tools, or output schema", () => {
		const input = providerInput(
			[],
			ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER_COMPACTION,
		);
		const result = assembleProviderCallRequest(input);
		expect(result.ok).toBe(true);
		if (!result.ok) {
			return;
		}
		expect(result.request).toMatchObject({
			requestKind:
				ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER_COMPACTION,
			system: [],
			tools: [],
		});
		expect(result.request.outputSchemaJson).toBeUndefined();
		expect(
			assembleProviderCallRequest({
				...input,
				runtime: { ...input.runtime, outputSchemaJson: '{"type":"object"}' },
			}),
		).toMatchObject({
			ok: false,
			error: { reason: "runtime_contract_validation" },
		});
	});
	test("requires the Runtime-configured provider stream timeout", () => {
		const input = providerInput([]);
		const { timeoutMs: _timeoutMs, ...runtimeWithoutTimeout } = input.runtime;
		expect(
			assembleProviderCallRequest({ ...input, runtime: runtimeWithoutTimeout }),
		).toMatchObject({
			ok: false,
			error: { reason: "bounded" },
		});
	});
	test("applies and notes per-entry and uniform description truncation", () => {
		const text = renderSkillGuidanceSegment(
			[
				skillEntry("sk_alpha", "1.0.0", "Alpha", "alpha", "a".repeat(5000)),
				skillEntry("sk_beta", "1.0.0", "Beta", "beta", "b".repeat(5000)),
				skillEntry("sk_gamma", "1.0.0", "Gamma", "gamma", "界".repeat(5000)),
			],
			1024,
		);
		expect(text).toContain("per-entry description cap applied");
		expect(text).toContain("uniform description shortening applied");
		const descriptionBytes = text
			.split("\n")
			.filter((line) => line.startsWith("{"))
			.map(
				(line) =>
					JSON.parse(line) as {
						readonly description: string;
					},
			)
			.reduce(
				(total, entry) =>
					total + new TextEncoder().encode(entry.description).byteLength,
				0,
			);
		expect(descriptionBytes).toBeLessThanOrEqual(1024);
		expect(new TextEncoder().encode(text).byteLength).toBeLessThan(
			MaxTextBytes,
		);
	});
	test("omits entries from the deterministic tail and notes the omission", () => {
		const text = renderSkillGuidanceSegment(
			[
				skillEntry("sk_alpha", "1.0.0", "a".repeat(30000), "alpha", ""),
				skillEntry("sk_beta", "1.0.0", "b".repeat(30000), "beta", ""),
				skillEntry("sk_zeta", "1.0.0", "z".repeat(30000), "zeta", ""),
			],
			1024,
		);
		expect(text).toContain('"skill_md_path":"/skills/alpha/SKILL.md"');
		expect(text).not.toContain('"skill_md_path":"/skills/zeta/SKILL.md"');
		expect(text).toContain("end-of-order skill omission applied");
		expect(new TextEncoder().encode(text).byteLength).toBeLessThan(
			MaxTextBytes,
		);
	});
});
function providerInput(
	skillsIndex: readonly SkillGuidanceIndexEntry[],
	requestKind = ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
	approvalReviewerPolicy = requestKind ===
	ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER
		? "Fixed approval reviewer policy."
		: undefined,
): ProviderCallAssemblyInput {
	return {
		identity: {
			workspaceId: "workspace_1",
			sessionId: "sesn_1",
			sessionThreadId: "thread_1",
			parentThreadId: "",
			bindingId: "binding_1",
			bindingGeneration: 1,
			targetPodUid: "pod_1",
			runtimeBindingToken: "runtime-token",
		},
		requestId: "provider_request_1",
		modelRequestId: "model_request_1",
		currentModel: { providerId: "openai", modelId: "gpt-5.5" },
		providerContext: [
			{
				role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
				content: [{ text: { text: "hello" } }],
			},
		],
		runtime: {
			systemInstructions: "You are Tetral Agent.",
			timeoutMs: 1800000,
			requestKind,
			skillsIndex,
			skillGuidanceDescriptionBudgetBytes: 32 * 1024,
			...(approvalReviewerPolicy !== undefined
				? { approvalReviewerPolicy }
				: {}),
			...(requestKind ===
			ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER
				? { outputSchemaJson: '{"type":"object"}' }
				: {}),
		},
	};
}
function skillEntry(
	skillId: string,
	version: string,
	name: string,
	directory: string,
	description: string,
): SkillGuidanceIndexEntry {
	return {
		skillId,
		skillVersionId: `skv_${skillId}_${version}`,
		version,
		name,
		description,
		directory,
	};
}
